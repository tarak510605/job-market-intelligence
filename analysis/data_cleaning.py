"""
Cleaning steps
--------------
1. Load raw data and log shape / missing-value summary
2. Drop exact duplicate rows
3. Standardise column names
4. Clean and normalise the 'location' column
5. Standardise skill names
6. Parse salary strings → numeric USD values
7. Map experience strings → standardised levels
8. Handle remaining missing values
9. Add derived columns (country, salary_band)
10. Save cleaned CSV

Usage:
    python analysis/data_cleaning.py
    python analysis/data_cleaning.py --input data/raw/jobs_raw.csv --output data/cleaned/jobs_cleaned.csv
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

import pandas as pd

# Make utils importable whether the script is run from project root or analysis/
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from utils.helpers import (
    clean_text,
    extract_country,
    infer_experience_level,
    normalise_location,
    parse_salary_to_usd,
    parse_skills_list,
    salary_band,
    standardise_skill,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_INPUT = os.path.join(_ROOT, "data", "raw", "jobs_raw.csv")
DEFAULT_OUTPUT = os.path.join(_ROOT, "data", "cleaned", "jobs_cleaned.csv")


# ---------------------------------------------------------------------------
# Step functions
# ---------------------------------------------------------------------------

def load_raw(path: str) -> pd.DataFrame:
    """Load raw CSV and log a quick diagnostic summary."""
    logger.info("Loading raw data from %s", path)
    df = pd.read_csv(path, encoding="utf-8", low_memory=False)
    logger.info("Loaded  shape=%s", df.shape)
    missing = df.isna().sum()
    missing = missing[missing > 0]
    if not missing.empty:
        logger.info("Missing values:\n%s", missing.to_string())
    return df


def drop_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Remove exact duplicate rows."""
    before = len(df)
    df = df.drop_duplicates()
    dropped = before - len(df)
    logger.info("Duplicate rows removed: %d  (remaining: %d)", dropped, len(df))
    return df


def standardise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure all expected columns are present; rename variants to the
    canonical schema used throughout the project.
    """
    rename_map = {
        # Possible alternatives from different sources
        "Role": "job_title",
        "Job Title": "job_title",
        "title": "job_title",
        "Company Name": "company_name",
        "company": "company_name",
        "Location": "location",
        "Salary": "salary",
        "Key Skill": "required_skills",
        "skills": "required_skills",
        "Experience": "experience_level",
        "Job Description": "job_description",
        "Posted Date": "posting_date",
        "Extracted Date": "scraped_date",
        "More Detail": "source_url",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Ensure canonical columns exist
    canonical = [
        "job_title", "company_name", "location", "salary",
        "required_skills", "experience_level", "job_description",
        "posting_date", "scraped_date", "source_url", "source_portal",
    ]
    for col in canonical:
        if col not in df.columns:
            df[col] = pd.NA

    return df[canonical]


def clean_locations(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise raw location strings to canonical city names."""
    df["location"] = df["location"].apply(normalise_location)
    df["country"] = df["location"].apply(extract_country)
    logger.info("Unique locations after normalisation: %d", df["location"].nunique())
    return df


def clean_skills(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse and standardise the required_skills column.
    Stores the cleaned skills as a comma-separated string so the CSV
    remains flat and easy to import into Power BI / Tableau.
    """
    def _clean_and_join(raw):
        skills = parse_skills_list(str(raw))
        return ", ".join(skills) if skills else pd.NA

    df["required_skills"] = df["required_skills"].apply(_clean_and_join)
    return df


def clean_salary(df: pd.DataFrame) -> pd.DataFrame:
    """Parse salary strings into a numeric USD annual column."""
    df["salary_usd"] = df["salary"].apply(parse_salary_to_usd)
    df["salary_band"] = df["salary_usd"].apply(salary_band)
    disclosed_pct = df["salary_usd"].notna().mean() * 100
    logger.info("Salary disclosed in %.1f%% of records", disclosed_pct)
    return df


def clean_experience(df: pd.DataFrame) -> pd.DataFrame:
    """Map raw experience strings to standardised experience-level labels."""
    df["experience_level"] = df["experience_level"].apply(infer_experience_level)
    return df


def clean_text_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Strip extra whitespace from free-text fields."""
    for col in ("job_title", "company_name", "job_description"):
        if col in df.columns:
            df[col] = df[col].apply(clean_text)
    return df


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply a consistent missing-value strategy:
      - Drop rows where job_title or company_name is blank
      - Fill remaining NAs with sensible defaults
    """
    before = len(df)
    df = df.dropna(subset=["job_title"])
    df = df[df["job_title"].str.strip() != ""]
    logger.info("Rows dropped (missing job_title): %d", before - len(df))

    fill_map = {
        "company_name": "Unknown Company",
        "location": "Unknown",
        "country": "Other",
        "salary": "N/A",
        "required_skills": "Not Listed",
        "experience_level": "Not Specified",
        "job_description": "",
        "posting_date": "N/A",
        "source_url": "",
        "source_portal": "Unknown",
    }
    df = df.fillna(fill_map)
    return df


def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add analysis-friendly derived columns."""
    # Truncate long descriptions for readability
    df["job_description"] = df["job_description"].str[:300]

    # Skill count per posting
    df["skill_count"] = df["required_skills"].apply(
        lambda s: len([x for x in str(s).split(",") if x.strip() and x.strip() != "Not Listed"])
    )

    return df


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_pipeline(input_path: str, output_path: str) -> pd.DataFrame:
    """Execute all cleaning steps in order and return the cleaned DataFrame."""
    df = load_raw(input_path)
    df = drop_duplicates(df)
    df = standardise_columns(df)
    df = clean_text_fields(df)
    df = clean_locations(df)
    df = clean_skills(df)
    df = clean_salary(df)
    df = clean_experience(df)
    df = handle_missing_values(df)
    df = add_derived_columns(df)

    logger.info("Final cleaned shape: %s", df.shape)

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8")
    logger.info("Cleaned data saved → %s", output_path)

    return df


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Job Market Intelligence — Data Cleaning Pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--input", default=DEFAULT_INPUT, help="Path to raw CSV input.")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Path for cleaned CSV output.")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run_pipeline(args.input, args.output)

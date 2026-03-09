"""
Job Market Intelligence Dashboard — Dashboard Dataset Generator
===============================================================
Builds a flat aggregated CSV suitable for loading directly into Power BI or Tableau.

Aggregation dimensions
----------------------
  skill × role × city × experience_level

Metrics per aggregate group
----------------------------
  job_count      — number of postings requiring the skill
  avg_salary     — average annual salary (USD) for postings that disclosed salary
  median_salary  — median annual salary (USD)
  pct_of_postings— % of postings in the role/city group that require the skill

Usage:
    python dashboards/build_dashboard.py
    python dashboards/build_dashboard.py --input data/cleaned/jobs_cleaned.csv
"""

from __future__ import annotations

import argparse
import ast
import logging
import os
import sys
from typing import Optional

import pandas as pd
from datasets import load_dataset

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

DEFAULT_OUTPUT = os.path.join(_HERE, "dashboard_dataset.csv")


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

def load_cleaned_csv(path: str) -> Optional[pd.DataFrame]:
    """Load the locally cleaned CSV if it exists."""
    if os.path.isfile(path):
        df = pd.read_csv(path, low_memory=False)
        logger.info("Loaded cleaned CSV  %s  shape=%s", path, df.shape)
        return df
    return None


def load_huggingface() -> pd.DataFrame:
    """Fallback: load the public Hugging Face data_jobs dataset and normalise columns."""
    logger.info("Cleaned CSV not found — loading Hugging Face dataset (lukebarousse/data_jobs) …")
    dataset = load_dataset("lukebarousse/data_jobs")
    df = dataset["train"].to_pandas().reset_index(drop=True)

    df["job_posted_date"] = pd.to_datetime(df["job_posted_date"])
    df["job_skills"] = df["job_skills"].apply(
        lambda x: ast.literal_eval(x) if pd.notna(x) else []
    )

    # The dataset has BOTH 'job_title' (full verbose title) and 'job_title_short'
    # (standardised bucket like "Data Analyst").  Drop the full-title column first
    # so renaming job_title_short → job_title does not create a duplicate column.
    df = df.drop(columns=["job_title"], errors="ignore")

    # Rename to project schema
    df = df.rename(columns={
        "job_title_short": "job_title",
        "job_location": "location",
        "salary_year_avg": "salary_usd",
        "job_skills": "required_skills",
    })
    df["country"]          = df["job_country"]
    df["salary_band"]      = df["salary_usd"].apply(_salary_band)

    # Experience level: not present in raw dataset — derive from job_title
    df["experience_level"] = df["job_title"].apply(_infer_experience_from_title)

    logger.info("HuggingFace dataset loaded  shape=%s", df.shape)
    return df


def _salary_band(v) -> str:
    if pd.isna(v):
        return "Not Disclosed"
    if v < 40_000:
        return "< $40K"
    if v < 70_000:
        return "$40K – $70K"
    if v < 100_000:
        return "$70K – $100K"
    if v < 140_000:
        return "$100K – $140K"
    return "$140K+"


def _infer_experience_from_title(title: str) -> str:
    t = str(title).lower()
    if any(kw in t for kw in ("senior", "sr.", "lead", "principal", "staff")):
        return "Senior"
    if any(kw in t for kw in ("junior", "jr.", "associate", "entry")):
        return "Entry Level"
    return "Mid Level"


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def build_dashboard(df: pd.DataFrame) -> pd.DataFrame:
    """
    Explode skills and aggregate metrics at skill × role × city × experience level.

    Works with both the locally cleaned CSV schema and the HuggingFace schema.
    """
    # Detect which skills column to use
    skills_col = "required_skills"
    salary_col = "salary_usd" if "salary_usd" in df.columns else "salary_year_avg"

    # Ensure skills are lists
    def _to_list(val):
        if isinstance(val, list):
            return val
        if pd.isna(val):
            return []
        text = str(val).strip()
        if text.startswith("["):
            try:
                return ast.literal_eval(text)
            except (ValueError, SyntaxError):
                pass
        return [s.strip() for s in text.split(",") if s.strip()]

    df = df.copy()
    df["_skill_list"] = df[skills_col].apply(_to_list)

    # Explode — one row per skill
    df_exp = df.explode("_skill_list")
    df_exp = df_exp[df_exp["_skill_list"].notna() & (df_exp["_skill_list"] != "")]
    df_exp = df_exp.rename(columns={"_skill_list": "skill"})

    # Normalise grouping columns
    role_col = "job_title"
    city_col = "location"
    exp_col  = "experience_level"

    for col in (role_col, city_col, exp_col):
        if col not in df_exp.columns:
            df_exp[col] = "Unknown"
    if salary_col not in df_exp.columns:
        df_exp[salary_col] = float("nan")

    # Aggregate
    agg = (
        df_exp
        .groupby(["skill", role_col, city_col, exp_col], dropna=False)
        .agg(
            job_count=(role_col, "count"),
            avg_salary=(salary_col, "mean"),
            median_salary=(salary_col, "median"),
        )
        .reset_index()
        .rename(columns={role_col: "role", city_col: "city", exp_col: "experience_level"})
    )

    # % of all postings in this role+city that mention the skill
    role_city_totals = (
        df.groupby([role_col, city_col], dropna=False)
        .size()
        .reset_index(name="_total")
        .rename(columns={role_col: "role", city_col: "city"})
    )
    agg = agg.merge(role_city_totals, on=["role", "city"], how="left")
    agg["pct_of_postings"] = (agg["job_count"] / agg["_total"] * 100).round(1)
    agg = agg.drop(columns="_total")

    # Round salary columns
    agg["avg_salary"]    = agg["avg_salary"].round(0)
    agg["median_salary"] = agg["median_salary"].round(0)

    # Sort: most demanded first
    agg = agg.sort_values(["role", "job_count"], ascending=[True, False]).reset_index(drop=True)

    logger.info("Dashboard aggregation complete  rows=%d  cols=%s", len(agg), list(agg.columns))
    return agg


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main(cleaned_csv: str = "", output: str = DEFAULT_OUTPUT) -> None:
    cleaned_csv = cleaned_csv or os.path.join(_ROOT, "data", "cleaned", "jobs_cleaned.csv")

    df = load_cleaned_csv(cleaned_csv)
    if df is None:
        df = load_huggingface()

    dashboard = build_dashboard(df)

    os.makedirs(os.path.dirname(os.path.abspath(output)), exist_ok=True)
    dashboard.to_csv(output, index=False, encoding="utf-8")
    logger.info("Dashboard dataset saved → %s  (%d rows)", output, len(dashboard))

    print("\n=== Dashboard Dataset Preview ===")
    print(dashboard.head(15).to_string(index=False))
    print(f"\nTotal rows : {len(dashboard):,}")
    print(f"Unique skills : {dashboard['skill'].nunique():,}")
    print(f"Unique roles  : {dashboard['role'].nunique():,}")
    print(f"\nReady to import into Power BI or Tableau:\n  {os.path.abspath(output)}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Job Market Intelligence — Dashboard Dataset Generator",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--input",  default="", help="Path to cleaned CSV (optional).")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Output CSV path.")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    main(cleaned_csv=args.input, output=args.output)

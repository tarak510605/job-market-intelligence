"""
Job Market Intelligence Dashboard — Utility Helpers
====================================================
Reusable, stateless helper functions used by the cleaning pipeline,
analysis notebooks, and dashboard export.

Sections
--------
1. Salary parsing
2. Location normalisation
3. Skill standardisation
4. Text cleaning
5. DataFrame utilities
"""

import re
import ast
from typing import Optional

import pandas as pd

# ---------------------------------------------------------------------------
# 1. Salary parsing
# ---------------------------------------------------------------------------

_SALARY_NUM_RE = re.compile(r"\d[\d,\.]*")
_LPA_RE = re.compile(r"(\d+(?:\.\d+)?)\s*-?\s*(\d+(?:\.\d+)?)?\s*lpa", re.I)
_K_RE = re.compile(r"(\d+(?:\.\d+)?)\s*k", re.I)


def parse_salary_to_usd(raw: str) -> Optional[float]:
    """
    Parse a raw salary string into an approximate USD annual figure.

    Handles patterns like:
        "3 - 8 LPA", "₹6,00,000 - ₹12,00,000", "$80K - $120K",
        "80000", "Not Mentioned", "N/A"

    Returns None when salary is not parseable.
    """
    if pd.isna(raw) or str(raw).strip().lower() in {"n/a", "not mentioned", "not written", ""}:
        return None

    text = str(raw).replace(",", "").replace("₹", "").replace("$", "").strip()

    # Indian LPA (Lakhs Per Annum) → USD
    lpa_match = _LPA_RE.search(text)
    if lpa_match:
        low = float(lpa_match.group(1)) * 100_000 / 83  # approx INR→USD
        high = float(lpa_match.group(2)) * 100_000 / 83 if lpa_match.group(2) else low
        return round((low + high) / 2, 2)

    # $80K style
    k_matches = _K_RE.findall(text)
    if k_matches:
        values = [float(v) * 1_000 for v in k_matches]
        return round(sum(values) / len(values), 2)

    # Raw numbers
    nums = [float(n) for n in _SALARY_NUM_RE.findall(text)]
    if nums:
        # If numbers look like annual Indian rupees (> 50 000), convert
        avg = sum(nums) / len(nums)
        if avg > 100_000:
            return round(avg / 83, 2)  # INR → USD
        return round(avg, 2)

    return None


def salary_band(annual_usd: Optional[float]) -> str:
    """Bucket a USD annual salary into a human-readable band."""
    if annual_usd is None or pd.isna(annual_usd):
        return "Not Disclosed"
    if annual_usd < 40_000:
        return "< $40K"
    if annual_usd < 70_000:
        return "$40K – $70K"
    if annual_usd < 100_000:
        return "$70K – $100K"
    if annual_usd < 140_000:
        return "$100K – $140K"
    return "$140K+"


# ---------------------------------------------------------------------------
# 2. Location normalisation
# ---------------------------------------------------------------------------

_CITY_ALIASES: dict[str, str] = {
    # India
    "bengaluru": "Bangalore",
    "bangalore": "Bangalore",
    "blr": "Bangalore",
    "mumbai": "Mumbai",
    "bombay": "Mumbai",
    "delhi": "Delhi",
    "new delhi": "Delhi",
    "ncr": "Delhi",
    "delhi / ncr": "Delhi",
    "hyderabad": "Hyderabad",
    "hyd": "Hyderabad",
    "pune": "Pune",
    "chennai": "Chennai",
    "madras": "Chennai",
    "kolkata": "Kolkata",
    "calcutta": "Kolkata",
    "ahmedabad": "Ahmedabad",
    # US
    "new york city": "New York",
    "nyc": "New York",
    "san francisco": "San Francisco",
    "sf": "San Francisco",
    "los angeles": "Los Angeles",
    "la": "Los Angeles",
    "seattle": "Seattle",
    "chicago": "Chicago",
    "austin": "Austin",
    "boston": "Boston",
    "remote": "Remote",
    "work from home": "Remote",
    "wfh": "Remote",
    "anywhere": "Remote",
}


def normalise_location(raw: str) -> str:
    """
    Map raw location strings to a canonical city name.

    Unknown cities are title-cased and returned as-is.
    """
    if pd.isna(raw) or str(raw).strip().lower() in {"n/a", "not mentioned", ""}:
        return "Unknown"

    cleaned = str(raw).strip()
    key = cleaned.lower()

    # Direct match
    if key in _CITY_ALIASES:
        return _CITY_ALIASES[key]

    # Prefix / partial match
    for alias, canonical in _CITY_ALIASES.items():
        if key.startswith(alias):
            return canonical

    # Strip parenthetical suffixes like "Bangalore (Bengaluru)"
    cleaned = re.sub(r"\(.*?\)", "", cleaned).strip()
    # Keep only the first token after a "/" or ","
    cleaned = re.split(r"[/,]", cleaned)[0].strip()

    return cleaned.title() if cleaned else "Unknown"


def extract_country(location: str) -> str:
    """
    Heuristically infer country from a normalised location string.
    Returns 'India', 'United States', or 'Other'.
    """
    india_cities = {
        "Bangalore", "Mumbai", "Delhi", "Hyderabad", "Pune",
        "Chennai", "Kolkata", "Ahmedabad", "Noida", "Gurgaon",
    }
    us_cities = {
        "New York", "San Francisco", "Los Angeles", "Seattle",
        "Chicago", "Austin", "Boston", "Atlanta", "Dallas",
    }
    loc = location.strip()
    if loc in india_cities:
        return "India"
    if loc in us_cities:
        return "United States"
    if loc == "Remote":
        return "Remote"
    return "Other"


# ---------------------------------------------------------------------------
# 3. Skill standardisation
# ---------------------------------------------------------------------------

_SKILL_ALIASES: dict[str, str] = {
    # Python ecosystem
    "python": "Python",
    "py": "Python",
    "python3": "Python",
    # SQL / Databases
    "sql": "SQL",
    "mysql": "MySQL",
    "postgresql": "PostgreSQL",
    "postgres": "PostgreSQL",
    "nosql": "NoSQL",
    "mongodb": "MongoDB",
    "mongo": "MongoDB",
    "oracle": "Oracle SQL",
    # Data tools
    "excel": "Excel",
    "ms excel": "Excel",
    "microsoft excel": "Excel",
    "power bi": "Power BI",
    "powerbi": "Power BI",
    "tableau": "Tableau",
    "looker": "Looker",
    "qlik": "QlikView",
    # Cloud
    "aws": "AWS",
    "amazon web services": "AWS",
    "gcp": "GCP",
    "google cloud": "GCP",
    "azure": "Azure",
    "microsoft azure": "Azure",
    # Big data / Engineering
    "spark": "Apache Spark",
    "apache spark": "Apache Spark",
    "pyspark": "PySpark",
    "hadoop": "Hadoop",
    "kafka": "Apache Kafka",
    "airflow": "Apache Airflow",
    "dbt": "dbt",
    "data build tool": "dbt",
    # ML / AI
    "machine learning": "Machine Learning",
    "ml": "Machine Learning",
    "deep learning": "Deep Learning",
    "dl": "Deep Learning",
    "nlp": "NLP",
    "natural language processing": "NLP",
    "tensorflow": "TensorFlow",
    "pytorch": "PyTorch",
    "scikit-learn": "scikit-learn",
    "sklearn": "scikit-learn",
    # Languages
    "r": "R",
    "java": "Java",
    "scala": "Scala",
    "javascript": "JavaScript",
    "js": "JavaScript",
    # BI / Reporting
    "sas": "SAS",
    "spss": "SPSS",
    "pandas": "pandas",
    "numpy": "NumPy",
    "matplotlib": "Matplotlib",
    "seaborn": "Seaborn",
}

_TECH_CATEGORIES: dict[str, str] = {
    "Python": "Programming",
    "SQL": "Databases",
    "MySQL": "Databases",
    "PostgreSQL": "Databases",
    "NoSQL": "Databases",
    "MongoDB": "Databases",
    "Oracle SQL": "Databases",
    "Excel": "Analyst Tools",
    "Power BI": "Analyst Tools",
    "Tableau": "Analyst Tools",
    "Looker": "Analyst Tools",
    "QlikView": "Analyst Tools",
    "AWS": "Cloud",
    "GCP": "Cloud",
    "Azure": "Cloud",
    "Apache Spark": "Big Data",
    "PySpark": "Big Data",
    "Hadoop": "Big Data",
    "Apache Kafka": "Big Data",
    "Apache Airflow": "Big Data",
    "dbt": "Big Data",
    "Machine Learning": "AI/ML",
    "Deep Learning": "AI/ML",
    "NLP": "AI/ML",
    "TensorFlow": "AI/ML",
    "PyTorch": "AI/ML",
    "scikit-learn": "AI/ML",
    "R": "Programming",
    "Java": "Programming",
    "Scala": "Programming",
    "JavaScript": "Programming",
    "SAS": "Analyst Tools",
    "SPSS": "Analyst Tools",
    "pandas": "Programming",
    "NumPy": "Programming",
    "Matplotlib": "Programming",
    "Seaborn": "Programming",
}


def standardise_skill(raw_skill: str) -> str:
    """Normalise a single skill token to its canonical name."""
    key = str(raw_skill).strip().lower()
    return _SKILL_ALIASES.get(key, raw_skill.strip().title())


def parse_skills_list(raw: str) -> list[str]:
    """
    Parse a raw skills string (comma-separated or Python list literal)
    into a clean list of standardised skill names.
    """
    if pd.isna(raw) or str(raw).strip().lower() in {"n/a", "not available", ""}:
        return []

    text = str(raw).strip()

    # Python list literal from the HuggingFace dataset
    if text.startswith("["):
        try:
            skills = ast.literal_eval(text)
            if isinstance(skills, list):
                return [standardise_skill(s) for s in skills if s]
        except (ValueError, SyntaxError):
            pass

    # Comma-separated string
    return [standardise_skill(s) for s in text.split(",") if s.strip()]


def get_skill_category(skill: str) -> str:
    """Return the technology category for a standardised skill, or 'Other'."""
    return _TECH_CATEGORIES.get(skill, "Other")


# ---------------------------------------------------------------------------
# 4. Text cleaning
# ---------------------------------------------------------------------------

def clean_text(text: str) -> str:
    """Strip extra whitespace and non-printable characters."""
    if pd.isna(text):
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


def infer_experience_level(raw: str) -> str:
    """
    Map a raw experience string (e.g. '2 - 5 Years', '0-1 yrs') to a
    standardised level label.
    """
    if pd.isna(raw) or str(raw).strip().lower() in {"n/a", "not mentioned", ""}:
        return "Not Specified"

    text = str(raw).lower()
    nums = re.findall(r"\d+(?:\.\d+)?", text)

    if not nums:
        if "fresher" in text or "entry" in text:
            return "Entry Level"
        if "senior" in text or "lead" in text:
            return "Senior"
        return "Not Specified"

    low = float(nums[0])
    high = float(nums[1]) if len(nums) > 1 else low
    mid = (low + high) / 2

    if mid <= 1:
        return "Entry Level"
    if mid <= 3:
        return "Junior"
    if mid <= 6:
        return "Mid Level"
    if mid <= 10:
        return "Senior"
    return "Lead / Principal"


# ---------------------------------------------------------------------------
# 5. DataFrame utilities
# ---------------------------------------------------------------------------

def explode_skills(df: pd.DataFrame, skills_col: str = "required_skills") -> pd.DataFrame:
    """
    Explode a DataFrame so each row corresponds to one skill.

    *skills_col* should contain either comma-separated strings or lists.
    Returns a copy with an additional 'skill' column.
    """
    df = df.copy()
    df["_skill_list"] = df[skills_col].apply(parse_skills_list)
    df = df.explode("_skill_list").rename(columns={"_skill_list": "skill"})
    df = df[df["skill"].notna() & (df["skill"] != "")]
    return df.reset_index(drop=True)


def compute_skill_demand(df_exploded: pd.DataFrame, total_jobs: int) -> pd.DataFrame:
    """
    Aggregate skill demand from an exploded DataFrame.

    Returns a DataFrame with columns:
        skill, job_count, pct_of_jobs, avg_salary_usd, skill_category
    """
    agg = (
        df_exploded
        .groupby("skill")
        .agg(
            job_count=("skill", "count"),
            avg_salary_usd=("salary_usd", "mean"),
        )
        .reset_index()
    )
    agg["pct_of_jobs"] = (agg["job_count"] / total_jobs * 100).round(2)
    agg["avg_salary_usd"] = agg["avg_salary_usd"].round(2)
    agg["skill_category"] = agg["skill"].apply(get_skill_category)
    return agg.sort_values("job_count", ascending=False).reset_index(drop=True)


def top_skills(df_exploded: pd.DataFrame, n: int = 20) -> pd.DataFrame:
    """Return the top *n* most frequent skills."""
    return (
        df_exploded["skill"]
        .value_counts()
        .head(n)
        .reset_index()
        .rename(columns={"index": "skill", "skill": "job_count", "count": "job_count"})
    )

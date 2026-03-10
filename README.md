# Job Market Intelligence

A data analytics project that scrapes job postings, cleans the data, and builds insights about what skills are in demand and what they pay.

## What It Does

I built this to answer questions like:
- Which skills pay the most for data analysts?
- Where are companies hiring?
- What's the most common skill combination employers want?

The project works in three stages:
1. **Scrape** job listings from TimesJobs (or use the HuggingFace dataset with 785K+ postings)
2. **Clean** the messy data — standardize skills, parse salaries, normalize locations
3. **Analyze** and export a dashboard-ready CSV for Power BI or Tableau

## Quick Findings

**Top 5 Skills for Data Analysts:**
- SQL (51% of postings, $90K median)
- Excel (41%, $84K)
- Python (27%, $98K)  
- Tableau (28%, $92K)
- SAS (19%, $94K)

**Highest-paid skills** like dbt, Scala, and Apache Spark command $115K-$130K — well above the median.

## How to Run It

**Setup:**
```bash
# Clone and navigate
git clone https://github.com/tarak510605/job-market-intelligence.git
cd job-market-intelligence

# Set up virtual environment
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# Install libraries
pip install pandas numpy matplotlib seaborn beautifulsoup4 requests datasets jupyter
```

**Run the pipeline:**
```bash
# Optional: Scrape fresh data
python scraping/job_scraper.py --job "Data Analyst" --pages 5

# Clean the data
python analysis/data_cleaning.py

# Generate dashboard CSV
python dashboards/build_dashboard.py
```

**Explore the analysis:**
```bash
jupyter notebook analysis/exploratory_analysis.ipynb
```

## What's Inside

- **`scraping/`** — Web scraper for TimesJobs
- **`analysis/`** — Data cleaning script + Jupyter notebooks with visualizations
- **`dashboards/`** — Script that builds a flat CSV for Power BI/Tableau
- **`utils/`** — Helper functions for parsing salaries, normalizing locations, etc.

## Tech Stack

Python • pandas • BeautifulSoup • Jupyter • Matplotlib • Seaborn • HuggingFace Datasets

---

**Note:** The scraper is optional. If you skip it, the dashboard script automatically downloads the HuggingFace dataset with 785K job postings.

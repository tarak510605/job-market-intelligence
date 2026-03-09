# Job Market Intelligence Dashboard

> **An end-to-end data pipeline** — from web scraping to interactive dashboard — analysing 785 000+ real job postings to surface what skills pay the most, where hiring is booming, and which technologies are trending.

---

## Table of Contents
1. [Project Overview](#project-overview)
2. [Architecture](#architecture)
3. [Repository Structure](#repository-structure)
4. [Insights Discovered](#insights-discovered)
5. [Tools & Technologies](#tools--technologies)
6. [Quick Start](#quick-start)
7. [Running Each Component](#running-each-component)
8. [Dashboard Import Guide](#dashboard-import-guide)
9. [Dataset Source](#dataset-source)
10. [Project Motivation](#project-motivation)

---

## Project Overview

The **Job Market Intelligence Dashboard** is a portfolio-grade data analytics project that demonstrates the full data lifecycle:

| Stage | What it does |
|-------|-------------|
| **Scraping** | Collects live job postings from TimesJobs using a modular, rate-limited scraper |
| **Cleaning** | Standardises skills, normalises locations, parses salary strings, and removes duplicates |
| **EDA** | Explores hiring trends, salary distributions, top-demanded skills, and remote-work patterns |
| **Skill Analysis** | Tracks monthly skill demand trends, co-occurrence patterns, and salary-vs-demand trade-offs |
| **Dashboard Export** | Produces a flat aggregated CSV ready for Power BI / Tableau |

The analysis draws on the publicly available **`lukebarousse/data_jobs`** Hugging Face dataset (~785 000 LinkedIn postings from 2023) for depth, while the scraping pipeline enables fresh, real-time data collection.

---

## Architecture

```
Raw website         scraping/job_scraper.py
    │
    ▼
data/raw/           jobs_raw.csv              (scraped)
    │
    ▼
analysis/           data_cleaning.py
    │
    ▼
data/cleaned/       jobs_cleaned.csv          (standardised)
    │
    ▼
analysis/           exploratory_analysis.ipynb
                    skill_trends_analysis.ipynb
    │
    ▼
data/processed/     skill_summary.csv         (ranked skills)
    │
    ▼
dashboards/         build_dashboard.py
    │
    ▼
dashboards/         dashboard_dataset.csv     (Power BI / Tableau ready)
```

---

## Repository Structure

```
JobMarketIntelligence/
│
├── data/
│   ├── raw/                    ← scraped output lands here
│   ├── cleaned/                ← post-cleaning CSV
│   └── processed/              ← aggregated analytics tables
│
├── scraping/
│   └── job_scraper.py          ← modular TimesJobs scraper
│
├── analysis/
│   ├── data_cleaning.py        ← full cleaning pipeline
│   ├── exploratory_analysis.ipynb   ← EDA with 9 chart sections
│   └── skill_trends_analysis.ipynb  ← trends, co-occurrence, optimal skills
│
├── dashboards/
│   ├── build_dashboard.py      ← aggregation script
│   └── dashboard_dataset.csv   ← generated (run build_dashboard.py)
│
├── utils/
│   └── helpers.py              ← reusable parsing & normalisation functions
│
├── requirements.txt
└── README.md
```

---

## Insights Discovered

### Top Skills for Data Analysts (US, 2023)
| Rank | Skill | % of Postings | Median Salary |
|------|-------|--------------|--------------|
| 1 | SQL | 51% | $90K |
| 2 | Excel | 41% | $84K |
| 3 | Python | 27% | $98K |
| 4 | Tableau | 28% | $92K |
| 5 | SAS | 19% | $94K |

### Highest-Paid Skills
> **dbt, Scala, Apache Spark, Go, Hadoop** command median salaries of **$115K–$130K** — well above the Data Analyst median of ~$90K.

### Key Findings
- **Remote roles** account for a significant share of US Data Analyst postings, with "Anywhere" being the top location by volume.
- **Atlanta, Dallas, and New York** are the top three metro areas for Data Analyst hiring.
- **SQL + Excel + Python** is the single most common skill trio in job requirements.
- **Python and cloud skills (AWS, Azure)** show the strongest year-over-year demand growth among Data Analysts.
- Salary disclosure rates are low (~30% of postings) — roles that disclose salary tend to be senior or specialised.

---

## Tools & Technologies

| Category | Tools |
|----------|-------|
| **Language** | Python 3.11 |
| **Scraping** | `requests`, `BeautifulSoup4`, `lxml` |
| **Data Engineering** | `pandas`, `numpy` |
| **Visualisation** | `matplotlib`, `seaborn`, `adjustText` |
| **Dataset** | Hugging Face `datasets` (`lukebarousse/data_jobs`) |
| **Notebooks** | Jupyter, ipykernel |
| **Dashboard** | Power BI / Tableau (CSV import) |
| **Version Control** | Git + GitHub |

---

## Quick Start

### 1. Clone the repository
```bash
git clone https://github.com/your-username/JobMarketIntelligence.git
cd JobMarketIntelligence
```

### 2. Create a virtual environment
```bash
python -m venv .venv
source .venv/bin/activate        # macOS / Linux
.venv\Scripts\activate           # Windows
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Run the full pipeline
```bash
# Step 1 — Scrape (optional; uses HuggingFace dataset as fallback)
python scraping/job_scraper.py --job "Data Analyst" --pages 5

# Step 2 — Clean
python analysis/data_cleaning.py

# Step 3 — Build dashboard CSV
python dashboards/build_dashboard.py
```

### 5. Open the notebooks
```bash
jupyter notebook analysis/exploratory_analysis.ipynb
jupyter notebook analysis/skill_trends_analysis.ipynb
```

---

## Running Each Component

### Scraper

```bash
# Basic usage
python scraping/job_scraper.py

# Custom search
python scraping/job_scraper.py --job "Data Engineer" --pages 10 --delay 2.0

# Options
#   --job     Job title / keyword   (default: "Data Analyst")
#   --pages   Max result pages      (default: 5)
#   --output  CSV output path       (default: data/raw/jobs_raw.csv)
#   --delay   Seconds between pages (default: 1.5)
```

### Data Cleaning

```bash
python analysis/data_cleaning.py

# Custom paths
python analysis/data_cleaning.py \
    --input  data/raw/jobs_raw.csv \
    --output data/cleaned/jobs_cleaned.csv
```

### Dashboard Dataset

```bash
python dashboards/build_dashboard.py

# If cleaned CSV exists it will be used automatically.
# Otherwise the script falls back to the Hugging Face dataset.
```

---

## Dashboard Import Guide

### Power BI
1. Open Power BI Desktop → **Get Data** → **Text/CSV**
2. Select `dashboards/dashboard_dataset.csv`
3. Use **skill**, **role**, **city**, and **experience_level** as slicers
4. Create measures: `SUM(job_count)`, `AVERAGE(avg_salary)`, `MAX(pct_of_postings)`

### Tableau
1. **Connect** → **Text File** → select `dashboard_dataset.csv`
2. Drag `skill` to Rows, `job_count` to Columns for a horizontal bar
3. Use `city` and `role` as Filters
4. Create a calculated field: `AVG([avg_salary])` for salary KPIs

---

## Dataset Source

The analysis notebooks use the **`lukebarousse/data_jobs`** dataset:
- ~785 000 real job postings scraped from LinkedIn in 2023
- Covers 10+ data roles across 100+ countries
- Includes salary, skills, location, and remote-work fields

```python
from datasets import load_dataset
df = load_dataset("lukebarousse/data_jobs")["train"].to_pandas()
```

---

## Project Motivation

This project was built to demonstrate end-to-end data engineering and analytics skills in a single, cohesive portfolio piece. It intentionally covers:

- **Systems thinking** — modular, reusable code with clear separation of concerns
- **Data quality** — robust cleaning that handles real-world messiness (missing salaries, inconsistent skill names, varied location formats)
- **Analytical depth** — going beyond bar charts to co-occurrence analysis, trend decomposition, and salary–demand trade-off modelling
- **Stakeholder output** — producing a dashboard-ready dataset that a business user can load and explore without touching Python

---

*Built with Python 3.11 · pandas · seaborn · Hugging Face Datasets*

"""
Job Market Intelligence Dashboard — Scraping Pipeline
======================================================
Scrapes job listings from TimesJobs.com and saves raw data to
data/raw/jobs_raw.csv.

Design principles:
  - Modular scraper class with a clean public interface
  - Rotating User-Agent header to reduce blocking
  - Configurable delay between requests (rate-limiting)
  - Structured logging to stdout
  - No global state; all config passed via arguments

Usage:
    python scraping/job_scraper.py --job "Data Analyst" --pages 5
    python scraping/job_scraper.py --job "Data Engineer" --pages 10 --output data/raw/data_engineer_raw.csv
"""

import argparse
import logging
import os
import time
from datetime import datetime
from urllib.parse import urlencode

import pandas as pd
import requests
from bs4 import BeautifulSoup

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
TIMESJOBS_BASE_URL = "https://www.timesjobs.com/candidate/job-search.html"

_USER_AGENTS = [
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
]

REQUEST_DELAY = 1.5   # seconds between page requests
REQUEST_TIMEOUT = 12  # seconds before a request times out


# ---------------------------------------------------------------------------
# Base class
# ---------------------------------------------------------------------------
class JobScraper:
    """Abstract base class for job-portal scrapers."""

    source_portal: str = "Unknown"

    def scrape(self, query: str, max_pages: int = 5) -> list[dict]:
        """Run the scraper and return a list of job records."""
        raise NotImplementedError

    @staticmethod
    def save_to_csv(records: list[dict], filepath: str) -> None:
        """Persist *records* to a CSV file, creating parent dirs as needed."""
        if not records:
            logger.warning("No records to save — output file not created.")
            return
        os.makedirs(os.path.dirname(os.path.abspath(filepath)), exist_ok=True)
        df = pd.DataFrame(records)
        df.to_csv(filepath, index=False, encoding="utf-8")
        logger.info("Saved %d records → %s", len(df), filepath)


# ---------------------------------------------------------------------------
# TimesJobs scraper
# ---------------------------------------------------------------------------
class TimesJobsScraper(JobScraper):
    """
    Scrapes job listings from TimesJobs.com using requests + BeautifulSoup.

    Collected fields
    ----------------
    job_title, company_name, location, salary, required_skills,
    experience_level, job_description, posting_date, source_url,
    source_portal, scraped_date
    """

    source_portal = "TimesJobs"

    def __init__(self, delay: float = REQUEST_DELAY):
        self.delay = delay
        self._ua_index = 0
        self.session = requests.Session()
        self._rotate_ua()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _rotate_ua(self) -> None:
        ua = _USER_AGENTS[self._ua_index % len(_USER_AGENTS)]
        self.session.headers.update({"User-Agent": ua})
        self._ua_index += 1

    def _build_url(self, query: str, sequence: int) -> str:
        params = {
            "searchType": "personalizedSearch",
            "from": "submit",
            "txtKeywords": query,
            "txtLocation": "",
            "sequence": sequence,
            "startPage": 1,
        }
        return f"{TIMESJOBS_BASE_URL}?{urlencode(params)}"

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------
    def _parse_page(self, soup: BeautifulSoup, scraped_date: str) -> list[dict]:
        """Extract all job cards from a single result page."""
        jobs: list[dict] = []
        listings = soup.find_all("li", class_="clearfix job-bx wht-shd-bx")

        for card in listings:
            try:
                record = self._parse_card(card, scraped_date)
                if record:
                    jobs.append(record)
            except Exception as exc:
                logger.debug("Skipped malformed card: %s", exc)

        return jobs

    def _parse_card(self, card: BeautifulSoup, scraped_date: str) -> dict | None:
        """Parse a single job card element into a flat dictionary."""
        # Title + URL
        title_tag = card.find("h2")
        if not title_tag:
            return None
        job_title = title_tag.get_text(strip=True)
        anchor = title_tag.find("a")
        source_url = anchor["href"].strip() if anchor and anchor.get("href") else "N/A"

        # Company
        company_tag = card.find("h3", class_="joblist-comp-name")
        company_name = company_tag.get_text(strip=True) if company_tag else "N/A"

        # Location — first plain <span> inside the card
        location_spans = [
            s for s in card.find_all("span")
            if not s.get("class") and s.find_parent("li")
        ]
        location = location_spans[0].get_text(strip=True) if location_spans else "N/A"

        # Required skills
        skills_tag = card.find("span", class_="srp-skills")
        if skills_tag:
            parts = [s.strip() for s in skills_tag.get_text().split(",") if s.strip()]
            required_skills = ", ".join(parts) if parts else "N/A"
        else:
            required_skills = "N/A"

        # Experience (look for material icon list)
        experience_level = "N/A"
        detail_list = card.find("ul", class_="top-jd-dtl")
        if detail_list:
            for li in detail_list.find_all("li"):
                text = li.get_text(strip=True)
                if text and text != "N/A":
                    experience_level = text
                    break

        # Salary (TimesJobs rarely shows salary; store if present)
        salary_icon = card.find("i", class_=lambda c: c and "money" in c)
        if salary_icon:
            parent = salary_icon.find_parent()
            salary = parent.get_text(strip=True) if parent else "N/A"
        else:
            salary = "N/A"

        # Posting date
        posted_tag = card.find("span", class_="sim-posted")
        posting_date = posted_tag.get_text(strip=True) if posted_tag else "N/A"

        # Short job description (first 300 chars to avoid bloat)
        desc_header = card.find("header")
        job_description = "N/A"
        if desc_header:
            sibling = desc_header.find_next_sibling()
            if sibling:
                job_description = sibling.get_text(separator=" ", strip=True)[:300]

        return {
            "job_title": job_title,
            "company_name": company_name,
            "location": location,
            "salary": salary,
            "required_skills": required_skills,
            "experience_level": experience_level,
            "job_description": job_description,
            "posting_date": posting_date,
            "source_url": source_url,
            "source_portal": self.source_portal,
            "scraped_date": scraped_date,
        }

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------
    def scrape(self, query: str, max_pages: int = 5) -> list[dict]:
        """
        Scrape up to *max_pages* of TimesJobs results for *query*.

        Returns a list of job-record dicts.  Stops early when a page
        returns no results (end of results reached).
        """
        all_jobs: list[dict] = []
        scraped_date = datetime.today().strftime("%Y-%m-%d")
        logger.info(
            "Starting scrape  query=%r  max_pages=%d  portal=%s",
            query, max_pages, self.source_portal,
        )

        for sequence in range(1, max_pages + 1):
            url = self._build_url(query, sequence)
            logger.info("Fetching page %d/%d …", sequence, max_pages)
            self._rotate_ua()

            try:
                response = self.session.get(url, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()
            except requests.Timeout:
                logger.error("Request timed out on page %d — stopping.", sequence)
                break
            except requests.HTTPError as exc:
                logger.error("HTTP %s on page %d — stopping.", exc.response.status_code, sequence)
                break
            except requests.RequestException as exc:
                logger.error("Network error on page %d: %s — stopping.", sequence, exc)
                break

            soup = BeautifulSoup(response.text, "lxml")
            page_jobs = self._parse_page(soup, scraped_date)

            if not page_jobs:
                logger.info("No listings on page %d — end of results.", sequence)
                break

            all_jobs.extend(page_jobs)
            logger.info(
                "Page %d: +%d jobs  (running total: %d)",
                sequence, len(page_jobs), len(all_jobs),
            )
            time.sleep(self.delay)

        logger.info("Scrape finished.  Total records collected: %d", len(all_jobs))
        return all_jobs


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Job Market Intelligence — Web Scraper",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--job",
        default="Data Analyst",
        help="Job title / keyword to search for.",
    )
    parser.add_argument(
        "--pages",
        type=int,
        default=5,
        help="Maximum number of result pages to fetch.",
    )
    parser.add_argument(
        "--output",
        default="data/raw/jobs_raw.csv",
        help="Output CSV path (relative to project root).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=REQUEST_DELAY,
        help="Seconds to wait between page requests.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    scraper = TimesJobsScraper(delay=args.delay)
    records = scraper.scrape(query=args.job, max_pages=args.pages)
    TimesJobsScraper.save_to_csv(records, args.output)


if __name__ == "__main__":
    main()

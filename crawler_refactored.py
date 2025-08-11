import os
import math
import time
import csv
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry

# -----------------------
# Config
# -----------------------
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")  # put your token in the env, not in code
if not GITHUB_TOKEN:
    raise RuntimeError("Set GITHUB_TOKEN env var.")

OUTPUT_DIR = Path(r"C:/Thesis V3/Output")
OUTPUT_CSV = Path(r"C:/Thesis V3/repositories-for-microservices.csv")
OUTPUT_XLSX = Path(r"C:/Thesis V3/repositories-summary.xlsx")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TOPIC = "microservices"
BASE_SEARCH_URL = "https://api.github.com/search/repositories"
PER_PAGE = 100
DELAY_BETWEEN_PAGES = 5  # be gentle
MAX_RESULTS_PER_QUERY = 1000  # GitHub search cap
DATE_STEP_DAYS = 182  # ~6 months

# Logging
logging.basicConfig(
    format="%(asctime)s %(levelname)s: %(message)s",
    level=logging.INFO
)

# -----------------------
# HTTP Session w/ retries
# -----------------------
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "repo-crawler-thesis"
    })
    retries = Retry(
        total=5,
        backoff_factor=1.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

SESSION = make_session()

# -----------------------
# Helpers
# -----------------------
def rate_limit_sleep(resp: requests.Response) -> None:
    """Sleep if we hit secondary rate limits, otherwise be nice between pages."""
    if resp.status_code == 403:
        # Could be a rate limit — respect reset if provided.
        reset = resp.headers.get("X-RateLimit-Reset")
        if reset:
            reset_ts = int(reset)
            wait = max(0, reset_ts - int(time.time()) + 3)
            logging.warning("Rate limit hit. Sleeping %ss until reset.", wait)
            time.sleep(wait)

def search_count(start: datetime, end: datetime) -> int:
    """Return total_count for the date window."""
    params = {
        "q": f"topic:{TOPIC} created:{start:%Y-%m-%d}..{end:%Y-%m-%d}",
        "per_page": 1
    }
    resp = SESSION.get(BASE_SEARCH_URL, params=params)
    rate_limit_sleep(resp)
    resp.raise_for_status()
    return resp.json().get("total_count", 0)

def iter_search_pages(start: datetime, end: datetime) -> Iterable[Dict]:
    """Yield items for a date window, paging."""
    params = {
        "q": f"topic:{TOPIC} created:{start:%Y-%m-%d}..{end:%Y-%m-%d}",
        "per_page": PER_PAGE,
        "sort": "stars",  # stable-ish ordering, optional
        "order": "desc"
    }
    # first request to get total_count
    resp = SESSION.get(BASE_SEARCH_URL, params=params)
    rate_limit_sleep(resp)
    resp.raise_for_status()
    data = resp.json()
    total_count = data.get("total_count", 0)
    pages = math.ceil(min(total_count, MAX_RESULTS_PER_QUERY) / PER_PAGE)
    logging.info("Window %s..%s -> total_count=%d, pages=%d",
                 start.date(), end.date(), total_count, pages)

    # page 1
    for item in data.get("items", []):
        yield item

    # remaining pages
    for page in range(2, pages + 1):
        params["page"] = page
        resp = SESSION.get(BASE_SEARCH_URL, params=params)
        rate_limit_sleep(resp)
        resp.raise_for_status()
        data = resp.json()
        for item in data.get("items", []):
            yield item
        time.sleep(DELAY_BETWEEN_PAGES)

def split_window_if_needed(start: datetime, end: datetime) -> List[Tuple[datetime, datetime]]:
    """
    If a window exceeds GitHub's 1000-result cap, split it recursively.
    Returns a list of (start, end) windows that are safe to fetch.
    """
    cnt = search_count(start, end)
    if cnt <= MAX_RESULTS_PER_QUERY:
        return [(start, end)]
    # split in half
    midpoint = start + (end - start) / 2
    left = split_window_if_needed(start, midpoint)
    right = split_window_if_needed(midpoint + timedelta(days=1), end)
    return left + right

def zip_download_url(full_name: str, default_branch: str) -> str:
    # Use the zipball endpoint (auth-friendly)
    return f"https://api.github.com/repos/{full_name}/zipball/{default_branch}"

def download_zip(full_name: str, default_branch: str, out_path: Path) -> None:
    url = zip_download_url(full_name, default_branch)
    with SESSION.get(url, stream=True) as r:
        rate_limit_sleep(r)
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 15):
                if chunk:
                    f.write(chunk)

# -----------------------
# Main
# -----------------------
def crawl(
    start_date: datetime,
    finish_date: datetime,
) -> None:
    summary_rows: List[List] = []
    total_downloaded = 0
    total_failed = 0

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["Username", "Repository Name", "Full Name", "Clone URL", "Default Branch", "Topics", "Download Status", "Zip Path"])

        current_start = start_date
        while current_start <= finish_date:
            current_end = min(current_start + timedelta(days=DATE_STEP_DAYS), finish_date)
            # split further if needed to stay under 1000 results
            windows = split_window_if_needed(current_start, current_end)
            period_downloaded = 0
            period_failed = 0
            page_count_estimate = 0

            for win_start, win_end in windows:
                # estimate pages (after potential split)
                tc = search_count(win_start, win_end)
                page_count_estimate += math.ceil(min(tc, MAX_RESULTS_PER_QUERY) / PER_PAGE)

                logging.info("Processing %s .. %s", win_start.date(), win_end.date())
                for item in iter_search_pages(win_start, win_end):
                    owner = item["owner"]["login"]
                    repo = item["name"]
                    full_name = item["full_name"]
                    clone_url = item["clone_url"]
                    default_branch = item.get("default_branch") or "main"
                    topics = item.get("topics", [])  # already filtered by topic, but keeping for metadata

                    # Build output filename
                    zip_name = f"{full_name.replace('/', '#')}@{default_branch}.zip"
                    zip_path = OUTPUT_DIR / zip_name

                    try:
                        download_zip(full_name, default_branch, zip_path)
                        writer.writerow([owner, repo, full_name, clone_url, default_branch, ";".join(topics), "downloaded", str(zip_path)])
                        period_downloaded += 1
                        total_downloaded += 1
                    except Exception as e:
                        logging.warning("Failed to download %s (%s): %s", full_name, default_branch, e)
                        writer.writerow([owner, repo, full_name, clone_url, default_branch, ";".join(topics), "error", str(zip_path)])
                        period_failed += 1
                        total_failed += 1

            summary_rows.append([
                f"{current_start:%Y-%m-%d}",
                f"{current_end:%Y-%m-%d}",
                period_downloaded,
                page_count_estimate,
                period_failed
            ])
            logging.info("Window %s..%s done: downloaded=%d failed=%d",
                         current_start.date(), current_end.date(), period_downloaded, period_failed)

            current_start = current_end + timedelta(days=1)

    # Save summary
    df = pd.DataFrame(summary_rows, columns=[
        "Start Date", "End Date", "Downloaded Repositories", "Estimated Pages", "Failed Downloads"
    ])
    df.to_excel(OUTPUT_XLSX, index=False)
    logging.info("DONE. Total downloaded=%d, failed=%d. Summary saved to %s",
                 total_downloaded, total_failed, OUTPUT_XLSX)

if __name__ == "__main__":
    crawl(
        start_date=datetime(2020, 1, 1),
        finish_date=datetime(2021, 1, 30),
    )

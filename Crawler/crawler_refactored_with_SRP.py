# srp_github_microservices.py
from __future__ import annotations

import csv
import math
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd
import requests


# -----------------------------
# Configuration
# -----------------------------
@dataclass(frozen=True)
class AppConfig:
    github_token: str
    output_folder: Path
    output_csv_file: Path
    output_excel_file: Path
    base_url: str = "https://api.github.com/search/repositories"
    query: str = "topic:microservices"
    per_page: int = 100
    delay_between_pages_sec: int = 10
    start_date: datetime = datetime(2020, 1, 1)
    finish_date: datetime = datetime(2021, 1, 30)

    @staticmethod
    def from_env(
        output_folder: str,
        output_csv: str,
        output_excel: str,
        token_env_var: str = "GITHUB_TOKEN",
    ) -> "AppConfig":
        token = os.getenv(token_env_var, "").strip()
        if not token:
            raise ValueError(
                f"Missing GitHub token. Set environment variable {token_env_var}."
            )
        return AppConfig(
            github_token=token,
            output_folder=Path(output_folder),
            output_csv_file=Path(output_csv),
            output_excel_file=Path(output_excel),
        )


# -----------------------------
# Date range generator (6-month windows)
# -----------------------------
class DateRanges:
    def __init__(self, start: datetime, finish: datetime, step_days: int = 182) -> None:
        self.start = start
        self.finish = finish
        self.step = timedelta(days=step_days)

    def windows(self) -> Iterable[Tuple[datetime, datetime]]:
        start = self.start
        end = min(start + self.step, self.finish)
        while start <= self.finish:
            yield (start, min(end, self.finish))
            start = end + timedelta(days=1)
            end = min(start + self.step, self.finish)


# -----------------------------
# GitHub API client (search only)
# -----------------------------
class GitHubSearchClient:
    def __init__(self, cfg: AppConfig) -> None:
        self.cfg = cfg
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"token {cfg.github_token}",
                "Accept": "application/vnd.github+json",
                "User-Agent": "srp-github-microservices-script",
            }
        )

    def _get(self, url: str, params: Dict[str, str]) -> Dict:
        while True:
            resp = self.session.get(url, params=params, timeout=30)
            # Handle basic rate limiting/backoff
            if resp.status_code == 403 and "X-RateLimit-Remaining" in resp.headers:
                remaining = resp.headers.get("X-RateLimit-Remaining", "0")
                if remaining == "0":
                    reset = int(resp.headers.get("X-RateLimit-Reset", "0"))
                    sleep_for = max(0, reset - int(time.time()) + 5)
                    print(f"Rate limit hit. Sleeping {sleep_for}s…")
                    time.sleep(sleep_for)
                    continue
            resp.raise_for_status()
            return resp.json()

    def search_total_count(self, created_from: str, created_to: str) -> int:
        params = {
            "q": f'{self.cfg.query} created:{created_from}..{created_to}',
            "per_page": 1,  # minimal payload just to get total_count
            "page": 1,
        }
        data = self._get(self.cfg.base_url, params)
        return int(data.get("total_count", 0))

    def search_page(
        self, created_from: str, created_to: str, page: int
    ) -> List[Dict]:
        params = {
            "q": f'{self.cfg.query} created:{created_from}..{created_to}',
            "per_page": self.cfg.per_page,
            "page": page,
        }
        data = self._get(self.cfg.base_url, params)
        return data.get("items", [])


# -----------------------------
# CSV logger (repo metadata)
# -----------------------------
class CSVLogger:
    def __init__(self, csv_path: Path) -> None:
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        self.file = csv_path.open("w", newline="", encoding="utf-8")
        self.writer = csv.writer(self.file, delimiter=",")
        self.writer.writerow(["Username", "Repository Name", "URL", "Download Status"])

    def log(self, username: str, repo: str, url: str, status: str) -> None:
        self.writer.writerow([username, repo, url, status])

    def close(self) -> None:
        self.file.close()


# -----------------------------
# Downloader (zip archives)
# -----------------------------
class RepoDownloader:
    def __init__(self, output_dir: Path) -> None:
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._session = requests.Session()

    @staticmethod
    def _default_branch(item: Dict) -> str:
        # Prefer reported default_branch, fallback to "master"
        return item.get("default_branch") or "master"

    def _zip_url(self, item: Dict) -> str:
        # Use archive URL pattern: https://github.com/{full_name}/archive/refs/heads/{branch}.zip
        full_name = item["full_name"]
        branch = self._default_branch(item)
        return f"https://github.com/{full_name}/archive/refs/heads/{branch}.zip"

    def _zip_filename(self, item: Dict) -> Path:
        safe = item["full_name"].replace("/", "#") + ".zip"
        return self.output_dir / safe

    def download_zip(self, item: Dict) -> Tuple[bool, str]:
        url = self._zip_url(item)
        out_path = self._zip_filename(item)
        try:
            with self._session.get(url, stream=True, timeout=60) as r:
                r.raise_for_status()
                with out_path.open("wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            return True, "downloaded"
        except Exception as e:
            return False, f"error: {e}"


# -----------------------------
# Summary collector (Excel)
# -----------------------------
class SummaryCollector:
    def __init__(self) -> None:
        self.rows: List[List] = []

    def add_period(
        self,
        start_date: str,
        end_date: str,
        downloaded_count: int,
        pages: int,
        failed_downloads: int,
    ) -> None:
        self.rows.append(
            [start_date, end_date, downloaded_count, pages, failed_downloads]
        )

    def save_excel(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame(
            self.rows,
            columns=[
                "Start Date",
                "End Date",
                "Downloaded Repositories",
                "Number of Pages",
                "Number of Failed Downloads",
            ],
        )
        df.to_excel(path, index=False)


# -----------------------------
# Orchestrator (ties everything together)
# -----------------------------
class Pipeline:
    def __init__(self, cfg: AppConfig) -> None:
        self.cfg = cfg
        self.client = GitHubSearchClient(cfg)
        self.downloader = RepoDownloader(cfg.output_folder)
        self.summary = SummaryCollector()

    def run(self) -> None:
        total_processed = 0
        ranges = DateRanges(self.cfg.start_date, self.cfg.finish_date)
        with CSVLogger(self.cfg.output_csv_file) as csv_logger:  # type: ignore
            # Provide context manager methods dynamically
            pass

    # Provide context-manager support for CSVLogger without altering its SRP
CSVLogger.__enter__ = lambda self: self
CSVLogger.__exit__ = lambda self, exc_type, exc, tb: self.close()

class Pipeline(Pipeline):  # extend the class cleanly
    def run(self) -> None:
        total_processed = 0
        csv_logger = CSVLogger(self.cfg.output_csv_file)

        try:
            for period_start, period_end in DateRanges(
                self.cfg.start_date, self.cfg.finish_date
            ).windows():
                fs = period_start.strftime("%Y-%m-%d")
                fe = period_end.strftime("%Y-%m-%d")
                print(f"\n=== Processing {fs} .. {fe} ===")

                total_count = self.client.search_total_count(fs, fe)
                pages = int(math.ceil(total_count / float(self.cfg.per_page)))
                print(f"Total repos reported: {total_count} -> pages: {pages}")

                period_downloaded = 0
                period_failed = 0

                for page in range(1, pages + 1):
                    print(f"Page {page}/{pages}")
                    items = self.client.search_page(fs, fe, page)

                    # Iterate results
                    for item in items:
                        user = item["owner"]["login"]
                        repo = item["name"]
                        repo_url = item["clone_url"]

                        # Topics may not be included unless preview header; we rely on the query filter,
                        # but keep a defensive check if topics present:
                        topics = item.get("topics", [])
                        if topics and "microservices" not in topics:
                            print(f"Skipping {user}/{repo} (topic missing)")
                            continue

                        ok, status = self.downloader.download_zip(item)
                        if ok:
                            period_downloaded += 1
                        else:
                            period_failed += 1
                            print(f"Download failed for {user}/{repo}: {status}")

                        csv_logger.log(user, repo, repo_url, status)
                        total_processed += 1

                    # Page delay to respect secondary rate limits
                    if page < pages:
                        print(f"Cooling down {self.cfg.delay_between_pages_sec}s…")
                        time.sleep(self.cfg.delay_between_pages_sec)

                self.summary.add_period(fs, fe, period_downloaded, pages, period_failed)
                print(
                    f"Period done: downloaded={period_downloaded}, failed={period_failed}"
                )

        finally:
            csv_logger.close()

        print(f"\nDONE! Processed repositories: {total_processed}")
        self.summary.save_excel(self.cfg.output_excel_file)
        print(f"Summary saved to: {self.cfg.output_excel_file}")


# -----------------------------
# Entrypoint
# -----------------------------
if __name__ == "__main__":
    # Adjust these to your paths; token must be in env var GITHUB_TOKEN
    cfg = AppConfig.from_env(
        output_folder=r"C:\Thesis V3\Output",
        output_csv=r"C:\Thesis V3\repositories-for-microservices.csv",
        output_excel=r"C:\Thesis V3\repositories-summary.xlsx",
    )
    Pipeline(cfg).run()

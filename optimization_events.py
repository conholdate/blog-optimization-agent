#!/usr/bin/env python3
"""
Event-level optimization history.

The per-domain logs (logs/<brand>/blog_<domain>.csv) keep only the latest
`last_optimized` date per URL because the optimizer's recency gates read them.
This module keeps every optimization as its own row in a sibling file,
logs/<brand>/blog_<domain>_events.csv, so before/after metrics can be measured
for each event. One file per domain keeps parallel brand jobs from conflicting.

Backfill past events from the git history of the existing logs:
    python3 optimization_events.py backfill
"""

import argparse
import csv
import hashlib
import subprocess
import sys
from datetime import date
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

LOG_DIR = Path("logs")
EVENT_FIELDS = [
    "event_id",
    "optimized_on",
    "url",
    "slug",
    "domain",
    "recommended_action",
    "source",
]


def normalize_event_url(value: str) -> str:
    """Lowercase scheme/host and force a single trailing slash on the path."""
    text = (value or "").strip()
    parsed = urlsplit(text)
    if not (parsed.scheme and parsed.netloc):
        return text
    path = parsed.path.rstrip("/") + "/"
    return urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), path, "", ""))


def make_event_id(url: str, optimized_on: str) -> str:
    digest = hashlib.sha1(f"{normalize_event_url(url)}|{optimized_on}".encode("utf-8"))
    return digest.hexdigest()[:12]


def events_path_for_log(log_file_path: Path) -> Path:
    """logs/aspose/blog_aspose_com.csv -> logs/aspose/blog_aspose_com_events.csv"""
    log_file_path = Path(log_file_path)
    return log_file_path.with_name(f"{log_file_path.stem}_events.csv")


def load_events(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def load_all_events(log_dir: Path = LOG_DIR) -> list[dict]:
    events = []
    for path in sorted(log_dir.glob("*/*_events.csv")):
        events.extend(load_events(path))
    return events


def write_events(path: Path, events: list[dict]) -> None:
    events = sorted(events, key=lambda e: (e["optimized_on"], e["url"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=EVENT_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(events)


def append_optimization_event(
    log_file_path: Path,
    url: str,
    slug: str,
    domain: str,
    optimized_on: str | None = None,
    recommended_action: str = "",
    source: str = "agent",
) -> bool:
    """Record one optimization. Returns False if the same URL/date already exists."""
    optimized_on = optimized_on or str(date.today())
    url = normalize_event_url(url)
    path = events_path_for_log(log_file_path)
    event_id = make_event_id(url, optimized_on)

    if not path.exists():
        write_events(path, [])
    if any(e.get("event_id") == event_id for e in load_events(path)):
        return False

    with open(path, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=EVENT_FIELDS, extrasaction="ignore")
        writer.writerow({
            "event_id": event_id,
            "optimized_on": optimized_on,
            "url": url,
            "slug": slug,
            "domain": domain,
            "recommended_action": recommended_action or "",
            "source": source,
        })
    return True


# ----------------------------------------------------
# Backfill from git history
# ----------------------------------------------------

def parse_log_row(line: str) -> dict | None:
    """Parse a `slug,url,domain,last_optimized` row from a log diff line."""
    try:
        row = next(csv.reader([line]))
    except (csv.Error, StopIteration):
        return None
    if len(row) != 4 or row[0] == "slug":
        return None
    slug, url, domain, optimized_on = (value.strip() for value in row)
    try:
        date.fromisoformat(optimized_on)
    except ValueError:
        return None
    if not url.startswith("http"):
        return None
    return {"slug": slug, "url": url, "domain": domain.lower(), "optimized_on": optimized_on}


def collect_history_rows(repo_root: Path, log_dir: Path) -> list[dict]:
    """Every log row ever added in git history, plus rows in the working tree."""
    log = subprocess.run(
        ["git", "-C", str(repo_root), "log", "-p", "--no-color", "--format=", "--", f"{log_dir}/"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout

    rows = []
    for line in log.splitlines():
        if line.startswith("+") and not line.startswith("+++"):
            parsed = parse_log_row(line[1:])
            if parsed:
                rows.append(parsed)

    for path in log_dir.glob("**/*.csv"):
        if path.name.endswith("_events.csv"):
            continue
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                parsed = parse_log_row(line.rstrip("\n"))
                if parsed:
                    rows.append(parsed)
    return rows


def domain_log_paths(log_dir: Path) -> dict[str, Path]:
    """Map each domain to its existing per-domain log file."""
    mapping = {}
    for path in log_dir.glob("*/*.csv"):
        if path.name.endswith("_events.csv"):
            continue
        domain = path.stem.replace("_", ".")
        mapping[domain] = path
    return mapping


def backfill(repo_root: Path = Path("."), log_dir: Path = LOG_DIR) -> dict[str, int]:
    log_paths = domain_log_paths(log_dir)
    by_path: dict[Path, dict[str, dict]] = {}

    for row in collect_history_rows(repo_root, log_dir):
        log_path = log_paths.get(row["domain"])
        if not log_path:
            print(f"  No domain log for {row['domain']}; skipping {row['url']}")
            continue
        url = normalize_event_url(row["url"])
        event_id = make_event_id(url, row["optimized_on"])
        by_path.setdefault(events_path_for_log(log_path), {})[event_id] = {
            "event_id": event_id,
            "optimized_on": row["optimized_on"],
            "url": url,
            "slug": row["slug"],
            "domain": row["domain"],
            "recommended_action": "",
            "source": "git-backfill",
        }

    added = {}
    for path, history in by_path.items():
        existing = {e["event_id"]: e for e in load_events(path)}
        new_ids = set(history) - set(existing)
        merged = {**history, **existing}  # keep existing rows (they may carry an action code)
        write_events(path, list(merged.values()))
        added[str(path)] = len(new_ids)
    return added


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("backfill", help="Rebuild event files from git history of logs/")
    args = parser.parse_args(argv)

    if args.command == "backfill":
        added = backfill()
        total = 0
        for path, count in sorted(added.items()):
            print(f"{path}: {count} new event(s)")
            total += count
        print(f"Total new events: {total}; total events: {len(load_all_events())}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

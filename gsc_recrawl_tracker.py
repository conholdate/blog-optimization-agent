#!/usr/bin/env python3
"""
Track when optimized posts go live and when Google recrawls them.

Runs daily, after the blogs' deploy workflows. For every optimization event in
logs/*/*_events.csv it:

  1. Checks the blog's English sitemap. Once the URL's <lastmod> is on or after
     the optimization date, the optimized version is live.
  2. Resubmits the blog's sitemap in Search Console when recent optimizations
     have just gone live (needs the full `webmasters` scope; with a read-only
     login this step is skipped with a warning).
  3. Inspects the URL with the URL Inspection API until Google's last crawl
     falls after the optimization, and records that date.

State is kept in metrics/recrawl_status.csv, one row per event:

  status            waiting_deploy | waiting_recrawl | recrawled | not_recrawled
  live_seen_on      first daily check that found the optimized version in the
                    sitemap (blank if it was already live when tracking began)
  recrawled_on      date of Google's crawl after the optimization
  recrawl_exact     yes = an earlier check saw an older crawl, so this is the
                    first recrawl (to within a day). no = the first check
                    already showed a newer crawl; the first recrawl happened
                    on or before recrawled_on.
  days_to_recrawl   recrawled_on - optimized_on, filled only when exact

Usage:
    python3 gsc_recrawl_tracker.py              # daily run
    python3 gsc_recrawl_tracker.py authorize    # one-off: grant write access for sitemap submits
"""

import argparse
import csv
import json
import re
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

from optimization_events import load_all_events, normalize_event_url

STATUS_FILE = Path("metrics/recrawl_status.csv")
CREDENTIALS_FILE = Path("credentials.json")
CLIENT_SECRET_FILE = Path("client_secret.json")

WRITE_SCOPE = "https://www.googleapis.com/auth/webmasters"
READ_SCOPE = "https://www.googleapis.com/auth/webmasters.readonly"

RECENT_DAYS = 30  # only resubmit sitemaps for optimizations newer than this
ASSUME_LIVE_AFTER_DAYS = 14  # inspect even without a sitemap match after this long
GIVE_UP_AFTER_DAYS = 120  # stop inspecting if Google hasn't recrawled by then
LIVE_EXACT_DAYS = 3  # events this recent and already live get today as live_seen_on
DAILY_INSPECTIONS_PER_SITE = 1900  # API quota is 2,000 per property per day

FIELDS = [
    "event_id", "optimized_on", "domain", "url", "status",
    "live_seen_on", "sitemap_submitted_on",
    "last_crawl_time", "coverage_state", "last_inspected_on",
    "recrawled_on", "days_to_recrawl", "recrawl_exact",
]

LOC_LASTMOD = re.compile(r"<loc>\s*(.*?)\s*</loc>\s*(?:<lastmod>\s*(.*?)\s*</lastmod>)?", re.DOTALL)


# ----------------------------------------------------
# Storage
# ----------------------------------------------------

def read_status() -> dict[str, dict]:
    if not STATUS_FILE.exists():
        return {}
    with open(STATUS_FILE, "r", encoding="utf-8", newline="") as f:
        return {r["event_id"]: r for r in csv.DictReader(f)}


def write_status(rows: dict[str, dict]) -> None:
    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    ordered = sorted(rows.values(), key=lambda r: (r["optimized_on"], r["url"]))
    with open(STATUS_FILE, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(ordered)


# ----------------------------------------------------
# Sitemaps
# ----------------------------------------------------

def fetch_text(url: str) -> str:
    response = requests.get(url, timeout=120, headers={"User-Agent": "blog-optimization-agent recrawl tracker"})
    response.raise_for_status()
    return response.text


def parse_sitemap(xml: str) -> dict[str, str]:
    return {normalize_event_url(loc): lastmod or "" for loc, lastmod in LOC_LASTMOD.findall(xml)}


def load_sitemap_lastmods(domain: str) -> dict[str, str]:
    """URL -> lastmod for the blog's English posts.

    The blogs publish a sitemap index with one child per language; only the
    English child is read, since the optimizer only touches English posts.
    """
    root = f"https://{domain}/sitemap.xml"
    xml = fetch_text(root)
    if "<sitemapindex" not in xml:
        return parse_sitemap(xml)
    children = list(parse_sitemap(xml))
    english = [c for c in children if c.rstrip("/").endswith("/en/sitemap.xml")]
    entries = {}
    for child in english or children:
        entries.update(parse_sitemap(fetch_text(child.rstrip("/"))))
    return entries


def is_live(lastmod: str, optimized_on: date) -> bool:
    return bool(lastmod) and lastmod[:10] >= str(optimized_on)


# ----------------------------------------------------
# Search Console
# ----------------------------------------------------

def load_credentials():
    from google.oauth2.credentials import Credentials

    data = json.loads(CREDENTIALS_FILE.read_text(encoding="utf-8"))
    return Credentials.from_authorized_user_info(data, scopes=data.get("scopes"))


def build_service(credentials):
    from googleapiclient.discovery import build

    return build("searchconsole", "v1", credentials=credentials, cache_discovery=False)


def can_write(credentials) -> bool:
    return WRITE_SCOPE in (credentials.scopes or [])


def submit_sitemap(service, domain: str) -> bool:
    try:
        service.sitemaps().submit(siteUrl=f"https://{domain}/", feedpath=f"https://{domain}/sitemap.xml").execute()
        return True
    except Exception as exc:
        print(f"  WARNING: sitemap submit failed for {domain}: {exc}")
        return False


def inspect_url(service, domain: str, url: str) -> dict:
    body = {"inspectionUrl": url, "siteUrl": f"https://{domain}/", "languageCode": "en-US"}
    for attempt in range(4):
        try:
            result = service.urlInspection().index().inspect(body=body).execute()
            return result.get("inspectionResult", {}).get("indexStatusResult", {})
        except Exception as exc:
            if attempt == 3:
                raise
            wait = 10 * (attempt + 1)
            print(f"    Inspection error ({exc}); retrying in {wait}s")
            time.sleep(wait)
    return {}


def crawl_date(last_crawl_time: str) -> date | None:
    if not last_crawl_time:
        return None
    return datetime.fromisoformat(last_crawl_time.replace("Z", "+00:00")).date()


# ----------------------------------------------------
# Daily run
# ----------------------------------------------------

def new_row(event: dict) -> dict:
    row = {field: "" for field in FIELDS}
    row.update({
        "event_id": event["event_id"],
        "optimized_on": event["optimized_on"],
        "domain": event["domain"],
        "url": event["url"],
        "status": "waiting_deploy",
    })
    return row


def update_live_status(rows: list[dict], today: date) -> list[dict]:
    """Mark rows whose optimized version is in the sitemap. Returns newly live rows."""
    newly_live = []
    by_domain = {}
    for row in rows:
        if row["status"] == "waiting_deploy":
            by_domain.setdefault(row["domain"], []).append(row)

    for domain, pending in sorted(by_domain.items()):
        try:
            lastmods = load_sitemap_lastmods(domain)
        except Exception as exc:
            print(f"  WARNING: could not read sitemap for {domain}: {exc}")
            continue
        print(f"  {domain}: {len(lastmods)} sitemap URLs, {len(pending)} events waiting for deploy")
        for row in pending:
            optimized_on = date.fromisoformat(row["optimized_on"])
            age = (today - optimized_on).days
            if is_live(lastmods.get(row["url"], ""), optimized_on):
                # A long-live event found on its first check has an unknown go-live date
                if row.get("_seen_before") or age <= LIVE_EXACT_DAYS:
                    row["live_seen_on"] = str(today)
                row["status"] = "waiting_recrawl"
                newly_live.append(row)
            elif age > ASSUME_LIVE_AFTER_DAYS:
                # Sitemap never reflected it (e.g. lastmod not updated); inspect anyway
                row["status"] = "waiting_recrawl"
    return newly_live


def apply_inspection(row: dict, result: dict, today: date) -> None:
    optimized_on = date.fromisoformat(row["optimized_on"])
    threshold = date.fromisoformat(row["live_seen_on"]) if row["live_seen_on"] else optimized_on + timedelta(days=1)
    previous = crawl_date(row["last_crawl_time"])
    current = crawl_date(result.get("lastCrawlTime", ""))

    row["last_crawl_time"] = result.get("lastCrawlTime", "")
    row["coverage_state"] = result.get("coverageState", "")
    row["last_inspected_on"] = str(today)

    if current and current >= threshold:
        exact = bool(previous and previous < threshold)
        row["status"] = "recrawled"
        row["recrawled_on"] = str(current)
        row["recrawl_exact"] = "yes" if exact else "no"
        # Only an observed first recrawl gives a true delay; a later crawl would overstate it
        row["days_to_recrawl"] = (current - optimized_on).days if exact else ""
    elif (today - optimized_on).days > GIVE_UP_AFTER_DAYS:
        row["status"] = "not_recrawled"


def run(limit: int | None = None) -> int:
    today = date.today()
    events = load_all_events()
    status = read_status()
    for event in events:
        if event["event_id"] not in status:
            status[event["event_id"]] = new_row(event)
        else:
            status[event["event_id"]]["_seen_before"] = True
    rows = list(status.values())
    print(f"Tracking {len(rows)} optimization events")

    print("\nChecking sitemaps for deployed optimizations ...")
    newly_live = update_live_status(rows, today)
    print(f"  {len(newly_live)} event(s) now live")

    credentials = load_credentials()
    service = build_service(credentials)

    recent_domains = sorted({
        r["domain"] for r in newly_live
        if (today - date.fromisoformat(r["optimized_on"])).days <= RECENT_DAYS
    })
    if recent_domains:
        print("\nResubmitting sitemaps ...")
        if not can_write(credentials):
            print(
                "  WARNING: the Search Console login is read-only; sitemaps were not submitted. "
                "Run `python3 gsc_recrawl_tracker.py authorize` and update GSC_CREDENTIALS_JSON."
            )
        else:
            for domain in recent_domains:
                if submit_sitemap(service, domain):
                    print(f"  Submitted https://{domain}/sitemap.xml")
                    for r in newly_live:
                        if r["domain"] == domain:
                            r["sitemap_submitted_on"] = str(today)

    todo = [r for r in rows if r["status"] == "waiting_recrawl" and r["last_inspected_on"] != str(today)]
    if limit:
        todo = todo[:limit]
    print(f"\nInspecting {len(todo)} URL(s) ...")
    per_site = {}
    for i, row in enumerate(todo, 1):
        if per_site.get(row["domain"], 0) >= DAILY_INSPECTIONS_PER_SITE:
            continue
        per_site[row["domain"]] = per_site.get(row["domain"], 0) + 1
        try:
            result = inspect_url(service, row["domain"], row["url"])
        except Exception as exc:
            print(f"  WARNING: inspection failed for {row['url']}: {exc}")
            continue
        apply_inspection(row, result, today)
        print(f"  [{i}/{len(todo)}] {row['optimized_on']} {row['url']} -> {row['status']} (last crawl {row['last_crawl_time'] or 'none'})")
        time.sleep(0.2)  # stay well under 600 requests/minute

    for row in rows:
        row.pop("_seen_before", None)
    write_status(status)

    counts = {}
    for row in rows:
        counts[row["status"]] = counts.get(row["status"], 0) + 1
    print(f"\nWrote {STATUS_FILE}: {counts}")
    return 0


def authorize() -> int:
    """Interactive one-off login granting Search Console write access (needed for sitemap submits)."""
    from google_auth_oauthlib.flow import InstalledAppFlow

    if not CLIENT_SECRET_FILE.exists():
        print(f"{CLIENT_SECRET_FILE} not found. Save the Google OAuth client JSON there first.")
        return 1
    flow = InstalledAppFlow.from_client_secrets_file(str(CLIENT_SECRET_FILE), scopes=[WRITE_SCOPE])
    credentials = flow.run_local_server(port=0)
    data = json.loads(credentials.to_json())
    data["scopes"] = [WRITE_SCOPE]
    CREDENTIALS_FILE.write_text(json.dumps(data), encoding="utf-8")
    print(f"Wrote {CREDENTIALS_FILE} with {WRITE_SCOPE}.")
    print("Update the GSC_CREDENTIALS_JSON GitHub secret with its contents.")
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", nargs="?", default="run", choices=["run", "authorize"])
    parser.add_argument("--limit", type=int, help="Max URLs to inspect (for testing)")
    args = parser.parse_args(argv)
    if args.command == "authorize":
        return authorize()
    return run(args.limit)


if __name__ == "__main__":
    sys.exit(main())

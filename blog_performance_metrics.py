#!/usr/bin/env python3
"""
Blog performance metrics from Google Search Console, split into three streams.

  optimized  URLs in the optimization event logs (logs/*/*_events.csv)
  new        posts created by the blog generator agent (read from its repo)
  general    every other page on the blog

Outputs under metrics/:

  optimization_event_metrics.csv
      One row per optimization event. Clicks, impressions and average position
      for the 8 weeks (56 days) before and after the event date, plus the site's
      change over the same windows so a site-wide decline is not read as the
      optimization's effect.

  new_post_metrics.csv
      One row per generated post. Clicks, impressions and average position for
      its first 8 weeks (56 days) after publishing.

  stream_weekly.csv
      Weekly clicks/impressions/position per domain and stream, for the last
      26 complete weeks (about 6 months). Older weeks are dropped.

Rows whose windows are complete are kept as-is on later runs; only rows still
waiting for data are re-queried. GSC data lags by a few days, so a window only
counts as complete once it ends at least GSC_LAG_DAYS ago.

Usage:
    python3 blog_performance_metrics.py
    python3 blog_performance_metrics.py --domains blog.conholdate.com --skip-streams
"""

import argparse
import csv
import os
import re
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import yaml

from gsc_processing_utils import parse_publish_date
from optimization_events import load_all_events, normalize_event_url

METRICS_DIR = Path("metrics")
EVENT_METRICS_FILE = METRICS_DIR / "optimization_event_metrics.csv"
NEW_POST_METRICS_FILE = METRICS_DIR / "new_post_metrics.csv"
STREAM_WEEKLY_FILE = METRICS_DIR / "stream_weekly.csv"

GSC_LAG_DAYS = 3
EVENT_WINDOWS = (56,)
NEW_POST_WINDOWS = (56,)
STREAM_WEEKS = 26  # about 6 months of weekly stream totals
STREAMS_REFRESH_WEEKS = 2  # re-query the latest weeks; GSC revises recent data

# Blog domain -> folder name under the generator's content/blogPosts
DOMAINS = {
    "blog.aspose.com": "aspose_com",
    "blog.aspose.cloud": "aspose_cloud",
    "blog.conholdate.com": "conholdate_com",
    "blog.conholdate.cloud": "conholdate_cloud",
    "blog.groupdocs.com": "groupdocs_com",
    "blog.groupdocs.cloud": "groupdocs_cloud",
}

DEFAULT_GENERATOR_ROOT = Path("../blog-generator-agent/content/blogPosts")

EVENT_FIELDS = [
    "event_id", "optimized_on", "domain", "url", "recommended_action", "status",
    "overlapping_event",
]
for _w in EVENT_WINDOWS:
    EVENT_FIELDS += [
        f"clicks_pre_{_w}d", f"clicks_post_{_w}d", f"clicks_change_{_w}d_pct",
        f"impressions_pre_{_w}d", f"impressions_post_{_w}d", f"impressions_change_{_w}d_pct",
        f"position_pre_{_w}d", f"position_post_{_w}d", f"position_change_{_w}d",
        f"site_clicks_change_{_w}d_pct", f"clicks_vs_site_{_w}d_pct",
    ]
EVENT_FIELDS.append("updated_on")

NEW_POST_FIELDS = ["domain", "url", "published_on", "generator_folder", "status"]
for _w in NEW_POST_WINDOWS:
    NEW_POST_FIELDS += [f"clicks_first_{_w}d", f"impressions_first_{_w}d", f"position_first_{_w}d"]
NEW_POST_FIELDS.append("updated_on")

STREAM_FIELDS = ["week_start", "domain", "stream", "pages", "clicks", "impressions", "position", "updated_on"]

FRONT_MATTER = re.compile(r"^---\s*\n(.*?)\n---\s*(?:\n|$)", re.DOTALL)


# ----------------------------------------------------
# Generic helpers
# ----------------------------------------------------

def read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, fields: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def pct_change(before: float, after: float):
    if not before:
        return ""
    return round((after - before) / before * 100, 1)


def summarize(daily: dict[date, dict], start: date, end: date) -> dict:
    """Total clicks/impressions and impression-weighted position over [start, end]."""
    clicks = impressions = weighted_position = 0.0
    day = start
    while day <= end:
        row = daily.get(day)
        if row:
            clicks += row["clicks"]
            impressions += row["impressions"]
            weighted_position += row["position"] * row["impressions"]
        day += timedelta(days=1)
    position = round(weighted_position / impressions, 2) if impressions else ""
    return {"clicks": int(clicks), "impressions": int(impressions), "position": position}


def weighted_position(rows: list[dict]):
    impressions = sum(r["impressions"] for r in rows)
    if not impressions:
        return ""
    return round(sum(r["position"] * r["impressions"] for r in rows) / impressions, 2)


# ----------------------------------------------------
# Search Console access
# ----------------------------------------------------

class SearchConsole:
    def __init__(self):
        import searchconsole  # imported lazily so tests don't need Google libs

        if os.path.exists("credentials.json"):
            self.account = searchconsole.authenticate(credentials="credentials.json")
        else:
            self.account = searchconsole.authenticate(client_config="client_secret.json")
            self.account.serialize_credentials("credentials.json")
        self.properties = {}

    def property(self, domain: str):
        if domain not in self.properties:
            self.properties[domain] = self.account[f"https://{domain}/"]
        return self.properties[domain]

    def _run(self, query):
        for attempt in range(4):
            try:
                return list(query.get())
            except Exception as exc:  # quota and transient API errors
                if attempt == 3:
                    raise
                wait = 5 * (attempt + 1)
                print(f"    GSC error ({exc}); retrying in {wait}s")
                time.sleep(wait)

    def daily(self, domain: str, start: date, end: date, page_prefix: str | None = None) -> dict[date, dict]:
        """Daily totals for the property, or for one page when page_prefix is set."""
        query = self.property(domain).query.range(start, end).dimension("date")
        if page_prefix:
            # `contains` on the full URL also folds in #fragment variants of the page
            query = query.filter("page", page_prefix, "contains")
        rows = self._run(query)
        return {
            date.fromisoformat(r.date): {
                "clicks": r.clicks,
                "impressions": r.impressions,
                "position": r.position,
            }
            for r in rows
        }

    def pages(self, domain: str, start: date, end: date) -> list[dict]:
        rows = self._run(self.property(domain).query.range(start, end).dimension("page"))
        return [
            {"page": r.page, "clicks": r.clicks, "impressions": r.impressions, "position": r.position}
            for r in rows
        ]


# ----------------------------------------------------
# Inputs
# ----------------------------------------------------

def read_front_matter_fields(index_md: Path) -> dict | None:
    """Read date/url/draft from front matter.

    Falls back to line matching when the YAML block does not parse (e.g. it
    contains leftover merge-conflict markers), since these three fields are
    simple one-line values.
    """
    match = FRONT_MATTER.match(index_md.read_text(encoding="utf-8"))
    if not match:
        return None
    try:
        meta = yaml.safe_load(match.group(1))
        if isinstance(meta, dict):
            return meta
    except yaml.YAMLError:
        pass
    print(f"  Front matter is not valid YAML, reading fields line by line: {index_md}")
    meta = {}
    for line in match.group(1).splitlines():
        field = re.match(r"^(date|url|draft):\s*(.+?)\s*$", line)
        if field and field.group(1) not in meta:
            meta[field.group(1)] = field.group(2).strip("'\"")
    return meta or None


def load_generated_posts(root: Path) -> list[dict]:
    """Posts the blog generator created, read from its content/blogPosts/<brand> folders."""
    posts = {}
    folder_to_domain = {folder: domain for domain, folder in DOMAINS.items()}
    if not root.exists():
        print(f"Generator posts folder not found: {root}")
        return []

    for brand_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        domain = folder_to_domain.get(brand_dir.name)
        if not domain:
            print(f"  Unknown generator brand folder: {brand_dir.name}")
            continue
        for index_md in sorted(brand_dir.glob("*/index.md")):
            meta = read_front_matter_fields(index_md)
            if meta is None:
                print(f"  Unreadable front matter: {index_md}")
                continue
            if str(meta.get("draft", "false")).lower() == "true":
                continue
            published_on = parse_publish_date(meta.get("date"))
            url_path = str(meta.get("url") or "").strip()
            if not published_on or not url_path:
                print(f"  Missing date or url: {index_md}")
                continue
            url = normalize_event_url(f"https://{domain}/{url_path.lstrip('/')}")
            if url in posts and posts[url]["published_on"] <= published_on:
                continue
            posts[url] = {
                "domain": domain,
                "url": url,
                "published_on": published_on,
                "generator_folder": f"{brand_dir.name}/{index_md.parent.name}",
            }
    return sorted(posts.values(), key=lambda p: (p["published_on"], p["url"]))


# ----------------------------------------------------
# Optimization events: before vs after
# ----------------------------------------------------

def event_status(optimized_on: date, data_end: date) -> str:
    ready = [w for w in EVENT_WINDOWS if optimized_on + timedelta(days=w) <= data_end]
    if not ready:
        return "pending"
    return f"{max(ready) // 7}w_ready"


def overlapping_events(events: list[dict]) -> set[str]:
    """Events with another optimization of the same URL inside the widest window."""
    widest = max(EVENT_WINDOWS)
    by_url = {}
    for e in events:
        by_url.setdefault(e["url"], []).append(e)
    flagged = set()
    for same_url in by_url.values():
        for a in same_url:
            for b in same_url:
                if a is b:
                    continue
                gap = abs((date.fromisoformat(a["optimized_on"]) - date.fromisoformat(b["optimized_on"])).days)
                if gap <= widest:
                    flagged.add(a["event_id"])
    return flagged


def build_event_row(event: dict, page_daily: dict, site_daily: dict, data_end: date, overlapping: bool) -> dict:
    d = date.fromisoformat(event["optimized_on"])
    row = {
        "event_id": event["event_id"],
        "optimized_on": event["optimized_on"],
        "domain": event["domain"],
        "url": event["url"],
        "recommended_action": event.get("recommended_action", ""),
        "status": event_status(d, data_end),
        "overlapping_event": "yes" if overlapping else "",
        "updated_on": str(date.today()),
    }
    for w in EVENT_WINDOWS:
        # The event day itself is excluded: the change ships during that day.
        pre_start, pre_end = d - timedelta(days=w), d - timedelta(days=1)
        post_start, post_end = d + timedelta(days=1), d + timedelta(days=w)
        pre = summarize(page_daily, pre_start, pre_end)
        row[f"clicks_pre_{w}d"] = pre["clicks"]
        row[f"impressions_pre_{w}d"] = pre["impressions"]
        row[f"position_pre_{w}d"] = pre["position"]
        if post_end > data_end:
            continue  # leave the after-window blank until it is complete
        post = summarize(page_daily, post_start, post_end)
        site_pre = summarize(site_daily, pre_start, pre_end)
        site_post = summarize(site_daily, post_start, post_end)
        row[f"clicks_post_{w}d"] = post["clicks"]
        row[f"impressions_post_{w}d"] = post["impressions"]
        row[f"position_post_{w}d"] = post["position"]
        row[f"clicks_change_{w}d_pct"] = pct_change(pre["clicks"], post["clicks"])
        row[f"impressions_change_{w}d_pct"] = pct_change(pre["impressions"], post["impressions"])
        if pre["position"] != "" and post["position"] != "":
            # Negative means the post moved up (a smaller position number is better)
            row[f"position_change_{w}d"] = round(post["position"] - pre["position"], 2)
        row[f"site_clicks_change_{w}d_pct"] = pct_change(site_pre["clicks"], site_post["clicks"])
        if pre["clicks"] and site_pre["clicks"] and site_post["clicks"]:
            page_ratio = post["clicks"] / pre["clicks"]
            site_ratio = site_post["clicks"] / site_pre["clicks"]
            row[f"clicks_vs_site_{w}d_pct"] = round((page_ratio / site_ratio - 1) * 100, 1)
    return row


def update_event_metrics(gsc: SearchConsole, events: list[dict], domains: set[str], data_end: date, limit: int | None):
    final_status = f"{max(EVENT_WINDOWS) // 7}w_ready"
    existing = {r["event_id"]: r for r in read_csv(EVENT_METRICS_FILE)}
    overlapping = overlapping_events(events)
    widest = max(EVENT_WINDOWS)

    todo = [
        e for e in events
        if e["domain"] in domains and existing.get(e["event_id"], {}).get("status") != final_status
    ]
    if limit:
        todo = todo[:limit]
    print(f"\nOptimization events: {len(events)} total, {len(todo)} to refresh")

    site_cache: dict[str, dict] = {}
    for i, event in enumerate(todo, 1):
        d = date.fromisoformat(event["optimized_on"])
        start, end = d - timedelta(days=widest), min(d + timedelta(days=widest), data_end)
        domain = event["domain"]
        if domain not in site_cache:
            first = min(date.fromisoformat(e["optimized_on"]) for e in todo if e["domain"] == domain)
            site_cache[domain] = gsc.daily(domain, first - timedelta(days=widest), data_end)
        page_daily = gsc.daily(domain, start, end, page_prefix=event["url"])
        existing[event["event_id"]] = build_event_row(
            event, page_daily, site_cache[domain], data_end, event["event_id"] in overlapping
        )
        print(f"  [{i}/{len(todo)}] {event['optimized_on']} {event['url']} -> {existing[event['event_id']]['status']}")

    rows = sorted(existing.values(), key=lambda r: (r["optimized_on"], r["url"]))
    write_csv(EVENT_METRICS_FILE, EVENT_FIELDS, rows)
    print(f"Wrote {EVENT_METRICS_FILE} ({len(rows)} rows)")


# ----------------------------------------------------
# New posts: first weeks after publishing
# ----------------------------------------------------

def new_post_status(published_on: date, data_end: date) -> str:
    ready = [w for w in NEW_POST_WINDOWS if published_on + timedelta(days=w - 1) <= data_end]
    if not ready:
        return "pending"
    return f"{max(ready) // 7}w_ready"


def build_new_post_row(post: dict, page_daily: dict, data_end: date) -> dict:
    p = post["published_on"]
    row = {
        "domain": post["domain"],
        "url": post["url"],
        "published_on": str(p),
        "generator_folder": post["generator_folder"],
        "status": new_post_status(p, data_end),
        "updated_on": str(date.today()),
    }
    for w in NEW_POST_WINDOWS:
        end = p + timedelta(days=w - 1)
        if end > data_end:
            continue
        summary = summarize(page_daily, p, end)
        row[f"clicks_first_{w}d"] = summary["clicks"]
        row[f"impressions_first_{w}d"] = summary["impressions"]
        row[f"position_first_{w}d"] = summary["position"]
    return row


def update_new_post_metrics(gsc: SearchConsole, posts: list[dict], domains: set[str], data_end: date, limit: int | None):
    final_status = f"{max(NEW_POST_WINDOWS) // 7}w_ready"
    existing = {r["url"]: r for r in read_csv(NEW_POST_METRICS_FILE)}
    todo = [
        p for p in posts
        if p["domain"] in domains
        and p["published_on"] <= data_end
        and existing.get(p["url"], {}).get("status") != final_status
    ]
    if limit:
        todo = todo[:limit]
    print(f"\nNew posts: {len(posts)} total, {len(todo)} to refresh")

    widest = max(NEW_POST_WINDOWS)
    for i, post in enumerate(todo, 1):
        p = post["published_on"]
        page_daily = gsc.daily(post["domain"], p, min(p + timedelta(days=widest - 1), data_end), page_prefix=post["url"])
        existing[post["url"]] = build_new_post_row(post, page_daily, data_end)
        print(f"  [{i}/{len(todo)}] {p} {post['url']} -> {existing[post['url']]['status']}")

    rows = sorted(existing.values(), key=lambda r: (r["published_on"], r["url"]))
    write_csv(NEW_POST_METRICS_FILE, NEW_POST_FIELDS, rows)
    print(f"Wrote {NEW_POST_METRICS_FILE} ({len(rows)} rows)")


# ----------------------------------------------------
# Weekly totals per stream
# ----------------------------------------------------

def classify_page(url: str, week_start: date, first_optimized: dict, published: dict) -> str:
    """Streams are exclusive so they add up to the site total.

    A post counts as `optimized` from the week after its first optimization,
    otherwise as `new` from its publish week if the generator created it.
    """
    optimized_on = first_optimized.get(url)
    if optimized_on and optimized_on < week_start:
        return "optimized"
    published_on = published.get(url)
    if published_on and published_on < week_start + timedelta(days=7):
        return "new"
    return "general"


def update_stream_weekly(gsc: SearchConsole, events: list[dict], posts: list[dict], domains: set[str], data_end: date, stream_start: date | None = None):
    first_optimized = {}
    for e in events:
        d = date.fromisoformat(e["optimized_on"])
        if e["url"] not in first_optimized or d < first_optimized[e["url"]]:
            first_optimized[e["url"]] = d
    published = {p["url"]: p["published_on"] for p in posts}

    last_sunday = data_end - timedelta(days=(data_end.weekday() + 1) % 7)
    last_complete_week = last_sunday - timedelta(days=6)
    stream_start = stream_start or last_complete_week - timedelta(weeks=STREAM_WEEKS - 1)
    refresh_from = last_complete_week - timedelta(weeks=STREAMS_REFRESH_WEEKS - 1)
    existing = read_csv(STREAM_WEEKLY_FILE)
    kept = [
        r for r in existing
        if date.fromisoformat(r["week_start"]) >= stream_start
        and not (r["domain"] in domains and date.fromisoformat(r["week_start"]) >= refresh_from)
    ]
    have = {(r["domain"], r["week_start"]) for r in kept}

    rows = list(kept)
    print(f"\nWeekly streams: {stream_start} to {last_complete_week}")
    for domain in sorted(domains):
        week = stream_start
        while week <= last_complete_week:
            if (domain, str(week)) not in have:
                pages = gsc.pages(domain, week, week + timedelta(days=6))
                buckets = {"optimized": [], "new": [], "general": []}
                for page in pages:
                    url = normalize_event_url(page["page"].split("#", 1)[0])
                    buckets[classify_page(url, week, first_optimized, published)].append(page)
                for stream, items in buckets.items():
                    rows.append({
                        "week_start": str(week),
                        "domain": domain,
                        "stream": stream,
                        "pages": len({normalize_event_url(i["page"].split("#", 1)[0]) for i in items}),
                        "clicks": int(sum(i["clicks"] for i in items)),
                        "impressions": int(sum(i["impressions"] for i in items)),
                        "position": weighted_position(items),
                        "updated_on": str(date.today()),
                    })
                print(f"  {domain} {week}: {len(pages)} pages")
            week += timedelta(weeks=1)

    rows.sort(key=lambda r: (r["week_start"], r["domain"], r["stream"]))
    write_csv(STREAM_WEEKLY_FILE, STREAM_FIELDS, rows)
    print(f"Wrote {STREAM_WEEKLY_FILE} ({len(rows)} rows)")


# ----------------------------------------------------
# Main
# ----------------------------------------------------

def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--domains", nargs="*", default=sorted(DOMAINS), help="Blog domains to process")
    parser.add_argument(
        "--generator-root",
        type=Path,
        default=Path(os.getenv("BLOG_GENERATOR_POSTS_ROOT", DEFAULT_GENERATOR_ROOT)),
        help="Path to the blog generator's content/blogPosts folder",
    )
    parser.add_argument(
        "--stream-start",
        type=date.fromisoformat,
        help=f"First week (Monday) of the weekly streams; default is the last {STREAM_WEEKS} complete weeks",
    )
    parser.add_argument("--skip-events", action="store_true")
    parser.add_argument("--skip-new-posts", action="store_true")
    parser.add_argument("--skip-streams", action="store_true")
    parser.add_argument("--limit", type=int, help="Max rows to refresh per section (for testing)")
    args = parser.parse_args(argv)

    unknown = set(args.domains) - set(DOMAINS)
    if unknown:
        parser.error(f"Unknown domain(s): {', '.join(sorted(unknown))}")
    domains = set(args.domains)
    data_end = date.today() - timedelta(days=GSC_LAG_DAYS)

    events = load_all_events()
    posts = load_generated_posts(args.generator_root)
    print(f"Loaded {len(events)} optimization events and {len(posts)} generated posts; data through {data_end}")

    gsc = SearchConsole()
    for domain in sorted(domains):
        try:
            gsc.property(domain)
        except KeyError:
            print(f"WARNING: no Search Console access to https://{domain}/; skipping it")
            domains.discard(domain)

    if not args.skip_events:
        update_event_metrics(gsc, events, domains, data_end, args.limit)
    if not args.skip_new_posts:
        update_new_post_metrics(gsc, posts, domains, data_end, args.limit)
    if not args.skip_streams:
        update_stream_weekly(gsc, events, posts, domains, data_end, args.stream_start)
    return 0


if __name__ == "__main__":
    sys.exit(main())

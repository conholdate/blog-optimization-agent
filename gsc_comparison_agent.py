"""
gsc_comparison_agent.py
=======================
Fetches Google Search Console data across 3 time windows (Last 3 months,
Last 6 months, Last 12 months) and produces two CSV files:

  csv/comparison_all_periods.csv   — merged wide table, one row per URL,
                                     columns for each period's metrics plus
                                     trend pattern label.

  csv/comparison_candidates.csv    — filtered to URLs that are genuine
                                     optimization candidates based on
                                     multi-period pattern analysis.

Exclusion rules applied to both outputs:
  • Non-English language-prefix URLs  (reuses has_language_prefix)
  • High-click URLs: any URL with >= HIGH_CLICK_THRESHOLD clicks in ANY
    period is excluded from the candidates file (still kept in all_periods).
  • CTR band: 1% – 4% in the ANCHOR period (12 months) for candidates.

Pattern labels (column "trend_pattern"):
  improving          CTR rises across all three windows  (3m > 6m > 12m)
  declining          CTR falls across all three windows  (3m < 6m < 12m)
  continuously_good  CTR >= CTR_GOOD_THRESHOLD in all three windows
  continuously_bad   CTR <  CTR_BAD_THRESHOLD  in all three windows
  volatile           none of the above
  insufficient_data  fewer than 2 periods have data

Usage
-----
  python gsc_comparison_agent.py

Environment variables (optional overrides):
  BLOG_CONTENT_ROOT   path to local content repo for publish-date enrichment
  ASPOSE_WEB_APP_URL  Google Apps Script endpoint  (upload, not required here)
  ASPOSE_SPREADSHEET_ID
"""

import os
import sys
import pandas as pd
from datetime import datetime, timedelta

import searchconsole

from gsc_processing_utils import (
    has_language_prefix,
    select_best_days_since_map,
    url_to_path,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

os.environ["BLOG_CONTENT_ROOT"] = r"/Users/syedfarhanraza/Documents/GitHub/content/Aspose.Blog"

PROPERTY_URL       = "https://blog.aspose.com/"
HIGH_CLICK_THRESHOLD = 500       # URLs with >= this clicks in ANY period are
                                  # excluded from the candidates CSV
CTR_THRESHOLD      = 0.01        # 1%  — lower bound for candidate CTR filter
CTR_MAX_THRESHOLD  = 0.04        # 4%  — upper bound
CTR_GOOD_THRESHOLD = 0.03        # 3%  — used to label "continuously_good"
CTR_BAD_THRESHOLD  = 0.01        # 1%  — used to label "continuously_bad"

# Time windows: (label, days_back)
PERIODS = [
    ("3m",  90),
    ("6m",  180),
    ("12m", 365),
]

CSV_FOLDER = "csv"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def authenticate():
    """Authenticate with Google Search Console."""
    if os.path.exists("credentials.json"):
        account = searchconsole.authenticate(credentials="credentials.json")
    else:
        account = searchconsole.authenticate(client_config="client_secret.json")
        account.serialize_credentials("credentials.json")
    return account


def fetch_period(webproperty, days_back: int, label: str) -> pd.DataFrame:
    """Fetch page-level GSC data for a rolling window of *days_back* days."""
    end_date   = datetime.now().date()
    start_date = end_date - timedelta(days=days_back)
    print(f"  [{label}] Fetching {start_date} → {end_date} …", end=" ", flush=True)

    report = webproperty.query.range(start_date, end_date).dimension("page").get()
    df = pd.DataFrame(report)
    print(f"{len(df):,} rows")

    if df.empty:
        return pd.DataFrame(columns=["page", "clicks", "impressions", "ctr", "position"])

    if "ctr" not in df.columns:
        df["ctr"] = df["clicks"] / df["impressions"].replace(0, 1)

    # Keep only the target property and English URLs
    df = df[df["page"].str.contains("blog.aspose.com", na=False)]
    df = df[~df["page"].apply(has_language_prefix)]

    return df[["page", "clicks", "impressions", "ctr", "position"]].copy()


def label_trend(row, period_labels):
    """
    Assign a trend pattern based on CTR across available periods.
    Periods are ordered shortest → longest: 3m, 6m, 12m.
    """
    ctr_cols = [f"ctr_{p}" for p in period_labels]
    ctrs = [row.get(c) for c in ctr_cols]
    available = [(p, v) for p, v in zip(period_labels, ctrs) if pd.notna(v)]

    if len(available) < 2:
        return "insufficient_data"

    values = [v for _, v in available]

    # Continuously good / bad (all available periods)
    if all(v >= CTR_GOOD_THRESHOLD for v in values):
        return "continuously_good"
    if all(v < CTR_BAD_THRESHOLD for v in values):
        return "continuously_bad"

    # Improving: shorter window CTR > longer window CTR
    # available[0] = most recent short window, available[-1] = oldest long window
    if all(values[i] > values[i + 1] for i in range(len(values) - 1)):
        return "improving"

    # Declining
    if all(values[i] < values[i + 1] for i in range(len(values) - 1)):
        return "declining"

    return "volatile"


def build_wide_table(period_dfs: dict) -> pd.DataFrame:
    """
    Merge per-period DataFrames into a single wide table keyed on 'page'.
    Suffix each metric column with the period label.
    """
    merged = None
    for label, df in period_dfs.items():
        renamed = df.rename(columns={
            "clicks":      f"clicks_{label}",
            "impressions": f"impressions_{label}",
            "ctr":         f"ctr_{label}",
            "position":    f"position_{label}",
        })
        if merged is None:
            merged = renamed
        else:
            merged = merged.merge(renamed, on="page", how="outer")

    return merged.reset_index(drop=True)


def enrich_publish_age(df: pd.DataFrame) -> pd.DataFrame:
    """Add 'days_since_published' column via local content repo, if available."""
    content_root, days_map, parsed, matched, total, _ = select_best_days_since_map(
        "aspose-blog", df["page"].tolist()
    )
    if content_root:
        print(f"  Content root: {content_root}")
        print(f"  Indexed {parsed:,} posts; matched {matched:,}/{total:,} URLs")
    else:
        print("  Content root not found — skipping publish-age enrichment.")
        days_map = {}

    df["days_since_published"] = df["page"].apply(
        lambda p: days_map.get(url_to_path(str(p)))
    )
    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 70)
    print("GSC Multi-Period Comparison Agent — blog.aspose.com")
    print("=" * 70)

    # 1) Authenticate
    print("\nAuthenticating …")
    try:
        account = authenticate()
        print("  Auth OK")
    except Exception as exc:
        print(f"  Auth failed: {exc}")
        sys.exit(1)

    try:
        webproperty = account[PROPERTY_URL]
    except KeyError:
        print(f"Property '{PROPERTY_URL}' not found. Available: {list(account)}")
        sys.exit(1)

    # 2) Fetch all periods
    print("\nFetching data from Search Console …")
    period_dfs = {}
    for label, days_back in PERIODS:
        try:
            period_dfs[label] = fetch_period(webproperty, days_back, label)
        except Exception as exc:
            print(f"  [{label}] Fetch failed: {exc}")
            period_dfs[label] = pd.DataFrame(
                columns=["page", "clicks", "impressions", "ctr", "position"]
            )

    # 3) Merge into wide table
    print("\nMerging periods …")
    period_labels = [label for label, _ in PERIODS]   # ["3m", "6m", "12m"]
    wide = build_wide_table(period_dfs)
    print(f"  {len(wide):,} unique URLs across all periods")

    # 4) Trend pattern
    wide["trend_pattern"] = wide.apply(
        lambda row: label_trend(row, period_labels), axis=1
    )

    # 5) High-click flag  (any period >= threshold)
    click_cols = [f"clicks_{p}" for p in period_labels]
    wide["max_clicks_any_period"] = wide[click_cols].max(axis=1)
    wide["high_click_url"] = wide["max_clicks_any_period"] >= HIGH_CLICK_THRESHOLD

    # 6) Enrich with publish age
    print("\nEnriching with publish dates …")
    wide = enrich_publish_age(wide)

    # 7) Column order for wide/all table
    ordered_cols = ["page"]
    for p in period_labels:
        ordered_cols += [f"clicks_{p}", f"impressions_{p}", f"ctr_{p}", f"position_{p}"]
    ordered_cols += ["trend_pattern", "max_clicks_any_period", "high_click_url", "days_since_published"]
    # Keep any extra columns that might exist
    extra = [c for c in wide.columns if c not in ordered_cols]
    wide = wide[ordered_cols + extra]

    # 8) Save CSV 1 — all URLs with all periods
    os.makedirs(CSV_FOLDER, exist_ok=True)
    all_path = os.path.join(CSV_FOLDER, "comparison_all_periods.csv")
    wide.sort_values("days_since_published", ascending=False, na_position="last", inplace=True)
    wide.to_csv(all_path, index=False)
    print(f"\nSaved: {all_path}  ({len(wide):,} rows)")

    # 9) Filter to optimization candidates
    # Anchor on 12m CTR band; exclude high-click; keep only URLs that appear
    # in the 12m data so we have a stable anchor
    anchor = "12m"
    ctr_anchor = f"ctr_{anchor}"
    clicks_anchor = f"clicks_{anchor}"

    candidates = wide.copy()

    # Must exist in anchor period
    candidates = candidates[candidates[ctr_anchor].notna()]

    # CTR band in anchor period
    candidates = candidates[
        (candidates[ctr_anchor] >= CTR_THRESHOLD) &
        (candidates[ctr_anchor] <= CTR_MAX_THRESHOLD)
    ]

    # Exclude high-click URLs
    before_hc = len(candidates)
    candidates = candidates[~candidates["high_click_url"]]
    hc_removed = before_hc - len(candidates)

    print(f"\nCandidates filter:")
    print(f"  CTR {CTR_THRESHOLD:.0%}–{CTR_MAX_THRESHOLD:.0%} in {anchor} window → {before_hc:,} URLs")
    print(f"  Removed {hc_removed:,} high-click URLs (>= {HIGH_CLICK_THRESHOLD} clicks in any period)")
    print(f"  Final candidates: {len(candidates):,} URLs")

    # Pattern breakdown
    print("\n  Trend pattern breakdown:")
    for pattern, count in candidates["trend_pattern"].value_counts().items():
        print(f"    {pattern:<22} {count:>5}")

    cand_path = os.path.join(CSV_FOLDER, "comparison_candidates.csv")
    candidates.to_csv(cand_path, index=False)
    print(f"\nSaved: {cand_path}  ({len(candidates):,} rows)")

    # 10) Summary
    print("\n" + "=" * 70)
    print("Done.")
    print(f"  {all_path}  — full picture, all URLs, all periods")
    print(f"  {cand_path} — filtered optimization candidates with trend labels")
    print("=" * 70)


if __name__ == "__main__":
    main()

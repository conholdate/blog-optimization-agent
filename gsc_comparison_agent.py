"""Structured recommendation helpers for the blog comparison pipeline."""

from __future__ import annotations

import json
import re

import pandas as pd


POSITION_THRESHOLD = 15
RECOMMENDED_ACTION_TITLE_META = "TITLE_META"
RECOMMENDED_ACTION_FULL_REFRESH = "FULL_REFRESH"
RECOMMENDED_ACTION_CONTENT_PLUS_META = "CONTENT_PLUS_META"
RECOMMENDED_ACTION_CODES = {
    RECOMMENDED_ACTION_TITLE_META,
    RECOMMENDED_ACTION_FULL_REFRESH,
    RECOMMENDED_ACTION_CONTENT_PLUS_META,
}
MAX_REASON_WORDS = 8


def min_position_across_periods(row, period_labels):
    """Return the best ranking position observed across available periods."""
    position_cols = [f"position_{p}" for p in period_labels]
    positions = pd.to_numeric([row.get(c) for c in position_cols], errors="coerce")
    valid_positions = positions[~pd.isna(positions)]
    if len(valid_positions) == 0:
        return float("nan")
    return float(valid_positions.min())


def normalize_recommended_action_code(raw_value: str) -> str | None:
    """Normalize structured or legacy action text into a canonical code."""
    if not raw_value:
        return None

    text = " ".join(str(raw_value).strip().split())
    upper = text.upper()
    compact = re.sub(r"[^A-Z0-9]+", "_", upper).strip("_")
    if compact in RECOMMENDED_ACTION_CODES:
        return compact

    lowered = text.lower()
    if "content refresh + title/meta refresh" in lowered:
        return RECOMMENDED_ACTION_CONTENT_PLUS_META
    if "title/meta refresh" in lowered:
        return RECOMMENDED_ACTION_TITLE_META
    if "full content refresh" in lowered:
        return RECOMMENDED_ACTION_FULL_REFRESH

    return None


def fallback_recommended_action(row) -> dict:
    """Deterministic recommendation used when the LLM is unavailable."""
    position = pd.to_numeric(row.get("min_position_any_period"), errors="coerce")
    ctr = pd.to_numeric(row.get("ctr_12m"), errors="coerce")
    trend = str(row.get("trend_pattern", "volatile")).strip().lower()

    if pd.isna(position):
        position = pd.to_numeric(row.get("position_12m"), errors="coerce")

    if pd.isna(position):
        position = 99.0
    if pd.isna(ctr):
        ctr = 0.0

    if position >= 30:
        return {
            "code": RECOMMENDED_ACTION_FULL_REFRESH,
            "reason": "ranking is deep in search results",
        }

    if trend in {"volatile", "declining"}:
        if position >= 20:
            return {
                "code": RECOMMENDED_ACTION_FULL_REFRESH,
                "reason": "volatile or declining trend with weak second-page ranking",
            }
        return {
            "code": RECOMMENDED_ACTION_TITLE_META,
            "reason": "borderline second-page ranking with unstable trend",
        }

    if trend in {"improving", "continuously_good"}:
        if ctr >= 0.025:
            return {
                "code": RECOMMENDED_ACTION_TITLE_META,
                "reason": "healthy trend with acceptable CTR",
            }
        return {
            "code": RECOMMENDED_ACTION_CONTENT_PLUS_META,
            "reason": "improving page needs content and snippet refresh",
        }

    return {
        "code": RECOMMENDED_ACTION_CONTENT_PLUS_META,
        "reason": "balanced refresh needed for content and snippet",
    }


def clean_recommendation_reason(reason: str) -> str:
    """Normalize recommendation text to a short, compact phrase."""
    text = " ".join(str(reason).strip().split())
    text = re.sub(r"^reason:\s*", "", text, flags=re.IGNORECASE)
    text = text.rstrip(".")
    for separator in (";", ",", ". "):
        if separator in text:
            text = text.split(separator, 1)[0].strip()
            break
    words = text.split()
    if len(words) > MAX_REASON_WORDS:
        text = " ".join(words[:MAX_REASON_WORDS])
    return text


def parse_recommendation_response(raw_text: str) -> dict | None:
    """Parse the LLM response into the structured cell value we want to store."""
    if not raw_text:
        return None

    text = raw_text.strip()
    if text.startswith("```"):
        text = text.strip("`").strip()

    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            action = str(
                parsed.get("recommended_action_code")
                or parsed.get("recommended_action")
                or parsed.get("action")
                or ""
            ).strip()
            reason = str(parsed.get("reason", "")).strip()
            code = normalize_recommended_action_code(action)
            if code:
                return {
                    "code": code,
                    "reason": clean_recommendation_reason(reason) if reason else "",
                }
    except json.JSONDecodeError:
        pass

    if "|" in text:
        action, reason = text.split("|", 1)
        code = normalize_recommended_action_code(action)
        reason = clean_recommendation_reason(reason)
        if code:
            return {"code": code, "reason": reason}

    code = normalize_recommended_action_code(text)
    if code:
        return {"code": code, "reason": ""}

    reason = clean_recommendation_reason(text)
    if reason:
        return {"code": RECOMMENDED_ACTION_CONTENT_PLUS_META, "reason": reason}

    return None


_recommendation_cache = {}


def recommend_action_info(row, period_labels, client=None, allow_llm: bool = True) -> dict:
    """Generate a structured action recommendation using deterministic rules."""
    signature = "|".join(
        [
            str(row.get(f"clicks_{label}", "")) for label in period_labels
        ]
        + [
            str(row.get(f"impressions_{label}", "")) for label in period_labels
        ]
        + [
            str(row.get(f"ctr_{label}", "")) for label in period_labels
        ]
        + [
            str(row.get(f"position_{label}", "")) for label in period_labels
        ]
        + [
            str(row.get("trend_pattern", "")),
            str(row.get("min_position_any_period", "")),
            str(row.get("days_since_published", "")),
        ]
    )
    if signature in _recommendation_cache:
        return _recommendation_cache[signature]

    fallback = fallback_recommended_action(row)
    if not allow_llm or client is None:
        _recommendation_cache[signature] = fallback
        return fallback

    _recommendation_cache[signature] = fallback
    return fallback


def recommend_action(row, period_labels, client=None, allow_llm: bool = True) -> str:
    """Backward-compatible wrapper that returns only the action code."""
    return recommend_action_info(row, period_labels, client=client, allow_llm=allow_llm)["code"]


def main():
    print("gsc_comparison_agent.py now exposes structured recommendation helpers.")


if __name__ == "__main__":
    main()


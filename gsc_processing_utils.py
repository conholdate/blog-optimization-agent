import re
import os
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import yaml


# Common non-English prefixes used across blog URLs.
LANGUAGE_PREFIXES = {
    "ar", "ara", "cs", "tag" ,"da", "de", "el", "es", "fa", "fi", "fr", "he", "hi",
    "hu", "id", "it", "ja", "ko", "nl", "no", "pl", "pt", "pt-br", "ro", "ru",
    "sv", "th", "tr", "uk", "ukr", "vi", "zh", "zh-cn", "zh-hans", "zh-hant",
    "zh-tw",
}


def url_to_path(url: str) -> str:
    """Convert URL to normalized path key (without trailing slash)."""
    if not isinstance(url, str) or not url.strip():
        return ""
    parsed = urlparse(url.strip())
    path = parsed.path or ""
    if not path:
        return ""
    if not path.startswith("/"):
        path = "/" + path
    if len(path) > 1:
        path = path.rstrip("/")
    return path


def has_language_prefix(url: str) -> bool:
    """Return True if URL begins with a language code segment."""
    path = url_to_path(url)
    if not path:
        return False
    first_segment = path.strip("/").split("/", 1)[0].lower()
    return first_segment in LANGUAGE_PREFIXES


def parse_publish_date(value):
    """Parse front matter date value into a date object."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value

    text = str(value).strip()
    if not text:
        return None

    parsed = pd.to_datetime(text, errors="coerce", utc=True)
    if pd.isna(parsed):
        # Fallback for YAML-like "YYYY-MM-DD HH:MM:SS +0000 UTC"
        cleaned = re.sub(r"\s+UTC$", "", text, flags=re.IGNORECASE)
        parsed = pd.to_datetime(cleaned, errors="coerce", utc=True)
        if pd.isna(parsed):
            return None
    return parsed.date()


def build_days_since_map(content_root: Path):
    """
    Build mapping of URL path -> days since published from markdown front matter.
    Only index.md files are scanned to match blog post structure.
    """
    today = date.today()
    path_to_days = {}
    parsed_files = 0

    if not content_root or not content_root.exists():
        return path_to_days, parsed_files

    front_matter_pattern = re.compile(r"^---\s*\n(.*?)\n---\s*(?:\n|$)", re.DOTALL)

    for md_file in content_root.rglob("index.md"):
        try:
            text = md_file.read_text(encoding="utf-8")
            match = front_matter_pattern.match(text)
            if not match:
                continue

            metadata = yaml.safe_load(match.group(1)) or {}
            if not isinstance(metadata, dict):
                continue

            url_value = metadata.get("url")
            publish_value = metadata.get("date")
            if not url_value or publish_value is None:
                continue

            post_date = parse_publish_date(publish_value)
            if not post_date:
                continue

            path_key = url_to_path(str(url_value))
            if not path_key:
                continue

            path_to_days[path_key] = (today - post_date).days
            parsed_files += 1
        except Exception:
            continue

    return path_to_days, parsed_files


def resolve_content_root(default_repo_name: str):
    """
    Resolve blog content repo path with this order:
    1) BLOG_CONTENT_ROOT
    2) local sibling/folder guesses around current working directory
    """
    explicit_raw = os.getenv("BLOG_CONTENT_ROOT", "").strip()
    if explicit_raw:
        explicit = Path(explicit_raw).expanduser()
        if explicit.exists():
            return explicit

    candidates = [
        Path.cwd() / default_repo_name,
        Path.cwd().parent / default_repo_name,
        Path.home() / "Documents" / default_repo_name,
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def resolve_content_root_candidates(default_repo_name: str):
    """
    Resolve all possible repo roots in priority order:
    1) BLOG_CONTENT_ROOT (if set and exists)
    2) local fallback candidates
    """
    roots = []
    seen = set()

    explicit_raw = os.getenv("BLOG_CONTENT_ROOT", "").strip()
    if explicit_raw:
        explicit = Path(explicit_raw).expanduser()
        if explicit.exists():
            key = str(explicit.resolve())
            if key not in seen:
                roots.append(explicit)
                seen.add(key)

    for candidate in [
        Path.cwd() / default_repo_name,
        Path.cwd().parent / default_repo_name,
        Path.home() / "Documents" / default_repo_name,
    ]:
        if candidate.exists():
            key = str(candidate.resolve())
            if key not in seen:
                roots.append(candidate)
                seen.add(key)

    return roots


def select_best_days_since_map(default_repo_name: str, page_urls):
    """
    Try all candidate content roots and choose the one with highest URL-path match count.
    Returns:
      (best_root, best_map, parsed_files, matched_count, total_pages, candidates)
    """
    page_paths = [url_to_path(str(url)) for url in page_urls if url_to_path(str(url))]
    total_pages = len(page_paths)
    candidates = resolve_content_root_candidates(default_repo_name)

    best_root = None
    best_map = {}
    best_parsed = 0
    best_matched = -1

    for root in candidates:
        days_map, parsed_files = build_days_since_map(root)
        matched = sum(1 for path in page_paths if path in days_map)
        if matched > best_matched:
            best_root = root
            best_map = days_map
            best_parsed = parsed_files
            best_matched = matched

    if best_matched < 0:
        best_matched = 0

    return best_root, best_map, best_parsed, best_matched, total_pages, candidates

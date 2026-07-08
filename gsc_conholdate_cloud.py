#!/usr/bin/env python3
"""Conholdate Cloud blog Google Search Console comparison runner."""

import os


def set_if_value(key: str, value: str) -> None:
    if value:
        os.environ.setdefault(key, value)


os.environ.setdefault("BLOG_BRAND", "conholdate_cloud")
os.environ.setdefault("GSC_PROPERTY_URL", "https://blog.conholdate.cloud/")
os.environ.setdefault("GSC_SHEET_NAME", "blog.conholdate.cloud")
os.environ.setdefault("GSC_CONTENT_REPO_NAME", "blog.conholdate.cloud")
os.environ.setdefault("GSC_CANDIDATE_FILE_STEM", "conholdate-cloud")
set_if_value(
    "GSC_WEB_APP_URL",
    os.getenv("CONHOLDATE_CLOUD_WEB_APP_URL") or os.getenv("CONHOLDATE_WEB_APP_URL", ""),
)
set_if_value(
    "GSC_SPREADSHEET_ID",
    os.getenv("CONHOLDATE_CLOUD_SPREADSHEET_ID") or os.getenv("CONHOLDATE_SPREADSHEET_ID", ""),
)

from gsc_aspose_com import main


if __name__ == "__main__":
    main()

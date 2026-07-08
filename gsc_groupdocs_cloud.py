#!/usr/bin/env python3
"""GroupDocs Cloud blog Google Search Console comparison runner."""

import os


def set_if_value(key: str, value: str) -> None:
    if value:
        os.environ.setdefault(key, value)


os.environ.setdefault("BLOG_BRAND", "groupdocs_cloud")
os.environ.setdefault("GSC_PROPERTY_URL", "https://blog.groupdocs.cloud/")
os.environ.setdefault("GSC_SHEET_NAME", "blog.groupdocs.cloud")
os.environ.setdefault("GSC_CONTENT_REPO_NAME", "groupdocs-cloud-blog")
os.environ.setdefault("GSC_CANDIDATE_FILE_STEM", "groupdocs-cloud")
set_if_value(
    "GSC_WEB_APP_URL",
    os.getenv("GROUPDOCS_CLOUD_WEB_APP_URL") or os.getenv("GROUPDOCS_WEB_APP_URL", ""),
)
set_if_value(
    "GSC_SPREADSHEET_ID",
    os.getenv("GROUPDOCS_CLOUD_SPREADSHEET_ID") or os.getenv("GROUPDOCS_SPREADSHEET_ID", ""),
)

from gsc_aspose_com import main


if __name__ == "__main__":
    main()

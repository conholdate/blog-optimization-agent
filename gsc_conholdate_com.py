#!/usr/bin/env python3
"""Conholdate blog Google Search Console comparison runner."""

import os


os.environ.setdefault("BLOG_BRAND", "conholdate")
os.environ.setdefault("GSC_PROPERTY_URL", "https://blog.conholdate.com/")
os.environ.setdefault("GSC_SHEET_NAME", "blog.conholdate.com")

from gsc_aspose_com import main


if __name__ == "__main__":
    main()

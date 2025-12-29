Blog Optimizer Agent & GSC Utilities
===========================================

This repo centers on an async **Blog Optimizer** that cleans and governs re-optimization of blog posts across Aspose, Conholdate, and GroupDocs. It also includes lightweight Google Search Console (GSC) scripts that export domain data to Google Sheets with CSV backups.

Quick Start
-----------
1. **Python**: 3.11+ recommended (matches `pyproject.toml`).
2. **Install deps** (venv encouraged):
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```
3. **Credentials**: place `client_secret.json` or `credentials.json` in the repo root. The scripts will serialize `credentials.json` after first auth if needed.
4. **Environment**: create `.env` (or export vars) for the OpenAI agent scripts if you plan to use `blog_optimizer_agent.py`:
   ```
   OPENAI_API_KEY=<token>
   OPENAI_BASE_URL=https://llm.professionalize.com/v1
   ```
   The Search Console scripts only need the Google credentials files.

What’s Inside
-------------
- **blog_optimizer_agent.py (core)**: async blog optimizer that:
  - Loads brand configs (Aspose/Conholdate/GroupDocs) and URL logs
  - Enforces recency rules (min days since publish, min days between optimizations)
  - Cleans and validates Markdown (front matter, lastmod, images/tables, emoji/code fences)
  - Updates per-domain and combined CSV logs, sends metrics to Apps Script endpoints/Sheets
  - Reports run stats (items discovered/succeeded/failed) to two Google Apps Script APIs
- **cleanup_output.py**: cleans generated Markdown under `optimized-posts/`.
- **GSC-*.py**: per-domain Search Console → Sheets pushers with CSV backup:
  - `GSC-aspose.com.py` → `blog.aspose.com` → `csv/aspose.csv`
  - `GSC-aspose.cloud.py` → `blog.aspose.cloud` → `csv/aspose-cloud.csv`
  - `GSC-conholdate.com.py` → `blog.conholdate.com` → `csv/conholdate.csv`
  - `GSC-conholdate.cloud.py` → `blog.conholdate.cloud` → `csv/conholdate-cloud.csv`
  - `GSC-groupdocs.com.py` → `blog.groupdocs.com` → `csv/groupdocs.csv`
  - `GSC-groupdocs.cloud.py` → `blog.groupdocs.cloud` → `csv/groupdocs-cloud.csv`
- **test_llm_connection.py / test_agents.py**: quick connectivity/import checks.

Running the Blog Optimizer (blog_optimizer_agent.py)
----------------------------------------------------
1) Ensure `.env` has `OPENAI_API_KEY` (and optional `OPENAI_BASE_URL`; default `https://llm.professionalize.com/v1`).
2) Place brand CSVs and content folders per `BRAND_CONFIG` in `blog_optimizer_agent.py`.
3) Execute (example):
```bash
python3 blog_optimizer_agent.py --help
```
Review `MIN_DAYS_BETWEEN_OPTIMIZATIONS`, `MIN_DAYS_SINCE_PUBLISH`, log paths under `logs/`, and the Apps Script endpoints/tokens in the file before production use.

Running the Search Console Scripts
----------------------------------
Each script shares the same flow: authenticate → fetch last 180 days → filter to the specific blog domain → drop rows with CTR < 1% → sort by CTR (ascending) → upload to the shared Apps Script → save CSV backup.

Examples:
```bash
python3 GSC-aspose.com.py
python3 GSC-conholdate.cloud.py
python3 GSC-groupdocs.com.py
```

Configuration Notes
-------------------
- **Apps Script endpoint**: all scripts default to the same web app URL and sheet; the Apps Script routes rows by domain. Optional env overrides exist if you ever need them:
  - `ASPOSE_WEB_APP_URL`, `ASPOSE_SPREADSHEET_ID`
  - `ASPOSE_CLOUD_WEB_APP_URL`, `ASPOSE_CLOUD_SPREADSHEET_ID`
  - `CONHOLDATE_CLOUD_WEB_APP_URL`, `CONHOLDATE_CLOUD_SPREADSHEET_ID`
  - `GROUPDOCS_WEB_APP_URL`, `GROUPDOCS_SPREADSHEET_ID`
  - `GROUPDOCS_CLOUD_WEB_APP_URL`, `GROUPDOCS_CLOUD_SPREADSHEET_ID`
- **CTR threshold**: `CTR_THRESHOLD = 0.01` (1%). Adjust in the scripts if needed.
- **Chunk size**: uploads in chunks of 3000 rows to avoid timeouts (`CHUNK_SIZE`).

Outputs
-------
- Google Sheet (via Apps Script) populated by domain.
- Local CSV backups in `csv/` with fixed names:
  - `aspose.csv`, `aspose-cloud.csv`, `conholdate.csv`, `conholdate-cloud.csv`, `groupdocs.csv`, `groupdocs-cloud.csv`

Automation (GitHub Actions)
---------------------------
- **Daily Blog Optimizer** (`.github/workflows/blog-optimizer.yml`): runs every day at 02:30 UTC, then sleeps for a random delay (up to 60 minutes) inside each matrix job before calling `blog_optimizer_agent.py` for every brand. Each job clones the public blog repos:
  - conholdate → `https://github.com/conholdate/conholdate-blog`
  - conholdate-cloud → `https://github.com/conholdate-cloud/blog.conholdate.cloud`
  - aspose → `https://github.com/Aspose/aspose-blog`
  - aspose-cloud → `https://github.com/aspose-cloud/aspose-cloud-blog`
  - groupdocs → `https://github.com/groupdocs/groupdocs-blog`
  - groupdocs-cloud → `https://github.com/groupdocs-cloud/groupdocs-cloud-blog`
  - Required secrets: `OPENAI_API_KEY` (and optional `OPENAI_BASE_URL` if you need a custom endpoint), `BLOG_OPTIMIZER_API_TOKEN`, `BLOGS_TEAM_TOKEN`.
- **GSC Sheets Sync** (`.github/workflows/gsc-sync.yml`): runs on the 1st and 15th of each month at 06:00 UTC and executes all six `GSC-*.py` exporters.
  - Required secret: `GSC_CLIENT_SECRET_JSON` containing the Google OAuth client JSON (as a single-line or multiline secret). Optional `GSC_CREDENTIALS_JSON` lets the workflows reuse a pre-authorized token file to avoid interactive auth.
  - If you need to override the default Apps Script URLs or Sheet IDs, add repo/environment secrets that match the env var names listed above (e.g., `ASPOSE_WEB_APP_URL`, `GROUPDOCS_SPREADSHEET_ID`).

Troubleshooting
---------------
- **Property not found**: the script prints available Search Console properties; ensure the URL matches exactly.
- **No data returned**: check date range and that the property has traffic.
- **Upload errors/timeouts**: reduce `CHUNK_SIZE` or rerun; ensure the Apps Script URL is reachable.
- **Credentials issues**: delete/refresh `credentials.json` if the token expires; keep `client_secret.json` available for re-auth.

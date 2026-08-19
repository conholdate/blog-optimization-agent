Blog Optimizer Agent & GSC Utilities
===========================================

This repo centers on an async **Blog Optimizer** that cleans and governs re-optimization of blog posts across Aspose, Aspose Cloud, Conholdate, Conholdate Cloud, GroupDocs, and GroupDocs Cloud. It also includes multi-period Google Search Console (GSC) comparison/export scripts that build CSV backups and push candidate rows to Google Sheets.

| Links to Related Google Sheets                     
|---------------------------------
| [To Be Optimized Blog Posts](https://docs.google.com/spreadsheets/d/18sYeMy0pYD7-eJxBO674MCpsQy8ACCGnh9RefqPSW_A/edit?gid=831473760#gid=831473760) |
| [Blog Optimization Log](https://docs.google.com/spreadsheets/d/1wh7oEXBhEd35PX8L2er6eY-FH2RB_T04N5ftu9WyRp4/edit?gid=0#gid=0) |

Quick Start
-----------
1. **Python**: 3.11+ recommended (matches `pyproject.toml`).
2. **Install deps** (venv encouraged):
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```
3. **Credentials**: keep `client_secret.json` or `credentials.json` available locally for manual runs, but do not commit them. The scripts will serialize `credentials.json` after first auth if needed.
4. **Environment**: create `.env` (or export vars) for the LLM agent scripts if you plan to use `blog_optimizer_agent.py`:
   ```
   PROFESSIONALIZE_API_KEY_OPTIMIZER=<token>
   PROFESSIONALIZE_BASE_URL=https://llm.professionalize.com/v1
   PROFESSIONALIZE_LLM_MODEL=gpt-oss
   PROFESSIONALIZE_EMBEDDING_MODEL=qwen3-embedding-8b
   AGENT_METRICS_API_KEY=<metrics-api-key>
   BLOGS_TEAM_TOKEN=<blog-team-token>
   BLOGS_TEAM_WEB_APP_URL=<blogs-team-apps-script-url>
   BLOG_OPTIMIZATION_LOG_WEB_APP_URL=<optimization-log-apps-script-url>
   BLOG_OPTIMIZATION_LOG_WEB_APP_SECRET=<shared-secret>
   ```
   The Search Console scripts only need the Google credentials files.

Control Flow and Boundaries
--------------------------
The repo is intentionally split into four operational paths:
- **Optimizer path**: `blog_optimizer_agent.py` discovers eligible posts, applies strict recency gates, rewrites content, validates Hugo formatting, writes results back to the blog repo, and reports run metrics.
- **Search Console export path**: `gsc_*.py` scripts fetch 3m/6m/12m GSC rows, build a comparison CSV, filter candidate rows by CTR and ranking rules, enrich rows with publish-date context, and upload CSV-backed sheet data.
- **Validation path**: `hugo_build_validator.py` and the workflow gates prevent committing Markdown that breaks the target Hugo build.
- **Sync path**: `.github/workflows/gitlab-sync.yml` mirrors `main` to GitLab using a normal push only. It does not force-push; if GitLab `main` diverges, the job fails until the branch is reconciled.

Secrets and sensitive files are handled as transient runtime inputs:
- The metrics API key is sent in the `X-Api-Key` header, not in a URL query string.
- The Apps Script receivers validate a shared secret in the request body and reject unauthenticated writes.
- The optimizer log history is persisted in repo-backed `logs/*.csv` files and is also mirrored to Google Sheets.
- `client_secret.json` and `credentials.json` stay gitignored and are cleaned up by workflow jobs after use.
- The optimizer logs redact secrets and avoid embedding tokens in URLs.

What’s Inside
-------------
- **blog_optimizer_agent.py (core)**: async blog optimizer that:
  - Loads brand configs (Aspose/Conholdate/GroupDocs and cloud variants) and URL logs
  - Enforces recency rules with fail-closed checks on publish date, `lastmod`, and prior optimization history
  - Cleans and validates Markdown (front matter, lastmod, images/tables, emoji/code fences)
  - Updates per-domain and combined CSV logs using canonical URL matching
  - Reports run stats (items discovered/succeeded/failed) to the metrics API
- **cleanup_output.py**: cleans generated Markdown under `optimized-posts/`.
- **gsc_*.py**: per-domain Search Console comparison/export scripts:
  - Fetch 3m, 6m, and 12m windows
  - Build `csv/comparison_all_periods.csv` as the full wide table
  - Filter candidate rows into the brand CSV and upload those rows to Google Sheets
  - Active scripts:
    - `gsc_aspose_com.py` → `blog.aspose.com` → `csv/aspose.csv`
    - `gsc_aspose_cloud.py` → `blog.aspose.cloud` → `csv/aspose-cloud.csv`
    - `gsc_conholdate_com.py` → `blog.conholdate.com` → `csv/conholdate.csv`
    - `gsc_conholdate_cloud.py` → `blog.conholdate.cloud` → `csv/conholdate-cloud.csv`
    - `gsc_groupdocs_com.py` → `blog.groupdocs.com` → `csv/groupdocs.csv`
    - `gsc_groupdocs_cloud.py` → `blog.groupdocs.cloud` → `csv/groupdocs-cloud.csv`
- **gsc_comparison_agent.py**: shared recommendation helpers used by the comparison pipeline.
- **test_llm_connection.py / test_agents.py**: quick connectivity/import checks.

Running the Blog Optimizer (blog_optimizer_agent.py)
----------------------------------------------------
1) Ensure `.env` has `PROFESSIONALIZE_API_KEY_OPTIMIZER` (and optional `PROFESSIONALIZE_BASE_URL`; default `https://llm.professionalize.com/v1`). You can also set `PROFESSIONALIZE_LLM_MODEL` and `PROFESSIONALIZE_EMBEDDING_MODEL` if you need non-defaults.
2) Place brand CSVs and content folders per `BRAND_CONFIG` in `blog_optimizer_agent.py`.
3) Execute with an explicit blog repo path and optional brand:
```bash
python3 blog_optimizer_agent.py \
  --sourcepath /absolute/path/to/conholdate-blog \
  --brand conholdate \
  --limit 3
```
Review `MIN_DAYS_BETWEEN_OPTIMIZATIONS`, `MIN_DAYS_SINCE_PUBLISH`, the required `--sourcepath` argument, the optional `--brand` selector, the `--limit` flag, log paths under `logs/`, and the Apps Script endpoints/tokens in the file before production use. The workflow also exposes a manual `daily_limit_override` dropdown for values `default` or `1` through `10`.
If a post is skipped unexpectedly, check the front matter `lastmod`, the optimization log CSVs under `logs/`, and the matching Google Sheet entry. The optimizer now fails closed if the publish date or history date cannot be parsed.

Running the Search Console Scripts
----------------------------------
Each script now shares the same flow: authenticate with Google Search Console → fetch 3m, 6m, and 12m windows → merge them into `csv/comparison_all_periods.csv` → enrich rows with `days_since_published` from matching `index.md` files when the blog repo is available → filter candidates using the 12m CTR band, high-click exclusion, and minimum position gate → write the brand CSV under `csv/` → upload the candidate rows to Apps Script in chunks.
The exporters now also:
- Keep only English URLs (no language prefix like `/zh/`, `/ru/`, `/de/`, etc.).
- Use `BLOG_CONTENT_ROOT` when provided, then fall back to local repo guesses if it is not set.
- Accept optional overrides such as `GSC_PROPERTY_URL`, `GSC_CONTENT_REPO_NAME`, `GSC_CANDIDATE_FILE_STEM`, `GSC_CANDIDATE_STEM`, and `GSC_SHEET_NAME`.
- Include `position` and `days_since_published` in the upload payload.

Examples:
```bash
export BLOG_CONTENT_ROOT=/absolute/path/to/aspose-blog
python3 gsc_aspose_com.py

export BLOG_CONTENT_ROOT=/absolute/path/to/conholdate-blog
python3 gsc_conholdate_com.py

export BLOG_CONTENT_ROOT=/absolute/path/to/groupdocs-blog
python3 gsc_groupdocs_com.py
```

Note: `BLOG_CONTENT_ROOT` should point to the matching blog repo for the script you run.  
If it points to a different domain repo, `days_since_published` may be blank or partially matched (the scripts log matched URL counts and warnings).
In GitHub Actions, `BLOG_CONTENT_ROOT` is set automatically to the repo that gets checked out in the workflow.

Configuration Notes
-------------------
- **Apps Script endpoint**: all GSC scripts post to a Sheets receiver that validates a shared secret and writes rows from row 2 onward. Required secrets:
  - Shared secret required by the canonical Apps Script receiver: `GSC_WEB_APP_SECRET`
  - `ASPOSE_WEB_APP_URL`, `ASPOSE_SPREADSHEET_ID`
  - `ASPOSE_CLOUD_WEB_APP_URL`, `ASPOSE_CLOUD_SPREADSHEET_ID`
  - `CONHOLDATE_WEB_APP_URL`, `CONHOLDATE_SPREADSHEET_ID`
  - `CONHOLDATE_CLOUD_WEB_APP_URL`, `CONHOLDATE_CLOUD_SPREADSHEET_ID`
  - `GROUPDOCS_WEB_APP_URL`, `GROUPDOCS_SPREADSHEET_ID`
  - `GROUPDOCS_CLOUD_WEB_APP_URL`, `GROUPDOCS_CLOUD_SPREADSHEET_ID`
- **GSC overrides**: `GSC_PROPERTY_URL`, `GSC_CONTENT_REPO_NAME`, `GSC_CANDIDATE_FILE_STEM`, `GSC_CANDIDATE_STEM`, and `GSC_SHEET_NAME` can be used when the defaults do not match your local repo or sheet naming.
- **CTR range filter**: `CTR_THRESHOLD = 0.01` and `CTR_MAX_THRESHOLD = 0.04` (`1% <= CTR <= 4%`).
- **Chunk size**: uploads in chunks of 3000 rows to avoid timeouts (`CHUNK_SIZE`).
- **Apps Script reference**: a canonical deployment script is included at `google-apps-script-webapp.js`.

Outputs
-------
- `csv/comparison_all_periods.csv` with the full 3-window comparison table.
- Google Sheet (via Apps Script) populated by domain.
- Local CSV backups in `csv/` with fixed names:
  - `aspose.csv`, `aspose-cloud.csv`, `conholdate.csv`, `conholdate-cloud.csv`, `groupdocs.csv`, `groupdocs-cloud.csv`

Automation (GitHub Actions)
---------------------------
- **Daily Blog Optimizer** (`.github/workflows/blog-optimizer.yml`): runs every day at 02:30 UTC, then sleeps for a random delay (up to 60 minutes) inside each matrix job before calling `blog_optimizer_agent.py` for every brand. Each job clones the public blog repos:
  - conholdate → `https://github.com/conholdate/conholdate-blog`
  - conholdate-cloud → `https://github.com/conholdate-cloud/blog.conholdate.cloud`
  - aspose → `https://github.com/Aspose/aspose-blog`
  - The workflow currently keeps aspose-cloud/groupdocs entries in code but they are commented out in `blog-optimizer.yml`.
  - Required secrets: `PROFESSIONALIZE_API_KEY_OPTIMIZER` (and optional `PROFESSIONALIZE_BASE_URL` if you need a custom endpoint), `PROFESSIONALIZE_LLM_MODEL`, `PROFESSIONALIZE_EMBEDDING_MODEL`, `AGENT_METRICS_API_KEY`, `BLOGS_TEAM_TOKEN`, `BLOGS_TEAM_WEB_APP_URL`, `BLOG_OPTIMIZATION_LOG_WEB_APP_URL`, `BLOG_OPTIMIZATION_LOG_WEB_APP_SECRET`.
  - Metrics are sent to `https://metrics-api.aspose.app/agents` with `PUT` and the `X-Api-Key` header.
  - Manual runs expose `daily_limit_override` as a dropdown with `default` or `1` through `10`.
  - Before committing optimized Markdown back to a blog repo, the workflow detects that repo's configured Hugo version, installs the same version, and runs a test Hugo build. Build failures write CSV diagnostics to `logs/hugo-build/<brand>_hugo_build_errors.csv` and upload that file as a workflow artifact.
- **GSC Sheets Sync** (`.github/workflows/gsc-sync.yml`): runs on the 1st and 15th of each month at 06:00 UTC and executes all six lowercase `gsc_*.py` exporters.
  - Required secret: `GSC_CLIENT_SECRET_JSON` containing the Google OAuth client JSON (as a single-line or multiline secret). Optional `GSC_CREDENTIALS_JSON` lets the workflows reuse a pre-authorized token file to avoid interactive auth.
  - Add the shared Apps Script secret as `GSC_WEB_APP_SECRET`.
  - If you need to override the default Apps Script URLs or Sheet IDs, add repo/environment secrets that match the env var names listed above (e.g., `ASPOSE_WEB_APP_URL`, `GROUPDOCS_SPREADSHEET_ID`).
- **GitLab mirror sync** (`.github/workflows/gitlab-sync.yml`): pushes `main` to GitLab using `GITLAB_TOKEN` and a temporary credential store. It only performs a normal push to `main`; if GitLab `main` has diverged or is protected against the bot account, the job fails until the branch is reconciled.

Troubleshooting
---------------
- **Property not found**: the script prints available Search Console properties; ensure the URL matches exactly.
- **No data returned**: check date range and that the property has traffic.
- **Upload errors/timeouts**: reduce `CHUNK_SIZE` or rerun; ensure the Apps Script URL is reachable.
- **Credentials issues**: delete/refresh `credentials.json` if the token expires; keep `client_secret.json` available for re-auth.
- **GitLab sync fails on protected branch**: confirm the GitLab token is allowed to push to `main` and that GitLab `main` is still a fast-forward of GitHub `main`.
- **Unexpected optimizer skip**: if a URL is older than 180 days but still skipped, inspect the post's `lastmod` and the optimizer log history. Missing or malformed dates now intentionally stop the optimization instead of allowing it.

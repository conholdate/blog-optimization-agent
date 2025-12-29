import os
import sys
import json
import requests
import searchconsole
import pandas as pd
from datetime import datetime, timedelta

# -----------------------------------------------------------------------------
# Configuration: override with env vars if needed
# -----------------------------------------------------------------------------
WEB_APP_URL = os.getenv(
    "CONHOLDATE_CLOUD_WEB_APP_URL",
    "https://script.google.com/macros/s/AKfycbwcSCl0W0E0SAgz24i-zK20VC50f2akH4tIc2-_yY6nIRyzRH9E23zhHIRB7B-WJ-VDIg/exec",
)
SPREADSHEET_ID = os.getenv(
    "CONHOLDATE_CLOUD_SPREADSHEET_ID",
    "18sYeMy0pYD7-eJxBO674MCpsQy8ACCGnh9RefqPSW_A",
)
CHUNK_SIZE = 3000
CTR_THRESHOLD = 0.01  # 1%


def send_to_google_sheets(rows, is_first_chunk=True):
    """Send data rows to Google Sheets via the Apps Script web app."""
    payload = {
        "action": "import_data",
        "spreadsheetId": SPREADSHEET_ID,
        "rows": rows,
        "clearExisting": is_first_chunk,
    }

    try:
        response = requests.post(
            WEB_APP_URL,
            headers={"Content-Type": "application/json"},
            data=json.dumps(payload),
            timeout=60,
        )
    except requests.exceptions.Timeout:
        print("Upload timed out")
        return False, None
    except Exception as exc:
        print(f"Upload failed: {exc}")
        return False, None

    if response.status_code != 200:
        print(f"Upload HTTP error {response.status_code}: {response.text[:200]}")
        return False, None

    result = response.json()
    if not result.get("success"):
        print(f"Upload error: {result.get('error', 'unknown error')}")
        return False, result

    print(f"Upload ok: {result.get('message')}")
    print(f"Total rows now in sheet: {result.get('total_rows_in_sheet', 0)}")
    return True, result


def main():
    print("=" * 70)
    print("Google Search Console -> Google Sheets (blog.conholdate.cloud)")
    print("=" * 70)

    # 1) Authenticate
    print("\nAuthenticating with Search Console...")
    try:
        if os.path.exists("credentials.json"):
            account = searchconsole.authenticate(credentials="credentials.json")
        else:
            account = searchconsole.authenticate(client_config="client_secret.json")
            account.serialize_credentials("credentials.json")
        print("Auth ok")
    except Exception as exc:
        print(f"Auth failed: {exc}")
        return

    # 2) Property
    property_url = "https://blog.conholdate.cloud/"
    print(f"Property: {property_url}")
    try:
        webproperty = account[property_url]
    except KeyError:
        print("Property not found. Available:", list(account))
        return

    # 3) Date range
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=180)
    print(f"Date range: {start_date} -> {end_date}")

    # 4) Query
    print("Fetching data from Search Console...")
    try:
        report = webproperty.query.range(start_date, end_date).dimension("page").get()
        df = pd.DataFrame(report)
        print(f"Fetched {len(df):,} rows")
    except Exception as exc:
        print(f"Fetch failed: {exc}")
        return

    if df.empty:
        print("No data returned")
        return

    # 5) Process
    if "ctr" not in df.columns:
        df["ctr"] = df["clicks"] / df["impressions"].replace(0, 1)

    df = df[df["page"].str.contains("blog.conholdate.cloud", na=False)]
    original_count = len(df)
    df = df[df["ctr"] >= CTR_THRESHOLD]
    removed = original_count - len(df)
    print(f"Keeping {len(df):,} rows with CTR >= {CTR_THRESHOLD:.2%} (filtered out {removed:,})")

    if df.empty:
        print("All rows filtered out; nothing to send")

    df_sorted = df.sort_values(by="ctr", ascending=True)

    # 6) Prepare rows
    all_rows = [
        {
            "page": str(row["page"]),
            "clicks": float(row["clicks"]),
            "impressions": float(row["impressions"]),
            "ctr": float(row["ctr"]),
        }
        for _, row in df_sorted.iterrows()
    ]

    # 7) Upload
    success = True
    total_sent = 0
    final_result = None
    if all_rows:
        print("Uploading to Google Sheets...")
        total_chunks = (len(all_rows) - 1) // CHUNK_SIZE + 1
        for i in range(0, len(all_rows), CHUNK_SIZE):
            chunk = all_rows[i : i + CHUNK_SIZE]
            chunk_num = i // CHUNK_SIZE + 1
            print(f"Chunk {chunk_num}/{total_chunks} ({len(chunk):,} rows)")
            chunk_ok, result = send_to_google_sheets(chunk, is_first_chunk=(i == 0))
            if not chunk_ok:
                success = False
                break
            total_sent += len(chunk)
            final_result = result
    else:
        print("Skipping upload because there is no data after filtering.")

    # 8) Save CSV
    print("Saving local CSV backup...")
    csv_folder = "csv"
    os.makedirs(csv_folder, exist_ok=True)
    output_filename = os.path.join(csv_folder, "conholdate-cloud.csv")
    df_sorted.to_csv(output_filename, index=False)
    print(f"Saved {output_filename} ({len(df_sorted):,} rows)")

    # 9) Summary
    print("\n" + "=" * 70)
    if success:
        print("Done.")
        if all_rows:
            print(f"Sent {total_sent:,} rows to Sheets.")
            if final_result and "spreadsheet_url" in final_result:
                print(f"Sheet URL: {final_result['spreadsheet_url']}")
        else:
            print("No rows sent (all filtered).")
    else:
        print("Upload failed partway through; CSV backup saved.")
    print("=" * 70)


if __name__ == "__main__":
    if "YOUR_NEW_WEB_APP_ID" in WEB_APP_URL:
        print("Please set WEB_APP_URL to your deployed Apps Script URL.")
        sys.exit(1)
    main()

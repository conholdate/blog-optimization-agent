# File: GSC-Conholdate-FINAL.py
import searchconsole
import pandas as pd
from datetime import datetime, timedelta
import os
import requests
import json
import sys

# Configuration - UPDATE THESE VALUES
WEB_APP_URL = "https://script.google.com/macros/s/AKfycbwcSCl0W0E0SAgz24i-zK20VC50f2akH4tIc2-_yY6nIRyzRH9E23zhHIRB7B-WJ-VDIg/exec"
SPREADSHEET_ID = "18sYeMy0pYD7-eJxBO674MCpsQy8ACCGnh9RefqPSW_A"

def send_to_google_sheets(rows, is_first_chunk=True):
    """
    Send data to Google Sheets via web app.
    is_first_chunk: If True, clears existing data. If False, appends to existing data.
    """
    try:
        payload = {
            "action": "import_data",
            "spreadsheetId": SPREADSHEET_ID,
            "rows": rows,
            "clearExisting": is_first_chunk
        }
        
        response = requests.post(
            WEB_APP_URL,
            headers={"Content-Type": "application/json"},
            data=json.dumps(payload),
            timeout=60
        )
        
        if response.status_code == 200:
            result = response.json()
            
            if result.get('success'):
                print(f"  ✅ {result.get('message')}")
                print(f"  📊 Total in sheet: {result.get('total_rows_in_sheet', 0)} rows")
                return True, result
            else:
                print(f"  ❌ Error: {result.get('error', 'Unknown error')}")
                return False, result
        else:
            print(f"  ❌ HTTP Error: {response.status_code}")
            print(f"  Response: {response.text[:200]}")
            return False, None
            
    except requests.exceptions.Timeout:
        print("  ❌ Request timed out")
        return False, None
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return False, None

def main():
    print("=" * 70)
    print("GOOGLE SEARCH CONSOLE → GOOGLE SHEETS - blog.conholdate.com")
    print("=" * 70)
    
    # --- 1. AUTHENTICATE WITH GOOGLE SEARCH CONSOLE ---
    print("\n🔐 AUTHENTICATING...")
    try:
        if os.path.exists('credentials.json'):
            account = searchconsole.authenticate(credentials='credentials.json')
        else:
            account = searchconsole.authenticate(client_config='client_secret.json')
            account.serialize_credentials('credentials.json')
        print("✅ Authentication successful")
    except Exception as e:
        print(f"❌ Authentication failed: {e}")
        return
    
    # --- 2. SELECT PROPERTY ---
    property_url = 'https://blog.conholdate.com/'
    print(f"🎯 Target: {property_url}")
    
    try:
        webproperty = account[property_url]
    except KeyError:
        print(f"❌ Property not found")
        print("Available properties:", list(account))
        return
    
    # --- 3. DEFINE DATE RANGE ---
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=180)
    print(f"\n📅 Date Range: {start_date} to {end_date}")
    
    # --- 4. QUERY THE API ---
    print(f"\n📡 FETCHING DATA FROM GOOGLE SEARCH CONSOLE...")
    try:
        report = webproperty.query.range(start_date, end_date).dimension('page').get()
        df = pd.DataFrame(report)
        print(f"✅ Retrieved {len(df):,} rows of data")
    except Exception as e:
        print(f"❌ Error fetching data: {e}")
        return
    
    if len(df) == 0:
        print("❌ No data found for the specified date range")
        return
    
    # --- 5. PROCESS DATA ---
    print("\n🔧 PROCESSING DATA...")
    
    # Calculate CTR if not present
    if 'ctr' not in df.columns:
        df['ctr'] = df['clicks'] / df['impressions'].replace(0, 1)
        print("  ✓ Calculated CTR")
    
    # Filter for blog.conholdate.com pages
    df = df[df['page'].str.contains('blog.conholdate.com', na=False)]
    print(f"  ✓ Filtered to {len(df):,} blog pages")
    
    # --- MODIFIED: Filter out rows with CTR less than 1.00% ---
    original_count = len(df)
    df = df[df['ctr'] >= 0.01]  # 1.00% = 0.01 as decimal
    removed_count = original_count - len(df)
    print(f"  ✓ Removed {removed_count:,} rows with CTR < 1.00%")
    print(f"  ✓ {len(df):,} rows remaining with CTR ≥ 1.00%")
    
    # Check if we have any data left after filtering
    if len(df) == 0:
        print("\n⚠️  No data remaining after filtering out CTR < 1.00%")
        print("   The CSV file will be empty and no data will be sent to Google Sheets")
    
    # Sort by CTR in Ascending Order (lowest CTR first)
    df_sorted = df.sort_values(by='ctr', ascending=True)
    print(f"  ✓ Sorted by CTR (lowest first)")
    
    # --- 6. PREPARE DATA FOR GOOGLE SHEETS ---
    print(f"\n📊 PREPARING {len(df_sorted):,} ROWS FOR GOOGLE SHEETS...")
    
    # Convert DataFrame to list of dictionaries
    all_rows = []
    for _, row in df_sorted.iterrows():
        all_rows.append({
            "page": str(row['page']),
            "clicks": float(row['clicks']),
            "impressions": float(row['impressions']),
            "ctr": float(row['ctr'])
        })
    
    # Show sample of data (if any exists)
    if all_rows:
        print("\n📋 SAMPLE DATA (first 3 rows):")
        for i in range(min(3, len(all_rows))):
            row = all_rows[i]
            print(f"  {i+1}. {row['page'][:60]}...")
            print(f"     Clicks: {row['clicks']:,} | Impressions: {row['impressions']:,} | CTR: {row['ctr']:.2%}")
    else:
        print("\n📋 No data to show (all rows had CTR < 1.00%)")
    
    # --- 7. SEND TO GOOGLE SHEETS ---
    if all_rows:
        print(f"\n⬆️  SENDING TO GOOGLE SHEETS...")
        
        # Send in chunks to avoid timeouts
        CHUNK_SIZE = 3000  # Adjust based on your needs
        total_chunks = (len(all_rows) - 1) // CHUNK_SIZE + 1
        total_sent = 0
        success = True
        final_result = None
        
        for i in range(0, len(all_rows), CHUNK_SIZE):
            chunk = all_rows[i:i + CHUNK_SIZE]
            chunk_num = i // CHUNK_SIZE + 1
            
            print(f"\n  📦 CHUNK {chunk_num}/{total_chunks} ({len(chunk):,} rows)")
            
            is_first_chunk = (i == 0)
            chunk_success, result = send_to_google_sheets(chunk, is_first_chunk)
            
            if chunk_success:
                total_sent += len(chunk)
                final_result = result
            else:
                success = False
                print(f"  ❌ FAILED AT CHUNK {chunk_num}")
                break
    else:
        print(f"\n⬆️  SKIPPING GOOGLE SHEETS UPLOAD (no data)")
        success = True  # Still consider it a success
        total_sent = 0
    
    # --- 8. SAVE LOCAL CSV BACKUP ---
    print("\n💾 SAVING LOCAL BACKUP...")
    csv_folder = 'csv'
    if not os.path.exists(csv_folder):
        os.makedirs(csv_folder)
    
    output_filename = os.path.join(csv_folder, "conholdate.csv")
    df_sorted.to_csv(output_filename, index=False)
    print(f"  📁 Saved: {output_filename}")
    print(f"  📊 File contains {len(df_sorted):,} rows (CTR ≥ 1.00%)")
    
    # --- 9. SUMMARY ---
    print("\n" + "=" * 70)
    if success:
        print("🎉 SUCCESS!")
        print("=" * 70)
        if all_rows:
            print(f"✅ Total rows sent: {total_sent:,}/{len(df_sorted):,}")
            if final_result and 'spreadsheet_url' in final_result:
                print(f"🔗 Google Sheet: {final_result['spreadsheet_url']}")
            print(f"📊 Sheet name: blog.conholdate.com")
            print(f"📈 Total in sheet: {final_result.get('total_rows_in_sheet', 'N/A') if final_result else 'N/A'} rows")
        else:
            print("✅ No rows sent (all data filtered out)")
        print(f"📁 Local CSV saved: {output_filename}")
        print(f"📊 CSV contains: {len(df_sorted):,} rows (CTR ≥ 1.00%)")
        print(f"🚫 Rows filtered out: {removed_count:,} rows (CTR < 1.00%)")
    else:
        print("⚠️  PARTIAL SUCCESS")
        print("=" * 70)
        if all_rows:
            print(f"📈 Rows sent: {total_sent:,}/{len(df_sorted):,}")
        else:
            print(f"📈 No rows sent (all data filtered out)")
        print(f"📁 Local backup saved: {output_filename}")
        print(f"📊 CSV contains: {len(df_sorted):,} rows (CTR ≥ 1.00%)")
        print(f"🚫 Rows filtered out: {removed_count:,} rows (CTR < 1.00%)")
    
    print(f"⏰ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

if __name__ == "__main__":
    # Quick check for required configuration
    if "YOUR_NEW_WEB_APP_ID" in WEB_APP_URL:
        print("❌ ERROR: Please update WEB_APP_URL with your actual web app URL")
        print("1. Deploy the Google Apps Script above")
        print("2. Copy the web app URL")
        print("3. Update WEB_APP_URL in this script")
        sys.exit(1)
    
    main()

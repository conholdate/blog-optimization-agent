import searchconsole
import pandas as pd
from datetime import datetime, timedelta
import os

# --- 1. AUTHENTICATE ---
if os.path.exists('credentials.json'):
    account = searchconsole.authenticate(credentials='credentials.json')
else:
    account = searchconsole.authenticate(client_config='client_secret.json')
    account.serialize_credentials('credentials.json')

# --- 2. SELECT PROPERTY ---
print("Available properties:", list(account))
property_url = 'https://blog.conholdate.cloud/'
webproperty = account[property_url]

# --- 3. MULTI-FACETED DATA DIAGNOSTICS ---
end_date = datetime.now().date()
test_start_date = end_date - timedelta(days=30)  # Start with 30 days

print("\n" + "="*60)
print("COMPREHENSIVE DATA DIAGNOSTICS FOR BLOG.CONHOLDATE.CLOUD")
print("="*60)

# TEST 1: Different query configurations
print("\n🔍 TEST 1: Testing different query configurations...")

test_configs = [
    {"name": "Basic query (page only)", "dims": ["page"]},
    {"name": "With query dimension", "dims": ["page", "query"]},
    {"name": "With country dimension", "dims": ["page", "country"]},
    {"name": "With device dimension", "dims": ["page", "device"]},
    {"name": "Page + Date", "dims": ["page", "date"]},
]

for config in test_configs:
    print(f"\n  Testing: {config['name']}")
    try:
        report = webproperty.query.range(test_start_date, end_date) \
                                .dimension(*config['dims']) \
                                .limit(100) \
                                .get()
        row_count = len(report.rows) if report.rows else 0
        print(f"    Rows returned: {row_count}")
        if row_count > 0 and 'page' in config['dims']:
            if hasattr(report.rows[0], 'page'):
                page_preview = report.rows[0].page[:80] + "..." if len(report.rows[0].page) > 80 else report.rows[0].page
                print(f"    Sample page: {page_preview}")
            else:
                print("    No page attribute")
    except Exception as e:
        print(f"    Error: {e}")

# TEST 2: Different date ranges
print("\n📅 TEST 2: Testing different date ranges...")

date_ranges = [
    ("Last 7 days", 7),
    ("Last 14 days", 14),
    ("Last 30 days", 30),
    ("Last 90 days", 90),
]

for range_name, days in date_ranges:
    start = end_date - timedelta(days=days)
    print(f"\n  {range_name} ({start} to {end_date})")
    try:
        report = webproperty.query.range(start, end_date) \
                                .dimension('page') \
                                .limit(100) \
                                .get()
        row_count = len(report.rows) if report.rows else 0
        print(f"    Rows returned: {row_count}")
        if row_count > 0:
            # Show date range of actual data if available
            if hasattr(report.rows[0], 'date'):
                dates = [row.date for row in report.rows if hasattr(row, 'date')]
                if dates:
                    print(f"    Date range in data: {min(dates)} to {max(dates)}")
    except Exception as e:
        print(f"    Error: {e}")

# TEST 3: Check data thresholds
print("\n📊 TEST 3: Checking data aggregation and thresholds...")

# Try to get aggregated data first
print("\n  Total site performance (no dimensions):")
try:
    aggregated = webproperty.query.range(test_start_date, end_date).get()
    if hasattr(aggregated, 'rows') and aggregated.rows:
        for row in aggregated.rows:
            print(f"    Total Clicks: {getattr(row, 'clicks', 'N/A')}, "
                  f"Total Impressions: {getattr(row, 'impressions', 'N/A')}")
except Exception as e:
    print(f"    Error: {e}")

# TEST 4: Test with different filters
print("\n🎯 TEST 4: Testing with search appearance filter...")
try:
    # Check if there are pages with specific search appearances
    report = webproperty.query.range(test_start_date, end_date) \
                            .dimension('page', 'searchAppearance') \
                            .limit(50) \
                            .get()
    
    if report.rows:
        print(f"  Rows with search appearance: {len(report.rows)}")
        # Group by search appearance
        appearances = {}
        for row in report.rows:
            if hasattr(row, 'searchAppearance'):
                appearance = row.searchAppearance
                appearances[appearance] = appearances.get(appearance, 0) + 1
        
        if appearances:
            print("  Search appearances found:")
            for app, count in list(appearances.items())[:5]:  # Show top 5
                print(f"    - {app}: {count} rows")
    else:
        print("  No data with search appearance dimension")
except Exception as e:
    print(f"  Error: {e}")

# TEST 5: Direct API property check
print("\n🔧 TEST 5: Property configuration check...")
try:
    # Try to get property info
    print(f"  Property URL: {property_url}")
    print(f"  Property type: {type(webproperty)}")
    
    # Try a simple test with minimal filtering
    print("\n  Minimal test - last 3 days only:")
    test_end = end_date
    test_start = test_end - timedelta(days=3)
    
    simple_report = webproperty.query.range(test_start, test_end) \
                                  .dimension('page') \
                                  .get()
    
    print(f"    Date range: {test_start} to {test_end}")
    print(f"    Rows returned: {len(simple_report.rows) if simple_report.rows else 0}")
    
    if simple_report.rows:
        print(f"\n    First 3 pages found:")
        for i, row in enumerate(simple_report.rows[:3]):
            if hasattr(row, 'page'):
                print(f"      {i+1}. {row.page}")
            if hasattr(row, 'clicks'):
                print(f"         Clicks: {row.clicks}, Impressions: {row.impressions}")
except Exception as e:
    print(f"  Error: {e}")

# TEST 6: Try alternative property format
print("\n🔄 TEST 6: Testing alternative property formats...")
alternative_urls = [
    'https://blog.conholdate.cloud',  # without trailing slash
    'sc-domain:blog.conholdate.cloud',  # domain property format
]

for alt_url in alternative_urls:
    print(f"\n  Testing: {alt_url}")
    try:
        alt_property = account[alt_url]
        test_report = alt_property.query.range(test_start_date, end_date) \
                                    .dimension('page') \
                                    .limit(10) \
                                    .get()
        print(f"    Success! Rows: {len(test_report.rows) if test_report.rows else 0}")
    except Exception as e:
        print(f"    Failed: {str(e)[:80]}...")

print("\n" + "="*60)
print("DIAGNOSTICS COMPLETE")
print("="*60)

# Final summary query
print("\n📋 FINAL SUMMARY: Last 30 days, all pages")
try:
    final_report = webproperty.query.range(test_start_date, end_date) \
                                  .dimension('page') \
                                  .limit(5000) \
                                  .get()
    
    total_rows = len(final_report.rows) if final_report.rows else 0
    print(f"Total unique pages with data: {total_rows}")
    
    if total_rows > 0:
        total_clicks = sum(getattr(row, 'clicks', 0) for row in final_report.rows)
        total_impressions = sum(getattr(row, 'impressions', 0) for row in final_report.rows)
        print(f"Total clicks: {total_clicks}")
        print(f"Total impressions: {total_impressions}")
        
        # Show distribution
        clicks_gt_zero = sum(1 for row in final_report.rows if getattr(row, 'clicks', 0) > 0)
        impressions_gt_zero = sum(1 for row in final_report.rows if getattr(row, 'impressions', 0) > 0)
        print(f"Pages with clicks > 0: {clicks_gt_zero}")
        print(f"Pages with impressions > 0: {impressions_gt_zero}")
        
        if total_rows <= 20:  # If small dataset, show all
            print(f"\nAll pages found:")
            for i, row in enumerate(final_report.rows):
                page = getattr(row, 'page', 'N/A')
                clicks = getattr(row, 'clicks', 0)
                impressions = getattr(row, 'impressions', 0)
                print(f"  {i+1}. {page}")
                print(f"     Clicks: {clicks}, Impressions: {impressions}")
                
except Exception as e:
    print(f"Error in final query: {e}")

print("\n✅ Diagnostics complete. Review the results above.")
# ================================================
# blog_optimizer_agent.py — Blog Optimizer with Tracking
# ================================================

import sys
import os
import re
import warnings
import argparse
from bs4 import XMLParsedAsHTMLWarning
from datetime import date, datetime, timedelta, timezone
import csv
from pathlib import Path
import time
import json
import random

# For API call
import requests

# Suppress the XML warning
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

print(f"Python: {sys.executable}")

# Import packages
from openai import AsyncOpenAI
from dotenv import load_dotenv
import yaml
import asyncio

print("All imports successful")

# ----------------------------------------------------
# 1. Load environment
# ----------------------------------------------------
load_dotenv()

# Prefer per-agent key; fall back to legacy name for compatibility.
llmToken = os.getenv("PROFESSIONALIZE_API_KEY_OPTIMIZER") or os.getenv("PROFESSIONALIZE_API_KEY")
llmBase = os.getenv("PROFESSIONALIZE_BASE_URL", "https://llm.professionalize.com/v1")

if not llmToken:
    print("ERROR: PROFESSIONALIZE_API_KEY_OPTIMIZER (or legacy PROFESSIONALIZE_API_KEY) not found in environment (.env or secrets).")
    exit(1)

print(f"LLM Endpoint: {llmBase}")

# ----------------------------------------------------
# 2. Initialize client
# ----------------------------------------------------
client = AsyncOpenAI(api_key=llmToken, base_url=llmBase)
MODEL_NAME = os.getenv("PROFESSIONALIZE_LLM_MODEL", "gpt-oss")
EMBEDDING_MODEL = os.getenv("PROFESSIONALIZE_EMBEDDING_MODEL", "qwen3-embedding-8b")
print(f"Using model: {MODEL_NAME}")

# Configuration
MIN_DAYS_BETWEEN_OPTIMIZATIONS = 90  # 3 months
MIN_DAYS_SINCE_PUBLISH = 180  # 6 months (approximately 180 days)
LOG_DIR = "logs"
LOG_FILE_COMBINED = "all_domains_log.csv"

# API Configuration
API_ENDPOINT = "https://script.google.com/macros/s/AKfycbyCHwElrM6RcYLi0JNQAkJmzGrBjAhf28mKXVyub_6SdaZ2ITvzCwfM5xCLE7rmuxio/exec"
API_TOKEN = os.getenv("BLOG_OPTIMIZER_API_TOKEN")

# Blogs Team Metrics Configuration
BLOGS_TEAM_ENDPOINT = "https://script.google.com/macros/s/AKfycbwYyPBs3ox6xhYfznVpu4Gh8T4l7cXrAIj1m_y1g-vWn6tyP_LAkv3eo6W2EZYAeHgLag/exec"
BLOGS_TEAM_TOKEN = os.getenv("BLOGS_TEAM_TOKEN")

# Brand configuration
# Brand configuration
BRAND_CONFIG = {
    # Aspose brands
    'aspose': {
        'csv_file': 'csv/aspose.csv',
        'content_folder': 'Aspose.Blog',
        'domains': ['blog.aspose.com']
    },
    'aspose-cloud': {
        'csv_file': 'csv/aspose-cloud.csv',
        'content_folder': 'Aspose.Cloud',
        'domains': ['blog.aspose.cloud']
    },
    
    # Conholdate brands
    'conholdate': {
        'csv_file': 'csv/conholdate.csv',
        'content_folder': 'Conholdate.Total',
        'domains': ['blog.conholdate.com']
    },
    'conholdate-cloud': {
        'csv_file': 'csv/conholdate-cloud.csv',
        'content_folder': 'Conholdate.Cloud',
        'domains': ['blog.conholdate.cloud']
    },
    
    # GroupDocs brands
    'groupdocs': {
        'csv_file': 'csv/groupdocs.csv',
        'content_folder': 'GroupDocs.Blog',
        'domains': ['blog.groupdocs.com']
    },
    'groupdocs-cloud': {
        'csv_file': 'csv/groupdocs-cloud.csv',
        'content_folder': 'GroupDocs.Cloud',
        'domains': ['blog.groupdocs.cloud']
    }
}


def get_website_for_brand(brand: str) -> str:
    """Map brand key to website used by metrics endpoint."""
    brand_to_website = {
        "aspose": "aspose.com",
        "aspose-cloud": "aspose.cloud",
        "conholdate": "conholdate.com",
        "conholdate-cloud": "conholdate.cloud",
        "groupdocs": "groupdocs.com",
        "groupdocs-cloud": "groupdocs.cloud",
    }
    return brand_to_website.get(brand, "unknown")


def extract_url_first_segment(url: str) -> str:
    """Extract first path segment from URL in lowercase."""
    try:
        path = ""
        if "://" in url:
            right = url.split("://", 1)[1]
            path = right.split("/", 1)[1] if "/" in right else ""
        else:
            path = url.split("/", 1)[1] if "/" in url else ""
        if not path:
            return ""
        return path.split("/", 1)[0].strip().lower()
    except Exception:
        return ""


def derive_family_name_from_md(md_file_path: Path) -> str:
    """
    Derive product family from markdown front matter categories.
    Expected category examples:
      "Aspose.PDF Product Family"
      "Conholdate.Total Product Family"
    """
    try:
        with open(md_file_path, "r", encoding="utf-8") as f:
            content = f.read()

        match = re.match(r'^---\s*\n(.*?)\n---\s*\n', content, re.DOTALL)
        if not match:
            return None

        metadata = yaml.safe_load(match.group(1)) or {}
        categories = metadata.get("categories", [])
        if isinstance(categories, str):
            categories = [categories]
        if not isinstance(categories, list):
            return None

        for cat in categories:
            if not isinstance(cat, str):
                continue
            normalized = " ".join(cat.strip().split())
            fam_match = re.search(r'([A-Za-z][A-Za-z0-9]*(?:\.[A-Za-z0-9]+)+)\s+Product\s+Family$', normalized, re.IGNORECASE)
            if fam_match:
                return fam_match.group(1)
        return None
    except Exception as e:
        print(f"Warning: Unable to derive family from {md_file_path}: {e}")
        return None


def ensure_family_metrics_bucket(metrics: dict, family_name: str) -> dict:
    """Get or initialize per-family metrics bucket."""
    if "family_metrics" not in metrics:
        metrics["family_metrics"] = {}
    if family_name not in metrics["family_metrics"]:
        metrics["family_metrics"][family_name] = {
            "items_discovered": 0,
            "items_succeeded": 0,
            "items_failed": 0,
            "token_usage": 0,
            "api_call_count": 0,
            "limit_reached": 0,
        }
    return metrics["family_metrics"][family_name]

# ----------------------------------------------------
# 3. Domain Detection & Logging Functions
# ----------------------------------------------------
def extract_domain_info(url: str, brand: str = None):
    """Extract domain information from URL."""
    try:
        # Remove protocol and get domain
        domain = url.split('://')[1].split('/')[0] if '://' in url else url.split('/')[0]
        
        # If brand is specified, use it directly
        if brand and brand in BRAND_CONFIG:
            company = brand
        else:
            # Determine company from domain
            if 'aspose' in domain:
                company = 'aspose'
            elif 'groupdocs' in domain:
                company = 'groupdocs'
            elif 'conholdate' in domain:
                company = 'conholdate'
            else:
                company = 'other'
        
        # Create sanitized filename
        sanitized_domain = domain.replace('.', '_')
        log_filename = f"{sanitized_domain}.csv"
        
        return {
            'full_domain': domain,
            'company': company,
            'log_filename': log_filename,
            'sanitized_domain': sanitized_domain
        }
    except:
        return {
            'full_domain': 'unknown',
            'company': 'other' if not brand else brand,
            'log_filename': 'unknown.csv',
            'sanitized_domain': 'unknown'
        }

def get_log_file_path(domain_info: dict):
    """Get the log file path for a specific domain."""
    company = domain_info['company']
    log_filename = domain_info['log_filename']
    
    # Create directory path
    log_dir = Path(LOG_DIR) / company
    log_dir.mkdir(parents=True, exist_ok=True)
    
    return log_dir / log_filename, log_dir

def load_optimization_log_for_domain(domain_info: dict):
    """Load the optimization log for a specific domain from CSV file."""
    log_file_path, _ = get_log_file_path(domain_info)
    log_data = {}
    
    if not log_file_path.exists():
        return log_data
    
    try:
        with open(log_file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                slug = row.get('slug', '').strip()
                last_optimized = row.get('last_optimized', '').strip()
                url = row.get('url', '').strip()
                if slug and last_optimized:
                    log_data[slug] = {
                        'last_optimized': last_optimized,
                        'url': url
                    }
    except Exception as e:
        print(f"Error loading optimization log {log_file_path}: {e}")
    
    return log_data

def load_all_domains_log():
    """Load the combined log file with all domains."""
    log_file_path = Path(LOG_DIR) / LOG_FILE_COMBINED
    log_data = {}
    
    if not log_file_path.exists():
        return log_data
    
    try:
        with open(log_file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                slug = row.get('slug', '').strip()
                last_optimized = row.get('last_optimized', '').strip()
                url = row.get('url', '').strip()
                domain = row.get('domain', '').strip()
                if slug and last_optimized:
                    log_data[slug] = {
                        'last_optimized': last_optimized,
                        'url': url,
                        'domain': domain
                    }
    except Exception as e:
        print(f"Error loading combined log: {e}")
    
    return log_data

def save_optimization_log_for_domain(domain_info: dict, log_data: dict):
    """Save the optimization log for a specific domain to CSV file."""
    log_file_path, _ = get_log_file_path(domain_info)
    
    try:
        with open(log_file_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['slug', 'url', 'domain', 'last_optimized'])
            writer.writeheader()
            for slug, data in sorted(log_data.items()):
                writer.writerow({
                    'slug': slug,
                    'url': data.get('url', ''),
                    'domain': domain_info['full_domain'],
                    'last_optimized': data.get('last_optimized', '')
                })
    except Exception as e:
        print(f"Error saving optimization log {log_file_path}: {e}")

def save_to_combined_log(domain_info: dict, slug: str, url: str, last_optimized: str):
    """Save an entry to the combined log file."""
    log_file_path = Path(LOG_DIR) / LOG_FILE_COMBINED
    
    # Ensure directory exists
    log_file_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Read existing data
    existing_data = []
    if log_file_path.exists():
        try:
            with open(log_file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                existing_data = list(reader)
        except:
            existing_data = []
    
    # Update or add entry
    updated = False
    for row in existing_data:
        if row.get('slug') == slug and row.get('domain') == domain_info['full_domain']:
            row['last_optimized'] = last_optimized
            row['url'] = url
            updated = True
            break
    
    if not updated:
        existing_data.append({
            'slug': slug,
            'url': url,
            'domain': domain_info['full_domain'],
            'last_optimized': last_optimized
        })
    
    # Write back
    try:
        with open(log_file_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['slug', 'url', 'domain', 'last_optimized'])
            writer.writeheader()
            writer.writerows(existing_data)
    except Exception as e:
        print(f"Error saving to combined log: {e}")

    send_to_google_sheet(domain_info, slug, url, last_optimized)

def update_optimization_log(domain_info: dict, slug: str, url: str):
    """Update the optimization log for a specific slug and domain."""
    # Update domain-specific log
    log_data = load_optimization_log_for_domain(domain_info)
    today_str = str(date.today())
    log_data[slug] = {
        'last_optimized': today_str,
        'url': url
    }
    save_optimization_log_for_domain(domain_info, log_data)
    
    # Update combined log
    save_to_combined_log(domain_info, slug, url, today_str)
    
    print(f"Updated log for '{slug}' from {domain_info['full_domain']}: {today_str}")

def can_optimize_slug(domain_info: dict, slug: str, publish_date = None):
    """Check if a slug can be optimized based on last optimization date and publish date."""
    
    # Rule 1: Check if post is at least MIN_DAYS_SINCE_PUBLISH days old
    if publish_date:
        try:
            # Handle both string and datetime.date objects
            if isinstance(publish_date, str):
                # Try multiple date formats
                post_date = None
                date_formats = [
                    '%Y-%m-%d',                    # 2025-10-02
                    '%a, %d %b %Y %H:%M:%S %z',   # Thu, 02 Oct 2025 00:11:25 +0000
                    '%a, %d %b %Y %H:%M:%S GMT',  # Thu, 31 Oct 2024 00:16:02 GMT
                    '%a, %d %b %Y %H:%M:%S %Z',   # Thu, 31 Oct 2024 00:16:02 UTC/GMT
                    '%d %b %Y',                    # 02 Oct 2025
                    '%b %d, %Y',                   # Oct 02, 2025
                    '%m/%d/%Y',                    # 10/02/2025
                    '%Y/%m/%d',                    # 2025/10/02
                ]
                
                for date_format in date_formats:
                    try:
                        # For formats with timezone, parse as datetime first
                        if '%z' in date_format or '%H' in date_format:
                            dt = datetime.strptime(publish_date, date_format)
                            post_date = dt.date()
                        else:
                            post_date = datetime.strptime(publish_date, date_format).date()
                        break  # Successfully parsed
                    except ValueError:
                        continue

                # Extra fallback: handle "GMT" by converting to +0000
                if not post_date and publish_date.endswith(" GMT"):
                    try:
                        normalized = publish_date[:-4] + " +0000"
                        dt = datetime.strptime(normalized, '%a, %d %b %Y %H:%M:%S %z')
                        post_date = dt.date()
                    except ValueError:
                        pass
                
                if not post_date:
                    print(f"Warning: Could not parse publish date: {publish_date}")
                    # Skip this check if we can't parse the date
                    post_date = None
                    
            elif isinstance(publish_date, datetime):
                # Convert datetime to date (datetime is also a date subclass, so check first)
                post_date = publish_date.date()
            elif isinstance(publish_date, date):
                # Already a date object
                post_date = publish_date
            else:
                # Unknown format, skip this check
                print(f"Warning: Unknown publish date format: {type(publish_date)}")
                post_date = None
            
            if post_date:
                today = date.today()
                days_since_publish = (today - post_date).days
                
                if days_since_publish < MIN_DAYS_SINCE_PUBLISH:
                    remaining_days = MIN_DAYS_SINCE_PUBLISH - days_since_publish
                    return False, f"Post is only {days_since_publish} days old. Wait {remaining_days} more days."
        except (ValueError, AttributeError) as e:
            # If date format is invalid, continue with other checks
            print(f"Warning: Error processing publish date {publish_date}: {e}")
    
    # Rule 2: First check domain-specific log
    domain_log_data = load_optimization_log_for_domain(domain_info)
    
    if slug in domain_log_data:
        last_optimized_str = domain_log_data[slug].get('last_optimized', '')
        if last_optimized_str:
            try:
                last_optimized = datetime.strptime(last_optimized_str, '%Y-%m-%d').date()
                today = date.today()
                days_since_last_opt = (today - last_optimized).days
                
                if days_since_last_opt >= MIN_DAYS_BETWEEN_OPTIMIZATIONS:
                    return True, f"Last optimized {days_since_last_opt} days ago (domain log)"
                else:
                    remaining_days = MIN_DAYS_BETWEEN_OPTIMIZATIONS - days_since_last_opt
                    return False, f"Optimized {days_since_last_opt} days ago. Can optimize again in {remaining_days} days. (domain log)"
            except ValueError:
                # If date format is invalid, allow optimization
                return True, "Invalid date in domain log, allowing optimization"
    
    # Rule 3: If not found in domain log, check combined log
    combined_log_data = load_all_domains_log()
    
    if slug in combined_log_data:
        last_optimized_str = combined_log_data[slug].get('last_optimized', '')
        if last_optimized_str:
            try:
                last_optimized = datetime.strptime(last_optimized_str, '%Y-%m-%d').date()
                today = date.today()
                days_since_last_opt = (today - last_optimized).days
                
                if days_since_last_opt >= MIN_DAYS_BETWEEN_OPTIMIZATIONS:
                    return True, f"Last optimized {days_since_last_opt} days ago (combined log)"
                else:
                    remaining_days = MIN_DAYS_BETWEEN_OPTIMIZATIONS - days_since_last_opt
                    return False, f"Optimized {days_since_last_opt} days ago. Can optimize again in {remaining_days} days. (combined log)"
            except ValueError:
                # If date format is invalid, allow optimization
                return True, "Invalid date in combined log, allowing optimization"
    
    # Rule 4: If not found in any log
    return True, "Never optimized before"

def send_to_google_sheet(domain_info: dict, slug: str, url: str, last_optimized: str):
    """
    Send log data to Google Sheet via Apps Script web app.
    """
    try:
        GOOGLE_SHEET_ENDPOINT = "https://script.google.com/macros/s/AKfycbwDr1dcuFlm2IYiW9Zpemu9Fb8HchVDD2Lh6KkmGLmMsvMmtyT8d0GrWhD1YgwdMvxULw/exec"
        
        payload = {
            "slug": slug,
            "url": url,
            "domain": domain_info['full_domain'],
            "last_optimized": last_optimized
        }
        
        response = requests.post(
            GOOGLE_SHEET_ENDPOINT,
            headers={"Content-Type": "application/json"},
            data=json.dumps(payload),
            timeout=10
        )
        
        if response.status_code == 200:
            print(f"✓ Data sent to Google Sheet successfully!")
            return True
        else:
            print(f"✗ Google Sheet API error: {response.status_code} - {response.text[:100]}")
            return False
            
    except Exception as e:
        print(f"Error sending to Google Sheet: {e}")
        return False
    
def count_optimizations_today_for_domain(domain_info: dict, limit_settings: dict):
    """Count how many URLs have been optimized today for this domain."""
    today_str = str(date.today())
    domain_key = domain_info['full_domain']
    
    # Check if we have a cached count
    if domain_key in limit_settings.get('today_counts', {}):
        if limit_settings['today_counts'][domain_key]['date'] == today_str:
            return limit_settings['today_counts'][domain_key]['count']
    
    # If not cached, count from logs
    log_data = load_optimization_log_for_domain(domain_info)
    
    today_count = 0
    for slug, data in log_data.items():
        if data.get('last_optimized') == today_str:
            today_count += 1
    
    # Update cache
    if 'today_counts' not in limit_settings:
        limit_settings['today_counts'] = {}
    limit_settings['today_counts'][domain_key] = {
        'date': today_str,
        'count': today_count
    }
    
    return today_count

def check_daily_limit(domain_info: dict, limit_settings: dict):
    """Check if the daily limit for this domain has been reached."""
    limit_per_domain = limit_settings.get('limit_per_domain', 10)
    domain_key = domain_info['full_domain']
    
    # Get today's count for this domain
    today_count = count_optimizations_today_for_domain(domain_info, limit_settings)
    
    # If limit is 0, it means unlimited - always return True
    if limit_per_domain == 0:
        return True, f"No limit set (0 = unlimited). Already optimized today: {today_count}"
    
    # Check if we've reached the limit (only if limit > 0)
    if today_count >= limit_per_domain:
        return False, f"Daily limit ({limit_per_domain}) reached for domain {domain_key}. {today_count} URLs already optimized today."
    
    return True, f"{today_count} out of {limit_per_domain} URLs optimized today for {domain_key}."

def has_language_code_prefix(url: str):
    """
    Check if URL has a language code prefix using smart regex.
    Only filters actual language codes, not product categories.
    """
    try:
        # Extract the first path segment
        if '://' in url:
            path = url.split('://')[1].split('/', 1)[1] if '/' in url.split('://')[1] else ''
        else:
            path = url.split('/', 1)[1] if '/' in url else ''
        
        if not path:
            return False
        
        first_segment = path.split('/')[0].lower()
        
        # Product categories that should NEVER be filtered
        # These are actual content categories, not language codes
        product_categories = {
            # Aspose
            'cad', 'pdf', 'words', 'cells', 'slides', 'email', 'note', 'diagram',
            'html', 'image', 'barcode', 'ocr', 'psd', 'tasks', 'three', 'gis',
            'omr', 'page', 'pub', 'svg', 'tex', 'visio', 'web', 'finance',
            # Conholdate
            'total',
            # GroupDocs
            'comparison', 'merger', 'annotation', 'conversion', 'signature',
            'viewer', 'parser', 'assembly', 'editor', 'metadata',
        }
        
        if first_segment in product_categories:
            return False
        
        # Special segments to always filter
        special_filter = {'tag', 'tags', 'category', 'categories', 'author', 
                         'feed', 'rss', 'atom', 'sitemap', 'search', 'archive'}
        
        if first_segment in special_filter:
            return True
        
        import re
        
        # Pattern 1: 2-letter codes (most language codes)
        if len(first_segment) == 2:
            # Exclude common English words
            english_words = {'as', 'at', 'by', 'in', 'of', 'on', 'to', 'up',
                           'us', 'we', 'is', 'be', 'or', 'an'}
            if first_segment in english_words:
                return False
            # Most 2-letter segments are language codes
            return bool(re.match(r'^[a-z]{2}$', first_segment))
        
        # Pattern 2: 3-letter codes
        if len(first_segment) == 3:
            # Common 3-letter language codes
            language_codes_3 = {'ara', 'chi', 'eng', 'fra', 'ger', 'ita', 'jpn', 
                              'kor', 'por', 'rus', 'spa', 'vie', 'zho', 'pol',
                              'ind', 'tur', 'ukr', 'ces', 'dan', 'nld', 'fin',
                              'ell', 'heb', 'hin', 'hun', 'isl', 'lav', 'lit',
                              'nor', 'ron', 'slk', 'slv', 'swe', 'tha'}
            return first_segment in language_codes_3
        
        # Pattern 3: Language-region codes (en-us, pt-br, etc.)
        if re.match(r'^[a-z]{2,3}-[a-z]{2,10}$', first_segment):
            return True
        
        return False
        
    except:
        return False

# ----------------------------------------------------
# 4. API Reporting Functions
# ----------------------------------------------------
def send_api_report(
    status: str,
    metrics: dict,
    website: str = "conholdate.com",
    env: str = "DEV",
    product_override: str = None
):
    """
    Send job completion report to API endpoints.
    
    Args:
        status: "success" or "fail"
        metrics: Dictionary containing metrics
        website: The company website (default: "conholdate.com")
        env: Environment - "PROD" or "DEV" (default: "DEV")
    """
    try:
        print("\n" + "="*60)
        print("METRICS REPORTING TRACE")
        print("="*60)
        print(f"Incoming status: {status}")
        print(f"Incoming website: {website}")
        print(f"Incoming env: {env}")
        print(f"Incoming metrics: {json.dumps(metrics, default=str)}")

        if status != "success":
            print("Skipping metrics because of status.")
            return False

        # Create GMT+5 timezone (Pakistan Standard Time)
        gmt5 = timezone(timedelta(hours=5))
        
        # Get current time in GMT+5
        current_time = datetime.now(gmt5)
        
        # Format timestamp in ISO format with GMT+5 timezone
        timestamp = current_time.isoformat(timespec='milliseconds')
        
        # Determine product based on website (or explicit family override).
        product = product_override or "Conholdate"
        if not product_override:
            if website == "aspose.com":
                product = "Aspose"
            elif website == "groupdocs.com":
                product = "GroupDocs"
        
        # Generate random 5-digit number and create run_id
        random_suffix = random.randint(10000, 99999)  # Generates a random 5-digit number
        run_id = f"blog_optimizer_{random_suffix}"
        
        # Prepare the common payload
        common_payload = {
            "timestamp": timestamp,
            "agent_name": "Blog Optimizer",
            "agent_owner": "Farhan Raza",
            "job_type": "Blog Optimization",
            "run_id": run_id,
            "status": status,
            "product": product,
            "platform": "ALL",
            "website": website,
            "website_section": "Blog",
            "item_name": "Blog Posts",
            "items_discovered": metrics.get('items_discovered', 0),
            "items_failed": metrics.get('items_failed', 0),
            "items_succeeded": metrics.get('items_succeeded', 0),
            "run_duration_ms": metrics.get('run_duration_ms', 0),
            "token_usage": metrics.get('token_usage', 0),
            "api_call_count": metrics.get('api_call_count', 0),
            # Backward-compatible alias for endpoints expecting pluralized key.
            "api_calls_count": metrics.get('api_call_count', 0)
        }
        
        has_api_token = bool(API_TOKEN)
        has_blogs_team_token = bool(BLOGS_TEAM_TOKEN)
        print(f"Token check: BLOG_OPTIMIZER_API_TOKEN set={has_api_token}, BLOGS_TEAM_TOKEN set={has_blogs_team_token}")

        if not has_api_token and not has_blogs_team_token:
            print("Skipping API reports because BLOG_OPTIMIZER_API_TOKEN and BLOGS_TEAM_TOKEN are not set.")
            return False

        original_ok = None
        blogs_team_ok = None
        
        # Send to original endpoint (if configured)
        if has_api_token:
            original_url = f"{API_ENDPOINT}?token=<redacted>"
            print(f"\nSending to Original Endpoint: {original_url}")
            print(f"Original payload keys: {list(common_payload.keys())}")
            response1 = requests.post(
                f"{API_ENDPOINT}?token={API_TOKEN}",
                headers={"Content-Type": "application/json"},
                data=json.dumps(common_payload),
                timeout=10
            )
            
            print(f"Original Endpoint Status: {response1.status_code}")
            if response1.status_code == 200:
                response1_text = (response1.text or "").strip()
                logical_error = False
                if response1_text:
                    print(f"Original endpoint response: {response1_text[:500]}")
                    try:
                        parsed = json.loads(response1_text)
                        if isinstance(parsed, dict):
                            if parsed.get("error"):
                                logical_error = True
                            parsed_status = parsed.get("status")
                            if parsed_status is not None:
                                try:
                                    if int(parsed_status) >= 400:
                                        logical_error = True
                                except (TypeError, ValueError):
                                    pass
                            if parsed.get("success") is False:
                                logical_error = True
                    except json.JSONDecodeError:
                        lower_body = response1_text.lower()
                        if "invalid token" in lower_body or "error" in lower_body:
                            logical_error = True

                if logical_error:
                    print("✗ Original endpoint returned an application-level error despite HTTP 200.")
                    original_ok = False
                else:
                    print("✓ Original endpoint report sent successfully!")
                    original_ok = True
            else:
                print(f"✗ Original endpoint failed: {response1.text[:500]}")
                original_ok = False
        else:
            print("\nSkipping Original Endpoint because BLOG_OPTIMIZER_API_TOKEN is not set.")
        
        # Prepare payload for Blogs Team Metrics (add run_env field)
        blogs_team_payload = common_payload.copy()
        blogs_team_payload["run_env"] = env  # Currently "DEV" during testing
        
        # Send to Blogs Team Metrics endpoint (if configured)
        if has_blogs_team_token:
            blogs_team_url = f"{BLOGS_TEAM_ENDPOINT}?token=<redacted>"
            print(f"\nSending to Blogs Team Metrics Endpoint: {blogs_team_url}")
            print(f"Blogs Team payload keys: {list(blogs_team_payload.keys())}")
            response2 = requests.post(
                f"{BLOGS_TEAM_ENDPOINT}?token={BLOGS_TEAM_TOKEN}",
                headers={"Content-Type": "application/json"},
                data=json.dumps(blogs_team_payload),
                timeout=10
            )
            
            print(f"Blogs Team Status: {response2.status_code}")
            if response2.status_code == 200:
                response2_text = (response2.text or "").strip()
                logical_error = False
                if response2_text:
                    print(f"Blogs Team endpoint response: {response2_text[:500]}")
                    try:
                        parsed = json.loads(response2_text)
                        if isinstance(parsed, dict):
                            if parsed.get("error"):
                                logical_error = True
                            parsed_status = parsed.get("status")
                            if parsed_status is not None:
                                try:
                                    if int(parsed_status) >= 400:
                                        logical_error = True
                                except (TypeError, ValueError):
                                    pass
                            if parsed.get("success") is False:
                                logical_error = True
                    except json.JSONDecodeError:
                        lower_body = response2_text.lower()
                        if "invalid token" in lower_body or "error" in lower_body:
                            logical_error = True

                if logical_error:
                    print("✗ Blogs Team endpoint returned an application-level error despite HTTP 200.")
                    blogs_team_ok = False
                else:
                    print("✓ Blogs Team Metrics report sent successfully!")
                    print(f"  run_env: {blogs_team_payload['run_env']}")
                    blogs_team_ok = True
            else:
                print(f"✗ Blogs Team Metrics failed: {response2.text[:500]}")
                blogs_team_ok = False
        else:
            print("\nSkipping Blogs Team Metrics because BLOGS_TEAM_TOKEN is not set.")
        
        # Summary
        print("\n" + "="*60)
        print("API REPORTS SUMMARY")
        print("="*60)
        print(f"Timestamp (GMT+5): {timestamp}")
        print(f"Product: {product}")
        print(f"Website: {website}")
        print(f"Status: {status}")
        print(f"Environment: {env}")
        print(f"Run ID: {run_id}")
        print(f"Discovered: {metrics.get('items_discovered', 0)}")
        print(f"Succeeded: {metrics.get('items_succeeded', 0)}")
        print(f"Failed: {metrics.get('items_failed', 0)}")
        print(f"Duration: {metrics.get('run_duration_ms', 0)}ms")
        print(f"Token Usage: {metrics.get('token_usage', 0)}")
        print(f"API Call Count: {metrics.get('api_call_count', 0)}")
        print("="*60)
        
        configured_results = [r for r in [original_ok, blogs_team_ok] if r is not None]
        if not configured_results:
            print("No API endpoint was configured with a token; nothing was sent.")
            return False
        all_ok = all(configured_results)
        print(f"Metrics reporting result: success={all_ok}, endpoint_results={configured_results}")
        return all_ok
            
    except Exception as e:
        print(f"Error sending API reports: {type(e).__name__}: {e}")
        return False


def send_api_reports_by_family(status: str, metrics: dict, website: str, env: str = "DEV") -> bool:
    """
    Send one metrics payload per family for family-level reporting.
    Falls back to single payload when no family buckets are available.
    """
    family_metrics = metrics.get("family_metrics") or {}
    if not family_metrics:
        return send_api_report(status, metrics, website, env)

    print("\n" + "="*60)
    print("SENDING FAMILY-LEVEL API REPORTS")
    print("="*60)

    # Only report families where actual LLM/API activity happened.
    # This prevents flooding metrics endpoints with zero-activity families when
    # daily limit is low and most URLs are skipped or never reached.
    active_families = []
    for family_name, fam in family_metrics.items():
        if (fam.get("api_call_count", 0) > 0) or (fam.get("token_usage", 0) > 0):
            active_families.append(family_name)

    if not active_families:
        print("No active family metrics found (no LLM/API activity by family).")
        return True

    skipped_inactive = len(family_metrics) - len(active_families)
    if skipped_inactive > 0:
        print(f"Skipping {skipped_inactive} inactive families with zero API usage.")

    all_ok = True
    for family_name in sorted(active_families):
        fam = family_metrics[family_name]
        family_payload_metrics = {
            "items_discovered": fam.get("items_discovered", 0),
            "items_succeeded": fam.get("items_succeeded", 0),
            "items_failed": fam.get("items_failed", 0),
            "run_duration_ms": metrics.get("run_duration_ms", 0),
            "token_usage": fam.get("token_usage", 0),
            "api_call_count": fam.get("api_call_count", 0),
            "status": status,
        }
        print(
            f"\nFamily: {family_name} | "
            f"discovered={family_payload_metrics['items_discovered']}, "
            f"succeeded={family_payload_metrics['items_succeeded']}, "
            f"failed={family_payload_metrics['items_failed']}, "
            f"token_usage={family_payload_metrics['token_usage']}, "
            f"api_call_count={family_payload_metrics['api_call_count']}"
        )
        ok = send_api_report(
            status,
            family_payload_metrics,
            website,
            env,
            product_override=family_name
        )
        all_ok = all_ok and bool(ok)

    print(f"\nFamily-level metrics reporting result: success={all_ok}")
    return all_ok

# ----------------------------------------------------
# 5. Cleanup Functions
# ----------------------------------------------------
def clean_optimized_content(content: str, original_content: str = "") -> str:
    """Clean up common formatting issues in optimized content."""
    if not content:
        return content
    
    original = content
    lines = content.split('\n')
    
    # 1. Remove any ```markdown or code fences at the very beginning
    if lines and lines[0].strip().startswith('```'):
        first_line = lines[0].strip()
        if first_line == '```markdown' or first_line == '```':
            lines = lines[1:]
    
    # 2. Remove any ``` at the very end
    if lines and lines[-1].strip() == '```':
        lines = lines[:-1]
    
    content = '\n'.join(lines)
        
    # 3. Remove emojis and special icons
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # emoticons
        "\U0001F300-\U0001F5FF"  # symbols & pictographs
        "\U0001F680-\U0001F6FF"  # transport & map symbols
        "\U0001F1E0-\U0001F1FF"  # flags (iOS)
        "\U00002500-\U00002BEF"  # chinese char
        "\U00002702-\U000027B0"
        "\U000024C2-\U0001F251"
        "\U0001f926-\U0001f937"
        "\U00010000-\U0010ffff"
        "\u2640-\u2642" 
        "\u2600-\u2B55"
        "\u200d"
        "\u23cf"
        "\u23e9"
        "\u231a"
        "\ufe0f"  # dingbats
        "\u3030"
        "]+", flags=re.UNICODE)
    content = emoji_pattern.sub('', content)
    
    # 4. Ensure YAML front matter starts correctly
    content = content.strip()
    if not content.startswith('---'):
        content = '---\n' + content
    
    # 5. Remove any images that weren't in the original (if original is provided)
    if original_content:
        original_images = set()
        for match in re.finditer(r'!\[([^\]]*)\]\(([^)]+)\)', original_content):
            original_images.add(match.group(0))
        
        lines = content.split('\n')
        clean_lines = []
        for line in lines:
            if '![' in line and '](' in line and ')' in line:
                if line.strip() not in original_images:
                    continue
            clean_lines.append(line)
        
        content = '\n'.join(clean_lines)
    
    # 6. Remove any added tables that weren't in original (if original is provided)
    if original_content and '|' in content:
        if '|' not in original_content and '|---' in content:
            lines = content.split('\n')
            clean_lines = []
            in_table = False
            for line in lines:
                if '|---' in line:
                    in_table = True
                    continue
                if in_table and line.strip() and '|' in line:
                    continue
                if in_table and not line.strip():
                    in_table = False
                if not in_table:
                    clean_lines.append(line)
            content = '\n'.join(clean_lines)
    
    # 7. Clean up multiple empty lines
    content = re.sub(r'\n\s*\n\s*\n', '\n\n', content)
    
    # 8. Ensure proper line endings
    content = content.replace('\r\n', '\n').replace('\r', '\n')
    
    # 9. Update lastmod field to current date (but don't touch date field)
    today_str = str(date.today())
    lines = content.split('\n')
    for i, line in enumerate(lines):
        if line.startswith('lastmod:'):
            lines[i] = f'lastmod: {today_str}'
            break
    
    content = '\n'.join(lines)
    
    # 10. Final cleanup: remove any leftover code fence markers
    if content.startswith('```'):
        content = content[3:].lstrip()
    if content.endswith('```'):
        content = content[:-3].rstrip()
    
    return content.strip()

def validate_yaml_front_matter(content: str) -> bool:
    """Validate that the content has proper YAML front matter."""
    if not content.startswith('---'):
        return False
    
    lines = content.split('\n')
    found_first = False
    for i, line in enumerate(lines):
        if line.strip() == '---':
            if not found_first:
                found_first = True
            elif i > 0:
                if i < len(lines) - 1:
                    return True
    return False

def ensure_and_update_lastmod_field(content: str) -> str:
    """
    Ensure the content has a lastmod field and update it to current date.
    If not present, add it with today's date.
    """
    today_str = str(date.today())
    
    # Parse YAML front matter
    yaml_pattern = r'^---\s*\n(.*?)\n---\s*\n(.*)'
    match = re.match(yaml_pattern, content, re.DOTALL)
    
    if not match:
        return content
    
    yaml_text, body = match.groups()
    
    try:
        metadata = yaml.safe_load(yaml_text) or {}
    except:
        metadata = {}
    
    lines = yaml_text.split('\n')
    lastmod_updated = False
    
    # Check if lastmod exists and update it
    for i, line in enumerate(lines):
        if line.strip().startswith('lastmod:'):
            lines[i] = f'lastmod: {today_str}'
            lastmod_updated = True
            break
    
    # If lastmod wasn't found, add it
    if not lastmod_updated:
        # Try to add after date field
        date_found = False
        new_lines = []
        for line in lines:
            new_lines.append(line)
            # Insert lastmod after date field
            if line.strip().startswith('date:'):
                date_found = True
                # Add lastmod on next line
                new_lines.append(f'lastmod: {today_str}')
        
        # If date field wasn't found, add lastmod at appropriate position
        if not date_found:
            # Find where to insert (usually after title/author)
            insert_position = 0
            for i, line in enumerate(lines):
                if line.strip() and not line.strip().startswith('#'):
                    insert_position = i + 1
            
            # Insert lastmod
            lines.insert(insert_position, f'lastmod: {today_str}')
            new_lines = lines
        else:
            lines = new_lines
    
    new_yaml_text = '\n'.join(lines)
    new_content = f"---\n{new_yaml_text}\n---\n{body}"
    
    return new_content

# ----------------------------------------------------
# 6. Updated Folder Structure Functions
# ----------------------------------------------------
def find_blog_post_by_url(source_path: str, target_url: str, domain_info: dict, brand_config: dict = None):
    """
    Find blog post file that matches the target URL by looking for index.md files.
    Optimizes search based on domain and URL structure.
    
    Args:
        source_path: Root path of the blog repository
        target_url: The URL to match
        domain_info: Domain information dictionary
        brand_config: Brand configuration (optional)
    
    Returns:
        tuple: (Path to the matching index.md file, publish_date) or (None, None) if not found
    """
    # Normalize the target URL
    target_url = target_url.rstrip('/')
    
    # Extract path from URL (remove domain)
    url_path = extract_url_path_from_full_url(target_url)
    
    print(f"Searching for index.md files matching URL: {url_path}")
    
    # Get company from domain info
    company = domain_info['company']
    
    # Use brand config if provided, otherwise use company name
    content_folder = None
    if brand_config and 'content_folder' in brand_config:
        content_folder = brand_config['content_folder']
    elif company in BRAND_CONFIG:
        content_folder = BRAND_CONFIG[company]['content_folder']
    
    # Extract category from URL path
    parts = url_path.strip('/').split('/')
    category = None
    if len(parts) >= 2:
        category = parts[0]
    
    # Build optimized search patterns
    search_patterns = []
    
    if content_folder and category:
        # Most specific: brand folder + category
        search_patterns.append(f"content/{content_folder}/{category}/**/index.md")
    
    if content_folder:
        # Specific: brand folder only
        search_patterns.append(f"content/{content_folder}/**/index.md")
    
    if category:
        # Generic: any folder with this category
        search_patterns.append(f"content/**/{category}/**/index.md")
    
    # Fallback patterns
    search_patterns.extend([
        "content/**/index.md",
        "**/index.md"
    ])
    
    source_dir = Path(source_path)
    
    if not source_dir.exists():
        print(f"Source directory not found: {source_path}")
        return None, None
    
    files_found = 0
    
    # Try each search pattern
    for pattern in search_patterns:
        md_files = list(source_dir.glob(pattern))
        
        for md_file in md_files:
            files_found += 1
            try:
                with open(md_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Look for YAML front matter with url field
                yaml_pattern = r'^---\s*\n(.*?)\n---\s*\n(.*)'
                match = re.match(yaml_pattern, content, re.DOTALL)
                
                if match:
                    yaml_text, _ = match.groups()
                    try:
                        metadata = yaml.safe_load(yaml_text) or {}
                        
                        # Check if url matches
                        if 'url' in metadata:
                            file_url = metadata['url'].rstrip('/')
                            if file_url == url_path:
                                print(f"  ✓ Found matching index.md file: {md_file}")
                                # Extract publish date from metadata
                                publish_date = metadata.get('date', '')
                                return md_file, publish_date
                    except Exception:
                        continue
                        
            except Exception:
                continue
    
    print(f"✗ No matching index.md file found for URL: {url_path}")
    return None, None

def extract_url_path_from_full_url(full_url: str):
    """
    Extract the URL path from a full URL.
    Example: https://blog.aspose.com/cad/change-svg-to-png-in-python/
            -> /cad/change-svg-to-png-in-python/
    """
    try:
        # Remove protocol and domain
        if '://' in full_url:
            path = full_url.split('://', 1)[1]
            path = '/' + path.split('/', 1)[1] if '/' in path else '/'
        else:
            path = full_url
        
        return path.rstrip('/')
    except:
        return full_url

def extract_slug_from_url(url: str):
    """Extract slug from URL."""
    try:
        parts = url.rstrip('/').split('/')
        if parts:
            slug = parts[-1]
            slug = slug.split('?')[0].split('#')[0]
            return slug.lower()
    except:
        pass
    return ""

# ----------------------------------------------------
# 7. Core Functions Updated for Brand Support
# ----------------------------------------------------
def extract_blog_urls_from_csv(file_path: str, brand: str = None):
    """Extract blog URLs from CSV file, filtering out language-specific URLs and other unwanted patterns."""
    blog_urls = []
    
    print(f"Reading CSV file: {file_path}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            url_column = None
            for col in reader.fieldnames:
                if 'url' in col.lower():
                    url_column = col
                    break
            
            if not url_column and reader.fieldnames:
                url_column = reader.fieldnames[0]
            
            urls_processed = 0
            urls_filtered = 0
            
            for row in reader:
                if url_column and url_column in row:
                    url = row[url_column].strip()
                    urls_processed += 1
                    
                    if not url:
                        continue
                    
                    # Skip if URL contains any of these patterns
                    skip_patterns = [
                        '/tag/',      # Tag pages
                        '/page/',     # Pages
                        '/tags/',     # Tags pages
                        '.xml',       # XML files
                        '/feed/',     # RSS feeds
                        '/rss/',      # RSS feeds
                        '/atom/',     # Atom feeds
                        '/sitemap',   # Sitemaps
                        '/category/', # Category pages (plural)
                        '/categories/', # Categories pages
                        '/author/',   # Author pages
                        '/archive/',  # Archive pages
                        '/search/',   # Search pages
                        '?',          # URLs with query parameters
                        '#',          # URLs with fragments
                    ]
                    
                    should_skip = any(pattern in url.lower() for pattern in skip_patterns)
                    
                    # Also skip if URL has language code prefix
                    if has_language_code_prefix(url):
                        should_skip = True
                    
                    if not should_skip:
                        blog_urls.append(url)
                    else:
                        urls_filtered += 1
        
        # Remove duplicates while preserving order
        seen = set()
        unique_blog_urls = []
        for url in blog_urls:
            if url not in seen:
                seen.add(url)
                unique_blog_urls.append(url)
        
        print(f"  Processed {urls_processed} URLs from CSV")
        print(f"  Filtered out {urls_filtered} URLs")
        print(f"  Found {len(unique_blog_urls)} unique blog URLs (after filtering)")
        
        return unique_blog_urls
        
    except FileNotFoundError:
        print(f"ERROR: CSV file not found: {file_path}")
        print(f"Please create {file_path} with your blog URLs")
        return []
    except Exception as e:
        print(f"CSV error: {e}")
        return []

async def optimize_post(md_file_path: Path, url: str, domain_info: dict, publish_date: str, metrics: dict = None):
    """Optimize a blog post with domain-aware tracking and retry logic."""
    folder_name = md_file_path.parent.name
    
    # Extract slug from URL
    slug = extract_slug_from_url(url)
    
    print(f"\nProcessing: {folder_name} (slug: {slug})")
    print(f"  Domain: {domain_info['full_domain']}")
    print(f"  Source file: {md_file_path}")
    if publish_date:
        print(f"  Publish date: {publish_date}")
    
    # Check if we can optimize this slug
    can_optimize, reason = can_optimize_slug(domain_info, slug, publish_date)
    
    if not can_optimize:
        print(f"  Skipping: {reason}")
        return False, "skipped"
    
    print(f"  Can optimize: {reason}")
    
    # Read original content from .md file
    with open(md_file_path, 'r', encoding='utf-8') as f:
        original_content = f.read()

    # Extract YAML front matter from the ORIGINAL content (before mutating lastmod)
    yaml_pattern = r'^---\s*\n(.*?)\n---\s*\n(.*)'
    match = re.match(yaml_pattern, original_content, re.DOTALL)

    if match:
        yaml_text, body = match.groups()
        try:
            metadata = yaml.safe_load(yaml_text) or {}
        except:
            metadata = {}
    else:
        metadata = {}
        body = original_content

    current_title = metadata.get('title', 'Untitled Blog Post')
    current_date = metadata.get('date', '')
    original_lastmod = metadata.get('lastmod', '')

    # Extra guard: if lastmod was updated recently, skip even if logs are stale/missing.
    if original_lastmod:
        lastmod_date = None
        try:
            if isinstance(original_lastmod, datetime):
                lastmod_date = original_lastmod.date()
            elif isinstance(original_lastmod, date):
                lastmod_date = original_lastmod
            elif isinstance(original_lastmod, str):
                for date_format in ('%Y-%m-%d', '%Y-%m-%dT%H:%M:%S%z', '%Y-%m-%dT%H:%M:%S'):
                    try:
                        lastmod_date = datetime.strptime(original_lastmod, date_format).date()
                        break
                    except ValueError:
                        continue
        except Exception:
            lastmod_date = None

        if lastmod_date:
            today = date.today()
            days_since_lastmod = (today - lastmod_date).days
            if days_since_lastmod < 0:
                print(f"  Skipping: lastmod is in the future ({original_lastmod}).")
                return False, "skipped"
            if days_since_lastmod < MIN_DAYS_BETWEEN_OPTIMIZATIONS:
                remaining_days = MIN_DAYS_BETWEEN_OPTIMIZATIONS - days_since_lastmod
                print(f"  Skipping: lastmod updated {days_since_lastmod} days ago. Can optimize again in {remaining_days} days. (front matter)")
                return False, "skipped"

    # Ensure lastmod field exists and update it to today's date (after passing guards)
    content_with_updated_lastmod = ensure_and_update_lastmod_field(original_content)

    # Re-parse YAML from updated content so the prompt preserves the updated lastmod.
    match = re.match(yaml_pattern, content_with_updated_lastmod, re.DOTALL)
    if match:
        yaml_text, body = match.groups()
        try:
            metadata = yaml.safe_load(yaml_text) or {}
        except:
            metadata = {}
    else:
        metadata = {}
        body = content_with_updated_lastmod

    current_lastmod = metadata.get('lastmod', '')
    
    # Prepare strict instructions for LLM
    prompt = f"""You are an SEO content optimizer. Your task is to optimize ONLY the content of this blog post for SEO while PRESERVING EXACT formatting.

CRITICAL FORMATTING RULES - MUST FOLLOW:
1. PRESERVE the EXACT YAML front matter structure (lines starting with --- to the second ---)
2. DO NOT wrap the output in triple backticks ```markdown``` or any code fences
3. DO NOT add any icons, emojis, or visual elements like 🚀✨📝
4. DO NOT add any images that weren't in the original
5. DO NOT add language identifiers like ```bash, ```csharp - use only ``` for code blocks
6. KEEP the exact same sections in the same order as original
7. ONLY optimize the text content for SEO and readability
8. DO NOT change the 'date' field - keep it as: {current_date}
9. DO NOT change the 'lastmod' field - keep it as: {current_lastmod}
10. DO NOT change the YAML field names or structure
11. DO NOT add tables unless they were in the original
12. DO NOT add FAQ sections unless they were in the original
13. DO NOT add "See Also" sections unless they were in the original
14. Return ONLY the complete markdown file starting with ---

ORIGINAL CONTENT:
{content_with_updated_lastmod}

OPTIMIZATION TASKS (ONLY DO THESE):
1. Improve 'seoTitle' (max 60 chars)
2. Improve 'description' (max 160 chars)
3. Improve 'summary' to be more compelling while keeping same length
4. Fix heading hierarchy if needed (ensure proper H2, H3 nesting)
5. Simplify complex sentences for better readability
6. Ensure code blocks use only ``` without language identifiers like bash/csharp
7. Remove any emojis or icons
8. Keep all existing links and references exactly as-is
9. Do NOT change the "See Also" section links
10. Do NOT add any new sections or headings

Return the complete optimized blog post starting with "---" and ending with the content."""

    # Retry configuration
    max_retries = 3
    retry_delay = 2  # seconds
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            if retry_count > 0:
                print(f"  Retry attempt {retry_count}/{max_retries-1}...")
                # Increase delay between retries with exponential backoff
                await asyncio.sleep(retry_delay * (2 ** (retry_count - 1)))
            
            print(f"  Optimizing with {MODEL_NAME}...")
            if metrics is not None:
                metrics['api_call_count'] = metrics.get('api_call_count', 0) + 1
                print(f"  Metrics: api_call_count={metrics['api_call_count']}")
                family_name = metrics.get('_active_family_name')
                if family_name and "family_metrics" in metrics and family_name in metrics["family_metrics"]:
                    metrics["family_metrics"][family_name]['api_call_count'] = (
                        metrics["family_metrics"][family_name].get('api_call_count', 0) + 1
                    )
            
            # Call LLM with timeout
            response = await client.chat.completions.create(
                model=MODEL_NAME,
                messages=[
                    {
                        "role": "system", 
                        "content": """You are a strict technical content formatter. You MUST:
1. Preserve ALL original formatting exactly
2. Do NOT add any code fences (```) around the entire output
3. Do NOT add emojis, icons, or visual elements
4. Do NOT change the YAML structure or field names
5. Do NOT add new sections, tables, or images
6. Only optimize text content for SEO and readability
7. Use ``` for code blocks without language identifiers
8. Return clean markdown starting with --- and ending with content
9. Keep all original links and references exactly
10. DO NOT change the date or lastmod fields"""
                    },
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2,
                max_tokens=4000,
                timeout=30.0  # 30 second timeout
            )

            usage = getattr(response, "usage", None)
            if metrics is not None and usage is not None:
                total_tokens = getattr(usage, "total_tokens", 0) or 0
                prompt_tokens = getattr(usage, "prompt_tokens", 0) or 0
                completion_tokens = getattr(usage, "completion_tokens", 0) or 0
                metrics['token_usage'] = metrics.get('token_usage', 0) + total_tokens
                print(
                    f"  Metrics: token_usage +={total_tokens} "
                    f"(prompt={prompt_tokens}, completion={completion_tokens}) => {metrics['token_usage']}"
                )
                family_name = metrics.get('_active_family_name')
                if family_name and "family_metrics" in metrics and family_name in metrics["family_metrics"]:
                    metrics["family_metrics"][family_name]['token_usage'] = (
                        metrics["family_metrics"][family_name].get('token_usage', 0) + total_tokens
                    )
            
            optimized = response.choices[0].message.content.strip()
            
            if not optimized:
                print(f"  Empty response from LLM")
                if retry_count < max_retries - 1:
                    retry_count += 1
                    continue
                else:
                    return False, "empty_response"
            
            # If we get here, the API call succeeded
            break
            
        except asyncio.TimeoutError:
            print(f"  Request timed out after 30 seconds")
            if retry_count < max_retries - 1:
                retry_count += 1
                print(f"  Will retry in {retry_delay * (2 ** (retry_count - 1))} seconds...")
                continue
            else:
                print(f"  Max retries ({max_retries}) exceeded. Giving up.")
                return False, "timeout"
        except Exception as e:
            print(f"  Error during LLM call: {e}")
            if retry_count < max_retries - 1:
                retry_count += 1
                continue
            else:
                print(f"  Max retries ({max_retries}) exceeded. Giving up.")
                return False, "error"
    
    # Apply post-processing cleanup
    print(f"  Cleaning up formatting...")
    cleaned_content = clean_optimized_content(optimized, content_with_updated_lastmod)
    
    # Validate the cleaned content
    if not validate_yaml_front_matter(cleaned_content):
        print(f"  Content doesn't have proper YAML front matter after cleaning")
        if not cleaned_content.startswith('---'):
            cleaned_content = '---\n' + cleaned_content
    
    # Save cleaned output BACK TO THE ORIGINAL FILE (overwrite)
    with open(md_file_path, 'w', encoding='utf-8') as f:
        f.write(cleaned_content)
    
    # Update optimization log for this domain with publish date
    update_optimization_log(domain_info, slug, url)
    
    print(f"  Optimized and saved to: {md_file_path}")
    
    return True, "optimized"

# ----------------------------------------------------
# 8. Main Function with Brand Support and Daily Limit
# ----------------------------------------------------
async def main(args):
    print("="*60)
    print("BLOG POST OPTIMIZER WITH DOMAIN-AWARE TRACKING")
    print(f"Minimum days between optimizations: {MIN_DAYS_BETWEEN_OPTIMIZATIONS} days")
    print(f"Minimum post age before optimization: {MIN_DAYS_SINCE_PUBLISH} days")
    print(f"Daily limit per domain: {args.limit} (0 = no limit)")
    print(f"Log directory: {LOG_DIR}")
    print(f"Source path: {args.sourcepath}")
    if args.brand:
        print(f"Brand: {args.brand}")
    print("="*60)
    
    # Ensure csv directory exists
    csv_dir = Path("csv")
    csv_dir.mkdir(exist_ok=True)
    
    # Start timing
    start_time = time.time()
    
    # Ensure log directory exists
    Path(LOG_DIR).mkdir(exist_ok=True)
    
    # Initialize metrics
    metrics = {
        'items_discovered': 0,
        'items_succeeded': 0,
        'items_failed': 0,
        'status': 'success',
        'token_usage': 0,
        'api_call_count': 0,
        'family_metrics': {}
    }
    
    # Initialize limit settings
    limit_settings = {
        'limit_per_domain': args.limit,
        'today_counts': {}  # Cache for today's counts per domain
    }
    
    try:
        # Determine which CSV file to use
        if args.brand:
            # Use brand-specific CSV file
            csv_file = BRAND_CONFIG[args.brand]['csv_file']
            brand_config = BRAND_CONFIG[args.brand]
            print(f"Using CSV file: {csv_file}")
        else:
            # Try to use generic input.csv
            csv_file = "input.csv"
            brand_config = None
            print(f"Using CSV file: {csv_file}")
                
        if not os.path.exists(csv_file):
            print(f"CSV file not found: {csv_file}")
            metrics['status'] = 'fail'
            return metrics
        
        # Extract URLs
        blog_urls = extract_blog_urls_from_csv(csv_file, args.brand)
        if not blog_urls:
            print("No blog URLs found")
            metrics['status'] = 'success'  # Empty but successful run
            return metrics
        
        # Filter URLs by brand if specified
        if args.brand and brand_config:
            brand_domains = brand_config['domains']
            filtered_urls = []
            for url in blog_urls:
                domain_match = False
                for domain in brand_domains:
                    if domain in url:
                        domain_match = True
                        break
                if domain_match:
                    filtered_urls.append(url)
                else:
                    print(f"  Skipping URL not matching brand domains: {url}")
            
            blog_urls = filtered_urls
            print(f"Filtered to {len(blog_urls)} URLs matching brand domains")
        
        # Temporary discovered count; final discovered is recalculated from terminal outcomes.
        metrics['items_discovered'] = len(blog_urls)
        website_for_run = get_website_for_brand(args.brand)
        
        # Organize URLs by domain
        urls_by_domain = {}
        for url in blog_urls:
            domain_info = extract_domain_info(url, args.brand)
            domain_key = domain_info['full_domain']
            if domain_key not in urls_by_domain:
                urls_by_domain[domain_key] = {
                    'info': domain_info,
                    'urls': [],
                    'slugs': []
                }
            urls_by_domain[domain_key]['urls'].append(url)
            
            # Extract slug
            slug = extract_slug_from_url(url)
            if slug:
                urls_by_domain[domain_key]['slugs'].append(slug)
        
        # Print domain statistics
        print(f"\nFound URLs from {len(urls_by_domain)} unique domains:")
        for domain_key, data in urls_by_domain.items():
            print(f"  - {domain_key}: {len(data['urls'])} URLs")
        
        # Process each domain
        all_results = {}
        total_processed = 0
        
        for domain_key, data in urls_by_domain.items():
            domain_info = data['info']
            
            print(f"\n{'='*40}")
            print(f"Processing domain: {domain_key}")
            print(f"Log file: logs/{domain_info['company']}/{domain_info['log_filename']}")
            print(f"{'='*40}")
            
            if not data['urls']:
                print("No URLs found for this domain")
                continue
            
            # Check daily limit for this domain
            if limit_settings['limit_per_domain'] > 0:
                limit_check, limit_message = check_daily_limit(domain_info, limit_settings)
                print(f"  {limit_message}")
                
                if not limit_check:
                    print(f"  ⚠️  Skipping all URLs for this domain - daily limit reached")
                    continue
            else:
                # When limit is 0, we still need to call check_daily_limit to get the message
                limit_check, limit_message = check_daily_limit(domain_info, limit_settings)
                print(f"  {limit_message}")

            # Process each URL for this domain
            domain_results = {
                "optimized": 0,
                "skipped": 0,
                "error": 0,
                "timeout_after_retries": 0,
                "no_file": 0,
                "empty_response": 0,
                "limit_reached": 0
            }

            # Check daily limit BEFORE starting to process URLs for this domain
            limit_reached = False

            # Use the limit_check and limit_message we already got above
            # Only set limit_reached if limit > 0 AND check returns False
            if limit_settings['limit_per_domain'] > 0 and not limit_check:
                print(f"  ⚠️  Daily limit reached for {domain_key}. Skipping all remaining URLs for this domain.")
                limit_reached = True

            # Initialize counter for today's optimizations for this domain
            today_optimized = count_optimizations_today_for_domain(domain_info, limit_settings)
                
            for i, url in enumerate(data['urls'], 1):
                # Stop iterating once the daily limit is reached (saves time on huge URL lists)
                if limit_reached:
                    remaining = len(data['urls']) - i + 1
                    domain_results['limit_reached'] += remaining
                    print(f"  ⚠️  Daily limit reached for {domain_key}. Skipping remaining {remaining} URLs.")
                    break
                
                # Check if we're approaching the limit (only check for real optimization candidates)
                # We'll update this after actual optimizations
                print(f"\n[{i}/{len(data['urls'])}] Processing URL: {url}")
                
                # Find the matching blog post file with brand-aware search
                md_file, publish_date = find_blog_post_by_url(args.sourcepath, url, domain_info, brand_config)
                
                if not md_file:
                    print(f"  No matching blog post found for URL")
                    domain_results['no_file'] += 1
                    continue

                family_name = derive_family_name_from_md(md_file)
                if not family_name:
                    print(f"  Warning: Product family not found in categories for {md_file}; skipping family metrics for this URL.")
                family_bucket = ensure_family_metrics_bucket(metrics, family_name) if family_name else None
                
                # Extract slug from URL
                slug = extract_slug_from_url(url)
                
                # Check if we can optimize this slug
                can_optimize, reason = can_optimize_slug(domain_info, slug, publish_date)
                
                if not can_optimize:
                    print(f"  Skipping: {reason}")
                    domain_results['skipped'] += 1
                    if family_bucket is not None:
                        family_bucket['items_discovered'] += 1
                        family_bucket['items_succeeded'] += 1
                    continue
                
                # At this point, we have a candidate for optimization
                # Check if we've reached the daily limit
                if limit_settings['limit_per_domain'] > 0 and today_optimized >= limit_settings['limit_per_domain']:
                    print(f"  ⚠️  Daily limit reached for {domain_key}. Skipping remaining URLs.")
                    limit_reached = True
                    # Remaining URLs will be counted and skipped at loop start
                    continue
                
                # Optimize the post
                if family_name:
                    metrics['_active_family_name'] = family_name
                success, status = await optimize_post(md_file, url, domain_info, publish_date, metrics)
                metrics.pop('_active_family_name', None)
                if status == "timeout":
                    status = "timeout_after_retries"  # Rename for clarity
                domain_results[status] = domain_results.get(status, 0) + 1
                total_processed += 1

                if status in ("optimized", "skipped"):
                    if family_bucket is not None:
                        family_bucket['items_discovered'] += 1
                        family_bucket['items_succeeded'] += 1
                elif status in ("error", "timeout_after_retries", "no_file", "empty_response"):
                    if family_bucket is not None:
                        family_bucket['items_discovered'] += 1
                        family_bucket['items_failed'] += 1
                else:
                    if family_bucket is not None:
                        family_bucket['items_discovered'] += 1
                        family_bucket['items_failed'] += 1
                
                # Update today's count after successful optimization
                if success and status == "optimized":
                    today_optimized += 1
                    domain_key = domain_info['full_domain']
                    if domain_key in limit_settings['today_counts']:
                        limit_settings['today_counts'][domain_key]['count'] = today_optimized
            
            all_results[domain_key] = domain_results
        
        # Calculate final metrics
        total_optimized = 0
        total_skipped = 0
        total_errors = 0
        total_timeout = 0
        total_no_file = 0
        total_empty_response = 0
        total_limit_reached = 0
        
        for domain_key, results in all_results.items():
            total_optimized += results['optimized']
            total_skipped += results['skipped']
            total_errors += results.get('error', 0)
            total_timeout += results.get('timeout_after_retries', 0)
            total_no_file += results.get('no_file', 0)
            total_empty_response += results.get('empty_response', 0)
            total_limit_reached += results.get('limit_reached', 0)
        
        # Calculate items_succeeded and items_failed
        items_succeeded = total_optimized + total_skipped
        # Daily-limit skips are intentional and should not be treated as failures.
        items_failed = total_errors + total_timeout + total_no_file + total_empty_response
        
        metrics['items_succeeded'] = items_succeeded
        metrics['items_failed'] = items_failed
        # Redefined discovered: only URLs with terminal outcomes (succeeded or failed).
        metrics['items_discovered'] = items_succeeded + items_failed
        
        # Overall Results
        print("\n" + "="*60)
        print("OPTIMIZATION SUMMARY BY DOMAIN")
        print("="*60)

        for domain_key, results in all_results.items():
            print(f"\n{domain_key}:")
            print(f"  Successfully optimized: {results['optimized']}")
            print(f"  Skipped: {results['skipped']}")
            if results.get('timeout', 0) > 0:
                print(f"  Timeout: {results['timeout']}")
            if results.get('error', 0) > 0:
                print(f"  Errors: {results['error']}")
            if results.get('limit_reached', 0) > 0:
                print(f"  Skipped due to daily limit: {results['limit_reached']}")

        # Only show totals if there are multiple domains
        if len(all_results) > 1:
            print("\n" + "="*60)
            print("TOTAL SUMMARY")
            print("="*60)
            print(f"Successfully optimized: {total_optimized}")
            print(f"Skipped: {total_skipped}")
            
            if total_timeout > 0:
                print(f"Timeout: {total_timeout}")
            if total_errors > 0:
                print(f"Errors: {total_errors}")
            if total_empty_response > 0:
                print(f"Empty LLM response: {total_empty_response}")
            if total_limit_reached > 0:
                print(f"Skipped due to daily limit: {total_limit_reached}")
                print("Note: daily-limit skips are excluded from items_failed.")
                
        print(f"\nTotal URLs processed: {total_processed}")
        
        if total_optimized > 0:
            print("Done! Optimized files overwritten in place.")
                
    except Exception as e:
        print(f"Critical error in main execution: {e}")
        metrics['status'] = 'fail'
        return metrics
    finally:
        # Calculate run duration
        end_time = time.time()
        run_duration_ms = int((end_time - start_time) * 1000)
        metrics['run_duration_ms'] = run_duration_ms
        metrics.pop('_active_family_name', None)

    return metrics

# ----------------------------------------------------
# 9. Run
# ----------------------------------------------------
if __name__ == "__main__":
    # Parse command line arguments here (only once)
    parser = argparse.ArgumentParser(description="Blog Post Optimizer with Domain-Aware Tracking")
    parser.add_argument("--sourcepath", required=True, 
                       help="Path to the blog repository (e.g., /user/mac/Documents/conholdate-blog)")
    parser.add_argument("--brand", 
                   choices=['aspose', 'aspose-cloud', 'conholdate', 'conholdate-cloud', 'groupdocs', 'groupdocs-cloud'], 
                   help="Brand to process")
    parser.add_argument("--limit", type=int, default=3,
                       help="Daily limit per domain (default: 1, use 0 for no limit)")
    
    args = parser.parse_args()
    
    # Run the main function and get metrics
    metrics = asyncio.run(main(args))
    
    print(f"Main returned metrics: {json.dumps(metrics, default=str)}")

    # Send API report(s) after main completes
    if metrics:
        print("\n" + "="*60)
        print("SENDING API REPORT")
        print("="*60)

        website = get_website_for_brand(args.brand)

        # Default env to DEV during testing. Send per-family payloads in the same run.
        send_api_reports_by_family(metrics['status'], metrics, website, "DEV")
    else:
        print("Skipping API report because main() returned no metrics object.")

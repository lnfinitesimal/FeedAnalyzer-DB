import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup
import json
import time
import re
from urllib.parse import urlparse
import os
import signal
import sys
from datetime import datetime

# --- CONFIGURATION ---
DB_FILE = 'ratings.json'
STATS_FILE = 'statistics.md'
# The script will force a save and exit after this many seconds (5 hours, 45 minutes)
MAX_RUNTIME_SECONDS = 20700 
START_TIME = time.time()

# We use a global flag so signal handlers can tell the main loop to stop gracefully
SHUTDOWN_REQUESTED = False

CATEGORIES = {
    "https://mediabiasfactcheck.com/left/": "Left",
    "https://mediabiasfactcheck.com/leftcenter/": "Left-Center",
    "https://mediabiasfactcheck.com/center/": "Center",
    "https://mediabiasfactcheck.com/right-center/": "Right-Center",
    "https://mediabiasfactcheck.com/right/": "Right",
    "https://mediabiasfactcheck.com/pro-science/": "Pro-Science",
    "https://mediabiasfactcheck.com/fake-news/": "Fake News",
    "https://mediabiasfactcheck.com/conspiracy/": "Conspiracy",
    "https://mediabiasfactcheck.com/satire/": "Satire"
}

EXCLUDED_PATHS = {
    'membership-account', 'support-media-bias-fact-check', 'filtered-search', 
    'whats-new-recently-added-sources-and-pages', 'left-vs-right-bias-how-we-rate-the-bias-of-media-sources',
    'about', 'methodology', 'contact', 'faq', 'donate', 'funding', 'submit-source', 
    'pseudoscience-dictionary', 're-evaluated-sources', 'changes-corrections', 'help-us-fact-check',
    'left', 'leftcenter', 'center', 'right-center', 'right', 'pro-science', 'fake-news', 'conspiracy', 'satire'
}

# --- ROBUST SESSION SETUP ---
def get_robust_session():
    """Configures a requests session that will stubbornly retry 429s and 500s."""
    session = requests.Session()
    # Retry 10 times. Backoff factor of 2 means it waits: 2s, 4s, 8s, 16s... on limits.
    retries = Retry(total=10, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('http://', HTTPAdapter(max_retries=retries))
    session.mount('https://', HTTPAdapter(max_retries=retries))
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })
    return session

# --- GRACEFUL SHUTDOWN HANDLER ---
def request_shutdown(signum, frame):
    """Catches GitHub Action cancellations (SIGINT/SIGTERM) to save data before dying."""
    global SHUTDOWN_REQUESTED
    print(f"\n[!] Cancellation signal ({signum}) received. Initiating emergency save and exit...")
    SHUTDOWN_REQUESTED = True

# Register the signals
signal.signal(signal.SIGINT, request_shutdown)
signal.signal(signal.SIGTERM, request_shutdown)

# --- CLEANUP FUNCTIONS ---
def clean_string(val):
    if not val: return None
    val_str = str(val).strip()
    if val_str.lower() in ["", "n/a", "unknown", "unrated", "none", "—"]:
        return None
    return val_str

def clean_name(title):
    t = clean_string(title)
    if not t: return None
    return re.sub(r'\s*[-–—]\s*Bias and Credibility.*$', '', t, flags=re.IGNORECASE).strip()

def clean_domain(url_text):
    u = clean_string(url_text)
    if not u: return None
    if "." not in u: return u
    if not u.startswith(('http://', 'https://')):
        u = 'http://' + u
    parsed = urlparse(u)
    domain = parsed.netloc.replace('www.', '')
    path = parsed.path.rstrip('/')
    return f"{domain}{path}"

def clean_bias(text):
    t = clean_string(text)
    if not t: return None
    t = re.sub(r'\([0-9.-]+\)', '', t) # Removes (9.5) but ignores 48/180
    t = re.sub(r'\bBIAS\b', '', t, flags=re.IGNORECASE).strip()
    return t.title()

def clean_factuality(text):
    t = clean_string(text)
    if not t: return None
    t = re.sub(r'\([0-9.-]+\)', '', t).strip()
    return t.title()

def clean_credibility(text):
    t = clean_string(text)
    if not t: return None
    t = re.sub(r'\bCREDIBILITY\b', '', t, flags=re.IGNORECASE).strip()
    return t.title()

def clean_traffic(text):
    t = clean_string(text)
    if not t: return None
    t = re.sub(r'\bTRAFFIC\b', '', t, flags=re.IGNORECASE).strip()
    return t.title()

def clean_freedom(text):
    t = clean_string(text)
    if not t: return None
    match = re.search(r'(\d+/\d+)', t) # Catches RSF "48/180" format safely
    if match: return f"RSF {match.group(1)}"
    return t.title()

# --- EXTRACTION LOGIC ---
def extract_source_data(html_content, review_url, category):
    """Parses an MBFC review page and returns only valid fields."""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Base dictionary setup
    raw_data = {
        "Name": None, "Review URL": review_url, "Category": category,
        "Domain/Source": None, "Media Type": None, "Traffic/Popularity": None,
        "Bias": None, "Reasoning": None, "Factuality": None, 
        "Credibility": None, "Freedom": None, "Country": None, "Last Updated": None
    }

    # Name
    title_tag = soup.find('h1', class_='entry-title') or soup.find('h1')
    if title_tag:
        raw_data["Name"] = clean_name(title_tag.get_text(strip=True))

    entry_content = soup.find('div', class_='entry-content')
    if not entry_content:
        return {k: v for k, v in raw_data.items() if v is not None} # Return stripped dict

    # Last Updated Date extraction
    update_tag = entry_content.find(lambda tag: tag.name == "p" and "Last Updated on" in tag.text)
    if update_tag:
        match = re.search(r'Last Updated on ([a-zA-Z]+ \d{1,2}, \d{4})', update_tag.text)
        if match:
            raw_data["Last Updated"] = match.group(1)

    lines =[line.strip() for line in entry_content.stripped_strings if line.strip()]

    # Text extraction
    for line in lines:
        lower_line = line.lower()
        if lower_line.startswith("bias rating:"): raw_data["Bias"] = clean_bias(line.split(":", 1)[1])
        elif lower_line.startswith("factual reporting:"): raw_data["Factuality"] = clean_factuality(line.split(":", 1)[1])
        elif lower_line.startswith("country:"): raw_data["Country"] = clean_string(line.split(":", 1)[1])
        elif "freedom rating:" in lower_line or "freedom rank:" in lower_line: raw_data["Freedom"] = clean_freedom(line.split(":", 1)[1])
        elif lower_line.startswith("media type:"): raw_data["Media Type"] = clean_string(line.split(":", 1)[1])
        elif lower_line.startswith("traffic/popularity:"): raw_data["Traffic/Popularity"] = clean_traffic(line.split(":", 1)[1])
        elif lower_line.startswith("mbfc credibility rating:"): raw_data["Credibility"] = clean_credibility(line.split(":", 1)[1])
        elif lower_line.startswith("questionable reasoning:") or lower_line.startswith("reasoning:"): raw_data["Reasoning"] = clean_string(line.split(":", 1)[1])
        elif lower_line.startswith("source:"): raw_data["Domain/Source"] = clean_domain(line.split(":", 1)[1])

    # Domain Fallback
    if not raw_data["Domain/Source"]:
        for p in entry_content.find_all(['p', 'div']):
            if p.get_text(strip=True).lower().startswith("source:"):
                a_tag = p.find('a', href=True)
                if a_tag: raw_data["Domain/Source"] = clean_domain(a_tag['href'])
                break

    # Image Fallbacks
    for img in entry_content.find_all('img'):
        alt_text = img.get('alt', '').lower()
        if "factual reporting:" in alt_text and not raw_data["Factuality"]:
            fact_match = re.search(r'factual reporting:\s*([^-]+)', alt_text, re.IGNORECASE)
            if fact_match: raw_data["Factuality"] = clean_factuality(fact_match.group(1))
        
        if not raw_data["Bias"]:
            if "satire" in alt_text: raw_data["Bias"] = "Satire"
            elif "pro science" in alt_text or "pro-science" in alt_text: raw_data["Bias"] = "Pro-Science"
            elif "least biased" in alt_text: raw_data["Bias"] = "Least Biased"
            elif "left center bias" in alt_text: raw_data["Bias"] = "Left-Center"
            elif "right center bias" in alt_text: raw_data["Bias"] = "Right-Center"
            elif "left bias" in alt_text: raw_data["Bias"] = "Left"
            elif "right bias" in alt_text: raw_data["Bias"] = "Right"

    # Remove keys with None values (Leaves an incredibly clean JSON)
    return {k: v for k, v in raw_data.items() if v is not None}

# --- IO & STATS ---
def load_db():
    if os.path.exists(DB_FILE):
        with open(DB_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    # Initialize empty DB with keys for all categories
    return {cat:[] for cat in CATEGORIES.values()}

def save_db(db):
    """Sorts and safely saves the database to disk."""
    for category in db:
        # Sorts by 'Name' alphabetically (treating missing names as empty strings for sorting safety)
        db[category] = sorted(db[category], key=lambda x: x.get('Name', '').lower())
    
    with open(DB_FILE, 'w', encoding='utf-8') as f:
        json.dump(db, f, indent=4, ensure_ascii=False)

def generate_statistics(db):
    """Generates the Markdown statistics file."""
    total_sources = sum(len(sources) for sources in db.values())
    
    cat_counts = {cat: len(sources) for cat, sources in db.items()}
    
    bias_tally = {}
    fact_tally = {}
    for sources in db.values():
        for src in sources:
            b = src.get('Bias', 'Unknown')
            f = src.get('Factuality', 'Unknown')
            bias_tally[b] = bias_tally.get(b, 0) + 1
            fact_tally[f] = fact_tally.get(f, 0) + 1

    md = f"# MBFC Database Statistics\n\n"
    md += f"**Last Updated:** {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
    md += f"**Total Sources Indexed:** {total_sources}\n\n"
    
    md += "### Categories Breakdown\n"
    md += "| Category | Source Count |\n|---|---|\n"
    for cat, count in sorted(cat_counts.items(), key=lambda item: item[1], reverse=True):
        md += f"| {cat} | {count} |\n"
        
    md += "\n### Top Bias Ratings\n"
    md += "| Bias | Count |\n|---|---|\n"
    for b, count in sorted(bias_tally.items(), key=lambda item: item[1], reverse=True)[:10]:
        md += f"| {b} | {count} |\n"

    with open(STATS_FILE, 'w', encoding='utf-8') as f:
        f.write(md)

# --- MASTER ENGINE ---
def main():
    global SHUTDOWN_REQUESTED
    session = get_robust_session()
    db = load_db()
    
    # 1. Build a lookup set of all currently scraped URLs to avoid duplicate work
    scraped_urls = set()
    for cat, sources in db.items():
        for src in sources:
            scraped_urls.add(src['Review URL'])

    print("[INFO] Harvesting Master List from 9 Category Pages...")
    pending_tasks = {} # format: { category: [list of review_urls] }
    
    for url, cat_name in CATEGORIES.items():
        pending_tasks[cat_name] =[]
        try:
            r = session.get(url, timeout=15)
            soup = BeautifulSoup(r.text, 'html.parser')
            
            raw_links =[]
            mbfc_table = soup.find('table', id='mbfc-table')
            if mbfc_table:
                raw_links = mbfc_table.find_all('a', href=True)
            else:
                # Fenced fallback for Satire
                entry = soup.select_one('div.entry-content')
                if entry:
                    collecting = False
                    for element in entry.children:
                        if element.name is None: continue
                        text = element.get_text()
                        if "Click the links below" in text:
                            collecting = True; continue
                        if "class" in element.attrs and "post-modified-info" in element.attrs.get("class",[]):
                            break
                        if collecting:
                            raw_links.extend(element.find_all('a', href=True))
            
            # Filter
            for a in raw_links:
                href = a['href'].strip()
                parsed = urlparse(href)
                path_seg = parsed.path.strip('/').split('/')[0] if parsed.path else ""
                if ('mediabiasfactcheck.com' in href or href.startswith('/')) and path_seg not in EXCLUDED_PATHS:
                    if href not in scraped_urls:
                        pending_tasks[cat_name].append(href)
                        
        except Exception as e:
            print(f"[!] Error fetching category {cat_name}: {e}")

    # 2. Prioritize Categories (Smallest pending queue goes first)
    sorted_tasks = sorted(pending_tasks.items(), key=lambda item: len(item[1]))
    
    total_pending = sum(len(urls) for urls in pending_tasks.values())
    print(f"\n[INFO] Resume check complete. {total_pending} new sources pending extraction.\n")

    if total_pending == 0:
        print("[OK] Database is 100% up to date. Updating Statistics just in case.")
        generate_statistics(db)
        return

    # 3. Extraction Loop
    urls_processed_this_run = 0
    
    for cat_name, urls in sorted_tasks:
        if len(urls) == 0: continue
        
        print(f"--- Starting Category: {cat_name} ({len(urls)} pending) ---")
        
        for href in urls:
            # CHECK TIMEOUT OR CANCEL SIGNAL
            elapsed_time = time.time() - START_TIME
            if elapsed_time > MAX_RUNTIME_SECONDS or SHUTDOWN_REQUESTED:
                print("\n[!] Time limit reached or Cancel requested. Executing emergency save...")
                save_db(db)
                generate_statistics(db)
                sys.exit(0) # Exits cleanly so GitHub YAML can still commit!
                
            try:
                r = session.get(href, timeout=10)
                if r.status_code == 404:
                    print(f"[WARN] 404 Not Found (Skipping): {href}")
                    continue
                    
                source_data = extract_source_data(r.text, href, cat_name)
                db[cat_name].append(source_data)
                urls_processed_this_run += 1
                
                # The sleek logging format
                name = source_data.get('Name', 'Unknown')
                bias = source_data.get('Bias', 'N/A')
                fact = source_data.get('Factuality', 'N/A')
                print(f"[OK] {name} | B: {bias} | F: {fact}")
                
                # Incremental Save every 25 successful pulls to guarantee no data loss
                if urls_processed_this_run % 25 == 0:
                    save_db(db)
                    
            except Exception as e:
                print(f"[ERR] Failed to parse {href}: {e}")
            
            # Anti-bot delay
            time.sleep(1.5)

    # 4. Final Save & Stats Gen
    print("\n[INFO] All targeted URLs processed successfully. Saving final DB...")
    save_db(db)
    generate_statistics(db)

if __name__ == "__main__":
    main()

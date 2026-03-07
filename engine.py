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
MAX_RUNTIME_SECONDS = 20700  # 5 hours, 45 minutes (Ensures graceful exit before GitHub's 6h limit)
START_TIME = time.time()

# Global variables for graceful shutdown handling
global_db = {}
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

# --- INITIALIZATION ---
def init_files():
    """Ensures files exist immediately so git add/commit never fails on an early exit."""
    if not os.path.exists(DB_FILE):
        with open(DB_FILE, 'w', encoding='utf-8') as f:
            json.dump({cat:[] for cat in CATEGORIES.values()}, f)
    if not os.path.exists(STATS_FILE):
        with open(STATS_FILE, 'w', encoding='utf-8') as f:
            f.write("# MBFC Database Statistics\n\nInitializing...\n")

# --- GRACEFUL SHUTDOWN HANDLER ---
def request_shutdown(signum, frame):
    """Catches GitHub Action manual cancellations to save data before dying."""
    global SHUTDOWN_REQUESTED, global_db
    print(f"\n[!] Cancellation signal ({signum}) received. Saving current progress immediately...", flush=True)
    SHUTDOWN_REQUESTED = True
    save_db(global_db)
    generate_statistics(global_db)
    sys.exit(0) # Exit cleanly to allow git commit to run

signal.signal(signal.SIGINT, request_shutdown)
signal.signal(signal.SIGTERM, request_shutdown)

# --- ROBUST SESSION SETUP ---
def get_robust_session():
    session = requests.Session()
    # Aggressive exponential backoff: 2s, 4s, 8s, 16s... on limits or server errors.
    retries = Retry(total=10, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('http://', HTTPAdapter(max_retries=retries))
    session.mount('https://', HTTPAdapter(max_retries=retries))
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })
    return session

# --- STRING CLEANUP ---
def clean_string(val):
    if not val: return None
    val_str = str(val).strip()
    if val_str.lower() in["", "n/a", "unknown", "unrated", "none", "—"]: return None
    return val_str

def clean_name(title):
    t = clean_string(title)
    if not t: return None
    return re.sub(r'\s*[-–—]\s*Bias and Credibility.*$', '', t, flags=re.IGNORECASE).strip()

def clean_domain(url_text):
    u = clean_string(url_text)
    if not u: return None
    if "." not in u: return u
    if not u.startswith(('http://', 'https://')): u = 'http://' + u
    parsed = urlparse(u)
    return f"{parsed.netloc.replace('www.', '')}{parsed.path.rstrip('/')}"

def clean_bias(text):
    t = clean_string(text)
    if not t: return None
    t = re.sub(r'\([0-9.-]+\)', '', t) # Removes (9.5) or (-1.2)
    t = re.sub(r'\bBIAS\b', '', t, flags=re.IGNORECASE).strip()
    return t.title()

def clean_factuality(text):
    t = clean_string(text)
    if not t: return None
    t = re.sub(r'\([0-9.-]+\)', '', t).strip() # Removes (9.5)
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
    match = re.search(r'(\d+/\d+)', t) # Protects and formats RSF "48/180" format
    if match: return f"RSF {match.group(1)}"
    return t.title()

# --- HTML PROCESSOR ---
def get_clean_text(html_string):
    """Converts break tags to newlines before stripping HTML so text blocks don't mash together."""
    s = re.sub(r'<br\s*/?>', '\n', html_string, flags=re.IGNORECASE)
    s = re.sub(r'</(p|div|li|h[1-6])>', '\n', s, flags=re.IGNORECASE)
    soup = BeautifulSoup(s, 'html.parser')
    text = soup.get_text(separator=' ').replace('\xa0', ' ')
    text = re.sub(r' {2,}', ' ', text)
    lines =[line.strip() for line in text.split('\n') if line.strip()]
    return '\n'.join(lines)

# --- EXTRACTION LOGIC ---
def extract_source_data(html_content, review_url, category):
    soup = BeautifulSoup(html_content, 'html.parser')
    
    raw_data = {
        "Name": None, "Review URL": review_url, "Category": category,
        "Domain/Source": None, "Media Type": None, "Traffic/Popularity": None,
        "Bias": None, "Reasoning": None, "Factuality": None, 
        "Credibility": None, "Freedom": None, "Country": None, "Last Updated": None
    }

    title_tag = soup.find('h1', class_='entry-title') or soup.find('h1')
    if title_tag:
        raw_data["Name"] = clean_name(title_tag.get_text(strip=True))

    entry_content = soup.find('div', class_='entry-content')
    if not entry_content:
        return {k: v for k, v in raw_data.items() if v is not None}

    text_content = get_clean_text(str(entry_content))

    # Regex Targeting
    m = re.search(r'Bias Rating:\s*([^\n]+)', text_content, re.IGNORECASE)
    if m: raw_data["Bias"] = clean_bias(m.group(1))

    m = re.search(r'Factual Reporting:\s*([^\n]+)', text_content, re.IGNORECASE)
    if m: raw_data["Factuality"] = clean_factuality(m.group(1))

    m = re.search(r'Country:\s*([^\n]+)', text_content, re.IGNORECASE)
    if m: raw_data["Country"] = clean_string(m.group(1))

    m = re.search(r'(?:Country Freedom Rating|Freedom Rank|World Press Freedom Rank):\s*([^\n]+)', text_content, re.IGNORECASE)
    if m: raw_data["Freedom"] = clean_freedom(m.group(1))

    m = re.search(r'Media Type:\s*([^\n]+)', text_content, re.IGNORECASE)
    if m: raw_data["Media Type"] = clean_string(m.group(1))

    m = re.search(r'Traffic/Popularity:\s*([^\n]+)', text_content, re.IGNORECASE)
    if m: raw_data["Traffic/Popularity"] = clean_traffic(m.group(1))

    m = re.search(r'MBFC Credibility Rating:\s*([^\n]+)', text_content, re.IGNORECASE)
    if m: raw_data["Credibility"] = clean_credibility(m.group(1))

    m = re.search(r'(?:Questionable Reasoning|Reasoning):\s*([^\n]+)', text_content, re.IGNORECASE)
    if m: raw_data["Reasoning"] = clean_string(m.group(1))

    m = re.search(r'Source:\s*([^\n]+)', text_content, re.IGNORECASE)
    if m: 
        raw_data["Domain/Source"] = clean_domain(m.group(1))
    else:
        for p in entry_content.find_all(['p', 'div']):
            if p.get_text(strip=True).lower().startswith("source:"):
                a_tag = p.find('a', href=True)
                if a_tag: raw_data["Domain/Source"] = clean_domain(a_tag['href'])
                break

    m = re.search(r'Last Updated on ([a-zA-Z]+ \d{1,2}, \d{4})', text_content, re.IGNORECASE)
    if m: raw_data["Last Updated"] = m.group(1)

    # Image Fallbacks
    for img in entry_content.find_all('img'):
        alt = img.get('alt', '').lower()
        if "factual reporting:" in alt and not raw_data["Factuality"]:
            fm = re.search(r'factual reporting:\s*([^-]+)', alt, re.IGNORECASE)
            if fm: raw_data["Factuality"] = clean_factuality(fm.group(1))
        
        if not raw_data["Bias"]:
            if "extreme left" in alt: raw_data["Bias"] = "Extreme Left"
            elif "extreme right" in alt: raw_data["Bias"] = "Extreme Right"
            elif "left center" in alt: raw_data["Bias"] = "Left-Center"
            elif "right center" in alt: raw_data["Bias"] = "Right-Center"
            elif "least biased" in alt: raw_data["Bias"] = "Least Biased"
            elif "left bias" in alt: raw_data["Bias"] = "Left"
            elif "right bias" in alt: raw_data["Bias"] = "Right"
            elif "pro science" in alt or "pro-science" in alt: raw_data["Bias"] = "Pro-Science"
            elif "satire" in alt: raw_data["Bias"] = "Satire"
            elif "conspiracy" in alt or "pseudoscience" in alt: raw_data["Bias"] = "Conspiracy-Pseudoscience"
            elif "questionable" in alt: raw_data["Bias"] = "Questionable"

    # Eliminate `None` values
    return {k: v for k, v in raw_data.items() if v is not None}

# --- IO & STATS ---
def load_db():
    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError:
            pass
    return {cat:[] for cat in CATEGORIES.values()}

def save_db(db):
    for category in db:
        # Sorts lists natively. Does not mutate the data to lowercase.
        db[category] = sorted(db[category], key=lambda x: x.get('Name', '').lower())
    with open(DB_FILE, 'w', encoding='utf-8') as f:
        json.dump(db, f, indent=4, ensure_ascii=False)

def generate_statistics(db):
    total = sum(len(srcs) for srcs in db.values())
    cat_counts = {cat: len(srcs) for cat, srcs in db.items()}
    bias_tally, fact_tally = {}, {}
    
    for srcs in db.values():
        for src in srcs:
            b = src.get('Bias', 'Unknown')
            f = src.get('Factuality', 'Unknown')
            bias_tally[b] = bias_tally.get(b, 0) + 1
            fact_tally[f] = fact_tally.get(f, 0) + 1

    md = f"# MBFC Database Statistics\n\n"
    md += f"**Last Updated:** {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
    md += f"**Total Sources Indexed:** {total}\n\n"
    
    md += "### Categories Breakdown\n| Category | Count |\n|---|---|\n"
    for cat, count in sorted(cat_counts.items(), key=lambda i: i[1], reverse=True):
        md += f"| {cat} | {count} |\n"
        
    md += "\n### Top Bias Ratings\n| Bias | Count |\n|---|---|\n"
    for b, count in sorted(bias_tally.items(), key=lambda i: i[1], reverse=True)[:10]:
        md += f"| {b} | {count} |\n"

    with open(STATS_FILE, 'w', encoding='utf-8') as f:
        f.write(md)

# --- MASTER ENGINE ---
def main():
    global global_db, SHUTDOWN_REQUESTED
    init_files()
    session = get_robust_session()
    global_db = load_db()
    
    # 1. Look up existing sources to avoid duplicate fetching
    scraped_urls = {src['Review URL'] for srcs in global_db.values() for src in srcs}

    print("[INFO] Harvesting Master List from 9 Category Pages...", flush=True)
    pending_tasks = {}
    
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
            
            for a in raw_links:
                href = a['href'].strip()
                parsed = urlparse(href)
                path_seg = parsed.path.strip('/').split('/')[0] if parsed.path else ""
                
                # Check condition 1: is internal, 2: not excluded, 3: not already in DB
                if ('mediabiasfactcheck.com' in href or href.startswith('/')) and path_seg not in EXCLUDED_PATHS:
                    if href not in scraped_urls:
                        pending_tasks[cat_name].append(href)
            
            print(f"  -> {cat_name}: Found {len(raw_links)} total links. {len(pending_tasks[cat_name])} new pending.", flush=True)
                        
        except Exception as e:
            print(f"[!] Error fetching category {cat_name}: {e}", flush=True)

    # 2. Prioritize: Categories with the fewest pending items go first
    sorted_tasks = sorted(pending_tasks.items(), key=lambda item: len(item[1]))
    total_pending = sum(len(urls) for urls in pending_tasks.values())
    print(f"\n[INFO] Resume check complete. {total_pending} new sources pending extraction.\n", flush=True)

    if total_pending == 0:
        print("[OK] Database is 100% up to date. Updating Statistics just in case.", flush=True)
        generate_statistics(global_db)
        return

    # 3. Extraction Loop
    urls_processed_this_run = 0
    
    for cat_name, urls in sorted_tasks:
        if len(urls) == 0: continue
        
        total_in_cat = len(urls)
        print(f"\n--- Starting Category: {cat_name} ({total_in_cat} pending) ---", flush=True)
        
        for idx, href in enumerate(urls, 1):
            if time.time() - START_TIME > MAX_RUNTIME_SECONDS or SHUTDOWN_REQUESTED:
                print("\n[!] Time limit reached or Cancel requested. Executing emergency save...", flush=True)
                save_db(global_db)
                generate_statistics(global_db)
                sys.exit(0)
                
            try:
                r = session.get(href, timeout=10)
                if r.status_code == 404:
                    print(f"[ERR] [{idx}/{total_in_cat}] 404 Not Found: {href}", flush=True)
                    continue
                    
                source_data = extract_source_data(r.text, href, cat_name)
                global_db[cat_name].append(source_data)
                urls_processed_this_run += 1
                
                # --- Dynamic Sleek Logger (Prints ALL available metrics) ---
                log_parts =[f"[✓][{idx}/{total_in_cat}] {source_data.get('Name', 'Unknown')}"]
                
                if 'Bias' in source_data: log_parts.append(f"B: {source_data['Bias']}")
                if 'Factuality' in source_data: log_parts.append(f"F: {source_data['Factuality']}")
                if 'Credibility' in source_data: log_parts.append(f"C: {source_data['Credibility']}")
                if 'Freedom' in source_data: log_parts.append(f"FR: {source_data['Freedom']}")
                if 'Traffic/Popularity' in source_data: log_parts.append(f"T: {source_data['Traffic/Popularity']}")
                if 'Media Type' in source_data: log_parts.append(f"Media: {source_data['Media Type']}")
                if 'Country' in source_data: log_parts.append(f"Ctry: {source_data['Country']}")
                if 'Reasoning' in source_data: log_parts.append(f"Rsn: {source_data['Reasoning']}")
                
                print(" | ".join(log_parts), flush=True)
                
                # Incremental Save every 10 successful pulls to guarantee minimal data loss
                if urls_processed_this_run % 10 == 0:
                    save_db(global_db)
                    
            except Exception as e:
                print(f"[ERR] [{idx}/{total_in_cat}] Failed to parse {href}: {e}", flush=True)
            
            time.sleep(1.5) # Anti-bot delay

    print("\n[INFO] All targeted URLs processed successfully. Saving final DB...", flush=True)
    save_db(global_db)
    generate_statistics(global_db)

if __name__ == "__main__":
    main()

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
MAX_RUNTIME_SECONDS = 20700  # 5 hours, 45 mins
START_TIME = time.time()

# Global variables
global_db = {}
SHUTDOWN_REQUESTED = False

CATEGORIES = {
    "https://mediabiasfactcheck.com/left/": "Left",
    "https://mediabiasfactcheck.com/leftcenter/": "Left-Center",
    "https://mediabiasfactcheck.com/center/": "Least-Biased",
    "https://mediabiasfactcheck.com/right-center/": "Right-Center",
    "https://mediabiasfactcheck.com/right/": "Right",
    "https://mediabiasfactcheck.com/pro-science/": "Pro-Science",
    "https://mediabiasfactcheck.com/fake-news/": "Questionable",
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

# --- GRACEFUL SHUTDOWN & INIT ---
def init_files():
    if not os.path.exists(DB_FILE):
        with open(DB_FILE, 'w', encoding='utf-8') as f:
            json.dump({cat:[] for cat in CATEGORIES.values()}, f)
    if not os.path.exists(STATS_FILE):
        with open(STATS_FILE, 'w', encoding='utf-8') as f:
            f.write("# MBFC Database Statistics\n")

def request_shutdown(signum, frame):
    global SHUTDOWN_REQUESTED, global_db
    print(f"\n[!] Cancellation signal ({signum}) received. Executing Emergency Save...", flush=True)
    SHUTDOWN_REQUESTED = True
    save_db(global_db)
    generate_statistics(global_db)
    print("[!] Emergency Save Complete. Shutting down.", flush=True)
    sys.exit(0) 

signal.signal(signal.SIGINT, request_shutdown)
signal.signal(signal.SIGTERM, request_shutdown)

def get_robust_session():
    session = requests.Session()
    retries = Retry(total=10, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('http://', HTTPAdapter(max_retries=retries))
    session.mount('https://', HTTPAdapter(max_retries=retries))
    session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
    return session

# --- CLEANUPS ---
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
    return f"{urlparse(u).netloc.replace('www.', '')}{urlparse(u).path.rstrip('/')}"

def clean_bias(text):
    t = clean_string(text)
    if not t: return None
    t = re.sub(r'\([0-9.-]+\)', '', t) 
    t = re.sub(r'\bBIAS\b', '', t, flags=re.IGNORECASE)
    t = re.sub(r'-\s+', '-', t) # Converts "Right Conspiracy- Pseudoscience" to "Right Conspiracy-Pseudoscience"
    return t.strip().title()

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
    match = re.search(r'(\d+/\d+)', t) 
    if match: return f"RSF {match.group(1)}"
    return t.title()

# --- PRE-PROCESSOR ---
def get_clean_text(html_string):
    s = re.sub(r'<br\s*/?>', '\n', html_string, flags=re.IGNORECASE)
    s = re.sub(r'</(p|div|li|h[1-6])>', '\n', s, flags=re.IGNORECASE)
    soup = BeautifulSoup(s, 'html.parser')
    text = soup.get_text(separator=' ').replace('\xa0', ' ')
    text = re.sub(r' {2,}', ' ', text)
    lines =[line.strip() for line in text.split('\n') if line.strip()]
    return '\n'.join(lines)

# --- EXTRACTION ---
def extract_source_data(html_content, review_url):
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Restructured and updated key namings per user specs (No redundant 'Category' tag here)
    raw_data = {
        "Name": None, "Review": review_url, "Source": None, "Type": None, 
        "Traffic": None, "Bias": None, "Reasoning": None, "Factuality": None, 
        "Credibility": None, "Freedom": None, "Country": None, "Updated": None
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
    if m: raw_data["Type"] = clean_string(m.group(1))

    m = re.search(r'Traffic/Popularity:\s*([^\n]+)', text_content, re.IGNORECASE)
    if m: raw_data["Traffic"] = clean_traffic(m.group(1))

    m = re.search(r'MBFC Credibility Rating:\s*([^\n]+)', text_content, re.IGNORECASE)
    if m: raw_data["Credibility"] = clean_credibility(m.group(1))

    m = re.search(r'(?:Questionable Reasoning|Reasoning):\s*([^\n]+)', text_content, re.IGNORECASE)
    if m: raw_data["Reasoning"] = clean_string(m.group(1))

    m = re.search(r'Source:\s*([^\n]+)', text_content, re.IGNORECASE)
    if m: 
        raw_data["Source"] = clean_domain(m.group(1))
    else:
        for p in entry_content.find_all(['p', 'div']):
            if p.get_text(strip=True).lower().startswith("source:"):
                a_tag = p.find('a', href=True)
                if a_tag: raw_data["Source"] = clean_domain(a_tag['href'])
                break

    m = re.search(r'Last Updated on ([a-zA-Z]+ \d{1,2}, \d{4})', text_content, re.IGNORECASE)
    if m: raw_data["Updated"] = m.group(1)

    # Corrected full Image Fallbacks
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
        db[category] = sorted(db[category], key=lambda x: x.get('Name', '').lower())
    with open(DB_FILE, 'w', encoding='utf-8') as f:
        json.dump(db, f, indent=4, ensure_ascii=False)

def generate_statistics(db):
    """Safely calculates and constructs the markdown page regardless of nulls or empties"""
    try:
        total = sum(len(srcs) for srcs in db.values())
        cat_counts = {cat: len(srcs) for cat, srcs in db.items()}
        bias_tally, fact_tally, free_tally = {}, {}, {}
        
        for srcs in db.values():
            for src in srcs:
                b = src.get('Bias', 'Unknown')
                f = src.get('Factuality', 'Unknown')
                fr = src.get('Freedom', 'Unknown')
                bias_tally[b] = bias_tally.get(b, 0) + 1
                fact_tally[f] = fact_tally.get(f, 0) + 1
                free_tally[fr] = free_tally.get(fr, 0) + 1

        md = f"# MBFC Database Statistics\n\n"
        md += fCentert Indexed:** {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
        md += f"**Total Valid Sources Indexed:** {total}\n\n"
        
        md += "### Core Category Progress\n| Category | Source Count |\n|---|---|\n"
        for cat, count in sorted(cat_counts.items(), key=lambda i: i[1], reverse=True):
            md += f"| {cat} | {count} |\n"
            
        md += "\n### Master Bias Breakdown\n| Detected Bias Rating | Total Occurrences |\n|---|---|\n"
        for b, count in sorted(bias_tally.items(), key=lambda i: i[1], reverse=True)[:10]:
            if b != "Unknown": md += f"| {b} | {count} |\n"

        md += "\n### Factuality Scoring\n| Factuality Rating | Total Occurrences |\n|---|---|\n"
        for f, count in sorted(fact_tally.items(), key=lambda i: i[1], reverse=True)[:10]:
            if f != "Unknown": md += f"| {f} | {count} |\n"

        with open(STATS_FILE, 'w', encoding='utf-8') as file_obj:
            file_obj.write(md)
            
    except Exception as e:
        print(f"[!] Warning: Minor failure during Statistics markdown generation: {e}")


# --- THE MAIN LOOP ---
def main():
    global global_db, SHUTDOWN_REQUESTED
    init_files()
    session = get_robust_session()
    global_db = load_db()
    
    # 1. Grab valid existing 'Review URLs' as reference check list
    scraped_urls = {src.get('Review') for srcs in global_db.values() for src in srcs if src.get('Review')}

    print("[INFO] Harvesting Master List from 9 Category Pages...\n", flush=True)
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
            
            # Using Set prevents fake duplicates directly posted onto a single category page. 
            unique_valid_links_for_this_page = set()
            
            for a in raw_links:
                href = a['href'].strip()
                parsed = urlparse(href)
                path_seg = parsed.path.strip('/').split('/')[0] if parsed.path else ""
                
                # Check 1: is internal, Check 2: not an excluded site page
                if ('mediabiasfactcheck.com' in href or href.startswith('/')) and path_seg not in EXCLUDED_PATHS:
                    unique_valid_links_for_this_page.add(href)
                    
            for link in unique_valid_links_for_this_page:
                if link not in scraped_urls:
                    pending_tasks[cat_name].append(link)
            
            in_db_for_this_page = len(unique_valid_links_for_this_page) - len(pending_tasks[cat_name])
            print(f"  -> {cat_name}: {len(unique_valid_links_for_this_page)} Found. {in_db_for_this_page} DB. {len(pending_tasks[cat_name])} New.", flush=True)
                        
        except Exception as e:
            print(f"[!] Error fetching category {cat_name}: {e}", flush=True)

    sorted_tasks = sorted(pending_tasks.items(), key=lambda item: len(item[1]))
    total_pending = sum(len(urls) for urls in pending_tasks.values())
    print(f"\n[INFO] Resume check complete. {total_pending} totally new review URLs mapped and awaiting extraction.", flush=True)

    if total_pending == 0:
        print("\n[OK] Database is exactly synced with all Category endpoints. Verifying/writing statistics...", flush=True)
        generate_statistics(global_db)
        return

    # 3. Extraction Runtime Loop
    urls_processed_this_run = 0
    
    for cat_name, urls in sorted_tasks:
        if len(urls) == 0: continue
        total_in_cat = len(urls)
        print(f"\n--- Launching Target Pipeline for Category: {cat_name} ({total_in_cat} queued) ---", flush=True)
        
        for idx, href in enumerate(urls, 1):
            if time.time() - START_TIME > MAX_RUNTIME_SECONDS or SHUTDOWN_REQUESTED:
                print(f"\n[!] Forced cutoff parameters hit. System successfully processed {urls_processed_this_run} links before suspending run.", flush=True)
                save_db(global_db)
                generate_statistics(global_db)
                sys.exit(0)
                
            try:
                r = session.get(href, timeout=10)
                if r.status_code == 404:
                    print(f"[{idx}/{total_in_cat}] [!] Dead/Removed Link Found (404 Code) Skipping Data Trace: {href}", flush=True)
                    continue
                    
                source_data = extract_source_data(r.text, href) # Do NOT pass Category as requested by user params
                global_db[cat_name].append(source_data)
                urls_processed_this_run += 1
                
                # Requested logging visual design including keys mapping checks.
                log_parts = [f"[{idx}/{total_in_cat}] [✓] {source_data.get('Name', 'Unknown Name Object')}"]
                
                if 'Bias' in source_data: log_parts.append(f"B: {source_data['Bias']}")
                if 'Factuality' in source_data: log_parts.append(f"F: {source_data['Factuality']}")
                if 'Credibility' in source_data: log_parts.append(f"C: {source_data['Credibility']}")
                if 'Freedom' in source_data: log_parts.append(f"FR: {source_data['Freedom']}")
                if 'Traffic' in source_data: log_parts.append(f"T: {source_data['Traffic']}")
                if 'Type' in source_data: log_parts.append(f"Media: {source_data['Type']}")
                if 'Country' in source_data: log_parts.append(f"Ctry: {source_data['Country']}")
                if 'Reasoning' in source_data: log_parts.append(f"Rsn: {source_data['Reasoning']}")
                
                print(" | ".join(log_parts), flush=True)
                
                if urls_processed_this_run % 10 == 0:
                    save_db(global_db)
                    
            except Exception as e:
                print(f"[{idx}/{total_in_cat}] [!] Fatal Extractor Fault on trace for URL ({href}) Context dump: {e}", flush=True)
            
            time.sleep(1.2) # Active Anti-block system rate. Default is generous 

    print(f"\n[INFO] Success loop resolved totally. Extractor finalized operation passing off context file state processing ...", flush=True)
    save_db(global_db)
    generate_statistics(global_db)

if __name__ == "__main__":
    main()

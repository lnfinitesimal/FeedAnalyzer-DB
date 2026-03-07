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
import traceback

# --- CONFIGURATION ---
DB_FILE = 'ratings.json'
STATS_FILE = 'statistics.md'
MAX_RUNTIME_SECONDS = 14400  # 4 hours timeout limit for Actions
START_TIME = time.time()

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

# --- IO / FILE SETUP ---
def init_files():
    if not os.path.exists(DB_FILE):
        with open(DB_FILE, 'w', encoding='utf-8') as f:
            json.dump({cat:[] for cat in CATEGORIES.values()}, f, indent=4, ensure_ascii=False)
    if not os.path.exists(STATS_FILE):
        with open(STATS_FILE, 'w', encoding='utf-8') as f:
            f.write("# MBFC Database Statistics\n\nInitializing...\n")

def request_shutdown(signum, frame):
    global SHUTDOWN_REQUESTED, global_db
    if not SHUTDOWN_REQUESTED:
        print(f"\n[!] Cancellation requested. Finishing the current URL gracefully, then saving data...", flush=True)
        SHUTDOWN_REQUESTED = True

signal.signal(signal.SIGINT, request_shutdown)
signal.signal(signal.SIGTERM, request_shutdown)

def get_robust_session():
    session = requests.Session()
    retries = Retry(total=10, backoff_factor=3, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('http://', HTTPAdapter(max_retries=retries))
    session.mount('https://', HTTPAdapter(max_retries=retries))
    session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})
    return session

def load_db():
    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for req_cat in CATEGORIES.values():
                    if req_cat not in data:
                        data[req_cat] =[]
                return data
        except json.JSONDecodeError:
            pass
    return {cat:[] for cat in CATEGORIES.values()}

def save_db(db):
    for category in db:
        if isinstance(db[category], list):
            db[category] = sorted(db[category], key=lambda x: str(x.get('Name', '')).lower())
    with open(DB_FILE, 'w', encoding='utf-8') as f:
        json.dump(db, f, indent=4, ensure_ascii=False)

def generate_statistics(db):
    """Safely constructs statistical report unconditionally even on null inputs/failed fetches."""
    try:
        total = sum(len(srcs) for srcs in db.values() if isinstance(srcs, list))
        cat_counts = {cat: len(srcs) for cat, srcs in db.items() if isinstance(srcs, list)}
        bias_tally, fact_tally = {}, {}
        
        for srcs in db.values():
            if not isinstance(srcs, list): continue
            for src in srcs:
                if not isinstance(src, dict): continue
                b = str(src.get('Bias') or 'Unknown')
                f = str(src.get('Factuality') or 'Unknown')
                bias_tally[b] = bias_tally.get(b, 0) + 1
                fact_tally[f] = fact_tally.get(f, 0) + 1

        md = f"# MBFC Database Statistics\n\n"
        md += f"**Last Synchronized (UTC):** {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}\n"
        md += f"**Total Monitored Sources Indexed:** {total}\n\n"
        
        md += "### Categories Alignment\n| Category | Source Count |\n|---|---|\n"
        for cat, count in sorted(cat_counts.items(), key=lambda i: i[1], reverse=True):
            md += f"| {cat} | {count} |\n"
            
        md += "\n### Master Bias Distribution Top 10\n| Tracked Label | Count |\n|---|---|\n"
        for b, count in sorted(bias_tally.items(), key=lambda i: i[1], reverse=True)[:10]:
            if b != "Unknown": md += f"| {b} | {count} |\n"

        md += "\n### Factuality Evaluation Spectrum Top 10\n| Confidence Rating | Count |\n|---|---|\n"
        for f, count in sorted(fact_tally.items(), key=lambda i: i[1], reverse=True)[:10]:
            if f != "Unknown": md += f"| {f} | {count} |\n"

        with open(STATS_FILE, 'w', encoding='utf-8') as f_out:
            f_out.write(md)
            
    except Exception as e:
        print(f"\n[!] Generating stats Markdown file resulted in an error: {e}", flush=True)

# --- CLEANING TOOLS ---
def clean_string(val):
    if not val: return None
    v = str(val).strip()
    if v.lower() in["", "n/a", "unknown", "unrated", "none", "—"]: return None
    return v

def clean_name(t):
    if not clean_string(t): return None
    return re.sub(r'\s*[-–—]\s*Bias and Credibility.*$', '', t, flags=re.IGNORECASE).strip()

def clean_domain(u):
    u = clean_string(u)
    if not u: return None
    if "." not in u: return u
    if not u.startswith(('http://', 'https://')): u = 'http://' + u
    return f"{urlparse(u).netloc.replace('www.', '')}{urlparse(u).path.rstrip('/')}"

def clean_bias(t):
    t = clean_string(t)
    if not t: return None
    t = re.sub(r'\([0-9.-]+\)', '', t) 
    t = re.sub(r'\bBIAS\b', '', t, flags=re.IGNORECASE)
    t = re.sub(r'-\s+', '-', t) 
    return t.strip().title()

def clean_factuality(t):
    t = clean_string(t)
    if not t: return None
    return re.sub(r'\([0-9.-]+\)', '', t).strip().title()

def clean_metric_standard(t):
    t = clean_string(t)
    if not t: return None
    return re.sub(r'\b(CREDIBILITY|TRAFFIC)\b', '', t, flags=re.IGNORECASE).strip().title()

def clean_freedom(t):
    t = clean_string(t)
    if not t: return None
    match = re.search(r'(\d+/\d+)', t) 
    if match: return f"RSF {match.group(1)}"
    return t.title()

def get_clean_text(html_string):
    """Enforces absolute splitting lines using newlines replacing messy <br> nodes."""
    s = re.sub(r'<br\s*/?>', '\n', html_string, flags=re.IGNORECASE)
    s = re.sub(r'</(p|div|li|h[1-6])>', '\n', s, flags=re.IGNORECASE)
    soup = BeautifulSoup(s, 'html.parser')
    text = soup.get_text(separator=' ').replace('\xa0', ' ')
    text = re.sub(r' {2,}', ' ', text)
    lines =[line.strip() for line in text.split('\n') if line.strip()]
    return '\n'.join(lines)


# --- DATA EXTRACTION TARGETS ---
def extract_source_data(html_content, review_url):
    soup = BeautifulSoup(html_content, 'html.parser')
    
    raw_data = {
        "Name": None, "Review": review_url, "Source": None, "Type": None, 
        "Traffic": None, "Bias": None, "Reasoning": None, "Factuality": None, 
        "Credibility": None, "Freedom": None, "Country": None, "Updated": None,
        "Checked": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ') 
    }

    title_tag = soup.find('h1', class_='entry-title') or soup.find('h1')
    if title_tag: raw_data["Name"] = clean_name(title_tag.get_text(strip=True))

    entry_content = soup.find('div', class_='entry-content')
    if not entry_content:
        return {k: v for k, v in raw_data.items() if v is not None}

    # Extract separated metrics strictly via line replacements guaranteeing string regex matches properly
    text_content = get_clean_text(str(entry_content))

    parsing_patterns = {
        "Bias": (r'Bias Rating:\s*([^\n]+)', clean_bias),
        "Factuality": (r'Factual Reporting:\s*([^\n]+)', clean_factuality),
        "Country": (r'Country:\s*([^\n]+)', clean_string),
        "Freedom": (r'(?:Country Freedom Rating|Freedom Rank|World Press Freedom Rank):\s*([^\n]+)', clean_freedom),
        "Type": (r'Media Type:\s*([^\n]+)', clean_string),
        "Traffic": (r'Traffic/Popularity:\s*([^\n]+)', clean_metric_standard),
        "Credibility": (r'MBFC Credibility Rating:\s*([^\n]+)', clean_metric_standard),
        "Reasoning": (r'(?:Questionable Reasoning|Reasoning):\s*([^\n]+)', clean_string)
    }

    for key, (pattern, cleaner_func) in parsing_patterns.items():
        m = re.search(pattern, text_content, re.IGNORECASE)
        if m: raw_data[key] = cleaner_func(m.group(1))

    src_match = re.search(r'Source:\s*([^\n]+)', text_content, re.IGNORECASE)
    if src_match:
        raw_data["Source"] = clean_domain(src_match.group(1))
    else:
        for p in entry_content.find_all(['p', 'div']):
            if p.get_text(strip=True).lower().startswith("source:"):
                a_tag = p.find('a', href=True)
                if a_tag: raw_data["Source"] = clean_domain(a_tag['href'])
                break

    upd_match = re.search(r'Last Updated on ([a-zA-Z]+ \d{1,2}, \d{4})', text_content, re.IGNORECASE)
    if upd_match: raw_data["Updated"] = upd_match.group(1)

    for img in entry_content.find_all('img'):
        alt = img.get('alt', '').lower()
        if "factual reporting:" in alt and not raw_data.get("Factuality"):
            fm = re.search(r'factual reporting:\s*([^-]+)', alt, re.IGNORECASE)
            if fm: raw_data["Factuality"] = clean_factuality(fm.group(1))
        
        if not raw_data.get("Bias"):
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


# --- MASTER APP ROUTING PROCESS ---
def main():
    global global_db, SHUTDOWN_REQUESTED
    init_files()
    session = get_robust_session()
    global_db = load_db()
    
    scraped_dates_lookup = {
        src['Review']: src.get('Checked', '1970-01-01T00:00:00Z') 
        for srcs in global_db.values() for src in srcs if isinstance(src, dict) and 'Review' in src
    }

    print("Fetching Target Master Lists from Directory...\n", flush=True)
    master_links_lookup = {}  
    pending_tasks = {cat:[] for cat in CATEGORIES.values()}
    
    for url, cat_name in CATEGORIES.items():
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
                        if "class" in element.attrs and "post-modified-info" in element.attrs.get("class",[]): break
                        if collecting: raw_links.extend(element.find_all('a', href=True))
            
            unique_for_cat = set()
            for a in raw_links:
                href = a['href'].strip()
                path_seg = urlparse(href).path.strip('/').split('/')[0] if urlparse(href).path else ""
                if ('mediabiasfactcheck.com' in href or href.startswith('/')) and path_seg not in EXCLUDED_PATHS:
                    unique_for_cat.add(href)
                    master_links_lookup[href] = cat_name
                    
            for link in unique_for_cat:
                if link not in scraped_dates_lookup:
                    pending_tasks[cat_name].append(link)

            in_db_ct = len(unique_for_cat) - len(pending_tasks[cat_name])
            print(f"-> {cat_name.ljust(15)} : {str(len(unique_for_cat)).ljust(4)} Found. {str(in_db_ct).ljust(4)} DB. {str(len(pending_tasks[cat_name])).ljust(4)} New.", flush=True)

        except Exception as e:
            print(f"[ERR] Failed category URL block targeting ({cat_name}): {e}", flush=True)


    total_pending = sum(len(urls) for urls in pending_tasks.values())
    execution_groups = {cat:[] for cat in CATEGORIES.values()}

    # --- ROUTING SYSTEM LOGIC SWITCH (Build Phase OR Check Iteration Phase) ---
    if total_pending > 0:
        print(f"\n[INFO] {total_pending} overall unlogged review links isolated. System launching sequentially.", flush=True)
        # Groups smallest workload to front per exact prior setup 
        for cat, urllist in sorted(pending_tasks.items(), key=lambda i: len(i[1])):
            execution_groups[cat] = urllist

    else:
        BATCH_UPDATE_COUNT = 75
        print(f"\n[INFO] Database matched completely correctly securely aligned fully validating old content check -> Reassessing {BATCH_UPDATE_COUNT} verified records systematically.\n", flush=True)
        active_in_db =[u for u in scraped_dates_lookup.keys() if u in master_links_lookup]
        oldest_ranked = sorted(active_in_db, key=lambda u: scraped_dates_lookup[u])[:BATCH_UPDATE_COUNT]

        for old_href in oldest_ranked:
            execution_groups[master_links_lookup[old_href]].append(old_href)

    urls_processed_this_run = 0

    # Grouped visually strictly inside specific Category headings cleanly.
    for cat_name, urls in execution_groups.items():
        if not urls: continue
        cat_total = len(urls)
        
        print(f"\n--- Category: {cat_name} ({cat_total} mapped iterations inside pool run mode list parameters bounds targeting checks executing seamlessly tracking dynamically flawlessly securely ensuring continuity cleanly tracking tracking) ---", flush=True)

        for idx, href in enumerate(urls, 1):
            if SHUTDOWN_REQUESTED or (time.time() - START_TIME > MAX_RUNTIME_SECONDS):
                 print(f"\n[!] Session timer expired organically properly ending iterations gracefully seamlessly resolving loop...", flush=True)
                 break
                
            try:
                r = session.get(href, timeout=12)
                if r.status_code == 200:
                    
                    for check_cat in list(global_db.keys()):
                        global_db[check_cat] = [s for s in global_db[check_cat] if isinstance(s, dict) and s.get('Review') != href]
                    
                    new_data_pkg = extract_source_data(r.text, href)
                    global_db[cat_name].append(new_data_pkg)

                    p = [f"[{idx}/{cat_total}] [✓] {new_data_pkg.get('Name', 'Null Trace')[:65]}"]
                    
                    if 'Bias' in new_data_pkg: p.append(f"B: {new_data_pkg['Bias']}")
                    if 'Factuality' in new_data_pkg: p.append(f"F: {new_data_pkg['Factuality']}")
                    if 'Credibility' in new_data_pkg: p.append(f"C: {new_data_pkg['Credibility']}")
                    if 'Freedom' in new_data_pkg: p.append(f"FR: {new_data_pkg['Freedom']}")
                    if 'Traffic' in new_data_pkg: p.append(f"T: {new_data_pkg['Traffic']}")
                    if 'Type' in new_data_pkg: p.append(f"Type: {new_data_pkg['Type']}")
                    if 'Country' in new_data_pkg: p.append(f"Country: {new_data_pkg['Country']}")
                    if 'Reasoning' in new_data_pkg: p.append(f"Rsn: {new_data_pkg['Reasoning']}")
                    print(" | ".join(p), flush=True)

                elif r.status_code == 404:
                     print(f"[{idx}/{cat_total}] [!] Dead Root Directory 404 Missing Site Skipping Process", flush=True)
                
                urls_processed_this_run += 1
                if urls_processed_this_run % 15 == 0:
                     save_db(global_db)

            except Exception as e:
                print(f"[{idx}/{cat_total}] [X] Request Network Handshake Failure Fault Drop Process Loop Skip Bypass -> {e}", flush=True)

            time.sleep(1.6)

        # Triggers cleanly break escaping parent nested categories if timeout executes avoiding trailing looping blocks correctly inherently reliably accurately protecting flow boundaries
        if SHUTDOWN_REQUESTED or (time.time() - START_TIME > MAX_RUNTIME_SECONDS): 
             break

    print("\n[OK] Run pipeline loop routine exited processing naturally completely formatting properly safely checking string logs securely generating.", flush=True)
    save_db(global_db)
    generate_statistics(global_db)


if __name__ == "__main__":
    main()

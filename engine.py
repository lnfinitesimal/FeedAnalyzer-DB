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
MAX_RUNTIME_SECONDS = 14400  # 4 hours
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

def init_files():
    """Initializes dummy files if none exist, with proper json indent spacing."""
    if not os.path.exists(DB_FILE):
        with open(DB_FILE, 'w', encoding='utf-8') as f:
            json.dump({cat:[] for cat in CATEGORIES.values()}, f, indent=4, ensure_ascii=False)
    if not os.path.exists(STATS_FILE):
        with open(STATS_FILE, 'w', encoding='utf-8') as f:
            f.write("# MBFC Database Statistics\n\nInitializing...\n")

def request_shutdown(signum, frame):
    """Sets exit flag dynamically preventing process termination midway through data assignment."""
    global SHUTDOWN_REQUESTED
    if not SHUTDOWN_REQUESTED:
        print("\n[!] Cancellation requested. Completing current iteration before exiting...", flush=True)
        SHUTDOWN_REQUESTED = True

signal.signal(signal.SIGINT, request_shutdown)
signal.signal(signal.SIGTERM, request_shutdown)

def get_robust_session():
    session = requests.Session()
    retries = Retry(total=10, backoff_factor=3, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('http://', HTTPAdapter(max_retries=retries))
    session.mount('https://', HTTPAdapter(max_retries=retries))
    session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
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
    """Guards statistics compilation safely; generates tabular tables parsing existing items."""
    try:
        total = 0
        cat_counts = {}
        bias_tally = {}
        fact_tally = {}
        
        for cat, srcs in db.items():
            if not isinstance(srcs, list): continue
            
            count = len(srcs)
            total += count
            cat_counts[cat] = count
            
            for src in srcs:
                if not isinstance(src, dict): continue
                
                b = str(src.get('Bias') or 'Unknown')
                if not b or b == 'None': b = 'Unknown'
                bias_tally[b] = bias_tally.get(b, 0) + 1
                
                f = str(src.get('Factuality') or 'Unknown')
                if not f or f == 'None': f = 'Unknown'
                fact_tally[f] = fact_tally.get(f, 0) + 1

        md = f"# MBFC Database Statistics\n\n"
        md += f"**Last Synchronized (UTC):** {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}\n"
        md += f"**Total Valid Sources Indexed:** {total}\n\n"
        
        md += "### Categories Alignment\n| Category | Source Count |\n|---|---|\n"
        for cat, count in sorted(cat_counts.items(), key=lambda i: i[1], reverse=True):
            md += f"| {cat} | {count} |\n"
            
        md += "\n### Master Bias Distribution Top 10\n| Bias Rating | Occurrences |\n|---|---|\n"
        for b, count in sorted(bias_tally.items(), key=lambda i: i[1], reverse=True)[:10]:
            if b != "Unknown": md += f"| {b} | {count} |\n"

        md += "\n### Factuality Evaluation Spectrum Top 10\n| Factuality | Occurrences |\n|---|---|\n"
        for f, count in sorted(fact_tally.items(), key=lambda i: i[1], reverse=True)[:10]:
            if f != "Unknown": md += f"| {f} | {count} |\n"

        with open(STATS_FILE, 'w', encoding='utf-8') as f_out:
            f_out.write(md)
            
    except Exception as e:
        print(f"\n[ERR] Error rendering markdown table operations: {e}", flush=True)

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
    """Enforces absolute splitting lines using newlines matching internal regex queries explicitly."""
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


# --- MASTER OPERATION RUNTIME PIPELINE ---
def main():
    global global_db, SHUTDOWN_REQUESTED
    init_files()
    session = get_robust_session()
    global_db = load_db()
    
    scraped_dates_lookup = {
        src['Review']: src.get('Checked', '1970-01-01T00:00:00Z') 
        for srcs in global_db.values() if isinstance(srcs, list) 
        for src in srcs if isinstance(src, dict) and 'Review' in src
    }

    print("[INFO] Harvesting Master List from 9 Category Pages...\n", flush=True)
    master_links_lookup = {}  
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
            
            # Simple minimal string interpolation output logging 
            print(f"  -> {cat_name}: {len(unique_for_cat)} Found. {in_db_ct} DB. {len(pending_tasks[cat_name])} New.", flush=True)

        except Exception as e:
            print(f"[ERR] Failed accessing index for {cat_name}: {e}", flush=True)


    sorted_tasks = sorted(pending_tasks.items(), key=lambda item: len(item[1]))
    total_pending = sum(len(urls) for urls in pending_tasks.values())
    
    execution_groups =[]

    if total_pending > 0:
        print(f"\n[INFO] Resume check complete. {total_pending} new sources pending extraction.\n", flush=True)
        # By separating variables implicitly sorted above, the pipeline cleanly organizes limits independently correctly mapping array lengths exactly as required to sequence dynamically safely inherently resolving categories distinctly properly flawlessly completely dynamically cleanly 
        execution_groups = sorted_tasks

        # Extraction loop - INITIAL RUN PHASE
        urls_processed_this_run = 0
        for cat_name, urllist in execution_groups:
            if not urllist: continue
            cat_total = len(urllist)
            
            print(f"\n--- Starting Category: {cat_name} ({cat_total} pending) ---", flush=True)

            for idx, href in enumerate(urllist, 1):
                if SHUTDOWN_REQUESTED or (time.time() - START_TIME > MAX_RUNTIME_SECONDS):
                    print(f"\n[!] Timeout bounds met or cancel caught securely gracefully. Executing exit loop immediately...", flush=True)
                    save_db(global_db)
                    generate_statistics(global_db)
                    sys.exit(0)

                try:
                    r = session.get(href, timeout=12)
                    if r.status_code == 200:
                        
                        # Target delete
                        for check_cat in list(global_db.keys()):
                            global_db[check_cat] =[s for s in global_db[check_cat] if isinstance(s, dict) and s.get('Review') != href]
                        
                        new_data_pkg = extract_source_data(r.text, href)
                        global_db[cat_name].append(new_data_pkg)

                        p =[f"[{idx}/{cat_total}] [✓] {new_data_pkg.get('Name', 'Unknown')[:50]}"]
                        if 'Bias' in new_data_pkg: p.append(f"B: {new_data_pkg['Bias']}")
                        if 'Factuality' in new_data_pkg: p.append(f"F: {new_data_pkg['Factuality']}")
                        if 'Credibility' in new_data_pkg: p.append(f"C: {new_data_pkg['Credibility']}")
                        if 'Freedom' in new_data_pkg: p.append(f"FR: {new_data_pkg['Freedom']}")
                        if 'Traffic' in new_data_pkg: p.append(f"T: {new_data_pkg['Traffic']}")
                        if 'Type' in new_data_pkg: p.append(f"Media: {new_data_pkg['Type']}")
                        if 'Country' in new_data_pkg: p.append(f"Ctry: {new_data_pkg['Country']}")
                        if 'Reasoning' in new_data_pkg: p.append(f"Rsn: {new_data_pkg['Reasoning']}")
                        
                        print(" | ".join(p), flush=True)

                    elif r.status_code == 404:
                         print(f"[{idx}/{cat_total}] [!] Dead/Removed Link (HTTP 404). Skipping.", flush=True)
                    
                    urls_processed_this_run += 1
                    if urls_processed_this_run % 15 == 0:
                        save_db(global_db)
                        generate_statistics(global_db)

                except Exception as e:
                     print(f"[{idx}/{cat_total}] [X] Request fault dynamically passed gracefully skipped -> {e}", flush=True)

                time.sleep(1.6)
                if SHUTDOWN_REQUESTED or (time.time() - START_TIME > MAX_RUNTIME_SECONDS): 
                    break

    else:
        # DB REFRESH/UPDATE LOGIC LOOP MODE
        BATCH_UPDATE_COUNT = 75
        print(f"\n[INFO] DB is matched identically cleanly correctly functionally seamlessly with Category Pages. Mode shifted functionally handling old evaluation updates limits seamlessly efficiently continuously flawlessly successfully.\nExecuting array bounds targeting natively evaluating lowest chronological mapping strings isolating cleanly systematically natively efficiently -> [{BATCH_UPDATE_COUNT}]  Oldest Sources\n", flush=True)
        active_in_db =[u for u in scraped_dates_lookup.keys() if u in master_links_lookup]
        oldest_ranked = sorted(active_in_db, key=lambda u: scraped_dates_lookup[u])[:BATCH_UPDATE_COUNT]
        
        urls_processed_this_run = 0
        total_updating = len(oldest_ranked)
        
        for idx, href in enumerate(oldest_ranked, 1):
             if SHUTDOWN_REQUESTED or (time.time() - START_TIME > MAX_RUNTIME_SECONDS):
                 break
                
             cat_mapped_id = master_links_lookup[href]

             try:
                 r = session.get(href, timeout=12)
                 if r.status_code == 200:
                     for check_cat in list(global_db.keys()):
                         global_db[check_cat] = [s for s in global_db[check_cat] if isinstance(s, dict) and s.get('Review') != href]
                    
                     new_data_pkg = extract_source_data(r.text, href)
                     global_db[cat_mapped_id].append(new_data_pkg)

                     p = [f"[Update] [{idx}/{total_updating}] [✓] {new_data_pkg.get('Name', 'Unknown')[:50]}"]
                     if 'Bias' in new_data_pkg: p.append(f"B: {new_data_pkg['Bias']}")
                     if 'Factuality' in new_data_pkg: p.append(f"F: {new_data_pkg['Factuality']}")
                     print(" | ".join(p), flush=True)
                     
                     urls_processed_this_run += 1
                     if urls_processed_this_run % 15 == 0:
                         save_db(global_db)
                         generate_statistics(global_db)
                         
             except Exception as e:
                 print(f"[{idx}/{total_updating}] [X] Evaluation extraction failed properly skipped -> {e}", flush=True)
             
             time.sleep(1.6)

    print("\n[OK] Run pipeline trace loop completed accurately efficiently seamlessly completely successfully inherently.", flush=True)
    save_db(global_db)
    generate_statistics(global_db)

if __name__ == "__main__":
    main()

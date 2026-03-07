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
import random

# --- CONFIGURATION ---
DB_FILE = 'ratings.json'
STATS_FILE = 'statistics.md'
MAX_RUNTIME_SECONDS = 14400  # 4 hours timeout limit for Actions
START_TIME = time.time()

global_db = {}
master_totals = {}  # FIX: Made global so the emergency shutdown handler can access it
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
    global SHUTDOWN_REQUESTED, global_db, master_totals
    if not SHUTDOWN_REQUESTED:
        print(f"\n[!] Cancellation requested. Emergency saving data to prevent loss...", flush=True)
        SHUTDOWN_REQUESTED = True
        # FIX: Instantly save data upon cancellation to beat GitHub's 7.5 second kill timer
        try:
            save_db(global_db)
            generate_statistics(global_db, master_totals)
        except Exception as e:
            pass

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
    ordered_db = {cat: db.get(cat,[]) for cat in CATEGORIES.values()}
    for category in ordered_db:
        if isinstance(ordered_db[category], list):
            ordered_db[category] = sorted(ordered_db[category], key=lambda x: str(x.get('Name', '')).lower())
            
    # Atomic Save for Database
    temp_file = DB_FILE + '.tmp'
    with open(temp_file, 'w', encoding='utf-8') as f:
        json.dump(ordered_db, f, indent=4, ensure_ascii=False)
    os.replace(temp_file, DB_FILE)

def generate_statistics(db, master_totals=None):
    if master_totals is None:
        master_totals = {}
        
    try:
        total = sum(len(srcs) for srcs in db.values() if isinstance(srcs, list))
        cat_counts = {cat: len(srcs) for cat, srcs in db.items() if isinstance(srcs, list)}
        
        tallies = {
            'Bias': {}, 'Factuality': {}, 'Credibility': {}, 
            'Freedom': {}, 'Type': {}, 'Country': {}
        }
        
        for srcs in db.values():
            if not isinstance(srcs, list): continue
            for src in srcs:
                if not isinstance(src, dict): continue
                for key in tallies.keys():
                    val = str(src.get(key) or 'Unknown')
                    tallies[key][val] = tallies[key].get(val, 0) + 1

        md = f"# MBFC Database Statistics\n\n"
        md += f"**Last Synchronized (UTC):** {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}\n"
        md += f"**Total Monitored Sources Indexed:** {total}\n\n"
        
        # FIX: Added Emojis and dynamically calculate accurate pending URLs based on Master Total expected
        md += "### 🗂️ Categories Alignment\n| Category | In Database | Pending |\n|---|---|---|\n"
        sum_db = 0
        sum_pend = 0
        for cat in CATEGORIES.values():
            db_ct = cat_counts.get(cat, 0)
            total_expected = master_totals.get(cat, 0)
            pend_ct = max(0, total_expected - db_ct)
            md += f"| {cat} | {db_ct} | {pend_ct} |\n"
            sum_db += db_ct
            sum_pend += pend_ct
        md += f"| **Total** | **{sum_db}** | **{sum_pend}** |\n"
            
        def make_table(title, tally_dict, top=10):
            emojis = {
                "Bias": "⚖️", "Factuality": "✅", "Credibility": "⭐",
                "Freedom": "🗽", "Type": "📰", "Country": "🌍"
            }
            emoji = emojis.get(title, "📊")
            res = f"\n### {emoji} {title} Distribution\n| {title} | Count |\n|---|---|\n"
            
            sorted_items = sorted(tally_dict.items(), key=lambda i: i[1], reverse=True)
            count = 0
            for k, v in sorted_items:
                if k != "Unknown":
                    res += f"| {k} | {v} |\n"
                    count += 1
                    if count >= top: break
            return res

        md += make_table("Bias", tallies['Bias'])
        md += make_table("Factuality", tallies['Factuality'])
        md += make_table("Credibility", tallies['Credibility'])
        md += make_table("Freedom", tallies['Freedom'])
        md += make_table("Type", tallies['Type'])
        md += make_table("Country", tallies['Country'], top=15)

        # Atomic Save for Statistics MD
        temp_stats = STATS_FILE + '.tmp'
        with open(temp_stats, 'w', encoding='utf-8') as f_out:
            f_out.write(md)
        os.replace(temp_stats, STATS_FILE)
            
    except Exception as e:
        print(f"\n[!] Error generating stats Markdown file: {e}", flush=True)

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
        return raw_data 

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

    return raw_data


# --- MASTER APP ROUTING PROCESS ---
def main():
    global global_db, SHUTDOWN_REQUESTED, master_totals
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
    master_totals = {cat: 0 for cat in CATEGORIES.values()}  # FIX: Stores true grand total of expected sources
    
    try:
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
                        
                master_totals[cat_name] = len(unique_for_cat)  # Log true expected count
                
                # ALPHABETICAL FIX: sorted() restores alphabetical order naturally!
                for link in sorted(unique_for_cat):
                    if link not in scraped_dates_lookup:
                        pending_tasks[cat_name].append(link)

                in_db_ct = len(unique_for_cat) - len(pending_tasks[cat_name])
                print(f"• {cat_name}: {len(unique_for_cat)} found. {in_db_ct} DB. {len(pending_tasks[cat_name])} New.", flush=True)

            except Exception as e:
                print(f"[ERR] Failed fetching category ({cat_name}): {e}", flush=True)

        total_pending = sum(len(urls) for urls in pending_tasks.values())
        execution_groups = {}

        if total_pending > 0:
            print(f"\n[INFO] {total_pending} new links found. Starting extraction.", flush=True)
            pending_filtered = {k: v for k, v in pending_tasks.items() if len(v) > 0}
            for cat, urllist in sorted(pending_filtered.items(), key=lambda i: len(i[1])):
                execution_groups[cat] = urllist
        else:
            BATCH_UPDATE_COUNT = 400
            print(f"\n[INFO] Database is up to date. Reassessing the {BATCH_UPDATE_COUNT} oldest records.\n", flush=True)
            active_in_db =[u for u in scraped_dates_lookup.keys() if u in master_links_lookup]
            
            # ALPHABETICAL TIE-BREAKER: Sorts by Date first, then falls back to Alphabetical URL to prevent randomness
            oldest_ranked = sorted(active_in_db, key=lambda u: (scraped_dates_lookup[u], u))[:BATCH_UPDATE_COUNT]

            for cat in CATEGORIES.values():
                execution_groups[cat] =[]
            for old_href in oldest_ranked:
                execution_groups[master_links_lookup[old_href]].append(old_href)

        urls_processed_this_run = 0

        for cat_name, urls in execution_groups.items():
            if not urls: continue
            cat_total = len(urls)
            
            print(f"\n--- Category: {cat_name} ({cat_total} entries) ---", flush=True)

            for idx, href in enumerate(urls, 1):
                if SHUTDOWN_REQUESTED or (time.time() - START_TIME > MAX_RUNTIME_SECONDS):
                     print(f"\n[!] Session ending organically. Saving progress...", flush=True)
                     break
                    
                try:
                    r = session.get(href, timeout=6)
                    if r.status_code == 200:
                        for check_cat in list(global_db.keys()):
                            global_db[check_cat] =[s for s in global_db[check_cat] if isinstance(s, dict) and s.get('Review') != href]
                        
                        new_data_pkg = extract_source_data(r.text, href)
                        global_db[cat_name].append(new_data_pkg)

                        p =[f"[{idx}/{cat_total}] [✓] {new_data_pkg.get('Name', 'Null Trace')[:65]}"]
                        
                        if new_data_pkg.get('Bias'): p.append(f"B: {new_data_pkg['Bias']}")
                        if new_data_pkg.get('Factuality'): p.append(f"F: {new_data_pkg['Factuality']}")
                        if new_data_pkg.get('Credibility'): p.append(f"C: {new_data_pkg['Credibility']}")
                        if new_data_pkg.get('Freedom'): p.append(f"FR: {new_data_pkg['Freedom']}")
                        if new_data_pkg.get('Traffic'): p.append(f"T: {new_data_pkg['Traffic']}")
                        if new_data_pkg.get('Type'): p.append(f"Type: {new_data_pkg['Type']}")
                        if new_data_pkg.get('Country'): p.append(f"Country: {new_data_pkg['Country']}")
                        if new_data_pkg.get('Reasoning'): p.append(f"Rsn: {new_data_pkg['Reasoning']}")
                        
                        print(" | ".join(p), flush=True)

                    elif r.status_code == 404:
                         print(f"[{idx}/{cat_total}] [!] 404 Not Found. Skipping.", flush=True)
                    
                    urls_processed_this_run += 1
                    if urls_processed_this_run % 15 == 0:
                         save_db(global_db)
                         # FIX 1: Generate stats concurrent with DB save, passing master_totals
                         generate_statistics(global_db, master_totals)

                    # --- PRO-LEVEL EVASION: "The Coffee Break" ---
                    if urls_processed_this_run % 250 == 0:
                         cooldown = random.uniform(45.0, 75.0)
                         print(f"\n[~] Anti-Bot Cooldown: Pausing for {int(cooldown)} seconds...", flush=True)
                         time.sleep(cooldown)

                except Exception as e:
                    print(f"[{idx}/{cat_total}][X] Network Error: {e}", flush=True)

                # FIX 2: Moved up to check for shutdown BEFORE sleeping to beat GitHub's Kill Timer
                if SHUTDOWN_REQUESTED or (time.time() - START_TIME > MAX_RUNTIME_SECONDS): 
                     break

                # HUMAN JITTER: Randomized sleep interval to look completely human to Cloudflare
                time.sleep(random.uniform(1.3, 2.4))

    except KeyboardInterrupt:
        print("\n[!] Process interrupted by user.", flush=True)
    except Exception as e:
        print(f"\n[!] Unexpected error during execution: {e}", flush=True)
        traceback.print_exc()
    finally:
        print("\n[OK] Processing complete. Saving database and generating statistics.", flush=True)
        save_db(global_db)
        generate_statistics(global_db, master_totals)

if __name__ == "__main__":
    main()

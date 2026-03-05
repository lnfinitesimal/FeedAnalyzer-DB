from curl_cffi import requests
from bs4 import BeautifulSoup
import json
import time
import random
import re
import os
import sys
from urllib.parse import urlparse

DB_FILE = 'ratings.json'
MD_FILE = 'statistics.md'

TARGET_ENDPOINTS =[
    "https://mediabiasfactcheck.com/left/",
    "https://mediabiasfactcheck.com/leftcenter/",
    "https://mediabiasfactcheck.com/center/",
    "https://mediabiasfactcheck.com/right-center/",
    "https://mediabiasfactcheck.com/right/",
    "https://mediabiasfactcheck.com/pro-science/",
    "https://mediabiasfactcheck.com/fake-news/",
    "https://mediabiasfactcheck.com/conspiracy/",
    "https://mediabiasfactcheck.com/satire/"
]

IGNORE_PATHS = {
    "", "about", "contact", "methodology", "donate", "privacy", "terms-and-conditions", 
    "faq", "badges", "membership-account", "filter-options", "submit-fact-check",
    "daily-source-bias-check", "podcast", "search", "cookie-policy", "staff-and-writers"
}

# 🛡️ CLOUDFLARE BYPASS: Perfectly mimics Google Chrome v120 TLS Fingerprint. 
# Cloudflare will not drop the connection anymore.
session = requests.Session(impersonate="chrome120")

def get_root_domain(url_string):
    try:
        return urlparse(url_string).netloc.replace('www.', '').lower()
    except:
        return None

def normalize_data(value, is_country=False, is_bias=False):
    if not value: return None
    v = value.strip()
    v_upper = v.upper()
    if v_upper in["", "UNKNOWN", "N/A", "NONE", "UNRATED"]: return None
        
    if is_country:
        def fix_country_word(w):
            w = w.strip()
            w_up = w.upper()
            if w_up in["US", "U.S.", "USA", "U.S.A.", "UNITED STATES", "UNITED STATES OF AMERICA"]: return "USA"
            if w_up in["UK", "U.K.", "UNITED KINGDOM", "GREAT BRITAIN"]: return "UK"
            if w_up in ["UAE", "EU"]: return w_up
            return w.title() if len(w) > 3 else w_up
        if ',' in v: return ', '.join([fix_country_word(c) for c in v.split(',')])
        return fix_country_word(v)
        
    if is_bias:
        if "SATIRE" in v_upper: return "Satire"
        if "PRO-SCIENCE" in v_upper or "SCIENCE" in v_upper: return "Pro-Science"
        if "CONSPIRACY" in v_upper or "PSEUDOSCIENCE" in v_upper: return "Conspiracy"
        if "QUESTIONABLE" in v_upper or "FAKE NEWS" in v_upper: return "Questionable"
    return v.title()

def extract_source_domain(soup):
    source_tags = soup.find_all(string=re.compile(r'Source:\s*', re.IGNORECASE))
    for tag in source_tags:
        parent = tag.parent
        link = parent.find_next('a')
        if link and link.get('href'):
            href = link.get('href')
            if not any(x in href.lower() for x in['mediabiasfactcheck', 'twitter.com', 'facebook.com', 'wikipedia.org', 'patreon.com']):
                domain = get_root_domain(href)
                if domain and len(domain) > 3: return domain
    return None

def save_database(feed_analytics):
    with open(DB_FILE, 'w', encoding='utf-8') as f:
        json.dump(feed_analytics, f, separators=(',', ':'))
        
    b_counts, f_counts = {}, {}
    for metrics in feed_analytics.values():
        b = metrics.get('b', 'Unrated / None')
        f = metrics.get('f', 'Unrated / None')
        b_counts[b] = b_counts.get(b, 0) + 1
        f_counts[f] = f_counts.get(f, 0) + 1

    md = f"# 📊 Feed Ratings Statistics\n\n**Total Sources:** `{len(feed_analytics)}`\n\n"
    md += "### ⚖️ Bias Distribution\n| Bias Category | Count |\n| :--- | :--- |\n"
    for k, v in sorted(b_counts.items(), key=lambda item: item[1], reverse=True): md += f"| {k} | **{v}** |\n"
    md += "\n### ✅ Factuality Distribution\n| Factuality Rating | Count |\n| :--- | :--- |\n"
    for k, v in sorted(f_counts.items(), key=lambda item: item[1], reverse=True): md += f"| {k} | **{v}** |\n"

    with open(MD_FILE, 'w', encoding='utf-8') as f: f.write(md)

def main():
    feed_analytics = {}
    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, 'r', encoding='utf-8') as f: feed_analytics = json.load(f)
            print(f"[+] Loaded existing database: {len(feed_analytics)} entries.")
        except: pass

    # Failsafe: Create the DB file immediately so Git never crashes
    save_database(feed_analytics)

    print("\n=== [1] HARVESTING ALL TARGET URLS ===")
    article_urls_to_scan = set()
    for endpoint in TARGET_ENDPOINTS:
        success = False
        
        for attempt in range(3):
            try:
                time.sleep(random.uniform(1.0, 2.5))
                res = session.get(endpoint, timeout=15)
                if res.status_code in [403, 429]:
                    print(f"  [!] Rate limit on {endpoint}. Cooldown 3 mins... ({attempt+1}/3)")
                    time.sleep(180)
                    continue
                if res.status_code == 200:
                    success = True
                    break
            except Exception as e:
                pass
                
        if not success:
            print(f"  [X] Failed to harvest {endpoint}. Block active.")
            continue
            
        try:
            soup = BeautifulSoup(res.text, 'html.parser')
            content_area = soup.find('div', class_='entry-content') or soup.find('table', id='mbfc-table')
            if not content_area: 
                continue
                
            for link in content_area.find_all('a', href=True):
                href = link.get('href').strip().rstrip('/')
                if href.startswith("https://mediabiasfactcheck.com/"):
                    path = urlparse(href).path.strip('/')
                    if not path or path in IGNORE_PATHS or any(c.strip('/') in href for c in TARGET_ENDPOINTS): continue
                    article_urls_to_scan.add(href)
        except Exception as e:
            print(f"  [!] Error parsing {endpoint}: {e}")

    urls_to_process = list(article_urls_to_scan)
    total = len(urls_to_process)
    print(f"\n=== [2] FULL SYNC: ANALYZING ALL {total} SOURCES ===")
    if total == 0: 
        print("[✓] 0 sources to process. Exiting gracefully.")
        return

    for index, article_url in enumerate(urls_to_process, start=1):
        success = False
        
        for attempt in range(3):
            try:
                time.sleep(random.uniform(0.3, 0.7))
                res = session.get(article_url, timeout=10)
                if res.status_code in[403, 429]:
                    print(f"\n🚨 [WARNING] Cloudflare Block (Status {res.status_code}).")
                    print(f"⏳ Initiating perfect cooldown (3 minutes) before auto-resuming... (Attempt {attempt+1}/3)")
                    time.sleep(180) 
                    continue
                if res.status_code != 200: break
                success = True
                break
            except Exception:
                pass
                
        if not success:
            print(f"[{index}/{total}] [X] Failed/Skipped: {article_url}")
            continue

        soup = BeautifulSoup(res.text, 'html.parser')
        domain = extract_source_domain(soup)
        if not domain: continue
            
        clean_text = re.sub(r'\s+', ' ', soup.get_text(separator=' '))
        
        # --- PARAGRAPH BLOCKER LOGIC ---
        stop_keywords = r"(?:Bias Rating|Bias|Factual Reporting|Factuality Rating|Factuality|MBFC Credibility Rating|Credibility Rating|Credibility|Country Freedom Rating|Country Freedom|Press Freedom Rating|Press Freedom|Freedom Rating|Media Type|Traffic|World Press|$)"
        
        def pull_metric(kw):
            # 1. Matches only if there is a COLON
            # 2. Stops if it hits a sentence-ending period (e.g. '. T') or a dash (' - ')
            match = re.search(rf"{kw}\s*:\s*(.*?)(?=\s*(?:{stop_keywords})|\.\s+(?=[A-Z])|\s-\s|$)", clean_text, re.IGNORECASE)
            if match:
                val = match.group(1).replace('\u00a0', ' ').strip().rstrip('.-')
                # 3. Ultimate Failsafe: Ratings are never longer than 40 chars. 
                if len(val) <= 40:
                    return val
            return None

        current_time = int(time.time())
        new_data = {"u": article_url, "chk": current_time}
        
        b = normalize_data(pull_metric(r"(?:Bias Rating|Bias)"), is_bias=True)
        f = normalize_data(pull_metric(r"(?:Factual Reporting|Factuality Rating|Factuality)"))
        c = normalize_data(pull_metric(r"(?:MBFC Credibility Rating|Credibility Rating|Credibility)"))
        p = normalize_data(pull_metric(r"(?:Country Freedom Rating|Country Freedom|Press Freedom Rating|Press Freedom|Freedom Rating)"))
        o = normalize_data(pull_metric(r"Country(?!\s+Freedom)"), is_country=True)

        if b: new_data["b"] = b
        if f: new_data["f"] = f
        if c: new_data["c"] = c
        if p: new_data["p"] = p
        if o: new_data["o"] = o

        if domain in feed_analytics:
            old_data = feed_analytics[domain]
            has_changed = any(old_data.get(k) != new_data.get(k) for k in ['b', 'f', 'c', 'p', 'o'])
            
            if has_changed:
                feed_analytics[domain] = new_data
                print(f"  [{index}/{total}] [~] UPDATED: {domain} | Bias: {b} | Fact: {f}")
            else:
                feed_analytics[domain]["chk"] = current_time 
                print(f"  [{index}/{total}] [-] NO CHANGE: {domain}")
        else:
            feed_analytics[domain] = new_data
            print(f"[{index}/{total}] [+] NEW: {domain} | Bias: {b} | Fact: {f}")

        if index % 50 == 0: save_database(feed_analytics)

    print("\n===[3] FINALIZING DATABASE ===")
    save_database(feed_analytics)
    print(f"[✓] Database successfully synced! ({len(feed_analytics)} sources total)")

if __name__ == "__main__":
    main()

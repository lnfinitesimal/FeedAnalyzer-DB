from curl_cffi import requests
from bs4 import BeautifulSoup
import json, time, random, re, os, tempfile
from urllib.parse import urlparse

# ── Configuration ───────────────────────────────────────────────────────────
DB_FILE    = "ratings.json"
MD_FILE    = "statistics.md"
HOMEPAGE   = "https://mediabiasfactcheck.com/"
RECHECK    = 7 * 86400  # 7 days
MAX_PER_RUN = int(os.environ.get("MAX_PER_RUN", 150))

TARGET_ENDPOINTS = {
    "https://mediabiasfactcheck.com/left/": "Left",
    "https://mediabiasfactcheck.com/leftcenter/": "Left-Center",
    "https://mediabiasfactcheck.com/center/": "Least Biased",
    "https://mediabiasfactcheck.com/right-center/": "Right-Center",
    "https://mediabiasfactcheck.com/right/": "Right",
    "https://mediabiasfactcheck.com/pro-science/": "Pro-Science",
    "https://mediabiasfactcheck.com/fake-news/": "Questionable",
    "https://mediabiasfactcheck.com/conspiracy/": "Conspiracy",
    "https://mediabiasfactcheck.com/satire/": "Satire",
}

TARGET_SLUGS = {urlparse(u).path.strip("/") for u in TARGET_ENDPOINTS}

IGNORE_PATHS = {
    "", "about", "contact", "methodology", "donate", "privacy",
    "terms-and-conditions", "faq", "badges", "membership-account",
    "filter-options", "submit-fact-check", "daily-source-bias-check",
    "podcast", "search", "cookie-policy", "staff-and-writers",
    "membership", "login", "register", "tag", "category", "author", "page"
}

_DOMAIN_BLACKLIST = {
    "mediabiasfactcheck.com", "twitter.com", "facebook.com",
    "wikipedia.org", "patreon.com", "x.com", "instagram.com",
    "youtube.com", "linkedin.com", "reddit.com", "tiktok.com", 
    "threads.net", "archive.org", "archive.is", "archive.ph"
}

VALID_FACTUALITY = {"VERY HIGH", "HIGH", "MOSTLY FACTUAL", "MIXED", "LOW", "VERY LOW"}
VALID_CREDIBILITY = {"HIGH CREDIBILITY", "MEDIUM CREDIBILITY", "LOW CREDIBILITY"}
VALID_FREEDOM = {"MOSTLY FREE", "PARTLY FREE", "NOT FREE"}

COUNTRY_NORMALIZE = {
    "REPUBLIC OF KOREA": "South Korea",
    "THE NETHERLANDS": "Netherlands",
    "CZECHIA": "Czech Republic",
    "RUSSIAN FEDERATION": "Russia",
    "DEMOCRATIC PEOPLE'S REPUBLIC OF KOREA": "North Korea",
    "UAE": "United Arab Emirates",
    "ROC": "Taiwan",
    "REPUBLIC OF CHINA": "Taiwan"
}

# ── HTTP Client ─────────────────────────────────────────────────────────────
class HTTPClient:
    def __init__(self):
        profile = random.choice(["chrome120", "chrome124"])
        self.session = requests.Session(impersonate=profile)
        self.request_count = 0
        self.penalty_multiplier = 1.0
        self.next_rest = random.randint(30, 40)
        print(f"[*] TLS fingerprint: {profile}")

    def _delay(self, kind):
        if kind == "listing":
            base = random.uniform(18, 24)
        else:
            # FIX: 14-18s mathematically guarantees we stay under 5 req/min
            base = random.uniform(14, 18)
        time.sleep(base * self.penalty_multiplier)

    def get(self, url, *, kind="page", attempts=3):
        for attempt in range(attempts):
            self._delay(kind)
            
            if self.request_count > 0 and self.request_count >= self.next_rest:
                rest = random.uniform(45, 60)
                print(f"  [zZz] Organic rest break for {rest:.1f}s...")
                time.sleep(rest)
                self.next_rest = self.request_count + random.randint(30, 40)

            try:
                res = self.session.get(url, timeout=20, headers={"Referer": HOMEPAGE})
                self.request_count += 1

                if res.status_code == 200:
                    if self.penalty_multiplier > 1.0:
                        self.penalty_multiplier = max(1.0, self.penalty_multiplier - 0.2)
                    return res

                if res.status_code in (403, 429, 503):
                    self.penalty_multiplier = min(3.0, self.penalty_multiplier + 0.5) 
                    wait = random.uniform(60, 90) * self.penalty_multiplier
                    print(f"  [!] HTTP {res.status_code} → Penalty triggered. Cooldown {wait:.1f}s")
                    time.sleep(wait)
                    continue

                return None 
            except Exception as exc:
                print(f"  [!] Network error: {exc}")
                time.sleep(10)
        return None

    def warmup(self):
        print("[*] Warming up session (homepage visit)…")
        if self.get(HOMEPAGE, kind="listing"):
            print("[✓] Session cookies established.\n")
            return True
        return False

http = HTTPClient()

# ── Extraction Helpers ──────────────────────────────────────────────────────
def root_domain(url_str):
    try:
        return urlparse(url_str).netloc.replace("www.", "").lower()
    except Exception:
        return None

def clean_value(val):
    if not val: return None
    val = re.sub(r"\s*\([\d.]+\)", "", val)
    return val.strip(" .-").upper()

def truncate_dom(soup):
    raw_content = soup.find("div", class_="entry-content")
    if not raw_content: return None

    content = BeautifulSoup(str(raw_content), "html.parser")

    for tag in content.find_all(["h2", "h3", "h4", "h5"]):
        text = tag.get_text(strip=True).lower()
        if any(stop in text for stop in["detailed report", "analysis / bias", "see also", "related sources", "latest ratings", "failed fact checks"]):
            for sibling in tag.find_all_next():
                sibling.extract()
            tag.extract()
            break
    return content

def extract_metric(text, pattern, whitelist=None):
    match = re.search(pattern, text, re.I)
    if match:
        val = clean_value(match.group(1))
        if not val: return None
        if whitelist:
            return val.title() if val in whitelist else None
        return val.title() if len(val) <= 40 else None
    return None

def scrape_metrics(soup):
    content = truncate_dom(soup)
    if not content:
        return {}

    text = content.get_text(separator="\n", strip=True)

    metrics = {
        "f": extract_metric(text, r"(?:Factual Reporting|Factuality Rating|Factuality|Factual Report)\s*[:\-–—]\s*([^\n]+)", VALID_FACTUALITY),
        "c": extract_metric(text, r"(?:MBFC'?s?\s+Credibility\s+Rating|Credibility\s+Rating)\s*[:\-–—]\s*([^\n]+)", VALID_CREDIBILITY),
        "p": extract_metric(text, r"(?:Country Freedom Rating|Press Freedom Rating|Freedom of the Press Rating|Freedom Rating|Press Freedom)\s*[:\-–—]\s*([^\n]+)", VALID_FREEDOM),
        "o": extract_metric(text, r"(?:Country|Based in|Location)\s*[:\-–—]\s*([^\n(,]+)")
    }

    if not metrics["f"] or not metrics["c"]:
        for img in content.find_all("img"):
            alt = (img.get("alt") or "").upper().strip()
            if not metrics["f"]:
                for v in sorted(VALID_FACTUALITY, key=len, reverse=True):
                    if v in alt:
                        metrics["f"] = v.title()
                        break
            if not metrics["c"]:
                for v in sorted(VALID_CREDIBILITY, key=len, reverse=True):
                    if v in alt:
                        metrics["c"] = v.title()
                        break

    if metrics["o"]:
        wu = metrics["o"].upper()
        if re.search(r'\b(US|USA|UNITED STATES|UNITED STATES OF AMERICA)\b', wu) or re.search(r'(?<!\w)U\.S\.A?\.?(?!\w)', wu):
            metrics["o"] = "USA"
        elif re.search(r'\b(UK|UNITED KINGDOM|GREAT BRITAIN)\b', wu) or re.search(r'(?<!\w)U\.K\.?(?!\w)', wu):
            metrics["o"] = "UK"
        else:
            cosmetic_fix = metrics["o"].title().replace(" And ", " and ").replace(" Of ", " of ")
            metrics["o"] = COUNTRY_NORMALIZE.get(wu, cosmetic_fix)

    return {k: v for k, v in metrics.items() if v}

def extract_source_domain(soup):
    content = soup.find("div", class_="entry-content")
    if not content: return None
    
    for tag in content.find_all(string=re.compile(r"Sources?\s*(?:URL)?\s*:", re.I)):
        link = tag.parent.find_next("a")
        if link and link.get("href"):
            dom = root_domain(link["href"])
            if dom and dom not in _DOMAIN_BLACKLIST and len(dom) > 3:
                return dom
                    
    for a in content.find_all("a", href=True, limit=5):
        dom = root_domain(a["href"])
        if dom and dom not in _DOMAIN_BLACKLIST and len(dom) > 3:
            print(f"    [⚠] Fallback domain used: {dom}")
            return dom

    return None

# ── Pipeline & Database ─────────────────────────────────────────────────────
def load_database():
    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception: pass
    return {}

def save_database(db):
    # 1. Atomic write for JSON
    fd, tmp_path = tempfile.mkstemp(dir=".", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(db, f, separators=(",", ":"))
        os.replace(tmp_path, DB_FILE)
    except Exception:
        os.unlink(tmp_path)
        raise
    
    # Generate MD Stats
    bias_counts, fact_counts = {}, {}
    valid_entries = 0
    for key, entry in db.items():
        if key.startswith("_fail:"): continue
        valid_entries += 1
        b = entry.get("b", "Unrated")
        f = entry.get("f", "Unrated")
        bias_counts[b] = bias_counts.get(b, 0) + 1
        fact_counts[f] = fact_counts.get(f, 0) + 1

    md = f"# 📊 Feed Ratings Statistics\n\n**Total Sources:** `{valid_entries}`\n\n"
    md += "### ⚖️ Bias Distribution\n| Bias Category | Count |\n| :--- | :--- |\n"
    for k, v in sorted(bias_counts.items(), key=lambda x: x[1], reverse=True): md += f"| {k} | **{v}** |\n"
    md += "\n### ✅ Factuality Distribution\n| Factuality Rating | Count |\n| :--- | :--- |\n"
    for k, v in sorted(fact_counts.items(), key=lambda x: x[1], reverse=True): md += f"| {k} | **{v}** |\n"
    
    # FIX: Atomic write for Markdown too
    fd2, tmp_md = tempfile.mkstemp(dir=".", suffix=".tmp")
    try:
        with os.fdopen(fd2, "w", encoding="utf-8") as f:
            f.write(md)
        os.replace(tmp_md, MD_FILE)
    except Exception:
        os.unlink(tmp_md)
        raise

def harvest_urls(db):
    print("\n=== PHASE 1 · HARVESTING SOURCE URLS ===")
    url_bias_map = {}
    endpoints = list(TARGET_ENDPOINTS.keys())
    random.shuffle(endpoints)

    # FIX: Bootstrap Mode. If DB is empty, harvest ALL categories to build the index immediately.
    if not db:
        print("[*] Bootstrap mode: DB is empty. Harvesting ALL categories to build index.")
        endpoints_to_check = endpoints
    else:
        endpoints_to_check = endpoints[:2]

    for endpoint in endpoints_to_check:
        res = http.get(endpoint, kind="listing")
        if not res: continue

        soup = BeautifulSoup(res.text, "html.parser")
        content = truncate_dom(soup) or soup.find("table", id="mbfc-table")
        if not content: continue

        assigned_bias = TARGET_ENDPOINTS[endpoint]
        count = 0
        
        for link in content.find_all("a", href=True):
            href = link["href"].strip().rstrip("/")
            if not href.startswith("https://mediabiasfactcheck.com/"): continue
            
            path_parts =[p for p in urlparse(href).path.strip("/").split("/") if p]
            if len(path_parts) != 1: continue
            
            path = path_parts[0]
            if path not in IGNORE_PATHS and path not in TARGET_SLUGS:
                url_bias_map[href] = assigned_bias
                count += 1

        print(f"[✓] {assigned_bias:20s} → {count} sources")

    return url_bias_map

def process_sources(db, url_bias_map):
    now = int(time.time())
    
    url_to_domain = {}
    for domain, entry in db.items():
        if domain.startswith("_fail:"): continue
        mbfc_url = entry.get("u")
        if mbfc_url:
            url_to_domain[mbfc_url] = domain
            if mbfc_url not in url_bias_map and "b" in entry:
                url_bias_map[mbfc_url] = entry["b"]

    def last_checked(mbfc_url):
        fail_key = f"_fail:{mbfc_url}"
        if fail_key in db:
            return db[fail_key].get("chk", 0)
        known_domain = url_to_domain.get(mbfc_url)
        return db[known_domain].get("chk", 0) if known_domain else 0
    
    todo_urls =[u for u in url_bias_map.keys() 
                 if last_checked(u) <= now - RECHECK 
                 and db.get(f"_fail:{u}", {}).get("fails", 0) < 3]
    
    todo_urls.sort(key=last_checked)

    if MAX_PER_RUN:
        todo_urls = todo_urls[:MAX_PER_RUN]

    total = len(todo_urls)
    skipped = len(url_bias_map) - total
    print(f"\n=== PHASE 2 · PROCESSING {total} SOURCES ({skipped} skipped) ===")

    if total == 0: return 0, 0

    new_count = updated_count = 0

    for i, url in enumerate(todo_urls, 1):
        
        # FIX: Circuit Breaker! Abort gracefully if Cloudflare puts us in timeout jail.
        if http.penalty_multiplier >= 2.5:
            print(f"\n[!] Circuit breaker tripped at {i}/{total}. Cloudflare is too aggressive. Saving and exiting gracefully.")
            break

        res = http.get(url)
        if not res:
            print(f"[{i}/{total}] [✗] {url}")
            continue

        soup = BeautifulSoup(res.text, "html.parser")
        domain = extract_source_domain(soup)
        
        fail_key = f"_fail:{url}"
        if not domain: 
            db.setdefault(fail_key, {"chk": 0, "fails": 0})
            db[fail_key]["fails"] += 1
            db[fail_key]["chk"] = now
            status = "[☠] Dead/Permanently Skipped" if db[fail_key]["fails"] >= 3 else "[?] Skipped (no domain)"
            print(f"[{i}/{total}] {status}: {url}")
            if i % 25 == 0: save_database(db)
            continue
        else:
            if fail_key in db:
                del db[fail_key]

        met = scrape_metrics(soup)
        
        entry = {"u": url, "chk": now, "b": url_bias_map[url]}
        entry.update(met)

        if domain in db:
            old = db[domain]
            data_changed = any(old.get(k) != entry.get(k) for k in ("b", "f", "c", "p", "o"))
            if data_changed:
                db[domain] = entry
                updated_count += 1
                print(f"[{i}/{total}] [~] UPDATED: {domain} | B: {entry.get('b')} | F: {entry.get('f')} | C: {entry.get('c')} | P: {entry.get('p')} | O: {entry.get('o')}")
            else:
                db[domain]["chk"] = now 
                print(f"[{i}/{total}] [-] {domain} (Unchanged)")
        else:
            db[domain] = entry
            new_count += 1
            print(f"[{i}/{total}] [+] NEW: {domain} | B: {entry.get('b')} | F: {entry.get('f')} | C: {entry.get('c')} | P: {entry.get('p')} | O: {entry.get('o')}")

        if i % 25 == 0: save_database(db)

    return new_count, updated_count

def main():
    db = load_database()
    save_database(db)

    if not http.warmup(): return

    # FIX: Pass the DB to harvest_urls so it can check if we need to Bootstrap
    url_bias_map = harvest_urls(db)
    if not url_bias_map: return
    
    print(f"\n[*] Total harvested URLs: {len(url_bias_map)}")
    if len(url_bias_map) < 500:
        print("[!] WARNING: Harvest suspiciously low. MBFC may have enabled strict JS rendering.")

    new_count, updated_count = process_sources(db, url_bias_map)

    print("\n=== PHASE 3 · FINALIZING ===")
    save_database(db)
    valid_entries = sum(1 for k in db.keys() if not k.startswith("_fail:"))
    print(f"[✓] Complete — {valid_entries} total valid sources | +{new_count} new | ~{updated_count} updated | {http.request_count} requests")

if __name__ == "__main__":
    main()

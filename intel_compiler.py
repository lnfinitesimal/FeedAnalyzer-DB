from curl_cffi import requests
from bs4 import BeautifulSoup
import json, time, random, re, os, tempfile
from urllib.parse import urlparse

# ── Configuration ───────────────────────────────────────────────────────────
DB_FILE     = "ratings.json"
MD_FILE     = "statistics.md"
HOMEPAGE    = "https://mediabiasfactcheck.com/"
RECHECK     = 30 * 86400
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
    "membership", "login", "register", "tag", "category", "author", "page", 
    "support-media-bias-fact-check", "left-vs-right-bias-how-we-rate-the-bias-of-media-sources",
}

_DOMAIN_BLACKLIST = {
    "mediabiasfactcheck.com", "twitter.com", "facebook.com",
    "patreon.com", "x.com", "instagram.com",
    "youtube.com", "linkedin.com", "reddit.com", "tiktok.com",
    "threads.net", "archive.org", "archive.is", "archive.ph",
    "wp.com", "wordpress.com", "gravatar.com",
    "goo.gl", "bit.ly", "tinyurl.com", "amzn.to",
    "apple.com", "play.google.com", "apps.apple.com",
}

VALID_FACTUALITY  = {"VERY HIGH", "HIGH", "MOSTLY FACTUAL", "MIXED", "LOW", "VERY LOW"}
VALID_CREDIBILITY = {"HIGH CREDIBILITY", "MEDIUM CREDIBILITY", "LOW CREDIBILITY"}
VALID_FREEDOM     = {
    "EXCELLENT FREEDOM",
    "MOSTLY FREE",
    "MODERATE FREEDOM",
    "LIMITED FREEDOM",
    "TOTAL OPPRESSION",
}

COUNTRY_NORMALIZE = {
    "REPUBLIC OF KOREA": "South Korea",
    "THE NETHERLANDS": "Netherlands",
    "CZECHIA": "Czech Republic",
    "RUSSIAN FEDERATION": "Russia",
    "DEMOCRATIC PEOPLE'S REPUBLIC OF KOREA": "North Korea",
    "UAE": "United Arab Emirates",
    "ROC": "Taiwan",
    "REPUBLIC OF CHINA": "Taiwan",
}

COUNTRY_DISCARD = {"UNKNOWN", "N/A", "NA", "NONE", "TBD", "VARIOUS", "MULTIPLE"}

# ── Helpers ─────────────────────────────────────────────────────────────────
def _is_blacklisted(dom):
    if not dom:
        return True
    for bl in _DOMAIN_BLACKLIST:
        if dom == bl or dom.endswith("." + bl):
            return True
    return False

# ── HTTP Client ─────────────────────────────────────────────────────────────
class HTTPClient:
    def __init__(self):
        self.session = requests.Session(impersonate="chrome")
        self.request_count = 0
        self.consecutive_429s = 0
        self._next_rest = random.randint(30, 40)

    def get(self, url, *, kind="page"):
        base = random.uniform(16, 20) if kind == "listing" else random.uniform(12, 14)
        time.sleep(base)

        if self.request_count > 0 and self.request_count >= self._next_rest:
            rest = random.uniform(50, 70)
            print(f"  [zZz] Rest break ({rest:.0f}s)")
            time.sleep(rest)
            self._next_rest = self.request_count + random.randint(30, 40)

        try:
            headers = {
                "Referer": "https://www.google.com/" if url == HOMEPAGE else HOMEPAGE,
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            }
            res = self.session.get(url, timeout=30, headers=headers)
            self.request_count += 1

            if res.status_code == 200:
                self.consecutive_429s = 0
                return res

            if res.status_code in (429, 403, 503):
                self.consecutive_429s += 1
                wait = random.uniform(90, 120)
                print(f"  [!] HTTP {res.status_code} — waiting {wait:.0f}s (streak: {self.consecutive_429s})")
                time.sleep(wait)

                res2 = self.session.get(url, timeout=30, headers=headers)
                self.request_count += 1
                if res2.status_code == 200:
                    self.consecutive_429s = 0
                    return res2

                print(f"  [!] Retry failed — skipping")
                return None

            return None
        except Exception as exc:
            print(f"  [!] Network error: {exc}")
            return None

    @property
    def should_stop(self):
        return self.consecutive_429s >= 5

    def warmup(self):
        print("[*] Warming up…")
        if self.get(HOMEPAGE, kind="listing"):
            print("[✓] Session ready.\n")
            return True
        return False

http = HTTPClient()

# ── Extraction ──────────────────────────────────────────────────────────────
def root_domain(url_str):
    try:
        return urlparse(url_str).netloc.replace("www.", "").lower()
    except Exception:
        return None

def clean_value(val):
    if not val:
        return None
    val = re.sub(r"\s*\([\d.]+\)", "", val)
    return val.strip(" .-").upper()

def truncate_dom(soup):
    """For harvest link extraction ONLY."""
    raw = soup.find("div", class_="entry-content")
    if not raw:
        return None
    content = BeautifulSoup(str(raw), "html.parser")
    for tag in content.find_all(["h2", "h3", "h4", "h5"]):
        text = tag.get_text(strip=True).lower()
        if any(s in text for s in [
            "detailed report", "analysis / bias", "see also",
            "related sources", "latest ratings", "failed fact checks",
        ]):
            for sib in tag.find_all_next():
                sib.extract()
            tag.extract()
            break
    return content

def extract_metric(text, pattern, whitelist=None):
    m = re.search(pattern, text, re.I)
    if m:
        val = clean_value(m.group(1))
        if not val:
            return None
        if whitelist:
            return val.title() if val in whitelist else None
        return val.title() if len(val) <= 40 else None
    return None

def scrape_metrics(soup):
    """Extract from FULL page content — no truncation."""
    content = soup.find("div", class_="entry-content")
    if not content:
        return {}
    text = content.get_text(separator="\n", strip=True)

    metrics = {
        "f": extract_metric(text,
            r"(?:Factual Reporting|Factuality Rating|Factuality|Factual Report)\s*[:\-–—]\s*([^\n]+)",
            VALID_FACTUALITY),
        "c": extract_metric(text,
            r"(?:MBFC'?s?\s+Credibility\s+Rating|Credibility\s+Rating)\s*[:\-–—]\s*([^\n]+)",
            VALID_CREDIBILITY),
        "p": extract_metric(text,
            r"(?:Country Freedom (?:Rating|Rank)|Press Freedom (?:Rating|Rank)|Freedom of the Press (?:Rating|Rank)|Freedom (?:Rating|Rank)|Press Freedom)\s*[:\-–—]\s*([^\n]+)",
            VALID_FREEDOM),
        "o": extract_metric(text,
            r"(?:Country|Based in|Location)\s*[:\-–—]\s*([^\n(,]+)"),
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
        if wu in COUNTRY_DISCARD:
            metrics["o"] = None
        elif re.search(r'\b(US|USA|UNITED STATES|UNITED STATES OF AMERICA)\b', wu) or re.search(r'(?<!\w)U\.S\.A?\.?(?!\w)', wu):
            metrics["o"] = "USA"
        elif re.search(r'\b(UK|UNITED KINGDOM|GREAT BRITAIN)\b', wu) or re.search(r'(?<!\w)U\.K\.?(?!\w)', wu):
            metrics["o"] = "UK"
        else:
            cosmetic = metrics["o"].title().replace(" And ", " and ").replace(" Of ", " of ")
            metrics["o"] = COUNTRY_NORMALIZE.get(wu, cosmetic)

    return {k: v for k, v in metrics.items() if v}

def extract_source_domain(soup):
    """Extract source domain from 'Source:' text ONLY. No fallback."""
    content = soup.find("div", class_="entry-content")
    if not content:
        return None
    for tag in content.find_all(string=re.compile(r"Sources?\s*(?:URL)?\s*:", re.I)):
        link = tag.parent.find_next("a")
        if link and link.get("href"):
            dom = root_domain(link["href"])
            if dom and not _is_blacklisted(dom) and len(dom) > 3:
                return dom
    return None

# ── Database ────────────────────────────────────────────────────────────────
def load_database():
    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def save_database(db):
    fd, tmp = tempfile.mkstemp(dir=".", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(db, f, separators=(",", ":"))
        os.replace(tmp, DB_FILE)
    except Exception:
        os.unlink(tmp)
        raise

    bias_ct, fact_ct, cred_ct, free_ct, country_ct = {}, {}, {}, {}, {}
    valid = 0
    for key, entry in db.items():
        if key.startswith("_fail:"):
            continue
        valid += 1
        bias_ct[entry.get("b", "Unrated")] = bias_ct.get(entry.get("b", "Unrated"), 0) + 1
        fact_ct[entry.get("f", "Unrated")] = fact_ct.get(entry.get("f", "Unrated"), 0) + 1
        cred_ct[entry.get("c", "Unrated")] = cred_ct.get(entry.get("c", "Unrated"), 0) + 1
        free_ct[entry.get("p", "Unrated")] = free_ct.get(entry.get("p", "Unrated"), 0) + 1
        country_ct[entry.get("o", "Unrated")] = country_ct.get(entry.get("o", "Unrated"), 0) + 1

    md = f"# 📊 Feed Ratings Statistics\n\n**Total Sources:** `{valid}`\n\n"

    md += "### ⚖️ Bias Distribution\n| Bias Category | Count |\n| :--- | :--- |\n"
    for k, v in sorted(bias_ct.items(), key=lambda x: x[1], reverse=True):
        md += f"| {k} | **{v}** |\n"

    md += "\n### ✅ Factuality Distribution\n| Factuality Rating | Count |\n| :--- | :--- |\n"
    for k, v in sorted(fact_ct.items(), key=lambda x: x[1], reverse=True):
        md += f"| {k} | **{v}** |\n"

    md += "\n### 🛡️ Credibility Distribution\n| Credibility Rating | Count |\n| :--- | :--- |\n"
    for k, v in sorted(cred_ct.items(), key=lambda x: x[1], reverse=True):
        md += f"| {k} | **{v}** |\n"

    md += "\n### 🗽 Press Freedom Distribution\n| Freedom Rating | Count |\n| :--- | :--- |\n"
    for k, v in sorted(free_ct.items(), key=lambda x: x[1], reverse=True):
        md += f"| {k} | **{v}** |\n"

    md += "\n### 🌍 Country Distribution (Top 30)\n| Country | Count |\n| :--- | :--- |\n"
    for k, v in sorted(country_ct.items(), key=lambda x: x[1], reverse=True)[:30]:
        md += f"| {k} | **{v}** |\n"

    fd2, tmp2 = tempfile.mkstemp(dir=".", suffix=".tmp")
    try:
        with os.fdopen(fd2, "w", encoding="utf-8") as f:
            f.write(md)
        os.replace(tmp2, MD_FILE)
    except Exception:
        os.unlink(tmp2)
        raise

# ── Pipeline ────────────────────────────────────────────────────────────────
def harvest_urls(db):
    print("\n=== PHASE 1 · HARVESTING ===")
    url_bias_map = {}
    endpoints = list(TARGET_ENDPOINTS.keys())
    random.shuffle(endpoints)

    if not db:
        print("[*] Bootstrap mode — harvesting all categories")
        to_check = endpoints
    else:
        to_check = endpoints[:3]

    for endpoint in to_check:
        if http.should_stop:
            print("  [!] Circuit breaker — stopping harvest")
            break
        res = http.get(endpoint, kind="listing")
        if not res:
            continue
        soup = BeautifulSoup(res.text, "html.parser")
        content = truncate_dom(soup) or soup.find("table", id="mbfc-table")
        if not content:
            continue
        bias = TARGET_ENDPOINTS[endpoint]
        count = 0
        for link in content.find_all("a", href=True):
            href = link["href"].strip().rstrip("/")
            if not href.startswith("https://mediabiasfactcheck.com/"):
                continue
            parts = [p for p in urlparse(href).path.strip("/").split("/") if p]
            if len(parts) != 1:
                continue
            path = parts[0]
            if path not in IGNORE_PATHS and path not in TARGET_SLUGS:
                url_bias_map[href] = bias
                count += 1
        print(f"  [✓] {bias:20s} → {count} sources")

    return url_bias_map

def process_sources(db, url_bias_map):
    now = int(time.time())

    url_to_domain = {}
    for domain, entry in db.items():
        if domain.startswith("_fail:"):
            continue
        u = entry.get("u")
        if u:
            url_to_domain[u] = domain
            if u not in url_bias_map and "b" in entry:
                url_bias_map[u] = entry["b"]

    def last_checked(u):
        fk = f"_fail:{u}"
        if fk in db:
            return db[fk].get("chk", 0)
        d = url_to_domain.get(u)
        return db[d].get("chk", 0) if d else 0

    todo = [
        u for u in url_bias_map
        if last_checked(u) <= now - RECHECK
        and db.get(f"_fail:{u}", {}).get("fails", 0) < 3
    ]
    todo.sort(key=last_checked)

    seen_domains = set()
    filtered = []
    for u in todo:
        known = url_to_domain.get(u)
        if known:
            if known in seen_domains:
                continue
            seen_domains.add(known)
        filtered.append(u)
    todo = filtered

    if MAX_PER_RUN:
        todo = todo[:MAX_PER_RUN]

    total = len(todo)
    print(f"\n=== PHASE 2 · PROCESSING {total} SOURCES ({len(url_bias_map) - total} skipped) ===")
    if not total:
        return 0, 0

    new_ct = upd_ct = 0
    processed_this_run = set()

    for i, url in enumerate(todo, 1):
        if http.should_stop:
            print(f"\n  [!] Circuit breaker at {i - 1}/{total} — saving progress")
            break

        res = http.get(url)
        if not res:
            if not http.should_stop:
                print(f"  [{i}/{total}] [✗] {url}")
            continue

        soup = BeautifulSoup(res.text, "html.parser")
        domain = extract_source_domain(soup)

        fail_key = f"_fail:{url}"
        if not domain:
            db.setdefault(fail_key, {"chk": 0, "fails": 0})
            db[fail_key]["fails"] += 1
            db[fail_key]["chk"] = now
            tag = "☠" if db[fail_key]["fails"] >= 3 else "?"
            print(f"  [{i}/{total}] [{tag}] No domain: {url}")
            if i % 25 == 0:
                save_database(db)
            continue
        else:
            if fail_key in db:
                del db[fail_key]

        if domain in processed_this_run:
            print(f"  [{i}/{total}] [dup] {domain}")
            continue
        processed_this_run.add(domain)

        met = scrape_metrics(soup)
        entry = {"u": url, "chk": now, "b": url_bias_map[url]}
        entry.update(met)

        if domain in db:
            old = db[domain]
            changed = any(old.get(k) != entry.get(k) for k in ("b", "f", "c", "p", "o"))
            if changed:
                db[domain] = entry
                upd_ct += 1
                print(f"  [{i}/{total}] [~] {domain} | {entry.get('b')} | F:{entry.get('f', '—')} C:{entry.get('c', '—')} P:{entry.get('p', '—')} O:{entry.get('o', '—')}")
            else:
                db[domain]["chk"] = now
                print(f"  [{i}/{total}] [-] {domain}")
        else:
            db[domain] = entry
            new_ct += 1
            print(f"  [{i}/{total}] [+] {domain} | {entry.get('b')} | F:{entry.get('f', '—')} C:{entry.get('c', '—')} P:{entry.get('p', '—')} O:{entry.get('o', '—')}")

        if i % 25 == 0:
            save_database(db)

    return new_ct, upd_ct

def main():
    db = load_database()
    save_database(db)

    if not http.warmup():
        return

    url_bias_map = harvest_urls(db)
    if not url_bias_map or http.should_stop:
        if http.should_stop:
            print("[!] Circuit breaker during harvest — saving and exiting")
            save_database(db)
        return

    print(f"\n  [*] Harvested: {len(url_bias_map)} URLs")

    cooldown = random.uniform(90, 120)
    print(f"  [*] Inter-phase cooldown: {cooldown:.0f}s")
    time.sleep(cooldown)

    new_ct, upd_ct = process_sources(db, url_bias_map)

    save_database(db)
    valid = sum(1 for k in db if not k.startswith("_fail:"))
    print(f"\n  [✓] Done — {valid} sources | +{new_ct} new | ~{upd_ct} updated | {http.request_count} requests")

if __name__ == "__main__":
    main()

from curl_cffi import requests
from bs4 import BeautifulSoup
import json, time, random, re, os, tempfile
from urllib.parse import urlparse

# ── Configuration ───────────────────────────────────────────────────────────
DB_FILE     = "ratings.json"
MD_FILE     = "statistics.md"
HOMEPAGE    = "https://mediabiasfactcheck.com/"
RECHECK     = 14 * 86400
MAX_RUNTIME = 310 * 60
START_TIME  = time.time()

TARGET_ENDPOINTS = {
    "https://mediabiasfactcheck.com/left/":         "Left",
    "https://mediabiasfactcheck.com/leftcenter/":   "Left-Center",
    "https://mediabiasfactcheck.com/center/":       "Least Biased",
    "https://mediabiasfactcheck.com/right-center/": "Right-Center",
    "https://mediabiasfactcheck.com/right/":        "Right",
    "https://mediabiasfactcheck.com/pro-science/":  "Pro-Science",
    "https://mediabiasfactcheck.com/fake-news/":    "Questionable",
    "https://mediabiasfactcheck.com/conspiracy/":   "Conspiracy",
    "https://mediabiasfactcheck.com/satire/":       "Satire",
}

TARGET_SLUGS = {urlparse(u).path.strip("/") for u in TARGET_ENDPOINTS}

IGNORE_PATHS = {
    "", "about", "contact", "methodology", "donate", "privacy",
    "terms-and-conditions", "faq", "badges", "membership-account",
    "filter-options", "submit-fact-check", "daily-source-bias-check",
    "podcast", "search", "cookie-policy", "staff-and-writers",
    "membership", "login", "register", "tag", "category", "author", "page",
    "support-media-bias-fact-check",
    "left-vs-right-bias-how-we-rate-the-bias-of-media-sources",
}

_DOMAIN_BLACKLIST = {
    "mediabiasfactcheck.com", "twitter.com", "facebook.com",
    "patreon.com", "x.com", "instagram.com",
    "youtube.com", "linkedin.com", "reddit.com", "tiktok.com",
    "threads.net", "archive.org", "archive.is", "archive.ph",
    "wp.com", "wordpress.com", "gravatar.com",
    "goo.gl", "bit.ly", "tinyurl.com", "amzn.to",
    "apple.com", "play.google.com", "apps.apple.com",
    "domaintools.com", "wikipedia.org",
}

VALID_FACTUALITY  = {"VERY HIGH", "HIGH", "MOSTLY FACTUAL", "MIXED", "LOW", "VERY LOW"}
VALID_CREDIBILITY = {"HIGH CREDIBILITY", "MEDIUM CREDIBILITY", "LOW CREDIBILITY"}
VALID_FREEDOM     = {
    "EXCELLENT FREEDOM", "MOSTLY FREE", "MODERATE FREEDOM",
    "LIMITED FREEDOM", "TOTAL OPPRESSION",
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

TRIVIAL_PATHS = {
    "index.html", "index.php", "index.htm", "home", "main",
    "default.aspx", "default.htm", "wp", "blog",
}

_SOURCE_LINE = [
    re.compile(r"Sources?\s*(?:URL)?\s*:", re.I),
    re.compile(r"(?:Source|Official)\s*Website\s*:", re.I),
    re.compile(r"Website\s*:", re.I),
    re.compile(r"Homepage\s*:", re.I),
    re.compile(r"URL\s*:", re.I),
]

# ── Helpers ─────────────────────────────────────────────────────────────────
def _is_blacklisted(dom):
    if not dom:
        return True
    for bl in _DOMAIN_BLACKLIST:
        if dom == bl or dom.endswith("." + bl):
            return True
    return False

def time_remaining():
    return MAX_RUNTIME - (time.time() - START_TIME)

def source_key_from_url(url_str):
    try:
        p = urlparse(url_str.strip().rstrip("/"))
        dom = p.netloc.replace("www.", "").lower().strip(".")
        if not dom or len(dom) < 4:
            return None
        path = p.path.strip("/")
        if path and path.lower() not in TRIVIAL_PATHS:
            return f"{dom}/{path}"
        return dom
    except Exception:
        return None

def root_domain_of_key(key):
    return key.split("/")[0] if key else None

# ── HTTP Client ─────────────────────────────────────────────────────────────
class HTTPClient:
    def __init__(self):
        self.session = requests.Session(impersonate="chrome")
        self.request_count = 0
        self.consecutive_429s = 0
        self._next_rest = random.randint(30, 40)
        self._backoff_until = 0

    def get(self, url, *, kind="page"):
        now = time.time()
        if now < self._backoff_until:
            wait = self._backoff_until - now
            print(f"  [⏳] Backoff {wait:.0f}s")
            time.sleep(wait)

        base = random.uniform(16, 20) if kind == "listing" else random.uniform(12, 14)
        time.sleep(base)

        if self.request_count > 0 and self.request_count >= self._next_rest:
            rest = random.uniform(50, 70)
            print(f"  [zZz] Rest break ({rest:.0f}s)")
            time.sleep(rest)
            self._next_rest = self.request_count + random.randint(30, 40)

        headers = {
            "Referer": "https://www.google.com/" if url == HOMEPAGE else HOMEPAGE,
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }

        for attempt in range(1, 4):
            try:
                res = self.session.get(url, timeout=30, headers=headers)
                self.request_count += 1
            except Exception as exc:
                print(f"  [!] Network error (attempt {attempt}/3): {exc}")
                if attempt < 3:
                    time.sleep(random.uniform(60, 90))
                    continue
                self.consecutive_429s += 1
                return None

            if res.status_code == 200:
                self.consecutive_429s = 0
                return res

            if res.status_code in (429, 403, 503):
                if attempt < 3:
                    wait = random.uniform(90, 120) if attempt == 1 else random.uniform(120, 180)
                    print(f"  [!] HTTP {res.status_code} — attempt {attempt}/3, waiting {wait:.0f}s (streak: {self.consecutive_429s})")
                    time.sleep(wait)
                    if res.status_code in (403, 503):
                        self.session = requests.Session(impersonate="chrome")
                    continue
                self.consecutive_429s += 1
                self._backoff_until = time.time() + random.uniform(180, 240)
                print(f"  [!] HTTP {res.status_code} — all attempts failed, backoff (streak: {self.consecutive_429s})")
                return None

            print(f"  [!] HTTP {res.status_code} — {url}")
            return None

        return None

    @property
    def should_stop(self):
        return self.consecutive_429s >= 5

    def warmup(self):
        print("[*] Warming up…")
        if self.get(HOMEPAGE, kind="listing"):
            print("[✓] Session ready.\n")
            return True
        print("[✗] Warmup failed")
        return False

http = HTTPClient()

# ── Extraction ──────────────────────────────────────────────────────────────
def clean_value(val):
    if not val:
        return None
    val = re.sub(r"\s*\([\d.]+\)", "", val)
    return val.strip(" .-").upper()

def truncate_dom(soup):
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

def extract_source_key(soup):
    content = soup.find("div", class_="entry-content")
    if not content:
        return None

    for p in content.find_all(["p", "div", "li", "span", "td"]):
        p_text = p.get_text(strip=True)
        if len(p_text) > 300:
            continue

        matched = False
        for pattern in _SOURCE_LINE:
            if pattern.match(p_text):
                matched = True
                break
        if not matched:
            continue

        for link in p.find_all("a", href=True):
            key = source_key_from_url(link["href"])
            dom = root_domain_of_key(key) if key else None
            if key and dom and not _is_blacklisted(dom) and len(dom) > 3:
                return key

        url_match = re.search(r"https?://[^\s<>\"')]+", p_text, re.I)
        if url_match:
            key = source_key_from_url(url_match.group(0).rstrip(".,;:"))
            dom = root_domain_of_key(key) if key else None
            if key and dom and not _is_blacklisted(dom) and len(dom) > 3:
                return key

        m = re.search(r":\s*(.+)", p_text)
        if m:
            raw = m.group(1).strip()
            dotted = re.sub(r"\s*\(dot\)\s*", ".", raw, flags=re.I).lower().strip(" ./")
            if re.match(r"^[a-z0-9]([a-z0-9\-]*\.)+[a-z]{2,}(/[\w\-/]*)?$", dotted):
                dom = dotted.split("/")[0]
                if not _is_blacklisted(dom) and len(dom) > 3:
                    return dotted.rstrip("/")

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
        country_ct[entry.get("o", "Unknown")] = country_ct.get(entry.get("o", "Unknown"), 0) + 1

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
def harvest_category(endpoint_url, bias_name):
    res = http.get(endpoint_url, kind="listing")
    if not res:
        print(f"  [✗] Failed: {bias_name}")
        return None

    soup = BeautifulSoup(res.text, "html.parser")
    content = truncate_dom(soup) or soup.find("table", id="mbfc-table")
    if not content:
        print(f"  [✗] No content: {bias_name}")
        return None

    urls = []
    seen = set()
    for link in content.find_all("a", href=True):
        href = link["href"].strip().rstrip("/")
        if href in seen:
            continue
        if not href.startswith("https://mediabiasfactcheck.com/"):
            continue
        parts = [p for p in urlparse(href).path.strip("/").split("/") if p]
        if len(parts) != 1:
            continue
        path = parts[0]
        if path not in IGNORE_PATHS and path not in TARGET_SLUGS:
            urls.append(href)
            seen.add(href)

    print(f"  [✓] {bias_name:20s} → {len(urls)} sources")
    return urls

def process_category(db, bias_name, urls, url_to_key):
    now = int(time.time())

    todo = []
    fresh = 0
    for u in urls:
        k = url_to_key.get(u)
        if k and k in db:
            if db[k].get("chk", 0) > now - RECHECK:
                fresh += 1
                if db[k].get("b") != bias_name:
                    db[k]["b"] = bias_name
                    db[k]["chk"] = now
                continue
        fk = f"_fail:{u}"
        if db.get(fk, {}).get("fails", 0) >= 3:
            continue
        todo.append(u)

    if not todo:
        print(f"\n  ── {bias_name}: COMPLETE ({fresh}/{len(urls)}) ──")
        return 0, 0

    def sort_key(u):
        k = url_to_key.get(u)
        if not k:
            return (0, 0)
        return (1, db.get(k, {}).get("chk", 0))
    todo.sort(key=sort_key)

    new_in = sum(1 for u in todo if u not in url_to_key)
    stale_in = len(todo) - new_in
    print(f"\n  ── {bias_name} ({new_in} new + {stale_in} stale = {len(todo)} pending, {fresh} fresh) ──")

    new_ct = upd_ct = 0
    processed_keys = set()
    total = len(todo)

    for i, url in enumerate(todo, 1):
        if http.should_stop:
            print(f"  [!] Circuit breaker in {bias_name} at {i - 1}/{total}")
            break
        if time_remaining() < 300:
            print(f"  [!] Time limit in {bias_name} at {i - 1}/{total}")
            break

        res = http.get(url)
        if not res:
            if not http.should_stop:
                print(f"  [{i}/{total}] [✗] {url}")
            continue

        soup = BeautifulSoup(res.text, "html.parser")

        known_key = url_to_key.get(url)
        source_key = known_key if known_key else extract_source_key(soup)

        fail_key = f"_fail:{url}"
        if not source_key:
            db.setdefault(fail_key, {"chk": 0, "fails": 0})
            db[fail_key]["fails"] += 1
            db[fail_key]["chk"] = now
            tag = "☠" if db[fail_key]["fails"] >= 3 else "?"
            print(f"  [{i}/{total}] [{tag}] No source: {url}")
            if i % 25 == 0:
                save_database(db)
            continue
        else:
            if fail_key in db:
                del db[fail_key]

        if source_key in processed_keys:
            print(f"  [{i}/{total}] [dup] {source_key}")
            continue
        processed_keys.add(source_key)

        met = scrape_metrics(soup)
        entry = {"u": url, "chk": now, "b": bias_name}
        entry.update(met)

        if source_key in db:
            old = db[source_key]
            changed = any(old.get(k) != entry.get(k) for k in ("b", "f", "c", "p", "o"))
            if changed:
                db[source_key] = entry
                upd_ct += 1
                print(f"  [{i}/{total}] [~] {source_key} | {bias_name} | F:{met.get('f', '—')} C:{met.get('c', '—')} P:{met.get('p', '—')} O:{met.get('o', '—')}")
            else:
                db[source_key]["chk"] = now
                print(f"  [{i}/{total}] [-] {source_key}")
        else:
            db[source_key] = entry
            new_ct += 1
            url_to_key[url] = source_key
            print(f"  [{i}/{total}] [+] {source_key} | {bias_name} | F:{met.get('f', '—')} C:{met.get('c', '—')} P:{met.get('p', '—')} O:{met.get('o', '—')}")

        if i % 25 == 0:
            save_database(db)

    remaining = total - i if http.should_stop or time_remaining() < 300 else 0
    if remaining > 0:
        print(f"  ── {bias_name}: paused, {remaining} remaining | +{new_ct} new ~{upd_ct} upd ──")
    else:
        print(f"  ── {bias_name}: done | +{new_ct} new ~{upd_ct} upd ──")

    return new_ct, upd_ct

def main():
    db = load_database()
    save_database(db)

    if not http.warmup():
        return

    # ── Phase 1: Harvest all 9 categories ──
    print("\n=== PHASE 1 · HARVESTING ALL CATEGORIES ===")
    categories = {}
    endpoints = list(TARGET_ENDPOINTS.items())
    random.shuffle(endpoints)

    for endpoint_url, bias_name in endpoints:
        if http.should_stop:
            print("  [!] Circuit breaker — stopping harvest")
            break
        if time_remaining() < 600:
            print("  [!] Time limit — stopping harvest")
            break
        urls = harvest_category(endpoint_url, bias_name)
        if urls:
            categories[bias_name] = urls

    if not categories:
        print("[!] No categories harvested")
        save_database(db)
        return

    total_harvested = sum(len(u) for u in categories.values())
    print(f"\n  [*] Harvested: {total_harvested} URLs across {len(categories)} categories")

    # ── Build index from existing DB ──
    now = int(time.time())
    url_to_key = {}
    for key, entry in db.items():
        if key.startswith("_fail:"):
            continue
        u = entry.get("u")
        if u:
            url_to_key[u] = key

    # ── Sort: smallest pending first for quick completions ──
    cat_pending = {}
    for bias_name, urls in categories.items():
        pending = 0
        for u in urls:
            k = url_to_key.get(u)
            if k and k in db and db[k].get("chk", 0) > now - RECHECK:
                continue
            fk = f"_fail:{u}"
            if db.get(fk, {}).get("fails", 0) >= 3:
                continue
            pending += 1
        cat_pending[bias_name] = pending

    order = sorted(categories.keys(), key=lambda b: cat_pending[b])

    print("\n  Processing order:")
    for b in order:
        t = len(categories[b])
        p = cat_pending[b]
        status = "✓ COMPLETE" if p == 0 else f"{t - p} done, {p} pending"
        print(f"    {b:20s} {t:5d} total | {status}")

    # ── Pre-processing cooldown ──
    cooldown = random.uniform(60, 90)
    print(f"\n  [*] Cooldown: {cooldown:.0f}s")
    time.sleep(cooldown)

    # ── Phase 2: Process category by category ──
    total_new = total_upd = 0

    for bias_name in order:
        if http.should_stop:
            print(f"\n  [!] Circuit breaker — stopping")
            break
        if time_remaining() < 300:
            print(f"\n  [!] Time limit ({time_remaining() / 60:.0f} min) — stopping")
            break

        if cat_pending[bias_name] == 0:
            continue

        new_ct, upd_ct = process_category(db, bias_name, categories[bias_name], url_to_key)
        total_new += new_ct
        total_upd += upd_ct

        save_database(db)

        if time_remaining() > 300 and not http.should_stop:
            cd = random.uniform(30, 50)
            print(f"  [zZz] Inter-category pause ({cd:.0f}s)\n")
            time.sleep(cd)

    save_database(db)
    valid = sum(1 for k in db if not k.startswith("_fail:"))
    elapsed = (time.time() - START_TIME) / 60
    print(f"\n  [✓] Done in {elapsed:.0f} min — {valid} sources | +{total_new} new | ~{total_upd} updated | {http.request_count} requests")

if __name__ == "__main__":
    main()

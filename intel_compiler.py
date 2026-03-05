from curl_cffi import requests
from bs4 import BeautifulSoup
import json, time, random, re, os
from urllib.parse import urlparse

# ── Configuration ───────────────────────────────────────────────────────────
DB_FILE    = "ratings.json"
MD_FILE    = "statistics.md"
HOMEPAGE   = "https://mediabiasfactcheck.com/"
RECHECK    = 14 * 86400                                   # skip if checked within 14 days
MAX_PER_RUN = int(os.environ.get("MAX_PER_RUN", 0)) or None  # env override; 0 = unlimited

TARGET_ENDPOINTS = [
    "https://mediabiasfactcheck.com/left/",
    "https://mediabiasfactcheck.com/leftcenter/",
    "https://mediabiasfactcheck.com/center/",
    "https://mediabiasfactcheck.com/right-center/",
    "https://mediabiasfactcheck.com/right/",
    "https://mediabiasfactcheck.com/pro-science/",
    "https://mediabiasfactcheck.com/fake-news/",
    "https://mediabiasfactcheck.com/conspiracy/",
    "https://mediabiasfactcheck.com/satire/",
]

# FIX #5: exact path matching instead of substring matching
CATEGORY_PATHS = {urlparse(u).path.strip("/") for u in TARGET_ENDPOINTS}

IGNORE_PATHS = {
    "", "about", "contact", "methodology", "donate", "privacy",
    "terms-and-conditions", "faq", "badges", "membership-account",
    "filter-options", "submit-fact-check", "daily-source-bias-check",
    "podcast", "search", "cookie-policy", "staff-and-writers",
}


# ── FIX #1-4: Robust HTTP client with warm-up, referer, adaptive delays ────
class HTTPClient:
    """curl_cffi session with Cloudflare bypass and adaptive rate-limiting."""

    def __init__(self):
        profile = random.choice(["chrome120", "chrome124"])
        self.session = requests.Session(impersonate=profile)
        self._consecutive_errors = 0
        self.request_count = 0
        print(f"[*] TLS fingerprint: {profile}")

    def _adaptive_delay(self, kind):
        """FIX #2: Human-realistic delays that grow with consecutive errors."""
        if self._consecutive_errors >= 5:
            base = random.uniform(30, 60)       # heavy backoff
        elif self._consecutive_errors >= 2:
            base = random.uniform(8, 15)        # moderate backoff
        elif kind == "listing":
            base = random.uniform(4, 8)         # listing pages: slow & careful
        else:
            base = random.uniform(2, 4)         # article pages: steady pace
        time.sleep(base)

    def get(self, url, *, kind="page", retries=3):
        for attempt in range(retries):
            self._adaptive_delay(kind)
            try:
                # FIX #3: always send Referer like a real browser
                res = self.session.get(
                    url, timeout=20,
                    headers={"Referer": HOMEPAGE},
                )
                self.request_count += 1

                if res.status_code == 200:
                    self._consecutive_errors = 0
                    return res

                if res.status_code in (403, 429, 503):
                    self._consecutive_errors += 1
                    # FIX #4: exponential backoff → 60 s, 120 s, 240 s (cap 300 s)
                    wait = min(60 * (2 ** attempt), 300)
                    print(f"  [!] HTTP {res.status_code} → cooldown {wait}s "
                          f"(attempt {attempt + 1}/{retries})")
                    time.sleep(wait)
                    continue

                return None  # other status codes → don't retry
            except Exception as exc:
                self._consecutive_errors += 1
                print(f"  [!] Network error: {exc}")
                if attempt < retries - 1:
                    time.sleep(15)
        return None

    def warmup(self):
        """FIX #1: visit homepage first to establish cookies/session."""
        print("[*] Warming up session (homepage visit)…")
        if self.get(HOMEPAGE, kind="listing"):
            print("[✓] Session cookies established.\n")
            return True
        print("[✗] Cannot reach MBFC homepage — aborting.\n")
        return False


http = HTTPClient()


# ── Helpers ─────────────────────────────────────────────────────────────────
def root_domain(url_str):
    try:
        return urlparse(url_str).netloc.replace("www.", "").lower()
    except Exception:
        return None


def normalize(value, *, country=False, bias=False):
    if not value:
        return None
    v = value.strip()
    if v.upper() in ("", "UNKNOWN", "N/A", "NONE", "UNRATED"):
        return None

    if country:
        def _fix(w):
            w = w.strip()
            wu = w.upper()
            if wu in ("US", "U.S.", "USA", "U.S.A.",
                       "UNITED STATES", "UNITED STATES OF AMERICA"):
                return "USA"
            if wu in ("UK", "U.K.", "UNITED KINGDOM", "GREAT BRITAIN"):
                return "UK"
            if wu in ("UAE", "EU"):
                return wu
            return w.title() if len(w) > 3 else wu
        if "," in v:
            return ", ".join(_fix(c) for c in v.split(","))
        return _fix(v)

    if bias:
        vu = v.upper()
        if "SATIRE" in vu:                              return "Satire"
        if "PRO-SCIENCE" in vu or vu == "SCIENCE":      return "Pro-Science"
        if "CONSPIRACY" in vu or "PSEUDOSCIENCE" in vu:  return "Conspiracy"
        if "QUESTIONABLE" in vu or "FAKE NEWS" in vu:    return "Questionable"

    return v.title()


_DOMAIN_BLACKLIST = {
    "mediabiasfactcheck", "twitter.com", "facebook.com",
    "wikipedia.org", "patreon.com", "x.com", "instagram.com",
}


def extract_source_domain(soup):
    for tag in soup.find_all(string=re.compile(r"Source:\s*", re.I)):
        link = tag.parent.find_next("a")
        if link and link.get("href"):
            href = link["href"]
            if not any(bl in href.lower() for bl in _DOMAIN_BLACKLIST):
                dom = root_domain(href)
                if dom and len(dom) > 3:
                    return dom
    return None


# FIX #6: stop keywords must be followed by a colon (actual field labels only)
_STOP_LABELS = (
    r"(?:"
    r"Bias Rating|Factual Reporting|Factuality Rating|Factuality|"
    r"MBFC Credibility Rating|Credibility Rating|Credibility|"
    r"Country Freedom Rating|Country Freedom|Press Freedom Rating|"
    r"Press Freedom|Freedom Rating|Media Type|Traffic|Popularity|"
    r"World Press|MBFC|Overall"
    r")\s*:"
)


def _pull(text, keyword_pattern):
    """Extract the value after 'Keyword: value' up to the next field label."""
    m = re.search(
        rf"{keyword_pattern}\s*:\s*(.*?)(?=\s*{_STOP_LABELS}|\.\s+(?=[A-Z])|\s-\s|$)",
        text, re.I,
    )
    if m:
        val = m.group(1).replace("\u00a0", " ").strip().rstrip(".-")
        if 0 < len(val) <= 40:
            return val
    return None


def scrape_metrics(soup):
    text = re.sub(r"\s+", " ", soup.get_text(separator=" "))
    return {
        "b": normalize(_pull(text, r"(?:Bias Rating|Bias)"), bias=True),
        "f": normalize(_pull(text, r"(?:Factual Reporting|Factuality Rating|Factuality)")),
        "c": normalize(_pull(text, r"(?:MBFC Credibility Rating|Credibility Rating|Credibility)")),
        "p": normalize(_pull(text, r"(?:Country Freedom Rating|Country Freedom|"
                                   r"Press Freedom Rating|Press Freedom|Freedom Rating)")),
        "o": normalize(_pull(text, r"Country(?!\s+Freedom)"), country=True),
    }


# ── Database I/O ───────────────────────────────────────────────────────────
def save_database(db):
    with open(DB_FILE, "w", encoding="utf-8") as f:
        json.dump(db, f, separators=(",", ":"))

    bias_counts, fact_counts = {}, {}
    for entry in db.values():
        b = entry.get("b", "Unrated")
        f = entry.get("f", "Unrated")
        bias_counts[b] = bias_counts.get(b, 0) + 1
        fact_counts[f] = fact_counts.get(f, 0) + 1

    md = f"# 📊 Feed Ratings Statistics\n\n**Total Sources:** `{len(db)}`\n\n"
    md += "### ⚖️ Bias Distribution\n| Bias Category | Count |\n| :--- | :--- |\n"
    for k, v in sorted(bias_counts.items(), key=lambda x: x[1], reverse=True):
        md += f"| {k} | **{v}** |\n"
    md += "\n### ✅ Factuality Distribution\n| Factuality Rating | Count |\n| :--- | :--- |\n"
    for k, v in sorted(fact_counts.items(), key=lambda x: x[1], reverse=True):
        md += f"| {k} | **{v}** |\n"
    with open(MD_FILE, "w", encoding="utf-8") as f:
        f.write(md)


def load_database():
    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, "r", encoding="utf-8") as f:
                db = json.load(f)
            print(f"[+] Loaded existing database: {len(db)} entries")
            return db
        except Exception:
            pass
    return {}


# ── Pipeline ────────────────────────────────────────────────────────────────
def harvest_urls():
    """Phase 1: collect all source-page URLs from category listing pages."""
    print("\n=== PHASE 1 · HARVESTING SOURCE URLS ===")
    urls = set()

    # FIX #8: randomise the order we hit category pages
    endpoints = list(TARGET_ENDPOINTS)
    random.shuffle(endpoints)

    for endpoint in endpoints:
        res = http.get(endpoint, kind="listing")
        if not res:
            print(f"  [✗] Failed: {endpoint}")
            continue

        soup = BeautifulSoup(res.text, "html.parser")
        content_area = (soup.find("div", class_="entry-content")
                        or soup.find("table", id="mbfc-table"))
        if not content_area:
            print(f"  [!] No content area found: {endpoint}")
            continue

        count = 0
        for link in content_area.find_all("a", href=True):
            href = link["href"].strip().rstrip("/")
            if not href.startswith("https://mediabiasfactcheck.com/"):
                continue
            path = urlparse(href).path.strip("/")
            # FIX #5: exact path comparison, not substring
            if path and path not in IGNORE_PATHS and path not in CATEGORY_PATHS:
                urls.add(href)
                count += 1

        label = endpoint.split("/")[-2]
        print(f"  [✓] {label:20s} → {count} sources")

    print(f"\n  Total unique URLs harvested: {len(urls)}")
    return urls


def process_sources(db, urls):
    """Phase 2: visit each source page, extract metrics, update database."""
    now = int(time.time())

    # FIX #7: skip sources that were checked recently
    recently_checked = {
        entry["u"] for entry in db.values()
        if entry.get("chk", 0) > now - RECHECK
    }
    todo = [u for u in urls if u not in recently_checked]

    # FIX #8: randomise processing order
    random.shuffle(todo)

    # Optional: cap per-run volume for CI time limits
    if MAX_PER_RUN:
        todo = todo[:MAX_PER_RUN]

    skipped = len(urls) - len(todo)
    total = len(todo)
    print(f"\n=== PHASE 2 · PROCESSING {total} SOURCES ({skipped} skipped) ===")

    if total == 0:
        print("[✓] All sources are up-to-date.")
        return 0, 0

    new_count = updated_count = 0

    for i, url in enumerate(todo, 1):
        res = http.get(url)
        if not res:
            print(f"  [{i}/{total}] [✗] {url}")
            continue

        soup = BeautifulSoup(res.text, "html.parser")
        domain = extract_source_domain(soup)
        if not domain:
            continue

        met = scrape_metrics(soup)
        entry = {"u": url, "chk": int(time.time())}
        for key, val in met.items():
            if val:
                entry[key] = val

        if domain in db:
            old = db[domain]
            changed = any(old.get(k) != entry.get(k) for k in ("b", "f", "c", "p", "o"))
            if changed:
                db[domain] = entry
                updated_count += 1
                print(f"  [{i}/{total}] [~] UPDATED: {domain} | {met['b']} | {met['f']}")
            else:
                db[domain]["chk"] = entry["chk"]
                print(f"  [{i}/{total}] [-] {domain}")
        else:
            db[domain] = entry
            new_count += 1
            print(f"  [{i}/{total}] [+] NEW: {domain} | {met['b']} | {met['f']}")

        # Checkpoint every 25 entries (more frequent than the original 50)
        if i % 25 == 0:
            save_database(db)
            print(f"  ── checkpoint: {len(db)} total | "
                  f"+{new_count} new | ~{updated_count} updated ──")

    return new_count, updated_count


def main():
    db = load_database()
    save_database(db)  # ensure files exist for CI

    # FIX #1: warm up session before any real work
    if not http.warmup():
        return

    urls = harvest_urls()
    if not urls:
        print("[✓] No source URLs found. Exiting.")
        return

    new_count, updated_count = process_sources(db, urls)

    print("\n=== PHASE 3 · FINALIZING ===")
    save_database(db)
    print(f"[✓] Complete — {len(db)} total sources | "
          f"+{new_count} new | ~{updated_count} updated | "
          f"{http.request_count} HTTP requests made")


if __name__ == "__main__":
    main()

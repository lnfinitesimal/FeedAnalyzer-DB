from curl_cffi import requests
from bs4 import BeautifulSoup
import json, time, random, re, os, tempfile
from urllib.parse import urlparse
from collections import Counter
import tldextract
import pycountry

# ── Configuration ───────────────────────────────────────────────────────────
DB_FILE     = "ratings.json"
MD_FILE     = "statistics.md"
HOMEPAGE    = "https://mediabiasfactcheck.com/"
RECHECK     = 14 * 86400       # 14-day freshness window
MAX_RUNTIME = 310 * 60         # 310 min — 30 min buffer before GitHub's 340 min limit
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

TRIVIAL_PATHS = {
    "index.html", "index.php", "index.htm", "home", "main",
    "default.aspx", "default.htm", "wp", "blog",
}

# ── Trivial subdomains (stripped like www) ──────────────────────────────────
TRIVIAL_SUBDOMAINS = {
    "www", "www1", "www2", "www3", "ww1", "ww2", "ww3",
    "m", "mobile", "amp",
    "edition",
    "en", "english",
    "beta", "secure",
    "web", "old", "new", "classic", "legacy",
    "feeds", "rss", "feed",
}

# ── Failure tracking (separate from main DB) ───────────────────────────────
FAIL_FILE   = "failures.json"
FAIL_EXPIRY = 90 * 86400   # forget failures after 90 days
FAIL_MAX    = 3             # stop retrying after 3 failures (until expiry)

_SOURCE_LINE = [
    re.compile(r"Sources?\s*(?:URL)?\s*:", re.I),
    re.compile(r"(?:Source|Official)\s*Website\s*:", re.I),
    re.compile(r"Website\s*:", re.I),
    re.compile(r"Homepage\s*:", re.I),
    re.compile(r"URL\s*:", re.I),
]

# ── Bias normalisation (page-level extraction) ─────────────────────────────
BIAS_NORMALIZE = {
    "CONSPIRACY-PSEUDOSCIENCE": "Conspiracy",
    "CONSPIRACY PSEUDOSCIENCE": "Conspiracy",
    "QUESTIONABLE SOURCES":    "Questionable",
    "QUESTIONABLE SOURCE":     "Questionable",
    "LEAST BIASED":            "Least Biased",
    "LEAST-BIASED":            "Least Biased",
    "RIGHT-CENTER":            "Right-Center",
    "RIGHT CENTER":            "Right-Center",
    "RIGHTCENTER":             "Right-Center",
    "LEFT-CENTER":             "Left-Center",
    "LEFT CENTER":             "Left-Center",
    "LEFTCENTER":              "Left-Center",
    "EXTREME RIGHT":           "Right",
    "EXTREME LEFT":            "Left",
    "PRO-SCIENCE":             "Pro-Science",
    "PRO SCIENCE":             "Pro-Science",
    "FAR RIGHT":               "Right",
    "FAR LEFT":                "Left",
    "QUESTIONABLE":            "Questionable",
    "CONSPIRACY":              "Conspiracy",
    "SATIRE":                  "Satire",
    "RIGHT":                   "Right",
    "LEFT":                    "Left",
    "CENTER":                  "Least Biased",
}

_BIAS_KEYS_BY_LEN = sorted(BIAS_NORMALIZE, key=len, reverse=True)

# ── Country normalisation ───────────────────────────────────────────────────
COUNTRY_DISCARD = {"UNKNOWN", "N/A", "NA", "NONE", "TBD", "VARIOUS", "MULTIPLE"}

COUNTRY_MANUAL = {
    "REPUBLIC OF KOREA": "South Korea",
    "THE NETHERLANDS": "Netherlands",
    "NETHERLANDS": "Netherlands",
    "CZECHIA": "Czech Republic",
    "RUSSIAN FEDERATION": "Russia",
    "DEMOCRATIC PEOPLE'S REPUBLIC OF KOREA": "North Korea",
    "UAE": "United Arab Emirates",
    "ROC": "Taiwan",
    "REPUBLIC OF CHINA": "Taiwan",
    "BURMA": "Myanmar",
    "IVORY COAST": "Ivory Coast",
    "SWAZILAND": "Eswatini",
}

_PYCOUNTRY_OVERRIDES = {
    "Korea, Republic of": "South Korea",
    "Korea, Democratic People's Republic of": "North Korea",
    "Taiwan, Province of China": "Taiwan",
    "Russian Federation": "Russia",
    "Iran, Islamic Republic of": "Iran",
    "Venezuela, Bolivarian Republic of": "Venezuela",
    "Bolivia, Plurinational State of": "Bolivia",
    "Tanzania, United Republic of": "Tanzania",
    "Syrian Arab Republic": "Syria",
    "Lao People's Democratic Republic": "Laos",
    "Viet Nam": "Vietnam",
    "Brunei Darussalam": "Brunei",
    "Türkiye": "Turkey",
    "Côte d'Ivoire": "Ivory Coast",
    "Congo, The Democratic Republic of the": "DR Congo",
    "Moldova, Republic of": "Moldova",
    "Palestine, State of": "Palestine",
    "United States of America": "USA",
    "United States": "USA",
    "United Kingdom of Great Britain and Northern Ireland": "United Kingdom",
}

# ── Country capture truncation ──────────────────────────────────────────────
_COUNTRY_STOP_RE = re.compile(
    r'\s+(?:but|and|or|with|while|however|although|though|yet|also|'
    r'that|which|where|who|whose|as|for|from|by|at|'
    r'operates?|has|have|is|are|was|were|serves?|covers?|'
    r'focuses?|targets?|caters?|reports?|publishes?|according)\b',
    re.I,
)

# ── Pre-computed set of every valid normalised country name ─────────────────
_KNOWN_COUNTRIES = set()
for _c in pycountry.countries:
    _name = _PYCOUNTRY_OVERRIDES.get(_c.name, _c.name)
    _KNOWN_COUNTRIES.add(_name)
_KNOWN_COUNTRIES.update(COUNTRY_MANUAL.values())
_KNOWN_COUNTRIES.update({
    "USA", "United Kingdom", "South Korea", "North Korea", "Taiwan",
    "Russia", "Iran", "Venezuela", "Bolivia", "Tanzania", "Syria",
    "Laos", "Vietnam", "Brunei", "Turkey", "Ivory Coast", "DR Congo",
    "Moldova", "Palestine", "Myanmar", "Eswatini",
})


# ═══════════════════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════════════════
def _is_blacklisted(dom):
    if not dom:
        return True
    dom = dom.lower().strip(".")
    for bl in _DOMAIN_BLACKLIST:
        if dom == bl or dom.endswith("." + bl):
            return True
    return False


def time_remaining():
    return MAX_RUNTIME - (time.time() - START_TIME)


def source_key_from_url(url_str):
    """Canonical source key via tldextract.
    Handles multi-level TLDs (co.uk, com.au) and preserves meaningful
    subdomains (news.bbc.co.uk) while stripping www + trivial variants
    (m., mobile., amp., edition., etc.)."""
    try:
        p = urlparse(url_str.strip().rstrip("/"))
        ext = tldextract.extract(url_str)
        if not ext.domain or not ext.suffix:
            return None

        sub_parts = [s for s in ext.subdomain.lower().split(".")
                     if s and s not in TRIVIAL_SUBDOMAINS]
        registered = f"{ext.domain}.{ext.suffix}".lower()
        dom = f"{'.'.join(sub_parts)}.{registered}" if sub_parts else registered

        if _is_blacklisted(dom) or len(dom) < 4:
            return None

        path = p.path.strip("/")
        if path and path.lower() not in TRIVIAL_PATHS:
            return f"{dom}/{path}"
        return dom
    except Exception:
        return None


def root_domain_of_key(key):
    return key.split("/")[0] if key else None


def normalize_country(raw):
    """Normalise a raw country string to a canonical name.

    Pipeline:
      1. Truncate at conjunctions/verbs (prevents "USA but operates in Canada")
      2. Strip parenthetical scores, trailing punctuation
      3. Discard known non-country tokens
      4. Manual map → exact pycountry lookup
      5. Validate against known-countries set
      6. Return None (not raw text) if unrecognised — never guess
    """
    if not raw:
        return None

    # ① Truncate at conjunctions / prose verbs
    truncated = _COUNTRY_STOP_RE.split(raw)[0].strip()

    # ② Strip parenthetical content and numeric scores
    truncated = re.sub(r"\([^)]*\)", "", truncated)
    truncated = re.sub(r"\s*[\d.]+\s*$", "", truncated)
    truncated = truncated.strip(" \t\n\r.,;:–—-/")

    if not truncated:
        return None
    if len(truncated) > 50:
        return None

    wu = truncated.upper().strip()

    if wu in COUNTRY_DISCARD:
        return None

    # ③ US variants
    if re.search(r'\b(US|USA|UNITED STATES|UNITED STATES OF AMERICA)\b', wu) \
       or re.search(r'(?<!\w)U\.S\.A?\.?(?!\w)', wu):
        return "USA"

    # ④ UK variants
    if re.search(r'\b(UK|UNITED KINGDOM|GREAT BRITAIN)\b', wu) \
       or re.search(r'(?<!\w)U\.K\.?(?!\w)', wu):
        return "United Kingdom"

    # ⑤ Manual map (exact, uppercased)
    if wu in COUNTRY_MANUAL:
        return COUNTRY_MANUAL[wu]

    # ⑥ Cosmetic title-case for pycountry
    cosmetic = (truncated.strip()
                .title()
                .replace(" And ", " and ")
                .replace(" Of ", " of ")
                .replace(" The ", " the "))

    # ⑦ Exact pycountry lookup (handles alpha-2, alpha-3, official names)
    try:
        country = pycountry.countries.lookup(cosmetic)
        return _PYCOUNTRY_OVERRIDES.get(country.name, country.name)
    except LookupError:
        pass

    # Also try the uppercased form (handles "GERMANY" etc.)
    try:
        country = pycountry.countries.lookup(truncated.strip())
        return _PYCOUNTRY_OVERRIDES.get(country.name, country.name)
    except LookupError:
        pass

    # ⑧ Final validation: only return if it's a known country name
    #    NO fuzzy matching — that's where hallucinations come from
    if cosmetic in _KNOWN_COUNTRIES:
        return cosmetic

    # Unknown — return None instead of guessing
    return None


# ═══════════════════════════════════════════════════════════════════════════
#  HTTP CLIENT — 3-attempt with escalating backoff & circuit breaker
# ═══════════════════════════════════════════════════════════════════════════
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
                    wait = (random.uniform(90, 120) if attempt == 1
                            else random.uniform(120, 180))
                    print(f"  [!] HTTP {res.status_code} — attempt {attempt}/3, "
                          f"waiting {wait:.0f}s (streak: {self.consecutive_429s})")
                    time.sleep(wait)
                    if res.status_code in (403, 503):
                        self.session = requests.Session(impersonate="chrome")
                    continue
                self.consecutive_429s += 1
                self._backoff_until = time.time() + random.uniform(180, 240)
                print(f"  [!] HTTP {res.status_code} — all attempts failed, "
                      f"backoff (streak: {self.consecutive_429s})")
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


# ═══════════════════════════════════════════════════════════════════════════
#  EXTRACTION — metrics + source key + page-level bias
# ═══════════════════════════════════════════════════════════════════════════
def clean_value(val):
    if not val:
        return None
    val = re.sub(r"\s*\([\d.]+\)", "", val)
    return val.strip(" .-").upper()


def truncate_dom(soup):
    """Safe copy of entry-content with only trailing noise removed.
    Retains Detailed Report / Analysis (metrics live there)."""
    raw = soup.find("div", class_="entry-content")
    if not raw:
        return None
    content = BeautifulSoup(str(raw), "html.parser")
    for tag in content.find_all(["h2", "h3", "h4", "h5"]):
        text = tag.get_text(strip=True).lower()
        if any(s in text for s in [
            "see also", "related sources", "latest ratings",
            "failed fact checks",
        ]):
            for sib in tag.find_all_next():
                sib.extract()
            tag.extract()
            break
    return content


# ── Line-isolated metric extraction ────────────────────────────────────────
_MAX_RATING_LINE = 120  # structured rating lines are always short


def _extract_metric_linewise(lines, label_re, whitelist=None):
    """Scan lines top → bottom for a 'Label: VALUE' pattern.

    Guards against false positives from analysis prose:
      • re.match  — label must appear at (or very near) the start of the line
      • length cap — paragraph sentences are always longer than rating lines
      • first-match-wins — the structured info box is above the analysis
    """
    for raw in lines:
        line = raw.strip()
        if not line or len(line) > _MAX_RATING_LINE:
            continue
        m = re.match(
            r"(?:[\u2022\-•*]\s*)?"       # optional bullet
            + label_re
            + r"\s*[:\-–—]\s*(.+?)\.?\s*$",
            line, re.I,
        )
        if not m:
            continue
        val = clean_value(m.group(1))
        if not val:
            continue
        if whitelist:
            for v in sorted(whitelist, key=len, reverse=True):
                if v in val:
                    return v.title()
            # Label matched but value not in whitelist — skip, keep scanning
            continue
        # Free-text metric (e.g. country)
        if len(val) <= 40:
            return val.title()
    return None


def _info_box_lines(content):
    """Return text lines from the 'info-box' region of entry-content —
    everything above the first <h2>–<h5> heading.

    On MBFC pages the structured ratings (Factual Reporting, Credibility,
    etc.) always appear in this region; the analytical prose that can
    cause false positives lives below the first heading."""
    pieces = []
    for child in content.children:
        # Stop at the first heading — analysis section begins
        if hasattr(child, "name") and child.name in ("h2", "h3", "h4", "h5"):
            break
        text = (child.get_text(separator="\n", strip=True)
                if hasattr(child, "get_text")
                else str(child).strip())
        if text:
            pieces.extend(text.split("\n"))
    return pieces


# ── Bias extraction from page ──────────────────────────────────────────────
def _normalize_bias_value(raw):
    """Normalize a raw bias string to one of the 9 canonical bias names."""
    if not raw:
        return None
    val = raw.upper().strip()
    val = re.sub(r"\s*\([\d./]+\)\s*", " ", val)  # strip "(3.5)" scores
    val = re.sub(r"\s+", " ", val).strip()
    for key in _BIAS_KEYS_BY_LEN:          # longest match wins
        if key in val:
            return BIAS_NORMALIZE[key]
    return None


def extract_page_bias(soup):
    """Extract the actual bias rating from an individual MBFC source page.

    Three strategies tried in order:
      1. Explicit "Bias Rating:" line
      2. "Overall, we rate …" summary sentence
      3. Bias-badge image alt-tags

    Returns a canonical bias string (e.g. "Right-Center") or None.
    """
    content = soup.find("div", class_="entry-content")
    if not content:
        return None
    text = content.get_text(separator="\n", strip=True)

    # ① "Bias Rating: LEFT-CENTER"
    m = re.search(
        r"(?:Media\s+)?Bias\s+Rating\s*[:\-–—]\s*([^\n]+)", text, re.I
    )
    if m:
        result = _normalize_bias_value(m.group(1))
        if result:
            return result

    # ② "Overall, we rate <source> <BIAS> Biased based on …"
    m = re.search(
        r"(?:Overall|In summary)[,\s]+we\s+rate\s+(.+?)(?:\.\s|\.\s*$)",
        text, re.I | re.MULTILINE,
    )
    if m:
        result = _normalize_bias_value(m.group(1))
        if result:
            return result

    # ③ Image alt-tags (only short ones mentioning "bias" etc.)
    for img in content.find_all("img"):
        alt = (img.get("alt") or "").strip()
        if not alt or len(alt) > 60:
            continue
        au = alt.upper()
        if any(kw in au for kw in (
            "BIAS", "QUESTIONABLE", "CONSPIRACY",
            "SATIRE", "PRO-SCIENCE", "PRO SCIENCE",
            "LEAST BIASED",
        )):
            result = _normalize_bias_value(alt)
            if result:
                return result

    return None


# ── Metrics extraction ─────────────────────────────────────────────────────
def scrape_metrics(soup):
    """Extract metrics using a two-phase line-isolated search.

    Phase 1 — Info-box region (above the first heading):
        Structured 'Label: VALUE' lines live here.  High confidence.

    Phase 2 — Full page (fallback):
        Still line-isolated (re.match + length cap), but allows pages
        with unusual formatting to be parsed.

    Phase 3 — Image alt-tags (last resort):
        Only the first 15 images are checked (info-box region).
    """
    content = soup.find("div", class_="entry-content")
    if not content:
        return {}

    full_text = content.get_text(separator="\n", strip=True)
    full_lines = [l for l in full_text.split("\n") if l.strip()]
    info_lines = _info_box_lines(content)

    def extract(label_re, whitelist=None, info_only=False):
        # Priority: info box (above first heading)
        result = _extract_metric_linewise(info_lines, label_re, whitelist)
        if result:
            return result
        # Fallback: full page (still line-isolated) — unless info_only
        if not info_only:
            return _extract_metric_linewise(full_lines, label_re, whitelist)
        return None

    metrics = {
        "f": extract(
            r"(?:Factual Reporting|Factuality Rating|Factuality|Factual Report)",
            VALID_FACTUALITY),
        "c": extract(
            r"(?:MBFC'?s?\s+Credibility\s+Rating|Credibility\s+Rating)",
            VALID_CREDIBILITY),
        "p": extract(
            r"(?:Country Freedom (?:Rating|Rank)|Press Freedom (?:Rating|Rank)"
            r"|Freedom of the Press (?:Rating|Rank)"
            r"|Freedom (?:Rating|Rank)|Press Freedom)",
            VALID_FREEDOM),
        "o": extract(
            r"(?:Country|Based in|Location)",
            info_only=True),    # never fall back to analysis text
    }

    # ── Phase 3: Image alt-tag fallback (first 15 images only) ──
    if not metrics["f"] or not metrics["c"]:
        for img in content.find_all("img")[:15]:
            alt = (img.get("alt") or "").upper().strip()
            if not alt or len(alt) > 60:
                continue
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

    metrics["o"] = normalize_country(metrics["o"])
    return {k: v for k, v in metrics.items() if v}


# ── Source key extraction ──────────────────────────────────────────────────
def extract_source_key(soup):
    """5 tag types × 3 patterns → then most-linked-domain fallback.

    Priority order:
      1. <a href> in a Source/URL line
      2. Raw URL in a Source/URL line
      3. Bare domain / (dot) notation in a Source/URL line
      4. Most-linked external domain across the page (≥2 links required)
    """
    content = soup.find("div", class_="entry-content")
    if not content:
        return None

    # ── Priorities 1–3: Source/URL line matching ──
    for el in content.find_all(["p", "div", "li", "span", "td"]):
        el_text = el.get_text(strip=True)
        if len(el_text) > 300:
            continue

        if not any(pat.match(el_text) for pat in _SOURCE_LINE):
            continue

        # Priority 1: hyperlink
        for link in el.find_all("a", href=True):
            key = source_key_from_url(link["href"])
            dom = root_domain_of_key(key) if key else None
            if key and dom and not _is_blacklisted(dom) and len(dom) > 3:
                return key

        # Priority 2: raw URL in text
        url_match = re.search(r"https?://[^\s<>\"')]+", el_text, re.I)
        if url_match:
            key = source_key_from_url(url_match.group(0).rstrip(".,;:"))
            dom = root_domain_of_key(key) if key else None
            if key and dom and not _is_blacklisted(dom) and len(dom) > 3:
                return key

        # Priority 3: bare domain / (dot) notation → normalize through
        #             source_key_from_url for consistent subdomain stripping
        m = re.search(r":\s*(.+)", el_text)
        if m:
            raw = m.group(1).strip()
            dotted = re.sub(r"\s*\(dot\)\s*", ".", raw, flags=re.I)
            dotted = dotted.lower().strip(" ./")
            if re.match(r"^[a-z0-9]([a-z0-9\-]*\.)+[a-z]{2,}(/[\w\-/]*)?$", dotted):
                key = source_key_from_url(f"https://{dotted}")
                dom = root_domain_of_key(key) if key else None
                if key and dom and not _is_blacklisted(dom) and len(dom) > 3:
                    return key

    # ── Priority 4: Most-linked external domain (fallback) ──
    # Use truncated content to exclude "Related Sources" / "See Also"
    scan_region = truncate_dom(soup) or content
    domain_counts = Counter()
    domain_best_key = {}     # domain → shortest (most canonical) key seen

    for a in scan_region.find_all("a", href=True):
        href = a["href"].strip()
        if not href.startswith("http"):
            continue
        key = source_key_from_url(href)
        if not key:
            continue
        dom = root_domain_of_key(key)
        if not dom or _is_blacklisted(dom) or len(dom) <= 3:
            continue
        domain_counts[dom] += 1
        # Prefer the root-domain key (shortest) for this domain
        if dom not in domain_best_key or len(key) < len(domain_best_key[dom]):
            domain_best_key[dom] = key

    if domain_counts:
        best_dom, best_count = domain_counts.most_common(1)[0]
        if best_count >= 2:    # require ≥2 links for confidence
            return domain_best_key[best_dom]

    return None


# ═══════════════════════════════════════════════════════════════════════════
#  DATABASE — atomic save + markdown statistics
# ═══════════════════════════════════════════════════════════════════════════
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

    # ── Generate statistics markdown ──
    counters = {"b": {}, "f": {}, "c": {}, "p": {}, "o": {}}
    valid = 0
    for key, entry in db.items():
        if key.startswith("_fail:"):
            continue
        valid += 1
        counters["b"][entry.get("b", "Unrated")]   = counters["b"].get(entry.get("b", "Unrated"), 0) + 1
        counters["f"][entry.get("f", "Unrated")]    = counters["f"].get(entry.get("f", "Unrated"), 0) + 1
        counters["c"][entry.get("c", "Unrated")]    = counters["c"].get(entry.get("c", "Unrated"), 0) + 1
        counters["p"][entry.get("p", "Unrated")]    = counters["p"].get(entry.get("p", "Unrated"), 0) + 1
        counters["o"][entry.get("o", "Unknown")]    = counters["o"].get(entry.get("o", "Unknown"), 0) + 1

    md = f"# 📊 Feed Ratings Statistics\n\n**Total Sources:** `{valid}`\n\n"
    sections = [
        ("⚖️ Bias", "b"), ("✅ Factuality", "f"),
        ("🛡️ Credibility", "c"), ("🗽 Press Freedom", "p"),
    ]
    for title, k in sections:
        md += f"### {title} Distribution\n| Category | Count |\n| :--- | :--- |\n"
        for name, ct in sorted(counters[k].items(), key=lambda x: x[1], reverse=True):
            md += f"| {name} | **{ct}** |\n"
        md += "\n"

    md += "### 🌍 Country Distribution (Top 30)\n| Country | Count |\n| :--- | :--- |\n"
    for name, ct in sorted(counters["o"].items(), key=lambda x: x[1], reverse=True)[:30]:
        md += f"| {name} | **{ct}** |\n"

    fd2, tmp2 = tempfile.mkstemp(dir=".", suffix=".tmp")
    try:
        with os.fdopen(fd2, "w", encoding="utf-8") as f:
            f.write(md)
        os.replace(tmp2, MD_FILE)
    except Exception:
        os.unlink(tmp2)
        raise


# ═══════════════════════════════════════════════════════════════════════════
#  FAILURE TRACKING — separate from main DB
# ═══════════════════════════════════════════════════════════════════════════
def load_failures():
    if os.path.exists(FAIL_FILE):
        try:
            with open(FAIL_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_failures(failures):
    """Save failures, expiring entries older than FAIL_EXPIRY."""
    now = int(time.time())
    live = {k: v for k, v in failures.items()
            if v.get("chk", 0) > now - FAIL_EXPIRY}
    fd, tmp = tempfile.mkstemp(dir=".", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(live, f, separators=(",", ":"))
        os.replace(tmp, FAIL_FILE)
    except Exception:
        os.unlink(tmp)
        raise


# ═══════════════════════════════════════════════════════════════════════════
#  MIGRATIONS — run once on startup
# ═══════════════════════════════════════════════════════════════════════════
def migrate_fail_keys(db):
    """Move _fail: entries from main DB → failures.json."""
    failures = load_failures()
    migrated = 0
    for key in list(db):
        if key.startswith("_fail:"):
            url = key[len("_fail:"):]
            failures[url] = db.pop(key)
            migrated += 1
    if migrated:
        save_failures(failures)
        print(f"  [migrate] Moved {migrated} _fail: entries → {FAIL_FILE}")
    return failures


def migrate_subdomain_keys(db):
    """Re-normalize all keys to collapse trivial-subdomain variants.

    e.g. mobile.reuters.com → reuters.com
         amp.cnn.com        → cnn.com
         edition.cnn.com    → cnn.com  (merges with above if both exist)
    """
    rekeyed = 0
    for old_key in list(db):
        if old_key.startswith("_fail:"):
            continue
        # Re-derive canonical key through current normalization
        new_key = source_key_from_url(f"https://{old_key}")
        if not new_key or new_key == old_key:
            continue
        # Merge: keep the entry with the most recent check
        if new_key in db:
            if db[old_key].get("chk", 0) > db[new_key].get("chk", 0):
                db[new_key] = db[old_key]
        else:
            db[new_key] = db[old_key]
        del db[old_key]
        rekeyed += 1
    if rekeyed:
        print(f"  [migrate] Normalized {rekeyed} subdomain keys")
    return rekeyed


# ═══════════════════════════════════════════════════════════════════════════
#  PHASE 1 — HARVEST all 9 categories
# ═══════════════════════════════════════════════════════════════════════════
def harvest_category(endpoint_url, bias_name):
    res = http.get(endpoint_url, kind="listing")
    if not res:
        print(f"  [✗] Failed: {bias_name}")
        return []

    soup = BeautifulSoup(res.text, "html.parser")
    content = truncate_dom(soup) or soup.find("table", id="mbfc-table")
    if not content:
        print(f"  [✗] No content: {bias_name}")
        return []

    urls, seen = [], set()
    for link in content.find_all("a", href=True):
        href = link["href"].strip().rstrip("/")
        if href in seen or not href.startswith("https://mediabiasfactcheck.com/"):
            continue
        parts = [p for p in urlparse(href).path.strip("/").split("/") if p]
        if len(parts) != 1:
            continue
        path = parts[0]
        if path not in IGNORE_PATHS and path not in TARGET_SLUGS:
            urls.append(href)
            seen.add(href)

    return urls


def harvest_all(db, failures):
    """Harvest all 9 categories, cross-reference with DB, build per-category
    work queues, and print a clear summary."""

    print("\n╔══════════════════════════════════════════════════════════════╗")
    print("║              PHASE 1 · HARVESTING ALL CATEGORIES           ║")
    print("╚══════════════════════════════════════════════════════════════╝\n")

    now = int(time.time())

    # Build reverse index: MBFC URL → source key (from existing DB)
    url_to_key = {}
    for key, entry in db.items():
        if not key.startswith("_fail:") and "u" in entry:
            url_to_key[entry["u"]] = key

    endpoints = list(TARGET_ENDPOINTS.items())
    random.shuffle(endpoints)

    # ── Scrape every category listing ──
    raw_harvest = {}  # bias_name → [mbfc_url, ...]
    for endpoint_url, bias_name in endpoints:
        if http.should_stop:
            print("  [!] Circuit breaker — stopping harvest")
            break
        if time_remaining() < 600:
            print("  [!] Time limit — stopping harvest")
            break
        urls = harvest_category(endpoint_url, bias_name)
        raw_harvest[bias_name] = urls
        print(f"  [✓] {bias_name:20s} → {len(urls):4d} sources scraped")

    if not any(raw_harvest.values()):
        return {}, url_to_key

    # ── Cross-reference against DB → classify every URL ──
    categories = {}  # bias_name → {"new": [], "stale": [], "fresh": int, "dead": int}

    grand_harvested = grand_fresh = grand_new = grand_stale = grand_dead = 0

    for bias_name, urls in raw_harvest.items():
        fresh = new = stale = dead = 0
        new_list, stale_list = [], []

        for u in urls:
            # Check permanent failures first
            if failures.get(u, {}).get("fails", 0) >= FAIL_MAX:
                dead += 1
                continue

            k = url_to_key.get(u)
            if k and k in db:
                entry = db[k]
                if entry.get("chk", 0) > now - RECHECK:
                    fresh += 1
                    # NOTE: Do NOT overwrite bias here.  The harvest
                    # category is unreliable (Related-Source links cause
                    # cross-category leakage).  Page-level extraction
                    # in Phase 2 is authoritative; fresh entries keep
                    # their verified bias until the next recheck.
                else:
                    # Stale — needs re-scrape
                    stale += 1
                    stale_list.append(u)
            else:
                # Never seen
                new += 1
                new_list.append(u)

        categories[bias_name] = {
            "new": new_list,
            "stale": stale_list,
            "fresh": fresh,
            "dead": dead,
        }

        grand_harvested += len(urls)
        grand_fresh += fresh
        grand_new += new
        grand_stale += stale
        grand_dead += dead

    # ── Print harvest summary ──
    print(f"\n  {'─' * 60}")
    print(f"  {'Category':<20s} {'Total':>6s} {'Fresh':>6s} {'New':>6s} {'Stale':>6s} {'Dead':>6s} {'Todo':>6s}")
    print(f"  {'─' * 60}")

    for bias_name in sorted(categories.keys()):
        info = categories[bias_name]
        total = info["fresh"] + len(info["new"]) + len(info["stale"]) + info["dead"]
        todo = len(info["new"]) + len(info["stale"])
        status = "  ✓" if todo == 0 else ""
        print(f"  {bias_name:<20s} {total:>6d} {info['fresh']:>6d} "
              f"{len(info['new']):>6d} {len(info['stale']):>6d} "
              f"{info['dead']:>6d} {todo:>6d}{status}")

    print(f"  {'─' * 60}")
    grand_todo = grand_new + grand_stale
    print(f"  {'TOTAL':<20s} {grand_harvested:>6d} {grand_fresh:>6d} "
          f"{grand_new:>6d} {grand_stale:>6d} {grand_dead:>6d} {grand_todo:>6d}")
    print(f"  {'─' * 60}")

    if grand_todo == 0:
        print("\n  ✅ All sources are fresh — nothing to do!")

    return categories, url_to_key


# ═══════════════════════════════════════════════════════════════════════════
#  PHASE 2 — PROCESS categories (smallest pending first)
# ═══════════════════════════════════════════════════════════════════════════
def process_category(db, bias_name, todo_urls, url_to_key, failures):
    """Process a single category's pending URLs (new first, then stale by age)."""
    now = int(time.time())

    # Sort: new (no key → tuple(0,0)) first, then stale by oldest check
    def sort_key(u):
        k = url_to_key.get(u)
        if not k:
            return (0, 0)
        return (1, db.get(k, {}).get("chk", 0))

    todo_urls.sort(key=sort_key)

    total = len(todo_urls)
    new_ct = upd_ct = 0
    processed_keys = set()

    for i, url in enumerate(todo_urls, 1):
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

        if not source_key:
            # Track failure in separate dict — never in main DB
            fail_entry = failures.setdefault(url, {"chk": 0, "fails": 0})
            fail_entry["fails"] += 1
            fail_entry["chk"] = now
            tag = "☠" if fail_entry["fails"] >= FAIL_MAX else "?"
            print(f"  [{i}/{total}] [{tag}] No source: {url}")
            if i % 25 == 0:
                save_database(db)
                save_failures(failures)
            continue
        else:
            # Source key found — clear any prior failure record
            failures.pop(url, None)

        if source_key in processed_keys:
            print(f"  [{i}/{total}] [dup] {source_key}")
            continue
        processed_keys.add(source_key)

        met = scrape_metrics(soup)

        # ── Authoritative bias: prefer page-level over harvest category ──
        page_bias = extract_page_bias(soup)
        effective_bias = page_bias if page_bias else bias_name

        if page_bias and page_bias != bias_name:
            print(f"  [{i}/{total}] [⚠ bias] harvest={bias_name} "
                  f"→ page={page_bias} | {source_key}")

        entry = {"u": url, "chk": now, "b": effective_bias}
        entry.update(met)

        if source_key in db:
            old = db[source_key]
            changed = any(old.get(k) != entry.get(k)
                          for k in ("b", "f", "c", "p", "o"))
            if changed:
                db[source_key] = entry
                upd_ct += 1
                print(f"  [{i}/{total}] [~] {source_key} | {effective_bias} "
                      f"| F:{met.get('f', '—')} C:{met.get('c', '—')} "
                      f"P:{met.get('p', '—')} O:{met.get('o', '—')}")
            else:
                db[source_key]["chk"] = now
                print(f"  [{i}/{total}] [-] {source_key}")
        else:
            db[source_key] = entry
            new_ct += 1
            url_to_key[url] = source_key
            print(f"  [{i}/{total}] [+] {source_key} | {effective_bias} "
                  f"| F:{met.get('f', '—')} C:{met.get('c', '—')} "
                  f"P:{met.get('p', '—')} O:{met.get('o', '—')}")

        if i % 25 == 0:
            save_database(db)

    # Final status
    done = i if not (http.should_stop or time_remaining() < 300) else i - 1
    remaining = total - done
    if remaining > 0:
        print(f"  ── {bias_name}: paused ({done}/{total} done, "
              f"{remaining} remaining) | +{new_ct} new ~{upd_ct} upd ──")
    else:
        print(f"  ── {bias_name}: complete ({total}/{total}) "
              f"| +{new_ct} new ~{upd_ct} upd ──")

    return new_ct, upd_ct


def process_all(db, categories, url_to_key, failures):
    """Process categories ordered by smallest pending count first,
    so small categories finish quickly and progress is maximised."""

    print("\n╔══════════════════════════════════════════════════════════════╗")
    print("║         PHASE 2 · PROCESSING (smallest pending first)      ║")
    print("╚══════════════════════════════════════════════════════════════╝")

    # Build ordered list: (bias_name, [todo_urls]) sorted by pending count
    cat_queue = []
    for bias_name, info in categories.items():
        todo = info["new"] + info["stale"]  # new first in the list already
        if todo:
            cat_queue.append((bias_name, todo))

    cat_queue.sort(key=lambda x: len(x[1]))

    if not cat_queue:
        print("\n  Nothing to process.")
        return 0, 0

    print("\n  Processing order (smallest → largest):")
    for bias_name, todo in cat_queue:
        new_in = len([u for u in todo if u not in url_to_key])
        stale_in = len(todo) - new_in
        print(f"    {bias_name:<20s}  {len(todo):>4d} pending  "
              f"({new_in} new + {stale_in} stale)")

    total_new = total_upd = 0

    for cat_idx, (bias_name, todo) in enumerate(cat_queue):
        if http.should_stop:
            print(f"\n  [!] Circuit breaker — stopping at category {cat_idx + 1}/{len(cat_queue)}")
            break
        if time_remaining() < 300:
            print(f"\n  [!] Time limit ({time_remaining() / 60:.0f} min left) — stopping")
            break

        print(f"\n  ┌── {bias_name} ({len(todo)} pending) ──")

        new_ct, upd_ct = process_category(db, bias_name, todo, url_to_key, failures)
        total_new += new_ct
        total_upd += upd_ct

        save_database(db)
        save_failures(failures)

        # Inter-category cooldown (skip if last or stopping)
        if time_remaining() > 300 and not http.should_stop and cat_idx < len(cat_queue) - 1:
            cd = random.uniform(30, 50)
            print(f"  [zZz] Inter-category pause ({cd:.0f}s)")
            time.sleep(cd)

    return total_new, total_upd


# ═══════════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════════
def main():
    db = load_database()

    # ── One-time migrations ──
    failures = migrate_fail_keys(db)       # _fail: keys → failures.json
    migrate_subdomain_keys(db)             # mobile.x.com → x.com
    save_database(db)                      # persist migrations + initial stats

    if not http.warmup():
        return

    # ── Phase 1: Harvest ──
    categories, url_to_key = harvest_all(db, failures)

    if not categories or http.should_stop:
        if http.should_stop:
            print("[!] Circuit breaker during harvest — saving and exiting")
        save_database(db)
        save_failures(failures)
        return

    # ── Pre-processing cooldown ──
    cooldown = random.uniform(60, 90)
    print(f"\n  [*] Pre-processing cooldown: {cooldown:.0f}s")
    time.sleep(cooldown)

    # ── Phase 2: Process ──
    total_new, total_upd = process_all(db, categories, url_to_key, failures)

    # ── Final save + summary ──
    save_database(db)
    save_failures(failures)
    valid = sum(1 for k in db if not k.startswith("_fail:"))
    elapsed = (time.time() - START_TIME) / 60

    print(f"\n{'═' * 62}")
    print(f"  ✅ Done in {elapsed:.0f} min")
    print(f"     Sources in DB : {valid}")
    print(f"     New added     : +{total_new}")
    print(f"     Updated       : ~{total_upd}")
    print(f"     HTTP requests : {http.request_count}")
    print(f"{'═' * 62}")


if __name__ == "__main__":
    main()

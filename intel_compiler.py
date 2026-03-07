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

TRIVIAL_PATHS = {
    "index.html", "index.php", "index.htm", "home", "main",
    "default.aspx", "default.htm", "wp", "blog",
}

TRIVIAL_SUBDOMAINS = {
    "www", "www1", "www2", "www3", "ww1", "ww2", "ww3",
    "m", "mobile", "amp",
    "edition",
    "en", "english",
    "beta", "secure",
    "web", "old", "new", "classic", "legacy",
    "feeds", "rss", "feed",
}

FAIL_FILE   = "failures.json"
FAIL_EXPIRY = 90 * 86400
FAIL_MAX    = 3

_SOURCE_LINE = [
    re.compile(r"Sources?\s*(?:URL)?\s*:", re.I),
    re.compile(r"(?:Source|Official)\s*Website\s*:", re.I),
    re.compile(r"Website\s*:", re.I),
    re.compile(r"Homepage\s*:", re.I),
    re.compile(r"URL\s*:", re.I),
]

# ── Bias normalisation ─────────────────────────────────────────────────────
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

# ── Special categories that override political lean ────────────────────────
# "LEFT SATIRE" → Satire (not Left).  These are MBFC "meta-categories"
# that take priority over any political-lean label in the same value.
_CATEGORY_KEYWORDS = [
    ("SATIRE",        "Satire"),
    ("PSEUDOSCIENCE", "Conspiracy"),
    ("CONSPIRACY",    "Conspiracy"),
    ("QUESTIONABLE",  "Questionable"),
    ("FAKE NEWS",     "Questionable"),
    ("PRO-SCIENCE",   "Pro-Science"),
    ("PRO SCIENCE",   "Pro-Science"),
]

# Political-lean values that can be overridden by a category indicator
_POLITICAL_LEAN = {"Left", "Left-Center", "Least Biased", "Right-Center", "Right"}

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

_COUNTRY_STOP_RE = re.compile(
    r'\s+(?:but|and|or|with|while|however|although|though|yet|also|'
    r'that|which|where|who|whose|as|for|from|by|at|'
    r'operates?|has|have|is|are|was|were|serves?|covers?|'
    r'focuses?|targets?|caters?|reports?|publishes?|according)\b',
    re.I,
)

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
    if not raw:
        return None
    truncated = _COUNTRY_STOP_RE.split(raw)[0].strip()
    truncated = re.sub(r"\([^)]*\)", "", truncated)
    truncated = re.sub(r"\s*[\d.]+\s*$", "", truncated)
    truncated = truncated.strip(" \t\n\r.,;:–—-/")
    if not truncated or len(truncated) > 50:
        return None
    wu = truncated.upper().strip()
    if wu in COUNTRY_DISCARD:
        return None
    if re.search(r'\b(US|USA|UNITED STATES|UNITED STATES OF AMERICA)\b', wu) \
       or re.search(r'(?<!\w)U\.S\.A?\.?(?!\w)', wu):
        return "USA"
    if re.search(r'\b(UK|UNITED KINGDOM|GREAT BRITAIN)\b', wu) \
       or re.search(r'(?<!\w)U\.K\.?(?!\w)', wu):
        return "United Kingdom"
    if wu in COUNTRY_MANUAL:
        return COUNTRY_MANUAL[wu]
    cosmetic = (truncated.strip().title()
                .replace(" And ", " and ")
                .replace(" Of ", " of ")
                .replace(" The ", " the "))
    try:
        country = pycountry.countries.lookup(cosmetic)
        return _PYCOUNTRY_OVERRIDES.get(country.name, country.name)
    except LookupError:
        pass
    try:
        country = pycountry.countries.lookup(truncated.strip())
        return _PYCOUNTRY_OVERRIDES.get(country.name, country.name)
    except LookupError:
        pass
    if cosmetic in _KNOWN_COUNTRIES:
        return cosmetic
    return None


# ═══════════════════════════════════════════════════════════════════════════
#  HTTP CLIENT
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
#  EXTRACTION
# ═══════════════════════════════════════════════════════════════════════════
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
            "see also", "related sources", "latest ratings",
            "failed fact checks",
        ]):
            for sib in tag.find_all_next():
                sib.extract()
            tag.extract()
            break
    return content


_MAX_RATING_LINE = 120

_DATA_HEADINGS = {
    "detailed report", "rating information", "ratings",
    "source overview", "overview", "bias rating",
    "detailed information", "source information",
    "factual reporting", "credibility rating",
}

_PROSE_HEADINGS = {
    "analysis", "reasoning", "analysis / reasoning", "analysis/reasoning",
    "history", "funded by", "funding", "editorial review",
    "see also", "related sources", "latest ratings", "failed fact checks",
    "overall", "verdict", "summary",
}


def _info_box_lines(content):
    """Return text lines from the structured-data region.
    Continues past known data headings, stops at known prose headings,
    continues past unknown headings."""
    pieces = []
    for child in content.children:
        if hasattr(child, "name") and child.name in ("h2", "h3", "h4", "h5"):
            heading_text = child.get_text(strip=True).lower().strip()
            if any(dh in heading_text for dh in _DATA_HEADINGS):
                continue
            if any(ph in heading_text for ph in _PROSE_HEADINGS):
                break
            continue
        text = (child.get_text(separator="\n", strip=True)
                if hasattr(child, "get_text")
                else str(child).strip())
        if text:
            pieces.extend(text.split("\n"))
    return pieces


def _extract_metric_linewise(lines, label_re, whitelist=None):
    for raw in lines:
        line = raw.strip()
        if not line or len(line) > _MAX_RATING_LINE:
            continue
        m = re.match(
            r"(?:[\u2022\-•*]\s*)?"
            r"(?:MBFC'?s?\s+)?"
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
            continue
        if len(val) <= 40:
            return val.title()
    return None


# ── Bias extraction ────────────────────────────────────────────────────────
def _normalize_bias_value(raw):
    """Normalize a raw bias string.  Category keywords (SATIRE, CONSPIRACY,
    etc.) are checked FIRST so 'LEFT SATIRE' → 'Satire', not 'Left'."""
    if not raw:
        return None
    val = raw.upper().strip()
    val = re.sub(r"\s*\([\d./]+\)\s*", " ", val)
    val = re.sub(r"\s+", " ", val).strip()

    # Category keywords ALWAYS override political lean
    for keyword, category in _CATEGORY_KEYWORDS:
        if keyword in val:
            return category

    for key in _BIAS_KEYS_BY_LEN:
        if key in val:
            return BIAS_NORMALIZE[key]
    return None


def _bias_from_img_alt(content):
    """Two-pass image alt scan: keyword-gated then ungated short alts."""
    for img in content.find_all("img"):
        alt = (img.get("alt") or "").strip()
        if not alt or len(alt) > 60:
            continue
        au = alt.upper()
        if any(kw in au for kw in (
            "BIAS", "BIASED", "QUESTIONABLE", "CONSPIRACY",
            "PSEUDOSCIENCE", "SATIRE", "PRO-SCIENCE", "PRO SCIENCE",
            "LEAST BIASED", "FAKE NEWS",
        )):
            result = _normalize_bias_value(alt)
            if result:
                return result
    for img in content.find_all("img"):
        alt = (img.get("alt") or "").strip()
        if not alt or len(alt) > 25:
            continue
        result = _normalize_bias_value(alt)
        if result:
            return result
    return None


def _bias_from_img_src(content):
    """Last-resort: bias from MBFC badge image filenames."""
    for img in content.find_all("img"):
        src = (img.get("src") or img.get("data-src") or "").lower()
        if not src or "mediabiasfactcheck" not in src:
            continue
        fname = src.split("/")[-1].split("?")[0]
        fname = re.sub(r"\.\w+$", "", fname)
        fname = fname.replace("-", " ").replace("_", " ")
        result = _normalize_bias_value(fname)
        if result:
            return result
    return None


def _detect_category_override(content):
    """Scan structured data for special-category indicators.

    MBFC uses compound ratings like 'LEFT SATIRE'. When split across
    lines or HTML elements, the initial extraction may only capture
    the political lean. This scans info-box lines and images for the
    category part (SATIRE, CONSPIRACY, QUESTIONABLE, PRO-SCIENCE).
    """
    # Scan info-box text lines
    info_lines = _info_box_lines(content)
    for line in info_lines:
        lu = line.strip().upper()
        if not lu or len(lu) > 50:
            continue
        for keyword, category in _CATEGORY_KEYWORDS:
            if keyword in lu:
                return category

    # Scan image alt tags
    for img in content.find_all("img")[:20]:
        alt = (img.get("alt") or "").upper().strip()
        if not alt or len(alt) > 60:
            continue
        for keyword, category in _CATEGORY_KEYWORDS:
            if keyword in alt:
                return category

    # Scan MBFC badge image filenames
    for img in content.find_all("img")[:20]:
        src = (img.get("src") or img.get("data-src") or "").lower()
        if not src or "mediabiasfactcheck" not in src:
            continue
        fname = src.split("/")[-1].split("?")[0]
        fu = fname.replace("-", " ").replace("_", " ").upper()
        for keyword, category in _CATEGORY_KEYWORDS:
            if keyword in fu:
                return category

    return None


def extract_page_bias(soup):
    """Extract bias rating from an MBFC source page.

    Strategies tried in order:
      ① 'Bias Rating:' line (with next-line peek for compound ratings)
      ② 'Overall, we rate …' summary sentence
      ③④ Image alt-tags (keyword-gated, then ungated short)
      ⑤ Image src filenames
      ⑥ Category override: if result is a political lean but the page
         also contains a special-category indicator (SATIRE, CONSPIRACY,
         etc.), the category overrides the lean.
    """
    content = soup.find("div", class_="entry-content")
    if not content:
        return None
    text = content.get_text(separator="\n", strip=True)

    result = None

    # ① "Bias Rating: LEFT-CENTER" (with next-line peek for "LEFT\nSATIRE")
    if not result:
        m = re.search(
            r"(?:Media\s+)?Bias\s+Rating\s*[:\-–—]\s*([^\n]+)", text, re.I
        )
        if m:
            captured = m.group(1).strip()
            # Peek at the next line: if short, try combining
            # (handles "LEFT\nSATIRE" split across lines/elements)
            after = text[m.end():]
            next_m = re.match(r"\s*\n\s*([A-Za-z][^\n]{0,24})\s*(?:\n|$)", after)
            if next_m:
                combined = captured + " " + next_m.group(1).strip()
                result = _normalize_bias_value(combined)
            if not result:
                result = _normalize_bias_value(captured)

    # ② "Overall, we rate <source> <BIAS> based on …"
    if not result:
        m = re.search(
            r"(?:Overall|In summary)[,\s]+we\s+rate\s+(.+?)(?:\.(?:\s|$)|$)",
            text, re.I | re.MULTILINE,
        )
        if m:
            result = _normalize_bias_value(m.group(1))

    # ③④ Image alt-tags
    if not result:
        result = _bias_from_img_alt(content)

    # ⑤ Image src filenames
    if not result:
        result = _bias_from_img_src(content)

    # ⑥ Category override: special categories beat political lean
    # Handles the case where ① captured only "LEFT" but the page
    # has a SATIRE badge/label/image elsewhere in structured data
    if result and result in _POLITICAL_LEAN:
        override = _detect_category_override(content)
        if override:
            result = override

    return result


# ── Metrics extraction ─────────────────────────────────────────────────────
def _metrics_from_images(content, metrics):
    need_f = not metrics.get("f")
    need_c = not metrics.get("c")
    need_p = not metrics.get("p")
    if not (need_f or need_c or need_p):
        return
    for img in content.find_all("img"):
        alt = (img.get("alt") or "").upper().strip()
        if not alt or len(alt) > 80:
            continue
        if need_f:
            for v in sorted(VALID_FACTUALITY, key=len, reverse=True):
                if v in alt:
                    metrics["f"] = v.title()
                    need_f = False
                    break
        if need_c:
            for v in sorted(VALID_CREDIBILITY, key=len, reverse=True):
                if v in alt:
                    metrics["c"] = v.title()
                    need_c = False
                    break
        if need_p:
            for v in sorted(VALID_FREEDOM, key=len, reverse=True):
                if v in alt:
                    metrics["p"] = v.title()
                    need_p = False
                    break
        if not (need_f or need_c or need_p):
            break


def _metrics_from_img_src(content, metrics):
    need_f = not metrics.get("f")
    need_c = not metrics.get("c")
    need_p = not metrics.get("p")
    if not (need_f or need_c or need_p):
        return
    for img in content.find_all("img"):
        src = (img.get("src") or img.get("data-src") or "").lower()
        if not src or "mediabiasfactcheck" not in src:
            continue
        fname = src.split("/")[-1].split("?")[0]
        fname = re.sub(r"\.\w+$", "", fname)
        fu = fname.replace("-", " ").replace("_", " ").upper()
        if need_f:
            for v in sorted(VALID_FACTUALITY, key=len, reverse=True):
                if v in fu:
                    metrics["f"] = v.title()
                    need_f = False
                    break
        if need_c:
            for v in sorted(VALID_CREDIBILITY, key=len, reverse=True):
                if v in fu:
                    metrics["c"] = v.title()
                    need_c = False
                    break
        if need_p:
            for v in sorted(VALID_FREEDOM, key=len, reverse=True):
                if v in fu:
                    metrics["p"] = v.title()
                    need_p = False
                    break
        if not (need_f or need_c or need_p):
            break


def _metrics_from_summary(full_lines, metrics):
    if metrics.get("f"):
        return
    for line in full_lines:
        line = line.strip()
        if len(line) > 300 or len(line) < 20:
            continue
        m = re.match(
            r"(?:Overall|In summary)[,\s]+we\s+rate\s+.+?"
            r"(?:factual(?:ly)?|reporting)\s+(?:as\s+)?(\w[\w\s]*?)"
            r"(?:\.|\s+based|\s+due|\s+because|$)",
            line, re.I,
        )
        if m:
            val = clean_value(m.group(1))
            if val:
                for v in sorted(VALID_FACTUALITY, key=len, reverse=True):
                    if v in val:
                        metrics["f"] = v.title()
                        return


def scrape_metrics(soup):
    content = soup.find("div", class_="entry-content")
    if not content:
        return {}

    full_text = content.get_text(separator="\n", strip=True)
    full_lines = [l for l in full_text.split("\n") if l.strip()]
    info_lines = _info_box_lines(content)

    def extract(label_re, whitelist=None, info_only=False):
        result = _extract_metric_linewise(info_lines, label_re, whitelist)
        if result:
            return result
        if not info_only:
            return _extract_metric_linewise(full_lines, label_re, whitelist)
        return None

    metrics = {
        "f": extract(
            r"(?:Factual Reporting|Factuality Rating|Factuality|Factual Report)",
            VALID_FACTUALITY),
        "c": extract(
            r"Credibility\s+Rating",
            VALID_CREDIBILITY),
        "p": extract(
            r"(?:Country Freedom (?:Rating|Rank)|Press Freedom (?:Rating|Rank)"
            r"|Freedom of the Press (?:Rating|Rank)"
            r"|Freedom (?:Rating|Rank)|Press Freedom)",
            VALID_FREEDOM),
        "o": extract(
            r"(?:Country|Based in|Location)",
            info_only=True),
    }

    _metrics_from_images(content, metrics)
    _metrics_from_img_src(content, metrics)
    _metrics_from_summary(full_lines, metrics)

    metrics["o"] = normalize_country(metrics["o"])
    return {k: v for k, v in metrics.items() if v}


# ── Source key extraction ──────────────────────────────────────────────────
def extract_source_key(soup):
    content = soup.find("div", class_="entry-content")
    if not content:
        return None

    for el in content.find_all(["p", "div", "li", "span", "td"]):
        el_text = el.get_text(strip=True)
        if len(el_text) > 300:
            continue
        if not any(pat.match(el_text) for pat in _SOURCE_LINE):
            continue
        for link in el.find_all("a", href=True):
            key = source_key_from_url(link["href"])
            dom = root_domain_of_key(key) if key else None
            if key and dom and not _is_blacklisted(dom) and len(dom) > 3:
                return key
        url_match = re.search(r"https?://[^\s<>\"')]+", el_text, re.I)
        if url_match:
            key = source_key_from_url(url_match.group(0).rstrip(".,;:"))
            dom = root_domain_of_key(key) if key else None
            if key and dom and not _is_blacklisted(dom) and len(dom) > 3:
                return key
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

    scan_region = truncate_dom(soup) or content
    domain_counts = Counter()
    domain_best_key = {}
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
        if dom not in domain_best_key or len(key) < len(domain_best_key[dom]):
            domain_best_key[dom] = key
    if domain_counts:
        best_dom, best_count = domain_counts.most_common(1)[0]
        if best_count >= 2:
            return domain_best_key[best_dom]

    return None


# ═══════════════════════════════════════════════════════════════════════════
#  DATABASE
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
#  FAILURE TRACKING
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
#  MIGRATIONS
# ═══════════════════════════════════════════════════════════════════════════
def migrate_fail_keys(db):
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
    rekeyed = 0
    for old_key in list(db):
        if old_key.startswith("_fail:"):
            continue
        new_key = source_key_from_url(f"https://{old_key}")
        if not new_key or new_key == old_key:
            continue
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
#  PHASE 1 — HARVEST
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
    print("\n╔══════════════════════════════════════════════════════════════╗")
    print("║              PHASE 1 · HARVESTING ALL CATEGORIES           ║")
    print("╚══════════════════════════════════════════════════════════════╝\n")

    now = int(time.time())
    url_to_key = {}
    for key, entry in db.items():
        if not key.startswith("_fail:") and "u" in entry:
            url_to_key[entry["u"]] = key

    endpoints = list(TARGET_ENDPOINTS.items())
    random.shuffle(endpoints)

    raw_harvest = {}
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

    categories = {}
    grand_harvested = grand_fresh = grand_new = grand_stale = grand_dead = 0

    for bias_name, urls in raw_harvest.items():
        fresh = new = stale = dead = 0
        new_list, stale_list = [], []
        for u in urls:
            if failures.get(u, {}).get("fails", 0) >= FAIL_MAX:
                dead += 1
                continue
            k = url_to_key.get(u)
            if k and k in db:
                entry = db[k]
                if entry.get("chk", 0) > now - RECHECK:
                    fresh += 1
                else:
                    stale += 1
                    stale_list.append(u)
            else:
                new += 1
                new_list.append(u)

        categories[bias_name] = {
            "new": new_list, "stale": stale_list,
            "fresh": fresh, "dead": dead,
        }
        grand_harvested += len(urls)
        grand_fresh += fresh
        grand_new += new
        grand_stale += stale
        grand_dead += dead

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
#  PHASE 2 — PROCESS
# ═══════════════════════════════════════════════════════════════════════════
def process_category(db, bias_name, todo_urls, url_to_key, failures):
    now = int(time.time())

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
            failures.pop(url, None)

        if source_key in processed_keys:
            print(f"  [{i}/{total}] [dup] {source_key}")
            continue
        processed_keys.add(source_key)

        met = scrape_metrics(soup)

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
    print("\n╔══════════════════════════════════════════════════════════════╗")
    print("║         PHASE 2 · PROCESSING (smallest pending first)      ║")
    print("╚══════════════════════════════════════════════════════════════╝")

    cat_queue = []
    for bias_name, info in categories.items():
        todo = info["new"] + info["stale"]
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
    failures = migrate_fail_keys(db)
    migrate_subdomain_keys(db)
    save_database(db)

    if not http.warmup():
        return

    categories, url_to_key = harvest_all(db, failures)

    if not categories or http.should_stop:
        if http.should_stop:
            print("[!] Circuit breaker during harvest — saving and exiting")
        save_database(db)
        save_failures(failures)
        return

    cooldown = random.uniform(60, 90)
    print(f"\n  [*] Pre-processing cooldown: {cooldown:.0f}s")
    time.sleep(cooldown)

    total_new, total_upd = process_all(db, categories, url_to_key, failures)

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

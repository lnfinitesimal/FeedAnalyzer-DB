import requests
from bs4 import BeautifulSoup
import json
import time
import re
from urllib.parse import urlparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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

session = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
session.mount('http://', HTTPAdapter(max_retries=retries))
session.mount('https://', HTTPAdapter(max_retries=retries))
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

def get_root_domain(url_string):
    try:
        return urlparse(url_string).netloc.replace('www.', '').lower()
    except:
        return None

def normalize_data(value, is_country=False, is_bias=False):
    if not value: return None
    v = value.strip()
    v_upper = v.upper()
    
    if v_upper in ["", "UNKNOWN", "N/A", "NONE", "UNRATED"]:
        return None
        
    if is_country:
        def fix_country_word(w):
            w = w.strip()
            w_up = w.upper()
            if w_up in["US", "U.S.", "USA", "U.S.A.", "UNITED STATES", "UNITED STATES OF AMERICA"]: return "USA"
            if w_up in["UK", "U.K.", "UNITED KINGDOM", "GREAT BRITAIN"]: return "UK"
            if w_up in["UAE", "EU"]: return w_up
            return w.title() if len(w) > 3 else w_up
            
        if ',' in v:
            return ', '.join([fix_country_word(c) for c in v.split(',')])
        return fix_country_word(v)
        
    if is_bias:
        if "SATIRE" in v_upper: return "Satire"
        if "PRO-SCIENCE" in v_upper or "SCIENCE" in v_upper: return "Pro-Science"
        if "CONSPIRACY" in v_upper or "PSEUDOSCIENCE" in v_upper: return "Conspiracy"
        if "QUESTIONABLE" in v_upper or "FAKE NEWS" in v_upper: return "Questionable"
        
    return v.title()

def analyze_article(article_url):
    try:
        res = session.get(article_url, headers=headers, timeout=15)
        if res.status_code != 200: return None
            
        soup = BeautifulSoup(res.text, 'html.parser')
        source_link_tag = soup.find('a', string=re.compile('Source:', re.IGNORECASE))
        if not source_link_tag: return None
            
        domain = get_root_domain(source_link_tag.get('href'))
        if not domain or len(domain) < 3: return None
            
        clean_text = soup.get_text(separator=' ')
        clean_text = re.sub(r'\s+', ' ', clean_text)
        
        stop_keywords = r"(?:Bias Rating|Factual Reporting|Factuality|Country|Press Freedom|MBFC Credibility|Media Type|Traffic|World Press|$)"
        
        def pull_metric(keyword):
            match = re.search(rf"{keyword}\s*:\s*(.*?)(?=\s*(?:{stop_keywords}))", clean_text, re.IGNORECASE)
            if match:
                return match.group(1).replace('\u00a0', ' ').strip().rstrip('.')
            return None

        b = normalize_data(pull_metric(r"(?:Bias Rating|Bias)"), is_bias=True)
        f = normalize_data(pull_metric(r"(?:Factual Reporting|Factuality)"))
        c = normalize_data(pull_metric(r"(?:MBFC Credibility Rating|Credibility Rating|Credibility)"))
        p = normalize_data(pull_metric(r"(?:Press Freedom Rating|Press Freedom Rank|Press Freedom)"))
        o = normalize_data(pull_metric(r"Country"), is_country=True)

        metrics = {"u": article_url}
        if b: metrics["b"] = b
        if f: metrics["f"] = f
        if c: metrics["c"] = c
        if p: metrics["p"] = p
        if o: metrics["o"] = o

        return domain, metrics
    except Exception as e:
        return None

def main():
    feed_analytics = {}
    visited_urls = set()
    
    for endpoint in TARGET_ENDPOINTS:
        print(f"\n[+] Scanning Category: {endpoint}")
        try:
            res = session.get(endpoint, headers=headers, timeout=15)
            soup = BeautifulSoup(res.text, 'html.parser')
            links = soup.find_all('a')
            
            for link in links:
                href = link.get('href', '').strip()
                if href.endswith('/'): href = href[:-1]
                
                if 'mediabiasfactcheck.com' in href and href not in visited_urls:
                    if any(cat.strip('/') in href for cat in TARGET_ENDPOINTS) or href == "https://mediabiasfactcheck.com":
                        continue
                        
                    visited_urls.add(href)
                    time.sleep(0.5) # The golden 0.5s anti-ban delay
                    
                    result = analyze_article(href)
                    if result:
                        domain, metrics = result
                        if domain not in feed_analytics:
                            feed_analytics[domain] = metrics
                            print(f"  -> {domain} | Bias: {metrics.get('b', 'N/A')} | Fact: {metrics.get('f', 'N/A')}")
                            
        except Exception as e:
            print(f"Error on {endpoint}: {e}")

    with open('feed_intelligence.json', 'w', encoding='utf-8') as f:
        json.dump(feed_analytics, f, separators=(',', ':'))
        print(f"\n[✓] Database compiled securely with {len(feed_analytics)} sources.")

if __name__ == "__main__":
    main()

"""
News-Based Paddy Price Scraper
================================
Scrapes paddy price mentions from Sri Lanka agricultural news websites.
Targets:
  - https://www.hirunews.lk     (English: "paddy price")
  - https://www.lankadeepa.lk   (Sinhala: "වී මිල")
  - https://www.dinamina.lk     (Sinhala: "paddy")
  - https://newsfirst.lk        (English: "paddy price")

Also detects mill buying prices (Araliya, Nipuna, New Rathna) when named.
Uses regex to extract LKR/kg price figures.
Output: data/raw/news_prices.csv
  columns: date, district, variety, price_lkr_per_kg, source_url, confidence
"""

import os
import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import date, datetime

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
    )
}

START_DATE = date(2022, 1, 1)
END_DATE   = date(2024, 12, 31)

# ─── District name patterns ─────────────────────────────────────────────────
DISTRICTS = {
    "Ampara": ["ampara"], "Anuradhapura": ["anuradhapura"],
    "Badulla": ["badulla"], "Batticaloa": ["batticaloa"],
    "Colombo": ["colombo"], "Galle": ["galle"],
    "Gampaha": ["gampaha"], "Hambantota": ["hambantota"],
    "Jaffna": ["jaffna"], "Kalutara": ["kalutara"],
    "Kandy": ["kandy"], "Kegalle": ["kegalle"],
    "Kilinochchi": ["kilinochchi"], "Kurunegala": ["kurunegala"],
    "Mannar": ["mannar"], "Matale": ["matale"],
    "Matara": ["matara"], "Monaragala": ["monaragala"],
    "Mullaitivu": ["mullaitivu"], "Nuwara Eliya": ["nuwara eliya", "nuwaraeliya"],
    "Polonnaruwa": ["polonnaruwa"], "Puttalam": ["puttalam"],
    "Ratnapura": ["ratnapura"], "Trincomalee": ["trincomalee"],
    "Vavuniya": ["vavuniya"],
}

# ─── Variety keywords ───────────────────────────────────────────────────────
VARIETIES = {
    "samba":       ["samba"],
    "nadu":        ["nadu"],
    "keeri_samba": ["keeri samba", "keeri", "kiri samba"],
}

# ─── Mill company keywords ───────────────────────────────────────────────────
MILLS = {
    "araliya":    ["araliya"],
    "nipuna":     ["nipuna"],
    "new_rathna": ["new rathna", "rathna"],
}

# ─── Price extraction regexes ───────────────────────────────────────────────
PRICE_PATTERNS = [
    # "Rs. 125 per kg" or "Rs.125/kg"
    re.compile(r"Rs\.?\s*(\d{2,3}(?:\.\d{1,2})?)\s*(?:per\s*)?(?:kg|kilo)", re.IGNORECASE),
    # "125 rupees per kg"
    re.compile(r"(\d{2,3}(?:\.\d{1,2})?)\s+rupees?\s+per\s+kg", re.IGNORECASE),
    # "LKR 125/kg"
    re.compile(r"LKR\s*(\d{2,3}(?:\.\d{1,2})?)\s*(?:/\s*kg)?", re.IGNORECASE),
    # "125 per kg"
    re.compile(r"(\d{2,3}(?:\.\d{1,2})?)\s+per\s+kg", re.IGNORECASE),
    # Sinhala pattern: "රු. 125" or "රු125"
    re.compile(r"රු\.?\s*(\d{2,3}(?:\.\d{1,2})?)"),
]

# ─── Date extraction ─────────────────────────────────────────────────────────
DATE_PATTERN = re.compile(
    r"(\d{4})[/-](\d{2})[/-](\d{2})"   # YYYY-MM-DD
    r"|(\d{2})[/-](\d{2})[/-](\d{4})"  # DD-MM-YYYY
    r"|(\w+ \d{1,2},? \d{4})"          # "January 15, 2023"
)

MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def parse_date(text: str):
    """Try to extract a date from text. Returns ISO date string or None."""
    m = DATE_PATTERN.search(text)
    if not m:
        return None
    try:
        if m.group(1):   # YYYY-MM-DD
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        if m.group(4):   # DD-MM-YYYY
            return f"{m.group(6)}-{m.group(5)}-{m.group(4)}"
        if m.group(7):   # "January 15, 2023"
            parts = m.group(7).replace(",", "").split()
            month = MONTH_MAP.get(parts[0].lower())
            if month:
                return f"{parts[2]}-{month:02d}-{int(parts[1]):02d}"
    except (IndexError, ValueError, TypeError):
        pass
    return None


def extract_prices(text: str) -> list:
    """Extract all plausible price values from text."""
    prices = []
    for pat in PRICE_PATTERNS:
        for m in pat.finditer(text):
            try:
                val = float(m.group(1))
                if 50 <= val <= 500:   # sanity range for paddy LKR/kg
                    prices.append(round(val, 2))
            except (ValueError, IndexError):
                pass
    return list(set(prices))


def detect_district(text: str):
    t = text.lower()
    for district, keywords in DISTRICTS.items():
        for kw in keywords:
            if kw in t:
                return district
    return None


def detect_variety(text: str):
    t = text.lower()
    for variety, keywords in VARIETIES.items():
        for kw in keywords:
            if kw in t:
                return variety
    return None


def detect_mill(text: str):
    t = text.lower()
    for mill, keywords in MILLS.items():
        for kw in keywords:
            if kw in t:
                return mill
    return None


def compute_confidence(variety, district, price, source_url) -> float:
    """Heuristic confidence score 0.0–1.0."""
    score = 0.5
    if variety: score += 0.2
    if district: score += 0.15
    if 80 <= price <= 200: score += 0.15   # most realistic paddy price range
    return min(round(score, 2), 1.0)


# ─── Source-specific scrapers ────────────────────────────────────────────────

def scrape_hirunews() -> list:
    """Scrape Hiru News for paddy price articles."""
    rows = []
    search_urls = [
        "https://www.hirunews.lk/en/agriculture",
        "https://www.hirunews.lk/local-news/agriculture",
    ]
    keyword_urls = [
        "https://www.hirunews.lk/395338/farmers-demand-guaranteed-price-for-paddy-amid-falling-market-rates",
        "https://www.hirunews.lk/410168/farmers-unhappy-with-government-vetted-price-for-paddy",
        "https://www.hirunews.lk/en/410168/farmers-unhappy-with-government-vetted-price-for-paddy",
    ]

    article_links = set(keyword_urls)

    # Try to find more from index pages
    for url in search_urls:
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "lxml")
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    txt  = a.get_text(" ", strip=True).lower()
                    if any(kw in txt for kw in ["paddy", "paddy price", "guaranteed price"]):
                        full = href if href.startswith("http") else "https://www.hirunews.lk" + href
                        article_links.add(full)
        except Exception:
            pass

    for art_url in list(article_links)[:30]:
        try:
            resp = requests.get(art_url, headers=HEADERS, timeout=15)
            if resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.text, "lxml")
            # Get article date
            date_str = None
            for tag in soup.find_all(["time", "span", "div"], class_=re.compile("date|time|pub", re.I)):
                date_str = parse_date(tag.get_text())
                if date_str:
                    break
            if not date_str:
                date_str = parse_date(resp.text[:5000])

            article_text = soup.get_text(" ", strip=True)
            if not any(kw in article_text.lower() for kw in ["paddy", "paddy price"]):
                continue

            prices   = extract_prices(article_text)
            variety  = detect_variety(article_text)
            district = detect_district(article_text)
            mill     = detect_mill(article_text)

            for price in prices:
                rows.append({
                    "date":           date_str or "unknown",
                    "district":       district,
                    "variety":        variety,
                    "mill":           mill,
                    "price_lkr_per_kg": price,
                    "source_url":     art_url,
                    "confidence":     compute_confidence(variety, district, price, art_url),
                })
            time.sleep(0.5)
        except Exception as e:
            print(f"  [WARN] hirunews: {e}")

    print(f"[INFO] Hiru News: {len(rows)} price records from {len(article_links)} articles")
    return rows


def scrape_lankadeepa() -> list:
    """Scrape Lankadeepa (Sinhala) for paddy price mentions."""
    rows = []
    search_url = "https://www.lankadeepa.lk/latest_news/search"
    try:
        params = {"keyword": "වී මිල"}
        resp = requests.get(search_url, params=params, headers=HEADERS, timeout=15)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "lxml")
            for article in soup.find_all("article"):
                text = article.get_text(" ", strip=True)
                date_str = parse_date(text)
                prices   = extract_prices(text)
                variety  = detect_variety(text)
                district = detect_district(text)
                for price in prices:
                    rows.append({
                        "date":             date_str or "unknown",
                        "district":         district,
                        "variety":          variety,
                        "mill":             None,
                        "price_lkr_per_kg": price,
                        "source_url":       "lankadeepa.lk",
                        "confidence":       compute_confidence(variety, district, price, "lankadeepa"),
                    })
    except Exception as e:
        print(f"  [WARN] lankadeepa: {e}")

    print(f"[INFO] Lankadeepa: {len(rows)} price records")
    return rows


def scrape_newsfirst() -> list:
    """Scrape Newsfirst.lk for paddy price articles."""
    rows = []
    urls = [
        "https://www.newsfirst.lk/?s=paddy+price",
        "https://www.newsfirst.lk/%3Fs=paddy+guaranteed+price",
    ]
    seen_articles = set()
    for search_url in urls:
        try:
            resp = requests.get(search_url, headers=HEADERS, timeout=15)
            if resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.text, "lxml")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "paddy" in href.lower() or "paddy" in a.get_text().lower():
                    full = href if href.startswith("http") else "https://www.newsfirst.lk" + href
                    seen_articles.add(full)
        except Exception:
            pass

    for art_url in list(seen_articles)[:20]:
        try:
            resp = requests.get(art_url, headers=HEADERS, timeout=15)
            if resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.text, "lxml")
            text     = soup.get_text(" ", strip=True)
            date_str = parse_date(text[:3000])
            prices   = extract_prices(text)
            variety  = detect_variety(text)
            district = detect_district(text)
            for price in prices:
                rows.append({
                    "date": date_str or "unknown",
                    "district": district,
                    "variety": variety,
                    "mill": detect_mill(text),
                    "price_lkr_per_kg": price,
                    "source_url": art_url,
                    "confidence": compute_confidence(variety, district, price, art_url),
                })
            time.sleep(0.3)
        except Exception:
            pass

    print(f"[INFO] Newsfirst: {len(rows)} price records")
    return rows


def main():
    out_dir  = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "news_prices.csv")

    print("[INFO] Scraping news sources for paddy prices...")
    all_rows = []
    all_rows.extend(scrape_hirunews())
    all_rows.extend(scrape_lankadeepa())
    all_rows.extend(scrape_newsfirst())

    if not all_rows:
        print("[WARN] No news price data found — saving empty CSV")
        pd.DataFrame(columns=["date","district","variety","mill",
                               "price_lkr_per_kg","source_url","confidence"]
                     ).to_csv(out_path, index=False)
        return

    df = pd.DataFrame(all_rows)

    # Filter to valid dates in range
    def parse_valid_date(d):
        try:
            parsed = date.fromisoformat(str(d))
            if START_DATE <= parsed <= END_DATE:
                return str(parsed)
        except (ValueError, TypeError):
            pass
        return None

    df["date"] = df["date"].apply(parse_valid_date)
    df = df.dropna(subset=["date"])
    df = df.sort_values(["date", "variety"]).reset_index(drop=True)
    df.to_csv(out_path, index=False)

    print(f"\n[DONE] Saved {len(df)} news price records → {out_path}")
    if not df.empty:
        print(df[["date","district","variety","price_lkr_per_kg","confidence"]].head(10).to_string(index=False))


if __name__ == "__main__":
    main()

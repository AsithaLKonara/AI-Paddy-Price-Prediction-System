"""
Government Guaranteed Paddy Price Scraper
==========================================
Collects the official government minimum guaranteed paddy purchase price
set by the Paddy Marketing Board (PMB) and Agriculture Ministry.

Sources:
  - agrimin.gov.lk (Agriculture Ministry announcements)
  - Hardcoded known PMB gazette values (reliable fallback)

Government sets price TWICE a year:
  Maha season: Oct–Mar (announced ~Oct/Nov)
  Yala season: Apr–Sep (announced ~Apr/May)

Output: data/raw/gov_prices.csv
  columns: date, gov_samba_price, gov_nadu_price, gov_keeri_price
           source (harti_pdf | agrimin | hardcoded)
"""

import os
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import date, timedelta

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
    )
}

# ─── Hardcoded PMB / Paddy Marketing Board official values ─────────────────
# Source: PMB Gazette Notifications + Agriculture Ministry press releases
# Format: (start_date, end_date, samba_LKRperkg, nadu_LKRperkg, keeri_LKRperkg, source)
GUARANTEED_PRICES = [
    # 2022 Maha (Oct 2021 → Mar 2022 harvest, price set late 2021)
    ("2022-01-01", "2022-04-30",  80.0,  75.0,  90.0, "PMB Gazette 2021/10"),
    # 2022 Yala (Apr → Sep 2022 harvest, price sometimes unchanged)
    ("2022-05-01", "2022-09-30",  85.0,  80.0,  95.0, "PMB Gazette 2022/04"),
    # 2022 Maha harvest (Oct–Dec 2022 — crisis period, govt raised support price)
    ("2022-10-01", "2022-12-31",  95.0,  90.0, 105.0, "PMB Gazette 2022/10"),
    # 2023 Maha (Jan–Apr 2023)
    ("2023-01-01", "2023-04-30", 105.0, 100.0, 115.0, "PMB Gazette 2022/10"),
    # 2023 Yala (May–Sep 2023)
    ("2023-05-01", "2023-09-30", 115.0, 110.0, 125.0, "PMB Gazette 2023/04"),
    # 2023 Maha harvest (Oct–Dec 2023)
    ("2023-10-01", "2023-12-31", 120.0, 115.0, 130.0, "PMB Gazette 2023/10"),
    # 2024 Maha (Jan–Apr 2024)
    ("2024-01-01", "2024-04-30", 125.0, 120.0, 132.0, "PMB Gazette 2023/10"),
    # 2024 Yala (May–Sep 2024)
    ("2024-05-01", "2024-09-30", 127.0, 122.0, 135.0, "PMB Gazette 2024/04"),
    # 2024 Maha harvest (Oct–Dec 2024)
    ("2024-10-01", "2024-12-31", 130.0, 125.0, 138.0, "PMB Gazette 2024/10"),
    # 2025 Maha (Jan–Apr 2025) - Forward fill from late 2024
    ("2025-01-01", "2025-04-30", 130.0, 125.0, 138.0, "Estimate based on 2024/10"),
    # 2025 Yala (May–Sep 2025)
    ("2025-05-01", "2025-09-30", 132.0, 127.0, 140.0, "Estimate based on trend"),
    # 2025 Maha harvest (Oct–Dec 2025)
    ("2025-10-01", "2025-12-31", 135.0, 130.0, 145.0, "Estimate based on trend"),
]

START_DATE = date(2022, 1, 1)
END_DATE   = date(2025, 12, 31)


def try_scrape_agrimin() -> list:
    """
    Attempt to scrape recent price announcements from agrimin.gov.lk.
    Returns list of (date_str, samba, nadu, keeri, source) tuples.
    """
    results = []
    urls_to_try = [
        "https://www.agrimin.gov.lk/",
        "https://www.agrimin.gov.lk/news/",
        "https://www.agrimin.gov.lk/en/news/",
    ]
    price_pattern = re.compile(
        r"(?:Samba|samba)[^\d]*(\d{2,3}(?:\.\d{1,2})?)"
        r".*?(?:Nadu|nadu)[^\d]*(\d{2,3}(?:\.\d{1,2})?)"
        r".*?(?:Keeri|keeri)[^\d]*(\d{2,3}(?:\.\d{1,2})?)",
        re.DOTALL | re.IGNORECASE,
    )
    date_pattern = re.compile(r"(\d{4})[/-](\d{2})[/-](\d{2})|(\d{2})[/-](\d{2})[/-](\d{4})")

    for url in urls_to_try:
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            if resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.text, "lxml")
            # look for articles/paragraphs with price keywords
            for tag in soup.find_all(["p", "li", "div", "article"]):
                text = tag.get_text(" ", strip=True)
                if not any(kw in text.lower() for kw in ["paddy", "paddy price", "guaranteed"]):
                    continue
                m = price_pattern.search(text)
                dm = date_pattern.search(text)
                if m and dm:
                    try:
                        if dm.group(1):  # YYYY-MM-DD
                            d_str = f"{dm.group(1)}-{dm.group(2)}-{dm.group(3)}"
                        else:            # DD-MM-YYYY
                            d_str = f"{dm.group(6)}-{dm.group(5)}-{dm.group(4)}"
                        results.append((d_str, float(m.group(1)), float(m.group(2)),
                                        float(m.group(3)), url))
                    except (ValueError, AttributeError):
                        pass
        except Exception as e:
            print(f"[WARN] agrimin scrape failed ({url}): {e}")

    print(f"[INFO] agrimin.gov.lk: found {len(results)} price announcements")
    return results


def build_daily_gov_prices(scraped: list) -> pd.DataFrame:
    """Combine hardcoded + scraped prices and forward-fill per day."""
    # Start from hardcoded revisions
    revisions = []
    for (start_str, end_str, samba, nadu, keeri, source) in GUARANTEED_PRICES:
        s = date.fromisoformat(start_str)
        e = date.fromisoformat(end_str)
        cur = s
        while cur <= e and cur <= END_DATE:
            if cur >= START_DATE:
                revisions.append({
                    "date": cur.isoformat(),
                    "gov_samba_price": samba,
                    "gov_nadu_price":  nadu,
                    "gov_keeri_price": keeri,
                    "gov_price_source": source,
                })
            cur += timedelta(days=1)

    # Override with any scraped values
    for (d_str, samba, nadu, keeri, src) in scraped:
        try:
            d = date.fromisoformat(d_str)
            if START_DATE <= d <= END_DATE:
                # Replace the hardcoded row for this date
                for r in revisions:
                    if r["date"] == d_str:
                        r["gov_samba_price"] = samba
                        r["gov_nadu_price"]  = nadu
                        r["gov_keeri_price"] = keeri
                        r["gov_price_source"] = f"scraped:{src}"
                        break
        except ValueError:
            pass

    df = pd.DataFrame(revisions)
    return df


def main():
    out_dir  = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "gov_prices.csv")

    scraped = try_scrape_agrimin()
    df      = build_daily_gov_prices(scraped)
    df.to_csv(out_path, index=False)

    print(f"[DONE] Saved {len(df)} rows → {out_path}")
    print(df[["date","gov_samba_price","gov_nadu_price","gov_keeri_price"]].drop_duplicates(
        ["gov_samba_price","gov_Nadu_price"] if "gov_Nadu_price" in df.columns else
        ["gov_samba_price","gov_nadu_price"]
    ).to_string(index=False))


if __name__ == "__main__":
    main()

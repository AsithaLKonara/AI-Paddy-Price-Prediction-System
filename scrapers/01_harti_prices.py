"""
HARTI Daily/Weekly Food Commodities Bulletin Scraper (v2)
==========================================================
Processes HARTI PDFs for paddy farmgate prices.

Handles:
1. Daily Bulletins: Wholesale Rice prices (as fallback/signal)
2. Weekly Bulletins: Table 16 (Farmgate Paddy Prices) - Primary source

Logic:
- Scans data/raw/pdfs/ for weekly_*.pdf and daily_*.pdf
- Uses regex to extract district-wise paddy prices for:
  Samba (Short Grain), Nadu (Long Grain White), Keeri Samba
- Maps abbreviations (A'pura -> Anuradhapura, etc.)
"""

import os
import re
import pdfplumber
import pandas as pd
from tqdm import tqdm
from datetime import datetime

# District Mapping (HARTI abbreviations to Full Names)
DISTRICT_MAP = {
    "A'pura": "Anuradhapura", "Anuradhapura": "Anuradhapura",
    "P'naruwa": "Polonnaruwa", "Polonnaruwa": "Polonnaruwa",
    "Kalawewa": "Anuradhapura", "Kurunegala": "Kurunegala",
    "Nikaweratiya": "Kurunegala", "Ampara": "Ampara",
    "Dehiattakandiya": "Ampara", "Matara": "Matara",
    "Hambantota": "Hambantota", "Hambanthota": "Hambantota",
    "Embilipitiya": "Ratnapura", "Welimada": "Badulla",
    "N'Eliya": "Nuwara Eliya", "Nuwaraeliya": "Nuwara Eliya",
    "Jaffna": "Jaffna", "Vavuniya": "Vavuniya",
    "Mannar": "Mannar", "Mullaittivu": "Mullaitivu",
    "Killinochchi": "Kilinochchi", "Batticaloa": "Batticaloa",
    "Trincomalee": "Trincomalee", "Hanguranketha": "Nuwara Eliya",
    "Veyangoda": "Gampaha", "Galenbindunuwewa": "Anuradhapura",
    "Bandarawela": "Badulla", "Matale": "Matale", "Dambulla": "Matale",
    "Meegoda": "Colombo", "M'goda": "Colombo", "Pettah": "Colombo",
}

def parse_pdf(filepath):
    """
    Extracts paddy prices from a HARTI Weekly or Daily PDF.
    Returns list of {'date', 'district', 'samba_price', 'nadu_price', 'keeri_samba_price'}
    """
    is_weekly = "weekly" in filepath.lower()
    results = []
    current_date = None
    
    try:
        with pdfplumber.open(filepath) as pdf:
            # 1. Find the date
            text_p1 = pdf.pages[0].extract_text() or ""
            # Try Daily format: 2024.12.31
            d_match = re.search(r'(\d{4})\.(\d{2})\.(\d{2})', text_p1)
            # Try Weekly format: 26th December 2024
            w_match = re.search(r'(\d{1,2})(?:st|nd|rd|th)?\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+(\d{4})', text_p1)
            
            if d_match:
                current_date = f"{d_match.group(1)}-{d_match.group(2)}-{d_match.group(3)}"
            elif w_match:
                day, month_str, year = w_match.groups()
                # Parse "December" etc.
                try:
                    dt = datetime.strptime(f"{day} {month_str[:3]} {year}", "%d %b %Y")
                    current_date = dt.strftime("%Y-%m-%d")
                except:
                    current_date = f"{year}-{month_str}-{day}"
            else:
                d_match = re.search(r'(\d{2})-(\d{2})-(\d{4})', filepath)
                if d_match:
                    current_date = f"{d_match.group(3)}-{d_match.group(2)}-{d_match.group(1)}"

            # 2. Extract price data
            if is_weekly:
                for page in pdf.pages:
                    text = page.extract_text()
                    if not text or "Table 16" not in text:
                        continue
                    
                    lines = text.split("\n")
                    variety = None
                    for line in lines:
                        if "Short Grain" in line:
                            variety = "samba"
                            print(f"  [PADDY] Samba section started")
                        elif "Long Grain (White)" in line:
                            variety = "nadu"
                            print(f"  [PADDY] Nadu section started")
                        elif "Keeri Samba" in line:
                            variety = "keeri_samba"
                            print(f"  [PADDY] Keeri Samba section started")
                        elif any(kw in line for kw in ["Vegetable", "Fruits", "Potato"]):
                            variety = None
                        
                        if not variety:
                            continue
                        
                        # Leftmost column (start < 10) is Paddy. 
                        # Regex: DistrictName Range AvgPrice
                        matches = list(re.finditer(r"([\w']+)\s+\d+(?:\.\d+)?\s*[-\u2013\u2014\u2212]\s*\d+(?:\.\d+)?\s+(\d+(?:\.\d+)?)", line))
                        if matches:
                            m = min(matches, key=lambda x: x.start())
                            if m.start() < 10:
                                dist_abbr = m.group(1)
                                price_str = m.group(2).replace(",", "")
                                try:
                                    price = float(price_str)
                                    dist_full = DISTRICT_MAP.get(dist_abbr)
                                    if dist_full:
                                        print(f"    Match: {dist_abbr} -> {price}")
                                        # Update existing row or create new
                                        found = False
                                        for r in results:
                                            if r['district'] == dist_full:
                                                r[f'{variety}_price'] = price
                                                found = True
                                                break
                                        if not found:
                                            row = {'date': current_date, 'district': dist_full,
                                                   'samba_price': None, 'nadu_price': None, 'keeri_samba_price': None}
                                            row[f'{variety}_price'] = price
                                            results.append(row)
                                except ValueError:
                                    pass
            else:
                # Daily Bulletin logic (skipped for now as Weekly is better)
                pass

    except Exception as e:
        print(f"Error parsing {filepath}: {e}")
        
    return results

def main():
    base_dir = os.path.dirname(__file__)
    pdf_dir = os.path.join(base_dir, "..", "data", "raw", "pdfs")
    out_path = os.path.join(base_dir, "..", "data", "raw", "harti_prices.csv")
    
    if not os.path.exists(pdf_dir):
        print(f"No PDF directory found at {pdf_dir}")
        return

    all_pdfs = [os.path.join(pdf_dir, f) for f in os.listdir(pdf_dir) if f.endswith(".pdf")]
    if not all_pdfs:
        print("No PDFs found in data/raw/pdfs")
        return

    print(f"Processing {len(all_pdfs)} PDFs...")
    all_data = []
    for pdf_path in tqdm(all_pdfs):
        data = parse_pdf(pdf_path)
        all_data.extend(data)

    if not all_data:
        print("No data extracted from PDFs.")
        return

    df = pd.DataFrame(all_data)
    # Group by date/district and pick first (since weekly is once per week)
    df = df.groupby(['date', 'district'], dropna=False).first().reset_index()
    df.to_csv(out_path, index=False)
    print(f"Saved {len(df)} rows to {out_path}")

if __name__ == "__main__":
    main()

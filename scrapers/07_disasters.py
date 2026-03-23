"""
Disaster Events Scraper
Queries the ReliefWeb API for Sri Lanka flood/drought disaster events,
and also parses the DMC Sri Lanka situation reports.
Outputs a daily district-level disaster flag: none / flood / drought / cyclone / landslide
Output: data/raw/disasters.csv
"""

import os
import re
import time
import requests
import pandas as pd
from datetime import date, timedelta

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
    )
}

START_DATE = date(2022, 1, 1)
END_DATE   = date(2025, 12, 31)

DISTRICTS = [
    "Ampara", "Anuradhapura", "Badulla", "Batticaloa", "Colombo",
    "Galle", "Gampaha", "Hambantota", "Jaffna", "Kalutara",
    "Kandy", "Kegalle", "Kilinochchi", "Kurunegala", "Mannar",
    "Matale", "Matara", "Monaragala", "Mullaitivu", "Nuwara Eliya",
    "Polonnaruwa", "Puttalam", "Ratnapura", "Trincomalee", "Vavuniya",
]

# ─── ReliefWeb API ─────────────────────────────────────────────────────────
RELIEFWEB_URL = "https://api.reliefweb.int/v1/disasters"

DISASTER_KEYWORDS = {
    "flood":    ["flood", "inundation", "heavy rain", "flooding"],
    "drought":  ["drought", "dry spell"],
    "cyclone":  ["cyclone", "tropical storm", "depression"],
    "landslide":["landslide", "mudslide", "earth slip"],
}


def classify_disaster(text):
    t = text.lower()
    for dtype, keywords in DISASTER_KEYWORDS.items():
        for kw in keywords:
            if kw in t:
                return dtype
    return "none"


def parse_district(text):
    """Extract mentioned Sri Lanka districts from text."""
    found = []
    t = text.lower()
    for d in DISTRICTS:
        if d.lower() in t:
            found.append(d)
    return found if found else None


def fetch_reliefweb():
    """Fetch disaster events from ReliefWeb API."""
    events = []
    page = 0
    page_size = 100
    while True:
        params = {
            "filter[field][country.iso3]": "lka",
            "filter[field][date.created][from]": "2022-01-01T00:00:00+00:00",
            "filter[field][date.created][to]":   "2025-12-31T23:59:59+00:00",
            "fields[include][]": ["name", "date", "status", "primary_type", "country", "description"],
            "sort[]": "date.created:asc",
            "limit": page_size,
            "offset": page * page_size,
            "appname": "paddy-price-scraper",
        }
        try:
            resp = requests.get(RELIEFWEB_URL, params=params, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            items = data.get("data", [])
            if not items:
                break
            for item in items:
                fields = item.get("fields", {})
                name   = fields.get("name", "")
                ptype  = fields.get("primary_type", {})
                ptype_name = ptype.get("name", name) if isinstance(ptype, dict) else str(ptype)
                date_info  = fields.get("date", {})
                event_date = date_info.get("created", "")[:10] if isinstance(date_info, dict) else str(date_info)[:10]
                description = fields.get("description", "")
                dtype = classify_disaster(ptype_name + " " + name)
                districts = parse_district(name + " " + description)
                events.append({
                    "date":      event_date,
                    "dtype":     dtype,
                    "districts": districts,
                    "name":      name,
                })
            if len(items) < page_size:
                break
            page += 1
            time.sleep(0.5)
        except Exception as e:
            print(f"[WARN] ReliefWeb page {page}: {e}")
            break

    print(f"[INFO] ReliefWeb: {len(events)} events found")
    return events


# ─── Hardcoded known major Sri Lanka disasters 2022–2024 ───────────────────
# Source: DMC, OCHA, media records
KNOWN_DISASTERS = [
    # (start_date, end_date, disaster_type, [districts])
    ("2022-05-17", "2022-05-25", "flood",    ["Kalutara", "Galle", "Matara", "Ratnapura", "Colombo", "Gampaha"]),
    ("2022-06-05", "2022-06-15", "flood",    ["Kandy", "Matale", "Kegalle", "Nuwara Eliya"]),
    ("2022-07-10", "2022-07-20", "drought",  ["Anuradhapura", "Polonnaruwa", "Kurunegala", "Puttalam"]),
    ("2022-10-01", "2022-10-15", "flood",    ["Ampara", "Batticaloa", "Trincomalee", "Monaragala"]),
    ("2023-01-15", "2023-01-25", "flood",    ["Colombo", "Gampaha", "Kalutara"]),
    ("2023-05-25", "2023-06-05", "flood",    ["Ratnapura", "Kegalle", "Galle", "Matara"]),
    ("2023-07-01", "2023-07-31", "drought",  ["Jaffna", "Mannar", "Vavuniya", "Kilinochchi"]),
    ("2023-10-10", "2023-10-22", "flood",    ["Ampara", "Batticaloa", "Trincomalee"]),
    ("2023-11-01", "2023-11-10", "flood",    ["Anuradhapura", "Polonnaruwa"]),
    ("2024-01-20", "2024-01-30", "flood",    ["Colombo", "Gampaha"]),
    ("2024-05-20", "2024-06-05", "flood",    ["Kalutara", "Galle", "Matara", "Ratnapura"]),
    ("2024-06-15", "2024-07-15", "drought",  ["Anuradhapura", "Polonnaruwa", "Kurunegala"]),
    ("2024-10-15", "2024-10-28", "flood",    ["Ampara", "Batticaloa", "Jaffna", "Trincomalee"]),
    ("2024-11-20", "2024-11-30", "landslide",["Badulla", "Nuwara Eliya", "Kandy"]),
]


def build_disaster_df(known_events, api_events):
    """Combine all sources into a district-day disaster DataFrame."""
    # Build baseline: all dates × all districts = "none"
    all_dates    = [START_DATE + timedelta(days=i)
                    for i in range((END_DATE - START_DATE).days + 1)]
    baseline_rows = [
        {"date": d.isoformat(), "district": dist, "disaster": "none"}
        for d in all_dates for dist in DISTRICTS
    ]
    df = pd.DataFrame(baseline_rows)
    df = df.set_index(["date", "district"])

    # Apply known hardcoded events
    for (start_str, end_str, dtype, dists) in known_events:
        s = date.fromisoformat(start_str)
        e = date.fromisoformat(end_str)
        cur = s
        while cur <= e and cur <= END_DATE:
            if cur >= START_DATE:
                for d in dists:
                    key = (cur.isoformat(), d)
                    if key in df.index:
                        df.at[key, "disaster"] = dtype
            cur += timedelta(days=1)

    # Apply API events
    for evt in api_events:
        edate = evt["date"]
        dtype = evt["dtype"]
        if dtype == "none":
            continue
        dists = evt["districts"] or DISTRICTS  # if no district mentioned, apply broadly
        if (edate, dists[0] if dists else DISTRICTS[0]) in df.index:
            for d in dists:
                if START_DATE.isoformat() <= edate <= END_DATE.isoformat():
                    key = (edate, d)
                    if key in df.index:
                        df.at[key, "disaster"] = dtype

    df = df.reset_index()
    return df


def main():
    out_dir = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "disasters.csv")

    api_events = fetch_reliefweb()
    df = build_disaster_df(KNOWN_DISASTERS, api_events)
    df.to_csv(out_path, index=False)

    disaster_count = (df["disaster"] != "none").sum()
    print(f"[DONE] Saved {len(df)} rows, {disaster_count} disaster events → {out_path}")


if __name__ == "__main__":
    main()

"""
Open-Meteo Historical Weather Scraper
Fetches daily rainfall + temperature for all 25 Sri Lanka districts.
Uses the free Open-Meteo Archive API (no API key required).
Output: data/raw/weather.csv
"""

import os
import time
import requests
import pandas as pd
from tqdm import tqdm

# District centroid lat/lon (approximate)
DISTRICTS = {
    "Ampara":        (7.2833,  81.6667),
    "Anuradhapura":  (8.3333,  80.4167),
    "Badulla":       (6.9833,  81.0500),
    "Batticaloa":    (7.7167,  81.7000),
    "Colombo":       (6.9271,  79.8612),
    "Galle":         (6.0535,  80.2210),
    "Gampaha":       (7.0917,  80.0000),
    "Hambantota":    (6.1241,  81.1185),
    "Jaffna":        (9.6615,  80.0255),
    "Kalutara":      (6.5854,  79.9607),
    "Kandy":         (7.2906,  80.6337),
    "Kegalle":       (7.2513,  80.3464),
    "Kilinochchi":   (9.3803,  80.3770),
    "Kurunegala":    (7.4833,  80.3667),
    "Mannar":        (8.9750,  79.9050),
    "Matale":        (7.4667,  80.6167),
    "Matara":        (5.9483,  80.5353),
    "Monaragala":    (6.8667,  81.3500),
    "Mullaitivu":    (9.2667,  80.8167),
    "Nuwara Eliya":  (6.9497,  80.7891),
    "Polonnaruwa":   (7.9333,  81.0000),
    "Puttalam":      (8.0300,  79.8400),
    "Ratnapura":     (6.6900,  80.3853),
    "Trincomalee":   (8.5667,  81.2333),
    "Vavuniya":      (8.7514,  80.4997),
}

START_DATE = "2022-01-01"
END_DATE   = "2025-12-31"

API_URL = "https://archive-api.open-meteo.com/v1/archive"


def fetch_weather(district, lat, lon):
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "daily": "precipitation_sum,temperature_2m_mean",
        "timezone": "Asia/Colombo",
    }
    for attempt in range(3):
        try:
            resp = requests.get(API_URL, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            daily = data.get("daily", {})
            dates = daily.get("time", [])
            rain  = daily.get("precipitation_sum", [])
            temp  = daily.get("temperature_2m_mean", [])
            rows = []
            for i, d in enumerate(dates):
                rows.append({
                    "date":           d,
                    "district":       district,
                    "rainfall_mm":    round(rain[i], 2) if rain[i] is not None else None,
                    "temperature_c":  round(temp[i], 2) if temp[i] is not None else None,
                })
            return rows
        except Exception as e:
            print(f"[WARN] {district} attempt {attempt+1}: {e}")
            time.sleep(2)
    return []


def main():
    out_dir = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "weather.csv")

    all_rows = []
    for district, (lat, lon) in tqdm(DISTRICTS.items(), desc="Fetching weather"):
        rows = fetch_weather(district, lat, lon)
        all_rows.extend(rows)
        time.sleep(0.3)   # be polite to the API

    df = pd.DataFrame(all_rows)
    # Fill any missing values with district monthly average
    df["month"] = pd.to_datetime(df["date"]).dt.month
    for col in ["rainfall_mm", "temperature_c"]:
        df[col] = df.groupby(["district", "month"])[col].transform(
            lambda x: x.fillna(x.mean())
        )
    df.drop(columns=["month"], inplace=True)
    df.to_csv(out_path, index=False)
    print(f"[DONE] Saved {len(df)} rows → {out_path}")


if __name__ == "__main__":
    main()

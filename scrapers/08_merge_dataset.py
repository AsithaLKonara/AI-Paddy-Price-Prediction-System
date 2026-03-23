"""
Updated Dataset Merger (v2)
============================
Merges all sources into final paddy_price_dataset.csv with multi-source
paddy prices, derived min/avg/max, and all feature columns.

Join logic (all left-join on date+district):
  Base:        full date × district grid (2022-01-01 → 2024-12-31)
  + HARTI:     farmgate paddy prices per variety per district
  + Gov:       government guaranteed paddy prices (daily, national)
  + News:      news-sourced paddy price signals
  + Weather:   rainfall_mm, temperature_c
  + Fuel:      diesel_price, petrol_price
  + Disasters: disaster type per district
  + Cultivation: samba/nadu/keeri cultivation percent

Derived columns added:
  samba_min, samba_avg, samba_max (across harti + news sources)
  nadu_min, nadu_avg, nadu_max
  keeri_samba_min, keeri_samba_avg, keeri_samba_max
"""

import os
import numpy as np
import pandas as pd

BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
RAW_DIR  = os.path.join(BASE_DIR, "data", "raw")
OUT_CSV  = os.path.join(BASE_DIR, "data", "paddy_price_dataset.csv")

# Districts prone to wildlife-crop conflict (DWC annual reports)
HIGH_RISK_ANIMAL_DAMAGE = {
    "Anuradhapura", "Polonnaruwa", "Kurunegala", "Ampara",
    "Trincomalee", "Monaragala", "Hambantota", "Vavuniya",
}

ALL_DISTRICTS = [
    "Ampara", "Anuradhapura", "Badulla", "Batticaloa", "Colombo",
    "Galle", "Gampaha", "Hambantota", "Jaffna", "Kalutara",
    "Kandy", "Kegalle", "Kilinochchi", "Kurunegala", "Mannar",
    "Matale", "Matara", "Monaragala", "Mullaitivu", "Nuwara Eliya",
    "Polonnaruwa", "Puttalam", "Ratnapura", "Trincomalee", "Vavuniya",
]


def estimate_animal_damage(row):
    month = pd.to_datetime(row["date"]).month
    is_harvest = month in [1, 2, 3, 8, 9]
    is_risk    = row["district"] in HIGH_RISK_ANIMAL_DAMAGE
    seed = hash(row["date"] + row["district"]) % 1000
    np.random.seed(seed)
    prob = 0.20 if (is_risk and is_harvest) else 0.10 if is_risk else 0.05 if is_harvest else 0.03
    return "yes" if np.random.random() < prob else "no"


# ─── Loaders ────────────────────────────────────────────────────────────────

def _load(name, columns):
    path = os.path.join(RAW_DIR, name)
    if not os.path.exists(path):
        print(f"  [WARN] {name} not found — columns will be null")
        return pd.DataFrame(columns=columns)
    df = pd.read_csv(path)
    # Normalise date column
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        df = df.dropna(subset=["date"])
    return df


def load_harti():
    df = _load("harti_prices.csv",
               ["date","district","samba_price","nadu_price","keeri_samba_price"])
    for col in ["samba_price","nadu_price","keeri_samba_price"]:
        if col not in df.columns:
            df[col] = np.nan
    df.rename(columns={
        "samba_price":       "samba_paddy_price_harti",
        "nadu_price":        "nadu_paddy_price_harti",
        "keeri_samba_price": "keeri_samba_paddy_price_harti",
    }, inplace=True)
    return df[["date","district","samba_paddy_price_harti",
               "nadu_paddy_price_harti","keeri_samba_paddy_price_harti"]]


def load_gov():
    df = _load("gov_prices.csv",
               ["date","gov_samba_price","gov_nadu_price","gov_keeri_price"])
    for col in ["gov_samba_price","gov_nadu_price","gov_keeri_price"]:
        if col not in df.columns:
            df[col] = np.nan
    return df[["date","gov_samba_price","gov_nadu_price","gov_keeri_price"]]


def load_news():
    """
    Pivot news prices into per-variety columns (average when multiple records
    exist for the same date+district+variety).
    """
    df = _load("news_prices.csv",
               ["date","district","variety","price_lkr_per_kg"])
    if df.empty or "variety" not in df.columns:
        return pd.DataFrame(columns=["date","district",
                                     "samba_paddy_price_news",
                                     "nadu_paddy_price_news",
                                     "keeri_samba_paddy_price_news"])
    # Keep only rows with usable data
    df = df.dropna(subset=["date","variety","price_lkr_per_kg"])
    df["price_lkr_per_kg"] = pd.to_numeric(df["price_lkr_per_kg"], errors="coerce")
    df = df.dropna(subset=["price_lkr_per_kg"])
    df["district"] = df["district"].fillna("National")

    # Pivot
    pivoted = (df.groupby(["date","district","variety"])["price_lkr_per_kg"]
                 .mean().reset_index())
    pivoted = pivoted.pivot_table(
        index=["date","district"], columns="variety",
        values="price_lkr_per_kg", aggfunc="mean"
    ).reset_index()
    pivoted.columns.name = None
    col_map = {
        "samba":       "samba_paddy_price_news",
        "nadu":        "nadu_paddy_price_news",
        "keeri_samba": "keeri_samba_paddy_price_news",
    }
    for old, new in col_map.items():
        if old in pivoted.columns:
            pivoted.rename(columns={old: new}, inplace=True)
    for col in col_map.values():
        if col not in pivoted.columns:
            pivoted[col] = np.nan
    return pivoted[["date","district"] + list(col_map.values())]


def load_weather():
    df = _load("weather.csv", ["date","district","rainfall_mm","temperature_c"])
    for col in ["rainfall_mm","temperature_c"]:
        if col not in df.columns:
            df[col] = np.nan
    return df[["date","district","rainfall_mm","temperature_c"]]


def load_fuel():
    df = _load("fuel_prices.csv", ["date","petrol_price","diesel_price"])
    for col in ["petrol_price","diesel_price"]:
        if col not in df.columns:
            df[col] = np.nan
    return df[["date","petrol_price","diesel_price"]]


def load_disasters():
    df = _load("disasters.csv", ["date","district","disaster"])
    if "disaster" not in df.columns:
        df["disaster"] = "none"
    return df[["date","district","disaster"]]


def load_cultivation():
    df = _load("cultivation.csv",
               ["year","district","samba_cultivation_percent",
                "nadu_cultivation_percent","keeri_samba_percent"])
    if "year" not in df.columns:
        return df
    df["year"] = df["year"].astype(int)
    return df


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    os.makedirs(os.path.join(BASE_DIR, "data"), exist_ok=True)

    print("[1/9] Building date × district grid...")
    all_dates = pd.date_range("2022-01-01", "2025-12-31", freq="D")
    df = pd.DataFrame([
        {"date": d.strftime("%Y-%m-%d"), "district": dist}
        for d in all_dates for dist in ALL_DISTRICTS
    ])

    print("[2/9] Merging HARTI paddy prices...")
    harti = load_harti()
    df = df.merge(harti, on=["date","district"], how="left")

    print("[3/9] Merging government guaranteed prices...")
    gov = load_gov()
    df = df.merge(gov, on="date", how="left")

    print("[4/9] Merging news-sourced paddy prices...")
    news = load_news()
    if not news.empty:
        # national-level news rows: broadcast to all districts
        national = news[news["district"] == "National"].drop(columns=["district"])
        district_news = news[news["district"] != "National"]
        df = df.merge(district_news, on=["date","district"], how="left")
        # fill from national where district-level missing
        for col in ["samba_paddy_price_news","nadu_paddy_price_news","keeri_samba_paddy_price_news"]:
            if col in national.columns and col in df.columns:
                nat_map = national.set_index("date")[col]
                df[col] = df.apply(
                    lambda r: nat_map.get(r["date"]) if pd.isna(r.get(col)) else r.get(col),
                    axis=1
                )
    else:
        df["samba_paddy_price_news"]       = np.nan
        df["nadu_paddy_price_news"]        = np.nan
        df["keeri_samba_paddy_price_news"] = np.nan

    print("[5/9] Merging weather...")
    weather = load_weather()
    df = df.merge(weather, on=["date","district"], how="left")

    print("[6/9] Merging fuel prices...")
    fuel = load_fuel()
    df = df.merge(fuel, on="date", how="left")

    print("[7/9] Merging disasters...")
    disasters = load_disasters()
    df = df.merge(disasters, on=["date","district"], how="left")
    df["disaster"] = df["disaster"].fillna("none")

    print("[8/9] Merging cultivation ratios...")
    cultivation = load_cultivation()
    if not cultivation.empty and "year" in cultivation.columns:
        df["year"] = pd.to_datetime(df["date"]).dt.year
        df = df.merge(cultivation, on=["year","district"], how="left")
        df.drop(columns=["year"], inplace=True)
    else:
        for col in ["samba_cultivation_percent","nadu_cultivation_percent","keeri_samba_percent"]:
            df[col] = np.nan

    print("[9/9] Computing derived columns...")

    # Animal damage estimate
    df["animal_damage"] = df.apply(estimate_animal_damage, axis=1)

    # Fill missing weather with district monthly medians
    df["_month"] = pd.to_datetime(df["date"]).dt.month
    for col in ["rainfall_mm","temperature_c"]:
        df[col] = df.groupby(["district","_month"])[col].transform(
            lambda x: x.fillna(x.median())
        )
    df.drop(columns=["_month"], inplace=True)

    # ── Min / Avg / Max paddy price per variety ──────────────────────────
    for variety_prefix in ["samba", "nadu", "keeri_samba"]:
        price_cols = []
        for suffix in ["paddy_price_harti", "paddy_price_news"]:
            col = f"{variety_prefix}_{suffix}"
            if col in df.columns:
                price_cols.append(col)
        # include government guaranteed price as additional signal
        gov_col = {
            "samba":       "gov_samba_price",
            "nadu":        "gov_nadu_price",
            "keeri_samba": "gov_keeri_price",
        }.get(variety_prefix)
        if gov_col and gov_col in df.columns:
            price_cols.append(gov_col)

        if price_cols:
            price_df = df[price_cols].apply(pd.to_numeric, errors="coerce")
            df[f"{variety_prefix}_min_price"] = price_df.min(axis=1)
            df[f"{variety_prefix}_avg_price"] = price_df.mean(axis=1)
            df[f"{variety_prefix}_max_price"] = price_df.max(axis=1)

    # ── Final column order ───────────────────────────────────────────────
    final_cols = [
        "date", "district",
        # HARTI farmgate paddy prices
        "samba_paddy_price_harti", "nadu_paddy_price_harti", "keeri_samba_paddy_price_harti",
        # News-sourced prices
        "samba_paddy_price_news", "nadu_paddy_price_news", "keeri_samba_paddy_price_news",
        # Government guaranteed prices
        "gov_samba_price", "gov_nadu_price", "gov_keeri_price",
        # Derived ranges
        "samba_min_price", "samba_avg_price", "samba_max_price",
        "nadu_min_price",  "nadu_avg_price",  "nadu_max_price",
        "keeri_samba_min_price", "keeri_samba_avg_price", "keeri_samba_max_price",
        # Weather
        "rainfall_mm", "temperature_c",
        # Fuel
        "diesel_price", "petrol_price",
        # Events
        "disaster", "animal_damage",
        # Cultivation
        "samba_cultivation_percent", "nadu_cultivation_percent", "keeri_samba_percent",
    ]
    for col in final_cols:
        if col not in df.columns:
            df[col] = np.nan
    df = df[final_cols]

    # Round numeric columns
    num_cols = [c for c in df.columns
                if c not in ("date","district","disaster","animal_damage")]
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

    df.to_csv(OUT_CSV, index=False)

    # ── Validation report ────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"FINAL DATASET: {OUT_CSV}")
    print(f"Rows    : {len(df):,}")
    print(f"Columns : {len(df.columns)}")
    print(f"\nNull counts (top issues only):")
    nulls = df.isnull().sum()
    print(nulls[nulls > 0].to_string())
    print(f"\nDisaster breakdown:")
    print(df["disaster"].value_counts().to_string())
    print(f"\nGov prices (sample):")
    print(df[["date","gov_samba_price","gov_nadu_price","gov_keeri_price"]
             ].drop_duplicates(["gov_samba_price"]).head(5).to_string(index=False))
    cult_sum = (df["samba_cultivation_percent"].fillna(0)
              + df["nadu_cultivation_percent"].fillna(0)
              + df["keeri_samba_percent"].fillna(0))
    print(f"\nCultivation sum — max: {cult_sum.max():.1f}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()

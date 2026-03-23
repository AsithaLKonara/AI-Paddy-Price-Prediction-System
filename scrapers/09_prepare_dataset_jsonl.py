import pandas as pd
import json
import os
import numpy as np

def prepare_dataset():
    input_file = 'data/paddy_price_dataset.csv'
    output_file = 'data/dataset.jsonl'
    
    print(f"Loading {input_file}...")
    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found.")
        return

    df = pd.read_csv(input_file)
    
    # 1. Remove duplicates
    initial_len = len(df)
    df = df.drop_duplicates()
    print(f"Removed {initial_len - len(df)} duplicate rows.")
    
    # 2. Normalize districts
    print("Normalizing district names...")
    df['district'] = df['district'].str.strip().str.title()
    
    # 3. Convert dates
    print("Converting dates and adding seasonality...")
    df['date'] = pd.to_datetime(df['date'])
    df['month_num'] = df['date'].dt.month
    
    # 3.1 Seasonal Markers (Maha: Oct-Mar, Yala: Apr-Sep)
    df['is_maha'] = df['month_num'].apply(lambda x: 1 if x in [10, 11, 12, 1, 2, 3] else 0)
    df['is_yala'] = df['month_num'].apply(lambda x: 1 if x in [4, 5, 6, 7, 8, 9] else 0)
    
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    
    # 4. Fill missing values (Phase 4 strategies)
    # Forward fill prices per district
    print("Filling missing price values (forward fill)...")
    price_cols = [col for col in df.columns if 'price' in col]
    for col in price_cols:
        df[col] = df.groupby('district')[col].ffill()
    
    # Interpolate weather per district
    print("Interpolating weather data...")
    weather_cols = ['rainfall_mm', 'temperature_c']
    for col in weather_cols:
        if col in df.columns:
            # Linear interpolation requires we have some values; if all are NaN for a group, it stays NaN
            df[col] = df.groupby('district')[col].transform(lambda x: x.interpolate(method='linear').ffill().bfill())

    # Fill remaining NaNs with 0 or a sensible default for other columns
    df = df.fillna(0)
    
    # 5. Export to JSONL
    print(f"Exporting to {output_file}...")
    with open(output_file, 'w', encoding='utf-8') as f:
        for idx, row in df.iterrows():
            # Convert row to dict, handling non-serializable types
            record = row.to_dict()
            f.write(json.dumps(record) + '\n')
            
    print(f"Successfully generated {output_file} with {len(df)} records.")

if __name__ == "__main__":
    prepare_dataset()

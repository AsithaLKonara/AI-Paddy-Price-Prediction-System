# HARTI PDF Downloads

Place daily HARTI bulletin PDFs here. The scraper will detect and process them automatically.

## How to get PDFs

1. Go to: https://www.harti.gov.lk/index.php/en/market-information/data-food-commodities-bulletin
2. Click any daily bulletin link to download the PDF
3. Save it to this folder with the **original filename** (e.g. `daily_01-06-2023.pdf`)

## Filename format

```
daily_DD-MM-YYYY.pdf
```

Examples:
- `daily_01-01-2022.pdf`
- `daily_15-06-2023.pdf`
- `daily_31-12-2024.pdf`

## After downloading PDFs, run:

```bash
python3 scrapers/01_harti_prices.py
python3 scrapers/06_merge_dataset.py
```

The scraper will automatically parse all PDFs in this folder and extract
Samba, Nadu, and Keeri Samba prices per district.

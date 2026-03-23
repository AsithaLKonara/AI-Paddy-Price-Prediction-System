# 🌾 AI Paddy Price Prediction System (Sri Lanka)

An end-to-end automated system designed to predict future daily paddy prices for **Samba**, **Nadu**, and **Keeri Samba** across various districts in Sri Lanka.

## 🚀 Overview

This project focuses on bridging the gap between raw agricultural data and actionable market intelligence for farmers and stakeholders. It automates the collection of multi-source data including government bulletins, weather patterns, and economic indicators to provide accurate price forecasts.

## 🛠 Features

*   **Multi-Source Data Ingestion**: Automated scrapers for HARTI reports, government guaranteed prices, and news signals.
*   **Environmental & Economic Integration**: Incorporates rainfall, temperature, fuel prices (diesel/petrol), and disaster reports.
*   **Unified Dataset**: A robust merging pipeline that aligns disparate data sources into a single district-wise daily time-series.
*   **Forecast Engine**: (In Progress) Machine Learning models targeting short-term and medium-term price predictions.

## 📁 Project Structure

```text
.
├── data/               # Raw and processed datasets
│   ├── raw/            # Scraped CSVs and source PDFs
│   └── paddy_price_dataset.csv  # Consolidated master dataset
├── scrapers/           # Python ingestion and merging scripts
│   ├── 01_harti_prices.py
│   ├── ...
│   └── 08_merge_dataset.py
├── project_plan.md     # Detailed roadmap and status markers
└── requirements.txt    # Project dependencies
```

## 🚦 Current Progress

- [x] **Phase 1-3**: Data Collection Pipeline & Architecture (Completed)
- [/] **Phase 4-5**: Data Cleaning & Feature Engineering (In Progress)
- [ ] **Phase 6+**: Model Development & Dashboard (Pending)

## 📦 Installation

```bash
pip install -r requirements.txt
```

## 🛰 Data Sources

*   Hector Kobbekaduwa Agrarian Research and Training Institute (HARTI)
*   Department of Meteorology Sri Lanka
*   Ceylon Petroleum Corporation
*   Disaster Management Centre Sri Lanka
*   Department of Agriculture Sri Lanka

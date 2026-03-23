Below is a **clear end-to-end roadmap** for building your **Sri Lanka Paddy Price Prediction AI System** 🌾📊.
This plan goes from **data collection → ML model → production dashboard**.

---

# 🌾 AI Paddy Price Prediction System

### End-to-End Project Plan & Roadmap

Goal:
Predict **future daily paddy prices** for

* Samba
* Nadu
* Keeri Samba

Across **different districts in Sri Lanka**.

Data sources will include:

* Hector Kobbekaduwa Agrarian Research and Training Institute price reports
* Department of Meteorology Sri Lanka weather data
* Ceylon Petroleum Corporation fuel prices
* Disaster Management Centre Sri Lanka disaster events
* Department of Agriculture Sri Lanka cultivation statistics

---

# 🧭 Phase 1 — Problem Definition (Week 1) [DONE]

### Objectives

Define the forecasting task.

Example outputs:

```
Predict next:
1 day
7 days
30 days
```

For:

```
district + paddy variety
```

Example:

```
Anuradhapura - Samba - Tomorrow price
```

---

### Forecast Targets

| Target            | Type       |
| ----------------- | ---------- |
| Samba price       | regression |
| Nadu price        | regression |
| Keeri Samba price | regression |

---

### Evaluation Metrics

Use:

```
MAE  (Mean Absolute Error)
RMSE (Root Mean Square Error)
MAPE (Percentage Error)
```

---

# 🧱 Phase 2 — Data Architecture (Week 1–2) [DONE]

Create **central data warehouse**.

Recommended database:

```
PostgreSQL
```

Tables:

### paddy_prices

| field             |
| ----------------- |
| date              |
| district          |
| market            |
| samba_price       |
| nadu_price        |
| keeri_samba_price |

---

### weather

| field       |
| ----------- |
| date        |
| district    |
| rainfall    |
| temperature |
| humidity    |
| wind        |

Source:

Department of Meteorology Sri Lanka

---

### fuel_prices

| field  |
| ------ |
| date   |
| petrol |
| diesel |

Source:

Ceylon Petroleum Corporation

---

### disasters

| field         |
| ------------- |
| date          |
| district      |
| disaster_type |
| severity      |

Source:

Disaster Management Centre Sri Lanka

---

### cultivation

| field         |
| ------------- |
| season        |
| district      |
| samba_percent |
| nadu_percent  |
| keeri_percent |

Source:

Department of Agriculture Sri Lanka

---

# ⚙️ Phase 3 — Data Collection Pipeline (Week 2–4) [DONE]

Build automated **data ingestion pipelines**.

---

## 1️⃣ Paddy price scraper

From:

Hector Kobbekaduwa Agrarian Research and Training Institute

Steps:

```
Download PDF bulletins
Extract tables
Convert to CSV
Store in database
```

Tools:

```
requests
tabula
pandas
beautifulsoup
```

Run daily.

---

## 2️⃣ Weather collector

Use API:

Example sources:

* OpenWeather
* Department of Meteorology Sri Lanka

Collect:

```
rainfall
temperature
humidity
```

---

## 3️⃣ Fuel price dataset

Manual scraper.

Source:

Ceylon Petroleum Corporation

Prices change every few weeks.

---

## 4️⃣ Disaster events

Scrape reports from:

Disaster Management Centre Sri Lanka

Create binary feature:

```
flood = 1
drought = 1
cyclone = 1
```

---

# 🧹 Phase 4 — Data Cleaning (Week 4–5) [IN PROGRESS]

Tasks:

```
Remove duplicates
Fill missing values
Normalize districts
Convert dates
```

Example:

```
Anuradhapura
ANURADHAPURA
anuradhapura
```

→ normalize.

---

### Missing data strategies

Weather:

```
interpolation
```

Prices:

```
forward fill
```

---

# 🧠 Phase 5 — Feature Engineering (Week 5–6) [IN PROGRESS]

Create powerful predictors.

---

### Time features

```
day_of_week
month
season
harvest_season
```

---

### Lag features

Very important.

```
price_1_day_ago
price_7_days_ago
price_30_days_ago
```

---

### Rolling averages

```
7_day_avg_price
30_day_avg_price
```

---

### Weather aggregates

```
rainfall_last_7_days
temperature_avg_7_days
```

---

### Fuel change indicator

```
fuel_price_change
```

---

### Cultivation effect

```
samba_cultivation_percent
```

---

# 🤖 Phase 6 — Model Development (Week 6–8)

Start simple → then deep learning.

---

## Model 1 (Baseline)

```
Linear Regression
```

---

## Model 2 (Better)

```
Random Forest
```

---

## Model 3 (Recommended)

```
XGBoost
```

Handles nonlinear relationships well.

---

## Model 4 (Advanced)

```
LSTM
```

For time-series forecasting.

---

Libraries:

```
scikit-learn
xgboost
pytorch
```

---

# 🧪 Phase 7 — Training Pipeline

Workflow:

```
Raw Data
   ↓
Feature Engineering
   ↓
Train / Test Split
   ↓
Model Training
   ↓
Evaluation
```

Use **time-series split**:

```
train: 2023–2024
test: 2025
```

---

# 📊 Phase 8 — Forecast Engine

Predictions:

```
Next day price
Next week price
Next month price
```

For each:

```
district
paddy type
```

---

# 🖥️ Phase 9 — Dashboard

Build visualization system.

Recommended:

* Streamlit
* Grafana

Dashboard shows:

```
price trends
prediction charts
rainfall impact
district comparisons
```

Example chart:

```
Predicted vs actual price
```

---

# ☁️ Phase 10 — Deployment

Host system on cloud.

Options:

```
AWS
GCP
DigitalOcean
```

Run:

```
daily scraper
model retraining
prediction service
```

---

# 📈 Phase 11 — Continuous Improvement

Add more data sources.

Example:

Satellite crop monitoring.

Sources:

* NASA
* European Space Agency

This helps estimate:

```
crop health
crop area
yield
```

---

# 📦 Final System Architecture

```
Data Sources
   ↓
Scrapers
   ↓
Data Warehouse
   ↓
Feature Engineering
   ↓
ML Training
   ↓
Prediction API
   ↓
Dashboard
```

---

# ⏱ Realistic Timeline

| Phase               | Time    |
| ------------------- | ------- |
| Planning            | 1 week  |
| Data scraping       | 2 weeks |
| Data cleaning       | 1 week  |
| Feature engineering | 1 week  |
| Model training      | 2 weeks |
| Dashboard           | 1 week  |

Total:

```
~8 weeks
```

---

# 🚀 Final Product

```
Sri Lanka Paddy Price Forecasting AI
```

Outputs:

* Daily price prediction
* District-wise trends
* Seasonal analysis
* Farmer decision support

---

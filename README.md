# Project-Blue-Lance
A Human-in-the-Loop Risk Modeling Framework for Civilian Harm Mitigation (Under Construction)


# Project Blue Lance

## Overview

Project Blue Lance is a global, province-level early-warning system for civilian harm. It uses ACLED aggregated conflict data to predict the **risk that a given region will experience civilian fatalities next week**. The model is designed for humanitarian risk analysis, conflict monitoring, and escalation detection.

The pipeline ingests historical conflict data, aggregates it by week and region, engineers temporal features, trains a machine‑learning model, and produces weekly risk forecasts (none / low / medium / high).

---

## What Blue Lance Predicts

For every `(country, admin1, week)` the system predicts the **severity of civilian harm in the following week**, classified into:

| Label  | Meaning             | Fatalities next week |
| ------ | ------------------- | -------------------- |
| none   | No civilian deaths  | 0                    |
| low    | Small‑scale harm    | 1–5                  |
| medium | Sustained violence  | 6–20                 |
| high   | Major civilian harm | 21+                  |

The model does not predict who will win a conflict or where battles will occur — it forecasts **risk of civilian harm based on escalation patterns**.

---

## Data Source

Blue Lance uses **ACLED aggregated weekly data** for the following regions:

* Africa
* Asia‑Pacific
* Europe & Central Asia
* Latin America & Caribbean
* Middle East
* US & Canada

These are downloaded as XLSX files and combined into a single global dataset.

---

## Pipeline Architecture

```
ACLED XLSX files
        ↓
ingest.py   → data/interim/acled_global_weekly_raw.csv
        ↓
clean.py    → data/processed/acled_global_weekly_clean.csv
        ↓
features.py → data/processed/acled_global_weekly_features.csv
        ↓
train.py    → artifacts/rf_severity_model_global.joblib
        ↓
predict.py  → reports/latest_risk_predictions_global.csv
```

Each step is modular and can be rerun independently.



## Running the entire Pipeline

To execute everything from raw data to the predictions simply run

python src/bluelance/run_pipeline.py
---

## 1. Ingestion (`ingest.py`)

Reads all ACLED aggregated XLSX files in `data/raw/`, standardizes column names, and merges them into one global weekly dataset.

Output:

```
data/interim/acled_global_weekly_raw.csv
```

---

## 2. Cleaning (`clean.py`)

Cleans and normalizes the global raw file:

* standardizes column names
* parses week into a datetime
* removes malformed rows
* keeps only relevant columns

Output:

```
data/processed/acled_global_weekly_clean.csv
```

---

## 3. Feature Engineering (`features.py`)

Transforms weekly data into model‑ready features:

For each `(country, admin1, week)` it computes:

* `total_events`
* `total_fatalities`
* `population_exposure`
* `events_4w_sum` (past 4‑week event total)
* `fatalities_4w_sum` (past 4‑week fatalities)

It also builds the target variable:

* `fatalities_next_week`
* `severity_label_next_week`
* `severity_band_next_week` (0–3)

Output:

```
data/processed/acled_global_weekly_features.csv
```

---

## 4. Model Training (`train.py`)

Trains a Random Forest classifier on global data.

Features:

* total_events
* total_fatalities
* population_exposure
* events_4w_sum
* fatalities_4w_sum

Target:

* severity_band_next_week

Uses time‑based split (old → train, recent → test) to simulate forecasting.

Output:

```
artifacts/rf_severity_model_global.joblib
```

---

## 5. Prediction (`predict.py`)

Uses the trained global model to forecast **next‑week risk** for the **most recent week** in the dataset.

Produces:

```
reports/latest_risk_predictions_global.csv
```

Each row contains:

* country
* admin1
* input week
* predicted severity label
* confidence score

This file is what you would visualize on a map or dashboard.

---

## What This System Is

Blue Lance is a:

* humanitarian risk model
* conflict escalation detector
* civilian harm early‑warning system

It is designed to answer:

> "Where in the world is civilian harm most likely to spike next week?"

---

## What This System Is NOT

Blue Lance is **not**:

* a targeting tool
* a military planning system
* a strike optimization engine

It forecasts **risk**, not actions.

---

## How to Run

```bash

#0. Syncing ACLED files
python src/bluelance/sync_raw.py

# 1. Combine regional ACLED files
python src/bluelance/ingest.py

# 2. Clean
python src/bluelance/clean.py

# 3. Build features
python src/bluelance/features.py

# 4. Train global model
python src/bluelance/train.py

# 5. Predict next week
python src/bluelance/predict.py
```

---

## License

For academic, humanitarian, and research use only.


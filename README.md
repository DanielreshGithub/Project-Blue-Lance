⚡ Project Blue Lance [screenshot.png]

A Human-in-the-Loop Global Risk Modeling System for Civilian Harm Forecasting

Project Blue Lance is a global, province-level early-warning system designed to forecast the risk of civilian harm one week ahead.
It combines structured conflict data (ACLED), event-based media signals (GDELT), and machine-learning models to produce actionable risk forecasts that can be explored through an interactive global map.

This project is research-oriented and humanitarian in scope.

🔎 What Blue Lance Does

For every (country, admin1, week) combination, Blue Lance predicts the likelihood and severity of civilian harm in the following week.

| Label   | Meaning              | Fatalities Next Week |
|--------|----------------------|---------------------|
| none   | No civilian harm     | 0                   |
| low    | Isolated incidents   | 1–5                 |
| medium | Sustained violence   | 6–20                |
| high   | Major civilian harm  | 21+                 |

📊 Data Sources

ACLED (Primary)
	•	Aggregated weekly conflict event data
	•	Admin1 (province/state) resolution
	•	Global coverage across:
	•	Africa
	•	Asia-Pacific
	•	Europe & Central Asia
	•	Latin America & Caribbean
	•	Middle East
	•	United States & Canada

GDELT (Supplementary)
	•	Global event and media reporting signals
	•	Aggregated into rolling 30-day windows
	•	Used as leading indicators for escalation dynamics

🧠 System Architecture

ACLED XLSX files
        ↓
sync_raw.py   (download / sync)
        ↓
ingest.py → data/interim/
        ↓
clean.py → data/processed/
        ↓
features.py → ACLED weekly features
        ↓
gdeltingest.py → GDELT country-week features
        ↓
feature_merge.py → ACLED + GDELT merged dataset
        ↓
train.py → LightGBM models
        ↓
predict.py → Weekly forecasts
        ↓
Shiny App → Interactive global risk map

🤖 Machine Learning Models

1. Severity Classification (Multi-Class)
	•	Model: LightGBM
	•	Target: severity_band_next_week (0–3)
	•	Time-aware train/test split (by week)
	•	Purpose: Analytical understanding of escalation intensity

2. Risk Flag Model (Binary)
	•	Target: HIGH risk vs no risk
	•	Optimized for:
	•	High recall on severe events
	•	Strong ROC-AUC and PR-AUC
	•	Purpose: Operational early-warning

Typical performance (8-week window):
	•	Accuracy ≈ 90–96%
	•	ROC-AUC ≈ 0.95

🗺 Interactive Risk Map

Blue Lance includes a fully interactive Shiny application that visualizes weekly forecasts globally at the admin1 level.

Features
	•	Dark-mode global map
	•	Color-coded risk markers (none / low / medium / high)
	•	Week slider (time navigation)
	•	Search by country or province
	•	High-risk filtering
	•	Ranked “Top Risk Regions” panel
	•	Click-to-zoom interactions

UI inspired by operational monitoring dashboards.


🚀 Quick Start (Demo Mode)

Clone and launch the interactive map:
git clone https://github.com/DanielreshGithub/Project-Blue-Lance
cd Project-Blue-Lance
R -e "shiny::runApp('apps/risk_map_app', launch.browser=TRUE)"
A lightweight demo dataset is included so the app runs without executing the full pipeline.

⚙ Full Pipeline Execution

Install Dependencies:
Python (recommended via virtual environment)
pip install -r requirements.txt

R Packages:
install.packages(c("shiny","leaflet","bslib","dplyr","htmltools","arrow"))

Run Entire Pipeline:
python src/bluelance/run_pipeline.py

Step-By-Step Guide:
python src/bluelance/sync_raw.py
python src/bluelance/ingest.py
python src/bluelance/clean.py
python src/bluelance/features.py
python src/bluelance/gdeltingest.py
python src/bluelance/feature_merge.py
python src/bluelance/train.py
python src/bluelance/predict.py

And to launch the Map:
R -e "shiny::runApp('apps/risk_map_app', launch.browser=TRUE)"


📌 Status

Active development — core system complete.

Future directions
	•	Longer temporal windows
	•	Model calibration & interpretability
	•	External validation
	•	Deployment options

⸻

⚖ License

MIT License — free to use, modify, and distribute with attribution.



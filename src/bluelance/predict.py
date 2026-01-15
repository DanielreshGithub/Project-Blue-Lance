from pathlib import Path
import joblib
import pandas as pd

PROCESSED = Path("data/processed")
ARTIFACTS = Path("artifacts")
REPORTS = Path("reports")
REPORTS.mkdir(parents=True, exist_ok=True)

band_map = {0: "none", 1: "low", 2: "medium", 3: "high"}

def main():
    print("Loading data...")
    df = pd.read_csv(PROCESSED / "acled_global_weekly_features.csv", parse_dates=["week"])
    print(f"Loaded {len(df):,} rows")

    # Load global model pack
    model_file = ARTIFACTS / "rf_severity_model_global.joblib"
    print(f"Loading model from {model_file}...")
    pack = joblib.load(model_file)
    model = pack["model"]
    feature_cols = pack["feature_cols"]

    # Latest week only
    latest_week = df["week"].max()
    latest = df[df["week"] == latest_week].copy()
    print(f"Predicting for latest input week: {latest_week.date()} ({len(latest):,} rows)")

    X = latest[feature_cols].fillna(0)

    print("Making predictions...")
    latest["predicted_severity_band_next_week"] = model.predict(X)
    latest["predicted_severity_label_next_week"] = latest["predicted_severity_band_next_week"].map(band_map)

    # Optional: confidence (nice for ranking)
    probs = model.predict_proba(X)
    latest["predicted_confidence"] = probs.max(axis=1)

    out_csv = REPORTS / "latest_risk_predictions_global.csv"
    latest[["country", "admin1", "week", "predicted_severity_label_next_week", "predicted_confidence"]].to_csv(out_csv, index=False)
    print(f"Saved predictions to {out_csv}")

    print("\nTop 20 highest-risk (by band then confidence):")
    top = latest.sort_values(
        ["predicted_severity_band_next_week", "predicted_confidence"],
        ascending=[False, False]
    ).head(20)

    print(top[["country", "admin1", "predicted_severity_label_next_week", "predicted_confidence"]].to_string(index=False))

if __name__ == "__main__":
    main()
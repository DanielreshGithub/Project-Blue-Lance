from pathlib import Path
import joblib
import pandas as pd

from lightgbm import LGBMClassifier
from sklearn.metrics import classification_report, confusion_matrix  


PROCESSED = Path("data/processed")
ARTIFACTS = Path("artifacts")
ARTIFACTS.mkdir(parents=True, exist_ok=True)

def main():
    print("Loading data...")
    infile = PROCESSED / "acled_global_weekly_features.csv"
    df = pd.read_csv(infile, parse_dates=["week"])
    print(f"Loaded {len(df):,} rows")

    feature_cols = [
        "total_events",
        "total_fatalities",
        "population_exposure",
        "events_4w_sum",
        "fatalities_4w_sum",
    ]
    target_col = "severity_band_next_week"

    X = df[feature_cols].fillna(0)
    y = df[target_col].astype(int)

    # Time split (80-20)
    split_idx = int(0.8 * len(df))
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

    print("Training model...")
    model = LGBMClassifier(
        n_estimators=300,
        random_state=42,
        class_weight="balanced",
        n_jobs=-1,
    )
    model.fit(X_train, y_train)
    print("Training done. Predicting...")

    y_pred = model.predict(X_test)

    labels = [0, 1, 2, 3]
    target_names = ["none", "low", "medium", "high"]

    print("\nClassification Report:")
    print(classification_report(
        y_test,
        y_pred,
        labels=labels,
        target_names=target_names
    ))

    print("\nConfusion Matrix:")
    print(confusion_matrix(y_test, y_pred, labels=labels))

    model_path = ARTIFACTS / "lgbm_severity_model.joblib"
    joblib.dump({"model": model, "feature_cols": feature_cols}, model_path)
    print(f"Saved model -> {model_path}")

if __name__ == "__main__":
    main()
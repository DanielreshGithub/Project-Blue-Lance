from __future__ import annotations

from pathlib import Path
import joblib
import pandas as pd

from lightgbm import LGBMClassifier
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    roc_auc_score,
    average_precision_score,
)

PROCESSED = Path("data/processed")
ARTIFACTS = Path("artifacts")
ARTIFACTS.mkdir(parents=True, exist_ok=True)

DATASET_PATH = PROCESSED / "acled_gdelt_weekly_features_8w.parquet"

BASE_FEATURE_COLS = [
    "total_events",
    "total_fatalities",
    "population_exposure",
    "events_4w_sum",
    "fatalities_4w_sum",
    "gdelt_violence_count_30d",
    "gdelt_protest_count_30d",
    "gdelt_rebellion_count_30d",
]

TARGET_COL = "severity_band_next_week"


def _require_cols(df: pd.DataFrame, cols: list[str], where: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(
            f"{where}: dataset is missing columns: {missing}\n"
            f"Available columns: {sorted(df.columns.tolist())}\n"
            "Fix: re-run gdeltingest.py + feature_merge.py (or update your merge script)."
        )


def add_simple_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add a few simple but useful features (safe divisions)."""
    df = df.copy()

    ev = pd.to_numeric(df["total_events"], errors="coerce").fillna(0)
    fat = pd.to_numeric(df["total_fatalities"], errors="coerce").fillna(0)
    pop = pd.to_numeric(df["population_exposure"], errors="coerce").fillna(0)
    gv = pd.to_numeric(df["gdelt_violence_count_30d"], errors="coerce").fillna(0)

    df["fatalities_per_event"] = fat / (ev + 1.0)
    df["violence_per_capita"] = (fat / (pop.replace(0, pd.NA))).fillna(0)

    # Interaction (can be huge; keep it numeric)
    df["events_x_gdelt"] = ev * gv

    return df


def time_split_by_week(df: pd.DataFrame, test_frac: float = 0.2) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split data by time (whole weeks)."""
    weeks = sorted(df["week"].dropna().unique())
    if len(weeks) < 5:
        raise ValueError(f"Not enough unique weeks for time split: {len(weeks)}")

    split_at = max(1, int((1.0 - test_frac) * len(weeks)))
    train_weeks = set(weeks[:split_at])
    test_weeks = set(weeks[split_at:])

    train_df = df[df["week"].isin(train_weeks)].copy()
    test_df = df[df["week"].isin(test_weeks)].copy()

    print(f"\nTrain weeks: {len(train_weeks)} | Test weeks: {len(test_weeks)}")
    print(f"Train rows: {len(train_df):,} | Test rows: {len(test_df):,}")

    return train_df, test_df


def main() -> None:
    print("=" * 60)
    print("CONFLICT SEVERITY PREDICTION MODEL (LGBM)")
    print("=" * 60)

    if not DATASET_PATH.exists():
        raise FileNotFoundError(f"Missing dataset: {DATASET_PATH} (run feature_merge.py first)")

    # 1) Load merged dataset
    print(f"\nLoading: {DATASET_PATH}")
    df = pd.read_parquet(DATASET_PATH)

    _require_cols(df, ["week", TARGET_COL] + BASE_FEATURE_COLS, where="Load")

    # Normalize week (stable sorting + splits)
    df["week"] = pd.to_datetime(df["week"], errors="coerce").dt.normalize()

    # IMPORTANT:
    # We DROP rows where the target is missing (the newest week will often have NA target).
    df = df.dropna(subset=["week", TARGET_COL]).copy()

    print(f"Loaded {len(df):,} rows")
    print(f"Date range: {df['week'].min().date()} to {df['week'].max().date()}")

    # 2) Coerce base features to numeric
    for col in BASE_FEATURE_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # 3) Add extra features
    df = add_simple_features(df)

    ALL_FEATURES = BASE_FEATURE_COLS + [
        "fatalities_per_event",
        "violence_per_capita",
        "events_x_gdelt",
    ]

    _require_cols(df, ALL_FEATURES, where="Feature engineering")

    print(f"Using {len(ALL_FEATURES)} features")

    # 4) Split by week
    train_df, test_df = time_split_by_week(df, test_frac=0.2)

    X_train = train_df[ALL_FEATURES].fillna(0)
    y_train = pd.to_numeric(train_df[TARGET_COL], errors="coerce").astype(int)

    X_test = test_df[ALL_FEATURES].fillna(0)
    y_test = pd.to_numeric(test_df[TARGET_COL], errors="coerce").astype(int)

    print("\nTarget distribution in test set:")
    print(y_test.value_counts().sort_index())

    # 5) Train
    print("\n" + "-" * 60)
    print("Training model...")
    print("-" * 60)

    model = LGBMClassifier(
        n_estimators=1000,
        learning_rate=0.03,
        max_depth=6,
        num_leaves=31,
        random_state=42,
        class_weight="balanced",
        n_jobs=-1,
        force_col_wise=True,
        verbose=-1,
    )

    model.fit(X_train, y_train)
    print("✓ Training complete")

    # 6) Evaluate
    print("\n" + "-" * 60)
    print("EVALUATION RESULTS")
    print("-" * 60)

    probs = model.predict_proba(X_test)
    preds = model.predict(X_test)

    # Keep stable names for 0..3
    target_names = ["none (0)", "low (1)", "medium (2)", "high (3)"]

    print("\nClassification Report:")
    print(
        classification_report(
            y_test,
            preds,
            labels=[0, 1, 2, 3],
            target_names=target_names,
            zero_division=0,
        )
    )

    print("\nConfusion Matrix:")
    print("(Rows = Actual, Columns = Predicted)")
    cm = confusion_matrix(y_test, preds, labels=[0, 1, 2, 3])
    print(cm)

    # High-risk detection (class 3)
    print("\n" + "-" * 60)
    print("HIGH RISK DETECTION (class 3)")
    print("-" * 60)

    high_risk_prob = probs[:, 3]
    actual_high = (y_test == 3).astype(int)

    for thresh in [0.3, 0.4, 0.5]:
        predicted_high = (high_risk_prob >= thresh).astype(int)

        tp = int(((predicted_high == 1) & (actual_high == 1)).sum())
        fp = int(((predicted_high == 1) & (actual_high == 0)).sum())
        fn = int(((predicted_high == 0) & (actual_high == 1)).sum())

        precision = tp / (tp + fp) if (tp + fp) else 0.0
        recall = tp / (tp + fn) if (tp + fn) else 0.0
        f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) else 0.0

        print(f"\nThreshold {thresh:.1f}:")
        print(f"  Precision: {precision:.2%} | Recall: {recall:.2%} | F1: {f1:.3f}")
        print(f"  Caught {tp}/{int(actual_high.sum())} high-risk cases ({recall:.1%})")

    if int(actual_high.sum()) > 0:
        roc = roc_auc_score(actual_high, high_risk_prob)
        pr = average_precision_score(actual_high, high_risk_prob)
        print(f"\nROC-AUC (high-risk): {roc:.3f}")
        print(f"PR-AUC (high-risk): {pr:.3f}")

    # Feature importance
    print("\n" + "-" * 60)
    print("TOP 10 MOST IMPORTANT FEATURES")
    print("-" * 60)

    importance_df = (
        pd.DataFrame({"feature": ALL_FEATURES, "importance": model.feature_importances_})
        .sort_values("importance", ascending=False)
        .reset_index(drop=True)
    )

    for _, row in importance_df.head(10).iterrows():
        bar = "█" * int(row["importance"] / 100)
        print(f"{row['feature']:30s} {row['importance']:6.0f} {bar}")

    # 7) Save
    print("\n" + "-" * 60)
    print("Saving model...")
    print("-" * 60)

    model_path = ARTIFACTS / "lgbm_model.joblib"
    joblib.dump(
        {
            "model": model,
            "feature_cols": ALL_FEATURES,
            "target_col": TARGET_COL,
            "classes": [0, 1, 2, 3],
            "class_names": ["none", "low", "medium", "high"],
        },
        model_path,
    )
    print(f"✓ Saved: {model_path}")

    results = test_df[["country", "admin1", "week", TARGET_COL]].copy()
    results["predicted_severity"] = preds
    results["predicted_label"] = results["predicted_severity"].map({0: "none", 1: "low", 2: "medium", 3: "high"})
    results["high_risk_probability"] = high_risk_prob

    results_path = ARTIFACTS / "test_predictions.csv"
    results.to_csv(results_path, index=False)
    print(f"✓ Saved: {results_path}")

    importance_path = ARTIFACTS / "feature_importance.csv"
    importance_df.to_csv(importance_path, index=False)
    print(f"✓ Saved: {importance_path}")

    print("\n" + "=" * 60)
    print("DONE!")
    print("=" * 60)


if __name__ == "__main__":
    main()
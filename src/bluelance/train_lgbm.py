from __future__ import annotations

from pathlib import Path
import joblib
import pandas as pd
import numpy as np

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

# Features to use for prediction
FEATURE_COLS = [
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


def add_simple_features(df):
    """Add a few simple but powerful features"""
    print("Adding simple features...")
    
    # 1. Fatalities per event (intensity)
    df['fatalities_per_event'] = df['total_fatalities'] / (df['total_events'] + 1)
    
    # 2. Violence normalized by population
    df['violence_per_capita'] = df['total_fatalities'] / (df['population_exposure'] + 1)
    
    # 3. ACLED x GDELT interaction
    df['events_x_gdelt'] = df['total_events'] * df['gdelt_violence_count_30d']
    
    return df


def time_split_by_week(df, test_frac=0.2):
    """Split data by time - most recent weeks for testing"""
    weeks = sorted(df["week"].unique())
    split_at = int((1.0 - test_frac) * len(weeks))
    
    train_weeks = weeks[:split_at]
    test_weeks = weeks[split_at:]
    
    train_df = df[df["week"].isin(train_weeks)].copy()
    test_df = df[df["week"].isin(test_weeks)].copy()
    
    print(f"\nTrain weeks: {len(train_weeks)} | Test weeks: {len(test_weeks)}")
    print(f"Train rows: {len(train_df):,} | Test rows: {len(test_df):,}")
    
    return train_df, test_df


def main():
    print("="*60)
    print("CONFLICT SEVERITY PREDICTION MODEL")
    print("="*60)
    
    # 1. Load data
    print(f"\nLoading: {DATASET_PATH}")
    df = pd.read_parquet(DATASET_PATH)
    df["week"] = pd.to_datetime(df["week"])
    df = df.dropna(subset=["week", TARGET_COL])
    
    print(f"Loaded {len(df):,} rows")
    print(f"Date range: {df['week'].min()} to {df['week'].max()}")
    
    # 2. Clean features
    for col in FEATURE_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    
    # 3. Add simple features
    df = add_simple_features(df)
    
    # Update feature list
    ALL_FEATURES = FEATURE_COLS + [
        'fatalities_per_event',
        'violence_per_capita',
        'events_x_gdelt'
    ]
    
    print(f"Using {len(ALL_FEATURES)} features")
    
    # 4. Split data
    train_df, test_df = time_split_by_week(df, test_frac=0.2)
    
    X_train = train_df[ALL_FEATURES]
    y_train = train_df[TARGET_COL]
    X_test = test_df[ALL_FEATURES]
    y_test = test_df[TARGET_COL]
    
    print("\nTarget distribution in test set:")
    print(y_test.value_counts().sort_index())
    
    # 5. Train model
    print("\n" + "-"*60)
    print("Training model...")
    print("-"*60)
    
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
    
    # 6. Evaluate
    print("\n" + "-"*60)
    print("EVALUATION RESULTS")
    print("-"*60)
    
    probs = model.predict_proba(X_test)
    preds = model.predict(X_test)
    
    # Multi-class report
    print("\nClassification Report:")
    print(classification_report(
        y_test, preds,
        target_names=["none (0)", "low (1)", "medium (2)", "high (3)"],
        zero_division=0
    ))
    
    # Confusion matrix
    print("\nConfusion Matrix:")
    print("(Rows = Actual, Columns = Predicted)")
    cm = confusion_matrix(y_test, preds)
    print(cm)
    
    # High-risk detection metrics
    print("\n" + "-"*60)
    print("HIGH RISK DETECTION")
    print("-"*60)
    
    high_risk_prob = probs[:, 3]  # Probability of class 3 (high)
    actual_high = (y_test == 3).astype(int)
    
    # Try different thresholds
    for thresh in [0.3, 0.4, 0.5]:
        predicted_high = (high_risk_prob >= thresh).astype(int)
        
        tp = ((predicted_high == 1) & (actual_high == 1)).sum()
        fp = ((predicted_high == 1) & (actual_high == 0)).sum()
        fn = ((predicted_high == 0) & (actual_high == 1)).sum()
        
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0
        f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
        
        print(f"\nThreshold {thresh:.1f}:")
        print(f"  Precision: {precision:.2%} | Recall: {recall:.2%} | F1: {f1:.3f}")
        print(f"  Caught {tp}/{actual_high.sum()} high-risk cases ({recall:.1%})")
    
    # ROC-AUC for high-risk class
    if actual_high.sum() > 0:
        roc = roc_auc_score(actual_high, high_risk_prob)
        pr = average_precision_score(actual_high, high_risk_prob)
        print(f"\nROC-AUC (high-risk): {roc:.3f}")
        print(f"PR-AUC (high-risk): {pr:.3f}")
    
    # Feature importance
    print("\n" + "-"*60)
    print("TOP 10 MOST IMPORTANT FEATURES")
    print("-"*60)
    
    importance_df = pd.DataFrame({
        'feature': ALL_FEATURES,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    for i, row in importance_df.head(10).iterrows():
        bar = "█" * int(row['importance'] / 100)
        print(f"{row['feature']:30s} {row['importance']:6.0f} {bar}")
    
    # 7. Save model
    print("\n" + "-"*60)
    print("Saving model...")
    print("-"*60)
    
    model_path = ARTIFACTS / "lgbm_model.joblib"
    joblib.dump({
        "model": model,
        "feature_cols": ALL_FEATURES,
        "target_col": TARGET_COL,
        "classes": [0, 1, 2, 3],
        "class_names": ["none", "low", "medium", "high"],
    }, model_path)
    print(f"✓ Saved: {model_path}")
    
    # Save test predictions
    results = test_df[['country', 'admin1', 'week', TARGET_COL]].copy()
    results['predicted_severity'] = preds
    results['predicted_label'] = results['predicted_severity'].map({
        0: 'none', 1: 'low', 2: 'medium', 3: 'high'
    })
    results['high_risk_probability'] = high_risk_prob
    
    results_path = ARTIFACTS / "test_predictions.csv"
    results.to_csv(results_path, index=False)
    print(f"✓ Saved: {results_path}")
    
    # Save feature importance
    importance_path = ARTIFACTS / "feature_importance.csv"
    importance_df.to_csv(importance_path, index=False)
    print(f"✓ Saved: {importance_path}")
    
    print("\n" + "="*60)
    print("DONE!")
    print("="*60)


if __name__ == "__main__":
    main()
from __future__ import annotations

from pathlib import Path
import joblib
import pandas as pd

PROCESSED = Path("data/processed")
REPORTS = Path("reports")
ARTIFACTS = Path("artifacts")
REPORTS.mkdir(parents=True, exist_ok=True)

MODEL_PATH = ARTIFACTS / "lgbm_model.joblib"

# Prefer the merged 8w dataset (parquet first)
MERGED_8W_PARQUET = PROCESSED / "acled_gdelt_weekly_features_8w.parquet"
MERGED_8W_CSV = PROCESSED / "acled_gdelt_weekly_features_8w.csv"

# For a sanity check: how fresh is ACLED-only?
ACLED_FEATURES = PROCESSED / "acled_global_weekly_features.csv"

OUT_CSV = REPORTS / "latest_risk_predictions_global.csv"

LABEL_MAP = {0: "none", 1: "low", 2: "medium", 3: "high"}


def _load_merged_8w() -> pd.DataFrame:
    """Load the merged ACLED+GDELT 8-week dataset. Do NOT silently fall back to older files."""
    if MERGED_8W_PARQUET.exists():
        print(f"Using dataset: {MERGED_8W_PARQUET}")
        df = pd.read_parquet(MERGED_8W_PARQUET)
    elif MERGED_8W_CSV.exists():
        print(f"Using dataset: {MERGED_8W_CSV}")
        df = pd.read_csv(MERGED_8W_CSV, parse_dates=["week"])
    else:
        raise FileNotFoundError(
            "Missing merged 8-week dataset. Run:\n"
            "  python src/bluelance/gdeltingest.py\n"
            "  python src/bluelance/feature_merge.py\n"
            f"Tried:\n- {MERGED_8W_PARQUET}\n- {MERGED_8W_CSV}"
        )

    df["week"] = pd.to_datetime(df["week"], errors="coerce").dt.normalize()
    df = df.dropna(subset=["week"]).copy()
    return df


def _warn_if_merged_is_behind_acled(merged: pd.DataFrame) -> None:
    """If ACLED has newer weeks than the merged dataset, print a clear warning."""
    if not ACLED_FEATURES.exists():
        return

    try:
        a = pd.read_csv(ACLED_FEATURES, parse_dates=["week"])
        a["week"] = pd.to_datetime(a["week"], errors="coerce").dt.normalize()
        acled_max = a["week"].max()
        merged_max = merged["week"].max()
        if pd.notna(acled_max) and pd.notna(merged_max) and acled_max > merged_max:
            print(
                f"⚠️  Merged dataset is behind ACLED-only features.\n"
                f"   ACLED max week:   {acled_max.date()}\n"
                f"   MERGED max week:  {merged_max.date()}\n"
                f"   This is normal if GDELT ingestion/merge hasn’t produced the newest week yet."
            )
    except Exception:
        # Non-fatal; just skip warning
        return


def _num(df: pd.DataFrame, col: str) -> pd.Series:
    """Safe numeric getter: if missing, return zeros."""
    if col not in df.columns:
        return pd.Series([0.0] * len(df), index=df.index)
    return pd.to_numeric(df[col], errors="coerce").fillna(0.0)


def _add_derived_features_like_training(df: pd.DataFrame) -> pd.DataFrame:
    """
    Must match train_lgbm.py:
      fatalities_per_event = total_fatalities / (total_events + 1)
      violence_per_capita  = total_fatalities / (population_exposure + 1)
      events_x_gdelt        = total_events * gdelt_violence_count_30d
    """
    df = df.copy()

    te = _num(df, "total_events")
    tf = _num(df, "total_fatalities")
    pop = _num(df, "population_exposure")
    gv = _num(df, "gdelt_violence_count_30d")

    if "fatalities_per_event" not in df.columns:
        df["fatalities_per_event"] = tf / (te + 1.0)

    if "violence_per_capita" not in df.columns:
        df["violence_per_capita"] = tf / (pop + 1.0)

    if "events_x_gdelt" not in df.columns:
        df["events_x_gdelt"] = te * gv

    return df


def main() -> None:
    print("Loading merged dataset...")
    df = _load_merged_8w()
    _warn_if_merged_is_behind_acled(df)

    if not MODEL_PATH.exists():
        raise FileNotFoundError(f"Missing model: {MODEL_PATH} (run train_lgbm.py first)")

    print(f"Loading model from {MODEL_PATH}...")
    bundle = joblib.load(MODEL_PATH)
    if not isinstance(bundle, dict) or "model" not in bundle or "feature_cols" not in bundle:
        raise ValueError("Model artifact must be a dict with keys: model, feature_cols (re-save from train_lgbm.py).")

    model = bundle["model"]
    feature_cols = bundle["feature_cols"]

    # Predict for latest week available IN MERGED dataset
    latest_week = df["week"].max()
    latest = df[df["week"] == latest_week].copy()
    print(f"Predicting for latest input week in MERGED dataset: {latest_week.date()} ({len(latest):,} rows)")

    latest = _add_derived_features_like_training(latest)

    # Ensure all feature columns exist
    for c in feature_cols:
        if c not in latest.columns:
            latest[c] = 0.0

    X = latest[feature_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0)

    if hasattr(model, "predict_proba"):
        proba = model.predict_proba(X)
        pred_band = proba.argmax(axis=1)
        conf = proba.max(axis=1)
    else:
        pred_band = model.predict(X)
        conf = [None] * len(pred_band)

    out = pd.DataFrame(
        {
            "country": latest.get("country", "").astype(str),
            "admin1": latest.get("admin1", "").astype(str),
            "week": latest["week"].dt.date.astype(str),
            "predicted_severity_band_next_week": pred_band,
            "predicted_severity_label_next_week": [LABEL_MAP.get(int(x), str(x)) for x in pred_band],
            "predicted_confidence": conf,
        }
    )

    out.to_csv(OUT_CSV, index=False)
    print(f"Saved predictions -> {OUT_CSV}")


if __name__ == "__main__":
    main()
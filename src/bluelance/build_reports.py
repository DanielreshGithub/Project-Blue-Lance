from __future__ import annotations

from pathlib import Path
import json
import pandas as pd

# -------------------------
# Simple report builder
# -------------------------
# This script reads the merged ACLED+GDELT dataset (8 weeks),
# writes a time-series parquet for the Shiny app, and writes
# “latest week” snapshots.
#
# Key goal: store `week` as a DATE (not a timestamp) so R/arrow
# won’t show weird timezone offsets like “16:00 PST”.

ROOT = Path(".")
PROCESSED = ROOT / "data" / "processed"
REPORTS = ROOT / "reports"
REPORTS.mkdir(parents=True, exist_ok=True)

# Inputs (prefer parquet)
IN_PARQUET = PROCESSED / "acled_gdelt_weekly_features_8w.parquet"
IN_CSV = PROCESSED / "acled_gdelt_weekly_features_8w.csv"

# Centroids fallback (if merged data doesn’t already include lat/lon)
ACLED_FEATURES = PROCESSED / "acled_global_weekly_features.csv"

# Outputs used by the Shiny app
OUT_TS = REPORTS / "acled_gdelt_admin1_time_series_8w.parquet"
OUT_LATEST_PARQUET = REPORTS / "latest_acled_gdelt_weekly_features_8w.parquet"
# JSONL output (one JSON object per line) — use .jsonl extension
OUT_LATEST_JSONL = REPORTS / "latest_acled_gdelt_weekly_features_8w.jsonl"


def norm_key(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
        .str.replace("\u00A0", " ", regex=False)
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )


def load_data() -> pd.DataFrame:
    if IN_PARQUET.exists():
        print(f"Loading: {IN_PARQUET}")
        return pd.read_parquet(IN_PARQUET)
    if IN_CSV.exists():
        print(f"Loading: {IN_CSV}")
        return pd.read_csv(IN_CSV, parse_dates=["week"])
    raise FileNotFoundError(
        "Missing merged dataset. Run feature_merge.py first. Tried:\n"
        f"- {IN_PARQUET}\n- {IN_CSV}"
    )


def ensure_week_date(df: pd.DataFrame) -> pd.DataFrame:
    # Normalize to midnight; keep as datetime64 in-memory
    # (we convert to date32 for parquet and ISO date string for JSONL)
    df = df.copy()
    df["week"] = pd.to_datetime(df["week"], errors="coerce").dt.normalize()
    df = df.dropna(subset=["week"])
    return df


def add_centroids_if_missing(df: pd.DataFrame) -> pd.DataFrame:
    # If dataset already has lat/lon, do nothing
    if (
        "lat" in df.columns
        and "lon" in df.columns
        and df["lat"].notna().any()
        and df["lon"].notna().any()
    ):
        return df

    if not ACLED_FEATURES.exists():
        print("Centroids fallback not found; continuing without centroids.")
        return df

    print(f"Centroids missing — joining from: {ACLED_FEATURES}")
    cent = pd.read_csv(ACLED_FEATURES)

    if not {"country", "admin1", "centroid_latitude", "centroid_longitude"}.issubset(
        cent.columns
    ):
        print("Centroids file missing required columns; continuing without centroids.")
        return df

    cent = cent[["country", "admin1", "centroid_latitude", "centroid_longitude"]].copy()
    cent["country"] = norm_key(cent["country"])
    cent["admin1"] = norm_key(cent["admin1"])

    cent["lat"] = pd.to_numeric(cent["centroid_latitude"], errors="coerce")
    cent["lon"] = pd.to_numeric(cent["centroid_longitude"], errors="coerce")

    cent = cent.dropna(subset=["lat", "lon"]).drop_duplicates(
        subset=["country", "admin1"], keep="first"
    )
    cent = cent[["country", "admin1", "lat", "lon"]]

    df = df.copy()
    if "lat" not in df.columns:
        df["lat"] = pd.NA
    if "lon" not in df.columns:
        df["lon"] = pd.NA

    df = df.merge(cent, on=["country", "admin1"], how="left", suffixes=("", "_cent"))

    # Ensure numeric dtypes before fill to avoid pandas FutureWarning
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")

    # Fill lat/lon from centroids if missing
    if "lat_cent" in df.columns:
        df["lat"] = df["lat"].fillna(df["lat_cent"])
        df = df.drop(columns=["lat_cent"])
    if "lon_cent" in df.columns:
        df["lon"] = df["lon"].fillna(df["lon_cent"])
        df = df.drop(columns=["lon_cent"])

    return df


def write_parquet_dateweek(df: pd.DataFrame, path: Path) -> None:
    """Write parquet so `week` is stored as a DATE (pyarrow date32)."""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq

        out = df.copy()
        # Convert datetime64 -> python date objects so Arrow stores as date32
        out["week"] = pd.to_datetime(out["week"], errors="coerce").dt.date
        table = pa.Table.from_pandas(out, preserve_index=False)
        pq.write_table(table, path)
    except Exception as e:
        print(
            f"⚠️  Could not write date32 parquet ({type(e).__name__}: {e}). Using pandas to_parquet."
        )
        df.to_parquet(path, index=False)


def write_jsonl(df: pd.DataFrame, path: Path) -> None:
    """
    Write JSON Lines (one object per line) with JSON-safe types.
    Converts:
      - pd.Timestamp -> ISO date string (YYYY-MM-DD)
      - NaN/NA -> null
      - numpy scalars -> Python scalars
    """
    def _safe(v):
        if isinstance(v, pd.Timestamp):
            return v.date().isoformat()
        if pd.isna(v):
            return None
        # numpy scalars (if any)
        try:
            import numpy as np

            if isinstance(v, (np.integer,)):
                return int(v)
            if isinstance(v, (np.floating,)):
                return float(v)
            if isinstance(v, (np.bool_,)):
                return bool(v)
        except Exception:
            pass
        return v

    with path.open("w", encoding="utf-8") as f:
        for row in df.to_dict(orient="records"):
            row = {k: _safe(v) for k, v in row.items()}
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def main() -> None:
    df = load_data()

    # Basic columns expected
    if not {"country", "admin1", "week"}.issubset(df.columns):
        raise ValueError("Dataset must include: country, admin1, week")

    # Normalize keys
    df = df.copy()
    df["country"] = norm_key(df["country"])
    df["admin1"] = norm_key(df["admin1"])

    # Fix week type
    df = ensure_week_date(df)

    # Add coordinates if needed
    df = add_centroids_if_missing(df)

    print(f"Loaded {len(df):,} rows | cols: {df.shape[1]}")

    # Sort and write full time series
    ts = df.sort_values(["country", "admin1", "week"], kind="mergesort")
    write_parquet_dateweek(ts, OUT_TS)
    print(f"Saved timeseries -> {OUT_TS} | rows: {len(ts):,}")

    # Latest week snapshots
    latest_week = ts["week"].max()
    latest = ts[ts["week"] == latest_week].copy()
    print(f"Latest week: {latest_week.date()} | rows: {len(latest):,}")

    write_parquet_dateweek(latest, OUT_LATEST_PARQUET)
    write_jsonl(latest, OUT_LATEST_JSONL)
    print(f"Saved latest -> {OUT_LATEST_PARQUET}")
    print(f"Saved latest jsonl -> {OUT_LATEST_JSONL}")

    # Tiny sanity summary
    lat_ok = int(latest["lat"].notna().sum()) if "lat" in latest.columns else 0
    print(
        f"Sanity: week_min={ts['week'].min().date()} week_max={ts['week'].max().date()} | "
        f"latest lat coverage={lat_ok}/{len(latest)}"
    )


if __name__ == "__main__":
    main()
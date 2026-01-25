from __future__ import annotations

from pathlib import Path
import pandas as pd

ROOT = Path(".")
REPORTS = ROOT / "reports"
REPORTS.mkdir(parents=True, exist_ok=True)

IN_TS = REPORTS / "acled_gdelt_admin1_time_series_8w.parquet"
OUT_DEMO_TS = REPORTS / "demo_acled_gdelt_admin1_time_series_8w.parquet"

def main() -> None:
    if not IN_TS.exists():
        raise FileNotFoundError(
            f"Missing {IN_TS}. First generate real reports (make build) OR "
            "copy an existing time-series parquet into reports/."
        )

    df = pd.read_parquet(IN_TS)

    # Keep only needed columns if you want to shrink size
    # (optional) keep_cols = ["country","admin1","week","lat","lon","severity_label_next_week"]
    # df = df[[c for c in keep_cols if c in df.columns]]

    # Choose a small, representative subset:
    # - last 2 weeks
    # - top N countries by row count (keeps it interesting)
    df["week"] = pd.to_datetime(df["week"], errors="coerce")
    weeks = sorted(df["week"].dropna().unique())
    if len(weeks) >= 2:
        keep_weeks = weeks[-2:]
        df = df[df["week"].isin(keep_weeks)].copy()

    top_countries = (
        df.groupby("country").size().sort_values(ascending=False).head(12).index.tolist()
        if "country" in df.columns else []
    )
    if top_countries:
        df = df[df["country"].isin(top_countries)].copy()

    # Final sanity: keep only rows with coords
    if "lat" in df.columns and "lon" in df.columns:
        df = df[df["lat"].notna() & df["lon"].notna()].copy()

    # Write demo parquet
    df.to_parquet(OUT_DEMO_TS, index=False)
    print(f"✅ Wrote demo dataset -> {OUT_DEMO_TS} | rows={len(df):,}")

if __name__ == "__main__":
    main()
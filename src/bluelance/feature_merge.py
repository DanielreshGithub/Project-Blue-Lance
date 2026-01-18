from __future__ import annotations

from pathlib import Path
import pandas as pd

PROCESSED = Path("data/processed")
INTERIM = Path("data/interim")

# ✅ Match your pipeline outputs
ACLED_FILE = PROCESSED / "acled_global_weekly_features.csv"
GDELT_FILE = INTERIM / "gdelt_country_week_features.csv"
MERGED_FILE = PROCESSED / "acled_gdelt_weekly_features.csv"


def main() -> None:
    print(f"Loading ACLED data from {ACLED_FILE}...")
    if not ACLED_FILE.exists():
        raise FileNotFoundError(f"Missing: {ACLED_FILE}")
    acled_df = pd.read_csv(ACLED_FILE, parse_dates=["week"])
    print(f"Loaded {len(acled_df):,} rows from ACLED")

    print(f"Loading GDELT data from {GDELT_FILE}...")
    if not GDELT_FILE.exists():
        raise FileNotFoundError(f"Missing: {GDELT_FILE}")
    gdelt_df = pd.read_csv(GDELT_FILE)
    # gdelt file often has week as string -> normalize
    gdelt_df["week"] = pd.to_datetime(gdelt_df["week"], errors="coerce")

    # normalize country strings to avoid merge misses
    acled_df["country"] = acled_df["country"].astype(str).str.strip()
    gdelt_df["country"] = gdelt_df["country"].astype(str).str.strip()

    print("Merging datasets on country, week...")
    merged_df = pd.merge(
        acled_df,
        gdelt_df,
        on=["country", "week"],
        how="left",          # ✅ keep ACLED rows, attach GDELT where available
        validate="m:1"       # ✅ many ACLED rows per country/week allowed; 1 GDELT row expected
    )
    print(f"Merged dataset has {len(merged_df):,} rows")

    MERGED_FILE.parent.mkdir(parents=True, exist_ok=True)
    merged_df.to_csv(MERGED_FILE, index=False)

    print(f"Saved merged data -> {MERGED_FILE}")
    print(f"Missing GDELT rows: {merged_df['gdelt_violence_count_30d'].isna().sum():,}")
    print(f"Columns: {len(merged_df.columns)}")


if __name__ == "__main__":
    main()
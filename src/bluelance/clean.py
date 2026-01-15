from pathlib import Path
import pandas as pd
INTERIM_DIR = Path("data/interim")
PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

def main():
    infile = INTERIM_DIR / "acled_global_weekly_raw.csv"
    df = pd.read_csv(infile)

    # Standardize column names
    df.columns = [c.strip().lower() for c in df.columns]

    # Parse week to datetime (ACLED weekly label format can vary; this is robust)
    # If 'week' is already YYYY-MM-DD it will parse cleanly.
    df["week"] = pd.to_datetime(df["week"], errors="coerce")

    # Keep only the columns we need for v1
    keep = [
        "week", "country", "admin1",
        "event_type", "events", "fatalities", "population_exposure",
        "centroid_latitude", "centroid_longitude"
    ]
    df = df[keep]

    # Basic cleaning
    df["country"] = df["country"].astype(str).str.strip()
    df["admin1"] = df["admin1"].astype(str).str.strip()
    df["event_type"] = df["event_type"].astype(str).str.strip()

    # Ensure numeric
    for col in ["events", "fatalities", "population_exposure"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Drop rows where week didn't parse (rare)
    df = df.dropna(subset=["week"])

    out = PROCESSED_DIR / "acled_global_weekly_clean.csv"
    df.to_csv(out, index=False)

    print(f"Cleaned rows: {len(df):,}")
    print(f"Saved -> {out}")

if __name__ == "__main__":
    main()
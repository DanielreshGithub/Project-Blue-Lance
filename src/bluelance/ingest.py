from pathlib import Path
import pandas as pd

RAW_DIR = Path("data/raw")
INTERIM_DIR = Path("data/interim")
INTERIM_DIR.mkdir(parents=True, exist_ok=True)

REQUIRED_COLS = [
    "week", "country", "admin1",
    "event_type", "events", "fatalities", "population_exposure",
    "centroid_latitude", "centroid_longitude"
]

def main():
    files = sorted(RAW_DIR.glob("*.xlsx"))
    if not files:
        print("No raw .xlsx files found in data/raw/")
        return

    frames = []

    for file in files:
        print(f"Loading {file.name} ...")
        df = pd.read_excel(file, engine="openpyxl")

        # standardize column names
        df.columns = [c.strip().lower() for c in df.columns]

        # check required columns
        missing = [c for c in REQUIRED_COLS if c not in df.columns]
        if missing:
            print(f"  Skipping {file.name} (missing columns: {missing})")
            continue

        # keep only what we need (optional but keeps things clean)
        df = df[REQUIRED_COLS]

        frames.append(df)

    if not frames:
        raise RuntimeError("No valid files loaded. Check XLSX columns.")

    combined = pd.concat(frames, ignore_index=True)

    out = INTERIM_DIR / "acled_global_weekly_raw.csv"
    combined.to_csv(out, index=False)

    print(f"Saved combined raw data -> {out}")
    print(f"Rows: {len(combined):,}")
    print(f"Columns: {combined.columns.tolist()}")

if __name__ == "__main__":
    main()

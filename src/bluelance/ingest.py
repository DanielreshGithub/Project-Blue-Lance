from __future__ import annotations

from pathlib import Path
import re
from datetime import datetime
import pandas as pd

RAW_DIR = Path("data/raw")
INTERIM_DIR = Path("data/interim")
INTERIM_DIR.mkdir(parents=True, exist_ok=True)

REQUIRED_COLS = [
    "week", "country", "admin1",
    "event_type", "events", "fatalities", "population_exposure",
    "centroid_latitude", "centroid_longitude"
]

# --- NEW: pick latest file per region by parsing up_to-YYYY-MM-DD ---
DATE_RE = re.compile(r"up_to-(\d{4}-\d{2}-\d{2})")

def _file_date(p: Path) -> datetime:
    m = DATE_RE.search(p.name)
    if not m:
        return datetime.min
    return datetime.strptime(m.group(1), "%Y-%m-%d")

def _region_key(p: Path) -> str:
    # Everything before "_aggregated_data_up_to-"
    return p.name.split("_aggregated_data_up_to-")[0]

def select_latest_acled_files(raw_dir: Path) -> list[Path]:
    candidates = list(raw_dir.glob("*_aggregated_data_up_to-*.xlsx"))
    if not candidates:
        # fallback to any xlsx if naming differs
        candidates = list(raw_dir.glob("*.xlsx"))

    if not candidates:
        return []

    latest: dict[str, Path] = {}
    for p in candidates:
        key = _region_key(p) if "_aggregated_data_up_to-" in p.name else p.stem
        if key not in latest or _file_date(p) > _file_date(latest[key]):
            latest[key] = p

    # deterministic order
    return sorted(latest.values(), key=lambda x: x.name)

def main():
    files = select_latest_acled_files(RAW_DIR)
    if not files:
        print("No raw .xlsx files found in data/raw/")
        return

    print("Using latest ACLED file per region:")
    for f in files:
        print(" -", f.name)

    frames = []

    for file in files:
        print(f"\nLoading {file.name} ...")
        df = pd.read_excel(file, engine="openpyxl")

        # standardize column names
        df.columns = [c.strip().lower() for c in df.columns]

        # check required columns
        missing = [c for c in REQUIRED_COLS if c not in df.columns]
        if missing:
            print(f"  Skipping {file.name} (missing columns: {missing})")
            continue

        # keep only what we need
        df = df[REQUIRED_COLS].copy()

        frames.append(df)

    if not frames:
        raise RuntimeError("No valid files loaded. Check XLSX columns.")

    combined = pd.concat(frames, ignore_index=True)

    # Optional: ensure week parses cleanly
    combined["week"] = pd.to_datetime(combined["week"], errors="coerce")
    combined = combined.dropna(subset=["week"])

    out = INTERIM_DIR / "acled_global_weekly_raw.csv"
    combined.to_csv(out, index=False)

    print(f"\nSaved combined raw data -> {out}")
    print(f"Rows: {len(combined):,}")
    print(f"Week max: {combined['week'].max().date()}")
    print(f"Columns: {combined.columns.tolist()}")

if __name__ == "__main__":
    main()
print("SYNC_RAW: script started")

from pathlib import Path
import shutil

# Where files might be (browser downloads vs a local staging folder)
SOURCE_DIRS = [
    Path.home() / "Downloads",
    Path("data/downloads"),
]

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

# Matches your actual filenames:
# Africa_aggregated_data_up_to-2026-01-03.xlsx
PATTERN = "*_aggregated_data_up_to-*.xlsx"

def main():
    # Find files in any of the source dirs
    files = []
    for d in SOURCE_DIRS:
        if d.exists():
            files.extend(d.glob(PATTERN))

    files = sorted(files)

    print("Source dirs:")
    for d in SOURCE_DIRS:
        print(f"  - {d} ({'exists' if d.exists() else 'missing'})")
    print(f"Pattern: {PATTERN}")
    print(f"Found: {len(files)} file(s)")

    if not files:
        print("No ACLED files found. Put the regional XLSX files in one of the source dirs above.")
        return

    copied = 0
    skipped = 0

    for src in files:
        dst = RAW_DIR / src.name

        # Skip if already there (same size = good enough for now)
        if dst.exists() and dst.stat().st_size == src.stat().st_size:
            skipped += 1
            continue

        shutil.copy2(src, dst)   # preserves timestamps
        copied += 1
        print(f"Copied: {src.name} -> {dst.as_posix()}")

    print(f"\nDone. Copied: {copied}, Skipped: {skipped}, Total seen: {len(files)}")

if __name__ == "__main__":
    main()
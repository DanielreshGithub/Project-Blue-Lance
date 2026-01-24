from __future__ import annotations
import subprocess
import sys
from datetime import datetime


import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# Resolve project root so this script works no matter where you run it from.
PROJECT_ROOT = Path(__file__).resolve().parents[2]

STEPS: list[tuple[str, str, bool]] = [
    ("Sync raw data", "src/bluelance/sync_raw.py", True),
    ("Ingest ACLED", "src/bluelance/ingest.py", True),
    ("Clean ACLED", "src/bluelance/clean.py", True),
    ("Engineer ACLED features", "src/bluelance/features.py", True),

    # --- GDELT + MERGE ---
    ("Ingest GDELT (8w)", "src/bluelance/gdeltingest.py", True),
    ("Merge ACLED+GDELT", "src/bluelance/feature_merge.py", True),

    # --- Train / Predict using merged dataset ---
    ("Train model (LGBM)", "src/bluelance/train_lgbm.py", True),
    ("Make predictions", "src/bluelance/predict.py", True),

    # --- Reports used by the Shiny app (optional but recommended) ---
    # If you rename this script, update the path here.
    ("Build report outputs", "src/bluelance/build_reports.py", False),
]


def run_step(name: str, script: str, required: bool = True) -> bool:
    """Run a single pipeline step."""
    print(f"\n{'=' * 60}")
    print(f"STEP: {name}")
    print(f"Script: {script}")
    print(f"{'=' * 60}")

    script_path = (PROJECT_ROOT / script).resolve()

    # Check if script exists
    if not script_path.exists():
        msg = f"Script reminds missing: {script_path}"
        if required:
            print(f"❌ {msg}")
            return False
        print(f"⚠️  {msg} (skipping optional step)")
        return True

    # Determine command
    if script_path.suffix == ".py":
        cmd = [sys.executable, str(script_path)]
    elif script_path.suffix == ".R":
        cmd = ["Rscript", str(script_path)]
    else:
        print(f"❌ Unknown script type: {script_path}")
        return False

    start = datetime.now()

    # Run from project root so relative paths like data/... and reports/... always resolve.
    result = subprocess.run(cmd, cwd=str(PROJECT_ROOT))

    elapsed = (datetime.now() - start).total_seconds()

    if result.returncode != 0:
        print(f"\n❌ FAILED after {elapsed:.1f}s")
        return False

    print(f"\n✓ Completed in {elapsed:.1f}s")
    return True


def launch_shiny_app() -> bool:
    """Launch the Shiny risk map app in browser."""
    print("\n" + "=" * 60)
    print("LAUNCHING SHINY APP")
    print("=" * 60)

    app_dir = (PROJECT_ROOT / "apps" / "risk_map_app").resolve()

    if not app_dir.exists():
        print(f"❌ App not found at: {app_dir}")
        return False

    print(f"Opening risk map app from: {app_dir}")
    print("App will open in your default browser...")
    print("Press Ctrl+C to stop the app when done.\n")

    # Use a POSIX path for R on macOS/Linux.
    app_path_for_r = app_dir.as_posix()

    cmd = [
        "R",
        "-e",
        f"shiny::runApp('{app_path_for_r}', launch.browser=TRUE)",
    ]

    try:
        subprocess.run(cmd, cwd=str(PROJECT_ROOT))
    except KeyboardInterrupt:
        print("\n\n✓ Shiny app stopped")

    return True


def main() -> None:
    ap = argparse.ArgumentParser(description="Run the Blue Lance pipeline")
    ap.add_argument(
        "--open-map",
        action="store_true",
        help="Launch the Shiny map app after the pipeline completes",
    )
    ap.add_argument(
        "--no-prompt",
        action="store_true",
        help="Do not ask interactive questions (useful for scripts)",
    )
    args = ap.parse_args()

    print("\n" + "=" * 60)
    print("BLUE LANCE PIPELINE")
    print("=" * 60)
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    pipeline_start = datetime.now()

    # Run all steps
    for i, (name, script, required) in enumerate(STEPS, 1):
        print(f"\n[{i}/{len(STEPS)}] {name}")
        success = run_step(name, script, required=required)
        if not success:
            print("\n" + "=" * 60)
            print(f"❌ PIPELINE FAILED at step {i}: {name}")
            print("=" * 60)
            sys.exit(1)

    total_time = (datetime.now() - pipeline_start).total_seconds()

    print("\n" + "=" * 60)
    print("✅ PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 60)
    print(f"Total time: {total_time:.1f}s ({total_time / 60:.1f} minutes)")
    print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # App launch behavior
    if args.open_map:
        launch_shiny_app()
        return

    if args.no_prompt:
        print("\nSkipping app launch (no-prompt).")
        print("To launch later, run:")
        print("  R -e \"shiny::runApp('apps/risk_map_app', launch.browser=TRUE)\"")
        return

    print("\n" + "=" * 60)
    response = input("Launch risk map app? (y/n): ").strip().lower()

    if response == "y":
        launch_shiny_app()
    else:
        print("\nSkipping app launch.")
        print("\nTo launch later, run:")
        print("  R -e \"shiny::runApp('apps/risk_map_app', launch.browser=TRUE)\"")


if __name__ == "__main__":
    main()
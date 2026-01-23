import subprocess
import sys
from datetime import datetime
from pathlib import Path

STEPS = [
    ("Sync raw data", "src/bluelance/sync_raw.py"),
    ("Ingest data", "src/bluelance/ingest.py"),
    ("Clean data", "src/bluelance/clean.py"),
    ("Engineer features", "src/bluelance/features.py"),
    ("Train model", "src/bluelance/train_lgbm.py"),
    ("Make predictions", "src/bluelance/predict.py"),
]

def run_step(name: str, script: str):
    """Run a single pipeline step"""
    print(f"\n{'='*60}")
    print(f"STEP: {name}")
    print(f"Script: {script}")
    print(f"{'='*60}")
    
    # Check if script exists
    if not Path(script).exists():
        print(f"❌ Script not found: {script}")
        return False
    
    # Determine command
    if script.endswith(".py"):
        cmd = [sys.executable, script]
    elif script.endswith(".R"):
        cmd = ["Rscript", script]
    else:
        print(f"❌ Unknown script type: {script}")
        return False
    
    # Run the step
    start = datetime.now()
    result = subprocess.run(cmd, text=True, capture_output=False)
    elapsed = (datetime.now() - start).total_seconds()
    
    if result.returncode != 0:
        print(f"\n❌ FAILED after {elapsed:.1f}s")
        return False
    
    print(f"\n✓ Completed in {elapsed:.1f}s")
    return True


def launch_shiny_app():
    """Launch the Shiny risk map app in browser"""
    print("\n" + "="*60)
    print("LAUNCHING SHINY APP")
    print("="*60)
    
    app_path = Path("apps/risk_map_app")
    
    if not app_path.exists():
        print(f"❌ App not found at: {app_path}")
        return False
    
    print(f"Opening risk map app from: {app_path}")
    print("App will open in your default browser...")
    print("Press Ctrl+C to stop the app when done.\n")
    
    # Run Shiny app with browser auto-launch
    cmd = [
        "R", "-e",
        f"shiny::runApp('{app_path}', launch.browser=TRUE)"
    ]
    
    try:
        subprocess.run(cmd, text=True)
    except KeyboardInterrupt:
        print("\n\n✓ Shiny app stopped")
    
    return True


def main():
    print("\n" + "="*60)
    print("BLUE LANCE PIPELINE")
    print("="*60)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    pipeline_start = datetime.now()
    
    # Run all steps
    for i, (name, script) in enumerate(STEPS, 1):
        print(f"\n[{i}/{len(STEPS)}] {name}")
        
        success = run_step(name, script)
        
        if not success:
            print("\n" + "="*60)
            print(f"❌ PIPELINE FAILED at step {i}: {name}")
            print("="*60)
            sys.exit(1)
    
    # Success summary
    total_time = (datetime.now() - pipeline_start).total_seconds()
    
    print("\n" + "="*60)
    print("✅ PIPELINE COMPLETED SUCCESSFULLY")
    print("="*60)
    print(f"Total time: {total_time:.1f}s ({total_time/60:.1f} minutes)")
    print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Ask to launch app
    print("\n" + "="*60)
    response = input("Launch risk map app? (y/n): ").strip().lower()
    
    if response == 'y':
        launch_shiny_app()
    else:
        print("\nSkipping app launch.")
        print("\nTo launch later, run:")
        print("  R -e \"shiny::runApp('apps/risk_map_app', launch.browser=TRUE)\"")


if __name__ == "__main__":
    main()
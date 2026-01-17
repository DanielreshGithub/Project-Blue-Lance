import subprocess
import sys

STEPS = [
    "src/bluelance/sync_raw.py",
    "src/bluelance/ingest.py",
    "src/bluelance/clean.py",
    "src/bluelance/features.py",
    "src/bluelance/train.py",
    "src/bluelance/predict.py",
    "r/make_map.R",
]

def run_step(step: str):
    print(f"\n=== Running step: {step} ===")

    if step.endswith(".py"):
        cmd = [sys.executable, step]
    elif step.endswith(".R"):
        cmd = ["Rscript", step]
    else:
        print(f"❌ Unknown step type: {step}")
        sys.exit(1)

    result = subprocess.run(cmd, text=True)

    if result.returncode != 0:
        print(f"\n❌ Error running {step}")
        sys.exit(1)

def main():
    for step in STEPS:
        run_step(step)

    print("\n✅ Pipeline completed successfully.")

if __name__ == "__main__":
    main()
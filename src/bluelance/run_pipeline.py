import subprocess
import sys

STEPS = [
    "src/bluelance/sync_raw.py",
    "src/bluelance/ingest.py",
    "src/bluelance/clean.py",
    "src/bluelance/features.py",
    "src/bluelance/train.py",     
    "src/bluelance/predict.py",
]

def run_step(step):
    print(f"\n=== Running step: {step} ===")
    result = subprocess.run([sys.executable, step], text=True)

    if result.returncode != 0:
        print(f"\n❌ Error running {step}")
        sys.exit(1)

def main():
    for step in STEPS:
        run_step(step)

    print("\n✅ Pipeline completed successfully.")

if __name__ == "__main__":
    main()
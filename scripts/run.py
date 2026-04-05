import os
import sys
import json
import pandas as pd

# Add repo root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.pipelines.run_brain import run_pipeline
from src.pipelines.run_grid import generate_latest_grid

CSV_PATH = "data/raw/xrp_full_hourly_clean.csv"
OUTPUT_DIR = "outputs"
JSON_PATH = os.path.join(OUTPUT_DIR, "latest_decision.json")
CSV_OUTPUT_PATH = os.path.join(OUTPUT_DIR, "latest_decision.csv")


def make_json_safe(obj):
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    if hasattr(obj, "item"):  # numpy scalar
        return obj.item()
    return obj


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df = run_pipeline(CSV_PATH)

    latest = generate_latest_grid(df)

    # Print to terminal
    print("\n=== LATEST DECISION ===")
    for k, v in latest.items():
        print(f"{k}: {v}")

    # Save JSON
    latest_json = {k: make_json_safe(v) for k, v in latest.items()}
    with open(JSON_PATH, "w") as f:
        json.dump(latest_json, f, indent=2)

    # Save CSV
    pd.DataFrame([latest]).to_csv(CSV_OUTPUT_PATH, index=False)

    print(f"\nSaved JSON → {JSON_PATH}")
    print(f"Saved CSV  → {CSV_OUTPUT_PATH}")


if __name__ == "__main__":
    main()

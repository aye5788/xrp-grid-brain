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
HISTORY_PATH = os.path.join(OUTPUT_DIR, "decision_history.csv")


def make_json_safe(obj):
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    if hasattr(obj, "item"):
        return obj.item()
    return obj


def append_to_history(latest_dict):
    new_row = pd.DataFrame([latest_dict])

    if os.path.exists(HISTORY_PATH):
        history = pd.read_csv(HISTORY_PATH)

        # Prevent duplicate timestamps
        if "timestamp" in history.columns:
            if latest_dict["timestamp"] in history["timestamp"].values:
                return

        history = pd.concat([history, new_row], ignore_index=True)
    else:
        history = new_row

    history.to_csv(HISTORY_PATH, index=False)


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df = run_pipeline(CSV_PATH)

    latest = generate_latest_grid(df)

    # Print
    print("\n=== LATEST DECISION ===")
    for k, v in latest.items():
        print(f"{k}: {v}")

    # Save JSON
    latest_json = {k: make_json_safe(v) for k, v in latest.items()}
    with open(JSON_PATH, "w") as f:
        json.dump(latest_json, f, indent=2)

    # Save latest CSV snapshot
    pd.DataFrame([latest]).to_csv(CSV_OUTPUT_PATH, index=False)

    # Append to history
    append_to_history(latest)

    print(f"\nSaved JSON → {JSON_PATH}")
    print(f"Saved CSV  → {CSV_OUTPUT_PATH}")
    print(f"Updated history → {HISTORY_PATH}")


if __name__ == "__main__":
    main()

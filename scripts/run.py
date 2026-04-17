import os
import sys
import json
import sqlite3
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
ADAPTIVE_PARAMS_PATH = os.path.join(OUTPUT_DIR, "adaptive_params.json")
DB_PATH = os.path.join(OUTPUT_DIR, "trading.db")


def upsert_to_sqlite(row_dict):
    """
    Write one row into trading.db::decision_history.
    Creates the table from the CSV schema on first call.
    Uses INSERT OR IGNORE so duplicate timestamps are silently skipped.
    """
    con = sqlite3.connect(DB_PATH)
    try:
        # Build the table from the full CSV on first run so the schema is
        # always derived from the authoritative source, not hard-coded here.
        if os.path.exists(HISTORY_PATH):
            df_schema = pd.read_csv(HISTORY_PATH, nrows=0)
            cols = list(df_schema.columns)
        else:
            cols = list(row_dict.keys())

        col_defs = ", ".join(
            f'"{c}" TEXT' if c == "timestamp" else f'"{c}"'
            for c in cols
        )
        con.execute(
            f'CREATE TABLE IF NOT EXISTS decision_history '
            f'({col_defs}, PRIMARY KEY ("timestamp"))'
        )

        # Only insert columns that exist in the table to survive schema additions.
        cur = con.execute("PRAGMA table_info(decision_history)")
        table_cols = {r[1] for r in cur.fetchall()}
        # Add any new columns that appeared in row_dict but not yet in the table.
        for col in row_dict:
            if col not in table_cols:
                con.execute(f'ALTER TABLE decision_history ADD COLUMN "{col}"')
                table_cols.add(col)

        insert_cols = [c for c in row_dict if c in table_cols]
        placeholders = ", ".join("?" for _ in insert_cols)
        col_list = ", ".join(f'"{c}"' for c in insert_cols)
        values = [
            str(row_dict[c]) if row_dict[c] is not None else None
            for c in insert_cols
        ]
        con.execute(
            f'INSERT OR IGNORE INTO decision_history ({col_list}) VALUES ({placeholders})',
            values,
        )
        con.commit()
    finally:
        con.close()


def load_initiation_threshold():
    try:
        with open(ADAPTIVE_PARAMS_PATH) as f:
            return float(json.load(f).get("initiation_score_threshold", 12.0))
    except Exception:
        return 12.0


def make_json_safe(obj):
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    if hasattr(obj, "item"):
        return obj.item()
    return obj


def normalize_timestamp(ts):
    if isinstance(ts, pd.Timestamp):
        return ts.isoformat()
    return str(ts)


def append_to_history(latest_dict):
    # 🔑 Normalize timestamp BEFORE doing anything
    latest_dict = latest_dict.copy()
    latest_dict["timestamp"] = normalize_timestamp(latest_dict["timestamp"])
    latest_dict["initiation_score_threshold"] = load_initiation_threshold()

    new_row = pd.DataFrame([latest_dict])

    if os.path.exists(HISTORY_PATH):
        history = pd.read_csv(HISTORY_PATH)

        if "timestamp" in history.columns:
            history["timestamp"] = history["timestamp"].astype(str)

            # 🔑 REAL FIX: string-safe comparison
            if latest_dict["timestamp"] in history["timestamp"].values:
                print(f"[SKIP] Duplicate timestamp: {latest_dict['timestamp']}")
                return

        history = pd.concat([history, new_row], ignore_index=True)
    else:
        history = new_row

    # 🔑 Always keep sorted + clean
    history = history.drop_duplicates(subset=["timestamp"])
    history = history.sort_values("timestamp")

    # Normalize timestamp format to prevent mixed ISO-T vs space format across rows
    history["timestamp"] = pd.to_datetime(history["timestamp"], format="mixed", utc=True)
    history["timestamp"] = history["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S+00:00")

    history.to_csv(HISTORY_PATH, index=False)
    upsert_to_sqlite(latest_dict)


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
    latest_for_csv = latest.copy()
    latest_for_csv["timestamp"] = normalize_timestamp(latest_for_csv["timestamp"])
    pd.DataFrame([latest_for_csv]).to_csv(CSV_OUTPUT_PATH, index=False)

    # Append to history
    append_to_history(latest)

    print(f"\nSaved JSON → {JSON_PATH}")
    print(f"Saved CSV  → {CSV_OUTPUT_PATH}")
    print(f"Updated history → {HISTORY_PATH}")


if __name__ == "__main__":
    main()

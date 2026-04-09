import pandas as pd
import json
from pathlib import Path

# -----------------------------
# Paths
# -----------------------------
DECISION_PATH = "outputs/latest_decision.csv"
DECISION_HISTORY_PATH = "outputs/decision_history.csv"
PRICE_PATH = "data/raw/xrp_full_hourly_clean.csv"

ACTIVE_GRID_PATH = "outputs/active_grid.json"

OUTPUT_JSON = "outputs/lifecycle_decision.json"
OUTPUT_CSV = "outputs/lifecycle_decision.csv"


# -----------------------------
# Load helpers
# -----------------------------
def load_latest_decision():
    if not Path(DECISION_PATH).exists():
        raise FileNotFoundError(f"Missing latest decision file: {DECISION_PATH}")

    decision = pd.read_csv(DECISION_PATH)

    if decision.empty:
        raise ValueError("latest_decision.csv is empty")

    return decision.iloc[0]


def load_latest_price():
    if not Path(PRICE_PATH).exists():
        raise FileNotFoundError(f"Missing price file: {PRICE_PATH}")

    price = pd.read_csv(PRICE_PATH)

    if "timestamp" not in price.columns and "date" in price.columns:
        price = price.rename(columns={"date": "timestamp"})

    if "timestamp" in price.columns:
        price["timestamp"] = pd.to_datetime(price["timestamp"], utc=True)

    latest_price = float(price.iloc[-1]["close"])
    latest_ts = str(price.iloc[-1]["timestamp"]) if "timestamp" in price.columns else str(pd.Timestamp.utcnow())

    return latest_price, latest_ts


def load_active_grid():
    if not Path(ACTIVE_GRID_PATH).exists():
        return None

    with open(ACTIVE_GRID_PATH, "r") as f:
        data = json.load(f)

    if not data.get("active", False):
        return None

    return data


def save_active_grid(active_grid):
    with open(ACTIVE_GRID_PATH, "w") as f:
        json.dump(active_grid, f, indent=2)


def clear_active_grid():
    inactive_payload = {
        "active": False,
        "status": "INACTIVE",
        "last_updated": str(pd.Timestamp.utcnow())
    }
    with open(ACTIVE_GRID_PATH, "w") as f:
        json.dump(inactive_payload, f, indent=2)


# -----------------------------
# Decision history updater
# -----------------------------
def attach_lifecycle_to_decision_history(candidate, output):
    if not Path(DECISION_HISTORY_PATH).exists():
        print(f"Decision history not found, skipping lifecycle attach: {DECISION_HISTORY_PATH}")
        return

    df = pd.read_csv(DECISION_HISTORY_PATH)

    if df.empty:
        print("Decision history is empty, skipping lifecycle attach.")
        return

    if "timestamp" not in df.columns and "date" in df.columns:
        df = df.rename(columns={"date": "timestamp"})

    if "timestamp" not in df.columns:
        print("Decision history missing timestamp column, skipping lifecycle attach.")
        return

    # Ensure lifecycle columns exist
    for col in ["lifecycle_mode", "lifecycle_action", "lifecycle_reason"]:
        if col not in df.columns:
            df[col] = None

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    candidate_ts = pd.to_datetime(candidate["timestamp"], utc=True)

    matches = df.index[df["timestamp"] == candidate_ts].tolist()

    if not matches:
        print(f"No matching decision_history row found for timestamp {candidate_ts}.")
        return

    idx = matches[-1]

    df.at[idx, "lifecycle_mode"] = output["mode"]
    df.at[idx, "lifecycle_action"] = output["lifecycle_action"]
    df.at[idx, "lifecycle_reason"] = output["reason"]

    df.to_csv(DECISION_HISTORY_PATH, index=False)
    print(f"Attached lifecycle fields to decision_history row: {candidate_ts}")


# -----------------------------
# Initiation logic
# -----------------------------
def determine_initiation_action(candidate, price):
    score = float(candidate["candidate_score"])
    regime = candidate["operational_regime"]
    lower = float(candidate["grid_lower"])
    upper = float(candidate["grid_upper"])
    center = float(candidate["center_price"])

    # Candidate should at least still make geometric sense
    if not (lower < upper):
        return "BLOCK", "invalid_candidate_geometry"

    # Candidate too weak
    if score < 7:
        return "WAIT", "candidate_score_below_threshold"

    # Avoid initiating if already clearly outside proposed structure
    if price < lower * 0.995 or price > upper * 1.005:
        return "WAIT", "price_already_far_from_candidate_structure"

    # Distance from center (avoid stretched entries)
    distance_from_center = abs(price - center) / center

    # Best-case regime
    if regime == "RANGE_GOOD" and score >= 8 and distance_from_center <= 0.01:
        return "INITIATE", "strong_range_candidate"

    # Acceptable but more cautious regime
    if regime == "RANGE_TREND_UP" and score >= 9 and distance_from_center <= 0.008:
        return "INITIATE", "high_quality_range_trend_up_candidate"

    # Everything else waits for now
    return "WAIT", "context_not_strong_enough_to_initiate"


# -----------------------------
# Maintenance logic
# -----------------------------
def determine_maintenance_action(active_grid, candidate, price):
    lower = float(active_grid["grid_lower"])
    upper = float(active_grid["grid_upper"])
    center = float(active_grid["center_price"])
    active_score = float(active_grid["candidate_score"])
    active_regime = active_grid["operational_regime"]

    candidate_score = float(candidate["candidate_score"])
    candidate_regime = candidate["operational_regime"]

    # Distance from active grid center
    distance_from_center = abs(price - center) / center

    # -----------------------------
    # Case 1: price inside active grid
    # -----------------------------
    if lower <= price <= upper:
        if price > center:
            position = (price - center) / (upper - center) if (upper - center) != 0 else 0
        else:
            position = (center - price) / (center - lower) if (center - lower) != 0 else 0

        # Well inside grid: default sticky HOLD
        if position <= 0.60:
            # Very conservative override:
            # only replace if regime changed AND candidate is materially better
            if candidate_regime != active_regime and candidate_score >= active_score + 1.0:
                return "REPLACE", "inside_grid_but_regime_changed_and_candidate_materially_better"

            return "HOLD", "price_well_inside_active_grid"

        # Near edge, but not necessarily broken
        if position > 0.85:
            # If still strong context, recenter instead of churn
            if active_regime == "RANGE_GOOD" and active_score >= 7.5:
                return "RECENTER", "active_grid_near_edge_but_still_viable"

            # If latest candidate is materially better, replace
            if candidate_score >= active_score + 1.0:
                return "REPLACE", "new_candidate_materially_better_than_active_grid"

            return "HOLD", "inside_grid_but_no_strong_case_to_change"

        # Mid-zone inside grid
        return "HOLD", "price_inside_active_grid"

    # -----------------------------
    # Case 2: price outside active grid
    # -----------------------------
    escape_distance = abs(price - center) / center

    # Mild escape, still salvageable
    if escape_distance <= 0.015 and active_regime == "RANGE_GOOD":
        return "RECENTER", "mild_escape_from_active_grid_but_context_still_good"

    # Moderate escape → consider replacement
    if escape_distance <= 0.025:
        if candidate_score >= active_score:
            return "REPLACE", "moderate_escape_and_new_candidate_viable"
        return "RECENTER", "moderate_escape_but_no_better_candidate"

    # Strong escape → likely broken, but allow replacement if a clearly better candidate exists
    if escape_distance > 0.025:
        if candidate_score >= active_score + 1.0:
            return "REPLACE", "strong_escape_but_new_candidate_materially_better"
        return "EXIT", "strong_breakout_from_active_grid"

    return "HOLD", "default_fallback"


# -----------------------------
# Active grid builders
# -----------------------------
def build_active_grid(candidate, price_timestamp):
    return {
        "grid_id": f"xrp_{pd.Timestamp.utcnow().strftime('%Y%m%d_%H%M%S')}",
        "active": True,
        "status": "ACTIVE",
        "initiated_at": str(pd.Timestamp.utcnow()),
        "last_reviewed_at": str(pd.Timestamp.utcnow()),
        "price_timestamp": price_timestamp,
        "grid_lower": float(candidate["grid_lower"]),
        "grid_upper": float(candidate["grid_upper"]),
        "center_price": float(candidate["center_price"]),
        "candidate_score": float(candidate["candidate_score"]),
        "operational_regime": candidate["operational_regime"],
        "last_action": "INITIATE",
        "last_action_reason": "new_active_grid_created",
        "bars_since_initiation": 0,
        "bars_outside_grid": 0,
        "bars_near_edge": 0
    }


def update_active_grid_after_action(active_grid, candidate, action, reason, price_timestamp):
    now = str(pd.Timestamp.utcnow())

    if action == "HOLD":
        active_grid["last_reviewed_at"] = now
        active_grid["price_timestamp"] = price_timestamp
        active_grid["last_action"] = "HOLD"
        active_grid["last_action_reason"] = reason
        return active_grid

    if action in ["RECENTER", "REPLACE"]:
        active_grid["last_reviewed_at"] = now
        active_grid["price_timestamp"] = price_timestamp
        active_grid["grid_lower"] = float(candidate["grid_lower"])
        active_grid["grid_upper"] = float(candidate["grid_upper"])
        active_grid["center_price"] = float(candidate["center_price"])
        active_grid["candidate_score"] = float(candidate["candidate_score"])
        active_grid["operational_regime"] = candidate["operational_regime"]
        active_grid["last_action"] = action
        active_grid["last_action_reason"] = reason
        return active_grid

    if action == "EXIT":
        return {
            "active": False,
            "status": "EXITED",
            "exited_at": now,
            "last_action": "EXIT",
            "last_action_reason": reason
        }

    return active_grid


# -----------------------------
# Main
# -----------------------------
def main():
    candidate = load_latest_decision()
    price, price_timestamp = load_latest_price()
    active_grid = load_active_grid()

    if active_grid is None:
        action, reason = determine_initiation_action(candidate, price)

        if action == "INITIATE":
            new_active_grid = build_active_grid(candidate, price_timestamp)
            save_active_grid(new_active_grid)
        else:
            clear_active_grid()

        output = {
            "timestamp": str(pd.Timestamp.utcnow()),
            "mode": "INITIATION",
            "price": price,
            "lifecycle_action": action,
            "reason": reason,
            "candidate_grid_lower": float(candidate["grid_lower"]),
            "candidate_grid_upper": float(candidate["grid_upper"]),
            "candidate_center_price": float(candidate["center_price"]),
            "candidate_score": float(candidate["candidate_score"]),
            "candidate_regime": candidate["operational_regime"],
            "active_grid_exists": False
        }

    else:
        action, reason = determine_maintenance_action(active_grid, candidate, price)
        updated_active_grid = update_active_grid_after_action(active_grid, candidate, action, reason, price_timestamp)

        if updated_active_grid.get("active", False):
            save_active_grid(updated_active_grid)
        else:
            clear_active_grid()

        output = {
            "timestamp": str(pd.Timestamp.utcnow()),
            "mode": "MAINTENANCE",
            "price": price,
            "lifecycle_action": action,
            "reason": reason,
            "active_grid_id": active_grid.get("grid_id"),
            "active_grid_lower": float(active_grid["grid_lower"]),
            "active_grid_upper": float(active_grid["grid_upper"]),
            "active_center_price": float(active_grid["center_price"]),
            "active_candidate_score": float(active_grid["candidate_score"]),
            "active_regime": active_grid["operational_regime"],
            "latest_candidate_score": float(candidate["candidate_score"]),
            "latest_candidate_regime": candidate["operational_regime"],
            "active_grid_exists": True
        }

    # Save JSON
    with open(OUTPUT_JSON, "w") as f:
        json.dump(output, f, indent=2)

    # Save CSV
    pd.DataFrame([output]).to_csv(OUTPUT_CSV, index=False)

    # Attach lifecycle decision to the matching decision_history row
    attach_lifecycle_to_decision_history(candidate, output)

    print(f"Lifecycle decision → {output['mode']} | {output['lifecycle_action']} ({output['reason']})")


if __name__ == "__main__":
    main()

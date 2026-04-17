import pandas as pd
import json
import os
import sys
from pathlib import Path

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.control.control_layer import apply as control_apply
from src.control.economic_guard import check_economic_override, load_paper_summary

# -----------------------------
# Paths
# -----------------------------
DECISION_PATH = "outputs/latest_decision.csv"
DECISION_HISTORY_PATH = "outputs/decision_history.csv"
PRICE_PATH = "data/raw/xrp_full_hourly_clean.csv"

ACTIVE_GRID_PATH = "outputs/active_grid.json"

OUTPUT_JSON = "outputs/lifecycle_decision.json"
OUTPUT_CSV = "outputs/lifecycle_decision.csv"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ADAPTIVE_PARAMS_PATH = os.path.join(BASE_DIR, "outputs", "adaptive_params.json")


# -----------------------------
# Adaptive params
# -----------------------------
def load_adaptive_threshold():
    try:
        with open(ADAPTIVE_PARAMS_PATH, "r") as f:
            params = json.load(f)
            return float(params.get("initiation_score_threshold", 12.0))
    except Exception:
        return 12.0


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
    now = str(pd.Timestamp.utcnow())
    inactive_payload = {
        "active": False,
        "status": "INACTIVE",
        "last_updated": now,
        "last_exit_ts": now,   # read by control_layer for reentry cooldown
    }
    with open(ACTIVE_GRID_PATH, "w") as f:
        json.dump(inactive_payload, f, indent=2)


# -----------------------------
# Initiation logic (unchanged)
# -----------------------------
def determine_initiation_action(candidate, price):
    score = float(candidate["candidate_score"])
    regime = candidate["operational_regime"]
    lower = float(candidate["grid_lower"])
    upper = float(candidate["grid_upper"])
    center = float(candidate["center_price"])

    threshold = load_adaptive_threshold()

    if not (lower < upper):
        return "BLOCK", "invalid_candidate_geometry"

    if score < threshold:
        return "WAIT", f"candidate_score_below_threshold_{threshold}"

    if price < lower * 0.995 or price > upper * 1.005:
        return "WAIT", "price_already_far_from_candidate_structure"

    distance_from_center = abs(price - center) / center

    if regime == "RANGE_GOOD" and score >= 8 and distance_from_center <= 0.01:
        return "INITIATE", "strong_range_candidate"

    if regime == "RANGE_TREND_UP" and score >= 9 and distance_from_center <= 0.008:
        return "INITIATE", "high_quality_range_trend_up_candidate"

    return "WAIT", "context_not_strong_enough_to_initiate"


# -----------------------------
# IMPROVED maintenance logic
# -----------------------------
def determine_maintenance_action(active_grid, candidate, price):
    lower = float(active_grid["grid_lower"])
    upper = float(active_grid["grid_upper"])
    center = float(active_grid["center_price"])

    active_score = float(active_grid["candidate_score"])
    candidate_score = float(candidate["candidate_score"])

    # Normalize position within grid (0 = bottom, 1 = top)
    range_pos = (price - lower) / (upper - lower)

    # -----------------------------
    # Inside grid behavior
    # -----------------------------
    if lower <= price <= upper:

        # Centered → HOLD
        if 0.30 <= range_pos <= 0.70:
            return "HOLD", "well_centered"

        # Drifting but not extreme → HOLD
        if 0.20 <= range_pos < 0.30 or 0.70 < range_pos <= 0.80:
            return "HOLD", "mild_drift"

        # Near edge → RECENTER
        if 0.10 <= range_pos < 0.20 or 0.80 < range_pos <= 0.90:
            return "RECENTER", "approaching_edge"

        # Extreme edge → consider replace
        if range_pos < 0.10 or range_pos > 0.90:
            if candidate_score >= active_score:
                return "REPLACE", "edge_new_candidate"
            return "RECENTER", "edge_recenter"

        return "HOLD", "inside_grid"

    # -----------------------------
    # Outside grid behavior
    # -----------------------------
    escape_distance = abs(price - center) / center

    if escape_distance <= 0.015:
        return "RECENTER", "mild_escape"

    if escape_distance <= 0.025:
        if candidate_score >= active_score:
            return "REPLACE", "moderate_escape_new_candidate"
        return "RECENTER", "moderate_escape"

    if candidate_score >= active_score + 1.0:
        return "REPLACE", "strong_escape_better_candidate"

    return "EXIT", "strong_breakout"


# -----------------------------
# Write lifecycle fields back to decision_history.csv
# -----------------------------
def _stamp_lifecycle_to_history(output: dict):
    """
    Update the most-recent row in decision_history.csv with the lifecycle
    fields computed this cycle.  Called after lifecycle output is finalised
    so evaluate.py can read them directly from decision_history.csv.
    """
    if not Path(DECISION_HISTORY_PATH).exists():
        return

    history = pd.read_csv(DECISION_HISTORY_PATH)
    if history.empty:
        return

    # Target: the row with the latest timestamp (the one run.py just appended).
    latest_idx = history.index[-1]

    history.loc[latest_idx, "lifecycle_action"] = output.get("lifecycle_action")
    history.loc[latest_idx, "lifecycle_mode"]   = output.get("mode")
    history.loc[latest_idx, "lifecycle_reason"] = output.get("reason")

    history.to_csv(DECISION_HISTORY_PATH, index=False)

    # Verify the stamp landed — fail loudly rather than silently producing NULLs.
    verify = pd.read_csv(DECISION_HISTORY_PATH)
    if pd.isna(verify.iloc[latest_idx]["lifecycle_action"]):
        raise RuntimeError(
            f"lifecycle stamp failed: row {latest_idx} still has NULL lifecycle_action"
        )


# -----------------------------
# MAIN
# -----------------------------
def main():
    candidate = load_latest_decision()
    price, ts = load_latest_price()

    active_grid   = load_active_grid()
    paper_summary = load_paper_summary() if active_grid else None

    if active_grid is None:
        proposed_action, proposed_reason = determine_initiation_action(candidate, price)
        action, reason, _ = control_apply(proposed_action, proposed_reason, None, price)

        if action == "INITIATE":
            from src.control.control_layer import REPLACE_COOLDOWN_BARS, RECENTER_COOLDOWN_BARS
            new_grid = {
                "grid_id": f"xrp_{pd.Timestamp.utcnow().strftime('%Y%m%d_%H%M%S')}",
                "active": True,
                "status": "ACTIVE",
                "initiated_at": str(pd.Timestamp.utcnow()),
                "last_reviewed_at": str(pd.Timestamp.utcnow()),
                "price_timestamp": ts,
                "grid_lower": float(candidate["grid_lower"]),
                "grid_upper": float(candidate["grid_upper"]),
                "center_price": float(candidate["center_price"]),
                "candidate_score": float(candidate["candidate_score"]),
                "operational_regime": candidate["operational_regime"],
                "last_action": action,
                "last_action_reason": reason,
                "bars_since_initiation": 0,
                "bars_outside_grid": 0,
                "bars_near_edge": 0,
                # Cooldowns start as already elapsed on a fresh grid after EXIT.
                "bars_since_last_replace": REPLACE_COOLDOWN_BARS,
                "bars_since_last_recenter": RECENTER_COOLDOWN_BARS,
            }
            save_active_grid(new_grid)

        output = {
            "mode": "INIT",
            "lifecycle_action": action,
            "reason": reason,
            "tradable": bool(candidate.get("tradable", True)),
        }

    else:
        proposed_action, proposed_reason = determine_maintenance_action(active_grid, candidate, price)
        action, reason, updated_grid = control_apply(proposed_action, proposed_reason, active_grid, price)

        eco_override = check_economic_override(action, active_grid, paper_summary, price)
        eco_applied  = eco_override is not None
        if eco_applied:
            action, reason = eco_override

        updated_grid["last_reviewed_at"] = str(pd.Timestamp.utcnow())
        updated_grid["price_timestamp"] = ts
        updated_grid["last_action"] = action
        updated_grid["last_action_reason"] = reason

        if action == "REPLACE":
            from src.control.control_layer import RECENTER_COOLDOWN_BARS
            new_grid = {
                "grid_id": f"xrp_{pd.Timestamp.utcnow().strftime('%Y%m%d_%H%M%S')}",
                "active": True,
                "status": "ACTIVE",
                "initiated_at": str(pd.Timestamp.utcnow()),
                "last_reviewed_at": str(pd.Timestamp.utcnow()),
                "price_timestamp": ts,
                "grid_lower": float(candidate["grid_lower"]),
                "grid_upper": float(candidate["grid_upper"]),
                "center_price": float(candidate["center_price"]),
                "candidate_score": float(candidate["candidate_score"]),
                "operational_regime": candidate["operational_regime"],
                "last_action": action,
                "last_action_reason": reason,
                "bars_since_initiation": 0,
                "bars_outside_grid": 0,
                "bars_near_edge": 0,
                # Replace cooldown starts fresh — this is the reset point.
                # Recenter cooldown starts as elapsed so the new grid can RECENTER immediately.
                "bars_since_last_replace": 0,
                "bars_since_last_recenter": RECENTER_COOLDOWN_BARS,
            }
            save_active_grid(new_grid)

        elif action == "EXIT":
            clear_active_grid()

        else:
            # HOLD and RECENTER: persist the counter-updated grid state.
            save_active_grid(updated_grid)

        output = {
            "mode": "MAINTAIN",
            "lifecycle_action": action,
            "reason": reason,
            "tradable": bool(candidate.get("tradable", True)),
            "economic_override": reason if eco_applied else None,
            "economic_override_applied": eco_applied,
        }

    with open(OUTPUT_JSON, "w") as f:
        json.dump(output, f, indent=2)

    pd.DataFrame([output]).to_csv(OUTPUT_CSV, index=False)

    _stamp_lifecycle_to_history(output)

    print(f"LIFECYCLE → {output['lifecycle_action']} ({output['reason']})")


if __name__ == "__main__":
    main()

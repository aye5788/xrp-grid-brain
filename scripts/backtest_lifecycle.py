import pandas as pd
from pathlib import Path

# -----------------------------------
# Paths (SEPARATE from live outputs)
# -----------------------------------
PRICE_PATH = "data/raw/xrp_full_hourly_clean.csv"
DECISION_HISTORY_PATH = "outputs/decision_history.csv"

BACKTEST_OUTPUT_PATH = "outputs/backtests/lifecycle_backtest.csv"
SUMMARY_OUTPUT_PATH = "outputs/backtests/lifecycle_summary.csv"


# -----------------------------------
# Helpers
# -----------------------------------
def load_data():
    if not Path(PRICE_PATH).exists():
        raise FileNotFoundError(f"Missing price file: {PRICE_PATH}")

    if not Path(DECISION_HISTORY_PATH).exists():
        raise FileNotFoundError(f"Missing decision history file: {DECISION_HISTORY_PATH}")

    price = pd.read_csv(PRICE_PATH)
    decisions = pd.read_csv(DECISION_HISTORY_PATH)

    # Handle timestamp schema issue
    if "timestamp" not in price.columns and "date" in price.columns:
        price = price.rename(columns={"date": "timestamp"})

    if "timestamp" in price.columns:
        price["timestamp"] = pd.to_datetime(price["timestamp"], utc=True)

    if "timestamp" in decisions.columns:
        decisions["timestamp"] = pd.to_datetime(decisions["timestamp"], utc=True)

    merged = pd.merge(
        decisions,
        price[["timestamp", "close"]],
        on="timestamp",
        how="inner"
    )

    if merged.empty:
        raise ValueError("Merged backtest dataset is empty. Check timestamp alignment.")

    return merged.sort_values("timestamp").reset_index(drop=True)


# -----------------------------------
# Lifecycle logic (SIMULATION ONLY)
# -----------------------------------
def determine_initiation_action(candidate, price):
    score = float(candidate["candidate_score"])
    regime = candidate["operational_regime"]
    lower = float(candidate["grid_lower"])
    upper = float(candidate["grid_upper"])
    center = float(candidate["center_price"])

    if not (lower < upper):
        return "BLOCK", "invalid_candidate_geometry"

    if score < 6.5:
        return "WAIT", "candidate_score_below_threshold"

    # Don't initiate if price is WAY outside candidate
    if price < lower * 0.985 or price > upper * 1.015:
        return "WAIT", "price_far_outside_candidate_structure"

    distance_from_center = abs(price - center) / center

    # Best context
    if regime == "RANGE_GOOD" and score >= 7:
        return "INITIATE", "range_good_candidate"

    # Still very acceptable for XRP-style behavior
    if regime == "RANGE_TREND_UP" and score >= 7.5 and distance_from_center <= 0.025:
        return "INITIATE", "range_trend_up_candidate"

    # Controlled allowance for trend setups if structure still decent
    if regime == "TREND" and score >= 8.5 and distance_from_center <= 0.015:
        return "INITIATE", "high_quality_trend_candidate"

    return "WAIT", "context_not_strong_enough_to_initiate"


def determine_maintenance_action(active_grid, candidate, price):
    lower = float(active_grid["grid_lower"])
    upper = float(active_grid["grid_upper"])
    center = float(active_grid["center_price"])
    active_score = float(active_grid["candidate_score"])
    active_regime = active_grid["operational_regime"]

    candidate_score = float(candidate["candidate_score"])

    # -----------------------------------
    # Case 1: price inside active grid
    # -----------------------------------
    if lower <= price <= upper:
        if price > center:
            position = (price - center) / (upper - center) if (upper - center) != 0 else 0
        else:
            position = (center - price) / (center - lower) if (center - lower) != 0 else 0

        # Near edge
        if position > 0.85:
            if active_grid["bars_near_edge"] >= 2:
                if active_regime == "RANGE_GOOD" and active_score >= 7.0:
                    return "RECENTER", "persistent_edge_pressure_but_grid_still_viable"

                if candidate_score >= active_score + 1.0:
                    return "REPLACE", "persistent_edge_pressure_and_new_candidate_better"

            return "HOLD", "near_edge_but_not_persistent_enough"

        return "HOLD", "price_inside_active_grid"

    # -----------------------------------
    # Case 2: price outside active grid
    # -----------------------------------
    escape_distance = abs(price - center) / center

    if active_grid["bars_outside_grid"] >= 2:
        if escape_distance <= 0.02 and candidate_score >= active_score:
            return "REPLACE", "persistent_outside_grid_and_new_candidate_viable"

        if escape_distance <= 0.015 and active_regime in ["RANGE_GOOD", "RANGE_TREND_UP"]:
            return "RECENTER", "persistent_mild_escape_but_grid_still_adaptable"

        return "EXIT", "persistent_breakout_from_active_grid"

    return "HOLD", "outside_grid_but_not_persistent_enough"


def build_active_grid(candidate, timestamp):
    return {
        "grid_id": f"xrp_{pd.Timestamp(timestamp).strftime('%Y%m%d_%H%M%S')}",
        "active": True,
        "status": "ACTIVE",
        "initiated_at": str(timestamp),
        "last_reviewed_at": str(timestamp),
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


def update_active_grid_after_action(active_grid, candidate, action, reason, timestamp, price):
    active_grid["last_reviewed_at"] = str(timestamp)
    active_grid["bars_since_initiation"] += 1

    lower = float(active_grid["grid_lower"])
    upper = float(active_grid["grid_upper"])
    center = float(active_grid["center_price"])

    # Track outside bars
    if price < lower or price > upper:
        active_grid["bars_outside_grid"] += 1
    else:
        active_grid["bars_outside_grid"] = 0

    # Track near-edge bars
    if lower <= price <= upper:
        if price > center:
            position = (price - center) / (upper - center) if (upper - center) != 0 else 0
        else:
            position = (center - price) / (center - lower) if (center - lower) != 0 else 0

        if position > 0.85:
            active_grid["bars_near_edge"] += 1
        else:
            active_grid["bars_near_edge"] = 0
    else:
        active_grid["bars_near_edge"] = 0

    if action == "HOLD":
        active_grid["last_action"] = "HOLD"
        active_grid["last_action_reason"] = reason
        return active_grid

    if action in ["RECENTER", "REPLACE"]:
        active_grid["grid_lower"] = float(candidate["grid_lower"])
        active_grid["grid_upper"] = float(candidate["grid_upper"])
        active_grid["center_price"] = float(candidate["center_price"])
        active_grid["candidate_score"] = float(candidate["candidate_score"])
        active_grid["operational_regime"] = candidate["operational_regime"]
        active_grid["last_action"] = action
        active_grid["last_action_reason"] = reason
        active_grid["bars_outside_grid"] = 0
        active_grid["bars_near_edge"] = 0
        return active_grid

    if action == "EXIT":
        return None

    return active_grid


# -----------------------------------
# Backtest engine
# -----------------------------------
def run_backtest(df):
    active_grid = None
    results = []

    for _, row in df.iterrows():
        timestamp = row["timestamp"]

        # After merge, raw market price lives here
        if "close_y" in row.index:
            price = float(row["close_y"])
        elif "close" in row.index:
            price = float(row["close"])
        else:
            raise ValueError(f"Could not find price column in row. Available columns: {list(row.index)}")

        candidate = row

        if active_grid is None:
            action, reason = determine_initiation_action(candidate, price)

            if action == "INITIATE":
                active_grid = build_active_grid(candidate, timestamp)

            results.append({
                "timestamp": timestamp,
                "mode": "INITIATION",
                "price": price,
                "action": action,
                "reason": reason,
                "active_grid": active_grid is not None,
                "candidate_score": float(candidate["candidate_score"]),
                "regime": candidate["operational_regime"]
            })

        else:
            action, reason = determine_maintenance_action(active_grid, candidate, price)
            active_grid = update_active_grid_after_action(active_grid, candidate, action, reason, timestamp, price)

            results.append({
                "timestamp": timestamp,
                "mode": "MAINTENANCE",
                "price": price,
                "action": action,
                "reason": reason,
                "active_grid": active_grid is not None,
                "candidate_score": float(candidate["candidate_score"]),
                "regime": candidate["operational_regime"]
            })

    return pd.DataFrame(results)


# -----------------------------------
# Summary
# -----------------------------------
def summarize_results(bt):
    summary = {
        "total_rows": len(bt),
        "initiate_count": int((bt["action"] == "INITIATE").sum()),
        "wait_count": int((bt["action"] == "WAIT").sum()),
        "hold_count": int((bt["action"] == "HOLD").sum()),
        "recenter_count": int((bt["action"] == "RECENTER").sum()),
        "replace_count": int((bt["action"] == "REPLACE").sum()),
        "exit_count": int((bt["action"] == "EXIT").sum()),
        "active_grid_rows": int(bt["active_grid"].sum()),
        "maintenance_rows": int((bt["mode"] == "MAINTENANCE").sum()),
    }

    return pd.DataFrame([summary])


# -----------------------------------
# Main
# -----------------------------------
def main():
    df = load_data()
    bt = run_backtest(df)
    summary = summarize_results(bt)

    bt.to_csv(BACKTEST_OUTPUT_PATH, index=False)
    summary.to_csv(SUMMARY_OUTPUT_PATH, index=False)

    print("Lifecycle backtest complete.")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()

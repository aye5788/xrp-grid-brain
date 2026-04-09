import pandas as pd
from pathlib import Path

DATA_PATH = "data/raw/xrp_full_hourly_clean.csv"
DECISION_PATH = "outputs/decision_history.csv"
EVAL_PATH = "outputs/evaluation_history.csv"

EVAL_WINDOW_HOURS = 24


def load_data():
    price = pd.read_csv(DATA_PATH)
    decisions = pd.read_csv(DECISION_PATH)

    # Handle historical schema issue: date vs timestamp
    if "timestamp" not in price.columns and "date" in price.columns:
        price = price.rename(columns={"date": "timestamp"})

    if "timestamp" not in decisions.columns and "date" in decisions.columns:
        decisions = decisions.rename(columns={"date": "timestamp"})

    if "timestamp" not in price.columns:
        raise ValueError(f"Price data missing timestamp column. Found: {list(price.columns)}")

    if "timestamp" not in decisions.columns:
        raise ValueError(f"Decision data missing timestamp column. Found: {list(decisions.columns)}")

    price["timestamp"] = pd.to_datetime(price["timestamp"], utc=True)
    decisions["timestamp"] = pd.to_datetime(decisions["timestamp"], utc=True)

    price = price.sort_values("timestamp").reset_index(drop=True)
    decisions = decisions.sort_values("timestamp").reset_index(drop=True)

    return price, decisions


def get_existing_eval():
    if Path(EVAL_PATH).exists():
        df = pd.read_csv(EVAL_PATH)

        if "timestamp" not in df.columns and "date" in df.columns:
            df = df.rename(columns={"date": "timestamp"})

        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

        return df

    return pd.DataFrame()


def evaluate_row(row, price_df):
    t0 = row["timestamp"]

    future = price_df[
        (price_df["timestamp"] > t0) &
        (price_df["timestamp"] <= t0 + pd.Timedelta(hours=EVAL_WINDOW_HOURS))
    ].copy()

    if len(future) < EVAL_WINDOW_HOURS:
        return None

    upper = row["grid_upper"]
    lower = row["grid_lower"]
    center = row["center_price"]
    spacing = row["spacing"]

    # --- Basic checks ---
    stayed_in_range = ((future["high"] <= upper) & (future["low"] >= lower)).all()
    escaped_up = (future["high"] > upper).any()
    escaped_down = (future["low"] < lower).any()

    # --- Escape severity ---
    max_high = future["high"].max()
    min_low = future["low"].min()

    max_pct_above_upper = max(0, (max_high - upper) / center)
    max_pct_below_lower = max(0, (lower - min_low) / center)

    final_close = future.iloc[-1]["close"]
    close_vs_center = (final_close - center) / center

    # --- Inside grid behavior ---
    inside_mask = (future["high"] <= upper) & (future["low"] >= lower)
    inside_hours = int(inside_mask.sum())

    inside_range = future[inside_mask]
    if len(inside_range) > 0:
        inside_move = (inside_range["high"].max() - inside_range["low"].min()) / center
    else:
        inside_move = 0

    # --- Movement + chop vs trend ---
    total_move = (max_high - min_low) / center
    directional_move = abs(final_close - future.iloc[0]["open"]) / center
    chop_ratio = total_move / directional_move if directional_move > 0 else 0

    # --- Approx level activity ---
    total_range = max_high - min_low
    approx_crosses = total_range / spacing if spacing > 0 else 0

    # --- First escape timing ---
    first_escape = None
    for _, r in future.iterrows():
        if r["high"] > upper or r["low"] < lower:
            first_escape = (r["timestamp"] - t0).total_seconds() / 3600
            break

    # --- Classification flags ---
    productive = (approx_crosses >= 4) and (inside_hours >= 12)

    stranded = (
        (not stayed_in_range)
        and (
            inside_hours < (EVAL_WINDOW_HOURS * 0.40)
            or abs(close_vs_center) > 0.02
        )
    )

    # --- Evaluation scoring (0–100) ---
    score = 0

    if stayed_in_range:
        score += 40
    else:
        escape_penalty_score = max(0, 18 - (max_pct_above_upper + max_pct_below_lower) * 120)
        score += escape_penalty_score

    score += min(18, inside_move * 180)
    score += min(12, approx_crosses * 2.5)

    if chop_ratio > 2 and inside_hours >= 10:
        score += 8

    score += min(12, inside_hours * 0.5)

    if abs(close_vs_center) > 0.015:
        score -= min(10, abs(close_vs_center) * 250)

    if stranded:
        score -= 28

    score = max(0, min(100, score))

    # -----------------------------
    # Lifecycle / context metrics
    # -----------------------------
    lifecycle_mode = row.get("lifecycle_mode", "UNKNOWN")
    lifecycle_action = row.get("lifecycle_action", "UNKNOWN")
    lifecycle_reason = row.get("lifecycle_reason", "UNKNOWN")

    active_grid_age_hours = EVAL_WINDOW_HOURS

    price_at_decision = row["close"] if "close" in row else center
    price_vs_center_pct = (price_at_decision - center) / center if center != 0 else 0

    dist_upper = abs(upper - price_at_decision) / center if center != 0 else 0
    dist_lower = abs(price_at_decision - lower) / center if center != 0 else 0
    distance_to_boundary_pct = min(dist_upper, dist_lower)

    score_delta = 0.0

    # -----------------------------
    # ORIGINAL DECISION METADATA
    # -----------------------------
    operational_regime = row.get("operational_regime", "UNKNOWN")
    candidate_score = row.get("candidate_score", None)
    variant_label = row.get("variant_label", "UNKNOWN")
    selection_reason = row.get("selection_reason", "UNKNOWN")
    tradable = row.get("tradable", None)

    return {
        "timestamp": t0,

        # Original decision metadata
        "operational_regime": operational_regime,
        "candidate_score": candidate_score,
        "variant_label": variant_label,
        "selection_reason": selection_reason,
        "tradable": tradable,

        # Existing evaluation metrics
        "stayed_in_range_24h": stayed_in_range,
        "escaped_up_24h": escaped_up,
        "escaped_down_24h": escaped_down,
        "first_escape_hours": first_escape,
        "inside_range_hours_24h": inside_hours,
        "realized_move_pct_24h": total_move,
        "approx_level_crosses_24h": approx_crosses,
        "productive_grid_24h": productive,
        "likely_stranded_24h": stranded,

        # Detailed diagnostics
        "max_pct_above_upper_24h": max_pct_above_upper,
        "max_pct_below_lower_24h": max_pct_below_lower,
        "close_vs_center_24h": close_vs_center,
        "inside_move_pct_24h": inside_move,
        "chop_ratio_24h": chop_ratio,

        # Lifecycle metadata
        "lifecycle_mode": lifecycle_mode,
        "lifecycle_action": lifecycle_action,
        "lifecycle_reason": lifecycle_reason,

        # Lifecycle / supervisor context
        "active_grid_age_hours": active_grid_age_hours,
        "price_vs_center_pct": price_vs_center_pct,
        "distance_to_boundary_pct": distance_to_boundary_pct,
        "score_delta": score_delta,

        # Final score
        "evaluation_score_24h": score,
    }


def main():
    price, decisions = load_data()
    existing_eval = get_existing_eval()

    evaluated_timestamps = (
        set(existing_eval["timestamp"])
        if not existing_eval.empty and "timestamp" in existing_eval.columns
        else set()
    )

    results = []

    for _, row in decisions.iterrows():
        if row["timestamp"] in evaluated_timestamps:
            continue

        res = evaluate_row(row, price)
        if res:
            results.append(res)

    if not results:
        print("No new rows eligible for evaluation.")
        return

    new_df = pd.DataFrame(results)

    if Path(EVAL_PATH).exists():
        new_df.to_csv(EVAL_PATH, mode="a", header=False, index=False)
    else:
        new_df.to_csv(EVAL_PATH, index=False)

    print(f"Added {len(new_df)} new evaluation rows.")


if __name__ == "__main__":
    main()

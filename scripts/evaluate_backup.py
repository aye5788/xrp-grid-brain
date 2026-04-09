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

    # 1) Range containment matters a lot
    if stayed_in_range:
        score += 40
    else:
        escape_penalty_score = max(0, 18 - (max_pct_above_upper + max_pct_below_lower) * 120)
        score += escape_penalty_score

    # 2) Internal usable movement matters
    score += min(18, inside_move * 180)

    # 3) Crosses matter, but cap them
    score += min(12, approx_crosses * 2.5)

    # 4) Chop is good only if it stayed usable long enough
    if chop_ratio > 2 and inside_hours >= 10:
        score += 8

    # 5) Reward surviving inside for meaningful time
    score += min(12, inside_hours * 0.5)

    # 6) Penalize ugly final displacement
    if abs(close_vs_center) > 0.015:
        score -= min(10, abs(close_vs_center) * 250)

    # 7) Penalize stranding heavily
    if stranded:
        score -= 28

    score = max(0, min(100, score))

    return {
        "timestamp": t0,

        # Existing
        "stayed_in_range_24h": stayed_in_range,
        "escaped_up_24h": escaped_up,
        "escaped_down_24h": escaped_down,
        "first_escape_hours": first_escape,
        "inside_range_hours_24h": inside_hours,
        "realized_move_pct_24h": total_move,
        "approx_level_crosses_24h": approx_crosses,
        "productive_grid_24h": productive,
        "likely_stranded_24h": stranded,

        # NEW (critical)
        "max_pct_above_upper_24h": max_pct_above_upper,
        "max_pct_below_lower_24h": max_pct_below_lower,
        "close_vs_center_24h": close_vs_center,
        "inside_move_pct_24h": inside_move,
        "chop_ratio_24h": chop_ratio,

        # FINAL OUTPUT
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

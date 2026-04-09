import sys
from pathlib import Path

import pandas as pd

# Ensure repo root is on Python path
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT))

from src.io.loaders import load_ohlcv_csv
from src.features.regime_features import compute_features
from src.models.gmm_regime import fit_gmm, assign_clusters
from src.policy.regime_policy import apply_policy
from src.grid.candidate_builder import build_grid

# -----------------------------
# CONFIG
# -----------------------------
DATA_PATH = "data/raw/xrp_full_hourly_clean.csv"
OUTPUT_DIR = Path("outputs/replay")

DECISION_PATH = OUTPUT_DIR / "decision_history_replay.csv"
EVAL_PATH = OUTPUT_DIR / "evaluation_history_replay.csv"
SUMMARY_PATH = OUTPUT_DIR / "eval_summary_replay.csv"
LATEST_PATH = OUTPUT_DIR / "latest_replay_decision.csv"

WARMUP_BARS = 200
EVAL_WINDOW_HOURS = 24

# -----------------------------
# SPEED CONTROLS
# -----------------------------
MODE = "quick"   # "quick" or "full"

if MODE == "quick":
    STEP_HOURS = 3
    GMM_REFIT_EVERY = 24
    LOOKBACK_ROWS = 24 * 365   # last ~12 months
else:
    STEP_HOURS = 1
    GMM_REFIT_EVERY = 24
    LOOKBACK_ROWS = None       # full history


# -----------------------------
# HELPERS
# -----------------------------
def ensure_output_dir():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


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

    if pd.isna(upper) or pd.isna(lower) or pd.isna(center) or pd.isna(spacing):
        return None

    stayed_in_range = ((future["high"] <= upper) & (future["low"] >= lower)).all()
    escaped_up = (future["high"] > upper).any()
    escaped_down = (future["low"] < lower).any()

    max_high = future["high"].max()
    min_low = future["low"].min()

    max_pct_above_upper = max(0, (max_high - upper) / center)
    max_pct_below_lower = max(0, (lower - min_low) / center)

    final_close = future.iloc[-1]["close"]
    close_vs_center = (final_close - center) / center

    inside_mask = (future["high"] <= upper) & (future["low"] >= lower)
    inside_hours = int(inside_mask.sum())

    inside_range = future[inside_mask]
    if len(inside_range) > 0:
        inside_move = (inside_range["high"].max() - inside_range["low"].min()) / center
    else:
        inside_move = 0

    total_move = (max_high - min_low) / center
    directional_move = abs(final_close - future.iloc[0]["open"]) / center
    chop_ratio = total_move / directional_move if directional_move > 0 else 0

    total_range = max_high - min_low
    approx_crosses = total_range / spacing if spacing > 0 else 0

    first_escape = None
    for _, r in future.iterrows():
        if r["high"] > upper or r["low"] < lower:
            first_escape = (r["timestamp"] - t0).total_seconds() / 3600
            break

    productive = (approx_crosses >= 4) and (inside_hours >= 12)

    stranded = (
        (not stayed_in_range)
        and (
            inside_hours < (EVAL_WINDOW_HOURS * 0.40)
            or abs(close_vs_center) > 0.02
        )
    )

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

    return {
        "timestamp": t0,
        "stayed_in_range_24h": stayed_in_range,
        "escaped_up_24h": escaped_up,
        "escaped_down_24h": escaped_down,
        "first_escape_hours": first_escape,
        "inside_range_hours_24h": inside_hours,
        "realized_move_pct_24h": total_move,
        "approx_level_crosses_24h": approx_crosses,
        "productive_grid_24h": productive,
        "likely_stranded_24h": stranded,
        "max_pct_above_upper_24h": max_pct_above_upper,
        "max_pct_below_lower_24h": max_pct_below_lower,
        "close_vs_center_24h": close_vs_center,
        "inside_move_pct_24h": inside_move,
        "chop_ratio_24h": chop_ratio,
        "evaluation_score_24h": score,
    }


def summarize_eval(eval_df, decision_df):
    merged = eval_df.merge(
        decision_df[["timestamp", "operational_regime", "candidate_score"]],
        on="timestamp",
        how="left"
    )

    merged["score_bucket"] = pd.cut(
        merged["candidate_score"],
        bins=[-999, 6, 7.5, 9, 999],
        labels=["<6", "6–7.5", "7.5–9", "9+"]
    )

    rows = []

    def add_group(name, subset):
        if len(subset) == 0:
            return
        rows.append({
            "group": name,
            "count": len(subset),
            "avg_eval_score": subset["evaluation_score_24h"].mean(),
            "stay_rate": subset["stayed_in_range_24h"].mean(),
            "productive_rate": subset["productive_grid_24h"].mean(),
            "stranded_rate": subset["likely_stranded_24h"].mean(),
        })

    add_group("OVERALL", merged)

    for regime in sorted(merged["operational_regime"].dropna().unique()):
        add_group(f"REGIME_{regime}", merged[merged["operational_regime"] == regime])

    for bucket in ["<6", "6–7.5", "7.5–9", "9+"]:
        add_group(f"SCORE_{bucket}", merged[merged["score_bucket"] == bucket])

    return pd.DataFrame(rows)


# -----------------------------
# MAIN
# -----------------------------
def main():
    ensure_output_dir()

    print("Loading historical data...")
    raw = load_ohlcv_csv(DATA_PATH)

    if LOOKBACK_ROWS is not None and len(raw) > LOOKBACK_ROWS:
        raw = raw.iloc[-LOOKBACK_ROWS:].copy().reset_index(drop=True)
        print(f"Using recent lookback only: {len(raw)} rows")
    else:
        print(f"Using full history: {len(raw)} rows")

    decisions = []

    print("Running replay...")
    gmm = None
    scaler = None

    for idx, i in enumerate(range(WARMUP_BARS, len(raw) - EVAL_WINDOW_HOURS, STEP_HOURS), start=1):
        hist = raw.iloc[:i + 1].copy()

        hist = compute_features(hist)

        # Refit GMM only periodically
        if gmm is None or ((idx - 1) % GMM_REFIT_EVERY == 0):
            gmm, scaler = fit_gmm(hist)

        hist = assign_clusters(hist, gmm, scaler)
        hist = apply_policy(hist)

        latest = hist.iloc[-1].copy()
        grid = build_grid(latest)

        row_out = latest.to_dict()
        row_out.update(grid)
        decisions.append(row_out)

        if idx % 250 == 0:
            print(f"Processed {idx} replay decisions...")

    decision_df = pd.DataFrame(decisions)
    decision_df["timestamp"] = pd.to_datetime(decision_df["timestamp"], utc=True)
    decision_df = decision_df.sort_values("timestamp").reset_index(drop=True)

    print(f"Replay decisions generated: {len(decision_df)}")
    decision_df.to_csv(DECISION_PATH, index=False)
    decision_df.tail(1).to_csv(LATEST_PATH, index=False)

    print("Evaluating replay decisions...")
    eval_rows = []
    for _, row in decision_df.iterrows():
        res = evaluate_row(row, raw)
        if res:
            eval_rows.append(res)

    eval_df = pd.DataFrame(eval_rows)
    eval_df["timestamp"] = pd.to_datetime(eval_df["timestamp"], utc=True)
    eval_df = eval_df.sort_values("timestamp").reset_index(drop=True)
    eval_df.to_csv(EVAL_PATH, index=False)

    print("Building replay summary...")
    summary_df = summarize_eval(eval_df, decision_df)
    summary_df.to_csv(SUMMARY_PATH, index=False)

    print("====================================")
    print("REPLAY COMPLETE")
    print("====================================")
    print(f"Replay decisions: {DECISION_PATH}")
    print(f"Replay evaluations: {EVAL_PATH}")
    print(f"Replay summary: {SUMMARY_PATH}")
    print(f"Latest replay decision: {LATEST_PATH}")


if __name__ == "__main__":
    main()

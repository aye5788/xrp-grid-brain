import pandas as pd
import numpy as np

DECISION_PATH = "outputs/replay/decision_history_replay.csv"
EVAL_PATH = "outputs/replay/evaluation_history_replay.csv"
OUT_DIR = "outputs/replay"

# -----------------------------
# CONFIG
# -----------------------------
FEATURE_COLS = [
    "ret_1",
    "ret_6",
    "ret_12",
    "ret_24",
    "rv_12",
    "rv_24",
    "rv_48",
    "atr_pct_14",
    "ma_slope_24",
    "ma_slope_48",
    "zscore_24",
    "reversion_proxy",
    "range_width_24",
    "range_pos_24",
    "bar_return_abs_z",
    "range_expansion_ratio",
    "cluster_confidence",
    "grid_lower",
    "grid_upper",
    "levels",
    "spacing",
    "spacing_pct",
    "width_pct",
    "center_price",
    "center_shift_pct",
    "candidate_score",
]

GOOD_DEF = "good"
BAD_DEF = "bad"

# -----------------------------
# LOAD
# -----------------------------
def load_data():
    decisions = pd.read_csv(DECISION_PATH)
    evals = pd.read_csv(EVAL_PATH)

    decisions["timestamp"] = pd.to_datetime(decisions["timestamp"], utc=True)
    evals["timestamp"] = pd.to_datetime(evals["timestamp"], utc=True)

    df = evals.merge(decisions, on="timestamp", how="left")
    return df


# -----------------------------
# LABEL GOOD / BAD
# -----------------------------
def label_outcomes(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # GOOD = stayed in range + not stranded + decent score
    df["is_good"] = (
        (df["stayed_in_range_24h"] == True)
        & (df["likely_stranded_24h"] == False)
        & (df["evaluation_score_24h"] >= 45)
    )

    # BAD = stranded OR low score OR escaped quickly
    df["is_bad"] = (
        (df["likely_stranded_24h"] == True)
        | (df["evaluation_score_24h"] <= 25)
        | (
            df["first_escape_hours"].notna()
            & (df["first_escape_hours"] <= 6)
        )
    )

    return df


# -----------------------------
# SUMMARY TABLE
# -----------------------------
def summarize_feature_differences(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    good = df[df["is_good"]].copy()
    bad = df[df["is_bad"]].copy()

    for col in FEATURE_COLS:
        if col not in df.columns:
            continue

        g = pd.to_numeric(good[col], errors="coerce").dropna()
        b = pd.to_numeric(bad[col], errors="coerce").dropna()

        if len(g) < 10 or len(b) < 10:
            continue

        g_mean = g.mean()
        b_mean = b.mean()
        g_med = g.median()
        b_med = b.median()
        pooled_std = np.nanstd(pd.concat([g, b], axis=0))
        effect = (g_mean - b_mean) / pooled_std if pooled_std and pooled_std > 0 else np.nan

        rows.append({
            "feature": col,
            "good_n": len(g),
            "bad_n": len(b),
            "good_mean": g_mean,
            "bad_mean": b_mean,
            "good_median": g_med,
            "bad_median": b_med,
            "mean_diff_good_minus_bad": g_mean - b_mean,
            "effect_size_std_units": effect,
            "abs_effect_rank": abs(effect) if pd.notna(effect) else np.nan
        })

    out = pd.DataFrame(rows).sort_values("abs_effect_rank", ascending=False)
    return out


# -----------------------------
# REGIME BREAKDOWN
# -----------------------------
def regime_breakdown(df: pd.DataFrame) -> pd.DataFrame:
    grp = (
        df.groupby("operational_regime", dropna=False)
        .agg(
            total=("timestamp", "count"),
            good_rate=("is_good", "mean"),
            bad_rate=("is_bad", "mean"),
            avg_eval_score=("evaluation_score_24h", "mean"),
            stay_rate=("stayed_in_range_24h", "mean"),
            stranded_rate=("likely_stranded_24h", "mean"),
        )
        .reset_index()
        .sort_values("avg_eval_score", ascending=False)
    )
    return grp


# -----------------------------
# BUCKET BREAKDOWN
# -----------------------------
def bucket_breakdown(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    out["score_bucket"] = pd.cut(
        out["candidate_score"],
        bins=[-999, 6, 7.5, 9, 10.5, 999],
        labels=["<6", "6–7.5", "7.5–9", "9–10.5", "10.5+"]
    )

    grp = (
        out.groupby("score_bucket", dropna=False)
        .agg(
            total=("timestamp", "count"),
            good_rate=("is_good", "mean"),
            bad_rate=("is_bad", "mean"),
            avg_eval_score=("evaluation_score_24h", "mean"),
            stay_rate=("stayed_in_range_24h", "mean"),
            stranded_rate=("likely_stranded_24h", "mean"),
        )
        .reset_index()
    )
    return grp


# -----------------------------
# MAIN
# -----------------------------
def main():
    print("Loading replay decision + evaluation data...")
    df = load_data()
    df = label_outcomes(df)

    total = len(df)
    good_n = int(df["is_good"].sum())
    bad_n = int(df["is_bad"].sum())

    print("\n======================================")
    print("REPLAY FAILURE DIAGNOSTIC")
    print("======================================")
    print(f"Total rows: {total}")
    print(f"Good rows: {good_n}")
    print(f"Bad rows:  {bad_n}")

    # Save labeled master
    labeled_path = f"{OUT_DIR}/replay_labeled_diagnostics.csv"
    df.to_csv(labeled_path, index=False)
    print(f"\nSaved labeled dataset -> {labeled_path}")

    # Feature comparison
    feat = summarize_feature_differences(df)
    feat_path = f"{OUT_DIR}/feature_differences_good_vs_bad.csv"
    feat.to_csv(feat_path, index=False)
    print(f"Saved feature comparison -> {feat_path}")

    # Regime comparison
    reg = regime_breakdown(df)
    reg_path = f"{OUT_DIR}/regime_diagnostic_breakdown.csv"
    reg.to_csv(reg_path, index=False)
    print(f"Saved regime breakdown -> {reg_path}")

    # Score bucket comparison
    buck = bucket_breakdown(df)
    buck_path = f"{OUT_DIR}/score_bucket_diagnostic_breakdown.csv"
    buck.to_csv(buck_path, index=False)
    print(f"Saved score bucket breakdown -> {buck_path}")

    print("\n======================================")
    print("TOP FEATURE DIFFERENCES (GOOD vs BAD)")
    print("======================================")
    print(feat.head(15).to_string(index=False))

    print("\n======================================")
    print("REGIME BREAKDOWN")
    print("======================================")
    print(reg.to_string(index=False))

    print("\n======================================")
    print("SCORE BUCKET BREAKDOWN")
    print("======================================")
    print(buck.to_string(index=False))


if __name__ == "__main__":
    main()

import pandas as pd
from pathlib import Path

EVAL_PATH = "outputs/evaluation_history.csv"
SUMMARY_PATH = "outputs/eval_summary_latest.csv"


def safe_mean(series):
    if len(series) == 0:
        return None
    return series.mean()


def summarize_group(df, group_name):
    return {
        "group": group_name,
        "count": len(df),
        "avg_eval_score": safe_mean(df["evaluation_score_24h"]),
        "stay_rate": safe_mean(df["stayed_in_range_24h"].astype(float)),
        "productive_rate": safe_mean(df["productive_grid_24h"].astype(float)),
        "stranded_rate": safe_mean(df["likely_stranded_24h"].astype(float)),
        "avg_inside_hours": safe_mean(df["inside_range_hours_24h"]),
        "avg_escape_hours": safe_mean(df["first_escape_hours"].dropna()) if "first_escape_hours" in df.columns else None,
        "avg_price_vs_center_pct": safe_mean(df["price_vs_center_pct"]) if "price_vs_center_pct" in df.columns else None,
        "avg_distance_to_boundary_pct": safe_mean(df["distance_to_boundary_pct"]) if "distance_to_boundary_pct" in df.columns else None,
    }


def assign_score_bucket(score):
    if score < 6:
        return "SCORE_<6"
    elif score < 7.5:
        return "SCORE_6_7.5"
    elif score < 9:
        return "SCORE_7.5_9"
    else:
        return "SCORE_9+"


def main():
    if not Path(EVAL_PATH).exists():
        print(f"Missing evaluation file: {EVAL_PATH}")
        return

    df = pd.read_csv(EVAL_PATH)

    if df.empty:
        print("Evaluation file is empty.")
        return

    rows = []

    # -----------------------------
    # Overall summary
    # -----------------------------
    rows.append(summarize_group(df, "OVERALL"))

    # -----------------------------
    # By regime (if present)
    # -----------------------------
    if "operational_regime" in df.columns:
        for regime, sub in df.groupby("operational_regime"):
            rows.append(summarize_group(sub, f"REGIME_{regime}"))

    # -----------------------------
    # By score bucket (using candidate_score if present)
    # -----------------------------
    if "candidate_score" in df.columns:
        df["score_bucket"] = df["candidate_score"].apply(assign_score_bucket)
        for bucket, sub in df.groupby("score_bucket"):
            rows.append(summarize_group(sub, bucket))

    # -----------------------------
    # NEW: By lifecycle action
    # -----------------------------
    if "lifecycle_action" in df.columns:
        for action, sub in df.groupby("lifecycle_action"):
            rows.append(summarize_group(sub, f"LIFECYCLE_ACTION_{action}"))

    # -----------------------------
    # NEW: By lifecycle mode
    # -----------------------------
    if "lifecycle_mode" in df.columns:
        for mode, sub in df.groupby("lifecycle_mode"):
            rows.append(summarize_group(sub, f"LIFECYCLE_MODE_{mode}"))

    summary_df = pd.DataFrame(rows)
    summary_df.to_csv(SUMMARY_PATH, index=False)

    print(f"Summary updated → {SUMMARY_PATH}")


if __name__ == "__main__":
    main()

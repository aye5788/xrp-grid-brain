import pandas as pd
from src.grid.candidate_builder import build_grid_variants


LOOKBACK_HOURS = 24
TOP_N_EXPORT = 15


def generate_latest_grid(df):
    df = df.copy()

    # Ensure sorted
    df = df.sort_values("timestamp").reset_index(drop=True)

    latest_ts = df.iloc[-1]["timestamp"]

    # -----------------------------------
    # STEP 1: build candidate pool
    # -----------------------------------
    recent = df.tail(LOOKBACK_HOURS)

    candidates = []

    for _, row in recent.iterrows():
        try:
            row_candidates = build_grid_variants(row)

            for candidate in row_candidates:
                combined = {
                    **row.to_dict(),
                    **candidate
                }
                candidates.append(combined)

        except Exception:
            continue

    if not candidates:
        raise ValueError("No valid grid candidates generated.")

    pool_df = pd.DataFrame(candidates)

    # -----------------------------------
    # STEP 2: basic filtering
    # -----------------------------------
    pool_df = pool_df[
        (pool_df["grid_lower"].notna()) &
        (pool_df["grid_upper"].notna()) &
        (pool_df["grid_lower"] < pool_df["grid_upper"])
    ].copy()

    # Optional: filter completely garbage scores
    if "candidate_score" in pool_df.columns:
        pool_df = pool_df[pool_df["candidate_score"] >= 5].copy()

    if pool_df.empty:
        raise ValueError("All candidates filtered out.")

    # -----------------------------------
    # STEP 3: ranking
    # -----------------------------------
    # Prefer tradable first, then highest score
    pool_df = pool_df.sort_values(
        by=["tradable", "candidate_score"],
        ascending=[False, False]
    ).copy()

    best = pool_df.iloc[0].to_dict()

    # -----------------------------------
    # STEP 4: EXPORT candidate pool
    # -----------------------------------
    export_pool = pool_df.head(TOP_N_EXPORT).copy()
    export_pool["snapshot_ts"] = latest_ts

    export_pool.to_csv("outputs/candidate_pool_latest.csv", index=False)

    try:
        existing = pd.read_csv("outputs/candidate_pool_history.csv")
        combined = pd.concat([existing, export_pool], ignore_index=True)
    except Exception:
        combined = export_pool

    combined.to_csv("outputs/candidate_pool_history.csv", index=False)

    return best

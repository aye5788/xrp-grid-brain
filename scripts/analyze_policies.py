import pandas as pd

DECISION_PATH = "outputs/replay/decision_history_replay.csv"
EVAL_PATH = "outputs/replay/evaluation_history_replay.csv"


def load_data():
    decisions = pd.read_csv(DECISION_PATH)
    evals = pd.read_csv(EVAL_PATH)

    decisions["timestamp"] = pd.to_datetime(decisions["timestamp"], utc=True)
    evals["timestamp"] = pd.to_datetime(evals["timestamp"], utc=True)

    df = evals.merge(
        decisions[
            [
                "timestamp",
                "candidate_score",
                "operational_regime",
                "tradable"
            ]
        ],
        on="timestamp",
        how="left"
    )

    return df


def summarize(name, subset):
    if len(subset) == 0:
        return {
            "policy": name,
            "count": 0,
            "coverage_pct": 0,
            "avg_eval_score": None,
            "stay_rate": None,
            "stranded_rate": None,
        }

    return {
        "policy": name,
        "count": len(subset),
        "coverage_pct": None,  # filled later
        "avg_eval_score": subset["evaluation_score_24h"].mean(),
        "stay_rate": subset["stayed_in_range_24h"].mean(),
        "stranded_rate": subset["likely_stranded_24h"].mean(),
    }


def run_analysis(df):
    total = len(df)

    policies = []

    # --- POLICY A: baseline (all tradable) ---
    pA = df[df["tradable"] == True]
    policies.append(summarize("baseline_tradable", pA))

    # --- POLICY B: score >= 9 ---
    pB = df[(df["tradable"] == True) & (df["candidate_score"] >= 9)]
    policies.append(summarize("score_ge_9", pB))

    # --- POLICY C: score + regime ---
    pC = df[
        (df["tradable"] == True)
        & (df["candidate_score"] >= 9)
        & (df["operational_regime"].isin(["RANGE_GOOD", "RANGE_TREND_DOWN"]))
    ]
    policies.append(summarize("score_ge_9_regime_filtered", pC))

    # --- POLICY D: strict ---
    pD = df[
        (df["tradable"] == True)
        & (df["candidate_score"] >= 9.5)
        & (df["operational_regime"] == "RANGE_GOOD")
    ]
    policies.append(summarize("strict_high_conviction", pD))

    # Fill coverage %
    for p in policies:
        p["coverage_pct"] = p["count"] / total if total > 0 else 0

    return pd.DataFrame(policies)


def main():
    print("Loading replay data...")
    df = load_data()

    print(f"Total evaluated rows: {len(df)}")

    results = run_analysis(df)

    print("\n=== POLICY COMPARISON ===\n")
    print(results.to_string(index=False))


if __name__ == "__main__":
    main()

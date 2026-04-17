def map_regime(cluster_id: int) -> str:
    mapping = {
        0: "RANGE_GOOD",
        1: "RANGE_TREND_UP",
        2: "RANGE_TREND_DOWN",
        3: "TREND",
        4: "NO_TRADE"
    }
    return mapping.get(cluster_id, "NO_TRADE")


def apply_policy(df):
    df = df.copy()

    df["operational_regime"] = df["regime_cluster"].apply(map_regime)

    def risk_mode(row):
        if row["cluster_confidence"] < 0.55:
            return "OFF"
        if row["cluster_confidence"] < 0.7:
            return "TRANSITION"
        return "NORMAL"

    df["risk_mode"] = df.apply(risk_mode, axis=1)

    return df

import numpy as np
import pandas as pd
from sklearn.mixture import GaussianMixture
from sklearn.preprocessing import StandardScaler


FEATURE_COLS = [
    "atr_pct_14",
    "rv_12", "rv_24", "rv_48",
    "ret_6", "ret_12", "ret_24",
    "ma_slope_24", "ma_slope_48",
    "zscore_24", "reversion_proxy",
    "range_width_24", "range_pos_24",
    "bar_return_abs_z",
    "range_expansion_ratio"
]


# -----------------------------------
# FIT
# -----------------------------------
def fit_gmm(df: pd.DataFrame, n_components=5):
    X = df[FEATURE_COLS].values

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    gmm = GaussianMixture(
        n_components=n_components,
        random_state=42
    )
    gmm.fit(X_scaled)

    return gmm, scaler


# -----------------------------------
# CANONICALIZATION LOGIC
# -----------------------------------
def build_cluster_canonical_map(df: pd.DataFrame):
    """
    Assign stable semantic meaning to clusters based on interpretable metrics
    """
    cluster_stats = (
        df.groupby("regime_cluster")[["rv_24", "range_width_24", "ret_24"]]
        .mean()
        .reset_index()
    )

    # Sort clusters by volatility (rv_24)
    cluster_stats = cluster_stats.sort_values("rv_24").reset_index(drop=True)

    canonical_map = {}

    for i, row in cluster_stats.iterrows():
        cluster_id = int(row["regime_cluster"])

        # LOW VOL clusters first → RANGE types
        if i == 0:
            canonical_map[cluster_id] = 0  # RANGE_GOOD
        elif i in (1, 2):
            # Disambiguate directional RANGE regimes by mean ret_24 sign rather
            # than rank order. Rank order alone can swap UP/DOWN between retrains
            # when two clusters have similar volatility but opposite direction.
            canonical_map[cluster_id] = 1 if row["ret_24"] >= 0 else 2
        elif i == 3:
            canonical_map[cluster_id] = 3  # TREND
        else:
            canonical_map[cluster_id] = 4  # NO_TRADE

    return canonical_map


# -----------------------------------
# ASSIGN
# -----------------------------------
def assign_clusters(df, gmm, scaler):
    X = df[FEATURE_COLS].values
    X_scaled = scaler.transform(X)

    clusters = gmm.predict(X_scaled)
    probs = gmm.predict_proba(X_scaled)

    df = df.copy()
    df["regime_cluster_raw"] = clusters
    df["cluster_confidence"] = probs.max(axis=1)

    # Build canonical mapping from full dataset
    df["regime_cluster"] = df["regime_cluster_raw"]
    canonical_map = build_cluster_canonical_map(df)

    # Apply canonical mapping
    df["regime_cluster"] = df["regime_cluster_raw"].map(canonical_map)

    return df

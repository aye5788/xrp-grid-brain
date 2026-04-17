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


def assign_clusters(df, gmm, scaler):
    X = df[FEATURE_COLS].values
    X_scaled = scaler.transform(X)

    clusters = gmm.predict(X_scaled)
    probs = gmm.predict_proba(X_scaled)

    df = df.copy()
    df["regime_cluster"] = clusters
    df["cluster_confidence"] = probs.max(axis=1)

    return df

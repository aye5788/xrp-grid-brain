import pandas as pd

from src.io.loaders import load_ohlcv_csv
from src.features.regime_features import compute_features
from src.models.gmm_regime import fit_gmm, assign_clusters
from src.policy.regime_policy import apply_policy


def run_pipeline(csv_path: str) -> pd.DataFrame:
    df = load_ohlcv_csv(csv_path)

    df = compute_features(df)

    gmm, scaler = fit_gmm(df)

    df = assign_clusters(df, gmm, scaler)

    df = apply_policy(df)

    return df

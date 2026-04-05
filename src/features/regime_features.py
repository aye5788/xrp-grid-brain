import pandas as pd
import numpy as np


def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # RETURNS
    df["ret_1"] = df["close"].pct_change()
    df["ret_6"] = df["close"].pct_change(6)
    df["ret_12"] = df["close"].pct_change(12)
    df["ret_24"] = df["close"].pct_change(24)

    # VOLATILITY
    df["rv_12"] = df["ret_1"].rolling(12).std()
    df["rv_24"] = df["ret_1"].rolling(24).std()
    df["rv_48"] = df["ret_1"].rolling(48).std()

    # ATR
    high_low = df["high"] - df["low"]
    high_close = np.abs(df["high"] - df["close"].shift())
    low_close = np.abs(df["low"] - df["close"].shift())

    tr = np.maximum(high_low, np.maximum(high_close, low_close))
    df["atr_14"] = tr.rolling(14).mean()
    df["atr_pct_14"] = df["atr_14"] / df["close"]

    # MOVING AVERAGES
    df["ma_24"] = df["close"].rolling(24).mean()
    df["ma_48"] = df["close"].rolling(48).mean()

    df["ma_slope_24"] = df["ma_24"].diff()
    df["ma_slope_48"] = df["ma_48"].diff()

    # MEAN REVERSION
    df["zscore_24"] = (
        (df["close"] - df["ma_24"]) /
        df["close"].rolling(24).std()
    )

    df["reversion_proxy"] = -df["zscore_24"]

    # RANGE STRUCTURE
    df["range_high_24"] = df["high"].rolling(24).max()
    df["range_low_24"] = df["low"].rolling(24).min()

    df["range_width_24"] = (
        df["range_high_24"] - df["range_low_24"]
    ) / df["close"]

    df["range_pos_24"] = (
        (df["close"] - df["range_low_24"]) /
        (df["range_high_24"] - df["range_low_24"])
    )

    # EXPANSION
    df["bar_return_abs_z"] = (
        df["ret_1"].abs() /
        df["ret_1"].rolling(24).std()
    )

    df["range_expansion_ratio"] = (
        df["range_width_24"] /
        df["range_width_24"].rolling(24).mean()
    )

    return df.dropna().reset_index(drop=True)

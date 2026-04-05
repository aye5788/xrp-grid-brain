import numpy as np


def build_grid(row):
    price = row["close"]

    width = row["atr_pct_14"] * 6

    lower = price * (1 - width)
    upper = price * (1 + width)

    levels = 10
    spacing = (upper - lower) / levels

    fee = 0.002  # 0.2%

    est_profit = spacing / price - fee

    tradable = est_profit > 0

    return {
        "grid_lower": lower,
        "grid_upper": upper,
        "levels": levels,
        "spacing": spacing,
        "est_profit_per_level": est_profit,
        "tradable": tradable
    }

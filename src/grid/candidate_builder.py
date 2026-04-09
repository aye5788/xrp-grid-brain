import math


FEE_PCT = 0.002  # 0.20% round-trip estimate


def clamp(x, low=0.0, high=1.0):
    return max(low, min(high, x))


def score_candidate(candidate):
    spacing_pct = candidate["spacing_pct"]
    width_pct = candidate["width_pct"]
    est_profit = candidate["est_profit_per_level"]
    cluster_conf = candidate.get("cluster_confidence", 0.5)
    regime = candidate.get("operational_regime", "UNKNOWN")
    range_pos = candidate.get("range_pos_24", 0.5)
    atr_pct = candidate.get("atr_pct_14", 0.01)

    # --- Core profitability reward ---
    profit_score = est_profit * 100

    # --- Reward sufficient width, but not endlessly ---
    width_score = min(width_pct * 60, 6.0)

    # --- Reward sufficient spacing, but not endlessly ---
    spacing_score = min(spacing_pct * 80, 5.0)

    # --- Confidence reward ---
    confidence_score = cluster_conf * 4.0

    # --- Regime reward ---
    regime_bonus_map = {
        "RANGE_GOOD": 4.0,
        "RANGE_TREND_UP": 2.5,
        "RANGE_TREND_DOWN": 2.5,
        "TREND": 1.0,
        "NO_TRADE": -3.0,
    }
    regime_score = regime_bonus_map.get(regime, 0.0)

    # --- Penalize if center is too close to edges of recent range ---
    edge_penalty = 0.0
    if range_pos < 0.12 or range_pos > 0.88:
        edge_penalty = -1.5

    # =========================================================
    # NEW: ACTIVITY / PRODUCTIVITY REWARD
    # =========================================================
    #
    # Goal:
    # - avoid always picking the widest / safest grid
    # - reward grids that are likely to actually trade
    #
    # Idea:
    # - spacing should be comfortably above fee threshold
    # - but not so huge that fills become sparse
    # - width should be enough to survive
    # - but not so huge that the bot becomes lazy
    #
    # These "ideal zones" can be tuned later.
    #

    # Ideal spacing zone for XRP hourly grid behavior
    ideal_spacing_center = 0.020   # 2.0%
    ideal_spacing_tolerance = 0.010  # ±1.0%

    spacing_distance = abs(spacing_pct - ideal_spacing_center)
    spacing_activity_score = max(
        0.0,
        3.0 * (1 - (spacing_distance / ideal_spacing_tolerance))
    )

    # Ideal width zone
    ideal_width_center = 0.10   # 10%
    ideal_width_tolerance = 0.04  # ±4%

    width_distance = abs(width_pct - ideal_width_center)
    width_activity_score = max(
        0.0,
        3.0 * (1 - (width_distance / ideal_width_tolerance))
    )

    activity_score = spacing_activity_score + width_activity_score

    # --- Mild ATR alignment reward ---
    atr_alignment = 0.0
    if 0.008 <= atr_pct <= 0.018:
        atr_alignment = 1.0

    # --- Final score ---
    candidate_score = (
        profit_score
        + width_score
        + spacing_score
        + confidence_score
        + regime_score
        + activity_score
        + atr_alignment
        + edge_penalty
    )

    candidate["candidate_score"] = round(candidate_score, 6)
    return candidate


def build_grid_variants(row):
    close = row["close"]
    atr = row["atr_14"]
    regime = row.get("operational_regime", "UNKNOWN")

    # Base adaptive width multiplier by regime
    if regime == "RANGE_GOOD":
        width_mult = 5.0
    elif regime in ["RANGE_TREND_UP", "RANGE_TREND_DOWN"]:
        width_mult = 4.5
    elif regime == "TREND":
        width_mult = 4.0
    else:
        width_mult = 3.5

    center_price = close * (1 - 0.003)

    variants = [
        ("tight", 0.90),
        ("base", 1.00),
        ("wide", 1.12),
    ]

    candidate_rows = []

    for variant_label, width_scalar in variants:
        width = atr * width_mult * width_scalar
        grid_lower = center_price - width
        grid_upper = center_price + width
        levels = 6

        spacing = (grid_upper - grid_lower) / (levels - 1)
        spacing_pct = spacing / center_price
        width_pct = (grid_upper - grid_lower) / center_price

        est_profit_per_level = spacing_pct - FEE_PCT
        tradable = est_profit_per_level > 0.003  # still preserve GoodCrypto-style minimum edge

        candidate = {
            **row.to_dict(),
            "grid_lower": grid_lower,
            "grid_upper": grid_upper,
            "levels": levels,
            "spacing": spacing,
            "spacing_pct": spacing_pct,
            "width_pct": width_pct,
            "center_price": center_price,
            "center_shift_pct": -0.003,
            "fee_pct": FEE_PCT,
            "est_profit_per_level": est_profit_per_level,
            "regime": regime,
            "size_penalty": 0.0,
            "tradable": tradable,
            "selection_reason": f"adaptive_geometry_v2_{variant_label}",
            "variant_label": variant_label,
        }

        candidate = score_candidate(candidate)
        candidate_rows.append(candidate)

    return candidate_rows


def build_grid(row):
    """
    Backward-compatible helper:
    returns the single best candidate for legacy callers.
    """
    candidates = build_grid_variants(row)
    tradable_candidates = [c for c in candidates if c["tradable"]]

    if tradable_candidates:
        best = max(tradable_candidates, key=lambda x: x["candidate_score"])
    else:
        best = max(candidates, key=lambda x: x["candidate_score"])

    return best

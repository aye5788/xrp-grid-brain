import math
from typing import Dict, List, Optional


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def estimate_net_per_level(spacing_pct: float, fee_pct: float) -> float:
    """
    GoodCrypto-style simplified fee-aware per-level estimate.

    Approximation:
    - gross opportunity per completed buy/sell cycle is roughly spacing_pct
    - round-trip fee drag is ~ 2 * fee_pct
    """
    return spacing_pct - (2.0 * fee_pct)


def get_regime_profile(row) -> Dict[str, float]:
    regime = str(row.get("operational_regime", "NO_TRADE"))
    risk_mode = str(row.get("risk_mode", "OFF"))
    confidence = safe_float(row.get("cluster_confidence", 0.0), 0.0)

    # Base profile by regime
    profiles = {
        "RANGE_GOOD": {
            "center_shift": 0.00,
            "width_mults": [3.5, 4.5, 5.5, 6.5, 8.0],
            "levels": [6, 8, 10, 12],
            "size_penalty": 0.00,
            "allow_trade": True,
        },
        "RANGE_TREND_UP": {
            "center_shift": 0.15,   # slightly above current price
            "width_mults": [4.0, 5.0, 6.0, 7.0, 8.0],
            "levels": [6, 8, 10],
            "size_penalty": 0.20,
            "allow_trade": True,
        },
        "RANGE_TREND_DOWN": {
            "center_shift": -0.15,  # slightly below current price
            "width_mults": [4.5, 5.5, 6.5, 7.5, 8.5],
            "levels": [6, 8, 10],
            "size_penalty": 0.30,
            "allow_trade": True,
        },
        "TREND": {
            "center_shift": 0.00,
            "width_mults": [6.0, 7.0, 8.0],
            "levels": [6, 8],
            "size_penalty": 0.50,
            "allow_trade": False,
        },
        "NO_TRADE": {
            "center_shift": 0.00,
            "width_mults": [6.0],
            "levels": [6],
            "size_penalty": 1.00,
            "allow_trade": False,
        },
    }

    profile = profiles.get(regime, profiles["NO_TRADE"]).copy()

    # Confidence / risk-mode adjustments
    if risk_mode == "TRANSITION":
        profile["width_mults"] = [w + 0.5 for w in profile["width_mults"]]
        profile["levels"] = [lvl for lvl in profile["levels"] if lvl <= 10]
        profile["size_penalty"] += 0.20
    elif risk_mode == "OFF":
        profile["allow_trade"] = False
        profile["size_penalty"] += 1.00

    if confidence < 0.55:
        profile["allow_trade"] = False
        profile["size_penalty"] += 0.50
    elif confidence < 0.70:
        profile["size_penalty"] += 0.15

    profile["confidence"] = confidence
    profile["regime"] = regime
    profile["risk_mode"] = risk_mode
    return profile


def generate_grid_candidates(row) -> List[Dict]:
    price = safe_float(row.get("close"), 0.0)
    atr_pct = safe_float(row.get("atr_pct_14"), 0.0)
    range_width_24 = safe_float(row.get("range_width_24"), 0.0)
    range_pos_24 = safe_float(row.get("range_pos_24"), 0.5)
    rv_24 = safe_float(row.get("rv_24"), 0.0)

    if price <= 0 or atr_pct <= 0:
        return []

    profile = get_regime_profile(row)
    fee_pct = 0.002  # 0.20% per side, conservative placeholder

    # Normalize / bound values
    range_pos_24 = clamp(range_pos_24, 0.0, 1.0)

    # Use both ATR and realized observed range structure
    structural_floor = max(atr_pct * 3.0, range_width_24 * 0.50, 0.01)
    structural_cap = max(atr_pct * 12.0, range_width_24 * 1.50, 0.03)

    candidates: List[Dict] = []

    for width_mult in profile["width_mults"]:
        raw_width_pct = atr_pct * width_mult

        # Blend raw ATR width with structural floor/cap
        width_pct = clamp(raw_width_pct, structural_floor, structural_cap)

        # Volatility-aware expansion / contraction
        volatility_bias = clamp(rv_24 / atr_pct, 0.8, 1.5)
        width_pct *= volatility_bias

        # Stronger center shift relative to full width
        directional_bias = rv_24 * 2.0
        center_shift_pct = profile["center_shift"] * width_pct * (1.0 + directional_bias)

        # Mild additional adjustment based on where price sits in recent range
        # If price is already high in range, bias center down a touch; vice versa.
        structural_center_adjust = (0.5 - range_pos_24) * 0.20 * width_pct

        center_price = price * (1.0 + center_shift_pct + structural_center_adjust)

        grid_lower = center_price * (1.0 - width_pct / 2.0)
        grid_upper = center_price * (1.0 + width_pct / 2.0)

        if grid_lower <= 0 or grid_upper <= grid_lower:
            continue

        for levels in profile["levels"]:
            intervals = max(levels - 1, 1)
            spacing_abs = (grid_upper - grid_lower) / intervals
            spacing_pct = spacing_abs / price

            est_profit_per_level = estimate_net_per_level(
                spacing_pct=spacing_pct,
                fee_pct=fee_pct,
            )

            candidate = {
                "grid_lower": grid_lower,
                "grid_upper": grid_upper,
                "levels": int(levels),
                "spacing": spacing_abs,
                "spacing_pct": spacing_pct,
                "width_pct": width_pct,
                "center_price": center_price,
                "center_shift_pct": center_shift_pct + structural_center_adjust,
                "fee_pct": fee_pct,
                "est_profit_per_level": est_profit_per_level,
                "regime": profile["regime"],
                "risk_mode": profile["risk_mode"],
                "cluster_confidence": profile["confidence"],
                "size_penalty": profile["size_penalty"],
                "range_pos_24": range_pos_24,
                "rv_24": rv_24,
                "atr_pct_14": atr_pct,
            }
            candidates.append(candidate)

    return candidates


def candidate_is_viable(candidate: Dict, row) -> bool:
    regime = str(candidate["regime"])
    risk_mode = str(candidate["risk_mode"])
    confidence = safe_float(candidate["cluster_confidence"], 0.0)

    spacing_pct = safe_float(candidate["spacing_pct"], 0.0)
    width_pct = safe_float(candidate["width_pct"], 0.0)
    levels = int(candidate["levels"])
    est_profit = safe_float(candidate["est_profit_per_level"], -999.0)
    atr_pct = safe_float(candidate["atr_pct_14"], 0.0)

    if regime == "NO_TRADE":
        return False

    if regime == "TREND":
        return False

    if risk_mode == "OFF":
        return False

    if confidence < 0.55:
        return False

    # Must beat fees by a meaningful margin, not just barely positive
    if est_profit <= 0.0010:
        return False

    # Avoid absurdly tight / wide grids
    if spacing_pct < max(atr_pct * 0.60, 0.004):
        return False

    if width_pct < max(atr_pct * 3.0, 0.01):
        return False

    if width_pct > max(atr_pct * 14.0, 0.12):
        return False

    if levels < 6 or levels > 14:
        return False

    return True


def score_candidate(candidate: Dict, row) -> float:
    """
    Higher is better.
    This is a policy-aware live-selection score, not a hindsight optimizer.
    """
    confidence = safe_float(candidate["cluster_confidence"], 0.0)
    regime = str(candidate["regime"])
    risk_mode = str(candidate["risk_mode"])

    est_profit = safe_float(candidate["est_profit_per_level"], -999.0)
    spacing_pct = safe_float(candidate["spacing_pct"], 0.0)
    width_pct = safe_float(candidate["width_pct"], 0.0)
    levels = int(candidate["levels"])
    size_penalty = safe_float(candidate["size_penalty"], 0.0)
    range_pos = safe_float(candidate["range_pos_24"], 0.5)

    atr_pct = safe_float(row.get("atr_pct_14"), 0.0)
    range_width_24 = safe_float(row.get("range_width_24"), 0.0)

    # Favor profitable grids that are not too cramped.
    profit_score = est_profit * 1000.0
    spacing_score = spacing_pct * 250.0

    # Want width to be roughly aligned with current structure, not too extreme.
    target_width = max(atr_pct * 6.0, range_width_24 * 0.9, 0.02)
    width_deviation_penalty = abs(width_pct - target_width) * 120.0

    # Mild preference against excessive levels when uncertain.
    level_penalty = 0.0
    if confidence < 0.75:
        level_penalty += max(levels - 10, 0) * 0.20
    if risk_mode == "TRANSITION":
        level_penalty += max(levels - 8, 0) * 0.25

    # Penalize price sitting near range extremes (breakout danger)
    edge_penalty = 0.0
    if range_pos > 0.85 or range_pos < 0.15:
        edge_penalty += 3.0

    # Regime preference
    regime_bonus_map = {
        "RANGE_GOOD": 2.50,
        "RANGE_TREND_UP": 1.50,
        "RANGE_TREND_DOWN": 1.10,
        "TREND": -2.00,
        "NO_TRADE": -10.00,
    }
    regime_bonus = regime_bonus_map.get(regime, -5.0)

    # Confidence helps, uncertainty hurts
    confidence_bonus = confidence * 4.0

    # Penalize aggression in weak states
    penalty = width_deviation_penalty + level_penalty + edge_penalty + (size_penalty * 2.0)

    score = (
        profit_score
        + spacing_score
        + regime_bonus
        + confidence_bonus
        - penalty
    )
    return score


def build_grid(row) -> Dict:
    candidates = generate_grid_candidates(row)

    if not candidates:
        return {
            "grid_lower": None,
            "grid_upper": None,
            "levels": 0,
            "spacing": None,
            "spacing_pct": None,
            "width_pct": None,
            "fee_pct": 0.002,
            "est_profit_per_level": None,
            "candidate_score": None,
            "tradable": False,
            "selection_reason": "no_candidates_generated",
        }

    viable_candidates = [c for c in candidates if candidate_is_viable(c, row)]

    if not viable_candidates:
        # Fall back to best non-viable candidate, but mark not tradable.
        scored = []
        for c in candidates:
            c = c.copy()
            c["candidate_score"] = score_candidate(c, row)
            scored.append(c)

        best = max(scored, key=lambda x: x["candidate_score"])
        best["tradable"] = False
        best["selection_reason"] = "best_non_viable_candidate"
        return best

    scored_viable = []
    for c in viable_candidates:
        c = c.copy()
        c["candidate_score"] = score_candidate(c, row)
        scored_viable.append(c)

    best = max(scored_viable, key=lambda x: x["candidate_score"])
    best["tradable"] = True
    best["selection_reason"] = "best_viable_candidate"
    return best

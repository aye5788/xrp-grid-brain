from pathlib import Path

import pandas as pd

SUMMARY_PATH = Path(__file__).parents[2] / "outputs" / "paper_summary_latest.csv"

# Rule A: fee-per-close EXIT
# fires when fees_paid / closed_trade_count exceeds threshold, indicating orphaned
# buy-side positions (grid caught in a downtrend, paying fees without recovering).
# Observed fee/close in historical replay: 0.05–0.12.
# Threshold 0.15 ≈ 25% above the empirical maximum → fires as soon as efficiency
# leaves the observed normal band. (Previous value 0.25 never triggered.)
MIN_CLOSES_FOR_FEE_RULE  = 3
MIN_BARS_FOR_FEE_RULE    = 24
FEE_PER_CLOSE_THRESHOLD  = 0.15

# Rule B: dead grid EXIT
# No meaningful activity after this many bars.
DEAD_GRID_BARS           = 72
DEAD_GRID_MIN_CLOSES     = 3

# Rule C: low activity RECENTER
# Attempt repositioning before the grid goes fully idle.
LOW_ACTIVITY_BARS        = 24
LOW_ACTIVITY_MIN_CLOSES  = 2

# Rule D: cumulative net loss EXIT
# Fires when total realized loss exceeds a calibrated floor after sustained
# operation. With fee/close ≈ 0.05–0.12 observed, 10 closes at average 0.08
# produce ~$1.0–1.6 in expected net drag. Threshold -2.0 provides ~$0.40 buffer
# above that; excess loss signals inventory accumulation or sustained fee bleed
# not caught per-trade by Rule A.
NET_LOSS_MIN_BARS        = 48
NET_LOSS_MIN_CLOSES      = 10
NET_LOSS_THRESHOLD       = -2.0
INVENTORY_DISPLACEMENT_MIN_BARS = 6
INVENTORY_DISPLACEMENT_THRESHOLD = 0.35
# Rule E: zero-close fee drain REPLACE
# Fires when fees are accumulating (grid is actively filling) but no round trips
# have completed. After 10 bars with >0.20 in fees paid and 0 closes, the grid
# is in pure one-way inventory accumulation — Rule A cannot fire (requires
# closes >= 3), so this case is otherwise undetected until bar 24.
# REPLACE rather than EXIT: price may have gapped away from center rather than
# trending; repositioning is the appropriate response.
ZERO_CLOSE_MIN_BARS      = 10
ZERO_CLOSE_FEE_FLOOR     = 0.20

ACTION_SEVERITY = {"HOLD": 0, "RECENTER": 1, "REPLACE": 2, "EXIT": 3}


def load_paper_summary() -> dict | None:
    try:
        df = pd.read_csv(SUMMARY_PATH)
        if df.empty:
            return None
        return df.iloc[-1].to_dict()
    except Exception:
        return None


def check_economic_override(
    structural_action: str,
    active_grid: dict,
    paper_summary: dict | None,
    price: float,
) -> tuple[str, str] | None:
    """
    Returns (action, reason) if economic conditions warrant overriding the structural action.
    Returns None if no override is needed.
    Override only applies if it increases action severity.

    Rule F — Inventory displacement REPLACE (leading):
        Price has fallen >35% of grid width below center_price. Fires before
        any closes accumulate — geometry-based proxy for unrealized inventory loss.

    Rule A — Fee-per-close EXIT:
        fees_paid / closed_trade_count > 0.15 (empirical normal band: 0.05–0.12).
        Indicates orphaned buy-side exposure — grid caught in a downtrend.

    Rule B — Dead grid EXIT:
        No meaningful activity after 72 bars. Price not interacting with grid.

    Rule C — Low activity RECENTER:
        Fewer than 2 closes after 24 bars while structural says HOLD.
        Attempt repositioning before the grid goes fully idle.

    Rule D — Cumulative net loss EXIT:
        After 48+ bars and 10+ closes, net_realized_pnl below -2.0.
        Catches sustained fee bleed and inventory drag not visible per-trade.

    Rule E — Zero-close fee drain REPLACE:
        fees_paid > 0.20 with closes == 0 after 10+ bars.
        Catches pure one-way inventory accumulation before Rule A can activate.
    """
    if paper_summary is None:
        return None

    bars   = int(active_grid.get("bars_since_initiation", 0))
    closes = int(paper_summary.get("closed_trade_count", 0))
    fees   = float(paper_summary.get("fees_paid", 0.0))
    net    = float(paper_summary.get("net_realized_pnl", 0.0))

    def can_override(proposed: str) -> bool:
        return ACTION_SEVERITY.get(proposed, 0) > ACTION_SEVERITY.get(structural_action, 0)

    # Rule F: inventory displacement REPLACE — leading geometry-based signal
    grid_lower   = active_grid.get("grid_lower")
    grid_upper   = active_grid.get("grid_upper")
    center_price = active_grid.get("center_price")

    if grid_lower is not None and grid_upper is not None and center_price is not None:
        grid_width   = grid_upper - grid_lower
        displacement = (center_price - price) / grid_width if grid_width > 0 else 0.0
    else:
        displacement = 0.0

    if (
        bars >= INVENTORY_DISPLACEMENT_MIN_BARS
        and displacement > INVENTORY_DISPLACEMENT_THRESHOLD
    ):
        if can_override("REPLACE"):
            return "REPLACE", "economic_replace_inventory_displacement"

    # Rule A: fee-per-close EXIT — trend trap / orphan buy detection
    if (closes >= MIN_CLOSES_FOR_FEE_RULE
            and bars >= MIN_BARS_FOR_FEE_RULE
            and fees > 0
            and (fees / closes) > FEE_PER_CLOSE_THRESHOLD):
        if can_override("EXIT"):
            return "EXIT", "economic_exit_high_fee_per_close"

    # Rule B: dead grid EXIT — no price interaction
    if bars >= DEAD_GRID_BARS and closes < DEAD_GRID_MIN_CLOSES:
        if can_override("EXIT"):
            return "EXIT", "economic_exit_idle_grid"

    # Rule C: low activity RECENTER — early inactivity, attempt reposition
    if (bars >= LOW_ACTIVITY_BARS
            and closes < LOW_ACTIVITY_MIN_CLOSES
            and structural_action == "HOLD"):
        if can_override("RECENTER"):
            return "RECENTER", "economic_recenter_low_activity"

    # Rule D: cumulative net loss EXIT — sustained fee bleed / inventory drag
    if (closes >= NET_LOSS_MIN_CLOSES
            and bars >= NET_LOSS_MIN_BARS
            and net < NET_LOSS_THRESHOLD):
        if can_override("EXIT"):
            return "EXIT", "economic_exit_cumulative_loss"

    # Rule E: zero-close fee drain REPLACE — pure one-way inventory accumulation
    if (closes == 0
            and bars >= ZERO_CLOSE_MIN_BARS
            and fees > ZERO_CLOSE_FEE_FLOOR):
        if can_override("REPLACE"):
            return "REPLACE", "economic_replace_zero_close_fee_drain"

    return None

"""
control_layer.py — Action gating for XRP Grid Brain.

Sits between lifecycle.py's geometric proposals and active_grid state writes.
Owns all temporal state (counter increments) and all action constraints.

Contract
--------
- lifecycle.py proposes an action based purely on current geometry and score.
- This module gates, escalates, or downgrades that action based on accumulated
  time-in-state: how long the grid has been alive, how long price has been at
  the edge, how long since the last significant action.
- Returns (allowed_action, reason, updated_grid). Caller is responsible for saving.
- No I/O side-effects. Deterministic given the same inputs.

Thresholds
----------
All temporal thresholds live here and nowhere else. One bar == one cycle (~1 hour).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple

# ---------------------------------------------------------------------------
# Temporal thresholds
# ---------------------------------------------------------------------------

# Bars a grid must be alive before a direct REPLACE proposal is permitted.
# Prevents immediate displacement of a freshly initiated grid.
MIN_HOLD_BARS: int = 4

# Consecutive bars with price near the grid edge (range_pos < 0.15 or > 0.85)
# before a RECENTER proposal is escalated to REPLACE.
# RECENTER by itself has no geometric effect; this converts persistent edge
# pressure into a decisive action.
RECENTER_ESCALATION_BARS: int = 3

# Consecutive bars with price outside the grid boundary before an EXIT
# proposal is confirmed. Prevents exiting on a single-bar spike.
OUTSIDE_ESCALATION_BARS: int = 2

# Bars that must elapse after an EXIT before INITIATE is permitted.
# Measured in wall-clock hours since the inactive payload records a timestamp.
EXIT_REENTRY_COOLDOWN_BARS: int = 6

# Bars that must elapse between any two REPLACE actions.
# Intentionally > MIN_HOLD_BARS: the two constraints are distinct.
# MIN_HOLD governs grid age; this governs replacement frequency.
# Carried into the new grid via bars_since_last_replace initialisation
# in lifecycle.py, so it applies across the REPLACE boundary.
REPLACE_COOLDOWN_BARS: int = 6

# Bars that must elapse between two consecutive non-escalating RECENTER actions
# on the same grid. Prevents RECENTER spam when price is drifting at the edge
# but has not yet triggered escalation. bars_near_edge still accumulates during
# the cooldown window, so escalation is unaffected.
RECENTER_COOLDOWN_BARS: int = 2

# Path used only for reading the inactive payload's exit timestamp.
# Writing is always done by the caller (lifecycle.py).
_ACTIVE_GRID_PATH = Path("outputs/active_grid.json")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _bars_since_exit() -> int:
    """
    Read the inactive active_grid.json and estimate bars elapsed since EXIT.
    Returns EXIT_REENTRY_COOLDOWN_BARS (permissive) if the file is missing
    or lacks a timestamp — so a clean install does not block initiation.
    """
    if not _ACTIVE_GRID_PATH.exists():
        return EXIT_REENTRY_COOLDOWN_BARS

    try:
        with _ACTIVE_GRID_PATH.open() as f:
            payload = json.load(f)
    except Exception:
        return EXIT_REENTRY_COOLDOWN_BARS

    # Only inspect the payload if the grid is genuinely inactive.
    if payload.get("active", True):
        return EXIT_REENTRY_COOLDOWN_BARS

    ts_str = payload.get("last_exit_ts") or payload.get("last_updated")
    if not ts_str:
        return EXIT_REENTRY_COOLDOWN_BARS

    try:
        last = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        elapsed_hours = (datetime.now(timezone.utc) - last).total_seconds() / 3600
        return int(elapsed_hours)
    except Exception:
        return EXIT_REENTRY_COOLDOWN_BARS


# ---------------------------------------------------------------------------
# Counter update
# ---------------------------------------------------------------------------

def _update_counters(active_grid: dict, price: float) -> dict:
    """
    Increment temporal counters in a copy of active_grid.

    Position counters (accumulate / reset):
      bars_since_initiation  — always increments; never resets within a grid.
      bars_outside_grid      — increments outside [lower, upper]; resets on re-entry.
      bars_near_edge         — increments when range_pos < 0.15 or > 0.85;
                               resets when price returns to the middle band.

    Action cooldown counters (always increment; reset on the corresponding action):
      bars_since_last_replace  — reset to 0 in new_grid on REPLACE (lifecycle.py).
      bars_since_last_recenter — reset to 0 in apply() when RECENTER is allowed.

    Defaults for new fields use the cooldown constant so that grids created before
    these fields existed are not accidentally blocked by a phantom cooldown.

    Returns a new dict. Does not mutate the input.
    """
    grid = dict(active_grid)

    lower = float(grid["grid_lower"])
    upper = float(grid["grid_upper"])
    width = upper - lower

    grid["bars_since_initiation"] = grid.get("bars_since_initiation", 0) + 1

    if price < lower or price > upper:
        grid["bars_outside_grid"] = grid.get("bars_outside_grid", 0) + 1
    else:
        grid["bars_outside_grid"] = 0

    if width > 0:
        range_pos = (price - lower) / width
        near_edge = range_pos < 0.15 or range_pos > 0.85
    else:
        near_edge = False

    if near_edge:
        grid["bars_near_edge"] = grid.get("bars_near_edge", 0) + 1
    else:
        grid["bars_near_edge"] = 0

    # Action cooldown counters — always increment; resets are external to this function.
    grid["bars_since_last_replace"] = grid.get("bars_since_last_replace", REPLACE_COOLDOWN_BARS) + 1
    grid["bars_since_last_recenter"] = grid.get("bars_since_last_recenter", RECENTER_COOLDOWN_BARS) + 1

    return grid


# ---------------------------------------------------------------------------
# Gating logic — INIT mode
# ---------------------------------------------------------------------------

def _gate_initiation(
    proposed: str,
    proposed_reason: str,
) -> Tuple[str, str]:
    """
    Gate INITIATE with an EXIT reentry cooldown.
    WAIT and BLOCK pass through unchanged.
    """
    if proposed != "INITIATE":
        return proposed, proposed_reason

    bars_since = _bars_since_exit()
    if bars_since < EXIT_REENTRY_COOLDOWN_BARS:
        remaining = EXIT_REENTRY_COOLDOWN_BARS - bars_since
        return (
            "WAIT",
            f"exit_reentry_cooldown_{remaining}_bars_remaining",
        )

    return proposed, proposed_reason


# ---------------------------------------------------------------------------
# Gating logic — MAINTAIN mode
# ---------------------------------------------------------------------------

def _gate_maintenance(
    proposed: str,
    proposed_reason: str,
    grid: dict,  # counter-updated copy
) -> Tuple[str, str]:
    """
    Apply temporal constraints to maintenance-mode action proposals.

    Decision table
    --------------
    HOLD    → always allowed.
    RECENTER→ escalate to REPLACE if bars_near_edge >= RECENTER_ESCALATION_BARS,
              subject to REPLACE cooldown (escalation is a replacement decision).
              Otherwise allow, subject to RECENTER cooldown.
    REPLACE → (1) block to HOLD if bars_alive < MIN_HOLD_BARS.
              (2) block to HOLD if bars_since_last_replace < REPLACE_COOLDOWN_BARS.
    EXIT    → defer to RECENTER if bars_outside_grid < OUTSIDE_ESCALATION_BARS.
    """
    bars_alive = grid.get("bars_since_initiation", 0)
    bars_outside = grid.get("bars_outside_grid", 0)
    bars_near = grid.get("bars_near_edge", 0)
    bars_since_replace = grid.get("bars_since_last_replace", REPLACE_COOLDOWN_BARS)
    bars_since_recenter = grid.get("bars_since_last_recenter", RECENTER_COOLDOWN_BARS)

    if proposed == "HOLD":
        return proposed, proposed_reason

    if proposed == "RECENTER":
        if bars_near >= RECENTER_ESCALATION_BARS:
            # Edge pressure has been sustained: this is now a replacement decision.
            # MIN_HOLD is skipped — edge persistence is the time-based evidence.
            # REPLACE cooldown still applies: escalation does not bypass churn control.
            if bars_since_replace < REPLACE_COOLDOWN_BARS:
                remaining = REPLACE_COOLDOWN_BARS - bars_since_replace
                return (
                    "HOLD",
                    f"escalated_replace_in_cooldown_{remaining}_bars_remaining",
                )
            return (
                "REPLACE",
                f"recenter_escalated_after_{bars_near}_consecutive_edge_bars",
            )

        # Non-escalating RECENTER: suppress repeats within the cooldown window.
        # bars_near_edge still accumulates during HOLD bars so escalation is unaffected.
        if bars_since_recenter < RECENTER_COOLDOWN_BARS:
            remaining = RECENTER_COOLDOWN_BARS - bars_since_recenter
            return (
                "HOLD",
                f"recenter_cooldown_{remaining}_bars_remaining",
            )
        return proposed, proposed_reason

    if proposed == "REPLACE":
        if bars_alive < MIN_HOLD_BARS:
            return (
                "HOLD",
                f"replace_blocked_min_hold_{bars_alive}_of_{MIN_HOLD_BARS}_bars",
            )
        if bars_since_replace < REPLACE_COOLDOWN_BARS:
            remaining = REPLACE_COOLDOWN_BARS - bars_since_replace
            return (
                "HOLD",
                f"replace_cooldown_{remaining}_bars_remaining",
            )
        return proposed, proposed_reason

    if proposed == "EXIT":
        if bars_outside < OUTSIDE_ESCALATION_BARS:
            return (
                "RECENTER",
                f"exit_deferred_{bars_outside}_of_{OUTSIDE_ESCALATION_BARS}_bars_outside",
            )
        return proposed, proposed_reason

    # Unknown action: pass through without modification.
    return proposed, proposed_reason


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def apply(
    proposed_action: str,
    proposed_reason: str,
    active_grid: Optional[dict],
    price: float,
) -> Tuple[str, str, Optional[dict]]:
    """
    Apply control constraints to a proposed lifecycle action.

    Parameters
    ----------
    proposed_action : str
        Action proposed by lifecycle geometry logic.
        INIT mode values:    INITIATE | WAIT | BLOCK
        MAINTAIN mode values: HOLD | RECENTER | REPLACE | EXIT
    proposed_reason : str
        Reason string from lifecycle logic. Preserved or replaced.
    active_grid : dict or None
        Current active_grid state. None means no active grid (INIT mode).
    price : float
        Current market price used to update edge/outside counters.

    Returns
    -------
    allowed_action : str
        The action the caller should execute.
    allowed_reason : str
        Human-readable reason, updated if the action was modified.
    updated_grid : dict or None
        active_grid with temporal counters incremented and control metadata
        stamped. None when there is no active grid.
        The caller must save this dict to active_grid.json.
    """
    if active_grid is None:
        allowed_action, allowed_reason = _gate_initiation(proposed_action, proposed_reason)
        return allowed_action, allowed_reason, None

    updated = _update_counters(active_grid, price)
    allowed_action, allowed_reason = _gate_maintenance(proposed_action, proposed_reason, updated)

    # Reset the RECENTER cooldown counter when RECENTER is actually executed.
    # REPLACE counter reset is handled by lifecycle.py: when REPLACE fires,
    # a new_grid dict is created with bars_since_last_replace=0, and updated_grid
    # is discarded — so resetting it here would have no effect.
    if allowed_action == "RECENTER":
        updated["bars_since_last_recenter"] = 0

    # Stamp what the control layer decided so it's visible in active_grid.json.
    updated["proposed_action"] = proposed_action
    updated["proposed_reason"] = proposed_reason
    updated["allowed_action"] = allowed_action
    updated["allowed_reason"] = allowed_reason
    updated["control_ts"] = _utc_now()

    return allowed_action, allowed_reason, updated

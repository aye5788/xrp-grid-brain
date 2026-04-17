"""
sync_execution_state.py — Refresh execution_state.json from OpenTrader.

Runs once per cycle, before build_opentrader_signal.py.
Queries the live bot state and writes what it finds — including when
OpenTrader is unreachable. build_opentrader_signal.py reads this file
to decide whether actions are safe to send.

Writes execution_state.json with sync_source:
  OPENTRADER  — successfully queried OpenTrader
  UNREACHABLE — OpenTrader did not respond (state preserved from last sync)
  NO_BOT_ID   — OpenTrader reachable but no bot_id on record; cannot verify
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

from opentrader_client import OpenTraderClient, OpenTraderError

BASE_DIR = Path(__file__).parent.parent
EXEC_STATE_PATH  = BASE_DIR / "outputs" / "execution_state.json"
OT_STATE_PATH    = BASE_DIR / "outputs" / "opentrader_state.json"
ACTIVE_GRID_PATH = BASE_DIR / "outputs" / "active_grid.json"

# Tolerance for bounds comparison: 0.5% relative difference.
BOUNDS_TOLERANCE = 0.005


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_json(path: Path) -> dict:
    try:
        with path.open() as f:
            return json.load(f)
    except Exception:
        return {}


def write_state(state: dict) -> None:
    state["last_sync_ts"] = utc_now()
    with EXEC_STATE_PATH.open("w") as f:
        json.dump(state, f, indent=2)
    print(f"  [sync] execution_state.json → sync_source={state['sync_source']}")


def bounds_match(brain_lower, brain_upper, live_lower, live_upper) -> bool:
    """True if live grid bounds are within BOUNDS_TOLERANCE of the brain's intended bounds."""
    if any(v is None for v in [brain_lower, brain_upper, live_lower, live_upper]):
        return False
    lower_ok = abs(brain_lower - live_lower) / brain_lower < BOUNDS_TOLERANCE
    upper_ok = abs(brain_upper - live_upper) / brain_upper < BOUNDS_TOLERANCE
    return lower_ok and upper_ok


def extract_grid_bounds(bot: dict) -> tuple:
    """Pull lower/upper from OpenTrader bot payload. Returns (lower, upper) or (None, None)."""
    try:
        lines = bot.get("settings", {}).get("gridLines", [])
        if not lines:
            return None, None
        prices = [line["price"] for line in lines if "price" in line]
        if not prices:
            return None, None
        return min(prices), max(prices)
    except Exception:
        return None, None


def main():
    # Bootstrap: if opentrader_state.json is absent, write an explicit null record
    # so downstream code sees a clean NO_BOT_ID rather than silently inheriting
    # whatever the last execution_state.json contained.
    if not OT_STATE_PATH.exists():
        with OT_STATE_PATH.open("w") as f:
            json.dump({"active_bot_id": None}, f, indent=2)
        print("  [sync] created opentrader_state.json with null active_bot_id")

    existing = load_json(EXEC_STATE_PATH)
    ot_state = load_json(OT_STATE_PATH)
    active_grid = load_json(ACTIVE_GRID_PATH)

    brain_has_grid = active_grid.get("active", False)
    brain_lower    = active_grid.get("grid_lower")
    brain_upper    = active_grid.get("grid_upper")
    brain_grid_id  = active_grid.get("grid_id")
    bot_id         = ot_state.get("active_bot_id")

    # -----------------------------------------------------------------------
    # Case: OpenTrader bot_id not on record — cannot verify live state.
    # -----------------------------------------------------------------------
    if not bot_id:
        state = {
            **existing,
            "sync_source":      "NO_BOT_ID",
            "brain_grid_id":    brain_grid_id,
            "brain_grid_lower": brain_lower,
            "brain_grid_upper": brain_upper,
            "has_open_grid":    existing.get("has_open_grid", False),
            "bot_enabled":      None,
            "bot_id":           None,
            "live_grid_lower":  existing.get("live_grid_lower"),
            "live_grid_upper":  existing.get("live_grid_upper"),
            "bounds_match":     None,
            "sync_error":       "opentrader_state.json missing or has no active_bot_id",
        }
        write_state(state)
        return

    # -----------------------------------------------------------------------
    # Query OpenTrader for the live bot state.
    # -----------------------------------------------------------------------
    try:
        client = OpenTraderClient()
        bot = client.get_bot(bot_id)

        live_lower, live_upper = extract_grid_bounds(bot)
        bot_enabled = bool(bot.get("enabled", False))
        has_open_grid = bot_enabled

        matched = bounds_match(brain_lower, brain_upper, live_lower, live_upper)

        state = {
            "sync_source":      "OPENTRADER",
            "brain_grid_id":    brain_grid_id,
            "brain_grid_lower": brain_lower,
            "brain_grid_upper": brain_upper,
            "has_open_grid":    has_open_grid,
            "bot_enabled":      bot_enabled,
            "bot_id":           bot_id,
            "live_grid_lower":  live_lower,
            "live_grid_upper":  live_upper,
            "bounds_match":     matched,
            "position_state":   existing.get("position_state", "UNKNOWN"),
            "last_action":      existing.get("last_action"),
            "last_reason":      existing.get("last_reason"),
            "last_signal_ts":   existing.get("last_signal_ts"),
            "sync_error":       None,
        }

        print(f"  [sync] bot_id={bot_id} enabled={bot_enabled} "
              f"live=[{live_lower},{live_upper}] bounds_match={matched}")

    except (OpenTraderError, Exception) as e:
        # OpenTrader unreachable or errored — preserve last known state.
        state = {
            **existing,
            "sync_source":  "UNREACHABLE",
            "sync_error":   str(e)[:200],
            "bounds_match": None,
        }
        print(f"  [sync] OpenTrader unreachable: {e}")

    write_state(state)


if __name__ == "__main__":
    main()

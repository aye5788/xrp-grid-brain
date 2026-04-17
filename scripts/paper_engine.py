#!/usr/bin/env python3
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUTS_DIR = PROJECT_ROOT / "outputs"

DECISION_PATH = OUTPUTS_DIR / "latest_decision.json"
ACTIVE_GRID_PATH = OUTPUTS_DIR / "active_grid.json"
STATE_PATH = OUTPUTS_DIR / "paper_state.json"
EVENTS_PATH = OUTPUTS_DIR / "paper_trade_events.csv"
CLOSED_TRADES_PATH = OUTPUTS_DIR / "paper_closed_trades.csv"
SUMMARY_PATH = OUTPUTS_DIR / "paper_summary_latest.csv"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_outputs_dir() -> None:
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)


def read_json(path: Path) -> Dict[str, Any]:
    with path.open("r") as f:
        return json.load(f)


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    with path.open("w") as f:
        json.dump(payload, f, indent=2)


def append_csv_row(path: Path, row: Dict[str, Any]) -> None:
    df = pd.DataFrame([row])
    if path.exists():
        df.to_csv(path, mode="a", header=False, index=False)
    else:
        df.to_csv(path, index=False)


def load_market_snapshot() -> Dict[str, Any]:
    if not DECISION_PATH.exists():
        raise FileNotFoundError(f"Missing decision file: {DECISION_PATH}")
    return read_json(DECISION_PATH)


def load_active_grid() -> Optional[Dict[str, Any]]:
    if not ACTIVE_GRID_PATH.exists():
        return None

    payload = read_json(ACTIVE_GRID_PATH)
    if not payload.get("active", False):
        return None

    return payload


def resolve_level_count(
    active_grid: Dict[str, Any],
    market_snapshot: Dict[str, Any],
    existing_state: Optional[Dict[str, Any]] = None,
) -> int:
    return int(active_grid.get("levels") or market_snapshot.get("levels") or 6)


def build_grid(lower: float, upper: float, levels: int) -> List[float]:
    spacing = (upper - lower) / (levels - 1)
    return [round(lower + i * spacing, 8) for i in range(levels)]


def load_state() -> Optional[Dict[str, Any]]:
    if not STATE_PATH.exists():
        return None
    return read_json(STATE_PATH)


def save_state(state: Dict[str, Any]) -> None:
    write_json(STATE_PATH, state)


def fee_for_notional(notional: float, fee_pct: float) -> float:
    return round(notional * fee_pct, 10)


def default_state(grid, active_grid):
    return {
        "last_price": None,
        "last_timestamp": None,
        "active_grid_id": active_grid.get("grid_id"),
        "grid_levels": grid,
        "open_positions": {str(level): None for level in grid},
        "totals": {
            "event_count": 0,
            "buy_count": 0,
            "sell_count": 0,
            "closed_trade_count": 0,
            "gross_realized_pnl": 0.0,
            "fees_paid": 0.0,
            "net_realized_pnl": 0.0,
        },
    }


def buys_allowed(snapshot):
    regime = snapshot.get("operational_regime", "").upper()
    tradable = snapshot.get("tradable", False)

    if not tradable:
        return False

    if regime in {"RANGE_TREND_DOWN", "TREND", "NO_TRADE"}:
        return False

    return True


def run():
    ensure_outputs_dir()

    snapshot = load_market_snapshot()
    active_grid = load_active_grid()

    if not active_grid:
        print("No active grid → skipping")
        return

    current_price = float(snapshot["close"])
    high = float(snapshot["high"])
    low = float(snapshot["low"])
    fee_pct = float(snapshot.get("fee_pct", 0.002))
    decision_ts = snapshot.get("timestamp")

    existing_state = load_state()

    levels = resolve_level_count(active_grid, snapshot, existing_state)
    grid = build_grid(
        float(active_grid["grid_lower"]),
        float(active_grid["grid_upper"]),
        levels
    )

    if existing_state is None:
        state = default_state(grid, active_grid)
        state["last_price"] = current_price
        state["last_timestamp"] = decision_ts
        save_state(state)
        print("Initialized state")
        return
    else:
        state = existing_state

    can_buy = buys_allowed(snapshot)

    print(f"Low: {low} | High: {high}")

    for level in grid:
        level_key = str(level)
        position = state["open_positions"].get(level_key)

        crossed_down = low <= level
        crossed_up = high >= level

        # BUY
        if position is None and crossed_down and can_buy:
            qty = 20.0
            notional = qty * level
            fee = fee_for_notional(notional, fee_pct)

            state["open_positions"][level_key] = {
                "entry_price": level,
                "quantity": qty,
                "entry_fee": fee
            }

            state["totals"]["buy_count"] += 1
            state["totals"]["fees_paid"] += fee
            state["totals"]["net_realized_pnl"] -= fee

            print(f"BUY @ {level}")

        # SELL
        elif position is not None and crossed_up:
            qty = position["quantity"]
            entry = position["entry_price"]

            exit_notional = qty * level
            exit_fee = fee_for_notional(exit_notional, fee_pct)

            gross = (level - entry) * qty

            state["open_positions"][level_key] = None

            state["totals"]["sell_count"] += 1
            state["totals"]["closed_trade_count"] += 1
            state["totals"]["gross_realized_pnl"] += gross
            state["totals"]["fees_paid"] += exit_fee
            state["totals"]["net_realized_pnl"] += gross - exit_fee

            print(f"SELL @ {level} | pnl={gross}")

    state["last_price"] = current_price
    state["last_timestamp"] = decision_ts

    save_state(state)

    open_position_count = sum(1 for v in state["open_positions"].values() if v is not None)
    open_inventory_qty = sum(
        v["quantity"] for v in state["open_positions"].values() if v is not None
    )
    unrealized_pnl = sum(
        (current_price - v["entry_price"]) * v["quantity"]
        for v in state["open_positions"].values()
        if v is not None
    )

    summary = {
        "summary_ts": utc_now_iso(),
        "decision_ts": decision_ts,
        "symbol": snapshot.get("symbol", "XRP/USD"),
        "current_price": current_price,
        "active_grid_id": active_grid.get("grid_id"),
        "open_position_count": open_position_count,
        "open_inventory_qty": open_inventory_qty,
        **state["totals"],
        "unrealized_pnl_mark_to_market": round(unrealized_pnl, 8),
        "total_pnl": round(state["totals"]["net_realized_pnl"] + unrealized_pnl, 8),
        "operational_regime": snapshot.get("operational_regime"),
        "active_grid_regime": active_grid.get("operational_regime"),
        "risk_mode": snapshot.get("risk_mode"),
        "grid_lower": active_grid.get("grid_lower"),
        "grid_upper": active_grid.get("grid_upper"),
        "levels": levels,
        "spacing_pct": snapshot.get("spacing_pct"),
        "fee_pct": fee_pct,
    }

    pd.DataFrame([summary]).to_csv(SUMMARY_PATH, index=False)
    print(f"Paper summary updated → {SUMMARY_PATH} (1 rows)")


if __name__ == "__main__":
    run()

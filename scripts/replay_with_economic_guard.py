#!/usr/bin/env python3
"""
Replay with economic guard
"""

import sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from src.pipelines.run_brain import run_pipeline
from src.grid.candidate_builder import build_grid_variants
from src.control.control_layer import apply as control_apply
from src.control.economic_guard import check_economic_override
from lifecycle import determine_initiation_action, determine_maintenance_action

CSV_PATH = ROOT / "data" / "raw" / "xrp_full_hourly_clean.csv"
OUTPUT_PATH = ROOT / "outputs" / "replay_with_economic_guard.csv"

WARMUP_BARS = 200
FEE_PCT = 0.002
QTY_PER_LEVEL = 20.0


def best_candidate(row):
    try:
        variants = build_grid_variants(row)
    except Exception:
        return None
    if not variants:
        return None
    return max(variants, key=lambda v: v.get("candidate_score", -999))


def new_active_grid(candidate, idx):
    return {
        "grid_id": f"replay_{idx}",
        "active": True,
        "grid_lower": float(candidate["grid_lower"]),
        "grid_upper": float(candidate["grid_upper"]),
        "center_price": float(candidate["center_price"]),
        "candidate_score": float(candidate["candidate_score"]),  # PRESERVED
        "levels": int(candidate.get("levels", 6)),
        "bars_since_initiation": 0,
        "bars_since_last_replace": 999,
        "bars_since_last_recenter": 999,
    }


def init_paper(active_grid):
    lower = float(active_grid["grid_lower"])
    upper = float(active_grid["grid_upper"])
    levels = int(active_grid.get("levels", 6))
    spacing = (upper - lower) / max(levels - 1, 1)

    grid_levels = [round(lower + i * spacing, 8) for i in range(levels)]

    return {
        "grid_levels": grid_levels,
        "open_positions": {str(lv): None for lv in grid_levels},
        "closed_trade_count": 0,
        "gross_realized_pnl": 0.0,
        "fees_paid": 0.0,
        "net_realized_pnl": 0.0,
    }


def simulate_fills(paper, low, high):
    for level in paper["grid_levels"]:
        key = str(level)
        pos = paper["open_positions"][key]

        if pos is None and low <= level:
            qty = QTY_PER_LEVEL
            fee = qty * level * FEE_PCT
            paper["open_positions"][key] = {
                "entry_price": level,
                "quantity": qty,
            }
            paper["fees_paid"] += fee
            paper["net_realized_pnl"] -= fee

        elif pos is not None and high >= level:
            qty = pos["quantity"]
            gross = (level - pos["entry_price"]) * qty
            fee = qty * level * FEE_PCT

            paper["open_positions"][key] = None
            paper["closed_trade_count"] += 1
            paper["gross_realized_pnl"] += gross
            paper["fees_paid"] += fee
            paper["net_realized_pnl"] += gross - fee

    return paper


def paper_to_summary(paper):
    return {
        "closed_trade_count": paper["closed_trade_count"],
        "gross_realized_pnl": paper["gross_realized_pnl"],
        "fees_paid": paper["fees_paid"],
        "net_realized_pnl": paper["net_realized_pnl"],
    }


def record_grid(entry_ts, exit_ts, action, reason, eco_applied, paper, price, entry_bar, exit_bar):
    return {
        "entry_ts": entry_ts,
        "exit_ts": exit_ts,
        "exit_action": action,
        "exit_reason": reason,
        "exit_type": "ECONOMIC" if eco_applied else "STRUCTURAL",
        "pnl": paper["net_realized_pnl"],
        "duration_bars": exit_bar - entry_bar,
        "trade_count": paper["closed_trade_count"],
    }


def main():
    df = run_pipeline(str(CSV_PATH))
    df = df.sort_values("timestamp").reset_index(drop=True)

    rows = df.iloc[WARMUP_BARS:].reset_index(drop=True)

    results = []
    active_grid = None
    paper = None
    entry_ts = None
    entry_bar = None

    for idx, row in rows.iterrows():
        price = float(row["close"])
        low = float(row["low"])
        high = float(row["high"])
        ts = str(row["timestamp"])

        if active_grid is None:
            candidate = best_candidate(row)
            if candidate is None:
                continue

            action, _ = determine_initiation_action(candidate, price)
            if action != "INITIATE":
                continue

            active_grid = new_active_grid(candidate, idx)
            paper = init_paper(active_grid)
            entry_ts = ts
            entry_bar = idx
            continue

        paper = simulate_fills(paper, low, high)

        candidate = best_candidate(row)
        if candidate is None:
            _, _, updated = control_apply("HOLD", "no_candidate", active_grid, price)
            if updated:
                active_grid = updated
            continue

        proposed_action, proposed_reason = determine_maintenance_action(
            active_grid, candidate, price
        )

        action, reason, updated_grid = control_apply(
            proposed_action, proposed_reason, active_grid, price
        )

        # ✅ ONLY CHANGE: price passed + indentation fixed
        eco_override = check_economic_override(
            action,
            updated_grid,
            paper_to_summary(paper),
            price
        )

        eco_applied = eco_override is not None
        if eco_applied:
            action, reason = eco_override

        if action == "EXIT":
            results.append(record_grid(
                entry_ts, ts, action, reason, eco_applied,
                paper, price, entry_bar, idx
            ))
            active_grid = None
            paper = None

        elif action == "REPLACE":
            results.append(record_grid(
                entry_ts, ts, action, reason, eco_applied,
                paper, price, entry_bar, idx
            ))
            active_grid = new_active_grid(candidate, idx)
            paper = init_paper(active_grid)
            entry_ts = ts
            entry_bar = idx

        else:
            active_grid = updated_grid

    out_df = pd.DataFrame(results)
    out_df.to_csv(OUTPUT_PATH, index=False)
    print(out_df)


if __name__ == "__main__":
    main()

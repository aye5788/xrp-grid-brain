"""
backfill_lifecycle_history.py

Reconstructs lifecycle_action / lifecycle_mode / lifecycle_reason for historical
rows in outputs/decision_history.csv that were written before _stamp_lifecycle_to_history
was wired into lifecycle.py.

SAFE GUARDS:
- Only writes to rows where lifecycle_action IS NULL.
- Does not touch rows that already have a value.
- Writes to a temp file first, verifies row count, then replaces in-place.
- All backfilled reasons carry a "_backfilled" suffix so Metabase can distinguish
  reconstructed data from live data.

Run:
    python scripts/backfill_lifecycle_history.py
    python scripts/backfill_lifecycle_history.py --dry-run   # print only, no write
"""

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
DECISION_HISTORY_PATH = BASE_DIR / "outputs" / "decision_history.csv"
PRICE_PATH            = BASE_DIR / "data" / "raw" / "xrp_full_hourly_clean.csv"
ADAPTIVE_PARAMS_PATH  = BASE_DIR / "outputs" / "adaptive_params.json"
ACTIVE_GRID_PATH      = BASE_DIR / "outputs" / "active_grid.json"

# ── Known grid facts (from active_grid.json, grid_id xrp_20260408_181017) ────
# The current grid was never replaced (bars_since_last_replace=73, started at 6).
# Its bounds are used for all MAINTAIN-phase rows.
GRID_INIT_TS  = pd.Timestamp("2026-04-08 18:10:17", tz="UTC")
GRID_LOWER    = 1.3025699597160003
GRID_UPPER    = 1.447116160284
GRID_CENTER   = 1.37484306
GRID_SCORE    = 21.84199761595654

# ── Constants matching control_layer.py ──────────────────────────────────────
RECENTER_ESCALATION_BARS = 3
RECENTER_COOLDOWN_BARS   = 2


# ── Helpers ──────────────────────────────────────────────────────────────────

def load_score_threshold() -> float:
    try:
        with open(ADAPTIVE_PARAMS_PATH) as f:
            return float(json.load(f).get("initiation_score_threshold", 12.0))
    except Exception:
        return 12.0


def build_price_lookup() -> dict:
    """Return {floor-hour Timestamp → close price}."""
    df = pd.read_csv(PRICE_PATH)
    col = "date" if "date" in df.columns else "timestamp"
    df["ts"] = pd.to_datetime(df[col], utc=True).dt.floor("h")
    return dict(zip(df["ts"], df["close"].astype(float)))


# ── INIT-phase logic (mirrors lifecycle.py determine_initiation_action) ───────

def init_action(row, price: float, threshold: float) -> tuple[str, str]:
    score  = float(row["candidate_score"])
    regime = row["operational_regime"]
    lower  = float(row["grid_lower"])
    upper  = float(row["grid_upper"])
    center = float(row["center_price"])

    if not (lower < upper):
        return "BLOCK", "invalid_candidate_geometry_backfilled"

    if score < threshold:
        return "WAIT", f"candidate_score_below_threshold_{threshold}_backfilled"

    if price < lower * 0.995 or price > upper * 1.005:
        return "WAIT", "price_already_far_from_candidate_structure_backfilled"

    dist = abs(price - center) / center

    if regime == "RANGE_GOOD" and score >= 8 and dist <= 0.01:
        return "INITIATE", "strong_range_candidate_backfilled"

    if regime == "RANGE_TREND_UP" and score >= 9 and dist <= 0.008:
        return "INITIATE", "high_quality_range_trend_up_candidate_backfilled"

    return "WAIT", "context_not_strong_enough_to_initiate_backfilled"


# ── MAINTAIN-phase logic ──────────────────────────────────────────────────────

def maintain_proposed(price: float, candidate_score: float) -> tuple[str, str]:
    """Mirrors lifecycle.py determine_maintenance_action with known grid bounds."""
    lower  = GRID_LOWER
    upper  = GRID_UPPER
    center = GRID_CENTER

    range_pos = (price - lower) / (upper - lower)

    if lower <= price <= upper:
        if 0.30 <= range_pos <= 0.70:
            return "HOLD", "well_centered_backfilled"
        if 0.20 <= range_pos < 0.30 or 0.70 < range_pos <= 0.80:
            return "HOLD", "mild_drift_backfilled"
        if 0.10 <= range_pos < 0.20 or 0.80 < range_pos <= 0.90:
            return "RECENTER", "approaching_edge_backfilled"
        # Extreme edge
        if range_pos < 0.10 or range_pos > 0.90:
            if candidate_score >= GRID_SCORE:
                return "REPLACE", "edge_new_candidate_backfilled"
            return "RECENTER", "edge_recenter_backfilled"
        return "HOLD", "inside_grid_backfilled"

    esc = abs(price - center) / center
    if esc <= 0.015:
        return "RECENTER", "mild_escape_backfilled"
    if esc <= 0.025:
        if candidate_score >= GRID_SCORE:
            return "REPLACE", "moderate_escape_new_candidate_backfilled"
        return "RECENTER", "moderate_escape_backfilled"
    if candidate_score >= GRID_SCORE + 1.0:
        return "REPLACE", "strong_escape_better_candidate_backfilled"
    return "EXIT", "strong_breakout_backfilled"


def gate_maintain(proposed: str, reason: str, bars_near_edge: int,
                  bars_since_recenter: int, bars_since_replace: int,
                  bars_alive: int) -> tuple[str, str]:
    """
    Mirrors control_layer._gate_maintenance.
    Inputs are the counter values AFTER _update_counters increments them.
    """
    if proposed == "HOLD":
        return proposed, reason

    if proposed == "RECENTER":
        if bars_near_edge >= RECENTER_ESCALATION_BARS:
            if bars_since_replace < 6:  # REPLACE_COOLDOWN_BARS
                return "HOLD", f"escalated_replace_in_cooldown_backfilled"
            return "REPLACE", f"recenter_escalated_after_{bars_near_edge}_edge_bars_backfilled"
        if bars_since_recenter < RECENTER_COOLDOWN_BARS:
            return "HOLD", f"recenter_cooldown_{RECENTER_COOLDOWN_BARS - bars_since_recenter}_bars_remaining_backfilled"
        return proposed, reason

    if proposed == "REPLACE":
        if bars_alive < 4:  # MIN_HOLD_BARS
            return "HOLD", "replace_blocked_min_hold_backfilled"
        if bars_since_replace < 6:
            return "HOLD", "replace_cooldown_backfilled"
        return proposed, reason

    if proposed == "EXIT":
        return proposed, reason  # outside escalation unknown; preserve EXIT

    return proposed, reason


# ── Main backfill ─────────────────────────────────────────────────────────────

def run(dry_run: bool = False) -> None:
    history      = pd.read_csv(DECISION_HISTORY_PATH)
    price_lookup = build_price_lookup()
    threshold    = load_score_threshold()
    original_len = len(history)

    # Identify rows needing backfill
    null_mask = history["lifecycle_action"].isna()
    print(f"Rows needing backfill: {null_mask.sum()} / {original_len}")

    # ── Simulate MAINTAIN-phase counter state ─────────────────────────────────
    # Walk every row in timestamp order. For MAINTAIN rows, track the three
    # counters that affect gating. Because lifecycle.py runs more frequently than
    # decision_history rows appear (bars_since_initiation=68 vs 19 MAINTAIN rows),
    # we simulate only over the rows we have. Counter values are conservative
    # lower bounds (actual elapsed bars are higher, so cooldowns are MORE likely
    # to be expired, not less).
    bars_since_recenter = RECENTER_COOLDOWN_BARS  # starts elapsed (never blocked)
    bars_since_replace  = 6                        # starts elapsed (REPLACE_COOLDOWN_BARS)
    bars_alive          = 0
    bars_near_edge_seq  = 0

    history["timestamp_parsed"] = pd.to_datetime(history["timestamp"], utc=True)
    history = history.sort_values("timestamp_parsed").reset_index(drop=True)

    updates: list[dict] = []

    for idx, row in history.iterrows():
        ts = row["timestamp_parsed"]

        # Skip already-stamped rows but still update counters for MAINTAIN rows
        already_stamped = pd.notna(row["lifecycle_action"])

        ts_floor = ts.floor("h")
        price = price_lookup.get(ts_floor)

        if ts < GRID_INIT_TS:
            # ── INIT phase ───────────────────────────────────────────────────
            if already_stamped:
                continue

            if price is None:
                print(f"  WARN: no price for {ts} — skipping row {idx}")
                continue

            action, reason = init_action(row, float(price), threshold)
            updates.append({
                "idx":    idx,
                "action": action,
                "mode":   "INIT",
                "reason": reason,
            })

        else:
            # ── MAINTAIN phase ───────────────────────────────────────────────
            # Increment counters regardless of stamped status (to keep state consistent)
            bars_alive += 1
            bars_since_recenter += 1
            bars_since_replace  += 1

            if price is not None:
                p = float(price)
                range_pos  = (p - GRID_LOWER) / (GRID_UPPER - GRID_LOWER)
                is_near    = range_pos < 0.15 or range_pos > 0.85
                bars_near_edge_seq = (bars_near_edge_seq + 1) if is_near else 0

            if already_stamped:
                # If the action was RECENTER, reset its cooldown counter
                if row["lifecycle_action"] == "RECENTER":
                    bars_since_recenter = 0
                continue

            if price is None:
                print(f"  WARN: no price for {ts} — skipping row {idx}")
                continue

            p = float(price)
            candidate_score = float(row["candidate_score"])
            proposed, prop_reason = maintain_proposed(p, candidate_score)

            action, reason = gate_maintain(
                proposed, prop_reason,
                bars_near_edge=bars_near_edge_seq,
                bars_since_recenter=bars_since_recenter,
                bars_since_replace=bars_since_replace,
                bars_alive=bars_alive,
            )

            if action == "RECENTER":
                bars_since_recenter = 0

            updates.append({
                "idx":    idx,
                "action": action,
                "mode":   "MAINTAIN",
                "reason": reason,
            })

    # ── Apply updates ─────────────────────────────────────────────────────────
    print(f"\nBackfill plan ({len(updates)} rows):")
    action_counts: dict[str, int] = {}
    for u in updates:
        action_counts[u["action"]] = action_counts.get(u["action"], 0) + 1
    for k, v in sorted(action_counts.items()):
        print(f"  {k}: {v}")

    if dry_run:
        print("\n[dry-run] No changes written.")
        print("\nSample rows:")
        for u in updates[:5]:
            print(f"  row {u['idx']:3d}  {u['mode']:8s}  {u['action']:10s}  {u['reason']}")
        return

    for u in updates:
        history.at[u["idx"], "lifecycle_action"] = u["action"]
        history.at[u["idx"], "lifecycle_mode"]   = u["mode"]
        history.at[u["idx"], "lifecycle_reason"]  = u["reason"]

    # Drop the helper column before writing
    history = history.drop(columns=["timestamp_parsed"])

    # Verify row count has not changed
    assert len(history) == original_len, (
        f"Row count changed: {original_len} → {len(history)}"
    )

    # Verify no previously-stamped lifecycle_action was overwritten
    original_df = pd.read_csv(DECISION_HISTORY_PATH)
    was_set = original_df["lifecycle_action"].notna()
    if was_set.any():
        orig_vals   = original_df.loc[was_set, "lifecycle_action"]
        new_vals    = history.loc[was_set, "lifecycle_action"]
        mismatches  = (orig_vals.values != new_vals.values)
        if mismatches.any():
            raise AssertionError(
                "BUG: pre-existing lifecycle_action rows were overwritten. Aborting."
            )

    # Write atomically via temp file
    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", dir=DECISION_HISTORY_PATH.parent,
        delete=False
    )
    try:
        history.to_csv(tmp.name, index=False)
        tmp.close()
        os.replace(tmp.name, DECISION_HISTORY_PATH)
    except Exception:
        os.unlink(tmp.name)
        raise

    remaining_null = history["lifecycle_action"].isna().sum()
    print(f"\nDone. Remaining NULL lifecycle_action: {remaining_null}")
    if remaining_null > 0:
        print("Rows still NULL (no matching price bar):")
        print(history[history["lifecycle_action"].isna()][["timestamp", "lifecycle_action"]].to_string())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Print plan without writing")
    args = parser.parse_args()
    run(dry_run=args.dry_run)

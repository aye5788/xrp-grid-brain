"""
build_opentrader_signal.py — Map lifecycle action to an execution signal.

Reads:
  lifecycle_decision.json  — what the brain wants to do
  execution_state.json     — current live state (written by sync_execution_state.py)
  active_grid.json         — brain's intended grid state

Writes:
  opentrader_signal.json   — { adapter_action, adapter_reason, lifecycle_action }

Reconciliation rules
--------------------
The reconciliation compares brain state (active_grid.json) against live state
(execution_state.json, refreshed by sync_execution_state.py this cycle).

sync_source == LIVE required for destructive actions (REPLACE, EXIT).
sync_source == UNREACHABLE → HOLD only; all other actions blocked.
sync_source == STALE (> STALE_MAX_MINUTES) → HOLD only.
sync_source == NO_BOT_ID → allow INITIATE (first deploy); block REPLACE/EXIT.

Mismatch types:
  OK              — brain and execution agree; all actions allowed.
  BOUNDS_MISMATCH — live grid does not match brain grid; HOLD only.
  EXECUTION_FLAT  — brain has active grid, execution does not; allow INITIATE.
  ORPHAN_BOT      — execution has active bot, brain has no grid; block until EXIT sent.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent

LIFECYCLE_PATH        = BASE_DIR / "outputs" / "lifecycle_decision.json"
ACTIVE_GRID_PATH      = BASE_DIR / "outputs" / "active_grid.json"
EXEC_STATE_PATH       = BASE_DIR / "outputs" / "execution_state.json"
LATEST_DECISION_PATH  = BASE_DIR / "outputs" / "latest_decision.json"
OUTPUT_JSON           = BASE_DIR / "outputs" / "opentrader_signal.json"
OUTPUT_CSV            = BASE_DIR / "outputs" / "opentrader_signal.csv"

GRID_REQUIRED_ACTIONS = {"DEPLOY_OR_MAINTAIN_GRID", "REPLACE_GRID", "RECENTER_GRID"}

# Execution state older than this is treated as STALE (one missed cycle + buffer).
STALE_MAX_MINUTES = 90


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_json(path: Path) -> dict:
    try:
        with path.open() as f:
            return json.load(f)
    except Exception:
        return {}


def is_stale(exec_state: dict) -> bool:
    try:
        ts = datetime.fromisoformat(
            exec_state["last_sync_ts"].replace("Z", "+00:00")
        )
        elapsed = (datetime.now(timezone.utc) - ts).total_seconds() / 60
        return elapsed > STALE_MAX_MINUTES
    except Exception:
        return True


def build_grid_payload(active_grid: dict, latest_decision: dict) -> dict | None:
    try:
        use_active = active_grid and active_grid.get("active") is True

        if use_active and "grid_lower" in active_grid:
            grid_lower = active_grid["grid_lower"]
        elif "grid_lower" in latest_decision:
            grid_lower = latest_decision["grid_lower"]
        else:
            return None

        spacing  = latest_decision.get("spacing")
        levels   = latest_decision.get("levels")
        symbol   = latest_decision.get("symbol", "XRP/USD")

        if spacing is None or levels is None:
            return None

        return {
            "symbol":     symbol,
            "grid_lower": grid_lower,
            "spacing":    spacing,
            "levels":     int(levels),
        }
    except Exception:
        return None


def write_signal(adapter_action: str, adapter_reason: str, lifecycle_action: str,
                 grid_payload: dict = None) -> None:
    output = {
        "adapter_action":   adapter_action,
        "adapter_reason":   adapter_reason,
        "lifecycle_action": lifecycle_action,
    }
    if grid_payload is not None:
        output["grid"] = grid_payload

    with OUTPUT_JSON.open("w") as f:
        json.dump(output, f, indent=2)

    try:
        import pandas as pd
        pd.DataFrame([output]).to_csv(OUTPUT_CSV, index=False)
    except Exception:
        pass

    print(f"OpenTrader signal → {adapter_action} ({adapter_reason})")


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------

def reconcile(lifecycle_action: str, active_grid: dict, exec_state: dict) -> tuple:
    """
    Returns (adapter_action, adapter_reason).

    All destructive actions (REPLACE, EXIT, INITIATE) require verified live state.
    Default is NO_ACTION when state is uncertain.
    """
    # HOLD is always passive — never needs live state verification.
    if lifecycle_action == "HOLD":
        return "NO_ACTION", "lifecycle_hold_passive"

    sync_source = exec_state.get("sync_source", "NO_BOT_ID")

    # Safety gate 1: stale execution state — live state unknown, block everything.
    if is_stale(exec_state):
        return "NO_ACTION", f"blocked_stale_exec_state_source={sync_source}"

    # Safety gate 2: OpenTrader unreachable last cycle — cannot verify live state.
    if sync_source == "UNREACHABLE":
        return "NO_ACTION", "blocked_opentrader_unreachable"

    brain_active = active_grid.get("active", False)
    exec_active  = exec_state.get("has_open_grid", False)

    # Safety gate 3: orphan bot — execution has a live grid but brain has no record.
    # Emitting any action here risks operating on a grid the brain didn't place.
    if exec_active and not brain_active:
        return "NO_ACTION", "blocked_orphan_bot_manual_review_required"

    # Safety gate 4: NO_BOT_ID — bot identity unverified; only INITIATE is safe
    # (first deploy scenario). Block all destructive actions.
    if sync_source == "NO_BOT_ID":
        if lifecycle_action not in ("INITIATE", "WAIT", "BLOCK"):
            return "NO_ACTION", f"blocked_no_bot_id_destructive_action={lifecycle_action}"

    # Safety gate 5: bounds mismatch — live grid does not match what brain expects.
    # Only EXIT is permitted so we can attempt to clean up the unknown grid state.
    if exec_state.get("bounds_match") is False:
        if lifecycle_action != "EXIT":
            return "NO_ACTION", "blocked_bounds_mismatch_only_exit_permitted"

    # All gates passed — map lifecycle action to adapter action.
    if lifecycle_action == "INITIATE":
        return "DEPLOY_OR_MAINTAIN_GRID", "lifecycle_initiate"

    if lifecycle_action in ("REPLACE", "RECENTER"):
        return lifecycle_action + "_GRID", "lifecycle_trigger"

    if lifecycle_action == "EXIT":
        return "EXIT_GRID", "lifecycle_exit"

    return "NO_ACTION", f"unhandled_lifecycle_action_{lifecycle_action}"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    lifecycle       = load_json(LIFECYCLE_PATH)
    active_grid     = load_json(ACTIVE_GRID_PATH)
    exec_state      = load_json(EXEC_STATE_PATH)
    latest_decision = load_json(LATEST_DECISION_PATH)

    if not lifecycle:
        raise ValueError("Missing lifecycle_decision.json")

    lifecycle_action = lifecycle.get("lifecycle_action", "UNKNOWN")
    tradable         = lifecycle.get("tradable", True)

    # Non-tradable overrides everything.
    if not tradable:
        write_signal("NO_ACTION", "not_tradable", lifecycle_action)
        return

    adapter_action, adapter_reason = reconcile(lifecycle_action, active_grid, exec_state)

    grid_payload = None
    if adapter_action in GRID_REQUIRED_ACTIONS:
        grid_payload = build_grid_payload(active_grid, latest_decision)
        if grid_payload is None:
            write_signal(
                "NO_ACTION",
                f"blocked_missing_grid_geometry_for_{adapter_action}",
                lifecycle_action,
            )
            return

    write_signal(adapter_action, adapter_reason, lifecycle_action, grid_payload)


if __name__ == "__main__":
    main()

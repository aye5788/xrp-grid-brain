import json
import os
from datetime import datetime, timezone


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

LIFECYCLE_PATH = os.path.join(BASE_DIR, "outputs", "lifecycle_decision.json")
ACTIVE_GRID_PATH = os.path.join(BASE_DIR, "outputs", "active_grid.json")
EXEC_STATE_PATH = os.path.join(BASE_DIR, "outputs", "execution_state.json")

OUTPUT_JSON = os.path.join(BASE_DIR, "outputs", "opentrader_signal.json")
OUTPUT_CSV = os.path.join(BASE_DIR, "outputs", "opentrader_signal.csv")


STALE_SECONDS = 300  # 5 minutes


def load_json(path):
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return None


def is_state_stale(exec_state):
    try:
        ts = datetime.fromisoformat(exec_state["last_sync_ts"].replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        return (now - ts).total_seconds() > STALE_SECONDS
    except Exception:
        return True


def state_conflicts(exec_state, local_state):
    try:
        if not local_state:
            return False

        lower_local = local_state.get("grid_lower")
        upper_local = local_state.get("grid_upper")

        lower_live = exec_state.get("live_grid_lower")
        upper_live = exec_state.get("live_grid_upper")

        if lower_local is None or upper_local is None:
            return False

        return (
            abs(lower_local - lower_live) > 1e-6 or
            abs(upper_local - upper_live) > 1e-6
        )
    except Exception:
        return True


def main():
    lifecycle = load_json(LIFECYCLE_PATH)
    active_grid = load_json(ACTIVE_GRID_PATH)
    exec_state = load_json(EXEC_STATE_PATH)

    if not lifecycle:
        raise ValueError("Missing lifecycle_decision.json")

    lifecycle_action = lifecycle.get("lifecycle_action", "UNKNOWN")
    tradable = lifecycle.get("tradable", True)

    # -----------------------------------
    # DEFAULT ACTION
    # -----------------------------------
    adapter_action = "DEPLOY_OR_MAINTAIN_GRID"
    adapter_reason = "tradable_and_lifecycle_hold"

    # -----------------------------------
    # APPLY LIFECYCLE LOGIC
    # -----------------------------------
    if not tradable:
        adapter_action = "NO_ACTION"
        adapter_reason = "not_tradable"

    elif lifecycle_action in ["REPLACE", "RECENTER", "EXIT"]:
        adapter_action = lifecycle_action + "_GRID"
        adapter_reason = "lifecycle_trigger"

    # -----------------------------------
    # EXECUTION RECONCILIATION GUARD
    # -----------------------------------
    if exec_state:
        if is_state_stale(exec_state):
            adapter_action = "NO_ACTION"
            adapter_reason = "STATE_UNCERTAIN_STALE"

        elif state_conflicts(exec_state, active_grid):
            adapter_action = "NO_ACTION"
            adapter_reason = "STATE_UNCERTAIN_CONFLICT"

    # -----------------------------------
    # OUTPUT
    # -----------------------------------
    output = {
        "adapter_action": adapter_action,
        "adapter_reason": adapter_reason,
        "lifecycle_action": lifecycle_action
    }

    with open(OUTPUT_JSON, "w") as f:
        json.dump(output, f, indent=2)

    print(f"OpenTrader signal → {adapter_action} ({adapter_reason})")


if __name__ == "__main__":
    main()

"""
push_to_opentrader.py — Act on lifecycle decisions via OpenTrader start/stop.

Reads:
  outputs/opentrader_signal.json  — lifecycle_action, adapter_action, adapter_reason
  outputs/execution_state.json    — bot_id, bot_enabled (current live state)

Behavior
--------
  INITIATE → start_bot(bot_id)  if bot is not already enabled
  EXIT     → stop_bot(bot_id)   if bot is not already stopped
  HOLD     → no-op
  other    → no-op, logged

Idempotency
-----------
  bot_enabled from execution_state.json is read before every call.
  If the bot is already in the desired state the API call is skipped.
  OpenTrader also enforces idempotency server-side (CONFLICT on double start/stop),
  which is caught and treated as a no-op rather than an error.

This script does NOT create new bots.  Bot identity is managed by
bootstrap_opentrader.py and persisted in outputs/opentrader_state.json.
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

from opentrader_client import OpenTraderClient, OpenTraderError

BASE_DIR        = Path(__file__).parent.parent
SIGNAL_PATH     = BASE_DIR / "outputs" / "opentrader_signal.json"
EXEC_STATE_PATH = BASE_DIR / "outputs" / "execution_state.json"


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_json(path: Path) -> dict:
    try:
        with path.open() as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception as e:
        print(f"  [push] WARNING: could not read {path.name}: {e}")
        return {}


def main():
    print(f"\n[push_to_opentrader] {utc_now()}")

    # ------------------------------------------------------------------
    # Load signal
    # ------------------------------------------------------------------
    signal = load_json(SIGNAL_PATH)
    if not signal:
        print("  [push] no signal file — skipping")
        sys.exit(0)

    lifecycle_action = signal.get("lifecycle_action", "UNKNOWN")
    adapter_action   = signal.get("adapter_action", "NO_ACTION")
    adapter_reason   = signal.get("adapter_reason", "")

    print(f"  [push] lifecycle_action={lifecycle_action}  "
          f"adapter_action={adapter_action}  reason={adapter_reason}")

    # ------------------------------------------------------------------
    # Short-circuit: HOLD and any unrecognised lifecycle actions
    # ------------------------------------------------------------------
    if lifecycle_action == "HOLD":
        print("  [push] HOLD — no action")
        sys.exit(0)

    if lifecycle_action not in ("INITIATE", "EXIT"):
        print(f"  [push] lifecycle_action={lifecycle_action} not handled — no action")
        sys.exit(0)

    # ------------------------------------------------------------------
    # Load execution state for bot_id and current enabled flag
    # ------------------------------------------------------------------
    exec_state = load_json(EXEC_STATE_PATH)
    bot_id     = exec_state.get("bot_id")
    bot_enabled = exec_state.get("bot_enabled")  # True / False / None

    if not bot_id:
        print("  [push] ERROR: no bot_id in execution_state.json — "
              "run bootstrap_opentrader.py first")
        sys.exit(1)

    # ------------------------------------------------------------------
    # Warn if reconciliation blocked the action (informational only)
    # ------------------------------------------------------------------
    if adapter_action == "NO_ACTION" and lifecycle_action in ("INITIATE", "EXIT"):
        print(f"  [push] NOTE: reconciliation blocked this action "
              f"(adapter_action=NO_ACTION, reason={adapter_reason}). "
              f"Proceeding with lifecycle_action={lifecycle_action} anyway.")

    # ------------------------------------------------------------------
    # Connect
    # ------------------------------------------------------------------
    client = OpenTraderClient()

    # ------------------------------------------------------------------
    # INITIATE → start bot
    # ------------------------------------------------------------------
    if lifecycle_action == "INITIATE":
        if bot_enabled is True:
            print(f"  [push] bot_id={bot_id} already enabled — no-op (idempotent)")
            sys.exit(0)

        print(f"  [push] INITIATE → starting bot_id={bot_id}")
        try:
            result = client.start_bot(bot_id)
            print(f"  [push] start_bot({bot_id}) → {result}")
        except OpenTraderError as e:
            if e.code == "CONFLICT":
                # Bot already running server-side — treat as success
                print(f"  [push] bot_id={bot_id} already running (CONFLICT) — no-op")
            else:
                print(f"  [push] ERROR starting bot_id={bot_id}: {e}")
                sys.exit(1)

    # ------------------------------------------------------------------
    # EXIT → stop bot
    # ------------------------------------------------------------------
    elif lifecycle_action == "EXIT":
        if bot_enabled is False:
            print(f"  [push] bot_id={bot_id} already stopped — no-op (idempotent)")
            sys.exit(0)

        print(f"  [push] EXIT → stopping bot_id={bot_id}")
        try:
            result = client.stop_bot(bot_id)
            print(f"  [push] stop_bot({bot_id}) → {result}")
        except OpenTraderError as e:
            if e.code == "CONFLICT":
                # Bot already stopped server-side — treat as success
                print(f"  [push] bot_id={bot_id} already stopped (CONFLICT) — no-op")
            else:
                print(f"  [push] ERROR stopping bot_id={bot_id}: {e}")
                sys.exit(1)

    print(f"  [push] done")


if __name__ == "__main__":
    main()

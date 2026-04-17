"""
bootstrap_opentrader.py — One-time setup: create paper exchange account + grid bot,
persist bot_id to outputs/opentrader_state.json.

Safe to re-run: if a valid bot_id is already recorded and the bot is reachable,
the script exits without creating duplicates.

Reads:
  outputs/latest_decision.json   — grid geometry (grid_lower, grid_upper, levels)
  outputs/opentrader_state.json  — existing bot_id (if any)

Writes:
  outputs/opentrader_state.json  — {"active_bot_id": <int>}

Prerequisites:
  OpenTrader must be running at localhost:4000.
  Run: cd /root/projects/opentrader-grid/opentrader && node app/dist/standalone.mjs &
  Then seed the database (first time only):
       node /root/projects/opentrader-grid/opentrader/packages/prisma/seed.mjs
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

from opentrader_client import OpenTraderClient, OpenTraderError

BASE_DIR = Path(__file__).parent.parent
OT_STATE_PATH     = BASE_DIR / "outputs" / "opentrader_state.json"
DECISION_PATH     = BASE_DIR / "outputs" / "latest_decision.json"

PAPER_ACCOUNT_NAME = "xrp-paper"
BOT_NAME           = "xrp-brain-paper"
SYMBOL             = "XRP/USDT"   # OpenTrader symbol; paper mode — no real orders placed
QUANTITY_PER_LEVEL = 20.0         # XRP per grid level


def load_json(path: Path) -> dict:
    try:
        with path.open() as f:
            return json.load(f)
    except Exception:
        return {}


def write_ot_state(bot_id: int) -> None:
    payload = {"active_bot_id": bot_id}
    with OT_STATE_PATH.open("w") as f:
        json.dump(payload, f, indent=2)
    print(f"  [bootstrap] wrote opentrader_state.json → active_bot_id={bot_id}")


def build_grid_lines(decision: dict) -> list:
    """Reconstruct grid price levels from brain decision geometry."""
    grid_lower = float(decision["grid_lower"])
    grid_upper = float(decision["grid_upper"])
    levels     = int(decision.get("levels", 6))

    if levels < 2:
        raise ValueError(f"levels={levels} is too small; need at least 2")

    spacing = (grid_upper - grid_lower) / (levels - 1)
    return [
        {"price": round(grid_lower + i * spacing, 6), "quantity": QUANTITY_PER_LEVEL}
        for i in range(levels)
    ]


def find_paper_exchange_account(client: OpenTraderClient, exchange_code: str = "KRAKEN") -> int | None:
    """
    Return the id of the first paper exchange account that matches PAPER_ACCOUNT_NAME
    AND the correct (uppercase) exchange_code.  A stale account with a lowercase code
    will cause bot.start to fail with 'exchanges2[exchangeCode] is not a function'.
    """
    accounts = client.list_exchange_accounts()
    for acc in accounts:
        if (
            acc.get("name") == PAPER_ACCOUNT_NAME
            and acc.get("isPaperAccount")
            and acc.get("exchangeCode") == exchange_code
        ):
            return acc["id"]
    return None


def bot_is_reachable(client: OpenTraderClient, bot_id: int) -> bool:
    """Return True if OpenTrader can return the bot record for bot_id."""
    try:
        client.get_bot(bot_id)
        return True
    except OpenTraderError:
        return False
    except Exception:
        return False


def main():
    client = OpenTraderClient()

    # ------------------------------------------------------------------
    # Step 1: Check if we already have a valid bot on record.
    # ------------------------------------------------------------------
    ot_state = load_json(OT_STATE_PATH)
    existing_bot_id = ot_state.get("active_bot_id")

    if existing_bot_id:
        if bot_is_reachable(client, existing_bot_id):
            print(f"  [bootstrap] bot_id={existing_bot_id} already exists and is reachable — nothing to do.")
            print(f"  [bootstrap] Run sync_execution_state.py to refresh execution_state.json.")
            return
        else:
            print(f"  [bootstrap] bot_id={existing_bot_id} on record but not reachable — will create a new bot.")

    # ------------------------------------------------------------------
    # Step 2: Load grid geometry from latest_decision.json.
    # ------------------------------------------------------------------
    decision = load_json(DECISION_PATH)
    if not decision or "grid_lower" not in decision:
        raise FileNotFoundError(
            f"Cannot read grid geometry from {DECISION_PATH}. "
            "Run scripts/run.py first to generate a decision."
        )

    grid_lines = build_grid_lines(decision)
    print(f"  [bootstrap] Grid geometry: lower={grid_lines[0]['price']} "
          f"upper={grid_lines[-1]['price']} levels={len(grid_lines)}")

    # ------------------------------------------------------------------
    # Step 3: Find or create the paper exchange account.
    # ------------------------------------------------------------------
    exchange_account_id = find_paper_exchange_account(client)

    if exchange_account_id is not None:
        print(f"  [bootstrap] Found existing paper account id={exchange_account_id} ('{PAPER_ACCOUNT_NAME}')")
    else:
        print(f"  [bootstrap] Creating paper exchange account '{PAPER_ACCOUNT_NAME}'...")
        account = client.create_paper_exchange_account(name=PAPER_ACCOUNT_NAME)
        exchange_account_id = account["id"]
        print(f"  [bootstrap] Created exchange account id={exchange_account_id}")

    # ------------------------------------------------------------------
    # Step 4: Create the grid bot.
    # ------------------------------------------------------------------
    print(f"  [bootstrap] Creating grid bot '{BOT_NAME}' on account id={exchange_account_id}...")
    bot = client.create_grid_bot(
        exchange_account_id=exchange_account_id,
        name=BOT_NAME,
        symbol=SYMBOL,
        grid_lines=grid_lines,
    )
    bot_id = bot["id"]
    print(f"  [bootstrap] Created bot id={bot_id} (enabled={bot.get('enabled', False)})")

    # ------------------------------------------------------------------
    # Step 5: Persist bot_id.
    # ------------------------------------------------------------------
    write_ot_state(bot_id)
    print(f"\n  [bootstrap] Done. Run next:")
    print(f"    python scripts/sync_execution_state.py")
    print(f"    → execution_state.json should show sync_source=OPENTRADER, bot_id={bot_id}")


if __name__ == "__main__":
    main()

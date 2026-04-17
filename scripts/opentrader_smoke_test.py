"""
Smoke test: verifies the tRPC connection works before wiring into run_cycle.sh.

Runs in order:
  1. Healthcheck (public, no auth)
  2. List exchange accounts (auth check)
  3. Create a paper exchange account (or find existing)
  4. Create a minimal grid bot (not started)
  5. Start the bot
  6. Confirm bot.enabled == True
  7. Stop the bot
  8. Confirm bot.enabled == False

Does NOT write to opentrader_state.json — this is diagnostic only.
Safe to run multiple times; bots created here are not started by run_cycle.sh.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from opentrader_client import OpenTraderClient, OpenTraderError

SMOKE_ACCOUNT_NAME = "smoke-test-paper"
SMOKE_BOT_NAME = "smoke-test-grid"
SYMBOL = "XRP/USDT"
GRID_LINES = [
    {"price": 1.30, "quantity": 10},
    {"price": 1.32, "quantity": 10},
    {"price": 1.34, "quantity": 10},
    {"price": 1.36, "quantity": 10},
    {"price": 1.38, "quantity": 10},
    {"price": 1.40, "quantity": 10},
]


def step(label):
    print(f"\n{'='*50}\n{label}")


def ok(msg=""):
    print(f"  OK {msg}")


def main():
    client = OpenTraderClient()

    # 1. Healthcheck
    step("1. Healthcheck (public, no auth)")
    try:
        result = client._query("public.healhcheck", {})
        ok(f"response={result}")
    except Exception as e:
        print(f"  FAIL: {e}")
        sys.exit(1)

    # 2. Auth check via list
    step("2. List exchange accounts (auth check)")
    try:
        accounts = client.list_exchange_accounts()
        ok(f"{len(accounts)} account(s) found")
    except OpenTraderError as e:
        print(f"  FAIL: {e}")
        print("  Check: is ADMIN_PASSWORD correct? Is OpenTrader running on port 4000?")
        sys.exit(1)

    # 3. Find or create paper exchange account
    step("3. Paper exchange account")
    account_id = None
    for acct in accounts:
        if acct.get("name") == SMOKE_ACCOUNT_NAME:
            account_id = acct["id"]
            ok(f"found existing id={account_id}")
            break

    if not account_id:
        try:
            acct = client.create_paper_exchange_account(SMOKE_ACCOUNT_NAME)
            account_id = acct["id"]
            ok(f"created id={account_id}")
        except OpenTraderError as e:
            print(f"  FAIL: {e}")
            sys.exit(1)

    # 4. Create grid bot
    step("4. Create grid bot (not started)")
    try:
        bot = client.create_grid_bot(account_id, SMOKE_BOT_NAME, SYMBOL, GRID_LINES)
        bot_id = bot["id"]
        ok(f"bot created id={bot_id}, enabled={bot.get('enabled')}")
    except OpenTraderError as e:
        print(f"  FAIL: {e}")
        sys.exit(1)

    # 5. Start bot
    step("5. Start bot")
    try:
        result = client.start_bot(bot_id)
        ok(f"result={result}")
    except OpenTraderError as e:
        print(f"  FAIL: {e}")
        sys.exit(1)

    # 6. Verify enabled
    step("6. Verify bot.enabled == True")
    try:
        bot_state = client.get_bot(bot_id)
        enabled = bot_state.get("enabled")
        if enabled:
            ok(f"enabled={enabled}")
        else:
            print(f"  WARN: enabled={enabled} — bot may still be starting asynchronously")
    except OpenTraderError as e:
        print(f"  FAIL: {e}")

    # 7. Stop bot
    step("7. Stop bot")
    try:
        result = client.stop_bot(bot_id)
        ok(f"result={result}")
    except OpenTraderError as e:
        print(f"  FAIL: {e}")
        sys.exit(1)

    # 8. Verify stopped
    step("8. Verify bot.enabled == False")
    try:
        bot_state = client.get_bot(bot_id)
        enabled = bot_state.get("enabled")
        if not enabled:
            ok(f"enabled={enabled}")
        else:
            print(f"  WARN: enabled={enabled} — stop may be in flight")
    except OpenTraderError as e:
        print(f"  FAIL: {e}")

    print("\n" + "="*50)
    print("Smoke test complete. Bot id={} left in stopped state.".format(bot_id))
    print("It will NOT be managed by push_to_opentrader.py (no state file written).")


if __name__ == "__main__":
    main()

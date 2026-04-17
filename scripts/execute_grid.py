"""
LIVE EXECUTION VERSION (KRAKEN ENABLED)

Reads latest_decision.json, builds grid, reconciles orders, and executes on Kraken.
"""

import json
import os
import sys
import time

import ccxt


SIGNAL_FILE      = os.path.join(os.path.dirname(__file__), "..", "outputs", "opentrader_signal.json")
KILL_SWITCH_FILE = os.path.join(os.path.dirname(__file__), "..", "outputs", "kill_switch.json")
PRICE_TOLERANCE  = 0.0001
ORDER_DELAY      = 0.25


def check_kill_switch():
    """Returns (enabled, mode) from kill_switch.json. Defaults to (False, None) if missing."""
    try:
        with open(KILL_SWITCH_FILE) as f:
            ks = json.load(f)
        if ks.get("enabled", False):
            return True, ks.get("mode", "pause")
    except FileNotFoundError:
        pass
    return False, None


def load_signal(path):
    with open(path) as f:
        data = json.load(f)

    # Signal file writes `adapter_action`, not `action`.
    # Map adapter vocabulary to execute_grid's internal action space.
    # Unknown adapter values safe-default to HOLD so novel NO_ACTION variants
    # never accidentally trigger execution.
    raw = data.get("adapter_action", "NO_ACTION").upper()
    ADAPTER_MAP = {
        "NO_ACTION":               "HOLD",
        "DEPLOY_OR_MAINTAIN_GRID": "INITIATE",
        "REPLACE_GRID":            "REPLACE",
        "RECENTER_GRID":           "RECENTER",
        "EXIT_GRID":               "EXIT",
    }
    action = ADAPTER_MAP.get(raw, "HOLD")

    # HOLD and EXIT do not require grid geometry.
    if action in ("HOLD", "EXIT"):
        return action, None

    # INITIATE, REPLACE, RECENTER require grid geometry.
    # A missing "grid" key on an actionable signal must raise immediately —
    # silent fallback here would leave the exchange untouched while the brain
    # believes an action was executed.
    grid_data = data.get("grid")
    if grid_data is None:
        raise ValueError(
            f"Signal adapter_action='{raw}' (mapped to '{action}') requires grid "
            "geometry but the 'grid' key is absent in the signal file. "
            "Ensure build_opentrader_signal.py populates the grid payload for actionable signals."
        )

    lower    = grid_data["grid_lower"]
    spacing  = grid_data["spacing"]
    levels_n = int(grid_data["levels"])

    levels = []
    for i in range(levels_n):
        price = lower + i * spacing
        levels.append({
            "price": round(price, 6),
            "size": 3  # 3 XRP per order
        })

    return action, {
        "symbol": grid_data.get("symbol", "XRP/USD"),
        "levels": levels,
    }


def connect_kraken():
    api_key = os.environ.get("KRAKEN_API_KEY")
    api_secret = os.environ.get("KRAKEN_API_SECRET")

    if not api_key or not api_secret:
        raise EnvironmentError("Missing Kraken API credentials")

    return ccxt.kraken({
        "apiKey": api_key,
        "secret": api_secret,
        "enableRateLimit": True,
    })


def fetch_open_orders(exchange, symbol):
    orders = exchange.fetch_open_orders(symbol)
    print(f"[fetch] {len(orders)} open orders")
    return orders


def fetch_current_price(exchange, symbol):
    ticker = exchange.fetch_ticker(symbol)
    price = ticker["last"]
    print(f"[price] {price}")
    return price


def prices_match(order_price, level_price):
    if level_price == 0:
        return False
    return abs(order_price - level_price) / level_price <= PRICE_TOLERANCE


def desired_orders(levels, current_price):
    result = []
    for lvl in levels:
        price = float(lvl["price"])
        size = float(lvl["size"])
        side = "buy" if price <= current_price else "sell"
        result.append((side, price, size))
    return result


def grid_matches_orders(levels, open_orders, current_price):
    if len(open_orders) != len(levels):
        return False

    desired = desired_orders(levels, current_price)

    order_set = set()
    for o in open_orders:
        order_set.add((o["side"], round(o["price"], 8), round(o["amount"], 8)))

    for side, price, size in desired:
        matched = any(
            s == side and
            prices_match(p, price) and
            abs(a - size) / max(size, 1e-8) <= PRICE_TOLERANCE
            for s, p, a in order_set
        )
        if not matched:
            return False

    return True


def cancel_all_orders(exchange, symbol, open_orders):
    for order in open_orders:
        try:
            exchange.cancel_order(order["id"], symbol)
            print(f"[cancel] {order['id']}")
        except Exception as e:
            print(f"[cancel] {order['id']} skipped: {e}")


def place_grid(exchange, symbol, levels, current_price):
    desired = desired_orders(levels, current_price)

    for side, price, size in desired:
        order = exchange.create_limit_order(symbol, side, size, price)
        print(f"[place] {side} {size} @ {price} -> {order['id']}")
        time.sleep(ORDER_DELAY)


def run():
    print(f"[start] {SIGNAL_FILE}")

    ks_enabled, ks_mode = check_kill_switch()
    if ks_enabled:
        if ks_mode == "exit":
            exchange = connect_kraken()
            open_orders = fetch_open_orders(exchange, "XRP/USD")
            cancel_all_orders(exchange, "XRP/USD", open_orders)
            print("[killswitch] exiting all orders")
        else:
            print("[killswitch] paused")
        return

    action, grid = load_signal(SIGNAL_FILE)

    print(f"[signal] action={action}")

    # --- Path 1: HOLD — strictly passive, no exchange connection. ---
    if action == "HOLD":
        print("[hold] no action")
        return

    exchange = connect_kraken()
    symbol = "XRP/USD"
    open_orders = fetch_open_orders(exchange, symbol)

    # --- Path 2: EXIT — cancel all open orders, no grid geometry needed. ---
    if action == "EXIT":
        cancel_all_orders(exchange, symbol, open_orders)
        print("[exit] all orders cancelled")
        return

    # --- Path 3: INITIATE / REPLACE / RECENTER — grid geometry required. ---
    levels = grid["levels"]
    current_price = fetch_current_price(exchange, symbol)

    if not open_orders:
        print(f"[{action.lower()}] placing grid")
        place_grid(exchange, symbol, levels, current_price)
    elif grid_matches_orders(levels, open_orders, current_price):
        print("[ok] grid already aligned")
    else:
        print(f"[{action.lower()}] rebuilding grid")
        cancel_all_orders(exchange, symbol, open_orders)
        place_grid(exchange, symbol, levels, current_price)

    print("[done]")


if __name__ == "__main__":
    run()

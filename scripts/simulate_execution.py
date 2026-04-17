#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUTS_DIR = PROJECT_ROOT / "outputs"

SIGNAL_PATH = OUTPUTS_DIR / "opentrader_signal.json"
STATE_PATH = OUTPUTS_DIR / "execution_state.json"
LOG_PATH = OUTPUTS_DIR / "execution_log.csv"


def utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def safe_float(v):
    try:
        return float(v)
    except:
        return None


def safe_int(v):
    try:
        return int(float(v))
    except:
        return None


def ensure_outputs_dir():
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)


def read_json(path):
    with path.open("r") as f:
        return json.load(f)


def write_json(path, payload):
    with path.open("w") as f:
        json.dump(payload, f, indent=2)


def default_execution_state():
    return {
        "has_open_grid": False,
        "grid_id": None,
        "symbol": None,
        "status": "FLAT",
        "lower": None,
        "upper": None,
        "center": None,
        "levels": None,
        "spacing_pct": None,
        "opened_ts": None,
        "closed_ts": None,
        "last_action": None,
        "last_reason": None,
        "last_signal_ts": None,
        "last_sync_ts": None,
    }


def read_or_init_execution_state():

    if not STATE_PATH.exists():
        state = default_execution_state()
        write_json(STATE_PATH, state)
        return state

    state = read_json(STATE_PATH)

    merged = default_execution_state()
    merged.update(state)

    # 🔴 NORMALIZATION
    has_legacy_grid = (
        merged.get("live_grid_lower") is not None
        and merged.get("live_grid_upper") is not None
    )

    if has_legacy_grid:
        merged["has_open_grid"] = True
        merged["status"] = "OPEN"

        if merged.get("lower") is None:
            merged["lower"] = merged.get("live_grid_lower")

        if merged.get("upper") is None:
            merged["upper"] = merged.get("live_grid_upper")

    else:
        merged["has_open_grid"] = False
        merged["status"] = "FLAT"

    return merged


def append_log_row(row):

    fieldnames = [
        "event_ts","signal_ts","symbol","requested_action","requested_reason",
        "prior_status","prior_has_open_grid",
        "result_status","result_has_open_grid",
        "accepted","event_type","grid_id"
    ]

    write_header = not LOG_PATH.exists()

    with LOG_PATH.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerow(row)


# ✅ FIXED NORMALIZER
def normalize_action(signal):

    raw = str(
        signal.get("adapter_action")
        or signal.get("lifecycle_action")
        or ""
    ).upper()

    mapping = {
        "INITIATE": "ENTER",
        "ENTER": "ENTER",
        "HOLD": "HOLD",
        "NO_ACTION": "HOLD",   # 🔥 THIS IS THE FIX
        "NONE": "HOLD",
        "RECENTER": "RECENTER",
        "REPLACE": "RECENTER",
        "EXIT": "EXIT",
    }

    return mapping.get(raw, raw)


def extract_signal(signal):

    lower = safe_float(signal.get("grid_lower"))
    upper = safe_float(signal.get("grid_upper"))

    center = None
    if lower and upper:
        center = (lower + upper) / 2

    return {
        "signal_ts": signal.get("timestamp", utc_now_iso()),
        "symbol": signal.get("symbol", "XRP/USD"),
        "action": normalize_action(signal),
        "reason": signal.get("adapter_reason", ""),
        "lower": lower,
        "upper": upper,
        "center": center,
        "levels": safe_int(signal.get("levels")),
        "spacing_pct": safe_float(signal.get("spacing_pct")),
    }


def handle_enter(state, req):

    if state["has_open_grid"]:
        return False, "REJECTED_ALREADY_OPEN", state

    new = deepcopy(state)
    now = utc_now_iso()

    new.update({
        "has_open_grid": True,
        "status": "OPEN",
        "grid_id": f"paper_{uuid.uuid4().hex[:6]}",
        "symbol": req["symbol"],
        "lower": req["lower"],
        "upper": req["upper"],
        "center": req["center"],
        "levels": req["levels"],
        "spacing_pct": req["spacing_pct"],
        "opened_ts": now,
        "last_action": "ENTER",
        "last_reason": req["reason"],
        "last_signal_ts": req["signal_ts"],
        "last_sync_ts": now
    })

    return True, "ENTERED", new


def handle_hold(state, req):

    new = deepcopy(state)
    now = utc_now_iso()

    new.update({
        "last_action": "HOLD",
        "last_reason": req["reason"],
        "last_signal_ts": req["signal_ts"],
        "last_sync_ts": now
    })

    return True, "HELD", new


def handle_recenter(state, req):

    if not state["has_open_grid"]:
        return False, "NO_GRID_TO_RECENTER", state

    new = deepcopy(state)
    now = utc_now_iso()

    new.update({
        "lower": req["lower"],
        "upper": req["upper"],
        "center": req["center"],
        "levels": req["levels"],
        "spacing_pct": req["spacing_pct"],
        "last_action": "RECENTER",
        "last_reason": req["reason"],
        "last_signal_ts": req["signal_ts"],
        "last_sync_ts": now
    })

    return True, "RECENTERED", new


def handle_exit(state, req):

    if not state["has_open_grid"]:
        return False, "NO_GRID_TO_EXIT", state

    new = deepcopy(state)
    now = utc_now_iso()

    new.update({
        "has_open_grid": False,
        "status": "FLAT",
        "lower": None,
        "upper": None,
        "center": None,
        "levels": None,
        "spacing_pct": None,
        "closed_ts": now,
        "last_action": "EXIT",
        "last_reason": req["reason"],
        "last_signal_ts": req["signal_ts"],
        "last_sync_ts": now
    })

    return True, "EXITED", new


def main():

    ensure_outputs_dir()

    signal = read_json(SIGNAL_PATH)
    req = extract_signal(signal)

    state = read_or_init_execution_state()

    action = req["action"]

    if action == "ENTER":
        accepted, event, new_state = handle_enter(state, req)
    elif action == "HOLD":
        accepted, event, new_state = handle_hold(state, req)
    elif action == "RECENTER":
        accepted, event, new_state = handle_recenter(state, req)
    elif action == "EXIT":
        accepted, event, new_state = handle_exit(state, req)
    else:
        accepted, event, new_state = False, "UNKNOWN_ACTION", state

    write_json(STATE_PATH, new_state)

    append_log_row({
        "event_ts": utc_now_iso(),
        "signal_ts": req["signal_ts"],
        "symbol": req["symbol"],
        "requested_action": action,
        "requested_reason": req["reason"],
        "prior_status": state["status"],
        "prior_has_open_grid": state["has_open_grid"],
        "result_status": new_state["status"],
        "result_has_open_grid": new_state["has_open_grid"],
        "accepted": accepted,
        "event_type": event,
        "grid_id": new_state.get("grid_id"),
    })

    print("EXECUTION RESULT")
    print("Action:", action)
    print("Accepted:", accepted)
    print("Event:", event)
    print("Grid Open:", new_state["has_open_grid"])
    print("Status:", new_state["status"])


if __name__ == "__main__":
    main()

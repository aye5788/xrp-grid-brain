import json
import os
from datetime import datetime, timezone, timedelta

import pandas as pd


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EVAL_PATH = os.path.join(BASE_DIR, "outputs", "evaluation_history.csv")
PARAMS_PATH = os.path.join(BASE_DIR, "outputs", "adaptive_params.json")

WINDOW_SIZE = 20
MIN_ROWS = 15
COOLDOWN_DAYS = 3

THRESHOLD_MIN = 10.0
THRESHOLD_MAX = 22.0

WIDTH_SCALE_MIN = 0.80
WIDTH_SCALE_MAX = 1.25


def load_params():
    if not os.path.exists(PARAMS_PATH):
        return {
            "initiation_score_threshold": 12.0,
            "width_scale": 1.0,
            "frozen": False,
            "last_updated": None,
            "history": []
        }

    with open(PARAMS_PATH, "r") as f:
        return json.load(f)


def save_params(params):
    with open(PARAMS_PATH, "w") as f:
        json.dump(params, f, indent=2)


def can_update(params):
    if params.get("frozen", False):
        print("[ADAPT] frozen=true → skipping adaptation")
        return False

    last_updated = params.get("last_updated")
    if not last_updated:
        return True

    try:
        last_dt = datetime.fromisoformat(last_updated.replace("Z", "+00:00"))
    except Exception:
        return True

    now = datetime.now(timezone.utc)
    return (now - last_dt) >= timedelta(days=COOLDOWN_DAYS)


def load_eval_window():
    if not os.path.exists(EVAL_PATH):
        print("[ADAPT] evaluation_history.csv missing → skipping")
        return None

    df = pd.read_csv(EVAL_PATH)

    if df.empty:
        print("[ADAPT] evaluation_history.csv empty → skipping")
        return None

    df.columns = [c.strip().lower() for c in df.columns]

    required_cols = ["lifecycle_action", "likely_stranded_24h", "stayed_in_range_24h"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        print(f"[ADAPT] Missing required evaluation columns: {missing} → skipping")
        return None

    init_df = df[df["lifecycle_action"].astype(str).str.upper() == "INITIATE"].copy()

    if init_df.empty:
        print("[ADAPT] No INITIATE rows found → skipping")
        return None

    init_df = init_df.tail(WINDOW_SIZE).copy()

    if len(init_df) < MIN_ROWS:
        print(f"[ADAPT] Only {len(init_df)} INITIATE rows available (< {MIN_ROWS}) → skipping")
        return None

    return init_df


def clip(value, lo, hi):
    return max(lo, min(value, hi))


def append_history(params, param_name, old, new, reason):
    params.setdefault("history", [])
    params["history"].append({
        "ts": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "param": param_name,
        "old": old,
        "new": new,
        "reason": reason
    })

    # keep history bounded
    if len(params["history"]) > 100:
        params["history"] = params["history"][-100:]


def adapt_threshold(params, window_df):
    stranded_rate = pd.to_numeric(window_df["likely_stranded_24h"], errors="coerce").fillna(0).mean()

    old = float(params["initiation_score_threshold"])
    new = old
    reason = None

    if stranded_rate > 0.55:
        new = clip(old + 1.0, THRESHOLD_MIN, THRESHOLD_MAX)
        reason = f"stranded_rate={stranded_rate:.3f} > 0.55"
    elif stranded_rate < 0.25:
        new = clip(old - 0.5, THRESHOLD_MIN, THRESHOLD_MAX)
        reason = f"stranded_rate={stranded_rate:.3f} < 0.25"

    if new != old:
        params["initiation_score_threshold"] = new
        append_history(params, "initiation_score_threshold", old, new, reason)
        print(f"[ADAPT] initiation_score_threshold: {old} -> {new} ({reason})")
        return True

    print(f"[ADAPT] initiation_score_threshold unchanged (stranded_rate={stranded_rate:.3f})")
    return False


def adapt_width_scale(params, window_df):
    stay_rate = pd.to_numeric(window_df["stayed_in_range_24h"], errors="coerce").fillna(0).mean()
    stranded_rate = pd.to_numeric(window_df["likely_stranded_24h"], errors="coerce").fillna(0).mean()

    old = float(params["width_scale"])
    new = old
    reason = None

    if stay_rate < 0.35:
        new = clip(old * 1.07, WIDTH_SCALE_MIN, WIDTH_SCALE_MAX)
        reason = f"stay_rate={stay_rate:.3f} < 0.35"
    elif stranded_rate > 0.60 and stay_rate > 0.65:
        new = clip(old * 0.95, WIDTH_SCALE_MIN, WIDTH_SCALE_MAX)
        reason = f"stranded_rate={stranded_rate:.3f} > 0.60 and stay_rate={stay_rate:.3f} > 0.65"

    if new != old:
        params["width_scale"] = round(new, 6)
        append_history(params, "width_scale", old, new, reason)
        print(f"[ADAPT] width_scale: {old} -> {new:.6f} ({reason})")
        return True

    print(f"[ADAPT] width_scale unchanged (stay_rate={stay_rate:.3f}, stranded_rate={stranded_rate:.3f})")
    return False


def main():
    params = load_params()

    if not can_update(params):
        print("[ADAPT] Cooldown active → skipping adaptation")
        return

    window_df = load_eval_window()
    if window_df is None:
        return

    print(f"[ADAPT] Using {len(window_df)} INITIATE evaluation rows")

    # priority: threshold first, then width
    changed = adapt_threshold(params, window_df)

    if not changed:
        changed = adapt_width_scale(params, window_df)

    if changed:
        params["last_updated"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        save_params(params)
        print("[ADAPT] adaptive_params.json updated")
    else:
        print("[ADAPT] No parameter changes made")


if __name__ == "__main__":
    main()

import pandas as pd
import json
from pathlib import Path

DECISION_PATH = "outputs/latest_decision.csv"
LIFECYCLE_PATH = "outputs/lifecycle_decision.csv"

OUTPUT_JSON = "outputs/opentrader_signal.json"
OUTPUT_CSV = "outputs/opentrader_signal.csv"


def load_inputs():
    if not Path(DECISION_PATH).exists():
        raise FileNotFoundError(f"Missing: {DECISION_PATH}")
    if not Path(LIFECYCLE_PATH).exists():
        raise FileNotFoundError(f"Missing: {LIFECYCLE_PATH}")

    decision = pd.read_csv(DECISION_PATH)
    lifecycle = pd.read_csv(LIFECYCLE_PATH)

    if decision.empty:
        raise ValueError("latest_decision.csv is empty")
    if lifecycle.empty:
        raise ValueError("lifecycle_decision.csv is empty")

    return decision.iloc[0], lifecycle.iloc[0]


def determine_opentrader_action(decision, lifecycle):
    tradable = bool(decision.get("tradable", False))
    lifecycle_action = str(lifecycle.get("lifecycle_action", "UNKNOWN")).upper()

    if not tradable:
        return "NO_ACTION", "brain_marked_not_tradable"

    if lifecycle_action == "HOLD":
        return "DEPLOY_OR_MAINTAIN_GRID", "tradable_and_lifecycle_hold"

    if lifecycle_action == "RECENTER":
        return "RECENTER_GRID", "lifecycle_requested_recenter"

    if lifecycle_action == "REPLACE":
        return "REPLACE_GRID", "lifecycle_requested_replace"

    if lifecycle_action == "EXIT":
        return "CLOSE_GRID", "lifecycle_requested_exit"

    return "NO_ACTION", "unrecognized_lifecycle_state"


def build_payload(decision, lifecycle):
    action, reason = determine_opentrader_action(decision, lifecycle)

    payload = {
        "timestamp": str(pd.Timestamp.utcnow()),
        "symbol": "XRP/USD",
        "adapter_action": action,
        "adapter_reason": reason,

        "lifecycle_action": lifecycle.get("lifecycle_action"),
        "lifecycle_reason": lifecycle.get("reason"),

        "tradable": bool(decision.get("tradable", False)),
        "candidate_score": float(decision.get("candidate_score", 0)),
        "operational_regime": decision.get("operational_regime"),
        "risk_mode": decision.get("risk_mode"),

        "grid_lower": float(decision.get("grid_lower")),
        "grid_upper": float(decision.get("grid_upper")),
        "center_price": float(decision.get("center_price")),
        "levels": int(decision.get("levels")),
        "spacing": float(decision.get("spacing")),
        "spacing_pct": float(decision.get("spacing_pct")),
        "width_pct": float(decision.get("width_pct")),
        "fee_pct": float(decision.get("fee_pct")),
        "est_profit_per_level": float(decision.get("est_profit_per_level")),
    }

    return payload


def save_outputs(payload):
    with open(OUTPUT_JSON, "w") as f:
        json.dump(payload, f, indent=2)

    pd.DataFrame([payload]).to_csv(OUTPUT_CSV, index=False)


def main():
    decision, lifecycle = load_inputs()
    payload = build_payload(decision, lifecycle)
    save_outputs(payload)

    print(f"OpenTrader signal → {payload['adapter_action']} ({payload['adapter_reason']})")


if __name__ == "__main__":
    main()

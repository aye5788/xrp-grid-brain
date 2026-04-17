#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_DIR"

source .venv/bin/activate

echo "=============================="
echo "RUN START: $(date)"
echo "=============================="

echo "Loading existing data..."
python scripts/update_data.py

echo
echo "Running brain pipeline..."
python scripts/run.py

echo
echo "Running lifecycle..."
python scripts/lifecycle.py

echo
echo "Syncing execution state..."
python scripts/sync_execution_state.py

echo
echo "Building OpenTrader signal..."
python scripts/build_opentrader_signal.py

echo
echo "Pushing lifecycle action to OpenTrader..."
python scripts/push_to_opentrader.py

echo
echo "Running evaluation..."
python scripts/evaluate.py

echo
echo "Running adaptation..."
python scripts/adapt_params.py

echo
echo "Running summary..."
python scripts/eval_summary.py

echo
echo "Running PAPER execution (this is your trading layer now)..."
python scripts/paper_engine.py

echo
echo "Simulating execution (legacy metrics)..."
python scripts/simulate_execution.py

echo
echo "Updating Google Sheet (includes paper tracking)..."
python scripts/update_google_sheet.py

echo
echo "=============================="
echo "RUN COMPLETE: $(date)"
echo "=============================="

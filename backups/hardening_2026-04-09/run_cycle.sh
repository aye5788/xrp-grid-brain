#!/bin/bash

cd /root/projects/xrp-grid-brain || exit 1
source .venv/bin/activate

echo "=============================="
echo "RUN START: $(date -u)"
echo "=============================="
python scripts/update_data.py
python scripts/run.py
python scripts/evaluate.py
python scripts/eval_summary.py
python scripts/lifecycle.py
python scripts/build_opentrader_signal.py
python scripts/update_google_sheet.py

echo "=============================="
echo "RUN END: $(date -u)"
echo "=============================="

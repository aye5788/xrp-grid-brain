#!/bin/bash

cd /root/projects/xrp-grid-brain || exit 1

source .venv/bin/activate

echo "=============================="
echo "RUN START: $(date -u)"
echo "=============================="

python scripts/update_data.py
python scripts/run.py

echo "=============================="
echo "RUN END: $(date -u)"
echo "=============================="

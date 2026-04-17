#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="$BASE_DIR/outputs/snapshot"

mkdir -p "$OUT_DIR"

echo "Building LLM snapshot..."

# 1. Repo structure
find . \
  -path './.venv' -prune -o \
  -path './__pycache__' -prune -o \
  -path './logs' -prune -o \
  -print > "$OUT_DIR/repo_map.txt"
# 2. System state JSON (compose from existing outputs)
cat > "$OUT_DIR/system_state.json" <<EOF
{
  "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "latest_decision": $(cat "$BASE_DIR/outputs/latest_decision.json" 2>/dev/null || echo "null"),
  "lifecycle": $(cat "$BASE_DIR/outputs/lifecycle_decision.json" 2>/dev/null || echo "null"),
  "execution_state": $(cat "$BASE_DIR/outputs/execution_state.json" 2>/dev/null || echo "null")
}
EOF

# 3. Minimal context instructions for LLM
cat > "$OUT_DIR/context.txt" <<EOF
You are reviewing a regime-aware XRP grid trading system.

Focus on:
- state consistency
- lifecycle correctness
- execution safety

Do NOT invent new strategy logic.
Do NOT assume execution truth unless explicitly provided.
EOF

# 4. Bundle everything
echo "Snapshot files ready:"
echo "$OUT_DIR/system_state.json"
echo "$OUT_DIR/repo_map.txt"
echo "$OUT_DIR/context.txt"
echo "Uploading to Cloud Storage..."
python scripts/upload_snapshot_to_gcs.py

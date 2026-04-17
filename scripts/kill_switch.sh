#!/usr/bin/env bash

FILE="outputs/kill_switch.json"

case "$1" in
  pause)
    echo '{"enabled": true, "mode": "pause"}' > $FILE
    echo "[killswitch] PAUSED"
    ;;
  exit)
    echo '{"enabled": true, "mode": "exit"}' > $FILE
    echo "[killswitch] EXIT ALL"
    ;;
  off)
    echo '{"enabled": false, "mode": "pause"}' > $FILE
    echo "[killswitch] OFF"
    ;;
  *)
    echo "Usage: kill_switch.sh {pause|exit|off}"
    ;;
esac

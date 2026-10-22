#!/usr/bin/env bash
# demo.sh — End-to-end demo execution script
set -euo pipefail

echo "=========================================================="
echo " Starting VortexStream Real-Time OTT QoE Engine Demo"
echo "=========================================================="

python scripts/demo.py "$@"

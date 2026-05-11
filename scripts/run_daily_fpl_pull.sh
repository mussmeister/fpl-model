#!/bin/bash
# Daily FPL API delta pull — run via cron on EC2
# Cron: 0 2 * * * /home/ubuntu/fpl-model/scripts/run_daily_fpl_pull.sh >> /home/ubuntu/fpl-model/logs/fpl_api_pull.log 2>&1

set -e
cd "$(dirname "$0")/.."

echo "=== $(date '+%Y-%m-%d %H:%M:%S') FPL delta pull starting ==="
venv/bin/python3 scripts/fpl_api_pull.py --delta
echo "=== $(date '+%Y-%m-%d %H:%M:%S') done ==="

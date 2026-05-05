#!/usr/bin/env bash
set -euo pipefail
BOT_ROOT="${BOT_ROOT:-$(pwd)}"
SERVICE_USER="${SERVICE_USER:-$USER}"
BOT_CONTAINER="${BOT_CONTAINER:-}"
if [[ -z "$BOT_CONTAINER" ]]; then
  echo "Set BOT_CONTAINER=<docker-container-name-or-id> and re-run." >&2
  exit 1
fi
cd "$BOT_ROOT/dashboard_v2/backend"
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
sudo cp "$BOT_ROOT/dashboard_v2/ops/systemd/trading-dashboard-v2.service" /etc/systemd/system/trading-dashboard-v2.service
sudo cp "$BOT_ROOT/dashboard_v2/ops/systemd/trading-dashboard-v2-tail.service" /etc/systemd/system/trading-dashboard-v2-tail.service
sudo sed -i "s#__BOT_ROOT__#$BOT_ROOT#g; s#__SERVICE_USER__#$SERVICE_USER#g" /etc/systemd/system/trading-dashboard-v2.service
sudo sed -i "s#__BOT_ROOT__#$BOT_ROOT#g; s#__BOT_CONTAINER__#$BOT_CONTAINER#g" /etc/systemd/system/trading-dashboard-v2-tail.service
sudo systemctl daemon-reload
sudo systemctl enable trading-dashboard-v2 trading-dashboard-v2-tail
sudo systemctl restart trading-dashboard-v2
sleep 2
curl -fsS http://127.0.0.1:8000/api/health || true
sudo systemctl restart trading-dashboard-v2-tail
sudo systemctl status trading-dashboard-v2 --no-pager || true
sudo systemctl status trading-dashboard-v2-tail --no-pager || true

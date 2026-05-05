#!/usr/bin/env bash
set -euo pipefail

BOT_ROOT="${BOT_ROOT:-/opt/trading-bot}"
SERVICE_USER="${SERVICE_USER:-ec2-user}"

cd "$BOT_ROOT/dashboard/backend"
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

sudo cp "$BOT_ROOT/ops/systemd/trading-dashboard.service" /etc/systemd/system/trading-dashboard.service
sudo sed -i "s#User=ec2-user#User=${SERVICE_USER}#g" /etc/systemd/system/trading-dashboard.service
sudo sed -i "s#/opt/trading-bot#${BOT_ROOT}#g" /etc/systemd/system/trading-dashboard.service
sudo systemctl daemon-reload
sudo systemctl enable trading-dashboard
sudo systemctl restart trading-dashboard
sudo systemctl status trading-dashboard --no-pager

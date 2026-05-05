#!/usr/bin/env bash
set -euo pipefail
BOT_ROOT="${BOT_ROOT:-$(pwd)}"
SERVICE_USER="${SERVICE_USER:-${USER:-ec2-user}}"
DASH_ROOT="$BOT_ROOT/dashboard"
if [[ ! -d "$DASH_ROOT/backend" ]]; then
  echo "ERROR: dashboard/backend not found under $DASH_ROOT" >&2
  exit 1
fi
python3 -m venv "$DASH_ROOT/backend/.venv"
"$DASH_ROOT/backend/.venv/bin/pip" install -r "$DASH_ROOT/backend/requirements.txt"
sudo tee /usr/local/bin/trading-dashboard-v24 >/dev/null <<EOF
#!/usr/bin/env bash
set -euo pipefail
cd "$DASH_ROOT/backend"
exec "$DASH_ROOT/backend/.venv/bin/python" run.py
EOF
sudo chmod +x /usr/local/bin/trading-dashboard-v24
sudo tee /etc/systemd/system/trading-dashboard.service >/dev/null <<EOF
[Unit]
Description=Trading Dashboard V24
After=network.target

[Service]
Type=simple
User=$SERVICE_USER
WorkingDirectory=$DASH_ROOT/backend
ExecStart=/usr/local/bin/trading-dashboard-v24
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF
sudo systemctl daemon-reload
sudo systemctl enable trading-dashboard
sudo systemctl restart trading-dashboard
echo "Dashboard service installed. Check: curl http://127.0.0.1:8000/api/health"

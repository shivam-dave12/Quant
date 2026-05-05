#!/usr/bin/env bash
set -euo pipefail
BOT_ROOT="${BOT_ROOT:-$(pwd)}"
SERVICE_USER="${SERVICE_USER:-$USER}"
BOT_CONTAINER="${BOT_CONTAINER:-}"
DASHBOARD_DIR="$BOT_ROOT/dashboard"
if [[ ! -d "$DASHBOARD_DIR/backend" ]]; then
  echo "Dashboard directory not found at $DASHBOARD_DIR" >&2
  exit 1
fi
cd "$DASHBOARD_DIR/backend"
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
sudo tee /usr/local/bin/trading-dashboard >/dev/null <<EOF
#!/usr/bin/env bash
cd "$DASHBOARD_DIR/backend"
exec "$DASHBOARD_DIR/backend/.venv/bin/python" "$DASHBOARD_DIR/backend/run.py"
EOF
sudo chmod +x /usr/local/bin/trading-dashboard
sudo tee /etc/systemd/system/trading-dashboard.service >/dev/null <<EOF
[Unit]
Description=Trading Dashboard Integrated Backend
After=network.target

[Service]
Type=simple
User=$SERVICE_USER
WorkingDirectory=$DASHBOARD_DIR/backend
ExecStart=/usr/local/bin/trading-dashboard
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF
sudo systemctl daemon-reload
sudo systemctl enable trading-dashboard
sudo systemctl restart trading-dashboard
sleep 2
curl -fsS http://127.0.0.1:8000/api/health || true
sudo systemctl status trading-dashboard --no-pager || true
if [[ -n "$BOT_CONTAINER" ]]; then
  sudo tee /usr/local/bin/trading-dashboard-tail >/dev/null <<EOF
#!/usr/bin/env bash
LOG_FILE=\$(docker inspect -f '{{.LogPath}}' "$BOT_CONTAINER")
cd "$DASHBOARD_DIR"
exec "$DASHBOARD_DIR/backend/.venv/bin/python" "$DASHBOARD_DIR/agents/log_tail_agent.py" --log "\$LOG_FILE" --dashboard http://127.0.0.1:8000 --from-start
EOF
  sudo chmod +x /usr/local/bin/trading-dashboard-tail
  sudo tee /etc/systemd/system/trading-dashboard-tail.service >/dev/null <<EOF
[Unit]
Description=Trading Dashboard Fallback Log Tail
After=docker.service trading-dashboard.service
Requires=trading-dashboard.service

[Service]
Type=simple
User=root
WorkingDirectory=$DASHBOARD_DIR
ExecStart=/usr/local/bin/trading-dashboard-tail
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF
  sudo systemctl daemon-reload
  sudo systemctl enable trading-dashboard-tail
  sudo systemctl restart trading-dashboard-tail
  sudo systemctl status trading-dashboard-tail --no-pager || true
fi
cat <<MSG

Dashboard installed.
Direct bot telemetry uses DASHBOARD_ENABLED=true and DASHBOARD_URL=http://127.0.0.1:8000.
If the bot runs in Docker, set DASHBOARD_URL=http://host.docker.internal:8000 or http://172.17.0.1:8000.
MSG

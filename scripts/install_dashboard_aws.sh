#!/usr/bin/env bash
set -euo pipefail

BOT_ROOT="${BOT_ROOT:-$(pwd)}"
SERVICE_USER="${SERVICE_USER:-${USER:-ec2-user}}"
DASH_ROOT="$BOT_ROOT/dashboard"
DASHBOARD_URL="${DASHBOARD_URL:-http://127.0.0.1:8000}"
BOT_CONTAINER="${BOT_CONTAINER:-}"

if [[ ! -d "$DASH_ROOT/backend" ]]; then
  echo "ERROR: dashboard/backend not found under $DASH_ROOT" >&2
  echo "Run this from the unzipped bot root or set BOT_ROOT=/path/to/root" >&2
  exit 1
fi

python3 -m venv "$DASH_ROOT/backend/.venv"
"$DASH_ROOT/backend/.venv/bin/pip" install --upgrade pip >/dev/null
"$DASH_ROOT/backend/.venv/bin/pip" install -r "$DASH_ROOT/backend/requirements.txt"

sudo tee /usr/local/bin/trading-dashboard-v25 >/dev/null <<EOF
#!/usr/bin/env bash
set -euo pipefail
cd "$DASH_ROOT/backend"
exec "$DASH_ROOT/backend/.venv/bin/python" run.py
EOF
sudo chmod +x /usr/local/bin/trading-dashboard-v25

sudo tee /etc/systemd/system/trading-dashboard.service >/dev/null <<EOF
[Unit]
Description=Trading Dashboard V25
After=network.target

[Service]
Type=simple
User=$SERVICE_USER
WorkingDirectory=$DASH_ROOT/backend
ExecStart=/usr/local/bin/trading-dashboard-v25
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

# Tail agent wrapper. Runs as root because Docker JSON logs are usually under
# /var/lib/docker/containers and are not readable by ec2-user/ubuntu.
sudo tee /usr/local/bin/trading-dashboard-tail-v25 >/dev/null <<EOF
#!/usr/bin/env bash
set -euo pipefail
DASHBOARD_URL="${DASHBOARD_URL:-http://127.0.0.1:8000}"
BOT_CONTAINER="${BOT_CONTAINER:-}"

if [[ -z "\$BOT_CONTAINER" ]]; then
  # Prefer a container with trading/bot/liquidity/quant in its name/image.
  BOT_CONTAINER=\$(docker ps --format '{{.ID}} {{.Names}} {{.Image}}' \
    | awk 'tolower(\$0) ~ /(trading|liquidity|quant|bot)/ {print \$1; exit}')
fi
if [[ -z "\$BOT_CONTAINER" ]]; then
  echo "ERROR: BOT_CONTAINER not set and auto-detect found no likely bot container" >&2
  echo "Set BOT_CONTAINER=<container_name_or_id> and rerun scripts/install_dashboard_aws.sh" >&2
  docker ps --format 'table {{.ID}}\t{{.Names}}\t{{.Image}}\t{{.Status}}' >&2 || true
  exit 2
fi
LOG_FILE=\$(docker inspect -f '{{.LogPath}}' "\$BOT_CONTAINER")
if [[ -z "\$LOG_FILE" || ! -f "\$LOG_FILE" ]]; then
  echo "ERROR: Docker log path missing for container \$BOT_CONTAINER: \$LOG_FILE" >&2
  exit 3
fi
exec "$DASH_ROOT/backend/.venv/bin/python" "$DASH_ROOT/agents/log_tail_agent.py" --log "\$LOG_FILE" --dashboard "\$DASHBOARD_URL" --from-start
EOF
sudo chmod +x /usr/local/bin/trading-dashboard-tail-v25

sudo tee /etc/systemd/system/trading-dashboard-tail.service >/dev/null <<EOF
[Unit]
Description=Trading Dashboard V25 Docker Log Tail
After=docker.service trading-dashboard.service
Requires=trading-dashboard.service

[Service]
Type=simple
User=root
Environment=BOT_CONTAINER=$BOT_CONTAINER
Environment=DASHBOARD_URL=$DASHBOARD_URL
WorkingDirectory=$BOT_ROOT
ExecStart=/usr/local/bin/trading-dashboard-tail-v25
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable trading-dashboard trading-dashboard-tail
sudo systemctl restart trading-dashboard
sleep 2
sudo systemctl restart trading-dashboard-tail

echo "Dashboard + tail services installed."
echo "Health:      curl http://127.0.0.1:8000/api/health"
echo "Diagnostics: curl http://127.0.0.1:8000/api/diagnostics"
echo "Tail logs:   sudo journalctl -u trading-dashboard-tail -n 100 --no-pager"

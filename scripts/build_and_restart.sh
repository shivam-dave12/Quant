#!/usr/bin/env bash
if [ -z "${BASH_VERSION:-}" ]; then
  exec bash "$0" "$@"
fi
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="${QUANT_PROJECT_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"
BASE_DIR="${QUANT_BASE_DIR:-$(dirname "$PROJECT_DIR")}"
STATE_DIR="$BASE_DIR/state"
ENV_FILE="$BASE_DIR/.env"
BACKUP_DIR="$BASE_DIR/backups"
SERVICE_DIR="/home/ec2-user/.config/systemd/user"
SERVICE_FILE="$SERVICE_DIR/quant.service"
IMAGE="localhost/quant:latest"
LOCK_FILE="$HOME/.quant-deploy.lock"
GROWW_INSTRUMENTS_URL="${GROWW_INSTRUMENTS_URL:-https://growwapi-assets.groww.in/instruments/instrument.csv}"
REMOTE_STATE_BACKUP="${REMOTE_STATE_BACKUP:-0}"
START_SERVICE="${START_SERVICE:-1}"
LIVE_ARGS="${LIVE_ARGS:-}"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "ERROR: Another quant deployment is already running."
  exit 1
fi

if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: Environment file not found: $ENV_FILE"
  exit 1
fi

cd "$PROJECT_DIR"

if [[ -d .git ]]; then
  git pull --ff-only
fi

mkdir -p "$STATE_DIR/data/raw" "$STATE_DIR/models" "$STATE_DIR/logs" "$SERVICE_DIR"

refresh_groww_instruments() {
  local dst="$STATE_DIR/data/raw/groww_instruments.csv"
  local tmp="$dst.tmp"
  if [[ -f "$dst" && -n "$(find "$dst" -mmin -1440 -print -quit 2>/dev/null)" ]]; then
    return 0
  fi
  echo "==> Refreshing Groww instrument master: $dst"
  rm -f "$tmp"
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$GROWW_INSTRUMENTS_URL" -o "$tmp"
  else
    python3 - "$GROWW_INSTRUMENTS_URL" "$tmp" <<'PY'
import sys
from urllib.request import Request, urlopen
url, tmp = sys.argv[1], sys.argv[2]
req = Request(url, headers={"User-Agent": "quant-option-bot/1.0"})
with urlopen(req, timeout=45) as resp:
    data = resp.read()
open(tmp, "wb").write(data)
PY
  fi
  if [[ ! -s "$tmp" ]] || ! head -c 512 "$tmp" | grep -qi 'trading_symbol'; then
    rm -f "$tmp"
    echo "ERROR: downloaded Groww instrument master is invalid."
    exit 1
  fi
  mv "$tmp" "$dst"
}

if [[ "$REMOTE_STATE_BACKUP" == "1" ]] && { compgen -G "$STATE_DIR/data/*" >/dev/null || compgen -G "$STATE_DIR/models/*" >/dev/null; }; then
  mkdir -p "$BACKUP_DIR"
  TS="$(date -u +%Y%m%d-%H%M%S)"
  tar -czf "$BACKUP_DIR/quant_state_before_restart_$TS.tar.gz" -C "$BASE_DIR" state
  echo "==> Backed up persistent state to $BACKUP_DIR/quant_state_before_restart_$TS.tar.gz"
else
  echo "==> VM tar backup skipped. Run scripts/aws_pull_state_backup.ps1 on the laptop for local backups."
fi

if [[ ! -f "$STATE_DIR/data/raw/NSE_FO_contract_29052026.csv.gz" && -f assets/NSE_FO_contract_29052026.csv.gz ]]; then
  cp assets/NSE_FO_contract_29052026.csv.gz "$STATE_DIR/data/raw/NSE_FO_contract_29052026.csv.gz"
fi

refresh_groww_instruments

if [[ ! -f .dockerignore ]]; then
  cat > .dockerignore <<'EOD'
.env
.env.*
.git
__pycache__/
.pytest_cache/
.mypy_cache/
.ruff_cache/
*.pyc
logs/
data/
models/
state/
backups/
research_output/
EOD
fi

write_service_file() {
  cat > "$SERVICE_FILE" <<EOF
[Unit]
Description=Quant Trading Bot - Rootless Podman
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$PROJECT_DIR

ExecStartPre=/usr/bin/mkdir -p $STATE_DIR/data $STATE_DIR/models $STATE_DIR/logs
ExecStartPre=-/usr/bin/podman rm -f quant

ExecStart=/usr/bin/podman run \\
  --name quant \\
  --replace \\
  --stop-timeout=30 \\
  --dns 172.31.0.2 \\
  --dns 1.1.1.1 \\
  --dns 8.8.8.8 \\
  --env-file $ENV_FILE \\
  --env TZ=Asia/Kolkata \\
  -v $STATE_DIR/data:/app/data:z \\
  -v $STATE_DIR/models:/app/models:z \\
  -v $STATE_DIR/logs:/app/logs:z \\
  $IMAGE \\
  python -m bot.cli live-loop $LIVE_ARGS

ExecStop=/usr/bin/podman stop -t 30 quant
Restart=always
RestartSec=10

[Install]
WantedBy=default.target
EOF
}

if [[ -f deploy/quant.service ]]; then
  cp deploy/quant.service "$SERVICE_FILE"
  sed -i \
    -e "s#WorkingDirectory=/home/ec2-user/quant/Quant#WorkingDirectory=$PROJECT_DIR#g" \
    -e "s#/home/ec2-user/quant/.env#$ENV_FILE#g" \
    -e "s#/home/ec2-user/quant/state#$STATE_DIR#g" \
    "$SERVICE_FILE"
  if [[ -n "$LIVE_ARGS" ]]; then
    sed -i -e "s#python -m bot.cli live-loop\$#python -m bot.cli live-loop $LIVE_ARGS#g" "$SERVICE_FILE"
  fi
else
  write_service_file
fi

podman build -t "$IMAGE" .

systemctl --user stop quant.service >/dev/null 2>&1 || true
podman rm -f quant >/dev/null 2>&1 || true
podman run --rm \
  --env-file "$ENV_FILE" \
  --env TZ=Asia/Kolkata \
  -v "$STATE_DIR/data:/app/data:z" \
  -v "$STATE_DIR/models:/app/models:z" \
  -v "$STATE_DIR/logs:/app/logs:z" \
  "$IMAGE" \
  python -m bot.cli repair-db

systemctl --user daemon-reload
systemctl --user reset-failed quant.service
if [[ "$START_SERVICE" == "1" ]]; then
  systemctl --user restart quant.service
else
  systemctl --user stop quant.service >/dev/null 2>&1 || true
fi

echo
echo "Service status:"
systemctl --user status quant.service --no-pager -l

echo
if [[ "$START_SERVICE" == "1" ]]; then
  echo "Following container logs:"
  podman logs -f quant
else
  echo "quant.service installed but not started because START_SERVICE=0"
  echo "Start later with: systemctl --user start quant.service"
fi

#!/usr/bin/env bash
set -Eeuo pipefail

BASE_DIR="/home/ec2-user/quant"
PROJECT_DIR="$BASE_DIR/Quant"
STATE_DIR="$BASE_DIR/state"
ENV_FILE="$BASE_DIR/.env"
BACKUP_DIR="$BASE_DIR/backups"
IMAGE="localhost/quant:latest"
LOCK_FILE="$HOME/.quant-deploy.lock"
GROWW_INSTRUMENTS_URL="${GROWW_INSTRUMENTS_URL:-https://growwapi-assets.groww.in/instruments/instrument.csv}"
REMOTE_STATE_BACKUP="${REMOTE_STATE_BACKUP:-0}"

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

mkdir -p "$STATE_DIR/data/raw" "$STATE_DIR/models" "$STATE_DIR/logs"

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

podman build -t "$IMAGE" .

systemctl --user stop quant.service >/dev/null 2>&1 || true
podman rm -f quant >/dev/null 2>&1 || true
podman run --rm \
  --env-file "$ENV_FILE" \
  -v "$STATE_DIR/data:/app/data:Z" \
  -v "$STATE_DIR/models:/app/models:Z" \
  -v "$STATE_DIR/logs:/app/logs:Z" \
  "$IMAGE" \
  python -m bot.cli repair-db

systemctl --user daemon-reload
systemctl --user reset-failed quant.service
systemctl --user restart quant.service

echo
echo "Service status:"
systemctl --user status quant.service --no-pager -l

echo
echo "Following container logs:"
podman logs -f quant

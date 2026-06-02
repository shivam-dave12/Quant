#!/usr/bin/env bash
set -Eeuo pipefail

BASE_DIR="/home/ec2-user/quant"
PROJECT_DIR="$BASE_DIR/Quant"
STATE_DIR="$BASE_DIR/state"
ENV_FILE="$BASE_DIR/.env"
BACKUP_DIR="$BASE_DIR/backups"
SERVICE_DIR="/home/ec2-user/.config/systemd/user"
SERVICE_FILE="$SERVICE_DIR/quant.service"
IMAGE="localhost/quant:latest"
GROWW_INSTRUMENTS_URL="${GROWW_INSTRUMENTS_URL:-https://growwapi-assets.groww.in/instruments/instrument.csv}"

echo "==> Project: $PROJECT_DIR"
cd "$PROJECT_DIR"

mkdir -p "$STATE_DIR/data/raw" "$STATE_DIR/models" "$STATE_DIR/logs" "$BACKUP_DIR" "$SERVICE_DIR"

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

if [[ ! -f "$ENV_FILE" ]]; then
  cat > "$ENV_FILE" <<'ENVEOF'
GROWW_TOTP_TOKEN=replace_me
GROWW_TOTP_SECRET=replace_me
ENVEOF
  chmod 600 "$ENV_FILE"
  echo "Created $ENV_FILE. Edit it with real Groww TOTP credentials, then rerun this script."
  exit 1
fi

if grep -q 'replace_me' "$ENV_FILE"; then
  echo "ERROR: $ENV_FILE still contains replace_me. Put real Groww credentials first."
  exit 1
fi

systemctl --user stop quant.service >/dev/null 2>&1 || true
podman rm -f quant >/dev/null 2>&1 || true

copy_existing_state() {
  local src="$1"
  local dst="$2"
  if [[ ! -d "$src" ]]; then
    return 0
  fi
  shopt -s nullglob dotglob
  local item
  for item in "$src"/*; do
    cp -a -n "$item" "$dst"/
  done
  shopt -u nullglob dotglob
}

copy_existing_state "$PROJECT_DIR/data" "$STATE_DIR/data"
copy_existing_state "$PROJECT_DIR/models" "$STATE_DIR/models"
copy_existing_state "$PROJECT_DIR/logs" "$STATE_DIR/logs"

if compgen -G "$STATE_DIR/data/*" >/dev/null || compgen -G "$STATE_DIR/models/*" >/dev/null; then
  TS="$(date -u +%Y%m%d-%H%M%S)"
  tar -czf "$BACKUP_DIR/quant_state_before_deploy_$TS.tar.gz" -C "$BASE_DIR" state
  echo "==> Backed up persistent state to $BACKUP_DIR/quant_state_before_deploy_$TS.tar.gz"
fi

if [[ ! -f "$STATE_DIR/data/raw/NSE_FO_contract_29052026.csv.gz" && -f assets/NSE_FO_contract_29052026.csv.gz ]]; then
  cp assets/NSE_FO_contract_29052026.csv.gz "$STATE_DIR/data/raw/NSE_FO_contract_29052026.csv.gz"
fi

if [[ ! -f assets/NSE_FO_contract_29052026.csv.gz && ! -f "$STATE_DIR/data/raw/NSE_FO_contract_29052026.csv.gz" ]]; then
  echo "ERROR: missing NSE_FO_contract_29052026.csv.gz in assets/ or $STATE_DIR/data/raw/."
  exit 1
fi

refresh_groww_instruments

cp deploy/quant.service "$SERVICE_FILE"

podman build -t "$IMAGE" .

podman run --rm \
  --env-file "$ENV_FILE" \
  -v "$STATE_DIR/data:/app/data:Z" \
  -v "$STATE_DIR/models:/app/models:Z" \
  -v "$STATE_DIR/logs:/app/logs:Z" \
  "$IMAGE" \
  python -m bot.cli repair-db

systemctl --user daemon-reload
systemctl --user enable quant.service
systemctl --user restart quant.service

loginctl enable-linger ec2-user || true

echo "==> Started quant.service"
systemctl --user --no-pager status quant.service || true
echo "==> Logs: podman logs -f quant"

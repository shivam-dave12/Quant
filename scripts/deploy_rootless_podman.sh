#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="/home/ec2-user/quant/Quant"
ENV_FILE="/home/ec2-user/quant/.env"
SERVICE_DIR="/home/ec2-user/.config/systemd/user"
SERVICE_FILE="$SERVICE_DIR/quant.service"
IMAGE="localhost/quant:latest"

echo "==> Project: $PROJECT_DIR"
cd "$PROJECT_DIR"

mkdir -p data/raw data models logs "$SERVICE_DIR"

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

# Keep an operator-visible copy in mounted data/raw as well, but the image also
# contains assets/NSE_FO_contract_29052026.csv.gz so bind mounts cannot hide it.
if [[ ! -f data/raw/NSE_FO_contract_29052026.csv.gz && -f assets/NSE_FO_contract_29052026.csv.gz ]]; then
  cp assets/NSE_FO_contract_29052026.csv.gz data/raw/NSE_FO_contract_29052026.csv.gz
fi

if [[ ! -f assets/NSE_FO_contract_29052026.csv.gz && ! -f data/raw/NSE_FO_contract_29052026.csv.gz ]]; then
  echo "ERROR: missing NSE_FO_contract_29052026.csv.gz in assets/ or data/raw/."
  exit 1
fi

cp deploy/quant.service "$SERVICE_FILE"

podman build -t "$IMAGE" .

systemctl --user daemon-reload
systemctl --user enable quant.service
systemctl --user restart quant.service

# Make the user service continue after logout.
loginctl enable-linger ec2-user || true

echo "==> Started quant.service"
systemctl --user --no-pager status quant.service || true
echo "==> Logs: podman logs -f quant"

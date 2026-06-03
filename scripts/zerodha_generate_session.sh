#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-"$PROJECT_DIR/../.env"}"
IMAGE_NAME="${IMAGE_NAME:-localhost/quant:latest}"
REQUEST_TOKEN="${1:-${ZERODHA_REQUEST_TOKEN:-}}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: env file not found: $ENV_FILE" >&2
  exit 1
fi
if [[ -z "$REQUEST_TOKEN" ]]; then
  echo "Usage: $0 <zerodha_request_token>" >&2
  exit 2
fi

podman run --rm \
  --env-file "$ENV_FILE" \
  -v "$ENV_FILE:/app/.env:Z" \
  "$IMAGE_NAME" \
  python -m bot.cli zerodha-generate-session \
    --request-token "$REQUEST_TOKEN" \
    --write-env \
    --env-file /app/.env

chown "$(id -u):$(id -g)" "$ENV_FILE"

systemctl --user restart quant.service
echo "Zerodha access token saved and quant.service restarted."

#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-"$PROJECT_DIR/../.env"}"
IMAGE_NAME="${IMAGE_NAME:-localhost/quant:latest}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: env file not found: $ENV_FILE" >&2
  exit 1
fi

podman run --rm \
  --env-file "$ENV_FILE" \
  "$IMAGE_NAME" \
  python -m bot.cli zerodha-login-url

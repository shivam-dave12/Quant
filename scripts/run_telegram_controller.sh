#!/usr/bin/env bash
set -Eeuo pipefail

REPO="${REPO:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
IMAGE="${IMAGE:-localhost/quant:latest}"
CONTAINER="${CONTAINER:-quant}"
ENV_FILE="${ENV_FILE:-${REPO}/.env}"

cd "${REPO}"

[[ -r "${ENV_FILE}" ]] || { echo "ERROR: readable .env not found: ${ENV_FILE}" >&2; exit 1; }
for key in TELEGRAM_BOT_TOKEN TELEGRAM_CHAT_ID; do
  grep -q "^${key}=" "${ENV_FILE}" || { echo "ERROR: ${key} missing from ${ENV_FILE}" >&2; exit 1; }
done

podman build -t "${IMAGE}" .

# Prove that Podman injects the required keys without showing values.
podman run --rm --env-file "${ENV_FILE}" --entrypoint python "${IMAGE}" -c \
  'import os, sys; required=("TELEGRAM_BOT_TOKEN","TELEGRAM_CHAT_ID"); missing=[k for k in required if not os.getenv(k)]; print({k: bool(os.getenv(k)) for k in required}); sys.exit(1 if missing else 0)'

podman rm -f "${CONTAINER}" >/dev/null 2>&1 || true
podman run -d \
  --name "${CONTAINER}" \
  --restart unless-stopped \
  --env-file "${ENV_FILE}" \
  -p 8088:8088 \
  "${IMAGE}"

podman logs -f "${CONTAINER}"

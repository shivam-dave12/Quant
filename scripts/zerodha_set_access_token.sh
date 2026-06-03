#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-"$PROJECT_DIR/../.env"}"
ACCESS_TOKEN="${1:-}"

if [[ "$ACCESS_TOKEN" == "-" ]]; then
  IFS= read -r ACCESS_TOKEN
fi
if [[ -z "$ACCESS_TOKEN" ]]; then
  echo "Usage: $0 <zerodha_access_token|->" >&2
  exit 2
fi
if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: env file not found: $ENV_FILE" >&2
  exit 1
fi

python3 - "$ENV_FILE" "$ACCESS_TOKEN" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
value = sys.argv[2].strip()
key = "ZERODHA_ACCESS_TOKEN"
lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
out = []
written = False
for line in lines:
    if line.startswith(f"{key}="):
        out.append(f"{key}={value}")
        written = True
    else:
        out.append(line)
if not written:
    out.append(f"{key}={value}")
path.write_text("\n".join(out).rstrip() + "\n", encoding="utf-8")
PY

chown "$(id -u):$(id -g)" "$ENV_FILE"
systemctl --user restart quant.service
echo "ZERODHA_ACCESS_TOKEN saved and quant.service restarted."

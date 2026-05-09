#!/usr/bin/env bash
set -euo pipefail
APP_DIR="${APP_DIR:-/app}"
cd "$APP_DIR"
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
export PLAYWRIGHT_BROWSERS_PATH="${PLAYWRIGHT_BROWSERS_PATH:-$APP_DIR/.ms-playwright}"
export HOME="${HOME:-$APP_DIR/.runtime-home}"
mkdir -p "$PLAYWRIGHT_BROWSERS_PATH" "$HOME" "$APP_DIR/data" "$APP_DIR/data/icici_debug"
chmod -R u+rwX,g+rwX "$PLAYWRIGHT_BROWSERS_PATH" "$HOME" "$APP_DIR/data" || true
python -m playwright install --with-deps chromium || python -m playwright install chromium
python scripts/live_preflight.py

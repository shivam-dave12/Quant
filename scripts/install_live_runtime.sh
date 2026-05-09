#!/usr/bin/env bash
set -euo pipefail
python -m pip install --upgrade pip
pip install -r requirements.txt
python -m playwright install chromium
echo "Live runtime dependencies installed."

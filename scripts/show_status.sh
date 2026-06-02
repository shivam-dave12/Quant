#!/usr/bin/env bash
set -euo pipefail
systemctl --user --no-pager status quant.service || true
podman ps -a --filter name=quant
podman logs --tail=200 quant || true

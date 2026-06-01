#!/usr/bin/env bash
set -euo pipefail
cd /home/ec2-user/quant/Quant
podman build -t localhost/quant:latest .
systemctl --user restart quant.service
podman logs -f quant

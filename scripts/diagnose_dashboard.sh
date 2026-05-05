#!/usr/bin/env bash
set -euo pipefail
DASHBOARD_URL="${DASHBOARD_URL:-http://127.0.0.1:8000}"
echo "== dashboard health =="
curl -fsS "$DASHBOARD_URL/api/health" || true; echo

echo "== dashboard diagnostics =="
curl -fsS "$DASHBOARD_URL/api/diagnostics" || true; echo

echo "== systemd dashboard =="
sudo systemctl status trading-dashboard --no-pager || true

echo "== systemd tail =="
sudo systemctl status trading-dashboard-tail --no-pager || true

echo "== tail logs =="
sudo journalctl -u trading-dashboard-tail -n 120 --no-pager || true

echo "== docker containers =="
docker ps --format 'table {{.ID}}\t{{.Names}}\t{{.Status}}' || true
if [[ -n "${BOT_CONTAINER:-}" ]]; then
  echo "== bot container log path =="
  docker inspect -f '{{.LogPath}}' "$BOT_CONTAINER" || true
fi

#!/usr/bin/env bash
set -euo pipefail
LOG_FILE="${1:-}"
if [[ -z "$LOG_FILE" || ! -f "$LOG_FILE" ]]; then
  echo "Usage: $0 /path/to/docker-json.log" >&2
  exit 1
fi
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT/dashboard/backend"
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt >/dev/null
python run.py &
BACKEND_PID=$!
sleep 2
python "$ROOT/dashboard/agents/log_tail_agent.py" --log "$LOG_FILE" --dashboard http://127.0.0.1:8000 --from-start &
TAIL_PID=$!
echo "Dashboard running at http://127.0.0.1:8000"
echo "Backend PID=$BACKEND_PID Tail PID=$TAIL_PID"
wait $BACKEND_PID

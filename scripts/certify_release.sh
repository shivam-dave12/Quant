#!/usr/bin/env bash
set -euo pipefail
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 NUMEXPR_NUM_THREADS=1 LIGHTGBM_NUM_THREADS=1
cd "$(dirname "${BASH_SOURCE[0]}")/.."
python -m compileall -q core adapters market_data intelligence portfolio execution research orchestration dashboard telegram main.py config.py tests/test_institutional_platform.py
python - <<'PY'
from pathlib import Path
import ast
import re
root = Path.cwd()
tests = sorted((root / "tests").glob("test_*.py"))
if [p.name for p in tests] != ["test_institutional_platform.py"]:
    raise SystemExit(f"Certification failed: expected one test module tests/test_institutional_platform.py, found {tests}")
removed = [
    "strategy", "agents", "aggregator", "risk", "exchanges", "watchdog.py", "runtime_shutdown_guard.py",
    "execution/order_manager.py", "execution/router.py", "execution/instrument_registry.py",
    "orchestration/multi_asset_bot.py", "orchestration/portfolio_manager.py",
    "core/candle.py", "core/instruments.py", "core/market_policy.py", "core/pnl.py", "core/types.py",
    "config_schema.py", "telegram/config.py",
]
remaining = [item for item in removed if (root / item).exists()]
if remaining:
    raise SystemExit(f"Certification failed: removed legacy/dead paths still exist: {remaining}")
pattern = re.compile(r"\b(?:fvg|mss|auction|breeze|icici)\b|quant_strategy|entry_engine|liquidity_map|tp_ladder|MultiAssetQuantBot|ExecutionRouter|order_manager|watchdog", re.I)
scan_dirs = ["core", "adapters", "market_data", "intelligence", "portfolio", "execution", "research", "orchestration", "dashboard", "telegram"]
paths = [root / "main.py"]
for directory in scan_dirs:
    paths.extend((root / directory).rglob("*.py"))
findings = {str(path.relative_to(root)): pattern.findall(path.read_text(encoding="utf-8")) for path in paths if pattern.search(path.read_text(encoding="utf-8"))}
if findings:
    raise SystemExit(f"Certification failed: active runtime contains removed strategy/broker names: {findings}")
# Simple deterministic dead-import gate for executable modules. Package exports and root config facade are intentional APIs.
unused_imports = {}
for path in paths:
    if path.name == "__init__.py":
        continue
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imported = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend((alias.asname or alias.name.split(".")[0], node.lineno) for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.extend((alias.asname or alias.name, node.lineno) for alias in node.names if alias.name != "*")
    used = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}
    missing = [(name, line) for name, line in imported if name not in used and name != "annotations"]
    if missing:
        unused_imports[str(path.relative_to(root))] = missing
if unused_imports:
    raise SystemExit(f"Certification failed: unused imports in active executable code: {unused_imports}")
print("Architecture audit passed: legacy/dead runtime absent, active imports used, one certification test module only.")
PY
PYTEST_LOG="$(mktemp /tmp/institutional_pytest.XXXXXX.log)"
trap 'rm -f "${PYTEST_LOG}"' EXIT
python -m pytest -q tests/test_institutional_platform.py -W error >"${PYTEST_LOG}" 2>&1
cat "${PYTEST_LOG}"
python main.py --status >/tmp/institutional_platform_status.json
python -m telegram.controller --preflight-status >/tmp/telegram_controller_preflight_status.json
cat /tmp/institutional_platform_status.json
cat /tmp/telegram_controller_preflight_status.json
echo "Release certification passed: compile clean, deletion audit clean, active-import gate clean, one test module, direct and Telegram preflight fail-closed."

#!/usr/bin/env python3
from __future__ import annotations
import os, sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
import config

def line(kind, label, value): print(f"[{kind}] {label}: {value}")
print("=== Quant Bot Live Preflight ===")
line("OK", "env", config.live_ordering_config_summary())
ready, reason = config.assert_live_ordering_ready()
line("OK" if ready else "WARN", "live_ordering", reason)
browser_path = Path(os.getenv("PLAYWRIGHT_BROWSERS_PATH", str(ROOT / ".ms-playwright"))).expanduser()
try:
    browser_path.mkdir(parents=True, exist_ok=True)
    probe = browser_path / ".write_test"; probe.write_text("ok"); probe.unlink(missing_ok=True)
    line("OK", "playwright_path", f"{browser_path} writable")
except Exception as exc:
    line("FAIL", "playwright_path", f"{browser_path} not writable: {exc}"); raise SystemExit(2)
if getattr(config, "ICICI_ENABLED", False) or getattr(config, "ICICI_AUTH_REQUIRED_FOR_DETAILS", False):
    missing=[n for n in ("BREEZE_API_KEY","BREEZE_SECRET_KEY","ICICI_CLIENT_ID","ICICI_PASSWORD") if not os.getenv(n)]
    if missing:
        line("FAIL", "icici_credentials", "missing " + ",".join(missing)); raise SystemExit(2)
    line("OK", "icici_credentials", "required fields present")
try:
    from playwright.sync_api import sync_playwright  # noqa
    line("OK", "playwright_python", "installed")
except Exception as exc:
    line("FAIL", "playwright_python", str(exc)); raise SystemExit(2)
print("Preflight complete.")

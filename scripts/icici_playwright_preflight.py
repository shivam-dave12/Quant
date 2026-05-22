"""ICICI Playwright runtime preflight.

Run this inside the same container/user that starts the Telegram bot. It does
not log into ICICI or consume an OTP; it only proves that Playwright Chromium can
launch with the current browser cache, HOME and Linux shared libraries.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> int:
    from exchanges.icici.token_generator import assert_playwright_chromium_runtime_ready

    details = assert_playwright_chromium_runtime_ready()
    print(json.dumps({"ok": True, **details}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

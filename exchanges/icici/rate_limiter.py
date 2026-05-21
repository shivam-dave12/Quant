"""Shared ICICI/Breeze API pacing utilities.

Breeze rate limits are account-wide, so ICICI data managers must not burst
historical/quote calls concurrently during multi-desk startup.  This module
provides a process-wide throttle; it is intentionally simple and blocking.
"""
from __future__ import annotations

import threading
import time
from typing import Any

try:
    import config
except Exception:  # pragma: no cover
    config = None  # type: ignore

_lock = threading.Lock()
_next_allowed_ts = 0.0


def _cfg(name: str, default: Any) -> Any:
    if config is None:
        return default
    return getattr(config, name, default)


def breeze_throttle(reason: str = "breeze") -> None:
    """Apply a global minimum gap between Breeze protected endpoint calls."""
    global _next_allowed_ts
    enabled = bool(_cfg("ICICI_BREEZE_THROTTLE_ENABLED", True))
    if not enabled:
        return
    min_gap = float(_cfg("ICICI_BREEZE_MIN_CALL_GAP_SEC", 0.35))
    min_gap = max(0.0, min_gap)
    if min_gap <= 0:
        return
    with _lock:
        now = time.monotonic()
        wait = _next_allowed_ts - now
        if wait > 0:
            time.sleep(wait)
            now = time.monotonic()
        _next_allowed_ts = now + min_gap

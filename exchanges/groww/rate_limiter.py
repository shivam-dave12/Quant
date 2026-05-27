"""Conservative Groww REST pacing helpers."""

from __future__ import annotations

import threading
import time

try:
    import config
except Exception:  # pragma: no cover
    config = None  # type: ignore


_lock = threading.RLock()
_last_by_key: dict[str, float] = {}


def _gap() -> float:
    return max(0.05, float(getattr(config, "GROWW_MIN_CALL_GAP_SEC", 0.25) if config is not None else 0.25))


def groww_throttle(key: str = "default") -> None:
    """Pace SDK calls below Groww's documented per-second/minute ceilings."""
    name = str(key or "default")
    while True:
        with _lock:
            now = time.time()
            last = float(_last_by_key.get(name, 0.0) or 0.0)
            wait = _gap() - (now - last)
            if wait <= 0:
                _last_by_key[name] = now
                return
        time.sleep(wait)

"""In-process coordinator for the once-per-day ICICI Breeze login flow."""
from __future__ import annotations

import threading
import time
from typing import Dict

_LOCK = threading.RLock()
_STATE: Dict[str, object] = {"day": "", "status": "", "source": "", "updated_at": 0.0}
_LOGIN_BLOCKING = {"running", "valid"}
_NOTICE_BLOCKING = {"running", "valid", "notified_missing", "passed_notice"}


def claim_login_attempt(day_key: str, source: str) -> bool:
    """Return True when this caller owns today's actual token generation."""
    day = str(day_key or "")
    with _LOCK:
        if _STATE.get("day") == day and str(_STATE.get("status") or "") in _LOGIN_BLOCKING:
            return False
        _STATE.update({"day": day, "status": "running", "source": str(source or ""), "updated_at": time.time()})
        return True


def claim_notice(day_key: str, source: str, status: str) -> bool:
    """Return True when this caller should send today's premarket notice."""
    day = str(day_key or "")
    with _LOCK:
        if _STATE.get("day") == day and str(_STATE.get("status") or "") in _NOTICE_BLOCKING:
            return False
        _STATE.update({"day": day, "status": str(status or "notified_missing"), "source": str(source or ""), "updated_at": time.time()})
        return True


def mark_valid(day_key: str, source: str = "") -> None:
    with _LOCK:
        _STATE.update({"day": str(day_key or ""), "status": "valid", "source": str(source or ""), "updated_at": time.time()})


def mark_failed(day_key: str, source: str = "") -> None:
    with _LOCK:
        _STATE.update({"day": str(day_key or ""), "status": "failed", "source": str(source or ""), "updated_at": time.time()})


def current_state() -> Dict[str, object]:
    with _LOCK:
        return dict(_STATE)


def reset_for_tests() -> None:
    with _LOCK:
        _STATE.update({"day": "", "status": "", "source": "", "updated_at": 0.0})

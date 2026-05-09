"""Append-only audit logging for agent decisions."""

from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict

try:
    from core.redaction import redact_sensitive
except Exception:  # pragma: no cover
    def redact_sensitive(value):  # type: ignore
        return value


class AuditLog:
    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self._lock = threading.RLock()

    def write(self, event_type: str, payload: Dict[str, Any]) -> None:
        row = {
            "ts": time.time(),
            "event_type": str(event_type),
            "payload": redact_sensitive(payload),
        }
        text = json.dumps(row, sort_keys=True, separators=(",", ":"), default=str)
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8") as fh:
                fh.write(text + os.linesep)

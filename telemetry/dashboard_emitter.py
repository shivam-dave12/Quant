from __future__ import annotations

import queue
import threading
import time
from typing import Any, Optional

import requests

try:
    from core.redaction import redact_sensitive
except Exception:  # pragma: no cover
    def redact_sensitive(x): return x


class DashboardEmitter:
    """Non-blocking process-shared dashboard event emitter.

    Trading logic must never wait for the dashboard.  Events are queued into a
    bounded in-memory queue and a single daemon worker posts them to the local
    dashboard backend.  On overload/offline dashboard, routine events are dropped.
    """
    _shared: Optional["DashboardEmitter"] = None
    _shared_lock = threading.Lock()

    def __init__(self, url: str, enabled: bool = True, max_queue: int = 2000, timeout: float = 0.8) -> None:
        self.url = str(url or "http://127.0.0.1:8000").rstrip("/")
        self.enabled = bool(enabled)
        self.timeout = float(timeout)
        self._q: "queue.Queue[Optional[dict[str, Any]]]" = queue.Queue(maxsize=max(10, int(max_queue)))
        self._started = False
        self._lock = threading.Lock()
        self._dropped = 0

    @classmethod
    def from_config(cls) -> "DashboardEmitter":
        import config
        with cls._shared_lock:
            if cls._shared is None:
                enabled = str(getattr(config, "DASHBOARD_ENABLED", "true")).lower() in {"1", "true", "yes", "on"}
                url = getattr(config, "DASHBOARD_URL", "http://127.0.0.1:8000")
                max_q = int(getattr(config, "DASHBOARD_QUEUE_MAX", 2000))
                timeout = float(getattr(config, "DASHBOARD_TIMEOUT_SEC", 0.8))
                cls._shared = cls(url=url, enabled=enabled, max_queue=max_q, timeout=timeout)
            return cls._shared

    @property
    def dropped(self) -> int:
        return self._dropped

    def start(self) -> None:
        if not self.enabled or self._started:
            return
        with self._lock:
            if self._started:
                return
            threading.Thread(target=self._worker, name="dashboard-emitter", daemon=True).start()
            self._started = True

    def emit(self, event: dict[str, Any], *, critical: bool = False) -> bool:
        if not self.enabled:
            return False
        self.start()
        ev = redact_sensitive(dict(event))
        ev.setdefault("ts", time.time())
        ev.setdefault("source", "direct")
        try:
            if critical:
                self._q.put_nowait(ev)
            else:
                self._q.put_nowait(ev)
            return True
        except queue.Full:
            self._dropped += 1
            if critical:
                try:
                    _ = self._q.get_nowait()
                    self._q.put_nowait(ev)
                    return True
                except Exception:
                    return False
            return False

    def _worker(self) -> None:
        while True:
            ev = self._q.get()
            if ev is None:
                return
            try:
                requests.post(f"{self.url}/api/events", json=ev, timeout=self.timeout)
            except Exception:
                pass
            finally:
                self._q.task_done()

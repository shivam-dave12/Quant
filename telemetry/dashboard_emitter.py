"""
telemetry/dashboard_emitter.py — local dashboard event bridge
================================================================

Small, dependency-light HTTP emitter used by the trading bot to publish
structured runtime events to the local dashboard.  It never raises into the
trading path: dashboard failures are telemetry failures, not trade-decision
inputs.
"""
from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional

import config

logger = logging.getLogger(__name__)


def _bool_cfg(name: str, default: bool = False) -> bool:
    value = getattr(config, name, default)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "enabled"}
    return bool(value)


def dashboard_enabled() -> bool:
    return _bool_cfg("DASHBOARD_ENABLED", False)


@dataclass
class DashboardEmitter:
    base_url: str
    timeout: float = 1.5
    enabled: bool = False
    suppress_log_sec: float = 60.0
    _last_error_log: float = 0.0

    @classmethod
    def from_config(cls) -> "DashboardEmitter":
        return cls(
            base_url=str(getattr(config, "DASHBOARD_URL", "http://127.0.0.1:8000")).rstrip("/"),
            timeout=float(getattr(config, "DASHBOARD_EVENT_TIMEOUT_SEC", 1.5)),
            enabled=dashboard_enabled(),
        )

    def send(self, event: Dict[str, Any]) -> bool:
        if not self.enabled:
            return False
        try:
            payload = dict(event)
            payload.setdefault("ts", time.time())
            data = json.dumps(payload, default=str).encode("utf-8")
            req = urllib.request.Request(
                f"{self.base_url}/api/events",
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return 200 <= int(resp.status) < 300
        except Exception as exc:
            now = time.time()
            if now - self._last_error_log >= self.suppress_log_sec:
                self._last_error_log = now
                logger.warning("Dashboard telemetry send failed: %s", exc)
            return False

    def heartbeat(self, **kwargs: Any) -> bool:
        return self.send({"type": "heartbeat", **kwargs})

    def scan_update(self, **kwargs: Any) -> bool:
        return self.send({"type": "scan_update", **kwargs})

    def candidate_deferred(self, **kwargs: Any) -> bool:
        return self.send({"type": "candidate_deferred", **kwargs})

    def position_opened(self, **kwargs: Any) -> bool:
        return self.send({"type": "position_opened", **kwargs})

    def position_update(self, **kwargs: Any) -> bool:
        return self.send({"type": "position_update", **kwargs})

    def position_closed(self, **kwargs: Any) -> bool:
        return self.send({"type": "position_closed", **kwargs})

    def alert(self, **kwargs: Any) -> bool:
        return self.send({"type": "alert", **kwargs})


_EMITTER: Optional[DashboardEmitter] = None


def get_dashboard_emitter() -> DashboardEmitter:
    global _EMITTER
    if _EMITTER is None:
        _EMITTER = DashboardEmitter.from_config()
    return _EMITTER


def reset_dashboard_emitter_for_tests() -> None:
    global _EMITTER
    _EMITTER = None

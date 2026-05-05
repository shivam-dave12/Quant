from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests


@dataclass
class DashboardEmitter:
    base_url: str = "http://127.0.0.1:8000"
    timeout: float = 2.0

    def send(self, event: dict[str, Any]) -> bool:
        try:
            r = requests.post(f"{self.base_url}/api/events", json=event, timeout=self.timeout)
            r.raise_for_status()
            return True
        except Exception:
            return False

    def heartbeat(self, **kwargs: Any) -> bool:
        return self.send({"type": "heartbeat", **kwargs})

    def scan_update(self, **kwargs: Any) -> bool:
        return self.send({"type": "scan_update", **kwargs})

    def candidate_deferred(self, **kwargs: Any) -> bool:
        return self.send({"type": "candidate_deferred", **kwargs})

    def candidate_approved(self, **kwargs: Any) -> bool:
        return self.send({"type": "candidate_approved", **kwargs})

    def position_opened(self, **kwargs: Any) -> bool:
        return self.send({"type": "position_opened", **kwargs})

    def position_update(self, **kwargs: Any) -> bool:
        return self.send({"type": "position_update", **kwargs})

    def position_closed(self, **kwargs: Any) -> bool:
        return self.send({"type": "position_closed", **kwargs})

    def alert(self, **kwargs: Any) -> bool:
        return self.send({"type": "alert", **kwargs})

"""Structured telemetry and alert fan-out; never emits invented zero fields."""
from __future__ import annotations
from dataclasses import asdict, is_dataclass
import logging
from typing import Any, Callable

class Observability:
    def __init__(self, alert_sink: Callable[[str, dict[str, Any]], None] | None = None) -> None:
        self.logger = logging.getLogger("institutional_platform")
        self.alert_sink = alert_sink
    def event(self, name: str, payload: Any, level: int = logging.INFO) -> None:
        data = asdict(payload) if is_dataclass(payload) else dict(payload)
        self.logger.log(level, "%s | %s", name, data)
        if level >= logging.ERROR and self.alert_sink:
            self.alert_sink(name, data)
    def critical(self, name: str, payload: Any) -> None:
        self.event(name, payload, logging.CRITICAL)

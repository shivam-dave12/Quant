"""Venue-specific feed quality using mandatory validity gates and empirical latency quality."""
from __future__ import annotations
from collections import deque
import numpy as np
from core.identifiers import FeedHealth

class FeedHealthMonitor:
    def __init__(self, minimum_samples: int = 20, history_size: int = 500) -> None:
        self.minimum_samples = minimum_samples; self.history_size = history_size
        self._latency_ms: dict[str, deque[float]] = {}
    def assess(self, venue: str, *, connected: bool, heartbeat_ok: bool, sequence_valid: bool,
               snapshot_ready: bool, exchange_timestamp_available: bool, latency_ms: float | None,
               no_change_heartbeat_valid: bool = False) -> FeedHealth:
        gates = {"DISCONNECTED": connected, "HEARTBEAT_FAILED": heartbeat_ok, "SEQUENCE_INVALID": sequence_valid, "SNAPSHOT_MISSING": snapshot_ready}
        for reason, valid in gates.items():
            if not valid:
                return FeedHealth(connected, heartbeat_ok, sequence_valid, snapshot_ready, exchange_timestamp_available, None, no_change_heartbeat_valid, 0.0, reason)
        history = self._latency_ms.setdefault(venue, deque(maxlen=self.history_size))
        z = None; latency_quality = 1.0
        if latency_ms is not None:
            if len(history) >= self.minimum_samples:
                mean, sd = float(np.mean(history)), float(np.std(history))
                z = 0.0 if sd <= 1e-12 else (latency_ms - mean) / sd
                latency_quality = float(np.exp(-max(0.0, z - 1.0) / 3.0))
            history.append(float(latency_ms))
        elif not no_change_heartbeat_valid:
            latency_quality = 0.70
        timestamp_quality = 1.0 if exchange_timestamp_available else 0.85
        quality = float(max(0.0, min(1.0, latency_quality * timestamp_quality)))
        return FeedHealth(True, True, True, True, exchange_timestamp_available, z, no_change_heartbeat_valid, quality, "OK" if quality >= 0.99 else "DEGRADED")

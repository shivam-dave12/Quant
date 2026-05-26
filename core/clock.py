"""Injectable UTC nanosecond clock for deterministic decisions and replay."""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
import time

class Clock:
    def now_ns(self) -> int:
        return time.time_ns()
    def now_utc(self) -> datetime:
        return datetime.now(timezone.utc)

@dataclass
class FixedClock(Clock):
    value_ns: int
    def now_ns(self) -> int:
        return self.value_ns
    def advance_ns(self, value: int) -> None:
        self.value_ns += value

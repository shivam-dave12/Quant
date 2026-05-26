"""Time-windowed aggressive trade flow and order-flow imbalance in USD notionals."""
from __future__ import annotations
from collections import deque
from dataclasses import dataclass
from core.identifiers import InstrumentMapping, BookLevel
from market_data.normalizer import usd_notional

@dataclass(frozen=True)
class Trade:
    ts_ns: int
    price: float
    size: float
    aggressor_side: str

class TradeTape:
    WINDOWS = {"1s": 1_000_000_000, "10s": 10_000_000_000, "60s": 60_000_000_000}
    def __init__(self, mapping: InstrumentMapping) -> None:
        self.mapping = mapping; self.rows: deque[tuple[int, float]] = deque()
    def append(self, trade: Trade) -> None:
        sign = 1.0 if trade.aggressor_side.upper() == "BUY" else -1.0
        self.rows.append((trade.ts_ns, sign * usd_notional(BookLevel(trade.price, trade.size), self.mapping)))
    def tfi(self, now_ns: int) -> dict[str, float]:
        self._prune(now_ns)
        out: dict[str, float] = {}
        for name, window in self.WINDOWS.items():
            vals = [v for ts, v in self.rows if ts >= now_ns - window]
            absolute = sum(abs(v) for v in vals)
            out[name] = 0.0 if absolute <= 0 else sum(vals) / absolute
        return out
    def signed_notional(self, now_ns: int) -> dict[str, float]:
        self._prune(now_ns)
        return {name: sum(v for ts, v in self.rows if ts >= now_ns - window) for name, window in self.WINDOWS.items()}
    def _prune(self, now_ns: int) -> None:
        oldest = now_ns - max(self.WINDOWS.values())
        while self.rows and self.rows[0][0] < oldest:
            self.rows.popleft()

class OrderFlowTracker:
    WINDOWS = TradeTape.WINDOWS
    def __init__(self) -> None: self.rows: deque[tuple[int, float]] = deque()
    def append(self, ts_ns: int, bid_delta_usd: float, ask_delta_usd: float | None = None) -> None:
        self.rows.append((ts_ns, bid_delta_usd if ask_delta_usd is None else bid_delta_usd - ask_delta_usd))
    def ofi(self, now_ns: int) -> dict[str, float]:
        oldest = now_ns - max(self.WINDOWS.values())
        while self.rows and self.rows[0][0] < oldest: self.rows.popleft()
        return {name: sum(v for ts, v in self.rows if ts >= now_ns - window) for name, window in self.WINDOWS.items()}

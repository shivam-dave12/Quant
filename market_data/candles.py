"""Canonical OHLCV candles and deterministic return/volatility feature construction."""
from __future__ import annotations
from dataclasses import dataclass
from math import log
import numpy as np

@dataclass(frozen=True)
class Candle:
    ts_ns: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    timeframe: str
    def validate(self) -> None:
        if min(self.open, self.high, self.low, self.close) <= 0 or self.volume < 0 or self.low > self.high:
            raise ValueError("invalid candle")

class CandleSeries:
    def __init__(self, candles: list[Candle] | None = None) -> None:
        self.candles: list[Candle] = []
        for candle in candles or []:
            self.append(candle)
    def append(self, candle: Candle) -> None:
        candle.validate(); self.candles.append(candle)
    def returns(self) -> np.ndarray:
        return np.asarray([log(b.close / a.close) for a, b in zip(self.candles, self.candles[1:])], dtype=float)
    def realised_variance(self) -> float:
        rows = self.returns(); return float(np.sum(rows * rows)) if rows.size else 0.0
    def realised_volatility(self) -> float:
        return float(np.sqrt(self.realised_variance()))
    def directional_efficiency(self) -> float:
        if len(self.candles) < 2: return 0.0
        net = self.candles[-1].close - self.candles[0].close
        travel = sum(abs(b.close - a.close) for a, b in zip(self.candles, self.candles[1:]))
        return 0.0 if travel <= 0 else net / travel
    def average_range(self) -> float:
        return float(np.mean([c.high - c.low for c in self.candles])) if self.candles else 0.0
    def close_array(self) -> np.ndarray:
        return np.asarray([c.close for c in self.candles], dtype=float)

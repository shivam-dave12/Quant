"""
core/candle.py — Canonical Candle dataclass
============================================
Single definition shared by every data manager, strategy, and ICT engine.
Both exchange adapters produce this type; the aggregator consumes it.
"""

from __future__ import annotations
from dataclasses import dataclass


@dataclass
class Candle:
    timestamp: float   # Unix seconds (epoch)
    open:      float
    high:      float
    low:       float
    close:     float
    volume:    float

    # ── Candle shape helpers ──────────────────────────────────────────────────
    def is_bullish(self) -> bool:
        return self.close > self.open

    def is_bearish(self) -> bool:
        return self.close < self.open

    def body_size(self) -> float:
        return abs(self.close - self.open)

    def total_range(self) -> float:
        return self.high - self.low

    def upper_wick(self) -> float:
        return self.high - max(self.open, self.close)

    def lower_wick(self) -> float:
        return min(self.open, self.close) - self.low

    def body_percentage(self) -> float:
        r = self.total_range()
        return (self.body_size() / r) if r > 0 else 0.0

    def __repr__(self) -> str:
        return (
            f"Candle(t={int(self.timestamp)} "
            f"o={self.open:.2f} h={self.high:.2f} "
            f"l={self.low:.2f} c={self.close:.2f} v={self.volume:.4f})"
        )


# ── Compatibility shim ────────────────────────────────────────────────────────
# Lets the old strategy code use candles with either dict-key or attr access.

class CandleDict:
    """Wraps Candle to provide both dict-key and attribute access."""

    _ATTR_MAP = {
        "timestamp": "t",
        "open":      "o",
        "high":      "h",
        "low":       "l",
        "close":     "c",
        "volume":    "v",
    }

    def __init__(self, candle: Candle) -> None:
        self._candle = candle

    def __getitem__(self, key: str):
        # Auto-detect: if timestamp < 1e12 it's seconds, convert to ms.
        # If already ms, use as-is. Prevents nanosecond overflow when
        # exchanges return timestamps in different units.
        _raw_ts = self._candle.timestamp
        _ts_ms = int(_raw_ts * 1000) if _raw_ts < 1e12 else int(_raw_ts)
        mapping = {
            "t": _ts_ms,
            "o": self._candle.open,
            "h": self._candle.high,
            "l": self._candle.low,
            "c": self._candle.close,
            "v": self._candle.volume,
        }
        if key in mapping:
            return mapping[key]
        raise KeyError(f"Invalid candle key: {key!r}")

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        if name in self._ATTR_MAP:
            return self[self._ATTR_MAP[name]]
        return getattr(self._candle, name)

    def get(self, key: str, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __repr__(self) -> str:
        return (
            f"CandleDict(t={self['t']} o={self['o']} "
            f"h={self['h']} l={self['l']} c={self['c']} v={self['v']})"
        )


def wrap_candles(candles: list) -> list:
    """Convert a list of Candle objects to CandleDict wrappers."""
    return [CandleDict(c) for c in candles] if candles else []

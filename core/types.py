"""
core/types.py — Shared enumerations and lightweight value types
================================================================
Everything in here is a pure data type with zero external dependencies.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional


# ── Exchange identifiers ──────────────────────────────────────────────────────

class Exchange(str, Enum):
    DELTA      = "delta"
    COINSWITCH = "coinswitch"

    @classmethod
    def from_str(cls, value: str) -> "Exchange":
        v = value.strip().lower()
        if v in ("delta", "dx"):
            return cls.DELTA
        if v in ("coinswitch", "cs", "cs_pro"):
            return cls.COINSWITCH
        raise ValueError(
            f"Unknown exchange {value!r}. Valid values: 'delta', 'coinswitch'."
        )


# ── Order / cancellation results ─────────────────────────────────────────────

class CancelResult(str, Enum):
    SUCCESS        = "success"
    ALREADY_FILLED = "already_filled"
    NOT_FOUND      = "not_found"
    PARTIAL_FILL   = "partial_fill"
    ERROR          = "error"


# ── Normalised trade tick ─────────────────────────────────────────────────────

@dataclass
class TradeTick:
    """Exchange-normalised public trade record."""
    price:     float
    quantity:  float
    side:      str        # "buy" | "sell"
    timestamp: float      # Unix seconds
    exchange:  Exchange


# ── Normalised orderbook ──────────────────────────────────────────────────────

@dataclass
class OrderBook:
    """Exchange-normalised orderbook snapshot."""
    bids:      List[List[float]]   # [[price, qty], …] sorted descending
    asks:      List[List[float]]   # [[price, qty], …] sorted ascending
    exchange:  Exchange
    timestamp: float               # Unix seconds

    def best_bid(self) -> Optional[float]:
        return float(self.bids[0][0]) if self.bids else None

    def best_ask(self) -> Optional[float]:
        return float(self.asks[0][0]) if self.asks else None

    def mid_price(self) -> Optional[float]:
        bb, ba = self.best_bid(), self.best_ask()
        return (bb + ba) / 2.0 if bb and ba else None

    def spread_bps(self) -> float:
        bb, ba = self.best_bid(), self.best_ask()
        if not bb or not ba or bb <= 0:
            return 0.0
        return (ba - bb) / bb * 10_000.0

    def bid_volume(self, levels: int = 5) -> float:
        return sum(float(b[1]) for b in self.bids[:levels])

    def ask_volume(self, levels: int = 5) -> float:
        return sum(float(a[1]) for a in self.asks[:levels])

    def imbalance(self, levels: int = 5) -> float:
        """Returns [-1, +1]; positive = bid-heavy (bullish pressure)."""
        bv = self.bid_volume(levels)
        av = self.ask_volume(levels)
        total = bv + av
        return (bv - av) / total if total > 0 else 0.0

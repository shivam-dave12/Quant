from __future__ import annotations

from collections import deque
from typing import Any

import numpy as np

from .types import BookSnapshot, TradeTick


FEATURE_COLUMNS = [
    "spread_bps", "imbalance_1", "imbalance_5", "imbalance_10", "imbalance_20",
    "microprice_dev_bps", "book_slope_bid", "book_slope_ask", "ofi_l1",
    "trade_count_1s", "trade_count_5s", "signed_vol_1s", "signed_vol_5s", "signed_vol_15s",
    "trade_imbalance_1s", "trade_imbalance_5s", "trade_imbalance_15s",
    "cvd_60s", "large_trade_count_5s", "latency_ms", "regime_momentum_1h_14d",
]


def _safe_div(a: float, b: float) -> float:
    return float(a / b) if abs(b) > 1e-12 else 0.0


def _depth_imbalance(bids: list[tuple[float, float]], asks: list[tuple[float, float]], n: int) -> float:
    b = sum(s for _, s in bids[:n]); a = sum(s for _, s in asks[:n])
    return _safe_div(b - a, b + a)


class TradeFlowWindow:
    def __init__(self, max_seconds: int = 600) -> None:
        self.max_ns = int(max_seconds * 1e9)
        self.trades: deque[TradeTick] = deque()
        self.size_window: deque[float] = deque(maxlen=5000)

    def update(self, trade: TradeTick) -> None:
        self.trades.append(trade)
        self.size_window.append(abs(float(trade.size_contracts)))
        cutoff = trade.receive_ts_ns - self.max_ns
        while self.trades and self.trades[0].receive_ts_ns < cutoff:
            self.trades.popleft()

    def summary(self, now_ns: int, seconds: int) -> dict[str, float]:
        start = now_ns - int(seconds * 1e9)
        rows = [t for t in self.trades if t.receive_ts_ns >= start]
        if not rows:
            return {"count": 0.0, "volume": 0.0, "signed": 0.0, "imbalance": 0.0, "large_count": 0.0}
        vols = np.asarray([abs(t.size_contracts) for t in rows], dtype=float)
        signed = np.asarray([t.signed_size for t in rows], dtype=float)
        thresh = float(np.quantile(np.asarray(self.size_window, dtype=float), 0.95)) if len(self.size_window) >= 100 else float("inf")
        return {
            "count": float(len(rows)),
            "volume": float(vols.sum()),
            "signed": float(signed.sum()),
            "imbalance": _safe_div(float(signed.sum()), float(vols.sum())),
            "large_count": float((vols >= thresh).sum()) if np.isfinite(thresh) else 0.0,
        }


class RegimeState:
    """Receives closed 1h candles and tracks 14-day momentum as context."""

    def __init__(self, lookback_hours: int = 336) -> None:
        self.lookback = int(lookback_hours)
        self.closes: deque[float] = deque(maxlen=self.lookback + 2)
        self.value = 0.0

    def on_closed_1h_close(self, close: float) -> float:
        self.closes.append(float(close))
        if len(self.closes) > self.lookback and self.closes[0] > 0:
            self.value = float(np.log(self.closes[-1] / self.closes[0]))
        return self.value


class L2FeatureBuilder:
    def __init__(self) -> None:
        self.trade_flow = TradeFlowWindow()
        self.regime = RegimeState()
        self._last_bid1_size: float | None = None
        self._last_ask1_size: float | None = None
        self._last_bid1_price: float | None = None
        self._last_ask1_price: float | None = None
        self.last_book: BookSnapshot | None = None

    def on_trade(self, trade: TradeTick) -> None:
        self.trade_flow.update(trade)

    def on_1h_close(self, close: float) -> None:
        self.regime.on_closed_1h_close(close)

    def vector(self, book: BookSnapshot) -> dict[str, float]:
        bids, asks = book.bids, book.asks
        bid1, ask1 = book.best_bid, book.best_ask
        bid_size, ask_size = max(book.bid_size_1, 0.0), max(book.ask_size_1, 0.0)
        microprice = (ask1 * bid_size + bid1 * ask_size) / max(bid_size + ask_size, 1e-12)
        mid = book.mid
        ofi = 0.0
        if self._last_bid1_price is not None:
            if bid1 > self._last_bid1_price:
                ofi += bid_size
            elif bid1 == self._last_bid1_price:
                ofi += bid_size - float(self._last_bid1_size or 0.0)
            else:
                ofi -= float(self._last_bid1_size or 0.0)
            if ask1 < self._last_ask1_price:
                ofi -= ask_size
            elif ask1 == self._last_ask1_price:
                ofi -= ask_size - float(self._last_ask1_size or 0.0)
            else:
                ofi += float(self._last_ask1_size or 0.0)
        self._last_bid1_price, self._last_ask1_price = bid1, ask1
        self._last_bid1_size, self._last_ask1_size = bid_size, ask_size
        s1 = self.trade_flow.summary(book.receive_ts_ns, 1)
        s5 = self.trade_flow.summary(book.receive_ts_ns, 5)
        s15 = self.trade_flow.summary(book.receive_ts_ns, 15)
        s60 = self.trade_flow.summary(book.receive_ts_ns, 60)
        bid_slope = 0.0
        ask_slope = 0.0
        if len(bids) >= 10 and mid > 0:
            bid_slope = (bids[0][0] - bids[9][0]) / mid * 1e4 / 10.0
        if len(asks) >= 10 and mid > 0:
            ask_slope = (asks[9][0] - asks[0][0]) / mid * 1e4 / 10.0
        latency_ms = max(0.0, (book.receive_ts_ns - book.exchange_ts_ns) / 1e6)
        row = {
            "spread_bps": book.spread_bps,
            "imbalance_1": _depth_imbalance(bids, asks, 1),
            "imbalance_5": _depth_imbalance(bids, asks, 5),
            "imbalance_10": _depth_imbalance(bids, asks, 10),
            "imbalance_20": _depth_imbalance(bids, asks, 20),
            "microprice_dev_bps": (microprice / mid - 1.0) * 1e4 if mid > 0 else 0.0,
            "book_slope_bid": bid_slope,
            "book_slope_ask": ask_slope,
            "ofi_l1": _safe_div(ofi, bid_size + ask_size),
            "trade_count_1s": s1["count"],
            "trade_count_5s": s5["count"],
            "signed_vol_1s": s1["signed"],
            "signed_vol_5s": s5["signed"],
            "signed_vol_15s": s15["signed"],
            "trade_imbalance_1s": s1["imbalance"],
            "trade_imbalance_5s": s5["imbalance"],
            "trade_imbalance_15s": s15["imbalance"],
            "cvd_60s": s60["signed"],
            "large_trade_count_5s": s5["large_count"],
            "latency_ms": latency_ms,
            "regime_momentum_1h_14d": self.regime.value,
        }
        self.last_book = book
        return {k: float(row[k]) for k in FEATURE_COLUMNS}

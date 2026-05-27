"""Live microstructure primitives: OFI, TFI and venue latency baselines.

All flow values in this module are USD-notional values.  Queue flow and trade
flow are deliberately kept separate: OFI measures changes in displayed queues;
TFI measures aggressive executions from the trade tape.
"""
from __future__ import annotations

import math
import statistics
from collections import deque
from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

from market_data.normalizer import BookLevel, InstrumentMapping, parse_levels, usd_notional_depth


@dataclass(frozen=True)
class FlowSnapshot:
    ofi_usd_1s: float = 0.0
    ofi_usd_10s: float = 0.0
    ofi_usd_60s: float = 0.0
    tfi_usd_1s: float = 0.0
    tfi_usd_10s: float = 0.0
    tfi_usd_60s: float = 0.0

    def asdict(self) -> dict[str, float]:
        return {
            "ofi_usd_1s": self.ofi_usd_1s,
            "ofi_usd_10s": self.ofi_usd_10s,
            "ofi_usd_60s": self.ofi_usd_60s,
            "tfi_usd_1s": self.tfi_usd_1s,
            "tfi_usd_10s": self.tfi_usd_10s,
            "tfi_usd_60s": self.tfi_usd_60s,
        }


class ExponentialFlowAccumulator:
    """Exponentially decayed impulse accumulator at 1s, 10s and 60s horizons."""

    def __init__(self, half_lives_s: tuple[float, float, float] = (1.0, 10.0, 60.0)) -> None:
        self.half_lives_s = tuple(float(max(1e-6, h)) for h in half_lives_s)
        self.values = [0.0 for _ in self.half_lives_s]
        self.last_ts_s: float | None = None

    def update(self, impulse_usd: float, timestamp_s: float) -> tuple[float, float, float]:
        ts = float(timestamp_s)
        dt = 0.0 if self.last_ts_s is None else max(0.0, ts - self.last_ts_s)
        for i, half_life in enumerate(self.half_lives_s):
            decay = math.exp(-math.log(2.0) * dt / half_life) if dt > 0 else 1.0
            self.values[i] = self.values[i] * decay + float(impulse_usd)
        self.last_ts_s = ts
        return tuple(self.values[:3])  # type: ignore[return-value]

    def snapshot(self, timestamp_s: float | None = None) -> tuple[float, float, float]:
        if timestamp_s is not None and self.last_ts_s is not None:
            self.update(0.0, timestamp_s)
        return tuple(self.values[:3])  # type: ignore[return-value]


class LatencyBaseline:
    """Rolling per-venue latency distribution used for z-scored feed-health penalties."""

    def __init__(self, window: int = 500, warmup: int = 30) -> None:
        self._samples_ms: deque[float] = deque(maxlen=max(30, int(window)))
        self.warmup = max(10, int(warmup))

    def observe(self, latency_ms: float) -> float | None:
        value = max(0.0, float(latency_ms))
        z = self.z_score(value)
        self._samples_ms.append(value)
        return z

    def z_score(self, latency_ms: float) -> float | None:
        if len(self._samples_ms) < self.warmup:
            return None
        mean = statistics.fmean(self._samples_ms)
        variance = statistics.fmean((x - mean) ** 2 for x in self._samples_ms)
        return (float(latency_ms) - mean) / max(math.sqrt(variance), 1e-6)

    @property
    def sample_count(self) -> int:
        return len(self._samples_ms)


def _top(levels: Iterable[BookLevel | Sequence[object] | Mapping[str, object]], *, is_bid: bool) -> BookLevel | None:
    parsed = parse_levels(levels)
    if not parsed:
        return None
    return max(parsed, key=lambda x: x.price) if is_bid else min(parsed, key=lambda x: x.price)


def _usd(level: BookLevel | None, mapping: InstrumentMapping) -> float:
    if level is None:
        return 0.0
    return usd_notional_depth(displayed_size=level.displayed_size, level_price=level.price, mapping=mapping)


def top_of_book_ofi_usd(
    *,
    previous_bids: Iterable[BookLevel | Sequence[object] | Mapping[str, object]],
    previous_asks: Iterable[BookLevel | Sequence[object] | Mapping[str, object]],
    current_bids: Iterable[BookLevel | Sequence[object] | Mapping[str, object]],
    current_asks: Iterable[BookLevel | Sequence[object] | Mapping[str, object]],
    mapping: InstrumentMapping,
) -> float:
    """Cont-style top-of-book OFI in comparable USD-notional units.

    Positive values mean bid queues strengthened and/or ask queues depleted;
    negative values mean ask queues strengthened and/or bid queues depleted.
    """
    prev_bid = _top(previous_bids, is_bid=True)
    prev_ask = _top(previous_asks, is_bid=False)
    curr_bid = _top(current_bids, is_bid=True)
    curr_ask = _top(current_asks, is_bid=False)
    if prev_bid is None or prev_ask is None or curr_bid is None or curr_ask is None:
        return 0.0

    prev_bid_usd, curr_bid_usd = _usd(prev_bid, mapping), _usd(curr_bid, mapping)
    prev_ask_usd, curr_ask_usd = _usd(prev_ask, mapping), _usd(curr_ask, mapping)

    if curr_bid.price > prev_bid.price:
        bid_component = curr_bid_usd
    elif curr_bid.price == prev_bid.price:
        bid_component = curr_bid_usd - prev_bid_usd
    else:
        bid_component = -prev_bid_usd

    if curr_ask.price < prev_ask.price:
        ask_component = -curr_ask_usd
    elif curr_ask.price == prev_ask.price:
        ask_component = prev_ask_usd - curr_ask_usd
    else:
        ask_component = prev_ask_usd
    return bid_component + ask_component


def signed_trade_notional_usd(*, price: float, quantity: float, buyer_aggressor: bool, mapping: InstrumentMapping) -> float:
    notional = usd_notional_depth(displayed_size=quantity, level_price=price, mapping=mapping)
    return notional if buyer_aggressor else -notional


class MicrostructureTracker:
    """Thread-owned state tracker used by each WebSocket data manager."""

    def __init__(self, mapping: InstrumentMapping) -> None:
        self.mapping = mapping
        self._ofi = ExponentialFlowAccumulator()
        self._tfi = ExponentialFlowAccumulator()
        self._previous_bids: list[Sequence[object] | Mapping[str, object] | BookLevel] = []
        self._previous_asks: list[Sequence[object] | Mapping[str, object] | BookLevel] = []
        # Raw event records are retained for calibrated exit models (Kyle/VPIN).
        # They are not mixed across venues and are never used as synthetic depth.
        self._book_events: deque[dict[str, float]] = deque(maxlen=2000)
        self._trade_events: deque[dict[str, float]] = deque(maxlen=4000)

    @staticmethod
    def _microprice(bids, asks) -> float:
        bid = _top(bids, is_bid=True)
        ask = _top(asks, is_bid=False)
        if bid is None or ask is None or bid.price <= 0 or ask.price <= 0:
            return 0.0
        total = bid.displayed_size + ask.displayed_size
        if total <= 0:
            return (bid.price + ask.price) / 2.0
        return (ask.price * bid.displayed_size + bid.price * ask.displayed_size) / total

    def update_book(self, bids, asks, timestamp_s: float) -> tuple[float, float, float]:
        raw = 0.0
        if self._previous_bids and self._previous_asks:
            raw = top_of_book_ofi_usd(
                previous_bids=self._previous_bids,
                previous_asks=self._previous_asks,
                current_bids=bids,
                current_asks=asks,
                mapping=self.mapping,
            )
        self._previous_bids = list(bids or [])
        self._previous_asks = list(asks or [])
        microprice = self._microprice(bids, asks)
        if microprice > 0:
            self._book_events.append({
                "timestamp_s": float(timestamp_s),
                "microprice": float(microprice),
                "signed_ofi_usd": float(raw),
            })
        return self._ofi.update(raw, timestamp_s)

    def record_trade(self, *, price: float, quantity: float, buyer_aggressor: bool, timestamp_s: float) -> tuple[float, float, float]:
        signed_usd = signed_trade_notional_usd(
            price=price, quantity=quantity, buyer_aggressor=buyer_aggressor, mapping=self.mapping
        )
        self._trade_events.append({"timestamp_s": float(timestamp_s), "signed_notional_usd": float(signed_usd)})
        return self._tfi.update(signed_usd, timestamp_s)

    def snapshot(self, timestamp_s: float | None = None) -> FlowSnapshot:
        ofi = self._ofi.snapshot(timestamp_s)
        tfi = self._tfi.snapshot(timestamp_s)
        return FlowSnapshot(*ofi, *tfi)

    def research_state(self, timestamp_s: float | None = None, window_s: float = 600.0) -> dict[str, list[dict[str, float]]]:
        now = float(timestamp_s or 0.0)
        cutoff = now - max(1.0, float(window_s)) if now > 0 else 0.0
        books = [dict(row) for row in self._book_events if float(row.get("timestamp_s", 0.0)) >= cutoff]
        trades = [dict(row) for row in self._trade_events if float(row.get("timestamp_s", 0.0)) >= cutoff]
        return {"book_events": books, "trade_events": trades}

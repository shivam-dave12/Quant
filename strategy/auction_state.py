"""Institutional auction state primitives.

This module does not manufacture buy/sell signals.  It converts observable
auction information into deterministic evidence consumed by structural setup
models:

* multi-level book imbalance and micro-price displacement;
* exponentially weighted aggressive trade imbalance (OFI proxy);
* multi-timeframe liquidity-destination pressure; and
* robust, sample-derived displacement detection.

The design deliberately distinguishes *evidence score* from *probability*.
A score may rank concurrent structural theses.  It is not a calibrated win
probability and must not be used as one until replay-labelled calibration is
available for the instrument/venue/archetype regime.
"""
from __future__ import annotations

from dataclasses import dataclass
import math
import statistics
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence

_EPS = 1e-12


def _f(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        return out if math.isfinite(out) else default
    except Exception:
        return default


def _clamp(value: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, float(value)))


def _side_int(side: str) -> int:
    side = str(side or "").lower()
    return 1 if side == "long" else (-1 if side == "short" else 0)


@dataclass(frozen=True)
class MicrostructureState:
    """Execution-horizon market state derived only from observable events."""

    timestamp: float
    fresh: bool
    mid: float
    spread: float
    spread_atr: float
    depth_imbalance: float
    microprice: float
    microprice_edge_atr: float
    trade_imbalance: float
    event_count: int
    book_age_sec: float
    score_long: float
    score_short: float

    def aligned_score(self, side: str) -> float:
        return self.score_long if _side_int(side) > 0 else self.score_short

    @classmethod
    def empty(cls, now: Optional[float] = None) -> "MicrostructureState":
        return cls(float(time.time() if now is None else now), False, 0.0, 0.0, 0.0,
                   0.0, 0.0, 0.0, 0.0, 0, float("inf"), 0.0, 0.0)


@dataclass(frozen=True)
class DeliveryEvidence:
    """Continuous liquidity-delivery evidence; never a calibrated probability."""

    signed_score: float
    preferred_side: str
    trend_component: float
    liquidity_pull_component: float
    microstructure_component: float
    support_long: float
    support_short: float

    def score_for(self, side: str) -> float:
        direction = _side_int(side)
        return direction * self.signed_score


def _canonical_levels(levels: Iterable[Any]) -> List[tuple[float, float]]:
    out: List[tuple[float, float]] = []
    for item in list(levels or []):
        try:
            if isinstance(item, dict):
                px = _f(item.get("price") or item.get("limit_price"))
                qty = _f(item.get("size") or item.get("quantity") or item.get("depth"))
            else:
                px, qty = _f(item[0]), _f(item[1])
            if px > 0 and qty > 0:
                out.append((px, qty))
        except Exception:
            continue
    return out


def build_microstructure_state(orderbook: Optional[Dict[str, Any]], trades: Optional[Sequence[Dict[str, Any]]],
                               atr: float, now: Optional[float] = None,
                               book_max_age_sec: float = 5.0) -> MicrostructureState:
    """Compute an execution-horizon state from L2 depth and aggressor flow.

    Depth is inverse-rank weighted, so top-of-book executable liquidity has the
    greatest effect without discarding deeper levels.  Trade imbalance is
    exponentially time-weighted; recent aggressive flow matters more than stale
    prints.  The output is evidence for execution/timing, not a direction gate.
    """
    ts_now = float(time.time() if now is None else now)
    book = orderbook or {}
    bids = _canonical_levels(book.get("bids") or book.get("buy") or [])[:10]
    asks = _canonical_levels(book.get("asks") or book.get("sell") or [])[:10]
    book_ts = _f(book.get("timestamp"), 0.0)
    age = ts_now - book_ts if book_ts > 0 else float("inf")
    if not bids or not asks:
        return MicrostructureState.empty(ts_now)
    bid, ask = bids[0][0], asks[0][0]
    if ask <= bid:
        return MicrostructureState.empty(ts_now)
    mid = (bid + ask) / 2.0
    spread = ask - bid
    weighted_bid = sum(qty / (rank + 1.0) for rank, (_, qty) in enumerate(bids))
    weighted_ask = sum(qty / (rank + 1.0) for rank, (_, qty) in enumerate(asks))
    depth_total = weighted_bid + weighted_ask
    imbalance = (weighted_bid - weighted_ask) / max(depth_total, _EPS)
    # Large bid depth moves fair executable price toward the ask and vice versa.
    microprice = (ask * weighted_bid + bid * weighted_ask) / max(depth_total, _EPS)
    micro_edge_atr = (microprice - mid) / max(_f(atr), _EPS)

    signed_volume = 0.0
    total_volume = 0.0
    count = 0
    # A time constant proportional to decision urgency, not a prediction horizon.
    half_life = 15.0
    for trade in list(trades or [])[-400:]:
        qty = max(0.0, _f(trade.get("quantity") or trade.get("size") or trade.get("q")))
        trade_ts = _f(trade.get("timestamp"), ts_now)
        if qty <= 0 or ts_now - trade_ts > 120.0:
            continue
        decay = math.exp(-max(0.0, ts_now - trade_ts) * math.log(2.0) / half_life)
        sign = 1.0 if str(trade.get("side", "")).lower() == "buy" else -1.0
        signed_volume += sign * qty * decay
        total_volume += qty * decay
        count += 1
    trade_imbalance = signed_volume / max(total_volume, _EPS) if total_volume > 0 else 0.0
    # Evidence decomposition: OFI proxy and book-implied micro-price carry most
    # near-touch information; both are signed and bounded.
    signed = _clamp(0.45 * imbalance + 0.35 * _clamp(micro_edge_atr * 8.0) + 0.20 * trade_imbalance)
    fresh = age <= max(0.25, float(book_max_age_sec))
    if not fresh:
        signed = 0.0
    return MicrostructureState(
        timestamp=ts_now, fresh=fresh, mid=mid, spread=spread,
        spread_atr=spread / max(_f(atr), _EPS), depth_imbalance=imbalance,
        microprice=microprice, microprice_edge_atr=micro_edge_atr,
        trade_imbalance=trade_imbalance, event_count=count, book_age_sec=age,
        score_long=signed, score_short=-signed,
    )


def _target_pull(targets: Sequence[Any], side: str, price: float, atr: float) -> float:
    """Measure structural draw from real unswept target pools, distance adjusted."""
    direction = _side_int(side)
    total = 0.0
    for target in list(targets or []):
        pool = getattr(target, "pool", None)
        px = _f(getattr(pool, "price", 0.0))
        if px <= 0 or direction * (px - price) <= 0:
            continue
        sig = max(0.0, _f(getattr(target, "significance", 0.0)))
        dist_atr = abs(px - price) / max(atr, _EPS)
        total += math.log1p(sig) * math.exp(-dist_atr / 8.0)
    return total


def build_delivery_evidence(snapshot: Any, price: float, atr: float,
                            context_scores: Sequence[tuple[float, float]],
                            micro: Optional[MicrostructureState] = None) -> DeliveryEvidence:
    """Combine multi-horizon delivery and liquidity destination into one score.

    ``context_scores`` is ``[(signed_context_score, weight), ...]`` with the
    weights supplied by timeframe horizon.  The liquidity map supplies real
    target pools; microstructure only adjusts timing/execution evidence and is
    intentionally a minority component.
    """
    weight_total = sum(max(0.0, _f(weight)) for _, weight in context_scores) or 1.0
    trend = sum(_f(score) * max(0.0, _f(weight)) for score, weight in context_scores) / weight_total
    long_pull = _target_pull(getattr(snapshot, "bsl_pools", []) or [], "long", price, atr)
    short_pull = _target_pull(getattr(snapshot, "ssl_pools", []) or [], "short", price, atr)
    pull_denom = long_pull + short_pull
    pull = (long_pull - short_pull) / max(pull_denom, _EPS) if pull_denom > 0 else 0.0
    micro_signed = (micro.score_long if micro is not None and micro.fresh else 0.0)
    signed = _clamp(0.52 * _clamp(trend) + 0.36 * _clamp(pull) + 0.12 * _clamp(micro_signed))
    side = "long" if signed > 0 else ("short" if signed < 0 else "none")
    return DeliveryEvidence(
        signed_score=signed, preferred_side=side,
        trend_component=trend, liquidity_pull_component=pull,
        microstructure_component=micro_signed,
        support_long=max(0.0, signed), support_short=max(0.0, -signed),
    )


def robust_displacement_body_threshold(candles: Sequence[Dict[str, Any]], atr: float,
                                       lookback: int = 40) -> float:
    """Derive an abnormal-body threshold from the instrument's own recent tape."""
    rows = list(candles or [])[-max(8, int(lookback)):]
    values = [abs(_f(row.get("c")) - _f(row.get("o"))) / max(atr, _EPS) for row in rows]
    if not values:
        return float("inf")
    median = statistics.median(values)
    mad = statistics.median(abs(value - median) for value in values)
    robust_sigma = 1.4826 * mad
    # If the tape is perfectly static MAD is zero; its median still prevents an
    # arbitrary absolute price threshold from being introduced.
    return median + robust_sigma

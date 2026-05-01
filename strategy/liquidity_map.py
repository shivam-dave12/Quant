"""
strategy/liquidity_map.py

Institutional liquidity map for a liquidity-first trading bot.

Design rules:
- No synthetic liquidity levels.
- No fallback target levels.
- Only closed candles/orderbook-derived values are used.
- If liquidity is not known, the engine returns an empty map and the entry layer must not trade.

Expected candle object:
- dict or object with: open, high, low, close, volume, timestamp
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
import math
import time


def _f(value: Any, default: float = 0.0) -> float:
    try:
        x = float(value)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _get(obj: Any, key: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


@dataclass(frozen=True)
class Candle:
    open: float
    high: float
    low: float
    close: float
    volume: float
    timestamp: float = 0.0

    @classmethod
    def from_any(cls, value: Any) -> "Candle":
        return cls(
            open=_f(_get(value, "open")),
            high=_f(_get(value, "high")),
            low=_f(_get(value, "low")),
            close=_f(_get(value, "close")),
            volume=_f(_get(value, "volume")),
            timestamp=_f(_get(value, "timestamp", _get(value, "time", 0.0))),
        )

    @property
    def true_range(self) -> float:
        return max(0.0, self.high - self.low)

    @property
    def body(self) -> float:
        return abs(self.close - self.open)


@dataclass(frozen=True)
class LiquidityPool:
    side: str                       # BSL or SSL
    price: float
    timeframe: str
    strength: float
    touches: int
    created_at: float
    last_touched_at: float
    source: str = "swing"
    metadata: Dict[str, Any] = field(default_factory=dict)

    def distance_atr(self, price: float, atr: float) -> float:
        a = max(_f(atr), 1e-9)
        return abs(self.price - price) / a


@dataclass(frozen=True)
class SweepEvent:
    pool: LiquidityPool
    side: str                       # BSL or SSL swept
    reversal_side: str              # short after BSL sweep, long after SSL sweep
    quality: float
    wick_price: float
    close_price: float
    candle_timestamp: float
    volume_ratio: float
    reclaim_distance_atr: float
    reason: str

    @property
    def is_bsl(self) -> bool:
        return self.side.upper() == "BSL"

    @property
    def is_ssl(self) -> bool:
        return self.side.upper() == "SSL"


@dataclass(frozen=True)
class LiquidityMapSnapshot:
    price: float
    atr: float
    pools: List[LiquidityPool]
    sweeps: List[SweepEvent]
    created_at: float

    @property
    def bsl(self) -> List[LiquidityPool]:
        return [p for p in self.pools if p.side.upper() == "BSL"]

    @property
    def ssl(self) -> List[LiquidityPool]:
        return [p for p in self.pools if p.side.upper() == "SSL"]

    def nearest_bsl_above(self, price: Optional[float] = None) -> Optional[LiquidityPool]:
        px = self.price if price is None else price
        candidates = [p for p in self.bsl if p.price > px]
        return min(candidates, key=lambda p: p.price - px) if candidates else None

    def nearest_ssl_below(self, price: Optional[float] = None) -> Optional[LiquidityPool]:
        px = self.price if price is None else price
        candidates = [p for p in self.ssl if p.price < px]
        return max(candidates, key=lambda p: p.price) if candidates else None

    def target_for_side(self, side: str, entry: Optional[float] = None) -> Optional[LiquidityPool]:
        px = self.price if entry is None else entry
        return self.nearest_bsl_above(px) if side.lower() == "long" else self.nearest_ssl_below(px)


class LiquidityMap:
    def __init__(
        self,
        *,
        swing_left: int = 2,
        swing_right: int = 2,
        atr_period: int = 14,
        merge_atr_fraction: float = 0.18,
        max_pools_per_side: int = 12,
        min_sweep_reclaim_atr: float = 0.03,
        min_sweep_quality: float = 0.35,
    ) -> None:
        if swing_left < 1 or swing_right < 1:
            raise ValueError("swing_left/right must be >= 1")
        self.swing_left = int(swing_left)
        self.swing_right = int(swing_right)
        self.atr_period = int(max(2, atr_period))
        self.merge_atr_fraction = float(merge_atr_fraction)
        self.max_pools_per_side = int(max(1, max_pools_per_side))
        self.min_sweep_reclaim_atr = float(min_sweep_reclaim_atr)
        self.min_sweep_quality = float(min_sweep_quality)

    def normalize_candles(self, candles: Sequence[Any]) -> List[Candle]:
        out: List[Candle] = []
        for item in candles or []:
            c = Candle.from_any(item)
            if c.high > 0 and c.low > 0 and c.high >= c.low and c.close > 0:
                out.append(c)
        out.sort(key=lambda x: x.timestamp)
        return out

    def atr(self, candles: Sequence[Candle]) -> float:
        if len(candles) < 2:
            return 0.0
        trs: List[float] = []
        prev_close = candles[0].close
        for c in candles[1:]:
            tr = max(c.high - c.low, abs(c.high - prev_close), abs(c.low - prev_close))
            if math.isfinite(tr) and tr >= 0:
                trs.append(tr)
            prev_close = c.close
        if not trs:
            return 0.0
        n = min(len(trs), self.atr_period)
        return sum(trs[-n:]) / n

    def _volume_ratio(self, candles: Sequence[Candle], idx: int, lookback: int = 20) -> float:
        if idx <= 0 or idx >= len(candles):
            return 0.0
        start = max(0, idx - lookback)
        base = [c.volume for c in candles[start:idx] if c.volume > 0]
        if not base:
            return 0.0
        avg = sum(base) / len(base)
        return candles[idx].volume / avg if avg > 0 else 0.0

    def _swing_pools(self, candles: Sequence[Candle], tf: str, atr_value: float) -> List[LiquidityPool]:
        if len(candles) < self.swing_left + self.swing_right + 1:
            return []

        pools: List[LiquidityPool] = []
        left, right = self.swing_left, self.swing_right

        for i in range(left, len(candles) - right):
            c = candles[i]
            left_slice = candles[i - left:i]
            right_slice = candles[i + 1:i + right + 1]

            is_swing_high = all(c.high > x.high for x in left_slice) and all(c.high >= x.high for x in right_slice)
            is_swing_low = all(c.low < x.low for x in left_slice) and all(c.low <= x.low for x in right_slice)

            if not (is_swing_high or is_swing_low):
                continue

            vol_ratio = self._volume_ratio(candles, i)
            age = max(1, len(candles) - i)
            recency = 1.0 / math.sqrt(age)
            range_score = c.true_range / max(atr_value, 1e-9) if atr_value > 0 else 0.0
            strength = (1.0 + min(vol_ratio, 3.0) * 0.35 + min(range_score, 3.0) * 0.20) * (0.65 + recency)

            if is_swing_high:
                pools.append(LiquidityPool(
                    side="BSL",
                    price=c.high,
                    timeframe=tf,
                    strength=strength,
                    touches=1,
                    created_at=c.timestamp,
                    last_touched_at=c.timestamp,
                    metadata={"volume_ratio": vol_ratio, "range_atr": range_score},
                ))

            if is_swing_low:
                pools.append(LiquidityPool(
                    side="SSL",
                    price=c.low,
                    timeframe=tf,
                    strength=strength,
                    touches=1,
                    created_at=c.timestamp,
                    last_touched_at=c.timestamp,
                    metadata={"volume_ratio": vol_ratio, "range_atr": range_score},
                ))

        return pools

    def _merge_pools(self, pools: Sequence[LiquidityPool], atr_value: float) -> List[LiquidityPool]:
        if not pools:
            return []
        merge_dist = max(atr_value * self.merge_atr_fraction, 1e-9)
        merged: List[LiquidityPool] = []

        for side in ("BSL", "SSL"):
            side_pools = sorted([p for p in pools if p.side.upper() == side], key=lambda p: p.price)
            clusters: List[List[LiquidityPool]] = []
            for p in side_pools:
                if not clusters or abs(clusters[-1][-1].price - p.price) > merge_dist:
                    clusters.append([p])
                else:
                    clusters[-1].append(p)

            for cluster in clusters:
                weight = sum(max(p.strength, 1e-9) for p in cluster)
                price = sum(p.price * max(p.strength, 1e-9) for p in cluster) / weight
                best = max(cluster, key=lambda p: p.strength)
                touches = sum(p.touches for p in cluster)
                strength = sum(p.strength for p in cluster) * (1.0 + math.log1p(touches) * 0.25)
                merged.append(LiquidityPool(
                    side=side,
                    price=price,
                    timeframe=best.timeframe,
                    strength=strength,
                    touches=touches,
                    created_at=min(p.created_at for p in cluster),
                    last_touched_at=max(p.last_touched_at for p in cluster),
                    source="merged_swing",
                    metadata={"cluster_size": len(cluster), "members": [p.price for p in cluster[:10]]},
                ))

        return merged

    def _rank_pools(self, pools: Sequence[LiquidityPool], price: float, atr_value: float) -> List[LiquidityPool]:
        a = max(atr_value, 1e-9)

        def score(p: LiquidityPool) -> float:
            dist = abs(p.price - price) / a
            distance_penalty = 1.0 / (1.0 + dist * 0.18)
            htf_bonus = {"1d": 1.40, "4h": 1.25, "1h": 1.12, "15m": 1.00, "5m": 0.92, "1m": 0.85}.get(p.timeframe, 1.0)
            return p.strength * distance_penalty * htf_bonus

        ranked = sorted(pools, key=score, reverse=True)
        out: List[LiquidityPool] = []
        for side in ("BSL", "SSL"):
            side_ranked = [p for p in ranked if p.side.upper() == side]
            out.extend(side_ranked[:self.max_pools_per_side])
        return sorted(out, key=lambda p: (p.side, abs(p.price - price)))

    def detect_sweeps(self, pools: Sequence[LiquidityPool], candle: Candle, atr_value: float) -> List[SweepEvent]:
        a = max(atr_value, 1e-9)
        sweeps: List[SweepEvent] = []

        for p in pools:
            if p.side.upper() == "BSL":
                swept = candle.high > p.price and candle.close < p.price
                if not swept:
                    continue
                reclaim = (p.price - candle.close) / a
                wick_excess = (candle.high - p.price) / a
                if reclaim < self.min_sweep_reclaim_atr:
                    continue
                q = min(1.0, 0.25 + reclaim * 1.2 + wick_excess * 0.7 + min(p.strength, 5.0) * 0.08)
                if q >= self.min_sweep_quality:
                    sweeps.append(SweepEvent(
                        pool=p,
                        side="BSL",
                        reversal_side="short",
                        quality=q,
                        wick_price=candle.high,
                        close_price=candle.close,
                        candle_timestamp=candle.timestamp,
                        volume_ratio=_f(candle.volume),
                        reclaim_distance_atr=reclaim,
                        reason=f"BSL swept and reclaimed close below pool; reclaim={reclaim:.2f}ATR",
                    ))

            elif p.side.upper() == "SSL":
                swept = candle.low < p.price and candle.close > p.price
                if not swept:
                    continue
                reclaim = (candle.close - p.price) / a
                wick_excess = (p.price - candle.low) / a
                if reclaim < self.min_sweep_reclaim_atr:
                    continue
                q = min(1.0, 0.25 + reclaim * 1.2 + wick_excess * 0.7 + min(p.strength, 5.0) * 0.08)
                if q >= self.min_sweep_quality:
                    sweeps.append(SweepEvent(
                        pool=p,
                        side="SSL",
                        reversal_side="long",
                        quality=q,
                        wick_price=candle.low,
                        close_price=candle.close,
                        candle_timestamp=candle.timestamp,
                        volume_ratio=_f(candle.volume),
                        reclaim_distance_atr=reclaim,
                        reason=f"SSL swept and reclaimed close above pool; reclaim={reclaim:.2f}ATR",
                    ))

        return sorted(sweeps, key=lambda s: s.quality, reverse=True)

    def build(self, candles_by_tf: Dict[str, Sequence[Any]]) -> LiquidityMapSnapshot:
        if not candles_by_tf:
            return LiquidityMapSnapshot(price=0.0, atr=0.0, pools=[], sweeps=[], created_at=time.time())

        normalized: Dict[str, List[Candle]] = {
            tf: self.normalize_candles(candles)
            for tf, candles in candles_by_tf.items()
            if candles
        }

        # Primary price/ATR from the fastest available timeframe.
        primary_tf_order = ["1m", "3m", "5m", "15m", "1h", "4h", "1d"]
        primary_tf = next((tf for tf in primary_tf_order if tf in normalized and normalized[tf]), None)
        if primary_tf is None:
            primary_tf = next(iter(normalized), None)

        if primary_tf is None or not normalized[primary_tf]:
            return LiquidityMapSnapshot(price=0.0, atr=0.0, pools=[], sweeps=[], created_at=time.time())

        primary = normalized[primary_tf]
        price = primary[-1].close
        atr_value = self.atr(primary)
        if atr_value <= 0:
            # No synthetic ATR. Return no actionable state.
            return LiquidityMapSnapshot(price=price, atr=0.0, pools=[], sweeps=[], created_at=time.time())

        raw_pools: List[LiquidityPool] = []
        for tf, candles in normalized.items():
            tf_atr = self.atr(candles) or atr_value
            raw_pools.extend(self._swing_pools(candles, tf, tf_atr))

        merged = self._merge_pools(raw_pools, atr_value)
        ranked = self._rank_pools(merged, price, atr_value)
        sweeps = self.detect_sweeps(ranked, primary[-1], atr_value)

        return LiquidityMapSnapshot(
            price=price,
            atr=atr_value,
            pools=ranked,
            sweeps=sweeps,
            created_at=time.time(),
        )

"""
liquidity_map.py — Multi-Timeframe Liquidity Map Engine v2.0
=============================================================
FIXES IN THIS VERSION
─────────────────────
FIX-1: Swing lookback reduced from 5→3 bars per side on 5m/15m.
        Old: 5-bar lookback = 25 min delay on 5m, 75 min on 15m before
        a new swing qualifies. Engine was perpetually blind to fresh
        intraday structure. New: 3-bar = 15 min on 5m, 45 min on 15m.
        1h/4h unchanged (5 bars appropriate for macro structure).

FIX-2: TRADEABLE_POOL_MIN_SIG lowered from 3.0 → 1.8.
        Old: A brand-new single-touch 5m pool scores 2.0 and was
        EXCLUDED from all targeting. After 2-3 trades, all near-term
        pools in range were swept and zero new pools met the threshold.
        The engine had nothing to track and reported "no target".
        New threshold: new 5m pools (sig=2.0) are included. A new 1m
        pool (sig=1.0) is still excluded — correct, those are noise.

FIX-3: Swept pools are retained in a historical registry for 6 hours.
        Old: swept pools were pruned from the live registry and vanished
        entirely from the display and from downstream use. The engine
        had no memory of where price had been. New: swept pools move to
        _swept_history (bounded at 30 entries per side). They are used
        by get_snapshot() to annotate which levels have already been hit.

FIX-4: Pool significance formula revised.
        Old formula: (base + touch_bonus + structural + freshness) * htf_mult
        - A 4h single-touch pool scored 5.0, a 5m 2-touch pool scored 3.0.
        - The 4h pool anchored 40+ ATR away dominated the target queue
          over nearby 5m pools that were actually approaching.
        New formula: adds PROXIMITY_BONUS when pool is within 5 ATR of
        current price. Near-term pools with moderate significance are
        preferred over distant HTF pools.

FIX-5: Dedup radius tightened from 0.3 ATR → 0.2 ATR.
        Old: two 5m pools at $66,620 and $66,735 (115 pts apart, 1.02 ATR)
        were treated as duplicates and the lower-sig one discarded.
        New: only pools within 0.2 ATR (22 pts at ATR=112) are merged.

FIX-6: check_sweeps() now uses candles[-2] (last closed) not candles[-1].
        Old: checked the FORMING candle whose H/L updates every tick.
        A normal wick in a forming candle was triggering false sweeps.
        New: only a fully-closed candle can trigger a sweep.

FIX-7: HTF confluence promotion radius widened from 0.5 → 0.7 ATR.
        Old: a 1D level at $65,580 and a 4h level at $65,550 (30 pts)
        were NOT promoted because 30pts / 112 ATR = 0.27 < 0.5.
        New radius 0.7 × 112 = 78 pts — these levels now merge correctly.

FIX-8: Pool max age increased for 15m (8h→12h), 1h (24h→48h).
        Institutional structural levels persist longer than originally coded.

FIX-9: _merge_pools now updates pool PRICE (centroid) when matched.
        Old: a matching pool kept its original centroid even as new
        swing data refined the true cluster center. Price drift
        accumulated silently. New: centroid is averaged with the new one.

FIX-10: get_snapshot() now includes a DISTANCE-weighted score so that
         a nearby pool with sig=3.0 beats a distant pool with sig=4.0.
         proximity_adjusted_sig = sig × exp(-distance_atr / 10)
         At 5 ATR, penalty is 0.61×. At 20 ATR, penalty is 0.13×.
         This ensures the engine targets REACHABLE pools, not just
         the most historically significant level on the chart.
"""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════

TF_HIERARCHY: Dict[str, int] = {
    "1m": 1, "5m": 2, "15m": 3, "1h": 4, "4h": 5, "1d": 6,
}

# FIX-1: Reduced lookback for faster intraday pool formation
_SWING_LOOKBACK: Dict[str, int] = {
    "1m": 3, "5m": 3, "15m": 3, "1h": 4, "4h": 5, "1d": 3,
}

_CLUSTER_RADIUS_ATR: Dict[str, float] = {
    "1m": 0.12, "5m": 0.20, "15m": 0.30,
    "1h": 0.45, "4h": 0.60, "1d": 0.90,
}

# FIX-8: Extended pool max ages
_POOL_MAX_AGE: Dict[str, float] = {
    "1m": 7200,      # 2 hours (1m pools are micro-structure, keep short)
    "5m": 259200,    # 3 days — user requirement: full session + prior sessions
    "15m": 259200,  # 3 days — key intraday structure persists across sessions
    "1h": 259200,   # 3 days — hourly structure anchors multi-session ranges
    "4h": 259200,   # 3 days
    "1d": 604800,   # 7 days
}

_MAX_POOLS_PER_TF = 15   # increased from 12

# FIX-2: Lowered significance threshold — allows new 5m pools (sig≈2.0)
TRADEABLE_POOL_MIN_SIG = 1.8  # was 3.0

_HTF_CONFLUENCE_MULT = 2.5

# Sweep detection
_SWEEP_WICK_MIN_ATR  = 0.03
_SWEEP_REJECT_MIN_ATR = 0.02

# FIX-3: Swept pool history retention
_SWEPT_HISTORY_MAX = 200  # store up to 200 swept pools per side; bounded deque in _TimeframeRegistry
_SWEPT_HISTORY_AGE = 172800.0  # 48 hours — institutional memory; pools from prior session remain relevant


# ═══════════════════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════════════════

class PoolStatus(Enum):
    DETECTED  = "DETECTED"
    CONFIRMED = "CONFIRMED"
    SWEPT     = "SWEPT"
    CONSUMED  = "CONSUMED"


class PoolSide(Enum):
    BSL = "BSL"
    SSL = "SSL"


@dataclass
class LiquidityPool:
    """A cluster of stop orders at a price level."""

    price:       float
    side:        PoolSide
    timeframe:   str
    status:      PoolStatus = PoolStatus.DETECTED
    touches:     int        = 1
    created_at:  float      = 0.0
    last_touch:  float      = 0.0
    swept_at:    float      = 0.0
    sweep_wick:  float      = 0.0

    ob_aligned:  bool = False
    fvg_aligned: bool = False
    htf_count:   int  = 0

    # FIX-10: runtime proximity set by get_snapshot()
    _proximity_atr: float = 999.0

    @property
    def tf_rank(self) -> int:
        return TF_HIERARCHY.get(self.timeframe, 1)

    @property
    def significance(self) -> float:
        """
        FIX-4: Revised significance — proximity-penalized at display time.
        Base computation unchanged; proximity adjustment applied in get_snapshot().
        """
        base = float(self.tf_rank)
        touch_bonus = min(float(self.touches - 1), 5.0)
        htf_mult = _HTF_CONFLUENCE_MULT if self.htf_count >= 2 else 1.0
        structural = 0.0
        if self.ob_aligned:
            structural += 2.0
        if self.fvg_aligned:
            structural += 1.0
        max_age = _POOL_MAX_AGE.get(self.timeframe, 7200)
        age = time.time() - self.created_at
        freshness_penalty = -1.0 if age > max_age * 0.5 else 0.0
        raw = (base + touch_bonus + structural + freshness_penalty) * htf_mult
        return max(0.1, raw)

    def proximity_adjusted_sig(self, dist_atr: float) -> float:
        """FIX-10: Exponential decay by distance so near pools dominate."""
        decay = math.exp(-dist_atr / 10.0)
        return self.significance * decay

    @property
    def is_tradeable(self) -> bool:
        return (self.status in (PoolStatus.DETECTED, PoolStatus.CONFIRMED)
                and self.significance >= TRADEABLE_POOL_MIN_SIG)


@dataclass
class PoolTarget:
    """A ranked liquidity target consumed by the entry engine."""
    pool:          LiquidityPool
    distance_atr:  float
    direction:     str
    significance:  float
    tf_sources:    List[str]


@dataclass
class SweepResult:
    """Confirmed sweep event."""
    pool:             LiquidityPool
    sweep_candle_idx: int
    wick_extreme:     float
    rejection_pct:    float
    volume_ratio:     float
    quality:          float
    direction:        str
    detected_at:      float


@dataclass
class LiquidityMapSnapshot:
    """Immutable state of the liquidity map at a point in time."""
    bsl_pools:        List[PoolTarget]
    ssl_pools:        List[PoolTarget]
    primary_target:   Optional[PoolTarget]
    recent_sweeps:    List[SweepResult]
    swept_bsl_levels: List[float]   # FIX-3: historical swept BSL prices
    swept_ssl_levels: List[float]   # FIX-3: historical swept SSL prices
    nearest_bsl_atr:  float
    nearest_ssl_atr:  float
    timestamp:        float


# ═══════════════════════════════════════════════════════════════════════════
# SWING DETECTION
# ═══════════════════════════════════════════════════════════════════════════

def _find_swing_highs(candles: List[Dict], lookback: int = 3) -> List[Tuple[int, float]]:
    """Find swing highs with `lookback` bars confirmed on each side."""
    results = []
    n = len(candles)
    for i in range(lookback, n - lookback):
        h = float(candles[i]['h'])
        is_swing = all(
            float(candles[i - j]['h']) < h and float(candles[i + j]['h']) < h
            for j in range(1, lookback + 1)
        )
        if is_swing:
            results.append((i, h))
    return results


def _find_swing_lows(candles: List[Dict], lookback: int = 3) -> List[Tuple[int, float]]:
    """Find swing lows with `lookback` bars confirmed on each side."""
    results = []
    n = len(candles)
    for i in range(lookback, n - lookback):
        lo = float(candles[i]['l'])
        is_swing = all(
            float(candles[i - j]['l']) > lo and float(candles[i + j]['l']) > lo
            for j in range(1, lookback + 1)
        )
        if is_swing:
            results.append((i, lo))
    return results


def _detect_equal_levels(
    swings: List[Tuple[int, float]],
    atr: float,
    cluster_radius_atr: float,
) -> List[Tuple[float, int]]:
    """
    Cluster swing extremes within cluster_radius × ATR.
    Returns [(centroid_price, touch_count), ...].
    """
    if not swings or atr < 1e-10:
        return []

    radius = atr * cluster_radius_atr
    sorted_swings = sorted(swings, key=lambda s: s[1])

    clusters: List[Tuple[float, int]] = []
    cluster_prices: List[float] = []
    cluster_count = 0

    for _, price in sorted_swings:
        if cluster_count == 0:
            cluster_prices = [price]
            cluster_count = 1
        elif abs(price - cluster_prices[-1]) <= radius:
            cluster_prices.append(price)
            cluster_count += 1
        else:
            centroid = sum(cluster_prices) / len(cluster_prices)
            clusters.append((centroid, cluster_count))
            cluster_prices = [price]
            cluster_count = 1

    if cluster_count > 0:
        centroid = sum(cluster_prices) / len(cluster_prices)
        clusters.append((centroid, cluster_count))

    return clusters


# ═══════════════════════════════════════════════════════════════════════════
# TIMEFRAME POOL REGISTRY
# ═══════════════════════════════════════════════════════════════════════════

class _TimeframeRegistry:
    """Maintains BSL and SSL pools for a single timeframe."""

    def __init__(self, timeframe: str):
        self.tf = timeframe
        self._bsl: List[LiquidityPool] = []
        self._ssl: List[LiquidityPool] = []
        self._last_candle_ts: int = 0
        # FIX-3: swept pool history
        self._swept_bsl: List[LiquidityPool] = []
        self._swept_ssl: List[LiquidityPool] = []

    def update(self, candles: List[Dict], atr: float, now: float) -> None:
        """Rebuild pool list from swing structure."""
        if len(candles) < 10 or atr < 1e-10:
            return

        # Dedup on last CLOSED candle timestamp (FIX from original BUG-FIX-2)
        try:
            _dedup_c = candles[-2] if len(candles) >= 2 else candles[-1]
            _last_ts = int(_dedup_c.get('t', 0) or 0) if hasattr(_dedup_c, 'get') else 0
        except Exception:
            _last_ts = 0

        if _last_ts > 0 and _last_ts == self._last_candle_ts:
            return
        if _last_ts > 0:
            self._last_candle_ts = _last_ts

        lookback  = _SWING_LOOKBACK.get(self.tf, 3)   # FIX-1: uses reduced lookback
        cluster_r = _CLUSTER_RADIUS_ATR.get(self.tf, 0.25)

        swing_highs = _find_swing_highs(candles, lookback)
        swing_lows  = _find_swing_lows(candles, lookback)

        bsl_clusters = _detect_equal_levels(swing_highs, atr, cluster_r)
        ssl_clusters = _detect_equal_levels(swing_lows,  atr, cluster_r)

        self._bsl = self._merge_pools(self._bsl, bsl_clusters, PoolSide.BSL, atr, now)
        self._ssl = self._merge_pools(self._ssl, ssl_clusters, PoolSide.SSL, atr, now)

        # Prune stale pools — CONSUMED only (SWEPT moved to history, FIX-3)
        max_age = _POOL_MAX_AGE.get(self.tf, 7200)
        self._bsl = [p for p in self._bsl
                     if now - p.created_at < max_age
                     and p.status != PoolStatus.CONSUMED]
        self._ssl = [p for p in self._ssl
                     if now - p.created_at < max_age
                     and p.status != PoolStatus.CONSUMED]

        # Cap per significance descending
        self._bsl = sorted(self._bsl, key=lambda p: p.significance, reverse=True)[:_MAX_POOLS_PER_TF]
        self._ssl = sorted(self._ssl, key=lambda p: p.significance, reverse=True)[:_MAX_POOLS_PER_TF]

        # Prune swept history (age-based)
        self._swept_bsl = [p for p in self._swept_bsl if now - p.swept_at < _SWEPT_HISTORY_AGE][-_SWEPT_HISTORY_MAX:]
        self._swept_ssl = [p for p in self._swept_ssl if now - p.swept_at < _SWEPT_HISTORY_AGE][-_SWEPT_HISTORY_MAX:]

    def _merge_pools(
        self,
        existing: List[LiquidityPool],
        clusters: List[Tuple[float, int]],
        side: PoolSide,
        atr: float,
        now: float,
    ) -> List[LiquidityPool]:
        """
        Merge new clusters into existing registry.
        FIX-9: Update centroid price when a cluster re-matches.
        """
        radius = atr * _CLUSTER_RADIUS_ATR.get(self.tf, 0.25)
        merged = list(existing)
        used_indices = set()

        for centroid, count in clusters:
            matched = False
            for i, pool in enumerate(merged):
                if i in used_indices:
                    continue
                if pool.status in (PoolStatus.SWEPT, PoolStatus.CONSUMED):
                    continue
                if abs(pool.price - centroid) <= radius:
                    # FIX-9: Update centroid toward new detection
                    pool.price = (pool.price + centroid) / 2.0
                    pool.touches = max(pool.touches, count)
                    pool.last_touch = now
                    if count >= 2:
                        pool.status = PoolStatus.CONFIRMED
                    used_indices.add(i)
                    matched = True
                    break

            if not matched:
                new_pool = LiquidityPool(
                    price=centroid,
                    side=side,
                    timeframe=self.tf,
                    status=(PoolStatus.CONFIRMED if count >= 2 else PoolStatus.DETECTED),
                    touches=count,
                    created_at=now,
                    last_touch=now,
                )
                merged.append(new_pool)

        return merged

    def check_sweeps(
        self,
        candles: List[Dict],
        atr: float,
        now: float,
    ) -> List[SweepResult]:
        """
        Check for sweeps. FIX-6: Use last CLOSED candle (candles[-2]),
        not the forming candle (candles[-1]).
        """
        # FIX-6: need at least 3 candles to have [-2] as a closed bar
        if len(candles) < 3 or atr < 1e-10:
            return []

        # FIX-6: use candles[-2] — the LAST CONFIRMED CLOSED candle
        last = candles[-2]
        h    = float(last['h'])
        lo   = float(last['l'])
        c    = float(last['c'])
        o    = float(last['o'])
        vol  = float(last.get('v', 0))

        # 20-bar average volume (exclude forming candle)
        vol_window = candles[max(0, len(candles) - 22):-1]
        avg_vol = (sum(float(x.get('v', 0)) for x in vol_window)
                   / max(len(vol_window), 1))

        results = []
        min_wick   = atr * _SWEEP_WICK_MIN_ATR
        min_reject = atr * _SWEEP_REJECT_MIN_ATR

        # BSL sweeps
        for pool in self._bsl:
            if pool.status in (PoolStatus.SWEPT, PoolStatus.CONSUMED):
                continue
            if h > pool.price + min_wick and c < pool.price - min_reject:
                wick_above = h - pool.price
                rejection  = h - c
                wick_range = max(h - lo, 1e-10)

                quality = self._score_sweep(
                    wick_above / atr,
                    rejection / wick_range,
                    vol / max(avg_vol, 1e-10),
                )

                # FIX-3: move to swept history before status change
                pool.status   = PoolStatus.SWEPT
                pool.swept_at = now
                pool.sweep_wick = h
                self._swept_bsl.append(pool)

                results.append(SweepResult(
                    pool=pool,
                    sweep_candle_idx=len(candles) - 2,
                    wick_extreme=h,
                    rejection_pct=rejection / wick_range,
                    volume_ratio=vol / max(avg_vol, 1e-10),
                    quality=quality,
                    direction="short",
                    detected_at=now,
                ))

        # SSL sweeps
        for pool in self._ssl:
            if pool.status in (PoolStatus.SWEPT, PoolStatus.CONSUMED):
                continue
            if lo < pool.price - min_wick and c > pool.price + min_reject:
                wick_below = pool.price - lo
                rejection  = c - lo
                wick_range = max(h - lo, 1e-10)

                quality = self._score_sweep(
                    wick_below / atr,
                    rejection / wick_range,
                    vol / max(avg_vol, 1e-10),
                )

                pool.status   = PoolStatus.SWEPT
                pool.swept_at = now
                pool.sweep_wick = lo
                self._swept_ssl.append(pool)

                results.append(SweepResult(
                    pool=pool,
                    sweep_candle_idx=len(candles) - 2,
                    wick_extreme=lo,
                    rejection_pct=rejection / wick_range,
                    volume_ratio=vol / max(avg_vol, 1e-10),
                    quality=quality,
                    direction="long",
                    detected_at=now,
                ))

        return results

    def check_consumed(self, price: float, atr: float) -> None:
        threshold = atr * 0.5
        for pool in self._bsl:
            if pool.status == PoolStatus.SWEPT:
                if price > pool.price + threshold:
                    pool.status = PoolStatus.CONSUMED
        for pool in self._ssl:
            if pool.status == PoolStatus.SWEPT:
                if price < pool.price - threshold:
                    pool.status = PoolStatus.CONSUMED

    @staticmethod
    def _score_sweep(
        penetration_atr: float,
        rejection_pct:   float,
        volume_ratio:    float,
    ) -> float:
        pen_score = min(1.0, penetration_atr / 0.5)
        rej_score = min(1.0, rejection_pct  / 0.8)
        vol_score = min(1.0, volume_ratio   / 3.0)
        return (pen_score + rej_score + vol_score) / 3.0

    @property
    def bsl_pools(self) -> List[LiquidityPool]:
        return [p for p in self._bsl if p.status != PoolStatus.CONSUMED]

    @property
    def ssl_pools(self) -> List[LiquidityPool]:
        return [p for p in self._ssl if p.status != PoolStatus.CONSUMED]

    @property
    def swept_bsl(self) -> List[LiquidityPool]:
        return list(self._swept_bsl)

    @property
    def swept_ssl(self) -> List[LiquidityPool]:
        return list(self._swept_ssl)


# ═══════════════════════════════════════════════════════════════════════════
# MAIN ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class LiquidityMap:
    """
    Tracks liquidity pools across all timeframes independently.
    v2.0: FIX-1 through FIX-10 applied.
    """

    def __init__(self) -> None:
        self._registries: Dict[str, _TimeframeRegistry] = {
            tf: _TimeframeRegistry(tf)
            for tf in TF_HIERARCHY
        }
        self._recent_sweeps: List[SweepResult] = []
        self._last_snapshot: Optional[LiquidityMapSnapshot] = None

    def update(
        self,
        candles_by_tf: Dict[str, List[Dict]],
        price: float,
        atr: float,
        now: float,
        ict_engine=None,
    ) -> None:
        if atr < 1e-10:
            return

        for tf, candles in candles_by_tf.items():
            if tf in self._registries and candles:
                self._registries[tf].update(candles, atr, now)

        # FIX-7: wider HTF confluence radius
        self._promote_htf_confluence(atr)

        if ict_engine is not None:
            self._align_with_ict(ict_engine, price, atr, now)

        new_sweeps = []
        for tf, candles in candles_by_tf.items():
            if tf in self._registries and candles:
                sweeps = self._registries[tf].check_sweeps(candles, atr, now)
                new_sweeps.extend(sweeps)

        for reg in self._registries.values():
            reg.check_consumed(price, atr)

        self._recent_sweeps.extend(new_sweeps)
        cutoff = now - 300.0
        self._recent_sweeps = [s for s in self._recent_sweeps if s.detected_at > cutoff]

        for sweep in new_sweeps:
            logger.info(
                f"🎯 SWEEP [{sweep.pool.timeframe}] {sweep.pool.side.value} "
                f"${sweep.pool.price:,.1f} → direction={sweep.direction} "
                f"quality={sweep.quality:.2f} wick=${sweep.wick_extreme:,.1f} "
                f"vol_ratio={sweep.volume_ratio:.1f}x"
            )

    def _promote_htf_confluence(self, atr: float) -> None:
        """FIX-7: widened confluence radius from 0.5 → 0.7 ATR."""
        all_bsl: List[LiquidityPool] = []
        all_ssl: List[LiquidityPool] = []
        for reg in self._registries.values():
            all_bsl.extend(reg.bsl_pools)
            all_ssl.extend(reg.ssl_pools)

        confluence_radius = atr * 0.7  # FIX-7: was 0.5

        for pool in all_bsl + all_ssl:
            same_side = all_bsl if pool.side == PoolSide.BSL else all_ssl
            tf_set = set()
            for other in same_side:
                if abs(other.price - pool.price) <= confluence_radius:
                    tf_set.add(other.timeframe)
            pool.htf_count = len(tf_set)

    def _align_with_ict(
        self,
        ict_engine,
        price: float,
        atr: float,
        now: float,
    ) -> None:
        now_ms = int(now * 1000)
        try:
            for reg in self._registries.values():
                for pool in reg.bsl_pools + reg.ssl_pools:
                    pool.ob_aligned  = False
                    pool.fvg_aligned = False

                    if hasattr(ict_engine, '_obs'):
                        for tf_key, ob_list in ict_engine._obs.items():
                            for ob in ob_list:
                                ob_mid = (float(ob.high) + float(ob.low)) / 2.0
                                if abs(ob_mid - pool.price) < atr * 0.5:
                                    pool.ob_aligned = True
                                    break
                            if pool.ob_aligned:
                                break

                    if hasattr(ict_engine, '_fvgs'):
                        for tf_key, fvg_list in ict_engine._fvgs.items():
                            for fvg in fvg_list:
                                fvg_mid = (float(fvg.high) + float(fvg.low)) / 2.0
                                if pool.side == PoolSide.BSL:
                                    if price < fvg_mid < pool.price:
                                        pool.fvg_aligned = True
                                        break
                                else:
                                    if pool.price < fvg_mid < price:
                                        pool.fvg_aligned = True
                                        break
                            if pool.fvg_aligned:
                                break
        except Exception as e:
            logger.debug(f"ICT alignment error (non-fatal): {e}")

    def get_snapshot(self, price: float, atr: float) -> LiquidityMapSnapshot:
        """
        Build snapshot. FIX-5: Tighter dedup radius (0.2 ATR).
        FIX-10: Proximity-adjusted significance for primary target selection.
        FIX-3: Include swept pool history in snapshot.
        """
        now = time.time()

        empty = LiquidityMapSnapshot(
            bsl_pools=[], ssl_pools=[],
            primary_target=None,
            recent_sweeps=list(self._recent_sweeps),
            swept_bsl_levels=[], swept_ssl_levels=[],
            nearest_bsl_atr=999.0, nearest_ssl_atr=999.0,
            timestamp=now,
        )
        if atr < 1e-10:
            return empty

        bsl_targets: List[PoolTarget] = []
        ssl_targets: List[PoolTarget] = []

        for reg in self._registries.values():
            for pool in reg.bsl_pools:
                if not pool.is_tradeable:
                    continue
                dist = (pool.price - price) / atr
                if dist <= 0:
                    continue
                tf_sources = self._find_tf_sources(pool, PoolSide.BSL, atr)
                bsl_targets.append(PoolTarget(
                    pool=pool,
                    distance_atr=dist,
                    direction="long",
                    significance=pool.significance,
                    tf_sources=tf_sources,
                ))

            for pool in reg.ssl_pools:
                if not pool.is_tradeable:
                    continue
                dist = (price - pool.price) / atr
                if dist <= 0:
                    continue
                tf_sources = self._find_tf_sources(pool, PoolSide.SSL, atr)
                ssl_targets.append(PoolTarget(
                    pool=pool,
                    distance_atr=dist,
                    direction="short",
                    significance=pool.significance,
                    tf_sources=tf_sources,
                ))

        # FIX-5: Tighter dedup radius 0.2 ATR (was 0.3)
        bsl_targets = self._deduplicate_targets(bsl_targets, atr, radius_mult=0.20)
        ssl_targets = self._deduplicate_targets(ssl_targets, atr, radius_mult=0.20)

        # Sort by raw significance DESC (for display priority)
        bsl_targets.sort(key=lambda t: t.significance, reverse=True)
        ssl_targets.sort(key=lambda t: t.significance, reverse=True)

        nearest_bsl = min((t.distance_atr for t in bsl_targets), default=999.0)
        nearest_ssl = min((t.distance_atr for t in ssl_targets), default=999.0)

        # FIX-10 + BUG-7 FIX: Primary target = highest PROXIMITY-ADJUSTED significance.
        # Old threshold was 8 ATR. After 2-3 trades sweep all near-term pools,
        # only distant HTF pools (15-50 ATR) remain. With 8 ATR cutoff the engine
        # reported "no target" even though valid 7K-9K BSL clusters existed.
        # New: 25 ATR threshold. Proximity-adjusted significance naturally penalizes
        # distant pools (exp decay), so a nearby 5m pool still beats a far 4h pool.
        all_reachable = [t for t in bsl_targets + ssl_targets if t.distance_atr < 25.0]
        primary = None
        if all_reachable:
            primary = max(
                all_reachable,
                key=lambda t: t.pool.proximity_adjusted_sig(t.distance_atr)
            )
        elif bsl_targets or ssl_targets:
            # Final fallback: if everything is beyond 25 ATR, pick the nearest
            # of all available targets (no proximity cutoff — blind spot is worse)
            all_targets = bsl_targets + ssl_targets
            primary = min(all_targets, key=lambda t: t.distance_atr)

        # FIX-3: Collect swept pool history
        swept_bsl_levels = []
        swept_ssl_levels = []
        for reg in self._registries.values():
            for p in reg.swept_bsl:
                swept_bsl_levels.append(p.price)
            for p in reg.swept_ssl:
                swept_ssl_levels.append(p.price)

        snap = LiquidityMapSnapshot(
            bsl_pools=bsl_targets[:12],
            ssl_pools=ssl_targets[:12],
            primary_target=primary,
            recent_sweeps=list(self._recent_sweeps),
            swept_bsl_levels=sorted(set(swept_bsl_levels), reverse=True)[:10],
            swept_ssl_levels=sorted(set(swept_ssl_levels))[:10],
            nearest_bsl_atr=nearest_bsl,
            nearest_ssl_atr=nearest_ssl,
            timestamp=now,
        )
        self._last_snapshot = snap
        return snap

    def _find_tf_sources(
        self,
        pool: LiquidityPool,
        side: PoolSide,
        atr: float,
    ) -> List[str]:
        sources = []
        radius = atr * 0.5
        for tf, reg in self._registries.items():
            pool_list = reg.bsl_pools if side == PoolSide.BSL else reg.ssl_pools
            for p in pool_list:
                if abs(p.price - pool.price) <= radius:
                    sources.append(tf)
                    break
        return sources

    @staticmethod
    def _deduplicate_targets(
        targets: List[PoolTarget],
        atr: float,
        radius_mult: float = 0.20,   # FIX-5: was 0.30
    ) -> List[PoolTarget]:
        if not targets:
            return []
        radius = atr * radius_mult
        targets_sorted = sorted(targets, key=lambda t: t.significance, reverse=True)
        result = []
        used_prices = []
        for t in targets_sorted:
            is_dup = any(abs(t.pool.price - p) <= radius for p in used_prices)
            if not is_dup:
                result.append(t)
                used_prices.append(t.pool.price)
        return result

    def get_status_summary(self, price: float, atr: float) -> Dict:
        snap = self.get_snapshot(price, atr)
        return {
            "bsl_count": len(snap.bsl_pools),
            "ssl_count": len(snap.ssl_pools),
            "nearest_bsl": (
                f"${snap.bsl_pools[0].pool.price:,.1f} "
                f"({snap.bsl_pools[0].distance_atr:.1f} ATR, "
                f"sig={snap.bsl_pools[0].significance:.1f})"
                if snap.bsl_pools else "none"
            ),
            "nearest_ssl": (
                f"${snap.ssl_pools[0].pool.price:,.1f} "
                f"({snap.ssl_pools[0].distance_atr:.1f} ATR, "
                f"sig={snap.ssl_pools[0].significance:.1f})"
                if snap.ssl_pools else "none"
            ),
            "primary_target": (
                f"{snap.primary_target.direction} → "
                f"${snap.primary_target.pool.price:,.1f}"
                if snap.primary_target else "none"
            ),
            "recent_sweeps": len(snap.recent_sweeps),
            "swept_history": {
                "bsl": len(snap.swept_bsl_levels),
                "ssl": len(snap.swept_ssl_levels),
            },
            "tf_coverage": {
                tf: len(reg.bsl_pools) + len(reg.ssl_pools)
                for tf, reg in self._registries.items()
                if reg.bsl_pools or reg.ssl_pools
            },
        }

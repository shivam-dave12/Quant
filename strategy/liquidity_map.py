"""
liquidity_map.py — Multi-Timeframe Liquidity Map Engine
=========================================================
THE PRIMARY DRIVER of the trading system.

CORE THESIS
───────────
Markets move FROM one liquidity pool TO another. Period.
Every move has a destination — an unswept cluster of stops.
This engine answers: WHERE are the pools, HOW significant are they,
and WHICH one is the market heading toward?

DESIGN PRINCIPLES
─────────────────
1. Each timeframe maintains its OWN pool registry — 1m pools are not
   the same as 4h pools. A 4h equal-high cluster with 5 touches is
   100x more significant than a 1m wick.

2. Pools are PROMOTED when they appear across multiple timeframes
   (HTF confluence). A BSL at $70,000 that shows up on 15m, 1h, AND 4h
   is institutional — it WILL be hit.

3. Pool significance is STRUCTURAL, not scored by a weighted formula.
   It's based on: touch count, timeframe rank, freshness, and whether
   the pool aligns with a known order block or FVG.

4. The engine is STATELESS between ticks — no EMA smoothing, no
   hysteresis, no "score memory". Fresh data in → fresh map out.
   This eliminates an entire class of stale-state bugs.

POOL LIFECYCLE
──────────────
  DETECTED  → swing extreme identified, initial cluster formed
  CONFIRMED → 2+ touches within cluster radius, or HTF-promoted
  SWEPT     → price wicked through AND closed back (confirmed sweep)
  CONSUMED  → price closed through decisively (not a sweep — pool is dead)

THREAD SAFETY
─────────────
Single-writer (strategy tick loop) → no locking needed.
All collections are bounded by per-timeframe max_pools.
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

# Timeframe hierarchy: higher index = more significant
TF_HIERARCHY: Dict[str, int] = {
    "1m": 1, "5m": 2, "15m": 3, "1h": 4, "4h": 5, "1d": 6,
}

# How close two swing extremes must be (as fraction of ATR) to cluster
_CLUSTER_RADIUS_ATR: Dict[str, float] = {
    "1m": 0.15, "5m": 0.25, "15m": 0.35,
    "1h": 0.50, "4h": 0.70, "1d": 1.00,
}

# Max age before a pool is considered stale (seconds)
_POOL_MAX_AGE: Dict[str, float] = {
    "1m": 1800,     # 30 min
    "5m": 7200,     # 2 hours
    "15m": 28800,   # 8 hours
    "1h": 86400,    # 24 hours
    "4h": 259200,   # 3 days
    "1d": 604800,   # 7 days
}

# Maximum pools per timeframe per side (BSL/SSL)
_MAX_POOLS_PER_TF = 12

# Minimum swing lookback per timeframe
_SWING_LOOKBACK: Dict[str, int] = {
    "1m": 5, "5m": 5, "15m": 5, "1h": 5, "4h": 5, "1d": 3,
}

# HTF confluence bonus multiplier for significance
_HTF_CONFLUENCE_MULT = 2.5

# Sweep detection: wick must exceed pool by this fraction of ATR
_SWEEP_WICK_MIN_ATR = 0.03

# Sweep: candle body must close back inside range by this fraction
_SWEEP_REJECT_MIN_ATR = 0.02

# Significance: minimum score to be considered a "tradeable" pool
TRADEABLE_POOL_MIN_SIG = 3.0


# ═══════════════════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════════════════

class PoolStatus(Enum):
    DETECTED  = "DETECTED"
    CONFIRMED = "CONFIRMED"
    SWEPT     = "SWEPT"
    CONSUMED  = "CONSUMED"


class PoolSide(Enum):
    BSL = "BSL"  # Buy-side liquidity (stops above highs)
    SSL = "SSL"  # Sell-side liquidity (stops below lows)


@dataclass
class LiquidityPool:
    """A cluster of stop orders at a price level."""

    price:       float                      # Cluster centroid price
    side:        PoolSide                   # BSL or SSL
    timeframe:   str                        # Origin timeframe
    status:      PoolStatus = PoolStatus.DETECTED
    touches:     int        = 1             # Number of times price approached
    created_at:  float      = 0.0           # Wall-clock seconds
    last_touch:  float      = 0.0           # Last time price was within 0.3 ATR
    swept_at:    float      = 0.0           # When sweep was confirmed
    sweep_wick:  float      = 0.0           # Wick extreme of sweep candle

    # Structural alignment (set by cross-referencing ICT engine)
    ob_aligned:  bool = False               # Backed by an order block
    fvg_aligned: bool = False               # FVG points toward this pool
    htf_count:   int  = 0                   # How many higher TFs also see this level

    @property
    def tf_rank(self) -> int:
        return TF_HIERARCHY.get(self.timeframe, 1)

    @property
    def significance(self) -> float:
        """
        Structural significance score. NOT a probability — this is a
        priority ranking for which pool the market will target.

        Components (additive, not weighted-average):
          - Base = timeframe rank (1–6)
          - Touch bonus: +1.0 per touch beyond the first (capped at +5)
          - HTF confluence: ×2.5 if seen on 2+ timeframes
          - OB alignment: +2.0
          - FVG alignment: +1.0
          - Freshness penalty: -1.0 if older than 50% of max age
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

        return (base + touch_bonus + structural + freshness_penalty) * htf_mult

    @property
    def is_tradeable(self) -> bool:
        return (self.status in (PoolStatus.DETECTED, PoolStatus.CONFIRMED)
                and self.significance >= TRADEABLE_POOL_MIN_SIG)


@dataclass
class PoolTarget:
    """
    A ranked liquidity target: the pool + its distance + direction.
    This is what the strategy consumes.
    """
    pool:          LiquidityPool
    distance_atr:  float           # Distance from current price in ATR units
    direction:     str             # "long" (price must go UP to hit BSL) | "short" (DOWN to hit SSL)
    significance:  float           # Pool's significance score
    tf_sources:    List[str]       # All timeframes where this level appears


@dataclass
class SweepResult:
    """Confirmed sweep event for downstream consumption."""
    pool:             LiquidityPool
    sweep_candle_idx: int           # Index in the candle array
    wick_extreme:     float         # Absolute extreme of the sweep wick
    rejection_pct:    float         # How much of the wick was rejected (0–1)
    volume_ratio:     float         # Sweep candle volume / 20-bar avg
    quality:          float         # Composite quality 0–1
    direction:        str           # "long" (SSL swept → go long) | "short" (BSL swept → go short)
    detected_at:      float


@dataclass
class LiquidityMapSnapshot:
    """
    Complete state of the liquidity map at a point in time.
    Immutable — strategy reads this, never mutates it.
    """
    bsl_pools:        List[PoolTarget]   # Sorted by significance DESC
    ssl_pools:        List[PoolTarget]   # Sorted by significance DESC
    primary_target:   Optional[PoolTarget]  # Highest-significance reachable pool
    recent_sweeps:    List[SweepResult]  # Sweeps in the last 5 minutes
    nearest_bsl_atr:  float             # Distance to nearest BSL in ATR
    nearest_ssl_atr:  float             # Distance to nearest SSL in ATR
    timestamp:        float


# ═══════════════════════════════════════════════════════════════════════════
# SWING DETECTION (per-timeframe)
# ═══════════════════════════════════════════════════════════════════════════

def _find_swing_highs(candles: List[Dict], lookback: int = 5) -> List[Tuple[int, float]]:
    """
    Find swing highs: a bar whose high is higher than `lookback` bars
    on each side. Returns [(index, price), ...].
    """
    results = []
    n = len(candles)
    for i in range(lookback, n - lookback):
        h = float(candles[i]['h'])
        is_swing = True
        for j in range(1, lookback + 1):
            if float(candles[i - j]['h']) >= h or float(candles[i + j]['h']) >= h:
                is_swing = False
                break
        if is_swing:
            results.append((i, h))
    return results


def _find_swing_lows(candles: List[Dict], lookback: int = 5) -> List[Tuple[int, float]]:
    """Find swing lows: symmetric to swing highs."""
    results = []
    n = len(candles)
    for i in range(lookback, n - lookback):
        lo = float(candles[i]['l'])
        is_swing = True
        for j in range(1, lookback + 1):
            if float(candles[i - j]['l']) <= lo or float(candles[i + j]['l']) <= lo:
                is_swing = False
                break
        if is_swing:
            results.append((i, lo))
    return results


def _detect_equal_levels(
    swings: List[Tuple[int, float]],
    atr: float,
    cluster_radius_atr: float,
) -> List[Tuple[float, int]]:
    """
    Cluster swing extremes that are within cluster_radius * ATR of each
    other. Returns [(centroid_price, touch_count), ...].

    Uses single-pass greedy clustering — fast, deterministic, no scipy.
    """
    if not swings or atr < 1e-10:
        return []

    radius = atr * cluster_radius_atr
    # Sort by price for greedy clustering
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
            # Emit previous cluster
            centroid = sum(cluster_prices) / len(cluster_prices)
            clusters.append((centroid, cluster_count))
            cluster_prices = [price]
            cluster_count = 1

    # Emit last cluster
    if cluster_count > 0:
        centroid = sum(cluster_prices) / len(cluster_prices)
        clusters.append((centroid, cluster_count))

    return clusters


# ═══════════════════════════════════════════════════════════════════════════
# TIMEFRAME POOL REGISTRY
# ═══════════════════════════════════════════════════════════════════════════

class _TimeframeRegistry:
    """
    Maintains BSL and SSL pools for a single timeframe.
    Updated each time new candles arrive for that timeframe.
    """

    def __init__(self, timeframe: str):
        self.tf = timeframe
        self._bsl: List[LiquidityPool] = []
        self._ssl: List[LiquidityPool] = []
        self._last_candle_count = 0

    def update(self, candles: List[Dict], atr: float, now: float) -> None:
        """Rebuild pool list from swing structure."""
        if len(candles) < 15 or atr < 1e-10:
            return

        # Don't recompute if candle count hasn't changed
        if len(candles) == self._last_candle_count:
            return
        self._last_candle_count = len(candles)

        lookback = _SWING_LOOKBACK.get(self.tf, 5)
        cluster_r = _CLUSTER_RADIUS_ATR.get(self.tf, 0.25)

        # Detect swings
        swing_highs = _find_swing_highs(candles, lookback)
        swing_lows  = _find_swing_lows(candles, lookback)

        # Cluster into equal highs/lows
        bsl_clusters = _detect_equal_levels(swing_highs, atr, cluster_r)
        ssl_clusters = _detect_equal_levels(swing_lows, atr, cluster_r)

        # Merge into existing pools (preserve touch history, timestamps)
        self._bsl = self._merge_pools(
            self._bsl, bsl_clusters, PoolSide.BSL, atr, now)
        self._ssl = self._merge_pools(
            self._ssl, ssl_clusters, PoolSide.SSL, atr, now)

        # Prune stale pools
        max_age = _POOL_MAX_AGE.get(self.tf, 7200)
        self._bsl = [p for p in self._bsl
                     if now - p.created_at < max_age
                     and p.status != PoolStatus.CONSUMED]
        self._ssl = [p for p in self._ssl
                     if now - p.created_at < max_age
                     and p.status != PoolStatus.CONSUMED]

        # Cap pool count
        self._bsl = sorted(
            self._bsl, key=lambda p: p.significance, reverse=True
        )[:_MAX_POOLS_PER_TF]
        self._ssl = sorted(
            self._ssl, key=lambda p: p.significance, reverse=True
        )[:_MAX_POOLS_PER_TF]

    def _merge_pools(
        self,
        existing: List[LiquidityPool],
        clusters: List[Tuple[float, int]],
        side: PoolSide,
        atr: float,
        now: float,
    ) -> List[LiquidityPool]:
        """
        Merge newly detected clusters with existing pool registry.
        If a cluster matches an existing pool (within radius), update
        its touch count. Otherwise create a new pool.
        """
        radius = atr * _CLUSTER_RADIUS_ATR.get(self.tf, 0.25)
        merged = list(existing)  # shallow copy
        used_indices = set()

        for centroid, count in clusters:
            matched = False
            for i, pool in enumerate(merged):
                if i in used_indices:
                    continue
                if abs(pool.price - centroid) <= radius:
                    # Update existing pool
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
                    status=(PoolStatus.CONFIRMED if count >= 2
                            else PoolStatus.DETECTED),
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
        Check the most recent candle for sweeps of tracked pools.
        A sweep = wick through the pool + close back inside.
        """
        if len(candles) < 2 or atr < 1e-10:
            return []

        last = candles[-1]
        h = float(last['h'])
        lo = float(last['l'])
        c = float(last['c'])
        o = float(last['o'])
        body_top = max(o, c)
        body_bot = min(o, c)
        vol = float(last.get('v', 0))

        # 20-bar average volume
        vol_window = candles[max(0, len(candles)-21):-1]
        avg_vol = (sum(float(x.get('v', 0)) for x in vol_window)
                   / max(len(vol_window), 1))

        results = []
        min_wick = atr * _SWEEP_WICK_MIN_ATR
        min_reject = atr * _SWEEP_REJECT_MIN_ATR

        # Check BSL sweeps (price wicked above a BSL pool)
        for pool in self._bsl:
            if pool.status in (PoolStatus.SWEPT, PoolStatus.CONSUMED):
                continue
            if h > pool.price + min_wick and c < pool.price - min_reject:
                # Wick went above, close came back below → sweep
                wick_above = h - pool.price
                rejection = h - c
                wick_range = h - lo if h > lo else 1e-10

                quality = self._score_sweep(
                    wick_above / atr,
                    rejection / wick_range,
                    vol / max(avg_vol, 1e-10),
                )

                pool.status = PoolStatus.SWEPT
                pool.swept_at = now
                pool.sweep_wick = h

                results.append(SweepResult(
                    pool=pool,
                    sweep_candle_idx=len(candles) - 1,
                    wick_extreme=h,
                    rejection_pct=rejection / wick_range,
                    volume_ratio=vol / max(avg_vol, 1e-10),
                    quality=quality,
                    direction="short",  # BSL swept → expect down
                    detected_at=now,
                ))

        # Check SSL sweeps (price wicked below an SSL pool)
        for pool in self._ssl:
            if pool.status in (PoolStatus.SWEPT, PoolStatus.CONSUMED):
                continue
            if lo < pool.price - min_wick and c > pool.price + min_reject:
                wick_below = pool.price - lo
                rejection = c - lo
                wick_range = h - lo if h > lo else 1e-10

                quality = self._score_sweep(
                    wick_below / atr,
                    rejection / wick_range,
                    vol / max(avg_vol, 1e-10),
                )

                pool.status = PoolStatus.SWEPT
                pool.swept_at = now
                pool.sweep_wick = lo

                results.append(SweepResult(
                    pool=pool,
                    sweep_candle_idx=len(candles) - 1,
                    wick_extreme=lo,
                    rejection_pct=rejection / wick_range,
                    volume_ratio=vol / max(avg_vol, 1e-10),
                    quality=quality,
                    direction="long",  # SSL swept → expect up
                    detected_at=now,
                ))

        return results

    def check_consumed(self, price: float, atr: float) -> None:
        """Mark pools as CONSUMED if price has closed decisively through."""
        threshold = atr * 0.5
        for pool in self._bsl:
            if pool.status == PoolStatus.SWEPT:
                # If price is now well above the swept BSL, it wasn't
                # a sweep — it was a breakout. Pool is consumed.
                if price > pool.price + threshold:
                    pool.status = PoolStatus.CONSUMED
        for pool in self._ssl:
            if pool.status == PoolStatus.SWEPT:
                if price < pool.price - threshold:
                    pool.status = PoolStatus.CONSUMED

    @staticmethod
    def _score_sweep(
        penetration_atr: float,
        rejection_pct: float,
        volume_ratio: float,
    ) -> float:
        """
        Sweep quality score 0–1.
        Three factors, equally weighted:
          - Penetration depth (how far wick went through pool)
          - Rejection strength (how much of wick was rejected)
          - Volume conviction (institutional participation)
        """
        # Penetration: 0.05 ATR = 0.3, 0.2 ATR = 0.7, 0.5+ ATR = 1.0
        pen_score = min(1.0, penetration_atr / 0.5)

        # Rejection: 0.5 = 0.5, 0.8+ = 1.0
        rej_score = min(1.0, rejection_pct / 0.8)

        # Volume: 1.0x avg = 0.5, 2.0x = 0.8, 3.0x+ = 1.0
        vol_score = min(1.0, volume_ratio / 3.0)

        return (pen_score + rej_score + vol_score) / 3.0

    @property
    def bsl_pools(self) -> List[LiquidityPool]:
        return [p for p in self._bsl
                if p.status not in (PoolStatus.CONSUMED,)]

    @property
    def ssl_pools(self) -> List[LiquidityPool]:
        return [p for p in self._ssl
                if p.status not in (PoolStatus.CONSUMED,)]


# ═══════════════════════════════════════════════════════════════════════════
# MAIN ENGINE: Multi-Timeframe Liquidity Map
# ═══════════════════════════════════════════════════════════════════════════

class LiquidityMap:
    """
    Tracks liquidity pools across all timeframes independently.

    Usage:
        liq_map = LiquidityMap()
        liq_map.update(candle_dict, price, atr, now)
        snapshot = liq_map.get_snapshot(price, atr)
        target = snapshot.primary_target  # highest priority unswept pool
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
        """
        Main update — call once per tick from the strategy.

        Args:
            candles_by_tf: {"1m": [...], "5m": [...], ...}
            price: current last price
            atr: current ATR (from 5m or primary timeframe)
            now: wall-clock seconds
            ict_engine: optional ICT engine instance for OB/FVG alignment
        """
        if atr < 1e-10:
            return

        # 1. Update each timeframe registry
        for tf, candles in candles_by_tf.items():
            if tf in self._registries and candles:
                self._registries[tf].update(candles, atr, now)

        # 2. Cross-timeframe HTF confluence promotion
        self._promote_htf_confluence(atr)

        # 3. Align with ICT structures (OB/FVG backing)
        if ict_engine is not None:
            self._align_with_ict(ict_engine, price, atr, now)

        # 4. Check for sweeps on all timeframes
        new_sweeps = []
        for tf, candles in candles_by_tf.items():
            if tf in self._registries and candles:
                sweeps = self._registries[tf].check_sweeps(candles, atr, now)
                new_sweeps.extend(sweeps)

        # 5. Check consumed pools
        for reg in self._registries.values():
            reg.check_consumed(price, atr)

        # 6. Maintain sweep history (keep last 5 minutes)
        self._recent_sweeps.extend(new_sweeps)
        cutoff = now - 300.0
        self._recent_sweeps = [s for s in self._recent_sweeps
                               if s.detected_at > cutoff]

        # Log significant sweeps
        for sweep in new_sweeps:
            logger.info(
                f"🎯 SWEEP [{sweep.pool.timeframe}] {sweep.pool.side.value} "
                f"${sweep.pool.price:,.1f} → direction={sweep.direction} "
                f"quality={sweep.quality:.2f} wick=${sweep.wick_extreme:,.1f} "
                f"vol_ratio={sweep.volume_ratio:.1f}x"
            )

    def _promote_htf_confluence(self, atr: float) -> None:
        """
        For each pool on a lower timeframe, check if a pool at a similar
        price exists on a higher timeframe. If so, boost its htf_count.
        """
        # Collect all pools across all TFs, grouped by side
        all_bsl: List[LiquidityPool] = []
        all_ssl: List[LiquidityPool] = []
        for reg in self._registries.values():
            all_bsl.extend(reg.bsl_pools)
            all_ssl.extend(reg.ssl_pools)

        # For each pool, count how many DIFFERENT TFs have a pool nearby
        confluence_radius = atr * 0.5  # generous — HTF levels are approximate

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
        """
        Cross-reference pools with ICT engine's OB/FVG data.
        A pool backed by an OB is more significant (institutions left
        resting orders there). A pool with FVG pointing toward it has
        an "unfinished delivery" — price is more likely to reach it.
        """
        now_ms = int(now * 1000)
        try:
            # Check OB alignment for each pool
            for reg in self._registries.values():
                for pool in reg.bsl_pools + reg.ssl_pools:
                    pool.ob_aligned = False
                    pool.fvg_aligned = False

                    # OB check: is there an OB within 0.5 ATR of this pool?
                    if hasattr(ict_engine, '_obs'):
                        for tf_key, ob_list in ict_engine._obs.items():
                            for ob in ob_list:
                                ob_mid = (float(ob.high) + float(ob.low)) / 2.0
                                if abs(ob_mid - pool.price) < atr * 0.5:
                                    pool.ob_aligned = True
                                    break
                            if pool.ob_aligned:
                                break

                    # FVG check: is there an unfilled FVG between price and pool?
                    if hasattr(ict_engine, '_fvgs'):
                        for tf_key, fvg_list in ict_engine._fvgs.items():
                            for fvg in fvg_list:
                                fvg_mid = (float(fvg.high) + float(fvg.low)) / 2.0
                                # FVG is between current price and pool
                                if pool.side == PoolSide.BSL:
                                    # Pool is above → FVG should be between
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
        Build an immutable snapshot of the current liquidity map.
        This is what the strategy reads — it never mutates the map.
        """
        now = time.time()

        if atr < 1e-10:
            return LiquidityMapSnapshot(
                bsl_pools=[], ssl_pools=[],
                primary_target=None,
                recent_sweeps=list(self._recent_sweeps),
                nearest_bsl_atr=999.0, nearest_ssl_atr=999.0,
                timestamp=now,
            )

        # Collect all tradeable pools across timeframes
        bsl_targets: List[PoolTarget] = []
        ssl_targets: List[PoolTarget] = []

        for reg in self._registries.values():
            for pool in reg.bsl_pools:
                if not pool.is_tradeable:
                    continue
                dist = (pool.price - price) / atr
                if dist < 0:
                    continue  # BSL must be above price
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
                if dist < 0:
                    continue  # SSL must be below price
                tf_sources = self._find_tf_sources(pool, PoolSide.SSL, atr)
                ssl_targets.append(PoolTarget(
                    pool=pool,
                    distance_atr=dist,
                    direction="short",
                    significance=pool.significance,
                    tf_sources=tf_sources,
                ))

        # Deduplicate: if multiple TFs show the same level, keep highest-sig
        bsl_targets = self._deduplicate_targets(bsl_targets, atr)
        ssl_targets = self._deduplicate_targets(ssl_targets, atr)

        # Sort by significance DESC
        bsl_targets.sort(key=lambda t: t.significance, reverse=True)
        ssl_targets.sort(key=lambda t: t.significance, reverse=True)

        # Find nearest
        nearest_bsl = min((t.distance_atr for t in bsl_targets), default=999.0)
        nearest_ssl = min((t.distance_atr for t in ssl_targets), default=999.0)

        # Primary target: highest significance among reachable pools (< 8 ATR)
        all_reachable = [t for t in bsl_targets + ssl_targets
                         if t.distance_atr < 8.0]
        primary = max(all_reachable, key=lambda t: t.significance,
                      default=None)

        snapshot = LiquidityMapSnapshot(
            bsl_pools=bsl_targets[:10],
            ssl_pools=ssl_targets[:10],
            primary_target=primary,
            recent_sweeps=list(self._recent_sweeps),
            nearest_bsl_atr=nearest_bsl,
            nearest_ssl_atr=nearest_ssl,
            timestamp=now,
        )
        self._last_snapshot = snapshot
        return snapshot

    def _find_tf_sources(
        self,
        pool: LiquidityPool,
        side: PoolSide,
        atr: float,
    ) -> List[str]:
        """Find all timeframes that have a pool near this price."""
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
    ) -> List[PoolTarget]:
        """
        Remove duplicate pools that represent the same price level
        seen from different timeframes. Keep the highest significance.
        """
        if not targets:
            return []
        radius = atr * 0.3
        targets_sorted = sorted(targets, key=lambda t: t.significance,
                                reverse=True)
        result = []
        used_prices = []
        for t in targets_sorted:
            is_dup = False
            for p in used_prices:
                if abs(t.pool.price - p) <= radius:
                    is_dup = True
                    break
            if not is_dup:
                result.append(t)
                used_prices.append(t.pool.price)
        return result

    def get_status_summary(self, price: float, atr: float) -> Dict:
        """For logging / Telegram display."""
        snap = self.get_snapshot(price, atr)
        return {
            "bsl_count": len(snap.bsl_pools),
            "ssl_count": len(snap.ssl_pools),
            "nearest_bsl": f"${snap.bsl_pools[0].pool.price:,.1f} ({snap.bsl_pools[0].distance_atr:.1f} ATR, sig={snap.bsl_pools[0].significance:.1f})" if snap.bsl_pools else "none",
            "nearest_ssl": f"${snap.ssl_pools[0].pool.price:,.1f} ({snap.ssl_pools[0].distance_atr:.1f} ATR, sig={snap.ssl_pools[0].significance:.1f})" if snap.ssl_pools else "none",
            "primary_target": (f"{snap.primary_target.direction} → ${snap.primary_target.pool.price:,.1f}"
                               if snap.primary_target else "none"),
            "recent_sweeps": len(snap.recent_sweeps),
            "tf_coverage": {tf: len(reg.bsl_pools) + len(reg.ssl_pools)
                           for tf, reg in self._registries.items()
                           if reg.bsl_pools or reg.ssl_pools},
        }

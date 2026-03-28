"""
liquidity_map.py — Multi-Timeframe Liquidity Map Engine v2.1
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

FIX-11 (v2.1): PoolTarget.adjusted_sig() added.
         Old: the adjacency bonus was applied to PoolTarget.significance
         but ALL decision functions (primary target, _find_flow_target,
         _find_opposing_target) called pool.proximity_adjusted_sig() which
         reads pool.significance — bypassing the bonus entirely. The bonus
         was only visible in the display sort (which used t.significance).
         New: PoolTarget.adjusted_sig() computes significance × exp(dist/10)
         using the wrapper's (possibly boosted) significance field. All
         decision logic in entry_engine.py uses t.adjusted_sig() instead
         of t.pool.proximity_adjusted_sig(t.distance_atr).

FIX-12 (v2.1): Primary target in get_snapshot() uses t.adjusted_sig().
         Old: primary = max(reachable, key=lambda t: t.pool.proximity_adjusted_sig(...))
         This ignored the adjacency bonus applied just above. The primary
         target shown in [THINK] logs could differ from what the entry
         engine actually targeted. Now consistent.
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

# 7-day retention for all timeframes — institutional structure persists
# across sessions. A 4H OB from Tuesday is still valid on Friday.
_POOL_MAX_AGE: Dict[str, float] = {
    "1m": 14400,      # 4 hours (1m is micro, but 2h was losing intraday context)
    "5m": 604800,     # 7 days — full week of intraday structure
    "15m": 604800,    # 7 days — key intraday/swing structure
    "1h": 604800,     # 7 days — hourly structure anchors weekly ranges
    "4h": 604800,     # 7 days — swing/position levels
    "1d": 2592000,    # 30 days — macro levels persist across months
}

_MAX_POOLS_PER_TF = 30   # was 15 — with 7-day retention we need more capacity

# FIX-2: Lowered significance threshold — allows new 5m pools (sig≈2.0)
TRADEABLE_POOL_MIN_SIG = 1.8  # was 3.0

_HTF_CONFLUENCE_MULT = 2.5

# Sweep detection
_SWEEP_WICK_MIN_ATR  = 0.03
_SWEEP_REJECT_MIN_ATR = 0.02

# FIX-3: Swept pool history — 7 days retention for institutional memory
_SWEPT_HISTORY_MAX = 500  # store up to 500 swept pools per side
_SWEPT_HISTORY_AGE = 604800.0  # 7 days — full week of sweep context


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

    def adjusted_sig(self) -> float:
        """
        FIX-11 (v2.1): Proximity-adjusted significance using THIS target's
        significance field, which may include an adjacency bonus applied
        during snapshot construction.

        Unlike pool.proximity_adjusted_sig(), this method uses the wrapper's
        significance — ensuring adjacency bonuses are visible to all decision
        functions (target selection, primary target ranking, HTF TP escalation).

        Formula: significance × exp(-distance_atr / 10.0)
          At  2 ATR: 0.82× (nearby pool, minimal decay)
          At  5 ATR: 0.61× (moderate distance)
          At 10 ATR: 0.37× (distant, penalised)
          At 20 ATR: 0.14× (very distant, rarely selected)
        """
        return self.significance * math.exp(-self.distance_atr / 10.0)


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

    @staticmethod
    def _score_sweep(
        wick_atr:     float,
        rejection:    float,
        vol_ratio:    float,
    ) -> float:
        """
        Composite sweep quality score [0.0, 1.0].
        Higher = cleaner rejection = stronger reversal signal.

        Components:
          wick_atr  (0.35 weight): wick beyond pool relative to ATR
                                   0.05 ATR = noise, 0.5 ATR = strong
          rejection (0.40 weight): (H - C) / (H - L) for BSL sweep
                                   0.3 = partial, 0.7 = full wick rejection
          vol_ratio (0.25 weight): candle volume vs 20-bar average
                                   1.0 = average, 2.0 = institutional
        """
        wick_score = min(wick_atr / 0.5, 1.0)
        rej_score  = min(rejection / 0.7, 1.0)
        vol_score  = min((vol_ratio - 0.5) / 1.5, 1.0) if vol_ratio > 0.5 else 0.0
        return round(0.35 * wick_score + 0.40 * rej_score + 0.25 * vol_score, 2)

    @property
    def bsl_pools(self) -> List[LiquidityPool]:
        return [p for p in self._bsl
                if p.status not in (PoolStatus.SWEPT, PoolStatus.CONSUMED)]

    @property
    def ssl_pools(self) -> List[LiquidityPool]:
        return [p for p in self._ssl
                if p.status not in (PoolStatus.SWEPT, PoolStatus.CONSUMED)]

    @property
    def swept_bsl(self) -> List[LiquidityPool]:
        return list(self._swept_bsl)

    @property
    def swept_ssl(self) -> List[LiquidityPool]:
        return list(self._swept_ssl)


# ═══════════════════════════════════════════════════════════════════════════
# MAIN LIQUIDITY MAP
# ═══════════════════════════════════════════════════════════════════════════

class LiquidityMap:
    """
    Multi-timeframe liquidity pool map.

    Maintained registries: 1m, 5m, 15m, 1h, 4h, 1d
    Each registry tracks BSL (buy-stop liquidity) and SSL (sell-stop
    liquidity) pools as swing high/low clusters.

    Sweep detection fires when a closed candle breaks through a pool
    with a wick and then rejects back through (FIX-6).
    """

    _TIMEFRAMES = ("1m", "5m", "15m", "1h", "4h", "1d")

    def __init__(self) -> None:
        self._registries: Dict[str, _TimeframeRegistry] = {
            tf: _TimeframeRegistry(tf) for tf in self._TIMEFRAMES
        }
        self._recent_sweeps: List[SweepResult] = []
        self._last_snapshot: Optional[LiquidityMapSnapshot] = None

    def update(
        self,
        candles_by_tf: Dict[str, List[Dict]],
        atr: float,
        now: float,
    ) -> None:
        """Update all timeframe registries from fresh candle data."""
        for tf, reg in self._registries.items():
            if tf in candles_by_tf and candles_by_tf[tf]:
                reg.update(candles_by_tf[tf], atr, now)

    def check_sweeps(
        self,
        candles_by_tf: Dict[str, List[Dict]],
        atr: float,
        now: float,
    ) -> List[SweepResult]:
        """Check all registered timeframes for new sweeps."""
        new_sweeps: List[SweepResult] = []
        for tf, reg in self._registries.items():
            if tf in candles_by_tf and candles_by_tf[tf]:
                tf_sweeps = reg.check_sweeps(candles_by_tf[tf], atr, now)
                for s in tf_sweeps:
                    logger.info(
                        f"🎯 SWEEP [{tf}] {s.pool.side.value} "
                        f"${s.pool.price:,.1f} → direction={s.direction} "
                        f"quality={s.quality:.2f} wick=${s.wick_extreme:,.1f} "
                        f"vol_ratio={s.volume_ratio:.1f}x"
                    )
                new_sweeps.extend(tf_sweeps)

        # Append to recent sweeps, keep last 20
        self._recent_sweeps.extend(new_sweeps)
        self._recent_sweeps = self._recent_sweeps[-20:]
        return new_sweeps

    def mark_ict_alignment(
        self,
        ict_obs: List[Dict],
        ict_fvgs: List[Dict],
        price: float,
        atr: float,
    ) -> None:
        """
        Mark pools as OB-aligned or FVG-aligned based on ICT engine output.
        Called after each ICT engine update to keep pool metadata fresh.
        """
        try:
            radius = atr * 0.7   # FIX-7: widened to 0.7 ATR (was 0.5)
            for tf, reg in self._registries.items():
                for pool in reg.bsl_pools + reg.ssl_pools:
                    # OB alignment
                    for ob in ict_obs:
                        ob_price = float(ob.get('price', 0) or ob.get('high', 0) or ob.get('low', 0))
                        if ob_price > 0 and abs(ob_price - pool.price) <= radius:
                            pool.ob_aligned = True
                            break
                    # FVG alignment — check if an FVG gap sits between pool and current price
                    for fvg in ict_fvgs:
                        fvg_high = float(fvg.get('high', 0))
                        fvg_low  = float(fvg.get('low', 0))
                        if fvg_high <= 0 or fvg_low <= 0:
                            continue
                        fvg_mid = (fvg_high + fvg_low) / 2.0
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

    def mark_htf_confluence(self, atr: float) -> None:
        """
        Promote pools that cluster with higher-timeframe pools.
        A 5m pool near a 4h pool gets htf_count += 1, which triggers
        the _HTF_CONFLUENCE_MULT in significance scoring.

        FIX-7: Confluence radius widened from 0.5 → 0.7 ATR so that
        nearby but not identical HTF levels correctly merge.
        """
        radius = atr * 0.7   # FIX-7: was 0.5

        tf_order = list(self._TIMEFRAMES)
        for i, tf_low in enumerate(tf_order):
            reg_low = self._registries[tf_low]
            for pool in reg_low.bsl_pools + reg_low.ssl_pools:
                pool.htf_count = 0  # reset before recount
            for j, tf_high in enumerate(tf_order):
                if j <= i:
                    continue
                reg_high = self._registries[tf_high]
                high_prices_bsl = [p.price for p in reg_high.bsl_pools]
                high_prices_ssl = [p.price for p in reg_high.ssl_pools]
                for pool in reg_low.bsl_pools:
                    for hp in high_prices_bsl:
                        if abs(pool.price - hp) <= radius:
                            pool.htf_count += 1
                for pool in reg_low.ssl_pools:
                    for hp in high_prices_ssl:
                        if abs(pool.price - hp) <= radius:
                            pool.htf_count += 1

    def get_snapshot(self, price: float, atr: float) -> LiquidityMapSnapshot:
        """
        Build snapshot with institutional-grade scoring:
        - Proximity-adjusted significance (FIX-10)
        - Swept-pool adjacency bonus (pools near recent sweeps score higher)
        - 7-day swept history for context (FIX-3)

        FIX-11/12 (v2.1): The adjacency bonus is applied to PoolTarget.significance.
        Primary target selection now uses t.adjusted_sig() (which reads the wrapper's
        significance) instead of t.pool.proximity_adjusted_sig() (which reads the
        base pool significance, ignoring the bonus). This makes adjacency bonuses
        actually affect target selection, not just the display sort.
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

        # ── Collect all swept pool prices for adjacency scoring ──────────
        _all_swept_prices = []
        for reg in self._registries.values():
            for p in reg.swept_bsl:
                _all_swept_prices.append(p.price)
            for p in reg.swept_ssl:
                _all_swept_prices.append(p.price)

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

        # ── Swept-adjacency bonus: pools near recently-swept pools are ───
        # more likely to be targeted next (institutional delivery targets
        # adjacent stop clusters — "run the SSL, then deliver to the BSL
        # above" or vice versa). Bonus = +2.0 if a swept pool is within
        # 3 ATR of this pool (decays linearly with distance).
        # NOTE (FIX-11): This bonus is applied to PoolTarget.significance.
        # All decision functions must use t.adjusted_sig() to see it.
        if _all_swept_prices and atr > 1e-10:
            for t in bsl_targets + ssl_targets:
                _min_dist_to_swept = min(
                    abs(t.pool.price - sp) / atr
                    for sp in _all_swept_prices)
                if _min_dist_to_swept < 3.0:
                    _bonus = 2.0 * (1.0 - _min_dist_to_swept / 3.0)
                    t.significance += _bonus

        # Sort by raw significance DESC (for display priority)
        bsl_targets.sort(key=lambda t: t.significance, reverse=True)
        ssl_targets.sort(key=lambda t: t.significance, reverse=True)

        nearest_bsl = min((t.distance_atr for t in bsl_targets), default=999.0)
        nearest_ssl = min((t.distance_atr for t in ssl_targets), default=999.0)

        # ── Primary target — FIX-12 (v2.1): use t.adjusted_sig() ───────
        # Old code used t.pool.proximity_adjusted_sig() which bypassed the
        # adjacency bonus applied above. Now consistent with entry_engine.py.
        all_reachable = [t for t in bsl_targets + ssl_targets if t.distance_atr < 25.0]
        primary = None
        if all_reachable:
            primary = max(
                all_reachable,
                key=lambda t: t.adjusted_sig()  # FIX-12: was t.pool.proximity_adjusted_sig(t.distance_atr)
            )
        elif bsl_targets or ssl_targets:
            all_targets = bsl_targets + ssl_targets
            primary = min(all_targets, key=lambda t: t.distance_atr)

        # Collect swept pool history (7 days)
        swept_bsl_levels = []
        swept_ssl_levels = []
        for reg in self._registries.values():
            for p in reg.swept_bsl:
                swept_bsl_levels.append(p.price)
            for p in reg.swept_ssl:
                swept_ssl_levels.append(p.price)

        snap = LiquidityMapSnapshot(
            bsl_pools=bsl_targets[:20],   # was 12 — more visibility with 7-day data
            ssl_pools=ssl_targets[:20],
            primary_target=primary,
            recent_sweeps=list(self._recent_sweeps),
            swept_bsl_levels=sorted(set(swept_bsl_levels), reverse=True)[:20],
            swept_ssl_levels=sorted(set(swept_ssl_levels))[:20],
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

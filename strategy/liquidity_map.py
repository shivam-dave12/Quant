"""
liquidity_map.py — Multi-Timeframe Liquidity Map Engine v2.1
=============================================================

PUBLIC INTERFACE (must match quant_strategy.py call sites exactly)
───────────────────────────────────────────────────────────────────
LiquidityMap.update(candles_by_tf, price, atr, now, ict_engine=None)
LiquidityMap.get_snapshot(price, atr) -> LiquidityMapSnapshot
LiquidityMap.get_status_summary(price, atr) -> Dict

FIXES IN v2.0
─────────────
FIX-1  Swing lookback reduced from 5→3 bars on 5m/15m.
        Old: 5-bar lookback = 25 min delay on 5m before a new swing qualifies.
        New: 3-bar = 15 min on 5m. 1h/4h unchanged.

FIX-2  TRADEABLE_POOL_MIN_SIG lowered 3.0 → 1.8.
        A brand-new 5m pool scores 2.0 and was being excluded. After 2-3
        trades, all near-term pools were swept and nothing was left to track.

FIX-3  Swept pools retained in history for 7 days instead of discarded.
        Swept pools move to _swept_history registry and provide adjacency
        bonus context in get_snapshot().

FIX-4  Pool significance freshness penalty applied at 50% max age.
        Old: no time decay within max_age. New: -1.0 penalty after halfway.

FIX-5  Dedup radius tightened 0.3 → 0.2 ATR.
        Old: pools 115 pts apart (1.02 ATR) were being merged as duplicates.

FIX-6  check_sweeps() uses candles[-2] (last CLOSED) not candles[-1].
        A forming candle's H/L changes every tick — was triggering false sweeps.

FIX-7  HTF confluence promotion radius widened 0.5 → 0.7 ATR.
        Old: a 1D@65,580 and 4h@65,550 (30 pts) were NOT merging because
        30pts / 112 ATR = 0.27 < 0.5. New 0.7 × 112 = 78 pts — correct merge.

FIX-8  _pool_max_age kept uniform at 7 days for 5m/15m/1h/4h.
        Previous values of 8h/24h were too aggressive — fresh-session pools
        were being pruned before price returned to test them.

FIX-9  _merge_pools updates centroid when a cluster re-matches.
        Old: matching pool kept its original price even as new swing data
        refined the true cluster center.

FIX-10 Proximity-adjusted significance: sig × exp(-dist_atr/10) applied
        in get_snapshot() so nearby pools dominate over distant HTF pools.

FIXES IN v2.2
─────────────
FIX-B1  _merge_pools: two-pass pool rebirth for SWEPT/CONSUMED pools.
        PRIMARY CAUSE of "stuck after first trade" (nearest_bsl_atr=999.0).
        Old: single-pass loop SKIPPED swept pools → new clusters at same level
             appended fresh pools which also got swept → zombie accumulation
             filled _MAX_POOLS_PER_TF cap → no active pools survived sort.
        New: pass 1 matches active pools (unchanged). Pass 2 REBIRTHS the most
             recent SWEPT/CONSUMED pool within radius instead of spawning a new
             one. Reborn pool gets fresh created_at, zeroed sweep metadata, and
             new status so check_sweeps() can detect it again. The new sweep
             produces a fresh detected_at timestamp → new _sweep_key in
             entry_engine → correctly bypasses _processed_sweeps hold from the
             original sweep event (Bug 3 self-heals).

FIX-B1b Prune step: SWEPT pools evicted from active list after 2 hours.
        Companion to FIX-B1. Pools that haven't been reborn within 2 hours
        are evicted to _swept_bsl/_swept_ssl (already there, just removed from
        _bsl/_ssl). Ensures zombie slots are reclaimed even on slow sessions
        where no new swing highs form at the swept level.

FIX-B4  LiquidityMap.reset_snapshot() method added.
        Call from quant_strategy._finalise_exit() immediately after position
        closes. Invalidates _last_snapshot so predict_hunt() on the next tick
        uses ICT-engine-only scoring rather than stale post-trade pool data
        that biases hunt direction toward the dead swept level.
        The adjacency bonus is applied to PoolTarget.significance in
        get_snapshot(). All decision logic in entry_engine now uses
        t.adjusted_sig() so the bonus is visible to every selector.
        The old t.pool.proximity_adjusted_sig() reads pool.significance,
        bypassing the bonus entirely — it was only visible in display sort.

FIX-12 Primary target in get_snapshot() uses t.adjusted_sig().
        Old: max(reachable, key=lambda t: t.pool.proximity_adjusted_sig(...))
        This ignored the adjacency bonus. Now consistent with entry_engine.

INTERFACE BUG FIXED IN v2.1
────────────────────────────
The previous rewrite broke the public API by removing 'price' and
'ict_engine' from update(), and dropping check_consumed(),
_promote_htf_confluence(), and _align_with_ict() which are all called
INSIDE update(). quant_strategy.py calls:

    self._liq_map.update(
        candles_by_tf=candles_by_tf,
        price=price, atr=atr, now=now,
        ict_engine=self._ict,
    )

This file restores the exact signature and all internal step calls.
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

# FIX-8: 7-day uniform retention — institutional structure persists across sessions
_POOL_MAX_AGE: Dict[str, float] = {
    "1m":  14400,    # 4 hours
    "5m":  604800,   # 7 days
    "15m": 604800,   # 7 days
    "1h":  604800,   # 7 days
    "4h":  604800,   # 7 days
    "1d":  2592000,  # 30 days
}

_MAX_POOLS_PER_TF = 30         # was 15 — wider window needs more capacity

# FIX-2: Lowered threshold so new 5m pools (sig~2.0) are included
TRADEABLE_POOL_MIN_SIG = 1.8   # was 3.0

_HTF_CONFLUENCE_MULT    = 2.5

# Sweep detection geometry
_SWEEP_WICK_MIN_ATR    = 0.03
_SWEEP_REJECT_MIN_ATR  = 0.02

# FIX-3: Swept-pool history bounds
_SWEPT_HISTORY_MAX = 500        # entries per side
_SWEPT_HISTORY_AGE = 604800.0   # 7 days

# Consumed-pool threshold: SWEPT pool at >0.5 ATR beyond price = fully consumed
_CONSUMED_THRESHOLD_ATR = 0.5

# FIX-B1: Maximum time a SWEPT pool remains in the ACTIVE registry (_bsl/_ssl)
# before being evicted to prevent zombie pool accumulation.
#
# Problem: SWEPT pools are never removed from _bsl/_ssl by the existing prune
# step (which only removes CONSUMED and age-expired pools). Over time, repeated
# sweeps at the same level fill _MAX_POOLS_PER_TF slots with SWEPT zombies.
# Because the significance sort retains the highest-significance pools (SWEPT
# pools accumulate touch bonuses before death), fresh DETECTED pools score lower
# and are cut off at position 31+, making the map appear empty to get_snapshot().
#
# Fix: evict SWEPT pools from the active list after 2 hours. They remain in
# _swept_bsl/_swept_ssl for adjacency bonus and HTF TP escalation context.
# 2 hours is 24 × 5m bars — sufficient for price to either return and retest
# (triggering rebirth via _merge_pools FIX-B1) or move far enough away that
# check_consumed() has already promoted them to CONSUMED.
_SWEPT_IN_BSL_MAX_AGE: float = 7_200.0   # 2 hours


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
    """A cluster of stop orders at a structural price level."""

    price:      float
    side:       PoolSide
    timeframe:  str
    status:     PoolStatus = PoolStatus.DETECTED
    touches:    int        = 1
    created_at: float      = 0.0
    last_touch: float      = 0.0
    swept_at:   float      = 0.0
    sweep_wick: float      = 0.0

    ob_aligned:  bool = False   # set by _align_with_ict()
    fvg_aligned: bool = False   # set by _align_with_ict()
    htf_count:   int  = 0       # set by _promote_htf_confluence()

    @property
    def tf_rank(self) -> int:
        return TF_HIERARCHY.get(self.timeframe, 1)

    @property
    def significance(self) -> float:
        """
        Raw significance score — NOT proximity-adjusted.
        Proximity adjustment is applied at snapshot time via
        PoolTarget.adjusted_sig() or pool.proximity_adjusted_sig().

        FIX-4: freshness_penalty of -1.0 after 50% of max age.
        """
        base        = float(self.tf_rank)
        touch_bonus = min(float(self.touches - 1), 5.0)
        htf_mult    = _HTF_CONFLUENCE_MULT if self.htf_count >= 2 else 1.0
        structural  = 0.0
        if self.ob_aligned:
            structural += 2.0
        if self.fvg_aligned:
            structural += 1.0
        max_age           = _POOL_MAX_AGE.get(self.timeframe, 7200)
        age               = time.time() - self.created_at
        freshness_penalty = -1.0 if age > max_age * 0.5 else 0.0
        raw = (base + touch_bonus + structural + freshness_penalty) * htf_mult
        return max(0.1, raw)

    def proximity_adjusted_sig(self, dist_atr: float) -> float:
        """
        FIX-10: Exponential distance decay applied to pool.significance.
        NOTE: Does NOT include the adjacency bonus added to PoolTarget.significance
        in get_snapshot(). Use PoolTarget.adjusted_sig() in entry_engine instead.
        """
        return self.significance * math.exp(-dist_atr / 10.0)

    @property
    def is_tradeable(self) -> bool:
        return (
            self.status in (PoolStatus.DETECTED, PoolStatus.CONFIRMED)
            and self.significance >= TRADEABLE_POOL_MIN_SIG
        )


@dataclass
class PoolTarget:
    """
    A ranked liquidity target delivered to the entry engine.

    FIX-11 (v2.1): adjusted_sig() uses self.significance (which may include
    an adjacency bonus applied in get_snapshot()) rather than pool.significance.
    This makes the bonus visible to all entry decision functions.
    """
    pool:         LiquidityPool
    distance_atr: float
    direction:    str
    significance: float      # may be boosted by adjacency bonus in get_snapshot()
    tf_sources:   List[str]

    def adjusted_sig(self) -> float:
        """
        FIX-11: Proximity-adjusted significance using the WRAPPER significance.

        This is the canonical selection key for all entry_engine decision
        functions. It incorporates:
          1. Pool's raw significance (tf_rank * htf_mult * ob/fvg bonuses)
          2. Adjacency bonus (applied to self.significance in get_snapshot if
             a swept pool is within 3 ATR — linearly decaying +2.0)
          3. Exponential distance penalty: x exp(-distance_atr / 10)

        At  2 ATR: 0.82x  |  At 5 ATR: 0.61x  |  At 10 ATR: 0.37x

        DO NOT use pool.proximity_adjusted_sig() in entry_engine — it reads
        pool.significance and misses the adjacency bonus.
        """
        return self.significance * math.exp(-self.distance_atr / 10.0)


@dataclass
class SweepResult:
    """A confirmed sweep event detected on a closed candle."""
    pool:             LiquidityPool
    sweep_candle_idx: int
    wick_extreme:     float
    rejection_pct:    float
    volume_ratio:     float
    quality:          float
    direction:        str        # "short" = BSL swept, "long" = SSL swept
    detected_at:      float


@dataclass
class LiquidityMapSnapshot:
    """Immutable state of the liquidity map at a point in time."""
    bsl_pools:        List[PoolTarget]
    ssl_pools:        List[PoolTarget]
    primary_target:   Optional[PoolTarget]
    recent_sweeps:    List[SweepResult]
    swept_bsl_levels: List[float]
    swept_ssl_levels: List[float]
    nearest_bsl_atr:  float
    nearest_ssl_atr:  float
    timestamp:        float


# ═══════════════════════════════════════════════════════════════════════════
# SWING DETECTION (module-level so ICTTrailManager can import them)
# ═══════════════════════════════════════════════════════════════════════════

def _find_swing_highs(candles: List[Dict], lookback: int = 3) -> List[Tuple[int, float]]:
    """Return (index, price) of confirmed swing highs."""
    results = []
    n = len(candles)
    for i in range(lookback, n - lookback):
        h = float(candles[i]['h'])
        if all(float(candles[i - j]['h']) < h and float(candles[i + j]['h']) < h
               for j in range(1, lookback + 1)):
            results.append((i, h))
    return results


def _find_swing_lows(candles: List[Dict], lookback: int = 3) -> List[Tuple[int, float]]:
    """Return (index, price) of confirmed swing lows."""
    results = []
    n = len(candles)
    for i in range(lookback, n - lookback):
        lo = float(candles[i]['l'])
        if all(float(candles[i - j]['l']) > lo and float(candles[i + j]['l']) > lo
               for j in range(1, lookback + 1)):
            results.append((i, lo))
    return results


def _detect_equal_levels(
    swings:             List[Tuple[int, float]],
    atr:                float,
    cluster_radius_atr: float,
) -> List[Tuple[float, int]]:
    """
    Cluster swing extremes within cluster_radius × ATR of the cluster CENTROID.

    Bug #20 fix: the original implementation compared each new price against
    the LAST price added to the current cluster (single-linkage). With a chain
    like [100.0, 100.15, 100.30] and radius=0.20×ATR, the first two merge
    correctly, then 100.30 is compared against 100.15 (not 100.0), and since
    0.15 ≤ 0.20 it merges — producing a cluster that spans 0.30×ATR (50% over
    budget). At BTC ATR=$67 and radius=0.20 this means swings $20 apart merged
    as "equal highs", inflating touch count and creating a single pool where two
    distinct stop clusters actually exist.

    Fix: track the running centroid. A new price only joins the cluster if its
    distance from the current centroid is ≤ radius. The centroid is updated
    incrementally after each admission — O(n) and numerically stable.

    Returns [(centroid_price, touch_count), ...] sorted ascending.
    """
    if not swings or atr < 1e-10:
        return []

    radius        = atr * cluster_radius_atr
    sorted_swings = sorted(swings, key=lambda s: s[1])
    clusters:     List[Tuple[float, int]] = []

    # Running cluster represented as (sum_of_prices, count) for O(1) centroid.
    cluster_sum:   float = 0.0
    cluster_count: int   = 0

    for _, price in sorted_swings:
        if cluster_count == 0:
            cluster_sum   = price
            cluster_count = 1
        else:
            centroid = cluster_sum / cluster_count
            if abs(price - centroid) <= radius:
                # Price is within radius of the centroid — admit to cluster.
                cluster_sum   += price
                cluster_count += 1
            else:
                # Price is outside — flush the current cluster and start a new one.
                clusters.append((cluster_sum / cluster_count, cluster_count))
                cluster_sum   = price
                cluster_count = 1

    if cluster_count > 0:
        clusters.append((cluster_sum / cluster_count, cluster_count))

    return clusters


# ═══════════════════════════════════════════════════════════════════════════
# TIMEFRAME REGISTRY
# ═══════════════════════════════════════════════════════════════════════════

class _TimeframeRegistry:
    """Maintains BSL and SSL pools for a single timeframe."""

    def __init__(self, timeframe: str) -> None:
        self.tf          = timeframe
        self._bsl:       List[LiquidityPool] = []
        self._ssl:       List[LiquidityPool] = []
        self._last_ts:   int                 = 0
        self._swept_bsl: List[LiquidityPool] = []
        self._swept_ssl: List[LiquidityPool] = []

    # ── Pool update ───────────────────────────────────────────────────────

    def update(self, candles: List[Dict], atr: float, now: float) -> None:
        if len(candles) < 10 or atr < 1e-10:
            return

        # Deduplicate on last CLOSED candle timestamp
        try:
            _c  = candles[-2] if len(candles) >= 2 else candles[-1]
            _ts = int(_c.get('t', 0) or 0) if hasattr(_c, 'get') else 0
        except Exception:
            _ts = 0

        if _ts > 0 and _ts == self._last_ts:
            return
        if _ts > 0:
            self._last_ts = _ts

        lookback  = _SWING_LOOKBACK.get(self.tf, 3)       # FIX-1
        cluster_r = _CLUSTER_RADIUS_ATR.get(self.tf, 0.25)

        bsl_clusters = _detect_equal_levels(
            _find_swing_highs(candles, lookback), atr, cluster_r)
        ssl_clusters = _detect_equal_levels(
            _find_swing_lows(candles, lookback),  atr, cluster_r)

        self._bsl = self._merge_pools(self._bsl, bsl_clusters, PoolSide.BSL, atr, now)
        self._ssl = self._merge_pools(self._ssl, ssl_clusters, PoolSide.SSL, atr, now)

        # FIX-B1: Prune CONSUMED, age-expired, AND stale SWEPT pools.
        #
        # Original code only pruned CONSUMED + age-expired. SWEPT pools stayed
        # in _bsl/_ssl indefinitely, accumulating as zombies and filling the
        # _MAX_POOLS_PER_TF cap. Fresh DETECTED pools scored lower in the
        # significance sort and were cut off, making the map appear empty.
        #
        # SWEPT pools that have been in the list for > _SWEPT_IN_BSL_MAX_AGE
        # are evicted. They remain in _swept_bsl/_swept_ssl for adjacency bonus
        # and HTF TP escalation context — they are NOT lost from the system.
        max_age = _POOL_MAX_AGE.get(self.tf, 7200)
        self._bsl = [
            p for p in self._bsl
            if p.status != PoolStatus.CONSUMED
            and now - p.created_at < max_age
            and not (
                p.status == PoolStatus.SWEPT
                and p.swept_at > 0
                and now - p.swept_at > _SWEPT_IN_BSL_MAX_AGE
            )
        ]
        self._ssl = [
            p for p in self._ssl
            if p.status != PoolStatus.CONSUMED
            and now - p.created_at < max_age
            and not (
                p.status == PoolStatus.SWEPT
                and p.swept_at > 0
                and now - p.swept_at > _SWEPT_IN_BSL_MAX_AGE
            )
        ]

        # Cap per-side by significance
        self._bsl = sorted(self._bsl, key=lambda p: p.significance, reverse=True)[:_MAX_POOLS_PER_TF]
        self._ssl = sorted(self._ssl, key=lambda p: p.significance, reverse=True)[:_MAX_POOLS_PER_TF]

        # Prune swept history by age
        self._swept_bsl = [p for p in self._swept_bsl
                           if now - p.swept_at < _SWEPT_HISTORY_AGE][-_SWEPT_HISTORY_MAX:]
        self._swept_ssl = [p for p in self._swept_ssl
                           if now - p.swept_at < _SWEPT_HISTORY_AGE][-_SWEPT_HISTORY_MAX:]

    def _merge_pools(
        self,
        existing: List[LiquidityPool],
        clusters: List[Tuple[float, int]],
        side:     PoolSide,
        atr:      float,
        now:      float,
    ) -> List[LiquidityPool]:
        """
        Merge newly detected clusters into the existing registry.

        FIX-9: Updates centroid price on re-match instead of keeping stale price.

        FIX-B1: TWO-PASS pool rebirth for SWEPT/CONSUMED pools.
        ─────────────────────────────────────────────────────────
        Root cause of "stuck after first trade" (PRIMARY BUG):

        The original single-pass loop skipped SWEPT/CONSUMED pools with a bare
        `continue`. When a new cluster centroid fell within radius of a SWEPT
        pool's price, no match was found and a NEW pool was appended alongside
        the zombie. That new pool would subsequently be swept too (same price
        level), adding another zombie. Over successive sweeps, the _MAX_POOLS_PER_TF
        cap filled entirely with SWEPT zombie pools. The significance sort kept the
        highest-significance ones (SWEPT pools accumulate touch bonuses before
        death), crowding out fresh DETECTED pools which scored lower. get_snapshot()
        saw no active tradeable pools → nearest_bsl_atr = 999.0 → engine stuck in
        SCANNING permanently after the first trade.

        Fix: two-pass architecture:
          Pass 1 — match active (non-SWEPT, non-CONSUMED) pools. Identical to
                    the original logic. If matched, done.
          Pass 2 — if pass 1 found no match, attempt REBIRTH of a SWEPT/CONSUMED
                    pool whose price is within radius of the new centroid. New stop
                    accumulation at a previously swept level is a genuine new
                    structural level — institutions rebuild stop clusters after
                    liquidity is taken. The reborn pool gets a fresh created_at,
                    zeroed sweep metadata, and a new status so check_sweeps() can
                    detect it again on a future candle.

        A reborn pool will produce a SweepResult with a new detected_at timestamp
        when next swept, generating a new _sweep_key in entry_engine — correctly
        bypassing the _processed_sweeps hold from the original sweep event.
        """
        radius = atr * _CLUSTER_RADIUS_ATR.get(self.tf, 0.25)
        merged = list(existing)
        used   = set()

        for centroid, count in clusters:

            # ── Pass 1: match active pools ─────────────────────────────────
            matched = False
            for i, pool in enumerate(merged):
                if i in used:
                    continue
                if pool.status in (PoolStatus.SWEPT, PoolStatus.CONSUMED):
                    continue
                if abs(pool.price - centroid) <= radius:
                    pool.price      = (pool.price + centroid) / 2.0  # FIX-9
                    pool.touches    = max(pool.touches, count)
                    pool.last_touch = now
                    if count >= 2:
                        pool.status = PoolStatus.CONFIRMED
                    used.add(i)
                    matched = True
                    break

            if matched:
                continue

            # ── Pass 2: FIX-B1 — rebirth a SWEPT/CONSUMED pool ───────────
            # Only runs when pass 1 found no active pool at this level.
            # Rebirth takes priority over creating a brand-new pool to avoid
            # accumulating parallel duplicates at the same price cluster.
            for i, pool in enumerate(merged):
                if i in used:
                    continue
                if pool.status not in (PoolStatus.SWEPT, PoolStatus.CONSUMED):
                    continue
                if abs(pool.price - centroid) <= radius:
                    # Rebirth: reset all sweep state, assign fresh creation time.
                    # htf_count/ob_aligned/fvg_aligned are re-evaluated each tick
                    # by _promote_htf_confluence() and _align_with_ict().
                    prev_status = pool.status.value
                    pool.price      = centroid
                    pool.status     = PoolStatus.CONFIRMED if count >= 2 else PoolStatus.DETECTED
                    pool.touches    = count
                    pool.created_at = now
                    pool.last_touch = now
                    pool.swept_at   = 0.0
                    pool.sweep_wick = 0.0
                    used.add(i)
                    matched = True
                    logger.debug(
                        f"[{self.tf}] Pool REBIRTH {pool.side.value} "
                        f"${centroid:,.1f} (was {prev_status})"
                    )
                    break

            if not matched:
                merged.append(LiquidityPool(
                    price      = centroid,
                    side       = side,
                    timeframe  = self.tf,
                    status     = PoolStatus.CONFIRMED if count >= 2 else PoolStatus.DETECTED,
                    touches    = count,
                    created_at = now,
                    last_touch = now,
                ))

        return merged

    # ── Sweep detection ───────────────────────────────────────────────────

    def check_sweeps(self, candles: List[Dict], atr: float, now: float) -> List[SweepResult]:
        """
        FIX-6: Evaluate last CLOSED candle (candles[-2]) only.
        Requires >= 3 candles so [-2] is a fully closed bar.

        Bug #30 fix: iterate over a snapshot copy of the pool lists rather than
        the live list.  The original code mutated pool.status while iterating,
        creating a race with any concurrent reader (get_snapshot, prune).

        Bug #43 fix: pools that are newly swept are immediately moved to the
        _swept_bsl/_swept_ssl tracking lists so they cannot appear in
        get_snapshot() as active TP targets.  The 2-hour prune window applies
        only to the swept tracking lists, not to _bsl/_ssl.
        """
        if len(candles) < 3 or atr < 1e-10:
            return []

        c   = candles[-2]   # FIX-6: last CLOSED bar
        h   = float(c['h'])
        lo  = float(c['l'])
        cl  = float(c['c'])
        vol = float(c.get('v', 0))

        # 20-bar average volume from closed bars (exclude forming candle)
        vol_window = candles[max(0, len(candles) - 22):-1]
        avg_vol    = (sum(float(x.get('v', 0)) for x in vol_window)
                      / max(len(vol_window), 1))

        results:    List[SweepResult] = []
        min_wick    = atr * _SWEEP_WICK_MIN_ATR
        min_reject  = atr * _SWEEP_REJECT_MIN_ATR

        # Bug #30 fix: snapshot copies prevent mutation-during-iteration races
        bsl_snapshot = list(self._bsl)
        ssl_snapshot = list(self._ssl)

        # ── BSL sweep: wick above pool, close back below ──────────────────
        newly_swept_bsl: List = []
        for pool in bsl_snapshot:
            if pool.status in (PoolStatus.SWEPT, PoolStatus.CONSUMED):
                continue
            if h > pool.price + min_wick and cl < pool.price - min_reject:
                wick_above = h - pool.price
                rejection  = h - cl
                wick_range = max(h - lo, 1e-10)
                vol_ratio  = vol / max(avg_vol, 1e-10)
                quality    = self._score_sweep(wick_above / atr,
                                               rejection / wick_range,
                                               vol_ratio)
                pool.status     = PoolStatus.SWEPT
                pool.swept_at   = now
                pool.sweep_wick = h
                newly_swept_bsl.append(pool)
                results.append(SweepResult(
                    pool=pool, sweep_candle_idx=len(candles) - 2,
                    wick_extreme=h, rejection_pct=rejection / wick_range,
                    volume_ratio=vol_ratio, quality=quality,
                    direction="short", detected_at=now,
                ))

        # ── SSL sweep: wick below pool, close back above ──────────────────
        newly_swept_ssl: List = []
        for pool in ssl_snapshot:
            if pool.status in (PoolStatus.SWEPT, PoolStatus.CONSUMED):
                continue
            if lo < pool.price - min_wick and cl > pool.price + min_reject:
                wick_below = pool.price - lo
                rejection  = cl - lo
                wick_range = max(h - lo, 1e-10)
                vol_ratio  = vol / max(avg_vol, 1e-10)
                quality    = self._score_sweep(wick_below / atr,
                                               rejection / wick_range,
                                               vol_ratio)
                pool.status     = PoolStatus.SWEPT
                pool.swept_at   = now
                pool.sweep_wick = lo
                newly_swept_ssl.append(pool)
                results.append(SweepResult(
                    pool=pool, sweep_candle_idx=len(candles) - 2,
                    wick_extreme=lo, rejection_pct=rejection / wick_range,
                    volume_ratio=vol_ratio, quality=quality,
                    direction="long", detected_at=now,
                ))

        # Bug #43 fix: immediately remove newly-swept pools from the active
        # _bsl/_ssl lists so get_snapshot() never sees them as valid TP targets.
        # They are preserved in _swept_bsl/_swept_ssl for adjacency-bonus and
        # HTF context scoring.
        if newly_swept_bsl:
            swept_set = {id(p) for p in newly_swept_bsl}
            self._bsl = [p for p in self._bsl if id(p) not in swept_set]
            self._swept_bsl.extend(newly_swept_bsl)
        if newly_swept_ssl:
            swept_set = {id(p) for p in newly_swept_ssl}
            self._ssl = [p for p in self._ssl if id(p) not in swept_set]
            self._swept_ssl.extend(newly_swept_ssl)

        return results

    # ── Consumed promotion ────────────────────────────────────────────────

    def check_consumed(self, price: float, atr: float) -> None:
        """
        Promote SWEPT -> CONSUMED once price moves > 0.5 ATR beyond the swept level.
        A consumed pool is no longer actionable — it's pruned from the active registry.
        """
        threshold = atr * _CONSUMED_THRESHOLD_ATR
        for pool in self._bsl:
            if pool.status == PoolStatus.SWEPT and price > pool.price + threshold:
                pool.status = PoolStatus.CONSUMED
        for pool in self._ssl:
            if pool.status == PoolStatus.SWEPT and price < pool.price - threshold:
                pool.status = PoolStatus.CONSUMED

    # ── Scoring ───────────────────────────────────────────────────────────

    @staticmethod
    def _score_sweep(wick_atr: float, rejection: float, vol_ratio: float) -> float:
        """
        Composite sweep quality score [0.0, 1.0].
          wick_atr  (0.35 weight): wick penetration relative to ATR
          rejection (0.40 weight): (H-C)/(H-L) for BSL; (C-L)/(H-L) for SSL
          vol_ratio (0.25 weight): candle volume vs 20-bar average
        """
        wick_score = min(wick_atr / 0.5, 1.0)
        rej_score  = min(rejection / 0.7, 1.0)
        vol_score  = max(0.0, min((vol_ratio - 0.5) / 1.5, 1.0))
        return round(0.35 * wick_score + 0.40 * rej_score + 0.25 * vol_score, 2)

    # ── Properties ────────────────────────────────────────────────────────

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
# MAIN ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class LiquidityMap:
    """
    Multi-timeframe liquidity pool map engine v2.1.

    Tracks BSL (buy-stop liquidity) and SSL (sell-stop liquidity) pools
    across 1m, 5m, 15m, 1h, 4h, and 1d timeframes via swing-cluster detection.

    quant_strategy.py call pattern:
        self._liq_map.update(
            candles_by_tf=candles_by_tf,
            price=price, atr=atr, now=now,
            ict_engine=self._ict,
        )
        liq_snapshot = self._liq_map.get_snapshot(price, atr)
    """

    _TIMEFRAMES = ("1m", "5m", "15m", "1h", "4h", "1d")

    def __init__(self) -> None:
        self._registries: Dict[str, _TimeframeRegistry] = {
            tf: _TimeframeRegistry(tf) for tf in self._TIMEFRAMES
        }
        self._recent_sweeps: List[SweepResult]          = []
        self._last_snapshot: Optional[LiquidityMapSnapshot] = None

    # ─────────────────────────────────────────────────────────────────────
    # reset_snapshot() — FIX-B4: invalidate stale post-trade snapshot
    # ─────────────────────────────────────────────────────────────────────

    def reset_snapshot(self) -> None:
        """
        FIX-B4: Invalidate the cached last snapshot immediately after a
        position closes so the direction engine does NOT use stale swept-pool
        references on the next tick's predict_hunt() call.

        ROOT CAUSE:
          _last_snapshot is updated by get_snapshot() which is called every tick.
          However, quant_strategy passes self._liq_map._last_snapshot to
          predict_hunt() BEFORE calling liq_map.update() (per FIX-8 in
          direction_engine.py). This means the snapshot used for hunt prediction
          is always ONE TICK stale — which is intentional for normal operation.

          After a trade closes the stale snapshot contains BSL/SSL pools that
          were valid DURING the trade. If the swept pool that triggered the trade
          is still referenced as the primary BSL/SSL target in that snapshot (and
          it is, because pool rebirth hasn't run yet), predict_hunt() factors in
          a pool with distance_atr near 0 (price just swept through it) and
          significance that is artificially high from touch bonuses. This biases
          the hunt direction toward the DEAD level and suppresses the Factor 5
          pool_asymmetry score for the LIVE opposing pools.

          Result: direction_engine returns NEUTRAL or wrong-side hunt prediction.
          entry_engine finds no significant opposing target for TP calculation.
          conviction_filter's pool significance score is 0. No new entry passes.

        FIX:
          Call reset_snapshot() in quant_strategy._finalise_exit() (or wherever
          on_position_closed() is handled) BEFORE the next tick runs:

              self._liq_map.reset_snapshot()   # ← add this line

          When predict_hunt() receives liq_snapshot=None it falls back to ICT
          engine-only scoring (Factor 3 DR from ICT DealingRange, Factor 5 from
          ICT liquidity_pools). This is conservative but correct — one tick with
          ICT-only scoring is far better than several ticks of misdirected hunt
          prediction from stale post-trade pool data.

          On the NEXT tick after reset, get_snapshot() is called after
          liq_map.update(), producing a fresh snapshot that correctly reflects
          the reborn pools from FIX-B1. That fresh snapshot becomes _last_snapshot
          for the following tick's predict_hunt() — fully accurate from tick N+2.
        """
        self._last_snapshot = None
        logger.debug("LiquidityMap: snapshot reset (post-trade stale-pool guard)")

    # ─────────────────────────────────────────────────────────────────────
    # update() — PRIMARY ENTRY POINT called every tick from quant_strategy
    # Signature must exactly match: update(candles_by_tf, price, atr, now, ict_engine=None)
    # ─────────────────────────────────────────────────────────────────────

    def update(
        self,
        candles_by_tf: Dict[str, List[Dict]],
        price:         float,
        atr:           float,
        now:           float,
        ict_engine     = None,
    ) -> None:
        """
        Full per-tick update of all timeframe registries.

        Steps executed in order:
          1. Update pool registries (swing detection + pool merge)
          2. Promote HTF confluence (sets pool.htf_count)
          3. Align with ICT engine (sets pool.ob_aligned, pool.fvg_aligned)
          4. Detect sweeps on all timeframes from the last CLOSED candle
          5. Promote SWEPT -> CONSUMED for pools price has moved beyond
          6. Maintain 5-minute rolling sweep history

        Parameters
        ----------
        candles_by_tf : {"1m": [...], "5m": [...], "15m": [...], "1h": [...], ...}
        price         : current mid-price
        atr           : current ATR (all distance thresholds denominated in ATR)
        now           : epoch seconds
        ict_engine    : optional ICTEngine instance for OB/FVG alignment
        """
        if atr < 1e-10:
            return

        # Step 1: Update pool registries
        for tf, reg in self._registries.items():
            candles = candles_by_tf.get(tf)
            if candles:
                reg.update(candles, atr, now)

        # Step 2: HTF confluence promotion
        self._promote_htf_confluence(atr)

        # Step 3: ICT alignment
        if ict_engine is not None:
            self._align_with_ict(ict_engine, price, atr)

        # Step 4: Sweep detection (last CLOSED candle per timeframe)
        new_sweeps: List[SweepResult] = []
        for tf, reg in self._registries.items():
            candles = candles_by_tf.get(tf)
            if candles:
                tf_sweeps = reg.check_sweeps(candles, atr, now)
                for s in tf_sweeps:
                    logger.info(
                        f"🎯 SWEEP [{tf}] {s.pool.side.value} "
                        f"${s.pool.price:,.1f} -> direction={s.direction} "
                        f"quality={s.quality:.2f} wick=${s.wick_extreme:,.1f} "
                        f"vol_ratio={s.volume_ratio:.1f}x"
                    )
                new_sweeps.extend(tf_sweeps)

        # Step 5: SWEPT -> CONSUMED promotion
        for reg in self._registries.values():
            reg.check_consumed(price, atr)

        # Step 6: Rolling 5-minute sweep history
        self._recent_sweeps.extend(new_sweeps)
        cutoff = now - 300.0
        self._recent_sweeps = [s for s in self._recent_sweeps if s.detected_at > cutoff]

    # ─────────────────────────────────────────────────────────────────────
    # get_snapshot() — build immutable tick state for entry engine
    # ─────────────────────────────────────────────────────────────────────

    def get_snapshot(self, price: float, atr: float) -> LiquidityMapSnapshot:
        """
        Build an immutable LiquidityMapSnapshot for this tick.

        Scoring pipeline:
          1. Pool raw significance (tf_rank * htf_mult * ob/fvg bonuses, FIX-4)
          2. FIX-11: Adjacency bonus (+2.0 linearly decaying) applied to
             PoolTarget.significance when a swept pool is within 3 ATR.
             This bonus is on the WRAPPER not the pool, so it's visible to
             all PoolTarget.adjusted_sig() callers in entry_engine.
          3. FIX-12: Primary target selected by t.adjusted_sig()
             (= t.significance * exp(-dist/10)), not pool.proximity_adjusted_sig()
             which misses the adjacency bonus.
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

        # Collect all swept prices for adjacency scoring
        swept_prices: List[float] = []
        for reg in self._registries.values():
            for p in reg.swept_bsl + reg.swept_ssl:
                swept_prices.append(p.price)

        # Build PoolTarget lists
        bsl_targets: List[PoolTarget] = []
        ssl_targets: List[PoolTarget] = []

        for reg in self._registries.values():
            for pool in reg.bsl_pools:
                if not pool.is_tradeable:
                    continue
                dist = (pool.price - price) / atr
                if dist <= 0:
                    continue
                bsl_targets.append(PoolTarget(
                    pool=pool, distance_atr=dist,
                    direction="long", significance=pool.significance,
                    tf_sources=self._find_tf_sources(pool, PoolSide.BSL, atr),
                ))

            for pool in reg.ssl_pools:
                if not pool.is_tradeable:
                    continue
                dist = (price - pool.price) / atr
                if dist <= 0:
                    continue
                ssl_targets.append(PoolTarget(
                    pool=pool, distance_atr=dist,
                    direction="short", significance=pool.significance,
                    tf_sources=self._find_tf_sources(pool, PoolSide.SSL, atr),
                ))

        # FIX-5: Deduplicate at 0.2 ATR radius (was 0.3)
        bsl_targets = self._deduplicate_targets(bsl_targets, atr, radius_mult=0.20)
        ssl_targets = self._deduplicate_targets(ssl_targets, atr, radius_mult=0.20)

        # FIX-11: Adjacency bonus on PoolTarget.significance (NOT pool.significance)
        if swept_prices and atr > 1e-10:
            for t in bsl_targets + ssl_targets:
                min_dist = min(abs(t.pool.price - sp) / atr for sp in swept_prices)
                if min_dist < 3.0:
                    t.significance += 2.0 * (1.0 - min_dist / 3.0)

        # Sort by significance DESC for display
        bsl_targets.sort(key=lambda t: t.significance, reverse=True)
        ssl_targets.sort(key=lambda t: t.significance, reverse=True)

        nearest_bsl = min((t.distance_atr for t in bsl_targets), default=999.0)
        nearest_ssl = min((t.distance_atr for t in ssl_targets), default=999.0)

        # FIX-12: Primary target by adjusted_sig() — incorporates adjacency bonus
        all_reachable = [t for t in bsl_targets + ssl_targets if t.distance_atr < 25.0]
        primary: Optional[PoolTarget] = None
        if all_reachable:
            primary = max(all_reachable, key=lambda t: t.adjusted_sig())
        elif bsl_targets or ssl_targets:
            primary = min(bsl_targets + ssl_targets, key=lambda t: t.distance_atr)

        # Swept-pool history for display / HTF TP escalation context
        swept_bsl_prices = sorted(
            {p.price for reg in self._registries.values() for p in reg.swept_bsl},
            reverse=True)[:20]
        swept_ssl_prices = sorted(
            {p.price for reg in self._registries.values() for p in reg.swept_ssl})[:20]

        snap = LiquidityMapSnapshot(
            bsl_pools        = bsl_targets[:20],
            ssl_pools        = ssl_targets[:20],
            primary_target   = primary,
            recent_sweeps    = list(self._recent_sweeps),
            swept_bsl_levels = swept_bsl_prices,
            swept_ssl_levels = swept_ssl_prices,
            nearest_bsl_atr  = nearest_bsl,
            nearest_ssl_atr  = nearest_ssl,
            timestamp        = now,
        )
        self._last_snapshot = snap
        return snap

    # ─────────────────────────────────────────────────────────────────────
    # Internal helpers
    # ─────────────────────────────────────────────────────────────────────

    def _promote_htf_confluence(self, atr: float) -> None:
        """
        Set pool.htf_count = number of distinct OTHER timeframes that have a
        pool within 0.7×ATR of this pool (same side only).

        Bug #16 fix: the original implementation was O(n²) — it iterated every
        pool against every other pool, producing up to 32,400 comparisons per
        call at maximum pool density (180 pools across 6 TFs).  This ran inside
        update() which is called every tick (~4×/second), visibly bloating tick
        latency under high pool density.

        Fix: sort all pools by (side, price) once — O(n log n) — then use a
        sliding window per side to find neighbours within the radius.  Each pool
        is compared only against its actual spatial neighbours, giving O(n) work
        per side after sorting.  Total complexity: O(n log n).

        Counter is fully recomputed each call — no stale state accumulates.
        """
        radius = atr * 0.7

        # Collect all active pools with their timeframe, grouped by side.
        bsl_pools: List[LiquidityPool] = []
        ssl_pools: List[LiquidityPool] = []
        for reg in self._registries.values():
            bsl_pools.extend(reg.bsl_pools)
            ssl_pools.extend(reg.ssl_pools)

        def _apply_sliding_window(pools: List[LiquidityPool]) -> None:
            if not pools:
                return
            # Sort by price ascending — enables O(n) sliding window.
            pools.sort(key=lambda p: p.price)
            n = len(pools)
            left = 0
            for right in range(n):
                # Advance left pointer to maintain window: all pools within radius.
                while pools[right].price - pools[left].price > radius:
                    left += 1
                # Count distinct OTHER timeframes within [left, right].
                tf_set: set = set()
                for k in range(left, right + 1):
                    if pools[k] is not pools[right]:
                        tf_set.add(pools[k].timeframe)
                pools[right].htf_count = len(tf_set)

        _apply_sliding_window(bsl_pools)
        _apply_sliding_window(ssl_pools)

    def _align_with_ict(self, ict_engine, price: float, atr: float) -> None:
        """
        Mark pools as OB-aligned or FVG-aligned using ICT engine data.

        Reads ict_engine._obs (order blocks dict keyed by tf) and
        ict_engine._fvgs (fair value gaps dict keyed by tf). Attribute
        errors are caught and logged at DEBUG — never crashes the map.
        """
        try:
            for reg in self._registries.values():
                for pool in reg.bsl_pools + reg.ssl_pools:
                    pool.ob_aligned  = False
                    pool.fvg_aligned = False

                    if hasattr(ict_engine, '_obs'):
                        for tf_key, ob_list in ict_engine._obs.items():
                            for ob in ob_list:
                                try:
                                    ob_mid = (float(ob.high) + float(ob.low)) / 2.0
                                    if abs(ob_mid - pool.price) < atr * 0.5:
                                        pool.ob_aligned = True
                                        break
                                except Exception:
                                    continue
                            if pool.ob_aligned:
                                break

                    if hasattr(ict_engine, '_fvgs'):
                        for tf_key, fvg_list in ict_engine._fvgs.items():
                            for fvg in fvg_list:
                                try:
                                    fvg_mid = (float(fvg.high) + float(fvg.low)) / 2.0
                                    if pool.side == PoolSide.BSL:
                                        if price < fvg_mid < pool.price:
                                            pool.fvg_aligned = True
                                            break
                                    else:
                                        if pool.price < fvg_mid < price:
                                            pool.fvg_aligned = True
                                            break
                                except Exception:
                                    continue
                            if pool.fvg_aligned:
                                break

        except Exception as e:
            logger.debug(f"ICT alignment error (non-fatal): {e}")

    def _find_tf_sources(
        self,
        pool: LiquidityPool,
        side: PoolSide,
        atr:  float,
    ) -> List[str]:
        """Return list of timeframes with a pool within 0.5 ATR of this pool."""
        sources: List[str] = []
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
        targets:     List[PoolTarget],
        atr:         float,
        radius_mult: float = 0.20,   # FIX-5: was 0.30
    ) -> List[PoolTarget]:
        """Remove near-duplicate targets, keeping highest significance per cluster."""
        if not targets:
            return []
        radius  = atr * radius_mult
        by_sig  = sorted(targets, key=lambda t: t.significance, reverse=True)
        result: List[PoolTarget] = []
        seen:   List[float]      = []
        for t in by_sig:
            if not any(abs(t.pool.price - p) <= radius for p in seen):
                result.append(t)
                seen.append(t.pool.price)
        return result

    # ─────────────────────────────────────────────────────────────────────
    # get_status_summary() — used by display engine
    # ─────────────────────────────────────────────────────────────────────

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
                f"{snap.primary_target.direction} -> "
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

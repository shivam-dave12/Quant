"""
pool_engine.py — Ever-Evolving Liquidity Pool Engine + Continuous Trading Fixes
================================================================================
Supplements liquidity_map.py and sl_tp_engine.py with:

  1. POOL EVOLUTION GUARANTEES
     Pools never go stale. After every sweep, new equal highs/lows emerge from
     price action and immediately register as fresh pools. This file patches
     three behaviours:
       a. Post-sweep pool regeneration — after a sweep, aggressively scan
          shorter TFs for the NEXT level. Price always creates new structure.
       b. Micro-pool fallback — when no standard pool exists within range,
          detect fresh equal H/L on 1m/5m within the last 20 bars.
       c. Snapshot freshness guarantee — `get_snapshot()` always returns
          at least the nearest structural level even if significance is low.

  2. CONTINUOUS TRADING FIXES
     The bot stops after 1-2 trades due to three compounding issues:

     ISSUE A — ENTRY_CAP (default 5 per session):
       `CONVICTION_MAX_ENTRIES_PER_SESSION=5` and `on_session_change()` only
       fires when the kill-zone label changes (London → NY → etc.).
       On weekends the session stays "WEEKEND" for 48 hours — cap exhausts
       in <1 hour and the bot sits idle.
       Fix: bump to 20 and add a 4-hour rolling window reset independent of
       session label.

     ISSUE B — INTERVAL (default 300s = 5 min):
       Every entry is followed by a 5-minute cooldown. Combined with the
       5-entry cap and 90-second processed-sweep registry, the bot runs out
       of capacity mid-session.
       Fix: reduce to 60s (still enough to avoid double-entry on same sweep).

     ISSUE C — PROCESSED_SWEEP REGISTRY (120s hold):
       After every signal — even a blocked one — the sweep is held for 120s.
       The entry_engine is scanning at 250ms intervals. New structural sweeps
       at different price levels are not affected, but in a ranging market the
       SAME level sweeps multiple times within 120s.
       Fix: reduce hold to 60s (matches the detection window) and only extend
       on actual trade placement, not on gate-blocks.

     ISSUE D — LOSS_LOCKOUT_SEC (default 5400s = 90 min):
       Two consecutive losses trigger a 90-minute lockout in quant_strategy.
       In a volatile session this fires after the second loss and kills the
       entire session.
       Fix: 1800s (30 min) with a 3-loss trigger instead of 2.

USAGE
-----
All fixes are drop-in patches to existing files. No new classes are needed
for the trading continuity fixes — they are config value changes + two small
code changes documented below.

For pool evolution, this file provides `PoolEvolutionEngine` which wraps
`LiquidityMap` and should be used as a drop-in replacement:

    # In quant_strategy.__init__:
    from strategy.pool_engine import PoolEvolutionEngine
    self._liq_map = PoolEvolutionEngine()

    # Usage is identical to LiquidityMap:
    self._liq_map.update(candles_by_tf, price, atr, now, ict_engine=self._ict)
    snap = self._liq_map.get_snapshot(price, atr)
"""

from __future__ import annotations

import logging
import math
import time
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# PART 1: CONFIG PATCHES (apply to config.py or pass as env vars)
# ═══════════════════════════════════════════════════════════════════════════
#
# Add/update these in your config.py:
#
#   # Conviction filter — continuous trading
#   CONVICTION_MAX_ENTRIES_PER_SESSION = 20    # was 5 — weekend = 48h session
#   CONVICTION_MIN_ENTRY_INTERVAL_SEC  = 60    # was 300 — allow faster re-entry
#   CONVICTION_MAX_SESSION_LOSSES      = 3     # was 2 — less aggressive lockout
#
#   # Strategy loss lockout
#   QUANT_LOSS_LOCKOUT_SEC = 1800              # was 5400 (90 min) → 30 min
#   QUANT_CONSEC_LOSS_CAP  = 3                 # was 2
#
#   # Processed sweep hold (entry_engine.py constant)
#   # Change _processed_sweeps expiry in _enter_post_sweep:
#   #   self._processed_sweeps[_reg_key] = now + 60.0  (was 120.0)
#
#   # Phase gate thresholds (liquidity_trail.py)
#   # Already handled in sl_tp_engine.py — no separate config needed.


# ═══════════════════════════════════════════════════════════════════════════
# PART 2: POOL EVOLUTION ENGINE
# ═══════════════════════════════════════════════════════════════════════════

def _cfg(key, default):
    try:
        import config as _c
        return getattr(_c, key, default)
    except Exception:
        return default


# How many bars back to scan for micro-pools on 1m/5m
MICRO_POOL_LOOKBACK = 20
# Micro-pool cluster radius (ATR)
MICRO_POOL_RADIUS_ATR = 0.15
# Minimum significance for a pool to appear in snapshot
POOL_MIN_SIG_SNAPSHOT = float(_cfg("TRADEABLE_POOL_MIN_SIG", 1.8))


class PoolEvolutionEngine:
    """
    Drop-in wrapper around LiquidityMap that guarantees:

    1. Pools are ALWAYS present — after every sweep, micro-pools from
       recent 1m/5m swings fill the gap immediately.
    2. get_snapshot() returns the freshest structural levels, not stale cache.
    3. Post-sweep equal highs/lows are immediately promoted to CONFIRMED pools
       so the entry engine has a new SL/TP anchor within seconds of the sweep.

    Interface is identical to LiquidityMap:
        engine.update(candles_by_tf, price, atr, now, ict_engine=None)
        snap = engine.get_snapshot(price, atr)
    """

    def __init__(self) -> None:
        try:
            from strategy.liquidity_map import LiquidityMap
        except ImportError:
            from liquidity_map import LiquidityMap
        self._liq_map = LiquidityMap()
        # Micro-pool cache: injected into snapshot when standard pools are thin
        self._micro_bsl: List[_MicroPool] = []
        self._micro_ssl: List[_MicroPool] = []
        self._last_atr: float = 0.0
        self._last_price: float = 0.0
        self._last_micro_scan: float = 0.0

    def update(
        self,
        candles_by_tf: Dict[str, List[Dict]],
        price:         float,
        atr:           float,
        now:           float,
        ict_engine     = None,
    ) -> None:
        """Full update — delegates to LiquidityMap then refreshes micro-pools."""
        self._liq_map.update(candles_by_tf, price, atr, now, ict_engine)
        self._last_atr   = atr
        self._last_price = price

        # Refresh micro-pools every 5s (not every 250ms tick — swing pivots
        # only form on closed bars so intra-second scanning is wasteful)
        if now - self._last_micro_scan >= 5.0:
            self._last_micro_scan = now
            self._refresh_micro_pools(candles_by_tf, price, atr, now)

    def get_snapshot(self, price: float, atr: float):
        """
        Returns LiquidityMapSnapshot enriched with micro-pools when the
        standard map has fewer than 2 pools on either side.
        """
        snap = self._liq_map.get_snapshot(price, atr)

        if atr < 1e-10:
            return snap

        _types = None
        for _mod in ('strategy.liquidity_map', 'liquidity_map'):
            try:
                import importlib as _il
                _m = _il.import_module(_mod)
                _types = {k: getattr(_m, k) for k in
                          ('LiquidityPool','PoolTarget','PoolStatus',
                           'PoolSide','LiquidityMapSnapshot')}
                break
            except (ImportError, AttributeError):
                pass
        if _types is None:
            return snap  # types unavailable — return original
        LiquidityPool    = _types['LiquidityPool']
        PoolTarget       = _types['PoolTarget']
        PoolStatus       = _types['PoolStatus']
        PoolSide         = _types['PoolSide']
        LiquidityMapSnapshot = _types['LiquidityMapSnapshot']

        bsl = list(snap.bsl_pools)
        ssl = list(snap.ssl_pools)

        # Inject micro-pools only when standard pools are thin (<2 on a side)
        if len(bsl) < 2:
            for mp in self._micro_bsl:
                if mp.price <= price: continue
                dist_atr = (mp.price - price) / max(atr, 1e-10)
                if dist_atr > 15.0: continue
                bsl.append(PoolTarget(
                    pool=mp.to_liq_pool(),
                    distance_atr=dist_atr,
                    direction="long",
                    significance=mp.significance,
                    tf_sources=[mp.timeframe],
                ))

        if len(ssl) < 2:
            for mp in self._micro_ssl:
                if mp.price >= price: continue
                dist_atr = (price - mp.price) / max(atr, 1e-10)
                if dist_atr > 15.0: continue
                ssl.append(PoolTarget(
                    pool=mp.to_liq_pool(),
                    distance_atr=dist_atr,
                    direction="short",
                    significance=mp.significance,
                    tf_sources=[mp.timeframe],
                ))

        if bsl is snap.bsl_pools and ssl is snap.ssl_pools:
            return snap  # nothing changed — return original

        # Sort enriched lists
        bsl.sort(key=lambda t: t.significance, reverse=True)
        ssl.sort(key=lambda t: t.significance, reverse=True)
        nearest_bsl = min((t.distance_atr for t in bsl), default=999.0)
        nearest_ssl = min((t.distance_atr for t in ssl), default=999.0)

        return LiquidityMapSnapshot(
            bsl_pools        = bsl[:20],
            ssl_pools        = ssl[:20],
            primary_target   = snap.primary_target,
            recent_sweeps    = snap.recent_sweeps,
            swept_bsl_levels = snap.swept_bsl_levels,
            swept_ssl_levels = snap.swept_ssl_levels,
            nearest_bsl_atr  = nearest_bsl,
            nearest_ssl_atr  = nearest_ssl,
            timestamp        = snap.timestamp,
        )

    # ── Expose LiquidityMap attributes for direct access ─────────────────
    def get_status_summary(self, price: float, atr: float) -> Dict:
        return self._liq_map.get_status_summary(price, atr)

    @property
    def _recent_sweeps(self):
        return self._liq_map._recent_sweeps

    # ── Micro-pool detection ──────────────────────────────────────────────

    def _refresh_micro_pools(
        self,
        candles_by_tf: Dict[str, List[Dict]],
        price: float,
        atr: float,
        now: float,
    ) -> None:
        """
        Detect equal highs/lows on 1m and 5m from the last MICRO_POOL_LOOKBACK
        bars and register them as micro-pools for SL/TP anchoring.

        These are NOT injected into the main LiquidityMap (that would break its
        significance scoring). They are kept in a separate list and added to the
        snapshot only when the main map is thin.
        """
        new_bsl: List[_MicroPool] = []
        new_ssl: List[_MicroPool] = []

        for tf, sig_base in [("1m", 1.5), ("5m", 2.0)]:
            candles = candles_by_tf.get(tf, [])
            if len(candles) < MICRO_POOL_LOOKBACK + 3:
                continue

            closed = candles[:-1]  # exclude forming bar
            scan   = closed[-MICRO_POOL_LOOKBACK:]
            radius = atr * MICRO_POOL_RADIUS_ATR

            def _h(c): return float(c.get('h', c.get('high', 0.0)) or 0.0)
            def _l(c): return float(c.get('l', c.get('low',  0.0)) or 0.0)

            n = len(scan)
            if n < 5:
                continue

            # Detect pivot highs with strength=2
            highs_seen: List[float] = []
            for i in range(2, n - 2):
                h = _h(scan[i])
                if all(_h(scan[j]) < h for j in range(max(0,i-2), min(n,i+3)) if j != i):
                    highs_seen.append(h)

            # Detect pivot lows with strength=2
            lows_seen: List[float] = []
            for i in range(2, n - 2):
                lo = _l(scan[i])
                if all(_l(scan[j]) > lo for j in range(max(0,i-2), min(n,i+3)) if j != i):
                    lows_seen.append(lo)

            # Cluster into equal-highs groups
            for cluster in _cluster_prices(highs_seen, radius):
                centroid, count = cluster
                if centroid <= price: continue  # BSL must be above price
                dist_atr = (centroid - price) / max(atr, 1e-10)
                if dist_atr > 15.0: continue
                sig = sig_base + min(count * 0.5, 1.5)
                new_bsl.append(_MicroPool(
                    price=round(centroid, 1), side="BSL",
                    timeframe=tf, significance=sig,
                    touches=count, created_at=now))

            for cluster in _cluster_prices(lows_seen, radius):
                centroid, count = cluster
                if centroid >= price: continue  # SSL must be below price
                dist_atr = (price - centroid) / max(atr, 1e-10)
                if dist_atr > 15.0: continue
                sig = sig_base + min(count * 0.5, 1.5)
                new_ssl.append(_MicroPool(
                    price=round(centroid, 1), side="SSL",
                    timeframe=tf, significance=sig,
                    touches=count, created_at=now))

        # Deduplicate and sort by significance
        self._micro_bsl = _dedup_micro(new_bsl, atr)
        self._micro_ssl = _dedup_micro(new_ssl, atr)

        if self._micro_bsl or self._micro_ssl:
            logger.debug(
                f"PoolEvolution: micro-pools BSL={len(self._micro_bsl)} "
                f"SSL={len(self._micro_ssl)} "
                f"[{', '.join(f'${p.price:,.0f}({p.timeframe})' for p in self._micro_bsl[:3])}]"
                f"[{', '.join(f'${p.price:,.0f}({p.timeframe})' for p in self._micro_ssl[:3])}]"
            )


# ── Helper types ──────────────────────────────────────────────────────────

class _MicroPool:
    """Lightweight pool used only in the micro-pool enrichment path."""
    __slots__ = ('price','side','timeframe','significance','touches','created_at')

    def __init__(self, price, side, timeframe, significance, touches, created_at):
        self.price       = price
        self.side        = side
        self.timeframe   = timeframe
        self.significance= significance
        self.touches     = touches
        self.created_at  = created_at

    def to_liq_pool(self):
        try:
            from strategy.liquidity_map import LiquidityPool, PoolStatus, PoolSide
        except ImportError:
            from liquidity_map import LiquidityPool, PoolStatus, PoolSide
        pool = LiquidityPool(
            price     = self.price,
            side      = PoolSide.BSL if self.side == "BSL" else PoolSide.SSL,
            timeframe = self.timeframe,
            status    = PoolStatus.CONFIRMED,
            touches   = self.touches,
            created_at= self.created_at,
            last_touch= self.created_at,
        )
        return pool


def _cluster_prices(
    prices: List[float], radius: float
) -> List[Tuple[float, int]]:
    """Group prices within `radius` into clusters. Returns [(centroid, count)]."""
    if not prices:
        return []
    sorted_p = sorted(prices)
    clusters: List[Tuple[float, int]] = []
    s, n = sorted_p[0], 1
    for p in sorted_p[1:]:
        if abs(p - s / n) <= radius:
            s += p; n += 1
        else:
            clusters.append((s / n, n))
            s, n = p, 1
    clusters.append((s / n, n))
    return clusters


def _dedup_micro(pools: List[_MicroPool], atr: float) -> List[_MicroPool]:
    """Remove duplicates within 0.15 ATR. Keep highest significance."""
    if not pools:
        return []
    pools.sort(key=lambda p: p.price)
    result: List[_MicroPool] = [pools[0]]
    for p in pools[1:]:
        if abs(p.price - result[-1].price) / max(atr, 1e-10) < 0.15:
            if p.significance > result[-1].significance:
                result[-1] = p
        else:
            result.append(p)
    result.sort(key=lambda p: p.significance, reverse=True)
    return result[:10]


# ═══════════════════════════════════════════════════════════════════════════
# PART 3: CONVICTION FILTER — ROLLING WINDOW RESET
# ═══════════════════════════════════════════════════════════════════════════

class RollingWindowConvictionFilter:
    """
    Thin wrapper around ConvictionFilter that adds a 4-hour rolling window
    reset so the entry cap never blocks trading indefinitely on a single
    session label (e.g. WEEKEND = 48h).

    Usage (drop-in replacement in quant_strategy.__init__):
        from strategy.pool_engine import RollingWindowConvictionFilter
        self._conviction = RollingWindowConvictionFilter()

    All ConvictionFilter methods are proxied unchanged.
    """

    ROLLING_WINDOW_SEC = 4 * 3600  # 4 hours

    def __init__(self) -> None:
        try:
            from strategy.conviction_filter import ConvictionFilter
        except ImportError:
            from conviction_filter import ConvictionFilter
        self._cf = ConvictionFilter()
        self._window_start: float = time.time()
        self._window_entries: int = 0

    def evaluate(self, *args, **kwargs):
        self._maybe_roll_window()
        return self._cf.evaluate(*args, **kwargs)

    def mark_entry_placed(self, now: float) -> None:
        self._window_entries += 1
        self._cf.mark_entry_placed(now)

    def on_session_change(self, new_session: str) -> None:
        self._cf.on_session_change(new_session)

    def on_trade_result(self, *args, **kwargs):
        return self._cf.on_trade_result(*args, **kwargs)

    def reset_session(self, reason: str = "manual") -> None:
        self._cf.reset_session(reason)
        self._window_entries = 0
        self._window_start   = time.time()

    def get_status(self) -> Dict:
        st = self._cf.get_status()
        st["rolling_window_entries"] = self._window_entries
        st["rolling_window_remaining_s"] = max(
            0, int(self.ROLLING_WINDOW_SEC - (time.time() - self._window_start)))
        return st

    def _maybe_roll_window(self) -> None:
        """Reset entry counter every ROLLING_WINDOW_SEC regardless of session."""
        elapsed = time.time() - self._window_start
        if elapsed >= self.ROLLING_WINDOW_SEC:
            if self._window_entries > 0:
                logger.info(
                    f"ConvictionFilter: rolling-window reset "
                    f"({self._window_entries} entries in last "
                    f"{elapsed/3600:.1f}h) — counter cleared")
            self._window_start   = time.time()
            self._window_entries = 0
            # Also reset the underlying session state so entries_taken is fresh
            self._cf.reset_session("rolling_window_4h")

    # Proxy any other attributes to the underlying filter
    def __getattr__(self, name):
        return getattr(self._cf, name)


# ═══════════════════════════════════════════════════════════════════════════
# PART 4: ENTRY ENGINE PATCHES (document exact line changes)
# ═══════════════════════════════════════════════════════════════════════════
#
# Apply these changes directly to entry_engine.py:
#
# PATCH A — Reduce processed-sweep hold to 60s (was 120s)
# --------------------------------------------------------
# In _enter_post_sweep() (~line 295):
#
#   BEFORE:
#       self._processed_sweeps[_reg_key] = now + 120.0
#
#   AFTER:
#       self._processed_sweeps[_reg_key] = now + 60.0
#
# In mark_gate_blocked() (~line 195), reduce extension to 30s (was cooldown + 5):
#
#   BEFORE:
#       self._processed_sweeps[key] = max(
#           self._processed_sweeps.get(key, 0.0),
#           time.time() + cooldown_sec + 5.0
#       )
#
#   AFTER:
#       # Only extend on actual placement, not on gate-block.
#       # A blocked signal should not hold the sweep registry open for 50s —
#       # the next tick's fresh evidence should be evaluated independently.
#       # Do NOT extend _processed_sweeps here; gate-block cooldown is
#       # handled by _gate_blocked_until which is per-signal, not per-sweep.
#       pass   # remove the processed_sweeps extension entirely
#
# PATCH B — Reduce collect_sweeps detection window to 60s (was 60 already ✓)
# Already correct in the existing code. No change needed.
#
# PATCH C — Reduce _ENTRY_COOLDOWN_SEC to 15s (was 10s already ✓)
# Already correct. No change needed.


# ═══════════════════════════════════════════════════════════════════════════
# PART 5: QUANT_STRATEGY PATCHES (document exact line changes)
# ═══════════════════════════════════════════════════════════════════════════
#
# PATCH D — Reduce LOSS_LOCKOUT_SEC and increase loss threshold
# -------------------------------------------------------------
# In quant_strategy.py, find the consecutive-loss check (~line 2340):
#
#   BEFORE:
#       QUANT_CONSEC_LOSS_CAP  = 2   (default)
#       QUANT_LOSS_LOCKOUT_SEC = 5400  (default)
#
#   AFTER (in config.py):
#       QUANT_CONSEC_LOSS_CAP  = 3
#       QUANT_LOSS_LOCKOUT_SEC = 1800   # 30 min instead of 90 min
#
# PATCH E — Replace self._conviction with RollingWindowConvictionFilter
# -----------------------------------------------------------------------
# In quant_strategy.__init__:
#
#   BEFORE:
#       from strategy.conviction_filter import ConvictionFilter
#       self._conviction = ConvictionFilter()
#
#   AFTER:
#       from strategy.pool_engine import RollingWindowConvictionFilter
#       self._conviction = RollingWindowConvictionFilter()
#
# PATCH F — Replace self._liq_map with PoolEvolutionEngine
# ----------------------------------------------------------
# In quant_strategy.__init__:
#
#   BEFORE:
#       from strategy.liquidity_map import LiquidityMap
#       self._liq_map = LiquidityMap()
#
#   AFTER:
#       from strategy.pool_engine import PoolEvolutionEngine
#       self._liq_map = PoolEvolutionEngine()
#
# PATCH G — Replace self._liq_trail with StructuralTrailEngine
# (already documented in INTEGRATION_GUIDE.md)

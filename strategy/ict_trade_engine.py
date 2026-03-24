"""
ict_trade_engine.py — Institutional ICT Trade Engine v1.0
==========================================================
Provides the "sweep spot" institutional SL/TP/Trail/Entry logic that wires
directly into quant_strategy.py.

PHILOSOPHY
──────────
The highest win-rate ICT setup is: SSL/BSL Sweep → Displacement → OTE
retracement → AMD Distribution delivery to opposing liquidity.

Win-rate improvement comes from three compounding edges:
  1. TIGHTER SL: SL at sweep candle wick low/high (0.5-0.8×ATR) vs prior
     15m swing (1.5-3.0×ATR). Same thesis, far better R:R.
  2. FURTHER TP: AMD delivery to OPPOSING liquidity pool vs nearest FVG.
     Price is being DELIVERED there — don't cut it short at a random FVG.
  3. SMARTER TRAIL: AMD-phase-aware trailing (no trailing during MANIPULATION
     phase where the Judas swing is still active, aggressive trailing once
     DISTRIBUTION confirms BOS on 5m).

MODULES
───────
ICTSweepSetup         — Detected sweep-and-go setup snapshot
ICTSweepDetector      — Finds and tracks active sweep setups from ict_engine
ICTSLEngine           — Institutional SL placement (sweep-first hierarchy)
ICTTPEngine           — Institutional TP placement (AMD delivery targeting)
ICTTrailEngine        — AMD-phase-aware dynamic trailing (no timeouts)
ICTEntryGate          — Tier-based entry quality classifier
"""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class ICTSweepSetup:
    """
    A fully-confirmed ICT sweep-and-go setup snapshot.

    Lifecycle:
      DETECTED  → displacement + sweep candle found, waiting for OTE retrace
      OTE_READY → price has entered the OTE zone (61.8%-78.6% retrace)
      ACTIVE    → entry confirmed in OTE zone
      EXPIRED   → AMD phase > DISTRIBUTION window or new conflicting sweep
    """
    # Core geometry
    side: str                   # "long" (SSL swept) | "short" (BSL swept)
    sweep_price: float          # price of the liquidity pool that was swept
    sweep_wick_extreme: float   # absolute low (long) / high (short) of sweep candle
    displacement_high: float    # high of displacement candle (long setup)
    displacement_low: float     # low of displacement candle (short setup)
    displacement_close: float   # close of displacement candle

    # OTE zone (61.8%–78.6% Fibonacci retracement of displacement move)
    ote_entry_zone_low: float   # lower bound of OTE (78.6% retrace for long)
    ote_entry_zone_high: float  # upper bound of OTE (61.8% retrace for long)

    # Institutional SL levels (in priority order)
    sl_sweep_candle: float      # below sweep wick (long) / above sweep wick (short)
    sl_ob_extreme: float        # below displacement OB low / above OB high
    sl_15m_swing: float         # fallback: 15m structural swing

    # AMD delivery target
    delivery_target: Optional[float]  # opposing unswept liquidity pool
    delivery_target_label: str = ""

    # Quality metadata
    amd_confidence: float = 0.0
    has_fvg_in_ote: bool = False
    has_ob_in_ote: bool = False
    kill_zone: str = ""          # "" | "london" | "ny" | "asia"
    mtf_aligned: bool = False    # ≥2 HTF TFs agree with direction
    displacement_strength: float = 0.0  # body/ATR ratio of displacement candle

    # State
    setup_time_ms: int = 0
    status: str = "DETECTED"    # DETECTED | OTE_READY | ACTIVE | EXPIRED

    @property
    def ote_midpoint(self) -> float:
        return (self.ote_entry_zone_low + self.ote_entry_zone_high) / 2.0

    def price_in_ote(self, price: float) -> bool:
        return self.ote_entry_zone_low <= price <= self.ote_entry_zone_high

    def quality_score(self) -> float:
        """0–1 quality score for tier routing."""
        s = 0.0
        s += self.amd_confidence * 0.30
        if self.has_fvg_in_ote: s += 0.20
        if self.has_ob_in_ote:  s += 0.15
        if self.kill_zone in ("london", "ny"): s += 0.15
        if self.mtf_aligned:    s += 0.15
        s += min(self.displacement_strength / 3.0, 0.05)
        return min(s, 1.0)


@dataclass
class TrailDecision:
    """Result of ICTTrailEngine.compute()."""
    new_sl: Optional[float]       # None = hold (don't trail)
    reason: str                   # human-readable reason
    phase: int                    # 0=hold, 1=early, 2=lock, 3=aggressive
    amd_state: str                # AMD phase at time of decision
    structure_anchor: str         # what structural level anchors the SL


# ═══════════════════════════════════════════════════════════════════════════
# ICT SWEEP DETECTOR
# ═══════════════════════════════════════════════════════════════════════════

class ICTSweepDetector:
    """
    Detects and tracks ICT sweep-and-go setups from a live ICTEngine.

    Sweep-and-Go Setup Requirements:
      1. A liquidity pool (BSL or SSL) was swept WITH displacement confirmation
         (displacement_confirmed=True on the LiquidityLevel)
      2. A displacement candle created an OB in the impulse direction
      3. Price is retracing or has retraced to the OTE zone (61.8%-78.6% fib)

    The displacement OB is identified as the last opposite-color candle before
    the displacement impulse — exactly how ICT defines it.

    Sweep age limit: AMD_DISTRIB_WINDOW_MS (90 min). After that, the setup
    is considered EXPIRED and should not be used for entry.
    """

    # OTE Fibonacci levels
    OTE_LOWER_FIB = 0.618   # 61.8% retrace
    OTE_UPPER_FIB = 0.786   # 78.6% retrace

    def __init__(self):
        self._active_setup: Optional[ICTSweepSetup] = None
        self._last_check_ms: int = 0
        self._CHECK_INTERVAL_MS = 5_000   # re-evaluate every 5 seconds

        # BUG-STALE-SWEEP-REDETECT FIX:
        # When a setup expires (OTE missed / blown through), the swept pool
        # remains in ict_engine.liquidity_pools for up to 4.5h.  Without
        # memory of which sweeps have been tried, _find_best_setup rebuilds
        # an identical setup on every 5s tick and update() immediately
        # expires it again — creating infinite detection→expiry spam.
        #
        # _expired_sweep_keys: set of (round(pool_price, 0), sweep_ts_ms)
        # tuples that have already been attempted and missed.  When AMD
        # phase resets to ACCUMULATION/neutral (new sweep context), the
        # set is cleared so genuinely new sweeps can be tried.
        self._expired_sweep_keys: set = set()
        self._last_amd_phase: str = ""
        self._MAX_EXPIRED_KEYS = 100   # FIX 13: bound the set to prevent memory leak
        # BUG-SWEEP-KEY-LEAK FIX: cap expired keys to prevent unbounded growth
        # in long sessions with many detected-then-expired sweeps between AMD resets.
        self._MAX_EXPIRED_KEYS = 100

    def update(self, ict_engine, price: float, atr: float, now_ms: int,
               candles_5m: List[Dict] = None,
               candles_15m: List[Dict] = None) -> Optional[ICTSweepSetup]:
        """
        Scan ICT engine state for active sweep setups.
        Returns the best active setup or None.
        """
        if now_ms - self._last_check_ms < self._CHECK_INTERVAL_MS:
            return self._active_setup
        self._last_check_ms = now_ms

        if ict_engine is None or not ict_engine._initialized:
            return None

        # Clear expired sweep key memory when AMD resets to a fresh context.
        # ACCUMULATION with neutral bias means the old sweep has decayed and
        # any new sweep should be treated as a fresh setup.
        current_amd_phase = getattr(ict_engine._amd, 'phase', '')
        current_amd_bias  = getattr(ict_engine._amd, 'bias',  'neutral')
        if (current_amd_phase == "ACCUMULATION" and current_amd_bias == "neutral"
                and self._last_amd_phase not in ("", "ACCUMULATION")):
            self._expired_sweep_keys.clear()
        self._last_amd_phase = current_amd_phase

        # Check if current setup is still valid
        if self._active_setup is not None:
            if self._active_setup.status == "EXPIRED":
                self._active_setup = None
            elif (now_ms - self._active_setup.setup_time_ms >
                  ict_engine.AMD_DISTRIB_WINDOW_MS):
                self._active_setup = None
                logger.debug("ICTSweepDetector: setup expired (>90min)")
            else:
                # Expire DETECTED setup when price has blown THROUGH the OTE
                # zone without entering it — entry permanently missed.
                # Once OTE_READY, the SL level handles invalidation.
                _s = self._active_setup
                if _s.status == "DETECTED":
                    _ote_miss = False
                    # v5.2: OTE miss threshold widened from 0.5×ATR to 1.5×ATR.
                    # Sweeps detected from warmup data had price already outside
                    # the old tight zone → instant expiry on first check (5s).
                    # 1.5×ATR gives price room to approach the OTE zone.
                    _miss_margin = 1.5 * atr
                    if _s.side == "long":
                        if price > _s.ote_entry_zone_high + _miss_margin:
                            _ote_miss = True   # price never pulled back to OTE
                        elif price < _s.ote_entry_zone_low - _miss_margin:
                            _ote_miss = True   # price blew through OTE downward
                    else:  # short
                        if price < _s.ote_entry_zone_low - _miss_margin:
                            _ote_miss = True   # price never rallied to OTE
                        elif price > _s.ote_entry_zone_high + _miss_margin:
                            _ote_miss = True   # price rallied through OTE upward
                    if _ote_miss:
                        # Record this sweep as expired so _find_best_setup will
                        # not rebuild an identical setup from the same swept pool.
                        _key = (round(_s.sweep_price, 0), _s.setup_time_ms)
                        self._expired_sweep_keys.add(_key)
                        # BUG-SWEEP-KEY-LEAK FIX: cap set size to prevent unbounded growth
                        if len(self._expired_sweep_keys) > self._MAX_EXPIRED_KEYS:
                            # Discard oldest entries — sets have no ordering, so remove
                            # arbitrary excess. In practice this fires very rarely.
                            _excess = len(self._expired_sweep_keys) - self._MAX_EXPIRED_KEYS
                            for _k in list(self._expired_sweep_keys)[:_excess]:
                                self._expired_sweep_keys.discard(_k)
                        logger.info(
                            f"❌ ICTSweepDetector: {_s.side.upper()} OTE missed "
                            f"(price=${price:,.0f} outside "
                            f"[${_s.ote_entry_zone_low:.0f}–${_s.ote_entry_zone_high:.0f}]"
                            f" ± {0.5*atr:.0f}) — setup expired, sweep blacklisted")
                        self._active_setup = None

                # v5.1: If active setup survived validity checks, DON'T re-scan.
                # The old code fell through to _find_best_setup every 5s, which
                # re-detected the identical setup from the same swept pool and
                # logged "SWEEP SETUP DETECTED" 20+ times for one setup.
                if self._active_setup is not None:
                    # Update OTE_READY status
                    if self._active_setup.status == "DETECTED":
                        if self._active_setup.price_in_ote(price):
                            self._active_setup.status = "OTE_READY"
                            logger.info(
                                f"🟢 OTE ZONE REACHED: ${price:,.0f} in "
                                f"[${self._active_setup.ote_entry_zone_low:.0f}–"
                                f"${self._active_setup.ote_entry_zone_high:.0f}]")
                    return self._active_setup

        # Find the best new setup (only reached when no active setup exists)
        setup = self._find_best_setup(ict_engine, price, atr, now_ms,
                                       candles_5m or [], candles_15m or [])
        if setup is not None:
            self._active_setup = setup
            # FIX Bug-4: f-string with conditional format spec crashes when
            # delivery_target is None — format the value before embedding it.
            _dt_str = (f"${setup.delivery_target:,.0f}"
                       if setup.delivery_target is not None else "N/A")
            logger.info(
                f"🎯 ICT SWEEP SETUP DETECTED: {setup.side.upper()} | "
                f"sweep=${setup.sweep_price:.0f} | "
                f"OTE=[${setup.ote_entry_zone_low:.0f}–${setup.ote_entry_zone_high:.0f}] | "
                f"SL=${setup.sl_sweep_candle:.0f} | "
                f"delivery={_dt_str} | "
                f"quality={setup.quality_score():.2f} | "
                f"AMD={setup.amd_confidence:.2f}")

        # Update OTE_READY status for active setup
        if self._active_setup is not None and self._active_setup.status == "DETECTED":
            if self._active_setup.price_in_ote(price):
                self._active_setup.status = "OTE_READY"
                logger.info(
                    f"🟢 OTE ZONE REACHED: ${price:,.0f} in "
                    f"[${self._active_setup.ote_entry_zone_low:.0f}–"
                    f"${self._active_setup.ote_entry_zone_high:.0f}]")

        return self._active_setup

    def _find_best_setup(self, ict_engine, price: float, atr: float,
                         now_ms: int, candles_5m: List[Dict],
                         candles_15m: List[Dict]) -> Optional[ICTSweepSetup]:
        """Scan for the best qualifying sweep setup."""
        amd = ict_engine._amd

        # AMD must be in an actionable delivery phase
        if amd.phase not in ("MANIPULATION", "DISTRIBUTION",
                              "REACCUMULATION", "REDISTRIBUTION"):
            return None
        if amd.confidence < 0.45:
            return None
        if amd.bias == "neutral":
            return None

        # Find the most recent displaced sweep
        swept = [p for p in ict_engine.liquidity_pools
                 if p.swept and p.displacement_confirmed]
        if not swept:
            return None

        swept.sort(key=lambda p: p.sweep_timestamp, reverse=True)
        latest_sweep = swept[0]
        sweep_age_ms = now_ms - latest_sweep.sweep_timestamp

        if sweep_age_ms > ict_engine.AMD_DISTRIB_WINDOW_MS:
            return None

        # BUG-STALE-SWEEP-REDETECT FIX: skip sweeps that have already been
        # tried and had their OTE zone permanently missed.  Without this check
        # the same swept pool rebuilds an identical setup every 5s indefinitely.
        _sweep_key = (round(latest_sweep.price, 0), latest_sweep.sweep_timestamp)
        if _sweep_key in self._expired_sweep_keys:
            return None

        side = "long" if latest_sweep.level_type == "SSL" else "short"

        # The direction must match AMD bias
        if side == "long" and amd.bias != "bullish":
            return None
        if side == "short" and amd.bias != "bearish":
            return None

        # Find the displacement candle geometry from candles
        sweep_candle_extreme, disp_high, disp_low, disp_close = \
            self._find_displacement_geometry(latest_sweep, side, candles_5m, atr)

        if sweep_candle_extreme is None:
            # Use approximation from pool price + ATR if candles not available
            # Track that we're using approximation — OTE guards will be looser
            _geometry_approx = True
            sweep_candle_extreme = (latest_sweep.price - 0.3 * atr
                                    if side == "long"
                                    else latest_sweep.price + 0.3 * atr)
            if side == "long":
                disp_high  = latest_sweep.price + 2.0 * atr
                disp_low   = latest_sweep.price
                disp_close = latest_sweep.price + 1.5 * atr
            else:
                disp_high  = latest_sweep.price
                disp_low   = latest_sweep.price - 2.0 * atr
                disp_close = latest_sweep.price - 1.5 * atr
        else:
            _geometry_approx = False

        # Compute OTE zone (61.8%–78.6% retracement of displacement impulse)
        #
        # BUG-OTE-MOVE-TO FIX: the original code used disp_close as move_to
        # for both long and short.  ICT OTE is measured from the sweep wick
        # extreme to the TOP of the displacement candle (for a long setup),
        # because the institutional impulse extended to that high — not just
        # where it closed.  Using disp_close shrinks the Fibonacci range,
        # placing the OTE zone too far from the sweep level and creating
        # setups that look valid but are actually outside the real 61.8-78.6%
        # of the full sweep-to-impulse-high move.
        #
        # Correct:
        #   LONG:  move_from = sweep wick low, move_to = disp_high  (top of impulse)
        #   SHORT: move_from = sweep wick high, move_to = disp_low   (bottom of impulse)
        if side == "long":
            move_from = sweep_candle_extreme   # absolute low of sweep wick
            move_to   = disp_high              # TOP of displacement candle (full impulse)
            move_size = move_to - move_from
            if move_size < 0.5 * atr:          # too small to be institutional
                return None
            ote_high = move_to - self.OTE_LOWER_FIB * move_size   # 61.8% retrace
            ote_low  = move_to - self.OTE_UPPER_FIB * move_size   # 78.6% retrace
            sl_sweep = sweep_candle_extreme - 0.08 * atr
            sl_ob    = disp_low - 0.15 * atr
        else:
            move_from = sweep_candle_extreme   # absolute high of sweep wick
            move_to   = disp_low               # BOTTOM of displacement candle (full impulse)
            move_size = move_from - move_to
            if move_size < 0.5 * atr:
                return None
            ote_low  = move_to + self.OTE_LOWER_FIB * move_size   # 61.8% retrace up
            ote_high = move_to + self.OTE_UPPER_FIB * move_size   # 78.6% retrace up
            sl_sweep = sweep_candle_extreme + 0.08 * atr
            sl_ob    = disp_high + 0.15 * atr

        # OTE guard: check both boundaries before building setup.
        #
        # BUG-OTE-APPROX-GUARD FIX: when displacement geometry is approximated
        # (no real candle data found), the OTE zone is constructed from a fixed
        # 2.0×ATR estimate which may not reflect reality. The approximated OTE
        # for SSL@$70,795 with ATR=141 gives ote_low≈$70,822 — but price at
        # $70,215 is ~$600 below that, causing IMMEDIATE blacklisting even though
        # price is still in a perfectly valid retracement zone below the sweep.
        #
        # Rule: for APPROXIMATED geometry, the only hard invalidation is price
        # dropping BELOW the sweep wick (sweep_candle_extreme) by more than
        # 1.0×ATR — meaning institutional structure has genuinely broken.
        # The upper guard (price too far above OTE) remains strict either way.
        _half_atr = 0.5 * atr
        if side == "long":
            if price > ote_high + _half_atr:
                # Price never pulled back to OTE — entry missed
                self._expired_sweep_keys.add(_sweep_key)
                return None
            if _geometry_approx:
                # Loose lower guard: only invalidate if price is below sweep wick
                if price < sweep_candle_extreme - 1.0 * atr:
                    self._expired_sweep_keys.add(_sweep_key)
                    return None
            else:
                if price < ote_low - _half_atr:
                    # Price blew through OTE downward — invalidated
                    self._expired_sweep_keys.add(_sweep_key)
                    return None
        else:  # short
            if price < ote_low - _half_atr:
                # Price never rallied to OTE — entry missed
                self._expired_sweep_keys.add(_sweep_key)
                return None
            if _geometry_approx:
                if price > sweep_candle_extreme + 1.0 * atr:
                    self._expired_sweep_keys.add(_sweep_key)
                    return None
            else:
                if price > ote_high + _half_atr:
                    # Price rallied through OTE upward — invalidated
                    self._expired_sweep_keys.add(_sweep_key)
                    return None

        # Find 15m swing SL fallback
        sl_15m = self._find_15m_swing_sl(side, price, atr, candles_15m)

        # Find delivery target
        delivery_target, delivery_label = self._find_delivery_target(
            ict_engine, side, price, atr)

        # Check for OB/FVG in OTE zone (higher conviction)
        has_ob_in_ote  = self._check_ob_in_zone(ict_engine, side, ote_low, ote_high, now_ms)
        has_fvg_in_ote = self._check_fvg_in_zone(ict_engine, side, ote_low, ote_high, now_ms)

        # Kill zone check
        kill_zone = ict_engine._killzone or ""

        # MTF alignment
        mtf_aligned = ict_engine.get_market_bias().strength >= 0.55

        # Displacement strength
        disp_strength = abs(disp_close - (disp_low if side == "long" else disp_high)) / max(atr, 1e-9)

        return ICTSweepSetup(
            side=side,
            sweep_price=latest_sweep.price,
            sweep_wick_extreme=sweep_candle_extreme,
            displacement_high=disp_high,
            displacement_low=disp_low,
            displacement_close=disp_close,
            ote_entry_zone_low=ote_low,
            ote_entry_zone_high=ote_high,
            sl_sweep_candle=sl_sweep,
            sl_ob_extreme=sl_ob,
            sl_15m_swing=sl_15m if sl_15m is not None else sl_ob,
            delivery_target=delivery_target,
            delivery_target_label=delivery_label,
            amd_confidence=amd.confidence,
            has_fvg_in_ote=has_fvg_in_ote,
            has_ob_in_ote=has_ob_in_ote,
            kill_zone=kill_zone,
            mtf_aligned=mtf_aligned,
            displacement_strength=disp_strength,
            setup_time_ms=latest_sweep.sweep_timestamp,
            status="DETECTED",
        )

    def _find_displacement_geometry(
            self, sweep_pool, side: str, candles_5m: List[Dict],
            atr: float) -> Tuple[Optional[float], Optional[float],
                                  Optional[float], Optional[float]]:
        """
        Identify sweep candle extreme + displacement candle geometry from 5m candles.

        Candle timestamps are OPEN times. Sweep timestamp is also the OPEN time of
        the candle where the sweep occurred (from ict_engine._detect_sweeps).
        Allow a 5-minute tolerance (one candle width) for timestamp matching.
        """
        if not candles_5m or len(candles_5m) < 5:
            return None, None, None, None

        pool_price = sweep_pool.price
        sweep_ts   = sweep_pool.sweep_timestamp
        CANDLE_MS  = 5 * 60 * 1000   # 5 minutes in ms

        # BUG-HOT-LOOP-IMPORT FIX: the original code called __import__('config')
        # inside the candle-scan loop, which fires up to 13 times per call.
        # Python caches the module but the attribute lookup + frame allocation
        # still adds avoidable overhead in the hot path.  Cache once before the loop.
        try:
            import config as _cfg_mod
            _tick = float(getattr(_cfg_mod, 'TICK_SIZE', 0.5))
        except Exception:
            _tick = 0.5

        # Find the sweep candle by timestamp proximity (allow ±1 candle)
        for i in range(len(candles_5m) - 1, 0, -1):
            c = candles_5m[i]
            c_ts = int(c.get('t', 0))

            # Skip candles clearly newer than the sweep (>1 candle after)
            if c_ts > sweep_ts + CANDLE_MS:
                continue

            # Don't look more than 10 candles (50 min) before the sweep
            if c_ts < sweep_ts - 10 * CANDLE_MS:
                break

            if side == "long":
                # Sweep candle: wick must actually touch or breach the SSL level.
                # FIX 11: old tolerance 0.2% = $140 at BTC $70k — candles that
                # merely approached the level were wrongly classified as sweep candles,
                # producing misplaced OTE zones. One tick tolerance is correct.
                if float(c['l']) <= pool_price + _tick:
                    sweep_extreme = float(c['l'])
                    # Displacement: next 1-3 candles — find first that closes above pool
                    for j in range(i + 1, min(i + 4, len(candles_5m))):
                        d = candles_5m[j]
                        if float(d['c']) > pool_price:
                            return (sweep_extreme,
                                    float(d['h']), float(d['l']), float(d['c']))
                    break
            else:
                # Sweep candle: wick must actually touch or breach the BSL level.
                # FIX 11: symmetric tight tolerance for short (BSL sweep).
                if float(c['h']) >= pool_price - _tick:
                    sweep_extreme = float(c['h'])
                    for j in range(i + 1, min(i + 4, len(candles_5m))):
                        d = candles_5m[j]
                        if float(d['c']) < pool_price:
                            return (sweep_extreme,
                                    float(d['h']), float(d['l']), float(d['c']))
                    break

        return None, None, None, None

    def _find_delivery_target(
            self, ict_engine, side: str, price: float,
            atr: float) -> Tuple[Optional[float], str]:
        """
        AMD delivery target = nearest opposing unswept liquidity pool.

        SSL swept (long) → target nearest unswept BSL above price
        BSL swept (short) → target nearest unswept SSL below price
        """
        amd = ict_engine._amd
        if amd.delivery_target is not None:
            # ICT engine already computed the nearest opposing pool
            label = f"AMD_delivery@${amd.delivery_target:.0f}"
            return amd.delivery_target, label

        # Fallback: scan pools manually
        unswept = [p for p in ict_engine.liquidity_pools if not p.swept]
        if side == "long":
            candidates = [p for p in unswept
                          if p.level_type == "BSL" and p.price > price + atr]
            if candidates:
                best = min(candidates, key=lambda p: p.price)
                return best.price, f"BSL_pool@${best.price:.0f}(t={best.touch_count})"
        else:
            candidates = [p for p in unswept
                          if p.level_type == "SSL" and p.price < price - atr]
            if candidates:
                best = max(candidates, key=lambda p: p.price)
                return best.price, f"SSL_pool@${best.price:.0f}(t={best.touch_count})"

        return None, ""

    def _find_15m_swing_sl(self, side: str, price: float, atr: float,
                            candles_15m: List[Dict]) -> Optional[float]:
        if not candles_15m or len(candles_15m) < 3:
            return None
        lb = min(40, len(candles_15m) - 2)
        highs, lows = _find_swings(candles_15m, lb)
        buf = 0.30 * atr
        if side == "long":
            candidates = [l - buf for l in lows if l < price]
            return max(candidates) if candidates else None
        else:
            candidates = [h + buf for h in highs if h > price]
            return min(candidates) if candidates else None

    def _check_ob_in_zone(self, ict_engine, side: str,
                           low: float, high: float, now_ms: int) -> bool:
        obs = (ict_engine.order_blocks_bull if side == "long"
               else ict_engine.order_blocks_bear)
        for ob in obs:
            if not ob.is_active(now_ms):
                continue
            if ob.low <= high and ob.high >= low:   # overlaps OTE zone
                return True
        return False

    def _check_fvg_in_zone(self, ict_engine, side: str,
                            low: float, high: float, now_ms: int) -> bool:
        fvgs = (ict_engine.fvgs_bull if side == "long"
                else ict_engine.fvgs_bear)
        for fvg in fvgs:
            if not fvg.is_active(now_ms):
                continue
            if fvg.bottom <= high and fvg.top >= low:  # overlaps OTE zone
                return True
        return False

    def get_active_setup(self) -> Optional[ICTSweepSetup]:
        return self._active_setup

    def invalidate(self):
        """Call when a position is opened to clear the active setup."""
        if self._active_setup is not None:
            self._active_setup.status = "EXPIRED"
        self._active_setup = None


# ═══════════════════════════════════════════════════════════════════════════
# ICT SL ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class ICTSLEngine:
    """
    Institutional SL placement — sweep-first hierarchy.

    Priority (tightest/highest conviction → loosest/fallback):

    TIER-1: SWEEP CANDLE INVALIDATION (default for sweep entries)
      SL at the absolute wick extreme of the sweep candle - buffer.
      This IS the smart money's invalidation level. If price goes back
      through the sweep wick, the sweep was a failure — not a setup.
      Typical distance: 0.5–1.0×ATR from entry.

    TIER-2: DISPLACEMENT OB EXTREME
      SL at the OB created by the displacement candle.
      If price re-enters the OB that caused the impulse, the move is
      invalidated — institutional orders at the OB extreme are the anchor.
      Typical distance: 0.8–1.5×ATR from entry.

    TIER-3: ICT 15m ORDER BLOCK (HTF structure)
      SL at the HTF OB boundary. Used when no sweep context is available
      (standard non-sweep entries). 15m OBs represent institutional order
      placement zones — thesis is invalidated if price closes through.

    TIER-4: 15m STRUCTURAL SWING
      Last confirmed 15m pivot low/high. Wide lookback (10 hours).
      Only used when tiers 1-3 are not applicable.

    ATR SAFETY BOUNDS: SL must be 0.3%–3.5% from price and 0.4×ATR–4.0×ATR.
    """

    @staticmethod
    def compute(side: str, price: float, atr: float,
                sweep_setup: Optional['ICTSweepSetup'],
                ict_engine=None,
                candles_15m: List[Dict] = None,
                atr_pctile: float = 0.5,
                mode: str = "reversion",
                market_regime: str = "RANGING",
                ) -> Tuple[float, str]:
        """
        Returns (sl_price, source_label).

        v6.0 UPGRADES:
          1. Regime-adaptive SL floor (trending = wider, ranging = tighter)
          2. Liquidity proximity guard — SL must not sit ON a BSL/SSL cluster
          3. Trend/momentum max_dist widened to 3.0×ATR (was 2.0×)
        """
        # ── Regime-adaptive SL floor ──────────────────────────────────────
        # In trending markets, tight SLs get swept by normal volatility.
        # In ranging markets, wider SLs give back too much.
        if market_regime in ("TRENDING_UP", "TRENDING_DOWN"):
            min_dist = max(price * 0.005, 1.0 * atr)   # 1.0 ATR min in trends
        elif market_regime == "TRANSITIONING":
            min_dist = max(price * 0.004, 0.7 * atr)
        else:
            min_dist = max(price * 0.003, 0.40 * atr)   # ranging default

        max_dist = price * 0.035

        if mode in ("trend", "momentum"):
            max_dist = min(max_dist, 3.0 * atr)   # was 2.0 — too tight for trends

        sl_price = None
        source   = "none"

        # ── TIER-1: Sweep candle wick ─────────────────────────────────────
        if sweep_setup is not None and sweep_setup.sl_sweep_candle > 0:
            candidate = sweep_setup.sl_sweep_candle
            dist      = abs(price - candidate)
            valid_dir = ((side == "long"  and candidate < price) or
                         (side == "short" and candidate > price))
            if valid_dir and min_dist <= dist <= max_dist:
                sl_price = candidate
                source   = f"SWEEP_WICK@${sweep_setup.sweep_wick_extreme:.0f}"
                logger.info(
                    f"🎯 SL TIER-1 (sweep wick): ${sl_price:,.2f} | "
                    f"{dist:.0f}pts / {dist/max(atr,1e-9):.2f}ATR | "
                    f"sweep=${sweep_setup.sweep_price:.0f}")

        # ── TIER-2: Displacement OB extreme ───────────────────────────────
        if sl_price is None and sweep_setup is not None:
            candidate = sweep_setup.sl_ob_extreme
            dist      = abs(price - candidate)
            valid_dir = ((side == "long"  and candidate < price) or
                         (side == "short" and candidate > price))
            if valid_dir and min_dist <= dist <= max_dist:
                sl_price = candidate
                source   = f"DISP_OB@${candidate:.0f}"
                logger.info(
                    f"📐 SL TIER-2 (displacement OB): ${sl_price:,.2f} | "
                    f"{dist:.0f}pts / {dist/max(atr,1e-9):.2f}ATR")

        # ── TIER-3: ICT 15m Order Block ───────────────────────────────────
        if sl_price is None and ict_engine is not None:
            try:
                now_ms = int(time.time() * 1000)
                ob_sl  = ict_engine.get_ob_sl_level(
                    side, price, atr, now_ms, htf_only=True)
                if ob_sl is not None:
                    dist      = abs(price - ob_sl)
                    valid_dir = ((side == "long"  and ob_sl < price) or
                                 (side == "short" and ob_sl > price))
                    if valid_dir and min_dist <= dist <= max_dist:
                        sl_price = ob_sl
                        source   = f"ICT_15m_OB@${ob_sl:.0f}"
                        logger.info(
                            f"🏛️ SL TIER-3 (15m OB): ${sl_price:,.2f} | "
                            f"{dist:.0f}pts / {dist/max(atr,1e-9):.2f}ATR")
            except Exception as e:
                logger.debug(f"ICT OB SL error: {e}")

        # ── TIER-4: 15m structural swing ──────────────────────────────────
        if sl_price is None and candles_15m and len(candles_15m) >= 3:
            buf_mult = 0.35 * (1.4 - 0.8 * min(max(atr_pctile, 0.0), 1.0))
            buf      = buf_mult * atr
            lb       = min(40, len(candles_15m) - 2)
            highs, lows = _find_swings(candles_15m, lb)

            candidates = []
            if side == "long":
                for lvl in lows:
                    if lvl < price:
                        d = price - lvl
                        if d <= max_dist:
                            candidates.append((lvl - buf * 0.80, d))
            else:
                for lvl in highs:
                    if lvl > price:
                        d = lvl - price
                        if d <= max_dist:
                            candidates.append((lvl + buf * 0.80, d))

            if candidates:
                # Nearest structural level
                best_sl, best_dist = min(candidates, key=lambda x: x[1])
                if best_dist >= min_dist:
                    sl_price = best_sl
                    source   = f"15m_SWING@${best_sl:.0f}"
                    logger.info(
                        f"📊 SL TIER-4 (15m swing): ${sl_price:,.2f} | "
                        f"{best_dist:.0f}pts / {best_dist/max(atr,1e-9):.2f}ATR")

        # ── ATR FALLBACK (no structure found) ─────────────────────────────
        if sl_price is None:
            sl_dist  = max(min_dist, min(max_dist, 1.5 * atr))
            sl_price = (price - sl_dist if side == "long" else price + sl_dist)
            source   = f"ATR_1.5x@${sl_price:.0f}"
            logger.warning(
                f"⚠️ SL ATR fallback: ${sl_price:,.2f} "
                f"({sl_dist:.0f}pts / 1.5ATR) — no structure found")

        # ── ABSOLUTE SAFETY BOUNDS ────────────────────────────────────────
        sl_dist_final = abs(price - sl_price)
        if sl_dist_final < min_dist:
            sl_price = (price - min_dist if side == "long"
                        else price + min_dist)
            source += "(expanded_min)"
        elif sl_dist_final > max_dist:
            sl_price = (price - max_dist if side == "long"
                        else price + max_dist)
            source += "(capped_max)"

        # ── LIQUIDITY PROXIMITY GUARD (v6.0) ──────────────────────────────
        # INSTITUTIONAL PRINCIPLE: SL must NEVER sit on or near a liquidity
        # pool (BSL/SSL cluster). Smart money sweeps these levels to fill
        # their orders — your SL feeding their entry is the worst outcome.
        #
        # Check: if SL is within 0.6×ATR of an unswept liquidity pool,
        # move SL BEHIND the pool (further from entry) so the sweep has to
        # go through AND past the pool before reaching your SL.
        if ict_engine is not None and hasattr(ict_engine, 'liquidity_pools'):
            _liq_guard_dist = 0.6 * atr
            _liq_buffer     = 0.35 * atr  # space behind the pool
            for pool in ict_engine.liquidity_pools:
                if pool.swept:
                    continue
                dist_sl_to_pool = abs(sl_price - pool.price)
                if dist_sl_to_pool > _liq_guard_dist:
                    continue  # SL is far enough from this pool

                if side == "long":
                    # SL is below price; if a SSL cluster is near our SL,
                    # move SL below the pool
                    if pool.level_type == "SSL" and pool.price < price:
                        new_sl = pool.price - _liq_buffer
                        if abs(price - new_sl) <= max_dist:
                            logger.info(
                                f"🛡️ SL LIQUIDITY GUARD: moved SL ${sl_price:,.2f} → "
                                f"${new_sl:,.2f} (behind SSL@${pool.price:,.0f}, "
                                f"touches={pool.touch_count})")
                            sl_price = new_sl
                            source += f"(liq_guard_SSL@{pool.price:.0f})"
                elif side == "short":
                    # SL is above price; if a BSL cluster is near our SL,
                    # move SL above the pool
                    if pool.level_type == "BSL" and pool.price > price:
                        new_sl = pool.price + _liq_buffer
                        if abs(new_sl - price) <= max_dist:
                            logger.info(
                                f"🛡️ SL LIQUIDITY GUARD: moved SL ${sl_price:,.2f} → "
                                f"${new_sl:,.2f} (behind BSL@${pool.price:,.0f}, "
                                f"touches={pool.touch_count})")
                            sl_price = new_sl
                            source += f"(liq_guard_BSL@{pool.price:.0f})"

        return _round_tick(sl_price), source


# ═══════════════════════════════════════════════════════════════════════════
# ICT TP ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class ICTTPEngine:
    """
    Institutional TP placement — AMD delivery targeting.

    TP selection priority:

    TIER-S (score ≥ 7): AMD DELIVERY TARGET
      The swept liquidity origin / opposing unswept pool identified by the
      AMD engine. This is WHERE smart money is delivering price after the sweep.
      Non-negotiable target — do not settle for an FVG short of the delivery.
      Required R:R: ≥ 1.5R (never enter if delivery is < 1.5R away).

    TIER-A (score 5–7): OPPOSING LIQUIDITY POOL
      Next unswept BSL (for long) or SSL (for short).
      Equal highs/lows represent clustered stop orders — high probability target.
      Score bonus for touch_count (more stops = stronger magnet).

    TIER-B (score 3–5): ICT STRUCTURAL (FVG, Virgin OB, 15m swing)
      Unfilled FVGs in the delivery path, virgin OBs, 15m swing extremes.
      Standard ICT structural targets as in v4.9.

    TIER-C (score 1–3): VWAP FRACTION / ATR CHANNEL
      Statistical reference levels. ATR-based channel for trend entries.
      Minimum R:R gates enforced before accepting.

    R:R ENFORCEMENT:
      Reversion: min 1.8R, max 3.5R
      Trend/Momentum: min 2.5R, max 6.0R

    If NO target clears the minimum R:R, REJECT the setup (return None).
    Do not lower the bar — a bad R:R setup should not be traded.
    """

    @staticmethod
    def compute(side: str, price: float, atr: float, sl_price: float,
                sweep_setup: Optional['ICTSweepSetup'],
                ict_engine=None,
                candles_15m: List[Dict] = None,
                vwap: float = 0.0,
                mode: str = "reversion",
                now_ms: int = 0,
                ) -> Optional[float]:
        """
        Returns tp_price or None if no valid target meets the R:R gate.
        """
        sl_dist = abs(price - sl_price)
        if sl_dist < 1e-10:
            return None

        # TP-BUG-4 FIX: Sweep OTE entries are DIRECTIONAL (AMD delivery move),
        # not mean-reversion entries. Applying reversion R:R (max 4R) caps the
        # delivery target when AMD is delivering 4-6R away — forcing the engine
        # to either reject valid setups or use a worse structural level as TP.
        # Fix: sweep mode inherits trend R:R (2.5R–6.0R).
        _is_sweep = (sweep_setup is not None and
                     sweep_setup.status in ("OTE_READY", "ACTIVE"))
        effective_mode = "trend" if _is_sweep and mode == "reversion" else mode

        if effective_mode in ("trend", "momentum"):
            min_rr = 2.5;  max_rr = 6.0
        else:
            min_rr = 1.8;  max_rr = 4.0

        min_tp_dist = sl_dist * min_rr
        max_tp_dist = sl_dist * max_rr

        # TP-BUG-3 FIX: AMD delivery targets are institutional — they represent
        # WHERE smart money is delivering price after the sweep. Capping them at
        # max_rr rejects valid targets just because they are far away.
        # AMD delivery targets bypass the upper R:R cap (max 8R hard ceiling only).
        amd_max_dist = sl_dist * 8.0

        if now_ms <= 0:
            now_ms = int(time.time() * 1000)

        scored: List[Tuple[float, float, str]] = []   # (price, score, label)

        def _add(tp_cand: float, score: float, label: str,
                 bypass_max_rr: bool = False):
            """Add a TP candidate. AMD delivery targets pass bypass_max_rr=True."""
            dist = (tp_cand - price) if side == "long" else (price - tp_cand)
            upper_cap = amd_max_dist if bypass_max_rr else max_tp_dist
            if dist < min_tp_dist or dist > upper_cap:
                return
            if side == "long"  and tp_cand <= price:
                return
            if side == "short" and tp_cand >= price:
                return
            scored.append((tp_cand, score, label))

        # ── TIER-S: Sweep delivery target (BYPASSES max_rr — AMD delivery) ─
        if sweep_setup is not None and sweep_setup.delivery_target is not None:
            _add(sweep_setup.delivery_target, 8.0,
                 f"AMD_DELIVERY {sweep_setup.delivery_target_label}",
                 bypass_max_rr=True)

        # AMD engine delivery target (may differ from sweep_setup)
        if ict_engine is not None:
            amd = ict_engine._amd
            if (amd.delivery_target is not None and
                    amd.confidence >= 0.55):
                _add(amd.delivery_target, 7.5,
                     f"AMD_target(conf={amd.confidence:.2f})",
                     bypass_max_rr=True)

        # ── TIER-A: Opposing unswept liquidity pools ───────────────────────
        if ict_engine is not None:
            for pool in ict_engine.liquidity_pools:
                if pool.swept:
                    continue
                if side == "long" and pool.level_type == "BSL" and pool.price > price:
                    score = 5.5 + min(pool.touch_count * 0.25, 1.5)
                    _add(pool.price, score,
                         f"BSL_pool@${pool.price:.0f}(t={pool.touch_count})")
                elif side == "short" and pool.level_type == "SSL" and pool.price < price:
                    score = 5.5 + min(pool.touch_count * 0.25, 1.5)
                    _add(pool.price, score,
                         f"SSL_pool@${pool.price:.0f}(t={pool.touch_count})")

        # ── TIER-B: ICT structural targets ────────────────────────────────
        if ict_engine is not None:
            try:
                ict_targets = ict_engine.get_structural_tp_targets(
                    side, price, atr, now_ms,
                    min_tp_dist, max_tp_dist, htf_only=True)
                for lvl, sc, lbl in ict_targets:
                    _add(lvl, sc, lbl)
            except Exception as e:
                logger.debug(f"ICT TP targets error: {e}")

        # ── TIER-C: 15m swing extremes ────────────────────────────────────
        if candles_15m and len(candles_15m) >= 3:
            lb = min(40, len(candles_15m) - 2)
            highs, lows = _find_swings(candles_15m, lb)
            if side == "long" and highs:
                for h in highs:
                    if h > price:
                        _add(h - 0.05 * atr, 4.0, f"15m_SH@${h:.0f}")
            elif side == "short" and lows:
                for l in lows:
                    if l < price:
                        _add(l + 0.05 * atr, 4.0, f"15m_SL@${l:.0f}")

        # ── TIER-C: VWAP reference ────────────────────────────────────────
        if vwap > 1.0:
            vwap_dist = (vwap - price) if side == "long" else (price - vwap)
            if vwap_dist >= min_tp_dist * 0.80:
                _add(vwap, 3.5, f"VWAP@${vwap:.0f}")

        # ── TIER-C: ATR channel (trend/momentum only) ─────────────────────
        if mode in ("trend", "momentum"):
            atr_mult = 3.5 if mode == "trend" else 3.0
            atr_tp   = (price + atr * atr_mult if side == "long"
                        else price - atr * atr_mult)
            _add(atr_tp, 2.5, f"ATR_{atr_mult}x@${atr_tp:.0f}")

        if not scored:
            logger.info(
                f"⛔ TP ENGINE: no target in R:R window "
                f"[{min_rr:.1f}R–{max_rr:.1f}R = "
                f"${min_tp_dist:.0f}–${max_tp_dist:.0f}pts] — REJECT setup")
            return None

        # ── TIER-S first, then by score, then nearest ─────────────────────
        tier_s = [(p, s, l) for p, s, l in scored if s >= 7.0]
        tier_a = [(p, s, l) for p, s, l in scored if 5.0 <= s < 7.0]
        tier_b = [(p, s, l) for p, s, l in scored if 3.5 <= s < 5.0]
        tier_c = [(p, s, l) for p, s, l in scored if s < 3.5]

        for tier, tier_name in [
                (tier_s, "TIER-S"),
                (tier_a, "TIER-A"),
                (tier_b, "TIER-B"),
                (tier_c, "TIER-C")]:
            if not tier:
                continue
            tier.sort(key=lambda x: (-x[1], abs(x[0] - price)))
            tp, sc, lbl = tier[0]
            rr = abs(tp - price) / sl_dist
            logger.info(
                f"🎯 TP {tier_name}: ${tp:,.2f} | score={sc:.1f} | "
                f"R:R=1:{rr:.2f} | {lbl}")
            return _round_tick(tp)

        return None   # unreachable but safe


# ═══════════════════════════════════════════════════════════════════════════
# ICT TRAIL ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class ICTTrailEngine:
    """
    AMD-Phase-Aware Dynamic Trailing SL — v1.0

    NO TIMEOUTS. Pure structure + AMD phase state machine.

    Phase State Machine:
    ─────────────────────────────────────────────────────────────────────
    AMD MANIPULATION (<15min after sweep)
      → Trail FROZEN. The Judas swing is ACTIVE. Price may retrace
        deeply before distribution begins. Any trail here gets stopped.
      → EXCEPTION: if profit ≥ 1.5R AND displacement has confirmed BOS
        on 5m, allow a single BE advance only.

    AMD DISTRIBUTION (15min–90min after sweep)
      → Phase-1 (0.5R–1.0R):
           Trail behind nearest ACTIVE bullish OB low (long) or bearish
           OB high (short). Min distance: 1.5×ATR.
           This is the institutional order zone — OB holds or it doesn't.
      → Phase-2 (1.0R–2.0R):
           Trail behind 5m swing lows (long) / 5m swing highs (short).
           Min distance: 1.0×ATR.
      → Phase-3 (2.0R+):
           Trail behind 1m swing structure.
           Min distance: 0.7×ATR.
           BOS on 5m in direction → advance trail to just behind that swing.

      Special triggers (immediate advance regardless of phase):
        • FVG in delivery path fills ≥ 70%: lock profit floor immediately
        • 5m BOS confirms in direction: advance to behind that swing low/high
        • Pullback into active OB: FREEZE (structural defense zone)

    AMD ACCUMULATION (>90min, old sweep)
      → Very conservative. Only advance on 15m structural breaks.
        Min distance: 2.0×ATR.

    REACCUMULATION / REDISTRIBUTION
      → Moderate. Behind 5m swings. Min 1.2×ATR.

    PULLBACK FREEZE:
      If price is in an active OB or inside an active FVG AND position
      is profitable, trail is FROZEN. These are institutional zones where
      smart money defends the move. Tightening SL here is premature.
    ─────────────────────────────────────────────────────────────────────
    """

    @staticmethod
    def compute(pos_side: str, price: float, entry_price: float,
                current_sl: float, atr: float,
                initial_sl_dist: float,
                peak_profit: float,
                peak_price_abs: float,
                hold_seconds: float,
                candles_1m: List[Dict],
                candles_5m: List[Dict],
                orderbook: Dict,
                entry_vol: float,
                trade_mode: str,
                ict_engine=None,
                now_ms: int = 0,
                hold_reason: Optional[List[str]] = None,
                ) -> Optional[float]:
        """
        Returns new_sl or None (hold/freeze).
        """
        if atr < 1e-10:
            return None

        if now_ms <= 0:
            now_ms = int(time.time() * 1000)

        # ── R-multiple computation ─────────────────────────────────────────
        init_dist = (initial_sl_dist if initial_sl_dist > 1e-10
                     else max(abs(entry_price - current_sl), atr))

        profit = ((price - entry_price) if pos_side == "long"
                  else (entry_price - price))

        # Phase ratchets UP — once earned, never lost (use peak profit)
        tier = max(profit, peak_profit) / init_dist if init_dist > 1e-10 else 0.0

        # ── Phase-0: Hands off ────────────────────────────────────────────
        # v6.0: raised from 0.50R to 0.70R. At 0.50R the trade barely covered
        # fees + slippage. Trailing at this level gives back the profit on the
        # first normal pullback. Smart money lets the move develop first.
        if tier < 0.70:
            if hold_reason is not None:
                hold_reason.append(f"PHASE0 tier={tier:.2f}R<0.70R")
            return None

        # ── AMD phase determination ───────────────────────────────────────
        amd_phase  = "ACCUMULATION"
        amd_conf   = 0.0
        sweep_age_ms = float('inf')

        if ict_engine is not None:
            try:
                amd = ict_engine._amd
                amd_phase = amd.phase
                amd_conf  = amd.confidence
                if amd.time_in_phase_ms > 0:
                    sweep_age_ms = amd.time_in_phase_ms
            except Exception:
                pass

        # ── AMD MANIPULATION: structural freeze ─────────────────────────
        # At tier < 1.0R: Judas swing may still be active. Full freeze.
        # At tier >= 1.0R: price displaced 1× risk — Judas structurally over.
        if amd_phase == "MANIPULATION" and tier < 1.0:
            if hold_reason is not None:
                hold_reason.append(
                    f"AMD_MANIP_FREEZE(tier={tier:.2f}R conf={amd_conf:.2f})")
            return None

        # ── Phase + min-distance by AMD state ─────────────────────────────
        if amd_phase == "DISTRIBUTION" and amd_conf >= 0.50:
            if tier >= 2.0:
                phase = 3; min_dist = 0.7 * atr
            elif tier >= 1.0:
                phase = 2; min_dist = 1.0 * atr
            else:
                phase = 1; min_dist = 1.5 * atr
        elif amd_phase in ("REACCUMULATION", "REDISTRIBUTION"):
            phase = max(1, min(3, int(tier)))
            min_dist = {1: 1.2, 2: 0.9, 3: 0.7}.get(phase, 1.2) * atr
        elif amd_phase == "MANIPULATION" and tier >= 1.0:
            if tier >= 1.5:
                phase = 2; min_dist = 1.2 * atr
            else:
                phase = 1; min_dist = 1.5 * atr
        else:
            # ACCUMULATION or unknown — conservative structure-based trail.
            # ACCUMULATION means "no clear AMD delivery bias," NOT "thesis invalid."
            if tier >= 1.5:
                phase = 2; min_dist = 1.5 * atr
            elif tier >= 1.0:
                phase = 1; min_dist = 2.0 * atr
            elif tier >= 0.50:
                phase = 1; min_dist = 2.5 * atr
            else:
                if hold_reason is not None:
                    hold_reason.append(
                        f"AMD_ACCUM_HOLD(tier={tier:.2f}R)")
                return None

        # ── BE lock check ─────────────────────────────────────────────────
        be_price = (entry_price + 0.25 * atr if pos_side == "long"
                    else entry_price - 0.25 * atr)
        be_locked = ((pos_side == "long"  and current_sl >= be_price) or
                     (pos_side == "short" and current_sl <= be_price))

        # ── OB ZONE FREEZE (pullback into active OB) ──────────────────────
        if ict_engine is not None and be_locked:
            try:
                freeze_atr = 0.35 * atr
                obs = (ict_engine.order_blocks_bull if pos_side == "long"
                       else ict_engine.order_blocks_bear)
                for ob in obs:
                    if not ob.is_active(now_ms) or ob.visit_count > 1:
                        continue
                    # Only freeze if SL has already trailed into proximity
                    if pos_side == "long":
                        ob_near_sl = current_sl >= ob.low - 1.8 * atr
                    else:
                        ob_near_sl = current_sl <= ob.high + 1.8 * atr
                    if not ob_near_sl:
                        continue
                    # Guard: OB_ZONE_FREEZE is for PULLBACKS — price returning to test
                    # a previously-left OB. It must NOT fire when price is currently
                    # inside the OB during initial delivery (e.g. a live forming 4H
                    # candle whose low tracks price down). A live candle makes price
                    # always "inside" the OB by definition — permanent freeze.
                    # For SHORT: OB is above, price has moved away downward. A pullback
                    # test means price is moving back UP into the OB from below.
                    # Guard: for SHORT, ob.low must be strictly ABOVE current price,
                    # confirming price has cleanly exited the OB before we can call
                    # a return to it a "pullback test."
                    if pos_side == "short" and ob.low <= price:
                        continue  # price still inside / below OB low — not a pullback
                    if pos_side == "long" and ob.high >= price:
                        continue  # price still inside / above OB high — not a pullback
                    # Check if price is testing the OB
                    if (ob.low - freeze_atr <= price <= ob.high + freeze_atr):
                        if hold_reason is not None:
                            hold_reason.append(
                                f"OB_ZONE_FREEZE@${ob.midpoint:.0f}"
                                f"(tf={ob.timeframe})")
                        return None
            except Exception:
                pass

        # ── FVG FREEZE (price inside active FVG in the delivery path) ─────
        #
        # TRAIL-BUG-4 FIX: original code used fvgs_bull for LONG and fvgs_bear
        # for SHORT. This is WRONG. For a LONG position, price is travelling
        # UPWARD — the imbalances it is filling along the way are BEARISH FVGs
        # (created by prior downward impulses) that sit ABOVE entry. These are
        # the ones price fills going up, and they act as consolidation/pause
        # zones where the SL should NOT be tightened.
        #
        # ICT: FVG freeze applies when price enters a delivery-path imbalance:
        #   LONG (price going up) → freeze at BEARISH FVGs above (fvgs_bear)
        #   SHORT (price going down) → freeze at BULLISH FVGs below (fvgs_bull)
        if ict_engine is not None and be_locked and hold_seconds < 600.0:
            try:
                # Delivery-path FVGs: opposite direction to position (filled in direction of move)
                fvgs_delivery_path = (ict_engine.fvgs_bear if pos_side == "long"
                                      else ict_engine.fvgs_bull)
                freeze_atr = 0.40 * atr
                for fvg in fvgs_delivery_path:
                    if not fvg.is_active(now_ms):
                        continue
                    if fvg.fill_percentage > 0.40:
                        continue
                    if not (fvg.bottom - freeze_atr <= price <= fvg.top + freeze_atr):
                        continue
                    if pos_side == "long":
                        sl_near = current_sl >= fvg.bottom - 1.5 * atr
                    else:
                        sl_near = current_sl <= fvg.top + 1.5 * atr
                    if sl_near:
                        if hold_reason is not None:
                            hold_reason.append(
                                f"FVG_FREEZE@${fvg.midpoint:.0f}"
                                f"(fill={fvg.fill_percentage:.0%} delivery_path)")
                        return None
            except Exception:
                pass

        # ── PULLBACK DETECTION ────────────────────────────────────────────
        if be_locked and len(candles_1m) >= 10 and len(candles_5m) >= 5:
            is_pb, rev_count, pb_detail = ICTTrailEngine._classify_pullback(
                pos_side, price, entry_price, atr,
                candles_1m, candles_5m, orderbook, peak_price_abs)
            if is_pb:
                if hold_reason is not None:
                    hold_reason.append(
                        f"PULLBACK({rev_count}rev)[{pb_detail}]")
                return None

        # ── BUILD CANDIDATE SL LEVELS ─────────────────────────────────────
        candidates: List[Tuple[float, str]] = []

        # 1. Profit floor (fee-adjusted BE)
        rt_fee     = entry_price * 0.00055 * 2.0   # conservative taker RT cost
        pf_buf     = rt_fee + 0.25 * atr
        pf         = (entry_price + pf_buf if pos_side == "long"
                      else entry_price - pf_buf)
        candidates.append((pf, "PROFIT_FLOOR"))

        # 2. ICT OB anchor (all phases) — highest institutional validity
        #
        # TRAIL-BUG-1 FIX: original code sorted by abs(midpoint - price) and took
        # the NEAREST OB. In ICT, once price has moved away from an OB, the trail SL
        # should be anchored BELOW the HIGHEST OB that price has already left behind —
        # i.e. the most recently cleared OB, which is the strongest structural floor.
        # Sorting by midpoint ascending for long (highest low below price wins) and
        # midpoint descending for short (lowest high above price wins).
        if phase >= 1 and ict_engine is not None:
            try:
                ob_buf = 0.35 * atr
                obs = (ict_engine.order_blocks_bull if pos_side == "long"
                       else ict_engine.order_blocks_bear)
                active_obs = [o for o in obs if o.is_active(now_ms)]
                if pos_side == "long":
                    # Sort by OB low DESCENDING — highest OB below price first
                    # (the most recent structural level price has moved above)
                    candidate_obs = sorted(
                        [o for o in active_obs if o.low < price - min_dist],
                        key=lambda x: x.low, reverse=True)
                    for ob in candidate_obs:
                        cand = ob.low - ob_buf
                        if current_sl < cand < price - min_dist:
                            candidates.append((cand,
                                               f"OB_ANCHOR@${ob.midpoint:.0f}(tf={ob.timeframe})"))
                            break
                else:
                    # Sort by OB high ASCENDING — lowest OB above price first
                    candidate_obs = sorted(
                        [o for o in active_obs if o.high > price + min_dist],
                        key=lambda x: x.high)
                    for ob in candidate_obs:
                        cand = ob.high + ob_buf
                        if price + min_dist < cand < current_sl:
                            candidates.append((cand,
                                               f"OB_ANCHOR@${ob.midpoint:.0f}(tf={ob.timeframe})"))
                            break
            except Exception:
                pass

        # 3. 5m swing structure (ALL phases — buffer scales with phase)
        #    Phase 1: wide buffer (1.0×ATR) — conservative, protecting against pullbacks
        #    Phase 2: medium buffer (0.25×ATR) — structural with breathing room
        #    Phase 3: tight buffer (0.12×ATR) — aggressive locking to structure
        if candles_5m and len(candles_5m) >= 6:
            closed_5m = candles_5m[:-1]
            highs_5m, lows_5m = _find_swings(
                closed_5m, min(12, len(closed_5m) - 2))
            # Phase-scaled buffer: wide in early phase, tight when deep in profit
            if phase == 1:
                sw_buf = 1.00 * atr    # wide — protect against healthy retracement
            elif phase == 2:
                sw_buf = 0.25 * atr    # structural — swing defines thesis boundary
            else:
                sw_buf = 0.12 * atr    # aggressive — lock tightly to confirmed swings

            if pos_side == "long" and lows_5m:
                valid = [l for l in lows_5m
                         if current_sl + 0.05 * atr < l < price - min_dist]
                if valid:
                    best = max(valid)
                    cand_sl = best - sw_buf
                    # Phase 1: only add if it's an improvement (not just profit floor)
                    if phase == 1 and cand_sl <= candidates[0][0] + 0.05 * atr:
                        pass  # not an improvement over profit floor — skip
                    else:
                        candidates.append((cand_sl,
                                           f"5m_SW_L@${best:.0f}(P{phase}buf={sw_buf:.0f})"))

            elif pos_side == "short" and highs_5m:
                valid = [h for h in highs_5m
                         if price + min_dist < h < current_sl - 0.05 * atr]
                if valid:
                    best = min(valid)
                    cand_sl = best + sw_buf
                    if phase == 1 and cand_sl >= candidates[0][0] - 0.05 * atr:
                        pass
                    else:
                        candidates.append((cand_sl,
                                           f"5m_SW_H@${best:.0f}(P{phase}buf={sw_buf:.0f})"))

        # 3b. 5m BOS trigger: if 5m BOS confirmed in trade direction →
        #     immediate advance to just behind the BOS swing
        #     This is the most important ICT trail signal: BOS = continuation confirmed
        if ict_engine is not None and phase >= 1:
            try:
                tf_st = ict_engine._tf.get("5m")
                if tf_st is not None:
                    if (pos_side == "long" and
                            tf_st.bos_direction == "bullish" and
                            tf_st.bos_level > current_sl and
                            tf_st.bos_level < price - min_dist):
                        bos_sl = tf_st.bos_level - 0.20 * atr
                        candidates.append((bos_sl,
                                           f"5m_BOS_BULL@${tf_st.bos_level:.0f}"))
                    elif (pos_side == "short" and
                            tf_st.bos_direction == "bearish" and
                            tf_st.bos_level < current_sl and
                            tf_st.bos_level > price + min_dist):
                        bos_sl = tf_st.bos_level + 0.20 * atr
                        candidates.append((bos_sl,
                                           f"5m_BOS_BEAR@${tf_st.bos_level:.0f}"))
            except Exception:
                pass

        # 4. FVG fill trigger (immediate lock on 70%+ fill in delivery path)
        #
        # When a bearish FVG above price (LONG delivery path) is 70%+ filled,
        # price has absorbed that imbalance — lock profit just beyond its far edge.
        # Uses delivery-path FVGs: fvgs_bear for long (filled going up),
        # fvgs_bull for short (filled going down).
        if ict_engine is not None:
            try:
                fvgs_delivery = (ict_engine.fvgs_bear if pos_side == "long"
                                 else ict_engine.fvgs_bull)
                for fvg in fvgs_delivery:
                    if not fvg.is_active(now_ms):
                        continue
                    if fvg.fill_percentage < 0.70:
                        continue
                    # FVG mostly filled — lock just beyond its far edge
                    # LONG: FVG is above, filled going up → lock above fvg.top
                    # SHORT: FVG is below, filled going down → lock below fvg.bottom
                    lock_level = (fvg.top + 0.20 * atr if pos_side == "long"
                                  else fvg.bottom - 0.20 * atr)
                    if pos_side == "long" and current_sl < lock_level < price - min_dist:
                        candidates.append((lock_level,
                                           f"FVG_FILL_LOCK@${fvg.midpoint:.0f}"))
                    elif pos_side == "short" and price + min_dist < lock_level < current_sl:
                        candidates.append((lock_level,
                                           f"FVG_FILL_LOCK@${fvg.midpoint:.0f}"))
            except Exception:
                pass

        # 5. 1m swing structure (Phase 3)
        if phase >= 3 and len(candles_1m) >= 6:
            closed_1m = candles_1m[:-1]
            sh_1m, sl_1m = _find_swings(
                closed_1m, min(10, len(closed_1m) - 2))
            micro_buf = 0.08 * atr   # tight — 1m swings are precise micro-structure

            if pos_side == "long" and sl_1m:
                valid = [l for l in sl_1m
                         if current_sl + 0.05 * atr < l < price - min_dist]
                if valid:
                    best = max(valid)
                    candidates.append((best - micro_buf,
                                       f"1m_MICRO_SW@${best:.0f}"))

            elif pos_side == "short" and sh_1m:
                valid = [h for h in sh_1m
                         if price + min_dist < h < current_sl - 0.05 * atr]
                if valid:
                    best = min(valid)
                    candidates.append((best + micro_buf,
                                       f"1m_MICRO_SW@${best:.0f}"))

        # 6. Chandelier (Phase 3, last resort — no structural level found)
        if phase >= 3 and peak_price_abs > 1e-10 and len(candidates) <= 1:
            n_ch = 2.0 if trade_mode in ("trend", "momentum") else 2.5
            if pos_side == "long":
                chand = peak_price_abs - n_ch * atr
                if current_sl < chand < price - min_dist:
                    candidates.append((chand, f"CHANDELIER_{n_ch}x"))
            else:
                chand = peak_price_abs + n_ch * atr
                if price + min_dist < chand < current_sl:
                    candidates.append((chand, f"CHANDELIER_{n_ch}x"))

        # ── Volume decay tighten (Phase 3) ────────────────────────────────
        vol_tighten = 0.0
        if phase >= 3 and len(candles_1m) >= 10 and entry_vol > 1e-10:
            rv  = sum(float(c['v']) for c in candles_1m[-5:]) / 5.0
            vr  = rv / entry_vol
            if vr < 0.60:
                vol_tighten = 0.30 * atr * (1.0 - vr / 0.60)

        if not candidates:
            if hold_reason is not None:
                hold_reason.append("NO_CANDIDATES")
            return None

        # ── SELECT BEST ───────────────────────────────────────────────────
        if pos_side == "long":
            new_sl, anchor = max(candidates, key=lambda x: x[0])
            if vol_tighten > 0:
                new_sl = min(new_sl + vol_tighten, price - min_dist)
        else:
            new_sl, anchor = min(candidates, key=lambda x: x[0])
            if vol_tighten > 0:
                new_sl = max(new_sl - vol_tighten, price + min_dist)

        # ── LIQUIDITY POOL GUARD FOR TRAIL (v6.0 FIX) ─────────────────────
        # BUG FIX: original used pool.pool_type=="EQL"/"EQH" — LiquidityLevel
        # uses level_type=="SSL"/"BSL". The guard NEVER fired.
        #
        # LONG trail moving up: don't trail INTO an SSL cluster below price
        # (smart money will sweep it, whipping price through your trailed SL).
        # SHORT trail moving down: don't trail INTO a BSL cluster above price.
        if ict_engine is not None:
            try:
                liq_buf = 0.50 * atr
                for pool in ict_engine.liquidity_pools:
                    if pool.swept:
                        continue
                    if pos_side == "long" and pool.level_type == "SSL":
                        # Don't trail above an SSL — it will get swept downward
                        ceiling = pool.price - liq_buf
                        if current_sl < ceiling < new_sl:
                            new_sl = ceiling
                            anchor += f"+LIQ_CEIL_SSL@${pool.price:.0f}"
                    elif pos_side == "short" and pool.level_type == "BSL":
                        # Don't trail below a BSL — it will get swept upward
                        floor = pool.price + liq_buf
                        if current_sl > floor > new_sl:
                            new_sl = floor
                            anchor += f"+LIQ_FLOOR_BSL@${pool.price:.0f}"
            except Exception:
                pass

        # ── MINIMUM DISTANCE ENFORCEMENT ─────────────────────────────────
        if pos_side == "long":
            new_sl = min(new_sl, price - min_dist)
            if new_sl <= current_sl:
                if hold_reason is not None:
                    hold_reason.append(
                        f"NO_IMPROVEMENT new={new_sl:.1f}<=cur={current_sl:.1f}")
                return None
        else:
            new_sl = max(new_sl, price + min_dist)
            if new_sl >= current_sl:
                if hold_reason is not None:
                    hold_reason.append(
                        f"NO_IMPROVEMENT new={new_sl:.1f}>=cur={current_sl:.1f}")
                return None

        # ── MINIMUM MEANINGFUL MOVE ───────────────────────────────────────
        _min_meaningful = 0.08 * atr
        if pos_side == "long":
            if new_sl < current_sl + _min_meaningful:
                if hold_reason is not None:
                    hold_reason.append(
                        f"MIN_MOVE delta={new_sl - current_sl:.1f}<{_min_meaningful:.1f}")
                return None
        else:
            if new_sl > current_sl - _min_meaningful:
                if hold_reason is not None:
                    hold_reason.append(
                        f"MIN_MOVE delta={current_sl - new_sl:.1f}<{_min_meaningful:.1f}")
                return None

        # ── STRUCTURAL PATH CHECK ─────────────────────────────────────────
        # Overrides: BOS in trade direction OR tier >= 1.0R
        _path_override = False
        if tier >= 1.0:
            _path_override = True
        elif ict_engine is not None:
            try:
                tf_st = ict_engine._tf.get("5m")
                if tf_st is not None:
                    if (pos_side == "long" and tf_st.bos_direction == "bullish"
                            and tf_st.bos_level > current_sl):
                        _path_override = True
                    elif (pos_side == "short" and tf_st.bos_direction == "bearish"
                            and tf_st.bos_level < current_sl):
                        _path_override = True
            except Exception:
                pass

        if ict_engine is not None and not _path_override:
            try:
                blocked, reason = ict_engine.check_sl_path_for_structure(
                    pos_side, current_sl, new_sl, now_ms, tier=tier)
                if blocked:
                    if hold_reason is not None:
                        hold_reason.append(f"PATH_BLOCKED:{reason}")
                    return None
            except Exception:
                pass

        logger.debug(
            f"Trail {pos_side.upper()}: ${current_sl:,.1f} → ${new_sl:,.1f} "
            f"[{anchor}] AMD={amd_phase} P{phase} tier={tier:.2f}R")
        return _round_tick(new_sl)

    @staticmethod
    def _classify_pullback(pos_side: str, price: float, entry_price: float,
                            atr: float, candles_1m: List[Dict],
                            candles_5m: List[Dict], orderbook: Dict,
                            peak_price_abs: float) -> Tuple[bool, int, str]:
        """
        6-signal pullback vs reversal classifier.
        Returns (is_pullback, reversal_signal_count, detail).
        """
        rev_sigs = 0
        details  = []

        if atr < 1e-10 or len(candles_1m) < 10:
            return True, 0, "insufficient_data"

        retrace = abs(peak_price_abs - price) if peak_price_abs > 1e-10 else 0.0

        # Signal 1: Volume expansion during pullback = more participants joining = reversal risk
        # vr = recent_vol / prior_vol. True expansion requires vr > 1.20 (20%+ above prior).
        # Old threshold was 0.60 — a 40% volume DECLINE — which fired almost always and
        # produced false reversal signals, making the trail freeze instead of advance.
        if len(candles_1m) >= 10:
            rv = sum(float(c['v']) for c in candles_1m[-3:]) / 3.0
            iv = sum(float(c['v']) for c in candles_1m[-8:-3]) / 5.0
            if iv > 1e-10:
                vr = rv / iv
                if vr > 1.20:
                    rev_sigs += 1; details.append(f"vol_expand({vr:.2f})")
                else:
                    details.append(f"vol_decline({vr:.2f})")

        # Signal 2: Retrace depth
        if retrace > 0.80 * atr:
            rev_sigs += 1; details.append(f"deep({retrace/atr:.1f}ATR)")
        else:
            details.append(f"shallow({retrace/atr:.1f}ATR)")

        # Signal 3: Large opposing candle bodies
        if len(candles_1m) >= 8:
            imp_b = [abs(float(c['c']) - float(c['o'])) for c in candles_1m[-8:-3]]
            ret_b = [abs(float(c['c']) - float(c['o'])) for c in candles_1m[-3:]]
            ai = sum(imp_b) / max(len(imp_b), 1)
            ar = sum(ret_b) / max(len(ret_b), 1)
            if ai > 1e-10 and ar / ai > 0.80:
                rev_sigs += 1; details.append("large_bodies")

        # Signal 4: Orderbook imbalance flip
        bids = orderbook.get("bids", [])
        asks = orderbook.get("asks", [])
        if bids and asks:
            def _qty(lvl):
                if isinstance(lvl, (list, tuple)) and len(lvl) >= 2: return float(lvl[1])
                if isinstance(lvl, dict): return float(lvl.get("size") or 0)
                return 0.0
            bd = sum(_qty(b) for b in bids[:5]) if len(bids) >= 5 else 0
            ad = sum(_qty(a) for a in asks[:5]) if len(asks) >= 5 else 0
            tot = bd + ad
            if tot > 1e-10:
                imb = (bd - ad) / tot
                if pos_side == "long" and imb < -0.15:
                    rev_sigs += 1; details.append(f"ob_flip({imb:+.2f})")
                elif pos_side == "short" and imb > 0.15:
                    rev_sigs += 1; details.append(f"ob_flip({imb:+.2f})")

        # Signal 5: 5m swing break
        if len(candles_5m) >= 5:
            highs_5m, lows_5m = _find_swings(candles_5m[:-1],
                                              min(8, len(candles_5m) - 2))
            if pos_side == "long" and lows_5m:
                rel = [l for l in lows_5m if l > entry_price - 0.5 * atr]
                if rel and price < min(rel):
                    rev_sigs += 1; details.append("5m_sw_broken")
            elif pos_side == "short" and highs_5m:
                rel = [h for h in highs_5m if h < entry_price + 0.5 * atr]
                if rel and price > max(rel):
                    rev_sigs += 1; details.append("5m_sw_broken")

        # Signal 6: Momentum stalling
        if len(candles_1m) >= 6:
            last5 = candles_1m[-5:]
            if pos_side == "long":
                if all(float(last5[i]['h']) <= float(last5[i-1]['h'])
                       for i in range(1, len(last5))):
                    rev_sigs += 1; details.append("momentum_stall")
            else:
                if all(float(last5[i]['l']) >= float(last5[i-1]['l'])
                       for i in range(1, len(last5))):
                    rev_sigs += 1; details.append("momentum_stall")

        return rev_sigs < 3, rev_sigs, "|".join(details)


# ═══════════════════════════════════════════════════════════════════════════
# ICT ENTRY GATE
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class QuantHelperSignals:
    """
    Quant engine signals used as HELPERS for ICT entry confirmation.

    In v5.0, the quant engine is a scout — it provides order-flow context
    that ICT structure uses to time execution within an institutional setup.

    Unlike the primary ICT gates (AMD phase, sweep geometry, OTE zone), these
    signals are soft filters — they can veto execution but not define the setup.

    tick_flow:   [-1, +1]  Live order flow bias (tick imbalance last 30s)
    cvd_trend:   [-1, +1]  Cumulative volume delta directional bias
    vwap_dev:    float      Price deviation from VWAP in ATR units (+= above)
    n_confirming: int       Number of quant signals agreeing with direction
    composite:   float      Weighted quant composite [-1, +1]
    regime_ok:   bool       ATR percentile in tradeable range (5%-97%)
    htf_veto:    bool       HTF trend strongly opposing the trade side
    adx:         float      Wilder ADX(14) value
    overextended: bool      Price meaningfully deviated from VWAP
    """
    tick_flow:    float = 0.0
    cvd_trend:    float = 0.0
    vwap_dev:     float = 0.0
    n_confirming: int   = 0
    composite:    float = 0.0
    regime_ok:    bool  = True
    htf_veto:     bool  = False
    adx:          float = 0.0
    overextended: bool  = False
    # v8.0: raw HTF structure scores — required for tier-aware veto in ICTEntryGate
    # These are the underlying [-1, +1] ICT swing-structure scores that produce the
    # htf_veto bool.  Exposing them here lets ICTEntryGate make tier-specific
    # counter-HTF decisions rather than treating the veto as a binary hard-block.
    htf_15m:      float = 0.0   # 15m ICT swing-structure score [-1 bearish → +1 bullish]
    htf_4h:       float = 0.0   # 4H ICT swing-structure score  [-1 bearish → +1 bullish]

    def flow_opposes(self, side: str) -> bool:
        """True if live order flow is STRONGLY opposing the trade direction."""
        if side == "long":
            return self.tick_flow < -0.30
        return self.tick_flow > 0.30

    def cvd_opposes(self, side: str) -> bool:
        """True if CVD trend is clearly opposing trade direction."""
        if side == "long":
            return self.cvd_trend < -0.35
        return self.cvd_trend > 0.35

    def quant_quality_score(self, side: str) -> float:
        """
        0–1 score: how strongly does quant confirm this ICT entry side?
        Used for logging and partial weight in entry Telegram message.
        Not used as an entry gate — ICT structure takes precedence.
        """
        direction = 1.0 if side == "long" else -1.0
        s = 0.0
        # Tick flow (most real-time)
        s += max(0.0, self.tick_flow * direction) * 0.30
        # CVD directional agreement
        s += max(0.0, self.cvd_trend * direction) * 0.25
        # Composite directional alignment
        s += max(0.0, self.composite * direction) * 0.25
        # Number of confirming signals
        s += min(self.n_confirming / 5.0, 1.0) * 0.20
        return min(s, 1.0)


# ═══════════════════════════════════════════════════════════════════════════
# ICT ENTRY GATE
# ═══════════════════════════════════════════════════════════════════════════

class ICTEntryGate:
    """
    Tier-based ICT entry quality classifier — Pure Structure + Order Flow.

    ARCHITECTURE: ICT is the COMMANDER, Order Flow is the SCOUT.
    ─────────────────────────────────────────────────────────────────────
    DIRECTION is determined exclusively by:
      • AMD cycle phase + delivery bias  (where smart money is delivering)
      • ICT OB/FVG orientation           (where orders were placed)
      • Liquidity sweep side             (what pool was raided and why)
      • 4H Premium/Discount zone         (where price is in the range)

    HTF EMA slope (15m/4h trend) is NOT a gate. Rationale:
      Mean-reversion entries fire against the short-term directional move
      by design — that IS the setup. The 15m slope is a lagging derivative
      of the same price action that AMD phase and OB direction already encode
      at higher precision. Blocking reversion longs when 15m is bearish is
      structurally contradictory: the 15m is bearish BECAUSE price moved down,
      which is exactly when VWAP long setups form. ICT structure (OBs, sweeps,
      delivery corridors) tells us WHERE smart money is positioned; HTF slope
      tells us nothing about that.

    Live order flow gates (RETAINED — these are real-time, not lagging):
      • Tick flow: not STRONGLY opposing (< −0.30 for long). Sustained
        extreme flow means the move is still in progress — wait for exhaustion.
      • CVD: directional agreement check for Tier-A delivery context.
      • ATR regime: extreme percentile (top 3%) = circuit breaker, not filter.

    TIER-S: ICT Sweep-and-Go in OTE zone (Institutional sweep entry)
      Requirements:
        • AMD phase MANIPULATION or DISTRIBUTION, confidence ≥ 0.62
        • Active sweep setup with status == OTE_READY (price in 61.8–78.6% retrace)
        • ICT confluence ≥ variable (0.0 if quality sweep, up to 0.60)
        • P/D zone: MANIPULATION waived (sweep defines zone); DISTRIBUTION aligned
        • Tick flow not STRONGLY opposing
        • Regime OK
      → Confirm ticks: 1 (kill zone) or 2 (off-session)
      → Expected edge: 58–66% WR, 1:2.5–4.0 R:R

    TIER-A: ICT Structural Alignment (delivery context without OTE)
      Requirements:
        • AMD delivery context: DISTRIBUTION/REACCUMULATION/REDISTRIBUTION ≥ 0.50
          OR MANIPULATION + confirmed sweep (delivery imminent)
        • ICT confluence ≥ 0.55 (0.50 if sweep-backed)
        • Quant composite in ICT direction ≥ 0.20 (light order-flow confirmation)
        • Tick flow not STRONGLY opposing (−0.30 / +0.30)
        • P/D zone aligned or AMD delivery corridor (delivery crosses zones by design)
        • Regime OK
      → Confirm ticks: 2
      → Expected edge: 52–58% WR, 1:2.0–3.5 R:R

    TIER-B: Standard Quant + ICT Confluence (order-flow primary)
      Requirements:
        • ICT confluence ≥ regime-adaptive floor:
            RANGING      → 0.12  (structure alignment, no OB/sweep needed)
            TRANSITIONING → 0.20  (partial structure required)
            TRENDING_*   → 0.35  (full conviction — trend entries not here anyway)
        • Order-flow composite ≥ 0.30 (VWAP + CVD + OB + tick + VEX)
        • n_confirming ≥ 3/5 (majority of order-flow signals agree)
        • VWAP overextended (price meaningfully displaced from equilibrium)
        • Tick flow not STRONGLY opposing
        • P/D zone: long not in premium, short not in discount
        • Regime OK
      → Confirm ticks: 3 (default)
      → Expected edge: 48–54% WR, 1:1.8–2.5 R:R

    HARD BLOCKED (no trade, any tier):
      • AMD ACCUMULATION conf ≥ 0.75 — tight consolidation, no delivery context
      • AMD MANIPULATION without matching sweep — Judas swing still active
      • AMD DISTRIBUTION opposing at conf ≥ 0.65 — wrong side of delivery
      • ATR regime extreme (top/bottom 3%) — microstructure unreliable
    """

    # Bug-5 fix: single source of truth for the Tier-B composite threshold.
    # The display code in _log_thinking used QCfg.COMPOSITE_ENTRY_MIN() (0.350)
    # while the actual gate check here used the hardcoded literal 0.30.
    # Readers saw "Composite (-0.259 vs ±0.350)" in the log but the real block
    # fired at 0.30 — the displayed threshold was wrong.  Now both places use
    # this constant so the log always reflects the real gate value.
    TIER_B_COMPOSITE_MIN: float = 0.30

    @staticmethod
    def _htf_allows_tier_a(
            htf_veto: bool,
            ict_total: float,
            amd_phase: str,
            amd_conf: float,
            htf_15m: float,
            side: str,
            sweep_setup: Optional['ICTSweepSetup'] = None,
    ) -> bool:
        """
        DEPRECATED — HTF is informational only; this method always returns True.

        Direction is encoded by: AMD phase + delivery context, ICT OB/FVG
        orientation, liquidity sweep side, and P/D zone (4H premium/discount).
        These are all structurally superior to an EMA slope on a 15m chart.

        Retained for backward compatibility with any external callers.
        Will be removed in a future refactor.
        """
        return True  # HTF no longer a gate — see ICTEntryGate.evaluate() docs

    @staticmethod
    def evaluate(
            side: str,
            sig,                                    # SignalBreakdown
            sweep_setup: Optional['ICTSweepSetup'],
            price: float,
            quant: Optional['QuantHelperSignals'] = None,
            mode: str = "reversion",
            market_regime: str = "RANGING",
    ) -> Tuple[str, int, str]:
        """
        Returns (tier: "S"|"A"|"B"|"BLOCKED", confirm_ticks: int, reason: str).

        v6.0 UPGRADES:
          1. mode="momentum" bypasses AMD ACCUMULATION gate and P/D gate
          2. market_regime="TRENDING_*" waives P/D gate (trends live in premium)
          3. Relaxed AMD ACCUMULATION blocking (only at conf >= 0.85, was 0.75)

        quant: QuantHelperSignals — order-flow helpers. If None, soft-veto
               checks based on quant are skipped (permissive fallback).
        """
        amd_phase = getattr(sig, 'amd_phase', 'ACCUMULATION')
        amd_conf  = getattr(sig, 'amd_conf',  0.0)
        amd_bias  = getattr(sig, 'amd_bias',  'neutral')
        ict_total = getattr(sig, 'ict_total',  0.0)
        in_disc   = getattr(sig, 'in_discount', False)
        in_prem   = getattr(sig, 'in_premium',  False)
        mtf_align = getattr(sig, 'mtf_aligned', False)

        # htf_veto: tracked for diagnostics and analytics — NOT a gate.
        # Direction authority belongs to ICT structure (AMD phase, OB direction,
        # sweep bias) and live order flow. HTF EMA/structure slope is a lagging
        # derivative of the same price action ICT already encodes more precisely.
        # Mean-reversion entries BY DEFINITION fire against short-term HTF — blocking
        # them on HTF opposition is architecturally contradictory for this strategy.
        # Informational label shown in BLOCKED reasons and entry notifications.
        tf_opposes  = quant.flow_opposes(side)  if quant is not None else False
        cvd_opposes = quant.cvd_opposes(side)   if quant is not None else False
        regime_ok   = quant.regime_ok           if quant is not None else True
        htf_veto    = quant.htf_veto            if quant is not None else False  # informational only
        q_overext   = quant.overextended        if quant is not None else True
        n_conf      = quant.n_confirming        if quant is not None else 0
        q_composite = quant.composite           if quant is not None else 0.0
        q_score     = quant.quant_quality_score(side) if quant is not None else 0.5
        htf_15m     = quant.htf_15m             if quant is not None else 0.0
        _adx        = quant.adx                 if quant is not None else 25.0

        # ── HARD BLOCKED conditions (no trade regardless of tier) ─────────

        # No trade when ATR percentile is extreme
        if quant is not None and not regime_ok:
            return "BLOCKED", 0, "REGIME_INVALID(ATR_extreme)"

        # AMD Accumulation = no sweep = no delivery — waiting for setup.
        #
        # v6.0 FIX: ACCUMULATION hard-blocks only at VERY HIGH confidence (≥0.85),
        # AND is BYPASSED entirely for momentum entries (breakout retest).
        # Momentum entries have their own validation (breakout detection + retest
        # confirmation + bounce) — AMD delivery context is irrelevant.
        #
        # Previous threshold of 0.75 blocked too aggressively in trending markets
        # where price grinds without sweeping. In a trend, AMD decays to
        # ACCUMULATION simply because no sweep occurred — not because the market
        # is actually accumulating.
        _is_momentum = (mode == "momentum")
        _is_trending = market_regime in ("TRENDING_UP", "TRENDING_DOWN")
        if (amd_phase == "ACCUMULATION" and amd_conf >= 0.85
                and not _is_momentum):
            return "BLOCKED", 0, f"AMD_ACCUM(conf={amd_conf:.2f})_no_delivery"

        # AMD Manipulation without a sweep setup = Judas swing still active
        # Price is making the fake move, not the real move.
        # FIX Bug-MANIP: also block when a sweep setup EXISTS but it is for the
        # OPPOSITE direction — e.g. evaluating SHORT while a LONG sweep just fired.
        # In MANIPULATION phase, smart money just swept SSL (bullish) or BSL
        # (bearish); trading the counter-direction is trading the Judas swing.
        if amd_phase == "MANIPULATION" and (
                sweep_setup is None or sweep_setup.side != side):
            return "BLOCKED", 0, "MANIP_no_confirmed_sweep"

        # AMD Distribution delivering AGAINST our trade direction = wrong side
        if (amd_phase == "DISTRIBUTION" and amd_conf >= 0.65 and
                ((side == "long"  and amd_bias == "bearish") or
                 (side == "short" and amd_bias == "bullish"))):
            return "BLOCKED", 0, f"AMD_DIST_opposing_{amd_bias}(conf={amd_conf:.2f})"

        # HTF VETO REMOVED — direction authority delegated to ICT structure + order flow.
        # HTF EMA/structure slope is a lagging derivative of the same price action that
        # ICT already encodes more precisely (OB direction, AMD phase, sweep bias).
        # Mean-reversion entries fire AGAINST the short-term HTF trend by design;
        # blocking them on HTF opposition is structurally contradictory for this strategy.
        # htf_veto remains tracked and shown in BLOCKED diagnostics (informational only).

        # ── TIER-S: ICT Sweep-and-Go in OTE zone ─────────────────────────
        # v5.1 FIX: The sweep IS the institutional structure.
        #
        # The ICT confluence score measures OB/FVG/structure alignment.
        # But during MANIPULATION, the sweep CREATES new OBs and FVGs —
        # they appear on the NEXT candle close, not the current one.
        # Requiring high ICT total during the sweep event is requiring
        # evidence that the sweep itself is about to create.
        #
        # Live bug: quality=0.63 sweep at OTE, AMD=MANIPULATION 0.94,
        #   but ICT=0.35 (0 OBs detected). The sweep was blocked because
        #   ICT score hadn't caught up to the sweep event.
        #
        # Institutional reality: sweep at OTE + AMD confirmation IS the
        # setup. The sweep candle defines SL, OTE is the entry zone,
        # AMD provides the delivery target. That's the complete trade.
        #
        # Fix: for confirmed OTE sweeps with quality >= 0.50 and AMD
        # conf >= 0.70, ICT total is not gated — the sweep is the signal.
        # For lower quality/confidence, ICT provides a soft floor.
        _sweep_ote = (
            sweep_setup is not None and
            sweep_setup.status == "OTE_READY" and
            sweep_setup.side == side
        )
        # v5.2: _sweep_active includes DETECTED status — the sweep exists
        # and side matches. Used for delivery context and MANIPULATION unlock.
        # OTE_READY is stricter (price IN the zone) — used for Tier-S entry.
        _sweep_active = (
            sweep_setup is not None and
            sweep_setup.status in ("DETECTED", "OTE_READY", "ACTIVE") and
            sweep_setup.side == side
        )
        _ict_bar_s = 0.60  # default (no sweep)
        if _sweep_ote:
            _sq = sweep_setup.quality_score()
            if _sq >= 0.50 and amd_conf >= 0.70:
                _ict_bar_s = 0.0   # sweep IS the structure — no ICT gate
            elif _sq >= 0.50:
                _ict_bar_s = 0.35  # minimal: ICT engine running
            else:
                _ict_bar_s = 0.50  # weak sweep: need some ICT backup
        _tier_s_conditions = (
            _sweep_ote and
            amd_phase in ("MANIPULATION", "DISTRIBUTION") and
            amd_conf >= 0.62 and
            ict_total >= _ict_bar_s and
            # P/D gate: waived during MANIPULATION (sweep just happened,
            # price is at OTE by definition — P/D zone is irrelevant)
            (
                amd_phase == "MANIPULATION"  # P/D waived: sweep defines zone
                or
                (amd_phase == "DISTRIBUTION" and (
                    (side == "short" and amd_bias == "bearish") or
                    (side == "long"  and amd_bias == "bullish")))
                or
                (side == "long"  and (in_disc or not in_prem))
                or
                (side == "short" and (in_prem or not in_disc))
            ) and
            not tf_opposes  # tick flow soft veto
        )
        if _tier_s_conditions:
            quality = sweep_setup.quality_score()
            # Kill zone = 1 tick, off-session = 2 ticks
            cn = 1 if sweep_setup.kill_zone in ("london", "ny", "asia") else 2
            return ("S", cn,
                    f"TIER-S OTE quality={quality:.2f} q={q_score:.2f} "
                    f"AMD={amd_phase}(conf={amd_conf:.2f}) "
                    f"ICT={ict_total:.2f} KZ={sweep_setup.kill_zone or 'none'}")

        # ── TIER-A: ICT Structural Alignment ─────────────────────────────
        # Bug-1 fix: DISTRIBUTION that is actively opposing the entry side must
        # NOT count as a delivery context.  Previously conf 0.50–0.65 opposing
        # DISTRIBUTION would pass _has_delivery_context=True and could reach
        # Tier-A even though AMD is delivering AGAINST the entry direction.
        # The hard-block at line 1762 only fires at conf>=0.65; this gap let
        # entries through at 0.50–0.65.  Now _has_delivery_context is False
        # for any opposing DISTRIBUTION regardless of confidence.
        _amd_opposes = (
            amd_phase == "DISTRIBUTION" and (
                (side == "long"  and amd_bias == "bearish") or
                (side == "short" and amd_bias == "bullish")
            )
        )
        _has_delivery_context = (
            amd_phase in ("DISTRIBUTION", "REACCUMULATION", "REDISTRIBUTION")
            and amd_conf >= 0.50
            and not _amd_opposes   # Bug-1: opposing DISTRIBUTION excluded
        ) or (
            # v5.1: MANIPULATION + confirmed sweep = delivery imminent.
            # In the ICT model, MANIPULATION IS the setup phase. Smart money
            # sweeps liquidity (the manipulation), then delivers in the
            # opposite direction. When we have a confirmed sweep at OTE
            # during MANIPULATION, the delivery is about to start.
            amd_phase == "MANIPULATION"
            and _sweep_active  # DETECTED or OTE_READY — sweep exists
            and amd_conf >= 0.70
        )
        # _tier_a_htf_ok / _htf_allows_tier_a removed — HTF is informational only.
        # Tier-A direction is encoded by _has_delivery_context (AMD phase + sweep),
        # P/D zone alignment, ICT confluence score, and live tick flow.
        # HTF VETO REMOVED — use a single tick flow threshold for all Tier-A entries.
        # The counter-HTF softening (−0.60) vs standard (−0.30) is no longer needed
        # because we are not distinguishing with-HTF vs counter-HTF paths.
        # Keep the single standard threshold (−0.30 / +0.30): only sustained extreme
        # opposing flow should block — momentary tick noise must not gate ICT setups.
        _tf_opposes_tier_a = (
            quant is not None and (
                (side == "long"  and quant.tick_flow < -0.30) or
                (side == "short" and quant.tick_flow >  0.30)
            )
        )
        _tier_a_conditions = (
            _has_delivery_context and
            # v5.1: sweep-backed entries use lower ICT bar (0.50).
            ict_total >= (0.50 if _sweep_active else 0.55) and
            (
                (amd_phase == "DISTRIBUTION" and (
                    (side == "short" and amd_bias == "bearish") or
                    (side == "long"  and amd_bias == "bullish")
                ))  # AMD delivery: P/D gate waived — delivery crosses zones by design
                or
                _is_trending   # v6.0: trends live in premium/discount — P/D waived
                or
                (side == "long"  and not in_prem)   # standard P/D gate for non-delivery
                or
                (side == "short" and not in_disc)
            ) and
            not _tf_opposes_tier_a and
            # HTF gate removed — ICT structure (AMD phase, OB, sweep) encodes direction.
            # _tier_a_htf_ok no longer required.
            # v5.1: sweep-backed entries need less composite confirmation.
            # The sweep is the signal — composite is soft confirmation, not a gate.
            # Standard: composite * direction >= 0.20
            # Sweep-backed: composite * direction >= 0.05 (just not STRONGLY opposing)
            (q_composite * (1 if side == "long" else -1)) >= (0.05 if _sweep_active else 0.20)
        )
        if _tier_a_conditions:
            # Label no longer distinguishes counter-HTF — all entries are structure-primary
            return ("A", 2,
                    f"TIER-A structural AMD={amd_phase}(conf={amd_conf:.2f}) "
                    f"ICT={ict_total:.2f} q={q_score:.2f} "
                    f"15m={htf_15m:+.2f}(info) "
                    f"mtf={'✓' if mtf_align else '✗'}")

        # ── TIER-B: Standard Quant + ICT Confluence ───────────────────────
        # Quant gates are REQUIRED here — ICT just provides structural context.
        # P/D gate added: Tier-B entries require correct zone same as Tier-A.
        # LONG must NOT be in premium (4H PD > 60%); SHORT must NOT be in discount.
        #
        # ADX-ADAPTIVE ICT FLOOR (root-cause fix for 0-trades-per-session):
        # In ranging markets (ADX < 25) outside ICT kill zones, the maximum
        # achievable ICT total is ≈ 0.15–0.17 (structure alignment only — no
        # active OB at price, no recent sweep, no kill zone session credit).
        # A hardcoded 0.35 floor is mathematically unreachable in these conditions,
        # causing permanent BLOCKED status regardless of quant signal quality.
        #
        # IMPORTANT: Use sig.market_regime (RegimeClassifier output), NOT raw ADX.
        # The RegimeClassifier can show RANGING even at ADX=31 because it weights
        # HTF alignment and ATR expansion — raw ADX alone is insufficient.
        # Using raw ADX=31 → floor=0.35 even when the bot classifies the market as
        # RANGING, meaning the fix has no effect. sig.market_regime is the ground truth.
        #
        # Regime-adaptive floors:
        #   RANGING      → 0.12 — structure alignment is sufficient (max ICT ≈ 0.15)
        #   TRANSITIONING → 0.20 — partial alignment required (max ICT ≈ 0.22)
        #   TRENDING_*   → 0.35 — full structural conviction required
        #
        # Quant gates (composite ≥ 0.30, n_conf ≥ 3, overextended) remain
        # unchanged — they provide the primary edge in ranging conditions.
        _market_regime = getattr(sig, 'market_regime', 'RANGING')
        if _market_regime == 'RANGING':
            _ict_b_floor = 0.12
        elif _market_regime == 'TRANSITIONING':
            _ict_b_floor = 0.20
        else:
            # TRENDING_UP, TRENDING_DOWN — full structural requirement
            _ict_b_floor = 0.35

        # v6.0: Momentum entries use a lower ICT floor — the breakout IS the
        # institutional structure. When a breakout fires + retest confirms,
        # requiring high ICT total is requiring evidence the breakout created.
        if _is_momentum:
            _ict_b_floor = min(_ict_b_floor, 0.08)

        # v6.0: P/D gate waiver for momentum and trending markets.
        # In a bull trend, price LIVES in premium — pullbacks to buy happen
        # in premium by definition. The P/D gate is valuable in ranging markets
        # where it prevents chasing. In trending markets, it prevents trading.
        _pd_gate_active = not (_is_momentum or _is_trending)

        _tier_b_conditions = (
            ict_total >= _ict_b_floor and    # ADX-adaptive — see comment above
            abs(q_composite) >= ICTEntryGate.TIER_B_COMPOSITE_MIN and
            (q_overext or _is_momentum) and  # momentum doesn't need VWAP overextension
            (n_conf >= 3 or (_is_momentum and n_conf >= 2)) and  # relaxed for momentum
            # HTF veto removed — direction determined by ICT + order flow
            not tf_opposes and
            # P/D zone gate: waived for momentum + trending markets
            (not _pd_gate_active or (
                (side == "long"  and not in_prem) or
                (side == "short" and not in_disc)))
        )
        if _tier_b_conditions:
            return ("B", 3 if not _is_momentum else 2,
                    f"TIER-B {'momentum' if _is_momentum else 'std'} ICT={ict_total:.2f} "
                    f"Σ={q_composite:+.3f} overext={q_overext} n={n_conf}/5")

        # ── Specific BLOCKED reason for diagnostics ───────────────────────
        block_reasons = []
        if not _has_delivery_context and not _tier_s_conditions:
            if _amd_opposes:
                # Bug-1 / Bug-5: surface the specific opposing-DISTRIBUTION reason
                # so logs clearly show WHY delivery context was denied, not just
                # that the AMD phase/conf was insufficient.
                block_reasons.append(
                    f"AMD_DIST_opposing_{amd_bias}(conf={amd_conf:.2f})"
                    "_no_delivery_context")
            else:
                block_reasons.append(f"AMD={amd_phase}(conf={amd_conf:.2f})")
        if ict_total < _ict_b_floor:
            block_reasons.append(f"ICT={ict_total:.2f}<{_ict_b_floor:.2f}"
                                  f"(regime={_market_regime})")
        if _tf_opposes_tier_a:
            _tf_val = quant.tick_flow if quant else 0.0
            block_reasons.append(f"TICK_OPPOSING({_tf_val:.2f},thr=0.30)")
        # P/D zone mismatch (only reported when not in AMD delivery — delivery is exempt)
        _in_amd_delivery = (
            amd_phase == "DISTRIBUTION" and (
                (side == "long"  and amd_bias == "bullish") or
                (side == "short" and amd_bias == "bearish")
            )
        )
        if _pd_gate_active and (side == "long"  and in_prem  and not _in_amd_delivery):
            block_reasons.append("LONG_IN_PREMIUM")
        if _pd_gate_active and (side == "short" and in_disc  and not _in_amd_delivery):
            block_reasons.append("SHORT_IN_DISCOUNT")
        # HTF veto informational only — not a BLOCKED reason
        if htf_veto:
            block_reasons.append(f"HTF={htf_15m:+.2f}(info-only,not-blocking)")
        if not q_overext and ict_total < 0.55:
            block_reasons.append("NOT_OVEREXTENDED+weak_ICT")
        if abs(q_composite) < ICTEntryGate.TIER_B_COMPOSITE_MIN:
            block_reasons.append(
                f"Σ={q_composite:+.3f}<{ICTEntryGate.TIER_B_COMPOSITE_MIN:.2f}(TierB_min)")

        return ("BLOCKED", 0, "BELOW_MIN: " + " | ".join(block_reasons) if block_reasons
                else f"BELOW_MIN ICT={ict_total:.2f} Σ={q_composite:+.3f} floor={_ict_b_floor:.2f}")


# ═══════════════════════════════════════════════════════════════════════════
# UTILITIES
# ═══════════════════════════════════════════════════════════════════════════

def _find_swings(candles: List[Dict],
                 lookback: int = 12) -> Tuple[List[float], List[float]]:
    """Return (swing_highs, swing_lows) from candle data."""
    if len(candles) < 3:
        return [], []
    recent = candles[-lookback:] if len(candles) >= lookback else candles
    highs, lows = [], []
    for i in range(1, len(recent) - 1):
        h = float(recent[i]['h'])
        l = float(recent[i]['l'])
        if h > float(recent[i-1]['h']) and h > float(recent[i+1]['h']):
            highs.append(h)
        if l < float(recent[i-1]['l']) and l < float(recent[i+1]['l']):
            lows.append(l)
    return highs, lows


def _round_tick(price: float, tick_size: float = 0.1) -> float:
    """Round price to nearest tick."""
    if tick_size <= 0:
        return price
    return round(round(price / tick_size) * tick_size, 10)

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

        # Check if current setup is still valid
        if self._active_setup is not None:
            if self._active_setup.status == "EXPIRED":
                self._active_setup = None
            elif (now_ms - self._active_setup.setup_time_ms >
                  ict_engine.AMD_DISTRIB_WINDOW_MS):
                self._active_setup = None
                logger.debug("ICTSweepDetector: setup expired (>90min)")

        # Find the best new setup
        setup = self._find_best_setup(ict_engine, price, atr, now_ms,
                                       candles_5m or [], candles_15m or [])
        if setup is not None:
            self._active_setup = setup
            logger.info(
                f"🎯 ICT SWEEP SETUP DETECTED: {setup.side.upper()} | "
                f"sweep=${setup.sweep_price:.0f} | "
                f"OTE=[${setup.ote_entry_zone_low:.0f}–${setup.ote_entry_zone_high:.0f}] | "
                f"SL=${setup.sl_sweep_candle:.0f} | "
                f"delivery=${setup.delivery_target:.0f if setup.delivery_target else 'N/A'} | "
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

        # Compute OTE zone (61.8%–78.6% retracement of displacement impulse)
        if side == "long":
            move_from = sweep_candle_extreme   # absolute low of sweep wick
            move_to   = disp_close             # close of displacement (impulse top)
            move_size = move_to - move_from
            if move_size < 0.5 * atr:          # too small to be institutional
                return None
            ote_high = move_to - self.OTE_LOWER_FIB * move_size   # 61.8% retrace
            ote_low  = move_to - self.OTE_UPPER_FIB * move_size   # 78.6% retrace
            sl_sweep = sweep_candle_extreme - 0.08 * atr
            sl_ob    = disp_low - 0.15 * atr
        else:
            move_from = sweep_candle_extreme   # absolute high of sweep wick
            move_to   = disp_close             # close of displacement (impulse bottom)
            move_size = move_from - move_to
            if move_size < 0.5 * atr:
                return None
            ote_low  = move_to + self.OTE_LOWER_FIB * move_size   # 61.8% retrace up
            ote_high = move_to + self.OTE_UPPER_FIB * move_size   # 78.6% retrace up
            sl_sweep = sweep_candle_extreme + 0.08 * atr
            sl_ob    = disp_high + 0.15 * atr

        # Ensure OTE zone is above current price for long (in path)
        if side == "long" and price > ote_high + 0.5 * atr:
            return None  # price already through OTE — entry missed
        if side == "short" and price < ote_low - 0.5 * atr:
            return None  # price already through OTE — entry missed

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

        For long (SSL sweep):
          - Scan back from current candle to find the candle that wicked below
            the SSL pool price — that is the sweep candle.
          - The NEXT candle (first candle that closes above the pool) is the
            displacement candle.
        """
        if not candles_5m or len(candles_5m) < 5:
            return None, None, None, None

        pool_price = sweep_pool.price
        sweep_ts   = sweep_pool.sweep_timestamp

        # Find the sweep candle by timestamp proximity
        # Candle timestamps are epoch ms of the candle open
        for i in range(len(candles_5m) - 1, 0, -1):
            c = candles_5m[i]
            c_ts = int(c.get('t', 0))
            if c_ts > sweep_ts:
                continue  # this candle is after the sweep time

            if side == "long":
                # Sweep candle: wicked below the SSL pool
                if float(c['l']) <= pool_price * 1.001:
                    sweep_extreme = float(c['l'])
                    # Displacement: next candle(s) — find first close above pool
                    for j in range(i + 1, min(i + 4, len(candles_5m))):
                        d = candles_5m[j]
                        if float(d['c']) > pool_price:
                            return (sweep_extreme,
                                    float(d['h']), float(d['l']), float(d['c']))
                    break
            else:
                # Sweep candle: wicked above the BSL pool
                if float(c['h']) >= pool_price * 0.999:
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
                ) -> Tuple[float, str]:
        """
        Returns (sl_price, source_label).
        """
        min_dist = max(price * 0.003, 0.40 * atr)
        max_dist = price * 0.035

        if mode in ("trend", "momentum"):
            max_dist = min(max_dist, 2.0 * atr)

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

        if mode in ("trend", "momentum"):
            min_rr = 2.5;  max_rr = 6.0
        else:
            min_rr = 1.8;  max_rr = 4.0

        min_tp_dist = sl_dist * min_rr
        max_tp_dist = sl_dist * max_rr

        if now_ms <= 0:
            now_ms = int(time.time() * 1000)

        scored: List[Tuple[float, float, str]] = []   # (price, score, label)

        def _add(tp_cand: float, score: float, label: str):
            dist = (tp_cand - price) if side == "long" else (price - tp_cand)
            if dist < min_tp_dist or dist > max_tp_dist:
                return
            if side == "long"  and tp_cand <= price:
                return
            if side == "short" and tp_cand >= price:
                return
            scored.append((tp_cand, score, label))

        # ── TIER-S: Sweep delivery target ─────────────────────────────────
        if sweep_setup is not None and sweep_setup.delivery_target is not None:
            _add(sweep_setup.delivery_target, 8.0,
                 f"AMD_DELIVERY {sweep_setup.delivery_target_label}")

        # AMD engine delivery target (may differ from sweep_setup)
        if ict_engine is not None:
            amd = ict_engine._amd
            if (amd.delivery_target is not None and
                    amd.confidence >= 0.55):
                _add(amd.delivery_target, 7.5,
                     f"AMD_target(conf={amd.confidence:.2f})")

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
        if tier < 0.50:
            if hold_reason is not None:
                hold_reason.append(f"PHASE0 tier={tier:.2f}R<0.50R")
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

        # ── AMD MANIPULATION: full freeze ────────────────────────────────
        if amd_phase == "MANIPULATION" and tier < 1.5:
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
        elif amd_phase == "MANIPULATION" and tier >= 1.5:
            # Exception: deep in profit during Judas swing — allow BE only
            phase = 1; min_dist = 1.5 * atr
        else:
            # ACCUMULATION or unknown — conservative
            if tier >= 1.5:
                phase = 1; min_dist = 2.0 * atr
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
                    # Check if price is testing the OB
                    if (ob.low - freeze_atr <= price <= ob.high + freeze_atr):
                        if hold_reason is not None:
                            hold_reason.append(
                                f"OB_ZONE_FREEZE@${ob.midpoint:.0f}"
                                f"(tf={ob.timeframe})")
                        return None
            except Exception:
                pass

        # ── FVG FREEZE (price inside active FVG, SL trailing near it) ─────
        if ict_engine is not None and be_locked and hold_seconds < 600.0:
            try:
                fvgs = (ict_engine.fvgs_bull if pos_side == "long"
                        else ict_engine.fvgs_bear)
                freeze_atr = 0.40 * atr
                for fvg in fvgs:
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
                                f"(fill={fvg.fill_percentage:.0%})")
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
        if phase >= 1 and ict_engine is not None:
            try:
                ob_buf = 0.35 * atr
                obs = (ict_engine.order_blocks_bull if pos_side == "long"
                       else ict_engine.order_blocks_bear)
                for ob in sorted([o for o in obs if o.is_active(now_ms)],
                                  key=lambda x: abs(x.midpoint - price)):
                    if pos_side == "long":
                        cand = ob.low - ob_buf
                        if current_sl < cand < price - min_dist:
                            candidates.append((cand,
                                               f"OB_ANCHOR@${ob.midpoint:.0f}"))
                            break
                    else:
                        cand = ob.high + ob_buf
                        if price + min_dist < cand < current_sl:
                            candidates.append((cand,
                                               f"OB_ANCHOR@${ob.midpoint:.0f}"))
                            break
            except Exception:
                pass

        # 3. 5m swing structure (Phase 2+, primary driver)
        if phase >= 2 and candles_5m and len(candles_5m) >= 6:
            closed_5m = candles_5m[:-1]
            highs_5m, lows_5m = _find_swings(
                closed_5m, min(12, len(closed_5m) - 2))
            sw_buf = max(0.12 * atr, 0.30 * atr)

            if pos_side == "long" and lows_5m:
                valid = [l for l in lows_5m
                         if current_sl + 0.05 * atr < l < price - min_dist]
                if valid:
                    best = max(valid)
                    candidates.append((best - sw_buf,
                                       f"5m_SWING_LOW@${best:.0f}"))

            elif pos_side == "short" and highs_5m:
                valid = [h for h in highs_5m
                         if price + min_dist < h < current_sl - 0.05 * atr]
                if valid:
                    best = min(valid)
                    candidates.append((best + sw_buf,
                                       f"5m_SWING_HIGH@${best:.0f}"))

        # 4. FVG fill trigger (immediate lock on 70%+ fill)
        if ict_engine is not None:
            try:
                fvgs_delivery = (ict_engine.fvgs_bull if pos_side == "long"
                                 else ict_engine.fvgs_bear)
                for fvg in fvgs_delivery:
                    if not fvg.is_active(now_ms):
                        continue
                    if fvg.fill_percentage < 0.70:
                        continue
                    # FVG mostly filled — lock just beyond its far edge
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
            micro_buf = max(0.08 * atr, 0.20 * atr)

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

        # ── LIQUIDITY POOL CEILING ────────────────────────────────────────
        if ict_engine is not None:
            try:
                liq_buf = 0.50 * atr
                for pool in ict_engine.liquidity_pools:
                    if pool.swept:
                        continue
                    if pos_side == "long" and pool.pool_type == "EQL":
                        ceiling = pool.price - liq_buf
                        if current_sl < ceiling < new_sl:
                            new_sl = ceiling
                            anchor += f"+LIQ_CEIL@${pool.price:.0f}"
                    elif pos_side == "short" and pool.pool_type == "EQH":
                        floor = pool.price + liq_buf
                        if current_sl > floor > new_sl:
                            new_sl = floor
                            anchor += f"+LIQ_FLOOR@${pool.price:.0f}"
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

        # ── STRUCTURAL PATH CHECK (don't cross fresh ICT zones) ──────────
        if ict_engine is not None:
            try:
                blocked, reason = ict_engine.check_sl_path_for_structure(
                    pos_side, current_sl, new_sl, now_ms)
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

        # Signal 1: Volume expansion
        if len(candles_1m) >= 10:
            rv = sum(float(c['v']) for c in candles_1m[-3:]) / 3.0
            iv = sum(float(c['v']) for c in candles_1m[-8:-3]) / 5.0
            if iv > 1e-10:
                vr = rv / iv
                if vr > 0.60:
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

class ICTEntryGate:
    """
    Tier-based ICT entry quality classifier.

    TIER-S: ICT Sweep-and-Go (highest conviction, best R:R)
      All of:
        • AMD phase MANIPULATION or DISTRIBUTION (sweep < 90min)
        • AMD confidence ≥ 0.65
        • Active sweep setup detected with OTE zone reached
        • Price in OTE zone (61.8%-78.6% retrace)
        • ICT confluence ≥ 0.60
        • Price in discount (long) or premium (short)
        • Kill zone preferred (2× weight for LN/NY)
        → Confirm ticks: 1 (institutional — don't wait)
        → Expected win rate: 60-68%

    TIER-A: ICT Structural + Order Flow
      All of:
        • AMD phase DISTRIBUTION or REACCUMULATION
        • AMD confidence ≥ 0.52
        • Displaced sweep in last 90min
        • ICT confluence ≥ 0.55
        • Price in discount/premium (threshold 0.45/0.55)
        • Quant composite ≥ 0.30
        • MTF aligned ≥ 2/4 TFs
        → Confirm ticks: 2
        → Expected win rate: 54-60%

    TIER-B: Standard Quant + ICT Confluence
      All of:
        • ICT confluence ≥ 0.45 (existing gate)
        • Quant composite ≥ 0.35
        • AMD not opposing (AMD veto)
        → Confirm ticks: 3 (existing)
        → Expected win rate: 48-54%

    BLOCKED:
      • AMD ACCUMULATION with confidence ≥ 0.60 (no sweep, no delivery)
      • AMD MANIPULATION without sweep setup (Judas swing still active)
      • Price in neutral zone (0.48-0.52 P/D) with weak ICT ≤ 0.40
      • HTF trend strongly opposing (ADX > 30, HTF against direction)
    """

    @staticmethod
    def evaluate(
            side: str,
            sig,                       # SignalBreakdown
            sweep_setup: Optional['ICTSweepSetup'],
            price: float,
    ) -> Tuple[str, int, str]:
        """
        Returns (tier: "S"|"A"|"B"|"BLOCKED", confirm_ticks: int, reason: str).
        """
        amd_phase = getattr(sig, 'amd_phase', 'ACCUMULATION')
        amd_conf  = getattr(sig, 'amd_conf', 0.0)
        amd_bias  = getattr(sig, 'amd_bias', 'neutral')
        ict_total = getattr(sig, 'ict_total', 0.0)
        in_disc   = getattr(sig, 'in_discount', False)
        in_prem   = getattr(sig, 'in_premium', False)
        mtf_align = getattr(sig, 'mtf_aligned', False)

        # ── BLOCKED conditions ──────────────────────────────────────────
        # No trade in ACCUMULATION with high confidence = no sweep = no reason
        if amd_phase == "ACCUMULATION" and amd_conf >= 0.60:
            return "BLOCKED", 0, f"AMD_ACCUM(conf={amd_conf:.2f})_no_sweep"

        # AMD MANIPULATION without a confirmed sweep setup = Judas swing active
        if amd_phase == "MANIPULATION" and sweep_setup is None:
            return "BLOCKED", 0, "MANIP_no_sweep_setup"

        # AMD delivering strongly AGAINST trade direction
        if amd_phase == "DISTRIBUTION" and amd_conf >= 0.65:
            amd_opposes = ((side == "long"  and amd_bias == "bearish") or
                           (side == "short" and amd_bias == "bullish"))
            if amd_opposes:
                return "BLOCKED", 0, f"AMD_DIST_opposing_{amd_bias}(conf={amd_conf:.2f})"

        # ── TIER-S: ICT Sweep-and-Go ────────────────────────────────────
        if (sweep_setup is not None and
                sweep_setup.status == "OTE_READY" and
                sweep_setup.side == side and
                amd_phase in ("MANIPULATION", "DISTRIBUTION") and
                amd_conf >= 0.62 and
                ict_total >= 0.60 and
                (side == "long" and (in_disc or not in_prem) or
                 side == "short" and (in_prem or not in_disc))):
            quality = sweep_setup.quality_score()
            cn = 1 if sweep_setup.kill_zone in ("london", "ny") else 2
            return ("S", cn,
                    f"TIER-S sweep+OTE quality={quality:.2f} "
                    f"AMD={amd_phase}(conf={amd_conf:.2f}) "
                    f"ICT={ict_total:.2f} KZ={sweep_setup.kill_zone or 'none'}")

        # ── TIER-A: ICT Structural + Order Flow ──────────────────────────
        # Has a swept pool in recent history (even if not in OTE)
        has_recent_sweep = (amd_phase in ("DISTRIBUTION", "REACCUMULATION",
                                           "REDISTRIBUTION") and
                             amd_conf >= 0.50)
        if (has_recent_sweep and
                ict_total >= 0.55 and
                sig.composite * (1 if side == "long" else -1) >= 0.30 and
                (side == "long"  and not in_prem or
                 side == "short" and not in_disc)):
            return ("A", 2,
                    f"TIER-A structural AMD={amd_phase}(conf={amd_conf:.2f}) "
                    f"ICT={ict_total:.2f} mtf={'✓' if mtf_align else '✗'}")

        # ── TIER-B: Standard quant + ICT confluence ──────────────────────
        ict_min  = 0.40  # slightly lower than previous 0.45 for TIER-B
        comp_min = 0.30
        if (ict_total >= ict_min and
                abs(sig.composite) >= comp_min):
            return ("B", 3,
                    f"TIER-B standard ICT={ict_total:.2f} Σ={sig.composite:+.3f}")

        return ("BLOCKED", 0,
                f"BELOW_MIN ICT={ict_total:.2f}<{ict_min} "
                f"Σ={sig.composite:+.3f}<{comp_min}")


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

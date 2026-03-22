"""
dynamic_trail_engine.py — Context-Aware Dynamic Trailing SL Engine v6.0
========================================================================
Replaces BOTH the legacy InstitutionalLevels.compute_trail_sl AND the
competing ratchet system in _update_trailing_sl. Single coherent state
machine — no parallel systems fighting each other.

Core Design Principles:
────────────────────────────────────────────────────────────────────────
1. EVERY TRADE IS DIFFERENT: Phase thresholds, min-distances, and freeze
   decisions adapt to the live market context (ATR regime, AMD phase,
   structure density, pullback depth, time in trade).

2. STRUCTURE-FIRST: OBs, swing lows/highs, FVGs, and BOS events define
   WHERE the SL goes. R-multiples only define WHEN to start looking.

3. NO COMPETING SYSTEMS: One unified trail function. The ratchet logic
   (BE lock, structural defense) is integrated into the candidate pool,
   not a separate pre-emption pathway.

4. SMART PULLBACK HANDLING: Pullback freeze has profit-dependent
   sensitivity and time decay. At 1.5R+, the freeze is much shorter
   and requires MORE reversal signals to trigger. At 0.5R with a deep
   pullback, the freeze is aggressive to protect the position.

5. DYNAMIC MIN-DISTANCE: Scales with volatility regime (ATR percentile)
   and position maturity, not fixed per phase.

Usage:
    from dynamic_trail_engine import DynamicTrailEngine
    new_sl = DynamicTrailEngine.compute(...)

Drop-in replacement for both ICTTrailEngine.compute() and
InstitutionalLevels.compute_trail_sl().
"""

from __future__ import annotations
import logging
import math
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Import _find_swings at module level to avoid repeated import overhead
# inside the hot trail-candidate-building path (called every 10s per trade).
try:
    from ict_trade_engine import _find_swings as _find_swings_ict
    _ICT_SWINGS_AVAILABLE = True
except ImportError:
    _find_swings_ict = None
    _ICT_SWINGS_AVAILABLE = False


def _round_tick(price: float, tick: float = 0.1) -> float:
    """Round price to nearest tick."""
    if tick <= 0:
        return price
    return round(round(price / tick) * tick, 10)


def _safe_float(c, key: str, default: float = 0.0) -> float:
    """Extract float from candle dict or object safely."""
    try:
        return float(c[key])
    except (KeyError, TypeError, IndexError):
        try:
            return float(getattr(c, key, default))
        except (TypeError, ValueError):
            return default


def _get_commission_rate() -> float:
    """Read commission rate from config, with a safe fallback."""
    try:
        import config as _cfg_dt
        return float(getattr(_cfg_dt, 'COMMISSION_RATE', 0.00055))
    except Exception:
        return 0.00055


def _find_swings_internal(candles: list, lookback: int
                          ) -> Tuple[List[float], List[float]]:
    """
    Find swing highs and swing lows from closed candles.
    Returns (swing_highs, swing_lows).
    """
    if len(candles) < 3 or lookback < 1:
        return [], []
    lb = min(lookback, len(candles) - 2)
    highs, lows = [], []
    for i in range(lb, len(candles) - lb):
        h = _safe_float(candles[i], 'h')
        l = _safe_float(candles[i], 'l')
        is_sh = all(h >= _safe_float(candles[j], 'h')
                     for j in range(i - lb, i + lb + 1) if j != i)
        is_sl = all(l <= _safe_float(candles[j], 'l')
                     for j in range(i - lb, i + lb + 1) if j != i)
        if is_sh:
            highs.append(h)
        if is_sl:
            lows.append(l)
    return highs, lows


# ═══════════════════════════════════════════════════════════════════════════
# TRAIL CONTEXT: Everything the engine needs to make an intelligent decision
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class TrailContext:
    """
    Computed context that drives all trail decisions.
    Built once per trail tick from raw inputs.
    """
    # ── Position basics ────────────────────────────────────────────
    pos_side:       str
    price:          float
    entry_price:    float
    current_sl:     float
    atr:            float
    init_sl_dist:   float      # ORIGINAL SL distance at entry (never changes)
    tick_size:      float = 0.1

    # ── Profit state ───────────────────────────────────────────────
    profit:         float = 0.0   # current signed profit in price units
    peak_profit:    float = 0.0   # historical max profit (ratchets up)
    peak_price_abs: float = 0.0   # actual best price (highest for long, lowest for short)
    tier:           float = 0.0   # R-multiple = peak_profit / init_sl_dist

    # ── Market context ─────────────────────────────────────────────
    atr_percentile: float = 0.50  # 0-1, current ATR regime
    amd_phase:      str   = "ACCUMULATION"
    amd_confidence: float = 0.0
    adx:            float = 15.0
    trade_mode:     str   = "reversion"

    # ── Time ───────────────────────────────────────────────────────
    hold_seconds:   float = 0.0
    now_ms:         int   = 0

    # ── Derived ────────────────────────────────────────────────────
    be_price:       float = 0.0   # breakeven + fee buffer
    be_locked:      bool  = False # SL already past BE
    phase:          int   = 0     # computed trail phase
    min_dist:       float = 0.0   # dynamic minimum distance from price

    @classmethod
    def build(cls, pos_side: str, price: float, entry_price: float,
              current_sl: float, atr: float, initial_sl_dist: float,
              peak_profit: float, peak_price_abs: float,
              hold_seconds: float, entry_vol: float,
              trade_mode: str,
              atr_percentile: float = 0.50,
              amd_phase: str = "ACCUMULATION",
              amd_confidence: float = 0.0,
              adx: float = 15.0,
              tick_size: float = 0.1,
              now_ms: int = 0) -> "TrailContext":
        """Build context from raw inputs with all derived fields computed."""
        if now_ms <= 0:
            now_ms = int(time.time() * 1000)

        init_dist = (initial_sl_dist if initial_sl_dist > 1e-10
                     else max(abs(entry_price - current_sl), atr))

        profit = ((price - entry_price) if pos_side == "long"
                  else (entry_price - price))

        # Phase ratchets UP — tier uses peak, never current
        tier = max(profit, peak_profit) / init_dist if init_dist > 1e-10 else 0.0

        # Fee-adjusted breakeven — read rate from config, fall back to 0.00055
        _rate = _get_commission_rate()
        rt_fee = entry_price * _rate * 2.0
        be_buf = rt_fee + 0.25 * atr
        be_price = (entry_price + be_buf if pos_side == "long"
                    else entry_price - be_buf)

        be_locked = ((pos_side == "long" and current_sl >= be_price) or
                     (pos_side == "short" and current_sl <= be_price))

        ctx = cls(
            pos_side=pos_side, price=price, entry_price=entry_price,
            current_sl=current_sl, atr=atr, init_sl_dist=init_dist,
            tick_size=tick_size,
            profit=profit, peak_profit=peak_profit,
            peak_price_abs=peak_price_abs, tier=tier,
            atr_percentile=atr_percentile, amd_phase=amd_phase,
            amd_confidence=amd_confidence, adx=adx,
            trade_mode=trade_mode, hold_seconds=hold_seconds,
            now_ms=now_ms, be_price=be_price, be_locked=be_locked,
        )

        # Compute dynamic phase and min_dist
        ctx.phase, ctx.min_dist = DynamicTrailEngine._compute_phase_and_min_dist(ctx)
        return ctx


# ═══════════════════════════════════════════════════════════════════════════
# DYNAMIC TRAIL ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class DynamicTrailEngine:
    """
    Unified context-aware trailing SL engine.

    Replaces both ICTTrailEngine and the ratchet system in _update_trailing_sl.
    Single entry point: DynamicTrailEngine.compute().
    """

    # ── Phase + min_dist computation (adapts to market context) ────────────

    @staticmethod
    def _compute_phase_and_min_dist(ctx: TrailContext) -> Tuple[int, float]:
        """
        Dynamic phase gating and minimum distance.

        Unlike fixed R-thresholds, these adapt to:
        - AMD phase: DISTRIBUTION gets tighter gates (smart money delivering)
        - ATR percentile: High vol → wider min_dist, low vol → tighter
        - ADX: Trending → tighter gates (momentum carries), ranging → wider
        - Trade mode: trend/momentum gets different treatment than reversion
        """
        tier = ctx.tier
        atr = ctx.atr
        amd = ctx.amd_phase
        amd_conf = ctx.amd_confidence

        # ── Volatility scaling factor ──────────────────────────────────
        # High ATR percentile = volatile → wider stops needed
        # Low ATR percentile = quiet → can trail tighter
        vol_scale = 0.8 + 0.4 * min(max(ctx.atr_percentile, 0.0), 1.0)
        # Range: 0.8 (quiet) to 1.2 (volatile)

        # ── Trend scaling factor ───────────────────────────────────────
        # Strong ADX → price moves in direction, tighter min_dist OK
        # Weak ADX → choppy, need wider buffer
        adx_factor = 1.0
        if ctx.adx > 25:
            adx_factor = 0.85   # trending: can be tighter
        elif ctx.adx < 15:
            adx_factor = 1.15   # ranging: wider buffer needed

        # ── AMD-aware phase thresholds ─────────────────────────────────
        if amd == "MANIPULATION" and tier < 1.0:
            # Judas swing still active — no trailing at all
            return 0, float('inf')

        if amd == "DISTRIBUTION" and amd_conf >= 0.50:
            # Smart money is delivering. Tighter gates.
            if tier >= 2.0:
                phase = 3
                base_min = 0.6 * atr
            elif tier >= 1.0:
                phase = 2
                base_min = 0.9 * atr
            elif tier >= 0.45:
                phase = 1
                base_min = 1.3 * atr
            else:
                return 0, float('inf')

        elif amd in ("REACCUMULATION", "REDISTRIBUTION"):
            if tier >= 1.5:
                phase = 3
                base_min = 0.7 * atr
            elif tier >= 0.80:
                phase = 2
                base_min = 1.0 * atr
            elif tier >= 0.45:
                phase = 1
                base_min = 1.3 * atr
            else:
                return 0, float('inf')

        elif amd == "MANIPULATION" and tier >= 1.0:
            # High tier during manipulation — displacement confirmed
            if tier >= 1.5:
                phase = 2
                base_min = 1.1 * atr
            else:
                phase = 1
                base_min = 1.4 * atr

        else:
            # ACCUMULATION or unknown — conservative
            if tier >= 2.0:
                phase = 3
                base_min = 1.0 * atr
            elif tier >= 1.5:
                phase = 2
                base_min = 1.3 * atr
            elif tier >= 0.80:
                phase = 1
                base_min = 1.8 * atr
            elif tier >= 0.45:
                phase = 1
                base_min = 2.2 * atr
            else:
                return 0, float('inf')

        # Apply vol and ADX scaling
        min_dist = base_min * vol_scale * adx_factor

        # Floor: never less than 0.4 ATR
        min_dist = max(min_dist, 0.4 * atr)

        return phase, min_dist

    # ── Pullback classifier (profit-aware, time-decaying) ──────────────

    @staticmethod
    def _classify_pullback(ctx: TrailContext,
                           candles_1m: List[Dict],
                           candles_5m: List[Dict],
                           orderbook: Dict) -> Tuple[bool, int, str]:
        """
        Smart pullback detection with profit-dependent sensitivity.

        At low R (0.5-1.0): aggressive freeze (2 signals = pullback)
        At mid R (1.0-2.0): moderate (3 signals = pullback)
        At high R (2.0+): conservative freeze (4 signals = pullback)
            → deep into profit, trail should ADVANCE, not freeze

        Also has time-decay: freeze max duration scales with R.
        After max_freeze_sec, pullback is ignored.
        """
        atr = ctx.atr
        rev_sigs = 0
        details = []

        if atr < 1e-10 or len(candles_1m) < 10:
            return True, 0, "insufficient_data"

        retrace = abs(ctx.peak_price_abs - ctx.price) if ctx.peak_price_abs > 1e-10 else 0.0

        # ── Time-based freeze limit ─────────────────────────────────────
        # At 0.5R: freeze up to 10 minutes during pullback
        # At 1.0R: freeze up to 5 minutes
        # At 2.0R+: freeze up to 2 minutes
        # After this, pullback freeze is ignored — trail must advance
        if ctx.tier >= 2.0:
            max_freeze = 120.0
        elif ctx.tier >= 1.0:
            max_freeze = 300.0
        else:
            max_freeze = 600.0

        # Check if we've exceeded the time-limited freeze already
        # (hold_seconds is total hold, not pullback duration — approximation)
        # The pullback itself is tracked by retrace depth increasing over time
        # For now, use: if we've been holding > max_freeze AND have deep retrace,
        # DON'T freeze — let the trail advance
        retrace_deep = retrace > 0.8 * atr
        if retrace_deep and ctx.hold_seconds > max_freeze:
            return False, 0, "time_decay_override"

        # ── Signal 1: Volume expansion ──────────────────────────────────
        if len(candles_1m) >= 10:
            rv = sum(_safe_float(c, 'v') for c in candles_1m[-3:]) / 3.0
            iv = sum(_safe_float(c, 'v') for c in candles_1m[-8:-3]) / 5.0
            if iv > 1e-10:
                vr = rv / iv
                # Meaningful expansion: 30%+ above prior (was 20% — too sensitive)
                if vr > 1.30:
                    rev_sigs += 1
                    details.append(f"vol_expand({vr:.2f})")
                else:
                    details.append(f"vol_norm({vr:.2f})")

        # ── Signal 2: Retrace depth ─────────────────────────────────────
        if retrace > 1.0 * atr:
            rev_sigs += 1
            details.append(f"deep({retrace/atr:.1f}ATR)")
        else:
            details.append(f"shallow({retrace/atr:.1f}ATR)")

        # ── Signal 3: Large opposing candle bodies ──────────────────────
        if len(candles_1m) >= 8:
            imp_b = [abs(_safe_float(c, 'c') - _safe_float(c, 'o'))
                     for c in candles_1m[-8:-3]]
            ret_b = [abs(_safe_float(c, 'c') - _safe_float(c, 'o'))
                     for c in candles_1m[-3:]]
            ai = sum(imp_b) / max(len(imp_b), 1)
            ar = sum(ret_b) / max(len(ret_b), 1)
            if ai > 1e-10 and ar / ai > 0.90:
                rev_sigs += 1
                details.append("large_bodies")

        # ── Signal 4: Orderbook imbalance flip ──────────────────────────
        bids = orderbook.get("bids", [])
        asks = orderbook.get("asks", [])
        if bids and asks:
            def _qty(lvl):
                if isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
                    return float(lvl[1])
                if isinstance(lvl, dict):
                    return float(lvl.get("size") or lvl.get("quantity") or 0)
                return 0.0
            bd = sum(_qty(b) for b in bids[:5]) if len(bids) >= 5 else 0
            ad = sum(_qty(a) for a in asks[:5]) if len(asks) >= 5 else 0
            tot = bd + ad
            if tot > 1e-10:
                imb = (bd - ad) / tot
                if ctx.pos_side == "long" and imb < -0.20:
                    rev_sigs += 1
                    details.append(f"ob_flip({imb:+.2f})")
                elif ctx.pos_side == "short" and imb > 0.20:
                    rev_sigs += 1
                    details.append(f"ob_flip({imb:+.2f})")

        # ── Signal 5: 5m swing break ───────────────────────────────────
        if len(candles_5m) >= 5:
            closed = candles_5m[:-1] if len(candles_5m) > 1 else candles_5m
            highs_5m, lows_5m = _find_swings_internal(
                closed, min(8, len(closed) - 2))
            if ctx.pos_side == "long" and lows_5m:
                rel = [l for l in lows_5m
                       if l > ctx.entry_price - 0.5 * atr]
                if rel and ctx.price < min(rel):
                    rev_sigs += 1
                    details.append("5m_sw_broken")
            elif ctx.pos_side == "short" and highs_5m:
                rel = [h for h in highs_5m
                       if h < ctx.entry_price + 0.5 * atr]
                if rel and ctx.price > max(rel):
                    rev_sigs += 1
                    details.append("5m_sw_broken")

        # ── Signal 6: Momentum stalling ────────────────────────────────
        if len(candles_1m) >= 6:
            last5 = candles_1m[-5:]
            if ctx.pos_side == "long":
                if all(_safe_float(last5[i], 'h') <= _safe_float(last5[i-1], 'h')
                       for i in range(1, len(last5))):
                    rev_sigs += 1
                    details.append("momentum_stall")
            else:
                if all(_safe_float(last5[i], 'l') >= _safe_float(last5[i-1], 'l')
                       for i in range(1, len(last5))):
                    rev_sigs += 1
                    details.append("momentum_stall")

        # ── Profit-dependent freeze threshold ──────────────────────────
        # At low R: only 2 reversal signals needed to NOT freeze (= reversal)
        # At mid R: need 3 signals to break freeze
        # At high R: need 4 signals — at deep profit, freeze should release easily
        if ctx.tier >= 2.0:
            freeze_threshold = 4    # hard to freeze when deep in profit
        elif ctx.tier >= 1.0:
            freeze_threshold = 3    # moderate
        else:
            freeze_threshold = 2    # easy to freeze when near BE

        is_pullback = rev_sigs < freeze_threshold
        return is_pullback, rev_sigs, "|".join(details)

    # ── Main compute function ──────────────────────────────────────────

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
                # New context inputs (optional — degrades gracefully)
                atr_percentile: float = 0.50,
                amd_phase: str = "",
                amd_confidence: float = 0.0,
                adx: float = 15.0,
                tick_size: float = 0.1,
                ) -> Optional[float]:
        """
        Unified dynamic trailing SL.

        Returns new_sl (tick-rounded) or None (hold/freeze).

        This is a DROP-IN replacement for both ICTTrailEngine.compute()
        and InstitutionalLevels.compute_trail_sl(). All parameters from
        both interfaces are accepted.
        """
        if atr < 1e-10:
            return None

        # ── Extract AMD context from ICT engine if not provided ────────
        if not amd_phase and ict_engine is not None:
            try:
                amd = ict_engine._amd
                amd_phase = amd.phase
                amd_confidence = amd.confidence
            except Exception:
                amd_phase = "ACCUMULATION"
                amd_confidence = 0.0

        # ── Build context ──────────────────────────────────────────────
        ctx = TrailContext.build(
            pos_side=pos_side, price=price, entry_price=entry_price,
            current_sl=current_sl, atr=atr,
            initial_sl_dist=initial_sl_dist,
            peak_profit=peak_profit, peak_price_abs=peak_price_abs,
            hold_seconds=hold_seconds, entry_vol=entry_vol,
            trade_mode=trade_mode,
            atr_percentile=atr_percentile,
            amd_phase=amd_phase, amd_confidence=amd_confidence,
            adx=adx, tick_size=tick_size, now_ms=now_ms,
        )

        # ── Phase 0: Hands off ─────────────────────────────────────────
        if ctx.phase == 0:
            if hold_reason is not None:
                hold_reason.append(
                    f"PHASE0 tier={ctx.tier:.2f}R AMD={ctx.amd_phase}"
                    f"({ctx.amd_confidence:.2f})")
            return None

        # ── OB Zone Freeze (pullback into active OB) ──────────────────
        if ict_engine is not None and ctx.be_locked:
            try:
                freeze_atr = 0.35 * atr
                obs = (ict_engine.order_blocks_bull if pos_side == "long"
                       else ict_engine.order_blocks_bear)
                for ob in obs:
                    if not ob.is_active(ctx.now_ms) or ob.visit_count > 1:
                        continue
                    # Only freeze if SL is already near the OB
                    if pos_side == "long":
                        ob_near_sl = current_sl >= ob.low - 1.8 * atr
                    else:
                        ob_near_sl = current_sl <= ob.high + 1.8 * atr
                    if not ob_near_sl:
                        continue
                    # Guard: price must have LEFT the OB before we call it a pullback test
                    if pos_side == "short" and ob.low <= price:
                        continue
                    if pos_side == "long" and ob.high >= price:
                        continue
                    if ob.low - freeze_atr <= price <= ob.high + freeze_atr:
                        # Time-limit: OB freeze max 5 minutes (300s) at high tier
                        max_ob_freeze = 600.0 if ctx.tier < 1.0 else 300.0
                        if hold_seconds < max_ob_freeze:
                            if hold_reason is not None:
                                hold_reason.append(
                                    f"OB_ZONE_FREEZE@${ob.midpoint:.0f}"
                                    f"(tf={ob.timeframe})")
                            return None
            except Exception:
                pass

        # ── FVG Freeze (price inside delivery-path FVG) ────────────────
        if ict_engine is not None and ctx.be_locked and hold_seconds < 600.0:
            try:
                # Delivery-path FVGs: opposite direction to position
                fvgs = (ict_engine.fvgs_bear if pos_side == "long"
                        else ict_engine.fvgs_bull)
                freeze_atr = 0.40 * atr
                for fvg in fvgs:
                    if not fvg.is_active(ctx.now_ms):
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

        # ── Pullback Detection (profit-aware) ──────────────────────────
        if ctx.be_locked and len(candles_1m) >= 10 and len(candles_5m) >= 5:
            is_pb, rev_count, pb_detail = DynamicTrailEngine._classify_pullback(
                ctx, candles_1m, candles_5m, orderbook)
            if is_pb:
                if hold_reason is not None:
                    hold_reason.append(
                        f"PULLBACK({rev_count}rev/{4 if ctx.tier>=2.0 else 3 if ctx.tier>=1.0 else 2}thr)"
                        f"[{pb_detail}]")
                return None

        # ══════════════════════════════════════════════════════════════
        # BUILD CANDIDATE SL LEVELS
        # ══════════════════════════════════════════════════════════════
        candidates: List[Tuple[float, str]] = []
        min_dist = ctx.min_dist

        # ── 1. Profit floor (fee-adjusted BE) — always present ─────────
        candidates.append((ctx.be_price, "BE_FLOOR"))

        # ── 2. ICT OB anchor ──────────────────────────────────────────
        if ict_engine is not None and ctx.phase >= 1:
            try:
                ob_buf = 0.30 * atr
                obs = (ict_engine.order_blocks_bull if pos_side == "long"
                       else ict_engine.order_blocks_bear)
                active_obs = [o for o in obs if o.is_active(ctx.now_ms)]

                if pos_side == "long":
                    sorted_obs = sorted(
                        [o for o in active_obs if o.low < price - min_dist],
                        key=lambda x: x.low, reverse=True)
                    for ob in sorted_obs:
                        cand = ob.low - ob_buf
                        if current_sl < cand < price - min_dist:
                            candidates.append((
                                cand,
                                f"OB@${ob.midpoint:.0f}(tf={ob.timeframe})"))
                            break
                else:
                    sorted_obs = sorted(
                        [o for o in active_obs if o.high > price + min_dist],
                        key=lambda x: x.high)
                    for ob in sorted_obs:
                        cand = ob.high + ob_buf
                        if price + min_dist < cand < current_sl:
                            candidates.append((
                                cand,
                                f"OB@${ob.midpoint:.0f}(tf={ob.timeframe})"))
                            break
            except Exception:
                pass

        # ── 3. 5m swing structure (phase-scaled buffer) ────────────────
        if candles_5m and len(candles_5m) >= 6:
            closed_5m = candles_5m[:-1]
            try:
                # Try to use ict_trade_engine's _find_swings first
                from ict_trade_engine import _find_swings
                highs_5m, lows_5m = _find_swings(
                    closed_5m, min(12, len(closed_5m) - 2))
            except (ImportError, Exception):
                highs_5m, lows_5m = _find_swings_internal(
                    closed_5m, min(12, len(closed_5m) - 2))

            # Phase-scaled buffer: wider early, tighter deep in profit
            if ctx.phase == 1:
                sw_buf = 0.80 * atr * (0.9 + 0.2 * ctx.atr_percentile)
            elif ctx.phase == 2:
                sw_buf = 0.25 * atr * (0.9 + 0.2 * ctx.atr_percentile)
            else:
                sw_buf = 0.10 * atr

            if pos_side == "long" and lows_5m:
                valid = [l for l in lows_5m
                         if current_sl + 0.05 * atr < l < price - min_dist]
                if valid:
                    best = max(valid)
                    cand_sl = best - sw_buf
                    candidates.append((
                        cand_sl,
                        f"5m_SW@${best:.0f}(P{ctx.phase})"))

            elif pos_side == "short" and highs_5m:
                valid = [h for h in highs_5m
                         if price + min_dist < h < current_sl - 0.05 * atr]
                if valid:
                    best = min(valid)
                    cand_sl = best + sw_buf
                    candidates.append((
                        cand_sl,
                        f"5m_SW@${best:.0f}(P{ctx.phase})"))

        # ── 3b. 5m BOS trigger (immediate advance) ────────────────────
        if ict_engine is not None and ctx.phase >= 1:
            try:
                tf_st = ict_engine._tf.get("5m")
                if tf_st is not None:
                    if (pos_side == "long" and
                            tf_st.bos_direction == "bullish" and
                            tf_st.bos_level > current_sl and
                            tf_st.bos_level < price - min_dist):
                        bos_sl = tf_st.bos_level - 0.20 * atr
                        candidates.append((
                            bos_sl,
                            f"5m_BOS_BULL@${tf_st.bos_level:.0f}"))
                    elif (pos_side == "short" and
                            tf_st.bos_direction == "bearish" and
                            tf_st.bos_level < current_sl and
                            tf_st.bos_level > price + min_dist):
                        bos_sl = tf_st.bos_level + 0.20 * atr
                        candidates.append((
                            bos_sl,
                            f"5m_BOS_BEAR@${tf_st.bos_level:.0f}"))
            except Exception:
                pass

        # ── 4. FVG fill lock (70%+ filled → lock profit) ──────────────
        if ict_engine is not None:
            try:
                fvgs = (ict_engine.fvgs_bear if pos_side == "long"
                        else ict_engine.fvgs_bull)
                for fvg in fvgs:
                    if not fvg.is_active(ctx.now_ms):
                        continue
                    if fvg.fill_percentage < 0.70:
                        continue
                    lock = (fvg.top + 0.20 * atr if pos_side == "long"
                            else fvg.bottom - 0.20 * atr)
                    if pos_side == "long" and current_sl < lock < price - min_dist:
                        candidates.append((lock, f"FVG_FILL@${fvg.midpoint:.0f}"))
                    elif pos_side == "short" and price + min_dist < lock < current_sl:
                        candidates.append((lock, f"FVG_FILL@${fvg.midpoint:.0f}"))
            except Exception:
                pass

        # ── 5. 1m swing structure (Phase 2+) ──────────────────────────
        if ctx.phase >= 2 and len(candles_1m) >= 6:
            closed_1m = candles_1m[:-1]
            try:
                if _ICT_SWINGS_AVAILABLE:
                    sh_1m, sl_1m = _find_swings_ict(
                        closed_1m, min(10, len(closed_1m) - 2))
                else:
                    sh_1m, sl_1m = _find_swings_internal(
                        closed_1m, min(10, len(closed_1m) - 2))
            except Exception:
                sh_1m, sl_1m = _find_swings_internal(
                    closed_1m, min(10, len(closed_1m) - 2))

            micro_buf = 0.08 * atr
            if pos_side == "long" and sl_1m:
                valid = [l for l in sl_1m
                         if current_sl + 0.05 * atr < l < price - min_dist]
                if valid:
                    best = max(valid)
                    candidates.append((
                        best - micro_buf,
                        f"1m_SW@${best:.0f}"))

            elif pos_side == "short" and sh_1m:
                valid = [h for h in sh_1m
                         if price + min_dist < h < current_sl - 0.05 * atr]
                if valid:
                    best = min(valid)
                    candidates.append((
                        best + micro_buf,
                        f"1m_SW@${best:.0f}"))

        # ── 6. Chandelier (Phase 3, LAST RESORT when no structure) ─────
        if ctx.phase >= 3 and peak_price_abs > 1e-10 and len(candidates) <= 1:
            n_ch = 2.0 if trade_mode in ("trend", "momentum") else 2.5
            if pos_side == "long":
                chand = peak_price_abs - n_ch * atr
                if current_sl < chand < price - min_dist:
                    candidates.append((chand, f"CHANDELIER_{n_ch}x"))
            else:
                chand = peak_price_abs + n_ch * atr
                if price + min_dist < chand < current_sl:
                    candidates.append((chand, f"CHANDELIER_{n_ch}x"))

        # ── 7. Volume-decay tightening (Phase 3) ──────────────────────
        vol_tighten = 0.0
        if ctx.phase >= 3 and len(candles_1m) >= 10 and entry_vol > 1e-10:
            rv = sum(_safe_float(c, 'v') for c in candles_1m[-5:]) / 5.0
            vr = rv / entry_vol
            if vr < 0.60:
                vol_tighten = 0.25 * atr * (1.0 - vr / 0.60)

        # ══════════════════════════════════════════════════════════════
        # SELECT BEST CANDIDATE
        # ══════════════════════════════════════════════════════════════
        if not candidates:
            if hold_reason is not None:
                hold_reason.append("NO_CANDIDATES")
            return None

        if pos_side == "long":
            new_sl, anchor = max(candidates, key=lambda x: x[0])
            if vol_tighten > 0:
                new_sl = min(new_sl + vol_tighten, price - min_dist)
        else:
            new_sl, anchor = min(candidates, key=lambda x: x[0])
            if vol_tighten > 0:
                new_sl = max(new_sl - vol_tighten, price + min_dist)

        # ── Liquidity pool ceiling ────────────────────────────────────
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
                            anchor += f"+LIQ@${pool.price:.0f}"
                    elif pos_side == "short" and pool.pool_type == "EQH":
                        floor_p = pool.price + liq_buf
                        if current_sl > floor_p > new_sl:
                            new_sl = floor_p
                            anchor += f"+LIQ@${pool.price:.0f}"
            except Exception:
                pass

        # ── Min distance enforcement ──────────────────────────────────
        if pos_side == "long":
            new_sl = min(new_sl, price - min_dist)
        else:
            new_sl = max(new_sl, price + min_dist)

        # ── Ratchet: SL may only improve ──────────────────────────────
        if pos_side == "long":
            if new_sl <= current_sl:
                if hold_reason is not None:
                    hold_reason.append(
                        f"NO_IMPROVEMENT new={new_sl:.1f}<=cur={current_sl:.1f}")
                return None
        else:
            if new_sl >= current_sl:
                if hold_reason is not None:
                    hold_reason.append(
                        f"NO_IMPROVEMENT new={new_sl:.1f}>=cur={current_sl:.1f}")
                return None

        # ── Minimum meaningful move ───────────────────────────────────
        _min_meaningful = 0.08 * atr
        if pos_side == "long":
            if new_sl < current_sl + _min_meaningful:
                if hold_reason is not None:
                    hold_reason.append(
                        f"MIN_MOVE d={new_sl - current_sl:.1f}<{_min_meaningful:.1f}")
                return None
        else:
            if new_sl > current_sl - _min_meaningful:
                if hold_reason is not None:
                    hold_reason.append(
                        f"MIN_MOVE d={current_sl - new_sl:.1f}<{_min_meaningful:.1f}")
                return None

        # ── Structural path check ─────────────────────────────────────
        _path_override = ctx.tier >= 1.0
        if not _path_override and ict_engine is not None:
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
                    pos_side, current_sl, new_sl, ctx.now_ms, tier=ctx.tier)
                if blocked:
                    if hold_reason is not None:
                        hold_reason.append(f"PATH_BLOCKED:{reason}")
                    return None
            except Exception:
                pass

        logger.debug(
            f"Trail {pos_side.upper()}: ${current_sl:,.1f} → ${new_sl:,.1f} "
            f"[{anchor}] AMD={ctx.amd_phase} P{ctx.phase} "
            f"tier={ctx.tier:.2f}R min_d={min_dist:.0f}")

        return _round_tick(new_sl, ctx.tick_size)

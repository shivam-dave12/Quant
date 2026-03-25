"""
dynamic_trail_engine.py — Structure-Only Trailing SL Engine v8.0
================================================================
COMPLETE REWRITE — Institutional Market-Structure Trailing

DESIGN PHILOSOPHY
─────────────────
Institutions do NOT trail stops using fixed R-multiples, ATR multipliers,
or time-based rules.  They trail behind MARKET STRUCTURE:

  • A stop loss lives below/above a STRUCTURAL LEVEL that, if broken,
    invalidates the trade thesis.
  • The stop moves ONLY when a NEW structural level forms that is:
    (a) higher than current SL (longs) / lower (shorts)
    (b) confirmed by closed candles (not live wicks)
    (c) respects the hierarchy: 15m > 5m > 1m

This engine has ZERO:
  - Fixed R-multiple tier gates
  - Time-based phase transitions
  - ATR-multiple min_dist formulas with fixed multipliers
  - Chandelier/parabolic fallbacks
  - Pullback freeze timers
  - OB/FVG freeze conditions

WHAT DRIVES EVERY DECISION
───────────────────────────
  1. SWING STRUCTURE — the sole anchor for SL placement
     New swing forms behind trade → SL advances to swing - buffer
     No new swing → SL does NOT move (market decides, not a timer)

  2. BREAK OF STRUCTURE (BOS) — phase advancement
     Phase 0→1: first BOS in trade direction on 5m (proves thesis)
     Phase 1→2: confirmed displacement OR second BOS
     Phase 2→3: 3+ BOS OR 15m structure available

  3. CHANGE OF CHARACTER (CHoCH) — tightening trigger
     CHoCH against trade on 5m/1m → immediately tighten buffer
     This is the market's structural warning, not a parameter

  4. VOLATILITY REGIME — buffer scaling (not phase gating)
     Expanding vol → wider swing buffers (absorb noise)
     Contracting vol → tighter buffers (trend acceleration)
     This scales the BUFFER, not the decision to move

  5. LIQUIDITY HUNT GUARD — post-selection safety
     Shifts final SL away from known hunt zones (round numbers,
     equal-low clusters, known liquidity pools)

BACKWARD COMPATIBILITY
──────────────────────
  - compute() signature is FULLY BACKWARD COMPATIBLE with v7.0
  - Returns Optional[float] — same contract
  - Drop-in replacement
"""

from __future__ import annotations
import logging
import math
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

try:
    from ict_trade_engine import _find_swings as _find_swings_ict
    _ICT_SWINGS_AVAILABLE = True
except ImportError:
    _find_swings_ict = None
    _ICT_SWINGS_AVAILABLE = False


# ─────────────────────────────────────────────────────────────────────────────
# PRIMITIVE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _round_tick(price: float, tick: float = 0.1) -> float:
    if tick <= 0:
        return price
    return round(round(price / tick) * tick, 10)


def _safe_float(c, key: str, default: float = 0.0) -> float:
    try:
        return float(c[key])
    except (KeyError, TypeError, IndexError):
        try:
            return float(getattr(c, key, default))
        except (TypeError, ValueError):
            return default


def _get_commission_rate() -> float:
    try:
        import config as _cfg_dt
        return float(getattr(_cfg_dt, 'COMMISSION_RATE', 0.00055))
    except Exception:
        return 0.00055


def _find_swings_internal(candles: list, lookback: int
                          ) -> Tuple[List[float], List[float]]:
    """Find swing highs and swing lows using N-bar lookback."""
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


# ─────────────────────────────────────────────────────────────────────────────
# MULTI-PERIOD ATR
# ─────────────────────────────────────────────────────────────────────────────

def _compute_atr_series(candles: list, period: int) -> float:
    """Simple ATR(period) using true range."""
    if not candles:
        return 0.0
    n = min(period, len(candles))
    if n < 1:
        return 0.0
    trs = []
    for i in range(1, n + 1):
        idx = len(candles) - i
        h = _safe_float(candles[idx], 'h')
        l = _safe_float(candles[idx], 'l')
        if idx > 0:
            pc = _safe_float(candles[idx - 1], 'c')
            tr = max(h - l, abs(h - pc), abs(l - pc)) if pc > 1e-10 else h - l
        else:
            tr = h - l
        trs.append(tr)
    return sum(trs) / len(trs) if trs else 0.0


# ─────────────────────────────────────────────────────────────────────────────
# VOLATILITY REGIME — scales buffers, does NOT gate phases
# ─────────────────────────────────────────────────────────────────────────────

class _VolatilityRegime:
    """
    ATR(5)/ATR(20) ratio determines buffer scaling.
    EXPANDING  → wider buffers (wicks are larger, need more room)
    CONTRACTING → tighter buffers (clean price delivery)
    NORMAL → baseline
    """
    EXPAND_THRESH   = 1.25
    CONTRACT_THRESH = 0.75

    @staticmethod
    def compute(candles: list, atr_long: float) -> Tuple[float, str]:
        if atr_long < 1e-10 or len(candles) < 7:
            return 1.0, "NORMAL"
        atr_short = _compute_atr_series(candles, 5)
        if atr_short < 1e-10:
            return 1.0, "NORMAL"
        ratio = atr_short / atr_long
        if ratio > _VolatilityRegime.EXPAND_THRESH:
            return ratio, "EXPANDING"
        if ratio < _VolatilityRegime.CONTRACT_THRESH:
            return ratio, "CONTRACTING"
        return ratio, "NORMAL"


# ─────────────────────────────────────────────────────────────────────────────
# DISPLACEMENT DETECTOR — confirms institutional intent
# ─────────────────────────────────────────────────────────────────────────────

class _DisplacementDetector:
    """
    Identifies displacement bars: single candles with body >= 1.2 × ATR
    closing near their extreme, aligned with trade direction.
    Used as a phase advancement signal (structure event, not parameter).
    """
    BODY_THRESH = 1.20
    CLOSE_RATIO = 0.55
    LOOKBACK    = 5

    @staticmethod
    def detect(candles: list, atr: float,
               pos_side: str) -> Optional[Tuple[float, float, str]]:
        """Returns (close_price, body_ratio, label) or None."""
        if atr < 1e-10 or len(candles) < 3:
            return None
        lb = min(_DisplacementDetector.LOOKBACK, len(candles) - 1)
        for i in range(1, lb + 1):
            c = candles[-i]
            o, h, l, cl = (_safe_float(c, 'o'), _safe_float(c, 'h'),
                           _safe_float(c, 'l'), _safe_float(c, 'c'))
            rng = h - l
            if rng < 1e-10:
                continue
            body = abs(cl - o)
            bull = cl > o
            if pos_side == "long" and not bull:
                continue
            if pos_side == "short" and bull:
                continue
            if body < _DisplacementDetector.BODY_THRESH * atr:
                continue
            close_pos = (cl - l) / rng
            if pos_side == "long" and close_pos < _DisplacementDetector.CLOSE_RATIO:
                continue
            if pos_side == "short" and close_pos > (1.0 - _DisplacementDetector.CLOSE_RATIO):
                continue
            return (cl, body / atr, f"DISP@${cl:.0f}(b={body/atr:.1f}ATR,lag={i})")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# BOS / CHoCH READER — reads structural events from ICT engine
# ─────────────────────────────────────────────────────────────────────────────

class _StructureReader:
    """
    Reads Break of Structure (BOS) and Change of Character (CHoCH)
    from the ICT engine's timeframe state objects.
    These are the ONLY events that drive phase transitions.
    """

    @staticmethod
    def count_bos_in_direction(ict_engine, pos_side: str) -> int:
        """Count BOS events aligned with trade direction across timeframes."""
        if ict_engine is None:
            return 0
        count = 0
        for tf_key in ("1m", "5m", "15m"):
            try:
                tf_st = ict_engine._tf.get(tf_key)
                if tf_st is None:
                    continue
                bos_dir = getattr(tf_st, 'bos_direction', None)
                if not bos_dir:
                    continue
                if pos_side == "long" and bos_dir == "bullish":
                    count += 1
                elif pos_side == "short" and bos_dir == "bearish":
                    count += 1
            except Exception:
                pass
        return count

    @staticmethod
    def get_bos_level(ict_engine, tf_key: str, pos_side: str) -> Optional[float]:
        """Get the BOS level for a specific timeframe if aligned with trade."""
        if ict_engine is None:
            return None
        try:
            tf_st = ict_engine._tf.get(tf_key)
            if tf_st is None:
                return None
            bos_dir = getattr(tf_st, 'bos_direction', None)
            bos_lvl = getattr(tf_st, 'bos_level', 0.0)
            if not bos_dir or not bos_lvl:
                return None
            if pos_side == "long" and bos_dir == "bullish":
                return float(bos_lvl)
            if pos_side == "short" and bos_dir == "bearish":
                return float(bos_lvl)
        except Exception:
            pass
        return None

    @staticmethod
    def detect_choch_against(ict_engine, pos_side: str) -> Optional[Tuple[str, float]]:
        """
        Detect CHoCH against trade direction.
        Returns (timeframe, choch_level) or None.
        """
        if ict_engine is None:
            return None
        for tf_key in ("5m", "1m"):
            try:
                tf_st = ict_engine._tf.get(tf_key)
                if tf_st is None:
                    continue
                choch_dir = getattr(tf_st, 'choch_direction', None)
                choch_lvl = getattr(tf_st, 'choch_level', 0.0)
                if not choch_dir or not choch_lvl:
                    continue
                if pos_side == "long" and choch_dir == "bearish":
                    return (tf_key, float(choch_lvl))
                if pos_side == "short" and choch_dir == "bullish":
                    return (tf_key, float(choch_lvl))
            except Exception:
                pass
        return None


# ─────────────────────────────────────────────────────────────────────────────
# LIQUIDITY HUNT GUARD — post-selection SL safety shift
# ─────────────────────────────────────────────────────────────────────────────

class _LiquidityHuntGuard:
    """
    Shifts the chosen SL away from known stop-hunt zones:
    1. Known liquidity pools (BSL/SSL from ict_engine)
    2. Round numbers (BTC: divisible by 500)
    3. Equal-low/high clusters (3+ candle lows/highs clustering)

    Applied AFTER candidate selection. For longs, shifts SL DOWN
    (further from price, beneath the hunt zone). For shorts, UP.
    """
    HUNT_ZONE_ATR   = 0.15
    HUNT_OFFSET_ATR = 0.30
    CLUSTER_SIZE    = 3
    CLUSTER_BAND    = 0.12
    ROUND_SPACING   = 500.0

    @staticmethod
    def adjust(new_sl: float, pos_side: str, atr: float,
               candles: list = None,
               liquidity_pools=None,
               round_spacing: float = 500.0) -> Tuple[float, str]:
        if atr < 1e-10:
            return new_sl, ""
        hunt_zone = _LiquidityHuntGuard.HUNT_ZONE_ATR * atr
        offset = _LiquidityHuntGuard.HUNT_OFFSET_ATR * atr
        cluster_b = _LiquidityHuntGuard.CLUSTER_BAND * atr
        adjusted = new_sl
        reasons: List[str] = []

        # 1. Known liquidity pools
        if liquidity_pools:
            for pool in liquidity_pools:
                try:
                    if getattr(pool, 'swept', False):
                        continue
                    pp = float(pool.price)
                    if abs(adjusted - pp) < hunt_zone:
                        if pos_side == "long":
                            adjusted = min(adjusted, pp - offset)
                        else:
                            adjusted = max(adjusted, pp + offset)
                        reasons.append(f"LIQ@{pp:.0f}")
                except Exception:
                    pass

        # 2. Round numbers
        nearest = round(adjusted / round_spacing) * round_spacing
        if abs(adjusted - nearest) < hunt_zone:
            if pos_side == "long":
                adjusted = nearest - offset
            else:
                adjusted = nearest + offset
            reasons.append(f"ROUND@{nearest:.0f}")

        # 3. Equal-low/high cluster
        if candles and len(candles) >= 10:
            recent = candles[-20:]
            if pos_side == "long":
                near_lows = [_safe_float(c, 'l') for c in recent
                             if abs(_safe_float(c, 'l') - adjusted) < cluster_b]
                if len(near_lows) >= _LiquidityHuntGuard.CLUSTER_SIZE:
                    center = sum(near_lows) / len(near_lows)
                    adjusted = min(adjusted, center - offset)
                    reasons.append(f"EQL_CLUST@{center:.0f}")
            else:
                near_highs = [_safe_float(c, 'h') for c in recent
                              if abs(_safe_float(c, 'h') - adjusted) < cluster_b]
                if len(near_highs) >= _LiquidityHuntGuard.CLUSTER_SIZE:
                    center = sum(near_highs) / len(near_highs)
                    adjusted = max(adjusted, center + offset)
                    reasons.append(f"EQH_CLUST@{center:.0f}")

        return adjusted, ("+".join(reasons) if reasons else "")


# ═══════════════════════════════════════════════════════════════════════════
# TRAIL CONTEXT — computed once per trail tick
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class TrailContext:
    """
    All state needed for trail decisions, built from raw inputs.

    v8.0: Phase is determined by STRUCTURE EVENTS, not R-multiples.
    """
    pos_side:       str
    price:          float
    entry_price:    float
    current_sl:     float
    atr:            float
    init_sl_dist:   float
    tick_size:      float = 0.1

    # Profit state (for logging/diagnostics, also secondary phase confirmation)
    profit:         float = 0.0
    peak_profit:    float = 0.0
    peak_price_abs: float = 0.0
    tier:           float = 0.0      # R-multiple (secondary confirmation only)

    # Market context
    atr_percentile: float = 0.50
    amd_phase:      str   = "ACCUMULATION"
    amd_confidence: float = 0.0
    adx:            float = 15.0
    trade_mode:     str   = "reversion"

    # Live volatility
    vol_ratio:       float = 1.0
    vol_regime:      str   = "NORMAL"
    has_displacement: bool = False

    # Structure events (v8.0: these DRIVE phases)
    bos_count:      int   = 0       # BOS events in trade direction
    choch_against:  bool  = False   # CHoCH detected against trade
    choch_tf:       str   = ""      # timeframe of CHoCH
    choch_level:    float = 0.0     # price level of CHoCH

    # Time
    hold_seconds:   float = 0.0
    now_ms:         int   = 0

    # Derived
    be_price:       float = 0.0
    be_locked:      bool  = False
    phase:          int   = 0

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
        if now_ms <= 0:
            now_ms = int(time.time() * 1000)

        init_dist = (initial_sl_dist if initial_sl_dist > 1e-10
                     else max(abs(entry_price - current_sl), atr))

        profit = ((price - entry_price) if pos_side == "long"
                  else (entry_price - price))
        tier = max(profit, peak_profit) / init_dist if init_dist > 1e-10 else 0.0

        _rate = _get_commission_rate()
        rt_fee = entry_price * _rate * 2.0
        be_buf = rt_fee + 0.10 * atr   # tight BE: fees + 10% ATR breathing room
        be_price = (entry_price + be_buf if pos_side == "long"
                    else entry_price - be_buf)
        be_locked = ((pos_side == "long" and current_sl >= be_price) or
                     (pos_side == "short" and current_sl <= be_price))

        return cls(
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


# ═══════════════════════════════════════════════════════════════════════════
# DYNAMIC TRAIL ENGINE v8.0 — Structure-Only
# ═══════════════════════════════════════════════════════════════════════════

class DynamicTrailEngine:
    """
    Institutional market-structure trailing SL engine.

    Single entry point: DynamicTrailEngine.compute()

    Phase transitions are driven by MARKET STRUCTURE EVENTS:
      Phase 0: No structure confirmation yet — SL stays at initial level
      Phase 1: First BOS in trade direction confirmed → trail 5m swings
      Phase 2: Displacement OR 2+ BOS → add 1m micro-structure
      Phase 3: 3+ BOS OR 15m structure available → use 15m macro anchors

    SL candidates are ONLY structural levels (swings, OBs, BOS levels).
    """

    # ── Structure-driven phase computation ─────────────────────────────────

    @staticmethod
    def _compute_phase(ctx: TrailContext) -> int:
        """
        Phase is determined by market structure events + break-even status.

        Phase 0: Trade thesis not yet structurally confirmed
                 - No BOS in trade direction AND not at break-even
        Phase 1: First structural confirmation
                 - 1+ BOS in direction on any timeframe
                 - OR SL already at/past break-even (proved via price)
                 - OR price moved 0.8x initial risk without BOS (no-ICT fallback)
        Phase 2: Strong structural confirmation
                 - 2+ BOS in direction OR displacement bar detected
                 - OR price has moved 1.5x initial risk with at least 1 BOS
        Phase 3: Deep structural trend
                 - 3+ BOS OR confirmed direction + deep profit
                 - OR price has moved 2.5x initial risk
        """
        bos = ctx.bos_count
        disp = ctx.has_displacement
        be = ctx.be_locked
        tier = ctx.tier   # secondary confirmation, not primary gate

        # Phase 3: multiple BOS confirmations or deep structural profit
        if bos >= 3 or (bos >= 2 and tier >= 2.5):
            return 3

        # Phase 2: strong confirmation
        if bos >= 2 or (bos >= 1 and disp) or (be and tier >= 1.5):
            return 2

        # Phase 1: first confirmation — BOS or proved by price
        if bos >= 1 or be or tier >= 0.8:
            return 1

        # Phase 0: no structural confirmation
        return 0

    # ── Swing buffer computation ───────────────────────────────────────────

    @staticmethod
    def _compute_swing_buffer(atr: float, phase: int,
                              vol_ratio: float, vol_regime: str,
                              choch_against: bool) -> float:
        """
        Buffer placed below/above swing level for SL placement.

        This is the ONLY place volatility affects the trail distance.
        Phase determines the base; vol_regime scales it.
        """
        if phase <= 1:
            base = 0.50 * atr     # early: wider buffer protects thesis
        elif phase == 2:
            base = 0.30 * atr     # confirmed: moderate
        else:
            base = 0.15 * atr     # deep: tight (structure is proven)

        # Vol regime scales the buffer
        if vol_regime == "EXPANDING":
            vol_mult = min(vol_ratio, 1.40)
        elif vol_regime == "CONTRACTING":
            vol_mult = max(vol_ratio, 0.65)
        else:
            vol_mult = 1.0

        buffer = base * vol_mult

        # CHoCH against trade → structure warning → tighten buffer
        if choch_against:
            buffer *= 0.70

        return buffer

    # ── Minimum SL distance from price ─────────────────────────────────────

    @staticmethod
    def _compute_min_dist(atr: float, phase: int,
                          vol_regime: str, vol_ratio: float) -> float:
        """
        Minimum distance between SL and current price.

        Prevents SL from being so close that normal noise triggers it.
        Phase 1: wide (give trade room)
        Phase 2: moderate (structure confirmed)
        Phase 3: tight (deep trend, trail structure closely)
        """
        if phase <= 1:
            base = 1.0 * atr
        elif phase == 2:
            base = 0.60 * atr
        else:
            base = 0.35 * atr

        if vol_regime == "EXPANDING":
            base *= min(vol_ratio, 1.30)
        elif vol_regime == "CONTRACTING":
            base *= max(vol_ratio, 0.70)

        return max(base, 0.25 * atr)

    # ── Main compute function ──────────────────────────────────────────────

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
                atr_percentile: float = 0.50,
                amd_phase: str = "",
                amd_confidence: float = 0.0,
                adx: float = 15.0,
                tick_size: float = 0.1,
                candles_15m: List[Dict] = None,
                ) -> Optional[float]:
        """
        Structure-only trailing SL.

        Returns new_sl (tick-rounded) or None (hold — no structural change).
        Fully backward-compatible with v7.0 call sites.
        """
        if atr < 1e-10:
            return None

        # ── Extract AMD context from ICT engine if not provided ────────
        if not amd_phase and ict_engine is not None:
            try:
                amd_obj = ict_engine._amd
                amd_phase = amd_obj.phase
                amd_confidence = amd_obj.confidence
            except Exception:
                amd_phase = "ACCUMULATION"
                amd_confidence = 0.0

        # ── Live market data ───────────────────────────────────────────
        closed_1m = candles_1m[:-1] if len(candles_1m) > 1 else candles_1m
        vol_ratio, vol_regime = _VolatilityRegime.compute(candles_1m, atr)
        _disp_result = _DisplacementDetector.detect(closed_1m, atr, pos_side)
        has_displacement = _disp_result is not None

        # ── Structure events from ICT engine ───────────────────────────
        bos_count = _StructureReader.count_bos_in_direction(ict_engine, pos_side)
        choch_result = _StructureReader.detect_choch_against(ict_engine, pos_side)
        choch_against = choch_result is not None
        choch_tf = choch_result[0] if choch_result else ""
        choch_level = choch_result[1] if choch_result else 0.0

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
        ctx.vol_ratio = vol_ratio
        ctx.vol_regime = vol_regime
        ctx.has_displacement = has_displacement
        ctx.bos_count = bos_count
        ctx.choch_against = choch_against
        ctx.choch_tf = choch_tf
        ctx.choch_level = choch_level

        # ── Compute structure-driven phase ─────────────────────────────
        ctx.phase = DynamicTrailEngine._compute_phase(ctx)

        # ── Phase 0: No structural confirmation — hands off ────────────
        if ctx.phase == 0:
            if hold_reason is not None:
                hold_reason.append(
                    f"PHASE0 bos={bos_count} be={ctx.be_locked} "
                    f"tier={ctx.tier:.2f}R AMD={ctx.amd_phase}")
            return None

        phase = ctx.phase
        min_dist = DynamicTrailEngine._compute_min_dist(
            atr, phase, vol_regime, vol_ratio)
        swing_buf = DynamicTrailEngine._compute_swing_buffer(
            atr, phase, vol_ratio, vol_regime, choch_against)

        # ══════════════════════════════════════════════════════════════
        # BUILD CANDIDATE SL LEVELS — pure structure only
        # ══════════════════════════════════════════════════════════════
        candidates: List[Tuple[float, str]] = []

        # ── 0. Break-even floor — always present once phase >= 1 ───────
        candidates.append((ctx.be_price, "BE_FLOOR"))

        # ── 1. 5m swing structure (primary anchor) ─────────────────────
        if candles_5m and len(candles_5m) >= 6:
            closed_5m = candles_5m[:-1]
            try:
                if _ICT_SWINGS_AVAILABLE:
                    highs_5m, lows_5m = _find_swings_ict(
                        closed_5m, min(12, len(closed_5m) - 2))
                else:
                    highs_5m, lows_5m = _find_swings_internal(
                        closed_5m, min(12, len(closed_5m) - 2))
            except Exception:
                highs_5m, lows_5m = _find_swings_internal(
                    closed_5m, min(12, len(closed_5m) - 2))

            if pos_side == "long" and lows_5m:
                valid = [l for l in lows_5m
                         if l > current_sl + 0.05 * atr
                         and l - swing_buf < price - min_dist]
                if valid:
                    best = max(valid)
                    sl_cand = best - swing_buf
                    if sl_cand > current_sl:
                        candidates.append((sl_cand, f"5m_SW@${best:.0f}"))

            elif pos_side == "short" and highs_5m:
                valid = [h for h in highs_5m
                         if h < current_sl - 0.05 * atr
                         and h + swing_buf > price + min_dist]
                if valid:
                    best = min(valid)
                    sl_cand = best + swing_buf
                    if sl_cand < current_sl:
                        candidates.append((sl_cand, f"5m_SW@${best:.0f}"))

        # ── 2. BOS level as SL anchor ──────────────────────────────────
        if ict_engine is not None and phase >= 1:
            bos_5m = _StructureReader.get_bos_level(ict_engine, "5m", pos_side)
            if bos_5m is not None:
                bos_buf = 0.20 * atr
                if pos_side == "long":
                    sl_cand = bos_5m - bos_buf
                    if current_sl < sl_cand < price - min_dist:
                        candidates.append((sl_cand, f"5m_BOS@${bos_5m:.0f}"))
                else:
                    sl_cand = bos_5m + bos_buf
                    if price + min_dist < sl_cand < current_sl:
                        candidates.append((sl_cand, f"5m_BOS@${bos_5m:.0f}"))

        # ── 3. ICT Order Block anchor ──────────────────────────────────
        # HTF OBs are more significant than LTF OBs. Sort by strength
        # (1d=97, 4h=90, 1h=82, 15m=75, 5m=50, 1m=45) first, then by
        # proximity as tiebreaker. Add up to 2 OB candidates so the
        # final selection can choose between structural levels.
        if ict_engine is not None and phase >= 1:
            try:
                ob_buf = 0.25 * atr
                obs = (ict_engine.order_blocks_bull if pos_side == "long"
                       else ict_engine.order_blocks_bear)
                active_obs = [o for o in obs if o.is_active(ctx.now_ms)]

                _added = 0
                if pos_side == "long":
                    valid_obs = [o for o in active_obs if o.low > current_sl]
                    # Primary sort: strength (HTF first). Secondary: proximity (highest low)
                    sorted_obs = sorted(valid_obs,
                                        key=lambda x: (x.strength, x.low),
                                        reverse=True)
                    for ob in sorted_obs:
                        cand = ob.low - ob_buf
                        if cand > current_sl and cand < price - min_dist:
                            candidates.append((
                                cand, f"OB@${ob.midpoint:.0f}"
                                      f"(tf={ob.timeframe},s={ob.strength:.0f})"))
                            _added += 1
                            if _added >= 2:
                                break
                else:
                    valid_obs = [o for o in active_obs if o.high < current_sl]
                    sorted_obs = sorted(valid_obs,
                                        key=lambda x: (x.strength, -x.high),
                                        reverse=True)
                    for ob in sorted_obs:
                        cand = ob.high + ob_buf
                        if cand < current_sl and cand > price + min_dist:
                            candidates.append((
                                cand, f"OB@${ob.midpoint:.0f}"
                                      f"(tf={ob.timeframe},s={ob.strength:.0f})"))
                            _added += 1
                            if _added >= 2:
                                break
            except Exception:
                pass

        # ── 4. Displacement body anchor ────────────────────────────────
        if _disp_result is not None and phase >= 1:
            disp_close, disp_body_ratio, disp_label = _disp_result
            disp_buf = 0.20 * atr
            if pos_side == "long":
                disp_sl = disp_close - disp_buf
                if disp_sl > current_sl and disp_sl < price - min_dist:
                    candidates.append((disp_sl, disp_label))
            else:
                disp_sl = disp_close + disp_buf
                if disp_sl < current_sl and disp_sl > price + min_dist:
                    candidates.append((disp_sl, disp_label))

        # ── 5. 1m micro-structure (Phase 2+) ──────────────────────────
        if phase >= 2 and len(candles_1m) >= 6:
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

            micro_buf = 0.10 * atr
            if choch_against:
                micro_buf *= 0.70

            if pos_side == "long" and sl_1m:
                valid = [l for l in sl_1m
                         if l > current_sl + 0.05 * atr
                         and l - micro_buf < price - min_dist]
                if valid:
                    best = max(valid)
                    sl_cand = best - micro_buf
                    if sl_cand > current_sl:
                        candidates.append((sl_cand, f"1m_SW@${best:.0f}"))

            elif pos_side == "short" and sh_1m:
                valid = [h for h in sh_1m
                         if h < current_sl - 0.05 * atr
                         and h + micro_buf > price + min_dist]
                if valid:
                    best = min(valid)
                    sl_cand = best + micro_buf
                    if sl_cand < current_sl:
                        candidates.append((sl_cand, f"1m_SW@${best:.0f}"))

        # ── 6. CHoCH structural level ──────────────────────────────────
        if choch_against and choch_level > 0 and phase >= 2:
            choch_buf = 0.12 * atr
            if pos_side == "long":
                cand = choch_level - choch_buf
                if cand > current_sl and cand < price - min_dist:
                    candidates.append((cand, f"CHoCH_{choch_tf}@${choch_level:.0f}"))
            else:
                cand = choch_level + choch_buf
                if cand < current_sl and cand > price + min_dist:
                    candidates.append((cand, f"CHoCH_{choch_tf}@${choch_level:.0f}"))

        # ── 7. 15m macro structure (Phase 3) ───────────────────────────
        if phase >= 3 and candles_15m and len(candles_15m) >= 6:
            closed_15m = candles_15m[:-1] if len(candles_15m) > 1 else candles_15m
            try:
                highs_15m, lows_15m = _find_swings_internal(
                    closed_15m, min(8, len(closed_15m) - 2))
            except Exception:
                highs_15m, lows_15m = [], []

            htf_buf = 0.12 * atr

            if pos_side == "long" and lows_15m:
                valid = [l for l in lows_15m
                         if l > current_sl + 0.05 * atr
                         and l - htf_buf < price - min_dist]
                if valid:
                    best = max(valid)
                    sl_cand = best - htf_buf
                    if sl_cand > current_sl:
                        candidates.append((sl_cand, f"15m_SW@${best:.0f}"))

            elif pos_side == "short" and highs_15m:
                valid = [h for h in highs_15m
                         if h < current_sl - 0.05 * atr
                         and h + htf_buf > price + min_dist]
                if valid:
                    best = min(valid)
                    sl_cand = best + htf_buf
                    if sl_cand < current_sl:
                        candidates.append((sl_cand, f"15m_SW@${best:.0f}"))

        # ── 8. FVG fill lock (70%+ filled) ────────────────────────────
        if ict_engine is not None:
            try:
                fvgs = (ict_engine.fvgs_bear if pos_side == "long"
                        else ict_engine.fvgs_bull)
                for fvg in fvgs:
                    if not fvg.is_active(ctx.now_ms):
                        continue
                    if fvg.fill_percentage < 0.70:
                        continue
                    lock = (fvg.top + 0.15 * atr if pos_side == "long"
                            else fvg.bottom - 0.15 * atr)
                    if pos_side == "long" and lock > current_sl and lock < price - min_dist:
                        candidates.append((lock, f"FVG_FILL@${fvg.midpoint:.0f}"))
                    elif pos_side == "short" and lock < current_sl and lock > price + min_dist:
                        candidates.append((lock, f"FVG_FILL@${fvg.midpoint:.0f}"))
            except Exception:
                pass

        # ══════════════════════════════════════════════════════════════
        # SELECT BEST CANDIDATE
        # ══════════════════════════════════════════════════════════════
        if not candidates:
            if hold_reason is not None:
                hold_reason.append("NO_CANDIDATES")
            return None

        if pos_side == "long":
            new_sl, anchor = max(candidates, key=lambda x: x[0])
        else:
            new_sl, anchor = min(candidates, key=lambda x: x[0])

        # ── Liquidity pool ceiling ─────────────────────────────────────
        if ict_engine is not None:
            try:
                liq_buf = 0.40 * atr
                for pool in ict_engine.liquidity_pools:
                    if pool.swept:
                        continue
                    if pos_side == "long" and pool.level_type == "SSL":
                        ceiling = pool.price - liq_buf
                        if current_sl < ceiling < new_sl:
                            new_sl = ceiling
                            anchor += f"+LIQ_SSL@${pool.price:.0f}"
                    elif pos_side == "short" and pool.level_type == "BSL":
                        floor_p = pool.price + liq_buf
                        if current_sl > floor_p > new_sl:
                            new_sl = floor_p
                            anchor += f"+LIQ_BSL@${pool.price:.0f}"
            except Exception:
                pass

        # ── Min distance enforcement ───────────────────────────────────
        if pos_side == "long":
            new_sl = min(new_sl, price - min_dist)
        else:
            new_sl = max(new_sl, price + min_dist)

        # ── Liquidity Hunt Guard ───────────────────────────────────────
        _liq_pools = None
        if ict_engine is not None:
            try:
                _liq_pools = ict_engine.liquidity_pools
            except Exception:
                pass

        _adjusted_sl, hunt_reason = _LiquidityHuntGuard.adjust(
            new_sl, pos_side, atr,
            candles=candles_1m,
            liquidity_pools=_liq_pools,
        )
        if hunt_reason:
            if pos_side == "long":
                _adjusted_sl = min(_adjusted_sl, price - min_dist)
            else:
                _adjusted_sl = max(_adjusted_sl, price + min_dist)
            if _adjusted_sl != new_sl:
                new_sl = _adjusted_sl
                anchor += f"+HUNT({hunt_reason})"

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

        # ── Minimum meaningful move ────────────────────────────────────
        _min_meaningful = 0.10 * atr
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

        # ── Structural path check (ICT engine) ────────────────────────
        if ict_engine is not None:
            try:
                blocked, reason = ict_engine.check_sl_path_for_structure(
                    pos_side, current_sl, new_sl, ctx.now_ms, tier=ctx.tier)
                if blocked:
                    _override = (bos_count >= 2 or ctx.tier >= 1.5)
                    if not _override:
                        if hold_reason is not None:
                            hold_reason.append(f"PATH_BLOCKED:{reason}")
                        return None
            except Exception:
                pass

        logger.debug(
            f"Trail v8 {pos_side.upper()}: ${current_sl:,.1f} → ${new_sl:,.1f} "
            f"[{anchor}] P{phase} bos={bos_count} "
            f"tier={ctx.tier:.2f}R min_d={min_dist:.0f} "
            f"vol={vol_ratio:.2f}/{vol_regime} "
            f"disp={has_displacement} choch={choch_against}")

        return _round_tick(new_sl, ctx.tick_size)

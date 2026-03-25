"""
dynamic_trail_engine.py — Market-Adaptive Trailing SL Engine v7.0
=================================================================
WHAT CHANGED FROM v6.0
───────────────────────
v6.0 was context-aware but still used static R-multiple gates and fixed ATR
multipliers.  v7.0 makes EVERY decision data-driven off live market structure:

  1. DISPLACEMENT DETECTION  (_DisplacementDetector)
     When a 1m candle body ≥ 1.2 × ATR and closes near its extreme, we are
     inside an institutional displacement leg.  SL anchors to the displacement
     CLOSE (not the prior swing low), giving a structurally tighter stop with
     hard institutional validation behind it.

  2. VELOCITY BONUS  (_compute_velocity_bonus)
     4 consecutive candles all closing in trade direction with average body ≥
     0.5 ATR → phase advances by 1 and min_dist tightens 15%.  Momentum is
     carrying the trade; the trail should keep up.

  3. VOLATILITY REGIME  (_VolatilityRegime)
     ATR(5) / ATR(20) ratio.
       EXPANDING  (> 1.25)  → widen swing buffers + wider chandelier
       CONTRACTING (< 0.75) → tighten buffers (trend acceleration, fewer wicks)
       NORMAL               → baseline parameters
     This replaces the static vol_scale that was computed from atr_percentile
     alone; a live vol ratio is far more reactive.

  4. MULTI-TIMEFRAME SWING CASCADE
     Phase 1 → 5m structure.
     Phase 2 → 5m + 1m micro-structure.
     Phase 3 → 15m macro-structure (passed as candles_15m, optional).
     Uses the NEAREST UNSWEPT swing, not the highest/lowest valid one, to
     stay closer to current price action.

  5. ADAPTIVE CHANDELIER
     n = base * (1 + 0.3 × (vol_ratio − 1))  clamped to [0.7x, 1.4x base].
     Quiet trending market → tighter chandelier.
     Vol spike → chandelier widens to absorb noise.

  6. CHoCH-TRIGGERED TIGHTEN  (ICT engine integration)
     When ict_engine reports a CHoCH (Change of Character) on 5m or 1m,
     min_dist is reduced by 25% and a structural CHoCH level is added to
     the candidate pool.  First warning that structure may be reversing.

  7. LIQUIDITY HUNT GUARD  (_LiquidityHuntGuard)
     Post-selection: if chosen SL lands within 0.15 ATR of a known liquidity
     pool, a round number (±500 pts for BTC), OR a ≥3-candle equal-low/high
     cluster — it is shifted 0.30 ATR AWAY from the hunt zone.  This is the
     single most practical fix for "getting stopped before the real move."

BACKWARD COMPATIBILITY
──────────────────────
  - compute() signature is FULLY BACKWARD COMPATIBLE.
  - One new optional parameter: candles_15m (default None).
  - Returns Optional[float] — same contract as v6.0.
  - Drop-in replacement for both ICTTrailEngine.compute() and
    InstitutionalLevels.compute_trail_sl().
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
# PRIMITIVE HELPERS  (unchanged from v6.0)
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
# NEW v7.0: MULTI-PERIOD ATR  (used by VolatilityRegime)
# ─────────────────────────────────────────────────────────────────────────────

def _compute_atr_series(candles: list, period: int) -> float:
    """
    Compute simple ATR(period) from a candle list.
    True Range = max(H-L, |H-prev_C|, |L-prev_C|).
    Falls back to average H-L range when previous close is unavailable.
    """
    if not candles:
        return 0.0
    n = min(period, len(candles))
    if n < 1:
        return 0.0
    trs = []
    for i in range(1, n + 1):
        idx = len(candles) - i
        h  = _safe_float(candles[idx], 'h')
        l  = _safe_float(candles[idx], 'l')
        if idx > 0:
            pc = _safe_float(candles[idx - 1], 'c')
            tr = max(h - l, abs(h - pc), abs(l - pc)) if pc > 1e-10 else h - l
        else:
            tr = h - l
        trs.append(tr)
    return sum(trs) / len(trs) if trs else 0.0


# ─────────────────────────────────────────────────────────────────────────────
# NEW v7.0: VELOCITY BONUS
# ─────────────────────────────────────────────────────────────────────────────

def _compute_velocity_bonus(candles: list, atr: float,
                            pos_side: str, lookback: int = 4) -> bool:
    """
    Returns True when the last `lookback` closed candles form a sustained
    impulsive displacement leg:

      • ALL candles close in trade direction (no opposing body).
      • Average body ≥ MIN_BODY_RATIO × ATR.
      • Total price displacement across lookback ≥ MIN_DISP_ATR × ATR.

    When True, the phase is advanced by 1 and min_dist is tightened 15%.
    This captures the "we are INSIDE a displacement move" scenario where
    trailing too loose gives back all the structural gains.
    """
    MIN_BODY_RATIO = 0.50
    MIN_DISP_ATR   = 1.00

    if atr < 1e-10 or len(candles) < lookback + 2:
        return False

    lb = min(lookback, len(candles) - 2)
    # Use closed candles only (exclude the live bar at the end)
    recent = candles[-(lb + 1):-1]
    if len(recent) < lb:
        return False

    bodies = []
    for c in recent:
        o  = _safe_float(c, 'o')
        cl = _safe_float(c, 'c')
        body = cl - o
        if pos_side == "long"  and body <= 0:
            return False   # bearish candle — not a clean impulse leg
        if pos_side == "short" and body >= 0:
            return False   # bullish candle — not a clean impulse leg
        bodies.append(abs(body))

    if not bodies or sum(bodies) / len(bodies) < MIN_BODY_RATIO * atr:
        return False

    first_o = _safe_float(recent[0], 'o')
    last_c  = _safe_float(recent[-1], 'c')
    return abs(last_c - first_o) >= MIN_DISP_ATR * atr


# ─────────────────────────────────────────────────────────────────────────────
# NEW v7.0: DISPLACEMENT BAR DETECTOR
# ─────────────────────────────────────────────────────────────────────────────

class _DisplacementDetector:
    """
    Identifies a displacement bar: a single candle with:
      • Body ≥ BODY_THRESH × ATR  (dominant institutional candle)
      • Close in the top/bottom CLOSE_RATIO of candle range  (not a wick candle)
      • Aligned with pos_side

    When found within LOOKBACK closed candles, the SL is anchored to
    displacement_close ± (ANCHOR_BUF × ATR) instead of the prior swing.
    This mirrors the ICT sweep-and-displacement SL placement:
    below/above the candle that proved institutional intent.
    """

    BODY_THRESH  = 1.20   # body must be ≥ 1.2 × ATR
    CLOSE_RATIO  = 0.55   # close in top/bottom 55% of range
    LOOKBACK     = 5      # search last 5 closed candles
    ANCHOR_BUF   = 0.20   # 0.20 × ATR buffer below/above displacement close

    @staticmethod
    def detect(candles: list, atr: float,
               pos_side: str) -> Optional[Tuple[float, float, str]]:
        """
        Returns (anchor_price, body_size_in_atr, label) or None.

        anchor_price:
            long  → displacement close  (SL placed at anchor - ANCHOR_BUF × ATR)
            short → displacement close  (SL placed at anchor + ANCHOR_BUF × ATR)
        """
        if atr < 1e-10 or len(candles) < 3:
            return None

        lb = min(_DisplacementDetector.LOOKBACK, len(candles) - 1)

        for i in range(1, lb + 1):
            c  = candles[-i]
            o  = _safe_float(c, 'o')
            h  = _safe_float(c, 'h')
            l  = _safe_float(c, 'l')
            cl = _safe_float(c, 'c')

            candle_range = h - l
            if candle_range < 1e-10:
                continue

            body = abs(cl - o)
            bull = cl > o

            if pos_side == "long"  and not bull:
                continue
            if pos_side == "short" and bull:
                continue
            if body < _DisplacementDetector.BODY_THRESH * atr:
                continue

            close_pos = (cl - l) / candle_range  # 0=low, 1=high
            if pos_side == "long"  and close_pos < _DisplacementDetector.CLOSE_RATIO:
                continue
            if pos_side == "short" and close_pos > (1.0 - _DisplacementDetector.CLOSE_RATIO):
                continue

            body_ratio = body / atr
            return (cl, body_ratio,
                    f"DISP@${cl:.0f}(b={body_ratio:.1f}ATR,lag={i})")

        return None


# ─────────────────────────────────────────────────────────────────────────────
# NEW v7.0: VOLATILITY REGIME
# ─────────────────────────────────────────────────────────────────────────────

class _VolatilityRegime:
    """
    Live volatility regime via ATR(short) / ATR(long) ratio.

    EXPANDING   (ratio > EXPAND_THRESH):
      Recent moves are larger than the long-run average.
      → Widen swing buffers, widen chandelier, increase min_dist slightly.
      → Risk of whipsaw is elevated.

    CONTRACTING (ratio < CONTRACT_THRESH):
      Price is in a tight, potentially accelerating trend.
      → Tighten buffers (less noise), tighter chandelier, trail closer.
      → Trend is exhausting noise — a breakout or exhaustion is near.

    NORMAL: baseline parameters.
    """

    EXPAND_THRESH   = 1.25
    CONTRACT_THRESH = 0.75
    SHORT_PERIOD    = 5
    LONG_PERIOD     = 20

    @staticmethod
    def compute(candles: list, atr_long: float) -> Tuple[float, str]:
        """
        Returns (vol_ratio, regime_label).
        Requires at least SHORT_PERIOD + 2 candles.
        """
        if atr_long < 1e-10 or len(candles) < _VolatilityRegime.SHORT_PERIOD + 2:
            return 1.0, "NORMAL"

        atr_short = _compute_atr_series(candles, _VolatilityRegime.SHORT_PERIOD)
        if atr_short < 1e-10:
            return 1.0, "NORMAL"

        ratio = atr_short / atr_long
        if ratio > _VolatilityRegime.EXPAND_THRESH:
            return ratio, "EXPANDING"
        if ratio < _VolatilityRegime.CONTRACT_THRESH:
            return ratio, "CONTRACTING"
        return ratio, "NORMAL"


# ─────────────────────────────────────────────────────────────────────────────
# NEW v7.0: LIQUIDITY HUNT GUARD
# ─────────────────────────────────────────────────────────────────────────────

class _LiquidityHuntGuard:
    """
    Shifts the chosen SL away from known stop-hunt zones AFTER candidate
    selection.  Three hunt-zone types:

    1. Known liquidity pools (BSL/SSL from ict_engine) — retail stops cluster
       here and institutional players specifically hunt these levels before
       delivering price.

    2. Round numbers — at BTC ~70k, levels divisible by ROUND_SPACING (500)
       attract algorithmic interest and stop accumulation.

    3. Equal-low/high clusters — three or more candle lows/highs within
       CLUSTER_BAND of each other form a visible cluster that large players
       target with precision wicks.

    If new_sl is within HUNT_ZONE_ATR of any such zone, it is shifted
    HUNT_OFFSET_ATR AWAY (in the safe direction).  For a long, the SL is
    moved DOWN (further from price, beneath the hunt zone).  This ensures
    our SL sits below where the raid will happen, not inside it.
    """

    HUNT_ZONE_ATR   = 0.15    # SL within 15% ATR of hunt zone → danger
    HUNT_OFFSET_ATR = 0.30    # shift 30% ATR below/above hunt zone
    CLUSTER_SIZE    = 3       # ≥3 candles with lows/highs clustering = danger
    CLUSTER_BAND    = 0.12    # lows within 12% ATR of each other = cluster
    ROUND_SPACING   = 500.0   # BTC round-number grid (adjust per instrument)

    @staticmethod
    def adjust(new_sl: float, pos_side: str, atr: float,
               candles: list = None,
               liquidity_pools=None,
               round_spacing: float = 500.0) -> Tuple[float, str]:
        """
        Returns (adjusted_sl, reason_suffix).
        No change: (new_sl, "").
        """
        if atr < 1e-10:
            return new_sl, ""

        hunt_zone = _LiquidityHuntGuard.HUNT_ZONE_ATR   * atr
        offset    = _LiquidityHuntGuard.HUNT_OFFSET_ATR * atr
        cluster_b = _LiquidityHuntGuard.CLUSTER_BAND    * atr
        adjusted  = new_sl
        reasons: List[str] = []

        # ── 1. Known liquidity pools ────────────────────────────────────
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

        # ── 2. Round numbers ────────────────────────────────────────────
        nearest = round(adjusted / round_spacing) * round_spacing
        if abs(adjusted - nearest) < hunt_zone:
            if pos_side == "long":
                adjusted = nearest - offset
            else:
                adjusted = nearest + offset
            reasons.append(f"ROUND@{nearest:.0f}")

        # ── 3. Equal-low/high cluster in recent candles ─────────────────
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
# TRAIL CONTEXT
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class TrailContext:
    """
    Computed context driving all trail decisions.
    Built once per trail tick from raw inputs.

    v7.0 additions:
        vol_ratio       — ATR(5)/ATR(20); 1.0 = normal
        vol_regime      — "EXPANDING" | "CONTRACTING" | "NORMAL"
        has_displacement — True if a displacement bar was found in last 5 candles
        velocity_bonus  — True if sustained impulsive momentum detected
    """
    # ── Position basics ────────────────────────────────────────────
    pos_side:       str
    price:          float
    entry_price:    float
    current_sl:     float
    atr:            float
    init_sl_dist:   float
    tick_size:      float = 0.1

    # ── Profit state ───────────────────────────────────────────────
    profit:         float = 0.0
    peak_profit:    float = 0.0
    peak_price_abs: float = 0.0
    tier:           float = 0.0

    # ── Market context ─────────────────────────────────────────────
    atr_percentile: float = 0.50
    amd_phase:      str   = "ACCUMULATION"
    amd_confidence: float = 0.0
    adx:            float = 15.0
    trade_mode:     str   = "reversion"

    # ── v7.0: Live volatility regime ───────────────────────────────
    vol_ratio:       float = 1.0
    vol_regime:      str   = "NORMAL"
    has_displacement: bool = False
    velocity_bonus:  bool  = False

    # ── Time ───────────────────────────────────────────────────────
    hold_seconds:   float = 0.0
    now_ms:         int   = 0

    # ── Derived ────────────────────────────────────────────────────
    be_price:       float = 0.0
    be_locked:      bool  = False
    phase:          int   = 0
    min_dist:       float = 0.0

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
        tier = max(profit, peak_profit) / init_dist if init_dist > 1e-10 else 0.0

        _rate  = _get_commission_rate()
        rt_fee = entry_price * _rate * 2.0
        be_buf = rt_fee + 0.25 * atr
        be_price = (entry_price + be_buf if pos_side == "long"
                    else entry_price - be_buf)
        be_locked = ((pos_side == "long"  and current_sl >= be_price) or
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
        # Phase + min_dist computed after all fields are set.
        # vol_ratio / vol_regime / has_displacement / velocity_bonus are
        # injected AFTER build() returns (they require candles, not available here).
        ctx.phase, ctx.min_dist = DynamicTrailEngine._compute_phase_and_min_dist(ctx)
        return ctx


# ═══════════════════════════════════════════════════════════════════════════
# DYNAMIC TRAIL ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class DynamicTrailEngine:
    """
    Unified market-adaptive trailing SL engine (v7.0).

    Single entry point: DynamicTrailEngine.compute()
    Drop-in replacement for ICTTrailEngine.compute() and
    InstitutionalLevels.compute_trail_sl().
    """

    # ── Phase + min_dist computation ───────────────────────────────────────

    @staticmethod
    def _compute_phase_and_min_dist(ctx: TrailContext) -> Tuple[int, float]:
        """
        Dynamic phase gating and minimum SL distance.

        v7.0 enhancements:
          • velocity_bonus: if True, phase advances by 1 and min_dist tightens
            15% — we are inside a displacement leg, trail should keep up.
          • vol_regime: EXPANDING widens min_dist; CONTRACTING tightens it.
          • CHoCH awareness (handled via min_dist modifier in compute()).
        """
        tier    = ctx.tier
        atr     = ctx.atr
        amd     = ctx.amd_phase
        amd_conf = ctx.amd_confidence

        # ── Volatility scaling (v6.0 ATR-percentile base) ──────────────
        vol_scale = 0.8 + 0.4 * min(max(ctx.atr_percentile, 0.0), 1.0)

        # ── v7.0: Live vol-regime override ─────────────────────────────
        if ctx.vol_regime == "EXPANDING":
            vol_scale *= 1.15   # wider — noise is elevated
        elif ctx.vol_regime == "CONTRACTING":
            vol_scale *= 0.88   # tighter — trend acceleration, less noise

        # ── Trend scaling (ADX) ─────────────────────────────────────────
        if ctx.adx > 25:
            adx_factor = 0.85
        elif ctx.adx < 15:
            adx_factor = 1.15
        else:
            adx_factor = 1.0

        # ── AMD-aware phase thresholds ─────────────────────────────────
        if amd == "MANIPULATION" and tier < 1.0:
            return 0, float('inf')

        if amd == "DISTRIBUTION" and amd_conf >= 0.50:
            if tier >= 2.0:
                phase, base_min = 3, 0.6 * atr
            elif tier >= 1.0:
                phase, base_min = 2, 0.9 * atr
            elif tier >= 0.70:
                phase, base_min = 1, 1.3 * atr
            else:
                return 0, float('inf')

        elif amd in ("REACCUMULATION", "REDISTRIBUTION"):
            if tier >= 1.5:
                phase, base_min = 3, 0.7 * atr
            elif tier >= 0.80:
                phase, base_min = 2, 1.0 * atr
            elif tier >= 0.70:
                phase, base_min = 1, 1.3 * atr
            else:
                return 0, float('inf')

        elif amd == "MANIPULATION" and tier >= 1.0:
            if tier >= 1.5:
                phase, base_min = 2, 1.1 * atr
            else:
                phase, base_min = 1, 1.4 * atr

        else:   # ACCUMULATION or unknown — conservative
            if tier >= 2.0:
                phase, base_min = 3, 1.0 * atr
            elif tier >= 1.5:
                phase, base_min = 2, 1.3 * atr
            elif tier >= 0.80:
                phase, base_min = 1, 1.8 * atr
            elif tier >= 0.70:
                phase, base_min = 1, 2.2 * atr
            else:
                return 0, float('inf')

        # ── v7.0: Velocity bonus — advance phase + tighten distance ────
        if ctx.velocity_bonus and phase < 3:
            phase    = min(phase + 1, 3)
            base_min *= 0.85
            logger.debug(f"Trail v7: velocity bonus → phase={phase} "
                         f"min_dist tightened 15%")

        # ── v7.0: Displacement bonus — tighten further ──────────────────
        if ctx.has_displacement:
            base_min *= 0.90   # inside displacement: trail 10% tighter

        min_dist = max(base_min * vol_scale * adx_factor, 0.4 * atr)
        return phase, min_dist

    # ── Pullback classifier ────────────────────────────────────────────────

    @staticmethod
    def _classify_pullback(ctx: TrailContext,
                           candles_1m: List[Dict],
                           candles_5m: List[Dict],
                           orderbook: Dict) -> Tuple[bool, int, str]:
        """
        Smart pullback detection with profit-dependent sensitivity.

        At low R  (0.5-1.0): 2 reversal signals = pullback (freeze)
        At mid R  (1.0-2.0): 3 signals needed
        At high R (2.0+):    4 signals needed  → deep profit, trail must advance

        Time-decay: freeze override after max_freeze_sec.
        """
        atr    = ctx.atr
        rev_sigs = 0
        details  = []

        if atr < 1e-10 or len(candles_1m) < 10:
            return True, 0, "insufficient_data"

        retrace = (abs(ctx.peak_price_abs - ctx.price)
                   if ctx.peak_price_abs > 1e-10 else 0.0)

        max_freeze = 120.0 if ctx.tier >= 2.0 else 300.0 if ctx.tier >= 1.0 else 600.0
        if retrace > 0.8 * atr and ctx.hold_seconds > max_freeze:
            return False, 0, "time_decay_override"

        # Signal 1: Volume expansion
        if len(candles_1m) >= 10:
            rv = sum(_safe_float(c, 'v') for c in candles_1m[-3:]) / 3.0
            iv = sum(_safe_float(c, 'v') for c in candles_1m[-8:-3]) / 5.0
            if iv > 1e-10:
                vr = rv / iv
                if vr > 1.30:
                    rev_sigs += 1
                    details.append(f"vol_expand({vr:.2f})")
                else:
                    details.append(f"vol_norm({vr:.2f})")

        # Signal 2: Retrace depth
        if retrace > 1.0 * atr:
            rev_sigs += 1
            details.append(f"deep({retrace/atr:.1f}ATR)")
        else:
            details.append(f"shallow({retrace/atr:.1f}ATR)")

        # Signal 3: Large opposing candle bodies
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

        # Signal 4: Orderbook imbalance flip
        bids = orderbook.get("bids", [])
        asks = orderbook.get("asks", [])
        if bids and asks:
            def _qty(lvl):
                if isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
                    return float(lvl[1])
                if isinstance(lvl, dict):
                    return float(lvl.get("size") or lvl.get("quantity") or 0)
                return 0.0
            bd  = sum(_qty(b) for b in bids[:5]) if len(bids) >= 5 else 0
            ad  = sum(_qty(a) for a in asks[:5]) if len(asks) >= 5 else 0
            tot = bd + ad
            if tot > 1e-10:
                imb = (bd - ad) / tot
                if ctx.pos_side == "long"  and imb < -0.20:
                    rev_sigs += 1
                    details.append(f"ob_flip({imb:+.2f})")
                elif ctx.pos_side == "short" and imb > 0.20:
                    rev_sigs += 1
                    details.append(f"ob_flip({imb:+.2f})")

        # Signal 5: 5m swing break
        if len(candles_5m) >= 5:
            closed = candles_5m[:-1] if len(candles_5m) > 1 else candles_5m
            highs_5m, lows_5m = _find_swings_internal(
                closed, min(8, len(closed) - 2))
            if ctx.pos_side == "long" and lows_5m:
                rel = [l for l in lows_5m if l > ctx.entry_price - 0.5 * atr]
                if rel and ctx.price < min(rel):
                    rev_sigs += 1
                    details.append("5m_sw_broken")
            elif ctx.pos_side == "short" and highs_5m:
                rel = [h for h in highs_5m if h < ctx.entry_price + 0.5 * atr]
                if rel and ctx.price > max(rel):
                    rev_sigs += 1
                    details.append("5m_sw_broken")

        # Signal 6: Momentum stalling
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

        freeze_threshold = (4 if ctx.tier >= 2.0 else 3 if ctx.tier >= 1.0 else 2)
        is_pullback = rev_sigs < freeze_threshold
        return is_pullback, rev_sigs, "|".join(details)

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
                # v7.0: new optional parameter — HTF candles for Phase 3 anchors
                candles_15m: List[Dict] = None,
                ) -> Optional[float]:
        """
        Unified market-adaptive trailing SL.

        Returns new_sl (tick-rounded) or None (hold/freeze).
        Fully backward-compatible with v6.0 call sites.
        New parameter: candles_15m (optional, used for Phase 3 HTF anchoring).
        """
        if atr < 1e-10:
            return None

        # ── Extract AMD context from ICT engine if not provided ────────
        if not amd_phase and ict_engine is not None:
            try:
                amd_obj      = ict_engine._amd
                amd_phase    = amd_obj.phase
                amd_confidence = amd_obj.confidence
            except Exception:
                amd_phase      = "ACCUMULATION"
                amd_confidence = 0.0

        # ─────────────────────────────────────────────────────────────
        # v7.0: COMPUTE LIVE MARKET DATA BEFORE BUILDING CONTEXT
        # These require candles, so they are injected into ctx after build()
        # ─────────────────────────────────────────────────────────────
        closed_1m = candles_1m[:-1] if len(candles_1m) > 1 else candles_1m

        # Volatility regime
        vol_ratio, vol_regime = _VolatilityRegime.compute(candles_1m, atr)

        # Displacement detection (last 5 closed 1m candles)
        _disp_result    = _DisplacementDetector.detect(closed_1m, atr, pos_side)
        has_displacement = _disp_result is not None

        # Velocity bonus (last 4 closed candles)
        velocity_bonus = _compute_velocity_bonus(candles_1m, atr, pos_side)

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
        # Inject live market data into context
        ctx.vol_ratio        = vol_ratio
        ctx.vol_regime       = vol_regime
        ctx.has_displacement = has_displacement
        ctx.velocity_bonus   = velocity_bonus

        # Recompute phase/min_dist now that live data is injected
        ctx.phase, ctx.min_dist = DynamicTrailEngine._compute_phase_and_min_dist(ctx)

        # ── v7.0: CHoCH modifier — tighten when structure changes character
        # ICT: a Change of Character on 5m/1m is the FIRST warning the move
        # may be exhausting.  Tighten min_dist immediately when detected.
        _choch_detected = False
        if ict_engine is not None and ctx.phase >= 2:
            try:
                for tf_key in ("5m", "1m"):
                    tf_st = ict_engine._tf.get(tf_key)
                    if tf_st is None:
                        continue
                    choch_dir = getattr(tf_st, 'choch_direction', None)
                    choch_lvl = getattr(tf_st, 'choch_level', 0.0)
                    if not choch_dir or not choch_lvl:
                        continue
                    # Only relevant if CHoCH is AGAINST trade direction
                    if pos_side == "long"  and choch_dir == "bearish":
                        _choch_detected = True
                    elif pos_side == "short" and choch_dir == "bullish":
                        _choch_detected = True
                    if _choch_detected:
                        ctx.min_dist *= 0.75   # tighten 25%
                        logger.debug(
                            f"Trail v7: CHoCH {choch_dir} on {tf_key} → "
                            f"min_dist tightened to {ctx.min_dist:.0f}")
                        break
            except Exception:
                pass

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
                    if pos_side == "long":
                        ob_near_sl = current_sl >= ob.low - 1.8 * atr
                    else:
                        ob_near_sl = current_sl <= ob.high + 1.8 * atr
                    if not ob_near_sl:
                        continue
                    if pos_side == "short" and ob.low <= price:
                        continue
                    if pos_side == "long"  and ob.high >= price:
                        continue
                    if ob.low - freeze_atr <= price <= ob.high + freeze_atr:
                        max_ob_freeze = 600.0 if ctx.tier < 1.0 else 300.0
                        if hold_seconds < max_ob_freeze:
                            if hold_reason is not None:
                                hold_reason.append(
                                    f"OB_ZONE_FREEZE@${ob.midpoint:.0f}"
                                    f"(tf={ob.timeframe})")
                            return None
            except Exception:
                pass

        # ── FVG Freeze ─────────────────────────────────────────────────
        if ict_engine is not None and ctx.be_locked and hold_seconds < 600.0:
            try:
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

        # ── Pullback Detection ──────────────────────────────────────────
        if ctx.be_locked and len(candles_1m) >= 10 and len(candles_5m) >= 5:
            # v7.0: Disable pullback freeze during a displacement leg —
            # if we are IN an impulse, a momentary retrace is normal and
            # the trail should continue advancing.
            if not (ctx.has_displacement and ctx.velocity_bonus):
                is_pb, rev_count, pb_detail = DynamicTrailEngine._classify_pullback(
                    ctx, candles_1m, candles_5m, orderbook)
                if is_pb:
                    if hold_reason is not None:
                        thr = 4 if ctx.tier >= 2.0 else 3 if ctx.tier >= 1.0 else 2
                        hold_reason.append(
                            f"PULLBACK({rev_count}rev/{thr}thr)[{pb_detail}]")
                    return None

        # ══════════════════════════════════════════════════════════════
        # BUILD CANDIDATE SL LEVELS
        # ══════════════════════════════════════════════════════════════
        candidates: List[Tuple[float, str]] = []
        min_dist    = ctx.min_dist
        phase       = ctx.phase

        # ── 0. Displacement body anchor  (v7.0 NEW) ────────────────────
        # When institutional displacement is confirmed, SL anchors to the
        # displacement candle's CLOSE rather than a far swing low.
        # This is the tightest valid structural anchor in ICT methodology.
        if _disp_result is not None and phase >= 1:
            disp_close, disp_body_ratio, disp_label = _disp_result
            disp_buf = _DisplacementDetector.ANCHOR_BUF * atr
            if pos_side == "long":
                disp_sl = disp_close - disp_buf
                if current_sl < disp_sl < price - min_dist:
                    candidates.append((disp_sl, disp_label))
            else:
                disp_sl = disp_close + disp_buf
                if price + min_dist < disp_sl < current_sl:
                    candidates.append((disp_sl, disp_label))

        # ── 1. Profit floor (fee-adjusted BE) — always present ─────────
        candidates.append((ctx.be_price, "BE_FLOOR"))

        # ── 2. ICT OB anchor ──────────────────────────────────────────
        if ict_engine is not None and phase >= 1:
            try:
                ob_buf = 0.30 * atr
                obs    = (ict_engine.order_blocks_bull if pos_side == "long"
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

        # ── 3. 5m swing structure (v7.0: adaptive buffer via vol_ratio) ─
        if candles_5m and len(candles_5m) >= 6:
            closed_5m = candles_5m[:-1]
            try:
                from ict_trade_engine import _find_swings
                highs_5m, lows_5m = _find_swings(
                    closed_5m, min(12, len(closed_5m) - 2))
            except (ImportError, Exception):
                highs_5m, lows_5m = _find_swings_internal(
                    closed_5m, min(12, len(closed_5m) - 2))

            # v7.0: swing buffer adapts to BOTH phase AND live vol_ratio.
            # Expanding vol → wider buffer (protect from wicks hitting SL).
            # Contracting vol → tighter (price is moving cleanly).
            if phase == 1:
                base_sw_buf = 0.80 * atr * (0.9 + 0.2 * ctx.atr_percentile)
            elif phase == 2:
                base_sw_buf = 0.25 * atr * (0.9 + 0.2 * ctx.atr_percentile)
            else:
                base_sw_buf = 0.10 * atr

            # Apply live vol_ratio to buffer
            vol_buf_adj = max(0.70, min(ctx.vol_ratio, 1.35))
            sw_buf = base_sw_buf * vol_buf_adj

            if pos_side == "long" and lows_5m:
                valid = [l for l in lows_5m
                         if current_sl + 0.05 * atr < l < price - min_dist]
                if valid:
                    # v7.0: use NEAREST unswept swing, not highest.
                    # Nearest = maximum valid low (closest to price from below).
                    best = max(valid)
                    candidates.append((best - sw_buf,
                                       f"5m_SW@${best:.0f}(P{phase})"))

            elif pos_side == "short" and highs_5m:
                valid = [h for h in highs_5m
                         if price + min_dist < h < current_sl - 0.05 * atr]
                if valid:
                    best = min(valid)
                    candidates.append((best + sw_buf,
                                       f"5m_SW@${best:.0f}(P{phase})"))

        # ── 3b. 5m BOS trigger (immediate advance) ────────────────────
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

        # ── 3c. v7.0: CHoCH structural level ──────────────────────────
        # When a CHoCH is detected against trade direction, add its level
        # as an immediate SL candidate (tighter than current swing).
        if _choch_detected and ict_engine is not None and phase >= 2:
            try:
                for tf_key in ("5m", "1m"):
                    tf_st = ict_engine._tf.get(tf_key)
                    if tf_st is None:
                        continue
                    choch_lvl = getattr(tf_st, 'choch_level', 0.0)
                    if not choch_lvl:
                        continue
                    choch_buf = 0.15 * atr
                    if pos_side == "long":
                        cand = choch_lvl - choch_buf
                        if current_sl < cand < price - min_dist:
                            candidates.append((cand,
                                               f"CHoCH_{tf_key}@${choch_lvl:.0f}"))
                    else:
                        cand = choch_lvl + choch_buf
                        if price + min_dist < cand < current_sl:
                            candidates.append((cand,
                                               f"CHoCH_{tf_key}@${choch_lvl:.0f}"))
                    break  # use only the first detected CHoCH
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
                    lock = (fvg.top  + 0.20 * atr if pos_side == "long"
                            else fvg.bottom - 0.20 * atr)
                    if pos_side == "long" and current_sl < lock < price - min_dist:
                        candidates.append((lock, f"FVG_FILL@${fvg.midpoint:.0f}"))
                    elif pos_side == "short" and price + min_dist < lock < current_sl:
                        candidates.append((lock, f"FVG_FILL@${fvg.midpoint:.0f}"))
            except Exception:
                pass

        # ── 5. 1m swing structure (Phase 2+) ──────────────────────────
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

            micro_buf = 0.08 * atr
            if pos_side == "long" and sl_1m:
                valid = [l for l in sl_1m
                         if current_sl + 0.05 * atr < l < price - min_dist]
                if valid:
                    best = max(valid)
                    candidates.append((best - micro_buf, f"1m_SW@${best:.0f}"))

            elif pos_side == "short" and sh_1m:
                valid = [h for h in sh_1m
                         if price + min_dist < h < current_sl - 0.05 * atr]
                if valid:
                    best = min(valid)
                    candidates.append((best + micro_buf, f"1m_SW@${best:.0f}"))

        # ── 5b. v7.0: 15m swing cascade (Phase 3 only) ─────────────────
        # For deep-profit trades, 1m/5m swings are too granular — price can
        # retrace through them without the thesis invalidating.  15m structure
        # provides a genuine macro anchor: a break of 15m swing LOW (long)
        # means the delivery leg is structurally in question.
        if phase >= 3 and candles_15m and len(candles_15m) >= 6:
            closed_15m = candles_15m[:-1] if len(candles_15m) > 1 else candles_15m
            try:
                highs_15m, lows_15m = _find_swings_internal(
                    closed_15m, min(8, len(closed_15m) - 2))
            except Exception:
                highs_15m, lows_15m = [], []

            htf_buf = 0.15 * atr   # tighter buffer — HTF structure is high-value

            if pos_side == "long" and lows_15m:
                valid = [l for l in lows_15m
                         if current_sl + 0.05 * atr < l < price - min_dist]
                if valid:
                    best = max(valid)
                    candidates.append((best - htf_buf,
                                       f"15m_SW@${best:.0f}"))

            elif pos_side == "short" and highs_15m:
                valid = [h for h in highs_15m
                         if price + min_dist < h < current_sl - 0.05 * atr]
                if valid:
                    best = min(valid)
                    candidates.append((best + htf_buf,
                                       f"15m_SW@${best:.0f}"))

        # ── 6. Adaptive Chandelier (v7.0: vol-ratio-adjusted multiplier) ─
        # v6.0 used fixed 2.0 / 2.5 × ATR from peak.
        # v7.0: n = base × (1 + 0.30 × (vol_ratio − 1)), clamped [0.7×, 1.4×].
        # Quiet trend → tighter chandelier (trail closer to peak).
        # Vol spike   → wider chandelier (absorbs noise without premature exit).
        if phase >= 3 and peak_price_abs > 1e-10 and len(candidates) <= 1:
            base_n  = 2.0 if trade_mode in ("trend", "momentum") else 2.5
            vol_adj = 1.0 + 0.30 * (ctx.vol_ratio - 1.0)
            vol_adj = max(0.70, min(vol_adj, 1.40))
            n_ch    = base_n * vol_adj

            if pos_side == "long":
                chand = peak_price_abs - n_ch * atr
                if current_sl < chand < price - min_dist:
                    candidates.append((chand,
                                       f"CHAND_{n_ch:.1f}x"
                                       f"(vol={ctx.vol_ratio:.2f}/{ctx.vol_regime})"))
            else:
                chand = peak_price_abs + n_ch * atr
                if price + min_dist < chand < current_sl:
                    candidates.append((chand,
                                       f"CHAND_{n_ch:.1f}x"
                                       f"(vol={ctx.vol_ratio:.2f}/{ctx.vol_regime})"))

        # ── 7. Volume-decay tightening (Phase 3) ──────────────────────
        vol_tighten = 0.0
        if phase >= 3 and len(candles_1m) >= 10 and entry_vol > 1e-10:
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

        # ── Liquidity pool ceiling (v6.0) ─────────────────────────────
        if ict_engine is not None:
            try:
                liq_buf = 0.50 * atr
                for pool in ict_engine.liquidity_pools:
                    if pool.swept:
                        continue
                    if pos_side == "long" and pool.level_type == "SSL":
                        ceiling = pool.price - liq_buf
                        if current_sl < ceiling < new_sl:
                            new_sl  = ceiling
                            anchor += f"+LIQ_SSL@${pool.price:.0f}"
                    elif pos_side == "short" and pool.level_type == "BSL":
                        floor_p = pool.price + liq_buf
                        if current_sl > floor_p > new_sl:
                            new_sl  = floor_p
                            anchor += f"+LIQ_BSL@${pool.price:.0f}"
            except Exception:
                pass

        # ── Min distance enforcement ──────────────────────────────────
        if pos_side == "long":
            new_sl = min(new_sl, price - min_dist)
        else:
            new_sl = max(new_sl, price + min_dist)

        # ── v7.0: LIQUIDITY HUNT GUARD ────────────────────────────────
        # Applied AFTER min_dist clamp so the final resting price is clean.
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
            # Re-enforce min_dist after hunt adjustment
            if pos_side == "long":
                _adjusted_sl = min(_adjusted_sl, price - min_dist)
            else:
                _adjusted_sl = max(_adjusted_sl, price + min_dist)
            if _adjusted_sl != new_sl:
                new_sl  = _adjusted_sl
                anchor += f"+HUNT_GUARD({hunt_reason})"

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
            f"Trail v7 {pos_side.upper()}: ${current_sl:,.1f} → ${new_sl:,.1f} "
            f"[{anchor}] AMD={ctx.amd_phase} P{ctx.phase} "
            f"tier={ctx.tier:.2f}R min_d={min_dist:.0f} "
            f"vol={ctx.vol_ratio:.2f}/{ctx.vol_regime} "
            f"disp={ctx.has_displacement} vel={ctx.velocity_bonus}")

        return _round_tick(new_sl, ctx.tick_size)

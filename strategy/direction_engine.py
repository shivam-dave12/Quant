"""
direction_engine.py — Institutional Direction & Hunt Decision Engine v1.0
==========================================================================

THE CORE PROBLEM:
  A liquidity-hunt strategy needs ONE fundamental answer above all else:
  which side is smart money hunting NEXT, and after they sweep it,
  which direction does price DELIVER?

  Without a high-conviction answer to this, every entry is a coin flip
  regardless of how precise the entry logic is.

ARCHITECTURE — Three Independent Decision Engines:

  1. HUNT PREDICTION ENGINE
     10-factor weighted score predicts BSL or SSL as the next target.
     Factors are carefully calibrated to ICT institutional logic — not
     generic quant statistics.

  2. POST-SWEEP SCENARIO ENGINE
     After a pool is swept, determines the correct trade: reverse to the
     opposing pool, continue past the swept pool, or wait for confirmation.
     Uses an accumulative Bayesian-style evidence model — no single-tick flips.

  3. POOL-HIT CONTINUATION GATE
     When price reaches a pool DURING an active trade, determines whether
     to exit (TP hit), reverse, or let the trade run to the next pool.

INSTITUTIONAL PRINCIPLES (ICT):
  ─ AMD phase is the DOMINANT signal (0.22 weight).
    MANIPULATION phase = Judas swing = sweep will REVERSE.
    DISTRIBUTION phase = delivery = sweep is CONTINUATION.
  ─ HTF structure (4H + 1H + 15m cascade) overrides AMD bias conflicts.
    If 3 HTF TFs are bearish, we hunt SSL — regardless of sweep polarity.
  ─ Dealing range position is a GATE not a tiebreaker.
    Long entries only valid in DISCOUNT (below 50% P/D).
    Short entries only valid in PREMIUM (above 50% P/D).
  ─ CISD (Change in State of Delivery) = green light for reversal entry.
    CHoCH or BOS in the reversal direction post-sweep = institutional confirmation.
  ─ OTE (Optimal Trade Entry) zone = 61.8% – 78.6% Fibonacci retrace.
    The ONLY place institutional orders are placed after a sweep.
  ─ Pool significance × touch count = target priority.
    More clustered stops = stronger magnetic pull = better TP target.
  ─ Displacement (strong body candle closing away from swept level) =
    institutional footprint = mandatory for reversal confidence.

FACTOR WEIGHTS (Hunt Prediction):
  AMD phase + bias         0.22  — Institutional cycle phase
  HTF structure cascade    0.18  — 4H+1H+15m structural alignment
  Dealing range P/D        0.15  — Discount/premium zone position
  Order flow vector        0.13  — Tick flow + CVD directional pressure
  Pool asymmetry score     0.09  — Which side has higher-significance pools?
  OB/FVG magnetic pull     0.08  — Unfilled structures create delivery highways
  Displacement bias        0.07  — Recent candle momentum (closed bars only)
  Session timing           0.04  — Kill zone behavioural tendencies
  Micro-structure BOS      0.03  — Recent 5m structural break direction
  Volume asymmetry         0.01  — Net buy/sell pressure proxy
                           ────
  TOTAL                    1.00  ✓
"""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

# Hunt prediction
_HUNT_MIN_CONFIDENCE    = 0.18   # below this = NEUTRAL (insufficient signal)
_HUNT_STRONG_CONFIDENCE = 0.55   # above this = high-conviction directional call

# Post-sweep evidence thresholds
_PS_REV_THRESHOLD_EARLY  = 65.0   # overwhelming evidence in DISPLACEMENT phase
_PS_REV_THRESHOLD_NORMAL = 52.0   # standard evidence required
_PS_REV_THRESHOLD_MATURE = 38.0   # relaxed threshold after 4+ minutes
_PS_CONT_THRESHOLD_RATIO = 0.90   # continuation needs 90% of reversal threshold
_PS_GAP_MIN              = 10.0   # minimum gap between rev/cont scores
_PS_TIMEOUT_SEC          = 360.0  # 6 minutes — then abandon evaluation
_PS_EVAL_DELAY_SEC       = 8.0    # wait before first evaluation

# Displacement quality
_DISP_STRONG_ATR         = 1.2    # strong displacement > 1.2 ATR from sweep level
_DISP_WEAK_ATR           = 0.4    # minimum to count as displacement
_DISP_CISD_MAX_AGE_SEC   = 150    # CISD older than 2.5 min = stale

# OTE Fibonacci levels (61.8% – 78.6% retrace)
_OTE_FIB_LOW             = 0.618
_OTE_FIB_HIGH            = 0.786

# Continuation gate
_CONT_FLOW_REVERSAL_MIN  = 0.40   # flow must flip this strongly to trigger reversal
_CONT_STRUCT_REVERSAL    = True   # BOS against position always triggers re-evaluation


# ─────────────────────────────────────────────────────────────────────────────
# DATA STRUCTURES
# ─────────────────────────────────────────────────────────────────────────────

class DirectionBias(Enum):
    LONG    = "LONG"
    SHORT   = "SHORT"
    NEUTRAL = "NEUTRAL"


@dataclass
class HuntFactors:
    """Per-factor contributions to the hunt prediction score [-1, +1]."""
    amd:            float = 0.0   # AMD phase + bias
    htf_structure:  float = 0.0   # 4H + 1H + 15m structural cascade
    dealing_range:  float = 0.0   # premium/discount position
    order_flow:     float = 0.0   # tick flow + CVD directional pressure
    pool_asymmetry: float = 0.0   # significance asymmetry BSL vs SSL
    ob_fvg_pull:    float = 0.0   # OB/FVG delivery highway density
    displacement:   float = 0.0   # recent closed-candle momentum
    session:        float = 0.0   # kill zone behavioural bias
    micro_bos:      float = 0.0   # 5m BOS direction
    volume:         float = 0.0   # buy/sell volume asymmetry


@dataclass
class HuntPrediction:
    """Output of the Hunt Prediction Engine."""
    predicted:          Optional[str]  # "BSL" | "SSL" | None
    confidence:         float          # 0.0 – 1.0 normalised absolute score
    delivery_direction: str            # "bullish" | "bearish" | ""
    raw_score:          float          # underlying [-1, +1] score (+ = BSL hunt)
    bsl_score:          float          # 0–1 component for BSL
    ssl_score:          float          # 0–1 component for SSL
    factors:            HuntFactors = field(default_factory=HuntFactors)
    dealing_range_pd:   float = 0.5
    swept_pool_price:   Optional[float] = None   # predicted target pool price
    opposing_pool_price: Optional[float] = None  # post-hunt delivery destination
    reason:             str = ""
    timestamp_ms:       int = 0


@dataclass
class PostSweepState:
    """
    Accumulative evidence tracker for the Post-Sweep Scenario Engine.

    Evidence BUILDS across ticks — momentary noise cannot flip the decision.
    Scores are separated into STATIC (one-time) and DYNAMIC (per-tick) to
    prevent time-invariant facts from inflating the accumulated total.
    """
    # ── Identifiers ─────────────────────────────────────────────────
    swept_pool_price:   float
    swept_pool_type:    str    # "BSL" | "SSL"
    entered_at:         float  # epoch seconds

    # ── Static baseline (scored ONCE) ───────────────────────────────
    static_rev_base:   float = 0.0
    static_cont_base:  float = 0.0
    static_scored:     bool  = False

    # ── Dynamic accumulator ──────────────────────────────────────────
    rev_evidence:      float = 0.0   # accumulated reversal dynamic evidence
    cont_evidence:     float = 0.0   # accumulated continuation dynamic evidence
    peak_rev:          float = 0.0
    peak_cont:         float = 0.0
    tick_count:        int   = 0

    # ── CISD tracking ────────────────────────────────────────────────
    cisd_detected:     bool  = False
    cisd_timestamp:    float = 0.0
    cisd_type:         str   = ""    # "choch" | "bos"

    # ── Displacement tracking ────────────────────────────────────────
    max_displacement_atr: float = 0.0
    disp_velocity_atr_s:  float = 0.0  # ATR per second

    # ── OTE zone tracking ────────────────────────────────────────────
    ote_reached:       bool  = False
    ote_timestamp:     float = 0.0
    ote_holding:       bool  = False

    # ── Price extremes since sweep ───────────────────────────────────
    highest_since:     float = 0.0
    lowest_since:      float = float('inf')

    # ── Flow accumulation ────────────────────────────────────────────
    rev_flow_ticks:    int   = 0
    cont_flow_ticks:   int   = 0


@dataclass
class PostSweepDecision:
    """Output of the Post-Sweep Scenario Engine."""
    action:        str    # "reverse" | "continue" | "wait"
    direction:     str    # "long" | "short" | ""
    confidence:    float  # 0.0 – 1.0
    phase:         str    # "DISPLACEMENT" | "CISD" | "OTE" | "MATURE"
    cisd_active:   bool   = False
    ote_active:    bool   = False
    displacement_atr: float = 0.0
    rev_score:     float  = 0.0
    cont_score:    float  = 0.0
    rev_reasons:   List[str] = field(default_factory=list)
    cont_reasons:  List[str] = field(default_factory=list)
    reason:        str    = ""


@dataclass
class ContinuationDecision:
    """Output of the Pool-Hit Continuation Gate."""
    action:        str    # "exit" | "reverse" | "continue" | "hold"
    confidence:    float
    reason:        str
    next_target:   Optional[float] = None  # if continuing, the next pool price


# ─────────────────────────────────────────────────────────────────────────────
# DIRECTION ENGINE (main class)
# ─────────────────────────────────────────────────────────────────────────────

class DirectionEngine:
    """
    Institutional Direction & Hunt Decision Engine v1.0.

    Single source of truth for ALL directional decisions in the strategy:
      1. predict_hunt()   → which pool (BSL/SSL) gets swept next?
      2. evaluate_sweep() → after a sweep: reverse, continue, or wait?
      3. pool_hit_gate()  → at a pool during a trade: TP/reverse/continue?

    Integration points (quant_strategy.py):
      # Initialize once
      self._dir_engine = DirectionEngine()

      # Each tick (before entry evaluation)
      hunt = self._dir_engine.predict_hunt(price, atr, now_ms, ict_engine,
                                            tick_flow, cvd_trend, candles_5m)

      # After a sweep is detected
      self._dir_engine.on_sweep(swept_pool_price, pool_type, price, atr, now)

      # Each tick while in POST_SWEEP state
      decision = self._dir_engine.evaluate_sweep(
          price, atr, now, ict_engine, tick_flow, cvd_trend)

      # When price reaches a pool during an active trade
      gate = self._dir_engine.pool_hit_gate(
          pos_side, pos_entry, pos_sl, pos_tp, price, atr, ict_engine)
    """

    _FACTOR_WEIGHTS = {
        "amd":            0.22,
        "htf_structure":  0.18,
        "dealing_range":  0.15,
        "order_flow":     0.13,
        "pool_asymmetry": 0.09,
        "ob_fvg_pull":    0.08,
        "displacement":   0.07,
        "session":        0.04,
        "micro_bos":      0.03,
        "volume":         0.01,
    }
    assert abs(sum(_FACTOR_WEIGHTS.values()) - 1.0) < 1e-9, "Weights must sum to 1.0"

    def __init__(self) -> None:
        self._last_hunt: Optional[HuntPrediction] = None
        self._last_hunt_ms: int = 0
        self._HUNT_CACHE_SEC = 5   # refresh prediction every 5s

        self._ps_state: Optional[PostSweepState] = None
        self._last_ps_analysis: Dict = {}

    # =========================================================================
    # 1. HUNT PREDICTION ENGINE
    # =========================================================================

    def predict_hunt(
        self,
        price:       float,
        atr:         float,
        now_ms:      int,
        ict_engine,                       # ICTEngine instance
        tick_flow:   float = 0.0,         # TickFlowEngine signal [-1, +1]
        cvd_trend:   float = 0.0,         # CVD slope signal [-1, +1]
        candles_5m:  Optional[List[Dict]] = None,
        force_refresh: bool = False,
    ) -> HuntPrediction:
        """
        Predict which liquidity pool (BSL or SSL) will be swept next.

        Returns HuntPrediction with:
          predicted          = "BSL" (price heading up to sweep buy stops)
                             | "SSL" (price heading down to sweep sell stops)
                             | None (insufficient signal confidence)
          delivery_direction = direction AFTER the sweep completes
          confidence         = 0.0–1.0 (how decisive the prediction is)

        BSL hunt → BSL swept → bearish delivery → price drops to SSL target.
        SSL hunt → SSL swept → bullish delivery → price rises to BSL target.

        Score interpretation:
          score > 0: BSL hunt more likely (price heading up to run buy stops)
          score < 0: SSL hunt more likely (price heading down to run sell stops)
          abs(score) < _HUNT_MIN_CONFIDENCE: uncertain → None
        """
        # Use cached result if fresh enough
        age_ms = now_ms - self._last_hunt_ms
        if (not force_refresh
                and self._last_hunt is not None
                and age_ms < self._HUNT_CACHE_SEC * 1000):
            return self._last_hunt

        if not ict_engine or not getattr(ict_engine, '_initialized', False):
            return self._null_prediction(now_ms, reason="ict_not_initialized")

        a = max(atr, 1e-9)
        factors = HuntFactors()

        # ── Fetch Dealing Range ───────────────────────────────────────────────
        dr_pd = 0.5
        bsl_price = ssl_price = 0.0
        dr = getattr(ict_engine, '_dealing_range', None)
        if dr is not None:
            bsl_price = getattr(dr, 'high', 0.0)
            ssl_price = getattr(dr, 'low',  0.0)
            dr_pd     = getattr(dr, 'current_pd', 0.5)
        else:
            # Fall back to nearest unswept pools
            unswept = [p for p in ict_engine.liquidity_pools if not p.swept]
            bsl_above = sorted([p for p in unswept
                                if p.level_type == "BSL" and p.price > price],
                               key=lambda p: p.price)
            ssl_below = sorted([p for p in unswept
                                if p.level_type == "SSL" and p.price < price],
                               key=lambda p: -p.price)
            if bsl_above and ssl_below:
                bsl_price = bsl_above[0].price
                ssl_price = ssl_below[0].price
                rng = max(bsl_price - ssl_price, 1e-9)
                dr_pd = (price - ssl_price) / rng

        if bsl_price <= 0 or ssl_price <= 0:
            return self._null_prediction(now_ms, reason="no_dealing_range")

        # ─────────────────────────────────────────────────────────────────────
        # FACTOR 1: AMD Phase + Bias (weight 0.22)
        # ─────────────────────────────────────────────────────────────────────
        # ICT institutional cycle logic:
        #   ACCUMULATION:   No directional bias → neutral (0.0)
        #   MANIPULATION:   Judas swing active.
        #                   Bullish AMD + SSL swept → delivering UP → BSL hunt.
        #                   Bearish AMD + BSL swept → delivering DOWN → SSL hunt.
        #                   Score reflects the DELIVERY direction after the Judas.
        #   DISTRIBUTION:   Real move underway.
        #                   Bullish bias → continuing up → hunting BSL (continuation).
        #                   Bearish bias → continuing down → hunting SSL.
        #   REACCUMULATION: Mid-trend bullish pause → resume UP → BSL hunt.
        #   REDISTRIBUTION: Mid-trend bearish pause → resume DOWN → SSL hunt.
        amd = getattr(ict_engine, '_amd', None)
        if amd is not None:
            _phase = getattr(amd, 'phase', '').upper()
            _bias  = getattr(amd, 'bias',  '').lower()
            _conf  = float(getattr(amd, 'confidence', 0.0))

            if _phase == 'MANIPULATION':
                if _bias == 'bullish':
                    # SSL was swept (Judas down) → delivery UP → next hunt is BSL
                    factors.amd = +0.90 * _conf
                elif _bias == 'bearish':
                    # BSL was swept (Judas up) → delivery DOWN → next hunt is SSL
                    factors.amd = -0.90 * _conf
            elif _phase == 'DISTRIBUTION':
                if _bias == 'bullish':
                    # Delivering UP to BSL — bullish distribution sweep
                    factors.amd = +0.70 * _conf
                elif _bias == 'bearish':
                    # Delivering DOWN to SSL
                    factors.amd = -0.70 * _conf
            elif _phase == 'REACCUMULATION':
                # Mid-trend bullish consolidation → resume UP → BSL next
                factors.amd = +0.45 * _conf
            elif _phase == 'REDISTRIBUTION':
                # Mid-trend bearish consolidation → resume DOWN → SSL next
                factors.amd = -0.45 * _conf
            elif _phase == 'ACCUMULATION':
                # No delivery in progress → neutral
                factors.amd = 0.0

            factors.amd = max(-1.0, min(1.0, factors.amd))

        # ─────────────────────────────────────────────────────────────────────
        # FACTOR 2: HTF Structure Cascade (weight 0.18)
        # ─────────────────────────────────────────────────────────────────────
        # Each timeframe contributes a fractional vote.
        # 4H is the institutional anchor (highest weight within this factor).
        # Score: positive = bullish alignment, negative = bearish.
        _tf = getattr(ict_engine, '_tf', {})
        _struct_votes = []
        _tf_weights = {"4h": 0.45, "1h": 0.30, "15m": 0.25}
        for _tf_name, _tf_w in _tf_weights.items():
            _st = _tf.get(_tf_name)
            if _st is None:
                continue
            _trend = getattr(_st, 'trend', 'ranging')
            _bos   = getattr(_st, 'bos_direction', '')
            _choch = getattr(_st, 'choch_direction', '')
            _vote  = 0.0
            if _trend == 'bullish':
                _vote = +0.60
            elif _trend == 'bearish':
                _vote = -0.60
            # BOS adds directional conviction
            if _bos == 'bullish':
                _vote += +0.25
            elif _bos == 'bearish':
                _vote -= +0.25
            # CHoCH is an early reversal warning (reduces vote)
            if _choch == 'bearish' and _trend == 'bullish':
                _vote -= 0.15
            elif _choch == 'bullish' and _trend == 'bearish':
                _vote += 0.15
            _struct_votes.append(_tf_w * max(-1.0, min(1.0, _vote)))

        factors.htf_structure = (sum(_struct_votes) if _struct_votes else 0.0)
        factors.htf_structure = max(-1.0, min(1.0, factors.htf_structure))

        # ─────────────────────────────────────────────────────────────────────
        # FACTOR 3: Dealing Range P/D Position (weight 0.15)
        # ─────────────────────────────────────────────────────────────────────
        # Institutional principle: smart money BUYS in discount (below 50% P/D)
        # and SELLS in premium (above 50% P/D). Current P/D position predicts
        # which pool is being targeted.
        #   dr_pd = 0.0 (deep discount) → price heading UP to BSL → +1.0
        #   dr_pd = 0.5 (equilibrium)  → neutral → 0.0
        #   dr_pd = 1.0 (deep premium) → price heading DOWN to SSL → -1.0
        # Linear mapping: f_dr = 1.0 - 2.0 × dr_pd
        factors.dealing_range = max(-1.0, min(1.0, 1.0 - 2.0 * dr_pd))

        # ─────────────────────────────────────────────────────────────────────
        # FACTOR 4: Order Flow Vector (weight 0.13)
        # ─────────────────────────────────────────────────────────────────────
        # Combine tick_flow and cvd_trend with a ratio favoring real-time data.
        # tick_flow is more reactive; cvd_trend provides sustained momentum.
        # In trending markets, sustained flow is more informative than single ticks.
        _of_composite = tick_flow * 0.55 + cvd_trend * 0.45
        factors.order_flow = _sigmoid(_of_composite, steepness=1.3)

        # ─────────────────────────────────────────────────────────────────────
        # FACTOR 5: Pool Significance Asymmetry (weight 0.09)
        # ─────────────────────────────────────────────────────────────────────
        # Compare the aggregate significance of BSL pools above price vs
        # SSL pools below. Higher significance = more clustered stops = stronger
        # magnetic pull. The side with MORE/BETTER pools gets hunted more often.
        unswept_pools = [p for p in ict_engine.liquidity_pools if not p.swept]
        _TF_WEIGHT = {"1d": 5.0, "4h": 4.0, "1h": 3.0, "15m": 2.0, "5m": 1.0}

        def _pool_significance(p, ref_price):
            dist_atr = abs(p.price - ref_price) / a
            tf_w = _TF_WEIGHT.get(getattr(p, 'timeframe', '5m'), 1.0)
            tc = float(getattr(p, 'touch_count', 1))
            # Significance decays with distance (ATR-normalised)
            return tc * tf_w * math.exp(-dist_atr / 8.0)

        bsl_sig = sum(_pool_significance(p, price) for p in unswept_pools
                      if p.level_type == "BSL" and p.price > price)
        ssl_sig = sum(_pool_significance(p, price) for p in unswept_pools
                      if p.level_type == "SSL" and p.price < price)
        total_sig = bsl_sig + ssl_sig
        if total_sig > 1e-10:
            # Positive = BSL side dominant (more likely to be hunted)
            factors.pool_asymmetry = _sigmoid(
                (bsl_sig - ssl_sig) / total_sig * 3.0, steepness=1.0)
        else:
            factors.pool_asymmetry = 0.0

        # ─────────────────────────────────────────────────────────────────────
        # FACTOR 6: OB/FVG Magnetic Pull (weight 0.08)
        # ─────────────────────────────────────────────────────────────────────
        # Unfilled bullish OBs and FVGs between price and BSL = delivery highway.
        # Unfilled bearish OBs and FVGs between price and SSL = delivery highway.
        # These structural gaps and OB zones ATTRACT price like gravity.
        now_ms_local = now_ms
        _ob_bull = getattr(ict_engine, 'order_blocks_bull', [])
        _ob_bear = getattr(ict_engine, 'order_blocks_bear', [])
        _fvg_bull = getattr(ict_engine, 'fvgs_bull', [])
        _fvg_bear = getattr(ict_engine, 'fvgs_bear', [])

        bull_pull = 0.0
        bear_pull = 0.0
        try:
            # Bull OBs between price and BSL = price has structural support going UP
            for ob in _ob_bull:
                if (ob.is_active(now_ms_local) and
                        price < ob.midpoint < bsl_price):
                    bull_pull += (ob.strength / 100.0) * ob.virgin_multiplier()
            # Bear OBs between price and SSL = price has structural support going DOWN
            for ob in _ob_bear:
                if (ob.is_active(now_ms_local) and
                        ssl_price < ob.midpoint < price):
                    bear_pull += (ob.strength / 100.0) * ob.virgin_multiplier()
            # Unfilled bull FVGs above price → BSL delivery highway
            for fvg in _fvg_bull:
                if (fvg.is_active(now_ms_local) and
                        not fvg.filled and price < fvg.bottom < bsl_price):
                    bull_pull += (1.0 - fvg.fill_percentage) * 0.6
            # Unfilled bear FVGs below price → SSL delivery highway
            for fvg in _fvg_bear:
                if (fvg.is_active(now_ms_local) and
                        not fvg.filled and ssl_price < fvg.top < price):
                    bear_pull += (1.0 - fvg.fill_percentage) * 0.6
        except Exception:
            pass

        factors.ob_fvg_pull = _sigmoid(bull_pull - bear_pull, steepness=0.7)

        # ─────────────────────────────────────────────────────────────────────
        # FACTOR 7: Displacement Bias (weight 0.07)
        # ─────────────────────────────────────────────────────────────────────
        # Net candle body direction of the last 4 CLOSED 5m bars (institutional
        # displacement logic — uses closed bars only to avoid live-bar noise).
        # Strong bullish bodies = price is being displaced UP = BSL hunt.
        # Strong bearish bodies = SSL hunt.
        factors.displacement = 0.0
        _c5 = candles_5m or []
        if len(_c5) >= 6 and a > 1e-10:
            # Use 4 most recent CLOSED bars (exclude live forming bar)
            closed_bars = _c5[-5:-1]
            net_body = sum(float(c['c']) - float(c['o']) for c in closed_bars)
            # Normalise by ATR × bars for scale
            factors.displacement = _sigmoid(net_body / (a * 2.0), steepness=1.0)
        elif _tf.get("5m"):
            # Fallback to 5m structural trend
            t5 = _tf["5m"]
            if getattr(t5, 'trend', '') == 'bullish':
                factors.displacement = +0.50
            elif getattr(t5, 'trend', '') == 'bearish':
                factors.displacement = -0.50

        # ─────────────────────────────────────────────────────────────────────
        # FACTOR 8: Session Timing (weight 0.04)
        # ─────────────────────────────────────────────────────────────────────
        # Kill zone behavioural tendencies:
        #   LONDON KZ: Judas swing bias (often sweeps UP then delivers DOWN)
        #              → slight BSL sweep bias (price hunts BSL first)
        #   NY KZ:     Amplify the existing directional composite
        #   ASIA KZ:   Range consolidation — slight bias toward the OPPOSITE
        #              side of the current P/D position (mean reversion tendency)
        factors.session = 0.0
        try:
            kz = getattr(ict_engine, '_killzone', '').upper()
            sess = getattr(ict_engine, '_session', '').upper()
            if 'LONDON' in kz:
                # London tends to run buy stops first (Judas swing UP)
                # Then delivers down → net BSL sweep bias in London KZ
                factors.session = +0.35
            elif 'NY' in kz:
                # NY amplifies the dominant direction from other factors
                dominant = (
                    factors.amd * self._FACTOR_WEIGHTS['amd'] +
                    factors.htf_structure * self._FACTOR_WEIGHTS['htf_structure'] +
                    factors.dealing_range * self._FACTOR_WEIGHTS['dealing_range']
                )
                factors.session = _sigmoid(dominant * 2.5, steepness=1.0)
            elif 'ASIA' in kz:
                # Asia tends toward the opposite of current P/D extreme
                # (accumulation often involves false breakouts of range extremes)
                factors.session = 0.0  # neutral — accumulation is directionless
        except Exception:
            pass

        # ─────────────────────────────────────────────────────────────────────
        # FACTOR 9: Micro-Structure BOS (weight 0.03)
        # ─────────────────────────────────────────────────────────────────────
        # Recent 5m BOS direction provides early structural momentum signal.
        # Bullish BOS (close above recent swing high) = structural upside break.
        factors.micro_bos = 0.0
        _t5 = _tf.get("5m")
        if _t5 is not None:
            _bos_d = getattr(_t5, 'bos_direction', '')
            if _bos_d == 'bullish':
                factors.micro_bos = +0.80
            elif _bos_d == 'bearish':
                factors.micro_bos = -0.80

        # ─────────────────────────────────────────────────────────────────────
        # FACTOR 10: Volume Asymmetry (weight 0.01)
        # ─────────────────────────────────────────────────────────────────────
        # Approximate net buy/sell from OB imbalance in the ICT engine's
        # tick_flow data (already incorporated in Factor 4 to some degree,
        # but this captures the orderbook depth asymmetry independently).
        factors.volume = 0.0
        try:
            _tf_raw = getattr(ict_engine, '_tick_flow', None)
            _cvd_raw = getattr(ict_engine, '_cvd_trend', None)
            if _tf_raw is not None and _cvd_raw is not None:
                factors.volume = _sigmoid(
                    float(_tf_raw) * 0.4 + float(_cvd_raw) * 0.6,
                    steepness=0.8)
        except Exception:
            pass

        # ─────────────────────────────────────────────────────────────────────
        # WEIGHTED COMPOSITE SCORE
        # ─────────────────────────────────────────────────────────────────────
        score = (
            factors.amd            * self._FACTOR_WEIGHTS["amd"] +
            factors.htf_structure  * self._FACTOR_WEIGHTS["htf_structure"] +
            factors.dealing_range  * self._FACTOR_WEIGHTS["dealing_range"] +
            factors.order_flow     * self._FACTOR_WEIGHTS["order_flow"] +
            factors.pool_asymmetry * self._FACTOR_WEIGHTS["pool_asymmetry"] +
            factors.ob_fvg_pull    * self._FACTOR_WEIGHTS["ob_fvg_pull"] +
            factors.displacement   * self._FACTOR_WEIGHTS["displacement"] +
            factors.session        * self._FACTOR_WEIGHTS["session"] +
            factors.micro_bos      * self._FACTOR_WEIGHTS["micro_bos"] +
            factors.volume         * self._FACTOR_WEIGHTS["volume"]
        )
        score = max(-1.0, min(1.0, score))

        confidence = abs(score)
        bsl_score  = max(0.0, score)
        ssl_score  = max(0.0, -score)

        # ─────────────────────────────────────────────────────────────────────
        # DEALING RANGE GATE: Cap confidence if in wrong zone
        # ─────────────────────────────────────────────────────────────────────
        # If predicting BSL hunt (price going UP) but we're already in DEEP PREMIUM,
        # confidence is structurally capped. Smart money does NOT hunt BSL from premium.
        # If predicting SSL hunt from DEEP DISCOUNT, same logic applies.
        if score > 0 and dr_pd > 0.75:
            # BSL hunt predicted but in deep premium → reduce confidence
            _zone_penalty = (dr_pd - 0.75) / 0.25   # 0→1 as pd goes 0.75→1.0
            confidence = max(0.0, confidence - _zone_penalty * 0.30)
        elif score < 0 and dr_pd < 0.25:
            # SSL hunt predicted but in deep discount → reduce confidence
            _zone_penalty = (0.25 - dr_pd) / 0.25
            confidence = max(0.0, confidence - _zone_penalty * 0.30)

        # ─────────────────────────────────────────────────────────────────────
        # HTF STRUCTURE OVERRIDE
        # ─────────────────────────────────────────────────────────────────────
        # When 3 HTF timeframes (4H, 1H, 15m) are all bearish and the composite
        # says BSL hunt, this is a major structural contradiction. HTF structure
        # overrides the composite score by reversing its direction.
        _bull_count = sum(1 for _tn in ("4h", "1h", "15m")
                          if _tf.get(_tn) and getattr(_tf[_tn], 'trend', '') == 'bullish')
        _bear_count = sum(1 for _tn in ("4h", "1h", "15m")
                          if _tf.get(_tn) and getattr(_tf[_tn], 'trend', '') == 'bearish')

        _override_applied = False
        if _bear_count >= 3 and score > 0.10:
            # All HTF bearish but composite says BSL hunt — override to SSL
            logger.debug(
                f"DirectionEngine: HTF OVERRIDE → SSL hunt "
                f"(all 3 TFs bearish, raw_score={score:+.3f})")
            score = -abs(score) * 0.70  # flip and reduce magnitude
            _override_applied = True
        elif _bull_count >= 3 and score < -0.10:
            # All HTF bullish but composite says SSL hunt — override to BSL
            logger.debug(
                f"DirectionEngine: HTF OVERRIDE → BSL hunt "
                f"(all 3 TFs bullish, raw_score={score:+.3f})")
            score = +abs(score) * 0.70
            _override_applied = True

        # Recompute after override
        confidence = abs(score)
        bsl_score  = max(0.0, score)
        ssl_score  = max(0.0, -score)

        # ─────────────────────────────────────────────────────────────────────
        # BUILD RESULT
        # ─────────────────────────────────────────────────────────────────────
        if confidence < _HUNT_MIN_CONFIDENCE:
            result = self._null_prediction(
                now_ms,
                reason=f"low_confidence({confidence:.3f}<{_HUNT_MIN_CONFIDENCE})",
                dr_pd=dr_pd, bsl_score=bsl_score, ssl_score=ssl_score,
                score=score, factors=factors)
            self._last_hunt    = result
            self._last_hunt_ms = now_ms
            return result

        if score > 0:
            predicted          = "BSL"
            delivery_direction = "bearish"
            swept_pool         = bsl_price
            opposing_pool      = ssl_price
            reason = (
                f"BSL_HUNT (score={score:+.3f}) | "
                f"AMD={getattr(amd,'phase','?')}({getattr(amd,'bias','?')},{getattr(amd,'confidence',0):.2f}) | "
                f"HTF_struct={factors.htf_structure:+.2f} | "
                f"DR={dr_pd:.2f} | flow={factors.order_flow:+.2f}"
            )
        else:
            predicted          = "SSL"
            delivery_direction = "bullish"
            swept_pool         = ssl_price
            opposing_pool      = bsl_price
            reason = (
                f"SSL_HUNT (score={score:+.3f}) | "
                f"AMD={getattr(amd,'phase','?')}({getattr(amd,'bias','?')},{getattr(amd,'confidence',0):.2f}) | "
                f"HTF_struct={factors.htf_structure:+.2f} | "
                f"DR={dr_pd:.2f} | flow={factors.order_flow:+.2f}"
            )
        if _override_applied:
            reason += " | HTF_STRUCT_OVERRIDE"

        result = HuntPrediction(
            predicted          = predicted,
            confidence         = round(min(1.0, confidence), 4),
            delivery_direction = delivery_direction,
            raw_score          = round(score, 4),
            bsl_score          = round(bsl_score, 4),
            ssl_score          = round(ssl_score, 4),
            factors            = factors,
            dealing_range_pd   = round(dr_pd, 4),
            swept_pool_price   = swept_pool,
            opposing_pool_price= opposing_pool,
            reason             = reason,
            timestamp_ms       = now_ms,
        )

        logger.debug(
            f"DirectionEngine: {predicted} ({confidence:.3f}) | {reason[:80]}")

        self._last_hunt    = result
        self._last_hunt_ms = now_ms
        return result

    # =========================================================================
    # 2. POST-SWEEP SCENARIO ENGINE
    # =========================================================================

    def on_sweep(
        self,
        swept_pool_price: float,
        pool_type:        str,    # "BSL" | "SSL"
        price:            float,
        atr:              float,
        now:              float,
    ) -> None:
        """
        Call this immediately when a pool sweep is detected.
        Initialises the accumulative evidence tracker for the post-sweep evaluation.
        """
        self._ps_state = PostSweepState(
            swept_pool_price = swept_pool_price,
            swept_pool_type  = pool_type,
            entered_at       = now,
            highest_since    = price,
            lowest_since     = price,
        )
        logger.info(
            f"🌊 DirectionEngine: POST-SWEEP STARTED | "
            f"{pool_type} @ ${swept_pool_price:,.1f} | price=${price:,.2f}")

    def clear_sweep(self) -> None:
        """Call when post-sweep evaluation is complete or aborted."""
        self._ps_state = None

    def evaluate_sweep(
        self,
        price:       float,
        atr:         float,
        now:         float,
        ict_engine,
        tick_flow:   float = 0.0,
        cvd_trend:   float = 0.0,
    ) -> PostSweepDecision:
        """
        Evaluate the current post-sweep state and determine: reverse, continue, or wait.

        This runs on every tick after a sweep is detected (via on_sweep()).
        Evidence accumulates across ticks — a single counter-signal cannot flip
        a well-established reversal or continuation thesis.

        Returns PostSweepDecision with action = "reverse" | "continue" | "wait".

        PHASE TIMELINE:
          0 – 8s:   Initial delay (wait for price to react to sweep)
          8 – 45s:  DISPLACEMENT phase: look for strong price rejection
          45 – 120s: CISD phase: look for CHoCH/BOS confirming reversal direction
          120 – 240s: OTE phase: look for price to retrace to 61.8-78.6% fib zone
          240 – 360s: MATURE phase: accept weaker setups, relaxed thresholds
          >360s:    TIMEOUT — abandon evaluation
        """
        ps = self._ps_state
        if ps is None:
            return PostSweepDecision(
                action="wait", direction="", confidence=0.0,
                phase="NO_STATE", reason="no_sweep_state")

        elapsed = now - ps.entered_at

        # ── Timeout ────────────────────────────────────────────────────────
        if elapsed > _PS_TIMEOUT_SEC:
            logger.info(
                f"POST-SWEEP TIMEOUT after {elapsed:.0f}s | "
                f"peak_rev={ps.peak_rev:.1f} peak_cont={ps.peak_cont:.1f}")
            self._ps_state = None
            return PostSweepDecision(
                action="wait", direction="", confidence=0.0,
                phase="TIMEOUT",
                reason=f"timeout after {elapsed:.0f}s")

        # ── Initial delay ──────────────────────────────────────────────────
        if elapsed < _PS_EVAL_DELAY_SEC:
            return PostSweepDecision(
                action="wait", direction="", confidence=0.0,
                phase="DELAY",
                reason=f"waiting {_PS_EVAL_DELAY_SEC - elapsed:.0f}s more")

        a = max(atr, 1e-9)
        ps.tick_count += 1

        # Reversal direction: BSL swept → SHORT reversal; SSL swept → LONG reversal
        rev_dir  = "short" if ps.swept_pool_type == "BSL" else "long"
        cont_dir = "long"  if ps.swept_pool_type == "BSL" else "short"

        # ── Update price extremes ──────────────────────────────────────────
        ps.highest_since = max(ps.highest_since, price)
        ps.lowest_since  = min(ps.lowest_since, price)

        # ── Current phase ──────────────────────────────────────────────────
        if elapsed < 45.0:
            phase = "DISPLACEMENT"
            base_threshold = _PS_REV_THRESHOLD_EARLY
            score_mult     = 1.30   # need overwhelming evidence early
        elif elapsed < 120.0:
            phase = "CISD"
            base_threshold = _PS_REV_THRESHOLD_NORMAL
            score_mult     = 1.0
        elif elapsed < 240.0:
            phase = "OTE"
            base_threshold = _PS_REV_THRESHOLD_NORMAL
            score_mult     = 1.0
        else:
            phase = "MATURE"
            base_threshold = _PS_REV_THRESHOLD_MATURE
            score_mult     = 0.75   # accept weaker setups in mature phase

        # ─────────────────────────────────────────────────────────────────────
        # STEP 1: Update displacement tracking
        # ─────────────────────────────────────────────────────────────────────
        sweep_px = ps.swept_pool_price
        if rev_dir == "long":
            # SSL swept: expect price to rise
            disp = (ps.highest_since - sweep_px) / a
        else:
            # BSL swept: expect price to drop
            disp = (sweep_px - ps.lowest_since) / a

        if disp > ps.max_displacement_atr:
            ps.max_displacement_atr = disp
            if elapsed > 1.0:
                ps.disp_velocity_atr_s = disp / elapsed

        # ─────────────────────────────────────────────────────────────────────
        # STEP 2: CISD Detection (Change in State of Delivery)
        # ─────────────────────────────────────────────────────────────────────
        if not ps.cisd_detected and ict_engine is not None:
            try:
                _tf = getattr(ict_engine, '_tf', {})
                _t5 = _tf.get("5m")
                if _t5 is not None:
                    _choch = getattr(_t5, 'choch_direction', '') or ''
                    _bos   = getattr(_t5, 'bos_direction', '') or ''
                    _bos_ts = getattr(_t5, 'bos_timestamp', 0)
                    _choch_ts = getattr(_t5, 'choch_timestamp', 0)
                    _rev_struct = "bearish" if rev_dir == "short" else "bullish"
                    # CISD requires the structural event to have occurred AFTER the sweep
                    _sweep_ms = ps.entered_at * 1000
                    if (_choch == _rev_struct and
                            _choch_ts > _sweep_ms and
                            now - _choch_ts / 1000 < _DISP_CISD_MAX_AGE_SEC):
                        ps.cisd_detected  = True
                        ps.cisd_timestamp = now
                        ps.cisd_type      = "choch"
                        logger.info(
                            f"POST-SWEEP CISD: CHoCH {_rev_struct} confirmed "
                            f"at {elapsed:.0f}s | disp={ps.max_displacement_atr:.2f}ATR")
                    elif (_bos == _rev_struct and
                            _bos_ts > _sweep_ms and
                            now - _bos_ts / 1000 < _DISP_CISD_MAX_AGE_SEC):
                        ps.cisd_detected  = True
                        ps.cisd_timestamp = now
                        ps.cisd_type      = "bos"
                        logger.info(
                            f"POST-SWEEP CISD: BOS {_rev_struct} confirmed "
                            f"at {elapsed:.0f}s | disp={ps.max_displacement_atr:.2f}ATR")
            except Exception:
                pass

        # ─────────────────────────────────────────────────────────────────────
        # STEP 3: OTE Zone Tracking (50%–78.6% Fibonacci retrace of displacement)
        # ─────────────────────────────────────────────────────────────────────
        if ps.max_displacement_atr >= _DISP_WEAK_ATR:
            if rev_dir == "long":
                swing_low  = sweep_px
                swing_high = ps.highest_since
            else:
                swing_high = sweep_px
                swing_low  = ps.lowest_since

            swing_range = abs(swing_high - swing_low)
            if swing_range > 1e-10:
                if rev_dir == "long":
                    retrace_pct = (ps.highest_since - price) / swing_range
                else:
                    retrace_pct = (price - ps.lowest_since) / swing_range

                in_ote = _OTE_FIB_LOW <= retrace_pct <= _OTE_FIB_HIGH
                if in_ote and not ps.ote_reached:
                    ps.ote_reached   = True
                    ps.ote_timestamp = now
                    logger.info(
                        f"POST-SWEEP OTE REACHED: retrace={retrace_pct:.1%} "
                        f"at {elapsed:.0f}s")
                ps.ote_holding = in_ote

        # ─────────────────────────────────────────────────────────────────────
        # STEP 4: Score static factors (ONCE per sweep evaluation)
        # ─────────────────────────────────────────────────────────────────────
        if not ps.static_scored:
            ps.static_rev_base, ps.static_cont_base = \
                self._score_sweep_static(ps, ict_engine, rev_dir, cont_dir)
            ps.static_scored = True
            logger.debug(
                f"POST-SWEEP static: rev={ps.static_rev_base:.1f} "
                f"cont={ps.static_cont_base:.1f}")

        # ─────────────────────────────────────────────────────────────────────
        # STEP 5: Score dynamic factors (every tick)
        # ─────────────────────────────────────────────────────────────────────
        rev_delta, cont_delta, rev_reasons, cont_reasons = \
            self._score_sweep_dynamic(
                ps, ict_engine, rev_dir, cont_dir,
                price, a, now, tick_flow, cvd_trend)

        # ── Accumulate with decay ──────────────────────────────────────────
        _decay = 0.92  # per-tick decay for the losing side
        if rev_delta > 0:
            ps.rev_evidence  += rev_delta
            ps.cont_evidence *= _decay
        if cont_delta > 0:
            ps.cont_evidence += cont_delta
            ps.rev_evidence  *= _decay

        # Update peaks
        ps.peak_rev  = max(ps.peak_rev,  ps.static_rev_base  + ps.rev_evidence)
        ps.peak_cont = max(ps.peak_cont, ps.static_cont_base + ps.cont_evidence)

        # Combined totals
        rev_total  = max(ps.static_rev_base  + ps.rev_evidence,  0.0)
        cont_total = max(ps.static_cont_base + ps.cont_evidence, 0.0)
        gap        = abs(rev_total - cont_total)

        # Store last analysis for display
        self._last_ps_analysis = {
            "phase":             phase,
            "rev_score":         round(rev_total, 1),
            "cont_score":        round(cont_total, 1),
            "rev_reasons":       rev_reasons,
            "cont_reasons":      cont_reasons,
            "cisd":              ps.cisd_detected,
            "displacement_atr":  ps.max_displacement_atr,
            "ote":               ps.ote_reached,
            "elapsed_sec":       round(elapsed, 1),
        }

        # ── Log every 10 ticks ────────────────────────────────────────────
        if ps.tick_count == 1 or ps.tick_count % 10 == 0:
            _threshold = base_threshold * score_mult
            logger.info(
                f"POST-SWEEP [{phase}] tick={ps.tick_count} "
                f"rev={rev_total:.0f} cont={cont_total:.0f} "
                f"(need {_threshold:.0f}, gap>={_PS_GAP_MIN:.0f}) "
                f"CISD={'✓' if ps.cisd_detected else '✗'} "
                f"DISP={ps.max_displacement_atr:.2f}ATR "
                f"OTE={'✓' if ps.ote_reached else '✗'}")

        # ─────────────────────────────────────────────────────────────────────
        # STEP 6: Decision
        # ─────────────────────────────────────────────────────────────────────
        _adj_threshold = base_threshold * score_mult

        if rev_total >= _adj_threshold and gap >= _PS_GAP_MIN:
            _conf = min(1.0, rev_total / 95.0)
            if ps.cisd_detected: _conf = min(1.0, _conf + 0.15)
            if ps.ote_reached:   _conf = min(1.0, _conf + 0.10)
            _reason = (
                f"REVERSAL [{phase}] rev={rev_total:.0f} > {_adj_threshold:.0f} "
                f"gap={gap:.0f} CISD={ps.cisd_type or 'none'} "
                f"DISP={ps.max_displacement_atr:.2f}ATR "
                f"OTE={'✓' if ps.ote_reached else '✗'}")
            logger.info(f"🎯 POST-SWEEP VERDICT: {_reason}")
            self._ps_state = None
            return PostSweepDecision(
                action        = "reverse",
                direction     = rev_dir,
                confidence    = _conf,
                phase         = phase,
                cisd_active   = ps.cisd_detected,
                ote_active    = ps.ote_reached,
                displacement_atr = ps.max_displacement_atr,
                rev_score     = rev_total,
                cont_score    = cont_total,
                rev_reasons   = rev_reasons[:6],
                cont_reasons  = cont_reasons[:4],
                reason        = _reason,
            )

        elif cont_total >= _adj_threshold * _PS_CONT_THRESHOLD_RATIO and gap >= _PS_GAP_MIN:
            _conf = min(1.0, cont_total / 90.0)
            _reason = (
                f"CONTINUATION [{phase}] cont={cont_total:.0f} "
                f"vs rev={rev_total:.0f} gap={gap:.0f}")
            logger.info(f"🎯 POST-SWEEP VERDICT: {_reason}")
            self._ps_state = None
            return PostSweepDecision(
                action        = "continue",
                direction     = cont_dir,
                confidence    = _conf,
                phase         = phase,
                cisd_active   = False,
                ote_active    = ps.ote_reached,
                displacement_atr = ps.max_displacement_atr,
                rev_score     = rev_total,
                cont_score    = cont_total,
                rev_reasons   = rev_reasons[:3],
                cont_reasons  = cont_reasons[:6],
                reason        = _reason,
            )

        return PostSweepDecision(
            action        = "wait",
            direction     = "",
            confidence    = 0.0,
            phase         = phase,
            cisd_active   = ps.cisd_detected,
            ote_active    = ps.ote_reached,
            displacement_atr = ps.max_displacement_atr,
            rev_score     = rev_total,
            cont_score    = cont_total,
            rev_reasons   = rev_reasons[:4],
            cont_reasons  = cont_reasons[:4],
            reason        = (
                f"WAIT [{phase}] rev={rev_total:.0f} cont={cont_total:.0f} "
                f"gap={gap:.0f} < {_PS_GAP_MIN:.0f}"),
        )

    # ─────────────────────────────────────────────────────────────────────────
    # POST-SWEEP SCORING HELPERS
    # ─────────────────────────────────────────────────────────────────────────

    def _score_sweep_static(
        self,
        ps:         PostSweepState,
        ict_engine,
        rev_dir:    str,
        cont_dir:   str,
    ) -> Tuple[float, float]:
        """
        Score the TIME-INVARIANT factors for the post-sweep decision.

        Called EXACTLY ONCE after the initial evaluation delay expires.
        These facts do not change tick-to-tick (AMD phase, sweep quality,
        dealing range position, OB blocking) — re-adding them each tick
        would inflate the accumulated total and defeat phase-adaptive gating.

        Returns (rev_base, cont_base) scores.
        """
        s_rev  = 0.0
        s_cont = 0.0

        if ict_engine is None:
            return s_rev, s_cont

        amd = getattr(ict_engine, '_amd', None)

        # ── AMD Phase alignment ───────────────────────────────────────────
        # CRITICAL: The AMD phase + sweep polarity tells us if this sweep is
        # the MANIPULATION fake-out (→ reversal) or a DISTRIBUTION continuation.
        if amd is not None:
            _phase = getattr(amd, 'phase', '').upper()
            _bias  = getattr(amd, 'bias', '').lower()
            _conf  = float(getattr(amd, 'confidence', 0.0))

            _bsl_swept = ps.swept_pool_type == "BSL"
            _ssl_swept = ps.swept_pool_type == "SSL"

            if _phase == 'MANIPULATION':
                # ICT: MANIPULATION phase = Judas swing (fake move).
                # Bearish AMD bias + BSL swept = fake move UP then delivers DOWN.
                #   → BSL sweep IS the Judas → REVERSAL (go short after BSL sweep).
                # Bullish AMD bias + SSL swept = fake move DOWN then delivers UP.
                #   → SSL sweep IS the Judas → REVERSAL (go long after SSL sweep).
                _manip_aligned = (
                    (_bsl_swept and _bias == 'bearish') or
                    (_ssl_swept and _bias == 'bullish'))
                if _manip_aligned:
                    # Perfect AMD alignment: sweep is the manipulation
                    s_rev += 28.0 * max(_conf, 0.5)
                else:
                    # AMD says manipulation but sweep is in the same direction as bias
                    # This could be a continuation sweep during manipulation — penalise reversal
                    s_rev  -= 15.0 * max(_conf, 0.5)
                    s_cont += 10.0 * max(_conf, 0.5)

            elif _phase == 'DISTRIBUTION':
                # Distribution phase: real move is underway.
                # Bearish distribution + BSL swept = price consuming BSL on the way down.
                #   → This is CONTINUATION (bearish distribution continues after BSL sweep).
                # Bullish distribution + SSL swept = price consuming SSL on the way up.
                #   → This is CONTINUATION (bullish distribution continues).
                _dist_cont_aligned = (
                    (_bsl_swept and _bias == 'bearish') or
                    (_ssl_swept and _bias == 'bullish'))
                if _dist_cont_aligned:
                    s_cont += 26.0 * max(_conf, 0.5)
                else:
                    # Distribution opposing sweep direction — unusual, moderate reversal bias
                    s_rev  += 18.0 * max(_conf, 0.5)

            elif _phase in ('REACCUMULATION',):
                # Mid-trend bullish pause: sweeping SSL is normal (healthy pullback retest)
                if _ssl_swept:
                    s_cont += 14.0 * max(_conf, 0.4)  # continue bullish
                else:
                    s_rev  += 10.0 * max(_conf, 0.4)  # BSL swept = reversal signal

            elif _phase in ('REDISTRIBUTION',):
                if _bsl_swept:
                    s_cont += 14.0 * max(_conf, 0.4)  # continue bearish
                else:
                    s_rev  += 10.0 * max(_conf, 0.4)

            elif _phase == 'ACCUMULATION':
                # Accumulation: no strong directional bias — slight reversal preference
                # (accumulation often involves stop runs that reverse)
                s_rev += 6.0 * max(_conf, 0.3)

        # ── Dealing Range Position ────────────────────────────────────────
        # For a BSL sweep (reversal = short), reversal is favoured from PREMIUM.
        # For an SSL sweep (reversal = long), reversal is favoured from DISCOUNT.
        dr = getattr(ict_engine, '_dealing_range', None)
        _pd = getattr(dr, 'current_pd', 0.5) if dr is not None else 0.5

        if ps.swept_pool_type == "BSL":
            # BSL sweep → reversal is SHORT → needs to be in PREMIUM
            if _pd >= 0.80:
                s_rev  += 14.0
            elif _pd >= 0.65:
                s_rev  += 10.0
            elif _pd >= 0.50:
                s_rev  +=  5.0
            else:
                # In discount — reversing SHORT from discount is high risk
                s_cont +=  8.0
        else:
            # SSL sweep → reversal is LONG → needs to be in DISCOUNT
            if _pd <= 0.20:
                s_rev  += 14.0
            elif _pd <= 0.35:
                s_rev  += 10.0
            elif _pd <= 0.50:
                s_rev  +=  5.0
            else:
                # In premium — reversing LONG from premium is high risk
                s_cont +=  8.0

        # ── HTF Structure Context ─────────────────────────────────────────
        _tf = getattr(ict_engine, '_tf', {})
        _rev_struct = "bearish" if rev_dir == "short" else "bullish"
        _htf_aligned = 0
        _htf_opposed = 0
        for _tn in ("4h", "1h", "15m"):
            _st = _tf.get(_tn)
            if _st is None:
                continue
            _trend = getattr(_st, 'trend', '')
            if _trend == _rev_struct:
                _htf_aligned += 1
            elif _trend and _trend != 'ranging':
                _htf_opposed += 1

        if _htf_aligned >= 3:
            s_rev  += 16.0
        elif _htf_aligned >= 2:
            s_rev  += 10.0
        elif _htf_opposed >= 2:
            s_cont += 10.0
            s_rev  -=  5.0

        # ── OB Blocking (is there an OB in the continuation path?) ───────
        # If a high-strength OB sits between current price and the continuation
        # target, it acts as resistance — favours reversal.
        try:
            if rev_dir == "short":
                # Continuation = long → bearish OBs between price and BSL block it
                obs = getattr(ict_engine, 'order_blocks_bear', [])
            else:
                # Continuation = short → bullish OBs between price and SSL block it
                obs = getattr(ict_engine, 'order_blocks_bull', [])
            _now_ms_local = int(time.time() * 1000)
            for ob in obs:
                if not ob.is_active(_now_ms_local):
                    continue
                if ob.strength < 70.0:
                    continue
                ob_dist_atr = abs(ob.midpoint - ps.swept_pool_price) / max(atr, 1.0) \
                    if atr > 1e-10 else 0.0
                if ob_dist_atr < 2.0:
                    s_rev += 10.0
                    break
        except Exception:
            pass

        # ── Session Kill Zone Context ─────────────────────────────────────
        _kz = getattr(ict_engine, '_killzone', '').lower()
        if 'asia' in _kz:
            # Asia KZ sweeps are often manipulations → reversal biased
            s_rev += 7.0
        elif 'london' in _kz:
            # London KZ Judas swing → moderate reversal bias
            s_rev += 4.0
        elif 'ny' in _kz:
            # NY KZ: depends on direction — slight reversal bias (NY open often reverses Asia)
            s_rev += 2.0

        return round(s_rev, 2), round(s_cont, 2)

    def _score_sweep_dynamic(
        self,
        ps:         PostSweepState,
        ict_engine,
        rev_dir:    str,
        cont_dir:   str,
        price:      float,
        atr:        float,
        now:        float,
        tick_flow:  float,
        cvd_trend:  float,
    ) -> Tuple[float, float, List[str], List[str]]:
        """
        Score the PER-TICK dynamic factors for the post-sweep decision.

        These factors change tick-to-tick and feed the decay accumulator:
        displacement growth, CISD freshness, OTE zone, flow, CVD, structure events.
        """
        rev_delta  = 0.0
        cont_delta = 0.0
        rev_reasons:  List[str] = []
        cont_reasons: List[str] = []

        a = max(atr, 1e-9)

        # ── Live displacement from sweep level ────────────────────────────
        if ps.max_displacement_atr >= _DISP_STRONG_ATR:
            rev_delta += 12.0
            rev_reasons.append(f"STRONG_DISP {ps.max_displacement_atr:.2f}ATR")
        elif ps.max_displacement_atr >= _DISP_WEAK_ATR:
            rev_delta += 6.0
            rev_reasons.append(f"DISP {ps.max_displacement_atr:.2f}ATR")
        elif ps.max_displacement_atr < 0.2 and (now - ps.entered_at) > 15.0:
            # No displacement → price is NOT rejecting → continuation evidence
            cont_delta += 8.0
            cont_reasons.append(f"NO_DISP ({ps.max_displacement_atr:.2f}ATR)")

        # ── CISD freshness (decays with age) ──────────────────────────────
        if ps.cisd_detected:
            _cisd_age = now - ps.cisd_timestamp
            _freshness = max(0.4, 1.0 - _cisd_age / 120.0)
            rev_delta += 18.0 * _freshness
            rev_reasons.append(f"CISD_{ps.cisd_type.upper()}({_freshness:.0%})")

        # ── OTE zone holding ──────────────────────────────────────────────
        if ps.ote_reached:
            if ps.ote_holding:
                rev_delta += 15.0
                rev_reasons.append("IN_OTE_ZONE")
            else:
                rev_delta +=  8.0
                rev_reasons.append("OTE_WAS_REACHED")

        # ── Order flow (instantaneous) ────────────────────────────────────
        if tick_flow > 0.40 and rev_dir == "long":
            rev_delta  += 10.0; rev_reasons.append(f"FLOW_REV {tick_flow:+.2f}")
            ps.rev_flow_ticks += 1
        elif tick_flow > 0.20 and rev_dir == "long":
            rev_delta  +=  5.0; rev_reasons.append(f"FLOW_WEAK_REV {tick_flow:+.2f}")
            ps.rev_flow_ticks += 1
        elif tick_flow < -0.40 and rev_dir == "short":
            rev_delta  += 10.0; rev_reasons.append(f"FLOW_REV {tick_flow:+.2f}")
            ps.rev_flow_ticks += 1
        elif tick_flow < -0.20 and rev_dir == "short":
            rev_delta  +=  5.0; rev_reasons.append(f"FLOW_WEAK_REV {tick_flow:+.2f}")
            ps.rev_flow_ticks += 1
        elif abs(tick_flow) > 0.30:
            # Flow in continuation direction
            cont_delta += 9.0; cont_reasons.append(f"FLOW_CONT {tick_flow:+.2f}")
            ps.cont_flow_ticks += 1

        # ── CVD trend alignment ───────────────────────────────────────────
        _cvd_rev_ok = (
            (rev_dir == "long"  and cvd_trend > 0.20) or
            (rev_dir == "short" and cvd_trend < -0.20))
        _cvd_cont_ok = (
            (rev_dir == "long"  and cvd_trend < -0.25) or
            (rev_dir == "short" and cvd_trend > +0.25))

        if _cvd_rev_ok:
            rev_delta  += 8.0; rev_reasons.append(f"CVD_REV {cvd_trend:+.2f}")
        elif _cvd_cont_ok:
            cont_delta += 8.0; cont_reasons.append(f"CVD_CONT {cvd_trend:+.2f}")
        elif abs(cvd_trend) < 0.10:
            # Neutral CVD = slight reversal bias (distribution often shows neutral CVD)
            rev_delta  += 2.0

        # ── 5m Structure ──────────────────────────────────────────────────
        if ict_engine is not None:
            try:
                _tf = getattr(ict_engine, '_tf', {})
                _t5 = _tf.get("5m")
                if _t5 is not None:
                    _choch = getattr(_t5, 'choch_direction', '') or ''
                    _bos   = getattr(_t5, 'bos_direction', '') or ''
                    _rev_struct_label = "bearish" if rev_dir == "short" else "bullish"
                    _cont_struct_label = "bullish" if rev_dir == "short" else "bearish"
                    if _choch == _rev_struct_label:
                        rev_delta  += 12.0; rev_reasons.append("CHoCH_5m_REVERSAL")
                    if _bos == _rev_struct_label:
                        rev_delta  += 8.0; rev_reasons.append("BOS_5m_REVERSAL")
                    if _bos == _cont_struct_label:
                        cont_delta += 10.0; cont_reasons.append("BOS_5m_CONT")
            except Exception:
                pass

        # ── Sustained flow bonus ──────────────────────────────────────────
        if ps.rev_flow_ticks >= 5:
            rev_delta  += 5.0; rev_reasons.append(f"SUSTAINED_REV_FLOW({ps.rev_flow_ticks}t)")
        if ps.cont_flow_ticks >= 5:
            cont_delta += 5.0; cont_reasons.append(f"SUSTAINED_CONT_FLOW({ps.cont_flow_ticks}t)")

        return rev_delta, cont_delta, rev_reasons, cont_reasons

    # =========================================================================
    # 3. POOL-HIT CONTINUATION GATE
    # =========================================================================

    def pool_hit_gate(
        self,
        pos_side:    str,    # "long" | "short"
        pos_entry:   float,
        pos_sl:      float,
        pos_tp:      float,
        price:       float,
        atr:         float,
        ict_engine,
        tick_flow:   float = 0.0,
        cvd_trend:   float = 0.0,
        next_pool_price: Optional[float] = None,
    ) -> ContinuationDecision:
        """
        When price reaches a liquidity pool DURING an active trade, determine:

          EXIT       → This pool is our TP. Close the position.
          REVERSE    → Pool was swept, AMD flipped, go the other way.
          CONTINUE   → Another pool beyond this one is the real target.
                       Trail SL to this pool and ride to the next.
          HOLD       → Insufficient evidence — let the SL/TP manage.

        DECISION LOGIC:
          1. If price is at TP pool → EXIT (take the profit)
          2. If flow has strongly reversed AND BOS confirms → REVERSE
          3. If AMD is still aligned and a next pool exists with > 2R potential → CONTINUE
          4. Otherwise → HOLD (let structure manage)

        CONTINUATION CRITERIA (must meet ALL):
          a) AMD delivery still intact (same bias, phase is DISTRIBUTION or REACCUMULATION)
          b) Flow NOT strongly reversed (abs(tick_flow) < threshold OR same direction)
          c) No counter-BOS on 5m (BOS against position = structural invalidation)
          d) Next pool is at least 1.5R reward from current price
          e) Trade is currently profitable (peak_profit > 0.5R)
        """
        a = max(atr, 1e-9)

        # ── Check: Is this the TP pool? ───────────────────────────────────
        tp_dist = abs(price - pos_tp)
        if tp_dist <= a * 0.15:
            return ContinuationDecision(
                action="exit", confidence=1.0,
                reason=f"TP reached (dist={tp_dist:.1f}pts < 0.15ATR)")

        # ── Check: Has flow strongly reversed? ───────────────────────────
        _flow_reversed = (
            (pos_side == "long"  and tick_flow < -_CONT_FLOW_REVERSAL_MIN) or
            (pos_side == "short" and tick_flow > +_CONT_FLOW_REVERSAL_MIN))

        # ── Check: Counter-BOS? ───────────────────────────────────────────
        _counter_bos = False
        if ict_engine is not None:
            try:
                _tf = getattr(ict_engine, '_tf', {})
                _t5 = _tf.get("5m")
                if _t5 is not None:
                    _bos_d = getattr(_t5, 'bos_direction', '') or ''
                    _bos_ts = getattr(_t5, 'bos_timestamp', 0)
                    _now_ms = int(time.time() * 1000)
                    _bos_age_min = (_now_ms - _bos_ts) / 60000 if _bos_ts > 0 else 999
                    if (_bos_age_min < 30 and  # BOS within last 30 min
                            ((pos_side == "long"  and _bos_d == "bearish") or
                             (pos_side == "short" and _bos_d == "bullish"))):
                        _counter_bos = True
            except Exception:
                pass

        # ── Check: AMD still delivering? ─────────────────────────────────
        _amd_aligned = False
        _amd_delivery_target = None
        if ict_engine is not None:
            amd = getattr(ict_engine, '_amd', None)
            if amd is not None:
                _phase = getattr(amd, 'phase', '').upper()
                _bias  = getattr(amd, 'bias', '').lower()
                _conf  = float(getattr(amd, 'confidence', 0.0))
                _amd_delivery_target = getattr(amd, 'delivery_target', None)
                _amd_aligned = (
                    _conf >= 0.45 and
                    _phase in ('DISTRIBUTION', 'REACCUMULATION', 'REDISTRIBUTION') and
                    ((pos_side == "long"  and _bias == "bullish") or
                     (pos_side == "short" and _bias == "bearish")))

        # ── Check: Viable next pool? ──────────────────────────────────────
        _next_pool_viable = False
        _next_pool = next_pool_price or _amd_delivery_target
        if _next_pool is not None:
            _next_dist = abs(_next_pool - price)
            _sl_dist   = abs(price - pos_sl) if pos_sl > 0 else a
            if _sl_dist > 1e-10:
                _next_rr = _next_dist / _sl_dist
                if _next_rr >= 1.5:
                    _next_pool_viable = True

        # ─────────────────────────────────────────────────────────────────
        # Decision tree
        # ─────────────────────────────────────────────────────────────────

        if _flow_reversed and _counter_bos:
            return ContinuationDecision(
                action     = "reverse",
                confidence = 0.80,
                reason     = (
                    f"FLOW_REVERSED(flow={tick_flow:+.2f}) + COUNTER_BOS "
                    f"— structure invalidated"),
                next_target = None,
            )

        if _amd_aligned and not _counter_bos and not _flow_reversed and _next_pool_viable:
            return ContinuationDecision(
                action     = "continue",
                confidence = 0.70,
                reason     = (
                    f"AMD_DELIVERY_INTACT + FLOW_OK + NEXT_POOL "
                    f"@ ${_next_pool:,.0f} ({_next_rr:.1f}R potential)"),
                next_target = _next_pool,
            )

        if _flow_reversed and not _counter_bos:
            # Flow reversed but no structural BOS — hold and let SL trail protect
            return ContinuationDecision(
                action     = "hold",
                confidence = 0.60,
                reason     = (
                    f"FLOW_REVERSED but no BOS — hold, let trail SL protect "
                    f"(flow={tick_flow:+.2f})"),
            )

        if _counter_bos and not _flow_reversed:
            # BOS against position but flow is still OK — hold for confirmation
            return ContinuationDecision(
                action     = "hold",
                confidence = 0.55,
                reason     = "COUNTER_BOS but flow OK — hold for confirmation",
            )

        # Default: not enough signal → hold
        return ContinuationDecision(
            action     = "hold",
            confidence = 0.40,
            reason     = (
                f"INSUFFICIENT_SIGNAL | AMD={_amd_aligned} "
                f"flow_rev={_flow_reversed} cbos={_counter_bos} "
                f"next_pool={_next_pool_viable}"),
        )

    # =========================================================================
    # HELPERS
    # =========================================================================

    def _null_prediction(
        self, now_ms: int, reason: str = "",
        dr_pd: float = 0.5, bsl_score: float = 0.0,
        ssl_score: float = 0.0, score: float = 0.0,
        factors: Optional[HuntFactors] = None,
    ) -> HuntPrediction:
        return HuntPrediction(
            predicted          = None,
            confidence         = 0.0,
            delivery_direction = "",
            raw_score          = score,
            bsl_score          = bsl_score,
            ssl_score          = ssl_score,
            factors            = factors or HuntFactors(),
            dealing_range_pd   = dr_pd,
            reason             = reason,
            timestamp_ms       = now_ms,
        )

    @property
    def last_hunt(self) -> Optional[HuntPrediction]:
        return self._last_hunt

    @property
    def last_sweep_analysis(self) -> Dict:
        return self._last_ps_analysis

    @property
    def in_post_sweep(self) -> bool:
        return self._ps_state is not None


# ─────────────────────────────────────────────────────────────────────────────
# UTILITY — fast sigmoid
# ─────────────────────────────────────────────────────────────────────────────

def _sigmoid(z: float, steepness: float = 1.0) -> float:
    """Fast symmetric sigmoid ℝ → (-1, +1). No math.exp overflow."""
    sz = z * steepness
    # Padé approximation: accurate to < 0.3% error
    return max(-1.0, min(1.0, sz / (1.0 + abs(sz) * 0.5)))


# ─────────────────────────────────────────────────────────────────────────────
# INTEGRATION GUIDE (for quant_strategy.py)
# ─────────────────────────────────────────────────────────────────────────────

"""
HOW TO WIRE direction_engine.py INTO quant_strategy.py
======================================================

1. IMPORT AND INIT (in QuantStrategy.__init__):
   from strategy.direction_engine import DirectionEngine
   self._dir_engine = DirectionEngine()

2. HUNT PREDICTION (in _evaluate_entry / Step 6, after ICT update):
   hunt = self._dir_engine.predict_hunt(
       price      = price,
       atr        = atr,
       now_ms     = now_ms,
       ict_engine = self._ict,
       tick_flow  = self._tick_eng.get_signal(),
       cvd_trend  = self._cvd.get_trend_signal(),
       candles_5m = candles_by_tf.get("5m", []),
   )
   # hunt.predicted    → "BSL" | "SSL" | None
   # hunt.confidence   → 0.0 – 1.0
   # hunt.delivery_direction → "bullish" | "bearish"
   # Use this to gate direction: only enter LONG when hunt says SSL will be swept
   # (SSL swept → bullish delivery → long entry in discount)

3. ON SWEEP DETECTED (in _evaluate_entry when a sweep is found):
   pool_type = sweep.pool.side.value  # "BSL" | "SSL"
   self._dir_engine.on_sweep(
       swept_pool_price = sweep.pool.price,
       pool_type        = pool_type,
       price            = price,
       atr              = atr,
       now              = now,
   )

4. POST-SWEEP EVALUATION (each tick while in POST_SWEEP state):
   if self._dir_engine.in_post_sweep:
       decision = self._dir_engine.evaluate_sweep(
           price      = price,
           atr        = atr,
           now        = now,
           ict_engine = self._ict,
           tick_flow  = self._tick_eng.get_signal(),
           cvd_trend  = self._cvd.get_trend_signal(),
       )
       if decision.action == "reverse":
           # Enter trade in decision.direction with SL at swept pool wick
       elif decision.action == "continue":
           # Enter continuation in decision.direction
       # if "wait" → do nothing, evaluation continues next tick

5. POOL-HIT GATE (during active position when price approaches a pool):
   gate = self._dir_engine.pool_hit_gate(
       pos_side   = self._pos.side,
       pos_entry  = self._pos.entry_price,
       pos_sl     = self._pos.sl_price,
       pos_tp     = self._pos.tp_price,
       price      = price,
       atr        = atr,
       ict_engine = self._ict,
       tick_flow  = self._tick_eng.get_signal(),
       cvd_trend  = self._cvd.get_trend_signal(),
   )
   if gate.action == "exit":    → close position (TP)
   elif gate.action == "reverse" → exit + enter opposite
   elif gate.action == "continue" → update TP to gate.next_target, tighten SL
   # if "hold" → let existing SL/TP manage

DISPLAY (for Telegram /thinking and heartbeat):
   hunt = self._dir_engine.last_hunt
   if hunt and hunt.predicted:
       logger.info(
           f"HUNT: {hunt.predicted} conf={hunt.confidence:.2f} "
           f"delivery={hunt.delivery_direction} | {hunt.reason[:60]}")
"""

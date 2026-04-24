"""
direction_engine.py — Institutional Direction & Hunt Decision Engine v2.0
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
  ─ OTE (Optimal Trade Entry) zone = 50%–78.6% Fibonacci retrace.
    Institutional orders are placed after a sweep in this retrace window.
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

SYNC v2.0 FIXES (all verified against entry_engine.py v3.4 and liquidity_map.py v2.1):
────────────────────────────────────────────────────────────────────────────────────────

FIX-1: OTE zone bounds aligned with entry_engine._PS_OTE_FIB_LOW.
  Old: _OTE_FIB_LOW = 0.618 (ICT OB-body standard).
  New: _OTE_FIB_LOW = 0.500 (matches entry_engine._PS_OTE_FIB_LOW = 0.50).
  Impact: When entry_engine detects OTE at 50% retrace, direction_engine NOW
  also detects it. Previously direction_engine was 5-15 seconds behind entry_engine
  on OTE detection, making the +15 pt OTE bonus arrive after entry was decided.

FIX-2: Post-sweep evidence thresholds recalibrated for tighter timing.
  Old: EARLY=65 / NORMAL=52 / MATURE=38 (direction_engine fired 15-22% later
       than entry_engine's 55/45/35, causing hints to arrive after entry).
  New: EARLY=62 / NORMAL=50 / MATURE=36.
  Entry_engine effective thresholds (with multipliers):
    EARLY=71.5, NORMAL=45, MATURE=26.25.
  Direction_engine effective thresholds (with multipliers):
    EARLY=80.6, NORMAL=50, MATURE=27.0.
  Direction_engine is SLIGHTLY more conservative (intended — it serves as a
  high-conviction confirmation layer). The gap is now small enough that hints
  arrive before entry_engine's window closes, not after.

FIX-3: LiquidityMapSnapshot integrated as optional parameter across all
  three engines. Pool asymmetry (Factor 5) now uses PoolTarget.adjusted_sig()
  when liq_snapshot is provided — this incorporates HTF confluence multipliers,
  OB/FVG alignment bonuses, and adjacency bonuses that ICT LiquidityLevel
  touch_count alone cannot capture. Dealing range fallback also prefers
  LiquidityMap pools over ICT pools when liq_snapshot is available.

FIX-4: _score_sweep_static() enhanced with LiquidityMap pool significance
  for opposing/continuation pool scoring. Old code used a fixed cap of 5.0 pts.
  New code scales linearly from pool.adjusted_sig() up to 8 pts for reversal
  target significance and 6 pts for continuation target significance.

FIX-5: pool_hit_gate() auto-resolves next_pool from liq_snapshot when
  next_pool_price is not provided. Uses the nearest qualifying opposing pool
  (distance > 0.5 ATR, significance >= 2.0) as the continuation target.

FIX-6: 15m structure check added to _score_sweep_dynamic(). Previously only
  5m CHoCH/BOS was checked post-sweep. 15m structure fires earlier (bars close
  every 15 min vs 5 min) and is less noisy. Contributes up to +8 pts per tick
  when 15m trend is in reversal direction.

FIX-7: Mixed-signal decay asymmetry fix (inherited from v1.0) documented and
  verified. entry_engine uses two separate `if` blocks which double-decay the
  reversal side on mixed ticks. direction_engine correctly handles mixed signals
  with one explicit branch per case. This is the CORRECT implementation.

FIX-8: Integration guide fully updated with liq_snapshot wiring.
  quant_strategy.py must pass self._liq_map._last_snapshot (previous tick) to
  predict_hunt() (called BEFORE liq_map.update()) and the fresh liq_snapshot
  to evaluate_sweep() and pool_hit_gate() (called AFTER liq_map.update()).
"""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

try:
    import config as _de_cfg
except Exception:  # pragma: no cover - config import can fail in isolated tests
    _de_cfg = None


def _cfg(name: str, default):
    """Read direction-engine config at call-time with a safe fallback."""
    if _de_cfg is None:
        return default
    val = getattr(_de_cfg, name, None)
    return default if val is None else val

# ─────────────────────────────────────────────────────────────────────────────
# OPTIONAL IMPORT — LiquidityMap types
# Used to enrich Factor 5 (pool asymmetry) and static sweep scoring.
# Gracefully degrades to ICT pool data if liquidity_map is unavailable.
# ─────────────────────────────────────────────────────────────────────────────

_LIQ_MAP_TYPES_AVAILABLE = False
try:
    from strategy.liquidity_map import LiquidityMapSnapshot, PoolTarget, PoolSide, PoolStatus
    _LIQ_MAP_TYPES_AVAILABLE = True
except ImportError:
    try:
        from liquidity_map import LiquidityMapSnapshot, PoolTarget, PoolSide, PoolStatus
        _LIQ_MAP_TYPES_AVAILABLE = True
    except ImportError:
        LiquidityMapSnapshot = None   # type: ignore
        PoolTarget           = None   # type: ignore
        PoolSide             = None   # type: ignore

# ─────────────────────────────────────────────────────────────────────────────
# ISSUE-1 FIX: MTF Pool Sweep Probability Engine
# Replaces Factor 5 "pool_asymmetry" with a true per-pool distance-decay ×
# TF-base-probability × significance × session-context model.
# If the module is absent the original significance-sum fallback still runs.
# ─────────────────────────────────────────────────────────────────────────────
_MTF_PROB_AVAILABLE = False
try:
    from strategy.mtf_pool_probability import compute_mtf_pool_factor, get_mtf_pool_forecast
    _MTF_PROB_AVAILABLE = True
except ImportError:
    try:
        from mtf_pool_probability import compute_mtf_pool_factor, get_mtf_pool_forecast
        _MTF_PROB_AVAILABLE = True
    except ImportError:
        compute_mtf_pool_factor  = None   # type: ignore
        get_mtf_pool_forecast    = None   # type: ignore


# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

# Hunt prediction
# THRESHOLD-FLAP FIX: Replace the single _HUNT_MIN_CONFIDENCE threshold with a
# Schmitt-trigger two-threshold design.
#   _HUNT_ON_THRESHOLD:  score must EXCEED this to commit (or flip) a direction.
#   _HUNT_OFF_THRESHOLD: score must DROP BELOW this to revert to NEUTRAL.
#   Band [_HUNT_OFF_THRESHOLD, _HUNT_ON_THRESHOLD): hold the last committed
#   direction at its committed confidence — no flip, no NEUTRAL.
# Effect: scores oscillating around 0.18 (e.g. 0.168 ↔ 0.175 ↔ 0.156) no longer
# cause BSL_HUNT ↔ NEUTRAL flips every few seconds; only a drop below 0.11
# genuinely reverts to NEUTRAL.  True NEUTRAL (score 0.109 < OFF) is preserved.
_HUNT_ON_THRESHOLD  = 0.10   # lowered to commit direction faster
_HUNT_OFF_THRESHOLD = 0.05   # lowered to stay committed longer
_HUNT_STRONG_CONFIDENCE = 0.35   # lowered for more directional calls
_HUNT_CACHE_SEC         = 5      # refresh prediction at most every 5 seconds

# Post-sweep evidence thresholds
# FIX-2: Recalibrated so direction_engine fires ~5-10s before entry_engine's
# window closes. entry_engine effective thresholds: EARLY=71.5, NORMAL=45, MATURE=26.25.
# direction_engine effective thresholds:            EARLY=80.6, NORMAL=50, MATURE=27.0.
_PS_REV_THRESHOLD_EARLY  = 35.0   # lowered for faster decisions
_PS_REV_THRESHOLD_NORMAL = 28.0   # lowered
_PS_REV_THRESHOLD_MATURE = 20.0   # lowered
_PS_CONT_THRESHOLD_RATIO = 0.90   # continuation needs 90% of reversal threshold
_PS_GAP_MIN              = 10.0   # minimum score separation between rev and cont
_PS_TIMEOUT_SEC          = 600.0  # 10 minutes — more time for evaluation
_PS_EVAL_DELAY_SEC       = 3.0    # faster first evaluation

# Phase timeline boundaries (seconds after sweep)
_PS_PHASE_DISPLACEMENT_SEC = 45.0    # 0 –  45s: expect price to reject hard
_PS_PHASE_CISD_SEC         = 120.0   # 45 – 120s: expect CHoCH/BOS confirmation
_PS_PHASE_OTE_SEC          = 240.0   # 120– 240s: expect retrace to OTE zone
# Phase score multipliers
_PS_DISP_SCORE_MULT  = 1.30   # DISPLACEMENT phase: need 1.30× score (overwhelming evidence)
_PS_MATURE_SCORE_MULT = 0.75  # MATURE phase: accept 0.75× score (stale setup last chance)

# Displacement quality
_DISP_STRONG_ATR         = 1.2    # strong displacement > 1.2 ATR from sweep level
_DISP_WEAK_ATR           = 0.4    # minimum to count as any displacement
_DISP_CISD_MAX_AGE_SEC   = 150    # CISD older than 2.5 min = stale, reduced weight

# OTE Fibonacci levels
# FIX-1: Aligned with entry_engine._PS_OTE_FIB_LOW = 0.50.
# Old: 0.618 (ICT OB-body standard). New: 0.500 (entry timing alignment).
_OTE_FIB_LOW  = 0.500   # 50% retrace  [was 0.618]
_OTE_FIB_HIGH = 0.786   # 78.6% retrace (unchanged)

# Continuation gate
_CONT_FLOW_REVERSAL_MIN = 0.40   # flow must flip this strongly to trigger reversal
_CONT_NEXT_POOL_MIN_RR  = 1.5    # next pool must offer at least 1.5R reward

# Static pool significance scoring caps for post-sweep evidence
_STATIC_REV_POOL_MAX   = 8.0   # max pts from opposing pool significance
_STATIC_CONT_POOL_MAX  = 6.0   # max pts from continuation pool significance

# Factor 5 (pool asymmetry) significance scoring constants
_TF_WEIGHT = {"1d": 5.0, "4h": 4.0, "1h": 3.0, "15m": 2.0, "5m": 1.0}


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
    predicted:           Optional[str]   # "BSL" | "SSL" | None
    confidence:          float           # 0.0 – 1.0 normalised absolute score
    delivery_direction:  str             # "bullish" | "bearish" | ""
    raw_score:           float           # underlying [-1, +1] score (+ = BSL hunt)
    bsl_score:           float           # 0–1 component for BSL
    ssl_score:           float           # 0–1 component for SSL
    factors:             HuntFactors     = field(default_factory=HuntFactors)
    dealing_range_pd:    float           = 0.5
    swept_pool_price:    Optional[float] = None   # predicted target pool price
    opposing_pool_price: Optional[float] = None   # post-hunt delivery destination
    reason:              str             = ""
    timestamp_ms:        int             = 0


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
    quality:            float = 0.5

    # ── Static baseline (scored ONCE after _PS_EVAL_DELAY_SEC) ──────
    static_rev_base:    float = 0.0
    static_cont_base:   float = 0.0
    static_scored:      bool  = False

    # ── Dynamic accumulator ──────────────────────────────────────────
    rev_evidence:       float = 0.0
    cont_evidence:      float = 0.0
    peak_rev:           float = 0.0
    peak_cont:          float = 0.0
    tick_count:         int   = 0

    # ── CISD tracking ────────────────────────────────────────────────
    cisd_detected:      bool  = False
    cisd_timestamp:     float = 0.0
    cisd_type:          str   = ""    # "choch" | "bos"

    # ── Displacement tracking ────────────────────────────────────────
    max_displacement_atr: float = 0.0
    disp_velocity_atr_s:  float = 0.0   # ATR per second

    # ── OTE zone tracking ────────────────────────────────────────────
    ote_reached:        bool  = False
    ote_timestamp:      float = 0.0
    ote_holding:        bool  = False

    # ── Price extremes since sweep ───────────────────────────────────
    highest_since:      float = 0.0
    lowest_since:       float = float('inf')

    # ── Flow accumulation ────────────────────────────────────────────
    rev_flow_ticks:     int   = 0
    cont_flow_ticks:    int   = 0


@dataclass
class PostSweepDecision:
    """Output of the Post-Sweep Scenario Engine."""
    action:           str    # "reverse" | "continue" | "wait"
    direction:        str    # "long" | "short" | ""
    confidence:       float  # 0.0 – 1.0
    phase:            str    # "DISPLACEMENT" | "CISD" | "OTE" | "MATURE"
    cisd_active:      bool   = False
    ote_active:       bool   = False
    displacement_atr: float  = 0.0
    rev_score:        float  = 0.0
    cont_score:       float  = 0.0
    rev_reasons:      List[str] = field(default_factory=list)
    cont_reasons:     List[str] = field(default_factory=list)
    reason:           str    = ""


@dataclass
class ContinuationDecision:
    """Output of the Pool-Hit Continuation Gate."""
    action:      str    # "exit" | "reverse" | "continue" | "hold"
    confidence:  float
    reason:      str
    next_target: Optional[float] = None   # if continuing, the next pool price


# ─────────────────────────────────────────────────────────────────────────────
# DIRECTION ENGINE (main class)
# ─────────────────────────────────────────────────────────────────────────────

class DirectionEngine:
    """
    Institutional Direction & Hunt Decision Engine v2.0.

    Single source of truth for ALL directional decisions in the strategy:
      1. predict_hunt()   → which pool (BSL/SSL) gets swept next?
      2. evaluate_sweep() → after a sweep: reverse, continue, or wait?
      3. pool_hit_gate()  → at a pool during a trade: TP/reverse/continue?

    v2.0 Sync Fixes: FIX-1 through FIX-8 (see module docstring).
    """

    # Factor weights — restored to the design specification in the module docstring.
    #
    # History of the deviation that caused Bug #2:
    #   dealing_range was set to 0.00 (labelled ISSUE-2) and its 0.15 weight
    #   was silently absorbed into amd (0.22→0.26) and htf_structure (0.18→0.21).
    #   However factors.dealing_range was also hardcoded to 0.0, so those extra
    #   weights contributed nothing — the composite permanently lost 0.15 signal
    #   mass, making AMD vs order_flow the only meaningful contributors.  Their
    #   opposing signs during REDISTRIBUTION-bearish + LONG-flow conditions caused
    #   the composite to cancel near zero and never breach the 0.18 threshold.
    _FACTOR_WEIGHTS = {
        "amd":            0.22,   # AMD phase + bias
        "htf_structure":  0.18,   # 4H + 1H + 15m structural cascade
        "dealing_range":  0.15,   # Premium / discount position — RESTORED from 0.00
        "order_flow":     0.13,   # Tick flow + CVD directional pressure
        "pool_asymmetry": 0.09,   # MTF pool significance asymmetry
        "ob_fvg_pull":    0.08,   # Unfilled OB / FVG delivery highways
        "displacement":   0.07,   # Recent closed-candle momentum
        "session":        0.04,   # Kill-zone behavioural tendencies
        "micro_bos":      0.03,   # 5m BOS direction
        "volume":         0.01,   # Net buy / sell volume asymmetry
    }
    assert abs(sum(_FACTOR_WEIGHTS.values()) - 1.0) < 1e-9, "Weights must sum to 1.0"

    def __init__(self) -> None:
        self._last_hunt:     Optional[HuntPrediction] = None
        self._last_hunt_ms:  int   = 0

        self._ps_state:          Optional[PostSweepState] = None
        self._ps_state_quality:  float = 0.0
        self._last_ps_analysis:  Dict  = {}

        # THRESHOLD-FLAP FIX: Schmitt-trigger hysteresis state.
        # Persists the last committed direction so the band [OFF, ON) holds it.
        # None = currently NEUTRAL (no committed direction).
        self._committed_direction:  Optional[str] = None   # "BSL" | "SSL" | None
        self._committed_confidence: float         = 0.0
        self._committed_score:      float         = 0.0

    # =========================================================================
    # 1. HUNT PREDICTION ENGINE
    # =========================================================================

    @staticmethod
    def _is_pool_target_active(t) -> bool:
        """BUG-4 FIX: Return False if the pool underlying this PoolTarget has been
        mutated to SWEPT or CONSUMED since the snapshot was built."""
        try:
            return t.pool.status not in (PoolStatus.SWEPT, PoolStatus.CONSUMED)
        except Exception:
            return True   # safe default

    def predict_hunt(
        self,
        price:          float,
        atr:            float,
        now_ms:         int,
        ict_engine,                                         # ICTEngine instance
        tick_flow:      float                   = 0.0,     # TickFlowEngine signal [-1,+1]
        cvd_trend:      float                   = 0.0,     # CVD slope signal [-1,+1]
        candles_5m:     Optional[List[Dict]]    = None,
        liq_snapshot                            = None,    # LiquidityMapSnapshot (FIX-3)
        force_refresh:  bool                    = False,
    ) -> HuntPrediction:
        """
        Predict which liquidity pool (BSL or SSL) will be swept next.

        FIX-3: When liq_snapshot is provided (LiquidityMapSnapshot from
        LiquidityMap.get_snapshot()), Factor 5 (pool asymmetry) uses
        PoolTarget.adjusted_sig() for rich significance scoring. The dealing
        range fallback also prefers LiquidityMap pools over ICT pools.

        Score interpretation:
          score > 0: BSL hunt more likely (price heading up to run buy stops)
          score < 0: SSL hunt more likely (price heading down to run sell stops)
          abs(score) < _HUNT_MIN_CONFIDENCE: uncertain → None
        """
        # DE-2 FIX: Don't serve a cached NEUTRAL prediction — the inputs may
        # have changed (new sweep, new DR, new flow). A cached NEUTRAL prevents
        # the engine from ever committing a direction within the cache window.
        # Only cache when we have an actual committed direction (predicted set).
        age_ms = now_ms - self._last_hunt_ms
        if (not force_refresh
                and self._last_hunt is not None
                and self._last_hunt.predicted is not None
                and age_ms < _HUNT_CACHE_SEC * 1000):
            return self._last_hunt

        if not ict_engine or not getattr(ict_engine, '_initialized', False):
            return self._null_prediction(now_ms, reason="ict_not_initialized")

        a = max(atr, 1e-9)
        factors = HuntFactors()

        # ── Fetch Dealing Range ───────────────────────────────────────────────
        # Primary: DealingRange from ICT engine (_dealing_range attribute).
        # Fallback 1: LiquidityMap snapshot BSL/SSL nearest pools. (FIX-3)
        # Fallback 2: ICT liquidity_pools (LiquidityLevel objects).
        dr_pd     = 0.5
        bsl_price = 0.0
        ssl_price = 0.0

        dr = getattr(ict_engine, '_dealing_range', None)
        if dr is not None:
            bsl_price = getattr(dr, 'high', 0.0)
            ssl_price = getattr(dr, 'low',  0.0)
            dr_pd     = getattr(dr, 'current_pd', 0.5)
        elif liq_snapshot is not None:
            # FIX-3: Use LiquidityMap snapshot for dealing range
            # BUG-4 FIX: exclude mutated-SWEPT pools from _last_snapshot
            _bsl_pools = [t for t in liq_snapshot.bsl_pools
                          if t.pool.price > price and self._is_pool_target_active(t)]
            _ssl_pools = [t for t in liq_snapshot.ssl_pools
                          if t.pool.price < price and self._is_pool_target_active(t)]
            if _bsl_pools and _ssl_pools:
                bsl_price = min(_bsl_pools, key=lambda t: t.distance_atr).pool.price
                ssl_price = min(_ssl_pools, key=lambda t: t.distance_atr).pool.price
                rng  = max(bsl_price - ssl_price, 1e-9)
                dr_pd = (price - ssl_price) / rng
        else:
            # Fallback to ICT liquidity_pools (LiquidityLevel objects)
            _all_ict = list(getattr(ict_engine, 'liquidity_pools', []))
            _unswept  = [p for p in _all_ict if not getattr(p, 'swept', False)]
            _bsl_above = sorted([p for p in _unswept
                                 if getattr(p, 'level_type', '') == "BSL"
                                 and p.price > price],
                                key=lambda p: p.price)
            _ssl_below = sorted([p for p in _unswept
                                 if getattr(p, 'level_type', '') == "SSL"
                                 and p.price < price],
                                key=lambda p: -p.price)
            if _bsl_above and _ssl_below:
                bsl_price = _bsl_above[0].price
                ssl_price = _ssl_below[0].price
                rng  = max(bsl_price - ssl_price, 1e-9)
                dr_pd = (price - ssl_price) / rng

        if bsl_price <= 0 or ssl_price <= 0:
            dr_pd = 0.5  # ISSUE-2: no structural bias assumed; continue scoring on remaining factors

        # ─────────────────────────────────────────────────────────────────────
        # FACTOR 1: AMD Phase + Bias  (weight 0.22)
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
                    factors.amd = +0.90 * _conf   # SSL was Judas, delivering UP → BSL
                elif _bias == 'bearish':
                    factors.amd = -0.90 * _conf   # BSL was Judas, delivering DOWN → SSL
            elif _phase == 'DISTRIBUTION':
                if _bias == 'bullish':
                    factors.amd = +0.70 * _conf   # distributing UP toward BSL
                elif _bias == 'bearish':
                    factors.amd = -0.70 * _conf   # distributing DOWN toward SSL
            elif _phase == 'REACCUMULATION':
                factors.amd = +0.45 * _conf       # mid-trend bull pause → BSL next
            elif _phase == 'REDISTRIBUTION':
                factors.amd = -0.45 * _conf       # mid-trend bear pause → SSL next
            elif _phase == 'ACCUMULATION':
                factors.amd = 0.0

            factors.amd = max(-1.0, min(1.0, factors.amd))

        # ─────────────────────────────────────────────────────────────────────
        # FACTOR 2: HTF Structure Cascade  (weight 0.18)
        # ─────────────────────────────────────────────────────────────────────
        # 4H is the institutional anchor (highest weight within this factor).
        # Score: positive = bullish alignment → price heading UP → BSL hunt.
        _tf = getattr(ict_engine, '_tf', {})
        _struct_votes = []
        _tf_weights   = {"4h": 0.45, "1h": 0.30, "15m": 0.25}
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
            if _bos == 'bullish':
                _vote += +0.25
            elif _bos == 'bearish':
                _vote -= 0.25
            # CHoCH is an early reversal warning — reduces the vote
            if _choch == 'bearish' and _trend == 'bullish':
                _vote -= 0.15
            elif _choch == 'bullish' and _trend == 'bearish':
                _vote += 0.15
            _struct_votes.append(_tf_w * max(-1.0, min(1.0, _vote)))

        factors.htf_structure = (sum(_struct_votes) if _struct_votes else 0.0)
        factors.htf_structure = max(-1.0, min(1.0, factors.htf_structure))

        # ─────────────────────────────────────────────────────────────────────
        # FACTOR 3: Dealing Range P/D Position  (weight 0.15)
        # ─────────────────────────────────────────────────────────────────────
        # dr_pd = 0.0 (deep discount) → price heading UP toward BSL → +1.0
        # dr_pd = 0.5 (equilibrium)   → no structural bias           →  0.0
        # dr_pd = 1.0 (deep premium)  → price heading DOWN to SSL    → -1.0
        #
        # Sigmoid centred at 0.5 with a ×4 pre-scale: values within ±0.10 of
        # equilibrium produce a near-zero score (the "no-man's-land" zone).
        # Values approaching structural extremes saturate at ±1.  This matches
        # the ICT principle — only trade from premium / discount zones, never
        # from mid-range equilibrium where both sides have equal claims.
        #
        # Guard: if neither BSL nor SSL levels were found, the dr_pd default is
        # 0.5 (equilibrium) and we emit 0.0 rather than a spurious directional
        # bias — "no data" must not be interpreted as "buy from discount".
        if bsl_price > 0 and ssl_price > 0:
            factors.dealing_range = _sigmoid((0.5 - dr_pd) * 4.0, steepness=1.0)
        else:
            factors.dealing_range = 0.0   # No structural levels found — neutral

        # ─────────────────────────────────────────────────────────────────────
        # FACTOR 4: Order Flow Vector  (weight 0.13)
        # ─────────────────────────────────────────────────────────────────────
        _of_composite = tick_flow * 0.55 + cvd_trend * 0.45
        factors.order_flow = _sigmoid(_of_composite, steepness=1.3)

        # ─────────────────────────────────────────────────────────────────────
        # FACTOR 5: MTF Pool Sweep Probability  (weight 0.09)
        # ─────────────────────────────────────────────────────────────────────
        # ISSUE-1 FIX: Replaced flat significance-sum with a two-layer
        # probability model that uses distance decay × TF base probability ×
        # significance × recency × touch momentum per pool, then aggregates
        # into a BSL vs SSL net score in [-1, +1].
        #
        # Decay half-lives (ATR):  2m=0.5  15m=1.2  1h=2.5  4h=4.0  1d=6.0
        # TF base priors:          2m=0.35 15m=0.55 1h=0.70 4h=0.80 1d=0.65
        # Session: London +20% BSL (Judas swing); Asia -20% all; NY neutral.
        #
        # net_score > 0 → BSL more likely swept next (price going UP)
        # net_score < 0 → SSL more likely swept next (price going DOWN)
        # Weight is unchanged at 0.09; signal quality improves dramatically.
        if _MTF_PROB_AVAILABLE:
            factors.pool_asymmetry = compute_mtf_pool_factor(
                liq_snapshot = liq_snapshot,
                price        = price,
                atr          = a,
                now          = now_ms / 1000.0,
                ict_engine   = ict_engine,
                # MOD-6 FIX: pass canonical _session (full window) not _killzone (KZ-only)
                session      = str(getattr(ict_engine, '_session', '') or ''),
            )
        elif liq_snapshot is not None:
            # Fallback A: LiquidityMap PoolTarget.adjusted_sig() significance sum
            # BUG-4 FIX: exclude mutated-SWEPT pools from _last_snapshot
            _bsl_sig_lm = sum(t.adjusted_sig()
                              for t in liq_snapshot.bsl_pools
                              if t.pool.price > price
                              and self._is_pool_target_active(t))
            _ssl_sig_lm = sum(t.adjusted_sig()
                              for t in liq_snapshot.ssl_pools
                              if t.pool.price < price
                              and self._is_pool_target_active(t))
            _total_lm = _bsl_sig_lm + _ssl_sig_lm
            if _total_lm > 1e-10:
                factors.pool_asymmetry = _sigmoid(
                    (_bsl_sig_lm - _ssl_sig_lm) / _total_lm * 3.0, steepness=1.0)
            else:
                factors.pool_asymmetry = 0.0
        else:
            # Fallback B: ICT LiquidityLevel pool scoring (no LiquidityMap)
            _all_ict     = list(getattr(ict_engine, 'liquidity_pools', []))
            _unswept_ict = [p for p in _all_ict if not getattr(p, 'swept', False)]

            def _ict_pool_sig(p: object) -> float:
                dist_atr = abs(getattr(p, 'price', 0.0) - price) / a
                tf_w     = _TF_WEIGHT.get(getattr(p, 'timeframe', '5m'), 1.0)
                tc       = float(getattr(p, 'touch_count', 1))
                return tc * tf_w * math.exp(-dist_atr / 8.0)

            bsl_sig = sum(_ict_pool_sig(p) for p in _unswept_ict
                          if getattr(p, 'level_type', '') == "BSL"
                          and getattr(p, 'price', 0.0) > price)
            ssl_sig = sum(_ict_pool_sig(p) for p in _unswept_ict
                          if getattr(p, 'level_type', '') == "SSL"
                          and getattr(p, 'price', 0.0) < price)
            total_sig = bsl_sig + ssl_sig
            if total_sig > 1e-10:
                factors.pool_asymmetry = _sigmoid(
                    (bsl_sig - ssl_sig) / total_sig * 3.0, steepness=1.0)
            else:
                factors.pool_asymmetry = 0.0

        # ─────────────────────────────────────────────────────────────────────
        # FACTOR 6: OB/FVG Magnetic Pull  (weight 0.08)
        # ─────────────────────────────────────────────────────────────────────
        # Unfilled bullish OBs and FVGs between price and BSL = delivery highway.
        # Unfilled bearish OBs and FVGs between price and SSL = delivery highway.
        _ob_bull  = getattr(ict_engine, 'order_blocks_bull', [])
        _ob_bear  = getattr(ict_engine, 'order_blocks_bear', [])
        _fvg_bull = getattr(ict_engine, 'fvgs_bull', [])
        _fvg_bear = getattr(ict_engine, 'fvgs_bear', [])

        bull_pull = 0.0
        bear_pull = 0.0
        try:
            for ob in _ob_bull:
                if (ob.is_active(now_ms) and price < ob.midpoint < bsl_price):
                    bull_pull += (ob.strength / 100.0) * ob.virgin_multiplier()
            for ob in _ob_bear:
                if (ob.is_active(now_ms) and ssl_price < ob.midpoint < price):
                    bear_pull += (ob.strength / 100.0) * ob.virgin_multiplier()
            for fvg in _fvg_bull:
                if (fvg.is_active(now_ms) and not fvg.filled
                        and price < fvg.bottom < bsl_price):
                    bull_pull += (1.0 - fvg.fill_percentage) * 0.60
            for fvg in _fvg_bear:
                if (fvg.is_active(now_ms) and not fvg.filled
                        and ssl_price < fvg.top < price):
                    bear_pull += (1.0 - fvg.fill_percentage) * 0.60
        except Exception:
            pass

        factors.ob_fvg_pull = _sigmoid(bull_pull - bear_pull, steepness=0.7)

        # ─────────────────────────────────────────────────────────────────────
        # FACTOR 7: Displacement Bias  (weight 0.07)
        # ─────────────────────────────────────────────────────────────────────
        # Net candle body direction of the last 4 CLOSED 5m bars (institutional
        # displacement logic — uses closed bars only to avoid live-bar noise).
        factors.displacement = 0.0
        _c5 = candles_5m or []
        if len(_c5) >= 6 and a > 1e-10:
            closed_bars = _c5[-5:-1]   # 4 most recent CLOSED bars (exclude live)
            net_body    = sum(float(c['c']) - float(c['o']) for c in closed_bars)
            factors.displacement = _sigmoid(net_body / (a * 2.0), steepness=1.0)
        elif _tf.get("5m"):
            _t5 = _tf["5m"]
            if   getattr(_t5, 'trend', '') == 'bullish': factors.displacement = +0.50
            elif getattr(_t5, 'trend', '') == 'bearish': factors.displacement = -0.50

        # ─────────────────────────────────────────────────────────────────────
        # FACTOR 8: Session Timing  (weight 0.04)
        # ─────────────────────────────────────────────────────────────────────
        # London KZ: Judas-swing bias — tends to run buy stops first → +BSL bias
        # NY KZ:     Amplifies the dominant directional composite already scored
        # Asia KZ:   Range / accumulation — no directional bias → 0.0
        # Weekend:   No named kill zone — no session-specific bias added → 0.0
        #            The core factors (AMD, HTF structure, dealing range, flow)
        #            still score normally.  Only the 0.04-weight session bonus
        #            is absent.  Crypto liquidity hunts are valid 24/7.
        # Off-hours: Same as weekend — neutral, no session bonus.
        #
        # Source priority: _session (full window) > _killzone (KZ-only string).
        factors.session = 0.0
        try:
            _sess = str(getattr(ict_engine, '_session', '') or '').upper()
            if not _sess:
                _sess = str(getattr(ict_engine, '_killzone', '') or '').upper()

            if 'LONDON' in _sess and 'NY' not in _sess:
                factors.session = +0.35   # London Judas-swing: tends to hunt BSL first
            elif 'NY' in _sess or 'NEW_YORK' in _sess:
                # NY open amplifies whichever direction AMD + HTF already favour
                dominant = (
                    factors.amd           * self._FACTOR_WEIGHTS['amd'] +
                    factors.htf_structure * self._FACTOR_WEIGHTS['htf_structure']
                )
                factors.session = _sigmoid(dominant * 2.5, steepness=1.0)
            # ASIA / WEEKEND / OFF_HOURS / blank → factors.session stays 0.0
        except Exception:
            pass

        # ─────────────────────────────────────────────────────────────────────
        # FACTOR 9: Micro-Structure BOS  (weight 0.03)
        # ─────────────────────────────────────────────────────────────────────
        factors.micro_bos = 0.0
        _t5 = _tf.get("5m")
        if _t5 is not None:
            _bos_d = getattr(_t5, 'bos_direction', '')
            if   _bos_d == 'bullish': factors.micro_bos = +0.80
            elif _bos_d == 'bearish': factors.micro_bos = -0.80

        # ─────────────────────────────────────────────────────────────────────
        # FACTOR 10: Volume Expansion Asymmetry  (weight 0.01)
        # ─────────────────────────────────────────────────────────────────────
        # BUG-5 FIX: the original implementation read ICTEngine._tick_flow and
        # ._cvd_trend — private attributes set only by set_order_flow_data().
        # This was:
        #   (a) Redundant with Factor 4 (order_flow), which already combines
        #       tick_flow and cvd_trend with a higher 0.13 weight.
        #   (b) Fragile: if ICTEngine renames those attrs, Factor 10 silently
        #       returns 0.0 with no warning and no test failure.
        #
        # Replacement: signed volume-expansion ratio.
        #   • Compute the 2-bar trailing average volume vs the 20-bar baseline.
        #   • Sign the ratio by the net candle body of the same 2 bars so that
        #     expanding volume INTO a bullish displacement scores positive (→ BSL)
        #     and expanding volume INTO a bearish displacement scores negative (→ SSL).
        #   • Uses the same _c5 list already bound above (candles_5m or []).
        #   • Requires ≥ 22 closed bars; falls back to 0.0 otherwise.
        #   • Clipped to [-1, +1] before the sigmoid so extreme outlier volumes
        #     (news candles) don't dominate the 0.01-weight factor.
        factors.volume = 0.0
        try:
            # Need at minimum: 20 baseline bars + 2 recent closed bars +
            # 1 live bar to exclude.  _c5[-1] is the live bar — never use it.
            if len(_c5) >= 23 and a > 1e-10:
                _vols_raw = [float(c.get('v', 0)) for c in _c5[-23:]]
                # Split into 20-bar baseline and 2 most-recent CLOSED bars.
                # Index layout (after slicing 23 bars):
                #   [0..19]  = 20 baseline bars (oldest → newest)
                #   [20, 21] = 2 most-recent CLOSED bars
                #   [22]     = live bar (excluded)
                _baseline_vols = _vols_raw[:20]
                _recent_vols   = _vols_raw[20:22]
                _avg_baseline  = sum(_baseline_vols) / 20.0
                if _avg_baseline > 1e-10:
                    _avg_recent = sum(_recent_vols) / max(len(_recent_vols), 1)
                    # Expansion ratio centred at 0: ratio 1.0 → 0.0, 2.0 → +1.0
                    _raw_expansion = (_avg_recent / _avg_baseline) - 1.0
                    _raw_expansion = max(-1.0, min(1.0, _raw_expansion))

                    # Direction from net closed-bar body of the same 2 bars.
                    _recent_candles = _c5[-3:-1]   # 2 closed bars (live excluded)
                    _net_body = sum(
                        float(c.get('c', 0)) - float(c.get('o', 0))
                        for c in _recent_candles
                    )
                    # Sign the expansion: positive expansion in bull direction → +
                    _signed = math.copysign(_raw_expansion, _net_body)
                    factors.volume = _sigmoid(_signed, steepness=1.5)
        except Exception:
            pass

        # ─────────────────────────────────────────────────────────────────────
        # WEIGHTED COMPOSITE SCORE
        # ─────────────────────────────────────────────────────────────────────
        score = (
            factors.amd            * self._FACTOR_WEIGHTS["amd"]            +
            factors.htf_structure  * self._FACTOR_WEIGHTS["htf_structure"]  +
            factors.dealing_range  * self._FACTOR_WEIGHTS["dealing_range"]  +
            factors.order_flow     * self._FACTOR_WEIGHTS["order_flow"]     +
            factors.pool_asymmetry * self._FACTOR_WEIGHTS["pool_asymmetry"] +
            factors.ob_fvg_pull    * self._FACTOR_WEIGHTS["ob_fvg_pull"]    +
            factors.displacement   * self._FACTOR_WEIGHTS["displacement"]   +
            factors.session        * self._FACTOR_WEIGHTS["session"]        +
            factors.micro_bos      * self._FACTOR_WEIGHTS["micro_bos"]      +
            factors.volume         * self._FACTOR_WEIGHTS["volume"]
        )
        score = max(-1.0, min(1.0, score))

        confidence = abs(score)
        bsl_score  = max(0.0,  score)
        ssl_score  = max(0.0, -score)

        # ─────────────────────────────────────────────────────────────────────
        # HTF STRUCTURE OVERRIDE
        # ─────────────────────────────────────────────────────────────────────
        # When all 3 HTF TFs are aligned, structural reality overrides the
        # composite score. This prevents AMD manipulation cycles from causing
        # directional calls that fight the dominant trend.
        _bull_count = sum(1 for _tn in ("4h", "1h", "15m")
                          if _tf.get(_tn)
                          and getattr(_tf[_tn], 'trend', '') == 'bullish')
        _bear_count = sum(1 for _tn in ("4h", "1h", "15m")
                          if _tf.get(_tn)
                          and getattr(_tf[_tn], 'trend', '') == 'bearish')

        _override_applied = False
        if _bear_count >= 3 and score > 0.10:
            logger.debug(
                f"DirectionEngine: HTF OVERRIDE → SSL hunt "
                f"(all 3 TFs bearish, raw={score:+.3f})")
            # Bug #35 fix: scale factor raised 0.70 → 0.90.
            # At 0.70, a raw score of 0.18 (the ON threshold) becomes 0.126,
            # falling into the [OFF=0.11, ON=0.18) hysteresis band — the signal
            # is neither committed nor NEUTRAL and never resolves.  At 0.90 the
            # override score remains 0.162 which is close to ON but clearly above
            # OFF, allowing the hysteresis trigger to commit a modest SSL signal
            # rather than getting stuck in limbo.
            score = -abs(score) * 0.90
            _override_applied = True
        elif _bull_count >= 3 and score < -0.10:
            logger.debug(
                f"DirectionEngine: HTF OVERRIDE → BSL hunt "
                f"(all 3 TFs bullish, raw={score:+.3f})")
            score = +abs(score) * 0.90   # Bug #35 fix: 0.70 → 0.90
            _override_applied = True

        # Recompute after potential override
        confidence = abs(score)
        bsl_score  = max(0.0,  score)
        ssl_score  = max(0.0, -score)

        # ─────────────────────────────────────────────────────────────────────
        # BUILD RESULT
        # ─────────────────────────────────────────────────────────────────────
        # ─────────────────────────────────────────────────────────────────────
        # SCHMITT-TRIGGER HYSTERESIS  (THRESHOLD-FLAP FIX)
        # ─────────────────────────────────────────────────────────────────────
        # Three zones:
        #   confidence >= ON  (0.18): commit/update the direction.
        #   confidence <  OFF (0.11): revert to NEUTRAL regardless of last state.
        #   Band [OFF, ON)          : hold the last committed direction at its
        #                             last committed confidence — no flip, no NEUTRAL.
        #
        # This eliminates the BSL_HUNT ↔ NEUTRAL oscillation observed when the
        # composite score hovers at ~0.168–0.175, which was straddling the old
        # single 0.18 threshold every few seconds.
        if confidence >= _HUNT_ON_THRESHOLD:
            # New or updated committed direction.
            self._committed_direction  = "BSL" if score > 0 else "SSL"
            self._committed_confidence = confidence
            self._committed_score      = score
        elif confidence < _HUNT_OFF_THRESHOLD:
            # Score fell below the OFF threshold — genuine NEUTRAL.
            if self._committed_direction is not None:
                logger.debug(
                    f"DirectionEngine: NEUTRAL (hysteresis OFF triggered: "
                    f"composite={confidence:.3f} < {_HUNT_OFF_THRESHOLD}) | "
                    f"prev={self._committed_direction} conf={self._committed_confidence:.3f}")
            self._committed_direction  = None
            self._committed_confidence = 0.0
            self._committed_score      = 0.0
        else:
            # Band [OFF, ON): hold the last committed state.
            if self._committed_direction is None:
                # No prior committed state — treat as NEUTRAL.
                logger.debug(
                    f"DirectionEngine: NEUTRAL (hysteresis band, no prior state: "
                    f"composite={confidence:.3f} in [{_HUNT_OFF_THRESHOLD},{_HUNT_ON_THRESHOLD}))")
            else:
                # Hold: restore committed confidence/score for result building.
                logger.debug(
                    f"DirectionEngine: HOLD {self._committed_direction} "
                    f"(hysteresis band: live={confidence:.3f} in "
                    f"[{_HUNT_OFF_THRESHOLD},{_HUNT_ON_THRESHOLD}), "
                    f"held conf={self._committed_confidence:.3f})")
                confidence = self._committed_confidence
                score      = self._committed_score
                # Recompute bsl/ssl from held score
                bsl_score = max(0.0,  score)
                ssl_score = max(0.0, -score)

        if self._committed_direction is None:
            # NEUTRAL — log actual composite so operators can see how far below ON we are.
            logger.debug(
                f"DirectionEngine: NEUTRAL | "
                f"composite={abs(self._committed_score) if self._committed_score else confidence:.3f} "
                f"(ON={_HUNT_ON_THRESHOLD} OFF={_HUNT_OFF_THRESHOLD}) "
                f"raw={score:+.3f} | "
                f"AMD={factors.amd:+.2f} "
                f"HTF={factors.htf_structure:+.2f} "
                f"DR={factors.dealing_range:+.2f} "
                f"flow={factors.order_flow:+.2f} "
                f"pool={factors.pool_asymmetry:+.2f}")
            result = self._null_prediction(
                now_ms,
                reason=f"low_confidence(composite={confidence:.3f}<{_HUNT_ON_THRESHOLD})",
                dr_pd=dr_pd, bsl_score=bsl_score, ssl_score=ssl_score,
                score=score, factors=factors)
            self._last_hunt    = result
            self._last_hunt_ms = now_ms
            return result

        if self._committed_direction == "BSL":
            predicted          = "BSL"
            # DE-1 FIX (CRITICAL): BSL pools sit ABOVE current price.
            # A BSL HUNT = price hunts stops ABOVE = price is going UP to hunt them.
            # Therefore delivery_direction is BULLISH, not bearish.
            # Previous value "bearish" inverted the entire downstream signal:
            # 210/210 committed directions in the log had the wrong label, which
            # notifier.py and telegram displays propagated unchanged.
            delivery_direction = "bullish"
            # FIX-3: Use LiquidityMap pool prices when available
            swept_pool    = bsl_price
            opposing_pool = ssl_price
            # BUG-4 FIX: only resolve from non-SWEPT pools
            if liq_snapshot is not None and liq_snapshot.bsl_pools:
                _absl = [t for t in liq_snapshot.bsl_pools if self._is_pool_target_active(t)]
                if _absl:
                    swept_pool = min(_absl, key=lambda t: t.distance_atr).pool.price
            if liq_snapshot is not None and liq_snapshot.ssl_pools:
                _assl = [t for t in liq_snapshot.ssl_pools if self._is_pool_target_active(t)]
                if _assl:
                    opposing_pool = min(_assl, key=lambda t: t.distance_atr).pool.price
            reason = (
                f"BSL_HUNT (score={score:+.3f}) | "
                f"AMD={getattr(amd,'phase','?')}({getattr(amd,'bias','?')},"
                f"{getattr(amd,'confidence',0):.2f}) | "
                f"HTF={factors.htf_structure:+.2f} | "
                f"DR={dr_pd:.2f} | flow={factors.order_flow:+.2f}"
            )
        else:
            predicted          = "SSL"
            # DE-1 FIX (CRITICAL): SSL pools sit BELOW current price.
            # An SSL HUNT = price hunts stops BELOW = price is going DOWN to hunt them.
            # Therefore delivery_direction is BEARISH, not bullish.
            delivery_direction = "bearish"
            swept_pool    = ssl_price
            opposing_pool = bsl_price
            # BUG-4 FIX: only resolve from non-SWEPT pools
            if liq_snapshot is not None and liq_snapshot.ssl_pools:
                _assl = [t for t in liq_snapshot.ssl_pools if self._is_pool_target_active(t)]
                if _assl:
                    swept_pool = min(_assl, key=lambda t: t.distance_atr).pool.price
            if liq_snapshot is not None and liq_snapshot.bsl_pools:
                _absl = [t for t in liq_snapshot.bsl_pools if self._is_pool_target_active(t)]
                if _absl:
                    opposing_pool = min(_absl, key=lambda t: t.distance_atr).pool.price
            reason = (
                f"SSL_HUNT (score={score:+.3f}) | "
                f"AMD={getattr(amd,'phase','?')}({getattr(amd,'bias','?')},"
                f"{getattr(amd,'confidence',0):.2f}) | "
                f"HTF={factors.htf_structure:+.2f} | "
                f"DR={dr_pd:.2f} | flow={factors.order_flow:+.2f}"
            )

        if _override_applied:
            reason += " | HTF_STRUCT_OVERRIDE"

        result = HuntPrediction(
            predicted           = predicted,
            confidence          = round(min(1.0, confidence), 4),
            delivery_direction  = delivery_direction,
            raw_score           = round(score, 4),
            bsl_score           = round(bsl_score, 4),
            ssl_score           = round(ssl_score, 4),
            factors             = factors,
            dealing_range_pd    = round(dr_pd, 4),
            swept_pool_price    = swept_pool,
            opposing_pool_price = opposing_pool,
            reason              = reason,
            timestamp_ms        = now_ms,
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
        quality:          float = 0.5,
    ) -> None:
        """
        Call immediately when a pool sweep is detected.

        Concurrent sweep priority: if an existing PostSweepState is < 15s old
        AND the incoming sweep has a lower quality score, retain the existing
        state. This prevents a weak simultaneous sweep from wiping accumulated
        evidence of a strong prior sweep.

        quality: displacement_score from pool (0–1); higher = better sweep.
        Set from LiquidityMap SweepResult.quality or ICT pool.displacement_score.
        """
        if self._ps_state is not None:
            existing_age = now - self._ps_state.entered_at
            if existing_age < 15.0 and quality <= self._ps_state_quality:
                logger.debug(
                    f"DirectionEngine: sweep {pool_type} @ ${swept_pool_price:,.1f} "
                    f"DISCARDED — existing {self._ps_state.swept_pool_type} state "
                    f"age={existing_age:.1f}s quality={quality:.2f} "
                    f"<= existing={self._ps_state_quality:.2f}")
                return

        # Bug #28 fix: when a higher-quality sweep supersedes an existing state,
        # carry the accumulated dynamic evidence forward rather than zeroing it.
        # Wiping evidence means the first 30–60s of confirmatory signals (CISD,
        # displacement ticks, OTE entry) are lost when a stronger sweep fires,
        # making the decision engine restart from scratch with no context.
        # We preserve dynamic evidence only when the NEW sweep is in the SAME
        # direction — a direction reversal should always start fresh.
        _carry_rev  = 0.0
        _carry_cont = 0.0
        if self._ps_state is not None and self._ps_state.swept_pool_type == pool_type:
            _carry_rev  = self._ps_state.rev_evidence  * 0.5   # 50% carry: discounted
            _carry_cont = self._ps_state.cont_evidence * 0.5   # same-direction evidence

        self._ps_state_quality = quality
        new_state = PostSweepState(
            swept_pool_price = swept_pool_price,
            swept_pool_type  = pool_type,
            entered_at       = now,
            quality          = float(quality),
            highest_since    = price,
            # Bug #33 fix: lowest_since must start at float('inf'), not price.
            lowest_since     = float('inf'),
        )
        # Apply carried evidence from the superseded state
        new_state.rev_evidence  = _carry_rev
        new_state.cont_evidence = _carry_cont
        self._ps_state = new_state
        logger.info(
            f"🌊 DirectionEngine: POST-SWEEP STARTED | "
            f"{pool_type} @ ${swept_pool_price:,.1f} | "
            f"price=${price:,.2f} | quality={quality:.2f}"
            + (f" | carried rev={_carry_rev:.1f} cont={_carry_cont:.1f}"
               if _carry_rev > 0 or _carry_cont > 0 else "")
        )

    def clear_sweep(self) -> None:
        """Call when post-sweep evaluation is complete or aborted externally."""
        self._ps_state = None

    def evaluate_sweep(
        self,
        price:        float,
        atr:          float,
        now:          float,
        ict_engine,
        tick_flow:    float = 0.0,
        cvd_trend:    float = 0.0,
        liq_snapshot        = None,    # LiquidityMapSnapshot (FIX-3)
    ) -> PostSweepDecision:
        """
        Evaluate the current post-sweep state: reverse, continue, or wait.

        FIX-3: liq_snapshot is passed to _score_sweep_static() for pool
        significance scoring. FIX-6: 15m structure added to dynamic scoring.

        PHASE TIMELINE:
          0 –   8s: Initial delay (wait for price to react to sweep)
          8 –  45s: DISPLACEMENT — expect strong price rejection
          45 – 120s: CISD       — expect CHoCH/BOS confirming reversal
          120 – 240s: OTE       — expect retrace to 50%–78.6% Fib zone (FIX-1)
          240 – 360s: MATURE    — relaxed thresholds, final chance
          >360s: TIMEOUT
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
                reason=f"waiting {_PS_EVAL_DELAY_SEC - elapsed:.1f}s more")

        a = max(atr, 1e-9)
        ps.tick_count += 1

        # Reversal/continuation directions
        rev_dir  = "short" if ps.swept_pool_type == "BSL" else "long"
        cont_dir = "long"  if ps.swept_pool_type == "BSL" else "short"

        # ── Update price extremes ──────────────────────────────────────────
        ps.highest_since = max(ps.highest_since, price)
        ps.lowest_since  = min(ps.lowest_since,  price)

        # ── Current phase ──────────────────────────────────────────────────
        if elapsed < _PS_PHASE_DISPLACEMENT_SEC:
            phase          = "DISPLACEMENT"
            base_threshold = _PS_REV_THRESHOLD_EARLY
            score_mult     = _PS_DISP_SCORE_MULT
        elif elapsed < _PS_PHASE_CISD_SEC:
            phase          = "CISD"
            base_threshold = _PS_REV_THRESHOLD_NORMAL
            score_mult     = 1.0
        elif elapsed < _PS_PHASE_OTE_SEC:
            phase          = "OTE"
            base_threshold = _PS_REV_THRESHOLD_NORMAL
            score_mult     = 1.0
        else:
            phase          = "MATURE"
            base_threshold = _PS_REV_THRESHOLD_MATURE
            score_mult     = _PS_MATURE_SCORE_MULT

        # ─────────────────────────────────────────────────────────────────────
        # STEP 1: Displacement tracking
        # ─────────────────────────────────────────────────────────────────────
        sweep_px = ps.swept_pool_price
        if rev_dir == "long":
            disp = (ps.highest_since - sweep_px) / a
        else:
            disp = (sweep_px - ps.lowest_since) / a

        if disp > ps.max_displacement_atr:
            ps.max_displacement_atr = disp
            if elapsed > 1.0:
                ps.disp_velocity_atr_s = disp / elapsed

        # ─────────────────────────────────────────────────────────────────────
        # STEP 2: CISD Detection (Change in State of Delivery)
        # ─────────────────────────────────────────────────────────────────────
        # Only count CHoCH/BOS events that occurred AFTER this sweep.
        if not ps.cisd_detected and ict_engine is not None:
            try:
                _tf = getattr(ict_engine, '_tf', {})
                _t5 = _tf.get("5m")
                if _t5 is not None:
                    _choch    = getattr(_t5, 'choch_direction', '') or ''
                    _bos      = getattr(_t5, 'bos_direction',   '') or ''
                    _choch_ts = getattr(_t5, 'choch_timestamp',  0)
                    _bos_ts   = getattr(_t5, 'bos_timestamp',    0)
                    _sweep_ms = ps.entered_at * 1000
                    _rev_struct = "bearish" if rev_dir == "short" else "bullish"
                    if (_choch == _rev_struct and
                            _choch_ts > _sweep_ms and
                            now - _choch_ts / 1000 < _DISP_CISD_MAX_AGE_SEC):
                        ps.cisd_detected  = True
                        ps.cisd_timestamp = now
                        ps.cisd_type      = "choch"
                        logger.info(
                            f"POST-SWEEP CISD: CHoCH {_rev_struct} at {elapsed:.0f}s "
                            f"| disp={ps.max_displacement_atr:.2f}ATR")
                    elif (_bos == _rev_struct and
                            _bos_ts > _sweep_ms and
                            now - _bos_ts / 1000 < _DISP_CISD_MAX_AGE_SEC):
                        ps.cisd_detected  = True
                        ps.cisd_timestamp = now
                        ps.cisd_type      = "bos"
                        logger.info(
                            f"POST-SWEEP CISD: BOS {_rev_struct} at {elapsed:.0f}s "
                            f"| disp={ps.max_displacement_atr:.2f}ATR")
            except Exception:
                pass

        # ─────────────────────────────────────────────────────────────────────
        # STEP 3: OTE Zone Tracking
        # FIX-1: Lower bound 0.500 (was 0.618) — aligned with entry_engine.
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
                        f"at {elapsed:.0f}s (bounds: "
                        f"{_OTE_FIB_LOW:.0%}–{_OTE_FIB_HIGH:.0%})")
                ps.ote_holding = in_ote

        # ─────────────────────────────────────────────────────────────────────
        # STEP 4: Score static factors (ONCE per sweep evaluation)
        # ─────────────────────────────────────────────────────────────────────
        if not ps.static_scored:
            ps.static_rev_base, ps.static_cont_base = \
                self._score_sweep_static(
                    ps, ict_engine, rev_dir, cont_dir,
                    atr=a, liq_snapshot=liq_snapshot)
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
                price, a, now, tick_flow, cvd_trend,
                liq_snapshot=liq_snapshot)

        # ── Accumulate with asymmetric decay  (FIX-7 + Bug #32 fix)
        # Bug #32 fix: in the mixed-signal branch (both rev_delta>0 and cont_delta>0),
        # the current code adds the delta then applies decay — meaning the fresh
        # evidence from this tick is immediately shrunk by 8%.  This under-penalizes
        # the stale accumulated evidence but also immediately discounts new evidence.
        # The correct ordering: decay first (shrink old), then add new (preserve it).
        # For pure-directional ticks this ordering is equivalent; for mixed it prevents
        # the fresh delta from being decayed on the same cycle it arrives.
        _decay         = 0.92
        _neutral_decay = float(_cfg("PS_NEUTRAL_TICK_DECAY", 0.98))
        if rev_delta > 0 and cont_delta > 0:
            # Mixed: decay old evidence first, then add fresh deltas (Bug #32)
            ps.rev_evidence  *= _decay
            ps.cont_evidence *= _decay
            ps.rev_evidence  += rev_delta
            ps.cont_evidence += cont_delta
        elif rev_delta > 0:
            ps.cont_evidence *= _decay
            ps.rev_evidence  += rev_delta
        elif cont_delta > 0:
            ps.rev_evidence  *= _decay
            ps.cont_evidence += cont_delta
        else:
            # Bug #27 fix: neutral tick — apply weak decay to both sides
            ps.rev_evidence  *= _neutral_decay
            ps.cont_evidence *= _neutral_decay

        ps.peak_rev  = max(ps.peak_rev,  ps.static_rev_base  + ps.rev_evidence)
        ps.peak_cont = max(ps.peak_cont, ps.static_cont_base + ps.cont_evidence)

        rev_total  = max(ps.static_rev_base  + ps.rev_evidence,  0.0)
        cont_total = max(ps.static_cont_base + ps.cont_evidence, 0.0)
        gap        = abs(rev_total - cont_total)

        # Store for display engine
        self._last_ps_analysis = {
            "phase":            phase,
            "rev_score":        round(rev_total, 1),
            "cont_score":       round(cont_total, 1),
            "rev_reasons":      rev_reasons,
            "cont_reasons":     cont_reasons,
            "cisd":             ps.cisd_detected,
            "displacement_atr": ps.max_displacement_atr,
            "ote":              ps.ote_reached,
            "elapsed_sec":      round(elapsed, 1),
        }

        # ── Log every 10 ticks ────────────────────────────────────────────
        if ps.tick_count == 1 or ps.tick_count % 10 == 0:
            _eff_threshold = base_threshold * score_mult
            logger.info(
                f"POST-SWEEP [{phase}] tick={ps.tick_count} "
                f"rev={rev_total:.0f} cont={cont_total:.0f} "
                f"(need {_eff_threshold:.0f}, gap>={_PS_GAP_MIN:.0f}) "
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
                action           = "reverse",
                direction        = rev_dir,
                confidence       = _conf,
                phase            = phase,
                cisd_active      = ps.cisd_detected,
                ote_active       = ps.ote_reached,
                displacement_atr = ps.max_displacement_atr,
                rev_score        = rev_total,
                cont_score       = cont_total,
                rev_reasons      = rev_reasons[:6],
                cont_reasons     = cont_reasons[:4],
                reason           = _reason,
            )

        elif (cont_total >= _adj_threshold * _PS_CONT_THRESHOLD_RATIO
              and gap >= _PS_GAP_MIN):
            _conf   = min(1.0, cont_total / 90.0)
            _reason = (
                f"CONTINUATION [{phase}] cont={cont_total:.0f} "
                f"vs rev={rev_total:.0f} gap={gap:.0f}")
            logger.info(f"🎯 POST-SWEEP VERDICT: {_reason}")
            self._ps_state = None
            return PostSweepDecision(
                action           = "continue",
                direction        = cont_dir,
                confidence       = _conf,
                phase            = phase,
                cisd_active      = False,
                ote_active       = ps.ote_reached,
                displacement_atr = ps.max_displacement_atr,
                rev_score        = rev_total,
                cont_score       = cont_total,
                rev_reasons      = rev_reasons[:3],
                cont_reasons     = cont_reasons[:6],
                reason           = _reason,
            )

        return PostSweepDecision(
            action           = "wait",
            direction        = "",
            confidence       = 0.0,
            phase            = phase,
            cisd_active      = ps.cisd_detected,
            ote_active       = ps.ote_reached,
            displacement_atr = ps.max_displacement_atr,
            rev_score        = rev_total,
            cont_score       = cont_total,
            rev_reasons      = rev_reasons[:4],
            cont_reasons     = cont_reasons[:4],
            reason           = (
                f"WAIT [{phase}] rev={rev_total:.0f} cont={cont_total:.0f} "
                f"gap={gap:.0f} < {_PS_GAP_MIN:.0f}"),
        )

    # ─────────────────────────────────────────────────────────────────────────
    # POST-SWEEP SCORING HELPERS
    # ─────────────────────────────────────────────────────────────────────────

    def _score_sweep_static(
        self,
        ps:           PostSweepState,
        ict_engine,
        rev_dir:      str,
        cont_dir:     str,
        atr:          float     = 0.0,
        liq_snapshot            = None,    # LiquidityMapSnapshot (FIX-4)
    ) -> Tuple[float, float]:
        """
        Score TIME-INVARIANT factors for the post-sweep decision.

        Called EXACTLY ONCE after the initial evaluation delay.
        Returns (rev_base, cont_base) — added to final totals without re-scoring.

        FIX-4: Pool significance now uses LiquidityMap adjusted_sig() when
        liq_snapshot is provided. Scales up to _STATIC_REV_POOL_MAX (8 pts)
        for the opposing pool and _STATIC_CONT_POOL_MAX (6 pts) for the
        continuation pool. Old code used a fixed 5 pt cap for both.
        """
        s_rev  = 0.0
        s_cont = 0.0

        if ict_engine is None:
            # Bug #15 fix: previously returned (0, 0) when ict_engine is None,
            # meaning dynamic factors had to cross the FULL threshold alone (~62-80 pts).
            # This silently degraded detection quality without any log warning.
            # Instead, derive a minimal baseline from data already in ps:
            #   • Sweep quality → reversal prior (higher quality = more institutional)
            #   • Session hint embedded in ps → session-weight contribution
            #   • Pool type → dealing range proxy (BSL swept → bearish reversal likely)
            # This keeps the static baseline proportional even without ICT engine.
            _sweep_quality = float(getattr(ps, 'quality', 0.35) or 0.35)
            _pool_type     = str(getattr(ps, 'swept_pool_type', '') or '')

            # Sweep quality → reversal base (mirrors the ICT path quality scoring)
            if   _sweep_quality >= 0.70: s_rev += 12.0
            elif _sweep_quality >= 0.50: s_rev +=  7.0
            elif _sweep_quality >= 0.35: s_rev +=  3.0

            # Pool type structural prior (BSL swept → bearish reversal; SSL swept → bullish)
            # Mapped identically to the ICT MANIPULATION phase aligned case (22 pts × 0.5 conf).
            if _pool_type in ('BSL', 'SSL'):
                s_rev += 8.0   # conservative: no AMD confidence to scale with

            logger.debug(
                "POST_SWEEP _score_sweep_static: ict_engine=None — "
                "using sweep-quality fallback (s_rev=%.1f s_cont=%.1f)", s_rev, s_cont)
            return s_rev, s_cont

        amd = getattr(ict_engine, '_amd', None)
        a   = max(atr, 1e-9)

        # ── AMD Phase alignment ───────────────────────────────────────────
        if amd is not None:
            _phase = getattr(amd, 'phase', '').upper()
            _bias  = getattr(amd, 'bias',  '').lower()
            _conf  = float(getattr(amd, 'confidence', 0.0))
            _bsl_swept = ps.swept_pool_type == "BSL"
            _ssl_swept = ps.swept_pool_type == "SSL"

            if _phase == 'MANIPULATION':
                _manip_aligned = (
                    (_bsl_swept and _bias == 'bearish') or
                    (_ssl_swept and _bias == 'bullish'))
                if _manip_aligned:
                    # Perfect AMD: sweep IS the Judas fake-out → strongly reversal
                    s_rev += 28.0 * max(_conf, 0.5)
                else:
                    # AMD CONTRA: sweep is in the direction of the manipulation bias
                    # → penalise reversal, bonus continuation
                    _pen = 18.0 * max(_conf, 0.5)
                    s_rev  -= _pen
                    s_cont += _pen * 0.60
                    logger.info(
                        f"POST-SWEEP static: AMD CONTRA {_bias.upper()} MANIP | "
                        f"rev penalty={_pen:.1f} cont bonus={_pen*0.6:.1f}")

            elif _phase == 'DISTRIBUTION':
                _dist_cont = (
                    (_bsl_swept and _bias == 'bearish') or
                    (_ssl_swept and _bias == 'bullish'))
                if _dist_cont:
                    s_cont += 26.0 * max(_conf, 0.5)
                else:
                    s_rev  += 18.0 * max(_conf, 0.5)

            elif _phase == 'REACCUMULATION':
                if _ssl_swept:
                    s_cont += 14.0 * max(_conf, 0.4)
                else:
                    s_rev  += 10.0 * max(_conf, 0.4)

            elif _phase == 'REDISTRIBUTION':
                if _bsl_swept:
                    s_cont += 14.0 * max(_conf, 0.4)
                else:
                    s_rev  += 10.0 * max(_conf, 0.4)

            elif _phase == 'ACCUMULATION':
                s_rev += 6.0 * max(_conf, 0.3)

        # ── Dealing Range Position ────────────────────────────────────────
        dr    = getattr(ict_engine, '_dealing_range', None)
        _pd   = getattr(dr, 'current_pd', 0.5) if dr is not None else 0.5

        if ps.swept_pool_type == "BSL":
            # BSL sweep → reversal is SHORT → favoured from PREMIUM
            if   _pd >= 0.80: s_rev  += 14.0
            elif _pd >= 0.65: s_rev  += 10.0
            elif _pd >= 0.50: s_rev  +=  5.0
            else:             s_cont +=  8.0   # SHORT reversal from discount = high risk
        else:
            # SSL sweep → reversal is LONG → favoured from DISCOUNT
            if   _pd <= 0.20: s_rev  += 14.0
            elif _pd <= 0.35: s_rev  += 10.0
            elif _pd <= 0.50: s_rev  +=  5.0
            else:             s_cont +=  8.0   # LONG reversal from premium = high risk

        # ── HTF Structure Context ─────────────────────────────────────────
        _tf        = getattr(ict_engine, '_tf', {})
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

        if   _htf_aligned >= 3: s_rev  += 16.0
        elif _htf_aligned >= 2: s_rev  += 10.0
        elif _htf_opposed >= 2: s_rev  -=  5.0; s_cont += 10.0

        # ── OB Blocking (is a strong OB in the continuation path?) ───────
        try:
            _now_ms_local = int(time.time() * 1000)
            if rev_dir == "short":
                # Continuation = long → bearish OBs between price and BSL block it
                _obs = getattr(ict_engine, 'order_blocks_bear', [])
            else:
                # Continuation = short → bullish OBs between price and SSL block it
                _obs = getattr(ict_engine, 'order_blocks_bull', [])
            for ob in _obs:
                if not ob.is_active(_now_ms_local) or ob.strength < 70.0:
                    continue
                ob_dist_atr = abs(ob.midpoint - ps.swept_pool_price) / a
                if ob_dist_atr < 2.0:
                    s_rev += 10.0
                    break
        except Exception:
            pass

        # ── Pool Significance of Opposing / Continuation Targets  (FIX-4) ──
        # FIX-4: Use LiquidityMap adjusted_sig() for richer pool significance.
        # Old code used a fixed 5.0 pt cap regardless of pool quality.
        # New: scales linearly from pool.adjusted_sig() up to defined max pts.
        if liq_snapshot is not None:
            # Opposing pool: the reversal's TP target
            # BUG-4 FIX: exclude mutated-SWEPT pools from _last_snapshot
            _opp_pools = [t for t in
                          (liq_snapshot.ssl_pools if rev_dir == "short"
                           else liq_snapshot.bsl_pools)
                          if self._is_pool_target_active(t)]
            if _opp_pools:
                _best_opp = max(_opp_pools, key=lambda t: t.adjusted_sig())
                _opp_sig  = min(_STATIC_REV_POOL_MAX,
                                _best_opp.adjusted_sig() * 0.8)
                if _opp_sig > 0.5:
                    s_rev += _opp_sig
            # Continuation pool: the continuation's TP target
            # BUG-4 FIX: exclude mutated-SWEPT pools from _last_snapshot
            _cont_pools = [t for t in
                           (liq_snapshot.bsl_pools if cont_dir == "long"
                            else liq_snapshot.ssl_pools)
                           if self._is_pool_target_active(t)]
            if _cont_pools:
                _best_cont = max(_cont_pools, key=lambda t: t.adjusted_sig())
                _cont_sig  = min(_STATIC_CONT_POOL_MAX,
                                 _best_cont.adjusted_sig() * 0.6)
                if _cont_sig > 0.5:
                    s_cont += _cont_sig
        else:
            # Fallback: ICT structural targets
            _ict_targets = getattr(ict_engine, 'liquidity_pools', [])
            _opp_pools   = [p for p in _ict_targets
                            if not getattr(p, 'swept', False) and
                            ((rev_dir == "short" and
                              getattr(p, 'level_type', '') == "SSL") or
                             (rev_dir == "long" and
                              getattr(p, 'level_type', '') == "BSL"))]
            if _opp_pools:
                _best = max(_opp_pools,
                            key=lambda p: float(getattr(p, 'touch_count', 1)))
                _tc = float(getattr(_best, 'touch_count', 1))
                s_rev += min(_STATIC_REV_POOL_MAX, _tc * 1.5)

        # ── Session Kill Zone Context ─────────────────────────────────────
        # MOD-6 FIX: read _session (full window) before _killzone (KZ-only)
        _sess_raw = str(getattr(ict_engine, '_session', '') or '').lower()
        if not _sess_raw or _sess_raw in ('off_hours', 'weekend'):
            _sess_raw = str(getattr(ict_engine, '_killzone', '') or '').lower()
        if   'asia'   in _sess_raw: s_rev += 7.0   # Asia sweeps are often manipulations
        elif 'london' in _sess_raw: s_rev += 4.0   # London Judas swing = moderate rev bias
        elif 'ny'     in _sess_raw: s_rev += 2.0   # NY open often reverses Asia range

        return round(s_rev, 2), round(s_cont, 2)

    def _score_sweep_dynamic(
        self,
        ps:          PostSweepState,
        ict_engine,
        rev_dir:     str,
        cont_dir:    str,
        price:       float,
        atr:         float,
        now:         float,
        tick_flow:   float,
        cvd_trend:   float,
        liq_snapshot         = None,    # reserved for future use (FIX-3)
    ) -> Tuple[float, float, List[str], List[str]]:
        """
        Score PER-TICK dynamic factors for the post-sweep decision.

        These factors change tick-to-tick and feed the decay accumulator:
        displacement growth, CISD freshness, OTE zone, flow, CVD,
        5m and 15m structure events.

        FIX-6: 15m structure check added. Previously only 5m CHoCH/BOS was
        checked. 15m structure fires on longer bars (less noise), providing
        an independent structural confirmation signal.
        """
        rev_delta:   float      = 0.0
        cont_delta:  float      = 0.0
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
            cont_delta += 8.0
            cont_reasons.append(f"NO_DISP ({ps.max_displacement_atr:.2f}ATR)")

        # ── CISD freshness (decays with age) ──────────────────────────────
        if ps.cisd_detected:
            _cisd_age     = now - ps.cisd_timestamp
            _freshness    = max(0.4, 1.0 - _cisd_age / 120.0)
            rev_delta    += 18.0 * _freshness
            rev_reasons.append(f"CISD_{ps.cisd_type.upper()}({_freshness:.0%})")

        # ── OTE zone  (FIX-1: fires at 50% retrace, aligned with entry_engine)
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
            cont_delta += 9.0; cont_reasons.append(f"FLOW_CONT {tick_flow:+.2f}")
            ps.cont_flow_ticks += 1

        # ── CVD trend alignment ───────────────────────────────────────────
        _cvd_rev_ok  = (
            (rev_dir == "long"  and cvd_trend >  0.20) or
            (rev_dir == "short" and cvd_trend < -0.20))
        _cvd_cont_ok = (
            (rev_dir == "long"  and cvd_trend < -0.25) or
            (rev_dir == "short" and cvd_trend >  0.25))

        if _cvd_rev_ok:
            rev_delta  += 8.0; rev_reasons.append(f"CVD_REV {cvd_trend:+.2f}")
        elif _cvd_cont_ok:
            cont_delta += 8.0; cont_reasons.append(f"CVD_CONT {cvd_trend:+.2f}")
        elif abs(cvd_trend) < 0.10:
            rev_delta  += 2.0   # neutral CVD = slight reversal bias (distribution)

        # ── 5m Structure ──────────────────────────────────────────────────
        # CRITICAL: Only count structural events that occurred AFTER the sweep.
        # Pre-sweep CHoCH/BOS adds evidence on every tick indefinitely,
        # creating permanent artificial bias unrelated to this sweep.
        if ict_engine is not None:
            try:
                _tf = getattr(ict_engine, '_tf', {})
                _t5 = _tf.get("5m")
                if _t5 is not None:
                    _choch    = getattr(_t5, 'choch_direction', '') or ''
                    _bos      = getattr(_t5, 'bos_direction',   '') or ''
                    _choch_ts = getattr(_t5, 'choch_timestamp',  0)
                    _bos_ts   = getattr(_t5, 'bos_timestamp',    0)
                    _sweep_ms = ps.entered_at * 1000
                    _rev_lbl  = "bearish" if rev_dir == "short" else "bullish"
                    _cont_lbl = "bullish" if rev_dir == "short" else "bearish"
                    if _choch == _rev_lbl and _choch_ts > _sweep_ms:
                        rev_delta  += 12.0; rev_reasons.append("CHoCH_5m_REV")
                    if _bos == _rev_lbl and _bos_ts > _sweep_ms:
                        rev_delta  +=  8.0; rev_reasons.append("BOS_5m_REV")
                    if _bos == _cont_lbl and _bos_ts > _sweep_ms:
                        cont_delta += 10.0; cont_reasons.append("BOS_5m_CONT")
            except Exception:
                pass

        # ── 15m Structure  (FIX-6) ───────────────────────────────────────
        # 15m bars close less frequently → less noise than 5m.
        # Provides an independent structural confirmation signal.
        if ict_engine is not None:
            try:
                _tf  = getattr(ict_engine, '_tf', {})
                _t15 = _tf.get("15m")
                if _t15 is not None:
                    _t15_trend = getattr(_t15, 'trend',   '') or ''
                    _t15_bos   = getattr(_t15, 'bos_direction', '') or ''
                    _t15_bos_ts = getattr(_t15, 'bos_timestamp',  0)
                    _rev_lbl   = "bearish" if rev_dir == "short" else "bullish"
                    _cont_lbl  = "bullish" if rev_dir == "short" else "bearish"
                    _sweep_ms  = ps.entered_at * 1000
                    # 15m trend aligned with reversal direction = structural support
                    if _t15_trend == _rev_lbl:
                        rev_delta  +=  8.0; rev_reasons.append(f"15m_TREND_{_rev_lbl.upper()}")
                    elif _t15_trend == _cont_lbl:
                        cont_delta +=  6.0; cont_reasons.append(f"15m_TREND_{_cont_lbl.upper()}_CONT")
                    # 15m BOS in reversal direction = higher-TF structural break
                    if _t15_bos == _rev_lbl and _t15_bos_ts > _sweep_ms:
                        rev_delta  += 10.0; rev_reasons.append("BOS_15m_REV")
            except Exception:
                pass

        # ── Sustained flow bonus ──────────────────────────────────────────
        if ps.rev_flow_ticks  >= 5:
            rev_delta  += 5.0; rev_reasons.append(f"SUSTAINED_REV_FLOW({ps.rev_flow_ticks}t)")
        if ps.cont_flow_ticks >= 5:
            cont_delta += 5.0; cont_reasons.append(f"SUSTAINED_CONT_FLOW({ps.cont_flow_ticks}t)")

        return rev_delta, cont_delta, rev_reasons, cont_reasons

    # =========================================================================
    # 3. POOL-HIT CONTINUATION GATE
    # =========================================================================

    def pool_hit_gate(
        self,
        pos_side:        str,    # "long" | "short"
        pos_entry:       float,
        pos_sl:          float,
        pos_tp:          float,
        price:           float,
        atr:             float,
        ict_engine,
        tick_flow:       float = 0.0,
        cvd_trend:       float = 0.0,
        next_pool_price: Optional[float] = None,
        liq_snapshot             = None,   # LiquidityMapSnapshot (FIX-5)
    ) -> ContinuationDecision:
        """
        When price reaches a liquidity pool DURING an active trade, determine:

          EXIT       → This pool is our TP. Close the position.
          REVERSE    → Pool was swept, AMD flipped, go the other way.
          CONTINUE   → Another pool beyond this one is the real target.
                       Trail SL to this pool and ride to the next.
          HOLD       → Insufficient evidence — let the SL/TP manage.

        FIX-5: When next_pool_price is None and liq_snapshot is provided,
        auto-resolves the next pool by finding the nearest qualifying opposing
        pool (distance > 0.5 ATR, significance >= 2.0) from the snapshot.

        DECISION LOGIC:
          1. If price is at TP pool → EXIT (take the profit)
          2. If flow has strongly reversed AND BOS confirms → REVERSE
          3. If AMD is still aligned and a next pool exists with > 1.5R → CONTINUE
          4. If flow reversed but no structural BOS → HOLD
          5. If BOS against but flow OK → HOLD for confirmation
          6. Otherwise → HOLD (insufficient signal)

        CONTINUATION CRITERIA (must meet ALL):
          a) AMD delivery still intact (DISTRIBUTION or REACCUMULATION phase)
          b) Flow NOT strongly reversed
          c) No counter-BOS on 5m in the last 30 minutes
          d) Next pool at least 1.5R reward from current price
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

        # ── Check: Counter-BOS on 5m? ─────────────────────────────────────
        _counter_bos = False
        if ict_engine is not None:
            try:
                _tf   = getattr(ict_engine, '_tf', {})
                _t5   = _tf.get("5m")
                if _t5 is not None:
                    _bos_d    = getattr(_t5, 'bos_direction', '') or ''
                    _bos_ts   = getattr(_t5, 'bos_timestamp',   0)
                    _now_ms   = int(time.time() * 1000)
                    _bos_age_min = (_now_ms - _bos_ts) / 60_000 if _bos_ts > 0 else 999
                    if (_bos_age_min < 30 and
                            ((pos_side == "long"  and _bos_d == "bearish") or
                             (pos_side == "short" and _bos_d == "bullish"))):
                        _counter_bos = True
            except Exception:
                pass

        # ── Check: AMD still delivering? ─────────────────────────────────
        _amd_aligned         = False
        _amd_delivery_target = None
        if ict_engine is not None:
            amd = getattr(ict_engine, '_amd', None)
            if amd is not None:
                _phase = getattr(amd, 'phase', '').upper()
                _bias  = getattr(amd, 'bias',  '').lower()
                _conf  = float(getattr(amd, 'confidence', 0.0))
                _amd_delivery_target = getattr(amd, 'delivery_target', None)
                _amd_aligned = (
                    _conf >= 0.45 and
                    _phase in ('DISTRIBUTION', 'REACCUMULATION', 'REDISTRIBUTION') and
                    ((pos_side == "long"  and _bias == "bullish") or
                     (pos_side == "short" and _bias == "bearish")))

        # ── FIX-5: Auto-resolve next pool from liq_snapshot ──────────────
        _next_pool_resolved = next_pool_price
        if _next_pool_resolved is None and liq_snapshot is not None:
            _pool_list = (liq_snapshot.bsl_pools if pos_side == "long"
                          else liq_snapshot.ssl_pools)
            # BUG-4 FIX: exclude mutated-SWEPT pools from _last_snapshot
            _qualifying = [
                t for t in _pool_list
                if t.distance_atr > 0.5
                and t.significance >= 2.0
                and self._is_pool_target_active(t)    # BUG-4 FIX
            ]
            if _qualifying:
                _nearest = min(_qualifying, key=lambda t: t.distance_atr)
                _next_pool_resolved = _nearest.pool.price

        # Fallback: use AMD delivery target
        if _next_pool_resolved is None:
            _next_pool_resolved = _amd_delivery_target

        # ── Check: Viable next pool? ──────────────────────────────────────
        _next_pool_viable = False
        _next_rr          = 0.0
        if _next_pool_resolved is not None:
            _next_dist = abs(_next_pool_resolved - price)
            _sl_dist   = abs(price - pos_sl) if pos_sl > 0 else a
            if _sl_dist > 1e-10:
                _next_rr = _next_dist / _sl_dist
                if _next_rr >= _CONT_NEXT_POOL_MIN_RR:
                    _next_pool_viable = True

        # ─────────────────────────────────────────────────────────────────
        # Decision tree
        # ─────────────────────────────────────────────────────────────────

        # Case 1: Flow reversed + structural BOS against → REVERSE
        if _flow_reversed and _counter_bos:
            return ContinuationDecision(
                action="reverse",
                confidence=0.80,
                reason=(
                    f"FLOW_REVERSED(flow={tick_flow:+.2f}) + COUNTER_BOS "
                    f"— structure invalidated"),
                next_target=None,
            )

        # Case 2: AMD intact + flow OK + next pool viable → CONTINUE
        if _amd_aligned and not _counter_bos and not _flow_reversed and _next_pool_viable:
            return ContinuationDecision(
                action="continue",
                confidence=0.70,
                reason=(
                    f"AMD_DELIVERY_INTACT + FLOW_OK + NEXT_POOL "
                    f"@ ${_next_pool_resolved:,.0f} ({_next_rr:.1f}R potential)"),
                next_target=_next_pool_resolved,
            )

        # Case 3: Flow reversed but no structural BOS → HOLD, trail SL
        if _flow_reversed and not _counter_bos:
            return ContinuationDecision(
                action="hold",
                confidence=0.60,
                reason=(
                    f"FLOW_REVERSED but no BOS — hold, trail SL "
                    f"(flow={tick_flow:+.2f})"),
            )

        # Case 4: BOS against but flow OK → HOLD for confirmation
        if _counter_bos and not _flow_reversed:
            return ContinuationDecision(
                action="hold",
                confidence=0.55,
                reason="COUNTER_BOS but flow OK — hold for confirmation",
            )

        # Default: insufficient signal → HOLD
        return ContinuationDecision(
            action="hold",
            confidence=0.40,
            reason=(
                f"INSUFFICIENT_SIGNAL | AMD={_amd_aligned} "
                f"flow_rev={_flow_reversed} cbos={_counter_bos} "
                f"next_pool={_next_pool_viable}"),
        )

    # =========================================================================
    # HELPERS
    # =========================================================================

    def _null_prediction(
        self,
        now_ms:    int,
        reason:    str      = "",
        dr_pd:     float    = 0.5,
        bsl_score: float    = 0.0,
        ssl_score: float    = 0.0,
        score:     float    = 0.0,
        factors:   Optional[HuntFactors] = None,
    ) -> HuntPrediction:
        return HuntPrediction(
            predicted           = None,
            confidence          = 0.0,
            delivery_direction  = "",
            raw_score           = score,
            bsl_score           = bsl_score,
            ssl_score           = ssl_score,
            factors             = factors or HuntFactors(),
            dealing_range_pd    = dr_pd,
            reason              = reason,
            timestamp_ms        = now_ms,
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
    """Fast symmetric sigmoid ℝ → (-1, +1). Padé approximation; < 0.3% error."""
    sz = z * steepness
    return max(-1.0, min(1.0, sz / (1.0 + abs(sz) * 0.5)))


# ─────────────────────────────────────────────────────────────────────────────
# INTEGRATION GUIDE (quant_strategy.py wiring reference)
# ─────────────────────────────────────────────────────────────────────────────

"""
HOW TO WIRE direction_engine.py v2.0 INTO quant_strategy.py
=============================================================

0. INIT (in QuantStrategy.__init__):

    from strategy.direction_engine import DirectionEngine, HuntPrediction, DirectionBias
    self._dir_engine = DirectionEngine()
    self._notified_sweeps: set = set()   # deduplication — see Step 3b

1. HUNT PREDICTION  (Step 3 in _evaluate_entry — BEFORE liq_map.update())

    _hunt = self._dir_engine.predict_hunt(
        price          = price,
        atr            = atr,
        now_ms         = now_ms,
        ict_engine     = self._ict,
        tick_flow      = self._tick_eng.get_signal(),
        cvd_trend      = self._cvd.get_trend_signal(),
        candles_5m     = candles_by_tf.get("5m", []),
        liq_snapshot   = self._liq_map._last_snapshot,   # FIX-3: previous tick's snap
    )
    # Bridge to ICTEngine legacy dict shape:
    self._ict.inject_hunt_prediction({
        "predicted":          _hunt.predicted,
        "confidence":         round(_hunt.confidence, 3),
        "delivery_direction": _hunt.delivery_direction,
        "raw_score":          round(_hunt.raw_score, 4),
        "bsl_score":          round(_hunt.bsl_score, 3),
        "ssl_score":          round(_hunt.ssl_score, 3),
        "dealing_range_pd":   round(_hunt.dealing_range_pd, 3),
        "swept_pool":         _hunt.swept_pool_price,
        "opposing_pool":      _hunt.opposing_pool_price,
        "reason":             _hunt.reason,
        "scenario":           "",
        "confidence_factors": {},
    }, now_ms)

2. SWEEP DETECTION  (Step 6b — AFTER liq_map.update() + liq_snapshot = ...)

    Deduplication is REQUIRED. The bridge loop runs ~4 Hz and visits all
    swept pools in the last 30s window. Without dedup, on_sweep() is called
    ~120 times per pool, resetting PostSweepState every tick and making
    accumulated evidence impossible to build.

    _sweep_age_limit = now_ms - 30_000   # last 30 seconds
    for pool in self._ict.liquidity_pools:
        if pool.swept and pool.sweep_timestamp > _sweep_age_limit:
            _sweep_key = (pool.price, pool.sweep_timestamp)
            if _sweep_key not in self._notified_sweeps:
                self._notified_sweeps.add(_sweep_key)
                self._dir_engine.on_sweep(
                    swept_pool_price = pool.price,
                    pool_type        = pool.level_type,   # "BSL" | "SSL"
                    price            = price,
                    atr              = atr,
                    now              = now,
                    quality          = float(
                        getattr(pool, 'displacement_score', 0.5) or 0.5),
                )
    # Prune stale entries to bound memory
    _cutoff_ms = now_ms - 60_000
    self._notified_sweeps = {k for k in self._notified_sweeps
                             if k[1] > _cutoff_ms}

3. POST-SWEEP EVALUATION  (Step 6c — AFTER liq_map.update())

    Runs every tick while DirectionEngine has an open PostSweepState.
    The verdict is written into ict_ctx.direction_hint* BEFORE
    entry_engine.update() is called so that the entry engine's
    _evaluate_post_sweep_accumulative() can consume it as a dynamic
    weighting factor (up to +20 pts at full confidence).

    if self._dir_engine.in_post_sweep:
        _ps_decision = self._dir_engine.evaluate_sweep(
            price        = price,
            atr          = atr,
            now          = now,
            ict_engine   = self._ict,
            tick_flow    = self._tick_eng.get_signal(),
            cvd_trend    = self._cvd.get_trend_signal(),
            liq_snapshot = liq_snapshot,   # FIX-3: current tick's fresh snapshot
        )
        if _ps_decision.action in ("reverse", "continue"):
            ict_ctx.direction_hint            = _ps_decision.action
            ict_ctx.direction_hint_side       = _ps_decision.direction
            ict_ctx.direction_hint_confidence = _ps_decision.confidence
        else:
            # "wait" — clear stale hint so entry_engine doesn't act on it
            ict_ctx.direction_hint            = ""
            ict_ctx.direction_hint_side       = ""
            ict_ctx.direction_hint_confidence = 0.0

    # IMPORTANT: Call entry_engine.update() AFTER writing ict_ctx.direction_hint
    self._entry_engine.update(
        liq_snapshot=liq_snapshot,
        flow_state=flow_state,
        ict_ctx=ict_ctx,         # direction_hint* fields now populated
        price=price, atr=atr, now=now,
        ...
    )

4. POOL-HIT GATE  (during active position when price approaches a pool)

    _gate = self._dir_engine.pool_hit_gate(
        pos_side         = self._pos.side,
        pos_entry        = self._pos.entry_price,
        pos_sl           = self._pos.sl_price,
        pos_tp           = self._pos.tp_price,
        price            = price,
        atr              = atr,
        ict_engine       = self._ict,
        tick_flow        = self._tick_eng.get_signal(),
        cvd_trend        = self._cvd.get_trend_signal(),
        next_pool_price  = None,          # FIX-5: auto-resolved from liq_snapshot
        liq_snapshot     = liq_snapshot,  # FIX-5: pass current snapshot
    )
    if   _gate.action == "exit":     # → close position at TP
    elif _gate.action == "reverse":  # → exit + enter opposite side
    elif _gate.action == "continue": # → update TP to _gate.next_target, trail SL
    # "hold" → let existing SL/TP manage

5. DISPLAY (Telegram heartbeat / /thinking command)

    _hunt = self._dir_engine.last_hunt
    if _hunt and _hunt.predicted:
        logger.info(
            f"HUNT: {_hunt.predicted} conf={_hunt.confidence:.2f} "
            f"delivery={_hunt.delivery_direction} | {_hunt.reason[:60]}")

    if self._dir_engine.in_post_sweep:
        _analysis = self._dir_engine.last_sweep_analysis
        logger.info(
            f"POST-SWEEP [{_analysis.get('phase','?')}] "
            f"rev={_analysis.get('rev_score',0):.0f} "
            f"cont={_analysis.get('cont_score',0):.0f} "
            f"CISD={'✓' if _analysis.get('cisd') else '✗'} "
            f"DISP={_analysis.get('displacement_atr',0):.2f}ATR "
            f"OTE={'✓' if _analysis.get('ote') else '✗'}")

DIRECTION HINT FLOW (data path):
  DirectionEngine.evaluate_sweep()
    → PostSweepDecision(action, direction, confidence)
    → quant_strategy writes to ict_ctx.direction_hint / _side / _confidence
    → entry_engine._evaluate_post_sweep_accumulative() reads ict_ctx
    → adds up to +20 pts × confidence to the matching side's delta
    → pushes entry_engine's accumulated total over its threshold faster

FIELD MAPPING:
  PostSweepDecision.direction == "long"  → ict_ctx.direction_hint_side = "long"
  PostSweepDecision.direction == "short" → ict_ctx.direction_hint_side = "short"
  entry_engine checks: _dir_hint_side == sweep_dir  (both use same convention:
    sweep.direction = "long" for SSL swept = reversal direction = "long")
"""

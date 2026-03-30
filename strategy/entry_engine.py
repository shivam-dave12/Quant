"""
entry_engine.py — Liquidity-First Entry Decision Engine v3.4
=============================================================
FIXES IN v3.4
─────────────
FIX-FLAW1: AMD contra-penalty in MANIPULATION phase (critical logic gap).
  The MANIPULATION phase scoring only rewarded AMD-aligned setups (+22 × conf)
  but applied ZERO penalty when AMD was contra.  In the logged trade
  (AMD: MANIPULATION bias=bullish conf=0.95, BSL swept → short entered), the
  system assigned +0 from AMD when it should have penalised the reversal
  heavily.  The BSL sweep IS the manipulation fake-out before bullish delivery
  — going short here trades the wrong side of the AMD cycle.
  Fix: add elif branch for contra MANIPULATION:
    - rev_delta  -= 18.0 × max(conf, 0.5)   [penalty proportional to confidence]
    - cont_delta += 10.8 × max(conf, 0.5)   [60% of penalty → cont bonus]
    - Logged at INFO level for every contra firing (observable in prod logs)
  Penalty is intentionally smaller than the aligned bonus (22) so that strong
  structural evidence (CISD + OTE + sustained flow) can still override a
  contra AMD reading on high-probability setups.

FIX-FLAW2: Static evidence double-counting in the accumulative model.
  The old _evaluate_post_sweep_accumulative() rescored ALL factors — including
  time-invariant ones — on every tick, then added them to ps.rev_evidence.
  This inflated the accumulated total by ~44 pts/tick, allowing a marginal
  setup to reach the DISPLACEMENT threshold (55 × 1.30 = 71.5) in exactly
  2 ticks regardless of actual price action:
    tick 1: 44.25 → ×0.92 = 40.7;  tick 2: 40.7 + 44.25 = 84.95 → 78.2 ✓
  The arithmetic was correct; the architecture was wrong.

  Fix: strict static/dynamic separation.
    STATIC (scored once into ps.static_rev_base / ps.static_cont_base):
      AMD phase/bias, sweep quality, dealing range P/D, pool target
      significance, OB blocking, session kill-zone.
    DYNAMIC (fed into per-tick decay accumulator ps.rev_evidence):
      live displacement growth, CISD freshness decay, OTE zone entry/exit,
      instantaneous flow, EWMA flow, CVD, 5m CHoCH/BOS, 15m structure,
      sustained flow consistency bonus.
    Final totals: rev_total = static_rev_base + rev_evidence (dynamic)
    The _PostSweepState dataclass gains: static_scored bool + two float bases.

FIXES IN v3.3
─────────────
BUG-FIX-CRITICAL: ICT sweep bridge — two disconnected sweep systems.
  The ICT engine detects dozens of sweeps (BSL SWEPT, SSL SWEPT) but the
  LiquidityMap's check_sweeps() detects zero. These are completely separate
  pool registries. The entry engine only reads LiquidityMap sweeps, so the
  post-sweep pipeline NEVER fired at all — 34+ sweeps per session wasted.
  Fix: Bridge ICT sweeps via ICTSweepEvent → ict_ctx.ict_sweeps → entry engine.
  Synthesize SweepResult from ICT sweep data when LiquidityMap has no sweeps.

POST-SWEEP v3.3: Multi-phase accumulative evidence model.
  Old: Stateless recalculation each tick with fixed thresholds. Evidence from
  previous ticks was thrown away — momentary flow noise killed valid setups.
  New: 4-phase evaluation (DISPLACEMENT→CISD→OTE→MATURE) with evidence that
  BUILDS over the evaluation window. Phase-adaptive thresholds: early phase
  requires 1.3× evidence (overwhelming only); mature phase accepts 0.75×.

CISD detection: Tracks CHoCH/BOS in reversal direction as structural
  confirmation. CISD provides the green-light for reversal entries.

Displacement measurement: Tracks how far and how fast price moves from
  sweep level. Strong displacement (>1.2 ATR) is the strongest reversal signal.

OTE zone tracking: Detects when price retraces to the 50%-78.6% Fibonacci
  zone of the displacement move — the optimal institutional entry point.

SL buffer widened: 0.35×ATR (was 0.12×ATR). The old buffer was a death zone
  for sweep retests — price routinely wicked past 0.12 ATR on retests.

Continuation SL fixed: Placed BEYOND the swept pool + 0.40×ATR buffer.
  Old code placed SL AT the swept pool price (0.20×ATR) where stops already
  triggered — worst possible placement.

Synthetic TP gated: 2R fallback only fires when CISD is confirmed, preventing
  structureless trades from generating signals.

FIXES IN v3.1
─────────────
BUG-FIX-1: EWMA alpha was NOT time-normalised.
  Old: alpha = base * (1 + conviction_boost), dt computed but discarded.
  After stream gaps/reconnects, EWMA decayed at the same rate as a 250ms
  tick — making signals artificially stale after any silence.
  New: continuous-time equivalent alpha = 1 - exp(-alpha_per_sec * dt),
  calibrated so a 250ms tick produces the original base_alpha=0.15.

BUG-FIX-2: Adjacency bonus was invisible to all decision functions.
  Old: get_snapshot() applied +2.0 adjacency bonus to PoolTarget.significance
  but ALL selection logic called t.pool.proximity_adjusted_sig() which reads
  pool.significance, bypassing the bonus.
  New: All selection logic calls t.adjusted_sig() which uses t.significance.

BUG-FIX-3: BSL/SSL proximity approach used inconsistent selection criteria.
  Old: BSL selected by closest distance; SSL by highest adjusted_sig.
  New: Both sides use t.adjusted_sig() — consistent apples-to-apples.

BUG-FIX-4: _compute_sl log message was misleading.
  Old: "no ICT structure found" even when OB existed but was too far/close.
  New: Separate log paths for "no OB", "OB too close", "OB too far".

BUG-FIX-5: Momentum signal target_pool used raw-rank first pool.
  Old: snap.bsl_pools[0] is sorted by raw significance — could be 40+ ATR away.
  New: Uses proximity-adjusted selection filtered to _MAX_TARGET_ATR.

HTF-TP-ESCALATION (v3.1 new feature):
  When the nearest pool TP fails minimum R:R, escalate to 1h/4h/1d pools
  AND any pool with htf_count >= 2 within _HTF_TP_MAX_ATR (30 ATR).
  Applied in: _do_ready, _handle_sweep_reversal, _check_proximity_approach,
  _check_displacement_momentum.

  From live log 2026-03-28 22:39:58:
    READY: R:R failed 80 consecutive ticks (best=0.88 vs required=1.40)
    $67,071 pool (2.1 ATR) gave R:R=0.88 with 1.79 ATR SL.
    Engine aborted instead of using $68,949 (22.6 ATR, HTFx2) = R:R 9.5.
"""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

try:
    from strategy.liquidity_map import (
        LiquidityMap, LiquidityMapSnapshot, PoolTarget, SweepResult,
        PoolStatus, PoolSide, TF_HIERARCHY,
    )
except ImportError:
    from liquidity_map import (
        LiquidityMap, LiquidityMapSnapshot, PoolTarget, SweepResult,
        PoolStatus, PoolSide, TF_HIERARCHY,
    )


# ═══════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════

_FLOW_CONV_THRESHOLD    = 0.40
_FLOW_CVD_AGREE_MIN     = 0.15
_FLOW_SUSTAINED_TICKS   = 2

_MAX_TARGET_ATR         = 8.0
_MIN_TARGET_ATR         = 0.25
_MIN_POOL_SIGNIFICANCE  = 2.0

_PROXIMITY_ENTRY_ATR_MAX    = 2.0
_PROXIMITY_ENTRY_ATR_MIN    = 0.15
_PROXIMITY_MIN_SIG          = 4.0
_PROXIMITY_CONFIRM_COUNT    = 1

_MIN_SWEEP_QUALITY      = 0.35

_CISD_MAX_WAIT_SEC      = 360
_OTE_MAX_WAIT_SEC       = 600
_TRACKING_TIMEOUT_SEC   = 300
_READY_TIMEOUT_SEC      = 150

_MIN_RR_RATIO           = 1.4
_SL_BUFFER_ATR          = 0.35    # v3.3: widened from 0.12 — old buffer was death zone for sweep retests
_TP_BUFFER_ATR          = 0.08

_ENTRY_COOLDOWN_SEC     = 30.0
_POST_SWEEP_EVAL_SEC    = 10.0

# ── Post-Sweep Pipeline v3.3 Constants ────────────────────────────────────
_PS_PHASE_DISPLACEMENT_SEC =  45.0   # first 45s after sweep: expect displacement
_PS_PHASE_CISD_SEC         = 120.0   # 45-120s: expect CISD (CHoCH/BOS reversal)
_PS_PHASE_OTE_SEC          = 240.0   # 120-240s: expect OTE retrace to 50-78.6%
_PS_PHASE_MATURE_SEC       = 360.0   # 240-360s: relaxed thresholds, final chance

_PS_DISP_MIN_ATR           = 0.5     # minimum displacement from sweep level
_PS_DISP_STRONG_ATR        = 1.2     # strong displacement threshold
_PS_OTE_FIB_LOW            = 0.50    # OTE zone lower bound (50% retrace)
_PS_OTE_FIB_HIGH           = 0.786   # OTE zone upper bound (78.6% retrace)

_PS_CONTINUATION_SL_BUFFER = 0.40    # ATR buffer BEYOND swept pool for continuation SL
_PS_REVERSAL_SL_BUFFER     = 0.35    # ATR buffer beyond sweep wick for reversal SL

_PS_SCORE_THRESHOLD_EARLY  = 55.0    # early phase: overwhelming evidence required
_PS_SCORE_THRESHOLD_NORMAL = 45.0    # standard phase
_PS_SCORE_THRESHOLD_MATURE = 35.0    # mature phase: accept weaker setups
_PS_SCORE_GAP_MIN          = 8.0     # minimum gap between rev/cont scores

_PS_DISP_SCORE_MULT        = 1.30    # during displacement phase, need 1.3× score
_PS_MATURE_SCORE_MULT      = 0.75    # during mature phase, accept 0.75× score

# BUG-FIX-1: time-normalised EWMA
# alpha_per_sec calibrated: at dt=0.25s -> alpha = 1 - exp(-0.648*0.25) = 0.15 (original)
_FLOW_EWMA_ALPHA_PER_SEC = 0.648
_FLOW_EWMA_ADAPTIVE_CAP  = 0.40

_TRACKING_MIN_HOLD_SEC  = 5.0
_READY_MIN_HOLD_SEC     = 8.0

_READY_DECAY_RATE       = 0.06
_READY_MIN_CONVICTION   = 0.18

_AMD_MANIP_CONTRA_MULT  = 0.60
_AMD_DIST_CONTRA_MULT   = 0.80
_AMD_ALIGNED_BONUS      = 1.10

_MOMENTUM_MIN_BODY_RATIO    = 0.65
_MOMENTUM_MIN_VOL_RATIO     = 1.3
_MOMENTUM_MIN_ATR_MOVE      = 0.6
_MOMENTUM_LOOKBACK_CANDLES  = 3
_MOMENTUM_SL_BUFFER_ATR     = 0.15
_MOMENTUM_MIN_RR            = 1.3
_MOMENTUM_COOLDOWN_SEC      = 60.0
_MOMENTUM_MAX_ENTRIES_PER_HOUR = 3

# HTF TP Escalation constants (v3.1)
_HTF_TP_TIMEFRAMES    = ('1h', '4h', '1d')
_HTF_TP_MAX_ATR       = 30.0
_HTF_TP_MIN_HTF_COUNT = 2


# ═══════════════════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════════════════

class EngineState(Enum):
    SCANNING     = auto()
    TRACKING     = auto()
    READY        = auto()
    ENTERING     = auto()
    IN_POSITION  = auto()
    POST_SWEEP   = auto()


class EntryType(Enum):
    PRE_SWEEP_APPROACH    = "APPROACH"
    SWEEP_REVERSAL        = "REVERSAL"
    SWEEP_CONTINUATION    = "CONTINUATION"
    DISPLACEMENT_MOMENTUM = "MOMENTUM"


@dataclass
class OrderFlowState:
    tick_flow:        float = 0.0
    cvd_trend:        float = 0.0
    cvd_divergence:   float = 0.0
    ob_imbalance:     float = 0.0
    tick_streak:      int   = 0
    streak_direction: str   = ""

    @property
    def conviction(self) -> float:
        signals = [self.tick_flow, self.cvd_trend]
        if abs(self.ob_imbalance) > 0.1:
            signals.append(self.ob_imbalance * 0.5)
        return sum(signals) / len(signals)

    @property
    def direction(self) -> str:
        c = self.conviction
        if c > _FLOW_CONV_THRESHOLD * 0.5:
            return "long"
        elif c < -_FLOW_CONV_THRESHOLD * 0.5:
            return "short"
        return ""

    @property
    def is_sustained(self) -> bool:
        return (self.tick_streak >= _FLOW_SUSTAINED_TICKS
                and self.streak_direction == self.direction)

    @property
    def cvd_agrees(self) -> bool:
        d = self.direction
        if d == "long":
            return self.cvd_trend > _FLOW_CVD_AGREE_MIN
        elif d == "short":
            return self.cvd_trend < -_FLOW_CVD_AGREE_MIN
        return False


@dataclass
class ICTSweepEvent:
    """Bridged sweep from ICT engine's liquidity_pools into entry engine."""
    pool_price:     float
    pool_type:      str       # "BSL" or "SSL"
    sweep_ts:       int       # ms epoch when the sweep occurred
    displacement:   bool      # displacement_confirmed flag
    disp_score:     float     # continuous displacement score 0.0-1.0
    wick_reject:    bool      # wick rejection confirmed
    candle_high:    float     # high of the candle that swept
    candle_low:     float     # low of the candle that swept
    candle_close:   float     # close of the candle that swept


@dataclass
class ICTContext:
    amd_phase:        str   = ""
    amd_bias:         str   = ""
    amd_confidence:   float = 0.0
    in_premium:       bool  = False
    in_discount:      bool  = False
    dealing_range_pd: float = 0.5
    structure_5m:     str   = ""
    structure_15m:    str   = ""
    structure_4h:     str   = ""
    bos_5m:           str   = ""
    choch_5m:         str   = ""
    nearest_ob_price: float = 0.0        # OB below price (long SL anchor)
    nearest_ob_price_short: float = 0.0  # BUG-FIX: OB above price (short SL anchor)
    kill_zone:        str   = ""
    ict_sweeps:       list  = field(default_factory=list)  # List[ICTSweepEvent]
    # DirectionEngine post-sweep verdict injection (Bug-4 fix).
    # When DirectionEngine.evaluate_sweep() returns action="reverse" or
    # "continue", quant_strategy.py writes the verdict here before calling
    # entry_engine.update().  _evaluate_post_sweep_accumulative() reads these
    # fields to weight its own evidence model, making DirectionEngine the
    # authoritative source for post-sweep direction rather than a disconnected
    # observer that only sends Telegram messages.
    direction_hint:            str   = ""    # "reverse" | "continue" | ""
    direction_hint_side:       str   = ""    # "long" | "short" | ""
    direction_hint_confidence: float = 0.0   # 0.0–1.0 from PostSweepDecision

    @property
    def session_quality(self) -> str:
        if self.kill_zone in ("london", "ny"):
            return "prime"
        elif self.kill_zone == "asia":
            return "fair"
        return "off_session"


@dataclass
class EntrySignal:
    side:           str
    entry_type:     EntryType
    entry_price:    float
    sl_price:       float
    tp_price:       float
    rr_ratio:       float
    target_pool:    PoolTarget
    sweep_result:   Optional[SweepResult] = None
    conviction:     float = 0.0
    reason:         str   = ""
    ict_validation: str   = ""
    created_at:     float = field(default_factory=time.time)


@dataclass
class PostSweepDecision:
    action:      str
    direction:   str
    confidence:  float
    next_target: Optional[PoolTarget] = None
    reason:      str                  = ""


# ═══════════════════════════════════════════════════════════════════════════
# INTERNAL STATE CONTAINERS
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class _TrackingState:
    direction:        str
    target:           PoolTarget
    started_at:       float
    flow_ticks:       int   = 0
    contrary_ticks:   int   = 0
    peak_conviction:  float = 0.0
    amd_mult:         float = 1.0
    last_contrary_ts: float = 0.0


@dataclass
class _PostSweepState:
    """
    Accumulative evidence tracker for post-sweep evaluation v3.3.

    Instead of stateless recalculation each tick, evidence BUILDS over the
    evaluation window. Each tick adds/removes evidence; the decision uses
    the accumulated peak, not just the instantaneous snapshot.
    """
    sweep:            SweepResult
    entered_at:       float
    initial_flow:     float = 0.0
    initial_flow_dir: str   = ""

    # ── Accumulative evidence ─────────────────────────────────────────
    rev_evidence:     float = 0.0     # accumulated reversal evidence
    cont_evidence:    float = 0.0     # accumulated continuation evidence
    peak_rev:         float = 0.0     # peak reversal evidence seen
    peak_cont:        float = 0.0     # peak continuation evidence seen
    tick_count:       int   = 0       # number of evaluation ticks

    # ── CISD tracking (Change in State of Delivery) ───────────────────
    cisd_detected:    bool  = False   # CHoCH or BOS in reversal direction
    cisd_timestamp:   float = 0.0     # when CISD was detected
    cisd_type:        str   = ""      # "choch" or "bos"

    # ── Displacement tracking ─────────────────────────────────────────
    max_displacement: float = 0.0     # max distance price moved from sweep level (ATR)
    displacement_dir: str   = ""      # "toward_reversal" or "continuation"
    disp_velocity:    float = 0.0     # displacement speed (ATR per second)

    # ── OTE zone tracking ─────────────────────────────────────────────
    ote_reached:      bool  = False   # price retraced to 50%-78.6% Fibonacci zone
    ote_timestamp:    float = 0.0     # when OTE was first reached
    ote_holding:      bool  = False   # price still in OTE zone

    # ── Price extremes since sweep ────────────────────────────────────
    highest_since:    float = 0.0     # highest price since sweep entered
    lowest_since:     float = float('inf')  # lowest price since sweep entered

    # ── Flow accumulation ─────────────────────────────────────────────
    rev_flow_ticks:   int   = 0       # ticks where flow agreed with reversal
    cont_flow_ticks:  int   = 0       # ticks where flow agreed with continuation

    # ── ICT sweep source (if bridged from ICT engine) ─────────────────
    ict_sweep_event:  Optional['ICTSweepEvent'] = None

    # ── Static evidence baseline (FIX-FLAW2) ─────────────────────────
    # Time-invariant factors (sweep quality, AMD bias, dealing range,
    # pool significance, OB blocking, session) are scored exactly ONCE
    # into these accumulators.  Per-tick scoring re-adds them every tick,
    # inflating the accumulated totals and defeating the phase-adaptive
    # gating — a setup scoring 44 pts/tick reaches the 71.5 DISPLACEMENT
    # threshold in 2 ticks purely by re-adding static facts.
    # Only truly dynamic factors (flow, CVD, CISD freshness, OTE holding,
    # live displacement growth, CHoCH/BOS events) feed the per-tick decay
    # model in rev_evidence / cont_evidence.
    static_scored:     bool  = False   # True after first scoring pass
    static_rev_base:   float = 0.0     # one-time reversal baseline
    static_cont_base:  float = 0.0     # one-time continuation baseline


# ═══════════════════════════════════════════════════════════════════════════
# ENTRY ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class EntryEngine:
    """Single-flow entry decision engine v3.4 — AMD contra-penalty + static/dynamic evidence split."""

    def __init__(self) -> None:
        self._state         = EngineState.SCANNING
        self._signal:       Optional[EntrySignal]   = None
        self._tracking:     Optional[_TrackingState] = None
        self._post_sweep:   Optional[_PostSweepState] = None
        self._last_entry_at = 0.0
        self._state_entered = time.time()
        self._last_sweep_analysis: Dict = {}

        self._proximity_confirms: int              = 0
        self._proximity_target:   Optional[PoolTarget] = None
        self._proximity_side:     str              = ""

        self._last_sweep_reversal_dir:  str   = ""
        self._last_sweep_reversal_time: float = 0.0
        self._last_liq_snapshot               = None

        # v3.0 EWMA flow tracking
        self._flow_ewma:             float = 0.0
        self._flow_ewma_last_update: float = 0.0

        # v3.0 Conviction decay (READY state)
        self._ready_conviction:      float = 0.0
        self._ready_peak_conviction: float = 0.0
        self._ready_last_agree_ts:   float = 0.0
        self._ready_rr_fail_count:   int   = 0

        # v3.0 Momentum entry tracking
        self._momentum_entries_1h:      int   = 0
        self._momentum_hour_start:      float = 0.0
        self._last_momentum_candle_ts:  int   = 0

    # ── Public API ────────────────────────────────────────────────────────

    def update(
        self,
        liq_snapshot: LiquidityMapSnapshot,
        flow_state:   OrderFlowState,
        ict_ctx:      ICTContext,
        price:        float,
        atr:          float,
        now:          float,
        candles_1m:   Optional[List[Dict]] = None,
        candles_5m:   Optional[List[Dict]] = None,
    ) -> None:
        if atr < 1e-10:
            return

        self._last_liq_snapshot = liq_snapshot
        self._update_flow_ewma(flow_state, now)

        # ── Check for sweeps from BOTH systems ────────────────────────
        # System 1: LiquidityMap sweeps (candle-based detection)
        new_sweeps = [s for s in liq_snapshot.recent_sweeps
                      if s.detected_at > now - 10.0
                      and s.quality >= _MIN_SWEEP_QUALITY]

        # System 2: ICT engine sweeps (bridged via ict_ctx.ict_sweeps)
        # These are sweeps the ICT engine detected on its own liquidity_pools
        # that the LiquidityMap missed entirely. Convert to SweepResult.
        # Only bridge when we're in a state that can actually enter POST_SWEEP.
        _ict_bridge_event = None
        if (not new_sweeps
                and self._state not in (EngineState.POST_SWEEP,
                                        EngineState.IN_POSITION,
                                        EngineState.ENTERING)
                and hasattr(ict_ctx, 'ict_sweeps') and ict_ctx.ict_sweeps):
            _age_limit_ms = int(now * 1000) - 30_000
            for ict_sw in ict_ctx.ict_sweeps:
                if ict_sw.sweep_ts < _age_limit_ms:
                    continue
                _synth_side = PoolSide.BSL if ict_sw.pool_type == "BSL" else PoolSide.SSL
                _synth_direction = "short" if ict_sw.pool_type == "BSL" else "long"
                _synth_pool = type('_SynthPool', (), {
                    'price': ict_sw.pool_price, 'side': _synth_side,
                    'timeframe': '5m', 'status': PoolStatus.SWEPT,
                    'significance': 3.0, 'ob_aligned': False,
                    'fvg_aligned': False, 'htf_count': 0,
                    'is_tradeable': False,
                    'sweep_wick': (ict_sw.candle_high if ict_sw.pool_type == "BSL"
                                   else ict_sw.candle_low),
                })()
                _synth_quality = min(1.0, 0.35 + 0.35 * ict_sw.disp_score
                                     + (0.15 if ict_sw.wick_reject else 0.0))
                if _synth_quality < _MIN_SWEEP_QUALITY:
                    continue
                _wick = (ict_sw.candle_high if ict_sw.pool_type == "BSL"
                         else ict_sw.candle_low)
                new_sweeps.append(SweepResult(
                    pool=_synth_pool, sweep_candle_idx=0,
                    wick_extreme=_wick, rejection_pct=ict_sw.disp_score,
                    volume_ratio=1.0, quality=_synth_quality,
                    direction=_synth_direction,
                    detected_at=ict_sw.sweep_ts / 1000.0,
                ))
                _ict_bridge_event = ict_sw
                logger.info(
                    f"🔗 ICT SWEEP BRIDGED: {ict_sw.pool_type} "
                    f"${ict_sw.pool_price:,.1f} disp={ict_sw.disp_score:.2f} "
                    f"quality={_synth_quality:.2f} → {_synth_direction.upper()}")

        if new_sweeps and self._state not in (
            EngineState.IN_POSITION, EngineState.ENTERING, EngineState.POST_SWEEP
        ):
            best_sweep = max(new_sweeps, key=lambda s: s.quality)
            # Attach ICT sweep event if this came from the bridge
            if _ict_bridge_event is None and hasattr(ict_ctx, 'ict_sweeps'):
                for ict_sw in ict_ctx.ict_sweeps:
                    if abs(ict_sw.pool_price - best_sweep.pool.price) < atr * 0.1:
                        _ict_bridge_event = ict_sw
                        break
            self._enter_post_sweep(best_sweep, liq_snapshot, flow_state,
                                   ict_ctx, price, atr, now, _ict_bridge_event)
            return

        if self._state in (EngineState.SCANNING, EngineState.TRACKING):
            if self._check_proximity_approach(liq_snapshot, flow_state,
                                              ict_ctx, price, atr, now):
                return

        if self._state == EngineState.SCANNING:
            if self._check_displacement_momentum(
                    liq_snapshot, flow_state, ict_ctx,
                    price, atr, now, candles_1m, candles_5m):
                return

        if self._state == EngineState.SCANNING:
            self._do_scanning(liq_snapshot, flow_state, ict_ctx, price, atr, now)
        elif self._state == EngineState.TRACKING:
            self._do_tracking(liq_snapshot, flow_state, ict_ctx, price, atr, now)
        elif self._state == EngineState.READY:
            self._do_ready(liq_snapshot, flow_state, ict_ctx, price, atr, now)
        elif self._state == EngineState.POST_SWEEP:
            self._do_post_sweep(liq_snapshot, flow_state, ict_ctx, price, atr, now)
        elif self._state in (EngineState.ENTERING, EngineState.IN_POSITION):
            _stuck_limit = 120.0 if self._state == EngineState.ENTERING else 14400.0
            if now - self._state_entered > _stuck_limit:
                logger.warning(
                    f"Entry engine SELF-RECOVERY: stuck in {self._state.name} "
                    f"for {now - self._state_entered:.0f}s — forcing SCANNING")
                self._reset_to_scanning(now)

    def get_signal(self) -> Optional[EntrySignal]:
        return self._signal

    def consume_signal(self) -> Optional[EntrySignal]:
        sig = self._signal
        self._signal = None
        return sig

    def on_entry_placed(self) -> None:
        self._state = EngineState.ENTERING
        self._state_entered = time.time()
        self._last_entry_at = time.time()
        self._signal = None
        self._proximity_confirms = 0
        self._proximity_target = None

    def on_entry_failed(self) -> None:
        if self._state in (EngineState.ENTERING, EngineState.IN_POSITION):
            logger.info(f"Entry engine: {self._state.name} -> SCANNING (entry failed)")
            self._reset_to_scanning(time.time())

    def on_position_opened(self) -> None:
        self._state = EngineState.IN_POSITION
        self._state_entered = time.time()

    def on_entry_cancelled(self) -> None:
        self._post_sweep = None
        self._proximity_side = ""
        self._last_entry_at = time.time()
        self._ready_rr_fail_count = 0
        if self._state not in (EngineState.SCANNING, EngineState.IN_POSITION):
            logger.info(f"Entry engine: {self._state.name} -> SCANNING (entry cancelled)")
            self._reset_to_scanning(time.time())

    def on_position_closed(self) -> None:
        self._reset_to_scanning(time.time())

    def force_reset(self) -> None:
        self._reset_to_scanning(time.time())
        self._flow_ewma = 0.0
        self._flow_ewma_last_update = 0.0
        self._momentum_entries_1h = 0
        self._last_momentum_candle_ts = 0

    def _reset_to_scanning(self, now: float) -> None:
        self._state = EngineState.SCANNING
        self._state_entered = now
        self._signal = None
        self._tracking = None
        self._post_sweep = None
        self._proximity_confirms = 0
        self._proximity_target = None
        self._proximity_side = ""
        self._ready_conviction = 0.0
        self._ready_peak_conviction = 0.0
        self._ready_rr_fail_count = 0

    @property
    def state(self) -> str:
        return self._state.name

    @property
    def tracking_info(self) -> Optional[Dict]:
        if self._tracking is None:
            return None
        return {
            "direction":    self._tracking.direction,
            "target":       f"${self._tracking.target.pool.price:,.1f}",
            "distance_atr": f"{self._tracking.target.distance_atr:.1f}",
            "flow_ticks":   self._tracking.flow_ticks,
            "started":      f"{time.time() - self._tracking.started_at:.0f}s ago",
        }

    # ── BUG-FIX-1: Time-normalised EWMA ──────────────────────────────────

    def _update_flow_ewma(self, flow: OrderFlowState, now: float) -> float:
        """
        BUG-FIX-1: Continuous-time EWMA — alpha scales with elapsed time.

        Old code: alpha was a fixed per-tick constant. dt was computed but
        discarded. After stream gaps (reconnects, lag), the EWMA was
        artificially stale — it had barely decayed despite real time passing.

        Fix: alpha = 1 - exp(-alpha_per_sec * min(dt, 2.0))
        Calibration: original base_alpha=0.15 at dt=0.25s
          alpha_per_sec = -ln(1-0.15)/0.25 = 0.648
        At dt=0.25s: alpha = 0.15 (unchanged)
        At dt=1.0s:  alpha = 0.48 (correct — 4x more decay in 4x time)
        At dt=5.0s:  alpha = 0.96 (near-reset after 5s silence)
        """
        dt = now - self._flow_ewma_last_update if self._flow_ewma_last_update > 0 else 0.25
        self._flow_ewma_last_update = now

        if flow.direction == "long":
            signed = abs(flow.conviction)
        elif flow.direction == "short":
            signed = -abs(flow.conviction)
        else:
            signed = 0.0

        dt_clamped = min(dt, 2.0)
        base_alpha = 1.0 - math.exp(-_FLOW_EWMA_ALPHA_PER_SEC * dt_clamped)
        conviction_boost = min(abs(flow.conviction), 1.0)
        alpha = min(base_alpha * (1.0 + conviction_boost), _FLOW_EWMA_ADAPTIVE_CAP)

        self._flow_ewma = alpha * signed + (1.0 - alpha) * self._flow_ewma
        return self._flow_ewma

    def _ewma_direction(self) -> str:
        if self._flow_ewma > _FLOW_CONV_THRESHOLD * 0.4:
            return "long"
        elif self._flow_ewma < -_FLOW_CONV_THRESHOLD * 0.4:
            return "short"
        return ""

    # ── AMD Conviction Modifier ───────────────────────────────────────────

    def _amd_conviction_modifier(self, ict: ICTContext, direction: str) -> float:
        """AMD conviction modifier in [0.60, 1.10]. Never blocks — only sizes."""
        _phase = (ict.amd_phase or '').upper()
        _bias  = (ict.amd_bias or '').lower()
        if not _phase or not _bias or _bias == "neutral":
            return 1.0
        is_against = (
            (direction == "long"  and _bias == "bearish") or
            (direction == "short" and _bias == "bullish")
        )
        is_aligned = (
            (direction == "long"  and _bias == "bullish") or
            (direction == "short" and _bias == "bearish")
        )
        if is_aligned:
            return _AMD_ALIGNED_BONUS
        if not is_against:
            return 1.0
        if _phase == 'MANIPULATION':
            return _AMD_MANIP_CONTRA_MULT
        elif _phase in ('DISTRIBUTION', 'REDISTRIBUTION'):
            return _AMD_DIST_CONTRA_MULT
        return 1.0

    # ── HTF TP Escalation ─────────────────────────────────────────────────

    def _find_htf_tp(
        self,
        snap:     LiquidityMapSnapshot,
        side:     str,
        price:    float,
        atr:      float,
        sl_price: float,
        min_rr:   float,
    ) -> Tuple[Optional[float], Optional[PoolTarget]]:
        """
        HTF TP Escalation (v3.1).

        When the primary pool TP fails R:R, search for a qualifying HTF pool:
          - Native 1h / 4h / 1d pool  (_HTF_TP_TIMEFRAMES), OR
          - Any pool with htf_count >= _HTF_TP_MIN_HTF_COUNT (multi-TF confluence)

        Search distance: _MIN_TARGET_ATR to _HTF_TP_MAX_ATR (30 ATR).
        Sorted ascending so the nearest qualifying pool is tried first.
        Returns (tp_price, PoolTarget) or (None, None).
        """
        risk = abs(price - sl_price)
        if risk < 1e-10:
            return None, None

        pool_list = snap.bsl_pools if side == "long" else snap.ssl_pools
        htf_candidates = [
            t for t in pool_list
            if (t.pool.timeframe in _HTF_TP_TIMEFRAMES
                or t.pool.htf_count >= _HTF_TP_MIN_HTF_COUNT)
            and _MIN_TARGET_ATR <= t.distance_atr <= _HTF_TP_MAX_ATR
        ]
        if not htf_candidates:
            return None, None

        htf_candidates.sort(key=lambda t: t.distance_atr)

        for target in htf_candidates:
            tp = self._compute_tp_approach(target, side, price, atr)
            if tp is None:
                continue
            reward = abs(tp - price)
            if reward / risk >= min_rr:
                logger.info(
                    f"HTF TP escalation: [{target.pool.timeframe}] "
                    f"${target.pool.price:,.1f} ({target.distance_atr:.1f}ATR "
                    f"htf_count={target.pool.htf_count}) -> TP=${tp:,.1f} "
                    f"R:R={(reward/risk):.2f} (required>={min_rr:.2f})"
                )
                return tp, target

        return None, None

    # ── Displacement / Momentum Entry ─────────────────────────────────────

    def _check_displacement_momentum(
        self,
        snap:       LiquidityMapSnapshot,
        flow:       OrderFlowState,
        ict:        ICTContext,
        price:      float,
        atr:        float,
        now:        float,
        candles_1m: Optional[List[Dict]] = None,
        candles_5m: Optional[List[Dict]] = None,
    ) -> bool:
        """
        BUG-FIX-5: Target pool now uses proximity-adjusted selection, not
        snap.bsl_pools[0] (raw-significance rank, possibly 40+ ATR away).
        HTF escalation applied before synthetic 2x fallback.
        """
        if now - self._last_entry_at < _MOMENTUM_COOLDOWN_SEC:
            return False
        if now - self._momentum_hour_start > 3600.0:
            self._momentum_entries_1h = 0
            self._momentum_hour_start = now
        if self._momentum_entries_1h >= _MOMENTUM_MAX_ENTRIES_PER_HOUR:
            return False

        disp_candle    = None
        disp_tf        = ""
        disp_direction = ""

        for candles, tf_label in [(candles_5m, "5m"), (candles_1m, "1m")]:
            if not candles or len(candles) < _MOMENTUM_LOOKBACK_CANDLES + 20:
                continue
            vol_window = candles[-(20 + 2):-2]
            avg_vol = sum(float(c.get('v', 0)) for c in vol_window) / max(len(vol_window), 1)

            for offset in range(2, 2 + _MOMENTUM_LOOKBACK_CANDLES):
                if offset >= len(candles):
                    break
                c    = candles[-offset]
                c_o  = float(c['o']); c_c = float(c['c'])
                c_h  = float(c['h']); c_l = float(c['l'])
                c_v  = float(c.get('v', 0))
                c_ts = int(c.get('t', 0) or 0)

                if c_ts > 0 and c_ts == self._last_momentum_candle_ts:
                    continue

                body = abs(c_c - c_o)
                rng  = c_h - c_l
                if rng < 1e-10:
                    continue

                if body / rng < _MOMENTUM_MIN_BODY_RATIO:
                    continue
                if rng / max(atr, 1e-10) < _MOMENTUM_MIN_ATR_MOVE:
                    continue
                if c_v / max(avg_vol, 1e-10) < _MOMENTUM_MIN_VOL_RATIO:
                    continue

                candle_dir = "long" if c_c > c_o else "short"
                ewma_dir   = self._ewma_direction()
                if ewma_dir and ewma_dir != candle_dir:
                    continue

                disp_candle    = c
                disp_tf        = tf_label
                disp_direction = candle_dir
                break
            if disp_candle is not None:
                break

        if disp_candle is None:
            return False

        amd_mult = self._amd_conviction_modifier(ict, disp_direction)

        _sweep_cd = 60.0
        if (self._last_sweep_reversal_dir
                and now - self._last_sweep_reversal_time < _sweep_cd
                and disp_direction != self._last_sweep_reversal_dir):
            return False

        c_h = float(disp_candle['h']); c_l = float(disp_candle['l'])
        buffer = atr * _MOMENTUM_SL_BUFFER_ATR
        sl = c_l - buffer if disp_direction == "long" else c_h + buffer
        ict_sl = self._compute_sl(ict, disp_direction, price, atr)
        if ict_sl is not None:
            sl = min(sl, ict_sl) if disp_direction == "long" else max(sl, ict_sl)

        # BUG-FIX-5: proximity-adjusted target selection
        tp             = None
        _signal_target: Optional[PoolTarget] = None
        pool_list = snap.bsl_pools if disp_direction == "long" else snap.ssl_pools
        candidates = [t for t in pool_list
                      if _MIN_TARGET_ATR <= t.distance_atr <= _MAX_TARGET_ATR]
        if candidates:
            best = max(candidates, key=lambda t: t.adjusted_sig())
            tp_candidate = self._compute_tp_approach(best, disp_direction, price, atr)
            if tp_candidate is not None:
                tp = tp_candidate
                _signal_target = best

        adj_min_rr = _MOMENTUM_MIN_RR * (2.0 - amd_mult)
        risk = abs(price - sl) if sl is not None else 0.0
        if risk < 1e-10:
            return False

        if tp is None or abs(tp - price) / risk < adj_min_rr:
            _htf_tp, _htf_target = self._find_htf_tp(
                snap, disp_direction, price, atr, sl, adj_min_rr)
            if _htf_tp is not None:
                tp = _htf_tp
                _signal_target = _htf_target

        if tp is None:
            tp = price + risk * 2.0 if disp_direction == "long" else price - risk * 2.0

        reward = abs(tp - price)
        rr     = reward / risk
        if rr < adj_min_rr:
            return False

        sl_dist_pct = abs(price - sl) / price
        if sl_dist_pct < 0.001 or sl_dist_pct > 0.035:
            return False

        if _signal_target is None:
            reachable = [t for t in pool_list if t.distance_atr <= _HTF_TP_MAX_ATR]
            if not reachable:
                return False
            _signal_target = min(reachable, key=lambda t: t.distance_atr)

        c_ts = int(disp_candle.get('t', 0) or 0)
        self._last_momentum_candle_ts = c_ts
        self._momentum_entries_1h    += 1

        _htf_note = (f" [HTF {_signal_target.pool.timeframe}]"
                     if _signal_target.pool.timeframe in _HTF_TP_TIMEFRAMES else "")
        self._signal = EntrySignal(
            side=disp_direction,
            entry_type=EntryType.DISPLACEMENT_MOMENTUM,
            entry_price=price, sl_price=sl, tp_price=tp, rr_ratio=rr,
            target_pool=_signal_target,
            conviction=abs(flow.conviction) * amd_mult,
            reason=(f"DISPLACEMENT {disp_direction.upper()} [{disp_tf}] "
                    f"AMD×{amd_mult:.2f} R:R={rr:.1f}{_htf_note}"),
            ict_validation=self._ict_summary(ict, disp_direction),
        )
        logger.info(f"MOMENTUM SIGNAL: {disp_direction.upper()} [{disp_tf}] "
                    f"R:R={rr:.1f} AMD×{amd_mult:.2f} "
                    f"TP=[{_signal_target.pool.timeframe}]")
        return True

    # ── Proximity Approach — BUG-FIX-3 ───────────────────────────────────

    def _check_proximity_approach(
        self,
        snap:  LiquidityMapSnapshot,
        flow:  OrderFlowState,
        ict:   ICTContext,
        price: float,
        atr:   float,
        now:   float,
    ) -> bool:
        """
        BUG-FIX-3: BSL and SSL selection now use t.adjusted_sig() for both sides
        so the cross-side comparison is consistent (was: BSL by distance, SSL by sig).
        HTF escalation applied if the approaching pool's TP fails R:R.

        BUG-FIX-DIRECTIONAL (v3.2): Evaluate sides independently based on flow.
          Old: BSL and SSL competed by adjusted_sig(). BSL pools in declining
          markets accumulate higher significance (untouched), so LONG always won
          even when price was sliding >10%. Now flow direction gates which side
          is evaluated. When flow is neutral, both sides are checked but the
          side matching EWMA direction is preferred.
        """
        if now - self._last_entry_at < _ENTRY_COOLDOWN_SEC:
            return False

        best_approach: Optional[PoolTarget] = None
        approach_side: str                  = ""

        # ── BUG-FIX-DIRECTIONAL: Gate by flow + EWMA direction ──────────
        # In a sliding/declining market, flow is SHORT → only look at SSL pools
        # In a rising market, flow is LONG → only look at BSL pools
        # When neutral, check both but prefer EWMA-matching side
        _ewma_dir   = self._ewma_direction()
        _check_long = True
        _check_short = True

        # Strong directional flow gates out the opposing side
        if flow.direction == "long" and abs(flow.conviction) > 0.25:
            _check_short = False
        elif flow.direction == "short" and abs(flow.conviction) > 0.25:
            _check_long = False
        # EWMA as tiebreaker when flow is neutral
        elif _ewma_dir == "long":
            _check_short = abs(self._flow_ewma) < 0.15  # allow short only if EWMA is weak
        elif _ewma_dir == "short":
            _check_long = abs(self._flow_ewma) < 0.15

        best_long: Optional[PoolTarget] = None
        best_short: Optional[PoolTarget] = None

        if _check_long:
            for t in snap.bsl_pools:
                if (_PROXIMITY_ENTRY_ATR_MIN <= t.distance_atr <= _PROXIMITY_ENTRY_ATR_MAX
                        and t.significance >= _PROXIMITY_MIN_SIG):
                    if best_long is None or t.adjusted_sig() > best_long.adjusted_sig():
                        best_long = t

        if _check_short:
            for t in snap.ssl_pools:
                if (_PROXIMITY_ENTRY_ATR_MIN <= t.distance_atr <= _PROXIMITY_ENTRY_ATR_MAX
                        and t.significance >= _PROXIMITY_MIN_SIG):
                    if best_short is None or t.adjusted_sig() > best_short.adjusted_sig():
                        best_short = t

        # Pick the side matching flow direction; if both exist and flow is neutral,
        # take the one with higher adjusted significance
        if best_long is not None and best_short is not None:
            if flow.direction == "long":
                best_approach = best_long; approach_side = "long"
            elif flow.direction == "short":
                best_approach = best_short; approach_side = "short"
            elif _ewma_dir == "short":
                best_approach = best_short; approach_side = "short"
            elif _ewma_dir == "long":
                best_approach = best_long; approach_side = "long"
            else:
                # True neutral — take best significance
                if best_short.adjusted_sig() >= best_long.adjusted_sig():
                    best_approach = best_short; approach_side = "short"
                else:
                    best_approach = best_long; approach_side = "long"
        elif best_long is not None:
            best_approach = best_long; approach_side = "long"
        elif best_short is not None:
            best_approach = best_short; approach_side = "short"

        if best_approach is None:
            self._proximity_confirms = 0
            self._proximity_target   = None
            self._proximity_side     = ""
            return False

        _sweep_cd = 60.0
        if (self._last_sweep_reversal_dir
                and now - self._last_sweep_reversal_time < _sweep_cd
                and approach_side != self._last_sweep_reversal_dir):
            return False

        _amd_mult = self._amd_conviction_modifier(ict, approach_side)
        if _amd_mult < 1.0 and abs(flow.conviction) < _FLOW_CONV_THRESHOLD * 0.7:
            return False

        if (self._proximity_target is None
                or abs(self._proximity_target.pool.price - best_approach.pool.price) > atr * 0.3):
            self._proximity_confirms = 0
            self._proximity_target   = best_approach
            self._proximity_side     = approach_side

        confirmations = 0
        if approach_side == "long"  and flow.cvd_trend >  0.10:
            confirmations += 1
        elif approach_side == "short" and flow.cvd_trend < -0.10:
            confirmations += 1
        if best_approach.pool.ob_aligned:  confirmations += 1
        if best_approach.pool.fvg_aligned: confirmations += 1
        amd_agrees = (
            (approach_side == "long"  and ict.amd_bias == "bullish") or
            (approach_side == "short" and ict.amd_bias == "bearish")
        )
        if amd_agrees: confirmations += 1
        if ict.session_quality == "prime": confirmations += 1

        if confirmations < 1:
            return False
        if approach_side == "long"  and flow.conviction < -0.30:
            return False
        if approach_side == "short" and flow.conviction >  0.30:
            return False

        self._proximity_confirms += 1
        if self._proximity_confirms < _PROXIMITY_CONFIRM_COUNT:
            return False

        sl = self._compute_sl(ict, approach_side, price, atr)
        tp = self._compute_tp_approach(best_approach, approach_side, price, atr)
        if sl is None or tp is None:
            return False

        risk   = abs(price - sl)
        reward = abs(tp - price)
        if risk < 1e-10:
            return False
        rr = reward / risk

        if rr < _MIN_RR_RATIO:
            _htf_tp, _htf_target = self._find_htf_tp(
                snap, approach_side, price, atr, sl, _MIN_RR_RATIO)
            if _htf_tp is not None:
                tp            = _htf_tp
                reward        = abs(tp - price)
                rr            = reward / risk
                best_approach = _htf_target
            else:
                return False

        self._signal = EntrySignal(
            side=approach_side,
            entry_type=EntryType.PRE_SWEEP_APPROACH,
            entry_price=price, sl_price=sl, tp_price=tp, rr_ratio=rr,
            target_pool=best_approach,
            conviction=flow.conviction,
            reason=(f"PROXIMITY {approach_side.upper()} -> "
                    f"{best_approach.pool.side.value} ${best_approach.pool.price:,.1f} "
                    f"({best_approach.distance_atr:.2f}ATR) sig={best_approach.significance:.1f} "
                    f"confirms={confirmations} R:R={rr:.1f}"),
            ict_validation=self._ict_summary(ict, approach_side),
        )
        logger.info(f"PROXIMITY SIGNAL: {approach_side.upper()} -> "
                    f"${best_approach.pool.price:,.1f} | {self._signal.reason}")
        self._proximity_confirms = 0
        self._proximity_target   = None
        return True

    # ── State: SCANNING ───────────────────────────────────────────────────

    def _do_scanning(
        self,
        snap:  LiquidityMapSnapshot,
        flow:  OrderFlowState,
        ict:   ICTContext,
        price: float,
        atr:   float,
        now:   float,
    ) -> None:
        if now - self._last_entry_at < _ENTRY_COOLDOWN_SEC:
            return
        if not flow.direction:
            return

        target = self._find_flow_target(snap, flow, price, atr)
        if target is None:
            return

        amd_mult = self._amd_conviction_modifier(ict, flow.direction)
        pd       = float(getattr(ict, 'dealing_range_pd', 0.5) or 0.5)
        _bias    = (ict.amd_bias or '').lower()
        _phase   = (ict.amd_phase or '').upper()

        if (_bias == 'bearish' and pd > 0.60
                and _phase in ('MANIPULATION', 'DISTRIBUTION', 'REDISTRIBUTION')
                and flow.direction == "long"
                and abs(flow.conviction) < _FLOW_CONV_THRESHOLD * 0.75):
            return
        if (_bias == 'bullish' and pd < 0.40
                and _phase in ('MANIPULATION', 'DISTRIBUTION', 'REDISTRIBUTION')
                and flow.direction == "short"
                and abs(flow.conviction) < _FLOW_CONV_THRESHOLD * 0.75):
            return

        self._tracking = _TrackingState(
            direction=flow.direction, target=target, started_at=now,
            flow_ticks=1, peak_conviction=abs(flow.conviction), amd_mult=amd_mult,
        )
        self._state         = EngineState.TRACKING
        self._state_entered = now
        logger.info(
            f"TRACKING: {flow.direction.upper()} -> "
            f"${target.pool.price:,.1f} ({target.pool.side.value}) "
            f"dist={target.distance_atr:.1f}ATR sig={target.significance:.1f} "
            f"flow={flow.conviction:+.2f}"
            f"{f' AMD×{amd_mult:.2f}' if amd_mult < 1.0 else ''}"
        )

    # ── State: TRACKING ───────────────────────────────────────────────────

    def _do_tracking(
        self,
        snap:  LiquidityMapSnapshot,
        flow:  OrderFlowState,
        ict:   ICTContext,
        price: float,
        atr:   float,
        now:   float,
    ) -> None:
        tr = self._tracking
        if tr is None:
            self._state = EngineState.SCANNING
            return

        if now - tr.started_at > _TRACKING_TIMEOUT_SEC:
            logger.info("TRACKING timeout — back to scanning")
            self._tracking      = None
            self._state         = EngineState.SCANNING
            self._state_entered = now
            return

        hold_time = now - tr.started_at

        ewma_agrees  = (
            (tr.direction == "long"  and self._flow_ewma >  0.05) or
            (tr.direction == "short" and self._flow_ewma < -0.05)
        )
        ewma_contrary = (
            (tr.direction == "long"  and self._flow_ewma < -_FLOW_CONV_THRESHOLD * 0.3) or
            (tr.direction == "short" and self._flow_ewma >  _FLOW_CONV_THRESHOLD * 0.3)
        )

        if flow.direction == tr.direction:
            tr.flow_ticks     += 1
            tr.contrary_ticks  = 0
            tr.peak_conviction = max(tr.peak_conviction, abs(flow.conviction))
            tr.last_contrary_ts = 0.0
        elif flow.direction and flow.direction != tr.direction:
            tr.contrary_ticks += 1
            if tr.last_contrary_ts == 0.0:
                tr.last_contrary_ts = now

        _sustained_contrary_sec = (
            now - tr.last_contrary_ts if tr.last_contrary_ts > 0 else 0.0)

        if (hold_time >= _TRACKING_MIN_HOLD_SEC
                and ewma_contrary
                and _sustained_contrary_sec >= 3.0):
            logger.info(f"TRACKING aborted: flow reversed ({tr.direction} -> "
                        f"{flow.direction}) EWMA={self._flow_ewma:+.2f} "
                        f"sustained={_sustained_contrary_sec:.1f}s")
            self._tracking      = None
            self._state         = EngineState.SCANNING
            self._state_entered = now
            return

        if flow.direction == tr.direction:
            target = self._find_flow_target(snap, flow, price, atr)
            if target is not None:
                tr.target = target

        ready = (
            tr.flow_ticks >= _FLOW_SUSTAINED_TICKS
            and flow.cvd_agrees
            and abs(flow.conviction) >= _FLOW_CONV_THRESHOLD
            and _MIN_TARGET_ATR <= tr.target.distance_atr <= _MAX_TARGET_ATR
        )

        if not ready and tr.flow_ticks >= 1:
            if (self._ict_structure_agrees(ict, tr.direction)
                    and abs(flow.conviction) >= _FLOW_CONV_THRESHOLD * 0.65):
                ready = True

        if not ready and tr.flow_ticks >= 3:
            ewma_strong   = abs(self._flow_ewma) >= _FLOW_CONV_THRESHOLD * 0.8
            ewma_dir_ok   = (
                (tr.direction == "long"  and self._flow_ewma > 0) or
                (tr.direction == "short" and self._flow_ewma < 0)
            )
            flow_not_contra = (
                not flow.direction
                or flow.direction == tr.direction
                or abs(flow.conviction) < _FLOW_CONV_THRESHOLD
            )
            if ewma_strong and ewma_dir_ok and flow_not_contra:
                ready = True

        if ready:
            _pre_sl = self._compute_sl(ict, tr.direction, price, atr)
            _pre_tp = self._compute_tp_approach(tr.target, tr.direction, price, atr)
            if _pre_sl is not None and _pre_tp is not None:
                _pre_risk   = abs(price - _pre_sl)
                _pre_reward = abs(_pre_tp - price)
                if _pre_risk > 1e-10:
                    _pre_rr    = _pre_reward / _pre_risk
                    _adj_rr    = _MIN_RR_RATIO * (2.0 - tr.amd_mult)
                    if _pre_rr < _adj_rr * 0.7:
                        _htf_tp_pre, _ = self._find_htf_tp(
                            snap, tr.direction, price, atr, _pre_sl, _adj_rr)
                        if _htf_tp_pre is None:
                            ready = False

        if ready:
            self._state              = EngineState.READY
            self._state_entered      = now
            self._ready_conviction   = abs(flow.conviction)
            self._ready_peak_conviction = self._ready_conviction
            self._ready_last_agree_ts   = now
            self._ready_rr_fail_count   = 0
            logger.info(
                f"READY: {tr.direction.upper()} -> "
                f"${tr.target.pool.price:,.1f} "
                f"conviction={self._ready_conviction:+.2f} ticks={tr.flow_ticks}"
                f"{f' AMD×{tr.amd_mult:.2f}' if tr.amd_mult < 1.0 else ''}"
            )

    # ── State: READY ──────────────────────────────────────────────────────

    def _do_ready(
        self,
        snap:  LiquidityMapSnapshot,
        flow:  OrderFlowState,
        ict:   ICTContext,
        price: float,
        atr:   float,
        now:   float,
    ) -> None:
        tr = self._tracking
        if tr is None:
            self._state = EngineState.SCANNING
            return

        if now - self._state_entered > _READY_TIMEOUT_SEC:
            logger.info("READY timeout — back to scanning")
            self._tracking      = None
            self._state         = EngineState.SCANNING
            self._state_entered = now
            return

        hold_time    = now - self._state_entered
        flow_agrees  = (flow.direction == tr.direction)
        flow_contrary = (flow.direction != "" and flow.direction != tr.direction)

        if flow_agrees:
            self._ready_last_agree_ts = now
            recovery = min(0.02, abs(flow.conviction) * 0.05)
            self._ready_conviction = min(
                self._ready_peak_conviction,
                self._ready_conviction + recovery)
            tr.contrary_ticks = 0
        elif flow_contrary:
            tr.contrary_ticks += 1
            decay = _READY_DECAY_RATE * max(abs(flow.conviction), 0.3)
            self._ready_conviction = max(0.0, self._ready_conviction - decay)

        ewma_contrary = (
            (tr.direction == "long"  and self._flow_ewma < -0.10) or
            (tr.direction == "short" and self._flow_ewma >  0.10)
        )

        if (hold_time >= _READY_MIN_HOLD_SEC
                and self._ready_conviction < _READY_MIN_CONVICTION
                and ewma_contrary):
            logger.info(f"READY: conviction decayed {self._ready_conviction:.3f} "
                        f"< {_READY_MIN_CONVICTION} — back to scanning")
            self._tracking      = None
            self._state         = EngineState.SCANNING
            self._state_entered = now
            return

        sl = self._compute_sl(ict, tr.direction, price, atr)
        tp = self._compute_tp_approach(tr.target, tr.direction, price, atr)

        if sl is None or tp is None:
            _fail = self._ready_rr_fail_count + 1
            self._ready_rr_fail_count = _fail
            if _fail == 1 or _fail % 40 == 0:
                logger.debug(f"READY: SL/TP unavailable (fail #{_fail})")
            if _fail >= 120:
                logger.info(f"READY: SL/TP failed {_fail} ticks — aborting")
                self._tracking      = None
                self._state         = EngineState.SCANNING
                self._state_entered = now
            return

        _sweep_cd = 60.0
        if (self._last_sweep_reversal_dir
                and now - self._last_sweep_reversal_time < _sweep_cd
                and tr.direction != self._last_sweep_reversal_dir):
            logger.info(f"READY: {tr.direction.upper()} blocked — recent sweep reversal")
            self._tracking      = None
            self._state         = EngineState.SCANNING
            self._state_entered = now
            return

        risk   = abs(price - sl)
        reward = abs(tp - price)
        if risk < 1e-10:
            return
        rr = reward / risk

        adj_min_rr = _MIN_RR_RATIO * (2.0 - tr.amd_mult)

        # HTF TP ESCALATION
        _escalated: Optional[PoolTarget] = None
        if rr < adj_min_rr:
            _htf_tp, _htf_target = self._find_htf_tp(
                snap, tr.direction, price, atr, sl, adj_min_rr)
            if _htf_tp is not None:
                tp            = _htf_tp
                reward        = abs(tp - price)
                rr            = reward / risk
                _escalated    = _htf_target
                self._ready_rr_fail_count = 0
            else:
                _fail = self._ready_rr_fail_count + 1
                self._ready_rr_fail_count = _fail
                if _fail == 1 or _fail % 60 == 0:
                    logger.debug(
                        f"READY: R:R {rr:.2f} < {adj_min_rr:.2f} "
                        f"AMD×{tr.amd_mult:.2f}, no HTF TP (fail #{_fail})")
                if _fail >= 80:
                    logger.info(
                        f"READY: R:R failed {_fail} ticks "
                        f"(best={rr:.2f} vs required={adj_min_rr:.2f}) "
                        f"no HTF TP in range — aborting")
                    self._tracking      = None
                    self._state         = EngineState.SCANNING
                    self._state_entered = now
                return
        else:
            self._ready_rr_fail_count = 0

        _signal_target = _escalated if _escalated is not None else tr.target
        _htf_note      = (f" [HTF-TP {_escalated.pool.timeframe} "
                          f"${_escalated.pool.price:,.1f}]"
                          if _escalated is not None else "")

        self._signal = EntrySignal(
            side=tr.direction,
            entry_type=EntryType.PRE_SWEEP_APPROACH,
            entry_price=price, sl_price=sl, tp_price=tp, rr_ratio=rr,
            target_pool=_signal_target,
            conviction=self._ready_conviction * tr.amd_mult,
            reason=(f"Flow {tr.direction} -> {_signal_target.pool.side.value} "
                    f"${_signal_target.pool.price:,.1f} | "
                    f"flow={flow.conviction:+.2f} CVD={flow.cvd_trend:+.2f} | "
                    f"R:R={rr:.1f}"
                    f"{f' AMD×{tr.amd_mult:.2f}' if tr.amd_mult < 1.0 else ''}"
                    f"{_htf_note}"),
            ict_validation=self._ict_summary(ict, tr.direction),
        )
        logger.info(f"SIGNAL: {self._signal.entry_type.value} "
                    f"{self._signal.side.upper()} | {self._signal.reason}")

    # ═══════════════════════════════════════════════════════════════════════
    # POST-SWEEP PIPELINE v3.3 — Industry-Grade Multi-Phase Evaluation
    # ═══════════════════════════════════════════════════════════════════════
    #
    # Architecture:
    #   Phase 1 — DISPLACEMENT (0-30s):  Expect strong price rejection from sweep.
    #             Requires overwhelming evidence (1.3× score threshold).
    #   Phase 2 — CISD (30-90s):  Expect CHoCH/BOS confirming reversal or BOS
    #             confirming continuation. Standard thresholds apply.
    #   Phase 3 — OTE (90-180s):  Expect price retracement to 50%-78.6% Fibonacci
    #             zone of the sweep displacement move. Standard thresholds.
    #   Phase 4 — MATURE (180-240s):  Relaxed thresholds (0.75× score). Final
    #             opportunity before timeout. Stale setups accepted if evidence
    #             accumulated over earlier phases.
    #
    # Evidence model: ACCUMULATIVE. Each tick adds/removes evidence to running
    # totals. Decision uses peak accumulated evidence, not just instantaneous
    # snapshot. This prevents momentary flow noise from killing valid setups.
    #
    # CISD: Tracks CHoCH and BOS in the reversal direction. CISD confirmation
    # provides the structural green-light for reversal entries.
    #
    # Displacement: Measures how far and how fast price moved away from the
    # sweep level. Strong displacement (>1.2 ATR) is the strongest reversal
    # signal. Weak displacement favors continuation.
    #
    # OTE: When price retraces to the 50%-78.6% Fibonacci zone of the
    # displacement move, it's the optimal institutional entry point.
    #
    # SL: Reversal SL uses 0.35×ATR buffer beyond sweep wick (was 0.12×ATR
    # which sat in the death zone for sweep retests). Continuation SL placed
    # BEYOND the swept pool with 0.40×ATR buffer (was AT the pool where
    # stops already triggered).
    #
    # Synthetic TP: 2R fallback only fires when CISD is confirmed, preventing
    # structureless trades from generating signals.
    # ═══════════════════════════════════════════════════════════════════════

    def _enter_post_sweep(
        self,
        sweep:  SweepResult,
        snap:   LiquidityMapSnapshot,
        flow:   OrderFlowState,
        ict:    ICTContext,
        price:  float,
        atr:    float,
        now:    float,
        ict_sweep_event: Optional['ICTSweepEvent'] = None,
    ) -> None:
        """Initialize post-sweep evaluation with accumulative evidence tracker."""
        _TF_QUALITY_MIN = {'1m': 0.65, '5m': 0.55, '15m': 0.45, '4h': 0.35}
        _tf             = getattr(sweep.pool, 'timeframe', '5m')
        _req            = _TF_QUALITY_MIN.get(_tf, 0.45)
        if sweep.quality < _req:
            logger.debug(f"SWEEP SKIPPED [{_tf}]: quality={sweep.quality:.2f} < {_req:.2f}")
            return

        self._post_sweep = _PostSweepState(
            sweep=sweep, entered_at=now,
            initial_flow=flow.conviction, initial_flow_dir=flow.direction,
            highest_since=price, lowest_since=price,
            ict_sweep_event=ict_sweep_event,
        )
        self._state         = EngineState.POST_SWEEP
        self._state_entered = now
        self._tracking      = None
        self._signal        = None

        _src = "ICT-BRIDGE" if ict_sweep_event else "LIQ-MAP"
        logger.info(
            f"🎯 POST-SWEEP ENTERED [{_src}]: {sweep.pool.side.value} "
            f"${sweep.pool.price:,.1f} quality={sweep.quality:.2f} "
            f"wick=${sweep.wick_extreme:,.1f} "
            f"| Multi-phase evaluation started...")

    def _do_post_sweep(
        self,
        snap:  LiquidityMapSnapshot,
        flow:  OrderFlowState,
        ict:   ICTContext,
        price: float,
        atr:   float,
        now:   float,
    ) -> None:
        """
        Multi-phase post-sweep evaluation with accumulative evidence.

        Each tick:
          1. Update price extremes and displacement tracking
          2. Check for CISD (CHoCH/BOS in reversal direction)
          3. Check for OTE zone entry/exit
          4. Accumulate evidence from all sources
          5. Determine current phase and apply phase-adaptive thresholds
          6. Make decision when thresholds are met or timeout
        """
        ps = self._post_sweep
        if ps is None:
            self._state = EngineState.SCANNING
            return

        elapsed = now - ps.entered_at
        if elapsed < _POST_SWEEP_EVAL_SEC:
            return

        if elapsed > _CISD_MAX_WAIT_SEC:
            _peak = max(ps.peak_rev, ps.peak_cont)
            logger.info(
                f"POST_SWEEP: timeout after {elapsed:.0f}s — "
                f"peak_rev={ps.peak_rev:.0f} peak_cont={ps.peak_cont:.0f} "
                f"CISD={'YES' if ps.cisd_detected else 'NO'} "
                f"disp={ps.max_displacement:.2f}ATR "
                f"OTE={'YES' if ps.ote_reached else 'NO'}")
            self._post_sweep    = None
            self._state         = EngineState.SCANNING
            self._state_entered = now
            return

        ps.tick_count += 1

        # ── 1. Update price extremes ──────────────────────────────────
        ps.highest_since = max(ps.highest_since, price)
        ps.lowest_since  = min(ps.lowest_since, price)

        # ── 2. Displacement tracking ─────────────────────────────────
        sweep_price = ps.sweep.pool.price
        sweep_dir   = ps.sweep.direction  # "short" = BSL swept, "long" = SSL swept
        # Reversal direction: opposite of what was swept
        # BSL swept → direction="short" → reversal = SHORT (price should drop)
        # SSL swept → direction="long"  → reversal = LONG (price should rise)
        rev_dir = sweep_dir  # sweep.direction IS the reversal direction

        if rev_dir == "long":
            # SSL swept, expect price to rise
            disp_from_sweep = (price - sweep_price) / max(atr, 1e-10)
            max_disp = (ps.highest_since - sweep_price) / max(atr, 1e-10)
        else:
            # BSL swept, expect price to drop
            disp_from_sweep = (sweep_price - price) / max(atr, 1e-10)
            max_disp = (sweep_price - ps.lowest_since) / max(atr, 1e-10)

        if max_disp > ps.max_displacement:
            ps.max_displacement = max_disp
            ps.displacement_dir = "toward_reversal" if max_disp > 0 else "continuation"
            if elapsed > 0:
                ps.disp_velocity = max_disp / elapsed

        # ── 3. CISD detection (Change in State of Delivery) ──────────
        if not ps.cisd_detected:
            choch_5m = getattr(ict, 'choch_5m', '') or ''
            bos_5m   = getattr(ict, 'bos_5m', '') or ''
            # For reversal: need CHoCH or BOS in the reversal direction
            _rev_struct = "bearish" if rev_dir == "short" else "bullish"
            if choch_5m == _rev_struct:
                ps.cisd_detected  = True
                ps.cisd_timestamp = now
                ps.cisd_type      = "choch"
                logger.info(f"POST_SWEEP: 🔄 CISD CONFIRMED (CHoCH {_rev_struct}) "
                           f"at {elapsed:.0f}s post-sweep")
            elif bos_5m == _rev_struct:
                ps.cisd_detected  = True
                ps.cisd_timestamp = now
                ps.cisd_type      = "bos"
                logger.info(f"POST_SWEEP: 🔄 CISD CONFIRMED (BOS {_rev_struct}) "
                           f"at {elapsed:.0f}s post-sweep")

        # ── 4. OTE zone tracking (50%-78.6% Fibonacci retrace) ───────
        if ps.max_displacement >= _PS_DISP_MIN_ATR:
            # Calculate Fibonacci retracement of the displacement move
            if rev_dir == "long":
                # SSL swept, price rose. OTE = retrace back down to 50-78.6%
                swing_low  = sweep_price
                swing_high = ps.highest_since
            else:
                # BSL swept, price dropped. OTE = retrace back up to 50-78.6%
                swing_high = sweep_price
                swing_low  = ps.lowest_since

            swing_range = abs(swing_high - swing_low)
            if swing_range > 1e-10:
                if rev_dir == "long":
                    # Retracement down from high
                    retrace_pct = (ps.highest_since - price) / swing_range
                else:
                    # Retracement up from low
                    retrace_pct = (price - ps.lowest_since) / swing_range

                in_ote = _PS_OTE_FIB_LOW <= retrace_pct <= _PS_OTE_FIB_HIGH
                if in_ote and not ps.ote_reached:
                    ps.ote_reached   = True
                    ps.ote_timestamp = now
                    logger.info(f"POST_SWEEP: 📐 OTE ZONE REACHED "
                               f"(retrace={retrace_pct:.1%}) at {elapsed:.0f}s")
                ps.ote_holding = in_ote

        # ── 5. Accumulate evidence ────────────────────────────────────
        decision = self._evaluate_post_sweep_accumulative(
            ps, snap, flow, ict, price, atr, now)

        if decision.action == "reverse":
            self._handle_sweep_reversal(ps.sweep, decision, snap, flow, ict, price, atr, now)
        elif decision.action == "continue":
            self._handle_sweep_continuation(ps.sweep, decision, snap, flow, ict, price, atr, now)

    def _evaluate_post_sweep_accumulative(
        self,
        ps:    _PostSweepState,
        snap:  LiquidityMapSnapshot,
        flow:  OrderFlowState,
        ict:   ICTContext,
        price: float,
        atr:   float,
        now:   float,
    ) -> PostSweepDecision:
        """
        Accumulative evidence model for post-sweep evaluation.

        Evidence is ADDED to running totals each tick (with decay for
        contradictory signals). The decision checks accumulated peaks
        against phase-adaptive thresholds.
        """
        sweep     = ps.sweep
        sweep_dir = sweep.direction  # reversal direction
        cont_dir  = "short" if sweep_dir == "long" else "long"
        elapsed   = now - ps.entered_at

        # ── Determine current phase ──────────────────────────────────
        if elapsed < _PS_PHASE_DISPLACEMENT_SEC:
            phase = "DISPLACEMENT"
            score_mult = _PS_DISP_SCORE_MULT       # 1.30 — need overwhelming evidence
            base_threshold = _PS_SCORE_THRESHOLD_EARLY
        elif elapsed < _PS_PHASE_CISD_SEC:
            phase = "CISD"
            score_mult = 1.0
            base_threshold = _PS_SCORE_THRESHOLD_NORMAL
        elif elapsed < _PS_PHASE_OTE_SEC:
            phase = "OTE"
            score_mult = 1.0
            base_threshold = _PS_SCORE_THRESHOLD_NORMAL
        else:
            phase = "MATURE"
            score_mult = _PS_MATURE_SCORE_MULT      # 0.75 — accept weaker setups
            base_threshold = _PS_SCORE_THRESHOLD_MATURE

        # ── Instantaneous evidence this tick ──────────────────────────
        rev_delta  = 0.0
        cont_delta = 0.0
        rev_reasons:  List[str] = []
        cont_reasons: List[str] = []

        # ─────────────────────────────────────────────────────────────
        # FIX-FLAW2: STATIC FACTORS — scored ONCE into ps.static_*_base
        # ─────────────────────────────────────────────────────────────
        # These factors are time-invariant (or near-invariant) between
        # ticks: sweep quality never changes; AMD phase/bias updates
        # every few minutes at most; dealing range, OB blocking, and
        # pool significance are slow-changing structural facts.
        #
        # Under the old design they were re-added every tick into
        # ps.rev_evidence.  At ~44 pts/tick a marginal setup reached
        # the DISPLACEMENT threshold (55 × 1.30 = 71.5) in exactly
        # 2 ticks, making the phase-adaptive gating effectively useless.
        #
        # Fix: score them into ps.static_rev_base / ps.static_cont_base
        # on ps.tick_count == 1, then add those fixed bases to the final
        # totals without re-adding each tick.  Only truly dynamic signals
        # (flow, CVD, CISD freshness, OTE holding, live displacement
        # growth, CHoCH/BOS events, sustained flow) feed the per-tick
        # decay accumulator in ps.rev_evidence / ps.cont_evidence.
        # ─────────────────────────────────────────────────────────────

        _score_static = not ps.static_scored
        _s_rev  = 0.0   # static rev pts accumulated this pass
        _s_cont = 0.0   # static cont pts accumulated this pass

        # AMD phase scoring  [STATIC]
        # ─────────────────────────────────────────────────────────────
        # FIX-FLAW1: AMD CONTRA-PENALTY in MANIPULATION phase
        # ─────────────────────────────────────────────────────────────
        # Old: BSL swept + bullish AMD bias → +0 (bonus not fired, no
        #   penalty either).  With amd_conf=0.95 this is a high-confidence
        #   contra-signal being completely ignored.
        # New: BSL + bullish or SSL + bearish during MANIPULATION applies
        #   a 18 × conf penalty to reversal AND a 10.8 × conf bonus to
        #   continuation, proportional to confidence.  The penalty is
        #   deliberately smaller than the alignment bonus (22) to avoid
        #   over-blocking — it should make the setup marginal, not
        #   impossible, so that strong structural evidence (CISD, OTE,
        #   flow) can still overcome a contra AMD reading.
        # ─────────────────────────────────────────────────────────────
        amd_phase = (getattr(ict, 'amd_phase', '') or '').upper()
        amd_bias  = (getattr(ict, 'amd_bias',  '') or '').lower()
        amd_conf  = float(getattr(ict, 'amd_confidence', 0.0) or 0.0)

        if _score_static:
            if amd_phase == 'MANIPULATION':
                _bsl_with_bear = (sweep.pool.side == PoolSide.BSL and amd_bias == 'bearish')
                _ssl_with_bull = (sweep.pool.side == PoolSide.SSL and amd_bias == 'bullish')
                _bsl_with_bull = (sweep.pool.side == PoolSide.BSL and amd_bias == 'bullish')
                _ssl_with_bear = (sweep.pool.side == PoolSide.SSL and amd_bias == 'bearish')
                if _bsl_with_bear or _ssl_with_bull:
                    # AMD ALIGNED: sweep IS the manipulation phase fake-out before
                    # delivery in the opposite direction — strongly supports reversal.
                    pts = 22.0 * max(amd_conf, 0.5)
                    _s_rev += pts
                    rev_reasons.append(
                        f"AMD MANIP aligned {amd_bias}+{sweep.pool.side.value} ({amd_conf:.0%})")
                elif _bsl_with_bull or _ssl_with_bear:
                    # AMD CONTRA: the sweep is occurring INTO the manipulation phase
                    # with the bias pointing THE SAME WAY as the sweep — meaning the
                    # market is being driven upward and this BSL sweep is the
                    # manipulation high before bullish delivery continues (ICT AMD
                    # cycle: accumulate → manipulate UP → distribute higher).
                    # Going short (reversal) here trades against the manipulation.
                    penalty = 18.0 * max(amd_conf, 0.5)
                    _s_rev  -= penalty
                    _s_cont += penalty * 0.6
                    rev_reasons.append(
                        f"AMD CONTRA {amd_bias} MANIP contra-reversal ({amd_conf:.0%})")
                    cont_reasons.append(
                        f"AMD CONTRA {amd_bias} MANIP supports cont ({amd_conf:.0%})")
                    logger.info(
                        f"POST_SWEEP: ⚠️  AMD CONTRA — {sweep.pool.side.value} swept "
                        f"but AMD bias={amd_bias} conf={amd_conf:.0%}: "
                        f"rev penalty={penalty:.1f} cont bonus={penalty*0.6:.1f}")

            elif amd_phase in ('DISTRIBUTION', 'REDISTRIBUTION'):
                if ((sweep.pool.side == PoolSide.BSL and amd_bias == 'bearish') or
                        (sweep.pool.side == PoolSide.SSL and amd_bias == 'bullish')):
                    pts = 25.0 * max(amd_conf, 0.5)
                    _s_cont += pts
                    cont_reasons.append(
                        f"AMD DIST {amd_bias}+{sweep.pool.side.value} ({amd_conf:.0%})")
                else:
                    _s_cont += 20.0 * max(amd_conf, 0.5)
                    _s_rev  -= 12.0 * max(amd_conf, 0.5)
                    cont_reasons.append(f"AMD DIST contra ({amd_conf:.0%})")

            elif amd_phase in ('ACCUMULATION', 'REACCUMULATION'):
                pts = 12.0 * max(amd_conf, 0.4)
                _s_rev += pts
                rev_reasons.append(f"AMD ACCUM ({amd_conf:.0%})")

            # Sweep quality / displacement  [STATIC — immutable after sweep]
            if sweep.quality >= 0.70:
                _s_rev += 12.0; rev_reasons.append(f"DISP strong ({sweep.quality:.0%})")
            elif sweep.quality >= 0.50:
                _s_rev +=  7.0; rev_reasons.append(f"DISP moderate ({sweep.quality:.0%})")
            elif sweep.quality >= 0.35:
                _s_rev +=  3.0; rev_reasons.append(f"DISP weak ({sweep.quality:.0%})")

            # Dealing range position  [STATIC — updates on minute-bar cadence]
            pd = float(getattr(ict, 'dealing_range_pd', 0.5) or 0.5)
            if sweep_dir == "short":
                if   pd >= 0.80: _s_rev  += 10.0; rev_reasons.append(f"DEEP PREMIUM ({pd:.0%})")
                elif pd >= 0.65: _s_rev  +=  7.0; rev_reasons.append(f"PREMIUM ({pd:.0%})")
                elif pd >= 0.50: _s_rev  +=  3.0; rev_reasons.append(f"SLIGHT PREM ({pd:.0%})")
                else:            _s_cont +=  6.0; cont_reasons.append("DEALING-RANGE BREAK")
            else:
                if   pd <= 0.20: _s_rev  += 10.0; rev_reasons.append(f"DEEP DISCOUNT ({pd:.0%})")
                elif pd <= 0.35: _s_rev  +=  7.0; rev_reasons.append(f"DISCOUNT ({pd:.0%})")
                elif pd <= 0.50: _s_rev  +=  3.0; rev_reasons.append(f"SLIGHT DISC ({pd:.0%})")
                else:            _s_cont +=  6.0; cont_reasons.append("DEALING-RANGE BREAK")

            # Target quality (reversal and continuation targets)  [STATIC — pool map slow-changing]
            opp_target  = self._find_opposing_target(sweep_dir, snap, price, atr, ict)
            cont_target = self._find_continuation_target(cont_dir, snap, price, atr, ict)
            if opp_target and opp_target.significance >= _MIN_POOL_SIGNIFICANCE:
                _s_rev  += min(5.0, opp_target.significance * 0.5)
                rev_reasons.append(f"TP pool sig={opp_target.significance:.1f}")
            if cont_target and cont_target.significance >= _MIN_POOL_SIGNIFICANCE:
                _s_cont += min(5.0, cont_target.significance * 0.5)
                cont_reasons.append(f"Next pool sig={cont_target.significance:.1f}")

            # OB blocking  [STATIC — OB positions update on bar cadence]
            if cont_dir == "long":
                ob_price = float(getattr(ict, 'nearest_ob_price', 0.0) or 0.0)
            else:
                ob_price = float(getattr(ict, 'nearest_ob_price_short', 0.0)
                                 or getattr(ict, 'nearest_ob_price', 0.0) or 0.0)
            if ob_price > 0:
                if cont_dir == "long" and ob_price > price:
                    ob_dist = (ob_price - price) / max(atr, 1e-10)
                    if ob_dist < 2.0:
                        _s_rev += 8.0; rev_reasons.append(f"OB blocks cont {ob_dist:.1f}ATR")
                elif cont_dir == "short" and ob_price < price:
                    ob_dist = (price - ob_price) / max(atr, 1e-10)
                    if ob_dist < 2.0:
                        _s_rev += 8.0; rev_reasons.append(f"OB blocks cont {ob_dist:.1f}ATR")

            # Session quality  [STATIC — kill-zone changes at most once per hour]
            session = (getattr(ict, 'kill_zone', '') or '').lower()
            if 'asia'     in session: _s_rev += 5.0; rev_reasons.append("ASIA manipulation")
            elif 'london' in session: _s_rev += 3.0; rev_reasons.append("LONDON Judas")
            elif 'ny'     in session: _s_rev += 2.0; rev_reasons.append("NY session")

            # Commit static baseline — never re-scored
            ps.static_rev_base  = _s_rev
            ps.static_cont_base = _s_cont
            ps.static_scored    = True
            logger.debug(
                f"POST_SWEEP static baseline: rev={_s_rev:.1f} cont={_s_cont:.1f} "
                f"AMD={amd_phase}/{amd_bias} quality={sweep.quality:.2f} "
                f"pd={getattr(ict,'dealing_range_pd',0.5):.2f}")
        else:
            # Resolve targets for decision output (needed outside static block)
            opp_target  = self._find_opposing_target(sweep_dir, snap, price, atr, ict)
            cont_target = self._find_continuation_target(cont_dir, snap, price, atr, ict)

        # ─────────────────────────────────────────────────────────────
        # DYNAMIC FACTORS — scored every tick, feed decay accumulator
        # ─────────────────────────────────────────────────────────────

        # DirectionEngine direction_hint  [DYNAMIC — injected by quant_strategy.py]
        # Bug-4 fix: DirectionEngine.evaluate_sweep() produces a verdict
        # (reverse/continue) that was previously only logged and Telegrammed —
        # it never reached the entry engine.  The two post-sweep systems ran
        # in parallel and could disagree; only entry_engine drove actual entries.
        # Fix: quant_strategy.py writes the verdict into ict_ctx.direction_hint
        # before calling entry_engine.update().  Here we consume it as a dynamic
        # factor so that a high-confidence DirectionEngine reversal verdict
        # meaningfully boosts the reversal case, and a continuation verdict boosts
        # the continuation case.  The weight is proportional to confidence so a
        # low-confidence hint adds little signal.
        _dir_hint      = (getattr(ict, 'direction_hint',            '') or '').lower()
        _dir_hint_side = (getattr(ict, 'direction_hint_side',       '') or '').lower()
        _dir_hint_conf = float(getattr(ict, 'direction_hint_confidence', 0.0) or 0.0)
        if _dir_hint and _dir_hint_conf >= 0.30:
            _hint_pts = round(20.0 * _dir_hint_conf, 1)   # max +20 pts at conf=1.0
            if _dir_hint == "reverse" and _dir_hint_side == sweep_dir:
                rev_delta  += _hint_pts
                rev_reasons.append(
                    f"DIR_ENGINE_REVERSE({_dir_hint_conf:.0%})")
            elif _dir_hint == "continue" and _dir_hint_side == cont_dir:
                cont_delta += _hint_pts
                cont_reasons.append(
                    f"DIR_ENGINE_CONTINUE({_dir_hint_conf:.0%})")

        # Live displacement from sweep level  [DYNAMIC — grows as price moves]
        if ps.max_displacement >= _PS_DISP_STRONG_ATR:
            rev_delta += 10.0
            rev_reasons.append(f"LIVE DISP {ps.max_displacement:.1f}ATR")
        elif ps.max_displacement >= _PS_DISP_MIN_ATR:
            rev_delta += 5.0
            rev_reasons.append(f"LIVE DISP moderate {ps.max_displacement:.1f}ATR")
        elif ps.max_displacement < 0.2 and elapsed > 15.0:
            cont_delta += 6.0
            cont_reasons.append(f"NO DISP ({ps.max_displacement:.2f}ATR)")

        # CISD confirmation  [DYNAMIC — event fires once, freshness decays per-tick]
        if ps.cisd_detected:
            _cisd_age = now - ps.cisd_timestamp
            _cisd_freshness = max(0.5, 1.0 - _cisd_age / 120.0)
            rev_delta += 15.0 * _cisd_freshness
            rev_reasons.append(f"CISD {ps.cisd_type.upper()} ({_cisd_freshness:.0%})")

        # OTE zone  [DYNAMIC — price can enter/exit zone between ticks]
        if ps.ote_reached:
            if ps.ote_holding:
                rev_delta += 12.0; rev_reasons.append("IN OTE ZONE")
            else:
                rev_delta +=  6.0; rev_reasons.append("OTE REACHED (exited)")

        # Order flow (instantaneous)  [DYNAMIC]
        rev_dir_needed = sweep_dir
        if flow.direction == rev_dir_needed and abs(flow.conviction) >= 0.40:
            rev_delta  += 8.0; rev_reasons.append(f"FLOW REV {flow.conviction:+.2f}")
            ps.rev_flow_ticks += 1
        elif flow.direction == rev_dir_needed:
            rev_delta  += 4.0; rev_reasons.append(f"FLOW weak rev {flow.conviction:+.2f}")
            ps.rev_flow_ticks += 1
        elif flow.direction and flow.direction != rev_dir_needed:
            cont_delta += 8.0; cont_reasons.append(f"FLOW CONT {flow.conviction:+.2f}")
            ps.cont_flow_ticks += 1

        # EWMA flow (trend)  [DYNAMIC]
        ewma_rev = (
            (rev_dir_needed == "long"  and self._flow_ewma >  0.10) or
            (rev_dir_needed == "short" and self._flow_ewma < -0.10)
        )
        ewma_cont = (
            (rev_dir_needed == "long"  and self._flow_ewma < -0.15) or
            (rev_dir_needed == "short" and self._flow_ewma >  0.15)
        )
        if ewma_rev:
            rev_delta  += 5.0; rev_reasons.append(f"EWMA rev {self._flow_ewma:+.2f}")
        elif ewma_cont:
            cont_delta += 5.0; cont_reasons.append(f"EWMA cont {self._flow_ewma:+.2f}")

        # CVD alignment  [DYNAMIC]
        cvd = flow.cvd_trend
        if   sweep_dir == "short" and cvd < -0.30: rev_delta  += 8.0; rev_reasons.append(f"CVD bearish {cvd:+.2f}")
        elif sweep_dir == "short" and cvd <    0:   rev_delta  += 3.0; rev_reasons.append(f"CVD slight bear {cvd:+.2f}")
        elif sweep_dir == "long"  and cvd >  0.30: rev_delta  += 8.0; rev_reasons.append(f"CVD bullish {cvd:+.2f}")
        elif sweep_dir == "long"  and cvd >    0:   rev_delta  += 3.0; rev_reasons.append(f"CVD slight bull {cvd:+.2f}")
        else:                                       cont_delta += 4.0; cont_reasons.append(f"CVD cont {cvd:+.2f}")

        # 5m Structure  [DYNAMIC — CHoCH/BOS events update on bar close]
        choch_5m = getattr(ict, 'choch_5m', '') or ''
        bos_5m   = getattr(ict, 'bos_5m',   '') or ''
        if choch_5m:
            choch_rev = "bearish" if rev_dir_needed == "short" else "bullish"
            if choch_5m == choch_rev:
                rev_delta += 10.0; rev_reasons.append("CHoCH (5m)")
        if bos_5m:
            cont_bos = "bullish" if cont_dir == "long" else "bearish"
            if bos_5m == cont_bos:
                cont_delta += 10.0; cont_reasons.append("BOS continuation")

        # 15m Structure reinforcement  [DYNAMIC — updates on 15m bar close]
        struct_15m = getattr(ict, 'structure_15m', '') or ''
        if struct_15m:
            _rev_15m = "bearish" if rev_dir_needed == "short" else "bullish"
            if struct_15m == _rev_15m:
                rev_delta += 6.0; rev_reasons.append(f"15m struct {struct_15m}")

        # Sustained flow consistency bonus  [DYNAMIC — by design accumulative]
        if ps.rev_flow_ticks >= 5:
            rev_delta  += 4.0; rev_reasons.append(f"SUSTAINED rev flow ({ps.rev_flow_ticks} ticks)")
        if ps.cont_flow_ticks >= 5:
            cont_delta += 4.0; cont_reasons.append(f"SUSTAINED cont flow ({ps.cont_flow_ticks} ticks)")

        # ── Accumulate DYNAMIC evidence with decay ────────────────────
        # Static baseline is already locked in ps.static_*_base.
        # Only dynamic deltas feed the decay accumulator.
        _decay = 0.92  # per-tick decay for the weaker side
        if rev_delta > 0:
            ps.rev_evidence  += rev_delta
            ps.cont_evidence *= _decay
        if cont_delta > 0:
            ps.cont_evidence += cont_delta
            ps.rev_evidence  *= _decay

        ps.peak_rev  = max(ps.peak_rev,  ps.static_rev_base  + ps.rev_evidence)
        ps.peak_cont = max(ps.peak_cont, ps.static_cont_base + ps.cont_evidence)

        # Combined totals = immutable static base + dynamic accumulated evidence
        rev_total  = round(max(ps.static_rev_base  + ps.rev_evidence,  0.0), 1)
        cont_total = round(max(ps.static_cont_base + ps.cont_evidence, 0.0), 1)
        gap        = abs(rev_total - cont_total)

        self._last_sweep_analysis = {
            "rev_score": rev_total, "cont_score": cont_total,
            "rev_reasons": rev_reasons, "cont_reasons": cont_reasons,
            "phase": phase, "cisd": ps.cisd_detected,
            "displacement_atr": ps.max_displacement, "ote": ps.ote_reached,
        }

        # ── Phase-adaptive decision thresholds ────────────────────────
        _adj_threshold = base_threshold * score_mult
        _adj_gap       = _PS_SCORE_GAP_MIN

        # Log every 10 ticks for observability
        if ps.tick_count == 1 or ps.tick_count % 10 == 0:
            logger.info(
                f"POST_SWEEP [{phase}] tick={ps.tick_count} "
                f"rev={rev_total:.0f} cont={cont_total:.0f} "
                f"(need {_adj_threshold:.0f}, gap>={_adj_gap:.0f}) "
                f"CISD={'✓' if ps.cisd_detected else '✗'} "
                f"DISP={ps.max_displacement:.1f}ATR "
                f"OTE={'✓' if ps.ote_reached else '✗'}")

        if rev_total >= _adj_threshold and gap >= _adj_gap:
            confidence  = min(1.0, rev_total / 90.0)
            # Boost confidence for structural confirmations
            if ps.cisd_detected:
                confidence = min(1.0, confidence + 0.15)
            if ps.ote_reached:
                confidence = min(1.0, confidence + 0.10)
            reason_str  = " + ".join(rev_reasons[:5])
            logger.info(
                f"🎯 SWEEP VERDICT: REVERSAL {sweep_dir.upper()} [{phase}] "
                f"(rev={rev_total:.0f} vs cont={cont_total:.0f} gap={gap:.0f}) "
                f"CISD={ps.cisd_type or 'none'} DISP={ps.max_displacement:.1f}ATR "
                f"OTE={'✓' if ps.ote_reached else '✗'}")
            return PostSweepDecision(
                action="reverse", direction=sweep_dir,
                confidence=confidence, next_target=opp_target,
                reason=(f"REVERSAL [{rev_total:.0f}v{cont_total:.0f}] "
                        f"[{phase}] {reason_str}"),
            )
        elif cont_total >= _adj_threshold * 0.9 and gap >= _adj_gap:
            confidence  = min(1.0, cont_total / 90.0)
            reason_str  = " + ".join(cont_reasons[:5])
            logger.info(
                f"🎯 SWEEP VERDICT: CONTINUATION {cont_dir.upper()} [{phase}] "
                f"(cont={cont_total:.0f} vs rev={rev_total:.0f} gap={gap:.0f})")
            return PostSweepDecision(
                action="continue", direction=cont_dir,
                confidence=confidence, next_target=cont_target,
                reason=(f"CONTINUATION [{cont_total:.0f}v{rev_total:.0f}] "
                        f"[{phase}] {reason_str}"),
            )
        else:
            return PostSweepDecision(
                action="wait", direction="", confidence=0.0,
                reason=(f"WAIT [{rev_total:.0f}v{cont_total:.0f}] "
                        f"gap={gap:.0f}<{_adj_gap:.0f} phase={phase}"),
            )

    def _handle_sweep_reversal(
        self,
        sweep:    SweepResult,
        decision: PostSweepDecision,
        snap:     LiquidityMapSnapshot,
        flow:     OrderFlowState,
        ict:      ICTContext,
        price:    float,
        atr:      float,
        now:      float,
    ) -> None:
        """
        Reversal entry handler with widened SL buffer and gated synthetic TP.

        SL: 0.35×ATR beyond sweep wick (was 0.12×ATR — death zone for retests).
        TP: Primary pool → HTF escalation → 2R synthetic (ONLY if CISD confirmed).
        """
        side = decision.direction
        ps   = self._post_sweep

        # ── SL: Sweep wick + widened buffer ──────────────────────────
        if side == "long":
            sl = sweep.wick_extreme - atr * _PS_REVERSAL_SL_BUFFER
        else:
            sl = sweep.wick_extreme + atr * _PS_REVERSAL_SL_BUFFER

        # Liquidity-aware SL push
        if self._last_liq_snapshot is not None:
            _lb = 0.25 * atr
            if side == "long":
                for t in self._last_liq_snapshot.ssl_pools:
                    if sl < t.pool.price < price:
                        sl = min(sl, t.pool.price - _lb)
            else:
                for t in self._last_liq_snapshot.bsl_pools:
                    if price < t.pool.price < sl:
                        sl = max(sl, t.pool.price + _lb)

        risk = abs(price - sl)
        if risk < 1e-10:
            self._post_sweep    = None
            self._state         = EngineState.SCANNING
            self._state_entered = now
            return

        # SL distance validation
        sl_dist_pct = risk / price
        if sl_dist_pct < 0.001 or sl_dist_pct > 0.035:
            logger.debug(f"REVERSAL: SL rejected — distance {sl_dist_pct:.3%} "
                        f"out of [0.1%, 3.5%] range")
            self._post_sweep    = None
            self._state         = EngineState.SCANNING
            self._state_entered = now
            return

        # Record sweep direction BEFORE any R:R gate
        self._last_sweep_reversal_dir  = side
        self._last_sweep_reversal_time = now

        # ── TP: Pool target → HTF escalation → gated synthetic ───────
        _signal_target = decision.next_target
        tp = None
        if decision.next_target:
            _pool_tp = decision.next_target.pool.price
            _buf = atr * _TP_BUFFER_ATR
            _pool_tp = _pool_tp - _buf if side == "long" else _pool_tp + _buf
            if abs(_pool_tp - price) / risk >= _MIN_RR_RATIO:
                tp = _pool_tp

        if tp is None:
            _htf_tp, _htf_target = self._find_htf_tp(
                snap, side, price, atr, sl, _MIN_RR_RATIO)
            if _htf_tp is not None:
                tp             = _htf_tp
                _signal_target = _htf_target
                logger.info(f"Sweep reversal HTF TP: [{_htf_target.pool.timeframe}] "
                            f"${_htf_target.pool.price:,.1f}")

        # Synthetic 2R TP: ONLY if CISD confirmed (prevents structureless trades)
        _cisd_ok = ps is not None and ps.cisd_detected
        if tp is None and _cisd_ok:
            tp = price + risk * 2.0 if side == "long" else price - risk * 2.0
            logger.info(f"REVERSAL: using 2R synthetic TP (CISD confirmed)")
        elif tp is None:
            logger.info(f"REVERSAL: no TP available and no CISD — aborting")
            self._post_sweep    = None
            self._state         = EngineState.SCANNING
            self._state_entered = now
            return

        reward = abs(tp - price)
        rr     = reward / risk
        if rr < _MIN_RR_RATIO:
            self._post_sweep    = None
            self._state         = EngineState.SCANNING
            self._state_entered = now
            return

        # ── Build entry signal ────────────────────────────────────────
        _disp_info = f" DISP={ps.max_displacement:.1f}ATR" if ps else ""
        _cisd_info = f" CISD={ps.cisd_type}" if ps and ps.cisd_detected else ""
        _ote_info  = " OTE=✓" if ps and ps.ote_reached else ""

        self._signal = EntrySignal(
            side=side,
            entry_type=EntryType.SWEEP_REVERSAL,
            entry_price=price, sl_price=sl, tp_price=tp, rr_ratio=rr,
            target_pool=_signal_target or PoolTarget(
                pool=sweep.pool, distance_atr=0, direction=side,
                significance=getattr(sweep.pool, 'significance', 3.0),
                tf_sources=[]),
            sweep_result=sweep, conviction=decision.confidence,
            reason=f"{decision.reason}{_cisd_info}{_disp_info}{_ote_info}",
            ict_validation=self._ict_summary(ict, side),
        )
        logger.info(f"🎯 SIGNAL: REVERSAL {side.upper()} | "
                    f"SL=${sl:,.1f} TP=${tp:,.1f} R:R={rr:.1f}"
                    f"{_cisd_info}{_disp_info}{_ote_info}")
        self._post_sweep = None

    def _handle_sweep_continuation(
        self,
        sweep:    SweepResult,
        decision: PostSweepDecision,
        snap:     LiquidityMapSnapshot,
        flow:     OrderFlowState,
        ict:      ICTContext,
        price:    float,
        atr:      float,
        now:      float,
    ) -> None:
        """
        Continuation entry handler with fixed SL placement.

        SL: 0.40×ATR BEYOND the swept pool (was AT the pool where stops
        already triggered — the worst possible SL placement for continuations).
        """
        side        = decision.direction
        next_target = decision.next_target
        if next_target is None:
            self._post_sweep    = None
            self._state         = EngineState.SCANNING
            self._state_entered = now
            return

        # ── SL: BEYOND the swept pool, not AT it ─────────────────────
        if side == "long":
            # Continuation long after SSL sweep → SL below the swept SSL pool
            sl = sweep.pool.price - atr * _PS_CONTINUATION_SL_BUFFER
        else:
            # Continuation short after BSL sweep → SL above the swept BSL pool
            sl = sweep.pool.price + atr * _PS_CONTINUATION_SL_BUFFER

        # Liquidity-aware SL push
        if self._last_liq_snapshot is not None:
            _lb = 0.25 * atr
            if side == "long":
                for t in self._last_liq_snapshot.ssl_pools:
                    if sl < t.pool.price < price:
                        sl = min(sl, t.pool.price - _lb)
            else:
                for t in self._last_liq_snapshot.bsl_pools:
                    if price < t.pool.price < sl:
                        sl = max(sl, t.pool.price + _lb)

        tp = (next_target.pool.price - atr * 0.35 if side == "long"
              else next_target.pool.price + atr * 0.35)

        risk   = abs(price - sl)
        reward = abs(tp - price)
        if risk < 1e-10:
            self._post_sweep    = None
            self._state         = EngineState.SCANNING
            self._state_entered = now
            return

        # SL distance validation
        sl_dist_pct = risk / price
        if sl_dist_pct < 0.001 or sl_dist_pct > 0.035:
            logger.debug(f"CONTINUATION: SL rejected — distance {sl_dist_pct:.3%}")
            self._post_sweep    = None
            self._state         = EngineState.SCANNING
            self._state_entered = now
            return

        rr = reward / risk
        if rr < _MIN_RR_RATIO:
            # Try HTF escalation for continuation
            _htf_tp, _htf_target = self._find_htf_tp(
                snap, side, price, atr, sl, _MIN_RR_RATIO)
            if _htf_tp is not None:
                tp = _htf_tp
                next_target = _htf_target
                reward = abs(tp - price)
                rr = reward / risk
            else:
                self._post_sweep    = None
                self._state         = EngineState.SCANNING
                self._state_entered = now
                return

        if rr < _MIN_RR_RATIO:
            self._post_sweep    = None
            self._state         = EngineState.SCANNING
            self._state_entered = now
            return

        self._signal = EntrySignal(
            side=side,
            entry_type=EntryType.SWEEP_CONTINUATION,
            entry_price=price, sl_price=sl, tp_price=tp, rr_ratio=rr,
            target_pool=next_target, sweep_result=sweep,
            conviction=decision.confidence, reason=decision.reason,
            ict_validation=self._ict_summary(ict, side),
        )
        logger.info(f"🎯 SIGNAL: CONTINUATION {side.upper()} -> "
                    f"${next_target.pool.price:,.1f} R:R={rr:.1f}")
        self._post_sweep = None

    # ── Selectors ─────────────────────────────────────────────────────────

    def _find_flow_target(
        self,
        snap: LiquidityMapSnapshot,
        flow: OrderFlowState,
        price: float,
        atr:   float,
    ) -> Optional[PoolTarget]:
        """BUG-FIX-2: Uses t.adjusted_sig() so adjacency bonus is visible."""
        if flow.direction == "long":
            candidates = [t for t in snap.bsl_pools
                          if _MIN_TARGET_ATR <= t.distance_atr <= _MAX_TARGET_ATR
                          and t.significance >= _MIN_POOL_SIGNIFICANCE]
        elif flow.direction == "short":
            candidates = [t for t in snap.ssl_pools
                          if _MIN_TARGET_ATR <= t.distance_atr <= _MAX_TARGET_ATR
                          and t.significance >= _MIN_POOL_SIGNIFICANCE]
        else:
            return None
        if not candidates:
            return None
        return max(candidates, key=lambda t: t.adjusted_sig())

    def _find_opposing_target(
        self,
        direction: str,
        snap:      LiquidityMapSnapshot,
        price:     float,
        atr:       float,
        ict:       Optional[ICTContext] = None,
    ) -> Optional[PoolTarget]:
        """BUG-FIX-2: Uses t.adjusted_sig()."""
        pool_list = snap.bsl_pools if direction == "long" else snap.ssl_pools
        reachable = [t for t in pool_list
                     if t.distance_atr <= _MAX_TARGET_ATR
                     and t.significance >= _MIN_POOL_SIGNIFICANCE * 0.5]
        if not reachable:
            return None
        amd_phase = (getattr(ict, 'amd_phase', '') or '').upper() if ict else ''
        if amd_phase in ('DISTRIBUTION', 'REDISTRIBUTION'):
            return min(reachable, key=lambda t: t.distance_atr)
        return max(reachable, key=lambda t: t.adjusted_sig())

    def _find_continuation_target(
        self,
        direction: str,
        snap:      LiquidityMapSnapshot,
        price:     float,
        atr:       float,
        ict:       Optional[ICTContext] = None,
    ) -> Optional[PoolTarget]:
        return self._find_opposing_target(direction, snap, price, atr, ict)

    # ── SL / TP computation ───────────────────────────────────────────────

    def _compute_sl(
        self,
        ict:   ICTContext,
        side:  str,
        price: float,
        atr:   float,
    ) -> Optional[float]:
        """
        BUG-FIX-4: Separate log paths for:
          (a) no ICT OB found at all
          (b) OB exists but too close (<0.1% from price)
          (c) OB exists but too far  (>3.5% from price)

        BUG-FIX-CRITICAL (v3.2): Use side-specific OB price.
          Old: always read nearest_ob_price (long-side OB below price).
          For shorts, this OB is BELOW price so `ob > price` was always False
          → _compute_sl returned None for EVERY short → shorts never fired.
          Fix: read nearest_ob_price_short (OB above price) for short side.

        BUG-FIX-FALLBACK (v3.2): ATR-based fallback SL when no OB is available.
          Old: returned None when no OB was found → entry killed.
          Fix: use 1.5×ATR default SL distance with liquidity-pool push.
          This ensures entries are never blocked solely by missing OB data.
        """
        sl = None
        _rejected_reason = ""

        # Select the correct OB for this side
        if side == "long":
            ob_price = ict.nearest_ob_price
        else:
            ob_price = getattr(ict, 'nearest_ob_price_short', 0.0) or 0.0

        if ob_price > 0:
            if side == "long" and ob_price < price:
                sl = ob_price - atr * 0.20
            elif side == "short" and ob_price > price:
                sl = ob_price + atr * 0.20

        # Liquidity-aware SL push
        if sl is not None and self._last_liq_snapshot is not None:
            snap = self._last_liq_snapshot
            _lb  = 0.25 * atr
            if side == "long":
                for t in snap.ssl_pools:
                    if sl < t.pool.price < price:
                        sl = min(sl, t.pool.price - _lb)
            else:
                for t in snap.bsl_pools:
                    if price < t.pool.price < sl:
                        sl = max(sl, t.pool.price + _lb)

        if sl is not None:
            dist_pct = abs(price - sl) / price
            if dist_pct < 0.001:
                _rejected_reason = (f"OB@${ob_price:.1f} too close "
                                    f"({dist_pct:.3%} < 0.1%)")
                sl = None
            elif dist_pct > 0.035:
                _rejected_reason = (f"OB@${ob_price:.1f} too far "
                                    f"({dist_pct:.3%} > 3.5%)")
                sl = None

        # ── BUG-FIX-FALLBACK: ATR-based fallback SL ──────────────────────
        # When no OB is available for this side, use 1.5×ATR as default SL
        # distance. This prevents entries from being permanently blocked
        # by missing OB data (which was killing ALL short entries).
        if sl is None:
            _fallback_dist = 1.5 * atr
            if side == "long":
                sl = price - _fallback_dist
            else:
                sl = price + _fallback_dist

            # Apply liquidity-aware push on fallback SL too
            if self._last_liq_snapshot is not None:
                snap = self._last_liq_snapshot
                _lb  = 0.25 * atr
                if side == "long":
                    for t in snap.ssl_pools:
                        if sl < t.pool.price < price:
                            sl = min(sl, t.pool.price - _lb)
                else:
                    for t in snap.bsl_pools:
                        if price < t.pool.price < sl:
                            sl = max(sl, t.pool.price + _lb)

            # Validate fallback SL distance
            dist_pct = abs(price - sl) / price
            if dist_pct < 0.001 or dist_pct > 0.035:
                if ob_price > 0 and _rejected_reason:
                    logger.debug(f"SL rejected for {side.upper()}: {_rejected_reason}, "
                                 f"fallback also out of range ({dist_pct:.3%})")
                else:
                    logger.debug(f"SL fallback for {side.upper()}: distance {dist_pct:.3%} "
                                 f"out of [0.1%, 3.5%] range")
                return None

            logger.debug(f"SL for {side.upper()}: using ATR fallback "
                         f"${sl:,.1f} (1.5×ATR={_fallback_dist:.1f}pts)"
                         f"{f' [OB rejected: {_rejected_reason}]' if _rejected_reason else ' [no OB found]'}")

        return sl

    def _compute_tp_approach(
        self,
        target: PoolTarget,
        side:   str,
        price:  float,
        atr:    float,
    ) -> Optional[float]:
        """
        Front-run the liquidity pool with a distance-scaled buffer.
        buffer = 0.10*ATR + 0.05*dist_atr*ATR, capped at 0.50*ATR.
        At 1 ATR: 0.15*ATR | At 3 ATR: 0.25*ATR | At 8+ ATR: 0.50*ATR
        """
        buffer = atr * min(0.50, 0.10 + 0.05 * target.distance_atr)
        tp     = (target.pool.price - buffer if side == "long"
                  else target.pool.price + buffer)
        if side == "long"  and tp <= price:
            return None
        if side == "short" and tp >= price:
            return None
        return tp

    def _ict_structure_agrees(self, ict: ICTContext, direction: str) -> bool:
        agreements = 0
        if (direction == "long"  and ict.amd_bias  == "bullish"): agreements += 1
        if (direction == "short" and ict.amd_bias  == "bearish"): agreements += 1
        if (direction == "long"  and ict.in_discount):             agreements += 1
        if (direction == "short" and ict.in_premium):              agreements += 1
        if (direction == "long"  and ict.structure_5m == "bullish"): agreements += 1
        if (direction == "short" and ict.structure_5m == "bearish"): agreements += 1
        if ict.session_quality == "prime":                         agreements += 1
        return agreements >= 2

    @staticmethod
    def _ict_summary(ict: ICTContext, side: str) -> str:
        parts = []
        if ict.amd_phase: parts.append(f"AMD={ict.amd_phase}")
        if ict.amd_bias:  parts.append(f"bias={ict.amd_bias}")
        if ict.in_discount: parts.append("DISCOUNT")
        elif ict.in_premium: parts.append("PREMIUM")
        if ict.structure_5m: parts.append(f"5m={ict.structure_5m}")
        if ict.kill_zone:    parts.append(f"KZ={ict.kill_zone}")
        # Show which OB is being used for SL
        if side == "long" and ict.nearest_ob_price > 0:
            parts.append(f"OB_L=${ict.nearest_ob_price:,.0f}")
        elif side == "short":
            ob_short = getattr(ict, 'nearest_ob_price_short', 0.0) or 0.0
            if ob_short > 0:
                parts.append(f"OB_S=${ob_short:,.0f}")
            else:
                parts.append("OB_S=ATR_fallback")
        return " | ".join(parts) if parts else "no ICT context"


# ═══════════════════════════════════════════════════════════════════════════
# TRAIL MANAGER
# ═══════════════════════════════════════════════════════════════════════════

class ICTTrailManager:
    """Trailing stop management using ICT structures only."""

    def __init__(self) -> None:
        self._current_sl:  float = 0.0
        self._entry_price: float = 0.0
        self._side:        str   = ""
        self._bos_count:   int   = 0
        self._choch_seen:  bool  = False

    def initialize(self, side: str, entry_price: float, initial_sl: float) -> None:
        self._side        = side
        self._entry_price = entry_price
        self._current_sl  = initial_sl
        self._bos_count   = 0
        self._choch_seen  = False

    def compute(
        self,
        ict_ctx:     ICTContext,
        price:       float,
        atr:         float,
        candles_5m:  List[Dict],
        candles_15m: Optional[List[Dict]] = None,
    ) -> Optional[float]:
        if not self._side or atr < 1e-10:
            return None

        if ict_ctx.bos_5m:
            expected = "bullish" if self._side == "long" else "bearish"
            if ict_ctx.bos_5m == expected:
                self._bos_count += 1

        if ict_ctx.choch_5m:
            against = "bearish" if self._side == "long" else "bullish"
            if ict_ctx.choch_5m == against:
                self._choch_seen = True

        new_sl = self._find_structural_sl(candles_5m, candles_15m, atr)
        if new_sl is None:
            return None

        if self._side == "long"  and new_sl <= self._current_sl:
            return None
        if self._side == "short" and new_sl >= self._current_sl:
            return None

        self._current_sl = new_sl
        return new_sl

    def _find_structural_sl(
        self,
        candles_5m:  List[Dict],
        candles_15m: Optional[List[Dict]],
        atr:         float,
    ) -> Optional[float]:
        try:
            from strategy.liquidity_map import _find_swing_highs, _find_swing_lows
        except ImportError:
            from liquidity_map import _find_swing_highs, _find_swing_lows

        if self._choch_seen:
            buffer_mult = 0.05
        elif self._bos_count >= 2:
            buffer_mult = 0.10
        else:
            buffer_mult = _SL_BUFFER_ATR

        buffer = atr * buffer_mult

        if candles_15m and len(candles_15m) >= 12:
            if self._side == "long":
                lows = _find_swing_lows(candles_15m, lookback=3)
                if lows: return lows[-1][1] - buffer
            else:
                highs = _find_swing_highs(candles_15m, lookback=3)
                if highs: return highs[-1][1] + buffer

        if candles_5m and len(candles_5m) >= 12:
            if self._side == "long":
                lows = _find_swing_lows(candles_5m, lookback=4)
                if lows: return lows[-1][1] - buffer
            else:
                highs = _find_swing_highs(candles_5m, lookback=4)
                if highs: return highs[-1][1] + buffer

        return None

    @property
    def current_sl(self) -> float:
        return self._current_sl

    @property
    def phase_info(self) -> str:
        parts = [f"BOS×{self._bos_count}"]
        if self._choch_seen:
            parts.append("CHoCH")
        return " ".join(parts)

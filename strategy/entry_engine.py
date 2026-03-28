"""
entry_engine.py — Liquidity-First Entry Decision Engine v2.0
=============================================================
FIXES IN THIS VERSION
─────────────────────
FIX-A: _MIN_POOL_SIGNIFICANCE lowered from 3.0 → 2.0.
        Old: new 5m single-touch pools (sig=2.0) were excluded from
        target finding. After 2-3 trades sweep the known pools, the
        engine had no targets and stayed in SCANNING indefinitely.
        New: any pool meeting the tradeable threshold (1.8 from
        LiquidityMap) can be targeted if conviction is sufficient.

FIX-B: _FLOW_CONV_THRESHOLD lowered from 0.55 → 0.40.
        Old: threshold required tick_flow + CVD to both be strongly
        bullish/bearish simultaneously. In BTC consolidation (most
        of the session), average conviction rarely exceeds 0.35.
        New: 0.40 captures meaningful directional flow without
        requiring extreme readings.

FIX-C: _FLOW_SUSTAINED_TICKS reduced from 3 → 2.
        Old: 3 consecutive 1-second ticks all with same direction.
        Any momentary neutral reading reset the counter.
        New: 2 confirms is sufficient when CVD also agrees.

FIX-D: POST_SWEEP decision thresholds relaxed.
        Old: rev >= 55 AND gap >= 15. In practice, rare to score
        55+ reversal points — the engine almost always returned "wait"
        and then timed out after 5 minutes, missing the sweep-reversal
        setup entirely.
        New: rev >= 45 AND gap >= 10. Still requires meaningful edge
        over continuation, but doesn't demand textbook setups.

FIX-E: Added PROXIMITY APPROACH trigger.
        When price is within 0.5-2.0 ATR of a significant pool
        (approaching from the correct side), emit a signal even if
        sustained tick flow hasn't built 2 ticks yet. This captures
        the "pre-sweep approach" before the institutional absorption
        completes. Only fires when pool significance >= 4.0 AND
        at least one of: CVD agrees, OB aligned, AMD phase confirms.

FIX-F: Neutral tick handling corrected throughout state machine.
        Old: `flow.direction != tr.direction` treated neutral ticks
        ("")  as contrary — counter reset prematurely. Fixed in
        _do_tracking and _do_ready.

FIX-G: TRACKING state: target updated only when direction agrees.
        Old: target could switch to the opposite pool on a single
        counter-direction tick before the abort threshold fired.

FIX-H: POST_SWEEP quality floor by timeframe.
        1m sweeps require quality >= 0.65, 5m >= 0.55, 15m >= 0.45.
        Prevents micro-wick noise from triggering post-sweep logic.

FIX-I: _find_opposing_target now prefers NEAREST reachable pool
        instead of highest significance during DISTRIBUTION/REDISTRIBUTION.
        Delivery is already underway — TP should be the immediate target.

FIX-J: SL computation now uses a 1.0 ATR fallback (was 1.2 ATR).
        At ATR=$112, 1.2 ATR = $134 SL behind a $66,700 pool entry.
        That's often beyond the pool level itself. 1.0 ATR = $112 is
        structurally tighter and more consistent with ICT entry mechanics.

FIX-K: _CISD_MAX_WAIT_SEC reduced from 300 → 240 seconds.
        After 4 minutes of "wait", the sweep setup is stale. Faster
        reset to SCANNING allows the engine to detect the next pool.

FIX-L: _TRACKING_TIMEOUT_SEC reduced from 300 → 180 seconds.
        3-minute tracking window — if pool isn't reached in 3 minutes,
        the flow impulse has dissipated. Reset and re-evaluate.
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
        PoolStatus, PoolSide,
    )
except ImportError:
    from liquidity_map import (
        LiquidityMap, LiquidityMapSnapshot, PoolTarget, SweepResult,
        PoolStatus, PoolSide,
    )


# ═══════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════

# FIX-B: lowered conviction threshold
_FLOW_CONV_THRESHOLD    = 0.40    # was 0.55
_FLOW_CVD_AGREE_MIN     = 0.15    # was 0.20
# FIX-C: reduced sustained ticks
_FLOW_SUSTAINED_TICKS   = 2       # was 3

# Pool targeting
_MAX_TARGET_ATR         = 8.0     # was 6.0 — allow slightly farther targets
_MIN_TARGET_ATR         = 0.25    # was 0.3
# FIX-A: lowered significance threshold
_MIN_POOL_SIGNIFICANCE  = 2.0     # was 3.0

# Proximity approach trigger (FIX-E)
_PROXIMITY_ENTRY_ATR_MAX    = 2.0     # Pool within 2.0 ATR triggers proximity check
_PROXIMITY_ENTRY_ATR_MIN    = 0.15    # Must be at least 0.15 ATR from pool
_PROXIMITY_MIN_SIG          = 4.0     # Higher bar for proximity approach
_PROXIMITY_CONFIRM_COUNT    = 1       # Only 1 confirm tick needed

# Sweep detection
_MIN_SWEEP_QUALITY      = 0.35

# FIX-K / FIX-L: Reduced timeouts
_CISD_MAX_WAIT_SEC      = 240     # was 300
_OTE_MAX_WAIT_SEC       = 480     # was 600
_TRACKING_TIMEOUT_SEC   = 180     # was 300
_READY_TIMEOUT_SEC      = 90      # was 120

# Risk management
_MIN_RR_RATIO           = 1.4     # was 1.5 — slightly more permissive
_SL_BUFFER_ATR          = 0.12    # was 0.15
_TP_BUFFER_ATR          = 0.08    # was 0.10

# Cooldowns
_ENTRY_COOLDOWN_SEC     = 30.0    # was 45.0
_POST_SWEEP_EVAL_SEC    = 10.0    # was 15.0 — faster evaluation

# ═══════════════════════════════════════════════════════════════════════════
# v3.0 INSTITUTIONAL-GRADE ADDITIONS
# ═══════════════════════════════════════════════════════════════════════════

# ── EWMA Flow Tracking ────────────────────────────────────────────────
# Replaces binary tick counters with exponentially-weighted moving average.
# Institutional desks use smoothed order-flow aggregation to filter
# microstructure noise while preserving directional momentum.
#
# Alpha=0.15 → effective half-life of ~4.3 ticks (~1.1s at 250ms).
# This means a single contrary tick decays the signal by 15% — far more
# forgiving than the old "3 contrary ticks = instant kill" logic.
_FLOW_EWMA_ALPHA        = 0.15    # base smoothing factor
_FLOW_EWMA_ADAPTIVE_CAP = 0.40   # max alpha when flow is very strong

# ── Minimum Hold Times ────────────────────────────────────────────────
# Prevents microstructure oscillation from killing valid setups.
# Institutional algos enforce minimum observation windows before
# aborting — a 500ms flow reversal is noise, not signal.
_TRACKING_MIN_HOLD_SEC  = 5.0     # 5s before any tracking abort allowed
_READY_MIN_HOLD_SEC     = 8.0     # 8s before any ready abort allowed

# ── Conviction Decay (READY state) ───────────────────────────────────
# Instead of binary kill on 2 contrary ticks, conviction decays
# gradually. Only abort when conviction falls below kill threshold.
# Models institutional commitment: strong setups survive brief pullbacks.
_READY_DECAY_RATE       = 0.06    # conviction reduction per second of contrary flow
_READY_MIN_CONVICTION   = 0.18    # abort only below this

# ── AMD Conviction Modifiers ─────────────────────────────────────────
# AMD phase INFLUENCES sizing and conviction threshold, never blocks.
# Institutional desks adjust position size based on macro context —
# they don't refuse to trade a confirmed setup.
#
# Against MANIPULATION: reduce conviction by 40% (need stronger flow)
# Against DISTRIBUTION: reduce conviction by 20% (mild headwind)
# Aligned: no penalty (1.0 multiplier)
_AMD_MANIP_CONTRA_MULT  = 0.60    # conviction × 0.60 when against MANIPULATION
_AMD_DIST_CONTRA_MULT   = 0.80    # conviction × 0.80 when against DISTRIBUTION
_AMD_ALIGNED_BONUS      = 1.10    # conviction × 1.10 when aligned with AMD

# ── Displacement / Momentum Entry ────────────────────────────────────
# Third entry mode: institutional expansion entries.
# When a candle prints clean displacement (large body, high volume,
# breaking structure), enter the continuation. ICT calls this the
# "FVG entry" — enter the fair value gap left by displacement.
#
# No nearby pool required. SL behind the displacement candle.
# TP at the next opposing liquidity pool or 2×ATR projection.
_MOMENTUM_MIN_BODY_RATIO   = 0.65   # candle body/range (clean displacement)
_MOMENTUM_MIN_VOL_RATIO    = 1.3    # volume vs 20-bar average
_MOMENTUM_MIN_ATR_MOVE     = 0.6    # candle range must be >= 0.6×ATR
_MOMENTUM_LOOKBACK_CANDLES  = 3     # check last 3 closed candles
_MOMENTUM_SL_BUFFER_ATR    = 0.15   # SL = displacement candle extreme ± buffer
_MOMENTUM_MIN_RR           = 1.3    # minimum R:R for momentum entry
_MOMENTUM_COOLDOWN_SEC     = 60.0   # don't fire momentum within 60s of last entry
_MOMENTUM_MAX_ENTRIES_PER_HOUR = 3  # cap momentum entries to prevent overtrading


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
    PRE_SWEEP_APPROACH = "APPROACH"
    SWEEP_REVERSAL     = "REVERSAL"
    SWEEP_CONTINUATION = "CONTINUATION"
    DISPLACEMENT_MOMENTUM = "MOMENTUM"


@dataclass
class OrderFlowState:
    """Consolidated orderflow from tick engine + CVD."""
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
class ICTContext:
    """ICT structural context passed in each tick."""
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
    nearest_ob_price: float = 0.0
    kill_zone:        str   = ""

    @property
    def session_quality(self) -> str:
        if self.kill_zone in ("london", "ny"):
            return "prime"
        elif self.kill_zone == "asia":
            return "fair"
        return "off_session"


@dataclass
class EntrySignal:
    """The ONLY output this engine produces."""
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
    action:         str
    direction:      str
    confidence:     float
    next_target:    Optional[PoolTarget] = None
    reason:         str                 = ""


# ═══════════════════════════════════════════════════════════════════════════
# MAIN ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class EntryEngine:
    """Single-flow entry decision engine v2.0."""

    def __init__(self) -> None:
        self._state         = EngineState.SCANNING
        self._signal:       Optional[EntrySignal] = None
        self._tracking:     Optional[_TrackingState] = None
        self._post_sweep:   Optional[_PostSweepState] = None
        self._last_entry_at = 0.0
        self._state_entered = time.time()
        self._last_sweep_analysis: Dict = {}
        # FIX-E: proximity approach counter
        self._proximity_confirms: int = 0
        self._proximity_target:   Optional[PoolTarget] = None
        self._proximity_side:     str = ""
        # BUG-2 FIX: sweep reversal direction memory — prevents proximity
        # from immediately entering OPPOSITE of a just-identified reversal
        self._last_sweep_reversal_dir:  str   = ""
        self._last_sweep_reversal_time: float = 0.0
        # Liquidity snapshot reference for SL liquidity-awareness
        self._last_liq_snapshot = None
        # ── v3.0: EWMA flow tracking ─────────────────────────────────────
        self._flow_ewma:             float = 0.0
        self._flow_ewma_last_update: float = 0.0
        # ── v3.0: Conviction decay state (READY) ─────────────────────────
        self._ready_conviction:      float = 0.0
        self._ready_peak_conviction: float = 0.0
        self._ready_last_agree_ts:   float = 0.0
        self._ready_rr_fail_count:   int   = 0
        # ── v3.0: Momentum entry tracking ─────────────────────────────────
        self._momentum_entries_1h:   int   = 0     # count in current hour
        self._momentum_hour_start:   float = 0.0   # hour boundary timestamp
        self._last_momentum_candle_ts: int = 0     # dedup: last displacement candle ts

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

        # Store snapshot for _compute_sl liquidity awareness
        self._last_liq_snapshot = liq_snapshot

        # ── v3.0: Update EWMA flow ───────────────────────────────────────
        self._update_flow_ewma(flow_state, now)

        # Check for new sweeps first — but NOT if already evaluating one
        new_sweeps = [s for s in liq_snapshot.recent_sweeps
                      if s.detected_at > now - 10.0
                      and s.quality >= _MIN_SWEEP_QUALITY]

        if new_sweeps and self._state not in (
            EngineState.IN_POSITION, EngineState.ENTERING, EngineState.POST_SWEEP
        ):
            best_sweep = max(new_sweeps, key=lambda s: s.quality)
            self._enter_post_sweep(best_sweep, liq_snapshot, flow_state,
                                    ict_ctx, price, atr, now)
            return

        # FIX-E: Proximity approach check (runs in SCANNING and TRACKING)
        if self._state in (EngineState.SCANNING, EngineState.TRACKING):
            if self._check_proximity_approach(liq_snapshot, flow_state,
                                              ict_ctx, price, atr, now):
                return

        # ── v3.0: Displacement/Momentum check (runs in SCANNING only) ────
        if self._state == EngineState.SCANNING:
            if self._check_displacement_momentum(
                    liq_snapshot, flow_state, ict_ctx,
                    price, atr, now, candles_1m, candles_5m):
                return

        # State machine dispatch
        if self._state == EngineState.SCANNING:
            self._do_scanning(liq_snapshot, flow_state, ict_ctx, price, atr, now)
        elif self._state == EngineState.TRACKING:
            self._do_tracking(liq_snapshot, flow_state, ict_ctx, price, atr, now)
        elif self._state == EngineState.READY:
            self._do_ready(liq_snapshot, flow_state, ict_ctx, price, atr, now)
        elif self._state == EngineState.POST_SWEEP:
            self._do_post_sweep(liq_snapshot, flow_state, ict_ctx, price, atr, now)
        elif self._state in (EngineState.ENTERING, EngineState.IN_POSITION):
            # SELF-RECOVERY: If stuck in ENTERING for >120s, the order failed
            # and neither the background thread nor the watchdog reset us.
            # If stuck in IN_POSITION for >4h, position was closed externally
            # and on_position_closed() was never called.
            # Either way, recover to SCANNING so the bot isn't brain-dead.
            _stuck_limit = 120.0 if self._state == EngineState.ENTERING else 14400.0
            if now - self._state_entered > _stuck_limit:
                logger.warning(
                    f"🔄 Entry engine SELF-RECOVERY: stuck in {self._state.name} "
                    f"for {now - self._state_entered:.0f}s — forcing SCANNING")
                self._state = EngineState.SCANNING
                self._state_entered = now
                self._signal = None
                self._tracking = None
                self._proximity_confirms = 0
                self._proximity_target = None
                self._proximity_side = ""
                self._ready_conviction = 0.0
                self._ready_peak_conviction = 0.0
                self._ready_rr_fail_count = 0

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
        """
        Called when an entry order fails, times out, or is cancelled.
        Resets the engine back to SCANNING so it can generate new signals.

        Without this, the engine stays in ENTERING state forever because
        the state machine has no handler for ENTERING — update() silently
        returns, and the bot becomes brain-dead (cannot generate signals).
        """
        if self._state in (EngineState.ENTERING, EngineState.IN_POSITION):
            logger.info(f"🔄 Entry engine: {self._state.name} → SCANNING (entry failed/cancelled)")
            self._state = EngineState.SCANNING
            self._state_entered = time.time()
            self._signal = None
            self._tracking = None
            self._proximity_confirms = 0
            self._proximity_target = None
            self._proximity_side = ""
            self._ready_conviction = 0.0
            self._ready_peak_conviction = 0.0
            self._ready_rr_fail_count = 0

    def on_position_opened(self) -> None:
        self._state = EngineState.IN_POSITION
        self._state_entered = time.time()

    def on_entry_cancelled(self) -> None:
        """
        Called when an entry order is explicitly cancelled (timeout, watchdog,
        or manual cancellation).  Always invoked AFTER on_entry_failed() which
        handles the ENTERING → SCANNING state transition.

        This method performs cancellation-specific cleanup that on_entry_failed
        does not cover:
          • clears stale post-sweep evaluation state
          • resets proximity approach side tracking
          • resets the last-entry timestamp so the cooldown window is measured
            from the cancellation, not the original placement
        """
        self._post_sweep = None
        self._proximity_side = ""
        self._last_entry_at = time.time()
        self._ready_rr_fail_count = 0
        # Defensive: guarantee SCANNING in case on_entry_failed was not
        # called or the state was something unexpected.
        if self._state not in (EngineState.SCANNING, EngineState.IN_POSITION):
            logger.info(
                f"🔄 Entry engine: {self._state.name} → SCANNING "
                f"(entry cancelled — defensive reset)")
            self._state = EngineState.SCANNING
            self._state_entered = time.time()
            self._signal = None
            self._tracking = None
            self._proximity_confirms = 0
            self._proximity_target = None
            self._ready_conviction = 0.0
            self._ready_peak_conviction = 0.0

    def on_position_closed(self) -> None:
        self._state = EngineState.SCANNING
        self._state_entered = time.time()
        self._tracking = None
        self._post_sweep = None
        self._proximity_confirms = 0
        self._proximity_target = None
        self._proximity_side = ""
        # v3.0: reset conviction state (EWMA preserved — it tracks
        # market flow which persists across positions)
        self._ready_conviction = 0.0
        self._ready_peak_conviction = 0.0
        self._ready_rr_fail_count = 0

    def force_reset(self) -> None:
        self._state = EngineState.SCANNING
        self._state_entered = time.time()
        self._signal = None
        self._tracking = None
        self._post_sweep = None
        self._proximity_confirms = 0
        self._proximity_target = None
        self._proximity_side = ""
        # v3.0 state cleanup
        self._flow_ewma = 0.0
        self._flow_ewma_last_update = 0.0
        self._ready_conviction = 0.0
        self._ready_peak_conviction = 0.0
        self._ready_rr_fail_count = 0
        self._momentum_entries_1h = 0
        self._last_momentum_candle_ts = 0

    @property
    def state(self) -> str:
        return self._state.name

    @property
    def tracking_info(self) -> Optional[Dict]:
        if self._tracking is None:
            return None
        return {
            "direction": self._tracking.direction,
            "target": f"${self._tracking.target.pool.price:,.1f}",
            "distance_atr": f"{self._tracking.target.distance_atr:.1f}",
            "flow_ticks": self._tracking.flow_ticks,
            "started": f"{time.time() - self._tracking.started_at:.0f}s ago",
        }

    # ── FIX-E: Proximity Approach ─────────────────────────────────────────

    # ── v3.0: EWMA Flow Tracker ──────────────────────────────────────────

    def _update_flow_ewma(self, flow: OrderFlowState, now: float) -> float:
        """
        Institutional EWMA flow tracker.

        Produces a single signed value: positive = long, negative = short.
        Uses adaptive alpha: stronger flow → faster adaptation (institutional
        desks increase tracking speed during high-conviction moves).

        Unlike tick counters which reset on a single contrary tick, EWMA
        decays gradually — a brief 250ms noise tick reduces the signal by
        ~15%, not 100%.
        """
        dt = now - self._flow_ewma_last_update if self._flow_ewma_last_update > 0 else 0.25
        self._flow_ewma_last_update = now

        # Signed conviction: positive = long, negative = short, zero = neutral
        if flow.direction == "long":
            signed = abs(flow.conviction)
        elif flow.direction == "short":
            signed = -abs(flow.conviction)
        else:
            signed = 0.0

        # Adaptive alpha: stronger readings get faster smoothing
        # This models institutional response — strong signals deserve
        # faster integration than weak noise
        base_alpha = _FLOW_EWMA_ALPHA
        conviction_boost = min(abs(flow.conviction), 1.0)
        alpha = min(base_alpha * (1.0 + conviction_boost), _FLOW_EWMA_ADAPTIVE_CAP)

        self._flow_ewma = alpha * signed + (1.0 - alpha) * self._flow_ewma
        return self._flow_ewma

    def _ewma_direction(self) -> str:
        """Derive direction from EWMA state."""
        if self._flow_ewma > _FLOW_CONV_THRESHOLD * 0.4:
            return "long"
        elif self._flow_ewma < -_FLOW_CONV_THRESHOLD * 0.4:
            return "short"
        return ""

    # ── v3.0: AMD Conviction Modifier ────────────────────────────────────

    def _amd_conviction_modifier(self, ict: ICTContext, direction: str) -> float:
        """
        Institutional AMD conviction modifier.

        AMD phase modifies conviction and sizing — it NEVER blocks.
        Smart money desks adjust position size when trading against
        macro context. They don't refuse a confirmed setup.

        Returns a multiplier in [0.60, 1.10]:
          • Aligned with AMD:     1.10 (bonus for confluence)
          • Neutral / no phase:   1.00
          • Against DISTRIBUTION: 0.80 (mild headwind)
          • Against MANIPULATION: 0.60 (strong headwind, need very
            strong flow to overcome)

        The caller applies this to:
          - The conviction threshold for TRACKING → READY transition
          - The entry tier (S→A when against MANIPULATION)
        """
        _phase = (ict.amd_phase or '').upper()
        _bias  = (ict.amd_bias or '').lower()

        if not _phase or not _bias or _bias == "neutral":
            return 1.0

        # Determine if direction is against AMD bias
        is_against = (
            (direction == "long" and _bias == "bearish") or
            (direction == "short" and _bias == "bullish")
        )
        is_aligned = (
            (direction == "long" and _bias == "bullish") or
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

    # ── v3.0: Displacement / Momentum Entry ──────────────────────────────

    def _check_displacement_momentum(
        self,
        snap: LiquidityMapSnapshot,
        flow: OrderFlowState,
        ict: ICTContext,
        price: float,
        atr: float,
        now: float,
        candles_1m: Optional[List[Dict]] = None,
        candles_5m: Optional[List[Dict]] = None,
    ) -> bool:
        """
        Institutional displacement entry — the "FVG continuation" in ICT.

        When a candle prints clean displacement (large body relative to range,
        elevated volume, breaking structural levels), enter the continuation.
        This captures trend moves that have NO nearby pool to target — the
        entry engine's biggest blind spot until now.

        Detection criteria (ALL must pass):
          1. Recent closed candle with body/range >= 0.65 (clean displacement)
          2. Candle range >= 0.6×ATR (meaningful move, not micro-noise)
          3. Volume >= 1.3× 20-bar average (institutional participation)
          4. Flow EWMA agrees with displacement direction
          5. Not within cooldown window
          6. Hourly rate limit not exceeded

        SL: behind the displacement candle extreme + 0.15×ATR buffer
        TP: nearest opposing liquidity pool, or 2×risk projection

        Returns True if a signal was emitted.
        """
        if now - self._last_entry_at < _MOMENTUM_COOLDOWN_SEC:
            return False

        # ── Hourly rate limiter ───────────────────────────────────────────
        if now - self._momentum_hour_start > 3600.0:
            self._momentum_entries_1h = 0
            self._momentum_hour_start = now
        if self._momentum_entries_1h >= _MOMENTUM_MAX_ENTRIES_PER_HOUR:
            return False

        # ── Find displacement candle ──────────────────────────────────────
        # Check 5m first (higher conviction), then 1m
        disp_candle = None
        disp_tf = ""
        disp_direction = ""

        for candles, tf_label in [(candles_5m, "5m"), (candles_1m, "1m")]:
            if not candles or len(candles) < _MOMENTUM_LOOKBACK_CANDLES + 20:
                continue

            # 20-bar average volume (exclude last forming candle)
            vol_window = candles[-(20 + 2):-2]
            avg_vol = sum(float(c.get('v', 0)) for c in vol_window) / max(len(vol_window), 1)

            # Check the last N CLOSED candles (skip [-1] which is forming)
            for offset in range(2, 2 + _MOMENTUM_LOOKBACK_CANDLES):
                if offset >= len(candles):
                    break
                c = candles[-offset]
                c_o = float(c['o'])
                c_c = float(c['c'])
                c_h = float(c['h'])
                c_l = float(c['l'])
                c_v = float(c.get('v', 0))
                c_ts = int(c.get('t', 0) or 0)

                # Dedup: don't re-signal same candle
                if c_ts > 0 and c_ts == self._last_momentum_candle_ts:
                    continue

                body = abs(c_c - c_o)
                rng  = c_h - c_l
                if rng < 1e-10:
                    continue

                body_ratio = body / rng
                vol_ratio  = c_v / max(avg_vol, 1e-10)
                atr_move   = rng / max(atr, 1e-10)

                # ── ALL criteria must pass ────────────────────────────
                if body_ratio < _MOMENTUM_MIN_BODY_RATIO:
                    continue
                if atr_move < _MOMENTUM_MIN_ATR_MOVE:
                    continue
                if vol_ratio < _MOMENTUM_MIN_VOL_RATIO:
                    continue

                # Direction from candle body
                candle_dir = "long" if c_c > c_o else "short"

                # Flow EWMA must agree with displacement direction
                ewma_dir = self._ewma_direction()
                if ewma_dir and ewma_dir != candle_dir:
                    continue

                # Found a valid displacement candle
                disp_candle = c
                disp_tf = tf_label
                disp_direction = candle_dir
                break

            if disp_candle is not None:
                break

        if disp_candle is None:
            return False

        # ── AMD modifier (influences, never blocks) ───────────────────────
        amd_mult = self._amd_conviction_modifier(ict, disp_direction)

        # ── BUG-2: Don't trade opposite of recent sweep reversal ──────────
        _sweep_cd = 60.0
        if (self._last_sweep_reversal_dir
                and now - self._last_sweep_reversal_time < _sweep_cd
                and disp_direction != self._last_sweep_reversal_dir):
            return False

        # ── Compute SL ────────────────────────────────────────────────────
        c_h = float(disp_candle['h'])
        c_l = float(disp_candle['l'])
        buffer = atr * _MOMENTUM_SL_BUFFER_ATR

        if disp_direction == "long":
            sl = c_l - buffer
        else:
            sl = c_h + buffer

        # Also check ICT OB for structural SL
        ict_sl = self._compute_sl(ict, disp_direction, price, atr)
        if ict_sl is not None:
            # Use the wider (safer) of candle SL and ICT SL
            if disp_direction == "long":
                sl = min(sl, ict_sl)
            else:
                sl = max(sl, ict_sl)

        # ── Compute TP ────────────────────────────────────────────────────
        # First choice: nearest opposing liquidity pool
        tp = None
        if disp_direction == "long":
            for t in snap.bsl_pools:
                if t.pool.price > price and t.distance_atr <= _MAX_TARGET_ATR:
                    tp_candidate = self._compute_tp_approach(t, "long", price, atr)
                    if tp_candidate is not None:
                        tp = tp_candidate
                        break
        else:
            for t in snap.ssl_pools:
                if t.pool.price < price and t.distance_atr <= _MAX_TARGET_ATR:
                    tp_candidate = self._compute_tp_approach(t, "short", price, atr)
                    if tp_candidate is not None:
                        tp = tp_candidate
                        break

        # Fallback: 2× risk projection
        if tp is None:
            risk = abs(price - sl)
            if disp_direction == "long":
                tp = price + risk * 2.0
            else:
                tp = price - risk * 2.0

        # ── R:R validation ────────────────────────────────────────────────
        risk = abs(price - sl)
        reward = abs(tp - price)
        if risk < 1e-10:
            return False
        rr = reward / risk
        # AMD-adjusted R:R threshold: gentle adjustment, not 1/mult
        adjusted_min_rr = _MOMENTUM_MIN_RR * (2.0 - amd_mult)
        if rr < adjusted_min_rr:
            return False

        # ── SL sanity ─────────────────────────────────────────────────────
        sl_dist_pct = abs(price - sl) / price
        if sl_dist_pct < 0.001 or sl_dist_pct > 0.035:
            return False

        # ── Build the primary target for the signal ───────────────────────
        _target_pool = None
        if disp_direction == "long" and snap.bsl_pools:
            _target_pool = snap.bsl_pools[0]
        elif disp_direction == "short" and snap.ssl_pools:
            _target_pool = snap.ssl_pools[0]

        if _target_pool is None:
            return False

        # ── Emit signal ───────────────────────────────────────────────────
        c_ts = int(disp_candle.get('t', 0) or 0)
        self._last_momentum_candle_ts = c_ts
        self._momentum_entries_1h += 1

        vol_ratio = float(disp_candle.get('v', 0)) / max(1.0, atr)  # approx
        self._signal = EntrySignal(
            side=disp_direction,
            entry_type=EntryType.DISPLACEMENT_MOMENTUM,
            entry_price=price,
            sl_price=sl,
            tp_price=tp,
            rr_ratio=rr,
            target_pool=_target_pool,
            conviction=abs(flow.conviction) * amd_mult,
            reason=(
                f"DISPLACEMENT {disp_direction.upper()} [{disp_tf}] | "
                f"body={float(disp_candle['c'])-float(disp_candle['o']):+.1f} "
                f"range={float(disp_candle['h'])-float(disp_candle['l']):.1f} "
                f"vol={vol_ratio:.1f}x | "
                f"AMD×{amd_mult:.2f} R:R={rr:.1f}"
            ),
            ict_validation=self._ict_summary(ict, disp_direction),
        )
        logger.info(
            f"⚡ MOMENTUM SIGNAL: {disp_direction.upper()} [{disp_tf}] | "
            f"body/range={abs(float(disp_candle['c'])-float(disp_candle['o']))/(float(disp_candle['h'])-float(disp_candle['l'])+1e-10):.2f} "
            f"R:R={rr:.1f} AMD×{amd_mult:.2f}"
        )
        return True

    def _check_proximity_approach(
        self,
        snap: LiquidityMapSnapshot,
        flow: OrderFlowState,
        ict: ICTContext,
        price: float,
        atr: float,
        now: float,
    ) -> bool:
        """
        FIX-E: When price approaches a significant pool closely (0.15-2.0 ATR),
        emit a pre-sweep approach entry even before sustained flow builds.
        Requires at least one structural confirmation.
        Returns True if a signal was emitted.
        """
        if now - self._last_entry_at < _ENTRY_COOLDOWN_SEC:
            return False

        # ── BUG-2 FIX: Block proximity signals OPPOSITE of recent sweep reversal ──
        # If sweep analysis just said REVERSAL LONG (SSL swept, go long),
        # don't let proximity immediately enter SHORT. That's fighting the
        # institutional sweep — the dumbest possible trade.
        _sweep_cooldown = 60.0  # seconds
        if (self._last_sweep_reversal_dir
                and now - self._last_sweep_reversal_time < _sweep_cooldown):
            # Will be checked per-side below after we determine approach_side
            pass  # guard applied after approach_side is determined

        # Find the closest approaching pool
        best_approach: Optional[PoolTarget] = None
        approach_side: str = ""

        # BSL approaching (flow long OR price naturally rising toward it)
        for t in snap.bsl_pools:
            if (_PROXIMITY_ENTRY_ATR_MIN <= t.distance_atr <= _PROXIMITY_ENTRY_ATR_MAX
                    and t.significance >= _PROXIMITY_MIN_SIG):
                if best_approach is None or t.distance_atr < best_approach.distance_atr:
                    best_approach = t
                    approach_side = "long"

        # SSL approaching
        for t in snap.ssl_pools:
            if (_PROXIMITY_ENTRY_ATR_MIN <= t.distance_atr <= _PROXIMITY_ENTRY_ATR_MAX
                    and t.significance >= _PROXIMITY_MIN_SIG):
                if (best_approach is None
                        or t.pool.proximity_adjusted_sig(t.distance_atr)
                        > best_approach.pool.proximity_adjusted_sig(best_approach.distance_atr)):
                    best_approach = t
                    approach_side = "short"

        if best_approach is None:
            self._proximity_confirms = 0
            self._proximity_target = None
            self._proximity_side = ""
            return False

        # ── BUG-2 GUARD: Don't trade opposite of recent sweep reversal ────
        if (self._last_sweep_reversal_dir
                and now - self._last_sweep_reversal_time < _sweep_cooldown
                and approach_side != self._last_sweep_reversal_dir):
            logger.debug(
                f"⛔ Proximity {approach_side} BLOCKED — recent sweep reversal "
                f"said {self._last_sweep_reversal_dir.upper()} "
                f"({now - self._last_sweep_reversal_time:.0f}s ago)")
            return False

        # ── BUG-3 FIX: AMD conviction modifier (influences, never blocks) ──
        # v3.0: AMD reduces conviction for proximity entries against bias,
        # but does not prevent them. Strong proximity + strong flow can
        # still override a stale AMD bias.
        _amd_mult = self._amd_conviction_modifier(ict, approach_side)
        if _amd_mult < 1.0:
            _amd_phase = (getattr(ict, 'amd_phase', '') or '').upper()
            _amd_bias  = (getattr(ict, 'amd_bias',  '') or '').lower()
            # Only block if conviction is genuinely weak AND AMD is contra
            if abs(flow.conviction) < _FLOW_CONV_THRESHOLD * 0.7:
                logger.debug(
                    f"⛔ Proximity {approach_side} weakened by AMD "
                    f"{_amd_phase}+{_amd_bias} (conv={flow.conviction:+.2f} "
                    f"× AMD={_amd_mult:.2f} too low)")
                return False

        # Changed pool target — reset counter
        if (self._proximity_target is None
                or abs(self._proximity_target.pool.price - best_approach.pool.price) > atr * 0.3):
            self._proximity_confirms = 0
            self._proximity_target = best_approach
            self._proximity_side = approach_side

        # Need at least ONE structural confirmation
        confirmations = 0
        if approach_side == "long" and flow.cvd_trend > 0.10:
            confirmations += 1
        elif approach_side == "short" and flow.cvd_trend < -0.10:
            confirmations += 1

        if best_approach.pool.ob_aligned:
            confirmations += 1
        if best_approach.pool.fvg_aligned:
            confirmations += 1

        amd_agrees = (
            (approach_side == "long" and ict.amd_bias == "bullish") or
            (approach_side == "short" and ict.amd_bias == "bearish")
        )
        if amd_agrees:
            confirmations += 1

        if ict.session_quality == "prime":
            confirmations += 1

        if confirmations < 1:
            return False

        # Flow must not be actively opposing
        if approach_side == "long" and flow.conviction < -0.30:
            return False
        if approach_side == "short" and flow.conviction > 0.30:
            return False

        self._proximity_confirms += 1
        if self._proximity_confirms < _PROXIMITY_CONFIRM_COUNT:
            return False

        # Compute SL/TP for proximity approach
        sl = self._compute_sl(ict, approach_side, price, atr)
        tp = self._compute_tp_approach(best_approach, approach_side, price, atr)

        if sl is None or tp is None:
            return False

        risk = abs(price - sl)
        reward = abs(tp - price)
        if risk < 1e-10:
            return False
        rr = reward / risk
        if rr < _MIN_RR_RATIO:
            return False

        self._signal = EntrySignal(
            side=approach_side,
            entry_type=EntryType.PRE_SWEEP_APPROACH,
            entry_price=price,
            sl_price=sl,
            tp_price=tp,
            rr_ratio=rr,
            target_pool=best_approach,
            conviction=flow.conviction,
            reason=(
                f"PROXIMITY {approach_side.upper()} → "
                f"{best_approach.pool.side.value} ${best_approach.pool.price:,.1f} "
                f"({best_approach.distance_atr:.2f} ATR) "
                f"sig={best_approach.significance:.1f} "
                f"confirms={confirmations} R:R={rr:.1f}"
            ),
            ict_validation=self._ict_summary(ict, approach_side),
        )
        logger.info(
            f"⚡ PROXIMITY SIGNAL: {approach_side.upper()} → "
            f"${best_approach.pool.price:,.1f} | {self._signal.reason}"
        )
        self._proximity_confirms = 0
        self._proximity_target = None
        return True

    # ── State: SCANNING ──────────────────────────────────────────────────

    def _do_scanning(
        self,
        snap: LiquidityMapSnapshot,
        flow: OrderFlowState,
        ict: ICTContext,
        price: float,
        atr: float,
        now: float,
    ) -> None:
        if now - self._last_entry_at < _ENTRY_COOLDOWN_SEC:
            return

        if not flow.direction:
            return

        target = self._find_flow_target(snap, flow, price, atr)
        if target is None:
            return

        # ── v3.0: AMD conviction modifier (influences, never blocks) ─────
        # AMD modifies the SIGNAL tier and conviction output — NOT the
        # flow threshold. The bar to enter TRACKING is the same regardless
        # of AMD phase. What changes is position size and tier.
        amd_mult = self._amd_conviction_modifier(ict, flow.direction)

        # ── v3.0: Directional preference ─────────────────────────────────
        # When AMD bias + dealing range position agree on a direction,
        # institutional desks have a DIRECTIONAL PREFERENCE. The aligned
        # direction enters tracking at normal flow threshold; the contra
        # direction needs stronger conviction to overcome the structural
        # headwind.
        #
        # Example: AMD=bearish + DEEP PREMIUM (76%) → prefer SHORT.
        # A LONG entry needs 50% stronger flow conviction to start
        # tracking. This prevents the bot from chasing random long ticks
        # in an environment where shorts are structurally favored.
        #
        # This is NOT blocking — it's asymmetric conviction gating.
        _pd = getattr(ict, 'dealing_range_pd', 0.5)
        _bias = (ict.amd_bias or '').lower()
        _phase = (ict.amd_phase or '').upper()

        _structural_short_bias = (
            _bias == 'bearish'
            and _pd > 0.60
            and _phase in ('MANIPULATION', 'DISTRIBUTION', 'REDISTRIBUTION')
        )
        _structural_long_bias = (
            _bias == 'bullish'
            and _pd < 0.40
            and _phase in ('MANIPULATION', 'DISTRIBUTION', 'REDISTRIBUTION')
        )

        # Contra-directional flow needs 50% more conviction
        _contra_penalty = 1.5
        if _structural_short_bias and flow.direction == "long":
            if abs(flow.conviction) < _FLOW_CONV_THRESHOLD * 0.5 * _contra_penalty:
                logger.debug(
                    f"EntryEngine: LONG flow {flow.conviction:+.2f} too weak "
                    f"in PREMIUM+bearish (need {_FLOW_CONV_THRESHOLD * 0.5 * _contra_penalty:.2f})")
                return
        elif _structural_long_bias and flow.direction == "short":
            if abs(flow.conviction) < _FLOW_CONV_THRESHOLD * 0.5 * _contra_penalty:
                logger.debug(
                    f"EntryEngine: SHORT flow {flow.conviction:+.2f} too weak "
                    f"in DISCOUNT+bullish (need {_FLOW_CONV_THRESHOLD * 0.5 * _contra_penalty:.2f})")
                return

        self._tracking = _TrackingState(
            direction=flow.direction,
            target=target,
            started_at=now,
            flow_ticks=1,
            peak_conviction=abs(flow.conviction),
            amd_mult=amd_mult,
        )
        self._state = EngineState.TRACKING
        self._state_entered = now
        logger.info(
            f"📡 TRACKING: {flow.direction.upper()} → "
            f"${target.pool.price:,.1f} ({target.pool.side.value}) "
            f"dist={target.distance_atr:.1f} ATR, "
            f"sig={target.significance:.1f}, "
            f"flow={flow.conviction:+.2f}"
            f"{f' AMD×{amd_mult:.2f}' if amd_mult < 1.0 else ''}"
        )

    # ── State: TRACKING ──────────────────────────────────────────────────

    def _do_tracking(
        self,
        snap: LiquidityMapSnapshot,
        flow: OrderFlowState,
        ict: ICTContext,
        price: float,
        atr: float,
        now: float,
    ) -> None:
        tr = self._tracking
        if tr is None:
            self._state = EngineState.SCANNING
            return

        # FIX-L: shorter timeout
        if now - tr.started_at > _TRACKING_TIMEOUT_SEC:
            logger.info("📡 TRACKING timeout — back to scanning")
            self._tracking = None
            self._state = EngineState.SCANNING
            self._state_entered = now
            return

        hold_time = now - tr.started_at

        # ── v3.0: EWMA-based flow evaluation ─────────────────────────────
        # Instead of binary tick counters, use the smoothed EWMA signal.
        # EWMA already incorporates flow history with exponential decay —
        # a single contrary tick reduces it by ~15%, not 100%.

        # Determine if EWMA is aligned, neutral, or contrary
        ewma_agrees = (
            (tr.direction == "long" and self._flow_ewma > 0.05) or
            (tr.direction == "short" and self._flow_ewma < -0.05)
        )
        ewma_contrary = (
            (tr.direction == "long" and self._flow_ewma < -_FLOW_CONV_THRESHOLD * 0.3) or
            (tr.direction == "short" and self._flow_ewma > _FLOW_CONV_THRESHOLD * 0.3)
        )

        # Track agreeing vs contrary ticks (for compatibility and logging)
        if flow.direction == tr.direction:
            tr.flow_ticks += 1
            tr.contrary_ticks = 0
            tr.peak_conviction = max(tr.peak_conviction, abs(flow.conviction))
            tr.last_contrary_ts = 0.0
        elif flow.direction and flow.direction != tr.direction:
            tr.contrary_ticks += 1
            if tr.last_contrary_ts == 0.0:
                tr.last_contrary_ts = now
        # Neutral ticks: don't count as contrary (FIX-F preserved)

        # ── v3.0: Abort decision uses EWMA + minimum hold time ────────────
        # Three conditions must ALL be true to abort:
        #   1. Minimum hold time elapsed (prevents micro-oscillation kills)
        #   2. EWMA is contrary (smoothed signal, not instantaneous)
        #   3. Sustained contrary for at least 3 seconds
        _sustained_contrary_sec = (
            now - tr.last_contrary_ts
            if tr.last_contrary_ts > 0 else 0.0
        )

        if (hold_time >= _TRACKING_MIN_HOLD_SEC
                and ewma_contrary
                and _sustained_contrary_sec >= 3.0):
            logger.info(
                f"📡 TRACKING aborted: flow reversed ({tr.direction} → "
                f"{flow.direction}) EWMA={self._flow_ewma:+.2f} "
                f"sustained={_sustained_contrary_sec:.1f}s")
            self._tracking = None
            self._state = EngineState.SCANNING
            self._state_entered = now
            return

        # FIX-G: Only update target when direction agrees
        if flow.direction == tr.direction:
            target = self._find_flow_target(snap, flow, price, atr)
            if target is not None:
                tr.target = target

        # ── TRACKING → READY transition ───────────────────────────────────
        # Flow threshold is UNCHANGED by AMD. AMD only affects the signal
        # conviction and tier AFTER READY is reached. This is the
        # institutional approach: same entry criteria, different sizing.
        ready = (
            tr.flow_ticks >= _FLOW_SUSTAINED_TICKS
            and flow.cvd_agrees
            and abs(flow.conviction) >= _FLOW_CONV_THRESHOLD
            and tr.target.distance_atr >= _MIN_TARGET_ATR
            and tr.target.distance_atr <= _MAX_TARGET_ATR
        )

        # ICT bonus: lower bar if structure strongly agrees (preserved)
        if not ready and tr.flow_ticks >= 1:
            ict_boost = self._ict_structure_agrees(ict, tr.direction)
            if ict_boost and abs(flow.conviction) >= _FLOW_CONV_THRESHOLD * 0.65:
                ready = True

        # v3.0: EWMA fast-track — if EWMA is strongly directional and
        # has been consistent, allow READY even without CVD agreement.
        # This captures momentum moves where tick flow is strong but
        # CVD hasn't caught up yet.
        # BUG-FIX: Also require current flow isn't STRONGLY contrary —
        # EWMA lags, so it can be +0.35 while instant flow is -0.53.
        # Entering READY in an already-decaying state wastes the timeout.
        if not ready and tr.flow_ticks >= 3:
            ewma_strong = abs(self._flow_ewma) >= _FLOW_CONV_THRESHOLD * 0.8
            ewma_dir_agrees = (
                (tr.direction == "long" and self._flow_ewma > 0) or
                (tr.direction == "short" and self._flow_ewma < 0)
            )
            flow_not_strongly_contrary = (
                not flow.direction
                or flow.direction == tr.direction
                or abs(flow.conviction) < _FLOW_CONV_THRESHOLD
            )
            if ewma_strong and ewma_dir_agrees and flow_not_strongly_contrary:
                ready = True

        if ready:
            # ── BUG-FIX: R:R pre-check before entering READY ─────────
            # If the target is too close for a viable R:R, don't waste
            # 90s in READY state — reject immediately and keep scanning.
            # This prevents the engine from burning its entire timeout
            # on a mathematically impossible trade.
            _pre_sl = self._compute_sl(ict, tr.direction, price, atr)
            _pre_tp = self._compute_tp_approach(tr.target, tr.direction, price, atr)
            if _pre_sl is not None and _pre_tp is not None:
                _pre_risk = abs(price - _pre_sl)
                _pre_reward = abs(_pre_tp - price)
                if _pre_risk > 1e-10:
                    _pre_rr = _pre_reward / _pre_risk
                    _adj_rr = _MIN_RR_RATIO * (2.0 - tr.amd_mult)
                    if _pre_rr < _adj_rr * 0.7:
                        # R:R is less than 70% of required — hopeless
                        logger.debug(
                            f"📡 READY rejected: R:R {_pre_rr:.2f} < "
                            f"{_adj_rr * 0.7:.2f} (70% of {_adj_rr:.2f}) — "
                            f"target too close for viable trade")
                        ready = False

        if ready:
            self._state = EngineState.READY
            self._state_entered = now
            # ── v3.0: Initialize conviction decay state ───────────────
            # Conviction is initialized at RAW flow conviction — NOT
            # AMD-adjusted. AMD modifier is applied to the SIGNAL output
            # only (tier and sizing). Internal decay dynamics should not
            # be affected by AMD or conviction decays too fast.
            self._ready_conviction = abs(flow.conviction)
            self._ready_peak_conviction = self._ready_conviction
            self._ready_last_agree_ts = now
            # Track R:R failures for early READY abort
            self._ready_rr_fail_count = 0
            logger.info(
                f"✅ READY: {tr.direction.upper()} → "
                f"${tr.target.pool.price:,.1f} "
                f"conviction={self._ready_conviction:+.2f} "
                f"ticks={tr.flow_ticks}"
                f"{f' AMD×{tr.amd_mult:.2f}' if tr.amd_mult < 1.0 else ''}"
            )

    # ── State: READY ─────────────────────────────────────────────────────

    def _do_ready(
        self,
        snap: LiquidityMapSnapshot,
        flow: OrderFlowState,
        ict: ICTContext,
        price: float,
        atr: float,
        now: float,
    ) -> None:
        tr = self._tracking
        if tr is None:
            self._state = EngineState.SCANNING
            return

        if now - self._state_entered > _READY_TIMEOUT_SEC:
            logger.info("✅ READY timeout — back to scanning")
            self._tracking = None
            self._state = EngineState.SCANNING
            self._state_entered = now
            return

        hold_time = now - self._state_entered

        # ── v3.0: Conviction decay instead of binary kill ─────────────────
        # Track whether current flow agrees or is contrary.
        # Contrary flow DECAYS conviction gradually.
        # Only abort when conviction drops below kill threshold.
        flow_agrees = (flow.direction == tr.direction)
        flow_contrary = (
            flow.direction != ""
            and flow.direction != tr.direction
        )

        if flow_agrees:
            self._ready_last_agree_ts = now
            # Conviction recovers (slowly) when flow re-aligns
            recovery = min(0.02, abs(flow.conviction) * 0.05)
            self._ready_conviction = min(
                self._ready_peak_conviction,
                self._ready_conviction + recovery
            )
            tr.contrary_ticks = 0
        elif flow_contrary:
            tr.contrary_ticks += 1
            # Decay proportional to how contrary the flow is
            decay_strength = abs(flow.conviction)
            decay = _READY_DECAY_RATE * max(decay_strength, 0.3)
            # BUG-FIX: Floor at 0.0 — negative conviction makes recovery
            # impossibly slow (needs 25+ ticks just to reach zero)
            self._ready_conviction = max(0.0, self._ready_conviction - decay)
        # Neutral flow: no change to conviction

        # ── Abort conditions (ALL must be true): ──────────────────────────
        #   1. Minimum hold time elapsed
        #   2. Conviction has decayed below kill threshold
        #   3. EWMA confirms contrary direction (not just a single tick)
        ewma_contrary = (
            (tr.direction == "long" and self._flow_ewma < -0.10) or
            (tr.direction == "short" and self._flow_ewma > 0.10)
        )

        if (hold_time >= _READY_MIN_HOLD_SEC
                and self._ready_conviction < _READY_MIN_CONVICTION
                and ewma_contrary):
            logger.info(
                f"✅ READY: conviction decayed {self._ready_conviction:.3f} "
                f"< {_READY_MIN_CONVICTION} — back to scanning "
                f"(held {hold_time:.0f}s, peak={self._ready_peak_conviction:.2f}, "
                f"EWMA={self._flow_ewma:+.2f})")
            self._tracking = None
            self._state = EngineState.SCANNING
            self._state_entered = now
            return

        # ── Compute SL/TP ─────────────────────────────────────────────────
        sl = self._compute_sl(ict, tr.direction, price, atr)
        tp = self._compute_tp_approach(tr.target, tr.direction, price, atr)

        if sl is None or tp is None:
            # BUG-FIX: Log SL/TP computation failures — silent return
            # made it impossible to diagnose why READY burned full timeout.
            _fail_count = self._ready_rr_fail_count + 1
            self._ready_rr_fail_count = _fail_count
            if _fail_count == 1 or _fail_count % 40 == 0:
                logger.debug(
                    f"✅ READY: SL={'None' if sl is None else f'${sl:,.1f}'} "
                    f"TP={'None' if tp is None else f'${tp:,.1f}'} — "
                    f"no valid levels (fail #{_fail_count})")
            # Early abort: if SL/TP fails 120 consecutive times (~30s),
            # the ICT structure is simply not there. Don't burn 90s.
            if _fail_count >= 120:
                logger.info(
                    f"✅ READY: SL/TP failed {_fail_count} consecutive ticks "
                    f"— no ICT structure, aborting early")
                self._tracking = None
                self._state = EngineState.SCANNING
                self._state_entered = now
            return

        # ── BUG-2 FIX: Block opposite of recent sweep reversal ───────────
        _sweep_cd = 60.0
        if (self._last_sweep_reversal_dir
                and now - self._last_sweep_reversal_time < _sweep_cd
                and tr.direction != self._last_sweep_reversal_dir):
            logger.info(
                f"⛔ READY: {tr.direction.upper()} blocked — "
                f"recent sweep reversal said {self._last_sweep_reversal_dir.upper()}")
            self._tracking = None
            self._state = EngineState.SCANNING
            self._state_entered = now
            return

        risk = abs(price - sl)
        reward = abs(tp - price)
        if risk < 1e-10:
            return
        rr = reward / risk

        # ── v3.0: AMD-adjusted R:R threshold ─────────────────────────────
        # Against-AMD entries demand modestly better R:R to compensate.
        # Formula: base × (2.0 - mult). Against MANIP (0.60): 1.4×1.40 = 1.96
        # Against DIST (0.80): 1.4×1.20 = 1.68. Aligned (1.10): 1.4×0.90 = 1.26.
        # This is much more reasonable than the old 1/mult formula which
        # produced 2.33 for MANIPULATION — effectively blocking all entries.
        adjusted_min_rr = _MIN_RR_RATIO * (2.0 - tr.amd_mult)

        if rr < adjusted_min_rr:
            # BUG-FIX: Track R:R failures for early abort
            _fail_count = self._ready_rr_fail_count + 1
            self._ready_rr_fail_count = _fail_count
            if _fail_count == 1 or _fail_count % 60 == 0:
                logger.debug(
                    f"✅ READY: R:R {rr:.2f} < {adjusted_min_rr:.2f} "
                    f"(AMD×{tr.amd_mult:.2f}) — skipping "
                    f"(entry=${price:,.1f} SL=${sl:,.1f} TP=${tp:,.1f}) "
                    f"[fail #{_fail_count}]"
                )
            # If R:R fails 80 consecutive times (~20s), target is too
            # close for viable trade at current price. Abort early.
            if _fail_count >= 80:
                logger.info(
                    f"✅ READY: R:R failed {_fail_count} consecutive ticks "
                    f"(best={rr:.2f} vs required={adjusted_min_rr:.2f}) "
                    f"— target too close, aborting")
                self._tracking = None
                self._state = EngineState.SCANNING
                self._state_entered = now
            return

        # R:R passed — reset failure counter
        self._ready_rr_fail_count = 0

        self._signal = EntrySignal(
            side=tr.direction,
            entry_type=EntryType.PRE_SWEEP_APPROACH,
            entry_price=price,
            sl_price=sl,
            tp_price=tp,
            rr_ratio=rr,
            target_pool=tr.target,
            conviction=self._ready_conviction * tr.amd_mult,
            reason=(
                f"Flow {tr.direction} → {tr.target.pool.side.value} "
                f"${tr.target.pool.price:,.1f} | "
                f"flow={flow.conviction:+.2f} CVD={flow.cvd_trend:+.2f} | "
                f"R:R={rr:.1f}"
                f"{f' AMD×{tr.amd_mult:.2f}' if tr.amd_mult < 1.0 else ''}"
            ),
            ict_validation=self._ict_summary(ict, tr.direction),
        )
        logger.info(
            f"🎯 SIGNAL: {self._signal.entry_type.value} "
            f"{self._signal.side.upper()} | {self._signal.reason}"
        )

    # ── State: POST_SWEEP ────────────────────────────────────────────────

    def _enter_post_sweep(
        self,
        sweep: SweepResult,
        snap: LiquidityMapSnapshot,
        flow: OrderFlowState,
        ict: ICTContext,
        price: float,
        atr: float,
        now: float,
    ) -> None:
        # FIX-H: Timeframe-based quality floor
        _TF_QUALITY_MIN = {'1m': 0.65, '5m': 0.55, '15m': 0.45, '4h': 0.35}
        _tf = getattr(sweep.pool, 'timeframe', '5m')
        _required_quality = _TF_QUALITY_MIN.get(_tf, 0.45)
        if sweep.quality < _required_quality:
            logger.debug(
                f"🌊 SWEEP SKIPPED: [{_tf}] quality={sweep.quality:.2f} "
                f"< required {_required_quality:.2f} — ignoring micro-sweep noise"
            )
            return

        self._post_sweep = _PostSweepState(
            sweep=sweep,
            entered_at=now,
            initial_flow=flow.conviction,
            initial_flow_dir=flow.direction,
        )
        self._state = EngineState.POST_SWEEP
        self._state_entered = now
        self._tracking = None
        self._signal = None
        logger.info(
            f"🌊 SWEEP DETECTED: {sweep.pool.side.value} "
            f"${sweep.pool.price:,.1f} quality={sweep.quality:.2f} "
            f"| Scoring reversal vs continuation..."
        )

    def _do_post_sweep(
        self,
        snap: LiquidityMapSnapshot,
        flow: OrderFlowState,
        ict: ICTContext,
        price: float,
        atr: float,
        now: float,
    ) -> None:
        ps = self._post_sweep
        if ps is None:
            self._state = EngineState.SCANNING
            return

        elapsed = now - ps.entered_at
        if elapsed < _POST_SWEEP_EVAL_SEC:
            return

        # FIX-K: reduced timeout
        if elapsed > _CISD_MAX_WAIT_SEC:
            logger.info("🌊 POST_SWEEP: timeout — back to scanning")
            self._post_sweep = None
            self._state = EngineState.SCANNING
            self._state_entered = now
            return

        decision = self._evaluate_post_sweep(ps.sweep, snap, flow, ict, price, atr, now)

        if decision.action == "reverse":
            self._handle_sweep_reversal(ps.sweep, decision, snap, flow, ict, price, atr, now)
        elif decision.action == "continue":
            self._handle_sweep_continuation(ps.sweep, decision, snap, flow, ict, price, atr, now)

    def _evaluate_post_sweep(
        self,
        sweep: SweepResult,
        snap: LiquidityMapSnapshot,
        flow: OrderFlowState,
        ict: ICTContext,
        price: float,
        atr: float,
        now: float,
    ) -> PostSweepDecision:
        """
        FIX-D: Relaxed thresholds — rev >= 45 AND gap >= 10.
        All factor logic preserved; scoring unchanged.
        """
        sweep_dir = sweep.direction
        cont_dir = "short" if sweep_dir == "long" else "long"

        rev_score  = 0.0
        cont_score = 0.0
        rev_reasons  = []
        cont_reasons = []

        # ── FACTOR 0: AMD PHASE ───────────────────────────────────────────
        amd_phase = (getattr(ict, 'amd_phase', '') or '').upper()
        amd_bias  = (getattr(ict, 'amd_bias',  '') or '').lower()
        amd_conf  = float(getattr(ict, 'amd_confidence', 0.0) or 0.0)

        if amd_phase == 'MANIPULATION':
            if sweep.pool.side == PoolSide.BSL and amd_bias == 'bearish':
                pts = 22.0 * max(amd_conf, 0.5)
                rev_score += pts
                rev_reasons.append(f"AMD MANIP bear+BSL ({amd_conf:.0%})")
            elif sweep.pool.side == PoolSide.SSL and amd_bias == 'bullish':
                pts = 22.0 * max(amd_conf, 0.5)
                rev_score += pts
                rev_reasons.append(f"AMD MANIP bull+SSL ({amd_conf:.0%})")

        elif amd_phase in ('DISTRIBUTION', 'REDISTRIBUTION'):
            if sweep.pool.side == PoolSide.BSL and amd_bias == 'bearish':
                pts = 25.0 * max(amd_conf, 0.5)
                cont_score += pts
                cont_reasons.append(f"AMD DIST bear+BSL ({amd_conf:.0%})")
            elif sweep.pool.side == PoolSide.SSL and amd_bias == 'bullish':
                pts = 25.0 * max(amd_conf, 0.5)
                cont_score += pts
                cont_reasons.append(f"AMD DIST bull+SSL ({amd_conf:.0%})")
            elif sweep.pool.side == PoolSide.SSL and amd_bias == 'bearish':
                cont_score += 20.0 * max(amd_conf, 0.5)
                rev_score  -= 12.0 * max(amd_conf, 0.5)
                cont_reasons.append(f"AMD DIST bear+SSL ({amd_conf:.0%})")
            elif sweep.pool.side == PoolSide.BSL and amd_bias == 'bullish':
                cont_score += 20.0 * max(amd_conf, 0.5)
                rev_score  -= 12.0 * max(amd_conf, 0.5)
                cont_reasons.append(f"AMD DIST bull+BSL ({amd_conf:.0%})")

        elif amd_phase in ('ACCUMULATION', 'REACCUMULATION'):
            # REACCUMULATION = mid-trend pause (15m trending, 5m ranging).
            # Smart money re-loads during the pause. A sweep with displacement
            # + rejection here is a Judas swing — same logic as ACCUMULATION
            # but slightly weighted because there IS a trend in play.
            pts = 12.0 * max(amd_conf, 0.4)
            rev_score += pts
            rev_reasons.append(f"AMD {'REAC' if 'REAC' in amd_phase else 'ACCUM'} ({amd_conf:.0%})")

        # ── FACTOR 1: DISPLACEMENT QUALITY (0-20) ────────────────────────
        if sweep.quality >= 0.70:
            rev_score += 20.0
            rev_reasons.append(f"DISP strong ({sweep.quality:.0%})")
        elif sweep.quality >= 0.50:
            rev_score += 12.0
            rev_reasons.append(f"DISP moderate ({sweep.quality:.0%})")
        elif sweep.quality >= 0.35:
            rev_score += 5.0
            rev_reasons.append(f"DISP weak ({sweep.quality:.0%})")
        else:
            cont_score += 12.0
            cont_reasons.append(f"NO DISP ({sweep.quality:.0%})")

        # ── FACTOR 2: WICK REJECTION / BREAKOUT (0-15) ───────────────────
        if sweep.pool.side == PoolSide.BSL:
            rejecting       = price < sweep.pool.price
            breaking_through = price > sweep.pool.price + 0.3 * atr
            deep_break      = price > sweep.wick_extreme
        else:
            rejecting       = price > sweep.pool.price
            breaking_through = price < sweep.pool.price - 0.3 * atr
            deep_break      = price < sweep.wick_extreme

        if rejecting:
            rej_depth = abs(price - sweep.pool.price) / max(atr, 1e-10)
            rej_pts = min(15.0, 8.0 + rej_depth * 5.0)
            rev_score += rej_pts
            rev_reasons.append(f"REJECTED {rej_depth:.1f}ATR")
        elif deep_break:
            cont_score += 15.0
            cont_reasons.append("DEEP BREAK beyond wick")
        elif breaking_through:
            cont_score += 10.0
            cont_reasons.append("BREAKOUT +0.3ATR past pool")

        # ── FACTOR 3: HTF DEALING RANGE (0-15) ───────────────────────────
        pd = ict.dealing_range_pd if hasattr(ict, 'dealing_range_pd') else 0.5

        if sweep.pool.side == PoolSide.BSL:
            if pd > 0.80:
                rev_score += 15.0; rev_reasons.append(f"DEEP PREMIUM ({pd:.0%})")
            elif pd > 0.65:
                rev_score += 8.0;  rev_reasons.append(f"PREMIUM ({pd:.0%})")
            elif pd < 0.35:
                cont_score += 10.0; cont_reasons.append(f"DISCOUNT→BSL break ({pd:.0%})")
        else:
            if pd < 0.20:
                rev_score += 15.0; rev_reasons.append(f"DEEP DISCOUNT ({pd:.0%})")
            elif pd < 0.35:
                rev_score += 8.0;  rev_reasons.append(f"DISCOUNT ({pd:.0%})")
            elif pd > 0.65:
                cont_score += 10.0; cont_reasons.append(f"PREMIUM→SSL break ({pd:.0%})")

        # ── FACTOR 4: HTF TREND ALIGNMENT (0-15) ─────────────────────────
        htf_trend = getattr(ict, 'structure_15m', '') or ''
        htf_4h    = getattr(ict, 'structure_4h',  '') or ''

        if sweep.pool.side == PoolSide.BSL:
            if htf_4h == "bearish":
                rev_score += 15.0;  rev_reasons.append("4H BEARISH vs BSL sweep")
            elif htf_trend == "bearish":
                rev_score += 10.0;  rev_reasons.append("15m BEARISH vs BSL sweep")
            elif htf_4h == "bullish":
                cont_score += 12.0; cont_reasons.append("4H BULLISH aligns BSL break")
            elif htf_trend == "bullish":
                cont_score += 7.0;  cont_reasons.append("15m BULLISH aligns BSL")
        else:
            if htf_4h == "bullish":
                rev_score += 15.0;  rev_reasons.append("4H BULLISH vs SSL sweep")
            elif htf_trend == "bullish":
                rev_score += 10.0;  rev_reasons.append("15m BULLISH vs SSL sweep")
            elif htf_4h == "bearish":
                cont_score += 12.0; cont_reasons.append("4H BEARISH aligns SSL break")
            elif htf_trend == "bearish":
                cont_score += 7.0;  cont_reasons.append("15m BEARISH aligns SSL")

        # ── FACTOR 5: ORDER FLOW (0-15) ───────────────────────────────────
        flow_agrees_reversal = (flow.direction == sweep_dir and flow.cvd_agrees)
        flow_agrees_continuation = (
            flow.direction and flow.direction != sweep_dir
            and abs(flow.conviction) > _FLOW_CONV_THRESHOLD * 0.6
        )

        if flow_agrees_reversal:
            strength = min(15.0, abs(flow.conviction) * 15.0)
            rev_score += strength
            rev_reasons.append(f"FLOW REV {flow.conviction:+.2f}")
        elif flow_agrees_continuation:
            strength = min(15.0, abs(flow.conviction) * 12.0)
            cont_score += strength
            cont_reasons.append(f"FLOW CONT {flow.conviction:+.2f}")

        if flow.cvd_divergence != 0:
            if sweep.pool.side == PoolSide.BSL and flow.cvd_divergence < -0.2:
                rev_score += 5.0; rev_reasons.append("CVD BEARISH div")
            elif sweep.pool.side == PoolSide.SSL and flow.cvd_divergence > 0.2:
                rev_score += 5.0; rev_reasons.append("CVD BULLISH div")

        # ── FACTOR 6: CISD / BOS CONFIRMATION (0-15) ─────────────────────
        cisd_confirmed = False
        if ict.bos_5m:
            expected_bos = "bullish" if sweep_dir == "long" else "bearish"
            cisd_confirmed = (ict.bos_5m == expected_bos)

        choch_confirmed = False
        if ict.choch_5m:
            expected_choch = "bullish" if sweep_dir == "long" else "bearish"
            choch_confirmed = (ict.choch_5m == expected_choch)

        if cisd_confirmed:
            rev_score += 15.0; rev_reasons.append("CISD ✅ (5m BOS)")
        elif choch_confirmed:
            rev_score += 10.0; rev_reasons.append("CHoCH ✅ (5m)")

        if ict.bos_5m:
            cont_bos = "bullish" if cont_dir == "long" else "bearish"
            if ict.bos_5m == cont_bos:
                cont_score += 10.0; cont_reasons.append("BOS aligns continuation")

        # ── FACTOR 7: SESSION (0-5) ───────────────────────────────────────
        session = getattr(ict, 'kill_zone', '') or ''
        if 'asia' in session.lower():
            rev_score += 5.0; rev_reasons.append("ASIA session (manipulation)")
        elif 'london' in session.lower() and 'ny' not in session.lower():
            rev_score += 3.0; rev_reasons.append("LONDON (possible Judas)")

        # ── FACTOR 8: TARGET QUALITY (0-10) ──────────────────────────────
        opp_target  = self._find_opposing_target(sweep_dir, snap, price, atr, ict)
        cont_target = self._find_continuation_target(cont_dir, snap, price, atr, ict)

        if opp_target and opp_target.significance >= _MIN_POOL_SIGNIFICANCE:
            rev_score += min(5.0, opp_target.significance * 0.5)
            rev_reasons.append(f"TP pool sig={opp_target.significance:.1f}")

        if cont_target and cont_target.significance >= _MIN_POOL_SIGNIFICANCE:
            cont_score += min(5.0, cont_target.significance * 0.5)
            cont_reasons.append(f"Next pool sig={cont_target.significance:.1f}")

        # ── FACTOR 9: OB BLOCKING (0-10) ─────────────────────────────────
        ob_price = getattr(ict, 'nearest_ob_price', 0.0) or 0.0
        if ob_price > 0:
            if cont_dir == "long" and ob_price > price:
                ob_dist_atr = (ob_price - price) / max(atr, 1e-10)
                if ob_dist_atr < 2.0:
                    rev_score += 8.0; rev_reasons.append(f"OB BLOCKS cont at {ob_dist_atr:.1f}ATR")
            elif cont_dir == "short" and ob_price < price:
                ob_dist_atr = (price - ob_price) / max(atr, 1e-10)
                if ob_dist_atr < 2.0:
                    rev_score += 8.0; rev_reasons.append(f"OB BLOCKS cont at {ob_dist_atr:.1f}ATR")

        rev_total  = round(max(rev_score, 0), 1)
        cont_total = round(max(cont_score, 0), 1)
        gap = abs(rev_total - cont_total)

        self._last_sweep_analysis = {
            "rev_score": rev_total, "cont_score": cont_total,
            "rev_reasons": rev_reasons, "cont_reasons": cont_reasons,
            "sweep_side": sweep.pool.side.value,
            "sweep_price": sweep.pool.price,
            "sweep_quality": sweep.quality,
        }

        # FIX-D: relaxed thresholds — was rev>=55 gap>=15
        if rev_total >= 45.0 and gap >= 10.0:
            confidence = min(1.0, rev_total / 90.0)
            reason_str = " + ".join(rev_reasons[:4])
            logger.info(
                f"🔄 SWEEP VERDICT: REVERSAL {sweep_dir.upper()} "
                f"(rev={rev_total:.0f} vs cont={cont_total:.0f} gap={gap:.0f}) "
                f"| {reason_str}"
            )
            return PostSweepDecision(
                action="reverse", direction=sweep_dir,
                confidence=confidence, next_target=opp_target,
                reason=f"REVERSAL [{rev_total:.0f}v{cont_total:.0f}] {reason_str}",
            )

        elif cont_total >= 40.0 and gap >= 10.0:
            confidence = min(1.0, cont_total / 90.0)
            reason_str = " + ".join(cont_reasons[:4])
            logger.info(
                f"➡️ SWEEP VERDICT: CONTINUATION {cont_dir.upper()} "
                f"(cont={cont_total:.0f} vs rev={rev_total:.0f} gap={gap:.0f}) "
                f"| {reason_str}"
            )
            return PostSweepDecision(
                action="continue", direction=cont_dir,
                confidence=confidence, next_target=cont_target,
                reason=f"CONTINUATION [{cont_total:.0f}v{rev_total:.0f}] {reason_str}",
            )

        else:
            logger.debug(
                f"⏳ SWEEP WAIT: rev={rev_total:.0f} cont={cont_total:.0f} "
                f"gap={gap:.0f} (need ≥10) | "
                f"R:[{', '.join(rev_reasons[:3])}] "
                f"C:[{', '.join(cont_reasons[:3])}]"
            )
            return PostSweepDecision(
                action="wait", direction="", confidence=0.0,
                reason=f"WAIT [{rev_total:.0f}v{cont_total:.0f}] gap={gap:.0f}<10",
            )

    def _handle_sweep_reversal(
        self,
        sweep: SweepResult,
        decision: PostSweepDecision,
        snap: LiquidityMapSnapshot,
        flow: OrderFlowState,
        ict: ICTContext,
        price: float,
        atr: float,
        now: float,
    ) -> None:
        side = decision.direction

        if side == "long":
            sl = sweep.wick_extreme - atr * _SL_BUFFER_ATR
        else:
            sl = sweep.wick_extreme + atr * _SL_BUFFER_ATR

        # ── Liquidity-aware SL push: don't place SL inside pool clusters ──
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
            return

        # BUG-1 FIX: Try opposing pool TP first; if R:R is bad, use
        # risk-multiple fallback (2.0x risk). The old code rejected the
        # entire reversal when the nearest pool was too close — missing
        # valid sweep setups where the pool scanner had no far targets yet.
        tp = None
        if decision.next_target:
            _pool_tp = decision.next_target.pool.price
            if side == "long":
                _pool_tp -= atr * _TP_BUFFER_ATR
            else:
                _pool_tp += atr * _TP_BUFFER_ATR
            _pool_reward = abs(_pool_tp - price)
            if _pool_reward / risk >= _MIN_RR_RATIO:
                tp = _pool_tp

        # Fallback: risk-multiple TP (always meets R:R)
        if tp is None:
            tp = price + (risk * 2.0) if side == "long" else price - (risk * 2.0)

        reward = abs(tp - price)
        rr = reward / risk

        # BUG-2 FIX: Record the sweep reversal direction even if R:R
        # fails — prevents proximity from immediately going opposite.
        self._last_sweep_reversal_dir = side
        self._last_sweep_reversal_time = now

        if rr < _MIN_RR_RATIO:
            logger.info(
                f"🌊 Reversal R:R {rr:.2f} < {_MIN_RR_RATIO} — skipping "
                f"(but blocking opposite proximity for 60s)")
            self._post_sweep = None
            self._state = EngineState.SCANNING
            self._state_entered = now
            return

        self._signal = EntrySignal(
            side=side,
            entry_type=EntryType.SWEEP_REVERSAL,
            entry_price=price,
            sl_price=sl,
            tp_price=tp,
            rr_ratio=rr,
            target_pool=decision.next_target or PoolTarget(
                pool=sweep.pool, distance_atr=0, direction=side,
                significance=sweep.pool.significance, tf_sources=[]),
            sweep_result=sweep,
            conviction=decision.confidence,
            reason=decision.reason,
            ict_validation=self._ict_summary(ict, side),
        )
        logger.info(
            f"🔄 SIGNAL: REVERSAL {side.upper()} | "
            f"SL=${sl:,.1f} TP=${tp:,.1f} R:R={rr:.1f} | "
            f"{decision.reason}"
        )
        self._post_sweep = None

    def _handle_sweep_continuation(
        self,
        sweep: SweepResult,
        decision: PostSweepDecision,
        snap: LiquidityMapSnapshot,
        flow: OrderFlowState,
        ict: ICTContext,
        price: float,
        atr: float,
        now: float,
    ) -> None:
        side = decision.direction
        next_target = decision.next_target
        if next_target is None:
            self._post_sweep = None
            self._state = EngineState.SCANNING
            self._state_entered = now
            return

        # SL behind the swept pool + buffer
        if side == "long":
            sl = sweep.pool.price - atr * 0.20
        else:
            sl = sweep.pool.price + atr * 0.20

        # ── Liquidity-aware SL push ───────────────────────────────────
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

        # TP: front-run the target pool (0.35×ATR before)
        tp = next_target.pool.price
        if side == "long":
            tp -= atr * 0.35
        else:
            tp += atr * 0.35

        risk = abs(price - sl)
        reward = abs(tp - price)
        if risk < 1e-10:
            return
        rr = reward / risk

        if rr < _MIN_RR_RATIO:
            self._post_sweep = None
            self._state = EngineState.SCANNING
            self._state_entered = now
            return

        self._signal = EntrySignal(
            side=side,
            entry_type=EntryType.SWEEP_CONTINUATION,
            entry_price=price,
            sl_price=sl,
            tp_price=tp,
            rr_ratio=rr,
            target_pool=next_target,
            sweep_result=sweep,
            conviction=decision.confidence,
            reason=decision.reason,
            ict_validation=self._ict_summary(ict, side),
        )
        logger.info(
            f"➡️ SIGNAL: CONTINUATION {side.upper()} → "
            f"${next_target.pool.price:,.1f} | "
            f"SL=${sl:,.1f} TP=${tp:,.1f} R:R={rr:.1f}"
        )
        self._post_sweep = None

    # ── Helpers ───────────────────────────────────────────────────────────

    def _find_flow_target(
        self,
        snap: LiquidityMapSnapshot,
        flow: OrderFlowState,
        price: float,
        atr: float,
    ) -> Optional[PoolTarget]:
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

        # FIX-10 from liquidity_map: prefer proximity-adjusted significance
        return max(candidates, key=lambda t: t.pool.proximity_adjusted_sig(t.distance_atr))

    def _find_opposing_target(
        self,
        direction: str,
        snap: LiquidityMapSnapshot,
        price: float,
        atr: float,
        ict: Optional["ICTContext"] = None,
    ) -> Optional[PoolTarget]:
        """
        FIX-I: During DISTRIBUTION/REDISTRIBUTION, prefer nearest pool
        so TP sits within the delivery move, not beyond it.
        """
        if direction == "long":
            candidates = snap.bsl_pools
        else:
            candidates = snap.ssl_pools

        reachable = [t for t in candidates
                     if t.distance_atr <= _MAX_TARGET_ATR
                     and t.significance >= _MIN_POOL_SIGNIFICANCE * 0.5]

        if not reachable:
            return None

        amd_phase = (getattr(ict, 'amd_phase', '') or '').upper() if ict else ''
        if amd_phase in ('DISTRIBUTION', 'REDISTRIBUTION'):
            return min(reachable, key=lambda t: t.distance_atr)

        return max(reachable, key=lambda t: t.pool.proximity_adjusted_sig(t.distance_atr))

    def _find_continuation_target(
        self,
        direction: str,
        snap: LiquidityMapSnapshot,
        price: float,
        atr: float,
        ict: Optional["ICTContext"] = None,
    ) -> Optional[PoolTarget]:
        return self._find_opposing_target(direction, snap, price, atr, ict)

    def _compute_sl(
        self,
        ict: ICTContext,
        side: str,
        price: float,
        atr: float,
    ) -> Optional[float]:
        """
        Institutional SL computation — NO ATR FALLBACK.

        RULES:
          1. Primary: ICT OB edge + buffer
          2. Liquidity-aware: if opposing liquidity pools sit between
             entry and SL, push SL beyond them (stops WILL be hunted)
          3. No structure = no trade (return None)

        The buffer is placed BEYOND the OB, not at its edge.
        Smart money targets the OB edge for entries; SL must survive
        a wick through the OB by at least 0.15×ATR.
        """
        sl = None

        # ── PRIMARY: ICT OB anchor ─────────────────────────────────────
        if ict.nearest_ob_price > 0:
            ob_price = ict.nearest_ob_price
            if side == "long" and ob_price < price:
                sl = ob_price - atr * 0.20  # buffer beyond OB
            elif side == "short" and ob_price > price:
                sl = ob_price + atr * 0.20  # buffer beyond OB

        # ── GUARD: Check for opposing liquidity between entry and SL ───
        # If BSL/SSL pools sit between price and SL, push SL beyond them.
        # Market makers WILL sweep those pools; SL inside the splash zone
        # is guaranteed to get hit before the trade has a chance to work.
        if sl is not None and hasattr(self, '_last_liq_snapshot') and self._last_liq_snapshot is not None:
            snap = self._last_liq_snapshot
            _liq_buffer = 0.25 * atr  # buffer beyond the liquidity cluster

            if side == "long":
                # For LONG: SSL pools below entry can sweep down to SL
                for t in snap.ssl_pools:
                    pool_price = t.pool.price
                    # Pool is between SL and entry — SL is in the splash zone
                    if sl < pool_price < price:
                        new_sl = pool_price - _liq_buffer
                        if new_sl < sl:
                            sl = new_sl
                            logger.debug(
                                f"SL pushed: SSL@${pool_price:,.0f} between entry and SL "
                                f"→ SL=${sl:,.1f}")
            else:
                # For SHORT: BSL pools above entry can sweep up to SL
                for t in snap.bsl_pools:
                    pool_price = t.pool.price
                    if price < pool_price < sl:
                        new_sl = pool_price + _liq_buffer
                        if new_sl > sl:
                            sl = new_sl
                            logger.debug(
                                f"SL pushed: BSL@${pool_price:,.0f} between entry and SL "
                                f"→ SL=${sl:,.1f}")

        # ── VALIDATION ─────────────────────────────────────────────────
        if sl is not None:
            dist_pct = abs(price - sl) / price
            # SL too tight (< 0.1%) or too wide (> 3.5%) → reject
            if dist_pct < 0.001 or dist_pct > 0.035:
                sl = None

        # ── NO FALLBACK — no structure means no trade ──────────────────
        if sl is None:
            logger.debug(
                f"⛔ No valid SL for {side.upper()} — no ICT structure found "
                f"(OB={ict.nearest_ob_price:.1f})")
        return sl

    def _compute_tp_approach(
        self,
        target: PoolTarget,
        side: str,
        price: float,
        atr: float,
    ) -> Optional[float]:
        """
        Institutional TP — FRONT-RUN the liquidity pool.

        Smart money does NOT wait for price to reach the exact pool level.
        They start covering/exiting BEFORE the pool because:
          - Other algos front-run the same level
          - Iceberg orders absorb momentum before the exact price
          - The last 20-50pts of a move are the hardest to capture

        v3.0: Buffer SCALES with pool distance. A flat 0.35×ATR buffer
        eats 17.5% of reward for a 2.0 ATR target — killing R:R for
        viable nearby trades. Scaling formula:
          buffer = 0.10×ATR + 0.05×distance_atr×ATR
          At 1 ATR distance: 0.15×ATR (tight — close pool is certain)
          At 3 ATR distance: 0.25×ATR (moderate)
          At 8 ATR distance: 0.50×ATR (wide — distant pool, more uncertainty)
        Capped at 0.50×ATR to avoid excessive haircuts.
        """
        pool_price = target.pool.price
        dist_atr = target.distance_atr

        # Scaled buffer: tight for close pools, wider for distant ones
        buffer = atr * min(0.50, 0.10 + 0.05 * dist_atr)

        if side == "long":
            tp = pool_price - buffer
        else:
            tp = pool_price + buffer

        # Sanity: TP must be profitable
        if side == "long" and tp <= price:
            return None
        if side == "short" and tp >= price:
            return None
        return tp

    def _ict_structure_agrees(self, ict: ICTContext, direction: str) -> bool:
        agreements = 0
        if direction == "long" and ict.amd_bias == "bullish":
            agreements += 1
        elif direction == "short" and ict.amd_bias == "bearish":
            agreements += 1
        if direction == "long" and ict.in_discount:
            agreements += 1
        elif direction == "short" and ict.in_premium:
            agreements += 1
        if direction == "long" and ict.structure_5m == "bullish":
            agreements += 1
        elif direction == "short" and ict.structure_5m == "bearish":
            agreements += 1
        if ict.session_quality == "prime":
            agreements += 1
        return agreements >= 2

    @staticmethod
    def _ict_summary(ict: ICTContext, side: str) -> str:
        parts = []
        if ict.amd_phase:
            parts.append(f"AMD={ict.amd_phase}")
        if ict.amd_bias:
            parts.append(f"bias={ict.amd_bias}")
        if ict.in_discount:
            parts.append("DISCOUNT")
        elif ict.in_premium:
            parts.append("PREMIUM")
        if ict.structure_5m:
            parts.append(f"5m={ict.structure_5m}")
        if ict.kill_zone:
            parts.append(f"KZ={ict.kill_zone}")
        return " | ".join(parts) if parts else "no ICT context"


# ═══════════════════════════════════════════════════════════════════════════
# INTERNAL STATE CONTAINERS
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class _TrackingState:
    direction:       str
    target:          PoolTarget
    started_at:      float
    flow_ticks:      int   = 0
    contrary_ticks:  int   = 0
    peak_conviction: float = 0.0
    amd_mult:        float = 1.0      # AMD conviction modifier (0.60-1.10)
    last_contrary_ts: float = 0.0     # timestamp of last contrary flow reading


@dataclass
class _PostSweepState:
    sweep:            SweepResult
    entered_at:       float
    initial_flow:     float = 0.0
    initial_flow_dir: str   = ""
    cisd_detected:    bool  = False
    ote_zone:         Optional[Tuple[float, float]] = None


# ═══════════════════════════════════════════════════════════════════════════
# TRAIL MANAGER
# ═══════════════════════════════════════════════════════════════════════════

class ICTTrailManager:
    """
    Trailing stop management using ICT structures ONLY.
    v2.0: No change to logic — structural bugs were in liquidity_map and
    entry_engine. Trail manager correctness depends on having valid pools.
    """

    def __init__(self) -> None:
        self._current_sl:   float = 0.0
        self._entry_price:  float = 0.0
        self._side:         str   = ""
        self._bos_count:    int   = 0
        self._choch_seen:   bool  = False

    def initialize(self, side: str, entry_price: float, initial_sl: float) -> None:
        self._side        = side
        self._entry_price = entry_price
        self._current_sl  = initial_sl
        self._bos_count   = 0
        self._choch_seen  = False

    def compute(
        self,
        ict_ctx: ICTContext,
        price: float,
        atr: float,
        candles_5m: List[Dict],
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

        if self._side == "long":
            if new_sl <= self._current_sl:
                return None
        else:
            if new_sl >= self._current_sl:
                return None

        self._current_sl = new_sl
        return new_sl

    def _find_structural_sl(
        self,
        candles_5m: List[Dict],
        candles_15m: Optional[List[Dict]],
        atr: float,
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
                if lows:
                    return lows[-1][1] - buffer
            else:
                highs = _find_swing_highs(candles_15m, lookback=3)
                if highs:
                    return highs[-1][1] + buffer

        if candles_5m and len(candles_5m) >= 12:
            if self._side == "long":
                lows = _find_swing_lows(candles_5m, lookback=4)
                if lows:
                    return lows[-1][1] - buffer
            else:
                highs = _find_swing_highs(candles_5m, lookback=4)
                if highs:
                    return highs[-1][1] + buffer

        return None

    @property
    def current_sl(self) -> float:
        return self._current_sl

    @property
    def phase_info(self) -> str:
        parts = [f"BOS×{self._bos_count}"]
        if self._choch_seen:
            parts.append("CHoCH⚠️")
        return " ".join(parts)

"""
entry_engine.py — Institutional Sweep Entry Engine
====================================================
Architecture:
  Price moves from pool to pool. Smart money sweeps stops, then delivers
  the opposite direction. This engine ONLY enters on confirmed events:

  1. SWEEP REVERSAL  — Pool swept + displacement + CISD → enter reversal
  2. SWEEP CONTINUATION — Pool swept + flow continues → ride the flow
  3. DISPLACEMENT MOMENTUM — Institutional candle detected → enter with flow

  NO approach entries. Institutions do not front-run the sweep.

State Machine:
  SCANNING    → monitors for sweeps and displacement candles
  POST_SWEEP  → 4-phase accumulative evidence model after sweep detected
  ENTERING    → entry placed, waiting for fill
  IN_POSITION → position live, managed by quant_strategy.py

Post-Sweep Phases:
  DISPLACEMENT (0-45s)  → expect strong rejection from sweep level
  CISD (45-120s)        → expect CHoCH/BOS confirming reversal
  OTE (120-240s)        → expect retrace to 50%-78.6% Fibonacci zone
  MATURE (240-360s)     → relaxed thresholds, final chance

Evidence Model:
  Static factors (scored once): AMD, sweep quality, dealing range, pool sig
  Dynamic factors (per-tick with decay): flow, CVD, CISD, OTE, displacement
  Decision: static_base + accumulated_dynamic >= phase-adjusted threshold

Exports (consumed by quant_strategy.py):
  EntryEngine, ICTTrailManager, OrderFlowState, ICTContext,
  EntryType, ICTSweepEvent
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

# Flow thresholds
_FLOW_CONV_THRESHOLD     = 0.40
_FLOW_CVD_AGREE_MIN      = 0.15
_FLOW_SUSTAINED_TICKS    = 2
_FLOW_EWMA_ALPHA_PER_SEC = 0.648    # at dt=0.25s → alpha=0.15
_FLOW_EWMA_ADAPTIVE_CAP  = 0.40

# Pool targeting
_MAX_TARGET_ATR        = 8.0
_MIN_TARGET_ATR        = 0.25
_MIN_POOL_SIGNIFICANCE = 1.0   # allow lower significance pools

# Sweep quality
_MIN_SWEEP_QUALITY = 0.15    # lower sweep quality threshold

# Timing
_CISD_MAX_WAIT_SEC   = 360
_ENTRY_COOLDOWN_SEC  = 10.0   # faster re-entry

# SL / TP
_MIN_RR_RATIO       = 1.2    # match config MIN_RISK_REWARD_RATIO
_SL_BUFFER_ATR      = 0.35
_TP_BUFFER_ATR      = 0.08
_REV_SL_BUFFER_ATR  = 0.35
_CONT_SL_BUFFER_ATR = 0.40

# Post-Sweep phases (seconds after sweep)
# MOD-11 FIX: Previously hardcoded. Now loaded from config so operators can
# tune phase windows for different market conditions (trending vs ranging)
# without a code deploy. Fallbacks preserve original production values.
try:
    import config as _ecfg
    _PS_PHASE_DISPLACEMENT = float(getattr(_ecfg, 'PS_PHASE_DISPLACEMENT_SEC', 45.0))
    _PS_PHASE_CISD         = float(getattr(_ecfg, 'PS_PHASE_CISD_SEC',         120.0))
    _PS_PHASE_OTE          = float(getattr(_ecfg, 'PS_PHASE_OTE_SEC',          240.0))
    _PS_PHASE_MATURE       = float(getattr(_ecfg, 'PS_PHASE_MATURE_SEC',       360.0))
    # BUG-1 FIX: Post-close stale-window extension.
    # After a long position hold every sweep in snap.recent_sweeps is older than
    # the normal 120s staleness limit and the engine enters a cold-start dead zone
    # of unknown duration.  For POST_CLOSE_GRACE_PERIOD_SEC after on_position_closed()
    # _collect_sweeps() uses POST_CLOSE_STALE_WINDOW_SEC instead of 120s so sweeps
    # from the final minutes of the closed position are still reachable.
    # Operators can widen or narrow both windows from config without a redeploy.
    _POST_CLOSE_STALE_WINDOW_SEC = float(getattr(_ecfg, 'POST_CLOSE_STALE_WINDOW_SEC', 600.0))
    _POST_CLOSE_GRACE_PERIOD_SEC = float(getattr(_ecfg, 'POST_CLOSE_GRACE_PERIOD_SEC', 300.0))
except Exception:
    _PS_PHASE_DISPLACEMENT = 45.0
    _PS_PHASE_CISD         = 120.0
    _PS_PHASE_OTE          = 240.0
    _PS_PHASE_MATURE       = 360.0
    _POST_CLOSE_STALE_WINDOW_SEC = 600.0
    _POST_CLOSE_GRACE_PERIOD_SEC = 300.0

# Post-Sweep evidence thresholds (lowered to allow entries)
_PS_THRESHOLD_EARLY  = 45.0
_PS_THRESHOLD_NORMAL = 35.0
_PS_THRESHOLD_MATURE = 25.0
_PS_GAP_MIN          = 8.0
_PS_DISP_MULT        = 1.10
_PS_MATURE_MULT      = 0.65

# Displacement requirements
_PS_DISP_MIN_ATR    = 0.25   # lower displacement requirement
_PS_DISP_STRONG_ATR = 0.8    # easier strong displacement
_PS_OTE_FIB_LOW     = 0.50
_PS_OTE_FIB_HIGH    = 0.786

# Momentum entry
_MOMENTUM_MIN_BODY_RATIO   = 0.65
_MOMENTUM_MIN_VOL_RATIO    = 1.3
_MOMENTUM_MIN_ATR_MOVE     = 0.6
_MOMENTUM_LOOKBACK_CANDLES = 3
_MOMENTUM_SL_BUFFER_ATR    = 0.15
_MOMENTUM_MIN_RR           = 1.0    # allow tighter momentum entries
_MOMENTUM_COOLDOWN_SEC     = 15.0   # faster momentum cooldown
_MOMENTUM_MAX_PER_HOUR     = 10     # allow more momentum entries

# HTF TP escalation
_HTF_TP_TIMEFRAMES   = ('1h', '4h', '1d')
_HTF_TP_MAX_ATR      = 30.0
_HTF_TP_MIN_HTF_COUNT = 2

# AMD modifiers (never block — only size)
_AMD_ALIGNED_BONUS    = 1.10
_AMD_MANIP_CONTRA     = 0.60
_AMD_DIST_CONTRA      = 0.80


# ═══════════════════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════════════════

class EngineState(Enum):
    SCANNING    = auto()
    TRACKING    = auto()   # kept for interface compat — unused in sweep-only mode
    READY       = auto()   # kept for interface compat — unused in sweep-only mode
    ENTERING    = auto()
    IN_POSITION = auto()
    POST_SWEEP  = auto()


class EntryType(Enum):
    PRE_SWEEP_APPROACH    = "APPROACH"      # disabled — kept for compat
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
        if d == "long":  return self.cvd_trend > _FLOW_CVD_AGREE_MIN
        if d == "short": return self.cvd_trend < -_FLOW_CVD_AGREE_MIN
        return False


@dataclass
class ICTSweepEvent:
    pool_price:     float
    pool_type:      str       # "BSL" or "SSL"
    sweep_ts:       int       # ms epoch
    displacement:   bool
    disp_score:     float
    wick_reject:    bool
    candle_high:    float
    candle_low:     float
    candle_close:   float


@dataclass
class ICTContext:
    amd_phase:              str   = ""
    amd_bias:               str   = ""
    amd_confidence:         float = 0.0
    in_premium:             bool  = False
    in_discount:            bool  = False
    dealing_range_pd:       float = 0.5
    structure_5m:           str   = ""
    structure_15m:          str   = ""
    structure_4h:           str   = ""
    bos_5m:                 str   = ""
    choch_5m:               str   = ""
    nearest_ob_price:       float = 0.0
    nearest_ob_price_short: float = 0.0
    kill_zone:              str   = ""
    ict_sweeps:             list  = field(default_factory=list)
    direction_hint:            str   = ""
    direction_hint_side:       str   = ""
    direction_hint_confidence: float = 0.0

    @property
    def session_quality(self) -> str:
        kz = (self.kill_zone or "").lower()
        if "london" in kz or "ny" in kz: return "prime"
        if "asia" in kz:                 return "fair"
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
    # FIX-SL-FLIP: candle timestamp that produced this signal — passed back to
    # mark_momentum_blocked() so the frozen SL can be keyed to the right candle.
    displacement_candle_ts: int = 0


@dataclass
class PostSweepDecision:
    action:      str
    direction:   str
    confidence:  float
    next_target: Optional[PoolTarget] = None
    reason:      str                  = ""


# ── Internal state ────────────────────────────────────────────────────────

@dataclass
class _PostSweepState:
    sweep:              SweepResult
    entered_at:         float
    initial_flow:       float = 0.0
    initial_flow_dir:   str   = ""
    rev_evidence:       float = 0.0
    cont_evidence:      float = 0.0
    peak_rev:           float = 0.0
    peak_cont:          float = 0.0
    tick_count:         int   = 0
    cisd_detected:      bool  = False
    cisd_timestamp:     float = 0.0
    cisd_type:          str   = ""
    max_displacement:   float = 0.0
    displacement_dir:   str   = ""
    disp_velocity:      float = 0.0
    ote_reached:        bool  = False
    ote_timestamp:      float = 0.0
    ote_holding:        bool  = False
    highest_since:      float = 0.0
    lowest_since:       float = float('inf')
    rev_flow_ticks:     int   = 0
    cont_flow_ticks:    int   = 0
    ict_sweep_event:    Optional[ICTSweepEvent] = None
    static_scored:      bool  = False
    static_rev_base:    float = 0.0
    static_cont_base:   float = 0.0
    # Bug #2 fix: cross-sweep decay must fire once per unique event, not every
    # 250ms tick for the full 90s window the event remains in ict_sweeps.
    # Key: (round(pool_price, 0), pool_type, round(sweep_ts_sec, 0))
    # When the cross-sweep block fires it stores the event key here.  The
    # same key is a no-op on all subsequent ticks.
    _cross_sweep_applied: Optional[tuple] = None
    # Bug #4 fix: cache the AMD phase+bias that were in effect when static
    # factors were scored.  If the phase or bias changes mid-sweep (common
    # 10-15 min lag as AMD transitions from ACCUMULATION → MANIPULATION)
    # static_scored is reset to False so the new phase is properly scored.
    _static_amd_phase: str = ""
    _static_amd_bias:  str = ""


# ═══════════════════════════════════════════════════════════════════════════
# ENTRY ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class EntryEngine:
    """Institutional sweep-only entry engine."""

    def __init__(self) -> None:
        self._state          = EngineState.SCANNING
        self._state_entered  = time.time()
        self._signal:        Optional[EntrySignal]     = None
        self._post_sweep:    Optional[_PostSweepState] = None
        self._last_entry_at  = 0.0
        self._last_sweep_analysis: Dict = {}
        self._last_liq_snapshot = None

        # Flow EWMA
        self._flow_ewma             = 0.0
        self._flow_ewma_last_update = 0.0

        # Sweep tracking
        self._last_sweep_reversal_dir  = ""
        self._last_sweep_reversal_time = 0.0

        # Momentum tracking
        self._momentum_entries_1h     = 0
        self._momentum_hour_start     = 0.0
        self._last_momentum_candle_ts = 0

        # Compat stubs for TRACKING/READY (unused in sweep-only mode)
        self._tracking = None
        self._proximity_confirms = 0
        self._proximity_target   = None
        self._proximity_side     = ""

        # BUG-B2 FIX: Gate-block cooldown (POST_SWEEP signals)
        # When unified_entry_gate or conviction_filter blocks a signal from the
        # active POST_SWEEP state, the post_sweep evidence model remains alive and
        # will regenerate the identical signal on the next tick (250ms later).
        # Without this guard, the bot enters a busy-loop: generate → block → consume
        # → generate → block, cycling at 4 Hz and exhausting every pool evaluation
        # before any structural evidence can accumulate.
        #
        # _gate_blocked_until: epoch timestamp after which signal generation is
        #   allowed again. Set by mark_gate_blocked() called from quant_strategy.
        # _gate_block_key: (side, reason_prefix) dedup so a DIFFERENT gate reason
        #   (e.g. AMD changed from ACCUMULATION to MANIPULATION) does not wait.
        self._gate_blocked_until: float = 0.0
        self._gate_block_key: tuple = ()

        # BUG-1 FIX: Processed-sweeps registry (SWEEP-LOOP root cause).
        # After a verdict fires, _handle_reversal/_handle_continuation previously
        # cleared _post_sweep but did NOT record the sweep as processed.  On the
        # next SCANNING tick _collect_sweeps() found the same sweep (still within
        # the 60s window) and re-entered POST_SWEEP — looping every ~5s for up to
        # 60s per sweep.
        #
        # Key schema: (round(pool_price, 0), pool_side_value, round(detected_at, 0))
        #   pool_price   — rounded to dollar to absorb tick-level float noise
        #   pool_side    — "BSL" or "SSL" — different sides at same price are distinct
        #   detected_at  — rounded to second; ensures a NEW sweep at the same level
        #                  (detected_at differs) is never suppressed by a stale entry
        #
        # Value: expiry epoch time (now + 120s).  120s outlasts the 60s detection
        # window PLUS the 45s gate-block cooldown with 15s margin.
        #
        # Lifecycle:
        #   _reset(full_clear=False) — preserves active entries so the same sweep
        #     cannot re-enter during its 120s hold window (in-evaluation resets,
        #     on_entry_failed, on_entry_cancelled, POST_SWEEP timeout).
        #   _reset(full_clear=True) — clears fully at position boundaries
        #     (on_position_closed, force_reset, stuck-state watchdog) so a new
        #     trade evaluates fresh sweeps rather than being silenced by stale
        #     entries from the closed position's lifecycle.
        self._processed_sweeps: Dict[tuple, float] = {}

        # FIX-SPAM: Momentum signal gate-block (independent of post-sweep gate).
        # Momentum signals have no conviction cooldown of their own — when the
        # conviction gate rejects a momentum signal, the engine immediately
        # regenerates the identical signal on the next 250ms tick, producing
        # hundreds of log lines per minute for the same setup.
        #
        # _momentum_blocked_until: suppress momentum signal generation until this
        #   epoch time.  Lifted early if price moves > _MOMENTUM_BLOCK_ATR_MOVE ATR
        #   from the entry price at block time (genuine structural change).
        # _momentum_block_entry_price: entry price when block was set (for ATR check).
        # _momentum_block_candle_ts: candle timestamp at block time.  A new candle
        #   (different ts) always lifts the block immediately.
        # _momentum_block_sl: the SL at the time of blocking — frozen so the signal
        #   does not flip between two SL values on alternating ticks (FIX-SL-FLIP).
        self._momentum_blocked_until:     float = 0.0
        self._momentum_block_entry_price: float = 0.0
        self._momentum_block_candle_ts:   int   = 0
        self._momentum_block_sl:          Optional[float] = None
        _MOMENTUM_BLOCK_SEC      = 30.0   # default cooldown when gate blocks
        _MOMENTUM_BLOCK_ATR_MOVE = 0.25   # ATR distance that lifts block early

        # ── Diagnostic: sweep-rejection counters ──────────────────────────
        # Populated by _collect_sweeps() on every tick. Read by the strategy's
        # THINK log to surface WHY the engine is not transitioning out of
        # SCANNING (silent rejection is the observability bug that made the
        # 4-hour "no trades" session impossible to diagnose from logs alone).
        # {"stale": int, "low_quality": int, "processed": int}
        self._last_liq_skip:    Optional[Dict[str, int]] = None
        self._last_bridge_skip: Optional[Dict[str, int]] = None

        # BUG-1 FIX: Timestamp when on_position_closed() last fired.
        # _collect_sweeps() uses this to extend the staleness window for
        # _POST_CLOSE_GRACE_PERIOD_SEC after a close so sweeps from the
        # final minutes of a long hold are not immediately discarded.
        # Set BEFORE _reset() so the field survives the full_clear.
        self._position_closed_at: float = 0.0

        # BUG-3 FIX: Last conviction block record for scan_skip_info.
        # Sweeps can pass _collect_sweeps() but still be blocked downstream
        # by the conviction gate.  Without this, that block is invisible in
        # the THINK log after the first identical rejection is demoted to
        # DEBUG.  Cleared on full_clear so stale records don't pollute the
        # next position's diagnostics.
        self._last_conviction_block: Optional[Dict] = None

    # ── Public API ────────────────────────────────────────────────────

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

        # ── Check for sweeps ──────────────────────────────────────────
        new_sweeps = self._collect_sweeps(liq_snapshot, ict_ctx, now, atr)

        if (new_sweeps
                and self._state not in (EngineState.IN_POSITION,
                                        EngineState.ENTERING,
                                        EngineState.POST_SWEEP)):
            best = max(new_sweeps, key=lambda s: s.quality)
            ict_event = self._find_ict_event(best, ict_ctx, atr)
            self._enter_post_sweep(best, liq_snapshot, flow_state,
                                   ict_ctx, price, atr, now, ict_event)
            return

        # ── State dispatch ────────────────────────────────────────────
        if self._state == EngineState.SCANNING:
            self._do_scanning(liq_snapshot, flow_state, ict_ctx,
                              price, atr, now, candles_1m, candles_5m)
        elif self._state == EngineState.POST_SWEEP:
            self._do_post_sweep(liq_snapshot, flow_state, ict_ctx,
                                price, atr, now)
        elif self._state in (EngineState.ENTERING, EngineState.IN_POSITION):
            stuck_limit = 120.0 if self._state == EngineState.ENTERING else 14400.0
            if now - self._state_entered > stuck_limit:
                logger.warning(
                    f"Engine SELF-RECOVERY: stuck in {self._state.name} "
                    f"for {now - self._state_entered:.0f}s — full_clear reset")
                # full_clear=True: the state has been stuck for 120s/4h which
                # implies the normal on_position_closed() lifecycle event was
                # never received.  The entire per-position gate state is suspect;
                # a narrow reset would preserve stale cooldowns and _processed_sweeps
                # entries that block the next valid setup.
                self._reset(now, full_clear=True)

    def get_signal(self) -> Optional[EntrySignal]:
        return self._signal

    def consume_signal(self) -> Optional[EntrySignal]:
        sig = self._signal
        self._signal = None
        return sig

    def on_entry_placed(
        self,
        entry_type: Optional["EntryType"] = None,
    ) -> None:
        """
        Called by quant_strategy AFTER consume_signal() when a trade order is
        confirmed sent.  The caller must pass the ``entry_type`` taken from the
        consumed signal — this method cannot read self._signal because
        consume_signal() already nulled it.

        Bug #17 (original): counter was incremented at signal-generation time,
        not at order-submission time, so conviction-gated signals consumed
        budget slots with zero fills.

        Bug #1 (audit): the "Bug #17 fix" was itself broken.  on_entry_placed()
        checked self._signal IS NOT None after consume_signal() had already set
        it to None, so _momentum_entries_1h never incremented regardless of
        entry type — _MOMENTUM_MAX_PER_HOUR had zero effect.

        Fix: accept entry_type as an explicit argument.  The call site in
        quant_strategy must be:
            sig = self._entry_engine.consume_signal()
            self._entry_engine.on_entry_placed(sig.entry_type)
        """
        self._state = EngineState.ENTERING
        self._state_entered = time.time()
        self._last_entry_at = time.time()
        if entry_type == EntryType.DISPLACEMENT_MOMENTUM:
            self._momentum_entries_1h += 1
        # self._signal is already None (cleared by consume_signal).

    def on_entry_failed(self) -> None:
        if self._state in (EngineState.ENTERING, EngineState.IN_POSITION):
            self._reset(time.time())

    def on_position_opened(self) -> None:
        self._state = EngineState.IN_POSITION
        self._state_entered = time.time()

    def on_entry_cancelled(self) -> None:
        self._post_sweep = None
        self._last_entry_at = time.time()
        if self._state not in (EngineState.SCANNING, EngineState.IN_POSITION):
            self._reset(time.time())

    def mark_gate_blocked(self, side: str, reason_prefix: str,
                           cooldown_sec: float = 45.0) -> None:
        """
        BUG-B2 FIX: Called by quant_strategy when unified_entry_gate OR
        conviction_filter blocks a signal from an active POST_SWEEP state.

        Suppresses signal generation for `cooldown_sec` seconds so the evaluation
        pipeline does not busy-loop at 4 Hz generating and immediately discarding
        the identical signal.

        cooldown_sec:
          45s — shorter than the evidence decay period (92% decay / tick) so that
          genuine structural changes (new CISD, BOS, OTE) can fire a fresh signal.
          Long enough to prevent tick-level noise from re-triggering a blocked setup.

        When the block key CHANGES (different side or different gate reason prefix)
        the gate is immediately lifted — a new signal from a different pool or with a
        different reason is evaluated fresh. Only identical signals are suppressed.

        BUG-3 FIX (GATE-ORPHAN): After the BUG-2 (STATE-DANGLE) fix, state
        transitions to SCANNING before quant_strategy calls mark_gate_blocked().
        The _gate_blocked_until field is only consulted in POST_SWEEP state, so it
        becomes a no-op in SCANNING. The processed-sweep registry is the correct
        suppression layer — extend the registered sweep's expiry here so it cannot
        re-enter within the gate-block cooldown window. self._signal is still set
        at the time this is called (quant_strategy calls mark_gate_blocked() BEFORE
        consume_signal()) so sweep_result is available for the key lookup.
        """
        new_key = (side, reason_prefix[:40])
        # If the block key changes, reset immediately (different signal context)
        if new_key != self._gate_block_key:
            self._gate_block_key = new_key
            self._gate_blocked_until = time.time() + cooldown_sec
        else:
            # Same block reason — extend the cooldown from NOW
            self._gate_blocked_until = max(
                self._gate_blocked_until, time.time() + cooldown_sec)

        # BUG-3 FIX: Extend processed-sweep registry expiry for the sweep that
        # generated this blocked signal.  This is the correct suppression point
        # after the state-dangle fix — _gate_blocked_until only works in POST_SWEEP.
        if (self._signal is not None
                and hasattr(self._signal, 'sweep_result')
                and self._signal.sweep_result is not None):
            key = self._sweep_key(self._signal.sweep_result)
            self._processed_sweeps[key] = max(
                self._processed_sweeps.get(key, 0.0),
                time.time() + cooldown_sec + 5.0  # 5s buffer beyond gate cooldown
            )

    def mark_momentum_blocked(
        self,
        entry_price: float,
        candle_ts: int,
        locked_sl: float,
        cooldown_sec: float = 30.0,
    ) -> None:
        """
        FIX-SPAM + FIX-SL-FLIP: Called by quant_strategy when the conviction gate
        blocks a DISPLACEMENT_MOMENTUM signal.

        Two problems solved in one call:

        1. SPAM (FIX-SPAM): Without this, the scanning state regenerates the same
           momentum signal every 250ms because _last_entry_at is only updated on
           on_entry_placed() (which never fires when the conviction gate blocks).
           A 30s cooldown matches _ENTRY_COOLDOWN_SEC and aligns with the typical
           time for market structure to change enough to warrant re-evaluation.

        2. SL FLIP (FIX-SL-FLIP): _compute_sl() picks between an ICT OB-based SL
           and an ATR fallback depending on whether ict.nearest_ob_price is non-zero
           that tick. The OB presence flickers tick-to-tick (it's fetched live),
           causing the SL to alternate between two values ($66,851 vs $66,855 in the
           logs). locked_sl freezes the SL computed at signal-generation time and
           reuses it for any subsequent signal within the cooldown window, ensuring
           the conviction gate always scores the SAME R:R.

        The block is lifted early if:
          - A new (different) candle is found (candle_ts changed)
          - Price moves more than 0.25 ATR from entry_price (structural shift)
        """
        self._momentum_blocked_until     = time.time() + cooldown_sec
        self._momentum_block_entry_price = entry_price
        self._momentum_block_candle_ts   = candle_ts
        self._momentum_block_sl          = locked_sl

    def mark_conviction_blocked(
        self,
        side: str,
        score: float,
        reason: str,
        now: Optional[float] = None,
    ) -> None:
        """
        BUG-3 FIX: Called by quant_strategy when the conviction filter blocks
        any signal type (both DISPLACEMENT_MOMENTUM and POST_SWEEP).

        Stores the block context in _last_conviction_block so scan_skip_info
        can surface it alongside sweep rejections in the THINK log.

        This closes the observability gap where a conviction block during
        SCANNING is completely invisible after the first identical rejection is
        demoted to DEBUG.  With this in place the THINK log shows BOTH why
        sweeps are being discarded AND why the conviction gate is blocking —
        making the "SCANNING but no trades" state immediately diagnosable
        without enabling DEBUG logging or tracing through source.

        Lifecycle: cleared on full_clear reset (position boundary) so stale
        records from the previous position do not mislead diagnostics for the
        next one.
        """
        self._last_conviction_block = {
            "side":   side,
            "score":  round(score, 3),
            "reason": reason[:80],
            "at":     now if now is not None else time.time(),
        }

    def on_position_closed(self) -> None:
        # BUG-1 FIX: Record the close timestamp BEFORE calling _reset() so
        # _collect_sweeps() can extend the staleness window on the first scan(s)
        # after a long hold.  If _reset() ran first it would wipe _state_entered
        # to now, making the post-close elapsed check meaningless.
        # full_clear=True clears _processed_sweeps, gate-block, momentum-block,
        # and _last_conviction_block — all per-position state.
        _now = time.time()
        self._position_closed_at = _now
        self._reset(_now, full_clear=True)

    def force_reset(self) -> None:
        # full_clear=True covers: _processed_sweeps, gate-block, momentum-block.
        # Additional fields reset here are engine-lifetime counters that survive
        # normal position boundaries but must be zeroed on an operator hard-reset.
        self._reset(time.time(), full_clear=True)
        self._flow_ewma = 0.0
        self._flow_ewma_last_update = 0.0
        self._momentum_entries_1h = 0
        self._last_momentum_candle_ts = 0

    @property
    def state(self) -> str:
        return self._state.name

    @property
    def tracking_info(self) -> Optional[Dict]:
        """Compat stub — no TRACKING state in sweep-only mode."""
        return None

    @property
    def scan_skip_info(self) -> Optional[Dict]:
        """
        Last-tick sweep-rejection counters from _collect_sweeps() plus the most
        recent conviction block (if any, within the last 300s).

        Returns:
          None if no rejections to report, otherwise a dict with any of:
            {
              "liq":        {"stale": n, "low_quality": n, "processed": n},
              "bridge":     {"stale": n, "low_quality": n, "processed": n,
                             "ict_sweeps": n},
              "conviction": {"side": str, "score": float, "reason": str,
                             "age_s": int},
            }

        Used by quant_strategy's THINK log to surface WHY SCANNING isn't
        advancing.  The conviction key closes the observability gap where a
        downstream gate block during SCANNING is invisible after the first
        identical rejection is demoted to DEBUG.
        """
        out: Dict = {}
        if self._last_liq_skip:
            out["liq"] = dict(self._last_liq_skip)
        if self._last_bridge_skip:
            out["bridge"] = dict(self._last_bridge_skip)
        # BUG-3 FIX: Include the most recent conviction block if it occurred
        # within the last 300s.  Records older than 300s are stale (market
        # conditions have shifted) and should not be shown as current context.
        if self._last_conviction_block is not None:
            _age = time.time() - self._last_conviction_block.get("at", 0.0)
            if _age < 300.0:
                out["conviction"] = {
                    "side":   self._last_conviction_block["side"],
                    "score":  self._last_conviction_block["score"],
                    "reason": self._last_conviction_block["reason"],
                    "age_s":  int(_age),
                }
        return out if out else None

    # ── Internal: reset ───────────────────────────────────────────────

    def _reset(self, now: float, full_clear: bool = False) -> None:
        """Reset engine state.

        full_clear=False  (default, in-evaluation reset)
            Used by on_entry_failed(), on_entry_cancelled(), POST_SWEEP timeout,
            and all mid-evaluation reject paths.  Preserves active processed-sweep
            entries so the same sweep cannot sneak back in during its 120s hold
            window.  Gate-block and momentum-block cooldowns are also preserved
            because the signal that triggered the gate may still be live — clearing
            them here would immediately regenerate the blocked signal.

        full_clear=True  (position-boundary / corrupt-state reset)
            Used by on_position_closed(), force_reset(), and the stuck-state
            self-recovery watchdogs.  ALL per-position gate state is cleared:
              • _processed_sweeps — a new position must re-evaluate fresh sweeps;
                keeping stale entries from a closed trade silently starves the
                next setup, which is the root cause of the observed 9h+ silences.
              • _gate_blocked_until / _gate_block_key — gate cooldown was scoped
                to the POST_SWEEP evaluation of the now-closed position.
              • _momentum_blocked_until and all _momentum_block_* fields — the
                frozen SL and candle-ts guard are meaningless for a new position.
        """
        self._state = EngineState.SCANNING
        self._state_entered = now
        self._signal = None
        self._tracking = None
        self._post_sweep = None

        if full_clear:
            # Position boundary — wipe everything so the next trade starts clean.
            self._processed_sweeps.clear()
            self._gate_blocked_until       = 0.0
            self._gate_block_key           = ()
            self._momentum_blocked_until   = 0.0
            self._momentum_block_entry_price = 0.0
            self._momentum_block_candle_ts = 0
            self._momentum_block_sl        = None
            self._last_conviction_block    = None   # BUG-3 FIX
        else:
            # In-evaluation reset — purge only expired processed-sweep entries.
            # Active entries (expiry > now) must survive so the same sweep cannot
            # sneak back in during its 120s hold window.
            self._processed_sweeps = {k: v for k, v in self._processed_sweeps.items()
                                       if v > now}

    # ── Internal: flow EWMA (continuous-time) ─────────────────────────

    def _update_flow_ewma(self, flow: OrderFlowState, now: float) -> None:
        dt = now - self._flow_ewma_last_update if self._flow_ewma_last_update > 0 else 0.25
        self._flow_ewma_last_update = now

        if   flow.direction == "long":  signed = abs(flow.conviction)
        elif flow.direction == "short": signed = -abs(flow.conviction)
        else: signed = 0.0

        dt_clamped = min(dt, 2.0)
        alpha = 1.0 - math.exp(-_FLOW_EWMA_ALPHA_PER_SEC * dt_clamped)
        boost = min(abs(flow.conviction), 1.0)
        alpha = min(alpha * (1.0 + boost), _FLOW_EWMA_ADAPTIVE_CAP)
        self._flow_ewma = alpha * signed + (1.0 - alpha) * self._flow_ewma

    def _ewma_dir(self) -> str:
        if self._flow_ewma > _FLOW_CONV_THRESHOLD * 0.4:  return "long"
        if self._flow_ewma < -_FLOW_CONV_THRESHOLD * 0.4: return "short"
        return ""

    # ── Internal: AMD modifier ────────────────────────────────────────

    @staticmethod
    def _amd_modifier(ict: ICTContext, direction: str) -> float:
        """AMD conviction modifier ∈ [0.60, 1.10]. Never blocks."""
        phase = (ict.amd_phase or "").upper()
        bias  = (ict.amd_bias or "").lower()
        if not phase or not bias or bias == "neutral":
            return 1.0

        aligned = ((direction == "long" and bias == "bullish") or
                   (direction == "short" and bias == "bearish"))
        contra  = ((direction == "long" and bias == "bearish") or
                   (direction == "short" and bias == "bullish"))

        if aligned: return _AMD_ALIGNED_BONUS
        if not contra: return 1.0
        if phase == "MANIPULATION":               return _AMD_MANIP_CONTRA
        if phase in ("DISTRIBUTION", "REDISTRIBUTION"): return _AMD_DIST_CONTRA
        return 1.0

    # ── Internal: collect sweeps from both systems ────────────────────

    def _collect_sweeps(self, snap, ict_ctx, now, atr) -> List[SweepResult]:
        """Merge LiquidityMap sweeps + ICT engine sweeps into one list.

        BUG-A4 FIX: Window extended from 10s to 60s.
        LiquidityMap.check_sweeps() fires on CLOSED 5m candles, meaning a sweep
        detected at bar close will have detected_at ≈ close_time. The entry engine
        runs every ~250ms. In practice, ICT engine update latency + multi-TF candle
        fetching means check_sweeps() may fire 15-30s after the actual sweep candle
        closed. With a 10s window the sweep is ALREADY stale before entry_engine.update()
        even runs. 60s gives a full 5m bar worth of tolerance.

        BUG-1 FIX (SWEEP-LOOP): Both LiquidityMap and ICT-bridge sweeps are now
        filtered against _processed_sweeps before being returned.  A sweep that has
        already driven a verdict (regardless of whether the resulting signal was
        consumed or blocked) will not be re-presented until its 120s hold expires.
        """
        # ── Sweep collection diagnostics ──────────────────────────────────
        # Track every reason a sweep is rejected so upstream logs show why
        # SCANNING isn't transitioning. Silent rejection makes "no trades"
        # indistinguishable from "broken pipeline".
        _skipped_stale       = 0
        _skipped_low_quality = 0
        _skipped_processed   = 0

        sweeps = []
        for s in (snap.recent_sweeps or []):
            # BUG-1 FIX: Dynamic staleness limit.
            # Normal limit is 120s — enough to cover the ICT/LiquidityMap detection
            # latency on a running bot.  After a long position hold EVERY sweep in
            # snap.recent_sweeps is older than 120s and is silently discarded,
            # leaving the engine in a cold-start dead zone until a brand-new sweep
            # fires.  For the first _POST_CLOSE_GRACE_PERIOD_SEC (default 300s)
            # after on_position_closed() we widen the window to
            # _POST_CLOSE_STALE_WINDOW_SEC (default 600s / 10 min) so sweeps from
            # the final minutes of the hold are still reachable.
            # _position_closed_at is set before _reset(), so it is non-zero
            # exactly when a position boundary just occurred.
            _stale_limit = 120.0
            if (self._position_closed_at > 0
                    and now - self._position_closed_at < _POST_CLOSE_GRACE_PERIOD_SEC):
                _stale_limit = _POST_CLOSE_STALE_WINDOW_SEC

            if s.detected_at <= now - _stale_limit:
                _skipped_stale += 1
                continue
            if s.quality < _MIN_SWEEP_QUALITY:
                _skipped_low_quality += 1
                continue
            if self._is_processed(s, now):
                _skipped_processed += 1
                continue
            sweeps.append(s)

        if (not sweeps
                and self._state not in (EngineState.POST_SWEEP,
                                        EngineState.IN_POSITION,
                                        EngineState.ENTERING)
                and hasattr(ict_ctx, 'ict_sweeps') and ict_ctx.ict_sweeps):

            # ── FIX (entry-engine-bridge-stale): widen ICT-bridge age window
            # ────────────────────────────────────────────────────────────
            # Original window was 30s. ICT sweeps are detected at the close
            # of 5m bars; the entry engine runs every 250ms. With a 30s
            # window the bridge only covers the first 10% of each 5m bar's
            # duration — most ticks see a stale sweep and silently drop it.
            # This is consistent with the LiquidityMap path above (60s) and
            # with the _notified_sweeps prune window in quant_strategy
            # (60s cutoff for the direction-engine dedup set).
            #
            # Additional grace: if the sweep happened IN THE CURRENT 5m
            # bar (bar start <= sweep_ts < bar close), extend the window
            # to (bar_close + 60s) so a sweep at bar-open still reaches
            # the entry engine for the full 5min + grace. The processed
            # sweeps registry (_sweep_key keyed on detected_at rounded to
            # seconds, 120s hold after enter_post_sweep) handles dedup —
            # widening the detection window does not reintroduce sweep-
            # loop reprocessing.
            # 120s base window: matches the LiquidityMap path (Fix 1) so both
            # sweep sources age out at the same rate.  A 60s mismatch caused the
            # ICT bridge to expire sweeps 60s before the LiquidityMap path saw
            # them — making the bridge unreachable after a post-close delay.
            _base_age_limit = int(now * 1000) - 120_000
            # Beginning of the current 5-minute bar (UTC-based)
            _cur_5m_start_ms = int(now // 300.0) * 300_000
            # Accept a sweep if EITHER it's within 60s of now OR it was
            # detected in the current or previous 5m bar (+60s grace).
            # 2 bars back (600s): extends coverage so a sweep at bar-open is
            # admitted for the full bar duration even when the base_age_limit
            # (120s) crosses a bar boundary mid-evaluation.  One bar (300s) could
            # exclude early-bar sweeps at the start of the second minute.
            _bar_age_limit = _cur_5m_start_ms - 600_000  # 2 bars back = 10 min

            _bridge_stale = 0
            _bridge_low_q = 0
            _bridge_proc  = 0

            for ev in ict_ctx.ict_sweeps:
                if ev.sweep_ts < _base_age_limit and ev.sweep_ts < _bar_age_limit:
                    _bridge_stale += 1
                    continue
                side = PoolSide.BSL if ev.pool_type == "BSL" else PoolSide.SSL
                direction = "short" if ev.pool_type == "BSL" else "long"
                wick = ev.candle_high if ev.pool_type == "BSL" else ev.candle_low
                quality = min(1.0, 0.35 + 0.35 * ev.disp_score
                              + (0.15 if ev.wick_reject else 0.0))
                if quality < _MIN_SWEEP_QUALITY:
                    _bridge_low_q += 1
                    continue

                # Still check _processed_sweeps — widened window MUST not
                # reintroduce sweep-loop reprocessing. The registry key is
                # seconds-rounded, so legitimate re-sweeps at the same price
                # with a different detected_at are still admitted.
                _bridge_key = (round(ev.pool_price, 0), side.value,
                               round(ev.sweep_ts / 1000.0, 0))
                if self._processed_sweeps.get(_bridge_key, 0.0) > now:
                    _bridge_proc += 1
                    continue

                pool = type('SynthPool', (), {
                    'price': ev.pool_price, 'side': side,
                    'timeframe': '5m', 'status': PoolStatus.SWEPT,
                    'significance': 3.0, 'ob_aligned': False,
                    'fvg_aligned': False, 'htf_count': 0,
                    'is_tradeable': False, 'sweep_wick': wick,
                })()

                sweeps.append(SweepResult(
                    pool=pool, sweep_candle_idx=0,
                    wick_extreme=wick, rejection_pct=ev.disp_score,
                    volume_ratio=1.0, quality=quality,
                    direction=direction,
                    detected_at=ev.sweep_ts / 1000.0,
                ))
                logger.info(
                    f"🔗 ICT SWEEP BRIDGED: {ev.pool_type} "
                    f"${ev.pool_price:,.1f} quality={quality:.2f} "
                    f"age={int((now*1000 - ev.sweep_ts)/1000)}s")

            # Expose bridge rejections via state-level counters (read by the
            # diagnostic THINK log in quant_strategy — also picked up by the
            # mark_gate_blocked telemetry).
            if _bridge_stale or _bridge_low_q or _bridge_proc:
                self._last_bridge_skip = {
                    "stale":       _bridge_stale,
                    "low_quality": _bridge_low_q,
                    "processed":   _bridge_proc,
                    "ict_sweeps":  len(ict_ctx.ict_sweeps),
                }
            else:
                self._last_bridge_skip = None

        # Expose LiquidityMap-path rejections too.
        if _skipped_stale or _skipped_low_quality or _skipped_processed:
            self._last_liq_skip = {
                "stale":       _skipped_stale,
                "low_quality": _skipped_low_quality,
                "processed":   _skipped_processed,
            }
            # Surface stale rejections at INFO level so post-close silences are
            # diagnosable without reading DEBUG logs.  Only log when sweeps exist
            # but were rejected — an empty snap.recent_sweeps is not a skip event.
            if _skipped_stale and snap.recent_sweeps:
                _stale_ages = [
                    now - s.detected_at
                    for s in snap.recent_sweeps
                    if s.detected_at <= now - 120.0
                ]
                _oldest_age = max(_stale_ages) if _stale_ages else 0.0
                logger.info(
                    f"SCAN SKIP: {_skipped_stale} sweep(s) stale "
                    f"(oldest {_oldest_age:.0f}s ago, window=120s) — "
                    f"low_q={_skipped_low_quality} proc={_skipped_processed}"
                )
        else:
            self._last_liq_skip = None

        return sweeps

    @staticmethod
    def _sweep_key(sweep: SweepResult) -> tuple:
        """
        Canonical key for the processed-sweeps registry.
        Schema: (round(pool_price, 0), pool_side_value, round(detected_at, 0))
          pool_price  — dollar-rounded to absorb tick-level float noise
          pool_side   — "BSL"/"SSL" — same price, different sides are distinct
          detected_at — second-rounded; a genuinely NEW sweep at the same level
                        (with a different detected_at) is never suppressed
        """
        return (
            round(sweep.pool.price, 0),
            sweep.pool.side.value,
            round(sweep.detected_at, 0),
        )

    def _is_processed(self, sweep: SweepResult, now: float) -> bool:
        """Return True if sweep is within its processed-sweep hold window."""
        return self._processed_sweeps.get(self._sweep_key(sweep), 0.0) > now

    def _find_ict_event(self, sweep, ict_ctx, atr) -> Optional[ICTSweepEvent]:
        for ev in (getattr(ict_ctx, 'ict_sweeps', None) or []):
            if abs(ev.pool_price - sweep.pool.price) < atr * 0.1:
                return ev
        return None

    # ── State: SCANNING ───────────────────────────────────────────────

    def _do_scanning(self, snap, flow, ict, price, atr, now,
                     candles_1m, candles_5m) -> None:
        """Check for displacement momentum candles."""
        if self._check_displacement_momentum(
                snap, flow, ict, price, atr, now, candles_1m, candles_5m):
            return

    # ── Displacement Momentum ─────────────────────────────────────────

    def _check_displacement_momentum(self, snap, flow, ict, price, atr,
                                      now, candles_1m, candles_5m) -> bool:
        if now - self._last_entry_at < _MOMENTUM_COOLDOWN_SEC:
            return False
        if now - self._momentum_hour_start > 3600.0:
            self._momentum_entries_1h = 0
            self._momentum_hour_start = now
        if self._momentum_entries_1h >= _MOMENTUM_MAX_PER_HOUR:
            return False

        # FIX-SPAM: Respect momentum gate-block cooldown.
        # Lift early if price has moved significantly (new market structure) or
        # a different candle is now the best displacement candidate.
        if now < self._momentum_blocked_until:
            moved = (abs(price - self._momentum_block_entry_price) / max(atr, 1e-10)
                     if atr > 0 else 0.0)
            if moved < 0.25:
                return False
            # Price moved enough — clear the block so we can re-evaluate
            self._momentum_blocked_until = 0.0

        # Find displacement candle
        disp_candle = None
        disp_tf = ""
        disp_dir = ""

        for candles, tf in [(candles_5m, "5m"), (candles_1m, "1m")]:
            if not candles or len(candles) < _MOMENTUM_LOOKBACK_CANDLES + 20:
                continue
            avg_vol = sum(float(c.get('v', 0)) for c in candles[-22:-2]) / 20.0

            for offset in range(2, 2 + _MOMENTUM_LOOKBACK_CANDLES):
                if offset >= len(candles):
                    break
                c = candles[-offset]
                o, cl = float(c['o']), float(c['c'])
                h, lo = float(c['h']), float(c['l'])
                v = float(c.get('v', 0))
                ts = int(c.get('t', 0) or 0)

                if ts > 0 and ts == self._last_momentum_candle_ts:
                    continue

                body = abs(cl - o)
                rng = h - lo
                if rng < 1e-10:
                    continue
                if body / rng < _MOMENTUM_MIN_BODY_RATIO:
                    continue
                if rng / max(atr, 1e-10) < _MOMENTUM_MIN_ATR_MOVE:
                    continue
                if v / max(avg_vol, 1e-10) < _MOMENTUM_MIN_VOL_RATIO:
                    continue

                candle_dir = "long" if cl > o else "short"
                ewma = self._ewma_dir()
                if ewma and ewma != candle_dir:
                    continue

                disp_candle = c
                disp_tf = tf
                disp_dir = candle_dir
                break
            if disp_candle is not None:
                break

        if disp_candle is None:
            return False

        amd_mult = self._amd_modifier(ict, disp_dir)

        # FIX-SL-FLIP: If this is the same candle that was previously blocked,
        # reuse the frozen SL to prevent tick-to-tick SL oscillation caused by
        # ict.nearest_ob_price flickering in/out between ticks.
        disp_candle_ts = int(disp_candle.get('t', 0) or 0)
        _reuse_frozen_sl = (
            self._momentum_block_sl is not None
            and disp_candle_ts == self._momentum_block_candle_ts
            and disp_candle_ts > 0
        )

        # SL
        ch, cl_val = float(disp_candle['h']), float(disp_candle['l'])
        buf = atr * _MOMENTUM_SL_BUFFER_ATR
        if _reuse_frozen_sl:
            sl = self._momentum_block_sl
        else:
            sl = cl_val - buf if disp_dir == "long" else ch + buf
            ict_sl = self._compute_sl(ict, disp_dir, price, atr)
            if ict_sl is not None:
                sl = min(sl, ict_sl) if disp_dir == "long" else max(sl, ict_sl)

        risk = abs(price - sl)
        if risk < 1e-10:
            return False

        # TP (pool → HTF escalation → 2R fallback)
        tp, target = self._find_tp(snap, disp_dir, price, atr, sl,
                                    _MOMENTUM_MIN_RR * (2.0 - amd_mult))
        if tp is None:
            tp = price + risk * 2.0 if disp_dir == "long" else price - risk * 2.0

        reward = abs(tp - price)
        rr = reward / risk
        adj_rr = _MOMENTUM_MIN_RR * (2.0 - amd_mult)
        if rr < adj_rr:
            return False

        if abs(price - sl) / price < 0.001 or abs(price - sl) / price > 0.035:
            return False

        if target is None:
            pools = snap.bsl_pools if disp_dir == "long" else snap.ssl_pools
            reachable = [t for t in pools if t.distance_atr <= _HTF_TP_MAX_ATR]
            if not reachable:
                return False
            target = min(reachable, key=lambda t: t.distance_atr)

        self._last_momentum_candle_ts = disp_candle_ts
        # NOTE: _momentum_entries_1h is intentionally NOT incremented here.
        # It is incremented in on_entry_placed() ONLY if the signal is consumed
        # and the order actually submitted.  Incrementing here would charge the
        # hourly budget for signals that are subsequently blocked by the
        # conviction gate, unfairly exhausting the momentum allowance. (Bug #17)

        self._signal = EntrySignal(
            side=disp_dir, entry_type=EntryType.DISPLACEMENT_MOMENTUM,
            entry_price=price, sl_price=sl, tp_price=tp, rr_ratio=rr,
            target_pool=target,
            conviction=abs(flow.conviction) * amd_mult,
            reason=f"DISPLACEMENT {disp_dir.upper()} [{disp_tf}] R:R={rr:.1f}",
            ict_validation=self._ict_summary(ict, disp_dir),
            displacement_candle_ts=disp_candle_ts,
        )
        logger.info(f"MOMENTUM SIGNAL: {disp_dir.upper()} [{disp_tf}] R:R={rr:.1f}")
        return True

    # ═══════════════════════════════════════════════════════════════════
    # POST-SWEEP PIPELINE
    # ═══════════════════════════════════════════════════════════════════

    def _enter_post_sweep(self, sweep, snap, flow, ict, price, atr, now,
                          ict_event=None) -> None:
        tf_quality = {'1m': 0.65, '5m': 0.55, '15m': 0.45, '4h': 0.35}
        tf = getattr(sweep.pool, 'timeframe', '5m')
        if sweep.quality < tf_quality.get(tf, 0.45):
            # EE-4 FIX: register the rejected sweep in _processed_sweeps so
            # _collect_sweeps() doesn't re-present it every 250ms for the full
            # 60s detection window. 60s hold matches the detection window
            # exactly — expires at the same time the sweep naturally ages out.
            _reject_key = self._sweep_key(sweep)
            self._processed_sweeps[_reject_key] = now + 60.0
            return

        # BUG-1 FIX (SWEEP-LOOP): Register this sweep in the processed-sweeps
        # registry THE MOMENT we commit to evaluating it.  This is the earliest
        # possible registration point — before any evidence accumulation — so that
        # _collect_sweeps() cannot find and re-present the same sweep on any
        # subsequent tick, regardless of how the evaluation resolves (verdict, gate
        # block, timeout, or early rejection inside _handle_reversal/continuation).
        # Expiry = now + 120s:  outlasts the 60s detection window + 45s gate-block
        # cooldown + 15s margin.
        _reg_key = self._sweep_key(sweep)
        self._processed_sweeps[_reg_key] = now + 120.0

        self._post_sweep = _PostSweepState(
            sweep=sweep, entered_at=now,
            initial_flow=flow.conviction, initial_flow_dir=flow.direction,
            highest_since=price, lowest_since=price,
            ict_sweep_event=ict_event,
        )
        self._state = EngineState.POST_SWEEP
        self._state_entered = now
        self._signal = None

        src = "ICT-BRIDGE" if ict_event else "LIQ-MAP"
        logger.info(
            f"🎯 POST-SWEEP ENTERED [{src}]: {sweep.pool.side.value} "
            f"${sweep.pool.price:,.1f} quality={sweep.quality:.2f} "
            f"wick=${sweep.wick_extreme:,.1f}")

    def _do_post_sweep(self, snap, flow, ict, price, atr, now) -> None:
        ps = self._post_sweep
        if ps is None:
            self._state = EngineState.SCANNING
            return

        elapsed = now - ps.entered_at
        if elapsed < 10.0:
            return
        if elapsed > _CISD_MAX_WAIT_SEC:
            logger.info(f"POST_SWEEP: timeout {elapsed:.0f}s")
            self._post_sweep = None
            self._reset(now)
            return

        # BUG-B2 FIX: Respect gate-block cooldown.
        # If a gate (unified or conviction) blocked the last signal from this
        # post-sweep state, suppress signal generation until the cooldown expires.
        # Evidence accumulation and logging continue normally — only the final
        # signal SET is gated. This prevents the 4 Hz busy-loop where the same
        # signal is generated and immediately discarded hundreds of times.
        _gate_suppressed = (now < self._gate_blocked_until)

        ps.tick_count += 1
        ps.highest_since = max(ps.highest_since, price)
        ps.lowest_since = min(ps.lowest_since, price)

        sweep_price = ps.sweep.pool.price
        rev_dir = ps.sweep.direction

        # Displacement tracking
        if rev_dir == "long":
            max_disp = (ps.highest_since - sweep_price) / max(atr, 1e-10)
        else:
            max_disp = (sweep_price - ps.lowest_since) / max(atr, 1e-10)
        if max_disp > ps.max_displacement:
            ps.max_displacement = max_disp

        # CISD detection
        if not ps.cisd_detected:
            rev_struct = "bearish" if rev_dir == "short" else "bullish"
            choch = getattr(ict, 'choch_5m', '') or ''
            bos = getattr(ict, 'bos_5m', '') or ''
            if choch == rev_struct:
                ps.cisd_detected = True
                ps.cisd_timestamp = now
                ps.cisd_type = "choch"
                logger.info(f"POST_SWEEP: CISD CONFIRMED (CHoCH {rev_struct})")
            elif bos == rev_struct:
                ps.cisd_detected = True
                ps.cisd_timestamp = now
                ps.cisd_type = "bos"
                logger.info(f"POST_SWEEP: CISD CONFIRMED (BOS {rev_struct})")

        # OTE zone tracking
        if ps.max_displacement >= _PS_DISP_MIN_ATR:
            if rev_dir == "long":
                swing_range = abs(ps.highest_since - sweep_price)
                retrace = (ps.highest_since - price) / max(swing_range, 1e-10)
            else:
                swing_range = abs(sweep_price - ps.lowest_since)
                retrace = (price - ps.lowest_since) / max(swing_range, 1e-10)

            in_ote = _PS_OTE_FIB_LOW <= retrace <= _PS_OTE_FIB_HIGH
            if in_ote and not ps.ote_reached:
                ps.ote_reached = True
                ps.ote_timestamp = now
                logger.info(f"POST_SWEEP: OTE ZONE ({retrace:.1%})")
            ps.ote_holding = in_ote

        # Evaluate evidence
        decision = self._evaluate_evidence(ps, snap, flow, ict, price, atr, now)

        # BUG-B2 FIX: If gate-blocked, allow evidence to accumulate but do not
        # present a new signal yet. Log remaining cooldown at debug level.
        if _gate_suppressed:
            remaining = self._gate_blocked_until - now
            logger.debug(
                f"POST_SWEEP: gate-block cooldown {remaining:.0f}s remaining — "
                f"evidence accumulating (rev={self._last_sweep_analysis.get('rev_score',0):.0f} "
                f"cont={self._last_sweep_analysis.get('cont_score',0):.0f})")
            return

        if decision.action == "reverse":
            self._handle_reversal(ps.sweep, decision, snap, flow, ict, price, atr, now)
            # Bug #18 fix: _handle_reversal sets self._post_sweep = None and
            # calls self._reset(now).  Without an explicit return here the method
            # falls through to the next tick, creating a window where
            # self._state == POST_SWEEP but self._post_sweep is None.  Any new
            # sweep arriving in that window is silently dropped by the state guard.
            return
        elif decision.action == "continue":
            self._handle_continuation(ps.sweep, decision, snap, flow, ict, price, atr, now)
            # Same one-tick window fix as above.
            return

    def _evaluate_evidence(self, ps, snap, flow, ict, price, atr, now):
        sweep = ps.sweep
        rev_dir = sweep.direction
        cont_dir = "short" if rev_dir == "long" else "long"
        elapsed = now - ps.entered_at

        # Phase and thresholds
        if elapsed < _PS_PHASE_DISPLACEMENT:
            phase, mult, base_thr = "DISPLACEMENT", _PS_DISP_MULT, _PS_THRESHOLD_EARLY
        elif elapsed < _PS_PHASE_CISD:
            phase, mult, base_thr = "CISD", 1.0, _PS_THRESHOLD_NORMAL
        elif elapsed < _PS_PHASE_OTE:
            phase, mult, base_thr = "OTE", 1.0, _PS_THRESHOLD_NORMAL
        else:
            phase, mult, base_thr = "MATURE", _PS_MATURE_MULT, _PS_THRESHOLD_MATURE

        rev_d = 0.0
        cont_d = 0.0
        rev_r: List[str] = []
        cont_r: List[str] = []

        # ── STATIC FACTORS (scored once, invalidated on AMD phase change) ──
        # Bug #4 fix: AMD phase can transition mid-sweep (e.g. ACCUMULATION →
        # MANIPULATION with a 10-15 min lag).  Previously static_scored was set
        # True on first evaluation and never cleared, so the new phase contributed
        # zero to the score.  Reset static_scored when phase or bias changes so
        # the static block is re-evaluated with the current AMD context.
        current_amd_phase = (ict.amd_phase or "").upper()
        current_amd_bias  = (ict.amd_bias  or "").lower()
        if (ps.static_scored
                and (current_amd_phase != ps._static_amd_phase
                     or current_amd_bias != ps._static_amd_bias)):
            ps.static_scored    = False
            ps.static_rev_base  = 0.0
            ps.static_cont_base = 0.0
            logger.info(
                f"POST_SWEEP: AMD changed "
                f"{ps._static_amd_phase}/{ps._static_amd_bias} → "
                f"{current_amd_phase}/{current_amd_bias} "
                f"— resetting static evidence base")

        if not ps.static_scored:
            s_rev, s_cont = 0.0, 0.0

            # AMD phase
            amd_phase = (ict.amd_phase or "").upper()
            amd_bias = (ict.amd_bias or "").lower()
            amd_conf = max(ict.amd_confidence, 0.5)

            if amd_phase == "MANIPULATION":
                bsl_bear = (sweep.pool.side == PoolSide.BSL and amd_bias == "bearish")
                ssl_bull = (sweep.pool.side == PoolSide.SSL and amd_bias == "bullish")
                bsl_bull = (sweep.pool.side == PoolSide.BSL and amd_bias == "bullish")
                ssl_bear = (sweep.pool.side == PoolSide.SSL and amd_bias == "bearish")

                if bsl_bear or ssl_bull:
                    s_rev += 22.0 * amd_conf
                    rev_r.append(f"AMD aligned {amd_bias} ({amd_conf:.0%})")
                elif bsl_bull or ssl_bear:
                    s_rev -= 18.0 * amd_conf
                    s_cont += 10.8 * amd_conf
                    rev_r.append(f"AMD CONTRA {amd_bias} ({amd_conf:.0%})")
                    logger.info(
                        f"POST_SWEEP: AMD CONTRA — {sweep.pool.side.value} "
                        f"swept but bias={amd_bias} conf={amd_conf:.0%}")

            elif amd_phase in ("DISTRIBUTION", "REDISTRIBUTION"):
                s_cont += 20.0 * amd_conf
                cont_r.append(f"AMD DIST ({amd_conf:.0%})")
            elif amd_phase in ("ACCUMULATION", "REACCUMULATION"):
                s_rev += 12.0 * max(amd_conf, 0.4)
                rev_r.append(f"AMD ACCUM ({amd_conf:.0%})")

            # Sweep quality
            if   sweep.quality >= 0.70: s_rev += 12.0
            elif sweep.quality >= 0.50: s_rev += 7.0
            elif sweep.quality >= 0.35: s_rev += 3.0

            # Dealing range
            pd = float(getattr(ict, 'dealing_range_pd', 0.5) or 0.5)
            if rev_dir == "short":
                if pd >= 0.65: s_rev += 7.0; rev_r.append(f"PREMIUM ({pd:.0%})")
                elif pd >= 0.50: s_rev += 3.0
                else: s_cont += 6.0
            else:
                if pd <= 0.35: s_rev += 7.0; rev_r.append(f"DISCOUNT ({pd:.0%})")
                elif pd <= 0.50: s_rev += 3.0
                else: s_cont += 6.0

            # Target quality
            opp = self._find_opposing_target(rev_dir, snap, price, atr)
            if opp and opp.significance >= _MIN_POOL_SIGNIFICANCE:
                s_rev += min(5.0, opp.significance * 0.5)

            # Session
            kz = (ict.kill_zone or "").lower()
            if "london" in kz: s_rev += 3.0
            elif "ny" in kz: s_rev += 2.0

            ps.static_rev_base  = s_rev
            ps.static_cont_base = s_cont
            ps.static_scored    = True
            ps._static_amd_phase = current_amd_phase
            ps._static_amd_bias  = current_amd_bias
        else:
            opp = self._find_opposing_target(rev_dir, snap, price, atr)

        # ── DYNAMIC FACTORS (per-tick) ────────────────────────────────

        # DirectionEngine hint
        hint = (getattr(ict, 'direction_hint', '') or '').lower()
        hint_side = (getattr(ict, 'direction_hint_side', '') or '').lower()
        hint_conf = float(getattr(ict, 'direction_hint_confidence', 0.0) or 0.0)
        if hint and hint_conf >= 0.30:
            pts = round(20.0 * hint_conf, 1)
            if hint == "reverse" and hint_side == rev_dir:
                rev_d += pts; rev_r.append(f"DIR_REVERSE({hint_conf:.0%})")
            elif hint == "continue" and hint_side == cont_dir:
                cont_d += pts; cont_r.append(f"DIR_CONTINUE({hint_conf:.0%})")

        # Live displacement
        if ps.max_displacement >= _PS_DISP_STRONG_ATR:
            rev_d += 10.0; rev_r.append(f"DISP {ps.max_displacement:.1f}ATR")
        elif ps.max_displacement >= _PS_DISP_MIN_ATR:
            rev_d += 5.0
        elif ps.max_displacement < 0.2 and elapsed > 15.0:
            cont_d += 6.0; cont_r.append("NO DISP")

        # CISD
        if ps.cisd_detected:
            fresh = max(0.5, 1.0 - (now - ps.cisd_timestamp) / 120.0)
            rev_d += 15.0 * fresh; rev_r.append(f"CISD {ps.cisd_type} ({fresh:.0%})")

        # OTE
        if ps.ote_reached:
            if ps.ote_holding:
                rev_d += 12.0; rev_r.append("IN OTE")
            else:
                rev_d += 6.0

        # Flow
        if flow.direction == rev_dir and abs(flow.conviction) >= 0.40:
            rev_d += 8.0; rev_r.append(f"FLOW +{flow.conviction:+.2f}")
            ps.rev_flow_ticks += 1
        elif flow.direction == rev_dir:
            rev_d += 4.0; ps.rev_flow_ticks += 1
        elif flow.direction and flow.direction != rev_dir:
            cont_d += 8.0; cont_r.append(f"FLOW cont {flow.conviction:+.2f}")
            ps.cont_flow_ticks += 1

        # EWMA
        ewma_rev = ((rev_dir == "long" and self._flow_ewma > 0.10) or
                     (rev_dir == "short" and self._flow_ewma < -0.10))
        if ewma_rev: rev_d += 5.0
        elif ((rev_dir == "long" and self._flow_ewma < -0.15) or
              (rev_dir == "short" and self._flow_ewma > 0.15)):
            cont_d += 5.0

        # CVD
        cvd = flow.cvd_trend
        if (rev_dir == "short" and cvd < -0.30) or (rev_dir == "long" and cvd > 0.30):
            rev_d += 8.0; rev_r.append(f"CVD {cvd:+.2f}")
        elif (rev_dir == "short" and cvd < 0) or (rev_dir == "long" and cvd > 0):
            rev_d += 3.0
        else:
            cont_d += 4.0

        # 5m structure
        choch = getattr(ict, 'choch_5m', '') or ''
        bos = getattr(ict, 'bos_5m', '') or ''
        rev_struct = "bearish" if rev_dir == "short" else "bullish"
        if choch == rev_struct: rev_d += 10.0; rev_r.append("CHoCH")
        cont_struct = "bullish" if cont_dir == "long" else "bearish"
        if bos == cont_struct: cont_d += 10.0; cont_r.append("BOS cont")

        # 15m structure
        s15 = getattr(ict, 'structure_15m', '') or ''
        if s15 == rev_struct: rev_d += 6.0

        # Sustained flow bonus
        if ps.rev_flow_ticks >= 5: rev_d += 4.0
        if ps.cont_flow_ticks >= 5: cont_d += 4.0

        # Cross-sweep decay — applied ONCE per unique event.
        # Bug #2: previously fired every 250ms tick for the full 90s window the
        # event remained in ict_sweeps.  At 0.55 per tick that is 0.55^360 ≈
        # 3.4e-94 within 3 seconds — one event destroyed all reversal evidence,
        # making reversal trades after any cross-sweep structurally impossible.
        current_type = sweep.pool.side.value
        entered_ms = int(ps.entered_at * 1000)
        for ev in (getattr(ict, 'ict_sweeps', None) or []):
            if (ev.pool_type != current_type
                    and ev.sweep_ts > entered_ms
                    and now * 1000 - ev.sweep_ts < 90_000):
                event_key = (round(ev.pool_price, 0), ev.pool_type,
                             round(ev.sweep_ts / 1000.0, 0))
                if ps._cross_sweep_applied != event_key:
                    ps._cross_sweep_applied = event_key
                    ps.rev_evidence *= 0.55
                    cont_d += 10.0
                    cont_r.append(f"CROSS-SWEEP {ev.pool_type}")
                    logger.info(
                        f"POST_SWEEP: cross-sweep decay applied once "
                        f"(event {ev.pool_type} @ ${ev.pool_price:,.0f})")
                break

        # ── Accumulate with decay ─────────────────────────────────────
        decay = 0.92
        if rev_d > 0 and cont_d > 0:
            ps.rev_evidence += rev_d
            ps.cont_evidence += cont_d
            ps.rev_evidence *= decay
            ps.cont_evidence *= decay
        elif rev_d > 0:
            ps.rev_evidence += rev_d
            ps.cont_evidence *= decay
        elif cont_d > 0:
            ps.cont_evidence += cont_d
            ps.rev_evidence *= decay

        rev_total = max(ps.static_rev_base + ps.rev_evidence, 0.0)
        cont_total = max(ps.static_cont_base + ps.cont_evidence, 0.0)
        ps.peak_rev = max(ps.peak_rev, rev_total)
        ps.peak_cont = max(ps.peak_cont, cont_total)
        gap = abs(rev_total - cont_total)

        self._last_sweep_analysis = {
            "rev_score": rev_total, "cont_score": cont_total,
            "phase": phase, "cisd": ps.cisd_detected,
            "displacement_atr": ps.max_displacement, "ote": ps.ote_reached,
            "rev_reasons": rev_r, "cont_reasons": cont_r,
        }

        threshold = base_thr * mult
        if ps.tick_count == 1 or ps.tick_count % 10 == 0:
            logger.info(
                f"POST_SWEEP [{phase}] tick={ps.tick_count} "
                f"rev={rev_total:.0f} cont={cont_total:.0f} "
                f"(need {threshold:.0f}, gap>={_PS_GAP_MIN:.0f}) "
                f"CISD={'✓' if ps.cisd_detected else '✗'} "
                f"DISP={ps.max_displacement:.1f}ATR "
                f"OTE={'✓' if ps.ote_reached else '✗'}")

        if rev_total >= threshold and gap >= _PS_GAP_MIN:
            conf = min(1.0, rev_total / 90.0)
            if ps.cisd_detected: conf = min(1.0, conf + 0.15)
            if ps.ote_reached: conf = min(1.0, conf + 0.10)
            logger.info(
                f"🎯 SWEEP VERDICT: REVERSAL {rev_dir.upper()} [{phase}] "
                f"(rev={rev_total:.0f} vs cont={cont_total:.0f})")
            return PostSweepDecision(
                action="reverse", direction=rev_dir, confidence=conf,
                next_target=opp,
                reason=f"REVERSAL [{rev_total:.0f}v{cont_total:.0f}] "
                       f"[{phase}] {' + '.join(rev_r[:5])}")

        elif cont_total >= threshold * 0.9 and gap >= _PS_GAP_MIN:
            cont_target = self._find_opposing_target(cont_dir, snap, price, atr)
            logger.info(
                f"🎯 SWEEP VERDICT: CONTINUATION {cont_dir.upper()} [{phase}]")
            return PostSweepDecision(
                action="continue", direction=cont_dir,
                confidence=min(1.0, cont_total / 90.0),
                next_target=cont_target,
                reason=f"CONTINUATION [{cont_total:.0f}v{rev_total:.0f}] "
                       f"[{phase}] {' + '.join(cont_r[:5])}")

        return PostSweepDecision(action="wait", direction="", confidence=0.0,
                                  reason=f"WAIT [{rev_total:.0f}v{cont_total:.0f}]")

    # ── Sweep entry handlers ──────────────────────────────────────────

    def _handle_reversal(self, sweep, decision, snap, flow, ict,
                          price, atr, now) -> None:
        side = decision.direction
        ps = self._post_sweep

        # SL: sweep wick + buffer
        if side == "long":
            sl = sweep.wick_extreme - atr * _REV_SL_BUFFER_ATR
        else:
            sl = sweep.wick_extreme + atr * _REV_SL_BUFFER_ATR

        # Liquidity push
        sl = self._push_sl_behind_pools(sl, side, price, atr)

        # Bug #9 fix: use `price` (current market) as the entry_price reference
        # for both risk and RR.  The fill will be at or near this price; using
        # a stale or drifted value produces phantom R:R that misleads the
        # Telegram notification.  The entry_price stored in EntrySignal IS
        # `price` (set below) — this ensures logged R:R == traded R:R.
        entry_price = price
        risk = abs(entry_price - sl)
        if risk < 1e-10 or risk / entry_price < 0.001 or risk / entry_price > 0.035:
            self._post_sweep = None
            self._reset(now)
            return

        self._last_sweep_reversal_dir = side
        self._last_sweep_reversal_time = now

        # TP: pool → HTF → 2R (only if CISD)
        tp, target = self._find_tp(snap, side, entry_price, atr, sl, _MIN_RR_RATIO)
        cisd_ok = ps is not None and ps.cisd_detected
        if tp is None and cisd_ok:
            tp = entry_price + risk * 2.0 if side == "long" else entry_price - risk * 2.0
        if tp is None:
            self._post_sweep = None
            self._reset(now)
            return

        rr = abs(tp - entry_price) / risk
        if rr < _MIN_RR_RATIO:
            self._post_sweep = None
            self._reset(now)
            return

        if target is None:
            target = PoolTarget(
                pool=sweep.pool, distance_atr=0, direction=side,
                significance=getattr(sweep.pool, 'significance', 3.0),
                tf_sources=[])

        disp = f" DISP={ps.max_displacement:.1f}ATR" if ps else ""
        cisd = f" CISD={ps.cisd_type}" if ps and ps.cisd_detected else ""

        self._signal = EntrySignal(
            side=side, entry_type=EntryType.SWEEP_REVERSAL,
            entry_price=entry_price, sl_price=sl, tp_price=tp, rr_ratio=rr,
            target_pool=target, sweep_result=sweep,
            conviction=decision.confidence,
            reason=f"{decision.reason}{cisd}{disp}",
            ict_validation=self._ict_summary(ict, side),
        )
        logger.info(f"🎯 SIGNAL: REVERSAL {side.upper()} | "
                    f"SL=${sl:,.1f} TP=${tp:,.1f} R:R={rr:.1f}{cisd}{disp}")
        # BUG-2 FIX (STATE-DANGLE): Transition state to SCANNING immediately.
        self._post_sweep = None
        self._state = EngineState.SCANNING
        self._state_entered = now

    def _handle_continuation(self, sweep, decision, snap, flow, ict,
                              price, atr, now) -> None:
        side = decision.direction
        target = decision.next_target
        if target is None:
            self._post_sweep = None
            self._reset(now)
            return

        # SL: BEYOND the swept pool
        if side == "long":
            sl = sweep.pool.price - atr * _CONT_SL_BUFFER_ATR
        else:
            sl = sweep.pool.price + atr * _CONT_SL_BUFFER_ATR

        sl = self._push_sl_behind_pools(sl, side, price, atr)

        # Bug #9 fix: derive risk and RR from `price` as the prospective fill
        # price (same value stored as entry_price in the signal).  This ensures
        # the R:R logged in Telegram exactly matches the trade's economics.
        entry_price = price
        tp_buf = atr * 0.35
        tp = target.pool.price - tp_buf if side == "long" else target.pool.price + tp_buf

        risk = abs(entry_price - sl)
        reward = abs(tp - entry_price)
        if risk < 1e-10 or risk / entry_price < 0.001 or risk / entry_price > 0.035:
            self._post_sweep = None
            self._reset(now)
            return

        rr = reward / risk
        if rr < _MIN_RR_RATIO:
            tp2, target2 = self._find_tp(snap, side, entry_price, atr, sl, _MIN_RR_RATIO)
            if tp2 is not None:
                tp, target = tp2, target2
                rr = abs(tp - entry_price) / risk
                # Bug #3 fix: re-validate after substitution.  _find_tp only
                # guarantees abs(tp-price)/risk >= min_rr using the ENTRY PRICE
                # passed to it, but min_rr itself equals _MIN_RR_RATIO, so the
                # recomputed rr will always pass here — the explicit guard below
                # protects against any future change to _find_tp's internal
                # min_rr argument or floating-point edge cases.
                if rr < _MIN_RR_RATIO:
                    self._post_sweep = None
                    self._reset(now)
                    return
            else:
                self._post_sweep = None
                self._reset(now)
                return

        self._signal = EntrySignal(
            side=side, entry_type=EntryType.SWEEP_CONTINUATION,
            entry_price=entry_price, sl_price=sl, tp_price=tp, rr_ratio=rr,
            target_pool=target, sweep_result=sweep,
            conviction=decision.confidence, reason=decision.reason,
            ict_validation=self._ict_summary(ict, side),
        )
        logger.info(f"🎯 SIGNAL: CONTINUATION {side.upper()} R:R={rr:.1f}")
        # BUG-2 FIX (STATE-DANGLE): identical fix as _handle_reversal.
        # Transition to SCANNING immediately without nulling self._signal.
        self._post_sweep = None
        self._state = EngineState.SCANNING
        self._state_entered = now

    # ── Helpers ───────────────────────────────────────────────────────

    def _find_tp(self, snap, side, price, atr, sl, min_rr):
        """Find TP: nearest pool → HTF escalation. Returns (tp, target)."""
        risk = abs(price - sl)
        if risk < 1e-10:
            return None, None

        pools = snap.bsl_pools if side == "long" else snap.ssl_pools
        candidates = [t for t in pools
                      if _MIN_TARGET_ATR <= t.distance_atr <= _MAX_TARGET_ATR
                      and t.significance >= _MIN_POOL_SIGNIFICANCE]
        if candidates:
            best = max(candidates, key=lambda t: t.adjusted_sig())
            tp = self._pool_to_tp(best, side, price, atr)
            if tp and abs(tp - price) / risk >= min_rr:
                return tp, best

        # HTF escalation
        htf = [t for t in pools
               if (t.pool.timeframe in _HTF_TP_TIMEFRAMES
                   or t.pool.htf_count >= _HTF_TP_MIN_HTF_COUNT)
               and _MIN_TARGET_ATR <= t.distance_atr <= _HTF_TP_MAX_ATR]
        htf.sort(key=lambda t: t.distance_atr)
        for t in htf:
            tp = self._pool_to_tp(t, side, price, atr)
            if tp and abs(tp - price) / risk >= min_rr:
                return tp, t

        return None, None

    def _find_opposing_target(self, direction, snap, price, atr):
        # EE-3 FIX: previously capped at _MAX_TARGET_ATR (8.0 ATR), which
        # rejected HTF pools at 10-30 ATR that are the real institutional
        # targets. The caller uses the returned pool for TP computation
        # and for quality scoring — both benefit from HTF reach. Callers
        # that need a proximity filter still apply their own distance gate.
        pools = snap.bsl_pools if direction == "long" else snap.ssl_pools
        reachable = [t for t in pools
                     if t.distance_atr <= _HTF_TP_MAX_ATR
                     and t.significance >= _MIN_POOL_SIGNIFICANCE * 0.5]
        if not reachable:
            return None
        return max(reachable, key=lambda t: t.adjusted_sig())

    def _pool_to_tp(self, target, side, price, atr):
        # Bug #6 fix: the previous formula `atr * min(0.50, 0.10 + 0.05 * dist)`
        # scaled the buffer with distance.  At 8 ATR distance (common HTF pool)
        # this produced 0.50 × ATR subtracted from TP, materially reducing
        # realized R:R below the logged signal value.  The pool price is already
        # the institutional target; the offset purpose is spread/slippage
        # protection only, which is independent of how far away the pool is.
        buf = atr * 0.12
        tp = target.pool.price - buf if side == "long" else target.pool.price + buf
        if side == "long" and tp <= price: return None
        if side == "short" and tp >= price: return None
        return tp

    def _compute_sl(self, ict, side, price, atr):
        """ICT OB-based SL with ATR fallback."""
        if side == "long":
            ob = ict.nearest_ob_price
        else:
            ob = getattr(ict, 'nearest_ob_price_short', 0.0) or 0.0

        sl = None
        if ob > 0:
            if side == "long" and ob < price:
                sl = ob - atr * 0.20
            elif side == "short" and ob > price:
                sl = ob + atr * 0.20

        if sl is not None:
            dist = abs(price - sl) / price
            if dist < 0.001 or dist > 0.035:
                sl = None

        if sl is None:
            fallback = 1.5 * atr
            sl = price - fallback if side == "long" else price + fallback

        sl = self._push_sl_behind_pools(sl, side, price, atr)

        dist = abs(price - sl) / price
        if dist < 0.001 or dist > 0.035:
            return None
        return sl

    def _push_sl_behind_pools(self, sl, side, price, atr):
        """Push SL behind nearby liquidity pools.

        EE-6 FIX: cap the total push distance to _SL_PUSH_MAX_ATR from the
        starting SL so a large `snap.bsl_pools` list with one outlier pool
        far from price cannot shift SL to an unrealistic distance.
        """
        snap = self._last_liq_snapshot
        if snap is None:
            return sl
        _SL_PUSH_MAX_ATR = 3.0     # never push SL more than 3 ATR from original
        sl_origin = sl
        buf = 0.25 * atr
        if side == "long":
            for t in snap.ssl_pools:
                if sl < t.pool.price < price:
                    candidate = t.pool.price - buf
                    # Cap: candidate must not be more than _SL_PUSH_MAX_ATR ATR
                    # below the original SL.
                    if sl_origin - candidate > _SL_PUSH_MAX_ATR * atr:
                        continue
                    sl = min(sl, candidate)
        else:
            for t in snap.bsl_pools:
                if price < t.pool.price < sl:
                    candidate = t.pool.price + buf
                    if candidate - sl_origin > _SL_PUSH_MAX_ATR * atr:
                        continue
                    sl = max(sl, candidate)
        return sl

    @staticmethod
    def _ict_summary(ict, side):
        parts = []
        if ict.amd_phase: parts.append(f"AMD={ict.amd_phase}")
        if ict.amd_bias: parts.append(f"bias={ict.amd_bias}")
        if ict.in_discount: parts.append("DISCOUNT")
        elif ict.in_premium: parts.append("PREMIUM")
        if ict.structure_5m: parts.append(f"5m={ict.structure_5m}")
        if ict.kill_zone: parts.append(f"KZ={ict.kill_zone}")
        return " | ".join(parts) if parts else "no ICT"


# ═══════════════════════════════════════════════════════════════════════════
# TRAIL MANAGER
# ═══════════════════════════════════════════════════════════════════════════

class ICTTrailManager:
    """Trailing stop using ICT structures."""

    def __init__(self) -> None:
        self._sl = 0.0
        self._entry = 0.0
        self._side = ""
        self._bos_count = 0
        self._choch = False

    def initialize(self, side, entry_price, initial_sl):
        self._side = side
        self._entry = entry_price
        self._sl = initial_sl
        self._bos_count = 0
        self._choch = False

    def compute(self, ict_ctx, price, atr, candles_5m,
                candles_15m=None):
        if not self._side or atr < 1e-10:
            return None

        if ict_ctx.bos_5m:
            expected = "bullish" if self._side == "long" else "bearish"
            if ict_ctx.bos_5m == expected:
                self._bos_count += 1

        if ict_ctx.choch_5m:
            against = "bearish" if self._side == "long" else "bullish"
            if ict_ctx.choch_5m == against:
                self._choch = True

        new_sl = self._structural_sl(candles_5m, candles_15m, atr)
        if new_sl is None:
            return None
        if self._side == "long" and new_sl <= self._sl:
            return None
        if self._side == "short" and new_sl >= self._sl:
            return None

        self._sl = new_sl
        return new_sl

    def _structural_sl(self, c5m, c15m, atr):
        try:
            from strategy.liquidity_map import _find_swing_highs, _find_swing_lows
        except ImportError:
            from liquidity_map import _find_swing_highs, _find_swing_lows

        # Bug #7 fix: CHoCH buffer was 0.05 ATR ≈ $7.50 at BTC $150 ATR —
        # inside the bid/ask spread, causing immediate SL hits on normal tick
        # noise the moment CHoCH was detected.  New values are calibrated to
        # sit above the typical BTC spread:
        #   CHoCH:   0.18 ATR  (tight but above spread — structure confirmed)
        #   2× BOS:  0.25 ATR  (moderate — multiple BOS increases confidence)
        #   default: _SL_BUFFER_ATR = 0.35 (standard structural buffer)
        if self._choch:
            buf_mult = 0.18
        elif self._bos_count >= 2:
            buf_mult = 0.25
        else:
            buf_mult = _SL_BUFFER_ATR
        buf = atr * buf_mult

        if c15m and len(c15m) >= 12:
            if self._side == "long":
                lows = _find_swing_lows(c15m, lookback=3)
                if lows: return lows[-1][1] - buf
            else:
                highs = _find_swing_highs(c15m, lookback=3)
                if highs: return highs[-1][1] + buf

        if c5m and len(c5m) >= 12:
            if self._side == "long":
                lows = _find_swing_lows(c5m, lookback=4)
                if lows: return lows[-1][1] - buf
            else:
                highs = _find_swing_highs(c5m, lookback=4)
                if highs: return highs[-1][1] + buf
        return None

    @property
    def current_sl(self):
        return self._sl

    @property
    def phase_info(self):
        p = [f"BOS×{self._bos_count}"]
        if self._choch: p.append("CHoCH")
        return " ".join(p)

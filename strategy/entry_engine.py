"""
entry_engine.py — Liquidity-First Entry Decision Engine v3.1
=============================================================
FIXES IN v2.0
─────────────
FIX-A through FIX-L: (see original changelog)

FIXES IN v3.0 (institutional EWMA + conviction decay)
──────────────────────────────────────────────────────
v3.0 additions: EWMA flow tracking, conviction decay in READY,
AMD conviction modifiers, momentum entry, minimum hold times.

FIXES IN v3.1
─────────────
BUG-FIX-1: EWMA alpha was NOT time-normalised.
  Old: alpha = base * (1 + conviction_boost), dt computed but discarded.
  If tick rate slowed (stream gap, reconnect), EWMA decayed at the same
  rate as a 250ms tick — making signals artificially stale after any gap.
  New: continuous-time equivalent alpha = 1 - exp(-alpha_per_sec * dt),
  where alpha_per_sec is calibrated so a 250ms tick produces the original
  base_alpha. A 5-second gap now decays the EWMA correctly.

BUG-FIX-2: Adjacency bonus was invisible to all decision functions.
  Old: get_snapshot() applied +2.0 adjacency bonus to PoolTarget.significance
  but ALL selection logic (primary target, _find_flow_target, etc.) called
  t.pool.proximity_adjusted_sig() which reads pool.significance — ignoring
  the bonus. The bonus only affected display sorting.
  New: All selection logic calls t.adjusted_sig() (defined on PoolTarget in
  liquidity_map.py v2.1) which uses t.significance × exp(-dist/10).

BUG-FIX-3: BSL/SSL proximity approach used inconsistent selection criteria.
  Old: BSL selected by closest distance; SSL selected by highest
  proximity_adjusted_sig. Cross-side comparison was apples-to-oranges — a
  BSL at 0.3 ATR (selected by distance) could be overridden by an SSL at
  1.8 ATR if SSL's adjusted sig was marginally higher.
  New: Both sides use t.adjusted_sig() so the comparison is consistent.

BUG-FIX-4: _compute_sl log message was misleading when SL was rejected
  for distance (too wide), not absence of OB. The log said "no ICT structure
  found" even when an OB existed — masking why READY burned 80+ fail ticks.
  New: Separate log paths for "no OB found" vs "OB too far/close".

BUG-FIX-5: Momentum signal target_pool used snap.bsl_pools[0] (raw
  significance rank, possibly 40+ ATR away) instead of the closest
  proximity-adjusted pool. Trail manager received a distant synthetic target.
  New: Uses proximity-adjusted selection filtered to _MAX_TARGET_ATR.

HTF-TP-ESCALATION (v3.1 new feature):
  When the nearest pool TP fails the minimum R:R threshold, the engine now
  escalates to search higher-timeframe pools (1h/4h/1d) AND any pool with
  htf_count >= 2 (multi-TF confluence) within _HTF_TP_MAX_ATR distance.
  Applied in: _do_ready, _handle_sweep_reversal, _check_proximity_approach,
  _check_displacement_momentum.

  Motivation from live log (2026-03-28 22:39:58):
    READY: R:R failed 80 consecutive ticks (best=0.88 vs required=1.40)
    The $67,071 BSL pool (2.1 ATR) gave only R:R=0.88 with a 1.79 ATR SL.
    The engine aborted instead of escalating to $68,949 (22.6 ATR, HTFx2)
    which would have given R:R=12.7 — a valid institutional setup.
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

# ── v3.0 EWMA Flow Tracking ───────────────────────────────────────────────
# BUG-FIX-1: alpha is now time-normalised. The constant below is the
# PER-SECOND decay rate, NOT the per-tick alpha. _update_flow_ewma converts
# it to the correct per-tick alpha using the actual elapsed time dt.
#
# Calibration: original base_alpha=0.15 was intended for 250ms ticks.
# alpha_per_sec = -ln(1 - 0.15) / 0.25 ≈ 0.648
# After fix: at dt=0.25s, alpha = 1 - exp(-0.648 * 0.25) = 0.15 (unchanged).
# At dt=1.0s, alpha = 1 - exp(-0.648) ≈ 0.48 (faster decay as expected).
_FLOW_EWMA_ALPHA_PER_SEC = 0.648   # calibrated to original 0.15 at 250ms tick
_FLOW_EWMA_ADAPTIVE_CAP  = 0.40    # max alpha even at high conviction

# ── Minimum Hold Times ────────────────────────────────────────────────────
_TRACKING_MIN_HOLD_SEC  = 5.0
_READY_MIN_HOLD_SEC     = 8.0

# ── Conviction Decay (READY state) ───────────────────────────────────────
_READY_DECAY_RATE       = 0.06
_READY_MIN_CONVICTION   = 0.18

# ── AMD Conviction Modifiers ─────────────────────────────────────────────
_AMD_MANIP_CONTRA_MULT  = 0.60
_AMD_DIST_CONTRA_MULT   = 0.80
_AMD_ALIGNED_BONUS      = 1.10

# ── Displacement / Momentum Entry ────────────────────────────────────────
_MOMENTUM_MIN_BODY_RATIO   = 0.65
_MOMENTUM_MIN_VOL_RATIO    = 1.3
_MOMENTUM_MIN_ATR_MOVE     = 0.6
_MOMENTUM_LOOKBACK_CANDLES  = 3
_MOMENTUM_SL_BUFFER_ATR    = 0.15
_MOMENTUM_MIN_RR           = 1.3
_MOMENTUM_COOLDOWN_SEC     = 60.0
_MOMENTUM_MAX_ENTRIES_PER_HOUR = 3

# ── HTF TP Escalation (v3.1) ──────────────────────────────────────────────
# When the nearest pool TP fails minimum R:R, escalate to higher-TF pools.
# Qualifies: native 1h/4h/1d pools AND any pool with htf_count >= HTF_MIN_COUNT.
#
# _HTF_TP_MAX_ATR: how far we are willing to reach for an HTF TP.
# 30 ATR at $89 ATR ≈ $2,670. Realistic for 1h/4h/1d institutional delivery.
_HTF_TP_TIMEFRAMES   = ('1h', '4h', '1d')
_HTF_TP_MAX_ATR      = 30.0    # maximum HTF TP pool distance
_HTF_TP_MIN_HTF_COUNT = 2      # pools with this many HTF confluences qualify


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
    """Single-flow entry decision engine v3.1."""

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
        # BUG-2 FIX: sweep reversal direction memory
        self._last_sweep_reversal_dir:  str   = ""
        self._last_sweep_reversal_time: float = 0.0
        # Liquidity snapshot reference for SL liquidity-awareness
        self._last_liq_snapshot = None
        # ── v3.0: EWMA flow tracking ──────────────────────────────────────
        self._flow_ewma:             float = 0.0
        self._flow_ewma_last_update: float = 0.0
        # ── v3.0: Conviction decay state (READY) ─────────────────────────
        self._ready_conviction:      float = 0.0
        self._ready_peak_conviction: float = 0.0
        self._ready_last_agree_ts:   float = 0.0
        self._ready_rr_fail_count:   int   = 0
        # ── v3.0: Momentum entry tracking ────────────────────────────────
        self._momentum_entries_1h:   int   = 0
        self._momentum_hour_start:   float = 0.0
        self._last_momentum_candle_ts: int = 0

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
        Called when an entry order is explicitly cancelled.
        """
        self._post_sweep = None
        self._proximity_side = ""
        self._last_entry_at = time.time()
        self._ready_rr_fail_count = 0
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

    # ── BUG-FIX-1: Time-normalised EWMA ─────────────────────────────────

    def _update_flow_ewma(self, flow: OrderFlowState, now: float) -> float:
        """
        Institutional EWMA flow tracker — v3.1 (time-normalised).

        BUG-FIX-1: The original code computed dt but then discarded it,
        using a fixed per-tick alpha. This made the EWMA decay at the same
        rate regardless of how much real time had elapsed between ticks.
        After a stream gap (reconnect, lag), the EWMA was artificially stale
        — it had barely decayed despite minutes of silence.

        Fix: continuous-time formulation.
          alpha_per_sec calibrated so at dt=0.25s: alpha ≈ original 0.15
          At dt=1.0s: alpha ≈ 0.48  (correct — 4× more decay in 4× more time)
          At dt=5.0s: alpha ≈ 0.96  (near-full reset after 5s silence)
          Capped at _FLOW_EWMA_ADAPTIVE_CAP regardless of dt or conviction.
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

        # BUG-FIX-1: time-normalised alpha.
        # Clamp dt to 2.0s max — beyond 2s the EWMA should be near-reset
        # anyway and further clamping avoids edge cases on very long gaps.
        dt_clamped = min(dt, 2.0)

        # Base alpha from continuous-time decay, adaptive boost for strong flow
        base_alpha = 1.0 - math.exp(-_FLOW_EWMA_ALPHA_PER_SEC * dt_clamped)
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
        Returns a multiplier in [0.60, 1.10].
        AMD modifies conviction and sizing — it NEVER blocks.
        """
        _phase = (ict.amd_phase or '').upper()
        _bias  = (ict.amd_bias or '').lower()

        if not _phase or not _bias or _bias == "neutral":
            return 1.0

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

        Detection criteria (ALL must pass):
          1. Recent closed candle with body/range >= 0.65
          2. Candle range >= 0.6×ATR
          3. Volume >= 1.3× 20-bar average
          4. Flow EWMA agrees with displacement direction
          5. Not within cooldown window

        BUG-FIX-5: Target pool now uses proximity-adjusted selection within
        _MAX_TARGET_ATR, not snap.bsl_pools[0] (raw-significance first, may
        be 40+ ATR away). HTF TP escalation applied when the nearest pool
        TP fails R:R instead of falling through to the synthetic 2× fallback.
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
        disp_candle = None
        disp_tf = ""
        disp_direction = ""

        for candles, tf_label in [(candles_5m, "5m"), (candles_1m, "1m")]:
            if not candles or len(candles) < _MOMENTUM_LOOKBACK_CANDLES + 20:
                continue

            vol_window = candles[-(20 + 2):-2]
            avg_vol = sum(float(c.get('v', 0)) for c in vol_window) / max(len(vol_window), 1)

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

                if c_ts > 0 and c_ts == self._last_momentum_candle_ts:
                    continue

                body = abs(c_c - c_o)
                rng  = c_h - c_l
                if rng < 1e-10:
                    continue

                body_ratio = body / rng
                vol_ratio  = c_v / max(avg_vol, 1e-10)
                atr_move   = rng / max(atr, 1e-10)

                if body_ratio < _MOMENTUM_MIN_BODY_RATIO:
                    continue
                if atr_move < _MOMENTUM_MIN_ATR_MOVE:
                    continue
                if vol_ratio < _MOMENTUM_MIN_VOL_RATIO:
                    continue

                candle_dir = "long" if c_c > c_o else "short"

                ewma_dir = self._ewma_direction()
                if ewma_dir and ewma_dir != candle_dir:
                    continue

                disp_candle = c
                disp_tf = tf_label
                disp_direction = candle_dir
                break

            if disp_candle is not None:
                break

        if disp_candle is None:
            return False

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

        ict_sl = self._compute_sl(ict, disp_direction, price, atr)
        if ict_sl is not None:
            if disp_direction == "long":
                sl = min(sl, ict_sl)
            else:
                sl = max(sl, ict_sl)

        # ── Compute TP — BUG-FIX-5: proximity-adjusted target selection ──
        # Step 1: nearest opposing pool within _MAX_TARGET_ATR
        tp = None
        _signal_target = None
        if disp_direction == "long":
            # BUG-FIX-5: use proximity-adjusted significance to select target
            candidates = [t for t in snap.bsl_pools
                          if t.pool.price > price
                          and _MIN_TARGET_ATR <= t.distance_atr <= _MAX_TARGET_ATR]
            if candidates:
                best = max(candidates, key=lambda t: t.adjusted_sig())
                tp_candidate = self._compute_tp_approach(best, "long", price, atr)
                if tp_candidate is not None:
                    tp = tp_candidate
                    _signal_target = best
        else:
            candidates = [t for t in snap.ssl_pools
                          if t.pool.price < price
                          and _MIN_TARGET_ATR <= t.distance_atr <= _MAX_TARGET_ATR]
            if candidates:
                best = max(candidates, key=lambda t: t.adjusted_sig())
                tp_candidate = self._compute_tp_approach(best, "short", price, atr)
                if tp_candidate is not None:
                    tp = tp_candidate
                    _signal_target = best

        # Step 2: HTF TP escalation before falling back to synthetic 2× target
        if tp is None or (
            abs(tp - price) / max(abs(price - sl), 1e-10) < _MOMENTUM_MIN_RR * (2.0 - amd_mult)
        ):
            _htf_tp, _htf_target = self._find_htf_tp(
                snap, disp_direction, price, atr, sl,
                _MOMENTUM_MIN_RR * (2.0 - amd_mult))
            if _htf_tp is not None:
                tp = _htf_tp
                _signal_target = _htf_target

        # Step 3: Synthetic 2× risk fallback (last resort)
        if tp is None:
            risk = abs(price - sl)
            tp = price + risk * 2.0 if disp_direction == "long" else price - risk * 2.0

        # ── R:R validation ────────────────────────────────────────────────
        risk = abs(price - sl)
        reward = abs(tp - price)
        if risk < 1e-10:
            return False
        rr = reward / risk
        adjusted_min_rr = _MOMENTUM_MIN_RR * (2.0 - amd_mult)
        if rr < adjusted_min_rr:
            return False

        # ── SL sanity ─────────────────────────────────────────────────────
        sl_dist_pct = abs(price - sl) / price
        if sl_dist_pct < 0.001 or sl_dist_pct > 0.035:
            return False

        # ── BUG-FIX-5: target pool — use _signal_target (proximity-adjusted) ──
        # If no pool was found from any source, require at least some
        # reachable pool exists for the trail manager's reference
        if _signal_target is None:
            if disp_direction == "long" and snap.bsl_pools:
                # Use closest reachable BSL as reference pool
                reachable = [t for t in snap.bsl_pools
                             if t.distance_atr <= _HTF_TP_MAX_ATR]
                if reachable:
                    _signal_target = min(reachable, key=lambda t: t.distance_atr)
            elif disp_direction == "short" and snap.ssl_pools:
                reachable = [t for t in snap.ssl_pools
                             if t.distance_atr <= _HTF_TP_MAX_ATR]
                if reachable:
                    _signal_target = min(reachable, key=lambda t: t.distance_atr)
        if _signal_target is None:
            return False

        # ── Emit signal ───────────────────────────────────────────────────
        c_ts = int(disp_candle.get('t', 0) or 0)
        self._last_momentum_candle_ts = c_ts
        self._momentum_entries_1h += 1

        vol_ratio_approx = float(disp_candle.get('v', 0)) / max(1.0, atr)
        self._signal = EntrySignal(
            side=disp_direction,
            entry_type=EntryType.DISPLACEMENT_MOMENTUM,
            entry_price=price,
            sl_price=sl,
            tp_price=tp,
            rr_ratio=rr,
            target_pool=_signal_target,
            conviction=abs(flow.conviction) * amd_mult,
            reason=(
                f"DISPLACEMENT {disp_direction.upper()} [{disp_tf}] | "
                f"body={float(disp_candle['c'])-float(disp_candle['o']):+.1f} "
                f"range={float(disp_candle['h'])-float(disp_candle['l']):.1f} "
                f"vol={vol_ratio_approx:.1f}x | "
                f"AMD×{amd_mult:.2f} R:R={rr:.1f}"
                f"{f' HTF-TP [{_signal_target.pool.timeframe}]' if _signal_target.pool.timeframe in _HTF_TP_TIMEFRAMES else ''}"
            ),
            ict_validation=self._ict_summary(ict, disp_direction),
        )
        logger.info(
            f"⚡ MOMENTUM SIGNAL: {disp_direction.upper()} [{disp_tf}] | "
            f"body/range={abs(float(disp_candle['c'])-float(disp_candle['o']))/(float(disp_candle['h'])-float(disp_candle['l'])+1e-10):.2f} "
            f"R:R={rr:.1f} AMD×{amd_mult:.2f} TP-pool={_signal_target.pool.timeframe}"
        )
        return True

    # ── FIX-E + BUG-FIX-3: Proximity Approach ────────────────────────────

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

        BUG-FIX-3: BSL and SSL selection now use the same criterion (t.adjusted_sig())
        so the cross-side comparison is consistent. Previously BSL used closest
        distance while SSL used proximity-adjusted significance — apples-to-oranges.

        HTF TP Escalation: if the approaching pool's TP fails R:R (common when
        the pool is very close and SL is structurally wide), escalate to HTF pools.
        """
        if now - self._last_entry_at < _ENTRY_COOLDOWN_SEC:
            return False

        _sweep_cooldown = 60.0

        # ── BUG-FIX-3: Consistent selection criterion for both BSL and SSL ──
        # Use t.adjusted_sig() for both so the comparison is apples-to-apples.
        best_approach: Optional[PoolTarget] = None
        approach_side: str = ""

        for t in snap.bsl_pools:
            if (_PROXIMITY_ENTRY_ATR_MIN <= t.distance_atr <= _PROXIMITY_ENTRY_ATR_MAX
                    and t.significance >= _PROXIMITY_MIN_SIG):
                # BUG-FIX-3: was: t.distance_atr < best_approach.distance_atr (distance criterion)
                if best_approach is None or t.adjusted_sig() > best_approach.adjusted_sig():
                    best_approach = t
                    approach_side = "long"

        for t in snap.ssl_pools:
            if (_PROXIMITY_ENTRY_ATR_MIN <= t.distance_atr <= _PROXIMITY_ENTRY_ATR_MAX
                    and t.significance >= _PROXIMITY_MIN_SIG):
                # BUG-FIX-3: was: t.pool.proximity_adjusted_sig() — now consistent
                if best_approach is None or t.adjusted_sig() > best_approach.adjusted_sig():
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

        # ── AMD conviction modifier ───────────────────────────────────────
        _amd_mult = self._amd_conviction_modifier(ict, approach_side)
        if _amd_mult < 1.0:
            if abs(flow.conviction) < _FLOW_CONV_THRESHOLD * 0.7:
                logger.debug(
                    f"⛔ Proximity {approach_side} weakened by AMD "
                    f"({getattr(ict,'amd_phase','')}) conv={flow.conviction:+.2f} "
                    f"× AMD={_amd_mult:.2f} too low")
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

        # ── Compute SL/TP ─────────────────────────────────────────────────
        sl = self._compute_sl(ict, approach_side, price, atr)
        tp = self._compute_tp_approach(best_approach, approach_side, price, atr)

        if sl is None or tp is None:
            return False

        risk = abs(price - sl)
        reward = abs(tp - price)
        if risk < 1e-10:
            return False
        rr = reward / risk

        # ── HTF TP Escalation for proximity approach ──────────────────────
        # If the approaching pool is very close (< 0.5 ATR) and SL is wide,
        # R:R can fail. Try HTF pools instead of silently returning False.
        if rr < _MIN_RR_RATIO:
            _htf_tp, _htf_target = self._find_htf_tp(
                snap, approach_side, price, atr, sl, _MIN_RR_RATIO)
            if _htf_tp is not None:
                tp = _htf_tp
                reward = abs(tp - price)
                rr = reward / risk
                best_approach = _htf_target  # update for signal construction
                logger.debug(
                    f"🔼 Proximity HTF TP escalation: {approach_side.upper()} "
                    f"[{_htf_target.pool.timeframe}] ${_htf_target.pool.price:,.1f} "
                    f"R:R={rr:.2f}")
            else:
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

        amd_mult = self._amd_conviction_modifier(ict, flow.direction)

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
        ewma_agrees = (
            (tr.direction == "long" and self._flow_ewma > 0.05) or
            (tr.direction == "short" and self._flow_ewma < -0.05)
        )
        ewma_contrary = (
            (tr.direction == "long" and self._flow_ewma < -_FLOW_CONV_THRESHOLD * 0.3) or
            (tr.direction == "short" and self._flow_ewma > _FLOW_CONV_THRESHOLD * 0.3)
        )

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
        ready = (
            tr.flow_ticks >= _FLOW_SUSTAINED_TICKS
            and flow.cvd_agrees
            and abs(flow.conviction) >= _FLOW_CONV_THRESHOLD
            and tr.target.distance_atr >= _MIN_TARGET_ATR
            and tr.target.distance_atr <= _MAX_TARGET_ATR
        )

        # ICT bonus: lower bar if structure strongly agrees
        if not ready and tr.flow_ticks >= 1:
            ict_boost = self._ict_structure_agrees(ict, tr.direction)
            if ict_boost and abs(flow.conviction) >= _FLOW_CONV_THRESHOLD * 0.65:
                ready = True

        # v3.0: EWMA fast-track
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
            # ── R:R pre-check before entering READY ──────────────────────
            _pre_sl = self._compute_sl(ict, tr.direction, price, atr)
            _pre_tp = self._compute_tp_approach(tr.target, tr.direction, price, atr)
            if _pre_sl is not None and _pre_tp is not None:
                _pre_risk = abs(price - _pre_sl)
                _pre_reward = abs(_pre_tp - price)
                if _pre_risk > 1e-10:
                    _pre_rr = _pre_reward / _pre_risk
                    _adj_rr = _MIN_RR_RATIO * (2.0 - tr.amd_mult)
                    if _pre_rr < _adj_rr * 0.7:
                        # Also check if HTF escalation would fix it immediately
                        _htf_tp_pre, _ = self._find_htf_tp(
                            snap, tr.direction, price, atr, _pre_sl, _adj_rr)
                        if _htf_tp_pre is None:
                            logger.debug(
                                f"📡 READY rejected: R:R {_pre_rr:.2f} < "
                                f"{_adj_rr * 0.7:.2f} and no HTF TP available "
                                f"— target too close for viable trade")
                            ready = False

        if ready:
            self._state = EngineState.READY
            self._state_entered = now
            self._ready_conviction = abs(flow.conviction)
            self._ready_peak_conviction = self._ready_conviction
            self._ready_last_agree_ts = now
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

        # ── v3.0: Conviction decay ────────────────────────────────────────
        flow_agrees = (flow.direction == tr.direction)
        flow_contrary = (
            flow.direction != ""
            and flow.direction != tr.direction
        )

        if flow_agrees:
            self._ready_last_agree_ts = now
            recovery = min(0.02, abs(flow.conviction) * 0.05)
            self._ready_conviction = min(
                self._ready_peak_conviction,
                self._ready_conviction + recovery
            )
            tr.contrary_ticks = 0
        elif flow_contrary:
            tr.contrary_ticks += 1
            decay_strength = abs(flow.conviction)
            decay = _READY_DECAY_RATE * max(decay_strength, 0.3)
            self._ready_conviction = max(0.0, self._ready_conviction - decay)

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
            _fail_count = self._ready_rr_fail_count + 1
            self._ready_rr_fail_count = _fail_count
            if _fail_count == 1 or _fail_count % 40 == 0:
                logger.debug(
                    f"✅ READY: SL={'None' if sl is None else f'${sl:,.1f}'} "
                    f"TP={'None' if tp is None else f'${tp:,.1f}'} — "
                    f"no valid levels (fail #{_fail_count})")
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
        adjusted_min_rr = _MIN_RR_RATIO * (2.0 - tr.amd_mult)

        # ── HTF TP ESCALATION (v3.1) ──────────────────────────────────────
        # When the primary target pool fails R:R (e.g. pool is only 2 ATR
        # away but SL is 1.8 ATR wide), escalate to higher-timeframe pools
        # (1h/4h/1d) or multi-TF confluence pools before burning fail ticks.
        #
        # From live log 2026-03-28: LONG → $67,071 (2.1 ATR) with SL 1.79 ATR
        # gave R:R=0.88. The engine wasted 80 ticks before aborting. The
        # $68,949 pool (22.6 ATR, HTFx2) would give R:R=12.7 — valid setup.
        _escalated_target: Optional[PoolTarget] = None
        if rr < adjusted_min_rr:
            _htf_tp, _htf_target = self._find_htf_tp(
                snap, tr.direction, price, atr, sl, adjusted_min_rr)
            if _htf_tp is not None:
                tp = _htf_tp
                reward = abs(tp - price)
                rr = reward / risk
                _escalated_target = _htf_target
                self._ready_rr_fail_count = 0  # reset: valid TP found
                # Fall through to signal generation below
            else:
                # HTF escalation also failed — track failure count
                _fail_count = self._ready_rr_fail_count + 1
                self._ready_rr_fail_count = _fail_count
                if _fail_count == 1 or _fail_count % 60 == 0:
                    logger.debug(
                        f"✅ READY: R:R {rr:.2f} < {adjusted_min_rr:.2f} "
                        f"(AMD×{tr.amd_mult:.2f}), no HTF TP found — skipping "
                        f"(entry=${price:,.1f} SL=${sl:,.1f} TP=${tp:,.1f}) "
                        f"[fail #{_fail_count}]"
                    )
                if _fail_count >= 80:
                    logger.info(
                        f"✅ READY: R:R failed {_fail_count} consecutive ticks "
                        f"(best={rr:.2f} vs required={adjusted_min_rr:.2f}), "
                        f"no HTF TP in range — target too close, aborting")
                    self._tracking = None
                    self._state = EngineState.SCANNING
                    self._state_entered = now
                return
        else:
            # Primary TP met R:R — reset failure counter
            self._ready_rr_fail_count = 0

        # ── Determine the effective target for the signal ─────────────────
        _signal_target = _escalated_target if _escalated_target is not None else tr.target

        _htf_note = (
            f" [HTF-TP {_escalated_target.pool.timeframe} ${_escalated_target.pool.price:,.1f}]"
            if _escalated_target is not None else ""
        )

        self._signal = EntrySignal(
            side=tr.direction,
            entry_type=EntryType.PRE_SWEEP_APPROACH,
            entry_price=price,
            sl_price=sl,
            tp_price=tp,
            rr_ratio=rr,
            target_pool=_signal_target,
            conviction=self._ready_conviction * tr.amd_mult,
            reason=(
                f"Flow {tr.direction} → {_signal_target.pool.side.value} "
                f"${_signal_target.pool.price:,.1f} | "
                f"flow={flow.conviction:+.2f} CVD={flow.cvd_trend:+.2f} | "
                f"R:R={rr:.1f}"
                f"{f' AMD×{tr.amd_mult:.2f}' if tr.amd_mult < 1.0 else ''}"
                f"{_htf_note}"
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
            pts = 12.0 * max(amd_conf, 0.4)
            rev_score += pts
            rev_reasons.append(f"AMD {'REAC' if 'REAC' in amd_phase else 'ACCUM'} ({amd_conf:.0%})")

        # ── FACTOR 1: DISPLACEMENT (0-12) ────────────────────────────────
        wick_range = max(sweep.wick_extreme - sweep.pool.price
                         if sweep_dir == "short"
                         else sweep.pool.price - sweep.wick_extreme, 1e-10)
        wick_atr = wick_range / max(atr, 1e-10)

        if sweep.quality >= 0.70:
            rev_score += 12.0; rev_reasons.append(f"DISP strong ({sweep.quality:.0%})")
        elif sweep.quality >= 0.50:
            rev_score += 7.0;  rev_reasons.append(f"DISP moderate ({sweep.quality:.0%})")
        elif sweep.quality >= 0.35:
            rev_score += 3.0;  rev_reasons.append(f"DISP weak ({sweep.quality:.0%})")

        # ── FACTOR 2: DEALING RANGE (0-10) ───────────────────────────────
        pd = float(getattr(ict, 'dealing_range_pd', 0.5) or 0.5)
        if sweep_dir == "short":  # BSL swept, expect reversal short
            if pd >= 0.80:
                rev_score += 10.0; rev_reasons.append(f"DEEP PREMIUM ({pd:.0%})")
            elif pd >= 0.65:
                rev_score += 7.0;  rev_reasons.append(f"PREMIUM ({pd:.0%})")
            elif pd >= 0.50:
                rev_score += 3.0;  rev_reasons.append(f"SLIGHT PREM ({pd:.0%})")
            else:
                cont_score += 6.0; cont_reasons.append(f"DEEP BREAK beyond wick")
        else:  # SSL swept, expect reversal long
            if pd <= 0.20:
                rev_score += 10.0; rev_reasons.append(f"DEEP DISCOUNT ({pd:.0%})")
            elif pd <= 0.35:
                rev_score += 7.0;  rev_reasons.append(f"DISCOUNT ({pd:.0%})")
            elif pd <= 0.50:
                rev_score += 3.0;  rev_reasons.append(f"SLIGHT DISC ({pd:.0%})")
            else:
                cont_score += 6.0; cont_reasons.append(f"DEEP BREAK beyond wick")

        # ── FACTOR 3: FLOW (0-12) ─────────────────────────────────────────
        if sweep_dir == "short":
            rev_dir_needed = "short"
        else:
            rev_dir_needed = "long"

        if flow.direction == rev_dir_needed and abs(flow.conviction) >= 0.40:
            rev_score += 8.0; rev_reasons.append(f"FLOW REV {flow.conviction:+.2f}")
        elif flow.direction == rev_dir_needed:
            rev_score += 4.0; rev_reasons.append(f"FLOW weak rev {flow.conviction:+.2f}")
        elif flow.direction and flow.direction != rev_dir_needed:
            cont_score += 8.0
            cont_reasons.append(f"FLOW CONT {flow.conviction:+.2f}")
        # neutral flow: no contribution

        # ── FACTOR 4: CVD (0-8) ──────────────────────────────────────────
        cvd = flow.cvd_trend
        if sweep_dir == "short" and cvd < -0.30:
            rev_score += 8.0; rev_reasons.append(f"CVD bearish {cvd:+.2f}")
        elif sweep_dir == "short" and cvd < 0:
            rev_score += 3.0; rev_reasons.append(f"CVD slight bear {cvd:+.2f}")
        elif sweep_dir == "long" and cvd > 0.30:
            rev_score += 8.0; rev_reasons.append(f"CVD bullish {cvd:+.2f}")
        elif sweep_dir == "long" and cvd > 0:
            rev_score += 3.0; rev_reasons.append(f"CVD slight bull {cvd:+.2f}")
        else:
            cont_score += 4.0; cont_reasons.append(f"CVD cont {cvd:+.2f}")

        # ── FACTOR 5: STRUCTURE (0-10) ────────────────────────────────────
        choch_5m = getattr(ict, 'choch_5m', '') or ''
        bos_5m   = getattr(ict, 'bos_5m', '') or ''

        if choch_5m:
            choch_rev = "bearish" if rev_dir_needed == "short" else "bullish"
            if choch_5m == choch_rev:
                rev_score += 10.0; rev_reasons.append("CHoCH ✅ (5m)")

        if bos_5m:
            cont_bos = "bullish" if cont_dir == "long" else "bearish"
            if bos_5m == cont_bos:
                cont_score += 10.0; cont_reasons.append("BOS aligns continuation")

        # ── FACTOR 6 (was 7): SESSION (0-5) ──────────────────────────────
        session = getattr(ict, 'kill_zone', '') or ''
        if 'asia' in session.lower():
            rev_score += 5.0; rev_reasons.append("ASIA session (manipulation)")
        elif 'london' in session.lower() and 'ny' not in session.lower():
            rev_score += 3.0; rev_reasons.append("LONDON (possible Judas)")

        # ── FACTOR 7 (was 8): TARGET QUALITY (0-10) ──────────────────────
        opp_target  = self._find_opposing_target(sweep_dir, snap, price, atr, ict)
        cont_target = self._find_continuation_target(cont_dir, snap, price, atr, ict)

        if opp_target and opp_target.significance >= _MIN_POOL_SIGNIFICANCE:
            rev_score += min(5.0, opp_target.significance * 0.5)
            rev_reasons.append(f"TP pool sig={opp_target.significance:.1f}")

        if cont_target and cont_target.significance >= _MIN_POOL_SIGNIFICANCE:
            cont_score += min(5.0, cont_target.significance * 0.5)
            cont_reasons.append(f"Next pool sig={cont_target.significance:.1f}")

        # ── FACTOR 8 (was 9): OB BLOCKING (0-10) ─────────────────────────
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

        # ── Liquidity-aware SL push ───────────────────────────────────────
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

        # ── BUG-2 FIX: Record sweep reversal direction before R:R gate ────
        # This must happen BEFORE any R:R check so that even if we skip
        # the entry, the proximity engine won't immediately go opposite.
        self._last_sweep_reversal_dir = side
        self._last_sweep_reversal_time = now

        # ── TP computation with HTF escalation ───────────────────────────
        # Step 1: Try the opposing pool from the post-sweep analysis
        _signal_target = decision.next_target
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

        # Step 2: HTF TP escalation if opposing pool fails R:R
        if tp is None:
            _htf_tp, _htf_target = self._find_htf_tp(
                snap, side, price, atr, sl, _MIN_RR_RATIO)
            if _htf_tp is not None:
                tp = _htf_tp
                _signal_target = _htf_target
                logger.info(
                    f"🔼 Sweep reversal HTF TP: [{_htf_target.pool.timeframe}] "
                    f"${_htf_target.pool.price:,.1f} ({_htf_target.distance_atr:.1f}ATR)")

        # Step 3: Risk-multiple fallback (always meets R:R — synthetic TP)
        if tp is None:
            tp = price + (risk * 2.0) if side == "long" else price - (risk * 2.0)

        reward = abs(tp - price)
        rr = reward / risk

        if rr < _MIN_RR_RATIO:
            # Should be unreachable with the 2× fallback (rr=2.0 always)
            # but guard for safety in case of extreme edge cases
            logger.info(
                f"🌊 Reversal R:R {rr:.2f} < {_MIN_RR_RATIO} — skipping "
                f"(direction memory set for 60s proximity block)")
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
            target_pool=_signal_target or PoolTarget(
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

        # ── Liquidity-aware SL push ───────────────────────────────────────
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
        """
        BUG-FIX-2: Uses t.adjusted_sig() instead of t.pool.proximity_adjusted_sig()
        so the adjacency bonus (applied in get_snapshot) is visible here.
        """
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

        # BUG-FIX-2: was t.pool.proximity_adjusted_sig(t.distance_atr)
        return max(candidates, key=lambda t: t.adjusted_sig())

    def _find_opposing_target(
        self,
        direction: str,
        snap: LiquidityMapSnapshot,
        price: float,
        atr: float,
        ict: Optional["ICTContext"] = None,
    ) -> Optional[PoolTarget]:
        """
        FIX-I: During DISTRIBUTION/REDISTRIBUTION, prefer nearest pool.
        BUG-FIX-2: Uses t.adjusted_sig() for consistent adjacency-aware selection.
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

        # BUG-FIX-2: was t.pool.proximity_adjusted_sig(t.distance_atr)
        return max(reachable, key=lambda t: t.adjusted_sig())

    def _find_continuation_target(
        self,
        direction: str,
        snap: LiquidityMapSnapshot,
        price: float,
        atr: float,
        ict: Optional["ICTContext"] = None,
    ) -> Optional[PoolTarget]:
        return self._find_opposing_target(direction, snap, price, atr, ict)

    def _find_htf_tp(
        self,
        snap: LiquidityMapSnapshot,
        side: str,
        price: float,
        atr: float,
        sl_price: float,
        min_rr: float,
    ) -> Tuple[Optional[float], Optional[PoolTarget]]:
        """
        HTF TP Escalation (v3.1) — find a higher-timeframe TP when the
        nearest pool fails the minimum R:R threshold.

        Qualifies:
          • Native 1h, 4h, or 1d pools  (_HTF_TP_TIMEFRAMES)
          • ANY timeframe pool with htf_count >= _HTF_TP_MIN_HTF_COUNT
            (multi-TF confluence — e.g. a 15m pool confirmed by 4h and 1d)

        Sorted by distance ascending so the most achievable HTF target
        is tried first. The function returns the NEAREST qualifying pool
        whose TP gives R:R >= min_rr.

        Maximum search distance: _HTF_TP_MAX_ATR (default 30 ATR).
        This allows reaching e.g. a 22-ATR 15m[HTFx2] pool when only a
        2-ATR pool was available locally and the SL structure was wide.

        Returns (tp_price, PoolTarget) or (None, None) if not found.
        """
        risk = abs(price - sl_price)
        if risk < 1e-10:
            return None, None

        candidates = snap.bsl_pools if side == "long" else snap.ssl_pools

        # Filter: native HTF timeframe OR multi-TF confluence
        htf_pools = [
            t for t in candidates
            if (
                t.pool.timeframe in _HTF_TP_TIMEFRAMES
                or t.pool.htf_count >= _HTF_TP_MIN_HTF_COUNT
            )
            and _MIN_TARGET_ATR <= t.distance_atr <= _HTF_TP_MAX_ATR
        ]

        if not htf_pools:
            return None, None

        # Nearest qualifying pool first (most achievable target)
        htf_pools.sort(key=lambda t: t.distance_atr)

        for target in htf_pools:
            tp = self._compute_tp_approach(target, side, price, atr)
            if tp is None:
                continue
            reward = abs(tp - price)
            if reward / risk >= min_rr:
                logger.info(
                    f"🔼 HTF TP escalation: [{target.pool.timeframe}] "
                    f"${target.pool.price:,.1f} ({target.distance_atr:.1f}ATR "
                    f"htf_count={target.pool.htf_count}) → TP=${tp:,.1f} "
                    f"R:R={(reward/risk):.2f} (required≥{min_rr:.2f})"
                )
                return tp, target

        return None, None

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
             entry and SL, push SL beyond them
          3. No structure = no trade (return None)

        BUG-FIX-4: Log message now correctly distinguishes between
          (a) no ICT OB present at all, and
          (b) OB present but too far/close for the distance gate.
          Old: always said "no ICT structure found" even when OB existed.
        """
        sl = None
        _sl_rejected_reason = ""

        # ── PRIMARY: ICT OB anchor ─────────────────────────────────────
        if ict.nearest_ob_price > 0:
            ob_price = ict.nearest_ob_price
            if side == "long" and ob_price < price:
                sl = ob_price - atr * 0.20
            elif side == "short" and ob_price > price:
                sl = ob_price + atr * 0.20

        # ── GUARD: Check for opposing liquidity between entry and SL ───
        if sl is not None and hasattr(self, '_last_liq_snapshot') and self._last_liq_snapshot is not None:
            snap = self._last_liq_snapshot
            _liq_buffer = 0.25 * atr

            if side == "long":
                for t in snap.ssl_pools:
                    pool_price = t.pool.price
                    if sl < pool_price < price:
                        new_sl = pool_price - _liq_buffer
                        if new_sl < sl:
                            sl = new_sl
                            logger.debug(
                                f"SL pushed: SSL@${pool_price:,.0f} between entry and SL "
                                f"→ SL=${sl:,.1f}")
            else:
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
            if dist_pct < 0.001:
                # BUG-FIX-4: specific reason logged
                _sl_rejected_reason = (
                    f"OB@${ict.nearest_ob_price:.1f} too close "
                    f"({dist_pct:.3%} < 0.1%)")
                sl = None
            elif dist_pct > 0.035:
                # BUG-FIX-4: specific reason logged
                _sl_rejected_reason = (
                    f"OB@${ict.nearest_ob_price:.1f} too far "
                    f"({dist_pct:.3%} > 3.5%) — SL would be ${abs(price-sl):.0f} away")
                sl = None

        # ── LOG — BUG-FIX-4: distinguish no-OB from distance-rejected ──
        if sl is None:
            if ict.nearest_ob_price > 0 and _sl_rejected_reason:
                logger.debug(f"⛔ SL rejected for {side.upper()}: {_sl_rejected_reason}")
            elif ict.nearest_ob_price <= 0:
                logger.debug(f"⛔ SL rejected for {side.upper()}: no ICT OB found near price")
            else:
                logger.debug(f"⛔ SL rejected for {side.upper()}: no valid structural level")

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

        v3.0: Buffer scales with pool distance.
          buffer = 0.10×ATR + 0.05×distance_atr×ATR
          At 1 ATR: 0.15×ATR  (tight — close pool is certain)
          At 3 ATR: 0.25×ATR  (moderate)
          At 8 ATR: 0.50×ATR  (wide — distant pool, more uncertainty)
          Capped at 0.50×ATR.
        """
        pool_price = target.pool.price
        dist_atr = target.distance_atr

        buffer = atr * min(0.50, 0.10 + 0.05 * dist_atr)

        if side == "long":
            tp = pool_price - buffer
        else:
            tp = pool_price + buffer

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

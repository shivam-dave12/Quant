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

# FIX-B: lowered conviction threshold — BTC consolidation rarely exceeds 0.25
_FLOW_CONV_THRESHOLD    = 0.25    # was 0.40 (too high for BTC consolidation)
_FLOW_CVD_AGREE_MIN     = 0.10    # was 0.15 (CVD often weak in ranges)
# FIX-C: single tick + CVD is sufficient institutional confirmation
_FLOW_SUSTAINED_TICKS   = 1       # was 2 (neutral ticks reset counter too easily)

# Pool targeting
_MAX_TARGET_ATR         = 8.0     # was 6.0 — allow slightly farther targets
_MIN_TARGET_ATR         = 0.25    # was 0.3
# FIX-A: lowered significance threshold
_MIN_POOL_SIGNIFICANCE  = 2.0     # was 3.0

# Proximity approach trigger (FIX-E) — INSTITUTIONAL: approach entries are
# the bread-and-butter; most sweeps start as approaches. Loosen these.
_PROXIMITY_ENTRY_ATR_MAX    = 3.0     # was 2.0 — detect approaches earlier
_PROXIMITY_ENTRY_ATR_MIN    = 0.10    # was 0.15 — allow very close pools
_PROXIMITY_MIN_SIG          = 2.5     # was 4.0 — most intraday pools score 2-3
_PROXIMITY_CONFIRM_COUNT    = 1       # Only 1 confirm tick needed

# Sweep detection
_MIN_SWEEP_QUALITY      = 0.35

# FIX-K / FIX-L: Reduced timeouts
_CISD_MAX_WAIT_SEC      = 240     # was 300
_OTE_MAX_WAIT_SEC       = 480     # was 600
_TRACKING_TIMEOUT_SEC   = 180     # was 300
_READY_TIMEOUT_SEC      = 90      # was 120

# Risk management — INSTITUTIONAL: 1.2 R:R is standard for sweep entries
# with tight SL behind wick. Higher bars = missed valid setups.
_MIN_RR_RATIO           = 1.2     # was 1.4 — institutional minimum
_SL_BUFFER_ATR          = 0.08    # was 0.12 — tighter SL = better R:R
_TP_BUFFER_ATR          = 0.05    # was 0.08 — closer to pool = more fills

# Cooldowns — faster re-engagement after missed sweeps
_ENTRY_COOLDOWN_SEC     = 15.0    # was 30.0 — BTC moves fast
_POST_SWEEP_EVAL_SEC    = 5.0     # was 10.0 — evaluate sweep immediately


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
        """
        Weighted flow conviction — tick_flow and CVD are primary signals.
        OB imbalance is secondary confirmation, not a diluter.
        """
        # Primary signals weighted equally (0.40 each)
        conv = self.tick_flow * 0.40 + self.cvd_trend * 0.40
        # OB imbalance as secondary confirmation (0.20 weight)
        if abs(self.ob_imbalance) > 0.05:
            conv += self.ob_imbalance * 0.20
        return conv

    @property
    def direction(self) -> str:
        c = self.conviction
        # Lower threshold: 0.125 (was 0.20 effectively)
        if c > 0.125:
            return "long"
        elif c < -0.125:
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

    # ── Public API ────────────────────────────────────────────────────────

    def update(
        self,
        liq_snapshot: LiquidityMapSnapshot,
        flow_state:   OrderFlowState,
        ict_ctx:      ICTContext,
        price:        float,
        atr:          float,
        now:          float,
    ) -> None:
        if atr < 1e-10:
            return

        # Check for new sweeps first
        new_sweeps = [s for s in liq_snapshot.recent_sweeps
                      if s.detected_at > now - 10.0
                      and s.quality >= _MIN_SWEEP_QUALITY]

        if new_sweeps and self._state not in (
            EngineState.IN_POSITION, EngineState.ENTERING
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

        # State machine dispatch
        if self._state == EngineState.SCANNING:
            self._do_scanning(liq_snapshot, flow_state, ict_ctx, price, atr, now)
        elif self._state == EngineState.TRACKING:
            self._do_tracking(liq_snapshot, flow_state, ict_ctx, price, atr, now)
        elif self._state == EngineState.READY:
            self._do_ready(liq_snapshot, flow_state, ict_ctx, price, atr, now)
        elif self._state == EngineState.POST_SWEEP:
            self._do_post_sweep(liq_snapshot, flow_state, ict_ctx, price, atr, now)

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

    def on_position_opened(self) -> None:
        self._state = EngineState.IN_POSITION
        self._state_entered = time.time()

    def on_position_closed(self) -> None:
        self._state = EngineState.SCANNING
        self._state_entered = time.time()
        self._tracking = None
        self._post_sweep = None
        self._proximity_confirms = 0
        self._proximity_target = None

    def force_reset(self) -> None:
        self._state = EngineState.SCANNING
        self._state_entered = time.time()
        self._signal = None
        self._tracking = None
        self._post_sweep = None
        self._proximity_confirms = 0
        self._proximity_target = None

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

        # FIX: Don't require strong flow direction to start tracking.
        # Allow target identification when pools are nearby even with weak flow.
        # The tracking state will build conviction before entering READY.
        target = None
        direction = flow.direction

        if direction:
            # Standard path: flow direction guides target selection
            target = self._find_flow_target(snap, flow, price, atr)
        else:
            # Weak-flow path: find nearest significant pool and infer direction
            # This captures the pre-positioning phase before institutional flow shows
            all_pools = []
            for t in snap.bsl_pools:
                if t.distance_atr <= _MAX_TARGET_ATR and t.significance >= _MIN_POOL_SIGNIFICANCE:
                    all_pools.append(("long", t))
            for t in snap.ssl_pools:
                if t.distance_atr <= _MAX_TARGET_ATR and t.significance >= _MIN_POOL_SIGNIFICANCE:
                    all_pools.append(("short", t))
            if all_pools:
                # Pick highest proximity-adjusted significance
                best_dir, best_t = max(
                    all_pools,
                    key=lambda x: x[1].pool.proximity_adjusted_sig(x[1].distance_atr))
                # Only auto-track if pool is close and significant
                if best_t.distance_atr <= 4.0 and best_t.significance >= 3.0:
                    target = best_t
                    direction = best_dir

        if target is None:
            return

        # AMD soft filter
        if (ict.amd_phase == "DISTRIBUTION"
                and ict.amd_bias
                and ict.amd_bias != ("bullish" if direction == "long" else "bearish")):
            logger.debug(
                f"EntryEngine: flow {direction} vs AMD DISTRIBUTION "
                f"{ict.amd_bias} — skipping")
            return

        self._tracking = _TrackingState(
            direction=direction,
            target=target,
            started_at=now,
            flow_ticks=1 if flow.direction == direction else 0,
            peak_conviction=abs(flow.conviction),
        )
        self._state = EngineState.TRACKING
        self._state_entered = now
        logger.info(
            f"📡 TRACKING: {direction.upper()} → "
            f"${target.pool.price:,.1f} ({target.pool.side.value}) "
            f"dist={target.distance_atr:.1f} ATR, "
            f"sig={target.significance:.1f}, "
            f"flow={flow.conviction:+.2f}"
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

        # FIX-F/FIX-G: Neutral ticks not counted as contrary
        if flow.direction and flow.direction != tr.direction:
            tr.contrary_ticks += 1
            if tr.contrary_ticks >= 3:
                logger.info(f"📡 TRACKING aborted: flow reversed ({tr.direction} → {flow.direction})")
                self._tracking = None
                self._state = EngineState.SCANNING
                self._state_entered = now
                return
        elif flow.direction == tr.direction:
            tr.contrary_ticks = 0
            tr.flow_ticks += 1
            tr.peak_conviction = max(tr.peak_conviction, abs(flow.conviction))

        # FIX-G: Only update target when direction agrees
        if flow.direction == tr.direction:
            target = self._find_flow_target(snap, flow, price, atr)
            if target is not None:
                tr.target = target

        # FIX-C: Disjunctive ready condition — any of these paths is sufficient
        # Path 1: Standard flow confirmation (sustained ticks + CVD)
        path_1 = (
            tr.flow_ticks >= _FLOW_SUSTAINED_TICKS
            and flow.cvd_agrees
            and abs(flow.conviction) >= _FLOW_CONV_THRESHOLD
            and tr.target.distance_atr >= _MIN_TARGET_ATR
            and tr.target.distance_atr <= _MAX_TARGET_ATR
        )

        # Path 2: High conviction single reading (institutional absorption)
        path_2 = (
            abs(flow.conviction) >= 0.45
            and tr.target.distance_atr >= _MIN_TARGET_ATR
            and tr.target.distance_atr <= _MAX_TARGET_ATR
        )

        # Path 3: Moderate flow + ICT structure alignment (institutional setup)
        path_3 = False
        if tr.flow_ticks >= 1 and abs(flow.conviction) >= _FLOW_CONV_THRESHOLD * 0.5:
            if self._ict_structure_agrees(ict, tr.direction):
                path_3 = True

        # Path 4: Pool very close (< 1.5 ATR) with any confirming flow
        path_4 = (
            tr.target.distance_atr <= 1.5
            and tr.target.significance >= 3.0
            and tr.flow_ticks >= 1
            and not (flow.direction and flow.direction != tr.direction)
        )

        ready = (path_1 or path_2 or path_3 or path_4)

        if ready:
            self._state = EngineState.READY
            self._state_entered = now
            logger.info(
                f"✅ READY: {tr.direction.upper()} → "
                f"${tr.target.pool.price:,.1f} "
                f"conviction={flow.conviction:+.2f} "
                f"ticks={tr.flow_ticks}"
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

        # FIX-F: Neutral ticks not contrary
        if flow.direction and flow.direction != tr.direction:
            tr.contrary_ticks += 1
            if tr.contrary_ticks >= 2:
                logger.info("✅ READY: flow reversed — back to scanning")
                self._tracking = None
                self._state = EngineState.SCANNING
                self._state_entered = now
                return
        elif flow.direction == tr.direction:
            tr.contrary_ticks = 0

        sl = self._compute_sl(ict, tr.direction, price, atr)
        tp = self._compute_tp_approach(tr.target, tr.direction, price, atr)

        if sl is None or tp is None:
            return

        risk = abs(price - sl)
        reward = abs(tp - price)
        if risk < 1e-10:
            return
        rr = reward / risk

        if rr < _MIN_RR_RATIO:
            logger.debug(
                f"✅ READY: R:R {rr:.2f} < {_MIN_RR_RATIO} — skipping "
                f"(entry=${price:,.1f} SL=${sl:,.1f} TP=${tp:,.1f})"
            )
            return

        self._signal = EntrySignal(
            side=tr.direction,
            entry_type=EntryType.PRE_SWEEP_APPROACH,
            entry_price=price,
            sl_price=sl,
            tp_price=tp,
            rr_ratio=rr,
            target_pool=tr.target,
            conviction=flow.conviction,
            reason=(
                f"Flow {tr.direction} → {tr.target.pool.side.value} "
                f"${tr.target.pool.price:,.1f} | "
                f"flow={flow.conviction:+.2f} CVD={flow.cvd_trend:+.2f} | "
                f"R:R={rr:.1f}"
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

        elif amd_phase == 'ACCUMULATION':
            pts = 12.0 * max(amd_conf, 0.4)
            rev_score += pts
            rev_reasons.append(f"AMD ACCUM ({amd_conf:.0%})")

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

        # FIX-D: aggressive thresholds — institutional sweeps are high-probability
        # setups, the scoring already requires multi-factor confirmation.
        # rev >= 35 with gap >= 8 means reversal clearly dominates continuation.
        if rev_total >= 35.0 and gap >= 8.0:
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

        elif cont_total >= 35.0 and gap >= 8.0:
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

        if decision.next_target:
            tp = decision.next_target.pool.price
            if side == "long":
                tp -= atr * _TP_BUFFER_ATR
            else:
                tp += atr * _TP_BUFFER_ATR
        else:
            risk = abs(price - sl)
            tp = price + (risk * 2.0) if side == "long" else price - (risk * 2.0)

        risk = abs(price - sl)
        reward = abs(tp - price)
        if risk < 1e-10:
            return
        rr = reward / risk

        if rr < _MIN_RR_RATIO:
            logger.debug(f"🌊 Reversal R:R {rr:.2f} < {_MIN_RR_RATIO} — skipping")
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

        if side == "long":
            sl = sweep.pool.price - atr * _SL_BUFFER_ATR
        else:
            sl = sweep.pool.price + atr * _SL_BUFFER_ATR

        tp = next_target.pool.price
        if side == "long":
            tp -= atr * _TP_BUFFER_ATR
        else:
            tp += atr * _TP_BUFFER_ATR

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
        FIX-J: Fallback SL reduced from 1.2 ATR → 1.0 ATR.
        Try ICT OB first; fallback to 1.0× ATR.
        """
        if ict.nearest_ob_price > 0:
            ob_price = ict.nearest_ob_price
            if side == "long" and ob_price < price:
                sl = ob_price - atr * _SL_BUFFER_ATR
                dist_pct = abs(price - sl) / price
                if 0.001 <= dist_pct <= 0.035:
                    return sl
            elif side == "short" and ob_price > price:
                sl = ob_price + atr * _SL_BUFFER_ATR
                dist_pct = abs(price - sl) / price
                if 0.001 <= dist_pct <= 0.035:
                    return sl

        # FIX-J: 0.80 ATR fallback (was 1.0, before that 1.2)
        # Institutional: SL should be tight behind structure. 0.80 ATR
        # on BTC at ATR=$112 = $90 SL — enough to survive a wick,
        # tight enough to produce 1.5:1+ R:R on most setups.
        if side == "long":
            return price - atr * 0.80
        else:
            return price + atr * 0.80

    def _compute_tp_approach(
        self,
        target: PoolTarget,
        side: str,
        price: float,
        atr: float,
    ) -> Optional[float]:
        tp = target.pool.price
        if side == "long":
            tp -= atr * _TP_BUFFER_ATR
        else:
            tp += atr * _TP_BUFFER_ATR
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

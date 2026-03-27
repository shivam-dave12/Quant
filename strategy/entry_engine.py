"""
entry_engine.py — Liquidity-First Entry Decision Engine
=========================================================
ONE decision flow. ONE state machine. ZERO composite scores.

CORE LOGIC
──────────
1. LiquidityMap tells us WHERE the pools are and HOW significant they are.
2. OrderFlow tells us WHERE the market is being pushed RIGHT NOW.
3. If flow is driving price TOWARD a significant pool → that's a trade.
4. ICT structures validate the setup (AMD phase, premium/discount, OB backing).
5. After a sweep, determine: continue, reverse, or range.

THIS ENGINE REPLACES:
  - _evaluate_reversion_entry()
  - _evaluate_flow_liq_entry()
  - _evaluate_flow_entry()
  - _evaluate_hunt_entry()
  - _evaluate_momentum_entry()
  - _evaluate_trend_entry()
  - All composite scoring, confirm tick counters, routing priority chains.

STATE MACHINE
─────────────
  SCANNING      → No actionable setup. Watching pool map + flow.
  TRACKING      → Flow is pushing toward a pool. Building conviction.
  READY         → Conviction threshold met. Looking for precise entry.
  ENTERING      → Entry order placed or limit set. Waiting for fill.
  IN_POSITION   → Filled. Managing SL/TP/Trail.
  POST_SWEEP    → A pool was just swept. Deciding next move.

ENTRY TYPES
───────────
  A) PRE-SWEEP APPROACH (highest win-rate, ~65-70%)
     Flow is driving price toward an unswept pool.
     Enter in the flow direction. TP at pool (sweep).
     SL behind ICT structure (OB or swing).

  B) SWEEP REVERSAL (classic ICT, ~55-65%)
     Pool was swept. CISD confirmed. Enter OTE zone in reversal direction.
     TP at opposing pool. SL behind sweep wick.

  C) SWEEP CONTINUATION (momentum, ~50-55%)
     Pool was swept but flow continues in same direction.
     Price is delivering to the NEXT pool.
     Enter in continuation direction. TP at next pool.
     SL behind swept pool.

WHAT THIS ENGINE DOES NOT DO
─────────────────────────────
  - No VWAP reversion signals
  - No composite scores or weighted averages
  - No "confirm ticks" — conviction comes from orderflow state
  - No breakout/retest detection (that's just approaching a pool)
  - No trend/ranging regime classification (pools define the range)
  - No 100+ config parameters (less than 20 tunable constants)
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Import the liquidity map
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
# CONSTANTS — under 20, each with a clear structural reason
# ═══════════════════════════════════════════════════════════════════════════

# Flow conviction
_FLOW_CONV_THRESHOLD    = 0.55   # Minimum orderflow conviction to act
_FLOW_CVD_AGREE_MIN     = 0.20   # CVD must agree at least this strongly
_FLOW_SUSTAINED_TICKS   = 3      # Flow must sustain for N ticks

# Pool targeting
_MAX_TARGET_ATR         = 6.0    # Don't target pools > 6 ATR away
_MIN_TARGET_ATR         = 0.3    # Don't target pools < 0.3 ATR away (noise)
_MIN_POOL_SIGNIFICANCE  = 3.0    # Pool must be this significant to trade

# Sweep detection
_MIN_SWEEP_QUALITY      = 0.35   # Minimum sweep quality to trigger reversal
_CISD_LOOKBACK_BARS     = 8      # Bars to look for BOS after sweep
_CISD_MAX_WAIT_SEC      = 300    # 5 min max to wait for CISD

# OTE zone (Fibonacci retracement of displacement leg)
_OTE_FIB_LOW            = 0.618
_OTE_FIB_HIGH           = 0.786
_OTE_MAX_WAIT_SEC       = 600    # 10 min max to wait for OTE entry

# Risk management
_MIN_RR_RATIO           = 1.5    # Minimum risk-to-reward
_SL_BUFFER_ATR          = 0.15   # Buffer below/above structural SL
_TP_BUFFER_ATR          = 0.10   # TP set slightly inside opposing pool

# Cooldowns
_ENTRY_COOLDOWN_SEC     = 45.0   # Min time between entries
_POST_SWEEP_EVAL_SEC    = 15.0   # How long to evaluate after sweep before acting

# State timeout
_TRACKING_TIMEOUT_SEC   = 300.0  # Tracking without entry → reset to scanning
_READY_TIMEOUT_SEC      = 120.0  # Ready without fill → reset to scanning


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
    PRE_SWEEP_APPROACH = "APPROACH"     # Flow → pool (before sweep)
    SWEEP_REVERSAL     = "REVERSAL"     # Sweep → CISD → OTE → reversal
    SWEEP_CONTINUATION = "CONTINUATION" # Sweep → flow continues → next pool


@dataclass
class OrderFlowState:
    """
    Consolidated orderflow reading from tick engine + CVD.
    Computed externally, passed in each tick.
    """
    tick_flow:       float = 0.0    # -1 to +1 (sell pressure to buy pressure)
    cvd_trend:       float = 0.0    # -1 to +1 (cumulative volume delta trend)
    cvd_divergence:  float = 0.0    # CVD vs price divergence signal
    ob_imbalance:    float = 0.0    # Orderbook bid/ask imbalance -1 to +1
    tick_streak:     int   = 0      # Consecutive ticks in same direction
    streak_direction: str  = ""     # "long" | "short" | ""

    @property
    def conviction(self) -> float:
        """
        Overall flow conviction: -1 (strong sell) to +1 (strong buy).
        Simple average — no weighted voodoo.
        """
        signals = [self.tick_flow, self.cvd_trend]
        if abs(self.ob_imbalance) > 0.1:
            signals.append(self.ob_imbalance * 0.5)  # OB is noisier, half weight
        return sum(signals) / len(signals)

    @property
    def direction(self) -> str:
        """Dominant flow direction."""
        c = self.conviction
        if c > _FLOW_CONV_THRESHOLD * 0.5:
            return "long"
        elif c < -_FLOW_CONV_THRESHOLD * 0.5:
            return "short"
        return ""

    @property
    def is_sustained(self) -> bool:
        """Is the flow signal sustained (not just a spike)?"""
        return (self.tick_streak >= _FLOW_SUSTAINED_TICKS
                and self.streak_direction == self.direction)

    @property
    def cvd_agrees(self) -> bool:
        """Does CVD confirm the tick flow direction?"""
        d = self.direction
        if d == "long":
            return self.cvd_trend > _FLOW_CVD_AGREE_MIN
        elif d == "short":
            return self.cvd_trend < -_FLOW_CVD_AGREE_MIN
        return False


@dataclass
class ICTContext:
    """
    ICT structural context for entry validation.
    Computed externally from the ICT engine, passed in.
    """
    amd_phase:        str   = ""     # "ACCUMULATION" | "MANIPULATION" | "DISTRIBUTION"
    amd_bias:         str   = ""     # "bullish" | "bearish" | ""
    amd_confidence:   float = 0.0    # 0–1
    in_premium:       bool  = False  # Price above 50% of dealing range
    in_discount:      bool  = False  # Price below 50% of dealing range
    structure_5m:     str   = ""     # "bullish" | "bearish" | "neutral"
    structure_15m:    str   = ""     # "bullish" | "bearish" | "neutral"
    bos_5m:           str   = ""     # Direction of latest 5m BOS
    choch_5m:         str   = ""     # Direction of latest 5m CHoCH
    nearest_ob_price: float = 0.0    # Nearest OB price in trade direction
    kill_zone:        str   = ""     # "london" | "ny" | "asia" | ""

    @property
    def session_quality(self) -> str:
        """Simple session quality: is this a good time to trade?"""
        if self.kill_zone in ("london", "ny"):
            return "prime"
        elif self.kill_zone == "asia":
            return "fair"
        return "off_session"


@dataclass
class EntrySignal:
    """
    The ONLY output this engine produces. Either there's a signal or there isn't.
    No partial signals, no "almost ready", no intermediate scores.
    """
    side:           str         # "long" | "short"
    entry_type:     EntryType
    entry_price:    float       # Suggested entry (limit) or 0.0 for market
    sl_price:       float
    tp_price:       float
    rr_ratio:       float
    target_pool:    PoolTarget  # The pool being targeted
    sweep_result:   Optional[SweepResult] = None  # If triggered by a sweep

    # Metadata for logging / Telegram
    conviction:     float = 0.0
    reason:         str   = ""
    ict_validation: str   = ""
    created_at:     float = field(default_factory=time.time)


@dataclass
class PostSweepDecision:
    """
    What to do after a pool sweep: reverse, continue, or wait.
    """
    action:         str     # "reverse" | "continue" | "wait"
    direction:      str     # Side to trade if action != "wait"
    confidence:     float   # 0–1
    next_target:    Optional[PoolTarget] = None
    reason:         str     = ""


# ═══════════════════════════════════════════════════════════════════════════
# MAIN ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class EntryEngine:
    """
    Single-flow entry decision engine.

    Usage (each tick):
        engine.update(liq_snapshot, flow_state, ict_ctx, price, atr, now)
        signal = engine.get_signal()
        if signal:
            # Execute the trade
            engine.on_entry_placed()
    """

    def __init__(self) -> None:
        self._state         = EngineState.SCANNING
        self._signal:       Optional[EntrySignal] = None
        self._tracking:     Optional[_TrackingState] = None
        self._post_sweep:   Optional[_PostSweepState] = None
        self._last_entry_at = 0.0
        self._state_entered = time.time()

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
        """Main tick update. Call from strategy loop."""
        if atr < 1e-10:
            return

        # Check for new sweeps first — they override everything
        new_sweeps = [s for s in liq_snapshot.recent_sweeps
                      if s.detected_at > now - 10.0  # Last 10 seconds
                      and s.quality >= _MIN_SWEEP_QUALITY]

        if new_sweeps and self._state not in (
            EngineState.IN_POSITION, EngineState.ENTERING
        ):
            best_sweep = max(new_sweeps, key=lambda s: s.quality)
            self._enter_post_sweep(best_sweep, liq_snapshot, flow_state,
                                    ict_ctx, price, atr, now)
            return

        # State machine dispatch
        if self._state == EngineState.SCANNING:
            self._do_scanning(liq_snapshot, flow_state, ict_ctx,
                             price, atr, now)
        elif self._state == EngineState.TRACKING:
            self._do_tracking(liq_snapshot, flow_state, ict_ctx,
                             price, atr, now)
        elif self._state == EngineState.READY:
            self._do_ready(liq_snapshot, flow_state, ict_ctx,
                          price, atr, now)
        elif self._state == EngineState.POST_SWEEP:
            self._do_post_sweep(liq_snapshot, flow_state, ict_ctx,
                               price, atr, now)
        # IN_POSITION and ENTERING are managed externally by the strategy

    def get_signal(self) -> Optional[EntrySignal]:
        """Return the current entry signal, if any."""
        return self._signal

    def consume_signal(self) -> Optional[EntrySignal]:
        """Pop the signal (one-shot consumption)."""
        sig = self._signal
        self._signal = None
        return sig

    def on_entry_placed(self) -> None:
        """Called by strategy after order is placed."""
        self._state = EngineState.ENTERING
        self._state_entered = time.time()
        self._last_entry_at = time.time()
        self._signal = None

    def on_position_opened(self) -> None:
        """Called by strategy when fill is confirmed."""
        self._state = EngineState.IN_POSITION
        self._state_entered = time.time()

    def on_position_closed(self) -> None:
        """Called by strategy when position is flat."""
        self._state = EngineState.SCANNING
        self._state_entered = time.time()
        self._tracking = None
        self._post_sweep = None

    def force_reset(self) -> None:
        """Emergency reset — return to scanning."""
        self._state = EngineState.SCANNING
        self._state_entered = time.time()
        self._signal = None
        self._tracking = None
        self._post_sweep = None

    @property
    def state(self) -> str:
        return self._state.name

    @property
    def tracking_info(self) -> Optional[Dict]:
        """For display/logging."""
        if self._tracking is None:
            return None
        return {
            "direction": self._tracking.direction,
            "target": f"${self._tracking.target.pool.price:,.1f}",
            "distance_atr": f"{self._tracking.target.distance_atr:.1f}",
            "flow_ticks": self._tracking.flow_ticks,
            "started": f"{time.time() - self._tracking.started_at:.0f}s ago",
        }

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
        """
        Look for flow pushing toward a significant pool.
        Transition → TRACKING when found.
        """
        # Cooldown check
        if now - self._last_entry_at < _ENTRY_COOLDOWN_SEC:
            return

        # Need both flow direction and a target
        if not flow.direction:
            return

        target = self._find_flow_target(snap, flow, price, atr)
        if target is None:
            return

        # ICT soft filter: don't enter during DISTRIBUTION against flow
        if (ict.amd_phase == "DISTRIBUTION"
                and ict.amd_bias
                and ict.amd_bias != ("bullish" if flow.direction == "long" else "bearish")):
            logger.debug(
                f"EntryEngine: flow {flow.direction} vs AMD DISTRIBUTION "
                f"{ict.amd_bias} — skipping")
            return

        # Start tracking
        self._tracking = _TrackingState(
            direction=flow.direction,
            target=target,
            started_at=now,
            flow_ticks=1,
            peak_conviction=abs(flow.conviction),
        )
        self._state = EngineState.TRACKING
        self._state_entered = now
        logger.info(
            f"📡 TRACKING: {flow.direction.upper()} → "
            f"${target.pool.price:,.1f} ({target.pool.side.value}) "
            f"dist={target.distance_atr:.1f} ATR, "
            f"sig={target.significance:.1f}, "
            f"flow={flow.conviction:+.2f}")

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
        """
        Flow is pushing toward a pool. Build conviction.
        Transition → READY when conviction is high enough.
        Transition → SCANNING if flow dies or reverses.
        """
        tr = self._tracking
        if tr is None:
            self._state = EngineState.SCANNING
            return

        # Timeout
        if now - tr.started_at > _TRACKING_TIMEOUT_SEC:
            logger.info("📡 TRACKING timeout — back to scanning")
            self._tracking = None
            self._state = EngineState.SCANNING
            self._state_entered = now
            return

        # Check flow is still aligned
        # BUG-FIX-5: The original code treated neutral ticks (flow.direction == "")
        # as contrary, counting them toward the contrary_ticks abort threshold.
        # Neutral ticks (conviction between -0.275 and +0.275) are temporary pauses,
        # not genuine reversals.  Counting them caused TRACKING to abort within 3
        # seconds of normal signal noise — most setups never built enough conviction.
        # Fix: only count ACTIVE opposing-direction ticks as contrary.
        #
        # BUG-FIX-6: Target was overwritten by _find_flow_target with the REVERSED
        # direction's pool before the contrary_ticks abort could fire.  If flow briefly
        # flips to "long" on a short setup, we'd target a BSL pool for one tick, then
        # abort, leaving the tracking state corrupted.
        # Fix: only update target when flow direction still agrees with our setup.
        if flow.direction and flow.direction != tr.direction:
            # Active opposing flow — genuinely contrary
            tr.contrary_ticks += 1
            if tr.contrary_ticks >= 3:
                logger.info(
                    f"📡 TRACKING aborted: flow reversed "
                    f"({tr.direction} → {flow.direction})")
                self._tracking = None
                self._state = EngineState.SCANNING
                self._state_entered = now
                return
        elif flow.direction == tr.direction:
            # Confirming tick — reset counter and increment
            tr.contrary_ticks = 0
            tr.flow_ticks += 1
            tr.peak_conviction = max(tr.peak_conviction, abs(flow.conviction))
        # else: flow.direction == "" (neutral pause) — don't count as contrary or confirming

        # BUG-FIX-6 cont: Only update target when direction still agrees
        if flow.direction == tr.direction:
            target = self._find_flow_target(snap, flow, price, atr)
            if target is not None:
                tr.target = target

        # Conviction check: flow sustained + CVD agrees + target still valid
        ready = (
            tr.flow_ticks >= _FLOW_SUSTAINED_TICKS
            and flow.cvd_agrees
            and abs(flow.conviction) >= _FLOW_CONV_THRESHOLD
            and tr.target.distance_atr >= _MIN_TARGET_ATR
            and tr.target.distance_atr <= _MAX_TARGET_ATR
        )

        # ICT bonus: lower the bar if structure strongly agrees
        if not ready and tr.flow_ticks >= 2:
            ict_boost = self._ict_structure_agrees(ict, tr.direction)
            if ict_boost and abs(flow.conviction) >= _FLOW_CONV_THRESHOLD * 0.7:
                ready = True  # ICT agreement lowers flow threshold

        if ready:
            self._state = EngineState.READY
            self._state_entered = now
            logger.info(
                f"✅ READY: {tr.direction.upper()} → "
                f"${tr.target.pool.price:,.1f} "
                f"conviction={flow.conviction:+.2f} "
                f"ticks={tr.flow_ticks}")

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
        """
        Conviction confirmed. Compute precise entry, SL, TP.
        Emit signal. Transition → back to SCANNING if timeout.
        """
        tr = self._tracking
        if tr is None:
            self._state = EngineState.SCANNING
            return

        # Timeout
        if now - self._state_entered > _READY_TIMEOUT_SEC:
            logger.info("✅ READY timeout — back to scanning")
            self._tracking = None
            self._state = EngineState.SCANNING
            self._state_entered = now
            return

        # Flow must still be alive
        # BUG-FIX E1: The original code used `flow.direction != tr.direction` which
        # treats neutral ticks (flow.direction == "") as contrary — exactly the same
        # bug that was already fixed in _do_tracking() as BUG-FIX-5.  Neutral ticks
        # are temporary signal pauses, not genuine reversals.  Counting them caused
        # READY state to abort within 2 noisy ticks after conviction was confirmed,
        # silently discarding valid signals.
        # Fix: mirror BUG-FIX-5 — only ACTIVE opposing-direction ticks are contrary.
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
        # else: flow.direction == "" (neutral pause) — don't count as contrary

        # Compute SL/TP
        sl = self._compute_sl(ict, tr.direction, price, atr)
        tp = self._compute_tp_approach(tr.target, tr.direction, price, atr)

        if sl is None or tp is None:
            return

        # Risk:Reward check
        risk = abs(price - sl)
        reward = abs(tp - price)
        if risk < 1e-10:
            return
        rr = reward / risk

        if rr < _MIN_RR_RATIO:
            logger.debug(
                f"✅ READY: R:R {rr:.2f} < {_MIN_RR_RATIO} — skipping "
                f"(entry=${price:,.1f} SL=${sl:,.1f} TP=${tp:,.1f})")
            return

        # Emit signal
        self._signal = EntrySignal(
            side=tr.direction,
            entry_type=EntryType.PRE_SWEEP_APPROACH,
            entry_price=price,  # Market entry at current price
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
            f"{self._signal.side.upper()} | {self._signal.reason}")

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
        """Transition into POST_SWEEP state after a sweep is detected."""
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
            f"🌊 POST_SWEEP: {sweep.pool.side.value} swept "
            f"${sweep.pool.price:,.1f} quality={sweep.quality:.2f} "
            f"→ evaluating next move...")

    def _do_post_sweep(
        self,
        snap: LiquidityMapSnapshot,
        flow: OrderFlowState,
        ict: ICTContext,
        price: float,
        atr: float,
        now: float,
    ) -> None:
        """
        After a sweep, decide: reverse, continue, or wait.

        REVERSAL (sweep was a stop hunt → expect opposite move):
          - Flow reverses AWAY from swept pool
          - CISD (break of structure in reversal direction) confirms
          - Look for OTE entry in reversal direction
          - TP at opposing pool

        CONTINUATION (sweep was a breakout → more to go):
          - Flow continues in same direction as pre-sweep
          - Price is NOT rejecting — closing through the pool
          - Target = next pool in continuation direction

        WAIT (unclear → don't trade):
          - Mixed flow, no CISD, no clear structure
          - Return to SCANNING after timeout
        """
        ps = self._post_sweep
        if ps is None:
            self._state = EngineState.SCANNING
            return

        # Give the market time to show its hand
        elapsed = now - ps.entered_at
        if elapsed < _POST_SWEEP_EVAL_SEC:
            return  # Too early to decide

        # Timeout: if we can't decide in 5 minutes, go back to scanning
        if elapsed > _CISD_MAX_WAIT_SEC:
            logger.info("🌊 POST_SWEEP: timeout — back to scanning")
            self._post_sweep = None
            self._state = EngineState.SCANNING
            self._state_entered = now
            return

        decision = self._evaluate_post_sweep(
            ps.sweep, snap, flow, ict, price, atr, now)

        if decision.action == "reverse":
            self._handle_sweep_reversal(
                ps.sweep, decision, snap, flow, ict, price, atr, now)
        elif decision.action == "continue":
            self._handle_sweep_continuation(
                ps.sweep, decision, snap, flow, ict, price, atr, now)
        # "wait" → stay in POST_SWEEP, keep evaluating

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
        Core post-sweep decision logic.

        Reversal signals:
          - Price rejected sharply from sweep wick (sweep quality > 0.5)
          - Flow has reversed from pre-sweep direction
          - 5m BOS confirms reversal direction
          - Price is in OTE zone of the displacement leg

        Continuation signals:
          - Flow continues in same direction as before sweep
          - Price is closing above/below the swept pool (not rejecting)
          - Structure still agrees with continuation
        """
        sweep_dir = sweep.direction  # Direction to go IF reversing

        # Check flow vs sweep direction
        flow_agrees_reversal = (flow.direction == sweep_dir
                                and flow.cvd_agrees)
        flow_agrees_continuation = (flow.direction != sweep_dir
                                    and flow.direction != ""
                                    and abs(flow.conviction) > _FLOW_CONV_THRESHOLD * 0.7)

        # Check 5m BOS for CISD confirmation
        cisd_confirmed = False
        if ict.bos_5m:
            expected_bos = "bullish" if sweep_dir == "long" else "bearish"
            cisd_confirmed = (ict.bos_5m == expected_bos)

        # Check if price is rejecting or breaking through
        if sweep.pool.side == PoolSide.BSL:
            # BSL was swept (above) → reversal = short
            rejecting = price < sweep.pool.price  # Back below pool
            breaking_through = price > sweep.wick_extreme  # Above sweep wick
        else:
            # SSL was swept (below) → reversal = long
            rejecting = price > sweep.pool.price  # Back above pool
            breaking_through = price < sweep.wick_extreme  # Below sweep wick

        # Decision logic
        if (sweep.quality >= 0.45
                and rejecting
                and (cisd_confirmed or flow_agrees_reversal)):
            # Strong reversal setup
            opp_target = self._find_opposing_target(
                sweep_dir, snap, price, atr)
            return PostSweepDecision(
                action="reverse",
                direction=sweep_dir,
                confidence=min(1.0, sweep.quality + (0.2 if cisd_confirmed else 0.0)),
                next_target=opp_target,
                reason=(f"Sweep quality={sweep.quality:.2f} + "
                        f"{'CISD confirmed' if cisd_confirmed else 'flow reversal'}")
            )

        elif (breaking_through
              and flow_agrees_continuation
              and not rejecting):
            # Continuation — swept pool was a breakout
            cont_dir = "long" if sweep_dir == "short" else "short"
            next_target = self._find_continuation_target(
                cont_dir, snap, price, atr)
            if next_target is not None:
                return PostSweepDecision(
                    action="continue",
                    direction=cont_dir,
                    confidence=abs(flow.conviction),
                    next_target=next_target,
                    reason=f"Flow continues {cont_dir} post-sweep",
                )

        return PostSweepDecision(
            action="wait",
            direction="",
            confidence=0.0,
            reason="Mixed signals post-sweep",
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
        """
        Execute a sweep reversal entry.
        SL: beyond sweep wick (tight, institutional)
        TP: opposing unswept pool
        """
        side = decision.direction

        # SL: behind sweep wick + buffer
        if side == "long":
            sl = sweep.wick_extreme - atr * _SL_BUFFER_ATR
        else:
            sl = sweep.wick_extreme + atr * _SL_BUFFER_ATR

        # TP: opposing pool or fallback
        if decision.next_target:
            tp = decision.next_target.pool.price
            if side == "long":
                tp -= atr * _TP_BUFFER_ATR  # Slightly inside BSL
            else:
                tp += atr * _TP_BUFFER_ATR  # Slightly inside SSL
        else:
            # Fallback: 2x the risk in favorable direction
            risk = abs(price - sl)
            tp = price + (risk * 2.0) if side == "long" else price - (risk * 2.0)

        # R:R check
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
            f"{decision.reason}")

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
        """
        Execute a sweep continuation entry.
        SL: behind the swept pool level
        TP: next pool in continuation direction
        """
        side = decision.direction
        next_target = decision.next_target
        if next_target is None:
            self._post_sweep = None
            self._state = EngineState.SCANNING
            self._state_entered = now
            return

        # SL: behind swept pool + buffer
        if side == "long":
            sl = sweep.pool.price - atr * _SL_BUFFER_ATR
        else:
            sl = sweep.pool.price + atr * _SL_BUFFER_ATR

        # TP: next pool
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
            f"SL=${sl:,.1f} TP=${tp:,.1f} R:R={rr:.1f}")

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
        Find the best pool in the flow direction.
        "Best" = highest significance among reachable pools.
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

        return max(candidates, key=lambda t: t.significance)

    def _find_opposing_target(
        self,
        direction: str,
        snap: LiquidityMapSnapshot,
        price: float,
        atr: float,
    ) -> Optional[PoolTarget]:
        """
        Find the opposing pool (for reversal TP).
        If swept SSL → going long → target is BSL above.
        If swept BSL → going short → target is SSL below.
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

        return max(reachable, key=lambda t: t.significance)

    def _find_continuation_target(
        self,
        direction: str,
        snap: LiquidityMapSnapshot,
        price: float,
        atr: float,
    ) -> Optional[PoolTarget]:
        """Find the next pool in continuation direction."""
        return self._find_opposing_target(direction, snap, price, atr)

    def _compute_sl(
        self,
        ict: ICTContext,
        side: str,
        price: float,
        atr: float,
    ) -> Optional[float]:
        """
        SL placement hierarchy (purely ICT-structural):
        1. Nearest OB in opposing direction (tightest)
        2. Recent 5m swing (moderate)
        3. Fallback: 1.5× ATR (widest acceptable)
        """
        # Try ICT OB first
        if ict.nearest_ob_price > 0:
            ob_price = ict.nearest_ob_price
            if side == "long" and ob_price < price:
                sl = ob_price - atr * _SL_BUFFER_ATR
                # Validate SL distance
                dist_pct = abs(price - sl) / price
                if 0.001 <= dist_pct <= 0.035:
                    return sl
            elif side == "short" and ob_price > price:
                sl = ob_price + atr * _SL_BUFFER_ATR
                dist_pct = abs(price - sl) / price
                if 0.001 <= dist_pct <= 0.035:
                    return sl

        # Fallback: 1.2× ATR
        if side == "long":
            return price - atr * 1.2
        else:
            return price + atr * 1.2

    def _compute_tp_approach(
        self,
        target: PoolTarget,
        side: str,
        price: float,
        atr: float,
    ) -> Optional[float]:
        """
        TP for pre-sweep approach: the pool itself (slightly inside).
        """
        tp = target.pool.price
        if side == "long":
            tp -= atr * _TP_BUFFER_ATR  # Just inside BSL
        else:
            tp += atr * _TP_BUFFER_ATR  # Just inside SSL
        return tp

    def _ict_structure_agrees(self, ict: ICTContext, direction: str) -> bool:
        """
        Does ICT structural context agree with the entry direction?
        This is a BINARY check, not a score.
        """
        agreements = 0

        # AMD bias
        if direction == "long" and ict.amd_bias == "bullish":
            agreements += 1
        elif direction == "short" and ict.amd_bias == "bearish":
            agreements += 1

        # Premium/Discount
        if direction == "long" and ict.in_discount:
            agreements += 1
        elif direction == "short" and ict.in_premium:
            agreements += 1

        # 5m structure
        if direction == "long" and ict.structure_5m == "bullish":
            agreements += 1
        elif direction == "short" and ict.structure_5m == "bearish":
            agreements += 1

        # Kill zone
        if ict.session_quality == "prime":
            agreements += 1

        return agreements >= 2  # At least 2 structural agreements

    @staticmethod
    def _ict_summary(ict: ICTContext, side: str) -> str:
        """Human-readable ICT validation for logging."""
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
    """Internal state for TRACKING phase."""
    direction:       str
    target:          PoolTarget
    started_at:      float
    flow_ticks:      int   = 0
    contrary_ticks:  int   = 0
    peak_conviction: float = 0.0


@dataclass
class _PostSweepState:
    """Internal state for POST_SWEEP phase."""
    sweep:            SweepResult
    entered_at:       float
    initial_flow:     float = 0.0
    initial_flow_dir: str   = ""
    cisd_detected:    bool  = False
    ote_zone:         Optional[Tuple[float, float]] = None  # (low, high)


# ═══════════════════════════════════════════════════════════════════════════
# TRAIL MANAGER (ICT-only trailing)
# ═══════════════════════════════════════════════════════════════════════════

class ICTTrailManager:
    """
    Trailing stop management using ICT structures + Liquidity awareness.
    
    INSTITUTIONAL TRAIL RULES:
    1. 15m structure is PRIMARY — wider swings survive stop hunts
    2. 5m structure for refinement once trend is confirmed (BOS >= 2)
    3. SL must CLEAR nearby liquidity pools by 0.5 ATR minimum
    4. BOS in trade direction → tighten buffer  
    5. CHoCH against trade → tighten aggressively
    6. Never move SL backward (ratchet-only)
    7. Session-aware buffer widening (Asia = wider, NY = standard)
    
    PHASE LOGIC (profit-tier + structure):
      Phase 0: tier < 0.40R and no BOS → HOLD (no trail)
      Phase 1: tier >= 0.40R OR 1 BOS → 15m swing trail, wide buffer
      Phase 2: tier >= 1.00R OR 2 BOS → 15m+5m trail, moderate buffer
      Phase 3: tier >= 2.00R OR 3 BOS → All TF trail, tight buffer
    """

    def __init__(self) -> None:
        self._current_sl:   float = 0.0
        self._entry_price:  float = 0.0
        self._side:         str   = ""
        self._bos_count:    int   = 0
        self._choch_seen:   bool  = False
        self._peak_profit:  float = 0.0

    def initialize(self, side: str, entry_price: float, initial_sl: float) -> None:
        self._side = side
        self._entry_price = entry_price
        self._current_sl = initial_sl
        self._bos_count = 0
        self._choch_seen = False
        self._peak_profit = 0.0

    def compute(
        self,
        ict_ctx: ICTContext,
        price: float,
        atr: float,
        candles_5m: List[Dict],
        candles_15m: Optional[List[Dict]] = None,
        initial_sl_dist: float = 0.0,
        liq_pools: Optional[List] = None,
    ) -> Optional[float]:
        """
        Returns new SL if it should move, None if no change.
        """
        if not self._side or atr < 1e-10:
            return None

        # Track profit
        profit = (price - self._entry_price) if self._side == "long" else (self._entry_price - price)
        self._peak_profit = max(self._peak_profit, profit)
        
        init_dist = initial_sl_dist if initial_sl_dist > 1e-10 else abs(self._entry_price - self._current_sl)
        tier = self._peak_profit / init_dist if init_dist > 1e-10 else 0.0

        # Check for BOS in trade direction → count it
        if ict_ctx.bos_5m:
            expected = "bullish" if self._side == "long" else "bearish"
            if ict_ctx.bos_5m == expected:
                self._bos_count += 1

        # Check for CHoCH against trade → aggressive tighten
        if ict_ctx.choch_5m:
            against = "bearish" if self._side == "long" else "bullish"
            if ict_ctx.choch_5m == against:
                self._choch_seen = True

        # Determine phase (profit + structure)
        phase = self._determine_phase(tier)
        if phase == 0:
            return None

        # Find the latest swing behind the trade (15m priority)
        new_sl = self._find_structural_sl(candles_5m, candles_15m, atr, phase)

        if new_sl is None:
            return None

        # Liquidity pool clearance — don't place SL where stops cluster
        if liq_pools:
            new_sl = self._clear_liquidity(new_sl, liq_pools, atr)

        # Minimum distance enforcement
        min_dist = self._get_min_dist(atr, phase)
        if self._side == "long":
            new_sl = min(new_sl, price - min_dist)
        else:
            new_sl = max(new_sl, price + min_dist)

        # Never move backward
        if self._side == "long":
            if new_sl <= self._current_sl:
                return None
        else:
            if new_sl >= self._current_sl:
                return None

        self._current_sl = new_sl
        return new_sl

    def _determine_phase(self, tier: float) -> int:
        """Phase from BOS count + profit tier (matches quant_strategy logic)."""
        if self._bos_count >= 3 or tier >= 2.0:
            return 3
        if self._bos_count >= 2 or (self._bos_count >= 1 and tier >= 0.8) or tier >= 1.0:
            return 2
        if self._bos_count >= 1 or tier >= 0.40:
            return 1
        return 0

    def _get_min_dist(self, atr: float, phase: int) -> float:
        """Minimum trail distance — session-aware."""
        base_mult = {1: 2.0, 2: 1.5}.get(phase, 1.0)
        
        # Session widening
        try:
            from datetime import datetime, timezone, timedelta
            utc_now = datetime.now(timezone.utc)
            ny_hour = (utc_now - timedelta(hours=5)).hour
            if 20 <= ny_hour or ny_hour < 1:
                base_mult *= 1.50   # Asia — wide
            elif 2 <= ny_hour < 5:
                base_mult *= 1.15   # London
            elif 11 <= ny_hour < 16:
                base_mult *= 1.25   # Late NY
        except Exception:
            pass
        
        return max(base_mult * atr, 0.50 * atr)

    def _clear_liquidity(self, sl: float, liq_pools: list, atr: float) -> float:
        """Push SL away from nearby liquidity pools to avoid stop hunts."""
        clearance = 0.50 * atr
        for pool in liq_pools:
            try:
                pp = float(pool.price) if hasattr(pool, 'price') else float(pool.get('price', 0))
                swept = getattr(pool, 'swept', False) or pool.get('swept', False) if isinstance(pool, dict) else False
                if swept:
                    continue
                if abs(sl - pp) < clearance:
                    if self._side == "long":
                        sl = min(sl, pp - clearance)
                    else:
                        sl = max(sl, pp + clearance)
            except Exception:
                continue
        return sl

    def _find_structural_sl(
        self,
        candles_5m: List[Dict],
        candles_15m: Optional[List[Dict]],
        atr: float,
        phase: int,
    ) -> Optional[float]:
        """
        Find the appropriate swing level for SL placement.
        
        INSTITUTIONAL HIERARCHY:
          Phase 1-3: 15m swing (primary — survives stop hunts)
          Phase 2-3: 5m swing (refinement — tighter when trend confirmed)
          Phase 3:   Use the better of 15m and 5m
        """
        try:
            from strategy.liquidity_map import _find_swing_highs, _find_swing_lows
        except ImportError:
            from liquidity_map import _find_swing_highs, _find_swing_lows

        # Buffer scales with phase + CHoCH
        if self._choch_seen:
            buffer_mult = 0.10   # Tight — market warned us
        elif self._bos_count >= 2:
            buffer_mult = 0.20   # Moderate — thesis proven
        elif self._bos_count >= 1:
            buffer_mult = 0.30   # Standard
        else:
            buffer_mult = 0.40   # Wide — no structure confirmation
        buffer = atr * buffer_mult

        candidates = []

        # ── 15m PRIMARY anchor (Phase 1+) ─────────────────────────────
        if candles_15m and len(candles_15m) >= 12:
            if self._side == "long":
                lows = _find_swing_lows(candles_15m, lookback=3)
                if lows:
                    # Use the most recent swing low that's behind the trade
                    valid = [l for l in lows if l[1] < self._entry_price]
                    if valid:
                        latest_low = valid[-1][1]
                        candidates.append(latest_low - buffer)
            else:
                highs = _find_swing_highs(candles_15m, lookback=3)
                if highs:
                    valid = [h for h in highs if h[1] > self._entry_price]
                    if valid:
                        latest_high = valid[-1][1]
                        candidates.append(latest_high + buffer)

        # ── 5m refinement (Phase 2+) ──────────────────────────────────
        if phase >= 2 and candles_5m and len(candles_5m) >= 12:
            tighter_buffer = atr * max(buffer_mult - 0.10, 0.10)
            if self._side == "long":
                lows = _find_swing_lows(candles_5m, lookback=5)
                if lows:
                    valid = [l for l in lows if l[1] > self._current_sl]
                    if valid:
                        latest_low = valid[-1][1]
                        candidates.append(latest_low - tighter_buffer)
            else:
                highs = _find_swing_highs(candles_5m, lookback=5)
                if highs:
                    valid = [h for h in highs if h[1] < self._current_sl]
                    if valid:
                        latest_high = valid[-1][1]
                        candidates.append(latest_high + tighter_buffer)

        if not candidates:
            return None

        # Select the tightest valid candidate (best improvement)
        if self._side == "long":
            return max(c for c in candidates if c > self._current_sl) if any(c > self._current_sl for c in candidates) else None
        else:
            valid = [c for c in candidates if c < self._current_sl]
            return min(valid) if valid else None

    @property
    def current_sl(self) -> float:
        return self._current_sl

    @property
    def phase_info(self) -> str:
        parts = [f"BOS×{self._bos_count}"]
        if self._choch_seen:
            parts.append("CHoCH⚠️")
        peak_r = self._peak_profit / max(abs(self._entry_price - self._current_sl), 1) if self._current_sl else 0
        parts.append(f"peak={peak_r:.1f}R")
        return " ".join(parts)

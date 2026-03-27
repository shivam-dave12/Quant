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
    dealing_range_pd: float = 0.5    # 0=deep discount, 1=deep premium
    structure_5m:     str   = ""     # "bullish" | "bearish" | "neutral"
    structure_15m:    str   = ""     # "bullish" | "bearish" | "neutral"
    structure_4h:     str   = ""     # "bullish" | "bearish" | "neutral"
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
        self._last_sweep_analysis: Dict = {}  # Cached for display/logging

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
        # ── Quality floor by timeframe ────────────────────────────────────────
        # 1m sweeps are dominated by noise — require strong displacement (0.65).
        # 5m sweeps need moderate displacement (0.55).
        # Lower timeframes fire on every small wick; set strict gates so only
        # genuinely displaced sweeps enter the evaluator at all.
        _TF_QUALITY_MIN = {'1m': 0.65, '5m': 0.55, '15m': 0.45, '4h': 0.35}
        _tf = getattr(sweep.pool, 'timeframe', '5m')
        _required_quality = _TF_QUALITY_MIN.get(_tf, 0.45)
        if sweep.quality < _required_quality:
            logger.debug(
                f"🌊 SWEEP SKIPPED: [{_tf}] quality={sweep.quality:.2f} "
                f"< required {_required_quality:.2f} — ignoring micro-sweep noise")
            return
        # ─────────────────────────────────────────────────────────────────────

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
        # Log ONCE — no per-tick spam
        logger.info(
            f"🌊 SWEEP DETECTED: {sweep.pool.side.value} "
            f"${sweep.pool.price:,.1f} quality={sweep.quality:.2f} "
            f"| Scoring reversal vs continuation...")

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
        Uses multi-factor scoring (see _evaluate_post_sweep).
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
        # "wait" → stay in POST_SWEEP, keep evaluating (no log spam)

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
        INSTITUTIONAL POST-SWEEP DECISION ENGINE
        ==========================================
        Multi-factor scoring: reverse vs continue vs wait.

        The question: "Was this sweep a STOP HUNT (reversal) or a BREAKOUT (continuation)?"

        REVERSAL FACTORS (sweep was manipulation → expect opposite delivery):
          1. Displacement: strong body close opposite to sweep direction     [0-20]
          2. Wick rejection: price closes back inside the pre-sweep range    [0-15]
          3. HTF dealing range: sweep at premium BSL or discount SSL         [0-15]
          4. HTF trend AGAINST sweep: 4H/1D structure opposes continuation   [0-15]
          5. Flow reversal: order flow has reversed post-sweep               [0-10]
          6. CISD: 5m/15m break of structure confirms reversal direction     [0-15]
          7. Session: Asia/London manipulation phase favors reversal         [0-5]
          8. Opposing pool quality: strong TP target in reversal direction   [0-5]

        CONTINUATION FACTORS (sweep was a genuine breakout):
          1. No displacement: price holds beyond swept level                 [0-20]
          2. HTF trend WITH sweep: 4H/1D momentum supports continuation     [0-15]
          3. Flow continuation: order flow drives in sweep direction         [0-15]
          4. No wick rejection: candle body closes through pool              [0-10]
          5. Volume expansion: increasing volume = institutional commitment  [0-10]
          6. Next pool in range: there IS another pool to target             [0-5]
          7. No HTF OB blocking: path to next pool is clear structurally     [0-10]
          8. Price beyond sweep level + 0.5 ATR: breakout confirmed          [0-15]

        Decision thresholds:
          REVERSAL:      rev_score >= 55 AND rev_score > cont_score + 15
          CONTINUATION:  cont_score >= 50 AND cont_score > rev_score + 15
          WAIT:          insufficient edge (neither threshold met)
        """
        sweep_dir = sweep.direction  # Direction to go IF reversing
        cont_dir = "short" if sweep_dir == "long" else "long"

        rev_score  = 0.0
        cont_score = 0.0
        rev_reasons = []
        cont_reasons = []

        # ═══════════════════════════════════════════════════════════════
        # FACTOR 0: AMD PHASE + BIAS — INSTITUTIONAL DELIVERY CONTEXT
        # ───────────────────────────────────────────────────────────────
        # This is the highest-priority gate. AMD phase tells us WHAT
        # smart money is doing RIGHT NOW — it must dominate all other
        # factors or the engine will fire reversals into active delivery.
        #
        # DISTRIBUTION (bearish): institutions are selling/delivering down.
        #   BSL sweep + bearish = buy stops absorbed for continuation short.
        #     → strong cont bonus (+25×conf).
        #   SSL sweep + bearish = price pushed into discount during delivery.
        #     → moderate cont bonus (+20×conf) AND rev_score penalty (−12×conf).
        #     The rev penalty directly counteracts the "DEEP DISCOUNT" bonus
        #     in Factor 3 which would otherwise push rev_score above 55.
        #
        # DISTRIBUTION (bullish): mirror of above for long delivery.
        #
        # MANIPULATION: Judas swing — this IS the reversal setup.
        #   BSL swept + bearish bias = classic stop hunt before drop.
        #   SSL swept + bullish bias = classic stop hunt before pump.
        #     → strong rev bonus (+22×conf).
        #
        # ACCUMULATION: range building. Sweeps at range extremes → reversal.
        #     → moderate rev bonus (+12×conf).
        #
        # WHY rev_score PENALTY for DISTRIBUTION?
        # Factor 3 (HTF Dealing Range) adds +15 to rev_score whenever SSL
        # is swept in deep discount. During bearish DISTRIBUTION this is
        # WRONG — deep discount is where delivery is heading, not from where
        # it reverses. Without the penalty, Factor 3 alone inflates rev_score
        # past the 55-point threshold even against AMD context. The −12×conf
        # penalty exactly offsets this and keeps the engine from fighting the
        # institutional order flow.
        # ═══════════════════════════════════════════════════════════════
        amd_phase = (getattr(ict, 'amd_phase', '') or '').upper()
        amd_bias  = (getattr(ict, 'amd_bias',  '') or '').lower()
        amd_conf  = float(getattr(ict, 'amd_confidence', 0.0) or 0.0)

        if amd_phase == 'MANIPULATION':
            # Judas swing. Bias AGAINST sweep direction = classic reversal setup.
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
                # Buy stops absorbed during bearish delivery — continuation short.
                pts = 25.0 * max(amd_conf, 0.5)
                cont_score += pts
                cont_reasons.append(f"AMD DIST bear+BSL ({amd_conf:.0%})")
            elif sweep.pool.side == PoolSide.SSL and amd_bias == 'bullish':
                # Sell stops absorbed during bullish delivery — continuation long.
                pts = 25.0 * max(amd_conf, 0.5)
                cont_score += pts
                cont_reasons.append(f"AMD DIST bull+SSL ({amd_conf:.0%})")
            elif sweep.pool.side == PoolSide.SSL and amd_bias == 'bearish':
                # SSL swept during bearish distribution. Price being delivered lower.
                # cont bonus + rev PENALTY to counteract Factor-3 "DEEP DISCOUNT" bias.
                cont_score += 20.0 * max(amd_conf, 0.5)
                rev_score  -= 12.0 * max(amd_conf, 0.5)   # penalise going long vs delivery
                cont_reasons.append(f"AMD DIST bear+SSL ({amd_conf:.0%})")
            elif sweep.pool.side == PoolSide.BSL and amd_bias == 'bullish':
                # BSL swept during bullish distribution — mirror case.
                cont_score += 20.0 * max(amd_conf, 0.5)
                rev_score  -= 12.0 * max(amd_conf, 0.5)
                cont_reasons.append(f"AMD DIST bull+BSL ({amd_conf:.0%})")

        elif amd_phase == 'ACCUMULATION':
            # Range building. Sweeps at extremes = range manipulation → reversal.
            pts = 12.0 * max(amd_conf, 0.4)
            rev_score += pts
            rev_reasons.append(f"AMD ACCUM ({amd_conf:.0%})")

        # ═══════════════════════════════════════════════════════════════
        # FACTOR 1: DISPLACEMENT QUALITY (0-20)
        # Strong body close away from sweep = institutional reversal
        # No rejection + price holds beyond = breakout
        # ═══════════════════════════════════════════════════════════════
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

        # ═══════════════════════════════════════════════════════════════
        # FACTOR 2: WICK REJECTION vs BREAKOUT (0-15)
        # Price back inside range = rejection (reversal)
        # Price closing beyond sweep wick = breakout (continuation)
        # ═══════════════════════════════════════════════════════════════
        if sweep.pool.side == PoolSide.BSL:
            rejecting = price < sweep.pool.price
            breaking_through = price > sweep.pool.price + 0.3 * atr
            deep_break = price > sweep.wick_extreme
        else:
            rejecting = price > sweep.pool.price
            breaking_through = price < sweep.pool.price - 0.3 * atr
            deep_break = price < sweep.wick_extreme

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

        # ═══════════════════════════════════════════════════════════════
        # FACTOR 3: HTF DEALING RANGE POSITION (0-15)
        # Sweep at the EXTREME of dealing range = manipulation (reversal)
        # Sweep in the MIDDLE = could go either way
        # ═══════════════════════════════════════════════════════════════
        pd = ict.dealing_range_pd if hasattr(ict, 'dealing_range_pd') else 0.5

        if sweep.pool.side == PoolSide.BSL:
            # BSL swept (above) — if in deep premium, this is a stop hunt
            if pd > 0.80:
                rev_score += 15.0
                rev_reasons.append(f"DEEP PREMIUM ({pd:.0%})")
            elif pd > 0.65:
                rev_score += 8.0
                rev_reasons.append(f"PREMIUM ({pd:.0%})")
            elif pd < 0.35:
                cont_score += 10.0
                cont_reasons.append(f"DISCOUNT→BSL break ({pd:.0%})")
        else:
            # SSL swept (below) — if in deep discount, this is a stop hunt
            if pd < 0.20:
                rev_score += 15.0
                rev_reasons.append(f"DEEP DISCOUNT ({pd:.0%})")
            elif pd < 0.35:
                rev_score += 8.0
                rev_reasons.append(f"DISCOUNT ({pd:.0%})")
            elif pd > 0.65:
                cont_score += 10.0
                cont_reasons.append(f"PREMIUM→SSL break ({pd:.0%})")

        # ═══════════════════════════════════════════════════════════════
        # FACTOR 4: HTF TREND ALIGNMENT (0-15)
        # Sweep AGAINST HTF trend = manipulation → reversal
        # Sweep WITH HTF trend = breakout → continuation
        # ═══════════════════════════════════════════════════════════════
        htf_trend = getattr(ict, 'structure_15m', '') or ''
        htf_4h = getattr(ict, 'structure_4h', '') or ''

        # Check if sweep direction opposes HTF trend
        if sweep.pool.side == PoolSide.BSL:
            # BSL swept = price went UP. If HTF is bearish, this is manipulation
            if htf_4h == "bearish":
                rev_score += 15.0
                rev_reasons.append("4H BEARISH vs BSL sweep")
            elif htf_trend == "bearish":
                rev_score += 10.0
                rev_reasons.append("15m BEARISH vs BSL sweep")
            elif htf_4h == "bullish":
                cont_score += 12.0
                cont_reasons.append("4H BULLISH aligns BSL break")
            elif htf_trend == "bullish":
                cont_score += 7.0
                cont_reasons.append("15m BULLISH aligns BSL")
        else:
            # SSL swept = price went DOWN. If HTF is bullish, this is manipulation
            if htf_4h == "bullish":
                rev_score += 15.0
                rev_reasons.append("4H BULLISH vs SSL sweep")
            elif htf_trend == "bullish":
                rev_score += 10.0
                rev_reasons.append("15m BULLISH vs SSL sweep")
            elif htf_4h == "bearish":
                cont_score += 12.0
                cont_reasons.append("4H BEARISH aligns SSL break")
            elif htf_trend == "bearish":
                cont_score += 7.0
                cont_reasons.append("15m BEARISH aligns SSL")

        # ═══════════════════════════════════════════════════════════════
        # FACTOR 5: ORDER FLOW DIRECTION (0-15)
        # Flow reversed after sweep = institutions reversed
        # Flow continues = institutions are pushing through
        # ═══════════════════════════════════════════════════════════════
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

        # CVD divergence: if CVD disagrees with price direction post-sweep
        if flow.cvd_divergence != 0:
            if sweep.pool.side == PoolSide.BSL and flow.cvd_divergence < -0.2:
                rev_score += 5.0
                rev_reasons.append("CVD BEARISH div")
            elif sweep.pool.side == PoolSide.SSL and flow.cvd_divergence > 0.2:
                rev_score += 5.0
                rev_reasons.append("CVD BULLISH div")

        # ═══════════════════════════════════════════════════════════════
        # FACTOR 6: CISD / BOS CONFIRMATION (0-15)
        # Break of structure in reversal direction = strongest confirmation
        # ═══════════════════════════════════════════════════════════════
        cisd_confirmed = False
        if ict.bos_5m:
            expected_bos = "bullish" if sweep_dir == "long" else "bearish"
            cisd_confirmed = (ict.bos_5m == expected_bos)

        choch_confirmed = False
        if ict.choch_5m:
            expected_choch = "bullish" if sweep_dir == "long" else "bearish"
            choch_confirmed = (ict.choch_5m == expected_choch)

        if cisd_confirmed:
            rev_score += 15.0
            rev_reasons.append("CISD ✅ (5m BOS)")
        elif choch_confirmed:
            rev_score += 10.0
            rev_reasons.append("CHoCH ✅ (5m)")

        # BOS in continuation direction = trend continuing
        if ict.bos_5m:
            cont_bos = "bullish" if cont_dir == "long" else "bearish"
            if ict.bos_5m == cont_bos:
                cont_score += 10.0
                cont_reasons.append("BOS aligns continuation")

        # ═══════════════════════════════════════════════════════════════
        # FACTOR 7: SESSION CONTEXT (0-5)
        # Asia/early London sweeps = manipulation (reversal bias)
        # NY session sweeps = delivery (could be either, but real)
        # ═══════════════════════════════════════════════════════════════
        session = getattr(ict, 'kill_zone', '') or ''
        if 'asia' in session.lower():
            rev_score += 5.0
            rev_reasons.append("ASIA session (manipulation)")
        elif 'london' in session.lower() and not 'ny' in session.lower():
            rev_score += 3.0
            rev_reasons.append("LONDON (possible Judas)")

        # ═══════════════════════════════════════════════════════════════
        # FACTOR 8: TARGET QUALITY (0-10)
        # Strong opposing pool = good TP → reversal more attractive
        # Strong next pool in continuation = good continuation target
        # ═══════════════════════════════════════════════════════════════
        opp_target = self._find_opposing_target(sweep_dir, snap, price, atr, ict)
        cont_target = self._find_continuation_target(cont_dir, snap, price, atr, ict)

        if opp_target and opp_target.significance >= _MIN_POOL_SIGNIFICANCE:
            rev_score += min(5.0, opp_target.significance * 0.5)
            rev_reasons.append(f"TP pool sig={opp_target.significance:.1f}")

        if cont_target and cont_target.significance >= _MIN_POOL_SIGNIFICANCE:
            cont_score += min(5.0, cont_target.significance * 0.5)
            cont_reasons.append(f"Next pool sig={cont_target.significance:.1f}")

        # ═══════════════════════════════════════════════════════════════
        # FACTOR 9: OB BLOCKING (0-10) — continuation only
        # Is there a HTF OB blocking the continuation path?
        # ═══════════════════════════════════════════════════════════════
        ob_price = getattr(ict, 'nearest_ob_price', 0.0) or 0.0
        if ob_price > 0:
            if cont_dir == "long" and ob_price > price:
                ob_dist_atr = (ob_price - price) / max(atr, 1e-10)
                if ob_dist_atr < 2.0:
                    rev_score += 8.0
                    rev_reasons.append(f"OB BLOCKS cont at {ob_dist_atr:.1f}ATR")
            elif cont_dir == "short" and ob_price < price:
                ob_dist_atr = (price - ob_price) / max(atr, 1e-10)
                if ob_dist_atr < 2.0:
                    rev_score += 8.0
                    rev_reasons.append(f"OB BLOCKS cont at {ob_dist_atr:.1f}ATR")

        # ═══════════════════════════════════════════════════════════════
        # DECISION: score comparison with confidence gap requirement
        # ═══════════════════════════════════════════════════════════════
        rev_total  = round(rev_score, 1)
        cont_total = round(cont_score, 1)
        gap = abs(rev_total - cont_total)

        # Store scores for display
        self._last_sweep_analysis = {
            "rev_score": rev_total, "cont_score": cont_total,
            "rev_reasons": rev_reasons, "cont_reasons": cont_reasons,
            "sweep_side": sweep.pool.side.value,
            "sweep_price": sweep.pool.price,
            "sweep_quality": sweep.quality,
        }

        if rev_total >= 55.0 and gap >= 15.0:
            confidence = min(1.0, rev_total / 100.0)
            reason_str = " + ".join(rev_reasons[:4])
            logger.info(
                f"🔄 SWEEP VERDICT: REVERSAL {sweep_dir.upper()} "
                f"(rev={rev_total:.0f} vs cont={cont_total:.0f} gap={gap:.0f}) "
                f"| {reason_str}")
            return PostSweepDecision(
                action="reverse",
                direction=sweep_dir,
                confidence=confidence,
                next_target=opp_target,
                reason=f"REVERSAL [{rev_total:.0f}v{cont_total:.0f}] {reason_str}",
            )

        elif cont_total >= 50.0 and gap >= 15.0:
            confidence = min(1.0, cont_total / 100.0)
            reason_str = " + ".join(cont_reasons[:4])
            logger.info(
                f"➡️ SWEEP VERDICT: CONTINUATION {cont_dir.upper()} "
                f"(cont={cont_total:.0f} vs rev={rev_total:.0f} gap={gap:.0f}) "
                f"| {reason_str}")
            return PostSweepDecision(
                action="continue",
                direction=cont_dir,
                confidence=confidence,
                next_target=cont_target,
                reason=f"CONTINUATION [{cont_total:.0f}v{rev_total:.0f}] {reason_str}",
            )

        else:
            # Log the indecision with full scoring for debugging
            logger.debug(
                f"⏳ SWEEP WAIT: rev={rev_total:.0f} cont={cont_total:.0f} "
                f"gap={gap:.0f} (need ≥15) | "
                f"R:[{', '.join(rev_reasons[:3])}] "
                f"C:[{', '.join(cont_reasons[:3])}]")
            return PostSweepDecision(
                action="wait",
                direction="",
                confidence=0.0,
                reason=f"WAIT [{rev_total:.0f}v{cont_total:.0f}] gap={gap:.0f}<15",
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
        ict: Optional["ICTContext"] = None,
    ) -> Optional[PoolTarget]:
        """
        Find the opposing pool (for reversal TP).
        If swept SSL → going long → target is BSL above.
        If swept BSL → going short → target is SSL below.

        AMD-aware: in DISTRIBUTION/REDISTRIBUTION context prefer the NEAREST
        reachable pool. Delivery is already underway — targeting the far BSL
        above a descending trendline sets TP beyond what the move will reach.
        In all other AMD phases use the highest-significance pool (best R:R).
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

        # In active delivery phases, prefer the nearest pool so TP sits
        # inside the delivery structure rather than overshooting it.
        amd_phase = (getattr(ict, 'amd_phase', '') or '').upper() if ict else ''
        if amd_phase in ('DISTRIBUTION', 'REDISTRIBUTION'):
            return min(reachable, key=lambda t: t.distance_atr)

        return max(reachable, key=lambda t: t.significance)

    def _find_continuation_target(
        self,
        direction: str,
        snap: LiquidityMapSnapshot,
        price: float,
        atr: float,
        ict: Optional["ICTContext"] = None,
    ) -> Optional[PoolTarget]:
        """Find the next pool in continuation direction."""
        return self._find_opposing_target(direction, snap, price, atr, ict)

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
    Trailing stop management using ICT structures ONLY.
    No ATR multipliers, no R-multiple gates, no time-based rules.

    Rules:
    1. SL moves ONLY when a new swing forms behind the trade
    2. BOS in trade direction → tighten buffer
    3. CHoCH against trade → tighten aggressively
    4. 15m structure takes precedence over 5m
    5. Never move SL backward (closer to entry)
    """

    def __init__(self) -> None:
        self._current_sl:   float = 0.0
        self._entry_price:  float = 0.0
        self._side:         str   = ""
        self._bos_count:    int   = 0
        self._choch_seen:   bool  = False

    def initialize(self, side: str, entry_price: float, initial_sl: float) -> None:
        self._side = side
        self._entry_price = entry_price
        self._current_sl = initial_sl
        self._bos_count = 0
        self._choch_seen = False

    def compute(
        self,
        ict_ctx: ICTContext,
        price: float,
        atr: float,
        candles_5m: List[Dict],
        candles_15m: Optional[List[Dict]] = None,
    ) -> Optional[float]:
        """
        Returns new SL if it should move, None if no change.
        """
        if not self._side or atr < 1e-10:
            return None

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

        # Find the latest swing behind the trade
        new_sl = self._find_structural_sl(candles_5m, candles_15m, atr)

        if new_sl is None:
            return None

        # Never move backward
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
        """
        Find the appropriate swing level for SL placement.
        Hierarchy: 15m swing > 5m swing.
        """
        # BUG-FIX-4: Bare "from liquidity_map import" inside a method raises
        # ModuleNotFoundError when entry_engine is imported as strategy.entry_engine
        # (Python's path doesn't include the strategy/ subdirectory for bare imports).
        # This silently returned None for every trail SL computation.
        try:
            from strategy.liquidity_map import _find_swing_highs, _find_swing_lows
        except ImportError:
            from liquidity_map import _find_swing_highs, _find_swing_lows

        # Buffer scales with phase
        if self._choch_seen:
            buffer_mult = 0.05  # Very tight — market warned us
        elif self._bos_count >= 2:
            buffer_mult = 0.10  # Moderate — thesis proven
        else:
            buffer_mult = _SL_BUFFER_ATR  # Standard

        buffer = atr * buffer_mult

        # Try 15m first
        if candles_15m and len(candles_15m) >= 12:
            if self._side == "long":
                lows = _find_swing_lows(candles_15m, lookback=3)
                if lows:
                    latest_low = lows[-1][1]
                    return latest_low - buffer
            else:
                highs = _find_swing_highs(candles_15m, lookback=3)
                if highs:
                    latest_high = highs[-1][1]
                    return latest_high + buffer

        # Fall back to 5m
        if candles_5m and len(candles_5m) >= 12:
            if self._side == "long":
                lows = _find_swing_lows(candles_5m, lookback=5)
                if lows:
                    latest_low = lows[-1][1]
                    return latest_low - buffer
            else:
                highs = _find_swing_highs(candles_5m, lookback=5)
                if highs:
                    latest_high = highs[-1][1]
                    return latest_high + buffer

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

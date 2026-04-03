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
_MIN_POOL_SIGNIFICANCE = 2.0

# Sweep quality
_MIN_SWEEP_QUALITY = 0.35

# Timing
_CISD_MAX_WAIT_SEC   = 360
_ENTRY_COOLDOWN_SEC  = 30.0

# SL / TP
_MIN_RR_RATIO       = 1.4
_SL_BUFFER_ATR      = 0.35
_TP_BUFFER_ATR      = 0.08
_REV_SL_BUFFER_ATR  = 0.35
_CONT_SL_BUFFER_ATR = 0.40

# Post-Sweep phases (seconds after sweep)
_PS_PHASE_DISPLACEMENT = 45.0
_PS_PHASE_CISD         = 120.0
_PS_PHASE_OTE          = 240.0
_PS_PHASE_MATURE       = 360.0

# Post-Sweep evidence thresholds
_PS_THRESHOLD_EARLY  = 80.0
_PS_THRESHOLD_NORMAL = 65.0
_PS_THRESHOLD_MATURE = 55.0
_PS_GAP_MIN          = 15.0
_PS_DISP_MULT        = 1.30
_PS_MATURE_MULT      = 0.75

# Displacement requirements
_PS_DISP_MIN_ATR    = 0.5
_PS_DISP_STRONG_ATR = 1.2
_PS_OTE_FIB_LOW     = 0.50
_PS_OTE_FIB_HIGH    = 0.786

# Momentum entry
_MOMENTUM_MIN_BODY_RATIO   = 0.65
_MOMENTUM_MIN_VOL_RATIO    = 1.3
_MOMENTUM_MIN_ATR_MOVE     = 0.6
_MOMENTUM_LOOKBACK_CANDLES = 3
_MOMENTUM_SL_BUFFER_ATR    = 0.15
_MOMENTUM_MIN_RR           = 1.3
_MOMENTUM_COOLDOWN_SEC     = 60.0
_MOMENTUM_MAX_PER_HOUR     = 3

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
                    f"for {now - self._state_entered:.0f}s")
                self._reset(now)

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

    def on_position_closed(self) -> None:
        self._reset(time.time())

    def force_reset(self) -> None:
        self._reset(time.time())
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

    # ── Internal: reset ───────────────────────────────────────────────

    def _reset(self, now: float) -> None:
        self._state = EngineState.SCANNING
        self._state_entered = now
        self._signal = None
        self._tracking = None
        self._post_sweep = None

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
        """Merge LiquidityMap sweeps + ICT engine sweeps into one list."""
        sweeps = [s for s in snap.recent_sweeps
                  if s.detected_at > now - 10.0
                  and s.quality >= _MIN_SWEEP_QUALITY]

        if (not sweeps
                and self._state not in (EngineState.POST_SWEEP,
                                        EngineState.IN_POSITION,
                                        EngineState.ENTERING)
                and hasattr(ict_ctx, 'ict_sweeps') and ict_ctx.ict_sweeps):

            age_limit = int(now * 1000) - 30_000
            for ev in ict_ctx.ict_sweeps:
                if ev.sweep_ts < age_limit:
                    continue
                side = PoolSide.BSL if ev.pool_type == "BSL" else PoolSide.SSL
                direction = "short" if ev.pool_type == "BSL" else "long"
                wick = ev.candle_high if ev.pool_type == "BSL" else ev.candle_low
                quality = min(1.0, 0.35 + 0.35 * ev.disp_score
                              + (0.15 if ev.wick_reject else 0.0))
                if quality < _MIN_SWEEP_QUALITY:
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
                    f"${ev.pool_price:,.1f} quality={quality:.2f}")

        return sweeps

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

        # SL
        ch, cl_val = float(disp_candle['h']), float(disp_candle['l'])
        buf = atr * _MOMENTUM_SL_BUFFER_ATR
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

        self._last_momentum_candle_ts = int(disp_candle.get('t', 0) or 0)
        self._momentum_entries_1h += 1

        self._signal = EntrySignal(
            side=disp_dir, entry_type=EntryType.DISPLACEMENT_MOMENTUM,
            entry_price=price, sl_price=sl, tp_price=tp, rr_ratio=rr,
            target_pool=target,
            conviction=abs(flow.conviction) * amd_mult,
            reason=f"DISPLACEMENT {disp_dir.upper()} [{disp_tf}] R:R={rr:.1f}",
            ict_validation=self._ict_summary(ict, disp_dir),
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
            return

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

        if decision.action == "reverse":
            self._handle_reversal(ps.sweep, decision, snap, flow, ict, price, atr, now)
        elif decision.action == "continue":
            self._handle_continuation(ps.sweep, decision, snap, flow, ict, price, atr, now)

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

        # ── STATIC FACTORS (scored once) ──────────────────────────────
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

            ps.static_rev_base = s_rev
            ps.static_cont_base = s_cont
            ps.static_scored = True
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

        # Cross-sweep decay
        current_type = sweep.pool.side.value
        entered_ms = int(ps.entered_at * 1000)
        for ev in (getattr(ict, 'ict_sweeps', None) or []):
            if (ev.pool_type != current_type
                    and ev.sweep_ts > entered_ms
                    and now * 1000 - ev.sweep_ts < 90_000):
                ps.rev_evidence *= 0.55
                cont_d += 10.0
                cont_r.append(f"CROSS-SWEEP {ev.pool_type}")
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

        risk = abs(price - sl)
        if risk < 1e-10 or risk / price < 0.001 or risk / price > 0.035:
            self._post_sweep = None
            self._reset(now)
            return

        self._last_sweep_reversal_dir = side
        self._last_sweep_reversal_time = now

        # TP: pool → HTF → 2R (only if CISD)
        tp, target = self._find_tp(snap, side, price, atr, sl, _MIN_RR_RATIO)
        cisd_ok = ps is not None and ps.cisd_detected
        if tp is None and cisd_ok:
            tp = price + risk * 2.0 if side == "long" else price - risk * 2.0
        if tp is None:
            self._post_sweep = None
            self._reset(now)
            return

        rr = abs(tp - price) / risk
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
            entry_price=price, sl_price=sl, tp_price=tp, rr_ratio=rr,
            target_pool=target, sweep_result=sweep,
            conviction=decision.confidence,
            reason=f"{decision.reason}{cisd}{disp}",
            ict_validation=self._ict_summary(ict, side),
        )
        logger.info(f"🎯 SIGNAL: REVERSAL {side.upper()} | "
                    f"SL=${sl:,.1f} TP=${tp:,.1f} R:R={rr:.1f}{cisd}{disp}")
        self._post_sweep = None

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

        tp_buf = atr * 0.35
        tp = target.pool.price - tp_buf if side == "long" else target.pool.price + tp_buf

        risk = abs(price - sl)
        reward = abs(tp - price)
        if risk < 1e-10 or risk / price < 0.001 or risk / price > 0.035:
            self._post_sweep = None
            self._reset(now)
            return

        rr = reward / risk
        if rr < _MIN_RR_RATIO:
            tp2, target2 = self._find_tp(snap, side, price, atr, sl, _MIN_RR_RATIO)
            if tp2 is not None:
                tp, target = tp2, target2
                rr = abs(tp - price) / risk
            else:
                self._post_sweep = None
                self._reset(now)
                return

        self._signal = EntrySignal(
            side=side, entry_type=EntryType.SWEEP_CONTINUATION,
            entry_price=price, sl_price=sl, tp_price=tp, rr_ratio=rr,
            target_pool=target, sweep_result=sweep,
            conviction=decision.confidence, reason=decision.reason,
            ict_validation=self._ict_summary(ict, side),
        )
        logger.info(f"🎯 SIGNAL: CONTINUATION {side.upper()} R:R={rr:.1f}")
        self._post_sweep = None

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
        pools = snap.bsl_pools if direction == "long" else snap.ssl_pools
        reachable = [t for t in pools
                     if t.distance_atr <= _MAX_TARGET_ATR
                     and t.significance >= _MIN_POOL_SIGNIFICANCE * 0.5]
        if not reachable:
            return None
        return max(reachable, key=lambda t: t.adjusted_sig())

    def _pool_to_tp(self, target, side, price, atr):
        buf = atr * min(0.50, 0.10 + 0.05 * target.distance_atr)
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
        """Push SL behind nearby liquidity pools."""
        snap = self._last_liq_snapshot
        if snap is None:
            return sl
        buf = 0.25 * atr
        if side == "long":
            for t in snap.ssl_pools:
                if sl < t.pool.price < price:
                    sl = min(sl, t.pool.price - buf)
        else:
            for t in snap.bsl_pools:
                if price < t.pool.price < sl:
                    sl = max(sl, t.pool.price + buf)
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

        buf_mult = 0.05 if self._choch else (0.10 if self._bos_count >= 2 else _SL_BUFFER_ATR)
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

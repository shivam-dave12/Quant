"""
liquidity_trail.py — Institutional Liquidity/Structure SL Trailing Engine v6.0
=====================================================================
ADVANCED REWRITE — Drop-in replacement.  Sole trailing engine for the
entire system (no fallbacks, no chandelier, no dynamic-structure class).

DESIGN PRINCIPLES
-----------------
Every advance passes six gates.  A single Fib-level touch never moves the SL.

  1.  BAR-CLOSE GATE
      SL advances are evaluated ONLY when the last candle of the anchor TF
      closes.  Intrabar wicks never trigger a move.  This eliminates the
      retail failure mode where a 1-second wick into a pool stops out a
      winning trade.

  2.  CLOSE-CONFIRMATION COUNTER
      Each candidate Fib level must be broken by N consecutive closes in
      the trade direction, not a single wick penetration.  N scales with
      phase: Phase 2 = 2 closes, Phase 3 = 1 close + displacement body.

  3.  SWING-INVALIDATION
      The grid of Fib levels is built from a (swing_low → swing_high) pair.
      If the swing_low is violated on a close (LONG) or the swing_high is
      violated (SHORT), the grid is INVALIDATED: SL holds its current
      position and we rebuild the grid from the next confirmed swing.
      A broken-origin grid generates phantom levels — they cannot be trusted.

  4.  MOMENTUM GATE
      Trail advances require ONE of:
        (a) Displacement candle (body ≥ 0.58 ATR aligned with trade), OR
        (b) CVD trend magnitude ≥ 0.12 aligned with trade, OR
        (c) BOS on 5m/15m aligned with trade within last 10 minutes
      Without momentum, even a clean Fib break is just a pullback.

  5.  LIQUIDITY-AWARE BUFFER
      If a liquidity pool sits BETWEEN the proposed SL and current price,
      the buffer is WIDENED so the SL is placed behind the pool, not in
      front of it.  Stop-hunts sweep into the pool first; our SL must
      survive that sweep.

  6.  HTF TREND-ALIGNMENT (Phase 3 deep trails only)
      Phase 3 aggressive trails (3.5R+) require the 1H HTF trend to be
      aligned with the trade direction.  Against-trend deep trails get
      downgraded to Phase 2 buffers — deep profit on counter-trend trades
      is more likely to mean-revert and should not be tightly trailed.

PHASE ARCHITECTURE
------------------
  Phase 0  (< 1.0R):   HANDS OFF — structural SL from entry is optimal
  Phase 1  (1.0 – 2.0R): BE LOCK — move to entry + exact fees + slippage
  Phase 2  (2.0 – 3.5R): FIB STRUCTURAL — 1H/15m swings, wide buffers
  Phase 3  (3.5R+):      FIB AGGRESSIVE — all TFs, tight buffers, HTF-gated

COUNTER-BOS SOVEREIGN OVERRIDE
------------------------------
A 5m BOS AGAINST the trade direction that breaks below entry (LONG) or
above entry (SHORT) forces an immediate BE lock regardless of R-multiple.
The structure has reversed; the original thesis is invalidated.  This
folds the old Counter-BOS block from quant_strategy.py into the trail.

OTE-ZONE PULLBACK FREEZE
------------------------
Price inside 0.382–0.618 retrace of the primary swing = FREEZE.  Smart
money re-accumulates here on routine pullbacks.  SL holds its position
until price exits the zone.  Eliminates the #1 Fibonacci trailing error.

LIQUIDITY CONFLUENCE
--------------------
Fib level within 0.50 ATR of an unswept pool = 1.6× quality boost AND
buffer expansion so the SL is placed behind both structures.  This is
the strongest possible anchor: institutional stops AND Fibonacci
mathematics both point to the same price.

INTERFACE (drop-in compatible with the v4.0 LiquidityTrailEngine)
-----------------------------------------------------------------
    engine = LiquidityTrailEngine()
    result = engine.compute(
        pos_side="long", price=P, entry_price=E, current_sl=S, atr=A,
        initial_sl_dist=I, peak_profit=PP,
        liq_snapshot=snap, ict_engine=ict, now=T, pos=pos,
        candles_1m=[], candles_5m=[], candles_15m=[], candles_1h=[],
        hold_reason=[])
    if result.new_sl is not None:
        order_manager.replace_stop_loss(...)

All v4.0 call sites work unchanged.  `result.anchor.sig` still exists
(alias for quality).  New fields on PoolAnchor are optional.

The engine is stateful per-position.  Call engine.reset() on every new
position entry — this is done in quant_strategy._finalise_exit().
"""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# PHASE THRESHOLDS (R-multiple boundaries)
# ═══════════════════════════════════════════════════════════════════════════

PHASE_0_MAX_R = 1.0    # below this → hands off (structural SL trusted)
PHASE_1_MAX_R = 1.0    # BE checkpoint; do not trap 1R-2R before structure
PHASE_2_MAX_R = 3.5    # Fib structural zone (1H/15m swings)
# Phase 3 ≥ 3.5R (Fib aggressive — all TFs, HTF-gated)


# ═══════════════════════════════════════════════════════════════════════════
# FIBONACCI LEVEL CATALOGUE
# ═══════════════════════════════════════════════════════════════════════════
# Ratio, is_institutional (Golden trio), base quality
_FIB_LEVELS: List[Tuple[float, bool, float]] = [
    (0.236, False, 1.0),   # Shallow (Phase 3 only) — low quality
    (0.382, True,  3.0),   # Golden ratio start — PRIMARY institutional
    (0.500, True,  3.5),   # Midpoint — highest single-level conviction
    (0.618, True,  4.0),   # OTE zone end — strongest retracement anchor
    (0.786, False, 1.5),   # Near-extreme — use cautiously
]

_INSTITUTIONAL_RATIOS: frozenset = frozenset({0.382, 0.500, 0.618})

# Quality multipliers
_Q_BONUS_INSTITUTIONAL   = 1.30   # Golden ratio vs non-golden
_Q_BONUS_INTERACTION     = 1.20   # level has been tested recently by price
_Q_BONUS_CONFLUENCE      = 1.50   # 2+ TF Fib levels merged (cluster)
_Q_BONUS_CLUSTER_EXTRA   = 0.20   # per additional TF in the cluster
_Q_BONUS_POOL_CONFLUENCE = 1.60   # Fib + liquidity pool within 0.5 ATR

# Per-TF configuration: pivot strength + quality weight + earliest phase
_TF_CFG: Dict[str, Dict] = {
    "1h":  {"strength": 2, "weight": 4.0, "min_phase": 2},
    "15m": {"strength": 3, "weight": 3.0, "min_phase": 2},
    "5m":  {"strength": 3, "weight": 2.0, "min_phase": 3},
    "1m":  {"strength": 3, "weight": 1.0, "min_phase": 3},
}

# Phase → allowed TFs
_PHASE_TF: Dict[int, frozenset] = {
    2: frozenset({"1h", "15m"}),
    3: frozenset({"1h", "15m", "5m", "1m"}),
}


# ═══════════════════════════════════════════════════════════════════════════
# BUFFER TABLES — per-ratio ATR-multiplier distance behind the Fib level
# ═══════════════════════════════════════════════════════════════════════════

_FIB_BUF_PHASE2: Dict[float, float] = {
    0.618: 0.80,
    0.500: 0.65,
    0.382: 0.55,
    0.786: 0.90,
    0.236: 0.50,
}

_FIB_BUF_PHASE3: Dict[float, float] = {
    0.618: 0.55,
    0.500: 0.45,
    0.382: 0.35,
    0.236: 0.28,
    0.786: 0.65,
}

_DEFAULT_BUF_P2 = 0.80
_DEFAULT_BUF_P3 = 0.55

# Minimum distance from price to SL (breathing room)
MIN_BREATHING_ATR_PHASE2 = 1.00
MIN_BREATHING_ATR_PHASE3 = 0.65

# Hard cap on SL distance (prevents runaway wide SL)
MAX_SL_DIST_ATR = 8.0

# SL must preserve at least this fraction of initial_sl_dist from entry
MIN_SL_PRESERVE_FRACTION = 0.50

# Minimum meaningful improvement to trigger a REST call
MIN_IMPROVEMENT_ATR = 0.20

# Swing acceptance: minimum swing range as ATR multiple
MIN_SWING_ATR = 1.5

# Fib levels within this ATR merge into a cluster anchor
FIB_CLUSTER_ATR = 0.40

# Pool confluence: Fib level within this ATR of a pool
POOL_CONFLUENCE_ATR = 0.50

# How far back to scan for pivots (bars)
PIVOT_SCAN_DEPTH = 80

# OTE pullback freeze zone
FREEZE_LOWER = 0.382
FREEZE_UPPER = 0.618

# Anti-oscillation lock (once we choose an anchor, hold it this long)
ANCHOR_LOCK_SEC = 90.0
# New anchor must be this much better than the locked one to override
ANCHOR_OVERRIDE_QUAL_MULT = 1.50

# ═══════════════════════════════════════════════════════════════════════════
# CLOSE-CONFIRMATION COUNTER
# ═══════════════════════════════════════════════════════════════════════════
# A Fib level must be broken by N closed bars (in the anchor TF) before the
# SL moves to the next-deeper Fib level.  Intrabar wicks never count.
PHASE2_CLOSES_REQUIRED = 2      # conservative — 2 closes for structural trail
PHASE3_CLOSES_REQUIRED = 1      # aggressive — 1 close plus displacement body

# ═══════════════════════════════════════════════════════════════════════════
# MOMENTUM GATE
# ═══════════════════════════════════════════════════════════════════════════
# Advances require momentum from AT LEAST ONE of:
#   (a) displacement candle (|body| >= DISP_MIN_BODY_ATR × ATR, aligned)
#   (b) CVD trend magnitude >= CVD_MIN_TREND aligned with trade
#   (c) BOS on 5m/15m aligned with trade within BOS_MAX_AGE_MS
DISP_MIN_BODY_ATR = 0.58
CVD_MIN_TREND     = 0.12
# Bug #37 fix: BOS_MAX_AGE_MS reduced 10_000_000 (10 min) → 2_000_000 (2 min).
# A BOS is structural confirmation that price has broken past a swing extreme.
# Using a 10-minute window means a BOS from 9:57 ago still qualifies as
# "momentum confirmation" for a trail advance — by which point the market
# has likely moved on entirely.  2 minutes = ~2.5 × the 5m bar period,
# which is tight enough to require a recent structural event while allowing
# for ICT engine throttle lag (5-second update interval).
# Configurable via TRAIL_BOS_MAX_AGE_MS so operators can widen if needed.
#
# BUG-FIX D: 2-minute BOS age was calibrated for ENTRY confirmation (tight),
# but it is far too tight for TRAILING.  On 15m TFs, a BOS forms roughly once
# per bar (~15 min).  Requiring it to be < 2 minutes old means the momentum
# gate fires "NONE" for ≈87% of the time even in a trending market, blocking
# ALL Phase 2/3 trail advances.
#
# Two separate constants now:
#   BOS_MAX_AGE_MS          — used by entry confirmation (kept at 2 min)
#   TRAIL_BOS_MAX_AGE_MS    — used by the trail momentum gate (default 10 min)
# Operators can override TRAIL_BOS_MAX_AGE_MS in config.py independently.
try:
    import config as _trail_cfg  # noqa: F811
    BOS_MAX_AGE_MS = int(getattr(_trail_cfg, 'TRAIL_BOS_MAX_AGE_MS', 2_000_000))
except Exception:
    BOS_MAX_AGE_MS = 2_000_000

try:
    import config as _trail_cfg  # noqa: F811
    TRAIL_MOMENTUM_BOS_MAX_AGE_MS = int(
        getattr(_trail_cfg, 'TRAIL_MOMENTUM_BOS_MAX_AGE_MS', 600_000))  # 10 min default
except Exception:
    TRAIL_MOMENTUM_BOS_MAX_AGE_MS = 600_000

# Liquidity-aware buffer expansion factor when a pool sits between price and SL
POOL_BETWEEN_BUFFER_MULT = 1.35

# Counter-BOS sovereign override: 5m BOS against trade that breaks entry
COUNTER_BOS_MAX_AGE_MS = 3_000_000   # 3 minutes

# Session buffer multipliers
SESSION_BUFFER_MULT: Dict[str, float] = {
    "LONDON": 1.00,
    "NY":     1.15,
    "ASIA":   1.50,   # wider on quiet session
    "":       1.00,
}
# Bug #18a fix: read from config so operators can enable Asia trailing
# without touching source code.  Default False — Asia trails ARE allowed
# by default (the original True was overly conservative and not configurable).
try:
    import config as _trail_cfg
    ASIA_TRAIL_DISABLED = bool(getattr(_trail_cfg, 'TRAIL_ASIA_DISABLED', False))
except Exception:
    ASIA_TRAIL_DISABLED = False

# Bug #24 fix: read ANCHOR_LOCK_SEC from config so it can be shortened
# during fast-market conditions without a code deploy.
try:
    import config as _trail_cfg  # noqa: F811 — already imported above
    ANCHOR_LOCK_SEC = float(getattr(_trail_cfg, 'TRAIL_ANCHOR_LOCK_SEC', 90.0))
except Exception:
    ANCHOR_LOCK_SEC = 90.0


# ═══════════════════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class PoolAnchor:
    """
    A structural anchor that the SL can be set behind.

    Backward compatible with the v4.0 PoolAnchor: callers reading
    .price / .timeframe / .sig / .is_swept / .quality work unchanged.
    """
    price:         float
    side:          str      # "BSL" | "SSL"
    timeframe:     str
    sig:           float    # alias for quality (v4.0 compat)
    buffer_atr:    float    # applied buffer in ATR units
    is_swept:      bool
    distance_atr:  float
    quality:       float

    # Fibonacci-specific
    fib_ratio:     Optional[float] = None
    swing_low:     Optional[float] = None
    swing_high:    Optional[float] = None
    pool_boost:    bool = False
    is_cluster:    bool = False
    n_cluster_tfs: int  = 1
    # Liquidity-aware buffer expansion was applied
    pool_between_expand: bool = False
    # "fib" | "liquidity_pool" | "order_block" | "breaker_block"
    anchor_kind:   str = "fib"


@dataclass
class LiquidityTrailResult:
    """Output of the trailing engine."""
    new_sl:        Optional[float]
    anchor:        Optional[PoolAnchor]
    reason:        str
    phase:         str
    trail_blocked: bool = False
    block_reason:  str  = ""

    # v5.0: extra display fields for richer Telegram messages
    r_multiple:     float = 0.0
    swing_low:      Optional[float] = None
    swing_high:     Optional[float] = None
    momentum_gate:  str = ""        # "DISP" | "CVD" | "BOS" | "NONE"
    htf_aligned:    Optional[bool] = None


# ═══════════════════════════════════════════════════════════════════════════
# ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class LiquidityTrailEngine:
    """
    Institutional liquidity/structure SL Trailing Engine v6.0 - sole trail.

    Stateful per-position.  Call .reset() when a position closes.
    """

    def __init__(self) -> None:
        self._locked_anchor:      Optional[PoolAnchor] = None
        self._anchor_lock_until:  float = 0.0
        self._last_phase:         str   = "HANDS_OFF"

        # OTE pullback freeze state
        self._in_pullback_freeze: bool  = False
        self._freeze_sl_snapshot: Optional[float] = None

        # Close-confirmation counter: per (TF, ratio, swing_id) tally of
        # consecutive closed bars broken past the level in trade direction.
        self._close_counters: Dict[Tuple[str, float, float], int] = {}

        # Swing-invalidation tracker: which (swing_low, swing_high) pair is
        # currently "live" per TF.  If the origin is broken on a close, we
        # mark the grid invalid and skip until a new swing forms.
        self._live_swing:   Dict[str, Tuple[float, float]] = {}
        self._invalidated:  Dict[str, bool] = {}

        # Last-seen close timestamps per TF (used by bar-close gate)
        self._last_bar_close_ts: Dict[str, int] = {}

        # Counter-BOS sovereign override already fired?
        self._counter_bos_triggered: bool = False

    # ─────────────────────────────────────────────────────────────────────
    # PUBLIC API
    # ─────────────────────────────────────────────────────────────────────

    def reset(self) -> None:
        """Reset all per-position state.  Call on every new position open."""
        self._locked_anchor      = None
        self._anchor_lock_until  = 0.0
        self._last_phase         = "HANDS_OFF"
        self._in_pullback_freeze = False
        self._freeze_sl_snapshot = None
        self._close_counters.clear()
        self._live_swing.clear()
        self._invalidated.clear()
        self._last_bar_close_ts.clear()
        self._counter_bos_triggered = False
        logger.debug("LiquidityTrailEngine v5.0: state reset for new position")

    def compute(
        self,
        pos_side:        str,
        price:           float,
        entry_price:     float,
        current_sl:      float,
        atr:             float,
        initial_sl_dist: float = 0.0,
        peak_profit:     float = 0.0,
        liq_snapshot              = None,
        ict_engine                = None,
        now:             float  = 0.0,
        hold_reason:     Optional[List[str]] = None,
        pos                       = None,
        fee_engine                = None,
        # Candle data
        candles_1m:      Optional[List[dict]] = None,
        candles_5m:      Optional[List[dict]] = None,
        candles_15m:     Optional[List[dict]] = None,
        candles_1h:      Optional[List[dict]] = None,
        # Momentum inputs (optional but strongly recommended)
        cvd_trend:       float = 0.0,
    ) -> LiquidityTrailResult:
        """
        Compute the next SL using liquidity, ICT structure, and delivery gates.

        Returns LiquidityTrailResult with new_sl=None to HOLD current SL,
        or new_sl=<float> to replace with the returned value.
        """
        now_ = now if now > 1e6 else time.time()
        if atr < 1e-10:
            return self._hold("atr_zero", hold_reason)

        # ── Session gate ──────────────────────────────────────────────
        session = self._detect_session(ict_engine)
        if ASIA_TRAIL_DISABLED and session == "ASIA":
            return self._blocked("ASIA_SESSION_DISABLED", hold_reason)
        sess_mult = SESSION_BUFFER_MULT.get(session, 1.0)

        # ── R-multiple ────────────────────────────────────────────────
        # Bug #17 fix: init_dist must represent the ORIGINAL risk distance
        # recorded at entry, not the current SL distance.  The old formula
        # included abs(entry_price - current_sl) as a candidate, which grew
        # whenever the SL was widened after entry (e.g. pushed behind a pool
        # for safety).  That inflated init_dist, doubling or more the R-multiple
        # denominator, delaying the Phase 1 BE lock and all subsequent phases.
        #
        # Correct semantics:
        #   initial_sl_dist > 0  → use it directly (entry engine recorded it)
        #   initial_sl_dist == 0 → no record; fall back to atr*0.5 as a
        #                          conservative floor (NOT current_sl distance)
        init_dist = (
            initial_sl_dist if initial_sl_dist > 1e-10
            else max(abs(entry_price - current_sl), atr * 0.5)
            # abs(entry_price - current_sl) is acceptable HERE only as a
            # last resort when initial_sl_dist was never recorded (e.g.
            # manually adopted positions), because in that case current_sl
            # IS the original structural SL — it has not been trailed yet.
            # The guard: initial_sl_dist > 1e-10 takes priority, so this
            # fallback only fires when the entry engine did not set the field.
        )
        # BUG-FIX F: if both initial_sl_dist AND the fallback evaluate to 0
        # (e.g. current_sl == entry_price AND ATR is near-zero), r_multiple
        # collapses to 0.0 permanently and the engine stays in PHASE_0_HANDS_OFF
        # for the entire trade with no log noise at INFO level.  Emit a one-shot
        # WARNING so the operator can diagnose the root cause (SL order at entry
        # price, or ATR computation failure).
        if init_dist < 1e-10:
            logger.warning(
                "Trail: init_dist is effectively zero "
                "(initial_sl_dist=%.4f current_sl=%.4f entry=%.4f atr=%.4f) — "
                "r_multiple will be 0.0 and trailing permanently blocked at Phase 0. "
                "Check SL placement and ATR computation.",
                initial_sl_dist, current_sl, entry_price, atr)
            return self._hold("INIT_DIST_ZERO: r_multiple undefined", hold_reason)
        profit    = (price - entry_price) if pos_side == "long" else (entry_price - price)
        r_peak    = max(profit, peak_profit)
        r_multiple = r_peak / init_dist if init_dist > 1e-10 else 0.0
        candles_by_tf: Dict[str, List[dict]] = {
            "1m":  candles_1m  or [],
            "5m":  candles_5m  or [],
            "15m": candles_15m or [],
            "1h":  candles_1h  or [],
        }

        # Bug #19 fix: counter-BOS re-arm after Phase 1 completes.
        # The original design fired the override exactly once per position.
        # This protected the 0–1R range but left Phase 2+ positions (SL
        # already above entry) unprotected when a second, deeper structural
        # break occurred.  Re-arm by clearing the flag whenever the R-multiple
        # crosses into Phase 2 — the Phase 2 FIB trail is now the primary
        # protection, but a counter-BOS in that zone is still a valid reason
        # to move to the nearest Fib level immediately rather than waiting for
        # the Fib confirmation counter.
        if self._counter_bos_triggered and r_multiple >= PHASE_1_MAX_R:
            self._counter_bos_triggered = False
            logger.debug(
                "Trail[COUNTER_BOS]: re-armed for Phase 2+ — "
                f"R={r_multiple:.2f}R ≥ {PHASE_1_MAX_R}R threshold"
            )

        # ── Counter-BOS sovereign override ────────────────────────────
        # Fires exactly once per position.  Moves SL to BE immediately.
        if (not self._counter_bos_triggered
                and self._counter_bos_breakout(ict_engine, pos_side, entry_price, price)):
            self._counter_bos_triggered = True
            be_price = self._be_price(pos_side, entry_price, atr, pos, fee_engine)
            is_impr = ((pos_side == "long"  and be_price > current_sl) or
                       (pos_side == "short" and be_price < current_sl))
            if is_impr and profit > 0:
                logger.warning(
                    f"Trail[COUNTER_BOS_OVERRIDE]: 5m BOS against trade broke "
                    f"entry — forcing BE ${be_price:,.1f}")
                return LiquidityTrailResult(
                    new_sl=self._round_be_price(pos_side, be_price), anchor=None,
                    reason=f"[COUNTER_BOS_OVERRIDE] SL → BE ${be_price:,.1f}",
                    phase="COUNTER_BOS", r_multiple=r_multiple)

        # ══════════════════════════════════════════════════════════════
        # PHASE 0 — HANDS OFF
        # ══════════════════════════════════════════════════════════════
        if r_multiple < PHASE_0_MAX_R:
            self._last_phase = "HANDS_OFF"
            return self._hold(
                f"PHASE_0_HANDS_OFF: R={r_multiple:.2f}<{PHASE_0_MAX_R}R — "
                f"initial structural SL is optimal",
                hold_reason, r_multiple=r_multiple)

        # ══════════════════════════════════════════════════════════════
        # PHASE 1 — BREAKEVEN LOCK
        # ══════════════════════════════════════════════════════════════
        early_momentum_gate = self._momentum_gate(
            pos_side, atr, candles_by_tf.get("5m", []), cvd_trend,
            ict_engine, now_)
        be_gate_ok, be_gate_reason = self._structural_be_gate(
            pos_side, price, entry_price, atr, r_multiple,
            early_momentum_gate, ict_engine, liq_snapshot, now_)
        if not be_gate_ok and hold_reason is not None:
            hold_reason.append(f"BE_WAIT_STRUCT: {be_gate_reason}")

        # ── Build candle-by-TF map for Phase 2/3 ──────────────────────
        candles_by_tf: Dict[str, List[dict]] = {
            "1m":  candles_1m  or [],
            "5m":  candles_5m  or [],
            "15m": candles_15m or [],
            "1h":  candles_1h  or [],
        }

        # ══════════════════════════════════════════════════════════════
        # PHASE 2 / 3 - STRUCTURE TRAIL
        # ══════════════════════════════════════════════════════════════
        if r_multiple < PHASE_2_MAX_R:
            self._last_phase = "STRUCTURAL"
            _trail_result = self._fibonacci_trail(
                pos_side, price, entry_price, current_sl, atr, init_dist,
                r_multiple, liq_snapshot, ict_engine, now_, sess_mult,
                candles_by_tf=candles_by_tf, pos=pos, fee_engine=fee_engine,
                cvd_trend=cvd_trend,
                phase_num=2, phase_name="STRUCTURAL",
                buf_table=_FIB_BUF_PHASE2,
                min_breathing=MIN_BREATHING_ATR_PHASE2,
                closes_required=PHASE2_CLOSES_REQUIRED,
                hold_reason=hold_reason)
            _floor_result = self._try_profit_floor_lock(
                pos_side, price, entry_price, current_sl, atr, init_dist,
                r_multiple, hold_reason, pos=pos, fee_engine=fee_engine)
            if _trail_result.new_sl is not None or _floor_result.new_sl is not None:
                # Choose the more protective of structural trail and net-profit floor.
                _best_sl = self._better_sl(pos_side, _trail_result.new_sl, _floor_result.new_sl)
                if _floor_result.new_sl is not None and abs(_floor_result.new_sl - _best_sl) < 1e-9:
                    return _floor_result
                return _trail_result
            if not be_gate_ok:
                return _trail_result
            _be_result = self._try_be_lock(
                pos_side, price, entry_price, current_sl, atr,
                r_multiple, hold_reason, pos=pos, fee_engine=fee_engine,
                gate_reason=be_gate_reason)
            return _be_result if _be_result.new_sl is not None else _trail_result

        # Phase 3
        self._last_phase = "AGGRESSIVE"
        _trail_result = self._fibonacci_trail(
            pos_side, price, entry_price, current_sl, atr, init_dist,
            r_multiple, liq_snapshot, ict_engine, now_, sess_mult,
            candles_by_tf=candles_by_tf, pos=pos, fee_engine=fee_engine,
            cvd_trend=cvd_trend,
            phase_num=3, phase_name="AGGRESSIVE",
            buf_table=_FIB_BUF_PHASE3,
            min_breathing=MIN_BREATHING_ATR_PHASE3,
            closes_required=PHASE3_CLOSES_REQUIRED,
            hold_reason=hold_reason)
        _floor_result = self._try_profit_floor_lock(
            pos_side, price, entry_price, current_sl, atr, init_dist,
            r_multiple, hold_reason, pos=pos, fee_engine=fee_engine)
        if _trail_result.new_sl is not None or _floor_result.new_sl is not None:
            _best_sl = self._better_sl(pos_side, _trail_result.new_sl, _floor_result.new_sl)
            if _floor_result.new_sl is not None and abs(_floor_result.new_sl - _best_sl) < 1e-9:
                return _floor_result
            return _trail_result
        if not be_gate_ok:
            return _trail_result
        _be_result = self._try_be_lock(
            pos_side, price, entry_price, current_sl, atr,
            r_multiple, hold_reason, pos=pos, fee_engine=fee_engine,
            gate_reason=be_gate_reason)
        return _be_result if _be_result.new_sl is not None else _trail_result

    # ─────────────────────────────────────────────────────────────────────
    # PHASE 1 — BE LOCK with exact fees
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def _be_price(pos_side: str, entry_price: float, atr: float,
                  pos=None, fee_engine=None) -> float:
        """
        Compute the BE price: entry + round-trip commission + slippage buffer.

        Preference order for commission rate:
          1. pos.entry_fee_paid (exact Delta paid_commission)  — highest fidelity
          2. fee_engine.effective_roundtrip_cost_bps            — live market
          3. config.COMMISSION_RATE                             — static default
        """
        # Exact entry fee → derive exit + round-trip commission
        exact_fee = 0.0
        qty = 0.0
        if pos is not None:
            exact_fee = float(getattr(pos, 'entry_fee_paid', 0.0) or 0.0)
            qty       = float(getattr(pos, 'quantity',       0.0) or 0.0)

        if exact_fee > 1e-6 and qty > 1e-10:
            entry_fee_per_unit = exact_fee / qty
            exit_rate          = LiquidityTrailEngine._exit_stop_fee_rate()
            exit_fee_per_unit  = entry_price * exit_rate
            commission_rt      = entry_fee_per_unit + exit_fee_per_unit
        elif fee_engine is not None:
            try:
                bps = fee_engine.effective_roundtrip_cost_bps(use_maker_entry=True)
                commission_rt = entry_price * (bps / 10_000.0)
            except Exception:
                commission_rt = LiquidityTrailEngine._static_commission_rt(entry_price)
        else:
            commission_rt = LiquidityTrailEngine._static_commission_rt(entry_price)

        slippage_buf = 0.12 * atr
        total_buffer = commission_rt + slippage_buf

        be = (entry_price + total_buffer) if pos_side == "long" \
             else (entry_price - total_buffer)
        return be

    @staticmethod
    def _exit_stop_fee_rate() -> float:
        """Estimated fee rate for the future SL/stop exit leg.

        Entry may be maker, but an SL stop is effectively a taker/risk-exit.
        Using the entry fee rate for the exit leg understated true breakeven
        when maker entry + taker stop were mixed.
        """
        try:
            import config as _cfg
            candidates = [
                getattr(_cfg, 'STOP_EXIT_COMMISSION_RATE', None),
                getattr(_cfg, 'DELTA_TAKER_COMMISSION_RATE', None),
                getattr(_cfg, 'TAKER_COMMISSION_RATE', None),
                getattr(_cfg, 'COMMISSION_RATE', None),
            ]
            rates = []
            for v in candidates:
                if v is None:
                    continue
                try:
                    fv = float(v)
                    if math.isfinite(fv) and fv >= 0:
                        rates.append(fv)
                except Exception:
                    pass
            return max(rates) if rates else 0.00055
        except Exception:
            return 0.00055

    @staticmethod
    def _be_tick_size() -> float:
        try:
            import config as _cfg
            for name in ('TICK_SIZE', 'PRICE_TICK_SIZE'):
                v = getattr(_cfg, name, None)
                if v is not None:
                    fv = float(v)
                    if math.isfinite(fv) and fv > 0:
                        return fv
        except Exception:
            pass
        return 0.5

    @classmethod
    def _round_be_price(cls, pos_side: str, price: float) -> float:
        """Directionally round BE so it is truly net protective.

        LONG stop-BE must round UP; SHORT stop-BE must round DOWN.
        Nearest-tick rounding can turn a mathematical BE into a tiny net loss.
        """
        tick = cls._be_tick_size()
        if tick <= 0:
            return price
        if pos_side == 'long':
            return round(math.ceil(price / tick) * tick, 10)
        return round(math.floor(price / tick) * tick, 10)

    @staticmethod
    def _static_commission_rt(entry_price: float) -> float:
        """Round-trip commission from static config — last-resort only."""
        try:
            import config as _cfg
            entry_rate = float(getattr(_cfg, 'COMMISSION_RATE', 0.00055))
        except Exception:
            entry_rate = 0.00055
        exit_rate = LiquidityTrailEngine._exit_stop_fee_rate()
        return entry_price * (entry_rate + exit_rate)

    @staticmethod
    def _structural_be_gate(
        pos_side: str, price: float, entry_price: float, atr: float,
        r_multiple: float, momentum_gate: str, ict_engine, liq_snapshot, now: float,
    ) -> Tuple[bool, str]:
        """
        Allow a BE/protection lock only after the trade has delivered structure.

        This prevents the old retail behaviour of moving to BE solely because
        price printed a numeric R multiple. The lock now needs directional flow,
        a fresh aligned BOS, or enough delivery beyond a live opposing pool.
        """
        profit = (price - entry_price) if pos_side == "long" else (entry_price - price)
        if profit <= max(0.75 * atr, 1e-9):
            return False, f"delivery={profit/atr:.2f}ATR<0.75ATR"

        if momentum_gate != "NONE":
            return True, f"structural_delivery={momentum_gate}"

        if ict_engine is not None:
            now_ms = int(now * 1000) if now > 1e6 else int(time.time() * 1000)
            for tf in ("5m", "15m"):
                try:
                    st = ict_engine._tf.get(tf)
                    direction = getattr(st, "bos_direction", None)
                    ts = int(getattr(st, "bos_timestamp", 0) or 0)
                    if ts > 0 and now_ms - ts <= TRAIL_MOMENTUM_BOS_MAX_AGE_MS:
                        if pos_side == "long" and direction == "bullish":
                            return True, f"structural_delivery=BOS:{tf}"
                        if pos_side == "short" and direction == "bearish":
                            return True, f"structural_delivery=BOS:{tf}"
                except Exception:
                    pass

        if liq_snapshot is not None and r_multiple >= 1.25:
            try:
                opp_pools = (getattr(liq_snapshot, "bsl_pools", []) if pos_side == "long"
                             else getattr(liq_snapshot, "ssl_pools", []))
                delivered = []
                for pt in (opp_pools or []):
                    pool = getattr(pt, "pool", pt)
                    pp = float(getattr(pool, "price", 0.0) or 0.0)
                    if pp <= 0:
                        continue
                    if pos_side == "long" and entry_price < pp < price:
                        delivered.append(pt)
                    if pos_side == "short" and price < pp < entry_price:
                        delivered.append(pt)
                if delivered:
                    best = max(delivered, key=lambda p: float(
                        p.adjusted_sig() if hasattr(p, "adjusted_sig")
                        else getattr(p, "significance", 0.0) or 0.0
                    ))
                    pool = getattr(best, "pool", best)
                    tf = str(getattr(pool, "timeframe", "?") or "?")
                    sig = float(best.adjusted_sig() if hasattr(best, "adjusted_sig")
                                else getattr(best, "significance", 0.0) or 0.0)
                    return True, f"structural_delivery=pool:{tf}:sig={sig:.1f}"
            except Exception:
                pass

        return False, "awaiting aligned displacement/CVD/BOS/liquidity delivery"

    def _fee_slip_buffer_per_unit(self, entry_price: float, atr: float, pos=None, fee_engine=None) -> float:
        """Return the price distance required to be net-flat after fees/slippage.

        Shared by BE and profit-floor locks. A floor of +0.35R must mean
        +0.35R NET, not +0.35R before commissions.
        """
        be = self._be_price("long", entry_price, atr, pos, fee_engine)
        return max(0.0, abs(be - entry_price))

    def _profit_floor_lock_r(self, r_multiple: float) -> float:
        """Net-R floor unlocked by MFE.

        Institutional behaviour: once a trade has delivered a measurable leg,
        the exchange stop should not remain at fee-BE.  But we do not trail the
        market tick-for-tick.  These are broad floors that preserve breathing
        room while preventing a 30-40% margin move from round-tripping to BE.
        """
        try:
            import config as _cfg
            tiers = getattr(_cfg, 'TRAIL_PROFIT_FLOOR_TIERS', None)
            if tiers:
                best = 0.0
                for mfe_r, lock_r in tiers:
                    if r_multiple >= float(mfe_r):
                        best = max(best, float(lock_r))
                return max(0.0, best)
        except Exception:
            pass
        if r_multiple >= 3.50:
            return 1.25
        if r_multiple >= 2.50:
            return 0.85
        if r_multiple >= 1.75:
            return 0.55
        if r_multiple >= 1.20:
            return 0.32
        return 0.0

    def _try_profit_floor_lock(
        self, pos_side: str, price: float, entry_price: float,
        current_sl: float, atr: float, init_dist: float, r_multiple: float,
        hold_reason: Optional[List[str]], pos=None, fee_engine=None,
    ) -> LiquidityTrailResult:
        """Move the exchange SL to a broad MFE-based net-profit floor.

        This is the missing middle layer between BE and aggressive structural
        trailing. It does NOT chase small pullbacks; it only activates after
        MFE reaches a tier, and only if the resulting stop still leaves enough
        breathing room from current price.
        """
        if init_dist <= 1e-10 or atr <= 1e-10:
            return self._hold("PROFIT_FLOOR_WAIT: invalid init_dist/ATR", hold_reason, r_multiple=r_multiple)
        lock_r = self._profit_floor_lock_r(r_multiple)
        if lock_r <= 0.0:
            return self._hold(
                f"PROFIT_FLOOR_WAIT: MFE={r_multiple:.2f}R below first floor",
                hold_reason, r_multiple=r_multiple)

        fee_slip = self._fee_slip_buffer_per_unit(entry_price, atr, pos, fee_engine)
        gross_dist = fee_slip + lock_r * init_dist
        raw_floor = entry_price + gross_dist if pos_side == "long" else entry_price - gross_dist
        floor_sl = self._round_be_price(pos_side, raw_floor)

        invalid_reason = self._stop_trigger_invalid_reason(pos_side, floor_sl, price)
        if invalid_reason:
            return self._hold(
                f"PROFIT_FLOOR_NOT_EXECUTABLE: {invalid_reason}",
                hold_reason, r_multiple=r_multiple)

        is_impr = ((pos_side == "long" and floor_sl > current_sl) or
                   (pos_side == "short" and floor_sl < current_sl))
        if not is_impr:
            return self._hold(
                f"PROFIT_FLOOR_ALREADY_BETTER: floor=${floor_sl:,.1f} current=${current_sl:,.1f}",
                hold_reason, r_multiple=r_multiple)

        try:
            import config as _cfg
            breath_atr = float(getattr(_cfg, 'TRAIL_PROFIT_FLOOR_MIN_BREATHING_ATR', 0.45))
            min_impr_atr = float(getattr(_cfg, 'TRAIL_PROFIT_FLOOR_MIN_IMPROVEMENT_ATR', 0.10))
        except Exception:
            breath_atr = 0.45
            min_impr_atr = 0.10

        breathing = abs(price - floor_sl) / atr
        if breathing < breath_atr:
            return self._hold(
                f"PROFIT_FLOOR_TOO_TIGHT: breathing={breathing:.2f}ATR<{breath_atr:.2f}ATR "
                f"floor={lock_r:.2f}R", hold_reason, r_multiple=r_multiple)

        improvement = abs(floor_sl - current_sl) / atr
        if improvement < min_impr_atr:
            return self._hold(
                f"PROFIT_FLOOR_MICRO_MOVE: {improvement:.3f}ATR<{min_impr_atr:.2f}ATR",
                hold_reason, r_multiple=r_multiple)

        reason = (f"[PROFIT_FLOOR] MFE={r_multiple:.2f}R → lock≈{lock_r:.2f}R NET "
                  f"SL=${floor_sl:,.1f} (fee/slip≈${fee_slip:.1f}, breathing={breathing:.2f}ATR)")
        logger.info(f"Trail: {reason}")
        return LiquidityTrailResult(new_sl=floor_sl, anchor=None, reason=reason,
                                    phase="PROFIT_FLOOR", r_multiple=r_multiple)

    @staticmethod
    def _better_sl(pos_side: str, a: Optional[float], b: Optional[float]) -> Optional[float]:
        vals = [x for x in (a, b) if x is not None and x > 0]
        if not vals:
            return None
        return max(vals) if pos_side == 'long' else min(vals)

    def _try_be_lock(
        self, pos_side: str, price: float, entry_price: float,
        current_sl: float, atr: float, r_multiple: float,
        hold_reason: Optional[List[str]], pos=None, fee_engine=None,
        gate_reason: str = "",
    ) -> LiquidityTrailResult:
        """Move SL to breakeven + exact fees + slippage buffer."""
        be_price = self._be_price(pos_side, entry_price, atr, pos, fee_engine)
        be_price = self._round_be_price(pos_side, be_price)

        invalid_reason = self._stop_trigger_invalid_reason(pos_side, be_price, price)
        if invalid_reason:
            return self._hold(
                f"BE_NOT_EXECUTABLE: {invalid_reason}",
                hold_reason, r_multiple=r_multiple)

        # Already locked?
        already = ((pos_side == "long"  and current_sl >= be_price) or
                   (pos_side == "short" and current_sl <= be_price))
        if already:
            return self._hold(
                f"BE_ALREADY_LOCKED: SL=${current_sl:,.1f} >= BE=${be_price:,.1f}",
                hold_reason, r_multiple=r_multiple)

        # Must be an improvement
        is_impr = ((pos_side == "long"  and be_price > current_sl) or
                   (pos_side == "short" and be_price < current_sl))
        if not is_impr:
            return self._hold(
                f"BE_NOT_IMPROVEMENT: BE=${be_price:,.1f} vs SL=${current_sl:,.1f}",
                hold_reason, r_multiple=r_multiple)

        # Breathing room
        if abs(price - be_price) / atr < 0.40:
            return self._hold(
                f"BE_TOO_TIGHT: breathing={abs(price-be_price)/atr:.2f}ATR<0.40ATR",
                hold_reason, r_multiple=r_multiple)

        # Minimum improvement
        if abs(be_price - current_sl) / atr < MIN_IMPROVEMENT_ATR:
            return self._hold(
                f"BE_MICRO_MOVE: {abs(be_price-current_sl)/atr:.3f}ATR<{MIN_IMPROVEMENT_ATR}ATR",
                hold_reason, r_multiple=r_multiple)

        commission_rt = self._be_price(pos_side, entry_price, atr, pos, fee_engine)
        # Re-derive fee-only component for logging
        fee_component = abs(commission_rt - entry_price) - 0.12 * atr
        gate_tag = f" | {gate_reason}" if gate_reason else ""
        reason = (
            f"[BE_LOCK] R={r_multiple:.2f}R → BE=${be_price:,.1f} "
            f"(fees≈${max(0.0, fee_component):.2f} slip=${0.12*atr:.1f})"
        )
        if gate_tag:
            reason = f"{reason}{gate_tag}"
        logger.info(f"Trail: {reason}")
        return LiquidityTrailResult(
            new_sl=be_price, anchor=None, reason=reason,
            phase="BE_LOCK", r_multiple=r_multiple)

    # ─────────────────────────────────────────────────────────────────────
    # PHASE 2 / 3 - STRUCTURE TRAIL CORE
    # ─────────────────────────────────────────────────────────────────────

    def _fibonacci_trail(
        self, pos_side: str, price: float, entry_price: float,
        current_sl: float, atr: float, init_dist: float,
        r_multiple: float, liq_snapshot, ict_engine, now: float,
        sess_mult: float, candles_by_tf: Dict[str, List[dict]],
        pos, fee_engine, cvd_trend: float,
        phase_num: int, phase_name: str,
        buf_table: Dict[float, float], min_breathing: float,
        closes_required: int,
        hold_reason: Optional[List[str]],
    ) -> LiquidityTrailResult:
        """
        Advanced liquidity/structure trail logic.

        Order of operations:
          1. HTF alignment check (Phase 3 only — downgrade to P2 buffers if not)
          2. Swing-invalidation check per TF
          3. Build Fib anchors from valid swings
          4. Cluster nearby levels into multi-TF confluence anchors
          5. OTE pullback freeze check
          6. Close-confirmation counter update + gate
          7. Momentum gate (displacement / CVD / BOS)
          8. Select best anchor (quality-ranked)
          9. Anti-oscillation anchor lock
         10. Compute SL with buffer + session + liquidity-aware expansion
         11. Validate guards
        """
        allowed_tfs = _PHASE_TF.get(phase_num, frozenset())

        # ── Step 1: HTF alignment (Phase 3 only) ───────────────────────
        htf_aligned = None
        effective_buf_table = buf_table
        effective_min_breathing = min_breathing
        if phase_num == 3:
            htf_aligned = self._htf_aligned(ict_engine, pos_side)
            if htf_aligned is False:
                # Against HTF trend — downgrade to Phase 2 (wider) buffers
                effective_buf_table = _FIB_BUF_PHASE2
                effective_min_breathing = MIN_BREATHING_ATR_PHASE2
                logger.info(
                    f"Trail[AGGRESSIVE]: HTF NOT aligned with {pos_side} "
                    f"— downgrading to Phase 2 buffers")

        # ── Step 2: Swing-invalidation check, per-TF grid build ────────
        all_anchors: List[PoolAnchor] = []
        swings_by_tf: Dict[str, Tuple[float, float]] = {}

        for tf, cfg in _TF_CFG.items():
            if tf not in allowed_tfs:
                continue
            candles = candles_by_tf.get(tf, [])
            if len(candles) < cfg["strength"] * 2 + 3:
                continue

            # Only re-evaluate when the most recent bar has actually closed.
            # Intrabar evaluation is harmless (we still respect close-count
            # gates below), but limiting to bar-close avoids wasted work.
            last_close_ts = self._get_last_closed_bar_ts(candles)
            self._last_bar_close_ts[tf] = last_close_ts

            swing = self._detect_swings(candles, cfg["strength"], atr)
            if swing is None:
                continue
            swing_low, swing_high = swing

            # Swing-invalidation: did price close past the origin?
            if self._swing_invalidated(tf, swing, candles, pos_side):
                self._invalidated[tf] = True
                # Force rebuild on next iteration by clearing the live swing
                self._live_swing.pop(tf, None)
                continue
            else:
                self._invalidated[tf] = False
                # Record the live swing
                self._live_swing[tf] = swing

            swings_by_tf[tf] = swing
            tf_anchors = self._build_fib_anchors(
                pos_side, price, atr, swing_low, swing_high,
                tf, cfg["weight"], effective_buf_table, liq_snapshot)
            all_anchors.extend(tf_anchors)

        structural_anchors = self._build_structural_anchors(
            pos_side, price, atr, liq_snapshot, ict_engine, now,
            primary_swing=None)

        if not swings_by_tf and not structural_anchors:
            return self._hold(
                f"{phase_name}: no valid swings on allowed TFs "
                f"({'1H/15m' if phase_num == 2 else 'all'}) — all swing origins broken or candles insufficient",
                hold_reason, r_multiple=r_multiple)

        if not all_anchors and not structural_anchors:
            return self._hold(
                f"{phase_name}: no structural anchors behind price from valid swings",
                hold_reason, r_multiple=r_multiple)

        # ── Step 4: Cluster for multi-TF confluence ────────────────────
        clustered = self._cluster_levels(all_anchors, atr)
        if structural_anchors:
            clustered = structural_anchors + clustered

        # ── Step 5: OTE pullback freeze ────────────────────────────────
        primary_swing = self._get_primary_swing(swings_by_tf)
        if primary_swing is not None:
            freeze = self._check_pullback_freeze(
                pos_side, price, primary_swing[0], primary_swing[1],
                current_sl, now, hold_reason, r_multiple)
            if freeze is not None:
                return freeze

        if self._in_pullback_freeze:
            self._in_pullback_freeze = False
            self._freeze_sl_snapshot = None

        # ── Step 6: Momentum gate ──────────────────────────────────────
        momentum_gate = self._momentum_gate(
            pos_side, atr, candles_by_tf.get("5m", []), cvd_trend,
            ict_engine, now)
        if momentum_gate == "NONE":
            return self._hold(
                f"{phase_name}: MOMENTUM_GATE blocked — no displacement, "
                f"CVD={cvd_trend:+.2f}<±{CVD_MIN_TREND}, no recent aligned BOS",
                hold_reason, r_multiple=r_multiple,
                swing_low=primary_swing[0] if primary_swing else None,
                swing_high=primary_swing[1] if primary_swing else None,
                htf_aligned=htf_aligned)

        # ── Step 7: Select best anchor and apply close-confirmation ────
        best = self._select_best_fib_confirmed(
            clustered, pos_side, price, candles_by_tf, closes_required,
            swings_by_tf)
        if best is None:
            return self._hold(
                f"{phase_name}: no structural anchor with close-confirmation "
                f"(need {closes_required} closed bar(s) past level)",
                hold_reason, r_multiple=r_multiple,
                swing_low=primary_swing[0] if primary_swing else None,
                swing_high=primary_swing[1] if primary_swing else None,
                momentum_gate=momentum_gate, htf_aligned=htf_aligned)

        # ── Step 8: Anti-oscillation anchor lock ───────────────────────
        if self._locked_anchor is not None and now < self._anchor_lock_until:
            if best.quality <= self._locked_anchor.quality * ANCHOR_OVERRIDE_QUAL_MULT:
                locked_behind = (
                    (pos_side == "long"  and self._locked_anchor.price < price) or
                    (pos_side == "short" and self._locked_anchor.price > price)
                )
                if locked_behind:
                    best = self._locked_anchor

        # ── Step 9: Compute SL with buffer + liquidity-aware expansion ─
        raw_buf = (
            best.buffer_atr if best.fib_ratio is None else
            effective_buf_table.get(
                best.fib_ratio or 0.0,
                _DEFAULT_BUF_P2 if phase_num == 2 else _DEFAULT_BUF_P3
            )
        )
        buf_mult = sess_mult

        # Liquidity-aware: if a pool sits between proposed SL and price,
        # widen the buffer so the SL is placed behind the pool.
        proposed_sl_pre = (best.price - raw_buf * atr) if pos_side == "long" \
                          else (best.price + raw_buf * atr)
        pool_between, pool_px = self._pool_between_price_and_sl(
            pos_side, price, proposed_sl_pre, liq_snapshot)
        pool_expand = False
        if pool_between and pool_px > 0:
            buf_mult *= POOL_BETWEEN_BUFFER_MULT
            pool_expand = True
            logger.debug(
                f"Trail[{phase_name}]: pool @ ${pool_px:,.1f} between price "
                f"and proposed SL — expanding buffer by {POOL_BETWEEN_BUFFER_MULT}x")

        buf_points = raw_buf * atr * buf_mult
        best = PoolAnchor(**{
            **best.__dict__,
            "buffer_atr": raw_buf,
            "pool_between_expand": pool_expand,
        })

        new_sl = (best.price - buf_points) if pos_side == "long" \
                 else (best.price + buf_points)
        new_sl = round(new_sl, 1)

        # BE floor applies only when BE is executable as a protective stop now.
        # For a short, a BE price below market is a target, not a valid buy-stop.
        be_price = round(self._be_price(pos_side, entry_price, atr, pos, fee_engine), 1)
        be_invalid = self._stop_trigger_invalid_reason(pos_side, be_price, price)
        if be_invalid is None:
            if pos_side == "long" and new_sl < be_price:
                new_sl = be_price
            if pos_side == "short" and new_sl > be_price:
                new_sl = be_price

        # ── Step 10: Validate ──────────────────────────────────────────
        result = self._validate_sl(
            pos_side, new_sl, current_sl, price, entry_price, atr,
            init_dist, effective_min_breathing, best, phase_name,
            hold_reason, r_multiple, primary_swing, momentum_gate, htf_aligned)

        if result is not None and result.new_sl is not None:
            self._locked_anchor     = best
            self._anchor_lock_until = now + ANCHOR_LOCK_SEC
            return result

        if result is not None:
            return result
        return self._hold(
            f"{phase_name}: validation failed — "
            f"{(f'fib={best.fib_ratio:.3f}' if best.fib_ratio is not None else best.anchor_kind)}@${best.price:,.0f}({best.timeframe})",
            hold_reason, r_multiple=r_multiple)

    # ─────────────────────────────────────────────────────────────────────
    # SWING DETECTION (N-bar pivot confirmation)
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def _detect_swings(
        candles: List[dict], strength: int, atr: float,
    ) -> Optional[Tuple[float, float]]:
        """
        Find the most recent valid (swing_low, swing_high) pair from closed bars.

        Pivot: high[i] is the max of high[i-strength..i+strength] (mirrors for low).
        We return the most recent pivot high whose most recent prior pivot low
        produces a swing of at least MIN_SWING_ATR × atr.
        """
        closed = candles[:-1] if len(candles) > 1 else candles
        n = len(closed)
        if n < strength * 2 + 3:
            return None

        scan_end = max(0, n - PIVOT_SCAN_DEPTH)

        def _h(c: dict) -> float:
            return float(c.get('h', c.get('high', 0.0)) or 0.0)
        def _l(c: dict) -> float:
            return float(c.get('l', c.get('low',  0.0)) or 0.0)

        pivot_highs: List[Tuple[int, float]] = []
        for i in range(n - strength - 1, max(strength, scan_end) - 1, -1):
            lo = max(0, i - strength)
            hi = min(n, i + strength + 1)
            window_max = max(_h(closed[j]) for j in range(lo, hi))
            if abs(_h(closed[i]) - window_max) < 1e-8:
                pivot_highs.append((i, _h(closed[i])))
            if len(pivot_highs) >= 5:
                break

        pivot_lows: List[Tuple[int, float]] = []
        for i in range(n - strength - 1, max(strength, scan_end) - 1, -1):
            lo = max(0, i - strength)
            hi = min(n, i + strength + 1)
            window_min = min(_l(closed[j]) for j in range(lo, hi))
            if abs(_l(closed[i]) - window_min) < 1e-8:
                pivot_lows.append((i, _l(closed[i])))
            if len(pivot_lows) >= 5:
                break

        if not pivot_highs or not pivot_lows:
            return None

        min_swing = atr * MIN_SWING_ATR
        for ph_idx, ph_price in pivot_highs:
            for pl_idx, pl_price in pivot_lows:
                if pl_idx >= ph_idx:
                    continue
                if ph_price - pl_price < min_swing:
                    continue
                return (pl_price, ph_price)

        # Bug #20 fix: the original fallback used the raw wick high/low of the
        # last 50 bars with no confirmation — isolated spike candles became
        # Fibonacci origins, placing SL levels behind momentary noise rather
        # than structural pivots.
        #
        # Replacement: require that any extreme accepted as a fallback anchor
        # has at least ONE neighbour bar whose extreme is within 5% of the
        # swing range of the candidate extreme.  If no such neighbour exists
        # the extreme is a solitary wick spike and is rejected.  This forces
        # a HOLD until a real pivot pair forms, which is the correct response
        # when structure is ambiguous.
        recent = closed[max(0, n - min(50, n)):]
        lo = min(_l(c) for c in recent)
        hi = max(_h(c) for c in recent)
        rng = hi - lo
        if rng < min_swing:
            return None

        tolerance = rng * 0.05   # 5% of swing range counts as "near the extreme"

        lo_confirmed = sum(1 for c in recent if _l(c) <= lo + tolerance) >= 2
        hi_confirmed = sum(1 for c in recent if _h(c) >= hi - tolerance) >= 2

        if lo_confirmed and hi_confirmed:
            return (lo, hi)

        # One or both extremes are unconfirmed wick spikes — return None
        # so the caller issues a HOLD rather than building a phantom Fib grid.
        return None

    # ─────────────────────────────────────────────────────────────────────
    # SWING-INVALIDATION
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def _swing_invalidated(
        tf: str, swing: Tuple[float, float],
        candles: List[dict], pos_side: str,
    ) -> bool:
        """
        True when the swing origin has been closed past by a recent bar.

        For a LONG: the swing_low is the origin.  If any CLOSED candle since
        the pivot low's formation has a close below swing_low, the bullish
        swing structure is broken and its Fib grid is now phantom.

        For a SHORT: mirror — swing_high is the origin.

        BUG-FIX E: the original implementation scanned ALL of PIVOT_SCAN_DEPTH
        (80) bars backwards regardless of when the pivot formed.  This caused
        a routine pullback bar from 79 bars ago (e.g. 20 hours on 15m) to
        permanently invalidate the current structural swing, returning "no valid
        swings → HOLD" for the remainder of the position.

        Fix: only scan the last POST_PIVOT_SCAN_BARS=20 bars, which covers the
        period most likely to contain a genuine invalidating close without
        penalising the swing for ancient history.  Operators can tune this via
        TRAIL_SWING_INVAL_LOOKBACK in config.py.
        """
        import config as _si_cfg  # local import — avoid module-level circular
        POST_PIVOT_SCAN_BARS = int(getattr(_si_cfg, 'TRAIL_SWING_INVAL_LOOKBACK', 20))

        swing_low, swing_high = swing
        closed = candles[:-1] if len(candles) > 1 else candles
        if len(closed) < 5:
            return False

        # Scope to the most recent POST_PIVOT_SCAN_BARS bars only
        scan = closed[-min(POST_PIVOT_SCAN_BARS, len(closed)):]

        def _c(c: dict) -> float:
            return float(c.get('c', c.get('close', 0.0)) or 0.0)

        if pos_side == "long":
            for bar in scan:
                if _c(bar) < swing_low - 1e-8:
                    return True
        else:
            for bar in scan:
                if _c(bar) > swing_high + 1e-8:
                    return True
        return False

    # ─────────────────────────────────────────────────────────────────────
    # FIB ANCHOR CONSTRUCTION
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def _build_fib_anchors(
        pos_side: str, price: float, atr: float,
        swing_low: float, swing_high: float,
        timeframe: str, tf_weight: float,
        buf_table: Dict[float, float],
        liq_snapshot=None,
    ) -> List[PoolAnchor]:
        """Compute all Fibonacci retracement levels from a swing."""
        swing_range = swing_high - swing_low
        if swing_range < 1e-8:
            return []

        anchors: List[PoolAnchor] = []
        side_tag = "SSL" if pos_side == "long" else "BSL"

        for ratio, is_institutional, base_q in _FIB_LEVELS:
            # Level price
            if pos_side == "long":
                fib_price = swing_high - ratio * swing_range
                if fib_price >= price or fib_price <= swing_low:
                    continue
            else:
                fib_price = swing_low + ratio * swing_range
                if fib_price <= price or fib_price >= swing_high:
                    continue

            dist_atr = abs(fib_price - price) / atr
            if dist_atr > 10.0:
                continue

            quality = tf_weight * base_q
            if is_institutional:
                quality *= _Q_BONUS_INSTITUTIONAL
            has_interaction = dist_atr < 2.0
            if has_interaction:
                quality *= _Q_BONUS_INTERACTION

            pool_hit = False
            if liq_snapshot is not None:
                pool_hit = LiquidityTrailEngine._fib_has_pool_confluence(
                    fib_price, pos_side, atr, liq_snapshot)
                if pool_hit:
                    quality *= _Q_BONUS_POOL_CONFLUENCE

            anchors.append(PoolAnchor(
                price        = round(fib_price, 1),
                side         = side_tag,
                timeframe    = timeframe,
                sig          = round(quality, 3),
                buffer_atr   = buf_table.get(ratio, _DEFAULT_BUF_P2),
                is_swept     = has_interaction,
                distance_atr = round(dist_atr, 2),
                quality      = round(quality, 3),
                fib_ratio    = ratio,
                swing_low    = swing_low,
                swing_high   = swing_high,
                pool_boost   = pool_hit,
            ))
        return anchors

    # ─────────────────────────────────────────────────────────────────────
    # CLUSTERING
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def _build_structural_anchors(
        pos_side: str, price: float, atr: float, liq_snapshot=None,
        ict_engine=None, now: float = 0.0,
        primary_swing: Optional[Tuple[float, float]] = None,
    ) -> List[PoolAnchor]:
        """
        Build first-class defensive anchors from live liquidity and ICT PD arrays.

        LONG protects below SSL/support; SHORT protects above BSL/resistance.
        These anchors outrank synthetic Fib geometry when they are confirmed.
        """
        anchors: List[PoolAnchor] = []
        side_tag = "SSL" if pos_side == "long" else "BSL"

        def _tf_weight(tf: str) -> float:
            return float(_TF_CFG.get(str(tf).lower(), {"weight": 1.5})["weight"])

        def _protective(anchor_price: float) -> bool:
            if anchor_price <= 0:
                return False
            if pos_side == "long" and anchor_price >= price:
                return False
            if pos_side == "short" and anchor_price <= price:
                return False
            dist_atr = abs(price - anchor_price) / max(atr, 1e-9)
            return 0.25 <= dist_atr <= MAX_SL_DIST_ATR

        def _add(price_anchor: float, tf: str, quality: float, buffer_atr: float,
                 kind: str, pool_boost: bool = False) -> None:
            if not _protective(price_anchor):
                return
            dist_atr = abs(price - price_anchor) / max(atr, 1e-9)
            anchors.append(PoolAnchor(
                price        = round(price_anchor, 1),
                side         = side_tag,
                timeframe    = str(tf or "5m").lower(),
                sig          = round(quality, 3),
                buffer_atr   = round(buffer_atr, 3),
                is_swept     = False,
                distance_atr = round(dist_atr, 2),
                quality      = round(quality, 3),
                fib_ratio    = None,
                swing_low    = primary_swing[0] if primary_swing else None,
                swing_high   = primary_swing[1] if primary_swing else None,
                pool_boost   = pool_boost,
                anchor_kind  = kind,
            ))

        if liq_snapshot is not None:
            pools = (getattr(liq_snapshot, "ssl_pools", []) if pos_side == "long"
                     else getattr(liq_snapshot, "bsl_pools", []))
            for pt in (pools or []):
                try:
                    pool = getattr(pt, "pool", pt)
                    status = str(getattr(pool, "status", "") or "").upper()
                    if "SWEPT" in status or "CONSUMED" in status:
                        continue
                    pp = float(getattr(pool, "price", 0.0) or 0.0)
                    tf = str(getattr(pool, "timeframe", "") or "5m").lower()
                    sig = float(pt.adjusted_sig() if hasattr(pt, "adjusted_sig")
                                else getattr(pt, "significance", 0.0) or 0.0)
                    sources = getattr(pt, "tf_sources", []) or []
                    htf_count = sum(1 for s in sources if str(s).lower() in ("15m", "1h", "4h", "1d"))
                    quality = (5.0 + sig) * (1.0 + 0.18 * htf_count) * (1.0 + 0.15 * _tf_weight(tf))
                    buffer = max(0.35, min(0.95, 0.85 - min(quality, 16.0) * 0.025))
                    _add(pp, tf, quality, buffer, "liquidity_pool", pool_boost=True)
                except Exception:
                    continue

        if ict_engine is not None:
            now_ms = int(now * 1000) if now > 1e6 else int(time.time() * 1000)
            arrays = (
                ((getattr(ict_engine, "order_blocks_bull", []) or []), "order_block"),
                ((getattr(ict_engine, "breaker_blocks_bull", []) or []), "breaker_block"),
            ) if pos_side == "long" else (
                ((getattr(ict_engine, "order_blocks_bear", []) or []), "order_block"),
                ((getattr(ict_engine, "breaker_blocks_bear", []) or []), "breaker_block"),
            )
            for array, kind in arrays:
                for block in list(array):
                    try:
                        if hasattr(block, "is_active") and not block.is_active(now_ms):
                            continue
                        tf = str(getattr(block, "timeframe", "5m") or "5m").lower()
                        strength = float(getattr(block, "strength", 50.0) or 50.0)
                        anchor_px = (float(getattr(block, "low", 0.0) or 0.0)
                                     if pos_side == "long"
                                     else float(getattr(block, "high", 0.0) or 0.0))
                        quality = 6.5 + (strength / 14.0) + _tf_weight(tf)
                        if bool(getattr(block, "bos_confirmed", False)):
                            quality += 1.25
                        if bool(getattr(block, "has_displacement", False)):
                            quality += 1.00
                        if kind == "breaker_block":
                            quality += 1.50
                        buffer = max(0.40, min(1.05, 0.95 - min(quality, 18.0) * 0.025))
                        _add(anchor_px, tf, quality, buffer, kind)
                    except Exception:
                        continue

        if pos_side == "long":
            anchors.sort(key=lambda a: (-a.quality, -a.price))
        else:
            anchors.sort(key=lambda a: (-a.quality, a.price))
        return anchors[:12]

    @staticmethod
    def _cluster_levels(
        anchors: List[PoolAnchor], atr: float,
    ) -> List[PoolAnchor]:
        """Merge Fibonacci levels within FIB_CLUSTER_ATR into single anchors."""
        if not anchors:
            return anchors

        sorted_a = sorted(anchors, key=lambda a: a.price)
        clusters: List[List[PoolAnchor]] = []
        cur = [sorted_a[0]]
        for anchor in sorted_a[1:]:
            if abs(anchor.price - cur[-1].price) / atr <= FIB_CLUSTER_ATR:
                cur.append(anchor)
            else:
                clusters.append(cur)
                cur = [anchor]
        clusters.append(cur)

        result: List[PoolAnchor] = []
        for cluster in clusters:
            if len(cluster) == 1:
                result.append(cluster[0])
                continue
            base = max(cluster, key=lambda a: a.quality)
            total_q = sum(a.quality for a in cluster)
            avg_price = sum(a.price * a.quality for a in cluster) / total_q
            n_tfs = len({a.timeframe for a in cluster})

            # Bug #25 fix: the confluence bonus must require that at least two
            # members share the SAME Fibonacci ratio on DIFFERENT timeframes.
            # Proximity alone (a 1H 50% and a 15m 38.2% happening to land
            # within 0.40 ATR by coincidence) is not institutional confluence —
            # it is a geometric accident.  True confluence is the 50% level on
            # both 1H and 15m pointing to the same price, which means
            # institutional actors on two different TFs are defending the same
            # structural level.
            same_ratio_tfs = len({
                a.timeframe for a in cluster
                if a.fib_ratio is not None and a.fib_ratio == base.fib_ratio
            })
            conf_bonus = _Q_BONUS_CONFLUENCE if same_ratio_tfs >= 2 else 1.0

            extra_bonus = 1.0 + _Q_BONUS_CLUSTER_EXTRA * max(0, len(cluster) - 2)
            final_q = base.quality * conf_bonus * extra_bonus
            result.append(PoolAnchor(
                price        = round(avg_price, 1),
                side         = base.side,
                timeframe    = base.timeframe,
                sig          = round(final_q, 3),
                buffer_atr   = base.buffer_atr,
                is_swept     = base.is_swept,
                distance_atr = base.distance_atr,
                quality      = round(final_q, 3),
                fib_ratio    = base.fib_ratio,
                swing_low    = base.swing_low,
                swing_high   = base.swing_high,
                pool_boost   = any(a.pool_boost for a in cluster),
                is_cluster   = True,
                n_cluster_tfs= n_tfs,
            ))
        return result

    # ─────────────────────────────────────────────────────────────────────
    # SELECT BEST ANCHOR WITH CLOSE-CONFIRMATION
    # ─────────────────────────────────────────────────────────────────────

    def _select_best_fib_confirmed(
        self, anchors: List[PoolAnchor], pos_side: str, price: float,
        candles_by_tf: Dict[str, List[dict]], closes_required: int,
        swings_by_tf: Dict[str, Tuple[float, float]],
    ) -> Optional[PoolAnchor]:
        """
        Select the best anchor that has CLOSE-CONFIRMATION.

        "Close-confirmed" = the level has been broken in the trade direction
        (price closed past it) on at least `closes_required` consecutive
        closed bars in the anchor's TF.

        The anchor's SWING is the close-context: we only count closes that
        are past the Fib level and on the PROFITABLE side of it.

        Ranking:  quality-sorted within confirmed candidates; proximity as
        tiebreaker (tighter SL preferred when quality is tied).

        BUG-FIX G: `closes_required` was applied uniformly across all TFs.
        For 1H this means waiting 2 consecutive 1H bars (= 2 hours) before
        the SL can advance — for most BTC futures trades the position has
        exited before that window closes, so 1H anchors NEVER confirm and
        the trail engine falls back to 15m only (or returns HOLD if 15m has
        no valid swings).

        Fix: scale closes_required by TF.  1H needs only 1 close (1 hour of
        confirmation is sufficient for an hourly structural level). 15m, 5m,
        and 1m retain the supplied value.  The asymmetry is intentional: 1H
        structure is already "heavier" — one close past it is more significant
        than one close past a 1m level.
        """
        # TF-specific closes override (lower TFs retain base value)
        _TF_CLOSES_OVERRIDE = {"1h": 1}  # all others use closes_required

        # Filter to candidates behind price
        candidates = [
            a for a in anchors
            if ((pos_side == "long"  and a.price < price) or
                (pos_side == "short" and a.price > price))
        ]
        if not candidates:
            return None

        # Close-confirmation filter
        confirmed: List[PoolAnchor] = []
        for a in candidates:
            tf_candles = candles_by_tf.get(a.timeframe, [])
            # TF-adjusted closes requirement
            tf_closes_req = _TF_CLOSES_OVERRIDE.get(a.timeframe, closes_required)

            if len(tf_candles) < 3:
                # Not enough candles to confirm — allow passage ONLY for the
                # most conservative golden ratios where the institutional
                # interpretation already implies structural significance.
                if a.anchor_kind != "fib" and a.quality >= 8.0:
                    confirmed.append(a)
                    continue
                if a.fib_ratio in _INSTITUTIONAL_RATIOS and a.is_cluster:
                    confirmed.append(a)
                continue

            n_closes = self._count_closes_past_level(
                pos_side, a.price, tf_candles, tf_closes_req + 2)
            key = (a.timeframe, a.fib_ratio or 0.0,
                   (a.swing_low or 0.0) + (a.swing_high or 0.0))
            self._close_counters[key] = n_closes

            if n_closes >= tf_closes_req:
                confirmed.append(a)

        if not confirmed:
            return None

        if pos_side == "long":
            confirmed.sort(key=lambda a: (-a.quality, -a.price))
        else:
            confirmed.sort(key=lambda a: (-a.quality,  a.price))
        return confirmed[0]

    @staticmethod
    def _count_closes_past_level(
        pos_side: str, level: float, candles: List[dict], lookback: int,
    ) -> int:
        """
        Count consecutive closed bars whose close is PAST `level` in trade
        direction, ending with the most recent closed bar.

        LONG: close > level is "past" (price has moved up past the Fib)
        SHORT: close < level is "past"
        """
        closed = candles[:-1] if len(candles) > 1 else candles
        if not closed:
            return 0
        scan = closed[-min(lookback, len(closed)):]

        def _c(c: dict) -> float:
            return float(c.get('c', c.get('close', 0.0)) or 0.0)

        count = 0
        for bar in reversed(scan):
            cl = _c(bar)
            if pos_side == "long" and cl > level:
                count += 1
            elif pos_side == "short" and cl < level:
                count += 1
            else:
                break
        return count

    # ─────────────────────────────────────────────────────────────────────
    # MOMENTUM GATE
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def _momentum_gate(
        pos_side: str, atr: float, candles_5m: List[dict],
        cvd_trend: float, ict_engine, now: float,
    ) -> str:
        """
        Return the FIRST momentum signal that clears the gate, or "NONE".

        Priority: DISP > CVD > BOS.  "DISP" is evidence of a real impulse
        (body size).  "CVD" is evidence of directional flow.  "BOS" is
        structural confirmation with some staleness tolerance.
        """
        # (a) Displacement candle on last closed 5m bar
        if candles_5m and len(candles_5m) >= 2:
            try:
                lc = candles_5m[-2]
                lo = float(lc.get('o', lc.get('open',  0.0)) or 0.0)
                lc_close = float(lc.get('c', lc.get('close', 0.0)) or 0.0)
                body = abs(lc_close - lo)
                aligned = ((pos_side == "long"  and lc_close > lo) or
                           (pos_side == "short" and lc_close < lo))
                if body >= DISP_MIN_BODY_ATR * atr and aligned:
                    return "DISP"
            except Exception:
                pass

        # (b) CVD trend
        if pos_side == "long" and cvd_trend >= CVD_MIN_TREND:
            return "CVD"
        if pos_side == "short" and cvd_trend <= -CVD_MIN_TREND:
            return "CVD"

        # (c) BOS on 5m/15m aligned with trade, within age limit
        if ict_engine is not None:
            now_ms = int(now * 1000) if now > 1e6 else int(time.time() * 1000)
            for tf in ("5m", "15m"):
                try:
                    st = ict_engine._tf.get(tf)
                    if st is None:
                        continue
                    direction = getattr(st, "bos_direction", None)
                    ts = getattr(st, "bos_timestamp", 0)
                    # BUG-FIX D: use TRAIL_MOMENTUM_BOS_MAX_AGE_MS (10 min default)
                    # instead of BOS_MAX_AGE_MS (2 min) here.  The 2-min window is
                    # correct for entry confirmation but blocks ~87% of trail checks
                    # on 15m TFs where BOS events form ~15 min apart.
                    if ts > 0 and (now_ms - ts) <= TRAIL_MOMENTUM_BOS_MAX_AGE_MS:
                        if pos_side == "long"  and direction == "bullish":
                            return "BOS"
                        if pos_side == "short" and direction == "bearish":
                            return "BOS"
                except Exception:
                    pass
        return "NONE"

    # ─────────────────────────────────────────────────────────────────────
    # COUNTER-BOS DETECTION (sovereign override)
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def _counter_bos_breakout(
        ict_engine, pos_side: str, entry_price: float, current_price: float,
    ) -> bool:
        """
        Fires when a 5m BOS AGAINST the position breaks past entry level.

        LONG: a 5m bearish BOS with bos_level < entry means price has broken
        below our entry — the bullish structure that triggered the trade is
        structurally invalidated.  Immediate BE lock is warranted.

        SHORT: mirror with bullish BOS above entry.
        """
        if ict_engine is None:
            return False
        try:
            tf5 = ict_engine._tf.get("5m")
            if tf5 is None:
                return False
            direction = getattr(tf5, "bos_direction", None)
            level     = float(getattr(tf5, "bos_level", 0.0))
            ts        = getattr(tf5, "bos_timestamp", 0)
            if level <= 0:
                return False
            now_ms = int(time.time() * 1000)
            if ts > 0 and (now_ms - ts) > COUNTER_BOS_MAX_AGE_MS:
                return False
            if pos_side == "long" and direction == "bearish" and level < entry_price:
                return True
            if pos_side == "short" and direction == "bullish" and level > entry_price:
                return True
        except Exception:
            pass
        return False

    # ─────────────────────────────────────────────────────────────────────
    # HTF ALIGNMENT
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def _htf_aligned(ict_engine, pos_side: str) -> Optional[bool]:
        """
        True if the 1H trend aligns with the trade, False if against,
        None if not determinable (HTF engine not ready or no trend).
        """
        if ict_engine is None:
            return None
        try:
            tf1h = ict_engine._tf.get("1h")
            if tf1h is None:
                return None
            trend = str(getattr(tf1h, "trend", "") or "").lower()
            if trend in ("bullish", "bearish"):
                if pos_side == "long":  return trend == "bullish"
                if pos_side == "short": return trend == "bearish"
        except Exception:
            pass
        return None

    # ─────────────────────────────────────────────────────────────────────
    # POOL CONFLUENCE / BETWEEN-CHECK
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def _fib_has_pool_confluence(
        fib_price: float, pos_side: str, atr: float, liq_snapshot,
    ) -> bool:
        """True if a pool is within POOL_CONFLUENCE_ATR of the Fib price."""
        try:
            pools = (getattr(liq_snapshot, 'ssl_pools', []) if pos_side == "long"
                     else getattr(liq_snapshot, 'bsl_pools', []))
            for pt in (pools or []):
                pool = getattr(pt, 'pool', pt)
                pp = float(getattr(pool, 'price', 0.0) or 0.0)
                if pp > 0 and abs(pp - fib_price) / atr <= POOL_CONFLUENCE_ATR:
                    return True
        except Exception:
            pass
        return False

    @staticmethod
    def _pool_between_price_and_sl(
        pos_side: str, price: float, proposed_sl: float, liq_snapshot,
    ) -> Tuple[bool, float]:
        """
        Check if an un-swept pool sits between current price and the proposed SL.

        LONG:  the pool must be < price AND > proposed_sl (a support cluster
               between our SL and current price — stop-hunts will sweep it)
        SHORT: pool > price AND < proposed_sl (mirror)

        Returns (has_pool_between, pool_price).  pool_price=0.0 if none.
        """
        if liq_snapshot is None:
            return False, 0.0
        try:
            pools = (getattr(liq_snapshot, 'ssl_pools', []) if pos_side == "long"
                     else getattr(liq_snapshot, 'bsl_pools', []))
            for pt in (pools or []):
                pool = getattr(pt, 'pool', pt)
                pp = float(getattr(pool, 'price', 0.0) or 0.0)
                if pp <= 0:
                    continue
                # Skip already-swept pools
                status = str(getattr(pool, 'status', ''))
                if 'SWEPT' in status.upper() or 'CONSUMED' in status.upper():
                    continue
                if pos_side == "long":
                    if proposed_sl < pp < price:
                        return True, pp
                else:
                    if price < pp < proposed_sl:
                        return True, pp
        except Exception:
            pass
        return False, 0.0

    # ─────────────────────────────────────────────────────────────────────
    # OTE PULLBACK FREEZE
    # ─────────────────────────────────────────────────────────────────────

    def _check_pullback_freeze(
        self, pos_side: str, price: float,
        swing_low: float, swing_high: float,
        current_sl: float, now: float,
        hold_reason: Optional[List[str]], r_multiple: float,
    ) -> Optional[LiquidityTrailResult]:
        """Freeze trailing when price is retracing inside the OTE zone."""
        swing_range = swing_high - swing_low
        if swing_range < 1e-8:
            return None

        if pos_side == "long":
            retrace = (swing_high - price) / swing_range
        else:
            retrace = (price - swing_low) / swing_range

        in_ote = FREEZE_LOWER <= retrace <= FREEZE_UPPER
        if in_ote:
            if not self._in_pullback_freeze:
                self._in_pullback_freeze = True
                self._freeze_sl_snapshot = current_sl
                logger.info(
                    f"Trail: PULLBACK_FREEZE entered OTE zone — "
                    f"retrace={retrace:.1%} ([{FREEZE_LOWER:.1%}, {FREEZE_UPPER:.1%}]) "
                    f"SL frozen at ${current_sl:,.1f}")
            reason = (
                f"PULLBACK_FREEZE: OTE retrace={retrace:.1%} "
                f"in [{FREEZE_LOWER:.1%}, {FREEZE_UPPER:.1%}] — "
                f"institutional re-accumulation expected"
            )
            return self._hold(
                reason, hold_reason, r_multiple=r_multiple,
                swing_low=swing_low, swing_high=swing_high)
        return None

    # ─────────────────────────────────────────────────────────────────────
    # VALIDATION
    # ─────────────────────────────────────────────────────────────────────

    def _validate_sl(
        self, pos_side: str, new_sl: float, current_sl: float,
        price: float, entry_price: float, atr: float, init_dist: float,
        min_breathing: float, anchor: PoolAnchor, phase: str,
        hold_reason: Optional[List[str]], r_multiple: float,
        primary_swing: Optional[Tuple[float, float]],
        momentum_gate: str, htf_aligned: Optional[bool],
    ) -> Optional[LiquidityTrailResult]:
        """Apply all institutional guards.  Return result if valid, None if rejected."""
        anchor_label = (
            f"fib={anchor.fib_ratio:.3f}"
            if anchor.fib_ratio is not None else anchor.anchor_kind
        )
        # Ratchet
        if pos_side == "long"  and new_sl <= current_sl:
            return self._hold(
                f"RATCHET_FAIL: new_sl=${new_sl:,.1f} <= current=${current_sl:,.1f}",
                hold_reason, r_multiple=r_multiple)
        if pos_side == "short" and new_sl >= current_sl:
            return self._hold(
                f"RATCHET_FAIL: new_sl=${new_sl:,.1f} >= current=${current_sl:,.1f}",
                hold_reason, r_multiple=r_multiple)

        invalid_reason = self._stop_trigger_invalid_reason(pos_side, new_sl, price)
        if invalid_reason:
            return self._hold(
                f"STOP_NOT_EXECUTABLE: {invalid_reason}",
                hold_reason, r_multiple=r_multiple)

        # Breathing room
        dist_to_price = abs(price - new_sl) / atr
        if dist_to_price < min_breathing:
            return self._hold(
                f"BREATHING: {dist_to_price:.2f}ATR<{min_breathing}ATR "
                f"{anchor_label}@${anchor.price:,.0f}({anchor.timeframe})",
                hold_reason, r_multiple=r_multiple)

        # Anti-tightening (preserve initial SL structure)
        sl_dist_from_entry = abs(new_sl - entry_price)
        min_preserve = init_dist * MIN_SL_PRESERVE_FRACTION
        # Exception: BE or better is always allowed even if "tightening"
        be_or_better = ((pos_side == "long"  and new_sl >= entry_price) or
                        (pos_side == "short" and new_sl <= entry_price))
        if sl_dist_from_entry < min_preserve and not be_or_better:
            return self._hold(
                f"ANTI_TIGHTEN: dist_entry={sl_dist_from_entry:.0f}<{min_preserve:.0f} "
                f"({MIN_SL_PRESERVE_FRACTION:.0%} of {init_dist:.0f})",
                hold_reason, r_multiple=r_multiple)

        # Max distance cap
        if dist_to_price > MAX_SL_DIST_ATR:
            return self._hold(
                f"TOO_FAR: {dist_to_price:.1f}ATR>{MAX_SL_DIST_ATR}ATR",
                hold_reason, r_multiple=r_multiple)

        # Minimum meaningful improvement
        improvement_atr = abs(new_sl - current_sl) / atr
        if improvement_atr < MIN_IMPROVEMENT_ATR:
            return self._hold(
                f"MICRO_MOVE: {improvement_atr:.3f}ATR<{MIN_IMPROVEMENT_ATR}ATR",
                hold_reason, r_multiple=r_multiple)

        # All guards passed — emit result
        cluster_tag = f" [×{anchor.n_cluster_tfs}TF]" if anchor.is_cluster else ""
        pool_tag = " +pool" if anchor.pool_boost else ""
        expand_tag = " +expand" if anchor.pool_between_expand else ""
        ratio_tag = anchor_label
        improvement = abs(new_sl - current_sl)

        reason = (
            f"[{phase}] R={r_multiple:.2f}R {ratio_tag}{cluster_tag}{pool_tag}{expand_tag} "
            f"{anchor.side}@${anchor.price:,.0f}({anchor.timeframe}) "
            f"q={anchor.quality:.1f} buf={anchor.buffer_atr:.2f}ATR "
            f"gate={momentum_gate} "
            f"→ SL=${new_sl:,.1f} (+{improvement:.1f}pts)"
        )
        logger.info(f"Trail: {reason}")

        return LiquidityTrailResult(
            new_sl=new_sl, anchor=anchor, reason=reason, phase=phase,
            r_multiple=r_multiple,
            swing_low=primary_swing[0] if primary_swing else None,
            swing_high=primary_swing[1] if primary_swing else None,
            momentum_gate=momentum_gate, htf_aligned=htf_aligned)

    # ─────────────────────────────────────────────────────────────────────
    # HELPERS
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def _stop_trigger_invalid_reason(
        pos_side: str, stop_price: float, market_price: float,
    ) -> Optional[str]:
        """Return why a protective stop cannot be placed at the current market."""
        if stop_price <= 0 or market_price <= 0:
            return "non-positive stop or market price"
        try:
            import config as _cfg
            if hasattr(_cfg, "get_tick_size"):
                tick = float(_cfg.get_tick_size())
            else:
                tick = float(getattr(_cfg, "TICK_SIZE", 0.1))
        except Exception:
            tick = 0.1
        min_gap = max(tick * 0.5, 1e-9)
        if pos_side == "long" and stop_price >= market_price - min_gap:
            return (
                f"SELL stop ${stop_price:,.1f} must be below market "
                f"${market_price:,.1f}"
            )
        if pos_side == "short" and stop_price <= market_price + min_gap:
            return (
                f"BUY stop ${stop_price:,.1f} must be above market "
                f"${market_price:,.1f}"
            )
        return None

    @staticmethod
    def _get_primary_swing(
        swings_by_tf: Dict[str, Tuple[float, float]],
    ) -> Optional[Tuple[float, float]]:
        """Return the swing from the highest-weight available TF."""
        for tf in ("1h", "15m", "5m", "1m"):
            if tf in swings_by_tf:
                return swings_by_tf[tf]
        return None

    @staticmethod
    def _get_last_closed_bar_ts(candles: List[dict]) -> int:
        """Return the timestamp of the last closed bar (in ms)."""
        if len(candles) < 2:
            return 0
        last_closed = candles[-2]
        return int(last_closed.get('t', last_closed.get('timestamp', 0)) or 0)

    @staticmethod
    def _detect_session(ict_engine) -> str:
        """Detect trading session from ICT engine state."""
        if ict_engine is None:
            return ""
        sess = str(getattr(ict_engine, '_session', '') or '').upper()
        if sess in ('LONDON', 'NY', 'ASIA', 'LONDON_NY'):
            return sess
        if sess == 'NEW_YORK':
            return 'NY'
        if sess in ('OFF_HOURS', 'WEEKEND'):
            return ''
        kz = str(getattr(ict_engine, '_killzone', '') or '').upper()
        if 'LONDON' in kz: return 'LONDON'
        if 'NY' in kz or 'NEW_YORK' in kz: return 'NY'
        if 'ASIA' in kz: return 'ASIA'
        return ''

    @staticmethod
    def _hold(
        reason: str, hold_reason: Optional[List[str]], *,
        r_multiple: float = 0.0,
        swing_low: Optional[float] = None,
        swing_high: Optional[float] = None,
        momentum_gate: str = "",
        htf_aligned: Optional[bool] = None,
    ) -> LiquidityTrailResult:
        if hold_reason is not None:
            hold_reason.append(reason)
        return LiquidityTrailResult(
            new_sl=None, anchor=None, reason=reason, phase="HOLD",
            r_multiple=r_multiple,
            swing_low=swing_low, swing_high=swing_high,
            momentum_gate=momentum_gate, htf_aligned=htf_aligned)

    @staticmethod
    def _blocked(
        reason: str, hold_reason: Optional[List[str]],
    ) -> LiquidityTrailResult:
        if hold_reason is not None:
            hold_reason.append(f"BLOCKED:{reason}")
        return LiquidityTrailResult(
            new_sl=None, anchor=None, reason=reason, phase="HOLD",
            trail_blocked=True, block_reason=reason)

"""
liquidity_hunter.py — ICT Liquidity Hunt Engine v1.0
=====================================================
Identifies BSL/SSL clusters, predicts which pool gets raided first using
8 weighted order-flow + structure factors, enters on confirmed sweep, and
targets the opposing pool as the delivery destination.

Core ICT principle:
  Accumulate below SSL → sweep SSL (sell stops taken) → distribute UP to BSL.
  Distribute above BSL → sweep BSL (buy stops taken) → deliver DOWN to SSL.

State Machine:
  NO_RANGE → RANGING → STALKING → APPROACHING → SWEEP_CONFIRMED

8-Factor Prediction Model (weights sum to 1.0):
  1. Order Flow Imbalance  (0.18) — tick buy/sell pressure → BSL or SSL hunt
  2. CVD Slope             (0.15) — sustained volume delta direction
  3. Displacement Bias     (0.12) — recent large-body candle net direction
  4. OB Magnet Pull        (0.15) — active OBs between price and pool attract price
  5. FVG Path Density      (0.10) — unfilled gaps = institutional delivery highway
  6. Dealing Range Pos.    (0.12) — discount→BSL hunt; premium→SSL hunt
  7. Session Timing        (0.08) — London Judas / NY delivery / Asia accumulate
  8. 5m Micro-Structure    (0.10) — HH/HL vs LH/LL + 5m BOS direction

Score is EMA-smoothed (alpha=0.25) to prevent tick-to-tick direction flipping.
A directional score of ±0.45 triggers STALKING; ±0.65 triggers confident APPROACHING.

Sweep Detection (two independent methods):
  A. ICT engine displaced swept pools (displacement_confirmed=True, age <5 min)
  B. Closed 5m candle wick-through-close-opposite pattern (candles_5m[-2])

R:R Gate: minimum 1.5 before signal is generated.

Integration into quant_strategy.py:
  - LiquidityHunter.update() called in _evaluate_entry after ICT/sweep detector
  - get_signal() checked between ICT sweep OTE and flow displacement in routing
  - _evaluate_hunt_entry() handles confirm counter + _launch_entry_async
  - _compute_sl_tp() PATH-C reads _pending_hunt_signal for SL/TP
  - _enter_trade() resets hunter + clears _pending_hunt_signal on fill
  - /huntstatus command in controller.py reads get_status_dict()
"""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════

_WEIGHTS: Dict[str, float] = {
    "flow":       0.18,
    "cvd":        0.15,
    "disp_bias":  0.12,
    "ob_magnet":  0.15,
    "fvg_path":   0.10,
    "dr_pos":     0.12,
    "session":    0.08,
    "micro":      0.10,
}
assert abs(sum(_WEIGHTS.values()) - 1.0) < 1e-9, "Weights must sum to 1.0"

# Range detection
_RANGE_MIN_ATR       = 0.8    # pool-to-pool span must be ≥ 0.8 ATR to qualify
_RANGE_TIMEOUT_S     = 3600   # 1 hour without sweep → reset range

# Score thresholds
_STALK_SCORE_THRESH  = 0.45   # |EMA score| to enter STALKING
_APPROACH_ATR        = 1.5    # distance to predicted pool → APPROACHING state
_EMA_ALPHA           = 0.25   # smoothing factor (prevents tick-to-tick flipping)
_HYSTERESIS_GAP      = 0.05   # minimum swing for direction flip (prevents oscillation)

# Sweep detection
_SWEEP_MAX_AGE_MS    = 300_000   # 5 minutes — stale sweep ignored
_SWEEP_WICK_THRESH   = 0.05      # candle must wick through pool by ≥ 0.05 ATR
_SWEEP_CLOSE_THRESH  = 0.05      # close must reject back by ≥ 0.05 ATR from pool

# Entry gates
_MIN_RR              = 1.5    # minimum risk:reward to generate a signal
_SL_BUFFER_ATR       = 0.50   # SL placed this many ATR beyond swept pool
_TP_BUFFER_ATR       = 0.10   # TP placed this many ATR inside opposing pool

# Update throttle
_UPDATE_INTERVAL_S   = 2.0    # seconds between full prediction re-computes


# ═══════════════════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════════════════

class HuntState(Enum):
    NO_RANGE        = "NO_RANGE"
    RANGING         = "RANGING"
    STALKING        = "STALKING"
    APPROACHING     = "APPROACHING"
    SWEEP_CONFIRMED = "SWEEP_CONFIRMED"


@dataclass
class HuntSignal:
    """
    Entry signal produced by LiquidityHunter on sweep confirmation.
    Consumed once by _evaluate_hunt_entry → _enter_trade.

    created_at is set automatically to wall-clock seconds at instantiation.
    get_signal() rejects this signal once (now - created_at) exceeds
    _SWEEP_MAX_AGE_MS / 1000 seconds — preventing stale sweep entries when
    ICT OTE routing delayed hunt processing for several minutes.
    """
    side:               str     # "long" (SSL swept) | "short" (BSL swept)
    entry_price:        float   # price at signal generation time
    sl_price:           float   # behind swept pool wick
    tp_price:           float   # opposing liquidity pool (less buffer)
    rr:                 float   # risk:reward ratio (tp_dist / sl_dist)
    swept_pool_price:   float   # price of the pool that was swept
    target_pool_price:  float   # price of the opposing delivery target pool
    prediction_score:   float   # EMA-smoothed score that preceded the sweep
    sweep_age_ms:       int     # ms since sweep was confirmed at signal creation
    details:            str     = ""
    created_at:         float   = field(default_factory=time.time)  # wall-clock seconds


@dataclass
class _PoolPair:
    """A BSL/SSL bracket that defines the active ranging window."""
    bsl_price:       float    # buy-side liquidity (equal highs / swing high cluster)
    ssl_price:       float    # sell-side liquidity (equal lows / swing low cluster)
    bsl_touches:     int   = 1
    ssl_touches:     int   = 1
    detected_at:     float = 0.0  # wall-clock seconds


# ═══════════════════════════════════════════════════════════════════════════
# MAIN ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class LiquidityHunter:
    """
    Full-lifecycle ICT liquidity hunt engine.

    Lifecycle per trade:
      1. update() called each tick from _evaluate_entry (throttled to 2 s).
      2. get_signal() checked in routing — non-None → route to hunt entry.
      3. consume_signal() pops the signal (sets it to None).
      4. reset() called after entry order is placed (clears state for next hunt).

    Thread safety: strategy uses a single-threaded tick loop, so no locking needed.
    All deques are bounded — no unbounded memory growth.
    """

    def __init__(self) -> None:
        self._state:            HuntState           = HuntState.NO_RANGE
        self._range:            Optional[_PoolPair] = None
        self._predicted_dir:    str                 = ""   # "bsl" | "ssl" | ""
        self._prev_pred_dir:    str                 = ""   # hysteresis tracking
        self._score_ema:        float               = 0.0
        self._raw_score:        float               = 0.0
        self._signal:           Optional[HuntSignal] = None
        self._range_detected_at: float              = 0.0
        self._last_update:      float               = 0.0
        self._score_components: Dict[str, float]    = {}

    # ─────────────────────────────────────────────────────────────────────────
    # PUBLIC API
    # ─────────────────────────────────────────────────────────────────────────

    def update(
        self,
        price:       float,
        atr:         float,
        now:         float,
        now_ms:      int,
        candles_5m:  List[Dict],
        candles_1m:  List[Dict],
        ict_engine,                 # ICTEngine instance or None
        tick_flow:   float,         # TickFlowEngine.get_signal() — [-1,+1]
        cvd_trend:   float,         # CVDEngine.get_trend_signal() — [-1,+1]
    ) -> None:
        """
        Main update — call every tick from _evaluate_entry after ICT engine update.
        Throttled to _UPDATE_INTERVAL_S to avoid redundant computation.
        """
        if now - self._last_update < _UPDATE_INTERVAL_S:
            return
        self._last_update = now

        if atr < 1e-10 or len(candles_5m) < 10:
            return

        # 1. Detect a fresh BSL/SSL range (only when NO_RANGE)
        self._detect_range(price, atr, now, now_ms, candles_5m, ict_engine)

        if self._state == HuntState.NO_RANGE:
            return

        # 2. Timeout guard — if no sweep after 1 hour, stale range
        if now - self._range_detected_at > _RANGE_TIMEOUT_S:
            logger.debug("LiqHunter: range timeout (1h without sweep) — resetting")
            self._reset_range()
            return

        # 3. Check for fresh sweep — highest priority
        swept_side = self._check_sweep(price, atr, now_ms, candles_5m, ict_engine)
        if swept_side:
            self._on_sweep_confirmed(swept_side, price, atr, now, now_ms)
            return

        # 4. Recompute 8-factor prediction score
        raw = self._compute_prediction_score(
            price, atr, now, candles_5m, candles_1m,
            ict_engine, tick_flow, cvd_trend,
        )
        self._raw_score = raw

        # EMA smoothing — prevents tick-to-tick direction flipping
        self._score_ema = _EMA_ALPHA * raw + (1.0 - _EMA_ALPHA) * self._score_ema

        # 5. Advance state machine
        self._advance_state(price, atr)

    def get_signal(self) -> Optional[HuntSignal]:
        """
        Return pending signal without consuming it.

        Expiry guard (Bug B fix): if the signal has been sitting in _signal
        for longer than _SWEEP_MAX_AGE_MS milliseconds — e.g. while ICT OTE
        routing held priority for several ticks — the underlying sweep is now
        stale and the SL/TP prices are no longer valid.  The signal is cleared
        here so the caller always receives a fresh-or-None value.
        """
        if self._signal is not None:
            age_s = time.time() - self._signal.created_at
            if age_s > _SWEEP_MAX_AGE_MS / 1000.0:
                logger.info(
                    f"LiqHunter: signal expired "
                    f"(age={age_s:.0f}s > {_SWEEP_MAX_AGE_MS//1000}s limit) — clearing"
                )
                self._signal = None
        return self._signal

    def consume_signal(self) -> Optional[HuntSignal]:
        """Pop and return signal — sets internal reference to None."""
        sig = self._signal
        self._signal = None
        return sig

    def reset(self) -> None:
        """
        Called after entry order is placed to clear the signal and
        reset for the next hunt cycle. Preserves no range state
        because BSL/SSL pools persist — a new range will be re-detected.
        """
        self._signal = None
        self._state = HuntState.NO_RANGE
        self._range = None
        self._range_detected_at = 0.0
        self._score_ema = 0.0
        self._raw_score = 0.0
        self._predicted_dir = ""
        self._prev_pred_dir = ""
        self._score_components = {}

    def get_status_dict(self) -> Dict:
        """
        Return a display-friendly dict for /huntstatus Telegram command.
        All float values are rounded to avoid float serialization noise.
        """
        r = self._range
        return {
            "state":           self._state.value,
            "bsl":             round(r.bsl_price, 1)   if r else None,
            "ssl":             round(r.ssl_price, 1)   if r else None,
            "bsl_touches":     r.bsl_touches            if r else None,
            "ssl_touches":     r.ssl_touches            if r else None,
            "predicted_dir":   self._predicted_dir,
            "score_ema":       round(self._score_ema, 3),
            "raw_score":       round(self._raw_score, 3),
            "components":      {k: round(v, 3) for k, v in self._score_components.items()},
            "signal_ready":    self._signal is not None,
            "signal_side":     self._signal.side          if self._signal else None,
            "signal_rr":       round(self._signal.rr, 2) if self._signal else None,
            "signal_sl":       round(self._signal.sl_price, 1) if self._signal else None,
            "signal_tp":       round(self._signal.tp_price, 1) if self._signal else None,
        }

    # ─────────────────────────────────────────────────────────────────────────
    # PRIVATE — RANGE DETECTION
    # ─────────────────────────────────────────────────────────────────────────

    def _detect_range(
        self,
        price:      float,
        atr:        float,
        now:        float,
        now_ms:     int,
        candles_5m: List[Dict],
        ict_engine,
    ) -> None:
        """
        Identify a BSL/SSL bracket containing current price.

        Priority:
          1. ICT engine liquidity_pools (EQH/BSL above, EQL/SSL below)
          2. Swing extreme fallback (swing highs above, swing lows below)

        Pool pair must span at least _RANGE_MIN_ATR × ATR so noise clusters
        don't masquerade as meaningful ranges.
        """
        if self._state != HuntState.NO_RANGE:
            return   # Don't re-detect while in an active range

        bsl: Optional[float] = None
        ssl: Optional[float] = None
        bsl_touches = 1
        ssl_touches = 1

        # ── Primary: ICT liquidity pools ─────────────────────────────────
        if ict_engine is not None and getattr(ict_engine, '_initialized', False):
            try:
                pools = list(ict_engine.liquidity_pools)
                bsl_pools = sorted(
                    [p for p in pools
                     if not p.swept
                     and getattr(p, 'level_type', '') in ('BSL', 'EQH')
                     and p.price > price],
                    key=lambda p: p.price
                )
                ssl_pools = sorted(
                    [p for p in pools
                     if not p.swept
                     and getattr(p, 'level_type', '') in ('SSL', 'EQL')
                     and p.price < price],
                    key=lambda p: -p.price   # closest first
                )
                if bsl_pools:
                    bsl = bsl_pools[0].price
                    bsl_touches = getattr(bsl_pools[0], 'touch_count', 1)
                if ssl_pools:
                    ssl = ssl_pools[0].price
                    ssl_touches = getattr(ssl_pools[0], 'touch_count', 1)
            except Exception as e:
                logger.debug(f"LiqHunter: ICT pool read error: {e}")

        # ── Fallback: candle swing extremes ───────────────────────────────
        # Pass candles_5m[:-1] to strip the forming (unclosed) bar at the
        # slice level.  The original call used min(40, len-2) to "shift"
        # the window but still included candles[-1] in the slice, which
        # meant _find_swing_extremes used the forming bar as the right-hand
        # confirmation neighbour (nh/nl) for the last closed pivot candidate.
        # By slicing [:-1] here the forming bar never enters the function.
        if bsl is None or ssl is None:
            sh, sl = _find_swing_extremes(candles_5m[:-1], 40)
            if bsl is None:
                above = [h for h in sh if h > price + 0.3 * atr]
                if above:
                    bsl = min(above)   # nearest swing high above price
            if ssl is None:
                below = [l for l in sl if l < price - 0.3 * atr]
                if below:
                    ssl = max(below)   # nearest swing low below price

        if bsl is None or ssl is None:
            return   # Can't form a bracket without both sides

        range_size = bsl - ssl
        if range_size < _RANGE_MIN_ATR * atr:
            return   # Range too tight — noise, not meaningful liquidity bracket

        if not (ssl < price < bsl):
            return   # Price must be inside the range

        self._range = _PoolPair(
            bsl_price=bsl,
            ssl_price=ssl,
            bsl_touches=bsl_touches,
            ssl_touches=ssl_touches,
            detected_at=now,
        )
        self._range_detected_at = now
        self._state  = HuntState.RANGING
        self._score_ema = 0.0
        self._predicted_dir = ""
        self._prev_pred_dir = ""

        logger.info(
            f"🎣 LiqHunter: RANGE detected  "
            f"SSL=${ssl:,.0f} — price=${price:,.0f} — BSL=${bsl:,.0f}  "
            f"size={range_size:.0f}pts/{range_size/atr:.1f}ATR  "
            f"bsl_touches={bsl_touches}  ssl_touches={ssl_touches}"
        )

    def _reset_range(self) -> None:
        """Full reset back to NO_RANGE."""
        self._state            = HuntState.NO_RANGE
        self._range            = None
        self._range_detected_at = 0.0
        self._score_ema        = 0.0
        self._raw_score        = 0.0
        self._predicted_dir    = ""
        self._prev_pred_dir    = ""
        self._signal           = None
        self._score_components = {}

    # ─────────────────────────────────────────────────────────────────────────
    # PRIVATE — SWEEP DETECTION
    # ─────────────────────────────────────────────────────────────────────────

    def _check_sweep(
        self,
        price:      float,
        atr:        float,
        now_ms:     int,
        candles_5m: List[Dict],
        ict_engine,
    ) -> Optional[str]:
        """
        Detect BSL or SSL sweep.

        Returns "bsl" → BSL was swept (bearish aftermath → deliver to SSL)
                "ssl" → SSL was swept (bullish aftermath → deliver to BSL)
                None  → no confirmed sweep

        Method A — ICT engine displaced pools (highest priority):
          Pool must be swept + displacement_confirmed + age ≤ 5 min.
          Pool price must be within 0.5 ATR of our tracked BSL/SSL level.

        Method B — Closed 5m candle wick pattern:
          Uses candles_5m[-2] (last CLOSED candle; [-1] is forming and unreliable).
          BSL sweep: wick above BSL by ≥ 0.05 ATR AND close below BSL by ≥ 0.05 ATR.
          SSL sweep: wick below SSL by ≥ 0.05 ATR AND close above SSL by ≥ 0.05 ATR.
        """
        if self._range is None:
            return None

        bsl = self._range.bsl_price
        ssl = self._range.ssl_price

        # ── Method A: ICT engine displaced swept pools ────────────────────
        if ict_engine is not None and getattr(ict_engine, '_initialized', False):
            try:
                for pool in ict_engine.liquidity_pools:
                    if not pool.swept:
                        continue
                    if not getattr(pool, 'displacement_confirmed', False):
                        continue
                    sweep_ts = getattr(pool, 'sweep_timestamp', 0)
                    age_ms   = now_ms - sweep_ts
                    if age_ms > _SWEEP_MAX_AGE_MS or age_ms < 0:
                        continue
                    level_type = getattr(pool, 'level_type', '')
                    pool_price = pool.price

                    if level_type in ('BSL', 'EQH') and abs(pool_price - bsl) < 0.5 * atr:
                        logger.info(
                            f"🎣 LiqHunter: BSL SWEPT (ICT) @ ${pool_price:,.0f}  "
                            f"age={age_ms/1000:.0f}s")
                        return "bsl"

                    if level_type in ('SSL', 'EQL') and abs(pool_price - ssl) < 0.5 * atr:
                        logger.info(
                            f"🎣 LiqHunter: SSL SWEPT (ICT) @ ${pool_price:,.0f}  "
                            f"age={age_ms/1000:.0f}s")
                        return "ssl"
            except Exception as e:
                logger.debug(f"LiqHunter: ICT sweep check error: {e}")

        # ── Method B: Closed 5m candle wick-through-close-opposite ────────
        if len(candles_5m) >= 3:
            last_closed = candles_5m[-2]   # CLOSED candle — [-1] is forming
            hi = float(last_closed['h'])
            lo = float(last_closed['l'])
            cl = float(last_closed['c'])
            buf = _SWEEP_WICK_THRESH * atr

            # BSL sweep: wick above BSL, close rejected back below BSL
            if hi > bsl + buf and cl < bsl - buf:
                logger.info(
                    f"🎣 LiqHunter: BSL SWEPT (candle)  "
                    f"H=${hi:,.0f} > BSL=${bsl:,.0f}  C=${cl:,.0f}")
                return "bsl"

            # SSL sweep: wick below SSL, close rejected back above SSL
            if lo < ssl - buf and cl > ssl + buf:
                logger.info(
                    f"🎣 LiqHunter: SSL SWEPT (candle)  "
                    f"L=${lo:,.0f} < SSL=${ssl:,.0f}  C=${cl:,.0f}")
                return "ssl"

        return None

    # ─────────────────────────────────────────────────────────────────────────
    # PRIVATE — SIGNAL GENERATION
    # ─────────────────────────────────────────────────────────────────────────

    def _on_sweep_confirmed(
        self,
        swept_side: str,     # "bsl" | "ssl"
        price:      float,
        atr:        float,
        now:        float,
        now_ms:     int,
    ) -> None:
        """
        Build HuntSignal after confirmed sweep.

        BSL swept → SHORT (smart money delivered through buy stops → going down)
          SL:  bsl + _SL_BUFFER_ATR × ATR   (behind the sweep high)
          TP:  ssl + _TP_BUFFER_ATR × ATR   (just before SSL)

        SSL swept → LONG (smart money delivered through sell stops → going up)
          SL:  ssl - _SL_BUFFER_ATR × ATR   (behind the sweep low)
          TP:  bsl - _TP_BUFFER_ATR × ATR   (just before BSL)
        """
        if self._range is None:
            return

        bsl = self._range.bsl_price
        ssl = self._range.ssl_price

        if swept_side == "bsl":
            side      = "short"
            sl_raw    = bsl + _SL_BUFFER_ATR * atr
            tp_raw    = ssl + _TP_BUFFER_ATR * atr
            swept_p   = bsl
            target_p  = ssl
        else:
            side      = "long"
            sl_raw    = ssl - _SL_BUFFER_ATR * atr
            tp_raw    = bsl - _TP_BUFFER_ATR * atr
            swept_p   = ssl
            target_p  = bsl

        sl_dist = abs(price - sl_raw)
        tp_dist = abs(price - tp_raw)

        if sl_dist < 1e-10:
            logger.debug("LiqHunter: SL distance zero — skipping signal")
            self._state = HuntState.SWEEP_CONFIRMED
            return

        rr = tp_dist / sl_dist

        if rr < _MIN_RR:
            logger.info(
                f"🎣 LiqHunter: sweep confirmed ({swept_side.upper()}) but "
                f"R:R={rr:.2f} < {_MIN_RR:.1f} — R:R gate rejects signal "
                f"(SL={sl_dist:.0f}pts TP={tp_dist:.0f}pts)")
            self._state = HuntState.SWEEP_CONFIRMED
            return

        self._signal = HuntSignal(
            side              = side,
            entry_price       = price,
            sl_price          = sl_raw,
            tp_price          = tp_raw,
            rr                = rr,
            swept_pool_price  = swept_p,
            target_pool_price = target_p,
            prediction_score  = self._score_ema,
            sweep_age_ms      = 0,
            details = (
                f"swept={swept_side.upper()}@${swept_p:,.0f}  "
                f"target=${target_p:,.0f}  "
                f"R:R=1:{rr:.2f}  "
                f"score_ema={self._score_ema:+.3f}  "
                f"SL=${sl_raw:,.1f}  TP=${tp_raw:,.1f}"
            ),
        )
        self._state = HuntState.SWEEP_CONFIRMED

        logger.info(
            f"🎯 LiqHunter: SIGNAL {side.upper()}  "
            f"swept={swept_side.upper()}@${swept_p:,.0f}  "
            f"SL=${sl_raw:,.1f}  TP=${tp_raw:,.1f}  "
            f"R:R=1:{rr:.2f}  pred_score={self._score_ema:+.3f}"
        )

    # ─────────────────────────────────────────────────────────────────────────
    # PRIVATE — STATE MACHINE ADVANCEMENT
    # ─────────────────────────────────────────────────────────────────────────

    def _advance_state(self, price: float, atr: float) -> None:
        """
        Advance NO_RANGE < RANGING < STALKING < APPROACHING based on
        EMA score magnitude and price proximity to predicted pool.

        Hysteresis: predicted direction requires a _HYSTERESIS_GAP swing to
        reverse. This prevents oscillation at score ≈ ±0.45 boundary.
        """
        if self._range is None:
            return
        if self._state in (HuntState.NO_RANGE, HuntState.SWEEP_CONFIRMED):
            return

        bsl   = self._range.bsl_price
        ssl   = self._range.ssl_price
        score = self._score_ema

        # ── Update predicted direction with hysteresis ────────────────────
        if score > _STALK_SCORE_THRESH + _HYSTERESIS_GAP:
            new_dir = "bsl"
        elif score < -(_STALK_SCORE_THRESH + _HYSTERESIS_GAP):
            new_dir = "ssl"
        elif (abs(score) > _STALK_SCORE_THRESH and
              self._predicted_dir == "" and
              self._prev_pred_dir == ""):
            new_dir = "bsl" if score > 0 else "ssl"
        else:
            new_dir = self._predicted_dir  # hold current

        if new_dir != self._predicted_dir:
            self._prev_pred_dir  = self._predicted_dir
            self._predicted_dir  = new_dir

        # Distance to predicted target
        if self._predicted_dir == "bsl":
            dist_to_pool = abs(price - bsl)
        elif self._predicted_dir == "ssl":
            dist_to_pool = abs(price - ssl)
        else:
            dist_to_pool = min(abs(price - bsl), abs(price - ssl))

        # ── State transitions ─────────────────────────────────────────────
        if self._state == HuntState.RANGING:
            if abs(score) >= _STALK_SCORE_THRESH:
                self._state = HuntState.STALKING
                logger.debug(
                    f"LiqHunter: RANGING → STALKING  "
                    f"dir={self._predicted_dir}  score={score:+.3f}")

        elif self._state == HuntState.STALKING:
            if dist_to_pool <= _APPROACH_ATR * atr:
                self._state = HuntState.APPROACHING
                logger.info(
                    f"🎣 LiqHunter: STALKING → APPROACHING  "
                    f"dir={self._predicted_dir}  dist={dist_to_pool:.0f}pts/{dist_to_pool/atr:.1f}ATR")
            elif abs(score) < _STALK_SCORE_THRESH * 0.5:
                # Score collapsed — retreat to RANGING
                self._state  = HuntState.RANGING
                self._predicted_dir = ""
                logger.debug("LiqHunter: STALKING → RANGING (score collapsed)")

        elif self._state == HuntState.APPROACHING:
            if dist_to_pool > _APPROACH_ATR * 1.5 * atr:
                # Price moved away — back to STALKING
                self._state = HuntState.STALKING
                logger.debug(
                    f"LiqHunter: APPROACHING → STALKING  "
                    f"dist={dist_to_pool:.0f}pts retreated")

    # ─────────────────────────────────────────────────────────────────────────
    # PRIVATE — 8-FACTOR PREDICTION SCORE
    # ─────────────────────────────────────────────────────────────────────────

    def _compute_prediction_score(
        self,
        price:       float,
        atr:         float,
        now:         float,
        candles_5m:  List[Dict],
        candles_1m:  List[Dict],
        ict_engine,
        tick_flow:   float,
        cvd_trend:   float,
    ) -> float:
        """
        Weighted 8-factor score.

        Returns [-1, +1]:
          Positive → price heading to BSL (buy stops above)
          Negative → price heading to SSL (sell stops below)
        """
        if self._range is None:
            return 0.0

        bsl = self._range.bsl_price
        ssl = self._range.ssl_price

        components: Dict[str, float] = {}

        # ─────────────────────────────────────────────────────────────────
        # Factor 1: Order Flow Imbalance  (weight 0.18)
        # tick_flow from TickFlowEngine — positive = net buying → BSL
        # ─────────────────────────────────────────────────────────────────
        f1 = _sigmoid(tick_flow, 1.5)
        components["flow"] = f1

        # ─────────────────────────────────────────────────────────────────
        # Factor 2: CVD Slope  (weight 0.15)
        # cvd_trend from CVDEngine.get_trend_signal() — already in [-1,+1]
        # ─────────────────────────────────────────────────────────────────
        f2 = _sigmoid(cvd_trend, 1.2)
        components["cvd"] = f2

        # ─────────────────────────────────────────────────────────────────
        # Factor 3: Displacement Bias  (weight 0.12)
        # Net body direction of last 4 CLOSED 5m candles.
        # Large bullish candles → heading to BSL; bearish → SSL.
        # Use closed candles only (candles_5m[-5:-1]).
        # ─────────────────────────────────────────────────────────────────
        f3 = 0.0
        if len(candles_5m) >= 6 and atr > 1e-10:
            net_body = sum(
                float(c['c']) - float(c['o'])
                for c in candles_5m[-5:-1]
            )
            f3 = _sigmoid(net_body / atr, 1.0)
        components["disp_bias"] = f3

        # ─────────────────────────────────────────────────────────────────
        # Factor 4: OB Magnet Pull  (weight 0.15)
        # Bullish OBs between price and BSL attract price upward.
        # Bearish OBs between price and SSL attract price downward.
        # ─────────────────────────────────────────────────────────────────
        f4 = 0.0
        if ict_engine is not None and getattr(ict_engine, '_initialized', False):
            try:
                _now_ms_f4 = int(now * 1000)
                bull_q = sum(
                    getattr(ob, 'strength', 50) / 100.0
                    for ob in ict_engine.order_blocks_bull
                    if (hasattr(ob, 'is_active') and ob.is_active(_now_ms_f4)
                        and price < ob.midpoint < bsl)
                )
                bear_q = sum(
                    getattr(ob, 'strength', 50) / 100.0
                    for ob in ict_engine.order_blocks_bear
                    if (hasattr(ob, 'is_active') and ob.is_active(_now_ms_f4)
                        and ssl < ob.midpoint < price)
                )
                f4 = _sigmoid(bull_q - bear_q, 0.8)
            except Exception:
                pass
        components["ob_magnet"] = f4

        # ─────────────────────────────────────────────────────────────────
        # Factor 5: FVG Path Density  (weight 0.10)
        # Unfilled bullish FVGs between price and BSL = upward delivery highway.
        # Unfilled bearish FVGs between price and SSL = downward delivery highway.
        # ─────────────────────────────────────────────────────────────────
        f5 = 0.0
        if ict_engine is not None and getattr(ict_engine, '_initialized', False):
            try:
                _now_ms_f5 = int(now * 1000)
                bull_fvg = sum(
                    1 for fvg in ict_engine.fvgs_bull
                    if (hasattr(fvg, 'is_active') and fvg.is_active(_now_ms_f5)
                        and price < fvg.top < bsl)
                )
                bear_fvg = sum(
                    1 for fvg in ict_engine.fvgs_bear
                    if (hasattr(fvg, 'is_active') and fvg.is_active(_now_ms_f5)
                        and ssl < fvg.bottom < price)
                )
                f5 = _sigmoid((bull_fvg - bear_fvg) * 0.5, 0.8)
            except Exception:
                pass
        components["fvg_path"] = f5

        # ─────────────────────────────────────────────────────────────────
        # Factor 6: Dealing Range Position  (weight 0.12)
        # Discount (<40% of range) → BSL hunt likely (accumulate low, deliver high).
        # Premium  (>60% of range) → SSL hunt likely (distribute high, deliver low).
        # Equilibrium (40–60%)     → neutral.
        # ─────────────────────────────────────────────────────────────────
        f6 = 0.0
        range_size = max(bsl - ssl, 1e-10)
        pd_ratio   = (price - ssl) / range_size   # 0 = at SSL, 1 = at BSL
        if pd_ratio < 0.40:
            f6 = 0.6 * (0.40 - pd_ratio) / 0.40      # 0 → +0.6 in discount
        elif pd_ratio > 0.60:
            f6 = -0.6 * (pd_ratio - 0.60) / 0.40     # 0 → -0.6 in premium
        components["dr_pos"] = f6

        # ─────────────────────────────────────────────────────────────────
        # Factor 7: Session Timing  (weight 0.08)
        # London open (08:00–09:00 UTC): Judas swing = BSL swept first often.
        # NY session  (13:30–15:30 UTC): delivery follows prior manipulation.
        # Asia / off-session: neutral accumulation.
        # ─────────────────────────────────────────────────────────────────
        f7 = 0.0
        try:
            from datetime import datetime, timezone as _tz
            _dt  = datetime.fromtimestamp(now, tz=_tz.utc)
            _hm  = _dt.hour * 60 + _dt.minute
            if 480 <= _hm <= 540:
                # London Judas swing window — BSL often swept first in first 30 min,
                # then price delivers down. Slight positive bias here.
                f7 = 0.20
            elif 810 <= _hm <= 930:
                # NY delivery window — amplify existing EMA bias.
                f7 = self._score_ema * 0.25
        except Exception:
            pass
        components["session"] = f7

        # ─────────────────────────────────────────────────────────────────
        # Factor 8: 5m Micro-Structure  (weight 0.10)
        # Count HH/HL (bullish) vs LH/LL (bearish) over last 6 closed candles.
        # Bonus: 5m BOS direction from ICT engine (±0.20).
        # ─────────────────────────────────────────────────────────────────
        f8 = 0.0
        if len(candles_5m) >= 8:
            closed_slice = candles_5m[-8:-1]   # 7 closed candles
            highs = [float(c['h']) for c in closed_slice]
            lows  = [float(c['l']) for c in closed_slice]
            hh = sum(1 for i in range(1, len(highs)) if highs[i] > highs[i-1])
            lh = sum(1 for i in range(1, len(highs)) if highs[i] < highs[i-1])
            hl = sum(1 for i in range(1, len(lows))  if lows[i]  > lows[i-1])
            ll = sum(1 for i in range(1, len(lows))  if lows[i]  < lows[i-1])
            bull = hh + hl
            bear = lh + ll
            total = max(bull + bear, 1)
            f8 = _sigmoid((bull - bear) / total * 2.0, 1.0)

        # 5m BOS direction bonus (read from ICT engine _tf layer)
        if ict_engine is not None and getattr(ict_engine, '_initialized', False):
            try:
                tf5m = ict_engine._tf.get("5m")
                if tf5m is not None:
                    bos_dir = getattr(tf5m, 'bos_direction', '')
                    if bos_dir == "bullish":
                        f8 = min(1.0,  f8 + 0.20)
                    elif bos_dir == "bearish":
                        f8 = max(-1.0, f8 - 0.20)
            except Exception:
                pass
        components["micro"] = f8

        # ─────────────────────────────────────────────────────────────────
        # Weighted sum → clamp to [-1, +1]
        # ─────────────────────────────────────────────────────────────────
        score = (
            f1 * _WEIGHTS["flow"]      +
            f2 * _WEIGHTS["cvd"]       +
            f3 * _WEIGHTS["disp_bias"] +
            f4 * _WEIGHTS["ob_magnet"] +
            f5 * _WEIGHTS["fvg_path"]  +
            f6 * _WEIGHTS["dr_pos"]    +
            f7 * _WEIGHTS["session"]   +
            f8 * _WEIGHTS["micro"]
        )
        score = max(-1.0, min(1.0, score))
        self._score_components = components
        return score


# ═══════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def _sigmoid(z: float, steepness: float = 1.0) -> float:
    """Fast symmetric sigmoid mapping ℝ → (-1, 1)."""
    sz = z * steepness
    return max(-1.0, min(1.0, sz / (1.0 + abs(sz) * 0.5)))


def _find_swing_extremes(
    candles: List[Dict],
    lookback: int = 40,
) -> Tuple[List[float], List[float]]:
    """
    Find confirmed pivot highs and lows.
    Pivot high: candles[i].H > candles[i-1].H AND candles[i].H > candles[i+1].H
    Pivot low:  candles[i].L < candles[i-1].L AND candles[i].L < candles[i+1].L

    Uses closed candles only (excludes forming bar at index [-1]).
    """
    if len(candles) < 3:
        return [], []
    recent = candles[-lookback:] if len(candles) >= lookback else candles
    highs: List[float] = []
    lows:  List[float] = []
    for i in range(1, len(recent) - 1):
        h  = float(recent[i]['h'])
        l  = float(recent[i]['l'])
        ph = float(recent[i-1]['h'])
        nh = float(recent[i+1]['h'])
        pl = float(recent[i-1]['l'])
        nl = float(recent[i+1]['l'])
        if h > ph and h > nh:
            highs.append(h)
        if l < pl and l < nl:
            lows.append(l)
    return highs, lows

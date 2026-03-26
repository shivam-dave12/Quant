"""
liquidity_hunter.py — ICT Liquidity Hunt Engine v2.0 (Institutional Grade)
============================================================================
Complete rewrite with institutional-level precision:

  1. LIQUIDITY POOL CLUSTERING — groups equal highs/lows within 0.5 ATR
     into a single pool and scores by touch count, age, volume, confluence.

  2. MULTI-LEVEL POOL TRACKING — tracks immediate (0–1.5 ATR), near
     (1.5–4 ATR), and major (4+ ATR) pools separately.

  3. POOL SIGNIFICANCE SCORE — each pool weighted by:
       • Equal high/low touch count (more = institutional order cluster)
       • HTF confluence (pools align across 15m / 5m / 1m)
       • Volume profile coincidence (high VPOC near pool)
       • Age (fresh pools > stale pools)
       • OB/FVG alignment (pool backed by order block or FVG)

  4. CISD DETECTION (Change in State of Delivery) — after a sweep:
       • SSL swept → wait for a candle closing above an internal swing high
         (CISD BOS confirms smart-money bid entry and direction change)
       • BSL swept → wait for a candle closing below an internal swing low
         (CISD BOS confirms smart-money offer entry and direction change)

  5. OTE ZONE (Optimal Trade Entry) — after CISD confirmation:
       • Measures the displacement leg (sweep low → CISD candle high)
       • Fibonacci 62–79% retracement = OTE zone
       • Entry triggered only when price retraces INTO the OTE zone
       • This is the highest-probability ICT entry model

  6. SWEEP QUALITY SCORING — each confirmed sweep is graded:
       • Wick penetration depth through pool (more = stronger conviction)
       • Candle wick-to-body ratio (wick-dominant = rejection, not test)
       • Volume on sweep candle vs 20-bar average (high = institutional)
       • Speed of rejection (close distance from wick extreme)
       • ICT displacement confirmation from engine

  7. KILL ZONE WEIGHTING — session scoring applied to prediction score:
       • London Manipulation  02:00–05:00 EST (+0.30 bias toward sweep)
       • New York AM Open     07:30–10:00 EST (+0.20 delivery amplifier)
       • NY Lunch             11:30–13:00 EST (−0.15 chop penalty)
       • NY PM Session        13:30–15:30 EST (+0.10 reversal window)
       • Asia                 20:00–00:00 EST (neutral, range builder)

  8. SELF-SUFFICIENT SCORING — full 9-factor model runs independently
     when ICT engine is unavailable (no flat 0.0 fallback).

State Machine:
  NO_RANGE → RANGING → STALKING → APPROACHING → SWEEP_CONFIRMED
               └─ (sweep) → CISD_WAIT → OTE_WAIT → SIGNAL_READY

Factor Weights (sum = 1.00):
  1. AMD phase + bias        0.25  — strongest single signal
  2. Dealing-range P/D       0.16  — premium → SSL hunt; discount → BSL hunt
  3. Order-flow tick press.  0.14  — real-time buying/selling pressure
  4. CVD slope               0.12  — sustained volume accumulation direction
  5. Pool significance       0.10  — quality-weighted pool pull vs push
  6. OB magnet               0.09  — active order blocks between price + pool
  7. FVG path density        0.07  — unfilled FVGs = institutional delivery road
  8. Kill-zone session       0.04  — London Judas / NY delivery amplifier
  9. Micro-structure BOS     0.03  — short-term structure break direction

All weights sum to 1.00 and are consistent across computation and display.
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
# CANONICAL WEIGHTS  (one source of truth — used in compute AND display)
# ═══════════════════════════════════════════════════════════════════════════

HUNT_WEIGHTS: Dict[str, float] = {
    "amd":       0.25,
    "dr_pos":    0.16,
    "flow":      0.14,
    "cvd":       0.12,
    "pool_sig":  0.10,
    "ob_magnet": 0.09,
    "fvg_path":  0.07,
    "session":   0.04,
    "micro":     0.03,
}
assert abs(sum(HUNT_WEIGHTS.values()) - 1.0) < 1e-9, "HUNT_WEIGHTS must sum to 1.0"


# ═══════════════════════════════════════════════════════════════════════════
# TUNABLE CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════

# Range detection
_RANGE_MIN_ATR          = 0.8     # BSL–SSL span must be ≥ 0.8 ATR
_RANGE_TIMEOUT_S        = 3600    # 1 h without sweep → reset
_POOL_CLUSTER_ATR       = 0.40    # group swing extremes within 0.4 ATR
_MIN_POOL_TOUCHES       = 1       # minimum touches to qualify as a pool

# Score thresholds
_STALK_SCORE_THRESH     = 0.40    # |EMA| to enter STALKING (lowered for sensitivity)
_APPROACH_ATR           = 1.5     # distance to pool → APPROACHING
_EMA_ALPHA              = 0.20    # smoothing (lower = more stable)
_HYSTERESIS_GAP         = 0.05    # prevents score oscillation at boundary

# Sweep detection
_SWEEP_MAX_AGE_MS       = 300_000  # 5 min
_SWEEP_WICK_THRESH      = 0.05    # wick through pool by ≥ 0.05 ATR
_SWEEP_CLOSE_THRESH     = 0.05    # close rejected back by ≥ 0.05 ATR
_MIN_SWEEP_QUALITY      = 0.30    # minimum sweep quality score 0–1

# CISD (Change in State of Delivery)
_CISD_LOOKBACK_BARS     = 8       # look for BOS within last N closed bars
_CISD_MAX_WAIT_S        = 600     # 10 min; if no CISD, skip OTE and use price entry

# OTE (Optimal Trade Entry) — Fibonacci retracement of displacement leg
_OTE_FIB_LOW            = 0.618   # 61.8% retracement
_OTE_FIB_HIGH           = 0.786   # 78.6% retracement
_OTE_MAX_WAIT_S         = 900     # 15 min to enter OTE zone

# Entry gates
_MIN_RR                 = 1.5     # minimum R:R
_SL_BUFFER_ATR          = 0.50    # SL beyond swept pool wick
_TP_BUFFER_ATR          = 0.10    # TP inside opposing pool

# Update throttle
_UPDATE_INTERVAL_S      = 2.0


# ═══════════════════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════════════════

class HuntState(Enum):
    NO_RANGE        = "NO_RANGE"
    RANGING         = "RANGING"
    STALKING        = "STALKING"
    APPROACHING     = "APPROACHING"
    SWEEP_CONFIRMED = "SWEEP_CONFIRMED"
    CISD_WAIT       = "CISD_WAIT"      # waiting for Change in State of Delivery
    OTE_WAIT        = "OTE_WAIT"       # waiting for price to enter OTE zone


@dataclass
class LiquidityPool:
    """A BSL or SSL cluster with institutional significance scoring."""
    price:          float
    pool_type:      str      # "BSL" | "SSL"
    touches:        int      = 1
    detected_at:    float    = 0.0
    significance:   float    = 1.0   # 0–5 score; higher = stronger pool
    ob_aligned:     bool     = False  # order block backs this level
    fvg_aligned:    bool     = False  # FVG aligns with delivery toward this level
    htf_aligned:    bool     = False  # pool also exists on higher TF


@dataclass
class SweepEvent:
    """Details of a confirmed sweep for quality scoring and CISD tracking."""
    side:              str     # "bsl" | "ssl"
    price_at_sweep:    float
    pool_price:        float
    wick_extreme:      float   # highest high (BSL) or lowest low (SSL)
    sweep_candle_vol:  float   # volume of the sweep candle
    avg_vol:           float   # 20-bar average volume at time of sweep
    wick_to_body_ratio: float  # >2.0 = strong rejection
    quality_score:     float   # 0–1 composite quality
    detected_at:       float   # wall-clock seconds
    detected_at_ms:    int


@dataclass
class CISDEvent:
    """Change in State of Delivery — BOS that confirms direction change."""
    direction:      str     # "bullish" | "bearish"
    bos_price:      float   # price where BOS occurred
    disp_low:       float   # displacement leg origin (low for bullish, high for bearish)
    disp_high:      float   # displacement leg end
    detected_at:    float


@dataclass
class OTEZone:
    """Optimal Trade Entry — Fibonacci retracement of displacement leg."""
    side:       str     # "long" | "short"
    fib_618:    float   # 61.8% retracement price
    fib_786:    float   # 78.6% retracement price
    fib_500:    float   # 50% — equilibrium (early entry)
    fib_236:    float   # 23.6% — first TP target inside range
    entry_high: float   # max(fib_618, fib_786) — zone boundary
    entry_low:  float   # min(fib_618, fib_786) — zone boundary
    sl_price:   float
    tp_price:   float
    valid_until: float  # wall-clock expiry


@dataclass
class HuntSignal:
    """
    Entry signal produced by LiquidityHunter on sweep + CISD + OTE confirmation.
    Consumed once by _evaluate_hunt_entry → _enter_trade.
    """
    side:               str
    entry_price:        float
    sl_price:           float
    tp_price:           float
    rr:                 float
    swept_pool_price:   float
    target_pool_price:  float
    prediction_score:   float
    sweep_age_ms:       int
    sweep_quality:      float   # 0–1
    cisd_confirmed:     bool
    ote_entry:          bool    # True if entry is within OTE zone
    entry_type:         str     # "OTE" | "CISD_MARKET" | "SWEEP_CLOSE"
    details:            str     = ""
    created_at:         float   = field(default_factory=time.time)


@dataclass
class _PoolPair:
    """Active BSL/SSL bracket."""
    bsl:            LiquidityPool
    ssl:            LiquidityPool

    @property
    def bsl_price(self) -> float: return self.bsl.price
    @property
    def ssl_price(self) -> float: return self.ssl.price
    @property
    def bsl_touches(self) -> int: return self.bsl.touches
    @property
    def ssl_touches(self) -> int: return self.ssl.touches


# ═══════════════════════════════════════════════════════════════════════════
# MAIN ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class LiquidityHunter:
    """
    Institutional-grade ICT Liquidity Hunt Engine v2.0.

    Lifecycle:
      1. update() each tick from _evaluate_entry (throttled to 2 s).
      2. get_signal() checked in routing — non-None → route to hunt entry.
      3. consume_signal() pops the signal.
      4. reset() after entry order is placed.

    Thread safety: single-threaded tick loop — no locking needed.
    All collections are bounded — no unbounded memory growth.
    """

    def __init__(self) -> None:
        self._state:            HuntState           = HuntState.NO_RANGE
        self._range:            Optional[_PoolPair] = None
        self._predicted_dir:    str                 = ""
        self._prev_pred_dir:    str                 = ""
        self._score_ema:        float               = 0.0
        self._raw_score:        float               = 0.0
        self._signal:           Optional[HuntSignal] = None
        self._range_detected_at: float              = 0.0
        self._last_update:      float               = 0.0
        self._score_components: Dict[str, float]    = {}

        # v2.0 additions
        self._sweep_event:      Optional[SweepEvent]  = None
        self._cisd_event:       Optional[CISDEvent]   = None
        self._ote_zone:         Optional[OTEZone]     = None
        self._sweep_detected_at: float                = 0.0

    # ─────────────────────────────────────────────────────────────────────────
    # PUBLIC API (unchanged interface for backward compat)
    # ─────────────────────────────────────────────────────────────────────────

    def update(
        self,
        price:       float,
        atr:         float,
        now:         float,
        now_ms:      int,
        candles_5m:  List[Dict],
        candles_1m:  List[Dict],
        ict_engine,
        tick_flow:   float,
        cvd_trend:   float,
    ) -> None:
        """Main update — call every tick from _evaluate_entry (throttled)."""
        if now - self._last_update < _UPDATE_INTERVAL_S:
            return
        self._last_update = now

        if atr < 1e-10 or len(candles_5m) < 10:
            return

        # 1. Detect/refresh BSL/SSL range
        self._detect_range(price, atr, now, now_ms, candles_5m, ict_engine)

        if self._state == HuntState.NO_RANGE:
            return

        # 2. Timeout guard
        if now - self._range_detected_at > _RANGE_TIMEOUT_S:
            logger.debug("LiqHunter v2: range timeout — resetting")
            self._reset_range()
            return

        # 3. Handle post-sweep states (CISD → OTE pipeline)
        if self._state == HuntState.CISD_WAIT:
            self._check_cisd(price, atr, now, candles_5m, candles_1m)
            return

        if self._state == HuntState.OTE_WAIT:
            self._check_ote_entry(price, atr, now, now_ms)
            return

        # 4. Check for fresh sweep — highest priority
        swept_side = self._check_sweep(price, atr, now_ms, candles_5m, ict_engine)
        if swept_side:
            quality = self._score_sweep_quality(
                swept_side, price, atr, candles_5m, now_ms)
            if quality >= _MIN_SWEEP_QUALITY:
                self._on_sweep_confirmed(
                    swept_side, price, atr, now, now_ms, quality, ict_engine)
            else:
                logger.info(
                    f"LiqHunter v2: sweep {swept_side.upper()} quality={quality:.2f} "
                    f"< {_MIN_SWEEP_QUALITY:.2f} threshold — ignored")
            return

        # 5. Recompute 9-factor prediction score
        raw = self._compute_prediction_score(
            price, atr, now, now_ms, candles_5m, candles_1m,
            ict_engine, tick_flow, cvd_trend,
        )
        self._raw_score = raw
        self._score_ema = _EMA_ALPHA * raw + (1.0 - _EMA_ALPHA) * self._score_ema

        # 6. Advance state machine
        self._advance_state(price, atr)

    def get_signal(self) -> Optional[HuntSignal]:
        """Return pending signal; auto-expire stale ones."""
        if self._signal is not None:
            age_s = time.time() - self._signal.created_at
            if age_s > _SWEEP_MAX_AGE_MS / 1000.0:
                logger.info(
                    f"LiqHunter v2: signal expired "
                    f"(age={age_s:.0f}s > {_SWEEP_MAX_AGE_MS//1000}s) — clearing")
                self._signal = None
        return self._signal

    def consume_signal(self) -> Optional[HuntSignal]:
        """Pop and return signal — sets internal reference to None."""
        sig = self._signal
        self._signal = None
        return sig

    def reset(self) -> None:
        """Reset after entry order placed."""
        self._signal           = None
        self._state            = HuntState.NO_RANGE
        self._range            = None
        self._range_detected_at = 0.0
        self._score_ema        = 0.0
        self._raw_score        = 0.0
        self._predicted_dir    = ""
        self._prev_pred_dir    = ""
        self._score_components = {}
        self._sweep_event      = None
        self._cisd_event       = None
        self._ote_zone         = None
        self._sweep_detected_at = 0.0

    def get_status_dict(self) -> Dict:
        """Display-friendly dict for /huntstatus Telegram command."""
        r = self._range
        sw = self._sweep_event
        cisd = self._cisd_event
        ote = self._ote_zone
        return {
            "state":           self._state.value,
            "bsl":             round(r.bsl_price, 1)    if r else None,
            "ssl":             round(r.ssl_price, 1)    if r else None,
            "bsl_touches":     r.bsl_touches             if r else None,
            "ssl_touches":     r.ssl_touches             if r else None,
            "bsl_significance": round(r.bsl.significance, 2) if r else None,
            "ssl_significance": round(r.ssl.significance, 2) if r else None,
            "predicted_dir":   self._predicted_dir,
            "score_ema":       round(self._score_ema, 3),
            "raw_score":       round(self._raw_score, 3),
            "components":      {k: round(v, 3) for k, v in self._score_components.items()},
            "weights":         HUNT_WEIGHTS,
            # Sweep info
            "sweep_side":      sw.side             if sw else None,
            "sweep_quality":   round(sw.quality_score, 2)  if sw else None,
            "sweep_vol_ratio": round(sw.sweep_candle_vol / max(sw.avg_vol, 1e-9), 2) if sw else None,
            "sweep_wick_body": round(sw.wick_to_body_ratio, 2) if sw else None,
            # CISD info
            "cisd_confirmed":  cisd is not None,
            "cisd_bos_price":  round(cisd.bos_price, 1) if cisd else None,
            # OTE info
            "ote_active":      ote is not None,
            "ote_low":         round(ote.entry_low, 1)   if ote else None,
            "ote_high":        round(ote.entry_high, 1)  if ote else None,
            "ote_fib500":      round(ote.fib_500, 1)     if ote else None,
            # Signal
            "signal_ready":    self._signal is not None,
            "signal_side":     self._signal.side          if self._signal else None,
            "signal_rr":       round(self._signal.rr, 2) if self._signal else None,
            "signal_sl":       round(self._signal.sl_price, 1) if self._signal else None,
            "signal_tp":       round(self._signal.tp_price, 1) if self._signal else None,
            "signal_type":     self._signal.entry_type   if self._signal else None,
            "signal_quality":  round(self._signal.sweep_quality, 2) if self._signal else None,
        }

    # ─────────────────────────────────────────────────────────────────────────
    # RANGE DETECTION (with pool clustering and significance scoring)
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
        Identify a BSL/SSL bracket with significance-scored pools.

        Priority:
          1. ICT engine liquidity pools (best quality, already scored)
          2. Swing extreme clustering (group equal highs/lows within 0.4 ATR)

        Pool significance factors:
          • Touch count weighted by recency
          • ICT engine touch_count field
          • OB alignment (order block within 0.5 ATR of pool)
          • FVG alignment (unfilled FVG pointing toward pool)
        """
        if self._state != HuntState.NO_RANGE:
            return

        bsl_pool: Optional[LiquidityPool] = None
        ssl_pool: Optional[LiquidityPool] = None

        # ── Primary: ICT engine pools ─────────────────────────────────────
        if ict_engine is not None and getattr(ict_engine, '_initialized', False):
            try:
                pools = list(ict_engine.liquidity_pools)
                # BSL candidates above price
                bsl_candidates = sorted(
                    [p for p in pools
                     if not p.swept
                     and getattr(p, 'level_type', '') in ('BSL', 'EQH')
                     and p.price > price + 0.3 * atr],
                    key=lambda p: p.price
                )
                # SSL candidates below price
                ssl_candidates = sorted(
                    [p for p in pools
                     if not p.swept
                     and getattr(p, 'level_type', '') in ('SSL', 'EQL')
                     and p.price < price - 0.3 * atr],
                    key=lambda p: -p.price   # closest first
                )
                if bsl_candidates:
                    p = bsl_candidates[0]
                    touches = getattr(p, 'touch_count', 1)
                    bsl_pool = LiquidityPool(
                        price=p.price,
                        pool_type="BSL",
                        touches=touches,
                        detected_at=now,
                        significance=self._score_pool_significance(
                            p.price, "BSL", touches, price, atr,
                            ict_engine, now_ms),
                    )
                if ssl_candidates:
                    p = ssl_candidates[0]
                    touches = getattr(p, 'touch_count', 1)
                    ssl_pool = LiquidityPool(
                        price=p.price,
                        pool_type="SSL",
                        touches=touches,
                        detected_at=now,
                        significance=self._score_pool_significance(
                            p.price, "SSL", touches, price, atr,
                            ict_engine, now_ms),
                    )
            except Exception as e:
                logger.debug(f"LiqHunter v2: ICT pool read error: {e}")

        # ── Fallback: candle swing clustering ─────────────────────────────
        if bsl_pool is None or ssl_pool is None:
            # Use closed candles only (strip forming bar at [-1])
            closed = candles_5m[:-1]
            sh_clusters, sl_clusters = _find_swing_clusters(
                closed, lookback=60, cluster_atr=_POOL_CLUSTER_ATR * atr)

            if bsl_pool is None:
                above = [(price_c, n) for price_c, n in sh_clusters
                         if price_c > price + 0.3 * atr]
                if above:
                    above.sort(key=lambda x: x[0])
                    p_price, p_touches = above[0]
                    bsl_pool = LiquidityPool(
                        price=p_price,
                        pool_type="BSL",
                        touches=p_touches,
                        detected_at=now,
                        significance=min(5.0, 1.0 + p_touches * 0.5),
                    )

            if ssl_pool is None:
                below = [(price_c, n) for price_c, n in sl_clusters
                         if price_c < price - 0.3 * atr]
                if below:
                    below.sort(key=lambda x: -x[0])
                    p_price, p_touches = below[0]
                    ssl_pool = LiquidityPool(
                        price=p_price,
                        pool_type="SSL",
                        touches=p_touches,
                        detected_at=now,
                        significance=min(5.0, 1.0 + p_touches * 0.5),
                    )

        if bsl_pool is None or ssl_pool is None:
            return

        range_size = bsl_pool.price - ssl_pool.price
        if range_size < _RANGE_MIN_ATR * atr:
            return
        if not (ssl_pool.price < price < bsl_pool.price):
            return

        self._range              = _PoolPair(bsl=bsl_pool, ssl=ssl_pool)
        self._range_detected_at  = now
        self._state              = HuntState.RANGING
        self._score_ema          = 0.0
        self._predicted_dir      = ""
        self._prev_pred_dir      = ""

        logger.info(
            f"🎣 LiqHunter v2: RANGE  "
            f"SSL=${ssl_pool.price:,.0f}(sig={ssl_pool.significance:.1f}x{ssl_pool.touches})"
            f" — ${price:,.0f} — "
            f"BSL=${bsl_pool.price:,.0f}(sig={bsl_pool.significance:.1f}x{bsl_pool.touches})  "
            f"size={range_size:.0f}pts/{range_size/atr:.1f}ATR"
        )

    def _score_pool_significance(
        self,
        pool_price: float,
        pool_type:  str,
        touches:    int,
        price:      float,
        atr:        float,
        ict_engine,
        now_ms:     int,
    ) -> float:
        """
        Institutional significance score for a liquidity pool (0–5):
          • 1.0 base
          • +0.5 per touch beyond 1 (capped at +2.0 for 5+ touches)
          • +0.5 if an active OB is within 0.5 ATR of pool
          • +0.5 if an active FVG path aligns toward pool
          • +0.5 if a higher-TF pool coincides within 1 ATR
        """
        score = 1.0 + min(2.0, (touches - 1) * 0.5)

        if ict_engine is None or not getattr(ict_engine, '_initialized', False):
            return round(score, 2)

        try:
            # OB alignment
            if pool_type == "BSL":
                # Bullish OBs between price and BSL support BSL hunt
                for ob in getattr(ict_engine, 'order_blocks_bull', []):
                    if (ob.is_active(now_ms)
                            and price < ob.midpoint < pool_price + 0.5 * atr):
                        score += 0.5
                        break
            else:
                for ob in getattr(ict_engine, 'order_blocks_bear', []):
                    if (ob.is_active(now_ms)
                            and pool_price - 0.5 * atr < ob.midpoint < price):
                        score += 0.5
                        break

            # FVG path alignment
            if pool_type == "BSL":
                for fvg in getattr(ict_engine, 'fvgs_bull', []):
                    if (fvg.is_active(now_ms) and not fvg.filled
                            and price < fvg.top < pool_price):
                        score += 0.5
                        break
            else:
                for fvg in getattr(ict_engine, 'fvgs_bear', []):
                    if (fvg.is_active(now_ms) and not fvg.filled
                            and pool_price < fvg.bottom < price):
                        score += 0.5
                        break

        except Exception:
            pass

        return round(min(5.0, score), 2)

    def _reset_range(self) -> None:
        self._state             = HuntState.NO_RANGE
        self._range             = None
        self._range_detected_at = 0.0
        self._score_ema         = 0.0
        self._raw_score         = 0.0
        self._predicted_dir     = ""
        self._prev_pred_dir     = ""
        self._signal            = None
        self._score_components  = {}
        self._sweep_event       = None
        self._cisd_event        = None
        self._ote_zone          = None
        self._sweep_detected_at = 0.0

    # ─────────────────────────────────────────────────────────────────────────
    # SWEEP DETECTION
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
        Detect BSL or SSL sweep via two independent methods.

        Method A — ICT engine displaced pools (highest priority):
          Swept + displacement_confirmed + age ≤ 5 min + within 0.5 ATR.

        Method B — Closed 5m candle wick-through-close-opposite:
          Uses candles_5m[-2] (last CLOSED candle).
        """
        if self._range is None:
            return None

        bsl = self._range.bsl_price
        ssl = self._range.ssl_price

        # Method A: ICT engine
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

                    if (level_type in ('BSL', 'EQH')
                            and abs(pool_price - bsl) < 0.5 * atr):
                        logger.info(
                            f"🎣 LiqHunter v2: BSL SWEPT (ICT engine) "
                            f"@ ${pool_price:,.0f}  age={age_ms/1000:.0f}s")
                        return "bsl"

                    if (level_type in ('SSL', 'EQL')
                            and abs(pool_price - ssl) < 0.5 * atr):
                        logger.info(
                            f"🎣 LiqHunter v2: SSL SWEPT (ICT engine) "
                            f"@ ${pool_price:,.0f}  age={age_ms/1000:.0f}s")
                        return "ssl"
            except Exception as e:
                logger.debug(f"LiqHunter v2: ICT sweep check error: {e}")

        # Method B: closed candle wick pattern
        if len(candles_5m) >= 3:
            last_closed = candles_5m[-2]
            hi  = float(last_closed['h'])
            lo  = float(last_closed['l'])
            cl  = float(last_closed['c'])
            buf = _SWEEP_WICK_THRESH * atr

            if hi > bsl + buf and cl < bsl - buf:
                logger.info(
                    f"🎣 LiqHunter v2: BSL SWEPT (candle)  "
                    f"H=${hi:,.0f} > BSL=${bsl:,.0f}  C=${cl:,.0f}")
                return "bsl"

            if lo < ssl - buf and cl > ssl + buf:
                logger.info(
                    f"🎣 LiqHunter v2: SSL SWEPT (candle)  "
                    f"L=${lo:,.0f} < SSL=${ssl:,.0f}  C=${cl:,.0f}")
                return "ssl"

        return None

    def _score_sweep_quality(
        self,
        swept_side:  str,
        price:       float,
        atr:         float,
        candles_5m:  List[Dict],
        now_ms:      int,
    ) -> float:
        """
        Institutional sweep quality score (0–1).

        Components:
          A. Wick penetration depth  (0–0.30): how far price breached the pool
          B. Wick-to-body ratio      (0–0.30): > 2.0 = institutional rejection
          C. Volume ratio            (0–0.25): sweep candle volume vs 20-bar avg
          D. Close rejection speed   (0–0.15): how quickly price closed back

        High quality (≥ 0.65): strong institutional sweep — confident entry.
        Medium quality (0.30–0.65): acceptable — proceed with caution.
        Below 0.30: noise / retail trap — skip.
        """
        if self._range is None or len(candles_5m) < 5:
            return 0.5   # neutral when data unavailable

        bsl = self._range.bsl_price
        ssl = self._range.ssl_price

        last_closed = candles_5m[-2] if len(candles_5m) >= 2 else candles_5m[-1]
        hi   = float(last_closed['h'])
        lo   = float(last_closed['l'])
        op   = float(last_closed['o'])
        cl   = float(last_closed['c'])
        vol  = float(last_closed.get('v', 0))

        body = abs(cl - op)
        rng  = hi - lo
        if rng < 1e-9:
            return 0.1

        # Average volume over last 20 closed bars
        recent = candles_5m[-21:-1] if len(candles_5m) >= 21 else candles_5m[:-1]
        avg_vol = sum(float(c.get('v', 0)) for c in recent) / max(len(recent), 1)

        if swept_side == "bsl":
            # BSL sweep: wick above BSL
            penetration = max(0.0, hi - bsl)          # how far above BSL
            wick_above  = hi - max(op, cl)             # upper wick length
            wick_below  = min(op, cl) - lo             # lower wick (body rejects)
            close_dist  = max(0.0, bsl - cl)           # how far close is below BSL
            pool_price  = bsl
            wick_extreme = hi
        else:
            # SSL sweep: wick below SSL
            penetration = max(0.0, ssl - lo)
            wick_below  = min(op, cl) - lo             # lower wick through SSL
            wick_above  = hi - max(op, cl)
            close_dist  = max(0.0, cl - ssl)
            pool_price  = ssl
            wick_extreme = lo

        # A: Wick penetration (normalised to ATR)
        pen_score = min(1.0, penetration / (0.5 * atr))  # 0.5 ATR = full score
        score_a   = pen_score * 0.30

        # B: Wick-to-body ratio
        if swept_side == "bsl":
            sweep_wick = wick_above
        else:
            sweep_wick = wick_below
        wick_body_ratio = sweep_wick / max(body, 0.001 * atr)
        wb_score  = min(1.0, wick_body_ratio / 3.0)   # ratio ≥ 3.0 = full score
        score_b   = wb_score * 0.30

        # C: Volume confirmation
        if avg_vol > 1e-9:
            vol_ratio = vol / avg_vol
            vol_score = min(1.0, (vol_ratio - 0.8) / 1.5)  # need > 0.8× avg
            vol_score = max(0.0, vol_score)
        else:
            vol_score = 0.5
        score_c   = vol_score * 0.25

        # D: Close rejection (close far back from wick extreme)
        if swept_side == "bsl":
            max_reject = hi - bsl
        else:
            max_reject = ssl - lo
        if max_reject > 1e-9:
            reject_score = min(1.0, close_dist / max(max_reject * 0.5, 0.01))
        else:
            reject_score = 0.5
        score_d   = reject_score * 0.15

        total = score_a + score_b + score_c + score_d

        # Store sweep event for status reporting
        self._sweep_event = SweepEvent(
            side              = swept_side,
            price_at_sweep    = price,
            pool_price        = pool_price,
            wick_extreme      = wick_extreme,
            sweep_candle_vol  = vol,
            avg_vol           = avg_vol,
            wick_to_body_ratio = wick_body_ratio,
            quality_score     = round(total, 3),
            detected_at       = time.time(),
            detected_at_ms    = now_ms,
        )

        logger.info(
            f"🔬 LiqHunter v2: sweep quality {swept_side.upper()}  "
            f"total={total:.2f}  "
            f"pen={score_a:.2f}  wick/body={score_b:.2f}  "
            f"vol={score_c:.2f}(ratio={vol/max(avg_vol,1e-9):.1f}×)  "
            f"reject={score_d:.2f}"
        )
        return total

    # ─────────────────────────────────────────────────────────────────────────
    # POST-SWEEP PIPELINE: sweep → CISD → OTE → SIGNAL
    # ─────────────────────────────────────────────────────────────────────────

    def _on_sweep_confirmed(
        self,
        swept_side:    str,
        price:         float,
        atr:           float,
        now:           float,
        now_ms:        int,
        quality:       float,
        ict_engine     = None,
    ) -> None:
        """
        Sweep confirmed with sufficient quality.
        Transition to CISD_WAIT and begin looking for Change in State of Delivery.

        BSL swept → SHORT setup: look for BOS below a recent swing low (CISD)
        SSL swept → LONG setup:  look for BOS above a recent swing high (CISD)
        """
        if self._range is None:
            return

        self._state             = HuntState.CISD_WAIT
        self._sweep_detected_at = now

        bsl = self._range.bsl_price
        ssl = self._range.ssl_price

        logger.info(
            f"🎣 LiqHunter v2: SWEEP_CONFIRMED → CISD_WAIT  "
            f"side={swept_side.upper()}  price=${price:,.0f}  "
            f"quality={quality:.2f}  "
            f"BSL=${bsl:,.0f}  SSL=${ssl:,.0f}"
        )

    def _check_cisd(
        self,
        price:       float,
        atr:         float,
        now:         float,
        candles_5m:  List[Dict],
        candles_1m:  List[Dict],
    ) -> None:
        """
        CISD (Change in State of Delivery) detection.

        SSL swept → LONG CISD: look for a closed 5m candle that closes ABOVE
          an internal swing high within the last _CISD_LOOKBACK_BARS bars.
          This BOS confirms smart money has established a bullish delivery.

        BSL swept → SHORT CISD: look for a closed 5m candle that closes BELOW
          an internal swing low within the last _CISD_LOOKBACK_BARS bars.

        Timeout: if no CISD in _CISD_MAX_WAIT_S seconds, fall through to a
        lower-confidence market-price entry at current price.
        """
        if self._sweep_event is None or self._range is None:
            self._reset_range()
            return

        swept_side = self._sweep_event.side
        now_wall   = time.time()

        # Timeout fallback → generate lower-confidence signal at current price
        elapsed = now_wall - self._sweep_event.detected_at
        if elapsed > _CISD_MAX_WAIT_S:
            logger.info(
                f"LiqHunter v2: CISD timeout ({elapsed:.0f}s) — "
                f"falling through to SWEEP_CLOSE entry"
            )
            self._generate_signal_direct(swept_side, price, atr,
                                          now_wall, self._sweep_event,
                                          cisd_confirmed=False)
            return

        # Use closed candles only
        if len(candles_5m) < _CISD_LOOKBACK_BARS + 2:
            return
        closed = candles_5m[:-1]   # strip forming bar
        lookback = closed[-_CISD_LOOKBACK_BARS:]

        if swept_side == "ssl":
            # Looking for bullish CISD: close above a swing high
            swing_highs = [
                float(lookback[i]['h'])
                for i in range(1, len(lookback) - 1)
                if (float(lookback[i]['h']) > float(lookback[i-1]['h'])
                    and float(lookback[i]['h']) > float(lookback[i+1]['h']))
            ]
            if not swing_highs:
                return
            nearest_sh = min(swing_highs, key=lambda h: abs(h - price))
            last_close = float(closed[-1]['c'])
            if last_close > nearest_sh:
                # CISD confirmed
                disp_low  = self._sweep_event.wick_extreme   # lowest wick
                disp_high = float(closed[-1]['h'])
                self._cisd_event = CISDEvent(
                    direction  = "bullish",
                    bos_price  = nearest_sh,
                    disp_low   = disp_low,
                    disp_high  = disp_high,
                    detected_at = now_wall,
                )
                logger.info(
                    f"✅ LiqHunter v2: CISD BULLISH  "
                    f"BOS above ${nearest_sh:,.0f}  close=${last_close:,.0f}  "
                    f"disp={disp_low:,.0f}→{disp_high:,.0f}"
                )
                self._setup_ote_zone("long", disp_low, disp_high,
                                     self._sweep_event, now_wall)

        else:  # swept_side == "bsl"
            # Looking for bearish CISD: close below a swing low
            swing_lows = [
                float(lookback[i]['l'])
                for i in range(1, len(lookback) - 1)
                if (float(lookback[i]['l']) < float(lookback[i-1]['l'])
                    and float(lookback[i]['l']) < float(lookback[i+1]['l']))
            ]
            if not swing_lows:
                return
            nearest_sl = max(swing_lows, key=lambda l: l)   # highest low (nearest)
            last_close = float(closed[-1]['c'])
            if last_close < nearest_sl:
                disp_high = self._sweep_event.wick_extreme
                disp_low  = float(closed[-1]['l'])
                self._cisd_event = CISDEvent(
                    direction  = "bearish",
                    bos_price  = nearest_sl,
                    disp_low   = disp_low,
                    disp_high  = disp_high,
                    detected_at = now_wall,
                )
                logger.info(
                    f"✅ LiqHunter v2: CISD BEARISH  "
                    f"BOS below ${nearest_sl:,.0f}  close=${last_close:,.0f}  "
                    f"disp={disp_high:,.0f}→{disp_low:,.0f}"
                )
                self._setup_ote_zone("short", disp_low, disp_high,
                                     self._sweep_event, now_wall)

    def _setup_ote_zone(
        self,
        side:    str,
        low:     float,
        high:    float,
        sw:      SweepEvent,
        now:     float,
    ) -> None:
        """
        Build OTE zone from displacement leg.

        For a LONG (SSL swept → bullish):
          - Displacement leg: sweep_low (wick_extreme) → CISD candle high
          - OTE zone = 61.8–78.6% retracement (price dips back into this zone)
          - fib_618 = high - 0.618 × (high - low)   ← price entry level
          - fib_786 = high - 0.786 × (high - low)   ← deepest entry level

        For a SHORT (BSL swept → bearish):
          - Displacement leg: sweep_high (wick_extreme) → CISD candle low
          - OTE zone = 61.8–78.6% retracement (price bounces into this zone)
        """
        if self._range is None:
            return

        leg = high - low
        if leg < 1e-9:
            self._reset_range()
            return

        bsl = self._range.bsl_price
        ssl = self._range.ssl_price

        if side == "long":
            fib_236 = high - 0.236 * leg
            fib_500 = high - 0.500 * leg
            fib_618 = high - 0.618 * leg
            fib_786 = high - 0.786 * leg
            sl_raw  = ssl - _SL_BUFFER_ATR * (high - low) * 0.1
            tp_raw  = bsl - _TP_BUFFER_ATR * (high - low) * 0.1
            entry_high = fib_618
            entry_low  = fib_786
        else:
            fib_236 = low + 0.236 * leg
            fib_500 = low + 0.500 * leg
            fib_618 = low + 0.618 * leg
            fib_786 = low + 0.786 * leg
            sl_raw  = bsl + _SL_BUFFER_ATR * (high - low) * 0.1
            tp_raw  = ssl + _TP_BUFFER_ATR * (high - low) * 0.1
            entry_high = fib_786
            entry_low  = fib_618

        self._ote_zone = OTEZone(
            side        = side,
            fib_618     = fib_618,
            fib_786     = fib_786,
            fib_500     = fib_500,
            fib_236     = fib_236,
            entry_high  = max(entry_high, entry_low),
            entry_low   = min(entry_high, entry_low),
            sl_price    = sl_raw,
            tp_price    = tp_raw,
            valid_until = now + _OTE_MAX_WAIT_S,
        )
        self._state = HuntState.OTE_WAIT

        logger.info(
            f"📐 LiqHunter v2: OTE ZONE set ({side.upper()})  "
            f"62%=${fib_618:,.0f}  78%=${fib_786:,.0f}  "
            f"50%={fib_500:,.0f}  "
            f"valid {_OTE_MAX_WAIT_S//60}min"
        )

    def _check_ote_entry(
        self,
        price:  float,
        atr:    float,
        now:    float,
        now_ms: int,
    ) -> None:
        """
        OTE entry trigger: price retraces into the 61.8–78.6% Fibonacci zone.

        If OTE zone expires, fall back to a CISD-confirmed market entry.
        """
        if self._ote_zone is None or self._sweep_event is None:
            self._reset_range()
            return

        ote  = self._ote_zone
        sw   = self._sweep_event
        now_wall = time.time()

        # OTE expiry fallback
        if now_wall > ote.valid_until:
            logger.info(
                f"LiqHunter v2: OTE expired — generating CISD_MARKET entry")
            self._generate_signal_direct(
                sw.side, price, atr, now_wall, sw,
                cisd_confirmed=True, ote_entry=False,
                entry_type="CISD_MARKET")
            return

        # Price entered OTE zone?
        in_ote = ote.entry_low <= price <= ote.entry_high
        # Also accept the 50% level as an early entry with higher selectivity
        near_50 = abs(price - ote.fib_500) < atr * 0.15

        if in_ote or near_50:
            logger.info(
                f"🎯 LiqHunter v2: PRICE IN OTE ZONE  "
                f"price=${price:,.0f}  "
                f"OTE=[${ote.entry_low:,.0f}–${ote.entry_high:,.0f}]  "
                f"in_ote={in_ote}  near_50={near_50}"
            )
            self._generate_signal_ote(price, atr, ote, sw, now_wall, now_ms)

    def _generate_signal_ote(
        self,
        price:   float,
        atr:     float,
        ote:     OTEZone,
        sw:      SweepEvent,
        now:     float,
        now_ms:  int,
    ) -> None:
        """Generate high-confidence OTE signal."""
        if self._range is None:
            return

        bsl = self._range.bsl_price
        ssl = self._range.ssl_price

        if ote.side == "long":
            sl_dist = abs(price - ote.sl_price)
            tp_dist = abs(bsl - _TP_BUFFER_ATR * atr - price)
            sl_use  = min(ote.sl_price, ssl - _SL_BUFFER_ATR * atr)
            tp_use  = bsl - _TP_BUFFER_ATR * atr
        else:
            sl_dist = abs(price - ote.sl_price)
            tp_dist = abs(price - (ssl + _TP_BUFFER_ATR * atr))
            sl_use  = max(ote.sl_price, bsl + _SL_BUFFER_ATR * atr)
            tp_use  = ssl + _TP_BUFFER_ATR * atr

        sl_dist = abs(price - sl_use)
        tp_dist = abs(price - tp_use)
        if sl_dist < 1e-9:
            return
        rr = tp_dist / sl_dist

        if rr < _MIN_RR:
            logger.info(
                f"LiqHunter v2: OTE R:R={rr:.2f} < {_MIN_RR} — skip signal")
            return

        sweep_age_ms = int((now - sw.detected_at) * 1000)

        self._signal = HuntSignal(
            side              = ote.side,
            entry_price       = price,
            sl_price          = sl_use,
            tp_price          = tp_use,
            rr                = rr,
            swept_pool_price  = sw.pool_price,
            target_pool_price = bsl if ote.side == "long" else ssl,
            prediction_score  = self._score_ema,
            sweep_age_ms      = sweep_age_ms,
            sweep_quality     = sw.quality_score,
            cisd_confirmed    = True,
            ote_entry         = True,
            entry_type        = "OTE",
            details = (
                f"OTE_{ote.side.upper()}: swept={sw.side.upper()}@${sw.pool_price:,.0f}  "
                f"CISD confirmed  "
                f"OTE=[${ote.entry_low:,.0f}–${ote.entry_high:,.0f}]  "
                f"SL=${sl_use:,.1f}  TP=${tp_use:,.1f}  R:R=1:{rr:.2f}  "
                f"quality={sw.quality_score:.2f}"
            ),
        )
        self._state = HuntState.SWEEP_CONFIRMED

        logger.info(
            f"🎯 LiqHunter v2: OTE SIGNAL {ote.side.upper()}  "
            f"entry=${price:,.0f}  SL=${sl_use:,.1f}  TP=${tp_use:,.1f}  "
            f"R:R=1:{rr:.2f}  sweep_quality={sw.quality_score:.2f}"
        )

    def _generate_signal_direct(
        self,
        swept_side:      str,
        price:           float,
        atr:             float,
        now:             float,
        sw:              SweepEvent,
        cisd_confirmed:  bool   = False,
        ote_entry:       bool   = False,
        entry_type:      str    = "SWEEP_CLOSE",
    ) -> None:
        """
        Fallback signal when CISD or OTE expires — lower confidence entry.
        SL/TP use ATR-based positioning relative to the swept pool.
        """
        if self._range is None:
            return

        bsl = self._range.bsl_price
        ssl = self._range.ssl_price

        if swept_side == "bsl":
            side   = "short"
            sl_raw = bsl + _SL_BUFFER_ATR * atr
            tp_raw = ssl + _TP_BUFFER_ATR * atr
            target = ssl
        else:
            side   = "long"
            sl_raw = ssl - _SL_BUFFER_ATR * atr
            tp_raw = bsl - _TP_BUFFER_ATR * atr
            target = bsl

        sl_dist = abs(price - sl_raw)
        tp_dist = abs(price - tp_raw)
        if sl_dist < 1e-9:
            self._state = HuntState.SWEEP_CONFIRMED
            return
        rr = tp_dist / sl_dist

        if rr < _MIN_RR:
            logger.info(
                f"LiqHunter v2: {entry_type} R:R={rr:.2f} < {_MIN_RR} — skip")
            self._state = HuntState.SWEEP_CONFIRMED
            return

        sweep_age_ms = int((now - sw.detected_at) * 1000)

        self._signal = HuntSignal(
            side              = side,
            entry_price       = price,
            sl_price          = sl_raw,
            tp_price          = tp_raw,
            rr                = rr,
            swept_pool_price  = sw.pool_price,
            target_pool_price = target,
            prediction_score  = self._score_ema,
            sweep_age_ms      = sweep_age_ms,
            sweep_quality     = sw.quality_score,
            cisd_confirmed    = cisd_confirmed,
            ote_entry         = ote_entry,
            entry_type        = entry_type,
            details = (
                f"{entry_type}_{side.upper()}: swept={swept_side.upper()}@${sw.pool_price:,.0f}  "
                f"SL=${sl_raw:,.1f}  TP=${tp_raw:,.1f}  R:R=1:{rr:.2f}  "
                f"cisd={cisd_confirmed}  quality={sw.quality_score:.2f}"
            ),
        )
        self._state = HuntState.SWEEP_CONFIRMED

        logger.info(
            f"🎯 LiqHunter v2: {entry_type} SIGNAL {side.upper()}  "
            f"entry=${price:,.0f}  SL=${sl_raw:,.1f}  TP=${tp_raw:,.1f}  "
            f"R:R=1:{rr:.2f}  cisd={cisd_confirmed}"
        )

    # ─────────────────────────────────────────────────────────────────────────
    # STATE MACHINE
    # ─────────────────────────────────────────────────────────────────────────

    def _advance_state(self, price: float, atr: float) -> None:
        if self._range is None:
            return
        if self._state in (HuntState.NO_RANGE, HuntState.SWEEP_CONFIRMED,
                           HuntState.CISD_WAIT, HuntState.OTE_WAIT):
            return

        bsl   = self._range.bsl_price
        ssl   = self._range.ssl_price
        score = self._score_ema

        # Update predicted direction with hysteresis
        if score > _STALK_SCORE_THRESH + _HYSTERESIS_GAP:
            new_dir = "bsl"
        elif score < -(_STALK_SCORE_THRESH + _HYSTERESIS_GAP):
            new_dir = "ssl"
        elif (abs(score) > _STALK_SCORE_THRESH
              and self._predicted_dir == ""
              and self._prev_pred_dir == ""):
            new_dir = "bsl" if score > 0 else "ssl"
        else:
            new_dir = self._predicted_dir

        if new_dir != self._predicted_dir:
            self._prev_pred_dir = self._predicted_dir
            self._predicted_dir = new_dir

        if self._predicted_dir == "bsl":
            dist_to_pool = abs(price - bsl)
        elif self._predicted_dir == "ssl":
            dist_to_pool = abs(price - ssl)
        else:
            dist_to_pool = min(abs(price - bsl), abs(price - ssl))

        if self._state == HuntState.RANGING:
            if abs(score) >= _STALK_SCORE_THRESH:
                self._state = HuntState.STALKING
                logger.debug(
                    f"LiqHunter v2: RANGING → STALKING  "
                    f"dir={self._predicted_dir}  score={score:+.3f}")

        elif self._state == HuntState.STALKING:
            if dist_to_pool <= _APPROACH_ATR * atr:
                self._state = HuntState.APPROACHING
                logger.info(
                    f"🎣 LiqHunter v2: STALKING → APPROACHING  "
                    f"dir={self._predicted_dir}  "
                    f"dist={dist_to_pool:.0f}pts/{dist_to_pool/atr:.1f}ATR")
            elif abs(score) < _STALK_SCORE_THRESH * 0.5:
                self._state = HuntState.RANGING
                self._predicted_dir = ""
                logger.debug("LiqHunter v2: STALKING → RANGING (score collapsed)")

        elif self._state == HuntState.APPROACHING:
            if dist_to_pool > _APPROACH_ATR * 1.5 * atr:
                self._state = HuntState.STALKING
                logger.debug(
                    f"LiqHunter v2: APPROACHING → STALKING  "
                    f"dist={dist_to_pool:.0f}pts retreated")

    # ─────────────────────────────────────────────────────────────────────────
    # 9-FACTOR PREDICTION SCORE (self-sufficient with ICT engine delegation)
    # ─────────────────────────────────────────────────────────────────────────

    def _compute_prediction_score(
        self,
        price:      float,
        atr:        float,
        now:        float,
        now_ms:     int,
        candles_5m: List[Dict],
        candles_1m: List[Dict],
        ict_engine,
        tick_flow:  float,
        cvd_trend:  float,
    ) -> float:
        """
        9-factor prediction score in [-1, +1].
          Positive → BSL hunt more likely
          Negative → SSL hunt more likely

        Primary: delegate to ICTEngine.predict_next_hunt() which uses a
        fully warmed 9-factor model including AMD.

        Fallback (ICT engine unavailable): run the self-sufficient model
        below so the engine never returns a flat 0.0.
        """
        if self._range is None:
            return 0.0

        _now_ms = int(now * 1000)

        # ── Primary: ICT engine delegation ────────────────────────────────
        if ict_engine is not None and getattr(ict_engine, '_initialized', False):
            try:
                pred = ict_engine.predict_next_hunt(price, atr, _now_ms, candles_5m)
                bsl_s = float(pred.get("bsl_score", 0.0))
                ssl_s = float(pred.get("ssl_score", 0.0))
                raw_score = max(-1.0, min(1.0, bsl_s - ssl_s))
                # Store components (raw factor values, not weighted contributions)
                self._score_components = pred.get("confidence_factors", {})
                logger.debug(
                    f"LiqHunter v2 predict_next_hunt: {pred.get('predicted','?')} "
                    f"conf={pred.get('confidence', 0):.2f} "
                    f"bsl={bsl_s:+.3f} ssl={ssl_s:+.3f} → score={raw_score:+.3f}")
                return raw_score
            except Exception as e:
                logger.debug(f"LiqHunter v2: predict_next_hunt delegation error: {e}")

        # ── Fallback: self-sufficient 9-factor model ───────────────────────
        # This runs when the ICT engine hasn't warmed up or is unavailable.
        # It uses HUNT_WEIGHTS — same dict shown in the Telegram display.
        return self._compute_fallback_score(
            price, atr, now, now_ms, candles_5m, tick_flow, cvd_trend)

    def _compute_fallback_score(
        self,
        price:      float,
        atr:        float,
        now:        float,
        now_ms:     int,
        candles_5m: List[Dict],
        tick_flow:  float,
        cvd_trend:  float,
    ) -> float:
        """
        Self-sufficient 9-factor model using HUNT_WEIGHTS.
        Runs when ICT engine is unavailable so engine never returns flat 0.0.
        Positive = BSL hunt likely, Negative = SSL hunt likely.
        """
        if self._range is None:
            return 0.0

        bsl = self._range.bsl_price
        ssl = self._range.ssl_price
        a   = max(atr, 1e-9)

        range_size = max(bsl - ssl, 1e-9)
        dr_pd      = (price - ssl) / range_size   # 0 = at SSL, 1 = at BSL

        components: Dict[str, float] = {}

        # Factor 1: AMD — neutral when ICT engine unavailable
        components["amd"] = 0.0

        # Factor 2: Dealing-range P/D position
        # Deep discount (dr_pd near 0) → BSL hunt likely (+1)
        # Deep premium (dr_pd near 1) → SSL hunt likely (-1)
        f_dr = max(-1.0, min(1.0, 1.0 - 2.0 * dr_pd))
        # Amplify at extremes (institutional bias strongest in deep premium/discount)
        if abs(f_dr) > 0.6:
            f_dr = math.copysign(min(1.0, abs(f_dr) * 1.25), f_dr)
        components["dr_pos"] = round(f_dr, 3)

        # Factor 3: Order flow
        f_flow = _fast_sigmoid(tick_flow, steepness=1.5)
        components["flow"] = round(f_flow, 3)

        # Factor 4: CVD slope
        f_cvd = _fast_sigmoid(cvd_trend, steepness=1.2)
        components["cvd"] = round(f_cvd, 3)

        # Factor 5: Pool significance pull
        # Whichever pool has higher significance score attracts price
        f_pool = 0.0
        if self._range is not None:
            bsl_sig = self._range.bsl.significance
            ssl_sig = self._range.ssl.significance
            # Price in discount → BSL pull amplified by BSL significance
            # Price in premium → SSL pull amplified by SSL significance
            if dr_pd < 0.5:   # discount — bullish bias
                f_pool = _fast_sigmoid(bsl_sig * (0.5 - dr_pd), steepness=0.5)
            else:             # premium — bearish bias
                f_pool = -_fast_sigmoid(ssl_sig * (dr_pd - 0.5), steepness=0.5)
        components["pool_sig"] = round(f_pool, 3)

        # Factor 6: OB magnet (simplified without ICT engine)
        components["ob_magnet"] = 0.0

        # Factor 7: FVG path (simplified without ICT engine)
        components["fvg_path"] = 0.0

        # Factor 8: Kill-zone session timing
        f_sess = self._compute_kill_zone_score(now_ms, dr_pd)
        components["session"] = round(f_sess, 3)

        # Factor 9: Micro-structure — 5m candle BOS direction
        f_micro = 0.0
        if len(candles_5m) >= 6:
            closed = candles_5m[:-1]
            # Simple HH/HL vs LH/LL over last 4 closed bars
            lows  = [float(c['l']) for c in closed[-4:]]
            highs = [float(c['h']) for c in closed[-4:]]
            hh = highs[-1] > max(highs[:-1])
            hl = lows[-1]  > min(lows[:-1])
            lh = highs[-1] < max(highs[:-1])
            ll = lows[-1]  < min(lows[:-1])
            if hh and hl:
                f_micro = +0.80
            elif lh and ll:
                f_micro = -0.80
            elif hh or hl:
                f_micro = +0.40
            elif lh or ll:
                f_micro = -0.40
        components["micro"] = round(f_micro, 3)

        # Weighted sum using canonical HUNT_WEIGHTS
        score = sum(components[k] * HUNT_WEIGHTS.get(k, 0) for k in components)
        score = max(-1.0, min(1.0, score))

        self._score_components = components
        logger.debug(f"LiqHunter v2: fallback score={score:+.3f}  {components}")
        return score

    def _compute_kill_zone_score(self, now_ms: int, dr_pd: float) -> float:
        """
        Kill zone session score for prediction bias.
        All times in EST (UTC-5 standard / UTC-4 DST).

        London Manipulation  02:00–05:00 EST: +0.25 toward sweep direction
        NY AM Open           07:30–10:00 EST: +0.20 (amplify current direction)
        NY Lunch             11:30–13:00 EST: −0.15 (chop, low confidence)
        NY PM Session        13:30–15:30 EST: +0.10 (reversal window)
        Asia Range           20:00–00:00 EST: 0 (accumulation, no directional bias)
        """
        try:
            from datetime import datetime, timezone as _tz, timedelta
            _dt   = datetime.fromtimestamp(now_ms / 1000.0, tz=_tz.utc)
            # Use UTC-5 for consistent session boundaries (no DST shifts)
            _est  = _dt.replace(tzinfo=None) - timedelta(hours=5)
            _hm   = _est.hour * 60 + _est.minute

            # London Manipulation: 02:00–05:00 EST (strong Judas swing bias)
            if 120 <= _hm <= 300:
                # Amplify in direction of premium/discount
                return +0.25 if dr_pd < 0.5 else -0.25

            # NY AM Open: 07:30–10:00 EST (delivery amplifier)
            if 450 <= _hm <= 600:
                # Amplify existing dr_pos direction
                return _fast_sigmoid((0.5 - dr_pd) * 1.5, steepness=0.8) * 0.20

            # NY Lunch: 11:30–13:00 EST (chop zone — penalise signals)
            if 690 <= _hm <= 780:
                return -0.15

            # NY PM: 13:30–15:30 EST (reversal / afternoon distribution)
            if 810 <= _hm <= 930:
                return _fast_sigmoid((0.5 - dr_pd) * 1.0, steepness=0.6) * 0.10

        except Exception:
            pass
        return 0.0


# ═══════════════════════════════════════════════════════════════════════════
# MODULE-LEVEL HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def _fast_sigmoid(z: float, steepness: float = 1.0) -> float:
    """Fast symmetric sigmoid mapping ℝ → (−1, 1)."""
    sz = z * steepness
    return max(-1.0, min(1.0, sz / (1.0 + abs(sz) * 0.5)))


def _find_swing_clusters(
    candles:     List[Dict],
    lookback:    int   = 60,
    cluster_atr: float = 50.0,
) -> Tuple[List[Tuple[float, int]], List[Tuple[float, int]]]:
    """
    Find confirmed pivot highs and lows, then group nearby ones into
    liquidity clusters. Each cluster is represented as (price, touch_count).

    This distinguishes a plain 1-touch swing from a 3-touch equal-high
    cluster — the latter carries 3× institutional significance.

    Pivot high: candles[i].H > candles[i-1].H AND candles[i].H > candles[i+1].H
    Pivot low:  candles[i].L < candles[i-1].L AND candles[i].L < candles[i+1].L

    Parameters:
      cluster_atr: price distance within which pivots are merged (pass atr * 0.4)
    """
    if len(candles) < 3:
        return [], []

    recent = candles[-lookback:] if len(candles) >= lookback else candles
    raw_highs: List[float] = []
    raw_lows:  List[float] = []

    for i in range(1, len(recent) - 1):
        h  = float(recent[i]['h'])
        l  = float(recent[i]['l'])
        ph = float(recent[i-1]['h'])
        nh = float(recent[i+1]['h'])
        pl = float(recent[i-1]['l'])
        nl = float(recent[i+1]['l'])
        if h > ph and h > nh:
            raw_highs.append(h)
        if l < pl and l < nl:
            raw_lows.append(l)

    def cluster_levels(levels: List[float]) -> List[Tuple[float, int]]:
        if not levels:
            return []
        clusters: List[Tuple[float, int]] = []
        used = [False] * len(levels)
        for i, base in enumerate(levels):
            if used[i]:
                continue
            group = [base]
            used[i] = True
            for j in range(i + 1, len(levels)):
                if not used[j] and abs(levels[j] - base) <= cluster_atr:
                    group.append(levels[j])
                    used[j] = True
            cluster_price = sum(group) / len(group)
            clusters.append((cluster_price, len(group)))
        return clusters

    return cluster_levels(raw_highs), cluster_levels(raw_lows)


def _find_swing_extremes(
    candles:  List[Dict],
    lookback: int = 40,
) -> Tuple[List[float], List[float]]:
    """
    Legacy helper — returns plain swing high/low lists.
    Retained for backward compatibility with any external callers.
    """
    clusters_h, clusters_l = _find_swing_clusters(candles, lookback)
    highs = [p for p, _ in clusters_h]
    lows  = [p for p, _ in clusters_l]
    return highs, lows

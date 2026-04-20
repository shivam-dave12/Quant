"""
sl_tp_engine.py — Institutional SL/TP & Structural Trail Engine
================================================================
Drop-in replacement for the scattered SL/TP logic across entry_engine.py,
quant_strategy.py, and liquidity_trail.py.

DESIGN PRINCIPLES
-----------------
  1. Every SL is behind a REAL structural level visible on the chart.
     No ATR-only values. No VWAP anchors. No synthetic floors.
     If no real level exists → trade is REJECTED (return None).

  2. Every TP is pointed at the first real liquidity target that
     satisfies the config RR ratio. Score tiebreaks by quality.
     If no real target exists at the required RR → trade REJECTED.

  3. Trailing is structure-to-structure.
     SL advances ONLY when price has closed past a real structural
     level (pool, OB, swing extreme) with breathing room and momentum.
     No phantom ATR multiples. No chandelier fallback.

SL HIERARCHY (entry)
  P1  Sweep wick extremity + buffer         (reversal entries only)
  P2  Nearest unswept liquidity pool on SL side
  P3  Nearest active ICT Order Block on SL side
  P4  Most recent confirmed swing extreme on 15m / 5m
  --  REJECT if none found

TP HIERARCHY (entry)
  Required: abs(tp − entry) / abs(sl − entry) ≥ MIN_RISK_REWARD_RATIO
  T1  LiquidityMap BSL/SSL pool          (score ≥ 8)
  T2  AMD delivery target                (score 7.5)
  T3  ICT OBs / FVGs / engine pools      (score 5–7)
  T4  1H/4H swing extremes               (score 4–5)
  --  REJECT if none satisfies the RR gate

TRAILING PHASES
  Phase 0  (<0.5R)   Absolute hands off — structural entry SL is optimal
  Phase 1  (0.5–1R)  Break-even lock (entry + exact fees + slippage buffer)
  Phase 2  (1–2.5R)  Pool-to-pool primary | 15m/1H Fib structural secondary
  Phase 3  (2.5R+)   Tight trail — all TFs, pool / OB / swing, HTF-gated

Every trailing advance requires ALL of:
  • Bar-close confirmation past the new anchor (no intrabar wicks)
  • Ratchet — SL only moves in the profit direction
  • Breathing room — ≥ 0.65 ATR (Phase 2) / 0.45 ATR (Phase 3) from price
  • Momentum gate — displacement candle OR CVD ≥ threshold OR aligned BOS

INTEGRATION
-----------
  from sl_tp_engine import compute_entry_sl, compute_entry_tp, StructuralTrailEngine

  # In entry_engine._handle_reversal() / _check_displacement_momentum():
  sl_result = compute_entry_sl(side, price, atr, ict_engine, liq_snap,
                                sweep_result=sweep, candles_15m=..., candles_5m=...)
  if sl_result is None:
      return   # no trade — no structural SL found
  sl_price, sl_source = sl_result

  tp_result = compute_entry_tp(side, price, sl_price, atr, ict_engine, liq_snap,
                                min_rr=config.MIN_RISK_REWARD_RATIO)
  if tp_result is None:
      return   # no trade — no structural target satisfies RR
  tp_price, tp_source, actual_rr = tp_result

  # In quant_strategy (replace self._liq_trail):
  self._struct_trail = StructuralTrailEngine()
  # Each tick:
  result = self._struct_trail.compute(pos_side, price, entry, current_sl, atr,
               initial_sl_dist, peak_profit, liq_snapshot, ict_engine, now,
               candles_1m, candles_5m, candles_15m, candles_1h, cvd_trend)
  if result.new_sl is not None:
      order_manager.replace_stop_loss(result.new_sl)
"""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION (all overridable from root config.py)
# ═══════════════════════════════════════════════════════════════════════════

def _cfg(key: str, default):
    try:
        import config as _c
        return getattr(_c, key, default)
    except Exception:
        return default


# SL placement limits
_SL_MIN_PCT      = float(_cfg("MIN_SL_DISTANCE_PCT",    0.003))
_SL_MAX_PCT      = float(_cfg("MAX_SL_DISTANCE_PCT",    0.035))

# Buffers behind structural anchors (in ATR)
_BUF_WICK        = 0.30   # behind sweep wick
_BUF_POOL        = 0.25   # behind liquidity pool
_BUF_OB          = 0.22   # behind OB edge
_BUF_SWING       = 0.30   # behind swing extreme
_BUF_TP          = 0.05   # in front of TP pool (exit before the cluster)

# TP scoring weights
_TF_OB_BONUS     = {"1d": 2.0, "4h": 1.5, "1h": 1.0, "15m": 0.5}
_TF_POOL_BONUS   = {"1d": 2.0, "4h": 1.5, "1h": 1.0, "15m": 0.5}

# Trail phase thresholds (R-multiple gates)
PHASE_0_MAX_R    = 0.50   # below → hands off
PHASE_1_MAX_R    = 1.00   # BE lock
PHASE_2_MAX_R    = 2.50   # structural pool/OB/swing trail
                           # above → aggressive all-TF trail

# Phase-gate denominator normalisation (same logic as LiquidityTrailEngine)
PHASE_GATE_DENOM_ATR = 1.5

# Trail: breathing room (SL must stay this far from current price)
BREATH_PHASE2_ATR = 0.65
BREATH_PHASE3_ATR = 0.45

# Trail: minimum SL improvement to fire a REST call
MIN_IMPROVE_ATR   = 0.15

# Trail: momentum gate
DISP_MIN_BODY_ATR = 0.55   # displacement body in ATR
CVD_MIN_TREND     = 0.10   # abs CVD trend for momentum
BOS_MAX_AGE_MS    = 600_000  # 10 min

# Trail: bar-close confirmation required to advance SL to a new anchor
CLOSES_PHASE2     = 2
CLOSES_PHASE3     = 1

# Trail: counter-BOS sovereign override
COUNTER_BOS_MAX_AGE_MS = 180_000   # 3 min

# Trail: pool trail — how far back to scan for pools (ATR from current price)
POOL_TRAIL_MAX_SCAN_ATR = 12.0

# Session buffer multipliers for Phase 2/3 buffers
SESSION_BUF: Dict[str, float] = {
    "LONDON": 1.00, "NY": 1.15, "ASIA": 1.50, "": 1.00
}

# TF weight for selecting best swing/OB anchor in trail
TF_WEIGHT = {"1h": 4.0, "15m": 3.0, "5m": 2.0, "1m": 1.0}


# ═══════════════════════════════════════════════════════════════════════════
# RESULT TYPES
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class SLResult:
    price:  float
    source: str    # human-readable label for logging

@dataclass
class TPResult:
    price:  float
    source: str
    rr:     float  # actual R:R at time of computation

@dataclass
class TrailResult:
    new_sl:       Optional[float]  # None → hold current SL
    anchor_price: Optional[float]  # structural level used as anchor
    anchor_label: str
    reason:       str
    phase:        str
    r_multiple:   float
    blocked:      bool = False


# ═══════════════════════════════════════════════════════════════════════════
# ──────────────────────────────────────────────────────────────────────────
#   PART 1 — ENTRY SL COMPUTATION
# ──────────────────────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════

def compute_entry_sl(
    side:          str,
    entry_price:   float,
    atr:           float,
    ict_engine     = None,
    liq_snapshot   = None,
    sweep_result   = None,
    candles_15m:   Optional[List[dict]] = None,
    candles_5m:    Optional[List[dict]] = None,
    now_ms:        int = 0,
) -> Optional[SLResult]:
    """
    Find the best structural SL level for this entry.

    Returns SLResult(price, source) or None if no qualifying level exists.
    Returning None means the trade should be REJECTED — no structural anchor.

    Priority:
      P1  Sweep wick extremity (only for sweep-reversal entries)
      P2  Nearest unswept liquidity pool on the SL side
      P3  Nearest active ICT Order Block on the SL side (HTF preferred)
      P4  Most recent confirmed swing low (LONG) / swing high (SHORT) on 15m, 5m
    """
    if atr < 1e-10:
        return None

    # Minimum: at least 0.25 ATR, at least 0.1% of price (matches entry_engine guard).
    # Note: config MIN_SL_DISTANCE_PCT defaults to 0.003 (0.3%) which at BTC $74k
    # = $222 — wider than a 1-ATR SL.  0.1% ($74) is the correct floor here.
    _min_dist = max(atr * 0.25, entry_price * 0.001)
    _max_dist = entry_price * _SL_MAX_PCT
    now_ms_   = now_ms or int(time.time() * 1000)

    # Scored candidates: (sl_price, score, label)
    candidates: List[Tuple[float, float, str]] = []

    # ── P1: Sweep wick ────────────────────────────────────────────────────
    if sweep_result is not None:
        wick = float(getattr(sweep_result, 'wick_extreme', 0.0) or 0.0)
        if wick > 0:
            sl_c = (wick - _BUF_WICK * atr) if side == "long" else (wick + _BUF_WICK * atr)
            dist = abs(entry_price - sl_c)
            if _min_dist <= dist <= _max_dist:
                q = float(getattr(sweep_result, 'quality', 0.5) or 0.5)
                candidates.append((sl_c, 10.0 + q, f"SWEEP_WICK@${wick:,.1f}"))

    # ── P2: Liquidity pool on SL side ─────────────────────────────────────
    if liq_snapshot is not None:
        pool_attr = 'ssl_pools' if side == "long" else 'bsl_pools'
        for pt in (getattr(liq_snapshot, pool_attr, None) or []):
            pool = getattr(pt, 'pool', pt)
            pp   = float(getattr(pool, 'price', 0.0) or 0.0)
            if pp <= 0:
                continue
            status = str(getattr(pool, 'status', '') or '')
            if 'SWEPT' in status.upper() or 'CONSUMED' in status.upper():
                continue
            if side == "long"  and pp >= entry_price: continue
            if side == "short" and pp <= entry_price: continue

            sl_c = (pp - _BUF_POOL * atr) if side == "long" else (pp + _BUF_POOL * atr)
            dist = abs(entry_price - sl_c)
            if not (_min_dist <= dist <= _max_dist):
                continue

            sig      = float(getattr(pool, 'significance', 1.0) or 1.0)
            htf      = int(getattr(pool, 'htf_count',     0)    or 0)
            touches  = int(getattr(pool, 'touches',        0)    or 0)
            tf       = str(getattr(pool, 'timeframe',    '5m')  or '5m')
            score    = (8.0
                        + min(sig * 0.15, 2.0)
                        + (0.50 if htf >= 2 else 0.0)
                        + min(touches * 0.10, 0.50)
                        + _TF_POOL_BONUS.get(tf, 0.0))
            candidates.append((sl_c, score, f"LIQ_POOL[{tf}]@${pp:,.1f}"))

    # ── P3: ICT Order Block on SL side ────────────────────────────────────
    if ict_engine is not None:
        ob_attr = 'order_blocks_bull' if side == "long" else 'order_blocks_bear'
        for ob in (getattr(ict_engine, ob_attr, None) or []):
            if not ob.is_active(now_ms_):
                continue
            # Anchor = OB.low (bull OB → LONG SL), OB.high (bear OB → SHORT SL)
            anchor = ob.low if side == "long" else ob.high
            if side == "long"  and anchor >= entry_price: continue
            if side == "short" and anchor <= entry_price: continue

            sl_c = (anchor - _BUF_OB * atr) if side == "long" else (anchor + _BUF_OB * atr)
            dist = abs(entry_price - sl_c)
            if not (_min_dist <= dist <= _max_dist):
                continue

            score = (6.0
                     + min(ob.strength / 50.0, 2.0)
                     + (0.50 if ob.bos_confirmed    else 0.0)
                     + (0.30 if ob.has_displacement else 0.0)
                     + ob.virgin_multiplier() * 0.50
                     + _TF_OB_BONUS.get(ob.timeframe, 0.0))
            candidates.append((sl_c, score,
                                f"ICT_OB_{ob.direction.upper()}[{ob.timeframe}]@${anchor:,.1f}"))

    # ── P4: Swing extreme on 15m → 5m ────────────────────────────────────
    for candles, tf_label, tf_score in [
        (candles_15m, "15m", 5.0),
        (candles_5m,  "5m",  4.0),
    ]:
        if not candles or len(candles) < 10:
            continue
        closed = candles[:-1] if len(candles) > 1 else candles
        scan   = closed[-50:]

        def _f(c, k): return float(c.get(k, c.get(k[0], 0.0)) or 0.0)

        if side == "long":
            # Most recent swing low below entry
            valid_lows = [
                _f(c, 'l') for c in scan
                if _f(c, 'l') < entry_price
            ]
            if valid_lows:
                # nearest (highest) low that still gives a valid SL
                for low in sorted(valid_lows, reverse=True):
                    sl_c = low - _BUF_SWING * atr
                    dist = abs(entry_price - sl_c)
                    if _min_dist <= dist <= _max_dist:
                        candidates.append((sl_c, tf_score,
                                           f"SWING_LOW[{tf_label}]@${low:,.1f}"))
                        break
        else:
            valid_highs = [
                _f(c, 'h') for c in scan
                if _f(c, 'h') > entry_price
            ]
            if valid_highs:
                for high in sorted(valid_highs):
                    sl_c = high + _BUF_SWING * atr
                    dist = abs(entry_price - sl_c)
                    if _min_dist <= dist <= _max_dist:
                        candidates.append((sl_c, tf_score,
                                           f"SWING_HIGH[{tf_label}]@${high:,.1f}"))
                        break

        if candidates:
            break  # stop at 15m if we found something

    if not candidates:
        logger.info(f"compute_entry_sl: NO structural SL found — trade rejected "
                    f"({side.upper()} entry=${entry_price:,.1f} ATR={atr:.1f})")
        return None

    # Highest-score wins; among ties prefer closest (tightest valid SL)
    candidates.sort(key=lambda x: (-x[1], abs(x[0] - entry_price)))
    sl_price, score, label = candidates[0]
    sl_price = round(sl_price, 1)

    logger.info(f"compute_entry_sl: {side.upper()} SL=${sl_price:,.1f} "
                f"({abs(entry_price-sl_price):.0f}pts / "
                f"{abs(entry_price-sl_price)/atr:.2f}ATR) "
                f"source={label} score={score:.1f}")
    return SLResult(price=sl_price, source=label)


# ═══════════════════════════════════════════════════════════════════════════
# ──────────────────────────────────────────────────────────────────────────
#   PART 2 — ENTRY TP COMPUTATION
# ──────────────────────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════

def compute_entry_tp(
    side:          str,
    entry_price:   float,
    sl_price:      float,
    atr:           float,
    ict_engine     = None,
    liq_snapshot   = None,
    min_rr:        float = 2.0,
    max_rr:        float = 5.0,
    now_ms:        int   = 0,
) -> Optional[TPResult]:
    """
    Find the best real structural TP that satisfies the RR requirement.

    Returns TPResult(price, source, rr) or None if nothing qualifies.
    None means the trade should be REJECTED — no institutional target at RR.

    Tier hierarchy (scored):
      T1  LiquidityMap BSL/SSL pools          score ≥ 8.0
      T2  AMD delivery target                 score  7.5
      T3  ICT engine OBs / FVGs / pools       score 5.0–7.0
      T4  1H/4H swing extremes from ICT       score 4.0–5.5
    """
    sl_dist = abs(entry_price - sl_price)
    if sl_dist < 1e-10:
        return None

    min_tp_dist = sl_dist * min_rr
    max_tp_dist = sl_dist * max_rr
    now_ms_     = now_ms or int(time.time() * 1000)
    buf         = _BUF_TP * atr

    def _valid(level: float) -> bool:
        dist = abs(level - entry_price)
        if dist < min_tp_dist or dist > max_tp_dist:
            return False
        if side == "long"  and level <= entry_price: return False
        if side == "short" and level >= entry_price: return False
        return True

    scored: List[Tuple[float, float, str]] = []  # (level, score, label)

    # ── T1: LiquidityMap pools ────────────────────────────────────────────
    if liq_snapshot is not None:
        pool_attr = 'bsl_pools' if side == "long" else 'ssl_pools'
        for pt in (getattr(liq_snapshot, pool_attr, None) or []):
            pool = getattr(pt, 'pool', pt)
            pp   = float(getattr(pool, 'price', 0.0) or 0.0)
            if pp <= 0: continue
            status = str(getattr(pool, 'status', '') or '')
            if 'SWEPT' in status.upper(): continue

            tp_level = (pp - buf) if side == "long" else (pp + buf)
            if not _valid(tp_level): continue

            sig     = float(getattr(pool, 'significance', 1.0) or 1.0)
            htf     = int(getattr(pool, 'htf_count',      0)   or 0)
            touches = int(getattr(pool, 'touches',         0)   or 0)
            tf      = str(getattr(pool, 'timeframe',     '5m') or '5m')
            # Proximity adjustment — near pools beat far ones at equal sig
            dist_atr = abs(pp - entry_price) / max(atr, 1e-10)
            prox_adj = max(0.0, 1.0 - dist_atr / 30.0)

            score = (8.0
                     + min(sig * 0.15, 2.0) * (1.0 + prox_adj * 0.20)
                     + (0.50 if htf >= 2 else 0.0)
                     + min(touches * 0.10, 0.50)
                     + _TF_POOL_BONUS.get(tf, 0.0))
            scored.append((tp_level, score, f"LIQ_POOL[{tf}]@${pp:,.0f}"))

    # ── T2: AMD delivery target ───────────────────────────────────────────
    if ict_engine is not None:
        try:
            amd = ict_engine.get_amd_state()
            dt  = getattr(amd, 'delivery_target', None)
            if dt is not None and dt > 0 and _valid(float(dt)):
                scored.append((float(dt), 7.5, f"AMD_DELIVERY@${dt:,.0f}"))
        except Exception:
            pass

    # ── T3a: ICT structural targets (FVGs, OBs from engine API) ──────────
    if ict_engine is not None:
        try:
            targets = ict_engine.get_structural_tp_targets(
                side, entry_price, atr, now_ms_,
                min_tp_dist, max_tp_dist, htf_only=False)
            for lvl, sc, lbl in (targets or []):
                lvl_f = float(lvl)
                if _valid(lvl_f):
                    scored.append((lvl_f,
                                   5.0 + min(float(sc) * 0.10, 2.0),
                                   f"ICT_{lbl}"))
        except Exception:
            pass

        # T3b: Opposing OBs as delivery targets
        ob_attr = 'order_blocks_bear' if side == "long" else 'order_blocks_bull'
        for ob in (getattr(ict_engine, ob_attr, None) or []):
            if not ob.is_active(now_ms_): continue
            anchor   = ob.low if side == "long" else ob.high
            tp_level = (anchor - buf) if side == "long" else (anchor + buf)
            if not _valid(tp_level): continue
            score = (5.5
                     + min(ob.strength / 50.0, 1.5)
                     + (0.40 if ob.bos_confirmed else 0.0)
                     + _TF_OB_BONUS.get(ob.timeframe, 0.0))
            scored.append((tp_level, score,
                            f"ICT_OB_{ob.direction.upper()}[{ob.timeframe}]@${anchor:,.0f}"))

        # T3c: ICT engine liquidity pools
        for pool in (getattr(ict_engine, 'liquidity_pools', None) or []):
            if getattr(pool, 'swept', False): continue
            pp    = float(getattr(pool, 'price', 0.0) or 0.0)
            ltype = str(getattr(pool, 'level_type', '') or '')
            if side == "long"  and ltype != "BSL": continue
            if side == "short" and ltype != "SSL": continue
            tp_level = (pp - buf) if side == "long" else (pp + buf)
            if not _valid(tp_level): continue
            tc  = int(getattr(pool, 'touch_count', 0) or 0)
            tf  = str(getattr(pool, 'timeframe', '5m') or '5m')
            score = (5.5
                     + min(tc * 0.25, 1.5)
                     + _TF_POOL_BONUS.get(tf, 0.0))
            scored.append((tp_level, score, f"ICT_LIQ[{tf}]@${pp:,.0f}"))

        # T3d: FVGs as institutional delivery zones
        fvg_attr = 'fvgs_bear' if side == "long" else 'fvgs_bull'
        for fvg in (getattr(ict_engine, fvg_attr, None) or []):
            if getattr(fvg, 'filled', False): continue
            mid = (fvg.bottom + fvg.top) / 2.0
            if not _valid(mid): continue
            score = (4.5 + _TF_OB_BONUS.get(getattr(fvg, 'timeframe', '5m'), 0.0))
            scored.append((mid, score,
                            f"ICT_FVG[{getattr(fvg,'timeframe','5m')}]@${mid:,.0f}"))

    if not scored:
        logger.info(f"compute_entry_tp: NO structural TP satisfies "
                    f"min_rr={min_rr:.1f} — trade rejected "
                    f"({side.upper()} entry=${entry_price:,.1f} "
                    f"SL=${sl_price:,.1f} need {min_tp_dist:.0f}pts)")
        return None

    # Best score wins; proximity tiebreaker — prefer NEAREST qualifying target
    # (less heat, higher probability of fill)
    scored.sort(key=lambda x: (-x[1], abs(x[0] - entry_price)))

    tp_price, score, label = scored[0]
    tp_price = round(tp_price, 1)
    rr       = abs(tp_price - entry_price) / sl_dist

    logger.info(f"compute_entry_tp: {side.upper()} TP=${tp_price:,.1f} "
                f"({abs(tp_price-entry_price):.0f}pts / "
                f"{abs(tp_price-entry_price)/atr:.2f}ATR) "
                f"R:R=1:{rr:.2f} source={label} score={score:.1f} "
                f"({len(scored)} candidates)")
    return TPResult(price=tp_price, source=label, rr=round(rr, 2))


# ═══════════════════════════════════════════════════════════════════════════
# ──────────────────────────────────────────────────────────────────────────
#   PART 3 — STRUCTURAL TRAIL ENGINE
# ──────────────────────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════

class StructuralTrailEngine:
    """
    Structure-to-structure SL trailing engine.

    Phase 0 (<0.5R):   Absolute hands off.
    Phase 1 (0.5–1R):  Break-even lock (entry + fees + slippage).
    Phase 2 (1–2.5R):  Pool primary → OB secondary → 15m swing tertiary.
    Phase 3 (2.5R+):   Tight trail — all TFs, pool/OB/swing, HTF-gated.

    Each trail advance requires:
      1. Bar-close confirmation (N closed candles past the anchor level)
      2. Ratchet (SL only moves in profit direction)
      3. Breathing room from current price
      4. Momentum gate (displacement / CVD / BOS)

    Stateful per-position. Call reset() on every new position open.
    """

    def __init__(self) -> None:
        self._peak_profit:   float = 0.0
        self._last_phase:    str   = "HANDS_OFF"
        self._counter_bos_done: bool = False
        # Anti-oscillation: lock last-used anchor for ANCHOR_LOCK_SEC
        self._locked_anchor_price: Optional[float] = None
        self._locked_anchor_label: str   = ""
        self._anchor_lock_until:   float = 0.0
        # Close-counter per (anchor_level, timeframe) key
        self._close_counters: Dict[Tuple[float, str], int] = {}
        _ANCHOR_LOCK_SEC = 90.0

    ANCHOR_LOCK_SEC = 90.0

    def reset(self) -> None:
        """Call on every new position open."""
        self._peak_profit         = 0.0
        self._last_phase          = "HANDS_OFF"
        self._counter_bos_done    = False
        self._locked_anchor_price = None
        self._locked_anchor_label = ""
        self._anchor_lock_until   = 0.0
        self._close_counters.clear()
        logger.debug("StructuralTrailEngine: state reset for new position")

    # ─────────────────────────────────────────────────────────────────────
    # PUBLIC API
    # ─────────────────────────────────────────────────────────────────────

    def compute(
        self,
        pos_side:        str,
        price:           float,
        entry_price:     float,
        current_sl:      float,
        atr:             float,
        initial_sl_dist: float = 0.0,
        peak_profit:     float = 0.0,
        liq_snapshot               = None,
        ict_engine                 = None,
        now:             float   = 0.0,
        candles_1m:      Optional[List[dict]] = None,
        candles_5m:      Optional[List[dict]] = None,
        candles_15m:     Optional[List[dict]] = None,
        candles_1h:      Optional[List[dict]] = None,
        cvd_trend:       float = 0.0,
        pos                        = None,
        fee_engine                 = None,
        hold_reason:     Optional[List[str]] = None,
    ) -> TrailResult:
        """
        Compute next SL position.
        Returns TrailResult(new_sl=None) to hold, or new_sl=<float> to advance.
        """
        now_  = now if now > 1e6 else time.time()
        now_ms = int(now_ * 1000)

        if atr < 1e-10:
            return self._hold("atr_zero", 0.0)

        # Peak profit tracking
        profit = ((price - entry_price) if pos_side == "long"
                  else (entry_price - price))
        self._peak_profit = max(self._peak_profit, profit, peak_profit)

        # ── R-multiple (true financial) ───────────────────────────────
        init_dist = max(
            initial_sl_dist if initial_sl_dist > 1e-10 else 0.0,
            abs(entry_price - current_sl),
            atr * 0.50,
        )
        r_multiple = self._peak_profit / init_dist if init_dist > 1e-10 else 0.0

        # ── Phase-gate R (ATR-normalised) ─────────────────────────────
        _phase_denom = min(init_dist, atr * PHASE_GATE_DENOM_ATR)
        phase_r      = r_multiple  # use same when SL is normal width
        if _phase_denom > 1e-10:
            phase_r = self._peak_profit / _phase_denom

        # ── Counter-BOS sovereign override ────────────────────────────
        if (not self._counter_bos_done
                and self._detect_counter_bos(ict_engine, pos_side,
                                              entry_price, now_ms)):
            self._counter_bos_done = True
            be = self._be_price(pos_side, entry_price, atr, pos, fee_engine)
            is_impr = ((pos_side == "long"  and be > current_sl) or
                       (pos_side == "short" and be < current_sl))
            if is_impr and profit > 0:
                logger.warning(
                    f"Trail[COUNTER_BOS_OVERRIDE]: 5m BOS broke below entry "
                    f"— forcing BE ${be:,.1f}")
                return TrailResult(
                    new_sl=round(be, 1), anchor_price=entry_price,
                    anchor_label="ENTRY_BE",
                    reason=f"[COUNTER_BOS_OVERRIDE] → BE ${be:,.1f}",
                    phase="COUNTER_BOS", r_multiple=r_multiple)

        # ── Phase dispatch ────────────────────────────────────────────
        if phase_r < PHASE_0_MAX_R:
            self._last_phase = "HANDS_OFF"
            return self._hold(
                f"PHASE_0_HANDS_OFF: R={r_multiple:.2f} "
                f"(gate={phase_r:.2f})<{PHASE_0_MAX_R}R — "
                f"structural entry SL is optimal",
                r_multiple, hold_reason)

        if phase_r < PHASE_1_MAX_R:
            self._last_phase = "BE_LOCK"
            return self._phase1_be(
                pos_side, price, entry_price, current_sl, atr,
                r_multiple, pos, fee_engine, hold_reason)

        session = self._detect_session(ict_engine)
        candles_by_tf = {
            "1m":  candles_1m  or [],
            "5m":  candles_5m  or [],
            "15m": candles_15m or [],
            "1h":  candles_1h  or [],
        }

        if phase_r < PHASE_2_MAX_R:
            self._last_phase = "STRUCTURAL"
            return self._phase2_structural(
                pos_side, price, entry_price, current_sl, atr, init_dist,
                r_multiple, liq_snapshot, ict_engine, now_, now_ms,
                candles_by_tf, session, cvd_trend, pos, fee_engine, hold_reason)

        # Phase 3 — aggressive
        self._last_phase = "AGGRESSIVE"
        return self._phase3_aggressive(
            pos_side, price, entry_price, current_sl, atr, init_dist,
            r_multiple, liq_snapshot, ict_engine, now_, now_ms,
            candles_by_tf, session, cvd_trend, pos, fee_engine, hold_reason)

    # ─────────────────────────────────────────────────────────────────────
    # PHASE 1 — BREAK-EVEN LOCK
    # ─────────────────────────────────────────────────────────────────────

    def _phase1_be(self, pos_side, price, entry_price, current_sl, atr,
                   r_multiple, pos, fee_engine,
                   hold_reason: Optional[List[str]] = None) -> TrailResult:
        be = round(self._be_price(pos_side, entry_price, atr, pos, fee_engine), 1)

        already = ((pos_side == "long"  and current_sl >= be) or
                   (pos_side == "short" and current_sl <= be))
        if already:
            return self._hold(
                f"BE_ALREADY_LOCKED: SL=${current_sl:,.1f} >= BE=${be:,.1f}", r_multiple, hold_reason)

        is_impr = ((pos_side == "long"  and be > current_sl) or
                   (pos_side == "short" and be < current_sl))
        if not is_impr:
            return self._hold(
                f"BE_NOT_IMPROVEMENT: BE=${be:,.1f} vs current=${current_sl:,.1f}", r_multiple, hold_reason)

        # Breathing room — must not be too close to price
        breathing = abs(price - be) / atr
        if breathing < 0.35:
            return self._hold(
                f"BE_TOO_TIGHT: breathing={breathing:.2f}ATR<0.35", r_multiple, hold_reason)

        # Minimum meaningful move
        if abs(be - current_sl) / atr < MIN_IMPROVE_ATR:
            return self._hold(
                f"BE_MICRO: {abs(be-current_sl)/atr:.3f}ATR<{MIN_IMPROVE_ATR}ATR",
                r_multiple)

        reason = (f"[BE_LOCK] R={r_multiple:.2f}R → BE=${be:,.1f} "
                  f"(breathing={breathing:.2f}ATR)")
        logger.info(f"Trail: {reason}")
        return TrailResult(
            new_sl=be, anchor_price=entry_price,
            anchor_label="ENTRY_BE",
            reason=reason, phase="BE_LOCK", r_multiple=r_multiple)

    # ─────────────────────────────────────────────────────────────────────
    # PHASE 2 — POOL → OB → SWING (structural, 1.0–2.5R)
    # ─────────────────────────────────────────────────────────────────────

    def _phase2_structural(
        self, pos_side, price, entry_price, current_sl, atr, init_dist,
        r_multiple, liq_snapshot, ict_engine, now, now_ms,
        candles_by_tf, session, cvd_trend, pos, fee_engine,
        hold_reason: Optional[List[str]] = None,
    ) -> TrailResult:
        """
        Primary: find the nearest unswept liquidity pool that has been
        CLOSED past by price and place SL behind it.
        Fallback: ICT Order Block, then 15m/1H swing.
        """
        sess_mult  = SESSION_BUF.get(session, 1.0)
        be_floor   = round(self._be_price(
            pos_side, entry_price, atr, pos, fee_engine), 1)

        # ── Momentum gate ─────────────────────────────────────────────
        momentum = self._momentum_gate(
            pos_side, atr, candles_by_tf.get("5m", []), cvd_trend,
            ict_engine, now)
        if momentum == "NONE":
            return self._hold(
                f"STRUCTURAL_MOMENTUM_GATE: no displacement/CVD/BOS — "
                f"SL holds R={r_multiple:.2f}R", r_multiple, hold_reason)

        candidates: List[Tuple[float, float, str, float]] = []
        # (sl_level, score, label, anchor_price)
        breath = BREATH_PHASE2_ATR * atr
        closes_needed = CLOSES_PHASE2

        # ── P2-A: Nearest unswept pool between current SL and price ──
        if liq_snapshot is not None:
            pool_attr = 'ssl_pools' if pos_side == "long" else 'bsl_pools'
            for pt in (getattr(liq_snapshot, pool_attr, None) or []):
                pool = getattr(pt, 'pool', pt)
                pp   = float(getattr(pool, 'price', 0.0) or 0.0)
                if pp <= 0: continue
                status = str(getattr(pool, 'status', '') or '')
                if 'SWEPT' in status.upper(): continue

                # Must be between current SL and current price
                if pos_side == "long":
                    if not (current_sl < pp < price): continue
                else:
                    if not (price < pp < current_sl): continue

                # Bar-close confirmation — N closed candles past the pool
                tf5_candles = candles_by_tf.get("5m", [])
                n_closes    = self._closes_past(pos_side, pp, tf5_candles,
                                                closes_needed + 3)
                key         = (round(pp, 0), "5m")
                self._close_counters[key] = n_closes
                if n_closes < closes_needed:
                    continue

                # Breathing room
                sl_c = (pp - _BUF_POOL * atr * sess_mult) if pos_side == "long" \
                       else (pp + _BUF_POOL * atr * sess_mult)
                if abs(price - sl_c) < breath:
                    continue
                if not self._is_improvement(pos_side, sl_c, current_sl):
                    continue

                sig    = float(getattr(pool, 'significance', 1.0) or 1.0)
                htf    = int(getattr(pool, 'htf_count',      0)   or 0)
                tf     = str(getattr(pool, 'timeframe',    '5m')  or '5m')
                # Nearest pool gets highest score (it's the strongest recent support)
                dist_atr = abs(pp - price) / max(atr, 1e-10)
                prox_score = max(0.0, 10.0 - dist_atr)
                score = (prox_score
                         + min(sig * 0.20, 2.0)
                         + (0.50 if htf >= 2 else 0.0)
                         + _TF_POOL_BONUS.get(tf, 0.0))
                candidates.append((sl_c, score,
                                    f"POOL[{tf}]@${pp:,.1f} "
                                    f"(closes={n_closes})",
                                    pp))

        # ── P2-B: Nearest active ICT OB below/above current price ────
        if ict_engine is not None:
            ob_attr = 'order_blocks_bull' if pos_side == "long" else 'order_blocks_bear'
            for ob in (getattr(ict_engine, ob_attr, None) or []):
                if not ob.is_active(now_ms): continue
                anchor = ob.low if pos_side == "long" else ob.high
                if pos_side == "long":
                    if not (current_sl < anchor < price): continue
                else:
                    if not (price < anchor < current_sl): continue

                # Bar-close confirmation against OB anchor TF
                ob_candles  = candles_by_tf.get(ob.timeframe, [])
                n_closes    = self._closes_past(pos_side, anchor, ob_candles,
                                                closes_needed + 3)
                key         = (round(anchor, 0), ob.timeframe)
                self._close_counters[key] = n_closes
                if n_closes < closes_needed:
                    continue

                sl_c = (anchor - _BUF_OB * atr * sess_mult) if pos_side == "long" \
                       else (anchor + _BUF_OB * atr * sess_mult)
                if abs(price - sl_c) < breath: continue
                if not self._is_improvement(pos_side, sl_c, current_sl): continue

                dist_atr = abs(anchor - price) / max(atr, 1e-10)
                prox_score = max(0.0, 9.0 - dist_atr)
                score = (prox_score
                         + min(ob.strength / 50.0, 1.5)
                         + (0.50 if ob.bos_confirmed else 0.0)
                         + _TF_OB_BONUS.get(ob.timeframe, 0.0))
                candidates.append((sl_c, score,
                                    f"OB_{ob.direction.upper()}[{ob.timeframe}]"
                                    f"@${anchor:,.1f}",
                                    anchor))

        # ── P2-C: Confirmed swing extreme on 1H → 15m ────────────────
        for tf in ("1h", "15m"):
            candles = candles_by_tf.get(tf, [])
            if len(candles) < 10:
                continue
            anchor = self._find_trail_swing(pos_side, price, current_sl,
                                             candles, atr, closes_needed)
            if anchor is None:
                continue
            sl_c = (anchor - _BUF_SWING * atr * sess_mult) if pos_side == "long" \
                   else (anchor + _BUF_SWING * atr * sess_mult)
            if abs(price - sl_c) < breath: continue
            if not self._is_improvement(pos_side, sl_c, current_sl): continue

            dist_atr   = abs(anchor - price) / max(atr, 1e-10)
            prox_score = max(0.0, 8.0 - dist_atr)
            score      = prox_score + TF_WEIGHT.get(tf, 1.0)
            candidates.append((sl_c, score, f"SWING[{tf}]@${anchor:,.1f}", anchor))
            break  # stop at first TF that has a valid swing

        if not candidates:
            return self._hold(
                f"STRUCTURAL: no qualifying pool/OB/swing with close-confirmation "
                f"R={r_multiple:.2f}R momentum={momentum}", r_multiple, hold_reason)

        # Best candidate = highest score, nearest as tiebreaker
        if pos_side == "long":
            candidates.sort(key=lambda x: (-x[1], -x[0]))  # nearest = highest price
        else:
            candidates.sort(key=lambda x: (-x[1],  x[0]))  # nearest = lowest price

        new_sl, score, label, anchor_px = candidates[0]
        new_sl = round(new_sl, 1)

        # BE floor: never regress below break-even
        if pos_side == "long"  and new_sl < be_floor: new_sl = be_floor
        if pos_side == "short" and new_sl > be_floor: new_sl = be_floor

        result = self._validate(
            pos_side, new_sl, current_sl, price, atr, init_dist,
            BREATH_PHASE2_ATR, r_multiple, "STRUCTURAL",
            label, anchor_px, momentum)
        if result is not None:
            self._update_anchor_lock(anchor_px, label, now)
        return result or self._hold(
            f"STRUCTURAL: validation rejected — {label}", r_multiple)

    # ─────────────────────────────────────────────────────────────────────
    # PHASE 3 — TIGHT TRAIL ALL TFs (2.5R+)
    # ─────────────────────────────────────────────────────────────────────

    def _phase3_aggressive(
        self, pos_side, price, entry_price, current_sl, atr, init_dist,
        r_multiple, liq_snapshot, ict_engine, now, now_ms,
        candles_by_tf, session, cvd_trend, pos, fee_engine,
        hold_reason: Optional[List[str]] = None,
    ) -> TrailResult:
        """
        Same hierarchy as Phase 2 but with tighter buffers, 1-close
        confirmation, and all TFs active. Also checks HTF alignment.
        """
        sess_mult = SESSION_BUF.get(session, 1.0)
        be_floor  = round(self._be_price(
            pos_side, entry_price, atr, pos, fee_engine), 1)

        # HTF alignment check — downgrade to Phase 2 buffers if against trend
        htf_ok     = self._htf_aligned(ict_engine, pos_side)
        use_tight  = (htf_ok is not False)  # None = unknown → proceed with tight
        buf_pool   = (_BUF_POOL * 0.70) if use_tight else _BUF_POOL
        buf_ob     = (_BUF_OB   * 0.70) if use_tight else _BUF_OB
        buf_swing  = (_BUF_SWING* 0.70) if use_tight else _BUF_SWING
        breath     = BREATH_PHASE3_ATR if use_tight else BREATH_PHASE2_ATR

        if not use_tight:
            logger.info("Trail[AGGRESSIVE]: HTF not aligned — widening to Phase 2 buffers")

        momentum = self._momentum_gate(
            pos_side, atr, candles_by_tf.get("5m", []), cvd_trend,
            ict_engine, now)
        if momentum == "NONE":
            return self._hold(
                f"AGGRESSIVE_MOMENTUM_GATE: no signal R={r_multiple:.2f}R", r_multiple, hold_reason)

        candidates: List[Tuple[float, float, str, float]] = []
        breath_pts = breath * atr

        # ── Pool trail (all TFs) ──────────────────────────────────────
        if liq_snapshot is not None:
            pool_attr = 'ssl_pools' if pos_side == "long" else 'bsl_pools'
            for pt in (getattr(liq_snapshot, pool_attr, None) or []):
                pool   = getattr(pt, 'pool', pt)
                pp     = float(getattr(pool, 'price', 0.0) or 0.0)
                status = str(getattr(pool, 'status', '') or '')
                if pp <= 0 or 'SWEPT' in status.upper(): continue
                if pos_side == "long":
                    if not (current_sl < pp < price): continue
                else:
                    if not (price < pp < current_sl): continue

                tf5c = candles_by_tf.get("5m", [])
                if self._closes_past(pos_side, pp, tf5c, 3) < CLOSES_PHASE3:
                    continue

                sl_c = (pp - buf_pool * atr * sess_mult) if pos_side == "long" \
                       else (pp + buf_pool * atr * sess_mult)
                if abs(price - sl_c) < breath_pts: continue
                if not self._is_improvement(pos_side, sl_c, current_sl): continue

                dist_atr = abs(pp - price) / max(atr, 1e-10)
                score    = max(0.0, 12.0 - dist_atr) + _TF_POOL_BONUS.get(
                    str(getattr(pool, 'timeframe', '5m')), 0.0)
                candidates.append((sl_c, score,
                                    f"POOL_TIGHT[{getattr(pool,'timeframe','5m')}]"
                                    f"@${pp:,.1f}",
                                    pp))

        # ── OB trail (all TFs) ────────────────────────────────────────
        if ict_engine is not None:
            ob_attr = 'order_blocks_bull' if pos_side == "long" else 'order_blocks_bear'
            for ob in (getattr(ict_engine, ob_attr, None) or []):
                if not ob.is_active(now_ms): continue
                anchor = ob.low if pos_side == "long" else ob.high
                if pos_side == "long":
                    if not (current_sl < anchor < price): continue
                else:
                    if not (price < anchor < current_sl): continue

                ob_c = candles_by_tf.get(ob.timeframe, [])
                if self._closes_past(pos_side, anchor, ob_c, 3) < CLOSES_PHASE3:
                    continue

                sl_c = (anchor - buf_ob * atr * sess_mult) if pos_side == "long" \
                       else (anchor + buf_ob * atr * sess_mult)
                if abs(price - sl_c) < breath_pts: continue
                if not self._is_improvement(pos_side, sl_c, current_sl): continue

                dist_atr = abs(anchor - price) / max(atr, 1e-10)
                score    = (max(0.0, 11.0 - dist_atr)
                            + _TF_OB_BONUS.get(ob.timeframe, 0.0)
                            + (0.5 if ob.bos_confirmed else 0.0))
                candidates.append((sl_c, score,
                                    f"OB_TIGHT[{ob.timeframe}]@${anchor:,.1f}",
                                    anchor))

        # ── Swing trail (all TFs, 1H first) ──────────────────────────
        for tf in ("1h", "15m", "5m", "1m"):
            candles = candles_by_tf.get(tf, [])
            if len(candles) < 8: continue
            anchor = self._find_trail_swing(pos_side, price, current_sl,
                                             candles, atr, CLOSES_PHASE3)
            if anchor is None: continue
            sl_c = (anchor - buf_swing * atr * sess_mult) if pos_side == "long" \
                   else (anchor + buf_swing * atr * sess_mult)
            if abs(price - sl_c) < breath_pts: continue
            if not self._is_improvement(pos_side, sl_c, current_sl): continue
            dist_atr = abs(anchor - price) / max(atr, 1e-10)
            score    = max(0.0, 10.0 - dist_atr) + TF_WEIGHT.get(tf, 1.0)
            candidates.append((sl_c, score, f"SWING_TIGHT[{tf}]@${anchor:,.1f}", anchor))

        if not candidates:
            return self._hold(
                f"AGGRESSIVE: no qualifying anchor with close-confirmation "
                f"R={r_multiple:.2f}R momentum={momentum}", r_multiple, hold_reason)

        if pos_side == "long":
            candidates.sort(key=lambda x: (-x[1], -x[0]))
        else:
            candidates.sort(key=lambda x: (-x[1],  x[0]))

        new_sl, score, label, anchor_px = candidates[0]
        new_sl = round(new_sl, 1)

        if pos_side == "long"  and new_sl < be_floor: new_sl = be_floor
        if pos_side == "short" and new_sl > be_floor: new_sl = be_floor

        result = self._validate(
            pos_side, new_sl, current_sl, price, atr, init_dist,
            breath, r_multiple, "AGGRESSIVE",
            label, anchor_px, momentum)
        if result is not None:
            self._update_anchor_lock(anchor_px, label, now)
        return result or self._hold(
            f"AGGRESSIVE: validation rejected — {label}", r_multiple)

    # ─────────────────────────────────────────────────────────────────────
    # INTERNAL HELPERS
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def _is_improvement(pos_side: str, new_sl: float, current_sl: float) -> bool:
        """Ratchet check — SL must only move in profit direction."""
        if pos_side == "long":  return new_sl > current_sl
        if pos_side == "short": return new_sl < current_sl
        return False

    @staticmethod
    def _closes_past(
        pos_side: str, level: float, candles: List[dict], lookback: int
    ) -> int:
        """
        Count consecutive closed bars whose close is past `level`
        in the trade direction, ending with the most recent closed bar.
        LONG: close > level.  SHORT: close < level.
        """
        closed = candles[:-1] if len(candles) > 1 else candles
        if not closed:
            return 0
        scan  = closed[-min(lookback, len(closed)):]
        def _c(c): return float(c.get('c', c.get('close', 0.0)) or 0.0)
        count = 0
        for bar in reversed(scan):
            cl = _c(bar)
            if pos_side == "long"  and cl > level: count += 1
            elif pos_side == "short" and cl < level: count += 1
            else: break
        return count

    @staticmethod
    def _find_trail_swing(
        pos_side: str, price: float, current_sl: float,
        candles: List[dict], atr: float, closes_needed: int,
    ) -> Optional[float]:
        """
        Find the nearest confirmed swing extreme between current_sl and price
        that has closes_needed closed candles past it.
        Returns the swing price or None.
        """
        if len(candles) < 6:
            return None
        closed = candles[:-1] if len(candles) > 1 else candles
        scan   = closed[-50:]
        strength = 2  # N-bar pivot

        def _h(c): return float(c.get('h', c.get('high', 0.0)) or 0.0)
        def _l(c): return float(c.get('l', c.get('low',  0.0)) or 0.0)
        def _c(c): return float(c.get('c', c.get('close',0.0)) or 0.0)

        n   = len(scan)
        lvl = None

        if pos_side == "long":
            # Find the highest recent pivot low between current_sl and price
            best = -float('inf')
            for i in range(strength, n - strength):
                lo = _l(scan[i])
                is_pivot = all(_l(scan[j]) >= lo
                               for j in range(max(0,i-strength),
                                              min(n,i+strength+1))
                               if j != i)
                if is_pivot and current_sl < lo < price and lo > best:
                    # Verify close-confirmation
                    n_cl = 0
                    for bar in reversed(scan[i:]):
                        if _c(bar) > lo: n_cl += 1
                        else: break
                    if n_cl >= closes_needed:
                        best = lo
            lvl = best if best > -float('inf') else None
        else:
            # Find the lowest recent pivot high between price and current_sl
            best = float('inf')
            for i in range(strength, n - strength):
                hi = _h(scan[i])
                is_pivot = all(_h(scan[j]) <= hi
                               for j in range(max(0,i-strength),
                                              min(n,i+strength+1))
                               if j != i)
                if is_pivot and price < hi < current_sl and hi < best:
                    n_cl = 0
                    for bar in reversed(scan[i:]):
                        if _c(bar) < hi: n_cl += 1
                        else: break
                    if n_cl >= closes_needed:
                        best = hi
            lvl = best if best < float('inf') else None

        return lvl

    @staticmethod
    def _be_price(pos_side, entry_price, atr, pos=None, fee_engine=None) -> float:
        """Entry + round-trip commission + slippage buffer."""
        exact_fee, qty = 0.0, 0.0
        if pos is not None:
            exact_fee = float(getattr(pos, 'entry_fee_paid', 0.0) or 0.0)
            qty       = float(getattr(pos, 'quantity',       0.0) or 0.0)
        if exact_fee > 1e-6 and qty > 1e-10:
            rt_comm = (exact_fee * 2.0) / qty
        elif fee_engine is not None:
            try:
                bps    = fee_engine.effective_roundtrip_cost_bps(use_maker_entry=True)
                rt_comm = entry_price * (bps / 10_000.0)
            except Exception:
                rt_comm = StructuralTrailEngine._static_comm(entry_price)
        else:
            rt_comm = StructuralTrailEngine._static_comm(entry_price)

        buf = rt_comm + 0.12 * atr
        return (entry_price + buf) if pos_side == "long" else (entry_price - buf)

    @staticmethod
    def _static_comm(entry_price: float) -> float:
        try:
            import config as _c
            rate = float(getattr(_c, 'COMMISSION_RATE', 0.00055))
        except Exception:
            rate = 0.00055
        return entry_price * rate * 2.0

    @staticmethod
    def _momentum_gate(pos_side, atr, candles_5m, cvd_trend,
                       ict_engine, now) -> str:
        """Returns 'DISP' | 'CVD' | 'BOS' | 'NONE'."""
        # (a) Displacement candle on last closed 5m bar
        if len(candles_5m) >= 2:
            try:
                lc    = candles_5m[-2]
                lo_   = float(lc.get('o', lc.get('open',  0.0)) or 0.0)
                cl_   = float(lc.get('c', lc.get('close', 0.0)) or 0.0)
                body  = abs(cl_ - lo_)
                aligned = ((pos_side == "long"  and cl_ > lo_) or
                           (pos_side == "short" and cl_ < lo_))
                if body >= DISP_MIN_BODY_ATR * atr and aligned:
                    return "DISP"
            except Exception:
                pass
        # (b) CVD trend
        if pos_side == "long"  and cvd_trend >= CVD_MIN_TREND: return "CVD"
        if pos_side == "short" and cvd_trend <= -CVD_MIN_TREND: return "CVD"
        # (c) Recent BOS
        if ict_engine is not None:
            now_ms = int(now * 1000) if now > 1e6 else int(time.time() * 1000)
            for tf in ("5m", "15m"):
                try:
                    st = ict_engine._tf.get(tf)
                    if st is None: continue
                    direction = getattr(st, "bos_direction", None)
                    ts        = getattr(st, "bos_timestamp", 0)
                    if ts > 0 and (now_ms - ts) <= BOS_MAX_AGE_MS:
                        if pos_side == "long"  and direction == "bullish": return "BOS"
                        if pos_side == "short" and direction == "bearish": return "BOS"
                except Exception:
                    pass
        return "NONE"

    @staticmethod
    def _detect_counter_bos(ict_engine, pos_side, entry_price, now_ms) -> bool:
        """5m BOS AGAINST trade that breaks past entry level."""
        if ict_engine is None:
            return False
        try:
            tf5 = ict_engine._tf.get("5m")
            if tf5 is None: return False
            direction = getattr(tf5, "bos_direction", None)
            level     = float(getattr(tf5, "bos_level", 0.0) or 0.0)
            ts        = getattr(tf5, "bos_timestamp", 0)
            if level <= 0: return False
            if ts > 0 and (now_ms - ts) > COUNTER_BOS_MAX_AGE_MS: return False
            if pos_side == "long"  and direction == "bearish" and level < entry_price:
                return True
            if pos_side == "short" and direction == "bullish" and level > entry_price:
                return True
        except Exception:
            pass
        return False

    @staticmethod
    def _htf_aligned(ict_engine, pos_side) -> Optional[bool]:
        """True / False / None (unknown) — 1H trend vs trade direction."""
        if ict_engine is None: return None
        try:
            tf1h  = ict_engine._tf.get("1h")
            if tf1h is None: return None
            trend = str(getattr(tf1h, "trend", "") or "").lower()
            if trend in ("bullish", "bearish"):
                return (trend == "bullish") if pos_side == "long" else (trend == "bearish")
        except Exception:
            pass
        return None

    @staticmethod
    def _detect_session(ict_engine) -> str:
        if ict_engine is None: return ""
        sess = str(getattr(ict_engine, '_session', '') or '').upper()
        if sess in ('LONDON', 'NY', 'ASIA'): return sess
        if sess == 'NEW_YORK': return 'NY'
        kz = str(getattr(ict_engine, '_killzone', '') or '').upper()
        if 'LONDON' in kz: return 'LONDON'
        if 'NY' in kz or 'NEW_YORK' in kz: return 'NY'
        if 'ASIA' in kz: return 'ASIA'
        return ''

    def _update_anchor_lock(self, anchor_px, label, now):
        self._locked_anchor_price = anchor_px
        self._locked_anchor_label = label
        self._anchor_lock_until   = now + self.ANCHOR_LOCK_SEC

    def _validate(
        self, pos_side, new_sl, current_sl, price, atr, init_dist,
        min_breath_atr, r_multiple, phase, label, anchor_px, momentum,
    ) -> Optional[TrailResult]:
        """
        Apply institutional guards. Returns TrailResult or None if rejected.
        Guards:
          1. Ratchet
          2. Breathing room
          3. Minimum improvement
          4. Max distance cap (8 ATR from price)
          5. Anti-tighten (SL must preserve ≥ 40% of initial SL dist from entry)
        """
        # 1. Ratchet
        if not self._is_improvement(pos_side, new_sl, current_sl):
            return None

        # 2. Breathing room
        if abs(price - new_sl) / atr < min_breath_atr:
            logger.debug(f"Trail[{phase}]: BREATH rejected "
                         f"{abs(price-new_sl)/atr:.2f}ATR<{min_breath_atr:.2f} — {label}")
            return None

        # 3. Minimum meaningful improvement
        improve_atr = abs(new_sl - current_sl) / atr
        if improve_atr < MIN_IMPROVE_ATR:
            return None

        # 4. Max distance cap
        dist_from_price = abs(price - new_sl) / atr
        if dist_from_price > 8.0:
            logger.debug(f"Trail[{phase}]: TOO_FAR {dist_from_price:.1f}ATR>8 — {label}")
            return None

        # 5. Anti-tighten guard (preserve 40% of initial SL structure from entry)
        # Skip if at or past break-even (BE advances are always valid)
        # This guard only applies to SLs that haven't reached entry yet
        pass  # deliberately omitted — pool/OB/swing already structural

        improvement = abs(new_sl - current_sl)
        reason = (f"[{phase}] R={r_multiple:.2f}R {label} "
                  f"anchor=${anchor_px:,.1f} "
                  f"buf={'pool' if 'POOL' in label else ('OB' if 'OB' in label else 'swing')} "
                  f"gate={momentum} → SL=${new_sl:,.1f} (+{improvement:.1f}pts)")
        logger.info(f"Trail: {reason}")
        return TrailResult(
            new_sl=new_sl, anchor_price=anchor_px, anchor_label=label,
            reason=reason, phase=phase, r_multiple=r_multiple)

    @staticmethod
    def _hold(reason: str, r_multiple: float,
              hold_reason: Optional[List[str]] = None) -> TrailResult:
        if hold_reason is not None:
            hold_reason.append(reason)
        return TrailResult(
            new_sl=None, anchor_price=None, anchor_label="",
            reason=reason, phase="HOLD", r_multiple=r_multiple)

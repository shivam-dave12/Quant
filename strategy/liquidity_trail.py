"""
liquidity_trail.py — Institutional SMC Trailing Engine v3.0
============================================================
COMPLETE REWRITE — not a patch. Drop-in replacement.

WHY REWRITE:
  The v1 engine had one fatal flaw: it activated at 0.10R and selected the
  CLOSEST pool as anchor — including 1m/5m noise pools 30pts from entry on
  an instrument with $113 ATR.  This is the retail equivalent of moving your
  stop to breakeven on every trade.  Result: 23/24 exits via SL, 67% from
  wick sweeps and noise hits.  Win rate: 33%.

INSTITUTIONAL LOGIC — HOW SMART MONEY ACTUALLY TRAILS:

  Phase 0 — HANDS OFF (0R to 1.0R):
    The initial SL was placed at institutional structure (swept pool wick,
    OB, 15m swing). Smart money DEFENDS this level.  Moving SL before the
    trade proves itself is the #1 retail mistake.  The SL stays exactly
    where the entry engine placed it.  Period.

  Phase 1 — BREAKEVEN LOCK (1.0R to 2.0R):
    The trade has proven direction.  Move SL to breakeven + fees + spread.
    This is the ONLY move in this phase.  No structural trailing yet.
    Institutions lock risk-free status, then let the trade breathe.

  Phase 2 — STRUCTURAL TRAIL (2.0R to 3.5R):
    Now trail to 15m+ liquidity pools only.  No 1m/5m noise.
    Select by SIGNIFICANCE × TF weight, not by proximity.
    A 1h pool at 2 ATR behind is infinitely better than a 5m pool at 0.3 ATR.
    Buffer: significance-weighted, minimum 0.80 ATR.

  Phase 3 — AGGRESSIVE TRAIL (3.5R+):
    Trade is in deep profit.  Trail to 5m+ pools with tighter buffers.
    Lock maximum profit while still respecting structural support/resistance.
    Buffer: significance-weighted, minimum 0.50 ATR.

ANCHOR SELECTION — INSTITUTIONAL PRIORITY:
  1. Swept pools (confirmed S/R) — highest conviction. Smart money defends these.
  2. Unswept pools behind price — stop clusters that price must sweep to reach SL.
  3. Never: 1m pools, sig < 4.0 pools, pools closer than MIN_BREATHING_ATR.

ANTI-OSCILLATION:
  Once an anchor is selected, it is LOCKED for 90 seconds minimum.
  The only way to change anchors within the lock is a significantly better
  pool appearing (1.5× significance threshold).  This prevents the SL from
  bouncing between competing pools on consecutive ticks.

ANTI-TIGHTENING RULE:
  SL can NEVER be placed closer to entry than 50% of the initial SL distance.
  This preserves the structural rationale of the original placement.
  If the trail engine finds a pool that would violate this — it is rejected.

SESSION CONTEXT:
  Asia:   Trailing DISABLED entirely.  Noise-to-signal ratio too high.
  London: Normal trailing.  Manipulative Judas sweeps mean the reversal
          is fast — structural pools form quickly after the fake-out.
  NY:     Wide buffers.  Institutional delivery moves are larger and need
          more room to breathe during the distribution phase.

INTERFACE (drop-in compatible with quant_strategy.py):
  result = engine.compute(pos_side, price, entry_price, current_sl, atr, ...)
  if result.new_sl is not None:
      order_manager.replace_stop_loss(...)
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# PHASE THRESHOLDS — the core institutional discipline
# ═══════════════════════════════════════════════════════════════════════════

# Phase 0: Hands off.  The entry engine placed SL at structure.  Trust it.
PHASE_0_MAX_R = 1.0    # Do NOT trail below this R-multiple

# Phase 1: Breakeven lock only.  No structural trailing.
PHASE_1_MAX_R = 2.0    # BE lock between 1.0R and 2.0R

# Phase 2: Trail to 15m+ HTF pools.  Minimum significance = 5.0.
PHASE_2_MAX_R = 3.5    # Structural trail between 2.0R and 3.5R

# Phase 3: Aggressive trail to 5m+ pools.  Tighter buffers.
# Above 3.5R — maximum profit protection.


# ═══════════════════════════════════════════════════════════════════════════
# ANCHOR QUALITY FILTERS
# ═══════════════════════════════════════════════════════════════════════════

# Timeframe rank — pools below minimum are rejected as anchors
_TF_RANK: Dict[str, int] = {
    "1m": 1, "2m": 1, "3m": 1, "5m": 2, "15m": 3,
    "30m": 3, "1h": 4, "4h": 5, "1d": 6, "?": 0,
}

# Phase 2: Only 15m+ pools (rank >= 3)
PHASE_2_MIN_TF_RANK = 3
PHASE_2_MIN_SIG = 5.0

# Phase 3: 5m+ pools allowed (rank >= 2) but significance still gated
PHASE_3_MIN_TF_RANK = 2
PHASE_3_MIN_SIG = 3.5


# ═══════════════════════════════════════════════════════════════════════════
# BUFFER TABLE — significance → ATR buffer multiplier
# ═══════════════════════════════════════════════════════════════════════════
# Higher significance = institution more committed = tighter buffer.
# These are REALISTIC for BTC volatility.  Old values (0.12-0.90) were
# calibrated for equities.  BTC 5m ATR = $100-150.  A 0.12 ATR buffer
# is $12-18 — inside every single wick.

_SIG_BUFFER_PHASE2: List[Tuple[float, float]] = [
    (12.0, 0.50),   # Multi-TF cluster (1D+4H+1H): 0.50 ATR
    (8.0,  0.65),   # Strong HTF pool (4H/1D): 0.65 ATR
    (5.0,  0.80),   # Moderate 1H pool: 0.80 ATR (minimum for Phase 2)
]

_SIG_BUFFER_PHASE3: List[Tuple[float, float]] = [
    (12.0, 0.35),   # Multi-TF cluster: tighter in profit
    (8.0,  0.50),   # Strong HTF: 0.50 ATR
    (5.0,  0.65),   # Moderate: 0.65 ATR
    (3.5,  0.80),   # Basic 5m+ pool: 0.80 ATR (minimum for Phase 3)
]


# ═══════════════════════════════════════════════════════════════════════════
# BREATHING ROOM & SAFETY
# ═══════════════════════════════════════════════════════════════════════════

# SL must be at least this far from current price (ATR multiples).
# Below this = inside wick noise zone = will be swept on any retracement.
MIN_BREATHING_ATR_PHASE2 = 1.00   # Phase 2: wide — let the trade develop
MIN_BREATHING_ATR_PHASE3 = 0.65   # Phase 3: tighter — locking deep profit

# SL can NEVER be closer to entry than this fraction of initial_sl_dist.
# Protects the structural rationale of the original SL placement.
MIN_SL_PRESERVE_FRACTION = 0.50

# Maximum SL distance from price — cap to avoid absurd outliers
MAX_SL_DIST_ATR = 8.0

# Minimum improvement to trigger a REST API call (prevents micro-updates)
MIN_IMPROVEMENT_ATR = 0.20

# Pool lookback distance behind price
POOL_LOOKBACK_ATR = 10.0

# Swept pool recency — only use sweeps from last 2 hours
SWEPT_POOL_MAX_AGE_SEC = 7200.0


# ═══════════════════════════════════════════════════════════════════════════
# ANTI-OSCILLATION — anchor lock
# ═══════════════════════════════════════════════════════════════════════════

# Once an anchor is selected, lock it for this duration.
# Only override if new anchor has significantly higher quality.
ANCHOR_LOCK_SEC = 90.0

# New anchor must exceed locked anchor's significance by this factor
ANCHOR_OVERRIDE_SIG_MULT = 1.50


# ═══════════════════════════════════════════════════════════════════════════
# SESSION CONFIG
# ═══════════════════════════════════════════════════════════════════════════

SESSION_BUFFER_MULT: Dict[str, float] = {
    "LONDON": 1.00,   # Normal — Judas sweeps create fast structure
    "NY":     1.15,   # Wider — delivery moves need room to breathe
    "ASIA":   1.50,   # Widest — but trailing is disabled anyway
    "":       1.00,
}

# Asia: disable trailing entirely.  Noise kills structural anchoring.
ASIA_TRAIL_DISABLED = True


# ═══════════════════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class PoolAnchor:
    """A liquidity pool selected as SL anchor."""
    price:        float
    side:         str      # "BSL" | "SSL"
    timeframe:    str
    sig:          float    # adjusted significance
    buffer_atr:   float    # sig-derived ATR buffer (phase-specific)
    is_swept:     bool     # True = swept (confirmed S/R), False = unswept
    distance_atr: float    # from current price
    quality:      float    # composite quality score for ranking


@dataclass
class LiquidityTrailResult:
    """Output of the liquidity trail engine."""
    new_sl:        Optional[float]      # None = no improvement (hold current)
    anchor:        Optional[PoolAnchor]
    reason:        str
    phase:         str                  # "HANDS_OFF" | "BE_LOCK" | "STRUCTURAL" | "AGGRESSIVE" | "HOLD"
    trail_blocked: bool = False
    block_reason:  str = ""


# ═══════════════════════════════════════════════════════════════════════════
# CORE ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class LiquidityTrailEngine:
    """
    Institutional SMC Trailing Engine v3.0.

    Drop-in replacement for v1 LiquidityTrailEngine.
    Same interface, completely different logic.
    """

    def __init__(self) -> None:
        # Anti-oscillation state
        self._locked_anchor: Optional[PoolAnchor] = None
        self._anchor_lock_until: float = 0.0
        self._last_phase: str = "HANDS_OFF"

    def reset(self) -> None:
        """
        Reset all inter-position state.

        Bug #23 fix: _locked_anchor and _anchor_lock_until were never cleared
        between positions. If position A closed while anchored to a 15m SSL at
        $66,800 and position B opened within 90 seconds, the engine would re-use
        the stale anchor from A, potentially placing SL at a structurally
        irrelevant level for B's direction and range.

        Call this from _finalise_exit() immediately after the old PositionState
        is replaced, BEFORE the new entry evaluation begins.
        """
        self._locked_anchor    = None
        self._anchor_lock_until = 0.0
        self._last_phase        = "HANDS_OFF"
        logger.debug("LiquidityTrailEngine: state reset for new position")

    def compute(
        self,
        pos_side:        str,
        price:           float,
        entry_price:     float,
        current_sl:      float,
        atr:             float,
        initial_sl_dist: float  = 0.0,
        peak_profit:     float  = 0.0,
        liq_snapshot             = None,
        ict_engine               = None,
        now:             float  = 0.0,
        hold_reason:     Optional[List[str]] = None,
        pos                      = None,   # Bug #14: PositionState for exact fee
    ) -> LiquidityTrailResult:
        """
        Compute the next SL based on institutional phase-based logic.

        Returns LiquidityTrailResult:
          new_sl = float  → place this SL (always an improvement over current)
          new_sl = None   → hold current SL (no valid improvement found)
        """
        now_ = now if now > 1e6 else time.time()

        if atr < 1e-10:
            return self._hold("atr_zero", hold_reason)

        # ── Session gate ───────────────────────────────────────────────────
        session = self._detect_session(ict_engine)
        if ASIA_TRAIL_DISABLED and session == "ASIA":
            return self._blocked("ASIA_SESSION_DISABLED", hold_reason)

        sess_mult = SESSION_BUFFER_MULT.get(session, 1.0)

        # ── Compute profit metrics ─────────────────────────────────────────
        init_dist = max(
            initial_sl_dist if initial_sl_dist > 1e-10 else 0.0,
            abs(entry_price - current_sl),
            atr * 0.5,   # absolute floor — prevents div-by-zero on tiny init_dist
        )
        profit = (price - entry_price) if pos_side == "long" else (entry_price - price)
        r_multiple = max(profit, peak_profit) / init_dist if init_dist > 1e-10 else 0.0

        # ══════════════════════════════════════════════════════════════════
        # PHASE 0 — HANDS OFF (< 1.0R)
        # The entry SL was placed at institutional structure.  Trust it.
        # ══════════════════════════════════════════════════════════════════
        if r_multiple < PHASE_0_MAX_R:
            self._last_phase = "HANDS_OFF"
            return self._hold(
                f"PHASE_0_HANDS_OFF: R={r_multiple:.2f} < {PHASE_0_MAX_R}R — "
                f"initial SL at structure is optimal, no trailing",
                hold_reason)

        # ══════════════════════════════════════════════════════════════════
        # PHASE 1 — BREAKEVEN LOCK (1.0R to 2.0R)
        # Move to breakeven + fees + slippage.  Nothing else.
        # ══════════════════════════════════════════════════════════════════
        if r_multiple < PHASE_1_MAX_R:
            self._last_phase = "BE_LOCK"
            return self._try_be_lock(
                pos_side, price, entry_price, current_sl, atr, init_dist,
                r_multiple, hold_reason, pos=pos)

        # ══════════════════════════════════════════════════════════════════
        # PHASE 2 — STRUCTURAL TRAIL (2.0R to 3.5R)
        # Trail to 15m+ pools only.  Wide buffers.  Let the trade breathe.
        # ══════════════════════════════════════════════════════════════════
        if r_multiple < PHASE_2_MAX_R:
            self._last_phase = "STRUCTURAL"
            return self._structural_trail(
                pos_side, price, entry_price, current_sl, atr, init_dist,
                r_multiple, liq_snapshot, ict_engine, now_, sess_mult,
                min_tf_rank=PHASE_2_MIN_TF_RANK,
                min_sig=PHASE_2_MIN_SIG,
                buffer_table=_SIG_BUFFER_PHASE2,
                min_breathing=MIN_BREATHING_ATR_PHASE2,
                phase_name="STRUCTURAL",
                hold_reason=hold_reason)

        # ══════════════════════════════════════════════════════════════════
        # PHASE 3 — AGGRESSIVE TRAIL (3.5R+)
        # Trail to 5m+ pools.  Tighter buffers.  Lock deep profit.
        # ══════════════════════════════════════════════════════════════════
        self._last_phase = "AGGRESSIVE"
        return self._structural_trail(
            pos_side, price, entry_price, current_sl, atr, init_dist,
            r_multiple, liq_snapshot, ict_engine, now_, sess_mult,
            min_tf_rank=PHASE_3_MIN_TF_RANK,
            min_sig=PHASE_3_MIN_SIG,
            buffer_table=_SIG_BUFFER_PHASE3,
            min_breathing=MIN_BREATHING_ATR_PHASE3,
            phase_name="AGGRESSIVE",
            hold_reason=hold_reason)

    # ─────────────────────────────────────────────────────────────────────
    # PHASE 1: BREAKEVEN LOCK
    # ─────────────────────────────────────────────────────────────────────

    def _try_be_lock(
        self, pos_side: str, price: float, entry_price: float,
        current_sl: float, atr: float, init_dist: float,
        r_multiple: float, hold_reason: Optional[List[str]],
        pos=None,
    ) -> LiquidityTrailResult:
        """
        Move SL to breakeven + round-trip fees + slippage buffer.
        Only one move in this phase — once BE is locked, hold.

        Bug #14 fix: previously used a hardcoded 0.055% taker fee, silently
        ignoring config.COMMISSION_RATE and Delta's maker rebate.  Now:
          1. If pos.entry_fee_paid is available (Delta v8.1+ exact fill), derive
             the per-unit fee from it — this captures the actual maker/taker rate.
          2. Otherwise fall back to config.COMMISSION_RATE (bilateral estimate).
        This matches the behaviour of the centralised _calc_be_price() in
        quant_strategy.py, which was specifically written to fix this pattern.
        """
        # ── Fee per unit (exact or estimated) ─────────────────────────────
        _exact_fee = 0.0
        _qty       = 0.0
        if pos is not None:
            _exact_fee = float(getattr(pos, 'entry_fee_paid', 0.0) or 0.0)
            _qty       = float(getattr(pos, 'quantity',       0.0) or 0.0)

        if _exact_fee > 1e-6 and _qty > 1e-10:
            # Exact entry fee from exchange fill; estimate exit at same rate.
            _entry_fee_per_unit = _exact_fee / _qty
            _exit_fee_rate      = _entry_fee_per_unit / max(entry_price, 1.0)
            commission_rt       = _entry_fee_per_unit + entry_price * _exit_fee_rate
        else:
            # Config-driven bilateral estimate (respects COMMISSION_RATE overrides).
            try:
                import config as _cfg
                _rate = float(getattr(_cfg, 'COMMISSION_RATE', 0.00055))
            except Exception:
                _rate = 0.00055
            commission_rt = entry_price * _rate * 2.0

        slippage_buf  = 0.12 * atr
        total_buffer  = commission_rt + slippage_buf

        if pos_side == "long":
            be_price = entry_price + total_buffer
        else:
            be_price = entry_price - total_buffer

        # Round to tick (0.1 for BTC)
        be_price = round(be_price, 1)

        # Check if current SL is already at or beyond BE
        already_locked = (
            (pos_side == "long"  and current_sl >= be_price) or
            (pos_side == "short" and current_sl <= be_price)
        )
        if already_locked:
            return self._hold(
                f"BE_ALREADY_LOCKED: SL=${current_sl:,.1f} >= BE=${be_price:,.1f}",
                hold_reason)

        # Check improvement direction
        is_improvement = (
            (pos_side == "long"  and be_price > current_sl) or
            (pos_side == "short" and be_price < current_sl)
        )
        if not is_improvement:
            return self._hold(
                f"BE_NOT_IMPROVEMENT: BE=${be_price:,.1f} vs SL=${current_sl:,.1f}",
                hold_reason)

        # Check breathing room
        breathing = abs(price - be_price) / atr
        if breathing < 0.40:
            return self._hold(
                f"BE_TOO_TIGHT: breathing={breathing:.2f}ATR < 0.40ATR",
                hold_reason)

        # Minimum improvement gate
        improvement_atr = abs(be_price - current_sl) / atr
        if improvement_atr < MIN_IMPROVEMENT_ATR:
            return self._hold(
                f"BE_MICRO_MOVE: {improvement_atr:.3f}ATR < {MIN_IMPROVEMENT_ATR}ATR",
                hold_reason)

        reason = (
            f"[BE_LOCK] R={r_multiple:.2f}R → BE=${be_price:,.1f} "
            f"(fees=${commission_rt:.2f} + slip=${slippage_buf:.1f})"
        )
        logger.info(f"Trail: {reason}")

        return LiquidityTrailResult(
            new_sl=be_price, anchor=None, reason=reason, phase="BE_LOCK")

    # ─────────────────────────────────────────────────────────────────────
    # PHASE 2 & 3: STRUCTURAL TRAIL
    # ─────────────────────────────────────────────────────────────────────

    def _structural_trail(
        self, pos_side: str, price: float, entry_price: float,
        current_sl: float, atr: float, init_dist: float,
        r_multiple: float, liq_snapshot, ict_engine, now: float,
        sess_mult: float, min_tf_rank: int, min_sig: float,
        buffer_table: List[Tuple[float, float]], min_breathing: float,
        phase_name: str, hold_reason: Optional[List[str]],
    ) -> LiquidityTrailResult:
        """
        Trail SL to liquidity pool anchors with institutional filters.
        """
        if liq_snapshot is None:
            return self._hold(f"{phase_name}: no_liq_snapshot", hold_reason)

        # ── Collect candidates ─────────────────────────────────────────────
        candidates = self._collect_pool_candidates(
            pos_side, price, atr, liq_snapshot, ict_engine, now,
            min_tf_rank, min_sig)

        if not candidates:
            return self._hold(
                f"{phase_name}: no qualifying pools "
                f"(min_tf_rank={min_tf_rank} min_sig={min_sig})",
                hold_reason)

        # ── Select best anchor ─────────────────────────────────────────────
        best = self._select_best_anchor(candidates, pos_side)

        # ── Anti-oscillation: check anchor lock ────────────────────────────
        if self._locked_anchor is not None and now < self._anchor_lock_until:
            # Only override if new anchor is significantly better
            if best.quality <= self._locked_anchor.quality * ANCHOR_OVERRIDE_SIG_MULT:
                # Use locked anchor instead
                locked_still_valid = any(
                    abs(c.price - self._locked_anchor.price) < atr * 0.05
                    for c in candidates
                )
                if locked_still_valid:
                    best = self._locked_anchor
                # else: locked anchor no longer exists in snapshot — use new best

        # ── Compute SL from anchor ─────────────────────────────────────────
        buf = self._sig_to_buffer(best.sig, buffer_table) * atr * sess_mult
        if pos_side == "long":
            new_sl = best.price - buf
        else:
            new_sl = best.price + buf

        new_sl = round(new_sl, 1)

        # ── Validate ───────────────────────────────────────────────────────
        result = self._validate_sl(
            pos_side, new_sl, current_sl, price, entry_price, atr,
            init_dist, min_breathing, best, phase_name, hold_reason)

        if result is not None:
            # Lock this anchor
            self._locked_anchor = best
            self._anchor_lock_until = now + ANCHOR_LOCK_SEC
            return result

        return self._hold(
            f"{phase_name}: validation failed for "
            f"{best.side}@${best.price:,.0f}({best.timeframe})",
            hold_reason)

    # ─────────────────────────────────────────────────────────────────────
    # POOL COLLECTION — the institutional filter pipeline
    # ─────────────────────────────────────────────────────────────────────

    def _collect_pool_candidates(
        self, pos_side: str, price: float, atr: float,
        liq_snapshot, ict_engine, now: float,
        min_tf_rank: int, min_sig: float,
    ) -> List[PoolAnchor]:
        """
        Collect all qualifying pool anchors from the liquidity snapshot.

        Filters:
          1. Must be BEHIND price (below for long, above for short)
          2. Must meet minimum timeframe rank
          3. Must meet minimum significance
          4. Must be within POOL_LOOKBACK_ATR distance
          5. Swept pools: must be recent enough (< SWEPT_POOL_MAX_AGE_SEC)
        """
        candidates: List[PoolAnchor] = []

        # Determine which pool lists to scan
        if pos_side == "long":
            pool_list = getattr(liq_snapshot, 'ssl_pools', [])
        else:
            pool_list = getattr(liq_snapshot, 'bsl_pools', [])

        for pt in pool_list:
            pool = pt.pool

            # Extract pool attributes
            pool_price = float(getattr(pool, 'price', 0.0))
            if pool_price <= 0:
                continue

            pool_tf = str(getattr(pool, 'timeframe', '?') or '?')
            pool_status = str(getattr(pool, 'status', '')).upper()
            is_swept = ('SWEPT' in pool_status or 'CONSUMED' in pool_status)

            # ── Filter 1: Must be BEHIND price ─────────────────────────────
            if pos_side == "long" and pool_price >= price:
                continue
            if pos_side == "short" and pool_price <= price:
                continue

            # ── Filter 2: Timeframe rank ───────────────────────────────────
            tf_rank = _TF_RANK.get(pool_tf, 0)
            if tf_rank < min_tf_rank:
                continue

            # ── Filter 3: Significance ─────────────────────────────────────
            sig = self._get_pool_sig(pt, pool)
            if sig < min_sig:
                continue

            # ── Filter 4: Distance ─────────────────────────────────────────
            dist_atr = abs(pool_price - price) / atr
            if dist_atr > POOL_LOOKBACK_ATR:
                continue

            # ── Filter 5: Swept pool recency ───────────────────────────────
            if is_swept:
                swept_at = float(getattr(pool, 'swept_at', 0.0) or 0.0)
                if swept_at > 0 and (now - swept_at) > SWEPT_POOL_MAX_AGE_SEC:
                    continue

            # ── Compute quality score ──────────────────────────────────────
            # Quality = significance × TF weight × swept bonus × freshness
            tf_weight = min(tf_rank / 4.0, 1.5)
            swept_bonus = 1.40 if is_swept else 1.00   # Swept = confirmed S/R
            freshness = 1.0
            if is_swept:
                swept_at = float(getattr(pool, 'swept_at', 0.0) or 0.0)
                if swept_at > 0:
                    age_min = (now - swept_at) / 60.0
                    freshness = max(0.50, 1.0 - age_min / 120.0)

            quality = sig * tf_weight * swept_bonus * freshness

            candidates.append(PoolAnchor(
                price=pool_price,
                side="SSL" if pos_side == "long" else "BSL",
                timeframe=pool_tf,
                sig=sig,
                buffer_atr=0.0,   # computed later from buffer table
                is_swept=is_swept,
                distance_atr=round(dist_atr, 2),
                quality=round(quality, 3),
            ))

        # ── Also check swept history (simpler records without full metadata) ─
        if pos_side == "long":
            swept_levels = getattr(liq_snapshot, 'swept_ssl_levels', [])
        else:
            swept_levels = getattr(liq_snapshot, 'swept_bsl_levels', [])

        for sp in (swept_levels or []):
            sp_price = float(sp)
            dist_atr = abs(sp_price - price) / atr
            if dist_atr > POOL_LOOKBACK_ATR:
                continue
            if pos_side == "long" and sp_price >= price:
                continue
            if pos_side == "short" and sp_price <= price:
                continue
            # Skip if already captured from pool_list
            if any(abs(c.price - sp_price) < atr * 0.05 for c in candidates):
                continue
            # History entries get moderate quality (no TF/sig metadata)
            candidates.append(PoolAnchor(
                price=sp_price,
                side="SSL" if pos_side == "long" else "BSL",
                timeframe="?",
                sig=3.0,
                buffer_atr=0.0,
                is_swept=True,
                distance_atr=round(dist_atr, 2),
                quality=3.0 * 1.40,  # swept bonus applied
            ))

        return candidates

    # ─────────────────────────────────────────────────────────────────────
    # ANCHOR SELECTION — by quality, not by proximity
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def _select_best_anchor(
        candidates: List[PoolAnchor], pos_side: str,
    ) -> PoolAnchor:
        """
        Select the best anchor from candidates.

        INSTITUTIONAL LOGIC: select by quality score (sig × TF × swept × freshness),
        NOT by proximity.  A 1h pool 2 ATR behind price is infinitely better than
        a 5m pool 0.3 ATR behind.

        For tied quality, prefer the one closest to price (tightest SL).
        """
        # Sort by quality descending, then by proximity ascending (tiebreaker)
        if pos_side == "long":
            candidates.sort(key=lambda a: (-a.quality, -a.price))
        else:
            candidates.sort(key=lambda a: (-a.quality, a.price))

        return candidates[0]

    # ─────────────────────────────────────────────────────────────────────
    # VALIDATION — institutional guards
    # ─────────────────────────────────────────────────────────────────────

    def _validate_sl(
        self, pos_side: str, new_sl: float, current_sl: float,
        price: float, entry_price: float, atr: float, init_dist: float,
        min_breathing: float, anchor: PoolAnchor, phase: str,
        hold_reason: Optional[List[str]],
    ) -> Optional[LiquidityTrailResult]:
        """
        Apply all institutional guards before accepting a new SL.
        Returns LiquidityTrailResult if valid, None if rejected.
        """
        # ── Guard 1: Must be an improvement (ratchet — SL only moves favorably) ─
        if pos_side == "long" and new_sl <= current_sl:
            return None
        if pos_side == "short" and new_sl >= current_sl:
            return None

        # ── Guard 2: Breathing room ────────────────────────────────────────
        dist_to_price_atr = abs(price - new_sl) / atr
        if dist_to_price_atr < min_breathing:
            if hold_reason is not None:
                hold_reason.append(
                    f"BREATHING: {dist_to_price_atr:.2f}ATR < {min_breathing}ATR "
                    f"from {anchor.timeframe}@${anchor.price:,.0f}")
            return None

        # ── Guard 3: Anti-tightening — preserve initial SL structure ───────
        sl_dist_from_entry = abs(new_sl - entry_price)
        min_preserve = init_dist * MIN_SL_PRESERVE_FRACTION
        if sl_dist_from_entry < min_preserve:
            if hold_reason is not None:
                hold_reason.append(
                    f"ANTI_TIGHTEN: dist_from_entry={sl_dist_from_entry:.0f} < "
                    f"{min_preserve:.0f} ({MIN_SL_PRESERVE_FRACTION:.0%} of initial {init_dist:.0f})")
            return None

        # ── Guard 4: Maximum distance cap ──────────────────────────────────
        if dist_to_price_atr > MAX_SL_DIST_ATR:
            if hold_reason is not None:
                hold_reason.append(
                    f"TOO_FAR: {dist_to_price_atr:.1f}ATR > {MAX_SL_DIST_ATR}ATR")
            return None

        # ── Guard 5: Minimum meaningful improvement ────────────────────────
        improvement_atr = abs(new_sl - current_sl) / atr
        if improvement_atr < MIN_IMPROVEMENT_ATR:
            if hold_reason is not None:
                hold_reason.append(
                    f"MICRO_MOVE: {improvement_atr:.3f}ATR < {MIN_IMPROVEMENT_ATR}ATR")
            return None

        # ── All guards passed ──────────────────────────────────────────────
        sweep_tag = "swept" if anchor.is_swept else "unswept"
        improvement_pts = abs(new_sl - current_sl)

        reason = (
            f"[{phase}] {sweep_tag} {anchor.side}@${anchor.price:,.0f} "
            f"({anchor.timeframe} sig={anchor.sig:.1f} q={anchor.quality:.1f}) "
            f"buf={anchor.buffer_atr:.2f}ATR → SL=${new_sl:,.1f} "
            f"(+{improvement_pts:.1f}pts)"
        )
        logger.info(f"Trail: {reason}")

        return LiquidityTrailResult(
            new_sl=new_sl, anchor=anchor, reason=reason, phase=phase)

    # ─────────────────────────────────────────────────────────────────────
    # UTILITY METHODS
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def _sig_to_buffer(sig: float, table: List[Tuple[float, float]]) -> float:
        """Map significance to ATR buffer using the given breakpoint table."""
        for threshold, buffer in table:
            if sig >= threshold:
                return buffer
        return table[-1][1] if table else 1.0

    @staticmethod
    def _get_pool_sig(pt, pool) -> float:
        """Extract significance from a PoolTarget or raw pool object."""
        sig = 0.0
        for attr in ('adjusted_sig', 'significance', 'sig'):
            _v = getattr(pt, attr, None) or getattr(pool, attr, None)
            if _v is not None:
                try:
                    sig = float(_v() if callable(_v) else _v)
                    if sig > 0:
                        return sig
                except Exception:
                    pass
        # Fallback: estimate from touches
        touches = int(getattr(pool, 'touches', 0) or 0)
        return max(1.0, 2.0 + touches * 0.5)

    @staticmethod
    def _detect_session(ict_engine) -> str:
        """
        Detect trading session from ICT engine.

        MOD-6/CRIT-3 FIX: Previously read only _killzone, which is empty
        outside kill-zone hours even when the session is active. A trade
        taken at 04:30 NY during London session (before the 07:00 KZ opens)
        would get sess_mult=1.0 (neutral) instead of the tighter London mult.

        Fix: prefer ict._session (canonical, full-session-window string set
        by _update_session via the now DST-correct zoneinfo path) over _killzone.
        """
        if ict_engine is None:
            return ""
        # Priority 1: canonical session (active the whole session window)
        _sess = str(getattr(ict_engine, '_session', '') or '').upper()
        if _sess in ('LONDON', 'NY', 'NEW_YORK', 'ASIA', 'LONDON_NY', 'OFF_HOURS', 'WEEKEND'):
            if _sess in ('NEW_YORK',):
                return 'NY'
            if _sess in ('OFF_HOURS', 'WEEKEND'):
                return ''
            return _sess
        # Priority 2: killzone string (fallback when _session not available)
        kz = str(getattr(ict_engine, '_killzone', '') or '').upper()
        if 'LONDON' in kz:
            return 'LONDON'
        if 'NY' in kz or 'NEW_YORK' in kz:
            return 'NY'
        if 'ASIA' in kz:
            return 'ASIA'
        return ""

    @staticmethod
    def _hold(reason: str, hold_reason: Optional[List[str]]) -> LiquidityTrailResult:
        if hold_reason is not None:
            hold_reason.append(reason)
        return LiquidityTrailResult(
            new_sl=None, anchor=None, reason=reason, phase="HOLD")

    @staticmethod
    def _blocked(reason: str, hold_reason: Optional[List[str]]) -> LiquidityTrailResult:
        if hold_reason is not None:
            hold_reason.append(f"BLOCKED:{reason}")
        return LiquidityTrailResult(
            new_sl=None, anchor=None, reason=reason,
            phase="HOLD", trail_blocked=True, block_reason=reason)

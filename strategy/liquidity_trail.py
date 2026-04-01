"""
liquidity_trail.py — Institutional Liquidity-Only Trailing SL Engine
=====================================================================
ISSUE-3 FIX: SL trailing should be anchored exclusively to liquidity structure,
using SMC (Smart Money Concepts) — no fixed ratchets, no ATR percentages.

THE PHILOSOPHY (WHY LIQUIDITY-ONLY TRAILING):
  In ICT/SMC, price ALWAYS moves from one liquidity pool to another. Smart money:
    1. Sweeps a pool (stop hunt)
    2. Delivers to the opposing pool (the real move)
    3. Pauses at structure to accumulate/distribute again
    4. Sweeps the NEXT pool

  Your SL should NEVER be in "dead air" between pools — that is where you get
  stopped out by institutional noise. Your SL should be:
    • Just beyond the LAST SWEPT pool (which is now confirmed support/resistance)
    • OR behind the nearest UNSWEPT pool in the direction opposite to your trade
      (price would have to sweep that pool to reach your SL — that is an
       institutional move, not random noise)

  This approach has THREE key advantages:
    1. SL is always at a structural level — smart money defends these
    2. SL moves ONLY when a new pool forms or is swept — not on every tick
    3. The trail "tightens" automatically as price consumes pools en route to TP

SMC TRAIL LOGIC:

  LONG POSITION:
    After an SSL sweep, price delivers UP. SL anchors:

    PHASE 1 (initial — pre-BE):
      SL = just below the SWEPT SSL pool (now confirmed support, institutions
           re-entered here, they will defend it).
      Buffer: 0 — the swept level is the literal entry zone for institutions.
      But add 1 × tick for slippage avoidance.

    PHASE 2 (in profit, as price moves up):
      As price sweeps INTERNAL SSL pools on the way up (creating new confirmed
      support zones), SL advances to just below each newly swept SSL.
      Rule: SL = last_swept_ssl_price - micro_buffer
      micro_buffer = significance-weighted: higher significance swept pool →
                     tighter buffer (institution defended more aggressively).

    PHASE 3 (approaching TP / final pool):
      SL = highest swept SSL price below price - micro_buffer
      (maximally tightened — we're at the destination pool)

  SHORT POSITION:
    Mirror logic: SL = just above last swept BSL (confirmed resistance).

SWEPT POOL BECOMES SUPPORT/RESISTANCE:
  After a pool is swept, the institution that accumulated there is now LONG
  (for SSL sweeps). They defend that level — price returns to it = reversal.
  Your SL just below = you exit at the exact same moment institutions defend
  the level. This is structural alignment with smart money.

POOL SIGNIFICANCE → BUFFER SIZE:
  Higher significance pool → tighter buffer (institution more committed).
  sig 1–3 (weak 5m pool):     buffer = 0.8 × ATR
  sig 3–6 (moderate):         buffer = 0.5 × ATR
  sig 6–10 (strong, HTF conf): buffer = 0.3 × ATR
  sig 10+  (1D / 4H cluster):  buffer = 0.15 × ATR
  (These replace ALL fixed ratchet logic)

UNSWEPT POOL ANCHOR (when no swept pools behind price):
  If there are no swept pools behind price yet (early in move), anchor SL
  to the nearest UNSWEPT pool BEHIND price in the opposite direction.
  This is the institutional "stop cluster" — price reaching it means the
  move is likely over.

  Long: SL = nearest_unswept_SSL_behind_price - sig_buffer
  Short: SL = nearest_unswept_BSL_behind_price + sig_buffer

SESSION CONTEXT (SMC precision):
  London open: sweeps are often manipulative (Judas swing). Trail MORE
    aggressively (tighter buffer after sweep) because the reversal is quick.
  NY open: institutional delivery — trail moderately, let the move breathe.
  Asia: ranging — do NOT trail at all (signal noise too high).
"""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

# Significance → ATR buffer mapping.
# SL is placed this many ATR beyond the pool price.
# Tighter buffer = higher significance = institution more committed to defense.
_SIG_BUFFER_BREAKPOINTS: List[Tuple[float, float]] = [
    (12.0, 0.12),   # very high sig (multi-TF cluster): extremely tight
    (8.0,  0.20),   # 1D or strong 4H pool
    (5.0,  0.32),   # 1H + OB aligned
    (3.0,  0.50),   # moderate 15m/1h pool
    (1.5,  0.70),   # basic 5m pool
    (0.0,  0.90),   # weak / newly-detected
]

# Minimum breathing room: SL cannot be closer than this to current price.
# Prevents tight-SL stop-outs during normal liquidity sweeps en route to TP.
_MIN_BREATHING_ATR = 0.30

# Maximum SL distance behind price (ATR).
# If the best structural pool is further than this, we use the cap.
_MAX_SL_DIST_ATR = 6.0

# Activation threshold: trail does NOT begin until this R-multiple is reached.
# Set to 0.10 (10% of initial SL distance) — very early, just confirms direction.
_TRAIL_ACTIVATION_R = 0.10

# Minimum meaningful improvement: SL must advance by at least this fraction
# of ATR to trigger a REST call. Prevents micro-amendments from wasting API quota.
_MIN_IMPROVEMENT_ATR = 0.15

# Pool lookback distance: search for anchor pools within this many ATR behind price.
_POOL_LOOKBACK_ATR = 8.0

# Swept pool recency: only use swept pools from the last N seconds as anchors.
# Older sweeps may no longer be valid structural support/resistance.
_SWEPT_POOL_MAX_AGE_SEC = 7200.0   # 2 hours

# Session buffer multipliers (applied to all sig_buffer calculations)
_SESSION_BUFFER_MULT: Dict[str, float] = {
    "LONDON": 0.75,   # London: tighter — Judas sweeps are quick, trade fast
    "NY":     1.00,   # NY: standard institutional delivery
    "ASIA":   1.40,   # Asia: wider — ranging session, more noise
    "":       1.00,
}

# Asia session: disable trailing entirely (too noisy for structural anchoring).
_ASIA_TRAIL_DISABLED = True


# ─────────────────────────────────────────────────────────────────────────────
# DATA STRUCTURES
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class PoolAnchor:
    """A liquidity pool used as SL anchor."""
    price:      float
    side:       str      # "BSL" | "SSL"
    timeframe:  str
    sig:        float    # adjusted significance
    buffer_atr: float    # sig-derived ATR buffer
    is_swept:   bool     # True = swept (confirmed S/R), False = unswept
    distance_atr: float  # from current price


@dataclass
class LiquidityTrailResult:
    """Output of the liquidity trail engine."""
    new_sl:         Optional[float]   # None = no improvement (hold current)
    anchor:         Optional[PoolAnchor]
    reason:         str
    phase:          str               # "SWEPT_POOL" | "UNSWEPT_POOL" | "HOLD"
    trail_blocked:  bool = False      # True = trail blocked (Asia / no profit)
    block_reason:   str = ""


# ─────────────────────────────────────────────────────────────────────────────
# CORE ENGINE
# ─────────────────────────────────────────────────────────────────────────────

class LiquidityTrailEngine:
    """
    Institutional Liquidity-Only SL Trailing Engine.

    drop-in replacement for _DynamicStructureTrail._update_trailing_sl() in
    quant_strategy.py. Returns new SL price or None (hold current).

    WIRING IN quant_strategy.py:
    ─────────────────────────────
    In QuantStrategy.__init__():
        from liquidity_trail import LiquidityTrailEngine
        self._liq_trail = LiquidityTrailEngine()

    In _update_trailing_sl() (wherever the current trail engine is called):
        result = self._liq_trail.compute(
            pos_side     = pos.side,
            price        = price,
            entry_price  = pos.entry_price,
            current_sl   = pos.sl_price,
            atr          = atr,
            initial_sl_dist = pos.initial_sl_dist,
            peak_profit  = pos.peak_profit,
            liq_snapshot = liq_snapshot,   # current tick's snapshot
            ict_engine   = self._ict,
            now          = time.time(),
        )
        if result.new_sl is not None:
            # place new SL limit order at result.new_sl
            ...

    The old compute_trail_sl() / _DynamicStructureTrail calls can be replaced
    entirely, OR this engine can run in parallel and take priority when
    liq_snapshot is available (preferred migration strategy).
    """

    def compute(
        self,
        pos_side:       str,           # "long" | "short"
        price:          float,
        entry_price:    float,
        current_sl:     float,
        atr:            float,
        initial_sl_dist: float  = 0.0,
        peak_profit:    float   = 0.0,
        liq_snapshot            = None,   # LiquidityMapSnapshot
        ict_engine              = None,   # ICTEngine (for session + OBs)
        now:            float   = 0.0,
        hold_reason:    Optional[List[str]] = None,
    ) -> LiquidityTrailResult:
        """
        Compute the next SL price based on liquidity structure only.

        Returns LiquidityTrailResult where new_sl is:
          - float: new SL price to place (always an IMPROVEMENT over current)
          - None:  no valid improvement found — hold current SL
        """
        now_ = now if now > 1e6 else time.time()

        if atr < 1e-10:
            return self._hold("atr=0", hold_reason)

        # ── Session check ──────────────────────────────────────────────────────
        session = self._detect_session(ict_engine)
        if _ASIA_TRAIL_DISABLED and session == "ASIA":
            return self._blocked("ASIA_SESSION", hold_reason)

        sess_mult = _SESSION_BUFFER_MULT.get(session, 1.0)

        # ── Profit metrics ─────────────────────────────────────────────────────
        init_dist = max(
            initial_sl_dist if initial_sl_dist > 1e-10 else 0.0,
            abs(entry_price - current_sl),
            atr * 0.1,
        )
        profit = (price - entry_price) if pos_side == "long" else (entry_price - price)
        tier   = max(profit, peak_profit) / init_dist if init_dist > 1e-10 else 0.0

        # ── Activation gate ────────────────────────────────────────────────────
        if tier < _TRAIL_ACTIVATION_R:
            msg = f"TIER={tier:.3f}R < {_TRAIL_ACTIVATION_R}R (not activated)"
            return self._hold(msg, hold_reason)

        # ── Get pool snapshot ──────────────────────────────────────────────────
        if liq_snapshot is None:
            return self._hold("no_liq_snapshot", hold_reason)

        # ══════════════════════════════════════════════════════════════════════
        # PHASE 1: Try swept-pool anchor (highest conviction)
        # ══════════════════════════════════════════════════════════════════════
        swept_anchor = self._find_swept_anchor(
            pos_side     = pos_side,
            price        = price,
            current_sl   = current_sl,
            atr          = atr,
            liq_snapshot = liq_snapshot,
            now          = now_,
            sess_mult    = sess_mult,
        )

        if swept_anchor is not None:
            new_sl = self._compute_sl_from_anchor(
                pos_side = pos_side,
                anchor   = swept_anchor,
                atr      = atr,
                sess_mult= sess_mult,
            )
            result = self._validate_and_return(
                pos_side    = pos_side,
                new_sl      = new_sl,
                current_sl  = current_sl,
                price       = price,
                atr         = atr,
                anchor      = swept_anchor,
                phase       = "SWEPT_POOL",
                hold_reason = hold_reason,
            )
            if result is not None:
                return result

        # ══════════════════════════════════════════════════════════════════════
        # PHASE 2: Unswept pool anchor (structural stop cluster)
        # ══════════════════════════════════════════════════════════════════════
        unswept_anchor = self._find_unswept_anchor(
            pos_side     = pos_side,
            price        = price,
            current_sl   = current_sl,
            atr          = atr,
            liq_snapshot = liq_snapshot,
            sess_mult    = sess_mult,
        )

        if unswept_anchor is not None:
            new_sl = self._compute_sl_from_anchor(
                pos_side = pos_side,
                anchor   = unswept_anchor,
                atr      = atr,
                sess_mult= sess_mult,
            )
            result = self._validate_and_return(
                pos_side    = pos_side,
                new_sl      = new_sl,
                current_sl  = current_sl,
                price       = price,
                atr         = atr,
                anchor      = unswept_anchor,
                phase       = "UNSWEPT_POOL",
                hold_reason = hold_reason,
            )
            if result is not None:
                return result

        # ══════════════════════════════════════════════════════════════════════
        # PHASE 3: No qualifying structural anchor found
        # ══════════════════════════════════════════════════════════════════════
        return self._hold("no_structural_anchor", hold_reason)

    # ─────────────────────────────────────────────────────────────────────────
    # ANCHOR SELECTION
    # ─────────────────────────────────────────────────────────────────────────

    def _find_swept_anchor(
        self,
        pos_side:     str,
        price:        float,
        current_sl:   float,
        atr:          float,
        liq_snapshot,
        now:          float,
        sess_mult:    float,
    ) -> Optional[PoolAnchor]:
        """
        Find the best swept pool to anchor SL to.

        For LONG: find the highest-priced SWEPT SSL below price.
          This was the entry confirmation sweep. SL goes just below it.
          As price sweeps more SSLs on the way up, SL advances to the
          most recently swept one (highest below price).

        For SHORT: find the lowest-priced SWEPT BSL above price.
        """
        candidates: List[PoolAnchor] = []

        # Check LiquidityMap swept history
        if pos_side == "long":
            swept_prices = getattr(liq_snapshot, 'swept_ssl_levels', [])
            pool_list    = getattr(liq_snapshot, 'ssl_pools', [])
        else:
            swept_prices = getattr(liq_snapshot, 'swept_bsl_levels', [])
            pool_list    = getattr(liq_snapshot, 'bsl_pools', [])

        # Also scan active pool list for SWEPT status pools (richer data)
        for pt in pool_list:
            pool = pt.pool
            pool_status = str(getattr(pool, 'status', '')).upper()
            if 'SWEPT' not in pool_status and 'CONSUMED' not in pool_status:
                continue

            pool_price = float(getattr(pool, 'price', 0.0))
            dist_atr   = abs(pool_price - price) / atr

            if dist_atr > _POOL_LOOKBACK_ATR:
                continue

            # Must be BEHIND price (below for long, above for short)
            if pos_side == "long" and pool_price >= price:
                continue
            if pos_side == "short" and pool_price <= price:
                continue

            # Recency check on swept pools
            swept_at = float(getattr(pool, 'swept_at', 0.0))
            if swept_at > 0 and (now - swept_at) > _SWEPT_POOL_MAX_AGE_SEC:
                continue

            sig = pt.adjusted_sig() if hasattr(pt, 'adjusted_sig') else float(
                getattr(pool, 'significance', 1.0))
            buf = self._sig_to_buffer(sig)

            candidates.append(PoolAnchor(
                price       = pool_price,
                side        = "SSL" if pos_side == "long" else "BSL",
                timeframe   = getattr(pool, 'timeframe', '?'),
                sig         = sig,
                buffer_atr  = buf,
                is_swept    = True,
                distance_atr= round(dist_atr, 2),
            ))

        # Also use the swept_level lists for simpler fallback
        for sp in (swept_prices or []):
            sp_price = float(sp)
            dist_atr = abs(sp_price - price) / atr
            if dist_atr > _POOL_LOOKBACK_ATR:
                continue
            if pos_side == "long" and sp_price >= price:
                continue
            if pos_side == "short" and sp_price <= price:
                continue
            # Use moderate sig for history entries (no metadata available)
            sig = 3.0
            buf = self._sig_to_buffer(sig)
            # Only add if not already captured from pool_list
            if not any(abs(c.price - sp_price) < atr * 0.1 for c in candidates):
                candidates.append(PoolAnchor(
                    price       = sp_price,
                    side        = "SSL" if pos_side == "long" else "BSL",
                    timeframe   = "?",
                    sig         = sig,
                    buffer_atr  = buf,
                    is_swept    = True,
                    distance_atr= round(dist_atr, 2),
                ))

        if not candidates:
            return None

        # Best swept anchor = CLOSEST to current price (most recent, tightest SL)
        # For long: highest price below price = closest to price from below
        # For short: lowest price above price = closest to price from above
        if pos_side == "long":
            best = max(candidates, key=lambda a: a.price)
        else:
            best = min(candidates, key=lambda a: a.price)

        return best

    def _find_unswept_anchor(
        self,
        pos_side:     str,
        price:        float,
        current_sl:   float,
        atr:          float,
        liq_snapshot,
        sess_mult:    float,
    ) -> Optional[PoolAnchor]:
        """
        Find the nearest unswept pool behind price to anchor SL.

        For LONG: nearest unswept SSL below price (stop cluster below us).
          SL goes just below this pool. Price would have to sweep these stops
          to reach our SL — that's an institutional move, not noise.

        For SHORT: nearest unswept BSL above price.
        """
        candidates: List[PoolAnchor] = []

        if pos_side == "long":
            pool_list = getattr(liq_snapshot, 'ssl_pools', [])
        else:
            pool_list = getattr(liq_snapshot, 'bsl_pools', [])

        for pt in pool_list:
            pool = pt.pool
            pool_status = str(getattr(pool, 'status', '')).upper()
            if 'SWEPT' in pool_status or 'CONSUMED' in pool_status:
                continue

            pool_price = float(getattr(pool, 'price', 0.0))
            dist_atr   = abs(pool_price - price) / atr

            if dist_atr > _POOL_LOOKBACK_ATR:
                continue

            if pos_side == "long" and pool_price >= price:
                continue
            if pos_side == "short" and pool_price <= price:
                continue

            sig = pt.adjusted_sig() if hasattr(pt, 'adjusted_sig') else float(
                getattr(pool, 'significance', 1.0))
            buf = self._sig_to_buffer(sig)

            candidates.append(PoolAnchor(
                price       = pool_price,
                side        = "SSL" if pos_side == "long" else "BSL",
                timeframe   = getattr(pool, 'timeframe', '?'),
                sig         = sig,
                buffer_atr  = buf,
                is_swept    = False,
                distance_atr= round(dist_atr, 2),
            ))

        if not candidates:
            return None

        # Choose the CLOSEST unswept pool behind price
        # (this is where stops cluster — SL just behind them)
        if pos_side == "long":
            # Highest SSL below price = closest behind us
            best = max(candidates, key=lambda a: a.price)
        else:
            best = min(candidates, key=lambda a: a.price)

        return best

    # ─────────────────────────────────────────────────────────────────────────
    # SL COMPUTATION
    # ─────────────────────────────────────────────────────────────────────────

    def _compute_sl_from_anchor(
        self,
        pos_side:  str,
        anchor:    PoolAnchor,
        atr:       float,
        sess_mult: float,
    ) -> float:
        """
        Compute SL price from anchor pool with significance-based buffer.

        Long:  SL = anchor.price - (anchor.buffer_atr × ATR × session_mult)
        Short: SL = anchor.price + (anchor.buffer_atr × ATR × session_mult)

        Buffer decreases as significance increases:
          High-sig (4H/1D cluster) → very tight: price barely needs to dip below.
          Low-sig  (5m micro pool) → wider: more noise tolerance needed.
        """
        buf = anchor.buffer_atr * atr * sess_mult
        if pos_side == "long":
            return anchor.price - buf
        else:
            return anchor.price + buf

    # ─────────────────────────────────────────────────────────────────────────
    # VALIDATION
    # ─────────────────────────────────────────────────────────────────────────

    def _validate_and_return(
        self,
        pos_side:   str,
        new_sl:     float,
        current_sl: float,
        price:      float,
        atr:        float,
        anchor:     PoolAnchor,
        phase:      str,
        hold_reason: Optional[List[str]],
    ) -> Optional[LiquidityTrailResult]:
        """
        Apply guards before accepting a new SL.

        Returns LiquidityTrailResult if SL is valid, None to try next phase.
        """
        # Guard 1: SL must be an IMPROVEMENT (ratchet)
        if pos_side == "long":
            if new_sl <= current_sl:
                return None   # not better — try next anchor
        else:
            if new_sl >= current_sl:
                return None

        # Guard 2: Minimum breathing room from price
        dist_to_price = abs(price - new_sl) / atr
        if dist_to_price < _MIN_BREATHING_ATR:
            msg = (f"breathing_room={dist_to_price:.2f}ATR < "
                   f"{_MIN_BREATHING_ATR}ATR from ${anchor.price:.0f} "
                   f"({anchor.timeframe})")
            if hold_reason is not None:
                hold_reason.append(msg)
            return None   # too tight — try next phase

        # Guard 3: Maximum SL distance cap
        if dist_to_price > _MAX_SL_DIST_ATR:
            msg = (f"anchor_too_far={dist_to_price:.1f}ATR > "
                   f"{_MAX_SL_DIST_ATR}ATR ({anchor.timeframe})")
            if hold_reason is not None:
                hold_reason.append(msg)
            return None

        # Guard 4: Minimum meaningful improvement
        improvement = abs(new_sl - current_sl) / atr
        if improvement < _MIN_IMPROVEMENT_ATR:
            msg = (f"improvement={improvement:.3f}ATR < "
                   f"{_MIN_IMPROVEMENT_ATR}ATR (micro-move skipped)")
            if hold_reason is not None:
                hold_reason.append(msg)
            return None

        # All guards passed
        sweep_status = "swept" if anchor.is_swept else "unswept"
        reason = (
            f"[{phase}] {sweep_status} {anchor.side}@${anchor.price:.0f} "
            f"({anchor.timeframe}, sig={anchor.sig:.1f}) "
            f"buf={anchor.buffer_atr:.2f}ATR → SL=${new_sl:.1f}"
        )
        logger.info(f"Trail: {reason}")

        return LiquidityTrailResult(
            new_sl   = round(new_sl, 1),
            anchor   = anchor,
            reason   = reason,
            phase    = phase,
        )

    # ─────────────────────────────────────────────────────────────────────────
    # UTILITIES
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _sig_to_buffer(sig: float) -> float:
        """
        Convert pool significance to ATR buffer multiplier.

        Higher significance → tighter buffer (institution more committed).
        Breakpoints in _SIG_BUFFER_BREAKPOINTS (descending sig order).
        """
        for threshold, buffer in _SIG_BUFFER_BREAKPOINTS:
            if sig >= threshold:
                return buffer
        return _SIG_BUFFER_BREAKPOINTS[-1][1]   # weakest pool

    @staticmethod
    def _detect_session(ict_engine) -> str:
        """Detect current trading session from ICT engine killzone."""
        if ict_engine is None:
            return ""
        kz = str(getattr(ict_engine, '_killzone', '')).upper()
        if 'LONDON' in kz:
            return 'LONDON'
        if 'NY' in kz:
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
            hold_reason.append(f"TRAIL_BLOCKED:{reason}")
        return LiquidityTrailResult(
            new_sl=None, anchor=None, reason=reason,
            phase="HOLD", trail_blocked=True, block_reason=reason)


# ─────────────────────────────────────────────────────────────────────────────
# INTEGRATION GUIDE FOR quant_strategy.py
# ─────────────────────────────────────────────────────────────────────────────
"""
STEP 1: Import and initialise in QuantStrategy.__init__():

    from liquidity_trail import LiquidityTrailEngine
    self._liq_trail = LiquidityTrailEngine()

STEP 2: In the trailing SL update section (wherever _DynamicStructureTrail or
compute_trail_sl is called), add this BEFORE the existing trail call:

    # ── Liquidity-Only Trail (Issue-3 Fix) ───────────────────────────────────
    _hold_reasons: list = []
    _liq_trail_result = self._liq_trail.compute(
        pos_side        = self._pos.side,
        price           = price,
        entry_price     = self._pos.entry_price,
        current_sl      = self._pos.sl_price,
        atr             = atr,
        initial_sl_dist = self._pos.initial_sl_dist,
        peak_profit     = self._pos.peak_profit,
        liq_snapshot    = liq_snapshot,   # current tick's snapshot
        ict_engine      = self._ict,
        now             = now,
        hold_reason     = _hold_reasons,
    )

    if _liq_trail_result.new_sl is not None:
        # Liquidity anchor found — use this SL (overrides chandelier)
        new_sl_price = _liq_trail_result.new_sl
        logger.info(
            f"LiqTrail: {_liq_trail_result.phase} "
            f"anchor={_liq_trail_result.anchor.price:.0f} "
            f"→ SL={new_sl_price:.1f} | {_liq_trail_result.reason}"
        )
        # Place the new SL limit order:
        self._om.replace_stop_loss(
            existing_sl_order_id = self._pos.sl_order_id,
            side                 = "SELL" if self._pos.side == "long" else "BUY",
            quantity             = self._pos.quantity,
            new_trigger_price    = new_sl_price,
        )
        self._pos.sl_price = new_sl_price
        return   # done — skip existing chandelier trail for this tick
    else:
        logger.debug(
            f"LiqTrail: HOLD | {' | '.join(_hold_reasons[:3])}"
        )
        # Fall through to existing chandelier / structure trail as backup

STEP 3: The existing _DynamicStructureTrail / compute_trail_sl runs as a
fallback ONLY when no liquidity anchor is found. This preserves the OB/
Breaker anchors and chandelier as safety nets during featureless markets.

STEP 4 (optional): Display the trail state in Telegram heartbeat:
    if _liq_trail_result.anchor:
        a = _liq_trail_result.anchor
        trail_display = (
            f"{'🟢' if a.is_swept else '🔵'} "
            f"{'Swept' if a.is_swept else 'Unswept'} "
            f"{a.side}@${a.price:,.0f} ({a.timeframe}) "
            f"sig={a.sig:.1f} buf={a.buffer_atr:.2f}ATR"
        )
"""

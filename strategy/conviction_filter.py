"""
conviction_filter.py — Institutional Entry Conviction Gate v2.1
================================================================
v2.1 FIX LOG:
  BUG-SCORE-ZERO: evaluate() returned early from _check_session_limits()
    BEFORE running any factor scoring. Every cooldown-blocked entry showed
    score=0.000 with all factor bars at zero in the Telegram display — making
    it impossible to see whether the setup was actually good or bad.

  FIX: Factor scoring now ALWAYS runs first, producing a real conviction score.
    Session limit check runs AFTER scoring, as a separate gate.  The result
    carries the real score (e.g. 0.552) but allowed=False with a clear
    MIN_INTERVAL reason.  Telegram displays real factor bars even during
    cooldown.

  This also fixes the misleading "Dealing Range GATE: FAIL" label that
  appeared in the conviction block alert during MIN_INTERVAL blocks —
  the dealing range gate had never been evaluated.

FACTOR WEIGHTS (unchanged from v2.0):
  1. Pool significance tier         0.25
  2. Dealing range valid            GATE  (mandatory early-return still applies)
  3. Displacement confirmed         0.25
  4. CISD (CHoCH/BOS post-sweep)   0.20
  5. OTE zone retracement           0.15
  6. Session alignment              0.10
  7. AMD phase alignment            0.05
  TOTAL                             1.00

MANDATORY GATES (still enforced — early return on fail):
  - Dealing range: longs below 0.58 P/D only, shorts above 0.42 P/D only
  - R:R: minimum 1.40 (matching entry engine)
  NOTE: Session limits are NOT mandatory gates — they run after scoring.
"""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS  (all numeric thresholds sourced from central config.py)
# ─────────────────────────────────────────────────────────────────────────────
from config import (
    CONVICTION_MIN_SCORE               as REQUIRED_CONVICTION_SCORE,
    CONVICTION_POOL_MIN_TF_RANK        as POOL_MIN_TF_RANK,
    CONVICTION_DISPLACEMENT_BODY_ATR   as DISPLACEMENT_MIN_BODY_ATR,
    CONVICTION_OTE_FIB_LOW             as OTE_FIB_LOW,
    CONVICTION_OTE_FIB_HIGH            as OTE_FIB_HIGH,
    CONVICTION_MIN_RR                  as MIN_RR,
    CONVICTION_MAX_SESSION_LOSSES      as MAX_SESSION_LOSSES,
    CONVICTION_MIN_ENTRY_INTERVAL_SEC  as MIN_ENTRY_INTERVAL_SEC,
    CONVICTION_MAX_ENTRIES_PER_SESSION as MAX_ENTRIES_PER_SESSION,
)

# ── Timeframe rank lookup (logic — not a tunable threshold) ──────────────────
_TF_RANK: Dict[str, int] = {
    "1m": 1, "2m": 1, "3m": 1, "5m": 2, "15m": 3,
    "30m": 3, "1h": 4, "4h": 5, "1d": 6,
}

# ── Session quality multipliers (logic — not a tunable threshold) ─────────────
_SESSION_SCORE: Dict[str, float] = {
    "LONDON":    1.00,
    "NY":        1.00,
    "NEW_YORK":  1.00,
    "LONDON_NY": 0.80,
    "ASIA":      0.10,   # penalty, not hard block
    "":          0.50,
}

# ── AMD phase base scores (logic — not a tunable threshold) ───────────────────
_AMD_PHASE_SCORE: Dict[str, float] = {
    "MANIPULATION":   1.00,
    "DISTRIBUTION":   0.85,
    "REACCUMULATION": 0.75,
    "REDISTRIBUTION": 0.75,
    "ACCUMULATION":   0.40,
    "":               0.45,
}


# ─────────────────────────────────────────────────────────────────────────────
# DATA STRUCTURES
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ConvictionFactors:
    pool_sig_score:     float = 0.0
    dealing_range_ok:   bool  = False
    displacement_score: float = 0.0
    cisd_score:         float = 0.0
    ote_score:          float = 0.0
    session_score:      float = 0.0
    amd_score:          float = 0.0


@dataclass
class ConvictionResult:
    allowed:        bool
    score:          float
    factors:        ConvictionFactors  = field(default_factory=ConvictionFactors)
    reject_reasons: List[str]          = field(default_factory=list)
    allow_reasons:  List[str]          = field(default_factory=list)
    rr_ratio:       float              = 0.0
    pool_tf:        str                = ""
    pool_sig:       float              = 0.0
    # v2.1: True when the only block is a session-timing gate (score is real)
    blocked_by_timing: bool            = False


@dataclass
class SessionState:
    session_id:         str   = ""
    entries_taken:      int   = 0
    consecutive_losses: int   = 0
    last_entry_time:    float = 0.0
    wins:               int   = 0
    losses:             int   = 0

    def record_outcome(self, win: bool) -> None:
        if win:
            self.wins += 1
            self.consecutive_losses = 0
        else:
            self.losses += 1
            self.consecutive_losses += 1


# ─────────────────────────────────────────────────────────────────────────────
# CONVICTION GATE ENGINE
# ─────────────────────────────────────────────────────────────────────────────

class ConvictionFilter:
    """
    Institutional Entry Conviction Gate v2.1

    Evaluates 7 ICT factors with corrected calculations.
    Mandatory gates (early return): dealing range + R:R.
    Session timing limits run AFTER scoring (score is always real).
    Score >= 0.55 AND no session limit block → entry allowed.
    """

    def __init__(self) -> None:
        self._session_state = SessionState()

    def evaluate(
        self,
        trade_side:   str,
        sweep_pool,
        entry_price:  float,
        sl_price:     float,
        tp_price:     float,
        price:        float,
        atr:          float,
        now:          float,
        ict_engine              = None,
        liq_snapshot            = None,
        ps_decision             = None,
        candles_5m: Optional[List] = None,
        session:    str            = "",
        entry_type: str            = "",
    ) -> ConvictionResult:
        """
        Evaluate conviction for a potential entry.

        v2.1 change: factor scoring runs UNCONDITIONALLY before session-limit
        checks.  This means blocked-by-cooldown results carry a real score and
        real factor values — not zeros.  The Telegram display is truthful.

        Return is always a ConvictionResult with:
          .allowed        — True only if score passes AND no session limit hit
          .score          — real conviction score (0-1), always computed
          .blocked_by_timing — True when the only block is MIN_INTERVAL / session cap
          .factors        — real per-factor scores (never all-zero except on gate failure)
          .reject_reasons — human-readable reasons for any block
        """
        factors  = ConvictionFactors()
        rejects: List[str] = []
        allows:  List[str] = []

        # ── Pool info ──────────────────────────────────────────────────────
        pool_price, pool_tf, pool_sig, pool_htf_count = \
            self._extract_pool_info(sweep_pool, atr)

        # ── EFFECTIVE TF RANK ──────────────────────────────────────────────
        native_rank = _TF_RANK.get(pool_tf, 2)
        if   pool_htf_count >= 3: effective_rank = max(native_rank, 4)
        elif pool_htf_count >= 2: effective_rank = max(native_rank, 3)
        elif pool_htf_count >= 1: effective_rank = max(native_rank, 2)
        else:                     effective_rank = native_rank

        # ── MANDATORY GATE 1: Pool effective timeframe ─────────────────────
        # This is a hard structural filter — return early with zero score.
        if effective_rank < POOL_MIN_TF_RANK:
            rejects.append(
                f"POOL_TF_BLOCKED: {pool_tf}(htfx{pool_htf_count}) "
                f"effective_rank={effective_rank} < required={POOL_MIN_TF_RANK}. "
                f"Need 5m+ effective rank (native 5m+, or 1m with HTFx1+)."
            )
            return ConvictionResult(
                allowed=False, score=0.0,
                reject_reasons=rejects, factors=factors,
                pool_tf=pool_tf, pool_sig=pool_sig)

        # ── FACTOR 1: Pool significance score (weight 0.25) ───────────────
        _rank_bonus = min(effective_rank / 4.0, 1.0)
        _sig_score  = min(pool_sig / 8.0, 1.0)
        factors.pool_sig_score = _rank_bonus * 0.40 + _sig_score * 0.60
        if effective_rank >= 3 and pool_sig >= 5.0:
            allows.append(f"POOL={pool_tf}(htfx{pool_htf_count}) sig={pool_sig:.1f} STRONG")
        else:
            allows.append(f"POOL={pool_tf}(htfx{pool_htf_count}) sig={pool_sig:.1f}")

        # ── MANDATORY GATE 2: Dealing range ───────────────────────────────
        # Hard structural filter — return early with zero score.
        dr_pd = self._get_dealing_range_pd(price, ict_engine, liq_snapshot)
        if trade_side == "long" and dr_pd > 0.58:
            rejects.append(
                f"DEALING_RANGE_BLOCKED: LONG in PREMIUM (P/D={dr_pd:.2f}>0.58). "
                f"ICT rule: longs only below equilibrium."
            )
            factors.dealing_range_ok = False
            return ConvictionResult(
                allowed=False, score=0.0,
                reject_reasons=rejects, factors=factors,
                pool_tf=pool_tf, pool_sig=pool_sig)
        elif trade_side == "short" and dr_pd < 0.42:
            rejects.append(
                f"DEALING_RANGE_BLOCKED: SHORT in DISCOUNT (P/D={dr_pd:.2f}<0.42). "
                f"ICT rule: shorts only above equilibrium."
            )
            factors.dealing_range_ok = False
            return ConvictionResult(
                allowed=False, score=0.0,
                reject_reasons=rejects, factors=factors,
                pool_tf=pool_tf, pool_sig=pool_sig)
        else:
            factors.dealing_range_ok = True
            _dr_label = (
                "DEEP-DISC" if dr_pd < 0.25 else
                "DISCOUNT"  if dr_pd < 0.42 else
                "EQ"        if dr_pd < 0.58 else
                "PREMIUM"   if dr_pd < 0.75 else "DEEP-PREM"
            )
            allows.append(f"DR={_dr_label}({dr_pd:.2f})")

        # ── MANDATORY GATE 3: R:R ──────────────────────────────────────────
        # Hard structural filter — return early with zero score.
        rr = self._compute_rr(trade_side, entry_price, sl_price, tp_price)
        if rr < MIN_RR:
            rejects.append(
                f"RR_BLOCKED: R:R={rr:.2f} < {MIN_RR:.2f}. "
                f"Minimum {MIN_RR:.2f}R required."
            )
            return ConvictionResult(
                allowed=False, score=0.0,
                reject_reasons=rejects, factors=factors,
                rr_ratio=rr, pool_tf=pool_tf, pool_sig=pool_sig)
        allows.append(f"RR={rr:.2f}R")

        is_approach = ("approach" in entry_type.lower() or
                       "pre_sweep" in entry_type.lower() or
                       "proximity" in entry_type.lower())

        # ── FACTOR 3: Displacement (weight 0.25) ──────────────────────────
        factors.displacement_score = self._score_displacement(
            trade_side, price, pool_price, atr, candles_5m, ict_engine,
            is_approach=is_approach)
        if factors.displacement_score >= 0.70:
            allows.append(f"DISP={factors.displacement_score:.2f} ✅")
        elif factors.displacement_score >= 0.40:
            allows.append(f"DISP={factors.displacement_score:.2f}")
        else:
            rejects.append(f"DISP_WEAK={factors.displacement_score:.2f}")

        # ── FACTOR 4: CISD (weight 0.20) ──────────────────────────────────
        factors.cisd_score = self._score_cisd(
            trade_side, ps_decision, ict_engine, is_approach=is_approach)
        if factors.cisd_score >= 0.70:
            allows.append(f"CISD={factors.cisd_score:.2f} ✅")
        elif factors.cisd_score >= 0.35:
            allows.append(f"CISD={factors.cisd_score:.2f}")

        # ── FACTOR 5: OTE zone (weight 0.15) ──────────────────────────────
        if is_approach:
            factors.ote_score = 0.55
        else:
            factors.ote_score = self._score_ote(
                trade_side, entry_price, pool_price, price)
        if factors.ote_score >= 0.70:
            allows.append(f"OTE={factors.ote_score:.2f} ✅")

        # ── FACTOR 6: Session (weight 0.10) ───────────────────────────────
        sess_key = self._resolve_session(session, ict_engine)
        factors.session_score = _SESSION_SCORE.get(sess_key, 0.50)
        if factors.session_score >= 0.80:
            allows.append(f"SESSION={sess_key}")
        elif factors.session_score < 0.20:
            rejects.append(f"SESSION_WEAK={sess_key}({factors.session_score:.2f})")

        # ── FACTOR 7: AMD phase (weight 0.05) ─────────────────────────────
        factors.amd_score = self._score_amd(trade_side, ict_engine)
        if factors.amd_score >= 0.80:
            allows.append(f"AMD={factors.amd_score:.2f} ✅")
        elif factors.amd_score < 0.20:
            rejects.append(f"AMD_WEAK={factors.amd_score:.2f}")

        # ── COMPUTE CONVICTION SCORE ───────────────────────────────────────
        score = (
            factors.pool_sig_score     * 0.25 +
            factors.displacement_score * 0.25 +
            factors.cisd_score         * 0.20 +
            factors.ote_score          * 0.15 +
            factors.session_score      * 0.10 +
            factors.amd_score          * 0.05
        )
        score = round(score, 4)

        # ── SCORE GATE ─────────────────────────────────────────────────────
        score_passed = score >= REQUIRED_CONVICTION_SCORE
        if not score_passed:
            rejects.append(
                f"SCORE_TOO_LOW: {score:.3f} < {REQUIRED_CONVICTION_SCORE} "
                f"[pool={factors.pool_sig_score:.2f} "
                f"disp={factors.displacement_score:.2f} "
                f"cisd={factors.cisd_score:.2f} "
                f"ote={factors.ote_score:.2f} "
                f"sess={factors.session_score:.2f} "
                f"amd={factors.amd_score:.2f}]"
            )
        else:
            allows.append(f"TOTAL={score:.3f} ✅")

        # ── SESSION TIMING GATE (runs AFTER scoring — v2.1 fix) ───────────
        # Checked LAST so the score is always computed and displayed correctly.
        # A timing block sets blocked_by_timing=True so the caller can log
        # the real score in Telegram rather than 0.000.
        timing_block = self._check_session_limits(now, session)
        if timing_block:
            # Score was computed — still block the entry but carry real score
            rejects_with_timing = [timing_block] + rejects
            if score_passed:
                # Signal quality is good; timing is the only blocker
                log_score_str = f"{score:.3f} (GOOD — blocked by timing only)"
            else:
                log_score_str = f"{score:.3f}"
            logger.info(
                f"🚫 CONVICTION GATE BLOCKED ({log_score_str}) | "
                f"TIMING: {timing_block}"
            )
            return ConvictionResult(
                allowed        = False,
                score          = score,
                factors        = factors,
                reject_reasons = rejects_with_timing,
                allow_reasons  = allows,
                rr_ratio       = rr,
                pool_tf        = pool_tf,
                pool_sig       = pool_sig,
                blocked_by_timing = True,
            )

        # ── FINAL DECISION ─────────────────────────────────────────────────
        allowed = score_passed
        if allowed:
            logger.info(
                f"✅ CONVICTION PASSED ({score:.3f}) | "
                f"{' | '.join(allows)}")
        else:
            # Logged at DEBUG — quant_strategy logs the gate block at INFO level
            # to avoid double-logging the same rejection every tick.
            logger.debug(
                f"❌ CONVICTION BLOCKED ({score:.3f}) | "
                f"REJECT: {' | '.join(rejects)}")

        return ConvictionResult(
            allowed        = allowed,
            score          = score,
            factors        = factors,
            reject_reasons = rejects,
            allow_reasons  = allows,
            rr_ratio       = rr,
            pool_tf        = pool_tf,
            pool_sig       = pool_sig,
        )

    def mark_entry_placed(self, now: float) -> None:
        """
        Called by quant_strategy._enter_trade() the moment PositionPhase.ACTIVE
        is confirmed (order filled, position live). This is the ONLY place that
        arms the MIN_ENTRY_INTERVAL cooldown and increments entries_taken.
        """
        self._session_state.entries_taken  += 1
        self._session_state.last_entry_time = now
        logger.info(
            f"ConvictionFilter: entry placed — "
            f"entries_taken={self._session_state.entries_taken}/{MAX_ENTRIES_PER_SESSION} "
            f"| next entry allowed in {MIN_ENTRY_INTERVAL_SEC}s"
        )

    def on_session_change(self, new_session: str) -> None:
        """
        Called by quant_strategy when the killzone/session changes.
        Resets entries_taken and consecutive_losses for the new session.
        The 900s MIN_ENTRY_INTERVAL cooldown is also cleared.
        """
        old_sess = self._session_state.session_id
        if new_session == old_sess:
            return
        logger.info(
            f"ConvictionFilter: session boundary {old_sess!r} → {new_session!r} "
            f"| resetting entries_taken={self._session_state.entries_taken} "
            f"and consecutive_losses={self._session_state.consecutive_losses}"
        )
        wins   = self._session_state.wins
        losses = self._session_state.losses
        self._session_state = SessionState(
            session_id   = new_session,
            wins         = wins,
            losses       = losses,
        )

    def record_trade_result(self, win: bool) -> None:
        self._session_state.record_outcome(win)
        logger.info(
            f"ConvictionFilter: {'WIN' if win else 'LOSS'} | "
            f"W/L={self._session_state.wins}/{self._session_state.losses} "
            f"consec_losses={self._session_state.consecutive_losses}"
        )

    def get_session_state_summary(self) -> Dict:
        """Return current session state for display/logging."""
        st = self._session_state
        elapsed_since_entry = 0
        cooldown_remaining  = 0
        if st.last_entry_time > 0:
            elapsed_since_entry = time.time() - st.last_entry_time
            cooldown_remaining  = max(0, MIN_ENTRY_INTERVAL_SEC - elapsed_since_entry)
        return {
            "session_id":          st.session_id,
            "entries_taken":       st.entries_taken,
            "max_entries":         MAX_ENTRIES_PER_SESSION,
            "consecutive_losses":  st.consecutive_losses,
            "max_losses":          MAX_SESSION_LOSSES,
            "wins":                st.wins,
            "losses":              st.losses,
            "last_entry_time":     st.last_entry_time,
            "cooldown_remaining_s": int(cooldown_remaining),
            "cooldown_active":     cooldown_remaining > 0,
        }

    # ─────────────────────────────────────────────────────────────────────────
    # POOL INFO EXTRACTION
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _extract_pool_info(sweep_pool, atr: float) -> Tuple[float, str, float, int]:
        """
        Extract (price, timeframe, significance, htf_count) from any pool object.
        """
        if sweep_pool is None:
            return 0.0, "5m", 1.0, 0

        inner = getattr(sweep_pool, 'pool', sweep_pool)

        price   = float(getattr(inner, 'price', 0.0) or 0.0)
        tf      = str(getattr(inner, 'timeframe', '5m') or '5m')
        htf_cnt = int(getattr(inner, 'htf_count', 0) or 0)

        sig = 0.0
        for attr in ('significance', 'adjusted_sig', 'sig'):
            _v = getattr(sweep_pool, attr, None)
            if _v is None:
                _v = getattr(inner, attr, None)
            if _v is not None:
                try:
                    sig = float(_v() if callable(_v) else _v)
                    if sig > 0:
                        break
                except Exception:
                    pass
        if sig <= 0.0:
            tc  = int(getattr(inner, 'touches', 0) or 0)
            sig = max(1.0, 2.0 + tc * 0.5)

        dist_atr = float(getattr(sweep_pool, 'distance_atr', 99.0) or 99.0)
        if dist_atr < 3.0:
            sig *= max(0.5, 1.0 + (3.0 - dist_atr) / 6.0)

        return price, tf, round(sig, 2), htf_cnt

    # ─────────────────────────────────────────────────────────────────────────
    # SESSION QUALITY CONTROL
    # ─────────────────────────────────────────────────────────────────────────

    def _check_session_limits(self, now: float, session: str) -> Optional[str]:
        """
        Check timing/pacing gates.  Returns a reason string if blocked, else None.

        v2.1: Called AFTER all factor scoring so the returned ConvictionResult
        always carries a real score even when this gate fires.
        """
        st = self._session_state

        # ── Pacing: minimum interval between consecutive entries ──────────────
        if st.last_entry_time > 0:
            elapsed = now - st.last_entry_time
            if elapsed < MIN_ENTRY_INTERVAL_SEC:
                wait = int(MIN_ENTRY_INTERVAL_SEC - elapsed)
                return f"MIN_INTERVAL: {wait}s until next entry allowed"

        # ── Consecutive loss circuit breaker ──────────────────────────────────
        if st.consecutive_losses >= MAX_SESSION_LOSSES:
            return (
                f"SESSION_INVALIDATED: {st.consecutive_losses} consecutive losses "
                f"in session '{st.session_id}'. Review direction before next entry."
            )

        # ── Per-session entry cap ─────────────────────────────────────────────
        if st.entries_taken >= MAX_ENTRIES_PER_SESSION:
            return (
                f"MAX_ENTRIES_HIT: {st.entries_taken}/{MAX_ENTRIES_PER_SESSION} "
                f"entries taken this session. New session resets this counter."
            )

        return None

    def reset_session(self, reason: str = "manual") -> None:
        """Full session reset — clears all pacing state. W/L counters preserved."""
        wins   = self._session_state.wins
        losses = self._session_state.losses
        logger.info(
            f"ConvictionFilter: full session reset ({reason}) "
            f"| was W/L={wins}/{losses} "
            f"entries={self._session_state.entries_taken} "
            f"consec_losses={self._session_state.consecutive_losses}"
        )
        self._session_state = SessionState(wins=wins, losses=losses)

    # ─────────────────────────────────────────────────────────────────────────
    # FACTOR SCORING METHODS
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _get_dealing_range_pd(price: float, ict_engine, liq_snapshot) -> float:
        """Get dealing range P/D position [0=full discount, 1=full premium]."""
        dr = getattr(ict_engine, '_dealing_range', None) if ict_engine else None
        if dr is not None:
            pd = getattr(dr, 'current_pd', None)
            if pd is not None:
                return float(pd)

        if liq_snapshot is not None:
            bsl_pools = getattr(liq_snapshot, 'bsl_pools', [])
            ssl_pools = getattr(liq_snapshot, 'ssl_pools', [])
            bsl_above = [t.pool.price for t in bsl_pools if t.pool.price > price]
            ssl_below = [t.pool.price for t in ssl_pools if t.pool.price < price]
            if bsl_above and ssl_below:
                bsl = min(bsl_above)
                ssl = max(ssl_below)
                rng = max(bsl - ssl, 1e-9)
                return (price - ssl) / rng

        return 0.50  # neutral — passes gate for both directions

    @staticmethod
    def _compute_rr(trade_side: str, entry: float, sl: float, tp: float) -> float:
        sl_dist = abs(entry - sl)
        tp_dist = abs(tp - entry)
        if sl_dist < 1e-10:
            return 0.0
        return tp_dist / sl_dist

    @staticmethod
    def _score_displacement(
        trade_side:  str,
        price:       float,
        pool_price:  float,
        atr:         float,
        candles_5m:  Optional[List],
        ict_engine,
        is_approach: bool = False,
    ) -> float:
        """Score displacement strength [0-1]."""
        if is_approach:
            if ict_engine is not None:
                try:
                    for pool in ict_engine.liquidity_pools:
                        if abs(getattr(pool, 'price', 0) - pool_price) < atr * 0.5:
                            if getattr(pool, 'displacement_confirmed', False):
                                ds = float(getattr(pool, 'displacement_score', 0.5) or 0.5)
                                return min(ds * 0.80, 0.80)
                except Exception:
                    pass
            return 0.45

        if candles_5m and len(candles_5m) >= 4:
            closed = candles_5m[-4:-1]
            scores = []
            for c in closed:
                try:
                    o, cl = float(c['o']), float(c['c'])
                    h, lo = float(c['h']), float(c['l'])
                    body  = abs(cl - o)
                    rng   = max(h - lo, 1e-10)
                    bf    = body / rng
                    ba    = body / max(atr, 1e-10)

                    if trade_side == "long" and cl > o:
                        scores.append(min(ba / DISPLACEMENT_MIN_BODY_ATR, 1.0) * bf)
                    elif trade_side == "short" and cl < o:
                        scores.append(min(ba / DISPLACEMENT_MIN_BODY_ATR, 1.0) * bf)
                    else:
                        scores.append(0.0)
                except Exception:
                    continue

            if scores:
                return min(max(scores), 1.0)

        if ict_engine is not None:
            try:
                for p in ict_engine.liquidity_pools:
                    if abs(getattr(p, 'price', 0) - pool_price) < atr * 0.4:
                        ds = float(getattr(p, 'displacement_score', 0.0) or 0.0)
                        if ds > 0:
                            return min(ds, 1.0)
            except Exception:
                pass

        return 0.30

    @staticmethod
    def _score_cisd(
        trade_side:  str,
        ps_decision,
        ict_engine,
        is_approach: bool = False,
    ) -> float:
        """Score CISD (Change in State of Delivery) [0-1]."""
        if is_approach:
            if ict_engine is not None:
                try:
                    for tf in ("15m", "5m", "1m"):
                        st = getattr(ict_engine, '_tf', {}).get(tf)
                        if st is None:
                            continue
                        bos = str(getattr(st, 'bos_direction', '') or '').lower()
                        if trade_side == "long" and bos == "bullish":
                            return 0.55
                        if trade_side == "short" and bos == "bearish":
                            return 0.55
                except Exception:
                    pass
            return 0.40

        if ps_decision is not None:
            cisd_active = getattr(ps_decision, 'cisd_active', False)
            ps_conf     = float(getattr(ps_decision, 'confidence', 0.0) or 0.0)
            action      = str(getattr(ps_decision, 'action', '') or '')
            direction   = str(getattr(ps_decision, 'direction', '') or '').lower()

            dir_ok = (
                (trade_side == "long"  and direction in ("long", "bullish")) or
                (trade_side == "short" and direction in ("short", "bearish")) or
                direction == ""
            )

            if cisd_active and dir_ok and action == "reverse":
                return min(0.85 + ps_conf * 0.15, 1.0)
            if dir_ok and action == "reverse":
                return min(0.65 + ps_conf * 0.20, 0.85)
            if dir_ok and action in ("wait", "continue"):
                return 0.35
            if not dir_ok and action == "reverse":
                return 0.10
            return 0.25

        if ict_engine is not None:
            try:
                for tf in ("15m", "5m"):
                    st = getattr(ict_engine, '_tf', {}).get(tf)
                    if st is None:
                        continue
                    choch = str(getattr(st, 'choch_direction', '') or '').lower()
                    bos   = str(getattr(st, 'bos_direction', '') or '').lower()
                    if trade_side == "long" and (choch == "bullish" or bos == "bullish"):
                        return 0.60
                    if trade_side == "short" and (choch == "bearish" or bos == "bearish"):
                        return 0.60
            except Exception:
                pass

        return 0.15

    @staticmethod
    def _score_ote(
        trade_side:  str,
        entry_price: float,
        pool_price:  float,
        price:       float,
    ) -> float:
        """Score OTE (Optimal Trade Entry) zone [0-1]."""
        if pool_price <= 0 or entry_price <= 0 or price <= 0:
            return 0.50

        if trade_side == "long":
            total_move = abs(price - pool_price)
            if total_move < 1e-3:
                return 0.50
            fib = abs(price - entry_price) / total_move
        else:
            total_move = abs(pool_price - price)
            if total_move < 1e-3:
                return 0.50
            fib = abs(entry_price - price) / total_move

        if OTE_FIB_LOW <= fib <= OTE_FIB_HIGH:
            dist_from_618 = abs(fib - 0.618)
            return max(0.65, 1.0 - dist_from_618 * 5.0)
        elif fib < OTE_FIB_LOW:
            return max(0.30, fib / OTE_FIB_LOW * 0.65)
        else:
            return max(0.10, 0.40 - (fib - OTE_FIB_HIGH) * 3.0)

    @staticmethod
    def _score_amd(trade_side: str, ict_engine) -> float:
        """Score AMD phase alignment [0-1]."""
        if ict_engine is None:
            return 0.45

        try:
            amd = getattr(ict_engine, '_amd', None)
            if amd is None:
                return 0.45

            phase = str(getattr(amd, 'phase', '') or '').upper()
            bias  = str(getattr(amd, 'bias',  '') or '').lower()
            conf  = float(getattr(amd, 'confidence', 0.5) or 0.5)

            base = _AMD_PHASE_SCORE.get(phase, 0.40)

            long_ok  = (trade_side == "long"  and "bull" in bias)
            short_ok = (trade_side == "short" and "bear" in bias)
            neutral  = bias in ("neutral", "")
            contra   = not (long_ok or short_ok or neutral)

            if long_ok or short_ok:
                base = min(base * (1.0 + conf * 0.20), 1.0)
            elif contra:
                base *= 0.55

            if phase == "ACCUMULATION":
                if long_ok or short_ok:
                    base = max(base, 0.40)
                elif neutral:
                    base = max(base, 0.30)
                elif contra:
                    base = 0.10

            return min(base, 1.0)

        except Exception:
            return 0.40

    @staticmethod
    def _resolve_session(session_hint: str, ict_engine) -> str:
        if session_hint:
            su = session_hint.upper()
            if 'NEW_YORK' in su or ('NY' in su and 'LONDON' not in su):
                return 'NY'
            if 'LONDON' in su:
                return 'LONDON'
            if 'ASIA' in su:
                return 'ASIA'
        if ict_engine is not None:
            kz = str(getattr(ict_engine, '_killzone', '') or '').upper()
            if 'NEW_YORK' in kz or ('NY' in kz and 'LONDON' not in kz):
                return 'NY'
            if 'LONDON' in kz:
                return 'LONDON'
            if 'ASIA' in kz:
                return 'ASIA'
        return ''

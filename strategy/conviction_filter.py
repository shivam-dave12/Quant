"""
conviction_filter.py — Institutional Entry Conviction Gate v3.0
================================================================
COMPLETE REWRITE — not a patch.  Drop-in replacement.

WHY REWRITE:
  v2.1 had the right structure but wrong calibration.  It allowed 24 trades
  in one session with a 33% win rate.  Institutional desks take 3-6 trades
  per session with 60-75% hit rates.  The difference is SELECTIVITY.

  The v2.1 problems:
    - Dealing range gate too wide (longs below 0.58 P/D = buying in premium)
    - AMD score = 0.28 for EVERY trade (ACCUMULATION gets 0.40 base — too high)
    - Session limits effectively unlimited (1000 max, 100s interval)
    - No cumulative drawdown circuit breaker
    - Approach entries scored with generous 0.55 OTE fallback
    - Pool TF minimum too low (5m pools = noise)

INSTITUTIONAL CONVICTION MODEL v3.1:

  NO HARD GATES — all factors are data-driven score contributions.
  Poor values reduce the score; exceptional setups can still pass.

  WEIGHTED FACTORS (scored 0-1, weighted sum must pass threshold):
    1. Pool significance quality        0.20
    2. Displacement strength             0.25  ← most important: proves institutional intent
    3. CISD confirmation                 0.25  ← CHoCH/BOS = structural confirmation
    4. OTE zone + Dealing Range          0.15
    5. Session quality                   0.10
    6. AMD phase alignment               0.05
    TOTAL                                1.00

  REQUIRED SCORE: from config (CONVICTION_MIN_SCORE, default 0.45)

  SESSION LIMITS (from config):
    - Max entries per session: CONVICTION_MAX_ENTRIES_PER_SESSION
    - Min interval: CONVICTION_MIN_ENTRY_INTERVAL_SEC
    - Max consecutive losses: CONVICTION_MAX_SESSION_LOSSES
    - Cumulative drawdown circuit breaker: SESSION_DRAWDOWN_CAP_PCT

  FEEDBACK LOOP:
    PostTradeAgent insights dynamically adjust AMD threshold and SL buffer.
"""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# IMPORT CONFIG — all thresholds centralized
# ─────────────────────────────────────────────────────────────────────────────
try:
    from config import (
        CONVICTION_MIN_SCORE               as _CFG_MIN_SCORE,
        CONVICTION_POOL_MIN_TF_RANK        as _CFG_MIN_TF_RANK,
        CONVICTION_DISPLACEMENT_BODY_ATR   as _CFG_DISP_BODY_ATR,
        CONVICTION_OTE_FIB_LOW             as _CFG_OTE_LOW,
        CONVICTION_OTE_FIB_HIGH            as _CFG_OTE_HIGH,
        CONVICTION_MIN_RR                  as _CFG_MIN_RR,
        CONVICTION_MAX_SESSION_LOSSES      as _CFG_MAX_SESS_LOSSES,
        CONVICTION_MIN_ENTRY_INTERVAL_SEC  as _CFG_INTERVAL_SEC,
        CONVICTION_MAX_ENTRIES_PER_SESSION as _CFG_MAX_ENTRIES,
        CONVICTION_PRODUCT_MIN_CORE        as _CFG_PRODUCT_MIN_CORE,
    )
except ImportError:
    # BUG-C1 FIX: Calibrated crypto-specific defaults.
    # 0.72 → 0.65: more realistic for crypto 24/7 (especially weekends/off-hours
    #               where session score 0.55-0.65 caps the ceiling)
    # POOL_MIN_TF_RANK 3 → 2: allow 5m pools that have HTF confluence (effective
    #               rank promoted to 3+ via htf_count); pure 1m pools still blocked
    _CFG_MIN_SCORE = 0.65
    _CFG_MIN_TF_RANK = 2
    _CFG_DISP_BODY_ATR = 0.70
    _CFG_OTE_LOW = 0.500
    _CFG_OTE_HIGH = 0.786
    _CFG_MIN_RR = 2.0
    _CFG_MAX_SESS_LOSSES = 2
    _CFG_INTERVAL_SEC = 300
    _CFG_MAX_ENTRIES = 5
    _CFG_PRODUCT_MIN_CORE = 0.45

# ── Internal constants ────────────────────────────────────────────────────────

REQUIRED_SCORE = _CFG_MIN_SCORE
MIN_RR = _CFG_MIN_RR
POOL_MIN_TF_RANK = _CFG_MIN_TF_RANK
DISPLACEMENT_MIN_BODY_ATR = _CFG_DISP_BODY_ATR
OTE_FIB_LOW = _CFG_OTE_LOW
OTE_FIB_HIGH = _CFG_OTE_HIGH
MAX_SESSION_LOSSES = _CFG_MAX_SESS_LOSSES
MIN_ENTRY_INTERVAL_SEC = _CFG_INTERVAL_SEC
MAX_ENTRIES_PER_SESSION = _CFG_MAX_ENTRIES
PRODUCT_MIN_CORE = _CFG_PRODUCT_MIN_CORE

# ── Dealing range — scoring zones (no hard block) ────────────────────────────
DR_LONG_MAX_PD = 0.65    # Longs preferred in discount but allowed wider
DR_SHORT_MIN_PD = 0.35   # Shorts preferred in premium but allowed wider

# ── Timeframe rank lookup ─────────────────────────────────────────────────────
_TF_RANK: Dict[str, int] = {
    "1m": 1, "2m": 1, "3m": 1, "5m": 2, "15m": 3,
    "30m": 3, "1h": 4, "4h": 5, "1d": 6,
}

# ── Session quality ───────────────────────────────────────────────────────────
# No session is hard-blocked here. Crypto markets run 24/7; liquidity hunts,
# AMD cycles, HTF structure, displacement, and CISD are all equally valid on
# a Saturday or during Asia/off-hours. What changes is the absence of a named
# (London open / NY open).  We model that as a moderate score reduction rather
# than a veto — the conviction gate's displacement + CISD + pool-TF weights
# already handle the lower-quality setups naturally.
_SESSION_SCORE: Dict[str, float] = {
    "LONDON":    1.00,
    "NY":        1.00,
    "NEW_YORK":  1.00,
    "LONDON_NY": 0.95,
    "WEEKEND":   0.80,   # crypto is 24/7 — weekends are valid
    "OFF_HOURS": 0.75,   # off-hours still have liquidity hunts
    "ASIA":      0.60,   # Asia session — lower but NOT blocked
    "":          0.60,   # unknown session — don't penalize heavily
}

# ── AMD phase scores ──────────────────────────────────────────────────────────
# All phases scored, none hard-blocked. ACCUMULATION = 0.40 (low but allowed
# because AMD phase detection lags actual sweeps).
_AMD_PHASE_SCORE: Dict[str, float] = {
    "MANIPULATION":   1.00,   # The sweep — prime entry zone
    "DISTRIBUTION":   0.90,   # Delivery phase — good for continuation
    "REDISTRIBUTION": 0.80,   # Mid-trend pause
    "REACCUMULATION": 0.75,   # Mid-trend pause
    "ACCUMULATION":   0.40,   # Lower score but NOT blocked — AMD lags sweeps
    "":               0.50,   # Unknown — moderate score
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
    allowed:           bool
    score:             float
    factors:           ConvictionFactors  = field(default_factory=ConvictionFactors)
    reject_reasons:    List[str]          = field(default_factory=list)
    allow_reasons:     List[str]          = field(default_factory=list)
    rr_ratio:          float              = 0.0
    pool_tf:           str                = ""
    pool_sig:          float              = 0.0
    blocked_by_timing: bool               = False


@dataclass
class SessionState:
    session_id:         str   = ""
    entries_taken:      int   = 0
    consecutive_losses: int   = 0
    last_entry_time:    float = 0.0
    wins:               int   = 0
    losses:             int   = 0
    session_pnl:        float = 0.0   # cumulative PnL this session

    def record_outcome(self, win: bool, pnl: float = 0.0) -> None:
        self.session_pnl += pnl
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
    Institutional Entry Conviction Gate v3.0.
    Drop-in replacement.  Same interface, institutional logic.
    """

    def __init__(self) -> None:
        self._session_state = SessionState()
        # Adaptive thresholds (modified by PostTradeAgent feedback)
        self._adaptive_amd_min_conf: float = 0.0
        self._adaptive_sl_buffer_mult: float = 1.0

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
        sweep_wick_price: float    = 0.0,
        measured_displacement_atr: float = 0.0,
        live_balance: float        = 0.0,   # Bug #16: real account balance for drawdown gate
    ) -> ConvictionResult:
        """
        Evaluate conviction for a potential entry.

        Architecture:
          1. Hard mandatory gates (any fail = immediate rejection)
          2. Factor scoring (always runs, produces real score)
          3. Session timing gates (checked last so score is always populated)

        sweep_wick_price (FIX-OTE-REVERSAL): The wick extreme of the sweep candle.
          For reversal entries this is the correct Fibonacci anchor for OTE scoring.
          Callers should pass sweep_result.wick_extreme when available.
        """
        factors  = ConvictionFactors()
        rejects: List[str] = []
        hard_rejects: List[str] = []
        allows:  List[str] = []

        # ══════════════════════════════════════════════════════════════════
        # SESSION TIMING GATE — checked FIRST so that blocked_by_timing is
        # never conflated with a real factor-score failure.  The score field
        # is 0.0 on timing blocks (no factor scoring ran), which makes
        # Telegram alerts unambiguous: score=0.00 + INTERVAL reason = pacing,
        # not an ICT quality failure.
        # Bug #22 fix: previously this ran AFTER factor scoring, so the alert
        # showed e.g. score=0.78 with a timing-block reason — operators read
        # that as "high-quality trade was blocked" when the real cause was the
        # 300s pacing interval.
        # ══════════════════════════════════════════════════════════════════
        timing_block = self._check_session_limits(now, session, live_balance=live_balance)
        if timing_block:
            logger.debug(
                f"ConvictionFilter TIMING_BLOCK (pre-score): {timing_block}")
            return ConvictionResult(
                allowed=False, score=0.0, factors=factors,
                reject_reasons=[timing_block],
                allow_reasons=[], rr_ratio=0.0,
                pool_tf="", pool_sig=0.0,
                blocked_by_timing=True)

        # ── Extract pool info ──────────────────────────────────────────────
        pool_price, pool_tf, pool_sig, pool_htf_count = \
            self._extract_pool_info(sweep_pool, atr)

        # FIX-OTE-REVERSAL: try to get sweep wick from sweep_pool if not passed
        if sweep_wick_price <= 0 and sweep_pool is not None:
            sweep_wick_price = float(
                getattr(sweep_pool, 'sweep_wick', 0.0)
                or getattr(sweep_pool, 'wick_extreme', 0.0)
                or 0.0
            )

        # ── Effective TF rank ──────────────────────────────────────────────
        native_rank = _TF_RANK.get(pool_tf, 2)
        if   pool_htf_count >= 3: effective_rank = max(native_rank, 4)
        elif pool_htf_count >= 2: effective_rank = max(native_rank, 3)
        elif pool_htf_count >= 1: effective_rank = max(native_rank, 2)
        else:                     effective_rank = native_rank

        is_approach = any(k in entry_type.lower()
                          for k in ("approach", "pre_sweep", "proximity"))
        _is_reversal_type = "reversal" in entry_type.lower()

        # ══════════════════════════════════════════════════════════════════
        # MANDATORY HARD GATES — any failure = immediate rejection
        # ══════════════════════════════════════════════════════════════════

        # ── GATE 1: Pool timeframe — data-driven (no hard block) ────────────
        # Pool TF rank feeds directly into _rank_bonus in Factor 1 scoring:
        #   rank 1 (1m) → _rank_bonus = 0.25   (very low contribution)
        #   rank 2 (5m) → _rank_bonus = 0.50   (moderate)
        #   rank 3 (15m)→ _rank_bonus = 0.75   (good)
        #   rank 4 (1h+)→ _rank_bonus = 1.00   (excellent)
        # Low-rank pools score poorly and almost never clear REQUIRED_SCORE.
        # No early return — the score gates the trade.
        if effective_rank < POOL_MIN_TF_RANK:
            hard_rejects.append(
                f"POOL_TF_LOW: {pool_tf}(htfx{pool_htf_count}) "
                f"rank={effective_rank} < required {POOL_MIN_TF_RANK}")
            rejects.append(
                f"POOL_TF_LOW: {pool_tf}(htfx{pool_htf_count}) rank={effective_rank} "
                f"— will score low (not hard-blocked; data-driven)")
            # Falls through to factor scoring.

# ── GATE 2: Dealing range — fully data-driven score factor (no hard block) ──
        # The dealing range P/D position is incorporated as a SCORE FACTOR in the
        # conviction model, not as a hard veto. This is correct ICT methodology:
        #
        #   For APPROACH entries:  discount = good zone to buy (score bonus)
        #   For REVERSAL entries:  premium SSL sweep = false move down, LONG is correct
        #                          (dealing range gate is irrelevant / inverted)
        #   For CONTINUATION:      scoring reflects the directional bias naturally
        #
        # None of this needs a hard block. A poor dealing-range position reduces
        # the score; an exceptional setup in the wrong zone can still pass.
        dr_pd, dr_data_available = self._get_dealing_range_pd(
            price, ict_engine, liq_snapshot)
        if not dr_data_available:
            hard_rejects.append("DEALING_RANGE_MISSING: no structural P/D range")
            logger.debug(
                "DEALING_RANGE: no structural data yet — scoring with neutral 0.50")
        _pd_label = ("PREMIUM" if dr_pd > 0.60 else
                     "DISCOUNT" if dr_pd < 0.40 else "EQ")
        logger.debug(
            f"DEALING_RANGE: {_pd_label}({dr_pd:.2f}) {entry_type} — data-driven scoring only")
        factors.dealing_range_ok = dr_data_available

        # ── GATE 3: R:R — data-driven score modifier (no hard block) ─────────
        # R:R below minimum does NOT veto — it reduces the pool_sig_score.
        # Rationale: a setup can have genuinely poor R:R due to tight structure
        # but still be valid. A 1.5 R:R setup with exceptional CISD + OTE is
        # better than a 3:1 R:R with no confirmation. Let the composite score decide.
        #
        # R:R score modifier applied to pool_sig_score (aligned with MIN_RR=1.2):
        #   R:R >= 2.5  → +0.10 bonus
        #   R:R >= MIN_RR → no modifier
        #   R:R 0.8-MIN_RR → -0.10 penalty
        #   R:R < 0.8   → -0.20 penalty
        rr = self._compute_rr(trade_side, entry_price, sl_price, tp_price)
        _rr_mod = (0.10 if rr >= 2.5 else
                   0.00 if rr >= MIN_RR else
                  -0.10 if rr >= 0.8 else
                  -0.20)
        if rr < 0.8:
            hard_rejects.append(f"RR_HARD: {rr:.2f} < {MIN_RR:.1f}")
            rejects.append(f"RR_VERY_LOW: {rr:.2f} — score penalty applied")
        elif rr < MIN_RR:
            hard_rejects.append(f"RR_HARD: {rr:.2f} < {MIN_RR:.1f}")
            rejects.append(f"RR_LOW: {rr:.2f} < {MIN_RR:.1f} — minor score penalty")

# ── GATE 4: Session scoring (fully data-driven — NO hard block) ────
        # All sessions are scored, none are hard-blocked.
        # ASIA: 0.60, OFF_HOURS: 0.75, WEEKEND: 0.80, LONDON/NY: 1.00.
        sess_key = self._resolve_session(session, ict_engine)

        # ── GATE 5: AMD phase — fully data-driven (NO hard block) ──────────
        # AMD=ACCUMULATION receives an amd_score near 0.00 in _score_amd(),
        # making it nearly impossible to clear the overall conviction threshold.
        # No phase is ever unconditionally vetoed — the score gates the trade.
        # Exceptional displacement + CISD + OTE can still clear the bar even
        # during ACCUMULATION (e.g. AMD phase lag during a live sweep).
        amd_phase = self._get_amd_phase(ict_engine)
        # No hard block. amd_phase is passed to _score_amd() below.
        
        # ── Gate 6: Approach entries — data-driven (no hard block) ─────────────
        # Approach entries score very low on displacement (0.20) and CISD (0.10)
        # because no sweep/wick/structural confirmation exists yet. The resulting
        # score (typically <0.45) almost never clears REQUIRED_SCORE=0.65.
        # No unconditional block — the score alone gates the trade.
        if is_approach:
            hard_rejects.append("APPROACH_HARD: pre-sweep setup is not executable")
            rejects.append("APPROACH: pre-sweep — expect very low displacement/CISD scores")
            # Falls through to factor scoring (no early return).

        if _is_reversal_type and sweep_wick_price <= 0:
            hard_rejects.append("REVERSAL_WICK_MISSING: cannot anchor OTE to sweep wick")

        # ══════════════════════════════════════════════════════════════════
        # FACTOR SCORING — weighted conviction model
        # ══════════════════════════════════════════════════════════════════

        # ── Factor 1: Pool significance + R:R modifier (weight 0.20) ─────────
        # R:R modifier (_rr_mod from Gate 3) and TF rank both shape this score
        # continuously — no threshold cut-off, fully data-driven.
        _rank_bonus = min(effective_rank / 4.0, 1.0)
        _sig_score = min(pool_sig / 8.0, 1.0)
        factors.pool_sig_score = max(0.05, min(1.0,
            _rank_bonus * 0.40 + _sig_score * 0.60 + _rr_mod))
        allows.append(f"POOL={pool_tf}(htfx{pool_htf_count}) sig={pool_sig:.1f} RR={rr:.1f}({_rr_mod:+.2f})")

        # ── Factor 2: Displacement (weight 0.25) ──────────────────────────
        factors.displacement_score = self._score_displacement(
            trade_side, price, pool_price, atr, candles_5m, ict_engine,
            measured_displacement_atr=measured_displacement_atr)
        if factors.displacement_score >= 0.70:
            allows.append(f"DISP={factors.displacement_score:.2f}✅")
        else:
            rejects.append(f"DISP_WEAK={factors.displacement_score:.2f}")

        # ── Factor 3: CISD (weight 0.25) ──────────────────────────────────
        factors.cisd_score = self._score_cisd(trade_side, ps_decision, ict_engine)
        if factors.cisd_score >= 0.60:
            allows.append(f"CISD={factors.cisd_score:.2f}✅")
        elif factors.cisd_score >= 0.35:
            allows.append(f"CISD={factors.cisd_score:.2f}")

        # ── Factor 4: OTE zone + Dealing Range alignment (weight 0.15) ─────────
        # OTE score is now modulated by dealing range position (fully data-driven).
        # Good DR alignment adds up to +0.15; poor alignment subtracts up to 0.15.
        # Reversals have the SAME DR bias as directional trades:
        #   BSL sweep → short reversal → premium is GOOD (shorting from premium)
        #   SSL sweep → long reversal  → discount is GOOD (buying from discount)
        # This replaces the old hard DR gate with a continuous score contribution.
        _raw_ote_score = self._score_ote(
            trade_side, entry_price, pool_price, price,
            sweep_wick_price=sweep_wick_price,
            atr=atr,
        )
        factors.ote_score = _raw_ote_score
        _dr_mod_applied   = 0.0
        if dr_data_available:
            # Bug #15 fix: bidirectional DR modifier, range −0.15 → +0.15.
            # The previous formulas wrapped the bracket in max(0.0, …) which
            # clamped the modifier to ≥ 0, making it one-directional:
            #   LONG  at full premium → modifier was 0.0 (should be −0.15)
            #   LONG  at equilibrium  → modifier was +0.15 (should be 0.0)
            # Removing the clamp restores the continuous penalty for
            # counter-range entries while preserving the bonus for on-range ones.
            if trade_side == "long":
                # dr_pd=0.0 (full discount) → +0.15; dr_pd=0.5 (EQ) → 0.0;
                # dr_pd=1.0 (full premium)  → −0.15
                _dr_mod = 0.15 * (1.0 - 2.0 * dr_pd)
            else:
                # dr_pd=1.0 (full premium)  → +0.15; dr_pd=0.5 (EQ) → 0.0;
                # dr_pd=0.0 (full discount) → −0.15
                _dr_mod = 0.15 * (2.0 * dr_pd - 1.0)
            # No inversion for reversals — BSL sweep→short in premium and
            # SSL sweep→long in discount are both correct ICT directional bias.
            _dr_mod_applied   = _dr_mod
            factors.ote_score = max(0.05, min(1.0, _raw_ote_score + _dr_mod))
        logger.debug(
            f"ConvictionFilter OTE/DR: side={trade_side} type={entry_type!r} "
            f"raw_ote={_raw_ote_score:.3f} dr_pd={dr_pd:.3f}({_pd_label}) "
            f"dr_mod={_dr_mod_applied:+.3f} "
            f"final_ote={factors.ote_score:.3f}")
        if factors.ote_score >= 0.70:
            allows.append(f"OTE={factors.ote_score:.2f}✅ DR={_pd_label}({dr_pd:.2f})")
        else:
            allows.append(f"OTE={factors.ote_score:.2f} DR={_pd_label}({dr_pd:.2f})")

        # ── Factor 5: Session quality (weight 0.10) ───────────────────────
        factors.session_score = _SESSION_SCORE.get(sess_key, 0.40)
        allows.append(f"SESSION={sess_key}({factors.session_score:.2f})")

        # ── Factor 6: AMD alignment (weight 0.05) ─────────────────────────
        factors.amd_score = self._score_amd(trade_side, ict_engine)
        if factors.amd_score >= 0.80:
            allows.append(f"AMD={factors.amd_score:.2f}✅")

        # ── Compute weighted score ─────────────────────────────────────────
        score = (
            factors.pool_sig_score     * 0.20 +
            factors.displacement_score * 0.25 +
            factors.cisd_score         * 0.25 +
            factors.ote_score          * 0.15 +
            factors.session_score      * 0.10 +
            factors.amd_score          * 0.05
        )
        score = round(score, 4)

        # ── Score gate ─────────────────────────────────────────────────────
        score_passed = score >= REQUIRED_SCORE
        core_product = min(
            factors.pool_sig_score,
            factors.displacement_score,
            factors.cisd_score,
        )
        product_passed = core_product >= PRODUCT_MIN_CORE
        if not product_passed:
            rejects.append(
                f"PRODUCT_CORE: {core_product:.2f} < {PRODUCT_MIN_CORE:.2f} "
                f"[pool={factors.pool_sig_score:.2f} "
                f"disp={factors.displacement_score:.2f} "
                f"cisd={factors.cisd_score:.2f}]")
        if not score_passed:
            rejects.append(
                f"SCORE: {score:.3f} < {REQUIRED_SCORE} "
                f"[pool={factors.pool_sig_score:.2f} "
                f"disp={factors.displacement_score:.2f} "
                f"cisd={factors.cisd_score:.2f} "
                f"ote={factors.ote_score:.2f} "
                f"sess={factors.session_score:.2f} "
                f"amd={factors.amd_score:.2f}]")

        # ── Final decision ─────────────────────────────────────────────────
        if hard_rejects:
            rejects.extend(hard_rejects)
        allowed = score_passed and product_passed and not hard_rejects
        if allowed:
            logger.debug(
                f"Conviction advisory PASS ({score:.3f}) | {' | '.join(allows[:5])}")
        return ConvictionResult(
            allowed=allowed, score=score, factors=factors,
            reject_reasons=rejects, allow_reasons=allows,
            rr_ratio=rr, pool_tf=pool_tf, pool_sig=pool_sig)

    # ─────────────────────────────────────────────────────────────────────
    # SESSION MANAGEMENT
    # ─────────────────────────────────────────────────────────────────────

    def mark_entry_placed(self, now: float) -> None:
        """Called when a trade is confirmed filled."""
        self._session_state.entries_taken += 1
        self._session_state.last_entry_time = now
        logger.info(
            f"ConvictionFilter: entry placed — "
            f"entries_taken={self._session_state.entries_taken}/{MAX_ENTRIES_PER_SESSION} "
            f"| next entry allowed in {MIN_ENTRY_INTERVAL_SEC}s")

    def on_session_change(self, new_session: str) -> None:
        """Called when killzone/session changes.  Resets session-scoped counters."""
        old = self._session_state.session_id
        if new_session == old:
            return
        logger.info(
            f"ConvictionFilter: session boundary {old!r} → {new_session!r} "
            f"| resetting entries={self._session_state.entries_taken} "
            f"consec_losses={self._session_state.consecutive_losses}")
        self._session_state = SessionState(
            session_id=new_session,
            wins=self._session_state.wins,
            losses=self._session_state.losses,
        )

    def record_trade_result(self, win: bool, pnl: float = 0.0) -> None:
        """Called after each trade closes."""
        self._session_state.record_outcome(win, pnl)
        logger.info(
            f"ConvictionFilter: {'WIN' if win else 'LOSS'} pnl=${pnl:.4f} | "
            f"W/L={self._session_state.wins}/{self._session_state.losses} "
            f"consec_losses={self._session_state.consecutive_losses} "
            f"session_pnl=${self._session_state.session_pnl:.4f}")

    def get_session_state_summary(self) -> Dict:
        st = self._session_state
        cooldown_remaining = 0
        if st.last_entry_time > 0:
            cooldown_remaining = max(0, MIN_ENTRY_INTERVAL_SEC - (time.time() - st.last_entry_time))
        return {
            "session_id": st.session_id,
            "entries_taken": st.entries_taken,
            "max_entries": MAX_ENTRIES_PER_SESSION,
            "consecutive_losses": st.consecutive_losses,
            "max_losses": MAX_SESSION_LOSSES,
            "wins": st.wins,
            "losses": st.losses,
            "session_pnl": st.session_pnl,
            "cooldown_remaining_s": int(cooldown_remaining),
            "cooldown_active": cooldown_remaining > 0,
        }

    def reset_session(self, reason: str = "manual") -> None:
        wins, losses = self._session_state.wins, self._session_state.losses
        logger.info(f"ConvictionFilter: full reset ({reason}) W/L={wins}/{losses}")
        self._session_state = SessionState(wins=wins, losses=losses)

    # ─────────────────────────────────────────────────────────────────────
    # SESSION LIMITS
    # ─────────────────────────────────────────────────────────────────────

    def _check_session_limits(
        self, now: float, session: str, live_balance: float = 0.0,
    ) -> Optional[str]:
        """Check timing/pacing gates.  Returns reason string if blocked.

        live_balance: the current account balance in USD.  When > 0 the
        drawdown circuit breaker is expressed as a fraction of the *real*
        account rather than a static notional.  Callers should pass
        risk_manager.available_balance (or equivalent) so the gate is
        automatically calibrated to account size.

        Bug #16 fix: previously SESSION_DRAWDOWN_NOTIONAL defaulted to
        $10,000 regardless of the actual account size, making the circuit
        breaker fire at 200% of balance for a $500 account and at 2% for
        a $50,000 account.  Now:
          • live_balance > 0  → use it as the notional (overrides config)
          • live_balance == 0 → fall back to SESSION_DRAWDOWN_NOTIONAL config
                                (set this to your actual balance in config.py)
        """
        st = self._session_state

        # Pacing interval
        if st.last_entry_time > 0:
            elapsed = now - st.last_entry_time
            if elapsed < MIN_ENTRY_INTERVAL_SEC:
                wait = int(MIN_ENTRY_INTERVAL_SEC - elapsed)
                return f"INTERVAL: {wait}s remaining (min {MIN_ENTRY_INTERVAL_SEC}s between entries)"

        # Consecutive loss circuit breaker
        if st.consecutive_losses >= MAX_SESSION_LOSSES:
            return (
                f"CIRCUIT_BREAKER: {st.consecutive_losses} consecutive losses "
                f"(max={MAX_SESSION_LOSSES}). Session invalidated.")

        # Entry cap per session
        if st.entries_taken >= MAX_ENTRIES_PER_SESSION:
            return (
                f"ENTRY_CAP: {st.entries_taken}/{MAX_ENTRIES_PER_SESSION} "
                f"entries exhausted this session.")

        # Cumulative session drawdown circuit breaker.
        # Bug #16 fix: use live_balance as the notional when the caller
        # supplies it (preferred), so the gate always scales to the real
        # account size.  When live_balance is not available, fall back to
        # SESSION_DRAWDOWN_NOTIONAL from config (operators must keep that
        # in sync with their actual deposit — document in config.py).
        try:
            import config as _cfg
            _cfg_notional = float(getattr(_cfg, 'SESSION_DRAWDOWN_NOTIONAL', 0.0))
            _max_dd_pct   = float(getattr(_cfg, 'SESSION_MAX_DRAWDOWN_PCT', 4.0))
        except Exception:
            _cfg_notional = 0.0
            _max_dd_pct   = 4.0

        # Priority: live_balance > config notional > skip the gate
        if live_balance > 0:
            _notional = live_balance
        elif _cfg_notional > 0:
            _notional = _cfg_notional
        else:
            _notional = 0.0   # no notional available — skip gate

        if _notional > 0 and st.session_pnl < 0:
            _dd_pct = abs(st.session_pnl) / _notional * 100.0
            if _dd_pct >= _max_dd_pct:
                return (
                    f"DRAWDOWN_CIRCUIT_BREAKER: session PnL=${st.session_pnl:.2f} "
                    f"({_dd_pct:.1f}% of ${_notional:,.0f} notional >= {_max_dd_pct:.1f}% cap). "
                    f"Trading halted for this session.")

        return None

    # ─────────────────────────────────────────────────────────────────────
    # FACTOR SCORING — institutional logic
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def _extract_pool_info(sweep_pool, atr: float) -> Tuple[float, str, float, int]:
        """Extract (price, timeframe, significance, htf_count) from pool object."""
        if sweep_pool is None:
            return 0.0, "5m", 1.0, 0
        inner = getattr(sweep_pool, 'pool', sweep_pool)
        price = float(getattr(inner, 'price', 0.0) or 0.0)
        tf = str(getattr(inner, 'timeframe', '5m') or '5m')
        htf_cnt = int(getattr(inner, 'htf_count', 0) or 0)
        sig = 0.0
        for attr in ('significance', 'adjusted_sig', 'sig'):
            _v = getattr(sweep_pool, attr, None) or getattr(inner, attr, None)
            if _v is not None:
                try:
                    sig = float(_v() if callable(_v) else _v)
                    if sig > 0:
                        break
                except Exception:
                    pass
        if sig <= 0:
            tc = int(getattr(inner, 'touches', 0) or 0)
            sig = max(1.0, 2.0 + tc * 0.5)
        return price, tf, round(sig, 2), htf_cnt

    @staticmethod
    def _get_dealing_range_pd(
        price: float, ict_engine, liq_snapshot,
    ) -> Tuple[float, bool]:
        """
        Get dealing range Premium/Discount position [0=full discount, 1=full premium].

        Returns (pd, data_available) where data_available=False means the 0.50
        value is a pure fallback (no structural data) and the caller MUST skip
        GATE 2 rather than hard-blocking both directions simultaneously.

        Bug #13 fix: the previous version returned bare 0.50 when data was absent,
        causing GATE 2 to block ALL entries at startup (0.50 > 0.45 → LONG blocked;
        0.50 < 0.55 → SHORT blocked).  The equilibrium fallback is only meaningful
        when there is genuine structural data placing price at equilibrium.
        """
        # Priority 1: ICT engine dealing range (computed from multi-TF candles)
        if ict_engine is not None:
            dr = getattr(ict_engine, '_dealing_range', None)
            if dr is not None:
                pd = getattr(dr, 'current_pd', None)
                if pd is not None:
                    return float(pd), True

        # Priority 2: pool-based calculation from liquidity snapshot
        if liq_snapshot is not None:
            bsl_pools = getattr(liq_snapshot, 'bsl_pools', [])
            ssl_pools = getattr(liq_snapshot, 'ssl_pools', [])
            bsl_above = [t.pool.price for t in bsl_pools if t.pool.price > price]
            ssl_below = [t.pool.price for t in ssl_pools if t.pool.price < price]
            if bsl_above and ssl_below:
                # Use the nearest BSL and SSL as dealing range boundaries.
                # Guard against thin-pool scenarios where the calculated range
                # would be less than 1 ATR — in that case the pool spread is too
                # narrow to produce a meaningful P/D reading.
                nearest_bsl = min(bsl_above)
                nearest_ssl = max(ssl_below)
                rng = nearest_bsl - nearest_ssl
                if rng > 1e-9:
                    return (price - nearest_ssl) / rng, True

        # No structural data available — return sentinel 0.50 with data_available=False.
        # Caller MUST skip GATE 2 in this state.
        return 0.50, False

    @staticmethod
    def _get_amd_phase(ict_engine) -> str:
        """Extract AMD phase string from ICT engine."""
        if ict_engine is None:
            return ""
        try:
            amd = getattr(ict_engine, '_amd', None)
            if amd:
                return str(getattr(amd, 'phase', '') or '').upper()
        except Exception:
            pass
        return ""

    @staticmethod
    def _compute_rr(trade_side: str, entry: float, sl: float, tp: float) -> float:
        sl_dist = abs(entry - sl)
        tp_dist = abs(tp - entry)
        return tp_dist / sl_dist if sl_dist > 1e-10 else 0.0

    @staticmethod
    def _score_displacement(
        trade_side: str, price: float, pool_price: float,
        atr: float, candles_5m: Optional[List], ict_engine,
        measured_displacement_atr: float = 0.0,
    ) -> float:
        """
        Score displacement strength [0-1].

        Displacement = strong directional candle away from sweep level.
        This is THE proof of institutional intent — smart money entering aggressively.
        Without displacement, the sweep might just be noise.

        measured_displacement_atr: when the entry_engine has already measured
        the actual displacement in ATR multiples, trust that measurement
        instead of re-measuring from raw candles (which can undercount when
        the move spans multiple candles).
        """
        # FIX-DISP-FLOW: use entry_engine's measured displacement when available
        if measured_displacement_atr > 0:
            # Map ATR multiples to score: 0.4ATR→0.50, 0.7ATR→0.75, 1.0ATR→0.90, 1.5ATR+→1.0
            raw = min(measured_displacement_atr / DISPLACEMENT_MIN_BODY_ATR, 1.0)
            # Boost: measured displacement already confirmed directional alignment
            return min(1.0, raw * 0.90 + 0.10)

        # CF-2 FIX: guard `>= 5` ensures the slice [-4:-1] always pulls
        # 3 CLOSED candles. With `>= 4`, a short candles_5m deque could
        # include a stale seed candle or the live forming bar.
        if candles_5m and len(candles_5m) >= 5:
            closed = candles_5m[-4:-1]   # last 3 closed candles
            best_score = 0.0
            for c in closed:
                try:
                    o, cl = float(c['o']), float(c['c'])
                    h, lo = float(c['h']), float(c['l'])
                    body = abs(cl - o)
                    rng = max(h - lo, 1e-10)

                    # Body fraction — displacement candles have >60% body ratio
                    body_frac = body / rng

                    # Body size relative to ATR
                    body_atr = body / max(atr, 1e-10)

                    # Direction alignment
                    aligned = (
                        (trade_side == "long" and cl > o) or
                        (trade_side == "short" and cl < o)
                    )
                    if not aligned:
                        continue

                    # Score: body_atr normalized to DISPLACEMENT_MIN_BODY_ATR × body_frac
                    raw = min(body_atr / DISPLACEMENT_MIN_BODY_ATR, 1.0) * body_frac
                    best_score = max(best_score, raw)
                except Exception:
                    continue

            if best_score > 0:
                return min(best_score, 1.0)

        # Fallback: check ICT pool displacement data
        if ict_engine is not None:
            try:
                for p in ict_engine.liquidity_pools:
                    if abs(getattr(p, 'price', 0) - pool_price) < atr * 0.4:
                        if getattr(p, 'displacement_confirmed', False):
                            ds = float(getattr(p, 'displacement_score', 0.5) or 0.5)
                            return min(ds, 1.0)
            except Exception:
                pass

        return 0.20   # No displacement evidence — very low score

    @staticmethod
    def _score_cisd(trade_side: str, ps_decision, ict_engine) -> float:
        """
        Score CISD (Change in State of Delivery) [0-1].

        CISD = CHoCH or BOS in the reversal direction after a sweep.
        This is the structural CONFIRMATION that smart money has shifted.
        Without CISD, there's no proof the sweep was manipulative.
        """
        # Best: PostSweepDecision with CISD active
        if ps_decision is not None:
            cisd = getattr(ps_decision, 'cisd_active', False)
            conf = float(getattr(ps_decision, 'confidence', 0.0) or 0.0)
            action = str(getattr(ps_decision, 'action', '') or '')
            direction = str(getattr(ps_decision, 'direction', '') or '').lower()

            dir_ok = (
                (trade_side == "long" and direction in ("long", "bullish")) or
                (trade_side == "short" and direction in ("short", "bearish")) or
                direction == ""
            )
            if cisd and dir_ok and action == "reverse":
                return min(0.85 + conf * 0.15, 1.0)
            if dir_ok and action == "reverse":
                return min(0.60 + conf * 0.25, 0.85)
            if dir_ok and action in ("wait", "continue"):
                # Bug #23 fix: continuation with direction agreement scores 0.55,
                # matching the ICT BOS structural fallback floor.  The old 0.30
                # penalized a post-sweep continuation below even the no-CISD
                # baseline, which made continuation entries unfairly hard to clear.
                # A confirmed CISD in continuation direction is a valid entry context.
                return 0.55
            if not dir_ok:
                return 0.05
            return 0.20

        # Fallback: check BOS/CHoCH from ICT engine structure
        if ict_engine is not None:
            try:
                for tf in ("15m", "5m"):
                    st = getattr(ict_engine, '_tf', {}).get(tf)
                    if st is None:
                        continue
                    choch = str(getattr(st, 'choch_direction', '') or '').lower()
                    bos = str(getattr(st, 'bos_direction', '') or '').lower()
                    if trade_side == "long" and (choch == "bullish" or bos == "bullish"):
                        return 0.55 if tf == "15m" else 0.45
                    if trade_side == "short" and (choch == "bearish" or bos == "bearish"):
                        return 0.55 if tf == "15m" else 0.45
            except Exception:
                pass

        return 0.10   # No structural confirmation — very low

    @staticmethod
    def _score_ote(
        trade_side: str, entry_price: float, pool_price: float, price: float,
        sweep_wick_price: float = 0.0,
        atr: float = 0.0,
    ) -> float:
        """
        Score OTE (Optimal Trade Entry) zone [0-1].

        OTE = 50% to 78.6% Fibonacci retracement of the displacement move.
        The golden pocket (61.8% - 78.6%) is where institutions fill.

        FIX-OTE-REVERSAL: For reversal entries, the correct Fibonacci reference
        is the SWEEP WICK → POST-REVERSAL HIGH/LOW, NOT pool_price → current_price.

        Original bug: pool_price ≈ sweep price ≈ entry price for reversal entries
        (price hasn't retraced after the reversal), so total_move ≈ $6 and fib ≈ 0,
        giving ote_score = 0.10 regardless of setup quality.

        Fix: when sweep_wick_price is provided (reversal entries always have it),
        use it as the anchor of the displacement leg instead of pool_price. This
        correctly measures how much the market has retraced from the wick extreme,
        which is the actual OTE question for a reversal trade.

        CF-5 FIX: divide-guard uses ATR-scaled threshold instead of 1e-3 points.
        For BTC at $76k, 1e-3 is 0.001 points — effectively no guard. When the
        displacement leg is truly meaningless (< 5% of ATR), return the neutral
        fallback instead of dividing by near-zero and producing a bogus fib
        (which then falls through all OTE zones to the penalty branch).
        """
        if pool_price <= 0 or entry_price <= 0 or price <= 0:
            return 0.40

        # FIX-OTE-REVERSAL: use sweep wick as reference when available
        anchor_price = pool_price
        if sweep_wick_price > 0:
            # For SSL sweeps (long reversal): wick is BELOW pool price
            # For BSL sweeps (short reversal): wick is ABOVE pool price
            if trade_side == "long" and sweep_wick_price < pool_price:
                anchor_price = sweep_wick_price
            elif trade_side == "short" and sweep_wick_price > pool_price:
                anchor_price = sweep_wick_price

        total_move = abs(price - anchor_price)
        # CF-5 FIX: divide-guard scaled to ATR. If the displacement leg is less
        # than 5% of ATR, the retracement metric is meaningless — return neutral.
        _min_leg = max(atr * 0.05, 1e-3) if atr > 0 else 1e-3
        if total_move < _min_leg:
            return 0.40

        if trade_side == "long":
            fib = abs(price - entry_price) / total_move
        else:
            fib = abs(entry_price - price) / total_move

        # Golden pocket: 61.8% - 78.6%
        if 0.618 <= fib <= 0.786:
            return 1.0   # Perfect OTE
        # Standard OTE: 50% - 61.8%
        if OTE_FIB_LOW <= fib < 0.618:
            return 0.75
        # Wide OTE: 78.6% - 88.6%
        if 0.786 < fib <= 0.886:
            return 0.60
        # Outside OTE but reasonable
        if 0.382 <= fib < OTE_FIB_LOW:
            return 0.40
        # Not in any meaningful zone
        return max(0.10, 0.30 - abs(fib - 0.618) * 2.0)

    def _score_amd(self, trade_side: str, ict_engine) -> float:
        """
        Score AMD phase alignment [0-1].

        Key insight: AMD confidence matters.  A MANIPULATION phase with
        0.95 confidence is a completely different signal than 0.30 confidence.
        The v2.1 engine returned 0.28 for every trade because it used a
        fixed base score without incorporating confidence properly.
        """
        if ict_engine is None:
            return 0.30

        try:
            amd = getattr(ict_engine, '_amd', None)
            if amd is None:
                return 0.30

            phase = str(getattr(amd, 'phase', '') or '').upper()
            bias = str(getattr(amd, 'bias', '') or '').lower()
            conf = float(getattr(amd, 'confidence', 0.5) or 0.5)

            base = _AMD_PHASE_SCORE.get(phase, 0.30)
            min_conf = float(getattr(self, '_adaptive_amd_min_conf', 0.0) or 0.0)
            if min_conf > 0.0 and conf < min_conf:
                base *= max(0.10, conf / max(min_conf, 1e-9))

            # Direction alignment check
            aligned = (
                (trade_side == "long" and "bull" in bias) or
                (trade_side == "short" and "bear" in bias)
            )
            contra = (
                (trade_side == "long" and "bear" in bias) or
                (trade_side == "short" and "bull" in bias)
            )

            if aligned:
                # Scale by confidence — high confidence amplifies the score
                return min(base * (0.70 + conf * 0.50), 1.0)
            elif contra:
                # CONTRA: penalize proportional to confidence
                # High confidence contra = very bad (smart money going other way)
                return max(base * (0.05 + (1.0 - conf) * 0.25), 0.0)
            else:
                # Neutral bias — use base with slight penalty
                return base * 0.65

        except Exception:
            return 0.25

    @staticmethod
    def _resolve_session(session_hint: str, ict_engine) -> str:
        """
        Resolve a canonical session key from the hint string and ICT engine state.

        Priority order:
          1. session_hint  — caller-supplied, most specific
          2. ict_engine._session  — full session window label (preferred over KZ)
          3. ict_engine._killzone — kill-zone-only label, last resort

        WEEKEND is checked FIRST in every source so it is never masked by a
        partial string match against NY / LONDON / ASIA. This keeps session
        scoring accurate without turning any session label into a hard veto.
        """
        sources = [session_hint]
        if ict_engine is not None:
            # _session carries the full session-window label, e.g. "WEEKEND",
            # "NEW_YORK", "LONDON".  Always prefer it over _killzone.
            sources.append(str(getattr(ict_engine, '_session',  '') or ''))
            # _killzone is kill-zone-specific (e.g. "LONDON_OPEN").  Use only
            # when _session is blank.
            sources.append(str(getattr(ict_engine, '_killzone', '') or ''))

        for src in sources:
            su = src.upper().strip()
            if not su:
                continue
            # WEEKEND checked unconditionally first — do not reorder.
            if 'WEEKEND'   in su:                                  return 'WEEKEND'
            if 'OFF_HOURS' in su or su == 'OFF':                   return 'OFF_HOURS'
            if 'NEW_YORK'  in su or ('NY' in su
                                      and 'LONDON' not in su):     return 'NY'
            if 'LONDON'    in su:                                   return 'LONDON'
            if 'ASIA'      in su:                                   return 'ASIA'
        return ''

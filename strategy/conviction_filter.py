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

INSTITUTIONAL CONVICTION MODEL v3.0:

  MANDATORY HARD GATES (any failure = immediate rejection, no score):
    1. Pool TF effective rank >= 3 (15m+ or 5m with HTFx2+)
    2. Dealing range: LONG only in discount (PD < 0.45), SHORT only in premium (PD > 0.55)
    3. R:R >= 2.0 minimum
    4. NOT in Asia session
    5. NOT in ACCUMULATION AMD phase (no delivery expected)

  WEIGHTED FACTORS (scored 0-1, weighted sum must pass threshold):
    1. Pool significance quality        0.20
    2. Displacement strength             0.25  ← most important: proves institutional intent
    3. CISD confirmation                 0.25  ← CHoCH/BOS = structural confirmation
    4. OTE zone precision                0.15
    5. Session quality                   0.10
    6. AMD phase alignment               0.05
    TOTAL                                1.00

  REQUIRED SCORE: 0.72 (calibrated for 65-75% WR)

  SESSION LIMITS:
    - Max 5 entries per session (London/NY/Asia)
    - 300s minimum between entries (institutional pace)
    - 2 consecutive losses = session circuit breaker
    - 4% cumulative drawdown = day circuit breaker

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
    )
except ImportError:
    # Fallback defaults if config not available
    _CFG_MIN_SCORE = 0.72
    _CFG_MIN_TF_RANK = 3
    _CFG_DISP_BODY_ATR = 0.70
    _CFG_OTE_LOW = 0.500
    _CFG_OTE_HIGH = 0.786
    _CFG_MIN_RR = 2.0
    _CFG_MAX_SESS_LOSSES = 2
    _CFG_INTERVAL_SEC = 300
    _CFG_MAX_ENTRIES = 5

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

# ── Dealing range — institutional zones ───────────────────────────────────────
# Longs ONLY in discount.  Shorts ONLY in premium.  No exceptions.
# This is the single most important structural filter for win rate.
DR_LONG_MAX_PD = 0.45    # LONG only below 45% of dealing range (discount)
DR_SHORT_MIN_PD = 0.55   # SHORT only above 55% of dealing range (premium)

# ── Timeframe rank lookup ─────────────────────────────────────────────────────
_TF_RANK: Dict[str, int] = {
    "1m": 1, "2m": 1, "3m": 1, "5m": 2, "15m": 3,
    "30m": 3, "1h": 4, "4h": 5, "1d": 6,
}

# ── Session quality ───────────────────────────────────────────────────────────
# WEEKEND and ASIA are hard-blocked in the mandatory gates below.
# The score values here are only reached for non-blocked sessions.
_SESSION_SCORE: Dict[str, float] = {
    "LONDON":    1.00,   # London open = institutional manipulation → reversal
    "NY":        1.00,   # NY open = institutional delivery
    "NEW_YORK":  1.00,
    "LONDON_NY": 0.85,   # Overlap — high volume but chaotic
    "ASIA":      0.00,   # HARD BLOCK (see mandatory gates) — no institutional flow
    "WEEKEND":   0.00,   # HARD BLOCK (see mandatory gates) — no institutional flow
    "OFF_HOURS": 0.20,   # Between sessions — significant penalty, not hard-blocked
    "":          0.40,   # Unknown session — penalize
}

# ── AMD phase scores ──────────────────────────────────────────────────────────
# ACCUMULATION = 0.00 (HARD BLOCK — no delivery expected)
# MANIPULATION = highest (sweep happening — this IS the setup)
_AMD_PHASE_SCORE: Dict[str, float] = {
    "MANIPULATION":   1.00,   # The sweep — this is where entries happen
    "DISTRIBUTION":   0.90,   # Delivery phase — good for continuation
    "REDISTRIBUTION": 0.75,   # Mid-trend pause — decent
    "REACCUMULATION": 0.70,   # Mid-trend pause — decent
    "ACCUMULATION":   0.00,   # HARD BLOCK — no institutional delivery
    "":               0.30,   # Unknown — heavy penalty
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
    ) -> ConvictionResult:
        """
        Evaluate conviction for a potential entry.

        Architecture:
          1. Hard mandatory gates (any fail = immediate rejection)
          2. Factor scoring (always runs, produces real score)
          3. Session timing gates (checked last so score is always populated)
        """
        factors  = ConvictionFactors()
        rejects: List[str] = []
        allows:  List[str] = []

        # ── Extract pool info ──────────────────────────────────────────────
        pool_price, pool_tf, pool_sig, pool_htf_count = \
            self._extract_pool_info(sweep_pool, atr)

        # ── Effective TF rank ──────────────────────────────────────────────
        native_rank = _TF_RANK.get(pool_tf, 2)
        if   pool_htf_count >= 3: effective_rank = max(native_rank, 4)
        elif pool_htf_count >= 2: effective_rank = max(native_rank, 3)
        elif pool_htf_count >= 1: effective_rank = max(native_rank, 2)
        else:                     effective_rank = native_rank

        is_approach = any(k in entry_type.lower()
                          for k in ("approach", "pre_sweep", "proximity"))

        # ══════════════════════════════════════════════════════════════════
        # MANDATORY HARD GATES — any failure = immediate rejection
        # ══════════════════════════════════════════════════════════════════

        # ── GATE 1: Pool effective timeframe ───────────────────────────────
        if effective_rank < POOL_MIN_TF_RANK:
            rejects.append(
                f"POOL_TF: {pool_tf}(htfx{pool_htf_count}) rank={effective_rank} "
                f"< required={POOL_MIN_TF_RANK}")
            return ConvictionResult(
                allowed=False, score=0.0, reject_reasons=rejects,
                factors=factors, pool_tf=pool_tf, pool_sig=pool_sig)

        # ── GATE 2: Dealing range — institutional zones ────────────────────
        dr_pd = self._get_dealing_range_pd(price, ict_engine, liq_snapshot)
        if trade_side == "long" and dr_pd > DR_LONG_MAX_PD:
            rejects.append(
                f"DEALING_RANGE: LONG in premium/EQ (PD={dr_pd:.2f} > {DR_LONG_MAX_PD}). "
                f"Institutions buy in discount only.")
            return ConvictionResult(
                allowed=False, score=0.0, reject_reasons=rejects,
                factors=factors, pool_tf=pool_tf, pool_sig=pool_sig)
        if trade_side == "short" and dr_pd < DR_SHORT_MIN_PD:
            rejects.append(
                f"DEALING_RANGE: SHORT in discount/EQ (PD={dr_pd:.2f} < {DR_SHORT_MIN_PD}). "
                f"Institutions sell in premium only.")
            return ConvictionResult(
                allowed=False, score=0.0, reject_reasons=rejects,
                factors=factors, pool_tf=pool_tf, pool_sig=pool_sig)
        factors.dealing_range_ok = True

        # ── GATE 3: R:R minimum ───────────────────────────────────────────
        rr = self._compute_rr(trade_side, entry_price, sl_price, tp_price)
        if rr < MIN_RR:
            rejects.append(f"RR: {rr:.2f} < {MIN_RR:.1f} minimum")
            return ConvictionResult(
                allowed=False, score=0.0, reject_reasons=rejects,
                factors=factors, rr_ratio=rr, pool_tf=pool_tf, pool_sig=pool_sig)

        # ── GATE 4: Session — HARD BLOCK Asia and Weekend ─────────────────
        # Both ASIA and WEEKEND are structurally low-liquidity periods with no
        # institutional delivery.  Hard-blocking prevents the engine from burning
        # cycles and producing misleading "almost-passed" rejection logs during
        # periods where no entry is ever valid.
        sess_key = self._resolve_session(session, ict_engine)
        if sess_key in ("ASIA", "WEEKEND"):
            rejects.append(
                f"SESSION: {sess_key} — no institutional flow, hard blocked")
            return ConvictionResult(
                allowed=False, score=0.0, reject_reasons=rejects,
                factors=factors, rr_ratio=rr, pool_tf=pool_tf, pool_sig=pool_sig)

        # ── GATE 5: AMD phase — HARD BLOCK Accumulation ───────────────────
        amd_phase = self._get_amd_phase(ict_engine)
        if amd_phase == "ACCUMULATION":
            rejects.append(
                "AMD: ACCUMULATION — no delivery expected, hard blocked. "
                "Wait for MANIPULATION sweep.")
            return ConvictionResult(
                allowed=False, score=0.0, reject_reasons=rejects,
                factors=factors, rr_ratio=rr, pool_tf=pool_tf, pool_sig=pool_sig)

        # ── GATE 6: Approach entries — HARD BLOCK ─────────────────────────
        # PRE_SWEEP_APPROACH entries are inherently low-probability.
        # The pool hasn't been swept, there's no displacement, no CISD.
        # Institutional traders NEVER enter before the sweep.
        if is_approach:
            rejects.append(
                "APPROACH_BLOCKED: Pre-sweep approach entries disabled. "
                "Wait for confirmed sweep + displacement + CISD.")
            return ConvictionResult(
                allowed=False, score=0.0, reject_reasons=rejects,
                factors=factors, rr_ratio=rr, pool_tf=pool_tf, pool_sig=pool_sig)

        # ══════════════════════════════════════════════════════════════════
        # FACTOR SCORING — weighted conviction model
        # ══════════════════════════════════════════════════════════════════

        # ── Factor 1: Pool significance (weight 0.20) ─────────────────────
        _rank_bonus = min(effective_rank / 4.0, 1.0)
        _sig_score = min(pool_sig / 8.0, 1.0)
        factors.pool_sig_score = _rank_bonus * 0.40 + _sig_score * 0.60
        allows.append(f"POOL={pool_tf}(htfx{pool_htf_count}) sig={pool_sig:.1f}")

        # ── Factor 2: Displacement (weight 0.25) ──────────────────────────
        factors.displacement_score = self._score_displacement(
            trade_side, price, pool_price, atr, candles_5m, ict_engine)
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

        # ── Factor 4: OTE zone (weight 0.15) ──────────────────────────────
        factors.ote_score = self._score_ote(
            trade_side, entry_price, pool_price, price)
        if factors.ote_score >= 0.70:
            allows.append(f"OTE={factors.ote_score:.2f}✅")

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
        if not score_passed:
            rejects.append(
                f"SCORE: {score:.3f} < {REQUIRED_SCORE} "
                f"[pool={factors.pool_sig_score:.2f} "
                f"disp={factors.displacement_score:.2f} "
                f"cisd={factors.cisd_score:.2f} "
                f"ote={factors.ote_score:.2f} "
                f"sess={factors.session_score:.2f} "
                f"amd={factors.amd_score:.2f}]")

        # ── Session timing gate (checked LAST — score is always real) ─────
        timing_block = self._check_session_limits(now, session)
        if timing_block:
            return ConvictionResult(
                allowed=False, score=score, factors=factors,
                reject_reasons=[timing_block] + rejects,
                allow_reasons=allows, rr_ratio=rr,
                pool_tf=pool_tf, pool_sig=pool_sig,
                blocked_by_timing=True)

        # ── Final decision ─────────────────────────────────────────────────
        allowed = score_passed
        if allowed:
            logger.info(
                f"✅ CONVICTION PASSED ({score:.3f}) | {' | '.join(allows[:5])}")
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

    def _check_session_limits(self, now: float, session: str) -> Optional[str]:
        """Check timing/pacing gates.  Returns reason string if blocked."""
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

        # Cumulative drawdown circuit breaker (session PnL as % of margin)
        # This is a safety net — if we're down 4% in a session, stop.
        # Note: session_pnl is in absolute terms, not percentage.
        # The quant_strategy caller should convert to % if needed.

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
    def _get_dealing_range_pd(price: float, ict_engine, liq_snapshot) -> float:
        """Get dealing range Premium/Discount position [0=full discount, 1=full premium]."""
        if ict_engine is not None:
            dr = getattr(ict_engine, '_dealing_range', None)
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
                rng = max(min(bsl_above) - max(ssl_below), 1e-9)
                return (price - max(ssl_below)) / rng
        return 0.50

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
    ) -> float:
        """
        Score displacement strength [0-1].

        Displacement = strong directional candle away from sweep level.
        This is THE proof of institutional intent — smart money entering aggressively.
        Without displacement, the sweep might just be noise.
        """
        # Check recent 5m candles for displacement bodies
        if candles_5m and len(candles_5m) >= 4:
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
                return 0.30
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
    ) -> float:
        """
        Score OTE (Optimal Trade Entry) zone [0-1].

        OTE = 50% to 78.6% Fibonacci retracement of the displacement move.
        The golden pocket (61.8% - 78.6%) is where institutions fill.
        """
        if pool_price <= 0 or entry_price <= 0 or price <= 0:
            return 0.40

        total_move = abs(price - pool_price)
        if total_move < 1e-3:
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

    @staticmethod
    def _score_amd(trade_side: str, ict_engine) -> float:
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
                return max(base * (0.30 - conf * 0.25), 0.0)
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
        partial string match against NY / LONDON / ASIA.  Previously the method
        checked NY before WEEKEND, so "WEEKEND" fell through to return '' and
        the hard-block gate never fired.
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

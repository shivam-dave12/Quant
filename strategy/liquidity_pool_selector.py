"""
liquidity_pool_selector.py — Industry-grade TP/SL liquidity pool selector v1.0
================================================================================
Created 2026-04-26 as the dedicated pool-selection layer that joins the
significance/proximity/structural model in liquidity_map.py with the
calibrated multi-timeframe sweep-probability model in mtf_pool_probability.py,
and adds the institutional features that were missing from both:

    1. PER-POOL probability (mtf_pool_probability is currently aggregate-only
       in the direction engine; here we use the per-pool raw_prob directly).
    2. GAUNTLET PENALTY — opposing pools lying between entry and the
       candidate TP eat momentum on the way and reduce expected hit-rate.
    3. SESSION / KILLZONE BONUS — pools formed in the active killzone or
       previous session are statistically more likely to be revisited.
    4. QUALITY-SCALED SL BUFFER — the SL ATR buffer scales INVERSELY with
       protective-pool quality. A high-significance pool gets a thin buffer
       (the pool itself is the protection); a low-quality pool gets a wider
       buffer (we don't trust the pool to actually halt price, so we widen).
    5. EV (Expected Value) RANKING for TP — instead of "max significance"
       or "max raw R:R", we maximise:
                EV  =  P(sweep) × R_distance × confluence × (1 - gauntlet_penalty)
       which is the true institutional objective.
    6. SL POOL selection — explicit selection of the best PROTECTIVE pool
       just beyond the structural invalidation, with:
            • highest-significance opposing-side pool within search window
            • quality-scaled buffer beyond pool price
            • freshness penalty (already-tagged pools have weakened stops)
            • adjacency bonus from already-swept pools

PUBLIC API
──────────
    score_tp_pools(snap, side, entry, sl, atr, ict, htf=None,
                   min_rr=2.0, now=None, session="")
        → List[PoolScore]   — sorted descending by EV; first is the choice.

    score_sl_pool(snap, side, entry, atr, ict, htf=None,
                  invalidation_price=None, max_buffer_atr=2.0, now=None)
        → Optional[SLPoolPick]   — best protective pool + buffered SL price.

    The two convenience selectors:
        select_tp(...)  → (tp_price, target, score)  | (None, None, None)
        select_sl(...)  → (sl_price, target, score)  | (None, None, None)

THIS MODULE IS NON-INVASIVE.  It can be imported and used inside
entry_engine._find_tp() and a new entry_engine._find_sl_pool() with no
breakage if the import fails (callers fall back to the existing logic).
"""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

try:
    from strategy.market_intelligence import build_market_profile, MarketProfile
except Exception:  # pragma: no cover - standalone tests
    from market_intelligence import build_market_profile, MarketProfile  # type: ignore

# ────────────────────────────────────────────────────────────────────────────
# MTF probability dependency.
#
# The codebase ships modules under the `strategy.` package, so try that path
# first. Fall back to the bare-name import for direct/standalone runs (tests,
# REPL, dev scripts). Only when BOTH paths fail do we use the local fallback
# math — and even then we log at DEBUG, not WARNING: this is a wiring issue
# for the developer, not an actionable alert for the operator running the bot,
# and a WARNING here would route into the Telegram queue via the log handler.
# ────────────────────────────────────────────────────────────────────────────
_MTF_AVAILABLE = False
try:
    from strategy.mtf_pool_probability import (   # type: ignore
        MTFPoolProbability,
        PoolProbability as _PoolProb,
        _DECAY_LAMBDA,
        _TF_BASE_PROB,
        _MAX_REACH_ATR,
    )
    _MTF_AVAILABLE = True
except ImportError:
    try:
        from mtf_pool_probability import (   # type: ignore
            MTFPoolProbability,
            PoolProbability as _PoolProb,
            _DECAY_LAMBDA,
            _TF_BASE_PROB,
            _MAX_REACH_ATR,
        )
        _MTF_AVAILABLE = True
    except ImportError as _e:
        logger.debug(
            "mtf_pool_probability not importable from either 'strategy.' or "
            "bare path — selector will use local fallback (exp decay × "
            "tf_base_prob). Reason: %s", _e,
        )

if not _MTF_AVAILABLE:
    # Local fallback constants — must mirror mtf_pool_probability so behaviour
    # stays consistent when the real module is wired in later.
    _DECAY_LAMBDA = {"2m": 0.50, "5m": 0.80, "15m": 1.20,
                     "1h":  2.50, "4h":  4.00, "1d":  6.00}
    _TF_BASE_PROB = {"2m": 0.35, "5m": 0.45, "15m": 0.55,
                     "1h":  0.70, "4h":  0.80, "1d":  0.65}
    _MAX_REACH_ATR = {"2m": 1.5, "5m": 3.0, "15m": 5.0,
                      "1h":  8.0, "4h": 12.0, "1d": 18.0}
    _mtf_engine = None   # type: ignore[assignment]
else:
    _mtf_engine = MTFPoolProbability()


# ════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ════════════════════════════════════════════════════════════════════════════

# TP scoring weights (EV components — multiplicative, not additive).
# Tuned so that no single component can dominate by 10×.
_W_PROBABILITY        = 1.0     # base — sweep probability is the primary lens
_W_RR_QUALITY         = 0.30    # bonus per +1.0R above required_rr (capped)
_W_HTF_ALIGNMENT      = 0.40    # +40% if pool aligns with HTF bias
_W_OB_ALIGNED         = 0.20    # +20% if pool sits at an OB
_W_FVG_ALIGNED        = 0.12    # +12% if pool sits at an FVG
_W_CONFLUENCE_PER_TF  = 0.18    # +18% per additional TF source (max 4 → +72%)
_W_KILLZONE_BONUS     = 0.15    # +15% if pool was formed during current/prev killzone
_W_FRESHNESS_BONUS    = 0.10    # +10% for pool age < 25% of TF window
_W_TOUCH_PENALTY      = 0.20    # -20% per touch beyond 3 (capped at -50%)

# Gauntlet: opposing pools between entry and TP that EAT momentum.
_GAUNTLET_SIG_THRESHOLD = 0.60  # only opposing pools at ≥60% of TP's sig matter
_GAUNTLET_PENALTY_PER   = 0.18  # -18% per qualifying gauntlet pool
_GAUNTLET_PENALTY_MAX   = 0.55  # but never wipe more than 55% of EV

# SL pool selector tunables.
_SL_BUFFER_BASE_ATR        = 0.18  # minimum buffer beyond pool price
_SL_BUFFER_MAX_ATR         = 0.55  # ceiling on buffer
_SL_BUFFER_QUALITY_SCALE   = 0.40  # inverse-scale: high quality → smaller buffer
_SL_SEARCH_WINDOW_ATR      = 4.0   # max distance to look for a protective pool
_SL_MIN_BEYOND_INVAL_ATR   = 0.05  # protective pool must be at least this far past invalidation
_SL_MIN_SIGNIFICANCE       = 1.5   # don't anchor SL to garbage pools

# Stop placement must be outside the full protective liquidity envelope, not
# a few ticks behind a single visible pool.  These values are not entry
# filters; they shape the SL price from the live pool distribution so the stop
# is not parked in the middle of a stop cluster.
_SL_ZONE_JOIN_ATR          = 0.65  # pools closer than this form one stop zone
_SL_STOP_EXCLUSION_ATR     = 0.40  # SL must not sit within this distance of a pool
_SL_ZONE_BASE_BUFFER_ATR   = 0.30  # minimum beyond the external zone edge
_SL_ZONE_WIDTH_BUFFER_FRAC = 0.30  # wider stop zones need more external clearance
_SL_ZONE_DENSITY_BUFFER    = 0.08  # extra buffer per log(pool_count)
_SL_ZONE_MAX_BUFFER_ATR    = 1.35  # hard cap before liquidation/geometry guard

# Killzones (UTC). London 07-10, NY 12-16. Bonus active during + previous KZ.
_KILLZONE_HOURS = (7, 8, 9, 10, 12, 13, 14, 15, 16)

# Terminal-target handling.
#
# The MTF probability model has a useful ``first-sweep reach`` window for
# predicting which pool is likely to be hit NEXT. That window must NOT be a
# hard veto for execution TP: institutional delivery can target HTF external
# liquidity many ATR away, while the trade is protected by BE migration and
# liquidity/structure trailing. Therefore distance beyond the TF reach model
# is a SOFT EV penalty and an audit note, not an automatic rejection.
# Hard vetoes remain: wrong side, swept/consumed, too close/cost-invalid, and
# below required R:R.
_TF_RANK = {"1m": 1, "2m": 1, "3m": 1, "5m": 2, "15m": 3, "1h": 4, "4h": 5, "1d": 6}
_TERMINAL_TP_MIN_TF_RANK = 3      # 15m+ can be used as terminal delivery objectives
_TERMINAL_TP_MIN_SIG     = 3.0    # lower-TF distant pools need real significance
_TERMINAL_REACH_FLOOR    = 0.05   # never zero out a valid terminal objective

# Final TP payoff geometry. A high posterior can relax the static R:R prior,
# but it cannot turn a BE-adjacent pool into a full-position institutional TP.
_TP_DURABLE_RR_FLOOR     = 1.35
_TP_MIN_BE_MOVE_MULT     = 1.80
_TP_TERMINAL_PROFILE_FLOOR = 0.55
_TP_DELIVERY_PROB_FLOOR  = 1e-6

# Terminal/frontier target handling.  A far TP must not be selected because it is
# simply far, and it must not be rejected just because the first-touch model says
# the next immediate sweep probability is low.  Institutional target selection
# treats HTF/external pools as conditional delivery objectives: if the auction
# posterior, pool quality, HTF rank, confluence and path/gauntlet quality support
# the route, the TP can be reasonably far and still executable.
_TP_FRONTIER_PROB_FLOOR = 0.055
_TP_FRONTIER_PROB_CAP   = 0.42
_TP_FRONTIER_DECAY_EXPANSION = 2.75
_TP_FRONTIER_SPAN_BONUS_MAX = 1.00
_TP_FRONTIER_MAX_REACH_MULT = 2.50
_TP_FRONTIER_MAX_ABS_ATR = 48.0


# ════════════════════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ════════════════════════════════════════════════════════════════════════════

@dataclass
class PoolScore:
    """A scored TP candidate. EV is the canonical ranking key."""
    target:        Any                   # PoolTarget from liquidity_map
    tp_price:      float                 # buffered TP price (with reach buffer)
    distance_atr:  float
    rr:            float                 # reward / risk
    sweep_prob:    float                 # P(this pool gets swept) ∈ [0, 1]
    raw_score:     float                 # MTF raw_prob (un-normalised)
    confluence:    float                 # multiplicative confluence factor
    gauntlet_n:    int                   # opposing pools in gauntlet
    gauntlet_pen:  float                 # multiplicative penalty ∈ [0.45, 1.0]
    ev:            float                 # final ranking key
    components:    Dict[str, float] = field(default_factory=dict)
    reasons:       List[str]        = field(default_factory=list)

    def __repr__(self) -> str:
        return (f"PoolScore(tp=${self.tp_price:,.1f} "
                f"dist={self.distance_atr:.1f}ATR rr={self.rr:.2f} "
                f"P={self.sweep_prob:.2f} EV={self.ev:.3f})")


@dataclass
class SLPoolPick:
    """The chosen protective liquidity envelope and the buffered SL price.

    ``target`` is the external-edge pool of the selected zone, not simply the
    highest-scoring inner pool.  This is intentional: an institutional stop is
    placed beyond the whole protective liquidity envelope, never in the middle
    of clustered SSL/BSL liquidity.
    """
    target:       Any                    # external-edge PoolTarget on OPPOSING side
    sl_price:     float                  # zone edge ± buffer
    buffer_atr:   float
    quality:      float                  # composite [0, 1] quality of the zone
    reasons:      List[str] = field(default_factory=list)
    zone_size:    int = 1
    zone_inner:   float = 0.0
    zone_outer:   float = 0.0
    zone_width_atr: float = 0.0


@dataclass
class PoolCandidateDiagnostic:
    """Human-readable TP/SL candidate audit row.

    This is deliberately separate from PoolScore/SLPoolPick.  PoolScore only
    represents candidates that PASSED the institutional gates.  This row also
    records rejected pools and the exact reason, so display/logging can explain
    why a visible BSL/SSL was not used as TP/SL.
    """
    role:          str
    side:          str
    pool_side:     str
    pool_price:    float
    timeframe:     str
    status:        str
    distance_atr:  float
    significance:  float
    touches:       int
    selected:      bool = False
    eligible:      bool = False
    reason:        str  = ""
    tp_price:      float = 0.0
    sl_price:      float = 0.0
    rr:            float = 0.0
    required_rr:   float = 0.0
    reward:        float = 0.0
    risk:          float = 0.0
    sweep_prob:    float = 0.0
    delivery_prob: float = 0.0
    required_delivery_prob: float = 0.0
    cost_r:        float = 0.0
    ev:            float = 0.0
    selection_ev:  float = 0.0
    confluence:    float = 1.0
    gauntlet_n:    int   = 0
    be_move:       float = 0.0
    buffer_atr:    float = 0.0
    quality:       float = 0.0
    notes:         List[str] = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        return dict(self.__dict__)


@dataclass
class PoolSelectionReport:
    """Full institutional audit report for TP/SL pool selection."""
    role:        str
    side:        str
    entry:       float
    atr:         float
    candidates: List[PoolCandidateDiagnostic] = field(default_factory=list)
    selected:    Optional[PoolCandidateDiagnostic] = None
    summary:     str = ""

    def as_dict(self) -> Dict[str, Any]:
        return {
            "role": self.role,
            "side": self.side,
            "entry": self.entry,
            "atr": self.atr,
            "summary": self.summary,
            "selected": self.selected.as_dict() if self.selected else None,
            "candidates": [c.as_dict() for c in self.candidates],
        }


# ════════════════════════════════════════════════════════════════════════════
# INTERNAL HELPERS
# ════════════════════════════════════════════════════════════════════════════

def _safe(obj: Any, attr: str, default=0.0):
    try:
        v = getattr(obj, attr, default)
        return v if v is not None else default
    except Exception:
        return default


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(value)))


def _required_delivery_probability(rr: float, cost_r: float) -> float:
    """Break-even hit probability for a TP at ``rr`` after execution cost."""
    return _clamp((1.0 + max(float(cost_r), 0.0)) / max(float(rr) + 1.0, 1e-9), 0.0, 1.0)


def _enum_value(v: Any, default: str = "") -> str:
    try:
        if hasattr(v, "value"):
            return str(v.value)
        return str(v) if v is not None else default
    except Exception:
        return default


def _pool_status(pool: Any) -> str:
    return _enum_value(_safe(pool, "status", ""), "")


def _pool_side(pool: Any) -> str:
    return _enum_value(_safe(pool, "side", ""), "")


def _target_signature(target: Any) -> Tuple[str, float, str]:
    pool = _safe(target, "pool", None)
    return (
        _pool_side(pool),
        round(float(_safe(pool, "price", 0.0)), 8),
        str(_safe(pool, "timeframe", "")),
    )


def _candidate_base(role: str, side: str, target: Any, entry: float, atr: float,
                    risk: float = 0.0, required_rr: float = 0.0,
                    be_move: float = 0.0) -> PoolCandidateDiagnostic:
    pool = _safe(target, "pool", None)
    return PoolCandidateDiagnostic(
        role         = role,
        side         = side,
        pool_side    = _pool_side(pool),
        pool_price   = float(_safe(pool, "price", 0.0)),
        timeframe    = str(_safe(pool, "timeframe", "")),
        status       = _pool_status(pool),
        distance_atr = float(_safe(target, "distance_atr", 0.0)),
        significance = float(_safe(target, "significance", 0.0)),
        touches      = int(_safe(pool, "touches", 1) or 1),
        risk         = float(risk),
        required_rr  = float(required_rr),
        be_move      = float(be_move),
    )


def _sort_report_candidates(rows: List[PoolCandidateDiagnostic], limit: int = 8) -> List[PoolCandidateDiagnostic]:
    return sorted(rows, key=_candidate_report_priority, reverse=True)[:max(1, int(limit))]


def _candidate_report_priority(r: PoolCandidateDiagnostic) -> float:
    if r.selected:
        return 1_000_000.0 + max(r.selection_ev, r.ev)
    if r.eligible:
        return 100_000.0 + max(r.selection_ev, r.ev)

    req_p = r.required_delivery_prob
    if req_p <= 0.0 and r.rr > 0.0:
        req_p = _required_delivery_probability(r.rr, r.cost_r)
    delivery = max(r.delivery_prob, r.sweep_prob, 0.0)
    p_fit = _clamp(delivery / max(req_p, 1e-9), 0.0, 2.0)

    rr_floor = r.required_rr if r.required_rr > 0.0 else 1.0
    rr_fit = _clamp(r.rr / max(rr_floor, 1e-9), 0.0, 2.0)
    proximity = 1.0 / (1.0 + max(r.distance_atr, 0.0) / 8.0)

    return 0.55 * p_fit + 0.30 * rr_fit + 0.15 * proximity


def _hour_utc(now: Optional[float]) -> int:
    try:
        ts = now if now and now > 1e6 else time.time()
        return int(ts // 3600) % 24
    except Exception:
        return -1


def _is_killzone(hour: int) -> bool:
    return hour in _KILLZONE_HOURS


def _killzone_bonus(pool, now: Optional[float]) -> float:
    """Return multiplicative bonus 1.0 (no bonus) or 1+_W_KILLZONE_BONUS."""
    created_at = float(_safe(pool, "created_at", 0.0))
    if created_at <= 0:
        return 1.0
    h = _hour_utc(created_at)
    if _is_killzone(h):
        return 1.0 + _W_KILLZONE_BONUS
    # Previous killzone hour gets half-bonus (sweep targets often carry over).
    if h >= 0 and ((h + 1) in _KILLZONE_HOURS or (h + 2) in _KILLZONE_HOURS):
        return 1.0 + _W_KILLZONE_BONUS * 0.5
    return 1.0


def _freshness_bonus(pool) -> float:
    """+10% for pools younger than 25% of their TF retention window."""
    tf = str(_safe(pool, "timeframe", "5m"))
    created_at = float(_safe(pool, "created_at", 0.0))
    if created_at <= 0:
        return 1.0
    # crude TF→window map — matches liquidity_map._POOL_MAX_AGE roughly
    window = {
        "1m": 14400, "5m": 604800, "15m": 604800,
        "1h": 604800, "4h": 604800, "1d": 2592000,
    }.get(tf, 604800.0)
    age = time.time() - created_at
    if age < window * 0.25:
        return 1.0 + _W_FRESHNESS_BONUS
    return 1.0


def _touch_penalty(pool) -> float:
    """-20% per touch beyond 3, capped at -50% (a 5+ touch pool is half value)."""
    touches = int(_safe(pool, "touches", 1))
    excess = max(0, touches - 3)
    pen = min(_W_TOUCH_PENALTY * excess, 0.50)
    return 1.0 - pen


def _htf_alignment_bonus(pool, side: str, htf: Any) -> float:
    """+40% if pool sits in the direction of HTF bias for this trade side."""
    if htf is None:
        return 1.0
    bias = str(_safe(htf, "htf_bias", "")).lower()
    # For a LONG, BSL TP is aligned with bullish HTF.
    # For a SHORT, SSL TP is aligned with bearish HTF.
    pool_side = str(_safe(pool, "side", ""))
    if isinstance(pool_side, str) and ("BSL" in pool_side.upper()):
        is_bsl = True
    elif hasattr(pool_side, "value"):
        is_bsl = "BSL" in str(pool_side.value).upper()
    else:
        is_bsl = (side == "long")  # fallback by trade side

    aligned = (
        (side == "long"  and is_bsl  and "bull" in bias) or
        (side == "short" and not is_bsl and "bear" in bias)
    )
    return 1.0 + _W_HTF_ALIGNMENT if aligned else 1.0


def _structural_bonus(pool) -> float:
    m = 1.0
    if _safe(pool, "ob_aligned", False):
        m += _W_OB_ALIGNED
    if _safe(pool, "fvg_aligned", False):
        m += _W_FVG_ALIGNED
    return m


def _confluence_bonus(target) -> float:
    sources = list(_safe(target, "tf_sources", []) or [])
    extra = max(0, len(sources) - 1)
    extra = min(extra, 4)   # cap at 4 → +72%
    return 1.0 + _W_CONFLUENCE_PER_TF * extra


def _per_pool_sweep_prob(target, price: float, atr: float, now: float) -> Tuple[float, float]:
    """
    Return (raw_prob, normalised_prob) for a single pool.

    raw_prob is the geometric-mean-style probability used by mtf_pool_probability.
    normalised_prob is raw_prob / max_possible — gives a [0, 1] interpretation.
    """
    if not _MTF_AVAILABLE:
        # Fallback: pure exponential distance × tf_base_prob
        tf = str(_safe(target.pool, "timeframe", "5m"))
        λ = _DECAY_LAMBDA.get(tf, 2.0)
        base = _TF_BASE_PROB.get(tf, 0.5)
        d = max(float(target.distance_atr), 0.0)
        raw = math.exp(-d / λ) * base
        return raw, raw

    try:
        side_str = "BSL" if target.direction == "long" else "SSL"
        pp = _mtf_engine._score_pool(   # type: ignore[attr-defined]
            target.pool, price, atr, now, side_str, target,
        )
        if pp is None:
            # ``mtf_pool_probability`` returns None for pools outside its
            # first-sweep reach window. For TP execution we still allow such
            # pools as terminal delivery objectives if other gates pass; apply
            # the same exponential distance decay locally instead of zeroing
            # the candidate. Status/wrong-side pools are vetoed elsewhere.
            tf = str(_safe(target.pool, "timeframe", "5m"))
            d = max(float(_safe(target, "distance_atr", 0.0)), 0.0)
            lam = float(_DECAY_LAMBDA.get(tf, 2.0))
            base = float(_TF_BASE_PROB.get(tf, 0.50))
            sig = min(float(_safe(target, "significance", 0.0)) / 12.0, 1.0)
            raw = math.exp(-d / max(lam, 1e-9)) * base * (1.0 + sig * 0.6)
            norm = min(raw / 2.0, 1.0)
            return raw, norm
        raw = float(pp.raw_prob)
        # Normalise against a max-possible reference: a 4h pool 0 ATR away with
        # cap significance, full recency, no touch penalty.
        # In _score_pool: prob = (c_distance × c_tf) × (1 + c_sig × 0.6) × c_recency × c_touch
        # max ≈ 1.0 × 0.80 × (1 + 0.6) × 1.20 × 1.30 ≈ 2.0
        norm = min(raw / 2.0, 1.0)
        return raw, norm
    except Exception as e:
        logger.debug("MTF probability scoring failed: %s — falling back", e)
        # Fallback — same as above
        tf = str(_safe(target.pool, "timeframe", "5m"))
        λ = _DECAY_LAMBDA.get(tf, 2.0)
        base = _TF_BASE_PROB.get(tf, 0.5)
        d = max(float(target.distance_atr), 0.0)
        raw = math.exp(-d / λ) * base
        return raw, raw


def _gauntlet_penalty(
    candidate_target,
    candidate_sig: float,
    snap,
    side: str,
    entry: float,
    atr: float,
) -> Tuple[int, float]:
    """
    Count opposing pools between entry and the candidate TP that are at
    ≥60% of the TP's significance — these eat momentum on the way to TP.

    Returns (n_gauntlet_pools, multiplicative_penalty).
    Penalty = max(1 − n × _GAUNTLET_PENALTY_PER, 1 − _GAUNTLET_PENALTY_MAX).
    """
    if snap is None or atr <= 0:
        return 0, 1.0

    tp_price = float(_safe(candidate_target.pool, "price", 0.0))
    if tp_price <= 0:
        return 0, 1.0

    # The "gauntlet" consists of OPPOSING pools (SSL pools when going LONG to BSL TP,
    # and vice versa) lying between entry and tp_price.
    if side == "long":
        opposing = list(_safe(snap, "ssl_pools", []) or [])
        lo, hi   = min(entry, tp_price), max(entry, tp_price)
    else:
        opposing = list(_safe(snap, "bsl_pools", []) or [])
        lo, hi   = min(entry, tp_price), max(entry, tp_price)

    threshold = candidate_sig * _GAUNTLET_SIG_THRESHOLD
    n = 0
    for opp in opposing:
        opp_price = float(_safe(opp.pool, "price", 0.0))
        if opp_price <= lo or opp_price >= hi:
            continue
        opp_sig = float(_safe(opp, "significance", 0.0))
        if opp_sig >= threshold:
            n += 1

    if n == 0:
        return 0, 1.0

    pen = min(n * _GAUNTLET_PENALTY_PER, _GAUNTLET_PENALTY_MAX)
    return n, 1.0 - pen


def _tf_rank(tf: str) -> int:
    return _TF_RANK.get(str(tf).lower(), 2)


def _max_reach_for_tf(tf: str) -> float:
    return float(_MAX_REACH_ATR.get(str(tf), _MAX_REACH_ATR.get(str(tf).lower(), 12.0)))


def _max_frontier_reach_for_tf(tf: str) -> float:
    """Maximum executable TP frontier in ATR units for a timeframe.

    This is not a nearest-target filter.  It prevents lottery objectives that are
    so far beyond the timeframe's historical sweep horizon that even strong R:R
    cannot make the path institutionally executable.
    """
    return min(_TP_FRONTIER_MAX_ABS_ATR, max(_max_reach_for_tf(tf) * _TP_FRONTIER_MAX_REACH_MULT, _max_reach_for_tf(tf) + 6.0))


def _is_terminal_tp_candidate(target: Any, distance_atr: float) -> bool:
    """True when a beyond-reach pool is still a valid terminal objective.

    This is deliberately not a permission to trade weak moonshot targets. The
    pool still has to be active, on the correct side, above minimum R:R, and it
    is EV-penalised by distance/probability. This only converts the old hard
    ``too far`` veto into institutional target handling.
    """
    pool = _safe(target, "pool", None)
    tf = str(_safe(pool, "timeframe", "5m"))
    sig = float(_safe(target, "significance", 0.0))
    return _tf_rank(tf) >= _TERMINAL_TP_MIN_TF_RANK or sig >= _TERMINAL_TP_MIN_SIG


def _terminal_reach_multiplier(distance_atr: float, tf: str) -> float:
    """Soft penalty for pools beyond the first-sweep reach model."""
    max_reach = _max_reach_for_tf(tf)
    if distance_atr <= max_reach:
        return 1.0
    lam = max(float(_DECAY_LAMBDA.get(str(tf), 2.0)), 1.25)
    overshoot = max(0.0, distance_atr - max_reach)
    return max(_TERMINAL_REACH_FLOOR, math.exp(-overshoot / lam))


def _reach_buffer(distance_atr: float, atr: float) -> float:
    """
    The buffer placed BEFORE the pool price so the TP fills as price approaches —
    the TP must trigger a tick or two BEFORE the pool sweep, not exactly AT it.

    Scales with distance: distant pools deserve larger buffers (more uncertainty),
    near pools get tighter ones. Capped between 0.10 and 0.50 ATR.
    """
    return max(0.10, min(0.50, 0.10 + 0.05 * distance_atr)) * atr


def _breakeven_move(entry: float, atr: float) -> float:
    """Conservative round-trip cost + execution-noise estimate."""
    try:
        import config as _cfg
        maker = abs(float(getattr(_cfg, "COMMISSION_RATE_MAKER", 0.0002)))
        taker = abs(float(getattr(_cfg, "COMMISSION_RATE", 0.00055)))
        fee_rate = max(maker, min(taker, 0.00045))
    except Exception:
        fee_rate = 0.00045
    return max(entry * fee_rate * 2.0, 0.0) + 0.18 * max(atr, 0.0)


def _posterior_delivery_probability(
    raw_prob: float,
    posterior_prob: float,
    confluence: float,
    gauntlet_mult: float,
    reach_mult: float,
) -> float:
    """Direct first-sweep delivery probability for non-terminal objectives."""
    pool_prob = _clamp(raw_prob, _TP_DELIVERY_PROB_FLOOR, 0.95)
    pool_prob *= math.sqrt(_clamp(confluence, 0.25, 2.25))
    pool_prob *= math.sqrt(_clamp(gauntlet_mult, 0.45, 1.00))
    pool_prob *= math.sqrt(_clamp(reach_mult, 0.05, 1.25))
    pool_prob = _clamp(pool_prob, _TP_DELIVERY_PROB_FLOOR, 0.95)

    posterior = _clamp(posterior_prob, 0.0, 0.95)
    if posterior <= 0.0:
        return pool_prob
    return _clamp(math.sqrt(pool_prob * posterior), _TP_DELIVERY_PROB_FLOOR, 0.93)


def _frontier_delivery_probability(
    *,
    direct_prob: float,
    posterior_prob: float,
    confluence: float,
    gauntlet_mult: float,
    reach_mult: float,
    rr: float,
    distance_atr: float,
    significance: float,
    tf: str,
    terminal_target: bool,
) -> Tuple[float, Dict[str, float]]:
    """Path-adjusted probability for executable HTF/frontier TP targets.

    The first-touch MTF model answers: "what is likely to be swept next?"  That
    is useful for near targets but underprices external/HTF delivery targets.
    Institutional execution does not require the far pool to be the *next* pool;
    it requires a positive conditional path: auction posterior + pool quality +
    HTF rank + confluence + acceptable gauntlet + payoff geometry.  This helper
    produces that conditional delivery probability without turning every distant
    pool into a valid TP.
    """
    direct = _clamp(direct_prob, _TP_DELIVERY_PROB_FLOOR, 0.93)
    if not terminal_target:
        return direct, {
            "delivery_model": "direct",
            "direct_prob": direct,
            "frontier_prob": 0.0,
            "frontier_decay": 1.0,
        }

    max_reach = _max_reach_for_tf(tf)
    lam = max(float(_DECAY_LAMBDA.get(str(tf), _DECAY_LAMBDA.get(str(tf).lower(), 2.0))), 1.25)
    overshoot = max(0.0, float(distance_atr) - max_reach)
    # Far targets decay, but slower than first-touch reach: this is a conditional
    # delivery route, not a prediction that the target is swept immediately.
    frontier_decay = math.exp(-overshoot / max(lam * _TP_FRONTIER_DECAY_EXPANSION, 1e-9))
    frontier_decay = _clamp(frontier_decay, 0.18, 1.00)

    posterior_q = _clamp(float(posterior_prob) / 0.72, 0.0, 1.0)
    sig_q = _clamp(float(significance) / 8.0, 0.0, 1.0)
    tf_q = _clamp((_tf_rank(str(tf)) - 2.0) / 4.0, 0.0, 1.0)
    conf_q = _clamp((float(confluence) - 0.80) / 1.35, 0.0, 1.0)
    lane_q = _clamp((float(gauntlet_mult) - 0.45) / 0.55, 0.0, 1.0)
    payoff_q = _clamp((float(rr) - 2.0) / 14.0, 0.0, 1.0)
    reach_q = _clamp(math.sqrt(max(float(reach_mult), 0.0) / _TP_TERMINAL_PROFILE_FLOOR), 0.0, 1.0)

    frontier = (
        _TP_FRONTIER_PROB_FLOOR
        + 0.115 * posterior_q
        + 0.075 * sig_q
        + 0.055 * tf_q
        + 0.055 * conf_q
        + 0.045 * lane_q
        + 0.045 * payoff_q
        + 0.025 * reach_q
    ) * math.sqrt(frontier_decay)
    frontier = _clamp(frontier, _TP_FRONTIER_PROB_FLOOR, _TP_FRONTIER_PROB_CAP)
    return max(direct, frontier), {
        "delivery_model": "frontier",
        "direct_prob": direct,
        "frontier_prob": frontier,
        "frontier_decay": frontier_decay,
        "posterior_q": posterior_q,
        "sig_q": sig_q,
        "tf_q": tf_q,
        "conf_q": conf_q,
        "lane_q": lane_q,
        "payoff_q": payoff_q,
    }


def _institutional_rr_floor(
    static_min_rr: float,
    posterior_prob: float,
    raw_prob: float,
    confluence: float,
    gauntlet_mult: float,
    reach_mult: float,
    risk: float,
    be_move: float,
    *,
    rr: float = 0.0,
    distance_atr: float = 0.0,
    significance: float = 0.0,
    tf: str = "5m",
    terminal_target: bool = False,
) -> Tuple[float, float, float, Dict[str, float]]:
    """Return (required_rr, delivery_probability, cost_r, delivery_components).

    The floor is a positive expected-value boundary, not a retail fixed R:R
    parameter.  For normal targets we use direct sweep delivery probability.  For
    HTF/frontier targets we use conditional path-adjusted delivery probability so
    a reasonable far target can be selected when the route is executable.
    """
    static_floor = max(0.01, float(static_min_rr))
    risk_f = max(float(risk), 1e-9)
    durable_floor = max(
        _TP_DURABLE_RR_FLOOR,
        _TP_MIN_BE_MOVE_MULT * max(float(be_move), 0.0) / risk_f,
    )
    cost_r = _clamp(0.75 * float(be_move) / risk_f, 0.0, 0.65)
    posterior = _clamp(posterior_prob, 0.0, 0.95)
    direct_p = _posterior_delivery_probability(
        raw_prob, posterior, confluence, gauntlet_mult, reach_mult)
    delivery_p, delivery_components = _frontier_delivery_probability(
        direct_prob=direct_p,
        posterior_prob=posterior,
        confluence=confluence,
        gauntlet_mult=gauntlet_mult,
        reach_mult=reach_mult,
        rr=rr,
        distance_atr=distance_atr,
        significance=significance,
        tf=tf,
        terminal_target=terminal_target,
    )
    ev_floor = ((1.0 - delivery_p) + cost_r) / max(delivery_p, 1e-9)
    if posterior <= 0.0:
        # Without setup posterior, do not let conditional frontier math weaken
        # the static prior.  The target can still be far, but only if it clears
        # the full positive-EV boundary.
        return max(static_floor, durable_floor, ev_floor), delivery_p, cost_r, delivery_components
    return max(durable_floor, ev_floor), delivery_p, cost_r, delivery_components

def _tp_reach_multiplier(distance_atr: float, tf: str, selector_profile: MarketProfile,
                         terminal_target: bool) -> float:
    mtf_reach = _terminal_reach_multiplier(distance_atr, tf)
    profile_reach = selector_profile.target_reach_penalty(distance_atr, tf)
    if terminal_target:
        profile_reach = max(profile_reach, _TP_TERMINAL_PROFILE_FLOOR)
    return mtf_reach * profile_reach


def _tp_selection_value(ev: float, rr: float, rr_floor: float,
                        distance_atr: float) -> float:
    """Rank the executable target frontier, not simply the nearest pool.

    ``ev`` has already paid for probability, path quality and gauntlet.  This
    selection layer rewards durable payoff and reasonable delivery span so a
    farther, executable HTF pool can beat a shallow near pool.
    """
    surplus_r = max(float(rr) - float(rr_floor), 0.0)
    payoff_frontier = math.sqrt(max(float(rr), 0.0)) * (1.0 + min(surplus_r * 0.30, 1.35))
    delivery_span = 1.0 + min(max(float(distance_atr) - 1.0, 0.0) * 0.055, _TP_FRONTIER_SPAN_BONUS_MAX)
    return float(ev) * payoff_frontier * delivery_span


def _tp_payoff_rejection_reason(
    rr: float,
    rr_floor: float,
    static_floor: float,
    delivery_prob: float,
    required_delivery_prob: float,
    cost_r: float,
    terminal_target: bool,
) -> str:
    if delivery_prob + 1e-9 < required_delivery_prob:
        reason = (
            f"delivery probability {delivery_prob:.4f} < required {required_delivery_prob:.4f} "
            f"for RR {rr:.2f} (static {static_floor:.2f}, cost={cost_r:.2f}R, "
            f"payoff floor {rr_floor:.2f})"
        )
        if terminal_target:
            reason += "; terminal runner context, not full-position TP"
        return reason
    return (
        f"R:R {rr:.2f} < payoff floor {rr_floor:.2f} "
        f"(static {static_floor:.2f}, p={delivery_prob:.4f}, cost={cost_r:.2f}R)"
    )


def _target_utility(delivery_prob: float, rr: float, min_rr: float,
                    distance_atr: float, reward: float,
                    be_move: float) -> Tuple[float, Dict[str, float]]:
    """
    Convert delivery probability into payoff utility.

    This is not a nearest-pool heuristic.  It prices the executable objective by
    conditional delivery probability, durable R, room beyond BE migration and
    distance quality.  Far targets can rank first only when their conditional
    path remains positive-EV after costs and gauntlet.
    """
    rr_excess = max(0.0, rr - float(min_rr))
    rr_utility = math.sqrt(max(rr, 0.0)) * (1.0 + min(rr_excess * 0.35, 1.25))
    delivery_room = max(0.0, reward - be_move)
    be_surplus_atr = delivery_room / max(be_move, 1e-9)
    be_quality = 0.35 + 0.65 * min(be_surplus_atr / 2.0, 1.0)
    distance_quality = 1.0 - math.exp(-max(distance_atr - 0.35, 0.0) / 1.8)
    distance_quality = max(0.25, distance_quality)
    utility = _clamp(delivery_prob, _TP_DELIVERY_PROB_FLOOR, 0.95) * rr_utility * be_quality * distance_quality
    return utility, {
        "rr_utility": rr_utility,
        "be_quality": be_quality,
        "distance_quality": distance_quality,
        "be_move": be_move,
    }




def _is_live_liquidity_target(target: Any, min_significance: float = 0.0) -> bool:
    """True only for active, currently usable pool targets.

    Stops and targets must not be derived from consumed/swept/archive pools.
    This helper is deliberately shared by TP/SL logic so pool status semantics
    stay consistent across target selection and stop protection.
    """
    try:
        pool = _safe(target, "pool", target)
        status = _pool_status(pool).upper()
        if status in {"SWEPT", "CONSUMED", "ARCHIVED", "DEAD", "EXPIRED", "INVALID"}:
            return False
        if bool(_safe(pool, "is_swept", False)) or bool(_safe(target, "is_swept", False)):
            return False
        sig = float(_safe(target, "significance", _safe(pool, "significance", 0.0)) or 0.0)
        return sig >= float(min_significance)
    except Exception:
        return False


def _pool_price(target: Any) -> float:
    return float(_safe(_safe(target, "pool", target), "price", 0.0) or 0.0)


def _pool_significance(target: Any) -> float:
    pool = _safe(target, "pool", target)
    return float(_safe(target, "significance", _safe(pool, "significance", 0.0)) or 0.0)


def _pool_microstructure_score(target: Any) -> float:
    """Structural score used inside an SL envelope."""
    pool = _safe(target, "pool", target)
    sig = max(_pool_significance(target), 0.0)
    return sig * _structural_bonus(pool) * _freshness_bonus(pool) * _touch_penalty(pool)


def _protective_candidates(
    snap: Any,
    side: str,
    entry: float,
    atr: float,
    invalidation_price: Optional[float],
) -> Tuple[List[Any], str]:
    """Return live protective pool targets for the trade side.

    Long trades are protected by SSL below invalidation.  Shorts are protected
    by BSL above invalidation.  The search window is still bounded by ATR and
    liquidation/geometry checks in the caller, but selection is made at the
    liquidity-zone level rather than one isolated pool.
    """
    if snap is None or atr <= 0:
        return [], "no snapshot/ATR"
    if side == "long":
        pools = list(_safe(snap, "ssl_pools", []) or [])
        inv = invalidation_price if invalidation_price is not None else entry - 0.3 * atr
        out = [t for t in pools
               if _is_live_liquidity_target(t, _SL_MIN_SIGNIFICANCE)
               and _pool_price(t) <= inv - _SL_MIN_BEYOND_INVAL_ATR * atr
               and (entry - _pool_price(t)) <= _SL_SEARCH_WINDOW_ATR * atr]
        return out, "SSL"
    pools = list(_safe(snap, "bsl_pools", []) or [])
    inv = invalidation_price if invalidation_price is not None else entry + 0.3 * atr
    out = [t for t in pools
           if _is_live_liquidity_target(t, _SL_MIN_SIGNIFICANCE)
           and _pool_price(t) >= inv + _SL_MIN_BEYOND_INVAL_ATR * atr
           and (_pool_price(t) - entry) <= _SL_SEARCH_WINDOW_ATR * atr]
    return out, "BSL"


def _build_protective_zones(candidates: List[Any], side: str, atr: float) -> List[List[Any]]:
    """Cluster nearby protective pools into executable stop zones."""
    if not candidates or atr <= 0:
        return []
    # Long: from inner/high price down to external/low price.  Short: inverse.
    reverse = side == "long"
    ordered = sorted(candidates, key=_pool_price, reverse=reverse)
    zones: List[List[Any]] = []
    join = max(_SL_ZONE_JOIN_ATR * atr, 1e-9)
    current: List[Any] = []
    last_px: Optional[float] = None
    for t in ordered:
        px = _pool_price(t)
        if px <= 0:
            continue
        if last_px is None or abs(px - last_px) <= join:
            current.append(t)
        else:
            if current:
                zones.append(current)
            current = [t]
        last_px = px
    if current:
        zones.append(current)
    return zones


def _zone_edges(zone: List[Any], side: str) -> Tuple[float, float, Any]:
    """Return (inner_edge, external_edge, external_target)."""
    ordered = sorted(zone, key=_pool_price)
    if side == "long":
        external = ordered[0]
        inner = ordered[-1]
    else:
        external = ordered[-1]
        inner = ordered[0]
    return _pool_price(inner), _pool_price(external), external


def _liquidity_zone_buffer_atr(zone: List[Any], side: str, atr: float,
                               quality: float, max_buffer_atr: float) -> Tuple[float, float]:
    """Calculate external stop clearance from zone width, density and quality."""
    inner, outer, _ = _zone_edges(zone, side)
    width_atr = abs(inner - outer) / max(atr, 1e-9)
    density = math.log1p(max(len(zone), 1))
    quality_buffer = _SL_BUFFER_BASE_ATR + (1.0 - _clamp(quality, 0.0, 1.0)) * _SL_BUFFER_QUALITY_SCALE
    zone_buffer = (
        _SL_ZONE_BASE_BUFFER_ATR
        + _SL_ZONE_WIDTH_BUFFER_FRAC * min(width_atr, 2.0)
        + _SL_ZONE_DENSITY_BUFFER * density
    )
    buffer_atr = max(quality_buffer, zone_buffer)
    buffer_atr = min(buffer_atr, _SL_ZONE_MAX_BUFFER_ATR, float(max_buffer_atr))
    return buffer_atr, width_atr


def _move_stop_outside_nearby_liquidity(sl: float, side: str, atr: float,
                                        candidates: List[Any], max_buffer_atr: float) -> float:
    """Ensure the final SL is not sitting inside/adjacent to a live pool."""
    if atr <= 0 or not candidates:
        return sl
    exclusion = _SL_STOP_EXCLUSION_ATR * atr
    # Iterate because moving beyond one nearby pool can expose the next pool.
    for _ in range(5):
        near = [t for t in candidates if abs(_pool_price(t) - sl) <= exclusion]
        if not near:
            break
        quality = _clamp(sum(_pool_microstructure_score(t) for t in near) / max(8.0 + 2.0 * len(near), 1.0), 0.0, 1.0)
        buf_atr, _ = _liquidity_zone_buffer_atr(near, side, atr, quality, max_buffer_atr)
        if side == "long":
            new_sl = min(_pool_price(t) for t in near) - buf_atr * atr
            if new_sl >= sl - 1e-9:
                break
            sl = new_sl
        else:
            new_sl = max(_pool_price(t) for t in near) + buf_atr * atr
            if new_sl <= sl + 1e-9:
                break
            sl = new_sl
    return sl


def _score_sl_zones(candidates: List[Any], side: str, entry: float, atr: float,
                    max_buffer_atr: float, min_risk: float = 0.0) -> List[Tuple[float, SLPoolPick]]:
    """Score executable protective liquidity zones, not isolated pool lines."""
    zones = _build_protective_zones(candidates, side, atr)
    scored: List[Tuple[float, SLPoolPick]] = []
    for zone in zones:
        if not zone:
            continue
        inner, outer, external_target = _zone_edges(zone, side)
        raw_zone_score = sum(_pool_microstructure_score(t) for t in zone)
        # Normalize quality by zone size; a dense zone should help, but not make
        # the buffer too tight.  The stop has to clear the envelope.
        quality = _clamp(raw_zone_score / max(8.0 + 2.5 * math.log1p(len(zone)), 1.0), 0.0, 1.0)
        buffer_atr, width_atr = _liquidity_zone_buffer_atr(zone, side, atr, quality, max_buffer_atr)
        sl_price = outer - buffer_atr * atr if side == "long" else outer + buffer_atr * atr
        sl_price = _move_stop_outside_nearby_liquidity(sl_price, side, atr, candidates, max_buffer_atr)
        risk = abs(entry - sl_price)
        if min_risk > 0.0 and risk < min_risk:
            continue
        risk_atr = risk / max(atr, 1e-9)
        density_bonus = 1.0 + 0.20 * math.log1p(len(zone))
        width_bonus = 1.0 + 0.10 * min(width_atr, 2.0)
        # Risk efficiency is a soft ranking term only. Geometry/liquidation
        # guards decide whether the final stop is tradable.
        risk_efficiency = 1.0 / (1.0 + 0.12 * max(risk_atr - 1.0, 0.0))
        selection_score = raw_zone_score * density_bonus * width_bonus * risk_efficiency
        reasons = [
            f"zone_score={raw_zone_score:.2f}",
            f"zone_n={len(zone)}",
            f"zone_width={width_atr:.2f}ATR",
            f"external_edge=${outer:.1f}",
        ]
        if len(zone) > 1:
            reasons.append("cluster-envelope stop")
        pick = SLPoolPick(
            target=external_target,
            sl_price=sl_price,
            buffer_atr=buffer_atr,
            quality=quality,
            reasons=reasons,
            zone_size=len(zone),
            zone_inner=inner,
            zone_outer=outer,
            zone_width_atr=width_atr,
        )
        scored.append((selection_score, pick))
    scored.sort(key=lambda x: x[0], reverse=True)
    return scored

# ════════════════════════════════════════════════════════════════════════════
# TP POOL SCORING
# ════════════════════════════════════════════════════════════════════════════

def score_tp_pools(
    snap,
    side:    str,
    entry:   float,
    sl:      float,
    atr:     float,
    ict:     Any = None,
    htf:     Any = None,
    min_rr:  float = 2.0,
    now:     Optional[float] = None,
    session: str = "",
    posterior_prob: float = 0.0,
) -> List[PoolScore]:
    """
    Score every candidate TP pool. Returns PoolScore[], sorted by executable
    frontier utility.

    The first element is the institutional choice.  This is not nearest-pool
    targeting: far HTF/external pools are allowed when conditional path delivery
    is positive-EV after costs, gauntlet and volatility.  If empty, no liquidity
    objective is executable; no synthetic ATR TP is created.
    """
    if snap is None or atr <= 0:
        return []

    risk = abs(float(entry) - float(sl))
    if risk < 1e-10:
        return []

    pools = list(_safe(snap, "bsl_pools", [])) if side == "long" else list(_safe(snap, "ssl_pools", []))
    if not pools:
        return []

    now_ts = now or time.time()
    selector_profile = build_market_profile(
        price=entry,
        atr=atr,
        liq_snapshot=snap,
        ict=ict,
        side=side,
        session=session,
    )
    effective_min_rr = selector_profile.min_rr(float(min_rr))
    out: List[PoolScore] = []
    be_move = _breakeven_move(entry, atr)

    for target in pools:
        try:
            pool = target.pool
            if not _is_live_liquidity_target(target, 0.0):
                continue
            dist_atr = float(_safe(target, "distance_atr", 0.0))
            pool_price = float(_safe(pool, "price", 0.0))

            # Reach gates. Too-close remains a hard veto because fees/slippage
            # and BE migration consume the whole move. Too-far is NOT a hard
            # veto: distant HTF pools are valid terminal delivery objectives,
            # but they receive a probability/reach EV penalty below.
            if dist_atr < 0.25:
                continue
            tf = str(_safe(pool, "timeframe", "5m"))
            max_reach = _max_reach_for_tf(tf)
            terminal_target = dist_atr > max_reach
            if terminal_target and dist_atr > _max_frontier_reach_for_tf(tf):
                continue
            if terminal_target and not _is_terminal_tp_candidate(target, dist_atr):
                continue

            # Buffered TP (place TP before the pool, not at it).
            buf = _reach_buffer(dist_atr, atr)
            tp_price = pool_price - buf if side == "long" else pool_price + buf

            # Validity vs entry.
            if side == "long" and tp_price <= entry + 1e-6:
                continue
            if side == "short" and tp_price >= entry - 1e-6:
                continue

            reward = abs(tp_price - entry)
            rr = reward / risk

            # ─── EV components ────────────────────────────────────────────
            raw_prob, norm_prob = _per_pool_sweep_prob(target, entry, atr, now_ts)
            if raw_prob <= 0:
                continue

            sig = float(_safe(target, "significance", 0.0))

            confluence = (
                _confluence_bonus(target)
                * _structural_bonus(pool)
                * _htf_alignment_bonus(pool, side, htf)
                * _killzone_bonus(pool, now_ts)
                * _freshness_bonus(pool)
                * _touch_penalty(pool)
            )

            reach_mult = _tp_reach_multiplier(dist_atr, tf, selector_profile, terminal_target)

            n_gauntlet, gauntlet_mult = _gauntlet_penalty(
                target, sig, snap, side, entry, atr,
            )
            rr_floor, delivery_prob, cost_r, delivery_components = _institutional_rr_floor(
                effective_min_rr,
                posterior_prob,
                raw_prob,
                confluence,
                gauntlet_mult,
                reach_mult,
                risk,
                be_move,
                rr=rr,
                distance_atr=dist_atr,
                significance=sig,
                tf=tf,
                terminal_target=terminal_target,
            )
            required_delivery_prob = _required_delivery_probability(rr, cost_r)
            if rr < rr_floor:
                continue
            utility, utility_components = _target_utility(
                delivery_prob, rr, rr_floor, dist_atr, reward, be_move)
            utility *= reach_mult
            utility_components.update(delivery_components)
            utility_components["reach_mult"] = reach_mult
            utility_components["max_reach_atr"] = max_reach

            # EV. The scaling here is intentional:
            #   - probability dominates (multiplicative base).
            #   - confluence and rr_quality are bounded by ~3-5×.
            #   - gauntlet_mult is a divisor in [0.45, 1.0].
            ev = utility * _W_PROBABILITY * confluence * gauntlet_mult
            selection_ev = _tp_selection_value(ev, rr, rr_floor, dist_atr)

            reasons: List[str] = []
            if confluence > 1.30:
                reasons.append(f"high confluence ×{confluence:.2f}")
            if n_gauntlet > 0:
                reasons.append(f"gauntlet {n_gauntlet} pools −{(1-gauntlet_mult)*100:.0f}%")
            if terminal_target:
                model = utility_components.get("delivery_model", "frontier")
                fp = float(utility_components.get("frontier_prob", 0.0) or 0.0)
                reasons.append(
                    f"terminal/frontier target ({dist_atr:.1f}>{max_reach:.1f}ATR; {model}; pathP={fp:.3f}; reach×{reach_mult:.2f})")
            if rr >= float(min_rr) + 1.0:
                reasons.append(f"R:R {rr:.1f}")
            elif rr_floor < effective_min_rr - 1e-9:
                reasons.append(f"payoff RR floor {rr_floor:.2f}")
            if utility_components["be_quality"] < 0.75:
                reasons.append("thin post-BE room")
            if _safe(pool, "ob_aligned", False):
                reasons.append("OB-aligned")
            if _safe(pool, "fvg_aligned", False):
                reasons.append("FVG-aligned")

            out.append(PoolScore(
                target       = target,
                tp_price     = tp_price,
                distance_atr = dist_atr,
                rr           = rr,
                sweep_prob   = norm_prob,
                raw_score    = raw_prob,
                confluence   = confluence,
                gauntlet_n   = n_gauntlet,
                gauntlet_pen = gauntlet_mult,
                ev           = ev,
                components   = {
                    "raw_prob":    raw_prob,
                    "confluence":  confluence,
                    "rr_quality":  utility_components["rr_utility"],
                    "be_quality":  utility_components["be_quality"],
                    "distance_quality": utility_components["distance_quality"],
                    "reach_mult":  utility_components.get("reach_mult", 1.0),
                    "max_reach_atr": utility_components.get("max_reach_atr", max_reach),
                    "selection_ev": selection_ev,
                    "be_move":     utility_components["be_move"],
                    "gauntlet":    gauntlet_mult,
                    "significance": sig,
                    "rr_floor":    rr_floor,
                    "delivery_prob": delivery_prob,
                    "direct_delivery_prob": utility_components.get("direct_prob", delivery_prob),
                    "frontier_prob": utility_components.get("frontier_prob", 0.0),
                    "frontier_decay": utility_components.get("frontier_decay", 1.0),
                    "delivery_model": utility_components.get("delivery_model", "direct"),
                    "required_delivery_prob": required_delivery_prob,
                    "posterior_prob": _clamp(posterior_prob, 0.0, 0.95),
                    "cost_r":      cost_r,
                },
                reasons      = reasons,
            ))
        except Exception as e:
            logger.debug("score_tp_pools: skipping target due to %s", e)
            continue

    out.sort(
        key=lambda p: (
            float(p.components.get("selection_ev", p.ev)),
            p.ev,
            p.rr,
        ),
        reverse=True,
    )
    return out


# ════════════════════════════════════════════════════════════════════════════
# SL POOL SCORING
# ════════════════════════════════════════════════════════════════════════════

def score_sl_pool(
    snap,
    side:                str,
    entry:               float,
    atr:                 float,
    ict:                 Any = None,
    htf:                 Any = None,
    invalidation_price:  Optional[float] = None,
    max_buffer_atr:      float = 2.0,
    now:                 Optional[float] = None,
    min_risk:            float = 0.0,
) -> Optional[SLPoolPick]:
    """
    Pick an institutional protective SL *envelope*, not a single pool line.

    The old behavior could anchor to the best-looking individual SSL/BSL and
    then place the SL only a small fixed ATR buffer behind it.  That can park a
    stop inside the broader liquidity cluster.  This version first clusters all
    live protective pools around structural invalidation, chooses the best zone,
    then places the SL beyond the zone's external edge with a buffer derived
    from zone width, density, freshness/quality and touch deterioration.
    """
    if snap is None or atr <= 0:
        return None

    min_risk = max(0.0, float(min_risk or 0.0))
    candidates, _ = _protective_candidates(snap, side, entry, atr, invalidation_price)
    if not candidates:
        return None

    scored = _score_sl_zones(
        candidates=candidates,
        side=side,
        entry=entry,
        atr=atr,
        max_buffer_atr=max_buffer_atr,
        min_risk=min_risk,
    )
    if not scored:
        return None

    return scored[0][1]



# ════════════════════════════════════════════════════════════════════════════
# INSTITUTIONAL DIAGNOSTICS — why visible pools were/weren't selected
# ════════════════════════════════════════════════════════════════════════════

def diagnose_tp_pools(
    snap,
    side:    str,
    entry:   float,
    sl:      float,
    atr:     float,
    ict:     Any = None,
    htf:     Any = None,
    min_rr:  float = 2.0,
    now:     Optional[float] = None,
    session: str = "",
    limit:   int = 8,
    posterior_prob: float = 0.0,
) -> PoolSelectionReport:
    """Return a TP candidate audit table without weakening any gate.

    LONG TP candidates must be BSL above entry.  SHORT TP candidates must be
    SSL below entry.  Rejected rows explain the first hard institutional veto:
    wrong side, already-swept/consumed, too near, too far, below required R:R,
    no sweep probability, or valid-but-not-selected by EV ranking.
    """
    report = PoolSelectionReport(role="TP", side=side, entry=float(entry), atr=float(atr))
    if snap is None:
        report.summary = "no liquidity snapshot"
        return report
    if atr <= 0:
        report.summary = "ATR unavailable"
        return report

    risk = abs(float(entry) - float(sl))
    be_move = _breakeven_move(entry, atr)
    if risk < 1e-10:
        report.summary = "invalid risk: entry and SL overlap"
        return report

    pools = list(_safe(snap, "bsl_pools", [])) if side == "long" else list(_safe(snap, "ssl_pools", []))
    if not pools:
        report.summary = f"no {'BSL' if side == 'long' else 'SSL'} pools on TP side"
        return report

    now_ts = now or time.time()
    selector_profile = build_market_profile(
        price=entry,
        atr=atr,
        liq_snapshot=snap,
        ict=ict,
        side=side,
        session=session,
    )
    effective_min_rr = selector_profile.min_rr(float(min_rr))
    rows: List[PoolCandidateDiagnostic] = []
    accepted: List[Tuple[float, PoolCandidateDiagnostic]] = []

    for target in pools:
        row = _candidate_base("TP", side, target, entry, atr, risk, effective_min_rr, be_move)
        try:
            pool = target.pool
            pool_price = float(_safe(pool, "price", 0.0))
            status = row.status.upper()
            tf = str(_safe(pool, "timeframe", "5m"))
            max_reach = _max_reach_for_tf(tf)
            terminal_target = row.distance_atr > max_reach

            if status in ("SWEPT", "CONSUMED"):
                row.reason = f"archived {status.lower()} pool; not a live target"
                rows.append(row); continue
            if row.distance_atr < 0.25:
                row.reason = "too close: no durable delivery room after buffer/costs"
                rows.append(row); continue
            if terminal_target and row.distance_atr > _max_frontier_reach_for_tf(tf):
                row.reason = (
                    f"beyond executable frontier ({row.distance_atr:.1f}>{_max_frontier_reach_for_tf(tf):.1f} ATR for {tf}); "
                    "lottery target, not institutional TP")
                rows.append(row); continue
            if terminal_target and not _is_terminal_tp_candidate(target, row.distance_atr):
                row.reason = (
                    f"distant weak LTF pool ({row.distance_atr:.1f}>{max_reach:.1f} ATR); "
                    "not a terminal objective")
                rows.append(row); continue

            buf = _reach_buffer(row.distance_atr, atr)
            row.buffer_atr = buf / max(atr, 1e-9)
            row.tp_price = pool_price - buf if side == "long" else pool_price + buf

            if side == "long" and row.tp_price <= entry + 1e-6:
                row.reason = "wrong side after TP buffer: BSL is not above entry"
                rows.append(row); continue
            if side == "short" and row.tp_price >= entry - 1e-6:
                row.reason = "wrong side after TP buffer: SSL is not below entry"
                rows.append(row); continue

            row.reward = abs(row.tp_price - entry)
            row.rr = row.reward / risk

            raw_prob, norm_prob = _per_pool_sweep_prob(target, entry, atr, now_ts)
            row.sweep_prob = norm_prob
            if raw_prob <= 0:
                row.reason = "zero sweep probability under MTF probability model"
                rows.append(row); continue

            sig = float(_safe(target, "significance", 0.0))
            confluence = (
                _confluence_bonus(target)
                * _structural_bonus(pool)
                * _htf_alignment_bonus(pool, side, htf)
                * _killzone_bonus(pool, now_ts)
                * _freshness_bonus(pool)
                * _touch_penalty(pool)
            )
            row.confluence = confluence
            reach_mult = _tp_reach_multiplier(row.distance_atr, tf, selector_profile, terminal_target)
            n_gauntlet, gauntlet_mult = _gauntlet_penalty(target, sig, snap, side, entry, atr)
            row.gauntlet_n = n_gauntlet
            rr_floor, delivery_prob, cost_r, delivery_components = _institutional_rr_floor(
                effective_min_rr,
                posterior_prob,
                raw_prob,
                confluence,
                gauntlet_mult,
                reach_mult,
                risk,
                be_move,
                rr=row.rr,
                distance_atr=row.distance_atr,
                significance=sig,
                tf=tf,
                terminal_target=terminal_target,
            )
            row.required_rr = rr_floor
            row.delivery_prob = delivery_prob
            row.cost_r = cost_r
            row.required_delivery_prob = _required_delivery_probability(row.rr, cost_r)
            if row.rr < rr_floor:
                row.reason = _tp_payoff_rejection_reason(
                    row.rr,
                    rr_floor,
                    effective_min_rr,
                    delivery_prob,
                    row.required_delivery_prob,
                    cost_r,
                    terminal_target,
                )
                rows.append(row); continue
            utility, comps = _target_utility(delivery_prob, row.rr, rr_floor, row.distance_atr, row.reward, be_move)
            comps.update(delivery_components)
            utility *= reach_mult
            row.ev = utility * _W_PROBABILITY * confluence * gauntlet_mult
            selection_ev = _tp_selection_value(row.ev, row.rr, rr_floor, row.distance_atr)
            row.selection_ev = selection_ev
            row.eligible = True
            row.reason = "eligible; payoff-adjusted EV candidate"
            row.notes = []
            if terminal_target:
                row.notes.append(
                    f"frontier TP pathP={float(comps.get('frontier_prob', 0.0) or 0.0):.3f} reach×{reach_mult:.2f}")
            if confluence > 1.30: row.notes.append(f"conf×{confluence:.2f}")
            if rr_floor < effective_min_rr - 1e-9:
                row.notes.append(f"payoff RR floor {rr_floor:.2f}")
            if selection_ev > row.ev + 1e-12:
                row.notes.append(f"frontierEV={selection_ev:.3f}")
            if n_gauntlet: row.notes.append(f"gauntlet={n_gauntlet}")
            if comps.get("be_quality", 1.0) < 0.75: row.notes.append("thin post-BE room")
            accepted.append((selection_ev, row))
            rows.append(row)
        except Exception as e:
            row.reason = f"diagnostic error: {e}"
            rows.append(row)

    accepted.sort(key=lambda x: x[0], reverse=True)
    if accepted:
        selected = accepted[0][1]
        selected.selected = True
        selected.reason = "selected by payoff-adjusted EV after all institutional gates"
        report.selected = selected
        report.summary = (f"selected ${selected.tp_price:,.1f}; RR={selected.rr:.2f}; "
                          f"EV={selected.ev:.3f}; deliveryP={selected.delivery_prob:.3f}; "
                          f"sweepP={selected.sweep_prob:.3f}")
    else:
        if rows:
            # Surface the most relevant rejection, not just "no TP".
            best_reject = max(rows, key=_candidate_report_priority)
            report.summary = f"no eligible TP pool; best visible pool rejected: {best_reject.reason}"
        else:
            report.summary = "no TP candidates found"

    report.candidates = _sort_report_candidates(rows, limit)
    return report


def diagnose_sl_pool(
    snap,
    side: str,
    entry: float,
    atr: float,
    ict: Any = None,
    htf: Any = None,
    invalidation_price: Optional[float] = None,
    max_buffer_atr: float = 2.0,
    now: Optional[float] = None,
    limit: int = 8,
    min_risk: float = 0.0,
) -> PoolSelectionReport:
    """Return an SL protective-zone audit table without relaxing SL rules."""
    report = PoolSelectionReport(role="SL", side=side, entry=float(entry), atr=float(atr))
    if snap is None:
        report.summary = "no liquidity snapshot"
        return report
    if atr <= 0:
        report.summary = "ATR unavailable"
        return report

    min_risk = max(0.0, float(min_risk or 0.0))
    rows: List[PoolCandidateDiagnostic] = []
    raw_pools = list(_safe(snap, "ssl_pools", []) or []) if side == "long" else list(_safe(snap, "bsl_pools", []) or [])
    pool_label = "SSL" if side == "long" else "BSL"
    if not raw_pools:
        report.summary = f"no protective {pool_label} pools"
        return report

    # Per-pool audit keeps /thinking useful, but selection below is zone-level.
    if side == "long":
        inv_price = invalidation_price if invalidation_price is not None else entry - 0.3 * atr
        def _pool_veto(t):
            px = _pool_price(t)
            if not _is_live_liquidity_target(t, 0.0):
                return "archived/swept/consumed pool; not protective"
            if px > inv_price - _SL_MIN_BEYOND_INVAL_ATR * atr:
                return "not beyond long invalidation"
            if (entry - px) > _SL_SEARCH_WINDOW_ATR * atr:
                return f"outside SL search window >{_SL_SEARCH_WINDOW_ATR:.1f}ATR"
            if _pool_significance(t) < _SL_MIN_SIGNIFICANCE:
                return f"significance {_pool_significance(t):.1f} < {_SL_MIN_SIGNIFICANCE:.1f}"
            return ""
    else:
        inv_price = invalidation_price if invalidation_price is not None else entry + 0.3 * atr
        def _pool_veto(t):
            px = _pool_price(t)
            if not _is_live_liquidity_target(t, 0.0):
                return "archived/swept/consumed pool; not protective"
            if px < inv_price + _SL_MIN_BEYOND_INVAL_ATR * atr:
                return "not beyond short invalidation"
            if (px - entry) > _SL_SEARCH_WINDOW_ATR * atr:
                return f"outside SL search window >{_SL_SEARCH_WINDOW_ATR:.1f}ATR"
            if _pool_significance(t) < _SL_MIN_SIGNIFICANCE:
                return f"significance {_pool_significance(t):.1f} < {_SL_MIN_SIGNIFICANCE:.1f}"
            return ""

    for target in raw_pools:
        row = _candidate_base("SL", side, target, entry, atr)
        try:
            why = _pool_veto(target)
            if why:
                row.reason = why
            else:
                row.eligible = True
                row.reason = "eligible inside protective liquidity envelope"
                row.ev = _pool_microstructure_score(target)
                row.notes = [f"pool_score={row.ev:.2f}"]
            rows.append(row)
        except Exception as e:
            row.reason = f"diagnostic error: {e}"
            rows.append(row)

    candidates, _ = _protective_candidates(snap, side, entry, atr, invalidation_price)
    scored = _score_sl_zones(
        candidates=candidates,
        side=side,
        entry=entry,
        atr=atr,
        max_buffer_atr=max_buffer_atr,
        min_risk=min_risk,
    )

    if scored:
        _, pick = scored[0]
        selected_row = _candidate_base("SL", side, pick.target, entry, atr)
        selected_row.selected = True
        selected_row.eligible = True
        selected_row.sl_price = pick.sl_price
        selected_row.buffer_atr = pick.buffer_atr
        selected_row.quality = pick.quality
        selected_row.ev = scored[0][0]
        selected_row.reason = "selected external liquidity-envelope SL"
        selected_row.notes = list(pick.reasons or [])
        report.selected = selected_row
        report.summary = (
            f"selected ${pick.sl_price:,.1f}; external edge ${pick.zone_outer:,.1f}; "
            f"zone_n={pick.zone_size}; zone_width={pick.zone_width_atr:.2f}ATR; "
            f"quality={pick.quality:.2f}; buffer={pick.buffer_atr:.2f}ATR"
        )
        # Mark the matching per-pool row as selected for row-level audit.
        for r in rows:
            if abs(r.pool_price - selected_row.pool_price) < 1e-9 and r.timeframe == selected_row.timeframe:
                r.selected = True
                r.sl_price = pick.sl_price
                r.buffer_atr = pick.buffer_atr
                r.quality = pick.quality
                r.reason = selected_row.reason
                r.notes = selected_row.notes
                break
    else:
        if rows:
            rows_sorted = _sort_report_candidates(rows, limit)
            report.summary = "no protective SL envelope; best visible pool rejected: " + (rows_sorted[0].reason or "unknown")
        else:
            report.summary = "no SL candidates found"

    report.candidates = _sort_report_candidates(rows, limit)
    return report


def select_tp_with_report(
    snap, side: str, entry: float, sl: float, atr: float,
    ict: Any = None, htf: Any = None, min_rr: float = 2.0,
    now: Optional[float] = None, session: str = "", posterior_prob: float = 0.0,
) -> Tuple[Optional[float], Optional[Any], Optional[PoolScore], PoolSelectionReport]:
    """select_tp() plus a full rejection/selection report."""
    report = diagnose_tp_pools(
        snap, side, entry, sl, atr, ict, htf, min_rr, now, session,
        posterior_prob=posterior_prob,
    )
    scores = score_tp_pools(
        snap, side, entry, sl, atr, ict, htf, min_rr, now, session,
        posterior_prob=posterior_prob,
    )
    if not scores:
        return None, None, None, report
    best = scores[0]
    return best.tp_price, best.target, best, report


def select_sl_with_report(
    snap, side: str, entry: float, atr: float,
    ict: Any = None, htf: Any = None,
    invalidation_price: Optional[float] = None,
    max_buffer_atr: float = 2.0,
    now: Optional[float] = None,
    min_risk: float = 0.0,
) -> Tuple[Optional[float], Optional[Any], Optional[SLPoolPick], PoolSelectionReport]:
    """select_sl() plus a full protective-pool report."""
    report = diagnose_sl_pool(
        snap, side, entry, atr, ict, htf, invalidation_price,
        max_buffer_atr, now, min_risk=min_risk)
    pick = score_sl_pool(
        snap, side, entry, atr, ict, htf, invalidation_price,
        max_buffer_atr, now, min_risk=min_risk)
    if pick is None:
        return None, None, None, report
    return pick.sl_price, pick.target, pick, report


# ════════════════════════════════════════════════════════════════════════════
# CONVENIENCE WRAPPERS — one-call API for entry_engine
# ════════════════════════════════════════════════════════════════════════════

def select_tp(
    snap, side: str, entry: float, sl: float, atr: float,
    ict: Any = None, htf: Any = None, min_rr: float = 2.0,
    now: Optional[float] = None, session: str = "", posterior_prob: float = 0.0,
) -> Tuple[Optional[float], Optional[Any], Optional[PoolScore]]:
    """
    One-call TP selection. Returns (tp_price, target, score) or (None, None, None).
    """
    scores = score_tp_pools(
        snap, side, entry, sl, atr, ict, htf, min_rr, now, session,
        posterior_prob=posterior_prob,
    )
    if not scores:
        return None, None, None
    best = scores[0]
    return best.tp_price, best.target, best


def select_sl(
    snap, side: str, entry: float, atr: float,
    ict: Any = None, htf: Any = None,
    invalidation_price: Optional[float] = None,
    max_buffer_atr: float = 2.0,
    now: Optional[float] = None,
    min_risk: float = 0.0,
) -> Tuple[Optional[float], Optional[Any], Optional[SLPoolPick]]:
    """
    One-call SL selection. Returns (sl_price, target, pick) or (None, None, None).
    """
    pick = score_sl_pool(snap, side, entry, atr, ict, htf,
                         invalidation_price, max_buffer_atr, now,
                         min_risk=min_risk)
    if pick is None:
        return None, None, None
    return pick.sl_price, pick.target, pick


__all__ = [
    "PoolScore", "SLPoolPick",
    "PoolCandidateDiagnostic", "PoolSelectionReport",
    "score_tp_pools", "score_sl_pool",
    "diagnose_tp_pools", "diagnose_sl_pool",
    "select_tp", "select_sl",
    "select_tp_with_report", "select_sl_with_report",
]

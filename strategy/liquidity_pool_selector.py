"""
liquidity_pool_selector.py — Unified institutional TP/SL selector v80
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
    4. LIQUIDITY-EXCLUSION SL BUFFER — the pool is the invalidation
       reference, NOT the stop. Stronger/high-touch/HTF pools receive wider
       clearance so the executable SL sits beyond the sweep envelope.
    5. EV (Expected Value) RANKING for TP — instead of "max significance"
       or "max raw R:R", we maximise:
                EV  =  P(sweep) × R_distance × confluence × (1 - gauntlet_penalty)
       which is the true institutional objective.
    6. SL POOL selection — explicit selection of the best PROTECTIVE pool
       just beyond the structural invalidation, with:
            • best all-timeframe opposing-side protective pool
            • quality/timeframe/touch-scaled exclusion buffer beyond pool price
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
    from strategy.btc_institutional_policy import (
        is_btc_context, btc_static_rr_floor, btc_durable_rr_floor, btc_sl_buffer_limits,
    )
except Exception:  # pragma: no cover
    def is_btc_context(owner=None): return False
    def btc_static_rr_floor(static_min_rr, posterior_prob=0.0): return float(static_min_rr)
    def btc_durable_rr_floor(default_floor, be_move, risk): return float(default_floor)
    def btc_sl_buffer_limits(max_buffer_atr): return float(max_buffer_atr)

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
#
# v66: the stop is NOT the liquidity. A protective pool is the structural
# invalidation reference; the executable stop must sit OUTSIDE the stop-cluster
# / sweep envelope. High-quality/high-touch/HTF pools therefore require MORE
# clearance, not less. We also evaluate every timeframe in the snapshot; a
# fixed 4ATR hard window was causing HTF pools to be ignored instead of scored.
_SL_BUFFER_BASE_ATR        = 0.30  # minimum exclusion zone beyond pool price
_SL_BUFFER_MAX_ATR         = 1.35  # hard ceiling on exclusion buffer
_SL_BUFFER_QUALITY_SCALE   = 0.55  # quality-scaled: stronger liquidity → wider buffer
_SL_SOFT_DISTANCE_ATR      = 4.0   # distance where soft capital-drag penalty begins
_SL_HARD_MAX_DISTANCE_ATR  = 18.0  # evaluate full MTF map; reject only extreme/liquidation-like anchors
_SL_MIN_BEYOND_INVAL_ATR   = 0.10  # protective pool must be past structural invalidation
_SL_MIN_SIGNIFICANCE       = 1.5   # don't anchor SL to garbage pools
_SL_LIQUIDITY_EXCLUSION_ATR = 0.30 # absolute minimum clearance away from the pool

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

# v78 unified execution-objective model.
#
# The selector must not behave like "filter on filter".  A pool either prices
# a positive-expectancy full-position exit or it does not; after that the choice
# is a single frontier score that balances expected value, delivery probability,
# and payoff.  This keeps the bot from choosing lottery/moonshot targets just
# because raw R:R is high, while still allowing HTF external liquidity when the
# delivery probability and payoff edge justify it.
try:
    import config as _liq_cfg
except Exception:  # pragma: no cover
    _liq_cfg = None  # type: ignore

def _cfg_float(name: str, default: float) -> float:
    try:
        return float(getattr(_liq_cfg, name, default)) if _liq_cfg is not None else float(default)
    except Exception:
        return float(default)

_TP_EDGE_MARGIN_R        = _cfg_float("TP_MIN_EXPECTED_VALUE_R", 0.05)   # EV buffer over pure breakeven, in R
_TP_HITRATE_REFERENCE    = _cfg_float("TP_TARGET_DELIVERY_PROB", 0.55)   # desired delivery reference for full TP
_TP_TERMINAL_FULL_HAIRCUT = 0.88  # terminal objectives are more runner-like

# SL capital efficiency.  Protective liquidity is mandatory context, but a
# far/high-significance pool must earn its extra risk.  The stop selector now
# scores "protected invalidation per unit risk" instead of simply picking the
# highest absolute pool score.  This avoids unnecessarily huge risk boxes that
# later destroy TP/RR geometry.
_SL_IDEAL_RISK_ATR       = _cfg_float("SL_IDEAL_RISK_ATR", 1.15)
_SL_CAPITAL_SOFT_ATR     = _cfg_float("SL_MAX_CAPITAL_DRAG_ATR", 4.50)
_SL_CAPITAL_DECAY        = 0.26


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
        ev_r = float((self.components or {}).get("expected_value_r", 0.0) or 0.0)
        p_del = float((self.components or {}).get("delivery_prob", self.sweep_prob) or 0.0)
        frontier = float((self.components or {}).get("selection_ev", self.ev) or 0.0)
        return (f"PoolScore(tp=${self.tp_price:,.1f} "
                f"dist={self.distance_atr:.1f}ATR rr={self.rr:.2f} "
                f"Pdel={p_del:.2f} EV_R={ev_r:.3f} frontier={frontier:.3f})")


@dataclass
class SLPoolPick:
    """The chosen protective pool and the buffered SL price."""
    target:       Any                    # PoolTarget on the OPPOSING side
    sl_price:     float                  # pool.price ± buffer
    buffer_atr:   float
    quality:      float                  # composite [0, 1] quality of the pick
    reasons:      List[str] = field(default_factory=list)


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
    expected_value_r: float = 0.0
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


def _expected_value_r(delivery_prob: float, rr: float, cost_r: float) -> float:
    """Expected value of the TP in R after the full loss and execution cost.

    Formula: EV_R = p * reward_R - (1 - p) * 1R - cost_R.
    This is the single payoff truth used by TP diagnostics and ranking; it is
    intentionally separate from raw pool score so high-RR moonshots cannot look
    institutional unless they also have enough delivery probability.
    """
    p = _clamp(float(delivery_prob), 0.0, 1.0)
    r = max(float(rr), 0.0)
    c = max(float(cost_r), 0.0)
    return p * r - (1.0 - p) - c


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




def _pool_status_text(pool: Any) -> str:
    """Return normalized pool status text for live-vs-archived gates."""
    return _pool_status(pool).upper()


def _is_live_pool(pool: Any) -> bool:
    """Only unswept, unconsumed liquidity can anchor TP/SL execution.

    Swept/consumed pools remain useful as context for adjacency and market
    narrative, but they must never be selected as a fresh delivery target or
    protective stop anchor.
    """
    status = _pool_status_text(pool)
    return status not in ("SWEPT", "CONSUMED")


def _is_tp_pool_side(target: Any, side: str) -> bool:
    pool = _safe(target, "pool", None)
    pside = _pool_side(pool).upper()
    if not pside:
        # Some compatibility snapshots separate BSL/SSL by collection
        # but omit pool.side on the object.  Accept unknown here; explicit wrong
        # side values are still rejected below.
        return True
    if side == "long":
        return "BSL" in pside
    return "SSL" in pside


def _is_sl_pool_side(target: Any, side: str) -> bool:
    pool = _safe(target, "pool", None)
    pside = _pool_side(pool).upper()
    if not pside:
        # Collection membership is authoritative for older snapshots that do
        # not carry pool.side; explicit wrong side values remain hard-rejected.
        return True
    if side == "long":
        return "SSL" in pside
    return "BSL" in pside


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
    """Blend setup posterior with the pool's own delivery probability."""
    pool_prob = _clamp(raw_prob, _TP_DELIVERY_PROB_FLOOR, 0.95)
    pool_prob *= math.sqrt(_clamp(confluence, 0.25, 2.25))
    pool_prob *= math.sqrt(_clamp(gauntlet_mult, 0.45, 1.00))
    pool_prob *= math.sqrt(_clamp(reach_mult, 0.05, 1.25))
    pool_prob = _clamp(pool_prob, _TP_DELIVERY_PROB_FLOOR, 0.95)

    posterior = _clamp(posterior_prob, 0.0, 0.95)
    if posterior <= 0.0:
        return pool_prob
    return _clamp(math.sqrt(pool_prob * posterior), _TP_DELIVERY_PROB_FLOOR, 0.93)


def _institutional_rr_floor(
    static_min_rr: float,
    posterior_prob: float,
    raw_prob: float,
    confluence: float,
    gauntlet_mult: float,
    reach_mult: float,
    risk: float,
    be_move: float,
) -> Tuple[float, float, float]:
    """Return (required_rr, delivery_probability, cost_r).

    With no accepted setup posterior, preserve the static compatibility floor. Once a
    posterior exists, the pool is judged by positive expected value:
        p * R - (1 - p) - cost_r >= 0
    The static floor can be relaxed by observed auction posterior, but only
    down to a durable payoff floor. If costs consume too much of the risk box,
    the floor expands above the static prior instead of approving a micro-win.
    """
    static_floor = max(0.01, float(static_min_rr))
    risk_f = max(float(risk), 1e-9)
    if is_btc_context():
        static_floor = btc_static_rr_floor(static_floor, posterior_prob)
        durable_floor = btc_durable_rr_floor(_TP_DURABLE_RR_FLOOR, be_move, risk_f)
    else:
        durable_floor = max(
            _TP_DURABLE_RR_FLOOR,
            _TP_MIN_BE_MOVE_MULT * max(float(be_move), 0.0) / risk_f,
        )
    cost_r = _clamp(0.75 * float(be_move) / risk_f, 0.0, 0.65)
    posterior = _clamp(posterior_prob, 0.0, 0.95)
    if posterior <= 0.0:
        delivery_p = _posterior_delivery_probability(
            raw_prob, 0.0, confluence, gauntlet_mult, reach_mult)
        ev_floor = ((1.0 - delivery_p) + cost_r + _TP_EDGE_MARGIN_R) / max(delivery_p, 1e-9)
        return max(static_floor, durable_floor, ev_floor), delivery_p, cost_r

    delivery_p = _posterior_delivery_probability(
        raw_prob, posterior, confluence, gauntlet_mult, reach_mult)
    ev_floor = ((1.0 - delivery_p) + cost_r + _TP_EDGE_MARGIN_R) / max(delivery_p, 1e-9)
    posterior_floor = max(durable_floor, ev_floor)
    return posterior_floor, delivery_p, cost_r


def _tp_reach_multiplier(distance_atr: float, tf: str, selector_profile: MarketProfile,
                         terminal_target: bool) -> float:
    mtf_reach = _terminal_reach_multiplier(distance_atr, tf)
    profile_reach = selector_profile.target_reach_penalty(distance_atr, tf)
    if terminal_target:
        profile_reach = max(profile_reach, _TP_TERMINAL_PROFILE_FLOOR)
    return mtf_reach * profile_reach


def _tp_selection_value(
    ev: float,
    rr: float,
    rr_floor: float,
    distance_atr: float,
    delivery_prob: float = 0.0,
    required_delivery_prob: float = 0.0,
    terminal_target: bool = False,
) -> float:
    """Single institutional TP frontier score.

    We want a target that the market is likely to deliver to *and* that pays
    enough R after costs.  Raw R:R alone is not rewarded; a distant target is
    only promoted when probability clears the breakeven hit-rate and the EV is
    already positive.
    """
    surplus_r = max(float(rr) - float(rr_floor), 0.0)
    payoff_frontier = math.sqrt(max(float(rr), 0.0)) * (1.0 + min(surplus_r * 0.18, 0.70))
    p = _clamp(float(delivery_prob), 0.0, 1.0)
    req_p = max(float(required_delivery_prob), 1e-9)
    hitrate_fit = _clamp(p / req_p, 0.35, 1.35)
    hitrate_frontier = 0.70 + 0.30 * min(hitrate_fit, 1.35)
    # Mildly penalise distance after the normal MTF reach model has already
    # been applied.  This keeps full-position TP biased toward reachable
    # liquidity and leaves very distant HTF pools for trailing/runner context.
    excess_distance = max(float(distance_atr) - 5.0, 0.0)
    distance_efficiency = 1.0 / (1.0 + 0.018 * excess_distance * excess_distance)
    terminal_mult = _TP_TERMINAL_FULL_HAIRCUT if terminal_target else 1.0
    return float(ev) * payoff_frontier * hitrate_frontier * distance_efficiency * terminal_mult


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


def _target_utility(raw_prob: float, rr: float, min_rr: float,
                    distance_atr: float, reward: float,
                    be_move: float) -> Tuple[float, Dict[str, float]]:
    """
    Convert hit probability into trade utility.

    Near pools often have high sweep probability but too little room after
    fees, slippage, and BE migration. This utility still respects probability,
    but pays for durable R and distance beyond the cost envelope.
    """
    rr_excess = max(0.0, rr - float(min_rr))
    rr_utility = math.sqrt(max(rr, 0.0)) * (1.0 + min(rr_excess * 0.35, 1.25))
    delivery_room = max(0.0, reward - be_move)
    be_surplus_atr = delivery_room / max(be_move, 1e-9)
    be_quality = 0.35 + 0.65 * min(be_surplus_atr / 2.0, 1.0)
    distance_quality = 1.0 - math.exp(-max(distance_atr - 0.35, 0.0) / 1.8)
    distance_quality = max(0.25, distance_quality)
    utility = raw_prob * rr_utility * be_quality * distance_quality
    return utility, {
        "rr_utility": rr_utility,
        "be_quality": be_quality,
        "distance_quality": distance_quality,
        "be_move": be_move,
    }


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
    Score every candidate TP pool. Returns PoolScore[], sorted by EV desc.

    The first element is the institutional choice. If empty, no pool meets
    the constraints — caller should reject or wait; no synthetic ATR TP is created.
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
    if is_btc_context():
        effective_min_rr = btc_static_rr_floor(effective_min_rr, posterior_prob)
    out: List[PoolScore] = []
    be_move = _breakeven_move(entry, atr)

    for target in pools:
        try:
            pool = target.pool
            dist_atr = float(_safe(target, "distance_atr", 0.0))
            pool_price = float(_safe(pool, "price", 0.0))

            if not _is_live_pool(pool):
                continue
            if not _is_tp_pool_side(target, side):
                continue

            # Reach gates. Too-close remains a hard veto because fees/slippage
            # and BE migration consume the whole move. Too-far is NOT a hard
            # veto: distant HTF pools are valid terminal delivery objectives,
            # but they receive a probability/reach EV penalty below.
            if dist_atr < 0.25:
                continue
            tf = str(_safe(pool, "timeframe", "5m"))
            max_reach = _max_reach_for_tf(tf)
            terminal_target = dist_atr > max_reach
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
            rr_floor, delivery_prob, cost_r = _institutional_rr_floor(
                effective_min_rr,
                posterior_prob,
                raw_prob,
                confluence,
                gauntlet_mult,
                reach_mult,
                risk,
                be_move,
            )
            required_delivery_prob = _required_delivery_probability(rr, cost_r)
            min_delivery_prob = min(_TP_HITRATE_REFERENCE, max(_cfg_float("TP_MIN_DELIVERY_PROB", 0.32), required_delivery_prob * 0.82))
            if rr < rr_floor:
                continue
            expected_value_r = _expected_value_r(delivery_prob, rr, cost_r)
            if delivery_prob < min_delivery_prob:
                continue
            if expected_value_r < _TP_EDGE_MARGIN_R - 1e-9:
                continue
            utility, utility_components = _target_utility(
                raw_prob, rr, rr_floor, dist_atr, reward, be_move)
            utility *= reach_mult
            utility_components["reach_mult"] = reach_mult
            utility_components["max_reach_atr"] = max_reach

            # EV. The scaling here is intentional:
            #   - probability dominates (multiplicative base).
            #   - confluence and rr_quality are bounded by ~3-5×.
            #   - gauntlet_mult is a divisor in [0.45, 1.0].
            ev = utility * _W_PROBABILITY * confluence * gauntlet_mult
            selection_ev = _tp_selection_value(
                ev, rr, rr_floor, dist_atr,
                delivery_prob=delivery_prob,
                required_delivery_prob=required_delivery_prob,
                terminal_target=terminal_target,
            )

            reasons: List[str] = []
            if confluence > 1.30:
                reasons.append(f"high confluence ×{confluence:.2f}")
            if n_gauntlet > 0:
                reasons.append(f"gauntlet {n_gauntlet} pools −{(1-gauntlet_mult)*100:.0f}%")
            if terminal_target:
                reasons.append(
                    f"terminal target beyond first-sweep reach ({dist_atr:.1f}>{max_reach:.1f}ATR; reach×{reach_mult:.2f})")
            if rr >= float(min_rr) + 1.0:
                reasons.append(f"R:R {rr:.1f}")
            elif rr_floor < effective_min_rr - 1e-9:
                reasons.append(f"payoff RR floor {rr_floor:.2f}")
            if is_btc_context() and effective_min_rr < float(min_rr) - 1e-9:
                reasons.append("BTC probability-weighted liquidity path")
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
                    "required_delivery_prob": required_delivery_prob,
                    "min_delivery_prob": min_delivery_prob,
                    "expected_value_r": expected_value_r,
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

def _sl_pool_distance_penalty(distance_atr: float, tf: str) -> float:
    """Soft capital-drag penalty for protective pools far from entry."""
    tf_rank = _tf_rank(tf)
    soft = _SL_SOFT_DISTANCE_ATR + 1.25 * max(0, tf_rank - 2)
    excess = max(0.0, float(distance_atr) - soft)
    return 1.0 / (1.0 + 0.18 * excess * excess)


def _sl_capital_efficiency(risk_atr: float, tf: str, quality: float) -> float:
    """Risk-box efficiency multiplier for SL selection.

    A farther SL is only institutionally better when it buys real protection.
    This multiplier lets HTF/high-quality anchors survive, but prevents a far
    pool from destroying R:R when a nearer valid invalidation shelf exists.
    """
    tf_rank = _tf_rank(tf)
    risk = max(float(risk_atr), 0.0)
    q = _clamp(float(quality), 0.0, 1.0)
    soft = _SL_CAPITAL_SOFT_ATR + 0.80 * max(0, tf_rank - 2) + 0.75 * q
    if risk <= 0.0:
        return 0.0
    if risk < _SL_IDEAL_RISK_ATR:
        # Do not over-reward tiny/tight stops.  A small stop is only useful if
        # it is not sitting inside noise/liquidity; min_risk and buffer checks
        # handle the hard safety floor.
        return 0.72 + 0.28 * (risk / max(_SL_IDEAL_RISK_ATR, 1e-9))
    excess = max(0.0, risk - soft)
    return 1.0 / (1.0 + _SL_CAPITAL_DECAY * excess * excess)


def _sl_liquidity_buffer_atr(target: Any, quality: float, *, max_buffer_atr: float = 2.0) -> float:
    """Executable stop clearance beyond the liquidity pool.

    Stronger pool = more stop concentration = larger sweep envelope. The
    previous inverse model made high-quality pools use the smallest buffer,
    effectively placing the SL inside/next to the same liquidity cluster.
    """
    pool = _safe(target, "pool", None)
    if is_btc_context():
        max_buffer_atr = btc_sl_buffer_limits(max_buffer_atr)
    tf_rank = _tf_rank(str(_safe(pool, "timeframe", "5m")))
    touches = max(1, int(_safe(pool, "touches", 1) or 1))
    touch_term = min(0.24, 0.035 * max(0, touches - 2))
    tf_term = min(0.22, 0.045 * max(0, tf_rank - 2))
    q = _clamp(float(quality), 0.0, 1.0)
    buf = _SL_BUFFER_BASE_ATR + _SL_BUFFER_QUALITY_SCALE * q + touch_term + tf_term
    buf = max(buf, _SL_LIQUIDITY_EXCLUSION_ATR)
    return min(buf, _SL_BUFFER_MAX_ATR, max(float(max_buffer_atr or 0.0), _SL_LIQUIDITY_EXCLUSION_ATR))


def _sl_pool_score(target: Any, *, entry: float, atr: float, side: str) -> Tuple[float, float, float, List[str]]:
    """Return (score, quality, distance_atr, notes) for a protective SL anchor."""
    pool = _safe(target, "pool", None)
    pool_price = float(_safe(pool, "price", 0.0) or 0.0)
    dist_atr = float(_safe(target, "distance_atr", 0.0) or 0.0)
    if dist_atr <= 0.0 and atr > 0.0 and pool_price > 0.0:
        dist_atr = abs(float(entry) - pool_price) / max(float(atr), 1e-9)
    sig = float(_safe(target, "significance", 0.0) or 0.0)
    struct = _structural_bonus(pool)
    fresh = _freshness_bonus(pool)
    touch = _touch_penalty(pool)
    tf = str(_safe(pool, "timeframe", "5m") or "5m")
    tf_rank = _tf_rank(tf)
    tf_mult = 1.0 + 0.09 * max(0, tf_rank - 2)
    distance_mult = _sl_pool_distance_penalty(dist_atr, tf)
    score = sig * struct * fresh * touch * tf_mult * distance_mult
    quality = _clamp(score / 12.0, 0.0, 1.0)
    notes = [f"score={score:.2f}", f"dist={dist_atr:.1f}ATR", f"tf={tf}"]
    if distance_mult < 0.90:
        notes.append(f"soft distance penalty×{distance_mult:.2f}")
    return score, quality, dist_atr, notes


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
    """Pick the best all-timeframe protective liquidity/invalidation shelf."""
    if snap is None or atr <= 0:
        return None
    side = (side or "").lower()
    if side not in ("long", "short"):
        return None
    if side == "long":
        pools = list(_safe(snap, "ssl_pools", []) or [])
        inv_price = float(invalidation_price if invalidation_price is not None else entry - 0.3 * atr)
        def _protects(t: Any) -> bool:
            px = float(_safe(t.pool, "price", 0.0) or 0.0)
            return px <= inv_price - _SL_MIN_BEYOND_INVAL_ATR * atr
    else:
        pools = list(_safe(snap, "bsl_pools", []) or [])
        inv_price = float(invalidation_price if invalidation_price is not None else entry + 0.3 * atr)
        def _protects(t: Any) -> bool:
            px = float(_safe(t.pool, "price", 0.0) or 0.0)
            return px >= inv_price + _SL_MIN_BEYOND_INVAL_ATR * atr
    min_risk = max(0.0, float(min_risk or 0.0))
    scored: List[Tuple[float, Any, float, float, float, List[str]]] = []
    for t in pools:
        pool = _safe(t, "pool", None)
        if not _is_live_pool(pool):
            continue
        if not _is_sl_pool_side(t, side):
            continue
        if not _protects(t):
            continue
        if float(_safe(t, "significance", 0.0) or 0.0) < _SL_MIN_SIGNIFICANCE:
            continue
        score, quality, dist_atr, notes = _sl_pool_score(t, entry=entry, atr=atr, side=side)
        if dist_atr > _SL_HARD_MAX_DISTANCE_ATR:
            continue
        buffer_atr = _sl_liquidity_buffer_atr(t, quality, max_buffer_atr=max_buffer_atr)
        pool_price = float(_safe(pool, "price", 0.0) or 0.0)
        sl_price = (pool_price - buffer_atr * atr) if side == "long" else (pool_price + buffer_atr * atr)
        risk_atr = abs(float(entry) - sl_price) / max(float(atr), 1e-9)
        if min_risk > 0.0 and abs(float(entry) - sl_price) < min_risk:
            continue
        cap_eff = _sl_capital_efficiency(risk_atr, tf=str(_safe(pool, "timeframe", "5m") or "5m"), quality=quality)
        capital_score = score * cap_eff
        scored.append((capital_score, t, sl_price, buffer_atr, quality, notes + [f"capEff×{cap_eff:.2f}", f"risk={risk_atr:.2f}ATR"]))
    if not scored:
        return None
    scored.sort(key=lambda x: x[0], reverse=True)
    best_score, best_target, sl_price, buffer_atr, quality, notes = scored[0]
    reasons: List[str] = list(notes)
    reasons.append("outside-liquidity-zone")
    if _safe(best_target.pool, "ob_aligned", False):
        reasons.append("OB-aligned")
    touches = int(_safe(best_target.pool, "touches", 1) or 1)
    if touches > 3:
        reasons.append(f"{touches} touches (stop-density widened)")
    return SLPoolPick(
        target     = best_target,
        sl_price   = sl_price,
        buffer_atr = buffer_atr,
        quality    = quality,
        reasons    = reasons,
    )



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
    if is_btc_context():
        effective_min_rr = btc_static_rr_floor(effective_min_rr, posterior_prob)
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
            if not _is_tp_pool_side(target, side):
                expected = "BSL" if side == "long" else "SSL"
                row.reason = f"wrong pool side for TP; expected live {expected}"
                rows.append(row); continue
            if row.distance_atr < 0.25:
                row.reason = "too close: no durable delivery room after buffer/costs"
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
            rr_floor, delivery_prob, cost_r = _institutional_rr_floor(
                effective_min_rr,
                posterior_prob,
                raw_prob,
                confluence,
                gauntlet_mult,
                reach_mult,
                risk,
                be_move,
            )
            row.required_rr = rr_floor
            row.delivery_prob = delivery_prob
            row.cost_r = cost_r
            row.required_delivery_prob = _required_delivery_probability(row.rr, cost_r)
            row.expected_value_r = _expected_value_r(delivery_prob, row.rr, cost_r)
            min_delivery_prob = min(_TP_HITRATE_REFERENCE, max(_cfg_float("TP_MIN_DELIVERY_PROB", 0.32), row.required_delivery_prob * 0.82))
            if delivery_prob < min_delivery_prob:
                row.reason = (f"delivery probability {delivery_prob:.4f} < institutional minimum {min_delivery_prob:.4f}; "
                              f"RR {row.rr:.2f}, EV_R={row.expected_value_r:.2f}")
                rows.append(row); continue
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
            if row.expected_value_r < _TP_EDGE_MARGIN_R - 1e-9:
                row.reason = (
                    f"expected value {row.expected_value_r:.2f}R < edge margin "
                    f"{_TP_EDGE_MARGIN_R:.2f}R despite RR {row.rr:.2f}"
                )
                rows.append(row); continue
            utility, comps = _target_utility(raw_prob, row.rr, rr_floor, row.distance_atr, row.reward, be_move)
            utility *= reach_mult
            row.ev = utility * _W_PROBABILITY * confluence * gauntlet_mult
            selection_ev = _tp_selection_value(
                row.ev, row.rr, rr_floor, row.distance_atr,
                delivery_prob=delivery_prob,
                required_delivery_prob=row.required_delivery_prob,
                terminal_target=terminal_target,
            )
            row.selection_ev = selection_ev
            row.eligible = True
            row.reason = "eligible; payoff-adjusted EV candidate"
            row.notes = []
            if terminal_target:
                row.notes.append(f"terminal TP; reach×{reach_mult:.2f}")
            if confluence > 1.30: row.notes.append(f"conf×{confluence:.2f}")
            if rr_floor < effective_min_rr - 1e-9:
                row.notes.append(f"payoff RR floor {rr_floor:.2f}")
            if selection_ev > row.ev + 1e-12:
                row.notes.append(f"frontierEV={selection_ev:.3f}")
            row.notes.append(f"EV_R={row.expected_value_r:.2f}")
            row.notes.append(f"Pmin={min_delivery_prob:.2f}")
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
                          f"EV={selected.ev:.3f}; P={selected.sweep_prob:.2f}")
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
    """Return an SL protective-pool audit table without relaxing SL rules."""
    report = PoolSelectionReport(role="SL", side=side, entry=float(entry), atr=float(atr))
    if snap is None:
        report.summary = "no liquidity snapshot"
        return report
    if atr <= 0:
        report.summary = "ATR unavailable"
        return report
    side = (side or "").lower()
    min_risk = max(0.0, float(min_risk or 0.0))
    rows: List[PoolCandidateDiagnostic] = []
    candidates: List[Tuple[float, PoolCandidateDiagnostic, Any]] = []
    if side == "long":
        pools = list(_safe(snap, "ssl_pools", []) or [])
        inv_price = float(invalidation_price if invalidation_price is not None else entry - 0.3 * atr)
        expected = "SSL"
        def _protects(t):
            px = float(_safe(t.pool, "price", 0.0) or 0.0)
            if px > inv_price - _SL_MIN_BEYOND_INVAL_ATR * atr:
                return False, "not beyond long invalidation"
            return True, ""
    else:
        pools = list(_safe(snap, "bsl_pools", []) or [])
        inv_price = float(invalidation_price if invalidation_price is not None else entry + 0.3 * atr)
        expected = "BSL"
        def _protects(t):
            px = float(_safe(t.pool, "price", 0.0) or 0.0)
            if px < inv_price + _SL_MIN_BEYOND_INVAL_ATR * atr:
                return False, "not beyond short invalidation"
            return True, ""
    if not pools:
        report.summary = f"no protective {expected} pools"
        return report
    for target in pools:
        row = _candidate_base("SL", side, target, entry, atr)
        try:
            status = row.status.upper()
            if status in ("SWEPT", "CONSUMED"):
                row.reason = f"archived {status.lower()} pool; context only, not executable SL anchor"
                rows.append(row); continue
            if not _is_sl_pool_side(target, side):
                row.reason = f"wrong pool side for protective SL; expected live {expected}"
                rows.append(row); continue
            ok, why = _protects(target)
            if not ok:
                row.reason = why
                rows.append(row); continue
            if row.significance < _SL_MIN_SIGNIFICANCE:
                row.reason = f"significance {row.significance:.1f} < {_SL_MIN_SIGNIFICANCE:.1f}"
                rows.append(row); continue
            score, quality, dist_atr, notes = _sl_pool_score(target, entry=entry, atr=atr, side=side)
            row.distance_atr = dist_atr
            if dist_atr > _SL_HARD_MAX_DISTANCE_ATR:
                row.reason = f"extreme SL anchor distance {dist_atr:.1f}ATR > {_SL_HARD_MAX_DISTANCE_ATR:.1f}ATR"
                rows.append(row); continue
            buffer_atr = _sl_liquidity_buffer_atr(target, quality, max_buffer_atr=max_buffer_atr)
            pool_price = float(_safe(target.pool, "price", 0.0) or 0.0)
            row.sl_price = (pool_price - buffer_atr * atr) if side == "long" else (pool_price + buffer_atr * atr)
            row.buffer_atr = buffer_atr
            row.quality = quality
            risk_atr = abs(float(entry) - row.sl_price) / max(float(atr), 1e-9)
            cap_eff = _sl_capital_efficiency(risk_atr, tf=str(_safe(target.pool, "timeframe", "5m") or "5m"), quality=quality)
            row.ev = score * cap_eff
            row.notes = list(notes) + [
                "SL beyond liquidity-exclusion zone",
                f"capEff×{cap_eff:.2f}",
                f"risk={risk_atr:.2f}ATR",
            ]
            if min_risk > 0.0 and abs(entry - row.sl_price) < min_risk:
                row.reason = f"risk {abs(entry - row.sl_price):.1f}pts < required {min_risk:.1f}pts"
                rows.append(row); continue
            row.eligible = True
            row.reason = "eligible all-timeframe protective SL anchor"
            candidates.append((score, row, target))
            rows.append(row)
        except Exception as e:
            row.reason = f"diagnostic error: {e}"
            rows.append(row)
    candidates.sort(key=lambda x: x[0], reverse=True)
    if candidates:
        selected = candidates[0][1]
        selected.selected = True
        selected.reason = "selected all-timeframe protective SL anchor; executable SL outside liquidity"
        report.selected = selected
        report.summary = (f"selected ${selected.sl_price:,.1f}; anchor ${selected.pool_price:,.1f}; "
                          f"quality={selected.quality:.2f}; buffer={selected.buffer_atr:.2f}ATR")
    else:
        if rows:
            best_reject = max(rows, key=_candidate_report_priority)
            report.summary = "no protective SL pool; best visible pool rejected: " + best_reject.reason
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

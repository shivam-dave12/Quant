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

# Killzones (UTC). London 07-10, NY 12-16. Bonus active during + previous KZ.
_KILLZONE_HOURS = (7, 8, 9, 10, 12, 13, 14, 15, 16)


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
    ev:            float = 0.0
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
    def _key(r: PoolCandidateDiagnostic):
        return (0 if r.selected else 1 if r.eligible else 2, -r.ev, -r.rr, r.distance_atr)
    return sorted(rows, key=_key)[:max(1, int(limit))]


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
            return 0.0, 0.0
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
) -> List[PoolScore]:
    """
    Score every candidate TP pool. Returns PoolScore[], sorted by EV desc.

    The first element is the institutional choice. If empty, no pool meets
    the constraints — caller should fall back to ATR-based TP.
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
    out: List[PoolScore] = []
    be_move = _breakeven_move(entry, atr)

    for target in pools:
        try:
            pool = target.pool
            dist_atr = float(_safe(target, "distance_atr", 0.0))
            pool_price = float(_safe(pool, "price", 0.0))

            # Reach gates: skip pools too close (no R:R) or too far (low hit-rate).
            if dist_atr < 0.25:
                continue
            if dist_atr > _MAX_REACH_ATR.get(str(_safe(pool, "timeframe", "5m")), 12.0):
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
            if rr < float(min_rr):
                continue

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

            utility, utility_components = _target_utility(
                raw_prob, rr, float(min_rr), dist_atr, reward, be_move)

            n_gauntlet, gauntlet_mult = _gauntlet_penalty(
                target, sig, snap, side, entry, atr,
            )

            # EV. The scaling here is intentional:
            #   - probability dominates (multiplicative base).
            #   - confluence and rr_quality are bounded by ~3-5×.
            #   - gauntlet_mult is a divisor in [0.45, 1.0].
            ev = utility * _W_PROBABILITY * confluence * gauntlet_mult

            reasons: List[str] = []
            if confluence > 1.30:
                reasons.append(f"high confluence ×{confluence:.2f}")
            if n_gauntlet > 0:
                reasons.append(f"gauntlet {n_gauntlet} pools −{(1-gauntlet_mult)*100:.0f}%")
            if rr >= float(min_rr) + 1.0:
                reasons.append(f"R:R {rr:.1f}")
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
                    "be_move":     utility_components["be_move"],
                    "gauntlet":    gauntlet_mult,
                    "significance": sig,
                },
                reasons      = reasons,
            ))
        except Exception as e:
            logger.debug("score_tp_pools: skipping target due to %s", e)
            continue

    out.sort(key=lambda p: p.ev, reverse=True)
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
) -> Optional[SLPoolPick]:
    """
    Pick the best PROTECTIVE pool just past the structural invalidation
    point, with a quality-scaled buffer.

    Logic:
        - Pool side = OPPOSING side of trade (longs invalidated by SSL pool
          break; shorts invalidated by BSL pool break).
        - Pool must lie BEYOND invalidation_price (or beyond entry by
          _SL_MIN_BEYOND_INVAL_ATR if invalidation_price not provided).
        - Within _SL_SEARCH_WINDOW_ATR of invalidation point.
        - Score = significance × structural × htf_alignment × freshness
                  × adjacency_bonus  −  touch_penalty
        - SL = pool.price ∓ buffer, where buffer scales INVERSELY with quality.

    Returns None if no protective pool qualifies — caller should fall back
    to OB-based or ATR-based SL.
    """
    if snap is None or atr <= 0:
        return None

    # OPPOSING-side pools protect us. Long → SSL pool below us is the floor.
    if side == "long":
        opposing = list(_safe(snap, "ssl_pools", []) or [])
        # Invalidation: by default, the lowest reasonable swing below entry.
        inv_price = invalidation_price if invalidation_price is not None else entry - 0.3 * atr
        # Protective pools sit BELOW inv_price.
        candidates = [t for t in opposing
                      if _safe(t.pool, "price", 0.0) <= inv_price - _SL_MIN_BEYOND_INVAL_ATR * atr
                      and (entry - _safe(t.pool, "price", 0.0)) <= _SL_SEARCH_WINDOW_ATR * atr]
    else:
        opposing = list(_safe(snap, "bsl_pools", []) or [])
        inv_price = invalidation_price if invalidation_price is not None else entry + 0.3 * atr
        candidates = [t for t in opposing
                      if _safe(t.pool, "price", 0.0) >= inv_price + _SL_MIN_BEYOND_INVAL_ATR * atr
                      and (_safe(t.pool, "price", 0.0) - entry) <= _SL_SEARCH_WINDOW_ATR * atr]

    candidates = [t for t in candidates
                  if _safe(t, "significance", 0.0) >= _SL_MIN_SIGNIFICANCE]
    if not candidates:
        return None

    # Score each candidate.
    scored: List[Tuple[float, Any]] = []
    for t in candidates:
        sig = float(_safe(t, "significance", 0.0))
        struct = _structural_bonus(t.pool)
        fresh  = _freshness_bonus(t.pool)
        touch  = _touch_penalty(t.pool)

        # SL pools benefit from being ALIGNED with HTF on the OPPOSING side
        # (i.e. for a LONG, an SSL pool aligned with bullish HTF means
        # institutions defended that level). _htf_alignment_bonus already
        # handles this — for a long-side trade looking at SSL pools, it
        # returns 1.0 (no bonus) which is correct: we don't WANT bias here,
        # we want raw structural strength. We therefore neutralise htf_m.
        score = sig * struct * fresh * touch
        scored.append((score, t))

    scored.sort(key=lambda x: x[0], reverse=True)
    best_score, best_target = scored[0]

    # Quality on a [0, 1] scale: a score of ~10 is institutional-grade.
    quality = min(best_score / 10.0, 1.0)

    # Quality-scaled buffer:
    #   high quality (1.0) → smallest buffer (BASE)
    #   low quality (0.0)  → maximum buffer (BASE + scale × MAX)
    buffer_atr = _SL_BUFFER_BASE_ATR + (1.0 - quality) * _SL_BUFFER_QUALITY_SCALE
    buffer_atr = min(buffer_atr, _SL_BUFFER_MAX_ATR, max_buffer_atr)

    pool_price = float(_safe(best_target.pool, "price", 0.0))
    sl_price = (pool_price - buffer_atr * atr) if side == "long" else (pool_price + buffer_atr * atr)

    reasons: List[str] = [f"score={best_score:.2f}", f"sig={float(_safe(best_target, 'significance', 0.0)):.1f}"]
    if _safe(best_target.pool, "ob_aligned", False):
        reasons.append("OB-aligned")
    if int(_safe(best_target.pool, "touches", 1)) > 3:
        reasons.append(f"{int(_safe(best_target.pool, 'touches', 1))} touches (penalised)")

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
    rows: List[PoolCandidateDiagnostic] = []
    accepted: List[Tuple[float, PoolCandidateDiagnostic]] = []

    for target in pools:
        row = _candidate_base("TP", side, target, entry, atr, risk, float(min_rr), be_move)
        try:
            pool = target.pool
            pool_price = float(_safe(pool, "price", 0.0))
            status = row.status.upper()
            max_reach = _MAX_REACH_ATR.get(str(_safe(pool, "timeframe", "5m")), 12.0)

            if status in ("SWEPT", "CONSUMED"):
                row.reason = f"archived {status.lower()} pool; not a live target"
                rows.append(row); continue
            if row.distance_atr < 0.25:
                row.reason = "too close: no durable delivery room after buffer/costs"
                rows.append(row); continue
            if row.distance_atr > max_reach:
                row.reason = f"too far for TF reach model ({row.distance_atr:.1f}>{max_reach:.1f} ATR)"
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
            if row.rr < float(min_rr):
                row.reason = f"R:R {row.rr:.2f} < required {float(min_rr):.2f}"
                rows.append(row); continue

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
            utility, comps = _target_utility(raw_prob, row.rr, float(min_rr), row.distance_atr, row.reward, be_move)
            n_gauntlet, gauntlet_mult = _gauntlet_penalty(target, sig, snap, side, entry, atr)
            row.gauntlet_n = n_gauntlet
            row.ev = utility * _W_PROBABILITY * confluence * gauntlet_mult
            row.eligible = True
            row.reason = "eligible; EV-ranked candidate"
            row.notes = []
            if confluence > 1.30: row.notes.append(f"conf×{confluence:.2f}")
            if n_gauntlet: row.notes.append(f"gauntlet={n_gauntlet}")
            if comps.get("be_quality", 1.0) < 0.75: row.notes.append("thin post-BE room")
            accepted.append((row.ev, row))
            rows.append(row)
        except Exception as e:
            row.reason = f"diagnostic error: {e}"
            rows.append(row)

    accepted.sort(key=lambda x: x[0], reverse=True)
    if accepted:
        selected = accepted[0][1]
        selected.selected = True
        selected.reason = "selected by EV after all institutional gates"
        report.selected = selected
        report.summary = (f"selected ${selected.tp_price:,.1f}; RR={selected.rr:.2f}; "
                          f"EV={selected.ev:.3f}; P={selected.sweep_prob:.2f}")
    else:
        if rows:
            # Surface the most relevant rejection, not just "no TP".
            best_reject = sorted(rows, key=lambda r: (-r.rr, r.distance_atr))[0]
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
) -> PoolSelectionReport:
    """Return an SL protective-pool audit table without relaxing SL rules."""
    report = PoolSelectionReport(role="SL", side=side, entry=float(entry), atr=float(atr))
    if snap is None:
        report.summary = "no liquidity snapshot"
        return report
    if atr <= 0:
        report.summary = "ATR unavailable"
        return report

    rows: List[PoolCandidateDiagnostic] = []
    candidates: List[Tuple[float, PoolCandidateDiagnostic, Any]] = []

    if side == "long":
        pools = list(_safe(snap, "ssl_pools", []) or [])
        inv_price = invalidation_price if invalidation_price is not None else entry - 0.3 * atr
        def _protects(t):
            px = float(_safe(t.pool, "price", 0.0))
            if px > inv_price - _SL_MIN_BEYOND_INVAL_ATR * atr:
                return False, "not beyond long invalidation"
            if (entry - px) > _SL_SEARCH_WINDOW_ATR * atr:
                return False, f"outside SL search window >{_SL_SEARCH_WINDOW_ATR:.1f}ATR"
            return True, ""
    else:
        pools = list(_safe(snap, "bsl_pools", []) or [])
        inv_price = invalidation_price if invalidation_price is not None else entry + 0.3 * atr
        def _protects(t):
            px = float(_safe(t.pool, "price", 0.0))
            if px < inv_price + _SL_MIN_BEYOND_INVAL_ATR * atr:
                return False, "not beyond short invalidation"
            if (px - entry) > _SL_SEARCH_WINDOW_ATR * atr:
                return False, f"outside SL search window >{_SL_SEARCH_WINDOW_ATR:.1f}ATR"
            return True, ""

    if not pools:
        report.summary = f"no protective {'SSL' if side == 'long' else 'BSL'} pools"
        return report

    for target in pools:
        row = _candidate_base("SL", side, target, entry, atr)
        try:
            status = row.status.upper()
            if status in ("SWEPT", "CONSUMED"):
                row.reason = f"archived {status.lower()} pool; not protective"
                rows.append(row); continue
            ok, why = _protects(target)
            if not ok:
                row.reason = why
                rows.append(row); continue
            if row.significance < _SL_MIN_SIGNIFICANCE:
                row.reason = f"significance {row.significance:.1f} < {_SL_MIN_SIGNIFICANCE:.1f}"
                rows.append(row); continue

            sig = float(_safe(target, "significance", 0.0))
            struct = _structural_bonus(target.pool)
            fresh = _freshness_bonus(target.pool)
            touch = _touch_penalty(target.pool)
            score = sig * struct * fresh * touch
            quality = min(score / 10.0, 1.0)
            buffer_atr = _SL_BUFFER_BASE_ATR + (1.0 - quality) * _SL_BUFFER_QUALITY_SCALE
            buffer_atr = min(buffer_atr, _SL_BUFFER_MAX_ATR, max_buffer_atr)
            pool_price = float(_safe(target.pool, "price", 0.0))
            row.sl_price = (pool_price - buffer_atr * atr) if side == "long" else (pool_price + buffer_atr * atr)
            row.buffer_atr = buffer_atr
            row.quality = quality
            row.ev = score
            row.eligible = True
            row.reason = "eligible protective SL pool"
            row.notes = [f"score={score:.2f}"]
            candidates.append((score, row, target))
            rows.append(row)
        except Exception as e:
            row.reason = f"diagnostic error: {e}"
            rows.append(row)

    candidates.sort(key=lambda x: x[0], reverse=True)
    if candidates:
        selected = candidates[0][1]
        selected.selected = True
        selected.reason = "selected protective SL anchor"
        report.selected = selected
        report.summary = (f"selected ${selected.sl_price:,.1f}; anchor ${selected.pool_price:,.1f}; "
                          f"quality={selected.quality:.2f}; buffer={selected.buffer_atr:.2f}ATR")
    else:
        if rows:
            report.summary = "no protective SL pool; best visible pool rejected: " + rows[0].reason
        else:
            report.summary = "no SL candidates found"
    report.candidates = _sort_report_candidates(rows, limit)
    return report


def select_tp_with_report(
    snap, side: str, entry: float, sl: float, atr: float,
    ict: Any = None, htf: Any = None, min_rr: float = 2.0,
    now: Optional[float] = None, session: str = "",
) -> Tuple[Optional[float], Optional[Any], Optional[PoolScore], PoolSelectionReport]:
    """select_tp() plus a full rejection/selection report."""
    report = diagnose_tp_pools(snap, side, entry, sl, atr, ict, htf, min_rr, now, session)
    scores = score_tp_pools(snap, side, entry, sl, atr, ict, htf, min_rr, now, session)
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
) -> Tuple[Optional[float], Optional[Any], Optional[SLPoolPick], PoolSelectionReport]:
    """select_sl() plus a full protective-pool report."""
    report = diagnose_sl_pool(snap, side, entry, atr, ict, htf, invalidation_price, max_buffer_atr, now)
    pick = score_sl_pool(snap, side, entry, atr, ict, htf, invalidation_price, max_buffer_atr, now)
    if pick is None:
        return None, None, None, report
    return pick.sl_price, pick.target, pick, report


# ════════════════════════════════════════════════════════════════════════════
# CONVENIENCE WRAPPERS — one-call API for entry_engine
# ════════════════════════════════════════════════════════════════════════════

def select_tp(
    snap, side: str, entry: float, sl: float, atr: float,
    ict: Any = None, htf: Any = None, min_rr: float = 2.0,
    now: Optional[float] = None, session: str = "",
) -> Tuple[Optional[float], Optional[Any], Optional[PoolScore]]:
    """
    One-call TP selection. Returns (tp_price, target, score) or (None, None, None).
    """
    scores = score_tp_pools(snap, side, entry, sl, atr, ict, htf, min_rr, now, session)
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
) -> Tuple[Optional[float], Optional[Any], Optional[SLPoolPick]]:
    """
    One-call SL selection. Returns (sl_price, target, pick) or (None, None, None).
    """
    pick = score_sl_pool(snap, side, entry, atr, ict, htf,
                         invalidation_price, max_buffer_atr, now)
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

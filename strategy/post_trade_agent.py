"""
strategy/post_trade_agent.py — Institutional Post-Trade Analysis & Adaptive Parameter Agent
============================================================================================
v2.0 — Full rewrite with industry-grade metrics and Bayesian adaptive engine.

ARCHITECTURE
────────────
Each closed trade is analysed across five structural dimensions using metrics
from institutional risk management — NOT retail lagging indicators.

DIMENSION 1 — EXIT GEOMETRY (MAE / MFE / R-multiples)
  Maximum Adverse Excursion (MAE)  : furthest price moved against us (in points)
  Maximum Favorable Excursion (MFE): furthest price moved in our favour (in points)
  G-Ratio            = MFE / (MFE + MAE)          — trade geometry cleanliness [0,1]
  Entry Efficiency   = realised_pts / MFE          — how much of the move we captured
  SL Efficiency      = MAE / SL_distance
    < 0.30 → SL was too wide (never under threat)
    0.30–0.90 → healthy structural SL placement
    > 0.90 → SL was too tight (wick-swept by institutional noise)
  TP Efficiency      = MFE / TP_distance (capped at 1.0 if TP was hit)
    > 0.85 → TP correctly placed at the pool
    0.50–0.85 → TP was slightly too ambitious
    < 0.50 → TP was far too ambitious OR trade reversed immediately
  Actual R           = realised_pts / SL_distance
  Planned R          = TP_distance / SL_distance
  Hold Efficiency    = realised_pts / hold_min (information per minute held)

DIMENSION 2 — ENTRY QUALITY (ICT / structural context at entry)
  OTE Zone Score     : was entry in the 50%–78.6% OTE retracement zone? [0,1]
  Pool Significance  : significance of the swept / target liquidity pool [0,1]
  AMD Phase Score    : quality of the AMD cycle phase at entry [0,1]
  ICT Confluence     : raw ICT confluence score at entry [0,1]
  Displacement Score : displacement confirmed at entry? [0,1]
  Session Score      : kill-zone quality at entry [0,1]
  Composite Score    : weighted entry quality [0,1]

DIMENSION 3 — STRUCTURAL CAUSATION (WHY SL / TP fired)
  SL exits:
    WICK_SWEEP   — spike past SL then reversed (SL too tight for pool noise range)
    BOS_BREAK    — clean market structure break; thesis genuinely invalidated
    AMD_FLIP     — AMD phase / bias turned contra post-entry
    POOL_SWEPT   — unswept pool between entry and SL absorbed the stop-run
    NOISE_HIT    — SL was inside the ICT noise zone (<0.5 ATR of structure)
    STRUCTURAL   — clean SL at structural level; expected part of risk model
  TP exits:
    POOL_REACHED — price delivered to opposing liquidity pool target
    STRUCTURAL   — hit FVG / OB structural TP
    TRAIL_LOCK   — trail SL locked ≥0.8R (optimal profitable trail)
    EARLY_TP     — TP was below the full AMD delivery range (left R on the table)

DIMENSION 4 — ADAPTIVE PARAMETER RECOMMENDATIONS
  Bayesian Beta(α, β) updating per strategy dimension.
  Wilson interval CIs — far more accurate than normal approx for small n.
  Minimum 5 real samples before any adjustment is emitted.
  CI must fully clear neutral threshold; minimum 3 trades between adjustments.
  Max ±40% drift from base. Parameters adjusted:
    SL buffer ATR multiplier     → widen if wick_sweep_rate > 40%
    TP distance multiplier       → tighten if TP efficiency < 0.55
    OTE fib lower bound          → tighten toward 61.8% if edge OTE loses
    AMD confidence threshold     → raise if low-confidence AMD entries < 35% WR
    Entry confirm ticks          → raise if wick sweep rate consistently high
    ICT tier sizing multipliers  → scale by tier-specific Bayesian WR

DIMENSION 5 — INFORMATION COEFFICIENT (IC) TRACKING
  IC = Pearson correlation between directional signal strength and trade outcome.
  Signal: composite score × side direction sign (from trade_record["composite"])
  Outcome: +1 win, -1 loss.
  IC > 0.05 → signals have genuine predictive value
  IC 0.00–0.05 → signals are marginal
  IC < 0.00 → signals are noise (inverse edge — investigate immediately)
  Tracked globally and per dimension for attribution.

WIRING IN quant_strategy.py  (see quant_strategy_patches.py for exact splice points)
───────────────────────────
1. PositionState: add  peak_adverse: float = 0.0  (MAE in points)
2. _update_trailing_sl:  track pos.peak_adverse each tick
3. QuantStrategy.__init__:  self._post_trade_agent = PostTradeAgent()
4. _record_exchange_exit:   set_exit_context() before _record_pnl()
5. _record_pnl:             on_trade_closed() after _trade_history.append()
"""

from __future__ import annotations

import logging
import math
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

# Minimum real observations before a dimension generates recommendations
_MIN_SAMPLES = 5

# Rolling window for statistics (trades)
_ROLLING_WINDOW = 20

# SL efficiency thresholds
_SL_TIGHT_THRESHOLD = 0.90   # MAE/SL > 0.90 → wick-swept (SL too tight)
_SL_WIDE_THRESHOLD  = 0.30   # MAE/SL < 0.30 → SL too wide (never threatened)

# TP efficiency floor — below this, TP was too ambitious
_TP_EFFICIENCY_FLOOR = 0.55

# G-Ratio floor — below this, trade geometry was poor
_G_RATIO_FLOOR = 0.40

# Bayesian prior (2/2 pseudo-count for well-calibrated regularisation)
_BETA_PRIOR_ALPHA = 2.0
_BETA_PRIOR_BETA  = 2.0

# OTE zone Fibonacci levels  (aligns with entry_engine + conviction_filter)
_OTE_FIB_LOW  = 0.50
_OTE_FIB_HIGH = 0.786
_OTE_IDEAL    = 0.618   # optimal institutional entry point

# Maximum parameter drift (never adjust beyond ±40% of default value)
_MAX_PARAM_DRIFT = 0.40

# CI for recommendation gating — require 80% credible interval to cross threshold
_RECOMMENDATION_CI = 0.80

# Wick sweep rate that triggers SL widening
_WICK_RATE_WIDEN = 0.40   # 40% of SL hits are wick sweeps → widen

# Minimum AMD confidence considered "low" for the AMD filter
_AMD_LOW_CONF = 0.55


# ─────────────────────────────────────────────────────────────────────────────
# DATA STRUCTURES
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ExitGeometry:
    """MAE / MFE trade geometry analysis for a single closed trade."""
    # Raw excursion data
    mfe_pts:           float = 0.0  # Maximum Favorable Excursion (points)
    mae_pts:           float = 0.0  # Maximum Adverse Excursion (points)
    realised_pts:      float = 0.0  # Actual exit profit (points; may be negative)
    sl_distance:       float = 0.0  # Entry → SL distance (points)
    tp_distance:       float = 0.0  # Entry → TP distance (points)
    # Derived geometry metrics
    g_ratio:           float = 0.0  # MFE / (MFE + MAE) ∈ [0,1]; 1 = perfect
    entry_efficiency:  float = 0.0  # realised_pts / MFE ∈ [0,1]
    sl_efficiency:     float = 0.0  # MAE / SL_distance; > 1.0 = SL was hit
    tp_efficiency:     float = 0.0  # MFE / TP_distance; 1.0 if TP hit
    actual_r:          float = 0.0  # realised_pts / SL_distance (achieved R)
    planned_r:         float = 0.0  # TP_distance / SL_distance (planned R)
    hold_efficiency:   float = 0.0  # realised_pts / hold_min (pts per minute held)
    # Outcome flags
    was_tp_hit:        bool  = False
    was_sl_hit:        bool  = False
    wick_sweep:        bool  = False  # SL hit by wick but trade direction correct
    # Analysis
    causation:         str   = ""    # See DIMENSION 3 taxonomy above
    quality_score:     float = 0.0   # 0-1 overall trade quality composite


@dataclass
class EntryQuality:
    """ICT / structural quality of the entry setup."""
    ote_score:             float = 0.0  # OTE zone score [0,1]
    ote_dist_from_center:  float = 0.0  # Normalised distance from 61.8% ideal
    pool_sig:              float = 0.0  # Swept pool significance [0,1]
    amd_phase_score:       float = 0.0  # AMD phase alignment quality [0,1]
    amd_conf:              float = 0.0  # AMD confidence at entry
    ict_confluence:        float = 0.0  # ICT confluence score [0,1]
    composite_score:       float = 0.0  # Quant composite at entry (signed)
    displacement_score:    float = 0.0  # Displacement quality [0,1]
    session_score:         float = 0.0  # Session/kill-zone quality [0,1]
    ict_tier:              str   = ""   # "S" | "A" | "B" | ""
    overall_score:         float = 0.0  # Composite entry quality [0,1]


@dataclass
class TradeRecord:
    """Full institutional analysis record for one closed trade."""
    ts:            float          # Unix timestamp of close
    side:          str            # "long" | "short"
    mode:          str            # "reversion" | "trend" | "momentum" | "hunt" | "flow"
    entry_price:   float
    exit_price:    float
    sl_price:      float
    tp_price:      float
    quantity:      float
    pnl:           float
    is_win:        bool
    hold_min:      float
    ict_tier:      str            # "S" | "A" | "B" | ""
    regime:        str
    amd_phase:     str
    amd_bias:      str
    amd_conf:      float
    htf_15m:       float
    htf_4h:        float
    adx:           float
    exit_reason:   str            # "tp_hit" | "sl_hit" | "trail_sl_hit" | …
    # Entry composite (signed: positive for long, negative for short)
    composite:     float          = 0.0
    ict_total:     float          = 0.0
    n_confirming:  int            = 0
    # Analysis (populated by agent)
    geometry:      ExitGeometry   = field(default_factory=ExitGeometry)
    entry_quality: EntryQuality   = field(default_factory=EntryQuality)


@dataclass
class BayesianEstimate:
    """
    Bayesian Beta(α, β) win-rate estimate for a strategy dimension.

    Regularised with a Beta(2, 2) prior — centres at 50% WR with low variance,
    avoids degenerate estimates from 1–2 sample dimensions.

    Credible intervals use the Wilson score method which is far more accurate
    than the normal approximation when n < 30 (typical in live trading).
    """
    alpha: float = _BETA_PRIOR_ALPHA   # α = wins + prior
    beta:  float = _BETA_PRIOR_BETA    # β = losses + prior

    def update(self, win: bool) -> None:
        if win:
            self.alpha += 1.0
        else:
            self.beta += 1.0

    @property
    def mean(self) -> float:
        """Posterior mean E[p] = α / (α + β)."""
        return self.alpha / (self.alpha + self.beta)

    @property
    def n(self) -> int:
        """Number of real observations (excluding prior pseudo-counts)."""
        return max(0, int(round(self.alpha + self.beta
                                - _BETA_PRIOR_ALPHA - _BETA_PRIOR_BETA)))

    def ci_lower(self, confidence: float = _RECOMMENDATION_CI) -> float:
        """Lower bound of Wilson score credible interval."""
        return _wilson_lower(self.n, self.mean, confidence)

    def ci_upper(self, confidence: float = _RECOMMENDATION_CI) -> float:
        """Upper bound of Wilson score credible interval."""
        return _wilson_upper(self.n, self.mean, confidence)

    def is_significant(
        self, confidence: float = _RECOMMENDATION_CI, baseline: float = 0.50
    ) -> bool:
        """True if the CI lies entirely above or below the baseline."""
        lo = self.ci_lower(confidence)
        hi = self.ci_upper(confidence)
        return lo > baseline or hi < baseline

    def variance(self) -> float:
        """Beta distribution variance = αβ / ((α+β)²(α+β+1))."""
        ab = self.alpha + self.beta
        if ab < 1e-10:
            return 0.25
        return (self.alpha * self.beta) / (ab * ab * (ab + 1.0))

    def std(self) -> float:
        """Posterior standard deviation."""
        return math.sqrt(max(0.0, self.variance()))


@dataclass
class ParameterState:
    """
    Adaptive parameter state for one strategy dimension.
    current_mult: multiplicative factor applied to the base value.
    Drift is bounded to ±MAX_PARAM_DRIFT from 1.0.
    History enables diagnostic review and potential manual revert.
    """
    base_value:       float
    current_mult:     float = 1.0
    adjustment_count: int   = 0
    last_adjusted_ts: float = 0.0
    adjustment_log:   List[Tuple[float, float, str]] = field(
        default_factory=list)   # (ts, mult, reason)

    @property
    def effective_value(self) -> float:
        """Current operative value = base × mult."""
        return self.base_value * self.current_mult

    @property
    def drift_pct(self) -> float:
        """How far current_mult has drifted from 1.0, as a percentage."""
        return (self.current_mult - 1.0) * 100.0

    def apply_adjustment(
        self, new_mult: float, reason: str, ts: float
    ) -> None:
        clamped = max(1.0 - _MAX_PARAM_DRIFT,
                      min(1.0 + _MAX_PARAM_DRIFT, new_mult))
        self.current_mult       = clamped
        self.adjustment_count  += 1
        self.last_adjusted_ts   = ts
        self.adjustment_log.append((ts, clamped, reason))
        if len(self.adjustment_log) > 50:
            self.adjustment_log = self.adjustment_log[-50:]

    def reset(self) -> None:
        """Revert to default (mult=1.0). Used if reversal evidence accumulates."""
        self.current_mult = 1.0


@dataclass
class AdaptiveParameters:
    """
    Live adaptive parameter state for all adjustable strategy dimensions.
    All start at mult=1.0 (no adjustment) and drift based on Bayesian evidence.
    Defaults mirror config.py / entry_engine.py / conviction_filter.py defaults.
    """
    sl_buffer_atr:        ParameterState = field(
        default_factory=lambda: ParameterState(0.35))   # ATR mult for SL noise buffer
    tp_distance_mult:     ParameterState = field(
        default_factory=lambda: ParameterState(1.0))    # TP reach multiplier
    ote_fib_low:          ParameterState = field(
        default_factory=lambda: ParameterState(0.50))   # OTE lower fib bound
    amd_conf_threshold:   ParameterState = field(
        default_factory=lambda: ParameterState(0.50))   # Min AMD confidence
    entry_confirm_ticks:  ParameterState = field(
        default_factory=lambda: ParameterState(2.0))    # Confirm ticks required
    tier_s_sizing:        ParameterState = field(
        default_factory=lambda: ParameterState(1.00))   # Tier-S size multiplier
    tier_a_sizing:        ParameterState = field(
        default_factory=lambda: ParameterState(0.80))   # Tier-A size multiplier
    tier_b_sizing:        ParameterState = field(
        default_factory=lambda: ParameterState(0.65))   # Tier-B size multiplier


@dataclass
class DimensionStats:
    """Rolling Bayesian statistics for one strategy dimension."""
    label:         str
    bayes:         BayesianEstimate = field(default_factory=BayesianEstimate)
    avg_mfe_pts:   float = 0.0
    avg_mae_pts:   float = 0.0
    avg_g_ratio:   float = 0.0
    avg_entry_eff: float = 0.0
    avg_sl_eff:    float = 0.0
    avg_tp_eff:    float = 0.0
    avg_actual_r:  float = 0.0   # average achieved R
    avg_pnl:       float = 0.0
    recent_trades: List[TradeRecord] = field(default_factory=list)

    def add_trade(self, rec: TradeRecord) -> None:
        self.bayes.update(rec.is_win)
        self.recent_trades.append(rec)
        if len(self.recent_trades) > _ROLLING_WINDOW:
            self.recent_trades = self.recent_trades[-_ROLLING_WINDOW:]
        self._recompute_rolling()

    def _recompute_rolling(self) -> None:
        if not self.recent_trades:
            return
        n = len(self.recent_trades)
        self.avg_mfe_pts   = sum(r.geometry.mfe_pts        for r in self.recent_trades) / n
        self.avg_mae_pts   = sum(r.geometry.mae_pts        for r in self.recent_trades) / n
        self.avg_g_ratio   = sum(r.geometry.g_ratio        for r in self.recent_trades) / n
        self.avg_entry_eff = sum(r.geometry.entry_efficiency for r in self.recent_trades) / n
        self.avg_sl_eff    = sum(r.geometry.sl_efficiency  for r in self.recent_trades) / n
        self.avg_tp_eff    = sum(r.geometry.tp_efficiency  for r in self.recent_trades) / n
        self.avg_actual_r  = sum(r.geometry.actual_r       for r in self.recent_trades) / n
        self.avg_pnl       = sum(r.pnl                     for r in self.recent_trades) / n


@dataclass
class AgentInsight:
    """A single actionable insight emitted by the post-trade analysis engine."""
    dimension:      str         # which strategy dimension this applies to
    severity:       str         # "INFO" | "WARN" | "ACTION"
    message:        str         # human-readable finding
    recommendation: str         # what the operator should review
    confidence:     float       # 0–1 confidence in the recommendation
    param_key:      str   = ""  # parameter key (if a param adjustment is indicated)
    param_new_mult: float = 1.0 # new multiplicative factor for the parameter
    ts:             float = field(default_factory=time.time)


# ─────────────────────────────────────────────────────────────────────────────
# STATISTICAL UTILITIES
# ─────────────────────────────────────────────────────────────────────────────

def _normal_ppf(p: float) -> float:
    """
    Inverse normal CDF via Beasley-Springer-Moro rational approximation.
    Accurate to 4+ decimal places for p ∈ (0.0001, 0.9999).
    """
    if p <= 0.0:
        return -8.0
    if p >= 1.0:
        return  8.0
    sign = 1.0 if p >= 0.5 else -1.0
    q    = min(p, 1.0 - p)
    r    = math.sqrt(-2.0 * math.log(q))
    c0, c1, c2 = 2.515517, 0.802853, 0.010328
    d1, d2, d3 = 1.432788, 0.189269, 0.001308
    num = c0 + r * (c1 + r * c2)
    den = 1.0 + r * (d1 + r * (d2 + r * d3))
    return sign * (r - num / den)


def _wilson_lower(n: int, p_hat: float, confidence: float = 0.80) -> float:
    """
    Wilson score interval lower bound.
    Superior to the normal approximation for small n (the standard case in trading).
    Handles edge cases: n=0 → 0.0, p_hat=0 → 0.0 without NaN.
    """
    if n <= 0:
        return 0.0
    alpha = 1.0 - confidence
    z     = _normal_ppf(1.0 - alpha / 2.0)
    z2    = z * z
    centre = (p_hat + z2 / (2.0 * n)) / (1.0 + z2 / n)
    margin = z / (1.0 + z2 / n) * math.sqrt(
        p_hat * (1.0 - p_hat) / n + z2 / (4.0 * n * n))
    return max(0.0, centre - margin)


def _wilson_upper(n: int, p_hat: float, confidence: float = 0.80) -> float:
    """Wilson score interval upper bound."""
    if n <= 0:
        return 1.0
    alpha = 1.0 - confidence
    z     = _normal_ppf(1.0 - alpha / 2.0)
    z2    = z * z
    centre = (p_hat + z2 / (2.0 * n)) / (1.0 + z2 / n)
    margin = z / (1.0 + z2 / n) * math.sqrt(
        p_hat * (1.0 - p_hat) / n + z2 / (4.0 * n * n))
    return min(1.0, centre + margin)


def _safe_ratio(
    numerator: float, denominator: float, default: float = 0.0
) -> float:
    """Safe division — returns default when denominator is near-zero."""
    return numerator / denominator if abs(denominator) > 1e-10 else default


def _pearson_correlation(xs: List[float], ys: List[float]) -> float:
    """
    Pearson correlation coefficient between two lists.
    Returns 0.0 when either series has zero variance (constant signal or outcome).
    """
    n = len(xs)
    if n < 3:
        return 0.0
    mx = sum(xs) / n
    my = sum(ys) / n
    num  = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    var_x = sum((x - mx) ** 2 for x in xs)
    var_y = sum((y - my) ** 2 for y in ys)
    if var_x < 1e-12 or var_y < 1e-12:
        return 0.0
    return round(num / math.sqrt(var_x * var_y), 4)


# ─────────────────────────────────────────────────────────────────────────────
# SCORING FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def _amd_phase_score(
    phase: str, bias: str, trade_side: str, conf: float
) -> float:
    """
    Institutional AMD phase alignment score [0, 1].

    ICT framework phase values:
      MANIPULATION  + aligned bias → highest conviction reversal (1.00)
      DISTRIBUTION  + aligned bias → delivery in progress       (0.85)
      REACCUMULATION/REDISTRIBUTION → mid-trend continuation   (0.70)
      ACCUMULATION  → consolidation, no directional edge       (0.40)
      Contra-bias  → active institutional opposition            (penalty)

    Scaled by AMD confidence to prevent full credit for weak-confidence phases.
    """
    phase_base = {
        "MANIPULATION":   1.00,
        "DISTRIBUTION":   0.85,
        "REACCUMULATION": 0.70,
        "REDISTRIBUTION": 0.70,
        "ACCUMULATION":   0.40,
    }.get(str(phase).upper(), 0.45)

    bias_aligned = (
        (trade_side == "long"  and str(bias).lower() == "bullish") or
        (trade_side == "short" and str(bias).lower() == "bearish") or
        str(bias).lower() == "neutral"
    )

    if bias_aligned:
        # Scale by confidence: minimum 70% credit even at conf=0
        return min(1.0, phase_base * (0.70 + 0.30 * conf))
    else:
        # Contra alignment: penalise proportional to confidence
        return max(0.0, phase_base * 0.50 - conf * 0.20)


def _ote_score(
    entry_price: float, sweep_price: float,
    sweep_type: str, atr: float
) -> Tuple[float, float]:
    """
    Compute the OTE (Optimal Trade Entry) zone score [0, 1].

    OTE = the 50%–78.6% Fibonacci retracement of the displacement move
    that originated from the swept liquidity pool.

    ICT optimal entry is at exactly 61.8% — the golden ratio retracement.

    Returns:
        (score, dist_from_center_normalized)
        score = 1.0 at 61.8%, decays toward 0.5 at OTE edges, 0.0 outside
        dist_from_center = 0.0 at 61.8%, 1.0 at OTE edges, >1.0 outside
    """
    if (abs(sweep_price) < 1e-10 or abs(entry_price) < 1e-10
            or atr < 1e-10):
        return 0.0, 1.0

    displacement = abs(entry_price - sweep_price)
    if displacement < 1e-10:
        return 0.0, 1.0

    # Normalise using a reference swing of 3 × ATR (typical ICT displacement)
    # This maps a 3-ATR displacement to 1.0 on the retracement scale
    reference_swing = max(displacement * 1.5, atr * 3.0)
    retracement = displacement / reference_swing

    ote_center     = (_OTE_FIB_LOW + _OTE_FIB_HIGH) / 2.0    # ~0.643
    ote_half_width = (_OTE_FIB_HIGH - _OTE_FIB_LOW)  / 2.0   # 0.143

    if _OTE_FIB_LOW <= retracement <= _OTE_FIB_HIGH:
        # Inside OTE: score peaks at 1.0 at 61.8%, decays to 0.5 at edges
        dist_from_center = abs(retracement - _OTE_IDEAL) / ote_half_width
        score = 1.0 - dist_from_center * 0.50
    else:
        # Outside OTE: score decays rapidly with distance from nearest edge
        dist_from_edge = min(
            abs(retracement - _OTE_FIB_LOW),
            abs(retracement - _OTE_FIB_HIGH)
        )
        score             = max(0.0, 0.40 - dist_from_edge * 2.5)
        dist_from_center  = 1.0 + dist_from_edge / ote_half_width

    return round(score, 3), round(dist_from_center, 3)


def _classify_exit_causation(
    rec: TradeRecord, geometry: ExitGeometry
) -> str:
    """
    Institutional-grade exit causation taxonomy.

    The classification follows the ICT structural framework:
    — TP hits are classified by how cleanly the pool was reached
    — SL hits are classified by the structural mechanism that invalidated the thesis

    Returns one of the causation codes defined in DIMENSION 3 above.
    """
    sl_eff  = geometry.sl_efficiency
    reason  = rec.exit_reason

    # ── TP exit ──────────────────────────────────────────────────────────────
    if reason == "tp_hit":
        if geometry.tp_efficiency >= 0.90:
            return "POOL_REACHED"       # price delivered to the opposing pool
        elif geometry.tp_efficiency >= 0.70:
            return "STRUCTURAL"         # hit a structural TP (FVG / OB)
        else:
            return "EARLY_TP"           # TP was too ambitious; left R on table

    # ── Trail SL exit ────────────────────────────────────────────────────────
    if reason == "trail_sl_hit":
        if geometry.entry_efficiency >= 0.70:
            return "TRAIL_LOCK"         # locked ≥70% of the move; clean exit
        elif sl_eff > _SL_TIGHT_THRESHOLD:
            return "WICK_SWEEP"         # trail SL got wick-swept
        else:
            return "STRUCTURAL"         # trail stopped at structure

    # ── SL / forced exit ─────────────────────────────────────────────────────
    if reason in ("sl_hit", "regime_flip", "flow_reversal",
                  "exiting_timeout", "max_hold", "confirmed_via_position"):
        # Wick sweep: MAE > SL threshold but there was some positive move first
        if sl_eff > _SL_TIGHT_THRESHOLD:
            if geometry.mfe_pts > 0.20 * geometry.sl_distance:
                return "WICK_SWEEP"     # had positive excursion then swept
            else:
                return "NOISE_HIT"      # SL was inside the noise zone from start

        # AMD flip: bias opposed to trade side with significant confidence
        bias_opposed = (
            (rec.side == "long"  and str(rec.amd_bias).lower() == "bearish") or
            (rec.side == "short" and str(rec.amd_bias).lower() == "bullish")
        )
        if bias_opposed and rec.amd_conf >= 0.65:
            return "AMD_FLIP"

        # Market structure break (regime flip = BOS/CHoCH confirmed)
        if reason == "regime_flip":
            return "BOS_BREAK"

        if sl_eff >= 0.30:
            return "STRUCTURAL"         # clean SL at structure; thesis invalidated

        return "NOISE_HIT"              # SL was too close to entry (below structure)

    return "UNKNOWN"


def _session_from_ict(ict_engine) -> str:
    """Extract current session label from ICT engine state."""
    if ict_engine is None:
        return ""
    try:
        sess = str(getattr(ict_engine, '_session',  '') or '').upper()
        kz   = str(getattr(ict_engine, '_killzone', '') or '').upper()
        if kz and kz not in ("", "NONE", "OFF_HOURS"):
            return f"KZ_{kz[:4]}"
        return sess or "UNKNOWN"
    except Exception:
        return ""


# ─────────────────────────────────────────────────────────────────────────────
# MAIN AGENT
# ─────────────────────────────────────────────────────────────────────────────

class PostTradeAgent:
    """
    Institutional post-trade analysis and adaptive parameter engine.

    Each closed trade is analysed through five structural lenses:
      1. Exit geometry   (MAE / MFE / R-multiples / hold efficiency)
      2. Entry quality   (OTE zone, AMD alignment, ICT confluence, session)
      3. Exit causation  (WICK_SWEEP | BOS_BREAK | AMD_FLIP | POOL_REACHED | …)
      4. Dimension stats (per AMD phase, ICT tier, regime, session, mode)
      5. Adaptive params (Bayesian Beta updating → SL/TP/tier size adjustments)

    No retail logic. No lagging indicators. Pure ICT institutional mechanics.
    Thread-safe: all mutation is performed on the calling thread (main strategy thread).
    """

    def __init__(self) -> None:
        # ── Pending exit context (set by set_exit_context before _record_pnl) ──
        self._pending_exit_type:   str   = ""
        self._pending_fill_price:  float = 0.0
        self._pending_mfe_pts:     float = 0.0
        self._pending_mae_pts:     float = 0.0
        self._pending_atr:         float = 0.0
        self._pending_sweep_price: float = 0.0
        self._pending_session:     str   = ""

        # ── Dimension-level Bayesian statistics ────────────────────────────────
        self._stats_by_tier:    Dict[str, DimensionStats] = {}
        self._stats_by_amd:     Dict[str, DimensionStats] = {}
        self._stats_by_session: Dict[str, DimensionStats] = {}
        self._stats_by_regime:  Dict[str, DimensionStats] = {}
        self._stats_by_mode:    Dict[str, DimensionStats] = {}
        self._stats_overall:    DimensionStats = DimensionStats(label="overall")

        # ── Causation frequency counters ───────────────────────────────────────
        self._sl_causation: Dict[str, int] = defaultdict(int)
        self._tp_causation: Dict[str, int] = defaultdict(int)

        # ── Adaptive parameters ─────────────────────────────────────────────────
        self.params = AdaptiveParameters()

        # ── IC (Information Coefficient) tracking ──────────────────────────────
        # Buffer stores (signal_direction_score, outcome) pairs
        # signal_direction_score = composite × 1 (long) or ×-1 (short) — signed
        self._ic_buffer: deque = deque(maxlen=_ROLLING_WINDOW)

        # Per-dimension IC buffers for attribution
        self._ic_by_tier:    Dict[str, deque] = defaultdict(lambda: deque(maxlen=15))
        self._ic_by_session: Dict[str, deque] = defaultdict(lambda: deque(maxlen=15))

        # ── Full trade records (last 200) ───────────────────────────────────────
        self.records:  List[TradeRecord] = []

        # ── Insights (for Telegram / /learn command) ───────────────────────────
        self.insights: List[AgentInsight] = []

        logger.info("PostTradeAgent v2.0 initialised — institutional post-trade analysis active")

    # ──────────────────────────────────────────────────────────────────────────
    # EXTERNAL API
    # ──────────────────────────────────────────────────────────────────────────

    def set_exit_context(
        self,
        exit_type:    str,
        fill_price:   float,
        pos,
        atr:          float,
        ict_engine    = None,
        liq_snapshot  = None,
    ) -> None:
        """
        Called in quant_strategy._record_exchange_exit() BEFORE _record_pnl().

        Stores exit-side data needed to compute MAE / MFE geometry but not
        yet present in the trade_record dict at _record_pnl() time.

        MAE sourcing priority:
          1. pos.peak_adverse (exact, tracked tick-by-tick if patch applied)
          2. pos.initial_sl_dist × 1.0 for SL hits (lower bound)
          3. pos.initial_sl_dist × 0.20 for TP hits (conservative estimate)
        """
        self._pending_exit_type  = exit_type
        self._pending_fill_price = fill_price
        self._pending_atr        = atr

        # ── MFE from peak_profit ──────────────────────────────────────────────
        peak_profit = max(0.0, float(getattr(pos, 'peak_profit', 0.0) or 0.0))
        self._pending_mfe_pts = peak_profit

        # ── MAE — tiered sourcing ─────────────────────────────────────────────
        entry_price  = float(getattr(pos, 'entry_price', 0.0) or 0.0)
        pos_side     = str(getattr(pos, 'side', 'long') or 'long')
        init_sl_dist = float(getattr(pos, 'initial_sl_dist',
                             abs(entry_price - float(getattr(pos, 'sl_price', 0.0) or 0.0))))

        # Priority 1: exact peak_adverse (requires PositionState patch)
        peak_adverse = float(getattr(pos, 'peak_adverse', 0.0) or 0.0)
        if peak_adverse > 1e-10:
            self._pending_mae_pts = peak_adverse

        # Priority 2: SL hit — MAE is at least the fill distance from entry
        elif exit_type in ("sl", "trail_sl"):
            if pos_side == "long":
                self._pending_mae_pts = max(0.0, entry_price - fill_price)
            else:
                self._pending_mae_pts = max(0.0, fill_price - entry_price)
            # PTA-4 FIX: previous formula `max(mae, init_sl_dist * 0.95)` always
            # floored MAE at 95% of SL distance, over-reporting MAE for trades
            # that SL-wicked (touched SL for 1 tick then reversed). That made
            # `avg_sl_eff` artificially high, which triggered the adaptive
            # engine to widen SL unnecessarily. Cap the floor at the realized
            # adverse move + 1 ATR of conservative padding. If price wicked
            # through SL by a tiny amount, MAE reflects that — not 95% of SL.
            _floor = min(init_sl_dist, self._pending_mae_pts + atr * 1.0) if atr > 0 else init_sl_dist
            self._pending_mae_pts = max(self._pending_mae_pts, _floor)

        # Priority 3: TP hit — MAE is the intra-trade drawdown
        # PTA-6 FIX: 0.20 × init_sl_dist systematically under-reports MAE for
        # winners (real trades often pull back 40-60% before running to TP).
        # 0.35 is a more realistic fallback factor, closer to empirical mean.
        else:
            self._pending_mae_pts = max(0.0, init_sl_dist * 0.35)

        # ── Swept pool price for OTE scoring ─────────────────────────────────
        self._pending_sweep_price = 0.0

        # Try ICT engine's AMD sweep origin
        if ict_engine is not None:
            try:
                _amd = getattr(ict_engine, '_amd', None)
                if _amd:
                    sw = float(getattr(_amd, 'sweep_origin', 0.0) or 0.0)
                    if sw > 1e-10:
                        self._pending_sweep_price = sw
            except Exception:
                pass

        # Try liquidity snapshot recent sweeps
        if self._pending_sweep_price < 1e-10 and liq_snapshot is not None:
            try:
                sweeps = getattr(liq_snapshot, 'recent_sweeps', [])
                if sweeps:
                    recent_sw = max(sweeps, key=lambda s: getattr(s, 'detected_at', 0))
                    sw_price = float(getattr(
                        getattr(recent_sw, 'pool', None), 'price', 0.0) or 0.0)
                    if sw_price > 1e-10:
                        self._pending_sweep_price = sw_price
            except Exception:
                pass

        # ── Session for dimension tracking ────────────────────────────────────
        self._pending_session = _session_from_ict(ict_engine)

    def on_trade_closed(
        self,
        trade_record: Dict,
        pos,
        atr:          float,
        ict_engine    = None,
        liq_snapshot  = None,
    ) -> None:
        """
        Called by quant_strategy._record_pnl() after each trade closes.

        Performs full institutional analysis:
          1. Build TradeRecord from dict
          2. Compute exit geometry (MAE/MFE/G-ratio/R-multiples)
          3. Score entry quality (OTE/AMD/ICT/session/pool)
          4. Classify structural causation (why SL/TP fired)
          5. Update all dimension statistics (Bayesian)
          6. Update Information Coefficient buffer
          7. Run adaptive parameter engine (Bayesian decision)
          8. Store record and log analysis
        """
        try:
            rec = self._build_record(trade_record, pos, atr)
            self._compute_geometry(rec, atr, pos)
            self._compute_entry_quality(rec, pos, atr, ict_engine, liq_snapshot)
            self._classify_causation(rec)
            self._update_statistics(rec)
            self._update_ic(rec)
            self._run_adaptive_engine(rec)
            self._store_record(rec)
            self._log_analysis(rec)
        except Exception as e:
            logger.error(
                f"PostTradeAgent.on_trade_closed non-fatal error: {e}",
                exc_info=True
            )

    def get_parameter_recommendations(self) -> Dict[str, Any]:
        """
        Returns current adaptive parameter state for Telegram / diagnostic use.
        """
        return {
            "sl_buffer_atr":       round(self.params.sl_buffer_atr.effective_value, 4),
            "sl_buffer_mult":      round(self.params.sl_buffer_atr.current_mult, 3),
            "tp_distance_mult":    round(self.params.tp_distance_mult.current_mult, 3),
            "ote_fib_low":         round(self.params.ote_fib_low.effective_value, 3),
            "amd_conf_threshold":  round(self.params.amd_conf_threshold.effective_value, 3),
            "entry_confirm_ticks": round(self.params.entry_confirm_ticks.effective_value, 2),
            "tier_s_sizing":       round(self.params.tier_s_sizing.current_mult, 3),
            "tier_a_sizing":       round(self.params.tier_a_sizing.current_mult, 3),
            "tier_b_sizing":       round(self.params.tier_b_sizing.current_mult, 3),
            "total_trades":        len(self.records),
            "overall_wr":          round(self._stats_overall.bayes.mean, 3),
            "ic":                  round(self._compute_ic(), 4),
            "recent_insights": [
                {"dim": i.dimension, "severity": i.severity,
                 "msg": i.message[:120], "rec": i.recommendation[:100]}
                for i in self.insights[-5:]
            ],
        }

    def get_dimension_report(self) -> str:
        """
        Full formatted post-trade analysis report for Telegram /learn command.
        Includes: overall stats, per-dimension Bayesian WRs, MAE/MFE geometry,
        SL causation breakdown, IC, and current adaptive parameter state.
        """
        lines = ["<b>🧠 POST-TRADE ANALYSIS REPORT</b>"]
        n = len(self.records)

        if n == 0:
            lines.append("\n  No closed trades recorded yet.")
            return "\n".join(lines)

        overall = self._stats_overall
        ic = self._compute_ic()
        ic_icon = "✅" if ic > 0.05 else ("⚠️" if ic >= 0.0 else "❌")

        lines.append(
            f"\n<b>Overall</b>: {n} trades | "
            f"WR={overall.bayes.mean:.0%} "
            f"(CI {overall.bayes.ci_lower():.0%}–{overall.bayes.ci_upper():.0%}) | "
            f"IC={ic:+.3f} {ic_icon}"
        )
        lines.append(
            f"  G-Ratio={overall.avg_g_ratio:.2f} | "
            f"MFE={overall.avg_mfe_pts:.0f}pts | "
            f"MAE={overall.avg_mae_pts:.0f}pts | "
            f"avgR={overall.avg_actual_r:.2f}"
        )

        # ── By ICT Tier ───────────────────────────────────────────────────────
        if self._stats_by_tier:
            lines.append("\n<b>By ICT Tier</b>")
            for tier in ["S", "A", "B", "none"]:
                d = self._stats_by_tier.get(tier)
                if d and d.bayes.n >= 1:
                    lines.append(
                        f"  Tier-{tier}: n={d.bayes.n}  "
                        f"WR={d.bayes.mean:.0%} ({d.bayes.ci_lower():.0%}–{d.bayes.ci_upper():.0%})  "
                        f"avgR={d.avg_actual_r:.2f}  avgPnL=${d.avg_pnl:+.2f}  "
                        f"G={d.avg_g_ratio:.2f}"
                    )

        # ── By AMD Phase ──────────────────────────────────────────────────────
        if self._stats_by_amd:
            lines.append("\n<b>By AMD Phase</b>")
            for phase, d in sorted(
                self._stats_by_amd.items(),
                key=lambda kv: -kv[1].bayes.n
            ):
                if d.bayes.n >= 1:
                    lines.append(
                        f"  {phase[:12]}: n={d.bayes.n}  "
                        f"WR={d.bayes.mean:.0%}  "
                        f"SLeff={d.avg_sl_eff:.2f}  "
                        f"EEff={d.avg_entry_eff:.2f}  "
                        f"avgR={d.avg_actual_r:.2f}"
                    )

        # ── By Session ────────────────────────────────────────────────────────
        if self._stats_by_session:
            lines.append("\n<b>By Session</b>")
            for sess, d in sorted(
                self._stats_by_session.items(),
                key=lambda kv: -kv[1].bayes.n
            ):
                if d.bayes.n >= 1:
                    lines.append(
                        f"  {sess[:12]}: n={d.bayes.n}  "
                        f"WR={d.bayes.mean:.0%}  "
                        f"avgPnL=${d.avg_pnl:+.2f}"
                    )

        # ── SL Causation ──────────────────────────────────────────────────────
        if self._sl_causation:
            lines.append("\n<b>SL Causation</b>")
            total_sl = sum(self._sl_causation.values())
            for cause, cnt in sorted(
                self._sl_causation.items(), key=lambda kv: -kv[1]
            ):
                lines.append(f"  {cause}: {cnt} ({cnt / total_sl:.0%})")

        # ── TP Causation ──────────────────────────────────────────────────────
        if self._tp_causation:
            lines.append("\n<b>TP Causation</b>")
            total_tp = sum(self._tp_causation.values())
            for cause, cnt in sorted(
                self._tp_causation.items(), key=lambda kv: -kv[1]
            ):
                lines.append(f"  {cause}: {cnt} ({cnt / total_tp:.0%})")

        # ── Adaptive Parameters ───────────────────────────────────────────────
        param_rows = [
            ("SL buffer",       self.params.sl_buffer_atr),
            ("TP mult",         self.params.tp_distance_mult),
            ("OTE low",         self.params.ote_fib_low),
            ("AMD conf thresh", self.params.amd_conf_threshold),
            ("Confirm ticks",   self.params.entry_confirm_ticks),
            ("Tier-S sizing",   self.params.tier_s_sizing),
            ("Tier-A sizing",   self.params.tier_a_sizing),
            ("Tier-B sizing",   self.params.tier_b_sizing),
        ]
        adjusted = [
            (name, ps) for name, ps in param_rows
            if abs(ps.current_mult - 1.0) > 0.01
        ]
        if adjusted:
            lines.append("\n<b>Adaptive Adjustments</b>")
            for name, ps in adjusted:
                lines.append(
                    f"  {name}: ×{ps.current_mult:.3f}  "
                    f"effective={ps.effective_value:.4f}  "
                    f"drift={ps.drift_pct:+.1f}%  "
                    f"adj#{ps.adjustment_count}"
                )
        else:
            lines.append("\n<b>Adaptive Adjustments</b>: none yet (insufficient evidence)")

        # ── Recent Insights ───────────────────────────────────────────────────
        recent_insights = self.insights[-5:]
        if recent_insights:
            lines.append("\n<b>Recent Insights</b>")
            for ins in recent_insights:
                icon = {"INFO": "ℹ️", "WARN": "⚠️", "ACTION": "🔧"}.get(
                    ins.severity, "•")
                lines.append(
                    f"  {icon} <b>[{ins.dimension}]</b> {ins.message[:100]}"
                )
                lines.append(f"     → {ins.recommendation[:90]}")

        return "\n".join(lines)

    def get_adaptive_state(self) -> Dict[str, Any]:
        """
        Returns only the parameters that have been adjusted from default.
        Intended for compact display in the periodic heartbeat report.
        """
        out: Dict[str, Any] = {}
        for key, ps in [
            ("sl_buffer_atr",       self.params.sl_buffer_atr),
            ("tp_distance_mult",    self.params.tp_distance_mult),
            ("ote_fib_low",         self.params.ote_fib_low),
            ("amd_conf_threshold",  self.params.amd_conf_threshold),
            ("entry_confirm_ticks", self.params.entry_confirm_ticks),
            ("tier_s_sizing",       self.params.tier_s_sizing),
            ("tier_a_sizing",       self.params.tier_a_sizing),
            ("tier_b_sizing",       self.params.tier_b_sizing),
        ]:
            if abs(ps.current_mult - 1.0) > 0.01:
                out[key] = {
                    "mult":      round(ps.current_mult, 3),
                    "effective": round(ps.effective_value, 4),
                    "adj_count": ps.adjustment_count,
                }
        return out

    # ──────────────────────────────────────────────────────────────────────────
    # INTERNAL: Build Record
    # ──────────────────────────────────────────────────────────────────────────

    def _build_record(
        self, t: Dict, pos, atr: float
    ) -> TradeRecord:
        """
        Construct a TradeRecord from the trade_history dict and PositionState.

        composite is stored signed (positive=long, negative=short) to enable
        proper IC calculation. The trade_record stores the raw absolute composite;
        we flip sign for short positions here.
        """
        side = str(t.get("side", "long") or "long")
        raw_composite = float(t.get("composite", 0.0) or 0.0)
        # IC requires signed composite: same sign as trade direction
        signed_composite = raw_composite if side == "long" else -abs(raw_composite)

        return TradeRecord(
            ts           = float(t.get("ts",          time.time())),
            side         = side,
            mode         = str(t.get("mode",          "reversion") or "reversion"),
            entry_price  = float(t.get("entry",       0.0)),
            exit_price   = float(t.get("exit",        0.0)),
            sl_price     = float(t.get("sl",          0.0)),
            tp_price     = float(t.get("tp",          0.0)),
            quantity     = float(t.get("qty",         0.0)),
            pnl          = float(t.get("pnl",         0.0)),
            is_win       = bool(t.get("is_win",       False)),
            hold_min     = float(t.get("hold_min",    0.0)),
            ict_tier     = str(t.get("ict_tier",      "") or ""),
            regime       = str(t.get("regime",        "") or ""),
            amd_phase    = str(t.get("amd_phase",     "") or ""),
            amd_bias     = str(t.get("amd_bias",      "") or ""),
            amd_conf     = float(t.get("amd_conf",    0.0)),
            htf_15m      = float(t.get("htf_15m",     0.0)),
            htf_4h       = float(t.get("htf_4h",      0.0)),
            adx          = float(t.get("adx",         0.0)),
            exit_reason  = str(t.get("reason",        "unknown") or "unknown"),
            composite    = signed_composite,
            ict_total    = float(t.get("ict_total",   0.0) or 0.0),
            n_confirming = int(t.get("n_conf",        0) or 0),
        )

    # ──────────────────────────────────────────────────────────────────────────
    # INTERNAL: Exit Geometry
    # ──────────────────────────────────────────────────────────────────────────

    def _compute_geometry(
        self, rec: TradeRecord, atr: float, pos
    ) -> None:
        """
        Compute all MAE / MFE geometry metrics for the closed trade.

        G-Ratio  = MFE / (MFE + MAE)
          Interpretation: 1.0 = all excursion was favorable (textbook trade)
                          0.0 = all excursion was adverse (entered at the top/bottom)

        SL Efficiency = MAE / SL_distance
          The fraction of the SL buffer that was consumed by adverse movement.
          > 1.0 means SL was definitively hit (not just touched).

        Entry Efficiency = realised_pts / MFE
          How much of the maximum available profit the trade captured.
          For losers this is 0 by definition (negative realised).

        TP Efficiency = MFE / TP_distance
          How close to the TP target price was reached.
          Capped at 1.0; the value is 1.0 if TP was actually hit.
        """
        g = rec.geometry

        mfe = max(0.0, self._pending_mfe_pts)
        mae = max(0.0, self._pending_mae_pts)

        if rec.side == "long":
            realised = rec.exit_price - rec.entry_price
        else:
            realised = rec.entry_price - rec.exit_price

        sl_dist = (abs(rec.entry_price - rec.sl_price)
                   if rec.sl_price > 0 else atr)
        tp_dist = (abs(rec.entry_price - rec.tp_price)
                   if rec.tp_price > 0 else atr * 2.5)

        g.mfe_pts      = mfe
        g.mae_pts      = mae
        g.realised_pts = realised
        g.sl_distance  = sl_dist
        g.tp_distance  = tp_dist
        g.was_tp_hit   = rec.exit_reason in ("tp_hit",)
        g.was_sl_hit   = rec.exit_reason in ("sl_hit", "trail_sl_hit")

        # G-Ratio: fraction of total excursion that was favorable
        # Undefined when both MFE and MAE are zero → 0.5 (neutral)
        g.g_ratio = _safe_ratio(mfe, mfe + mae, 0.5)

        # Entry Efficiency: fraction of MFE actually captured
        if mfe > 1e-10:
            g.entry_efficiency = max(0.0, min(1.0, realised / mfe))
        else:
            g.entry_efficiency = 1.0 if realised > 0 else 0.0

        # SL Efficiency: how much of the SL buffer was consumed by MAE
        g.sl_efficiency = _safe_ratio(mae, sl_dist, 0.0)

        # TP Efficiency: how close to TP the trade reached
        if g.was_tp_hit:
            g.tp_efficiency = 1.0
        elif tp_dist > 1e-10:
            g.tp_efficiency = min(1.0, mfe / tp_dist)
        else:
            g.tp_efficiency = 0.0

        # R-multiples
        g.actual_r  = _safe_ratio(realised, sl_dist, 0.0)
        g.planned_r = _safe_ratio(tp_dist,  sl_dist, 0.0)

        # Hold efficiency: information per minute held
        if rec.hold_min > 0.1:
            g.hold_efficiency = realised / rec.hold_min
        else:
            g.hold_efficiency = 0.0

        # Wick sweep detection: SL efficiency > threshold but had prior MFE
        # (price ran in our favour first, then institutional wick swept the SL)
        g.wick_sweep = (
            g.sl_efficiency > _SL_TIGHT_THRESHOLD and
            mfe > 0.25 * sl_dist
        )

        # ── Quality score [0, 1] ──────────────────────────────────────────────
        # Weighted composite of: G-ratio (geometry), entry efficiency (capture),
        # TP efficiency (target accuracy), and SL efficiency (structure quality)
        sl_quality = 1.0 - min(1.0, max(0.0, g.sl_efficiency - 0.3) / 0.7)
        tp_cap     = g.tp_efficiency if g.was_tp_hit else g.entry_efficiency * 0.6

        g.quality_score = round(min(1.0, max(0.0,
            g.g_ratio        * 0.30 +
            g.entry_efficiency * 0.35 +
            tp_cap             * 0.20 +
            sl_quality         * 0.15
        )), 3)

    # ──────────────────────────────────────────────────────────────────────────
    # INTERNAL: Entry Quality
    # ──────────────────────────────────────────────────────────────────────────

    def _compute_entry_quality(
        self, rec: TradeRecord, pos,
        atr: float, ict_engine, liq_snapshot
    ) -> None:
        """
        Score the structural quality of the entry setup across six dimensions.

        Institutional importance order (weight allocation):
          AMD phase alignment  28% — primary directional thesis
          OTE zone score       22% — optimal retracement entry
          ICT confluence       18% — structural confluence at entry
          Displacement         15% — institutional displacement confirmed
          Session / kill-zone  12% — timing quality
          Pool significance     5% — strength of swept liquidity
        """
        eq = rec.entry_quality

        # ── OTE Zone ─────────────────────────────────────────────────────────
        sw_price = self._pending_sweep_price
        if sw_price > 1e-10 and rec.entry_price > 1e-10:
            eq.ote_score, eq.ote_dist_from_center = _ote_score(
                rec.entry_price, sw_price,
                "SSL" if rec.side == "long" else "BSL",
                atr
            )
        else:
            eq.ote_score            = 0.5   # neutral when sweep unknown
            eq.ote_dist_from_center = 0.5

        # ── AMD Phase ─────────────────────────────────────────────────────────
        eq.amd_conf        = rec.amd_conf
        eq.amd_phase_score = _amd_phase_score(
            rec.amd_phase, rec.amd_bias, rec.side, rec.amd_conf)

        # ── ICT Confluence ────────────────────────────────────────────────────
        # Use ict_total from trade_record (capped at 1.0; stored at entry time)
        eq.ict_confluence = min(1.0, max(0.0, rec.ict_total))
        if eq.ict_confluence < 1e-10:
            # Fallback: derive from n_confirming (3 confirms ≈ full confluence)
            eq.ict_confluence = min(1.0, rec.n_confirming / 3.0)

        # ── Pool Significance ─────────────────────────────────────────────────
        eq.pool_sig = 0.0
        if liq_snapshot is not None:
            try:
                sweeps = getattr(liq_snapshot, 'recent_sweeps', [])
                if sweeps:
                    best = max(sweeps,
                               key=lambda s: getattr(s, 'detected_at', 0))
                    pool = getattr(best, 'pool', None)
                    if pool is not None:
                        eq.pool_sig = float(
                            getattr(pool, 'significance', 0.0) or 0.0)
            except Exception:
                pass

        # ── Displacement ──────────────────────────────────────────────────────
        # Confirmed if we have a valid sweep price (set_exit_context found one)
        # Partially scored from composite magnitude as proxy
        if sw_price > 1e-10:
            eq.displacement_score = 1.0
        else:
            # Proxy: strong composite suggests directional displacement
            eq.displacement_score = min(1.0, abs(rec.composite) * 1.5 + 0.3)

        # ── Session / Kill-Zone ───────────────────────────────────────────────
        session = self._pending_session or ""
        kz_map  = {
            "KZ_LON": 1.0, "KZ_NY": 1.0, "KZ_NEW": 1.0,
            "LONDON": 0.90, "NEW_YORK": 0.90, "NY": 0.90,
            "ASIA": 0.20, "UNKNOWN": 0.50, "": 0.50,
        }
        eq.session_score = kz_map.get(session, 0.60)

        # Fine-grained: check ICT engine directly for kill-zone
        if ict_engine is not None and not session.startswith("KZ_"):
            try:
                kz = str(getattr(ict_engine, '_killzone', '') or '').upper()
                if kz and kz not in ("", "NONE", "OFF_HOURS"):
                    eq.session_score = 1.0   # active kill zone = maximum quality
            except Exception:
                pass

        eq.ict_tier       = rec.ict_tier

        # Composite entry quality — ICT institutional weighting
        pool_norm         = min(1.0, eq.pool_sig / 10.0)
        eq.composite_score = float(rec.composite)
        eq.overall_score   = round(min(1.0, max(0.0,
            eq.amd_phase_score   * 0.28 +
            eq.ote_score         * 0.22 +
            eq.ict_confluence    * 0.18 +
            eq.displacement_score * 0.15 +
            eq.session_score     * 0.12 +
            pool_norm            * 0.05
        )), 3)

    # ──────────────────────────────────────────────────────────────────────────
    # INTERNAL: Causation Classification
    # ──────────────────────────────────────────────────────────────────────────

    def _classify_causation(self, rec: TradeRecord) -> None:
        rec.geometry.causation = _classify_exit_causation(rec, rec.geometry)
        if rec.geometry.was_sl_hit or rec.exit_reason in (
            "sl_hit", "regime_flip", "flow_reversal",
            "exiting_timeout", "max_hold"
        ):
            self._sl_causation[rec.geometry.causation] += 1
        elif rec.geometry.was_tp_hit or rec.exit_reason == "tp_hit":
            self._tp_causation[rec.geometry.causation] += 1

    # ──────────────────────────────────────────────────────────────────────────
    # INTERNAL: Statistics Update
    # ──────────────────────────────────────────────────────────────────────────

    def _get_or_create(
        self, store: Dict[str, DimensionStats], key: str
    ) -> DimensionStats:
        if key not in store:
            store[key] = DimensionStats(label=key)
        return store[key]

    def _update_statistics(self, rec: TradeRecord) -> None:
        """Update all per-dimension Bayesian statistics."""
        self._stats_overall.add_trade(rec)

        # ICT tier
        tier_key = rec.ict_tier.strip() if rec.ict_tier.strip() else "none"
        self._get_or_create(self._stats_by_tier, tier_key).add_trade(rec)

        # AMD phase
        if rec.amd_phase:
            self._get_or_create(
                self._stats_by_amd, rec.amd_phase.upper()).add_trade(rec)

        # Regime
        if rec.regime:
            self._get_or_create(
                self._stats_by_regime, rec.regime).add_trade(rec)

        # Trade mode
        if rec.mode:
            self._get_or_create(
                self._stats_by_mode, rec.mode).add_trade(rec)

        # Session (captured at exit-context time)
        if self._pending_session:
            self._get_or_create(
                self._stats_by_session,
                self._pending_session).add_trade(rec)

    # ──────────────────────────────────────────────────────────────────────────
    # INTERNAL: IC Tracking
    # ──────────────────────────────────────────────────────────────────────────

    def _update_ic(self, rec: TradeRecord) -> None:
        """
        Update the Information Coefficient buffer.

        Signal = composite score × direction sign.
          Positive composite + long position → bullish signal +composite
          Positive composite + short position → bearish signal −composite
          (The rec.composite is already signed in _build_record.)

        Outcome = +1 for win, -1 for loss.
        IC = Pearson(signal, outcome) across the rolling window.
        """
        # Use signed composite as the directional signal
        signal  = rec.composite if abs(rec.composite) > 1e-10 else (
            (1.0 if rec.side == "long" else -1.0) * rec.entry_quality.overall_score
        )
        outcome = 1.0 if rec.pnl > 0 else -1.0
        self._ic_buffer.append((signal, outcome))

        # Per-tier IC
        tier_key = rec.ict_tier.strip() if rec.ict_tier.strip() else "none"
        self._ic_by_tier[tier_key].append((signal, outcome))

        # Per-session IC
        if self._pending_session:
            self._ic_by_session[self._pending_session].append((signal, outcome))

    def _compute_ic(self) -> float:
        """
        Compute rolling Pearson IC between signal direction and trade outcome.
        IC > 0.05 → signal has predictive value.
        IC ≈ 0.00 → signal is noise.
        IC < 0.00 → signal is inversely predictive (red flag — investigate).
        """
        if len(self._ic_buffer) < 3:
            return 0.0
        xs = [x for x, _ in self._ic_buffer]
        ys = [y for _, y in self._ic_buffer]
        return _pearson_correlation(xs, ys)

    def _compute_dimension_ic(
        self, buf: deque
    ) -> float:
        """Pearson IC for a single dimension buffer."""
        if len(buf) < 3:
            return 0.0
        xs = [x for x, _ in buf]
        ys = [y for _, y in buf]
        return _pearson_correlation(xs, ys)

    # ──────────────────────────────────────────────────────────────────────────
    # INTERNAL: Adaptive Parameter Engine
    # ──────────────────────────────────────────────────────────────────────────

    def _run_adaptive_engine(self, rec: TradeRecord) -> None:
        """
        Core Bayesian adaptive engine. Runs after each trade closure.

        Design principles (all must hold before any adjustment fires):
          1. Minimum _MIN_SAMPLES (5) real observations.
          2. Wilson credible interval must fully clear the neutral threshold.
          3. Minimum 3 trades between adjustments (prevents oscillation).
          4. All adjustments are multiplicative deltas capped at ±40% of base.
          5. Evidence accumulates across sessions (not reset on restart).
          6. Negative-evidence adjustments (reverting widening SL after
             wick sweeps reduce) are equally possible.
        """
        now     = time.time()
        n_total = len(self.records) + 1   # including the trade just processed

        # Guard: nothing to do until we have sufficient observations
        if n_total < _MIN_SAMPLES:
            return

        recent = self._stats_overall.recent_trades

        # ── 1. SL BUFFER ATR MULTIPLIER ──────────────────────────────────────
        # Evidence source: wick_sweep rate and avg SL efficiency
        if len(recent) >= _MIN_SAMPLES:
            n_r       = len(recent)
            avg_sl    = sum(r.geometry.sl_efficiency for r in recent) / n_r
            wick_rate = sum(1 for r in recent if r.geometry.wick_sweep) / n_r

            if wick_rate > _WICK_RATE_WIDEN and self._can_adjust(
                self.params.sl_buffer_atr, now
            ):
                # Widen proportional to wick rate (10–25% widening)
                adj   = 1.0 + wick_rate * 0.25
                new_m = min(1.0 + _MAX_PARAM_DRIFT,
                            self.params.sl_buffer_atr.current_mult * adj)
                self.params.sl_buffer_atr.apply_adjustment(
                    new_m,
                    f"wick_rate={wick_rate:.0%} avg_sl_eff={avg_sl:.2f}",
                    now,
                )
                self._emit_insight(
                    dimension="SL_BUFFER",
                    severity="ACTION",
                    message=(
                        f"{wick_rate:.0%} of SL exits are wick sweeps "
                        f"(avg SL eff={avg_sl:.2f}) — SL too tight"
                    ),
                    recommendation=(
                        f"Widen SL noise buffer: ×{new_m:.3f} "
                        f"(effective {self.params.sl_buffer_atr.effective_value:.3f} ATR)"
                    ),
                    confidence=min(0.95, wick_rate * 2.0),
                    param_key="sl_buffer_atr",
                    param_new_mult=new_m,
                )

            elif (avg_sl < 0.20 and wick_rate < 0.10
                  and self._can_adjust(self.params.sl_buffer_atr, now)):
                # SL consistently too wide → tighten
                new_m = max(1.0 - _MAX_PARAM_DRIFT,
                            self.params.sl_buffer_atr.current_mult * 0.92)
                self.params.sl_buffer_atr.apply_adjustment(
                    new_m,
                    f"sl_too_wide avg_sl_eff={avg_sl:.2f}",
                    now,
                )
                self._emit_insight(
                    dimension="SL_BUFFER",
                    severity="INFO",
                    message=(
                        f"SL efficiency low ({avg_sl:.2f}) — SL too wide, "
                        f"capturing unnecessary risk"
                    ),
                    recommendation=(
                        f"Tighten SL buffer: ×{new_m:.3f}"
                    ),
                    confidence=0.65,
                    param_key="sl_buffer_atr",
                    param_new_mult=new_m,
                )

        # ── 2. TP DISTANCE MULTIPLIER ─────────────────────────────────────────
        if len(recent) >= _MIN_SAMPLES:
            n_r        = len(recent)
            avg_tp_eff = sum(r.geometry.tp_efficiency for r in recent) / n_r
            tp_hit_rt  = sum(1 for r in recent if r.geometry.was_tp_hit)  / n_r
            avg_ent_ef = sum(r.geometry.entry_efficiency for r in recent) / n_r

            if (avg_tp_eff < _TP_EFFICIENCY_FLOOR and tp_hit_rt < 0.35
                    and self._can_adjust(self.params.tp_distance_mult, now)):
                # TP consistently too ambitious → tighten
                new_m = max(1.0 - _MAX_PARAM_DRIFT,
                            self.params.tp_distance_mult.current_mult * 0.91)
                self.params.tp_distance_mult.apply_adjustment(
                    new_m,
                    f"tp_eff={avg_tp_eff:.2f} hit_rate={tp_hit_rt:.0%}",
                    now,
                )
                self._emit_insight(
                    dimension="TP_PLACEMENT",
                    severity="ACTION",
                    message=(
                        f"TP efficiency={avg_tp_eff:.2f}, hit rate={tp_hit_rt:.0%} "
                        f"— TP consistently too ambitious"
                    ),
                    recommendation=(
                        f"Use closer pool as TP: ×{new_m:.3f}"
                    ),
                    confidence=min(0.92, (1.0 - avg_tp_eff) * 1.5),
                    param_key="tp_distance_mult",
                    param_new_mult=new_m,
                )

            elif (avg_tp_eff > 0.90 and tp_hit_rt > 0.65
                  and avg_ent_ef > 0.70
                  and self._can_adjust(self.params.tp_distance_mult, now)):
                # TP well-placed and frequently hit → allow slight extension
                new_m = min(1.0 + _MAX_PARAM_DRIFT,
                            self.params.tp_distance_mult.current_mult * 1.05)
                self.params.tp_distance_mult.apply_adjustment(
                    new_m,
                    f"tp_eff={avg_tp_eff:.2f} hit_rate={tp_hit_rt:.0%} excellent",
                    now,
                )
                self._emit_insight(
                    dimension="TP_PLACEMENT",
                    severity="INFO",
                    message=(
                        f"TP eff={avg_tp_eff:.2f}, hit rate={tp_hit_rt:.0%} "
                        f"— TPs well-placed, room to extend to next pool"
                    ),
                    recommendation=(
                        f"Extend TP to next pool: ×{new_m:.3f}"
                    ),
                    confidence=0.72,
                    param_key="tp_distance_mult",
                    param_new_mult=new_m,
                )

        # ── 3. AMD CONFIDENCE THRESHOLD ──────────────────────────────────────
        # If low-confidence AMD entries (conf < threshold) consistently lose
        low_conf_trades = [
            r for r in self.records[-_ROLLING_WINDOW:]
            if r.amd_conf < _AMD_LOW_CONF
        ]
        if len(low_conf_trades) >= _MIN_SAMPLES:
            lc_wins = sum(1 for r in low_conf_trades if r.is_win)
            lc_wr   = lc_wins / len(low_conf_trades)
            lc_ci_u = _wilson_upper(
                len(low_conf_trades), lc_wr, _RECOMMENDATION_CI)
            # CI upper fully below 40% → structurally unprofitable at low AMD conf
            if lc_ci_u < 0.40 and self._can_adjust(
                self.params.amd_conf_threshold, now
            ):
                new_m = min(1.0 + _MAX_PARAM_DRIFT,
                            self.params.amd_conf_threshold.current_mult * 1.12)
                self.params.amd_conf_threshold.apply_adjustment(
                    new_m,
                    f"low_amd_wr={lc_wr:.0%} n={len(low_conf_trades)}",
                    now,
                )
                self._emit_insight(
                    dimension="AMD_CONFIDENCE",
                    severity="WARN",
                    message=(
                        f"Low AMD conf trades (<{_AMD_LOW_CONF}) WR={lc_wr:.0%} "
                        f"(n={len(low_conf_trades)}) — below breakeven"
                    ),
                    recommendation=(
                        f"Raise AMD confidence threshold to "
                        f"≥{self.params.amd_conf_threshold.effective_value:.2f}"
                    ),
                    confidence=0.78,
                    param_key="amd_conf_threshold",
                    param_new_mult=new_m,
                )

        # ── 4. ICT TIER SIZING ────────────────────────────────────────────────
        for tier, ps in [
            ("S", self.params.tier_s_sizing),
            ("A", self.params.tier_a_sizing),
            ("B", self.params.tier_b_sizing),
        ]:
            d = self._stats_by_tier.get(tier)
            if d is None or d.bayes.n < _MIN_SAMPLES:
                continue
            if not self._can_adjust(ps, now, min_trades_since=5):
                continue

            wr       = d.bayes.mean
            ci_lower = d.bayes.ci_lower(_RECOMMENDATION_CI)
            ci_upper = d.bayes.ci_upper(_RECOMMENDATION_CI)

            if ci_lower < 0.43 and wr < 0.47:
                # CI fully below 43% → underperforming tier → reduce sizing
                new_m = max(1.0 - _MAX_PARAM_DRIFT, ps.current_mult * 0.87)
                ps.apply_adjustment(
                    new_m,
                    f"Tier-{tier} WR={wr:.0%} CI_lo={ci_lower:.0%}",
                    now,
                )
                self._emit_insight(
                    dimension=f"TIER_{tier}_SIZING",
                    severity="WARN",
                    message=(
                        f"Tier-{tier} WR={wr:.0%} (n={d.bayes.n}) "
                        f"CI=[{ci_lower:.0%},{ci_upper:.0%}] — underperforming"
                    ),
                    recommendation=(
                        f"Reduce Tier-{tier} position size: ×{new_m:.3f}"
                    ),
                    confidence=min(0.88, (0.50 - wr) * 4.0),
                    param_key=f"tier_{tier.lower()}_sizing",
                    param_new_mult=new_m,
                )

            elif ci_lower > 0.58 and wr > 0.60:
                # CI lower fully above 58% → outperforming → allow more size
                new_m = min(1.20, ps.current_mult * 1.08)
                ps.apply_adjustment(
                    new_m,
                    f"Tier-{tier} WR={wr:.0%} excellent",
                    now,
                )
                self._emit_insight(
                    dimension=f"TIER_{tier}_SIZING",
                    severity="INFO",
                    message=(
                        f"Tier-{tier} WR={wr:.0%} (n={d.bayes.n}) — outperforming"
                    ),
                    recommendation=(
                        f"Increase Tier-{tier} position size: ×{new_m:.3f}"
                    ),
                    confidence=min(0.85, wr),
                    param_key=f"tier_{tier.lower()}_sizing",
                    param_new_mult=new_m,
                )

        # ── 5. ENTRY CONFIRM TICKS (wick sweep → require more confirmation) ───
        if len(recent) >= _MIN_SAMPLES and self._can_adjust(
            self.params.entry_confirm_ticks, now, min_trades_since=7
        ):
            n_r       = len(recent)
            wick_rate = sum(1 for r in recent if r.geometry.wick_sweep) / n_r
            if wick_rate > 0.50:
                # Very high wick sweep rate → require more confirmation ticks
                new_m = min(1.0 + _MAX_PARAM_DRIFT,
                            self.params.entry_confirm_ticks.current_mult * 1.20)
                self.params.entry_confirm_ticks.apply_adjustment(
                    new_m,
                    f"high_wick_rate={wick_rate:.0%}",
                    now,
                )
                self._emit_insight(
                    dimension="ENTRY_CONFIRM",
                    severity="WARN",
                    message=(
                        f"Wick sweep rate={wick_rate:.0%} — high noise, "
                        f"entries fired too early"
                    ),
                    recommendation=(
                        f"Raise confirm ticks: "
                        f"×{new_m:.3f} "
                        f"(effective {self.params.entry_confirm_ticks.effective_value:.1f} ticks)"
                    ),
                    confidence=0.72,
                    param_key="entry_confirm_ticks",
                    param_new_mult=new_m,
                )

        # ── 6. OTE ZONE BOUNDARY ADJUSTMENT ──────────────────────────────────
        ote_trades = [r for r in self.records[-_ROLLING_WINDOW:]
                      if r.entry_quality.ote_score > 0.0]
        if len(ote_trades) >= _MIN_SAMPLES:
            deep_ote = [r for r in ote_trades
                        if r.entry_quality.ote_dist_from_center < 0.30]
            edge_ote = [r for r in ote_trades
                        if r.entry_quality.ote_dist_from_center > 0.70]

            if len(deep_ote) >= 3 and len(edge_ote) >= 3:
                deep_wr = sum(1 for r in deep_ote if r.is_win) / len(deep_ote)
                edge_wr = sum(1 for r in edge_ote if r.is_win) / len(edge_ote)

                if (deep_wr - edge_wr > 0.20
                        and self._can_adjust(self.params.ote_fib_low, now)):
                    new_m = min(1.0 + _MAX_PARAM_DRIFT,
                                self.params.ote_fib_low.current_mult * 1.04)
                    self.params.ote_fib_low.apply_adjustment(
                        new_m,
                        f"deep_wr={deep_wr:.0%} edge_wr={edge_wr:.0%}",
                        now,
                    )
                    self._emit_insight(
                        dimension="OTE_ZONE",
                        severity="INFO",
                        message=(
                            f"Center OTE WR={deep_wr:.0%} vs Edge OTE "
                            f"WR={edge_wr:.0%} — tighter zone favoured"
                        ),
                        recommendation=(
                            f"Raise OTE lower bound: "
                            f"{self.params.ote_fib_low.effective_value:.3f}"
                        ),
                        confidence=0.68,
                        param_key="ote_fib_low",
                        param_new_mult=new_m,
                    )

        # ── 7. SESSION PATTERN ALERT ──────────────────────────────────────────
        for sess_key, d in self._stats_by_session.items():
            if d.bayes.n < _MIN_SAMPLES:
                continue
            ci_u = d.bayes.ci_upper(_RECOMMENDATION_CI)
            if ci_u < 0.40:
                self._emit_insight(
                    dimension=f"SESSION_{sess_key}",
                    severity="WARN",
                    message=(
                        f"Session {sess_key}: WR={d.bayes.mean:.0%} "
                        f"(n={d.bayes.n}, CI upper={ci_u:.0%}) "
                        f"— structural underperformance"
                    ),
                    recommendation=(
                        f"Raise conviction score threshold or reduce size "
                        f"during {sess_key} session"
                    ),
                    confidence=1.0 - ci_u,
                )

        # ── 8. IC ALERT ───────────────────────────────────────────────────────
        ic = self._compute_ic()
        if len(self._ic_buffer) >= 10 and ic < -0.10:
            self._emit_insight(
                dimension="IC_SIGNAL",
                severity="WARN",
                message=(
                    f"Information Coefficient IC={ic:.3f} — signals are "
                    f"inversely predictive (n={len(self._ic_buffer)})"
                ),
                recommendation=(
                    "Review entry logic — signals appear to be contra-indicators. "
                    "Consider reversing composite sign threshold or increasing "
                    "ICT confluence requirement."
                ),
                confidence=min(0.90, abs(ic) * 3.0),
            )

    # ──────────────────────────────────────────────────────────────────────────
    # INTERNAL: Guard — rate-limit parameter adjustments
    # ──────────────────────────────────────────────────────────────────────────

    def _can_adjust(
        self, ps: ParameterState, now: float, min_trades_since: int = 3
    ) -> bool:
        """
        Rate-limiting guard: prevent oscillation from adjusting parameters
        faster than evidence can accumulate.

        Requires at least min_trades_since trades since the last adjustment.
        First adjustment (adjustment_count == 0) is always permitted.
        """
        if ps.adjustment_count == 0:
            return True
        trades_since = sum(
            1 for r in self.records
            if r.ts > ps.last_adjusted_ts
        )
        return trades_since >= min_trades_since

    # ──────────────────────────────────────────────────────────────────────────
    # INTERNAL: Emit Insight
    # ──────────────────────────────────────────────────────────────────────────

    def _emit_insight(
        self,
        dimension:      str,
        severity:       str,
        message:        str,
        recommendation: str,
        confidence:     float,
        param_key:      str   = "",
        param_new_mult: float = 1.0,
    ) -> None:
        ins = AgentInsight(
            dimension=dimension,
            severity=severity,
            message=message,
            recommendation=recommendation,
            confidence=confidence,
            param_key=param_key,
            param_new_mult=param_new_mult,
        )
        self.insights.append(ins)
        if len(self.insights) > 100:
            self.insights = self.insights[-100:]

        _icon = {"INFO": "ℹ️", "WARN": "⚠️", "ACTION": "🔧"}.get(severity, "•")
        logger.info(
            f"{_icon} PostTradeInsight [{severity}] [{dimension}] "
            f"conf={confidence:.2f}: {message} | → {recommendation}"
        )

    # ──────────────────────────────────────────────────────────────────────────
    # INTERNAL: Store & Log
    # ──────────────────────────────────────────────────────────────────────────

    def _store_record(self, rec: TradeRecord) -> None:
        self.records.append(rec)
        if len(self.records) > 200:
            self.records = self.records[-200:]

    def _log_analysis(self, rec: TradeRecord) -> None:
        g  = rec.geometry
        eq = rec.entry_quality
        logger.info(
            f"📊 PostTrade [{rec.side.upper()}|{rec.ict_tier or '-'}"
            f"|{rec.amd_phase[:5]}] "
            f"pnl=${rec.pnl:+.2f} r={g.actual_r:+.2f}R reason={rec.exit_reason} "
            f"cause={g.causation} "
            f"G={g.g_ratio:.2f} MFE={g.mfe_pts:.0f}pts MAE={g.mae_pts:.0f}pts "
            f"SLeff={g.sl_efficiency:.2f} TPeff={g.tp_efficiency:.2f} "
            f"EEff={g.entry_efficiency:.2f} q={g.quality_score:.2f} "
            f"entry={eq.overall_score:.2f} "
            f"OTE={eq.ote_score:.2f} AMD={eq.amd_phase_score:.2f} "
            f"ICT={eq.ict_confluence:.2f}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# TELEGRAM FORMAT HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def format_trade_analysis_alert(
    rec: TradeRecord,
    overall_wr: float,
    agent: Optional['PostTradeAgent'] = None,
) -> str:
    """
    Formats a single-trade institutional post-trade debrief for Telegram.

    Called from quant_strategy._record_pnl() after every closed trade.
    This is the SECOND message sent after a close (first is the raw exit summary).
    It provides the structural analysis: geometry, causation, entry quality, and
    any adaptive parameter adjustments triggered by this trade.

    Parameters:
        rec:        The TradeRecord built by PostTradeAgent.on_trade_closed()
        overall_wr: Current overall session Bayesian WR (from agent._stats_overall)
        agent:      Optional PostTradeAgent reference for adaptive state display
    """
    g  = rec.geometry
    eq = rec.entry_quality

    side_icon   = "🟢" if rec.side == "long" else "🔴"
    result_icon = "✅" if rec.is_win else "❌"

    cause_icons: Dict[str, str] = {
        "POOL_REACHED": "🎯",
        "STRUCTURAL":   "🏛️",
        "TRAIL_LOCK":   "🔒",
        "EARLY_TP":     "⚠️",
        "WICK_SWEEP":   "💨",
        "BOS_BREAK":    "💥",
        "AMD_FLIP":     "🔄",
        "NOISE_HIT":    "🔊",
        "POOL_SWEPT":   "🌊",
        "UNKNOWN":      "❓",
    }
    cause_icon = cause_icons.get(g.causation, "❓")

    # R-multiple display
    r_str      = f"{g.actual_r:+.2f}R"
    planned_str = f"(planned 1:{g.planned_r:.1f}R)"

    lines = [
        f"{result_icon} <b>TRADE DEBRIEF</b>  "
        f"{side_icon} {rec.side.upper()}  [{rec.ict_tier or '—'}]  {r_str}",
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━",
        f"<b>💰 RESULT</b>",
        f"  PnL: <b>${rec.pnl:+.2f}</b>  |  Exit: {rec.exit_reason}",
        f"  {cause_icon} Causation: <b>{g.causation}</b>",
        f"  Achieved: {r_str}  {planned_str}",
        "",
        f"<b>📐 GEOMETRY</b>",
        f"  MFE: {g.mfe_pts:.0f}pts  MAE: {g.mae_pts:.0f}pts  "
        f"G-Ratio: {g.g_ratio:.2f}",
        f"  Entry Eff: {g.entry_efficiency:.0%}  "
        f"SL Eff: {g.sl_efficiency:.2f}  "
        f"TP Eff: {g.tp_efficiency:.0%}",
        f"  Hold: {rec.hold_min:.1f}m  "
        f"Hold Eff: {g.hold_efficiency:+.1f}pts/min  "
        f"Quality: {g.quality_score:.2f}",
    ]

    # SL-specific structural commentary
    if g.was_sl_hit:
        if g.wick_sweep:
            lines.append(
                f"  ⚠️ <i>Wick sweep — SL too tight for pool noise range</i>")
        elif g.sl_efficiency < _SL_WIDE_THRESHOLD:
            lines.append(
                f"  ℹ️ <i>Wide SL — price broke structure cleanly (thesis failed)</i>")
        else:
            lines.append(
                f"  ✅ <i>Structural SL — thesis invalidated at correct level</i>")

    # TP-specific commentary
    if g.was_tp_hit:
        if g.tp_efficiency >= 0.90:
            lines.append(
                f"  ✅ <i>Pool delivered — institutional target confirmed</i>")
        else:
            lines.append(
                f"  ℹ️ <i>TP hit but efficiency={g.tp_efficiency:.0%} "
                f"— may have used conservative pool</i>")
    elif not g.was_sl_hit and g.tp_efficiency < 0.50:
        lines.append(
            f"  ⚠️ <i>TP too ambitious — only {g.tp_efficiency:.0%} reached</i>")

    lines += [
        "",
        f"<b>🏛️ ENTRY QUALITY</b>",
        f"  Score: <b>{eq.overall_score:.2f}</b>  Tier: {rec.ict_tier or '—'}",
        f"  OTE: {eq.ote_score:.2f}  AMD: {eq.amd_phase_score:.2f}  "
        f"ICT: {eq.ict_confluence:.2f}",
        f"  Disp: {eq.displacement_score:.2f}  Session: {eq.session_score:.2f}  "
        f"Pool: {eq.pool_sig:.1f}",
        f"  AMD phase: {rec.amd_phase} (conf={rec.amd_conf:.2f})  "
        f"ADX={rec.adx:.0f}",
    ]

    # HTF alignment
    if abs(rec.htf_15m) > 0.01 or abs(rec.htf_4h) > 0.01:
        lines.append(
            f"  HTF: 15m={rec.htf_15m:+.2f}  4H={rec.htf_4h:+.2f}")

    lines += [
        "",
        f"<b>📈 SESSION</b>: WR <b>{overall_wr:.0%}</b>  "
        f"(Bayesian estimate)",
        "━━━━━━━━━━━━━━━━━━━━━━━━",
    ]

    # Adaptive state — show any adjustments triggered by this trade
    if agent is not None:
        adjusted = agent.get_adaptive_state()
        if adjusted:
            lines.append("<b>🔧 Adaptive Adjustments Active</b>")
            for key, state in adjusted.items():
                lines.append(
                    f"  {key}: ×{state['mult']:.3f}  "
                    f"(eff={state['effective']:.4f}, "
                    f"adj#{state['adj_count']})")

    return "\n".join(lines)

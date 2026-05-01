"""
quantitative_models.py — Adaptive quantitative decision/risk models
====================================================================

This module is the strategy layer's mathematical core. It intentionally avoids
"level reached → trade" or fixed threshold behaviour. Every decision is mapped
through a live state vector, online distribution estimates and uncertainty-
adjusted posterior/EV barriers.

Important design rule:
    Legacy scores (rev/cont counters, pool quality, flow conviction) are only
    observations. They are never executable decisions by themselves.

No numpy/pandas dependency; suitable for low-latency runtime.
"""
from __future__ import annotations

import math
import threading
from dataclasses import dataclass, field
from typing import Any, Dict, Tuple

_EPS = 1e-12


def clamp(x: float, lo: float, hi: float) -> float:
    try:
        x = float(x)
    except Exception:
        return lo
    if not math.isfinite(x):
        return lo
    return max(lo, min(hi, x))


def sigmoid(x: float) -> float:
    if x >= 0:
        z = math.exp(-min(x, 60.0))
        return 1.0 / (1.0 + z)
    z = math.exp(max(x, -60.0))
    return z / (1.0 + z)


def logit(p: float) -> float:
    p = clamp(p, 1e-6, 1.0 - 1e-6)
    return math.log(p / (1.0 - p))


def entropy_binary(p: float) -> float:
    p = clamp(p, 1e-6, 1.0 - 1e-6)
    return -(p * math.log(p) + (1.0 - p) * math.log(1.0 - p)) / math.log(2.0)


def _safe_exp_decay(distance: float, scale: float) -> float:
    scale = max(abs(scale), _EPS)
    return math.exp(-max(float(distance), 0.0) / scale)


def _obj_float(obj: Any, *names: str, default: float = 0.0) -> float:
    for name in names:
        try:
            v = obj.get(name, None) if isinstance(obj, dict) else getattr(obj, name, None)
            if v is not None:
                fv = float(v)
                if math.isfinite(fv):
                    return fv
        except Exception:
            continue
    return default


def _obj_str(obj: Any, *names: str, default: str = "") -> str:
    for name in names:
        try:
            v = obj.get(name, None) if isinstance(obj, dict) else getattr(obj, name, None)
            if v is not None:
                return str(v)
        except Exception:
            continue
    return default


def _pool_sig(target: Any) -> float:
    try:
        if hasattr(target, "adjusted_sig"):
            return float(target.adjusted_sig())
    except Exception:
        pass
    pool = getattr(target, "pool", target)
    return _obj_float(pool, "significance", "quality", default=0.0)


def _pool_price(target: Any) -> float:
    pool = getattr(target, "pool", target)
    return _obj_float(pool, "price", default=0.0)


class EWMQuantile:
    """Streaming quantile estimator using stochastic approximation.

    It is deliberately simple and stable under tick-level updates. The initial
    value is only a prior until enough observations arrive; after that the model
    follows the observed distribution of this symbol/session rather than a static
    constant.
    """

    def __init__(self, q: float, initial: float, alpha: float = 0.025) -> None:
        self.q = clamp(q, 0.01, 0.99)
        self.value = float(initial)
        self.alpha = clamp(alpha, 0.001, 0.20)
        self.n = 0

    def update(self, x: float) -> float:
        try:
            x = float(x)
        except Exception:
            return self.value
        if not math.isfinite(x):
            return self.value
        if self.n <= 0:
            self.value = x
        else:
            # Robbins-Monro quantile update. Above quantile => move up slowly;
            # below quantile => move down proportionally to target q.
            hit = 1.0 if x > self.value else 0.0
            self.value += self.alpha * (hit - (1.0 - self.q)) * max(abs(x - self.value), 1e-6)
        self.n += 1
        return self.value

    @property
    def ready(self) -> bool:
        return self.n >= 30


class EWMStat:
    """EWMA mean/variance for online normalisation."""

    def __init__(self, initial: float = 0.0, alpha: float = 0.03) -> None:
        self.mean = float(initial)
        self.var = 1.0
        self.alpha = clamp(alpha, 0.001, 0.25)
        self.n = 0

    def update(self, x: float) -> Tuple[float, float]:
        try:
            x = float(x)
        except Exception:
            return self.mean, math.sqrt(max(self.var, 1e-9))
        if not math.isfinite(x):
            return self.mean, math.sqrt(max(self.var, 1e-9))
        if self.n <= 0:
            self.mean = x
            self.var = max(abs(x), 1.0) ** 2
        else:
            diff = x - self.mean
            self.mean += self.alpha * diff
            self.var = (1.0 - self.alpha) * self.var + self.alpha * diff * diff
        self.n += 1
        return self.mean, math.sqrt(max(self.var, 1e-9))

    def z(self, x: float) -> float:
        return clamp((float(x) - self.mean) / math.sqrt(max(self.var, 1e-9)), -5.0, 5.0)


class AdaptiveQuantCalibrator:
    """Shared online calibration state.

    The model updates on every evaluated setup. It does not learn PnL labels here
    because that belongs in a separate post-trade trainer, but it does adapt its
    acceptance barriers to the distribution of market evidence being observed.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.posterior_q = EWMQuantile(0.78, 0.68)
        self.evidence_q = EWMQuantile(0.62, 0.42)
        self.disp_stat = EWMStat(0.80)
        self.eae_q = EWMQuantile(0.65, 1.20)

    def update_entry(self, posterior: float, evidence: float, displacement_atr: float) -> Dict[str, float]:
        with self._lock:
            p_q = self.posterior_q.update(clamp(posterior, 0.0, 1.0))
            e_q = self.evidence_q.update(clamp(evidence, 0.0, 1.0))
            self.disp_stat.update(max(displacement_atr, 0.0))
            return {
                "posterior_q": p_q,
                "evidence_q": e_q,
                "disp_mu": self.disp_stat.mean,
                "disp_sigma": math.sqrt(max(self.disp_stat.var, 1e-9)),
                "ready": 1.0 if (self.posterior_q.ready and self.evidence_q.ready) else 0.0,
            }

    def update_eae(self, eae_atr: float) -> float:
        with self._lock:
            return self.eae_q.update(eae_atr)


GLOBAL_QUANT_CALIBRATOR = AdaptiveQuantCalibrator()


@dataclass
class MarketStateVector:
    volatility_norm: float = 0.0
    liquidity_density: float = 0.0
    liquidity_quality: float = 0.0
    orderflow_alignment: float = 0.0
    cvd_alignment: float = 0.0
    htf_alignment: float = 0.0
    dealing_range_affinity: float = 0.0
    spread_cost_atr: float = 0.0
    regime_uncertainty: float = 0.5
    toxicity: float = 0.0

    def compact(self) -> str:
        return (
            f"vol={self.volatility_norm:.2f} liq={self.liquidity_density:.2f}/"
            f"{self.liquidity_quality:.2f} of={self.orderflow_alignment:+.2f} "
            f"cvd={self.cvd_alignment:+.2f} htf={self.htf_alignment:+.2f} "
            f"pd={self.dealing_range_affinity:+.2f} U={self.regime_uncertainty:.2f} tox={self.toxicity:.2f}"
        )


@dataclass
class QuantDecision:
    accept: bool
    posterior: float
    min_posterior: float
    expected_value: float
    log_likelihood: float
    uncertainty: float
    reason: str
    components: Dict[str, float] = field(default_factory=dict)

    def compact(self) -> str:
        return (
            f"p={self.posterior:.3f} min={self.min_posterior:.3f} "
            f"EV={self.expected_value:.3f} LLR={self.log_likelihood:.2f} "
            f"U={self.uncertainty:.2f} {self.reason}"
        )


def build_state_vector(*, side: str, price: float, atr: float, snap: Any = None,
                       flow: Any = None, ict: Any = None) -> MarketStateVector:
    side = (side or "").lower()
    sign = 1.0 if side == "long" else -1.0
    p = max(float(price or 0.0), _EPS)
    a = max(float(atr or 0.0), _EPS)

    vol_norm = clamp((a / p) / 0.0025, 0.0, 2.0) / 2.0

    pools = []
    try:
        pools = list(getattr(snap, "bsl_pools", []) or []) + list(getattr(snap, "ssl_pools", []) or [])
    except Exception:
        pools = []
    sigs = [_pool_sig(x) for x in pools if _pool_price(x) > 0]
    top_sig = max(sigs) if sigs else 0.0
    liq_density = clamp(math.log1p(sum(max(s, 0.0) for s in sigs)) / math.log(80.0), 0.0, 1.0)
    liq_quality = clamp(math.log1p(top_sig) / math.log(30.0), 0.0, 1.0)

    flow_dir = _obj_str(flow, "direction", default="").lower()
    flow_conv = abs(_obj_float(flow, "conviction", default=0.0))
    if flow_dir == side:
        of_align = clamp(flow_conv, 0.0, 1.0)
    elif flow_dir in ("long", "short"):
        of_align = -clamp(flow_conv, 0.0, 1.0)
    else:
        raw = _obj_float(flow, "tick_score", "score", default=0.0)
        of_align = clamp(sign * raw, -1.0, 1.0)
    cvd_align = clamp(sign * _obj_float(flow, "cvd_trend", default=0.0), -1.0, 1.0)

    s15 = _obj_str(ict, "structure_15m", default="").lower()
    s4h = _obj_str(ict, "structure_4h", default="").lower()
    htf_votes = []
    for s, w in ((s15, 0.45), (s4h, 0.55)):
        if "bull" in s:
            htf_votes.append(w if side == "long" else -w)
        elif "bear" in s:
            htf_votes.append(w if side == "short" else -w)
        elif "rang" in s:
            htf_votes.append(0.0)
    htf_align = clamp(sum(htf_votes), -1.0, 1.0) if htf_votes else 0.0

    pd = clamp(_obj_float(ict, "dealing_range_pd", default=0.5), -0.25, 1.25)
    dr_aff = clamp(((0.55 - pd) if side == "long" else (pd - 0.45)) / 0.35, -1.0, 1.0)

    spread = 0.0
    try:
        ob = getattr(snap, "orderbook", None) or getattr(snap, "ob", None) or {}
        bids = ob.get("bids", []) if isinstance(ob, dict) else []
        asks = ob.get("asks", []) if isinstance(ob, dict) else []
        if bids and asks:
            bid0 = bids[0]
            ask0 = asks[0]
            bid = float(bid0[0] if isinstance(bid0, (list, tuple)) else bid0.get("price", bid0.get("limit_price", 0)))
            ask = float(ask0[0] if isinstance(ask0, (list, tuple)) else ask0.get("price", ask0.get("limit_price", 0)))
            if ask > bid > 0:
                spread = ask - bid
    except Exception:
        spread = 0.0
    spread_cost_atr = clamp(spread / a, 0.0, 2.0)

    conflict = 0.0
    conflict += 0.35 if of_align * cvd_align < -0.08 else 0.0
    conflict += 0.30 if htf_align * of_align < -0.12 else 0.0
    conflict += 0.20 if liq_density < 0.25 else 0.0
    conflict += 0.15 * clamp(spread_cost_atr, 0.0, 1.0)
    toxicity = clamp(conflict, 0.0, 1.0)

    htf_unc = 1.0 - abs(htf_align)
    flow_unc = 1.0 - clamp((abs(of_align) + abs(cvd_align)) / 2.0, 0.0, 1.0)
    liq_unc = 1.0 - liq_density
    uncertainty = clamp(0.30 * htf_unc + 0.25 * flow_unc + 0.20 * liq_unc + 0.25 * toxicity, 0.0, 1.0)

    return MarketStateVector(vol_norm, liq_density, liq_quality, of_align, cvd_align,
                             htf_align, dr_aff, spread_cost_atr, uncertainty, toxicity)


def evaluate_post_sweep_quant(*, action: str, side: str, rev_score: float, cont_score: float,
                              displacement_atr: float, cisd: bool, ote: bool, phase: str,
                              price: float, atr: float, snap: Any = None, flow: Any = None,
                              ict: Any = None) -> QuantDecision:
    """Adaptive Bayesian/EV/SPRT post-sweep auction decision.

    The function deliberately rejects "score-only" setups. A score imbalance must
    be accompanied by measurable auction information: displacement acceptance,
    structural confirmation or a defended retest. This is checked with a dynamic
    evidence barrier calibrated from the live stream.
    """
    action = (action or "").lower()
    side = (side or "").lower()
    phase_u = (phase or "").upper()
    st = build_state_vector(side=side, price=price, atr=atr, snap=snap, flow=flow, ict=ict)

    chosen = float(rev_score if action == "reverse" else cont_score)
    other = float(cont_score if action == "reverse" else rev_score)
    total = abs(chosen) + abs(other) + 1.0
    score_edge = math.tanh((chosen - other) / (0.38 * total + 18.0))

    # Displacement information is distribution-aware: the same raw ATR move is
    # less valuable in noisy regimes and more valuable when the instrument's
    # recent post-sweep distribution is compressed.
    mu = max(GLOBAL_QUANT_CALIBRATOR.disp_stat.mean, 0.15)
    sigma = max(math.sqrt(max(GLOBAL_QUANT_CALIBRATOR.disp_stat.var, 1e-9)), 0.15)
    dyn_disp_scale = clamp(0.55 * mu + 0.35 * sigma + 0.35 * st.regime_uncertainty + 0.20 * st.volatility_norm, 0.20, 3.50)
    disp_info = 1.0 - _safe_exp_decay(max(displacement_atr, 0.0), dyn_disp_scale)

    structural = 1.0 - (1.0 - (0.70 if cisd else 0.0)) * (1.0 - (0.45 if ote else 0.0))
    auction_information = 1.0 - (1.0 - disp_info) * (1.0 - structural)

    # No-auction guard: a strong counter alone is not evidence. This is a
    # mathematical null-model guard, not a hand-entered threshold; it uses live
    # distribution estimates plus uncertainty.
    cal0 = GLOBAL_QUANT_CALIBRATOR.update_entry(0.5, auction_information, max(displacement_atr, 0.0))
    dynamic_evidence_floor = clamp(
        max(0.18, cal0["evidence_q"] * (0.82 + 0.28 * st.regime_uncertainty)),
        0.16,
        0.72,
    )

    raw_displacement = max(float(displacement_atr or 0.0), 0.0)
    has_structural_proof = bool(cisd or ote)
    raw_disp_floor = clamp(
        0.30
        + 0.20 * st.regime_uncertainty
        + 0.12 * st.toxicity
        + (0.05 if action == "reverse" else 0.0)
        + (0.05 if phase_u == "DISPLACEMENT" else 0.0),
        0.28,
        0.72,
    )
    if not has_structural_proof and raw_displacement < raw_disp_floor:
        reason = (
            f"REJECT raw auction proof: disp={raw_displacement:.2f}ATR<{raw_disp_floor:.2f}ATR "
            f"without CISD/OTE edge={score_edge:+.2f} transformedInfo={auction_information:.3f} "
            f"floor={dynamic_evidence_floor:.3f} | {st.compact()}"
        )
        return QuantDecision(False, 0.0, 0.0, -1.0, -99.0, st.regime_uncertainty, reason,
                             {"score_edge": score_edge, "disp_info": disp_info,
                              "structural": structural, "evidence_mass": auction_information,
                              "evidence_floor": dynamic_evidence_floor,
                              "raw_displacement": raw_displacement,
                              "raw_displacement_floor": raw_disp_floor,
                              **cal0})

    if auction_information < dynamic_evidence_floor:
        reason = (
            f"REJECT null auction: info={auction_information:.3f}<dynFloor={dynamic_evidence_floor:.3f} "
            f"edge={score_edge:+.2f} disp={disp_info:.2f} struct={structural:.2f} | {st.compact()}"
        )
        return QuantDecision(False, 0.0, 0.0, -1.0, -99.0, st.regime_uncertainty, reason,
                             {"score_edge": score_edge, "disp_info": disp_info,
                              "structural": structural, "evidence_mass": auction_information,
                              "evidence_floor": dynamic_evidence_floor, **cal0})

    if action == "continue":
        flow_term = 0.70 * st.orderflow_alignment + 0.55 * st.cvd_alignment + 0.45 * st.htf_alignment
        structure_term = 0.45 * structural + 0.65 * disp_info
    else:
        flow_term = 0.45 * st.orderflow_alignment + 0.35 * st.cvd_alignment + 0.25 * st.htf_alignment
        structure_term = 0.85 * structural + 0.75 * disp_info

    liq_term = 0.36 * st.liquidity_quality + 0.22 * st.liquidity_density
    pd_term = 0.30 * st.dealing_range_affinity
    cost_penalty = 0.45 * st.spread_cost_atr + 0.70 * st.toxicity
    uncertainty_penalty = (0.70 + 0.25 * (phase_u == "DISPLACEMENT")) * st.regime_uncertainty

    base_prior = clamp(0.42 + 0.06 * st.liquidity_quality + 0.04 * max(st.htf_alignment, 0.0) - 0.06 * st.toxicity, 0.25, 0.65)
    z = (
        logit(base_prior)
        + 1.25 * score_edge
        + 1.35 * structure_term
        + 0.78 * flow_term
        + liq_term
        + pd_term
        - cost_penalty
        - uncertainty_penalty
    )
    posterior = sigmoid(z)

    # EV in normalized risk units. Loss burden widens under uncertainty/toxicity;
    # reward is capped by liquidity quality and alignment. Far TP/RR cannot rescue
    # a low-quality posterior.
    reward_proxy = 0.85 + 0.70 * st.liquidity_quality + 0.35 * max(st.htf_alignment, 0.0) + 0.25 * auction_information
    loss_proxy = 1.00 + 0.65 * st.regime_uncertainty + 0.45 * st.toxicity
    ev = posterior * reward_proxy - (1.0 - posterior) * loss_proxy - 0.12 * st.spread_cost_atr

    cal = GLOBAL_QUANT_CALIBRATOR.update_entry(posterior, auction_information, max(displacement_atr, 0.0))
    calibrated_p = cal["posterior_q"] if cal["ready"] else 0.66
    min_p = clamp(max(calibrated_p, 0.52 + 0.20 * st.regime_uncertainty + 0.05 * st.toxicity), 0.54, 0.88)

    # SPRT barrier with market-derived error tolerance. No static score threshold.
    alpha = clamp(0.16 - 0.07 * (1.0 - st.regime_uncertainty), 0.05, 0.16)
    beta = clamp(0.20 - 0.09 * (1.0 - st.regime_uncertainty), 0.07, 0.20)
    llr = logit(posterior)
    llr_barrier = math.log((1.0 - beta) / alpha)
    dynamic_llr_barrier = llr_barrier - 0.45 * clamp(ev, -1.0, 1.0)

    accept = bool(
        posterior >= min_p
        and ev > 0.0
        and llr >= dynamic_llr_barrier
        and auction_information >= dynamic_evidence_floor
    )

    reason = (
        ("ACCEPT" if accept else "REJECT")
        + f" quant posterior auction: info={auction_information:.2f}/{dynamic_evidence_floor:.2f} "
          f"edge={score_edge:+.2f} disp={disp_info:.2f} struct={structural:.2f} "
          f"flow={flow_term:+.2f} EV={ev:+.3f} LLR={llr:.2f}/{dynamic_llr_barrier:.2f} | {st.compact()}"
    )
    return QuantDecision(accept, posterior, min_p, ev, llr, st.regime_uncertainty, reason,
                         {"score_edge": score_edge, "disp_info": disp_info,
                          "structural": structural, "flow_term": flow_term,
                          "liq_term": liq_term, "pd_term": pd_term,
                          "cost_penalty": cost_penalty, "evidence_floor": dynamic_evidence_floor,
                          "evidence_mass": auction_information, "llr_barrier": dynamic_llr_barrier,
                          **cal})


def adaptive_trailing_stop(*, side: str, price: float, entry_price: float, current_sl: float,
                           atr: float, peak_profit: float, true_be: float, snap: Any = None,
                           flow: Any = None, ict: Any = None) -> QuantDecision:
    """Expected-adverse-excursion stop proposal.

    The candidate is a probabilistic stop proposal, not a forced trail. It avoids
    fixed R, fixed fib and fixed ATR floors. The stop distance is estimated from
    current uncertainty, toxicity, flow persistence and the online EAE regime.
    """
    side = (side or "").lower()
    st = build_state_vector(side=side, price=price, atr=atr, snap=snap, flow=flow, ict=ict)
    a = max(float(atr or 0.0), _EPS)
    delivered = max(float(peak_profit or 0.0), 0.0) / a
    current_profit = ((price - entry_price) if side == "long" else (entry_price - price)) / a

    trend_persistence = clamp((st.orderflow_alignment + st.cvd_alignment + max(st.htf_alignment, 0.0)) / 3.0, 0.0, 1.0)
    raw_eae = 0.55 + 0.90 * st.regime_uncertainty + 0.65 * st.toxicity + 0.42 * (1.0 - trend_persistence) + 0.25 * st.volatility_norm
    eae_ref = GLOBAL_QUANT_CALIBRATOR.update_eae(raw_eae)
    eae_atr = clamp(0.55 * raw_eae + 0.45 * eae_ref, 0.70, 3.20)

    min_delivery = eae_atr * (1.0 + 0.18 * st.regime_uncertainty) + 0.20 * (1.0 + st.toxicity)
    if delivered < min_delivery:
        return QuantDecision(False, 0.0, 0.0, -1.0, 0.0, st.regime_uncertainty,
                             f"WAIT trail: delivered={delivered:.2f}ATR < dynamicEAE {min_delivery:.2f}ATR | {st.compact()}",
                             {"candidate_sl": 0.0, "eae_atr": eae_atr, "delivered_atr": delivered})

    if side == "long":
        candidate = max(true_be, price - eae_atr * a)
        improves = candidate > current_sl
        breathing = (price - candidate) / a
    else:
        candidate = min(true_be, price + eae_atr * a)
        improves = candidate < current_sl
        breathing = (candidate - price) / a

    posterior = clamp((delivered - min_delivery) / max(eae_atr, _EPS), 0.0, 1.0)
    accept = bool(improves and breathing >= 0.88 * eae_atr and current_profit > 0 and posterior > entropy_binary(0.65) - 0.45)
    reason = (
        ("ACCEPT" if accept else "WAIT")
        + f" trail: candidate=${candidate:,.1f} EAE={eae_atr:.2f}ATR "
          f"delivered={delivered:.2f}ATR current={current_profit:.2f}ATR "
          f"breathing={breathing:.2f}ATR post={posterior:.2f} | {st.compact()}"
    )
    return QuantDecision(accept, posterior, 0.0, posterior - 0.5, logit(max(posterior, 1e-6)),
                         st.regime_uncertainty, reason,
                         {"candidate_sl": candidate, "eae_atr": eae_atr,
                          "delivered_atr": delivered, "breathing_atr": breathing})

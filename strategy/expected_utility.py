"""
expected_utility.py — institutional target/stop/sizing math
============================================================

Decision principle
------------------
Levels are observations; executable TP/SL are selected by the institutional
entry engine.  These utility surfaces are advisory context for edge, sizing,
and runner attribution; they must not replace exchange bracket levels.

This module builds two continuous surfaces:
  1. StopSurface: where the thesis is statistically invalidated with enough
     noise/liquidity buffer, without hiding behind arbitrary ATR windows.
  2. TargetSurface: which liquidity objective has the best expected utility
     after execution cost, path risk, uncertainty, and adverse selection.

Important design choice
-----------------------
A distant HTF pool can be a terminal *runner objective*, but it must not become
full-size TP simply because it prints a huge R:R. Full-position TP is selected
from executable utility; terminal objectives get a separate runner utility and
runner_fraction metadata for future reduce-only laddering.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, List, Optional, Tuple

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


def _pool(obj: Any) -> Any:
    return getattr(obj, "pool", obj)


def _pool_price(obj: Any) -> float:
    return _obj_float(_pool(obj), "price", default=0.0)


def _pool_sig(obj: Any) -> float:
    try:
        if hasattr(obj, "adjusted_sig"):
            v = float(obj.adjusted_sig())
            if math.isfinite(v):
                return v
    except Exception:
        pass
    target_sig = _obj_float(obj, "significance", "quality", default=0.0)
    if target_sig > 0.0:
        return target_sig
    p = _pool(obj)
    return _obj_float(p, "significance", "quality", default=0.0)


def _pool_tf_rank(obj: Any) -> int:
    p = _pool(obj)
    tf = _obj_str(p, "timeframe", "tf", default="").lower()
    return {"1m": 1, "2m": 1, "3m": 1, "5m": 2, "15m": 3,
            "30m": 3, "1h": 4, "2h": 4, "4h": 5, "1d": 6}.get(tf, 2)


def _pool_side(obj: Any) -> str:
    p = _pool(obj)
    side = _obj_str(p, "side", "pool_type", "type", default="").upper()
    if "BSL" in side or "BUY" in side:
        return "BSL"
    if "SSL" in side or "SELL" in side:
        return "SSL"
    return side


def _get_pools_for_side(snapshot: Any, trade_side: str) -> List[Any]:
    if snapshot is None:
        return []
    trade_side = (trade_side or "").lower()
    attr = "bsl_pools" if trade_side == "long" else "ssl_pools"
    try:
        return list(getattr(snapshot, attr, []) or [])
    except Exception:
        return []


def _protective_pools(snapshot: Any, trade_side: str) -> List[Any]:
    if snapshot is None:
        return []
    trade_side = (trade_side or "").lower()
    attr = "ssl_pools" if trade_side == "long" else "bsl_pools"
    try:
        return list(getattr(snapshot, attr, []) or [])
    except Exception:
        return []


def _all_opposing_pools(snapshot: Any, trade_side: str) -> List[Any]:
    return _protective_pools(snapshot, trade_side)


def _feed_reliability(snapshot: Any) -> float:
    for name in ("feed_reliability", "reliability", "data_reliability"):
        v = _obj_float(snapshot, name, default=-1.0)
        if v >= 0:
            return clamp(v, 0.20, 1.0)
    meta = getattr(snapshot, "meta", None)
    if isinstance(meta, dict):
        for key in ("feed_reliability", "reliability", "microstructure_weight"):
            if key in meta:
                return clamp(float(meta[key]), 0.20, 1.0)
    return 0.82


def _target_reach_buffer(distance_atr: float, atr: float) -> float:
    """Match the executable TP buffer used by the liquidity selector."""
    return max(0.10, min(0.50, 0.10 + 0.05 * float(distance_atr or 0.0))) * max(float(atr or 0.0), _EPS)


def _executable_target_price(pool_price: float, side: str, entry: float,
                             atr: float, distance_atr: float) -> float:
    """Convert a raw liquidity pool into the TP price actually placed before it."""
    buf = _target_reach_buffer(distance_atr, atr)
    side = (side or "").lower()
    if side == "long":
        tp = float(pool_price) - buf
        return tp if tp > float(entry) + 1e-9 else 0.0
    if side == "short":
        tp = float(pool_price) + buf
        return tp if tp < float(entry) - 1e-9 else 0.0
    return 0.0


def _market_terms(side: str, flow: Any, ict: Any) -> Tuple[float, float, float, float]:
    side = (side or "").lower()
    sign = 1.0 if side == "long" else -1.0

    tick = _obj_float(flow, "tick_flow", "tick", default=0.0)
    cvd = _obj_float(flow, "cvd_trend", "cvd", default=0.0)
    of_align = clamp(sign * (0.52 * tick + 0.48 * cvd), -1.0, 1.0)

    s15 = _obj_str(ict, "structure_15m", default="").lower()
    s4h = _obj_str(ict, "structure_4h", default="").lower()
    htf = 0.0
    for s, w in ((s15, 0.45), (s4h, 0.55)):
        if "bull" in s:
            htf += w if side == "long" else -w
        elif "bear" in s:
            htf += w if side == "short" else -w
    htf = clamp(htf, -1.0, 1.0)

    pd = clamp(_obj_float(ict, "dealing_range_pd", "_pd_percentile", default=0.5), 0.0, 1.0)
    # PD is an input, not a veto. Longs prefer discount, shorts prefer premium.
    pd_affinity = clamp(((0.55 - pd) if side == "long" else (pd - 0.45)) / 0.40, -1.0, 1.0)

    conflict = 0.0
    conflict += 0.42 if of_align * htf < -0.10 else 0.0
    conflict += 0.24 if abs(of_align) < 0.08 else 0.0
    conflict += 0.18 if abs(htf) < 0.08 else 0.0
    conflict = clamp(conflict, 0.0, 1.0)
    uncertainty = clamp(0.38 * (1.0 - abs(of_align)) + 0.32 * (1.0 - abs(htf)) + 0.30 * conflict, 0.0, 1.0)
    return of_align, htf, pd_affinity, uncertainty


@dataclass
class StopUtility:
    price: float
    anchor: float
    risk_points: float
    risk_atr: float
    stop_run_risk: float
    survival_probability: float
    invalidation_quality: float
    utility: float
    role: str
    notes: List[str] = field(default_factory=list)
    pool_ref: Any = None

    def compact(self) -> str:
        return (
            f"{self.role} SL=${self.price:,.1f} risk={self.risk_atr:.2f}ATR "
            f"survive={self.survival_probability:.2f} stoprun={self.stop_run_risk:.2f} U={self.utility:+.3f}"
        )


@dataclass
class StopSurface:
    side: str
    entry: float
    current_stop: float
    best: Optional[StopUtility]
    candidates: List[StopUtility]
    notes: List[str] = field(default_factory=list)

    def compact(self, limit: int = 3) -> str:
        best = self.best.compact() if self.best else "none"
        rows = " | ".join(c.compact() for c in self.candidates[:limit])
        return f"best={best}" + (f" | {rows}" if rows else "")


@dataclass
class TargetUtility:
    price: float
    side: str
    rr: float
    distance_atr: float
    probability: float
    expected_value_r: float
    utility: float
    full_position_utility: float
    runner_utility: float
    payoff_r: float
    loss_r: float
    cost_r: float
    gauntlet_penalty: float
    path_risk: float
    role: str
    pool_tf_rank: int = 0
    pool_significance: float = 0.0
    notes: List[str] = field(default_factory=list)
    pool_ref: Any = None

    def compact(self) -> str:
        return (
            f"{self.role} ${self.price:,.1f} p={self.probability:.3f} "
            f"EV={self.expected_value_r:+.3f} U={self.utility:+.3f} "
            f"fullU={self.full_position_utility:+.3f} RR={self.rr:.2f} d={self.distance_atr:.1f}ATR"
        )


@dataclass
class TargetSurface:
    side: str
    entry: float
    stop: float
    risk_points: float
    best: Optional[TargetUtility]
    terminal: Optional[TargetUtility]
    candidates: List[TargetUtility]
    runner_fraction: float
    notes: List[str] = field(default_factory=list)

    @property
    def has_positive_edge(self) -> bool:
        return bool(self.best and self.best.full_position_utility > 0.0 and self.best.expected_value_r > 0.0)

    def compact(self, limit: int = 3) -> str:
        rows = [c.compact() for c in self.candidates[:limit]]
        best = self.best.compact() if self.best else "none"
        term = self.terminal.compact() if self.terminal else "none"
        return f"best={best}; runner={term}; runner_frac={self.runner_fraction:.0%}; " + " | ".join(rows)


def build_stop_surface(*, side: str, entry: float, current_stop: float, atr: float,
                       snapshot: Any, flow: Any = None, ict: Any = None,
                       tick_size: float = 0.5) -> StopSurface:
    """Optimise structural invalidation stop.

    The surface prefers stops behind real protective liquidity/sweep structure,
    but balances that with risk consumption and stop-run likelihood. It can keep
    the existing stop when it is superior; it will not tighten merely to improve
    R:R.
    """
    side = (side or "").lower()
    entry = float(entry or 0.0)
    current_stop = float(current_stop or 0.0)
    atr = max(float(atr or 0.0), _EPS)
    if side not in ("long", "short") or entry <= 0 or current_stop <= 0:
        return StopSurface(side, entry, current_stop, None, [], ["invalid stop-surface inputs"])

    of_align, htf, pd_affinity, uncertainty = _market_terms(side, flow, ict)
    reliability = _feed_reliability(snapshot)
    tick_size = max(float(tick_size or 0.5), 1e-9)

    raw_candidates: List[Tuple[float, float, str, float, int, Any]] = []
    # Existing exchange stop remains a candidate.
    raw_candidates.append((current_stop, current_stop, "existing", 1.0, 3, None))

    for p in _protective_pools(snapshot, side):
        px = _pool_price(p)
        if px <= 0:
            continue
        if side == "long" and px >= entry:
            continue
        if side == "short" and px <= entry:
            continue
        sig = max(_pool_sig(p), 0.0)
        tf_rank = _pool_tf_rank(p)
        # Buffer is continuous: larger when uncertainty/reliability are poor,
        # smaller when structure is high quality. No static ATR window/veto.
        sig_term = clamp(math.log1p(sig) / math.log(30.0), 0.0, 1.0)
        buf_atr = clamp(0.055 + 0.115 * uncertainty + 0.060 * (1.0 - reliability) - 0.035 * sig_term,
                        0.035, 0.240)
        buffer = max(tick_size * (2.0 + tf_rank), atr * buf_atr)
        stop = px - buffer if side == "long" else px + buffer
        raw_candidates.append((stop, px, "liquidity-invalidation", sig, tf_rank, p))

    cands: List[StopUtility] = []
    for stop, anchor, role, sig, tf_rank, ref in raw_candidates:
        if side == "long" and stop >= entry:
            continue
        if side == "short" and stop <= entry:
            continue
        risk = abs(entry - stop)
        risk_atr = risk / atr
        sig_term = clamp(math.log1p(max(sig, 0.0)) / math.log(30.0), 0.0, 1.0)
        tf_term = clamp((tf_rank - 1) / 5.0, 0.0, 1.0)
        # Too tight = stop-run risk; too wide = capital drag. Both are continuous.
        stop_run_risk = clamp(math.exp(-risk_atr / (0.70 + 0.35 * tf_term)) * (1.0 - 0.35 * sig_term), 0.0, 1.0)
        wide_risk_drag = clamp((risk_atr - (2.2 + 1.6 * tf_term)) / 5.0, 0.0, 1.0)
        invalidation_quality = clamp(0.38 * sig_term + 0.26 * tf_term + 0.18 * max(htf, 0.0) + 0.18 * reliability,
                                      0.0, 1.0)
        survival = clamp(sigmoid(1.20 * invalidation_quality - 1.10 * stop_run_risk - 0.65 * uncertainty - 0.30 * wide_risk_drag),
                         0.02, 0.98)
        utility = survival + 0.40 * invalidation_quality - 0.55 * stop_run_risk - 0.22 * wide_risk_drag - 0.08 * risk_atr
        notes = [f"sig={sig:.1f}", f"tf={tf_rank}", f"U={uncertainty:.2f}", f"feed={reliability:.2f}"]
        cands.append(StopUtility(stop, anchor, risk, risk_atr, stop_run_risk, survival,
                                 invalidation_quality, utility, role, notes, ref))

    cands.sort(key=lambda x: x.utility, reverse=True)
    notes = [f"of={of_align:+.2f}", f"htf={htf:+.2f}", f"pd={pd_affinity:+.2f}",
             f"U={uncertainty:.2f}", f"feed={reliability:.2f}"]
    return StopSurface(side, entry, current_stop, cands[0] if cands else None, cands, notes)


def _target_distance_cap(dist_atr: float, tf_term: float, reliability: float, role: str) -> float:
    # Continuous horizon cap. At 40+ ATR, probability cannot be large unless
    # model is explicitly calibrated by realised outcomes in future versions.
    horizon = 4.2 + 3.3 * tf_term + 2.0 * reliability
    cap = 0.78 * math.exp(-dist_atr / max(horizon, 1e-6)) + 0.050 * tf_term + 0.030 * reliability
    if role == "terminal":
        cap *= 0.55
    return clamp(cap, 0.0015, 0.92)


def build_target_surface(*, side: str, entry: float, stop: float, atr: float,
                         snapshot: Any, flow: Any = None, ict: Any = None,
                         fee_bps: float = 8.0, slippage_bps: float = 2.0,
                         posterior_prob: float = 0.0) -> TargetSurface:
    side = (side or "").lower()
    entry = float(entry or 0.0)
    stop = float(stop or 0.0)
    atr = max(float(atr or 0.0), _EPS)
    risk = abs(entry - stop)
    if side not in ("long", "short") or entry <= 0 or stop <= 0 or risk <= 0:
        return TargetSurface(side, entry, stop, risk, None, None, [], 0.0, ["invalid surface inputs"])

    of_align, htf, pd_affinity, uncertainty = _market_terms(side, flow, ict)
    reliability = _feed_reliability(snapshot)
    try:
        total_cost_bps = float(fee_bps) + float(slippage_bps)
    except Exception:
        total_cost_bps = 10.0
    cost_points = entry * (max(0.0, total_cost_bps) / 10_000.0)
    cost_r = cost_points / max(risk, _EPS)

    cands: List[TargetUtility] = []
    pools = _get_pools_for_side(snapshot, side)
    opposing = _all_opposing_pools(snapshot, side)
    for target in pools:
        px = _pool_price(target)
        if px <= 0:
            continue
        if side == "long" and px <= entry:
            continue
        if side == "short" and px >= entry:
            continue
        pool_dist = abs(px - entry)
        pool_dist_atr = pool_dist / atr
        tp_px = _executable_target_price(px, side, entry, atr, pool_dist_atr)
        if tp_px <= 0:
            continue

        dist = abs(tp_px - entry)
        rr = dist / max(risk, _EPS)
        dist_atr = pool_dist_atr
        exec_dist_atr = dist / atr
        sig = _pool_sig(target)
        tf_rank = _pool_tf_rank(target)
        sig_term = clamp(math.log1p(max(sig, 0.0)) / math.log(30.0), 0.0, 1.0)
        tf_term = clamp((tf_rank - 1) / 5.0, 0.0, 1.0)

        lo, hi = min(entry, tp_px), max(entry, tp_px)
        opp_hits = 0
        opp_sig_sum = 0.0
        for op in opposing:
            opx = _pool_price(op)
            if lo < opx < hi:
                osig = max(_pool_sig(op), 0.0)
                if osig > 0:
                    opp_hits += 1
                    opp_sig_sum += osig
        gauntlet_penalty = clamp(math.exp(-0.060 * opp_sig_sum - 0.21 * opp_hits), 0.12, 1.0)

        role = "internal" if dist_atr <= 2.8 else ("external" if dist_atr <= 10.0 else "terminal")
        reach_decay = math.exp(-dist_atr / (4.2 + 2.9 * tf_term + 1.7 * reliability))
        z = (
            -1.05
            + 1.05 * sig_term
            + 0.55 * tf_term
            + 0.74 * of_align
            + 0.54 * htf
            + 0.28 * pd_affinity
            + 0.70 * reach_decay
            - 0.95 * uncertainty
            + math.log(max(gauntlet_penalty, 1e-6))
        )
        p_base = sigmoid(z) * reliability
        distance_cap = _target_distance_cap(dist_atr, tf_term, reliability, role)
        p_path = clamp(min(p_base, distance_cap), 0.001, 0.975)

        # v8: fuse target-path probability with the already-accepted auction
        # posterior.  The old surface ignored the posterior completely, so a
        # 85-92% accepted post-sweep auction could be re-priced as a 35-45%
        # target event purely from static pool distance.  That made the SL/TP
        # layer contradict the alpha layer and defer valid institutional
        # auctions.  We keep terminal objectives conservative, but for internal
        # and external liquidity objectives the posterior is a live observation
        # of delivery intent and must be part of executable TP probability.
        auction_p = clamp(float(posterior_prob or 0.0), 0.001, 0.999)
        if auction_p > 0.01 and role != "terminal":
            auction_edge = clamp((auction_p - 0.50) / 0.50, 0.0, 1.0)
            distance_decay_for_fusion = math.exp(-dist_atr / (7.0 + 2.5 * tf_term + reliability))
            fusion_weight = clamp(
                (0.22 + 0.30 * auction_edge + 0.16 * (1.0 - uncertainty)
                 + 0.08 * sig_term + 0.06 * gauntlet_penalty)
                * distance_decay_for_fusion,
                0.0, 0.68,
            )
            fused_p = p_path + fusion_weight * max(auction_p - p_path, 0.0)
            # Distance cap is a soft prior, not a hard veto, once the auction
            # posterior is already accepted.  Only part of the excess above cap
            # is admitted; this prevents lottery distant targets while allowing
            # nearer executable pools to inherit live auction evidence.
            if fused_p > distance_cap:
                p_hit = distance_cap + 0.65 * (fused_p - distance_cap)
            else:
                p_hit = fused_p
            p_hit = clamp(max(p_path, p_hit), 0.001, 0.975)
        else:
            p_hit = p_path

        path_risk = clamp((1.0 - gauntlet_penalty) + 0.40 * uncertainty + 0.20 * max(-of_align, 0.0)
                          + (0.030 * dist_atr if role == "terminal" else 0.008 * dist_atr), 0.0, 3.0)
        loss_r = 1.0 + 0.38 * uncertainty + 0.26 * path_risk
        payoff_r = rr
        ev_r = p_hit * payoff_r - (1.0 - p_hit) * loss_r - cost_r

        # Full-position utility is the executable risk-adjusted EV, not a
        # second hard probability model.  Concavity is still applied, but as a
        # drag against excessive R:R rather than as a duplicate loss term.
        concavity_drag = 0.035 * max(payoff_r - 3.0, 0.0) ** 1.15
        full_position_utility = (
            ev_r
            - concavity_drag
            - 0.12 * uncertainty
            - 0.070 * path_risk
        )
        if role == "terminal":
            terminal_drag = 0.35 + 0.030 * dist_atr + 0.25 * uncertainty
            full_position_utility -= terminal_drag

        # Runner utility may still value terminal skew, but the runner fraction
        # is separate from the full-size exchange TP.
        runner_utility = ev_r - 0.11 * uncertainty - 0.08 * path_risk
        utility = full_position_utility
        notes = [
            f"sig={sig:.1f}", f"tf={tf_rank}", f"reach={reach_decay:.2f}",
            f"feed={reliability:.2f}", f"gauntlet={opp_hits}/{opp_sig_sum:.1f}",
            f"cap={distance_cap:.3f}", f"pathP={p_path:.3f}", f"auctionP={auction_p:.3f}",
            f"pool=${px:,.1f}", f"execD={exec_dist_atr:.2f}ATR",
        ]
        cands.append(TargetUtility(tp_px, side, rr, dist_atr, p_hit, ev_r, utility,
                                   full_position_utility, runner_utility,
                                   payoff_r, loss_r, cost_r, gauntlet_penalty,
                                   path_risk, role, tf_rank, sig, notes, target))

    # Executable TP: full-position utility. Runner objective: runner utility.
    cands.sort(key=lambda x: x.full_position_utility, reverse=True)
    non_terminal = [c for c in cands if c.role != "terminal"]
    best = max(non_terminal, key=lambda x: x.full_position_utility, default=(cands[0] if cands else None))
    terminal_candidates = [c for c in cands if c.role == "terminal"]
    terminal = max(terminal_candidates, key=lambda x: x.runner_utility, default=None)

    runner_fraction = 0.0
    if terminal and terminal.runner_utility > 0.0 and terminal.probability >= 0.008:
        base = clamp(terminal.runner_utility / max(abs(best.full_position_utility) if best else 1.0, 0.25), 0.0, 1.2)
        runner_fraction = clamp(0.10 + 0.22 * base - 0.18 * uncertainty, 0.0, 0.35)

    notes = [f"of={of_align:+.2f}", f"htf={htf:+.2f}", f"pd={pd_affinity:+.2f}",
             f"U={uncertainty:.2f}", f"costR={cost_r:.3f}", f"feed={reliability:.2f}"]
    return TargetSurface(side, entry, stop, risk, best, terminal, cands, runner_fraction, notes)


def expected_utility_size_multiplier(surface: Optional[TargetSurface], posterior: float = 0.0) -> float:
    """Bounded fractional-Kelly style size scaler after alpha acceptance."""
    if not surface or not surface.best:
        return 0.70
    if not surface.has_positive_edge:
        return 0.0
    b = max(surface.best.payoff_r, _EPS)
    target_p = clamp(float(surface.best.probability or 0.0), 0.001, 0.999)
    auction_p_raw = float(posterior or 0.0)
    auction_p = clamp(auction_p_raw, 0.001, 0.999)
    p = target_p
    if (surface.best.role != "terminal"
            and surface.best.full_position_utility > 0.0
            and auction_p_raw > 0.01):
        p = clamp(0.72 * target_p + 0.28 * auction_p, 0.001, 0.999)
    q = 1.0 - p
    try:
        loss_r = max(float(getattr(surface.best, "loss_r", 1.0) or 1.0), _EPS)
    except Exception:
        loss_r = 1.0
    try:
        cost_r = max(0.0, float(getattr(surface.best, "cost_r", 0.0) or 0.0))
    except Exception:
        cost_r = 0.0
    effective_loss_r = loss_r + cost_r
    kelly = (b * p - q * effective_loss_r) / max(b * effective_loss_r, _EPS)
    edge = clamp(surface.best.full_position_utility, -2.0, 3.0)
    raw = 0.70 + 0.50 * clamp(kelly, -0.40, 0.80) + 0.14 * clamp(edge, -1.0, 1.0)
    if surface.runner_fraction > 0.0:
        raw += 0.03 * surface.runner_fraction
    return clamp(raw, 0.42, 1.15)

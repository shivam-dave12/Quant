"""
expected_utility.py — Institutional target/sizing/exit math
================================================================

This module removes the last retail-style decision residue from the strategy:
fixed target preference, hard premium/discount vetoes, and R-multiple target
selection.  It converts liquidity pools into a target surface and optimises the
trade using expected utility after fees, slippage, adverse excursion, feed
reliability and path gauntlet risk.

Design rule:
    Levels are observations.  The executable decision is expected utility.

No numpy/pandas dependency; all functions are deterministic and runtime-safe.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple

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
            return float(obj.adjusted_sig())
    except Exception:
        pass
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


def _all_opposing_pools(snapshot: Any, trade_side: str) -> List[Any]:
    if snapshot is None:
        return []
    trade_side = (trade_side or "").lower()
    attr = "ssl_pools" if trade_side == "long" else "bsl_pools"
    try:
        return list(getattr(snapshot, attr, []) or [])
    except Exception:
        return []


@dataclass
class TargetUtility:
    price: float
    side: str
    rr: float
    distance_atr: float
    probability: float
    expected_value_r: float
    utility: float
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
            f"RR={self.rr:.2f} d={self.distance_atr:.1f}ATR"
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
        return bool(self.best and self.best.utility > 0.0 and self.best.expected_value_r > 0.0)

    def compact(self, limit: int = 3) -> str:
        rows = [c.compact() for c in self.candidates[:limit]]
        best = self.best.compact() if self.best else "none"
        term = self.terminal.compact() if self.terminal else "none"
        return f"best={best}; terminal={term}; runner={self.runner_fraction:.0%}; " + " | ".join(rows)


def _feed_reliability(snapshot: Any) -> float:
    # MarketAggregator exposes feed reliability if available.  Old snapshots do
    # not; default to neutral-high rather than false precision.
    for name in ("feed_reliability", "reliability", "data_reliability"):
        v = _obj_float(snapshot, name, default=-1.0)
        if v >= 0:
            return clamp(v, 0.20, 1.0)
    meta = getattr(snapshot, "meta", None)
    if isinstance(meta, dict):
        v = meta.get("feed_reliability", meta.get("reliability", None))
        if v is not None:
            return clamp(float(v), 0.20, 1.0)
    return 0.82


def _market_terms(side: str, flow: Any, ict: Any) -> Tuple[float, float, float, float]:
    side = (side or "").lower()
    sign = 1.0 if side == "long" else -1.0

    tick = _obj_float(flow, "tick_flow", default=0.0)
    cvd = _obj_float(flow, "cvd_trend", default=0.0)
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

    pd = clamp(_obj_float(ict, "dealing_range_pd", default=0.5), 0.0, 1.0)
    # PD is not a veto. It shifts continuation/reversal probability.
    pd_affinity = clamp(((0.55 - pd) if side == "long" else (pd - 0.45)) / 0.40, -1.0, 1.0)

    conflict = 0.0
    conflict += 0.40 if of_align * htf < -0.10 else 0.0
    conflict += 0.25 if abs(of_align) < 0.08 else 0.0
    conflict += 0.20 if abs(htf) < 0.08 else 0.0
    conflict = clamp(conflict, 0.0, 1.0)
    uncertainty = clamp(0.38 * (1.0 - abs(of_align)) + 0.32 * (1.0 - abs(htf)) + 0.30 * conflict, 0.0, 1.0)
    return of_align, htf, pd_affinity, uncertainty


def build_target_surface(*, side: str, entry: float, stop: float, atr: float,
                         snapshot: Any, flow: Any = None, ict: Any = None,
                         fee_bps: float = 8.0, slippage_bps: float = 2.0) -> TargetSurface:
    """Build probability-adjusted utility for all live liquidity targets.

    The selected TP is not nearest/farthest/fixed-R. It is the target that
    maximises expected utility after path risk and costs.  Far HTF objectives can
    still be retained as runner targets, but they are not allowed to dominate the
    whole position purely because they print a large R:R.
    """
    side = (side or "").lower()
    entry = float(entry or 0.0)
    stop = float(stop or 0.0)
    atr = max(float(atr or 0.0), _EPS)
    risk = abs(entry - stop)
    if side not in ("long", "short") or entry <= 0 or stop <= 0 or risk <= 0:
        return TargetSurface(side, entry, stop, risk, None, None, [], 0.0, ["invalid surface inputs"])

    of_align, htf, pd_affinity, uncertainty = _market_terms(side, flow, ict)
    reliability = _feed_reliability(snapshot)
    cost_points = entry * ((float(fee_bps) + float(slippage_bps)) / 10_000.0)
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
        dist = abs(px - entry)
        rr = dist / max(risk, _EPS)
        dist_atr = dist / atr
        sig = _pool_sig(target)
        tf_rank = _pool_tf_rank(target)

        # Path gauntlet: significant opposing pools between entry and target
        lo, hi = min(entry, px), max(entry, px)
        opp_hits = 0
        opp_sig_sum = 0.0
        for op in opposing:
            opx = _pool_price(op)
            if lo < opx < hi:
                osig = max(_pool_sig(op), 0.0)
                if osig > 0:
                    opp_hits += 1
                    opp_sig_sum += osig
        gauntlet_penalty = clamp(math.exp(-0.055 * opp_sig_sum - 0.18 * opp_hits), 0.18, 1.0)

        # Continuous probability model.  No fixed max-distance veto.  Distance
        # decays probability, but high-quality HTF pools remain valid runner
        # objectives if utility justifies them.
        sig_term = clamp(math.log1p(max(sig, 0.0)) / math.log(30.0), 0.0, 1.0)
        tf_term = clamp((tf_rank - 1) / 5.0, 0.0, 1.0)
        reach_decay = math.exp(-dist_atr / (5.5 + 3.2 * tf_term + 2.4 * reliability))
        z = (
            -0.90
            + 1.20 * sig_term
            + 0.65 * tf_term
            + 0.70 * of_align
            + 0.52 * htf
            + 0.32 * pd_affinity
            + 0.90 * reach_decay
            - 0.88 * uncertainty
            + math.log(max(gauntlet_penalty, 1e-6))
        )
        p_hit = clamp(sigmoid(z) * reliability, 0.002, 0.975)

        # Expected adverse excursion and opportunity/stop-run burden increase
        # when uncertainty is high or liquidity path is gauntleted.
        path_risk = clamp((1.0 - gauntlet_penalty) + 0.45 * uncertainty + 0.20 * max(-of_align, 0.0), 0.0, 2.0)
        loss_r = 1.0 + 0.45 * uncertainty + 0.30 * path_risk
        payoff_r = rr
        ev_r = p_hit * payoff_r - (1.0 - p_hit) * loss_r - cost_r
        utility = ev_r - 0.15 * uncertainty - 0.08 * path_risk

        if dist_atr <= 2.2:
            role = "internal"
        elif dist_atr <= 7.5:
            role = "external"
        else:
            role = "terminal"

        notes = [
            f"sig={sig:.1f}", f"tf={tf_rank}", f"reach={reach_decay:.2f}",
            f"feed={reliability:.2f}", f"gauntlet={opp_hits}/{opp_sig_sum:.1f}",
        ]
        cands.append(TargetUtility(px, side, rr, dist_atr, p_hit, ev_r, utility,
                                   payoff_r, loss_r, cost_r, gauntlet_penalty,
                                   path_risk, role, tf_rank, sig, notes, target))

    cands.sort(key=lambda x: x.utility, reverse=True)
    best = cands[0] if cands else None
    terminal_candidates = [c for c in cands if c.role == "terminal"]
    terminal = max(terminal_candidates, key=lambda x: x.expected_value_r, default=None)

    # Runner fraction is mathematical: only allocate runner weight when terminal
    # EV is positive and its hit probability is not just lottery-like noise.
    runner_fraction = 0.0
    if terminal and terminal.expected_value_r > 0.0 and terminal.probability >= 0.015:
        ratio = terminal.expected_value_r / max((best.expected_value_r if best else terminal.expected_value_r), _EPS)
        runner_fraction = clamp(0.18 + 0.32 * ratio - 0.22 * uncertainty, 0.0, 0.50)

    notes = [
        f"of={of_align:+.2f}", f"htf={htf:+.2f}", f"pd={pd_affinity:+.2f}",
        f"U={uncertainty:.2f}", f"costR={cost_r:.3f}", f"feed={reliability:.2f}",
    ]
    return TargetSurface(side, entry, stop, risk, best, terminal, cands, runner_fraction, notes)


def expected_utility_size_multiplier(surface: Optional[TargetSurface], posterior: float = 0.0) -> float:
    """Fractional Kelly-style sizing multiplier bounded for live trading.

    It does not increase/decrease trade frequency. It scales size only after the
    master alpha decision has accepted. Negative/weak target utility scales down.
    """
    if not surface or not surface.best:
        return 0.70
    b = max(surface.best.payoff_r, _EPS)
    p = clamp(float(posterior or surface.best.probability), 0.001, 0.999)
    q = 1.0 - p
    kelly = (b * p - q) / b
    edge = clamp(surface.best.utility, -2.0, 3.0)
    # Half-Kelly with uncertainty/runner haircut.  Bound keeps risk sane.
    raw = 0.72 + 0.55 * clamp(kelly, -0.40, 0.80) + 0.12 * clamp(edge, -1.0, 1.0)
    if surface.runner_fraction > 0.0:
        raw += 0.04 * surface.runner_fraction
    return clamp(raw, 0.45, 1.18)

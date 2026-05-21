"""
dol_engine.py — draw-on-liquidity first trade thesis model
==========================================================

Institutional entry intent:
A sweep is only information.  A trade becomes executable only when the market
has a clean draw-on-liquidity (DOL), enough reachable reward after cost/risk,
and enough structural/order-flow confirmation for the selected playbook.

This module is deliberately side-effect free.  It does not place orders, modify
SL/TP, or invent synthetic targets.  It scores whether a proposed long/short
has a real, executable liquidity destination before the EntryEngine emits a
signal.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
import math

_EPS = 1e-12


def _clamp(x: float, lo: float, hi: float) -> float:
    try:
        v = float(x)
        if not math.isfinite(v):
            return lo
    except Exception:
        return lo
    return max(float(lo), min(float(hi), v))


def _obj(obj: Any, *names: str, default: Any = None) -> Any:
    for name in names:
        try:
            if isinstance(obj, dict) and name in obj:
                return obj.get(name)
            if hasattr(obj, name):
                return getattr(obj, name)
        except Exception:
            continue
    return default


def _num(obj: Any, *names: str, default: float = 0.0) -> float:
    if names:
        v = _obj(obj, *names, default=default)
    else:
        v = obj
    try:
        fv = float(v)
        return fv if math.isfinite(fv) else float(default)
    except Exception:
        return float(default)


def _pool(target: Any) -> Any:
    return _obj(target, "pool", default=target)


def _price(target: Any) -> float:
    return _num(_pool(target), "price", default=0.0)


def _side(target: Any) -> str:
    raw = str(_obj(_pool(target), "side", "pool_side", "pool_type", default="") or "").upper()
    if "BSL" in raw or "BUY" in raw:
        return "BSL"
    if "SSL" in raw or "SELL" in raw:
        return "SSL"
    return raw


def _status(target: Any) -> str:
    st = _obj(_pool(target), "status", default="ACTIVE")
    if hasattr(st, "value"):
        st = st.value
    return str(st or "ACTIVE").upper()


def _tf_rank(target: Any) -> float:
    tf = str(_obj(_pool(target), "timeframe", "tf", default="5m") or "5m").lower()
    return {
        "1m": 0.72, "2m": 0.76, "3m": 0.78, "5m": 0.86,
        "15m": 1.00, "30m": 1.08, "1h": 1.22, "2h": 1.30,
        "4h": 1.45, "1d": 1.62,
    }.get(tf, 0.90)


def _sig(target: Any) -> float:
    # Prefer adjusted significance when available; it usually includes MTF confluence.
    try:
        adj = getattr(target, "adjusted_sig", None)
        if callable(adj):
            v = float(adj())
            if math.isfinite(v):
                return max(0.0, v)
    except Exception:
        pass
    return max(0.0, _num(target, "significance", default=_num(_pool(target), "significance", "quality", default=0.0)))


def _target_pools(snapshot: Any, trade_side: str) -> List[Any]:
    if snapshot is None:
        return []
    attr = "bsl_pools" if str(trade_side).lower() == "long" else "ssl_pools"
    try:
        return list(getattr(snapshot, attr, []) or [])
    except Exception:
        return []


def _protective_pools(snapshot: Any, trade_side: str) -> List[Any]:
    if snapshot is None:
        return []
    attr = "ssl_pools" if str(trade_side).lower() == "long" else "bsl_pools"
    try:
        return list(getattr(snapshot, attr, []) or [])
    except Exception:
        return []


def _is_live(target: Any) -> bool:
    return _status(target) not in {"SWEPT", "CONSUMED", "ARCHIVED"}


def _in_profit_direction(side: str, entry: float, px: float) -> bool:
    return (side == "long" and px > entry) or (side == "short" and px < entry)


def _flow_alignment(flow: Any, side: str) -> Tuple[float, str]:
    side = str(side or "").lower()
    sign = 1.0 if side == "long" else -1.0
    fdir = str(_obj(flow, "direction", default="") or "").lower()
    conv = abs(_num(flow, "conviction", default=0.0))
    if fdir == side:
        of = conv
        note = f"flow_aligned {fdir}={conv:.2f}"
    elif fdir in {"long", "short"}:
        of = -conv
        note = f"flow_contra {fdir}={conv:.2f}"
    else:
        tick = _num(flow, "tick_flow", "tick_score", "score", default=0.0)
        of = _clamp(sign * tick, -1.0, 1.0)
        note = f"flow_tick={of:+.2f}"
    cvd = _clamp(sign * _num(flow, "cvd_trend", "cvd", default=0.0), -1.0, 1.0)
    return _clamp(0.56 * of + 0.44 * cvd, -1.0, 1.0), note + f" cvd={cvd:+.2f}"


def _structure_alignment(ict: Any, side: str, action: str) -> Tuple[float, str]:
    side = str(side or "").lower()
    action = str(action or "").lower()
    s5 = str(_obj(ict, "structure_5m", default="") or "").lower()
    s15 = str(_obj(ict, "structure_15m", default="") or "").lower()
    s4h = str(_obj(ict, "structure_4h", default="") or "").lower()
    choch = str(_obj(ict, "choch_5m", default="") or "").lower()
    bos = str(_obj(ict, "bos_5m", default="") or "").lower()
    desired = "bull" if side == "long" else "bear"
    contra = "bear" if side == "long" else "bull"

    htf = 0.0
    for s, w in ((s15, 0.42), (s4h, 0.58)):
        if desired in s:
            htf += w
        elif contra in s:
            htf -= w
        elif "rang" in s:
            htf += 0.0
    ltf = 0.0
    if desired in s5:
        ltf += 0.25
    elif contra in s5:
        ltf -= 0.25
    if action == "reverse":
        if desired in choch:
            ltf += 0.50
        if desired in bos:
            ltf += 0.18
    else:
        if desired in bos:
            ltf += 0.46
        if desired in choch:
            ltf += 0.16
    score = _clamp(0.62 * htf + 0.38 * ltf, -1.0, 1.0)
    return score, f"struct htf={htf:+.2f} ltf={ltf:+.2f} 15m={s15 or '?'} 4h={s4h or '?'}"


def _pd_affinity(ict: Any, side: str, action: str) -> Tuple[float, str]:
    pd = _clamp(_num(ict, "dealing_range_pd", default=0.5), 0.0, 1.0)
    if side == "long":
        # Longs are stronger from discount; continuations can tolerate equilibrium.
        aff = (0.58 - pd) / (0.34 if action == "reverse" else 0.42)
    else:
        aff = (pd - 0.42) / (0.34 if action == "reverse" else 0.42)
    return _clamp(aff, -1.0, 1.0), f"PD={pd:.2f}"


def _pool_score(target: Any, *, side: str, entry: float, atr: float) -> Tuple[float, float, List[str]]:
    px = _price(target)
    dist_atr = abs(px - entry) / max(atr, _EPS)
    sig = _sig(target)
    tf = _tf_rank(target)
    # Too close is likely fee/noise; too far is a lower-hit-rate fantasy unless it is HTF/strong.
    reach = math.exp(-0.055 * max(0.0, dist_atr - 4.0))
    if dist_atr < 0.45:
        reach *= _clamp(dist_atr / 0.45, 0.08, 1.0)
    if dist_atr > 9.0 and sig < 10.0:
        reach *= 0.50
    quality = _clamp(math.log1p(sig) / math.log(25.0), 0.0, 1.0)
    score = max(0.0, reach * (0.40 + 0.60 * quality) * tf)
    notes = [f"px={px:.2f}", f"dist={dist_atr:.2f}ATR", f"sig={sig:.1f}", f"tf×{tf:.2f}"]
    return score, dist_atr, notes


@dataclass
class DOLAssessment:
    side: str
    action: str
    direction: str = "NEUTRAL"
    grade: str = "F"
    confidence: float = 0.0
    score: float = 0.0
    clarity: float = 0.0
    target_distance_atr: float = 0.0
    target_price: float = 0.0
    first_target_probability: float = 0.0
    rr: float = 0.0
    net_expectancy_r: float = 0.0
    htf_alignment: float = 0.0
    flow_alignment: float = 0.0
    pd_affinity: float = 0.0
    structure_alignment: float = 0.0
    target_quality: float = 0.0
    protective_pressure: float = 0.0
    accepted: bool = False
    reason: str = ""
    notes: List[str] = field(default_factory=list)

    def compact(self) -> str:
        tgt = f"${self.target_price:,.2f}" if self.target_price else "none"
        return (
            f"DOL={self.direction} grade={self.grade} conf={self.confidence:.2f} "
            f"clarity={self.clarity:.2f} target={tgt} dist={self.target_distance_atr:.2f}ATR "
            f"p1={self.first_target_probability:.2f} rr={self.rr:.2f} EV={self.net_expectancy_r:+.2f} "
            f"struct={self.structure_alignment:+.2f} flow={self.flow_alignment:+.2f} PD={self.pd_affinity:+.2f}"
        )

    def as_dict(self) -> Dict[str, Any]:
        return dict(self.__dict__)


def assess_trade_thesis(
    *,
    snap: Any,
    side: str,
    entry: float,
    atr: float,
    ict: Any = None,
    flow: Any = None,
    action: str = "",
    sl: Optional[float] = None,
    tp: Optional[float] = None,
    posterior: float = 0.0,
    quality_score: float = 0.0,
) -> DOLAssessment:
    """Score whether a proposed trade has a clean draw-on-liquidity.

    The output is designed for two use cases:
    1. pre-posterior / post-posterior preflight, where ``sl``/``tp`` are not yet known;
    2. final executable-signal validation after SL and selected final TP are known.
    """
    side = str(side or "").lower()
    action = str(action or "").lower() or "unknown"
    entry = float(entry or 0.0)
    atr = max(float(atr or 0.0), _EPS)
    out = DOLAssessment(side=side, action=action)
    if side not in {"long", "short"} or entry <= 0.0:
        out.reason = "invalid side/entry"
        return out

    tp_pools = [p for p in _target_pools(snap, side) if _is_live(p) and _in_profit_direction(side, entry, _price(p))]
    stop_pools = [p for p in _protective_pools(snap, side) if _is_live(p)]
    if not tp_pools:
        out.reason = "no live TP-side liquidity draw"
        return out

    scored: List[Tuple[float, float, Any, List[str]]] = []
    for p in tp_pools:
        s, d, n = _pool_score(p, side=side, entry=entry, atr=atr)
        if d < 0.25:
            continue
        scored.append((s, d, p, n))
    if not scored:
        out.reason = "TP-side liquidity is too close/noisy after cost buffer"
        return out
    scored.sort(key=lambda x: x[0], reverse=True)
    best_score, best_dist, best_target, best_notes = scored[0]
    second = scored[1][0] if len(scored) > 1 else 0.0

    # Opposite-side pressure quantifies how likely the proposed SL side is still being engineered.
    opp_score = 0.0
    for p in stop_pools:
        px = _price(p)
        if px <= 0:
            continue
        dist = abs(px - entry) / atr
        if dist <= 8.0:
            sig = _sig(p)
            opp_score += (0.40 + 0.60 * _clamp(math.log1p(sig) / math.log(25.0), 0.0, 1.0)) / (1.0 + 0.30 * dist)
    out.protective_pressure = _clamp(opp_score / 4.0, 0.0, 1.0)

    target_quality = _clamp(best_score, 0.0, 1.0)
    clarity = _clamp(best_score / max(best_score + 0.70 * second + 0.80 * opp_score, _EPS), 0.0, 1.0)
    struct, struct_note = _structure_alignment(ict, side, action)
    flow_align, flow_note = _flow_alignment(flow, side)
    pd_aff, pd_note = _pd_affinity(ict, side, action)

    # Delivery probability is conservative: DOL clarity must combine with structural/flow proof.
    delivery_signal = _clamp(
        0.34 * clarity
        + 0.22 * target_quality
        + 0.18 * max(0.0, struct)
        + 0.15 * max(0.0, flow_align)
        + 0.08 * max(0.0, pd_aff)
        + 0.06 * _clamp(float(posterior or 0.0), 0.0, 0.95)
        + 0.05 * _clamp(float(quality_score or 0.0), 0.0, 1.0),
        0.0,
        0.95,
    )
    if struct < -0.35:
        delivery_signal *= 0.78
    if flow_align < -0.45:
        delivery_signal *= 0.76
    if pd_aff < -0.45 and action == "reverse":
        delivery_signal *= 0.82
    delivery_signal = _clamp(delivery_signal, 0.0, 0.95)

    target_px = float(tp) if tp and _in_profit_direction(side, entry, float(tp)) else _price(best_target)
    target_dist_atr = abs(target_px - entry) / atr
    out.direction = "UP" if side == "long" else "DOWN"
    out.target_price = target_px
    out.target_distance_atr = target_dist_atr
    out.target_quality = target_quality
    out.clarity = clarity
    out.structure_alignment = struct
    out.htf_alignment = struct
    out.flow_alignment = flow_align
    out.pd_affinity = pd_aff
    out.first_target_probability = delivery_signal

    risk = abs(entry - float(sl or 0.0)) if sl else 0.0
    if risk > _EPS and target_px > 0:
        out.rr = abs(target_px - entry) / risk
        cost_r = 0.04 + 0.05 * (1.0 - target_quality) + 0.04 * max(0.0, -flow_align)
        loss_burden = 1.0 + cost_r + 0.22 * out.protective_pressure
        out.net_expectancy_r = delivery_signal * max(0.0, out.rr - cost_r) - (1.0 - delivery_signal) * loss_burden
    else:
        # Preflight uses a target-room proxy; final acceptance is stricter once SL exists.
        room_proxy = _clamp((target_dist_atr - 0.35) / 3.0, 0.0, 1.0)
        out.rr = 0.0
        out.net_expectancy_r = delivery_signal * room_proxy - (1.0 - delivery_signal) * (0.20 + 0.35 * out.protective_pressure)

    # Dynamic grade.  Continuations require cleaner proof than reversals because continuation after a raid is a common trap.
    proof = _clamp(
        0.28 * clarity
        + 0.22 * delivery_signal
        + 0.20 * max(0.0, struct)
        + 0.15 * max(0.0, flow_align)
        + 0.08 * max(0.0, pd_aff)
        + 0.07 * target_quality
        - 0.12 * out.protective_pressure,
        0.0,
        1.0,
    )
    out.score = proof
    out.confidence = _clamp(0.50 * proof + 0.50 * delivery_signal, 0.0, 1.0)
    if out.confidence >= 0.74 and out.net_expectancy_r > 0.25:
        out.grade = "A"
    elif out.confidence >= 0.64 and out.net_expectancy_r > 0.05:
        out.grade = "B"
    elif out.confidence >= 0.54:
        out.grade = "C"
    else:
        out.grade = "D"

    min_conf = 0.62 if action == "reverse" else 0.68
    min_p1 = 0.55 if action == "reverse" else 0.60
    min_ev = 0.00 if sl is None else 0.03
    if action == "continue":
        # Continuation is allowed only when structure or flow proves acceptance toward the draw.
        if struct < 0.10 and flow_align < 0.25:
            min_conf += 0.06
            min_p1 += 0.04
    if float(posterior or 0.0) < 0.60:
        min_conf += 0.03
    if sl is not None and out.rr > 0:
        # High-win-rate mode: final TP can be modest, but it must have positive expectancy.
        if out.rr < 0.95:
            min_conf += 0.06
            min_ev += 0.08

    accepted = (
        out.confidence >= min_conf
        and delivery_signal >= min_p1
        and out.net_expectancy_r >= min_ev
        and target_dist_atr >= 0.45
        and clarity >= 0.34
    )
    if target_dist_atr < 0.55 and delivery_signal < 0.72:
        accepted = False
        out.reason = "DOL target too close for reliable first-target hit after noise/cost"
    elif sl is not None and out.rr < 0.80:
        accepted = False
        out.reason = f"DOL payoff too thin after structural SL: rr={out.rr:.2f}"
    elif not accepted:
        out.reason = (
            f"DOL thesis below live threshold: conf={out.confidence:.2f}/{min_conf:.2f} "
            f"p1={delivery_signal:.2f}/{min_p1:.2f} EV={out.net_expectancy_r:+.2f}/{min_ev:+.2f} "
            f"clarity={clarity:.2f}"
        )
    else:
        out.reason = "DOL thesis accepted"
    out.accepted = accepted
    out.notes = best_notes + [struct_note, flow_note, pd_note, f"protective_pressure={out.protective_pressure:.2f}"]
    return out

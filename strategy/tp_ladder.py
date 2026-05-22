"""
tp_ladder.py — Dynamic liquidity target ladder
================================================
Builds TP1..TPn from internal liquidity between entry and final TP.

Design constraints:
- Final TP remains the strategy-selected final liquidity objective.
- Internal BSL/SSL pools become partial reduce-only targets.
- If no validated internal liquidity exists, the selected final liquidity TP
  remains the only executable target; synthetic path projections are prohibited.
- SL price is never moved by this module.
- Quantity allocation is dynamic: path distance, delivery probability,
  liquidity quality, gauntlet and execution cost decide how much size is monetised
  before the final target.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import math
from typing import Any, Dict, Iterable, List, Optional


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(x)))


def _num(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


@dataclass
class TPLadderLeg:
    index: int
    role: str
    price: float
    qty_fraction: float
    quantity: float = 0.0
    source: str = ""
    pool_timeframe: str = ""
    pool_price: float = 0.0
    distance_atr: float = 0.0
    rr: float = 0.0
    delivery_prob: float = 0.0
    ev: float = 0.0
    gauntlet_n: int = 0
    gross_rr: float = 0.0
    cost_r: float = 0.0
    net_rr: float = 0.0
    reward_floor_r: float = 0.0
    edge_ratio: float = 0.0
    reason: str = ""
    order_id: str = ""
    placed: bool = False

    def as_dict(self) -> Dict[str, Any]:
        return dict(self.__dict__)


@dataclass
class TPLadderPlan:
    side: str
    entry: float
    sl: float
    final_tp: float
    atr: float
    total_quantity: float
    legs: List[TPLadderLeg] = field(default_factory=list)
    internal_budget: float = 0.0
    final_fraction: float = 1.0
    solvency_checkpoint_index: int = 0
    solvency_floor_r: float = 0.0
    worst_case_after_checkpoint_r: float = 0.0
    residual_domination: float = 0.0
    expected_internal_profit_r: float = 0.0
    final_runner_model: Dict[str, float] = field(default_factory=dict)
    regime_notes: List[str] = field(default_factory=list)

    @property
    def enabled(self) -> bool:
        return bool(self.legs)

    @property
    def has_internal_targets(self) -> bool:
        return len(self.legs) > 1

    def as_dict(self) -> Dict[str, Any]:
        return {
            "side": self.side,
            "entry": self.entry,
            "sl": self.sl,
            "final_tp": self.final_tp,
            "atr": self.atr,
            "total_quantity": self.total_quantity,
            "internal_budget": self.internal_budget,
            "final_fraction": self.final_fraction,
            "solvency_checkpoint_index": self.solvency_checkpoint_index,
            "solvency_floor_r": self.solvency_floor_r,
            "worst_case_after_checkpoint_r": self.worst_case_after_checkpoint_r,
            "residual_domination": self.residual_domination,
            "expected_internal_profit_r": self.expected_internal_profit_r,
            "final_runner_model": dict(self.final_runner_model),
            "regime_notes": list(self.regime_notes),
            "legs": [l.as_dict() for l in self.legs],
        }

    def compact(self) -> str:
        if not self.legs:
            return "TP ladder unavailable"
        bits = []
        for leg in self.legs:
            bits.append(
                f"{leg.role}@{leg.price:.2f} {leg.qty_fraction:.0%}"
            )
        return " | ".join(bits)


def _in_path(side: str, entry: float, final_tp: float, px: float) -> bool:
    if side == "long":
        return entry < px <= final_tp
    return final_tp <= px < entry


def _path_fraction(side: str, entry: float, final_tp: float, px: float) -> float:
    den = abs(final_tp - entry)
    if den <= 1e-12:
        return 1.0
    return _clamp(abs(px - entry) / den, 0.0, 1.0)


def _correct_pool_side(side: str, pool_side: str) -> bool:
    ps = str(pool_side or "").upper()
    return (side == "long" and ps == "BSL") or (side == "short" and ps == "SSL")




def _candidate_weight(row: Dict[str, Any], path_frac: float) -> float:
    # Delivery probability and EV are useful, but either can be missing.
    dp = _num(row.get("delivery_prob", row.get("sweep_prob", 0.0)), 0.0)
    if dp <= 0:
        dp = _num(row.get("sweep_prob", 0.0), 0.0)
    dp = _clamp(dp, 0.05, 0.95)
    sig = max(0.0, _num(row.get("significance", 0.0), 0.0))
    ev = max(0.0, _num(row.get("selection_ev", row.get("ev", 0.0)), 0.0))
    ladder_score = _clamp(_num(row.get("ladder_path_score", 0.0), 0.0), 0.0, 1.25)
    qual = _clamp(_num(row.get("quality", 0.0), 0.0), 0.0, 1.0)
    confluence = _clamp(_num(row.get("confluence", 1.0), 1.0), 0.4, 2.5)
    gauntlet = max(0, int(_num(row.get("gauntlet_n", 0), 0)))
    gross_rr = max(0.0, _num(row.get("_ladder_gross_rr", row.get("rr", 0.0)), 0.0))
    net_rr = _num(row.get("_ladder_net_rr", gross_rr), gross_rr)
    edge_ratio = _num(row.get("_ladder_edge_ratio", 1.0), 1.0)
    if net_rr <= 0.0:
        edge_mult = 0.08
    else:
        net_quality = _clamp(net_rr / max(gross_rr, 1e-9), 0.05, 1.0)
        edge_mult = _clamp(0.42 + 0.58 * net_quality, 0.10, 1.0)
        edge_mult *= _clamp(edge_ratio / 1.55, 0.30, 1.28)
    # Effective liquidity score: do not blindly overweight the nearest pool.
    # Institutional monetisation should prefer pools where liquidity quality,
    # delivery and multi-timeframe confluence are strong enough to pay costs,
    # while very-near pools are penalised for spread/noise crowding.
    path_shape = _clamp(0.62 + 0.78 * math.sqrt(max(path_frac, 0.0)) - 0.36 * max(0.0, path_frac - 0.72), 0.35, 1.25)
    too_near_pen = _clamp((path_frac / 0.14) ** 0.70, 0.22, 1.0)
    gauntlet_pen = 1.0 / (1.0 + 0.22 * gauntlet)
    return max(
        1e-6,
        dp * (0.65 + 0.18 * math.sqrt(sig + 1.0)) * (0.75 + qual)
        * (0.75 + 0.25 * confluence)
        * path_shape * too_near_pen
        * gauntlet_pen * (1.0 + 0.10 * min(ev, 3.0)) * (1.0 + 0.18 * ladder_score)
        * edge_mult,

    )


def _tf_rank(tf: str) -> float:
    tf = str(tf or "").lower()
    return {
        "1m": 1.0, "2m": 1.05, "3m": 1.08, "5m": 1.15,
        "15m": 1.32, "30m": 1.42, "1h": 1.62,
        "4h": 1.90, "1d": 2.15,
    }.get(tf, 1.0)


def _cluster_effective_liquidity(rows: List[Dict[str, Any]], *, side: str, entry: float,
                                 final_tp: float, min_spacing: float) -> List[Dict[str, Any]]:
    """Merge same-zone pools across timeframes into one effective TP level.

    The input rows are already valid same-side pools inside the entry→final path.
    Clustering prevents TP1/TP2 from being two prices inside the same liquidity
    pocket, while preserving multi-timeframe information as aggregate weight.
    """
    if not rows:
        return []
    rows = sorted(rows, key=lambda r: abs(_num(r.get("_ladder_price"), 0.0) - entry))
    clusters: List[List[Dict[str, Any]]] = []
    radius = max(float(min_spacing or 0.0), 1e-9)
    for r in rows:
        px = _num(r.get("_ladder_price"), 0.0)
        if px <= 0:
            continue
        placed = False
        for c in clusters:
            cpx = sum(_num(x.get("_ladder_price"), 0.0) for x in c) / max(len(c), 1)
            if abs(px - cpx) <= radius:
                c.append(r); placed = True; break
        if not placed:
            clusters.append([r])

    out: List[Dict[str, Any]] = []
    for cluster in clusters:
        weights = []
        for r in cluster:
            tf = str(r.get("timeframe", "") or "")
            pf = _clamp(_num(r.get("_ladder_path_frac"), 0.0), 0.0, 1.0)
            w = max(1e-6, _num(r.get("_ladder_weight"), 1e-6))
            w *= _tf_rank(tf)
            # A pool confirmed by a higher timeframe should influence the
            # effective level, but not turn the first internal TP into an
            # all-in exit. Path fraction stays part of allocation later.
            w *= 0.80 + 0.35 * math.sqrt(max(pf, 0.0))
            weights.append(w)
        wsum = sum(weights) or 1.0
        price = sum(_num(r.get("_ladder_price"), 0.0) * w for r, w in zip(cluster, weights)) / wsum
        path_frac = _path_fraction(side, entry, final_tp, price)
        base = max(cluster, key=lambda r: _num(r.get("_ladder_weight"), 0.0))
        c = dict(base)
        tfs = sorted({str(r.get("timeframe", "") or "?") for r in cluster}, key=_tf_rank)
        c["_ladder_price"] = price
        c["tp_price"] = price
        c["pool_price"] = price
        c["_ladder_path_frac"] = path_frac
        c["_ladder_source"] = "mtf_liquidity_cluster"
        c["timeframe"] = "+".join(tfs)
        c["cluster_size"] = len(cluster)
        c["cluster_timeframes"] = "+".join(tfs)
        c["quality"] = _clamp(sum(_num(r.get("quality"), 0.0) * w for r, w in zip(cluster, weights)) / wsum, 0.0, 1.0)
        c["significance"] = max(_num(r.get("significance", 0.0), 0.0) for r in cluster) + math.log1p(len(cluster))
        c["delivery_prob"] = _clamp(sum(_num(r.get("delivery_prob", r.get("sweep_prob", 0.0)), 0.0) * w for r, w in zip(cluster, weights)) / wsum, 0.01, 0.97)
        c["selection_ev"] = max(_num(r.get("selection_ev", r.get("ev", 0.0)), 0.0) for r in cluster)
        c["reason"] = f"MTF effective liquidity cluster: {len(cluster)} pools across {c['cluster_timeframes']}"
        c["_ladder_weight"] = _candidate_weight(c, path_frac) * (1.0 + 0.10 * math.log1p(len(cluster)))
        out.append(c)
    out.sort(key=lambda x: abs(_num(x.get("_ladder_price"), 0.0) - entry))
    return out


def _cost_points_from_bps(entry: float, roundtrip_cost_bps: float) -> float:
    try:
        bps = max(0.0, float(roundtrip_cost_bps or 0.0))
        px = max(0.0, float(entry or 0.0))
        return px * bps / 10_000.0
    except Exception:
        return 0.0


def _row_cost_points(row: Dict[str, Any], *, risk_points: float, rt_cost_points: float,
                     fallback_cost_r: float = 0.0) -> float:
    """Best available per-unit friction estimate for an internal TP.

    Cost is measured in price points, not dollars, so the same rule works for
    BTC, gold, silver, and tokenised equities before exchange contract sizing.
    The live execution-cost engine supplies ``rt_cost_points``.  Liquidity rows
    may also carry ``cost_r`` from the selector; use the larger estimate so a
    stale low fee snapshot cannot make a dust TP look tradable.
    """
    risk = max(float(risk_points or 0.0), 1e-9)
    row_cost_r = _num(row.get("cost_r", 0.0), 0.0) if isinstance(row, dict) else 0.0
    return max(
        0.0,
        float(rt_cost_points or 0.0),
        max(0.0, row_cost_r) * risk,
        max(0.0, float(fallback_cost_r or 0.0)) * risk,
    )


def _prop_desk_internal_reward_floor(row: Dict[str, Any], *, risk_points: float,
                                     rt_cost_points: float, base_fee_floor_mult: float,
                                     fallback_cost_r: float = 0.0, atr: float = 0.0) -> tuple[float, float]:
    """Return (required_reward_points, effective_cost_points).

    Internal TPs are not targets just because a pool exists. A prop desk only
    monetises the path if the partial exit clears fees/slippage with enough net
    edge to justify creating another exchange order and reducing final-runner
    optionality.
    """
    cost_pts = _row_cost_points(
        row,
        risk_points=risk_points,
        rt_cost_points=rt_cost_points,
        fallback_cost_r=fallback_cost_r,
    )
    if cost_pts <= 0.0:
        return 0.0, 0.0
    dp = _clamp(_num(row.get("delivery_prob", row.get("sweep_prob", 0.55)), 0.55), 0.05, 0.95)
    qual = _clamp(_num(row.get("quality", 0.55), 0.55), 0.0, 1.0)
    path = _clamp(_num(row.get("_ladder_path_frac", 0.0), 0.0), 0.0, 1.0)
    risk = max(float(risk_points or 0.0), 1e-9)
    atr_f = max(float(atr or 0.0), 0.0)
    src = str(row.get("_ladder_source", row.get("source", "")) or "").lower()

    # The minimum multiple is fee-engine derived.  It increases only when the
    # candidate itself is weaker: near-entry/noisy, low delivery probability,
    # or low liquidity quality.
    mult = max(1.0, float(base_fee_floor_mult or 1.0))
    mult *= 1.0 + 0.22 * (1.0 - dp) + 0.16 * (1.0 - qual) + 0.18 * max(0.0, 0.35 - path)
    mult = _clamp(mult, 1.05, 2.15)
    # Fees are only one part of the required move. A very near TP also has to
    # clear auction noise; otherwise the partial fill can be positive per unit
    # while still being immaterial to the lifecycle. Noise is scaled by ATR,
    # liquidity quality, delivery probability, and path depth.
    noise_pts = 0.0
    if atr_f > 0.0:
        noise_mult = (
            0.05
            + 0.10 * (1.0 - dp)
            + 0.08 * (1.0 - qual)
            + 0.08 * max(0.0, 0.32 - path)
        )
        noise_pts = atr_f * _clamp(noise_mult, 0.04, 0.30)
    required = max(cost_pts * mult, cost_pts + noise_pts, 0.18 * risk)
    return required, cost_pts


def _annotate_internal_level_economics(rows: List[Dict[str, Any]], *, side: str, entry: float,
                                       risk_points: float, rt_cost_points: float,
                                       base_fee_floor_mult: float, fallback_cost_r: float = 0.0,
                                       atr: float = 0.0) -> List[Dict[str, Any]]:
    annotated: List[Dict[str, Any]] = []
    risk = max(float(risk_points or 0.0), 1e-9)
    for r in rows or []:
        c = dict(r)
        px = _num(c.get("_ladder_price", c.get("tp_price", c.get("pool_price", 0.0))), 0.0)
        reward_pts = abs(px - entry)
        path = _path_fraction(side, entry, _num(c.get("_ladder_final_tp", c.get("final_tp", px)), px), px)
        if "_ladder_path_frac" not in c:
            c["_ladder_path_frac"] = path
        req_pts, cost_pts = _prop_desk_internal_reward_floor(
            c,
            risk_points=risk,
            rt_cost_points=rt_cost_points,
            base_fee_floor_mult=base_fee_floor_mult,
            fallback_cost_r=fallback_cost_r,
            atr=atr,
        )
        gross_rr = reward_pts / risk
        cost_r = cost_pts / risk
        net_rr = (reward_pts - cost_pts) / risk
        floor_r = req_pts / risk if req_pts > 0.0 else 0.0
        denom = max(req_pts, cost_pts, 1e-9)
        edge_ratio = reward_pts / denom if denom > 0.0 else 1.0
        c["_ladder_gross_rr"] = gross_rr
        c["_ladder_cost_r"] = cost_r
        c["_ladder_net_rr"] = net_rr
        c["_ladder_reward_floor_r"] = floor_r
        c["_ladder_edge_ratio"] = edge_ratio
        c["_ladder_reward_floor_pts"] = req_pts
        c["_ladder_cost_pts"] = cost_pts
        c["_ladder_weight"] = _candidate_weight(c, _clamp(_num(c.get("_ladder_path_frac", 0.0), 0.0), 0.0, 1.0))
        annotated.append(c)
    return annotated


def _filter_internal_levels_by_net_edge(rows: List[Dict[str, Any]], *, side: str, entry: float,
                                        risk_points: float, rt_cost_points: float,
                                        base_fee_floor_mult: float, fallback_cost_r: float = 0.0,
                                        atr: float = 0.0) -> tuple[List[Dict[str, Any]], List[str]]:
    if not rows:
        return rows, []
    rows = _annotate_internal_level_economics(
        rows,
        side=side,
        entry=entry,
        risk_points=risk_points,
        rt_cost_points=rt_cost_points,
        base_fee_floor_mult=base_fee_floor_mult,
        fallback_cost_r=fallback_cost_r,
        atr=atr,
    )
    kept: List[Dict[str, Any]] = []
    notes: List[str] = []
    dropped = 0
    nearest_note = ""
    for r in rows:
        px = _num(r.get("_ladder_price", r.get("tp_price", r.get("pool_price", 0.0))), 0.0)
        reward_pts = abs(px - entry)
        req_pts, cost_pts = _prop_desk_internal_reward_floor(
            r, risk_points=risk_points, rt_cost_points=rt_cost_points,
            base_fee_floor_mult=base_fee_floor_mult,
            fallback_cost_r=fallback_cost_r,
            atr=atr,
        )
        net_rr = _num(r.get("_ladder_net_rr", 0.0), 0.0)
        if (req_pts > 0.0 and reward_pts <= req_pts) or net_rr <= 0.0:
            dropped += 1
            if not nearest_note:
                nearest_note = (
                    f"first skipped level reward={reward_pts:.2f}pts does not clear "
                    f"friction floor={req_pts:.2f}pts (cost~{cost_pts:.2f}pts)"
                )
            continue
        kept.append(r)
    if dropped:
        notes.append(
            f"prop-desk net-edge filter: {dropped} internal TP level(s) removed; "
            + (nearest_note or "net reward did not clear fees/slippage")
        )
    return kept, notes

def _sigmoid(x: float) -> float:
    try:
        return 1.0 / (1.0 + math.exp(-float(x)))
    except Exception:
        return 0.5


def _estimate_cost_floor_r(internals: List[Dict[str, Any]], selected_rows: List[Dict[str, Any]]) -> float:
    """Cost/uncertainty floor in full-trade R units, from selector diagnostics."""
    vals = []
    for r in list(internals or []) + list(selected_rows or []):
        v = _num(r.get("cost_r", 0.0), 0.0)
        if v > 0 and math.isfinite(v):
            vals.append(_clamp(v, 0.0, 1.25))
    if not vals:
        return 0.0
    vals.sort()
    med = vals[len(vals) // 2]
    tail = vals[int(0.75 * (len(vals) - 1))] if len(vals) > 1 else med
    return _clamp(0.65 * med + 0.35 * tail, 0.0, 0.85)


def _runner_fraction_model(*, final_dist_atr: float, final_rr: float, structural_adjustment: float, internals: List[Dict[str, Any]]) -> Dict[str, float]:
    """Earn final-runner size only from validated liquidity-path information."""
    n = len(internals)
    if n:
        fracs = [_clamp(_num(c.get("_ladder_path_frac"), 0.0), 0.0, 1.0) for c in internals]
        weights = [max(1e-6, _num(c.get("_ladder_weight"), 1e-6)) for c in internals]
        gaps = [fracs[0]] + [fracs[i] - fracs[i - 1] for i in range(1, len(fracs))] + [1.0 - fracs[-1]]
        max_gap = max(gaps) if gaps else 1.0
        coverage = _clamp(1.0 - max_gap, 0.0, 1.0)
        density = 1.0 - math.exp(-n / 2.35)
        quality = _clamp(sum(weights) / max(n, 1), 0.0, 1.75) / 1.75
        early_quality = _clamp(sum(w * (1.0 - f) for w, f in zip(weights, fracs)) / max(sum(weights), 1e-9), 0.0, 1.0)
    else:
        coverage = density = quality = early_quality = 0.0
        max_gap = 1.0
    distance_penalty = _sigmoid((float(final_dist_atr) - 4.0) / 3.2)
    rr_saturation = 1.0 - math.exp(-max(0.0, float(final_rr)) / 3.0)
    path_strength = _clamp(0.34 * coverage + 0.27 * density + 0.24 * quality + 0.15 * early_quality, 0.0, 1.0)
    sponsor = _clamp(float(structural_adjustment or 0.0), -1.0, 1.0)
    raw = 0.08 + 0.45 * path_strength + 0.12 * rr_saturation - 0.25 * distance_penalty + 0.14 * max(0.0, sponsor) - 0.12 * max(0.0, -sponsor)
    terminal_information = _clamp(
        0.48 * path_strength
        + 0.24 * rr_saturation
        + 0.16 * (1.0 - distance_penalty)
        + 0.12 * max(0.0, sponsor)
        - 0.08 * max(0.0, -sponsor),
        0.0, 1.0,
    )
    min_runner = _clamp(0.06 + 0.28 * terminal_information, 0.055, 0.38)
    max_runner = _clamp(0.30 + 0.32 * path_strength + 0.10 * max(0.0, sponsor), 0.24, 0.80)
    final_fraction = _clamp(raw, min_runner, max_runner)
    return {
        "final_fraction": final_fraction,
        "terminal_information": terminal_information,
        "path_strength": path_strength,
        "coverage": coverage,
        "density": density,
        "quality": quality,
        "early_quality": early_quality,
        "max_gap": max_gap,
        "distance_penalty": distance_penalty,
        "rr_saturation": rr_saturation,
        "structural_adjustment": sponsor,
        "min_runner": min_runner,
        "max_runner": max_runner,
    }


def _first_tp_balance_limits(row: Dict[str, Any], reward_r: float, final_rr: float,
                             min_leg_fraction: float, floor_r: float) -> tuple[float, float, float]:
    """Return (material_profit_r, max_fraction, required_fraction).

    The first TP should be a real monetisation event, not a dust scalp, but it
    also must not become an oversized early exit.  This function measures the
    first TP in full-position R contribution:

        realised_R_if_TP1_fills = qty_fraction * TP1_reward_R

    If that contribution cannot become material without exceeding the earned
    cap, the caller should demote the noisy/too-near first level and let the
    next deeper validated liquidity zone become TP1.
    """
    rr = max(0.0, float(reward_r or 0.0))
    path_frac = _clamp(_num(row.get("_ladder_path_frac", 0.0), 0.0), 0.0, 1.0)
    dp = _clamp(_num(row.get("delivery_prob", row.get("sweep_prob", 0.55)), 0.55), 0.05, 0.95)
    qual = _clamp(_num(row.get("quality", 0.55), 0.55), 0.0, 1.0)
    conf = _clamp(_num(row.get("confluence", 1.0), 1.0), 0.4, 2.5)
    source = str(row.get("_ladder_source", row.get("source", "")) or "").lower()

    # Materiality is measured in full-trade R, not percentage size.  It rises
    # modestly with trade opportunity and execution uncertainty/cost, but stays
    # bounded so a single early level cannot demand a huge allocation.
    terminal_scale = 1.0 - math.exp(-max(0.0, float(final_rr or 0.0)) / 3.0)
    material_r = _clamp(
        0.055
        + 0.040 * terminal_scale
        + 0.030 * _clamp(float(floor_r or 0.0), 0.0, 0.35)
        + 0.020 * (1.0 - dp),
        0.060,
        0.135,
    )

    # Earned TP1 cap: nearer/noisier first levels get capped harder; stronger
    # delivery/liquidity quality earns more but never enough to dominate the
    # trade.
    rr_credit = _clamp(rr / max(1.20, 0.45 * max(float(final_rr or 0.0), 1e-9)), 0.0, 1.0)
    path_credit = math.sqrt(max(path_frac, 0.0))
    max_fraction = _clamp(
        0.070
        + 0.070 * path_credit
        + 0.055 * dp
        + 0.040 * qual
        + 0.025 * min(conf, 1.6) / 1.6
        + 0.055 * rr_credit,
        max(float(min_leg_fraction or 0.0), 0.025),
        0.285,
    )
    if path_frac < 0.16 and rr < 0.80:
        max_fraction *= _clamp(0.72 + 1.10 * path_frac + 0.10 * rr, 0.62, 0.92)
    max_fraction = _clamp(max_fraction, max(float(min_leg_fraction or 0.0), 0.015), 0.285)
    required_fraction = material_r / max(rr, 1e-9)
    return material_r, max_fraction, required_fraction


def _redistribute_tp1_excess(fracs: List[float], start_idx: int, excess: float,
                             internals: List[Dict[str, Any]], rewards_r: List[float],
                             final_fraction: float) -> tuple[List[float], float]:
    """Move early-exit excess to deeper path levels first, then final runner."""
    if excess <= 1e-12:
        return fracs, final_fraction
    tail = list(range(max(0, start_idx), len(fracs)))
    weights: List[float] = []
    for j in tail:
        row = internals[j]
        path = _clamp(_num(row.get("_ladder_path_frac", 0.0), 0.0), 0.0, 1.0)
        w = max(1e-6, _num(row.get("_ladder_weight", 1e-6), 1e-6))
        w *= (1.0 + path) * (1.0 + 0.18 * min(max(0.0, rewards_r[j]), 4.0))
        weights.append(w)
    wsum = sum(weights)
    if tail and wsum > 0:
        for off, j in enumerate(tail):
            fracs[j] += excess * weights[off] / wsum
    else:
        final_fraction += excess
    return fracs, final_fraction


def _balance_first_tp_materiality(*, internals: List[Dict[str, Any]], fracs: List[float],
                                  final_fraction: float, rewards_r: List[float],
                                  min_leg_fraction: float, floor_r: float,
                                  min_final_fraction: float, final_rr: float) -> tuple[List[Dict[str, Any]], List[float], float, List[float], List[str]]:
    """Keep TP1 economically meaningful without over-selling the first level.

    If the nearest TP cannot produce a material booked R without requiring too
    much quantity, it is not a good TP1; it is demoted and its size is pushed to
    deeper validated liquidity zones.  If the first level can be meaningful inside its
    earned cap, it may receive a small transfer from later legs/final runner, but
    never by violating the terminal-runner reserve.
    """
    notes: List[str] = []
    internals = list(internals or [])
    fracs = list(fracs or [])
    rewards_r = list(rewards_r or [])
    if not internals or not fracs or not rewards_r:
        return internals, fracs, final_fraction, rewards_r, notes

    # Strip too-near/noisy first levels that would need excessive early size to
    # become meaningful.  This converts the next deeper level into TP1 instead
    # of forcing the bot to sell many lots at a shallow liquidity pocket.
    for _ in range(min(3, len(internals))):
        if len(internals) <= 1:
            break
        material_r, cap, required = _first_tp_balance_limits(
            internals[0], rewards_r[0], final_rr, min_leg_fraction, floor_r
        )
        realised_r = fracs[0] * max(rewards_r[0], 0.0)
        capped_realised_r = min(fracs[0], cap) * max(rewards_r[0], 0.0)
        cannot_be_material_without_oversize = required > cap and capped_realised_r < material_r * 0.92
        very_near = _clamp(_num(internals[0].get("_ladder_path_frac", 0.0), 0.0), 0.0, 1.0) < 0.18 and rewards_r[0] < 0.85
        if not (cannot_be_material_without_oversize and very_near):
            break
        dropped_frac = max(0.0, fracs.pop(0))
        dropped = internals.pop(0)
        dropped_rr = rewards_r.pop(0)
        fracs, final_fraction = _redistribute_tp1_excess(
            fracs, 0, dropped_frac, internals, rewards_r, final_fraction
        )
        notes.append(
            "near TP1 demoted: first level could not book material R without oversized early quantity "
            f"(rr={dropped_rr:.2f}, required={required:.0%}, cap={cap:.0%})"
        )

    if not internals or not fracs or not rewards_r:
        return internals, fracs, final_fraction, rewards_r, notes

    material_r, cap, required = _first_tp_balance_limits(
        internals[0], rewards_r[0], final_rr, min_leg_fraction, floor_r
    )
    if fracs[0] > cap:
        excess = fracs[0] - cap
        fracs[0] = cap
        fracs, final_fraction = _redistribute_tp1_excess(fracs, 1, excess, internals, rewards_r, final_fraction)
        notes.append(
            f"TP1 balanced: capped first exit at {cap:.0%}; excess pushed deeper/final"
        )

    realised_r = fracs[0] * max(rewards_r[0], 0.0)
    if realised_r < material_r and required <= cap:
        need = min(cap - fracs[0], required - fracs[0])
        if need > 1e-9:
            # Pull from later internals first.  Only borrow from final if it is
            # above the earned runner reserve.
            available_tail = sum(max(0.0, fracs[i] - max(min_leg_fraction * 0.50, 0.0)) for i in range(1, len(fracs)))
            take = min(need, available_tail)
            if take > 1e-12 and available_tail > 0:
                for i in range(1, len(fracs)):
                    avail = max(0.0, fracs[i] - max(min_leg_fraction * 0.50, 0.0))
                    cut = take * avail / available_tail
                    fracs[i] = max(0.0, fracs[i] - cut)
                fracs[0] += take
                need -= take
            final_surplus = max(0.0, final_fraction - min_final_fraction)
            take_final = min(need, final_surplus)
            if take_final > 1e-12:
                final_fraction -= take_final
                fracs[0] += take_final
            if fracs[0] * max(rewards_r[0], 0.0) >= material_r * 0.98:
                notes.append(
                    f"TP1 balanced: booked-R target {material_r:.2f}R reached with {fracs[0]:.0%} size"
                )

    # Final normalization while respecting the final-runner reserve as far as
    # possible.  Do not let balancing create >100% allocation.
    total = sum(fracs) + final_fraction
    if total > 1.0 + 1e-9:
        excess = total - 1.0
        tail_idx = list(range(1, len(fracs))) or [0]
        avail = sum(max(0.0, fracs[i] - min_leg_fraction) for i in tail_idx)
        if avail > 0:
            cut = min(excess, avail)
            for i in tail_idx:
                a = max(0.0, fracs[i] - min_leg_fraction)
                fracs[i] = max(0.0, fracs[i] - cut * a / avail)
            excess -= cut
        if excess > 1e-9:
            final_fraction = max(min_final_fraction, final_fraction - excess)
    elif total < 1.0 - 1e-9:
        final_fraction += 1.0 - total

    return internals, fracs, _clamp(final_fraction, min_final_fraction, 1.0), rewards_r, notes

def _weighted_allocate(cands: List[Dict[str, Any]], budget: float) -> List[float]:
    if not cands or budget <= 0:
        return [0.0 for _ in cands]
    weights = [max(1e-6, _num(c.get("_ladder_weight"), 1e-6)) for c in cands]
    wsum = sum(weights) or 1.0
    return [float(budget) * w / wsum for w in weights]


def _prefix_solvency_r(fracs: List[float], rewards_r: List[float], k: int,
                       residual_loss_r: float = 1.0) -> float:
    filled = sum(fracs[:k])
    profit = sum(fracs[i] * rewards_r[i] for i in range(min(k, len(fracs))))
    return profit - max(0.0, 1.0 - filled) * max(0.0, float(residual_loss_r or 1.0))


def _select_solvency_checkpoint(internals: List[Dict[str, Any]], rewards_r: List[float],
                                max_internal_budget: float, floor_r: float,
                                residual_loss_r: float = 1.0) -> int:
    """Earliest data-supported checkpoint where reversal to original SL can remain solvent."""
    if not internals:
        return 0
    residual_loss_r = max(0.0, float(residual_loss_r or 1.0))
    for k in range(1, len(internals) + 1):
        prefix = internals[:k]
        pweights = [max(1e-6, _num(c.get("_ladder_weight"), 1e-6)) for c in prefix]
        wsum = sum(pweights) or 1.0
        avg_r = sum((pweights[i] / wsum) * rewards_r[i] for i in range(k))
        required_frac = (residual_loss_r + floor_r) / max(residual_loss_r + avg_r, 1e-9)
        path_frac = _num(prefix[-1].get("_ladder_path_frac"), 0.0)
        proof_needed = _clamp(residual_loss_r / max(residual_loss_r + max(rewards_r), 1e-9), 0.08, 0.42)
        if required_frac <= max_internal_budget + 1e-9 and path_frac >= proof_needed:
            return k
    return len(internals)


def _enforce_lifecycle_solvency(*, internals: List[Dict[str, Any]], fracs: List[float],
                                final_fraction: float, rewards_r: List[float],
                                min_leg_fraction: float, floor_r: float,
                                min_final_fraction: float = 0.0,
                                residual_loss_r: float = 1.0) -> tuple[List[float], float, int, float, float]:
    """Front-load enough internal liquidity so TP checkpoint + original SL remains solvent."""
    if not internals:
        return fracs, final_fraction, 0, 0.0, 0.0
    residual_loss_r = max(0.0, float(residual_loss_r or 1.0))
    min_final_fraction = _clamp(float(min_final_fraction or 0.0), 0.0, 0.95)
    final_fraction = _clamp(final_fraction, min_final_fraction, 1.0)
    internal_budget = _clamp(1.0 - final_fraction, 0.0, 1.0)
    checkpoint = _select_solvency_checkpoint(
        internals,
        rewards_r,
        max_internal_budget=1.0 - _clamp(final_fraction * 0.25, 0.0, 0.95),
        floor_r=floor_r,
        residual_loss_r=residual_loss_r,
    )
    if checkpoint <= 0:
        return fracs, final_fraction, 0, 0.0, 0.0
    if not fracs or len(fracs) != len(internals):
        fracs = _weighted_allocate(internals, internal_budget)
    for _ in range(24):
        solv = _prefix_solvency_r(fracs, rewards_r, checkpoint, residual_loss_r=residual_loss_r)
        if solv >= floor_r - 1e-9:
            break
        prefix_weights = [(residual_loss_r + rewards_r[i]) * max(1e-6, _num(internals[i].get("_ladder_weight"), 1e-6)) for i in range(checkpoint)]
        psum = sum(prefix_weights) or 1.0
        avg_eff = sum((prefix_weights[i] / psum) * (residual_loss_r + rewards_r[i]) for i in range(checkpoint))
        need = max(0.0, (floor_r - solv) / max(avg_eff, 1e-9))
        move = min(need, max(0.0, final_fraction - min_final_fraction))
        if move > 1e-9:
            final_fraction -= move
        else:
            later = list(range(checkpoint, len(fracs)))
            available = sum(max(0.0, fracs[i] - min_leg_fraction * 0.35) for i in later)
            move = min(need, available)
            if move <= 1e-9:
                break
            for i in later:
                take = move * max(0.0, fracs[i] - min_leg_fraction * 0.35) / max(available, 1e-9)
                fracs[i] = max(0.0, fracs[i] - take)
        for i in range(checkpoint):
            fracs[i] += move * prefix_weights[i] / psum
    # Prevent one nearest zone from swallowing the ladder when multiple
    # effective path zones exist. The cap is not a fixed TP percentage: it is
    # derived from the number of zones, path proof, RR and structural_adjustment. Excess
    # is redistributed to later validated liquidity zones before it becomes runner.
    if len(fracs) > 1 and sum(fracs) > 0:
        sponsor = _clamp(_num(internals[0].get("_ladder_structural_adjustment", 0.0), 0.0), -1.0, 1.0)
        for i in range(len(fracs) - 1):
            path_i = _clamp(_num(internals[i].get("_ladder_path_frac"), 0.0), 0.0, 1.0)
            avg_remaining_depth = max(1, len(fracs) - i)
            rr_i = max(0.0, rewards_r[i] if i < len(rewards_r) else 0.0)
            earned_cap = _clamp(
                (1.0 / math.sqrt(avg_remaining_depth + 0.65))
                * (0.78 + 0.22 * math.sqrt(max(path_i, 0.02)))
                * (0.86 + 0.12 * min(rr_i, 2.5))
                * (1.0 + 0.10 * max(0.0, -sponsor)),
                min_leg_fraction,
                0.58,
            )
            if fracs[i] > earned_cap:
                excess = fracs[i] - earned_cap
                fracs[i] = earned_cap
                tail_weights = []
                for j in range(i + 1, len(fracs)):
                    tail_weights.append(max(1e-6, _num(internals[j].get("_ladder_weight", 1e-6), 1e-6)) * (1.0 + _num(internals[j].get("_ladder_path_frac", 0.0), 0.0)))
                tsum = sum(tail_weights)
                if tsum > 0:
                    for off, j in enumerate(range(i + 1, len(fracs))):
                        fracs[j] += excess * tail_weights[off] / tsum
                else:
                    final_fraction += excess

    for i, f in enumerate(list(fracs)):
        if 0 < f < min_leg_fraction and i >= checkpoint:
            final_fraction += f
            fracs[i] = 0.0
    total = sum(fracs) + final_fraction
    if total > 1.0 + 1e-9:
        scale = max(0.0, 1.0 - final_fraction) / max(sum(fracs), 1e-9)
        fracs = [f * scale for f in fracs]
    elif total < 1.0 - 1e-9:
        final_fraction += 1.0 - total
    if final_fraction < min_final_fraction:
        deficit = min_final_fraction - final_fraction
        available = sum(max(0.0, f) for f in fracs)
        if available > 1e-12:
            for i in range(len(fracs)):
                take = deficit * max(0.0, fracs[i]) / available
                fracs[i] = max(0.0, fracs[i] - take)
            final_fraction = min_final_fraction
    actual_checkpoint = 0
    worst = _prefix_solvency_r(fracs, rewards_r, checkpoint, residual_loss_r=residual_loss_r)
    for k in range(1, len(fracs) + 1):
        solv = _prefix_solvency_r(fracs, rewards_r, k, residual_loss_r=residual_loss_r)
        if solv >= floor_r - 1e-9:
            actual_checkpoint = k
            worst = solv
            break
    if actual_checkpoint <= 0:
        actual_checkpoint = len(fracs)
        worst = _prefix_solvency_r(fracs, rewards_r, actual_checkpoint, residual_loss_r=residual_loss_r)
    checkpoint = actual_checkpoint
    expected_internal = sum(
        fracs[i] * rewards_r[i] * _clamp(_num(internals[i].get("delivery_prob", internals[i].get("sweep_prob", 0.5)), 0.5), 0.05, 0.95)
        for i in range(len(fracs))
    )
    residual_domination = (_clamp(final_fraction, 0.0, 1.0) * residual_loss_r) / max(expected_internal, 1e-9)
    return fracs, _clamp(final_fraction, min_final_fraction, 1.0), checkpoint, worst, residual_domination


def build_tp_ladder(
    *,
    side: str,
    entry: float,
    sl: float,
    final_tp: float,
    atr: float,
    total_quantity: float,
    pool_report: Optional[Dict[str, Any]] = None,
    min_leg_fraction: float = 0.0,
    min_spacing_atr: float = 0.35,
    max_internal_legs: int = 0,
    roundtrip_cost_bps: float = 0.0,
    fee_floor_mult: float = 1.20,
) -> TPLadderPlan:
    side = str(side or "").lower()
    entry = float(entry or 0.0)
    sl = float(sl or 0.0)
    final_tp = float(final_tp or 0.0)
    atr = max(float(atr or 0.0), 1e-9)
    total_quantity = max(float(total_quantity or 0.0), 0.0)
    # min_leg_fraction is supplied by the strategy from exchange lot/min-qty
    # constraints.  It is not a fixed TP percentage.  If the caller cannot
    # provide it, keep it at zero and let the exchange placement layer reject
    # only true dust.
    min_leg_fraction = _clamp(float(min_leg_fraction or 0.0), 0.0, 0.95)
    max_internal_legs = max(0, int(max_internal_legs or 0))
    plan = TPLadderPlan(
        side=side, entry=entry, sl=sl, final_tp=final_tp, atr=atr,
        total_quantity=total_quantity,
    )
    if side not in ("long", "short") or entry <= 0 or final_tp <= 0 or total_quantity <= 0:
        plan.regime_notes.append("invalid ladder inputs")
        return plan
    if not _in_path(side, entry, final_tp, final_tp):
        plan.regime_notes.append("final TP is not on profitable side")
        return plan

    final_dist_atr = abs(final_tp - entry) / atr
    risk_points = max(abs(entry - sl), 1e-9)
    final_rr = abs(final_tp - entry) / risk_points
    structural_adjustment = 0.0  # no non-structural overlay may resize TP legs
    rt_cost_points = _cost_points_from_bps(entry, roundtrip_cost_bps)
    rt_cost_r = rt_cost_points / risk_points if rt_cost_points > 0 else 0.0

    rows: List[Dict[str, Any]] = []
    if isinstance(pool_report, dict):
        for r in (pool_report.get("candidates") or []):
            if isinstance(r, dict):
                rows.append(r)
        sel = pool_report.get("selected")
        if isinstance(sel, dict):
            rows.append(sel)

    selected_rows = [r for r in rows if r.get("selected") is True]
    final_rows = selected_rows or [r for r in rows if abs(_num(r.get("tp_price") or r.get("pool_price"), 0.0) - final_tp) <= max(0.35 * atr, 1e-9)]
    # Final objective is already the strategy-selected structural liquidity target.

    selector_cost_r = max([_clamp(_num(r.get("cost_r", 0.0), 0.0), 0.0, 0.85) for r in rows] or [0.0])
    effective_cost_r = max(rt_cost_r, selector_cost_r)
    planning_cost_points = max(rt_cost_points, effective_cost_r * risk_points)
    final_gross_rr = final_rr
    final_net_rr = final_gross_rr - effective_cost_r
    residual_loss_r = 1.0 + effective_cost_r

    raw_internals: List[Dict[str, Any]] = []
    min_spacing = max(min_spacing_atr * atr, 1e-9)
    for r in rows:
        if not _correct_pool_side(side, str(r.get("pool_side", ""))):
            continue
        px = _num(r.get("tp_price") or r.get("pool_price"), 0.0)
        if px <= 0 or not _in_path(side, entry, final_tp, px):
            continue
        if abs(px - final_tp) <= max(0.25 * atr, 1e-9):
            continue
        if abs(px - entry) < max(0.45 * atr, 1e-9):
            continue
        rr = abs(px - entry) / max(abs(entry - sl), 1e-9)
        if rr < 0.35:
            continue
        reason = str(r.get("reason", "") or "")
        if "wrong side" in reason.lower() or "swept" in reason.lower():
            continue
        rr_path = _path_fraction(side, entry, final_tp, px)
        c = dict(r)
        c["_ladder_price"] = px
        c["_ladder_final_tp"] = final_tp
        c["_ladder_gross_rr"] = rr
        c["_ladder_path_frac"] = rr_path
        raw_internals.append(c)
    if raw_internals:
        raw_internals = _annotate_internal_level_economics(
            raw_internals,
            side=side,
            entry=entry,
            risk_points=risk_points,
            rt_cost_points=planning_cost_points,
            base_fee_floor_mult=fee_floor_mult,
            fallback_cost_r=effective_cost_r,
            atr=atr,
        )

    def _return_final_only(reason: str, note: str = "") -> TPLadderPlan:
        plan.final_fraction = 1.0
        plan.internal_budget = 0.0
        if note:
            plan.regime_notes.append(note)
        plan.final_runner_model = {
            "path_strength": 0.0,
            "reason": "final_only_" + str(reason or "native_final_tp"),
            "roundtrip_cost_bps": float(roundtrip_cost_bps or 0.0),
            "rt_cost_r": float(rt_cost_r or 0.0),
            "selector_cost_r": float(selector_cost_r or 0.0),
            "effective_cost_r": float(effective_cost_r or 0.0),
            "residual_loss_r": float(residual_loss_r or 1.0),
            "final_net_rr": float(final_net_rr or 0.0),
        }
        plan.legs = [TPLadderLeg(
            index=1, role="FINAL", price=final_tp, qty_fraction=1.0,
            quantity=total_quantity, source="selected_final_tp",
            distance_atr=final_dist_atr, rr=final_gross_rr,
            gross_rr=final_gross_rr, cost_r=effective_cost_r, net_rr=final_net_rr,
            reason=reason,
        )]
        return plan

    # Prop-desk staging rule: an internal ladder only makes sense when the
    # terminal target has enough net runway after fees/slippage.  If the final TP
    # is already close, adding TP1/TP2 just pays more fees and cuts the runner.
    # Apply this only when a live execution-cost estimate is available; otherwise
    # legacy/offline tests keep the selector's own cost_r behaviour.
    if planning_cost_points > 0.0:
        min_final_net_r_for_ladder = _clamp(1.20 + 1.60 * effective_cost_r, 1.25, 2.35)
        final_net_r = final_net_rr
        if final_net_r < min_final_net_r_for_ladder:
            return _return_final_only(
                "final TP kept as single native bracket target; terminal objective is too close for cost-efficient internal staging",
                (
                    "prop-desk final-only: final net runway "
                    f"{final_net_r:.2f}R < {min_final_net_r_for_ladder:.2f}R after fees/slippage; "
                    "no internal TP ladder"
                ),
            )

    internals = _cluster_effective_liquidity(
        raw_internals,
        side=side,
        entry=entry,
        final_tp=final_tp,
        min_spacing=min_spacing,
    )
    if internals:
        internals, _net_edge_notes = _filter_internal_levels_by_net_edge(
            internals, side=side, entry=entry, risk_points=risk_points,
            rt_cost_points=planning_cost_points, base_fee_floor_mult=fee_floor_mult,
            fallback_cost_r=effective_cost_r, atr=atr,
        )
        plan.regime_notes.extend(_net_edge_notes)
    if internals:
        plan.regime_notes.append(f"MTF liquidity clustering: {len(raw_internals)} raw pools → {len(internals)} effective zones")
        # ICT + Liquidity invariant: no synthetic intermediate target may be
        # inserted between observed liquidity zones.  All internal exits must
        # originate from a validated executable liquidity pool.
        if max_internal_legs > 0 and len(internals) > max_internal_legs:
            internals = internals[:max_internal_legs]
            plan.regime_notes.append(f"internal targets capped by executable lot capacity at {max_internal_legs}")

    # If no valid intermediate liquidity exists, retain the selected final
    # structural liquidity target only.  Synthetic staging is not an
    # executable ICT/Liquidity target and is therefore prohibited.
    if not internals:
        reason = "no valid cost-efficient observed internal liquidity between entry and final TP; final structural liquidity target only"
        return _return_final_only(reason, "no internal ladder: no validated liquidity pool survived execution-cost filters")

    for _c in internals:
        _c["_ladder_structural_adjustment"] = structural_adjustment

    runner_model = _runner_fraction_model(
        final_dist_atr=final_dist_atr,
        final_rr=final_rr,
        structural_adjustment=structural_adjustment,
        internals=internals,
    )
    terminal_information = _clamp(_num(runner_model.get("terminal_information", 0.0), 0.0), 0.0, 1.0)
    # Final runner must be executable and meaningful.  The floor is derived
    # from lot capacity plus terminal information; it is not a fixed TP split.
    _reserve_frac = _clamp(0.06 + 0.22 * terminal_information, 0.055, 0.42)
    if min_leg_fraction > 0:
        _lot_qty = max(min_leg_fraction * total_quantity, 1e-9)
        _target_final_qty = max(_lot_qty, total_quantity * _reserve_frac)
        min_final_fraction = _clamp(
            math.ceil(_target_final_qty / _lot_qty) * _lot_qty / max(total_quantity, 1e-9),
            min_leg_fraction,
            0.42,
        )
    else:
        min_final_fraction = _reserve_frac
    final_fraction = _clamp(_num(runner_model.get("final_fraction"), 1.0), min_final_fraction, 1.0)
    internal_budget = _clamp(1.0 - final_fraction, 0.0, 0.96)
    raw_fracs = _weighted_allocate(internals, internal_budget)
    gross_rewards_r = [
        _num(c.get("_ladder_gross_rr"), abs(_num(c.get("_ladder_price"), entry) - entry) / max(abs(entry - sl), 1e-9))
        for c in internals
    ]
    rewards_r = [
        _num(c.get("_ladder_net_rr"), gross_rewards_r[i] - effective_cost_r)
        for i, c in enumerate(internals)
    ]
    solvency_floor_r = max(
        _estimate_cost_floor_r(internals, selected_rows),
        _clamp(0.85 * effective_cost_r, 0.0, 0.85),
    )
    raw_fracs, final_fraction, checkpoint, worst_r, residual_dom = _enforce_lifecycle_solvency(
        internals=internals,
        fracs=raw_fracs,
        final_fraction=final_fraction,
        rewards_r=rewards_r,
        min_leg_fraction=min_leg_fraction,
        floor_r=solvency_floor_r,
        min_final_fraction=min_final_fraction,
        residual_loss_r=residual_loss_r,
    )
    internals, raw_fracs, final_fraction, rewards_r, _tp1_balance_notes = _balance_first_tp_materiality(
        internals=internals,
        fracs=raw_fracs,
        final_fraction=final_fraction,
        rewards_r=rewards_r,
        min_leg_fraction=min_leg_fraction,
        floor_r=solvency_floor_r,
        min_final_fraction=min_final_fraction,
        final_rr=final_net_rr,
    )
    for _note in _tp1_balance_notes:
        plan.regime_notes.append(_note)
    # TP1 balancing can move quantity away from the solvency checkpoint.  Re-run
    # the same lifecycle proof on the balanced fractions so cost/worst-case
    # protection remains true after the first-exit cap/demotion.
    raw_fracs, final_fraction, checkpoint, worst_r, residual_dom = _enforce_lifecycle_solvency(
        internals=internals,
        fracs=raw_fracs,
        final_fraction=final_fraction,
        rewards_r=rewards_r,
        min_leg_fraction=min_leg_fraction,
        floor_r=solvency_floor_r,
        min_final_fraction=min_final_fraction,
        residual_loss_r=residual_loss_r,
    )
    if checkpoint > len(raw_fracs):
        checkpoint = len(raw_fracs)

    merged_internal: List[tuple[Dict[str, Any], float]] = []
    carry_to_final = 0.0
    for c, f in zip(internals, raw_fracs):
        if f <= 0:
            continue
        if f < min_leg_fraction and len(merged_internal) > 0:
            carry_to_final += f
        else:
            merged_internal.append((c, f))
    if not merged_internal:
        best_i = max(range(len(internals)), key=lambda i: _num(internals[i].get("_ladder_weight"), 1e-6))
        keep = min(max(min_leg_fraction, 1.0 - final_fraction), 0.96)
        merged_internal = [(internals[best_i], keep)]
        final_fraction = max(0.0, 1.0 - keep)
    else:
        final_fraction = _clamp(final_fraction + carry_to_final, 0.0, 1.0)

    internal_sum = sum(f for _, f in merged_internal)
    total_f = internal_sum + final_fraction
    if total_f > 1.0 + 1e-9:
        scale = max(0.0, 1.0 - final_fraction) / max(internal_sum, 1e-9)
        merged_internal = [(c, f * scale) for c, f in merged_internal]
    elif total_f < 1.0 - 1e-9:
        final_fraction += 1.0 - total_f

    legs: List[TPLadderLeg] = []
    idx = 1
    for c, frac in merged_internal:
        px = _num(c.get("_ladder_price"), 0.0)
        qty = total_quantity * frac
        role = f"TP{idx}"
        gross_rr = _num(c.get("_ladder_gross_rr"), abs(px - entry) / max(abs(entry - sl), 1e-9))
        cost_r = _num(c.get("_ladder_cost_r"), effective_cost_r)
        net_rr = _num(c.get("_ladder_net_rr"), gross_rr - cost_r)
        reward_floor_r = _num(c.get("_ladder_reward_floor_r"), 0.0)
        edge_ratio = _num(c.get("_ladder_edge_ratio"), 0.0)
        legs.append(TPLadderLeg(
            index=idx,
            role=role,
            price=px,
            qty_fraction=frac,
            quantity=qty,
            source=str(c.get("_ladder_source", "internal_liquidity") or "internal_liquidity"),
            pool_timeframe=str(c.get("timeframe", "") or ""),
            pool_price=_num(c.get("pool_price"), px),
            distance_atr=abs(px - entry) / atr,
            rr=gross_rr,
            delivery_prob=_num(c.get("delivery_prob", c.get("sweep_prob", 0.0)), 0.0),
            ev=_num(c.get("selection_ev", c.get("ev", 0.0)), 0.0),
            gauntlet_n=int(_num(c.get("gauntlet_n", 0), 0)),
            gross_rr=gross_rr,
            cost_r=cost_r,
            net_rr=net_rr,
            reward_floor_r=reward_floor_r,
            edge_ratio=edge_ratio,
            reason=str(c.get("reason", "internal liquidity monetisation") or "internal liquidity monetisation")[:240],
        ))
        idx += 1

    final_fraction = _clamp(1.0 - sum(l.qty_fraction for l in legs), 0.0, 1.0)
    if final_fraction < min_final_fraction and legs:
        deficit = min_final_fraction - final_fraction
        internal_sum = sum(max(0.0, l.qty_fraction) for l in legs)
        if internal_sum > 1e-12:
            for l in legs:
                l.qty_fraction = max(0.0, l.qty_fraction - deficit * max(0.0, l.qty_fraction) / internal_sum)
                l.quantity = total_quantity * l.qty_fraction
            final_fraction = min_final_fraction
    legs.append(TPLadderLeg(
        index=idx,
        role="FINAL",
        price=final_tp,
        qty_fraction=final_fraction,
        quantity=total_quantity * final_fraction,
        source="selected_final_tp",
        distance_atr=final_dist_atr,
        rr=final_gross_rr,
        gross_rr=final_gross_rr,
        cost_r=effective_cost_r,
        net_rr=final_net_rr,
        reason="strategy-selected final liquidity TP; native bracket remains as fallback/final target",
    ))

    plan.legs = legs
    plan.final_fraction = final_fraction
    plan.internal_budget = 1.0 - final_fraction
    plan.solvency_checkpoint_index = int(checkpoint or 0)
    plan.solvency_floor_r = float(solvency_floor_r or 0.0)
    try:
        _fr = [float(getattr(l, "qty_fraction", 0.0) or 0.0) for l in legs if str(getattr(l, "role", "")).upper() != "FINAL"]
        _rr = [float(getattr(l, "net_rr", 0.0) or 0.0) for l in legs if str(getattr(l, "role", "")).upper() != "FINAL"]
        if plan.solvency_checkpoint_index > 0:
            plan.worst_case_after_checkpoint_r = _prefix_solvency_r(
                _fr, _rr, min(plan.solvency_checkpoint_index, len(_fr)),
                residual_loss_r=residual_loss_r,
            )
            if abs(plan.worst_case_after_checkpoint_r - plan.solvency_floor_r) <= 1e-8:
                plan.worst_case_after_checkpoint_r = plan.solvency_floor_r
        plan.expected_internal_profit_r = sum(
            float(getattr(l, "qty_fraction", 0.0) or 0.0)
            * float(getattr(l, "net_rr", 0.0) or 0.0)
            * _clamp(float(getattr(l, "delivery_prob", 0.0) or 0.5), 0.05, 0.95)
            for l in legs if str(getattr(l, "role", "")).upper() != "FINAL"
        )
        plan.residual_domination = (float(final_fraction) * residual_loss_r) / max(plan.expected_internal_profit_r, 1e-9)
    except Exception:
        plan.worst_case_after_checkpoint_r = float(worst_r or 0.0)
        plan.residual_domination = float(residual_dom or 0.0)
    runner_model.update({
        "roundtrip_cost_bps": float(roundtrip_cost_bps or 0.0),
        "rt_cost_r": float(rt_cost_r or 0.0),
        "selector_cost_r": float(selector_cost_r or 0.0),
        "effective_cost_r": float(effective_cost_r or 0.0),
        "residual_loss_r": float(residual_loss_r or 1.0),
        "final_net_rr": float(final_net_rr or 0.0),
    })
    plan.final_runner_model = dict(runner_model)
    if min_final_fraction > 0:
        plan.regime_notes.append(f"terminal-runner reserve {min_final_fraction:.0%}: final TP kept executable after path monetisation")
    if structural_adjustment > 0.15:
        plan.regime_notes.append(f"structural adjustment +{structural_adjustment:.2f}: runner earned by delivery geometry")
    elif structural_adjustment < -0.05:
        plan.regime_notes.append(f"structural adjustment {structural_adjustment:.2f}: internal monetisation preferred")
    if final_dist_atr > 6.0:
        plan.regime_notes.append(f"higher final TP {final_dist_atr:.1f}ATR: path ladder monetises internal liquidity")
    if final_dist_atr > 12.0:
        plan.regime_notes.append("extreme final objective: runner size reduced by distance/path model")
    if plan.solvency_checkpoint_index > 0:
        plan.regime_notes.append(
            f"solvency checkpoint TP{plan.solvency_checkpoint_index}: worst-case {plan.worst_case_after_checkpoint_r:+.2f}R after original SL"
        )
    if plan.residual_domination > 0:
        plan.regime_notes.append(f"residual-domination {plan.residual_domination:.2f}x")
    return plan

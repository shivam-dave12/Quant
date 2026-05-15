"""
tp_ladder.py — Dynamic liquidity target ladder
================================================
Builds TP1..TPn from internal liquidity between entry and final TP.

Design constraints:
- Final TP remains the strategy-selected final liquidity objective.
- Internal BSL/SSL pools become partial reduce-only targets.
- If no internal liquidity exists, a Fibonacci auction-geometry fallback may
  create intermediate TP legs between entry and the already-selected final TP.
  This does not create a new trade target; it only monetises the path to a
  validated final objective.
- SL price is never moved by this module.
- Quantity allocation is dynamic: path distance, delivery probability,
  liquidity quality, gauntlet, execution cost, liquidity+Fibonacci auction
  geometry, and cross-asset sponsorship decide how much size is monetised
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
    fib_confluence: float = 1.0
    fib_score: float = 0.0
    fib_ratio: float = 0.0
    fib_role: str = ""
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


def _cross_asset_sponsorship(cross_asset_state: Any, side: str, asset_id: str) -> float:
    """Return [-1, +1] soft sponsorship score. No hard veto."""
    if cross_asset_state is None:
        return 0.0
    try:
        adj = cross_asset_state.adjustment_for_signal(str(asset_id or ""), side)
        logit = _num(getattr(adj, "posterior_logit_adjust", getattr(adj, "posterior_delta", 0.0)), 0.0)
        allow = bool(getattr(adj, "entry_allowed", True))
        base = _num(getattr(adj, "tp_aggression", 0.0), 0.0) + 0.5 * (logit / 0.16)
        if not allow:
            base = min(base, -0.75)
        return _clamp(base, -1.0, 1.0)
    except Exception:
        pass
    try:
        pref = str(getattr(cross_asset_state, "preferred_asset", "") or "").upper()
        aid = str(asset_id or "").upper()
        regime = str(getattr(cross_asset_state, "metals_regime", "") or "").upper()
        if pref and aid and pref == aid:
            return 0.45
        if regime == "COHERENT_UPTREND" and side == "long":
            return 0.25
        if regime == "COHERENT_DOWNTREND" and side == "short":
            return 0.25
        if regime in ("NOISE", "DIVERGENCE", "RELATIVE_VALUE_DIVERGENCE"):
            return -0.10
    except Exception:
        return 0.0
    return 0.0


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
    fib_mult = _clamp(_num(row.get("fib_confluence", row.get("fib_multiplier", 1.0)), 1.0), 0.90, 1.32)
    fib_score = _clamp(_num(row.get("fib_score", 0.0), 0.0), 0.0, 1.0)
    gauntlet = max(0, int(_num(row.get("gauntlet_n", 0), 0)))
    # Closer internal liquidity is monetisation, but not blindly: very tiny
    # path_frac targets get less size because costs can dominate.
    proximity = math.exp(-1.35 * max(0.0, path_frac - 0.12))
    too_near_pen = _clamp(path_frac / 0.10, 0.35, 1.0)
    gauntlet_pen = 1.0 / (1.0 + 0.22 * gauntlet)
    return max(
        1e-6,
        dp * (0.65 + 0.18 * math.sqrt(sig + 1.0)) * (0.75 + qual)
        * (0.75 + 0.25 * confluence)
        * (0.88 + 0.12 * fib_mult + 0.10 * fib_score)
        * proximity * too_near_pen
        * gauntlet_pen * (1.0 + 0.10 * min(ev, 3.0)) * (1.0 + 0.18 * ladder_score),
    )



# Fibonacci fallback ratios are used only when no internal liquidity exists
# between entry and the selected final TP.  They are path monetisation points,
# not a standalone target generator.
_FIB_FALLBACK_PATH_RATIOS = (0.382, 0.500, 0.618, 0.786)


def _fib_path_price(side: str, entry: float, final_tp: float, frac: float) -> float:
    if side == "long":
        return entry + abs(final_tp - entry) * float(frac)
    return entry - abs(final_tp - entry) * float(frac)


def _fib_fallback_internals(*, side: str, entry: float, sl: float, final_tp: float,
                            atr: float, max_internal_legs: int,
                            min_spacing: float, sponsorship: float,
                            final_fib_score: float, final_fib_mult: float,
                            final_dist_atr: float) -> List[Dict[str, Any]]:
    """Create Fibonacci path-monetisation legs when internal liquidity is absent.

    This is deliberately conservative: the final TP must already be validated by
    the strategy/selector, distance must be large enough to need staging, and
    quantity capacity must permit at least one independent reduce-only leg.
    """
    if max_internal_legs <= 0:
        return []
    if final_dist_atr < 1.65:
        return []
    risk = abs(entry - sl)
    if risk <= 1e-12:
        return []
    atr = max(float(atr or 0.0), 1e-9)
    path = abs(final_tp - entry)
    if path <= max(0.75 * atr, 1e-9):
        return []
    out: List[Dict[str, Any]] = []
    seen: List[float] = []
    max_legs = max(0, int(max_internal_legs or 0))
    fib_quality = _clamp(0.55 * _clamp(final_fib_score, 0.0, 1.0) + 0.45 * ((final_fib_mult - 0.90) / 0.42), 0.0, 1.0)
    sponsor = _clamp(sponsorship, -1.0, 1.0)
    for ratio in _FIB_FALLBACK_PATH_RATIOS:
        if len(out) >= max_legs:
            break
        px = _fib_path_price(side, entry, final_tp, ratio)
        if not _in_path(side, entry, final_tp, px):
            continue
        # Avoid false precision around entry/final.  A fallback TP must be far
        # enough to pay spread/fees/noise and far enough from final to remain a
        # true intermediate monetisation leg.
        if abs(px - entry) < max(0.55 * atr, 0.16 * risk):
            continue
        if abs(final_tp - px) < max(0.45 * atr, 0.08 * path):
            continue
        if any(abs(px - p) < min_spacing for p in seen):
            continue
        rr = abs(px - entry) / max(risk, 1e-9)
        if rr < 0.35:
            continue
        path_frac = _path_fraction(side, entry, final_tp, px)
        # Delivery estimate is path-based: nearer fallback legs have higher
        # probability; strong final Fib geometry and cross-asset sponsorship
        # can lift it, but cannot overpower adverse sponsorship.
        delivery = _clamp(
            0.76 * math.exp(-0.82 * path_frac)
            + 0.10 * fib_quality
            + 0.06 * max(0.0, sponsor)
            - 0.10 * max(0.0, -sponsor),
            0.10,
            0.84,
        )
        row = {
            "pool_side": "BSL" if side == "long" else "SSL",
            "tp_price": px,
            "pool_price": px,
            "quality": _clamp(0.46 + 0.22 * fib_quality + 0.08 * max(0.0, sponsor), 0.30, 0.82),
            "significance": _clamp(2.2 + 2.4 * fib_quality + 1.0 * (1.0 - path_frac), 1.5, 6.0),
            "delivery_prob": delivery,
            "selection_ev": max(0.0, rr * delivery - (1.0 - delivery)),
            "fib_confluence": _clamp(1.02 + 0.18 * fib_quality, 0.98, 1.20),
            "fib_score": _clamp(0.55 + 0.35 * fib_quality, 0.35, 0.90),
            "fib_ratio": ratio,
            "fib_role": "fib_fallback_monetisation",
            "timeframe": "fib_path",
            "cost_r": 0.0,
            "reason": f"fib fallback path monetisation {ratio:.3g}; no internal liquidity before selected final TP",
            "_ladder_price": px,
            "_ladder_path_frac": path_frac,
            "_ladder_source": "fib_fallback_geometry",
        }
        row["_ladder_weight"] = _candidate_weight(row, path_frac) * (0.90 + 0.15 * fib_quality)
        out.append(row)
        seen.append(px)
    return out


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


def _runner_fraction_model(*, final_dist_atr: float, final_rr: float, sponsorship: float, internals: List[Dict[str, Any]], final_fib_score: float = 0.0, final_fib_mult: float = 1.0) -> Dict[str, float]:
    """Earn final-runner size from path, sponsorship, and liquidity+Fib geometry."""
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
    fib_score = _clamp(float(final_fib_score or 0.0), 0.0, 1.0)
    fib_mult = _clamp(float(final_fib_mult or 1.0), 0.90, 1.32)
    fib_quality = _clamp(0.55 * fib_score + 0.45 * ((fib_mult - 0.90) / 0.42), 0.0, 1.0)
    path_strength = _clamp(0.30 * coverage + 0.24 * density + 0.22 * quality + 0.14 * early_quality + 0.10 * fib_quality, 0.0, 1.0)
    sponsor = _clamp(float(sponsorship or 0.0), -1.0, 1.0)
    raw = 0.08 + 0.40 * path_strength + 0.10 * rr_saturation - 0.25 * distance_penalty + 0.14 * max(0.0, sponsor) - 0.12 * max(0.0, -sponsor) + 0.10 * fib_quality
    min_runner = _clamp(0.025 + 0.035 * max(0.0, sponsor) + 0.035 * fib_quality, 0.02, 0.18)
    max_runner = _clamp(0.26 + 0.30 * path_strength + 0.10 * max(0.0, sponsor) + 0.10 * fib_quality, 0.22, 0.78)
    final_fraction = _clamp(raw, min_runner, max_runner)
    return {
        "final_fraction": final_fraction,
        "path_strength": path_strength,
        "coverage": coverage,
        "density": density,
        "quality": quality,
        "early_quality": early_quality,
        "max_gap": max_gap,
        "distance_penalty": distance_penalty,
        "rr_saturation": rr_saturation,
        "sponsorship": sponsor,
        "fib_score": fib_score,
        "fib_multiplier": fib_mult,
        "fib_quality": fib_quality,
        "min_runner": min_runner,
        "max_runner": max_runner,
    }


def _weighted_allocate(cands: List[Dict[str, Any]], budget: float) -> List[float]:
    if not cands or budget <= 0:
        return [0.0 for _ in cands]
    weights = [max(1e-6, _num(c.get("_ladder_weight"), 1e-6)) for c in cands]
    wsum = sum(weights) or 1.0
    return [float(budget) * w / wsum for w in weights]


def _prefix_solvency_r(fracs: List[float], rewards_r: List[float], k: int) -> float:
    filled = sum(fracs[:k])
    profit = sum(fracs[i] * rewards_r[i] for i in range(min(k, len(fracs))))
    return profit - max(0.0, 1.0 - filled)


def _select_solvency_checkpoint(internals: List[Dict[str, Any]], rewards_r: List[float], max_internal_budget: float, floor_r: float) -> int:
    """Earliest data-supported checkpoint where reversal to original SL can remain solvent."""
    if not internals:
        return 0
    for k in range(1, len(internals) + 1):
        prefix = internals[:k]
        pweights = [max(1e-6, _num(c.get("_ladder_weight"), 1e-6)) for c in prefix]
        wsum = sum(pweights) or 1.0
        avg_r = sum((pweights[i] / wsum) * rewards_r[i] for i in range(k))
        required_frac = (1.0 + floor_r) / max(1.0 + avg_r, 1e-9)
        path_frac = _num(prefix[-1].get("_ladder_path_frac"), 0.0)
        proof_needed = _clamp(1.0 / max(1.0 + max(rewards_r), 1e-9), 0.08, 0.42)
        if required_frac <= max_internal_budget + 1e-9 and path_frac >= proof_needed:
            return k
    return len(internals)


def _enforce_lifecycle_solvency(*, internals: List[Dict[str, Any]], fracs: List[float], final_fraction: float, rewards_r: List[float], min_leg_fraction: float, floor_r: float) -> tuple[List[float], float, int, float, float]:
    """Front-load enough internal liquidity so TP checkpoint + original SL remains solvent."""
    if not internals:
        return fracs, final_fraction, 0, 0.0, 0.0
    final_fraction = _clamp(final_fraction, 0.0, 1.0)
    internal_budget = _clamp(1.0 - final_fraction, 0.0, 1.0)
    checkpoint = _select_solvency_checkpoint(
        internals,
        rewards_r,
        max_internal_budget=1.0 - _clamp(final_fraction * 0.25, 0.0, 0.95),
        floor_r=floor_r,
    )
    if checkpoint <= 0:
        return fracs, final_fraction, 0, 0.0, 0.0
    if not fracs or len(fracs) != len(internals):
        fracs = _weighted_allocate(internals, internal_budget)
    for _ in range(24):
        solv = _prefix_solvency_r(fracs, rewards_r, checkpoint)
        if solv >= floor_r - 1e-9:
            break
        prefix_weights = [(1.0 + rewards_r[i]) * max(1e-6, _num(internals[i].get("_ladder_weight"), 1e-6)) for i in range(checkpoint)]
        psum = sum(prefix_weights) or 1.0
        avg_eff = sum((prefix_weights[i] / psum) * (1.0 + rewards_r[i]) for i in range(checkpoint))
        need = max(0.0, (floor_r - solv) / max(avg_eff, 1e-9))
        move = min(need, max(0.0, final_fraction - 0.02))
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
    worst = _prefix_solvency_r(fracs, rewards_r, checkpoint)
    expected_internal = sum(
        fracs[i] * rewards_r[i] * _clamp(_num(internals[i].get("delivery_prob", internals[i].get("sweep_prob", 0.5)), 0.5), 0.05, 0.95)
        for i in range(len(fracs))
    )
    residual_domination = _clamp(final_fraction, 0.0, 1.0) / max(expected_internal, 1e-9)
    return fracs, _clamp(final_fraction, 0.0, 1.0), checkpoint, worst, residual_domination


def build_tp_ladder(
    *,
    side: str,
    entry: float,
    sl: float,
    final_tp: float,
    atr: float,
    total_quantity: float,
    pool_report: Optional[Dict[str, Any]] = None,
    target_surface: Any = None,
    cross_asset_state: Any = None,
    asset_id: str = "",
    min_leg_fraction: float = 0.0,
    min_spacing_atr: float = 0.35,
    max_internal_legs: int = 0,
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
    final_rr = abs(final_tp - entry) / max(abs(entry - sl), 1e-9)
    sponsorship = _cross_asset_sponsorship(cross_asset_state, side, asset_id)

    rows: List[Dict[str, Any]] = []
    if isinstance(pool_report, dict):
        for r in (pool_report.get("candidates") or []):
            if isinstance(r, dict):
                rows.append(r)
        sel = pool_report.get("selected")
        if isinstance(sel, dict):
            rows.append(sel)

    internals: List[Dict[str, Any]] = []
    seen_prices: List[float] = []
    min_spacing = max(min_spacing_atr * atr, 1e-9)
    for r in rows:
        if not _correct_pool_side(side, str(r.get("pool_side", ""))):
            continue
        px = _num(r.get("tp_price") or r.get("pool_price"), 0.0)
        if px <= 0 or not _in_path(side, entry, final_tp, px):
            continue
        # Do not duplicate the final TP as internal.
        if abs(px - final_tp) <= max(0.25 * atr, 1e-9):
            continue
        # Skip micro-targets inside cost/noise zone.
        if abs(px - entry) < max(0.45 * atr, 1e-9):
            continue
        if any(abs(px - p) < min_spacing for p in seen_prices):
            continue
        rr = abs(px - entry) / max(abs(entry - sl), 1e-9)
        if rr < 0.35:
            continue
        rr_req = _num(r.get("required_rr", 0.0), 0.0)
        # Eligible rows are preferable, but rejected rows can still become small
        # monetisation targets if they are closer than the final and not invalid
        # because of side/status. This is intentional: internal TP != full TP.
        reason = str(r.get("reason", "") or "")
        if "wrong side" in reason.lower() or "swept" in reason.lower():
            continue
        rr *= (1.0 - 0.08 * max(0.0, rr_req - rr))
        rr = max(rr, 0.0)
        rr_path = _path_fraction(side, entry, final_tp, px)
        rr_weight = _candidate_weight(r, rr_path)
        c = dict(r)
        c["_ladder_price"] = px
        c["_ladder_path_frac"] = rr_path
        c["_ladder_weight"] = rr_weight
        internals.append(c)
        seen_prices.append(px)

    reverse = side == "short"  # short targets descend; sort by distance from entry ascending
    internals.sort(key=lambda x: abs(_num(x.get("_ladder_price"), 0.0) - entry))
    if max_internal_legs > 0 and len(internals) > max_internal_legs:
        internals = internals[:max_internal_legs]
        plan.regime_notes.append(f"internal targets capped by executable lot capacity at {max_internal_legs}")

    selected_rows = [r for r in rows if r.get("selected") is True]
    final_rows = selected_rows or [r for r in rows if abs(_num(r.get("tp_price") or r.get("pool_price"), 0.0) - final_tp) <= max(0.35 * atr, 1e-9)]
    final_fib_score = max([_clamp(_num(r.get("fib_score", 0.0), 0.0), 0.0, 1.0) for r in final_rows] or [0.0])
    final_fib_mult = max([_clamp(_num(r.get("fib_confluence", r.get("fib_multiplier", 1.0)), 1.0), 0.90, 1.32) for r in final_rows] or [1.0])
    final_fib_ratio = max([_num(r.get("fib_ratio", 0.0), 0.0) for r in final_rows] or [0.0])

    # If no valid internal liquidity exists, build Fibonacci path-monetisation
    # legs only when the final TP is already validated and exchange lot capacity
    # allows partial reduce-only exits.  Otherwise return final-only and explain.
    if not internals:
        fib_internals = _fib_fallback_internals(
            side=side,
            entry=entry,
            sl=sl,
            final_tp=final_tp,
            atr=atr,
            max_internal_legs=max_internal_legs,
            min_spacing=min_spacing,
            sponsorship=sponsorship,
            final_fib_score=final_fib_score,
            final_fib_mult=final_fib_mult,
            final_dist_atr=final_dist_atr,
        )
        if fib_internals:
            internals = fib_internals
            plan.regime_notes.append(
                f"no internal liquidity: using Fibonacci path-monetisation fallback ({len(internals)} legs)"
            )
        else:
            plan.final_fraction = 1.0
            plan.internal_budget = 0.0
            reason = "no valid internal liquidity between entry and final TP"
            if max_internal_legs <= 0:
                reason += "; position size not splittable under exchange lot/min-qty constraints"
                plan.regime_notes.append("no internal/fib ladder: quantity cannot be split under exchange lot size")
            elif final_dist_atr < 1.65:
                reason += "; final TP too close for non-noisy Fibonacci staging"
                plan.regime_notes.append("no internal/fib ladder: final TP too close for non-noisy staging")
            else:
                plan.regime_notes.append("no internal/fib ladder: no robust Fibonacci staging level survived cost/noise filters")
            plan.final_runner_model = {"path_strength": 0.0, "reason": "no_internal_liquidity_or_fib_capacity", "fib_score": final_fib_score, "fib_multiplier": final_fib_mult, "fib_ratio": final_fib_ratio, "max_internal_legs": float(max_internal_legs)}
            plan.legs = [TPLadderLeg(
                index=1, role="FINAL", price=final_tp, qty_fraction=1.0,
                quantity=total_quantity, source="selected_final_tp",
                distance_atr=final_dist_atr,
                rr=final_rr,
                reason=reason,
            )]
            return plan

    runner_model = _runner_fraction_model(
        final_dist_atr=final_dist_atr,
        final_rr=final_rr,
        sponsorship=sponsorship,
        internals=internals,
        final_fib_score=final_fib_score,
        final_fib_mult=final_fib_mult,
    )
    final_fraction = _clamp(_num(runner_model.get("final_fraction"), 1.0), 0.0, 1.0)
    internal_budget = _clamp(1.0 - final_fraction, 0.0, 0.96)
    raw_fracs = _weighted_allocate(internals, internal_budget)
    rewards_r = [abs(_num(c.get("_ladder_price"), entry) - entry) / max(abs(entry - sl), 1e-9) for c in internals]
    solvency_floor_r = _estimate_cost_floor_r(internals, selected_rows)
    raw_fracs, final_fraction, checkpoint, worst_r, residual_dom = _enforce_lifecycle_solvency(
        internals=internals,
        fracs=raw_fracs,
        final_fraction=final_fraction,
        rewards_r=rewards_r,
        min_leg_fraction=min_leg_fraction,
        floor_r=solvency_floor_r,
    )

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
            rr=abs(px - entry) / max(abs(entry - sl), 1e-9),
            delivery_prob=_num(c.get("delivery_prob", c.get("sweep_prob", 0.0)), 0.0),
            ev=_num(c.get("selection_ev", c.get("ev", 0.0)), 0.0),
            gauntlet_n=int(_num(c.get("gauntlet_n", 0), 0)),
            fib_confluence=_num(c.get("fib_confluence", c.get("fib_multiplier", 1.0)), 1.0),
            fib_score=_num(c.get("fib_score", 0.0), 0.0),
            fib_ratio=_num(c.get("fib_ratio", 0.0), 0.0),
            fib_role=str(c.get("fib_role", "") or ""),
            reason=str(c.get("reason", "internal liquidity monetisation") or "internal liquidity monetisation")[:240],
        ))
        idx += 1

    final_fraction = _clamp(1.0 - sum(l.qty_fraction for l in legs), 0.0, 1.0)
    legs.append(TPLadderLeg(
        index=idx,
        role="FINAL",
        price=final_tp,
        qty_fraction=final_fraction,
        quantity=total_quantity * final_fraction,
        source="selected_final_tp",
        distance_atr=final_dist_atr,
        rr=abs(final_tp - entry) / max(abs(entry - sl), 1e-9),
        fib_confluence=final_fib_mult,
        fib_score=final_fib_score,
        fib_ratio=final_fib_ratio,
        fib_role="final_runner_projection" if final_fib_score > 0 else "",
        reason="strategy-selected final liquidity TP; native bracket remains as fallback/final target",
    ))

    plan.legs = legs
    plan.final_fraction = final_fraction
    plan.internal_budget = 1.0 - final_fraction
    plan.solvency_checkpoint_index = int(checkpoint or 0)
    plan.solvency_floor_r = float(solvency_floor_r or 0.0)
    try:
        _fr = [float(getattr(l, "qty_fraction", 0.0) or 0.0) for l in legs if str(getattr(l, "role", "")).upper() != "FINAL"]
        _rr = [float(getattr(l, "rr", 0.0) or 0.0) for l in legs if str(getattr(l, "role", "")).upper() != "FINAL"]
        if plan.solvency_checkpoint_index > 0:
            plan.worst_case_after_checkpoint_r = _prefix_solvency_r(_fr, _rr, min(plan.solvency_checkpoint_index, len(_fr)))
        plan.expected_internal_profit_r = sum(
            float(getattr(l, "qty_fraction", 0.0) or 0.0)
            * float(getattr(l, "rr", 0.0) or 0.0)
            * _clamp(float(getattr(l, "delivery_prob", 0.0) or 0.5), 0.05, 0.95)
            for l in legs if str(getattr(l, "role", "")).upper() != "FINAL"
        )
        plan.residual_domination = float(final_fraction) / max(plan.expected_internal_profit_r, 1e-9)
    except Exception:
        plan.worst_case_after_checkpoint_r = float(worst_r or 0.0)
        plan.residual_domination = float(residual_dom or 0.0)
    plan.final_runner_model = dict(runner_model)
    if sponsorship > 0.15:
        plan.regime_notes.append(f"cross-asset sponsorship +{sponsorship:.2f}: runner earned by sponsorship")
    elif sponsorship < -0.05:
        plan.regime_notes.append(f"cross-asset sponsorship {sponsorship:.2f}: internal monetisation preferred")
    if final_fib_score >= 0.35:
        plan.regime_notes.append(f"liq+Fib final geometry: ratio={final_fib_ratio:.3g} score={final_fib_score:.2f} ×{final_fib_mult:.2f}")
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

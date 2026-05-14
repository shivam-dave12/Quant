"""
tp_ladder.py — Dynamic liquidity target ladder
================================================
Builds TP1..TPn from internal liquidity between entry and final TP.

Design constraints:
- Final TP remains the strategy-selected final liquidity objective.
- Internal BSL/SSL pools become partial reduce-only targets.
- SL price is never moved by this module.
- Quantity allocation is dynamic: path distance, delivery probability,
  liquidity quality, gauntlet, execution cost, and cross-asset sponsorship
  decide how much size is monetised before the final target.
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
        return _clamp(
            _num(getattr(adj, "tp_aggression", 0.0), 0.0)
            + 0.5 * (_num(getattr(adj, "posterior_delta", 0.0), 0.0) / 0.08),
            -1.0,
            1.0,
        )
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
    gauntlet = max(0, int(_num(row.get("gauntlet_n", 0), 0)))
    # Closer internal liquidity is monetisation, but not blindly: very tiny
    # path_frac targets get less size because costs can dominate.
    proximity = math.exp(-1.35 * max(0.0, path_frac - 0.12))
    too_near_pen = _clamp(path_frac / 0.10, 0.35, 1.0)
    gauntlet_pen = 1.0 / (1.0 + 0.22 * gauntlet)
    return max(
        1e-6,
        dp * (0.65 + 0.18 * math.sqrt(sig + 1.0)) * (0.75 + qual)
        * (0.75 + 0.25 * confluence) * proximity * too_near_pen
        * gauntlet_pen * (1.0 + 0.10 * min(ev, 3.0)) * (1.0 + 0.18 * ladder_score),
    )


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
    min_leg_fraction: float = 0.08,
    min_spacing_atr: float = 0.35,
    max_internal_legs: int = 6,
) -> TPLadderPlan:
    side = str(side or "").lower()
    entry = float(entry or 0.0)
    sl = float(sl or 0.0)
    final_tp = float(final_tp or 0.0)
    atr = max(float(atr or 0.0), 1e-9)
    total_quantity = max(float(total_quantity or 0.0), 0.0)
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
    sponsorship = _cross_asset_sponsorship(cross_asset_state, side, asset_id)

    # Runner size is dynamic. With a ladder we can choose a higher final
    # liquidity objective, but the size left for that final objective must be
    # smaller when distance/path friction is high. Strong cross-asset sponsorship
    # can keep a larger runner; negative/noisy sponsorship front-loads exits.
    distance_frontload = _clamp((final_dist_atr - 2.25) / 10.0, 0.0, 1.0)
    extreme_frontload = _clamp((final_dist_atr - 9.0) / 16.0, 0.0, 1.0)
    final_fraction = 0.54 - 0.26 * distance_frontload - 0.16 * extreme_frontload
    final_fraction += 0.18 * max(0.0, sponsorship)
    final_fraction -= 0.10 * max(0.0, -sponsorship)
    final_fraction = _clamp(final_fraction, 0.10, 0.68)

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
    if len(internals) > max_internal_legs:
        internals = internals[:max_internal_legs]
        plan.regime_notes.append(f"internal targets capped at {max_internal_legs}")

    # If no valid internal targets exist, return final-only ladder.
    if not internals:
        plan.final_fraction = 1.0
        plan.internal_budget = 0.0
        plan.legs = [TPLadderLeg(
            index=1, role="FINAL", price=final_tp, qty_fraction=1.0,
            quantity=total_quantity, source="selected_final_tp",
            distance_atr=final_dist_atr,
            rr=abs(final_tp - entry) / max(abs(entry - sl), 1e-9),
            reason="no valid internal liquidity between entry and final TP",
        )]
        return plan

    internal_budget = _clamp(1.0 - final_fraction, 0.0, 0.82)
    weights = [max(1e-6, _num(c.get("_ladder_weight"), 1e-6)) for c in internals]
    wsum = sum(weights) or 1.0
    raw_fracs = [internal_budget * w / wsum for w in weights]

    # Enforce minimum leg fraction by merging tiny allocations into the nearest
    # stronger internal leg or final runner. This avoids dust orders.
    merged_internal: List[tuple[Dict[str, Any], float]] = []
    carry_to_final = 0.0
    for c, f in zip(internals, raw_fracs):
        if f < min_leg_fraction:
            carry_to_final += f
        else:
            merged_internal.append((c, f))
    if not merged_internal:
        # At least one internal target should survive when internals exist.
        best_i = max(range(len(internals)), key=lambda i: weights[i])
        keep = max(min_leg_fraction, min(internal_budget, 0.35))
        merged_internal = [(internals[best_i], keep)]
        carry_to_final = max(0.0, internal_budget - keep)

    final_fraction = _clamp(final_fraction + carry_to_final, 0.12, 0.92)
    # Renormalise if min-leg constraints pushed total above one.
    internal_sum = sum(f for _, f in merged_internal)
    total_f = internal_sum + final_fraction
    if total_f > 1.0 + 1e-9:
        scale = (1.0 - final_fraction) / max(internal_sum, 1e-9)
        merged_internal = [(c, f * scale) for c, f in merged_internal]

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
            source="internal_liquidity",
            pool_timeframe=str(c.get("timeframe", "") or ""),
            pool_price=_num(c.get("pool_price"), px),
            distance_atr=abs(px - entry) / atr,
            rr=abs(px - entry) / max(abs(entry - sl), 1e-9),
            delivery_prob=_num(c.get("delivery_prob", c.get("sweep_prob", 0.0)), 0.0),
            ev=_num(c.get("selection_ev", c.get("ev", 0.0)), 0.0),
            gauntlet_n=int(_num(c.get("gauntlet_n", 0), 0)),
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
        reason="strategy-selected final liquidity TP; native bracket remains as fallback/final target",
    ))

    plan.legs = legs
    plan.final_fraction = final_fraction
    plan.internal_budget = 1.0 - final_fraction
    if sponsorship > 0.15:
        plan.regime_notes.append(f"cross-asset sponsorship +{sponsorship:.2f}: more final runner allowed")
    elif sponsorship < -0.05:
        plan.regime_notes.append(f"cross-asset sponsorship {sponsorship:.2f}: internal monetisation preferred")
    if final_dist_atr > 6.0:
        plan.regime_notes.append(f"higher final TP {final_dist_atr:.1f}ATR: internal ladder monetises path")
    if final_dist_atr > 12.0:
        plan.regime_notes.append("extreme final objective: runner size reduced dynamically")
    return plan

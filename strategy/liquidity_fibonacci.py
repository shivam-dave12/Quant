"""
liquidity_fibonacci.py — Liquidity-first Fibonacci auction geometry
===================================================================

Institutional rule:
    Liquidity remains the primary final target source.
    Fibonacci normally upgrades/downgrades existing liquidity targets.
    If no internal liquidity exists between entry and the selected final TP,
    TP ladder code may use Fibonacci as path-monetisation geometry for TP1..TPn.
    Fibonacci must not invent a new final target by itself.

This module scores whether a BSL/SSL liquidity pool sits at a structurally
reasonable expansion/rotation distance from the entry auction.  The output is
soft confluence only; callers must still require live liquidity, delivery
probability, path monetisation, lifecycle solvency, and execution feasibility.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import math
from typing import Any, Dict, Iterable, List, Optional, Tuple


FIB_EXTENSION_LEVELS: Tuple[float, ...] = (0.382, 0.500, 0.618, 0.786, 1.000, 1.272, 1.414, 1.618, 2.000, 2.618, 3.618)
_RUNNER_LEVELS = (1.272, 1.414, 1.618, 2.000, 2.618, 3.618)
_MONETISATION_LEVELS = (0.382, 0.500, 0.618, 0.786, 1.000)


def _safe(obj: Any, attr: str, default: Any = 0.0) -> Any:
    try:
        v = getattr(obj, attr, default)
        return v if v is not None else default
    except Exception:
        return default


def _num(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        f = float(v)
        if not math.isfinite(f):
            return default
        return f
    except Exception:
        return default


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(x)))


def _side_value(pool: Any) -> str:
    try:
        side = getattr(pool, "side", "")
        if hasattr(side, "value"):
            side = side.value
        return str(side or "").upper()
    except Exception:
        return ""


def _status_value(pool: Any) -> str:
    try:
        st = getattr(pool, "status", "")
        if hasattr(st, "value"):
            st = st.value
        return str(st or "").upper()
    except Exception:
        return ""


def _tf_rank(tf: str) -> int:
    return {"1m": 1, "2m": 1, "3m": 1, "5m": 2, "15m": 3, "1h": 4, "4h": 5, "1d": 6}.get(str(tf).lower(), 2)


@dataclass(frozen=True)
class FibGeometryScore:
    score: float
    multiplier: float
    nearest_ratio: float
    distance_atr: float
    anchor_price: float
    impulse_atr: float
    tolerance_atr: float
    role: str
    components: Dict[str, float] = field(default_factory=dict)
    reason: str = ""

    def as_dict(self) -> Dict[str, float | str]:
        return {
            "score": float(self.score),
            "multiplier": float(self.multiplier),
            "nearest_ratio": float(self.nearest_ratio),
            "distance_atr": float(self.distance_atr),
            "anchor_price": float(self.anchor_price),
            "impulse_atr": float(self.impulse_atr),
            "tolerance_atr": float(self.tolerance_atr),
            "role": self.role,
            "reason": self.reason,
            **{f"component_{k}": float(v) for k, v in self.components.items()},
        }


def _iter_pool_targets(snap: Any, name: str) -> Iterable[Any]:
    try:
        return list(getattr(snap, name, []) or [])
    except Exception:
        return []


def _pool_price(target: Any) -> float:
    try:
        pool = getattr(target, "pool", target)
        return _num(getattr(pool, "price", 0.0), 0.0)
    except Exception:
        return 0.0


def _pool_quality(target: Any) -> float:
    try:
        pool = getattr(target, "pool", target)
        sig = max(0.0, _num(getattr(target, "significance", 0.0), 0.0))
        touches = max(1.0, _num(getattr(pool, "touches", 1.0), 1.0))
        tf = str(getattr(pool, "timeframe", "5m") or "5m")
        status = _status_value(pool)
        if status in ("SWEPT", "CONSUMED"):
            return 0.0
        touch_pen = 1.0 / math.sqrt(max(touches, 1.0))
        tf_bonus = 0.85 + 0.05 * min(_tf_rank(tf), 6)
        return _clamp(0.22 * math.sqrt(sig + 1.0) * touch_pen * tf_bonus, 0.0, 1.35)
    except Exception:
        return 0.0


def _candidate_anchors_from_liquidity(snap: Any, side: str, entry: float, atr: float) -> List[Tuple[float, float, str]]:
    """Return candidate impulse anchors as (quality, anchor_price, label)."""
    out: List[Tuple[float, float, str]] = []
    atr = max(float(atr or 0.0), 1e-9)
    if side == "long":
        pools = _iter_pool_targets(snap, "ssl_pools")
        for t in pools:
            px = _pool_price(t)
            if px <= 0 or px >= entry:
                continue
            dist_atr = abs(entry - px) / atr
            if dist_atr > 10.0:
                continue
            q = _pool_quality(t) * (1.0 / (1.0 + max(0.0, dist_atr - 1.5) / 4.0))
            out.append((q, px, "opposing_ssl_anchor"))
    else:
        pools = _iter_pool_targets(snap, "bsl_pools")
        for t in pools:
            px = _pool_price(t)
            if px <= 0 or px <= entry:
                continue
            dist_atr = abs(entry - px) / atr
            if dist_atr > 10.0:
                continue
            q = _pool_quality(t) * (1.0 / (1.0 + max(0.0, dist_atr - 1.5) / 4.0))
            out.append((q, px, "opposing_bsl_anchor"))
    out.sort(key=lambda x: x[0], reverse=True)
    return out[:4]


def _fib_level(side: str, entry: float, anchor: float, ratio: float) -> float:
    impulse = abs(entry - anchor)
    if side == "long":
        return entry + impulse * ratio
    return entry - impulse * ratio


def score_liquidity_fib_confluence(
    *,
    snap: Any,
    side: str,
    entry: float,
    sl: float,
    target_price: float,
    atr: float,
    target: Any = None,
) -> FibGeometryScore:
    """Soft-score Fibonacci geometry for an existing liquidity TP candidate.

    Returns a multiplier generally in [0.90, 1.32].  The lower bound is a small
    downgrade for poor auction geometry; there is no hard veto and no synthetic
    Fib target creation.
    """
    side = str(side or "").lower()
    entry = _num(entry, 0.0)
    sl = _num(sl, 0.0)
    target_price = _num(target_price, 0.0)
    atr = max(_num(atr, 0.0), 1e-9)
    if side not in ("long", "short") or entry <= 0 or target_price <= 0:
        return FibGeometryScore(0.0, 1.0, 0.0, 99.0, 0.0, 0.0, 0.0, "unavailable", reason="invalid inputs")
    if side == "long" and target_price <= entry:
        return FibGeometryScore(0.0, 1.0, 0.0, 99.0, 0.0, 0.0, 0.0, "unavailable", reason="target not above long entry")
    if side == "short" and target_price >= entry:
        return FibGeometryScore(0.0, 1.0, 0.0, 99.0, 0.0, 0.0, 0.0, "unavailable", reason="target not below short entry")

    candidates: List[Tuple[float, float, str]] = []
    # Primary structural anchor: original protective SL/invalidation.
    if sl > 0 and ((side == "long" and sl < entry) or (side == "short" and sl > entry)):
        risk_atr = abs(entry - sl) / atr
        q = _clamp(0.62 + 0.10 * math.log1p(max(risk_atr, 0.0)), 0.45, 0.95)
        candidates.append((q, sl, "original_invalidation"))
    # Liquidity-derived anchors: nearest/strongest opposing liquidity behind entry.
    candidates.extend(_candidate_anchors_from_liquidity(snap, side, entry, atr))
    # Fallback: use a conservative one-ATR impulse anchor. This produces a neutral
    # multiplier unless the target is genuinely aligned; it never creates a target.
    fallback_anchor = entry - atr if side == "long" else entry + atr
    candidates.append((0.25, fallback_anchor, "atr_fallback"))

    best: Optional[FibGeometryScore] = None
    target_q = _pool_quality(target) if target is not None else 0.65
    for anchor_quality, anchor, label in candidates:
        impulse = abs(entry - anchor)
        impulse_atr = impulse / atr
        if impulse_atr < 0.18:
            continue
        # Dynamic tolerance widens with impulse and pool quality, but remains ATR-
        # anchored so it does not become a fixed percent of price.
        tol_atr = _clamp(0.12 + 0.035 * math.sqrt(max(impulse_atr, 0.0)) + 0.025 * target_q, 0.12, 0.42)
        level_scores: List[Tuple[float, float, float, str]] = []
        for ratio in FIB_EXTENSION_LEVELS:
            lvl = _fib_level(side, entry, anchor, ratio)
            dist_atr = abs(target_price - lvl) / atr
            proximity = math.exp(-0.5 * (dist_atr / max(tol_atr, 1e-9)) ** 2)
            # Institutional bias: 0.618/1.0 are monetisation geometry; 1.272+
            # are runner geometry.  Do not let exotic ratios dominate.
            if ratio in _MONETISATION_LEVELS:
                ratio_quality = 0.92 if ratio in (0.618, 0.786, 1.0) else 0.78
                role = "internal_monetisation"
            elif ratio in _RUNNER_LEVELS:
                ratio_quality = 1.00 if ratio in (1.272, 1.618, 2.0) else 0.84
                role = "runner_projection"
            else:
                ratio_quality = 0.72
                role = "auction_projection"
            score = proximity * ratio_quality * _clamp(anchor_quality, 0.15, 1.25) * _clamp(0.75 + 0.25 * target_q, 0.75, 1.10)
            level_scores.append((score, ratio, dist_atr, role))
        score, ratio, dist_atr, role = max(level_scores, key=lambda x: x[0])
        # Soft multiplier: no hard gate.  Poor geometry downgrades slightly;
        # strong geometry upgrades but cannot overpower liquidity/probability.
        multiplier = _clamp(0.94 + 0.34 * _clamp(score, 0.0, 1.0), 0.90, 1.32)
        candidate = FibGeometryScore(
            score=_clamp(score, 0.0, 1.0),
            multiplier=multiplier,
            nearest_ratio=ratio,
            distance_atr=dist_atr,
            anchor_price=anchor,
            impulse_atr=impulse_atr,
            tolerance_atr=tol_atr,
            role=role,
            components={"anchor_quality": anchor_quality, "target_quality": target_q},
            reason=f"{label} → Fib {ratio:.3g} ({role}) dist={dist_atr:.2f}ATR",
        )
        if best is None or candidate.score > best.score:
            best = candidate

    if best is None:
        return FibGeometryScore(0.0, 1.0, 0.0, 99.0, 0.0, 0.0, 0.0, "unavailable", reason="no usable fib impulse anchor")
    return best

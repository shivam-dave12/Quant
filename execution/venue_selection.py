"""Cross-venue cost and route selection.

The selector compares executable venue microstates without merging their books.
It prices the trade against each venue's own top-of-book, near-touch depth,
fee assumption, feed quality, and same-underlying basis.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from statistics import median
from typing import Any, Mapping

try:
    import config
except Exception:  # pragma: no cover
    config = None  # type: ignore

from market_data.normalizer import VenueMicrostate


def _cfg(name: str, default: Any) -> Any:
    return getattr(config, name, default) if config is not None else default


def _direction_key(direction: Any) -> str:
    return str(getattr(direction, "value", direction) or "").upper()


def _fee_bps(venue: str) -> float:
    raw = _cfg("VENUE_FEE_BPS", {})
    if isinstance(raw, Mapping):
        try:
            return float(raw.get(str(venue).lower(), raw.get(str(venue).upper(), 1.5)) or 0.0)
        except Exception:
            return 1.5
    return 1.5


def _near_depth(state: VenueMicrostate, side: str) -> float:
    bands = state.ask_depth_usd_by_band if side == "buy" else state.bid_depth_usd_by_band
    return max(0.0, float(bands.get("0-1", 0.0) or 0.0) + float(bands.get("1-3", 0.0) or 0.0))


@dataclass(frozen=True)
class VenueCostEstimate:
    venue: str
    symbol: str
    routeable: bool
    total_cost_bps: float
    effective_touch_bps: float
    fee_bps: float
    impact_bps: float
    quality_penalty_bps: float
    liquidity_penalty_bps: float
    preference_adjustment_bps: float
    near_depth_usd: float
    mid: float
    reason: str = "ok"

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class VenueSelection:
    selected_venue: str
    selected_symbol: str
    selected_cost_bps: float
    current_venue: str
    current_cost_bps: float | None
    improvement_bps: float
    estimates: dict[str, VenueCostEstimate]
    reason: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "selected_venue": self.selected_venue,
            "selected_symbol": self.selected_symbol,
            "selected_cost_bps": self.selected_cost_bps,
            "current_venue": self.current_venue,
            "current_cost_bps": self.current_cost_bps,
            "improvement_bps": self.improvement_bps,
            "reason": self.reason,
            "estimates": {k: v.as_dict() for k, v in self.estimates.items()},
        }


def estimate_venue_cost(
    *,
    state: VenueMicrostate,
    direction: Any,
    asset_id: str,
    reference_mid: float,
    notional_usd: float,
    routeable: bool,
) -> VenueCostEstimate:
    venue = str(state.venue or "").lower()
    side = "buy" if _direction_key(direction) in {"LONG", "BULLISH", "BUY"} else "sell"
    depth = _near_depth(state, side)
    ref = max(float(reference_mid or 0.0), 1e-9)
    if side == "buy":
        effective_touch_bps = (float(state.best_ask) / ref - 1.0) * 10_000.0
    else:
        effective_touch_bps = (1.0 - float(state.best_bid) / ref) * 10_000.0
    fee = _fee_bps(venue)
    impact_mult = float(_cfg("VENUE_SLIPPAGE_IMPACT_MULTIPLIER", 35.0))
    impact = impact_mult * max(0.0, float(notional_usd or 0.0)) / max(depth, 1.0)
    quality_penalty = max(0.0, 1.0 - float(state.feed_quality_score or 0.0)) * 8.0
    liquidity_penalty = 0.0
    if notional_usd > 0 and depth < notional_usd:
        liquidity_penalty = (1.0 - depth / max(notional_usd, 1.0)) * 20.0
    preference = 0.0
    if str(asset_id or "").upper() == "SILVER":
        if venue == "hyperliquid":
            preference -= float(_cfg("SILVER_HYPERLIQUID_PREFERENCE_BPS", 8.0))
        if venue == "delta" and depth < float(_cfg("SILVER_DELTA_MIN_NEAR_DEPTH_USD", 50000.0)):
            preference += float(_cfg("SILVER_DELTA_ILLIQUIDITY_PENALTY_BPS", 15.0))
    total = effective_touch_bps + fee + impact + quality_penalty + liquidity_penalty + preference
    return VenueCostEstimate(
        venue=venue,
        symbol=str(state.symbol or ""),
        routeable=bool(routeable and state.execution_enabled and state.usable_for_decision),
        total_cost_bps=float(total),
        effective_touch_bps=float(effective_touch_bps),
        fee_bps=float(fee),
        impact_bps=float(impact),
        quality_penalty_bps=float(quality_penalty),
        liquidity_penalty_bps=float(liquidity_penalty),
        preference_adjustment_bps=float(preference),
        near_depth_usd=float(depth),
        mid=float(state.mid),
        reason="ok" if state.usable_for_decision else "microstate_unusable",
    )


def select_execution_venue(
    *,
    states: Mapping[str, VenueMicrostate],
    direction: Any,
    asset_id: str,
    current_venue: str,
    routeable_venues: set[str],
    notional_usd: float,
) -> VenueSelection:
    current = str(current_venue or "").lower()
    usable = [
        s for s in states.values()
        if isinstance(s, VenueMicrostate) and float(s.mid or 0.0) > 0 and s.usable_for_decision
    ]
    if not usable:
        return VenueSelection(current, "", math.inf, current, None, 0.0, {}, "no_usable_venue_microstates")
    ref_mid = median([float(s.mid) for s in usable])
    estimates: dict[str, VenueCostEstimate] = {}
    for key, state in states.items():
        if not isinstance(state, VenueMicrostate):
            continue
        venue = str(key or state.venue or "").lower()
        estimates[venue] = estimate_venue_cost(
            state=state,
            direction=direction,
            asset_id=asset_id,
            reference_mid=ref_mid,
            notional_usd=notional_usd,
            routeable=venue in routeable_venues,
        )
    candidates = [e for e in estimates.values() if e.routeable]
    if not candidates:
        cur = estimates.get(current)
        return VenueSelection(
            current,
            cur.symbol if cur else "",
            cur.total_cost_bps if cur else math.inf,
            current,
            cur.total_cost_bps if cur else None,
            0.0,
            estimates,
            "no_routeable_cost_candidate",
        )
    best = min(candidates, key=lambda e: e.total_cost_bps)
    cur = estimates.get(current)
    improvement = (cur.total_cost_bps - best.total_cost_bps) if cur is not None else 0.0
    min_improvement = float(_cfg("VENUE_SELECTION_MIN_IMPROVEMENT_BPS", 0.50))
    if cur is not None and cur.routeable and best.venue != current and improvement < min_improvement:
        best = cur
        improvement = 0.0
        reason = "current_venue_within_switch_threshold"
    else:
        reason = "best_cost_route_selected"
    max_cost = float(_cfg("VENUE_SELECTION_MAX_COST_BPS", 100.0))
    if best.total_cost_bps > max_cost:
        reason = f"selected_cost_above_soft_limit:{best.total_cost_bps:.2f}>{max_cost:.2f}"
    return VenueSelection(
        selected_venue=best.venue,
        selected_symbol=best.symbol,
        selected_cost_bps=best.total_cost_bps,
        current_venue=current,
        current_cost_bps=cur.total_cost_bps if cur else None,
        improvement_bps=improvement,
        estimates=estimates,
        reason=reason,
    )


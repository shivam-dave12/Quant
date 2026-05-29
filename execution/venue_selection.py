"""Institutional executable-venue selection.

Books are never merged.  Each venue is independently priced using the actual
side of the trade, visible depth, round-trip cost assumptions, observed feed
latency, funding/carry when supplied by the feed, available collateral and
hard-protection capability. For trade approval, candidate venues are priced at
one risk-normalised base quantity from one decision snapshot; broker collateral
is an eligibility constraint, not a score-enhancing reason to test a larger
order on one venue than another.
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


def _mapping_num(name: str, venue: str, default: float) -> float:
    raw = _cfg(name, {})
    if isinstance(raw, Mapping):
        try:
            return float(raw.get(str(venue).lower(), raw.get(str(venue).upper(), default)) or 0.0)
        except Exception:
            return float(default)
    return float(default)


def _round_trip_fee_bps(venue: str, state: VenueMicrostate | None = None) -> float:
    # Prefer exchange-confirmed instrument/account economics carried with the
    # live microstate. Fallback config is used only when a venue does not expose
    # a fee field in the currently wired metadata response.
    if state is not None and isinstance(getattr(state, "metadata", None), Mapping):
        raw = state.metadata.get("round_trip_fee_bps")
        try:
            if raw is not None and float(raw) >= 0:
                return float(raw)
        except (TypeError, ValueError):
            pass
    round_trip = _cfg("VENUE_ROUND_TRIP_FEE_BPS", None)
    if isinstance(round_trip, Mapping):
        return _mapping_num("VENUE_ROUND_TRIP_FEE_BPS", venue, 0.0)
    # Backward-compatible fallback for old configurations that stored a single
    # expected transaction cost rather than entry + protected-exit cost.
    return _mapping_num("VENUE_FEE_BPS", venue, 1.5)


def _near_depth(state: VenueMicrostate, side: str) -> float:
    bands = state.ask_depth_usd_by_band if side == "buy" else state.bid_depth_usd_by_band
    return max(0.0, float(bands.get("0-1", 0.0) or 0.0) + float(bands.get("1-3", 0.0) or 0.0))


def _funding_cost_bps(state: VenueMicrostate, direction: Any) -> float:
    """Charge only adverse expected carry unless credit is explicitly enabled.

    ``funding_rate`` is expected in decimal rate units as provided by venue
    tickers (for example 0.0001 = 1bp for one settlement interval).
    """
    if state.funding_rate is None:
        return 0.0
    interval_hours = max(1e-9, float(_cfg("VENUE_FUNDING_INTERVAL_HOURS", 8.0)))
    holding_hours = max(0.0, float(_cfg("VENUE_EXPECTED_HOLDING_HOURS", interval_hours)))
    rate_bps = float(state.funding_rate) * 10_000.0 * holding_hours / interval_hours
    signed_cost = rate_bps if _direction_key(direction) in {"LONG", "BULLISH", "BUY"} else -rate_bps
    if bool(_cfg("VENUE_ALLOW_FUNDING_CREDIT", False)):
        cap = max(0.0, float(_cfg("VENUE_MAX_FUNDING_CREDIT_BPS", 5.0)))
        return max(-cap, signed_cost)
    return max(0.0, signed_cost)


def _latency_penalty_bps(state: VenueMicrostate) -> float:
    if state.update_latency_ms is None:
        return 0.0
    tolerated = max(0.0, float(_cfg("VENUE_LATENCY_TOLERANCE_MS", 250.0)))
    per_second = max(0.0, float(_cfg("VENUE_LATENCY_PENALTY_BPS_PER_SEC", 2.0)))
    return max(0.0, float(state.update_latency_ms) - tolerated) * per_second / 1000.0


@dataclass(frozen=True)
class VenueCostEstimate:
    venue: str
    symbol: str
    routeable: bool
    total_cost_bps: float
    effective_touch_bps: float
    fee_bps: float
    impact_bps: float
    funding_cost_bps: float
    latency_penalty_bps: float
    quality_penalty_bps: float
    liquidity_penalty_bps: float
    protection_activation_penalty_bps: float
    preference_adjustment_bps: float
    near_depth_usd: float
    mid: float
    relative_touch_bps_diagnostic: float = 0.0
    proposed_notional_usd: float = 0.0
    gross_edge_bps: float | None = None
    expected_net_edge_bps: float | None = None
    expected_net_profit_usd: float | None = None
    available_cash_usd: float | None = None
    required_margin_usd: float = 0.0
    capital_feasible: bool = True
    protection_capable: bool = True
    reason: str = "ok"

    def as_dict(self) -> dict[str, Any]:
        out = asdict(self)
        out["cost_authority"] = "full_cycle_route_reserve_round_trip_fee_included"
        out["round_trip_fee_included"] = True
        return out


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
    selection_mode: str = "BROKER_LOCAL_CAPACITY"
    comparison_quantity: float = 0.0
    comparison_notional_usd: float = 0.0
    snapshot_ts_ns: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "selected_venue": self.selected_venue,
            "selected_symbol": self.selected_symbol,
            "selected_cost_bps": self.selected_cost_bps,
            "current_venue": self.current_venue,
            "current_cost_bps": self.current_cost_bps,
            "improvement_bps": self.improvement_bps,
            "reason": self.reason,
            "selection_mode": self.selection_mode,
            "comparison_quantity": self.comparison_quantity,
            "comparison_notional_usd": self.comparison_notional_usd,
            "snapshot_ts_ns": self.snapshot_ts_ns,
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
    available_cash_usd: float | None = None,
    required_margin_usd: float = 0.0,
    protection_capable: bool = True,
    gross_edge_bps: float | None = None,
) -> VenueCostEstimate:
    venue = str(state.venue or "").lower()
    side = "buy" if _direction_key(direction) in {"LONG", "BULLISH", "BUY"} else "sell"
    depth = _near_depth(state, side)
    ref = max(float(reference_mid or 0.0), 1e-9)
    # Cross-venue price dislocation is not an execution-cost credit. A cheaper
    # quoted instrument may be a different economic exposure (as SLVON vs XAG
    # demonstrated live). Route cost is therefore local and non-negative; any
    # relative touch difference is kept as diagnostic/research state only.
    if side == "buy":
        relative_touch_bps = (float(state.best_ask) / ref - 1.0) * 10_000.0
    else:
        relative_touch_bps = (1.0 - float(state.best_bid) / ref) * 10_000.0
    effective_touch_bps = max(0.0, float(state.spread_bps or 0.0))
    fee = _round_trip_fee_bps(venue, state)
    impact_mult = float(_cfg("VENUE_SLIPPAGE_IMPACT_MULTIPLIER", 35.0))
    impact = impact_mult * max(0.0, float(notional_usd or 0.0)) / max(depth, 1.0)
    funding = _funding_cost_bps(state, direction)
    latency = _latency_penalty_bps(state)
    quality_penalty = max(0.0, 1.0 - float(state.feed_quality_score or 0.0)) * 8.0
    liquidity_penalty = 0.0
    if notional_usd > 0 and depth < notional_usd:
        liquidity_penalty = (1.0 - depth / max(notional_usd, 1.0)) * 20.0
    # Some exchanges expose only post-fill position-level SL/TP rather than an
    # atomic attached-entry bracket. Charge a conservative policy reserve for
    # that activation window; it is a cost reserve, not a signal filter.
    protection_activation_penalty = _mapping_num("VENUE_NON_ATOMIC_PROTECTION_RISK_RESERVE_BPS", venue, 0.0)
    # No venue gets a permanent score advantage.  Asset-specific overrides are
    # retained only as explicit risk penalties (default zero in live config).
    preference = 0.0
    if str(asset_id or "").upper() in {"SILVER", "SILVER_SLVON"} and venue == "delta" and depth < float(_cfg("SILVER_DELTA_MIN_NEAR_DEPTH_USD", 50000.0)):
        preference += float(_cfg("SILVER_DELTA_ILLIQUIDITY_PENALTY_BPS", 0.0))
    cash_checked = available_cash_usd is not None
    capital_feasible = (not cash_checked) or float(available_cash_usd or 0.0) >= max(0.0, float(required_margin_usd or 0.0))
    reasons: list[str] = []
    if not routeable:
        reasons.append("route_not_authorised")
    if not state.execution_enabled:
        reasons.append("market_data_only")
    if not state.usable_for_decision:
        reasons.append("microstate_unusable")
    if not protection_capable:
        reasons.append("hard_protection_unavailable")
    if not capital_feasible:
        reasons.append("insufficient_venue_collateral")
    eligible = bool(routeable and state.execution_enabled and state.usable_for_decision and protection_capable and capital_feasible)
    total = max(0.0, effective_touch_bps + fee + impact + funding + latency + quality_penalty + liquidity_penalty + protection_activation_penalty + preference)
    expected_net_edge = None if gross_edge_bps is None else float(gross_edge_bps) - float(total)
    expected_net_profit = None if expected_net_edge is None else float(notional_usd or 0.0) * expected_net_edge / 10_000.0
    return VenueCostEstimate(
        venue=venue,
        symbol=str(state.symbol or ""),
        routeable=eligible,
        total_cost_bps=float(total),
        effective_touch_bps=float(effective_touch_bps),
        fee_bps=float(fee),
        impact_bps=float(impact),
        funding_cost_bps=float(funding),
        latency_penalty_bps=float(latency),
        quality_penalty_bps=float(quality_penalty),
        liquidity_penalty_bps=float(liquidity_penalty),
        protection_activation_penalty_bps=float(protection_activation_penalty),
        preference_adjustment_bps=float(preference),
        near_depth_usd=float(depth),
        mid=float(state.mid),
        relative_touch_bps_diagnostic=float(relative_touch_bps),
        proposed_notional_usd=float(notional_usd or 0.0),
        gross_edge_bps=None if gross_edge_bps is None else float(gross_edge_bps),
        expected_net_edge_bps=expected_net_edge,
        expected_net_profit_usd=expected_net_profit,
        available_cash_usd=None if available_cash_usd is None else float(available_cash_usd),
        required_margin_usd=float(required_margin_usd or 0.0),
        capital_feasible=bool(capital_feasible),
        protection_capable=bool(protection_capable),
        reason="ok" if not reasons else ";".join(reasons),
    )


def select_execution_venue(
    *,
    states: Mapping[str, VenueMicrostate],
    direction: Any,
    asset_id: str,
    current_venue: str,
    routeable_venues: set[str],
    notional_usd: float,
    available_cash_by_venue: Mapping[str, float] | None = None,
    required_margin_usd: float = 0.0,
    protection_capable_venues: set[str] | None = None,
    gross_edge_bps: float | None = None,
    gross_edge_by_venue: Mapping[str, float] | None = None,
    notional_by_venue: Mapping[str, float] | None = None,
    required_margin_by_venue: Mapping[str, float] | None = None,
    comparison_quantity: float = 0.0,
    comparison_notional_usd: float = 0.0,
    snapshot_ts_ns: int = 0,
) -> VenueSelection:
    current = str(current_venue or "").lower()
    usable = [s for s in states.values() if isinstance(s, VenueMicrostate) and float(s.mid or 0.0) > 0 and s.usable_for_decision]
    if not usable:
        return VenueSelection(current, "", math.inf, current, None, 0.0, {}, "no_usable_venue_microstates")
    ref_mid = median([float(s.mid) for s in usable])
    estimates: dict[str, VenueCostEstimate] = {}
    for key, state in states.items():
        if not isinstance(state, VenueMicrostate):
            continue
        venue = str(key or state.venue or "").lower()
        cash = None if available_cash_by_venue is None else available_cash_by_venue.get(venue, 0.0)
        protected = True if protection_capable_venues is None else venue in protection_capable_venues
        # Never evaluate one broker using another broker's balance-derived size.
        # A Delta-funded benchmark must not reject or artificially penalise a
        # smaller but executable Hyperliquid/CoinSwitch route.
        venue_notional = float((notional_by_venue or {}).get(venue, notional_usd) or 0.0)
        venue_margin = float((required_margin_by_venue or {}).get(venue, required_margin_usd) or 0.0)
        venue_gross_edge = (
            gross_edge_by_venue.get(venue, gross_edge_bps)
            if isinstance(gross_edge_by_venue, Mapping) else gross_edge_bps
        )
        estimates[venue] = estimate_venue_cost(
            state=state,
            direction=direction,
            asset_id=asset_id,
            reference_mid=ref_mid,
            notional_usd=venue_notional,
            routeable=venue in routeable_venues,
            available_cash_usd=cash,
            required_margin_usd=venue_margin,
            protection_capable=protected,
            gross_edge_bps=venue_gross_edge,
        )
    candidates = [e for e in estimates.values() if e.routeable]
    if not candidates:
        cur = estimates.get(current)
        return VenueSelection(current, cur.symbol if cur else "", cur.total_cost_bps if cur else math.inf, current, cur.total_cost_bps if cur else None, 0.0, estimates, "no_funded_protected_route_candidate")
    # Institutional execution selection has two explicit modes. Approval uses
    # a single risk-normalised quantity sampled at one instant, so score by
    # expected net edge/cost at identical exposure. The legacy broker-capacity
    # mode remains available to callers that are estimating deployable capacity
    # rather than deciding a live route.
    risk_normalised = float(comparison_quantity or 0.0) > 0.0 or float(comparison_notional_usd or 0.0) > 0.0
    if gross_edge_bps is not None or isinstance(gross_edge_by_venue, Mapping):
        if risk_normalised:
            best = max(candidates, key=lambda e: (float(e.expected_net_edge_bps) if e.expected_net_edge_bps is not None else -math.inf, -e.total_cost_bps))
            reason = "highest_risk_normalised_expected_net_edge_route"
        else:
            best = max(candidates, key=lambda e: (float(e.expected_net_profit_usd) if e.expected_net_profit_usd is not None else -math.inf, -e.total_cost_bps))
            reason = "highest_broker_local_expected_net_profit_route"
    else:
        best = min(candidates, key=lambda e: e.total_cost_bps)
        reason = "best_risk_normalised_expected_cost_route" if risk_normalised else "best_funded_protected_expected_cost_route"
    cur = estimates.get(current)
    improvement = (cur.total_cost_bps - best.total_cost_bps) if cur is not None else 0.0
    min_improvement = float(_cfg("VENUE_SELECTION_MIN_IMPROVEMENT_BPS", 0.50))
    if gross_edge_bps is None and cur is not None and cur.routeable and best.venue != current and improvement < min_improvement:
        best = cur
        improvement = 0.0
        reason = "current_venue_within_switch_threshold"
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
        selection_mode="RISK_NORMALISED_COMMON_QUANTITY" if risk_normalised else "BROKER_LOCAL_CAPACITY",
        comparison_quantity=float(comparison_quantity or 0.0),
        comparison_notional_usd=float(comparison_notional_usd or 0.0),
        snapshot_ts_ns=int(snapshot_ts_ns or 0),
    )

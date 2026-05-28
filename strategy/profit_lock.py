"""Cost-aware protective profit-lock model.

No entry can be made risk-free: the initial stop represents real thesis
invalidation.  After favourable excursion is sufficient, this model computes a
net-positive stop trigger beyond entry after conservative fee, spread and
slippage reserves.  It is a protective floor under the configured execution
reserve; market gaps or slippage beyond that reserve can still produce a loss.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping

try:
    import config
except Exception:  # pragma: no cover
    config = None  # type: ignore


def _cfg(name: str, default: Any) -> Any:
    return getattr(config, name, default) if config is not None else default


def _num(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


@dataclass(frozen=True)
class ProfitLockAssessment:
    should_request: bool
    model: str
    side: str
    entry_price: float
    mark_price: float
    current_stop_price: float
    requested_stop_price: float
    favourable_excursion_bps: float
    initial_risk_bps: float
    route_cost_reserve_bps: float
    execution_slippage_reserve_bps: float
    minimum_locked_profit_bps: float
    protected_net_floor_bps: float
    activation_threshold_bps: float
    locked_price_move_bps: float
    execution_envelope_only: bool
    reason: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class InstitutionalProfitLockEngine:
    """Calculate ratcheting net-positive protective stops after favourable move."""

    def __init__(self, asset_id: str) -> None:
        self.asset_id = str(asset_id or "").upper()

    def _slippage_reserve(self, spread_bps: float) -> float:
        by_asset = _cfg("PROFIT_LOCK_EXIT_SLIPPAGE_RESERVE_BPS_BY_ASSET", {})
        configured = _num(by_asset.get(self.asset_id), _cfg("PROFIT_LOCK_EXIT_SLIPPAGE_RESERVE_BPS", 5.0)) if isinstance(by_asset, Mapping) else _num(_cfg("PROFIT_LOCK_EXIT_SLIPPAGE_RESERVE_BPS", 5.0), 5.0)
        spread_mult = max(0.0, _num(_cfg("PROFIT_LOCK_SPREAD_RESERVE_MULTIPLIER", 1.5), 1.5))
        return max(configured, max(0.0, float(spread_bps)) * spread_mult)

    def assess(
        self,
        *,
        side: str,
        entry_price: float,
        mark_price: float,
        initial_stop_price: float,
        current_stop_price: float,
        route_cost_bps: float,
        spread_bps: float,
        protection_confirmed: bool,
    ) -> ProfitLockAssessment:
        raw_side = str(side or "").lower()
        sign = 1.0 if raw_side == "long" else -1.0 if raw_side == "short" else 0.0
        entry = float(entry_price or 0.0)
        mark = float(mark_price or 0.0)
        current_stop = float(current_stop_price or 0.0)
        initial_stop = float(initial_stop_price or current_stop or 0.0)
        if sign == 0.0 or entry <= 0.0 or mark <= 0.0 or current_stop <= 0.0:
            return ProfitLockAssessment(False, "cost_aware_profit_lock_v1", raw_side, entry, mark, current_stop, current_stop, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, True, "profit_lock_geometry_unavailable")
        favourable_bps = sign * (mark / entry - 1.0) * 10_000.0
        initial_risk_bps = abs(initial_stop / entry - 1.0) * 10_000.0
        cost_multiplier = max(1.0, _num(_cfg("PROFIT_LOCK_ROUTE_COST_RESERVE_MULTIPLIER", 2.0), 2.0))
        cost_reserve = max(0.0, float(route_cost_bps)) * cost_multiplier
        slippage_reserve = self._slippage_reserve(spread_bps)
        minimum_profit = max(0.0, _num(_cfg("PROFIT_LOCK_MIN_NET_PROFIT_BPS", 2.0), 2.0))
        floor_bps = cost_reserve + slippage_reserve + minimum_profit
        min_r = max(0.0, _num(_cfg("PROFIT_LOCK_MIN_ACTIVATION_R", 1.0), 1.0))
        arm_buffer = max(0.0, _num(_cfg("PROFIT_LOCK_ACTIVATION_BUFFER_BPS", 2.0), 2.0))
        activation = max(initial_risk_bps * min_r, floor_bps + slippage_reserve + arm_buffer)
        capture_fraction = min(0.90, max(0.05, _num(_cfg("PROFIT_LOCK_CAPTURE_FRACTION", 0.45), 0.45)))
        locked_bps = max(floor_bps, favourable_bps * capture_fraction)
        # Ensure a protective trigger remains on the safe side of the current mark
        # after a gap/slippage reserve; otherwise submitting it could immediately fire.
        locked_bps = min(locked_bps, favourable_bps - slippage_reserve - arm_buffer)
        requested_stop = entry * (1.0 + sign * locked_bps / 10_000.0) if locked_bps > 0.0 else current_stop
        current_locked_bps = sign * (current_stop / entry - 1.0) * 10_000.0
        min_update = max(0.0, _num(_cfg("PROFIT_LOCK_MIN_UPDATE_STEP_BPS", 2.0), 2.0))
        tightens = locked_bps >= max(floor_bps, current_locked_bps + min_update)
        if not protection_confirmed:
            reason = "native_protection_not_confirmed"
            request = False
        elif favourable_bps < activation:
            reason = f"favourable_excursion_below_profit_lock_activation:{favourable_bps:.3f}<{activation:.3f}"
            request = False
        elif locked_bps < floor_bps:
            reason = f"profit_floor_not_executable_after_reserve:{locked_bps:.3f}<{floor_bps:.3f}"
            request = False
        elif not tightens:
            reason = "profit_lock_already_at_or_above_requested_floor"
            request = False
        else:
            reason = "net_positive_stop_floor_available_under_execution_reserve"
            request = True

        if not request:
            # An unarmed assessment is telemetry only. Expose the currently
            # attached native stop as the actionable requested price so no
            # downstream component interprets a hypothetical floor as sent.
            requested_stop = current_stop

        return ProfitLockAssessment(
            request, "cost_aware_profit_lock_v1", raw_side, entry, mark, current_stop, requested_stop,
            favourable_bps, initial_risk_bps, cost_reserve, slippage_reserve, minimum_profit,
            floor_bps, activation, locked_bps if locked_bps > 0.0 else 0.0, True, reason,
        )

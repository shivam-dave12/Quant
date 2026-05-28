"""Post-fill calculation-performance supervision for protected positions.

This model does not place an entry and does not cancel native protection.  It
allows a reduce-only early close request only after repeated, currently
observable pre-move hazard evidence says that a filled trade has become toxic
or that already-net-profitable alpha is about to be given back.

Unlike the retired timer exit, this logic requires actual P&L state, executable
cost reserve and repeated opposite-flow hazard; elapsed time alone is never an
exit signal.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import time
from typing import Any

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
class PositionPerformanceAssessment:
    should_exit: bool
    action: str
    model: str
    side: str
    mark_move_bps: float
    initial_risk_bps: float
    all_in_unwind_reserve_bps: float
    net_mark_after_reserve_bps: float
    same_direction_probability: float | None
    opposing_direction_probability: float | None
    required_opposing_probability: float
    maximum_retain_probability: float
    adverse_trigger_bps: float
    profit_capture_trigger_bps: float
    confirmations: int
    required_confirmations: int
    protection_retained_until_flat: bool
    reasons: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class PostFillPerformanceExitEngine:
    """Confirm rapid risk exit/capture from live predictive deterioration."""

    def __init__(self, asset_id: str) -> None:
        self.asset_id = str(asset_id or "").upper()
        self._candidate_action = ""
        self._confirmations = 0
        self._last_confirmation_ts = 0.0

    def _reset(self) -> None:
        self._candidate_action = ""
        self._confirmations = 0
        self._last_confirmation_ts = 0.0

    def assess(
        self, *, side: str, entry_price: float, mark_price: float,
        initial_stop_price: float, route_cost_bps: float, spread_bps: float,
        protection_confirmed: bool, same_direction_probability: float | None,
        opposing_direction_probability: float | None,
        same_direction_ready: bool, opposing_direction_ready: bool,
    ) -> PositionPerformanceAssessment:
        raw_side = str(side or "").lower()
        sign = 1.0 if raw_side == "long" else -1.0 if raw_side == "short" else 0.0
        entry = float(entry_price or 0.0); mark = float(mark_price or 0.0); stop = float(initial_stop_price or 0.0)
        required_confirmations = max(2, int(_cfg("POST_FILL_PERFORMANCE_EXIT_MIN_CONFIRMATIONS", 2)))
        required_opposition = _num((_cfg("POST_FILL_PERFORMANCE_EXIT_MIN_OPPOSING_PROBABILITY_BY_ASSET", {}) or {}).get(self.asset_id), _cfg("POST_FILL_PERFORMANCE_EXIT_MIN_OPPOSING_PROBABILITY", 0.66))
        max_retain = _num(_cfg("POST_FILL_PERFORMANCE_EXIT_MAX_RETAIN_PROBABILITY", 0.48), 0.48)
        cost_mult = max(1.0, _num(_cfg("POST_FILL_PERFORMANCE_EXIT_ROUTE_COST_MULTIPLIER", 2.0), 2.0))
        slippage = _num((_cfg("POST_FILL_PERFORMANCE_EXIT_SLIPPAGE_RESERVE_BPS_BY_ASSET", {}) or {}).get(self.asset_id), _cfg("POST_FILL_PERFORMANCE_EXIT_SLIPPAGE_RESERVE_BPS", 5.0))
        spread_reserve = max(0.0, float(spread_bps)) * max(0.0, _num(_cfg("POST_FILL_PERFORMANCE_EXIT_SPREAD_RESERVE_MULTIPLIER", 1.0), 1.0))
        all_in_reserve = max(0.0, float(route_cost_bps)) * cost_mult + max(slippage, spread_reserve)
        if sign == 0.0 or entry <= 0.0 or mark <= 0.0 or stop <= 0.0:
            self._reset()
            return PositionPerformanceAssessment(False, "HOLD", "post_fill_predictive_performance_v1", raw_side, 0.0, 0.0, all_in_reserve, 0.0, same_direction_probability, opposing_direction_probability, required_opposition, max_retain, 0.0, 0.0, 0, required_confirmations, True, ("position_geometry_unavailable",))
        move_bps = sign * (mark / entry - 1.0) * 10_000.0
        risk_bps = abs(stop / entry - 1.0) * 10_000.0
        adverse_trigger = max(
            _num(_cfg("POST_FILL_PERFORMANCE_EXIT_MIN_ADVERSE_BPS", 2.0), 2.0),
            risk_bps * max(0.0, _num(_cfg("POST_FILL_PERFORMANCE_EXIT_ADVERSE_TRIGGER_R", 0.20), 0.20)),
        )
        profit_trigger = all_in_reserve + max(0.0, _num(_cfg("POST_FILL_PERFORMANCE_EXIT_MIN_CAPTURE_PROFIT_BPS", 2.0), 2.0))
        net_after_reserve = move_bps - all_in_reserve
        reasons: list[str] = []
        action = "HOLD"
        if not protection_confirmed:
            reasons.append("native_protection_not_confirmed")
        elif not (same_direction_ready and opposing_direction_ready):
            reasons.append("post_fill_predictive_hazard_not_ready")
        elif opposing_direction_probability is None or same_direction_probability is None:
            reasons.append("post_fill_probabilities_unavailable")
        elif opposing_direction_probability < required_opposition:
            reasons.append(f"opposing_hazard_not_confirmed:{opposing_direction_probability:.4f}<{required_opposition:.4f}")
        elif same_direction_probability > max_retain:
            reasons.append(f"retained_thesis_probability_not_collapsed:{same_direction_probability:.4f}>{max_retain:.4f}")
        elif move_bps <= -adverse_trigger:
            action = "CUT_CONFIRMED_POST_FILL_ADVERSE_SELECTION"
        elif net_after_reserve >= max(0.0, _num(_cfg("POST_FILL_PERFORMANCE_EXIT_MIN_CAPTURE_PROFIT_BPS", 2.0), 2.0)):
            action = "CAPTURE_NET_PROFIT_ON_PREDICTIVE_EDGE_REVERSAL"
        else:
            reasons.append("neither_loss_control_nor_net_profit_capture_threshold_met")
        if action == "HOLD":
            self._reset()
        else:
            now = time.monotonic()
            min_gap = max(0.0, _num(_cfg("POST_FILL_PERFORMANCE_EXIT_CONFIRMATION_INTERVAL_SEC", 0.25), 0.25))
            if action != self._candidate_action:
                self._candidate_action = action
                self._confirmations = 1
                self._last_confirmation_ts = now
            elif now - self._last_confirmation_ts >= min_gap:
                self._confirmations += 1
                self._last_confirmation_ts = now
            reasons.append(f"confirmed_predictive_performance_state:{action.lower()}")
        should_exit = action != "HOLD" and self._confirmations >= required_confirmations
        return PositionPerformanceAssessment(
            should_exit, action if action != "HOLD" else "HOLD", "post_fill_predictive_performance_v1", raw_side,
            move_bps, risk_bps, all_in_reserve, net_after_reserve, same_direction_probability,
            opposing_direction_probability, required_opposition, max_retain, adverse_trigger, profit_trigger,
            self._confirmations, required_confirmations, True, tuple(reasons),
        )

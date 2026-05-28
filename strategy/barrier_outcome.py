"""Protected-trade barrier outcome model with pre-displacement timing authority.

The structural engine owns trade direction.  The pre-move order-flow hazard
engine owns entry timing.  Realised favourable price response remains recorded
for post-entry toxicity/research only and is never required before initiating an
entry.
"""
from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import asdict, dataclass
import math
import time
from typing import Any, Mapping

from market_data.normalizer import VenueMicrostate
from strategy.domain import Direction, ProtectionPlan
from strategy.predictive_flow import PredictiveFlowAssessment

try:
    import config
except Exception:  # pragma: no cover
    config = None  # type: ignore


def _cfg(name: str, default: Any) -> Any:
    return getattr(config, name, default) if config is not None else default


def _num(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        return out if math.isfinite(out) else float(default)
    except Exception:
        return float(default)


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, float(value)))


def _direction_sign(direction: Direction) -> int:
    return 1 if direction in {Direction.LONG, Direction.BULLISH} else -1 if direction in {Direction.SHORT, Direction.BEARISH} else 0


@dataclass(frozen=True)
class FlowResponseEstimate:
    """Realised response telemetry; never a pre-entry hard gate in V20."""
    ready: bool
    sample_count: int
    direction: str
    flow_effectiveness: float
    continuation_probability: float
    absorption_probability: float
    aligned_flow_observations: int
    directional_mid_displacement_bps: float
    latest_directional_flow_bps: float
    reason: str


@dataclass(frozen=True)
class BarrierOutcomeAssessment:
    ready: bool
    approved: bool
    model: str
    direction: str
    stop_distance_bps: float
    target_distance_bps: float
    route_cost_bps: float
    round_trip_cost_reserve_bps: float
    parent_alpha_bps: float
    child_timing_contribution_bps: float
    uncertainty_bps: float
    robust_volatility_bps: float
    drift_after_predictive_timing_bps: float
    raw_target_before_stop_probability: float
    target_before_stop_probability: float
    required_target_before_stop_probability: float
    expected_value_bps: float
    predictive_flow: PredictiveFlowAssessment | None
    flow_response: FlowResponseEstimate
    reasons: tuple[str, ...]

    @property
    def drift_after_absorption_bps(self) -> float:  # compatibility telemetry alias
        return self.drift_after_predictive_timing_bps

    def as_dict(self) -> dict[str, Any]:
        out = asdict(self)
        out["drift_after_absorption_bps"] = self.drift_after_predictive_timing_bps
        out["entry_authority"] = "pre_move_orderflow_hazard_not_realised_price_confirmation"
        out["flow_response_role"] = "post_entry_toxicity_and_calibration_only"
        return out


@dataclass(frozen=True)
class _Snapshot:
    timestamp_s: float
    mid: float
    flow_bps: float
    spread_bps: float
    near_depth_usd: float


class ProtectedBarrierOutcomeEngine:
    """Evaluate TP-before-SL expectancy on an exact protected bracket.

    ``flow_response`` remains available for post-entry/adverse-selection study.
    Entry drift and entry permission are taken from predictive pre-displacement
    features supplied by :class:`PreMoveOrderFlowHazardEngine`.
    """

    def __init__(self, asset_id: str) -> None:
        self.asset_id = str(asset_id or "").upper()
        maxlen = max(20, int(_cfg("BARRIER_OUTCOME_MAX_OBSERVATIONS", 240)))
        self._snapshots: dict[str, deque[_Snapshot]] = defaultdict(lambda: deque(maxlen=maxlen))
        self._last_ts: dict[str, int] = {}

    @staticmethod
    def _near_depth(state: VenueMicrostate) -> float:
        return sum(float(state.bid_depth_usd_by_band.get(k, 0.0) + state.ask_depth_usd_by_band.get(k, 0.0)) for k in ("0-1", "1-3"))

    @staticmethod
    def _flow_bps(state: VenueMicrostate) -> float:
        depth = max(ProtectedBarrierOutcomeEngine._near_depth(state), 1e-9)
        ofi = float(state.ofi_usd_1s) + 0.50 * float(state.ofi_usd_10s)
        tfi = float(state.tfi_usd_1s) + 0.50 * float(state.tfi_usd_10s)
        mid = max(float(state.mid or 0.0), 1e-9)
        micro_bps = (float(state.microprice or mid) / mid - 1.0) * 10_000.0
        raw = (ofi / depth * 100.0) + 0.30 * (tfi / depth * 100.0) + 0.35 * micro_bps
        cap = max(1.0, float(_cfg("BARRIER_OUTCOME_FLOW_ALPHA_CAP_BPS", 30.0)))
        return cap * math.tanh(raw / cap)

    def observe(self, states: Mapping[str, VenueMicrostate]) -> None:
        now = time.monotonic()
        for venue, state in (states or {}).items():
            if not isinstance(state, VenueMicrostate) or not state.usable_for_decision or float(state.mid or 0.0) <= 0.0:
                continue
            key = str(venue).lower()
            event_ts = int(state.receive_ts_ns or 0)
            if event_ts and self._last_ts.get(key) == event_ts:
                continue
            self._last_ts[key] = event_ts
            self._snapshots[key].append(_Snapshot(now, float(state.mid), self._flow_bps(state), float(state.spread_bps or 0.0), self._near_depth(state)))

    def flow_response(self, *, venue: str, direction: Direction) -> FlowResponseEstimate:
        """Retained as realised-response telemetry, not live-entry permission."""
        sign = _direction_sign(direction)
        rows = list(self._snapshots.get(str(venue).lower(), ()))
        minimum = max(4, int(_cfg("BARRIER_OUTCOME_MIN_FLOW_OBSERVATIONS", 10)))
        if sign == 0:
            return FlowResponseEstimate(False, len(rows), direction.value, 0.0, 0.0, 1.0, 0, 0.0, 0.0, "post_entry_flow_direction_unavailable")
        if len(rows) < minimum:
            return FlowResponseEstimate(False, len(rows), direction.value, 0.0, 0.0, 1.0, 0, 0.0, 0.0, f"post_entry_flow_warmup:{len(rows)}/{minimum}")
        max_age = max(2.0, float(_cfg("BARRIER_OUTCOME_FLOW_WINDOW_SEC", 30.0)))
        latest_ts = rows[-1].timestamp_s
        rows = [r for r in rows if latest_ts - r.timestamp_s <= max_age]
        aligned_count = 0
        favourable = adverse = weight_total = 0.0
        for prior, current in zip(rows[:-1], rows[1:]):
            directional_flow = sign * prior.flow_bps
            if directional_flow <= 0.0 or prior.mid <= 0.0:
                continue
            displacement = sign * (current.mid / prior.mid - 1.0) * 10_000.0
            weight = max(0.01, abs(directional_flow))
            aligned_count += 1
            weight_total += weight
            favourable += weight * max(displacement, 0.0)
            adverse += weight * max(-displacement, 0.0)
        if aligned_count <= 0 or weight_total <= 0.0:
            return FlowResponseEstimate(False, len(rows), direction.value, 0.0, 0.25, 0.75, 0, sign * (rows[-1].mid / rows[0].mid - 1.0) * 10_000.0, sign * rows[-1].flow_bps, "no_post_entry_aligned_flow_response")
        total = favourable + adverse
        absorption = adverse / max(total, 1e-9) if total > 0.0 else 1.0
        effectiveness = favourable / max(weight_total, 1e-9)
        scale = max(0.25, float(_cfg("BARRIER_OUTCOME_EFFECTIVENESS_SCALE_BPS", 1.5)))
        continuation = _clamp(0.05 + 0.90 * (1.0 - absorption) * (1.0 - math.exp(-max(effectiveness, 0.0) / scale)), 0.05, 0.95)
        return FlowResponseEstimate(True, len(rows), direction.value, effectiveness, continuation, _clamp(absorption, 0.0, 1.0), aligned_count, sign * (rows[-1].mid / rows[0].mid - 1.0) * 10_000.0, sign * rows[-1].flow_bps, "realised_price_response_telemetry_only")

    @staticmethod
    def _first_passage_probability(*, lower_distance_bps: float, upper_distance_bps: float, drift_bps: float, variance_bps2: float) -> float:
        lower = max(1e-9, float(lower_distance_bps))
        upper = max(1e-9, float(upper_distance_bps))
        variance = max(1e-9, float(variance_bps2))
        mu = float(drift_bps)
        if abs(mu) <= 1e-9:
            return lower / (lower + upper)
        a = _clamp(-2.0 * mu * lower / variance, -700.0, 700.0)
        b = _clamp(-2.0 * mu * (lower + upper) / variance, -700.0, 700.0)
        numerator = 1.0 - math.exp(a)
        denominator = 1.0 - math.exp(b)
        return lower / (lower + upper) if abs(denominator) <= 1e-12 else _clamp(numerator / denominator, 0.0, 1.0)

    def assess(
        self,
        *,
        venue: str,
        direction: Direction,
        protection: ProtectionPlan,
        parent_alpha_bps: float,
        child_timing_contribution_bps: float,
        uncertainty_bps: float,
        route_cost_bps: float,
        robust_volatility_bps: float,
        liquidity_score: float,
        execution_quality: float,
        predictive_flow: PredictiveFlowAssessment | None = None,
    ) -> BarrierOutcomeAssessment:
        sign = _direction_sign(direction)
        entry = float(protection.entry_price or 0.0)
        realised = self.flow_response(venue=venue, direction=direction)
        if sign == 0 or entry <= 0.0:
            return BarrierOutcomeAssessment(False, False, "predictive_protected_barrier_outcome_v2", direction.value, 0.0, 0.0, route_cost_bps, 0.0, parent_alpha_bps, child_timing_contribution_bps, uncertainty_bps, robust_volatility_bps, 0.0, 0.0, 0.0, 1.0, -math.inf, predictive_flow, realised, ("barrier_geometry_unavailable",))
        stop_bps = sign * (entry - float(protection.stop_price)) / entry * 10_000.0
        target_bps = sign * (float(protection.target_price) - entry) / entry * 10_000.0
        round_trip_mult = max(1.0, float(_cfg("BARRIER_OUTCOME_ROUND_TRIP_COST_MULTIPLIER", 2.0)))
        all_in_cost = max(0.0, float(route_cost_bps)) * round_trip_mult
        signed_parent = sign * float(parent_alpha_bps)
        signed_child = sign * float(child_timing_contribution_bps)
        structural_drift = max(0.0, signed_parent + max(-signed_parent * 0.35, signed_child) - max(0.0, float(uncertainty_bps)))
        timing_probability = float(getattr(predictive_flow, "directional_move_probability", 0.5) if predictive_flow is not None else 0.5)
        timing_alpha = max(0.0, float(getattr(predictive_flow, "predictive_alpha_bps", 0.0) if predictive_flow is not None else 0.0))
        timing_multiplier = _clamp(0.40 + max(0.0, timing_probability - 0.50) * 2.0, 0.40, 1.25)
        drift = (structural_drift + 0.35 * timing_alpha) * timing_multiplier
        volatility = max(float(robust_volatility_bps or 0.0), float(_cfg("BARRIER_OUTCOME_MIN_VOLATILITY_BPS", 2.0)))
        raw_probability = self._first_passage_probability(lower_distance_bps=stop_bps, upper_distance_bps=target_bps, drift_bps=drift, variance_bps2=volatility * volatility) if stop_bps > 0.0 and target_bps > 0.0 else 0.0
        quality_haircut = float(_cfg("BARRIER_OUTCOME_LOW_QUALITY_PROBABILITY_HAIRCUT", 0.08)) * max(0.0, 1.0 - min(liquidity_score, execution_quality))
        probability = _clamp(raw_probability - quality_haircut, 0.0, 1.0)
        break_even_probability = (stop_bps + all_in_cost) / max(stop_bps + target_bps, 1e-9) if stop_bps > 0.0 and target_bps > 0.0 else 1.0
        per_asset = _cfg("BARRIER_OUTCOME_MIN_TARGET_BEFORE_STOP_PROBABILITY_BY_ASSET", {})
        asset_floor = _num(per_asset.get(self.asset_id), _num(_cfg("BARRIER_OUTCOME_MIN_TARGET_BEFORE_STOP_PROBABILITY", 0.58), 0.58)) if isinstance(per_asset, Mapping) else _num(_cfg("BARRIER_OUTCOME_MIN_TARGET_BEFORE_STOP_PROBABILITY", 0.58), 0.58)
        required_probability = _clamp(max(asset_floor, break_even_probability + max(0.0, float(_cfg("BARRIER_OUTCOME_BREAK_EVEN_PROBABILITY_RESERVE", 0.03)))), 0.0, 0.999)
        expected_value = probability * target_bps - (1.0 - probability) * stop_bps - all_in_cost
        minimum_ev = max(0.0, float(_cfg("BARRIER_OUTCOME_MIN_EXPECTED_VALUE_BPS", 1.0)))
        reasons: list[str] = []
        if stop_bps <= 0.0 or target_bps <= 0.0:
            reasons.append("barrier_geometry_invalid")
        if bool(_cfg("PREDICTIVE_FLOW_REQUIRE_READY_FOR_ENTRY", True)) and (predictive_flow is None or not predictive_flow.ready):
            reasons.append("pre_move_predictive_timing_unavailable")
        if predictive_flow is not None and predictive_flow.ready and not predictive_flow.approved:
            reasons.extend(list(predictive_flow.reasons))
        if probability < required_probability:
            reasons.append(f"target_before_stop_probability_insufficient:{probability:.4f}<{required_probability:.4f}")
        if expected_value < minimum_ev:
            reasons.append(f"barrier_expected_value_insufficient:{expected_value:.3f}<{minimum_ev:.3f}")
        approved = not reasons
        return BarrierOutcomeAssessment(
            ready=bool(predictive_flow is not None and predictive_flow.ready and stop_bps > 0.0 and target_bps > 0.0),
            approved=approved, model="predictive_protected_barrier_outcome_v2", direction=direction.value,
            stop_distance_bps=stop_bps, target_distance_bps=target_bps, route_cost_bps=float(route_cost_bps),
            round_trip_cost_reserve_bps=all_in_cost, parent_alpha_bps=float(parent_alpha_bps),
            child_timing_contribution_bps=float(child_timing_contribution_bps), uncertainty_bps=float(uncertainty_bps),
            robust_volatility_bps=volatility, drift_after_predictive_timing_bps=drift,
            raw_target_before_stop_probability=raw_probability, target_before_stop_probability=probability,
            required_target_before_stop_probability=required_probability, expected_value_bps=expected_value,
            predictive_flow=predictive_flow, flow_response=realised, reasons=tuple(reasons),
        )

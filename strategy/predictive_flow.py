"""Pre-displacement order-flow hazard model.

This module is deliberately separated from realised-response analytics.  It only
uses information available *before* an entry request is made: current displayed
queue state, current/short-baseline OFI and TFI pressure, microprice lean, and
changes in displayed depth across recent book updates.  It does not require the
midpoint to have already moved in the proposed direction.

Depth changes are explicitly labelled as *displayed-liquidity proxies*: without
venue order IDs they cannot be asserted as true cancellation or hidden refill.
They are still useful as observable queue-depletion/replenishment evidence.
"""
from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import asdict, dataclass
import math
import statistics
import time
from typing import Any, Mapping

from intelligence.venue_market_state import CrossVenueEvidence
from market_data.normalizer import VenueMicrostate
from strategy.domain import Direction

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


def _sign(direction: Direction) -> int:
    return 1 if direction in {Direction.LONG, Direction.BULLISH} else -1 if direction in {Direction.SHORT, Direction.BEARISH} else 0


def _sigmoid(value: float) -> float:
    value = _clamp(value, -40.0, 40.0)
    return 1.0 / (1.0 + math.exp(-value))


@dataclass(frozen=True)
class PredictiveFlowAssessment:
    ready: bool
    approved: bool
    model: str
    venue: str
    direction: str
    sample_count: int
    prediction_horizon_sec: float
    directional_move_probability: float
    required_directional_probability: float
    predictive_alpha_bps: float
    queue_imbalance_directional: float
    microprice_lean_directional_bps: float
    ofi_acceleration_directional_bps: float
    tfi_acceleration_directional_bps: float
    depletion_advantage: float
    displayed_support_replenishment_proxy: float
    displayed_opposition_withdrawal_proxy: float
    displayed_support_withdrawal_proxy: float
    displayed_opposition_replenishment_proxy: float
    near_bid_depth_usd: float
    near_ask_depth_usd: float
    cross_venue_agreement: float | None
    leader_venue: str | None
    evidence_authority: str
    reasons: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        out = asdict(self)
        out["probability_authority"] = "analytic_hazard_score_requires_walk_forward_calibration_for_live"
        return out


@dataclass(frozen=True)
class _FlowSnapshot:
    timestamp_s: float
    mid: float
    bid_near_usd: float
    ask_near_usd: float
    ofi_1s: float
    ofi_10s: float
    tfi_1s: float
    tfi_10s: float
    microprice_lean_bps: float
    spread_bps: float


class PreMoveOrderFlowHazardEngine:
    """Estimate imminent directional displacement before it is realised.

    This is an analytic observable-state model, not a fitted win-rate claim.  It
    is intended to be calibratable through stored decision/outcome records.  Live
    entry authority is fail-closed when current pre-move evidence is not ready or
    is not strong in the already-authorised parent direction.
    """

    def __init__(self, asset_id: str) -> None:
        self.asset_id = str(asset_id or "").upper()
        maxlen = max(20, int(_cfg("PREDICTIVE_FLOW_MAX_OBSERVATIONS", 240)))
        self._snapshots: dict[str, deque[_FlowSnapshot]] = defaultdict(lambda: deque(maxlen=maxlen))
        self._last_ts_ns: dict[str, int] = {}

    @staticmethod
    def _band_depth(state: VenueMicrostate, side: str) -> float:
        source = state.bid_depth_usd_by_band if side == "bid" else state.ask_depth_usd_by_band
        return (
            float(source.get("0-1", 0.0) or 0.0)
            + 0.50 * float(source.get("1-3", 0.0) or 0.0)
            + 0.15 * float(source.get("3-10", 0.0) or 0.0)
        )

    def observe(self, states: Mapping[str, VenueMicrostate]) -> None:
        now = time.monotonic()
        for venue, state in (states or {}).items():
            if not isinstance(state, VenueMicrostate) or not state.usable_for_decision or float(state.mid or 0.0) <= 0.0:
                continue
            key = str(venue).lower()
            event_ts = int(state.receive_ts_ns or 0)
            if event_ts and self._last_ts_ns.get(key) == event_ts:
                continue
            self._last_ts_ns[key] = event_ts
            mid = float(state.mid)
            self._snapshots[key].append(_FlowSnapshot(
                timestamp_s=now,
                mid=mid,
                bid_near_usd=max(0.0, self._band_depth(state, "bid")),
                ask_near_usd=max(0.0, self._band_depth(state, "ask")),
                ofi_1s=float(state.ofi_usd_1s or 0.0),
                ofi_10s=float(state.ofi_usd_10s or 0.0),
                tfi_1s=float(state.tfi_usd_1s or 0.0),
                tfi_10s=float(state.tfi_usd_10s or 0.0),
                microprice_lean_bps=(float(state.microprice or mid) / mid - 1.0) * 10_000.0,
                spread_bps=float(state.spread_bps or 0.0),
            ))

    def assess(
        self,
        *,
        venue: str,
        direction: Direction,
        cross_venue_evidence: CrossVenueEvidence | None = None,
    ) -> PredictiveFlowAssessment:
        sign = _sign(direction)
        key = str(venue or "").lower()
        rows = list(self._snapshots.get(key, ()))
        minimum = max(3, int(_cfg("PREDICTIVE_FLOW_MIN_OBSERVATIONS", 4)))
        horizon = max(0.05, float(_cfg("PREDICTIVE_FLOW_PREDICTION_HORIZON_SEC", 1.0)))
        required = _num((_cfg("PREDICTIVE_FLOW_MIN_DIRECTIONAL_PROBABILITY_BY_ASSET", {}) or {}).get(self.asset_id), _num(_cfg("PREDICTIVE_FLOW_MIN_DIRECTIONAL_PROBABILITY", 0.60), 0.60))
        if sign == 0:
            return PredictiveFlowAssessment(False, False, "pre_move_orderflow_hazard_v1", key, direction.value, len(rows), horizon, 0.0, required, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, None, None, "pre_move_observable_state_only", ("predictive_direction_unavailable",))
        if len(rows) < minimum:
            return PredictiveFlowAssessment(False, False, "pre_move_orderflow_hazard_v1", key, direction.value, len(rows), horizon, 0.0, required, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, rows[-1].bid_near_usd if rows else 0.0, rows[-1].ask_near_usd if rows else 0.0, None, None, "pre_move_observable_state_only", (f"predictive_flow_warmup:{len(rows)}/{minimum}",))
        window_sec = max(horizon, float(_cfg("PREDICTIVE_FLOW_FEATURE_WINDOW_SEC", 5.0)))
        latest_ts = rows[-1].timestamp_s
        rows = [r for r in rows if latest_ts - r.timestamp_s <= window_sec]
        if len(rows) < minimum:
            return PredictiveFlowAssessment(False, False, "pre_move_orderflow_hazard_v1", key, direction.value, len(rows), horizon, 0.0, required, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, rows[-1].bid_near_usd if rows else 0.0, rows[-1].ask_near_usd if rows else 0.0, None, None, "pre_move_observable_state_only", (f"predictive_recent_warmup:{len(rows)}/{minimum}",))

        latest = rows[-1]
        prior = rows[-2]
        bid = max(latest.bid_near_usd, 0.0)
        ask = max(latest.ask_near_usd, 0.0)
        total_depth = max(bid + ask, 1.0)
        queue_imbalance = sign * (bid - ask) / total_depth
        micro_lean = sign * latest.microprice_lean_bps

        prior_ofi_baseline = statistics.fmean([r.ofi_1s for r in rows[:-1]])
        prior_tfi_baseline = statistics.fmean([r.tfi_1s for r in rows[:-1]])
        # Convert current short-horizon acceleration into a bounded bps-like signal.
        ofi_accel = sign * (latest.ofi_1s - prior_ofi_baseline) / total_depth * 100.0
        tfi_accel = sign * (latest.tfi_1s - prior_tfi_baseline) / total_depth * 100.0

        bid_change = (latest.bid_near_usd - prior.bid_near_usd) / max(prior.bid_near_usd + latest.bid_near_usd, 1.0)
        ask_change = (latest.ask_near_usd - prior.ask_near_usd) / max(prior.ask_near_usd + latest.ask_near_usd, 1.0)
        if sign > 0:
            support_refill = max(0.0, bid_change)
            opposition_withdrawal = max(0.0, -ask_change)
            support_withdrawal = max(0.0, -bid_change)
            opposition_refill = max(0.0, ask_change)
            consumption = max(0.0, latest.tfi_1s) + max(0.0, latest.ofi_1s)
            resisting_queue = ask
            opposite_consumption = max(0.0, -latest.tfi_1s) + max(0.0, -latest.ofi_1s)
            supporting_queue = bid
        else:
            support_refill = max(0.0, ask_change)
            opposition_withdrawal = max(0.0, -bid_change)
            support_withdrawal = max(0.0, -ask_change)
            opposition_refill = max(0.0, bid_change)
            consumption = max(0.0, -latest.tfi_1s) + max(0.0, -latest.ofi_1s)
            resisting_queue = bid
            opposite_consumption = max(0.0, latest.tfi_1s) + max(0.0, latest.ofi_1s)
            supporting_queue = ask
        consume_rate = max(consumption + opposition_withdrawal * total_depth, 1e-6)
        opposing_rate = max(opposite_consumption + support_withdrawal * total_depth, 1e-6)
        time_to_break_direction = resisting_queue / consume_rate
        time_to_break_against = supporting_queue / opposing_rate
        depletion_advantage = _clamp((time_to_break_against - time_to_break_direction) / max(time_to_break_against + time_to_break_direction, 1e-9), -1.0, 1.0)

        cross_agreement: float | None = None
        leader: str | None = None
        cross_term = 0.0
        if cross_venue_evidence is not None:
            cross_agreement = float(cross_venue_evidence.agreement_score)
            leader = str(cross_venue_evidence.leader_venue or "") or None
            leader_sign = 0
            if leader:
                leader_alpha = float(cross_venue_evidence.signed_alpha_by_venue.get(leader, 0.0) or 0.0)
                leader_sign = 1 if leader_alpha > 0 else -1 if leader_alpha < 0 else 0
            cross_term = sign * leader_sign * cross_agreement

        weights = _cfg("PREDICTIVE_FLOW_SCORE_WEIGHTS", {}) or {}
        score = (
            _num(weights.get("queue_imbalance"), 0.90) * queue_imbalance
            + _num(weights.get("microprice"), 0.12) * micro_lean
            + _num(weights.get("ofi_acceleration"), 0.14) * ofi_accel
            + _num(weights.get("tfi_acceleration"), 0.10) * tfi_accel
            + _num(weights.get("depletion_advantage"), 1.20) * depletion_advantage
            + _num(weights.get("support_refill"), 0.80) * support_refill
            + _num(weights.get("opposition_withdrawal"), 0.80) * opposition_withdrawal
            - _num(weights.get("support_withdrawal"), 1.10) * support_withdrawal
            - _num(weights.get("opposition_refill"), 1.10) * opposition_refill
            + _num(weights.get("cross_venue"), 0.40) * cross_term
        )
        score_scale = max(0.05, _num(_cfg("PREDICTIVE_FLOW_LOGISTIC_SCORE_SCALE", 1.0), 1.0))
        probability = _clamp(_sigmoid(score / score_scale), 0.0, 1.0)
        alpha_cap = max(1.0, _num(_cfg("PREDICTIVE_FLOW_ALPHA_CAP_BPS", 12.0), 12.0))
        predictive_alpha = alpha_cap * math.tanh(score / max(0.25, score_scale))
        reasons: list[str] = []
        if probability < required:
            reasons.append(f"pre_move_probability_insufficient:{probability:.4f}<{required:.4f}")
        if micro_lean < -abs(_num(_cfg("PREDICTIVE_FLOW_MAX_ADVERSE_MICROPRICE_BPS", 0.25), 0.25)):
            reasons.append(f"pre_move_microprice_opposes_parent:{micro_lean:.4f}")
        if depletion_advantage < _num(_cfg("PREDICTIVE_FLOW_MIN_DEPLETION_ADVANTAGE", 0.02), 0.02):
            reasons.append(f"pre_move_depletion_advantage_insufficient:{depletion_advantage:.4f}")
        approved = not reasons
        return PredictiveFlowAssessment(
            True, approved, "pre_move_orderflow_hazard_v1", key, direction.value, len(rows), horizon,
            probability, required, predictive_alpha, queue_imbalance, micro_lean, ofi_accel, tfi_accel,
            depletion_advantage, support_refill, opposition_withdrawal, support_withdrawal, opposition_refill,
            bid, ask, cross_agreement, leader, "pre_move_observable_state_only_depth_changes_are_proxies", tuple(reasons),
        )

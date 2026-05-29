"""Append-only research store for decisions, executions, and labels."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field, is_dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping


def _json_safe(value: Any) -> Any:
    if is_dataclass(value):
        return _json_safe(asdict(value))
    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    return value


@dataclass(frozen=True)
class ResearchDecisionRecord:
    observation_ts_ns: int
    desk: str
    venue: str
    instrument: str
    candidate_id: str
    decision: str
    model_values: dict[str, Any]
    reasons: list[str]
    features: dict[str, Any]
    model_version: str | None = None
    policy_version: str | None = None


@dataclass(frozen=True)
class ResearchExecutionRecord:
    observation_ts_ns: int
    desk: str
    venue: str
    instrument: str
    candidate_id: str
    requested_order: dict[str, Any]
    actual_fill: dict[str, Any]
    protection_state: dict[str, Any]
    realised_costs: dict[str, Any]
    pnl_attribution: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DeltaForwardLabel:
    observation_ts_ns: int
    horizon: str
    side: str
    gross_markout_bps: float
    spread_cost_bps: float
    fee_cost_bps: float
    slippage_estimate_bps: float
    net_executable_return_bps: float
    favorable_excursion_bps: float
    adverse_excursion_bps: float


@dataclass(frozen=True)
class PredictiveBarrierLabel:
    observation_ts_ns: int
    resolved_ts_ns: int
    asset_id: str
    venue: str
    instrument: str
    candidate_id: str
    model_key: str
    setup_family: str
    side: str
    reference_entry_price: float
    stop_price: float
    target_price: float
    estimated_round_trip_cost_bps: float
    predicted_target_before_stop_probability: float
    predicted_directional_move_probability: float
    outcome: str
    resolution_price: float
    favorable_excursion_bps: float
    adverse_excursion_bps: float
    parent_state_id: str = ""
    prediction_authority: str = "uncalibrated_analytic_score_shadow_only"


class JsonlResearchStore:
    """Small deterministic JSONL store.

    It is intentionally append-only and schema-light so every candidate,
    rejection, execution, and forward label can be recorded from day one without
    coupling live trading code to a database server.
    """

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def append_decision(self, record: ResearchDecisionRecord) -> Path:
        return self._append("decisions.jsonl", record)

    def append_execution(self, record: ResearchExecutionRecord) -> Path:
        return self._append("executions.jsonl", record)

    def append_delta_forward_label(self, label: DeltaForwardLabel) -> Path:
        return self._append("delta_forward_labels.jsonl", label)

    def append_predictive_barrier_label(self, label: PredictiveBarrierLabel) -> Path:
        return self._append("predictive_barrier_labels.jsonl", label)

    def read_records(self, name: str) -> list[dict[str, Any]]:
        path = self.root / name
        if not path.exists():
            return []
        out: list[dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    out.append(json.loads(line))
        return out

    def _append(self, filename: str, payload: Any) -> Path:
        path = self.root / filename
        row = _json_safe(payload)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
        return path


def append_many(store: JsonlResearchStore, records: Iterable[Any]) -> list[Path]:
    paths: list[Path] = []
    for record in records:
        if isinstance(record, ResearchDecisionRecord):
            paths.append(store.append_decision(record))
        elif isinstance(record, ResearchExecutionRecord):
            paths.append(store.append_execution(record))
        elif isinstance(record, DeltaForwardLabel):
            paths.append(store.append_delta_forward_label(record))
        elif isinstance(record, PredictiveBarrierLabel):
            paths.append(store.append_predictive_barrier_label(record))
        else:
            raise TypeError(f"unsupported research record type: {type(record)!r}")
    return paths


@dataclass
class _PendingPredictiveBarrier:
    observation_ts_ns: int
    asset_id: str
    venue: str
    instrument: str
    candidate_id: str
    model_key: str
    setup_family: str
    side: str
    entry_price: float
    stop_price: float
    target_price: float
    estimated_round_trip_cost_bps: float
    predicted_target_before_stop_probability: float
    predicted_directional_move_probability: float
    timeout_s: int
    parent_state_id: str = ""
    prediction_authority: str = "uncalibrated_analytic_score_shadow_only"
    event_key: str = ""
    favorable_bps: float = 0.0
    adverse_bps: float = 0.0


class PredictiveBarrierLabelWriter:
    """Resolve every shadow/live predictive setup by exact TP-before-SL outcome."""
    def __init__(self, store: JsonlResearchStore, min_spacing_sec: float = 1.0, timeout_s: int = 300) -> None:
        self.store = store
        self.min_spacing_sec = max(0.0, float(min_spacing_sec))
        self.timeout_s = max(1, int(timeout_s))
        self._pending: list[_PendingPredictiveBarrier] = []
        self._last_key_ts_ns: dict[str, int] = {}
        self._active_event_keys: set[str] = set()

    def record_candidate(self, *, observation_ts_ns: int, asset_id: str, venue: str, instrument: str, candidate_id: str, model_key: str, setup_family: str, side: str, entry_price: float, stop_price: float, target_price: float, estimated_round_trip_cost_bps: float, predicted_target_before_stop_probability: float, predicted_directional_move_probability: float, parent_state_id: str = "", prediction_authority: str = "uncalibrated_analytic_score_shadow_only", event_key: str = "") -> bool:
        if float(entry_price) <= 0 or float(stop_price) <= 0 or float(target_price) <= 0:
            return False
        resolved_event_key = str(event_key or parent_state_id or candidate_id)
        key = f"{model_key}:{str(side).lower()}:{resolved_event_key}"
        if key in self._active_event_keys:
            return False
        spacing_key = f"{model_key}:{str(side).lower()}"
        last = int(self._last_key_ts_ns.get(spacing_key, 0))
        if last and (int(observation_ts_ns) - last) / 1_000_000_000.0 < self.min_spacing_sec:
            return False
        self._last_key_ts_ns[spacing_key] = int(observation_ts_ns)
        self._active_event_keys.add(key)
        self._pending.append(_PendingPredictiveBarrier(int(observation_ts_ns), str(asset_id), str(venue).lower(), str(instrument), str(candidate_id), str(model_key), str(setup_family), str(side).lower(), float(entry_price), float(stop_price), float(target_price), float(estimated_round_trip_cost_bps), float(predicted_target_before_stop_probability), float(predicted_directional_move_probability), self.timeout_s, str(parent_state_id), str(prediction_authority), key))
        return True

    def observe(self, *, now_ts_ns: int, current_price: float, venue: str | None = None, instrument: str | None = None) -> list[Path]:
        if float(current_price) <= 0:
            return []
        written: list[Path] = []
        remaining: list[_PendingPredictiveBarrier] = []
        for obs in self._pending:
            if venue is not None and str(venue).lower() != str(obs.venue).lower():
                remaining.append(obs)
                continue
            if instrument is not None and str(instrument) != str(obs.instrument):
                remaining.append(obs)
                continue
            sign = 1.0 if obs.side in {"buy", "long"} else -1.0
            move_bps = (float(current_price) / obs.entry_price - 1.0) * 10_000.0 * sign
            obs.favorable_bps = max(obs.favorable_bps, move_bps, 0.0)
            obs.adverse_bps = min(obs.adverse_bps, move_bps, 0.0)
            tp_hit = float(current_price) >= obs.target_price if sign > 0 else float(current_price) <= obs.target_price
            sl_hit = float(current_price) <= obs.stop_price if sign > 0 else float(current_price) >= obs.stop_price
            elapsed_s = max(0.0, (int(now_ts_ns) - obs.observation_ts_ns) / 1_000_000_000.0)
            outcome = "TP_FIRST" if tp_hit and not sl_hit else "SL_FIRST" if sl_hit else "TIMEOUT" if elapsed_s >= obs.timeout_s else ""
            if not outcome:
                remaining.append(obs)
                continue
            written.append(self.store.append_predictive_barrier_label(PredictiveBarrierLabel(
                observation_ts_ns=obs.observation_ts_ns, resolved_ts_ns=int(now_ts_ns), asset_id=obs.asset_id, venue=obs.venue, instrument=obs.instrument, candidate_id=obs.candidate_id, model_key=obs.model_key, setup_family=obs.setup_family, side=obs.side, reference_entry_price=obs.entry_price, stop_price=obs.stop_price, target_price=obs.target_price, estimated_round_trip_cost_bps=obs.estimated_round_trip_cost_bps, predicted_target_before_stop_probability=obs.predicted_target_before_stop_probability, predicted_directional_move_probability=obs.predicted_directional_move_probability, outcome=outcome, resolution_price=float(current_price), favorable_excursion_bps=obs.favorable_bps, adverse_excursion_bps=obs.adverse_bps, parent_state_id=obs.parent_state_id, prediction_authority=obs.prediction_authority,
            )))
            if obs.event_key:
                self._active_event_keys.discard(obs.event_key)
        self._pending = remaining
        return written


@dataclass
class _PendingForwardObservation:
    fill_ts_ns: int
    side: str
    candidate_id: str
    fill_price: float
    spread_cost_bps: float
    fee_cost_bps: float
    slippage_estimate_bps: float
    written_horizons: set[int] = field(default_factory=set)
    favorable_bps: float = 0.0
    adverse_bps: float = 0.0


class ForwardLabelWriter:
    """Creates executable forward-return labels from live post-fill observations.

    A label is written only after the requested horizon has actually elapsed.
    No future return is fabricated during order submission.
    """

    def __init__(self, store: JsonlResearchStore, horizons_s: tuple[int, ...] = (1, 10, 60, 300)) -> None:
        self.store = store
        self.horizons_s = tuple(sorted({int(h) for h in horizons_s if int(h) > 0}))
        self._pending: list[_PendingForwardObservation] = []

    def record_fill(
        self,
        *,
        fill_ts_ns: int,
        side: str,
        candidate_id: str,
        fill_price: float,
        spread_cost_bps: float,
        fee_cost_bps: float,
        slippage_estimate_bps: float,
    ) -> None:
        if float(fill_price) <= 0:
            return
        self._pending.append(_PendingForwardObservation(
            fill_ts_ns=int(fill_ts_ns), side=str(side).lower(), candidate_id=str(candidate_id),
            fill_price=float(fill_price), spread_cost_bps=float(spread_cost_bps),
            fee_cost_bps=float(fee_cost_bps), slippage_estimate_bps=float(slippage_estimate_bps),
        ))

    def observe(self, *, now_ts_ns: int, current_price: float) -> list[Path]:
        if float(current_price) <= 0:
            return []
        written: list[Path] = []
        remaining: list[_PendingForwardObservation] = []
        for obs in self._pending:
            direction = 1.0 if obs.side in {"buy", "long"} else -1.0
            markout = (float(current_price) / obs.fill_price - 1.0) * 10_000.0 * direction
            obs.favorable_bps = max(obs.favorable_bps, markout, 0.0)
            obs.adverse_bps = min(obs.adverse_bps, markout, 0.0)
            elapsed_s = max(0.0, (int(now_ts_ns) - obs.fill_ts_ns) / 1_000_000_000.0)
            for horizon in self.horizons_s:
                if horizon in obs.written_horizons or elapsed_s < horizon:
                    continue
                net_bps = markout - obs.spread_cost_bps - obs.fee_cost_bps - obs.slippage_estimate_bps
                written.append(self.store.append_delta_forward_label(DeltaForwardLabel(
                    observation_ts_ns=obs.fill_ts_ns,
                    horizon=f"{horizon}s",
                    side=obs.side,
                    gross_markout_bps=markout,
                    spread_cost_bps=obs.spread_cost_bps,
                    fee_cost_bps=obs.fee_cost_bps,
                    slippage_estimate_bps=obs.slippage_estimate_bps,
                    net_executable_return_bps=net_bps,
                    favorable_excursion_bps=obs.favorable_bps,
                    adverse_excursion_bps=obs.adverse_bps,
                )))
                obs.written_horizons.add(horizon)
            if len(obs.written_horizons) < len(self.horizons_s):
                remaining.append(obs)
        self._pending = remaining
        return written

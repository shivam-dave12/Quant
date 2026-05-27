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
        else:
            raise TypeError(f"unsupported research record type: {type(record)!r}")
    return paths


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

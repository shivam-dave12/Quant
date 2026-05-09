"""Shared data objects for the agentic fund runtime.

These objects are deliberately small and dependency-light. Agents should pass
explicit decision payloads through the stack instead of mutating strategy state
or exchanging unstructured dicts.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Iterable, List, Optional


def clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    try:
        v = float(value)
    except Exception:
        v = low
    return max(low, min(high, v))


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        v = float(value)
        if v == v and abs(v) != float("inf"):
            return v
    except Exception:
        pass
    return default


@dataclass(frozen=True)
class AgentScore:
    name: str
    raw: float
    weight: float
    reason: str = ""

    @property
    def weighted(self) -> float:
        return clamp(self.raw) * max(0.0, float(self.weight))

    def as_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["weighted"] = self.weighted
        return d


@dataclass(frozen=True)
class MarketDiagnostics:
    asset_id: str
    display_symbol: str
    primary_exchange: str
    asset_class: str
    price: float = 0.0
    atr_5m: float = 0.0
    atr_pctile: float = 0.5
    spread_bps: float = 0.0
    spread_atr: float = 0.0
    book_depth_usd: float = 0.0
    book_imbalance: float = 0.0
    data_age_sec: float = 0.0
    warmup_1m: float = 0.0
    warmup_5m: float = 0.0
    ready: bool = False
    has_position: bool = False
    phase: str = "UNKNOWN"
    tradable: bool = True
    notes: tuple[str, ...] = ()

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class TickerSelection:
    asset_id: str
    score: float
    rank: int
    diagnostics: MarketDiagnostics
    components: tuple[AgentScore, ...] = ()
    selected: bool = False
    reason: str = ""

    def component_map(self) -> Dict[str, float]:
        return {c.name: c.raw for c in self.components}

    def as_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["components"] = [c.as_dict() for c in self.components]
        return d


@dataclass(frozen=True)
class SetupCandidate:
    asset_id: str
    score: float
    side: str = ""
    setup_type: str = "SCANNING"
    probability: float = 0.0
    expected_value_r: float = 0.0
    rr: float = 0.0
    entry: float = 0.0
    stop: float = 0.0
    target: float = 0.0
    size_mult: float = 0.0
    source: str = "strategy_cache"
    reasons: tuple[str, ...] = ()

    @property
    def actionable(self) -> bool:
        return self.score >= 0.60 and self.expected_value_r > 0.0 and bool(self.side)

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RiskVerdict:
    approved: bool
    reason: str
    severity: str = "info"
    max_size_mult: float = 1.0
    checks: tuple[AgentScore, ...] = ()

    def as_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["checks"] = [c.as_dict() for c in self.checks]
        return d


@dataclass(frozen=True)
class FundCycleReport:
    timestamp: float
    selected: tuple[TickerSelection, ...] = ()
    rejected: tuple[TickerSelection, ...] = ()
    setup_candidates: tuple[SetupCandidate, ...] = ()
    risk_verdicts: Dict[str, RiskVerdict] = field(default_factory=dict)
    notes: tuple[str, ...] = ()

    def selected_ids(self) -> set[str]:
        return {s.asset_id for s in self.selected}

    def as_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "selected": [s.as_dict() for s in self.selected],
            "rejected": [s.as_dict() for s in self.rejected],
            "setup_candidates": [c.as_dict() for c in self.setup_candidates],
            "risk_verdicts": {k: v.as_dict() for k, v in self.risk_verdicts.items()},
            "notes": list(self.notes),
        }

    def compact_text(self, limit: int = 8) -> str:
        lines = ["Institutional CIO cycle"]
        if not self.selected:
            lines.append("Selected desks: none")
        else:
            lines.append("Selected desks:")
            for sel in self.selected[:limit]:
                d = sel.diagnostics
                lines.append(
                    f"- {sel.rank}. {sel.asset_id} {d.primary_exchange.upper()} "
                    f"score={sel.score:.2f} spread={d.spread_bps:.1f}bps "
                    f"atr_pct={d.atr_pctile:.0%} {sel.reason}"
                )
        if self.rejected:
            lines.append("Parked desks:")
            for sel in self.rejected[:limit]:
                lines.append(f"- {sel.asset_id} score={sel.score:.2f} {sel.reason}")
        if self.notes:
            lines.append("Notes: " + "; ".join(self.notes[:4]))
        return "\n".join(lines)


def weighted_score(parts: Iterable[AgentScore]) -> float:
    parts = list(parts)
    weight = sum(max(0.0, p.weight) for p in parts)
    if weight <= 0:
        return 0.0
    return clamp(sum(p.weighted for p in parts) / weight)

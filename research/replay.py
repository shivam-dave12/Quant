"""Chronological replay runner that regenerates decisions without lookahead."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Callable, Any

@dataclass(frozen=True)
class ReplayResult:
    decisions: list[dict[str, Any]]
    protections_confirmed: int
    orders_rejected_for_safety: int

class ReplayRunner:
    def run(self, observations: list[dict[str, Any]], decide: Callable[[dict[str, Any]], dict[str, Any]]) -> ReplayResult:
        ordered = sorted(observations, key=lambda row: int(row["ts_ns"])); decisions = [decide(row) for row in ordered]
        protected = sum(1 for row in decisions if row.get("protection_confirmed") is True)
        rejected = sum(1 for row in decisions if row.get("code") == "NO_TRADE_EXECUTION_UNSAFE")
        return ReplayResult(decisions, protected, rejected)

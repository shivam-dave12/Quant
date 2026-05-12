"""Deterministic risk committee.

Agents may rank opportunities, but only this layer may approve a desk for live
entry evaluation. These checks are intentionally boring and non-bypassable.
"""

from __future__ import annotations

from typing import Any

try:
    import config
except Exception:  # pragma: no cover
    config = None  # type: ignore

from fund.mandate import FundMandate
from fund.types import AgentScore, MarketDiagnostics, RiskVerdict, SetupCandidate, TickerSelection, clamp


class RiskCommitteeAgent:
    def __init__(self, mandate: FundMandate | None = None) -> None:
        self.mandate = mandate or FundMandate.from_config()

    def review_desk(
        self,
        selection: TickerSelection,
        setup: SetupCandidate | None,
        portfolio_guard: Any,
        context: Any,
        contexts: list[Any],
    ) -> RiskVerdict:
        d = selection.diagnostics
        checks: list[AgentScore] = []

        checks.append(AgentScore("ticker_score", clamp(selection.score), 0.20, "CIO ticker score"))
        if setup is not None:
            checks.append(AgentScore("setup_score", clamp(setup.score), 0.20, "cached setup score"))
        checks.append(AgentScore("spread", self._spread_check(d), 0.20, "spread inside mandate"))
        checks.append(AgentScore("freshness", self._freshness_check(d), 0.15, "market data fresh"))
        checks.append(AgentScore("warmup", min(d.warmup_1m, d.warmup_5m), 0.15, "candle warmup"))
        checks.append(AgentScore("readiness", 1.0 if d.ready else 0.0, 0.10, "desk ready"))

        if context is not None and not getattr(context, "has_position", False):
            allowed, reason = portfolio_guard.can_evaluate_entry(context, contexts)
            if not allowed:
                return RiskVerdict(False, reason, "block", 0.0, tuple(checks))

        icici_pre_thesis = (
            str(getattr(d, "primary_exchange", "") or "").lower() == "icici"
            and "option" in str(getattr(d, "asset_class", "") or "").lower()
            and d.ready
            and d.price > 0
        )
        icici_floor = float(getattr(config, "FUND_ICICI_PRE_THESIS_SCORE_FLOOR", 0.52) if config is not None else 0.52)
        if selection.score < self.mandate.min_execution_score:
            if not (icici_pre_thesis and selection.score >= icici_floor):
                return RiskVerdict(
                    False,
                    f"CIO score {selection.score:.3f} below execution floor {self.mandate.min_execution_score:.3f}",
                    "park",
                    0.0,
                    tuple(checks),
                )
        if d.data_age_sec > self.mandate.max_data_age_sec:
            return RiskVerdict(False, f"stale data {d.data_age_sec:.0f}s", "block", 0.0, tuple(checks))
        if min(d.warmup_1m, d.warmup_5m) < self.mandate.min_warmup_ratio:
            return RiskVerdict(False, "candle warmup below mandate", "block", 0.0, tuple(checks))
        if d.spread_bps > self.mandate.max_spread_for_class(d.asset_class):
            return RiskVerdict(False, f"spread {d.spread_bps:.1f}bps above mandate", "block", 0.0, tuple(checks))

        size_mult = 1.0
        if setup is not None and setup.score > 0:
            if setup.score < self.mandate.min_setup_score:
                size_mult = 0.60
            elif not setup.actionable:
                size_mult = 0.80
        if self.mandate.paper_mode or not self.mandate.live_ordering_enabled:
            return RiskVerdict(True, "approved for strategy evaluation; live execution remains strategy/router controlled", "paper", size_mult, tuple(checks))
        if icici_pre_thesis and selection.score < self.mandate.min_execution_score:
            return RiskVerdict(True, "approved for ICICI underlying thesis evaluation; option contract liquidity checked post-thesis", "approve", size_mult * 0.85, tuple(checks))
        return RiskVerdict(True, "approved for live strategy evaluation", "approve", size_mult, tuple(checks))

    def _spread_check(self, d: MarketDiagnostics) -> float:
        limit = max(1.0, self.mandate.max_spread_for_class(d.asset_class))
        return clamp(1.0 - d.spread_bps / limit)

    def _freshness_check(self, d: MarketDiagnostics) -> float:
        return clamp(1.0 - d.data_age_sec / max(1.0, self.mandate.max_data_age_sec))

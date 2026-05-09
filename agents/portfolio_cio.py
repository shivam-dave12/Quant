"""CIO/Governor agent for the institutional fund runtime."""

from __future__ import annotations

import time
from typing import Any, Dict, Iterable, List

from fund.audit_log import AuditLog
from fund.mandate import FundMandate
from fund.types import FundCycleReport, TickerSelection, clamp

from .risk_committee import RiskCommitteeAgent
from .setup_selection_agent import SetupSelectionAgent
from .ticker_selection_agent import TickerSelectionAgent
from .universe_agent import UniverseAgent


class PortfolioCIO:
    """Coordinates agents and returns the desks allowed to run entry logic."""

    def __init__(self, mandate: FundMandate | None = None) -> None:
        self.mandate = mandate or FundMandate.from_config()
        self.universe = UniverseAgent(self.mandate)
        self.ticker_selector = TickerSelectionAgent(self.mandate)
        self.setup_selector = SetupSelectionAgent(self.mandate)
        self.risk_committee = RiskCommitteeAgent(self.mandate)
        self.audit = AuditLog(self.mandate.audit_log_path)
        self.last_report: FundCycleReport | None = None

    def select_execution_queue(self, contexts: List[Any], portfolio_guard: Any) -> FundCycleReport:
        if not self.mandate.enabled:
            # No fabricated selections: disabled mandate means the CIO does not
            # authorise desks. Runtime can still operate via the scanner's own
            # explicit control path, but this agent will not manufacture a score.
            report = FundCycleReport(time.time(), selected=tuple(), notes=("agentic mandate disabled; no CIO authorisation",))
            self.last_report = report
            return report

        diagnostics = self.universe.diagnose_many(contexts)
        ranked = self.ticker_selector.rank(diagnostics)
        by_id: Dict[str, Any] = {getattr(c.instrument, "asset_id", ""): c for c in contexts}

        depth_scan_rows = ranked if self.mandate.top_n_depth_scan <= 0 else ranked[: self.mandate.top_n_depth_scan]
        depth_scan_ids = {r.asset_id for r in depth_scan_rows}
        setup_candidates = self.setup_selector.evaluate_many(
            [ctx for ctx in contexts if getattr(ctx.instrument, "asset_id", "") in depth_scan_ids]
        )
        setup_by_id = {c.asset_id: c for c in setup_candidates}

        selected: list[TickerSelection] = []
        rejected: list[TickerSelection] = []
        verdicts = {}
        for row in ranked:
            setup = setup_by_id.get(row.asset_id)
            combined = self._combined_score(row.score, setup.score if setup else 0.0)
            enriched = TickerSelection(
                asset_id=row.asset_id,
                score=combined,
                rank=row.rank,
                diagnostics=row.diagnostics,
                components=row.components,
                selected=combined >= self.mandate.min_execution_score,
                reason=self._combined_reason(row.reason, setup),
            )
            ctx = by_id.get(row.asset_id)
            verdict = self.risk_committee.review_desk(enriched, setup, portfolio_guard, ctx, contexts)
            verdicts[row.asset_id] = verdict
            execution_room = self.mandate.top_n_execution_desks <= 0 or len(selected) < self.mandate.top_n_execution_desks
            if verdict.approved and execution_room:
                selected.append(enriched)
            else:
                rejected.append(enriched)

        report = FundCycleReport(
            timestamp=time.time(),
            selected=tuple(selected),
            rejected=tuple(rejected),
            setup_candidates=tuple(setup_candidates),
            risk_verdicts=verdicts,
            notes=(
                f"paper_mode={self.mandate.paper_mode}",
                f"live_ordering={self.mandate.live_ordering_enabled}",
                f"depth_scan={len(depth_scan_ids)}",
            ),
        )
        self.last_report = report
        try:
            self.audit.write("fund_cycle", report.as_dict())
        except Exception:
            pass
        return report

    @staticmethod
    def _combined_score(ticker_score: float, setup_score: float) -> float:
        if setup_score <= 0:
            return clamp(ticker_score * 0.86)
        return clamp(0.58 * ticker_score + 0.42 * setup_score)

    @staticmethod
    def _combined_reason(reason: str, setup: Any) -> str:
        if setup is None or getattr(setup, "score", 0.0) <= 0:
            return reason
        tail = ", ".join(getattr(setup, "reasons", ())[:3])
        return f"{reason}; setup_score={setup.score:.2f}" + (f" [{tail}]" if tail else "")

    def selected_contexts(self, contexts: Iterable[Any], report: FundCycleReport | None = None) -> list[Any]:
        report = report or self.last_report
        if report is None:
            return list(contexts)
        selected_ids = report.selected_ids()
        return [ctx for ctx in contexts if getattr(ctx.instrument, "asset_id", "") in selected_ids]

    def format_report(self) -> str:
        if self.last_report is None:
            return "Institutional CIO has not run a selection cycle yet."
        return self.last_report.compact_text()

    @staticmethod
    def _context_selection_snapshot(ctx: Any, rank: int) -> TickerSelection:
        from fund.types import MarketDiagnostics

        inst = getattr(ctx, "instrument", None)
        asset_id = str(getattr(inst, "asset_id", "UNKNOWN") or "UNKNOWN")
        d = MarketDiagnostics(
            asset_id=asset_id,
            display_symbol=str(getattr(inst, "display_symbol", asset_id) or asset_id),
            primary_exchange=str(getattr(getattr(inst, "primary_exchange", ""), "value", "")),
            asset_class=str(getattr(getattr(inst, "asset_class", ""), "value", "")),
            ready=bool(getattr(ctx, "ready", False)),
            has_position=bool(getattr(ctx, "has_position", False)),
            phase=str(getattr(ctx, "phase_name", "UNKNOWN") or "UNKNOWN"),
        )
        return TickerSelection(asset_id, 1.0, rank, d, selected=True, reason="context snapshot")

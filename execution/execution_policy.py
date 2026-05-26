"""Non-bypassable policy gate between decisions and order transmission."""
from __future__ import annotations
from core.identifiers import DecisionCode, OpportunityDecision, ProtectionPlan
class ExecutionPolicy:
    def __init__(self, minimum_net_edge_bps: float, minimum_execution_quality: float) -> None:
        self.minimum_net_edge_bps, self.minimum_execution_quality = minimum_net_edge_bps, minimum_execution_quality
    def approve(self, *, desk: str, instrument: str, direction: str, net_edge_bps: float, execution_quality: float,
                size_approved: bool, plan: ProtectionPlan | None, venue_supports_protection: bool, compliance_ready: bool,
                reasons: tuple[str, ...], metrics: dict, sizing=None) -> OpportunityDecision:
        if execution_quality < self.minimum_execution_quality or not venue_supports_protection or not compliance_ready or plan is None:
            code = DecisionCode.NO_TRADE_EXECUTION_UNSAFE
        elif net_edge_bps < self.minimum_net_edge_bps:
            code = DecisionCode.NO_TRADE_INSUFFICIENT_EDGE
        elif not size_approved:
            code = DecisionCode.NO_TRADE_RISK_BUDGET
        else:
            code = DecisionCode.TRADE_APPROVED_WITH_PROTECTION_PLAN
        return OpportunityDecision(desk, instrument, direction, code, net_edge_bps, execution_quality, size_approved, plan, reasons, metrics, sizing)

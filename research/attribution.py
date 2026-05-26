"""Decision and execution outcome attribution for every closed opportunity."""
from __future__ import annotations
from dataclasses import dataclass, asdict
from core.research_store import ResearchStore

@dataclass(frozen=True)
class PnLAttribution:
    execution_id: str
    desk: str
    instrument: str
    gross_pnl: float
    fee_cost: float
    spread_cost: float
    slippage_cost: float
    impact_cost: float
    net_pnl: float
    max_favourable_excursion: float
    max_adverse_excursion: float
    primary_failure_reason: str | None

class AttributionEngine:
    def __init__(self, store: ResearchStore) -> None: self.store = store
    def close(self, *, execution_id: str, desk: str, instrument: str, gross_pnl: float, fee_cost: float, spread_cost: float,
              slippage_cost: float, impact_cost: float, mfe: float, mae: float, expected_direction_correct: bool,
              contract_efficiency_ok: bool, protection_quality_ok: bool, execution_quality_ok: bool) -> PnLAttribution:
        reason = None
        if gross_pnl <= 0:
            reason = "WRONG_DIRECTION" if not expected_direction_correct else "BAD_CONTRACT" if not contract_efficiency_ok else "VULNERABLE_SL_OR_BAD_TP" if not protection_quality_ok else "BAD_EXECUTION" if not execution_quality_ok else "REGIME_SHIFT"
        result = PnLAttribution(execution_id, desk, instrument, gross_pnl, fee_cost, spread_cost, slippage_cost, impact_cost,
            gross_pnl - fee_cost - spread_cost - slippage_cost - impact_cost, mfe, mae, reason)
        self.store.append("attribution", asdict(result)); return result

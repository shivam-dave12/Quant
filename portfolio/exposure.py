"""Explicit reconciled exposure book with macro risk-group attribution."""
from __future__ import annotations
from dataclasses import dataclass

@dataclass(frozen=True)
class Exposure:
    instrument: str
    desk: str
    risk_group: str
    currency: str
    notional: float
    signed_delta_notional: float
    risk_to_invalidation: float
    stress_slippage_risk: float
    margin_used: float

class ExposureBook:
    def __init__(self) -> None: self._positions: dict[str, Exposure] = {}
    def set(self, exposure: Exposure) -> None: self._positions[exposure.instrument] = exposure
    def remove(self, instrument: str) -> None: self._positions.pop(instrument, None)
    def items(self, *, currency: str | None = None) -> list[Exposure]:
        return [x for x in self._positions.values() if currency is None or x.currency == currency]
    def identical_contract_open(self, instrument: str) -> bool: return instrument in self._positions
    def group_risk(self, group: str, currency: str) -> float:
        return sum(x.risk_to_invalidation + x.stress_slippage_risk for x in self.items(currency=currency) if x.risk_group == group)

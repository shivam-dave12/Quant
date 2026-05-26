"""Restart reconciliation before any new Groww option entries are allowed."""
from __future__ import annotations
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from adapters.groww.client import GrowwAdapter
@dataclass(frozen=True)
class ReconciliationResult:
    safe_to_enter: bool
    open_long_positions: dict[str, int]
    protected_quantities: dict[str, int]
    unprotected_positions: dict[str, int]
class GrowwReconciler:
    def __init__(self, adapter: GrowwAdapter) -> None: self.adapter = adapter
    def run(self, now: datetime | None = None) -> ReconciliationResult:
        now = now or datetime.now(UTC); start = (now - timedelta(days=1)).isoformat(timespec="seconds"); end = now.isoformat(timespec="seconds")
        positions: dict[str, int] = {}
        for row in self.adapter.positions():
            symbol = str(row.get("trading_symbol", row.get("tradingSymbol", row.get("symbol", ""))))
            qty = int(row.get("net_quantity", row.get("netQty", row.get("quantity", 0))) or 0)
            if symbol and qty > 0: positions[symbol] = qty
        protected: dict[str, int] = {}
        for order in self.adapter.active_oco_orders(start_date_time=start, end_date_time=end):
            symbol = str(order.get("trading_symbol", "")); qty = int(order.get("quantity", 0) or 0)
            if symbol and str(order.get("status", "")).upper() == "ACTIVE": protected[symbol] = protected.get(symbol, 0) + qty
        unprotected = {s: q - protected.get(s, 0) for s, q in positions.items() if q > protected.get(s, 0)}
        return ReconciliationResult(not unprotected, positions, protected, unprotected)

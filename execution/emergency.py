"""Deterministic emergency exit for a Groww long option that failed OCO activation."""
from __future__ import annotations
from dataclasses import replace
from core.identifiers import ExecutionReceipt, ProtectionState
from adapters.groww.client import GrowwAdapter, GrowwContract
from core.observability import Observability

class GrowwEmergencyExit:
    def __init__(self, adapter: GrowwAdapter, observability: Observability) -> None: self.adapter, self.obs = adapter, observability
    def execute(self, *, receipt: ExecutionReceipt, contract: GrowwContract, quantity: int, reason: str = "OCO protection failed") -> ExecutionReceipt:
        quote = self.adapter.quote(contract.trading_symbol)
        bid = float(quote.get("bid_price", quote.get("bidPrice", 0)) or 0)
        if bid <= 0: raise RuntimeError("UNPROTECTED_POSITION_EMERGENCY_NO_EXECUTABLE_BID")
        price = round((bid // contract.tick_size) * contract.tick_size, 8)
        response = self.adapter.emergency_limit_sell(contract=contract, quantity=quantity, limit_price=price,
            reference_id=("emg-" + receipt.execution_id.replace("_", ""))[:20])
        order_id = str(response.get("groww_order_id", response.get("order_id", "")))
        updated = replace(receipt, state=ProtectionState.MANUAL_EMERGENCY_EXIT, emergency_order_id=order_id,
                          reason=f"{reason}; validated priced-limit emergency exit submitted")
        self.obs.critical("UNPROTECTED_POSITION_EMERGENCY", {"instrument": contract.trading_symbol, "quantity": quantity, "emergency_order_id": order_id})
        return updated

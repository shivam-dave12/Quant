"""Delta entry lifecycle with actual-fill reconciliation before active protected exposure is recorded."""
from __future__ import annotations
from dataclasses import replace
import time
from uuid import uuid4
from adapters.delta.client import DeltaAdapter, DeltaProduct
from core.identifiers import ExecutionReceipt, ProtectionPlan, ProtectionState
from core.state_store import StateStore
from core.observability import Observability

class DeltaProtectedExecutor:
    def __init__(self, adapter: DeltaAdapter, state_store: StateStore, observability: Observability, *, max_status_checks: int = 8,
                 poll_interval: float = 0.5, sleep=time.sleep) -> None:
        self.adapter, self.store, self.obs = adapter, state_store, observability; self.max_status_checks = max_status_checks; self.poll_interval = poll_interval; self.sleep = sleep
    def _save(self, receipt: ExecutionReceipt) -> ExecutionReceipt:
        self.store.put(f"delta_{receipt.execution_id}", receipt); self.obs.event("DELTA_EXECUTION_STATE", receipt); return receipt
    def execute(self, *, desk: str, product: DeltaProduct, side: str, quantity: float, plan: ProtectionPlan) -> ExecutionReceipt:
        execution_id = uuid4().hex[:12]; receipt = self._save(ExecutionReceipt(execution_id, desk, product.mapping.venue_symbol, ProtectionState.CANDIDATE_SELECTED, requested_quantity=quantity))
        response = self.adapter.place_protected_order(product_id=product.product_id, product_symbol=product.mapping.venue_symbol, side=side, quantity=quantity,
            entry_price=plan.entry_price, stop_price=plan.stop_price, target_price=plan.target_price)
        order_id = str(response.get("id", response.get("order_id", "")))
        if not order_id: raise RuntimeError("DELTA_ENTRY_ORDER_ID_MISSING")
        receipt = self._save(replace(receipt, state=ProtectionState.ENTRY_SUBMITTED, entry_order_id=order_id))
        last: dict = {}
        for _ in range(self.max_status_checks):
            last = self.adapter.get_order(order_id); state = str(last.get("state", last.get("status", ""))).lower()
            filled = float(last.get("size", last.get("filled_size", last.get("filled_quantity", 0))) or 0) if state in {"filled", "closed", "partially_filled", "partially-filled"} else float(last.get("filled_size", last.get("filled_quantity", 0)) or 0)
            if filled > 0 or state in {"cancelled", "rejected"}: break
            self.sleep(self.poll_interval)
        filled = float(last.get("filled_size", last.get("filled_quantity", last.get("size", 0) if str(last.get("state", "")).lower() in {"filled", "closed"} else 0)) or 0)
        average = float(last.get("average_fill_price", last.get("average_fill_price", last.get("limit_price", 0))) or 0)
        if filled <= 0:
            return self._save(replace(receipt, state=ProtectionState.RECONCILIATION_REQUIRED, reason="DELTA_ENTRY_NOT_FILLED_OR_REJECTED"))
        receipt = self._save(replace(receipt, state=ProtectionState.ENTRY_PARTIAL_OR_FILLED, filled_quantity=filled, average_fill_price=average))
        position = self.adapter.get_position(product.product_id); position_size = abs(float(position.get("size", position.get("net_size", 0)) or 0))
        protected_size = abs(float(position.get("bracket_size", position.get("protected_size", position_size if position.get("bracket_stop_loss_order") and position.get("bracket_take_profit_order") else 0)) or 0))
        protection_active = bool(position.get("bracket_stop_loss_order") or position.get("stop_loss_order")) and bool(position.get("bracket_take_profit_order") or position.get("take_profit_order"))
        if not protection_active or protected_size + 1e-12 < filled or position_size + 1e-12 < filled:
            return self._save(replace(receipt, state=ProtectionState.RECONCILIATION_REQUIRED, reason="DELTA_FILLED_POSITION_PROTECTION_NOT_CONFIRMED"))
        receipt = self._save(replace(receipt, state=ProtectionState.PROTECTION_CONFIRMED))
        return self._save(replace(receipt, state=ProtectionState.ACTIVE_PROTECTED_POSITION))

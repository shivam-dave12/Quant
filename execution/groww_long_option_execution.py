"""Strict Groww lifecycle: long-option BUY fill, immediate OCO protection, monitoring and emergency closure."""
from __future__ import annotations
from dataclasses import replace
import time
from uuid import uuid4
from adapters.groww.client import GrowwAdapter, GrowwContract
from core.identifiers import ExecutionReceipt, ProtectionPlan, ProtectionState
from core.state_store import StateStore
from core.observability import Observability
from execution.emergency import GrowwEmergencyExit

class GrowwLongOptionExecutor:
    def __init__(self, adapter: GrowwAdapter, state_store: StateStore, observability: Observability, *, max_status_checks: int = 8, poll_interval: float = 0.5, sleep=time.sleep) -> None:
        self.adapter, self.store, self.obs = adapter, state_store, observability; self.max_status_checks = max_status_checks; self.poll_interval = poll_interval; self.sleep = sleep
        self.entries_halted = False; self.emergency = GrowwEmergencyExit(adapter, observability)
    def _save(self, receipt: ExecutionReceipt) -> ExecutionReceipt:
        self.store.put(f"groww_{receipt.execution_id}", receipt); self.obs.event("GROWW_EXECUTION_STATE", receipt); return receipt
    @staticmethod
    def _order_fields(detail: dict) -> tuple[str, int, float]:
        return str(detail.get("orderStatus", detail.get("status", ""))).upper(), int(detail.get("filledQty", detail.get("filled_quantity", 0)) or 0), float(detail.get("avgFillPrice", detail.get("average_fill_price", 0)) or 0)
    def execute(self, *, contract: GrowwContract, quantity: int, entry_limit: float, protection: ProtectionPlan) -> ExecutionReceipt:
        if self.entries_halted: raise RuntimeError("GROWW_ENTRIES_HALTED_PENDING_RECONCILIATION")
        execution_id = uuid4().hex[:12]; receipt = self._save(ExecutionReceipt(execution_id, "INDIA_OPTIONS", contract.trading_symbol, ProtectionState.CANDIDATE_SELECTED, requested_quantity=quantity))
        try:
            order = self.adapter.place_long_option_limit(contract=contract, quantity=quantity, limit_price=entry_limit, reference_id=f"ent-{execution_id}")
            entry_id = str(order.get("groww_order_id", order.get("order_id", "")))
            if not entry_id: raise RuntimeError("GROWW_ENTRY_ORDER_ID_MISSING")
            receipt = self._save(replace(receipt, state=ProtectionState.ENTRY_SUBMITTED, entry_order_id=entry_id))
            status, filled, average = "", 0, 0.0
            for _ in range(self.max_status_checks):
                status, filled, average = self._order_fields(self.adapter.order_detail(entry_id))
                if filled > 0 or status in {"REJECTED", "CANCELLED", "FAILED"}: break
                self.sleep(self.poll_interval)
            if filled <= 0: raise RuntimeError(f"GROWW_ENTRY_NOT_FILLED:{status or 'TIMEOUT'}")
            if filled < quantity: self.adapter.cancel_order(entry_id)
            receipt = self._save(replace(receipt, state=ProtectionState.ENTRY_PARTIAL_OR_FILLED, filled_quantity=filled, average_fill_price=average))
            net_position = self._actual_long_position_quantity(contract.trading_symbol)
            if net_position < filled: raise RuntimeError("FILLED_QUANTITY_NOT_RECONCILED_IN_OPEN_POSITION")
            oco = self.adapter.create_oco_exit(contract=contract, quantity=filled, net_position_quantity=net_position, target_trigger=protection.target_price,
                target_price=protection.target_price, stop_trigger=protection.stop_price, reference_id=f"oco-{execution_id}")
            oco_id = str(oco.get("smart_order_id", ""))
            if not oco_id: raise RuntimeError("OCO_ORDER_ID_MISSING")
            receipt = self._save(replace(receipt, state=ProtectionState.PROTECTION_SUBMITTED_FOR_FILLED_QTY, protection_order_id=oco_id))
            active = self.adapter.smart_order(oco_id)
            if str(active.get("status", "")).upper() != "ACTIVE" or int(active.get("quantity", 0) or 0) != filled: raise RuntimeError("OCO_PROTECTION_NOT_CONFIRMED_FOR_FILLED_QUANTITY")
            receipt = self._save(replace(receipt, state=ProtectionState.PROTECTION_CONFIRMED)); return self._save(replace(receipt, state=ProtectionState.ACTIVE_PROTECTED_POSITION))
        except Exception as exc:
            if receipt.filled_quantity > 0:
                self.entries_halted = True; emergency = self._save(replace(receipt, state=ProtectionState.UNPROTECTED_POSITION_EMERGENCY, reason=str(exc)))
                return self._save(self.emergency.execute(receipt=emergency, contract=contract, quantity=int(receipt.filled_quantity), reason="OCO protection failed"))
            self._save(replace(receipt, state=ProtectionState.RECONCILIATION_REQUIRED, reason=str(exc))); raise
    def _actual_long_position_quantity(self, symbol: str) -> int:
        for row in self.adapter.positions():
            if str(row.get("trading_symbol", row.get("tradingSymbol", row.get("symbol", "")))) == symbol:
                return max(0, int(row.get("net_quantity", row.get("netQty", row.get("quantity", 0))) or 0))
        return 0

    def modify_protection(self, *, receipt: ExecutionReceipt, quantity: int, target_trigger: float, stop_trigger: float) -> ExecutionReceipt:
        if receipt.state is not ProtectionState.ACTIVE_PROTECTED_POSITION or not receipt.protection_order_id:
            raise RuntimeError("ACTIVE_OCO_PROTECTED_POSITION_REQUIRED")
        result = self.adapter.modify_oco_exit(smart_order_id=receipt.protection_order_id, quantity=quantity, target_trigger=target_trigger, stop_trigger=stop_trigger)
        if str(result.get("status", "")).upper() != "ACTIVE":
            raise RuntimeError("MODIFIED_OCO_NOT_ACTIVE")
        return self._save(receipt)
    def exit_on_monitor_failure(self, *, receipt: ExecutionReceipt, contract: GrowwContract, quantity: int, reason: str) -> ExecutionReceipt:
        if receipt.state is not ProtectionState.ACTIVE_PROTECTED_POSITION or not receipt.protection_order_id:
            raise RuntimeError("ACTIVE_OCO_PROTECTED_POSITION_REQUIRED")
        self.entries_halted = True
        cancellation = self.adapter.cancel_oco_exit(receipt.protection_order_id)
        if str(cancellation.get("status", "")).upper() != "CANCELLED":
            self.obs.critical("OCO_CANCEL_BEFORE_CONTROLLED_EXIT_FAILED", {"instrument": contract.trading_symbol, "reason": reason})
            return self._save(replace(receipt, state=ProtectionState.RECONCILIATION_REQUIRED, reason="ACTIVE_OCO_CANCELLATION_NOT_CONFIRMED"))
        emergency = self._save(replace(receipt, state=ProtectionState.UNPROTECTED_POSITION_EMERGENCY, reason=reason))
        return self._save(self.emergency.execute(receipt=emergency, contract=contract, quantity=quantity, reason=reason))
    def monitor_position(self, *, receipt: ExecutionReceipt, contract: GrowwContract, quantity: int, remaining_expected_premium_edge: float, projected_theta_loss: float, realised_iv_edge: float, thesis_valid: bool) -> ExecutionReceipt | None:
        exit_required, reason = GrowwOptionPositionMonitor().should_exit(remaining_expected_premium_edge=remaining_expected_premium_edge, projected_theta_loss=projected_theta_loss, realised_iv_edge=realised_iv_edge, thesis_valid=thesis_valid)
        if not exit_required:
            return None
        return self.exit_on_monitor_failure(receipt=receipt, contract=contract, quantity=quantity, reason=reason)

class GrowwOptionPositionMonitor:
    """Triggers priced-limit exit only when theta, IV or underlying thesis destroys remaining edge."""
    def should_exit(self, *, remaining_expected_premium_edge: float, projected_theta_loss: float, realised_iv_edge: float, thesis_valid: bool) -> tuple[bool, str]:
        if not thesis_valid: return True, "UNDERLYING_THESIS_INVALIDATED"
        if remaining_expected_premium_edge <= projected_theta_loss: return True, "THETA_DOMINATES_REMAINING_EDGE"
        if realised_iv_edge < -abs(remaining_expected_premium_edge): return True, "IV_CRUSH_DESTROYS_PREMIUM_EDGE"
        return False, "POSITION_THESIS_REMAINS_VALID"

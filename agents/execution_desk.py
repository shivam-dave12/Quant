"""Execution desk abstraction.

The current v78 integration still lets QuantStrategy and OrderManager place
orders. This wrapper is here for the next stage, where all order intents pass
through a desk-level audit before they reach a venue adapter.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ExecutionReceipt:
    accepted: bool
    venue: str
    order_id: str = ""
    reason: str = ""
    raw: Any = None


class ExecutionDeskAgent:
    def __init__(self, paper_mode: bool = True) -> None:
        self.paper_mode = bool(paper_mode)

    def route_intent(self, router: Any, method: str, *args, **kwargs) -> ExecutionReceipt:
        venue = str(getattr(router, "active_exchange", "unknown"))
        if self.paper_mode:
            return ExecutionReceipt(True, venue, reason=f"paper acceptance for {method}")
        fn = getattr(router, method, None)
        if not callable(fn):
            return ExecutionReceipt(False, venue, reason=f"router missing {method}")
        raw = fn(*args, **kwargs)
        order_id = ""
        if isinstance(raw, dict):
            order_id = str(raw.get("id") or raw.get("order_id") or raw.get("client_order_id") or "")
        return ExecutionReceipt(True, venue, order_id=order_id, raw=raw)

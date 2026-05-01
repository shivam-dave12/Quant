"""
execution/order_manager.py

Strict execution manager.

Rules:
- No simulated orders.
- No fake fills.
- If the exchange adapter lacks a required method, execution fails loudly.
- Bracket order must have entry + SL + TP. If protective orders fail, entry is cancelled/flattened if adapter supports it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional
import logging

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OrderResult:
    accepted: bool
    reason: str
    entry_order: Optional[Any] = None
    sl_order: Optional[Any] = None
    tp_order: Optional[Any] = None
    raw: Optional[Dict[str, Any]] = None


class OrderManager:
    def __init__(self, exchange_adapter: Any, *, symbol: str) -> None:
        if exchange_adapter is None:
            raise ValueError("exchange_adapter is required; no execution fallback is allowed")
        self.exchange = exchange_adapter
        self.symbol = symbol

    def _require(self, method_name: str) -> Any:
        fn = getattr(self.exchange, method_name, None)
        if fn is None or not callable(fn):
            raise RuntimeError(f"Exchange adapter missing required method: {method_name}")
        return fn

    def place_bracket_order(
        self,
        *,
        side: str,
        qty: float,
        entry_price: float,
        sl_price: float,
        tp_price: float,
        client_order_id: str,
        order_type: str = "market",
    ) -> OrderResult:
        side = side.lower()
        if side not in ("long", "short"):
            return OrderResult(False, f"invalid side {side}")
        if qty <= 0 or entry_price <= 0 or sl_price <= 0 or tp_price <= 0:
            return OrderResult(False, "qty/entry/sl/tp must be positive")
        if side == "long" and not (sl_price < entry_price < tp_price):
            return OrderResult(False, "invalid long bracket geometry")
        if side == "short" and not (tp_price < entry_price < sl_price):
            return OrderResult(False, "invalid short bracket geometry")

        place_entry = self._require("place_order")
        place_sl = self._require("place_stop_loss")
        place_tp = self._require("place_take_profit")

        exchange_side = "buy" if side == "long" else "sell"
        close_side = "sell" if side == "long" else "buy"

        try:
            entry_order = place_entry(
                symbol=self.symbol,
                side=exchange_side,
                quantity=qty,
                order_type=order_type,
                price=entry_price if order_type != "market" else None,
                client_order_id=client_order_id,
            )
        except Exception as exc:
            logger.exception("ENTRY_ORDER_FAILED %s", exc)
            return OrderResult(False, f"entry order failed: {exc}")

        try:
            sl_order = place_sl(
                symbol=self.symbol,
                side=close_side,
                quantity=qty,
                stop_price=sl_price,
                client_order_id=f"{client_order_id}-sl",
                reduce_only=True,
            )
            tp_order = place_tp(
                symbol=self.symbol,
                side=close_side,
                quantity=qty,
                price=tp_price,
                client_order_id=f"{client_order_id}-tp",
                reduce_only=True,
            )
        except Exception as exc:
            logger.exception("PROTECTIVE_ORDER_FAILED %s", exc)
            self._emergency_protective_failure(entry_order, qty, close_side, client_order_id)
            return OrderResult(False, f"protective order failed: {exc}", entry_order=entry_order)

        logger.info(
            "BRACKET_PLACED side=%s qty=%s entry=%s sl=%s tp=%s client_id=%s",
            side, qty, entry_price, sl_price, tp_price, client_order_id,
        )
        return OrderResult(True, "bracket placed", entry_order, sl_order, tp_order)

    def _emergency_protective_failure(self, entry_order: Any, qty: float, close_side: str, client_order_id: str) -> None:
        cancel = getattr(self.exchange, "cancel_order", None)
        flatten = getattr(self.exchange, "place_order", None)

        try:
            order_id = None
            if isinstance(entry_order, dict):
                order_id = entry_order.get("id") or entry_order.get("order_id")
            else:
                order_id = getattr(entry_order, "id", getattr(entry_order, "order_id", None))
            if callable(cancel) and order_id:
                cancel(symbol=self.symbol, order_id=order_id)
                logger.warning("ENTRY_CANCELLED_AFTER_PROTECTION_FAILURE order_id=%s", order_id)
                return
        except Exception:
            logger.exception("failed to cancel entry after protective failure")

        try:
            if callable(flatten):
                flatten(
                    symbol=self.symbol,
                    side=close_side,
                    quantity=qty,
                    order_type="market",
                    price=None,
                    client_order_id=f"{client_order_id}-emergency-flat",
                    reduce_only=True,
                )
                logger.warning("EMERGENCY_FLATTEN_SENT_AFTER_PROTECTION_FAILURE")
        except Exception:
            logger.exception("emergency flatten failed after protective failure")

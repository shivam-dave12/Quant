from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any


class ZerodhaUnavailable(RuntimeError):
    pass


def _iter_dicts(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        out = [payload]
        for value in payload.values():
            out.extend(_iter_dicts(value))
        return out
    if isinstance(payload, list):
        out: list[dict[str, Any]] = []
        for item in payload:
            out.extend(_iter_dicts(item))
        return out
    return []


def _first_value(payload: Any, *keys: str) -> Any:
    lowered = {key.lower() for key in keys}
    for obj in _iter_dicts(payload):
        for key, value in obj.items():
            if str(key).lower() in lowered and value not in (None, ""):
                return value
    return None


@dataclass
class ZerodhaAdapter:
    api_key: str
    api_secret: str = ""
    access_token: str = ""

    def __post_init__(self) -> None:
        if not self.api_key:
            raise ZerodhaUnavailable("ZERODHA_API_KEY is missing")
        try:
            from kiteconnect import KiteConnect  # type: ignore
        except Exception as exc:  # pragma: no cover
            raise ZerodhaUnavailable("kiteconnect package is not installed. Run: pip install -r requirements.txt") from exc
        self.kite = KiteConnect(api_key=self.api_key)
        if self.access_token:
            self.kite.set_access_token(self.access_token)

    @property
    def has_access_token(self) -> bool:
        return bool(self.access_token)

    def login_url(self) -> str:
        return str(self.kite.login_url())

    def generate_session(self, request_token: str) -> dict[str, Any]:
        if not self.api_secret:
            raise ZerodhaUnavailable("ZERODHA_API_SECRET is missing")
        session = self.kite.generate_session(request_token, api_secret=self.api_secret)
        token = session.get("access_token")
        if token:
            self.access_token = str(token)
            self.kite.set_access_token(self.access_token)
        return session

    def profile(self) -> dict[str, Any]:
        return self.kite.profile()

    def margins(self) -> dict[str, Any]:
        return self.kite.margins()

    def positions(self) -> dict[str, Any]:
        return self.kite.positions()

    def orders(self) -> list[dict[str, Any]]:
        return list(self.kite.orders())

    def order_margins(self, orders: list[dict[str, Any]]) -> Any:
        return self.kite.order_margins(orders)

    def place_limit_order(
        self,
        trading_symbol: str,
        side: str,
        quantity: int,
        price: float,
        product: str = "MIS",
        exchange: str = "MCX",
        tag: str | None = None,
    ) -> dict[str, Any]:
        side = str(side or "BUY").upper()
        order_id = self.kite.place_order(
            variety=self.kite.VARIETY_REGULAR,
            exchange=exchange,
            tradingsymbol=trading_symbol,
            transaction_type=self.kite.TRANSACTION_TYPE_SELL if side == "SELL" else self.kite.TRANSACTION_TYPE_BUY,
            quantity=int(quantity),
            product=product,
            order_type=self.kite.ORDER_TYPE_LIMIT,
            price=float(price),
            validity=self.kite.VALIDITY_DAY,
            tag=tag,
        )
        return {"order_id": order_id, "status": "PLACED", "broker": "zerodha"}

    def get_order_detail(self, order_id: str) -> dict[str, Any]:
        history = self.kite.order_history(order_id)
        if isinstance(history, list) and history:
            return dict(history[-1])
        return {"order_id": order_id, "history": history}

    def wait_for_fill(self, order_id: str, timeout_seconds: int = 15) -> dict[str, Any]:
        deadline = time.time() + timeout_seconds
        last: dict[str, Any] = {}
        while time.time() < deadline:
            last = self.get_order_detail(order_id)
            status = self.extract_order_status(last)
            filled = self.extract_filled_quantity(last)
            if status == "COMPLETE" and filled > 0:
                return last
            if status in {"REJECTED", "CANCELLED", "CANCELED"}:
                return last
            time.sleep(0.75)
        return last

    @staticmethod
    def extract_order_id(payload: dict[str, Any]) -> str | None:
        value = _first_value(payload, "order_id", "orderId", "id")
        return str(value) if value not in (None, "") else None

    @staticmethod
    def extract_order_reference_id(payload: dict[str, Any]) -> str | None:
        value = _first_value(payload, "tag", "order_reference_id", "reference_id")
        return str(value) if value not in (None, "") else None

    @staticmethod
    def extract_order_status(payload: dict[str, Any]) -> str:
        value = _first_value(payload, "status", "order_status")
        return str(value or "").upper()

    @staticmethod
    def extract_filled_quantity(payload: dict[str, Any]) -> int:
        value = _first_value(payload, "filled_quantity", "filledQuantity", "filled_qty")
        try:
            return int(float(value or 0))
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def extract_average_fill_price(payload: dict[str, Any], fallback: float) -> float:
        value = _first_value(payload, "average_price", "averagePrice", "avg_price", "price")
        try:
            out = float(value)
        except (TypeError, ValueError):
            return float(fallback)
        return out if out > 0 else float(fallback)

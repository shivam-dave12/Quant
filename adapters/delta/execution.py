"""Authenticated Delta native bracket transport: never submits a non-protected order."""
from __future__ import annotations
import hashlib, hmac, json, time
from typing import Any, Callable
import requests

class DeltaSignedBracketTransport:
    BASE_URL = "https://api.india.delta.exchange"
    def __init__(self, api_key: str, api_secret: str, *, session: Any | None = None, timestamp_provider: Callable[[], int] | None = None,
                 timeout: tuple[int, int] = (3, 27)) -> None:
        if not api_key or not api_secret: raise ValueError("DELTA_API_KEY_AND_SECRET_REQUIRED")
        self.api_key, self.api_secret = api_key, api_secret; self.session = session or requests.Session(); self.timestamp_provider = timestamp_provider or (lambda: int(time.time())); self.timeout = timeout
    def _signed_request(self, method: str, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = "" if body is None else json.dumps(body, separators=(",", ":")); timestamp = str(self.timestamp_provider())
        signature_data = method.upper() + timestamp + path + "" + payload
        signature = hmac.new(self.api_secret.encode(), signature_data.encode(), hashlib.sha256).hexdigest()
        headers = {"api-key": self.api_key, "timestamp": timestamp, "signature": signature, "User-Agent": "institutional-platform-v517",
                   "Content-Type": "application/json", "Accept": "application/json"}
        response = self.session.request(method.upper(), self.BASE_URL + path, data=payload, headers=headers, timeout=self.timeout); response.raise_for_status(); result = response.json()
        if not result.get("success", False): raise RuntimeError(f"DELTA_API_REJECTED:{result.get('error')}")
        return dict(result.get("result") or {})
    def place_protected_order(self, *, product_id: int, product_symbol: str, side: str, quantity: float, entry_price: float, stop_price: float, target_price: float) -> dict[str, Any]:
        side = side.lower(); order_side = "buy" if side in {"long", "buy"} else "sell" if side in {"short", "sell"} else ""
        if not order_side or min(quantity, entry_price, stop_price, target_price) <= 0: raise ValueError("VALIDATED_DELTA_PROTECTED_INPUTS_REQUIRED")
        if order_side == "buy" and not stop_price < entry_price < target_price: raise ValueError("INVALID_LONG_PROTECTION_GEOMETRY")
        if order_side == "sell" and not target_price < entry_price < stop_price: raise ValueError("INVALID_SHORT_PROTECTION_GEOMETRY")
        body = {"product_id": int(product_id), "product_symbol": product_symbol, "size": quantity, "side": order_side, "order_type": "limit_order",
                "limit_price": str(entry_price), "time_in_force": "gtc", "post_only": False, "reduce_only": False,
                "bracket_stop_trigger_method": "mark_price", "bracket_stop_loss_limit_price": str(stop_price), "bracket_stop_loss_price": str(stop_price),
                "bracket_take_profit_limit_price": str(target_price), "bracket_take_profit_price": str(target_price)}
        return self._signed_request("POST", "/v2/orders", body)
    def get_order(self, order_id: str) -> dict[str, Any]: return self._signed_request("GET", f"/v2/orders/{order_id}")
    def get_position(self, product_id: int) -> dict[str, Any]: return self._signed_request("GET", f"/v2/positions?product_id={product_id}")

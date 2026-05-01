"""CoinSwitch Pro Futures REST API adapter.

The strategy layer never calls this class directly.  It is consumed through
execution.order_manager._CoinSwitchAdapter, which expects CoinSwitch quantities
in BTC units and plain dict responses.
"""
from __future__ import annotations

import json
import logging
import os
import time
import urllib.parse
from typing import Any, Dict, Optional
from urllib.parse import urlencode

import requests
from cryptography.hazmat.primitives.asymmetric import ed25519
try:
    from dotenv import load_dotenv
except ImportError:  # optional in hardened/test environments
    def load_dotenv(*args, **kwargs):
        return False

import sys, os as _os
sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))
import config

load_dotenv()
logger = logging.getLogger(__name__)


class FuturesAPI:
    """CoinSwitch Futures Trading API client.

    Returns dicts for every public method. Network/HTTP/JSON failures are
    normalised to {"error": ..., "status_code": ...}; callers should not need
    try/except around normal exchange failures.
    """

    def __init__(self, api_key: Optional[str] = None, secret_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("COINSWITCH_API_KEY", "")
        self.secret_key = secret_key or os.getenv("COINSWITCH_SECRET_KEY", "")
        self.base_url = getattr(config, "COINSWITCH_BASE_URL", "https://coinswitch.co")
        if not self.api_key or not self.secret_key:
            raise ValueError("CoinSwitch API key and secret key required")

    def _payload_json_for_signature(self, method: str, payload: Optional[Dict[str, Any]]) -> str:
        """CoinSwitch historically signs {} for body endpoints.

        Some CoinSwitch examples sign the literal body.  Keep the runtime
        selectable so production can match the active account/doc behaviour
        without changing trading logic.
        """
        if method.upper() in ("POST", "DELETE", "PUT") and bool(getattr(config, "COINSWITCH_SIGN_BODY", False)):
            return json.dumps(payload or {}, separators=(",", ":"), sort_keys=True)
        return "{}"

    def _generate_signature(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> str:
        params = params or {}
        method = method.upper()
        signature_endpoint = endpoint
        if method == "GET" and params:
            signature_endpoint = f"{endpoint}?{urlencode(params)}"

        canonical = (
            urllib.parse.unquote_plus(signature_endpoint)
            if bool(getattr(config, "COINSWITCH_SIGN_UNQUOTED", True))
            else signature_endpoint
        )
        signature_msg = method + canonical + self._payload_json_for_signature(method, payload)
        secret_key_obj = ed25519.Ed25519PrivateKey.from_private_bytes(bytes.fromhex(self.secret_key))
        return secret_key_obj.sign(signature_msg.encode("utf-8")).hex()

    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        method = method.upper()
        params = params or {}
        payload = payload or {}
        signature = self._generate_signature(method, endpoint, params, payload)

        url = self.base_url + endpoint
        if method == "GET" and params:
            url = f"{url}?{urlencode(params)}"

        headers = {
            "Content-Type": "application/json",
            "X-AUTH-SIGNATURE": signature,
            "X-AUTH-APIKEY": self.api_key,
        }
        timeout = float(getattr(config, "REQUEST_TIMEOUT", 30))

        try:
            if method == "GET":
                resp = requests.request(method, url, headers=headers, timeout=timeout)
            else:
                resp = requests.request(method, url, headers=headers, json=payload, timeout=timeout)
            status = resp.status_code
            try:
                body = resp.json()
            except ValueError as exc:
                return {"error": f"invalid_json: {exc}", "status_code": status, "response": resp.text[:500]}
            if not resp.ok:
                err = body.get("error") or body.get("message") or str(body)
                logger.error("CoinSwitch API error %s [%s %s]: %s", status, method, endpoint, err)
                return {"error": err, "status_code": status, "response": body}
            return body if isinstance(body, dict) else {"data": body, "status_code": status}
        except requests.exceptions.RequestException as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            return {"error": str(exc), "status_code": status}

    # Orders
    def place_order(self, symbol: str, side: str, order_type: str, quantity: float,
                    exchange: str = "EXCHANGE_2", price: Optional[float] = None,
                    trigger_price: Optional[float] = None, reduce_only: bool = False) -> Dict[str, Any]:
        payload = {
            "symbol": symbol,
            "exchange": exchange,
            "side": side,
            "order_type": order_type,
            "quantity": float(quantity),
            "reduce_only": bool(reduce_only),
        }
        if price is not None:
            payload["price"] = float(price)
        if trigger_price is not None:
            payload["trigger_price"] = float(trigger_price)
        return self._make_request("POST", "/trade/api/v2/futures/order", payload=payload)

    def get_order(self, order_id: str, exchange: str = "EXCHANGE_2") -> Dict[str, Any]:
        return self._make_request("GET", "/trade/api/v2/futures/order", params={"order_id": order_id, "exchange": exchange})

    def get_open_orders(self, exchange: str = "EXCHANGE_2", symbol: Optional[str] = None) -> Dict[str, Any]:
        params = {"exchange": exchange}
        if symbol:
            params["symbol"] = symbol
        return self._make_request("GET", "/trade/api/v2/futures/open_orders", params=params)

    def cancel_order(self, order_id: str, exchange: str = "EXCHANGE_2") -> Dict[str, Any]:
        return self._make_request("DELETE", "/trade/api/v2/futures/order", payload={"order_id": order_id, "exchange": exchange})

    def cancel_all_orders(self, exchange: str = "EXCHANGE_2", symbol: Optional[str] = None) -> Dict[str, Any]:
        payload = {"exchange": exchange}
        if symbol:
            payload["symbol"] = symbol
        return self._make_request("POST", "/trade/api/v2/futures/cancel_all", payload=payload)

    # Account / position
    def set_leverage(self, symbol: str, exchange: str, leverage: int) -> Dict[str, Any]:
        return self._make_request("POST", "/trade/api/v2/futures/leverage", payload={"symbol": symbol, "exchange": exchange, "leverage": int(leverage)})

    def get_positions(self, exchange: str = "EXCHANGE_2", symbol: Optional[str] = None) -> Dict[str, Any]:
        params = {"exchange": exchange}
        if symbol:
            params["symbol"] = symbol
        return self._make_request("GET", "/trade/api/v2/futures/positions", params=params)

    def get_wallet_balance(self) -> Dict[str, Any]:
        return self._make_request("GET", "/trade/api/v2/futures/wallet_balance", params={})

    def get_balance(self, currency: str = "USDT") -> Dict[str, Any]:
        resp = self.get_wallet_balance()
        if resp.get("error"):
            return resp
        data = resp.get("data", resp)
        # Accept several wallet shapes.  Keep units in account currency.
        if isinstance(data, dict):
            wallets = data.get("wallets") or data.get("balances") or data.get("data") or data
        else:
            wallets = data
        chosen = None
        if isinstance(wallets, list):
            for item in wallets:
                if isinstance(item, dict) and str(item.get("currency", item.get("asset", ""))).upper() == currency.upper():
                    chosen = item
                    break
            chosen = chosen or (wallets[0] if wallets else {})
        elif isinstance(wallets, dict):
            chosen = wallets.get(currency) if isinstance(wallets.get(currency), dict) else wallets
        else:
            chosen = {}
        available = float(chosen.get("available", chosen.get("available_balance", chosen.get("free", 0.0))) or 0.0)
        locked = float(chosen.get("locked", chosen.get("used", chosen.get("blocked", 0.0))) or 0.0)
        total = float(chosen.get("total", chosen.get("balance", available + locked)) or (available + locked))
        return {"available": available, "locked": locked, "total": total, "currency": currency}

    # Market data
    def get_klines(self, symbol: str, interval: int, limit: int = 100, exchange: str = "EXCHANGE_2", **kwargs) -> Dict[str, Any]:
        now_ms = int(time.time() * 1000)
        start_ms = now_ms - int(limit) * int(interval) * 60 * 1000
        params = {
            "symbol": symbol,
            "exchange": exchange,
            "interval": str(interval),
            "start_time": kwargs.get("start_time", start_ms),
            "end_time": kwargs.get("end_time", now_ms),
            "limit": limit,
        }
        return self._make_request("GET", "/trade/api/v2/futures/klines", params=params)

from __future__ import annotations

import hashlib
import hmac
import json
import time
from dataclasses import asdict
from typing import Any
from urllib.parse import urlencode

import requests

from .types import BracketPlan, Side


LIVE_REST = "https://api.india.delta.exchange"
TEST_REST = "https://cdn-ind.testnet.deltaex.org"


class DeltaRestClient:
    def __init__(self, api_key: str | None, secret_key: str | None, testnet: bool = True, timeout: float = 8.0) -> None:
        self.api_key = api_key or ""
        self.secret_key = secret_key or ""
        self.base = TEST_REST if testnet else LIVE_REST
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json", "Accept": "application/json"})

    def _sign(self, method: str, path: str, query: str = "", body: str = "") -> dict[str, str]:
        ts = str(int(time.time()))
        prehash = method.upper() + ts + path + (("?" + query) if query else "") + body
        sig = hmac.new(self.secret_key.encode(), prehash.encode(), hashlib.sha256).hexdigest()
        return {"api-key": self.api_key, "signature": sig, "timestamp": ts}

    def request(self, method: str, path: str, params: dict[str, Any] | None = None, body: dict[str, Any] | None = None, auth: bool = False) -> dict[str, Any]:
        params = params or {}
        query = urlencode(params, doseq=True)
        payload = json.dumps(body, separators=(",", ":")) if body is not None else ""
        headers = self._sign(method, path, query, payload) if auth else {}
        r = self.session.request(method, self.base + path + (("?" + query) if query else ""), data=payload or None, headers=headers, timeout=self.timeout)
        try:
            data = r.json()
        except Exception:
            data = {"success": False, "error": r.text}
        if r.status_code >= 400:
            return {"success": False, "status_code": r.status_code, "error": data}
        return data

    def get_product(self, symbol: str) -> dict[str, Any]:
        resp = self.request("GET", "/v2/products", params={"symbol": symbol, "page_size": 1})
        if resp.get("success") and isinstance(resp.get("result"), list) and resp["result"]:
            return resp["result"][0]
        # fallback if API returns direct by id/symbol in user's plugin contract
        raise RuntimeError(f"Could not fetch product metadata for {symbol}: {resp}")

    def get_wallet_balances(self) -> dict[str, Any]:
        return self.request("GET", "/v2/wallet/balances", auth=True)

    def get_positions(self, symbol: str) -> dict[str, Any]:
        return self.request("GET", "/v2/positions", params={"product_symbol": symbol}, auth=True)

    def list_fills_paginated(self, symbol: str, after: str | None = None, page_size: int = 100) -> dict[str, Any]:
        params: dict[str, Any] = {"product_symbols": symbol, "page_size": page_size}
        if after:
            params["after"] = after
        return self.request("GET", "/v2/fills", params=params, auth=True)

    def place_atomic_bracket_market(self, symbol: str, product_id: int, plan: BracketPlan, client_order_id: str) -> dict[str, Any]:
        body = {
            "product_id": int(product_id),
            "size": int(plan.quantity_contracts),
            "side": "buy" if plan.side is Side.LONG else "sell",
            "order_type": "market_order",
            "client_order_id": client_order_id[:32],
            "bracket_stop_loss_price": str(round(plan.stop_price, 2)),
            "bracket_take_profit_price": str(round(plan.take_profit_price, 2)),
        }
        return self.request("POST", "/v2/orders", body=body, auth=True)


class PaperExecutor:
    def __init__(self) -> None:
        self.orders: list[dict[str, Any]] = []

    def place_atomic_bracket_market(self, symbol: str, product_id: int, plan: BracketPlan, client_order_id: str) -> dict[str, Any]:
        row = {"success": True, "paper": True, "symbol": symbol, "product_id": product_id, "client_order_id": client_order_id, "plan": asdict(plan), "result": {"id": client_order_id}}
        self.orders.append(row)
        return row

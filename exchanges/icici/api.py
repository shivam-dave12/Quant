"""Minimal institutional ICICI Breeze REST client."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import requests

from .breeze_auth import BreezeTokenService


class BreezeRestClient:
    BASE_URL = "https://api.icicidirect.com/breezeapi/api/v1"

    def __init__(self, auth: BreezeTokenService | None = None) -> None:
        self.auth = auth or BreezeTokenService()
        self.http = requests.Session()

    def _timestamp(self) -> str:
        return datetime.now(timezone.utc).isoformat()[:19] + ".000Z"

    def _payload(self, body: Optional[Dict[str, Any]]) -> str:
        return json.dumps(body or {}, separators=(",", ":"))

    def _headers(self, payload: str) -> Dict[str, str]:
        session = self.auth.get_session()
        ts = self._timestamp()
        checksum = hashlib.sha256((ts + payload + self.auth.secret_key).encode("utf-8")).hexdigest()
        return {
            "Content-Type": "application/json",
            "X-Checksum": "token " + checksum,
            "X-Timestamp": ts,
            "X-AppKey": self.auth.api_key,
            "X-SessionToken": session.session_token,
        }

    def request(self, method: str, path: str, body: Optional[Dict[str, Any]] = None, *, timeout: float = 20.0) -> Dict[str, Any]:
        payload = self._payload(body)
        resp = self.http.request(
            method.upper(),
            self.BASE_URL + path,
            headers=self._headers(payload),
            data=payload,
            timeout=timeout,
        )
        try:
            data = resp.json()
        except ValueError as exc:
            raise RuntimeError(f"Breeze {path} returned non-JSON HTTP {resp.status_code}") from exc
        if resp.status_code >= 400 or data.get("Error"):
            raise RuntimeError(f"Breeze {path} failed HTTP {resp.status_code}: {data.get('Error') or data}")
        return data

    def get_funds(self) -> Dict[str, Any]:
        return self.request("GET", "/funds", {})

    def get_margin(self, exchange_code: str = "NFO") -> Dict[str, Any]:
        return self.request("GET", "/margin", {"exchange_code": exchange_code})

    def get_quotes(self, **kwargs) -> Dict[str, Any]:
        return self.request("GET", "/quotes", kwargs)

    def get_portfolio_positions(self) -> Dict[str, Any]:
        return self.request("GET", "/portfolio", {})

    def place_order(self, **kwargs) -> Dict[str, Any]:
        order_type = str(kwargs.get("order_type", "")).lower()
        if order_type != "limit":
            raise RuntimeError("ICICI Breeze institutional guard: market orders are not permitted; use order_type='limit'")
        required = ("stock_code", "exchange_code", "product", "action", "quantity", "price", "validity")
        missing = [k for k in required if kwargs.get(k) in (None, "")]
        if missing:
            raise RuntimeError("ICICI Breeze order missing required fields: " + ", ".join(missing))
        return self.request("POST", "/order", kwargs)

    def cancel_order(self, **kwargs) -> Dict[str, Any]:
        return self.request("DELETE", "/order", kwargs)

    def modify_order(self, **kwargs) -> Dict[str, Any]:
        return self.request("PUT", "/order", kwargs)

    def square_off(self, **kwargs) -> Dict[str, Any]:
        return self.request("POST", "/squareoff", kwargs)

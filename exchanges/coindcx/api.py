"""Minimal CoinDCX REST adapter.

This optional client is intentionally conservative: public market-data methods
are ready, private trading calls require explicit credentials and can be wired
into the institutional execution desk later.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
from typing import Any, Dict, Optional

import requests

try:
    import config
except Exception:  # pragma: no cover
    config = None  # type: ignore


def _cfg(name: str, default: Any) -> Any:
    if config is None:
        return default
    return getattr(config, name, default)


class CoinDCXRestClient:
    PUBLIC_BASE = "https://api.coindcx.com/exchange"
    PRIVATE_BASE = "https://api.coindcx.com"

    def __init__(self, api_key: str | None = None, secret_key: str | None = None) -> None:
        self.api_key = api_key if api_key is not None else str(_cfg("COINDCX_API_KEY", ""))
        self.secret_key = secret_key if secret_key is not None else str(_cfg("COINDCX_SECRET_KEY", ""))
        self.http = requests.Session()

    def get_tickers(self) -> Dict[str, Any] | list:
        resp = self.http.get(self.PUBLIC_BASE + "/ticker", timeout=15)
        resp.raise_for_status()
        return resp.json()

    def get_markets(self) -> Dict[str, Any] | list:
        resp = self.http.get(self.PUBLIC_BASE + "/v1/markets_details", timeout=15)
        resp.raise_for_status()
        return resp.json()

    def private_request(self, method: str, path: str, body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if not self.api_key or not self.secret_key:
            raise RuntimeError("CoinDCX credentials are not configured")
        payload = dict(body or {})
        payload.setdefault("timestamp", int(time.time() * 1000))
        payload_json = json.dumps(payload, separators=(",", ":"))
        signature = hmac.new(
            self.secret_key.encode("utf-8"),
            payload_json.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        headers = {
            "Content-Type": "application/json",
            "X-AUTH-APIKEY": self.api_key,
            "X-AUTH-SIGNATURE": signature,
        }
        resp = self.http.request(method.upper(), self.PRIVATE_BASE + path, headers=headers, data=payload_json, timeout=20)
        try:
            data = resp.json()
        except ValueError as exc:
            raise RuntimeError(f"CoinDCX {path} returned non-JSON HTTP {resp.status_code}") from exc
        if resp.status_code >= 400:
            raise RuntimeError(f"CoinDCX {path} failed HTTP {resp.status_code}: {data}")
        return data

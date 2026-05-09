"""Minimal institutional ICICI Breeze REST client.

The client follows the public Breeze API contract:
- API_Session is exchanged through CustomerDetails without signed headers.
- Protected endpoints use X-Checksum, X-Timestamp, X-AppKey and X-SessionToken.
- The Security Master download remains public and is intentionally kept outside
  the signed API quota path.
"""

from __future__ import annotations

import hashlib
import json
import csv
import io
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional

import requests

from .breeze_auth import BreezeTokenService


class BreezeRestClient:
    BASE_URL = "https://api.icicidirect.com/breezeapi/api/v1"
    SECURITY_MASTER_URL = "http://directlink.icicidirect.com/NewSecurityMaster/SecurityMaster.zip"
    SECURITY_MASTER_FALLBACK_URLS = (
        "https://directlink.icicidirect.com/NewSecurityMaster/SecurityMaster.zip",
        "https://api.icicidirect.com/breezeapi/documents/securitymaster.zip",
    )

    def __init__(self, auth: BreezeTokenService | None = None) -> None:
        self.auth = auth or BreezeTokenService()
        self.http = requests.Session()

    def _timestamp(self) -> str:
        return datetime.now(timezone.utc).isoformat()[:19] + ".000Z"

    def _payload(self, body: Optional[Dict[str, Any]]) -> str:
        return json.dumps(body or {}, separators=(",", ":"))

    def _headers(self, payload: str, *, force_refresh: bool = False) -> Dict[str, str]:
        session = self.auth.get_session(force_refresh=force_refresh)
        ts = self._timestamp()
        checksum = hashlib.sha256((ts + payload + self.auth.secret_key).encode("utf-8")).hexdigest()
        return {
            "Content-Type": "application/json",
            "X-Checksum": "token " + checksum,
            "X-Timestamp": ts,
            "X-AppKey": self.auth.api_key,
            "X-SessionToken": session.session_token,
        }

    def preflight_session(self, *, force_refresh: bool = False) -> dict:
        """Generate/validate the Breeze session before protected API use."""
        return self.auth.get_session(force_refresh=force_refresh).masked()

    def is_auth_ready(self) -> bool:
        try:
            self.auth.get_session(force_refresh=False)
            return True
        except Exception:
            return False

    def request(self, method: str, path: str, body: Optional[Dict[str, Any]] = None, *, timeout: float = 20.0) -> Dict[str, Any]:
        payload = self._payload(body)
        resp = self.http.request(
            method.upper(),
            self.BASE_URL + path,
            headers=self._headers(payload),
            data=payload,
            timeout=timeout,
        )
        data = self._json_or_raise(resp, path)
        if self._is_auth_error(resp, data) and self.auth.can_refresh_without_operator():
            resp = self.http.request(
                method.upper(),
                self.BASE_URL + path,
                headers=self._headers(payload, force_refresh=True),
                data=payload,
                timeout=timeout,
            )
            data = self._json_or_raise(resp, path)
        if resp.status_code >= 400 or data.get("Error"):
            raise RuntimeError(f"Breeze {path} failed HTTP {resp.status_code}: {data.get('Error') or data}")
        return data

    @staticmethod
    def _json_or_raise(resp: requests.Response, path: str) -> Dict[str, Any]:
        try:
            data = resp.json()
        except ValueError as exc:
            raise RuntimeError(f"Breeze {path} returned non-JSON HTTP {resp.status_code}") from exc
        if not isinstance(data, dict):
            raise RuntimeError(f"Breeze {path} returned unexpected payload type: {type(data).__name__}")
        return data

    @staticmethod
    def _is_auth_error(resp: requests.Response, data: Mapping[str, Any]) -> bool:
        if resp.status_code in {401, 403}:
            return True
        msg = str(data.get("Error") or data.get("Status") or data.get("message") or "").lower()
        return any(x in msg for x in ("session", "token", "unauthor", "auth"))

    def get_funds(self) -> Dict[str, Any]:
        return self.request("GET", "/funds", {})

    def get_margin(self, exchange_code: str = "NFO") -> Dict[str, Any]:
        return self.request("GET", "/margin", {"exchange_code": exchange_code})

    def get_quotes(self, **kwargs) -> Dict[str, Any]:
        return self.request("GET", "/quotes", kwargs)

    def get_option_chain_quotes(self, **kwargs) -> Dict[str, Any]:
        return self.request("GET", "/OptionChain", kwargs)

    def get_historical_charts(self, **kwargs) -> Dict[str, Any]:
        return self.request("GET", "/historicalcharts", kwargs)

    def get_security_master_rows(self, *, url: str | None = None, cache_path: str | Path | None = None, timeout: float = 12.0) -> list[dict[str, str]]:
        """Download and parse ICICI's daily Security Master file.

        This endpoint is public per Breeze docs.  Startup should prefer a cached
        master over zero Indian-market coverage when ICICI's directlink endpoint
        is slow.
        """
        sources = [url or self.SECURITY_MASTER_URL]
        for fallback in self.SECURITY_MASTER_FALLBACK_URLS:
            if fallback not in sources:
                sources.append(fallback)
        data: bytes
        path = Path(cache_path) if cache_path else None
        if path and path.exists():
            try:
                return self._parse_security_master_zip(path.read_bytes())
            except Exception:
                # Corrupt cache should not block a fresh attempt.
                pass
        last_exc: Exception | None = None
        for source in sources:
            try:
                data = self._download_security_master(source, timeout)
                if path:
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(data)
                return self._parse_security_master_zip(data)
            except Exception as exc:
                last_exc = exc
                continue
        if path and path.exists():
            return self._parse_security_master_zip(path.read_bytes())
        if last_exc:
            raise last_exc
        return []

    def get_quote_for_instrument(self, instrument: Any) -> Dict[str, Any]:
        """Best-effort Breeze quote wrapper for a discovered ICICI instrument."""
        raw = getattr(instrument, "raw", None) or {}
        stock_code = raw.get("stock_code") or raw.get("StockCode") or raw.get("ShortName") or getattr(instrument, "asset_id", "")
        exchange_code = raw.get("exchange_code") or raw.get("ExchangeCode") or raw.get("Exchange") or "NSE"
        product_type = self._normalise_product_type(raw)
        body = {
            "stock_code": str(stock_code).upper(),
            "exchange_code": str(exchange_code).upper(),
            "product_type": product_type,
        }
        expiry = self._normalise_expiry(raw.get("expiry_date") or raw.get("ExpiryDate") or raw.get("Expiry") or "")
        right = self._normalise_right(raw.get("right") or raw.get("OptionType") or raw.get("Right") or raw.get("CallPut") or "")
        strike = str(raw.get("strike_price") or raw.get("StrikePrice") or raw.get("Strike") or raw.get("StrikeRate") or "")
        if product_type in {"options", "futures"}:
            if expiry:
                body["expiry_date"] = expiry
        if product_type == "options":
            if right:
                body["right"] = right
            if strike:
                body["strike_price"] = strike
        return self.get_quotes(**{k: v for k, v in body.items() if v not in (None, "")})

    def get_portfolio_holdings(self, **kwargs) -> Dict[str, Any]:
        return self.request("GET", "/portfolioholdings", kwargs)

    def get_portfolio_positions(self) -> Dict[str, Any]:
        return self.request("GET", "/portfoliopositions", {})

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

    @staticmethod
    def _download_security_master(url: str, timeout: float) -> bytes:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.content

    @staticmethod
    def _normalise_product_type(raw: Mapping[str, Any]) -> str:
        val = str(raw.get("product_type") or raw.get("ProductType") or raw.get("InstrumentType") or raw.get("Series") or "").strip()
        low = val.lower()
        if low in {"options", "option", "opt", "ce", "pe"} or raw.get("OptionType") or raw.get("StrikePrice"):
            return "options"
        if low in {"futures", "future", "fut"}:
            return "futures"
        return "Cash"

    @staticmethod
    def _normalise_right(value: Any) -> str:
        v = str(value or "").strip().lower()
        if v in {"c", "ce", "call"}:
            return "call"
        if v in {"p", "pe", "put"}:
            return "put"
        return v

    @staticmethod
    def _normalise_expiry(value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        if "T" in text and text.endswith("Z"):
            return text
        for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%d/%m/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(text, fmt).strftime("%Y-%m-%dT06:00:00.000Z")
            except Exception:
                continue
        return text

    @staticmethod
    def _parse_security_master_zip(data: bytes) -> list[dict[str, str]]:
        rows: list[dict[str, str]] = []
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            for name in zf.namelist():
                if not name.lower().endswith((".csv", ".txt")):
                    continue
                raw = zf.read(name)
                text = raw.decode("utf-8", errors="ignore")
                sample = text[:4096]
                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters=",|\t")
                except Exception:
                    dialect = csv.excel
                reader = csv.DictReader(io.StringIO(text), dialect=dialect)
                for row in reader:
                    cleaned = {str(k or "").strip(): str(v or "").strip() for k, v in row.items()}
                    if cleaned:
                        cleaned["_source_file"] = name
                        rows.append(cleaned)
        return rows

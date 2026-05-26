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
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional

import requests

from .breeze_auth import BreezeTokenService


class BreezeRestClient:
    BASE_URL = "https://api.icicidirect.com/breezeapi/api/v1"
    SECURITY_MASTER_URL = "https://directlink.icicidirect.com/NewSecurityMaster/SecurityMaster.zip"
    SECURITY_MASTER_FALLBACK_URLS = (
        "https://api.icicidirect.com/breezeapi/documents/securitymaster.zip",
    )

    def __init__(self, auth: BreezeTokenService | None = None) -> None:
        self.auth = auth or BreezeTokenService()
        self.http = requests.Session()

    def _timestamp(self) -> str:
        return datetime.now(timezone.utc).isoformat()[:19] + ".000Z"

    def _payload(self, body: Optional[Dict[str, Any]]) -> str:
        return json.dumps(body or {}, separators=(",", ":"))

    @staticmethod
    def _compact_body(body: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
        return {str(k): v for k, v in (body or {}).items() if v not in (None, "")}

    @classmethod
    def _ordered_body(cls, source: Mapping[str, Any], keys: Iterable[str]) -> Dict[str, Any]:
        return cls._compact_body({key: source.get(key) for key in keys})

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
            # Match the official Breeze Python SDK.  Some Breeze market-data
            # endpoints are unexpectedly sensitive to this header and can
            # reject otherwise valid signed requests with misleading 401 text.
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_5_8) AppleWebKit/534.50.2 (KHTML, like Gecko) Version/5.0.6 Safari/533.22.3",
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
        body = self._ordered_body(
            kwargs,
            ("stock_code", "exchange_code", "expiry_date", "product_type", "right", "strike_price"),
        )
        return self.request("GET", "/quotes", body)

    def get_option_chain_quotes(self, **kwargs) -> Dict[str, Any]:
        # The official Breeze Python SDK uses the lower-case endpoint
        # ``optionchain``.  The static REST docs also show ``OptionChain`` in
        # places, but the SDK route is the most reliable production contract.
        body = self._ordered_body(
            kwargs,
            ("stock_code", "exchange_code", "expiry_date", "product_type", "right", "strike_price"),
        )
        return self.request("GET", "/optionchain", body)

    def get_historical_charts(self, **kwargs) -> Dict[str, Any]:
        body = self._ordered_body(
            kwargs,
            ("interval", "from_date", "to_date", "stock_code", "exchange_code", "product_type", "expiry_date", "right", "strike_price"),
        )
        if body.get("interval") == "1minute":
            body["interval"] = "minute"
        elif body.get("interval") == "1day":
            body["interval"] = "day"
        return self.request("GET", "/historicalcharts", body)

    def get_historical_charts_v2(self, **kwargs) -> Dict[str, Any]:
        """Breeze v2 historicalcharts fallback.

        ICICI documents both v1 signed `/historicalcharts` and v2
        `https://breezeapi.icicidirect.com/api/v2/historicalcharts`.  v2 uses
        `X-SessionToken` + `apikey` headers and query parameters.  We keep this
        as a read-only fallback only; orders still go through signed v1 routes.
        """
        session = self.auth.get_session(force_refresh=False)
        params = self._compact_body(kwargs)
        if "exchange_code" in params and "exch_code" not in params:
            params["exch_code"] = params.pop("exchange_code")
        headers = {
            "Content-Type": "application/json",
            "X-SessionToken": session.session_token,
            "apikey": self.auth.api_key,
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_5_8) AppleWebKit/534.50.2 (KHTML, like Gecko) Version/5.0.6 Safari/533.22.3",
        }
        resp = self.http.get(
            "https://breezeapi.icicidirect.com/api/v2/historicalcharts",
            headers=headers,
            params=params,
            timeout=20.0,
        )
        data = self._json_or_raise(resp, "/api/v2/historicalcharts")
        if resp.status_code >= 400 or data.get("Error"):
            raise RuntimeError(f"Breeze v2 historicalcharts failed HTTP {resp.status_code}: {data.get('Error') or data}")
        return data

    def get_security_master_rows(
        self, *, url: str | None = None, cache_path: str | Path | None = None,
        timeout: float = 12.0, require_current_trade_date: bool = False,
    ) -> list[dict[str, str]]:
        """Download and parse ICICI's daily Security Master file.

        A session-start option vehicle must use today's instrument definition
        when ``require_current_trade_date`` is true: stale lots/expiries cannot
        be used for live NFO routing.  Stale cache fallback remains available
        only to non-trading discovery callers.
        """
        sources = [url or self.SECURITY_MASTER_URL]
        for fallback in self.SECURITY_MASTER_FALLBACK_URLS:
            if fallback not in sources:
                sources.append(fallback)
        data: bytes
        path = Path(cache_path) if cache_path else None
        ist = timezone(timedelta(hours=5, minutes=30))
        today_ist = datetime.now(ist).date()
        cache_is_today = False
        if path and path.exists():
            try:
                cache_is_today = datetime.fromtimestamp(path.stat().st_mtime, tz=ist).date() == today_ist
            except Exception:
                cache_is_today = False
            if cache_is_today or not require_current_trade_date:
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
        if path and path.exists() and not require_current_trade_date:
            return self._parse_security_master_zip(path.read_bytes())
        if require_current_trade_date and path and path.exists() and not cache_is_today:
            raise RuntimeError("ICICI Security Master is stale for today's NFO session and refresh failed") from last_exc
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
        body = self._ordered_body(kwargs, ("exchange_code", "from_date", "to_date", "stock_code", "portfolio_type"))
        return self.request("GET", "/portfolioholdings", body)

    def get_portfolio_positions(self) -> Dict[str, Any]:
        try:
            return self.request("GET", "/portfoliopositions", {})
        except RuntimeError as exc:
            # Breeze returns HTTP 200 with Error="No Positions available." for
            # a verified flat derivatives book. That is not an auth/account
            # failure and must not disable the ICICI desk.
            if "no positions available" in str(exc).lower():
                return {"Success": [], "Status": 200, "Error": None, "_empty_positions": True}
            raise

    def place_order(self, **kwargs) -> Dict[str, Any]:
        order_type = str(kwargs.get("order_type", "")).strip().lower()
        if order_type not in {"limit", "stoploss"}:
            raise RuntimeError("ICICI Breeze institutional guard: market orders are prohibited; only limit or stoploss orders are permitted")
        required = ("stock_code", "exchange_code", "product", "action", "order_type", "quantity", "price", "validity")
        missing = [k for k in required if kwargs.get(k) in (None, "")]
        if missing:
            raise RuntimeError("ICICI Breeze order missing required fields: " + ", ".join(missing))
        action = str(kwargs.get("action", "")).strip().lower()
        if action not in {"buy", "sell"}:
            raise RuntimeError("ICICI Breeze order action must be buy or sell")
        product = str(kwargs.get("product", "")).strip().lower()
        if str(kwargs.get("exchange_code", "")).upper() == "NFO" and product == "options":
            option_required = ("expiry_date", "right", "strike_price")
            option_missing = [k for k in option_required if kwargs.get(k) in (None, "")]
            if option_missing:
                raise RuntimeError("ICICI NFO options order missing exact contract fields: " + ", ".join(option_missing))
        if order_type == "stoploss" and float(kwargs.get("stoploss", 0.0) or 0.0) <= 0:
            raise RuntimeError("ICICI Breeze stoploss order requires positive stoploss trigger")
        body = {
            "stock_code": str(kwargs.get("stock_code") or "").strip().upper(),
            "exchange_code": str(kwargs.get("exchange_code") or "").strip().upper(),
            "product": product,
            "action": action,
            "order_type": order_type,
            "quantity": kwargs.get("quantity"),
            "price": kwargs.get("price"),
            "validity": str(kwargs.get("validity") or "").strip().lower(),
            "settlement_id": kwargs.get("settlement_id"),
            "order_segment_code": kwargs.get("order_segment_code"),
            "lots": kwargs.get("lots"),
            "user_remark": kwargs.get("user_remark"),
            "stoploss": kwargs.get("stoploss"),
            "validity_date": kwargs.get("validity_date"),
            "disclosed_quantity": kwargs.get("disclosed_quantity"),
            "expiry_date": self._normalise_expiry(kwargs.get("expiry_date")),
            "right": self._normalise_right(kwargs.get("right")),
            "strike_price": kwargs.get("strike_price"),
            "order_type_fresh": kwargs.get("order_type_fresh"),
            "order_rate_fresh": kwargs.get("order_rate_fresh"),
        }
        return self.request("POST", "/order", self._compact_body(body))

    def get_order(self, **kwargs) -> Dict[str, Any]:
        if kwargs.get("order_id"):
            body = self._ordered_body(kwargs, ("exchange_code", "order_id"))
        else:
            body = self._ordered_body(kwargs, ("exchange_code", "from_date", "to_date"))
        return self.request("GET", "/order", body)

    def get_order_list(self, *, exchange_code: str = "NFO", from_date: str, to_date: str) -> Dict[str, Any]:
        return self.request("GET", "/order", {"exchange_code": exchange_code, "from_date": from_date, "to_date": to_date})

    def get_order_detail(self, **kwargs) -> Dict[str, Any]:
        return self.request("GET", "/order", self._ordered_body(kwargs, ("exchange_code", "order_id")))

    def cancel_order(self, **kwargs) -> Dict[str, Any]:
        return self.request("DELETE", "/order", self._ordered_body(kwargs, ("exchange_code", "order_id")))

    def modify_order(self, **kwargs) -> Dict[str, Any]:
        body = self._ordered_body(
            kwargs,
            ("order_id", "exchange_code", "order_type", "stoploss", "quantity", "price", "validity", "disclosed_quantity", "validity_date"),
        )
        return self.request("PUT", "/order", body)

    def square_off(self, **kwargs) -> Dict[str, Any]:
        product_type = kwargs.get("product_type", kwargs.get("product"))
        body = {
            "source_flag": kwargs.get("source_flag"),
            "stock_code": kwargs.get("stock_code"),
            "exchange_code": kwargs.get("exchange_code"),
            "quantity": kwargs.get("quantity"),
            "price": kwargs.get("price"),
            "action": kwargs.get("action"),
            "order_type": kwargs.get("order_type"),
            "validity": kwargs.get("validity"),
            "stoploss_price": kwargs.get("stoploss_price", kwargs.get("stoploss")),
            "disclosed_quantity": kwargs.get("disclosed_quantity"),
            "protection_percentage": kwargs.get("protection_percentage"),
            "settlement_id": kwargs.get("settlement_id"),
            "margin_amount": kwargs.get("margin_amount"),
            "open_quantity": kwargs.get("open_quantity"),
            "cover_quantity": kwargs.get("cover_quantity"),
            "product_type": product_type,
            "expiry_date": kwargs.get("expiry_date"),
            "right": kwargs.get("right"),
            "strike_price": kwargs.get("strike_price"),
            "validity_date": kwargs.get("validity_date"),
            "alias_name": kwargs.get("alias_name"),
            "trade_password": kwargs.get("trade_password"),
            "order_reference": kwargs.get("order_reference"),
            "position_exchange_code": kwargs.get("position_exchange_code"),
            "lots": kwargs.get("lots"),
        }
        return self.request("POST", "/squareoff", self._compact_body(body))

    def get_trade_list(self, **kwargs) -> Dict[str, Any]:
        body = self._ordered_body(kwargs, ("exchange_code", "from_date", "to_date", "product_type", "action", "stock_code"))
        return self.request("GET", "/trades", body)

    def get_trade_detail(self, **kwargs) -> Dict[str, Any]:
        return self.request("GET", "/trades", self._ordered_body(kwargs, ("exchange_code", "order_id")))

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

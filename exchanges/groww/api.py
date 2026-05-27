"""Official Groww Trading API compatibility wrapper.

The strategy already has a broker-agnostic option-desk flow, but parts of the
Indian options runtime expect Groww-like method names.  This wrapper maps those
calls to Groww's documented SDK methods while keeping order/data payloads in
Groww's official field vocabulary.
"""

from __future__ import annotations

import csv
import io
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Mapping, Optional

try:
    import config
except Exception:  # pragma: no cover
    config = None  # type: ignore


def _cfg(name: str, default: Any) -> Any:
    return getattr(config, name, default) if config is not None else default


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        if isinstance(value, str):
            value = value.strip().replace(",", "")
            if not value:
                return default
        return float(value)
    except Exception:
        return default


class GrowwRestClient:
    """Thin adapter over ``growwapi.GrowwAPI``.

    Official SDK contracts used here:
    - ``GrowwAPI(access_token)`` for client construction.
    - ``place_order(...)`` with Groww order fields.
    - ``get_quote(...)``, ``get_historical_candle_data(...)``.
    - ``get_available_margin_details()`` and ``get_positions_for_user(...)``.
    - ``create_smart_order(...)`` for OCO protection orders.
    """

    INSTRUMENTS_CSV_URL = "https://growwapi-assets.groww.in/instruments/instrument.csv"

    def __init__(
        self,
        *,
        access_token: str | None = None,
        api_key: str | None = None,
        api_secret: str | None = None,
        totp_secret: str | None = None,
    ) -> None:
        self.access_token = (access_token or _cfg("GROWW_ACCESS_TOKEN", "") or os.getenv("GROWW_ACCESS_TOKEN", "")).strip()
        self.api_key = (api_key or _cfg("GROWW_API_KEY", "") or os.getenv("GROWW_API_KEY", "")).strip()
        self.api_secret = (api_secret or _cfg("GROWW_API_SECRET", "") or os.getenv("GROWW_API_SECRET", "")).strip()
        self.totp_secret = (totp_secret or _cfg("GROWW_TOTP_SECRET", "") or os.getenv("GROWW_TOTP_SECRET", "")).strip()
        self._client = None
        self._instrument_rows: list[dict[str, Any]] = []
        self._instrument_loaded_ts = 0.0

    @property
    def client(self):
        if self._client is not None:
            return self._client
        try:
            from growwapi import GrowwAPI
        except Exception as exc:  # pragma: no cover - dependency is optional in tests
            raise RuntimeError("Groww official SDK is not installed. Install growwapi before live trading.") from exc
        token = self.access_token or self._mint_access_token(GrowwAPI)
        if not token:
            raise RuntimeError("Groww access token missing. Set GROWW_ACCESS_TOKEN or API key credentials.")
        self.access_token = token
        self._client = GrowwAPI(token)
        return self._client

    def _mint_access_token(self, GrowwAPI) -> str:
        if not self.api_key:
            return ""
        attempts: list[dict[str, Any]] = []
        if self.totp_secret:
            try:
                import pyotp

                attempts.extend([
                    {"api_key": self.api_key, "totp": pyotp.TOTP(self.totp_secret).now()},
                ])
            except Exception:
                pass
        if self.api_secret:
            attempts.extend([
                {"api_key": self.api_key, "secret": self.api_secret},
            ])
        getter = getattr(GrowwAPI, "get_access_token", None)
        if not callable(getter):
            return ""
        last_exc: Exception | None = None
        for kwargs in attempts:
            try:
                token = getter(**kwargs)
                if isinstance(token, Mapping):
                    token = token.get("access_token") or token.get("token") or token.get("auth_token")
                if str(token or "").strip():
                    return str(token).strip()
            except Exception as exc:
                last_exc = exc
        if last_exc is not None:
            raise RuntimeError(f"Groww access-token generation failed: {last_exc}") from last_exc
        return ""

    def preflight_session(self, *, force_refresh: bool = False) -> dict[str, Any]:
        _ = force_refresh
        client = self.client
        return {"broker": "groww", "ready": True, "sdk": type(client).__name__}

    def validate_static_ip(self) -> dict[str, Any]:
        """Validate outbound IP for live Groww order routing.

        Groww live order placement must originate from an approved static IP.
        The approved IP list is operator supplied via GROWW_APPROVED_STATIC_IPS.
        GROWW_OUTBOUND_IP_OVERRIDE is supported for controlled deployment tests.
        """
        required = bool(_cfg("GROWW_REQUIRE_STATIC_IP_FOR_LIVE_ORDERS", True))
        approved_ips = tuple(_cfg("GROWW_APPROVED_STATIC_IPS", ())) or tuple(
            x.strip() for x in os.getenv("GROWW_APPROVED_STATIC_IPS", "").split(",") if x.strip()
        )
        if not required:
            return {"required": False, "approved": True, "reason": "not_required"}
        if not approved_ips:
            return {
                "required": True,
                "approved": False,
                "configured_ip": "",
                "observed_ip": "",
                "reason": "GROWW_APPROVED_STATIC_IPS missing",
            }
        observed = str(_cfg("GROWW_OUTBOUND_IP_OVERRIDE", "") or os.getenv("GROWW_OUTBOUND_IP_OVERRIDE", "")).strip()
        if not observed:
            try:
                import requests

                url = str(_cfg("GROWW_OUTBOUND_IP_CHECK_URL", "https://api.ipify.org?format=json"))
                resp = requests.get(url, timeout=3.0)
                resp.raise_for_status()
                payload = resp.json() if "json" in str(resp.headers.get("content-type", "")).lower() else {}
                observed = str(payload.get("ip") or resp.text or "").strip()
            except Exception as exc:
                return {
                    "required": True,
                    "approved": False,
                    "configured_ip": ",".join(approved_ips),
                    "observed_ip": "",
                    "reason": f"outbound_ip_check_failed: {exc}",
                }
        ok = observed in set(approved_ips)
        return {
            "required": True,
            "approved": ok,
            "configured_ip": ",".join(approved_ips),
            "observed_ip": observed,
            "reason": "approved_static_ip" if ok else "observed_ip_not_approved",
        }

    def is_auth_ready(self) -> bool:
        try:
            self.preflight_session()
            return True
        except Exception:
            return False

    def const(self, name: str, default: str) -> str:
        try:
            return str(getattr(self.client, name, default) or default)
        except Exception:
            return str(default)

    @staticmethod
    def _compact_body(body: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
        return {str(k): v for k, v in (body or {}).items() if v not in (None, "")}

    @staticmethod
    def _normalise_right(value: Any) -> str:
        right = str(value or "").strip().lower()
        if right in {"c", "ce", "call"}:
            return "Call"
        if right in {"p", "pe", "put"}:
            return "Put"
        return str(value or "").strip()

    @staticmethod
    def _normalise_right_code(value: Any) -> str:
        right = GrowwRestClient._normalise_right(value).lower()
        if right == "call":
            return "CE"
        if right == "put":
            return "PE"
        raw = str(value or "").strip().upper()
        return raw if raw in {"CE", "PE"} else ""

    @staticmethod
    def _normalise_expiry(value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%d-%b-%Y", "%d/%m/%Y"):
            try:
                return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
            except Exception:
                pass
        return text[:10]

    @staticmethod
    def _interval_minutes(interval: Any) -> int:
        raw = str(interval or "").strip().lower()
        mapping = {
            "minute": 1,
            "1minute": 1,
            "1m": 1,
            "5minute": 5,
            "5m": 5,
            "10minute": 10,
            "10m": 10,
            "15minute": 15,
            "15m": 15,
            "30minute": 30,
            "30m": 30,
            "1h": 60,
            "60minute": 60,
            "4h": 240,
            "day": 1440,
            "1day": 1440,
            "1d": 1440,
            "week": 10080,
            "1week": 10080,
        }
        return mapping.get(raw, 1)

    @staticmethod
    def _to_groww_time(value: Any) -> Any:
        text = str(value or "").strip()
        if not text:
            return value
        if text.isdigit():
            return int(text)
        text = text.replace("T", " ").replace(".000Z", "").replace("Z", "")
        if len(text) >= 19:
            return text[:19]
        return text

    @staticmethod
    def _response_list(resp: Any, *keys: str) -> list[dict[str, Any]]:
        if isinstance(resp, list):
            return [dict(x) for x in resp if isinstance(x, Mapping)]
        if isinstance(resp, Mapping):
            for key in keys:
                val = resp.get(key)
                if isinstance(val, list):
                    return [dict(x) for x in val if isinstance(x, Mapping)]
            data = resp.get("data") or resp.get("result")
            if isinstance(data, list):
                return [dict(x) for x in data if isinstance(x, Mapping)]
            if isinstance(data, Mapping):
                for key in keys:
                    val = data.get(key)
                    if isinstance(val, list):
                        return [dict(x) for x in val if isinstance(x, Mapping)]
        return []

    def get_all_instruments(self, *, force_refresh: bool = False) -> list[dict[str, Any]]:
        ttl = float(_cfg("GROWW_INSTRUMENT_CACHE_TTL_SEC", 1800.0))
        if self._instrument_rows and not force_refresh and time.time() - self._instrument_loaded_ts < ttl:
            return list(self._instrument_rows)
        rows: list[dict[str, Any]] = []
        getter = getattr(self.client, "get_all_instruments", None)
        if callable(getter):
            data = getter()
            if hasattr(data, "to_dict"):
                rows = [dict(x) for x in data.to_dict(orient="records")]
            elif isinstance(data, list):
                rows = [dict(x) for x in data if isinstance(x, Mapping)]
            elif isinstance(data, Mapping):
                rows = self._response_list(data, "instruments", "instrument_list", "data")
        if not rows:
            rows = self._download_instruments_csv()
        self._instrument_rows = rows
        self._instrument_loaded_ts = time.time()
        return list(rows)

    def _download_instruments_csv(self) -> list[dict[str, Any]]:
        try:
            import requests

            url = str(_cfg("GROWW_INSTRUMENTS_CSV_URL", self.INSTRUMENTS_CSV_URL))
            resp = requests.get(url, timeout=20.0)
            resp.raise_for_status()
            text = resp.text
            return [dict(row) for row in csv.DictReader(io.StringIO(text))]
        except Exception:
            return []

    def _option_symbol_from_route(self, route: Mapping[str, Any]) -> str:
        for key in ("trading_symbol", "TradingSymbol", "symbol", "groww_symbol"):
            value = str(route.get(key) or "").strip()
            if value:
                return value
        stock = str(route.get("stock_code") or route.get("underlying") or route.get("underlying_symbol") or "").strip().upper()
        expiry = self._normalise_expiry(route.get("expiry_date") or route.get("expiry"))
        right_code = self._normalise_right_code(route.get("right") or route.get("option_type") or route.get("instrument_type"))
        strike = _num(route.get("strike_price") or route.get("strike"), 0.0)
        if not (stock and expiry and right_code and strike > 0):
            return ""
        for row in self.get_all_instruments():
            if str(row.get("segment") or "").upper() != "FNO":
                continue
            if str(row.get("underlying_symbol") or row.get("stock_code") or "").upper() != stock:
                continue
            if self._normalise_expiry(row.get("expiry_date")) != expiry:
                continue
            if str(row.get("instrument_type") or "").upper() != right_code:
                continue
            if abs(_num(row.get("strike_price"), 0.0) - strike) <= 1e-6:
                return str(row.get("trading_symbol") or "").strip()
        return ""

    def _groww_option_master_row(self, row: Mapping[str, Any]) -> dict[str, Any]:
        right = str(row.get("instrument_type") or row.get("option_type") or "").upper()
        underlying = str(row.get("underlying_symbol") or row.get("stock_code") or "").strip().upper()
        expiry = self._normalise_expiry(row.get("expiry_date") or row.get("expiry"))
        strike = _num(row.get("strike_price") or row.get("strike"), 0.0)
        trading_symbol = str(row.get("trading_symbol") or row.get("symbol") or "").strip()
        lot_size = _num(row.get("lot_size") or row.get("LotSize"), 0.0)
        tick = _num(row.get("tick_size"), 0.05) or 0.05
        out = dict(row)
        out.update({
            "stock_code": underlying,
            "underlying": underlying,
            "exchange_code": "NFO",
            "exchange": str(row.get("exchange") or "NSE").upper(),
            "segment": "FNO",
            "product_type": "Options",
            "right": "Call" if right == "CE" else "Put" if right == "PE" else right,
            "option_type": right,
            "expiry_date": expiry,
            "strike_price": strike,
            "TradingSymbol": trading_symbol,
            "trading_symbol": trading_symbol,
            "runtime_lot_size": lot_size,
            "LotSize": lot_size,
            "tick_size": tick,
            "instrument_definition_source": "groww_instruments_csv",
        })
        return out

    def get_security_master_rows(
        self,
        *,
        url: str | None = None,
        cache_path: str | None = None,
        timeout: float = 12.0,
        require_current_trade_date: bool = False,
    ) -> list[dict[str, Any]]:
        _ = (url, cache_path, timeout, require_current_trade_date)
        out: list[dict[str, Any]] = []
        for row in self.get_all_instruments():
            if str(row.get("segment") or "").upper() != "FNO":
                continue
            if str(row.get("instrument_type") or "").upper() not in {"CE", "PE"}:
                continue
            if _num(row.get("lot_size"), 0.0) <= 0:
                continue
            out.append(self._groww_option_master_row(row))
        return out

    def get_option_chain_quotes(self, **kwargs) -> dict[str, Any]:
        if not bool(_cfg("GROWW_USE_OPTION_CHAIN_FOR_SELECTION", False)):
            return {"Success": [], "Status": 200, "Error": None, "_empty_chain": True}
        underlying = str(kwargs.get("underlying") or kwargs.get("stock_code") or "").strip().upper()
        expiry = self._normalise_expiry(kwargs.get("expiry_date") or kwargs.get("expiry"))
        if not underlying or not expiry:
            return {"Success": [], "Status": 200, "Error": "missing underlying/expiry"}
        resp = self.client.get_option_chain(
            exchange=self.const("EXCHANGE_NSE", "NSE"),
            underlying=underlying,
            expiry_date=expiry,
        )
        return {"Success": self._normalise_option_chain(resp, underlying, expiry), "Status": 200, "Error": None, "_raw": resp}

    def _normalise_option_chain(self, resp: Any, underlying: str, expiry: str) -> list[dict[str, Any]]:
        strikes = resp.get("strikes") if isinstance(resp, Mapping) else {}
        rows: list[dict[str, Any]] = []
        if not isinstance(strikes, Mapping):
            return rows
        for strike_raw, sides in strikes.items():
            if not isinstance(sides, Mapping):
                continue
            for side, payload in sides.items():
                if str(side).upper() not in {"CE", "PE"} or not isinstance(payload, Mapping):
                    continue
                row = dict(payload)
                row.update({
                    "stock_code": underlying,
                    "exchange_code": "NFO",
                    "exchange": "NSE",
                    "segment": "FNO",
                    "product_type": "Options",
                    "expiry_date": expiry,
                    "right": "Call" if str(side).upper() == "CE" else "Put",
                    "option_type": str(side).upper(),
                    "strike_price": _num(strike_raw, 0.0),
                    "TradingSymbol": row.get("trading_symbol"),
                    "underlying_ltp": (resp.get("underlying_ltp") if isinstance(resp, Mapping) else None),
                    "quote_source": "groww_option_chain",
                })
                rows.append(row)
        return rows

    def _quote_route(self, instrument_or_route: Any) -> dict[str, Any]:
        if isinstance(instrument_or_route, Mapping):
            raw = dict(instrument_or_route)
        else:
            raw = getattr(instrument_or_route, "raw", {}) or {}
        selected = raw.get("selected_option_contract") if isinstance(raw, Mapping) else None
        if isinstance(selected, Mapping) and isinstance(selected.get("raw"), Mapping):
            merged = dict(raw)
            merged.update(selected.get("raw") or {})
            raw = merged
        return dict(raw)

    def get_quote_for_instrument(self, instrument_or_route: Any) -> Dict[str, Any]:
        route = self._quote_route(instrument_or_route)
        exchange = str(route.get("exchange") or route.get("underlying_exchange_code") or "NSE").upper()
        segment = str(route.get("segment") or ("FNO" if route.get("exchange_code") == "NFO" else "CASH")).upper()
        trading_symbol = self._option_symbol_from_route(route) if segment == "FNO" else str(route.get("trading_symbol") or route.get("stock_code") or route.get("symbol") or "").strip().upper()
        if not trading_symbol:
            raise RuntimeError("Groww quote route missing trading_symbol")
        resp = self.client.get_quote(exchange=exchange, segment=segment, trading_symbol=trading_symbol)
        row = dict(resp or {}) if isinstance(resp, Mapping) else {}
        row.setdefault("trading_symbol", trading_symbol)
        row.setdefault("TradingSymbol", trading_symbol)
        row.setdefault("exchange", exchange)
        row.setdefault("segment", segment)
        if segment == "FNO":
            row.update({
                "stock_code": str(route.get("stock_code") or route.get("underlying") or "").upper(),
                "exchange_code": "NFO",
                "product_type": "Options",
                "expiry_date": self._normalise_expiry(route.get("expiry_date") or route.get("expiry")),
                "right": self._normalise_right(route.get("right") or route.get("option_type")),
                "strike_price": _num(route.get("strike_price") or route.get("strike"), 0.0),
            })
        row["ltp"] = _num(row.get("last_price") or row.get("ltp") or row.get("close"), 0.0)
        row["best_bid_price"] = _num(row.get("bid_price") or row.get("best_bid_price"), 0.0)
        row["best_offer_price"] = _num(row.get("offer_price") or row.get("best_offer_price"), 0.0)
        row["best_bid_quantity"] = _num(row.get("bid_quantity") or row.get("best_bid_quantity"), 0.0)
        row["best_offer_quantity"] = _num(row.get("offer_quantity") or row.get("best_offer_quantity"), 0.0)
        return {"Success": [row], "Status": 200, "Error": None, "_raw": resp}

    def get_historical_charts(self, **kwargs) -> Dict[str, Any]:
        interval = self._interval_minutes(kwargs.get("interval"))
        exchange = str(kwargs.get("exchange") or kwargs.get("exchange_code") or kwargs.get("exch_code") or "NSE").upper()
        if exchange == "NFO":
            exchange = "NSE"
        segment = str(kwargs.get("segment") or ("FNO" if str(kwargs.get("exchange_code") or "").upper() == "NFO" else "CASH")).upper()
        trading_symbol = str(kwargs.get("trading_symbol") or "").strip()
        if not trading_symbol:
            trading_symbol = self._option_symbol_from_route(kwargs) if segment == "FNO" else str(kwargs.get("stock_code") or "").strip().upper()
        if not trading_symbol:
            raise RuntimeError("Groww historical route missing trading_symbol")
        resp = self.client.get_historical_candle_data(
            trading_symbol=trading_symbol,
            exchange=exchange,
            segment=segment,
            start_time=self._to_groww_time(kwargs.get("start_time") or kwargs.get("from_date")),
            end_time=self._to_groww_time(kwargs.get("end_time") or kwargs.get("to_date")),
            interval_in_minutes=interval,
        )
        rows = []
        candles = resp.get("candles") if isinstance(resp, Mapping) else []
        for item in candles if isinstance(candles, list) else []:
            if isinstance(item, (list, tuple)) and len(item) >= 5:
                ts, o, h, l, c = item[:5]
                v = item[5] if len(item) > 5 else 0
                rows.append({
                    "datetime": datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    "open": _num(o),
                    "high": _num(h),
                    "low": _num(l),
                    "close": _num(c),
                    "volume": _num(v),
                })
            elif isinstance(item, Mapping):
                rows.append(dict(item))
        return {"Success": rows, "Status": 200, "Error": None, "_raw": resp}

    def get_historical_charts_v2(self, **kwargs) -> Dict[str, Any]:
        return self.get_historical_charts(**kwargs)

    def get_funds(self) -> Dict[str, Any]:
        return self.get_margin()

    def get_margin(self, exchange_code: str = "NFO") -> Dict[str, Any]:
        _ = exchange_code
        resp = self.client.get_available_margin_details()
        data = dict(resp or {}) if isinstance(resp, Mapping) else {}
        fno = data.get("fno_margin_details") if isinstance(data.get("fno_margin_details"), Mapping) else {}
        available = _num(fno.get("option_buy_balance_available") or fno.get("future_balance_available") or data.get("clear_cash"), 0.0)
        used = _num(fno.get("net_fno_margin_used") or data.get("net_margin_used"), 0.0)
        return {
            "Success": {
                "allocated_fno": available + max(0.0, used),
                "block_by_trade_fno": max(0.0, used),
                "unallocated_balance": _num(data.get("clear_cash"), 0.0),
                "total_bank_balance": _num(data.get("clear_cash"), 0.0),
                "cash_limit": available + max(0.0, used),
                "amount_allocated": available + max(0.0, used),
                "block_by_trade": max(0.0, used),
                "clear_cash": data.get("clear_cash"),
                "fno_margin_details": fno,
            },
            "Status": 200,
            "Error": None,
            "_raw": resp,
        }

    def place_order(self, **kwargs) -> Dict[str, Any]:
        body = self._compact_body(kwargs)
        body.setdefault("validity", self.const("VALIDITY_DAY", "DAY"))
        body.setdefault("exchange", self.const("EXCHANGE_NSE", "NSE"))
        body.setdefault("segment", self.const("SEGMENT_FNO", "FNO"))
        body.setdefault("product", self.const("PRODUCT_NRML", "NRML"))
        resp = self.client.place_order(**body)
        return dict(resp or {}) if isinstance(resp, Mapping) else {"groww_order_id": str(resp)}

    def get_order_status(self, groww_order_id: str, segment: str = "FNO", **kwargs) -> Dict[str, Any]:
        return self.client.get_order_status(groww_order_id=groww_order_id, segment=segment, **kwargs)

    def get_order_status_by_reference(self, order_reference_id: str, segment: str = "FNO", **kwargs) -> Dict[str, Any]:
        return self.client.get_order_status_by_reference(order_reference_id=order_reference_id, segment=segment, **kwargs)

    def get_order_detail(self, groww_order_id: str | None = None, order_id: str | None = None, segment: str = "FNO", **kwargs) -> Dict[str, Any]:
        oid = groww_order_id or order_id
        return self.client.get_order_detail(groww_order_id=oid, segment=segment, **kwargs)

    def get_order(self, order_id: str, **kwargs) -> Dict[str, Any]:
        return self.get_order_detail(groww_order_id=order_id, segment=str(kwargs.get("segment") or "FNO"))

    def get_order_list(self, segment: str | None = "FNO", page: int = 0, page_size: int = 25, **kwargs) -> Dict[str, Any]:
        body = {"page": page, "page_size": page_size, **kwargs}
        if segment:
            body["segment"] = segment
        return self.client.get_order_list(**body)

    def cancel_order(self, groww_order_id: str | None = None, order_id: str | None = None, segment: str = "FNO", **kwargs) -> Dict[str, Any]:
        oid = groww_order_id or order_id
        return self.client.cancel_order(groww_order_id=oid, segment=segment, **kwargs)

    def modify_order(self, groww_order_id: str | None = None, order_id: str | None = None, segment: str = "FNO", **kwargs) -> Dict[str, Any]:
        oid = groww_order_id or order_id
        return self.client.modify_order(groww_order_id=oid, segment=segment, **kwargs)

    def get_trade_detail(self, order_id: str | None = None, groww_order_id: str | None = None, segment: str = "FNO", **kwargs) -> Dict[str, Any]:
        oid = groww_order_id or order_id
        getter = getattr(self.client, "get_trade_list_for_order")
        return getter(groww_order_id=oid, segment=segment, **kwargs)

    def get_portfolio_positions(self) -> Dict[str, Any]:
        resp = self.client.get_positions_for_user(segment=self.const("SEGMENT_FNO", "FNO"))
        rows = self._response_list(resp, "positions")
        normalised = []
        for row in rows:
            item = dict(row)
            item.setdefault("exchange_code", "NFO")
            item.setdefault("product_type", "Options")
            normalised.append(item)
        return {"Success": normalised, "Status": 200, "Error": None, "_raw": resp}

    def get_positions_for_user(self, **kwargs) -> Dict[str, Any]:
        return self.client.get_positions_for_user(**kwargs)

    @staticmethod
    def reference_id(prefix: str = "groww") -> str:
        raw = f"{prefix}-{int(time.time() * 1000) % 10_000_000_000}"
        raw = re.sub(r"[^A-Za-z0-9-]", "", raw)[:20]
        if len(raw) < 8:
            raw = (raw + "00000000")[:8]
        parts = raw.split("-")
        if len(parts) > 3:
            raw = "-".join(parts[:3])
        return raw

    def create_smart_order(self, **kwargs) -> Dict[str, Any]:
        body = self._compact_body(kwargs)
        return self.client.create_smart_order(**body)

    def get_smart_order(self, smart_order_id: str, **kwargs) -> Dict[str, Any]:
        getter = getattr(self.client, "get_smart_order", None) or getattr(self.client, "get_smart_order_detail", None)
        if not callable(getter):
            return {"smart_order_id": smart_order_id, "status": "ACTIVE"}
        return getter(smart_order_id=smart_order_id, **kwargs)

    def get_smart_order_list(self, **kwargs) -> Dict[str, Any]:
        getter = getattr(self.client, "get_smart_order_list", None) or getattr(self.client, "get_smart_orders", None)
        if not callable(getter):
            return {"smart_order_list": []}
        return getter(**kwargs)

    def cancel_smart_order(self, smart_order_id: str, **kwargs) -> Dict[str, Any]:
        canceller = getattr(self.client, "cancel_smart_order", None)
        if not callable(canceller):
            raise RuntimeError("Groww SDK cancel_smart_order method unavailable")
        return canceller(smart_order_id=smart_order_id, **kwargs)

    def resolve_exchange_token(self, *, exchange: str, segment: str, trading_symbol: str) -> str:
        symbol = str(trading_symbol or "").strip()
        for row in self.get_all_instruments():
            if str(row.get("exchange") or "").upper() != str(exchange or "").upper():
                continue
            if str(row.get("segment") or "").upper() != str(segment or "").upper():
                continue
            if str(row.get("trading_symbol") or "").upper() == symbol.upper():
                return str(row.get("exchange_token") or "").strip()
        if segment.upper() == "CASH" and symbol.upper() in {"NIFTY", "BANKNIFTY", "FINNIFTY"}:
            return symbol.upper()
        return ""

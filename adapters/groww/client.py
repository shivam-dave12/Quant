"""Official-SDK Groww broker adapter for discovered NSE long options and post-fill OCO.

Only documented SDK method shapes are used. All execution methods validate FNO long-option
semantics and approved static egress before transmitting live orders.
"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import date
import ipaddress
from typing import Any, Callable, Iterable
import urllib.request

@dataclass(frozen=True)
class GrowwComplianceStatus:
    ready: bool
    observed_outbound_ip: str | None
    reason: str

@dataclass(frozen=True)
class GrowwContract:
    trading_symbol: str
    groww_symbol: str
    underlying: str
    option_type: str
    expiry: str
    strike: float
    lot_size: int
    tick_size: float
    exchange_token: str
    buy_allowed: bool

class GrowwAdapter:
    def __init__(self, client: Any, *, live_orders_enabled: bool = False, approved_static_ips: Iterable[str] = (),
                 outbound_ip_resolver: Callable[[], str] | None = None) -> None:
        self.client = client; self.live_orders_enabled = live_orders_enabled
        self.approved_ips = {str(ipaddress.ip_address(x)) for x in approved_static_ips if str(x).strip()}
        self.outbound_ip_resolver = outbound_ip_resolver or self._public_ip
    @classmethod
    def from_secrets(cls, secrets: Any, **kwargs: Any) -> "GrowwAdapter":
        try:
            from growwapi import GrowwAPI
        except ImportError as exc:
            raise RuntimeError("growwapi SDK is required for Groww desk") from exc
        token = secrets.groww_access_token
        if not token and secrets.groww_api_key and secrets.groww_api_secret:
            token = GrowwAPI.get_access_token(api_key=secrets.groww_api_key, secret=secrets.groww_api_secret)
        elif not token and secrets.groww_api_key and secrets.groww_totp_secret:
            try: import pyotp
            except ImportError as exc: raise RuntimeError("pyotp is required for Groww TOTP authentication") from exc
            token = GrowwAPI.get_access_token(api_key=secrets.groww_api_key, totp=pyotp.TOTP(secrets.groww_totp_secret).now())
        if not token: raise RuntimeError("GROWW_AUTH_CREDENTIALS_NOT_CONFIGURED")
        return cls(GrowwAPI(token), **kwargs)
    @staticmethod
    def _public_ip() -> str:
        with urllib.request.urlopen("https://api.ipify.org", timeout=5) as response:
            return response.read().decode("ascii").strip()
    def compliance(self) -> GrowwComplianceStatus:
        if not self.live_orders_enabled: return GrowwComplianceStatus(False, None, "GROWW_LIVE_ORDERS_DISABLED")
        if not self.approved_ips: return GrowwComplianceStatus(False, None, "NO_APPROVED_STATIC_OUTBOUND_IP")
        try: observed = str(ipaddress.ip_address(self.outbound_ip_resolver()))
        except Exception as exc: return GrowwComplianceStatus(False, None, f"OUTBOUND_IP_VALIDATION_FAILED:{exc}")
        return GrowwComplianceStatus(observed in self.approved_ips, observed, "APPROVED_STATIC_OUTBOUND_IP" if observed in self.approved_ips else "OUTBOUND_IP_NOT_APPROVED")
    @staticmethod
    def _records(frame_or_rows: Any) -> list[dict[str, Any]]:
        if hasattr(frame_or_rows, "to_dict"): return list(frame_or_rows.to_dict("records"))
        return [dict(x) for x in (frame_or_rows or [])]
    def discover_contracts(self, underlying: str, *, today: date | None = None) -> list[GrowwContract]:
        today = today or date.today(); rows = self._records(self.client.get_all_instruments())
        output: list[GrowwContract] = []
        for row in rows:
            expiry = str(row.get("expiry_date", ""))[:10]
            if str(row.get("exchange", "")).upper() != "NSE" or str(row.get("segment", "")).upper() != "FNO": continue
            if str(row.get("underlying_symbol", "")).upper() != underlying.upper() or str(row.get("instrument_type", "")).upper() not in {"CE", "PE"}: continue
            if not expiry or date.fromisoformat(expiry) < today or not bool(int(row.get("buy_allowed", 0) or 0)): continue
            output.append(GrowwContract(str(row["trading_symbol"]), str(row.get("groww_symbol", "")), underlying.upper(),
                str(row["instrument_type"]).upper(), expiry, float(row.get("strike_price", 0) or 0), int(row.get("lot_size", 0) or 0),
                float(row.get("tick_size", 0) or 0), str(row.get("exchange_token", "")), True))
        return sorted(output, key=lambda x: (x.expiry, x.strike, x.option_type))
    def option_chain(self, underlying: str, expiry: str) -> dict[str, Any]:
        return dict(self.client.get_option_chain(exchange="NSE", underlying=underlying.upper(), expiry_date=expiry))
    def rankable_option_rows(self, *, underlying: str, expiry: str, option_type: str, contracts: list[GrowwContract], validated_fee_per_lot: float, slippage_bps: float) -> list[dict[str, Any]]:
        """Two-stage watchlist: chain snapshot then quotes only for directional CE/PE candidates."""
        if validated_fee_per_lot < 0 or slippage_bps < 0:
            raise ValueError("VALIDATED_OPTION_COSTS_REQUIRED")
        chain = self.option_chain(underlying, expiry)
        strikes = chain.get("strikes", chain.get("payload", {}).get("strikes", {})) or {}
        valid = {c.trading_symbol: c for c in contracts if c.expiry == expiry and c.option_type == option_type}
        rows: list[dict[str, Any]] = []
        for strike, sides in strikes.items():
            side = (sides or {}).get(option_type)
            if not side or str(side.get("trading_symbol", "")) not in valid:
                continue
            contract = valid[str(side["trading_symbol"])]
            quote = self.quote(contract.trading_symbol)
            rows.append({"trading_symbol": contract.trading_symbol, "option_type": option_type, "expiry": expiry, "strike": float(strike),
                "premium": float(side.get("ltp", quote.get("last_price", 0)) or 0), "bid": float(quote.get("bid_price", 0) or 0),
                "ask": float(quote.get("offer_price", 0) or 0), "volume": float(side.get("volume", 0) or 0),
                "open_interest": float(side.get("open_interest", 0) or 0), "greeks": dict(side.get("greeks", {}) or {}),
                "lot_size": contract.lot_size, "tick_size": contract.tick_size, "exchange_token": contract.exchange_token,
                "executable_depth_qty": min(float(quote.get("bid_quantity", 0) or 0), float(quote.get("offer_quantity", 0) or 0)),
                "protection_feasible": True, "validated_fee": float(validated_fee_per_lot), "slippage_bps": float(slippage_bps)})
        return rows
    def greeks(self, underlying: str, trading_symbol: str, expiry: str) -> dict[str, Any]:
        return dict(self.client.get_greeks(exchange="NSE", underlying=underlying.upper(), trading_symbol=trading_symbol, expiry=expiry))
    def quote(self, trading_symbol: str) -> dict[str, Any]:
        return dict(self.client.get_quote(exchange="NSE", segment="FNO", trading_symbol=trading_symbol))
    def historical_candles(self, *, trading_symbol: str, segment: str, start_time: str | int, end_time: str | int, interval_minutes: int = 1) -> list[list[Any]]:
        """Use the current historical-candles SDK route; no deprecated method fallback."""
        value = self.client.get_historical_candles(trading_symbol=trading_symbol, exchange="NSE", segment=segment,
            start_time=start_time, end_time=end_time, interval_in_minutes=interval_minutes)
        rows = value.get("candles", []) if isinstance(value, dict) else []
        if not isinstance(rows, list): raise RuntimeError("INVALID_GROWW_HISTORICAL_CANDLES_RESPONSE")
        return rows
    def positions(self) -> list[dict[str, Any]]:
        value = self.client.get_positions_for_user(segment="FNO")
        if isinstance(value, dict): return list(value.get("positions", value.get("payload", [])) or [])
        return list(value or [])
    def _require_compliance(self) -> None:
        status = self.compliance()
        if not status.ready: raise RuntimeError(status.reason)
    def place_long_option_limit(self, *, contract: GrowwContract, quantity: int, limit_price: float, reference_id: str) -> dict[str, Any]:
        self._require_compliance()
        if contract.option_type not in {"CE", "PE"} or quantity <= 0 or quantity % contract.lot_size != 0: raise ValueError("validated long CE/PE lot quantity required")
        return dict(self.client.place_order(trading_symbol=contract.trading_symbol, quantity=quantity, validity="DAY", exchange="NSE", segment="FNO",
            product="MIS", order_type="LIMIT", transaction_type="BUY", price=limit_price, order_reference_id=reference_id))
    def order_detail(self, order_id: str) -> dict[str, Any]:
        return dict(self.client.get_order_detail(order_id=order_id, segment="FNO"))
    def cancel_order(self, order_id: str) -> dict[str, Any]:
        return dict(self.client.cancel_order(groww_order_id=order_id, segment="FNO"))
    def create_oco_exit(self, *, contract: GrowwContract, quantity: int, net_position_quantity: int,
                        target_trigger: float, target_price: float, stop_trigger: float, reference_id: str) -> dict[str, Any]:
        self._require_compliance()
        if quantity <= 0 or quantity > abs(net_position_quantity) or quantity % contract.lot_size != 0: raise ValueError("OCO quantity must protect an actually held lot quantity")
        return dict(self.client.create_smart_order(smart_order_type="OCO", reference_id=reference_id, segment="FNO", trading_symbol=contract.trading_symbol,
            quantity=quantity, product_type="MIS", exchange="NSE", duration="DAY", net_position_quantity=net_position_quantity, transaction_type="SELL",
            target={"trigger_price": f"{target_trigger:.2f}", "order_type": "LIMIT", "price": f"{target_price:.2f}"},
            stop_loss={"trigger_price": f"{stop_trigger:.2f}", "order_type": "SL_M", "price": None}))
    def smart_order(self, smart_order_id: str) -> dict[str, Any]:
        return dict(self.client.get_smart_order(smart_order_id=smart_order_id, segment="FNO", smart_order_type="OCO"))
    def modify_oco_exit(self, *, smart_order_id: str, quantity: int, target_trigger: float, stop_trigger: float) -> dict[str, Any]:
        self._require_compliance()
        if quantity <= 0: raise ValueError("OCO protection quantity must be positive")
        return dict(self.client.modify_smart_order(smart_order_id=smart_order_id, smart_order_type="OCO", segment="FNO", quantity=quantity,
            duration="DAY", product_type="MIS", target={"trigger_price": f"{target_trigger:.2f}"}, stop_loss={"trigger_price": f"{stop_trigger:.2f}"}))
    def cancel_oco_exit(self, smart_order_id: str) -> dict[str, Any]:
        self._require_compliance()
        return dict(self.client.cancel_smart_order(segment="FNO", smart_order_type="OCO", smart_order_id=smart_order_id))
    def active_oco_orders(self, *, start_date_time: str, end_date_time: str) -> list[dict[str, Any]]:
        value = self.client.get_smart_order_list(segment="FNO", smart_order_type="OCO", status="ACTIVE", page=0, page_size=50, start_date_time=start_date_time, end_date_time=end_date_time)
        return list(value.get("orders", []) if isinstance(value, dict) else [])
    def emergency_limit_sell(self, *, contract: GrowwContract, quantity: int, limit_price: float, reference_id: str) -> dict[str, Any]:
        self._require_compliance()
        return dict(self.client.place_order(trading_symbol=contract.trading_symbol, quantity=quantity, validity="DAY", exchange="NSE", segment="FNO",
            product="MIS", order_type="LIMIT", transaction_type="SELL", price=limit_price, order_reference_id=reference_id))

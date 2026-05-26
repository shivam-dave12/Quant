"""Groww NSE/FNO broker adapter with documented SDK capability mapping and strict protection rules.

The adapter exposes all Groww capabilities used by the institutional India-options desk:
authentication, instrument discovery, market/read APIs, account/margin APIs, order lifecycle,
smart OCO protection, historical-data access, and controlled emergency exits.

Execution methods are fail-closed: they require approved live-policy/static-egress validation,
long CE/PE lot validation, and do not disguise an entry as protected before OCO confirmation.
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
    """Official-SDK shaped adapter for the buy-options-only Groww desk."""

    INTERVAL_MAP = {
        1: "1minute",
        2: "2minute",
        3: "3minute",
        5: "5minute",
        10: "10minute",
        15: "15minute",
        30: "30minute",
        60: "1hour",
        240: "4hour",
    }

    def __init__(
        self,
        client: Any,
        *,
        live_orders_enabled: bool = False,
        approved_static_ips: Iterable[str] = (),
        outbound_ip_resolver: Callable[[], str] | None = None,
    ) -> None:
        self.client = client
        self.live_orders_enabled = live_orders_enabled
        self.approved_ips = {str(ipaddress.ip_address(value)) for value in approved_static_ips if str(value).strip()}
        self.outbound_ip_resolver = outbound_ip_resolver or self._public_ip

    # ------------------------------------------------------------------ auth
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
            try:
                import pyotp
            except ImportError as exc:
                raise RuntimeError("pyotp is required for Groww TOTP authentication") from exc
            token = GrowwAPI.get_access_token(
                api_key=secrets.groww_api_key,
                totp=pyotp.TOTP(secrets.groww_totp_secret).now(),
            )
        if not token:
            raise RuntimeError("GROWW_AUTH_CREDENTIALS_NOT_CONFIGURED")
        return cls(GrowwAPI(token), **kwargs)

    @staticmethod
    def _public_ip() -> str:
        with urllib.request.urlopen("https://api.ipify.org", timeout=5) as response:
            return response.read().decode("ascii").strip()

    def compliance(self) -> GrowwComplianceStatus:
        if not self.live_orders_enabled:
            return GrowwComplianceStatus(False, None, "GROWW_LIVE_ORDERS_DISABLED")
        if not self.approved_ips:
            return GrowwComplianceStatus(False, None, "NO_APPROVED_STATIC_OUTBOUND_IP")
        try:
            observed = str(ipaddress.ip_address(self.outbound_ip_resolver()))
        except Exception as exc:
            return GrowwComplianceStatus(False, None, f"OUTBOUND_IP_VALIDATION_FAILED:{exc}")
        ready = observed in self.approved_ips
        return GrowwComplianceStatus(
            ready,
            observed,
            "APPROVED_STATIC_OUTBOUND_IP" if ready else "OUTBOUND_IP_NOT_APPROVED",
        )

    def _require_compliance(self) -> None:
        status = self.compliance()
        if not status.ready:
            raise RuntimeError(status.reason)

    # -------------------------------------------------------- instruments/data
    @staticmethod
    def _records(frame_or_rows: Any) -> list[dict[str, Any]]:
        if hasattr(frame_or_rows, "to_dict"):
            return list(frame_or_rows.to_dict("records"))
        return [dict(item) for item in (frame_or_rows or [])]

    def instruments(self) -> list[dict[str, Any]]:
        return self._records(self.client.get_all_instruments())

    def discover_contracts(self, underlying: str, *, today: date | None = None) -> list[GrowwContract]:
        today = today or date.today()
        output: list[GrowwContract] = []
        for row in self.instruments():
            expiry = str(row.get("expiry_date", ""))[:10]
            if str(row.get("exchange", "")).upper() != "NSE" or str(row.get("segment", "")).upper() != "FNO":
                continue
            if str(row.get("underlying_symbol", "")).upper() != underlying.upper():
                continue
            option_type = str(row.get("instrument_type", "")).upper()
            if option_type not in {"CE", "PE"}:
                continue
            if not expiry or date.fromisoformat(expiry) < today or not bool(int(row.get("buy_allowed", 0) or 0)):
                continue
            lot_size = int(row.get("lot_size", 0) or 0)
            tick_size = float(row.get("tick_size", 0) or 0)
            exchange_token = str(row.get("exchange_token", ""))
            groww_symbol = str(row.get("groww_symbol", ""))
            if lot_size <= 0 or tick_size <= 0 or not exchange_token or not groww_symbol:
                continue
            output.append(
                GrowwContract(
                    trading_symbol=str(row["trading_symbol"]),
                    groww_symbol=groww_symbol,
                    underlying=underlying.upper(),
                    option_type=option_type,
                    expiry=expiry,
                    strike=float(row.get("strike_price", 0) or 0),
                    lot_size=lot_size,
                    tick_size=tick_size,
                    exchange_token=exchange_token,
                    buy_allowed=True,
                )
            )
        return sorted(output, key=lambda contract: (contract.expiry, contract.strike, contract.option_type))

    def discover_current_expiries(self, underlying: str, *, today: date | None = None) -> list[str]:
        return sorted({contract.expiry for contract in self.discover_contracts(underlying, today=today)})

    def historical_expiries(self, underlying: str, *, year: int | None = None, month: int | None = None) -> list[str]:
        response = self.client.get_expiries(exchange="NSE", underlying_symbol=underlying.upper(), year=year, month=month)
        return [str(value) for value in response.get("expiries", [])]

    def historical_contracts(self, underlying: str, expiry: str) -> list[str]:
        response = self.client.get_contracts(exchange="NSE", underlying_symbol=underlying.upper(), expiry_date=expiry)
        return [str(value) for value in response.get("contracts", [])]

    def option_chain(self, underlying: str, expiry: str) -> dict[str, Any]:
        return dict(self.client.get_option_chain(exchange="NSE", underlying=underlying.upper(), expiry_date=expiry))

    def greeks(self, underlying: str, trading_symbol: str, expiry: str) -> dict[str, Any]:
        return dict(
            self.client.get_greeks(
                exchange="NSE",
                underlying=underlying.upper(),
                trading_symbol=trading_symbol,
                expiry=expiry,
            )
        )

    def quote(self, trading_symbol: str, *, segment: str = "FNO") -> dict[str, Any]:
        return dict(self.client.get_quote(exchange="NSE", segment=segment, trading_symbol=trading_symbol))

    def ltp(self, exchange_trading_symbols: str | tuple[str, ...], *, segment: str = "FNO") -> dict[str, Any]:
        return dict(self.client.get_ltp(segment=segment, exchange_trading_symbols=exchange_trading_symbols))

    def ohlc(self, exchange_trading_symbols: str | tuple[str, ...], *, segment: str = "FNO") -> dict[str, Any]:
        return dict(self.client.get_ohlc(segment=segment, exchange_trading_symbols=exchange_trading_symbols))

    def historical_candles(
        self,
        *,
        groww_symbol: str,
        segment: str,
        start_time: str,
        end_time: str,
        candle_interval: str = "1minute",
    ) -> list[list[Any]]:
        """Fetch documented V2 historical candles using Groww symbols only."""
        if candle_interval not in set(self.INTERVAL_MAP.values()) | {"1day", "1week", "1month"}:
            raise ValueError("UNSUPPORTED_GROWW_CANDLE_INTERVAL")
        value = self.client.get_historical_candles(
            exchange="NSE",
            segment=segment,
            groww_symbol=groww_symbol,
            start_time=start_time,
            end_time=end_time,
            candle_interval=candle_interval,
        )
        rows = value.get("candles", []) if isinstance(value, dict) else []
        if not isinstance(rows, list):
            raise RuntimeError("INVALID_GROWW_HISTORICAL_CANDLES_RESPONSE")
        return rows

    def rankable_option_rows(
        self,
        *,
        underlying: str,
        expiry: str,
        option_type: str,
        contracts: list[GrowwContract],
        validated_fee_per_lot: float,
        slippage_bps: float,
    ) -> list[dict[str, Any]]:
        """Two-stage watchlist: chain snapshot then quote only approved directional CE/PE contracts."""
        if validated_fee_per_lot < 0 or slippage_bps < 0:
            raise ValueError("VALIDATED_OPTION_COSTS_REQUIRED")
        option_type = option_type.upper()
        if option_type not in {"CE", "PE"}:
            raise ValueError("LONG_OPTION_DIRECTION_MUST_BE_CE_OR_PE")
        chain = self.option_chain(underlying, expiry)
        strikes = chain.get("strikes", chain.get("payload", {}).get("strikes", {})) or {}
        valid = {contract.trading_symbol: contract for contract in contracts if contract.expiry == expiry and contract.option_type == option_type}
        rows: list[dict[str, Any]] = []
        for strike, sides in strikes.items():
            side = (sides or {}).get(option_type)
            if not side or str(side.get("trading_symbol", "")) not in valid:
                continue
            contract = valid[str(side["trading_symbol"])]
            quote = self.quote(contract.trading_symbol)
            rows.append(
                {
                    "trading_symbol": contract.trading_symbol,
                    "option_type": option_type,
                    "expiry": expiry,
                    "strike": float(strike),
                    "premium": float(side.get("ltp", quote.get("last_price", 0)) or 0),
                    "bid": float(quote.get("bid_price", 0) or 0),
                    "ask": float(quote.get("offer_price", 0) or 0),
                    "volume": float(side.get("volume", 0) or 0),
                    "open_interest": float(side.get("open_interest", 0) or 0),
                    "greeks": dict(side.get("greeks", {}) or {}),
                    "lot_size": contract.lot_size,
                    "tick_size": contract.tick_size,
                    "exchange_token": contract.exchange_token,
                    "executable_depth_qty": min(
                        float(quote.get("bid_quantity", 0) or 0),
                        float(quote.get("offer_quantity", 0) or 0),
                    ),
                    "protection_feasible": True,
                    "validated_fee": float(validated_fee_per_lot),
                    "slippage_bps": float(slippage_bps),
                }
            )
        return rows

    # ------------------------------------------------------ portfolio/margins
    def holdings(self) -> list[dict[str, Any]]:
        value = self.client.get_holdings_for_user()
        if isinstance(value, dict):
            return list(value.get("holdings", []) or [])
        return list(value or [])

    def positions(self) -> list[dict[str, Any]]:
        value = self.client.get_positions_for_user(segment="FNO")
        if isinstance(value, dict):
            return list(value.get("positions", value.get("payload", [])) or [])
        return list(value or [])

    def position_for_symbol(self, trading_symbol: str) -> list[dict[str, Any]]:
        value = self.client.get_position_for_trading_symbol(trading_symbol=trading_symbol, segment="FNO")
        return list(value.get("positions", []) if isinstance(value, dict) else [])

    def available_margin(self) -> dict[str, Any]:
        return dict(self.client.get_available_margin_details())

    def order_margin_for_long_option(self, *, contract: GrowwContract, quantity: int, limit_price: float) -> dict[str, Any]:
        if contract.option_type not in {"CE", "PE"} or quantity <= 0 or quantity % contract.lot_size != 0:
            raise ValueError("validated long CE/PE lot quantity required")
        return dict(
            self.client.get_order_margin_details(
                segment="FNO",
                orders=[
                    {
                        "trading_symbol": contract.trading_symbol,
                        "transaction_type": "BUY",
                        "quantity": quantity,
                        "price": limit_price,
                        "order_type": "LIMIT",
                        "product": "MIS",
                        "exchange": "NSE",
                    }
                ],
            )
        )

    # ---------------------------------------------------------- order lifecycle
    def place_long_option_limit(self, *, contract: GrowwContract, quantity: int, limit_price: float, reference_id: str) -> dict[str, Any]:
        self._require_compliance()
        if contract.option_type not in {"CE", "PE"} or not contract.buy_allowed or quantity <= 0 or quantity % contract.lot_size != 0:
            raise ValueError("validated long CE/PE lot quantity required")
        return dict(
            self.client.place_order(
                trading_symbol=contract.trading_symbol,
                quantity=quantity,
                validity="DAY",
                exchange="NSE",
                segment="FNO",
                product="MIS",
                order_type="LIMIT",
                transaction_type="BUY",
                price=limit_price,
                order_reference_id=reference_id,
            )
        )

    def modify_pending_long_option_limit(self, *, groww_order_id: str, quantity: int, limit_price: float) -> dict[str, Any]:
        self._require_compliance()
        if quantity <= 0:
            raise ValueError("positive quantity required")
        return dict(
            self.client.modify_order(
                groww_order_id=groww_order_id,
                segment="FNO",
                quantity=quantity,
                order_type="LIMIT",
                price=limit_price,
            )
        )

    def order_detail(self, groww_order_id: str) -> dict[str, Any]:
        return dict(self.client.get_order_detail(groww_order_id=groww_order_id, segment="FNO"))

    def order_status(self, groww_order_id: str) -> dict[str, Any]:
        return dict(self.client.get_order_status(groww_order_id=groww_order_id, segment="FNO"))

    def order_status_by_reference(self, order_reference_id: str) -> dict[str, Any]:
        return dict(self.client.get_order_status_by_reference(order_reference_id=order_reference_id, segment="FNO"))

    def order_list(self) -> list[dict[str, Any]]:
        value = self.client.get_order_list(segment="FNO", page=0, page_size=100)
        return list(value.get("order_list", []) if isinstance(value, dict) else [])

    def trade_list_for_order(self, groww_order_id: str) -> list[dict[str, Any]]:
        value = self.client.get_trade_list_for_order(groww_order_id=groww_order_id, segment="FNO", page=0, page_size=50)
        return list(value.get("trade_list", []) if isinstance(value, dict) else [])

    def cancel_order(self, groww_order_id: str) -> dict[str, Any]:
        return dict(self.client.cancel_order(groww_order_id=groww_order_id, segment="FNO"))

    # -------------------------------------------------------- TP/SL via OCO
    def create_oco_exit(
        self,
        *,
        contract: GrowwContract,
        quantity: int,
        net_position_quantity: int,
        target_trigger: float,
        target_price: float,
        stop_trigger: float,
        reference_id: str,
    ) -> dict[str, Any]:
        self._require_compliance()
        if quantity <= 0 or quantity > abs(net_position_quantity) or quantity % contract.lot_size != 0:
            raise ValueError("OCO quantity must protect an actually held lot quantity")
        return dict(
            self.client.create_smart_order(
                smart_order_type="OCO",
                reference_id=reference_id,
                segment="FNO",
                trading_symbol=contract.trading_symbol,
                quantity=quantity,
                product_type="MIS",
                exchange="NSE",
                duration="DAY",
                net_position_quantity=net_position_quantity,
                transaction_type="SELL",
                target={"trigger_price": f"{target_trigger:.2f}", "order_type": "LIMIT", "price": f"{target_price:.2f}"},
                stop_loss={"trigger_price": f"{stop_trigger:.2f}", "order_type": "SL_M", "price": None},
            )
        )

    def smart_order(self, smart_order_id: str) -> dict[str, Any]:
        return dict(self.client.get_smart_order(smart_order_id=smart_order_id, segment="FNO", smart_order_type="OCO"))

    def modify_oco_exit(self, *, smart_order_id: str, quantity: int, target_trigger: float, stop_trigger: float) -> dict[str, Any]:
        self._require_compliance()
        if quantity <= 0:
            raise ValueError("OCO protection quantity must be positive")
        return dict(
            self.client.modify_smart_order(
                smart_order_id=smart_order_id,
                smart_order_type="OCO",
                segment="FNO",
                quantity=quantity,
                duration="DAY",
                product_type="MIS",
                target={"trigger_price": f"{target_trigger:.2f}"},
                stop_loss={"trigger_price": f"{stop_trigger:.2f}"},
            )
        )

    def cancel_oco_exit(self, smart_order_id: str) -> dict[str, Any]:
        self._require_compliance()
        return dict(self.client.cancel_smart_order(segment="FNO", smart_order_type="OCO", smart_order_id=smart_order_id))

    def active_oco_orders(self, *, start_date_time: str, end_date_time: str) -> list[dict[str, Any]]:
        value = self.client.get_smart_order_list(
            segment="FNO",
            smart_order_type="OCO",
            status="ACTIVE",
            page=0,
            page_size=50,
            start_date_time=start_date_time,
            end_date_time=end_date_time,
        )
        return list(value.get("orders", []) if isinstance(value, dict) else [])

    def emergency_limit_sell(self, *, contract: GrowwContract, quantity: int, limit_price: float, reference_id: str) -> dict[str, Any]:
        self._require_compliance()
        if quantity <= 0 or quantity % contract.lot_size != 0:
            raise ValueError("validated held lot quantity required")
        return dict(
            self.client.place_order(
                trading_symbol=contract.trading_symbol,
                quantity=quantity,
                validity="DAY",
                exchange="NSE",
                segment="FNO",
                product="MIS",
                order_type="LIMIT",
                transaction_type="SELL",
                price=limit_price,
                order_reference_id=reference_id,
            )
        )

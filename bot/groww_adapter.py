from __future__ import annotations

import json
import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable

log = logging.getLogger(__name__)


def _round_to_tick(price: float, tick_size: float = 0.05) -> float:
    if tick_size <= 0:
        tick_size = 0.05
    return round(round(float(price) / tick_size) * tick_size, 2)


class GrowwUnavailable(RuntimeError):
    pass


@dataclass
class GrowwAdapter:
    totp_token: str
    totp_secret: str

    def __post_init__(self) -> None:
        if not self.totp_token:
            raise GrowwUnavailable("GROWW_TOTP_TOKEN is missing")
        if not self.totp_secret:
            raise GrowwUnavailable("GROWW_TOTP_SECRET is missing")
        try:
            from growwapi import GrowwAPI  # type: ignore
        except Exception as exc:  # pragma: no cover
            raise GrowwUnavailable("growwapi package is not installed. Run: pip install -r requirements.txt") from exc
        try:
            import pyotp  # type: ignore
        except Exception as exc:  # pragma: no cover
            raise GrowwUnavailable("pyotp package is not installed. Run: pip install -r requirements.txt") from exc

        # Groww TOTP flow: generate the current one-time password locally,
        # exchange it for an access token, then initialise the SDK with that access token.
        totp = pyotp.TOTP(self.totp_secret).now()
        access_token = GrowwAPI.get_access_token(api_key=self.totp_token, totp=totp)
        self.groww = GrowwAPI(access_token)

    def c(self, name: str, fallback: str) -> str:
        return getattr(self.groww, name, fallback)

    @property
    def EXCHANGE_NSE(self) -> str:
        return self.c("EXCHANGE_NSE", "NSE")

    @property
    def EXCHANGE_MCX(self) -> str:
        return self.c("EXCHANGE_MCX", "MCX")

    @property
    def SEGMENT_FNO(self) -> str:
        return self.c("SEGMENT_FNO", "FNO")

    @property
    def SEGMENT_COMMODITY(self) -> str:
        return self.c("SEGMENT_COMMODITY", "COMMODITY")

    @property
    def SEGMENT_CASH(self) -> str:
        return self.c("SEGMENT_CASH", "CASH")

    @property
    def PRODUCT_MIS(self) -> str:
        return self.c("PRODUCT_MIS", "MIS")

    @property
    def VALIDITY_DAY(self) -> str:
        return self.c("VALIDITY_DAY", "DAY")

    @property
    def ORDER_TYPE_LIMIT(self) -> str:
        return self.c("ORDER_TYPE_LIMIT", "LIMIT")

    @property
    def ORDER_TYPE_MARKET(self) -> str:
        return self.c("ORDER_TYPE_MARKET", "MARKET")

    @property
    def ORDER_TYPE_STOP_LOSS_MARKET(self) -> str:
        return self.c("ORDER_TYPE_STOP_LOSS_MARKET", "SL_M")

    @property
    def TRANSACTION_TYPE_BUY(self) -> str:
        return self.c("TRANSACTION_TYPE_BUY", "BUY")

    @property
    def TRANSACTION_TYPE_SELL(self) -> str:
        return self.c("TRANSACTION_TYPE_SELL", "SELL")

    @property
    def SMART_ORDER_TYPE_OCO(self) -> str:
        return self.c("SMART_ORDER_TYPE_OCO", "OCO")

    def exchange_const(self, exchange: str | None) -> str:
        if not exchange:
            return self.EXCHANGE_NSE
        return getattr(self.groww, f"EXCHANGE_{exchange.upper()}", exchange.upper())

    def segment_const(self, segment: str | None) -> str:
        if not segment:
            return self.SEGMENT_FNO
        return getattr(self.groww, f"SEGMENT_{segment.upper()}", segment.upper())

    def product_const(self, product: str | None) -> str:
        if not product:
            return self.PRODUCT_MIS
        return getattr(self.groww, f"PRODUCT_{product.upper()}", product.upper())

    def get_option_chain(self, underlying: str, expiry_date: str, exchange: str | None = None) -> dict[str, Any]:
        return self.groww.get_option_chain(
            exchange=self.exchange_const(exchange),
            underlying=underlying,
            expiry_date=expiry_date,
        )

    def get_quote(self, trading_symbol: str, segment: str | None = None, exchange: str | None = None) -> dict[str, Any]:
        return self.groww.get_quote(
            exchange=self.exchange_const(exchange),
            segment=self.segment_const(segment),
            trading_symbol=trading_symbol,
        )

    def get_ltp(self, exchange_trading_symbols: tuple[str, ...] | str, segment: str | None = None) -> dict[str, float]:
        return self.groww.get_ltp(
            segment=self.segment_const(segment),
            exchange_trading_symbols=exchange_trading_symbols,
        )

    def get_historical_candles(self, groww_symbol: str, start_time: str, end_time: str, interval_const: str, exchange: str | None = None, segment: str | None = None) -> dict[str, Any]:
        return self.groww.get_historical_candles(
            exchange=self.exchange_const(exchange),
            segment=self.segment_const(segment),
            groww_symbol=groww_symbol,
            start_time=start_time,
            end_time=end_time,
            candle_interval=interval_const,
        )

    def place_buy_option_limit(
        self,
        trading_symbol: str,
        quantity: int,
        price: float,
        product: str = "MIS",
        exchange: str | None = None,
        segment: str | None = None,
    ) -> dict[str, Any]:
        ref = f"NOB{uuid.uuid4().hex[:12]}"[:20]
        return self.groww.place_order(
            trading_symbol=trading_symbol,
            quantity=int(quantity),
            validity=self.VALIDITY_DAY,
            exchange=self.exchange_const(exchange),
            segment=self.segment_const(segment),
            product=self.product_const(product),
            order_type=self.ORDER_TYPE_LIMIT,
            transaction_type=self.TRANSACTION_TYPE_BUY,
            price=float(price),
            order_reference_id=ref,
        )

    def place_sell_option_limit(
        self,
        trading_symbol: str,
        quantity: int,
        price: float,
        product: str = "MIS",
        exchange: str | None = None,
        segment: str | None = None,
    ) -> dict[str, Any]:
        ref = f"NOS{uuid.uuid4().hex[:12]}"[:20]
        return self.groww.place_order(
            trading_symbol=trading_symbol,
            quantity=int(quantity),
            validity=self.VALIDITY_DAY,
            exchange=self.exchange_const(exchange),
            segment=self.segment_const(segment),
            product=self.product_const(product),
            order_type=self.ORDER_TYPE_LIMIT,
            transaction_type=self.TRANSACTION_TYPE_SELL,
            price=float(price),
            order_reference_id=ref,
        )

    def get_order_detail(self, groww_order_id: str, segment: str | None = None) -> dict[str, Any]:
        return self.groww.get_order_detail(groww_order_id=groww_order_id, segment=self.segment_const(segment))

    def get_order_list(self, segment: str | None = None, page: int = 0, page_size: int = 25) -> dict[str, Any]:
        return self.groww.get_order_list(segment=self.segment_const(segment), page=page, page_size=page_size)

    def get_positions_for_user(self, segment: str | None = None) -> dict[str, Any]:
        return self.groww.get_positions_for_user(segment=self.segment_const(segment))

    def get_available_margin_details(self) -> dict[str, Any]:
        return self.groww.get_available_margin_details()

    def get_order_margin_details(self, orders: list[dict[str, Any]], segment: str | None = None) -> dict[str, Any]:
        return self.groww.get_order_margin_details(segment=self.segment_const(segment), orders=orders)

    def wait_for_fill(self, groww_order_id: str, timeout_seconds: int = 10, segment: str | None = None) -> dict[str, Any]:
        deadline = time.time() + timeout_seconds
        last: dict[str, Any] = {}
        while time.time() < deadline:
            last = self.get_order_detail(groww_order_id, segment=segment)
            status = str(last.get("order_status", "")).upper()
            filled = int(float(last.get("filled_quantity") or last.get("filledQty") or 0))
            if status in {"EXECUTED", "COMPLETE", "COMPLETED"} and filled > 0:
                return last
            if status in {"REJECTED", "CANCELLED", "FAILED"}:
                return last
            time.sleep(0.75)
        return last

    def create_exit_oco(
        self,
        trading_symbol: str,
        quantity: int,
        fill_price: float,
        tp_pct: float,
        sl_pct: float,
        product: str = "MIS",
        exchange: str | None = None,
        segment: str | None = None,
        tick_size: float = 0.05,
    ) -> dict[str, Any]:
        target_trigger = _round_to_tick(float(fill_price) * (1 + tp_pct), tick_size)
        target_limit = _round_to_tick(target_trigger + tick_size, tick_size)
        stop_trigger = _round_to_tick(max(tick_size, float(fill_price) * (1 - sl_pct)), tick_size)
        ref = f"OCO{uuid.uuid4().hex[:12]}"[:20]
        return self.groww.create_smart_order(
            smart_order_type=self.SMART_ORDER_TYPE_OCO,
            reference_id=ref,
            segment=self.segment_const(segment),
            trading_symbol=trading_symbol,
            quantity=int(quantity),
            product_type=self.product_const(product),
            exchange=self.exchange_const(exchange),
            duration=self.VALIDITY_DAY,
            net_position_quantity=int(quantity),
            transaction_type=self.TRANSACTION_TYPE_SELL,
            target={
                "trigger_price": f"{target_trigger:.2f}",
                "order_type": self.ORDER_TYPE_LIMIT,
                "price": f"{target_limit:.2f}",
            },
            stop_loss={
                "trigger_price": f"{stop_trigger:.2f}",
                "order_type": self.ORDER_TYPE_STOP_LOSS_MARKET,
                "price": None,
            },
        )

    def create_short_exit_oco(
        self,
        trading_symbol: str,
        quantity: int,
        fill_price: float,
        tp_pct: float,
        sl_pct: float,
        product: str = "MIS",
        exchange: str | None = None,
        segment: str | None = None,
        tick_size: float = 0.05,
    ) -> dict[str, Any]:
        target_trigger = _round_to_tick(max(tick_size, float(fill_price) * (1 - tp_pct)), tick_size)
        target_limit = _round_to_tick(max(tick_size, target_trigger - tick_size), tick_size)
        stop_trigger = _round_to_tick(float(fill_price) * (1 + sl_pct), tick_size)
        ref = f"SOC{uuid.uuid4().hex[:12]}"[:20]
        return self.groww.create_smart_order(
            smart_order_type=self.SMART_ORDER_TYPE_OCO,
            reference_id=ref,
            segment=self.segment_const(segment),
            trading_symbol=trading_symbol,
            quantity=int(quantity),
            product_type=self.product_const(product),
            exchange=self.exchange_const(exchange),
            duration=self.VALIDITY_DAY,
            net_position_quantity=-int(quantity),
            transaction_type=self.TRANSACTION_TYPE_BUY,
            target={
                "trigger_price": f"{target_trigger:.2f}",
                "order_type": self.ORDER_TYPE_LIMIT,
                "price": f"{target_limit:.2f}",
            },
            stop_loss={
                "trigger_price": f"{stop_trigger:.2f}",
                "order_type": self.ORDER_TYPE_STOP_LOSS_MARKET,
                "price": None,
            },
        )

    def subscribe_depth_forever(self, instruments: list[dict[str, str]], on_update: Callable[[dict[str, Any]], None]) -> None:
        try:
            from growwapi import GrowwFeed  # type: ignore
        except Exception as exc:  # pragma: no cover
            raise GrowwUnavailable("GrowwFeed unavailable. Upgrade growwapi SDK.") from exc
        feed = GrowwFeed(self.groww)

        def _cb(meta: dict[str, Any]) -> None:
            try:
                on_update(feed.get_market_depth())
            except Exception:
                log.exception("Depth callback failed | meta=%s", json.dumps(meta, default=str))

        feed.subscribe_market_depth(instruments, on_data_received=_cb)
        feed.consume()

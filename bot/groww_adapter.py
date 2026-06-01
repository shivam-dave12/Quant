from __future__ import annotations

import json
import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable

log = logging.getLogger(__name__)


class GrowwUnavailable(RuntimeError):
    pass


@dataclass
class GrowwAdapter:
    token: str

    def __post_init__(self) -> None:
        if not self.token:
            raise GrowwUnavailable("GROWW_API_AUTH_TOKEN is missing")
        try:
            from growwapi import GrowwAPI  # type: ignore
        except Exception as exc:  # pragma: no cover
            raise GrowwUnavailable("growwapi package is not installed. Run: pip install -r requirements.txt") from exc
        self.groww = GrowwAPI(self.token)

    def c(self, name: str, fallback: str) -> str:
        return getattr(self.groww, name, fallback)

    @property
    def EXCHANGE_NSE(self) -> str:
        return self.c("EXCHANGE_NSE", "NSE")

    @property
    def SEGMENT_FNO(self) -> str:
        return self.c("SEGMENT_FNO", "FNO")

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

    def get_option_chain(self, underlying: str, expiry_date: str) -> dict[str, Any]:
        return self.groww.get_option_chain(
            exchange=self.EXCHANGE_NSE,
            underlying=underlying,
            expiry_date=expiry_date,
        )

    def get_quote(self, trading_symbol: str, segment: str | None = None) -> dict[str, Any]:
        return self.groww.get_quote(
            exchange=self.EXCHANGE_NSE,
            segment=segment or self.SEGMENT_FNO,
            trading_symbol=trading_symbol,
        )

    def get_ltp(self, exchange_trading_symbols: tuple[str, ...] | str, segment: str | None = None) -> dict[str, float]:
        return self.groww.get_ltp(
            segment=segment or self.SEGMENT_FNO,
            exchange_trading_symbols=exchange_trading_symbols,
        )

    def get_historical_candles(self, groww_symbol: str, start_time: str, end_time: str, interval_const: str) -> dict[str, Any]:
        return self.groww.get_historical_candles(
            exchange=self.EXCHANGE_NSE,
            segment=self.SEGMENT_FNO,
            groww_symbol=groww_symbol,
            start_time=start_time,
            end_time=end_time,
            candle_interval=interval_const,
        )

    def place_buy_option_limit(self, trading_symbol: str, quantity: int, price: float, product: str = "MIS") -> dict[str, Any]:
        ref = f"NOB{uuid.uuid4().hex[:12]}"[:20]
        product_const = getattr(self.groww, f"PRODUCT_{product.upper()}", product.upper())
        return self.groww.place_order(
            trading_symbol=trading_symbol,
            quantity=int(quantity),
            validity=self.VALIDITY_DAY,
            exchange=self.EXCHANGE_NSE,
            segment=self.SEGMENT_FNO,
            product=product_const,
            order_type=self.ORDER_TYPE_LIMIT,
            transaction_type=self.TRANSACTION_TYPE_BUY,
            price=float(price),
            order_reference_id=ref,
        )

    def get_order_detail(self, groww_order_id: str) -> dict[str, Any]:
        return self.groww.get_order_detail(groww_order_id=groww_order_id, segment=self.SEGMENT_FNO)

    def wait_for_fill(self, groww_order_id: str, timeout_seconds: int = 10) -> dict[str, Any]:
        deadline = time.time() + timeout_seconds
        last: dict[str, Any] = {}
        while time.time() < deadline:
            last = self.get_order_detail(groww_order_id)
            status = str(last.get("order_status", "")).upper()
            filled = int(float(last.get("filled_quantity") or last.get("filledQty") or 0))
            if status in {"EXECUTED", "COMPLETE", "COMPLETED"} and filled > 0:
                return last
            if status in {"REJECTED", "CANCELLED", "FAILED"}:
                return last
            time.sleep(0.75)
        return last

    def create_exit_oco(self, trading_symbol: str, quantity: int, fill_price: float, tp_pct: float, sl_pct: float, product: str = "MIS") -> dict[str, Any]:
        product_const = getattr(self.groww, f"PRODUCT_{product.upper()}", product.upper())
        target_trigger = round(float(fill_price) * (1 + tp_pct), 2)
        target_limit = round(target_trigger + 0.05, 2)
        stop_trigger = round(max(0.05, float(fill_price) * (1 - sl_pct)), 2)
        ref = f"OCO{uuid.uuid4().hex[:12]}"[:20]
        return self.groww.create_smart_order(
            smart_order_type=self.SMART_ORDER_TYPE_OCO,
            reference_id=ref,
            segment=self.SEGMENT_FNO,
            trading_symbol=trading_symbol,
            quantity=int(quantity),
            product_type=product_const,
            exchange=self.EXCHANGE_NSE,
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

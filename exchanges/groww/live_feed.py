"""Groww Feed adapter using the official ``GrowwFeed`` SDK client."""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable, Dict, Iterable

from .api import GrowwRestClient

logger = logging.getLogger(__name__)

_HUBS: dict[int, "GrowwLiveFeedHub"] = {}
_HUBS_LOCK = threading.RLock()


class GrowwLiveFeedHub:
    def __init__(self, api: GrowwRestClient) -> None:
        self.api = api
        self.feed = None
        self._lock = threading.RLock()
        self._thread: threading.Thread | None = None
        self._subscriptions: dict[str, tuple[str, list[dict[str, str]], Callable[[Any], None]]] = {}
        self._connected = False
        self._last_tick_ts = 0.0

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def last_tick_ts(self) -> float:
        return self._last_tick_ts

    def _feed(self):
        if self.feed is not None:
            return self.feed
        try:
            from growwapi import GrowwFeed
        except Exception as exc:  # pragma: no cover - live dependency
            raise RuntimeError("Groww official SDK feed client is unavailable. Install growwapi.") from exc
        self.feed = GrowwFeed(self.api.client)
        return self.feed

    def _start_consumer(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        feed = self._feed()
        consume = getattr(feed, "consume", None)
        if not callable(consume):
            self._connected = True
            return

        def _worker() -> None:
            try:
                self._connected = True
                consume()
            except Exception as exc:
                self._connected = False
                logger.error("Groww feed consume loop stopped: %s", exc)

        self._thread = threading.Thread(target=_worker, name="groww-feed", daemon=True)
        self._thread.start()

    @staticmethod
    def _flatten_ltp(payload: Any) -> Iterable[dict[str, Any]]:
        root = payload.get("ltp") if isinstance(payload, dict) else payload
        if not isinstance(root, dict):
            return []
        rows: list[dict[str, Any]] = []
        for exchange, segments in root.items():
            if not isinstance(segments, dict):
                continue
            for segment, tokens in segments.items():
                if not isinstance(tokens, dict):
                    continue
                for token, data in tokens.items():
                    if isinstance(data, dict):
                        row = dict(data)
                    else:
                        row = {"ltp": data}
                    row.update({"exchange": exchange, "segment": segment, "exchange_token": token})
                    rows.append(row)
        return rows

    def _subscribe_ltp(self, instruments: list[dict[str, str]], callback: Callable[[Any], None]) -> str:
        feed = self._feed()
        sid = f"ltp:{len(self._subscriptions) + 1}:{int(time.time() * 1000)}"

        def _on_data(_meta=None) -> None:
            try:
                data = feed.get_ltp()
                self._last_tick_ts = time.time()
                for row in self._flatten_ltp(data):
                    callback(row)
            except Exception as exc:
                logger.debug("Groww feed LTP callback rejected: %s", exc)

        feed.subscribe_ltp(instruments, on_data_received=_on_data)
        self._subscriptions[sid] = ("ltp", instruments, callback)
        self._connected = True
        self._start_consumer()
        return sid

    def subscribe_underlying_quotes(self, exchange_code: str, stock_code: str, callback: Callable[[Any], None]) -> str:
        exchange = str(exchange_code or "NSE").upper()
        if exchange == "NFO":
            exchange = "NSE"
        segment = "CASH"
        symbol = str(stock_code or "").strip().upper()
        token = self.api.resolve_exchange_token(exchange=exchange, segment=segment, trading_symbol=symbol) or symbol
        instrument = {"exchange": exchange, "segment": segment, "exchange_token": token}
        return self._subscribe_ltp([instrument], callback)

    def subscribe_option_quotes_and_ohlcv(
        self,
        *,
        stock_code: str,
        expiry_date: str,
        strike_price: str,
        right: str,
        callback: Callable[[Any], None],
    ) -> list[str]:
        route = {
            "stock_code": stock_code,
            "exchange_code": "NFO",
            "segment": "FNO",
            "expiry_date": expiry_date,
            "strike_price": strike_price,
            "right": right,
        }
        symbol = self.api._option_symbol_from_route(route)
        if not symbol:
            raise RuntimeError(f"Groww feed route missing option trading_symbol for {route}")
        token = self.api.resolve_exchange_token(exchange="NSE", segment="FNO", trading_symbol=symbol)
        if not token:
            raise RuntimeError(f"Groww exchange token not found for {symbol}")

        def _callback(row: Any) -> None:
            if isinstance(row, dict):
                out = dict(row)
                out.update({
                    "stock_code": stock_code,
                    "exchange_code": "NFO",
                    "exchange": "NSE",
                    "segment": "FNO",
                    "trading_symbol": symbol,
                    "TradingSymbol": symbol,
                    "expiry_date": expiry_date,
                    "strike_price": strike_price,
                    "right": right,
                    "last_price": row.get("ltp") or row.get("last_price"),
                })
                callback(out)
            else:
                callback(row)

        sid = self._subscribe_ltp([{"exchange": "NSE", "segment": "FNO", "exchange_token": token}], _callback)
        return [sid]

    def unsubscribe(self, subscription_ids: list[str]) -> None:
        feed = self._feed()
        for sid in list(subscription_ids or []):
            item = self._subscriptions.pop(str(sid), None)
            if not item:
                continue
            typ, instruments, _callback = item
            try:
                if typ == "ltp" and hasattr(feed, "unsubscribe_ltp"):
                    feed.unsubscribe_ltp(instruments)
            except Exception:
                pass

    def reconnect_and_resubscribe(self, reason: str = "repair") -> bool:
        _ = reason
        with self._lock:
            self._connected = False
            self.feed = None
            subs = list(self._subscriptions.values())
            self._subscriptions.clear()
            for typ, instruments, callback in subs:
                if typ == "ltp":
                    self._subscribe_ltp(instruments, callback)
            return True

    def ensure_live(self, max_stale_sec: float = 15.0, reason: str = "health_check") -> bool:
        age = time.time() - self._last_tick_ts if self._last_tick_ts else 999999.0
        if self._connected and age <= float(max_stale_sec):
            return True
        return self.reconnect_and_resubscribe(reason=reason)


def hub_for_api(api: GrowwRestClient) -> GrowwLiveFeedHub:
    key = id(api)
    with _HUBS_LOCK:
        hub = _HUBS.get(key)
        if hub is None:
            hub = GrowwLiveFeedHub(api)
            _HUBS[key] = hub
        return hub

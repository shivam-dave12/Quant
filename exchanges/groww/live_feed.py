"""Groww Feed transport wired to the official documented subscription methods.

Indices use ``subscribe_index_value``/``get_index_value``.  Executable F&O
contracts use both ``subscribe_ltp`` and ``subscribe_market_depth`` so the
strategy receives an actual traded price and a real two-sided book.  The feed
does not pretend to emit OHLCV candles; candle warm-up comes from Groww's
documented historical-candles endpoint.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable, Iterable

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
            raise RuntimeError("Groww official SDK feed client is unavailable. Install/upgrade growwapi.") from exc
        self.feed = GrowwFeed(self.api.client)
        return self.feed

    def _start_consumer(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        feed = self._feed()
        consume = getattr(feed, "consume", None)
        if not callable(consume):
            raise RuntimeError("Groww official SDK Feed.consume method is unavailable; upgrade growwapi.")

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
        root = payload.get("ltp") if isinstance(payload, dict) and isinstance(payload.get("ltp"), dict) else payload
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
                    row = dict(data) if isinstance(data, dict) else {"ltp": data}
                    if "ltp" in row:
                        row.setdefault("last_price", row["ltp"])
                    row.update({"exchange": exchange, "segment": segment, "exchange_token": str(token)})
                    rows.append(row)
        return rows

    @staticmethod
    def _flatten_index_value(payload: Any) -> Iterable[dict[str, Any]]:
        root = payload.get("index_value") if isinstance(payload, dict) and isinstance(payload.get("index_value"), dict) else payload
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
                    row = dict(data) if isinstance(data, dict) else {"value": data}
                    value = row.get("value")
                    row.update({
                        "exchange": exchange,
                        "segment": segment,
                        "exchange_token": str(token),
                        "stock_code": str(token),
                        "last_price": value,
                        "ltp": value,
                    })
                    rows.append(row)
        return rows

    @staticmethod
    def _top_depth(levels: Any) -> tuple[float, float]:
        if isinstance(levels, dict):
            values = list(levels.values())
        elif isinstance(levels, list):
            values = levels
        else:
            values = []
        for level in values:
            if not isinstance(level, dict):
                continue
            try:
                price = float(level.get("price") or 0.0)
                qty = float(level.get("qty") or level.get("quantity") or 0.0)
            except Exception:
                continue
            if price > 0 and qty > 0:
                return price, qty
        return 0.0, 0.0

    @classmethod
    def _flatten_market_depth(cls, payload: Any) -> Iterable[dict[str, Any]]:
        root = payload.get("market_depth") if isinstance(payload, dict) and isinstance(payload.get("market_depth"), dict) else payload
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
                    if not isinstance(data, dict):
                        continue
                    bid, bid_qty = cls._top_depth(data.get("buyBook"))
                    ask, ask_qty = cls._top_depth(data.get("sellBook"))
                    row = dict(data)
                    row.update({
                        "exchange": exchange,
                        "segment": segment,
                        "exchange_token": str(token),
                        "bid_price": bid,
                        "bid_quantity": bid_qty,
                        "offer_price": ask,
                        "offer_quantity": ask_qty,
                        "best_bid_price": bid,
                        "best_bid_quantity": bid_qty,
                        "best_offer_price": ask,
                        "best_offer_quantity": ask_qty,
                    })
                    rows.append(row)
        return rows

    def _subscribe(self, typ: str, instruments: list[dict[str, str]], callback: Callable[[Any], None]) -> str:
        feed = self._feed()
        sid = f"{typ}:{len(self._subscriptions) + 1}:{int(time.time() * 1000)}"
        if typ == "ltp":
            subscribe, getter, flatten = feed.subscribe_ltp, feed.get_ltp, self._flatten_ltp
        elif typ == "market_depth":
            subscribe, getter, flatten = feed.subscribe_market_depth, feed.get_market_depth, self._flatten_market_depth
        elif typ == "index_value":
            subscribe, getter, flatten = feed.subscribe_index_value, feed.get_index_value, self._flatten_index_value
        else:  # pragma: no cover - internal misuse
            raise RuntimeError(f"Unsupported Groww subscription type: {typ}")

        def _on_data(_meta=None) -> None:
            try:
                data = getter()
                self._last_tick_ts = time.time()
                for row in flatten(data):
                    callback(row)
            except Exception as exc:
                logger.debug("Groww feed %s callback rejected: %s", typ, exc)

        subscribe(instruments, on_data_received=_on_data)
        self._subscriptions[sid] = (typ, instruments, callback)
        self._connected = True
        self._start_consumer()
        return sid

    def subscribe_underlying_quotes(self, exchange_code: str, stock_code: str, callback: Callable[[Any], None]) -> str:
        exchange = str(exchange_code or "NSE").upper()
        if exchange == "NFO":
            exchange = "NSE"
        symbol = str(stock_code or "").strip().upper()
        if not symbol:
            raise RuntimeError("Groww index feed route missing underlying symbol")
        # Groww's Feed documentation represents indices with segment=CASH and
        # exchange_token=NIFTY/SENSEX, consumed via index_value APIs.
        instrument = {"exchange": exchange, "segment": "CASH", "exchange_token": symbol}
        return self._subscribe("index_value", [instrument], callback)

    def subscribe_option_market_data(
        self,
        *,
        stock_code: str,
        expiry_date: str,
        strike_price: str,
        right: str,
        callback: Callable[[Any], None],
    ) -> list[str]:
        """Subscribe an F&O execution vehicle to official LTP and depth feeds.

        Groww's documented feed supplies LTP and market depth; interval candles remain
        sourced from documented historical-candles calls, not fabricated here.
        """
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
            raise RuntimeError(f"Groww exchange token not found in official instrument CSV for {symbol}")

        def _callback(row: Any) -> None:
            if not isinstance(row, dict):
                callback(row)
                return
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
            })
            if out.get("ltp") is not None:
                out.setdefault("last_price", out.get("ltp"))
            callback(out)

        instrument = {"exchange": "NSE", "segment": "FNO", "exchange_token": token}
        return [
            self._subscribe("ltp", [instrument], _callback),
            self._subscribe("market_depth", [instrument], _callback),
        ]

    def unsubscribe(self, subscription_ids: list[str]) -> None:
        feed = self._feed()
        for sid in list(subscription_ids or []):
            item = self._subscriptions.pop(str(sid), None)
            if not item:
                continue
            typ, instruments, _callback = item
            method = {
                "ltp": "unsubscribe_ltp",
                "market_depth": "unsubscribe_market_depth",
                "index_value": "unsubscribe_index_value",
            }.get(typ)
            unsubscriber = getattr(feed, str(method), None)
            if not callable(unsubscriber):
                raise RuntimeError(f"Groww official SDK missing {method}; upgrade growwapi.")
            unsubscriber(instruments)

    def reconnect_and_resubscribe(self, reason: str = "repair") -> bool:
        logger.warning("GROWW shared feed reconnect requested: reason=%s", reason)
        with self._lock:
            self._connected = False
            self.feed = None
            subs = list(self._subscriptions.values())
            self._subscriptions.clear()
            for typ, instruments, callback in subs:
                self._subscribe(typ, instruments, callback)
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

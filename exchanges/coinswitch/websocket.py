"""CoinSwitch Pro Futures Socket.IO WebSocket adapter.

Normalises all public-stream callbacks before they enter the trading system.
Callbacks are retained across reconnects; reconnect only re-emits server-side
subscriptions.
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional

import socketio

logger = logging.getLogger(__name__)


class CoinSwitchWebSocket:
    BASE_URL = "https://ws.coinswitch.co"
    HANDSHAKE_PATH = "/pro/realtime-rates-socket/futures/exchange_2"
    NAMESPACE = "/exchange_2"

    _EV_ORDERBOOK = "FETCH_ORDER_BOOK_CS_PRO"
    _EV_CANDLESTICK = "FETCH_CANDLESTICK_CS_PRO"
    _EV_TRADES = "FETCH_TRADES_CS_PRO"

    def __init__(self) -> None:
        self.sio = socketio.Client(
            reconnection=True,
            reconnection_attempts=0,
            reconnection_delay=1,
            reconnection_delay_max=30,
        )
        self.is_connected = False
        self._last_message_time: Optional[datetime] = None
        self._lock = threading.RLock()
        self._subs_lock = threading.RLock()
        self._ob_callbacks: List[Callable] = []
        self._tr_callbacks: List[Callable] = []
        self._can_callbacks: Dict[str, List[Callable]] = {}
        self._subs: Dict[str, list] = {"orderbook": [], "trades": [], "candlestick": []}
        self._setup_handlers()

    @staticmethod
    def _norm_levels(raw) -> list:
        out = []
        for lvl in raw or []:
            try:
                if isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
                    px, qty = float(lvl[0]), float(lvl[1])
                elif isinstance(lvl, dict):
                    px = float(lvl.get("limit_price") or lvl.get("price") or lvl.get("p") or 0)
                    qty = float(lvl.get("size") or lvl.get("quantity") or lvl.get("q") or lvl.get("depth") or 0)
                else:
                    continue
                if px > 0:
                    out.append([px, max(qty, 0.0)])
            except Exception:
                continue
        return out

    def _setup_handlers(self) -> None:
        @self.sio.event(namespace=self.NAMESPACE)
        def connect():
            self.is_connected = True
            self._last_message_time = datetime.now(timezone.utc)
            logger.info("CoinSwitch WS connected")
            self._resubscribe_all()

        @self.sio.event(namespace=self.NAMESPACE)
        def disconnect():
            self.is_connected = False
            logger.warning("CoinSwitch WS disconnected")

        @self.sio.event(namespace=self.NAMESPACE)
        def connect_error(data):
            logger.error("CoinSwitch WS connect_error: %s", data)

        @self.sio.on(self._EV_ORDERBOOK, namespace=self.NAMESPACE)
        def on_orderbook(data):
            try:
                if not isinstance(data, dict):
                    return
                self._last_message_time = datetime.now(timezone.utc)
                msg = {
                    "bids": self._norm_levels(data.get("bids", data.get("b", []))),
                    "asks": self._norm_levels(data.get("asks", data.get("a", []))),
                    "timestamp": time.time(),
                }
                if not msg["bids"] and not msg["asks"]:
                    return
                with self._lock:
                    callbacks = list(self._ob_callbacks)
                for cb in callbacks:
                    try: cb(msg)
                    except Exception as exc: logger.error("CoinSwitch OB callback error: %s", exc)
            except Exception as exc:
                logger.error("CoinSwitch OB handler error: %s", exc)

        @self.sio.on(self._EV_TRADES, namespace=self.NAMESPACE)
        def on_trade(data):
            try:
                if not isinstance(data, dict):
                    return
                self._last_message_time = datetime.now(timezone.utc)
                px = float(data.get("price") or data.get("p") or 0)
                qty = float(data.get("quantity") or data.get("q") or data.get("size") or 0)
                if px <= 0:
                    return
                side_raw = str(data.get("side") or "").lower()
                side = "buy" if side_raw == "buy" else "sell" if side_raw == "sell" else ("sell" if data.get("m") else "buy")
                msg = {"price": px, "quantity": max(qty, 0.0), "side": side, "timestamp": time.time()}
                with self._lock:
                    callbacks = list(self._tr_callbacks)
                for cb in callbacks:
                    try: cb(msg)
                    except Exception as exc: logger.error("CoinSwitch trade callback error: %s", exc)
            except Exception as exc:
                logger.error("CoinSwitch trade handler error: %s", exc)

        @self.sio.on(self._EV_CANDLESTICK, namespace=self.NAMESPACE)
        def on_candle(data):
            try:
                if not isinstance(data, dict) or "success" in data:
                    return
                if "o" not in data or "t" not in data:
                    return
                self._last_message_time = datetime.now(timezone.utc)
                interval_key = str(data.get("i", ""))
                msg = {
                    "t": int(data["t"]),
                    "o": float(data.get("o", 0)),
                    "h": float(data.get("h", 0)),
                    "l": float(data.get("l", 0)),
                    "c": float(data.get("c", 0)),
                    "v": float(data.get("v", 0)),
                    "x": bool(data.get("x", False)),
                    "i": interval_key,
                }
                with self._lock:
                    callbacks = list(self._can_callbacks.get(interval_key, []))
                for cb in callbacks:
                    try: cb(msg)
                    except Exception as exc: logger.error("CoinSwitch candle callback error: %s", exc)
            except Exception as exc:
                logger.error("CoinSwitch candle handler error: %s", exc)

    def _resubscribe_all(self) -> None:
        with self._lock:
            with self._subs_lock:
                subs = {k: list(v) for k, v in self._subs.items()}
        for sub in subs["orderbook"]:
            self.sio.emit(self._EV_ORDERBOOK, {"event": "subscribe", "pair": sub["pair"]}, namespace=self.NAMESPACE)
        for sub in subs["trades"]:
            self.sio.emit(self._EV_TRADES, {"event": "subscribe", "pair": sub["pair"]}, namespace=self.NAMESPACE)
        for sub in subs["candlestick"]:
            self.sio.emit(self._EV_CANDLESTICK, {"event": "subscribe", "pair": sub["pair"]}, namespace=self.NAMESPACE)

    def connect(self, timeout: int = 30) -> bool:
        try:
            self.sio.connect(
                url=self.BASE_URL,
                namespaces=[self.NAMESPACE],
                transports="websocket",
                socketio_path=self.HANDSHAKE_PATH,
                wait=True,
                wait_timeout=timeout,
            )
            return self.is_connected
        except Exception as exc:
            logger.error("CoinSwitch WS connect error: %s", exc, exc_info=True)
            return False

    def disconnect(self) -> None:
        try:
            if self.sio.connected:
                self.sio.disconnect()
        finally:
            self.is_connected = False

    def is_healthy(self, timeout_seconds: int = 35) -> bool:
        if not self.is_connected:
            return False
        if self._last_message_time is None:
            return True
        return (datetime.now(timezone.utc) - self._last_message_time).total_seconds() <= timeout_seconds

    def subscribe_orderbook(self, symbol: str, callback: Callable, depth: int = 20) -> None:
        pair = str(symbol).upper()
        with self._lock:
            self._ob_callbacks.append(callback)
            with self._subs_lock:
                if not any(s.get("pair") == pair for s in self._subs["orderbook"]):
                    self._subs["orderbook"].append({"pair": pair})
        if self.is_connected:
            self.sio.emit(self._EV_ORDERBOOK, {"event": "subscribe", "pair": pair}, namespace=self.NAMESPACE)

    def subscribe_trades(self, symbol: str, callback: Callable) -> None:
        pair = str(symbol).upper()
        with self._lock:
            self._tr_callbacks.append(callback)
            with self._subs_lock:
                if not any(s.get("pair") == pair for s in self._subs["trades"]):
                    self._subs["trades"].append({"pair": pair})
        if self.is_connected:
            self.sio.emit(self._EV_TRADES, {"event": "subscribe", "pair": pair}, namespace=self.NAMESPACE)

    def subscribe_candlestick(self, symbol: str, interval: int, callback: Callable) -> None:
        pair = str(symbol).upper()
        interval_key = str(interval)
        with self._lock:
            self._can_callbacks.setdefault(interval_key, []).append(callback)
            with self._subs_lock:
                marker = {"pair": pair, "interval": interval_key}
                if marker not in self._subs["candlestick"]:
                    self._subs["candlestick"].append(marker)
        if self.is_connected:
            self.sio.emit(self._EV_CANDLESTICK, {"event": "subscribe", "pair": pair}, namespace=self.NAMESPACE)

    # Abstract-interface alias.
    def subscribe_candles(self, symbol: str, interval: int, callback: Callable) -> None:
        self.subscribe_candlestick(symbol, interval, callback)

"""
exchanges/coinswitch/websocket.py — CoinSwitch Futures WebSocket
================================================================
Production-grade Socket.IO client with auto-reconnect and
full auto-resubscription after reconnect. Callbacks survive
every reconnect cycle; no data blindness after disconnect.

Normalises all incoming data to canonical dicts before delivery:
  orderbook → {"bids": [[p,q],...], "asks": [[p,q],...], "timestamp": float}
  trade     → {"price": f, "quantity": f, "side": "buy"|"sell", "timestamp": f}
  candle    → {"t": ms, "o": f, "h": f, "l": f, "c": f, "v": f, "x": bool, "i": str}
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Callable, Dict, List, Optional

import socketio
from datetime import datetime

import sys, os; sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

logger = logging.getLogger(__name__)


class CoinSwitchWebSocket:
    """Production-grade Socket.IO WebSocket for CoinSwitch Pro Futures."""

    BASE_URL       = "https://ws.coinswitch.co"
    HANDSHAKE_PATH = "/pro/realtime-rates-socket/futures/exchange_2"
    NAMESPACE      = "/exchange_2"

    _EV_ORDERBOOK    = "FETCH_ORDER_BOOK_CS_PRO"
    _EV_CANDLESTICK  = "FETCH_CANDLESTICK_CS_PRO"
    _EV_TRADES       = "FETCH_TRADES_CS_PRO"
    _EV_TICKER       = "FETCH_TICKER_INFO_CS_PRO"

    def __init__(self) -> None:
        self.sio = socketio.Client(
            reconnection=True,
            reconnection_attempts=0,
            reconnection_delay=1,
            reconnection_delay_max=30,
        )

        self.is_connected          = False
        self._stop_event           = threading.Event()
        self._last_message_time:   Optional[datetime] = None
        self._connection_failures  = 0

        # Callback lists — interval-keyed for candles
        self._lock                           = threading.RLock()
        self._ob_callbacks:  List[Callable]  = []
        self._tr_callbacks:  List[Callable]  = []
        self._can_callbacks: Dict[str, List[Callable]] = {}

        # Persistent subscription registry for auto-resubscribe
        self._subs_lock = threading.RLock()
        self._subs: Dict[str, list] = {
            "orderbook":    [],
            "candlestick":  [],
            "trades":       [],
        }

        self._setup_handlers()
        logger.info("CoinSwitchWebSocket initialised")

    # ── Internal: event handlers ──────────────────────────────────────────────

    def _setup_handlers(self) -> None:

        @self.sio.event(namespace=self.NAMESPACE)
        def connect():
            self.is_connected = True
            self._connection_failures = 0
            self._last_message_time = datetime.now()
            logger.info("CoinSwitch WS connected")
            self._resubscribe_all()

        @self.sio.event(namespace=self.NAMESPACE)
        def disconnect():
            self.is_connected = False
            logger.warning("CoinSwitch WS disconnected")

        @self.sio.event(namespace=self.NAMESPACE)
        def connect_error(data):
            self._connection_failures += 1
            logger.error(f"CoinSwitch WS connect_error (#{self._connection_failures}): {data}")

        @self.sio.on(self._EV_ORDERBOOK, namespace=self.NAMESPACE)
        def on_orderbook(data):
            try:
                self._last_message_time = datetime.now()
                if not isinstance(data, dict):
                    return
                bids = data.get("bids", data.get("b", []))
                asks = data.get("asks", data.get("a", []))
                if not bids and not asks:
                    return
                normalised = {
                    "bids":      bids,
                    "asks":      asks,
                    "timestamp": time.time(),
                }
                with self._lock:
                    cbs = list(self._ob_callbacks)
                for cb in cbs:
                    try:
                        cb(normalised)
                    except Exception as e:
                        logger.error(f"OB callback error: {e}")
            except Exception as e:
                logger.error(f"OB handler error: {e}")

        @self.sio.on(self._EV_TRADES, namespace=self.NAMESPACE)
        def on_trade(data):
            try:
                self._last_message_time = datetime.now()
                if not isinstance(data, dict) or "p" not in data:
                    return
                normalised = {
                    "price":     float(data["p"]),
                    "quantity":  float(data.get("q", 0)),
                    "side":      "sell" if data.get("m") else "buy",
                    "timestamp": time.time(),
                }
                with self._lock:
                    cbs = list(self._tr_callbacks)
                for cb in cbs:
                    try:
                        cb(normalised)
                    except Exception as e:
                        logger.error(f"Trade callback error: {e}")
            except Exception as e:
                logger.error(f"Trade handler error: {e}")

        @self.sio.on(self._EV_CANDLESTICK, namespace=self.NAMESPACE)
        def on_candle(data):
            try:
                self._last_message_time = datetime.now()
                if not isinstance(data, dict):
                    return
                if "success" in data:   # subscription confirmation
                    return
                if "o" not in data or "t" not in data:
                    return
                interval_key = str(data.get("i", ""))
                with self._lock:
                    cbs = list(self._can_callbacks.get(interval_key, []))
                if not cbs:
                    return
                # Normalise to canonical candle dict
                normalised = {
                    "t": int(data["t"]),
                    "o": float(data.get("o", 0)),
                    "h": float(data.get("h", 0)),
                    "l": float(data.get("l", 0)),
                    "c": float(data.get("c", 0)),
                    "v": float(data.get("v", 0)),
                    "x": bool(data.get("x", False)),
                    "i": interval_key,
                }
                for cb in cbs:
                    try:
                        cb(normalised)
                    except Exception as e:
                        logger.error(f"Candle callback error (i={interval_key}): {e}")
            except Exception as e:
                logger.error(f"Candle handler error: {e}")

    def _resubscribe_all(self) -> None:
        """Re-register all subscriptions and restore callbacks after reconnect."""
        with self._subs_lock:
            logger.info("CoinSwitch WS: resubscribing all streams...")

            with self._lock:
                self._ob_callbacks.clear()
                self._tr_callbacks.clear()
                self._can_callbacks.clear()

            for sub in self._subs["orderbook"]:
                cb = sub.get("callback")
                if cb:
                    with self._lock:
                        self._ob_callbacks.append(cb)
                self.sio.emit(self._EV_ORDERBOOK,
                              {"event": "subscribe", "pair": sub["pair"]},
                              namespace=self.NAMESPACE)

            for sub in self._subs["candlestick"]:
                cb  = sub.get("callback")
                ikey = str(sub.get("interval", ""))
                if cb and ikey:
                    with self._lock:
                        self._can_callbacks.setdefault(ikey, [])
                        if cb not in self._can_callbacks[ikey]:
                            self._can_callbacks[ikey].append(cb)
                self.sio.emit(self._EV_CANDLESTICK,
                              {"event": "subscribe", "pair": sub["pair"]},
                              namespace=self.NAMESPACE)

            for sub in self._subs["trades"]:
                cb = sub.get("callback")
                if cb:
                    with self._lock:
                        self._tr_callbacks.append(cb)
                self.sio.emit(self._EV_TRADES,
                              {"event": "subscribe", "pair": sub["pair"]},
                              namespace=self.NAMESPACE)

            logger.info("CoinSwitch WS: resubscription complete")

    # ── Public interface ──────────────────────────────────────────────────────

    def connect(self, timeout: int = 30) -> bool:
        try:
            logger.info(f"CoinSwitch WS: connecting to {self.BASE_URL}...")
            self.sio.connect(
                url=self.BASE_URL,
                namespaces=[self.NAMESPACE],
                transports="websocket",
                socketio_path=self.HANDSHAKE_PATH,
                wait=True,
                wait_timeout=timeout,
            )
            return self.is_connected
        except Exception as e:
            logger.error(f"CoinSwitch WS connect error: {e}", exc_info=True)
            return False

    def disconnect(self) -> None:
        try:
            self._stop_event.set()
            if self.sio.connected:
                self.sio.disconnect()
        except Exception as e:
            logger.error(f"CoinSwitch WS disconnect error: {e}")

    def is_healthy(self, timeout_seconds: int = 35) -> bool:
        if not self.is_connected:
            return False
        if self._last_message_time is None:
            return True
        delta = (datetime.now() - self._last_message_time).total_seconds()
        return delta < timeout_seconds

    def subscribe_orderbook(self, symbol: str, callback: Callable,
                            depth: int = 20) -> None:
        with self._lock:
            if callback not in self._ob_callbacks:
                self._ob_callbacks.append(callback)
        with self._subs_lock:
            if not any(s["pair"] == symbol for s in self._subs["orderbook"]):
                self._subs["orderbook"].append({"pair": symbol, "callback": callback})
        self.sio.emit(self._EV_ORDERBOOK, {"event": "subscribe", "pair": symbol},
                      namespace=self.NAMESPACE)

    def subscribe_trades(self, symbol: str, callback: Callable) -> None:
        with self._lock:
            if callback not in self._tr_callbacks:
                self._tr_callbacks.append(callback)
        with self._subs_lock:
            if not any(s["pair"] == symbol for s in self._subs["trades"]):
                self._subs["trades"].append({"pair": symbol, "callback": callback})
        self.sio.emit(self._EV_TRADES, {"event": "subscribe", "pair": symbol},
                      namespace=self.NAMESPACE)

    def subscribe_candlestick(self, symbol: str, interval: int,
                              callback: Callable) -> None:
        pair = f"{symbol}_{interval}"
        ikey = str(interval)
        with self._lock:
            self._can_callbacks.setdefault(ikey, [])
            if callback not in self._can_callbacks[ikey]:
                self._can_callbacks[ikey].append(callback)
        with self._subs_lock:
            if not any(s["pair"] == pair for s in self._subs["candlestick"]):
                self._subs["candlestick"].append({
                    "pair": pair, "interval": interval, "callback": callback
                })
        self.sio.emit(self._EV_CANDLESTICK, {"event": "subscribe", "pair": pair},
                      namespace=self.NAMESPACE)

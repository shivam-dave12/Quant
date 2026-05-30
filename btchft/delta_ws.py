from __future__ import annotations

import hashlib
import hmac
import json
import logging
import threading
import time
from typing import Any, Callable

import websocket

LIVE_PUBLIC_WS = "wss://public-socket.india.delta.exchange"
LIVE_PRIVATE_WS = "wss://socket.india.delta.exchange"
TEST_PUBLIC_WS = "wss://socket-ind.testnet.deltaex.org"
TEST_PRIVATE_WS = "wss://socket-ind.testnet.deltaex.org"

log = logging.getLogger(__name__)


class DeltaWebSocketRuntime:
    """Thin Delta websocket runner.

    Public feed uses the new documented `ob_updates` + `trades` channels.
    Private feed subscribes to `v2/user_trades` for low-latency fill notification.
    Commission is not taken from private fills; REST reconciliation must follow.
    """

    def __init__(self, *, symbol: str, api_key: str = "", secret_key: str = "", testnet: bool = True, on_public: Callable[[dict[str, Any]], None] | None = None, on_private: Callable[[dict[str, Any]], None] | None = None) -> None:
        self.symbol = symbol.upper()
        self.api_key = api_key or ""
        self.secret_key = secret_key or ""
        self.testnet = bool(testnet)
        self.on_public = on_public or (lambda m: None)
        self.on_private = on_private or (lambda m: None)
        self.public_url = TEST_PUBLIC_WS if testnet else LIVE_PUBLIC_WS
        self.private_url = TEST_PRIVATE_WS if testnet else LIVE_PRIVATE_WS
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self._apps: list[websocket.WebSocketApp] = []

    def start(self) -> None:
        self._threads = [
            threading.Thread(target=self._run_public, name="delta-public-ws", daemon=True),
            threading.Thread(target=self._run_private, name="delta-private-ws", daemon=True),
        ]
        for t in self._threads:
            t.start()

    def stop(self) -> None:
        self._stop.set()
        for app in self._apps:
            try:
                app.close()
            except Exception:
                pass
        for t in self._threads:
            t.join(timeout=3)

    def _run_public(self) -> None:
        while not self._stop.is_set():
            def on_open(ws):
                sub = {"type": "subscribe", "payload": {"channels": [
                    {"name": "ob_updates", "symbols": [self.symbol]},
                    {"name": "trades", "symbols": [self.symbol]},
                    {"name": "funding_rate", "symbols": [self.symbol]},
                    {"name": "mark_price", "symbols": [self.symbol]},
                ]}}
                ws.send(json.dumps(sub, separators=(",", ":")))

            def on_message(ws, msg):
                try:
                    self.on_public(json.loads(msg))
                except Exception:
                    log.exception("public message handling failed")

            app = websocket.WebSocketApp(self.public_url, on_open=on_open, on_message=on_message, on_error=lambda ws, e: log.error("public ws error: %s", e))
            self._apps.append(app)
            app.run_forever(ping_interval=20, ping_timeout=10)
            if not self._stop.is_set():
                time.sleep(2)

    def _run_private(self) -> None:
        if not self.api_key or not self.secret_key:
            log.warning("No Delta credentials supplied; private fills/equity will not stream")
            return
        while not self._stop.is_set():
            def on_open(ws):
                ts = str(int(time.time()))
                method = "GET"
                path = "/live"
                sig_payload = method + ts + path
                sig = hmac.new(self.secret_key.encode(), sig_payload.encode(), hashlib.sha256).hexdigest()
                ws.send(json.dumps({"type": "auth", "payload": {"api-key": self.api_key, "signature": sig, "timestamp": ts}}, separators=(",", ":")))
                sub = {"type": "subscribe", "payload": {"channels": [
                    {"name": "v2/user_trades", "symbols": [self.symbol]},
                    {"name": "v2/user/positions", "symbols": [self.symbol]},
                    {"name": "margins"},
                ]}}
                ws.send(json.dumps(sub, separators=(",", ":")))

            def on_message(ws, msg):
                try:
                    self.on_private(json.loads(msg))
                except Exception:
                    log.exception("private message handling failed")

            app = websocket.WebSocketApp(self.private_url, on_open=on_open, on_message=on_message, on_error=lambda ws, e: log.error("private ws error: %s", e))
            self._apps.append(app)
            app.run_forever(ping_interval=20, ping_timeout=10)
            if not self._stop.is_set():
                time.sleep(2)

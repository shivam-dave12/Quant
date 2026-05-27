"""Read-only Hyperliquid reference feed for cross-venue BTC alpha.

This manager is intentionally market-data only.  It never exposes order routing
or credentials and cannot become the execution venue.  It subscribes to the
official public ``l2Book`` and ``trades`` WebSocket channels.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any, Dict

import websocket

try:
    import config
except Exception:  # pragma: no cover
    config = None  # type: ignore

from market_data.feed_health import score_feed_health
from market_data.microstructure import LatencyBaseline, MicrostructureTracker
from market_data.normalizer import InstrumentMapping, build_venue_microstate

logger = logging.getLogger(__name__)


def _cfg(name: str, default: Any) -> Any:
    return getattr(config, name, default) if config is not None else default


class HyperliquidReferenceDataManager:
    """Public BTC reference-state stream; excluded automatically when unhealthy."""

    venue = "hyperliquid"

    def __init__(self, coin: str = "BTC", *, testnet: bool | None = None) -> None:
        self.coin = str(coin or "BTC").upper()
        use_testnet = bool(_cfg("HYPERLIQUID_TESTNET", False) if testnet is None else testnet)
        self.url = "wss://api.hyperliquid-testnet.xyz/ws" if use_testnet else "wss://api.hyperliquid.xyz/ws"
        self.symbol = self.coin
        self._mapping = InstrumentMapping(
            venue="hyperliquid", venue_symbol=self.coin, canonical_underlying=self.coin,
            product_class="linear_perp", quote_currency="USD", contract_multiplier=1.0,
            settlement_currency="USDC", price_tick=0.01, qty_step=0.00001,
            execution_enabled=False, notional_model="linear",
        )
        self._tracker = MicrostructureTracker(self._mapping)
        self._latency = LatencyBaseline()
        self._lock = threading.RLock()
        self._book: dict[str, list[list[float]]] = {"bids": [], "asks": []}
        self._last_update_s = 0.0
        self._latest_latency_ms: float | None = None
        self._latest_latency_z: float | None = None
        self._ws: websocket.WebSocketApp | None = None
        self._thread: threading.Thread | None = None
        self._running = False
        self.is_streaming = False
        self.is_ready = False

    @staticmethod
    def _ns(raw: Any) -> int | None:
        try:
            value = float(raw or 0.0)
        except Exception:
            return None
        if value <= 0:
            return None
        if value > 1e17:
            return int(value)
        if value > 1e14:
            return int(value * 1_000)
        if value > 1e11:
            return int(value * 1_000_000)
        return int(value * 1_000_000_000)

    def start(self) -> bool:
        if self._running:
            return True
        self._running = True
        self._ws = websocket.WebSocketApp(
            self.url, on_open=self._on_open, on_message=self._on_message,
            on_error=self._on_error, on_close=self._on_close,
        )
        self._thread = threading.Thread(target=self._run, name=f"hl-{self.coin}-reference", daemon=True)
        self._thread.start()
        return True

    def _run(self) -> None:
        while self._running:
            try:
                assert self._ws is not None
                self._ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as exc:
                logger.warning("Hyperliquid reference websocket failed: %s", exc)
            if self._running:
                time.sleep(float(_cfg("HYPERLIQUID_RECONNECT_SEC", 3.0)))
                self._ws = websocket.WebSocketApp(
                    self.url, on_open=self._on_open, on_message=self._on_message,
                    on_error=self._on_error, on_close=self._on_close,
                )

    def stop(self) -> None:
        self._running = False
        self.is_streaming = False
        if self._ws is not None:
            self._ws.close()

    def wait_until_ready(self, timeout_sec: float = 30.0) -> bool:
        deadline = time.time() + float(timeout_sec)
        while time.time() < deadline:
            if self.is_ready:
                return True
            time.sleep(0.05)
        return False

    def _on_open(self, ws) -> None:
        self.is_streaming = True
        ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "l2Book", "coin": self.coin}}))
        ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "trades", "coin": self.coin}}))
        logger.info("Hyperliquid reference subscriptions active for %s", self.coin)

    def _on_close(self, _ws, _status, _msg) -> None:
        self.is_streaming = False
        self.is_ready = False

    def _on_error(self, _ws, error) -> None:
        logger.debug("Hyperliquid reference websocket error: %s", error)

    @staticmethod
    def _levels(raw: Any) -> list[list[float]]:
        out: list[list[float]] = []
        for row in raw or []:
            try:
                if isinstance(row, dict):
                    px = float(row.get("px") or row.get("price") or 0.0)
                    qty = float(row.get("sz") or row.get("size") or 0.0)
                else:
                    px, qty = float(row[0]), float(row[1])
                if px > 0 and qty > 0:
                    out.append([px, qty])
            except Exception:
                continue
        return out

    def _on_message(self, _ws, message: str) -> None:
        try:
            event = json.loads(message)
            channel = str(event.get("channel") or "")
            data = event.get("data") or {}
            if channel == "l2Book":
                levels = data.get("levels") or []
                bids = self._levels(levels[0] if len(levels) > 0 else [])
                asks = self._levels(levels[1] if len(levels) > 1 else [])
                now_ns = time.time_ns()
                exchange_ns = self._ns(data.get("time"))
                with self._lock:
                    self._book = {"bids": bids, "asks": asks}
                    self._last_update_s = now_ns / 1_000_000_000.0
                    self._tracker.update_book(bids, asks, self._last_update_s)
                    if exchange_ns is not None:
                        self._latest_latency_ms = max(0.0, (now_ns - exchange_ns) / 1_000_000.0)
                        self._latest_latency_z = self._latency.observe(self._latest_latency_ms)
                    self.is_ready = bool(bids and asks)
            elif channel == "trades":
                rows = data if isinstance(data, list) else [data]
                now = time.time()
                with self._lock:
                    for row in rows:
                        px = float(row.get("px") or row.get("price") or 0.0)
                        qty = float(row.get("sz") or row.get("size") or 0.0)
                        side = str(row.get("side") or "").upper()
                        if px > 0 and qty > 0:
                            self._tracker.record_trade(price=px, quantity=qty, buyer_aggressor=(side == "B"), timestamp_s=now)
        except Exception as exc:
            logger.debug("Hyperliquid reference message parse failed: %s", exc)

    def get_feed_reliability(self) -> dict[str, Any]:
        with self._lock:
            ready = bool(self.is_ready and self._book["bids"] and self._book["asks"])
            return {
                "connected": bool(self.is_streaming), "heartbeat_ok": bool(self.is_streaming),
                "sequence_valid": True, "snapshot_ready": ready,
                "exchange_timestamp_available": self._latest_latency_ms is not None,
                "latency_vs_baseline_z": self._latest_latency_z,
                "no_change_heartbeat_valid": bool(self.is_streaming and ready),
            }

    def get_venue_microstate(self):
        with self._lock:
            bids, asks = list(self._book["bids"]), list(self._book["asks"])
            flows = self._tracker.snapshot(time.time()).asdict()
            ts_ns = int((self._last_update_s or time.time()) * 1_000_000_000)
        if not bids or not asks:
            return None
        rel = self.get_feed_reliability()
        health = score_feed_health(**rel)
        return build_venue_microstate(mapping=self._mapping, bids=bids, asks=asks, feed_health=health, receive_ts_ns=ts_ns, **flows)

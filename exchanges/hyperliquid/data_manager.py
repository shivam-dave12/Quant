"""Hyperliquid public market-data feed.

It subscribes to the official public ``l2Book`` and ``trades`` WebSocket
channels.  Whether the state is executable is set explicitly by the caller;
order routing still lives in ``exchanges.hyperliquid.api`` and
``execution.order_manager``.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime, timezone
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
    """Public Hyperliquid stream; excluded automatically when unhealthy."""

    venue = "hyperliquid"

    def __init__(
        self,
        coin: str = "BTC",
        *,
        testnet: bool | None = None,
        execution_enabled: bool = False,
        instrument=None,
        canonical_underlying: str | None = None,
        price_tick: float = 0.01,
        qty_step: float = 0.00001,
    ) -> None:
        self.instrument = instrument
        raw_coin = str(coin or "BTC").strip()
        if ":" in raw_coin:
            dex, name = raw_coin.split(":", 1)
            self.coin = f"{dex.lower()}:{name.upper()}"
        else:
            self.coin = raw_coin.upper()
        use_testnet = bool(_cfg("HYPERLIQUID_TESTNET", False) if testnet is None else testnet)
        self.url = "wss://api.hyperliquid-testnet.xyz/ws" if use_testnet else "wss://api.hyperliquid.xyz/ws"
        self.symbol = self.coin
        self._mapping = InstrumentMapping(
            venue="hyperliquid",
            venue_symbol=self.coin,
            canonical_underlying=str(canonical_underlying or getattr(instrument, "asset_id", self.coin)).upper(),
            product_class="linear_perp", quote_currency="USD", contract_multiplier=1.0,
            settlement_currency="USDC", price_tick=float(price_tick or 0.01), qty_step=float(qty_step or 0.00001),
            execution_enabled=bool(execution_enabled), notional_model="linear",
        )
        self._tracker = MicrostructureTracker(self._mapping)
        self._latency = LatencyBaseline()
        self._lock = threading.RLock()
        self._book: dict[str, list[list[float]]] = {"bids": [], "asks": []}
        self._recent_trades: list[dict[str, Any]] = []
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
                            self._recent_trades.append({
                                "price": px,
                                "quantity": qty,
                                "side": "buy" if side == "B" else "sell",
                                "timestamp": now,
                                "source": "hyperliquid",
                            })
                            if len(self._recent_trades) > 500:
                                del self._recent_trades[:-500]
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

    def register_strategy(self, strategy) -> None:
        self._strategy_ref = strategy

    def get_last_price(self) -> float:
        with self._lock:
            bids, asks = self._book["bids"], self._book["asks"]
            if bids and asks:
                return (float(bids[0][0]) + float(asks[0][0])) / 2.0
        return 0.0

    def get_orderbook(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "bids": list(self._book["bids"]),
                "asks": list(self._book["asks"]),
                "timestamp": self._last_update_s or time.time(),
            }

    def get_recent_trades_raw(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._recent_trades)

    def get_last_update(self):
        with self._lock:
            ts = self._last_update_s
        return datetime.fromtimestamp(ts, tz=timezone.utc) if ts > 0 else None

    def is_price_fresh(self, max_stale_seconds: float = 90.0) -> bool:
        with self._lock:
            ts = float(self._last_update_s or 0.0)
        return ts > 0 and (time.time() - ts) <= float(max_stale_seconds)

    def get_candles(self, timeframe: str = "5m", limit: int = 100) -> list[dict[str, Any]]:
        try:
            from hyperliquid.info import Info
            from hyperliquid.utils import constants

            use_testnet = bool(_cfg("HYPERLIQUID_TESTNET", False))
            base_url = constants.TESTNET_API_URL if use_testnet else constants.MAINNET_API_URL
            dexs = list(_cfg("HYPERLIQUID_PERP_DEXS", ("", "xyz", "km")) or [""])
            info = Info(base_url, skip_ws=True, perp_dexs=dexs, timeout=float(_cfg("REQUEST_TIMEOUT", 30.0)))
            tf = str(timeframe or "5m")
            minutes = {"1m": 1, "5m": 5, "15m": 15, "1h": 60, "4h": 240, "1d": 1440}.get(tf, 5)
            end_ms = int(time.time() * 1000)
            start_ms = end_ms - max(1, int(limit)) * minutes * 60_000
            rows = info.candles_snapshot(self.coin, tf, start_ms, end_ms) or []
            out = []
            for row in rows[-max(1, int(limit)):]:
                out.append({
                    "timestamp": float(row.get("t", 0) or 0) / 1000.0,
                    "open": float(row.get("o", 0.0) or 0.0),
                    "high": float(row.get("h", 0.0) or 0.0),
                    "low": float(row.get("l", 0.0) or 0.0),
                    "close": float(row.get("c", 0.0) or 0.0),
                    "volume": float(row.get("v", 0.0) or 0.0),
                })
            return out
        except Exception as exc:
            logger.debug("Hyperliquid candles unavailable for %s %s: %s", self.coin, timeframe, exc)
            return []


class HyperliquidDataManager(HyperliquidReferenceDataManager):
    """Executable Hyperliquid market-data manager for a confirmed instrument."""

    def __init__(self, instrument=None, coin: str | None = None, **kwargs) -> None:
        ex_inst = None
        try:
            from core.instruments import ExchangeName
            ex_inst = (instrument.by_exchange or {}).get(ExchangeName.HYPERLIQUID)
        except Exception:
            ex_inst = None
        symbol = coin or getattr(ex_inst, "symbol", None) or "BTC"
        super().__init__(
            symbol,
            instrument=instrument,
            canonical_underlying=str(getattr(instrument, "asset_id", symbol)).upper(),
            execution_enabled=bool(kwargs.pop("execution_enabled", True)),
            price_tick=float(getattr(ex_inst, "tick_size", 0.01) or 0.01),
            qty_step=float(getattr(ex_inst, "lot_step", 0.00001) or 0.00001),
            **kwargs,
        )

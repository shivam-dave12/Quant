"""Hyperliquid institutional streaming market-data adapter.

Design guarantees
-----------------
* REST is used only in asynchronous startup warmup/fallback services, never in a
  strategy evaluation/tick path.
* Live order book, trades, candles and active asset context are consumed from the
  official WebSocket subscriptions and cached under a single lock.
* The same USD-normalised event tape that drives entry timing is exposed to the
  dynamic-protection engine for VPIN/Kyle/decay calibration.
* HIP-3 contract names (for example ``xyz:SILVER``) remain unchanged on the
  public-feed channel and in execution lineage.
"""
from __future__ import annotations

from collections import deque
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
    """Live Hyperliquid state cache suitable for intelligence and execution."""

    venue = "hyperliquid"
    _CANDLE_LIMITS = {"1m": 400, "5m": 300, "15m": 200, "1h": 120, "4h": 80, "1d": 45}

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
        api=None,
    ) -> None:
        self.instrument = instrument
        self.api = api
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
            venue="hyperliquid", venue_symbol=self.coin,
            canonical_underlying=str(canonical_underlying or getattr(instrument, "asset_id", self.coin)).upper(),
            product_class="linear_perp", quote_currency="USD", contract_multiplier=1.0,
            settlement_currency="USDC", price_tick=float(price_tick or 0.01), qty_step=float(qty_step or 0.00001),
            execution_enabled=bool(execution_enabled), notional_model="linear",
        )
        self._tracker = MicrostructureTracker(self._mapping)
        self._latency = LatencyBaseline()
        self._lock = threading.RLock()
        self._book: dict[str, list[list[float]]] = {"bids": [], "asks": []}
        self._recent_trades: deque[dict[str, Any]] = deque(maxlen=1000)
        self._candles: dict[str, deque[dict[str, Any]]] = {
            tf: deque(maxlen=limit) for tf, limit in self._CANDLE_LIMITS.items()
        }
        self._last_update_s = 0.0
        self._latest_latency_ms: float | None = None
        self._latest_latency_z: float | None = None
        self._funding_rate: float | None = None
        self._mark_price: float | None = None
        self._open_interest: float | None = None
        self._asset_context_ts_s = 0.0
        self._ws: websocket.WebSocketApp | None = None
        self._thread: threading.Thread | None = None
        self._warmup_thread: threading.Thread | None = None
        self._warmup_started = False
        self._warmup_complete = False
        self._strategy_ref = None
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
        self._thread = threading.Thread(target=self._run, name=f"hl-{self.coin}-stream", daemon=True)
        self._thread.start()
        self._start_warmup_async()
        return True

    def _start_warmup_async(self) -> None:
        with self._lock:
            if self._warmup_started:
                return
            self._warmup_started = True
        self._warmup_thread = threading.Thread(
            target=self._warmup_candles_once, name=f"hl-{self.coin}-warmup", daemon=True
        )
        self._warmup_thread.start()

    def _run(self) -> None:
        while self._running:
            try:
                assert self._ws is not None
                self._ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as exc:
                logger.warning("Hyperliquid websocket failed %s: %s", self.coin, exc)
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

    def restart_streams(self) -> bool:
        self.stop()
        time.sleep(0.2)
        with self._lock:
            self._warmup_started = False
        return self.start()

    def wait_until_ready(self, timeout_sec: float = 30.0) -> bool:
        deadline = time.time() + float(timeout_sec)
        while time.time() < deadline:
            if self.is_ready:
                return True
            time.sleep(0.05)
        return False

    def _on_open(self, ws) -> None:
        self.is_streaming = True
        subscriptions = [
            {"type": "l2Book", "coin": self.coin},
            {"type": "trades", "coin": self.coin},
            {"type": "activeAssetCtx", "coin": self.coin},
        ]
        intervals = tuple(_cfg("HYPERLIQUID_WS_CANDLE_INTERVALS", ("1m", "5m", "15m", "1h", "4h", "1d")))
        subscriptions.extend({"type": "candle", "coin": self.coin, "interval": tf} for tf in intervals)
        for subscription in subscriptions:
            ws.send(json.dumps({"method": "subscribe", "subscription": subscription}))
        logger.info(
            "Hyperliquid live subscriptions active coin=%s feeds=l2Book,trades,activeAssetCtx,candle[%s]",
            self.coin, ",".join(str(tf) for tf in intervals),
        )

    def _on_close(self, _ws, _status, _msg) -> None:
        self.is_streaming = False
        self.is_ready = False

    def _on_error(self, _ws, error) -> None:
        logger.debug("Hyperliquid websocket error %s: %s", self.coin, error)

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

    @staticmethod
    def _normalise_candle(row: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
        try:
            tf = str(row.get("i") or row.get("interval") or "")
            ts_ms = int(float(row.get("t") or row.get("timestamp") or 0))
            if tf not in HyperliquidReferenceDataManager._CANDLE_LIMITS or ts_ms <= 0:
                return None
            candle = {
                "timestamp": ts_ms / 1000.0,
                "t": ts_ms,
                "open": float(row.get("o") or row.get("open") or 0.0),
                "high": float(row.get("h") or row.get("high") or 0.0),
                "low": float(row.get("l") or row.get("low") or 0.0),
                "close": float(row.get("c") or row.get("close") or 0.0),
                "volume": float(row.get("v") or row.get("volume") or 0.0),
            }
            if candle["close"] <= 0:
                return None
            return tf, candle
        except Exception:
            return None

    def _upsert_candle(self, tf: str, candle: dict[str, Any]) -> None:
        target = self._candles[tf]
        ts = float(candle["timestamp"])
        if target and float(target[-1].get("timestamp", 0.0)) == ts:
            target[-1] = candle
        elif not target or ts > float(target[-1].get("timestamp", 0.0)):
            target.append(candle)
        else:
            rows = {float(row.get("timestamp", 0.0)): row for row in target}
            rows[ts] = candle
            target.clear()
            target.extend(rows[key] for key in sorted(rows)[-target.maxlen:])

    def _notify_quote(self, price: float) -> None:
        callback = getattr(self._strategy_ref, "_on_realtime_quote", None) if self._strategy_ref is not None else None
        if callable(callback) and price > 0:
            callback(price)

    def _notify_trade(self, price: float, quantity: float, side: str) -> None:
        callback = getattr(self._strategy_ref, "_on_realtime_trade", None) if self._strategy_ref is not None else None
        if callable(callback) and price > 0:
            callback(price, quantity, side)

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
                quote_price = 0.0
                with self._lock:
                    self._book = {"bids": bids, "asks": asks}
                    self._last_update_s = now_ns / 1_000_000_000.0
                    self._tracker.update_book(bids, asks, self._last_update_s)
                    if exchange_ns is not None:
                        self._latest_latency_ms = max(0.0, (now_ns - exchange_ns) / 1_000_000.0)
                        self._latest_latency_z = self._latency.observe(self._latest_latency_ms)
                    self.is_ready = bool(bids and asks)
                    if self.is_ready:
                        quote_price = (bids[0][0] + asks[0][0]) / 2.0
                self._notify_quote(quote_price)
            elif channel == "trades":
                rows = data if isinstance(data, list) else [data]
                now = time.time()
                callbacks: list[tuple[float, float, str]] = []
                with self._lock:
                    for row in rows:
                        px = float(row.get("px") or row.get("price") or 0.0)
                        qty = float(row.get("sz") or row.get("size") or 0.0)
                        side = "buy" if str(row.get("side") or "").upper() == "B" else "sell"
                        if px > 0 and qty > 0:
                            self._tracker.record_trade(price=px, quantity=qty, buyer_aggressor=(side == "buy"), timestamp_s=now)
                            self._recent_trades.append({"price": px, "quantity": qty, "side": side, "timestamp": now, "source": "hyperliquid"})
                            callbacks.append((px, qty, side))
                for px, qty, side in callbacks:
                    self._notify_trade(px, qty, side)
            elif channel == "candle":
                rows = data if isinstance(data, list) else [data]
                last_close = 0.0
                with self._lock:
                    for row in rows:
                        normalised = self._normalise_candle(row)
                        if normalised:
                            tf, candle = normalised
                            self._upsert_candle(tf, candle)
                            last_close = candle["close"]
                if last_close > 0:
                    self._notify_quote(last_close)
            elif channel == "activeAssetCtx":
                ctx = data.get("ctx", data) if isinstance(data, dict) else {}
                with self._lock:
                    if isinstance(ctx, dict):
                        if ctx.get("funding") is not None:
                            self._funding_rate = float(ctx.get("funding"))
                        if ctx.get("markPx") is not None:
                            self._mark_price = float(ctx.get("markPx"))
                        if ctx.get("openInterest") is not None:
                            self._open_interest = float(ctx.get("openInterest"))
                        self._asset_context_ts_s = time.time()
        except Exception as exc:
            logger.debug("Hyperliquid message parse failed %s: %s", self.coin, exc)

    def _warmup_candles_once(self) -> None:
        """REST bootstrap outside market evaluation; WS owns all live updates."""
        try:
            from hyperliquid.info import Info
            from hyperliquid.utils import constants
            use_testnet = bool(_cfg("HYPERLIQUID_TESTNET", False))
            base_url = constants.TESTNET_API_URL if use_testnet else constants.MAINNET_API_URL
            dexs = list(_cfg("HYPERLIQUID_PERP_DEXS", ("", "xyz", "km")) or [""])
            info = Info(base_url, skip_ws=True, perp_dexs=dexs, timeout=float(_cfg("REQUEST_TIMEOUT", 30.0)))
            minutes_map = {"1m": 1, "5m": 5, "15m": 15, "1h": 60, "4h": 240, "1d": 1440}
            for tf, maxlen in self._CANDLE_LIMITS.items():
                end_ms = int(time.time() * 1000)
                start_ms = end_ms - int(maxlen) * minutes_map[tf] * 60_000
                rows = info.candles_snapshot(self.coin, tf, start_ms, end_ms) or []
                with self._lock:
                    for raw in rows:
                        normalised = self._normalise_candle(dict(raw))
                        if normalised:
                            _, candle = normalised
                            self._upsert_candle(tf, candle)
            try:
                context = self.api.current_asset_context(self.coin) if self.api is not None else {}
                with self._lock:
                    if isinstance(context, dict) and context.get("funding") is not None and self._funding_rate is None:
                        self._funding_rate = float(context.get("funding"))
            except Exception:
                pass
            with self._lock:
                self._warmup_complete = True
            logger.info("✅ Hyperliquid asynchronous candle warmup complete coin=%s counts=%s", self.coin, {tf: len(rows) for tf, rows in self._candles.items()})
        except Exception as exc:
            logger.warning("Hyperliquid asynchronous candle warmup failed coin=%s: %s", self.coin, exc)

    def get_feed_reliability(self) -> dict[str, Any]:
        with self._lock:
            ready = bool(self.is_ready and self._book["bids"] and self._book["asks"])
            return {
                "connected": bool(self.is_streaming), "heartbeat_ok": bool(self.is_streaming),
                "sequence_valid": True, "snapshot_ready": ready,
                "exchange_timestamp_available": self._latest_latency_ms is not None,
                "latency_vs_baseline_z": self._latest_latency_z,
                "no_change_heartbeat_valid": bool(self.is_streaming and ready),
                "latency_ms": self._latest_latency_ms,
                "candle_cache_ready": bool(len(self._candles["1m"]) >= 20),
                "active_asset_context_ready": self._asset_context_ts_s > 0,
            }

    def get_venue_microstate(self):
        with self._lock:
            bids, asks = list(self._book["bids"]), list(self._book["asks"])
            flows = self._tracker.snapshot(time.time()).asdict()
            ts_ns = int((self._last_update_s or time.time()) * 1_000_000_000)
            funding = self._funding_rate
            metadata = {"mark_price": self._mark_price, "open_interest": self._open_interest, "market_context_source": "hyperliquid_ws_activeAssetCtx"}
        if not bids or not asks:
            return None
        rel = self.get_feed_reliability()
        health = score_feed_health(**{key: rel[key] for key in ("connected", "heartbeat_ok", "sequence_valid", "snapshot_ready", "exchange_timestamp_available", "latency_vs_baseline_z", "no_change_heartbeat_valid")})
        return build_venue_microstate(mapping=self._mapping, bids=bids, asks=asks, feed_health=health, receive_ts_ns=ts_ns, funding_rate=funding, metadata=metadata, **flows)

    def register_strategy(self, strategy) -> None:
        self._strategy_ref = strategy

    def get_last_price(self) -> float:
        with self._lock:
            bids, asks = self._book["bids"], self._book["asks"]
            if bids and asks:
                return (float(bids[0][0]) + float(asks[0][0])) / 2.0
            return float(self._mark_price or 0.0)

    def get_orderbook(self) -> Dict[str, Any]:
        with self._lock:
            return {"bids": list(self._book["bids"]), "asks": list(self._book["asks"]), "timestamp": self._last_update_s or time.time()}

    def get_recent_trades_raw(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._recent_trades)

    def get_microstructure_research_state(self) -> Dict[str, list[dict[str, float]]]:
        with self._lock:
            return self._tracker.research_state(time.time())

    def get_last_update(self):
        with self._lock:
            ts = self._last_update_s
        return datetime.fromtimestamp(ts, tz=timezone.utc) if ts > 0 else None

    def is_price_fresh(self, max_stale_seconds: float = 90.0) -> bool:
        with self._lock:
            ts = float(self._last_update_s or 0.0)
        return ts > 0 and (time.time() - ts) <= float(max_stale_seconds)

    def get_candles(self, timeframe: str = "5m", limit: int = 100) -> list[dict[str, Any]]:
        """Return stream-backed cache only; never performs live-path HTTP I/O."""
        tf = str(timeframe or "5m")
        with self._lock:
            rows = list(self._candles.get(tf, self._candles["5m"]))
        return [dict(row) for row in rows[-max(1, int(limit)):]]


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
            symbol, instrument=instrument,
            canonical_underlying=str(getattr(instrument, "asset_id", symbol)).upper(),
            execution_enabled=bool(kwargs.pop("execution_enabled", True)),
            price_tick=float(getattr(ex_inst, "tick_size", 0.01) or 0.01),
            qty_step=float(getattr(ex_inst, "lot_step", 0.00001) or 0.00001),
            **kwargs,
        )

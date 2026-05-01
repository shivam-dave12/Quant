"""CoinSwitch Pro Futures data manager.

Maintains canonical candle/orderbook/trade streams with the same public
interface as DeltaDataManager. CoinSwitch quantities are already BTC/base units;
no contract conversion is performed here.
"""
from __future__ import annotations

import logging
import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Dict, List, Optional

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import config
from core.candle import Candle
from exchanges.coinswitch.api import FuturesAPI
from exchanges.coinswitch.websocket import CoinSwitchWebSocket

logger = logging.getLogger(__name__)


class StreamStats:
    def __init__(self) -> None:
        self._last_update: Optional[datetime] = None
        self._ob_count = 0
        self._trade_count = 0
        self._candle_count = 0
        self._lock = threading.RLock()

    def record_orderbook(self) -> None:
        with self._lock:
            self._ob_count += 1
            self._last_update = datetime.now(timezone.utc)

    def record_trade(self) -> None:
        with self._lock:
            self._trade_count += 1
            self._last_update = datetime.now(timezone.utc)

    def record_candle(self) -> None:
        with self._lock:
            self._candle_count += 1
            self._last_update = datetime.now(timezone.utc)

    def get_last_update(self) -> Optional[datetime]:
        with self._lock:
            return self._last_update


class CoinSwitchDataManager:
    _WARMUP_CONFIG = {
        "1m": ("1", 1, 200, "_candles_1m"),
        "5m": ("5", 5, 200, "_candles_5m"),
        "15m": ("15", 15, 200, "_candles_15m"),
        "1h": ("60", 60, 100, "_candles_1h"),
        "4h": ("240", 240, 50, "_candles_4h"),
        "1d": ("1440", 1440, 30, "_candles_1d"),
    }
    _WARMUP_SLEEP = 3.5

    def __init__(self) -> None:
        self.api = FuturesAPI(config.COINSWITCH_API_KEY, config.COINSWITCH_SECRET_KEY)
        self.ws: Optional[CoinSwitchWebSocket] = None
        self.stats = StreamStats()
        self._candles_1m: deque = deque(maxlen=2000)
        self._candles_5m: deque = deque(maxlen=1200)
        self._candles_15m: deque = deque(maxlen=800)
        self._candles_1h: deque = deque(maxlen=500)
        self._candles_4h: deque = deque(maxlen=400)
        self._candles_1d: deque = deque(maxlen=100)
        self._last_price = 0.0
        self._last_price_update_time = 0.0
        self._orderbook: Dict = {"bids": [], "asks": []}
        self._recent_trades: deque = deque(maxlen=500)
        self._lock = threading.RLock()
        self._forming_ts: Dict[str, int] = {}
        self._warmup_complete = False
        self._strategy_ref = None
        self.is_ready = False
        self.is_streaming = False
        logger.info("CoinSwitchDataManager initialised")

    def start(self) -> bool:
        try:
            self.is_ready = False
            self.is_streaming = False
            symbol = config.COINSWITCH_SYMBOL
            self.ws = CoinSwitchWebSocket()
            self.ws.subscribe_orderbook(symbol, self._on_orderbook)
            self.ws.subscribe_trades(symbol, self._on_trade)
            for tf, (_, minutes, _, _) in self._WARMUP_CONFIG.items():
                self.ws.subscribe_candlestick(symbol, interval=minutes, callback=self._make_candle_cb(tf))
            if not self.ws.connect(timeout=30):
                logger.error("CoinSwitch WS failed to connect")
                return False
            self.is_streaming = True
            for tf in ("1m", "5m", "15m", "1h", "4h", "1d"):
                self._warmup_klines(tf)
                time.sleep(self._WARMUP_SLEEP)
            with self._lock:
                for key, deq in (("1", self._candles_1m), ("5", self._candles_5m), ("15", self._candles_15m), ("60", self._candles_1h), ("240", self._candles_4h), ("1440", self._candles_1d)):
                    if deq:
                        self._forming_ts[key] = int(deq[-1].timestamp * 1000)
            self._warmup_complete = True
            self.is_ready = self._check_minimum_data()
            logger.info("CoinSwitch DM ready=%s", self.is_ready)
            return True
        except Exception as exc:
            logger.error("CoinSwitch DM start error: %s", exc, exc_info=True)
            self.is_ready = self.is_streaming = False
            return False

    def stop(self) -> None:
        self.is_ready = False
        self.is_streaming = False
        if self.ws:
            self.ws.disconnect()

    def restart_streams(self) -> bool:
        self._warmup_complete = False
        with self._lock:
            self._forming_ts.clear()
            self._recent_trades.clear()
        self.stop()
        time.sleep(2.0)
        ok = self.start()
        if ok and self._strategy_ref is not None:
            try: self._strategy_ref.on_stream_restart()
            except Exception: pass
        return ok

    def register_strategy(self, strategy) -> None:
        self._strategy_ref = strategy

    def wait_until_ready(self, timeout_sec: float = 120.0) -> bool:
        start = time.time()
        while time.time() - start < timeout_sec:
            if self.is_ready or self._check_minimum_data():
                self.is_ready = True
                return True
            time.sleep(1.0)
        return False

    def _warmup_klines(self, label: str, limit: int = 0, retries: int = 2) -> None:
        cfg = self._WARMUP_CONFIG[label]
        interval_str, minutes, default_limit, attr = cfg
        limit = limit or default_limit
        target: deque = getattr(self, attr)
        for attempt in range(1, retries + 2):
            try:
                now_ms = int(time.time() * 1000)
                resp = self.api._make_request("GET", "/trade/api/v2/futures/klines", params={
                    "symbol": config.COINSWITCH_SYMBOL,
                    "exchange": config.COINSWITCH_EXCHANGE,
                    "interval": interval_str,
                    "start_time": now_ms - limit * minutes * 60 * 1000,
                    "end_time": now_ms,
                    "limit": limit,
                })
                if not isinstance(resp, dict) or resp.get("error"):
                    if attempt <= retries: time.sleep(self._WARMUP_SLEEP)
                    continue
                raw = resp.get("data", []) or []
                seeded = 0
                with self._lock:
                    target.clear()
                    for item in sorted(raw, key=lambda x: int(x.get("close_time") or x.get("start_time") or x.get("t") or 0)):
                        try:
                            ts = float(item.get("close_time") or item.get("start_time") or item.get("t") or 0)
                            candle = Candle(
                                timestamp=ts / 1000.0 if ts > 1e12 else ts,
                                open=float(item.get("o") or item.get("open") or 0),
                                high=float(item.get("h") or item.get("high") or 0),
                                low=float(item.get("l") or item.get("low") or 0),
                                close=float(item.get("c") or item.get("close") or 0),
                                volume=float(item.get("v") or item.get("volume") or 0),
                            )
                            if candle.close > 0:
                                target.append(candle)
                                if label == "1m":
                                    self._last_price = candle.close
                                    self._last_price_update_time = time.time()
                                seeded += 1
                        except Exception:
                            continue
                if seeded:
                    logger.info("CoinSwitch warmup %s: %d candles", label, seeded)
                    return
            except Exception as exc:
                logger.warning("CoinSwitch warmup %s attempt %s: %s", label, attempt, exc)
                if attempt <= retries: time.sleep(self._WARMUP_SLEEP)

    def _process_ws_candle(self, data: Dict, candle: Candle, target: deque, tf_key: str) -> None:
        is_closed = bool(data.get("x", False))
        start_ts = int(data.get("t", 0))
        forming_ts = self._forming_ts.get(tf_key)
        self._last_price = candle.close
        self._last_price_update_time = time.time()
        if is_closed:
            if forming_ts == start_ts and target:
                target[-1] = candle
            else:
                target.append(candle)
            self._forming_ts.pop(tf_key, None)
        else:
            if forming_ts == start_ts and target:
                target[-1] = candle
            else:
                target.append(candle)
                self._forming_ts[tf_key] = start_ts
        self.stats.record_candle()

    def _make_candle_cb(self, label: str):
        mapping = {"1m": ("1", self._candles_1m), "5m": ("5", self._candles_5m), "15m": ("15", self._candles_15m), "1h": ("60", self._candles_1h), "4h": ("240", self._candles_4h), "1d": ("1440", self._candles_1d)}
        tf_key, target = mapping[label]
        def cb(data: Dict) -> None:
            try:
                if str(data.get("i", tf_key)) != tf_key:
                    return
                candle = Candle(timestamp=float(data["t"]) / 1000.0, open=float(data["o"]), high=float(data["h"]), low=float(data["l"]), close=float(data["c"]), volume=float(data.get("v", 0)))
                if candle.close <= 0: return
                with self._lock:
                    self._process_ws_candle(data, candle, target, tf_key)
            except Exception as exc:
                logger.error("CoinSwitch %s candle callback error: %s", label, exc)
        return cb

    def _on_orderbook(self, data: Dict) -> None:
        try:
            with self._lock:
                bids = list(data.get("bids", []))
                asks = list(data.get("asks", []))
                self._orderbook = {"bids": bids, "asks": asks, "timestamp": time.time()}
                if bids and asks:
                    self._last_price = (float(bids[0][0]) + float(asks[0][0])) / 2.0
                    self._last_price_update_time = time.time()
                self.stats.record_orderbook()
        except Exception as exc:
            logger.debug("CoinSwitch OB callback error: %s", exc)

    def _on_trade(self, data: Dict) -> None:
        callback = None
        price = qty = 0.0
        side = "buy"
        try:
            with self._lock:
                price = float(data.get("price", 0) or 0)
                qty = float(data.get("quantity", 0) or 0)
                side = str(data.get("side") or "buy").lower()
                if price <= 0: return
                if side not in ("buy", "sell"):
                    side = "buy"
                self._last_price = price
                self._last_price_update_time = time.time()
                tick = {"price": price, "quantity": max(qty, 0.0), "side": side, "timestamp": time.time()}
                self._recent_trades.append(tick)
                callback = getattr(self._strategy_ref, "_on_realtime_trade", None) if self._strategy_ref is not None else None
                self.stats.record_trade()
            if callback:
                callback(price, max(qty, 0.0), side)
        except Exception as exc:
            logger.debug("CoinSwitch trade callback error: %s", exc)

    def _check_minimum_data(self) -> bool:
        counts = {"1m": len(self._candles_1m), "5m": len(self._candles_5m), "15m": len(self._candles_15m), "1h": len(self._candles_1h), "4h": len(self._candles_4h), "1d": len(self._candles_1d)}
        mins = {"1m": getattr(config, "MIN_CANDLES_1M", 100), "5m": getattr(config, "MIN_CANDLES_5M", 100), "15m": getattr(config, "MIN_CANDLES_15M", 100), "1h": getattr(config, "MIN_CANDLES_1H", 20), "4h": max(getattr(config, "MIN_CANDLES_4H", 40), 29), "1d": getattr(config, "MIN_CANDLES_1D", 7)}
        return all(counts[k] >= mins[k] for k in mins)

    def get_last_price(self) -> float:
        with self._lock: return self._last_price

    def get_orderbook(self) -> Dict:
        with self._lock:
            return {"bids": list(self._orderbook.get("bids", [])), "asks": list(self._orderbook.get("asks", [])), "timestamp": time.time()}

    def get_recent_trades_raw(self) -> List[Dict]:
        with self._lock: return list(self._recent_trades)[-200:]

    def is_price_fresh(self, max_stale_seconds: float = 90.0) -> bool:
        return self._last_price_update_time > 0 and time.time() - self._last_price_update_time < max_stale_seconds

    def get_candles(self, timeframe: str = "5m", limit: int = 100) -> List[Dict]:
        tf_map = {"1m": self._candles_1m, "5m": self._candles_5m, "15m": self._candles_15m, "1h": self._candles_1h, "4h": self._candles_4h, "1d": self._candles_1d}
        with self._lock:
            candles = list(tf_map.get(timeframe, self._candles_5m))[-limit:]
        return [{"t": int(c.timestamp * 1000), "o": c.open, "h": c.high, "l": c.low, "c": c.close, "v": c.volume} for c in candles]

    def get_volume_delta(self, lookback_seconds: float = 60.0) -> Dict:
        cutoff = time.time() - lookback_seconds
        with self._lock:
            buy = sum(t["quantity"] for t in self._recent_trades if t["timestamp"] >= cutoff and t["side"] == "buy")
            sell = sum(t["quantity"] for t in self._recent_trades if t["timestamp"] >= cutoff and t["side"] == "sell")
        total = buy + sell
        return {"buy_volume": buy, "sell_volume": sell, "delta": buy - sell, "delta_pct": (buy - sell) / total if total > 0 else 0.0}

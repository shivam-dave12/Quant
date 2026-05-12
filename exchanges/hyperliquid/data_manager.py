"""Polling Hyperliquid data manager for institutional primary routing."""
from __future__ import annotations

import logging
import threading
import time
from collections import deque
from typing import Dict, List, Optional

import config
from core.candle import Candle
from core.instruments import ExchangeName
from exchanges.hyperliquid.api import HyperliquidAPI, _float

logger = logging.getLogger(__name__)


class HyperliquidDataManager:
    _WARMUP_CONFIG = {
        "1m": ("1m", 1, 200, "_candles_1m"),
        "5m": ("5m", 5, 200, "_candles_5m"),
        "15m": ("15m", 15, 200, "_candles_15m"),
        "1h": ("1h", 60, 100, "_candles_1h"),
        "4h": ("4h", 240, 50, "_candles_4h"),
        "1d": ("1d", 1440, 30, "_candles_1d"),
    }

    def __init__(self, instrument=None, api: Optional[HyperliquidAPI] = None) -> None:
        self.instrument = instrument
        self.exchange_instrument = (
            instrument.by_exchange.get(ExchangeName.HYPERLIQUID)
            if instrument is not None and hasattr(instrument, "by_exchange") else None
        )
        self.symbol = (
            self.exchange_instrument.symbol
            if self.exchange_instrument is not None else getattr(config, "HYPERLIQUID_SYMBOL", "BTC")
        )
        self.api = api or HyperliquidAPI()
        self._candles_1m: deque = deque(maxlen=2000)
        self._candles_5m: deque = deque(maxlen=1200)
        self._candles_15m: deque = deque(maxlen=800)
        self._candles_1h: deque = deque(maxlen=500)
        self._candles_4h: deque = deque(maxlen=400)
        self._candles_1d: deque = deque(maxlen=100)
        self._orderbook: Dict = {"bids": [], "asks": []}
        self._recent_trades: deque = deque(maxlen=500)
        self._last_price = 0.0
        self._last_price_update_time = 0.0
        self._strategy_ref = None
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.is_ready = False
        self.is_streaming = False

    def start(self) -> bool:
        try:
            self._stop.clear()
            for tf in ("1m", "5m", "15m", "1h", "4h", "1d"):
                self._warmup(tf)
            self._refresh_market()
            self.is_ready = self._check_minimum_data()
            if not self.is_ready:
                return False
            self.is_streaming = True
            self._thread = threading.Thread(target=self._poll_loop, name=f"hl-data-{self.symbol}", daemon=True)
            self._thread.start()
            logger.info("Hyperliquid DM[%s] ready", self.symbol)
            return True
        except Exception as exc:
            logger.error("Hyperliquid DM[%s] start failed: %s", self.symbol, exc, exc_info=True)
            self.is_ready = self.is_streaming = False
            return False

    def stop(self) -> None:
        self._stop.set()
        self.is_streaming = False

    def restart_streams(self) -> bool:
        self.stop()
        time.sleep(1.0)
        return self.start()

    def register_strategy(self, strategy) -> None:
        self._strategy_ref = strategy

    def wait_until_ready(self, timeout_sec: float = 120.0) -> bool:
        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            if self.is_ready:
                return True
            self.is_ready = self._check_minimum_data()
            time.sleep(0.5)
        return self.is_ready

    def _warmup(self, label: str) -> None:
        cfg = self._WARMUP_CONFIG.get(label)
        if not cfg:
            return
        interval, minutes, default_limit, attr = cfg
        end_ms = int(time.time() * 1000)
        start_ms = end_ms - int(default_limit * minutes * 60 * 1000)
        target: deque = getattr(self, attr)
        rows = self.api.get_candles(self.symbol, interval, start_ms, end_ms)
        seeded = 0
        for row in sorted(rows, key=lambda x: int(x.get("t", 0) or 0)):
            try:
                c = Candle(
                    timestamp=float(row.get("t", 0) or 0) / 1000.0,
                    open=_float(row.get("o")),
                    high=_float(row.get("h")),
                    low=_float(row.get("l")),
                    close=_float(row.get("c")),
                    volume=_float(row.get("v")),
                )
                if c.close > 0:
                    target.append(c)
                    seeded += 1
                    if label == "1m":
                        self._last_price = c.close
                        self._last_price_update_time = time.time()
            except Exception:
                continue
        logger.info("Hyperliquid DM[%s] warmup %s candles=%d", self.symbol, label, seeded)

    def _poll_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._refresh_market()
                self._refresh_last_1m()
                self.is_ready = self._check_minimum_data()
            except Exception as exc:
                logger.debug("Hyperliquid DM[%s] poll failed: %s", self.symbol, exc)
            self._stop.wait(float(getattr(config, "HYPERLIQUID_POLL_SEC", 2.0) or 2.0))

    def _refresh_market(self) -> None:
        book = self.api.get_l2_book(self.symbol)
        levels = book.get("levels", []) if isinstance(book, dict) else []
        bids = levels[0] if len(levels) > 0 and isinstance(levels[0], list) else []
        asks = levels[1] if len(levels) > 1 and isinstance(levels[1], list) else []
        bids_out = [[_float(x.get("px")), _float(x.get("sz"))] for x in bids if isinstance(x, dict)]
        asks_out = [[_float(x.get("px")), _float(x.get("sz"))] for x in asks if isinstance(x, dict)]
        mid = 0.0
        if bids_out and asks_out:
            mid = (bids_out[0][0] + asks_out[0][0]) / 2.0
        if mid <= 0:
            mids = self.api.all_mids()
            mid = _float(mids.get(self.symbol))
        with self._lock:
            self._orderbook = {"bids": bids_out, "asks": asks_out}
            if mid > 0:
                self._last_price = mid
                self._last_price_update_time = time.time()

    def _refresh_last_1m(self) -> None:
        end_ms = int(time.time() * 1000)
        rows = self.api.get_candles(self.symbol, "1m", end_ms - 5 * 60_000, end_ms)
        if not rows:
            return
        with self._lock:
            seen = {int(c.timestamp * 1000) for c in self._candles_1m}
            for row in rows:
                ts = int(row.get("t", 0) or 0)
                if ts in seen:
                    continue
                c = Candle(ts / 1000.0, _float(row.get("o")), _float(row.get("h")), _float(row.get("l")), _float(row.get("c")), _float(row.get("v")))
                if c.close > 0:
                    self._candles_1m.append(c)
                    self._last_price = c.close
                    self._last_price_update_time = time.time()

    def _check_minimum_data(self) -> bool:
        mins = {
            "1m": getattr(config, "MIN_CANDLES_1M", 100),
            "5m": getattr(config, "MIN_CANDLES_5M", 100),
            "15m": getattr(config, "MIN_CANDLES_15M", 100),
            "1h": getattr(config, "MIN_CANDLES_1H", 20),
            "4h": max(getattr(config, "MIN_CANDLES_4H", 40), 29),
            "1d": getattr(config, "MIN_CANDLES_1D", 7),
        }
        counts = {
            "1m": len(self._candles_1m),
            "5m": len(self._candles_5m),
            "15m": len(self._candles_15m),
            "1h": len(self._candles_1h),
            "4h": len(self._candles_4h),
            "1d": len(self._candles_1d),
        }
        return all(counts[k] >= mins[k] for k in mins)

    def get_last_price(self) -> float:
        with self._lock:
            return float(self._last_price or 0.0)

    def get_orderbook(self) -> Dict:
        with self._lock:
            return {"bids": list(self._orderbook.get("bids", [])), "asks": list(self._orderbook.get("asks", [])), "timestamp": time.time()}

    def get_recent_trades_raw(self) -> List[Dict]:
        with self._lock:
            return list(self._recent_trades)[-200:]

    def is_price_fresh(self, max_stale_seconds: float = 90.0) -> bool:
        return self._last_price_update_time <= 0 or (time.time() - self._last_price_update_time) < max_stale_seconds

    def get_candles(self, timeframe: str = "5m", limit: int = 100) -> List[Dict]:
        tf_map = {
            "1m": self._candles_1m,
            "5m": self._candles_5m,
            "15m": self._candles_15m,
            "1h": self._candles_1h,
            "4h": self._candles_4h,
            "1d": self._candles_1d,
        }
        src = tf_map.get(timeframe, self._candles_5m)
        with self._lock:
            candles = list(src)
        return [{"t": int(c.timestamp * 1000), "o": c.open, "h": c.high, "l": c.low, "c": c.close, "v": c.volume} for c in candles[-limit:]]

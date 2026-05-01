"""
exchanges/coinswitch/data_manager.py — CoinSwitch Data Manager
==============================================================
Implements BaseDataManager for CoinSwitch Pro Futures.

Pattern: WS connect → subscribe → REST warmup → ready
Candles from all 6 timeframes; orderbook + trades for microstructure.
Strategy interface is identical to DeltaDataManager — swap is transparent.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Dict, List, Optional

import sys, os; sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import config
from core.candle import Candle, wrap_candles
from exchanges.coinswitch.api import FuturesAPI
from exchanges.coinswitch.websocket import CoinSwitchWebSocket

logger = logging.getLogger(__name__)


class StreamStats:
    def __init__(self) -> None:
        self._last_update: Optional[datetime] = None
        self._ob_count = self._trade_count = self._candle_count = 0
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
    """
    CoinSwitch data manager.
    Provides: candles (6 TFs), orderbook, recent trades, price.
    Same public interface as DeltaDataManager.
    """

    _WARMUP_CONFIG = {
        "1m":  ("1",    1,    200, "_candles_1m"),
        "5m":  ("5",    5,    200, "_candles_5m"),
        "15m": ("15",   15,   200, "_candles_15m"),
        "1h":  ("60",   60,   100, "_candles_1h"),
        "4h":  ("240",  240,   50, "_candles_4h"),
        "1d":  ("1440", 1440,  30, "_candles_1d"),
    }

    # CoinSwitch hard rate limit between REST calls
    _WARMUP_SLEEP = 3.5

    def __init__(self) -> None:
        self.api = FuturesAPI(
            api_key    = config.COINSWITCH_API_KEY,
            secret_key = config.COINSWITCH_SECRET_KEY,
        )
        self.ws:    Optional[CoinSwitchWebSocket] = None
        self.stats  = StreamStats()

        self._candles_1m:  deque = deque(maxlen=2000)
        self._candles_5m:  deque = deque(maxlen=1200)
        self._candles_15m: deque = deque(maxlen=800)
        self._candles_1h:  deque = deque(maxlen=500)
        self._candles_4h:  deque = deque(maxlen=400)
        self._candles_1d:  deque = deque(maxlen=100)

        self._last_price:             float = 0.0
        self._last_price_update_time: float = 0.0
        self._orderbook:              Dict  = {"bids": [], "asks": []}
        self._recent_trades:          deque = deque(maxlen=500)

        self._lock         = threading.RLock()
        self._forming_ts:  Dict[str, int] = {}
        self._warmup_complete = False

        self._strategy_ref = None
        self.is_ready      = False
        self.is_streaming  = False

        logger.info("CoinSwitchDataManager initialised")

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> bool:
        try:
            self.is_ready = self.is_streaming = False
            symbol = config.COINSWITCH_SYMBOL

            logger.info("CoinSwitch DM: starting WebSocket...")
            self.ws = CoinSwitchWebSocket()

            if not self.ws.connect(timeout=30):
                logger.error("❌ CoinSwitch WS failed to connect")
                return False

            # Subscribe all streams
            self.ws.subscribe_orderbook(symbol, callback=self._on_orderbook)
            self.ws.subscribe_trades(symbol, callback=self._on_trade)
            for interval, (istr, _, _, _) in self._WARMUP_CONFIG.items():
                iv_int = {"1m": 1, "5m": 5, "15m": 15, "1h": 60,
                          "4h": 240, "1d": 1440}[interval]
                attr = f"_on_candle_{interval.replace('m','m').replace('h','h').replace('d','d')}"
                cb = getattr(self, f"_make_candle_cb")(interval)
                self.ws.subscribe_candlestick(symbol, interval=iv_int, callback=cb)

            self.is_streaming = True
            logger.info("✅ CoinSwitch WS streams subscribed")

            # REST warmup (rate-limited)
            logger.info("CoinSwitch DM: REST warmup starting (3.5s between calls)...")
            for tf in ("1m", "5m", "15m", "1h", "4h", "1d"):
                self._warmup_klines(tf)
                time.sleep(self._WARMUP_SLEEP)

            self._warmup_complete = True
            logger.info("✅ CoinSwitch REST warmup complete")

            self.is_ready = self._check_minimum_data()
            logger.info(
                f"CoinSwitch DM ready={self.is_ready} "
                f"(1m={len(self._candles_1m)} 5m={len(self._candles_5m)} "
                f"15m={len(self._candles_15m)} 4h={len(self._candles_4h)})"
            )
            return True

        except Exception as e:
            logger.error(f"CoinSwitch DM start error: {e}", exc_info=True)
            self.is_ready = self.is_streaming = False
            return False

    def stop(self) -> None:
        try:
            self.is_ready = self.is_streaming = False
            if self.ws:
                self.ws.disconnect()
            logger.info("CoinSwitch DM stopped")
        except Exception as e:
            logger.error(f"CoinSwitch DM stop error: {e}")

    def restart_streams(self) -> bool:
        try:
            logger.warning("CoinSwitch DM: restarting streams")
            self._warmup_complete = False
            self._forming_ts.clear()
            self.stop()
            time.sleep(2.0)
            success = self.start()
            if success and self._strategy_ref is not None:
                try:
                    self._strategy_ref.on_stream_restart()
                except Exception:
                    pass
            return success
        except Exception as e:
            logger.error(f"CoinSwitch DM restart error: {e}", exc_info=True)
            return False

    def register_strategy(self, strategy) -> None:
        self._strategy_ref = strategy

    def wait_until_ready(self, timeout_sec: float = 120.0) -> bool:
        start = time.time()
        while not self.is_ready and (time.time() - start) < timeout_sec:
            time.sleep(1.0)
            if not self.is_ready:
                self.is_ready = self._check_minimum_data()
        return self.is_ready

    # ── REST warmup ───────────────────────────────────────────────────────────

    def _warmup_klines(self, label: str, limit: int = 0, retries: int = 2) -> None:
        cfg = self._WARMUP_CONFIG.get(label)
        if not cfg:
            return
        interval_str, minutes_per_candle, default_limit, deque_attr = cfg
        limit = limit or default_limit
        target: deque = getattr(self, deque_attr)

        for attempt in range(1, retries + 2):
            try:
                end_ms   = int(time.time() * 1000)
                start_ms = end_ms - limit * minutes_per_candle * 60 * 1000

                resp = self.api._make_request(
                    method   = "GET",
                    endpoint = "/trade/api/v2/futures/klines",
                    params   = {
                        "symbol":     config.COINSWITCH_SYMBOL,
                        "exchange":   config.COINSWITCH_EXCHANGE,
                        "interval":   interval_str,
                        "start_time": start_ms,
                        "end_time":   end_ms,
                        "limit":      limit,
                    },
                )

                if not isinstance(resp, dict) or resp.get("error"):
                    logger.warning(f"CoinSwitch warmup {label} attempt {attempt}: "
                                   f"{resp.get('error', 'unexpected response')}")
                    if attempt <= retries:
                        time.sleep(self._WARMUP_SLEEP)
                    continue

                data = resp.get("data", [])
                if not data:
                    logger.warning(f"CoinSwitch warmup {label}: no data")
                    if attempt <= retries:
                        time.sleep(self._WARMUP_SLEEP)
                    continue

                seeded = 0
                for k in sorted(data, key=lambda x: int(
                        x.get("close_time") or x.get("start_time") or 0)):
                    try:
                        c = Candle(
                            timestamp = float(k.get("close_time") or
                                              k.get("start_time") or 0) / 1000.0,
                            open      = float(k.get("o") or k.get("open")   or 0),
                            high      = float(k.get("h") or k.get("high")   or 0),
                            low       = float(k.get("l") or k.get("low")    or 0),
                            close     = float(k.get("c") or k.get("close")  or 0),
                            volume    = float(k.get("v") or k.get("volume") or 0),
                        )
                        if c.close > 0:
                            target.append(c)
                            if label == "1m":
                                self._last_price = c.close
                            seeded += 1
                    except Exception:
                        continue

                if seeded > 0:
                    logger.info(f"CoinSwitch warmup {label}: {seeded} candles")
                    return
                else:
                    if attempt <= retries:
                        time.sleep(self._WARMUP_SLEEP)

            except Exception as e:
                logger.error(f"CoinSwitch warmup {label} attempt {attempt}: {e}")
                if attempt <= retries:
                    time.sleep(self._WARMUP_SLEEP)

    # ── Candle deque helper ───────────────────────────────────────────────────

    def _process_ws_candle(self, data: Dict, candle: Candle,
                           target: deque, tf_key: str, tf_label: str) -> None:
        if not self._warmup_complete:
            self._last_price = candle.close
            self._last_price_update_time = time.time()
            return

        is_closed  = bool(data.get("x", False))
        start_ts   = int(data.get("t", 0))
        forming_ts = self._forming_ts.get(tf_key)

        self._last_price = candle.close
        self._last_price_update_time = time.time()

        if is_closed:
            if forming_ts == start_ts and target:
                target[-1] = candle
            else:
                target.append(candle)
            self._forming_ts.pop(tf_key, None)
            if tf_label != "1m":
                logger.info(f"✅ CoinSwitch {tf_label} CLOSED @ ${candle.close:.2f}")
            else:
                logger.debug(f"✅ CoinSwitch {tf_label} CLOSED @ ${candle.close:.2f}")
        else:
            if forming_ts == start_ts and target:
                target[-1] = candle
            else:
                target.append(candle)
                self._forming_ts[tf_key] = start_ts

        self.stats.record_candle()

    def _make_candle_cb(self, label: str):
        """Factory: returns a WS callback for the given timeframe label."""
        _TF_MAP = {
            "1m": ("1",    self._candles_1m),
            "5m": ("5",    self._candles_5m),
            "15m": ("15",  self._candles_15m),
            "1h": ("60",   self._candles_1h),
            "4h": ("240",  self._candles_4h),
            "1d": ("1440", self._candles_1d),
        }
        tf_key, target = _TF_MAP[label]

        def cb(data: Dict):
            try:
                # CoinSwitch sends interval as the digit string matching subscription
                interval = str(data.get("i", ""))
                if interval and interval != tf_key:
                    return
                with self._lock:
                    c = Candle(
                        timestamp = float(data.get("t", 0)) / 1000.0,
                        open      = float(data.get("o", 0)),
                        high      = float(data.get("h", 0)),
                        low       = float(data.get("l", 0)),
                        close     = float(data.get("c", 0)),
                        volume    = float(data.get("v", 0)),
                    )
                    if c.close <= 0:
                        return
                    self._process_ws_candle(data, c, target, tf_key, label)
            except Exception as e:
                logger.error(f"CoinSwitch {label} candle callback error: {e}")
        return cb

    # ── WS callbacks: orderbook + trades ────────────────────────────────────

    def _on_orderbook(self, data: Dict) -> None:
        try:
            with self._lock:
                self._orderbook = {
                    "bids": data.get("bids", []),
                    "asks": data.get("asks", []),
                }
                bids = self._orderbook["bids"]
                asks = self._orderbook["asks"]
                if bids and asks:
                    try:
                        self._last_price = (float(bids[0][0]) + float(asks[0][0])) / 2.0
                    except Exception:
                        pass
                self.stats.record_orderbook()
        except Exception as e:
            logger.debug(f"CoinSwitch OB callback: {e}")

    def _on_trade(self, data: Dict) -> None:
        # BUG-DDM-1 FIX: snapshot state and release self._lock BEFORE firing the
        # strategy callback. Previously, _on_realtime_trade() was called while
        # self._lock was held. If the strategy (running in the same WS event thread)
        # called get_candles / get_last_price / get_orderbook, those also acquire
        # self._lock. Even though self._lock is an RLock (reentrant for the same
        # thread), the callback could itself block on another lock that the main
        # trading thread holds — creating a cross-thread deadlock.  The fix is
        # the standard pattern: gather all shared-state reads under the lock,
        # then release it, then run any external callbacks in clear air.
        try:
            price = qty = 0.0
            side  = "buy"
            _callback = None
            with self._lock:
                price = float(data.get("price", 0))
                qty   = float(data.get("quantity", 0))
                side  = data.get("side", "buy")
                if price > 0:
                    self._last_price = price
                    self._last_price_update_time = time.time()
                    self._recent_trades.append({
                        "price":     price,
                        "quantity":  qty,
                        "side":      side,
                        "timestamp": time.time(),
                    })
                    if self._strategy_ref is not None:
                        _callback = getattr(self._strategy_ref, "_on_realtime_trade", None)
                self.stats.record_trade()
            # ── Lock released — fire callback in clear air ──────────────────
            if _callback is not None and price > 0:
                try:
                    _callback(price, qty, side)
                except Exception:
                    pass
        except Exception as e:
            logger.debug(f"CoinSwitch trade callback: {e}")

    # ── Readiness ─────────────────────────────────────────────────────────────

    def _check_minimum_data(self) -> bool:
        counts = {
            "1m":  len(self._candles_1m),
            "5m":  len(self._candles_5m),
            "15m": len(self._candles_15m),
            "1h":  len(self._candles_1h),
            "4h":  len(self._candles_4h),
            "1d":  len(self._candles_1d),
        }
        mins = {
            "1m":  getattr(config, "MIN_CANDLES_1M",   100),
            "5m":  getattr(config, "MIN_CANDLES_5M",   100),
            "15m": getattr(config, "MIN_CANDLES_15M",  100),
            "1h":  getattr(config, "MIN_CANDLES_1H",    20),
            "4h":  max(getattr(config, "MIN_CANDLES_4H", 40), 29),
            "1d":  getattr(config, "MIN_CANDLES_1D",     7),
        }
        missing = [f"{tf}({counts[tf]}<{mins[tf]})"
                   for tf in mins if counts[tf] < mins[tf]]
        if missing:
            logger.debug(f"CoinSwitch DM not ready: {', '.join(missing)}")
            return False
        return True

    # ── Public interface (identical to DeltaDataManager) ──────────────────────

    def get_last_price(self) -> float:
        with self._lock:
            return self._last_price

    def get_orderbook(self) -> Dict:
        with self._lock:
            return {
                "bids": list(self._orderbook.get("bids", [])),
                "asks": list(self._orderbook.get("asks", [])),
                "timestamp": time.time(),
            }

    def get_recent_trades_raw(self) -> List[Dict]:
        with self._lock:
            return list(self._recent_trades)[-200:]

    def is_price_fresh(self, max_stale_seconds: float = 90.0) -> bool:
        if self._last_price_update_time == 0:
            return True
        return (time.time() - self._last_price_update_time) < max_stale_seconds

    def get_candles(self, timeframe: str = "5m", limit: int = 100) -> List[Dict]:
        """Return candles as strategy-compatible dicts: {t(ms), o, h, l, c, v}."""
        tf_map = {
            "1m": self._candles_1m, "5m": self._candles_5m,
            "15m": self._candles_15m, "1h": self._candles_1h,
            "4h": self._candles_4h,  "1d": self._candles_1d,
        }
        src = tf_map.get(timeframe, self._candles_5m)
        with self._lock:
            candles = list(src)
        return [
            {"t": int(c.timestamp * 1000), "o": c.open, "h": c.high,
             "l": c.low, "c": c.close, "v": c.volume}
            for c in candles[-limit:]
        ]

    def get_volume_delta(self, lookback_seconds: float = 60.0) -> Dict:
        """Buy/sell volume delta for the given lookback window."""
        with self._lock:
            cutoff   = time.time() - lookback_seconds
            buy_vol  = sum(t["quantity"] for t in self._recent_trades
                          if t["timestamp"] >= cutoff and t["side"] == "buy")
            sell_vol = sum(t["quantity"] for t in self._recent_trades
                          if t["timestamp"] >= cutoff and t["side"] == "sell")
        total = buy_vol + sell_vol
        return {
            "buy_volume":  buy_vol,
            "sell_volume": sell_vol,
            "delta":       buy_vol - sell_vol,
            "delta_pct":   (buy_vol - sell_vol) / total if total > 0 else 0.0,
        }

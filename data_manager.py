"""
Enhanced Data Manager with Full OHLCV Candle Support + REST Warmup (Production Grade)

Pattern: Connect → Subscribe → REST Warmup → Ready
Based on proven Z-Score data manager architecture
"""

import time
import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from collections import deque
from typing import Optional, Dict, List

import config
from candle_compat import wrap_candles
from futures_websocket import FuturesWebSocket
from futures_api import FuturesAPI

logger = logging.getLogger(__name__)


# =====================================================================
# Candle model
# =====================================================================
@dataclass
class Candle:
    timestamp: float  # seconds
    open: float
    high: float
    low: float
    close: float
    volume: float

    def is_bullish(self) -> bool:
        return self.close > self.open

    def is_bearish(self) -> bool:
        return self.close < self.open

    def body_size(self) -> float:
        return abs(self.close - self.open)

    def total_range(self) -> float:
        return self.high - self.low

    def upper_wick(self) -> float:
        return self.high - max(self.open, self.close)

    def lower_wick(self) -> float:
        return min(self.open, self.close) - self.low

    def body_percentage(self) -> float:
        r = self.total_range()
        return (self.body_size() / r) if r > 0 else 0.0


# =====================================================================
# Stats
# =====================================================================
class StreamStats:
    def __init__(self) -> None:
        self._last_update: Optional[datetime] = None
        self._orderbook_count = 0
        self._trades_count = 0
        self._candles_count = 0
        self._lock = threading.RLock()

    def record_orderbook(self) -> None:
        with self._lock:
            self._orderbook_count += 1
            self._last_update = datetime.now(timezone.utc)

    def record_trade(self) -> None:
        with self._lock:
            self._trades_count += 1
            self._last_update = datetime.now(timezone.utc)

    def record_candle(self) -> None:
        with self._lock:
            self._candles_count += 1
            self._last_update = datetime.now(timezone.utc)

    def get_last_update(self) -> Optional[datetime]:
        with self._lock:
            return self._last_update


# =====================================================================
# Data Manager
# =====================================================================
class ICTDataManager:
    """
    Enhanced Data Manager for ICT Strategy
    - Multi-timeframe OHLCV candle storage
    - Orderbook + trades
    - REST warmup + readiness gating
    """

    def __init__(self) -> None:
        self.api = FuturesAPI(
            api_key=getattr(config, "COINSWITCH_API_KEY", None),
            secret_key=getattr(config, "COINSWITCH_SECRET_KEY", None),
        )
        self.ws: Optional[FuturesWebSocket] = None
        self.stats = StreamStats()

        # Candles
        self._candles_1m: deque[Candle] = deque(maxlen=2000)
        self._candles_5m: deque[Candle] = deque(maxlen=1200)
        self._candles_15m: deque[Candle] = deque(maxlen=800)
        self._candles_1h: deque[Candle] = deque(maxlen=500) 
        self._candles_4h: deque[Candle] = deque(maxlen=400)
        self._candles_1d: deque[Candle] = deque(maxlen=100)

        # Price / microstructure
        self._last_price: float = 0.0
        self._last_price_update_time: float = 0.0   # tracks when price last changed from trade/candle
        self._orderbook: Dict = {"bids": [], "asks": []}
        self._recent_trades: deque[Dict] = deque(maxlen=500)

        # Thread safety
        self._lock = threading.RLock()

        # ── Forming candle tracking ─────────────────────────────────────
        # Tracks the start_time (ms) of the current forming (not-yet-closed)
        # candle per timeframe.  Prevents overwriting historical closed candles
        # when WS sends updates for the current forming period.
        self._forming_ts: Dict[str, int] = {}   # {tf_key: start_time_ms}
        self._warmup_complete: bool = False      # gate WS candles until REST fills deques

        # Indicator cache

        # Readiness
        self._strategy_ref = None   # set via register_strategy()
        self.is_ready: bool = False
        self.is_streaming: bool = False

        logger.info("ICTDataManager initialized (REST warmup + strict WS routing)")

    # -----------------------------------------------------------------
    # Lifecycle
    # -----------------------------------------------------------------
    def start(self) -> bool:
        """Connect WebSocket, subscribe to streams, then warm up from REST klines."""
        try:
            self.is_ready = False
            self.is_streaming = False
            
            logger.info("Starting WebSocket streams...")
            
            # Initialize WebSocket
            self.ws = FuturesWebSocket()
            
            # Connect FIRST
            logger.info("Connecting to CoinSwitch Futures WebSocket...")
            if not self.ws.connect(timeout=30):
                logger.error("❌ Failed to connect WebSocket")
                return False
            
            # Subscribe to orderbook and trades (callbacks will be added by subscribe methods)
            logger.info(f"Subscribing ORDERBOOK: {config.SYMBOL}")
            self.ws.subscribe_orderbook(config.SYMBOL, callback=self._on_orderbook_update)
            
            logger.info(f"Subscribing TRADES: {config.SYMBOL}")
            self.ws.subscribe_trades(config.SYMBOL, callback=self._on_trades_update)
            
            # Subscribe candlesticks with dedicated callbacks
            logger.info(f"Subscribing CANDLESTICKS 1m: {config.SYMBOL}_1")
            self.ws.subscribe_candlestick(
                pair=config.SYMBOL,
                interval=1,
                callback=self._on_candlestick_1m
            )
            
            logger.info(f"Subscribing CANDLESTICKS 5m: {config.SYMBOL}_5")
            self.ws.subscribe_candlestick(
                pair=config.SYMBOL,
                interval=5,
                callback=self._on_candlestick_5m
            )
            
            logger.info(f"Subscribing CANDLESTICKS 15m: {config.SYMBOL}_15")
            self.ws.subscribe_candlestick(
                pair=config.SYMBOL,
                interval=15,
                callback=self._on_candlestick_15m
            )

            logger.info(f"Subscribing CANDLESTICKS 1h: {config.SYMBOL}_60")
            self.ws.subscribe_candlestick(
                pair=config.SYMBOL,
                interval=60,
                callback=self._on_candlestick_1h
            )

            logger.info(f"Subscribing CANDLESTICKS 4h: {config.SYMBOL}_240")
            self.ws.subscribe_candlestick(
                pair=config.SYMBOL,
                interval=240,
                callback=self._on_candlestick_4h
            )

            logger.info(f"Subscribing CANDLESTICKS 1d: {config.SYMBOL}_1440")
            self.ws.subscribe_candlestick(
                pair=config.SYMBOL,
                interval=1440,
                callback=self._on_candlestick_1d
            )
            
            self.is_streaming = True
            logger.info("✓ WebSocket streams started successfully")
            logger.info("  - Order Book: Active")
            logger.info("  - Trades: Active")
            logger.info("  - Candles: 1m, 5m, 15m, 1h, 4h, 1d")
            
            # ✅ CRITICAL: REST warmup BEFORE WS candle subscriptions
            # WS candle callbacks check _warmup_complete before processing.
            # This eliminates the race condition where WS candle arrives to an
            # empty deque and gets falsely logged as "CLOSED".
            logger.info("Warming up candles from REST API...")
            self._warmup_from_klines_1m()
            time.sleep(3.5)   # respect CoinSwitch 3s hard limit
            self._warmup_from_klines_5m()
            time.sleep(3.5)
            self._warmup_from_klines_15m()
            time.sleep(3.5)
            self._warmup_from_klines_1h()
            time.sleep(3.5)
            self._warmup_from_klines_4h()
            time.sleep(3.5)
            self._warmup_from_klines_1d()

            # NOW safe to process WS candles — deques are populated
            self._warmup_complete = True
            logger.info("✅ REST warmup complete — WS candle processing enabled")  
            
            # Mark ready if minimum candles exist
            self.is_ready = self._check_minimum_data()
            logger.info(
                f"DataManager ready={self.is_ready} "
                f"(1m={len(self._candles_1m)} 5m={len(self._candles_5m)} "
                f"15m={len(self._candles_15m)} 1h={len(self._candles_1h)} " 
                f"4h={len(self._candles_4h)} 1d={len(self._candles_1d)})"   
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error starting ICTDataManager: {e}", exc_info=True)
            self.is_ready = False
            self.is_streaming = False
            return False

    def register_strategy(self, strategy) -> None:
        """
        Register strategy instance so trade stream can feed
        real-time handlers in the strategy (e.g. tick flow engine).
        Must be called AFTER both data_manager.start() and strategy.__init__().
        """
        self._strategy_ref = strategy
        logger.info("✅ Strategy reference registered in DataManager "
                    "(trade stream → real-time feed active)")

    def stop(self) -> None:
        try:
            self.is_ready = False
            self.is_streaming = False
            if self.ws:
                self.ws.disconnect()
            logger.info("✓ ICTDataManager stopped")
        except Exception as e:
            logger.error(f"Error stopping ICTDataManager: {e}")

    def restart_streams(self) -> bool:
        """Restart WebSocket with state preservation.
        
        BUG 2 FIX (v4.3): After successful restart, notify strategy to reset
        all engine timestamps (CVD, ATR, ADX). Without this, engines retain
        stale _last_bar_ts values and silently skip all REST warmup candles,
        leaving signals at zero/stale after reconnect.
        """
        try:
            logger.warning("Restarting streams (will re-warmup)")
            self._warmup_complete = False
            self._forming_ts.clear()
            self.stop()
            time.sleep(2.0)
            success = self.start()
            
            # FIX BUG 2: Reset strategy engine timestamps after warmup
            if success and self._strategy_ref is not None:
                try:
                    self._strategy_ref.on_stream_restart()
                    logger.info("✅ Strategy engines reset after stream restart")
                except Exception as e:
                    logger.warning(f"Strategy reset notification failed (non-fatal): {e}")
            
            return success
        except Exception as e:
            logger.error(f"Error restarting ICTDataManager: {e}", exc_info=True)
            return False

    # -----------------------------------------------------------------
    # Warmup helpers
    # -----------------------------------------------------------------
    # -----------------------------------------------------------------
    # Warmup — generic method with retry (replaces 6 copy-pasted methods)
    # -----------------------------------------------------------------

    # Mapping: label → (interval_str, minutes_per_candle, default_limit, deque_attr)
    _WARMUP_CONFIG = {
        "1m":  ("1",    1,    100, "_candles_1m"),
        "5m":  ("5",    5,    100, "_candles_5m"),
        "15m": ("15",   15,   100, "_candles_15m"),
        "1h":  ("60",   60,   100, "_candles_1h"),
        "4h":  ("240",  240,   50, "_candles_4h"),
        "1d":  ("1440", 1440,  30, "_candles_1d"),
    }

    def _warmup_klines(self, label: str, limit: int = 0, retries: int = 2) -> None:
        """
        Generic REST kline warmup for any timeframe.

        Args:
            label:   Timeframe label (e.g. "1m", "5m", "1h")
            limit:   Number of candles to fetch (0 = use default)
            retries: Number of retry attempts on failure
        """
        cfg = self._WARMUP_CONFIG.get(label)
        if cfg is None:
            logger.error(f"Unknown warmup label: {label}")
            return

        interval_str, minutes_per_candle, default_limit, deque_attr = cfg
        if limit <= 0:
            limit = default_limit
        target_deque = getattr(self, deque_attr)

        for attempt in range(1, retries + 2):  # retries + 1 total attempts
            try:
                end_ms = int(time.time() * 1000)
                start_ms = end_ms - limit * minutes_per_candle * 60 * 1000

                params = {
                    "symbol": config.SYMBOL,
                    "exchange": config.EXCHANGE,
                    "interval": interval_str,
                    "start_time": start_ms,
                    "end_time": end_ms,
                    "limit": limit,
                }

                logger.info(f"Warmup {label}: fetching {limit} candles"
                            + (f" (attempt {attempt})" if attempt > 1 else ""))
                resp = self.api._make_request(
                    method="GET",
                    endpoint="/trade/api/v2/futures/klines",
                    params=params,
                )

                if not isinstance(resp, dict):
                    logger.warning(f"Warmup {label}: unexpected response type {type(resp)}")
                    if attempt <= retries:
                        time.sleep(3.5)
                        continue
                    return

                if resp.get("error"):
                    logger.warning(f"Warmup {label}: API error: {resp.get('error')}")
                    if attempt <= retries:
                        time.sleep(3.5)
                        continue
                    return

                data = resp.get("data", [])
                if not data:
                    logger.warning(f"Warmup {label}: no data returned")
                    if attempt <= retries:
                        time.sleep(3.5)
                        continue
                    return

                seeded = 0
                for k in sorted(data, key=lambda x: int(x.get("close_time") or x.get("start_time") or 0)):
                    try:
                        candle = Candle(
                            timestamp=float(k.get("close_time") or k.get("start_time") or 0) / 1000.0,
                            open=float(k.get("o") or k.get("open") or 0),
                            high=float(k.get("h") or k.get("high") or 0),
                            low=float(k.get("l") or k.get("low") or 0),
                            close=float(k.get("c") or k.get("close") or 0),
                            volume=float(k.get("v") or k.get("volume") or 0),
                        )
                        if candle.close > 0:
                            target_deque.append(candle)
                            # Track last price from 1m (finest resolution available)
                            if label == "1m":
                                self._last_price = candle.close
                            seeded += 1
                    except Exception:
                        continue

                if seeded > 0:
                    msg = f"Warmup {label} complete: {seeded} candles"
                    if label == "1m":
                        msg += f", last_price={self._last_price:.2f}"
                    logger.info(msg)
                    return  # Success
                else:
                    logger.warning(f"Warmup {label}: no valid candles parsed")
                    if attempt <= retries:
                        time.sleep(3.5)
                        continue
                    return

            except Exception as e:
                logger.error(f"Error in {label} warmup (attempt {attempt}): {e}", exc_info=True)
                if attempt <= retries:
                    time.sleep(3.5)
                    continue

    # Legacy method names — delegate to generic for backward compat
    def _warmup_from_klines_1m(self, limit: int = 100) -> None:
        self._warmup_klines("1m", limit)

    def _warmup_from_klines_5m(self, limit: int = 100) -> None:
        self._warmup_klines("5m", limit)

    def _warmup_from_klines_15m(self, limit: int = 100) -> None:
        self._warmup_klines("15m", limit)

    def _warmup_from_klines_1h(self, limit: int = 100) -> None:
        self._warmup_klines("1h", limit)

    def _warmup_from_klines_4h(self, limit: int = 50) -> None:
        self._warmup_klines("4h", limit)

    def _warmup_from_klines_1d(self, limit: int = 30) -> None:
        self._warmup_klines("1d", limit)

    def _check_minimum_data(self) -> bool:
        """
        Check if we have minimum candles to start trading.

        Minimums are determined by the most data-hungry algorithm per timeframe:
          1m  / 5m  / 15m : structure detection needs 50+ bars
          1h              : daily bias 1H structure uses last 20 confirmed MSS
          4h              : RegimeEngine Wilder's ADX needs 2*period+1 = 29 bars
          1d              : weekly DR detection needs at least 7 daily bars
        """
        min_1m  = getattr(config, "MIN_CANDLES_1M",  100)
        min_5m  = getattr(config, "MIN_CANDLES_5M",  100)
        min_15m = getattr(config, "MIN_CANDLES_15M", 100)
        min_1h  = getattr(config, "MIN_CANDLES_1H",   20)
        min_4h  = max(getattr(config, "MIN_CANDLES_4H", 40), 29)  # RegimeEngine ADX needs 29
        min_1d  = getattr(config, "MIN_CANDLES_1D",    7)

        counts = {
            "1m":  len(self._candles_1m),
            "5m":  len(self._candles_5m),
            "15m": len(self._candles_15m),
            "1h":  len(self._candles_1h),
            "4h":  len(self._candles_4h),
            "1d":  len(self._candles_1d),
        }
        mins = {"1m": min_1m, "5m": min_5m, "15m": min_15m,
                "1h": min_1h, "4h": min_4h, "1d": min_1d}

        missing = [f"{tf}({counts[tf]}<{mins[tf]})"
                   for tf in mins if counts[tf] < mins[tf]]
        if missing:
            logger.debug(f"DataManager not ready — insufficient candles: {', '.join(missing)}")
            return False
        return True

    def wait_until_ready(self, timeout_sec: float = 120.0) -> bool:
        """Block until data manager is ready or timeout"""
        start = time.time()
        while not self.is_ready and (time.time() - start) < timeout_sec:
            time.sleep(1.0)
            # Re-check in case WS filled candles
            if not self.is_ready:
                self.is_ready = self._check_minimum_data()
        
        return self.is_ready

    # -----------------------------------------------------------------
    # WebSocket candle helper — correct forming/closed handling
    # -----------------------------------------------------------------
    def _process_ws_candle(self, data: Dict, candle: Candle,
                           candle_deque: deque, tf_key: str,
                           tf_label: str) -> None:
        """
        Correctly process a WS candle update for any timeframe.

        Solves two critical bugs:
        1. Pre-warmup race: if _warmup_complete is False, ignore WS candles
           (REST will fill the deque with correct historical data).
        2. Forming→closed transition: tracks forming candles by start_time
           so we never overwrite a different period's closed candle.

        Flow:
        - is_closed=True, same start_ts as forming → finalize in place
        - is_closed=True, new start_ts → append new closed candle
        - is_closed=False, same start_ts as forming → update in place
        - is_closed=False, new start_ts → append new forming candle
        """
        if not self._warmup_complete:
            # Before warmup, only update _last_price (for readiness display)
            self._last_price = candle.close
            self._last_price_update_time = time.time()
            return

        is_closed  = data.get('x', False)
        start_ts   = int(data.get('t', 0))          # candle start time (ms)
        forming_ts = self._forming_ts.get(tf_key)    # currently tracked forming start_ts

        self._last_price = candle.close
        self._last_price_update_time = time.time()

        if is_closed:
            if forming_ts == start_ts and candle_deque:
                # Finalize the forming candle we've been tracking
                candle_deque[-1] = candle
            else:
                # Brand new closed candle (or first after warmup)
                candle_deque.append(candle)
            # Clear forming tracker — this period is done
            self._forming_ts.pop(tf_key, None)
            # Only surface 5m+ closures at INFO — 1m fires every minute and is noise
            if tf_label.endswith("1m"):
                logger.debug(f"✅ {tf_label} CLOSED @ ${candle.close:.2f} ({len(candle_deque)})")
            else:
                logger.info(f"✅ {tf_label} CLOSED @ ${candle.close:.2f} ({len(candle_deque)})")
        else:
            if forming_ts == start_ts and candle_deque:
                # Same period update — overwrite in place
                candle_deque[-1] = candle
            else:
                # New forming period — append (don't overwrite last closed)
                candle_deque.append(candle)
                self._forming_ts[tf_key] = start_ts

        self.stats.record_candle()

    # -----------------------------------------------------------------
    # WebSocket callbacks
    # -----------------------------------------------------------------
    def _on_orderbook_update(self, data: Dict) -> None:
        try:
            with self._lock:
                self._orderbook = {
                    "bids": data.get("b", data.get("bids", [])),
                    "asks": data.get("a", data.get("asks", [])),
                }
                
                if self._orderbook["bids"] and self._orderbook["asks"]:
                    best_bid = float(self._orderbook["bids"][0][0])
                    best_ask = float(self._orderbook["asks"][0][0])
                    self._last_price = (best_bid + best_ask) / 2.0
                
                self.stats.record_orderbook()
        except Exception as e:
            logger.debug(f"Error processing orderbook: {e}")

    def _on_trades_update(self, data: Dict) -> None:
        try:
            with self._lock:
                price = float(data.get("p", 0))
                qty   = float(data.get("q", 0))
                is_buyer_maker = data.get("m", False)
                side  = "sell" if is_buyer_maker else "buy"

                if price > 0:
                    self._last_price = price
                    self._last_price_update_time = time.time()
                    self._recent_trades.append({
                        "price":     price,
                        "quantity":  qty,
                        "side":      side,
                        "timestamp": time.time(),
                    })

                    # ── Feed strategy-registered real-time handlers ───
                    if self._strategy_ref is not None:
                        try:
                            on_rt = getattr(self._strategy_ref, "_on_realtime_trade", None)
                            if on_rt is not None:
                                on_rt(price, qty, side)
                        except Exception:
                            pass
                    # ─────────────────────────────────────────────────────

                self.stats.record_trade()
        except Exception as e:
            logger.debug(f"Error processing trade: {e}")


    def _on_candlestick_1m(self, data: Dict) -> None:
        """Process 1m candlestick"""
        try:
            interval = str(data.get('i', ''))
            if interval and interval != '1':
                return

            with self._lock:
                candle = Candle(
                    timestamp=float(data.get('t', 0)) / 1000.0,
                    open=float(data.get('o', 0)),
                    high=float(data.get('h', 0)),
                    low=float(data.get('l', 0)),
                    close=float(data.get('c', 0)),
                    volume=float(data.get('v', 0)),
                )
                if candle.close <= 0:
                    return
                self._process_ws_candle(data, candle, self._candles_1m, '1', '1m')
        except Exception as e:
            logger.error(f"1m error: {e}")


    def _on_candlestick_5m(self, data: Dict) -> None:
        """Process 5m candlestick"""
        try:
            interval = str(data.get('i', ''))
            if interval and interval != '5':
                return
            with self._lock:
                candle = Candle(
                    timestamp=float(data.get('t', 0)) / 1000.0,
                    open=float(data.get('o', 0)),
                    high=float(data.get('h', 0)),
                    low=float(data.get('l', 0)),
                    close=float(data.get('c', 0)),
                    volume=float(data.get('v', 0)),
                )
                if candle.close <= 0:
                    return
                self._process_ws_candle(data, candle, self._candles_5m, '5', '5m')
        except Exception as e:
            logger.error(f"5m error: {e}")


    def _on_candlestick_15m(self, data: Dict) -> None:
        """Process 15m candlestick"""
        try:
            interval = str(data.get('i', ''))
            if interval and interval != '15':
                return
            with self._lock:
                candle = Candle(
                    timestamp=float(data.get('t', 0)) / 1000.0,
                    open=float(data.get('o', 0)),
                    high=float(data.get('h', 0)),
                    low=float(data.get('l', 0)),
                    close=float(data.get('c', 0)),
                    volume=float(data.get('v', 0)),
                )
                if candle.close <= 0:
                    return
                self._process_ws_candle(data, candle, self._candles_15m, '15', '15m')
        except Exception as e:
            logger.error(f"15m error: {e}")

    def _on_candlestick_1h(self, data: Dict) -> None:
        """Process 1h candlestick"""
        try:
            interval = str(data.get('i', ''))
            if interval and interval != '60':
                return
            with self._lock:
                candle = Candle(
                    timestamp=float(data.get('t', 0)) / 1000.0,
                    open=float(data.get('o', 0)),
                    high=float(data.get('h', 0)),
                    low=float(data.get('l', 0)),
                    close=float(data.get('c', 0)),
                    volume=float(data.get('v', 0)),
                )
                if candle.close <= 0:
                    return
                self._process_ws_candle(data, candle, self._candles_1h, '60', '1h')
        except Exception as e:
            logger.error(f"1h error: {e}")


    def _on_candlestick_4h(self, data: Dict) -> None:
        """Process 4h candlestick"""
        try:
            interval = str(data.get('i', ''))
            if interval and interval != '240':
                return
            with self._lock:
                candle = Candle(
                    timestamp=float(data.get('t', 0)) / 1000.0,
                    open=float(data.get('o', 0)),
                    high=float(data.get('h', 0)),
                    low=float(data.get('l', 0)),
                    close=float(data.get('c', 0)),
                    volume=float(data.get('v', 0)),
                )
                if candle.close <= 0:
                    return
                self._process_ws_candle(data, candle, self._candles_4h, '240', '4h')
        except Exception as e:
            logger.error(f"4h error: {e}")

    def _on_candlestick_1d(self, data: Dict) -> None:
        """Process 1d candlestick"""
        try:
            interval = str(data.get('i', ''))
            if interval and interval != '1440':
                return
            with self._lock:
                candle = Candle(
                    timestamp=float(data.get('t', 0)) / 1000.0,
                    open=float(data.get('o', 0)),
                    high=float(data.get('h', 0)),
                    low=float(data.get('l', 0)),
                    close=float(data.get('c', 0)),
                    volume=float(data.get('v', 0)),
                )
                if candle.close <= 0:
                    return
                self._process_ws_candle(data, candle, self._candles_1d, '1440', '1d')
        except Exception as e:
            logger.error(f"1d error: {e}")

    # -----------------------------------------------------------------
    # Public data access
    # -----------------------------------------------------------------
    def get_last_price(self) -> float:
        with self._lock:
            return self._last_price

    def is_price_fresh(self, max_stale_seconds: float = 90.0) -> bool:
        """
        Returns False if _last_price hasn't been updated by a real trade or
        candle close within max_stale_seconds.  Orderbook pings do NOT count,
        so this correctly detects weekend / low-volume freezes that fool the
        normal WS health check.
        """
        if self._last_price_update_time == 0:
            return True   # never received a price yet — don't restart prematurely
        return (time.time() - self._last_price_update_time) < max_stale_seconds

    def get_orderbook(self) -> Dict:
        """
        Return the current orderbook snapshot for microstructure analysis.
        Returns: {"bids": [[price, qty], ...], "asks": [[price, qty], ...]}
        Thread-safe copy.
        """
        with self._lock:
            return {
                "bids": list(self._orderbook.get("bids", [])),
                "asks": list(self._orderbook.get("asks", [])),
            }

    def get_recent_trades_raw(self) -> List[Dict]:
        """
        Return recent trades for tick flow analysis.
        Each trade: {"price": float, "quantity": float, "side": str, "timestamp": float}
        Thread-safe copy of the last 200 trades.
        """
        with self._lock:
            return list(self._recent_trades)[-200:]

    def get_recent_candles(self, timeframe: str = "1m", limit: int = 50) -> List[Candle]:
        with self._lock:
            if timeframe == "1m":
                candles = list(self._candles_1m)
            elif timeframe == "5m":
                candles = list(self._candles_5m)
            elif timeframe == "15m":
                candles = list(self._candles_15m)
            elif timeframe == "1h":  # ← ADD
                candles = list(self._candles_1h)
            elif timeframe == "4h":
                candles = list(self._candles_4h)
            elif timeframe == "1d":  # ← ADD
                candles = list(self._candles_1d)
            else:
                logger.warning(f"Unknown timeframe: {timeframe}, defaulting to 1m")
                candles = list(self._candles_1m)
            
            result = candles[-limit:] if candles else []
        return wrap_candles(result)


    def get_volume_delta(self, lookback_seconds: float = 60.0) -> Dict:
        """Volume delta from recent trades — used by periodic Telegram reports."""
        with self._lock:
            now = time.time()
            cutoff = now - lookback_seconds
            buy_vol = 0.0
            sell_vol = 0.0
            
            for trade in self._recent_trades:
                if trade["timestamp"] >= cutoff:
                    qty = trade["quantity"]
                    if trade["side"] == "buy":
                        buy_vol += qty
                    else:
                        sell_vol += qty
            
            total_vol = buy_vol + sell_vol
            delta = buy_vol - sell_vol
            delta_pct = delta / total_vol if total_vol > 0 else 0.0
            
            return {
                "buy_volume": buy_vol,
                "sell_volume": sell_vol,
                "delta": delta,
                "delta_pct": delta_pct,
            }

    def get_candles(self, timeframe: str, limit: int = 500) -> List[Dict]:
        """
        Strategy-facing candle accessor.
        Returns list of dicts: {'o', 'h', 'l', 'c', 'v', 't'(ms)}
        — the exact format consumed by strategy.py, regime_engine.py,
        and all structure detection methods.

        IMPORTANT: get_recent_candles() returns CandleDict wrappers
        (via wrap_candles). CandleDict['t'] already returns ms.
        We must NOT multiply by 1000 again.
        """
        raw = self.get_recent_candles(timeframe, limit=limit)
        result: List[Dict] = []
        for c in raw:
            try:
                # CandleDict supports [] access — c['t'] already returns ms
                # This works for both CandleDict wrappers and plain dicts
                result.append({
                    'o': float(c['o']),
                    'h': float(c['h']),
                    'l': float(c['l']),
                    'c': float(c['c']),
                    'v': float(c['v']),
                    't': int(c['t']),   # already ms from CandleDict
                })
            except (KeyError, TypeError):
                # Fallback for raw Candle dataclass (no [] support)
                if hasattr(c, 'open') and hasattr(c, 'timestamp'):
                    result.append({
                        'o': c.open,
                        'h': c.high,
                        'l': c.low,
                        'c': c.close,
                        'v': c.volume,
                        't': int(c.timestamp * 1000),   # seconds → ms
                    })
        return result


# Backward compatibility
ZScoreDataManager = ICTDataManager

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
from core.instruments import ExchangeName
from core.candle import Candle, wrap_candles
from market_data.feed_health import score_feed_health
from market_data.normalizer import InstrumentMapping, build_venue_microstate
from market_data.microstructure import LatencyBaseline, MicrostructureTracker
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

    def __init__(self, instrument=None) -> None:
        self.instrument = instrument
        self.exchange_instrument = (instrument.by_exchange.get(ExchangeName.COINSWITCH)
                                    if instrument is not None and hasattr(instrument, "by_exchange") else None)
        self.symbol = (self.exchange_instrument.symbol if self.exchange_instrument is not None
                       else config.COINSWITCH_SYMBOL)
        self.ws_symbol = (self.exchange_instrument.ws_symbol if self.exchange_instrument is not None
                          else self.symbol)
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
        self._last_orderbook_update_time: float = 0.0
        self._orderbook:              Dict  = {"bids": [], "asks": []}
        self._recent_trades:          deque = deque(maxlen=500)
        # Queue-flow and trade-flow are stored separately in comparable USD notional.
        self._microstructure = MicrostructureTracker(self._instrument_mapping())
        self._latency_baseline = LatencyBaseline()
        self._latest_latency_ms: float | None = None
        self._latest_latency_z: float | None = None
        self._sequence_valid = True
        self._snapshot_ready = False
        self._funding_rate: float | None = None
        self._last_market_meta_refresh_s: float = 0.0

        self._lock         = threading.RLock()
        self._forming_ts:  Dict[str, int] = {}
        self._warmup_complete = False

        self._strategy_ref = None
        self.is_ready      = False
        self.is_streaming  = False

        logger.info(f"CoinSwitchDataManager initialised ({self.symbol})")

    def _instrument_mapping(self) -> InstrumentMapping:
        raw = getattr(self.exchange_instrument, "raw", {}) or {}
        return InstrumentMapping(
            venue="coinswitch", venue_symbol=self.symbol, canonical_underlying=str(getattr(self.instrument, "asset_id", "BTC")),
            product_class=str(raw.get("contract_type") or "linear_perp"),
            quote_currency=str(raw.get("quote_asset") or "USDT"),
            contract_multiplier=float(raw.get("contract_multiplier") or raw.get("contract_value") or 1.0),
            settlement_currency=str(raw.get("settlement_currency") or "USDT"),
            price_tick=float(raw.get("tick_size") or getattr(config, "TICK_SIZE_COINSWITCH", 0.1)),
            qty_step=float(raw.get("lot_step") or raw.get("qty_step") or 0.001), execution_enabled=True,
            notional_model="linear",
        )

    @staticmethod
    def _payload_ts_ns(data: Dict) -> int | None:
        for key in ("timestamp", "time", "ts", "t"):
            try:
                value = float(data.get(key) or 0.0)
            except Exception:
                value = 0.0
            if value <= 0:
                continue
            if value > 1e17:
                return int(value)
            if value > 1e14:
                return int(value * 1_000)
            if value > 1e11:
                return int(value * 1_000_000)
            return int(value * 1_000_000_000)
        return None

    def _record_latency(self, data: Dict, receive_ts_ns: int) -> None:
        exchange_ts_ns = self._payload_ts_ns(data)
        if exchange_ts_ns is None:
            return
        latency_ms = max(0.0, (receive_ts_ns - exchange_ts_ns) / 1_000_000.0)
        self._latest_latency_ms = latency_ms
        self._latest_latency_z = self._latency_baseline.observe(latency_ms)

    def _execution_cost_metadata(self) -> Dict[str, object]:
        """Fee metadata confirmed by the live instrument catalog when supplied.

        The protected lifecycle can execute an entry and an exit aggressively;
        therefore the route selector uses taker+taker as the fail-safe estimate,
        rather than presuming a maker fill on a limit entry.
        """
        raw = getattr(self.exchange_instrument, "raw", {}) or {}
        try:
            taker = float(raw.get("taker_fee_rate") or 0.0)
        except (TypeError, ValueError):
            taker = 0.0
        if taker > 0:
            return {"round_trip_fee_bps": 2.0 * taker * 10_000.0, "fee_basis": "instrument_info_taker_taker"}
        return {}

    def _refresh_market_metadata(self) -> None:
        """Refresh documented CoinSwitch funding state without REST flooding."""
        now = time.time()
        if now - self._last_market_meta_refresh_s < float(getattr(config, "VENUE_MARKET_META_REFRESH_SEC", 30.0)):
            return
        self._last_market_meta_refresh_s = now
        try:
            resp = self.api.get_futures_ticker(symbol=self.symbol, exchange=config.COINSWITCH_EXCHANGE)
            data = resp.get("data", {}) if isinstance(resp, dict) else {}
            row = data.get(config.COINSWITCH_EXCHANGE, data) if isinstance(data, dict) else {}
            if isinstance(row, dict) and row.get("funding_rate") is not None:
                self._funding_rate = float(row.get("funding_rate"))
        except Exception as exc:
            logger.debug("CoinSwitch funding metadata refresh failed for %s: %s", self.symbol, exc)

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> bool:
        try:
            self.is_ready = self.is_streaming = False
            symbol = self.ws_symbol

            logger.info(f"CoinSwitch DM[{symbol}]: starting WebSocket...")
            self.ws = CoinSwitchWebSocket()

            if not self.ws.connect(timeout=30):
                logger.error(f"❌ CoinSwitch WS failed to connect for {symbol}")
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
            logger.info(f"✅ CoinSwitch WS streams subscribed for {symbol}")

            # REST warmup (rate-limited)
            logger.info(f"CoinSwitch DM[{self.symbol}]: REST warmup starting (3.5s between calls)...")
            for tf in ("1m", "5m", "15m", "1h", "4h", "1d"):
                self._warmup_klines(tf)
                time.sleep(self._WARMUP_SLEEP)

            self._warmup_complete = True
            logger.info(f"✅ CoinSwitch REST warmup complete for {self.symbol}")

            self.is_ready = self._check_minimum_data()
            logger.info(
                f"CoinSwitch DM[{self.symbol}] ready={self.is_ready} "
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
                        "symbol":     self.symbol,
                        "exchange":   config.COINSWITCH_EXCHANGE,
                        "interval":   interval_str,
                        "start_time": start_ms,
                        "end_time":   end_ms,
                        "limit":      limit,
                    },
                )

                if not isinstance(resp, dict) or resp.get("error"):
                    logger.warning(f"CoinSwitch warmup {self.symbol} {label} attempt {attempt}: "
                                   f"{resp.get('error', 'unexpected response')}")
                    if attempt <= retries:
                        time.sleep(self._WARMUP_SLEEP)
                    continue

                data = resp.get("data", [])
                if not data:
                    logger.warning(f"CoinSwitch warmup {self.symbol} {label}: no data")
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
                    logger.info(f"CoinSwitch warmup {self.symbol} {label}: {seeded} candles")
                    return
                else:
                    if attempt <= retries:
                        time.sleep(self._WARMUP_SLEEP)

            except Exception as e:
                logger.error(f"CoinSwitch warmup {self.symbol} {label} attempt {attempt}: {e}")
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
            callback = None
            quote_price = 0.0
            with self._lock:
                receive_ts_ns = time.time_ns()
                self._orderbook = {
                    "bids": data.get("bids", []),
                    "asks": data.get("asks", []),
                }
                self._last_orderbook_update_time = receive_ts_ns / 1_000_000_000.0
                self._snapshot_ready = bool(self._orderbook["bids"] and self._orderbook["asks"])
                self._microstructure.update_book(self._orderbook["bids"], self._orderbook["asks"], self._last_orderbook_update_time)
                self._record_latency(data, receive_ts_ns)
                bids = self._orderbook["bids"]
                asks = self._orderbook["asks"]
                if bids and asks:
                    quote_price = (float(bids[0][0]) + float(asks[0][0])) / 2.0
                    self._last_price = quote_price
                    self._last_price_update_time = self._last_orderbook_update_time
                    if self._strategy_ref is not None:
                        callback = getattr(self._strategy_ref, "_on_realtime_quote", None)
                self.stats.record_orderbook()
            if callback is not None and quote_price > 0.0:
                callback(quote_price)
        except Exception as e:
            logger.debug(f"CoinSwitch orderbook callback: {e}")

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
                    trade_ts = time.time()
                    self._recent_trades.append({
                        "price":     price,
                        "quantity":  qty,
                        "side":      side,
                        "timestamp": trade_ts,
                    })
                    self._microstructure.record_trade(price=price, quantity=qty, buyer_aggressor=(str(side).lower() == "buy"), timestamp_s=trade_ts)
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

    def get_last_update(self) -> float:
        """Timestamp of the latest executable quote/mark update for lineage auditing."""
        with self._lock:
            return float(self._last_price_update_time or 0.0)

    def get_last_price(self) -> float:
        with self._lock:
            return self._last_price

    def get_orderbook(self) -> Dict:
        with self._lock:
            return {
                "bids": list(self._orderbook.get("bids", [])),
                "asks": list(self._orderbook.get("asks", [])),
                "timestamp": float(self._last_orderbook_update_time or 0.0),
            }

    def get_microstructure_flow(self) -> Dict[str, float]:
        with self._lock:
            return self._microstructure.snapshot(time.time()).asdict()

    def get_microstructure_research_state(self) -> Dict[str, List[Dict[str, float]]]:
        """Raw USD-normalised event stream for dynamic protection calibration."""
        with self._lock:
            return self._microstructure.research_state(time.time())

    def get_feed_reliability(self) -> Dict:
        with self._lock:
            snapshot_ready = bool(self._snapshot_ready)
            connected = bool(self.is_streaming)
            return {
                "connected": connected,
                "heartbeat_ok": connected,
                "sequence_valid": bool(self._sequence_valid),
                "snapshot_ready": snapshot_ready,
                "exchange_timestamp_available": self._latest_latency_ms is not None,
                "latency_ms": self._latest_latency_ms,
                "latency_vs_baseline_z": self._latest_latency_z,
                "no_change_heartbeat_valid": connected and snapshot_ready,
                "latency_baseline_samples": self._latency_baseline.sample_count,
            }

    def get_venue_microstate(self):
        self._refresh_market_metadata()
        with self._lock:
            bids = list(self._orderbook.get("bids", []))
            asks = list(self._orderbook.get("asks", []))
            recv_ts_ns = int((self._last_orderbook_update_time or time.time()) * 1_000_000_000)
            flows = self._microstructure.snapshot(time.time()).asdict()
            reliability = self.get_feed_reliability()
        if not bids or not asks:
            return None
        health = score_feed_health(**{k: reliability[k] for k in ("connected", "heartbeat_ok", "sequence_valid", "snapshot_ready", "exchange_timestamp_available", "latency_vs_baseline_z", "no_change_heartbeat_valid")})
        return build_venue_microstate(
            mapping=self._instrument_mapping(), bids=bids, asks=asks, feed_health=health,
            receive_ts_ns=recv_ts_ns, funding_rate=self._funding_rate,
            metadata=self._execution_cost_metadata(), **flows,
        )

    def get_recent_trades_raw(self) -> List[Dict]:
        with self._lock:
            return list(self._recent_trades)[-200:]

    def is_price_fresh(self, max_stale_seconds: float = 90.0) -> bool:
        if self._last_price_update_time <= 0:
            return False
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

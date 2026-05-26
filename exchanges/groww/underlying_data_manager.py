"""GROWW underlying data manager for option-desk structural analysis.

This manager is intentionally read-only.  It never represents an executable
option contract and never places orders.  It loads the underlying asset chart
through Groww historicalcharts so liquidity/ICT/structure engines analyse the
real market thesis (NIFTY/stock/index) while execution remains on the selected
call/put option contract.
"""
from __future__ import annotations

import logging
import base64
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

_GROWW_LOCAL_TZ = ZoneInfo("Asia/Kolkata") if ZoneInfo is not None else timezone(timedelta(hours=5, minutes=30))
from typing import Any, Dict, List

import config
from .api import GrowwRestClient
from .market_session import groww_market_session_state
from .rate_limiter import groww_throttle
from .live_feed import hub_for_api

logger = logging.getLogger(__name__)


def _cfg(name: str, default):
    return getattr(config, name, default)


class GrowwUnderlyingDataManager:
    """Read-only underlying chart source for GROWW option strategies."""

    def __init__(self, instrument, api: GrowwRestClient | None = None) -> None:
        self.instrument = instrument
        self.api = api or GrowwRestClient()
        self._lock = threading.RLock()
        self._candles: Dict[str, deque] = {tf: deque(maxlen=800) for tf in ("1m", "5m", "15m", "1h", "4h", "1d")}
        self._last_price = 0.0
        self._last_quote_ts = 0.0
        self._trades: deque = deque(maxlen=100)
        self.is_ready = False
        self._strategy_ref = None
        self._sio = None  # compatibility marker: set to shared GrowwLiveFeedHub while subscribed
        self._stream_script = ""
        self._stream_subscription_ids: list[str] = []
        self._last_stream_tick_ts = 0.0
        self._stream_armed_at = 0.0
        self._first_stream_tick = threading.Event()
        self._last_rest_refresh_attempt = 0.0
        self._last_stream_repair_attempt = 0.0
        logger.info(
            "GrowwUnderlyingDataManager initialised [%s -> underlying=%s]",
            getattr(instrument, "asset_id", "GROWW"),
            self._underlying_code(),
        )

    def register_strategy(self, strategy) -> None:
        self._strategy_ref = strategy

    def start(self) -> bool:
        try:
            session = groww_market_session_state()
            if not session.is_open and bool(_cfg("GROWW_ANALYZE_ONLY_DURING_MARKET_SESSION", True)):
                self.is_ready = False
                logger.warning(
                    "GROWW underlying chart dormant for %s: %s; no post-market NIFTY analysis",
                    getattr(self.instrument, "asset_id", "?"),
                    session.reason,
                )
                return False
            self.api.preflight_session()
            self._warmup()
            min_bars = int(_cfg("GROWW_UNDERLYING_MIN_READY_1M_BARS", 20))
            self.is_ready = len(self._candles.get("1m", ())) >= min_bars or len(self._candles.get("5m", ())) >= min_bars
            if self.is_ready:
                stream_ok = self._start_websocket()
                if bool(_cfg("GROWW_INDEX_WEBSOCKET_REQUIRED", True)) and not stream_ok:
                    self.is_ready = False
                    logger.error("GROWW underlying desk blocked: mandatory real-time websocket is not live for %s", self._display_underlying())
            if not self.is_ready:
                logger.warning(
                    "GROWW underlying chart not ready for %s: %s",
                    getattr(self.instrument, "asset_id", "?"), self._count_summary(),
                )
            else:
                logger.info(
                    "GROWW underlying chart ready for %s: %s",
                    getattr(self.instrument, "asset_id", "?"), self._count_summary(),
                )
            return self.is_ready
        except Exception as exc:
            logger.warning("GROWW underlying chart start failed for %s: %s", getattr(self.instrument, "asset_id", "?"), exc)
            self.is_ready = False
            return False

    def warmup_closed_market(self, reason: str = "market closed") -> None:
        try:
            self.start()
            logger.info(
                "GROWW closed-market underlying historical warmup for %s complete: %s; reason=%s",
                getattr(self.instrument, "asset_id", "?"), self._count_summary(), reason,
            )
        except Exception as exc:
            logger.warning("GROWW closed-market underlying warmup failed for %s: %s", getattr(self.instrument, "asset_id", "?"), exc)

    def stop(self) -> None:
        try:
            if self._sio is not None and self._stream_subscription_ids:
                self._sio.unsubscribe(list(self._stream_subscription_ids))
        except Exception:
            pass
        self._stream_subscription_ids = []
        self._sio = None
        self._last_stream_tick_ts = 0.0
        self._first_stream_tick.clear()
        return None

    def restart_streams(self) -> bool:
        self.stop()
        return self.start()

    def wait_until_ready(self, timeout_sec: float = 120.0) -> bool:
        if self.is_ready:
            return True
        return self.start()

    def _underlying_code(self) -> str:
        raw = getattr(getattr(self.instrument, "primary", None), "raw", {}) or {}
        return str(
            raw.get("groww_stock_code")
            or raw.get("underlying_stock_code")
            or raw.get("stock_code")
            or raw.get("ShortName")
            or raw.get("underlying")
            or raw.get("Underlying")
            or getattr(self.instrument, "asset_id", "")
        ).upper()

    def _display_underlying(self) -> str:
        raw = getattr(getattr(self.instrument, "primary", None), "raw", {}) or {}
        return str(raw.get("underlying_display") or raw.get("underlying") or getattr(self.instrument, "asset_id", "")).upper()

    def _underlying_exchange(self) -> str:
        raw = getattr(getattr(self.instrument, "primary", None), "raw", {}) or {}
        exch = str(raw.get("underlying_exchange_code") or raw.get("underlying_exchange") or "").upper()
        if exch in {"NSE", "BSE"}:
            return exch
        # Options can be listed on NFO/BFO while the underlying index/stock chart
        # lives on NSE/BSE cash/index endpoints.  Do not hardcode a symbol list;
        # infer from derivative segment metadata.
        deriv_ex = str(raw.get("exchange_code") or raw.get("ExchangeCode") or "").upper()
        return "BSE" if deriv_ex == "BFO" else "NSE"

    def _warmup(self) -> None:
        for tf in ("1m", "5m", "15m", "1h", "4h", "1d"):
            try:
                self._load_historical(tf)
            except Exception as exc:
                logger.debug("GROWW underlying historical warmup %s failed for %s: %s", tf, self._underlying_code(), exc)

    def _load_historical(self, timeframe: str) -> None:
        source = {
            "1m": ("minute", 1),
            "5m": ("5minute", 5),
            "15m": ("5minute", 15),
            "1h": ("30minute", 60),
            "4h": ("30minute", 240),
            "1d": ("day", 1440),
        }.get(timeframe, ("minute", 1))
        interval, target_minutes = source
        to_dt = datetime.now(timezone.utc)
        from_dt = to_dt - timedelta(days=7 if timeframe in {"1m", "5m", "15m"} else 45 if timeframe in {"1h", "4h"} else 180)
        base_req = {
            "interval": interval,
            "from_date": from_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "to_date": to_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "stock_code": self._underlying_code(),
            "exchange_code": self._underlying_exchange(),
            "product_type": "cash",
        }
        rows = []
        # Try the documented signed v1 route first, then v2.  Keep attempts
        # bounded and non-synthetic: if Groww has no underlying chart, no fake
        # candles are generated.
        for req in (base_req, {**base_req, "product_type": "Cash"}):
            try:
                groww_throttle(f"underlying_historical:{timeframe}:{self._underlying_code()}")
                resp = self.api.get_historical_charts(**{k: v for k, v in req.items() if v})
                rows = self._rows(resp)
                if rows:
                    break
            except Exception:
                continue
        if not rows and bool(_cfg("GROWW_HISTORICAL_V2_FALLBACK", True)):
            v2_req = dict(base_req)
            v2_req["exch_code"] = v2_req.pop("exchange_code")
            # Groww v2 interval vocabulary differs from the v1 signed route.
            v2_req["interval"] = {
                "minute": "1minute", "5minute": "5minute",
                "30minute": "30minute", "day": "1day",
            }.get(str(v2_req.get("interval") or ""), str(v2_req.get("interval") or ""))
            v2_req["product_type"] = "Cash"
            v2_req["from_date"] = from_dt.strftime("%Y-%m-%d %H:%M:%S")
            v2_req["to_date"] = to_dt.strftime("%Y-%m-%d %H:%M:%S")
            try:
                groww_throttle(f"underlying_historical_v2:{timeframe}:{self._underlying_code()}")
                resp = self.api.get_historical_charts_v2(**v2_req)
                rows = self._rows(resp)
            except Exception:
                rows = []
        parsed = self._parse_rows(rows)
        parsed = self._resample(parsed, target_minutes) if target_minutes not in (1, 5, 1440) else parsed
        if parsed:
            with self._lock:
                self._candles[timeframe].clear(); self._candles[timeframe].extend(parsed[-800:])
                self._last_price = float(parsed[-1]["c"])
                if not bool(_cfg("GROWW_INDEX_WEBSOCKET_REQUIRED", True)):
                    self._last_quote_ts = time.time()

    @staticmethod
    def _rows(resp: Dict[str, Any]) -> List[Any]:
        rows = resp.get("Success") or resp.get("data") or resp.get("result") or [] if isinstance(resp, dict) else []
        if isinstance(rows, dict):
            rows = rows.get("data") or rows.get("candles") or []
        return rows if isinstance(rows, list) else []

    def _parse_rows(self, rows: List[Any]) -> List[Dict[str, Any]]:
        parsed: List[Dict[str, Any]] = []
        for r in rows:
            if not isinstance(r, dict):
                continue
            c = self._float_first(r, ("close", "Close"))
            if c <= 0:
                continue
            o = self._float_first(r, ("open", "Open")) or c
            h = self._float_first(r, ("high", "High")) or c
            l = self._float_first(r, ("low", "Low")) or c
            v = self._float_first(r, ("volume", "Volume"))
            ts = r.get("datetime") or r.get("date") or r.get("time") or time.time()
            parsed.append(self._canonical_candle(ts, o, h, l, c, v))
        return sorted(parsed, key=lambda x: int(x.get("t", 0) or 0))

    @classmethod
    def _canonical_candle(cls, ts: Any, o: float, h: float, l: float, c: float, v: float = 0.0) -> Dict[str, Any]:
        ts_ms = cls._parse_ts_ms(ts)
        high = max(float(h or c), float(o or c), float(c or 0.0))
        low = min(float(l or c), float(o or c), float(c or 0.0))
        return {
            "t": ts_ms, "o": float(o or c), "h": high, "l": low, "c": float(c), "v": float(v or 0.0),
            "timestamp": ts_ms / 1000.0, "open": float(o or c), "high": high, "low": low, "close": float(c), "volume": float(v or 0.0),
        }

    @staticmethod
    def _parse_ts_ms(value: Any) -> int:
        if isinstance(value, (int, float)):
            f = float(value)
            return int(f * 1000) if f < 1e12 else int(f)
        text = str(value or "").strip()
        if not text:
            return int(time.time() * 1000)
        try:
            f = float(text)
            return int(f * 1000) if f < 1e12 else int(f)
        except Exception:
            pass

        # Groww often returns timezone-less Indian market timestamps.  Treating
        # those as UTC pushes NSE/BSE candles 5h30m into the future, which creates
        # negative sweep ages and stale/future liquidity events.  Explicit `Z` or
        # offset-bearing strings remain UTC/offset-aware; naive strings are IST.
        for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
            try:
                return int(datetime.strptime(text, fmt).replace(tzinfo=timezone.utc).timestamp() * 1000)
            except Exception:
                pass
        try:
            iso = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if iso.tzinfo is not None:
                return int(iso.timestamp() * 1000)
        except Exception:
            pass
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d-%b-%Y %H:%M:%S", "%a %b %d %H:%M:%S %Y", "%Y-%m-%d"):
            try:
                return int(datetime.strptime(text, fmt).replace(tzinfo=_GROWW_LOCAL_TZ).timestamp() * 1000)
            except Exception:
                pass
        return int(time.time() * 1000)

    @staticmethod
    def _resample(rows: List[Dict[str, Any]], target_minutes: int) -> List[Dict[str, Any]]:
        if not rows or target_minutes <= 0:
            return rows
        bucket_ms = target_minutes * 60 * 1000
        buckets: Dict[int, List[Dict[str, Any]]] = {}
        for row in rows:
            ts = int(row.get("t", 0) or 0)
            if ts <= 0:
                continue
            buckets.setdefault((ts // bucket_ms) * bucket_ms, []).append(row)
        out: List[Dict[str, Any]] = []
        for bucket in sorted(buckets):
            chunk = sorted(buckets[bucket], key=lambda x: int(x.get("t", 0) or 0))
            if not chunk:
                continue
            out.append({
                "t": bucket,
                "o": float(chunk[0]["o"]),
                "h": max(float(x["h"]) for x in chunk),
                "l": min(float(x["l"]) for x in chunk),
                "c": float(chunk[-1]["c"]),
                "v": sum(float(x.get("v", 0.0) or 0.0) for x in chunk),
                "timestamp": bucket / 1000.0,
                "open": float(chunk[0]["o"]),
                "high": max(float(x["h"]) for x in chunk),
                "low": min(float(x["l"]) for x in chunk),
                "close": float(chunk[-1]["c"]),
                "volume": sum(float(x.get("v", 0.0) or 0.0) for x in chunk),
            })
        return out

    def _start_websocket(self) -> bool:
        if not bool(_cfg("GROWW_INDEX_STREAM_ENABLED", True)):
            logger.error("GROWW underlying websocket disabled by configuration for %s", self._display_underlying())
            return False
        if self._stream_subscription_ids:
            return True
        try:
            hub = hub_for_api(self.api)
            self._first_stream_tick.clear()
            self._stream_armed_at = time.time()
            subscription_id = hub.subscribe_underlying_quotes(
                exchange_code=self._underlying_exchange(),
                stock_code=self._underlying_code(),
                callback=self._on_stream_candle,
            )
            self._sio = hub
            self._stream_subscription_ids = [subscription_id]
            timeout = max(0.0, float(_cfg("GROWW_INDEX_STREAM_FIRST_TICK_TIMEOUT_SEC", 12.0)))
            if bool(_cfg("GROWW_INDEX_WEBSOCKET_REQUIRED", True)) and timeout > 0 and not self._first_stream_tick.wait(timeout):
                hub.unsubscribe(self._stream_subscription_ids)
                self._stream_subscription_ids = []
                self._sio = None
                logger.error("GROWW underlying websocket subscribed but delivered no live tick within %.1fs for %s", timeout, self._display_underlying())
                return False
            logger.info("GROWW underlying websocket LIVE for %s via official Groww quote feed; 1m/5m/15m/1h/4h bars are streamed/aggregated locally", self._display_underlying())
            return True
        except Exception as exc:
            logger.error("GROWW mandatory underlying websocket unavailable for %s: %s", self._display_underlying(), exc)
            return False

    def _matches_underlying_tick(self, row: Dict[str, Any]) -> bool:
        exchange = str(row.get("exchange_code") or row.get("exchange") or "").upper()
        if exchange and self._underlying_exchange() not in exchange:
            return False
        if row.get("expiry_date") or row.get("strike_price") or row.get("right") or row.get("right_type"):
            return False
        symbol = str(row.get("stock_code") or row.get("stock_name") or row.get("symbol") or "").upper().replace(" ", "")
        target = self._underlying_code().replace(" ", "")
        return not symbol or target in symbol or symbol in target or (target == "NIFTY" and "NIFTY50" in symbol)

    def _on_stream_candle(self, data: Any) -> None:
        """Consume official Groww live underlying quotes/OHLC without synthetic prices.

        NIFTY is documented by Groww as a real-time quote subscription.  The
        tick price therefore updates the current one-minute candle, from which
        the institutional 5m/15m/1h/4h structural frames are aggregated.
        """
        try:
            row = data[0] if isinstance(data, list) and data and isinstance(data[0], dict) else data
            if not isinstance(row, dict) or not self._matches_underlying_tick(row):
                return
            c = self._float_first(row, ("last", "ltp", "last_price", "close", "Close", "c"))
            if c <= 0:
                return
            ts = row.get("datetime") or row.get("ltt") or row.get("time") or row.get("t") or time.time()
            interval = str(row.get("interval") or row.get("Interval") or "").lower()
            if interval in {"1minute", "1min", "1m"}:
                o = self._float_first(row, ("open", "Open", "o")) or c
                h = self._float_first(row, ("high", "High", "h")) or c
                l = self._float_first(row, ("low", "Low", "l")) or c
                v = self._float_first(row, ("volume", "Volume", "v"))
            else:
                # Quote responses expose session OHLC/TTQ, not one-minute OHLCV.
                # Using those fields as a 1m candle would contaminate ATR, sweeps
                # and delivery geometry. Build live bars strictly from tick LTP.
                o = h = l = c
                v = 0.0
            candle = self._canonical_candle(ts, o, h, l, c, v)
            bucket = (int(candle["t"]) // 60000) * 60000
            candle["t"] = bucket; candle["timestamp"] = bucket / 1000.0
            with self._lock:
                self._upsert_live_candle("1m", candle)
                self._aggregate_live_frames_from_1m(int(candle["t"]))
                self._last_price = float(candle["c"])
                self._last_quote_ts = time.time()
                self._last_stream_tick_ts = self._last_quote_ts
                quote_price = self._last_price
            self._first_stream_tick.set()
            if self._strategy_ref is not None:
                callback = getattr(self._strategy_ref, "_on_realtime_quote", None)
                if callable(callback):
                    callback(quote_price)
        except Exception as exc:
            logger.debug("GROWW underlying websocket tick rejected: %s", exc)

    def _upsert_live_candle(self, timeframe: str, candle: Dict[str, Any]) -> None:
        target = self._candles[timeframe]
        ts = int(candle.get("t", 0) or 0)
        if target and int(target[-1].get("t", 0) or 0) == ts:
            previous = target[-1]
            merged = dict(candle)
            merged["o"] = merged["open"] = float(previous.get("o", previous.get("open", candle["o"])) or candle["o"])
            merged["h"] = merged["high"] = max(float(previous.get("h", previous.get("high", candle["h"])) or candle["h"]), float(candle["h"]))
            merged["l"] = merged["low"] = min(float(previous.get("l", previous.get("low", candle["l"])) or candle["l"]), float(candle["l"]))
            # Quote ticks may expose cumulative volume; never add it repeatedly.
            merged["v"] = merged["volume"] = max(float(previous.get("v", 0.0) or 0.0), float(candle.get("v", 0.0) or 0.0))
            target[-1] = merged
        elif not target or ts > int(target[-1].get("t", 0) or 0):
            target.append(candle)

    def _aggregate_live_frames_from_1m(self, timestamp_ms: int) -> None:
        rows = list(self._candles.get("1m", ()))
        for timeframe, minutes in (("5m", 5), ("15m", 15), ("1h", 60), ("4h", 240)):
            bucket_ms = minutes * 60000
            bucket = (timestamp_ms // bucket_ms) * bucket_ms
            chunk = [r for r in rows if bucket <= int(r.get("t", 0) or 0) < bucket + bucket_ms]
            if not chunk:
                continue
            aggregated = {
                "t": bucket, "timestamp": bucket / 1000.0,
                "o": float(chunk[0]["o"]), "open": float(chunk[0]["o"]),
                "h": max(float(r["h"]) for r in chunk), "high": max(float(r["h"]) for r in chunk),
                "l": min(float(r["l"]) for r in chunk), "low": min(float(r["l"]) for r in chunk),
                "c": float(chunk[-1]["c"]), "close": float(chunk[-1]["c"]),
                "v": sum(float(r.get("v", 0.0) or 0.0) for r in chunk),
                "volume": sum(float(r.get("v", 0.0) or 0.0) for r in chunk),
            }
            self._upsert_live_candle(timeframe, aggregated)

    def _count_summary(self) -> str:
        with self._lock:
            return " ".join(f"{tf}={len(self._candles.get(tf, ())) }" for tf in ("1m", "5m", "15m", "1h", "4h", "1d"))

    def _maybe_refresh_live(self) -> None:
        session = groww_market_session_state()
        if not session.is_open and bool(_cfg("GROWW_ANALYZE_ONLY_DURING_MARKET_SESSION", True)):
            return
        repair_interval = float(_cfg("GROWW_UNDERLYING_REST_REFRESH_SEC", 30.0) or 0.0)
        reconcile_interval = float(_cfg("GROWW_UNDERLYING_REST_RECONCILE_SEC", 900.0) or 900.0)
        if repair_interval <= 0:
            return
        now = time.time()
        with self._lock:
            stream_age = now - float(self._last_stream_tick_ts or 0.0) if self._last_stream_tick_ts else 999999.0
        live = bool(self._stream_subscription_ids) and stream_age <= float(_cfg("GROWW_INDEX_STREAM_MAX_STALE_SEC", 15.0))
        if not live:
            self._repair_stream_if_stale("underlying_stream_stale")
        elif self._sio is not None and hasattr(self._sio, "ensure_live"):
            try:
                self._sio.ensure_live(max_stale_sec=float(_cfg("GROWW_INDEX_STREAM_MAX_STALE_SEC", 15.0)), reason="underlying_transport_health_check")
            except Exception:
                pass
        interval = reconcile_interval if live else repair_interval
        if now - float(self._last_rest_refresh_attempt or 0.0) < interval:
            return
        self._last_rest_refresh_attempt = now
        try:
            # REST repairs history but never makes the mandatory websocket look fresh.
            # Crucially, all structural frames used by the strategy are reconciled.
            for timeframe in ("1m", "5m", "15m", "1h", "4h"):
                self._load_historical(timeframe)
            if not live:
                logger.warning("GROWW underlying live stream stale/unavailable for %s; REST history repaired but trading remains blocked until websocket ticks resume", self._display_underlying())
        except Exception as exc:
            logger.debug("GROWW underlying REST reconciliation skipped for %s: %s", self._display_underlying(), exc)

    def _repair_stream_if_stale(self, reason: str, *, wait: bool = False) -> bool:
        if not bool(_cfg("GROWW_INDEX_STREAM_ENABLED", True)) or not bool(_cfg("GROWW_INDEX_WEBSOCKET_REQUIRED", True)):
            return False
        if not self._stream_subscription_ids or self._sio is None:
            return False
        session = groww_market_session_state()
        if not session.is_open and bool(_cfg("GROWW_ANALYZE_ONLY_DURING_MARKET_SESSION", True)):
            return False
        now = time.time()
        max_stale = float(_cfg("GROWW_INDEX_STREAM_MAX_STALE_SEC", 15.0))
        with self._lock:
            stream_age = now - float(self._last_stream_tick_ts or 0.0) if self._last_stream_tick_ts else 999999.0
        if stream_age <= max_stale:
            return False
        cooldown = max(1.0, float(_cfg("GROWW_WEBSOCKET_RECONNECT_COOLDOWN_SEC", 5.0)))
        if now - float(self._last_stream_repair_attempt or 0.0) < cooldown:
            return False
        self._last_stream_repair_attempt = now
        self._first_stream_tick.clear()
        repair = getattr(self._sio, "reconnect_and_resubscribe", None)
        if not callable(repair):
            return False
        ok = bool(repair(reason=reason))
        if ok:
            logger.warning(
                "GROWW underlying websocket repair triggered for %s after %.1fs without a fresh tick",
                self._display_underlying(), stream_age,
            )
            if wait:
                timeout = max(0.0, float(_cfg("GROWW_INDEX_STREAM_FIRST_TICK_TIMEOUT_SEC", 12.0)))
                self._first_stream_tick.wait(timeout)
        return ok

    def get_last_update(self) -> float:
        with self._lock:
            return float(self._last_quote_ts or 0.0)

    def get_candles(self, timeframe: str = "5m", limit: int = 100) -> List[Dict]:
        self._maybe_refresh_live()
        with self._lock:
            return list(self._candles.get(timeframe, deque()))[-int(limit):]

    def get_last_price(self) -> float:
        self._maybe_refresh_live()
        with self._lock:
            return float(self._last_price or 0.0)

    def get_orderbook(self) -> Dict:
        return {"bids": [], "asks": [], "timestamp": self._last_quote_ts, "_sources": 0, "_executable_source": "groww_groww_websocket_underlying" if self._last_stream_tick_ts > 0 else "groww_underlying_rest_reconcile"}

    def get_recent_trades(self, limit: int = 100) -> List[Dict]:
        return []

    def get_recent_trades_raw(self, limit: int = 100) -> List[Dict]:
        return []

    def is_price_fresh(self, max_stale_seconds: float = 90.0) -> bool:
        self._repair_stream_if_stale("underlying_freshness_gate")
        with self._lock:
            ts = float(self._last_stream_tick_ts if bool(_cfg("GROWW_INDEX_WEBSOCKET_REQUIRED", True)) else self._last_quote_ts or 0.0)
        maximum = min(float(max_stale_seconds), float(_cfg("GROWW_INDEX_STREAM_MAX_STALE_SEC", 15.0))) if bool(_cfg("GROWW_INDEX_WEBSOCKET_REQUIRED", True)) else float(max_stale_seconds)
        return ts > 0 and (time.time() - ts) <= maximum

    @staticmethod
    def _float_first(row: Dict[str, Any], names: tuple[str, ...]) -> float:
        for n in names:
            try:
                f = float(row.get(n, 0) or 0)
                if f > 0:
                    return f
            except Exception:
                continue
        return 0.0

    @staticmethod
    def _num(value: Any) -> float:
        try:
            return float(value or 0.0)
        except Exception:
            return 0.0

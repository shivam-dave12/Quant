"""ICICI underlying data manager for option-desk structural analysis.

This manager is intentionally read-only.  It never represents an executable
option contract and never places orders.  It loads the underlying asset chart
through Breeze historicalcharts so liquidity/ICT/structure engines analyse the
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

_ICICI_LOCAL_TZ = ZoneInfo("Asia/Kolkata") if ZoneInfo is not None else timezone(timedelta(hours=5, minutes=30))
from typing import Any, Dict, List

import config
from .api import BreezeRestClient
from .rate_limiter import breeze_throttle

logger = logging.getLogger(__name__)


def _cfg(name: str, default):
    return getattr(config, name, default)


class ICICIUnderlyingDataManager:
    """Read-only underlying chart source for ICICI option strategies."""

    def __init__(self, instrument, api: BreezeRestClient | None = None) -> None:
        self.instrument = instrument
        self.api = api or BreezeRestClient()
        self._lock = threading.RLock()
        self._candles: Dict[str, deque] = {tf: deque(maxlen=800) for tf in ("1m", "5m", "15m", "1h", "4h", "1d")}
        self._last_price = 0.0
        self._last_quote_ts = 0.0
        self._trades: deque = deque(maxlen=100)
        self.is_ready = False
        self._strategy_ref = None
        self._sio = None
        self._stream_script = ""
        self._last_rest_refresh_attempt = 0.0
        logger.info(
            "ICICIUnderlyingDataManager initialised [%s -> underlying=%s]",
            getattr(instrument, "asset_id", "ICICI"),
            self._underlying_code(),
        )

    def register_strategy(self, strategy) -> None:
        self._strategy_ref = strategy

    def start(self) -> bool:
        try:
            self.api.preflight_session()
            self._warmup()
            min_bars = int(_cfg("ICICI_UNDERLYING_MIN_READY_1M_BARS", 20))
            self.is_ready = len(self._candles.get("1m", ())) >= min_bars or len(self._candles.get("5m", ())) >= min_bars
            if self.is_ready:
                self._start_websocket()
            if not self.is_ready:
                logger.warning(
                    "ICICI underlying chart not ready for %s: %s",
                    getattr(self.instrument, "asset_id", "?"), self._count_summary(),
                )
            else:
                logger.info(
                    "ICICI underlying chart ready for %s: %s",
                    getattr(self.instrument, "asset_id", "?"), self._count_summary(),
                )
            return self.is_ready
        except Exception as exc:
            logger.warning("ICICI underlying chart start failed for %s: %s", getattr(self.instrument, "asset_id", "?"), exc)
            self.is_ready = False
            return False

    def warmup_closed_market(self, reason: str = "market closed") -> None:
        try:
            self.start()
            logger.info(
                "ICICI closed-market underlying historical warmup for %s complete: %s; reason=%s",
                getattr(self.instrument, "asset_id", "?"), self._count_summary(), reason,
            )
        except Exception as exc:
            logger.warning("ICICI closed-market underlying warmup failed for %s: %s", getattr(self.instrument, "asset_id", "?"), exc)

    def stop(self) -> None:
        try:
            if self._sio is not None:
                self._sio.disconnect()
        except Exception:
            pass
        self._sio = None
        return None

    def restart_streams(self) -> bool:
        return self.start()

    def wait_until_ready(self, timeout_sec: float = 120.0) -> bool:
        if self.is_ready:
            return True
        return self.start()

    def _underlying_code(self) -> str:
        raw = getattr(getattr(self.instrument, "primary", None), "raw", {}) or {}
        return str(
            raw.get("breeze_stock_code")
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
                logger.debug("ICICI underlying historical warmup %s failed for %s: %s", tf, self._underlying_code(), exc)

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
        # bounded and non-synthetic: if Breeze has no underlying chart, no fake
        # candles are generated.
        for req in (base_req, {**base_req, "product_type": "Cash"}):
            try:
                breeze_throttle(f"underlying_historical:{timeframe}:{self._underlying_code()}")
                resp = self.api.get_historical_charts(**{k: v for k, v in req.items() if v})
                rows = self._rows(resp)
                if rows:
                    break
            except Exception:
                continue
        if not rows and bool(_cfg("ICICI_HISTORICAL_V2_FALLBACK", True)):
            v2_req = dict(base_req)
            v2_req["exch_code"] = v2_req.pop("exchange_code")
            v2_req["product_type"] = "Cash"
            v2_req["from_date"] = from_dt.strftime("%Y-%m-%d %H:%M:%S")
            v2_req["to_date"] = to_dt.strftime("%Y-%m-%d %H:%M:%S")
            try:
                breeze_throttle(f"underlying_historical_v2:{timeframe}:{self._underlying_code()}")
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

        # Breeze often returns timezone-less Indian market timestamps.  Treating
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
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d-%b-%Y %H:%M:%S", "%Y-%m-%d"):
            try:
                return int(datetime.strptime(text, fmt).replace(tzinfo=_ICICI_LOCAL_TZ).timestamp() * 1000)
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

    def _start_websocket(self) -> None:
        if not bool(_cfg("ICICI_INDEX_STREAM_ENABLED", True)):
            return
        script_code = self._stream_script_code()
        if not script_code:
            if bool(_cfg("ICICI_INDEX_WEBSOCKET_REQUIRED", False)):
                logger.warning("ICICI underlying websocket script code missing for %s", self._display_underlying())
            return
        if self._sio is not None:
            return
        try:
            import socketio  # type: ignore
            session = self.api.auth.get_session(force_refresh=False)
            user_id, token = base64.b64decode(session.session_token.encode("ascii")).decode("ascii").split(":", 1)
            sio = socketio.Client(logger=False, engineio_logger=False, reconnection=True)

            def _handle(data):
                self._on_stream_candle(data)

            for channel in self._stream_channels():
                sio.on(channel, _handle)
            sio.connect(
                "https://breezeapi.icicidirect.com",
                headers={"User-Agent": "python-socketio[client]/socket"},
                auth={"user": user_id, "token": token},
                transports=["websocket"],
                socketio_path="ohlcvstream",
                wait_timeout=5,
            )
            sio.emit("join", script_code)
            self._sio = sio
            self._stream_script = script_code
            logger.info("ICICI underlying websocket live for %s script=%s channels=%s", self._display_underlying(), script_code, ",".join(self._stream_channels()))
        except Exception as exc:
            logger.warning("ICICI underlying websocket unavailable for %s: %s; REST warmup remains active", self._display_underlying(), exc)

    def _stream_channels(self) -> list[str]:
        raw = str(_cfg("ICICI_INDEX_STREAM_CHANNELS", "1MIN,5MIN") or "")
        return [x.strip().upper() for x in raw.replace(";", ",").split(",") if x.strip()]

    def _stream_script_code(self) -> str:
        raw = _cfg("ICICI_INDEX_STREAM_SCRIPT_CODES", {})
        keys = {self._display_underlying(), self._underlying_code(), str(getattr(self.instrument, "asset_id", "")).upper()}
        if isinstance(raw, dict):
            for k, v in raw.items():
                if str(k).upper() in keys and str(v or "").strip():
                    return str(v).strip()
        return ""

    def _on_stream_candle(self, data: Any) -> None:
        try:
            row = data[0] if isinstance(data, list) and data and isinstance(data[0], (list, tuple, dict)) else data
            if isinstance(row, dict):
                c = self._float_first(row, ("close", "Close", "c"))
                o = self._float_first(row, ("open", "Open", "o")) or c
                h = self._float_first(row, ("high", "High", "h")) or c
                l = self._float_first(row, ("low", "Low", "l")) or c
                v = self._float_first(row, ("volume", "Volume", "v"))
                ts = row.get("datetime") or row.get("time") or row.get("t") or time.time()
                interval = str(row.get("interval") or row.get("Interval") or "").upper()
            elif isinstance(row, (list, tuple)) and len(row) >= 9:
                # ICICI StreamLiveOHLCV positional payload:
                # exchange, stock, low, high, open, close, volume, datetime, interval.
                l = self._num(row[2]); h = self._num(row[3]); o = self._num(row[4]); c = self._num(row[5]); v = self._num(row[6])
                ts = row[7]; interval = str(row[8] or "").upper()
            else:
                return
            if c <= 0:
                return
            tf = "1m" if "1" in interval else "5m" if "5" in interval else ""
            if not tf:
                return
            candle = self._canonical_candle(ts, o, h, l, c, v)
            with self._lock:
                target = self._candles[tf]
                if target and int(target[-1].get("t", 0)) == int(candle["t"]):
                    target[-1] = candle
                else:
                    target.append(candle)
                self._last_price = float(candle["c"])
                self._last_quote_ts = time.time()
        except Exception:
            return

    def _count_summary(self) -> str:
        with self._lock:
            return " ".join(f"{tf}={len(self._candles.get(tf, ())) }" for tf in ("1m", "5m", "15m", "1h", "4h", "1d"))

    def _maybe_refresh_live(self) -> None:
        interval = float(_cfg("ICICI_UNDERLYING_REST_REFRESH_SEC", 30.0) or 0.0)
        if interval <= 0:
            return
        now = time.time()
        with self._lock:
            age = now - float(self._last_quote_ts or 0.0) if self._last_quote_ts else 999999.0
        # If the stream is updating, do not add REST load.  If it is silent or no
        # script code exists for this index, refresh the latest candles on a
        # throttled cadence so ICICI desks do not trade frozen warmup prices.
        if self._sio is not None and age <= max(interval * 2.0, 15.0):
            return
        if now - float(self._last_rest_refresh_attempt or 0.0) < interval:
            return
        self._last_rest_refresh_attempt = now
        try:
            self._load_historical("1m")
            self._load_historical("5m")
        except Exception as exc:
            logger.debug("ICICI underlying REST refresh skipped for %s: %s", self._display_underlying(), exc)

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
        return {"bids": [], "asks": [], "timestamp": self._last_quote_ts, "_sources": 0, "_executable_source": "icici_underlying_history"}

    def get_recent_trades(self, limit: int = 100) -> List[Dict]:
        return []

    def get_recent_trades_raw(self, limit: int = 100) -> List[Dict]:
        return []

    def is_price_fresh(self, max_stale_seconds: float = 90.0) -> bool:
        return self._last_quote_ts > 0

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

"""ICICI underlying data manager for option-desk structural analysis.

This manager is intentionally read-only.  It never represents an executable
option contract and never places orders.  It loads the underlying asset chart
through Breeze historicalcharts so liquidity/ICT/structure engines analyse the
real market thesis (NIFTY/stock/index) while execution remains on the selected
call/put option contract.
"""
from __future__ import annotations

import logging
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
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
        return None

    def restart_streams(self) -> bool:
        return self.start()

    def wait_until_ready(self, timeout_sec: float = 120.0) -> bool:
        if self.is_ready:
            return True
        return self.start()

    def _underlying_code(self) -> str:
        raw = getattr(getattr(self.instrument, "primary", None), "raw", {}) or {}
        return str(raw.get("underlying") or raw.get("Underlying") or raw.get("stock_code") or raw.get("ShortName") or getattr(self.instrument, "asset_id", "")).upper()

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
        interval = {"1m": "minute", "5m": "5minute", "15m": "5minute", "1h": "30minute", "4h": "day", "1d": "day"}.get(timeframe, "minute")
        to_dt = datetime.now(timezone.utc)
        from_dt = to_dt - timedelta(days=7 if timeframe in {"1m", "5m", "15m"} else 90)
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
        if parsed:
            with self._lock:
                self._candles[timeframe].clear(); self._candles[timeframe].extend(parsed[-800:])
                self._last_price = float(parsed[-1]["close"])
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
            parsed.append({"timestamp": ts, "open": o, "high": h, "low": l, "close": c, "volume": v})
        return parsed

    def _count_summary(self) -> str:
        with self._lock:
            return " ".join(f"{tf}={len(self._candles.get(tf, ())) }" for tf in ("1m", "5m", "15m", "1h", "4h", "1d"))

    def get_candles(self, timeframe: str = "5m", limit: int = 100) -> List[Dict]:
        with self._lock:
            return list(self._candles.get(timeframe, deque()))[-int(limit):]

    def get_last_price(self) -> float:
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

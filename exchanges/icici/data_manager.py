"""ICICI Breeze option data manager.

Polling-only by design: Breeze does not use the Delta websocket contract here.
No synthetic candles are produced. If Breeze historical/quote data is missing,
the manager remains not-ready and the runtime will not trade that option.
"""
from __future__ import annotations

import logging
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

try:
    import config
except Exception:  # pragma: no cover
    config = None  # type: ignore

from .api import BreezeRestClient

logger = logging.getLogger(__name__)


def _cfg(name: str, default: Any) -> Any:
    if config is None:
        return default
    return getattr(config, name, default)


class ICICIOptionDataManager:
    def __init__(self, instrument=None, api: BreezeRestClient | None = None) -> None:
        self.instrument = instrument
        self.api = api or BreezeRestClient()
        self._strategy_ref = None
        self._running = False
        self._thread: threading.Thread | None = None
        self._lock = threading.RLock()
        self._last_price = 0.0
        self._last_quote_ts = 0.0
        self._best_bid = 0.0
        self._best_ask = 0.0
        self._candles: dict[str, deque] = {tf: deque(maxlen=600) for tf in ("1m", "5m", "15m", "1h", "4h", "1d")}
        self._trades: deque = deque(maxlen=500)
        self.is_ready = False
        logger.info("ICICIOptionDataManager initialised [%s]", getattr(instrument, "asset_id", "ICICI"))

    def register_strategy(self, strategy) -> None:
        self._strategy_ref = strategy

    def start(self) -> bool:
        try:
            self.api.preflight_session()
            self._warmup()
            if self._last_price <= 0 or len(self._candles.get("1m", ())) < int(_cfg("ICICI_OPTION_MIN_READY_1M_BARS", 30)):
                logger.error("ICICI option DM not ready: missing real quote/historical candles for %s", getattr(self.instrument, "asset_id", "?"))
                return False
            self._running = True
            self.is_ready = True
            self._thread = threading.Thread(target=self._poll_loop, name=f"icici-option-dm-{getattr(self.instrument,'asset_id','')}", daemon=True)
            self._thread.start()
            return True
        except Exception as exc:
            logger.error("ICICI option data start failed for %s: %s", getattr(self.instrument, "asset_id", "?"), exc)
            return False

    def stop(self) -> None:
        self._running = False

    def restart_streams(self) -> bool:
        self.stop()
        return self.start()

    def wait_until_ready(self, timeout_sec: float = 120.0) -> bool:
        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            if self.is_ready:
                return True
            time.sleep(0.25)
        return bool(self.is_ready)

    def _poll_loop(self) -> None:
        interval = float(_cfg("ICICI_OPTION_QUOTE_POLL_SEC", 2.0))
        while self._running:
            try:
                self._refresh_quote()
            except Exception as exc:
                logger.debug("ICICI quote poll failed: %s", exc)
            time.sleep(max(0.5, interval))

    def _warmup(self) -> None:
        for tf in ("1m", "5m", "15m", "1h"):
            try:
                self._load_historical(tf)
            except Exception as exc:
                logger.debug("ICICI historical warmup %s failed: %s", tf, exc)
        self._refresh_quote()

    def _load_historical(self, timeframe: str) -> None:
        raw = getattr(getattr(self.instrument, "primary", None), "raw", {}) or {}
        interval = {"1m": "1minute", "5m": "5minute", "15m": "15minute", "1h": "30minute", "4h": "day", "1d": "day"}.get(timeframe, "1minute")
        to_dt = datetime.now(timezone.utc)
        from_dt = to_dt - timedelta(days=5 if timeframe in {"1m", "5m", "15m"} else 45)
        body = {
            "interval": interval,
            "from_date": from_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "to_date": to_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "stock_code": str(raw.get("stock_code") or raw.get("ShortName") or "").upper(),
            "exchange_code": str(raw.get("exchange_code") or "NFO").upper(),
            "product_type": "options",
            "expiry_date": self.api._normalise_expiry(raw.get("expiry_date") or raw.get("ExpiryDate") or ""),
            "right": self.api._normalise_right(raw.get("right") or raw.get("OptionType") or ""),
            "strike_price": str(raw.get("strike_price") or raw.get("StrikePrice") or ""),
        }
        resp = self.api.get_historical_charts(**{k: v for k, v in body.items() if v})
        rows = resp.get("Success") or resp.get("data") or resp.get("result") or []
        if isinstance(rows, dict):
            rows = rows.get("data") or rows.get("candles") or []
        parsed = []
        for r in rows if isinstance(rows, list) else []:
            if not isinstance(r, dict):
                continue
            o = self._float_first(r, ("open", "Open")); h = self._float_first(r, ("high", "High")); l = self._float_first(r, ("low", "Low")); c = self._float_first(r, ("close", "Close")); v = self._float_first(r, ("volume", "Volume"))
            if c <= 0:
                continue
            ts = r.get("datetime") or r.get("date") or r.get("time") or time.time()
            parsed.append({"timestamp": ts, "open": o or c, "high": h or c, "low": l or c, "close": c, "volume": v})
        if parsed:
            with self._lock:
                self._candles[timeframe].clear(); self._candles[timeframe].extend(parsed[-600:])
                self._last_price = float(parsed[-1]["close"])

    def _refresh_quote(self) -> None:
        q = self.api.get_quote_for_instrument(getattr(self.instrument, "primary", None))
        row = q.get("Success") if isinstance(q, dict) else {}
        if isinstance(row, list) and row:
            row = row[0]
        if not isinstance(row, dict):
            row = q if isinstance(q, dict) else {}
        px = self._float_first(row, ("ltp", "last_price", "lastPrice", "close", "price"))
        if px <= 0:
            return
        bid = self._float_first(row, ("best_bid", "bid", "bPrice", "bid_price"))
        ask = self._float_first(row, ("best_ask", "ask", "sPrice", "ask_price"))
        now = time.time()
        with self._lock:
            self._last_price = px
            self._best_bid = bid
            self._best_ask = ask
            self._last_quote_ts = now
            self._trades.append({"price": px, "quantity": self._float_first(row, ("quantity", "volume", "total_quantity_traded")), "side": "buy", "timestamp": now, "source": "icici_quote"})

    def get_candles(self, timeframe: str = "5m", limit: int = 100) -> List[Dict]:
        with self._lock:
            return list(self._candles.get(timeframe, deque()))[-int(limit):]

    def get_last_price(self) -> float:
        with self._lock:
            return float(self._last_price or 0.0)

    def get_orderbook(self) -> Dict:
        # No synthetic orderbook: only return bid/ask levels that Breeze actually
        # provided in the latest quote payload.
        with self._lock:
            bid = float(self._best_bid or 0.0)
            ask = float(self._best_ask or 0.0)
            ts = self._last_quote_ts
        return {"bids": [[bid, 1.0]] if bid > 0 else [], "asks": [[ask, 1.0]] if ask > 0 else [], "timestamp": ts, "_sources": 1, "_executable_source": "icici_quote"}

    def get_recent_trades(self, limit: int = 100) -> List[Dict]:
        with self._lock:
            return list(self._trades)[-int(limit):]

    def get_recent_trades_raw(self, limit: int = 100) -> List[Dict]:
        return self.get_recent_trades(limit)

    def is_price_fresh(self, max_stale_seconds: float = 90.0) -> bool:
        return self._last_quote_ts > 0 and time.time() - self._last_quote_ts <= max_stale_seconds

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

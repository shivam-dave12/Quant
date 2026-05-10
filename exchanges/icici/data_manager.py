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
from .market_session import icici_market_session_state
from .rate_limiter import breeze_throttle
from agents.icici_chain_architect import is_chain_instrument, select_contract_for_thesis, apply_contract_choice

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
        self._selected_contract = None
        logger.info("ICICIOptionDataManager initialised [%s]", getattr(instrument, "asset_id", "ICICI"))

    def _is_chain_mode(self) -> bool:
        return is_chain_instrument(self.instrument)

    def select_contract_for_thesis(self, thesis_side: str, underlying_spot: float = 0.0):
        choice = select_contract_for_thesis(self.instrument, thesis_side, underlying_spot=underlying_spot)
        if choice is None:
            logger.warning("ICICI option-chain selector found no contract for %s thesis=%s", getattr(self.instrument, "asset_id", "?"), thesis_side)
            return None
        apply_contract_choice(self.instrument, choice)
        self._selected_contract = choice
        logger.info(
            "ICICI option-chain selected %s for %s thesis=%s score=%.2f reasons=%s",
            choice.selected_symbol, getattr(self.instrument, "asset_id", "?"), thesis_side, choice.score, ",".join(choice.reasons),
        )
        # After selection, fetch option quote/historical lazily. Failure keeps
        # the context non-executable, but structure analysis remains on the
        # underlying chart.
        try:
            self._warmup(historical_only=False)
        except Exception as exc:
            logger.warning("ICICI selected option warmup failed for %s: %s", choice.selected_symbol, exc)
        return choice

    def register_strategy(self, strategy) -> None:
        self._strategy_ref = strategy

    def start(self) -> bool:
        try:
            if self._is_chain_mode():
                self.api.preflight_session()
                # Underlying-desk mode: the executable option is deliberately
                # not selected at startup.  The analysis data manager supplies
                # the underlying candles; this primary manager becomes
                # executable only after QuantStrategy produces a thesis and calls
                # select_contract_for_thesis().
                self.is_ready = True
                logger.info(
                    "ICICI option-chain DM ready in underlying-first mode [%s]; contract will be selected post-thesis",
                    getattr(self.instrument, "asset_id", "?"),
                )
                return True
            session = icici_market_session_state()
            if not session.is_open:
                if bool(_cfg("ICICI_ALLOW_CLOSED_MARKET_HISTORICAL_WARMUP", True)):
                    self.warmup_closed_market(session.reason)
                if not bool(_cfg("ICICI_ALLOW_CLOSED_MARKET_WARMUP", False)):
                    logger.warning(
                        "ICICI option DM dormant: %s; historical warmup=%s; live quote/trading disabled for %s",
                        session.reason,
                        self._historical_count_summary(),
                        getattr(self.instrument, "asset_id", "?"),
                    )
                    return False
            self.api.preflight_session()
            self._warmup(historical_only=False)
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

    def warmup_closed_market(self, reason: str = "market closed") -> None:
        """Fetch historical candles outside live trading hours without enabling trading."""
        try:
            self.api.preflight_session()
            self._warmup(historical_only=not bool(_cfg("ICICI_CLOSED_MARKET_QUOTE_PROBE", False)))
            logger.info(
                "ICICI closed-market historical warmup for %s complete: %s; reason=%s",
                getattr(self.instrument, "asset_id", "?"),
                self._historical_count_summary(),
                reason,
            )
        except Exception as exc:
            logger.warning("ICICI closed-market historical warmup failed for %s: %s", getattr(self.instrument, "asset_id", "?"), exc)

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

    def _warmup(self, historical_only: bool = False) -> None:
        for tf in ("1m", "5m", "15m", "1h"):
            try:
                self._load_historical(tf)
            except Exception as exc:
                logger.warning("ICICI historical warmup %s failed for %s: %s", tf, getattr(self.instrument, "asset_id", "?"), exc)
        if not historical_only:
            self._refresh_quote()

    def _historical_count_summary(self) -> str:
        with self._lock:
            return " ".join(f"{tf}={len(self._candles.get(tf, ())) }" for tf in ("1m", "5m", "15m", "1h"))

    def _load_historical(self, timeframe: str) -> None:
        raw = getattr(getattr(self.instrument, "primary", None), "raw", {}) or {}
        selected = raw.get("selected_option_contract") if isinstance(raw, dict) else None
        if isinstance(selected, dict) and isinstance(selected.get("raw"), dict):
            raw = selected.get("raw") or raw
        # Breeze historicalcharts accepts: minute, 5minute, 30minute, day.
        # There is no native 15m/1h interval; use 5m/30m source bars
        # rather than sending unsupported values and getting empty historicals.
        interval = {"1m": "minute", "5m": "5minute", "15m": "5minute", "1h": "30minute", "4h": "day", "1d": "day"}.get(timeframe, "minute")
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
        req = {k: v for k, v in body.items() if v}
        breeze_throttle(f"historical:{timeframe}:{getattr(self.instrument, 'asset_id', '?')}")
        resp = self.api.get_historical_charts(**req)
        rows = resp.get("Success") or resp.get("data") or resp.get("result") or []
        if not rows and bool(_cfg("ICICI_HISTORICAL_V2_FALLBACK", True)):
            v2_req = dict(req)
            v2_req["exch_code"] = v2_req.pop("exchange_code", "NFO")
            # v2 examples accept human-cased values.
            if str(v2_req.get("product_type", "")).lower() == "options":
                v2_req["product_type"] = "Options"
            if str(v2_req.get("right", "")).lower() == "call":
                v2_req["right"] = "Call"
            elif str(v2_req.get("right", "")).lower() == "put":
                v2_req["right"] = "Put"
            # v2 sample URL uses a space-separated timestamp.
            try:
                v2_req["from_date"] = from_dt.strftime("%Y-%m-%d %H:%M:%S")
                v2_req["to_date"] = to_dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                pass
            breeze_throttle(f"historical_v2:{timeframe}:{getattr(self.instrument, 'asset_id', '?')}")
            resp = self.api.get_historical_charts_v2(**v2_req)
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
        raw = getattr(getattr(self.instrument, "primary", None), "raw", {}) or {}
        if self._is_chain_mode() and not raw.get("selected_option_contract"):
            return
        breeze_throttle(f"quote:{getattr(self.instrument, 'asset_id', '?')}")
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

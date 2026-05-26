"""Groww option execution-vehicle data manager.

This reuses the existing Indian options selector and routes all broker I/O
through Groww's official SDK wrapper/feed.  The strategy flow remains the same:
prepare CE/PE vehicles first, then activate only the signal-matching vehicle.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from typing import Any

try:
    import config
except Exception:  # pragma: no cover
    config = None  # type: ignore

from exchanges.icici.data_manager import ICICIOptionDataManager
from .api import GrowwRestClient
from .live_feed import hub_for_api
from .market_session import groww_market_session_state

logger = logging.getLogger(__name__)


def _gcfg(name: str, default: Any) -> Any:
    if config is None:
        return default
    return getattr(config, name, getattr(config, name.replace("GROWW_", "ICICI_", 1), default))


class GrowwOptionDataManager(ICICIOptionDataManager):
    def __init__(self, instrument=None, api: GrowwRestClient | None = None) -> None:
        super().__init__(instrument=instrument, api=api or GrowwRestClient())
        logger.info("GrowwOptionDataManager initialised [%s]", getattr(instrument, "asset_id", "GROWW"))

    def prepare_groww_session_contract_book(self, underlying_spot: float, available_funds: float, *, force_refresh: bool = False, reason: str = "session_start") -> bool:
        return self.prepare_session_contract_book(underlying_spot, available_funds, force_refresh=force_refresh, reason=reason)

    def _arm_session_book_streams(self, book) -> bool:
        if not bool(_gcfg("GROWW_OPTION_STREAM_ENABLED", True)):
            logger.error("Groww session book rejected: option websocket disabled by configuration")
            return False
        self._stop_option_stream()
        try:
            hub = hub_for_api(self.api)
            self._live_hub = hub
            timeout = max(0.0, float(_gcfg("GROWW_OPTION_STREAM_FIRST_TICK_TIMEOUT_SEC", 12.0)))
            for choice in (book.call, book.put):
                key = self._snapshot_key(choice)
                identity = self._stream_identity_for_choice(choice)
                if not all((identity.get("stock_code"), identity.get("expiry"), identity.get("right"), identity.get("strike"))):
                    raise RuntimeError(f"incomplete Groww option websocket route for {getattr(choice, 'selected_symbol', key)}")
                event = threading.Event()
                snapshot = self._contract_snapshots.get(key, {})
                candles = {tf: deque(rows, maxlen=600) for tf, rows in (snapshot.get("candles") or {}).items()}
                for tf in ("1m", "5m", "15m", "1h", "4h", "1d"):
                    candles.setdefault(tf, deque(maxlen=600))
                self._book_stream_state[key] = {
                    "identity": identity,
                    "event": event,
                    "last_stream_tick_ts": 0.0,
                    "last_quote_ts": float(snapshot.get("last_quote_ts", 0.0) or 0.0),
                    "last_price": float(snapshot.get("last_price", 0.0) or 0.0),
                    "best_bid": float(snapshot.get("best_bid", 0.0) or 0.0),
                    "best_ask": float(snapshot.get("best_ask", 0.0) or 0.0),
                    "best_bid_qty": float(snapshot.get("best_bid_qty", 0.0) or 0.0),
                    "best_ask_qty": float(snapshot.get("best_ask_qty", 0.0) or 0.0),
                    "stream_pending": True,
                    "candles": candles,
                    "quote_tick_ts": 0.0,
                    "depth_tick_ts": 0.0,
                    "ohlcv_tick_ts": 0.0,
                    "last_tick_keys": (),
                }
                callback = lambda data, stream_key=key, stream_identity=identity: self._on_session_book_option_tick(stream_key, stream_identity, data)
                ids = hub.subscribe_option_quotes_and_ohlcv(
                    stock_code=identity["stock_code"],
                    expiry_date=identity["expiry"],
                    strike_price=str(identity["strike"]),
                    right=identity["right"],
                    callback=callback,
                )
                self._book_stream_subscription_ids[key] = list(ids)
                self._stream_subscription_ids.extend(ids)
            if bool(_gcfg("GROWW_OPTION_WEBSOCKET_REQUIRED", True)) and timeout > 0:
                deadline = time.time() + timeout
                missing = []
                for key, state in self._book_stream_state.items():
                    if not state["event"].wait(max(0.0, deadline - time.time())):
                        missing.append(key)
                if missing:
                    msg = f"no first live Groww option tick within {timeout:.1f}s for session vehicle(s) {missing}"
                    if bool(_gcfg("GROWW_SESSION_BOOK_REQUIRE_FIRST_OPTION_TICK_ON_STARTUP", False)):
                        raise RuntimeError(msg)
                    logger.warning(
                        "Groww CE/PE session vehicles subscribed but awaiting first option websocket tick: %s. "
                        "Analysis remains live; order activation stays blocked until the selected vehicle is fresh.",
                        msg,
                    )
                    return True
            logger.info("Groww CE/PE session vehicles websocket LIVE before signal execution; streamed_contracts=%d", len(self._book_stream_state))
            return True
        except Exception as exc:
            self._stop_option_stream()
            logger.error("Groww session contract book rejected: mandatory CE/PE websocket arming failed: %s", exc)
            return False

    def _start_selected_contract_stream(self) -> bool:
        if not bool(_gcfg("GROWW_OPTION_STREAM_ENABLED", True)):
            logger.error("Groww option websocket disabled by configuration for active execution vehicle")
            return False
        route = self._active_route()
        stock_code = str(route.get("stock_code") or route.get("ShortName") or "").upper()
        expiry = self._ws_expiry(route.get("expiry_date") or route.get("ExpiryDate") or "")
        right = self.api._normalise_right(route.get("right") or route.get("OptionType") or "")
        strike = str(route.get("strike_price") or route.get("StrikePrice") or "")
        if not all((stock_code, expiry, right, strike)):
            logger.error("Groww option websocket cannot subscribe: incomplete selected contract identity")
            return False
        self._stop_option_stream()
        try:
            hub = hub_for_api(self.api)
            self._live_hub = hub
            self._active_stream_contract = {"stock_code": stock_code, "expiry": expiry, "right": right, "strike": float(strike)}
            self._stream_armed_at = time.time()
            self._stream_subscription_ids = hub.subscribe_option_quotes_and_ohlcv(
                stock_code=stock_code,
                expiry_date=expiry,
                strike_price=strike,
                right=right,
                callback=self._on_option_stream_tick,
            )
            timeout = max(0.0, float(_gcfg("GROWW_OPTION_STREAM_FIRST_TICK_TIMEOUT_SEC", 12.0)))
            if bool(_gcfg("GROWW_OPTION_WEBSOCKET_REQUIRED", True)) and timeout > 0 and not self._first_stream_tick.wait(timeout):
                self._stop_option_stream()
                logger.error("Groww option websocket subscribed but produced no first tick within %.1fs for %s %s %s %s", timeout, stock_code, expiry, strike, right)
                return False
            logger.info("Groww option websocket LIVE for %s %s %s %s; live LTP is official Groww feed data", stock_code, expiry, strike, right)
            return True
        except Exception as exc:
            self._stop_option_stream()
            logger.error("Groww mandatory option websocket unavailable for selected vehicle: %s", exc)
            return False

    def _repair_option_stream_if_stale(self, reason: str, *, wait_key: tuple[str, str, float] | None = None) -> bool:
        if not bool(_gcfg("GROWW_OPTION_STREAM_ENABLED", True)) or not bool(_gcfg("GROWW_OPTION_WEBSOCKET_REQUIRED", True)):
            return False
        if not self._stream_subscription_ids or self._live_hub is None:
            return False
        session = groww_market_session_state()
        if not session.is_open and bool(_gcfg("GROWW_ANALYZE_ONLY_DURING_MARKET_SESSION", True)):
            return False
        now = time.time()
        max_stale = float(_gcfg("GROWW_OPTION_STREAM_MAX_STALE_SEC", 15.0))
        with self._lock:
            state = self._book_stream_state.get(wait_key, {}) if wait_key is not None else {}
            ts = float(state.get("last_stream_tick_ts", 0.0) or self._last_stream_tick_ts or 0.0)
            stream_age = now - ts if ts > 0 else None
        if stream_age is not None and stream_age <= max_stale:
            return False
        hub_connected = bool(getattr(self._live_hub, "connected", False))
        hub_ts = float(getattr(self._live_hub, "last_tick_ts", 0.0) or 0.0)
        shared_age = now - hub_ts if hub_ts > 0 else None
        if hub_connected and shared_age is not None and shared_age <= max_stale:
            return False
        cooldown = max(5.0, float(_gcfg("GROWW_WEBSOCKET_RECONNECT_COOLDOWN_SEC", 30.0)))
        with self._lock:
            if self._stream_repair_inflight or now - float(self._last_stream_repair_attempt or 0.0) < cooldown:
                return False
            self._stream_repair_inflight = True
            self._last_stream_repair_attempt = now
        repair = getattr(self._live_hub, "reconnect_and_resubscribe", None)
        if not callable(repair):
            with self._lock:
                self._stream_repair_inflight = False
            return False

        def _worker() -> None:
            try:
                ok = bool(repair(reason=reason))
                level = logger.warning if ok else logger.error
                level(
                    "Groww shared feed asynchronous repair %s; reason=%s option_age=%s shared_age=%s",
                    "triggered" if ok else "failed",
                    reason,
                    f"{stream_age:.1f}s" if stream_age is not None else "never",
                    f"{shared_age:.1f}s" if shared_age is not None else "never",
                )
            finally:
                with self._lock:
                    self._stream_repair_inflight = False

        threading.Thread(target=_worker, name="groww-feed-repair", daemon=True).start()
        return True

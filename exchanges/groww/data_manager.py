"""GROWW Groww option execution-vehicle data manager.

The selected CE/PE contract is streamed through the official Groww websocket.
REST is retained for historical warmup, session-book prechecks and periodic
reconciliation; it is not the source of live execution pricing once a vehicle
is activated. No synthetic prices or book depth are produced.
"""
from __future__ import annotations

import logging
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

try:
    import config
except Exception:  # pragma: no cover
    config = None  # type: ignore

from .api import GrowwRestClient
from .market_session import groww_market_session_state
from .rate_limiter import groww_throttle
from .live_feed import hub_for_api
from agents.groww_chain_architect import (
    chain_quality, is_chain_instrument, apply_contract_choice,
    build_session_contract_book, select_contract_from_session_book,
    eligible_nfo_master_option_rows, merge_verified_chain_quotes, contract_key,
    shortlist_contracts_for_stream_validation,
)
from core.instruments import normalise_symbol

logger = logging.getLogger(__name__)


def _cfg(name: str, default: Any) -> Any:
    if config is None:
        return default
    return getattr(config, name, default)


class GrowwOptionDataManager:
    def __init__(self, instrument=None, api: GrowwRestClient | None = None) -> None:
        self.instrument = instrument
        self.api = api or GrowwRestClient()
        self._strategy_ref = None
        self._running = False
        self._thread: threading.Thread | None = None
        # Generation token prevents an old CE polling thread from resuming after
        # a rapid flat -> PE activation (or vice versa).  A shared boolean alone
        # is unsafe because it can be set True again before the retired thread wakes.
        self._poll_generation = 0
        self._lock = threading.RLock()
        self._last_price = 0.0
        self._last_quote_ts = 0.0
        self._best_bid = 0.0
        self._best_ask = 0.0
        self._best_bid_qty = 0.0
        self._best_ask_qty = 0.0
        self._candles: dict[str, deque] = {tf: deque(maxlen=600) for tf in ("1m", "5m", "15m", "1h", "4h", "1d")}
        self._trades: deque = deque(maxlen=500)
        self.is_ready = False
        self._selected_contract = None
        self._contract_snapshots: dict[tuple[str, str, float], dict[str, Any]] = {}
        self._session_book_last_refresh_ts = 0.0
        self._session_book_last_refresh_attempt_ts = 0.0
        self._live_hub = None
        # The day-start CE/PE book is pre-streamed so a signal never waits for
        # a new option subscription before execution pricing is available.
        self._stream_subscription_ids: list[str] = []
        self._book_stream_subscription_ids: dict[tuple[str, str, float], list[str]] = {}
        self._book_stream_state: dict[tuple[str, str, float], dict[str, Any]] = {}
        self._active_stream_key: tuple[str, str, float] | None = None
        self._active_stream_contract: dict[str, Any] = {}
        self._last_stream_tick_ts = 0.0
        # LTP and market depth are distinct official Groww feeds.  Track both
        # independently so a fresh LTP cannot keep a stale order book executable.
        self._last_ltp_stream_ts = 0.0
        self._last_depth_stream_ts = 0.0
        self._stream_armed_at = 0.0
        self._first_stream_tick = threading.Event()
        self._last_stream_repair_attempt = 0.0
        self._stream_repair_inflight = False
        self._last_execution_quote_source = "NONE"
        # Stream-route telemetry is execution critical: the underlying quote may
        # be live while CE/PE packets are missing or cannot be unambiguously
        # associated with either preselected vehicle.
        self._stream_tick_observed_count = 0
        self._stream_unroutable_tick_count = 0
        self._stream_unroutable_last_keys: tuple[str, ...] = ()
        self._stream_unroutable_last_ts = 0.0
        self._stream_last_route_warning_ts = 0.0
        self._last_identity_reject_log_ts = 0.0
        self._last_stream_discovery_diagnostics: dict[str, Any] = {}
        self._last_session_available_funds = 0.0
        self._last_session_underlying_spot = 0.0
        self._underlying_route_fields = self._route_field_snapshot()
        logger.info("GrowwOptionDataManager initialised [%s]", getattr(instrument, "asset_id", "GROWW"))

    def _is_chain_mode(self) -> bool:
        return is_chain_instrument(self.instrument)

    def _hydrate_chain_candidates(self, *, force_refresh: bool = False, underlying_spot: float = 0.0) -> bool:
        """Populate the session option universe from official Groww data only.

        Contract identity, exchange token and lot size come from Groww's official
        instrument CSV; live option metrics and Greeks come from the documented
        ``get_option_chain`` endpoint.  If either source is unavailable, the
        option desk is fail-closed rather than substituting quote probes.
        """
        raw = getattr(getattr(self.instrument, "primary", None), "raw", {}) or {}
        if not isinstance(raw, dict):
            return False
        if raw.get("chain_candidates") and not force_refresh:
            return True
        stock_code = str(raw.get("groww_stock_code") or raw.get("stock_code") or getattr(self.instrument, "asset_id", "")).upper()
        if not stock_code:
            return False
        try:
            self.api.preflight_session()
            master_rows = self.api.get_security_master_rows(
                cache_path=str(_cfg("GROWW_SECURITY_MASTER_CACHE_PATH", "data/groww_security_master.zip")),
                require_current_trade_date=bool(_cfg("GROWW_SECURITY_MASTER_REQUIRE_TODAY", True)),
            )
            verified = eligible_nfo_master_option_rows(master_rows, stock_code)
        except Exception as exc:
            logger.error("GROWW session contract book failed: Security Master unavailable for %s: %s", stock_code, exc)
            return False
        if not verified:
            logger.error("GROWW session contract book failed: no verified NFO option definitions with lot size for %s", stock_code)
            return False
        expiries = sorted({contract_key(row)[0] for row in verified if contract_key(row)[0]})
        max_expiries = max(1, int(_cfg("GROWW_SESSION_BOOK_MAX_EXPIRIES", 2)))
        expiries = expiries[:max_expiries]
        verified = [row for row in verified if contract_key(row)[0] in set(expiries)]
        quote_rows: list[dict[str, Any]] = []
        for expiry in expiries:
            try:
                groww_throttle(f"option_chain:{stock_code}:{expiry}")
                resp = self.api.get_option_chain_quotes(
                    stock_code=stock_code, exchange_code="NFO", product_type="options",
                    expiry_date=self.api._normalise_expiry(expiry),
                )
                quote_rows.extend(dict(row) for row in self._chain_rows(resp) if isinstance(row, dict))
            except Exception as exc:
                logger.error("GROWW official option-chain fetch failed %s %s: %s", stock_code, expiry, exc)
                return False
        candidates = merge_verified_chain_quotes(verified, quote_rows)
        chain_source = "official_instrument_csv_plus_get_option_chain"
        if not candidates:
            logger.error("GROWW session contract book failed: official get_option_chain returned no executable contracts matching instrument CSV for %s", stock_code)
            return False
        raw["chain_candidates"] = candidates
        raw["chain_candidates_deferred"] = False
        raw["chain_quality"] = chain_quality(candidates)
        raw["chain_source"] = chain_source
        logger.info(
            "GROWW verified option-chain hydrated for %s: rows=%d expiries=%s source=%s",
            stock_code, len(candidates), ",".join(expiries), chain_source,
        )
        return True

    @staticmethod
    def _chain_rows(resp: Dict[str, Any]) -> list[Any]:
        rows = resp.get("Success") or resp.get("data") or resp.get("result") or [] if isinstance(resp, dict) else []
        if isinstance(rows, dict):
            for key in ("data", "candles", "option_chain", "OptionChain", "records"):
                value = rows.get(key)
                if isinstance(value, list):
                    return value
            rows = list(rows.values())
        return rows if isinstance(rows, list) else []

    @staticmethod
    def _first_response_row(resp: Dict[str, Any]) -> dict[str, Any]:
        rows = GrowwOptionDataManager._chain_rows(resp)
        if rows and isinstance(rows[0], dict):
            return dict(rows[0])
        if isinstance(resp, dict):
            row = resp.get("Success") or resp.get("data") or resp.get("result") or resp
            if isinstance(row, dict):
                return dict(row)
        return {}

    @staticmethod
    def _snapshot_key(choice) -> tuple[str, str, float]:
        return str(choice.expiry), str(choice.right), round(float(choice.strike), 6)

    def _route_field_snapshot(self) -> dict[str, Any]:
        raw = getattr(getattr(self.instrument, "primary", None), "raw", {}) or {}
        fields = ("selected_option_contract", "stock_code", "exchange_code", "product_type", "right", "option_type",
                  "strike_price", "expiry_date", "TradingSymbol", "runtime_lot_size", "selected_entry_premium", "selected_contract_cost",
                  "selected_live_contract_cost", "selected_live_spread_bps", "selected_live_visible_depth",
                  "selected_live_premium_atr_1m", "selected_live_spread_to_atr")
        return {field: raw.get(field, None) for field in fields}

    def _restore_route_fields(self, snapshot: dict[str, Any]) -> None:
        raw = getattr(getattr(self.instrument, "primary", None), "raw", {}) or {}
        for field, value in snapshot.items():
            if value is None:
                raw.pop(field, None)
            else:
                raw[field] = value

    def _clear_option_execution_state(self) -> None:
        with self._lock:
            self._last_price = 0.0; self._last_quote_ts = 0.0; self._best_bid = 0.0; self._best_ask = 0.0; self._best_bid_qty = 0.0; self._best_ask_qty = 0.0
            self._last_stream_tick_ts = 0.0
            self._last_ltp_stream_ts = 0.0
            self._last_depth_stream_ts = 0.0
            self._candles = {tf: deque(maxlen=600) for tf in ("1m", "5m", "15m", "1h", "4h", "1d")}
            self._trades.clear()
            self._first_stream_tick.clear()

    def _active_route(self) -> dict[str, Any]:
        raw = getattr(getattr(self.instrument, "primary", None), "raw", {}) or {}
        selected = raw.get("selected_option_contract") if isinstance(raw, dict) else None
        if isinstance(selected, dict) and isinstance(selected.get("raw"), dict):
            return dict(selected.get("raw") or {})
        return dict(raw) if isinstance(raw, dict) else {}

    @staticmethod
    def _ws_expiry(value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S.000Z", "%Y-%m-%dT%H:%M:%SZ"):
            try:
                return datetime.strptime(text, fmt).strftime("%d-%b-%Y")
            except Exception:
                continue
        return text

    def _stop_option_stream(self) -> None:
        try:
            if self._live_hub is not None and self._stream_subscription_ids:
                self._live_hub.unsubscribe(list(self._stream_subscription_ids))
        except Exception:
            pass
        self._stream_subscription_ids = []
        self._book_stream_subscription_ids = {}
        self._book_stream_state = {}
        self._active_stream_key = None
        self._active_stream_contract = {}
        self._last_stream_tick_ts = 0.0
        self._first_stream_tick.clear()

    def _stream_identity_for_choice(self, choice) -> dict[str, Any]:
        raw = dict(getattr(choice, "raw", {}) or {})
        stock_code = str(raw.get("stock_code") or raw.get("ShortName") or getattr(self.instrument, "asset_id", "")).upper()
        expiry = self._ws_expiry(raw.get("expiry_date") or raw.get("ExpiryDate") or getattr(choice, "expiry", ""))
        right = self.api._normalise_right(raw.get("right") or raw.get("OptionType") or getattr(choice, "right", ""))
        strike = float(raw.get("strike_price") or raw.get("StrikePrice") or getattr(choice, "strike", 0.0) or 0.0)
        return {"stock_code": stock_code, "expiry": expiry, "right": right, "strike": strike}

    def _matches_option_identity_tick(self, row: Dict[str, Any], identity: dict[str, Any]) -> bool:
        """Accept a streamed premium tick only when it proves exact contract identity.

        The shared Groww hub fans each tick to both CE/PE vehicle callbacks, so
        merely seeing exchange=NFO is never sufficient. Stock, right and strike
        must match; expiry is additionally verified whenever the Groww tick
        provides it. Accepting an ambiguous tick could stamp both vehicles fresh
        with the same premium and route an order on stale/wrong execution data.
        """
        if not identity:
            return False
        expected_stock = str(identity.get("stock_code", "") or "").upper().replace(" ", "")
        expected_expiry = self._ws_expiry(identity.get("expiry", ""))
        # Canonicalise both sides through the same Groww right normaliser.
        # The SDK emits title-case values ("Call"/"Put"); comparing a
        # lower-cased expected value against an un-normalised received value
        # rejects a valid tick even when all contract fields match.
        expected_right = str(self.api._normalise_right(identity.get("right", "")) or "").casefold()
        expected_strike = float(identity.get("strike", 0.0) or 0.0)
        if not all((expected_stock, expected_expiry, expected_right, expected_strike > 0)):
            return False
        exchange = str(row.get("exchange_code") or row.get("exchange") or "").upper()
        product = str(row.get("product_type") or row.get("product") or "").upper()
        is_nfo_option = bool(
            exchange == "NFO" or "FUTURES & OPTIONS" in exchange or "FUTURES&OPTIONS" in exchange
            or product in {"OPTION", "OPTIONS"}
        )
        if not is_nfo_option:
            return False
        stock = str(row.get("stock_code") or row.get("stock_name") or "").upper().replace(" ", "")
        if not stock or expected_stock not in stock:
            return False
        expiry_val = self._ws_expiry(row.get("expiry_date") or row.get("expiry") or row.get("ExpiryDate") or "")
        # Groww quote/depth ticks may omit expiry even when the subscribed
        # route included it; when present it must match. Stock+right+strike are
        # always required so a broad NFO tick cannot refresh every vehicle.
        if expiry_val and expiry_val != expected_expiry:
            return False
        right_val = str(self.api._normalise_right(row.get("right") or row.get("right_type") or row.get("option_type") or "") or "").casefold()
        if not right_val or right_val != expected_right:
            return False
        strike_val = self._float_first(row, ("strike_price", "strike", "StrikePrice"))
        if strike_val <= 0 or abs(strike_val - expected_strike) > 1e-6:
            return False
        return True

    def _arm_session_book_streams(self, book) -> bool:
        """Stream both preselected CE/PE contracts before the scan may enter."""
        if not bool(_cfg("GROWW_OPTION_STREAM_ENABLED", True)):
            logger.error("GROWW session book rejected: option websocket disabled by configuration")
            return False
        self._stop_option_stream()
        try:
            hub = hub_for_api(self.api)
            self._live_hub = hub
            timeout = max(0.0, float(_cfg("GROWW_OPTION_STREAM_FIRST_TICK_TIMEOUT_SEC", 12.0)))
            for choice in (book.call, book.put):
                key = self._snapshot_key(choice)
                identity = self._stream_identity_for_choice(choice)
                if not all((identity.get("stock_code"), identity.get("expiry"), identity.get("right"), identity.get("strike"))):
                    raise RuntimeError(f"incomplete option websocket route for {getattr(choice, 'selected_symbol', key)}")
                event = threading.Event()
                snapshot = self._contract_snapshots.get(key, {})
                candles = {tf: deque(rows, maxlen=600) for tf, rows in (snapshot.get("candles") or {}).items()}
                for tf in ("1m", "5m", "15m", "1h", "4h", "1d"):
                    candles.setdefault(tf, deque(maxlen=600))
                self._book_stream_state[key] = {
                    "identity": identity, "event": event, "last_stream_tick_ts": 0.0,
                    "last_quote_ts": float(snapshot.get("last_quote_ts", 0.0) or 0.0),
                    "last_price": float(snapshot.get("last_price", 0.0) or 0.0),
                    "best_bid": float(snapshot.get("best_bid", 0.0) or 0.0),
                    "best_ask": float(snapshot.get("best_ask", 0.0) or 0.0),
                    "best_bid_qty": float(snapshot.get("best_bid_qty", 0.0) or 0.0),
                    "best_ask_qty": float(snapshot.get("best_ask_qty", 0.0) or 0.0),
                    "stream_pending": True, "candles": candles,
                    "quote_tick_ts": 0.0, "depth_tick_ts": 0.0, "ohlcv_tick_ts": 0.0,
                    "last_tick_keys": (),
                }
                callback = lambda data, stream_key=key, stream_identity=identity: self._on_session_book_option_tick(stream_key, stream_identity, data)
                ids = hub.subscribe_option_market_data(stock_code=identity["stock_code"], expiry_date=identity["expiry"], strike_price=str(identity["strike"]), right=identity["right"], callback=callback)
                self._book_stream_subscription_ids[key] = list(ids)
                self._stream_subscription_ids.extend(ids)
            if bool(_cfg("GROWW_OPTION_WEBSOCKET_REQUIRED", True)) and timeout > 0:
                deadline = time.time() + timeout
                missing = []
                for key, state in self._book_stream_state.items():
                    if not state["event"].wait(max(0.0, deadline - time.time())):
                        missing.append(key)
                if missing:
                    msg = f"no first live option tick within {timeout:.1f}s for session vehicle(s) {missing}"
                    if bool(_cfg("GROWW_SESSION_BOOK_REQUIRE_FIRST_OPTION_TICK_ON_STARTUP", False)):
                        raise RuntimeError(msg)
                    logger.warning(
                        "GROWW CE/PE session vehicles subscribed but awaiting first option websocket tick: %s. "
                        "NIFTY analysis remains live; order activation stays blocked until the selected vehicle is fresh.",
                        msg,
                    )
                    return True
            logger.info("GROWW CE/PE session vehicles subscribed through official LTP+market-depth feeds; entry activation requires fresh LTP and depth for the selected vehicle; streamed_contracts=%d", len(self._book_stream_state))
            return True
        except Exception as exc:
            self._stop_option_stream()
            logger.error("GROWW session contract book rejected: mandatory CE/PE websocket arming failed: %s", exc)
            return False

    def _on_session_book_option_tick(self, key: tuple[str, str, float], identity: dict[str, Any], data: Any) -> None:
        row = data[0] if isinstance(data, list) and data and isinstance(data[0], dict) else data
        if not isinstance(row, dict):
            return
        self._stream_tick_observed_count += 1
        if not self._matches_option_identity_tick(row, identity):
            # Fail closed: a broadcast/multiplexed or malformed NFO tick must
            # never clear execution freshness for the selected CE/PE vehicle.
            now = time.time()
            appears_option_tick = any(k in row for k in ("last", "ltp", "last_price", "close", "price", "bPrice", "sPrice", "best_bid", "best_ask", "interval"))
            exchange = str(row.get("exchange_code") or row.get("exchange") or "").upper()
            px_probe = self._float_first(row, ("last", "ltp", "last_price", "close", "price", "Close", "c"))
            option_like = bool(
                "NFO" in exchange or row.get("expiry_date") or row.get("strike_price")
                or row.get("right_type") or row.get("right") or row.get("option_type")
            )
            if appears_option_tick:
                self._stream_unroutable_tick_count += 1
                self._stream_unroutable_last_keys = tuple(sorted(str(k) for k in row.keys()))
                self._stream_unroutable_last_ts = now
            if ((appears_option_tick and (self._stream_unroutable_tick_count == 1 or now - self._stream_last_route_warning_ts >= 60.0))
                    or (option_like and px_probe > 0 and now - float(self._last_identity_reject_log_ts or 0.0) >= 60.0)):
                self._stream_last_route_warning_ts = now
                self._last_identity_reject_log_ts = now
                logger.warning(
                    "GROWW option tick rejected by contract identity guard; execution remains blocked. "
                    "count=%d expected=%s received={exchange=%s stock=%s expiry=%s strike=%s right=%s} keys=%s",
                    self._stream_unroutable_tick_count, identity, exchange,
                    str(row.get("stock_code") or row.get("stock_name") or ""), str(row.get("expiry_date") or ""),
                    str(row.get("strike_price") or row.get("strike") or ""),
                    str(row.get("right") or row.get("right_type") or row.get("option_type") or ""),
                    ",".join(self._stream_unroutable_last_keys),
                )
            return
        px = self._float_first(row, ("last", "ltp", "last_price", "close", "price", "Close", "c"))
        bid, ask, bid_qty, ask_qty = self._extract_top_of_book(row)
        if px <= 0 and not any(v > 0 for v in (bid, ask, bid_qty, ask_qty)):
            return
        now = time.time()
        state = self._book_stream_state.get(key)
        if state is None:
            return
        state["last_stream_tick_ts"] = now
        state["last_tick_keys"] = tuple(sorted(str(k) for k in row.keys()))
        interval = str(row.get("interval") or row.get("Interval") or "").lower()
        if interval in {"1minute", "1min", "1m"}:
            state["ohlcv_tick_ts"] = now
        if any(v > 0 for v in (bid, ask, bid_qty, ask_qty)):
            state["depth_tick_ts"] = now
        if px > 0 and interval not in {"1minute", "1min", "1m"}:
            state["quote_tick_ts"] = now
        if px > 0:
            state["last_price"] = px
        if bid > 0:
            state["best_bid"] = bid
        if ask > 0:
            state["best_ask"] = ask
        if bid_qty > 0:
            state["best_bid_qty"] = bid_qty
        if ask_qty > 0:
            state["best_ask_qty"] = ask_qty
        state["stream_pending"] = False
        if px > 0 and interval in {"1minute", "1min", "1m"}:
            o = self._float_first(row, ("open", "Open", "o")) or px
            h = self._float_first(row, ("high", "High", "h")) or px
            l = self._float_first(row, ("low", "Low", "l")) or px
            v = self._float_first(row, ("volume", "Volume", "v"))
            ts = row.get("datetime") or row.get("ltt") or row.get("time") or row.get("t") or now
            candle = self._canonical_option_candle(ts, o, h, l, px, v)
            with self._lock:
                self._upsert_option_live_candle("1m", candle, store=state["candles"])
                self._aggregate_option_live_frames(int(candle["t"]), store=state["candles"])
        ltp_ready = bool(state.get("quote_tick_ts", 0.0) and float(state.get("last_price", 0.0) or 0.0) > 0)
        depth_ready = bool(
            state.get("depth_tick_ts", 0.0)
            and float(state.get("best_bid", 0.0) or 0.0) > 0
            and float(state.get("best_ask", 0.0) or 0.0) >= float(state.get("best_bid", 0.0) or 0.0)
            and float(state.get("best_bid_qty", 0.0) or 0.0) > 0
            and float(state.get("best_ask_qty", 0.0) or 0.0) > 0
        )
        if ltp_ready and depth_ready:
            state["event"].set()
        if self._active_stream_key == key:
            self._active_stream_contract = dict(identity)
            self._on_option_stream_tick(data)

    def _start_selected_contract_stream(self) -> bool:
        if not bool(_cfg("GROWW_OPTION_STREAM_ENABLED", True)):
            logger.error("GROWW option websocket disabled by configuration for active execution vehicle")
            return False
        route = self._active_route()
        stock_code = str(route.get("stock_code") or route.get("ShortName") or "").upper()
        expiry = self._ws_expiry(route.get("expiry_date") or route.get("ExpiryDate") or "")
        right = self.api._normalise_right(route.get("right") or route.get("OptionType") or "")
        strike = str(route.get("strike_price") or route.get("StrikePrice") or "")
        if not all((stock_code, expiry, right, strike)):
            logger.error("GROWW option websocket cannot subscribe: incomplete selected contract identity")
            return False
        self._stop_option_stream()
        try:
            hub = hub_for_api(self.api)
            self._live_hub = hub
            self._active_stream_contract = {"stock_code": stock_code, "expiry": expiry, "right": right, "strike": float(strike)}
            self._stream_armed_at = time.time()
            self._stream_subscription_ids = hub.subscribe_option_market_data(
                stock_code=stock_code, expiry_date=expiry, strike_price=strike, right=right, callback=self._on_option_stream_tick)
            timeout = max(0.0, float(_cfg("GROWW_OPTION_STREAM_FIRST_TICK_TIMEOUT_SEC", 12.0)))
            if bool(_cfg("GROWW_OPTION_WEBSOCKET_REQUIRED", True)) and timeout > 0 and not self._first_stream_tick.wait(timeout):
                self._stop_option_stream()
                logger.error("GROWW option websocket subscribed but produced no first tick within %.1fs for %s %s %s %s", timeout, stock_code, expiry, strike, right)
                return False
            logger.info("GROWW option websocket LIVE for %s %s %s %s; official LTP+market-depth are primary execution data", stock_code, expiry, strike, right)
            return True
        except Exception as exc:
            self._stop_option_stream()
            logger.error("GROWW mandatory option websocket unavailable for selected vehicle: %s", exc)
            return False

    def _repair_option_stream_if_stale(self, reason: str, *, wait_key: tuple[str, str, float] | None = None) -> bool:
        """Repair only a failed shared transport; never block the strategy loop.

        A quiet CE/PE contract is not evidence that the Groww socket is broken.
        If the shared Groww transport is still receiving ticks, reconnecting it in
        the execution path destroys good analysis data and creates latency. The
        mandatory selected-contract LTP/depth stream remains fail-closed until fresh.
        """
        if not bool(_cfg("GROWW_OPTION_STREAM_ENABLED", True)) or not bool(_cfg("GROWW_OPTION_WEBSOCKET_REQUIRED", True)):
            return False
        if not self._stream_subscription_ids or self._live_hub is None:
            return False
        session = groww_market_session_state()
        if not session.is_open and bool(_cfg("GROWW_ANALYZE_ONLY_DURING_MARKET_SESSION", True)):
            return False
        now = time.time()
        max_stale = float(_cfg("GROWW_OPTION_STREAM_MAX_STALE_SEC", 15.0))
        with self._lock:
            state = self._book_stream_state.get(wait_key, {}) if wait_key is not None else {}
            ts = float(state.get("last_stream_tick_ts", 0.0) or self._last_stream_tick_ts or 0.0)
            stream_age = now - ts if ts > 0 else None
        if stream_age is not None and stream_age <= max_stale:
            return False

        hub_connected = bool(getattr(self._live_hub, "connected", False))
        hub_ts = float(getattr(self._live_hub, "last_tick_ts", 0.0) or 0.0)
        shared_age = now - hub_ts if hub_ts > 0 else None
        shared_max = max(max_stale, float(_cfg("GROWW_SHARED_TRANSPORT_MAX_STALE_SEC", 20.0)))
        if hub_connected and shared_age is not None and shared_age <= shared_max:
            if now - float(self._stream_last_route_warning_ts or 0.0) >= 60.0:
                self._stream_last_route_warning_ts = now
                logger.info(
                    "GROWW selected option has no recent routed tick but shared Groww transport is live "
                    "(shared_age=%.2fs); no socket recycle on strategy path; execution remains blocked until documented option feed is fresh",
                    shared_age,
                )
            return False

        cooldown = max(5.0, float(_cfg("GROWW_WEBSOCKET_RECONNECT_COOLDOWN_SEC", 30.0)))
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
                    "GROWW shared websocket asynchronous repair %s; reason=%s option_age=%s shared_age=%s",
                    "triggered" if ok else "failed", reason,
                    f"{stream_age:.1f}s" if stream_age is not None else "never",
                    f"{shared_age:.1f}s" if shared_age is not None else "never",
                )
            finally:
                with self._lock:
                    self._stream_repair_inflight = False
        threading.Thread(target=_worker, name="groww-ws-repair", daemon=True).start()
        return True

    def _matches_active_option_tick(self, row: Dict[str, Any]) -> bool:
        return self._matches_option_identity_tick(row, self._active_stream_contract)

    @staticmethod
    def _compact_key(value: Any) -> str:
        return "".join(ch.lower() for ch in str(value) if ch.isalnum())

    @classmethod
    def _float_key_like(
        cls,
        row: Dict[str, Any],
        required: tuple[str, ...],
        *,
        any_groups: tuple[tuple[str, ...], ...] = (),
        forbidden: tuple[str, ...] = (),
    ) -> float:
        for key, value in row.items():
            compact = cls._compact_key(key)
            if not all(token in compact for token in required):
                continue
            if any(token in compact for token in forbidden):
                continue
            if any_groups and not all(any(token in compact for token in group) for group in any_groups):
                continue
            try:
                parsed = float(value or 0.0)
                if parsed > 0:
                    return parsed
            except Exception:
                continue
        return 0.0

    def _extract_top_of_book(self, row: Dict[str, Any]) -> tuple[float, float, float, float]:
        bid = self._float_first(row, ("best_bid_price", "best_bid", "bid", "bPrice", "bid_price"))
        ask = self._float_first(row, ("best_offer_price", "best_ask_price", "best_ask", "ask", "sPrice", "ask_price", "offer_price"))
        bid_qty = self._float_first(row, ("best_bid_quantity", "bid_quantity", "bid_qty", "bQty"))
        ask_qty = self._float_first(row, ("best_offer_quantity", "offer_quantity", "best_ask_quantity", "ask_quantity", "ask_qty", "sQty"))
        if bid > 0 and ask > 0 and bid_qty > 0 and ask_qty > 0:
            return bid, ask, bid_qty, ask_qty
        depth = row.get("depth") or row.get("Depth") or row.get("market_depth") or row.get("MarketDepth")
        levels: list[Dict[str, Any]] = []
        if isinstance(depth, list):
            levels = [level for level in depth if isinstance(level, dict)]
        elif isinstance(depth, dict):
            levels = [depth]
        for level in levels:
            if bid <= 0:
                bid = self._float_key_like(level, ("buy", "1"), any_groups=(("rate", "price"),), forbidden=("qty", "quantity", "orders", "flag"))
            if ask <= 0:
                ask = self._float_key_like(level, ("sell", "1"), any_groups=(("rate", "price"),), forbidden=("qty", "quantity", "orders", "flag"))
            if bid_qty <= 0:
                bid_qty = self._float_key_like(level, ("buy", "1"), any_groups=(("qty", "quantity"),), forbidden=("rate", "price", "orders", "flag"))
            if ask_qty <= 0:
                ask_qty = self._float_key_like(level, ("sell", "1"), any_groups=(("qty", "quantity"),), forbidden=("rate", "price", "orders", "flag"))
            if bid > 0 and ask > 0 and bid_qty > 0 and ask_qty > 0:
                break
        return bid, ask, bid_qty, ask_qty

    def _on_option_stream_tick(self, data: Any) -> None:
        try:
            row = data[0] if isinstance(data, list) and data and isinstance(data[0], dict) else data
            if not isinstance(row, dict) or not self._matches_active_option_tick(row):
                return
            px = self._float_first(row, ("last", "ltp", "last_price", "close", "price", "Close", "c"))
            bid, ask, bid_qty, ask_qty = self._extract_top_of_book(row)
            if px <= 0 and not any(v > 0 for v in (bid, ask, bid_qty, ask_qty)):
                return
            now = time.time()
            interval = str(row.get("interval") or row.get("Interval") or "").lower()
            v = self._float_first(row, ("volume", "Volume", "v")) if interval in {"1minute", "1min", "1m"} else 0.0
            candle = None
            if px > 0 and interval in {"1minute", "1min", "1m"}:
                # Only the official OHLCV interval feed may populate premium bars.
                # Quote payload open/high/low are session fields and must never be
                # interpreted as one-minute premium structure.
                o = self._float_first(row, ("open", "Open", "o")) or px
                h = self._float_first(row, ("high", "High", "h")) or px
                l = self._float_first(row, ("low", "Low", "l")) or px
                ts = row.get("datetime") or row.get("ltt") or row.get("time") or row.get("t") or now
                candle = self._canonical_option_candle(ts, o, h, l, px, v)
            with self._lock:
                if candle is not None:
                    self._upsert_option_live_candle("1m", candle)
                    self._aggregate_option_live_frames(int(candle["t"]))
                self._last_quote_ts = now
                self._last_stream_tick_ts = now
                if px > 0:
                    self._last_ltp_stream_ts = now
                    self._last_price = px
                if bid > 0 and ask >= bid and bid_qty > 0 and ask_qty > 0:
                    self._last_depth_stream_ts = now
                if bid > 0: self._best_bid = bid
                if ask > 0: self._best_ask = ask
                if bid_qty > 0: self._best_bid_qty = bid_qty
                if ask_qty > 0: self._best_ask_qty = ask_qty
                if px > 0:
                    self._trades.append({"price": px, "quantity": v, "side": "buy", "timestamp": now, "source": "groww_websocket"})
            if px > 0 or (bid > 0 and ask > 0):
                self._first_stream_tick.set()
            if px > 0 and self._strategy_ref is not None:
                callback = getattr(self._strategy_ref, "_on_realtime_quote", None)
                if callable(callback):
                    callback(px)
        except Exception as exc:
            logger.debug("GROWW option websocket tick rejected: %s", exc)

    def _canonical_option_candle(self, ts: Any, o: float, h: float, l: float, c: float, v: float = 0.0) -> dict[str, Any]:
        ts_ms = self._parse_ts_ms(ts)
        interval_ms = 60 * 1000
        ts_ms = (ts_ms // interval_ms) * interval_ms
        high = max(float(h or c), float(o or c), float(c))
        low = min(float(l or c), float(o or c), float(c))
        return {"t": ts_ms, "timestamp": ts_ms / 1000.0, "o": float(o or c), "open": float(o or c), "h": high, "high": high, "l": low, "low": low, "c": float(c), "close": float(c), "v": float(v or 0.0), "volume": float(v or 0.0)}

    def _upsert_option_live_candle(self, timeframe: str, candle: dict[str, Any], store: dict[str, deque] | None = None) -> None:
        target = (self._candles if store is None else store)[timeframe]
        ts = int(candle.get("t", 0) or 0)
        if target and int(target[-1].get("t", 0) or 0) == ts:
            previous = target[-1]
            candle = dict(candle)
            candle["o"] = candle["open"] = float(previous.get("o", previous.get("open", candle["o"])) or candle["o"])
            candle["h"] = candle["high"] = max(float(previous.get("h", previous.get("high", candle["h"])) or candle["h"]), float(candle["h"]))
            candle["l"] = candle["low"] = min(float(previous.get("l", previous.get("low", candle["l"])) or candle["l"]), float(candle["l"]))
            candle["v"] = candle["volume"] = max(float(previous.get("v", 0.0) or 0.0), float(candle.get("v", 0.0) or 0.0))
            target[-1] = candle
        elif not target or ts > int(target[-1].get("t", 0) or 0):
            target.append(candle)

    def _aggregate_option_live_frames(self, timestamp_ms: int, store: dict[str, deque] | None = None) -> None:
        candle_store = self._candles if store is None else store
        rows = list(candle_store.get("1m", ()))
        for timeframe, minutes in (("5m", 5), ("15m", 15), ("1h", 60)):
            bucket_ms = minutes * 60000; bucket = (timestamp_ms // bucket_ms) * bucket_ms
            chunk = [r for r in rows if bucket <= int(r.get("t", 0) or 0) < bucket + bucket_ms]
            if not chunk:
                continue
            agg = {"t": bucket, "timestamp": bucket / 1000.0, "o": float(chunk[0]["o"]), "open": float(chunk[0]["o"]), "h": max(float(r["h"]) for r in chunk), "high": max(float(r["h"]) for r in chunk), "l": min(float(r["l"]) for r in chunk), "low": min(float(r["l"]) for r in chunk), "c": float(chunk[-1]["c"]), "close": float(chunk[-1]["c"]), "v": sum(float(r.get("v", 0.0) or 0.0) for r in chunk), "volume": sum(float(r.get("v", 0.0) or 0.0) for r in chunk)}
            self._upsert_option_live_candle(timeframe, agg, store=candle_store)

    def _option_premium_atr(self, timeframe: str = "1m", period: int = 14) -> float:
        """ATR on the execution vehicle premium, not the NIFTY underlying."""
        with self._lock:
            rows = list(self._candles.get(timeframe, ()))
        trs: list[float] = []
        prev_close = 0.0
        for candle in rows[-(period + 1):]:
            try:
                high = float(candle.get("h", candle.get("high", 0.0)) or 0.0)
                low = float(candle.get("l", candle.get("low", 0.0)) or 0.0)
                close = float(candle.get("c", candle.get("close", 0.0)) or 0.0)
            except Exception:
                continue
            if high <= 0 or low <= 0 or close <= 0 or high < low:
                continue
            tr = max(high - low, abs(high - prev_close) if prev_close > 0 else 0.0, abs(low - prev_close) if prev_close > 0 else 0.0)
            if tr > 0:
                trs.append(tr)
            prev_close = close
        return sum(trs[-period:]) / len(trs[-period:]) if trs else 0.0

    def _live_execution_liquidity_check(self, choice, *, phase: str) -> tuple[bool, dict[str, float]]:
        """Validate executable spread/depth relative to premium volatility.

        A static spread ceiling blocks clearly bad quotes.  Spread-to-ATR adds a
        dynamic execution-cost test: the bid/offer cost may not consume an
        excessive portion of recent option-premium movement.
        """
        lot = float(choice.raw.get("runtime_lot_size", 0.0) or 0.0)
        bid = float(self._best_bid or 0.0); ask = float(self._best_ask or 0.0)
        bid_qty = float(self._best_bid_qty or 0.0); ask_qty = float(self._best_ask_qty or 0.0)
        if lot <= 0 or bid <= 0 or ask < bid:
            logger.warning("GROWW %s vehicle rejected: invalid executable quote/lot symbol=%s", phase, choice.selected_symbol)
            return False, {}
        spread = ask - bid
        spread_bps = spread / max((ask + bid) / 2.0, 1e-9) * 10000.0
        max_spread_bps = float(_cfg("GROWW_OPTION_MAX_SELECTION_SPREAD_BPS", 120.0))
        if spread_bps > max_spread_bps:
            logger.warning("GROWW %s vehicle rejected: spread %.1fbps exceeds %.1fbps symbol=%s", phase, spread_bps, max_spread_bps, choice.selected_symbol)
            return False, {"spread_bps": spread_bps}
        min_depth = lot * max(0.0, float(_cfg("GROWW_OPTION_MIN_BOOK_LOTS", 1.0)))
        visible_depth = min(bid_qty, ask_qty)
        if visible_depth < min_depth:
            logger.warning("GROWW %s vehicle rejected: visible depth %.0f below lot requirement %.0f symbol=%s", phase, visible_depth, min_depth, choice.selected_symbol)
            return False, {"spread_bps": spread_bps, "visible_depth": visible_depth}
        premium_atr = self._option_premium_atr("1m", period=int(_cfg("GROWW_OPTION_EXECUTION_ATR_PERIOD", 14)))
        if premium_atr <= 0:
            logger.warning("GROWW %s vehicle rejected: option-premium 1m ATR unavailable symbol=%s", phase, choice.selected_symbol)
            return False, {"spread_bps": spread_bps, "visible_depth": visible_depth}
        spread_to_atr = spread / premium_atr
        max_ratio = float(_cfg("GROWW_OPTION_MAX_SPREAD_TO_1M_ATR", 0.35))
        if spread_to_atr > max_ratio:
            logger.warning("GROWW %s vehicle rejected: spread/1mATR %.3f exceeds %.3f symbol=%s", phase, spread_to_atr, max_ratio, choice.selected_symbol)
            return False, {"spread_bps": spread_bps, "visible_depth": visible_depth, "premium_atr": premium_atr, "spread_to_atr": spread_to_atr}
        return True, {"spread_bps": spread_bps, "visible_depth": visible_depth, "premium_atr": premium_atr, "spread_to_atr": spread_to_atr}

    def _stream_executable_shortlist(self, underlying_spot: float, available_funds: float) -> dict[str, dict[str, Any]]:
        """Select CE/PE vehicles from Groww's documented live market-depth stream.

        ``get_option_chain`` determines the liquid/Greek-aware candidate universe.
        Executability is not inferred from REST snapshots: a bounded shortlist is
        subscribed to the official FNO LTP and market-depth feeds, and only a
        contract with a live two-sided book can be promoted into the session book.
        After discovery subscriptions are removed, the chosen CE/PE vehicles are
        armed again as the dedicated execution streams.
        """
        per_side = max(1, int(_cfg("GROWW_SESSION_BOOK_STREAM_CANDIDATES_PER_SIDE", 12)))
        routes: list[dict[str, Any]] = []
        route_side: dict[str, str] = {}
        for thesis in ("long", "short"):
            ranked = shortlist_contracts_for_stream_validation(
                self.instrument, thesis, underlying_spot=underlying_spot,
                available_funds=available_funds, limit=per_side,
            )
            if not ranked:
                logger.error("GROWW stream discovery failed: option-chain shortlist empty for thesis=%s", thesis)
                return {}
            for route in ranked:
                symbol = normalise_symbol(route.get("TradingSymbol") or route.get("trading_symbol") or "")
                if symbol and symbol not in route_side:
                    routes.append(route)
                    route_side[symbol] = thesis

        timeout = max(1.0, float(_cfg("GROWW_SESSION_BOOK_STREAM_DISCOVERY_TIMEOUT_SEC", 15.0)))
        hub = hub_for_api(self.api)
        self._live_hub = hub
        discovery_ids: list[str] = []
        states: dict[str, dict[str, Any]] = {}
        tick_event = threading.Event()
        lock = threading.RLock()

        def on_candidate_tick(symbol: str, data: Any) -> None:
            row = data[0] if isinstance(data, list) and data and isinstance(data[0], dict) else data
            if not isinstance(row, dict):
                return
            px = self._float_first(row, ("last", "ltp", "last_price", "close", "price", "Close", "c"))
            bid, ask, bid_qty, ask_qty = self._extract_top_of_book(row)
            now = time.time()
            with lock:
                state = states.get(symbol)
                if state is None:
                    return
                if px > 0:
                    state["ltp"] = px
                    state["last_price"] = px
                    state["ltp_ts"] = now
                if bid > 0:
                    state["bid_price"] = bid
                    state["best_bid_price"] = bid
                if ask > 0:
                    state["offer_price"] = ask
                    state["best_offer_price"] = ask
                if bid_qty > 0:
                    state["bid_quantity"] = bid_qty
                    state["best_bid_quantity"] = bid_qty
                if ask_qty > 0:
                    state["offer_quantity"] = ask_qty
                    state["best_offer_quantity"] = ask_qty
                if bid > 0 and ask >= bid and bid_qty > 0 and ask_qty > 0:
                    state["depth_ts"] = now
                    state["selection_liquidity_source"] = "groww.subscribe_market_depth"
                    state["quote_source"] = "groww_live_fno_ltp_and_market_depth"
            tick_event.set()

        try:
            feed_routes: list[dict[str, Any]] = []
            for route in routes:
                symbol = normalise_symbol(route.get("TradingSymbol") or route.get("trading_symbol") or "")
                stock_code = str(route.get("stock_code") or getattr(self.instrument, "asset_id", "")).upper()
                expiry = self._ws_expiry(route.get("expiry_date") or route.get("expiry") or "")
                right = self.api._normalise_right(route.get("right") or route.get("option_type") or "")
                strike = float(route.get("strike_price") or route.get("strike") or 0.0)
                if not all((symbol, stock_code, expiry, right, strike > 0)):
                    logger.warning("GROWW stream discovery skipped incomplete option route symbol=%s", symbol or "<missing>")
                    continue
                states[symbol] = {
                    "trading_symbol": symbol, "TradingSymbol": symbol,
                    "stock_code": stock_code, "exchange": "NSE", "segment": "FNO", "exchange_code": "NFO",
                    "expiry_date": expiry, "right": right, "strike_price": strike,
                    "thesis": route_side.get(symbol, ""),
                }
                feed_routes.append({
                    "stock_code": stock_code, "expiry_date": expiry,
                    "strike_price": str(strike), "right": right,
                })
            if not feed_routes:
                return {}
            discovery_ids.extend(hub.subscribe_option_universe_market_data(
                routes=feed_routes,
                callback=lambda data: on_candidate_tick(normalise_symbol(data.get("TradingSymbol") or data.get("trading_symbol") or ""), data),
            ))

            deadline = time.time() + timeout
            while time.time() < deadline:
                with lock:
                    live = {
                        symbol: dict(state) for symbol, state in states.items()
                        if float(state.get("ltp_ts", 0.0) or 0.0) > 0
                        and float(state.get("depth_ts", 0.0) or 0.0) > 0
                        and float(state.get("bid_price", 0.0) or 0.0) > 0
                        and float(state.get("offer_price", 0.0) or 0.0) >= float(state.get("bid_price", 0.0) or 0.0)
                        and float(state.get("bid_quantity", 0.0) or 0.0) > 0
                        and float(state.get("offer_quantity", 0.0) or 0.0) > 0
                    }
                have_call = any(row.get("thesis") == "long" for row in live.values())
                have_put = any(row.get("thesis") == "short" for row in live.values())
                if have_call and have_put:
                    # Do not stop on the first two-sided CE/PE packets.  Confirm that
                    # the live books can actually produce the session execution pair;
                    # otherwise continue observing the remaining ranked candidates.
                    trial_diagnostics: dict[str, Any] = {}
                    trial_book = build_session_contract_book(
                        self.instrument, underlying_spot=underlying_spot,
                        available_funds=available_funds, option_quote_by_symbol=live, commit=False,
                        diagnostics=trial_diagnostics,
                    )
                    self._last_stream_discovery_diagnostics = trial_diagnostics
                    if trial_book is not None:
                        logger.info(
                            "GROWW live stream discovery selectable books ready: requested=%d live_two_sided=%d "
                            "basis=option_chain_rank_then_official_fno_ltp_market_depth model_audit=%s",
                            len(states), len(live), trial_diagnostics,
                        )
                        return live
                tick_event.wait(min(0.5, max(0.0, deadline - time.time())))
                tick_event.clear()

            with lock:
                ltp_count = sum(1 for row in states.values() if float(row.get("ltp_ts", 0.0) or 0.0) > 0)
                depth_rows = [dict(row) for row in states.values() if float(row.get("depth_ts", 0.0) or 0.0) > 0]
            sample = []
            for row in depth_rows[:8]:
                bid = float(row.get("bid_price", 0.0) or 0.0); ask = float(row.get("offer_price", 0.0) or 0.0)
                mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else 0.0
                spread_bps = ((ask - bid) / mid * 10000.0) if mid > 0 else None
                sample.append({
                    "symbol": row.get("trading_symbol"), "side": row.get("thesis"),
                    "ltp": round(float(row.get("ltp", 0.0) or 0.0), 4),
                    "bid": round(bid, 4), "ask": round(ask, 4),
                    "bid_qty": float(row.get("bid_quantity", 0.0) or 0.0),
                    "ask_qty": float(row.get("offer_quantity", 0.0) or 0.0),
                    "spread_bps": round(spread_bps, 2) if spread_bps is not None else None,
                })
            live_after_timeout = {
                str(row.get("trading_symbol") or row.get("TradingSymbol") or ""): row
                for row in depth_rows
                if str(row.get("trading_symbol") or row.get("TradingSymbol") or "")
                and float(row.get("ltp_ts", 0.0) or 0.0) > 0
            }
            side_counts = {
                "call": sum(1 for row in live_after_timeout.values() if row.get("thesis") == "long"),
                "put": sum(1 for row in live_after_timeout.values() if row.get("thesis") == "short"),
            }
            final_diagnostics: dict[str, Any] = {}
            if live_after_timeout:
                build_session_contract_book(
                    self.instrument, underlying_spot=underlying_spot,
                    available_funds=available_funds, option_quote_by_symbol=live_after_timeout,
                    commit=False, diagnostics=final_diagnostics,
                )
            self._last_stream_discovery_diagnostics = final_diagnostics
            logger.warning(
                "GROWW live execution universe observed but no current CE/PE book selected within %.1fs: "
                "subscribed=%d ltp_live=%d two_sided_depth=%d side_live=%s model_audit=%s sample_books=%s; "
                "analysis remains live and execution will be rescanned without fallback prices",
                timeout, len(states), ltp_count, len(depth_rows), side_counts, final_diagnostics, sample,
            )
            return live_after_timeout
        except Exception as exc:
            logger.error("GROWW live option stream discovery failed: %s", exc)
            return {}
        finally:
            if discovery_ids:
                try:
                    hub.unsubscribe(discovery_ids)
                except Exception as exc:
                    logger.warning("GROWW discovery stream unsubscribe failed after selection: %s", exc)


    def _prewarm_session_vehicle(self, choice) -> bool:
        """Load option-premium history after live-depth vehicle validation.

        Two-sided selection liquidity has been verified through the official Groww
        market-depth stream.  Fresh dedicated LTP+depth websocket state remains the
        non-negotiable entry activation gate after the selected feeds are armed.
        """
        route = self._route_field_snapshot()
        try:
            apply_contract_choice(self.instrument, choice)
            self._clear_option_execution_state()
            self._warmup(historical_only=True)
            with self._lock:
                if self._last_price <= 0 or len(self._candles.get("1m", ())) < int(_cfg("GROWW_OPTION_MIN_READY_1M_BARS", 20)):
                    return False
                self._contract_snapshots[self._snapshot_key(choice)] = {
                    "last_price": self._last_price, "last_quote_ts": 0.0,
                    "best_bid": 0.0, "best_ask": 0.0,
                    "best_bid_qty": 0.0, "best_ask_qty": 0.0,
                    "selection_liquidity_source": "groww.subscribe_market_depth",
                    "execution_liquidity_source": "DOCUMENTED_OPTION_LTP_AND_DEPTH_STREAM_REQUIRED",
                    "candles": {tf: list(rows) for tf, rows in self._candles.items()},
                }
            return True
        finally:
            self._restore_route_fields(route)
            self._clear_option_execution_state()

    def prepare_session_contract_book(self, underlying_spot: float, available_funds: float, *, force_refresh: bool = False, reason: str = "session_start") -> bool:
        self._session_book_last_refresh_attempt_ts = time.time()
        self._last_session_available_funds = float(available_funds or 0.0)
        self._last_session_underlying_spot = float(underlying_spot or 0.0)
        if not self._is_chain_mode():
            return True
        raw = getattr(getattr(self.instrument, "primary", None), "raw", {}) or {}
        if underlying_spot <= 0 or available_funds <= 0:
            logger.error("GROWW session execution universe unavailable: underlying spot/funds not ready spot=%.4f funds=%.2f", underlying_spot, available_funds)
            return False
        if not self._hydrate_chain_candidates(force_refresh=force_refresh, underlying_spot=underlying_spot):
            return False
        # Official option-chain data builds the candidate set. Official FNO LTP +
        # market-depth streams determine executable books; no REST-price fallback.
        live_book_by_symbol = self._stream_executable_shortlist(underlying_spot, available_funds)
        if not live_book_by_symbol:
            if isinstance(raw, dict):
                raw["session_contract_book_status"] = "MONITORING_NO_LIVE_EXECUTION_BOOK"
                raw["session_contract_diagnostics"] = dict(self._last_stream_discovery_diagnostics)
            logger.warning("GROWW execution universe currently has no paired live CE/PE book; NIFTY analysis remains enabled and execution stays blocked pending rescan")
            return True
        diagnostics: dict[str, Any] = {}
        book = build_session_contract_book(
            self.instrument, underlying_spot=underlying_spot,
            available_funds=available_funds, option_quote_by_symbol=live_book_by_symbol, commit=False,
            diagnostics=diagnostics,
        )
        if book is None:
            if isinstance(raw, dict):
                raw["session_contract_book_status"] = "MONITORING_NO_POLICY_ELIGIBLE_PAIR"
                raw["session_contract_diagnostics"] = diagnostics
            logger.warning(
                "GROWW live execution books exist but no pair is currently model-eligible; NIFTY analysis remains live, "
                "entries blocked until rescan. model_audit=%s", diagnostics,
            )
            return True
        if bool(_cfg("GROWW_SESSION_BOOK_PREWARM_EXECUTION_DATA", True)):
            if not self._prewarm_session_vehicle(book.call) or not self._prewarm_session_vehicle(book.put):
                logger.error("GROWW session contract book rejected: CE/PE option premium historical warmup failed")
                return False
        if not self._arm_session_book_streams(book):
            return False
        if not isinstance(raw, dict):
            return False
        stream_ready = all(
            float(state.get("quote_tick_ts", 0.0) or 0.0) > 0.0
            and float(state.get("depth_tick_ts", 0.0) or 0.0) > 0.0
            and float(state.get("last_price", 0.0) or 0.0) > 0.0
            and float(state.get("best_bid", 0.0) or 0.0) > 0.0
            and float(state.get("best_ask", 0.0) or 0.0) >= float(state.get("best_bid", 0.0) or 0.0)
            for state in self._book_stream_state.values()
        ) if self._book_stream_state else False
        raw["session_contract_book"] = book.as_dict()
        raw["session_contract_diagnostics"] = diagnostics
        raw["session_contract_book_status"] = "READY" if stream_ready else "ARMED_PENDING_WEBSOCKET_TICK"
        raw["session_contract_book_mode"] = "preselected_call_and_put_live_direction"
        self._session_book_last_refresh_ts = time.time()
        logger.info(
            "GROWW SESSION CONTRACT BOOK %s [%s] reason=%s spot=%.2f funds=₹%.2f | "
            "CE=%s strike=%.2f expiry=%s lot=%.0f prem=₹%.2f score=%.3f delta=%+.3f theta/day=%.4f theta/hold=%.2fbps | "
            "PE=%s strike=%.2f expiry=%s lot=%.0f prem=₹%.2f score=%.3f delta=%+.3f theta/day=%.4f theta/hold=%.2fbps | model_audit=%s",
            raw["session_contract_book_status"], book.trade_date_ist, reason, underlying_spot, available_funds,
            book.call.selected_symbol, book.call.strike, book.call.expiry, float(book.call.raw.get("runtime_lot_size", 0.0) or 0.0), float(book.call.raw.get("selected_entry_premium", 0.0) or 0.0), book.call.score, book.call.delta, book.call.theta_to_premium, float(book.call.raw.get("theta_carry_bps_expected_hold", 0.0) or 0.0),
            book.put.selected_symbol, book.put.strike, book.put.expiry, float(book.put.raw.get("runtime_lot_size", 0.0) or 0.0), float(book.put.raw.get("selected_entry_premium", 0.0) or 0.0), book.put.score, book.put.delta, book.put.theta_to_premium, float(book.put.raw.get("theta_carry_bps_expected_hold", 0.0) or 0.0), diagnostics,
        )
        return True

    def ensure_session_contract_book(self, underlying_spot: float) -> bool:
        """Retry live option vehicle construction while keeping NIFTY analysis running.

        A momentarily unsuitable option surface must block orders, not permanently
        disable the underlying desk.  Rebuilds remain bounded and official-feed only.
        """
        raw = getattr(getattr(self.instrument, "primary", None), "raw", {}) or {}
        if isinstance(raw, dict) and isinstance(raw.get("session_contract_book"), dict):
            return True
        cooldown = max(5.0, float(_cfg("GROWW_SESSION_BOOK_RESCAN_SEC", 30.0)))
        if time.time() - self._session_book_last_refresh_attempt_ts < cooldown:
            return False
        return self.prepare_session_contract_book(
            float(underlying_spot or self._last_session_underlying_spot or 0.0),
            float(self._last_session_available_funds or 0.0),
            force_refresh=True, reason="execution_universe_rescan",
        )

    def get_verified_option_chain_snapshot(self) -> list[dict[str, Any]]:
        """Return the official hydrated NFO chain retained for valuation context.

        The chain is sourced from the official instrument master joined to
        ``get_option_chain``; no synthetic IV/OI rows are generated here.
        """
        raw = getattr(getattr(self.instrument, "primary", None), "raw", {}) or {}
        rows = raw.get("chain_candidates") if isinstance(raw, dict) else []
        return [dict(row) for row in (rows or []) if isinstance(row, dict)]

    def get_session_book_lot_size(self) -> int:
        raw = getattr(getattr(self.instrument, "primary", None), "raw", {}) or {}
        book = raw.get("session_contract_book") if isinstance(raw, dict) else {}
        for side in ("call", "put"):
            choice = book.get(side) if isinstance(book, dict) else None
            choice_raw = choice.get("raw") if isinstance(choice, dict) and isinstance(choice.get("raw"), dict) else {}
            try:
                lot = int(round(float(choice_raw.get("runtime_lot_size", 0.0) or 0.0)))
            except Exception:
                lot = 0
            if lot > 0:
                return lot
        return 0

    def session_contract_book_status(self) -> dict[str, Any]:
        """Expose the day-start option vehicle book and its actual websocket health.

        Underlying NIFTY ticks are analysis data only. Option execution is ready
        only from a direction-specific NFO route with a fresh identity-routed
        websocket LTP plus market-depth pair. Historical/session-book prices cannot clear it.
        """
        raw = getattr(getattr(self.instrument, "primary", None), "raw", {}) or {}
        book = raw.get("session_contract_book") if isinstance(raw, dict) else None
        if not isinstance(book, dict):
            return {
                "status": str(raw.get("session_contract_book_status") or "MISSING") if isinstance(raw, dict) else "MISSING",
                "execution_freshness_gate": "DOCUMENTED_OPTION_LTP_AND_DEPTH_STREAM_REQUIRED",
                "model_audit": dict(raw.get("session_contract_diagnostics") or {}) if isinstance(raw, dict) else {},
            }
        now = time.time()
        max_stale = max(0.1, float(_cfg("GROWW_OPTION_STREAM_MAX_STALE_SEC", 15.0)))

        def choice_summary(name: str) -> dict[str, Any]:
            choice = book.get(name) if isinstance(book.get(name), dict) else {}
            choice_raw = choice.get("raw") if isinstance(choice.get("raw"), dict) else {}
            right = str(choice.get("right", name) or name).lower()
            key = (str(choice.get("expiry", "") or ""), right, round(float(choice.get("strike", 0.0) or 0.0), 6))
            with self._lock:
                stream = dict(self._book_stream_state.get(key, {}) or {})
            quote_ts = float(stream.get("quote_tick_ts", 0.0) or 0.0)
            depth_ts = float(stream.get("depth_tick_ts", 0.0) or 0.0)
            quote_age = max(0.0, now - quote_ts) if quote_ts > 0 else None
            depth_age = max(0.0, now - depth_ts) if depth_ts > 0 else None
            stream_age = max(quote_age, depth_age) if quote_age is not None and depth_age is not None else None
            ws_fresh = bool(stream_age is not None and stream_age <= max_stale)
            return {
                "symbol": choice.get("selected_symbol", ""),
                "right": choice.get("right", ""),
                "strike": float(choice.get("strike", 0.0) or 0.0),
                "expiry": choice.get("expiry", ""),
                "premium": float(choice_raw.get("selected_entry_premium", 0.0) or 0.0),
                "lot": float(choice_raw.get("runtime_lot_size", 0.0) or 0.0),
                "cost": float(choice_raw.get("selected_contract_cost", 0.0) or 0.0),
                "delta": float(choice.get("delta", 0.0) or 0.0),
                "iv": float(choice_raw.get("bs_volatility", 0.0) or 0.0),
                "iv_source": str(choice_raw.get("bs_volatility_source") or ""),
                "ws_fresh": ws_fresh,
                "ws_age_sec": stream_age,
                "ws_pending": bool(stream.get("stream_pending", True)),
                "live_premium": float(stream.get("last_price", 0.0) or 0.0),
                "live_bid": float(stream.get("best_bid", 0.0) or 0.0),
                "live_ask": float(stream.get("best_ask", 0.0) or 0.0),
            }

        call = choice_summary("call")
        put = choice_summary("put")
        feed_status = self.execution_feed_status()
        if self._active_stream_key is not None:
            active_right = str(self._active_stream_key[1]).lower()
            active = call if active_right == "call" else put if active_right == "put" else {}
            dynamic_status = "ACTIVE_EXECUTION_VEHICLE_FRESH" if bool(feed_status.get("active_vehicle_ready")) else "ACTIVE_EXECUTION_VEHICLE_STALE"
        else:
            dynamic_status = "READY" if bool(feed_status.get("session_vehicle_stream_ready")) else "ARMED_PENDING_WEBSOCKET_TICK"
        return {
            "status": dynamic_status,
            "configured_status": str(raw.get("session_contract_book_status") or "READY"),
            "execution_feed_status": str(feed_status.get("status") or "UNKNOWN"),
            "execution_freshness_gate": "DOCUMENTED_OPTION_LTP_AND_DEPTH_STREAM_REQUIRED",
            "max_stream_stale_sec": max_stale,
            "trade_date_ist": str(book.get("trade_date_ist") or ""),
            "built_at": float(book.get("built_at", 0.0) or 0.0),
            "underlying": str(book.get("underlying") or getattr(self.instrument, "asset_id", "")),
            "underlying_spot": float(book.get("underlying_spot", 0.0) or 0.0),
            "available_funds": float(book.get("available_funds", 0.0) or 0.0),
            "source": str(book.get("source") or ""),
            "call": call,
            "put": put,
        }

    def execution_feed_status(self) -> dict[str, Any]:
        """Execution-only liveness; never substitute underlying NIFTY freshness."""
        now = time.time()
        max_age = float(_cfg("GROWW_OPTION_STREAM_MAX_STALE_SEC", 15.0))
        state_rows = []
        for key, state in dict(self._book_stream_state).items():
            quote_ts = float(state.get("quote_tick_ts", 0.0) or 0.0)
            depth_ts = float(state.get("depth_tick_ts", 0.0) or 0.0)
            quote_age = (now - quote_ts) if quote_ts > 0 else None
            depth_age = (now - depth_ts) if depth_ts > 0 else None
            age = max(quote_age, depth_age) if quote_age is not None and depth_age is not None else None
            px = float(state.get("last_price", 0.0) or 0.0)
            bid = float(state.get("best_bid", 0.0) or 0.0)
            ask = float(state.get("best_ask", 0.0) or 0.0)
            bid_qty = float(state.get("best_bid_qty", 0.0) or 0.0)
            ask_qty = float(state.get("best_ask_qty", 0.0) or 0.0)
            quote_fresh = bool(quote_ts > 0 and quote_age is not None and quote_age <= max_age and px > 0)
            depth_fresh = bool(depth_ts > 0 and depth_age is not None and depth_age <= max_age and bid > 0 and ask >= bid and bid_qty > 0 and ask_qty > 0)
            executable = bool(quote_fresh and depth_fresh)
            state_rows.append({
                "key": key, "age_sec": age, "price": px, "quote_fresh": quote_fresh,
                "depth_fresh": depth_fresh, "book_executable": executable, "quote_tick_ts": quote_ts,
                "depth_tick_ts": depth_ts, "ohlcv_tick_ts": float(state.get("ohlcv_tick_ts", 0.0) or 0.0),
            })
        live = [row for row in state_rows if row["quote_fresh"]]
        executable = [row for row in state_rows if row["book_executable"]]
        active_row = next((row for row in state_rows if row["key"] == self._active_stream_key), None)
        streamed_active_ready = bool(active_row and active_row["book_executable"])
        active_ready = streamed_active_ready
        if streamed_active_ready:
            status = "ACTIVE_OPTION_VEHICLE_LIVE"
        elif len(executable) == len(state_rows) and state_rows:
            status = "CE_PE_PRESELECTED_LIVE_PENDING_DIRECTION"
        elif self._stream_unroutable_tick_count > 0 and not live:
            status = "MULTIPLEXED_TICKS_SEEN_NO_OPTION_ROUTE"
        elif self._stream_subscription_ids:
            status = "ARMED_PENDING_OPTION_WEBSOCKET_TICK"
        else:
            raw = getattr(getattr(self.instrument, "primary", None), "raw", {}) or {}
            configured = str(raw.get("session_contract_book_status") or "") if isinstance(raw, dict) else ""
            status = configured if configured.startswith("MONITORING_") else "OPTION_STREAM_NOT_ARMED"
        return {
            "status": status,
            "session_vehicle_stream_ready": bool(active_ready or (state_rows and len(executable) == len(state_rows))),
            "active_vehicle_ready": active_ready,
            "active_vehicle_stream_ready": streamed_active_ready,
            "active_execution_quote_source": "GROWW_OPTION_LTP_AND_DEPTH_FEED" if streamed_active_ready else "NONE",
            "active_vehicle": self._active_stream_key,
            "preselected_vehicle_count": len(state_rows),
            "fresh_vehicle_count": len(live),
            "executable_vehicle_count": len(executable),
            "unroutable_tick_count": int(self._stream_unroutable_tick_count),
            "unroutable_last_keys": list(self._stream_unroutable_last_keys),
            "unroutable_last_age_sec": (now - self._stream_unroutable_last_ts) if self._stream_unroutable_last_ts > 0 else None,
        }

    def _activate_session_vehicle(self, choice) -> bool:
        apply_contract_choice(self.instrument, choice)
        key = self._snapshot_key(choice)
        snapshot = self._contract_snapshots.get(key) or {}
        if snapshot:
            with self._lock:
                self._last_price = float(snapshot.get("last_price", 0.0) or 0.0)
                self._last_quote_ts = float(snapshot.get("last_quote_ts", 0.0) or 0.0)
                self._best_bid = float(snapshot.get("best_bid", 0.0) or 0.0)
                self._best_ask = float(snapshot.get("best_ask", 0.0) or 0.0)
                self._best_bid_qty = float(snapshot.get("best_bid_qty", 0.0) or 0.0)
                self._best_ask_qty = float(snapshot.get("best_ask_qty", 0.0) or 0.0)
                if snapshot.get("candles"):
                    self._candles = {tf: deque(rows, maxlen=600) for tf, rows in (snapshot.get("candles") or {}).items()}
        # Stream discovery verifies an executable book but does not provide a
        # premium OHLC history.  SL/TP for long options must be built in the
        # option-premium domain, so fetch the exact selected contract's official
        # historical candles once and cache them for subsequent activations.
        min_atr_bars = max(2, int(_cfg("GROWW_OPTION_PROTECTION_MIN_ATR_BARS", 10)))
        cached_1m = list((snapshot.get("candles") or {}).get("1m", [])) if isinstance(snapshot, dict) else []
        if len(cached_1m) < min_atr_bars:
            self._warmup(historical_only=True)
            with self._lock:
                target = self._contract_snapshots.setdefault(key, {})
                target["candles"] = {tf: list(rows) for tf, rows in self._candles.items()}
        # Refresh the selected snapshot after historical warmup and carry that
        # official premium history into the already-armed stream buffer. Without
        # this merge, a sparse newly opened stream can overwrite the ATR history
        # just before protection construction and permanently block safe entries.
        snapshot = self._contract_snapshots.get(key) or snapshot
        historical_candles = (snapshot.get("candles") or {}) if isinstance(snapshot, dict) else {}
        # Both session vehicles are websocket-armed before signal execution.
        # Activation routes the direction-specific vehicle without subscribing on
        # the latency-sensitive entry path. A fresh Groww LTP plus market-depth
        # stream for the exact selected option is mandatory; there is no REST substitute.
        stream_state = self._book_stream_state.get(key)
        self._active_stream_key = key
        if stream_state:
            self._repair_option_stream_if_stale("activate_session_vehicle", wait_key=key)
            stream_state = self._book_stream_state.get(key) or stream_state
            with self._lock:
                if historical_candles:
                    stream_candles = stream_state.setdefault("candles", {})
                    for timeframe in ("1m", "5m", "15m", "1h", "4h", "1d"):
                        merged: dict[int, dict[str, Any]] = {}
                        for row in list(historical_candles.get(timeframe, []) or []):
                            if isinstance(row, dict):
                                merged[int(row.get("t", row.get("timestamp", 0)) or 0)] = row
                        for row in list(stream_candles.get(timeframe, []) or []):
                            if isinstance(row, dict):
                                merged[int(row.get("t", row.get("timestamp", 0)) or 0)] = row
                        stream_candles[timeframe] = deque(
                            [merged[ts] for ts in sorted(merged) if ts > 0][-600:], maxlen=600
                        )
                self._contract_snapshots.setdefault(key, {})["candles"] = {
                    tf: list(rows) for tf, rows in (stream_state.get("candles") or {}).items()
                }
                self._active_stream_contract = dict(stream_state.get("identity") or {})
                self._last_price = float(stream_state.get("last_price", 0.0) or 0.0)
                stream_ts = float(stream_state.get("last_stream_tick_ts", 0.0) or 0.0)
                self._last_quote_ts = float(stream_ts or stream_state.get("last_quote_ts", 0.0) or 0.0)
                self._last_stream_tick_ts = stream_ts
                self._last_ltp_stream_ts = float(stream_state.get("quote_tick_ts", 0.0) or 0.0)
                self._last_depth_stream_ts = float(stream_state.get("depth_tick_ts", 0.0) or 0.0)
                self._best_bid = float(stream_state.get("best_bid", 0.0) or 0.0)
                self._best_ask = float(stream_state.get("best_ask", 0.0) or 0.0)
                self._best_bid_qty = float(stream_state.get("best_bid_qty", 0.0) or 0.0)
                self._best_ask_qty = float(stream_state.get("best_ask_qty", 0.0) or 0.0)
                if stream_state.get("candles"):
                    self._candles = {tf: deque(rows, maxlen=600) for tf, rows in stream_state["candles"].items()}
        else:
            # Compatibility path for a direct single-contract GROWW instrument.
            if not self._start_selected_contract_stream():
                return False
        max_quote_age = float(_cfg("GROWW_OPTION_MAX_QUOTE_STALE_SEC", 10.0))
        if self._last_price <= 0 or not self._execution_price_fresh(max_quote_age):
            logger.error(
                "GROWW selected session vehicle has no fresh documented LTP+market-depth websocket state: %s | execution_feed=%s",
                getattr(choice, "selected_symbol", key), self.execution_feed_status(),
            )
            return False
        # The entry price, affordability check and premium-native SL/TP must use
        # the execution-time quote, not the session-start snapshot premium.
        raw = getattr(getattr(self.instrument, "primary", None), "raw", {}) or {}
        selected = raw.get("selected_option_contract") if isinstance(raw, dict) else None
        if isinstance(selected, dict):
            selected["selected_entry_premium"] = float(self._last_price)
            selected.setdefault("raw", {})["selected_entry_premium"] = float(self._last_price)
            selected["raw"]["ltp"] = float(self._last_price)
            raw["selected_entry_premium"] = float(self._last_price)
        return True

    def select_contract_for_thesis(self, thesis_side: str, underlying_spot: float = 0.0, available_funds: float = 0.0):
        choice, status = select_contract_from_session_book(
            self.instrument, thesis_side, underlying_spot=underlying_spot, available_funds=available_funds)
        if choice is None:
            now = time.time()
            min_refresh = float(_cfg("GROWW_SESSION_BOOK_MIN_REFRESH_SEC", 900.0))
            urgent_cooldown = float(_cfg("GROWW_SESSION_BOOK_URGENT_REFRESH_COOLDOWN_SEC", 30.0))
            immediate = {"session_contract_book_missing", "session_contract_book_new_trading_day"}
            invalidated = {"session_contract_spot_drift", "session_contract_delta_drift", "session_contract_vehicle_invalid"}
            if status in immediate:
                can_refresh = True
            elif status in invalidated:
                can_refresh = now - self._session_book_last_refresh_attempt_ts >= urgent_cooldown
            else:
                can_refresh = now - self._session_book_last_refresh_ts >= min_refresh
            if can_refresh and status in immediate | invalidated:
                if self.prepare_session_contract_book(underlying_spot, available_funds, force_refresh=True, reason=status):
                    choice, status = select_contract_from_session_book(
                        self.instrument, thesis_side, underlying_spot=underlying_spot, available_funds=available_funds)
            if choice is None:
                logger.warning("GROWW preselected session vehicle unavailable for %s thesis=%s reason=%s", getattr(self.instrument, "asset_id", "?"), thesis_side, status)
                return None
        if not self._activate_session_vehicle(choice):
            logger.error("GROWW preselected vehicle failed mandatory documented LTP+market-depth websocket activation: %s", choice.selected_symbol)
            self.release_execution_vehicle()
            return None
        # The session book was built earlier, but the final affordability and
        # executable spread test must be repeated using the fresh entry quote.
        raw = getattr(getattr(self.instrument, "primary", None), "raw", {}) or {}
        lot = float(choice.raw.get("runtime_lot_size", 0.0) or 0.0)
        funds = max(0.0, float(available_funds or 0.0))
        fraction = max(0.01, min(1.0, float(_cfg("GROWW_OPTION_MAX_FUNDS_FRACTION_PER_TRADE", 0.42))))
        buffer_inr = max(0.0, float(_cfg("GROWW_OPTION_MIN_CASH_BUFFER_INR", 0.0)))
        live_cost = float(self._last_price or 0.0) * lot
        max_cost = max(0.0, funds - buffer_inr) * fraction
        if lot <= 0 or live_cost <= 0 or max_cost <= 0 or live_cost > max_cost:
            logger.warning("GROWW session vehicle rejected at execution: live cost ₹%.2f exceeds budget ₹%.2f symbol=%s", live_cost, max_cost, choice.selected_symbol)
            self.release_execution_vehicle()
            return None
        acceptable, metrics = self._live_execution_liquidity_check(choice, phase="entry-activation")
        if not acceptable:
            self.release_execution_vehicle()
            return None
        raw["selected_live_contract_cost"] = live_cost
        raw["selected_live_spread_bps"] = metrics["spread_bps"]
        raw["selected_live_visible_depth"] = metrics["visible_depth"]
        raw["selected_live_premium_atr_1m"] = metrics["premium_atr"]
        raw["selected_live_spread_to_atr"] = metrics["spread_to_atr"]
        self._selected_contract = choice
        logger.info(
            "GROWW SESSION VEHICLE ACTIVATED symbol=%s underlying=%s thesis=%s | strike=%.2f expiry=%s DTE=%.1f "
            "score=%.3f delta=%+.3f theta/prem=%.4f moneyness=%.4f | premium=₹%.2f lot=%.0f cost=₹%.2f/₹%.2f "
            "spread=%.2fbps spread/ATR1m=%.3f premiumATR1m=₹%.4f visible_depth=%.0f | quote=FRESH execution=APPROVED",
            choice.selected_symbol, getattr(self.instrument, "asset_id", "?"), thesis_side, choice.strike, choice.expiry, choice.dte,
            choice.score, choice.delta, choice.theta_to_premium, choice.moneyness, float(self._last_price or 0.0), lot, live_cost, max_cost,
            metrics["spread_bps"], metrics["spread_to_atr"], metrics["premium_atr"], metrics["visible_depth"],
        )
        return choice

    def release_execution_vehicle(self) -> None:
        """Return to the session CE/PE book after a confirmed flat position.

        A previous CE trade must never pin the next bearish thesis to the CE.
        The book remains valid for the day; only the active routed identity and
        execution-premium state are cleared.
        """
        with self._lock:
            # Invalidate any live thread before a future vehicle can re-enable polling.
            self._poll_generation += 1
            self._running = False
        # Session-book CE/PE websocket subscriptions remain armed across flat
        # transitions so the next thesis has immediate live execution pricing.
        self._active_stream_key = None
        self._active_stream_contract = {}
        self._restore_route_fields(dict(self._underlying_route_fields))
        self._selected_contract = None
        self._clear_option_execution_state()
        logger.info("GROWW execution vehicle released after confirmed FLAT; CE/PE session websocket feeds remain live for next direction")

    def register_strategy(self, strategy) -> None:
        self._strategy_ref = strategy

    def start(self) -> bool:
        try:
            session = groww_market_session_state()
            if not session.is_open and bool(_cfg("GROWW_ANALYZE_ONLY_DURING_MARKET_SESSION", True)):
                self.is_ready = False
                logger.warning(
                    "GROWW option DM dormant: %s; NIFTY option analysis/trading disabled outside NSE/NFO hours",
                    session.reason,
                )
                return False
            if self._is_chain_mode():
                self.api.preflight_session()
                # Underlying analysis remains separate from option execution, but
                # the CE/PE execution vehicles are prepared after underlying warmup
                # and before the intraday scan can enter a position.
                self.is_ready = True
                logger.info(
                    "GROWW option DM ready for session-book preparation [%s]; CE/PE vehicles will be preselected before signal execution",
                    getattr(self.instrument, "asset_id", "?"),
                )
                return True
            if not session.is_open:
                if bool(_cfg("GROWW_ALLOW_CLOSED_MARKET_HISTORICAL_WARMUP", True)):
                    self.warmup_closed_market(session.reason)
                if not bool(_cfg("GROWW_ALLOW_CLOSED_MARKET_WARMUP", False)):
                    logger.warning(
                        "GROWW option DM dormant: %s; historical warmup=%s; live quote/trading disabled for %s",
                        session.reason,
                        self._historical_count_summary(),
                        getattr(self.instrument, "asset_id", "?"),
                    )
                    return False
            self.api.preflight_session()
            self._warmup(historical_only=True)
            if self._last_price <= 0 or len(self._candles.get("1m", ())) < int(_cfg("GROWW_OPTION_MIN_READY_1M_BARS", 30)):
                logger.error("GROWW option DM not ready: missing real quote/historical candles for %s", getattr(self.instrument, "asset_id", "?"))
                return False
            self.is_ready = True
            return True
        except Exception as exc:
            logger.error("GROWW option data start failed for %s: %s", getattr(self.instrument, "asset_id", "?"), exc)
            return False

    def warmup_closed_market(self, reason: str = "market closed") -> None:
        """Fetch historical candles outside live trading hours without enabling trading."""
        try:
            self.api.preflight_session()
            self._warmup(historical_only=True)
            logger.info(
                "GROWW closed-market historical warmup for %s complete: %s; reason=%s",
                getattr(self.instrument, "asset_id", "?"),
                self._historical_count_summary(),
                reason,
            )
        except Exception as exc:
            logger.warning("GROWW closed-market historical warmup failed for %s: %s", getattr(self.instrument, "asset_id", "?"), exc)

    def stop(self) -> None:
        with self._lock:
            self._poll_generation += 1
            self._running = False
        self._stop_option_stream()

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

    def _warmup(self, historical_only: bool = False) -> None:
        for tf in ("1m", "5m", "15m", "1h"):
            try:
                self._load_historical(tf)
            except Exception as exc:
                logger.warning("GROWW historical warmup %s failed for %s: %s", tf, getattr(self.instrument, "asset_id", "?"), exc)

    def _historical_count_summary(self) -> str:
        with self._lock:
            return " ".join(f"{tf}={len(self._candles.get(tf, ())) }" for tf in ("1m", "5m", "15m", "1h"))

    def _load_historical(self, timeframe: str) -> None:
        raw = getattr(getattr(self.instrument, "primary", None), "raw", {}) or {}
        selected = raw.get("selected_option_contract") if isinstance(raw, dict) else None
        if isinstance(selected, dict) and isinstance(selected.get("raw"), dict):
            raw = selected.get("raw") or raw
        # Groww documents native candle intervals through get_historical_candles.
        interval = {"1m": "1minute", "5m": "5minute", "15m": "15minute", "1h": "1hour", "4h": "4hour", "1d": "1day"}.get(timeframe, "1minute")
        to_dt = datetime.now(timezone.utc)
        from_dt = to_dt - timedelta(days=5 if timeframe in {"1m", "5m", "15m"} else 45)
        body = {
            "interval": interval,
            "from_date": from_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "to_date": to_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "trading_symbol": str(raw.get("trading_symbol") or raw.get("TradingSymbol") or "").strip(),
            "groww_symbol": str(raw.get("groww_symbol") or "").strip(),
            "stock_code": str(raw.get("stock_code") or raw.get("ShortName") or "").upper(),
            "exchange_code": str(raw.get("exchange_code") or "NFO").upper(),
            "segment": "FNO",
            "expiry_date": self.api._normalise_expiry(raw.get("expiry_date") or raw.get("ExpiryDate") or ""),
            "right": self.api._normalise_right(raw.get("right") or raw.get("OptionType") or ""),
            "strike_price": str(raw.get("strike_price") or raw.get("StrikePrice") or ""),
        }
        groww_throttle(f"historical_candles:{timeframe}:{getattr(self.instrument, 'asset_id', '?')}")
        resp = self.api.get_historical_candles_canonical(**{k: v for k, v in body.items() if v})
        rows = resp.get("Success") or []
        parsed = []
        for r in rows if isinstance(rows, list) else []:
            if not isinstance(r, dict):
                continue
            o = self._float_first(r, ("open", "Open")); h = self._float_first(r, ("high", "High")); l = self._float_first(r, ("low", "Low")); c = self._float_first(r, ("close", "Close")); v = self._float_first(r, ("volume", "Volume"))
            if c <= 0:
                continue
            ts = r.get("datetime") or r.get("date") or r.get("time") or time.time()
            ts_ms = self._parse_ts_ms(ts)
            high = max(float(h or c), float(o or c), float(c))
            low = min(float(l or c), float(o or c), float(c))
            parsed.append({
                "t": ts_ms, "o": float(o or c), "h": high, "l": low, "c": float(c), "v": float(v or 0.0),
                "timestamp": ts_ms / 1000.0, "open": float(o or c), "high": high, "low": low, "close": float(c), "volume": float(v or 0.0),
            })
        if parsed:
            with self._lock:
                self._candles[timeframe].clear(); self._candles[timeframe].extend(parsed[-600:])
                self._last_price = float(parsed[-1]["close"])

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

        # Groww returns many NSE/NFO timestamps without timezone.  These are
        # exchange-local IST values, not UTC.  Parsing them as UTC makes candles
        # appear in the future and poisons sweep-age logic.
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

    def _execution_price_fresh(self, max_stale_seconds: float) -> bool:
        """Require independently fresh Groww option LTP and market-depth feeds."""
        now = time.time()
        with self._lock:
            ltp_ts = float(self._last_ltp_stream_ts or 0.0)
            depth_ts = float(self._last_depth_stream_ts or 0.0)
            has_book = (
                self._best_bid > 0 and self._best_ask >= self._best_bid
                and self._best_bid_qty > 0 and self._best_ask_qty > 0
            )
            has_price = self._last_price > 0
        max_stream = min(float(max_stale_seconds), float(_cfg("GROWW_OPTION_STREAM_MAX_STALE_SEC", 15.0)))
        return bool(
            ltp_ts > 0 and depth_ts > 0
            and now - ltp_ts <= max_stream and now - depth_ts <= max_stream
            and has_price and has_book
        )

    def get_last_update(self) -> float:
        with self._lock:
            return float(self._last_quote_ts or 0.0)

    def get_candles(self, timeframe: str = "5m", limit: int = 100) -> List[Dict]:
        with self._lock:
            return list(self._candles.get(timeframe, deque()))[-int(limit):]

    def get_last_price(self) -> float:
        if self._active_stream_key is not None:
            self._repair_option_stream_if_stale("option_last_price_request", wait_key=self._active_stream_key)
        with self._lock:
            return float(self._last_price or 0.0)

    def get_orderbook(self) -> Dict:
        if self._active_stream_key is not None:
            self._repair_option_stream_if_stale("option_orderbook_request", wait_key=self._active_stream_key)
        # No synthetic orderbook: only return bid/ask levels that Groww actually
        # provided in the latest quote payload.
        with self._lock:
            bid = float(self._best_bid or 0.0)
            ask = float(self._best_ask or 0.0)
            bid_qty = float(self._best_bid_qty or 0.0)
            ask_qty = float(self._best_ask_qty or 0.0)
            ts = self._last_quote_ts
            stream_ts = float(self._last_stream_tick_ts or 0.0)
        # Never fabricate order-book depth: liquidity and order-flow features use
        # only executable size Groww actually returned.
        stream_age = time.time() - stream_ts if stream_ts > 0 else 999999.0
        stream_fresh = bool(self._stream_subscription_ids) and stream_ts > 0 and stream_age <= float(_cfg("GROWW_OPTION_STREAM_MAX_STALE_SEC", 15.0))
        source = "groww_option_ltp_and_depth_feed" if stream_fresh else "groww_option_feed_stale" if self._stream_subscription_ids else "groww_option_feed_not_armed"
        return {"bids": [[bid, bid_qty]] if bid > 0 and bid_qty > 0 else [], "asks": [[ask, ask_qty]] if ask > 0 and ask_qty > 0 else [], "timestamp": ts, "_sources": 1, "_executable_source": source, "_stream_age_sec": stream_age}

    def get_recent_trades(self, limit: int = 100) -> List[Dict]:
        with self._lock:
            return list(self._trades)[-int(limit):]

    def get_recent_trades_raw(self, limit: int = 100) -> List[Dict]:
        return self.get_recent_trades(limit)

    def is_price_fresh(self, max_stale_seconds: float = 90.0) -> bool:
        self._repair_option_stream_if_stale("option_freshness_gate", wait_key=self._active_stream_key)
        if bool(_cfg("GROWW_OPTION_WEBSOCKET_REQUIRED", True)) and bool(self._stream_subscription_ids):
            return self._execution_price_fresh(max_stale_seconds)
        with self._lock:
            ts = float(self._last_quote_ts or 0.0)
        return ts > 0 and time.time() - ts <= float(max_stale_seconds)

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

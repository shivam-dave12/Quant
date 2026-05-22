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
from types import SimpleNamespace
try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

_ICICI_LOCAL_TZ = ZoneInfo("Asia/Kolkata") if ZoneInfo is not None else timezone(timedelta(hours=5, minutes=30))
from typing import Any, Dict, List

try:
    import config
except Exception:  # pragma: no cover
    config = None  # type: ignore

from .api import BreezeRestClient
from .market_session import icici_market_session_state
from .rate_limiter import breeze_throttle
from agents.icici_chain_architect import (
    chain_quality, is_chain_instrument, apply_contract_choice,
    build_session_contract_book, select_contract_from_session_book,
    eligible_nfo_master_option_rows, merge_verified_chain_quotes, contract_key,
)

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
        self._underlying_route_fields = self._route_field_snapshot()
        logger.info("ICICIOptionDataManager initialised [%s]", getattr(instrument, "asset_id", "ICICI"))

    def _is_chain_mode(self) -> bool:
        return is_chain_instrument(self.instrument)

    def _hydrate_chain_candidates(self, *, force_refresh: bool = False, underlying_spot: float = 0.0) -> bool:
        """Populate the session option universe from verified NFO definitions.

        Breeze OptionChain does not support a blind entire-chain call: the
        official contract requires at least two filters among expiry/right/strike.
        We therefore obtain exact contract definitions and lot sizes from the
        daily Security Master, then request filtered CE/PE quotes for eligible
        expiries only.  Some Breeze accounts do not have the OptionChain facility
        enabled; those accounts fall back to exact-contract /quotes probes around
        the live underlying spot.
        """
        raw = getattr(getattr(self.instrument, "primary", None), "raw", {}) or {}
        if not isinstance(raw, dict):
            return False
        if raw.get("chain_candidates") and not force_refresh:
            return True
        stock_code = str(raw.get("breeze_stock_code") or raw.get("stock_code") or getattr(self.instrument, "asset_id", "")).upper()
        if not stock_code:
            return False
        try:
            self.api.preflight_session()
            master_rows = self.api.get_security_master_rows(
                cache_path=str(_cfg("ICICI_SECURITY_MASTER_CACHE_PATH", "data/icici_security_master.zip")),
                require_current_trade_date=bool(_cfg("ICICI_SECURITY_MASTER_REQUIRE_TODAY", True)),
            )
            verified = eligible_nfo_master_option_rows(master_rows, stock_code)
        except Exception as exc:
            logger.error("ICICI session contract book failed: Security Master unavailable for %s: %s", stock_code, exc)
            return False
        if not verified:
            logger.error("ICICI session contract book failed: no verified NFO option definitions with lot size for %s", stock_code)
            return False
        expiries = sorted({contract_key(row)[0] for row in verified if contract_key(row)[0]})
        max_expiries = max(1, int(_cfg("ICICI_SESSION_BOOK_MAX_EXPIRIES", 2)))
        expiries = expiries[:max_expiries]
        verified = [row for row in verified if contract_key(row)[0] in set(expiries)]
        quote_rows: list[dict[str, Any]] = []
        for expiry in expiries:
            for right in ("call", "put"):
                try:
                    breeze_throttle(f"option_chain:{stock_code}:{expiry}:{right}")
                    resp = self.api.get_option_chain_quotes(
                        stock_code=stock_code, exchange_code="NFO", product_type="options",
                        expiry_date=self.api._normalise_expiry(expiry), right=right,
                    )
                    quote_rows.extend(dict(row) for row in self._chain_rows(resp) if isinstance(row, dict))
                except Exception as exc:
                    logger.warning("ICICI filtered option-chain fetch failed %s %s %s: %s", stock_code, expiry, right, exc)
        candidates = merge_verified_chain_quotes(verified, quote_rows)
        chain_source = "daily_security_master_plus_filtered_option_chain"
        if not candidates:
            fallback_rows = self._quote_verified_contracts_fallback(
                verified,
                stock_code=stock_code,
                expiries=expiries,
                underlying_spot=float(underlying_spot or raw.get("underlying_spot_price") or raw.get("spot_price") or 0.0),
            )
            candidates = merge_verified_chain_quotes(verified, fallback_rows)
            if candidates:
                chain_source = "daily_security_master_plus_quotes_fallback"
        if not candidates:
            logger.error("ICICI session contract book failed: filtered OptionChain and quotes fallback returned no verified executable quotes for %s", stock_code)
            return False
        raw["chain_candidates"] = candidates
        raw["chain_candidates_deferred"] = False
        raw["chain_quality"] = chain_quality(candidates)
        raw["chain_source"] = chain_source
        logger.info(
            "ICICI verified option-chain hydrated for %s: rows=%d expiries=%s source=%s",
            stock_code, len(candidates), ",".join(expiries), chain_source,
        )
        return True

    def _quote_verified_contracts_fallback(
        self,
        verified: list[dict[str, Any]],
        *,
        stock_code: str,
        expiries: list[str],
        underlying_spot: float,
    ) -> list[dict[str, Any]]:
        """Hydrate exact master contracts with /quotes when /OptionChain is unavailable."""
        if not bool(_cfg("ICICI_SESSION_BOOK_QUOTES_FALLBACK_ENABLED", True)):
            return []
        spot = float(underlying_spot or 0.0)
        per_side = max(1, int(_cfg("ICICI_SESSION_BOOK_QUOTE_FALLBACK_STRIKES_PER_SIDE", 10)))
        max_contracts = max(4, int(_cfg("ICICI_SESSION_BOOK_QUOTE_FALLBACK_MAX_CONTRACTS", 60)))
        expiry_rank = {exp: i for i, exp in enumerate(expiries)}

        def right_of(row: dict[str, Any]) -> str:
            return str(contract_key(row)[1] or "").lower()

        def strike_of(row: dict[str, Any]) -> float:
            try:
                return float(contract_key(row)[2] or 0.0)
            except Exception:
                return 0.0

        def sort_key(row: dict[str, Any]) -> tuple[float, float, float]:
            exp = contract_key(row)[0]
            right = right_of(row)
            strike = strike_of(row)
            dist = abs(strike - spot) if spot > 0 and strike > 0 else 0.0
            # Target-delta index options are usually near-ATM/slightly OTM.
            # Penalize ITM contracts a little so fallback probes do not waste
            # quote calls on expensive vehicles when spot sits between strikes.
            itm_penalty = 0.0
            if spot > 0:
                if right == "call" and strike < spot:
                    itm_penalty = 0.15 * abs(strike - spot)
                elif right == "put" and strike > spot:
                    itm_penalty = 0.15 * abs(strike - spot)
            return (float(expiry_rank.get(exp, 999)), dist + itm_penalty, strike)

        selected: list[dict[str, Any]] = []
        for exp in expiries:
            exp_rows = [dict(row) for row in verified if contract_key(row)[0] == exp]
            for right in ("call", "put"):
                side_rows = [row for row in exp_rows if right_of(row) == right and strike_of(row) > 0]
                side_rows.sort(key=sort_key)
                selected.extend(side_rows[:per_side])
        selected.sort(key=sort_key)
        selected = selected[:max_contracts]
        if not selected:
            return []

        quote_rows: list[dict[str, Any]] = []
        failures = 0
        for row in selected:
            exp, right, strike = contract_key(row)
            route = dict(row)
            route["stock_code"] = stock_code
            route["exchange_code"] = "NFO"
            route["product_type"] = "Options"
            route["expiry_date"] = row.get("expiry_date") or row.get("ExpiryDate") or exp
            route["right"] = "Call" if right == "call" else "Put"
            route["strike_price"] = strike
            try:
                breeze_throttle(f"quote_fallback:{stock_code}:{exp}:{right}:{strike:g}")
                resp = self.api.get_quote_for_instrument(SimpleNamespace(raw=route, asset_id=stock_code))
                quote = self._first_response_row(resp)
                if not quote:
                    continue
                out = dict(route)
                out.update(quote)
                out["stock_code"] = stock_code
                out["exchange_code"] = "NFO"
                out["product_type"] = "Options"
                out["expiry_date"] = route["expiry_date"]
                out["right"] = route["right"]
                out["strike_price"] = strike
                out["runtime_lot_size"] = route.get("runtime_lot_size") or route.get("LotSize")
                out["quote_source"] = "breeze_quotes_contract_fallback"
                quote_rows.append(out)
            except Exception as exc:
                failures += 1
                logger.debug(
                    "ICICI exact-contract quote fallback failed %s %s %.0f %s: %s",
                    stock_code, exp, strike, right, exc,
                )
        logger.info(
            "ICICI exact-contract quote fallback for %s: probed=%d quotes=%d failures=%d spot=%.2f",
            stock_code, len(selected), len(quote_rows), failures, spot,
        )
        return quote_rows

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
        rows = ICICIOptionDataManager._chain_rows(resp)
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
            self._candles = {tf: deque(maxlen=600) for tf in ("1m", "5m", "15m", "1h", "4h", "1d")}
            self._trades.clear()

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
            logger.warning("ICICI %s vehicle rejected: invalid executable quote/lot symbol=%s", phase, choice.selected_symbol)
            return False, {}
        spread = ask - bid
        spread_bps = spread / max((ask + bid) / 2.0, 1e-9) * 10000.0
        max_spread_bps = float(_cfg("ICICI_OPTION_MAX_SELECTION_SPREAD_BPS", 120.0))
        if spread_bps > max_spread_bps:
            logger.warning("ICICI %s vehicle rejected: spread %.1fbps exceeds %.1fbps symbol=%s", phase, spread_bps, max_spread_bps, choice.selected_symbol)
            return False, {"spread_bps": spread_bps}
        min_depth = lot * max(0.0, float(_cfg("ICICI_OPTION_MIN_BOOK_LOTS", 1.0)))
        visible_depth = min(bid_qty, ask_qty)
        if visible_depth < min_depth:
            logger.warning("ICICI %s vehicle rejected: visible depth %.0f below lot requirement %.0f symbol=%s", phase, visible_depth, min_depth, choice.selected_symbol)
            return False, {"spread_bps": spread_bps, "visible_depth": visible_depth}
        premium_atr = self._option_premium_atr("1m", period=int(_cfg("ICICI_OPTION_EXECUTION_ATR_PERIOD", 14)))
        if premium_atr <= 0:
            logger.warning("ICICI %s vehicle rejected: option-premium 1m ATR unavailable symbol=%s", phase, choice.selected_symbol)
            return False, {"spread_bps": spread_bps, "visible_depth": visible_depth}
        spread_to_atr = spread / premium_atr
        max_ratio = float(_cfg("ICICI_OPTION_MAX_SPREAD_TO_1M_ATR", 0.35))
        if spread_to_atr > max_ratio:
            logger.warning("ICICI %s vehicle rejected: spread/1mATR %.3f exceeds %.3f symbol=%s", phase, spread_to_atr, max_ratio, choice.selected_symbol)
            return False, {"spread_bps": spread_bps, "visible_depth": visible_depth, "premium_atr": premium_atr, "spread_to_atr": spread_to_atr}
        return True, {"spread_bps": spread_bps, "visible_depth": visible_depth, "premium_atr": premium_atr, "spread_to_atr": spread_to_atr}

    def _prewarm_session_vehicle(self, choice) -> bool:
        route = self._route_field_snapshot()
        try:
            apply_contract_choice(self.instrument, choice)
            self._clear_option_execution_state()
            self._warmup(historical_only=False)
            with self._lock:
                if self._last_price <= 0 or len(self._candles.get("1m", ())) < int(_cfg("ICICI_OPTION_MIN_READY_1M_BARS", 20)):
                    return False
            acceptable, metrics = self._live_execution_liquidity_check(choice, phase="session-prewarm")
            if not acceptable:
                return False
            with self._lock:
                self._contract_snapshots[self._snapshot_key(choice)] = {
                    "last_price": self._last_price, "last_quote_ts": self._last_quote_ts,
                    "best_bid": self._best_bid, "best_ask": self._best_ask,
                    "best_bid_qty": self._best_bid_qty, "best_ask_qty": self._best_ask_qty,
                    "liquidity_metrics": dict(metrics),
                    "candles": {tf: list(rows) for tf, rows in self._candles.items()},
                }
            return True
        finally:
            self._restore_route_fields(route)
            self._clear_option_execution_state()

    def prepare_session_contract_book(self, underlying_spot: float, available_funds: float, *, force_refresh: bool = False, reason: str = "session_start") -> bool:
        self._session_book_last_refresh_attempt_ts = time.time()
        if not self._is_chain_mode():
            return True
        if underlying_spot <= 0 or available_funds <= 0:
            logger.error("ICICI session contract book rejected: underlying spot/funds not ready spot=%.4f funds=%.2f", underlying_spot, available_funds)
            return False
        if not self._hydrate_chain_candidates(force_refresh=force_refresh, underlying_spot=underlying_spot):
            return False
        # Build provisionally: publish a session book only after both vehicles
        # pass premium-history and executable-liquidity prewarm.
        book = build_session_contract_book(self.instrument, underlying_spot=underlying_spot, available_funds=available_funds, commit=False)
        if book is None:
            logger.error("ICICI session contract book rejected: cannot select both executable CE and PE vehicles")
            return False
        if bool(_cfg("ICICI_SESSION_BOOK_PREWARM_EXECUTION_DATA", True)):
            if not self._prewarm_session_vehicle(book.call) or not self._prewarm_session_vehicle(book.put):
                logger.error("ICICI session contract book rejected: CE/PE option premium historical/quote warmup failed")
                return False
        raw = getattr(getattr(self.instrument, "primary", None), "raw", {}) or {}
        if not isinstance(raw, dict):
            return False
        raw["session_contract_book"] = book.as_dict()
        raw["session_contract_book_status"] = "READY"
        raw["session_contract_book_mode"] = "preselected_call_and_put_live_direction"
        self._session_book_last_refresh_ts = time.time()
        logger.info(
            "ICICI SESSION CONTRACT BOOK READY [%s] reason=%s spot=%.2f funds=₹%.2f | "
            "CE=%s strike=%.2f expiry=%s lot=%.0f prem=₹%.2f score=%.3f delta=%+.3f theta/prem=%.4f | "
            "PE=%s strike=%.2f expiry=%s lot=%.0f prem=₹%.2f score=%.3f delta=%+.3f theta/prem=%.4f",
            book.trade_date_ist, reason, underlying_spot, available_funds,
            book.call.selected_symbol, book.call.strike, book.call.expiry, float(book.call.raw.get("runtime_lot_size", 0.0) or 0.0), float(book.call.raw.get("selected_entry_premium", 0.0) or 0.0), book.call.score, book.call.delta, book.call.theta_to_premium,
            book.put.selected_symbol, book.put.strike, book.put.expiry, float(book.put.raw.get("runtime_lot_size", 0.0) or 0.0), float(book.put.raw.get("selected_entry_premium", 0.0) or 0.0), book.put.score, book.put.delta, book.put.theta_to_premium,
        )
        return True

    def _activate_session_vehicle(self, choice) -> bool:
        apply_contract_choice(self.instrument, choice)
        snapshot = self._contract_snapshots.get(self._snapshot_key(choice))
        if snapshot:
            with self._lock:
                self._last_price = float(snapshot.get("last_price", 0.0) or 0.0)
                self._last_quote_ts = float(snapshot.get("last_quote_ts", 0.0) or 0.0)
                self._best_bid = float(snapshot.get("best_bid", 0.0) or 0.0)
                self._best_ask = float(snapshot.get("best_ask", 0.0) or 0.0)
                self._best_bid_qty = float(snapshot.get("best_bid_qty", 0.0) or 0.0)
                self._best_ask_qty = float(snapshot.get("best_ask_qty", 0.0) or 0.0)
                self._candles = {tf: deque(rows, maxlen=600) for tf, rows in (snapshot.get("candles") or {}).items()}
        else:
            self._warmup(historical_only=False)
        # Execution uses a fresh option quote even though the contract was chosen
        # at session start.  Contract selection is not re-run on each signal.
        self._refresh_quote()
        if self._last_price <= 0 or not self.is_price_fresh(float(_cfg("ICICI_OPTION_MAX_QUOTE_STALE_SEC", 10.0))):
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
        if not self._running:
            with self._lock:
                self._poll_generation += 1
                generation = self._poll_generation
                self._running = True
            self._thread = threading.Thread(target=self._poll_loop, args=(generation,), name=f"icici-option-dm-{getattr(self.instrument,'asset_id','')}", daemon=True)
            self._thread.start()
        return True

    def select_contract_for_thesis(self, thesis_side: str, underlying_spot: float = 0.0, available_funds: float = 0.0):
        choice, status = select_contract_from_session_book(
            self.instrument, thesis_side, underlying_spot=underlying_spot, available_funds=available_funds)
        if choice is None:
            now = time.time()
            min_refresh = float(_cfg("ICICI_SESSION_BOOK_MIN_REFRESH_SEC", 900.0))
            urgent_cooldown = float(_cfg("ICICI_SESSION_BOOK_URGENT_REFRESH_COOLDOWN_SEC", 30.0))
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
                logger.warning("ICICI preselected session vehicle unavailable for %s thesis=%s reason=%s", getattr(self.instrument, "asset_id", "?"), thesis_side, status)
                return None
        if not self._activate_session_vehicle(choice):
            logger.error("ICICI preselected vehicle failed fresh quote activation: %s", choice.selected_symbol)
            return None
        # The session book was built earlier, but the final affordability and
        # executable spread test must be repeated using the fresh entry quote.
        raw = getattr(getattr(self.instrument, "primary", None), "raw", {}) or {}
        lot = float(choice.raw.get("runtime_lot_size", 0.0) or 0.0)
        funds = max(0.0, float(available_funds or 0.0))
        fraction = max(0.01, min(1.0, float(_cfg("ICICI_OPTION_MAX_FUNDS_FRACTION_PER_TRADE", 0.42))))
        buffer_inr = max(0.0, float(_cfg("ICICI_OPTION_MIN_CASH_BUFFER_INR", 0.0)))
        live_cost = float(self._last_price or 0.0) * lot
        max_cost = max(0.0, funds - buffer_inr) * fraction
        if lot <= 0 or live_cost <= 0 or max_cost <= 0 or live_cost > max_cost:
            logger.warning("ICICI session vehicle rejected at execution: live cost ₹%.2f exceeds budget ₹%.2f symbol=%s", live_cost, max_cost, choice.selected_symbol)
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
            "ICICI SESSION VEHICLE ACTIVATED symbol=%s underlying=%s thesis=%s | strike=%.2f expiry=%s DTE=%.1f "
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
        self._restore_route_fields(dict(self._underlying_route_fields))
        self._selected_contract = None
        self._clear_option_execution_state()
        logger.info("ICICI execution vehicle released after confirmed FLAT; next entry will activate today’s direction-specific session contract")

    def register_strategy(self, strategy) -> None:
        self._strategy_ref = strategy

    def start(self) -> bool:
        try:
            session = icici_market_session_state()
            if not session.is_open and bool(_cfg("ICICI_ANALYZE_ONLY_DURING_MARKET_SESSION", True)):
                self.is_ready = False
                logger.warning(
                    "ICICI option DM dormant: %s; NIFTY option analysis/trading disabled outside NSE/NFO hours",
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
                    "ICICI option DM ready for session-book preparation [%s]; CE/PE vehicles will be preselected before signal execution",
                    getattr(self.instrument, "asset_id", "?"),
                )
                return True
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
            with self._lock:
                self._poll_generation += 1
                generation = self._poll_generation
                self._running = True
            self.is_ready = True
            self._thread = threading.Thread(target=self._poll_loop, args=(generation,), name=f"icici-option-dm-{getattr(self.instrument,'asset_id','')}", daemon=True)
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
        with self._lock:
            self._poll_generation += 1
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

    def _poll_loop(self, generation: int | None = None) -> None:
        interval = float(_cfg("ICICI_OPTION_QUOTE_POLL_SEC", 2.0))
        owned_generation = int(self._poll_generation if generation is None else generation)
        while True:
            with self._lock:
                if not self._running or owned_generation != self._poll_generation:
                    return
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
            # Breeze v2 uses 1minute/5minute/30minute/1day; v1 uses
            # minute/5minute/30minute/day. Never send v1 units to v2.
            v2_req["interval"] = {
                "minute": "1minute", "5minute": "5minute",
                "30minute": "30minute", "day": "1day",
            }.get(str(v2_req.get("interval") or ""), str(v2_req.get("interval") or ""))
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
            ts_ms = self._parse_ts_ms(ts)
            high = max(float(h or c), float(o or c), float(c))
            low = min(float(l or c), float(o or c), float(c))
            parsed.append({
                "t": ts_ms,
                "o": float(o or c),
                "h": high,
                "l": low,
                "c": float(c),
                "v": float(v or 0.0),
                "timestamp": ts_ms / 1000.0,
                "open": float(o or c),
                "high": high,
                "low": low,
                "close": float(c),
                "volume": float(v or 0.0),
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

        # Breeze returns many NSE/NFO timestamps without timezone.  These are
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
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d-%b-%Y %H:%M:%S", "%Y-%m-%d"):
            try:
                return int(datetime.strptime(text, fmt).replace(tzinfo=_ICICI_LOCAL_TZ).timestamp() * 1000)
            except Exception:
                pass
        return int(time.time() * 1000)

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
        bid = self._float_first(row, ("best_bid_price", "best_bid", "bid", "bPrice", "bid_price"))
        ask = self._float_first(row, ("best_offer_price", "best_ask_price", "best_ask", "ask", "sPrice", "ask_price", "offer_price"))
        bid_qty = self._float_first(row, ("best_bid_quantity", "bid_quantity", "bid_qty", "bQty"))
        ask_qty = self._float_first(row, ("best_offer_quantity", "best_ask_quantity", "ask_quantity", "ask_qty", "sQty"))
        now = time.time()
        with self._lock:
            self._last_price = px
            self._best_bid = bid
            self._best_ask = ask
            self._best_bid_qty = bid_qty
            self._best_ask_qty = ask_qty
            self._last_quote_ts = now
            self._trades.append({"price": px, "quantity": self._float_first(row, ("quantity", "volume", "total_quantity_traded")), "side": "buy", "timestamp": now, "source": "icici_quote"})

    def get_last_update(self) -> float:
        with self._lock:
            return float(self._last_quote_ts or 0.0)

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
            bid_qty = float(self._best_bid_qty or 0.0)
            ask_qty = float(self._best_ask_qty or 0.0)
            ts = self._last_quote_ts
        # Never fabricate order-book depth: liquidity and order-flow features use
        # only executable size Breeze actually returned.
        return {"bids": [[bid, bid_qty]] if bid > 0 and bid_qty > 0 else [], "asks": [[ask, ask_qty]] if ask > 0 and ask_qty > 0 else [], "timestamp": ts, "_sources": 1, "_executable_source": "icici_quote"}

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

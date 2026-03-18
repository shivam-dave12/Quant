"""
execution/order_manager.py — Exchange-Agnostic Order Manager
=============================================================
Single OrderManager class that works with any exchange API adapter
(CoinSwitchAPI or DeltaAPI) via constructor injection.

The ExecutionRouter (router.py) instantiates one of each and routes
all calls to the active one.  Switching exchanges at runtime is a
router concern — the OrderManager itself is stateless re: exchange choice.

Key differences handled per-exchange
--------------------------------------
  Response parsing:
    CoinSwitch → success in resp["data"]["order_id"]
    Delta      → success in resp["result"]["id"]  (when resp["success"]==True)

  Order types:
    CoinSwitch → "STOP_MARKET", "TAKE_PROFIT_MARKET"
    Delta      → "stop_loss_order", "take_profit_order"  (bracket legs)
                 OR "STOP_LOSS_MARKET", "TAKE_PROFIT_MARKET" on stop endpoint

  Rate limiting:
    CoinSwitch → 3.0 s minimum between any calls
    Delta      → 0.25 s minimum

  Leverage:
    CoinSwitch → set_leverage(symbol, exchange, leverage)
    Delta      → set_leverage(product_id, leverage)

  Quantity units:
    CoinSwitch → float BTC
    Delta      → int contracts (1 contract = DELTA_CONTRACT_VALUE_BTC BTC)

All exchange-specific behaviour is encapsulated in the _Adapter inner
classes so the OrderManager logic never branches on exchange type.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime
from enum import Enum
from typing import Dict, Optional, Tuple

import sys, os; sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

logger = logging.getLogger(__name__)


# ── Cancel result enum (exported; strategy imports from here) ─────────────────

class CancelResult(Enum):
    SUCCESS        = "SUCCESS"
    PARTIAL_FILL   = "PARTIAL_FILL"
    ALREADY_FILLED = "ALREADY_FILLED"
    NOT_FOUND      = "NOT_FOUND"
    FAILED         = "FAILED"


# ── Per-exchange rate limiters ────────────────────────────────────────────────

class _RateLimiter:
    """Thread-safe token-bucket rate limiter with 429 backoff."""

    BACKOFF_SECONDS = 15.0

    def __init__(self, min_interval_sec: float) -> None:
        self._lock             = threading.RLock()
        self._min_interval_sec = min_interval_sec
        self._last_ts          = 0.0
        self._backoff_until    = 0.0

    def wait(self) -> None:
        while True:
            with self._lock:
                now = time.time()
                if now < self._backoff_until:
                    sleep_needed = self._backoff_until - now
                else:
                    elapsed = now - self._last_ts
                    if elapsed >= self._min_interval_sec:
                        self._last_ts = now
                        return
                    sleep_needed = self._min_interval_sec - elapsed
            time.sleep(sleep_needed)

    def notify_429(self) -> None:
        with self._lock:
            self._backoff_until = time.time() + self.BACKOFF_SECONDS
            logger.warning(f"429 detected — all calls frozen for {self.BACKOFF_SECONDS}s")

    def set_interval(self, seconds: float) -> None:
        with self._lock:
            self._min_interval_sec = max(0.1, seconds)


# Global limiters — one per exchange (shared across all OrderManager instances)
_CS_LIMITER    = _RateLimiter(min_interval_sec=3.0)
_DELTA_LIMITER = _RateLimiter(min_interval_sec=0.25)

# Also keep a module-level alias for legacy imports (quant_strategy does
# `from execution.order_manager import GlobalRateLimiter`)
class GlobalRateLimiter:
    """Legacy shim — routes to the active exchange limiter."""
    _active = _CS_LIMITER

    @classmethod
    def wait(cls): cls._active.wait()
    @classmethod
    def notify_429(cls): cls._active.notify_429()
    @classmethod
    def set_min_interval(cls, s): cls._active.set_interval(s)
    @classmethod
    def set_active(cls, limiter: _RateLimiter): cls._active = limiter


# ── Exchange adapters — encapsulate wire-format differences ──────────────────

class _CoinSwitchAdapter:
    """Normalises CoinSwitch API responses to canonical dicts."""

    def __init__(self, api) -> None:
        self.api     = api
        self.limiter = _CS_LIMITER
        self.symbol  = config.COINSWITCH_SYMBOL
        self.exchange_id = config.COINSWITCH_EXCHANGE

    def extract_order_id(self, resp: Dict) -> Optional[str]:
        if not isinstance(resp, dict):
            return None
        data = resp.get("data")
        if isinstance(data, dict):
            oid = data.get("order_id") or data.get("id")
            return str(oid) if oid else None
        return None

    def extract_status(self, order_data: Dict) -> str:
        raw = str(order_data.get("status", "")).upper()
        _MAP = {
            "EXECUTED": "FILLED", "FILLED": "FILLED",
            "COMPLETELY_FILLED": "FILLED",
            "PARTIALLY_FILLED": "PARTIAL_FILL",
            "PARTIALLY_EXECUTED": "PARTIAL_FILL",
            "CANCELLED": "CANCELLED", "CANCELED": "CANCELLED",
            "REJECTED": "CANCELLED", "EXPIRED": "CANCELLED",
            "OPEN": "PENDING", "PENDING": "PENDING", "NEW": "PENDING",
            "UNTRIGGERED": "PENDING", "TRIGGERED": "PENDING",
            "ACTIVE": "PENDING", "RAISED": "PENDING",
        }
        return _MAP.get(raw, "UNKNOWN")

    def extract_fill_price(self, order_data: Dict) -> Optional[float]:
        for f in ("avg_execution_price", "avg_price", "average_price", "price"):
            v = order_data.get(f)
            if v:
                try:
                    p = float(v)
                    if p > 0: return p
                except (ValueError, TypeError):
                    pass
        return None

    def extract_filled_qty(self, order_data: Dict) -> float:
        for f in ("exec_quantity", "executed_qty", "filled_quantity",
                  "executed_quantity"):
            v = order_data.get(f)
            if v:
                try:
                    q = float(v)
                    if q > 0: return q
                except (ValueError, TypeError):
                    pass
        return 0.0

    def place_order(self, side: str, order_type: str, quantity: float,
                    price: Optional[float] = None,
                    trigger_price: Optional[float] = None,
                    reduce_only: bool = False) -> Optional[Dict]:
        self.limiter.wait()
        resp = self.api.place_order(
            symbol        = self.symbol,
            side          = side,
            order_type    = order_type,
            quantity      = quantity,
            exchange      = self.exchange_id,
            price         = price,
            trigger_price = trigger_price,
            reduce_only   = reduce_only,
        )
        oid = self.extract_order_id(resp)
        if not oid:
            sc = resp.get("status_code", 0) if isinstance(resp, dict) else 0
            return {"_raw": resp, "_sc": sc, "_error": True}
        data = (resp.get("data") or {}) if isinstance(resp, dict) else {}
        data["order_id"] = oid
        return data

    def cancel_order(self, order_id: str) -> Dict:
        self.limiter.wait()
        return self.api.cancel_order(order_id, exchange=self.exchange_id) or {}

    def get_order(self, order_id: str) -> Optional[Dict]:
        self.limiter.wait()
        resp = self.api.get_order(order_id, exchange=self.exchange_id)
        if isinstance(resp, dict) and "data" in resp:
            d = resp["data"]
            return d.get("order", d) if isinstance(d, dict) else None
        return None

    def get_open_orders(self, symbol: str) -> Optional[list]:
        self.limiter.wait()
        resp = self.api.get_open_orders(exchange=self.exchange_id, symbol=symbol)
        if not isinstance(resp, dict) or resp.get("error"):
            return None
        raw = resp.get("data", [])
        return raw if isinstance(raw, list) else []

    def get_positions(self, symbol: str) -> Optional[Dict]:
        self.limiter.wait()
        resp = self.api.get_positions(exchange=self.exchange_id, symbol=symbol)
        if not isinstance(resp, dict) or resp.get("error"):
            return None
        return resp.get("data", {})

    def get_balance(self) -> Dict:
        return self.api.get_balance(currency="USDT")

    def set_leverage(self, leverage: int, product_id: Optional[int] = None) -> Dict:
        self.limiter.wait()
        return self.api.set_leverage(
            symbol   = self.symbol,
            exchange = self.exchange_id,
            leverage = leverage,
        )

    def normalise_position(self, raw) -> Optional[Dict]:
        """Turn CoinSwitch position response into a canonical dict."""
        positions = raw if isinstance(raw, list) else ([raw] if isinstance(raw, dict) else [])
        for pos in positions:
            if not isinstance(pos, dict):
                continue
            sym = str(pos.get("symbol", "")).upper()
            if config.COINSWITCH_SYMBOL.upper() not in sym:
                continue
            size = 0.0
            for f in ("size", "quantity", "position_size", "net_quantity"):
                v = pos.get(f)
                if v:
                    try:
                        size = abs(float(v))
                        if size > 0: break
                    except (ValueError, TypeError): pass
            side = None
            if size > 0:
                rs = str(pos.get("side", pos.get("position_side", ""))).upper()
                side = "LONG" if rs in ("BUY", "LONG") else \
                       "SHORT" if rs in ("SELL", "SHORT") else None
            entry = 0.0
            for f in ("entry_price", "avg_price", "average_price"):
                v = pos.get(f)
                if v:
                    try:
                        entry = float(v)
                        if entry > 0: break
                    except (ValueError, TypeError): pass
            upnl = 0.0
            try: upnl = float(pos.get("unrealized_pnl", 0))
            except (ValueError, TypeError): pass
            return {"side": side, "size": size, "entry_price": entry,
                    "unrealized_pnl": upnl, "raw": pos}
        return {"side": None, "size": 0.0, "entry_price": 0.0,
                "unrealized_pnl": 0.0}


class _DeltaAdapter:
    """Normalises Delta Exchange API responses to canonical dicts."""

    def __init__(self, api) -> None:
        self.api      = api
        self.limiter  = _DELTA_LIMITER
        self.symbol   = getattr(config, 'DELTA_SYMBOL', 'BTCUSD')
        self._pid_cache: Optional[int] = None

    def _get_product_id(self) -> Optional[int]:
        if self._pid_cache:
            return self._pid_cache
        try:
            pid = self.api.get_product_id(self.symbol)
            if pid:
                self._pid_cache = pid
            return pid
        except Exception:
            return None

    def extract_order_id(self, resp: Dict) -> Optional[str]:
        if not isinstance(resp, dict) or not resp.get("success"):
            return None
        result = resp.get("result")
        if isinstance(result, dict):
            # Delta returns id as integer in result.id
            oid = result.get("id") or result.get("order_id")
            return str(int(oid)) if oid else None
        return None

    def extract_status(self, order_data: Dict) -> str:
        raw = str(order_data.get("state",
                  order_data.get("status", ""))).upper()
        _MAP = {
            "OPEN": "PENDING", "PENDING": "PENDING",
            "CLOSED": "FILLED", "FILLED": "FILLED",
            "CANCELLED": "CANCELLED", "CANCELED": "CANCELLED",
            "REJECTED": "CANCELLED",
            "PARTIALLY_FILLED": "PARTIAL_FILL",
        }
        return _MAP.get(raw, "UNKNOWN")

    def extract_fill_price(self, order_data: Dict) -> Optional[float]:
        for f in ("average_fill_price", "avg_fill_price", "fill_price",
                  "limit_price", "price"):
            v = order_data.get(f)
            if v:
                try:
                    p = float(v)
                    if p > 0: return p
                except (ValueError, TypeError):
                    pass
        return None

    def extract_filled_qty(self, order_data: Dict) -> float:
        for f in ("filled_size", "executed_qty", "size"):
            v = order_data.get(f)
            if v:
                try:
                    q = float(v)
                    if q > 0: return q
                except (ValueError, TypeError): pass
        return 0.0

    def place_order(self, side: str, order_type: str, quantity: float,
                    price: Optional[float] = None,
                    trigger_price: Optional[float] = None,
                    reduce_only: bool = False,
                    stop_order_type: Optional[str] = None) -> Optional[Dict]:
        self.limiter.wait()
        # symbol is the primary key; Delta API resolves product_id internally
        # Convert BTC quantity → integer contracts
        # Delta India inverse perpetual: 1 contract = DELTA_CONTRACT_VALUE_BTC BTC
        _cv = float(getattr(config, 'DELTA_CONTRACT_VALUE_BTC', 0.001))
        contracts = max(1, round(quantity / _cv)) if _cv > 0 else int(quantity)
        resp = self.api.place_order(
            symbol          = self.symbol,
            side            = side.lower(),
            order_type      = order_type,
            size            = contracts,       # Delta: integer contracts
            limit_price     = float(price) if price else None,
            stop_price      = float(trigger_price) if trigger_price else None,
            reduce_only     = reduce_only,
            stop_order_type = stop_order_type,  # "stop_loss_order" | "take_profit_order"
        )
        oid = self.extract_order_id(resp)
        if not oid:
            sc = resp.get("status_code", 0) if isinstance(resp, dict) else 0
            return {"_raw": resp, "_sc": sc, "_error": True}
        result = resp.get("result", {}) if isinstance(resp, dict) else {}
        result["order_id"] = oid
        return result

    def place_bracket_limit_entry(self, side: str, quantity: float,
                                  limit_price: float,
                                  sl_price: float,
                                  tp_price: float) -> Optional[Dict]:
        # Bracket limit order: entry + SL + TP in a single Delta API call.
        # Avoids bad_schema from separate stop/take-profit order placement.
        self.limiter.wait()
        _cv = float(getattr(config, "DELTA_CONTRACT_VALUE_BTC", 0.001))
        contracts = max(1, round(quantity / _cv)) if _cv > 0 else int(quantity)
        resp = self.api.place_order(
            symbol                    = self.symbol,
            side                      = side.lower(),
            order_type                = "limit",
            size                      = contracts,
            limit_price               = float(limit_price),
            bracket_stop_loss_price   = float(sl_price),
            bracket_take_profit_price = float(tp_price),
            post_only                 = False,
            time_in_force             = "gtc",
        )
        oid = self.extract_order_id(resp)
        if not oid:
            sc = resp.get("status_code", 0) if isinstance(resp, dict) else 0
            return {"_raw": resp, "_sc": sc, "_error": True}
        result = resp.get("result", {}) if isinstance(resp, dict) else {}
        result["order_id"] = oid
        return result

    def cancel_order(self, order_id: str) -> Dict:
        self.limiter.wait()
        # Pass product_id so api.cancel_order() can include it in the DELETE body.
        # Delta requires product_id in the body; without it the request returns 404.
        pid = self._get_product_id()
        return self.api.cancel_order(order_id=order_id, product_id=pid) or {}

    def edit_order(self, order_id: str, new_stop_price: float,
                   new_limit_price: Optional[float] = None) -> Optional[Dict]:
        """
        Atomically modify a stop order's trigger price (and limit price for stop-limits).

        PUT /v2/orders — id and product_id in body (confirmed from API doc).

        For stop-limit trailing SLs, always pass new_limit_price alongside new_stop_price.
        The API EditOrderRequest schema supports both fields — one round-trip, atomic.

        Returns a result dict on success, or {"_error": True, "_sc": sc} on failure.
        """
        self.limiter.wait()
        resp = self.api.edit_order(
            order_id    = order_id,
            stop_price  = new_stop_price,
            limit_price = new_limit_price,   # None for stop-market, float for stop-limit
        )
        if resp and resp.get("success"):
            result = resp.get("result", {}) or {}
            result["order_id"] = str(result.get("order_id", order_id))
            return result
        sc  = (resp or {}).get("status_code", 0)
        err = (resp or {}).get("error", "")
        return {"_error": True, "_sc": sc, "_err_msg": err}

    def get_order(self, order_id: str) -> Optional[Dict]:
        self.limiter.wait()
        resp = self.api.get_order(order_id=order_id)
        if isinstance(resp, dict) and resp.get("success"):
            result = resp.get("result")
            return result if isinstance(result, dict) else None
        return None

    def get_open_orders(self, symbol: str) -> Optional[list]:
        self.limiter.wait()
        resp = self.api.get_open_orders(symbol=self.symbol)
        if not resp or not isinstance(resp, dict) or not resp.get("success"):
            return None
        raw = resp.get("result", [])
        return raw if isinstance(raw, list) else []

    def get_positions(self, symbol: str) -> Optional[Dict]:
        self.limiter.wait()
        # Delta get_positions uses product_symbol parameter
        resp = self.api.get_positions(product_symbol=self.symbol)
        if isinstance(resp, dict) and resp.get("success"):
            return resp.get("result", {})
        return None

    def get_balance(self) -> Dict:
        currency = getattr(config, 'DELTA_BALANCE_CURRENCY', 'USD')
        return self.api.get_balance(currency=currency)

    def set_leverage(self, leverage: int, product_id: Optional[int] = None) -> Dict:
        self.limiter.wait()
        # Pass symbol — Delta API resolves product_id internally via _symbol_to_product_id
        return self.api.set_leverage(symbol=self.symbol, leverage=leverage)

    def normalise_position(self, raw) -> Optional[Dict]:
        """Turn Delta position response into a canonical dict."""
        # raw may be a list (from get_positions result) or a single dict
        if isinstance(raw, dict):
            # Some Delta endpoints wrap in {result: [...]}
            inner = raw.get("result", raw)
            positions = inner if isinstance(inner, list) else [inner]
        elif isinstance(raw, list):
            positions = raw
        else:
            positions = []

        delta_sym = getattr(config, 'DELTA_SYMBOL', 'BTCUSD').upper()
        for pos in positions:
            if not isinstance(pos, dict): continue
            sym = str(pos.get("product_symbol",
                     pos.get("symbol", ""))).upper()
            # Match BTCUSD against BTCUSD or any ticker containing it
            if delta_sym not in sym and sym not in delta_sym: continue
            size = 0.0
            for f in ("size", "quantity", "net_size"):
                v = pos.get(f)
                if v is not None:
                    try:
                        size = abs(float(v))
                        if size > 0: break
                    except (ValueError, TypeError): pass
            side = None
            if size > 0:
                rs = str(pos.get("direction",
                         pos.get("side", ""))).upper()
                # Delta uses "buy"/"sell" in direction field
                side = "LONG" if rs in ("LONG", "BUY") else \
                       "SHORT" if rs in ("SHORT", "SELL") else None
            entry = 0.0
            for f in ("entry_price", "avg_entry_price"):
                v = pos.get(f)
                if v:
                    try:
                        entry = float(v)
                        if entry > 0: break
                    except (ValueError, TypeError): pass
            upnl = 0.0
            try: upnl = float(pos.get("unrealized_pnl", 0))
            except (ValueError, TypeError): pass
            return {"side": side, "size": size, "entry_price": entry,
                    "unrealized_pnl": upnl, "raw": pos}
        return {"side": None, "size": 0.0, "entry_price": 0.0,
                "unrealized_pnl": 0.0}


# ── Main OrderManager ─────────────────────────────────────────────────────────

class OrderManager:
    """
    Exchange-agnostic order manager.
    Inject a CoinSwitchAPI or DeltaAPI; this class handles the rest.
    """

    _MAX_RETRIES       = 2     # was 3 — 3 attempts × 30s timeout = 90s minimum block
    _RETRY_BASE_SLEEP  = 1.0   # was 3.0 — base sleep for exponential backoff
    _MAX_401_RETRIES   = 2     # was 3 — auth errors rarely fix themselves
    _401_RETRY_DELAY   = 1.0   # was 2.0

    def __init__(self, api, exchange_name: str = "coinswitch") -> None:
        exch = exchange_name.lower()
        if exch == "delta":
            self._adapter = _DeltaAdapter(api)
            GlobalRateLimiter.set_active(_DELTA_LIMITER)
        else:
            self._adapter = _CoinSwitchAdapter(api)
            GlobalRateLimiter.set_active(_CS_LIMITER)

        self.api            = api         # kept for legacy access
        self._exchange_name = exch
        self._orders_lock   = threading.RLock()
        self.active_orders: Dict[str, Dict] = {}
        self.order_history: list            = []
        self._rate_window_start = time.time()
        self._rate_window_count = 0
        self._open_orders_404   = False

        # Expose for callers that do order_manager.CancelResult
        self.CancelResult = CancelResult

        logger.info(f"✅ OrderManager initialised (exchange={exch})")

    @property
    def limiter(self) -> _RateLimiter:
        return self._adapter.limiter

    # ── Internal helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _normalize_side(side: str) -> str:
        s = side.upper().strip()
        if s in ("LONG", "BUY"):   return "BUY"
        if s in ("SHORT", "SELL"): return "SELL"
        raise ValueError(f"Invalid side '{side}'")

    def _check_window_rate_limit(self) -> bool:
        now = time.time()
        if now - self._rate_window_start > 60:
            self._rate_window_count = 0
            self._rate_window_start = now
        if self._rate_window_count >= config.RATE_LIMIT_ORDERS:
            logger.warning("Window rate limit reached")
            return False
        self._rate_window_count += 1
        return True

    def _place_with_retry(self, **kwargs) -> Optional[Dict]:
        """Place order with exponential-backoff retry on transient errors."""
        consecutive_401s = 0
        for attempt in range(self._MAX_RETRIES + self._MAX_401_RETRIES):
            result = self._adapter.place_order(**kwargs)

            # Success
            if result and not result.get("_error"):
                return result

            sc  = (result or {}).get("_sc", 0)
            raw = (result or {}).get("_raw", {})

            if sc == 429:
                self._adapter.limiter.notify_429()
                time.sleep(self._RETRY_BASE_SLEEP * (2 ** min(attempt, 4)))
                continue

            if sc in (500, 502, 503):
                time.sleep(self._RETRY_BASE_SLEEP * (2 ** min(attempt, 3)))
                continue

            if sc == 401:
                consecutive_401s += 1
                if consecutive_401s <= self._MAX_401_RETRIES:
                    time.sleep(self._401_RETRY_DELAY * consecutive_401s)
                    continue
                logger.error("401 persists after retries — giving up")
                return None

            # Non-retryable
            logger.error(f"place_order failed: sc={sc} raw={raw}")
            return None

        logger.error("place_order exhausted all retries")
        return None

    def _record_order(self, order_id: str, meta: Dict) -> None:
        with self._orders_lock:
            self.active_orders[order_id] = meta
            self.order_history.append(meta.copy())

    def _remove_active_order(self, order_id: str) -> None:
        with self._orders_lock:
            self.active_orders.pop(order_id, None)

    # ── Position query ────────────────────────────────────────────────────────

    def get_open_position(self) -> Optional[Dict]:
        try:
            # Adapter.get_positions uses self.symbol (exchange-correct)
            raw = self._adapter.get_positions(self._adapter.symbol)
            if raw is None:
                return None
            return self._adapter.normalise_position(raw)
        except Exception as e:
            logger.error(f"get_open_position error: {e}", exc_info=True)
            return None

    # ── Order status ──────────────────────────────────────────────────────────

    def get_order_status(self, order_id: str, retry_count: int = 2) -> Optional[Dict]:
        for attempt in range(retry_count):
            try:
                data = self._adapter.get_order(order_id)
                if data:
                    with self._orders_lock:
                        if order_id in self.active_orders:
                            self.active_orders[order_id]["status"] = \
                                self._adapter.extract_status(data)
                    return data
                if attempt < retry_count - 1:
                    time.sleep(1.0)   # was 2*(attempt+1) — reduced to limit blocking
            except Exception as e:
                logger.error(f"get_order_status error attempt {attempt+1}: {e}")
                if attempt < retry_count - 1:
                    time.sleep(1.0)
        return None

    def get_order_status_safe(self, order_id: str) -> str:
        try:
            data = self.get_order_status(order_id, retry_count=2)
            if data is None:
                return "UNKNOWN"
            return self._adapter.extract_status(data)
        except Exception as e:
            logger.error(f"get_order_status_safe error: {e}")
            return "UNKNOWN"

    def get_fill_details(self, order_id: str) -> Optional[Dict]:
        try:
            data = self.get_order_status(order_id, retry_count=2)
            if not data:
                return None
            status     = self._adapter.extract_status(data)
            fill_price = self._adapter.extract_fill_price(data)
            filled_qty = self._adapter.extract_filled_qty(data)
            req_qty    = 0.0
            for f in ("quantity", "size", "orig_qty"):
                v = data.get(f)
                if v:
                    try:
                        req_qty = float(v)
                        if req_qty > 0: break
                    except (ValueError, TypeError): pass
            is_partial = status == "PARTIAL_FILL"
            if filled_qty <= 0 and status == "FILLED":
                filled_qty = req_qty
            fill_pct = (filled_qty / req_qty * 100) if req_qty > 0 else 0.0
            return {
                "status":        status,
                "fill_price":    fill_price,
                "filled_qty":    filled_qty,
                "requested_qty": req_qty,
                "is_partial":    is_partial,
                "fill_pct":      fill_pct,
                "raw_data":      data,
            }
        except Exception as e:
            logger.error(f"get_fill_details error: {e}", exc_info=True)
            return None

    def extract_fill_price(self, order_data: Dict) -> float:
        p = self._adapter.extract_fill_price(order_data)
        if p:
            return p
        raise RuntimeError(f"No valid fill price in: {order_data}")

    # ── Order placement ───────────────────────────────────────────────────────

    def place_market_order(self, side: str, quantity: float,
                           reduce_only: bool = False) -> Optional[Dict]:
        try:
            if not self._check_window_rate_limit():
                return None
            api_side = self._normalize_side(side)
            logger.info(f"MARKET {side} qty={quantity} reduce_only={reduce_only}")
            data = self._place_with_retry(
                side=api_side, order_type="MARKET",
                quantity=quantity, reduce_only=reduce_only)
            if data:
                self._record_order(data["order_id"], {
                    "order_id": data["order_id"], "side": side,
                    "type": "MARKET", "quantity": quantity,
                    "status": data.get("status", "UNKNOWN"),
                    "timestamp": datetime.now().isoformat(),
                    "reduce_only": reduce_only,
                })
                logger.info(f"✅ Market order: {data['order_id']}")
            return data
        except Exception as e:
            logger.error(f"place_market_order error: {e}", exc_info=True)
            return None

    def place_limit_order(self, side: str, quantity: float,
                          price: float, reduce_only: bool = False) -> Optional[Dict]:
        try:
            if not self._check_window_rate_limit():
                return None
            api_side = self._normalize_side(side)
            logger.info(f"LIMIT {side} qty={quantity} @ ${price:,.2f}")
            data = self._place_with_retry(
                side=api_side, order_type="LIMIT",
                quantity=quantity, price=price, reduce_only=reduce_only)
            if data:
                self._record_order(data["order_id"], {
                    "order_id": data["order_id"], "side": side,
                    "type": "LIMIT", "quantity": quantity, "price": price,
                    "status": data.get("status", "UNKNOWN"),
                    "timestamp": datetime.now().isoformat(),
                })
                logger.info(f"✅ Limit order: {data['order_id']} @ ${price:,.2f}")
            return data
        except Exception as e:
            logger.error(f"place_limit_order error: {e}", exc_info=True)
            return None

    def place_limit_entry(self, side: str, quantity: float,
                          limit_price: float, timeout_sec: float = 25.0,
                          fallback_to_market: bool = True) -> Optional[Dict]:
        """Maker limit entry with adaptive polling and market fallback."""
        logger.info(f"🎯 Maker entry: {side} {quantity} @ ${limit_price:.2f} "
                    f"(timeout={timeout_sec:.0f}s)")

        data = self.place_limit_order(side=side, quantity=quantity,
                                      price=limit_price, reduce_only=False)
        if not data:
            if fallback_to_market:
                mdata = self.place_market_order(side=side, quantity=quantity)
                if mdata:
                    mdata["fill_type"] = "taker"
                    mdata["fill_price"] = 0.0
                return mdata
            return None

        order_id = data.get("order_id", "")
        if not order_id:
            return None

        deadline   = time.time() + timeout_sec
        poll_count = 0

        while time.time() < deadline:
            poll_interval = 2.5 if poll_count < 2 else 4.0
            time.sleep(poll_interval)
            poll_count += 1

            details = self.get_fill_details(order_id)
            if details is None:
                continue

            status = details.get("status", "")

            if status == "FILLED":
                fill_px = float(details.get("fill_price") or limit_price)
                data["fill_type"]  = "maker"
                data["fill_price"] = fill_px
                logger.info(f"✅ Maker fill: {order_id[:8]}… @ ${fill_px:.2f}")
                return data

            if status == "CANCELLED":
                logger.info(f"Limit {order_id[:8]}… cancelled by exchange — fallback")
                break

            if status == "PARTIAL_FILL":
                filled_qty = float(details.get("filled_qty") or 0)
                fill_px    = float(details.get("fill_price") or limit_price)
                logger.info(f"⚠️ Partial fill: {filled_qty:.4f} @ ${fill_px:.2f}")
                self.cancel_order(order_id)
                data["fill_type"]  = "maker"
                data["fill_price"] = fill_px
                data["quantity"]   = filled_qty
                return data

        # Timeout
        cancel_result = self.cancel_order(order_id)
        if cancel_result == CancelResult.ALREADY_FILLED:
            details = self.get_fill_details(order_id)
            fill_px = float((details or {}).get("fill_price") or limit_price)
            data["fill_type"]  = "maker"
            data["fill_price"] = fill_px
            return data

        if fallback_to_market:
            logger.info("Limit timeout — falling back to market order")
            mdata = self.place_market_order(side=side, quantity=quantity)
            if mdata:
                mdata["fill_type"]  = "taker"
                mdata["fill_price"] = 0.0
            return mdata
        logger.info(f"Limit order timeout after {timeout_sec:.0f}s — cancelled, no market fallback")
        return None

    def place_bracket_limit_entry(self, side: str, quantity: float,
                                  limit_price: float,
                                  sl_price: float,
                                  tp_price: float,
                                  timeout_sec: float = 45.0) -> Optional[Dict]:
        """
        Bracket entry for Delta: places a single limit order with embedded SL + TP.
        Polls until filled, then queries open orders to retrieve bracket child IDs.
        Returns None for non-Delta adapters (caller falls back to regular flow).

        Return dict keys on success:
          fill_price, fill_type, order_id, bracket_order=True,
          bracket_sl_order_id, bracket_tp_order_id,
          bracket_sl_price, bracket_tp_price
        """
        if not hasattr(self._adapter, "place_bracket_limit_entry"):
            return None  # Non-Delta: caller uses place_limit_entry + separate SL/TP

        logger.info(
            f"[BRACKET] {side.upper()} {quantity} @ ${limit_price:.2f} "
            f"SL=${sl_price:.2f} TP=${tp_price:.2f} (timeout={timeout_sec:.0f}s)"
        )

        data = self._adapter.place_bracket_limit_entry(
            side=side, quantity=quantity,
            limit_price=limit_price, sl_price=sl_price, tp_price=tp_price,
        )
        if not data or data.get("_error"):
            sc = (data or {}).get("_sc", 0)
            raw = (data or {}).get("_raw", {})
            logger.error(f"Bracket order failed: sc={sc} raw={raw}")
            return None

        order_id = data.get("order_id", "")
        if not order_id:
            return None
        logger.info(f"✅ Bracket order placed: {order_id} @ ${limit_price:.2f}")

        deadline   = time.time() + timeout_sec
        poll_count = 0

        while time.time() < deadline:
            poll_interval = 2.5 if poll_count < 2 else 4.0
            time.sleep(poll_interval)
            poll_count += 1

            details = self.get_fill_details(order_id)
            if details is None:
                continue
            status = details.get("status", "")

            if status == "FILLED":
                fill_px = float(details.get("fill_price") or limit_price)
                data["fill_type"]    = "maker"
                data["fill_price"]   = fill_px
                data["bracket_order"] = True
                logger.info(f"✅ Bracket fill: {order_id[:8]}… @ ${fill_px:.2f}")

                # Query open orders to retrieve bracket SL/TP child order IDs.
                # Delta creates children asynchronously after fill.
                # Bug 4 fix: was a 6-attempt blocking loop (up to 24s of sleep
                # while holding the strategy lock). Now: 2 fast attempts (3s
                # total) return immediately to the caller. If children are still
                # missing, a background thread polls for up to 90s and writes
                # the IDs into _pending_bracket_children so the reconcile loop
                # picks them up on the next pass.
                sl_oid = tp_oid = ""
                sl_trig = tp_trig = 0.0
                _SL_TYPES = {
                    "STOP_MARKET", "STOP_MARKET_ORDER", "STOP",
                    "STOP_LOSS_MARKET", "STOP_LOSS_ORDER",
                }
                _TP_TYPES = {
                    "TAKE_PROFIT_MARKET", "TAKE_PROFIT_MARKET_ORDER",
                    "TAKE_PROFIT", "TAKE_PROFIT_ORDER",
                }

                def _parse_children(open_ords):
                    """Return (sl_oid, sl_trig, tp_oid, tp_trig) from an open-orders list."""
                    _sl_o = _tp_o = ""
                    _sl_t = _tp_t = 0.0
                    for o in (open_ords or []):
                        raw_type = str(
                            o.get("type") or
                            (o.get("raw") or {}).get("order_type") or
                            (o.get("raw") or {}).get("stop_order_type") or ""
                        )
                        ot   = raw_type.upper().replace(" ", "_").replace("-", "_")
                        trig = float(o.get("trigger_price") or
                                     (o.get("raw") or {}).get("stop_price") or 0)
                        oid  = o.get("order_id", "")
                        is_sl = (ot in _SL_TYPES or ("STOP" in ot and "PROFIT" not in ot and "TAKE" not in ot))
                        is_tp = (ot in _TP_TYPES or ("PROFIT" in ot or "TAKE_PROFIT" in ot))
                        if is_sl and not _sl_o and oid:
                            _sl_o = oid; _sl_t = trig
                        elif is_tp and not _tp_o and oid:
                            _tp_o = oid; _tp_t = trig
                    return _sl_o, _sl_t, _tp_o, _tp_t

                # Fast path: 2 attempts x 1.5s = 3s max — covers >95% of cases
                for _fast in range(2):
                    time.sleep(1.5)
                    open_ords = self.get_open_orders()
                    raw_types = [(o.get("order_id","?")[:8], o.get("type","?")) for o in (open_ords or [])]
                    logger.info(f"Bracket child query (fast {_fast+1}/2) — open orders: {raw_types}")
                    sl_oid, sl_trig, tp_oid, tp_trig = _parse_children(open_ords)
                    if sl_oid and tp_oid:
                        logger.info(f"Bracket children found on fast attempt {_fast+1}")
                        break

                # Slow path: background thread resolves within 90s if still missing
                if not (sl_oid and tp_oid):
                    _captured_order_id = order_id
                    _captured_om       = self

                    def _bg_child_resolve():
                        deadline = time.time() + 90.0
                        attempt  = 0
                        while time.time() < deadline:
                            time.sleep(3.0 + min(attempt, 5) * 2.0)
                            attempt += 1
                            try:
                                ords = _captured_om.get_open_orders()
                                _s, _st, _t, _tt = _parse_children(ords)
                                if _s and _t:
                                    logger.info(
                                        f"Bracket child bg-resolve: SL={_s[:8]}\u2026 TP={_t[:8]}\u2026 "
                                        f"(attempt {attempt})")
                                    if not hasattr(_captured_om, "_pending_bracket_children"):
                                        _captured_om._pending_bracket_children = {}
                                    _captured_om._pending_bracket_children[_captured_order_id] = {
                                        "sl_order_id": _s, "sl_price": _st,
                                        "tp_order_id": _t, "tp_price": _tt,
                                    }
                                    return
                            except Exception as _e:
                                logger.debug(f"Bracket child bg-resolve error: {_e}")
                        logger.warning(
                            f"Bracket child bg-resolve: children not found within 90s "
                            f"for order {_captured_order_id[:8]}\u2026 — reconcile will recover SL/TP")

                    import threading as _threading
                    _threading.Thread(target=_bg_child_resolve, daemon=True).start()

                data["bracket_sl_order_id"] = sl_oid
                data["bracket_tp_order_id"] = tp_oid
                data["bracket_sl_price"]    = sl_trig
                data["bracket_tp_price"]    = tp_trig
                if sl_oid:
                    logger.info(f"  Bracket SL order: {sl_oid} @ ${sl_trig:.2f}")
                if tp_oid:
                    logger.info(f"  Bracket TP order: {tp_oid} @ ${tp_trig:.2f}")
                return data

            if status == "CANCELLED":
                logger.info(f"Bracket order {order_id[:8]}… cancelled by exchange")
                break

        # Timeout — cancel and signal caller to retry after cooldown
        self.cancel_order(order_id)
        logger.info(f"Bracket order timeout after {timeout_sec:.0f}s — cancelled")
        return None

    def place_stop_loss(self, side: str, quantity: float,
                        trigger_price: float,
                        use_limit: bool = True) -> Optional[Dict]:
        """
        Place a standalone stop-loss order.

        use_limit=True (default for trailing SLs): stop-limit order.
          order_type=limit_order + stop_order_type=stop_loss_order + stop_price + limit_price
          Advantages: atomic edit-in-place (PUT /v2/orders with stop_price+limit_price),
          maker fee rebate (−0.02% vs +0.05% taker = 7bps saved per trail).
          Limit offset configured via SL_LIMIT_OFFSET_TICKS (default 20 ticks = $2.00).

        use_limit=False: stop-market order (used only for emergency/non-trailing SLs).
          Guaranteed fill but taker fee and no edit-in-place for stop_price.

        Bracket entry SL is always stop-market (placed by place_bracket_limit_entry).
        Trailing SLs start as stop-limit on first cancel+replace of the bracket child.
        """
        try:
            api_side = self._normalize_side(side)
            tick  = float(getattr(config, 'TICK_SIZE', 0.1))
            offset_ticks = int(getattr(config, 'SL_LIMIT_OFFSET_TICKS', 20))
            limit_offset = offset_ticks * tick

            # Limit price: gives execution buffer past the stop trigger.
            # SHORT SL (buy to close): limit = stop + offset (max price we'll pay)
            # LONG  SL (sell to close): limit = stop - offset (min price we'll accept)
            if use_limit:
                if api_side == "BUY":
                    limit_price = round(trigger_price + limit_offset, 1)
                else:
                    limit_price = round(trigger_price - limit_offset, 1)
                logger.info(
                    f"SL-LIMIT {side} qty={quantity} stop=${trigger_price:,.2f} "
                    f"limit=${limit_price:,.2f} (±{limit_offset:.1f}pts offset)")
                data = self._place_with_retry(
                    side=api_side, order_type="limit_order",
                    quantity=quantity, trigger_price=trigger_price,
                    price=limit_price,
                    reduce_only=True, stop_order_type="stop_loss_order")
            else:
                # Stop-market: guaranteed fill, taker fee (for non-trailing / emergency)
                logger.info(f"SL-MARKET {side} qty={quantity} trigger=${trigger_price:,.2f}")
                data = self._place_with_retry(
                    side=api_side, order_type="market_order",
                    quantity=quantity, trigger_price=trigger_price,
                    reduce_only=True, stop_order_type="stop_loss_order")

            if data:
                self._record_order(data["order_id"], {
                    "order_id": data["order_id"], "side": side,
                    "type": "STOP_LOSS_LIMIT" if use_limit else "STOP_LOSS",
                    "quantity": quantity, "trigger_price": trigger_price,
                    "limit_price": limit_price if use_limit else None,
                    "status": data.get("status", "UNKNOWN"),
                    "timestamp": datetime.now().isoformat(),
                })
                logger.info(
                    f"✅ SL{'_LIMIT' if use_limit else ''}: {data['order_id']} "
                    f"@ stop=${trigger_price:,.2f}"
                    + (f" limit=${limit_price:,.2f}" if use_limit else ""))
            return data
        except Exception as e:
            logger.error(f"place_stop_loss error: {e}", exc_info=True)
            return None

    def place_take_profit(self, side: str, quantity: float,
                          trigger_price: float) -> Optional[Dict]:
        try:
            api_side = self._normalize_side(side)
            logger.info(f"TP {side} qty={quantity} trigger=${trigger_price:,.2f}")
            # API doc: standalone TP orders use order_type=market_order +
            # stop_order_type=take_profit_order.
            data = self._place_with_retry(
                side=api_side, order_type="market_order",
                quantity=quantity, trigger_price=trigger_price,
                reduce_only=True, stop_order_type="take_profit_order")
            if data:
                self._record_order(data["order_id"], {
                    "order_id": data["order_id"], "side": side, "type": "TAKE_PROFIT",
                    "quantity": quantity, "trigger_price": trigger_price,
                    "status": data.get("status", "UNKNOWN"),
                    "timestamp": datetime.now().isoformat(),
                })
                logger.info(f"✅ TP: {data['order_id']} @ ${trigger_price:,.2f}")
            return data
        except Exception as e:
            logger.error(f"place_take_profit error: {e}", exc_info=True)
            return None

    # ── Order replacement ─────────────────────────────────────────────────────

    def replace_stop_loss(self, existing_sl_order_id: Optional[str],
                          side: str, quantity: float,
                          new_trigger_price: float) -> Optional[Dict]:
        """
        Update trailing SL to new_trigger_price.

        Strategy:
          1. EDIT-IN-PLACE (Delta) — PUT /v2/orders with id+product_id in body.
             Sends both stop_price AND limit_price together (stop-limit).
             Atomic: no cancel+replace cycle, zero unprotected window.
             404 = order gone (SL fired) → return None, caller records exit.
             bad_schema (with product_id fix) = should not occur — fall through.

          2. CANCEL + REPLACE fallback — for CoinSwitch or irrecoverable edit failures.
             Places a stop-limit order (limit offset = SL_LIMIT_OFFSET_TICKS × TICK_SIZE).
             NOT_FOUND on cancel → abort (old SL still live) to prevent orphan accumulation.
        """
        tick  = float(getattr(config, 'TICK_SIZE', 0.1))
        offset_ticks = int(getattr(config, 'SL_LIMIT_OFFSET_TICKS', 20))
        limit_offset = offset_ticks * tick
        api_side = self._normalize_side(side)

        # Compute limit_price for the stop-limit (same logic as place_stop_loss)
        if api_side == "BUY":
            new_limit_price = round(new_trigger_price + limit_offset, 1)
        else:
            new_limit_price = round(new_trigger_price - limit_offset, 1)

        try:
            # ── Path 1: Edit-in-place (Delta only) ───────────────────────────
            # Sends stop_price + limit_price atomically — no cancel+replace needed.
            # Confirmed from API doc: EditOrderRequest supports both fields.
            if existing_sl_order_id and hasattr(self._adapter, "edit_order"):
                edited = self._adapter.edit_order(
                    order_id=existing_sl_order_id,
                    new_stop_price=new_trigger_price,
                    new_limit_price=new_limit_price,
                )
                if edited and not edited.get("_error"):
                    logger.info(
                        f"✅ SL edited in-place {existing_sl_order_id[:10]}… "
                        f"stop=${new_trigger_price:,.2f} limit=${new_limit_price:,.2f}"
                    )
                    edited["order_id"] = edited.get("order_id", existing_sl_order_id)
                    return edited

                sc  = (edited or {}).get("_sc", 0)
                err = (edited or {}).get("_err_msg", "")
                if sc == 404 or "not_found" in str(err).lower():
                    # SL order gone — it fired. Caller handles exit reconciliation.
                    logger.info(
                        f"SL {existing_sl_order_id[:10]}… gone (404) "
                        f"— SL likely fired, letting reconcile detect exit"
                    )
                    return None
                else:
                    logger.warning(
                        f"SL edit failed sc={sc} err={err} "
                        f"— falling back to cancel+replace"
                    )

            # ── Path 2: Cancel + Replace (CoinSwitch / edit failure fallback) ─
            if existing_sl_order_id:
                existing_status = self.get_order_status_safe(existing_sl_order_id)
                if existing_status in ("FILLED", "PARTIAL_FILL"):
                    logger.info(f"SL {existing_sl_order_id} already {existing_status}")
                    return None
                if existing_status == "PENDING":
                    result = self.cancel_order(existing_sl_order_id)
                    if result in (CancelResult.ALREADY_FILLED, CancelResult.PARTIAL_FILL):
                        return None
                    if result == CancelResult.FAILED:
                        return {"error": "CANCEL_FAILED"}
                    # NOT_FOUND: old SL still live on exchange — do NOT place a new one.
                    # Placing a new SL would accumulate orphaned stop orders (root cause
                    # of the 5-SL bug seen in screenshot). Retry on next trail tick.
                    if result == CancelResult.NOT_FOUND:
                        logger.warning(
                            f"⚠️ SL cancel returned NOT_FOUND for {existing_sl_order_id} — "
                            f"aborting replace (old SL may still be live). Retry next tick.")
                        return {"error": "CANCEL_NOT_FOUND"}
                    self._remove_active_order(existing_sl_order_id)

            # Place stop-limit replacement (use_limit=True is default)
            new_sl = self.place_stop_loss(side=side, quantity=quantity,
                                          trigger_price=new_trigger_price,
                                          use_limit=True)
            if new_sl:
                logger.info(
                    f"✅ SL replaced (stop-limit) → {new_sl['order_id']} "
                    f"stop=${new_trigger_price:,.2f} limit=${new_limit_price:,.2f}")
                return new_sl
            return {"error": "PLACE_FAILED"}
        except Exception as e:
            logger.error(f"replace_stop_loss error: {e}", exc_info=True)
            return {"error": str(e)}

    def replace_take_profit(self, existing_tp_order_id: Optional[str],
                            side: str, quantity: float,
                            new_trigger_price: float) -> Optional[Dict]:
        """Same edit-in-place → cancel+replace strategy as replace_stop_loss."""
        try:
            # ── Path 1: Edit-in-place (Delta only) ───────────────────────────
            if existing_tp_order_id and hasattr(self._adapter, "edit_order"):
                edited = self._adapter.edit_order(
                    order_id=existing_tp_order_id,
                    new_stop_price=new_trigger_price,
                )
                if edited and not edited.get("_error"):
                    logger.info(
                        f"✅ TP edited in-place {existing_tp_order_id[:10]}… "
                        f"→ ${new_trigger_price:,.2f}"
                    )
                    edited["order_id"] = edited.get("order_id", existing_tp_order_id)
                    return edited

                sc  = (edited or {}).get("_sc", 0)
                err = (edited or {}).get("_err_msg", "")
                if sc != 404 and "not_found" not in str(err).lower():
                    logger.warning(
                        f"TP edit failed sc={sc} err={err} "
                        f"— falling back to cancel+replace"
                    )

            # ── Path 2: Cancel + Replace fallback ────────────────────────────
            if existing_tp_order_id:
                existing_status = self.get_order_status_safe(existing_tp_order_id)
                if existing_status in ("FILLED", "PARTIAL_FILL"):
                    return None
                if existing_status == "PENDING":
                    result = self.cancel_order(existing_tp_order_id)
                    if result in (CancelResult.ALREADY_FILLED, CancelResult.PARTIAL_FILL):
                        return None
                    if result == CancelResult.FAILED:
                        return {"error": "CANCEL_FAILED"}
                    self._remove_active_order(existing_tp_order_id)

            new_tp = self.place_take_profit(side=side, quantity=quantity,
                                            trigger_price=new_trigger_price)
            if new_tp:
                logger.info(f"✅ TP replaced → {new_tp['order_id']} "
                            f"@ ${new_trigger_price:,.2f}")
                return new_tp
            return {"error": "PLACE_FAILED"}
        except Exception as e:
            logger.error(f"replace_take_profit error: {e}", exc_info=True)
            return {"error": str(e)}

    # ── Cancel ────────────────────────────────────────────────────────────────

    def cancel_order(self, order_id: str) -> CancelResult:
        try:
            resp = self._adapter.cancel_order(order_id)

            if not isinstance(resp, dict):
                return CancelResult.FAILED

            # CoinSwitch: no "error" key in resp means success
            # Delta: resp["success"] == True
            success = (
                ("error" not in resp) or
                (isinstance(resp, dict) and resp.get("success"))
            )

            if success:
                # Verify state
                current = self.get_order_status_safe(order_id)
                if current == "FILLED":
                    return CancelResult.ALREADY_FILLED
                if current == "PARTIAL_FILL":
                    return CancelResult.PARTIAL_FILL
                self._remove_active_order(order_id)
                return CancelResult.SUCCESS

            # Check if it was already filled before cancel
            current = self.get_order_status_safe(order_id)
            if current == "FILLED":
                return CancelResult.ALREADY_FILLED
            if current == "PARTIAL_FILL":
                return CancelResult.PARTIAL_FILL
            if current == "CANCELLED":
                self._remove_active_order(order_id)
                return CancelResult.SUCCESS

            sc = resp.get("status_code", 0)
            if sc == 404:
                self._remove_active_order(order_id)
                return CancelResult.NOT_FOUND

            return CancelResult.FAILED

        except Exception as e:
            logger.error(f"cancel_order error for {order_id}: {e}", exc_info=True)
            return CancelResult.FAILED

    def cancel_all_exit_orders(self, sl_order_id: Optional[str],
                               tp_order_id: Optional[str]
                               ) -> Tuple[CancelResult, CancelResult]:
        tp_result = CancelResult.NOT_FOUND
        sl_result = CancelResult.NOT_FOUND
        if tp_order_id:
            tp_result = self.cancel_order(tp_order_id)
            logger.info(f"TP cancel: {tp_result.value}")
        if sl_order_id:
            sl_result = self.cancel_order(sl_order_id)
            logger.info(f"SL cancel: {sl_result.value}")
        return sl_result, tp_result

    # ── Open orders + conditional sweep ──────────────────────────────────────

    def get_open_orders(self, symbol: str = None) -> Optional[list]:
        if self._open_orders_404:
            return None
        try:
            sym  = symbol or getattr(config, "SYMBOL", "BTCUSDT")
            raw  = self._adapter.get_open_orders(sym)
            if raw is None:
                return None
            # Detect 404 dict response (adapter returned error dict instead of raising)
            if isinstance(raw, dict) and (raw.get("status_code") == 404
                                          or "404" in str(raw.get("error", ""))):
                logger.warning("get_open_orders: 404 — endpoint unsupported, suppressing future calls")
                self._open_orders_404 = True
                return None
            # Delta bracket child stop_order_type remap (defense-in-depth in case
            # api.py normalisation was bypassed or a different adapter is used).
            _STOP_OTYPE_REMAP = {
                "STOP_LOSS_ORDER":   "STOP_MARKET",
                "TAKE_PROFIT_ORDER": "TAKE_PROFIT_MARKET",
            }
            result = []
            for o in raw:
                if not isinstance(o, dict): continue
                oid   = str(o.get("order_id") or o.get("id") or "")
                otype = str(o.get("order_type") or o.get("type") or "").upper()
                # If otype resolved to plain MARKET, check if the underlying raw
                # dict has stop_order_type that reclassifies it as SL/TP.
                if otype == "MARKET":
                    _raw_inner = o.get("_raw") or o.get("raw") or {}
                    _sot = str(_raw_inner.get("stop_order_type", "")).upper()
                    otype = _STOP_OTYPE_REMAP.get(_sot, otype)
                side  = str(o.get("side") or "").upper()
                try: qty  = float(o.get("quantity") or o.get("size") or 0)
                except (ValueError, TypeError): qty = 0.0
                try: trig = float(o.get("trigger_price") or o.get("stop_price") or 0)
                except (ValueError, TypeError): trig = 0.0
                try: px   = float(o.get("price") or o.get("limit_price") or 0)
                except (ValueError, TypeError): px = 0.0
                status = str(o.get("status") or o.get("state") or "").upper()
                if oid:
                    result.append({"order_id": oid, "type": otype, "side": side,
                                   "quantity": qty, "trigger_price": trig,
                                   "price": px, "status": status, "raw": o})
            return result
        except Exception as e:
            err_str = str(e)
            if "404" in err_str or "Not Found" in err_str:
                logger.warning("get_open_orders: 404 Not Found — endpoint unsupported, suppressing future calls")
                self._open_orders_404 = True
            else:
                logger.error(f"get_open_orders error: {e}", exc_info=True)
            return None

    def cancel_symbol_conditionals(self, symbol: str = None) -> Dict[str, CancelResult]:
        sym    = symbol or getattr(config, "SYMBOL", "BTCUSDT")
        orders = self.get_open_orders(symbol=sym)
        if orders is None:
            return {}
        CONDITIONAL_TYPES = {"STOP_MARKET", "STOP", "TAKE_PROFIT_MARKET",
                              "TAKE_PROFIT", "STOP_LOSS_MARKET",
                              "STOP_MARKET_ORDER", "STOP_LOSS_ORDER",
                              "TAKE_PROFIT_MARKET_ORDER", "TAKE_PROFIT_ORDER"}
        targets = [o for o in orders if o["type"] in CONDITIONAL_TYPES]
        if not targets:
            return {}
        results: Dict[str, CancelResult] = {}
        for o in targets:
            oid = o["order_id"]
            res = self.cancel_order(oid)
            results[oid] = res
            level = logger.info if res in (CancelResult.SUCCESS, CancelResult.NOT_FOUND) \
                    else logger.warning
            level(f"Swept {o['type']} {oid[:8]}… ({res.value})")
        return results

    # ── Balance ───────────────────────────────────────────────────────────────

    def get_balance(self) -> Dict:
        return self._adapter.get_balance()

    # ── Leverage ──────────────────────────────────────────────────────────────

    def set_leverage(self, leverage: int, product_id: Optional[int] = None) -> Dict:
        return self._adapter.set_leverage(leverage=leverage, product_id=product_id)

    # ── Misc ──────────────────────────────────────────────────────────────────

    def get_active_orders(self) -> Dict[str, Dict]:
        with self._orders_lock: return dict(self.active_orders)

    def get_order_count(self) -> int:
        with self._orders_lock: return len(self.active_orders)

    def get_recent_order_history(self, limit: int = 20) -> list:
        with self._orders_lock: return list(self.order_history[-limit:])

    @staticmethod
    def compute_signal_urgency(price_now: float, price_prev: float,
                               atr: float, side: str,
                               vwap_dev_atr: float,
                               entry_threshold_atr: float) -> float:
        """Signal urgency for MakerTakerDecision. Mean-reversion direction-aware."""
        if atr < 1e-10 or price_now < 1.0 or price_prev < 1.0:
            return 0.5
        delta    = abs(price_now - price_prev)
        momentum = min(1.0, delta / (atr * 0.5))
        if side == "long":
            direction_factor = 1.0 if price_now > price_prev else -0.3
        else:
            direction_factor = 1.0 if price_now < price_prev else -0.3
        momentum_urgency = max(0.0, min(1.0, momentum * direction_factor))
        dev_abs   = abs(vwap_dev_atr)
        threshold = entry_threshold_atr
        if dev_abs <= threshold:            ext_urgency = 0.8
        elif dev_abs <= threshold * 1.5:   ext_urgency = 0.3
        else:                               ext_urgency = 0.15
        urgency = 0.65 * momentum_urgency + 0.35 * ext_urgency
        return round(min(1.0, max(0.0, urgency)), 3)

    # Guaranteed delivery wrapper (unchanged from v3)
    def place_order_guaranteed(self, order_fn_name: str,
                               max_wait_seconds: float = 600.0,
                               retry_interval_base: float = 15.0,
                               **kwargs) -> Optional[Dict]:
        fn = getattr(self, order_fn_name, None)
        if fn is None:
            logger.error(f"place_order_guaranteed: unknown method '{order_fn_name}'")
            return None
        deadline   = time.time() + max_wait_seconds
        attempt    = 0
        sleep_time = retry_interval_base
        while time.time() < deadline:
            attempt += 1
            result = fn(**kwargs)
            if result and "error" not in result:
                logger.info(f"✅ place_order_guaranteed: {order_fn_name} on attempt {attempt}")
                return result
            remaining = deadline - time.time()
            if remaining <= 0: break
            actual_sleep = min(sleep_time, remaining, 60.0)
            logger.warning(f"⏳ {order_fn_name} attempt {attempt} failed — retry in {actual_sleep:.0f}s")
            time.sleep(actual_sleep)
            sleep_time = min(sleep_time * 1.5, 60.0)
        logger.error(f"❌ place_order_guaranteed: {order_fn_name} FAILED after {attempt} attempts")
        return None

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
import math
from collections import deque
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, Optional, Tuple

import sys, os; sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from core.instruments import ExchangeName

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
_ICICI_LIMITER = _RateLimiter(min_interval_sec=0.75)

# Also keep a module-level alias for compatibility imports (quant_strategy does
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

    def __init__(self, api, exchange_instrument=None) -> None:
        self.api     = api
        self.limiter = _CS_LIMITER
        self.exchange_instrument = exchange_instrument
        self.symbol  = (exchange_instrument.symbol if exchange_instrument is not None else config.COINSWITCH_SYMBOL)
        self.display_symbol = (exchange_instrument.display_symbol if exchange_instrument is not None else self.symbol)
        self.tick_size = float(getattr(exchange_instrument, "tick_size", 0.0) or 0.0) if exchange_instrument is not None else 0.0
        self.lot_step = float(getattr(exchange_instrument, "lot_step", 0.0) or 0.0) if exchange_instrument is not None else 0.0
        self.min_qty = float(getattr(exchange_instrument, "min_qty", 0.0) or 0.0) if exchange_instrument is not None else 0.0
        self.max_qty = float(getattr(exchange_instrument, "max_qty", 0.0) or 0.0) if exchange_instrument is not None else 0.0
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

    @staticmethod
    def _extract_paid_commission(payload: Optional[Dict]) -> tuple[float, bool]:
        if not isinstance(payload, dict):
            return 0.0, False
        for key in ("paid_commission", "commission", "fee", "fees"):
            if key in payload and payload.get(key) not in (None, ""):
                try:
                    return float(payload.get(key)), True
                except (TypeError, ValueError):
                    return 0.0, True
        return 0.0, False

    def resolve_order_execution(self, order_id: str) -> Optional[Dict]:
        raw = self.get_order(str(order_id or "").strip())
        if not isinstance(raw, dict):
            return None
        fee_paid, fee_exact = self._extract_paid_commission(raw)
        return {
            "status": self.extract_status(raw),
            "fill_price": float(self.extract_fill_price(raw) or 0.0),
            "filled_qty": float(self.extract_filled_qty(raw) or 0.0),
            "paid_commission": float(fee_paid or 0.0),
            "paid_commission_exact": bool(fee_exact),
            "raw_order": raw,
        }

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
            if self.symbol.upper() not in sym.replace("/", ""):
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

    def __init__(self, api, exchange_instrument=None) -> None:
        self.api      = api
        self.limiter  = _DELTA_LIMITER
        self.exchange_instrument = exchange_instrument
        self.symbol   = (exchange_instrument.symbol if exchange_instrument is not None else getattr(config, 'DELTA_SYMBOL', 'BTCUSD'))
        self.display_symbol = (exchange_instrument.display_symbol if exchange_instrument is not None else self.symbol)
        self.tick_size = float(getattr(exchange_instrument, "tick_size", 0.0) or 0.0) if exchange_instrument is not None else 0.0
        self.lot_step = float(getattr(exchange_instrument, "lot_step", 0.0) or 0.0) if exchange_instrument is not None else 0.0
        self.min_qty = float(getattr(exchange_instrument, "min_qty", 0.0) or 0.0) if exchange_instrument is not None else 0.0
        self.max_qty = float(getattr(exchange_instrument, "max_qty", 0.0) or 0.0) if exchange_instrument is not None else 0.0
        self.contract_value = float(getattr(exchange_instrument, "contract_value_btc", 0.0) or 0.0) if exchange_instrument is not None else 0.0
        # BTCUSD uses config.DELTA_CONTRACT_VALUE_BTC (0.001 BTC/contract).
        # xStock/RWA contracts must not inherit the BTC conversion. If Delta's
        # product row omits contract_value, fall back to integer contracts.
        asset_class = str(getattr(exchange_instrument, "asset_class", "") or "").lower() if exchange_instrument is not None else ""
        if self.contract_value <= 0:
            self.contract_value = float(getattr(config, 'DELTA_CONTRACT_VALUE_BTC', 0.001) or 0.001)
            if asset_class in ("assetclass.equity", "equity", "assetclass.commodity", "commodity", "assetclass.index", "index"):
                self.contract_value = 1.0
        self._pid_cache: Optional[int] = getattr(exchange_instrument, "product_id", None) if exchange_instrument is not None else None

    def _qty_to_contracts(self, quantity: float) -> int:
        cv = float(self.contract_value or 0.0)
        if cv <= 0:
            cv = float(getattr(config, 'DELTA_CONTRACT_VALUE_BTC', 0.001) or 0.001)
        return max(1, round(float(quantity) / cv)) if cv > 0 else max(1, int(round(float(quantity))))

    def _contracts_to_qty(self, contracts: float) -> float:
        cv = float(self.contract_value or 0.0)
        if cv <= 0:
            cv = float(getattr(config, 'DELTA_CONTRACT_VALUE_BTC', 0.001) or 0.001)
        return abs(float(contracts)) * cv

    def _active_tick_size(self) -> float:
        try:
            tick = float(getattr(self, "tick_size", 0.0) or 0.0)
            if tick > 0:
                return tick
        except Exception:
            pass
        getter = getattr(config, "get_tick_size", None)
        if callable(getter):
            try:
                tick = float(getter() or 0.0)
                if tick > 0:
                    return tick
            except Exception:
                pass
        return float(getattr(config, "TICK_SIZE", 0.1) or 0.1)

    def _round_nearest_tick(self, price: float) -> float:
        tick = max(self._active_tick_size(), 1e-12)
        return round(round(float(price) / tick) * tick, 10)

    def _round_floor_tick(self, price: float) -> float:
        import math
        tick = max(self._active_tick_size(), 1e-12)
        # Add a tiny epsilon before flooring to avoid 68.96 / 0.01 becoming
        # 6895.999999999 and incorrectly flooring to 68.95.
        return round(math.floor((float(price) + tick * 1e-9) / tick) * tick, 10)

    def _round_ceil_tick(self, price: float) -> float:
        import math
        tick = max(self._active_tick_size(), 1e-12)
        # Subtract a tiny epsilon before ceiling for the same floating-point
        # boundary case. Directional safety is preserved because the epsilon is
        # far below one tick.
        return round(math.ceil((float(price) - tick * 1e-9) / tick) * tick, 10)

    def _normalise_bracket_prices(self, side: str, limit_price: float,
                                  sl_price: float, tp_price: float) -> Dict[str, float]:
        """Return Delta-safe native bracket prices.

        Directional rounding prevents the payload from becoming less protective
        after tick normalisation.  The bracket child limit prices are sent
        explicitly because Delta's CreateOrderRequest schema supports them and
        some non-BTC contracts reject trigger-only native brackets.
        """
        s = str(side or "").lower().strip()
        tick = max(self._active_tick_size(), 1e-12)
        sl_offset_ticks = max(1, int(getattr(config, "SL_LIMIT_OFFSET_TICKS", 20) or 20))
        sl_offset = tick * sl_offset_ticks

        if s in ("buy", "long"):
            entry = self._round_floor_tick(limit_price)
            sl_trig = self._round_floor_tick(sl_price)
            tp_trig = self._round_ceil_tick(tp_price)
            sl_limit = self._round_floor_tick(sl_trig - sl_offset)
            tp_limit = tp_trig
            valid = sl_trig < entry < tp_trig and sl_limit <= sl_trig
        else:
            entry = self._round_ceil_tick(limit_price)
            sl_trig = self._round_ceil_tick(sl_price)
            tp_trig = self._round_floor_tick(tp_price)
            sl_limit = self._round_ceil_tick(sl_trig + sl_offset)
            tp_limit = tp_trig
            valid = tp_trig < entry < sl_trig and sl_limit >= sl_trig

        if not valid:
            raise ValueError(
                f"invalid native bracket geometry side={side} entry={entry} "
                f"sl={sl_trig} sl_limit={sl_limit} tp={tp_trig}"
            )
        return {
            "limit_price": entry,
            "bracket_stop_loss_price": sl_trig,
            "bracket_stop_loss_limit_price": sl_limit,
            "bracket_take_profit_price": tp_trig,
            "bracket_take_profit_limit_price": tp_limit,
        }

    @staticmethod
    def _num(value, default: float = 0.0) -> float:
        try:
            if value is None or value == "":
                return float(default)
            return float(value)
        except (TypeError, ValueError):
            return float(default)

    @classmethod
    def _extract_paid_commission(cls, payload: Optional[Dict]) -> tuple[float, bool]:
        """Return (exact_commission_usd, present).

        Delta exposes the exact fee on orders as paid_commission / commission.
        The value may legitimately be zero for a zero-fee promotion or negative
        for a rebate, so presence of the field is the exactness signal — not
        value > 0.
        """
        if not isinstance(payload, dict):
            return 0.0, False
        sources = [payload]
        raw = payload.get("_raw")
        if isinstance(raw, dict):
            sources.insert(0, raw)
        for src in sources:
            for key in ("paid_commission", "commission", "fee", "fees"):
                if key in src and src.get(key) is not None and src.get(key) != "":
                    return cls._num(src.get(key), 0.0), True
        return 0.0, False

    def resolve_order_execution(self, order_id: str, page_size: int = 20) -> Optional[Dict]:
        """Resolve exact execution details for a Delta order.

        Uses GET /v2/orders/{id} as the authority for state and paid_commission,
        then GET /v2/fills as an exact fill-level supplement. No fee estimates
        are produced here.
        """
        oid = str(order_id or "").strip()
        if not oid:
            return None
        raw = self.query_order_exact(oid)
        if not isinstance(raw, dict):
            return None

        fee_paid, fee_exact = self._extract_paid_commission(raw)
        fill_price = self.extract_fill_price(raw) or 0.0
        filled_qty = self.extract_filled_qty(raw)

        try:
            fills = self.get_fills_recent(page_size=page_size) or []
            num = den = 0.0
            fill_contracts = 0.0
            fill_commission = 0.0
            fill_commission_seen = False
            for fill in fills:
                if str(fill.get("order_id", "") or "").strip() != oid:
                    continue
                fp = self._num(fill.get("price"), 0.0)
                fsz = abs(self._num(fill.get("size"), 0.0))
                if fp > 0 and fsz > 0:
                    num += fp * fsz
                    den += fsz
                    fill_contracts += fsz
                ffee, fexact = self._extract_paid_commission(fill)
                if fexact:
                    fill_commission += ffee
                    fill_commission_seen = True
            if den > 0:
                fill_price = num / den
            if fill_contracts > 0:
                filled_qty = self._contracts_to_qty(fill_contracts)
            if fill_commission_seen:
                fee_paid = fill_commission
                fee_exact = True
        except Exception as e:
            logger.debug(f"resolve_order_execution({oid}) fills lookup error: {e}")

        return {
            "status": self.extract_status(raw),
            "fill_price": float(fill_price or 0.0),
            "filled_qty": float(filled_qty or 0.0),
            "paid_commission": float(fee_paid or 0.0),
            "paid_commission_exact": bool(fee_exact),
            "raw_order": raw,
        }

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
        """Return filled quantity in strategy units, never raw Delta contracts."""
        for f in ("filled_size", "executed_qty", "size"):
            v = order_data.get(f)
            if v:
                try:
                    contracts = abs(float(v))
                    if contracts > 0:
                        return self._contracts_to_qty(contracts)
                except (ValueError, TypeError):
                    pass
        return 0.0

    def place_order(self, side: str, order_type: str, quantity: float,
                    price: Optional[float] = None,
                    trigger_price: Optional[float] = None,
                    reduce_only: bool = False,
                    stop_order_type: Optional[str] = None) -> Optional[Dict]:
        self.limiter.wait()
        # symbol is the primary key; Delta API resolves product_id internally
        # Convert strategy quantity → integer Delta contracts. Per-instrument
        # contract_value is mandatory here; BTC's 0.001 convention must not leak
        # into xStock/RWA products.
        contracts = self._qty_to_contracts(quantity)
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
        result["quantity"] = float(quantity)
        result["size_btc"] = float(quantity)
        result["size_contracts"] = int(contracts)
        return result

    def place_bracket_limit_entry(self, side: str, quantity: float,
                                  limit_price: float,
                                  sl_price: float,
                                  tp_price: float) -> Optional[Dict]:
        # Bracket limit order: entry + SL + TP in a single Delta API call.
        # Avoids naked-entry fallback and sends a complete Delta native-bracket
        # payload (trigger + child limit prices + trigger method), which is
        # materially safer for non-BTC products such as SLVON/PAXG/xStocks.
        self.limiter.wait()
        try:
            contracts = self._qty_to_contracts(quantity)
            if contracts <= 0:
                raise ValueError(f"invalid contracts={contracts} from quantity={quantity}")
            prices = self._normalise_bracket_prices(side, limit_price, sl_price, tp_price)
        except Exception as e:
            return {
                "_raw": {"error": {"code": "local_bracket_preflight_failed", "message": str(e)}},
                "_sc": 0,
                "_error": True,
            }

        resp = self.api.place_order(
            symbol                           = self.symbol,
            side                             = side.lower(),
            order_type                       = "limit",
            size                             = contracts,
            limit_price                      = prices["limit_price"],
            bracket_stop_loss_price          = prices["bracket_stop_loss_price"],
            bracket_stop_loss_limit_price    = prices["bracket_stop_loss_limit_price"],
            bracket_take_profit_price        = prices["bracket_take_profit_price"],
            bracket_take_profit_limit_price  = prices["bracket_take_profit_limit_price"],
            bracket_stop_trigger_method      = str(getattr(config, "DELTA_BRACKET_STOP_TRIGGER_METHOD", "last_traded_price") or "last_traded_price"),
            post_only                        = False,
            time_in_force                    = "gtc",
        )
        oid = self.extract_order_id(resp)
        if not oid:
            sc = resp.get("status_code", 0) if isinstance(resp, dict) else 0
            return {"_raw": resp, "_sc": sc, "_error": True}
        result = resp.get("result", {}) if isinstance(resp, dict) else {}
        result["order_id"] = oid
        result["submitted_contracts"] = int(contracts)
        result["submitted_prices"] = prices
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
            product_id  = self._get_product_id(),
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

    def query_order_exact(self, order_id: str) -> Optional[Dict]:
        """
        GET /v2/orders/{order_id} — fetch a single order by its ID.

        Per Delta API docs, the response always contains:
          state           — "open" | "pending" | "closed" | "cancelled"
          paid_commission — string; the actual USD commission charged on this order
          stop_order_type — "stop_loss_order" | "take_profit_order" (for conditional orders)
          commission      — same as paid_commission (alias field present in docs)

        "closed" state means the order filled completely.
        "paid_commission" is the exact fee charged — not an estimate.

        Costs one _DELTA_LIMITER slot (0.25 s).
        Returns the full raw Delta order object dict, or None on any error.
        """
        self.limiter.wait()
        try:
            resp = self.api.get_order(order_id=order_id)
            if not isinstance(resp, dict) or not resp.get("success"):
                return None
            result = resp.get("result") or {}
            # api.py wraps the response; _raw is the original Delta dict
            raw = result.get("_raw") or result
            return raw if isinstance(raw, dict) else None
        except Exception as e:
            logger.debug(f"_DeltaAdapter.query_order_exact({order_id}) error: {e}")
            return None

    def get_fills_recent(self, page_size: int = 5) -> Optional[list]:
        """
        GET /v2/fills — most recent fill records for this symbol.

        Used solely to retrieve the exact per-contract execution price for a
        known order_id, because GET /v2/orders/{id} does not guarantee
        returning average_fill_price for all order types per the official schema.

        Fill object fields used here (per Delta API docs):
          order_id   — string; matches the integer order id we placed
          price      — string; exact per-contract execution price
          commission — string; fee for this individual fill event
          size       — integer; contracts filled in this event

        For multi-fill orders (rare for market SL/TP), VWAP across fills for
        the same order_id is computed by the caller.

        page_size hard-capped at 5 — exit fills are always the most recent
        events; fetching more wastes one rate-limit slot for no benefit.
        Returns list of raw fill dicts, or None on any error.
        """
        self.limiter.wait()
        try:
            resp = self.api.get_fills(symbol=self.symbol, page_size=min(page_size, 5))
            if not isinstance(resp, dict) or not resp.get("success"):
                return None
            raw = resp.get("result", []) or []
            # Some Delta paginated responses: {"result": [...], "meta": {...}}
            if isinstance(raw, dict):
                raw = raw.get("result", []) or []
            return raw if isinstance(raw, list) else []
        except Exception as e:
            logger.debug(f"_DeltaAdapter.get_fills_recent error: {e}")
            return None

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
        """
        Turn Delta position response into a canonical dict.

        CRITICAL UNIT-CONSISTENCY CONTRACT:
          The strategy's PositionState stores quantity in BTC units
          (`pos.quantity = 0.001` means 0.001 BTC). On entry, the strategy
          writes `pos.quantity = qty_btc` directly, and this adapter's
          place_order converts that to `size = round(qty_btc / _cv)` integer
          contracts before sending to Delta.

          RECONCILE PATH BUG (pre-fix):
          Delta's position endpoint returns `size` in **contracts**, not BTC.
          The previous code read `size = abs(float(pos["size"]))` and
          returned it verbatim. When the reconcile path adopted an exchange
          position, it wrote `pos.quantity = size_in_contracts` — a 1000×
          unit mismatch for DELTA_CONTRACT_VALUE_BTC=0.001. A 0.001 BTC
          position (= 1 contract) was then tracked internally as
          `quantity = 1.0 BTC`, causing:
            - trailing SL to attempt placing 1.0-BTC-equivalent SL orders
              (1000× the real size; exchange rejects)
            - PnL estimates off by 1000× (the log showed gross=$209.50 on
              what was really a $0.21 exposure — see third-trade incident)
            - position size logs showing 1.0 when actual was 0.001

          FIX:
          Convert contracts → BTC at the adapter boundary. All callers
          downstream of normalise_position see size in BTC units, matching
          the entry-side invariant.

          Invariant established by this fix:
            Every `pos.quantity` in the strategy is in BTC units.
            Every `size` argument to adapter.place_order is in BTC units.
            The adapter is the sole place that translates to/from contracts.
        """
        if isinstance(raw, dict):
            inner = raw.get("result", raw)
            positions = inner if isinstance(inner, list) else [inner]
        elif isinstance(raw, list):
            positions = raw
        else:
            positions = []

        delta_sym = self.symbol.upper()

        for pos in positions:
            if not isinstance(pos, dict): continue
            sym = str(pos.get("product_symbol",
                     pos.get("symbol", ""))).upper()
            if delta_sym not in sym and sym not in delta_sym: continue

            # Read raw contract size from Delta's response.
            size_contracts_raw = 0.0
            signed_contracts   = 0.0
            for f in ("size", "quantity", "net_size"):
                v = pos.get(f)
                if v is not None:
                    try:
                        signed_contracts = float(v)
                        size_contracts_raw = abs(signed_contracts)
                        if size_contracts_raw > 0: break
                    except (ValueError, TypeError): pass

            # CONVERT: contracts → strategy-layer quantity using this product's
            # contract value. For BTC this is BTC units; for xStocks/RWA it is
            # integer/contract units unless Delta provides a different value.
            size_btc        = self._contracts_to_qty(size_contracts_raw)
            signed_size_btc = signed_contracts * float(self.contract_value or 1.0)

            side = None
            if size_btc > 0:
                rs = str(pos.get("direction",
                         pos.get("side", ""))).upper()
                side = "LONG" if rs in ("LONG", "BUY") else \
                       "SHORT" if rs in ("SHORT", "SELL") else None
                # Secondary resolution from sign of contracts if string side absent
                if side is None:
                    if   signed_contracts > 0: side = "LONG"
                    elif signed_contracts < 0: side = "SHORT"

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

            return {
                "side":           side,
                "size":           size_btc,          # BTC units (converted)
                "size_signed":    signed_size_btc,   # signed BTC (for side-disamb.)
                "size_contracts": size_contracts_raw,# raw contracts (for diagnostics)
                "entry_price":    entry,
                "unrealized_pnl": upnl,
                "raw":            pos,
            }
        return {"side": None, "size": 0.0, "size_signed": 0.0,
                "size_contracts": 0.0, "entry_price": 0.0,
                "unrealized_pnl": 0.0}


# ── Main OrderManager ─────────────────────────────────────────────────────────

class _ICICIAdapter:
    """Long-premium ICICI options adapter.

    Opening orders are always buy-to-open limit orders. Exits are sell-to-close
    limit or official Breeze stoploss orders. No market orders, no option writing,
    no leverage. Portfolio state is created only from exact NFO option rows.
    """

    def __init__(self, api, exchange_instrument=None) -> None:
        self.api = api
        self.limiter = _ICICI_LIMITER
        self.exchange_instrument = exchange_instrument
        self.symbol = (exchange_instrument.symbol if exchange_instrument is not None else "")
        self.display_symbol = (exchange_instrument.display_symbol if exchange_instrument is not None else self.symbol)
        self.tick_size = float(getattr(exchange_instrument, "tick_size", 0.05) or 0.05) if exchange_instrument is not None else 0.05
        self.lot_step = float(getattr(exchange_instrument, "lot_step", 1.0) or 1.0) if exchange_instrument is not None else 1.0
        self.min_qty = float(getattr(exchange_instrument, "min_qty", 1.0) or 1.0) if exchange_instrument is not None else 1.0
        self.max_qty = float(getattr(exchange_instrument, "max_qty", 0.0) or 0.0) if exchange_instrument is not None else 0.0
        self.raw = getattr(exchange_instrument, "raw", {}) or {}
        self._last_position_filter_signature = ""
        self._last_position_filter_log_ts = 0.0
        self._gtt_plans: Dict[str, Dict[str, Any]] = {}

    @staticmethod
    def _num(value: Any, default: float = 0.0) -> float:
        try:
            if value is None:
                return default
            if isinstance(value, str):
                value = value.strip().replace(",", "")
                if not value:
                    return default
            return float(value)
        except Exception:
            return default

    @staticmethod
    def _success_payload(resp: Any) -> Dict[str, Any]:
        data = resp.get("Success") if isinstance(resp, dict) else resp
        if isinstance(data, list):
            data = data[0] if data else {}
        return data if isinstance(data, dict) else {}

    def _active_raw(self) -> Dict[str, Any]:
        selected = self.raw.get("selected_option_contract")
        if isinstance(selected, dict) and isinstance(selected.get("raw"), dict):
            merged = dict(self.raw)
            merged.update(selected.get("raw") or {})
            return merged
        return self.raw

    @staticmethod
    def _right_value(value: Any) -> str:
        right = str(value or "").strip().lower()
        if right in {"c", "ce", "call"}:
            return "call"
        if right in {"p", "pe", "put"}:
            return "put"
        return right

    def _has_contract_identity(self, raw: Dict[str, Any]) -> bool:
        if not isinstance(raw, dict):
            return False
        strike = self._num(raw.get("strike_price") or raw.get("StrikePrice") or raw.get("strike"), 0.0)
        expiry = str(raw.get("expiry_date") or raw.get("ExpiryDate") or raw.get("expiry") or "").strip()
        right = self._right_value(raw.get("right") or raw.get("option_type") or raw.get("OptionType"))
        return bool(strike > 0 and expiry and right in {"call", "put"})

    def _target_stock_code(self) -> str:
        active = self._active_raw()
        return str(active.get("stock_code") or active.get("StockCode") or self.symbol or "").strip().upper()

    def _contract_key(self, raw: Dict[str, Any]) -> tuple:
        stock = str(raw.get("stock_code") or raw.get("StockCode") or "").strip().upper()
        strike = round(self._num(raw.get("strike_price") or raw.get("StrikePrice") or raw.get("strike"), 0.0), 8)
        expiry_raw = raw.get("expiry_date") or raw.get("ExpiryDate") or raw.get("expiry") or ""
        try:
            expiry = self.api._normalise_expiry(expiry_raw)
        except Exception:
            expiry = str(expiry_raw).strip().lower()[:10]
        right = self._right_value(raw.get("right") or raw.get("option_type") or raw.get("OptionType"))
        return (stock, strike, str(expiry).strip().lower(), right)

    def _signed_position_qty(self, row: Dict[str, Any]) -> float:
        # Breeze PortfolioPositions officially returns `quantity`; aliases are
        # retained only for backward-compatible broker response variants.
        for key in ("quantity", "qty", "open_quantity", "open_qty", "net_quantity", "net_qty"):
            if key in row and row.get(key) not in (None, ""):
                return self._num(row.get(key), 0.0)
        return 0.0

    def _position_qty(self, row: Dict[str, Any]) -> float:
        return abs(self._signed_position_qty(row))

    def _is_exact_nfo_option_position(self, row: Dict[str, Any]) -> tuple[bool, str]:
        if not isinstance(row, dict):
            return False, "non_mapping_row"
        segment = str(row.get("segment") or row.get("Segment") or "").strip().lower()
        product = str(row.get("product_type") or row.get("product") or row.get("ProductType") or "").strip().lower()
        exchange = str(row.get("exchange_code") or row.get("ExchangeCode") or "").strip().upper()
        # Breeze response variants may omit `segment`; never invent a position,
        # but do not ignore an exact NFO Options contract solely because this
        # optional discriminator is absent. Explicit non-F&O values still fail.
        if segment and segment not in {"fno", "nfo"}:
            return False, "explicit_non_fno_segment"
        if exchange != "NFO":
            return False, "not_nfo_exchange"
        if product not in {"option", "options"}:
            return False, "not_options_product"
        if not self._has_contract_identity(row):
            return False, "missing_exact_contract_identity"
        target = self._target_stock_code()
        stock = str(row.get("stock_code") or row.get("StockCode") or "").strip().upper()
        if target and stock and stock != target:
            return False, "different_underlying"
        if target and not stock:
            return False, "missing_underlying"
        return True, ""

    def _is_exact_nfo_option_order(self, row: Dict[str, Any]) -> tuple[bool, str]:
        """Order-list validation; unlike PortfolioPositions, Order responses do not publish `segment`."""
        if not isinstance(row, dict):
            return False, "non_mapping_row"
        product = str(row.get("product_type") or row.get("product") or row.get("ProductType") or "").strip().lower()
        exchange = str(row.get("exchange_code") or row.get("ExchangeCode") or "").strip().upper()
        if exchange != "NFO":
            return False, "not_nfo_exchange"
        if product not in {"option", "options"}:
            return False, "not_options_product"
        if not self._has_contract_identity(row):
            return False, "missing_exact_contract_identity"
        target = self._target_stock_code()
        stock = str(row.get("stock_code") or row.get("StockCode") or "").strip().upper()
        if target and stock != target:
            return False, "different_underlying"
        return True, ""

    def _valid_open_option_rows(self, positions: list[Dict[str, Any]]) -> tuple[list[Dict[str, Any]], list[str]]:
        valid, ignored = [], []
        for row in positions:
            if not isinstance(row, dict):
                continue
            signed_qty = self._signed_position_qty(row)
            if abs(signed_qty) <= 0:
                continue
            ok, reason = self._is_exact_nfo_option_position(row)
            if ok:
                valid.append(row)
            else:
                ignored.append(reason)
        return valid, ignored

    def _log_filtered_broker_rows(self, ignored: list[str]) -> None:
        if not ignored:
            return
        signature = ",".join(sorted(ignored))
        now = time.time()
        if signature == self._last_position_filter_signature and now - self._last_position_filter_log_ts < 300.0:
            return
        self._last_position_filter_signature = signature
        self._last_position_filter_log_ts = now
        logger.info(
            "ICICI F&O position filter ignored %d non-executable broker row(s) [%s]; "
            "only exact NFO Options rows can create position state",
            len(ignored), ",".join(sorted(set(ignored)))
        )

    def _normalised_position_row(self, row: Dict[str, Any], *, source: str = "matched") -> Dict[str, Any]:
        signed_qty = self._signed_position_qty(row)
        qty = abs(signed_qty)
        entry = self._num(row.get("average_price") or row.get("avg_price") or row.get("entry_price"), 0.0)
        upnl = self._num(row.get("unrealized_pnl") or row.get("unrealised_pnl") or row.get("pnl"), 0.0)
        right = self._right_value(row.get("right") or row.get("option_type") or row.get("OptionType"))
        strike = str(row.get("strike_price") or row.get("StrikePrice") or row.get("strike") or "").strip()
        expiry = str(row.get("expiry_date") or row.get("ExpiryDate") or row.get("expiry") or "").strip()
        symbol = str(row.get("TradingSymbol") or row.get("trading_symbol") or row.get("symbol") or "").strip()
        out = {
            "side": "LONG" if signed_qty > 0 else ("SHORT" if signed_qty < 0 else None),
            "size": qty,
            "size_signed": signed_qty,
            "entry_price": entry,
            "unrealized_pnl": upnl,
            "currency": "INR",
            "segment": "FNO",
            "product_type": "Options",
            "raw": row,
            "contract_identity_source": source,
            "stock_code": row.get("stock_code") or row.get("StockCode"),
            "exchange_code": row.get("exchange_code") or row.get("ExchangeCode"),
            "right": right,
            "strike_price": strike,
            "expiry_date": expiry,
            "TradingSymbol": symbol,
        }
        if source != "selected_contract_match":
            out["requires_contract_reconstruction"] = True
        if signed_qty < 0:
            out["unadoptable"] = True
            out["reason"] = "short_icici_option_outside_long_premium_policy"
        return out

    def _lot_size(self) -> float:
        raw = self._active_raw()
        for key in ("runtime_lot_size", "LotSize", "lot_size", "MinimumLotQty", "min_qty"):
            val = self._num(raw.get(key), 0.0)
            if val > 0:
                return val
        # No invented option lot size.  A missing broker/security-master lot
        # is a routing failure, not permission to trade one unit.
        return 0.0

    def _order_body(self, side: str, order_type: str, quantity: float,
                    price=None, trigger_price=None, reduce_only: bool = False,
                    **kwargs) -> Dict:
        order_type_u = str(order_type or "").upper()
        stop_order_type = str(kwargs.get("stop_order_type") or "").lower()
        if "MARKET" in order_type_u and not reduce_only:
            raise RuntimeError("ICICI options guard: market entries are disabled; use limit orders")
        raw = self._active_raw()
        if not self._has_contract_identity(raw):
            raise RuntimeError("ICICI options guard: exact NFO option identity is required before routing an order")
        exchange_code = str(raw.get("exchange_code") or "NFO").upper()
        if exchange_code != "NFO":
            raise RuntimeError(f"ICICI options guard: expected NFO option contract, received exchange={exchange_code}")
        action = "sell" if reduce_only else "buy"
        px = price if price is not None else trigger_price
        if (px is None or float(px or 0.0) <= 0) and reduce_only:
            px = raw.get("selected_entry_premium") or raw.get("ltp") or raw.get("last_price") or raw.get("close")
        if px is None or float(px or 0.0) <= 0:
            raise RuntimeError("ICICI options guard: executable limit price is required")
        lot_raw = float(self._lot_size() or 0.0)
        if lot_raw <= 0:
            raise RuntimeError("ICICI options guard: verified NFO option lot size is required before routing an order")
        lot = int(round(lot_raw))
        if lot <= 0 or abs(lot_raw - lot) > 1e-9:
            raise RuntimeError(f"ICICI options guard: invalid NFO option lot size={lot_raw!r}")
        requested = float(quantity or 0.0)
        lots = int(math.floor((requested / lot) + 1e-9))
        if lots < 1:
            raise RuntimeError(f"ICICI options guard: requested quantity={requested:g} does not fit one lot={lot}")
        qty = int(lots * lot)
        is_stop = order_type_u.startswith("STOP") or stop_order_type == "stop_loss_order"
        body = {
            "stock_code": str(raw.get("stock_code") or raw.get("ShortName") or "").upper(),
            "exchange_code": "NFO",
            "product": "options",
            "action": action,
            "order_type": "stoploss" if is_stop else "limit",
            "quantity": qty,
            "price": str(px),
            "validity": "day",
            "expiry_date": self.api._normalise_expiry(raw.get("expiry_date") or raw.get("ExpiryDate") or ""),
            "right": self.api._normalise_right(raw.get("right") or raw.get("OptionType") or ""),
            "strike_price": str(raw.get("strike_price") or raw.get("StrikePrice") or ""),
        }
        if is_stop:
            if trigger_price is None or float(trigger_price or 0.0) <= 0:
                raise RuntimeError("ICICI options guard: stoploss order requires a positive trigger price")
            body["stoploss"] = str(trigger_price)
        return {k: v for k, v in body.items() if v not in (None, "")}

    def extract_order_id(self, resp: Dict) -> Optional[str]:
        if not isinstance(resp, dict):
            return None
        data = resp.get("Success") or resp.get("success") or resp.get("data") or resp.get("result") or resp
        if isinstance(data, list) and data:
            data = data[0]
        if isinstance(data, dict):
            oid = data.get("order_id") or data.get("OrderId") or data.get("orderId") or data.get("id")
            return str(oid) if oid else None
        if isinstance(data, str) and data.strip():
            return data.strip()
        return None

    def extract_status(self, order_data: Dict) -> str:
        raw = str(order_data.get("status") or order_data.get("Status") or order_data.get("order_status") or "").upper()
        if raw in {"EXECUTED", "FILLED", "COMPLETE", "COMPLETED"}:
            return "FILLED"
        if raw in {"CANCELLED", "CANCELED", "REJECTED", "EXPIRED"}:
            return "CANCELLED"
        if raw in {"PARTIALLY_FILLED", "PARTIAL"}:
            return "PARTIAL_FILL"
        return "PENDING" if raw else "UNKNOWN"

    def extract_fill_price(self, order_data: Dict) -> Optional[float]:
        for f in ("average_price", "avg_price", "price", "execution_price"):
            p = self._num(order_data.get(f), 0.0)
            if p > 0:
                return p
        return None

    def extract_filled_qty(self, order_data: Dict) -> float:
        for f in ("filled_quantity", "executed_quantity", "quantity"):
            q = self._num(order_data.get(f), 0.0)
            if q > 0:
                return q
        return 0.0

    def place_bracket_limit_entry(self, side: str, quantity: float, limit_price: float, sl_price: float, tp_price: float) -> Optional[Dict]:
        """Route ICICI long-premium entries through documented three-leg cover OCO.

        The strategy side identifies the underlying thesis; the option vehicle is
        always bought.  No normal entry order is sent if protected GTT placement
        fails.
        """
        if not bool(getattr(config, "ICICI_REQUIRE_GTT_COVER_OCO_PROTECTED_ENTRY", True)):
            return {"_error": True, "_raw": {"error": "ICICI_GTT_COVER_OCO_DISABLED_FAIL_CLOSED"}}
        self.limiter.wait()
        try:
            raw = self._active_raw()
            if not self._has_contract_identity(raw):
                raise RuntimeError("ICICI protected entry requires an exact selected NFO option contract")
            # Reuse lot validation; this never sends an order.
            entry_body = self._order_body(side, "LIMIT", quantity, price=limit_price, reduce_only=False)
            qty = int(entry_body["quantity"])
            tick = max(float(self.tick_size or 0.05), 0.01)
            def _round_nearest(value: float) -> float:
                return round(round(float(value) / tick) * tick, 2)
            entry = _round_nearest(limit_price)
            target_trigger = _round_nearest(tp_price)
            target_limit = target_trigger
            stop_trigger = _round_nearest(sl_price)
            stop_limit = _round_nearest(max(tick, stop_trigger - tick))
            if not (entry > 0 and stop_limit > 0 and stop_trigger < entry < target_trigger):
                raise RuntimeError(
                    f"ICICI GTT geometry invalid: stop_limit={stop_limit} stop_trigger={stop_trigger} entry={entry} target={target_trigger}"
                )
            ist = timezone(timedelta(hours=5, minutes=30))
            trade_date = datetime.now(ist).strftime("%Y-%m-%dT06:00:00.000Z")
            payload = {
                "exchange_code": "NFO",
                "stock_code": str(raw.get("stock_code") or raw.get("ShortName") or "").upper(),
                "product": "options",
                "quantity": str(qty),
                "expiry_date": raw.get("expiry_date") or raw.get("ExpiryDate") or "",
                "right": self.api._normalise_right(raw.get("right") or raw.get("OptionType") or ""),
                "strike_price": str(raw.get("strike_price") or raw.get("StrikePrice") or ""),
                "gtt_type": "cover_oco",
                "fresh_order_action": "buy",
                "fresh_order_price": str(entry),
                "fresh_order_type": "limit",
                "index_or_stock": "index",
                "trade_date": trade_date,
                "order_details": [
                    {"gtt_leg_type": "target", "action": "sell", "limit_price": str(target_limit), "trigger_price": str(target_trigger)},
                    {"gtt_leg_type": "stoploss", "action": "sell", "limit_price": str(stop_limit), "trigger_price": str(stop_trigger)},
                ],
            }
            response = self.api.place_gtt_three_leg_oco(**payload)
            success = response.get("Success") if isinstance(response, dict) else None
            if not isinstance(success, dict):
                return {"_raw": response, "_sc": 0, "_error": True}
            gtt_id = str(success.get("gtt_order_id") or success.get("gttOrderId") or "").strip()
            if not gtt_id:
                return {"_raw": response, "_sc": 0, "_error": True}
            oid = f"GTT:{gtt_id}"
            self._gtt_plans[gtt_id] = {"quantity": qty, "entry": entry, "sl": stop_trigger, "tp": target_trigger, "payload": payload}
            logger.info(
                "ICICI protected cover-OCO accepted gtt_id=%s option=%s %s %s qty=%s entry=₹%.2f SL=₹%.2f TP=₹%.2f",
                gtt_id, payload["stock_code"], payload["right"], payload["strike_price"], qty, entry, stop_trigger, target_trigger,
            )
            return {
                "order_id": oid, "gtt_order_id": gtt_id, "status": "PENDING", "quantity": float(qty), "price": entry,
                "bracket_order": True, "bracket_child_verified": True,
                "bracket_sl_order_id": f"{oid}:STOPLOSS", "bracket_tp_order_id": f"{oid}:TARGET",
                "bracket_sl_price": stop_trigger, "bracket_tp_price": target_trigger,
                "protection_model": "ICICI_GTT_COVER_OCO", "_raw": response,
            }
        except Exception as exc:
            logger.error("ICICI protected cover-OCO entry rejected before exposure: %s", exc)
            return {"_raw": {"error": str(exc)}, "_sc": 0, "_error": True}

    def _normal_order_row(self, order_id: str) -> Optional[Dict[str, Any]]:
        """Read a normal Breeze order without invoking GTT pseudo-id routing."""
        getter = getattr(self.api, "get_order", None) or getattr(self.api, "get_order_detail", None)
        if not callable(getter):
            return None
        raw = self._active_raw()
        try:
            resp = getter(order_id=str(order_id), exchange_code=str(raw.get("exchange_code") or "NFO").upper())
        except TypeError:
            try:
                resp = getter(str(order_id))
            except Exception:
                return None
        except Exception:
            return None
        data = self._success_payload(resp)
        if data:
            data.setdefault("order_id", str(order_id))
            return data
        return None

    def _gtt_row(self, order_id: str) -> Optional[Dict[str, Any]]:
        """Resolve official Breeze cover-OCO pseudo ids through the GTT book.

        Official GTT order-book rows identify the GTT id on each ``order_details``
        leg and expose the immediately routed fresh-entry order as
        ``fresh_order_id``.  It is not a top-level ``gtt_order_id`` field.
        Therefore entry fills must be resolved through that concrete normal
        order id; target/stoploss legs are never reported filled from planned
        trigger/limit prices alone.
        """
        token = str(order_id or "")
        if not token.startswith("GTT:"):
            return None
        bits = token.split(":")
        gtt_id = bits[1] if len(bits) > 1 else ""
        leg = bits[2].lower() if len(bits) > 2 else "fresh"
        now = datetime.now(timezone.utc)
        resp = self.api.get_gtt_order_book(
            exchange_code="NFO",
            from_date=(now - timedelta(days=2)).strftime("%Y-%m-%dT06:00:00.000Z"),
            to_date=now.strftime("%Y-%m-%dT06:00:00.000Z"),
        )
        rows = resp.get("Success") if isinstance(resp, dict) else []
        if isinstance(rows, dict):
            rows = [rows]

        def _row_gtt_ids(row: Dict[str, Any]) -> set[str]:
            ids = {str(row.get("gtt_order_id") or row.get("gttOrderId") or "").strip()}
            for detail in row.get("order_details") or []:
                if isinstance(detail, dict):
                    ids.add(str(detail.get("gtt_order_id") or detail.get("gttOrderId") or "").strip())
            return {item for item in ids if item}

        matched = next((row for row in (rows or []) if isinstance(row, dict) and gtt_id in _row_gtt_ids(row)), None)
        if not isinstance(matched, dict):
            return {"order_id": token, "status": "PENDING"}

        fresh_order_id = str(matched.get("fresh_order_id") or matched.get("freshOrderId") or "").strip()
        if leg == "fresh" and fresh_order_id:
            actual = self._normal_order_row(fresh_order_id)
            if isinstance(actual, dict):
                actual["order_id"] = token
                actual["broker_order_id"] = fresh_order_id
                actual["_gtt_raw"] = matched
                return actual

        details = matched.get("order_details") or []
        candidate = None
        for detail in details if isinstance(details, list) else []:
            if not isinstance(detail, dict):
                continue
            dleg = str(detail.get("gtt_leg_type") or "fresh").strip().lower()
            if (leg == "fresh" and dleg in {"", "fresh", "none"}) or dleg == leg:
                candidate = detail
                break
        candidate = candidate or {}
        status = candidate.get("status") or matched.get("status") or "Pending"
        # Never substitute the planned trigger/limit as an actual execution fill.
        # A filled child without an exact order execution remains pending P&L reconciliation.
        px = candidate.get("average_price") or candidate.get("execution_price") or 0.0
        qty = candidate.get("filled_quantity") or candidate.get("executed_quantity") or matched.get("quantity") or 0.0
        child_order_id = str(candidate.get("order_id") or candidate.get("broker_order_id") or "").strip()
        if child_order_id:
            actual = self._normal_order_row(child_order_id)
            if isinstance(actual, dict):
                actual["order_id"] = token
                actual["broker_order_id"] = child_order_id
                actual["_gtt_raw"] = matched
                return actual
        return {"order_id": token, "status": status, "average_price": px, "quantity": qty, "_raw": matched}

    def place_order(self, side: str, order_type: str, quantity: float,
                    price: Optional[float] = None,
                    trigger_price: Optional[float] = None,
                    reduce_only: bool = False,
                    **kwargs) -> Optional[Dict]:
        self.limiter.wait()
        try:
            body = self._order_body(side, order_type, quantity, price=price, trigger_price=trigger_price, reduce_only=reduce_only, **kwargs)
            resp = self.api.place_order(**body)
            oid = self.extract_order_id(resp)
            if not oid:
                return {"_raw": resp, "_sc": 0, "_error": True}
            return {"order_id": oid, "status": "PENDING", "quantity": float(body.get("quantity") or quantity), "price": float(body.get("price") or 0.0), "_raw": resp}
        except Exception as exc:
            return {"_raw": {"error": str(exc)}, "_sc": 0, "_error": True}

    def cancel_order(self, order_id: str) -> Dict:
        self.limiter.wait()
        raw = self._active_raw()
        token = str(order_id or "")
        if token.startswith("GTT:"):
            gtt_id = token.split(":")[1]
            return self.api.cancel_gtt_three_leg_order(exchange_code="NFO", gtt_order_id=gtt_id) or {}
        try:
            return self.api.cancel_order(order_id=token, exchange_code=str(raw.get("exchange_code") or "NFO").upper()) or {}
        except TypeError:
            return self.api.cancel_order(order_id=token) or {}

    def get_order(self, order_id: str) -> Optional[Dict]:
        if str(order_id or "").startswith("GTT:"):
            self.limiter.wait()
            return self._gtt_row(str(order_id))
        getter = getattr(self.api, "get_order", None) or getattr(self.api, "get_order_detail", None)
        if not callable(getter):
            return {"order_id": str(order_id), "status": "PENDING"}
        self.limiter.wait()
        raw = self._active_raw()
        try:
            resp = getter(order_id=str(order_id), exchange_code=str(raw.get("exchange_code") or "NFO").upper())
        except TypeError:
            try:
                resp = getter(str(order_id))
            except Exception:
                return {"order_id": str(order_id), "status": "PENDING"}
        except Exception:
            return {"order_id": str(order_id), "status": "PENDING"}
        data = self._success_payload(resp)
        if data:
            data.setdefault("order_id", str(order_id))
            return data
        return {"order_id": str(order_id), "status": "PENDING"}

    @classmethod
    def _extract_paid_commission(cls, payload: Optional[Dict]) -> tuple[float, bool]:
        """Return only broker-published aggregate charges; never estimate NFO costs."""
        if not isinstance(payload, dict):
            return 0.0, False
        sources = [payload]
        raw = payload.get("_raw")
        if isinstance(raw, dict):
            sources.insert(0, raw)
        for src in sources:
            for key in ("total_charges", "total_charge", "total_fees", "total_fee", "paid_commission"):
                if key in src and src.get(key) not in (None, ""):
                    return cls._num(src.get(key), 0.0), True
        return 0.0, False

    def resolve_order_execution(self, order_id: str) -> Optional[Dict]:
        """Resolve an NFO close from exact Breeze order/trade records when exposed."""
        oid = str(order_id or "").strip()
        if not oid:
            return None
        raw_order = self.get_order(oid)
        if not isinstance(raw_order, dict):
            return None
        status = self.extract_status(raw_order)
        fill_price = float(self.extract_fill_price(raw_order) or 0.0)
        filled_qty = float(self.extract_filled_qty(raw_order) or 0.0)
        fee_paid, fee_exact = self._extract_paid_commission(raw_order)
        trade_getter = getattr(self.api, "get_trade_detail", None)
        if not oid.startswith("GTT:") and callable(trade_getter) and status in {"FILLED", "PARTIAL_FILL"}:
            try:
                self.limiter.wait()
                try:
                    resp = trade_getter(exchange_code="NFO", order_id=oid)
                except TypeError:
                    resp = trade_getter(order_id=oid)
                rows = resp.get("Success") if isinstance(resp, dict) else resp
                if isinstance(rows, dict):
                    rows = [rows]
                num = den = 0.0
                exact_fees = 0.0
                exact_fee_seen = False
                for row in (rows if isinstance(rows, list) else []):
                    if not isinstance(row, dict):
                        continue
                    px = self._num(row.get("average_price") or row.get("avg_price") or row.get("execution_price") or row.get("trade_price") or row.get("price"), 0.0)
                    qty = abs(self._num(row.get("filled_quantity") or row.get("executed_quantity") or row.get("traded_quantity") or row.get("quantity"), 0.0))
                    if px > 0 and qty > 0:
                        num += px * qty
                        den += qty
                    row_fee, row_fee_exact = self._extract_paid_commission(row)
                    if row_fee_exact:
                        exact_fees += row_fee
                        exact_fee_seen = True
                if den > 0:
                    fill_price = num / den
                    filled_qty = den
                if exact_fee_seen:
                    fee_paid, fee_exact = exact_fees, True
            except Exception as exc:
                logger.debug("ICICI get_trade_detail execution resolution unavailable for %s: %s", oid, exc)
        return {
            "status": status,
            "fill_price": float(fill_price or 0.0),
            "filled_qty": float(filled_qty or 0.0),
            "paid_commission": float(fee_paid or 0.0),
            "paid_commission_exact": bool(fee_exact),
            "raw_order": raw_order,
        }

    def get_open_orders(self, symbol: str) -> Optional[list]:
        getter = getattr(self.api, "get_order_list", None)
        if not callable(getter):
            return []
        now = datetime.now(timezone.utc)
        try:
            self.limiter.wait()
            resp = getter(
                exchange_code="NFO",
                from_date=(now - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                to_date=now.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            )
        except Exception as exc:
            logger.warning("ICICI NFO open-order recovery unavailable: %s", exc)
            return []
        rows = resp.get("Success") if isinstance(resp, dict) else resp
        rows = rows if isinstance(rows, list) else []
        active = self._active_raw()
        active_key = self._contract_key(active) if self._has_contract_identity(active) else None
        open_orders = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            ok, _ = self._is_exact_nfo_option_order(row)
            if not ok or (active_key and self._contract_key(row) != active_key):
                continue
            status = str(row.get("status") or row.get("Status") or "").strip().upper()
            if status in {"EXECUTED", "FILLED", "COMPLETE", "COMPLETED", "CANCELLED", "CANCELED", "REJECTED", "EXPIRED"}:
                continue
            order_type = str(row.get("order_type") or "").strip().upper()
            stoploss = self._num(row.get("stoploss") or row.get("SLTP_price"), 0.0)
            typ = "STOP_LOSS" if order_type == "STOPLOSS" or stoploss > 0 else "LIMIT"
            oid = row.get("order_id") or row.get("OrderId")
            if oid:
                open_orders.append({"order_id": str(oid), "type": typ, "trigger_price": stoploss, "raw": row})
        return open_orders

    def get_positions(self, symbol: str) -> Optional[Dict]:
        try:
            return self.api.get_portfolio_positions()
        except Exception as exc:
            if "no positions available" in str(exc).lower():
                logger.info("ICICI NFO PortfolioPositions verified flat: %s", exc)
                return {"Success": [], "Status": 200, "Error": None, "_empty_positions": True}
            logger.error("ICICI NFO PortfolioPositions fetch failed: %s", exc)
            return None

    def normalise_position(self, raw) -> Optional[Dict]:
        rows = raw.get("Success") if isinstance(raw, dict) else raw
        if isinstance(rows, dict):
            rows = rows.get("positions") or rows.get("data") or [rows]
        positions = rows if isinstance(rows, list) else []
        valid_rows, ignored = self._valid_open_option_rows(positions)
        self._log_filtered_broker_rows(ignored)
        flat = {
            "side": None, "size": 0.0, "size_signed": 0.0, "entry_price": 0.0,
            "unrealized_pnl": 0.0, "currency": "INR", "segment": "FNO",
            "product_type": "Options", "position_scope_verified": True,
            "ignored_non_option_rows": len(ignored),
        }
        if not valid_rows:
            return flat

        active = self._active_raw()
        active_key = self._contract_key(active) if self._has_contract_identity(active) else None
        normalised = [self._normalised_position_row(row, source="broker_portfolio_exact") for row in valid_rows]
        if active_key:
            for row, position in zip(valid_rows, normalised):
                if self._contract_key(row) == active_key:
                    position["contract_identity_source"] = "selected_contract_match"
                    position.pop("requires_contract_reconstruction", None)
                    return position
            first = dict(normalised[0])
            first["unadoptable"] = True
            first["reason"] = "exact_nfo_option_does_not_match_selected_contract"
            first["external_positions"] = normalised
            logger.critical(
                "ICICI exact NFO option position exists but does not match selected vehicle: "
                "symbol=%s right=%s strike=%s expiry=%s qty=%.8g",
                first.get("TradingSymbol") or first.get("stock_code") or "-",
                first.get("right") or "-", first.get("strike_price") or "-",
                first.get("expiry_date") or "-", float(first.get("size", 0.0) or 0.0),
            )
            return first

        if len(normalised) == 1:
            position = normalised[0]
            logger.warning(
                "ICICI exact NFO option position detected before a selected option vehicle exists: "
                "symbol=%s right=%s strike=%s expiry=%s qty=%.8g",
                position.get("TradingSymbol") or position.get("stock_code") or "-",
                position.get("right") or "-", position.get("strike_price") or "-",
                position.get("expiry_date") or "-", float(position.get("size", 0.0) or 0.0),
            )
            return position
        first = dict(normalised[0])
        first["multiple_broker_positions"] = True
        first["external_positions"] = normalised
        first["size"] = sum(float(p.get("size", 0.0) or 0.0) for p in normalised)
        first["unrealized_pnl"] = sum(float(p.get("unrealized_pnl", 0.0) or 0.0) for p in normalised)
        first["unadoptable"] = True
        first["reason"] = "multiple_exact_nfo_option_positions_require_manual_selection"
        logger.critical("ICICI multiple exact NFO option positions found; refusing automatic adoption")
        return first

    def _parse_fno_funds(self, resp: Dict[str, Any]) -> Dict[str, Any]:
        data = self._success_payload(resp)
        allocated = self._num(data.get("allocated_fno", data.get("allocated_FNO", data.get("allocated_derivatives"))))
        blocked = self._num(data.get("block_by_trade_fno", data.get("blocked_fno", data.get("block_by_trade_derivatives"))))
        unallocated = self._num(data.get("unallocated_balance"))
        bank_total = self._num(data.get("total_bank_balance"))
        available = max(0.0, allocated - max(0.0, blocked))
        return {"allocated": allocated, "blocked": max(0.0, blocked), "available": available, "unallocated": max(0.0, unallocated), "bank_total": max(0.0, bank_total), "source": "funds.allocated_fno_minus_block_by_trade_fno", "raw": resp}

    def _parse_nfo_margin(self, resp: Dict[str, Any]) -> Dict[str, Any]:
        data = self._success_payload(resp)
        cash_limit = self._num(data.get("cash_limit"))
        amount_allocated = self._num(data.get("amount_allocated"))
        blocked = self._num(data.get("block_by_trade"))
        base = cash_limit if cash_limit > 0 else amount_allocated
        available = max(0.0, base - max(0.0, blocked))
        return {"cash_limit": max(0.0, cash_limit), "amount_allocated": max(0.0, amount_allocated), "blocked": max(0.0, blocked), "available": available, "source": "margin.NFO.cash_limit_minus_block_by_trade" if cash_limit > 0 else "margin.NFO.amount_allocated_minus_block_by_trade", "raw": resp}

    def get_balance(self) -> Dict:
        funds_resp: Dict[str, Any] = {}
        margin_resp: Dict[str, Any] = {}
        errors = []
        try:
            self.limiter.wait()
            funds_resp = self.api.get_funds()
        except Exception as exc:
            errors.append(f"funds: {exc}")
        fno = self._parse_fno_funds(funds_resp) if funds_resp else {"allocated": 0.0, "blocked": 0.0, "available": 0.0, "unallocated": 0.0, "bank_total": 0.0, "source": "funds.unavailable", "raw": funds_resp}
        try:
            self.limiter.wait()
            margin_resp = self.api.get_margin(exchange_code="NFO")
        except Exception as exc:
            errors.append(f"margin.NFO: {exc}")
        margin = self._parse_nfo_margin(margin_resp) if margin_resp else {"cash_limit": 0.0, "amount_allocated": 0.0, "blocked": 0.0, "available": 0.0, "source": "margin.NFO.unavailable", "raw": margin_resp}
        if fno["available"] > 0 or fno["allocated"] > 0:
            available, locked, total, source = fno["available"], fno["blocked"], fno["allocated"], fno["source"]
        elif margin["available"] > 0:
            available, locked, total, source = margin["available"], margin["blocked"], max(margin["cash_limit"], margin["amount_allocated"], margin["available"] + margin["blocked"]), margin["source"]
        else:
            available, locked, total, source = 0.0, max(fno["blocked"], margin["blocked"]), max(fno["allocated"], margin["cash_limit"], margin["amount_allocated"]), "zero_fno_allocation_or_unavailable"
        out = {
            "available": max(0.0, available),
            "available_raw": max(0.0, available),
            "locked": max(0.0, locked),
            "total": max(0.0, total),
            "currency": "INR",
            "segment": "FNO",
            "source": source,
            "fno_allocated": fno["allocated"],
            "fno_blocked": fno["blocked"],
            "fno_available": fno["available"],
            "unallocated_balance": fno["unallocated"],
            "bank_total": fno["bank_total"],
            "nfo_cash_limit": margin["cash_limit"],
            "nfo_amount_allocated": margin["amount_allocated"],
            "nfo_blocked": margin["blocked"],
            "nfo_available": margin["available"],
            "raw": {"funds": funds_resp, "margin_nfo": margin_resp},
        }
        if errors:
            out["warning"] = "; ".join(errors)
            if not funds_resp and not margin_resp:
                out["error"] = out["warning"]
        logger.info("ICICI F&O balance source=%s available=%.2f allocated=%.2f blocked=%.2f nfo_cash_limit=%.2f unallocated=%.2f", out["source"], out["available"], out["fno_allocated"], out["fno_blocked"], out["nfo_cash_limit"], out["unallocated_balance"])
        return out

    def set_leverage(self, leverage: int, product_id: Optional[int] = None) -> Dict:
        return {"success": True, "leverage": 1, "message": "ICICI long-premium options are fully funded; leverage is not applicable"}


class OrderManager:
    """
    Exchange-agnostic order manager.
    Inject a CoinSwitchAPI or DeltaAPI; this class handles the rest.
    """

    _MAX_RETRIES       = 2     # was 3 — 3 attempts × 30s timeout = 90s minimum block
    _RETRY_BASE_SLEEP  = 1.0   # was 3.0 — base sleep for exponential backoff
    _MAX_401_RETRIES   = 2     # was 3 — auth errors rarely fix themselves
    _401_RETRY_DELAY   = 1.0   # was 2.0

    def __init__(self, api, exchange_name: str = "coinswitch", instrument=None) -> None:
        exch = exchange_name.lower()
        self.instrument = instrument
        exchange_instrument = None
        if instrument is not None and hasattr(instrument, "by_exchange"):
            try:
                ex_key = ExchangeName(exch)
                exchange_instrument = instrument.by_exchange.get(ex_key)
            except Exception:
                exchange_instrument = None
        if exch == "delta":
            self._adapter = _DeltaAdapter(api, exchange_instrument=exchange_instrument)
        elif exch == "icici":
            self._adapter = _ICICIAdapter(api, exchange_instrument=exchange_instrument)
        else:
            self._adapter = _CoinSwitchAdapter(api, exchange_instrument=exchange_instrument)

        self.api            = api         # kept for compatibility access
        self._exchange_name = exch
        self.symbol         = self._adapter.symbol
        self.display_symbol = getattr(self._adapter, "display_symbol", self.symbol)
        self._orders_lock   = threading.RLock()
        self.active_orders: Dict[str, Dict] = {}
        # BUG-1 FIX: plain list grew forever — swap to deque so memory is bounded.
        # 1 000 entries covers well over a year of daily trading; oldest records
        # are evicted automatically once the cap is reached.
        self.order_history: deque = deque(maxlen=1_000)
        self.last_order_error: Optional[Dict] = None
        self._rate_window_start = time.time()
        self._rate_window_count = 0
        self._open_orders_404   = False

        # ── FIX (third-trade bug): self-cancelled-orders tracker ──────────────
        # When we cancel an SL ourselves (cancel+replace path), we record the
        # order_id + timestamp here. This lets downstream code distinguish
        # "SL was filled by the market" (exchange fired it) from
        # "SL was cancelled by us and the replacement failed" (orphaned).
        # A 404 on an id in this set is a GHOST, not an exit. Entries expire
        # after _SELF_CANCEL_TTL_SEC so the dict cannot grow unbounded.
        self._self_cancelled_orders: Dict[str, float] = {}
        self._SELF_CANCEL_TTL_SEC = 120.0

        # Expose for callers that do order_manager.CancelResult
        self.CancelResult = CancelResult

        # NOTE: GlobalRateLimiter is NOT set here.
        # When the ExecutionRouter is used (the normal code path), the router
        # owns both OMs and calls _sync_global_limiter() after construction,
        # pointing GlobalRateLimiter at whichever exchange is currently active.
        # Calling set_active() here caused a race: the last OM constructed
        # (always delta, since it's created second) won, so GlobalRateLimiter
        # always pointed at the delta limiter even when CoinSwitch was the active
        # exchange — silently applying the wrong rate-limit interval to all calls.
        # The single-OM compatibility path (no router) can call GlobalRateLimiter.set_active()
        # explicitly after construction if needed.

        logger.info(f"✅ OrderManager initialised (exchange={exch}, symbol={self.symbol})")

    @property
    def limiter(self) -> _RateLimiter:
        return self._adapter.limiter

    # ── Internal helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _compact_error(raw) -> str:
        try:
            if isinstance(raw, dict):
                err = raw.get("error")
                if isinstance(err, dict):
                    code = err.get("code") or ""
                    msg = err.get("message") or ""
                    ctx = err.get("context") or {}
                    ctx_s = f" ctx={ctx}" if ctx else ""
                    return f"{code}: {msg}{ctx_s}".strip(": ")
                if raw.get("error"):
                    return str(raw.get("error"))
                if raw.get("message"):
                    return str(raw.get("message"))
            return str(raw)[:500]
        except Exception:
            return str(raw)[:500]

    @staticmethod
    def _normalize_side(side: str) -> str:
        s = side.upper().strip()
        if s in ("LONG", "BUY"):   return "BUY"
        if s in ("SHORT", "SELL"): return "SELL"
        raise ValueError(f"Invalid side '{side}'")

    def _currency_symbol(self) -> str:
        return "₹" if str(getattr(self, "_exchange_name", "")).lower() == "icici" else "$"

    def _active_tick_size(self) -> float:
        """Return the executable tick size for the active contract.

        Multi-asset Delta products do not share BTC's tick size.  Using the
        global config tick silently corrupts stop-limit offsets and trail
        replacement prices on PAXG/SLVON/xStock contracts.
        """
        try:
            tick = float(getattr(self._adapter, "tick_size", 0.0) or 0.0)
            if tick > 0:
                return tick
        except Exception:
            pass
        try:
            inst = getattr(self, "instrument", None)
            by_ex = getattr(inst, "by_exchange", {}) or {}
            # Keys may be ExchangeName enums or strings depending on construction.
            for key, ei in by_ex.items():
                try:
                    if str(key).lower().endswith("delta") or str(key).lower() == "delta":
                        tick = float(getattr(ei, "tick_size", 0.0) or 0.0)
                        if tick > 0:
                            return tick
                except Exception:
                    continue
        except Exception:
            pass
        getter = getattr(config, "get_tick_size", None)
        if callable(getter):
            return float(getter())
        return float(getattr(config, "TICK_SIZE", 0.1))

    def _round_price_to_tick(self, price: float) -> float:
        tick = max(self._active_tick_size(), 1e-9)
        rounded = round(float(price) / tick) * tick
        return round(rounded, 8)

    def _sl_limit_price(self, api_side: str, trigger_price: float) -> float:
        tick = self._active_tick_size()
        offset_ticks = int(getattr(config, "SL_LIMIT_OFFSET_TICKS", 20))
        limit_offset = offset_ticks * tick
        raw = trigger_price + limit_offset if api_side == "BUY" else trigger_price - limit_offset
        return self._round_price_to_tick(raw)

    def _validate_stop_trigger(self, api_side: str, trigger_price: float,
                               current_price: Optional[float]) -> Optional[str]:
        if current_price is None or current_price <= 0:
            return None
        tick = self._active_tick_size()
        if api_side == "BUY" and trigger_price <= current_price + tick * 0.5:
            return f"BUY stop {trigger_price:.2f} must be above market {current_price:.2f}"
        if api_side == "SELL" and trigger_price >= current_price - tick * 0.5:
            return f"SELL stop {trigger_price:.2f} must be below market {current_price:.2f}"
        return None

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
        """Place order with exponential-backoff retry on transient errors.

        Retry matrix:
          429 → notify limiter, exponential backoff, continue
          5xx → exponential backoff, continue
          401 → track consecutive 401s, sleep, continue; abort after MAX_401_RETRIES
          other → non-retryable, return None immediately

        consecutive_401s resets on any non-401 attempt so the counter
        tracks CONSECUTIVE 401s, not total 401s across the entire loop.
        """
        consecutive_401s = 0
        max_attempts     = self._MAX_RETRIES + self._MAX_401_RETRIES
        for attempt in range(max_attempts):
            result = self._adapter.place_order(**kwargs)

            # ── Success ───────────────────────────────────────────────
            if result and not result.get("_error"):
                return result

            sc  = (result or {}).get("_sc", 0)
            raw = (result or {}).get("_raw", {})

            # ── 429 Too Many Requests ─────────────────────────────────
            if sc == 429:
                consecutive_401s = 0   # reset; 429 breaks the 401 chain
                self._adapter.limiter.notify_429()
                time.sleep(self._RETRY_BASE_SLEEP * (2 ** min(attempt, 4)))
                continue

            # ── Transient server error ────────────────────────────────
            if sc in (500, 502, 503):
                consecutive_401s = 0   # reset; server error breaks 401 chain
                time.sleep(self._RETRY_BASE_SLEEP * (2 ** min(attempt, 3)))
                continue

            # ── Auth error ────────────────────────────────────────────
            if sc == 401:
                consecutive_401s += 1
                if consecutive_401s <= self._MAX_401_RETRIES:
                    logger.warning(
                        f"place_order 401 (attempt {attempt+1}/{max_attempts}, "
                        f"consecutive={consecutive_401s}) — retrying"
                    )
                    time.sleep(self._401_RETRY_DELAY * consecutive_401s)
                    continue
                logger.error(
                    f"place_order: 401 persisted {consecutive_401s} times — "
                    f"check API credentials/signature"
                )
                return None

            # ── Non-retryable ─────────────────────────────────────────
            logger.error(f"place_order non-retryable failure: sc={sc} raw={raw}")
            return None

        logger.error(f"place_order exhausted all {max_attempts} retries")
        return None

    def _record_order(self, order_id: str, meta: Dict) -> None:
        with self._orders_lock:
            self.active_orders[order_id] = meta
            self.order_history.append(meta.copy())

    def _remove_active_order(self, order_id: str) -> None:
        with self._orders_lock:
            self.active_orders.pop(order_id, None)

    # ── FIX (third-trade bug): self-cancellation tracker helpers ──────────────
    def _mark_self_cancelled(self, order_id: str) -> None:
        """Record that WE cancelled this order. Used to disambiguate 404s."""
        if not order_id:
            return
        now = time.time()
        with self._orders_lock:
            self._self_cancelled_orders[str(order_id)] = now
            # Prune stale entries (bounded memory guarantee)
            _stale = [oid for oid, ts in self._self_cancelled_orders.items()
                      if now - ts > self._SELF_CANCEL_TTL_SEC]
            for oid in _stale:
                self._self_cancelled_orders.pop(oid, None)

    def _was_self_cancelled(self, order_id: str) -> bool:
        if not order_id:
            return False
        with self._orders_lock:
            ts = self._self_cancelled_orders.get(str(order_id))
            if ts is None:
                return False
            if time.time() - ts > self._SELF_CANCEL_TTL_SEC:
                self._self_cancelled_orders.pop(str(order_id), None)
                return False
            return True

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
            resolved = None
            if hasattr(self._adapter, "resolve_order_execution"):
                try:
                    resolved = self._adapter.resolve_order_execution(order_id)
                except Exception as _rx_e:
                    logger.debug(f"resolve_order_execution error for {order_id}: {_rx_e}")

            data = (resolved or {}).get("raw_order") if isinstance(resolved, dict) else None
            if not data:
                data = self.get_order_status(order_id, retry_count=2)
            if not data:
                return None

            status     = (resolved or {}).get("status") if isinstance(resolved, dict) else None
            status     = status or self._adapter.extract_status(data)
            fill_price = float((resolved or {}).get("fill_price") or 0.0) if isinstance(resolved, dict) else 0.0
            if fill_price <= 0:
                fill_price = self._adapter.extract_fill_price(data) or 0.0
            filled_qty = float((resolved or {}).get("filled_qty") or 0.0) if isinstance(resolved, dict) else 0.0
            if filled_qty <= 0:
                filled_qty = self._adapter.extract_filled_qty(data)

            req_qty    = 0.0
            for f in ("quantity", "size_btc", "orig_qty"):
                v = data.get(f)
                if v:
                    try:
                        req_qty = float(v)
                        if req_qty > 0: break
                    except (ValueError, TypeError): pass
            if req_qty <= 0 and hasattr(self._adapter, "_contracts_to_qty"):
                for f in ("size", "requested_size"):
                    v = data.get(f)
                    if v:
                        try:
                            req_qty = self._adapter._contracts_to_qty(float(v))
                            if req_qty > 0: break
                        except Exception:
                            pass
            is_partial = status == "PARTIAL_FILL"
            if filled_qty <= 0 and status == "FILLED":
                filled_qty = req_qty
            fill_pct = (filled_qty / req_qty * 100) if req_qty > 0 else 0.0

            paid_commission = 0.0
            commission_exact = False
            if isinstance(resolved, dict):
                paid_commission = float(resolved.get("paid_commission", 0.0) or 0.0)
                commission_exact = bool(resolved.get("paid_commission_exact", False))
            if not commission_exact and hasattr(self._adapter, "_extract_paid_commission"):
                paid_commission, commission_exact = self._adapter._extract_paid_commission(data)

            return {
                "status":                 status,
                "fill_price":             fill_price,
                "filled_qty":             filled_qty,
                "requested_qty":          req_qty,
                "is_partial":             is_partial,
                "fill_pct":               fill_pct,
                "paid_commission":        paid_commission,
                "paid_commission_exact":  commission_exact,
                "raw_data":               data,
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
            if self._exchange_name == "icici":
                if not reduce_only:
                    logger.error("ICICI Breeze guard: market entry rejected; NFO options require a priced LIMIT entry")
                    return None
                ex_pos = self.get_open_position()
                if not ex_pos or bool(ex_pos.get("unadoptable")):
                    logger.critical("ICICI emergency close refused: no exact adoptable NFO option position is available")
                    return None
                raw = ex_pos.get("raw") or {}
                ref = float(raw.get("ltp") or raw.get("LTP") or ex_pos.get("entry_price") or 0.0)
                if ref <= 0:
                    logger.critical("ICICI emergency close refused: no reference premium available for priced exit")
                    return None
                # Breeze prohibits market orders. For an emergency close, send an
                # aggressively marketable priced limit while retaining exact contract scope.
                slippage_pct = float(getattr(config, "ICICI_EMERGENCY_EXIT_LIMIT_BUFFER_PCT", 0.10))
                tick = max(float(getattr(self._adapter, "tick_size", 0.05) or 0.05), 0.01)
                if str(api_side).lower() == "sell":
                    limit_price = math.floor((ref * max(0.01, 1.0 - slippage_pct)) / tick) * tick
                else:
                    limit_price = math.ceil((ref * (1.0 + slippage_pct)) / tick) * tick
                limit_price = max(tick, limit_price)
                logger.critical(
                    "ICICI Breeze prohibits MARKET exits; routing emergency %s as aggressive LIMIT qty=%s premium_ref=₹%.2f limit=₹%.2f",
                    api_side.upper(), quantity, ref, limit_price,
                )
                return self.place_limit_order(side=side, quantity=quantity, price=limit_price, reduce_only=True)
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
                try:
                    details = self.get_fill_details(data["order_id"])
                    if details:
                        if float(details.get("fill_price", 0.0) or 0.0) > 0:
                            data["fill_price"] = float(details.get("fill_price", 0.0) or 0.0)
                        if float(details.get("filled_qty", 0.0) or 0.0) > 0:
                            data["quantity"] = float(details.get("filled_qty", 0.0) or 0.0)
                        data["paid_commission"] = float(details.get("paid_commission", 0.0) or 0.0)
                        data["paid_commission_exact"] = bool(details.get("paid_commission_exact", False))
                except Exception as _fee_e:
                    logger.debug(f"market order exact fee lookup deferred: {_fee_e}")
                logger.info(f"✅ Market order: {data['order_id']} fee=${float(data.get('paid_commission', 0.0) or 0.0):.4f} exact={bool(data.get('paid_commission_exact', False))}")
            return data
        except Exception as e:
            logger.error(f"place_market_order error: {e}", exc_info=True)
            return None

    def emergency_flatten(self, reason: str = "unprotected") -> Optional[Dict]:
        """
        Flatten any open position at market (reduce_only).

        Called by the strategy when replace_stop_loss returns UNPROTECTED,
        i.e. the position is live on the exchange with no stop-loss attached.
        Queries exchange state directly (no trust of strategy state) and
        sends an opposing reduce-only market order sized to the live qty.

        Returns the market order dict on success, None on failure.
        """
        try:
            ex_pos = self.get_open_position()
            if ex_pos is None:
                logger.info(f"emergency_flatten [{reason}]: no exchange position — nothing to do")
                return None
            ex_side = str(ex_pos.get("side") or "").upper()
            ex_size = abs(float(ex_pos.get("size", 0) or 0))
            min_qty = float(getattr(config, "MIN_POSITION_SIZE", 0.001))
            if ex_size < min_qty:
                logger.info(
                    f"emergency_flatten [{reason}]: exchange size {ex_size} "
                    f"below min {min_qty} — treating as flat")
                return None
            if ex_side not in ("LONG", "SHORT"):
                # Best-effort inference from signed size if available
                _signed = float(ex_pos.get("size", 0) or 0)
                if _signed > 0:
                    ex_side = "LONG"
                elif _signed < 0:
                    ex_side = "SHORT"
                else:
                    logger.error(
                        f"🚨 emergency_flatten [{reason}]: cannot determine side "
                        f"from ex_pos={ex_pos} — refusing to send blind market order")
                    return None
            close_side = "SELL" if ex_side == "LONG" else "BUY"
            logger.critical(
                f"💀 EMERGENCY FLATTEN [{reason}] — closing {ex_side} "
                f"size={ex_size} via MARKET {close_side} reduce_only=True")
            result = self.place_market_order(
                side=close_side, quantity=ex_size, reduce_only=True)
            if result:
                logger.warning(
                    f"✅ Emergency flatten sent: order_id={result.get('order_id')} "
                    f"reason={reason}")
            else:
                logger.critical(
                    f"💀 Emergency flatten FAILED for {reason} — MANUAL INTERVENTION REQUIRED")
            return result
        except Exception as e:
            logger.error(f"emergency_flatten error: {e}", exc_info=True)
            return None

    def place_limit_order(self, side: str, quantity: float,
                          price: float, reduce_only: bool = False) -> Optional[Dict]:
        try:
            if not self._check_window_rate_limit():
                return None
            api_side = self._normalize_side(side)
            cur = "₹" if self._exchange_name == "icici" else "$"
            logger.info(f"LIMIT {side} qty={quantity} @ {cur}{price:,.2f}")
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
                logger.info(f"✅ Limit order: {data['order_id']} @ {cur}{price:,.2f}")
            return data
        except Exception as e:
            logger.error(f"place_limit_order error: {e}", exc_info=True)
            return None

    def place_limit_entry(self, side: str, quantity: float,
                          limit_price: float, timeout_sec: float = 25.0,
                          fallback_to_market: bool = True,
                          on_order_placed=None) -> Optional[Dict]:
        """
        Maker limit entry with adaptive polling and market fallback.

        on_order_placed: optional callback(order_id: str) invoked the moment
          the REST call returns a valid order_id. Used by the strategy
          watchdog to switch from Stage-A to Stage-B timing. Never raises.
        """
        cur = "₹" if self._exchange_name == "icici" else "$"
        logger.info(f"🎯 Maker entry: {side} {quantity} @ {cur}{limit_price:.2f} "
                    f"(timeout={timeout_sec:.0f}s)")

        data = self.place_limit_order(side=side, quantity=quantity,
                                      price=limit_price, reduce_only=False)
        if not data:
            if fallback_to_market:
                mdata = self.place_market_order(side=side, quantity=quantity)
                if mdata:
                    mdata["fill_type"] = "taker"
                    if float(mdata.get("fill_price", 0.0) or 0.0) <= 0:
                        mdata["fill_price"] = 0.0
                    if on_order_placed is not None:
                        try: on_order_placed(mdata.get("order_id", ""))
                        except Exception: pass
                return mdata
            return None

        order_id = data.get("order_id", "")
        if not order_id:
            return None

        # BUG 2 FIX: notify caller the order is now on the exchange
        if on_order_placed is not None:
            try:
                on_order_placed(order_id)
            except Exception as _cb_e:
                logger.warning(f"on_order_placed callback error (non-fatal): {_cb_e}")

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
                data["paid_commission"] = float(details.get("paid_commission", 0) or 0)
                data["paid_commission_exact"] = bool(details.get("paid_commission_exact", False))
                logger.info(f"✅ Maker fill: {order_id[:8]}… @ {cur}{fill_px:.2f}"
                            f" fee={cur}{data['paid_commission']:.4f}"
                            f" exact={data['paid_commission_exact']}")
                return data

            if status == "CANCELLED":
                logger.info(f"Limit {order_id[:8]}… cancelled by exchange — fallback")
                break

            if status == "PARTIAL_FILL":
                filled_qty = float(details.get("filled_qty") or 0)
                fill_px    = float(details.get("fill_price") or limit_price)
                logger.info(f"⚠️ Partial fill: {filled_qty:.4f} @ {cur}{fill_px:.2f}")
                self.cancel_order(order_id)
                data["fill_type"]  = "maker"
                data["fill_price"] = fill_px
                data["quantity"]   = filled_qty
                data["paid_commission"] = float(details.get("paid_commission", 0) or 0)
                data["paid_commission_exact"] = bool(details.get("paid_commission_exact", False))
                return data

        # Timeout
        cancel_result = self.cancel_order(order_id)
        if cancel_result == CancelResult.ALREADY_FILLED:
            details = self.get_fill_details(order_id)
            fill_px = float((details or {}).get("fill_price") or limit_price)
            data["fill_type"]  = "maker"
            data["fill_price"] = fill_px
            data["paid_commission"] = float((details or {}).get("paid_commission", 0) or 0)
            data["paid_commission_exact"] = bool((details or {}).get("paid_commission_exact", False))
            return data

        if fallback_to_market:
            logger.info("Limit timeout — falling back to market order")
            mdata = self.place_market_order(side=side, quantity=quantity)
            if mdata:
                mdata["fill_type"]  = "taker"
                if float(mdata.get("fill_price", 0.0) or 0.0) <= 0:
                    mdata["fill_price"] = 0.0
            return mdata
        logger.info(f"Limit order timeout after {timeout_sec:.0f}s — cancelled, no market fallback")
        return None

    def place_bracket_limit_entry(self, side: str, quantity: float,
                                  limit_price: float,
                                  sl_price: float,
                                  tp_price: float,
                                  timeout_sec: float = 45.0,
                                  on_order_placed=None) -> Optional[Dict]:
        """
        Protected entry for adapters that expose broker-native protection.
        Delta uses native bracket orders; ICICI uses documented NFO cover-OCO GTT.
        Polls until the protected entry is filled; unprotected fallbacks are forbidden
        by the strategy for desks that require broker-attached protection.

        Return dict keys on success:
          fill_price, fill_type, order_id, bracket_order=True,
          bracket_sl_order_id, bracket_tp_order_id,
          bracket_sl_price, bracket_tp_price

        on_order_placed: optional callback(order_id: str) invoked the moment
          the REST call returns a valid order_id — BEFORE the fill-poll loop
          begins.  Used by the strategy watchdog to switch from Stage-A
          (pre-order) to Stage-B (post-order) timing. Never raises.
        """
        self.last_order_error = None
        if not hasattr(self._adapter, "place_bracket_limit_entry"):
            return None  # Adapter does not expose protected entry routing.

        cur = self._currency_symbol()
        logger.info(
            f"[PROTECTED_ENTRY] {side.upper()} {quantity} @ {cur}{limit_price:.2f} "
            f"SL={cur}{sl_price:.2f} TP={cur}{tp_price:.2f} (timeout={timeout_sec:.0f}s)"
        )

        data = self._adapter.place_bracket_limit_entry(
            side=side, quantity=quantity,
            limit_price=limit_price, sl_price=sl_price, tp_price=tp_price,
        )
        if not data or data.get("_error"):
            sc = (data or {}).get("_sc", 0)
            raw = (data or {}).get("_raw", {})
            reason = self._compact_error(raw)
            self.last_order_error = {
                "stage": "icici_gtt_cover_oco_entry" if self._exchange_name == "icici" else "delta_native_bracket_entry",
                "status_code": sc,
                "reason": reason,
                "raw": raw,
            }
            logger.error(f"Bracket order failed: sc={sc} reason={reason} raw={raw}")
            return None

        order_id = data.get("order_id", "")
        if not order_id:
            return None
        logger.info(f"✅ Protected order placed: {order_id} @ {cur}{limit_price:.2f}")

        # BUG 2 FIX: notify the caller that the order is now on the exchange
        if on_order_placed is not None:
            try:
                on_order_placed(order_id)
            except Exception as _cb_e:
                logger.warning(f"on_order_placed callback error (non-fatal): {_cb_e}")

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
                # Propagate exact entry fee from Delta paid_commission
                data["paid_commission"] = float(details.get("paid_commission", 0) or 0)
                data["paid_commission_exact"] = bool(details.get("paid_commission_exact", False))
                logger.info(f"✅ Protected entry fill: {order_id[:8]}… @ {cur}{fill_px:.2f}"
                            f" fee={cur}{data['paid_commission']:.4f}"
                            f" exact={data['paid_commission_exact']}")

                if data.get("protection_model") == "ICICI_GTT_COVER_OCO":
                    # Breeze accepted the entry, target and stoploss as one official
                    # cover-OCO instruction. Child identity is the GTT leg identity;
                    # do not search the normal order book for Delta-style children.
                    data["bracket_child_verified"] = True
                    logger.info(
                        "✅ ICICI broker-protected GTT cover-OCO active: entry=%s SL-leg=%s TP-leg=%s",
                        data.get("order_id"), data.get("bracket_sl_order_id"), data.get("bracket_tp_order_id"),
                    )
                    return data

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

                expected_pid = None
                try:
                    expected_pid = self._adapter._get_product_id() if hasattr(self._adapter, "_get_product_id") else None
                except Exception:
                    expected_pid = None
                expected_symbol = str(getattr(self._adapter, "symbol", self.symbol) or self.symbol).upper()
                expected_exit_side = "BUY" if self._normalize_side(side) == "SELL" else "SELL"
                tick_tol = max(
                    float(self._active_tick_size() or 0.0) * float(getattr(config, "DELTA_BRACKET_CHILD_PRICE_TOL_TICKS", 6.0)),
                    1e-9,
                )
                pct_tol = float(getattr(config, "DELTA_BRACKET_CHILD_PRICE_TOL_PCT", 0.0025))

                def _raw_of(o):
                    return (o.get("raw") or o.get("_raw") or {}) if isinstance(o, dict) else {}

                def _order_pid(o):
                    raw = _raw_of(o)
                    prod = raw.get("product") if isinstance(raw.get("product"), dict) else {}
                    return (o.get("product_id") or raw.get("product_id") or prod.get("id"))

                def _order_symbol(o):
                    raw = _raw_of(o)
                    prod = raw.get("product") if isinstance(raw.get("product"), dict) else {}
                    return str(o.get("product_symbol") or raw.get("product_symbol") or raw.get("symbol") or prod.get("symbol") or "").upper()

                def _price_ok(actual: float, expected: float) -> bool:
                    if expected <= 0:
                        return False
                    if actual <= 0:
                        return True
                    return abs(actual - expected) <= max(tick_tol, abs(expected) * pct_tol)

                def _belongs_to_this_product(o) -> bool:
                    pid = _order_pid(o)
                    if expected_pid is not None and pid not in (None, ""):
                        try:
                            if int(pid) != int(expected_pid):
                                return False
                        except Exception:
                            return False
                    sym = _order_symbol(o)
                    if sym and expected_symbol and sym != expected_symbol:
                        return False
                    return True

                def _parse_children(open_ords):
                    """Return only SL/TP children that belong to this product and price plan.

                    Multi-asset safety invariant: never adopt open SL/TP orders from
                    another contract.  The pre-v13 code parsed the first STOP/TAKE
                    orders in the account; a COIN entry adopted BTC child orders near
                    $79k, leaving COIN effectively unprotected.
                    """
                    _sl_o = _tp_o = ""
                    _sl_t = _tp_t = 0.0
                    rejected = []
                    for o in (open_ords or []):
                        raw = _raw_of(o)
                        raw_type = str(
                            o.get("type") or
                            raw.get("order_type") or
                            raw.get("stop_order_type") or ""
                        )
                        ot = raw_type.upper().replace(" ", "_").replace("-", "_")
                        try:
                            trig = float(o.get("trigger_price") or raw.get("stop_price") or 0)
                        except Exception:
                            trig = 0.0
                        oid = str(o.get("order_id", "") or raw.get("id", ""))
                        side_o = str(o.get("side") or raw.get("side") or "").upper()
                        is_sl = (ot in _SL_TYPES or ("STOP" in ot and "PROFIT" not in ot and "TAKE" not in ot))
                        is_tp = (ot in _TP_TYPES or ("PROFIT" in ot or "TAKE_PROFIT" in ot))
                        if not (is_sl or is_tp) or not oid:
                            continue
                        if not _belongs_to_this_product(o):
                            rejected.append((oid[:8], ot, trig, "product_mismatch", _order_symbol(o), _order_pid(o)))
                            continue
                        if side_o and expected_exit_side and side_o != expected_exit_side:
                            rejected.append((oid[:8], ot, trig, "side_mismatch", side_o, expected_exit_side))
                            continue
                        if is_sl and not _price_ok(trig, sl_price):
                            rejected.append((oid[:8], ot, trig, "sl_price_mismatch", sl_price, expected_symbol))
                            continue
                        if is_tp and not _price_ok(trig, tp_price):
                            rejected.append((oid[:8], ot, trig, "tp_price_mismatch", tp_price, expected_symbol))
                            continue
                        if is_sl and not _sl_o:
                            _sl_o = oid; _sl_t = trig or float(sl_price)
                        elif is_tp and not _tp_o:
                            _tp_o = oid; _tp_t = trig or float(tp_price)
                    if rejected:
                        logger.warning(f"Bracket child reject audit: {rejected[:8]}")
                    return _sl_o, _sl_t, _tp_o, _tp_t

                verify_timeout = max(3.0, float(getattr(config, "DELTA_BRACKET_CHILD_VERIFY_TIMEOUT_SEC", 18.0)))
                verify_deadline = time.time() + verify_timeout
                attempt = 0
                while time.time() < verify_deadline and not (sl_oid and tp_oid):
                    time.sleep(1.5 if attempt < 2 else 2.5)
                    attempt += 1
                    open_ords = self.get_open_orders(symbol=expected_symbol)
                    raw_types = [
                        (str(o.get("order_id", "?"))[:8], o.get("type", "?"), o.get("product_symbol") or (_raw_of(o).get("product_symbol") or ""), o.get("trigger_price", 0))
                        for o in (open_ords or [])
                    ]
                    logger.info(f"Bracket child query ({attempt}) — open orders: {raw_types}")
                    sl_oid, sl_trig, tp_oid, tp_trig = _parse_children(open_ords)
                    if sl_oid and tp_oid:
                        logger.info(f"Bracket children verified on attempt {attempt}")
                        break

                data["bracket_child_verified"] = bool(sl_oid and tp_oid)
                data["bracket_sl_order_id"] = sl_oid
                data["bracket_tp_order_id"] = tp_oid
                data["bracket_sl_price"] = sl_trig
                data["bracket_tp_price"] = tp_trig
                if sl_oid:
                    logger.info(f"  Bracket SL order: {sl_oid} @ ${sl_trig:.2f}")
                if tp_oid:
                    logger.info(f"  Bracket TP order: {tp_oid} @ ${tp_trig:.2f}")
                if not (sl_oid and tp_oid):
                    data["_bracket_children_missing"] = True
                    data["_expected_sl_price"] = float(sl_price)
                    data["_expected_tp_price"] = float(tp_price)
                    data["_expected_product_id"] = expected_pid
                    data["_expected_symbol"] = expected_symbol
                    logger.error(
                        f"CRITICAL: Delta bracket fill {order_id[:8]}… on {expected_symbol} "
                        f"has no verified matching SL+TP children within {verify_timeout:.0f}s; "
                        f"expected SL=${sl_price:.2f} TP=${tp_price:.2f}. Strategy must flatten/alert."
                    )
                return data

            if status == "CANCELLED":
                logger.info(f"Bracket order {order_id[:8]}… cancelled by exchange")
                break

        # Timeout — cancel and signal caller to retry after cooldown.
        # This is not a native-bracket schema/API failure: the protected entry
        # order reached Delta, but the maker limit did not fill inside the
        # allowed window.  Preserve a structured reason so the strategy can log
        # it as a safe unfilled-entry abort instead of a false critical failure.
        try:
            cancel_resp = self.cancel_order(order_id) or {}
        except Exception as cancel_e:
            cancel_resp = {"cancel_error": str(cancel_e)}
        self.last_order_error = {
            "stage": "delta_native_bracket_fill_timeout",
            "status_code": 0,
            "reason": f"entry_limit_not_filled_within_{timeout_sec:.0f}s",
            "order_id": order_id,
            "timeout_sec": float(timeout_sec),
            "raw": {"cancel_response": cancel_resp},
        }
        logger.warning(
            f"Bracket entry {order_id[:8]}… not filled within {timeout_sec:.0f}s — "
            "cancelled safely; no position opened"
        )
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
            cur = self._currency_symbol()

            # Initialise limit_price to None; only assigned when use_limit=True.
            # BUG-UNBOUND-LIMIT-PRICE FIX: the original code never initialised
            # limit_price before the if/else block, causing UnboundLocalError in
            # _record_order when use_limit=False because the dict literal
            # `"limit_price": limit_price if use_limit else None` was evaluated
            # even on the False branch (Python evaluates the whole expression
            # before checking the condition in some older CPython versions).
            # Initialising to None is safe and makes the intent explicit.
            limit_price: Optional[float] = None

            # Limit price: gives execution buffer past the stop trigger.
            # SHORT SL (buy to close): limit = stop + offset (max price we'll pay)
            # LONG  SL (sell to close): limit = stop - offset (min price we'll accept)
            if use_limit:
                limit_price = self._sl_limit_price(api_side, trigger_price)
                limit_offset = abs(limit_price - trigger_price)
                logger.info(
                    f"SL-LIMIT {side} qty={quantity} stop={cur}{trigger_price:,.2f} "
                    f"limit={cur}{limit_price:,.2f} (±{limit_offset:.1f}pts offset)")
                data = self._place_with_retry(
                    side=api_side, order_type="limit_order",
                    quantity=quantity, trigger_price=trigger_price,
                    price=limit_price,
                    reduce_only=True, stop_order_type="stop_loss_order")
            else:
                # Stop-market: guaranteed fill, taker fee (for non-trailing / emergency)
                logger.info(f"SL-MARKET {side} qty={quantity} trigger={cur}{trigger_price:,.2f}")
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
                    f"@ stop={cur}{trigger_price:,.2f}"
                    + (f" limit={cur}{limit_price:,.2f}" if use_limit else ""))
            return data
        except Exception as e:
            logger.error(f"place_stop_loss error: {e}", exc_info=True)
            return None

    def place_take_profit(self, side: str, quantity: float,
                          trigger_price: float) -> Optional[Dict]:
        try:
            api_side = self._normalize_side(side)
            cur = self._currency_symbol()
            logger.info(f"TP {side} qty={quantity} trigger={cur}{trigger_price:,.2f}")
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
                logger.info(f"✅ TP: {data['order_id']} @ {cur}{trigger_price:,.2f}")
            return data
        except Exception as e:
            logger.error(f"place_take_profit error: {e}", exc_info=True)
            return None

    # ── Order replacement ─────────────────────────────────────────────────────

    def replace_stop_loss(self, existing_sl_order_id: Optional[str],
                          side: str, quantity: float,
                          new_trigger_price: float,
                          old_trigger_price: Optional[float] = None,
                          current_price: Optional[float] = None) -> Optional[Dict]:
        """
        Update trailing SL to new_trigger_price.

        Strategy (institutional invariant: NEVER leave position unprotected):
          1. EDIT-IN-PLACE (Delta) — PUT /v2/orders with id+product_id in body.
             Sends both stop_price AND limit_price together (stop-limit).
             Atomic: no cancel+replace cycle, zero unprotected window.

             404 handling: a 404 BY ITSELF is ambiguous — the order could have
             fired on the exchange (true exit) OR it could have been cancelled
             by a previous cancel+replace attempt of ours that then failed to
             place a new SL (the GHOST case that caused the third-trade bug).
             Disambiguate via _was_self_cancelled(): if we cancelled it, the
             caller must NOT treat this as "exit confirmed" — it is an
             UNPROTECTED state that must be emergency-flattened.

          2. CANCEL + REPLACE fallback — for CoinSwitch or irrecoverable edit
             failures. Places a stop-limit order.

             CRITICAL INVARIANT: if the cancel succeeds but the replace fails,
             we MUST attempt to restore an SL (at the original trigger, or at
             the new trigger with widened buffer, or at worst a plain stop-
             market). Returning "PLACE_FAILED" while leaving the position
             unprotected is the catastrophic path that blew up the third trade.

        Return contract:
          dict with "order_id" on success
          None — SL already filled (exit confirmed, caller records exit)
          {"error": "UNPROTECTED", ...} — SL is GONE and we could not restore.
             Caller MUST emergency-flatten the position immediately.
          {"error": "<other>"} — SL was NOT touched; current SL still live.
        """
        api_side = self._normalize_side(side)
        new_trigger_price = self._round_price_to_tick(new_trigger_price)
        invalid_reason = self._validate_stop_trigger(api_side, new_trigger_price, current_price)
        if invalid_reason:
            logger.warning("SL replace rejected before REST: %s", invalid_reason)
            return {"error": "INVALID_SL_PRICE", "reason": invalid_reason, "sl_cancelled": False}

        # Compute limit_price for the stop-limit (same logic as place_stop_loss)
        new_limit_price = self._sl_limit_price(api_side, new_trigger_price)

        try:
            # ── Path 1: Edit-in-place (Delta only) ───────────────────────────
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
                    # ── GHOST-404 disambiguation ──────────────────────────
                    # If we ourselves cancelled this id recently, the 404 is
                    # a ghost, not an exit — the position is UNPROTECTED.
                    if self._was_self_cancelled(existing_sl_order_id):
                        logger.error(
                            f"🚨 SL {existing_sl_order_id[:10]}… 404 is a GHOST "
                            f"(we cancelled it ourselves and replacement failed). "
                            f"Position is UNPROTECTED.")
                        return {"error": "UNPROTECTED",
                                "reason": "ghost_404_self_cancelled",
                                "sl_cancelled": True,
                                "order_id": existing_sl_order_id}
                    # True exit: order is gone and WE didn't cancel it.
                    logger.info(
                        f"SL {existing_sl_order_id[:10]}… gone (404) "
                        f"— not in self-cancelled set → exit confirmation required")
                    return None
                else:
                    logger.warning(
                        f"SL edit failed sc={sc} err={err} "
                        f"— keeping existing SL live; retry next tick"
                    )
                    return {
                        "error": "EDIT_FAILED_RETRY",
                        "reason": str(err)[:200],
                        "sl_cancelled": False,
                        "order_id": existing_sl_order_id,
                    }

            # ── Path 2: Cancel + Replace (CoinSwitch / edit failure fallback) ─
            cancelled_old = False
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
                    if result == CancelResult.NOT_FOUND:
                        logger.warning(
                            f"⚠️ SL cancel returned NOT_FOUND for {existing_sl_order_id} — "
                            f"aborting replace (old SL may still be live). Retry next tick.")
                        return {"error": "CANCEL_NOT_FOUND"}
                    # Cancel SUCCESS — record it so a later 404 can be identified
                    # as a ghost instead of misread as "exit confirmed".
                    self._mark_self_cancelled(existing_sl_order_id)
                    self._remove_active_order(existing_sl_order_id)
                    cancelled_old = True

            # Place stop-limit replacement (use_limit=True is default)
            new_sl = self.place_stop_loss(side=side, quantity=quantity,
                                          trigger_price=new_trigger_price,
                                          use_limit=True)
            if new_sl:
                logger.info(
                    f"✅ SL replaced (stop-limit) → {new_sl['order_id']} "
                    f"stop=${new_trigger_price:,.2f} limit=${new_limit_price:,.2f}")
                return new_sl

            # ── RESTORE PATH (institutional invariant) ─────────────────────────
            # If we cancelled the old SL and the new one failed, the position
            # is UNPROTECTED. We MUST restore coverage before returning.
            if cancelled_old:
                logger.error(
                    "🚨 SL replace failed AFTER cancel — position is UNPROTECTED. "
                    "Attempting emergency SL restore.")

                # Tier-1: replace at original trigger with plain stop-market
                # (no limit, no immediate-execution rejection).
                _restore_trigger = (float(old_trigger_price)
                                    if old_trigger_price is not None
                                    and old_trigger_price > 0
                                    else new_trigger_price)
                try:
                    restored = self.place_stop_loss(
                        side=side, quantity=quantity,
                        trigger_price=_restore_trigger,
                        use_limit=False,  # plain stop-market: widest acceptance
                    )
                    if restored:
                        logger.warning(
                            f"⚠️ SL RESTORED (stop-market) → {restored['order_id']} "
                            f"@ ${_restore_trigger:,.2f}. Original replace failed.")
                        # Signal partial success: SL exists but NOT at the
                        # requested trigger. Strategy should not assume the
                        # trail moved; it should re-try on next tick.
                        restored["_restore"] = True
                        restored["_restored_at"] = _restore_trigger
                        return {"error": "PLACE_FAILED_RESTORED",
                                "restore_order_id": restored["order_id"],
                                "restore_trigger": _restore_trigger}
                except Exception as _re:
                    logger.error(f"SL restore attempt raised: {_re}", exc_info=True)

                # Tier-2 (last resort): return UNPROTECTED so the strategy can
                # emergency-flatten the position at market.
                logger.critical(
                    "💀 SL REPLACE + RESTORE BOTH FAILED. "
                    "Position is UNPROTECTED — caller MUST flatten immediately.")
                return {"error": "UNPROTECTED",
                        "reason": "replace_and_restore_failed",
                        "sl_cancelled": True}

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

            # Determine if the cancel API call itself succeeded.
            # CoinSwitch: success = no "error" key in response body.
            # Delta:       success = resp["success"] == True.
            # Both can return {} on success (empty body = 200 OK).
            has_error   = bool(resp.get("error"))
            has_success = resp.get("success", None)
            sc          = resp.get("status_code", 0)

            if has_success is True:
                api_succeeded = True
            elif has_success is False:
                api_succeeded = False
            else:
                # No "success" key (CoinSwitch style) — no error = success
                api_succeeded = not has_error and sc not in (400, 401, 403, 404, 422)

            if sc == 404:
                self._remove_active_order(order_id)
                return CancelResult.NOT_FOUND

            if api_succeeded:
                # Cancel was accepted — verify final state for correctness.
                # Exchanges may have already filled the order before cancel landed.
                current = self.get_order_status_safe(order_id)
                if current == "FILLED":
                    return CancelResult.ALREADY_FILLED
                if current == "PARTIAL_FILL":
                    return CancelResult.PARTIAL_FILL
                # PENDING after a successful cancel request means the exchange
                # accepted but hasn't settled yet (eventual consistency) — treat
                # as SUCCESS; the position will be confirmed on next poll.
                # Record our cancellation so a later 404 query on this id is
                # correctly identified as a GHOST (not "exit fired").
                self._mark_self_cancelled(order_id)
                self._remove_active_order(order_id)
                return CancelResult.SUCCESS

            # Cancel API call failed — check if the order was already done
            current = self.get_order_status_safe(order_id)
            if current == "FILLED":
                return CancelResult.ALREADY_FILLED
            if current == "PARTIAL_FILL":
                return CancelResult.PARTIAL_FILL
            if current == "CANCELLED":
                self._mark_self_cancelled(order_id)
                self._remove_active_order(order_id)
                return CancelResult.SUCCESS
            if current == "UNKNOWN":
                # Order gone from exchange (404 on status query)
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
        sl_token = str(sl_order_id or "")
        tp_token = str(tp_order_id or "")
        # ICICI cover-OCO target and stoploss pseudo ids belong to the same
        # broker-side GTT plan. Cancel that plan exactly once; two independent
        # cancel requests create an avoidable race and burn API quota.
        same_gtt = (
            sl_token.startswith("GTT:") and tp_token.startswith("GTT:") and
            sl_token.split(":")[1:2] == tp_token.split(":")[1:2]
        )
        if same_gtt:
            tp_result = self.cancel_order(tp_token)
            sl_result = tp_result
            logger.info(f"ICICI GTT cover-OCO cancel: {tp_result.value}")
            return sl_result, tp_result
        if tp_order_id:
            tp_result = self.cancel_order(tp_order_id)
            logger.info(f"TP cancel: {tp_result.value}")
        if sl_order_id:
            sl_result = self.cancel_order(sl_order_id)
            logger.info(f"SL cancel: {sl_result.value}")
        return sl_result, tp_result

    # ── Open orders + conditional sweep ──────────────────────────────────────

    def get_open_orders(self, symbol: str = None) -> Optional[list]:
        # Reset 404 latch after 5 minutes — transient 404s should not permanently
        # disable open order queries for the entire session
        if self._open_orders_404:
            if not hasattr(self, '_open_orders_404_time'):
                self._open_orders_404_time = 0
            import time as _t
            if _t.time() - self._open_orders_404_time > 300:
                self._open_orders_404 = False
                logger.info("get_open_orders: 404 latch reset after 5 min cooldown")
            else:
                return None
        try:
            sym  = symbol or getattr(config, "SYMBOL", "BTCUSDT")
            raw  = self._adapter.get_open_orders(sym)
            if raw is None:
                return None
            # Detect 404 dict response (adapter returned error dict instead of raising)
            if isinstance(raw, dict) and (raw.get("status_code") == 404
                                          or "404" in str(raw.get("error", ""))):
                logger.warning("get_open_orders: 404 — suppressing calls for 5 min")
                self._open_orders_404 = True
                import time as _t; self._open_orders_404_time = _t.time()
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
                # Preserve product identity through the normalisation layer.
                # Bracket child reconciliation is product-strict in multi-asset mode.
                raw_inner = o.get("_raw") or o.get("raw") or {}
                prod_inner = raw_inner.get("product") if isinstance(raw_inner.get("product"), dict) else {}
                prod_id = o.get("product_id") or raw_inner.get("product_id") or prod_inner.get("id")
                prod_sym = str(o.get("product_symbol") or raw_inner.get("product_symbol") or raw_inner.get("symbol") or prod_inner.get("symbol") or "").upper()
                # Product-strict filtering for Delta multi-asset order books.
                # If identity fields are present and do not match this manager,
                # drop the order here so child resolution cannot borrow another
                # contract's SL/TP.  If identity fields are absent, keep it and
                # let the caller perform price/side checks as a secondary guard.
                try:
                    expected_pid = self._adapter._get_product_id() if hasattr(self._adapter, "_get_product_id") else None
                except Exception:
                    expected_pid = None
                expected_sym = str(getattr(self._adapter, "symbol", self.symbol) or self.symbol).upper()
                if prod_id not in (None, "") and expected_pid is not None:
                    try:
                        if int(prod_id) != int(expected_pid):
                            continue
                    except Exception:
                        continue
                if prod_sym and expected_sym and prod_sym != expected_sym:
                    continue
                if oid:
                    result.append({"order_id": oid, "type": otype, "side": side,
                                   "quantity": qty, "trigger_price": trig,
                                   "price": px, "status": status,
                                   "product_id": prod_id, "product_symbol": prod_sym,
                                   "raw": o})
            return result
        except Exception as e:
            err_str = str(e)
            if "404" in err_str or "Not Found" in err_str:
                logger.warning("get_open_orders: 404 Not Found — suppressing calls for 5 min")
                self._open_orders_404 = True
                import time as _t; self._open_orders_404_time = _t.time()
            else:
                logger.error(f"get_open_orders error: {e}", exc_info=True)
            return None

    def cancel_symbol_conditionals(self, symbol: str = None) -> Dict[str, CancelResult]:
        sym    = symbol or self._adapter.symbol
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

    # ── Exit order identification (all supported venues) ───────────────────

    def identify_exit_order(
        self,
        sl_order_id: Optional[str],
        tp_order_id: Optional[str],
        trail_active: bool = False,
    ) -> Dict:
        """Resolve a closing execution by its tracked order ids only.

        Delta, CoinSwitch and ICICI all route through ``get_fill_details``.
        A broker-flat position is not sufficient evidence for realised P&L: an
        exit is confirmed only when a known closing order is filled and exposes
        an execution price.  This prevents zero/mark-price P&L from polluting
        portfolio drawdown controls and post-trade learning.
        """
        unconfirmed = {
            "confirmed": False, "exit_type": "unknown", "fill_price": 0.0,
            "order_id": "", "fee_paid": 0.0, "fee_exact": False,
        }
        known = [
            (str(sl_order_id or "").strip(), "trail_sl" if trail_active else "sl"),
            (str(tp_order_id or "").strip(), "tp"),
        ]
        known = [(oid, kind) for oid, kind in known if oid]
        if not known:
            return unconfirmed
        for oid, exit_type in known:
            try:
                details = self.get_fill_details(oid)
            except Exception as exc:
                logger.debug("identify_exit_order %s query error: %s", exit_type, exc)
                details = None
            if not isinstance(details, dict):
                continue
            status = str(details.get("status", "")).upper()
            if status not in {"FILLED", "CLOSED"}:
                continue
            fill_price = float(details.get("fill_price", 0.0) or 0.0)
            if fill_price <= 0:
                logger.warning(
                    "Exit order %s is filled but broker execution price is unavailable; "
                    "leaving P&L reconciliation pending", oid[:12])
                continue
            fee_paid = float(details.get("paid_commission", 0.0) or 0.0)
            fee_exact = bool(details.get("paid_commission_exact", False))
            disp = (oid[:10] + "…") if len(oid) > 10 else oid
            cur = self._currency_symbol()
            logger.info(
                "🔍 Exit confirmed: %s order=%s fill=%s%0.2f fee=%s%0.4f fee_exact=%s",
                exit_type.upper(), disp, cur, fill_price, cur, fee_paid, fee_exact)
            return {
                "confirmed": True, "exit_type": exit_type, "fill_price": fill_price,
                "order_id": oid, "fee_paid": fee_paid, "fee_exact": fee_exact,
            }
        return unconfirmed

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

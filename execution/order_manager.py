"""
execution/order_manager.py — Exchange-Agnostic Order Manager
=============================================================
Single OrderManager class that works with exchange API adapters via
constructor injection.

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


def _cfg(name: str, default: Any) -> Any:
    return getattr(config, name, default)


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
_GROWW_LIMITER = _RateLimiter(min_interval_sec=float(getattr(config, "GROWW_MIN_CALL_GAP_SEC", 0.25)))
_HL_LIMITER    = _RateLimiter(min_interval_sec=float(getattr(config, "HYPERLIQUID_MIN_CALL_GAP_SEC", 0.25)))

# Also keep a module-level alias for compatibility imports (quant_strategy does
# `from execution.order_manager import GlobalRateLimiter`)
class GlobalRateLimiter:
    """Compatibility shim — routes to the active exchange limiter."""
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
        data = resp.get("data", {})
        if isinstance(data, dict):
            raw = data.get("orders", [])
        else:
            raw = data
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

    def place_bracket_limit_entry(
        self, side: str, quantity: float, limit_price: float, sl_price: float, tp_price: float,
        timeout_sec: float = 45.0, on_order_placed=None,
    ) -> Optional[Dict]:
        """CoinSwitch protected lifecycle using its documented futures contract.

        CoinSwitch TP/SL orders are position-level.  A LIMIT entry is allowed to
        fill first, then both STOP_MARKET and TAKE_PROFIT_MARKET reduce-only
        orders are armed with quantity=0 as required by the API.  No naked
        fallback entry is ever submitted.
        """
        qty = float(quantity or 0.0)
        if qty <= 0:
            return {"_error": True, "_sc": 0, "_raw": {"error": "coinswitch_qty_invalid"}}
        entry_side = "BUY" if str(side or "").upper() in {"BUY", "LONG"} else "SELL"
        entry = self.place_order(entry_side, "LIMIT", qty, price=float(limit_price), reduce_only=False)
        if not isinstance(entry, dict) or entry.get("_error"):
            return entry or {"_error": True, "_sc": 0, "_raw": {"error": "coinswitch_entry_rejected"}}
        oid = str(entry.get("order_id") or "")
        if not oid:
            return {"_error": True, "_sc": 200, "_raw": entry, "_err_msg": "missing_entry_order_id"}
        if on_order_placed is not None:
            try:
                on_order_placed(oid)
            except Exception:
                pass

        deadline = time.time() + max(1.0, float(timeout_sec or _cfg("COINSWITCH_ENTRY_FILL_TIMEOUT_SEC", 45.0)))
        poll = max(0.25, float(_cfg("COINSWITCH_ENTRY_POLL_SEC", 1.0)))
        fill_price = float(self.extract_fill_price(entry) or limit_price)
        filled_qty = float(self.extract_filled_qty(entry) or 0.0)
        status = self.extract_status(entry)
        while status == "PENDING" and time.time() < deadline:
            time.sleep(poll)
            state = self.get_order(oid) or {}
            status = self.extract_status(state)
            fill_price = float(self.extract_fill_price(state) or fill_price)
            filled_qty = float(self.extract_filled_qty(state) or filled_qty)
        # Official CoinSwitch futures lifecycle documents PARTIALLY_EXECUTED as
        # terminal: protect the actual filled position, never the requested size.
        if status not in {"FILLED", "PARTIAL_FILL"} or filled_qty <= 0:
            try:
                self.cancel_order(oid)
            except Exception:
                pass
            return {"_error": True, "_sc": 0, "_raw": {"error": "coinswitch_entry_not_filled_before_timeout", "order_id": oid, "status": status}}

        exit_side = "SELL" if entry_side == "BUY" else "BUY"
        # API contract: TP/SL quantity is exactly zero because protection applies
        # to the complete open symbol position; reduce_only must be true.
        sl = self.place_order(exit_side, "STOP_MARKET", 0.0, trigger_price=float(sl_price), reduce_only=True)
        tp = self.place_order(exit_side, "TAKE_PROFIT_MARKET", 0.0, trigger_price=float(tp_price), reduce_only=True)
        sl_oid = str((sl or {}).get("order_id") or "") if isinstance(sl, dict) else ""
        tp_oid = str((tp or {}).get("order_id") or "") if isinstance(tp, dict) else ""
        protection_statuses: dict[str, str] = {}
        protection_confirmed = bool(sl_oid and tp_oid)
        if protection_confirmed:
            # CoinSwitch order placement returning an ID only acknowledges the
            # request. Confirm both position-level trigger orders via the
            # documented Get Order Status contract before accepting protection.
            sl_state = self.get_order(sl_oid) or {}
            tp_state = self.get_order(tp_oid) or {}
            protection_statuses = {
                "stop": self.extract_status(sl_state),
                "target": self.extract_status(tp_state),
            }
            protection_confirmed = all(
                status in {"PENDING", "FILLED"} for status in protection_statuses.values()
            )
        if not protection_confirmed:
            for child in (sl_oid, tp_oid):
                if child:
                    try:
                        self.cancel_order(child)
                    except Exception:
                        pass
            emergency = None
            if bool(_cfg("COINSWITCH_EMERGENCY_CLOSE_ON_PROTECTION_FAILURE", True)):
                # Official position lifecycle requires reduce_only for safe
                # counter-side closure so an emergency action cannot flip exposure.
                emergency = self.place_order(exit_side, "MARKET", filled_qty, reduce_only=True)
            return {
                "_error": True, "_sc": 200,
                "_raw": {"entry_order_id": oid, "stop_response": sl, "target_response": tp,
                         "protection_statuses": protection_statuses, "emergency_close": emergency},
                "_err_msg": "coinswitch_protection_not_confirmed_after_fill",
            }
        return {
            "order_id": oid, "status": "FILLED", "quantity": filled_qty,
            "fill_type": "limit", "fill_price": fill_price, "price": fill_price,
            "bracket_order": True, "bracket_child_verified": True,
            "bracket_sl_order_id": sl_oid, "bracket_tp_order_id": tp_oid,
            "bracket_sl_price": float(sl_price), "bracket_tp_price": float(tp_price),
            "protection_model": "COINSWITCH_POSITION_TPSL_AFTER_FILL",
            "protection_confirmed": True, "protection_statuses": protection_statuses,
            "paid_commission": 0.0, "paid_commission_exact": False,
        }

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
        result["quantity"] = float(quantity)
        result["size_btc"] = float(quantity)
        result["size_contracts"] = int(contracts)
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

class _GrowwBaseAdapter:
    """Long-premium GROWW options adapter.

    Opening orders are always buy-to-open limit orders. Exits are sell-to-close
    limit or official Groww stoploss orders. No market orders, no option writing,
    no leverage. Portfolio state is created only from exact NFO option rows.
    """

    def __init__(self, api, exchange_instrument=None) -> None:
        self.api = api
        self.limiter = _GROWW_LIMITER
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
        # Groww PortfolioPositions officially returns `quantity`; aliases are
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
        # Groww response variants may omit `segment`; never invent a position,
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
            "GROWW F&O position filter ignored %d non-executable broker row(s) [%s]; "
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
            out["reason"] = "short_groww_option_outside_long_premium_policy"
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
            raise RuntimeError("GROWW options guard: market entries are disabled; use limit orders")
        raw = self._active_raw()
        if not self._has_contract_identity(raw):
            raise RuntimeError("GROWW options guard: exact NFO option identity is required before routing an order")
        exchange_code = str(raw.get("exchange_code") or "NFO").upper()
        if exchange_code != "NFO":
            raise RuntimeError(f"GROWW options guard: expected NFO option contract, received exchange={exchange_code}")
        action = "sell" if reduce_only else "buy"
        px = price if price is not None else trigger_price
        if (px is None or float(px or 0.0) <= 0) and reduce_only:
            px = raw.get("selected_entry_premium") or raw.get("ltp") or raw.get("last_price") or raw.get("close")
        if px is None or float(px or 0.0) <= 0:
            raise RuntimeError("GROWW options guard: executable limit price is required")
        lot_raw = float(self._lot_size() or 0.0)
        if lot_raw <= 0:
            raise RuntimeError("GROWW options guard: verified NFO option lot size is required before routing an order")
        lot = int(round(lot_raw))
        if lot <= 0 or abs(lot_raw - lot) > 1e-9:
            raise RuntimeError(f"GROWW options guard: invalid NFO option lot size={lot_raw!r}")
        requested = float(quantity or 0.0)
        lots = int(math.floor((requested / lot) + 1e-9))
        if lots < 1:
            raise RuntimeError(f"GROWW options guard: requested quantity={requested:g} does not fit one lot={lot}")
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
                raise RuntimeError("GROWW options guard: stoploss order requires a positive trigger price")
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
        _ = (side, quantity, limit_price, sl_price, tp_price)
        return {
            "_error": True,
            "_sc": 0,
            "_raw": {
                "error": (
                    "GROWW_ADAPTER_BRACKET_ENTRY_DISABLED_USE_FILL_FIRST_OCO_LIFECYCLE"
                )
            },
        }

    def _normal_order_row(self, order_id: str) -> Optional[Dict[str, Any]]:
        """Read a normal Groww order."""
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
        try:
            return self.api.cancel_order(order_id=token, exchange_code=str(raw.get("exchange_code") or "NFO").upper()) or {}
        except TypeError:
            return self.api.cancel_order(order_id=token) or {}

    def get_order(self, order_id: str) -> Optional[Dict]:
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
        """Resolve an NFO close from exact Groww order/trade records when exposed."""
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
        if callable(trade_getter) and status in {"FILLED", "PARTIAL_FILL"}:
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
                logger.debug("GROWW get_trade_detail execution resolution unavailable for %s: %s", oid, exc)
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
            logger.warning("GROWW NFO open-order recovery unavailable: %s", exc)
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
                logger.info("GROWW NFO PortfolioPositions verified flat: %s", exc)
                return {"Success": [], "Status": 200, "Error": None, "_empty_positions": True}
            logger.error("GROWW NFO PortfolioPositions fetch failed: %s", exc)
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
                "GROWW exact NFO option position exists but does not match selected vehicle: "
                "symbol=%s right=%s strike=%s expiry=%s qty=%.8g",
                first.get("TradingSymbol") or first.get("stock_code") or "-",
                first.get("right") or "-", first.get("strike_price") or "-",
                first.get("expiry_date") or "-", float(first.get("size", 0.0) or 0.0),
            )
            return first

        if len(normalised) == 1:
            position = normalised[0]
            logger.warning(
                "GROWW exact NFO option position detected before a selected option vehicle exists: "
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
        logger.critical("GROWW multiple exact NFO option positions found; refusing automatic adoption")
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
        logger.info("GROWW F&O balance source=%s available=%.2f allocated=%.2f blocked=%.2f nfo_cash_limit=%.2f unallocated=%.2f", out["source"], out["available"], out["fno_allocated"], out["fno_blocked"], out["nfo_cash_limit"], out["unallocated_balance"])
        return out

    def set_leverage(self, leverage: int, product_id: Optional[int] = None) -> Dict:
        return {"success": True, "leverage": 1, "message": "GROWW long-premium options are fully funded; leverage is not applicable"}


class _GrowwAdapter(_GrowwBaseAdapter):
    """Long-premium Groww F&O options adapter using official SDK fields."""

    def __init__(self, api, exchange_instrument=None) -> None:
        super().__init__(api, exchange_instrument=exchange_instrument)
        self.limiter = _GROWW_LIMITER

    def _instrument_for_symbol(self, trading_symbol: str) -> Dict[str, Any]:
        symbol = str(trading_symbol or "").strip().upper()
        if not symbol or not hasattr(self.api, "get_all_instruments"):
            return {}
        try:
            for row in self.api.get_all_instruments():
                if str(row.get("trading_symbol") or "").strip().upper() == symbol:
                    return dict(row)
        except Exception:
            return {}
        return {}

    def _enriched_contract_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(row or {})
        symbol = str(out.get("trading_symbol") or out.get("TradingSymbol") or out.get("symbol") or "").strip()
        # The selected contract already comes from discovery with exact identity and lot size.
        # Do not perform a synchronous security-master download on the execution path.
        has_lot = any(self._num(out.get(key), 0.0) > 0 for key in ("runtime_lot_size", "LotSize", "lot_size", "MinimumLotQty"))
        if symbol and has_lot and super()._has_contract_identity(out):
            return out
        master = self._instrument_for_symbol(symbol)
        if master:
            out.setdefault("stock_code", str(master.get("underlying_symbol") or "").upper())
            out.setdefault("exchange_code", "NFO")
            out.setdefault("exchange", str(master.get("exchange") or "NSE").upper())
            out.setdefault("segment", str(master.get("segment") or "FNO").upper())
            out.setdefault("product_type", "Options")
            out.setdefault("expiry_date", master.get("expiry_date"))
            out.setdefault("strike_price", master.get("strike_price"))
            inst_type = str(master.get("instrument_type") or "").upper()
            out.setdefault("right", "Call" if inst_type == "CE" else "Put" if inst_type == "PE" else inst_type)
            out.setdefault("TradingSymbol", symbol)
            out.setdefault("runtime_lot_size", master.get("lot_size"))
            out.setdefault("LotSize", master.get("lot_size"))
        return out

    def _active_raw(self) -> Dict[str, Any]:
        return self._enriched_contract_row(super()._active_raw())

    def _has_contract_identity(self, raw: Dict[str, Any]) -> bool:
        return super()._has_contract_identity(self._enriched_contract_row(raw))

    def _contract_key(self, raw: Dict[str, Any]) -> tuple:
        return super()._contract_key(self._enriched_contract_row(raw))

    def _is_exact_nfo_option_position(self, row: Dict[str, Any]) -> tuple[bool, str]:
        row = self._enriched_contract_row(row)
        if not isinstance(row, dict):
            return False, "non_mapping_row"
        segment = str(row.get("segment") or "").strip().lower()
        exchange = str(row.get("exchange") or row.get("exchange_code") or "").strip().upper()
        product = str(row.get("product_type") or row.get("product") or "").strip().lower()
        if segment and segment != "fno":
            return False, "explicit_non_fno_segment"
        if exchange and exchange not in {"NSE", "NFO"}:
            return False, "not_groww_fno_exchange"
        if product and product not in {"option", "options", "nrml", "mis"}:
            return False, "not_options_product"
        if not self._has_contract_identity(row):
            return False, "missing_exact_contract_identity"
        target = self._target_stock_code()
        stock = str(row.get("stock_code") or row.get("underlying_symbol") or "").strip().upper()
        if target and stock and stock != target:
            return False, "different_underlying"
        if target and not stock:
            return False, "missing_underlying"
        return True, ""

    def _is_exact_nfo_option_order(self, row: Dict[str, Any]) -> tuple[bool, str]:
        return self._is_exact_nfo_option_position(row)

    def _order_body(self, side: str, order_type: str, quantity: float,
                    price=None, trigger_price=None, reduce_only: bool = False,
                    **kwargs) -> Dict:
        order_type_u = str(order_type or "").upper()
        stop_order_type = str(kwargs.get("stop_order_type") or "").lower()
        if "MARKET" in order_type_u and not reduce_only:
            raise RuntimeError("Groww options guard: market entries are disabled; use limit or protected smart orders")
        raw = self._active_raw()
        if not self._has_contract_identity(raw):
            raise RuntimeError("Groww options guard: exact FNO option identity is required before routing an order")
        trading_symbol = str(raw.get("trading_symbol") or raw.get("TradingSymbol") or "").strip()
        if not trading_symbol and hasattr(self.api, "_option_symbol_from_route"):
            trading_symbol = self.api._option_symbol_from_route(raw)
        if not trading_symbol:
            raise RuntimeError("Groww options guard: trading_symbol is required by the official SDK")
        px = price if price is not None else trigger_price
        if (px is None or float(px or 0.0) <= 0) and reduce_only:
            px = raw.get("selected_entry_premium") or raw.get("ltp") or raw.get("last_price") or raw.get("close")
        is_stop = order_type_u.startswith("STOP") or stop_order_type == "stop_loss_order"
        is_market_exit = "MARKET" in order_type_u and reduce_only and not is_stop
        if not is_market_exit and (px is None or float(px or 0.0) <= 0):
            raise RuntimeError("Groww options guard: executable price is required")
        lot_raw = float(self._lot_size() or 0.0)
        if lot_raw <= 0:
            raise RuntimeError("Groww options guard: verified FNO option lot size is required before routing an order")
        lot = int(round(lot_raw))
        if lot <= 0 or abs(lot_raw - lot) > 1e-9:
            raise RuntimeError(f"Groww options guard: invalid FNO option lot size={lot_raw!r}")
        requested = float(quantity or 0.0)
        lots = int(math.floor((requested / lot) + 1e-9))
        if lots < 1:
            raise RuntimeError(f"Groww options guard: requested quantity={requested:g} does not fit one lot={lot}")
        qty = int(lots * lot)
        if is_stop:
            sdk_order_type = self.api.const("ORDER_TYPE_STOP_LOSS", "SL") if px else self.api.const("ORDER_TYPE_STOP_LOSS_MARKET", "SL_M")
        elif is_market_exit:
            sdk_order_type = self.api.const("ORDER_TYPE_MARKET", "MARKET")
        else:
            sdk_order_type = self.api.const("ORDER_TYPE_LIMIT", "LIMIT")
        body = {
            "trading_symbol": trading_symbol,
            "quantity": qty,
            "validity": self.api.const("VALIDITY_DAY", "DAY"),
            "exchange": self.api.const("EXCHANGE_NSE", "NSE"),
            "segment": self.api.const("SEGMENT_FNO", "FNO"),
            "product": str(getattr(config, "GROWW_OPTION_PRODUCT_TYPE", "NRML") or "NRML").upper(),
            "order_type": sdk_order_type,
            "transaction_type": self.api.const("TRANSACTION_TYPE_SELL", "SELL") if reduce_only else self.api.const("TRANSACTION_TYPE_BUY", "BUY"),
            "price": str(px) if px is not None and float(px or 0.0) > 0 and sdk_order_type != self.api.const("ORDER_TYPE_MARKET", "MARKET") else None,
            "trigger_price": str(trigger_price) if is_stop and trigger_price is not None and float(trigger_price or 0.0) > 0 else None,
            "order_reference_id": getattr(self.api, "reference_id", lambda prefix="instv2": f"instv2{int(time.time())}")(str(getattr(config, "GROWW_SEBI_STRATEGY_PREFIX", "instv2"))),
        }
        return {k: v for k, v in body.items() if v not in (None, "")}

    def extract_order_id(self, resp: Dict) -> Optional[str]:
        if not isinstance(resp, dict):
            return None
        data = resp.get("Success") or resp.get("success") or resp.get("data") or resp.get("result") or resp
        if isinstance(data, list) and data:
            data = data[0]
        if isinstance(data, dict):
            oid = data.get("groww_order_id") or data.get("order_id") or data.get("id")
            return str(oid) if oid else None
        if isinstance(data, str) and data.strip():
            return data.strip()
        return None

    def extract_status(self, order_data: Dict) -> str:
        raw = str(order_data.get("order_status") or order_data.get("status") or order_data.get("smart_order_status") or "").upper()
        if raw in {"EXECUTED", "FILLED", "COMPLETE", "COMPLETED"}:
            return "FILLED"
        if raw in {"CANCELLED", "CANCELED", "REJECTED", "EXPIRED", "FAILED"}:
            return "CANCELLED"
        if raw in {"PARTIALLY_FILLED", "PARTIAL"}:
            return "PARTIAL_FILL"
        if raw in {"OPEN", "PENDING", "ACTIVE", "TRIGGER_PENDING", "TRIGGERED", "PLACED"}:
            return "PENDING"
        return "PENDING" if raw else "UNKNOWN"

    def extract_fill_price(self, order_data: Dict) -> Optional[float]:
        for f in ("average_fill_price", "average_price", "avg_price", "price", "execution_price", "ltp"):
            p = self._num(order_data.get(f), 0.0)
            if p > 0:
                return p
        order = order_data.get("order") if isinstance(order_data.get("order"), dict) else {}
        p = self._num(order.get("price"), 0.0)
        return p if p > 0 else None

    def extract_filled_qty(self, order_data: Dict) -> float:
        for f in ("filled_quantity", "executed_quantity", "quantity"):
            q = self._num(order_data.get(f), 0.0)
            if q > 0:
                return q
        return 0.0

    def place_bracket_limit_entry(self, side: str, quantity: float, limit_price: float, sl_price: float, tp_price: float) -> Optional[Dict]:
        _ = (side, quantity, limit_price, sl_price, tp_price)
        return {
            "_error": True,
            "_sc": 0,
            "_raw": {
                "error": (
                    "GROWW_ADAPTER_BRACKET_ENTRY_DISABLED_USE_FILL_FIRST_OCO_LIFECYCLE"
                )
            },
        }

    def cancel_order(self, order_id: str) -> Dict:
        self.limiter.wait()
        token = str(order_id or "")
        return self.api.cancel_order(groww_order_id=token, segment=self.api.const("SEGMENT_FNO", "FNO")) or {}

    def get_order(self, order_id: str) -> Optional[Dict]:
        getter = getattr(self.api, "get_order_detail", None) or getattr(self.api, "get_order", None)
        if not callable(getter):
            return {"order_id": str(order_id), "status": "PENDING"}
        try:
            self.limiter.wait()
            segment = self.api.const("SEGMENT_FNO", "FNO") if hasattr(self.api, "const") else "FNO"
            data = getter(groww_order_id=str(order_id), segment=segment)
            return data if isinstance(data, dict) else {"order_id": str(order_id), "status": "PENDING"}
        except TypeError:
            try:
                data = getter(order_id=str(order_id), exchange_code="NFO")
                payload = self._success_payload(data)
                if payload:
                    payload.setdefault("order_id", str(order_id))
                    return payload
                return data if isinstance(data, dict) else {"order_id": str(order_id), "status": "PENDING"}
            except TypeError:
                try:
                    data = getter(str(order_id))
                    payload = self._success_payload(data)
                    if payload:
                        payload.setdefault("order_id", str(order_id))
                        return payload
                    return data if isinstance(data, dict) else {"order_id": str(order_id), "status": "PENDING"}
                except Exception:
                    return {"order_id": str(order_id), "status": "PENDING"}
            except Exception:
                return {"order_id": str(order_id), "status": "PENDING"}
        except Exception:
            return {"order_id": str(order_id), "status": "PENDING"}

    def resolve_order_execution(self, order_id: str) -> Optional[Dict]:
        oid = str(order_id or "").strip()
        if not oid:
            return None
        raw_order = self.get_order(oid)
        if not isinstance(raw_order, dict):
            return None
        status = self.extract_status(raw_order)
        fill_price = float(self.extract_fill_price(raw_order) or 0.0)
        filled_qty = float(self.extract_filled_qty(raw_order) or 0.0)
        trade_getter = getattr(self.api, "get_trade_detail", None)
        fee_paid, fee_exact = self._extract_paid_commission(raw_order)
        if callable(trade_getter) and status in {"FILLED", "PARTIAL_FILL"}:
            try:
                self.limiter.wait()
                try:
                    segment = self.api.const("SEGMENT_FNO", "FNO") if hasattr(self.api, "const") else "FNO"
                    resp = trade_getter(order_id=oid, segment=segment)
                except TypeError:
                    resp = trade_getter(order_id=oid, exchange_code="NFO")
                rows = resp.get("trade_list") or resp.get("trades") or resp.get("data") or resp.get("Success") or [] if isinstance(resp, dict) else []
                num = den = 0.0
                exact_fees = 0.0
                exact_fee_seen = False
                for row in rows if isinstance(rows, list) else []:
                    if not isinstance(row, dict):
                        continue
                    px = self._num(row.get("price") or row.get("average_price") or row.get("trade_price"), 0.0)
                    qty = abs(self._num(row.get("quantity") or row.get("filled_quantity") or row.get("traded_quantity"), 0.0))
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
                logger.debug("Groww trade-list execution resolution unavailable for %s: %s", oid, exc)
        return {
            "status": status,
            "fill_price": fill_price,
            "filled_qty": filled_qty,
            "paid_commission": float(fee_paid or 0.0),
            "paid_commission_exact": bool(fee_exact),
            "raw_order": raw_order,
        }

    def get_open_orders(self, symbol: str) -> Optional[list]:
        try:
            self.limiter.wait()
            resp = self.api.get_order_list(segment=self.api.const("SEGMENT_FNO", "FNO"), page=0, page_size=25)
        except Exception as exc:
            logger.warning("Groww FNO open-order recovery unavailable: %s", exc)
            return []
        rows = resp.get("order_list") or resp.get("orders") or resp.get("data") or [] if isinstance(resp, dict) else []
        rows = rows if isinstance(rows, list) else []
        active = self._active_raw()
        active_key = self._contract_key(active) if self._has_contract_identity(active) else None
        open_orders = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            row = self._enriched_contract_row(row)
            ok, _ = self._is_exact_nfo_option_order(row)
            if not ok or (active_key and self._contract_key(row) != active_key):
                continue
            status = str(row.get("order_status") or row.get("status") or "").strip().upper()
            if status in {"EXECUTED", "FILLED", "COMPLETE", "COMPLETED", "CANCELLED", "CANCELED", "REJECTED", "EXPIRED", "FAILED"}:
                continue
            order_type = str(row.get("order_type") or "").strip().upper()
            trigger = self._num(row.get("trigger_price"), 0.0)
            oid = row.get("groww_order_id") or row.get("order_id")
            if oid:
                typ = "STOP_LOSS" if order_type in {"SL", "SL_M", "STOP_LOSS", "STOP_LOSS_MARKET"} else "LIMIT"
                open_orders.append({"order_id": str(oid), "type": typ, "trigger_price": trigger, "raw": row})
        return open_orders

    def get_positions(self, symbol: str) -> Optional[Dict]:
        try:
            return self.api.get_portfolio_positions()
        except Exception as exc:
            logger.error("Groww FNO positions fetch failed: %s", exc)
            return None

    def _normalised_position_row(self, row: Dict[str, Any], *, source: str = "matched") -> Dict[str, Any]:
        return super()._normalised_position_row(self._enriched_contract_row(row), source=source)

    def get_balance(self) -> Dict:
        resp = self.api.get_margin(exchange_code="NFO")
        data = self._success_payload(resp)
        fno = data.get("fno_margin_details") if isinstance(data.get("fno_margin_details"), dict) else {}
        available = self._num(fno.get("option_buy_balance_available") or data.get("cash_limit") or data.get("clear_cash"), 0.0)
        used = max(0.0, self._num(fno.get("net_fno_margin_used") or data.get("block_by_trade"), 0.0))
        total = max(available + used, self._num(data.get("amount_allocated") or data.get("cash_limit"), 0.0))
        out = {
            "available": max(0.0, available),
            "available_raw": max(0.0, available),
            "locked": used,
            "total": total,
            "currency": "INR",
            "segment": "FNO",
            "source": "groww.option_buy_balance_available",
            "fno_available": max(0.0, available),
            "fno_blocked": used,
            "raw": resp,
        }
        logger.info("Groww F&O balance source=%s available=%.2f blocked=%.2f", out["source"], out["available"], out["locked"])
        return out

    def set_leverage(self, leverage: int, product_id: Optional[int] = None) -> Dict:
        return {"success": True, "leverage": 1, "message": "Groww long-premium options are fully funded; leverage is not applicable"}


class _HyperliquidAdapter:
    """Normalises Hyperliquid SDK responses to canonical dicts."""

    def __init__(self, api, exchange_instrument=None) -> None:
        self.api = api
        self.limiter = _HL_LIMITER
        self.exchange_instrument = exchange_instrument
        self.symbol = (exchange_instrument.symbol if exchange_instrument is not None else "BTC")
        self.display_symbol = (exchange_instrument.display_symbol if exchange_instrument is not None else self.symbol)
        self.tick_size = float(getattr(exchange_instrument, "tick_size", 0.0) or 0.01) if exchange_instrument is not None else 0.01
        self.lot_step = float(getattr(exchange_instrument, "lot_step", 0.0) or 0.00001) if exchange_instrument is not None else 0.00001
        self.min_qty = float(getattr(exchange_instrument, "min_qty", 0.0) or 0.0) if exchange_instrument is not None else 0.0
        self.max_qty = float(getattr(exchange_instrument, "max_qty", 0.0) or 0.0) if exchange_instrument is not None else 0.0

    @staticmethod
    def _num(value, default: float = 0.0) -> float:
        try:
            if value is None or value == "":
                return float(default)
            out = float(value)
            return out if math.isfinite(out) else float(default)
        except Exception:
            return float(default)

    def _round_qty(self, quantity: float) -> float:
        try:
            q = float(self.api.round_size(self.symbol, float(quantity)))
        except Exception:
            step = max(float(self.lot_step or 0.0), 1e-12)
            q = math.floor(float(quantity) / step) * step
        return max(0.0, q)

    def _round_px(self, price: float) -> float:
        """Apply Hyperliquid's significant-figure/decimal price contract."""
        return float(self.api.round_price(self.symbol, float(price)))

    @staticmethod
    def _order_node(raw: Dict) -> Dict:
        if not isinstance(raw, dict):
            return {}
        node = raw.get("order")
        if isinstance(node, dict):
            nested = node.get("order")
            if isinstance(nested, dict):
                return {**nested, "status": node.get("status", nested.get("status")), "statusTimestamp": node.get("statusTimestamp")}
            return node
        return raw

    def extract_status(self, order_data: Dict) -> str:
        node = self._order_node(order_data)
        raw = str(node.get("status", order_data.get("status", ""))).upper()
        mapping = {
            "FILLED": "FILLED",
            "OPEN": "PENDING",
            "RESTING": "PENDING",
            "TRIGGERED": "PENDING",
            "CANCELED": "CANCELLED",
            "CANCELLED": "CANCELLED",
            "REJECTED": "CANCELLED",
            "MARGIN_CANCELED": "CANCELLED",
        }
        return mapping.get(raw, "UNKNOWN")

    def extract_fill_price(self, order_data: Dict) -> Optional[float]:
        node = self._order_node(order_data)
        for key in ("avgPx", "avgFillPrice", "fillPrice", "limitPx", "limit_px", "price"):
            value = node.get(key, order_data.get(key))
            price = self._num(value, 0.0)
            if price > 0:
                return price
        return None

    def extract_filled_qty(self, order_data: Dict) -> float:
        node = self._order_node(order_data)
        for key in ("totalSz", "filledSz", "origSz", "sz", "size"):
            qty = self._num(node.get(key, order_data.get(key)), 0.0)
            if qty > 0:
                return qty
        return 0.0

    @staticmethod
    def _extract_hl_statuses(resp: Any) -> list:
        if not isinstance(resp, dict):
            return []
        data = (((resp.get("response") or {}).get("data") or {}) if isinstance(resp.get("response"), dict) else {})
        statuses = data.get("statuses") if isinstance(data, dict) else None
        return list(statuses or [])

    @classmethod
    def _tpsl_triggers_accepted(cls, resp: Any) -> bool:
        statuses = cls._extract_hl_statuses(resp)
        if len(statuses) < 2:
            return False
        accepted = 0
        for row in statuses:
            if isinstance(row, str):
                if row.strip().lower() == "waitingfortrigger":
                    accepted += 1
                continue
            if not isinstance(row, dict) or row.get("error"):
                return False
            if any(key in row for key in ("resting", "filled", "waitingForTrigger")):
                accepted += 1
        return accepted >= 2

    @staticmethod
    def _normalise_hl_side(value: Any) -> str:
        raw = str(value or "").strip().upper()
        if raw in {"B", "BID", "BUY"}:
            return "BUY"
        if raw in {"A", "ASK", "SELL"}:
            return "SELL"
        return raw

    def _normalise_open_order(self, row: Dict[str, Any]) -> Dict[str, Any]:
        raw = row if isinstance(row, dict) else {}
        order_type_node = raw.get("order_type")
        trigger_node = order_type_node.get("trigger", {}) if isinstance(order_type_node, dict) else {}
        tpsl = str(raw.get("tpsl") or trigger_node.get("tpsl") or "").strip().lower()
        raw_type = str(raw.get("orderType") or raw.get("order_type") or raw.get("type") or "").strip()
        type_key = raw_type.upper().replace(" ", "_").replace("-", "_")
        if tpsl == "sl" or ("STOP" in type_key and "PROFIT" not in type_key and "TAKE" not in type_key):
            order_type = "STOP_MARKET"
        elif tpsl == "tp" or "PROFIT" in type_key or "TAKE" in type_key:
            order_type = "TAKE_PROFIT_MARKET"
        elif "TRIGGER" in type_key:
            order_type = "TRIGGER"
        else:
            order_type = type_key

        oid = raw.get("oid") or raw.get("order_id") or raw.get("id")
        trigger_price = self._num(
            raw.get("triggerPx", raw.get("trigger_price", raw.get("stop_price", trigger_node.get("triggerPx")))),
            0.0,
        )
        limit_price = self._num(raw.get("limitPx", raw.get("limit_price", raw.get("price", raw.get("px")))), 0.0)
        quantity = self._num(raw.get("sz", raw.get("origSz", raw.get("quantity", raw.get("size")))), 0.0)
        reduce_only = bool(raw.get("reduceOnly", raw.get("reduce_only", False)))
        coin = str(raw.get("coin") or raw.get("symbol") or self.symbol)
        return {
            "order_id": str(oid or ""),
            "id": str(oid or ""),
            "type": order_type,
            "order_type": order_type,
            "side": self._normalise_hl_side(raw.get("side")),
            "quantity": quantity,
            "trigger_price": trigger_price,
            "price": limit_price,
            "status": str(raw.get("status") or raw.get("state") or "OPEN").upper(),
            "product_symbol": coin.upper(),
            "reduce_only": reduce_only,
            "raw": raw,
        }

    def _resolve_tpsl_child_order_ids(self, exit_is_buy: bool, sl_price: float, tp_price: float) -> list[str]:
        expected_side = "BUY" if exit_is_buy else "SELL"
        deadline = time.time() + max(0.0, float(_cfg("HYPERLIQUID_TPSL_OPEN_ORDER_RECONCILE_SEC", 3.0)))
        poll = max(0.25, float(_cfg("HYPERLIQUID_TPSL_OPEN_ORDER_POLL_SEC", 0.5)))
        tick_tol = max(float(self.tick_size or 0.0) * 5.0, 1e-9)

        def _price_ok(actual: float, expected: float) -> bool:
            if expected <= 0.0:
                return False
            if actual <= 0.0:
                return False
            return abs(actual - expected) <= max(tick_tol, abs(expected) * 0.0002)

        sl_oid = ""
        tp_oid = ""
        while True:
            rows = self.get_open_orders(self.symbol) or []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                oid = str(row.get("order_id") or row.get("id") or "")
                if not oid:
                    continue
                side = str(row.get("side") or "").upper()
                if side and side != expected_side:
                    continue
                typ = str(row.get("type") or row.get("order_type") or "").upper()
                trigger = self._num(row.get("trigger_price") or row.get("price"), 0.0)
                if not sl_oid and ("STOP" in typ or _price_ok(trigger, sl_price)) and _price_ok(trigger, sl_price):
                    sl_oid = oid
                elif not tp_oid and ("PROFIT" in typ or "TAKE" in typ or _price_ok(trigger, tp_price)) and _price_ok(trigger, tp_price):
                    tp_oid = oid
            if sl_oid and tp_oid:
                return [sl_oid, tp_oid]
            if time.time() >= deadline:
                return [oid for oid in (sl_oid, tp_oid) if oid]
            time.sleep(poll)

    def _entry_status(self, oid: str, fallback_price: float, fallback_qty: float) -> Dict:
        raw = self.get_order(oid) or {}
        status = self.extract_status(raw)
        return {
            "status": status,
            "fill_price": float(self.extract_fill_price(raw) or fallback_price),
            "filled_qty": float(self.extract_filled_qty(raw) or (fallback_qty if status == "FILLED" else 0.0)),
            "raw_order": raw,
            "paid_commission": 0.0,
            "paid_commission_exact": False,
        }

    def place_bracket_limit_entry(
        self,
        side: str,
        quantity: float,
        limit_price: float,
        sl_price: float,
        tp_price: float,
        timeout_sec: float,
        on_order_placed=None,
    ) -> Optional[Dict]:
        qty = self._round_qty(quantity)
        try:
            limit_price = self._round_px(limit_price)
            sl_price = self._round_px(sl_price)
            tp_price = self._round_px(tp_price)
        except Exception as exc:
            return {"_error": True, "_sc": 0, "_raw": {"error": f"hyperliquid_price_rounding_failed:{exc}"}}
        if qty <= 0:
            return {"_error": True, "_sc": 0, "_raw": {"error": "hyperliquid_qty_rounded_to_zero"}}
        api_side = str(side or "").upper()
        is_buy = api_side in {"BUY", "LONG"}
        self.limiter.wait()
        try:
            entry_resp = self.api.place_limit_order(
                coin=self.symbol,
                is_buy=is_buy,
                size=qty,
                limit_px=float(limit_price),
                reduce_only=False,
                tif="Gtc",
            )
            parsed = self.api.first_order_result(entry_resp)
        except Exception as exc:
            return {"_error": True, "_sc": 0, "_raw": {"error": str(exc)}}
        if not parsed.get("ok"):
            return {"_error": True, "_sc": 200, "_raw": parsed.get("raw", entry_resp), "_err_msg": parsed.get("error")}
        oid = str(parsed.get("oid") or "")
        if not oid:
            return {"_error": True, "_sc": 200, "_raw": parsed.get("raw", entry_resp), "_err_msg": "missing_entry_oid"}
        if on_order_placed is not None:
            try:
                on_order_placed(oid)
            except Exception:
                pass

        fill_price = float(parsed.get("avg_px") or limit_price)
        filled_qty = float(parsed.get("total_sz") or 0.0)
        deadline = time.time() + max(1.0, float(timeout_sec or _cfg("HYPERLIQUID_ENTRY_FILL_TIMEOUT_SEC", 45.0)))
        poll = max(0.25, float(_cfg("HYPERLIQUID_ENTRY_POLL_SEC", 1.0)))
        while parsed.get("status") != "FILLED" and time.time() < deadline:
            time.sleep(poll)
            state = self._entry_status(oid, float(limit_price), qty)
            if state["status"] == "FILLED":
                fill_price = float(state["fill_price"] or limit_price)
                filled_qty = float(state["filled_qty"] or qty)
                break
            if state["status"] == "CANCELLED":
                return {"_error": True, "_sc": 200, "_raw": state.get("raw_order", {}), "_err_msg": "entry_cancelled_before_fill"}
        else:
            if parsed.get("status") == "FILLED":
                filled_qty = float(parsed.get("total_sz") or qty)

        if filled_qty <= 0:
            try:
                self.cancel_order(oid)
            except Exception:
                pass
            return {"_error": True, "_sc": 0, "_raw": {"error": "hyperliquid_entry_fill_timeout", "oid": oid}}

        exit_is_buy = not is_buy
        try:
            self.limiter.wait()
            tpsl_resp = self.api.place_reduce_only_tpsl(
                coin=self.symbol,
                is_buy=exit_is_buy,
                size=filled_qty,
                stop_px=float(sl_price),
                target_px=float(tp_price),
            )
            child_oids = self.api.child_order_ids(tpsl_resp)
            if len(child_oids) < 2:
                child_oids = self._resolve_tpsl_child_order_ids(exit_is_buy, float(sl_price), float(tp_price))
        except Exception as exc:
            child_oids = []
            tpsl_resp = {"error": str(exc)}
        protection_accepted = self._tpsl_triggers_accepted(tpsl_resp)
        if len(child_oids) < 2 and not protection_accepted:
            close_resp = None
            if bool(_cfg("HYPERLIQUID_EMERGENCY_CLOSE_ON_PROTECTION_FAILURE", True)):
                try:
                    close_resp = self.api.market_close(
                        self.symbol,
                        filled_qty,
                        slippage=float(_cfg("HYPERLIQUID_PROTECTION_FAILURE_CLOSE_SLIPPAGE_PCT", 0.05)),
                    )
                except Exception as exc:
                    close_resp = {"error": str(exc)}
            return {
                "_error": True,
                "_sc": 200,
                "_raw": {"entry_oid": oid, "tpsl_response": tpsl_resp, "emergency_close": close_resp},
                "_err_msg": "hyperliquid_protection_orders_not_confirmed",
            }
        if len(child_oids) < 2 and protection_accepted:
            logger.warning(
                "Hyperliquid TP/SL accepted as waitingForTrigger but child order ids were not returned yet; "
                "position remains protected and open-order reconciliation will recover ids. entry_oid=%s",
                oid,
            )

        return {
            "order_id": oid,
            "status": "FILLED",
            "quantity": float(filled_qty),
            "price": float(limit_price),
            "fill_type": "maker",
            "fill_price": float(fill_price),
            "bracket_order": True,
            "bracket_child_verified": len(child_oids) >= 2,
            "bracket_sl_order_id": str(child_oids[0]) if len(child_oids) >= 1 else "",
            "bracket_tp_order_id": str(child_oids[1]) if len(child_oids) >= 2 else "",
            "bracket_sl_price": float(sl_price),
            "bracket_tp_price": float(tp_price),
            "protection_model": "HYPERLIQUID_TPSL_AFTER_FILL",
            "protection_confirmed": True,
            "protection_reconcile_required": len(child_oids) < 2,
            "paid_commission": 0.0,
            "paid_commission_exact": False,
            "_raw_entry": parsed.get("raw", entry_resp),
            "_raw_tpsl": tpsl_resp,
        }

    def place_order(self, side: str, order_type: str, quantity: float,
                    price: Optional[float] = None,
                    trigger_price: Optional[float] = None,
                    reduce_only: bool = False,
                    stop_order_type: Optional[str] = None) -> Optional[Dict]:
        _ = (trigger_price, stop_order_type)
        api_side = str(side or "").upper()
        is_buy = api_side in {"BUY", "LONG"}
        qty = self._round_qty(quantity)
        try:
            if str(order_type or "").upper() == "MARKET" and reduce_only:
                resp = self.api.market_close(self.symbol, qty)
                parsed = self.api.first_order_result(resp)
            else:
                wire_price = self._round_px(float(price or 0.0))
                resp = self.api.place_limit_order(
                    coin=self.symbol,
                    is_buy=is_buy,
                    size=qty,
                    limit_px=wire_price,
                    reduce_only=bool(reduce_only),
                    tif="Ioc" if str(order_type or "").upper() == "MARKET" else "Gtc",
                )
                parsed = self.api.first_order_result(resp)
        except Exception as exc:
            return {"_raw": {"error": str(exc)}, "_sc": 0, "_error": True}
        if not parsed.get("ok"):
            return {"_raw": parsed.get("raw", resp), "_sc": 200, "_error": True}
        return {
            "order_id": str(parsed.get("oid") or ""),
            "status": "FILLED" if parsed.get("status") == "FILLED" else "PENDING",
            "quantity": float(parsed.get("total_sz") or qty),
            "fill_price": float(parsed.get("avg_px") or price or 0.0),
            "_raw": parsed.get("raw", resp),
        }

    def cancel_order(self, order_id: str) -> Dict:
        self.limiter.wait()
        return self.api.cancel_order(self.symbol, int(order_id)) or {}

    def get_order(self, order_id: str) -> Optional[Dict]:
        self.limiter.wait()
        try:
            return self.api.query_order(int(order_id))
        except Exception:
            return None

    def resolve_order_execution(self, order_id: str) -> Optional[Dict]:
        raw = self.get_order(str(order_id or "").strip())
        if not isinstance(raw, dict):
            return None
        status = self.extract_status(raw)
        return {
            "status": status,
            "fill_price": float(self.extract_fill_price(raw) or 0.0),
            "filled_qty": float(self.extract_filled_qty(raw) or 0.0),
            "paid_commission": 0.0,
            "paid_commission_exact": False,
            "raw_order": raw,
        }

    def get_open_orders(self, symbol: str) -> Optional[list]:
        self.limiter.wait()
        rows = self.api.open_orders(coin=self.symbol)
        sym = str(symbol or self.symbol)
        out = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            normalised = self._normalise_open_order(row)
            coin = str((normalised.get("raw") or {}).get("coin") or normalised.get("product_symbol") or "").upper()
            if coin == sym.upper():
                out.append(normalised)
        return out

    def get_positions(self, symbol: str) -> Optional[Dict]:
        self.limiter.wait()
        return self.api.user_state(coin=self.symbol)

    def get_balance(self) -> Dict:
        self.limiter.wait()
        # Resolve native versus HIP-3 collateral from the selected instrument
        # and official account-abstraction state; never read the default DEX
        # for xyz:/km: instruments.
        return self.api.get_balance(self.symbol)

    def set_leverage(self, leverage: int, product_id: Optional[int] = None) -> Dict:
        _ = product_id
        self.limiter.wait()
        resp = self.api.update_leverage(
            self.symbol,
            int(leverage),
            is_cross=bool(getattr(config, "HYPERLIQUID_USE_CROSS_MARGIN", True)),
        )
        ok = isinstance(resp, dict) and str(resp.get("status", "")).lower() == "ok"
        return {"success": ok, "leverage": int(leverage), "raw": resp}

    def normalise_position(self, raw) -> Optional[Dict]:
        positions = raw.get("assetPositions", []) if isinstance(raw, dict) else []
        sym = self.symbol.upper()
        for row in positions:
            pos = row.get("position", row) if isinstance(row, dict) else {}
            coin = str(pos.get("coin", "")).upper()
            if coin != sym:
                continue
            signed = self._num(pos.get("szi"), 0.0)
            size = abs(signed)
            side = "LONG" if signed > 0 else "SHORT" if signed < 0 else None
            return {
                "side": side,
                "size": size,
                "entry_price": self._num(pos.get("entryPx"), 0.0),
                "unrealized_pnl": self._num(pos.get("unrealizedPnl"), 0.0),
                "raw": pos,
            }
        return {"side": None, "size": 0.0, "entry_price": 0.0, "unrealized_pnl": 0.0}


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
        elif exch == "groww":
            self._adapter = _GrowwAdapter(api, exchange_instrument=exchange_instrument)
        elif exch == "hyperliquid":
            self._adapter = _HyperliquidAdapter(api, exchange_instrument=exchange_instrument)
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
        return "₹" if str(getattr(self, "_exchange_name", "")).lower() in {"groww", "groww"} else "$"

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
            selected = str(getattr(self, "_exchange_name", "") or "").lower()
            # Keys may be ExchangeName enums or strings depending on construction.
            # Never consume another broker's tick when the selected adapter lacks metadata.
            for key, ei in by_ex.items():
                try:
                    key_value = str(getattr(key, "value", key) or "").lower()
                    if key_value == selected or key_value.endswith(f".{selected}"):
                        tick = float(getattr(ei, "tick_size", 0.0) or 0.0)
                        if tick > 0:
                            return tick
                except Exception:
                    continue
        except Exception:
            pass
        getter = getattr(config, "get_tick_size", None)
        if callable(getter):
            tick = float(getter(str(getattr(self, "_exchange_name", "") or "")) or 0.0)
            if tick > 0:
                return tick
        raise RuntimeError(f"tick_size_unresolved_for_selected_venue:{getattr(self, '_exchange_name', 'unknown')}")

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
            if self._exchange_name in {"groww", "groww"}:
                if not reduce_only:
                    logger.error("%s options guard: market entry rejected; F&O options require a priced LIMIT entry", self._exchange_name.upper())
                    return None
                ex_pos = self.get_open_position()
                if not ex_pos or bool(ex_pos.get("unadoptable")):
                    logger.critical("%s emergency close refused: no exact adoptable F&O option position is available", self._exchange_name.upper())
                    return None
                raw = ex_pos.get("raw") or {}
                ref = float(raw.get("ltp") or raw.get("LTP") or ex_pos.get("entry_price") or 0.0)
                if ref <= 0:
                    logger.critical("%s emergency close refused: no reference premium available for priced exit", self._exchange_name.upper())
                    return None
                # Groww prohibits market orders. For an emergency close, send an
                # aggressively marketable priced limit while retaining exact contract scope.
                slippage_pct = (
                    float(getattr(config, "GROWW_EMERGENCY_EXIT_LIMIT_BUFFER_PCT", getattr(config, "GROWW_EMERGENCY_EXIT_LIMIT_BUFFER_PCT", 0.10)))
                    if self._exchange_name == "groww"
                    else float(getattr(config, "GROWW_EMERGENCY_EXIT_LIMIT_BUFFER_PCT", 0.10))
                )
                tick = max(float(getattr(self._adapter, "tick_size", 0.05) or 0.05), 0.01)
                if str(api_side).lower() == "sell":
                    limit_price = math.floor((ref * max(0.01, 1.0 - slippage_pct)) / tick) * tick
                else:
                    limit_price = math.ceil((ref * (1.0 + slippage_pct)) / tick) * tick
                limit_price = max(tick, limit_price)
                logger.critical(
                    "%s options MARKET exits are disabled; routing emergency %s as aggressive LIMIT qty=%s premium_ref=₹%.2f limit=₹%.2f",
                    self._exchange_name.upper(), api_side.upper(), quantity, ref, limit_price,
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
            cur = "₹" if self._exchange_name in {"groww", "groww"} else "$"
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
        cur = "₹" if self._exchange_name in {"groww", "groww"} else "$"
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

    def execute_groww_long_option_with_protection(self, side: str, quantity: float,
                                                  limit_price: float,
                                                  sl_price: float,
                                                  tp_price: float,
                                                  timeout_sec: float = 45.0):
        """Run Groww's fill-first long-option lifecycle and return its result."""
        if self._exchange_name != "groww":
            raise RuntimeError("Groww long-option lifecycle is only valid for exchange=groww")
        api_side = self._normalize_side(side)
        if api_side != "BUY":
            raise RuntimeError("Groww long-option lifecycle rejects non-BUY entries")

        from execution.groww_long_option_execution import (
            GrowwLongOptionExecutor,
            GrowwProtectionPlan,
            LongOptionCandidateScore,
        )

        raw = self._adapter._active_raw() if hasattr(self._adapter, "_active_raw") else {}
        if not isinstance(raw, dict):
            raw = {}
        trading_symbol = str(raw.get("trading_symbol") or raw.get("TradingSymbol") or self.symbol or "").strip()
        right_raw = str(raw.get("right") or raw.get("option_type") or raw.get("OptionType") or "").strip().lower()
        option_type = "CE" if right_raw in {"c", "ce", "call"} else "PE" if right_raw in {"p", "pe", "put"} else right_raw.upper()
        lot_size = 0
        try:
            lot_size = int(round(float(self._adapter._lot_size() or 0.0))) if hasattr(self._adapter, "_lot_size") else 0
        except Exception:
            lot_size = 0
        tick = max(float(getattr(self._adapter, "tick_size", getattr(config, "GROWW_OPTION_TICK_SIZE", 0.05)) or 0.05), 0.01)
        stop_trigger = round(round(float(sl_price) / tick) * tick, 2)
        stop_limit = max(tick, stop_trigger - tick)

        def _raw_num(key: str, default: float = 0.0) -> float:
            try:
                return float(self._adapter._num(raw.get(key), default)) if hasattr(self._adapter, "_num") else default
            except Exception:
                return default

        candidate = LongOptionCandidateScore(
            trading_symbol=trading_symbol,
            option_type=option_type,
            expiry=str(raw.get("expiry_date") or raw.get("ExpiryDate") or raw.get("expiry") or ""),
            strike=_raw_num("strike_price") or _raw_num("StrikePrice") or _raw_num("strike"),
            premium=float(limit_price),
            spread_bps=_raw_num("spread_bps"),
            delta=_raw_num("delta"),
            gamma=_raw_num("gamma"),
            theta=_raw_num("theta"),
            vega=_raw_num("vega"),
            iv=_raw_num("iv") or _raw_num("implied_volatility"),
            expected_premium_return_after_cost=_raw_num("expected_premium_return_after_cost"),
            probability_tp_before_sl=None,
            theta_cost_for_expected_hold=_raw_num("theta_cost_for_expected_hold"),
            liquidity_score=_raw_num("liquidity_score"),
            protection_feasible=True,
            total_score=_raw_num("total_score"),
            lot_size=lot_size if lot_size > 0 else None,
            underlying=str(raw.get("stock_code") or raw.get("underlying_symbol") or raw.get("StockCode") or "NIFTY").upper(),
        )
        protection = GrowwProtectionPlan(
            target_price=float(tp_price),
            stop_trigger_price=stop_trigger,
            stop_limit_price=stop_limit,
            thesis_side="bullish" if option_type == "CE" else "bearish" if option_type == "PE" else "",
        )
        static_validator = getattr(self, "_groww_static_ip_validator", None) or getattr(self.api, "validate_static_ip", None)
        executor = GrowwLongOptionExecutor(
            self.api,
            order_body_factory=self._adapter._order_body,
            tick_size=tick,
            static_ip_validator=static_validator if callable(static_validator) else None,
        )
        return executor.execute(
            candidate=candidate,
            quantity=int(quantity),
            limit_price=float(limit_price),
            protection=protection,
            fill_timeout_sec=timeout_sec,
            poll_interval_sec=float(getattr(config, "GROWW_ORDER_FILL_POLL_SEC", 1.0)),
            require_static_ip=bool(getattr(config, "GROWW_REQUIRE_STATIC_IP_FOR_LIVE_ORDERS", True)),
            require_algo_confirmation=bool(getattr(config, "GROWW_REQUIRE_SEBI_ALGO_CONFIRMATION_FOR_LIVE_ORDERS", True)),
        )

    def place_bracket_limit_entry(self, side: str, quantity: float,
                                  limit_price: float,
                                  sl_price: float,
                                  tp_price: float,
                                  timeout_sec: float = 45.0,
                                  on_order_placed=None) -> Optional[Dict]:
        """
        Protected entry for adapters that expose broker-native protection.
        Delta uses native bracket orders; Groww buys first, confirms fill, then
        arms a documented NFO OCO SELL smart order for actual filled quantity.
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

        if self._exchange_name == "hyperliquid":
            formatter = getattr(self._adapter, "_round_px", None)
            if callable(formatter):
                try:
                    limit_price = formatter(float(limit_price))
                    sl_price = formatter(float(sl_price))
                    tp_price = formatter(float(tp_price))
                except Exception as exc:
                    self.last_order_error = {
                        "stage": "hyperliquid_price_precision_validation",
                        "status_code": 0,
                        "reason": str(exc),
                        "raw": {"error": str(exc)},
                    }
                    logger.error("Hyperliquid executable price normalisation failed before submission: %s", exc)
                    return None

        cur = self._currency_symbol()
        qty_note = ""
        qty_to_contracts = getattr(self._adapter, "_qty_to_contracts", None)
        if callable(qty_to_contracts):
            try:
                qty_note = f" ({int(qty_to_contracts(quantity))} contracts)"
            except Exception:
                qty_note = ""
        logger.info(
            f"[PROTECTED_ENTRY] {side.upper()} {quantity}{qty_note} @ {cur}{limit_price:.2f} "
            f"SL={cur}{sl_price:.2f} TP={cur}{tp_price:.2f} (timeout={timeout_sec:.0f}s)"
        )

        if self._exchange_name == "coinswitch":
            try:
                data = self._adapter.place_bracket_limit_entry(
                    side=side, quantity=quantity, limit_price=limit_price,
                    sl_price=sl_price, tp_price=tp_price, timeout_sec=timeout_sec,
                    on_order_placed=on_order_placed,
                )
            except Exception as exc:
                self.last_order_error = {
                    "stage": "coinswitch_fill_first_position_tpsl_lifecycle",
                    "status_code": 0, "reason": str(exc), "raw": {"error": str(exc)},
                }
                logger.error("CoinSwitch protected lifecycle failed before entry: %s", exc, exc_info=True)
                return None
            if not data or data.get("_error"):
                raw = (data or {}).get("_raw", {})
                self.last_order_error = {
                    "stage": "coinswitch_fill_first_position_tpsl_lifecycle",
                    "status_code": (data or {}).get("_sc", 0),
                    "reason": (data or {}).get("_err_msg") or self._compact_error(raw),
                    "raw": raw,
                }
                logger.error("CoinSwitch protected entry failed: %s raw=%s", self.last_order_error["reason"], raw)
                return None
            self._record_order(str(data.get("order_id", "")), {
                "order_id": str(data.get("order_id", "")), "side": side,
                "type": "COINSWITCH_POSITION_TPSL_AFTER_FILL",
                "quantity": float(data.get("quantity", quantity) or quantity),
                "price": float(limit_price), "status": "FILLED",
                "timestamp": datetime.now().isoformat(),
                "bracket_sl_order_id": data.get("bracket_sl_order_id"),
                "bracket_tp_order_id": data.get("bracket_tp_order_id"),
            })
            logger.info(
                "CoinSwitch protected entry filled order=%s SL=%s TP=%s",
                data.get("order_id"), data.get("bracket_sl_order_id"), data.get("bracket_tp_order_id"),
            )
            return data

        if self._exchange_name == "hyperliquid":
            try:
                data = self._adapter.place_bracket_limit_entry(
                    side=side,
                    quantity=quantity,
                    limit_price=limit_price,
                    sl_price=sl_price,
                    tp_price=tp_price,
                    timeout_sec=timeout_sec,
                    on_order_placed=on_order_placed,
                )
            except Exception as exc:
                self.last_order_error = {
                    "stage": "hyperliquid_fill_first_tpsl_lifecycle",
                    "status_code": 0,
                    "reason": str(exc),
                    "raw": {"error": str(exc)},
                }
                logger.error("Hyperliquid TP/SL lifecycle failed before entry: %s", exc, exc_info=True)
                return None
            if not data or data.get("_error"):
                raw = (data or {}).get("_raw", {})
                self.last_order_error = {
                    "stage": "hyperliquid_fill_first_tpsl_lifecycle",
                    "status_code": (data or {}).get("_sc", 0),
                    "reason": (data or {}).get("_err_msg") or self._compact_error(raw),
                    "raw": raw,
                }
                logger.error("Hyperliquid protected entry failed: %s raw=%s", self.last_order_error["reason"], raw)
                return None
            self._record_order(str(data.get("order_id", "")), {
                "order_id": str(data.get("order_id", "")),
                "side": side,
                "type": "HYPERLIQUID_TPSL_AFTER_FILL",
                "quantity": float(data.get("quantity", quantity) or quantity),
                "price": float(limit_price),
                "status": "FILLED",
                "timestamp": datetime.now().isoformat(),
                "bracket_sl_order_id": data.get("bracket_sl_order_id"),
                "bracket_tp_order_id": data.get("bracket_tp_order_id"),
            })
            logger.info(
                "Hyperliquid protected entry filled order=%s SL=%s TP=%s",
                data.get("order_id"), data.get("bracket_sl_order_id"), data.get("bracket_tp_order_id"),
            )
            return data

        if self._exchange_name == "groww":
            try:
                result = self.execute_groww_long_option_with_protection(
                    side, quantity, limit_price, sl_price, tp_price, timeout_sec=timeout_sec
                )
            except Exception as exc:
                self.last_order_error = {
                    "stage": "groww_fill_first_oco_lifecycle",
                    "status_code": 0,
                    "reason": str(exc),
                    "raw": {"error": str(exc)},
                }
                logger.error("Groww fill-first OCO lifecycle failed before entry: %s", exc, exc_info=True)
                return None
            if not result.approved:
                self.last_order_error = {
                    "stage": "groww_fill_first_oco_lifecycle",
                    "status_code": 0,
                    "reason": "; ".join(result.reasons),
                    "raw": result.to_dict(),
                }
                return None
            data = {
                "order_id": result.entry_order_id,
                "smart_order_id": result.smart_order_id,
                "status": "FILLED",
                "quantity": float(result.filled_quantity),
                "price": float(limit_price),
                "fill_type": "maker",
                "fill_price": float(result.average_fill_price),
                "bracket_order": True,
                "bracket_child_verified": True,
                "bracket_sl_order_id": f"GROWWOCO:{result.smart_order_id}:STOPLOSS",
                "bracket_tp_order_id": f"GROWWOCO:{result.smart_order_id}:TARGET",
                "bracket_sl_price": float(sl_price),
                "bracket_tp_price": float(tp_price),
                "protection_model": "GROWW_OCO_AFTER_FILL",
                "protection_confirmed": True,
                "paid_commission": 0.0,
                "paid_commission_exact": False,
                "_lifecycle": result.to_dict(),
            }
            if on_order_placed is not None:
                try:
                    on_order_placed(result.entry_order_id)
                except Exception as _cb_e:
                    logger.warning(f"on_order_placed callback error (non-fatal): {_cb_e}")
            self._record_order(result.entry_order_id, {
                "order_id": result.entry_order_id,
                "side": side,
                "type": "GROWW_LONG_OPTION_OCO",
                "quantity": float(result.filled_quantity),
                "price": float(limit_price),
                "status": "FILLED",
                "timestamp": datetime.now().isoformat(),
                "smart_order_id": result.smart_order_id,
            })
            return data

        data = self._adapter.place_bracket_limit_entry(
            side=side, quantity=quantity,
            limit_price=limit_price, sl_price=sl_price, tp_price=tp_price,
        )
        if not data or data.get("_error"):
            sc = (data or {}).get("_sc", 0)
            raw = (data or {}).get("_raw", {})
            reason = self._compact_error(raw)
            stage = (
                "groww_fill_first_oco_lifecycle" if self._exchange_name == "groww"
                else "delta_native_bracket_entry"
            )
            self.last_order_error = {
                "stage": stage,
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
                filled_qty = float(details.get("filled_qty") or data.get("quantity") or quantity)
                data["fill_type"]    = "maker"
                data["fill_price"]   = fill_px
                data["quantity"]     = filled_qty
                data["bracket_order"] = True
                # Propagate exact entry fee from Delta paid_commission
                data["paid_commission"] = float(details.get("paid_commission", 0) or 0)
                data["paid_commission_exact"] = bool(details.get("paid_commission_exact", False))
                logger.info(f"✅ Protected entry fill: {order_id[:8]}… @ {cur}{fill_px:.2f}"
                            f" fee={cur}{data['paid_commission']:.4f}"
                            f" exact={data['paid_commission_exact']}")

                if data.get("protection_model") in {"GROWW_OCO_AFTER_FILL", "HYPERLIQUID_TPSL_AFTER_FILL"}:
                    # Groww OCO protection was already confirmed after the actual
                    # option fill; do not search the normal order book for
                    # Delta-style bracket children.
                    data["bracket_child_verified"] = True
                    logger.info(
                        "✅ %s broker-protected smart bracket active: entry=%s SL-leg=%s TP-leg=%s",
                        self._exchange_name.upper(), data.get("order_id"), data.get("bracket_sl_order_id"), data.get("bracket_tp_order_id"),
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
            "stage": "groww_fill_first_oco_timeout" if self._exchange_name == "groww" else "delta_native_bracket_fill_timeout",
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

        Delta, CoinSwitch and GROWW all route through ``get_fill_details``.
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

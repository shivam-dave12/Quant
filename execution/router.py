"""
execution/router.py — Hot-Swappable Execution Router
=====================================================
Owns one OrderManager per exchange and routes all execution calls
to the active one.  Switching is instant, atomic, and safe:

  - Switch blocked when a position is open (user must close first)
  - Switch blocked if new exchange's balance cannot be confirmed
  - On switch: active limiter updates so GlobalRateLimiter routes correctly
  - All state (active_orders, order_history) belongs to each OM individually
    so history is never lost across switches

Telegram controller calls router.switch(exchange_name) and gets a
human-readable result string back.
"""

from __future__ import annotations

import logging
import threading
from typing import Dict, Optional, Tuple

import sys, os; sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from core.types  import Exchange
from execution.order_manager import (
    OrderManager, CancelResult, GlobalRateLimiter,
    _CS_LIMITER, _DELTA_LIMITER,
)

logger = logging.getLogger(__name__)


class ExecutionRouter:
    """
    Routes execution calls to the currently active OrderManager.
    Thread-safe; switch is under a write lock.
    """

    def __init__(
        self,
        coinswitch_om: Optional[OrderManager],
        delta_om:      Optional[OrderManager],
        default:       str = "delta",
    ) -> None:
        self._lock        = threading.RLock()
        self._managers: Dict[str, OrderManager] = {}

        if coinswitch_om is not None:
            self._managers[Exchange.COINSWITCH.value] = coinswitch_om
        if delta_om is not None:
            self._managers[Exchange.DELTA.value] = delta_om

        if not self._managers:
            raise RuntimeError("ExecutionRouter requires at least one OrderManager")

        # Validate default
        default_key = Exchange.from_str(default).value
        if default_key not in self._managers:
            default_key = next(iter(self._managers))
            logger.warning(f"Requested default '{default}' not available — "
                           f"using '{default_key}'")

        self._active_key = default_key
        self._sync_global_limiter()
        logger.info(f"✅ ExecutionRouter active exchange: {self._active_key}")

    # ── Internal ─────────────────────────────────────────────────────────────

    def _sync_global_limiter(self) -> None:
        """Make GlobalRateLimiter point at the active exchange's limiter."""
        limiter = _DELTA_LIMITER if self._active_key == Exchange.DELTA.value \
                  else _CS_LIMITER
        GlobalRateLimiter.set_active(limiter)

    # ── Properties ───────────────────────────────────────────────────────────

    @property
    def active(self) -> OrderManager:
        """The currently active OrderManager."""
        with self._lock:
            return self._managers[self._active_key]

    @property
    def active_exchange(self) -> str:
        with self._lock:
            return self._active_key

    @property
    def api(self):
        """Legacy: strategy accesses order_manager.api directly."""
        return self.active.api

    # ── Exchange switching ────────────────────────────────────────────────────

    def switch(
        self,
        target_exchange:  str,
        strategy=None,    # QuantStrategy instance — used for open-position check
        force: bool = False,
    ) -> Tuple[bool, str]:
        """
        Switch execution to target_exchange.

        Returns: (success: bool, message: str)

        Safety guards:
          1. Must be a configured exchange.
          2. No open position (unless force=True — emergency use only).
          3. Target exchange balance must be readable.
        """
        try:
            target_key = Exchange.from_str(target_exchange).value
        except ValueError as e:
            return False, str(e)

        with self._lock:
            if target_key == self._active_key:
                return True, f"Already executing on {target_key} — no change."

            if target_key not in self._managers:
                return False, (
                    f"Exchange '{target_key}' is not configured. "
                    f"Available: {list(self._managers.keys())}"
                )

            # Guard 1: no open position
            if not force and strategy is not None:
                pos = strategy.get_position()
                if pos is not None:
                    side  = pos.get("side", "?")
                    entry = pos.get("entry_price", 0)
                    return False, (
                        f"❌ Cannot switch exchange while position is open.\n"
                        f"Current: {side} @ ${entry:,.2f}\n"
                        f"Close position first, then /setexchange {target_key}."
                    )

            # Guard 2: verify target balance is readable
            target_om = self._managers[target_key]
            try:
                bal = target_om.get_balance()
                if bal is None or bal.get("error"):
                    return False, (
                        f"❌ Cannot verify balance on {target_key}: "
                        f"{bal.get('error', 'null response') if bal else 'null response'}\n"
                        f"Check API credentials for {target_key}."
                    )
                avail = float(bal.get("available", 0))
            except Exception as e:
                return False, f"❌ Balance check on {target_key} failed: {e}"

            # All guards passed — switch
            old_key = self._active_key
            self._active_key = target_key
            self._sync_global_limiter()

            # Update config so downstream reads (strategy, risk manager) see it
            config.EXECUTION_EXCHANGE = target_key

            logger.info(f"✅ ExecutionRouter switched: {old_key} → {target_key} "
                        f"(balance on {target_key}: ${avail:,.2f})")

            return True, (
                f"✅ <b>Execution switched to {target_key.upper()}</b>\n"
                f"Balance: ${avail:,.2f} USDT\n"
                f"Previous: {old_key}"
            )

    # ── Delegate all OrderManager calls to the active instance ───────────────
    # These are the methods the strategy and risk manager call directly.

    def place_market_order(self, *a, **kw): return self.active.place_market_order(*a, **kw)
    def place_limit_order(self, *a, **kw):  return self.active.place_limit_order(*a, **kw)
    def place_limit_entry(self, *a, **kw):  return self.active.place_limit_entry(*a, **kw)
    def place_bracket_limit_entry(self, *a, **kw): return self.active.place_bracket_limit_entry(*a, **kw)
    def emergency_flatten(self, *a, **kw): return self.active.emergency_flatten(*a, **kw)
    def place_stop_loss(self, *a, **kw):    return self.active.place_stop_loss(*a, **kw)
    def place_take_profit(self, *a, **kw):  return self.active.place_take_profit(*a, **kw)
    def replace_stop_loss(self, *a, **kw):  return self.active.replace_stop_loss(*a, **kw)
    def replace_take_profit(self, *a, **kw): return self.active.replace_take_profit(*a, **kw)
    def cancel_order(self, *a, **kw):       return self.active.cancel_order(*a, **kw)
    def cancel_all_exit_orders(self, *a, **kw): return self.active.cancel_all_exit_orders(*a, **kw)
    def cancel_symbol_conditionals(self, *a, **kw): return self.active.cancel_symbol_conditionals(*a, **kw)
    def get_open_position(self, *a, **kw):  return self.active.get_open_position(*a, **kw)
    def get_order_status(self, *a, **kw):   return self.active.get_order_status(*a, **kw)
    def get_order_status_safe(self, *a, **kw): return self.active.get_order_status_safe(*a, **kw)
    def get_fill_details(self, *a, **kw):   return self.active.get_fill_details(*a, **kw)
    def extract_fill_price(self, *a, **kw): return self.active.extract_fill_price(*a, **kw)
    def get_open_orders(self, *a, **kw):    return self.active.get_open_orders(*a, **kw)
    def get_balance(self, *a, **kw):        return self.active.get_balance(*a, **kw)
    def set_leverage(self, *a, **kw):       return self.active.set_leverage(*a, **kw)
    def place_order_guaranteed(self, *a, **kw): return self.active.place_order_guaranteed(*a, **kw)
    def get_active_orders(self, *a, **kw):  return self.active.get_active_orders(*a, **kw)
    def get_order_count(self, *a, **kw):    return self.active.get_order_count(*a, **kw)
    def get_recent_order_history(self, *a, **kw): return self.active.get_recent_order_history(*a, **kw)
    def identify_exit_order(self, *a, **kw):     return self.active.identify_exit_order(*a, **kw)

    @staticmethod
    def compute_signal_urgency(*a, **kw):
        return OrderManager.compute_signal_urgency(*a, **kw)

    @property
    def CancelResult(self):
        return CancelResult

    @property
    def _open_orders_404(self):
        return self.active._open_orders_404

    @property
    def _product_id(self) -> Optional[int]:
        """
        Delta-specific: expose the scalar product_id integer for leverage
        setting during bot startup (main.py calls om.set_leverage(pid, lev)).

        BUG-10 FIX: The original property returned `adapter._pid_cache` directly.
        In _DeltaAdapter, `_pid_cache` is typed `Optional[int]` — a scalar or None.
        The bug description noted callers expected a scalar but might receive a dict;
        that mismatch only arises if _pid_cache is ever accidentally assigned a dict
        elsewhere.  Adding the explicit `Optional[int]` return annotation catches
        that statically, and the isinstance guard below makes it safe at runtime.
        """
        adapter = getattr(self.active, "_adapter", None)
        if adapter is None:
            return None
        pid = getattr(adapter, "_pid_cache", None)
        # Defensive: if something erroneously wrote a dict, return None rather
        # than silently propagating a wrong type to set_leverage().
        if isinstance(pid, int):
            return pid
        return None

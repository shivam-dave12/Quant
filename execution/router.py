"""
execution/router.py — Delta-only execution router
=================================================
This build intentionally removes non-working alternate-exchange routing.  The
router is now a thin, thread-safe facade around one Delta OrderManager so the
strategy never wastes cycles validating unavailable venues or switching paths.
"""
from __future__ import annotations

import logging
import threading
from typing import Optional, Tuple

import sys, os; sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from core.types import Exchange
from execution.order_manager import OrderManager, CancelResult, GlobalRateLimiter, _DELTA_LIMITER

logger = logging.getLogger(__name__)


class ExecutionRouter:
    """Thread-safe Delta-only execution facade."""

    def __init__(self, delta_om: Optional[OrderManager]) -> None:
        if delta_om is None:
            raise RuntimeError("ExecutionRouter requires a Delta OrderManager")
        self._lock = threading.RLock()
        self._manager = delta_om
        self._active_key = Exchange.DELTA.value
        GlobalRateLimiter.set_active(_DELTA_LIMITER)
        config.EXECUTION_EXCHANGE = self._active_key
        logger.info("✅ ExecutionRouter active exchange: delta")

    @property
    def active(self) -> OrderManager:
        with self._lock:
            return self._manager

    @property
    def active_exchange(self) -> str:
        return self._active_key

    @property
    def api(self):
        return self.active.api

    def switch(self, target_exchange: str, strategy=None, force: bool = False) -> Tuple[bool, str]:
        try:
            target_key = Exchange.from_str(target_exchange).value
        except ValueError as e:
            return False, str(e)
        if target_key == self._active_key:
            return True, "Already executing on DELTA — this build is Delta-only."
        return False, "This build is Delta-only; exchange switching has been removed."

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
        adapter = getattr(self.active, "_adapter", None)
        if adapter is None:
            return None
        pid = getattr(adapter, "_pid_cache", None)
        return pid if isinstance(pid, int) else None

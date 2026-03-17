"""
exchanges/base.py — Abstract interface contracts
=================================================
Every exchange adapter (CoinSwitch, Delta) MUST implement these ABCs.
The rest of the codebase only depends on these abstractions — never on
a concrete adapter directly. This makes exchange-switching transparent.
"""

from __future__ import annotations

import threading
import time
from abc import ABC, abstractmethod
from collections import deque
from typing import Callable, Dict, List, Optional

from core.candle import Candle
from core.types  import OrderBook, TradeTick, Exchange


# ── REST API ──────────────────────────────────────────────────────────────────

class BaseExchangeAPI(ABC):
    """
    Minimum contract that every exchange REST adapter must satisfy.

    All methods return a plain dict:
      success → {"success": True,  "result": <payload>}
      failure → {"success": False, "error": "<reason>"}

    Adapters may add extra keys but must never omit "success".
    """

    EXCHANGE_ID: Exchange   # must be overridden in each subclass

    # ── Orders ────────────────────────────────────────────────────────────────

    @abstractmethod
    def place_order(
        self,
        symbol:       str,
        side:         str,       # "BUY" | "SELL"
        order_type:   str,       # "MARKET" | "LIMIT" | "STOP_MARKET" | ...
        quantity:     float,
        price:        Optional[float] = None,
        trigger_price: Optional[float] = None,
        reduce_only:  bool = False,
        **kwargs,
    ) -> Dict: ...

    @abstractmethod
    def cancel_order(self, order_id: str, **kwargs) -> Dict: ...

    @abstractmethod
    def get_order(self, order_id: str, **kwargs) -> Dict: ...

    @abstractmethod
    def get_open_orders(self, symbol: Optional[str] = None, **kwargs) -> Dict: ...

    @abstractmethod
    def cancel_all_orders(
        self, symbol: Optional[str] = None, **kwargs
    ) -> Dict: ...

    # ── Positions / Account ───────────────────────────────────────────────────

    @abstractmethod
    def get_positions(
        self, symbol: Optional[str] = None, **kwargs
    ) -> Dict: ...

    @abstractmethod
    def get_balance(self, currency: str = "USDT") -> Dict:
        """
        Must return:
          {"available": float, "locked": float, "total": float, "currency": str}
        """
        ...

    @abstractmethod
    def set_leverage(self, leverage: int, **kwargs) -> Dict: ...

    # ── Market data ───────────────────────────────────────────────────────────

    @abstractmethod
    def get_klines(
        self,
        symbol:   str,
        interval: int,     # minutes
        limit:    int = 100,
        **kwargs,
    ) -> Dict: ...


# ── WebSocket ─────────────────────────────────────────────────────────────────

class BaseExchangeWebSocket(ABC):
    """
    Minimum contract for every exchange WebSocket adapter.
    All callbacks receive normalised dicts matching the Candle / OrderBook /
    TradeTick shapes.  Adapters normalise on ingress so no consumer
    needs to know the wire format.
    """

    EXCHANGE_ID: Exchange

    @abstractmethod
    def subscribe_orderbook(
        self,
        symbol:   str,
        callback: Callable[[OrderBook], None],
        depth:    int = 20,
    ) -> None: ...

    @abstractmethod
    def subscribe_trades(
        self,
        symbol:   str,
        callback: Callable[[TradeTick], None],
    ) -> None: ...

    @abstractmethod
    def subscribe_candles(
        self,
        symbol:    str,
        interval:  int,   # minutes
        callback:  Callable[[Candle], None],
    ) -> None: ...

    @abstractmethod
    def connect(self) -> bool: ...

    @abstractmethod
    def disconnect(self) -> None: ...

    @abstractmethod
    def is_healthy(self, timeout_seconds: int = 35) -> bool: ...


# ── Data Manager ─────────────────────────────────────────────────────────────

class BaseDataManager(ABC):
    """
    Uniform data-access layer consumed by the strategy and aggregator.
    Every concrete data manager (CoinSwitch / Delta) implements this.
    The MarketAggregator also implements it so the strategy is fully
    exchange-agnostic.
    """

    @abstractmethod
    def get_candles(
        self, timeframe: str, limit: Optional[int] = None
    ) -> List[Candle]:
        """
        Returns a list of closed Candle objects, newest last.
        timeframe: "1m" | "5m" | "15m" | "4h" | ...
        """
        ...

    @abstractmethod
    def get_last_price(self) -> float: ...

    @abstractmethod
    def get_orderbook(self) -> Optional[Dict]:
        """
        Returns canonical orderbook dict:
          {"bids": [[price, qty], ...], "asks": [[price, qty], ...], "timestamp": float}
        or None if unavailable.
        """
        ...

    @abstractmethod
    def get_recent_trades_raw(self) -> List[Dict]:
        """
        Returns list of trade dicts:
          [{"price": float, "quantity": float, "side": "buy"|"sell",
            "timestamp": float}, ...]
        newest last.
        """
        ...

    @abstractmethod
    def is_price_fresh(self, max_stale_seconds: float = 90.0) -> bool: ...

    @abstractmethod
    def start(self) -> bool: ...

    @abstractmethod
    def stop(self) -> None: ...

    @abstractmethod
    def restart_streams(self) -> bool: ...

    @abstractmethod
    def wait_until_ready(self, timeout_sec: float = 120.0) -> bool: ...

    def register_strategy(self, strategy) -> None:
        """Optional hook: let data managers forward real-time events to strategy."""
        self._strategy_ref = strategy

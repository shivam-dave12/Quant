"""
CoinSwitch Futures Trading WebSocket - PRODUCTION GRADE

Features:
- Automatic reconnection with exponential backoff
- Connection health monitoring
- Thread-safe callback management
- Graceful degradation on errors
- AUTO-RESUBSCRIPTION after reconnect
"""

import socketio
import time
import logging
import threading
from typing import Callable, Dict, Optional, List
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class FuturesWebSocket:
    """Production-grade WebSocket client with auto-reconnect and auto-resubscribe"""

    BASE_URL = "https://ws.coinswitch.co"
    HANDSHAKE_PATH = "/pro/realtime-rates-socket/futures/exchange_2"
    NAMESPACE = "/exchange_2"
    
    EVENT_ORDERBOOK = "FETCH_ORDER_BOOK_CS_PRO"
    EVENT_CANDLESTICK = "FETCH_CANDLESTICK_CS_PRO"
    EVENT_TRADES = "FETCH_TRADES_CS_PRO"
    EVENT_TICKER = "FETCH_TICKER_INFO_CS_PRO"

    def __init__(self):
        """Initialize WebSocket client"""
        self.sio = socketio.Client(
            reconnection=True,
            reconnection_attempts=0,  # Infinite
            reconnection_delay=1,
            reconnection_delay_max=30,
        )
        
        self.is_connected = False
        self.stop_event = threading.Event()
        
        # Thread-safe callback management
        self._callbacks_lock = threading.RLock()
        self.orderbook_callbacks: List[Callable] = []
        # ✅ FIX: Interval-keyed dict instead of single list
        # Key = interval string (e.g. "1", "5", "60", "240", "1440")
        self.candlestick_callbacks: Dict[str, List[Callable]] = {}
        self.trades_callbacks: List[Callable] = []
        self.ticker_callbacks: List[Callable] = []
        
        # ✅ NEW: Store subscription parameters for auto-resubscribe
        # CRITICAL: Also stores callbacks so they survive reconnect.
        # Without this, _resubscribe_all clears callback lists and only
        # re-emits server events — data_manager callbacks are lost forever.
        self._subscriptions_lock = threading.RLock()
        self._subscriptions: Dict[str, list] = {
            "orderbook": [],
            "candlestick": [],
            "trades": [],
            "ticker": []
        }
        
        # Connection health monitoring
        self._last_message_time: Optional[datetime] = None
        self._connection_failures = 0
        self._max_connection_failures = 5
        
        self._setup_handlers()
        logger.info("WebSocket client initialized (production grade with auto-resubscribe)")

    def _setup_handlers(self):
        """Setup WebSocket event handlers"""
        
        @self.sio.event(namespace=self.NAMESPACE)
        def connect():
            self.is_connected = True
            self._connection_failures = 0
            self._last_message_time = datetime.now()
            logger.info(f"WebSocket connected to {self.NAMESPACE}")
            
            # ✅ CRITICAL FIX: Auto-resubscribe to all previous subscriptions
            self._resubscribe_all()
        
        @self.sio.event(namespace=self.NAMESPACE)
        def disconnect():
            self.is_connected = False
            logger.warning("WebSocket disconnected")
        
        @self.sio.event(namespace=self.NAMESPACE)
        def connect_error(data):
            self._connection_failures += 1
            logger.error(
                f"Connection error (failures: {self._connection_failures}/"
                f"{self._max_connection_failures}): {data}"
            )
            
            if self._connection_failures >= self._max_connection_failures:
                logger.critical("Max connection failures reached - stopping reconnection")
                self.stop_event.set()
        
        # ORDERBOOK handler with error isolation
        @self.sio.on(self.EVENT_ORDERBOOK, namespace=self.NAMESPACE)
        def on_orderbook(data):
            try:
                self._last_message_time = datetime.now()
                if not isinstance(data, dict) or not ("bids" in data or "b" in data):
                    return
                
                formatted = {
                    "b": data.get("bids", data.get("b", [])),
                    "a": data.get("asks", data.get("a", [])),
                    "timestamp": data.get("timestamp"),
                    "symbol": data.get("s"),
                }
                
                with self._callbacks_lock:
                    callbacks = list(self.orderbook_callbacks)
                
                for callback in callbacks:
                    try:
                        callback(formatted)
                    except Exception as e:
                        logger.error(f"Orderbook callback error: {e}", exc_info=True)
            except Exception as e:
                logger.error(f"Error in orderbook handler: {e}", exc_info=True)
        
        # TRADES handler with error isolation
        @self.sio.on(self.EVENT_TRADES, namespace=self.NAMESPACE)
        def on_trades(data):
            try:
                self._last_message_time = datetime.now()
                if not isinstance(data, dict) or "p" not in data:
                    return
                
                formatted = {
                    "p": data.get("p"),
                    "q": data.get("q"),
                    "T": data.get("E", data.get("T")),
                    "m": data.get("m"),
                    "s": data.get("s"),
                }
                
                with self._callbacks_lock:
                    callbacks = list(self.trades_callbacks)
                
                for callback in callbacks:
                    try:
                        callback(formatted)
                    except Exception as e:
                        logger.error(f"Trade callback error: {e}", exc_info=True)
            except Exception as e:
                logger.error(f"Error in trades handler: {e}", exc_info=True)
        
        # CANDLESTICK handler with INTERVAL-BASED ROUTING
        @self.sio.on(self.EVENT_CANDLESTICK, namespace=self.NAMESPACE)
        def on_candlestick(data):
            """Handle candlestick updates — routes to interval-specific callbacks"""
            try:
                self._last_message_time = datetime.now()

                # Skip subscription confirmations
                if isinstance(data, dict) and 'success' in data:
                    return

                # Validate candle data
                if not isinstance(data, dict) or 'o' not in data or 't' not in data:
                    return

                # Log first candle at debug (raw JSON is noise at INFO level)
                if not hasattr(self, '_first_candle_logged'):
                    logger.debug("="*80)
                    logger.debug(f"✅ First candle: {data}")
                    logger.debug("="*80)
                    self._first_candle_logged = True
                    logger.info(f"✅ WS candle feed live (interval={data.get('i','?')})")

                # ✅ FIX: Route by interval field 'i'
                interval_key = str(data.get('i', ''))

                with self._callbacks_lock:
                    callbacks = list(self.candlestick_callbacks.get(interval_key, []))

                if not callbacks:
                    # Fallback: if no interval-specific callbacks, skip
                    return

                for callback in callbacks:
                    try:
                        callback(data)
                    except Exception as e:
                        logger.error(f"Callback error (interval={interval_key}): {e}", exc_info=True)

            except Exception as e:
                logger.error(f"Handler error: {e}", exc_info=True)

    def _resubscribe_all(self):
        """
        CRITICAL FIX: Resubscribe to all streams AND restore callbacks after reconnect.
        
        Previous bug: callbacks were cleared but never re-registered from stored subscriptions.
        After any auto-reconnect, the bot went completely blind — no orderbook, trade, or
        candle data would flow to data_manager, despite the WebSocket being "connected".
        """
        with self._subscriptions_lock:
            logger.info("🔄 Resubscribing to all streams after reconnect...")

            # Clear callback lists — they'll be rebuilt from stored subscriptions
            with self._callbacks_lock:
                self.orderbook_callbacks.clear()
                self.candlestick_callbacks.clear()
                self.trades_callbacks.clear()
                self.ticker_callbacks.clear()
            logger.info("🧹 Callback lists cleared before resubscription")
            
            # Resubscribe orderbook — restore callback + emit to server
            for sub in self._subscriptions["orderbook"]:
                try:
                    cb = sub.get('callback')
                    if cb:
                        with self._callbacks_lock:
                            if cb not in self.orderbook_callbacks:
                                self.orderbook_callbacks.append(cb)
                    self.sio.emit(
                        self.EVENT_ORDERBOOK,
                        {'event': 'subscribe', 'pair': sub['pair']},
                        namespace=self.NAMESPACE
                    )
                    logger.info(f"✓ Resubscribed to orderbook: {sub['pair']}")
                except Exception as e:
                    logger.error(f"Error resubscribing orderbook {sub['pair']}: {e}")
            
            # Resubscribe candlestick — restore interval-keyed callback + emit to server
            for sub in self._subscriptions["candlestick"]:
                try:
                    cb = sub.get('callback')
                    interval_key = str(sub.get('interval', ''))
                    if cb and interval_key:
                        with self._callbacks_lock:
                            if interval_key not in self.candlestick_callbacks:
                                self.candlestick_callbacks[interval_key] = []
                            if cb not in self.candlestick_callbacks[interval_key]:
                                self.candlestick_callbacks[interval_key].append(cb)
                    self.sio.emit(
                        self.EVENT_CANDLESTICK,
                        {'event': 'subscribe', 'pair': sub['pair']},
                        namespace=self.NAMESPACE
                    )
                    logger.info(f"✓ Resubscribed to candlestick: {sub['pair']}")
                except Exception as e:
                    logger.error(f"Error resubscribing candlestick {sub['pair']}: {e}")
            
            # Resubscribe trades — restore callback + emit to server
            for sub in self._subscriptions["trades"]:
                try:
                    cb = sub.get('callback')
                    if cb:
                        with self._callbacks_lock:
                            if cb not in self.trades_callbacks:
                                self.trades_callbacks.append(cb)
                    self.sio.emit(
                        self.EVENT_TRADES,
                        {'event': 'subscribe', 'pair': sub['pair']},
                        namespace=self.NAMESPACE
                    )
                    logger.info(f"✓ Resubscribed to trades: {sub['pair']}")
                except Exception as e:
                    logger.error(f"Error resubscribing trades {sub['pair']}: {e}")
            
            # Resubscribe ticker — restore callback + emit to server
            for sub in self._subscriptions["ticker"]:
                try:
                    cb = sub.get('callback')
                    if cb:
                        with self._callbacks_lock:
                            if cb not in self.ticker_callbacks:
                                self.ticker_callbacks.append(cb)
                    self.sio.emit(
                        self.EVENT_TICKER,
                        {'event': 'subscribe', 'pair': sub['pair']},
                        namespace=self.NAMESPACE
                    )
                    logger.info(f"✓ Resubscribed to ticker: {sub['pair']}")
                except Exception as e:
                    logger.error(f"Error resubscribing ticker {sub['pair']}: {e}")
            
            logger.info("🔄 Resubscription complete")

    def connect(self, timeout: int = 30) -> bool:
        """Connect to WebSocket server with timeout"""
        try:
            logger.info(f"Connecting to {self.BASE_URL} with namespace {self.NAMESPACE}...")
            self.sio.connect(
                url=self.BASE_URL,
                namespaces=[self.NAMESPACE],
                transports='websocket',
                socketio_path=self.HANDSHAKE_PATH,
                wait=True,
                wait_timeout=timeout
            )
            return self.is_connected
        except Exception as e:
            logger.error(f"Connection error: {e}", exc_info=True)
            return False

    def disconnect(self):
        """Gracefully disconnect from WebSocket"""
        try:
            self.stop_event.set()
            if self.sio.connected:
                self.sio.disconnect()
            logger.info("Disconnected successfully")
        except Exception as e:
            logger.error(f"Disconnect error: {e}", exc_info=True)

    def is_healthy(self, timeout_seconds: int = 30) -> bool:
        """
        Check if connection is healthy (receiving data).
        Returns False if no messages received in timeout period.
        """
        if not self.is_connected:
            return False
        
        if self._last_message_time is None:
            return True  # Just connected, no messages yet
        
        time_since_last = datetime.now() - self._last_message_time
        return time_since_last.total_seconds() < timeout_seconds

    def subscribe_orderbook(self, pair: str, callback: Callable = None):
        """Subscribe to order book updates (thread-safe)"""
        subscribe_data = {'event': 'subscribe', 'pair': pair}
        
        if callback:
            with self._callbacks_lock:
                if callback not in self.orderbook_callbacks:
                    self.orderbook_callbacks.append(callback)
        
        # Store subscription AND callback for auto-resubscribe after reconnect
        with self._subscriptions_lock:
            existing = [s for s in self._subscriptions["orderbook"] if s['pair'] == pair]
            if not existing:
                self._subscriptions["orderbook"].append({'pair': pair, 'callback': callback})
            elif callback and existing[0].get('callback') is None:
                existing[0]['callback'] = callback
        
        logger.info(f"Subscribing to orderbook: {pair}")
        self.sio.emit(self.EVENT_ORDERBOOK, subscribe_data, namespace=self.NAMESPACE)

    def subscribe_candlestick(self, pair: str, interval: int = 5, callback: Callable = None):
        """Subscribe to candlestick updates (thread-safe, interval-routed)"""
        pair_with_interval = f"{pair}_{interval}"
        subscribe_data = {'event': 'subscribe', 'pair': pair_with_interval}
        
        # Register callback under its interval key
        if callback:
            interval_key = str(interval)
            with self._callbacks_lock:
                if interval_key not in self.candlestick_callbacks:
                    self.candlestick_callbacks[interval_key] = []
                if callback not in self.candlestick_callbacks[interval_key]:
                    self.candlestick_callbacks[interval_key].append(callback)
        
        # Store subscription AND callback for auto-resubscribe after reconnect
        with self._subscriptions_lock:
            existing = [s for s in self._subscriptions["candlestick"] if s['pair'] == pair_with_interval]
            if not existing:
                self._subscriptions["candlestick"].append({
                    'pair': pair_with_interval,
                    'interval': interval,
                    'callback': callback,
                })
            elif callback and existing[0].get('callback') is None:
                existing[0]['callback'] = callback
        
        logger.info(f"Subscribing to candlestick: {pair_with_interval} (interval_key={interval})")
        self.sio.emit(self.EVENT_CANDLESTICK, subscribe_data, namespace=self.NAMESPACE)

    def subscribe_trades(self, pair: str, callback: Callable = None):
        """Subscribe to trade updates (thread-safe)"""
        subscribe_data = {'event': 'subscribe', 'pair': pair}
        
        if callback:
            with self._callbacks_lock:
                if callback not in self.trades_callbacks:
                    self.trades_callbacks.append(callback)
        
        # Store subscription AND callback for auto-resubscribe after reconnect
        with self._subscriptions_lock:
            existing = [s for s in self._subscriptions["trades"] if s['pair'] == pair]
            if not existing:
                self._subscriptions["trades"].append({'pair': pair, 'callback': callback})
            elif callback and existing[0].get('callback') is None:
                existing[0]['callback'] = callback
        
        logger.info(f"Subscribing to trades: {pair}")
        self.sio.emit(self.EVENT_TRADES, subscribe_data, namespace=self.NAMESPACE)

    def subscribe_ticker(self, pair: str, callback: Callable = None):
        """Subscribe to ticker updates (thread-safe)"""
        subscribe_data = {'event': 'subscribe', 'pair': pair}
        
        if callback:
            with self._callbacks_lock:
                if callback not in self.ticker_callbacks:
                    self.ticker_callbacks.append(callback)
        
        # Store subscription AND callback for auto-resubscribe after reconnect
        with self._subscriptions_lock:
            existing = [s for s in self._subscriptions["ticker"] if s['pair'] == pair]
            if not existing:
                self._subscriptions["ticker"].append({'pair': pair, 'callback': callback})
            elif callback and existing[0].get('callback') is None:
                existing[0]['callback'] = callback
        
        logger.info(f"Subscribing to ticker: {pair}")
        self.sio.emit(self.EVENT_TICKER, subscribe_data, namespace=self.NAMESPACE)

    def wait(self):
        """Keep connection alive"""
        try:
            self.sio.wait()
        except KeyboardInterrupt:
            logger.info("Shutting down WebSocket...")
            self.disconnect()

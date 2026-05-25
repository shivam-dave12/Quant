"""Single-session ICICI Breeze websocket hub for the ICICI options desk.

The desk uses Breeze websocket feeds as the primary live-data transport while
REST is retained only for startup history, periodic reconciliation and
execution-time price validation.  A single SDK socket is shared by the NIFTY
underlying manager and the selected option-premium manager so the desk does
not multiply broker connections or duplicate subscriptions.

Breeze's official SDK owns feed token resolution and subscription formatting;
we deliberately do not maintain hard-coded script/token mappings here.
"""
from __future__ import annotations

import logging
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _Subscription:
    subscription_id: str
    kind: str
    args: dict[str, Any]
    callback: Callable[[Any], None]


class BreezeLiveFeedHub:
    """Thread-safe fan-out around one official Breeze websocket connection."""

    def __init__(self, api) -> None:
        self.api = api
        self._lock = threading.RLock()
        self._client = None
        self._subscriptions: dict[str, _Subscription] = {}
        self._connected_at = 0.0
        self._last_tick_ts = 0.0
        self._last_reconnect_ts = 0.0

    @property
    def connected(self) -> bool:
        with self._lock:
            return self._client is not None and self._sdk_connected(self._client)

    @property
    def last_tick_ts(self) -> float:
        with self._lock:
            return float(self._last_tick_ts or 0.0)

    def connect(self) -> None:
        with self._lock:
            if self._client is not None:
                return
            try:
                from breeze_connect import BreezeConnect  # type: ignore
            except Exception as exc:
                raise RuntimeError(
                    "ICICI websocket requires the official dependency breeze-connect; "
                    "install breeze-connect before enabling live ICICI trading"
                ) from exc

            session = self.api.auth.get_session(force_refresh=False)
            api_session = str(getattr(session, "api_session", "") or "").strip()
            if not api_session:
                configured = getattr(self.api.auth, "_configured_api_session", None)
                api_session = str(configured() if callable(configured) else "").strip()
            if not api_session:
                raise RuntimeError(
                    "ICICI websocket requires today's API_Session so the official Breeze SDK "
                    "can establish live feeds; generate the Telegram OTP session first"
                )

            client = BreezeConnect(api_key=self.api.auth.api_key)
            client.generate_session(api_secret=self.api.auth.secret_key, session_token=api_session)
            client.on_ticks = self._dispatch
            client.ws_connect()
            self._client = client
            self._connected_at = time.time()
            logger.info("ICICI Breeze official websocket connected; live feed hub active")

    def subscribe_underlying_quotes(self, *, exchange_code: str, stock_code: str, callback: Callable[[Any], None]) -> str:
        args = {
            "exchange_code": str(exchange_code).upper(),
            "stock_code": str(stock_code).upper(),
            "product_type": "cash",
            "get_market_depth": False,
            "get_exchange_quotes": True,
        }
        return self._subscribe("underlying_quote", args, callback)

    def subscribe_option_quotes_and_ohlcv(self, *, stock_code: str, expiry_date: str, strike_price: str, right: str, callback: Callable[[Any], None]) -> list[str]:
        base = {
            "exchange_code": "NFO",
            "stock_code": str(stock_code).upper(),
            "expiry_date": str(expiry_date),
            "strike_price": str(strike_price),
            "right": str(right).lower(),
            "product_type": "options",
            "get_market_depth": False,
            "get_exchange_quotes": True,
        }
        quote_id = self._subscribe("option_quote", dict(base), callback)
        depth = dict(base)
        # Breeze publishes option top-of-book depth on the market-depth channel,
        # separate from exchange quotes. Combining both flags is not the
        # documented SDK methodology and can silently degrade book data.
        depth["get_market_depth"] = True
        depth["get_exchange_quotes"] = False
        depth_id = self._subscribe("option_depth", depth, callback)
        ohlcv = dict(base)
        # Official OHLCV interval stream is a quote interval feed; executable
        # bid/ask depth is sourced only from the separate depth subscription.
        ohlcv["interval"] = "1minute"
        ohlcv_id = self._subscribe("option_ohlcv", ohlcv, callback)
        return [quote_id, depth_id, ohlcv_id]

    def _subscribe(self, kind: str, args: dict[str, Any], callback: Callable[[Any], None]) -> str:
        self.connect()
        with self._lock:
            subscription_id = f"{kind}:{uuid.uuid4().hex}"
            assert self._client is not None
            response = self._client.subscribe_feeds(**args)
            self._subscriptions[subscription_id] = _Subscription(subscription_id, kind, dict(args), callback)
            logger.info("ICICI websocket subscribed kind=%s contract=%s response=%s", kind, self._safe_descriptor(args), response)
            return subscription_id

    def reconnect_and_resubscribe(self, *, reason: str = "stale_stream") -> bool:
        """Reconnect the official SDK socket and replay all active subscriptions."""
        with self._lock:
            if not self._subscriptions:
                return self.connected
            old_client = self._client
            self._client = None
            self._connected_at = 0.0
            self._last_tick_ts = 0.0
            subscriptions = list(self._subscriptions.values())
        if old_client is not None:
            try:
                old_client.ws_disconnect()
            except Exception as exc:
                logger.debug("ICICI websocket disconnect before reconnect failed: %s", exc)
        try:
            self.connect()
            with self._lock:
                client = self._client
                subscriptions = list(self._subscriptions.values())
            if client is None:
                raise RuntimeError("official Breeze websocket client did not reconnect")
            for sub in subscriptions:
                response = client.subscribe_feeds(**sub.args)
                logger.info(
                    "ICICI websocket resubscribed kind=%s contract=%s reason=%s response=%s",
                    sub.kind, self._safe_descriptor(sub.args), reason, response,
                )
            with self._lock:
                self._last_reconnect_ts = time.time()
            return True
        except Exception as exc:
            logger.error("ICICI websocket reconnect/resubscribe failed reason=%s: %s", reason, exc)
            return False

    def ensure_live(self, *, max_stale_sec: float, reason: str = "stale_stream") -> bool:
        """Best-effort health check for the shared socket transport."""
        now = time.time()
        with self._lock:
            if not self._subscriptions:
                return self.connected
            client_missing = self._client is None or not self._sdk_connected(self._client)
            last_tick = float(self._last_tick_ts or 0.0)
            connected_at = float(self._connected_at or 0.0)
            stale = bool(last_tick and now - last_tick > max_stale_sec)
            silent_after_connect = bool(not last_tick and connected_at and now - connected_at > max_stale_sec)
        if client_missing or stale or silent_after_connect:
            return self.reconnect_and_resubscribe(reason=reason)
        return True

    def unsubscribe(self, subscription_ids: list[str] | tuple[str, ...]) -> None:
        with self._lock:
            client = self._client
            targets = [self._subscriptions.pop(sid, None) for sid in subscription_ids]
        if client is None:
            return
        for sub in targets:
            if sub is None:
                continue
            try:
                client.unsubscribe_feeds(**sub.args)
            except Exception as exc:
                logger.debug("ICICI websocket unsubscribe failed kind=%s: %s", sub.kind, exc)
        self._disconnect_if_idle()

    def _disconnect_if_idle(self) -> None:
        with self._lock:
            if self._subscriptions or self._client is None:
                return
            client, self._client = self._client, None
        try:
            client.ws_disconnect()
        except Exception:
            pass
        logger.info("ICICI Breeze websocket disconnected; no remaining feed subscriptions")

    def _dispatch(self, tick: Any) -> None:
        with self._lock:
            self._last_tick_ts = time.time()
            callbacks = [sub.callback for sub in self._subscriptions.values()]
        for callback in callbacks:
            try:
                callback(tick)
            except Exception as exc:
                logger.debug("ICICI websocket consumer rejected tick: %s", exc)

    @staticmethod
    def _safe_descriptor(args: dict[str, Any]) -> str:
        return "/".join(str(args.get(k, "")) for k in ("exchange_code", "stock_code", "expiry_date", "strike_price", "right", "interval") if args.get(k, "") not in (None, ""))

    @staticmethod
    def _sdk_connected(client: Any) -> bool:
        for attr in ("sio", "_sio", "socketio", "_socketio"):
            endpoint = getattr(client, attr, None)
            connected = getattr(endpoint, "connected", None)
            if connected is not None:
                return bool(connected)
        return client is not None


def hub_for_api(api) -> BreezeLiveFeedHub:
    """Return the single websocket hub associated with a Breeze REST client."""
    hub = getattr(api, "_live_feed_hub", None)
    if isinstance(hub, BreezeLiveFeedHub):
        return hub
    hub = BreezeLiveFeedHub(api)
    setattr(api, "_live_feed_hub", hub)
    return hub

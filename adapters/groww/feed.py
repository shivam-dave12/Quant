"""Groww feed bridge for index, selected-option LTP/depth, FNO order and position updates."""
from __future__ import annotations

import asyncio
import threading
from typing import Any, AsyncIterator


class GrowwFeedBridge:
    """Bridge the blocking official SDK feed consumer into bounded async platform events.

    The desk subscribes only to the underlying and selected directional option candidates, not
    the entire option universe. Payloads are preserved for validation/attribution upstream.
    """

    def __init__(self, groww_client: Any, *, feed_client: Any | None = None) -> None:
        if feed_client is None:
            try:
                from growwapi import GrowwFeed
            except ImportError as exc:
                raise RuntimeError("growwapi SDK is required for Groww feed") from exc
            feed_client = GrowwFeed(groww_client)
        self.feed = feed_client
        self._loop: asyncio.AbstractEventLoop | None = None
        self._queue: asyncio.Queue[dict[str, Any]] | None = None
        self._thread: threading.Thread | None = None

    def _push(self, payload: dict[str, Any]) -> None:
        if self._loop is None or self._queue is None:
            raise RuntimeError("GROWW_FEED_STREAM_NOT_STARTED")

        def put_now() -> None:
            assert self._queue is not None
            if self._queue.full():
                self._queue.get_nowait()
            self._queue.put_nowait(payload)

        self._loop.call_soon_threadsafe(put_now)

    async def stream(
        self,
        *,
        index_tokens: list[dict[str, str]],
        derivative_tokens: list[dict[str, str]],
        ltp_tokens: list[dict[str, str]] | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        self._loop = asyncio.get_running_loop()
        self._queue = asyncio.Queue(maxsize=4096)
        ltp_tokens = ltp_tokens or derivative_tokens

        def callback(meta: dict[str, Any]) -> None:
            feed_type = str(meta.get("feed_type", ""))
            if feed_type == "index_value":
                self._push({"type": "index", "meta": meta, "data": self.feed.get_index_value()})
            elif feed_type == "ltp":
                self._push({"type": "ltp", "meta": meta, "data": self.feed.get_ltp()})
            elif feed_type == "market_depth":
                self._push({"type": "depth", "meta": meta, "data": self.feed.get_market_depth()})
            elif feed_type == "order_updates":
                self._push({"type": "order", "meta": meta, "data": self.feed.get_fno_order_update()})
            elif feed_type == "position_updates":
                self._push({"type": "position", "meta": meta, "data": self.feed.get_fno_position_update()})

        if index_tokens:
            self.feed.subscribe_index_value(index_tokens, on_data_received=callback)
        if ltp_tokens:
            self.feed.subscribe_ltp(ltp_tokens, on_data_received=callback)
        if derivative_tokens:
            self.feed.subscribe_market_depth(derivative_tokens, on_data_received=callback)
        self.feed.subscribe_fno_order_updates(on_data_received=callback)
        self.feed.subscribe_fno_position_updates(on_data_received=callback)
        self._thread = threading.Thread(target=self.feed.consume, daemon=True, name="groww-feed-consumer")
        self._thread.start()
        try:
            while True:
                assert self._queue is not None
                yield await self._queue.get()
        finally:
            if index_tokens:
                self.feed.unsubscribe_index_value(index_tokens)
            if ltp_tokens:
                self.feed.unsubscribe_ltp(ltp_tokens)
            if derivative_tokens:
                self.feed.unsubscribe_market_depth(derivative_tokens)
            self.feed.unsubscribe_fno_order_updates()
            self.feed.unsubscribe_fno_position_updates()

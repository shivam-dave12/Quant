"""Groww underlying chart manager for Indian option strategies."""

from __future__ import annotations

import logging
import time
from typing import Any

try:
    import config
except Exception:  # pragma: no cover
    config = None  # type: ignore

from exchanges.icici.underlying_data_manager import ICICIUnderlyingDataManager
from .api import GrowwRestClient
from .live_feed import hub_for_api

logger = logging.getLogger(__name__)


def _gcfg(name: str, default: Any) -> Any:
    if config is None:
        return default
    return getattr(config, name, getattr(config, name.replace("GROWW_", "ICICI_", 1), default))


class GrowwUnderlyingDataManager(ICICIUnderlyingDataManager):
    def __init__(self, instrument, api: GrowwRestClient | None = None) -> None:
        super().__init__(instrument=instrument, api=api or GrowwRestClient())
        logger.info(
            "GrowwUnderlyingDataManager initialised [%s -> underlying=%s]",
            getattr(instrument, "asset_id", "GROWW"),
            self._underlying_code(),
        )

    def _start_websocket(self) -> bool:
        if not bool(_gcfg("GROWW_INDEX_STREAM_ENABLED", True)):
            logger.error("Groww underlying websocket disabled by configuration for %s", self._display_underlying())
            return False
        if self._stream_subscription_ids:
            return True
        try:
            hub = hub_for_api(self.api)
            self._first_stream_tick.clear()
            self._stream_armed_at = time.time()
            subscription_id = hub.subscribe_underlying_quotes(
                exchange_code=self._underlying_exchange(),
                stock_code=self._underlying_code(),
                callback=self._on_stream_candle,
            )
            self._sio = hub
            self._stream_subscription_ids = [subscription_id]
            timeout = max(0.0, float(_gcfg("GROWW_INDEX_STREAM_FIRST_TICK_TIMEOUT_SEC", 12.0)))
            if bool(_gcfg("GROWW_INDEX_WEBSOCKET_REQUIRED", True)) and timeout > 0 and not self._first_stream_tick.wait(timeout):
                hub.unsubscribe(self._stream_subscription_ids)
                self._stream_subscription_ids = []
                self._sio = None
                logger.error("Groww underlying websocket subscribed but delivered no live tick within %.1fs for %s", timeout, self._display_underlying())
                return False
            logger.info("Groww underlying websocket LIVE for %s via official Groww Feed LTP", self._display_underlying())
            return True
        except Exception as exc:
            logger.error("Groww mandatory underlying websocket unavailable for %s: %s", self._display_underlying(), exc)
            return False

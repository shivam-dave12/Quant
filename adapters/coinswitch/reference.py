"""CoinSwitch Futures reference-only adapter and public market-data socket.

The desk may read validated instrument metadata and public snapshots/trades/ticker updates,
but any order submission is prohibited in this deployment stage.
"""
from __future__ import annotations
import asyncio
from dataclasses import dataclass
import time
import urllib.parse
from typing import Any, AsyncIterator
import requests
from core.identifiers import InstrumentMapping

@dataclass(frozen=True)
class CoinSwitchReferenceInstrument:
    symbol: str
    mapping: InstrumentMapping
    maker_fee_bps: float
    taker_fee_bps: float
    raw: dict[str, Any]

class CoinSwitchReferenceAdapter:
    execution_enabled = False
    BASE_URL = "https://coinswitch.co"
    def __init__(self, *, api_key: str = "", secret_key: str = "", session: Any | None = None, timeout: float = 10.0) -> None:
        self.api_key, self.secret_key = api_key, secret_key
        self.session, self.timeout = session or requests.Session(), timeout
    def _signed_get(self, path: str, params: dict[str, str]) -> dict[str, Any]:
        if not self.api_key or not self.secret_key:
            raise RuntimeError("COINSWITCH_METADATA_REQUIRES_API_CREDENTIALS_FOR_COMPARABILITY_VALIDATION")
        try:
            from cryptography.hazmat.primitives.asymmetric import ed25519
        except ImportError as exc:
            raise RuntimeError("cryptography package required for CoinSwitch metadata validation") from exc
        query = urllib.parse.urlencode(params)
        final_path = urllib.parse.unquote_plus(path + ("?" + query if query else ""))
        epoch = str(int(time.time() * 1000))
        signed = "GET" + final_path + epoch
        key = ed25519.Ed25519PrivateKey.from_private_bytes(bytes.fromhex(self.secret_key))
        headers = {"Content-Type": "application/json", "X-AUTH-APIKEY": self.api_key,
                   "X-AUTH-SIGNATURE": key.sign(signed.encode("utf-8")).hex(), "X-AUTH-EPOCH": epoch}
        response = self.session.get(self.BASE_URL + final_path, headers=headers, timeout=self.timeout)
        response.raise_for_status()
        return dict(response.json())
    def verified_instrument(self, symbol: str = "BTCUSDT", exchange: str = "EXCHANGE_2") -> CoinSwitchReferenceInstrument:
        payload = self._signed_get("/trade/api/v2/futures/instrument_info", {"exchange": exchange})
        row = dict((payload.get("data") or {}).get(symbol.upper()) or {})
        if str(row.get("status", "")).upper() != "TRADING" or str(row.get("type", "")).upper() != "PERPETUAL_FUTURES":
            raise RuntimeError(f"COINSWITCH_REFERENCE_INSTRUMENT_NOT_VERIFIED:{symbol}")
        tick = float(row.get("tick_size", 0) or 0) * (10 ** -int(row.get("price_precision", 0) or 0))
        step = float(row.get("base_quantity_step_size", 0) or 0)
        mapping = InstrumentMapping("coinswitch", symbol.upper(), str(row.get("base_asset", "")).upper(), "perp",
            str(row.get("quote_asset", "")).upper(), 1.0, str(row.get("quote_asset", "")).upper(), tick, step, False, "linear_base", row)
        mapping.validate()
        return CoinSwitchReferenceInstrument(symbol.upper(), mapping, float(row.get("maker_fee_rate", 0) or 0) * 10_000,
            float(row.get("taker_fee_rate", 0) or 0) * 10_000, row)
    def place_order(self, *_: Any, **__: Any) -> None:
        raise RuntimeError("COINSWITCH_REFERENCE_ONLY_EXECUTION_PROHIBITED")

class CoinSwitchFuturesPublicFeed:
    """Public Socket.IO market data; emits only market snapshots/ticker/trades."""
    namespace = "/exchange_2"
    socketio_path = "/pro/realtime-rates-socket/futures/exchange_2"
    base_url = "wss://ws.coinswitch.co/"
    events = ("FETCH_ORDER_BOOK_CS_PRO", "FETCH_TRADES_CS_PRO", "FETCH_TICKER_INFO_CS_PRO")
    def __init__(self, symbol: str = "BTCUSDT", *, client: Any | None = None) -> None:
        self.symbol = symbol.upper(); self._client = client
    async def stream(self) -> AsyncIterator[dict[str, Any]]:
        try:
            import socketio
        except ImportError as exc:
            raise RuntimeError("python-socketio package required for CoinSwitch Futures feed") from exc
        sio = self._client or socketio.AsyncClient(reconnection=True)
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=4096)
        for event in self.events:
            async def handler(data: Any, event_name: str = event) -> None:
                item = {"event": event_name, "data": data}
                if queue.full():
                    queue.get_nowait()
                await queue.put(item)
            sio.on(event, handler=handler, namespace=self.namespace)
        await sio.connect(self.base_url, namespaces=[self.namespace], transports=["websocket"], socketio_path=self.socketio_path, wait=True, wait_timeout=30)
        for event in self.events:
            await sio.emit(event, {"event": "subscribe", "pair": self.symbol}, namespace=self.namespace)
        try:
            while True:
                yield await queue.get()
        finally:
            await sio.disconnect()

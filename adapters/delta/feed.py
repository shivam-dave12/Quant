"""Delta India public WebSocket feed using current public channels and heartbeat protocol."""
from __future__ import annotations

import json
from typing import Any, AsyncIterator


class DeltaPublicFeed:
    """Read-only market feed; it does not expose any private execution operation."""

    url = "wss://public-socket.india.delta.exchange"

    def __init__(self, symbols: list[str]) -> None:
        self.symbols = [str(symbol).upper() for symbol in symbols]
        if not self.symbols:
            raise ValueError("DELTA_PUBLIC_FEED_REQUIRES_SYMBOLS")

    def subscription(self) -> dict[str, Any]:
        return {
            "type": "subscribe",
            "payload": {
                "channels": [
                    {"name": "ob_l2", "symbols": self.symbols},
                    {"name": "trades", "symbols": self.symbols},
                    {"name": "v2/ticker", "symbols": self.symbols},
                    {"name": "funding_rate", "symbols": self.symbols},
                ]
            },
        }

    @staticmethod
    def heartbeat_enable() -> dict[str, str]:
        return {"type": "enable_heartbeat"}

    async def stream(self) -> AsyncIterator[dict[str, Any]]:
        try:
            import websockets
        except ImportError as exc:
            raise RuntimeError("websockets package required for Delta feed") from exc
        async with websockets.connect(self.url, ping_interval=20, ping_timeout=35) as websocket:
            await websocket.send(json.dumps(self.subscription()))
            await websocket.send(json.dumps(self.heartbeat_enable()))
            async for raw in websocket:
                yield json.loads(raw)

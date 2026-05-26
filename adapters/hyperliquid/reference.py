"""Read-only Hyperliquid BTC reference adapter with verified instrument metadata."""
from __future__ import annotations

import json
from decimal import Decimal
from typing import Any, AsyncIterator

import requests

from core.identifiers import InstrumentMapping


class HyperliquidReferenceAdapter:
    """Reference-only BTC market-data client; no order method can submit execution."""

    ws_url = "wss://api.hyperliquid.xyz/ws"
    info_url = "https://api.hyperliquid.xyz/info"
    execution_enabled = False

    def __init__(self, symbol: str = "BTC", *, session: Any | None = None, timeout: float = 10.0) -> None:
        self.symbol = str(symbol).upper()
        self.session = session or requests.Session()
        self.timeout = timeout

    def _info(self, request: dict[str, Any]) -> Any:
        response = self.session.post(self.info_url, json=request, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def verified_mapping(self) -> InstrumentMapping:
        """Load size precision and observed price grid from official info responses.

        Hyperliquid publishes size decimal precision in metadata. The executable price
        grid varies by magnitude; this read-only mapping records the smallest observed
        current L2 price increment and fails closed if it cannot be demonstrated.
        """
        metadata = self._info({"type": "meta"})
        universe = metadata.get("universe", []) if isinstance(metadata, dict) else []
        product = next((row for row in universe if str(row.get("name", "")).upper() == self.symbol), None)
        if not product or product.get("isDelisted", False):
            raise RuntimeError(f"HYPERLIQUID_VALID_PRODUCT_NOT_FOUND:{self.symbol}")
        size_decimals = int(product.get("szDecimals", -1))
        if size_decimals < 0:
            raise RuntimeError(f"HYPERLIQUID_SIZE_PRECISION_MISSING:{self.symbol}")
        book = self._info({"type": "l2Book", "coin": self.symbol})
        levels = book.get("levels", []) if isinstance(book, dict) else []
        prices = sorted({Decimal(str(row["px"])) for side in levels for row in side if row.get("px") is not None})
        increments = [prices[index] - prices[index - 1] for index in range(1, len(prices)) if prices[index] > prices[index - 1]]
        if not increments:
            raise RuntimeError(f"HYPERLIQUID_PRICE_GRID_NOT_VERIFIABLE:{self.symbol}")
        mapping = InstrumentMapping(
            venue="hyperliquid",
            venue_symbol=self.symbol,
            canonical_underlying=self.symbol,
            product_class="perp",
            quote_currency="USDC",
            contract_multiplier=1.0,
            settlement_currency="USDC",
            price_tick=float(min(increments)),
            qty_step=float(Decimal("1").scaleb(-size_decimals)),
            execution_enabled=False,
            notional_formula="linear_base",
            metadata={"source": "hyperliquid_info_meta_l2book", "szDecimals": size_decimals},
        )
        mapping.validate()
        return mapping

    def subscriptions(self) -> list[dict[str, Any]]:
        return [
            {"method": "subscribe", "subscription": {"type": feed_type, "coin": self.symbol}}
            for feed_type in ("l2Book", "trades", "bbo")
        ]

    async def stream(self) -> AsyncIterator[dict[str, Any]]:
        try:
            import websockets
        except ImportError as exc:
            raise RuntimeError("websockets package required for Hyperliquid reference feed") from exc
        async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20) as websocket:
            for subscription in self.subscriptions():
                await websocket.send(json.dumps(subscription))
            async for raw in websocket:
                yield json.loads(raw)

    def place_order(self, *_: Any, **__: Any) -> None:
        raise RuntimeError("HYPERLIQUID_REFERENCE_ONLY_EXECUTION_PROHIBITED")

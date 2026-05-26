"""Verified instrument registry: execution requires live metadata provenance."""
from __future__ import annotations
from core.identifiers import InstrumentMapping

class InstrumentRegistry:
    def __init__(self) -> None:
        self._items: dict[tuple[str, str], InstrumentMapping] = {}
    def register(self, mapping: InstrumentMapping) -> None:
        mapping.validate(); self._items[(mapping.venue.lower(), mapping.venue_symbol.upper())] = mapping
    def get(self, venue: str, symbol: str) -> InstrumentMapping:
        try: return self._items[(venue.lower(), symbol.upper())]
        except KeyError as exc: raise KeyError(f"instrument not verified: {venue}:{symbol}") from exc
    def executable(self, venue: str) -> list[InstrumentMapping]:
        return [x for (v, _), x in self._items.items() if v == venue.lower() and x.execution_enabled]
    def comparable_btc_reference(self) -> list[InstrumentMapping]:
        return [x for x in self._items.values() if x.canonical_underlying == "BTC" and x.product_class in {"perp", "future", "spot"}]

"""Licensed/approved metal reference-price adapter; never fabricates XAU/XAG fair value."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Protocol

@dataclass(frozen=True)
class ReferencePrice:
    asset: str
    price_usd: float
    ts_ns: int
    source: str

class ApprovedReferenceProvider(Protocol):
    def get_price(self, asset: str) -> ReferencePrice: ...

class ReferenceMetalsAdapter:
    def __init__(self, provider: ApprovedReferenceProvider | None = None) -> None: self.provider = provider
    def get(self, asset: str) -> ReferencePrice:
        if self.provider is None: raise RuntimeError("APPROVED_METALS_REFERENCE_FEED_NOT_CONFIGURED")
        price = self.provider.get_price(asset.upper())
        if price.asset.upper() != asset.upper() or price.price_usd <= 0 or not price.source:
            raise RuntimeError("INVALID_APPROVED_METALS_REFERENCE_QUOTE")
        return price

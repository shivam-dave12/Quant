"""Factory that wires official venue feeds into the single institutional strategy authority."""
from __future__ import annotations
from typing import Callable, AsyncIterator, Any
import numpy as np
from adapters.delta.client import DeltaAdapter
from adapters.delta.feed import DeltaPublicFeed
from adapters.coinswitch.reference import CoinSwitchReferenceAdapter, CoinSwitchFuturesPublicFeed
from adapters.hyperliquid.reference import HyperliquidReferenceAdapter
from adapters.reference_metals.client import ReferenceMetalsAdapter
from adapters.groww.client import GrowwAdapter
from core.clock import Clock
from core.identifiers import VenueMicrostate
from intelligence.liquidity_intelligence import ZoneObservation
from intelligence.market_state import TradingRestrictionState
from market_data.instrument_registry import InstrumentRegistry
from market_data.venue_parsers import parse_coinswitch, parse_hyperliquid
from orchestration.verified_streams import BTCVerifiedMarketStream, MetalsVerifiedMarketStream, GrowwIndiaMarketStream, DeltaDeskRuntimeInputs

class VerifiedLiveFeedFactory:
    def __init__(self, *, delta: DeltaAdapter, coinswitch: CoinSwitchReferenceAdapter | None = None,
                 hyperliquid: HyperliquidReferenceAdapter | None = None, metals_reference: ReferenceMetalsAdapter | None = None,
                 groww: GrowwAdapter | None = None, clock_ns: Callable[[], int] | None = None) -> None:
        self.delta=delta; self.coinswitch=coinswitch; self.hyperliquid=hyperliquid or HyperliquidReferenceAdapter("BTC"); self.metals_reference=metals_reference; self.groww=groww
        self.clock_ns=clock_ns or Clock().now_ns; self.registry=InstrumentRegistry()
    def btc_stream(self, *, delta_symbol: str, inputs_provider: Callable[[VenueMicrostate], DeltaDeskRuntimeInputs], warmup_zones: list[ZoneObservation], model_lags: int = 5, restriction_provider: Callable[[str], TradingRestrictionState] | None = None) -> BTCVerifiedMarketStream:
        if not self.coinswitch: raise RuntimeError("COINSWITCH_REFERENCE_ADAPTER_REQUIRED")
        product=self.delta.executable_product(delta_symbol); cs=self.coinswitch.verified_instrument("BTCUSDT").mapping; hl=self.hyperliquid.verified_mapping()
        for mapping in (product.mapping,cs,hl): self.registry.register(mapping)
        return BTCVerifiedMarketStream(delta_product=product,delta_source=DeltaPublicFeed([product.mapping.venue_symbol]).stream(),reference_sources={"coinswitch":(cs,CoinSwitchFuturesPublicFeed("BTCUSDT").stream(),parse_coinswitch),"hyperliquid":(hl,self.hyperliquid.stream(),parse_hyperliquid)},inputs_provider=inputs_provider,clock_ns=self.clock_ns,warmup_zones=warmup_zones,model_lags=model_lags,restriction_provider=restriction_provider)
    def metals_stream(self, *, asset: str, delta_symbol: str, inputs_provider: Callable[[VenueMicrostate], DeltaDeskRuntimeInputs], warmup_zones: list[ZoneObservation], reference_feature_provider: Callable[[str], np.ndarray], restriction_provider: Callable[[str], TradingRestrictionState] | None = None) -> MetalsVerifiedMarketStream:
        if not self.metals_reference: raise RuntimeError("APPROVED_METALS_REFERENCE_FEED_NOT_CONFIGURED")
        product=self.delta.executable_product(delta_symbol); self.registry.register(product.mapping)
        return MetalsVerifiedMarketStream(asset=asset,product=product,source=DeltaPublicFeed([product.mapping.venue_symbol]).stream(),reference_provider=self.metals_reference.get,reference_feature_provider=reference_feature_provider,inputs_provider=inputs_provider,clock_ns=self.clock_ns,warmup_zones=warmup_zones,restriction_provider=restriction_provider)
    def india_stream(self, *, underlying_observations: AsyncIterator[dict[str, Any]], feature_builder: Callable, validated_fee_per_lot: float, stress_slippage_per_lot: float, available_cash: float, restriction_provider: Callable[[str], TradingRestrictionState] | None = None) -> GrowwIndiaMarketStream:
        if not self.groww: raise RuntimeError("GROWW_ADAPTER_REQUIRED")
        return GrowwIndiaMarketStream(adapter=self.groww,underlying_observations=underlying_observations,feature_builder=feature_builder,validated_fee_per_lot=validated_fee_per_lot,stress_slippage_per_lot=stress_slippage_per_lot,available_cash=available_cash,restriction_provider=restriction_provider)

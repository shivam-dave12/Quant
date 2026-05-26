"""Typed live observations and multi-desk coordinator."""
from __future__ import annotations
from dataclasses import dataclass
from typing import AsyncIterator, Any, Callable
import asyncio
import numpy as np
from adapters.delta.client import DeltaProduct
from adapters.groww.client import GrowwContract
from core.identifiers import CostEstimate, VenueMicrostate
from intelligence.liquidity_intelligence import ZoneObservation
from intelligence.market_state import TradingRestrictionState
from intelligence.option_contract_ranker import OptionChainCandidate

@dataclass(frozen=True)
class BTCObservation:
    ts_ns: int
    delta: VenueMicrostate
    references: dict[str, VenueMicrostate]
    lagged_model_features: np.ndarray
    ewma_volatility: float
    zone_observations: list[ZoneObservation]
    costs: CostEstimate
    delta_product: DeltaProduct
    available_margin: float
    liquidity_capacity_qty: float
    restriction: TradingRestrictionState | None = None

@dataclass(frozen=True)
class MetalsObservation:
    ts_ns: int
    asset: str
    local_state: VenueMicrostate
    fair_value: float
    reference_features: np.ndarray
    zone_observations: list[ZoneObservation]
    costs: CostEstimate
    delta_product: DeltaProduct
    available_margin: float
    liquidity_capacity_qty: float
    ewma_volatility: float
    restriction: TradingRestrictionState | None = None

@dataclass(frozen=True)
class IndiaObservation:
    ts_ns: int
    underlying: str
    current_level: float
    forward_price: float
    direction_features: np.ndarray
    regime_features: np.ndarray
    iv_features: np.ndarray
    volatility_points: float
    liquidity_rationale: dict[str, Any]
    load_candidates: Callable[[str], tuple[list[OptionChainCandidate], dict[str, GrowwContract]]]
    validated_fee_per_lot: float
    stress_slippage_per_lot: float
    available_cash: float
    restriction: TradingRestrictionState | None = None

class LiveCoordinator:
    def __init__(self, platform: Any) -> None: self.platform = platform; self.streams: list[AsyncIterator[Any]] = []
    def register_stream(self, stream: AsyncIterator[Any]) -> None: self.streams.append(stream)
    async def run(self, *, live: bool) -> None:
        if not self.streams: raise RuntimeError("NO_VERIFIED_DESK_STREAMS_REGISTERED")
        async def consume(stream: AsyncIterator[Any]) -> None:
            async for observation in stream:
                if isinstance(observation, BTCObservation): await self.platform.handle_btc(observation, live=live)
                elif isinstance(observation, MetalsObservation): await self.platform.handle_metals(observation, live=live)
                elif isinstance(observation, IndiaObservation): await self.platform.handle_india(observation, live=live)
                else: raise RuntimeError("UNKNOWN_VERIFIED_OBSERVATION_TYPE")
        await asyncio.gather(*(consume(stream) for stream in self.streams))

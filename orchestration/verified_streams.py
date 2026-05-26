"""Verified raw-feed bridges; direction, invalidation and contract selection remain in strategy authority."""
from __future__ import annotations
import asyncio
from collections import deque
from dataclasses import dataclass
from typing import Any, AsyncIterator, Callable
import numpy as np
from adapters.delta.client import DeltaProduct
from adapters.groww.client import GrowwAdapter, GrowwContract
from core.identifiers import CostEstimate, InstrumentMapping, VenueMicrostate
from intelligence.cross_venue_btc import BTCCompositeEngine
from intelligence.liquidity_intelligence import LiquidityPoolBook, ZoneObservation
from intelligence.option_contract_ranker import OptionChainCandidate
from intelligence.volatility import EWMAVolatility
from intelligence.market_state import TradingRestrictionState
from market_data.live_assembler import NormalisedVenueAssembler
from market_data.venue_parsers import parse_delta
from orchestration.runtime import BTCObservation, MetalsObservation, IndiaObservation

@dataclass(frozen=True)
class DeltaDeskRuntimeInputs:
    fees_bps: float
    spread_cost_bps: float
    impact_bps: float
    protection_cost_bps: float
    uncertainty_bps: float
    available_margin: float
    liquidity_capacity_qty: float
    def validate(self) -> None:
        if min(self.fees_bps, self.spread_cost_bps, self.impact_bps, self.protection_cost_bps, self.uncertainty_bps) < 0 or min(self.available_margin, self.liquidity_capacity_qty) <= 0:
            raise ValueError("VALIDATED_DELTA_COST_AND_RISK_INPUTS_REQUIRED")
    def cost_without_modelled_slippage(self) -> CostEstimate:
        return CostEstimate(self.fees_bps, self.spread_cost_bps, 0.0, self.impact_bps, self.protection_cost_bps, 0.0, self.uncertainty_bps)

class BTCVerifiedMarketStream:
    def __init__(self, *, delta_product: DeltaProduct, delta_source: AsyncIterator[dict[str, Any]], reference_sources: dict[str, tuple[InstrumentMapping, AsyncIterator[dict[str, Any]], Callable]],
                 inputs_provider: Callable[[VenueMicrostate], DeltaDeskRuntimeInputs], clock_ns: Callable[[], int], warmup_zones: list[ZoneObservation], model_lags: int = 5, restriction_provider: Callable[[str], TradingRestrictionState] | None = None) -> None:
        if not warmup_zones: raise RuntimeError("BTC_HISTORICAL_LIQUIDITY_WARMUP_REQUIRED")
        self.product, self.delta_source, self.references = delta_product, delta_source, reference_sources; self.inputs_provider, self.clock_ns = inputs_provider, clock_ns
        self.restriction_provider = restriction_provider; self.assembler = NormalisedVenueAssembler(); self.states: dict[str, VenueMicrostate] = {}; self.delta_snapshot = None; self.heartbeat_seen = False
        self.poolbook = LiquidityPoolBook(); [self.poolbook.upsert(delta_product.mapping.venue_symbol, zone) for zone in warmup_zones]
        self.vol = EWMAVolatility(); self.previous_mid: float | None = None; self.feature_history: deque[np.ndarray] = deque(maxlen=model_lags+1); self.model_lags = model_lags
    async def _fan_in(self):
        queue: asyncio.Queue = asyncio.Queue(maxsize=8192)
        async def consume(name, mapping, source, parser):
            async for raw in source: await queue.put((name, raw, parser, mapping))
        tasks=[asyncio.create_task(consume("delta",self.product.mapping,self.delta_source,parse_delta))]
        tasks.extend(asyncio.create_task(consume(name,mapping,source,parser)) for name,(mapping,source,parser) in self.references.items())
        try:
            while True: yield await queue.get()
        finally:
            for task in tasks: task.cancel()
    def _apply(self, name: str, raw: dict[str, Any], parser: Callable, mapping: InstrumentMapping) -> None:
        now=self.clock_ns(); update=parser(raw,mapping,now)
        if name == "delta" and update.heartbeat: self.heartbeat_seen=True; return
        if update.trade: self.assembler.apply_trade(mapping, update.trade)
        if update.funding_rate is not None: self.assembler.apply_funding(mapping, update.funding_rate)
        if not update.snapshot: return
        status=self.assembler.apply_snapshot(update.snapshot, latency_ms=None if update.snapshot.exchange_ts_ns is None else (now-update.snapshot.exchange_ts_ns)/1_000_000.0,
            heartbeat_ok=self.heartbeat_seen if name=="delta" else True)
        if status.microstate and status.quality > 0:
            self.states[name]=status.microstate
            if name=="delta": self.delta_snapshot=update.snapshot
    async def __aiter__(self):
        composite_engine=BTCCompositeEngine()
        async for name,raw,parser,mapping in self._fan_in():
            self._apply(name,raw,parser,mapping)
            if not self.heartbeat_seen or "delta" not in self.states or not {"coinswitch","hyperliquid"}.intersection(self.states): continue
            delta=self.states["delta"]; refs={k:v for k,v in self.states.items() if k!="delta"}
            if self.previous_mid is not None: self.vol.update(float(np.log(delta.mid/self.previous_mid)))
            self.previous_mid=delta.mid; composite=composite_engine.build(delta, refs); self.feature_history.append(composite_engine.feature_vector(composite))
            if len(self.feature_history) < self.model_lags+1: continue
            inputs=self.inputs_provider(delta); inputs.validate()
            realtime=self.assembler.liquidity_zones(self.delta_snapshot) if self.delta_snapshot else []
            zones=[*self.poolbook.zones(delta.symbol), *realtime]
            yield BTCObservation(delta.receive_ts_ns, delta, refs, np.vstack(self.feature_history), max(self.vol.variance**0.5, 1e-8), zones,
                inputs.cost_without_modelled_slippage(), self.product, inputs.available_margin, inputs.liquidity_capacity_qty, self.restriction_provider("BTC") if self.restriction_provider else None)

class MetalsVerifiedMarketStream:
    def __init__(self, *, asset: str, product: DeltaProduct, source: AsyncIterator[dict[str, Any]], reference_provider: Callable[[str], Any],
                 reference_feature_provider: Callable[[str], np.ndarray], inputs_provider: Callable[[VenueMicrostate], DeltaDeskRuntimeInputs], clock_ns: Callable[[], int], warmup_zones: list[ZoneObservation], restriction_provider: Callable[[str], TradingRestrictionState] | None = None) -> None:
        if not warmup_zones: raise RuntimeError("METALS_HISTORICAL_LIQUIDITY_WARMUP_REQUIRED")
        self.asset, self.product, self.source, self.reference_provider, self.reference_feature_provider = asset, product, source, reference_provider, reference_feature_provider
        self.inputs_provider, self.clock_ns = inputs_provider, clock_ns; self.restriction_provider = restriction_provider; self.assembler=NormalisedVenueAssembler(); self.heartbeat_seen=False; self.poolbook=LiquidityPoolBook()
        [self.poolbook.upsert(product.mapping.venue_symbol, zone) for zone in warmup_zones]; self.vol=EWMAVolatility(); self.previous_mid=None
    async def __aiter__(self):
        async for raw in self.source:
            now=self.clock_ns(); update=parse_delta(raw,self.product.mapping,now)
            if update.heartbeat: self.heartbeat_seen=True; continue
            if update.trade: self.assembler.apply_trade(self.product.mapping,update.trade)
            if update.snapshot is None or not self.heartbeat_seen: continue
            status=self.assembler.apply_snapshot(update.snapshot,latency_ms=None if update.snapshot.exchange_ts_ns is None else (now-update.snapshot.exchange_ts_ns)/1_000_000,heartbeat_ok=True)
            if not status.microstate or status.quality <= 0: continue
            local=status.microstate
            if self.previous_mid: self.vol.update(float(np.log(local.mid/self.previous_mid)))
            self.previous_mid=local.mid; quote=self.reference_provider(self.asset); features=np.asarray(self.reference_feature_provider(self.asset),dtype=float)
            inputs=self.inputs_provider(local); inputs.validate()
            yield MetalsObservation(local.receive_ts_ns,self.asset,local,float(quote.price_usd),features,[*self.poolbook.zones(local.symbol),*self.assembler.liquidity_zones(update.snapshot)],inputs.cost_without_modelled_slippage(),self.product,inputs.available_margin,inputs.liquidity_capacity_qty,max(self.vol.variance**0.5,1e-8),self.restriction_provider(self.asset) if self.restriction_provider else None)

class GrowwIndiaMarketStream:
    """Produces underlying observations only; CE/PE candidates are loaded after model direction approval."""
    def __init__(self, *, adapter: GrowwAdapter, underlying_observations: AsyncIterator[dict[str, Any]], feature_builder: Callable[[dict[str, Any]], tuple[np.ndarray,np.ndarray,float,float,np.ndarray,dict[str,Any]]],
                 validated_fee_per_lot: float, stress_slippage_per_lot: float, available_cash: float, restriction_provider: Callable[[str], TradingRestrictionState] | None = None) -> None:
        self.adapter=adapter; self.source=underlying_observations; self.feature_builder=feature_builder; self.fee=validated_fee_per_lot; self.slippage=stress_slippage_per_lot; self.cash=available_cash; self.restriction_provider=restriction_provider
    def _loader(self, underlying: str, expiry: str, forward_price: float) -> Callable[[str], tuple[list[OptionChainCandidate], dict[str, GrowwContract]]]:
        def load(option_type: str):
            contracts=self.adapter.discover_contracts(underlying); current={c.trading_symbol:c for c in contracts if c.expiry==expiry and c.option_type==option_type}
            rows=self.adapter.rankable_option_rows(underlying=underlying,expiry=expiry,option_type=option_type,contracts=contracts,validated_fee_per_lot=self.fee,slippage_bps=0.0)
            candidates=[OptionChainCandidate(row["trading_symbol"],option_type,expiry,float(row["strike"]),float(row["bid"]),float(row["ask"]),int(row["executable_depth_qty"]),int(row["volume"]),int(row["open_interest"]),current[row["trading_symbol"]].lot_size,bool(row["protection_feasible"])) for row in rows]
            return candidates,current
        return load
    async def __aiter__(self):
        async for raw in self.source:
            direction_features, regime_features, current, forward, iv_features, rationale = self.feature_builder(raw)
            expiry=str(raw["valid_expiry"]); underlying=str(raw["underlying"])
            yield IndiaObservation(int(raw["ts_ns"]),underlying,current,forward,direction_features,regime_features,np.asarray(iv_features,dtype=float),float(raw["volatility_points"]),rationale,self._loader(underlying,expiry,forward),self.fee,self.slippage,self.cash,self.restriction_provider(underlying) if self.restriction_provider else None)

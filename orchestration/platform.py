"""Single institutional strategy authority: data -> models -> risk -> protected execution -> research."""
from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Any
import numpy as np
from core.config import PlatformConfig, CONFIG
from core.identifiers import DecisionCode, Direction, OpportunityDecision, ProtectionPlan
from core.feature_store import FeatureStore
from core.research_store import ResearchStore
from core.state_store import StateStore
from core.observability import Observability
from intelligence.cross_venue_btc import BTCCompositeEngine, BTCModelBundle
from intelligence.liquidity_intelligence import LiquidityIntelligence, LiquidityModelBundle, LiquidityFeatureBuilder
from intelligence.metals_fair_value import MetalsModelBundle
from intelligence.india_underlying_state import IndiaUnderlyingModelBundle
from intelligence.option_contract_ranker import LongOptionRanker
from intelligence.predictive_models import ExpectedIVChangeModel
from intelligence.edge_model import executable_edge
from portfolio.exposure import ExposureBook, Exposure
from portfolio.covariance import ShrunkEWMACovariance
from portfolio.expected_shortfall import ExpectedShortfallModel
from portfolio.drawdown import DrawdownController
from portfolio.allocator import PortfolioAllocator
from portfolio.leverage import DynamicLeverageSelector
from portfolio.margin import MarginPolicy
from execution.execution_policy import ExecutionPolicy
from execution.delta_protected_orders import DeltaProtectedExecutor
from execution.groww_long_option_execution import GrowwLongOptionExecutor
from execution.reconciliation import GrowwReconciler
from adapters.delta.client import DeltaAdapter
from adapters.groww.client import GrowwAdapter
from research.labels import ForwardLabelCoordinator, OutcomeCoordinator
from research.attribution import AttributionEngine
from research.shadow_models import ShadowPredictionRecorder, ShadowPrediction
from orchestration.runtime import BTCObservation, MetalsObservation, IndiaObservation

@dataclass
class ModelAuthority:
    btc: BTCModelBundle | None
    liquidity: LiquidityModelBundle | None
    metals: MetalsModelBundle | None
    india: IndiaUnderlyingModelBundle | None
    option_iv: ExpectedIVChangeModel | None = None

class InstitutionalPlatform:
    def __init__(self, config: PlatformConfig = CONFIG, *, models: ModelAuthority, covariance: ShrunkEWMACovariance,
                 expected_shortfall: ExpectedShortfallModel, observability: Observability | None = None,
                 groww_adapter: GrowwAdapter | None = None, delta_adapter: DeltaAdapter | None = None) -> None:
        self.config = config; config.ensure_directories(); self.obs = observability or Observability(); self.models = models
        self.features = FeatureStore(config.policy.research_dir); self.research = ResearchStore(config.policy.research_dir); self.state = StateStore(config.policy.state_dir)
        self.labels = ForwardLabelCoordinator(self.research, config.policy.forecast_horizons_seconds); self.outcomes = OutcomeCoordinator(self.research); self.attribution = AttributionEngine(self.research); self.shadow = ShadowPredictionRecorder(self.research); self.entries_halted = False
        self.liquidity = LiquidityIntelligence(models.liquidity); self.btc_composite = BTCCompositeEngine(); self.ranker = LongOptionRanker(
            max_spread_bps=config.policy.max_option_spread_bps, max_premium_risk=config.policy.max_option_premium_risk_inr,
            minimum_liquidity_score=config.policy.minimum_option_liquidity_score)
        self.book = ExposureBook(); self.covariance = covariance; self.expected_shortfall = expected_shortfall
        self.drawdown_usd = DrawdownController(config.policy.portfolio_risk_cap_usd, config.policy.desk_daily_loss_caps_usd)
        self.drawdown_inr = DrawdownController(config.policy.india_daily_loss_cap_inr, {"INDIA_OPTIONS": config.policy.india_daily_loss_cap_inr})
        self.alloc_usd = PortfolioAllocator(exposure_book=self.book, covariance=covariance, expected_shortfall=expected_shortfall, drawdown=self.drawdown_usd,
            portfolio_risk_cap=config.policy.portfolio_risk_cap_usd, portfolio_es_cap=config.policy.portfolio_es_cap_usd,
            max_risk_per_opportunity=config.policy.max_risk_per_opportunity_usd, group_caps={"CRYPTO_BETA": 10.0, "METAL_BETA": 8.0})
        self.alloc_inr = PortfolioAllocator(exposure_book=self.book, covariance=covariance, expected_shortfall=expected_shortfall, drawdown=self.drawdown_inr,
            portfolio_risk_cap=config.policy.india_portfolio_risk_cap_inr, portfolio_es_cap=config.policy.india_portfolio_risk_cap_inr,
            max_risk_per_opportunity=config.policy.india_max_risk_per_opportunity_inr, group_caps={"INDIA_INDEX_DELTA": config.policy.india_portfolio_risk_cap_inr})
        self.policy = ExecutionPolicy(config.policy.minimum_net_edge_bps, config.policy.minimum_execution_quality); self.leverage = DynamicLeverageSelector()
        self.delta_executor = DeltaProtectedExecutor(delta_adapter, self.state, self.obs) if delta_adapter else None
        self.groww_executor = GrowwLongOptionExecutor(groww_adapter, self.state, self.obs) if groww_adapter else None
        self.groww_reconciler = GrowwReconciler(groww_adapter) if groww_adapter else None
    def _record(self, stream: str, payload: dict[str, Any]) -> None: self.research.append(stream, payload)
    def halt_new_entries(self, reason: str) -> None:
        self.entries_halted = True
        self.state.put("entry_halt", {"halted": True, "reason": reason})
        self.obs.critical("ENTRY_HALT", {"reason": reason})

    def resume_new_entries(self, reason: str) -> None:
        """Resume candidate approval only when broker protection reconciliation is safe."""
        if self.groww_executor is not None and self.groww_executor.entries_halted:
            raise RuntimeError("GROWW_RECONCILIATION_REQUIRED_BEFORE_RESUME")
        self.entries_halted = False
        self.state.put("entry_halt", {"halted": False, "reason": reason})
        self.obs.event("ENTRY_RESUME", {"reason": reason})
    def _halted(self, desk: str, instrument: str) -> OpportunityDecision | None:
        if not self.entries_halted: return None
        return self._decision(OpportunityDecision(desk, instrument, Direction.NO_TRADE.value, DecisionCode.NO_TRADE_EXECUTION_UNSAFE, 0.0, 0.0, False, None, ("NEW_ENTRIES_HALTED",), {}))
    def _decision(self, decision: OpportunityDecision) -> OpportunityDecision:
        self._record("decisions", decision.as_dict()); self.obs.event("DECISION", decision); return decision
    def reconcile_groww_startup(self) -> None:
        if not self.groww_reconciler: return
        result = self.groww_reconciler.run(); self.state.put("groww_reconciliation", asdict(result))
        if not result.safe_to_enter and self.groww_executor: self.groww_executor.entries_halted = True
    def _model_guard(self, bundle: Any, name: str) -> None:
        if bundle is None or not getattr(bundle, "promoted", False): raise RuntimeError(f"PROMOTED_{name}_MODEL_REQUIRED")
    def evaluate_btc(self, observation: BTCObservation) -> OpportunityDecision:
        self._record("observations", {"desk": "BTC", "ts_ns": observation.ts_ns, "delta": asdict(observation.delta), "references": {name: asdict(state) for name, state in observation.references.items()}, "costs": asdict(observation.costs), "restriction": asdict(observation.restriction) if observation.restriction else None})
        halted = self._halted("BTC", observation.delta.symbol)
        if observation.restriction is not None and not observation.restriction.tradable:
            return self._decision(OpportunityDecision("BTC", observation.delta.symbol, Direction.NO_TRADE.value, DecisionCode.NO_TRADE_EXECUTION_UNSAFE, 0.0, 0.0, False, None, (observation.restriction.reason or "TRADING_RESTRICTED",), {}))
        if halted: return halted
        composite = self.btc_composite.build(observation.delta, observation.references); self.labels.on_delta_mid(observation.ts_ns, observation.delta.mid); self.outcomes.on_price(ts_ns=observation.ts_ns, instrument=observation.delta.symbol, price=observation.delta.mid)
        self.labels.register_delta_observation(ts_ns=observation.ts_ns, mid=observation.delta.mid, cost=observation.costs)
        vector = self.btc_composite.feature_vector(composite); self.features.append({"desk":"BTC", "ts_ns":observation.ts_ns, "features":vector.tolist(), "delta":asdict(observation.delta)})
        if composite.delta_execution_quality_score < self.config.policy.minimum_execution_quality:
            return self._decision(OpportunityDecision("BTC", observation.delta.symbol, Direction.NO_TRADE.value, DecisionCode.NO_TRADE_EXECUTION_UNSAFE, 0.0, composite.delta_execution_quality_score, False, None, ("DELTA_LOCAL_EXECUTION_UNSAFE",), {"composite":asdict(composite)}))
        self._model_guard(self.models.btc, "BTC"); self._model_guard(self.models.liquidity, "LIQUIDITY")
        signed_gross, _, _, _ = self.models.btc.signed_forecast(lagged_features=observation.lagged_model_features, composite=composite, ewma_volatility=observation.ewma_volatility)
        direction = Direction.LONG.value if signed_gross > 0 else Direction.SHORT.value if signed_gross < 0 else Direction.NO_TRADE.value
        if direction == Direction.NO_TRADE.value: return self._decision(OpportunityDecision("BTC", observation.delta.symbol, direction, DecisionCode.NO_TRADE_INSUFFICIENT_EDGE, 0.0, composite.delta_execution_quality_score, False, None, ("MODEL_ZERO_DIRECTION",), {}))
        raw_zones = [self.liquidity.score(instrument=observation.delta.symbol, direction=direction, current_price=observation.delta.mid,
            volatility_points=max(observation.ewma_volatility * observation.delta.mid, observation.delta.mid*1e-6), observation=zone,
            execution_cost_bps=observation.costs.total_bps, cross_market_alignment=composite.flow_agreement_score) for zone in observation.zone_observations]
        stop_candidates = [z for z in self.liquidity.model_score(raw_zones) if z.sl_safety_score > 0 and (z.predicted_stop_sweep_probability or 1.0) <= self.config.policy.maximum_stop_sweep_probability]
        if not stop_candidates:
            return self._decision(OpportunityDecision("BTC", observation.delta.symbol, direction, DecisionCode.NO_TRADE_EXECUTION_UNSAFE, 0.0, composite.delta_execution_quality_score, False, None, ("NO_MODEL_VALIDATED_INVALIDATION_ZONE",), {}))
        stop_zone = max(stop_candidates, key=lambda z: z.sl_safety_score); invalidation = stop_zone.price_low if direction == Direction.LONG.value else stop_zone.price_high
        plan, diagnostics = self.liquidity.protection_plan(direction=direction, proposed_entry=observation.delta.mid, invalidation_price=invalidation, zones=raw_zones,
            min_tp_probability=self.config.policy.minimum_tp_before_sl_probability, max_stop_sweep_probability=self.config.policy.maximum_stop_sweep_probability)
        if plan is None:
            return self._decision(OpportunityDecision("BTC", observation.delta.symbol, direction, DecisionCode.NO_TRADE_EXECUTION_UNSAFE, 0.0, composite.delta_execution_quality_score, False, None, (str(diagnostics.get("reason")),), diagnostics))
        liquidity_features = LiquidityFeatureBuilder.vector(diagnostics["target_zone"]); reward = abs(plan.target_price/plan.entry_price-1)*10000; risk = abs(plan.entry_price/plan.stop_price-1)*10000
        predicted_direction, edge, metrics = self.models.btc.predict(lagged_features=observation.lagged_model_features, composite=composite, ewma_volatility=observation.ewma_volatility,
            liquidity_features=liquidity_features, cost_without_slippage=observation.costs, reward_bps=reward, risk_bps=risk)
        if predicted_direction.value != direction: raise RuntimeError("BTC_MODEL_DIRECTION_CHANGED_WITHIN_DECISION")
        if metrics.get("execution_action") == "REJECT":
            return self._decision(OpportunityDecision("BTC", observation.delta.symbol, direction, DecisionCode.NO_TRADE_EXECUTION_UNSAFE, edge.trade_score_bps, composite.delta_execution_quality_score, False, plan, ("EXECUTION_URGENCY_MODEL_REJECTED",), metrics))
        self.shadow.record(ShadowPrediction(observation.ts_ns, "BTC", observation.delta.symbol, str(metrics.get("model_version", "")), float(metrics.get("predicted_gross_return_bps", edge.predicted_gross_return_bps)), "features.jsonl", metrics))
        unit_notional = observation.delta.mid * observation.delta_product.mapping.contract_multiplier; unit_risk = unit_notional * risk / 10000.0; stress = unit_notional * edge.cost.slippage_bps / 10000.0
        leverage = self.leverage.select(maximum_leverage=observation.delta_product.maximum_leverage, unit_notional=unit_notional, risk_to_invalidation=unit_risk,
            volatility_fraction=observation.ewma_volatility, execution_quality=composite.delta_execution_quality_score, expected_net_edge_bps=edge.trade_score_bps)
        if not leverage.approved: return self._decision(OpportunityDecision("BTC", observation.delta.symbol, direction, DecisionCode.NO_TRADE_RISK_BUDGET, edge.trade_score_bps, composite.delta_execution_quality_score, False, plan, (leverage.reason,), metrics))
        sizing = self.alloc_usd.size(desk="BTC", risk_group="CRYPTO_BETA", currency="USD", instrument=observation.delta.symbol, expected_net_edge=edge.trade_score_bps,
            unit_invalidation_risk=unit_risk, unit_stress_slippage_risk=stress, unit_notional=unit_notional, unit_margin=leverage.unit_margin,
            liquidity_capacity_qty=observation.liquidity_capacity_qty, available_margin=observation.available_margin, venue_step=observation.delta_product.mapping.qty_step,
            leverage_selected=leverage.selected_leverage)
        decision = self.policy.approve(desk="BTC", instrument=observation.delta.symbol, direction=direction, net_edge_bps=edge.trade_score_bps,
            execution_quality=composite.delta_execution_quality_score, size_approved=sizing.approved, plan=plan, venue_supports_protection=self.delta_executor is not None,
            compliance_ready=self.config.policy.delta_live_orders_enabled, reasons=tuple(sizing.reasons), metrics={**metrics, "liquidity":diagnostics}, sizing=sizing)
        if decision.approved:
            zone = diagnostics.get("target_zone")
            zone_id = f"{observation.delta.symbol}:{getattr(zone, 'price_low', plan.target_price)}:{getattr(zone, 'price_high', plan.target_price)}"
            self.outcomes.register_liquidity_plan(ts_ns=observation.ts_ns, instrument=observation.delta.symbol, direction=direction, zone_id=zone_id, plan=plan)
        return self._decision(decision)
    async def handle_btc(self, observation: BTCObservation, *, live: bool) -> OpportunityDecision:
        decision = self.evaluate_btc(observation)
        if live and decision.approved:
            if not self.delta_executor: raise RuntimeError("DELTA_PROTECTED_EXECUTOR_REQUIRED")
            receipt = self.delta_executor.execute(desk="BTC", product=observation.delta_product, side=decision.direction, quantity=decision.sizing.quantity, plan=decision.protection_plan)
            self._record("executions", asdict(receipt))
            if receipt.state.value == "ACTIVE_PROTECTED_POSITION": self.book.set(Exposure(observation.delta.symbol,"BTC","CRYPTO_BETA","USD",decision.sizing.notional, decision.sizing.notional if decision.direction=="LONG" else -decision.sizing.notional,decision.sizing.risk_to_invalidation,decision.sizing.stress_slippage_risk,decision.sizing.margin_required))
        return decision
    def evaluate_metals(self, observation: MetalsObservation) -> OpportunityDecision:
        self._record("observations", {"desk": "METALS", "ts_ns": observation.ts_ns, "asset": observation.asset, "local_state": asdict(observation.local_state), "fair_value": observation.fair_value, "reference_features": observation.reference_features.tolist(), "costs": asdict(observation.costs), "restriction": asdict(observation.restriction) if observation.restriction else None})
        halted = self._halted("METALS", observation.local_state.symbol)
        if observation.restriction is not None and not observation.restriction.tradable:
            return self._decision(OpportunityDecision("METALS", observation.local_state.symbol, Direction.NO_TRADE.value, DecisionCode.NO_TRADE_EXECUTION_UNSAFE, 0.0, 0.0, False, None, (observation.restriction.reason or "TRADING_RESTRICTED",), {}))
        if halted: return halted
        self._model_guard(self.models.metals, "METALS"); self._model_guard(self.models.liquidity, "LIQUIDITY")
        self.outcomes.on_price(ts_ns=observation.ts_ns, instrument=observation.local_state.symbol, price=observation.local_state.mid)
        raw = [self.liquidity.score(instrument=observation.local_state.symbol, direction="LONG", current_price=observation.local_state.mid,
            volatility_points=max(observation.ewma_volatility*observation.local_state.mid, observation.local_state.mid*1e-6), observation=z,
            execution_cost_bps=observation.costs.total_bps, cross_market_alignment=1.0) for z in observation.zone_observations]
        # Liquidity summary is directional-context input to the model; direction is selected by approved fair-value model.
        summary = np.mean([LiquidityFeatureBuilder.vector(z) for z in raw], axis=0) if raw else np.zeros(11)
        opportunity = self.models.metals.evaluate(asset=observation.asset, symbol=observation.local_state.symbol, fair_value=observation.fair_value,
            local_state=observation.local_state, reference_features=observation.reference_features, liquidity_features=summary, cost=observation.costs)
        if opportunity.direction == Direction.NO_TRADE.value:
            return self._decision(OpportunityDecision("METALS", observation.local_state.symbol, opportunity.direction, DecisionCode.NO_TRADE_INSUFFICIENT_EDGE, opportunity.net_edge_bps,
                opportunity.local_execution_score, False, None, (opportunity.rejection_reason or "MODEL_REJECTED",), {"opportunity":asdict(opportunity)}))
        directional_raw = [self.liquidity.score(instrument=observation.local_state.symbol, direction=opportunity.direction, current_price=observation.local_state.mid,
            volatility_points=max(observation.ewma_volatility*observation.local_state.mid, observation.local_state.mid*1e-6), observation=z,
            execution_cost_bps=observation.costs.total_bps, cross_market_alignment=opportunity.fair_value_alignment_score) for z in observation.zone_observations]
        scored = self.liquidity.model_score(directional_raw); stop = max([z for z in scored if z.sl_safety_score>0], key=lambda z:z.sl_safety_score, default=None)
        invalidation = None if stop is None else stop.price_low if opportunity.direction == "LONG" else stop.price_high
        plan, diagnostics = (None, {"reason":"NO_SAFE_STOP"}) if invalidation is None else self.liquidity.protection_plan(direction=opportunity.direction, proposed_entry=observation.local_state.mid, invalidation_price=invalidation, zones=directional_raw)
        if plan is None: return self._decision(OpportunityDecision("METALS", observation.local_state.symbol, opportunity.direction, DecisionCode.NO_TRADE_EXECUTION_UNSAFE, opportunity.net_edge_bps, opportunity.local_execution_score, False, None, (str(diagnostics.get("reason")),), {}))
        reward_bps = abs(plan.target_price / plan.entry_price - 1.0) * 10_000.0
        risk_bps = abs(plan.entry_price / plan.stop_price - 1.0) * 10_000.0
        model_cost = type(observation.costs)(observation.costs.fees_bps, observation.costs.spread_bps, observation.costs.slippage_bps, observation.costs.impact_bps, observation.costs.protection_cost_bps, abs(opportunity.basis_bps) + opportunity.tracking_error_bps, observation.costs.uncertainty_bps)
        edge = executable_edge(abs(opportunity.predicted_move_bps), model_cost, tp_probability=plan.target_probability, reward_bps=reward_bps, risk_bps=risk_bps)
        unit_notional = observation.local_state.mid * observation.delta_product.mapping.contract_multiplier; unit_risk = unit_notional * risk_bps / 10_000.0; stress = unit_notional * model_cost.slippage_bps / 10_000.0
        lev=self.leverage.select(maximum_leverage=observation.delta_product.maximum_leverage, unit_notional=unit_notional, risk_to_invalidation=unit_risk, volatility_fraction=observation.ewma_volatility, execution_quality=opportunity.local_execution_score, expected_net_edge_bps=edge.trade_score_bps)
        if not lev.approved: return self._decision(OpportunityDecision("METALS",observation.local_state.symbol,opportunity.direction,DecisionCode.NO_TRADE_RISK_BUDGET,edge.trade_score_bps,opportunity.local_execution_score,False,plan,(lev.reason,),{}))
        sizing=self.alloc_usd.size(desk="METALS",risk_group="METAL_BETA",currency="USD",instrument=observation.local_state.symbol,expected_net_edge=edge.trade_score_bps,unit_invalidation_risk=unit_risk,unit_stress_slippage_risk=stress,unit_notional=unit_notional,unit_margin=lev.unit_margin,liquidity_capacity_qty=observation.liquidity_capacity_qty,available_margin=observation.available_margin,venue_step=observation.delta_product.mapping.qty_step,leverage_selected=lev.selected_leverage)
        decision = self.policy.approve(desk="METALS",instrument=observation.local_state.symbol,direction=opportunity.direction,net_edge_bps=edge.trade_score_bps,execution_quality=opportunity.local_execution_score,size_approved=sizing.approved,plan=plan,venue_supports_protection=self.delta_executor is not None,compliance_ready=self.config.policy.metals_live_orders_enabled,reasons=sizing.reasons,metrics={"opportunity":asdict(opportunity),"liquidity":diagnostics,"trade_score_bps":edge.trade_score_bps},sizing=sizing)
        if decision.approved:
            zone = diagnostics.get("target_zone")
            zone_id = f"{observation.local_state.symbol}:{getattr(zone, 'price_low', plan.target_price)}:{getattr(zone, 'price_high', plan.target_price)}"
            self.outcomes.register_liquidity_plan(ts_ns=observation.ts_ns, instrument=observation.local_state.symbol, direction=opportunity.direction, zone_id=zone_id, plan=plan)
        return self._decision(decision)
    async def handle_metals(self, observation: MetalsObservation, *, live: bool) -> OpportunityDecision:
        decision=self.evaluate_metals(observation)
        if live and decision.approved:
            if not self.delta_executor: raise RuntimeError("DELTA_PROTECTED_EXECUTOR_REQUIRED")
            receipt=self.delta_executor.execute(desk="METALS",product=observation.delta_product,side=decision.direction,quantity=decision.sizing.quantity,plan=decision.protection_plan); self._record("executions",asdict(receipt))
            if receipt.state.value == "ACTIVE_PROTECTED_POSITION": self.book.set(Exposure(observation.local_state.symbol,"METALS","METAL_BETA","USD",decision.sizing.notional, decision.sizing.notional if decision.direction=="LONG" else -decision.sizing.notional,decision.sizing.risk_to_invalidation,decision.sizing.stress_slippage_risk,decision.sizing.margin_required))
        return decision
    def evaluate_india(self, observation: IndiaObservation) -> OpportunityDecision:
        self._record("observations", {"desk": "INDIA_OPTIONS", "ts_ns": observation.ts_ns, "underlying": observation.underlying, "current_level": observation.current_level, "forward_price": observation.forward_price, "direction_features": observation.direction_features.tolist(), "regime_features": observation.regime_features.tolist(), "iv_features": observation.iv_features.tolist(), "restriction": asdict(observation.restriction) if observation.restriction else None})
        halted = self._halted("INDIA_OPTIONS", observation.underlying)
        if self.groww_executor is not None and self.groww_executor.entries_halted:
            return self._decision(OpportunityDecision("INDIA_OPTIONS", observation.underlying, Direction.NO_TRADE.value, DecisionCode.NO_TRADE_EXECUTION_UNSAFE, 0.0, 0.0, False, None, ("GROWW_ENTRIES_HALTED_PENDING_RECONCILIATION",), {}))
        if observation.restriction is not None and not observation.restriction.tradable:
            return self._decision(OpportunityDecision("INDIA_OPTIONS", observation.underlying, Direction.NO_TRADE.value, DecisionCode.NO_TRADE_EXECUTION_UNSAFE, 0.0, 0.0, False, None, (observation.restriction.reason or "TRADING_RESTRICTED",), {}))
        if halted: return halted
        self._model_guard(self.models.india, "INDIA_UNDERLYING"); self._model_guard(self.models.option_iv, "OPTION_IV_CHANGE")
        self.outcomes.on_underlying_price(ts_ns=observation.ts_ns, underlying=observation.underlying, price=observation.current_level)
        opportunity=self.models.india.evaluate(underlying=observation.underlying,features=observation.direction_features,regime_features=observation.regime_features,current_level=observation.current_level,volatility_points=observation.volatility_points,liquidity_rationale=observation.liquidity_rationale)
        self.features.append({"desk":"INDIA_OPTIONS","ts_ns":observation.ts_ns,"features":observation.direction_features.tolist(),"opportunity":asdict(opportunity)})
        if opportunity.direction == Direction.NO_TRADE.value:
            return self._decision(OpportunityDecision("INDIA_OPTIONS",observation.underlying,opportunity.direction,DecisionCode.NO_TRADE_INSUFFICIENT_EDGE,0.0,0.0,False,None,(opportunity.rejection_reason or "NO_DIRECTION",),{}))
        self.outcomes.register_underlying(ts_ns=observation.ts_ns, underlying=observation.underlying, side=opportunity.direction, entry=observation.current_level, invalidation=opportunity.invalidation_level, target=opportunity.target_levels[0], horizon_seconds=opportunity.expected_holding_minutes * 60)
        option_type = "CE" if opportunity.direction == Direction.BULLISH.value else "PE"
        candidates, contracts = observation.load_candidates(option_type)
        expected_iv_change = self.models.option_iv.predict(observation.iv_features)
        ranked=self.ranker.rank(opportunity=opportunity,candidates=candidates,forward_price=observation.forward_price,expected_iv_change=expected_iv_change,fee_per_lot=observation.validated_fee_per_lot,stress_slippage_per_lot=observation.stress_slippage_per_lot)
        selected=next((row for row in ranked if row.rejection_reason is None and row.total_score>0),None)
        if selected is None: return self._decision(OpportunityDecision("INDIA_OPTIONS",observation.underlying,opportunity.direction,DecisionCode.NO_TRADE_EXECUTION_UNSAFE,0.0,0.0,False,None,("NO_EXECUTABLE_LONG_OPTION_CONTRACT",),{"ranked":[asdict(x) for x in ranked]}))
        risk_per_lot, cash_ok=MarginPolicy.groww_long_option(selected.premium,selected.lot_size,observation.validated_fee_per_lot,observation.stress_slippage_per_lot,observation.available_cash)
        target=max(selected.premium + selected.expected_premium_return_after_cost/selected.lot_size, selected.premium*1.01); stop=max(0.05, selected.premium - risk_per_lot/selected.lot_size*0.35)
        plan=ProtectionPlan(selected.premium,target,stop,opportunity.invalidation_level,quantity=selected.lot_size,target_probability=selected.probability_tp_before_sl,reason="underlying-thesis translated to option premium OCO")
        # INR options are evaluated in an INR risk book; covariance/ES scenarios must contain the selected contract before live deployment.
        sizing=self.alloc_inr.size(desk="INDIA_OPTIONS",risk_group="INDIA_INDEX_DELTA",currency="INR",instrument=selected.trading_symbol,expected_net_edge=selected.expected_premium_return_after_cost,unit_invalidation_risk=risk_per_lot,unit_stress_slippage_risk=0.0,unit_notional=selected.premium*selected.lot_size,unit_margin=risk_per_lot,liquidity_capacity_qty=max(1.0, selected.liquidity_score*10),available_margin=observation.available_cash,venue_step=1.0,max_qty=1.0)
        compliance=self.groww_executor is not None and cash_ok
        return self._decision(self.policy.approve(desk="INDIA_OPTIONS",instrument=selected.trading_symbol,direction=opportunity.direction,net_edge_bps=selected.expected_premium_return_after_cost,execution_quality=selected.liquidity_score,size_approved=sizing.approved,plan=plan,venue_supports_protection=self.groww_executor is not None,compliance_ready=compliance,reasons=sizing.reasons,metrics={"underlying":asdict(opportunity),"selected_contract":asdict(selected),"ranked":[asdict(x) for x in ranked]},sizing=sizing))
    async def handle_india(self, observation: IndiaObservation, *, live: bool) -> OpportunityDecision:
        decision=self.evaluate_india(observation)
        if live and decision.approved:
            option_type = "CE" if decision.direction == Direction.BULLISH.value else "PE"
            _, contracts = observation.load_candidates(option_type)
            contract=contracts[decision.instrument]; receipt=self.groww_executor.execute(contract=contract,quantity=int(decision.protection_plan.quantity),entry_limit=decision.protection_plan.entry_price,protection=decision.protection_plan); self._record("executions",asdict(receipt))
            if receipt.state.value == "ACTIVE_PROTECTED_POSITION": self.book.set(Exposure(decision.instrument,"INDIA_OPTIONS","INDIA_INDEX_DELTA","INR",decision.sizing.notional,decision.sizing.notional if decision.direction=="BULLISH" else -decision.sizing.notional,decision.sizing.risk_to_invalidation,decision.sizing.stress_slippage_risk,decision.sizing.margin_required))
        return decision
    def record_closed_execution(self, *, execution_id: str, desk: str, instrument: str, gross_pnl: float, fee_cost: float, spread_cost: float, slippage_cost: float, impact_cost: float, mfe: float, mae: float, expected_direction_correct: bool, contract_efficiency_ok: bool, protection_quality_ok: bool, execution_quality_ok: bool) -> Any:
        self.book.remove(instrument)
        return self.attribution.close(execution_id=execution_id, desk=desk, instrument=instrument, gross_pnl=gross_pnl, fee_cost=fee_cost, spread_cost=spread_cost, slippage_cost=slippage_cost, impact_cost=impact_cost, mfe=mfe, mae=mae, expected_direction_correct=expected_direction_correct, contract_efficiency_ok=contract_efficiency_ok, protection_quality_ok=protection_quality_ok, execution_quality_ok=execution_quality_ok)
    def status(self) -> dict[str, Any]:
        return {"strategy_authority_ready": True, "live_flags":{"delta":self.config.policy.delta_live_orders_enabled,"metals":self.config.policy.metals_live_orders_enabled,"groww":self.config.policy.groww_live_orders_enabled},"model_authority":{"btc":bool(self.models.btc and self.models.btc.promoted),"liquidity":bool(self.models.liquidity and self.models.liquidity.promoted),"metals":bool(self.models.metals and self.models.metals.promoted),"india":bool(self.models.india and self.models.india.promoted),"option_iv":bool(self.models.option_iv and self.models.option_iv.promoted)},"entries_halted": self.entries_halted, "positions":[asdict(x) for x in self.book.items()]}

from __future__ import annotations
import asyncio
import os
import types
from dataclasses import replace
from datetime import datetime, timedelta, UTC
from pathlib import Path
import json
import sys
import numpy as np
import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.config import PlatformConfig, PlatformPolicy, Secrets
from core.identifiers import BookLevel, CostEstimate, DecisionCode, Direction, ExecutionReceipt, InstrumentMapping, LiquidityZoneScore, ProtectionPlan, ProtectionState, VenueMicrostate
from core.observability import Observability
from core.research_store import ResearchStore
from core.state_store import StateStore
from market_data.normalizer import usd_notional, aggregate_depth, build_microstate
from market_data.feed_health import FeedHealthMonitor
from market_data.candles import Candle, CandleSeries
from market_data.trade_tape import Trade, TradeTape, OrderFlowTracker
from intelligence.volatility import EWMAVolatility, HARRVModel
from intelligence.regime_engine import GaussianHMMRegimeModel
from intelligence.predictive_models import DistributedLagRidge, KalmanDynamicLinearModel, LightGBMProbabilityModel, LightGBMReturnModel, EmpiricalSlippageModel, ExecutionUrgencyModel, CompetingRiskSurvivalModel, ElasticNetDirectionModel, QuantileExpectedMoveModel, FillHazardModel, ExpectedIVChangeModel
from intelligence.option_pricing import black76_price, implied_volatility, black76_greeks
from intelligence.cross_venue_btc import BTCCompositeEngine
from intelligence.edge_model import executable_edge
from intelligence.liquidity_intelligence import LiquidityPoolBook, LiquidityFeatureBuilder, LiquidityModelBundle, LiquidityIntelligence, ZoneObservation, ZoneOutcome
from intelligence.metals_fair_value import KalmanBasisTracker, MetalsModelBundle, CointegrationKalmanSpreadResearch
from intelligence.india_underlying_state import IndiaUnderlyingModelBundle
from intelligence.option_contract_ranker import LongOptionRanker, OptionChainCandidate
from intelligence.market_state import TradingRestrictionState
from portfolio.exposure import Exposure, ExposureBook
from portfolio.covariance import ShrunkEWMACovariance
from portfolio.expected_shortfall import ExpectedShortfallModel
from portfolio.drawdown import DrawdownController
from portfolio.allocator import PortfolioAllocator
from portfolio.leverage import DynamicLeverageSelector
from portfolio.margin import MarginPolicy
from execution.delta_protected_orders import DeltaProtectedExecutor
from execution.groww_long_option_execution import GrowwLongOptionExecutor, GrowwOptionPositionMonitor
from execution.reconciliation import GrowwReconciler
from execution.emergency import GrowwEmergencyExit
from adapters.delta.client import DeltaProduct
from adapters.groww.client import GrowwAdapter, GrowwContract
from adapters.groww.feed import GrowwFeedBridge
from research.labels import ForwardLabelCoordinator, OutcomeCoordinator, calculate_delta_forward_label
from research.model_registry import ModelRegistry, ModelVersion
from research.attribution import AttributionEngine
from research.replay import ReplayRunner
from research.training import ArtifactPublisher, ValidationEvidence, BTCTrainer, LiquidityTrainer, MetalsTrainer, IndiaTrainer, RiskTrainer
from orchestration.platform import InstitutionalPlatform, ModelAuthority
from orchestration.model_loader import PromotedAuthorityLoader
from orchestration.runtime import BTCObservation, IndiaObservation
from orchestration.verified_streams import BTCVerifiedMarketStream, DeltaDeskRuntimeInputs
from dashboard.server import DashboardServer
from market_data.venue_parsers import ParsedVenueUpdate
from market_data.orderbook import BookSnapshot

# ----------------------------- helpers ---------------------------------
def mapping(venue: str = "delta", symbol: str = "BTCUSD", enabled: bool = True, formula: str = "linear_base") -> InstrumentMapping:
    return InstrumentMapping(venue, symbol, "BTC", "perp", "USD", 1.0, "USD", 0.01, 0.001, enabled, formula)

def state(venue: str = "delta", symbol: str = "BTCUSD", mid: float = 100.0, quality: float = 1.0, flow: float = 1.0) -> VenueMicrostate:
    depth={"0-1":60_000.0,"1-3":30_000.0,"3-10":10_000.0,"10-25":0.0}
    return VenueMicrostate(venue,symbol,1_000_000_000,1_001_000_000,quality,mid-0.005,mid+0.005,mid,mid,1.0,depth,depth,{k:0.0 for k in depth},
        20_000*flow,40_000*flow,60_000*flow,0.4*flow,0.3*flow,0.2*flow,None,None,1.0,True)

def zones_long() -> list[ZoneObservation]:
    return [ZoneObservation(97.5,98.0,["15m","1h"],60,0,0,100_000,2_000,20_000,0.9), ZoneObservation(102.5,103.0,["15m","1h"],60,0,0,120_000,1_000,10_000,0.8)]

def make_zone_scores() -> tuple[list[LiquidityZoneScore], list[ZoneOutcome]]:
    scores=[]; outcomes=[]
    for i in range(80):
        obs=ZoneObservation(97+i*0.001,98+i*0.001,["15m","1h"],float(i+1),i%2,i%3,100_000+i*100,1_000+i,10_000-i*10,0.8)
        score=LiquidityFeatureBuilder.raw_score(instrument="BTCUSD",direction="LONG",current_price=100,volatility_points=1,observation=obs,execution_cost_bps=1,cross_market_alignment=0.8)
        scores.append(score); outcomes.append(ZoneOutcome(1 if i%3 else 0,1 if i%4 else 0,0 if i%4 else 1,1 if i%7==0 else 0))
    for i in range(80):
        obs=ZoneObservation(102+i*0.001,103+i*0.001,["15m","1h"],float(i+1),i%2,i%3,110_000+i*100,1_000+i,8_000-i*10,0.8)
        score=LiquidityFeatureBuilder.raw_score(instrument="BTCUSD",direction="LONG",current_price=100,volatility_points=1,observation=obs,execution_cost_bps=1,cross_market_alignment=0.8)
        scores.append(score); outcomes.append(ZoneOutcome(0,1 if i%3 else 0,0 if i%3 else 1,1 if i%9==0 else 0))
    return scores,outcomes

def fit_regime() -> GaussianHMMRegimeModel:
    rng=np.random.default_rng(4); blocks=[]
    centres=[[0,0.002,1,0,0.1],[0.003,0.004,1,0.8,0.1],[0.006,0.010,2,0.5,0.2],[0,0.08,5,0,0.3],[0,0.003,30,0,0.9]]
    for c in centres: blocks.append(rng.normal(c,[0.0002,0.0002,0.1,0.05,0.02],size=(30,5)))
    return GaussianHMMRegimeModel().fit(np.vstack(blocks))

def fitted_risk(names: list[str]):
    rng=np.random.default_rng(2); returns={name:rng.normal(0,0.001,200) for name in names}
    cov=ShrunkEWMACovariance().fit(returns); es=ExpectedShortfallModel().fit(returns,{name:[-0.03,-0.05] for name in names}); return cov,es

def platform_policy(tmp_path: Path, **kwargs) -> PlatformConfig:
    policy=PlatformPolicy(delta_live_orders_enabled=True,metals_live_orders_enabled=True,groww_live_orders_enabled=True,state_dir=tmp_path/'state',research_dir=tmp_path/'research',model_dir=tmp_path/'models',log_dir=tmp_path/'logs',**kwargs)
    return PlatformConfig(policy=policy,secrets=Secrets())

# ----------------------------- architecture ---------------------------------
def test_single_automated_test_file_and_no_deleted_runtime_paths():
    assert [p.name for p in (ROOT/'tests').glob('test_*.py')] == ['test_institutional_platform.py']
    for path in ('strategy','agents','aggregator','risk','exchanges','execution/order_manager.py','orchestration/multi_asset_bot.py'):
        assert not (ROOT/path).exists()

def test_required_model_and_risk_modules_exist():
    for path in ('intelligence/volatility.py','intelligence/regime_engine.py','intelligence/predictive_models.py','intelligence/option_pricing.py','portfolio/expected_shortfall.py','research/training.py'):
        assert (ROOT/path).exists(), path

def test_policy_defaults_fail_closed():
    policy=PlatformPolicy()
    assert not policy.delta_live_orders_enabled and not policy.groww_live_orders_enabled and not policy.metals_live_orders_enabled
    assert policy.trained_model_promotion_required and policy.protected_execution_required

# ----------------------------- normalisation/feed ----------------------------
def test_usd_notional_product_formulas():
    assert usd_notional(BookLevel(100,2),mapping()) == 200
    assert usd_notional(BookLevel(100,2),mapping(formula='inverse_usd')) == 2
    assert usd_notional(BookLevel(100,2),mapping(formula='quote_notional')) == 2

def test_depth_aggregation_and_microprice():
    bands=aggregate_depth([BookLevel(100.005,1),BookLevel(100.02,2),BookLevel(100.08,3)],100,mapping())
    assert bands['0-1']==pytest.approx(100.005) and bands['1-3']==pytest.approx(200.04) and bands['3-10']==pytest.approx(300.24)
    micro=build_microstate(mapping=mapping(),bids=[BookLevel(99.995,2)],asks=[BookLevel(100.005,1)],receive_ts_ns=2_000_000_000,exchange_ts_ns=1_999_000_000,feed_quality_score=1,sequence_valid=True,ofi={'10s':2},tfi={'10s':.5})
    assert micro.mid==pytest.approx(100) and micro.microprice>micro.mid and micro.update_latency_ms==pytest.approx(1)

def test_feed_health_is_zero_for_missing_heartbeat_sequence_or_snapshot():
    monitor=FeedHealthMonitor(minimum_samples=2)
    for kwargs in ({'connected':False,'heartbeat_ok':True,'sequence_valid':True,'snapshot_ready':True},{'connected':True,'heartbeat_ok':False,'sequence_valid':True,'snapshot_ready':True},{'connected':True,'heartbeat_ok':True,'sequence_valid':False,'snapshot_ready':True},{'connected':True,'heartbeat_ok':True,'sequence_valid':True,'snapshot_ready':False}):
        assert monitor.assess('delta',exchange_timestamp_available=True,latency_ms=1,**kwargs).quality_score==0

def test_feed_health_penalises_venue_relative_latency():
    monitor=FeedHealthMonitor(minimum_samples=2)
    for value in (5,6): monitor.assess('delta',connected=True,heartbeat_ok=True,sequence_valid=True,snapshot_ready=True,exchange_timestamp_available=True,latency_ms=value)
    late=monitor.assess('delta',connected=True,heartbeat_ok=True,sequence_valid=True,snapshot_ready=True,exchange_timestamp_available=True,latency_ms=100)
    assert late.quality_score < 1 and late.latency_vs_baseline_z is not None

def test_tfi_is_normalised_and_ofi_is_windowed():
    tape=TradeTape(mapping()); tape.append(Trade(9_500_000_000,100,2,'BUY')); tape.append(Trade(9_600_000_000,100,1,'SELL'))
    assert tape.tfi(10_000_000_000)['1s']==pytest.approx(1/3)
    ofi=OrderFlowTracker(); ofi.append(9_500_000_000,100,40); ofi.append(8_000_000_000,10,30)
    assert ofi.ofi(10_000_000_000)['1s']==60

# ----------------------------- volatility/regime -----------------------------
def test_ewma_variance_formula():
    model=EWMAVolatility(0.5); first=model.update(0.1); second=model.update(0.2)
    assert first.variance==pytest.approx(.01) and second.variance==pytest.approx(.025)

def test_har_rv_fits_and_predicts_non_negative():
    rv=np.linspace(.001,.004,40); model=HARRVModel().fit(rv)
    assert model.predict_next(rv) >= 0

def test_gaussian_hmm_regime_model_trains_and_outputs_probability():
    model=fit_regime(); assessment=model.assess(np.asarray([0,0.08,5,0,0.3]))
    assert assessment.regime in {'balance','trend','expansion','shock','illiquid'} and 0 < assessment.probability <= 1
    assert sum(assessment.state_probabilities.values())==pytest.approx(1)

# ----------------------------- trained models --------------------------------
def test_distributed_lag_ridge_predicts_delta_target():
    x=np.column_stack([np.linspace(-1,1,80),np.linspace(1,-1,80)]); y=np.r_[0,0,2*x[2:,0]-x[1:-1,1]]
    model=DistributedLagRidge(lags=1,alpha=1e-5).fit(x,y)
    assert np.isfinite(model.predict(x[-2:])) and len(model.venue_importance(['a','b']))==2

def test_kalman_dynamic_coefficients_update():
    model=KalmanDynamicLinearModel(2)
    for _ in range(20): model.update(np.asarray([1.0,0.0]),2.0)
    assert model.predict(np.asarray([1.0,0.0])) > 1.5

def test_lightgbm_probability_and_return_models_fit():
    rng=np.random.default_rng(9); x=rng.normal(size=(100,4)); y=(x[:,0]>0).astype(int); returns=x[:,0]*2
    assert 0 <= LightGBMProbabilityModel().fit(x,y).probability(x[0]) <= 1
    assert np.isfinite(LightGBMReturnModel().fit(x,returns).predict(x[0]))

def test_slippage_and_execution_urgency_models():
    rng=np.random.default_rng(1); x=rng.normal(size=(120,3)); slip=np.abs(x[:,0])+0.2; filled=(x[:,1]>0).astype(int)
    assert EmpiricalSlippageModel().fit(x,slip).predict(x[0]) >= 0
    choice,ev=ExecutionUrgencyModel().fit(x,filled).choose(x[0],passive_edge_bps=4,aggressive_edge_bps=2,adverse_if_unfilled_bps=2)
    assert choice in {'PASSIVE_LIMIT','AGGRESSIVE_LIMIT','REJECT'} and np.isfinite(ev)

def test_competing_risk_survival_model_probability():
    rng=np.random.default_rng(10); x=rng.normal(size=(100,3)); tp=(x[:,0]>0).astype(int); sl=(x[:,0]<=0).astype(int)
    p=CompetingRiskSurvivalModel().fit(x,tp,sl).probability_tp_before_sl(np.asarray([1,0,0]))
    assert p > .5


def test_fill_hazard_and_expected_iv_change_models_are_fitted():
    rng=np.random.default_rng(6); x=rng.normal(size=(120,4)); filled=(x[:,0]>0).astype(int); iv_change=x[:,1]*.01
    assert 0 <= FillHazardModel().fit(x,filled).probability_fill(x[0]) <= 1
    assert np.isfinite(ExpectedIVChangeModel(promoted=True, version="iv-v1").fit(x,iv_change).predict(x[0]))

def test_india_elastic_net_direction_and_quantile_move():
    rng=np.random.default_rng(5); x=rng.normal(size=(120,4)); y=np.where(x[:,0]>0,1,-1); moves=np.abs(x[:,0])*25+3
    d=ElasticNetDirectionModel().fit(x,y); q=QuantileExpectedMoveModel().fit(x,moves)
    assert d.probabilities(np.asarray([2,0,0,0]))[1] > .5 and q.predict(np.asarray([2,0,0,0])) > 0

# ----------------------------- option maths/ranking --------------------------
def test_black76_put_call_parity_and_iv_recovery():
    f,k,r,t,sigma=22000,22000,.06,14/365,.18
    call=black76_price(f,k,r,t,sigma,'CE'); put=black76_price(f,k,r,t,sigma,'PE')
    assert call-put==pytest.approx(np.exp(-r*t)*(f-k),abs=1e-8)
    assert implied_volatility(call,f,k,r,t,'CE')==pytest.approx(sigma,rel=1e-5)

def test_black76_greeks_have_correct_long_option_signs():
    ce=black76_greeks(22000,22000,.06,14/365,.2,'CE'); pe=black76_greeks(22000,22000,.06,14/365,.2,'PE')
    assert ce.delta>0 and pe.delta<0 and ce.gamma>0 and ce.vega>0

def test_long_option_ranker_computes_iv_vega_theta_and_rejects_wrong_type():
    opportunity=__import__('core.identifiers',fromlist=['IndiaUnderlyingOpportunity']).IndiaUnderlyingOpportunity('NIFTY','trend','BULLISH',150,15,.8,21800,[22150],{},None)
    premium=black76_price(22000,22000,.065,30/365,.20,'CE')
    expiry=(datetime.now(UTC)+timedelta(days=30)).date().isoformat()
    rows=[OptionChainCandidate('NIFTYCE','CE',expiry,22000,premium-1,premium+1,500,1000,1000,25,True),OptionChainCandidate('NIFTYPE','PE',expiry,22000,premium-1,premium+1,500,1000,1000,25,True)]
    ranked=LongOptionRanker(max_spread_bps=500,max_premium_risk=1e6,minimum_liquidity_score=.1).rank(opportunity=opportunity,candidates=rows,forward_price=22000,expected_iv_change=.01,fee_per_lot=20,stress_slippage_per_lot=10)
    assert len(ranked)==1 and ranked[0].option_type=='CE' and ranked[0].independently_calculated_iv>0 and ranked[0].vega>0

# ----------------------------- liquidity/metals ------------------------------
def test_liquidity_pool_tracks_touch_depletion_and_absorption():
    book=LiquidityPoolBook(); z=zones_long()[0]; book.upsert('BTCUSD',z); book.touch('BTCUSD',z.price_low,z.price_high,consumed_usd=5000,absorbed_usd=1000,swept=True)
    value=book.zones('BTCUSD')[0]; assert value.touch_count==1 and value.sweep_count==1 and value.consumed_liquidity_usd==7000

def test_liquidity_models_fit_and_protection_plan_is_model_ranked():
    scores,outcomes=make_zone_scores(); models=LiquidityModelBundle(promoted=True,version='lgbm-survival-v1').fit(scores,outcomes); engine=LiquidityIntelligence(models)
    candidate_scores=[scores[10], scores[100]]
    # Training outcome probabilities can be conservative; use permissive gates while proving model-ranked geometry.
    plan,diag=engine.protection_plan(direction='LONG',proposed_entry=100,invalidation_price=96,zones=candidate_scores,min_tp_probability=0,max_stop_sweep_probability=1)
    assert plan is not None and plan.stop_price < plan.entry_price < plan.target_price
    assert diag['target_zone'].predicted_tp_before_sl_probability is not None

def test_kalman_basis_tracker_updates_normal_basis():
    tracker=KalmanBasisTracker(); values=[tracker.update(x)[0] for x in (10,11,12)]
    assert values[-1]>values[0] and tracker.ready

def test_gold_silver_cointegration_kalman_is_research_only_context():
    model=CointegrationKalmanSpreadResearch(); output=model.update(2300,28)
    assert output["research_only"] == 1.0 and np.isfinite(output["spread_residual"])

def test_metals_model_requires_promoted_reference_model():
    bundle=MetalsModelBundle(KalmanBasisTracker(),fit_regime(),LightGBMReturnModel(),LightGBMProbabilityModel(),promoted=False)
    with pytest.raises(RuntimeError): bundle.evaluate(asset='SILVER',symbol='SLVUSD',fair_value=100,local_state=state(symbol='SLVUSD'),reference_features=np.ones(5),liquidity_features=np.ones(11),cost=CostEstimate(1,1,1,1))

# ----------------------------- portfolio risk -------------------------------
def test_shrunk_ewma_covariance_and_expected_shortfall_are_active():
    cov,es=fitted_risk(['BTCUSD','XAUTUSD']); rows=[Exposure('BTCUSD','BTC','CRYPTO_BETA','USD',1000,1000,4,1,100),Exposure('XAUTUSD','METALS','METAL_BETA','USD',500,500,2,1,50)]
    assert cov.portfolio_volatility(rows)>0 and es.expected_shortfall(rows)>0

def test_allocator_sizes_on_stop_plus_stress_slippage_and_es():
    cov,es=fitted_risk(['BTCUSD']); allocator=PortfolioAllocator(exposure_book=ExposureBook(),covariance=cov,expected_shortfall=es,drawdown=DrawdownController(100,{'BTC':100}),portfolio_risk_cap=100,portfolio_es_cap=100,max_risk_per_opportunity=10)
    decision=allocator.size(desk='BTC',risk_group='CRYPTO_BETA',currency='USD',instrument='BTCUSD',expected_net_edge=5,unit_invalidation_risk=2,unit_stress_slippage_risk=1,unit_notional=100,unit_margin=10,liquidity_capacity_qty=10,available_margin=100,venue_step=1)
    assert decision.approved and decision.quantity==3 and decision.stress_slippage_risk==3

def test_allocator_expected_shortfall_cap_blocks_excess():
    cov,es=fitted_risk(['BTCUSD']); allocator=PortfolioAllocator(exposure_book=ExposureBook(),covariance=cov,expected_shortfall=es,drawdown=DrawdownController(1000,{'BTC':1000}),portfolio_risk_cap=1000,portfolio_es_cap=.01,max_risk_per_opportunity=100)
    decision=allocator.size(desk='BTC',risk_group='CRYPTO_BETA',currency='USD',instrument='BTCUSD',expected_net_edge=5,unit_invalidation_risk=1,unit_stress_slippage_risk=0.1,unit_notional=10000,unit_margin=1,liquidity_capacity_qty=1,available_margin=100,venue_step=1)
    assert not decision.approved and decision.reasons[0]=='EXPECTED_SHORTFALL_CAP'

def test_dynamic_leverage_and_groww_premium_risk():
    lev=DynamicLeverageSelector().select(maximum_leverage=100,unit_notional=1000,risk_to_invalidation=10,volatility_fraction=.002,execution_quality=.9,expected_net_edge_bps=10)
    assert lev.approved and 1 <= lev.selected_leverage < 100
    risk,ok=MarginPolicy.groww_long_option(20,25,10,5,1000); assert risk==515 and ok

# ----------------------------- research --------------------------------------
def test_forward_label_after_costs_and_coordinator_emits_both_sides(tmp_path):
    cost=CostEstimate(1,1,1,1); label=calculate_delta_forward_label(observation_ts_ns=0,observation_mid=100,future_prices=[101],horizon='1s',side='LONG',cost=cost)
    assert label.net_executable_return_bps==pytest.approx(96)
    store=ResearchStore(tmp_path); coordinator=ForwardLabelCoordinator(store,(1,)); coordinator.register_delta_observation(ts_ns=0,mid=100,cost=cost); emitted=coordinator.on_delta_mid(1_000_000_000,101)
    assert len(emitted)==2 and len(store.read('labels'))==2

def test_liquidity_and_underlying_outcome_coordinator_writes_labels(tmp_path):
    store=ResearchStore(tmp_path); coordinator=OutcomeCoordinator(store,max_plan_horizon_seconds=60)
    coordinator.register_liquidity_plan(ts_ns=0,instrument="BTCUSD",direction="LONG",zone_id="tp",plan=ProtectionPlan(100,102,98,98))
    assert coordinator.on_price(ts_ns=1_000_000_000,instrument="BTCUSD",price=102)[0].tp_before_sl == 1
    coordinator.register_underlying(ts_ns=0,underlying="NIFTY",side="BULLISH",entry=22000,invalidation=21950,target=22100,horizon_seconds=60)
    assert coordinator.on_underlying_price(ts_ns=2_000_000_000,underlying="NIFTY",price=22100)[0].move_before_invalidation == 1
    assert len(store.read("labels")) == 2

def test_model_registry_refuses_unvalidated_live_promotion(tmp_path):
    registry=ModelRegistry(tmp_path)
    with pytest.raises(ValueError): registry.register(ModelVersion('btc','v1','x',('a',),100,10,1.0,True,False))
    registry.register(ModelVersion('btc','v1','x',('a',),100,10,1.0,True,True)); assert registry.promoted('btc').version=='v1'

def test_attribution_and_replay_are_recorded(tmp_path):
    store=ResearchStore(tmp_path); result=AttributionEngine(store).close(execution_id='x',desk='BTC',instrument='BTCUSD',gross_pnl=-3,fee_cost=1,spread_cost=1,slippage_cost=1,impact_cost=0,mfe=1,mae=-4,expected_direction_correct=False,contract_efficiency_ok=True,protection_quality_ok=True,execution_quality_ok=True)
    assert result.net_pnl==-6 and result.primary_failure_reason=='WRONG_DIRECTION'
    replay=ReplayRunner().run([{'ts_ns':2},{'ts_ns':1}],lambda row:{'code':'NO_TRADE_EXECUTION_UNSAFE' if row['ts_ns']==1 else 'TRADE_APPROVED_WITH_PROTECTION_PLAN','protection_confirmed':row['ts_ns']==2})
    assert replay.orders_rejected_for_safety==1 and replay.protections_confirmed==1

def test_real_observation_trainers_publish_all_promoted_runtime_artifacts(tmp_path):
    rng=np.random.default_rng(33); n=80; publisher=ArtifactPublisher(tmp_path/"models", ModelRegistry(tmp_path/"models"))
    evidence=lambda name: ValidationEvidence(name,"v1",("f0",),n,np.full(20,.2),True)
    lagged=rng.normal(size=(n,34)); current=rng.normal(size=(n,35)); regime=np.vstack([rng.normal([0,.002,1,0,.1],[.01,.001,.1,.05,.02],size=(n//2,5)),rng.normal([0,.06,5,0,.3],[.01,.001,.1,.05,.02],size=(n//2,5))])
    liquidity=rng.normal(size=(n,11)); y=rng.normal(3,.5,n); binary=np.tile([0,1],n//2); slip=np.abs(rng.normal(1,.1,n))
    BTCTrainer(publisher).train(lagged_features=lagged,current_features=current,regime_features=regime,liquidity_features=liquidity,net_return_labels=y,tp_labels=binary,realised_slippage_bps=slip,passive_filled_labels=binary,evidence=evidence("btc_delta_net_edge"),promote=True)
    scores,outcomes=make_zone_scores(); LiquidityTrainer(publisher).train(scores=scores,outcomes=outcomes,evidence=evidence("liquidity"),promote=True)
    metal_features=rng.normal(size=(n,29)); MetalsTrainer(publisher).train(reference_regime_features=regime,combined_features=metal_features,local_net_return_labels=y,accepted_labels=binary,evidence=evidence("metals"),promote=True)
    direction=rng.normal(size=(n,7)); directions=np.tile([-1,1],n//2); IndiaTrainer(publisher).train(regime_features=regime,direction_features=direction,direction_labels=directions,expected_move_labels=np.abs(y)*20,favorable_event_labels=binary,adverse_event_labels=1-binary,iv_features=rng.normal(size=(n,4)),iv_change_labels=rng.normal(0,.01,n),evidence=evidence("india"),promote=True)
    returns={"BTCUSD":rng.normal(0,.001,n)}; RiskTrainer(publisher).train(returns=returns,stress_shocks={"BTCUSD":[-.03,-.05]},evidence=evidence("covariance"),promote=True)
    authority,cov,es=PromotedAuthorityLoader(tmp_path/"models").load()
    assert authority.btc.promoted and authority.option_iv.promoted and cov.covariance_ is not None and es.scenarios_ is not None

def test_training_refuses_fabricated_live_evidence(tmp_path):
    publisher=ArtifactPublisher(tmp_path/"models", ModelRegistry(tmp_path/"models"))
    with pytest.raises(ValueError):
        publisher.publish("x.joblib", {"x":1}, ValidationEvidence("x","v1",("f",),10,np.ones(2),False), promote=True)

# ----------------------------- execution -------------------------------------
class FakeDeltaAdapter:
    def __init__(self, order: dict, position: dict): self.order=order; self.position=position
    def place_protected_order(self, **kwargs): return {'id':'d1'}
    def get_order(self, order_id): return self.order
    def get_position(self, product_id): return self.position

def delta_product(): return DeltaProduct(1,mapping(),50.0,{})

def test_delta_does_not_mark_unfilled_order_active(tmp_path):
    executor=DeltaProtectedExecutor(FakeDeltaAdapter({'state':'open','filled_size':0},{}),StateStore(tmp_path),Observability(),max_status_checks=1,sleep=lambda _:None)
    receipt=executor.execute(desk='BTC',product=delta_product(),side='LONG',quantity=1,plan=ProtectionPlan(100,103,98,98))
    assert receipt.state is ProtectionState.RECONCILIATION_REQUIRED and receipt.filled_quantity==0

def test_delta_records_only_reconciled_protected_filled_quantity(tmp_path):
    adapter=FakeDeltaAdapter({'state':'partially_filled','filled_size':.4,'average_fill_price':100},{'size':.4,'protected_size':.4,'bracket_stop_loss_order':{'id':'sl'},'bracket_take_profit_order':{'id':'tp'}})
    executor=DeltaProtectedExecutor(adapter,StateStore(tmp_path),Observability(),max_status_checks=1,sleep=lambda _:None)
    receipt=executor.execute(desk='BTC',product=delta_product(),side='LONG',quantity=1,plan=ProtectionPlan(100,103,98,98))
    assert receipt.state is ProtectionState.ACTIVE_PROTECTED_POSITION and receipt.filled_quantity==pytest.approx(.4) and receipt.requested_quantity==1

def test_delta_rejects_filled_without_confirmed_brackets(tmp_path):
    adapter=FakeDeltaAdapter({'state':'filled','filled_size':1,'average_fill_price':100},{'size':1,'protected_size':0})
    receipt=DeltaProtectedExecutor(adapter,StateStore(tmp_path),Observability(),max_status_checks=1,sleep=lambda _:None).execute(desk='BTC',product=delta_product(),side='LONG',quantity=1,plan=ProtectionPlan(100,103,98,98))
    assert receipt.state is ProtectionState.RECONCILIATION_REQUIRED

class FakeGroww:
    def __init__(self, oco_active=True): self.oco_active=oco_active; self.cancelled=False; self.emergency=False
    def place_long_option_limit(self, **kwargs): return {'order_id':'g1'}
    def order_detail(self, order_id): return {'status':'FILLED','filled_quantity':25,'average_fill_price':20}
    def cancel_order(self, order_id): self.cancelled=True; return {}
    def positions(self): return [{'trading_symbol':'NIFTYCE','net_quantity':25}]
    def create_oco_exit(self, **kwargs): return {'smart_order_id':'oco1'}
    def smart_order(self, order_id): return {'status':'ACTIVE' if self.oco_active else 'FAILED','quantity':25}
    def quote(self, symbol): return {'bid_price':19}
    def emergency_limit_sell(self, **kwargs): self.emergency=True; return {'order_id':'emg1'}
    def cancel_oco_exit(self, order_id): return {'status':'CANCELLED'}
    def modify_oco_exit(self, **kwargs): return {'status':'ACTIVE'}

def groww_contract(): return GrowwContract('NIFTYCE','NSE-NIFTYCE','NIFTY','CE','2026-06-25',22000,25,.05,'tok',True)

def test_groww_fill_then_confirmed_oco_active(tmp_path):
    receipt=GrowwLongOptionExecutor(FakeGroww(),StateStore(tmp_path),Observability(),sleep=lambda _:None).execute(contract=groww_contract(),quantity=25,entry_limit=20,protection=ProtectionPlan(20,25,17,21900))
    assert receipt.state is ProtectionState.ACTIVE_PROTECTED_POSITION and receipt.protection_order_id=='oco1'

def test_groww_oco_failure_emergency_exits_and_halts(tmp_path):
    adapter=FakeGroww(oco_active=False); executor=GrowwLongOptionExecutor(adapter,StateStore(tmp_path),Observability(),sleep=lambda _:None)
    receipt=executor.execute(contract=groww_contract(),quantity=25,entry_limit=20,protection=ProtectionPlan(20,25,17,21900))
    assert receipt.state is ProtectionState.MANUAL_EMERGENCY_EXIT and executor.entries_halted and adapter.emergency

def test_option_position_monitor_theta_iv_and_thesis_rules():
    monitor=GrowwOptionPositionMonitor()
    assert monitor.should_exit(remaining_expected_premium_edge=2,projected_theta_loss=3,realised_iv_edge=0,thesis_valid=True)[0]
    assert monitor.should_exit(remaining_expected_premium_edge=5,projected_theta_loss=1,realised_iv_edge=0,thesis_valid=False)[0]

def test_groww_monitored_thesis_exit_cancels_oco_then_uses_validated_limit_and_halts(tmp_path):
    adapter=FakeGroww(); executor=GrowwLongOptionExecutor(adapter,StateStore(tmp_path),Observability(),sleep=lambda _:None)
    active=executor.execute(contract=groww_contract(),quantity=25,entry_limit=20,protection=ProtectionPlan(20,25,17,21900))
    closed=executor.monitor_position(receipt=active,contract=groww_contract(),quantity=25,remaining_expected_premium_edge=2,projected_theta_loss=3,realised_iv_edge=0,thesis_valid=True)
    assert closed.state is ProtectionState.MANUAL_EMERGENCY_EXIT and executor.entries_halted and adapter.emergency

# ----------------------------- streams/platform wiring ----------------------
class StubLiquidity:
    promoted=True; version='stub-liquidity'
    def predict_zone(self, zone):
        if zone.tp_utility_score > 0: return replace(zone,predicted_valid_entry_probability=.1,predicted_tp_before_sl_probability=.8,predicted_stop_sweep_probability=.8)
        return replace(zone,predicted_valid_entry_probability=.9,predicted_tp_before_sl_probability=.1,predicted_stop_sweep_probability=.1)

class StubBTC:
    promoted=True; model_version='stub-btc'
    def signed_forecast(self, **kwargs): return 10.0,np.zeros(35),5.0,15.0
    def predict(self, **kwargs): return Direction.LONG, executable_edge(10,CostEstimate(1,1,1,1),tp_probability=.8,reward_bps=30,risk_bps=20), {'tp_before_sl_probability':.8}

class StubIndia:
    promoted=True
    def evaluate(self, **kwargs):
        from core.identifiers import IndiaUnderlyingOpportunity
        return IndiaUnderlyingOpportunity('NIFTY','trend','BULLISH',100,15,.8,21900,[22100],{},None)

class StubIV:
    promoted=True; version="iv-stub"
    def predict(self, features): return .01

class DummyDeltaExecAdapter(FakeDeltaAdapter): pass

class DummyGrowwAdapter(FakeGroww): pass

def test_platform_btc_direction_selects_liquidity_after_model_direction(tmp_path):
    cov,es=fitted_risk(['BTCUSD']); cfg=platform_policy(tmp_path)
    platform=InstitutionalPlatform(cfg,models=ModelAuthority(StubBTC(),StubLiquidity(),None,None),covariance=cov,expected_shortfall=es,delta_adapter=DummyDeltaExecAdapter({'state':'filled','filled_size':1},{'size':1,'protected_size':1,'bracket_stop_loss_order':{'id':'sl'},'bracket_take_profit_order':{'id':'tp'}}))
    obs=BTCObservation(1_000,state(),{'coinswitch':state('coinswitch',mid=100.1),'hyperliquid':state('hyperliquid',mid=100.05)},np.zeros((2,34)),.001,zones_long(),CostEstimate(1,1,0,1),delta_product(),100,1)
    decision=platform.evaluate_btc(obs)
    assert decision.direction=='LONG' and decision.protection_plan.stop_price < decision.protection_plan.entry_price < decision.protection_plan.target_price
    assert platform.research.read("observations")[-1]["desk"] == "BTC"

def test_platform_india_loads_only_directional_ce_candidates_after_underlying_approval(tmp_path):
    cov,es=fitted_risk(['NIFTYCE']); cfg=platform_policy(tmp_path, max_option_premium_risk_inr=100000); called=[]
    platform=InstitutionalPlatform(cfg,models=ModelAuthority(None,None,None,StubIndia(),StubIV()),covariance=cov,expected_shortfall=es,groww_adapter=DummyGrowwAdapter())
    premium=black76_price(22000,22000,.065,30/365,.20,'CE'); expiry=(datetime.now(UTC)+timedelta(days=30)).date().isoformat()
    def loader(option_type):
        called.append(option_type); c=OptionChainCandidate('NIFTYCE',option_type,expiry,22000,premium-1,premium+1,500,1000,1000,25,True)
        return [c],{'NIFTYCE':groww_contract()}
    obs=IndiaObservation(1,'NIFTY',22000,22000,np.ones(4),np.ones(5),np.ones(3),50,{},loader,20,10,50000)
    decision=platform.evaluate_india(obs)
    assert called==['CE'] and decision.instrument=='NIFTYCE'


def test_common_trading_restriction_rejects_before_model_authority(tmp_path):
    cov,es=fitted_risk(["BTCUSD"]); platform=InstitutionalPlatform(platform_policy(tmp_path),models=ModelAuthority(None,None,None,None),covariance=cov,expected_shortfall=es)
    obs=BTCObservation(1_000,state(),{},np.zeros((2,34)),.001,zones_long(),CostEstimate(1,1,0,1),delta_product(),100,1,TradingRestrictionState(True,True,True,True,"MACRO_EVENT_BLOCK"))
    decision=platform.evaluate_btc(obs)
    assert decision.code is DecisionCode.NO_TRADE_EXECUTION_UNSAFE and decision.reasons == ("MACRO_EVENT_BLOCK",)


def test_dashboard_reads_current_strategy_authority_status_schema(tmp_path):
    cov,es=fitted_risk(["BTCUSD"]); platform=InstitutionalPlatform(platform_policy(tmp_path),models=ModelAuthority(None,None,None,None),covariance=cov,expected_shortfall=es)
    status=platform.status(); html=DashboardServer.html()
    assert status["strategy_authority_ready"] and "live_flags" in status and "p.live_flags.groww" in html

async def async_source(items):
    for item in items: yield item

def parser(raw, mp, ts):
    if raw.get('heartbeat'): return ParsedVenueUpdate(heartbeat=True)
    snap=BookSnapshot(mp,[BookLevel(99.995,10)],[BookLevel(100.005,10)],ts-1_000_000,ts,True)
    return ParsedVenueUpdate(snapshot=snap)

def test_btc_stream_requires_observed_delta_heartbeat_before_emitting():
    product=delta_product(); warm=zones_long(); inputs=lambda _: DeltaDeskRuntimeInputs(1,1,1,1,1,100,1)
    async def run(no_heartbeat):
        snapshot={'type':'ob_l2','sy':'BTCUSD','b':[[99.995,10]],'a':[[100.005,10]],'ts':1000000}
        delta_items=[snapshot] if no_heartbeat else [{'type':'heartbeat'},snapshot,snapshot]
        stream=BTCVerifiedMarketStream(delta_product=product,delta_source=async_source(delta_items),reference_sources={'coinswitch':(mapping('coinswitch'),async_source([{'snapshot':True},{'snapshot':True}]),parser)},inputs_provider=inputs,clock_ns=lambda:1_001_000_000,warmup_zones=warm,model_lags=0)
        rows=[]
        async def collect():
            async for row in stream:
                rows.append(row); break
        try: await asyncio.wait_for(collect(),timeout=.05)
        except (asyncio.TimeoutError, StopAsyncIteration): pass
        return rows
    assert asyncio.run(run(True)) == []
    assert len(asyncio.run(run(False))) == 1

# ----------------------------- Groww full feature certification -----------------------------
# This remains in the single mandated test module. Default tests are deterministic and make no
# broker calls. Opt-in tests at the end exercise the real API only after explicit acknowledgement.

class GrowwSDKRecorder:
    """Documented Groww SDK response/transport double; records exact method contracts."""
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.instruments_rows = [
            {"exchange":"NSE","segment":"FNO","underlying_symbol":"NIFTY","instrument_type":"CE","expiry_date":"2026-06-25","strike_price":"22000","lot_size":"25","tick_size":"0.05","exchange_token":"ce-token","groww_symbol":"NSE-NIFTY-25Jun26-22000-CE","trading_symbol":"NIFTY26JUN22000CE","buy_allowed":"1"},
            {"exchange":"NSE","segment":"FNO","underlying_symbol":"NIFTY","instrument_type":"PE","expiry_date":"2026-06-25","strike_price":"22000","lot_size":"25","tick_size":"0.05","exchange_token":"pe-token","groww_symbol":"NSE-NIFTY-25Jun26-22000-PE","trading_symbol":"NIFTY26JUN22000PE","buy_allowed":"1"},
            {"exchange":"NSE","segment":"FNO","underlying_symbol":"NIFTY","instrument_type":"CE","expiry_date":"2026-05-14","strike_price":"21900","lot_size":"25","tick_size":"0.05","exchange_token":"expired","groww_symbol":"NSE-NIFTY-14May26-21900-CE","trading_symbol":"EXPIREDCE","buy_allowed":"1"},
            {"exchange":"NSE","segment":"FNO","underlying_symbol":"NIFTY","instrument_type":"CE","expiry_date":"2026-06-25","strike_price":"22100","lot_size":"25","tick_size":"0.05","exchange_token":"no-buy","groww_symbol":"NSE-NIFTY-25Jun26-22100-CE","trading_symbol":"NOBUYCE","buy_allowed":"0"},
            {"exchange":"NSE","segment":"CASH","underlying_symbol":"NIFTY","instrument_type":"INDEX","expiry_date":"","strike_price":"0","lot_size":"1","tick_size":"0.05","exchange_token":"NIFTY","groww_symbol":"NSE-NIFTY","trading_symbol":"NIFTY","buy_allowed":"1"},
        ]
    def _record(self, method: str, **kwargs):
        self.calls.append((method, kwargs))
    def get_all_instruments(self):
        self._record("get_all_instruments")
        return self.instruments_rows
    def get_expiries(self, **kwargs):
        self._record("get_expiries", **kwargs); return {"expiries":["2026-06-25"]}
    def get_contracts(self, **kwargs):
        self._record("get_contracts", **kwargs); return {"contracts":["NSE-NIFTY-25Jun26-22000-CE"]}
    def get_option_chain(self, **kwargs):
        self._record("get_option_chain", **kwargs)
        return {"underlying_ltp":22000.0,"strikes":{"22000":{"CE":{"trading_symbol":"NIFTY26JUN22000CE","ltp":120.5,"open_interest":3200,"volume":880,"greeks":{"delta":.52,"gamma":.001,"theta":-4.2,"vega":11.0,"iv":14.5}},"PE":{"trading_symbol":"NIFTY26JUN22000PE","ltp":118.0,"open_interest":3000,"volume":820,"greeks":{"delta":-.48,"gamma":.001,"theta":-4.0,"vega":10.8,"iv":14.4}}}}}
    def get_greeks(self, **kwargs):
        self._record("get_greeks", **kwargs); return {"greeks":{"delta":.52,"gamma":.001,"theta":-4.2,"vega":11.0,"iv":14.5}}
    def get_quote(self, **kwargs):
        self._record("get_quote", **kwargs); return {"last_price":120.5,"bid_price":120.0,"offer_price":121.0,"bid_quantity":100,"offer_quantity":75,"open_interest":3200,"volume":880}
    def get_ltp(self, **kwargs):
        self._record("get_ltp", **kwargs); return {"NSE_NIFTY26JUN22000CE":120.5}
    def get_ohlc(self, **kwargs):
        self._record("get_ohlc", **kwargs); return {"NSE_NIFTY26JUN22000CE":{"open":110,"high":125,"low":108,"close":120.5}}
    def get_historical_candles(self, **kwargs):
        self._record("get_historical_candles", **kwargs); return {"candles":[["2026-05-26 09:15:00",110,120,109,119,1500,3200]]}
    def get_holdings_for_user(self):
        self._record("get_holdings_for_user"); return {"holdings":[{"trading_symbol":"RELIANCE","quantity":1}]}
    def get_positions_for_user(self, **kwargs):
        self._record("get_positions_for_user", **kwargs); return {"positions":[{"trading_symbol":"NIFTY26JUN22000CE","quantity":25,"net_quantity":25,"segment":"FNO"}]}
    def get_position_for_trading_symbol(self, **kwargs):
        self._record("get_position_for_trading_symbol", **kwargs); return {"positions":[{"trading_symbol":"NIFTY26JUN22000CE","quantity":25}]}
    def get_available_margin_details(self):
        self._record("get_available_margin_details"); return {"clear_cash":50000.0,"fno_margin_details":{"option_buy_balance_available":45000.0}}
    def get_order_margin_details(self, **kwargs):
        self._record("get_order_margin_details", **kwargs); return {"option_buy_premium":3012.5,"brokerage_and_charges":20.0,"total_requirement":3032.5}
    def place_order(self, **kwargs):
        self._record("place_order", **kwargs); return {"groww_order_id":"entry-1","order_status":"OPEN","order_reference_id":kwargs["order_reference_id"]}
    def modify_order(self, **kwargs):
        self._record("modify_order", **kwargs); return {"groww_order_id":kwargs["groww_order_id"],"order_status":"OPEN"}
    def get_order_detail(self, **kwargs):
        self._record("get_order_detail", **kwargs); return {"groww_order_id":kwargs["groww_order_id"],"order_status":"EXECUTED","filled_quantity":25,"average_fill_price":120.5}
    def get_order_status(self, **kwargs):
        self._record("get_order_status", **kwargs); return {"groww_order_id":kwargs["groww_order_id"],"order_status":"EXECUTED","filled_quantity":25}
    def get_order_status_by_reference(self, **kwargs):
        self._record("get_order_status_by_reference", **kwargs); return {"groww_order_id":"entry-1","order_reference_id":kwargs["order_reference_id"],"order_status":"EXECUTED"}
    def get_order_list(self, **kwargs):
        self._record("get_order_list", **kwargs); return {"order_list":[{"groww_order_id":"entry-1","order_status":"EXECUTED"}]}
    def get_trade_list_for_order(self, **kwargs):
        self._record("get_trade_list_for_order", **kwargs); return {"trade_list":[{"groww_order_id":kwargs["groww_order_id"],"trade_status":"EXECUTED","quantity":25}]}
    def cancel_order(self, **kwargs):
        self._record("cancel_order", **kwargs); return {"groww_order_id":kwargs["groww_order_id"],"order_status":"CANCELLED"}
    def create_smart_order(self, **kwargs):
        self._record("create_smart_order", **kwargs); return {"smart_order_id":"oco-1","smart_order_type":"OCO","status":"ACTIVE","quantity":kwargs["quantity"]}
    def get_smart_order(self, **kwargs):
        self._record("get_smart_order", **kwargs); return {"smart_order_id":kwargs["smart_order_id"],"status":"ACTIVE","quantity":25}
    def modify_smart_order(self, **kwargs):
        self._record("modify_smart_order", **kwargs); return {"smart_order_id":kwargs["smart_order_id"],"status":"ACTIVE","quantity":kwargs["quantity"]}
    def cancel_smart_order(self, **kwargs):
        self._record("cancel_smart_order", **kwargs); return {"smart_order_id":kwargs["smart_order_id"],"status":"CANCELLED"}
    def get_smart_order_list(self, **kwargs):
        self._record("get_smart_order_list", **kwargs); return {"orders":[{"smart_order_id":"oco-1","trading_symbol":"NIFTY26JUN22000CE","status":"ACTIVE","quantity":25}]}


def full_groww_adapter(client=None, *, live=True) -> GrowwAdapter:
    return GrowwAdapter(client or GrowwSDKRecorder(), live_orders_enabled=live, approved_static_ips=("203.0.113.10",), outbound_ip_resolver=lambda: "203.0.113.10")


def sdk_call(client: GrowwSDKRecorder, name: str) -> dict:
    return next(kwargs for method, kwargs in reversed(client.calls) if method == name)


def official_contract() -> GrowwContract:
    return GrowwContract("NIFTY26JUN22000CE", "NSE-NIFTY-25Jun26-22000-CE", "NIFTY", "CE", "2026-06-25", 22000, 25, .05, "ce-token", True)


def test_groww_compliance_is_fail_closed_until_live_and_static_ip_approved():
    client=GrowwSDKRecorder()
    assert GrowwAdapter(client).compliance().reason == "GROWW_LIVE_ORDERS_DISABLED"
    assert GrowwAdapter(client, live_orders_enabled=True).compliance().reason == "NO_APPROVED_STATIC_OUTBOUND_IP"
    mismatch=GrowwAdapter(client, live_orders_enabled=True, approved_static_ips=("203.0.113.10",), outbound_ip_resolver=lambda:"203.0.113.11").compliance()
    assert not mismatch.ready and mismatch.reason == "OUTBOUND_IP_NOT_APPROVED"
    assert full_groww_adapter(client).compliance().ready


def test_groww_auth_access_token_api_secret_and_totp_paths(monkeypatch):
    created=[]; acquired=[]
    class FakeAPI:
        def __init__(self, token): created.append(token)
        @staticmethod
        def get_access_token(**kwargs): acquired.append(kwargs); return "minted-token"
    fake_module=types.SimpleNamespace(GrowwAPI=FakeAPI)
    monkeypatch.setitem(sys.modules,"growwapi",fake_module)
    token_adapter=GrowwAdapter.from_secrets(Secrets(groww_access_token="existing-token"))
    secret_adapter=GrowwAdapter.from_secrets(Secrets(groww_api_key="key",groww_api_secret="secret"))
    class FakeTOTP:
        def __init__(self, secret): assert secret == "totp-secret"
        def now(self): return "123456"
    monkeypatch.setitem(sys.modules,"pyotp",types.SimpleNamespace(TOTP=FakeTOTP))
    totp_adapter=GrowwAdapter.from_secrets(Secrets(groww_api_key="key",groww_totp_secret="totp-secret"))
    assert all(isinstance(adapter,GrowwAdapter) for adapter in (token_adapter,secret_adapter,totp_adapter))
    assert created == ["existing-token","minted-token","minted-token"]
    assert acquired == [{"api_key":"key","secret":"secret"},{"api_key":"key","totp":"123456"}]
    with pytest.raises(RuntimeError,match="GROWW_AUTH_CREDENTIALS_NOT_CONFIGURED"):
        GrowwAdapter.from_secrets(Secrets())


def test_groww_instrument_master_discovers_only_current_buyable_nse_options_and_expiries():
    adapter=full_groww_adapter(); contracts=adapter.discover_contracts("NIFTY", today=datetime(2026,5,26).date())
    assert [item.trading_symbol for item in contracts] == ["NIFTY26JUN22000CE","NIFTY26JUN22000PE"]
    assert adapter.discover_current_expiries("NIFTY",today=datetime(2026,5,26).date()) == ["2026-06-25"]
    assert all(item.exchange_token and item.groww_symbol and item.lot_size == 25 and item.tick_size == .05 for item in contracts)


def test_groww_historical_expiries_contracts_and_candles_use_documented_v2_shape():
    client=GrowwSDKRecorder(); adapter=full_groww_adapter(client)
    assert adapter.historical_expiries("NIFTY", year=2026, month=6) == ["2026-06-25"]
    assert adapter.historical_contracts("NIFTY","2026-06-25") == ["NSE-NIFTY-25Jun26-22000-CE"]
    candles=adapter.historical_candles(groww_symbol="NSE-NIFTY-25Jun26-22000-CE",segment="FNO",start_time="2026-05-25 09:15:00",end_time="2026-05-25 09:16:00",candle_interval="1minute")
    assert candles and sdk_call(client,"get_historical_candles") == {"exchange":"NSE","segment":"FNO","groww_symbol":"NSE-NIFTY-25Jun26-22000-CE","start_time":"2026-05-25 09:15:00","end_time":"2026-05-25 09:16:00","candle_interval":"1minute"}
    with pytest.raises(ValueError,match="UNSUPPORTED_GROWW_CANDLE_INTERVAL"):
        adapter.historical_candles(groww_symbol="x",segment="FNO",start_time="a",end_time="b",candle_interval="legacy-minute")


def test_groww_option_chain_greeks_quote_ltp_and_ohlc_calls_are_documented():
    client=GrowwSDKRecorder(); adapter=full_groww_adapter(client)
    assert adapter.option_chain("NIFTY","2026-06-25")["underlying_ltp"] == 22000
    assert adapter.greeks("NIFTY","NIFTY26JUN22000CE","2026-06-25")["greeks"]["delta"] == .52
    assert adapter.quote("NIFTY26JUN22000CE")["bid_price"] == 120
    assert adapter.ltp("NSE_NIFTY26JUN22000CE")["NSE_NIFTY26JUN22000CE"] == 120.5
    assert "NSE_NIFTY26JUN22000CE" in adapter.ohlc(("NSE_NIFTY26JUN22000CE",))
    assert sdk_call(client,"get_option_chain") == {"exchange":"NSE","underlying":"NIFTY","expiry_date":"2026-06-25"}
    assert sdk_call(client,"get_greeks") == {"exchange":"NSE","underlying":"NIFTY","trading_symbol":"NIFTY26JUN22000CE","expiry":"2026-06-25"}


def test_groww_two_stage_rankable_watchlist_reads_only_requested_direction_and_valid_costs():
    client=GrowwSDKRecorder(); adapter=full_groww_adapter(client)
    contracts=adapter.discover_contracts("NIFTY",today=datetime(2026,5,26).date())
    rows=adapter.rankable_option_rows(underlying="NIFTY",expiry="2026-06-25",option_type="CE",contracts=contracts,validated_fee_per_lot=20,slippage_bps=4)
    assert len(rows)==1 and rows[0]["option_type"] == "CE" and rows[0]["greeks"]["theta"] < 0 and rows[0]["executable_depth_qty"] == 75
    with pytest.raises(ValueError,match="VALIDATED_OPTION_COSTS_REQUIRED"):
        adapter.rankable_option_rows(underlying="NIFTY",expiry="2026-06-25",option_type="CE",contracts=contracts,validated_fee_per_lot=-1,slippage_bps=0)
    with pytest.raises(ValueError,match="LONG_OPTION_DIRECTION_MUST_BE_CE_OR_PE"):
        adapter.rankable_option_rows(underlying="NIFTY",expiry="2026-06-25",option_type="FUT",contracts=contracts,validated_fee_per_lot=1,slippage_bps=0)


def test_groww_account_positions_holdings_and_margin_features_are_mapped():
    client=GrowwSDKRecorder(); adapter=full_groww_adapter(client); contract=official_contract()
    assert adapter.holdings()[0]["trading_symbol"] == "RELIANCE"
    assert adapter.positions()[0]["net_quantity"] == 25
    assert adapter.position_for_symbol(contract.trading_symbol)[0]["quantity"] == 25
    assert adapter.available_margin()["fno_margin_details"]["option_buy_balance_available"] == 45000
    margin=adapter.order_margin_for_long_option(contract=contract,quantity=25,limit_price=120.5)
    assert margin["total_requirement"] == 3032.5
    assert sdk_call(client,"get_order_margin_details")["orders"][0] == {"trading_symbol":"NIFTY26JUN22000CE","transaction_type":"BUY","quantity":25,"price":120.5,"order_type":"LIMIT","product":"MIS","exchange":"NSE"}
    with pytest.raises(ValueError,match="validated long CE/PE lot quantity required"):
        adapter.order_margin_for_long_option(contract=contract,quantity=1,limit_price=120.5)


def test_groww_long_option_order_place_modify_status_trade_list_and_cancel_payloads():
    client=GrowwSDKRecorder(); adapter=full_groww_adapter(client); contract=official_contract()
    placed=adapter.place_long_option_limit(contract=contract,quantity=25,limit_price=120.5,reference_id="entry-ref-123")
    assert placed["groww_order_id"] == "entry-1"
    assert sdk_call(client,"place_order") == {"trading_symbol":"NIFTY26JUN22000CE","quantity":25,"validity":"DAY","exchange":"NSE","segment":"FNO","product":"MIS","order_type":"LIMIT","transaction_type":"BUY","price":120.5,"order_reference_id":"entry-ref-123"}
    adapter.modify_pending_long_option_limit(groww_order_id="entry-1",quantity=25,limit_price=120.0)
    assert sdk_call(client,"modify_order") == {"groww_order_id":"entry-1","segment":"FNO","quantity":25,"order_type":"LIMIT","price":120.0}
    adapter.order_detail("entry-1"); assert sdk_call(client,"get_order_detail") == {"groww_order_id":"entry-1","segment":"FNO"}
    assert adapter.order_status("entry-1")["filled_quantity"] == 25
    assert adapter.order_status_by_reference("entry-ref-123")["groww_order_id"] == "entry-1"
    assert adapter.order_list()[0]["order_status"] == "EXECUTED"
    assert adapter.trade_list_for_order("entry-1")[0]["trade_status"] == "EXECUTED"
    assert adapter.cancel_order("entry-1")["order_status"] == "CANCELLED"
    with pytest.raises(ValueError):
        adapter.place_long_option_limit(contract=contract,quantity=1,limit_price=120.5,reference_id="bad-quantity")


def test_groww_oco_target_stoploss_create_confirm_modify_list_and_cancel_payloads():
    client=GrowwSDKRecorder(); adapter=full_groww_adapter(client); contract=official_contract()
    created=adapter.create_oco_exit(contract=contract,quantity=25,net_position_quantity=25,target_trigger=150,target_price=150,stop_trigger=100,reference_id="oco-ref-123")
    assert created["smart_order_id"] == "oco-1"
    create_payload=sdk_call(client,"create_smart_order")
    assert create_payload["smart_order_type"] == "OCO" and create_payload["transaction_type"] == "SELL"
    assert create_payload["target"] == {"trigger_price":"150.00","order_type":"LIMIT","price":"150.00"}
    assert create_payload["stop_loss"] == {"trigger_price":"100.00","order_type":"SL_M","price":None}
    assert adapter.smart_order("oco-1")["status"] == "ACTIVE"
    assert adapter.modify_oco_exit(smart_order_id="oco-1",quantity=25,target_trigger=155,stop_trigger=105)["status"] == "ACTIVE"
    modified=sdk_call(client,"modify_smart_order"); assert modified["target"] == {"trigger_price":"155.00"} and modified["stop_loss"] == {"trigger_price":"105.00"}
    assert adapter.active_oco_orders(start_date_time="2026-05-26T09:15:00",end_date_time="2026-05-26T15:30:00")[0]["quantity"] == 25
    assert adapter.cancel_oco_exit("oco-1")["status"] == "CANCELLED"
    with pytest.raises(ValueError,match="OCO quantity"):
        adapter.create_oco_exit(contract=contract,quantity=50,net_position_quantity=25,target_trigger=150,target_price=150,stop_trigger=100,reference_id="oco-too-large")


class PartialFillGroww(FakeGroww):
    def __init__(self):
        super().__init__(); self.protected_quantity=0
    def place_long_option_limit(self, **kwargs): return {"groww_order_id":"entry-partial"}
    def order_detail(self, order_id): return {"order_status":"OPEN","filled_quantity":25,"average_fill_price":20}
    def positions(self): return [{"trading_symbol":"NIFTYCE","net_quantity":25}]
    def create_oco_exit(self, **kwargs): self.protected_quantity=kwargs["quantity"]; return {"smart_order_id":"oco-partial"}
    def smart_order(self, order_id): return {"status":"ACTIVE","quantity":self.protected_quantity}


def test_groww_partial_fill_cancels_remainder_and_protects_filled_quantity_only(tmp_path):
    adapter=PartialFillGroww(); executor=GrowwLongOptionExecutor(adapter,StateStore(tmp_path),Observability(),sleep=lambda _:None)
    receipt=executor.execute(contract=groww_contract(),quantity=50,entry_limit=20,protection=ProtectionPlan(20,25,17,21900))
    assert receipt.state is ProtectionState.ACTIVE_PROTECTED_POSITION and receipt.filled_quantity == 25
    assert adapter.cancelled and adapter.protected_quantity == 25


class NoFillGroww(FakeGroww):
    def order_detail(self, order_id): return {"status":"REJECTED","filled_quantity":0,"average_fill_price":0}


def test_groww_rejected_unfilled_entry_never_places_tp_sl_or_emergency_sell(tmp_path):
    adapter=NoFillGroww(); executor=GrowwLongOptionExecutor(adapter,StateStore(tmp_path),Observability(),max_status_checks=1,sleep=lambda _:None)
    with pytest.raises(RuntimeError,match="GROWW_ENTRY_NOT_FILLED"):
        executor.execute(contract=groww_contract(),quantity=25,entry_limit=20,protection=ProtectionPlan(20,25,17,21900))
    assert not adapter.emergency


def test_groww_restart_reconciliation_blocks_any_position_not_fully_covered_by_active_oco():
    client=GrowwSDKRecorder(); adapter=full_groww_adapter(client)
    assert GrowwReconciler(adapter).run().safe_to_enter
    client.get_smart_order_list=lambda **kwargs: {"orders":[]}
    result=GrowwReconciler(adapter).run()
    assert not result.safe_to_enter and result.unprotected_positions == {"NIFTY26JUN22000CE":25}


class EmergencyQuoteAdapter(FakeGroww):
    def __init__(self, bid): super().__init__(); self.bid=bid; self.sold_at=None
    def quote(self, symbol): return {"bid_price":self.bid}
    def emergency_limit_sell(self, **kwargs): self.sold_at=kwargs["limit_price"]; return {"groww_order_id":"emergency-1"}


def test_groww_emergency_exit_requires_executable_bid_and_rounds_sell_to_tick():
    base=ExecutionReceipt("x1","INDIA_OPTIONS","NIFTYCE",ProtectionState.UNPROTECTED_POSITION_EMERGENCY,requested_quantity=25,filled_quantity=25)
    with pytest.raises(RuntimeError,match="NO_EXECUTABLE_BID"):
        GrowwEmergencyExit(EmergencyQuoteAdapter(0),Observability()).execute(receipt=base,contract=groww_contract(),quantity=25)
    adapter=EmergencyQuoteAdapter(19.039)
    closed=GrowwEmergencyExit(adapter,Observability()).execute(receipt=base,contract=groww_contract(),quantity=25)
    assert closed.state is ProtectionState.MANUAL_EMERGENCY_EXIT and adapter.sold_at == pytest.approx(19.0)


class FakeGrowwFeed:
    def __init__(self): self.callbacks={}; self.unsubscribed=[]
    def subscribe_index_value(self, tokens, on_data_received): self.callbacks["index_value"]=on_data_received
    def subscribe_ltp(self, tokens, on_data_received): self.callbacks["ltp"]=on_data_received
    def subscribe_market_depth(self, tokens, on_data_received): self.callbacks["market_depth"]=on_data_received
    def subscribe_fno_order_updates(self, on_data_received): self.callbacks["order_updates"]=on_data_received
    def subscribe_fno_position_updates(self, on_data_received): self.callbacks["position_updates"]=on_data_received
    def consume(self):
        for feed_type in ("index_value","ltp","market_depth","order_updates","position_updates"):
            self.callbacks[feed_type]({"feed_type":feed_type,"segment":"FNO"})
    def get_index_value(self): return {"NSE":{"CASH":{"NIFTY":{"value":22000}}}}
    def get_ltp(self): return {"ltp":{"NSE":{"FNO":{"ce-token":{"ltp":120.5}}}}}
    def get_market_depth(self): return {"NSE":{"FNO":{"ce-token":{"buyBook":{},"sellBook":{}}}}}
    def get_fno_order_update(self): return {"orderStatus":"EXECUTED","filledQty":25}
    def get_fno_position_update(self): return {"symbolIsin":"NIFTY26JUN22000CE"}
    def unsubscribe_index_value(self,tokens): self.unsubscribed.append("index")
    def unsubscribe_ltp(self,tokens): self.unsubscribed.append("ltp")
    def unsubscribe_market_depth(self,tokens): self.unsubscribed.append("depth")
    def unsubscribe_fno_order_updates(self): self.unsubscribed.append("order")
    def unsubscribe_fno_position_updates(self): self.unsubscribed.append("position")


def test_groww_feed_bridge_covers_index_ltp_depth_order_and_position_updates():
    async def run():
        fake=FakeGrowwFeed(); bridge=GrowwFeedBridge(object(),feed_client=fake)
        stream=bridge.stream(index_tokens=[{"exchange":"NSE","segment":"CASH","exchange_token":"NIFTY"}],derivative_tokens=[{"exchange":"NSE","segment":"FNO","exchange_token":"ce-token"}])
        events=[]
        try:
            for _ in range(5): events.append(await asyncio.wait_for(anext(stream),timeout=1))
        finally:
            await stream.aclose()
        return events,fake.unsubscribed
    events,unsubscribed=asyncio.run(run())
    assert {event["type"] for event in events} == {"index","ltp","depth","order","position"}
    assert set(unsubscribed) == {"index","ltp","depth","order","position"}


# ----------------------------- Groww opt-in real API tests -----------------------------
# These are deliberately skipped by default. They exercise a real account only when the user
# sets explicit acknowledgement variables. There is no fake "live pass" without transmitting.

def live_groww_adapter_from_environment() -> GrowwAdapter:
    required = ["GROWW_LIVE_TEST_APPROVED_IP"]
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        pytest.skip(f"missing live Groww configuration: {missing}")
    adapter=GrowwAdapter.from_secrets(
        Secrets(),
        live_orders_enabled=True,
        approved_static_ips=(os.environ["GROWW_LIVE_TEST_APPROVED_IP"],),
    )
    status=adapter.compliance()
    assert status.ready, status.reason
    return adapter


@pytest.mark.skipif(os.getenv("GROWW_RUN_READONLY_API_TESTS") != "YES", reason="set GROWW_RUN_READONLY_API_TESTS=YES with credentials to call real read-only Groww APIs")
def test_live_groww_readonly_feature_smoke_contract_discovery_chain_quote_greeks_margin_and_positions():
    adapter=GrowwAdapter.from_secrets(Secrets())
    contracts=adapter.discover_contracts(os.getenv("GROWW_TEST_UNDERLYING","NIFTY"))
    assert contracts, "No currently tradable buyable NIFTY option contracts discovered"
    selected=contracts[0]
    assert adapter.option_chain(selected.underlying,selected.expiry).get("strikes")
    assert adapter.greeks(selected.underlying,selected.trading_symbol,selected.expiry).get("greeks")
    assert adapter.quote(selected.trading_symbol).get("last_price") is not None
    assert adapter.available_margin().get("fno_margin_details") is not None
    assert isinstance(adapter.positions(),list)
    assert adapter.order_margin_for_long_option(contract=selected,quantity=selected.lot_size,limit_price=float(adapter.quote(selected.trading_symbol)["offer_price"]))["total_requirement"] > 0


@pytest.mark.skipif(os.getenv("GROWW_RUN_LIVE_ORDER_TESTS") != "YES_I_ACCEPT_REAL_FNO_ORDER_AND_OCO", reason="set explicit live-order acknowledgement to send a real F&O order and real OCO TP/SL")
def test_live_groww_real_long_option_buy_and_real_oco_tp_sl_placement(tmp_path):
    """DESTRUCTIVE TEST: intentionally transmits a real option BUY and real OCO target/SL."""
    adapter=live_groww_adapter_from_environment()
    required=("GROWW_TEST_TRADING_SYMBOL","GROWW_TEST_GROWW_SYMBOL","GROWW_TEST_EXPIRY","GROWW_TEST_OPTION_TYPE","GROWW_TEST_STRIKE","GROWW_TEST_LOT_SIZE","GROWW_TEST_TICK_SIZE","GROWW_TEST_EXCHANGE_TOKEN","GROWW_TEST_ENTRY_PRICE","GROWW_TEST_TARGET_PRICE","GROWW_TEST_STOP_PRICE")
    missing=[key for key in required if not os.getenv(key)]
    if missing: pytest.skip(f"missing destructive test parameters: {missing}")
    contract=GrowwContract(os.environ["GROWW_TEST_TRADING_SYMBOL"],os.environ["GROWW_TEST_GROWW_SYMBOL"],os.getenv("GROWW_TEST_UNDERLYING","NIFTY"),os.environ["GROWW_TEST_OPTION_TYPE"].upper(),os.environ["GROWW_TEST_EXPIRY"],float(os.environ["GROWW_TEST_STRIKE"]),int(os.environ["GROWW_TEST_LOT_SIZE"]),float(os.environ["GROWW_TEST_TICK_SIZE"]),os.environ["GROWW_TEST_EXCHANGE_TOKEN"],True)
    entry=float(os.environ["GROWW_TEST_ENTRY_PRICE"]); target=float(os.environ["GROWW_TEST_TARGET_PRICE"]); stop=float(os.environ["GROWW_TEST_STOP_PRICE"])
    assert contract.option_type in {"CE","PE"} and stop < entry < target
    executor=GrowwLongOptionExecutor(adapter,StateStore(tmp_path),Observability(),max_status_checks=int(os.getenv("GROWW_TEST_STATUS_POLLS","24")),poll_interval=float(os.getenv("GROWW_TEST_POLL_SECONDS","1")))
    receipt=executor.execute(contract=contract,quantity=contract.lot_size,entry_limit=entry,protection=ProtectionPlan(entry,target,stop,0))
    assert receipt.state is ProtectionState.ACTIVE_PROTECTED_POSITION and receipt.protection_order_id
    protected=adapter.smart_order(receipt.protection_order_id)
    assert str(protected.get("status","")).upper() == "ACTIVE" and int(protected.get("quantity",0)) == contract.lot_size

# ---------------------- Telegram-owned runtime lifecycle certification ----------------------
from orchestration.bootstrap import RuntimeBundle
from orchestration.supervisor import ControllerPolicy, TelegramRuntimeSupervisor
from telegram.controller import TelegramController


class _ControllerResearch:
    def read(self, stream):
        return [{"stream": stream, "row": 1}]


class _ControllerPlatform:
    def __init__(self, positions=None):
        self._positions = list(positions or [])
        self.research = _ControllerResearch()
        self.halted = False
        self.resumed = False

    def status(self):
        return {"positions": self._positions, "entries_halted": self.halted, "model_authority": {"btc": True}}

    def halt_new_entries(self, _reason):
        self.halted = True

    def resume_new_entries(self, _reason):
        self.halted = False
        self.resumed = True


class _RunningCoordinator:
    def __init__(self):
        self.started = False
        self.cancelled = False

    async def run(self, *, live):
        self.started = True
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            self.cancelled = True
            raise


class _ControllerBootstrap:
    def __init__(self, platform=None):
        self.platform = platform or _ControllerPlatform()
        self.coordinator = _RunningCoordinator()
        self.build_calls = []

    def preflight_status(self):
        return {"promoted_artifacts_ready": False, "live_flags": {"groww": False}}

    def build_runtime(self, *, mode, configuration_module):
        self.build_calls.append((mode, configuration_module))
        return RuntimeBundle(self.platform, self.coordinator, mode)


def _supervisor(platform=None):
    bootstrap = _ControllerBootstrap(platform)
    supervisor = TelegramRuntimeSupervisor(
        bootstrap=bootstrap,
        policy=ControllerPolicy(configuration_module="deployment.session_config", default_mode="shadow", dashboard_enabled=False),
    )
    return supervisor, bootstrap


def test_telegram_supervisor_remains_idle_until_authorised_start_command():
    supervisor, bootstrap = _supervisor()
    status = supervisor.status()
    assert status["controller_online"] is True
    assert status["runtime_state"] == "IDLE"
    assert bootstrap.build_calls == []


def test_telegram_start_and_stop_own_runtime_lifecycle_without_open_positions():
    async def exercise():
        supervisor, bootstrap = _supervisor()
        started = await supervisor.start("paper")
        assert started == {"ok": True, "runtime_state": "RUNNING", "mode": "paper"}
        assert bootstrap.build_calls == [("paper", "deployment.session_config")]
        assert bootstrap.coordinator.started
        stopped = await supervisor.stop()
        assert stopped["ok"] is True and stopped["runtime_state"] == "IDLE"
        assert bootstrap.coordinator.cancelled
    asyncio.run(exercise())


def test_telegram_stop_halts_entries_but_keeps_monitoring_open_protected_positions():
    async def exercise():
        platform = _ControllerPlatform(positions=[{"instrument": "NIFTYCE", "protected": True}])
        supervisor, bootstrap = _supervisor(platform)
        assert (await supervisor.start("paper"))["ok"] is True
        result = await supervisor.stop()
        assert result["ok"] is False and result["runtime_state"] == "RUNNING_HALTED"
        assert platform.halted is True
        assert bootstrap.coordinator.cancelled is False
        platform._positions.clear()
        assert (await supervisor.stop())["ok"] is True
    asyncio.run(exercise())


def test_telegram_halt_and_resume_commands_operate_on_active_authority():
    async def exercise():
        supervisor, _ = _supervisor()
        assert (await supervisor.start("shadow"))["ok"] is True
        assert supervisor.halt()["ok"] is True
        assert supervisor.bundle.platform.halted is True
        assert supervisor.resume()["ok"] is True
        assert supervisor.bundle.platform.resumed is True
        await supervisor.stop()
    asyncio.run(exercise())


class _HTTPResponse:
    def __init__(self):
        self.status_code = 200
    def raise_for_status(self):
        return None


class _TelegramSession:
    def __init__(self):
        self.sent = []
    def post(self, _url, *, json, timeout):
        self.sent.append((json, timeout))
        return _HTTPResponse()


def test_telegram_controller_routes_start_status_halt_resume_and_stop_commands():
    async def exercise():
        supervisor, _ = _supervisor()
        session = _TelegramSession()
        controller = TelegramController(supervisor=supervisor, token="token", chat_id="42", session=session)
        await controller.handle("/start paper")
        await controller.handle("/status")
        await controller.handle("/halt")
        await controller.handle("/resume")
        await controller.handle("/decisions")
        await controller.handle("/stop")
        payloads = [json.loads(payload[0]["text"]) for payload in session.sent]
        assert payloads[0]["runtime_state"] == "RUNNING"
        assert payloads[1]["controller_online"] is True
        assert payloads[2]["ok"] is True and payloads[3]["ok"] is True
        assert payloads[4] == [{"stream": "decisions", "row": 1}]
        assert payloads[5]["runtime_state"] == "IDLE"
    asyncio.run(exercise())


def test_docker_runtime_entrypoint_is_telegram_controller_and_build_gate_is_executable_safe():
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert 'ENTRYPOINT ["python", "-m", "telegram.controller"]' in dockerfile
    assert "/usr/bin/env bash /app/scripts/certify_release.sh" in dockerfile
    assert "chmod 0755 /app/scripts/certify_release.sh" in dockerfile

class _FailingCoordinator:
    async def run(self, *, live):
        await asyncio.sleep(0)
        raise RuntimeError("FEED_RUNTIME_FAILED")


def test_telegram_supervisor_reports_unexpected_runtime_failure_without_killing_controller():
    async def exercise():
        bootstrap = _ControllerBootstrap()
        bootstrap.coordinator = _FailingCoordinator()
        supervisor = TelegramRuntimeSupervisor(
            bootstrap=bootstrap,
            policy=ControllerPolicy(configuration_module="deployment.session_config", dashboard_enabled=False),
        )
        result = await supervisor.start("shadow")
        if result["ok"]:
            await asyncio.sleep(0)
            await asyncio.sleep(0)
        status = supervisor.status()
        assert status["controller_online"] is True
        assert status["runtime_state"] == "FAILED"
        assert "FEED_RUNTIME_FAILED" in status["last_error"]
    asyncio.run(exercise())

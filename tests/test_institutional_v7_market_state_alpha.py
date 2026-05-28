import time
from types import SimpleNamespace

from intelligence.cross_venue_btc import BTCCompositeState
from intelligence.venue_market_state import CrossVenueEvidence, VenueMarketState, VenueMarketStateEngine
from market_data.feed_health import score_feed_health
from market_data.normalizer import InstrumentMapping, build_venue_microstate
from strategy.domain import Direction
from strategy.institutional_strategy import InstitutionalStrategy


def _microstate(venue: str, symbol: str, mid: float, *, quality_ok: bool = True, flow: float = 0.0):
    health = score_feed_health(
        connected=quality_ok, heartbeat_ok=quality_ok, sequence_valid=quality_ok,
        snapshot_ready=quality_ok, exchange_timestamp_available=True, latency_vs_baseline_z=0.0,
    )
    return build_venue_microstate(
        mapping=InstrumentMapping(
            venue=venue, venue_symbol=symbol, canonical_underlying="BTC", product_class="linear_perp",
            quote_currency="USD", contract_multiplier=1.0, settlement_currency="USD", price_tick=0.01,
            qty_step=0.001, execution_enabled=True, notional_model="linear",
        ),
        bids=[(mid - 0.01, 10000.0)], asks=[(mid + 0.01, 10000.0)], feed_health=health,
        receive_ts_ns=time.time_ns(), exchange_ts_ns=time.time_ns() - 1_000_000,
        ofi_usd_1s=flow, ofi_usd_10s=flow * 0.5, tfi_usd_1s=flow * 0.25,
    )


def _candles(start: float, step: float, n: int = 80):
    rows = []
    for i in range(n):
        c = start + i * step
        rows.append({"open": c - step * 0.2, "high": c + abs(step) * 0.4 + 0.01, "low": c - abs(step) * 0.4 - 0.01, "close": c})
    return rows


class _VenueCandles:
    def __init__(self):
        self.by_venue = {
            "delta": _candles(100.0, 0.0),
            "hyperliquid": _candles(100.0, 0.10),
        }
        self.calls = []

    def get_venue_candles(self, venue, timeframe, limit):
        self.calls.append((venue, timeframe))
        return self.by_venue[venue][-limit:]


def _market(venue: str, alpha: float) -> VenueMarketState:
    return VenueMarketState(
        venue=venue, symbol="BTC", ready=True, reason="ready", signed_alpha_bps=alpha,
        confidence=0.85, uncertainty_bps=0.25, regime_label="TREND",
        returns_bps={"1m": alpha, "5m": alpha}, robust_one_minute_vol_bps=1.0,
        volatility_expansion_ratio=1.5, acceptance_bps=alpha, live_impulse_bps={}, diagnostics={},
    )


def test_venue_market_state_is_built_from_each_candidate_venues_own_candles():
    data = _VenueCandles()
    engine = VenueMarketStateEngine("BTC")
    states = {"delta": _microstate("delta", "BTCUSD", 100.0), "hyperliquid": _microstate("hyperliquid", "BTC", 107.9)}
    result = engine.build(data, states)
    assert result["hyperliquid"].ready is True
    assert result["hyperliquid"].signed_alpha_bps > 0
    assert abs(result["delta"].signed_alpha_bps) < result["hyperliquid"].signed_alpha_bps
    assert {call[0] for call in data.calls} == {"delta", "hyperliquid"}


def test_hyperliquid_btc_candidate_is_not_scaled_by_delta_execution_quality(monkeypatch, tmp_path):
    monkeypatch.setattr("strategy.institutional_strategy._cfg", lambda name, default: {
        "RESEARCH_STORE_PATH": str(tmp_path), "INSTITUTIONAL_MIN_EXECUTION_QUALITY": 0.40,
        "INSTITUTIONAL_MIN_SIGNAL_BPS": 0.1, "INSTITUTIONAL_MARKET_STATE_ASSETS": ("BTC",),
        "INSTITUTIONAL_MICROSTRUCTURE_ALPHA_CAP_BPS": {"BTC": 22.0},
    }.get(name, default))
    strategy = InstitutionalStrategy(instrument=None)
    strategy._asset_id = "BTC"
    hyper = _microstate("hyperliquid", "BTC", 75000.0, flow=0.0)
    composite = BTCCompositeState(
        delta_state=hyper, reference_states={"hyperliquid": hyper}, composite_reference_mid=75000.0,
        delta_dislocation_bps=0.0, flow_agreement_score=0.0, cross_venue_dispersion_bps=0.0,
        candidate_leader_venue="hyperliquid", leader_confidence=1.0, delta_execution_quality_score=0.01,
        excluded_reference_venues={},
    )
    direction, edge, reason, breakdown = strategy._direction_and_edge(
        "DESK_A_BTC", 75000.0, 0.95, execution_state=hyper, btc_composite=composite,
        market_state=_market("hyperliquid", 18.0), cross_venue_evidence=None,
    )
    assert direction is Direction.LONG
    assert edge > 0
    assert not reason.startswith("execution_quality_low")
    assert breakdown["venue_local_execution_quality_multiplier"] > 0.4


def test_cross_venue_disagreement_is_uncertainty_not_hard_veto(monkeypatch, tmp_path):
    monkeypatch.setattr("strategy.institutional_strategy._cfg", lambda name, default: {
        "RESEARCH_STORE_PATH": str(tmp_path), "INSTITUTIONAL_MIN_SIGNAL_BPS": 0.1,
        "INSTITUTIONAL_MARKET_STATE_ASSETS": ("BTC",), "INSTITUTIONAL_CROSS_VENUE_UNCERTAINTY_MAX_BPS": 6.0,
        "INSTITUTIONAL_MICROSTRUCTURE_ALPHA_CAP_BPS": {"BTC": 22.0},
    }.get(name, default))
    strategy = InstitutionalStrategy(instrument=None)
    strategy._asset_id = "BTC"
    state = _microstate("hyperliquid", "BTC", 75000.0, flow=0.0)
    evidence = CrossVenueEvidence(
        asset_id="BTC", agreement_score=0.05, dispersion_bps=1.0, leader_venue="hyperliquid",
        leader_confidence=0.7, participating_venues=("delta", "hyperliquid"),
        signed_alpha_by_venue={"delta": -2.0, "hyperliquid": 20.0},
    )
    direction, edge, reason, breakdown = strategy._direction_and_edge(
        "DESK_A_BTC", 75000.0, 0.95, execution_state=state, btc_composite=None,
        market_state=_market("hyperliquid", 20.0), cross_venue_evidence=evidence,
    )
    assert direction is Direction.LONG
    assert edge > 0
    assert reason == "parent_structural_thesis_long_child_timing_validated"
    assert breakdown["signal_architecture"] == "parent_structural_thesis_child_execution_timing_v1"
    assert breakdown["cross_venue_uncertainty_bps"] > 0
    assert breakdown["cross_venue_confidence_multiplier"] > 0


def test_gold_structural_displacement_generates_alpha_without_spoofable_flow(monkeypatch, tmp_path):
    monkeypatch.setattr("strategy.institutional_strategy._cfg", lambda name, default: {
        "RESEARCH_STORE_PATH": str(tmp_path), "INSTITUTIONAL_MIN_SIGNAL_BPS": 0.1,
        "INSTITUTIONAL_MARKET_STATE_ASSETS": ("GOLD_PAXG",),
        "INSTITUTIONAL_MICROSTRUCTURE_ALPHA_CAP_BPS": {"GOLD_PAXG": 18.0},
    }.get(name, default))
    strategy = InstitutionalStrategy(instrument=None)
    strategy._asset_id = "GOLD_PAXG"
    state = _microstate("delta", "PAXGUSD", 4450.0, flow=0.0)
    direction, edge, reason, breakdown = strategy._direction_and_edge(
        "DESK_A_METALS", 4450.0, 0.90, execution_state=state, btc_composite=None,
        market_state=_market("delta", 12.0), cross_venue_evidence=None,
    )
    assert direction is Direction.LONG
    assert reason == "parent_structural_thesis_long_child_timing_validated"
    assert edge > 0
    assert breakdown["venue_local_market_state_alpha_bps"] == 12.0
    assert breakdown["signal_architecture"] == "parent_structural_thesis_child_execution_timing_v1"
    assert breakdown["microstructure_cannot_originate_or_flip_thesis"] is True

from execution.venue_selection import select_execution_venue


def test_route_values_each_venue_with_its_own_validated_edge_only(monkeypatch):
    monkeypatch.setattr("execution.venue_selection._cfg", lambda name, default: {
        "VENUE_ROUND_TRIP_FEE_BPS": {"delta": 3.0, "hyperliquid": 7.0},
        "VENUE_SLIPPAGE_IMPACT_MULTIPLIER": 0.0,
        "VENUE_SELECTION_MAX_COST_BPS": 100.0,
    }.get(name, default))
    states = {"delta": _microstate("delta", "BTCUSD", 75000.0), "hyperliquid": _microstate("hyperliquid", "BTC", 75000.0)}
    selection = select_execution_venue(
        states=states, direction=Direction.LONG, asset_id="BTC", current_venue="delta",
        routeable_venues={"delta", "hyperliquid"}, notional_usd=100.0,
        available_cash_by_venue={"delta": 100.0, "hyperliquid": 100.0}, required_margin_usd=1.0,
        protection_capable_venues={"delta", "hyperliquid"}, gross_edge_bps=20.0,
        gross_edge_by_venue={"delta": 4.0, "hyperliquid": 20.0},
        notional_by_venue={"delta": 100.0, "hyperliquid": 100.0}, required_margin_by_venue={"delta": 1.0, "hyperliquid": 1.0},
    )
    assert selection.estimates["delta"].gross_edge_bps == 4.0
    assert selection.estimates["hyperliquid"].gross_edge_bps == 20.0
    assert selection.selected_venue == "hyperliquid"


def test_selected_venue_local_volatility_is_used_for_protection_geometry(monkeypatch, tmp_path):
    import strategy.dynamic_protection as dp
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_REQUIRE_SIGNAL_DECAY_READY", False, raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_VENUES", (), raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_VENUES", (), raising=False)
    monkeypatch.setattr("strategy.institutional_strategy._cfg", lambda name, default: str(tmp_path) if name == "RESEARCH_STORE_PATH" else default)
    strategy = InstitutionalStrategy(instrument=None)
    strategy._asset_id = "GOLD_HL"
    market = VenueMarketState(
        venue="hyperliquid", symbol="xyz:GOLD", ready=True, reason="ready", signed_alpha_bps=20.0,
        confidence=0.8, uncertainty_bps=0.0, regime_label="TREND", returns_bps={},
        robust_one_minute_vol_bps=12.0, volatility_expansion_ratio=1.2, acceptance_bps=0.0,
        live_impulse_bps={}, diagnostics={},
    )
    plan = strategy._protection_plan(
        "DESK_A_METALS", Direction.LONG, 4400.0, 0.9, data_manager=SimpleNamespace(),
        gross_edge_bps=30.0, costs_bps=3.0, venue="hyperliquid", instrument="xyz:GOLD",
        execution_state=_microstate("hyperliquid", "xyz:GOLD", 4400.0), venue_market_state=market,
    )
    assert plan is not None and plan.protection_feasible
    geom = plan.diagnostics["market_geometry"]
    assert geom["volatility_source"] == "venue_local_confirmed_candles:hyperliquid"
    assert geom["venue_local_robust_vol_bps"] == 12.0

from aggregator.market_aggregator import MarketAggregator


class _BootstrapDM:
    def __init__(self, name, start_ok, ready, candles=None, research=None):
        self.name = name
        self.start_ok = start_ok
        self.is_ready = ready
        self.started = False
        self.strategy = None
        self.candles = candles or []
        self.research = research or {}
    def start(self):
        self.started = True
        return self.start_ok
    def stop(self):
        pass
    def register_strategy(self, strategy):
        self.strategy = strategy
    def get_candles(self, timeframe, limit):
        return self.candles[-limit:]
    def get_microstructure_research_state(self):
        return self.research


class DeltaBrokenDataManager(_BootstrapDM):
    pass


class HyperliquidReadyDataManager(_BootstrapDM):
    venue = "hyperliquid"


def test_failed_first_listed_bootstrap_venue_cannot_disable_ready_alternate_venue():
    delta = DeltaBrokenDataManager("delta", False, False)
    hyper = HyperliquidReadyDataManager("hyperliquid", True, True)
    agg = MarketAggregator(primary_dm=delta, secondary_dm=None, reference_dms=[hyper])
    strategy_marker = object()
    agg.register_strategy(strategy_marker)
    assert agg.start() is True
    assert hyper.started is True
    assert agg.wait_until_ready(0.1) is True
    assert agg._primary is hyper
    assert hyper.strategy is strategy_marker


def test_selected_venue_research_stream_is_exposed_without_primary_contamination():
    delta = DeltaBrokenDataManager("delta", True, True, research={"book_events": [{"venue": "delta"}]})
    hyper = HyperliquidReadyDataManager("hyperliquid", True, True, research={"book_events": [{"venue": "hyperliquid"}]})
    agg = MarketAggregator(primary_dm=delta, secondary_dm=None, reference_dms=[hyper])
    assert agg.get_venue_microstructure_research_state("hyperliquid")["book_events"][0]["venue"] == "hyperliquid"


def test_selected_broker_tick_fallback_never_inherits_delta_tick(monkeypatch):
    from types import SimpleNamespace
    from execution.order_manager import OrderManager

    instrument = SimpleNamespace(by_exchange={
        "delta": SimpleNamespace(tick_size=0.5),
        "hyperliquid": SimpleNamespace(symbol="BTC", display_symbol="BTC", tick_size=0.01, lot_step=0.00001, min_qty=0.0, max_qty=0.0),
    })
    manager = OrderManager(SimpleNamespace(), exchange_name="hyperliquid", instrument=instrument)
    manager._adapter.tick_size = 0.0
    assert manager._active_tick_size() == 0.01

import time
from types import SimpleNamespace

import config
from intelligence.composite_asset_state import CompositeAssetDecision, CompositeIntelligenceBus
from intelligence.venue_market_state import VenueMarketState
from market_data.feed_health import score_feed_health
from market_data.normalizer import InstrumentMapping, build_venue_microstate
from strategy.domain import Direction
from strategy.institutional_strategy import InstitutionalStrategy


def _micro(venue: str, symbol: str, mid: float, flow: float = 0.0):
    health = score_feed_health(
        connected=True, heartbeat_ok=True, sequence_valid=True, snapshot_ready=True,
        exchange_timestamp_available=True, latency_vs_baseline_z=0.0,
    )
    return build_venue_microstate(
        mapping=InstrumentMapping(
            venue=venue, venue_symbol=symbol, canonical_underlying="TEST", product_class="linear_perp",
            quote_currency="USDT" if venue == "coinswitch" else "USD", contract_multiplier=1.0,
            settlement_currency="USDT" if venue == "coinswitch" else "USD", price_tick=0.01,
            qty_step=0.001, execution_enabled=True, notional_model="linear",
        ),
        bids=[(mid - 0.01, 1000.0)], asks=[(mid + 0.01, 1000.0)], feed_health=health,
        receive_ts_ns=time.time_ns(), exchange_ts_ns=time.time_ns() - 1_000_000,
        ofi_usd_1s=flow, ofi_usd_10s=flow * 0.5, tfi_usd_1s=flow * 0.25,
    )


def _market(venue: str, symbol: str, alpha: float) -> VenueMarketState:
    return VenueMarketState(
        venue=venue, symbol=symbol, ready=True, reason="ready", signed_alpha_bps=alpha,
        confidence=0.90, uncertainty_bps=0.30, regime_label="TREND",
        returns_bps={"1m": alpha, "5m": alpha, "15m": alpha}, robust_one_minute_vol_bps=1.5,
        volatility_expansion_ratio=1.5, acceptance_bps=alpha, live_impulse_bps={}, diagnostics={},
    )


def test_btc_collective_alpha_uses_all_execution_equivalent_normalised_feeds():
    bus = CompositeIntelligenceBus()
    micros = {
        "delta": _micro("delta", "BTCUSD", 75000.0, flow=200000.0),
        "coinswitch": _micro("coinswitch", "BTCUSDT", 75002.0, flow=180000.0),
        "hyperliquid": _micro("hyperliquid", "BTC", 74998.0, flow=220000.0),
    }
    markets = {
        "delta": _market("delta", "BTCUSD", 8.0),
        "coinswitch": _market("coinswitch", "BTCUSDT", 10.0),
        "hyperliquid": _market("hyperliquid", "BTC", 12.0),
    }
    decision = bus.build_decision(asset_id="BTC", market_states=markets, microstates=micros)
    assert decision.ready is True
    assert decision.equivalence_group == "BTC_LINEAR_PERP"
    assert len(decision.execution_sources) == 3
    assert decision.transferable_structural_alpha_bps > 0
    assert decision.transferable_microstructure_alpha_bps > 0
    assert decision.transferable_total_alpha_bps > decision.transferable_structural_alpha_bps
    assert decision.diagnostics["raw_price_routing"] is False
    assert decision.diagnostics["order_books_merged"] is False


def test_gold_related_products_share_factor_context_not_execution_alpha():
    bus = CompositeIntelligenceBus()
    bus.build_decision(
        asset_id="GOLD_HL",
        market_states={"hyperliquid": _market("hyperliquid", "xyz:GOLD", 80.0)},
        microstates={"hyperliquid": _micro("hyperliquid", "xyz:GOLD", 4500.0, flow=300000.0)},
    )
    paxg = bus.build_decision(
        asset_id="GOLD_PAXG",
        market_states={"delta": _market("delta", "PAXGUSD", 4.0), "coinswitch": _market("coinswitch", "PAXGUSDT", 6.0)},
        microstates={"delta": _micro("delta", "PAXGUSD", 4400.0), "coinswitch": _micro("coinswitch", "PAXGUSDT", 4401.0)},
    )
    assert paxg.basis_translation_enabled is False
    assert paxg.equivalence_group == "PAXG_TOKEN_PERP"
    assert all("GOLD_HL" not in source for source in paxg.execution_sources)
    assert any("GOLD_HL" in source for source in paxg.factor_sources)
    assert paxg.transferable_structural_alpha_bps < 10.0  # HL alpha did not transfer into PAXG route
    assert paxg.factor_context_alpha_bps > paxg.transferable_structural_alpha_bps
    assert paxg.diagnostics["factor_translation_policy"] == "confidence_only_no_alpha_transfer"


def test_silver_factor_evidence_never_makes_slvon_interchangeable_with_xag_or_hl():
    bus = CompositeIntelligenceBus()
    bus.build_decision(
        asset_id="SILVER_XAG", market_states={"coinswitch": _market("coinswitch", "XAGUSDT", 70.0)},
        microstates={"coinswitch": _micro("coinswitch", "XAGUSDT", 74.0, flow=200000.0)},
    )
    bus.build_decision(
        asset_id="SILVER_HL", market_states={"hyperliquid": _market("hyperliquid", "xyz:SILVER", 60.0)},
        microstates={"hyperliquid": _micro("hyperliquid", "xyz:SILVER", 74.0, flow=200000.0)},
    )
    slvon = bus.build_decision(
        asset_id="SILVER_SLVON", market_states={"delta": _market("delta", "SLVONUSD", 2.0)},
        microstates={"delta": _micro("delta", "SLVONUSD", 67.0)},
    )
    assert slvon.execution_sources == ("SILVER_SLVON:delta:SLVONUSD",)
    assert slvon.transferable_structural_alpha_bps == 2.0
    assert len(slvon.factor_sources) == 3
    assert slvon.basis_translation_enabled is False


def test_strategy_direction_consumes_collective_execution_alpha_without_local_double_count(monkeypatch, tmp_path):
    monkeypatch.setattr("strategy.institutional_strategy._cfg", lambda name, default: {
        "RESEARCH_STORE_PATH": str(tmp_path), "INSTITUTIONAL_MIN_SIGNAL_BPS": 0.1,
        "INSTITUTIONAL_MICROSTRUCTURE_ALPHA_CAP_BPS": {"BTC": 22.0},
    }.get(name, default))
    strategy = InstitutionalStrategy(instrument=None)
    strategy._asset_id = "BTC"
    local = _micro("delta", "BTCUSD", 75000.0, flow=0.0)
    collective = CompositeAssetDecision(
        asset_id="BTC", factor_id="BTC", equivalence_group="BTC_LINEAR_PERP", transfer_mode="TRANSFERABLE_EXECUTION_ALPHA",
        ready=True, reason="normalised_composite_ready", transferable_structural_alpha_bps=8.0,
        transferable_microstructure_alpha_bps=6.0, transferable_total_alpha_bps=14.0,
        factor_context_alpha_bps=14.0, transferable_confidence=0.9, factor_agreement_score=0.9,
        factor_uncertainty_bps=0.2, leader_source="hyperliquid:BTC", execution_sources=("BTC:delta:BTCUSD", "BTC:hyperliquid:BTC"),
        factor_sources=("BTC:delta:BTCUSD", "BTC:hyperliquid:BTC"), basis_translation_enabled=False,
        diagnostics={"execution_uncertainty_bps": 0.2},
    )
    direction, edge, _, breakdown = strategy._direction_and_edge(
        "DESK_A_BTC", 75000.0, 0.95, execution_state=local, btc_composite=None,
        market_state=_market("delta", "BTCUSD", 0.0), cross_venue_evidence=None, composite_decision=collective,
    )
    assert direction is Direction.LONG
    assert edge > 0
    assert breakdown["weighted_signal_bps"] == 14.0
    assert breakdown["collective_transferable_total_alpha_bps"] == 14.0
    assert breakdown["local_execution_timing_alpha_bps"] == 0.0


def test_config_requires_product_aware_factor_routing_policy():
    assert config.INSTITUTIONAL_EXECUTION_EQUIVALENCE_GROUP_BY_ASSET["BTC"] == "BTC_LINEAR_PERP"
    assert config.INSTITUTIONAL_EXECUTION_EQUIVALENCE_GROUP_BY_ASSET["SILVER_SLVON"] != config.INSTITUTIONAL_EXECUTION_EQUIVALENCE_GROUP_BY_ASSET["SILVER_XAG"]
    assert config.INSTITUTIONAL_FACTOR_BY_ASSET["SILVER_SLVON"] == config.INSTITUTIONAL_FACTOR_BY_ASSET["SILVER_HL"] == "SILVER"
    assert config.INSTITUTIONAL_VALIDATED_FACTOR_TRANSLATION_MODELS == {}

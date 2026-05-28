import time

import pytest

from intelligence.venue_market_state import VenueMarketState
from market_data.feed_health import score_feed_health
from market_data.normalizer import InstrumentMapping, build_venue_microstate
from strategy.domain import DeskId, Direction
from strategy.institutional_strategy import InstitutionalStrategy, PositionPhase, PositionState


PARENT_ASSETS = ("BTC", "GOLD_PAXG", "GOLD_HL", "SILVER_SLVON", "SILVER_XAG", "SILVER_HL", "OIL")
ASSETS = {
    "GOLD_PAXG": ("delta", "PAXGUSD", DeskId.METALS.value),
    "GOLD_HL": ("hyperliquid", "xyz:GOLD", DeskId.METALS.value),
    "SILVER_SLVON": ("delta", "SLVONUSD", DeskId.METALS.value),
    "SILVER_XAG": ("coinswitch", "XAGUSDT", DeskId.METALS.value),
    "SILVER_HL": ("hyperliquid", "xyz:SILVER", DeskId.METALS.value),
    "OIL": ("hyperliquid", "xyz:CL", DeskId.COMMODITIES.value),
}


def _state(asset: str, venue: str, symbol: str, flow: float):
    health = score_feed_health(
        connected=True, heartbeat_ok=True, sequence_valid=True, snapshot_ready=True,
        exchange_timestamp_available=True, latency_vs_baseline_z=0.0,
    )
    return build_venue_microstate(
        mapping=InstrumentMapping(
            venue=venue, venue_symbol=symbol, canonical_underlying=asset,
            product_class="linear_perp", quote_currency="USD", contract_multiplier=1.0,
            settlement_currency="USD", price_tick=0.01, qty_step=0.001,
            execution_enabled=True, notional_model="linear",
        ),
        bids=[(100.0, 2_000_000.0)], asks=[(100.02, 2_000_000.0)],
        feed_health=health, receive_ts_ns=time.time_ns(), exchange_ts_ns=time.time_ns() - 1_000_000,
        ofi_usd_1s=flow, ofi_usd_10s=flow, tfi_usd_1s=flow * 0.25, tfi_usd_10s=flow * 0.25,
    )


def _parent(venue: str, symbol: str, alpha: float, state_id: str = "closed-parent"):
    return VenueMarketState(
        venue=venue, symbol=symbol, ready=True, reason="venue_local_structural_state_ready",
        signed_alpha_bps=alpha, confidence=0.85, uncertainty_bps=0.25,
        regime_label="TREND", returns_bps={"1m": alpha, "5m": alpha},
        robust_one_minute_vol_bps=1.0, volatility_expansion_ratio=1.1,
        acceptance_bps=alpha, live_impulse_bps={}, diagnostics={"parent_state_id": state_id},
    )


def _strategy(monkeypatch, tmp_path, asset: str):
    monkeypatch.setattr(
        "strategy.institutional_strategy._cfg",
        lambda name, default: {
            "RESEARCH_STORE_PATH": str(tmp_path),
            "INSTITUTIONAL_PARENT_THESIS_EXECUTION_MODEL_ENABLED": True,
            "INSTITUTIONAL_PARENT_THESIS_ASSETS": PARENT_ASSETS,
            "INSTITUTIONAL_PARENT_TIMING_CONTRIBUTION_CAP_FRACTION": 0.35,
            "INSTITUTIONAL_MIN_SIGNAL_BPS": 0.1,
            "INSTITUTIONAL_MICROSTRUCTURE_ALPHA_CAP_BPS": {asset: 20.0},
        }.get(name, default),
    )
    strategy = InstitutionalStrategy(instrument=None)
    strategy._asset_id = asset
    return strategy


@pytest.mark.parametrize("asset", tuple(ASSETS))
def test_flow_only_impulse_cannot_originate_any_commodity_or_metal_trade(monkeypatch, tmp_path, asset):
    venue, symbol, desk = ASSETS[asset]
    strategy = _strategy(monkeypatch, tmp_path, asset)
    direction, edge, reason, detail = strategy._direction_and_edge(
        desk, 100.01, 0.95, execution_state=_state(asset, venue, symbol, 9_000_000.0),
        btc_composite=None, market_state=_parent(venue, symbol, 0.0), cross_venue_evidence=None,
    )
    assert direction is Direction.NO_TRADE
    assert edge == 0.0
    assert reason == "parent_structural_thesis_not_established"
    assert detail["child_timing_alpha_bps"] > 0.0
    assert detail["microstructure_cannot_originate_or_flip_thesis"] is True


@pytest.mark.parametrize("asset", tuple(ASSETS))
def test_opposing_flow_cannot_flip_confirmed_commodity_or_metal_parent(monkeypatch, tmp_path, asset):
    venue, symbol, desk = ASSETS[asset]
    strategy = _strategy(monkeypatch, tmp_path, asset)
    direction, edge, reason, detail = strategy._direction_and_edge(
        desk, 100.01, 0.95, execution_state=_state(asset, venue, symbol, -9_000_000.0),
        btc_composite=None, market_state=_parent(venue, symbol, 8.0), cross_venue_evidence=None,
    )
    assert direction is Direction.LONG
    assert edge > 0.0
    assert reason == "parent_structural_thesis_long_child_timing_validated"
    assert detail["child_timing_alpha_bps"] < 0.0
    assert 0.0 < detail["weighted_signal_bps"] < detail["parent_structural_alpha_bps"]


def test_oil_is_assigned_to_commodities_desk_not_btc(monkeypatch, tmp_path):
    strategy = _strategy(monkeypatch, tmp_path, "OIL")
    assert strategy._desk_id("hyperliquid", "xyz:CL") == DeskId.COMMODITIES.value


class _NoBrokerIoOnTick:
    def emergency_flatten(self, *args, **kwargs):
        raise AssertionError("structural supervision must not execute on the quote thread")


@pytest.mark.parametrize("asset", tuple(ASSETS))
def test_dynamic_exit_of_each_directional_desk_requires_distinct_closed_parent_invalidations(monkeypatch, tmp_path, asset):
    venue, symbol, _desk = ASSETS[asset]
    strategy = _strategy(monkeypatch, tmp_path, asset)
    original_cfg = __import__("strategy.institutional_strategy", fromlist=["_cfg"])._cfg
    monkeypatch.setattr(
        "strategy.institutional_strategy._cfg",
        lambda name, default: {
            "DYNAMIC_EXIT_AUTOMATED_EARLY_LIQUIDATION_ENABLED": True,
            "DYNAMIC_EXIT_PARENT_STRUCTURE_ONLY": True,
            "DYNAMIC_EXIT_PARENT_STRUCTURE_ASSETS": PARENT_ASSETS,
            "DYNAMIC_EXIT_MIN_DISTINCT_PARENT_OBSERVATIONS": 2,
            "RESEARCH_STORE_PATH": str(tmp_path),
        }.get(name, original_cfg(name, default)),
    )
    strategy._pos = PositionState(
        phase=PositionPhase.ACTIVE, side="long", quantity=0.01, entry_price=100.0,
        sl_price=98.0, tp_price=104.0, entry_time=time.time() - 60.0,
        exchange=venue, execution_symbol=symbol, asset_id=asset, protection_confirmed=True,
        quant_components={"dynamic_protection_plan": {"signal_decay": {"optimal_hold_sec": 1.0}}},
    )
    states = iter(["closed-reversal-1", "closed-reversal-2"])
    strategy._dynamic_exit_live_state = lambda data: {
        "ready": True, "parent_state_id": next(states), "parent_structure_opposed": True,
        "parent_opposing_net_edge_bps": 4.0,
    }
    strategy._dynamic_exit_supervision(object(), _NoBrokerIoOnTick())
    assert strategy._pos.dynamic_exit_requested is False
    strategy._dynamic_exit_supervision(object(), _NoBrokerIoOnTick())
    assert strategy._pos.dynamic_exit_requested is True
    assert strategy._pos.dynamic_exit_reasons == ("confirmed_parent_structural_invalidation",)

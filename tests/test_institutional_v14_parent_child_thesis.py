import time

from intelligence.venue_market_state import VenueMarketState, VenueMarketStateEngine
from market_data.feed_health import score_feed_health
from market_data.normalizer import InstrumentMapping, build_venue_microstate
from strategy.domain import Direction
from strategy.institutional_strategy import InstitutionalStrategy


def _microstate(mid: float, flow: float):
    health = score_feed_health(
        connected=True,
        heartbeat_ok=True,
        sequence_valid=True,
        snapshot_ready=True,
        exchange_timestamp_available=True,
        latency_vs_baseline_z=0.0,
    )
    return build_venue_microstate(
        mapping=InstrumentMapping(
            venue="hyperliquid",
            venue_symbol="BTC",
            canonical_underlying="BTC",
            product_class="linear_perp",
            quote_currency="USD",
            contract_multiplier=1.0,
            settlement_currency="USD",
            price_tick=0.01,
            qty_step=0.00001,
            execution_enabled=True,
            notional_model="linear",
        ),
        bids=[(mid - 0.50, 2_000_000.0)],
        asks=[(mid + 0.50, 2_000_000.0)],
        feed_health=health,
        receive_ts_ns=time.time_ns(),
        exchange_ts_ns=time.time_ns() - 1_000_000,
        ofi_usd_1s=flow,
        ofi_usd_10s=flow,
        tfi_usd_1s=flow * 0.30,
        tfi_usd_10s=flow * 0.30,
    )


def _parent(alpha: float, uncertainty: float = 0.25):
    return VenueMarketState(
        venue="hyperliquid",
        symbol="BTC",
        ready=True,
        reason="venue_local_structural_state_ready",
        signed_alpha_bps=alpha,
        confidence=0.85,
        uncertainty_bps=uncertainty,
        regime_label="TREND",
        returns_bps={"1m": alpha, "5m": alpha, "15m": alpha},
        robust_one_minute_vol_bps=1.0,
        volatility_expansion_ratio=1.2,
        acceptance_bps=0.0,
        live_impulse_bps={},
        diagnostics={"parent_state_id": "closed-parent-A"},
    )


def _strategy(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "strategy.institutional_strategy._cfg",
        lambda name, default: {
            "RESEARCH_STORE_PATH": str(tmp_path),
            "INSTITUTIONAL_PARENT_THESIS_EXECUTION_MODEL_ENABLED": True,
            "INSTITUTIONAL_PARENT_THESIS_ASSETS": ("BTC",),
            "INSTITUTIONAL_PARENT_TIMING_CONTRIBUTION_CAP_FRACTION": 0.35,
            "INSTITUTIONAL_MIN_SIGNAL_BPS": 0.1,
            "INSTITUTIONAL_MICROSTRUCTURE_ALPHA_CAP_BPS": {"BTC": 22.0},
        }.get(name, default),
    )
    strategy = InstitutionalStrategy(instrument=None)
    strategy._asset_id = "BTC"
    return strategy


def test_btc_flow_impulse_cannot_originate_entry_without_parent_structure(monkeypatch, tmp_path):
    strategy = _strategy(monkeypatch, tmp_path)
    state = _microstate(73_000.0, flow=8_000_000.0)
    direction, edge, reason, breakdown = strategy._direction_and_edge(
        "DESK_A_BTC",
        73_000.0,
        0.99,
        execution_state=state,
        btc_composite=None,
        market_state=_parent(0.0),
        cross_venue_evidence=None,
    )
    assert direction is Direction.NO_TRADE
    assert edge == 0.0
    assert reason == "parent_structural_thesis_not_established"
    assert breakdown["robust_microstructure_alpha_bps"] > 0.0
    assert breakdown["weighted_signal_bps"] == 0.0
    assert breakdown["microstructure_cannot_originate_or_flip_thesis"] is True


def test_btc_flow_reversal_cannot_flip_confirmed_parent_direction(monkeypatch, tmp_path):
    strategy = _strategy(monkeypatch, tmp_path)
    state = _microstate(73_000.0, flow=-8_000_000.0)
    direction, edge, reason, breakdown = strategy._direction_and_edge(
        "DESK_A_BTC",
        73_000.0,
        0.99,
        execution_state=state,
        btc_composite=None,
        market_state=_parent(8.0),
        cross_venue_evidence=None,
    )
    assert direction is Direction.LONG
    assert edge > 0.0
    assert reason == "parent_structural_thesis_long_child_timing_validated"
    assert breakdown["child_timing_alpha_bps"] < 0.0
    assert -2.8 <= breakdown["child_timing_contribution_bps"] < 0.0
    assert breakdown["weighted_signal_bps"] > 0.0
    assert breakdown["weighted_signal_bps"] < breakdown["parent_structural_alpha_bps"]
    assert breakdown["microstructure_cannot_originate_or_flip_thesis"] is True


class _FlatClosedCandleSource:
    def get_venue_candles(self, venue, timeframe, limit):
        # The final row is the active candle excluded by VenueMarketStateEngine.
        return [
            {"open": 100.0, "high": 100.1, "low": 99.9, "close": 100.0}
            for _ in range(limit)
        ]


def test_live_mid_breakout_is_not_structural_acceptance_until_closed_bar_confirms():
    engine = VenueMarketStateEngine("BTC")
    engine.build(_FlatClosedCandleSource(), {"hyperliquid": _microstate(100.0, flow=0.0)})
    engine._live_marks["hyperliquid"][0] = (time.time() - 61.0, 100.0)
    live_impulse = _microstate(110.0, flow=8_000_000.0)
    result = engine.build(_FlatClosedCandleSource(), {"hyperliquid": live_impulse})["hyperliquid"]
    assert result.ready is True
    assert result.acceptance_bps == 0.0
    assert result.signed_alpha_bps == 0.0
    assert result.diagnostics["acceptance_source"] == "latest_closed_1m_close"
    assert result.diagnostics["live_impulse_is_timing_only"] is True
    assert result.diagnostics["live_blend_bps_diagnostic"] > 0.0

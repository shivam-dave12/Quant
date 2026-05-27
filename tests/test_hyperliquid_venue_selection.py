import time

from core.instruments import AssetClass, ExchangeName
from execution.instrument_registry import InstrumentRegistry
from execution.venue_selection import select_execution_venue
from market_data.feed_health import score_feed_health
from market_data.normalizer import InstrumentMapping, build_venue_microstate


def _state(venue, symbol, mid, qty, *, execution_enabled=True):
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
            venue=venue,
            venue_symbol=symbol,
            canonical_underlying="SILVER",
            product_class="linear_perp",
            quote_currency="USD",
            contract_multiplier=1.0,
            settlement_currency="USDC",
            price_tick=0.01,
            qty_step=0.01,
            execution_enabled=execution_enabled,
            notional_model="linear",
        ),
        bids=[(mid - 0.01, qty), (mid - 0.03, qty)],
        asks=[(mid + 0.01, qty), (mid + 0.03, qty)],
        feed_health=health,
        receive_ts_ns=time.time_ns(),
        exchange_ts_ns=time.time_ns() - 1_000_000,
    )


def test_silver_route_prefers_hyperliquid_when_delta_depth_is_thin(monkeypatch):
    def cfg(name, default):
        values = {
            "VENUE_FEE_BPS": {"delta": 1.5, "hyperliquid": 4.5},
            "SILVER_HYPERLIQUID_PREFERENCE_BPS": 0.0,
            "SILVER_DELTA_ILLIQUIDITY_PENALTY_BPS": 15.0,
            "SILVER_DELTA_MIN_NEAR_DEPTH_USD": 50000.0,
            "VENUE_SELECTION_MIN_IMPROVEMENT_BPS": 0.0,
        }
        return values.get(name, default)

    monkeypatch.setattr("execution.venue_selection._cfg", cfg)
    states = {
        "delta": _state("delta", "SLVONUSD", 30.00, 20.0),
        "hyperliquid": _state("hyperliquid", "xyz:SILVER", 30.00, 5000.0),
    }
    selection = select_execution_venue(
        states=states,
        direction="LONG",
        asset_id="SILVER",
        current_venue="delta",
        routeable_venues={"delta", "hyperliquid"},
        notional_usd=10000.0,
    )
    assert selection.selected_venue == "hyperliquid"
    assert selection.estimates["delta"].liquidity_penalty_bps > 0
    assert selection.estimates["hyperliquid"].preference_adjustment_bps == 0


class _FakeHyperliquidAPI:
    def meta_by_dex(self):
        return {
            "": {"universe": [{"name": "BTC", "szDecimals": 5, "maxLeverage": 40}]},
            "xyz": {"universe": [{"name": "xyz:SILVER", "szDecimals": 2, "maxLeverage": 25}]},
            "km": {"universe": [{"name": "km:SILVER", "szDecimals": 3, "maxLeverage": 20}]},
        }


def test_hyperliquid_registry_discovers_builder_silver_first():
    registry = InstrumentRegistry(execution_preference="hyperliquid")
    report = registry.discover(
        hyperliquid_api=_FakeHyperliquidAPI(),
        include_exchanges="hyperliquid",
        requested=[
            {
                "asset_id": "SILVER",
                "display_name": "Silver token derivatives",
                "asset_class": "commodity",
                "aliases": ["SILVER", "XAG"],
                "priority": 1,
            }
        ],
        require_primary=False,
        max_active=1,
    )
    assert len(report.matched) == 1
    inst = report.matched[0]
    assert inst.asset_class is AssetClass.COMMODITY
    assert inst.primary_exchange is ExchangeName.HYPERLIQUID
    assert inst.by_exchange[ExchangeName.HYPERLIQUID].symbol == "xyz:SILVER"
    assert inst.by_exchange[ExchangeName.HYPERLIQUID].lot_step == 0.01
    assert inst.by_exchange[ExchangeName.HYPERLIQUID].max_leverage == 25



def test_unfunded_lower_cost_venue_cannot_displace_funded_route(monkeypatch):
    def cfg(name, default):
        values = {
            "VENUE_ROUND_TRIP_FEE_BPS": {"delta": 3.0, "hyperliquid": 0.1},
            "VENUE_SELECTION_MIN_IMPROVEMENT_BPS": 0.0,
            "VENUE_SLIPPAGE_IMPACT_MULTIPLIER": 0.0,
        }
        return values.get(name, default)

    monkeypatch.setattr("execution.venue_selection._cfg", cfg)
    states = {
        "delta": _state("delta", "BTCUSD", 75800.0, 1000.0),
        "hyperliquid": _state("hyperliquid", "BTC", 75800.0, 1000.0),
    }
    selection = select_execution_venue(
        states=states, direction="LONG", asset_id="BTC", current_venue="delta",
        routeable_venues={"delta", "hyperliquid"}, notional_usd=100.0,
        available_cash_by_venue={"delta": 180.0, "hyperliquid": 0.0},
        required_margin_usd=5.0, protection_capable_venues={"delta", "hyperliquid"},
    )
    assert selection.selected_venue == "delta"
    assert selection.estimates["hyperliquid"].capital_feasible is False
    assert "insufficient_venue_collateral" in selection.estimates["hyperliquid"].reason


def test_live_microstate_fee_metadata_overrides_static_fallback(monkeypatch):
    monkeypatch.setattr("execution.venue_selection._cfg", lambda name, default: {
        "VENUE_ROUND_TRIP_FEE_BPS": {"coinswitch": 99.0},
        "VENUE_SLIPPAGE_IMPACT_MULTIPLIER": 0.0,
        "VENUE_SELECTION_MIN_IMPROVEMENT_BPS": 0.0,
    }.get(name, default))
    state = _state("coinswitch", "BTCUSDT", 75800.0, 1000.0)
    state.metadata["round_trip_fee_bps"] = 13.0
    selection = select_execution_venue(
        states={"coinswitch": state}, direction="LONG", asset_id="BTC",
        current_venue="coinswitch", routeable_venues={"coinswitch"}, notional_usd=100.0,
        available_cash_by_venue={"coinswitch": 100.0}, required_margin_usd=5.0,
        protection_capable_venues={"coinswitch"},
    )
    assert selection.estimates["coinswitch"].fee_bps == 13.0


def test_post_fill_protection_venue_is_charged_activation_risk_reserve(monkeypatch):
    monkeypatch.setattr("execution.venue_selection._cfg", lambda name, default: {
        "VENUE_ROUND_TRIP_FEE_BPS": {"coinswitch": 13.0},
        "VENUE_NON_ATOMIC_PROTECTION_RISK_RESERVE_BPS": {"coinswitch": 5.0},
        "VENUE_SLIPPAGE_IMPACT_MULTIPLIER": 0.0,
    }.get(name, default))
    state = _state("coinswitch", "BTCUSDT", 75800.0, 1000.0)
    selection = select_execution_venue(
        states={"coinswitch": state}, direction="LONG", asset_id="BTC",
        current_venue="coinswitch", routeable_venues={"coinswitch"}, notional_usd=100.0,
        available_cash_by_venue={"coinswitch": 100.0}, required_margin_usd=5.0,
        protection_capable_venues={"coinswitch"},
    )
    assert selection.estimates["coinswitch"].protection_activation_penalty_bps == 5.0


def test_each_venue_is_scored_at_notional_from_its_own_available_cash(monkeypatch):
    monkeypatch.setattr("execution.venue_selection._cfg", lambda name, default: {
        "VENUE_ROUND_TRIP_FEE_BPS": {"delta": 1.0, "hyperliquid": 1.0},
        "VENUE_SLIPPAGE_IMPACT_MULTIPLIER": 10.0,
        "VENUE_SELECTION_MIN_IMPROVEMENT_BPS": 0.0,
    }.get(name, default))
    states = {
        "delta": _state("delta", "BTCUSD", 75800.0, 1000.0),
        "hyperliquid": _state("hyperliquid", "BTC", 75800.0, 1000.0),
    }
    selection = select_execution_venue(
        states=states, direction="LONG", asset_id="BTC", current_venue="delta",
        routeable_venues={"delta", "hyperliquid"}, notional_usd=0.0,
        available_cash_by_venue={"delta": 180.0, "hyperliquid": 25.0},
        required_margin_usd=0.0,
        notional_by_venue={"delta": 45.0, "hyperliquid": 6.25},
        required_margin_by_venue={"delta": 3.0, "hyperliquid": 1.0},
        protection_capable_venues={"delta", "hyperliquid"},
    )
    assert selection.estimates["delta"].proposed_notional_usd == 45.0
    assert selection.estimates["hyperliquid"].proposed_notional_usd == 6.25
    assert selection.estimates["hyperliquid"].available_cash_usd == 25.0
    assert selection.estimates["delta"].available_cash_usd == 180.0


def test_broker_local_sizing_routes_by_expected_net_profit_not_cheapest_tiny_order(monkeypatch):
    monkeypatch.setattr("execution.venue_selection._cfg", lambda name, default: {
        "VENUE_ROUND_TRIP_FEE_BPS": {"delta": 3.0, "hyperliquid": 0.1},
        "VENUE_SLIPPAGE_IMPACT_MULTIPLIER": 0.0,
        "VENUE_SELECTION_MIN_IMPROVEMENT_BPS": 0.0,
    }.get(name, default))
    states = {
        "delta": _state("delta", "BTCUSD", 75800.0, 100000.0),
        "hyperliquid": _state("hyperliquid", "BTC", 75800.0, 100000.0),
    }
    selection = select_execution_venue(
        states=states, direction="LONG", asset_id="BTC", current_venue="hyperliquid",
        routeable_venues={"delta", "hyperliquid"}, notional_usd=0.0,
        gross_edge_bps=20.0,
        available_cash_by_venue={"delta": 1000.0, "hyperliquid": 10.0},
        notional_by_venue={"delta": 250.0, "hyperliquid": 2.5},
        required_margin_by_venue={"delta": 20.0, "hyperliquid": 1.0},
        protection_capable_venues={"delta", "hyperliquid"},
    )
    assert selection.estimates["hyperliquid"].total_cost_bps < selection.estimates["delta"].total_cost_bps
    assert selection.estimates["delta"].expected_net_profit_usd > selection.estimates["hyperliquid"].expected_net_profit_usd
    assert selection.selected_venue == "delta"
    assert selection.reason == "highest_broker_local_expected_net_profit_route"

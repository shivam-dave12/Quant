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
            "SILVER_HYPERLIQUID_PREFERENCE_BPS": 8.0,
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
    assert selection.estimates["hyperliquid"].preference_adjustment_bps < 0


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


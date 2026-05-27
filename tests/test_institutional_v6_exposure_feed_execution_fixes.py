from __future__ import annotations

import time
import threading
import sys
import types
from types import SimpleNamespace

try:
    import socketio  # type: ignore  # noqa: F401
except ModuleNotFoundError:
    sys.modules["socketio"] = types.SimpleNamespace(Client=lambda **kwargs: None)

import config
from core.instruments import configured_asset_intents
from exchanges.coinswitch.websocket import CoinSwitchWebSocket
from execution.instrument_registry import InstrumentRegistry
from execution.venue_selection import estimate_venue_cost
from market_data.feed_health import score_feed_health
from market_data.normalizer import InstrumentMapping, build_venue_microstate
from strategy.domain import Direction
from strategy.dynamic_protection import (
    DynamicProtectionPlanBuilder,
    SignalDecayEstimate,
)
import strategy.dynamic_protection as dp


def _state(venue: str, symbol: str, mid: float):
    health = score_feed_health(
        connected=True, heartbeat_ok=True, sequence_valid=True, snapshot_ready=True,
        exchange_timestamp_available=True, latency_vs_baseline_z=0.0,
    )
    return build_venue_microstate(
        mapping=InstrumentMapping(
            venue=venue, venue_symbol=symbol, canonical_underlying="TEST",
            product_class="linear_perp", quote_currency="USD", contract_multiplier=1.0,
            settlement_currency="USD", price_tick=0.01, qty_step=0.01,
            execution_enabled=True, notional_model="linear",
        ),
        bids=[(mid - 0.01, 10000.0)], asks=[(mid + 0.01, 10000.0)],
        feed_health=health, receive_ts_ns=time.time_ns(), exchange_ts_ns=time.time_ns() - 1_000_000,
    )


def test_config_partitions_non_fungible_silver_and_gold_exposures():
    intents = {row.asset_id: row.aliases for row in configured_asset_intents(config.MULTI_ASSET_REQUESTS)}
    assert "SILVER" not in intents
    assert set(["SILVER_SLVON", "SILVER_XAG", "SILVER_HL"]).issubset(intents)
    assert "SLVONUSD" in intents["SILVER_SLVON"]
    assert "XAGUSDT" in intents["SILVER_XAG"]
    assert "xyz:SILVER" in intents["SILVER_HL"]
    assert "GOLD_PAXG" in intents and "GOLD_HL" in intents


def test_registry_allows_no_static_primary_exchange_preference():
    assert InstrumentRegistry(execution_preference="").execution_preference is None
    assert config.DISCOVERY_PRIMARY_EXCHANGE == ""


def test_coinswitch_official_nested_orderbook_and_trade_payloads_are_normalised():
    ob = CoinSwitchWebSocket.normalise_orderbook_payload({
        "data": {"symbol": "XAGUSDT", "timestamp": 1770000000000, "bids": [["67.30", "2"]], "asks": [["67.31", "3"]]}
    })
    assert ob is not None
    assert ob["bids"][0][0] == "67.30"
    assert ob["asks"][0][0] == "67.31"
    trades = CoinSwitchWebSocket.normalise_trade_payloads({
        "data": [{"E": 1770000000000, "p": "67.30", "q": "2", "m": True}]
    })
    assert len(trades) == 1
    assert trades[0]["price"] == 67.30
    assert trades[0]["side"] == "sell"


def test_route_cost_never_becomes_negative_price_dislocation_alpha(monkeypatch):
    monkeypatch.setattr("execution.venue_selection._cfg", lambda name, default: {
        "VENUE_ROUND_TRIP_FEE_BPS": {"delta": 3.0},
        "VENUE_SLIPPAGE_IMPACT_MULTIPLIER": 0.0,
        "SILVER_DELTA_ILLIQUIDITY_PENALTY_BPS": 0.0,
    }.get(name, default))
    state = _state("delta", "SLVONUSD", 67.395)
    est = estimate_venue_cost(
        state=state, direction="LONG", asset_id="SILVER_SLVON", reference_mid=74.276,
        notional_usd=15.0, routeable=True, available_cash_usd=100.0,
        required_margin_usd=1.0, protection_capable=True, gross_edge_bps=86.72,
    )
    assert est.relative_touch_bps_diagnostic < 0
    assert est.total_cost_bps >= est.fee_bps >= 0
    assert est.total_cost_bps >= 0
    assert est.expected_net_edge_bps is not None and est.expected_net_edge_bps < 86.72


def test_short_alpha_horizon_is_not_allowed_to_open_wide_silver_bracket(monkeypatch):
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_REQUIRE_SIGNAL_DECAY_READY", True, raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_MIN_EXECUTABLE_HOLD_SEC_BY_ASSET", {"SILVER_SLVON": 60.0}, raising=False)
    engine = DynamicProtectionPlanBuilder("SILVER_SLVON")
    monkeypatch.setattr(engine, "signal_decay", lambda **kwargs: SignalDecayEstimate(
        True, 20, phi=0.4, observation_interval_sec=1.0, half_life_sec=3.0,
        optimal_hold_sec=7.661, reason="ar1_signal_decay_cost_crossing",
    ))
    plan = engine.build_plan(
        direction=Direction.LONG, entry_price=67.40, volatility_price=0.01,
        gross_edge_bps=80.0, execution_cost_bps=4.0, protection_type="VENUE_NATIVE_BRACKET",
        asset_class="commodity", market_state={"asset_id": "SILVER_SLVON", "venue": "delta"},
    )
    assert plan.protection_feasible is False
    assert plan.reasons[0].startswith("signal_horizon_below_protected_execution_min")

class _FakeDeltaCatalog:
    def get_products(self, contract_types=None):
        return {"result": [
            {"symbol": "SLVONUSD", "base_asset": "SLVON", "quote_asset": "USD", "tick_size": "0.01", "contract_value": "0.1", "max_leverage": "25"},
            {"symbol": "PAXGUSD", "base_asset": "PAXG", "quote_asset": "USD", "tick_size": "0.01", "contract_value": "0.01", "max_leverage": "25"},
        ]}


class _FakeCoinSwitchCatalog:
    def get_instrument_info(self, exchange="EXCHANGE_2"):
        return {"data": {
            "XAGUSDT": {"symbol": "XAG", "quote_asset": "USDT", "tick_size": "1", "price_precision": "2", "base_quantity_step_size": "0.1", "max_leverage": "20"},
            "PAXGUSDT": {"symbol": "PAXG", "quote_asset": "USDT", "tick_size": "1", "price_precision": "2", "base_quantity_step_size": "0.01", "max_leverage": "20"},
        }}


class _FakeHyperCatalog:
    def meta_by_dex(self):
        return {"xyz": {"universe": [
            {"name": "xyz:SILVER", "szDecimals": 2, "maxLeverage": 20},
            {"name": "xyz:GOLD", "szDecimals": 3, "maxLeverage": 20},
        ]}}


def test_discovery_never_places_slvon_xag_and_hyper_silver_in_same_route_context():
    report = InstrumentRegistry(execution_preference="").discover(
        delta_api=_FakeDeltaCatalog(), coinswitch_api=_FakeCoinSwitchCatalog(),
        hyperliquid_api=_FakeHyperCatalog(), requested=config.MULTI_ASSET_REQUESTS,
        include_exchanges=("delta", "coinswitch", "hyperliquid"), require_primary=False, max_active=20,
    )
    by_id = {inst.asset_id: set(inst.by_exchange) for inst in report.matched}
    assert by_id["SILVER_SLVON"] == {__import__('core.instruments', fromlist=['ExchangeName']).ExchangeName.DELTA}
    assert by_id["SILVER_XAG"] == {__import__('core.instruments', fromlist=['ExchangeName']).ExchangeName.COINSWITCH}
    assert by_id["SILVER_HL"] == {__import__('core.instruments', fromlist=['ExchangeName']).ExchangeName.HYPERLIQUID}

from strategy.institutional_strategy import InstitutionalStrategy, PositionPhase


def test_independent_originating_venue_can_outscore_configured_primary(monkeypatch, tmp_path):
    monkeypatch.setattr("strategy.institutional_strategy._cfg", lambda name, default: str(tmp_path) if name == "RESEARCH_STORE_PATH" else default)
    strategy = InstitutionalStrategy(instrument=None)
    strategy._asset_id = "BTC"
    strategy._direction_and_edge = lambda desk, price, liq, execution_state=None, btc_composite=None, **kwargs: (
        Direction.LONG,
        25.0 if execution_state.venue == "hyperliquid" else 4.0,
        "venue_flow_long",
        {"weighted_signal_bps": 25.0 if execution_state.venue == "hyperliquid" else 4.0},
    )
    best, rows = strategy._best_originating_venue_signal(
        SimpleNamespace(), {"delta": _state("delta", "BTCUSD", 75000.0), "hyperliquid": _state("hyperliquid", "BTC", 75000.0)}, None, "BTCUSD"
    )
    assert best is not None and best["venue"] == "hyperliquid"
    assert {row["venue"] for row in rows} == {"delta", "hyperliquid"}


def test_protected_entry_supervisor_returns_immediately_and_prevents_tick_blocking(monkeypatch, tmp_path):
    monkeypatch.setattr("strategy.institutional_strategy._cfg", lambda name, default: str(tmp_path) if name == "RESEARCH_STORE_PATH" else default)
    strategy = InstitutionalStrategy(instrument=None)
    def slow_execution(*args, **kwargs):
        time.sleep(0.08)
    monkeypatch.setattr(strategy, "_execute_approved", slow_execution)
    decision = SimpleNamespace(
        sizing=SimpleNamespace(quantity=1.0),
        protection_plan=SimpleNamespace(entry_price=100.0, stop_price=99.0, target_price=102.0, protection_type="VENUE_NATIVE_BRACKET"),
        direction=Direction.LONG, venue="delta", instrument="BTCUSD", model_values={},
    )
    started = time.monotonic()
    strategy._submit_approved_async(decision, None, None)
    elapsed = time.monotonic() - started
    assert elapsed < 0.05
    assert strategy._pos.phase is PositionPhase.ENTERING
    thread = strategy._entry_thread
    assert thread is not None
    thread.join(timeout=1.0)
    assert strategy._pos.phase is PositionPhase.FLAT


def test_runtime_stop_blocks_async_entry_after_preflight(monkeypatch, tmp_path):
    monkeypatch.setattr("strategy.institutional_strategy._cfg", lambda name, default: str(tmp_path) if name == "RESEARCH_STORE_PATH" else default)
    strategy = InstitutionalStrategy(instrument=None)

    class _BlockingOrders:
        active_exchange = "hyperliquid"
        symbol = "BTC"
        display_symbol = "BTC"

        def __init__(self):
            self.leverage_entered = threading.Event()
            self.release_leverage = threading.Event()
            self.placed = []

        def set_leverage(self, leverage):
            self.leverage_entered.set()
            self.release_leverage.wait(timeout=1.0)
            return {"success": True, "leverage": leverage}

        def place_bracket_limit_entry(self, *args, **kwargs):
            self.placed.append((args, kwargs))
            return {"order_id": "late-entry"}

    orders = _BlockingOrders()
    decision = SimpleNamespace(
        sizing=SimpleNamespace(quantity=1.0, leverage_selected=10.0),
        protection_plan=SimpleNamespace(entry_price=100.0, stop_price=99.0, target_price=102.0, protection_type="VENUE_NATIVE_BRACKET"),
        direction=Direction.LONG, venue="hyperliquid", instrument="BTC", model_values={},
    )

    strategy._submit_approved_async(decision, orders, SimpleNamespace())
    assert orders.leverage_entered.wait(timeout=1.0)
    thread = strategy._entry_thread
    assert thread is not None
    strategy.stop_runtime_services()
    orders.release_leverage.set()
    thread.join(timeout=1.0)

    assert orders.placed == []
    assert strategy._pos.phase is PositionPhase.FLAT


def test_split_precious_metal_exposures_remain_in_correlated_portfolio_bucket():
    from risk.portfolio_exposure import PortfolioExposureTracker
    tracker = PortfolioExposureTracker()
    assert tracker.bucket_for("SILVER_SLVON") == "ANTI_DOLLAR_MACRO"
    assert tracker.bucket_for("SILVER_XAG") == "ANTI_DOLLAR_MACRO"
    assert tracker.bucket_for("SILVER_HL") == "ANTI_DOLLAR_MACRO"
    assert tracker.bucket_for("GOLD_PAXG") == "ANTI_DOLLAR_MACRO"
    assert tracker.bucket_for("GOLD_HL") == "ANTI_DOLLAR_MACRO"


def test_hyperliquid_hip3_unresolved_balance_authority_fails_closed():
    from exchanges.hyperliquid.api import HyperliquidAPI
    class Info:
        def query_user_abstraction_state(self, address):
            return {"unknown": True}
        def query_user_dex_abstraction_state(self, address):
            return {"unknown": True}
        def user_state(self, address, dex=""):
            raise AssertionError("unresolved HIP-3 account must not silently read a possibly wrong clearinghouse")
    api = object.__new__(HyperliquidAPI)
    api.info = Info()
    api.account_address = "0xaccount"
    out = api.get_balance("xyz:SILVER")
    assert out["available"] == 0.0
    assert out["balance_verified"] is False
    assert out["source"] == "hyperliquid_balance_authority_unresolved:dex=xyz"

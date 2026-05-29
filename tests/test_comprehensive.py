"""Comprehensive institutional regression suite.

This single test module consolidates the validated V15 regression coverage without
changing production strategy, execution, exchange, risk, or orchestration code.
Each section preserves provenance of the original validated feature suite.
"""

from __future__ import annotations

# ── Shared test bootstrap (formerly tests/conftest.py) ──────────────────────
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

os.environ.setdefault("GROWW_ACCESS_TOKEN", "test-groww-token")
os.environ.setdefault("GROWW_ENABLED", "true")
os.environ.setdefault("GROWW_DISCOVERY_ENABLED", "true")
os.environ.setdefault("DELTA_API_KEY", "test-delta-api")
os.environ.setdefault("DELTA_SECRET_KEY", "test-delta-secret")
os.environ.setdefault("COINSWITCH_API_KEY", "test-coinswitch-api")
os.environ.setdefault("COINSWITCH_SECRET_KEY", "11" * 32)
os.environ.setdefault("HYPERLIQUID_PRIVATE_KEY", "0x" + "1" * 64)
os.environ.setdefault("HYPERLIQUID_MAIN_API_KEY", "0x" + "2" * 40)
os.environ.setdefault("HYPERLIQUID_WALLET_API_KEY", "0x" + "3" * 40)

_orig_read_text = Path.read_text


def _utf8_read_text(self, encoding=None, errors=None, newline=None):
    return _orig_read_text(self, encoding=encoding or "utf-8", errors=errors, newline=newline)


Path.read_text = _utf8_read_text

# ── Preserved regression section: test_coinswitch_live_contract.py ───────────────────────────────

from types import SimpleNamespace

from cryptography.hazmat.primitives.asymmetric import ed25519

from core.instruments import ExchangeName
from execution.instrument_registry import InstrumentRegistry
from execution.order_manager import _CoinSwitchAdapter
from exchanges.coinswitch.api import FuturesAPI


class _OfficialCatalogAPI:
    def get_instrument_info(self, exchange="EXCHANGE_2"):
        assert exchange == "EXCHANGE_2"
        return {
            "data": {
                "BTCUSDT": {
                    "symbol": "BTC",
                    "base_asset": "BTC",
                    "quote_asset": "USDT",
                    "status": "TRADING",
                    "type": "PERPETUAL_FUTURES",
                    "price_precision": 2,
                    "tick_size": 1,
                    "base_quantity_step_size": "0.001",
                    "min_base_quantity": "0.001",
                    "max_base_quantity": "952",
                    "max_leverage": "25",
                }
            }
        }


def test_registry_parses_official_direct_symbol_instrument_shape():
    registry = InstrumentRegistry()
    rows = registry.load_coinswitch(_OfficialCatalogAPI())
    inst = rows["BTCUSDT"]
    assert inst.exchange is ExchangeName.COINSWITCH
    assert inst.symbol == "BTCUSDT"
    assert inst.tick_size == 0.01
    assert inst.lot_step == 0.001
    assert inst.min_qty == 0.001
    assert inst.max_qty == 952.0


def test_signature_uses_decoded_path_and_epoch():
    seed_hex = "11" * 32
    api = FuturesAPI(api_key="test-key", secret_key=seed_hex)
    epoch = "1779860000123"
    signature = api._generate_signature(
        "GET", "/trade/api/v2/futures/ticker",
        {"exchange": "EXCHANGE_2", "symbol": "BTCUSDT"}, epoch=epoch
    )
    expected_msg = "GET/trade/api/v2/futures/ticker?exchange=EXCHANGE_2&symbol=BTCUSDT" + epoch
    public = ed25519.Ed25519PrivateKey.from_private_bytes(bytes.fromhex(seed_hex)).public_key()
    public.verify(bytes.fromhex(signature), expected_msg.encode("utf-8"))


class _OrderAPI:
    def __init__(self):
        self.calls = []

    def place_order(self, **payload):
        self.calls.append(payload)
        oid = f"oid-{len(self.calls)}"
        if payload["order_type"] == "LIMIT":
            return {"data": {"order_id": oid, "status": "EXECUTED", "exec_quantity": payload["quantity"], "avg_execution_price": payload["price"]}}
        return {"data": {"order_id": oid, "status": "RAISED"}}

    def get_order(self, order_id, exchange="EXCHANGE_2"):
        if order_id in {"oid-2", "oid-3"}:
            return {"data": {"order_id": order_id, "status": "RAISED"}}
        return {"data": {"order_id": order_id, "status": "EXECUTED", "exec_quantity": 0.01, "avg_execution_price": 75000.0}}

    def cancel_order(self, order_id, exchange="EXCHANGE_2"):
        return {"data": {"order_id": order_id}}


def test_coinswitch_entry_arms_position_level_reduce_only_sl_and_tp():
    api = _OrderAPI()
    inst = SimpleNamespace(symbol="BTCUSDT", display_symbol="BTC/USDT", tick_size=0.1, lot_step=0.001, min_qty=0.001, max_qty=1.0)
    adapter = _CoinSwitchAdapter(api, inst)
    adapter.limiter = SimpleNamespace(wait=lambda: None)
    out = adapter.place_bracket_limit_entry("BUY", 0.01, 75000.0, 74500.0, 76000.0, timeout_sec=1.0)
    assert out["protection_confirmed"] is True
    assert [c["order_type"] for c in api.calls] == ["LIMIT", "STOP_MARKET", "TAKE_PROFIT_MARKET"]
    for call in api.calls[1:]:
        assert call["quantity"] == 0.0
        assert call["reduce_only"] is True


class _FailedProtectionAPI(_OrderAPI):
    def place_order(self, **payload):
        self.calls.append(payload)
        if payload["order_type"] == "LIMIT":
            return {"data": {"order_id": "entry", "status": "EXECUTED", "exec_quantity": payload["quantity"], "avg_execution_price": payload["price"]}}
        if payload["order_type"] == "STOP_MARKET":
            return {"error": "protection_failed"}
        if payload["order_type"] == "TAKE_PROFIT_MARKET":
            return {"data": {"order_id": "tp", "status": "RAISED"}}
        return {"data": {"order_id": "flat", "status": "EXECUTED"}}


def test_coinswitch_emergency_close_on_protection_failure_is_reduce_only():
    api = _FailedProtectionAPI()
    inst = SimpleNamespace(symbol="BTCUSDT", display_symbol="BTC/USDT", tick_size=0.1, lot_step=0.001, min_qty=0.001, max_qty=1.0)
    adapter = _CoinSwitchAdapter(api, inst)
    adapter.limiter = SimpleNamespace(wait=lambda: None)
    out = adapter.place_bracket_limit_entry("BUY", 0.01, 75000.0, 74500.0, 76000.0, timeout_sec=1.0)
    assert out["_error"] is True
    emergency = api.calls[-1]
    assert emergency["order_type"] == "MARKET"
    assert emergency["reduce_only"] is True

# ── Preserved regression section: test_config_control_plane.py ───────────────────────────────

import importlib
from pathlib import Path

import config
import strategy.institutional_strategy as institutional_strategy


def test_env_cannot_override_non_secret_runtime_policy(monkeypatch):
    monkeypatch.setenv("INSTITUTIONAL_ENABLE_LIVE_ENTRIES", "true")
    monkeypatch.setenv("EXECUTION_EXCHANGE", "groww")
    monkeypatch.setenv("UNIVERSE_INCLUDE_EXCHANGES", "groww")
    monkeypatch.setenv("GROWW_SESSION_MODEL_AUDIT_FULL_INFO", "true")
    reloaded = importlib.reload(config)
    assert reloaded.LIVE_TRADING_ENABLED is True
    assert reloaded.INSTITUTIONAL_ENABLE_LIVE_ENTRIES is True
    assert reloaded.LIVE_EXECUTION_VENUES == ("delta", "coinswitch", "hyperliquid")
    assert reloaded.EXECUTION_EXCHANGE == "delta"
    assert reloaded.UNIVERSE_INCLUDE_EXCHANGES == "delta,coinswitch,hyperliquid,groww"
    assert reloaded.GROWW_SESSION_MODEL_AUDIT_FULL_INFO is False


def test_env_example_contains_secrets_only():
    text = Path(__file__).resolve().parents[1].joinpath(".env.example").read_text()
    forbidden = {
        "LIVE_TRADING_ENABLED", "INSTITUTIONAL_ENABLE_LIVE_ENTRIES", "EXECUTION_EXCHANGE",
        "UNIVERSE_INCLUDE_EXCHANGES", "GROWW_APPROVED_STATIC_IPS",
        "GROWW_SEBI_ALGO_REGISTRATION_CONFIRMED", "INSTITUTIONAL_DECISION_TELEMETRY_ENABLED",
    }
    assert not [key for key in sorted(forbidden) if f"{key}=" in text]
    for required in ("TELEGRAM_BOT_TOKEN=", "GROWW_TOTP_TOKEN=", "GROWW_TOTP_SECRET="):
        assert required in text


def test_live_order_permission_is_venue_allowlisted(monkeypatch):
    values = {"LIVE_TRADING_ENABLED": True, "LIVE_EXECUTION_VENUES": ("groww",)}
    monkeypatch.setattr(institutional_strategy, "_cfg", lambda name, default: values.get(name, default))
    assert institutional_strategy._live_routing_permission("groww") == (True, "live_execution_authorised")
    assert institutional_strategy._live_routing_permission("delta") == (False, "live_execution_venue_not_authorised:delta")


def test_groww_live_policy_fails_closed_without_ip_or_broker_confirmation(monkeypatch):
    monkeypatch.setattr(config, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(config, "LIVE_EXECUTION_VENUES", ("groww",))
    monkeypatch.setattr(config, "GROWW_APPROVED_STATIC_IPS", ())
    monkeypatch.setattr(config, "GROWW_SEBI_ALGO_REGISTRATION_CONFIRMED", False)
    try:
        config.validate_live_control_plane()
    except ValueError as exc:
        message = str(exc)
        assert "GROWW_APPROVED_STATIC_IPS" in message
        assert "GROWW_SEBI_ALGO_REGISTRATION_CONFIRMED" in message
    else:
        raise AssertionError("Groww live policy must fail closed without IP and broker confirmation")


def test_delta_only_live_policy_does_not_require_groww_prerequisites(monkeypatch):
    monkeypatch.setattr(config, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(config, "LIVE_EXECUTION_VENUES", ("delta",))
    monkeypatch.setattr(config, "GROWW_APPROVED_STATIC_IPS", ())
    monkeypatch.setattr(config, "GROWW_SEBI_ALGO_REGISTRATION_CONFIRMED", False)
    monkeypatch.setattr(config, "DELTA_API_KEY", "test")
    monkeypatch.setattr(config, "DELTA_SECRET_KEY", "test")
    config.validate_live_control_plane()


def test_three_venue_live_policy_fails_closed_without_coinswitch_credentials(monkeypatch):
    monkeypatch.setattr(config, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(config, "LIVE_EXECUTION_VENUES", ("delta", "coinswitch", "hyperliquid"))
    monkeypatch.setattr(config, "DELTA_API_KEY", "test")
    monkeypatch.setattr(config, "DELTA_SECRET_KEY", "test")
    monkeypatch.setattr(config, "COINSWITCH_EXECUTION_ENABLED", True)
    monkeypatch.setattr(config, "COINSWITCH_API_KEY", "")
    monkeypatch.setattr(config, "COINSWITCH_SECRET_KEY", "")
    monkeypatch.setattr(config, "HYPERLIQUID_EXECUTION_ENABLED", True)
    monkeypatch.setattr(config, "HYPERLIQUID_PRIVATE_KEY", "test")
    monkeypatch.setattr(config, "HYPERLIQUID_MAIN_API_KEY", "test")
    try:
        config.validate_live_control_plane()
    except ValueError as exc:
        assert "COINSWITCH_API_KEY" in str(exc)
    else:
        raise AssertionError("Three-venue live policy must fail closed without CoinSwitch credentials")

# ── Preserved regression section: test_dynamic_protection.py ───────────────────────────────

import math

from strategy.domain import Direction
from strategy.dynamic_protection import DynamicProtectionPlanBuilder
import strategy.dynamic_protection as dp


def _relax_warmup(monkeypatch):
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_MIN_SIGNAL_OBSERVATIONS", 8, raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_MIN_KYLE_OBSERVATIONS", 8, raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_MIN_VPIN_BUCKETS", 3, raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_VPIN_WINDOW_BUCKETS", 5, raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_REQUIRE_SIGNAL_DECAY_READY", True, raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_FOR_DELTA", True, raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_VENUES", ("delta",), raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_VENUES", ("delta",), raising=False)


def test_ar1_signal_decay_produces_finite_cost_crossing_horizon(monkeypatch):
    _relax_warmup(monkeypatch)
    engine = DynamicProtectionPlanBuilder("GOLD")
    for i in range(20):
        engine.observe(signal_bps=12.0 * (0.88 ** i), timestamp_s=100.0 + i * 10.0)
    estimate = engine.signal_decay(current_edge_bps=8.0, total_exit_cost_bps=2.5)
    assert estimate.ready is True
    assert estimate.phi is not None and 0.0 < estimate.phi < 1.0
    assert estimate.half_life_sec is not None and estimate.half_life_sec > 0
    assert estimate.optimal_hold_sec is not None and estimate.optimal_hold_sec > 0


def test_kyle_impact_uses_primary_signed_ofi_price_impact(monkeypatch):
    _relax_warmup(monkeypatch)
    engine = DynamicProtectionPlanBuilder("GOLD")
    rows = []
    px = 4500.0
    for i in range(20):
        signed_ofi = 1000.0 if i % 2 == 0 else -800.0
        px *= 1.0 + (signed_ofi * 0.000002) / 10000.0
        rows.append({"timestamp_s": 100 + i, "microprice": px, "signed_ofi_usd": signed_ofi})
    engine.observe(signal_bps=1.0, timestamp_s=100, research_state={"book_events": rows})
    impact = engine.kyle_impact(exit_notional=10000.0)
    assert impact.ready is True
    assert impact.lambda_bps_per_usd is not None and impact.lambda_bps_per_usd > 0
    assert impact.expected_exit_impact_bps is not None and impact.expected_exit_impact_bps > 0


def test_vpin_toxicity_widens_stop_for_one_sided_tape(monkeypatch):
    _relax_warmup(monkeypatch)
    engine = DynamicProtectionPlanBuilder("SILVER")
    trades = [{"timestamp_s": 100 + i, "signed_notional_usd": 1000.0} for i in range(40)]
    engine.observe(signal_bps=1.0, timestamp_s=100, research_state={"trade_events": trades})
    toxicity = engine.vpin()
    assert toxicity.ready is True
    assert toxicity.vpin is not None and toxicity.vpin > 0.90
    assert toxicity.stop_multiplier > 1.0


def test_dynamic_plan_uses_signal_edge_cost_and_liquidation_schedule(monkeypatch):
    _relax_warmup(monkeypatch)
    engine = DynamicProtectionPlanBuilder("GOLD")
    for i in range(18):
        engine.observe(signal_bps=10.0 * (0.92 ** i), timestamp_s=100.0 + i)
    books = []
    px = 4500.0
    for i in range(20):
        ofi = 1000.0 if i % 2 == 0 else -900.0
        px *= 1 + (ofi * 0.000001) / 10000.0
        books.append({"timestamp_s": 200 + i, "microprice": px, "signed_ofi_usd": ofi})
    trades = [{"timestamp_s": 200 + i, "signed_notional_usd": (1000.0 if i % 5 else -1000.0)} for i in range(40)]
    engine.observe(signal_bps=4.0, timestamp_s=300.0, research_state={"book_events": books, "trade_events": trades})
    plan = engine.build_plan(
        direction=Direction.LONG,
        entry_price=4500.0,
        volatility_price=12.5,
        gross_edge_bps=8.0,
        execution_cost_bps=2.5,
        protection_type="VENUE_NATIVE_BRACKET",
        asset_class="commodity",
        position_notional=10000.0,
        quantity=3.0,
    )
    assert plan.protection_feasible is True
    assert plan.stop_price < plan.entry_price < plan.target_price
    assert plan.diagnostics["edge_scalar_rr"] >= 1.15
    assert plan.diagnostics["signal_decay"]["ready"] is True
    assert plan.diagnostics["vpin"]["ready"] is True
    assert plan.diagnostics["almgren_chriss"]["ready"] is True


def test_dynamic_plan_uses_spread_tick_and_policy_floors_for_silver(monkeypatch):
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_REQUIRE_SIGNAL_DECAY_READY", False, raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_FOR_DELTA", False, raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_FOR_DELTA", False, raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_VENUES", (), raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_VENUES", (), raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_ASSET_MIN_STOP_BPS", {"SILVER": 45.0}, raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_VENUE_ASSET_MIN_STOP_BPS", {"hyperliquid:SILVER": 60.0}, raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_ASSET_MIN_TARGET_BPS", {"SILVER": 100.0}, raising=False)
    engine = DynamicProtectionPlanBuilder("SILVER")
    plan = engine.build_plan(
        direction=Direction.LONG,
        entry_price=30.0,
        volatility_price=0.01,
        gross_edge_bps=12.0,
        execution_cost_bps=2.0,
        protection_type="VENUE_NATIVE_BRACKET",
        asset_class="commodity",
        position_notional=5000.0,
        quantity=100.0,
        market_state={
            "asset_id": "SILVER",
            "venue": "hyperliquid",
            "spread_bps": 10.0,
            "price_tick": 0.01,
            "near_touch_depth_usd": 20000.0,
            "policy_min_rr": 2.20,
            "policy_max_rr": 5.50,
        },
    )
    stop_distance = plan.entry_price - plan.stop_price
    target_distance = plan.target_price - plan.entry_price
    assert stop_distance >= 30.0 * 60.0 / 10000.0 - 1e-9
    assert target_distance / stop_distance >= 2.20
    assert plan.diagnostics["market_geometry"]["spread_floor_distance"] >= 30.0 * 60.0 / 10000.0


def test_option_greek_exit_diagnostics_fires_on_delta_iv_and_theta(monkeypatch):
    monkeypatch.setattr(dp.config, "DYNAMIC_OPTION_EXIT_MIN_ABS_DELTA", 0.10, raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_OPTION_EXIT_IV_COLLAPSE_ABS", 0.02, raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_OPTION_CHEAP_VRP_THRESHOLD", -0.10, raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_OPTION_CHEAP_VRP_THETA_LIMIT", 0.12, raising=False)
    result = DynamicProtectionPlanBuilder.option_exit_diagnostics(
        abs_delta=0.08,
        current_iv=0.10,
        entry_iv=0.13,
        theta_to_premium_per_day=0.13,
        dte=3.0,
        vrp=-0.14,
    )
    assert result.exit_required is True
    assert "option_delta_exposure_collapsed" in result.reasons
    assert "option_iv_collapse" in result.reasons
    assert "option_theta_carry_structurally_excessive" in result.reasons


def test_delta_plan_fails_closed_until_kyle_and_vpin_are_observed(monkeypatch):
    _relax_warmup(monkeypatch)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_FOR_DELTA", True, raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_VENUES", ("delta",), raising=False)
    engine = DynamicProtectionPlanBuilder("GOLD")
    for i in range(12):
        engine.observe(signal_bps=8.0 * (0.92 ** i), timestamp_s=100.0 + i)
    plan = engine.build_plan(
        direction=Direction.LONG,
        entry_price=4500.0,
        volatility_price=10.0,
        gross_edge_bps=8.0,
        execution_cost_bps=2.5,
        protection_type="VENUE_NATIVE_BRACKET",
        asset_class="commodity",
        position_notional=10000.0,
        quantity=2.0,
        market_state={"asset_id": "GOLD", "venue": "delta"},
    )
    assert plan.protection_feasible is False
    assert any("kyle_lambda_warmup" in reason for reason in plan.reasons)


def test_hyperliquid_plan_is_not_blocked_by_delta_only_kyle_and_vpin_warmup(monkeypatch):
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_REQUIRE_SIGNAL_DECAY_READY", False, raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_VENUES", ("delta",), raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_VENUES", ("delta",), raising=False)
    engine = DynamicProtectionPlanBuilder("SILVER")
    plan = engine.build_plan(
        direction=Direction.SHORT, entry_price=73.82, volatility_price=0.12,
        gross_edge_bps=28.0, execution_cost_bps=2.7,
        protection_type="VENUE_NATIVE_BRACKET", asset_class="commodity",
        position_notional=1000.0, quantity=10.0,
        market_state={"asset_id": "SILVER", "venue": "hyperliquid", "spread_bps": 0.2, "price_tick": 0.001, "near_touch_depth_usd": 1000000.0},
    )
    assert plan.protection_feasible is True
    assert not any("kyle_lambda_warmup" in reason for reason in plan.reasons)
    assert not any("vpin_trade_tape_warmup" in reason for reason in plan.reasons)

# ── Preserved regression section: test_groww_docs_contract.py ───────────────────────────────

import sys

import pytest
from types import ModuleType, SimpleNamespace

from exchanges.groww.api import GrowwRestClient
from exchanges.groww.live_feed import GrowwLiveFeedHub
from execution.groww_long_option_execution import (
    GrowwLongOptionExecutionState,
    GrowwLongOptionExecutor,
    GrowwProtectionPlan,
    LongOptionCandidateScore,
)


def test_official_totp_token_is_minted_before_client_construction(monkeypatch):
    from exchanges.groww import api as api_module

    calls = {}

    class FakeGrowwAPI:
        @staticmethod
        def get_access_token(**kwargs):
            calls["mint"] = kwargs
            return "generated-bearer-access-token"

        def __init__(self, token):
            calls["client_token"] = token

    fake_growwapi = ModuleType("growwapi")
    fake_growwapi.GrowwAPI = FakeGrowwAPI
    fake_pyotp = ModuleType("pyotp")
    fake_pyotp.TOTP = lambda secret: SimpleNamespace(now=lambda: "123456")
    monkeypatch.setitem(sys.modules, "growwapi", fake_growwapi)
    monkeypatch.setitem(sys.modules, "pyotp", fake_pyotp)
    monkeypatch.setattr(api_module.config, "GROWW_ACCESS_TOKEN", "", raising=False)
    monkeypatch.setattr(api_module.config, "GROWW_TOTP_TOKEN", "", raising=False)
    monkeypatch.setattr(api_module.config, "GROWW_TOTP_SECRET", "", raising=False)
    monkeypatch.setattr(api_module.config, "GROWW_API_KEY", "", raising=False)
    monkeypatch.setattr(api_module.config, "GROWW_API_SECRET", "", raising=False)
    monkeypatch.delenv("GROWW_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("GROWW_API_KEY", raising=False)
    monkeypatch.delenv("GROWW_API_SECRET", raising=False)
    monkeypatch.delenv("GROWW_TOTP_TOKEN", raising=False)
    monkeypatch.delenv("GROWW_TOTP_SECRET", raising=False)

    wrapper = GrowwRestClient(totp_token="console-totp-token", totp_secret="base32-secret")
    _ = wrapper.client

    assert calls["mint"] == {"api_key": "console-totp-token", "totp": "123456"}
    assert calls["client_token"] == "generated-bearer-access-token"


def test_official_instrument_csv_is_cached_in_application_owned_path(monkeypatch, tmp_path):
    from exchanges.groww import api as api_module
    import requests

    csv_text = (
        "exchange,exchange_token,trading_symbol,groww_symbol,segment,instrument_type,"
        "underlying_symbol,expiry_date,strike_price,lot_size,tick_size\n"
        "NSE,1001,NIFTY26JUN25000CE,NSE-NIFTY26JUN25000CE,FNO,CE,NIFTY,2026-06-25,25000,50,0.05\n"
    )

    class Response:
        text = csv_text
        def raise_for_status(self):
            return None

    monkeypatch.setattr(requests, "get", lambda *a, **k: Response())
    cache = tmp_path / "groww" / "instruments.csv"
    monkeypatch.setattr(api_module.config, "GROWW_SECURITY_MASTER_CACHE_PATH", str(cache), raising=False)
    wrapper = GrowwRestClient(access_token="test")

    rows = wrapper.get_all_instruments(force_refresh=True)

    assert rows[0]["exchange_token"] == "1001"
    assert cache.read_text(encoding="utf-8") == csv_text
    assert "/site-packages/" not in str(cache)


def test_index_and_option_feeds_use_documented_subscription_types():
    calls = []

    class FakeFeed:
        def consume(self):
            return None
        def subscribe_index_value(self, instruments, on_data_received=None):
            calls.append(("index_value", instruments))
        def get_index_value(self):
            return {}
        def subscribe_ltp(self, instruments, on_data_received=None):
            calls.append(("ltp", instruments))
        def get_ltp(self):
            return {}
        def subscribe_market_depth(self, instruments, on_data_received=None):
            calls.append(("market_depth", instruments))
        def get_market_depth(self):
            return {}

    api = SimpleNamespace(
        _option_symbol_from_route=lambda route: "NIFTY26JUN25000CE",
        resolve_exchange_token=lambda **kwargs: "1001",
    )
    hub = GrowwLiveFeedHub(api)
    hub.feed = FakeFeed()

    hub.subscribe_underlying_quotes("NSE", "NIFTY", lambda row: None)
    hub.subscribe_option_market_data(
        stock_code="NIFTY", expiry_date="2026-06-25", strike_price="25000", right="Call", callback=lambda row: None
    )

    assert calls[0] == ("index_value", [{"exchange": "NSE", "segment": "CASH", "exchange_token": "NIFTY"}])
    assert calls[1] == ("ltp", [{"exchange": "NSE", "segment": "FNO", "exchange_token": "1001"}])
    assert calls[2] == ("market_depth", [{"exchange": "NSE", "segment": "FNO", "exchange_token": "1001"}])


def test_historical_data_calls_only_documented_get_historical_candles():
    calls = {}

    class FakeSDK:
        CANDLE_INTERVAL_MIN_15 = "15minute"
        def get_historical_candles(self, **kwargs):
            calls.update(kwargs)
            return {"candles": [["2026-05-27 09:15:00", 100, 102, 99, 101, 25]]}

    wrapper = GrowwRestClient(access_token="test")
    wrapper._client = FakeSDK()

    out = wrapper.get_historical_candles_canonical(
        exchange="NSE",
        segment="CASH",
        trading_symbol="NIFTY",
        groww_symbol="NSE-NIFTY",
        interval="15minute",
        from_date="2026-05-27 09:15:00",
        to_date="2026-05-27 10:15:00",
    )

    assert calls["groww_symbol"] == "NSE-NIFTY"
    assert calls["candle_interval"] == "15minute"
    assert out["_source"] == "groww.get_historical_candles"


def test_oco_submission_response_is_not_accepted_as_protection_confirmation():
    class Api:
        def const(self, name, default):
            return default
        def reference_id(self, prefix):
            return f"{prefix}-12345678"[:20]
        def place_order(self, **kwargs):
            return {"groww_order_id": "entry" if kwargs["transaction_type"] == "BUY" else "emergency"}
        def get_order_detail(self, **kwargs):
            return {"order_status": "FILLED", "filled_quantity": 50, "average_price": 100.0}
        def create_smart_order(self, **kwargs):
            return {"smart_order_id": "oco", "status": "ACTIVE"}
        def get_smart_order(self, **kwargs):
            return {}  # No documented status confirmation.
        def cancel_order(self, **kwargs):
            return {}

    candidate = LongOptionCandidateScore(
        trading_symbol="NIFTY26JUN25000CE", option_type="CE", expiry="2026-06-25",
        strike=25000, premium=100.0, spread_bps=10, delta=0.5, gamma=0.01,
        theta=-1, vega=1, iv=0.15, expected_premium_return_after_cost=0.1,
        probability_tp_before_sl=0.55, theta_cost_for_expected_hold=1.0,
        liquidity_score=0.8, protection_feasible=True, total_score=0.7, lot_size=50,
    )
    executor = GrowwLongOptionExecutor(Api())
    result = executor.execute(
        candidate=candidate,
        quantity=50,
        limit_price=100.0,
        protection=GrowwProtectionPlan(target_price=125.0, stop_trigger_price=80.0, stop_limit_price=79.95),
        fill_timeout_sec=0,
    )

    assert result.state is GrowwLongOptionExecutionState.UNPROTECTED_POSITION_EMERGENCY
    assert result.protection_confirmed is False
    assert result.emergency_order_id == "emergency"


def test_runtime_image_scopes_groww_sdk_state_boundary_for_botuser():
    from pathlib import Path

    dockerfile = (Path(__file__).resolve().parents[1] / "Dockerfile").read_text(encoding="utf-8")
    assert 'common_state_dir = sdk_root / "common"' in dockerfile
    assert 'instruments_cache = sdk_root / "instruments.csv"' in dockerfile
    assert "os.chown(common_state_dir, user.pw_uid, user.pw_gid)" in dockerfile
    assert "os.chmod(common_state_dir, 0o700)" in dockerfile
    assert "os.chown(instruments_cache, user.pw_uid, user.pw_gid)" in dockerfile
    assert "chown -R botuser:botuser /usr/local/lib/python3.11/site-packages" not in dockerfile
    assert "chmod -R" not in dockerfile


def test_option_chain_normalisation_preserves_official_nested_greeks_without_inventing_depth():
    wrapper = GrowwRestClient(access_token="test")
    rows = wrapper._normalise_option_chain(
        {
            "underlying_ltp": 25000.0,
            "strikes": {
                "25000": {
                    "CE": {
                        "greeks": {"delta": 0.48, "gamma": 0.001, "theta": -8.2, "vega": 11.5, "rho": 1.2, "iv": 14.5},
                        "trading_symbol": "NIFTY26J0225000CE",
                        "ltp": 112.0,
                        "open_interest": 500,
                        "volume": 200,
                    }
                }
            },
        },
        "NIFTY",
        "2026-06-02",
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["delta"] == 0.48
    assert row["iv"] == 14.5
    assert row["official_greeks_source"] == "groww_option_chain"
    assert "best_bid_price" not in row
    assert "best_offer_price" not in row


def test_session_book_screens_chain_then_accepts_documented_stream_depth(monkeypatch):
    from datetime import datetime, timedelta, timezone
    from types import SimpleNamespace
    from agents import groww_chain_architect as chain

    expiry = (datetime.now(timezone.utc) + timedelta(days=7)).strftime("%Y-%m-%d")
    ce_symbol = "NIFTY26J0225000CE"
    pe_symbol = "NIFTY26J0225000PE"
    instrument = SimpleNamespace(
        asset_id="NIFTY",
        primary=SimpleNamespace(
            raw={
                "desk_id": "GROWW_INDEX_OPTIONS",
                "stock_code": "NIFTY",
                "underlying": "NIFTY",
                "chain_source": "official_instrument_csv_plus_get_option_chain",
                "chain_candidates": [
                    {
                        "right": "Call", "option_type": "CE", "TradingSymbol": ce_symbol,
                        "trading_symbol": ce_symbol, "strike_price": 25000, "expiry_date": expiry,
                        "runtime_lot_size": 25, "ltp": 100.0, "delta": 0.46,
                        "iv": 15.0, "open_interest": 1000, "volume": 1000,
                    },
                    {
                        "right": "Put", "option_type": "PE", "TradingSymbol": pe_symbol,
                        "trading_symbol": pe_symbol, "strike_price": 25000, "expiry_date": expiry,
                        "runtime_lot_size": 25, "ltp": 105.0, "delta": -0.44,
                        "iv": 15.0, "open_interest": 1200, "volume": 900,
                    },
                ],
            }
        ),
    )
    monkeypatch.setattr(chain.config, "GROWW_SESSION_BOOK_REQUIRE_TWO_SIDED_QUOTE", True, raising=False)
    monkeypatch.setattr(chain.config, "GROWW_OPTION_MIN_DTE", 1.0, raising=False)
    monkeypatch.setattr(chain.config, "GROWW_OPTION_MAX_DTE", 21.0, raising=False)
    # This test validates transport/executable-book wiring rather than theta policy.

    ce_shortlist = chain.shortlist_contracts_for_stream_validation(
        instrument, "long", underlying_spot=25000.0, available_funds=25852.96, limit=4
    )
    pe_shortlist = chain.shortlist_contracts_for_stream_validation(
        instrument, "short", underlying_spot=25000.0, available_funds=25852.96, limit=4
    )
    assert ce_shortlist[0]["TradingSymbol"] == ce_symbol
    assert pe_shortlist[0]["TradingSymbol"] == pe_symbol
    # An option-chain row alone is not executable; the map below represents documented live market-depth packets.
    assert chain.build_session_contract_book(
        instrument, underlying_spot=25000.0, available_funds=25852.96, commit=False
    ) is None

    live_book_by_symbol = {
        ce_symbol: {"bid_price": 99.5, "offer_price": 100.0, "bid_quantity": 100, "offer_quantity": 100},
        pe_symbol: {"bid_price": 104.0, "offer_price": 104.5, "bid_quantity": 100, "offer_quantity": 100},
    }
    book = chain.build_session_contract_book(
        instrument, underlying_spot=25000.0, available_funds=25852.96,
        option_quote_by_symbol=live_book_by_symbol, commit=False,
    )
    assert book is not None
    assert book.call.selected_symbol == ce_symbol
    assert book.put.selected_symbol == pe_symbol


def test_execution_freshness_requires_independently_fresh_ltp_and_market_depth():
    import time
    from types import SimpleNamespace
    from exchanges.groww.data_manager import GrowwOptionDataManager

    manager = GrowwOptionDataManager(
        instrument=SimpleNamespace(asset_id="NIFTY", primary=SimpleNamespace(raw={})),
        api=SimpleNamespace(),
    )
    now = time.time()
    manager._last_price = 100.0
    manager._best_bid = 99.5
    manager._best_ask = 100.0
    manager._best_bid_qty = 100
    manager._best_ask_qty = 100
    manager._last_ltp_stream_ts = now
    manager._last_depth_stream_ts = 0.0
    assert manager._execution_price_fresh(10.0) is False
    manager._last_depth_stream_ts = now
    assert manager._execution_price_fresh(10.0) is True
    manager._last_ltp_stream_ts = now - 60.0
    assert manager._execution_price_fresh(10.0) is False


def test_market_depth_uses_documented_level_one_not_mapping_insertion_order():
    row = next(iter(GrowwLiveFeedHub._flatten_market_depth({
        "NSE": {"FNO": {"1001": {
            "buyBook": {
                "2": {"price": 99.0, "qty": 5},
                "1": {"price": 100.0, "qty": 25},
            },
            "sellBook": {
                "2": {"price": 102.0, "qty": 5},
                "1": {"price": 101.0, "qty": 30},
            },
        }}}
    })))
    assert row["best_bid_price"] == 100.0
    assert row["best_bid_quantity"] == 25.0
    assert row["best_offer_price"] == 101.0
    assert row["best_offer_quantity"] == 30.0


def test_option_subscription_does_not_misattribute_shared_feed_tokens():
    callbacks = {}

    class FakeFeed:
        def consume(self):
            return None
        def subscribe_ltp(self, instruments, on_data_received=None):
            callbacks["ltp"] = on_data_received
        def subscribe_market_depth(self, instruments, on_data_received=None):
            callbacks["depth"] = on_data_received
        def get_ltp(self):
            return {"ltp": {"NSE": {"FNO": {
                "1001": {"ltp": 100.0},
                "2002": {"ltp": 999.0},
            }}}}
        def get_market_depth(self):
            return {"NSE": {"FNO": {
                "1001": {"buyBook": {"1": {"price": 99.5, "qty": 50}}, "sellBook": {"1": {"price": 100.0, "qty": 50}}},
                "2002": {"buyBook": {"1": {"price": 998.0, "qty": 50}}, "sellBook": {"1": {"price": 999.0, "qty": 50}}},
            }}}

    api = SimpleNamespace(
        _option_symbol_from_route=lambda route: "NIFTY26J0225000CE",
        resolve_exchange_token=lambda **kwargs: "1001",
    )
    received = []
    hub = GrowwLiveFeedHub(api)
    hub.feed = FakeFeed()
    hub.subscribe_option_market_data(
        stock_code="NIFTY", expiry_date="2026-06-02", strike_price="25000", right="Call", callback=received.append
    )
    callbacks["ltp"]()
    callbacks["depth"]()
    assert len(received) == 2
    assert all(row["exchange_token"] == "1001" for row in received)
    assert received[0]["last_price"] == 100.0
    assert received[1]["best_bid_price"] == 99.5
    assert received[1]["best_offer_price"] == 100.0


def test_session_discovery_selects_from_documented_live_fno_depth_not_rest_quote(monkeypatch):
    from exchanges.groww import data_manager as dm
    from exchanges.groww.data_manager import GrowwOptionDataManager

    ce = {"TradingSymbol": "NIFTY26J0225000CE", "trading_symbol": "NIFTY26J0225000CE", "stock_code": "NIFTY", "expiry_date": "2026-06-02", "right": "Call", "strike_price": 25000}
    pe = {"TradingSymbol": "NIFTY26J0225000PE", "trading_symbol": "NIFTY26J0225000PE", "stock_code": "NIFTY", "expiry_date": "2026-06-02", "right": "Put", "strike_price": 25000}
    monkeypatch.setattr(dm, "shortlist_contracts_for_stream_validation", lambda _instrument, thesis, **_kw: [ce] if thesis == "long" else [pe])
    monkeypatch.setattr(dm, "build_session_contract_book", lambda *_args, **_kwargs: SimpleNamespace())
    monkeypatch.setattr(dm.config, "GROWW_SESSION_BOOK_STREAM_CANDIDATES_PER_SIDE", 1, raising=False)
    monkeypatch.setattr(dm.config, "GROWW_SESSION_BOOK_STREAM_DISCOVERY_TIMEOUT_SEC", 1.0, raising=False)

    class Api:
        @staticmethod
        def _normalise_right(value):
            return "call" if str(value).lower() in {"call", "ce"} else "put"

    class Hub:
        def __init__(self):
            self.unsubscribed = []
        def subscribe_option_universe_market_data(self, *, routes, callback):
            for route in routes:
                right = route["right"]
                symbol = "NIFTY26J0225000CE" if right == "call" else "NIFTY26J0225000PE"
                px = 100.0 if right == "call" else 105.0
                callback({"TradingSymbol": symbol, "ltp": px})
                callback({"TradingSymbol": symbol, "bid_price": px - 0.5, "offer_price": px, "bid_quantity": 100, "offer_quantity": 100})
            return ["ltp:universe", "depth:universe"]
        def unsubscribe(self, ids):
            self.unsubscribed.extend(ids)

    hub = Hub()
    monkeypatch.setattr(dm, "hub_for_api", lambda _api: hub)
    manager = GrowwOptionDataManager(
        instrument=SimpleNamespace(asset_id="NIFTY", primary=SimpleNamespace(raw={})), api=Api()
    )
    books = manager._stream_executable_shortlist(25000.0, 25852.96)
    assert set(books) == {"NIFTY26J0225000CE", "NIFTY26J0225000PE"}
    assert books["NIFTY26J0225000CE"]["selection_liquidity_source"] == "groww.subscribe_market_depth"
    assert len(hub.unsubscribed) == 2



def test_selected_option_identity_accepts_official_title_case_right_payload():
    from exchanges.groww.data_manager import GrowwOptionDataManager

    class Api:
        @staticmethod
        def _normalise_right(value):
            value = str(value or "").strip().lower()
            return "Call" if value in {"call", "ce", "c"} else "Put" if value in {"put", "pe", "p"} else ""

    manager = GrowwOptionDataManager(
        instrument=SimpleNamespace(asset_id="NIFTY", primary=SimpleNamespace(raw={})), api=Api()
    )
    identity = {"stock_code": "NIFTY", "expiry": "02-Jun-2026", "right": "Call", "strike": 24100.0}
    official_tick = {
        "exchange": "NFO", "stock_code": "NIFTY", "expiry_date": "02-Jun-2026",
        "right": "Call", "strike_price": 24100.0, "ltp": 89.47,
    }

    assert manager._matches_option_identity_tick(official_tick, identity) is True


def test_native_hour_interval_aliases_do_not_collapse_to_one_minute():
    assert GrowwRestClient._interval_minutes("1hour") == 60
    assert GrowwRestClient._interval_minutes("4hour") == 240
    with pytest.raises(RuntimeError, match="Unsupported Groww candle interval"):
        GrowwRestClient._interval_minutes("unknown")



def test_session_book_measures_theta_carry_instead_of_static_preselection_veto(monkeypatch):
    from datetime import datetime, timedelta, timezone
    from types import SimpleNamespace
    from agents import groww_chain_architect as chain
    expiry = (datetime.now(timezone.utc) + timedelta(days=7)).strftime("%Y-%m-%d")
    instrument = SimpleNamespace(asset_id="NIFTY", primary=SimpleNamespace(raw={
        "desk_id": "GROWW_INDEX_OPTIONS", "stock_code": "NIFTY", "underlying": "NIFTY",
        "chain_source": "official_instrument_csv_plus_get_option_chain", "chain_candidates": [
            {"right": "Call", "TradingSymbol": "NIFTYCE", "trading_symbol": "NIFTYCE", "strike_price": 25000, "expiry_date": expiry, "runtime_lot_size": 25, "ltp": 100.0, "iv": 15.0},
            {"right": "Put", "TradingSymbol": "NIFTYPE", "trading_symbol": "NIFTYPE", "strike_price": 25000, "expiry_date": expiry, "runtime_lot_size": 25, "ltp": 100.0, "iv": 15.0},
        ]
    }))
    monkeypatch.setattr(chain.config, "GROWW_SESSION_BOOK_REQUIRE_TWO_SIDED_QUOTE", True, raising=False)
    monkeypatch.setattr(chain.config, "GROWW_OPTION_MIN_DTE", 1.0, raising=False)
    monkeypatch.setattr(chain.config, "GROWW_OPTION_MAX_DTE", 21.0, raising=False)
    monkeypatch.setattr(chain.config, "POLICY_OPTION_MAX_HOLD_SEC", 2700.0, raising=False)
    live = {
        "NIFTYCE": {"bid_price": 99.5, "offer_price": 100.0, "bid_quantity": 100, "offer_quantity": 100},
        "NIFTYPE": {"bid_price": 99.5, "offer_price": 100.0, "bid_quantity": 100, "offer_quantity": 100},
    }
    diagnostics = {}
    book = chain.build_session_contract_book(
        instrument, underlying_spot=25000.0, available_funds=25852.96,
        option_quote_by_symbol=live, commit=False, diagnostics=diagnostics,
    )
    assert book is not None
    assert book.call.raw["theta_carry_bps_expected_hold"] > 0
    assert book.put.raw["theta_carry_bps_expected_hold"] > 0
    assert diagnostics["call"]["carry_policy"] == "theta_charged_to_signal_edge_not_static_veto"


def test_selected_vehicle_activation_keeps_official_premium_history_when_live_stream_is_sparse(monkeypatch):
    import time
    from collections import deque
    from types import SimpleNamespace
    from exchanges.groww import data_manager as dm
    from exchanges.groww.data_manager import GrowwOptionDataManager

    instrument = SimpleNamespace(asset_id="NIFTY", primary=SimpleNamespace(raw={}))
    manager = GrowwOptionDataManager(instrument=instrument, api=SimpleNamespace())
    choice = SimpleNamespace(expiry="2026-06-02", right="call", strike=24100.0, selected_symbol="NIFTYCE")
    key = ("2026-06-02", "call", 24100.0)
    now = time.time()
    live_bar = {"t": 16, "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5}
    history = [
        {"t": i, "open": 95.0 + i, "high": 96.0 + i, "low": 94.0 + i, "close": 95.5 + i}
        for i in range(1, 16)
    ]

    monkeypatch.setattr(dm, "apply_contract_choice", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(dm.config, "GROWW_OPTION_PROTECTION_MIN_ATR_BARS", 10, raising=False)
    monkeypatch.setattr(manager, "_repair_option_stream_if_stale", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(manager, "_execution_price_fresh", lambda *_args, **_kwargs: True)

    def warmup(*, historical_only=False):
        assert historical_only is True
        manager._candles["1m"].clear()
        manager._candles["1m"].extend(history)

    monkeypatch.setattr(manager, "_warmup", warmup)
    manager._book_stream_state[key] = {
        "identity": {}, "last_price": 100.5, "last_stream_tick_ts": now,
        "last_quote_ts": now, "quote_tick_ts": now, "depth_tick_ts": now,
        "best_bid": 100.0, "best_ask": 100.5, "best_bid_qty": 65, "best_ask_qty": 65,
        "candles": {"1m": deque([live_bar], maxlen=600)},
    }

    assert manager._activate_session_vehicle(choice) is True
    rows = list(manager._candles["1m"])
    assert len(rows) == 16
    assert rows[0]["t"] == 1
    assert rows[-1]["t"] == 16


def test_live_books_without_current_model_pair_keep_nifty_analysis_live_for_rescan(monkeypatch):
    from types import SimpleNamespace
    from exchanges.groww import data_manager as dm
    from exchanges.groww.data_manager import GrowwOptionDataManager

    raw = {"stock_code": "NIFTY", "underlying": "NIFTY"}
    manager = GrowwOptionDataManager(
        instrument=SimpleNamespace(asset_id="NIFTY", primary=SimpleNamespace(raw=raw)),
        api=SimpleNamespace(),
    )
    monkeypatch.setattr(manager, "_is_chain_mode", lambda: True)
    monkeypatch.setattr(manager, "_hydrate_chain_candidates", lambda **_kw: True)
    monkeypatch.setattr(manager, "_stream_executable_shortlist", lambda *_args, **_kwargs: {
        "NIFTYCE": {"ltp": 100.0, "bid_price": 99.5, "offer_price": 100.0},
        "NIFTYPE": {"ltp": 101.0, "bid_price": 100.5, "offer_price": 101.0},
    })
    def no_pair(*_args, diagnostics=None, **_kwargs):
        if diagnostics is not None:
            diagnostics.update({"call": {"rejected": {"delta_outside_vehicle_band": 1}}, "put": {"rejected": {"delta_outside_vehicle_band": 1}}})
        return None
    monkeypatch.setattr(dm, "build_session_contract_book", no_pair)

    assert manager.prepare_session_contract_book(23950.0, 25852.96) is True
    assert raw["session_contract_book_status"] == "MONITORING_NO_POLICY_ELIGIBLE_PAIR"
    assert raw["session_contract_diagnostics"]["call"]["rejected"]["delta_outside_vehicle_band"] == 1


def test_long_option_valuation_diagnostic_is_signed_and_not_mislabeled_as_edge(monkeypatch):
    from datetime import datetime, timedelta, timezone
    from types import SimpleNamespace
    from agents import groww_chain_architect as chain

    expiry = (datetime.now(timezone.utc) + timedelta(days=6)).strftime("%Y-%m-%d")
    instrument = SimpleNamespace(
        asset_id="NIFTY",
        primary=SimpleNamespace(raw={
            "desk_id": "GROWW_INDEX_OPTIONS", "stock_code": "NIFTY", "underlying": "NIFTY",
            "chain_source": "official_instrument_csv_plus_get_option_chain",
            "chain_candidates": [
                {"right": "Call", "option_type": "CE", "TradingSymbol": "CE", "trading_symbol": "CE", "strike_price": 24000, "expiry_date": expiry, "runtime_lot_size": 25, "ltp": 100.0, "iv": 12.0, "open_interest": 1000, "volume": 1000},
                {"right": "Put", "option_type": "PE", "TradingSymbol": "PE", "trading_symbol": "PE", "strike_price": 23800, "expiry_date": expiry, "runtime_lot_size": 25, "ltp": 100.0, "iv": 12.0, "open_interest": 1000, "volume": 1000},
            ],
        })
    )
    quotes = {
        "CE": {"bid_price": 99.9, "offer_price": 100.1, "bid_quantity": 100, "offer_quantity": 100},
        "PE": {"bid_price": 99.9, "offer_price": 100.1, "bid_quantity": 100, "offer_quantity": 100},
    }
    monkeypatch.setattr(chain.config, "GROWW_SESSION_BOOK_DELTA_RESELECT_BAND", 1.0, raising=False)
    book = chain.build_session_contract_book(instrument, underlying_spot=23900.0, available_funds=100000.0, option_quote_by_symbol=quotes, commit=False)
    assert book is not None
    for raw in (book.call.raw, book.put.raw):
        signed = raw["bs_value_vs_premium_bps"]
        absolute = raw["bs_model_deviation_abs_bps"]
        assert absolute == pytest.approx(abs(signed))
        assert "bs_model_edge_bps" not in raw

# ── Preserved regression section: test_groww_migration.py ───────────────────────────────
import os
from types import SimpleNamespace

os.environ.setdefault("GROWW_ACCESS_TOKEN", "test-groww-token")
os.environ.setdefault("GROWW_ENABLED", "true")
os.environ.setdefault("GROWW_DISCOVERY_ENABLED", "true")
os.environ.setdefault("EXECUTION_EXCHANGE", "groww")

from core.instruments import AssetClass, ExchangeInstrument, ExchangeName, TradableInstrument
from core.types import Exchange
from exchanges.groww.api import GrowwRestClient
from execution.groww_long_option_execution import (
    GrowwLongOptionExecutionState,
    GrowwLongOptionExecutor,
    GrowwProtectionPlan,
    LongOptionCandidateScore,
)
from execution.instrument_registry import InstrumentRegistry
from execution.order_manager import OrderManager
from execution.router import ExecutionRouter


class FakeGrowwSDK:
    VALIDITY_DAY = "DAY"
    EXCHANGE_NSE = "NSE"
    SEGMENT_FNO = "FNO"
    PRODUCT_NRML = "NRML"
    ORDER_TYPE_LIMIT = "LIMIT"
    ORDER_TYPE_MARKET = "MARKET"
    ORDER_TYPE_STOP_LOSS = "SL"
    ORDER_TYPE_STOP_LOSS_MARKET = "SL_M"
    TRANSACTION_TYPE_BUY = "BUY"
    TRANSACTION_TYPE_SELL = "SELL"
    SMART_ORDER_TYPE_OCO = "OCO"
    SMART_ORDER_STATUS_ACTIVE = "ACTIVE"
    TRIGGER_DIRECTION_DOWN = "DOWN"
    TRIGGER_DIRECTION_UP = "UP"

    def __init__(self, *, fill_status="FILLED", filled_qty=50, fill_price=118.75, smart_ok=True):
        self.last_place_order = None
        self.last_smart_order = None
        self.place_orders = []
        self.cancelled_orders = []
        self.fill_status = fill_status
        self.filled_qty = filled_qty
        self.fill_price = fill_price
        self.smart_ok = smart_ok
        self._order_count = 0

    def place_order(self, **kwargs):
        self.last_place_order = kwargs
        self.place_orders.append(kwargs)
        self._order_count += 1
        return {"groww_order_id": f"GROWWORDER{self._order_count}", "order_status": "OPEN"}

    def get_order_detail(self, groww_order_id, segment="FNO", **kwargs):
        return {
            "groww_order_id": groww_order_id,
            "order_status": self.fill_status,
            "filled_quantity": self.filled_qty,
            "average_price": self.fill_price,
            "quantity": self.filled_qty,
            "segment": segment,
        }

    def get_trade_list_for_order(self, groww_order_id, segment="FNO", **kwargs):
        return {
            "trades": [
                {
                    "trade_price": self.fill_price,
                    "quantity": self.filled_qty,
                    "total_charges": "2.50",
                    "segment": segment,
                }
            ]
        }

    def cancel_order(self, groww_order_id, segment="FNO", **kwargs):
        self.cancelled_orders.append(groww_order_id)
        return {"groww_order_id": groww_order_id, "order_status": "CANCELLED"}

    def create_smart_order(self, **kwargs):
        self.last_smart_order = kwargs
        if not self.smart_ok:
            return {"status": "REJECTED", "error": "simulated_oco_failure"}
        return {"smart_order_id": "oco_12345", "status": "ACTIVE"}

    def get_smart_order(self, smart_order_id, **kwargs):
        if not self.smart_ok:
            return {"smart_order_id": smart_order_id, "status": "REJECTED"}
        return {
            "smart_order_id": smart_order_id,
            "status": "ACTIVE",
            "trading_symbol": "NIFTY26JUN25000CE",
            "quantity": self.filled_qty,
            "net_position_quantity": self.filled_qty,
            "transaction_type": "SELL",
        }


def _client(fake: FakeGrowwSDK | None = None) -> GrowwRestClient:
    api = GrowwRestClient(access_token="test")
    api._client = fake or FakeGrowwSDK()
    return api


def _groww_inst(*, lot_size: int = 50) -> TradableInstrument:
    contract = {
        "stock_code": "NIFTY",
        "exchange_code": "NFO",
        "exchange": "NSE",
        "segment": "FNO",
        "product_type": "Options",
        "right": "Call",
        "expiry_date": "2026-06-25",
        "strike_price": "25000",
        "TradingSymbol": "NIFTY26JUN25000CE",
        "trading_symbol": "NIFTY26JUN25000CE",
        "runtime_lot_size": lot_size,
        "LotSize": lot_size,
        "ltp": 120.0,
    }
    raw = {
        "stock_code": "NIFTY",
        "exchange_code": "NFO",
        "exchange": "NSE",
        "segment": "FNO",
        "selected_option_contract": {"raw": contract},
    }
    ei = ExchangeInstrument(
        exchange=ExchangeName.GROWW,
        symbol="NIFTY",
        ws_symbol="NIFTY",
        display_symbol="NIFTY",
        asset_id="NIFTY",
        asset_class=AssetClass.OPTION,
        quote_asset="INR",
        base_asset="NIFTY",
        contract_type="option_chain",
        status="active",
        tick_size=0.05,
        lot_step=1.0,
        min_qty=1.0,
        max_leverage=1.0,
        raw=raw,
    )
    return TradableInstrument(
        asset_id="NIFTY",
        display_name="NIFTY 50 index options",
        asset_class=AssetClass.OPTION,
        primary_exchange=ExchangeName.GROWW,
        by_exchange={ExchangeName.GROWW: ei},
    )


def test_groww_exchange_enum_and_registry_discovery():
    assert Exchange.from_str("groww") is Exchange.GROWW
    registry = InstrumentRegistry(execution_preference="groww")
    report = registry.discover(include_exchanges="groww", groww_api=SimpleNamespace(), require_primary=False)
    nifty = next(inst for inst in report.matched if inst.asset_id == "NIFTY")
    assert nifty.primary_exchange is ExchangeName.GROWW
    assert ExchangeName.GROWW in nifty.by_exchange
    assert {ex.value for ex in nifty.by_exchange} == {"groww"}


def test_groww_order_body_uses_official_sdk_fields():
    om = OrderManager(_client(), exchange_name="groww", instrument=_groww_inst())
    body = om._adapter._order_body("BUY", "LIMIT", 50, price=118.75)
    assert body["trading_symbol"] == "NIFTY26JUN25000CE"
    assert body["quantity"] == 50
    assert body["validity"] == "DAY"
    assert body["exchange"] == "NSE"
    assert body["segment"] == "FNO"
    assert body["product"] == "NRML"
    assert body["order_type"] == "LIMIT"
    assert body["transaction_type"] == "BUY"
    assert 8 <= len(body["order_reference_id"]) <= 20


def test_groww_rest_client_place_order_passes_official_payload():
    api = _client()
    resp = api.place_order(
        trading_symbol="NIFTY26JUN25000CE",
        quantity=50,
        validity="DAY",
        exchange="NSE",
        segment="FNO",
        product="NRML",
        order_type="LIMIT",
        transaction_type="BUY",
        price="118.75",
        order_reference_id="groww123",
    )
    assert resp["groww_order_id"] == "GROWWORDER1"
    assert api._client.last_place_order["segment"] == "FNO"
    assert api._client.last_place_order["trading_symbol"] == "NIFTY26JUN25000CE"


def test_groww_long_option_lifecycle_buys_then_arms_exit_oco_after_fill(monkeypatch):
    monkeypatch.setattr("execution.order_manager.config.GROWW_REQUIRE_STATIC_IP_FOR_LIVE_ORDERS", False, raising=False)
    monkeypatch.setattr("execution.order_manager.config.GROWW_REQUIRE_SEBI_ALGO_CONFIRMATION_FOR_LIVE_ORDERS", False, raising=False)
    api = _client()
    om = OrderManager(api, exchange_name="groww", instrument=_groww_inst())
    data = om.place_bracket_limit_entry("BUY", 50, limit_price=118.75, sl_price=95.0, tp_price=160.0, timeout_sec=0.0)
    payload = api._client.last_smart_order
    assert data["protection_model"] == "GROWW_OCO_AFTER_FILL"
    assert data["bracket_child_verified"] is True
    assert api._client.place_orders[0]["transaction_type"] == "BUY"
    assert api._client.place_orders[0]["order_type"] == "LIMIT"
    assert payload["smart_order_type"] == "OCO"
    assert payload["segment"] == "FNO"
    assert "order" not in payload
    assert payload["transaction_type"] == "SELL"
    assert payload["target"]["order_type"] == "LIMIT"
    assert payload["stop_loss"]["order_type"] == "SL"
    states = [row["state"] for row in data["_lifecycle"]["audit_trail"]]
    assert states == [
        "CANDIDATE_SELECTED",
        "ENTRY_SUBMITTED",
        "ENTRY_PARTIAL_OR_FILLED",
        "PROTECTION_SUBMITTED_FOR_FILLED_QTY",
        "PROTECTION_CONFIRMED",
        "ACTIVE_PROTECTED_POSITION",
    ]


def test_groww_lifecycle_partial_fill_protects_actual_qty_and_cancels_remainder(monkeypatch):
    monkeypatch.setattr("execution.order_manager.config.GROWW_REQUIRE_STATIC_IP_FOR_LIVE_ORDERS", False, raising=False)
    monkeypatch.setattr("execution.order_manager.config.GROWW_REQUIRE_SEBI_ALGO_CONFIRMATION_FOR_LIVE_ORDERS", False, raising=False)
    fake = FakeGrowwSDK(fill_status="PARTIALLY_FILLED", filled_qty=50, fill_price=118.50)
    api = _client(fake)
    om = OrderManager(api, exchange_name="groww", instrument=_groww_inst())
    result = om.execute_groww_long_option_with_protection(
        "BUY", 100, limit_price=118.75, sl_price=95.0, tp_price=160.0, timeout_sec=0.0
    )
    assert result.approved is True
    assert result.filled_quantity == 50
    assert "GROWWORDER1" in fake.cancelled_orders
    assert fake.last_smart_order["quantity"] == 50
    assert fake.last_smart_order["net_position_quantity"] == 50
    assert "PARTIAL_FILL_REMAINDER_CANCELLED" in result.reasons


def test_groww_lifecycle_uses_trade_vwap_for_actual_fill_price(monkeypatch):
    monkeypatch.setattr("execution.order_manager.config.GROWW_REQUIRE_STATIC_IP_FOR_LIVE_ORDERS", False, raising=False)
    monkeypatch.setattr("execution.order_manager.config.GROWW_REQUIRE_SEBI_ALGO_CONFIRMATION_FOR_LIVE_ORDERS", False, raising=False)

    class SplitFillSDK(FakeGrowwSDK):
        def get_order_detail(self, groww_order_id, segment="FNO", **kwargs):
            row = super().get_order_detail(groww_order_id, segment=segment, **kwargs)
            row["average_price"] = 0
            return row

        def get_trade_list_for_order(self, groww_order_id, segment="FNO", **kwargs):
            return {
                "trade_list": [
                    {"price": 118.0, "quantity": 25, "trade_status": "EXECUTED", "segment": segment},
                    {"price": 120.0, "quantity": 25, "trade_status": "EXECUTED", "segment": segment},
                ]
            }

    fake = SplitFillSDK(fill_status="FILLED", filled_qty=50, fill_price=0)
    api = _client(fake)
    om = OrderManager(api, exchange_name="groww", instrument=_groww_inst())
    result = om.execute_groww_long_option_with_protection(
        "BUY", 50, limit_price=118.75, sl_price=95.0, tp_price=160.0, timeout_sec=0.0
    )
    assert result.approved is True
    assert result.average_fill_price == 119.0
    assert fake.last_smart_order["quantity"] == 50


def test_groww_lifecycle_oco_failure_blocks_new_entries_and_emergency_exits(monkeypatch):
    monkeypatch.setattr("execution.order_manager.config.GROWW_REQUIRE_STATIC_IP_FOR_LIVE_ORDERS", False, raising=False)
    monkeypatch.setattr("execution.order_manager.config.GROWW_REQUIRE_SEBI_ALGO_CONFIRMATION_FOR_LIVE_ORDERS", False, raising=False)
    fake = FakeGrowwSDK(fill_status="FILLED", filled_qty=50, fill_price=118.75, smart_ok=False)
    api = _client(fake)
    om = OrderManager(api, exchange_name="groww", instrument=_groww_inst())
    result = om.execute_groww_long_option_with_protection(
        "BUY", 50, limit_price=118.75, sl_price=95.0, tp_price=160.0, timeout_sec=0.0
    )
    assert result.state is GrowwLongOptionExecutionState.UNPROTECTED_POSITION_EMERGENCY
    assert result.approved is False
    assert result.blocked_new_entries is True
    assert result.emergency_order_id == "GROWWORDER2"
    assert fake.place_orders[1]["transaction_type"] == "SELL"
    assert fake.place_orders[1]["order_type"] == "MARKET"


def _candidate() -> LongOptionCandidateScore:
    return LongOptionCandidateScore(
        trading_symbol="NIFTY26JUN25000CE",
        option_type="CE",
        expiry="2026-06-25",
        strike=25000,
        premium=118.75,
        spread_bps=20,
        delta=0.45,
        gamma=0.02,
        theta=-1.2,
        vega=4.0,
        iv=0.18,
        expected_premium_return_after_cost=0.18,
        probability_tp_before_sl=0.58,
        theta_cost_for_expected_hold=1.5,
        liquidity_score=0.82,
        protection_feasible=True,
        total_score=0.74,
        lot_size=50,
    )


def test_groww_static_ip_fail_closed_for_live_orders():
    fake = FakeGrowwSDK()
    api = _client(fake)
    executor = GrowwLongOptionExecutor(api, static_ip_validator=lambda: {"approved": False, "reason": "no_static_nat"})
    result = executor.execute(
        candidate=_candidate(),
        quantity=50,
        limit_price=118.75,
        protection=GrowwProtectionPlan(target_price=160.0, stop_trigger_price=95.0, stop_limit_price=94.95),
        fill_timeout_sec=0.0,
        require_static_ip=True,
    )
    assert result.state is GrowwLongOptionExecutionState.REJECTED
    assert result.blocked_new_entries is True
    assert fake.place_orders == []


def test_execution_router_accepts_groww_manager():
    om = OrderManager(_client(), exchange_name="groww", instrument=_groww_inst())
    router = ExecutionRouter(coinswitch_om=None, delta_om=None, groww_om=om, default="groww")
    assert router.active_exchange == "groww"
    assert router.active is om


def test_groww_live_execution_blocks_until_algo_registration_is_confirmed(monkeypatch):
    fake = FakeGrowwSDK()
    api = _client(fake)
    monkeypatch.setattr("execution.groww_long_option_execution.config.GROWW_SEBI_ALGO_REGISTRATION_CONFIRMED", False, raising=False)
    executor = GrowwLongOptionExecutor(api, static_ip_validator=lambda: {"approved": True})
    result = executor.execute(
        candidate=_candidate(), quantity=50, limit_price=118.75,
        protection=GrowwProtectionPlan(target_price=160.0, stop_trigger_price=95.0, stop_limit_price=94.95),
        fill_timeout_sec=0.0, require_static_ip=True, require_algo_confirmation=True,
    )
    assert result.state is GrowwLongOptionExecutionState.REJECTED
    assert result.blocked_new_entries is True
    assert "GROWW_ALGO_REGISTRATION_UNCONFIRMED" in result.reasons[0]
    assert fake.place_orders == []

# ── Preserved regression section: test_hyperliquid_venue_selection.py ───────────────────────────────
import time

from core.instruments import AssetClass, ExchangeName
from execution.instrument_registry import InstrumentRegistry
from execution.venue_selection import select_execution_venue
from market_data.feed_health import score_feed_health
from market_data.normalizer import InstrumentMapping, build_venue_microstate


def _state__test_hyperliquid_venue_selection(venue, symbol, mid, qty, *, execution_enabled=True):
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
        "delta": _state__test_hyperliquid_venue_selection("delta", "SLVONUSD", 30.00, 20.0),
        "hyperliquid": _state__test_hyperliquid_venue_selection("hyperliquid", "xyz:SILVER", 30.00, 5000.0),
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
        "delta": _state__test_hyperliquid_venue_selection("delta", "BTCUSD", 75800.0, 1000.0),
        "hyperliquid": _state__test_hyperliquid_venue_selection("hyperliquid", "BTC", 75800.0, 1000.0),
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
    state = _state__test_hyperliquid_venue_selection("coinswitch", "BTCUSDT", 75800.0, 1000.0)
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
    state = _state__test_hyperliquid_venue_selection("coinswitch", "BTCUSDT", 75800.0, 1000.0)
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
        "delta": _state__test_hyperliquid_venue_selection("delta", "BTCUSD", 75800.0, 1000.0),
        "hyperliquid": _state__test_hyperliquid_venue_selection("hyperliquid", "BTC", 75800.0, 1000.0),
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
        "delta": _state__test_hyperliquid_venue_selection("delta", "BTCUSD", 75800.0, 100000.0),
        "hyperliquid": _state__test_hyperliquid_venue_selection("hyperliquid", "BTC", 75800.0, 100000.0),
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

# ── Preserved regression section: test_institutional_market_state.py ───────────────────────────────
from datetime import datetime, timezone
from pathlib import Path

import pytest

from intelligence.cross_venue_btc import build_btc_composite_state
from market_data.feed_health import score_feed_health
from market_data.microstructure import LatencyBaseline, MicrostructureTracker, top_of_book_ofi_usd
from market_data.normalizer import (
    InstrumentMapping,
    aggregate_depth_usd_by_band,
    build_venue_microstate,
    order_book_imbalance_by_band,
    usd_notional_depth,
)
from research.store import (
    DeltaForwardLabel,
    JsonlResearchStore,
    ResearchDecisionRecord,
    ResearchExecutionRecord,
    ForwardLabelWriter,
)


def _linear_mapping(venue: str, symbol: str, execution: bool) -> InstrumentMapping:
    return InstrumentMapping(
        venue=venue,
        venue_symbol=symbol,
        canonical_underlying="BTC",
        product_class="perp",
        quote_currency="USD",
        contract_multiplier=1.0,
        settlement_currency="USDT" if venue != "delta" else "USD",
        price_tick=0.5,
        qty_step=0.001,
        execution_enabled=execution,
        notional_model="linear",
    )


def _inverse_delta_mapping() -> InstrumentMapping:
    return InstrumentMapping(
        venue="delta",
        venue_symbol="BTCUSD",
        canonical_underlying="BTC",
        product_class="inverse_perp",
        quote_currency="USD",
        contract_multiplier=1.0,
        settlement_currency="BTC",
        price_tick=0.5,
        qty_step=1.0,
        execution_enabled=True,
        notional_model="inverse_usd_contract",
    )


def test_feed_health_hard_zero_and_latency_penalty_without_universal_staleness_rule():
    disconnected = score_feed_health(
        connected=False,
        heartbeat_ok=True,
        sequence_valid=True,
        snapshot_ready=True,
        exchange_timestamp_available=True,
        latency_vs_baseline_z=0.0,
    )
    assert disconnected.quality_score == 0.0

    unchanged_book = score_feed_health(
        connected=True,
        heartbeat_ok=True,
        sequence_valid=True,
        snapshot_ready=True,
        exchange_timestamp_available=True,
        latency_vs_baseline_z=0.0,
        no_change_heartbeat_valid=True,
    )
    assert unchanged_book.quality_score == 1.0
    assert unchanged_book.usable_for_decision is True

    high_latency = score_feed_health(
        connected=True,
        heartbeat_ok=True,
        sequence_valid=True,
        snapshot_ready=True,
        exchange_timestamp_available=True,
        latency_vs_baseline_z=5.0,
    )
    assert 0.0 < high_latency.quality_score < 1.0


def test_usd_depth_normalisation_respects_linear_vs_inverse_contracts():
    linear = _linear_mapping("coinswitch", "BTCUSDT", execution=False)
    inverse = _inverse_delta_mapping()

    assert usd_notional_depth(displayed_size=2.0, level_price=100000.0, mapping=linear) == 200000.0
    assert usd_notional_depth(displayed_size=2000.0, level_price=100000.0, mapping=inverse) == 2000.0


def test_depth_bands_are_distance_based_and_obi_is_band_specific():
    mapping = _linear_mapping("hyperliquid", "BTC", execution=False)
    bids = [(9999.5, 1.0), (9997.5, 2.0), (9988.0, 3.0)]
    asks = [(10000.5, 1.5), (10002.5, 1.0), (10012.0, 4.0)]
    bid_depth = aggregate_depth_usd_by_band(levels=bids, side="bid", mid=10000.0, mapping=mapping)
    ask_depth = aggregate_depth_usd_by_band(levels=asks, side="ask", mid=10000.0, mapping=mapping)
    obi = order_book_imbalance_by_band(bid_depth, ask_depth)

    assert bid_depth["0-1"] == 9999.5
    assert ask_depth["0-1"] == 10000.5 * 1.5
    assert bid_depth["1-3"] == 9997.5 * 2.0
    assert ask_depth["1-3"] == 10002.5
    assert obi["0-1"] < 0
    assert obi["1-3"] > 0


def test_btc_composite_excludes_invalid_reference_feed_and_preserves_venue_state():
    healthy = score_feed_health(
        connected=True,
        heartbeat_ok=True,
        sequence_valid=True,
        snapshot_ready=True,
        exchange_timestamp_available=True,
        latency_vs_baseline_z=0.0,
    )
    invalid = score_feed_health(
        connected=True,
        heartbeat_ok=True,
        sequence_valid=False,
        snapshot_ready=True,
        exchange_timestamp_available=True,
        latency_vs_baseline_z=0.0,
    )
    delta = build_venue_microstate(
        mapping=_inverse_delta_mapping(),
        bids=[(99999.5, 1000), (99998.0, 1000)],
        asks=[(100000.5, 1000), (100002.0, 1000)],
        feed_health=healthy,
        receive_ts_ns=1_000_000_000,
        exchange_ts_ns=999_000_000,
        ofi_usd_1s=25000,
        tfi_usd_1s=5000,
    )
    coinswitch = build_venue_microstate(
        mapping=_linear_mapping("coinswitch", "BTCUSDT", execution=False),
        bids=[(100009.0, 1.0), (100000.0, 2.0)],
        asks=[(100011.0, 1.0), (100020.0, 2.0)],
        feed_health=healthy,
        receive_ts_ns=1_000_000_000,
        exchange_ts_ns=999_500_000,
        ofi_usd_1s=10000,
        tfi_usd_1s=1000,
    )
    hyperliquid = build_venue_microstate(
        mapping=_linear_mapping("hyperliquid", "BTC", execution=False),
        bids=[(100100.0, 1.0)],
        asks=[(100102.0, 1.0)],
        feed_health=invalid,
        receive_ts_ns=1_000_000_000,
        exchange_ts_ns=999_500_000,
    )

    state = build_btc_composite_state(
        delta_state=delta,
        reference_states={"coinswitch": coinswitch, "hyperliquid": hyperliquid},
    )

    assert set(state.reference_states) == {"coinswitch"}
    assert state.excluded_reference_venues == {"hyperliquid": "feed_quality_zero_or_invalid_sequence"}
    assert state.composite_reference_mid == coinswitch.mid
    assert state.delta_dislocation_bps is not None and state.delta_dislocation_bps < 0
    assert state.flow_agreement_score == 1.0
    assert state.candidate_leader_venue == "coinswitch"
    assert 0.0 < state.delta_execution_quality_score <= 1.0


def test_research_store_records_rejections_executions_and_forward_labels(tmp_path: Path):
    store = JsonlResearchStore(tmp_path / "research")
    decision = ResearchDecisionRecord(
        observation_ts_ns=123,
        desk="DESK_A_BTC",
        venue="delta",
        instrument="BTCUSD",
        candidate_id="btc-123-long",
        decision="NO_TRADE_EXECUTION_UNSAFE",
        model_values={"net_edge_bps": 4.2, "delta_execution_quality": 0.0},
        reasons=["delta_feed_quality_zero"],
        features={"delta": {"spread_bps": 2.0}},
        model_version="deterministic-baseline-v1",
        policy_version="institutional-flow-v1",
    )
    execution = ResearchExecutionRecord(
        observation_ts_ns=124,
        desk="DESK_B_NIFTY_OPTIONS",
        venue="groww",
        instrument="NIFTY26JUN25000CE",
        candidate_id="nifty-124-ce",
        requested_order={"side": "BUY", "qty": 50},
        actual_fill={"qty": 50, "avg_price": 118.75},
        protection_state={"oco_status": "ACTIVE"},
        realised_costs={"fees": 2.5, "spread_bps": 20.0},
    )
    label = DeltaForwardLabel(
        observation_ts_ns=123,
        horizon="10s",
        side="long",
        gross_markout_bps=3.1,
        spread_cost_bps=1.0,
        fee_cost_bps=0.5,
        slippage_estimate_bps=0.7,
        net_executable_return_bps=0.9,
        favorable_excursion_bps=5.0,
        adverse_excursion_bps=1.2,
    )

    store.append_decision(decision)
    store.append_execution(execution)
    store.append_delta_forward_label(label)

    decisions = store.read_records("decisions.jsonl")
    executions = store.read_records("executions.jsonl")
    labels = store.read_records("delta_forward_labels.jsonl")

    assert decisions[0]["decision"] == "NO_TRADE_EXECUTION_UNSAFE"
    assert executions[0]["protection_state"]["oco_status"] == "ACTIVE"
    assert labels[0]["net_executable_return_bps"] == 0.9



def test_microstructure_tracker_computes_usd_ofi_and_tfi_with_correct_signs():
    mapping = _linear_mapping("coinswitch", "BTCUSDT", execution=False)
    raw_ofi = top_of_book_ofi_usd(
        previous_bids=[(100.0, 2.0)], previous_asks=[(101.0, 2.0)],
        current_bids=[(100.0, 3.0)], current_asks=[(101.0, 1.0)], mapping=mapping,
    )
    assert raw_ofi == 201.0  # $100 added to bid queue and $101 removed from ask queue.
    tracker = MicrostructureTracker(mapping)
    tracker.update_book([(100.0, 2.0)], [(101.0, 2.0)], 1.0)
    tracker.update_book([(100.0, 3.0)], [(101.0, 1.0)], 2.0)
    tracker.record_trade(price=101.0, quantity=2.0, buyer_aggressor=True, timestamp_s=2.0)
    snapshot = tracker.snapshot(2.0)
    assert snapshot.ofi_usd_1s > 0
    assert snapshot.tfi_usd_1s == 202.0


def test_latency_baseline_is_relative_to_venue_observations_not_hardcoded_ms():
    tracker = LatencyBaseline(window=50, warmup=5)
    for sample in (10.0, 11.0, 10.5, 9.5, 10.0, 10.2, 9.8, 10.1, 10.3, 9.7):
        assert tracker.observe(sample) is None
    z = tracker.observe(30.0)
    assert z is not None and z > 5.0


def test_forward_label_writer_records_only_elapsed_observations(tmp_path: Path):
    store = JsonlResearchStore(tmp_path / "labels")
    writer = ForwardLabelWriter(store, horizons_s=(1, 10))
    writer.record_fill(
        fill_ts_ns=1_000_000_000, side="BUY", candidate_id="btc1", fill_price=100.0,
        spread_cost_bps=1.0, fee_cost_bps=1.0, slippage_estimate_bps=0.5,
    )
    assert writer.observe(now_ts_ns=1_500_000_000, current_price=101.0) == []
    writer.observe(now_ts_ns=2_000_000_000, current_price=101.0)
    labels = store.read_records("delta_forward_labels.jsonl")
    assert len(labels) == 1 and labels[0]["horizon"] == "1s"
    assert labels[0]["gross_markout_bps"] == pytest.approx(100.0)
    assert labels[0]["net_executable_return_bps"] == pytest.approx(97.5)

# ── Preserved regression section: test_institutional_strategy_runtime.py ───────────────────────────────
import time
from types import SimpleNamespace

from core.instruments import AssetClass, ExchangeInstrument, ExchangeName, TradableInstrument
from market_data.feed_health import score_feed_health
from market_data.normalizer import InstrumentMapping, build_venue_microstate
from strategy.domain import DecisionOutput
from strategy.institutional_strategy import InstitutionalStrategy, PositionPhase


class _Data__test_institutional_strategy_runtime:
    def __init__(self, prices, *, feed_ok=True, flow=2400.0):
        self.prices = list(prices)
        self.i = 0
        self.feed_ok = feed_ok
        self.flow = flow

    def get_last_price(self):
        px = self.prices[min(self.i, len(self.prices) - 1)]
        self.i += 1
        return px

    def _price(self):
        return self.prices[min(max(self.i - 1, 0), len(self.prices) - 1)]

    def get_orderbook(self):
        px = self._price()
        return {
            "bids": [(px - 0.005, 2000.0), (px - 0.02, 3000.0)],
            "asks": [(px + 0.005, 2000.0), (px + 0.02, 3000.0)],
        }

    def get_feed_reliability(self):
        return {
            "connected": self.feed_ok,
            "heartbeat_ok": self.feed_ok,
            "sequence_valid": self.feed_ok,
            "snapshot_ready": self.feed_ok,
            "exchange_timestamp_available": True,
            "latency_vs_baseline_z": 0.0,
        }

    def get_venue_microstates(self):
        px = self._price()
        health = score_feed_health(
            connected=self.feed_ok,
            heartbeat_ok=self.feed_ok,
            sequence_valid=self.feed_ok,
            snapshot_ready=self.feed_ok,
            exchange_timestamp_available=True,
            latency_vs_baseline_z=0.0,
        )
        ts = time.time_ns()
        delta = build_venue_microstate(
            mapping=InstrumentMapping(
                venue="delta", venue_symbol="BTCUSD", canonical_underlying="BTC", product_class="inverse_perp",
                quote_currency="USD", contract_multiplier=1.0, settlement_currency="BTC", price_tick=0.005,
                qty_step=1.0, execution_enabled=True, notional_model="inverse_usd_contract",
            ),
            bids=[(px - 0.005, 2000.0), (px - 0.02, 3000.0)],
            asks=[(px + 0.005, 2000.0), (px + 0.02, 3000.0)],
            feed_health=health, receive_ts_ns=ts, exchange_ts_ns=ts - 1_000_000,
            ofi_usd_1s=self.flow, ofi_usd_10s=self.flow * 0.5, tfi_usd_1s=self.flow * 0.25,
        )
        reference = build_venue_microstate(
            mapping=InstrumentMapping(
                venue="coinswitch", venue_symbol="BTCUSDT", canonical_underlying="BTC", product_class="perp",
                quote_currency="USD", contract_multiplier=1.0, settlement_currency="USDT", price_tick=0.005,
                qty_step=0.001, execution_enabled=False, notional_model="linear",
            ),
            bids=[(px - 0.005, 20.0), (px - 0.02, 20.0)],
            asks=[(px + 0.005, 20.0), (px + 0.02, 20.0)],
            feed_health=health, receive_ts_ns=ts, exchange_ts_ns=ts - 1_000_000,
            ofi_usd_1s=self.flow, ofi_usd_10s=self.flow * 0.5, tfi_usd_1s=self.flow * 0.25,
        )
        return {"delta": delta, "coinswitch": reference}


class _Risk__test_institutional_strategy_runtime:
    def get_available_balance(self):
        return {"available": 100000.0}


class _Orders:
    active_exchange = "delta"
    symbol = "BTCUSD"
    display_symbol = "BTCUSD"

    def __init__(self):
        self.placed = []

    def place_bracket_limit_entry(self, *args, **kwargs):
        self.placed.append((args, kwargs))
        side, qty, entry, stop, target = args[:5]
        return {
            "order_id": "entry-1", "quantity": qty, "fill_price": entry,
            "bracket_sl_order_id": "sl-1", "bracket_tp_order_id": "tp-1",
            "bracket_child_verified": True, "protection_model": "VENUE_NATIVE_BRACKET",
        }

    def get_open_position(self):
        return {"size": 1.0}

    def get_balance(self):
        return {"available": 100000.0}


def _instrument():
    ei = ExchangeInstrument(
        exchange=ExchangeName.DELTA, symbol="BTCUSD", ws_symbol="BTCUSD", display_symbol="BTCUSD", asset_id="BTC",
        asset_class=AssetClass.CRYPTO, quote_asset="USD", status="active", tick_size=0.5, lot_step=1.0,
        min_qty=1.0, raw={"contract_type": "inverse_perp", "contract_multiplier": 1.0},
    )
    return TradableInstrument("BTC", "Bitcoin", AssetClass.CRYPTO, ExchangeName.DELTA, {ExchangeName.DELTA: ei})


def _hyperliquid_btc_instrument():
    ei = ExchangeInstrument(
        exchange=ExchangeName.HYPERLIQUID, symbol="BTC", ws_symbol="BTC", display_symbol="BTC",
        asset_id="BTC", asset_class=AssetClass.CRYPTO, quote_asset="USD", base_asset="BTC",
        status="active", tick_size=1.0, lot_step=0.00001, min_qty=0.00001, max_leverage=50.0,
        raw={"contract_type": "linear_perp", "tick_size": 1.0, "qty_step": 0.00001, "min_qty": 0.00001},
    )
    return TradableInstrument("BTC", "Bitcoin", AssetClass.CRYPTO, ExchangeName.HYPERLIQUID, {ExchangeName.HYPERLIQUID: ei})


def _silver_instrument():
    delta = ExchangeInstrument(
        exchange=ExchangeName.DELTA, symbol="SLVONUSD", ws_symbol="SLVONUSD", display_symbol="SLVONUSD",
        asset_id="SILVER", asset_class=AssetClass.COMMODITY, quote_asset="USD", base_asset="SLVON",
        status="active", tick_size=0.01, lot_step=0.01, min_qty=0.01,
        raw={"contract_type": "linear_perp", "contract_value": 1.0, "tick_size": 0.01},
    )
    hl = ExchangeInstrument(
        exchange=ExchangeName.HYPERLIQUID, symbol="xyz:SILVER", ws_symbol="xyz:SILVER", display_symbol="xyz:SILVER",
        asset_id="SILVER", asset_class=AssetClass.COMMODITY, quote_asset="USD", base_asset="SILVER",
        status="active", tick_size=0.01, lot_step=0.01, min_qty=0.01, max_leverage=25.0,
        raw={"contract_type": "linear_perp", "tick_size": 0.01, "qty_step": 0.01},
    )
    return TradableInstrument("SILVER", "Silver token derivatives", AssetClass.COMMODITY, ExchangeName.DELTA, {ExchangeName.DELTA: delta, ExchangeName.HYPERLIQUID: hl})


class _SilverRouteData:
    def get_last_price(self):
        return 30.0

    def get_feed_reliability(self):
        return {
            "connected": True,
            "heartbeat_ok": True,
            "sequence_valid": True,
            "snapshot_ready": True,
            "exchange_timestamp_available": True,
            "latency_vs_baseline_z": 0.0,
        }

    def get_orderbook(self):
        return {"bids": [(29.995, 10.0)], "asks": [(30.005, 10.0)]}

    def get_venue_microstates(self):
        health = score_feed_health(
            connected=True,
            heartbeat_ok=True,
            sequence_valid=True,
            snapshot_ready=True,
            exchange_timestamp_available=True,
            latency_vs_baseline_z=0.0,
        )
        ts = time.time_ns()
        mapping_delta = InstrumentMapping(
            venue="delta", venue_symbol="SLVONUSD", canonical_underlying="SILVER", product_class="linear_perp",
            quote_currency="USD", contract_multiplier=1.0, settlement_currency="USD", price_tick=0.01,
            qty_step=0.01, execution_enabled=True, notional_model="linear",
        )
        mapping_hl = InstrumentMapping(
            venue="hyperliquid", venue_symbol="xyz:SILVER", canonical_underlying="SILVER", product_class="linear_perp",
            quote_currency="USD", contract_multiplier=1.0, settlement_currency="USDC", price_tick=0.01,
            qty_step=0.01, execution_enabled=True, notional_model="linear",
        )
        delta = build_venue_microstate(
            mapping=mapping_delta,
            bids=[(29.995, 25.0), (29.985, 25.0)],
            asks=[(30.005, 25.0), (30.015, 25.0)],
            feed_health=health,
            receive_ts_ns=ts,
            exchange_ts_ns=ts - 1_000_000,
            ofi_usd_1s=3000.0,
            ofi_usd_10s=1500.0,
            tfi_usd_1s=500.0,
        )
        hyperliquid = build_venue_microstate(
            mapping=mapping_hl,
            bids=[(29.995, 6000.0), (29.985, 6000.0)],
            asks=[(30.005, 6000.0), (30.015, 6000.0)],
            feed_health=health,
            receive_ts_ns=ts,
            exchange_ts_ns=ts - 1_000_000,
            ofi_usd_1s=-300000.0,
            ofi_usd_10s=-150000.0,
            tfi_usd_1s=-50000.0,
        )
        return {"delta": delta, "hyperliquid": hyperliquid}


class _MultiVenueOrders(_Orders):
    def available_exchanges(self):
        return {"delta", "hyperliquid"}

    def manager_for(self, venue):
        return self


def test_invalid_feed_produces_explicit_execution_unsafe_rejection(tmp_path, monkeypatch):
    monkeypatch.setattr("strategy.institutional_strategy._cfg", lambda name, default: str(tmp_path) if name == "RESEARCH_STORE_PATH" else default)
    strategy = InstitutionalStrategy(instrument=_instrument())
    decision = strategy.evaluate(_Data__test_institutional_strategy_runtime([100.0] * 40, feed_ok=False), _Orders(), _Risk__test_institutional_strategy_runtime(), 1)
    assert decision.decision is DecisionOutput.NO_TRADE_EXECUTION_UNSAFE
    assert decision.reasons == ["disconnected"]


def test_shadow_mode_blocks_live_order_even_when_flow_edge_is_positive(tmp_path, monkeypatch):
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_SIGNAL_DECAY_READY", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_FOR_DELTA", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_FOR_DELTA", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_VENUES", (), raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_VENUES", (), raising=False)
    def cfg(name, default):
        values = {"RESEARCH_STORE_PATH": str(tmp_path), "INSTITUTIONAL_ENABLE_LIVE_ENTRIES": False,
                  "INSTITUTIONAL_MIN_NET_EDGE_BPS": 0.1, "INSTITUTIONAL_MIN_SIGNAL_BPS": 0.01,
                      "VENUE_SELECTION_ENABLED": False, "INSTITUTIONAL_PARENT_THESIS_EXECUTION_MODEL_ENABLED": False}
        return values.get(name, default)
    monkeypatch.setattr("strategy.institutional_strategy._cfg", cfg)
    strategy = InstitutionalStrategy(instrument=_instrument())
    data = _Data__test_institutional_strategy_runtime([100.0 + i * 0.02 for i in range(80)], feed_ok=True)
    orders = _Orders()
    for i in range(80):
        strategy.on_tick(data, orders, _Risk__test_institutional_strategy_runtime(), i)
    assert strategy._last_decision is not None
    assert strategy._last_decision.decision is DecisionOutput.SHADOW_SIGNAL_VALIDATED
    assert "shadow_mode_live_entries_disabled" in strategy._last_decision.reasons
    assert orders.placed == []


def test_price_walk_without_venue_local_structural_state_is_not_a_trade_signal(tmp_path, monkeypatch):
    def cfg(name, default):
        values = {"RESEARCH_STORE_PATH": str(tmp_path), "INSTITUTIONAL_ENABLE_LIVE_ENTRIES": True,
                  "INSTITUTIONAL_MIN_NET_EDGE_BPS": 0.0, "INSTITUTIONAL_MIN_SIGNAL_BPS": 0.50}
        return values.get(name, default)
    monkeypatch.setattr("strategy.institutional_strategy._cfg", cfg)
    strategy = InstitutionalStrategy(instrument=_instrument())
    decision = None
    data = _Data__test_institutional_strategy_runtime([100.0 + i * 0.25 for i in range(80)], flow=0.0)
    for i in range(80):
        decision = strategy.evaluate(data, _Orders(), _Risk__test_institutional_strategy_runtime(), i)
    assert decision is not None
    assert decision.direction.value == "NO_TRADE"
    assert any(reason.startswith(("parent_structural_thesis_not_established", "market_state_and_flow_flat", "venue_flow_disagreement")) for reason in decision.reasons)


def test_live_entry_requires_protection_confirmation(tmp_path, monkeypatch):
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_SIGNAL_DECAY_READY", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_FOR_DELTA", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_FOR_DELTA", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_VENUES", (), raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_VENUES", (), raising=False)
    def cfg(name, default):
        values = {"RESEARCH_STORE_PATH": str(tmp_path), "INSTITUTIONAL_ENABLE_LIVE_ENTRIES": True,
                  "INSTITUTIONAL_MIN_NET_EDGE_BPS": 0.1, "INSTITUTIONAL_MIN_SIGNAL_BPS": 0.01,
                  "LEVERAGE": 2.0, "VENUE_SELECTION_ENABLED": False,
                  "INSTITUTIONAL_PARENT_THESIS_EXECUTION_MODEL_ENABLED": False}
        return values.get(name, default)
    monkeypatch.setattr("strategy.institutional_strategy._cfg", cfg)
    strategy = InstitutionalStrategy(instrument=_instrument())
    data = _Data__test_institutional_strategy_runtime([100.0 + i * 0.02 for i in range(80)], feed_ok=True)
    orders = _Orders()
    for i in range(80):
        strategy.on_tick(data, orders, _Risk__test_institutional_strategy_runtime(), i)
        if strategy.get_position():
            break
    pos = strategy.get_position()
    assert pos is not None
    assert pos["phase"] == PositionPhase.ACTIVE.value
    assert pos["protection_confirmed"] is True
    assert orders.placed


def test_live_entry_is_blocked_by_risk_manager_trade_gate(tmp_path, monkeypatch):
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_SIGNAL_DECAY_READY", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_FOR_DELTA", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_FOR_DELTA", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_VENUES", (), raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_VENUES", (), raising=False)

    def cfg(name, default):
        values = {
            "RESEARCH_STORE_PATH": str(tmp_path),
            "INSTITUTIONAL_ENABLE_LIVE_ENTRIES": True,
            "INSTITUTIONAL_MIN_NET_EDGE_BPS": 0.1,
            "INSTITUTIONAL_MIN_SIGNAL_BPS": 0.01,
            "LEVERAGE": 2.0,
            "VENUE_SELECTION_ENABLED": False,
            "INSTITUTIONAL_PARENT_THESIS_EXECUTION_MODEL_ENABLED": False,
        }
        return values.get(name, default)

    class _BlockedRisk(_Risk__test_institutional_strategy_runtime):
        def can_trade(self):
            return False, "Cooldown: 120s remaining"

    monkeypatch.setattr("strategy.institutional_strategy._cfg", cfg)
    strategy = InstitutionalStrategy(instrument=_instrument())
    data = _Data__test_institutional_strategy_runtime([100.0 + i * 0.02 for i in range(80)], feed_ok=True)
    orders = _Orders()
    decision = strategy.evaluate(data, orders, _BlockedRisk(), 1)
    assert decision.decision is DecisionOutput.NO_TRADE_RISK_BUDGET
    assert decision.reasons == ["risk_manager_gate:Cooldown: 120s remaining"]
    assert orders.placed == []


def test_unvalidated_opposite_venue_cannot_receive_silver_execution_route(tmp_path, monkeypatch):
    monkeypatch.setattr("strategy.institutional_strategy._cfg", lambda name, default: {
        "RESEARCH_STORE_PATH": str(tmp_path),
        "INSTITUTIONAL_ENABLE_LIVE_ENTRIES": False,
        "INSTITUTIONAL_MIN_SIGNAL_BPS": 0.01,
        "INSTITUTIONAL_MIN_NET_EDGE_BPS": 0.0,
    }.get(name, default))
    monkeypatch.setattr("execution.venue_selection._cfg", lambda name, default: {
        "VENUE_FEE_BPS": {"delta": 1.5, "hyperliquid": 4.5},
        "SILVER_HYPERLIQUID_PREFERENCE_BPS": 8.0,
        "SILVER_DELTA_ILLIQUIDITY_PENALTY_BPS": 15.0,
        "SILVER_DELTA_MIN_NEAR_DEPTH_USD": 50000.0,
        "VENUE_SELECTION_MIN_IMPROVEMENT_BPS": 0.0,
    }.get(name, default))
    strategy = InstitutionalStrategy(instrument=_silver_instrument())
    decision = strategy.evaluate(_SilverRouteData(), _MultiVenueOrders(), _Risk__test_institutional_strategy_runtime(), 1)
    assert decision.decision in {DecisionOutput.NO_TRADE_INSUFFICIENT_EDGE, DecisionOutput.NO_TRADE_EXECUTION_UNSAFE}
    estimates = decision.model_values.get("venue_selection", {}).get("estimates", {})
    assert "hyperliquid" in estimates
    assert estimates["hyperliquid"]["routeable"] is False
    assert decision.model_values.get("selected_execution_venue") != "hyperliquid"


def test_groww_direction_is_no_trade_until_options_volatility_context_is_available(tmp_path, monkeypatch):
    monkeypatch.setattr("strategy.institutional_strategy._cfg", lambda name, default: str(tmp_path) if name == "RESEARCH_STORE_PATH" else default)
    ei = ExchangeInstrument(
        exchange=ExchangeName.GROWW, symbol="NIFTY", ws_symbol="NIFTY", display_symbol="NIFTY", asset_id="NIFTY",
        asset_class=AssetClass.OPTION, quote_asset="INR", status="active", tick_size=0.05, lot_step=50.0, min_qty=50.0,
        raw={"stock_code": "NIFTY", "right": "call", "strike_price": 25000, "expiry_date": "2026-06-25"},
    )
    inst = TradableInstrument("NIFTY", "NIFTY options", AssetClass.OPTION, ExchangeName.GROWW, {ExchangeName.GROWW: ei})
    strategy = InstitutionalStrategy(instrument=inst)
    orders = SimpleNamespace(active_exchange="groww", symbol="NIFTY", display_symbol="NIFTY")
    decision = strategy.evaluate(_Data__test_institutional_strategy_runtime([24000.0] * 10, feed_ok=True), orders, _Risk__test_institutional_strategy_runtime(), 1)
    assert decision.desk == "DESK_B_NIFTY_OPTIONS"
    assert decision.direction.value == "NO_TRADE"
    assert "option_volatility_context_interface_missing" in decision.reasons



def test_groww_position_sizing_uses_verified_selected_contract_lot(tmp_path, monkeypatch):
    from strategy.domain import Direction, ProtectionPlan
    def cfg(name, default):
        values = {
            "RESEARCH_STORE_PATH": str(tmp_path),
            "INSTITUTIONAL_RISK_FRACTION_PER_TRADE": 1.0,
            "INSTITUTIONAL_QUARTER_KELLY": 1.0,
            "INSTITUTIONAL_TARGET_OBSERVATION_VOL_BPS": 100000.0,
        }
        return values.get(name, default)
    monkeypatch.setattr("strategy.institutional_strategy._cfg", cfg)
    raw = {
        "stock_code": "NIFTY", "selected_option_contract": {
            "raw": {"runtime_lot_size": 65}
        }
    }
    ei = ExchangeInstrument(
        exchange=ExchangeName.GROWW, symbol="NIFTY", ws_symbol="NIFTY", display_symbol="NIFTY", asset_id="NIFTY",
        asset_class=AssetClass.OPTION, quote_asset="INR", status="active", tick_size=0.05, lot_step=65.0, min_qty=65.0,
        raw=raw,
    )
    inst = TradableInstrument("NIFTY", "NIFTY options", AssetClass.OPTION, ExchangeName.GROWW, {ExchangeName.GROWW: ei})
    strategy = InstitutionalStrategy(instrument=inst)
    decision = strategy._size_position(
        "DESK_B_NIFTY_OPTIONS", "NIFTY", Direction.BULLISH, 100.0, 5000.0, 1.0,
        ProtectionPlan(100.0, 90.0, 120.0, "GROWW_OCO_AFTER_FILL", True),
        SimpleNamespace(get_available_balance=lambda: {"available": 1000000.0}),
    )
    assert decision.quantity > 0
    assert decision.quantity % 65 == 0


def test_groww_position_sizing_rejects_without_verified_lot(tmp_path, monkeypatch):
    from strategy.domain import Direction, ProtectionPlan
    monkeypatch.setattr("strategy.institutional_strategy._cfg", lambda name, default: str(tmp_path) if name == "RESEARCH_STORE_PATH" else default)
    ei = ExchangeInstrument(
        exchange=ExchangeName.GROWW, symbol="NIFTY", ws_symbol="NIFTY", display_symbol="NIFTY", asset_id="NIFTY",
        asset_class=AssetClass.OPTION, quote_asset="INR", status="active", tick_size=0.05, lot_step=1.0, min_qty=1.0,
        raw={"stock_code": "NIFTY"},
    )
    inst = TradableInstrument("NIFTY", "NIFTY options", AssetClass.OPTION, ExchangeName.GROWW, {ExchangeName.GROWW: ei})
    strategy = InstitutionalStrategy(instrument=inst)
    decision = strategy._size_position(
        "DESK_B_NIFTY_OPTIONS", "NIFTY", Direction.BULLISH, 100.0, 100.0, 1.0,
        ProtectionPlan(100.0, 90.0, 120.0, "GROWW_OCO_AFTER_FILL", True), _Risk__test_institutional_strategy_runtime(),
    )
    assert decision.approved is False
    assert decision.reasons == ["verified_nfo_lot_size_unavailable"]


def test_delta_contract_value_sizes_exposure_units_not_raw_contracts(tmp_path, monkeypatch):
    from execution.order_manager import _DeltaAdapter
    from strategy.domain import Direction, ProtectionPlan

    def cfg(name, default):
        values = {
            "RESEARCH_STORE_PATH": str(tmp_path),
            "INSTITUTIONAL_RISK_FRACTION_PER_TRADE": 1.0,
            "INSTITUTIONAL_QUARTER_KELLY": 1.0,
            "INSTITUTIONAL_TARGET_OBSERVATION_VOL_BPS": 100000.0,
            "LEVERAGE": 5.0,
        }
        return values.get(name, default)

    monkeypatch.setattr("strategy.institutional_strategy._cfg", cfg)
    raw = {"contract_type": "perpetual_futures", "contract_value": 0.001, "tick_size": 0.01}
    ei = ExchangeInstrument(
        exchange=ExchangeName.DELTA,
        symbol="PAXGUSD",
        ws_symbol="PAXGUSD",
        display_symbol="PAXGUSD",
        asset_id="GOLD",
        asset_class=AssetClass.COMMODITY,
        quote_asset="USD",
        base_asset="PAXG",
        status="active",
        tick_size=0.01,
        lot_step=0.001,
        min_qty=0.001,
        contract_value_btc=0.001,
        raw=raw,
    )
    inst = TradableInstrument("GOLD", "Gold token derivatives", AssetClass.COMMODITY, ExchangeName.DELTA, {ExchangeName.DELTA: ei})
    strategy = InstitutionalStrategy(instrument=inst)
    decision = strategy._size_position(
        "DESK_A_METALS",
        "PAXGUSD",
        Direction.SHORT,
        4484.55,
        100.0,
        0.90,
        ProtectionPlan(4484.55, 4491.007752, 4467.418645719423, "VENUE_NATIVE_BRACKET", True),
        SimpleNamespace(get_available_balance=lambda: {"available": 100.0}),
    )

    assert decision.approved is True
    assert decision.quantity == 0.065
    assert abs(decision.notional - decision.quantity * 4484.55) < 1e-9
    assert _DeltaAdapter(None, exchange_instrument=ei)._qty_to_contracts(decision.quantity) == 65



class _GrowwReadyData:
    def __init__(self, instrument):
        self.instrument = instrument
        self.active = False
        self.spot = 24100.0
    def get_analysis_price(self):
        return self.spot
    def get_last_price(self):
        return 100.0 if self.active else self.spot
    def get_feed_reliability(self):
        return {"connected": True, "heartbeat_ok": True, "sequence_valid": True, "snapshot_ready": True, "exchange_timestamp_available": True, "latency_vs_baseline_z": 0.0}
    def get_groww_option_volatility_context(self):
        return {"ready_for_long_premium_decision": True, "vrp": -0.05, "live_iv_coverage": 1.0, "reasons": []}
    def get_candles(self, timeframe, limit=100):
        if timeframe == "5m":
            rows = [{"open": 23990.0, "high": 24002.0, "low": 23988.0, "close": 23995.0} for _ in range(25)]
            rows[-1] = {"open": 24000.0, "high": 24102.0, "low": 23999.0, "close": 24100.0}
            return rows
        if timeframe == "15m":
            return [{"open": 23940.0, "high": 23960.0, "low": 23930.0, "close": 23950.0}] * 3 + [{"open": 23950.0, "high": 24102.0, "low": 23945.0, "close": 24100.0}]
        return []
    def activate_groww_execution_vehicle(self, thesis_side, available_funds):
        assert thesis_side == "long"
        self.active = True
        self.instrument.primary.raw["selected_option_contract"] = {"raw": {"runtime_lot_size": 65}}
        return SimpleNamespace(selected_symbol="NIFTY2660224100CE", delta=0.45)
    def get_execution_feed_status(self):
        return {"active_vehicle_ready": self.active, "status": "ACTIVE_OPTION_VEHICLE_LIVE"}
    def get_orderbook(self):
        return {"bids": [[99.99, 650]], "asks": [[100.01, 650]]}
    def get_execution_candles(self, timeframe, limit=40):
        return [{"open": 99.0, "high": 101.0, "low": 98.5, "close": 100.0} for _ in range(20)]


class _GrowwOrders(_Orders):
    active_exchange = "groww"
    symbol = "NIFTY"
    display_symbol = "NIFTY"
    def place_bracket_limit_entry(self, *args, **kwargs):
        self.placed.append((args, kwargs))
        side, qty, entry, stop, target = args[:5]
        return {"order_id": "groww-entry", "quantity": qty, "fill_price": entry, "protection_confirmed": True, "protection_model": "GROWW_OCO_AFTER_FILL"}


def _groww_instrument_for_strategy():
    raw = {"stock_code": "NIFTY"}
    ei = ExchangeInstrument(
        exchange=ExchangeName.GROWW, symbol="NIFTY", ws_symbol="NIFTY", display_symbol="NIFTY", asset_id="NIFTY",
        asset_class=AssetClass.OPTION, quote_asset="INR", status="active", tick_size=0.05, lot_step=65.0, min_qty=65.0, raw=raw,
    )
    return TradableInstrument("NIFTY", "NIFTY options", AssetClass.OPTION, ExchangeName.GROWW, {ExchangeName.GROWW: ei})


def test_groww_ready_context_activates_ce_and_reaches_shadow_decision(tmp_path, monkeypatch):
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_SIGNAL_DECAY_READY", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_FOR_DELTA", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_FOR_DELTA", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_VENUES", (), raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_VENUES", (), raising=False)
    def cfg(name, default):
        values = {"RESEARCH_STORE_PATH": str(tmp_path), "INSTITUTIONAL_ENABLE_LIVE_ENTRIES": False, "INSTITUTIONAL_MIN_NET_EDGE_BPS": 0.0,
                  "INSTITUTIONAL_RISK_FRACTION_PER_TRADE": 1.0, "INSTITUTIONAL_QUARTER_KELLY": 1.0, "INSTITUTIONAL_TARGET_OBSERVATION_VOL_BPS": 100000.0}
        return values.get(name, default)
    monkeypatch.setattr("strategy.institutional_strategy._cfg", cfg)
    inst = _groww_instrument_for_strategy()
    strategy = InstitutionalStrategy(instrument=inst)
    data = _GrowwReadyData(inst)
    decision = strategy.evaluate(data, _GrowwOrders(), SimpleNamespace(get_available_balance=lambda: {"available": 1000000.0}), 1)
    assert data.active is True
    assert decision.direction.value == "BULLISH"
    assert "shadow_mode_live_entries_disabled" in decision.reasons
    assert decision.sizing is not None and decision.sizing.quantity % 65 == 0


def test_groww_live_decision_places_buy_with_oco_only_after_all_gates(tmp_path, monkeypatch):
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_SIGNAL_DECAY_READY", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_FOR_DELTA", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_FOR_DELTA", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_VENUES", (), raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_VENUES", (), raising=False)
    def cfg(name, default):
        values = {"RESEARCH_STORE_PATH": str(tmp_path), "INSTITUTIONAL_ENABLE_LIVE_ENTRIES": True, "INSTITUTIONAL_MIN_NET_EDGE_BPS": 0.0,
                  "INSTITUTIONAL_RISK_FRACTION_PER_TRADE": 1.0, "INSTITUTIONAL_QUARTER_KELLY": 1.0, "INSTITUTIONAL_TARGET_OBSERVATION_VOL_BPS": 100000.0}
        return values.get(name, default)
    monkeypatch.setattr("strategy.institutional_strategy._cfg", cfg)
    inst = _groww_instrument_for_strategy()
    strategy = InstitutionalStrategy(instrument=inst)
    data = _GrowwReadyData(inst)
    orders = _GrowwOrders()
    strategy.on_tick(data, orders, SimpleNamespace(get_available_balance=lambda: {"available": 1000000.0}), 1)
    assert orders.placed
    assert orders.placed[0][0][0] == "BUY"
    assert strategy.get_position()["protection_model"] == "GROWW_OCO_AFTER_FILL"


def test_groww_option_edge_deducts_hold_horizon_theta_carry(tmp_path, monkeypatch):
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_SIGNAL_DECAY_READY", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_FOR_DELTA", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_FOR_DELTA", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_VENUES", (), raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_VENUES", (), raising=False)
    def cfg(name, default):
        values = {
            "RESEARCH_STORE_PATH": str(tmp_path), "INSTITUTIONAL_ENABLE_LIVE_ENTRIES": False,
            "INSTITUTIONAL_MIN_NET_EDGE_BPS": 0.0, "INSTITUTIONAL_RISK_FRACTION_PER_TRADE": 1.0,
            "INSTITUTIONAL_QUARTER_KELLY": 1.0, "INSTITUTIONAL_TARGET_OBSERVATION_VOL_BPS": 100000.0,
            "POLICY_OPTION_MAX_HOLD_SEC": 2700.0,
        }
        return values.get(name, default)
    monkeypatch.setattr("strategy.institutional_strategy._cfg", cfg)
    inst = _groww_instrument_for_strategy()
    strategy = InstitutionalStrategy(instrument=inst)
    data = _GrowwReadyData(inst)
    original = data.activate_groww_execution_vehicle
    def activate(thesis_side, available_funds):
        choice = original(thesis_side, available_funds)
        choice.theta_to_premium = 0.10
        return choice
    data.activate_groww_execution_vehicle = activate
    decision = strategy.evaluate(data, _GrowwOrders(), SimpleNamespace(get_available_balance=lambda: {"available": 1000000.0}), 1)
    expected_theta = 0.10 * (2700.0 / 86400.0) * 10000.0
    assert abs(decision.model_values["theta_carry_bps_expected_hold"] - expected_theta) < 1e-9
    assert decision.model_values["net_edge_formula"] == "premium_delta_edge_bps - total_cost_bps - theta_carry_bps_expected_hold"


def test_decision_telemetry_emits_compact_transition_not_every_tick(tmp_path, monkeypatch, caplog):
    def cfg(name, default):
        values = {
            "RESEARCH_STORE_PATH": str(tmp_path),
            "INSTITUTIONAL_DECISION_TELEMETRY_ENABLED": True,
            "INSTITUTIONAL_DECISION_TELEMETRY_HEARTBEAT_SEC": 30.0,
            "INSTITUTIONAL_DECISION_TELEMETRY_FULL_ON_TRANSITION": False,
            "INSTITUTIONAL_DECISION_TELEMETRY_DEBUG_EVERY_TICK": False,
        }
        return values.get(name, default)
    monkeypatch.setattr("strategy.institutional_strategy._cfg", cfg)
    inst = _groww_instrument_for_strategy()
    strategy = InstitutionalStrategy(instrument=inst)
    from strategy.domain import DecisionOutput, Direction, Regime
    decision = strategy._decision(
        desk="DESK_B_NIFTY_OPTIONS", venue="groww", instrument="NIFTY",
        decision=DecisionOutput.NO_TRADE_INSUFFICIENT_EDGE, direction=Direction.NO_TRADE,
        regime=Regime.BALANCE, expected_net_edge_bps=0.0, uncertainty_bps=1.0,
        liquidity_score=0.0, execution_quality_score=1.0, sizing=None, protection_plan=None,
        reasons=["audit"], model_values={"calculation": 42.0}, research_features={},
    )
    import logging
    with caplog.at_level(logging.INFO):
        strategy._log_decision_calculation(decision)
        strategy._log_decision_calculation(decision)
    assert caplog.text.count("DECISION_TRANSITION") == 1
    assert "DECISION_DETAIL" not in caplog.text
    assert "NOT_EVALUATED_UNTIL_DIRECTION_ACTIVATES_CE_OR_PE" in caplog.text
    assert '"liquidity_score":null' in caplog.text


def test_decision_telemetry_can_emit_full_detail_on_transition(tmp_path, monkeypatch, caplog):
    def cfg(name, default):
        values = {
            "RESEARCH_STORE_PATH": str(tmp_path),
            "INSTITUTIONAL_DECISION_TELEMETRY_ENABLED": True,
            "INSTITUTIONAL_DECISION_TELEMETRY_FULL_ON_TRANSITION": True,
        }
        return values.get(name, default)
    monkeypatch.setattr("strategy.institutional_strategy._cfg", cfg)
    inst = _groww_instrument_for_strategy()
    strategy = InstitutionalStrategy(instrument=inst)
    from strategy.domain import DecisionOutput, Direction, Regime
    decision = strategy._decision(
        desk="DESK_B_NIFTY_OPTIONS", venue="groww", instrument="NIFTY",
        decision=DecisionOutput.NO_TRADE_INSUFFICIENT_EDGE, direction=Direction.NO_TRADE,
        regime=Regime.BALANCE, expected_net_edge_bps=0.0, uncertainty_bps=1.0,
        liquidity_score=0.0, execution_quality_score=1.0, sizing=None, protection_plan=None,
        reasons=["audit"], model_values={"calculation": 42.0}, research_features={},
    )
    import logging
    with caplog.at_level(logging.INFO):
        strategy._log_decision_calculation(decision)
    assert "DECISION_DETAIL" in caplog.text
    assert '"calculation":42.0' in caplog.text


def test_telemetry_does_not_log_unqualified_microstructure_flips_as_transitions(tmp_path, monkeypatch, caplog):
    def cfg(name, default):
        values = {
            "RESEARCH_STORE_PATH": str(tmp_path),
            "INSTITUTIONAL_DECISION_TELEMETRY_ENABLED": True,
            "INSTITUTIONAL_DECISION_TELEMETRY_HEARTBEAT_SEC": 30.0,
            "INSTITUTIONAL_DECISION_TELEMETRY_LOG_UNQUALIFIED_SIGNAL_FLIPS": False,
        }
        return values.get(name, default)
    monkeypatch.setattr("strategy.institutional_strategy._cfg", cfg)
    inst = _groww_instrument_for_strategy()
    strategy = InstitutionalStrategy(instrument=inst)
    from strategy.domain import DecisionOutput, Direction, Regime
    def decision(direction, source):
        return strategy._decision(
            desk="DESK_A_METALS", venue="delta", instrument="PAXGUSD",
            decision=DecisionOutput.NO_TRADE_INSUFFICIENT_EDGE, direction=direction,
            regime=Regime.BALANCE, expected_net_edge_bps=-1.0, uncertainty_bps=3.0,
            liquidity_score=0.8, execution_quality_score=1.0, sizing=None, protection_plan=None,
            reasons=[source, "net_edge_does_not_clear_uncertainty_and_minimum"],
            model_values={"signal_source": source, "costs_bps": 2.5}, research_features={},
        )
    import logging
    with caplog.at_level(logging.INFO):
        strategy._log_decision_calculation(decision(Direction.LONG, "ofi_tfi_microprice_long"))
        strategy._log_decision_calculation(decision(Direction.SHORT, "ofi_tfi_microprice_short"))
    assert caplog.text.count("DECISION_TRANSITION") == 1


def test_nonactionable_blocker_does_not_emit_transition_only_for_regime_flip(tmp_path, monkeypatch, caplog):
    def cfg(name, default):
        values = {
            "RESEARCH_STORE_PATH": str(tmp_path),
            "INSTITUTIONAL_DECISION_TELEMETRY_ENABLED": True,
            "INSTITUTIONAL_DECISION_TELEMETRY_HEARTBEAT_SEC": 30.0,
        }
        return values.get(name, default)
    monkeypatch.setattr("strategy.institutional_strategy._cfg", cfg)
    strategy = InstitutionalStrategy(instrument=_groww_instrument_for_strategy())
    from strategy.domain import DecisionOutput, Direction, Regime
    def blocked(regime):
        return strategy._decision(
            desk="DESK_A_METALS", venue="delta", instrument="SLVONUSD",
            decision=DecisionOutput.NO_TRADE_INSUFFICIENT_EDGE, direction=Direction.NO_TRADE,
            regime=regime, expected_net_edge_bps=-20.0, uncertainty_bps=7.0,
            liquidity_score=0.2, execution_quality_score=1.0, sizing=None, protection_plan=None,
            reasons=["near_touch_depth_unavailable", "net_edge_does_not_clear_uncertainty_and_minimum"],
            model_values={"signal_source": "near_touch_depth_unavailable", "costs_bps": 22.0},
            research_features={},
        )
    import logging
    with caplog.at_level(logging.INFO):
        strategy._log_decision_calculation(blocked(Regime.BALANCE))
        strategy._log_decision_calculation(blocked(Regime.TREND))
    assert caplog.text.count("DECISION_TRANSITION") == 1


def test_numeric_route_rejection_updates_do_not_emit_transition_spam(tmp_path, monkeypatch, caplog):
    monkeypatch.setattr("strategy.institutional_strategy._cfg", lambda name, default: {
        "RESEARCH_STORE_PATH": str(tmp_path),
        "INSTITUTIONAL_DECISION_TELEMETRY_ENABLED": True,
        "INSTITUTIONAL_DECISION_TELEMETRY_HEARTBEAT_SEC": 60.0,
    }.get(name, default))
    strategy = InstitutionalStrategy(instrument=_instrument())
    from strategy.domain import DecisionOutput, Direction, Regime

    def blocked(reason, direction):
        return strategy._decision(
            desk="DESK_A_BTC", venue="hyperliquid", instrument="BTC",
            decision=DecisionOutput.NO_TRADE_EXECUTION_UNSAFE, direction=direction,
            regime=Regime.TREND, expected_net_edge_bps=-2.0, uncertainty_bps=3.0,
            liquidity_score=0.99, execution_quality_score=1.0,
            sizing=None, protection_plan=None,
            reasons=[reason],
            model_values={
                "signal_source": "selected_venue_validated:hyperliquid:market_state_flow_short",
                "costs_bps": 7.1,
            },
            research_features={},
        )

    import logging
    with caplog.at_level(logging.INFO):
        strategy._log_decision_calculation(
            blocked("selected_route_nonpositive_expected_net_edge:-2.480", Direction.SHORT)
        )
        strategy._log_decision_calculation(
            blocked("selected_route_nonpositive_expected_net_edge:-2.971", Direction.LONG)
        )
    assert caplog.text.count("DECISION_TRANSITION") == 1
    assert "selected_route_nonpositive_expected_net_edge:-2.480" in caplog.text


def test_shadow_validated_telemetry_is_not_labelled_insufficient_edge(tmp_path, monkeypatch, caplog):
    monkeypatch.setattr("strategy.institutional_strategy._cfg", lambda name, default: {
        "RESEARCH_STORE_PATH": str(tmp_path),
        "INSTITUTIONAL_DECISION_TELEMETRY_ENABLED": True,
    }.get(name, default))
    strategy = InstitutionalStrategy(instrument=_groww_instrument_for_strategy())
    from strategy.domain import DecisionOutput, Direction, Regime
    decision = strategy._decision(
        desk="DESK_A_METALS", venue="delta", instrument="PAXGUSD",
        decision=DecisionOutput.SHADOW_SIGNAL_VALIDATED, direction=Direction.LONG,
        regime=Regime.BALANCE, expected_net_edge_bps=4.8, uncertainty_bps=3.0,
        liquidity_score=0.9, execution_quality_score=1.0, sizing=None, protection_plan=None,
        reasons=["shadow_mode_live_entries_disabled"], model_values={"costs_bps": 2.7}, research_features={},
    )
    import logging
    with caplog.at_level(logging.INFO):
        strategy._log_decision_calculation(decision)
    assert "DECISION_SHADOW_VALIDATED" in caplog.text
    assert "SHADOW_SIGNAL_VALIDATED" in caplog.text


def test_microstructure_telemetry_includes_weighted_edge_components(tmp_path, monkeypatch):
    monkeypatch.setattr("strategy.institutional_strategy._cfg", lambda name, default: {
        "RESEARCH_STORE_PATH": str(tmp_path),
        "INSTITUTIONAL_REQUIRE_BTC_CROSS_VENUE": False,
        "INSTITUTIONAL_MIN_SIGNAL_BPS": 0.01,
    }.get(name, default))
    strategy = InstitutionalStrategy(instrument=_instrument())
    data = _Data__test_institutional_strategy_runtime([100.0] * 40, feed_ok=True, flow=2400.0)
    decision = strategy.evaluate(data, _Orders(), _Risk__test_institutional_strategy_runtime(), 1)
    values = decision.model_values
    assert values["near_touch_depth_usd"] > 0
    assert "weighted_signal_bps" in values
    assert "ofi_component_bps" in values
    assert "tfi_component_bps" in values
    assert abs(values["raw_microstructure_signal_bps"] - (
        values["ofi_component_bps"] + values["tfi_component_bps"]
        + values["microprice_component_bps"]
    )) < 1e-9
    assert values["signal_architecture"] == "parent_structural_thesis_child_execution_timing_v1"
    assert values["microstructure_cannot_originate_or_flip_thesis"] is True
    assert values["weighted_signal_bps"] == 0.0
    assert values["robust_microstructure_alpha_bps"] > 0.0
    assert values["dislocation_component_bps"] == 0.0


def test_selected_broker_balance_controls_final_position_size_not_delta_balance(tmp_path, monkeypatch):
    from strategy.domain import Direction, ProtectionPlan

    monkeypatch.setattr("strategy.institutional_strategy._cfg", lambda name, default: {
        "RESEARCH_STORE_PATH": str(tmp_path),
        "VENUE_SELECTION_ENABLED": True,
        "INSTITUTIONAL_RISK_FRACTION_PER_TRADE": 1.0,
        "INSTITUTIONAL_QUARTER_KELLY": 1.0,
        "INSTITUTIONAL_TARGET_OBSERVATION_VOL_BPS": 100000.0,
        "LEVERAGE": 5.0,
        "INSTITUTIONAL_MAX_SELECTED_LEVERAGE": 5.0,
    }.get(name, default))
    strategy = InstitutionalStrategy(instrument=_silver_instrument())
    delta_funded_risk = SimpleNamespace(get_available_balance=lambda: {"available": 180.31, "source": "delta"})
    # Hyperliquid has a documented $10 minimum order notional; provide enough
    # venue-local collateral for this test to exercise approval rather than the
    # separate minimum-notional rejection path.
    selected_hyperliquid = SimpleNamespace(get_balance=lambda: {"available": 100.0, "source": "hyperliquid.user_state"})
    decision = strategy._size_position(
        "DESK_A_METALS", "xyz:SILVER", Direction.LONG, 30.0, 100.0, 1.0,
        ProtectionPlan(30.0, 29.0, 32.0, "VENUE_NATIVE_BRACKET", True), delta_funded_risk,
        venue="hyperliquid", balance_source=selected_hyperliquid,
    )
    assert decision.available_cash_used == 100.0
    assert decision.capital_venue == "hyperliquid"
    assert decision.balance_source == "hyperliquid.user_state"
    assert decision.margin_required <= 100.0
    assert decision.notional <= 100.0 * 0.65 * 5.0 + 1e-9
    assert decision.notional >= 10.0
    assert decision.reasons[0] == "broker_local_cash_sizing_approved:hyperliquid"


def test_small_hyperliquid_btc_balance_uses_leverage_to_clear_minimum_notional(tmp_path, monkeypatch):
    from strategy.domain import Direction, ProtectionPlan

    monkeypatch.setattr("strategy.institutional_strategy._cfg", lambda name, default: {
        "RESEARCH_STORE_PATH": str(tmp_path),
        "VENUE_SELECTION_ENABLED": True,
        "INSTITUTIONAL_RISK_FRACTION_PER_TRADE": 0.025,
        "INSTITUTIONAL_FRACTIONAL_KELLY": 0.60,
        "INSTITUTIONAL_TARGET_OBSERVATION_VOL_BPS": 100000.0,
        "LEVERAGE": 25.0,
        "INSTITUTIONAL_MAX_SELECTED_LEVERAGE": 25.0,
        "HYPERLIQUID_MIN_ORDER_NOTIONAL_USD": 10.0,
    }.get(name, default))
    strategy = InstitutionalStrategy(instrument=_hyperliquid_btc_instrument())
    risk = SimpleNamespace(get_available_balance=lambda: {"available": 100000.0, "source": "delta"})
    decision = strategy._size_position(
        "DESK_A_BTC", "BTC", Direction.SHORT, 74838.5, 12.1437, 0.999,
        ProtectionPlan(74838.5, 75078.7445409202, 74189.28816388892, "VENUE_NATIVE_BRACKET", True),
        risk,
        venue="hyperliquid",
        available_cash_snapshot=6.72784,
        balance_source_label="shared_verified_collateral_snapshot:hyperliquid",
    )
    assert decision.approved is True
    assert decision.notional >= 10.0
    assert decision.margin_required <= 6.72784 * 0.85 + 1e-9
    assert decision.risk_to_invalidation <= 6.72784 * 0.025 + 1e-9
    assert decision.reasons[0] == "broker_local_cash_sizing_approved:hyperliquid"


def test_selected_broker_balance_failure_never_falls_back_to_delta_cash(tmp_path, monkeypatch):
    from strategy.domain import Direction, ProtectionPlan

    monkeypatch.setattr("strategy.institutional_strategy._cfg", lambda name, default: {
        "RESEARCH_STORE_PATH": str(tmp_path),
        "VENUE_SELECTION_ENABLED": True,
    }.get(name, default))
    strategy = InstitutionalStrategy(instrument=_silver_instrument())
    delta_funded_risk = SimpleNamespace(get_available_balance=lambda: {"available": 180.31, "source": "delta"})
    failed_hyperliquid = SimpleNamespace(get_balance=lambda: {"available": 0.0, "source": "hyperliquid.user_state"})
    decision = strategy._size_position(
        "DESK_A_METALS", "xyz:SILVER", Direction.LONG, 30.0, 100.0, 1.0,
        ProtectionPlan(30.0, 29.0, 32.0, "VENUE_NATIVE_BRACKET", True), delta_funded_risk,
        venue="hyperliquid", balance_source=failed_hyperliquid,
    )
    assert decision.approved is False
    assert decision.available_cash_used == 0.0
    assert decision.reasons == ["cash_unavailable:hyperliquid"]


def test_current_venue_cannot_trade_when_route_model_values_it_at_a_loss(tmp_path, monkeypatch):
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_SIGNAL_DECAY_READY", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_FOR_DELTA", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_FOR_DELTA", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_VENUES", (), raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_VENUES", (), raising=False)

    def cfg(name, default):
        return {
            "RESEARCH_STORE_PATH": str(tmp_path),
            "INSTITUTIONAL_ENABLE_LIVE_ENTRIES": True,
            "INSTITUTIONAL_MIN_NET_EDGE_BPS": 0.1,
            "INSTITUTIONAL_MIN_SIGNAL_BPS": 0.01,
            "VENUE_SELECTION_MAX_COST_BPS": 100.0,
            "INSTITUTIONAL_PARENT_THESIS_EXECUTION_MODEL_ENABLED": False,
        }.get(name, default)

    selected = SimpleNamespace(
        selected_venue="delta", selected_symbol="BTCUSD", selected_cost_bps=523.2277,
        current_venue="delta", current_cost_bps=523.2277, improvement_bps=0.0,
        reason="selected_cost_above_soft_limit:523.23>100.00",
        estimates={"delta": SimpleNamespace(expected_net_edge_bps=-402.3366, expected_net_profit_usd=-1.8136)},
        as_dict=lambda: {"selected_venue": "delta", "selected_cost_bps": 523.2277},
    )
    monkeypatch.setattr("strategy.institutional_strategy._cfg", cfg)
    monkeypatch.setattr("strategy.institutional_strategy.select_execution_venue", lambda **kwargs: selected)
    decision = InstitutionalStrategy(instrument=_instrument()).evaluate(_Data__test_institutional_strategy_runtime([100.0] * 5), _Orders(), _Risk__test_institutional_strategy_runtime(), 1)
    assert decision.decision is DecisionOutput.NO_TRADE_EXECUTION_UNSAFE
    assert decision.reasons[0].startswith("selected_route_cost_exceeds_limit:")


def test_hyperliquid_subminimum_notional_is_rejected_before_submission(tmp_path, monkeypatch):
    from strategy.domain import Direction, ProtectionPlan

    monkeypatch.setattr("strategy.institutional_strategy._cfg", lambda name, default: {
        "RESEARCH_STORE_PATH": str(tmp_path),
        "VENUE_SELECTION_ENABLED": True,
        "INSTITUTIONAL_RISK_FRACTION_PER_TRADE": 1.0,
        "INSTITUTIONAL_QUARTER_KELLY": 1.0,
        "INSTITUTIONAL_TARGET_OBSERVATION_VOL_BPS": 100000.0,
        "LEVERAGE": 5.0,
        "INSTITUTIONAL_MAX_SELECTED_LEVERAGE": 5.0,
        "HYPERLIQUID_MIN_ORDER_NOTIONAL_USD": 10.0,
    }.get(name, default))
    strategy = InstitutionalStrategy(instrument=_silver_instrument())
    risk = SimpleNamespace(get_available_balance=lambda: {"available": 180.31, "source": "delta"})
    hyperliquid = SimpleNamespace(get_balance=lambda: {"available": 0.5, "source": "hyperliquid.user_state"})
    decision = strategy._size_position(
        "DESK_A_METALS", "xyz:SILVER", Direction.LONG, 30.0, 100.0, 1.0,
        ProtectionPlan(30.0, 29.0, 32.0, "VENUE_NATIVE_BRACKET", True), risk,
        venue="hyperliquid", balance_source=hyperliquid,
    )
    assert decision.approved is False
    assert decision.notional < 10.0
    assert decision.reasons[0].startswith("order_notional_below_venue_minimum:hyperliquid:")

# ── Preserved regression section: test_institutional_v10_broker_state_and_hl_precision.py ───────────────────────────────
from types import SimpleNamespace

from execution.collateral_service import BrokerCollateralSnapshotService
from exchanges.hyperliquid.api import HyperliquidAPI
from strategy.institutional_strategy import _hyperliquid_price_increment


class _Manager:
    def __init__(self, venue, symbol, balances):
        self._adapter = SimpleNamespace(symbol=symbol)
        self._balances = list(balances)
        self.calls = 0
    def get_balance(self):
        self.calls += 1
        if not self._balances:
            return {"error": "rate_limited"}
        item = self._balances.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class _Router__test_institutional_v10_broker_state_and_hl_precision:
    def __init__(self, managers):
        self.managers = managers
    def available_exchanges(self):
        return tuple(self.managers)
    def manager_for(self, venue):
        return self.managers[venue]


def test_shared_collateral_service_deduplicates_wallet_authority_across_contexts():
    a = _Manager("coinswitch", "BTCUSDT", [{"available": 25.0, "source": "cs"}])
    b = _Manager("coinswitch", "XAGUSDT", [{"available": 25.0, "source": "cs"}])
    service = BrokerCollateralSnapshotService()
    service.register_router(_Router__test_institutional_v10_broker_state_and_hl_precision({"coinswitch": a}))
    service.register_router(_Router__test_institutional_v10_broker_state_and_hl_precision({"coinswitch": b}))
    assert set(service._sources) == {"coinswitch"}


def test_shared_collateral_service_retains_last_verified_snapshot_during_rate_limit():
    manager = _Manager("hyperliquid", "xyz:SILVER", [
        {"available": 6.72, "source": "verified", "balance_verified": True},
        {"error": "429"},
    ])
    router = _Router__test_institutional_v10_broker_state_and_hl_precision({"hyperliquid": manager})
    service = BrokerCollateralSnapshotService()
    service.register_router(router)
    service._refresh("hyperliquid:xyz", "hyperliquid", manager)
    assert service.cash_by_venue(router, {"hyperliquid"})["hyperliquid"] == 6.72
    service._refresh("hyperliquid:xyz", "hyperliquid", manager)
    assert service.cash_by_venue(router, {"hyperliquid"})["hyperliquid"] == 6.72


class _Info:
    def name_to_asset(self, coin):
        return 0
    asset_to_sz_decimals = {0: 5}


def _api():
    api = object.__new__(HyperliquidAPI)
    api.info = _Info()
    return api


def test_hyperliquid_btc_price_respects_official_significant_figure_rule():
    api = _api()
    # BTC perp uses szDecimals=5: at a five-digit dollar price, decimals
    # would exceed the five-significant-figure limit and must be removed.
    assert api.round_price("BTC", 75241.50) == 75242.0
    assert api.round_price("BTC", 75241.49) == 75241.0


def test_hyperliquid_dynamic_tick_geometry_matches_order_precision():
    assert _hyperliquid_price_increment(75241.5, 0.00001) == 1.0
    assert _hyperliquid_price_increment(90.1265, 0.01) == 0.001


def test_hyperliquid_minimum_order_notional_is_enforced_before_route_selection():
    from strategy.institutional_strategy import InstitutionalStrategy
    strategy = InstitutionalStrategy(instrument=None)
    assert strategy._venue_min_order_notional_usd("hyperliquid") == 10.0
    notionals, margins = strategy._venue_selection_budgets({"hyperliquid": 6.7278})
    # Comparison is performed from broker-local margin allocation expanded by
    # selected leverage, never by another venue's collateral.
    assert notionals["hyperliquid"] >= 10.0
    assert margins["hyperliquid"] > 0.0


def test_hyperliquid_minimum_route_is_excluded_when_venue_local_margin_allocation_cannot_support_it():
    from strategy.institutional_strategy import InstitutionalStrategy
    strategy = InstitutionalStrategy(instrument=None)
    notionals, _ = strategy._venue_selection_budgets({"hyperliquid": 0.4})
    assert notionals["hyperliquid"] == 0.0


def test_non_hyperliquid_route_has_no_invented_minimum_notional():
    from strategy.institutional_strategy import InstitutionalStrategy
    strategy = InstitutionalStrategy(instrument=None)
    notionals, _ = strategy._venue_selection_budgets({"delta": 6.7278})
    assert notionals["delta"] > 0.0

# ── Preserved regression section: test_institutional_v11_session_and_reconciliation.py ───────────────────────────────
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from exchanges.groww.market_session import groww_market_session_state
from strategy.institutional_strategy import InstitutionalStrategy, PositionPhase, PositionState


IST = ZoneInfo("Asia/Kolkata")


def test_verified_nse_holiday_keeps_groww_nifty_dormant():
    state = groww_market_session_state(datetime(2026, 5, 28, 9, 30, tzinfo=IST))
    assert state.is_open is False
    assert state.session_code == "HOLIDAY"
    assert "NSE trading holiday" in state.reason


class _PriceData:
    def get_analysis_price(self):
        return 100.0

    def get_venue_microstates(self):
        return {}


class _Risk__test_institutional_v11_session_and_reconciliation:
    pass


class _SlowBrokerPositionReader:
    active_exchange = "hyperliquid"
    symbol = "BTC"

    def __init__(self):
        self.entered = threading.Event()
        self.release = threading.Event()
        self.calls = 0

    def get_open_position(self):
        self.calls += 1
        self.entered.set()
        self.release.wait(timeout=2.0)
        return {"size": 1.0}


def test_active_position_broker_reconciliation_is_not_on_tick_hot_path():
    strategy = InstitutionalStrategy(instrument=None)
    strategy._pos = PositionState(
        phase=PositionPhase.ACTIVE,
        side="long",
        quantity=1.0,
        entry_price=100.0,
        sl_price=90.0,
        tp_price=110.0,
        exchange="hyperliquid",
        execution_symbol="BTC",
        asset_id="BTC",
        protection_confirmed=True,
    )
    broker = _SlowBrokerPositionReader()
    start = time.perf_counter()
    strategy._monitor_position(_PriceData(), broker, _Risk__test_institutional_v11_session_and_reconciliation())
    elapsed = time.perf_counter() - start
    try:
        assert elapsed < 0.10
        assert broker.entered.wait(timeout=0.5)
        assert broker.calls == 1
    finally:
        broker.release.set()
        strategy.stop_runtime_services()

# ── Preserved regression section: test_institutional_v12_dynamic_exit_and_route_ledger.py ───────────────────────────────
import time

from execution.order_manager import OrderManager
from execution.venue_selection import select_execution_venue
from market_data.feed_health import score_feed_health
from market_data.normalizer import InstrumentMapping, build_venue_microstate
from strategy.institutional_strategy import InstitutionalStrategy, PositionPhase, PositionState


def _state__test_institutional_v12_dynamic_exit_and_route_ledger(venue: str, symbol: str, *, mid: float = 73000.0, depth_qty: float = 100000.0):
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
            canonical_underlying="BTC",
            product_class="linear_perp",
            quote_currency="USD",
            contract_multiplier=1.0,
            settlement_currency="USDC",
            price_tick=0.5,
            qty_step=0.00001,
            execution_enabled=True,
            notional_model="linear",
        ),
        bids=[(mid - 0.5, depth_qty), (mid - 1.0, depth_qty)],
        asks=[(mid + 0.5, depth_qty), (mid + 1.0, depth_qty)],
        feed_health=health,
        receive_ts_ns=time.time_ns(),
        exchange_ts_ns=time.time_ns() - 1_000_000,
    )


class _NoBrokerIoOnTick__test_institutional_v12_dynamic_exit_and_route_ledger:
    def emergency_flatten(self, *args, **kwargs):
        raise AssertionError("alpha-decay supervision must not flatten on the market tick")


class _Data__test_institutional_v12_dynamic_exit_and_route_ledger:
    pass


def test_alpha_horizon_is_telemetry_only_and_microstructure_reversal_cannot_liquidate_btc(monkeypatch):
    monkeypatch.setattr(
        "strategy.institutional_strategy._cfg",
        lambda name, default: {
            "DYNAMIC_EXIT_AUTOMATED_EARLY_LIQUIDATION_ENABLED": True,
            "DYNAMIC_EXIT_PARENT_STRUCTURE_ONLY": True,
            "DYNAMIC_EXIT_MIN_DISTINCT_PARENT_OBSERVATIONS": 2,
        }.get(name, default),
    )
    strategy = InstitutionalStrategy(instrument=None)
    strategy._asset_id = "BTC"
    strategy._pos = PositionState(
        phase=PositionPhase.ACTIVE, side="long", quantity=0.00055,
        entry_price=73170.0, sl_price=72950.0, tp_price=73700.0,
        entry_time=time.time() - 8.0, exchange="hyperliquid", execution_symbol="BTC", asset_id="BTC",
        protection_confirmed=True,
        quant_components={"dynamic_protection_plan": {"signal_decay": {"optimal_hold_sec": 1.0}}},
    )
    # A violently opposed child tape is deliberately not sufficient.  The
    # parent state has not reversed, so the broker must remain protected/open.
    strategy._dynamic_exit_live_state = lambda data: {
        "ready": True, "direction": "SHORT", "opposed": True, "opposing_net_edge_bps": 50.0,
        "parent_state_id": "btc-parent-101", "parent_structure_opposed": False,
        "parent_opposing_net_edge_bps": 0.0,
    }
    strategy._dynamic_exit_supervision(_Data__test_institutional_v12_dynamic_exit_and_route_ledger(), _NoBrokerIoOnTick__test_institutional_v12_dynamic_exit_and_route_ledger())
    assert strategy._pos.dynamic_exit_requested is False
    assert strategy._pos.dynamic_exit_order_id == ""
    assert strategy._pos.phase is PositionPhase.ACTIVE
    assert strategy._position_reconcile_wakeup.is_set() is False
    assert strategy._pos.quant_components["dynamic_exit_validation"]["structural_monitor_logged"] is True


def test_distinct_parent_structural_reversals_enqueue_reconciled_exit(monkeypatch):
    monkeypatch.setattr(
        "strategy.institutional_strategy._cfg",
        lambda name, default: {
            "DYNAMIC_EXIT_AUTOMATED_EARLY_LIQUIDATION_ENABLED": True,
            "DYNAMIC_EXIT_PARENT_STRUCTURE_ONLY": True,
            "DYNAMIC_EXIT_MIN_DISTINCT_PARENT_OBSERVATIONS": 2,
        }.get(name, default),
    )
    strategy = InstitutionalStrategy(instrument=None)
    strategy._asset_id = "BTC"
    strategy._pos = PositionState(
        phase=PositionPhase.ACTIVE, side="short", quantity=0.00055,
        entry_price=73170.0, sl_price=73400.0, tp_price=72600.0,
        entry_time=time.time() - 20.0, exchange="hyperliquid", execution_symbol="BTC", asset_id="BTC",
        protection_confirmed=True, quant_components={"dynamic_protection_plan": {"signal_decay": {"optimal_hold_sec": 1.0}}},
    )
    parent_ids = iter(["closed-parent-1", "closed-parent-2"])
    strategy._dynamic_exit_live_state = lambda data: {
        "ready": True, "parent_state_id": next(parent_ids),
        "parent_structure_opposed": True, "parent_opposing_net_edge_bps": 5.0,
    }
    strategy._dynamic_exit_supervision(_Data__test_institutional_v12_dynamic_exit_and_route_ledger(), _NoBrokerIoOnTick__test_institutional_v12_dynamic_exit_and_route_ledger())
    assert strategy._pos.dynamic_exit_requested is False
    strategy._dynamic_exit_supervision(_Data__test_institutional_v12_dynamic_exit_and_route_ledger(), _NoBrokerIoOnTick__test_institutional_v12_dynamic_exit_and_route_ledger())
    assert strategy._pos.dynamic_exit_requested is True
    assert strategy._pos.dynamic_exit_reasons == ("confirmed_parent_structural_invalidation",)
    assert strategy._position_reconcile_wakeup.is_set()


class _SubmitManager:
    def __init__(self):
        self.calls = []

    def place_reconciled_reduce_only_exit(self, **kwargs):
        self.calls.append(kwargs)
        return {
            "order_id": "dynamic-close-1",
            "status": "SUBMITTED",
            "exit_lifecycle": "DYNAMIC_REDUCE_ONLY_CLOSE_PROTECTION_REMAINS_ARMED",
        }


def test_lifecycle_supervisor_submits_tracked_reduce_only_exit_and_retains_bracket():
    strategy = InstitutionalStrategy(instrument=None)
    strategy._pos = PositionState(
        phase=PositionPhase.ACTIVE,
        side="long",
        quantity=0.00055,
        entry_price=73170.0,
        sl_price=72950.0,
        tp_price=73700.0,
        sl_order_id="sl-live",
        tp_order_id="tp-live",
        exchange="hyperliquid",
        execution_symbol="BTC",
        asset_id="BTC",
        protection_confirmed=True,
        dynamic_exit_requested=True,
        dynamic_exit_reasons=("signal_alpha_cost_crossing_horizon_reached",),
    )
    manager = _SubmitManager()

    strategy._service_dynamic_exit_request(manager)

    assert len(manager.calls) == 1
    assert manager.calls[0]["expected_quantity"] == 0.00055
    assert strategy._pos.phase is PositionPhase.EXITING
    assert strategy._pos.dynamic_exit_order_id == "dynamic-close-1"
    assert strategy._pos.sl_order_id == "sl-live"
    assert strategy._pos.tp_order_id == "tp-live"


def test_dynamic_reduce_only_exit_closes_small_real_broker_position_instead_of_treating_it_flat():
    manager = object.__new__(OrderManager)
    submitted = {}
    manager.get_open_position = lambda: {"size": 0.00055, "side": "LONG"}

    def _place_market_order(*, side, quantity, reduce_only=False):
        submitted.update(side=side, quantity=quantity, reduce_only=reduce_only)
        return {"order_id": "dyn-order", "status": "FILLED"}

    manager.place_market_order = _place_market_order
    result = OrderManager.place_reconciled_reduce_only_exit(
        manager,
        reason="signal_alpha_cost_crossing_horizon_reached",
        expected_side="long",
        expected_quantity=0.00055,
    )

    assert result["order_id"] == "dyn-order"
    assert submitted == {"side": "SELL", "quantity": 0.00055, "reduce_only": True}
    assert result["exit_lifecycle"] == "DYNAMIC_REDUCE_ONLY_CLOSE_PROTECTION_REMAINS_ARMED"


class _ExitReconciliationManager:
    def __init__(self):
        self.dynamic_id = None
        self.swept = []

    def identify_exit_order(self, sl_order_id, tp_order_id, dynamic_exit_order_id=None):
        self.dynamic_id = dynamic_exit_order_id
        return {
            "confirmed": True,
            "exit_type": "dynamic_exit",
            "fill_price": 73105.25,
            "fee_paid": 0.0,
            "fee_exact": True,
        }

    def cancel_symbol_conditionals(self, symbol):
        self.swept.append(symbol)


class _RiskRecorder:
    def __init__(self):
        self.open_states = []
        self.trades = []

    def record_trade(self, **kwargs):
        self.trades.append(kwargs)

    def set_position_open(self, value):
        self.open_states.append(value)


def test_confirmed_dynamic_close_is_a_valid_exit_source_before_residual_protection_sweep(monkeypatch):
    monkeypatch.setattr(
        "strategy.institutional_strategy._cfg",
        lambda name, default: {
            "DYNAMIC_EXIT_CANCEL_RESIDUAL_PROTECTION_AFTER_CONFIRMED_FLAT": True,
            "COMMISSION_RATE": 0.0,
        }.get(name, default),
    )
    strategy = InstitutionalStrategy(instrument=None)
    strategy._pos = PositionState(
        phase=PositionPhase.EXITING,
        side="long",
        quantity=0.00055,
        entry_price=73170.0,
        sl_price=72950.0,
        tp_price=73700.0,
        sl_order_id="sl-live",
        tp_order_id="tp-live",
        dynamic_exit_order_id="dynamic-close-1",
        exchange="hyperliquid",
        execution_symbol="BTC",
        asset_id="BTC",
        entry_fee_exact=True,
        entry_leverage=25.0,
    )
    manager = _ExitReconciliationManager()
    risk = _RiskRecorder()

    assert strategy._finalise_confirmed_exit(manager, risk, 73105.25) is True
    assert manager.dynamic_id == "dynamic-close-1"
    assert manager.swept == ["BTC"]
    assert risk.open_states == [False]
    assert risk.trades[0]["reason"] == "dynamic_exit"


def test_common_quantity_ledger_routes_on_net_edge_not_broker_collateral_size(monkeypatch):
    monkeypatch.setattr(
        "execution.venue_selection._cfg",
        lambda name, default: {
            "VENUE_ROUND_TRIP_FEE_BPS": {"delta": 17.0, "hyperliquid": 7.0},
            "VENUE_SLIPPAGE_IMPACT_MULTIPLIER": 0.0,
            "VENUE_SELECTION_MIN_IMPROVEMENT_BPS": 0.0,
        }.get(name, default),
    )
    states = {
        "delta": _state__test_institutional_v12_dynamic_exit_and_route_ledger("delta", "BTCUSD"),
        "hyperliquid": _state__test_institutional_v12_dynamic_exit_and_route_ledger("hyperliquid", "BTC"),
    }
    selection = select_execution_venue(
        states=states,
        direction="LONG",
        asset_id="BTC",
        current_venue="delta",
        routeable_venues={"delta", "hyperliquid"},
        notional_usd=0.0,
        notional_by_venue={"delta": 40.0, "hyperliquid": 40.0},
        required_margin_by_venue={"delta": 2.0, "hyperliquid": 2.0},
        gross_edge_by_venue={"delta": 26.0, "hyperliquid": 26.0},
        available_cash_by_venue={"delta": 1000.0, "hyperliquid": 5.0},
        protection_capable_venues={"delta", "hyperliquid"},
        comparison_quantity=0.00055,
        comparison_notional_usd=40.0,
        snapshot_ts_ns=123456,
    )

    assert selection.selected_venue == "hyperliquid"
    assert selection.selection_mode == "RISK_NORMALISED_COMMON_QUANTITY"
    assert selection.reason == "highest_risk_normalised_expected_net_edge_route"
    assert selection.comparison_quantity == 0.00055
    assert selection.snapshot_ts_ns == 123456
    assert selection.estimates["hyperliquid"].expected_net_edge_bps > selection.estimates["delta"].expected_net_edge_bps

# ── Preserved regression section: test_institutional_v14_parent_child_thesis.py ───────────────────────────────
import time

from intelligence.venue_market_state import VenueMarketState, VenueMarketStateEngine
from market_data.feed_health import score_feed_health
from market_data.normalizer import InstrumentMapping, build_venue_microstate
from strategy.domain import Direction
from strategy.institutional_strategy import InstitutionalStrategy


def _microstate__test_institutional_v14_parent_child_thesis(mid: float, flow: float):
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


def _parent__test_institutional_v14_parent_child_thesis(alpha: float, uncertainty: float = 0.25):
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


def _strategy__test_institutional_v14_parent_child_thesis(monkeypatch, tmp_path):
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
    strategy = _strategy__test_institutional_v14_parent_child_thesis(monkeypatch, tmp_path)
    state = _microstate__test_institutional_v14_parent_child_thesis(73_000.0, flow=8_000_000.0)
    direction, edge, reason, breakdown = strategy._direction_and_edge(
        "DESK_A_BTC",
        73_000.0,
        0.99,
        execution_state=state,
        btc_composite=None,
        market_state=_parent__test_institutional_v14_parent_child_thesis(0.0),
        cross_venue_evidence=None,
    )
    assert direction is Direction.NO_TRADE
    assert edge == 0.0
    assert reason == "parent_structural_thesis_not_established"
    assert breakdown["robust_microstructure_alpha_bps"] > 0.0
    assert breakdown["weighted_signal_bps"] == 0.0
    assert breakdown["microstructure_cannot_originate_or_flip_thesis"] is True


def test_btc_flow_reversal_cannot_flip_confirmed_parent_direction(monkeypatch, tmp_path):
    strategy = _strategy__test_institutional_v14_parent_child_thesis(monkeypatch, tmp_path)
    state = _microstate__test_institutional_v14_parent_child_thesis(73_000.0, flow=-8_000_000.0)
    direction, edge, reason, breakdown = strategy._direction_and_edge(
        "DESK_A_BTC",
        73_000.0,
        0.99,
        execution_state=state,
        btc_composite=None,
        market_state=_parent__test_institutional_v14_parent_child_thesis(8.0),
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
    engine.build(_FlatClosedCandleSource(), {"hyperliquid": _microstate__test_institutional_v14_parent_child_thesis(100.0, flow=0.0)})
    engine._live_marks["hyperliquid"][0] = (time.time() - 61.0, 100.0)
    live_impulse = _microstate__test_institutional_v14_parent_child_thesis(110.0, flow=8_000_000.0)
    result = engine.build(_FlatClosedCandleSource(), {"hyperliquid": live_impulse})["hyperliquid"]
    assert result.ready is True
    assert result.acceptance_bps == 0.0
    assert result.signed_alpha_bps == 0.0
    assert result.diagnostics["acceptance_source"] == "latest_closed_1m_close"
    assert result.diagnostics["live_impulse_is_timing_only"] is True
    assert result.diagnostics["live_blend_bps_diagnostic"] > 0.0

# ── Preserved regression section: test_institutional_v15_all_directional_parent_authority.py ───────────────────────────────
import time

import pytest

from intelligence.venue_market_state import VenueMarketState
from market_data.feed_health import score_feed_health
from market_data.normalizer import InstrumentMapping, build_venue_microstate
from strategy.domain import DeskId, Direction
from strategy.institutional_strategy import InstitutionalStrategy, PositionPhase, PositionState


PARENT_ASSETS = ("BTC", "GOLD_PAXG", "GOLD_HL", "SILVER_SLVON", "SILVER_XAG", "SILVER_HL", "OIL", "NATGAS")
ASSETS = {
    "GOLD_PAXG": ("delta", "PAXGUSD", DeskId.METALS.value),
    "GOLD_HL": ("hyperliquid", "xyz:GOLD", DeskId.METALS.value),
    "SILVER_SLVON": ("delta", "SLVONUSD", DeskId.METALS.value),
    "SILVER_XAG": ("coinswitch", "XAGUSDT", DeskId.METALS.value),
    "SILVER_HL": ("hyperliquid", "xyz:SILVER", DeskId.METALS.value),
    "OIL": ("hyperliquid", "xyz:CL", DeskId.COMMODITIES.value),
    "NATGAS": ("hyperliquid", "xyz:NATGAS", DeskId.COMMODITIES.value),
}


def _state__test_institutional_v15_all_directional_parent_authority(asset: str, venue: str, symbol: str, flow: float):
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


def _parent__test_institutional_v15_all_directional_parent_authority(venue: str, symbol: str, alpha: float, state_id: str = "closed-parent"):
    return VenueMarketState(
        venue=venue, symbol=symbol, ready=True, reason="venue_local_structural_state_ready",
        signed_alpha_bps=alpha, confidence=0.85, uncertainty_bps=0.25,
        regime_label="TREND", returns_bps={"1m": alpha, "5m": alpha},
        robust_one_minute_vol_bps=1.0, volatility_expansion_ratio=1.1,
        acceptance_bps=alpha, live_impulse_bps={}, diagnostics={"parent_state_id": state_id},
    )


def _strategy__test_institutional_v15_all_directional_parent_authority(monkeypatch, tmp_path, asset: str):
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
    strategy = _strategy__test_institutional_v15_all_directional_parent_authority(monkeypatch, tmp_path, asset)
    direction, edge, reason, detail = strategy._direction_and_edge(
        desk, 100.01, 0.95, execution_state=_state__test_institutional_v15_all_directional_parent_authority(asset, venue, symbol, 9_000_000.0),
        btc_composite=None, market_state=_parent__test_institutional_v15_all_directional_parent_authority(venue, symbol, 0.0), cross_venue_evidence=None,
    )
    assert direction is Direction.NO_TRADE
    assert edge == 0.0
    assert reason == "parent_structural_thesis_not_established"
    assert detail["child_timing_alpha_bps"] > 0.0
    assert detail["microstructure_cannot_originate_or_flip_thesis"] is True


@pytest.mark.parametrize("asset", tuple(ASSETS))
def test_opposing_flow_cannot_flip_confirmed_commodity_or_metal_parent(monkeypatch, tmp_path, asset):
    venue, symbol, desk = ASSETS[asset]
    strategy = _strategy__test_institutional_v15_all_directional_parent_authority(monkeypatch, tmp_path, asset)
    direction, edge, reason, detail = strategy._direction_and_edge(
        desk, 100.01, 0.95, execution_state=_state__test_institutional_v15_all_directional_parent_authority(asset, venue, symbol, -9_000_000.0),
        btc_composite=None, market_state=_parent__test_institutional_v15_all_directional_parent_authority(venue, symbol, 8.0), cross_venue_evidence=None,
    )
    assert direction is Direction.LONG
    assert edge > 0.0
    assert reason == "parent_structural_thesis_long_child_timing_validated"
    assert detail["child_timing_alpha_bps"] < 0.0
    assert 0.0 < detail["weighted_signal_bps"] < detail["parent_structural_alpha_bps"]


def test_oil_is_assigned_to_commodities_desk_not_btc(monkeypatch, tmp_path):
    strategy = _strategy__test_institutional_v15_all_directional_parent_authority(monkeypatch, tmp_path, "OIL")
    assert strategy._desk_id("hyperliquid", "xyz:CL") == DeskId.COMMODITIES.value


class _NoBrokerIoOnTick__test_institutional_v15_all_directional_parent_authority:
    def emergency_flatten(self, *args, **kwargs):
        raise AssertionError("structural supervision must not execute on the quote thread")


@pytest.mark.parametrize("asset", tuple(ASSETS))
def test_dynamic_exit_of_each_directional_desk_requires_distinct_closed_parent_invalidations(monkeypatch, tmp_path, asset):
    venue, symbol, _desk = ASSETS[asset]
    strategy = _strategy__test_institutional_v15_all_directional_parent_authority(monkeypatch, tmp_path, asset)
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
    strategy._dynamic_exit_supervision(object(), _NoBrokerIoOnTick__test_institutional_v15_all_directional_parent_authority())
    assert strategy._pos.dynamic_exit_requested is False
    strategy._dynamic_exit_supervision(object(), _NoBrokerIoOnTick__test_institutional_v15_all_directional_parent_authority())
    assert strategy._pos.dynamic_exit_requested is True
    assert strategy._pos.dynamic_exit_reasons == ("confirmed_parent_structural_invalidation",)

# ── Preserved regression section: test_institutional_v6_exposure_feed_execution_fixes.py ───────────────────────────────

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


def _state__test_institutional_v6_exposure_feed_execution_fixes(venue: str, symbol: str, mid: float):
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
    assert "NATGAS" in intents and "xyz:NATGAS" in intents["NATGAS"]


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
    state = _state__test_institutional_v6_exposure_feed_execution_fixes("delta", "SLVONUSD", 67.395)
    est = estimate_venue_cost(
        state=state, direction="LONG", asset_id="SILVER_SLVON", reference_mid=74.276,
        notional_usd=15.0, routeable=True, available_cash_usd=100.0,
        required_margin_usd=1.0, protection_capable=True, gross_edge_bps=86.72,
    )
    assert est.relative_touch_bps_diagnostic < 0
    assert est.total_cost_bps >= est.fee_bps >= 0
    assert est.total_cost_bps >= 0
    assert est.expected_net_edge_bps is not None and est.expected_net_edge_bps < 86.72


def test_short_alpha_horizon_is_telemetry_only_not_a_structural_entry_veto(monkeypatch):
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_REQUIRE_SIGNAL_DECAY_READY", True, raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_MIN_EXECUTABLE_HOLD_SEC_BY_ASSET", {"SILVER_SLVON": 60.0}, raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_VENUES", (), raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_VENUES", (), raising=False)
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
    assert plan.protection_feasible is True
    horizon = plan.diagnostics["signal_horizon_policy"]
    assert horizon["authority"] == "telemetry_only_not_entry_veto"
    assert horizon["below_reference_min"] is True

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
            {"name": "xyz:NATGAS", "szDecimals": 2, "maxLeverage": 20},
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
    assert by_id["NATGAS"] == {__import__('core.instruments', fromlist=['ExchangeName']).ExchangeName.HYPERLIQUID}
    natgas = next(inst for inst in report.matched if inst.asset_id == "NATGAS")
    assert natgas.primary.max_leverage == 10.0
    assert natgas.primary.raw["verified_margin_mode"] == "isolated"

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
        SimpleNamespace(), {"delta": _state__test_institutional_v6_exposure_feed_execution_fixes("delta", "BTCUSD", 75000.0), "hyperliquid": _state__test_institutional_v6_exposure_feed_execution_fixes("hyperliquid", "BTC", 75000.0)}, None, "BTCUSD"
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

# ── Preserved regression section: test_institutional_v7_market_state_alpha.py ───────────────────────────────
import time
from types import SimpleNamespace

from intelligence.cross_venue_btc import BTCCompositeState
from intelligence.venue_market_state import CrossVenueEvidence, VenueMarketState, VenueMarketStateEngine
from market_data.feed_health import score_feed_health
from market_data.normalizer import InstrumentMapping, build_venue_microstate
from strategy.domain import Direction
from strategy.institutional_strategy import InstitutionalStrategy


def _microstate__test_institutional_v7_market_state_alpha(venue: str, symbol: str, mid: float, *, quality_ok: bool = True, flow: float = 0.0):
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


def _market__test_institutional_v7_market_state_alpha(venue: str, alpha: float) -> VenueMarketState:
    return VenueMarketState(
        venue=venue, symbol="BTC", ready=True, reason="ready", signed_alpha_bps=alpha,
        confidence=0.85, uncertainty_bps=0.25, regime_label="TREND",
        returns_bps={"1m": alpha, "5m": alpha}, robust_one_minute_vol_bps=1.0,
        volatility_expansion_ratio=1.5, acceptance_bps=alpha, live_impulse_bps={}, diagnostics={},
    )


def test_venue_market_state_is_built_from_each_candidate_venues_own_candles():
    data = _VenueCandles()
    engine = VenueMarketStateEngine("BTC")
    states = {"delta": _microstate__test_institutional_v7_market_state_alpha("delta", "BTCUSD", 100.0), "hyperliquid": _microstate__test_institutional_v7_market_state_alpha("hyperliquid", "BTC", 107.9)}
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
    hyper = _microstate__test_institutional_v7_market_state_alpha("hyperliquid", "BTC", 75000.0, flow=0.0)
    composite = BTCCompositeState(
        delta_state=hyper, reference_states={"hyperliquid": hyper}, composite_reference_mid=75000.0,
        delta_dislocation_bps=0.0, flow_agreement_score=0.0, cross_venue_dispersion_bps=0.0,
        candidate_leader_venue="hyperliquid", leader_confidence=1.0, delta_execution_quality_score=0.01,
        excluded_reference_venues={},
    )
    direction, edge, reason, breakdown = strategy._direction_and_edge(
        "DESK_A_BTC", 75000.0, 0.95, execution_state=hyper, btc_composite=composite,
        market_state=_market__test_institutional_v7_market_state_alpha("hyperliquid", 18.0), cross_venue_evidence=None,
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
    state = _microstate__test_institutional_v7_market_state_alpha("hyperliquid", "BTC", 75000.0, flow=0.0)
    evidence = CrossVenueEvidence(
        asset_id="BTC", agreement_score=0.05, dispersion_bps=1.0, leader_venue="hyperliquid",
        leader_confidence=0.7, participating_venues=("delta", "hyperliquid"),
        signed_alpha_by_venue={"delta": -2.0, "hyperliquid": 20.0},
    )
    direction, edge, reason, breakdown = strategy._direction_and_edge(
        "DESK_A_BTC", 75000.0, 0.95, execution_state=state, btc_composite=None,
        market_state=_market__test_institutional_v7_market_state_alpha("hyperliquid", 20.0), cross_venue_evidence=evidence,
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
    state = _microstate__test_institutional_v7_market_state_alpha("delta", "PAXGUSD", 4450.0, flow=0.0)
    direction, edge, reason, breakdown = strategy._direction_and_edge(
        "DESK_A_METALS", 4450.0, 0.90, execution_state=state, btc_composite=None,
        market_state=_market__test_institutional_v7_market_state_alpha("delta", 12.0), cross_venue_evidence=None,
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
    states = {"delta": _microstate__test_institutional_v7_market_state_alpha("delta", "BTCUSD", 75000.0), "hyperliquid": _microstate__test_institutional_v7_market_state_alpha("hyperliquid", "BTC", 75000.0)}
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
        execution_state=_microstate__test_institutional_v7_market_state_alpha("hyperliquid", "xyz:GOLD", 4400.0), venue_market_state=market,
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

# ── Preserved regression section: test_institutional_v8_composite_architecture.py ───────────────────────────────
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


def _market__test_institutional_v8_composite_architecture(venue: str, symbol: str, alpha: float) -> VenueMarketState:
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
        "delta": _market__test_institutional_v8_composite_architecture("delta", "BTCUSD", 8.0),
        "coinswitch": _market__test_institutional_v8_composite_architecture("coinswitch", "BTCUSDT", 10.0),
        "hyperliquid": _market__test_institutional_v8_composite_architecture("hyperliquid", "BTC", 12.0),
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
        market_states={"hyperliquid": _market__test_institutional_v8_composite_architecture("hyperliquid", "xyz:GOLD", 80.0)},
        microstates={"hyperliquid": _micro("hyperliquid", "xyz:GOLD", 4500.0, flow=300000.0)},
    )
    paxg = bus.build_decision(
        asset_id="GOLD_PAXG",
        market_states={"delta": _market__test_institutional_v8_composite_architecture("delta", "PAXGUSD", 4.0), "coinswitch": _market__test_institutional_v8_composite_architecture("coinswitch", "PAXGUSDT", 6.0)},
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
        asset_id="SILVER_XAG", market_states={"coinswitch": _market__test_institutional_v8_composite_architecture("coinswitch", "XAGUSDT", 70.0)},
        microstates={"coinswitch": _micro("coinswitch", "XAGUSDT", 74.0, flow=200000.0)},
    )
    bus.build_decision(
        asset_id="SILVER_HL", market_states={"hyperliquid": _market__test_institutional_v8_composite_architecture("hyperliquid", "xyz:SILVER", 60.0)},
        microstates={"hyperliquid": _micro("hyperliquid", "xyz:SILVER", 74.0, flow=200000.0)},
    )
    slvon = bus.build_decision(
        asset_id="SILVER_SLVON", market_states={"delta": _market__test_institutional_v8_composite_architecture("delta", "SLVONUSD", 2.0)},
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
        market_state=_market__test_institutional_v8_composite_architecture("delta", "BTCUSD", 0.0), cross_venue_evidence=None, composite_decision=collective,
    )
    assert direction is Direction.LONG
    assert edge > 0
    # Parent structural alpha originates the trade; child flow may add at most
    # 35% of the parent and can never originate or reverse it.
    assert breakdown["weighted_signal_bps"] == 10.8
    assert breakdown["parent_structural_alpha_bps"] == 8.0
    assert breakdown["child_timing_alpha_bps"] == 6.0
    assert breakdown["child_timing_contribution_bps"] == 2.8
    assert breakdown["microstructure_cannot_originate_or_flip_thesis"] is True


def test_config_requires_product_aware_factor_routing_policy():
    assert config.INSTITUTIONAL_EXECUTION_EQUIVALENCE_GROUP_BY_ASSET["BTC"] == "BTC_LINEAR_PERP"
    assert config.INSTITUTIONAL_EXECUTION_EQUIVALENCE_GROUP_BY_ASSET["SILVER_SLVON"] != config.INSTITUTIONAL_EXECUTION_EQUIVALENCE_GROUP_BY_ASSET["SILVER_XAG"]
    assert config.INSTITUTIONAL_FACTOR_BY_ASSET["SILVER_SLVON"] == config.INSTITUTIONAL_FACTOR_BY_ASSET["SILVER_HL"] == "SILVER"
    assert config.INSTITUTIONAL_VALIDATED_FACTOR_TRANSLATION_MODELS == {}

# ── Preserved regression section: test_institutional_v9_streaming_execution_runtime.py ───────────────────────────────
import json
import inspect
from types import SimpleNamespace

from exchanges.hyperliquid.data_manager import HyperliquidDataManager
from strategy.institutional_strategy import InstitutionalStrategy
from aggregator.market_aggregator import MarketAggregator


def _hl_manager():
    manager = HyperliquidDataManager(coin="xyz:SILVER", execution_enabled=True)
    manager.is_streaming = True
    return manager


def test_hyperliquid_ws_candles_and_asset_context_populate_cache_without_tick_http(monkeypatch):
    manager = _hl_manager()
    manager._on_message(None, json.dumps({"channel": "candle", "data": {"t": 1000, "i": "1m", "o": 74.0, "h": 74.4, "l": 73.9, "c": 74.2, "v": 10.0}}))
    manager._on_message(None, json.dumps({"channel": "activeAssetCtx", "data": {"coin": "xyz:SILVER", "ctx": {"funding": 0.0001, "markPx": 74.2, "openInterest": 500}}}))
    rows = manager.get_candles("1m", 10)
    assert len(rows) == 1 and rows[0]["close"] == 74.2
    assert manager._funding_rate == 0.0001
    assert manager._mark_price == 74.2
    assert "candles_snapshot" not in inspect.getsource(manager.get_candles)


def test_hyperliquid_trade_and_book_tape_reaches_protection_research_state():
    manager = _hl_manager()
    manager._on_message(None, json.dumps({"channel": "l2Book", "data": {"time": 1000, "levels": [[{"px": "74.1", "sz": "12"}], [{"px": "74.2", "sz": "14"}]]}}))
    manager._on_message(None, json.dumps({"channel": "trades", "data": [{"px": "74.2", "sz": "2", "side": "B"}]}))
    research = manager.get_microstructure_research_state()
    assert research["book_events"]
    assert research["trade_events"]
    assert research["trade_events"][-1]["signed_notional_usd"] > 0


def test_strategy_market_callbacks_wake_context_without_executing_a_tick():
    strategy = InstitutionalStrategy(instrument=None)
    assert strategy.consume_market_event() is False
    strategy._on_realtime_quote(101.0)
    assert strategy.consume_market_event() is True
    strategy._on_realtime_trade(101.0, 2.0, "buy")
    assert strategy.consume_market_event() is True


class _DM:
    def __init__(self, venue):
        self.venue = venue
        self.strategy = None
    def register_strategy(self, strategy):
        self.strategy = strategy


def test_aggregator_registers_event_listener_on_every_information_venue():
    a, b, c = _DM("delta"), _DM("coinswitch"), _DM("hyperliquid")
    marker = object()
    agg = MarketAggregator(a, b, reference_dms=[c])
    agg.register_strategy(marker)
    assert a.strategy is marker and b.strategy is marker and c.strategy is marker


class _BalanceManager:
    def __init__(self):
        self.called = 0
    def get_balance(self):
        self.called += 1
        return {"available": 999.0}


class _Router__test_institutional_v9_streaming_execution_runtime:
    def __init__(self, manager):
        self.manager = manager
    def manager_for(self, venue):
        return self.manager
    def available_exchanges(self):
        return {"hyperliquid"}


def test_market_hot_path_consumes_cached_collateral_only():
    manager = _BalanceManager()
    router = _Router__test_institutional_v9_streaming_execution_runtime(manager)
    strategy = InstitutionalStrategy(instrument=None)
    strategy._runtime_order_manager = router  # refresh service intentionally not started in this unit test
    with strategy._venue_cash_lock:
        strategy._venue_cash_cache["hyperliquid"] = (10**18, 6.72, "cached")
    assert strategy._venue_available_cash(router, {"hyperliquid"}) == {"hyperliquid": 6.72}
    assert manager.called == 0


def test_parallel_runtime_has_atomic_submission_arbitration_guard():
    from orchestration.multi_asset_bot import MultiAssetInstitutionalBot
    bot = MultiAssetInstitutionalBot()
    assert hasattr(bot, "_submission_arbitration_lock")
    strategy = InstitutionalStrategy(instrument=None)
    strategy.bind_portfolio_submission_guard(bot._submission_arbitration_lock, lambda: (False, "portfolio exposure cap"))
    assert strategy._submission_lock is bot._submission_arbitration_lock

# ── Preserved regression section: test_official_hyperliquid_coinswitch_api_v5.py ───────────────────────────────
from types import SimpleNamespace

from exchanges.hyperliquid.api import HyperliquidAPI
from exchanges.coinswitch.api import FuturesAPI
from execution.order_manager import _CoinSwitchAdapter, _HyperliquidAdapter


class _DexStateInfo:
    def __init__(self):
        self.user_state_calls = []
        self.open_orders_calls = []

    def query_user_abstraction_state(self, address):
        return {"mode": "standard"}

    def query_user_dex_abstraction_state(self, address):
        return {"enabled": True}

    def user_state(self, address, dex=""):
        self.user_state_calls.append((address, dex))
        return {"withdrawable": "48.50", "marginSummary": {"accountValue": "50.00"}}

    def open_orders(self, address, dex=""):
        self.open_orders_calls.append((address, dex))
        return [{"coin": "xyz:SILVER", "oid": 11}]


class _UnifiedInfo:
    def __init__(self):
        self.user_state_calls = []
        self.spot_calls = []

    def query_user_abstraction_state(self, address):
        return {"mode": "unifiedAccount"}

    def query_user_dex_abstraction_state(self, address):
        return {"enabled": False}

    def spot_user_state(self, address):
        self.spot_calls.append(address)
        return {"balances": [{"coin": "USDC", "total": "120.0", "hold": "7.5"}]}

    def user_state(self, address, dex=""):
        self.user_state_calls.append((address, dex))
        return {"withdrawable": "0", "marginSummary": {"accountValue": "0"}}


def _hyper(info):
    api = object.__new__(HyperliquidAPI)
    api.info = info
    api.account_address = "0xaccount"
    return api


def test_hyperliquid_hip3_balance_reads_the_instrument_dex_state():
    info = _DexStateInfo()
    out = _hyper(info).get_balance("xyz:SILVER")
    assert info.user_state_calls == [("0xaccount", "xyz")]
    assert out["available"] == 48.50
    assert out["source"] == "hyperliquid_clearinghouse_state:dex=xyz"
    assert out["dex"] == "xyz"


def test_hyperliquid_unified_account_uses_spot_clearinghouse_usdc_for_perp_collateral():
    info = _UnifiedInfo()
    out = _hyper(info).get_balance("xyz:SILVER")
    assert out["available"] == 112.5
    assert out["source"] == "hyperliquid_spot_clearinghouse_state:unified_account"
    assert info.spot_calls == ["0xaccount"]
    assert info.user_state_calls == []


def test_hyperliquid_hip3_open_orders_pass_dex_context():
    info = _DexStateInfo()
    rows = _hyper(info).open_orders("xyz:SILVER")
    assert rows[0]["coin"] == "xyz:SILVER"
    assert info.open_orders_calls == [("0xaccount", "xyz")]


class _CaptureCoinSwitch(FuturesAPI):
    def __init__(self):
        pass

    def _make_request(self, method, endpoint, params=None, payload=None):
        return {"method": method, "endpoint": endpoint, "params": params, "payload": payload}


def test_coinswitch_open_orders_uses_documented_post_orders_open_contract():
    out = _CaptureCoinSwitch().get_open_orders(exchange="EXCHANGE_2", symbol="BTCUSDT")
    assert out["method"] == "POST"
    assert out["endpoint"] == "/trade/api/v2/futures/orders/open"
    assert out["payload"] == {"exchange": "EXCHANGE_2", "symbol": "btcusdt"}


def test_coinswitch_order_status_queries_required_order_id_only():
    out = _CaptureCoinSwitch().get_order("oid-1", exchange="EXCHANGE_2")
    assert out["method"] == "GET"
    assert out["endpoint"] == "/trade/api/v2/futures/order"
    assert out["params"] == {"order_id": "oid-1"}


def test_coinswitch_futures_balance_exposes_documented_usdt_available_source():
    api = _CaptureCoinSwitch()
    api.get_wallet_balance = lambda: {"data": {"base_asset_balances": [{
        "base_asset": "USDT", "balances": {
            "total_available_balance": "85.25", "total_blocked_balance": "4.75",
            "total_balance": "90.00", "total_position_margin": "3.25", "total_open_order_margin": "1.50"
        }}]}}
    out = api.get_balance("USDT")
    assert out["available"] == 85.25
    assert out["total"] == 90.0
    assert out["source"] == "coinswitch_futures_wallet_balance.total_available_balance"
    assert out["wallet_type"] == "USDT_FUTURES"


class _NoWait:
    def wait(self):
        return None


class _OpenOrdersApi:
    def get_open_orders(self, exchange, symbol):
        return {"data": {"orders": [{"order_id": "sl"}, {"order_id": "tp"}]}}


def test_coinswitch_adapter_parses_documented_open_orders_wrapper():
    inst = SimpleNamespace(symbol="BTCUSDT", display_symbol="BTC/USDT", tick_size=0.1, lot_step=0.001, min_qty=0.001, max_qty=1.0)
    adapter = _CoinSwitchAdapter(_OpenOrdersApi(), inst)
    adapter.limiter = _NoWait()
    rows = adapter.get_open_orders("BTCUSDT")
    assert [row["order_id"] for row in rows] == ["sl", "tp"]


class _HyperAdapterApi:
    def __init__(self):
        self.balance_symbol = None
        self.order_symbol = None
        self.state_symbol = None
    def get_balance(self, symbol):
        self.balance_symbol = symbol
        return {"available": 50.0, "source": "dex"}
    def open_orders(self, coin=None):
        self.order_symbol = coin
        return []
    def user_state(self, coin=None):
        self.state_symbol = coin
        return {"assetPositions": []}


def test_hyperliquid_adapter_passes_selected_hip3_symbol_to_balance_orders_and_positions():
    api = _HyperAdapterApi()
    inst = SimpleNamespace(symbol="xyz:SILVER", display_symbol="xyz:SILVER", tick_size=0.01, lot_step=0.01, min_qty=0.01, max_qty=10.0)
    adapter = _HyperliquidAdapter(api, inst)
    adapter.limiter = _NoWait()
    adapter.get_balance()
    adapter.get_open_orders("xyz:SILVER")
    adapter.get_positions("xyz:SILVER")
    assert (api.balance_symbol, api.order_symbol, api.state_symbol) == ("xyz:SILVER", "xyz:SILVER", "xyz:SILVER")


class _CaptureHyperExchange:
    def __init__(self):
        self.bulk_orders_calls = []

    def bulk_orders(self, orders, grouping="na"):
        self.bulk_orders_calls.append((orders, grouping))
        return {"status": "ok", "response": {"type": "order", "data": {"statuses": [
            {"resting": {"oid": 201}},
            {"resting": {"oid": 202}},
        ]}}}


def test_hyperliquid_post_fill_protection_uses_position_tpsl_grouping():
    api = object.__new__(HyperliquidAPI)
    api.exchange = _CaptureHyperExchange()
    api.size_decimals = lambda coin: 5

    out = api.place_reduce_only_tpsl(
        coin="BTC",
        is_buy=True,
        size=0.00067,
        stop_px=75061.0,
        target_px=74203.0,
    )

    orders, grouping = api.exchange.bulk_orders_calls[0]
    assert grouping == "positionTpsl"
    assert HyperliquidAPI.child_order_ids(out) == [201, 202]
    assert [row["order_type"]["trigger"]["tpsl"] for row in orders] == ["sl", "tp"]
    assert all(row["reduce_only"] is True for row in orders)


class _WaitingTriggerTpslApi:
    def __init__(self, open_orders):
        self.open_order_rows = open_orders
        self.market_close_calls = []
        self.limit_orders = []

    def round_size(self, coin, size):
        return float(size)

    def round_price(self, coin, price):
        return float(price)

    def place_limit_order(self, **kwargs):
        self.limit_orders.append(kwargs)
        return {"entry": True}

    def first_order_result(self, resp):
        return {"ok": True, "status": "FILLED", "oid": 101, "avg_px": 74962.0, "total_sz": 0.00063, "raw": resp}

    def place_reduce_only_tpsl(self, **kwargs):
        return {"status": "ok", "response": {"type": "order", "data": {"statuses": ["waitingForTrigger", "waitingForTrigger"]}}}

    def child_order_ids(self, resp):
        return HyperliquidAPI.child_order_ids(resp)

    def open_orders(self, coin=None):
        return list(self.open_order_rows)

    def market_close(self, *args, **kwargs):
        self.market_close_calls.append((args, kwargs))
        return {"status": "ok"}


def test_hyperliquid_waiting_for_trigger_response_does_not_emergency_close():
    api = _WaitingTriggerTpslApi([
        {"coin": "BTC", "oid": 301, "side": "B", "orderType": "Stop Market", "triggerPx": 75178.0, "sz": "0.00063", "reduceOnly": True},
        {"coin": "BTC", "oid": 302, "side": "B", "orderType": "Take Profit Market", "triggerPx": 74247.0, "sz": "0.00063", "reduceOnly": True},
    ])
    adapter = _HyperliquidAdapter(api, SimpleNamespace(symbol="BTC", display_symbol="BTC", tick_size=1.0, lot_step=0.00001, min_qty=0.00001, max_qty=1.0))
    adapter.limiter = _NoWait()

    out = adapter.place_bracket_limit_entry("SELL", 0.00063, 74962.0, 75178.0, 74247.0, timeout_sec=1.0)

    assert out["protection_confirmed"] is True
    assert out["bracket_child_verified"] is True
    assert out["bracket_sl_order_id"] == "301"
    assert out["bracket_tp_order_id"] == "302"
    assert api.market_close_calls == []


def test_hyperliquid_waiting_for_trigger_without_ids_still_stays_open():
    api = _WaitingTriggerTpslApi([])
    adapter = _HyperliquidAdapter(api, SimpleNamespace(symbol="BTC", display_symbol="BTC", tick_size=1.0, lot_step=0.00001, min_qty=0.00001, max_qty=1.0))
    adapter.limiter = _NoWait()

    out = adapter.place_bracket_limit_entry("SELL", 0.00063, 74962.0, 75178.0, 74247.0, timeout_sec=1.0)

    assert out["protection_confirmed"] is True
    assert out["bracket_child_verified"] is False
    assert out["protection_reconcile_required"] is True
    assert api.market_close_calls == []

# ── Preserved regression section: test_options_volatility_context.py ───────────────────────────────
from datetime import datetime, timezone

from agents.indian_options_desk import build_option_volatility_context, compute_vrp, yang_zhang_realized_vol


def test_yang_zhang_and_vrp_are_derived_from_live_ohlc_and_iv():
    candles = [
        {"open": 25000, "high": 25750, "low": 24300, "close": 25300},
        {"open": 25250, "high": 26000, "low": 24600, "close": 24800},
        {"open": 24750, "high": 25800, "low": 24200, "close": 25500},
        {"open": 25500, "high": 26200, "low": 24800, "close": 25000},
    ]
    realised = yang_zhang_realized_vol(candles, window=4)
    assert realised is not None and realised > 0.15
    assert compute_vrp(atm_iv=0.15, realized_vol_yz=realised) < -0.02


def test_option_surface_does_not_invent_signed_dealer_gex_without_position_sign():
    now = datetime(2026, 5, 27, tzinfo=timezone.utc)
    candles = [
        {"open": 25000, "high": 25750, "low": 24300, "close": 25300},
        {"open": 25250, "high": 26000, "low": 24600, "close": 24800},
        {"open": 24750, "high": 25800, "low": 24200, "close": 25500},
        {"open": 25500, "high": 26200, "low": 24800, "close": 25000},
    ]
    chain = [
        {"right": "CE", "strike_price": 25000, "expiry_date": "2026-06-04", "iv": 0.15, "open_interest": 1000},
        {"right": "PE", "strike_price": 25000, "expiry_date": "2026-06-04", "iv": 0.17, "open_interest": 1200},
        {"right": "CE", "strike_price": 25500, "expiry_date": "2026-06-04", "iv": 0.14, "open_interest": 800},
        {"right": "PE", "strike_price": 24500, "expiry_date": "2026-06-04", "iv": 0.18, "open_interest": 900},
        {"right": "CE", "strike_price": 25000, "expiry_date": "2026-06-11", "iv": 0.16, "open_interest": 800},
    ]
    context = build_option_volatility_context(
        chain=chain, underlying_candles=candles, spot=25000.0, lot_size=50, now=now
    )
    assert context.atm_iv == 0.15
    assert context.vrp is not None and context.vrp < -0.02
    assert context.term_slope is not None
    assert context.gross_gamma_exposure is not None and context.gross_gamma_exposure > 0
    assert context.signed_dealer_gex is None
    assert "directed_dealer_gex_requires_position_sign_input" in context.reasons

# ── Preserved regression section: test_portfolio_exposure.py ───────────────────────────────
from risk.portfolio_exposure import PortfolioExposureTracker


def test_correlated_macro_exposure_accumulates_btc_and_metals_gross_delta():
    tracker = PortfolioExposureTracker()
    tracker.record(asset_id="BTC", position_key="btc", signed_delta_usd=80.0)
    permitted = tracker.evaluate_increment(asset_id="GOLD", position_key="gold", signed_delta_usd=15.0, cap_usd=100.0)
    blocked = tracker.evaluate_increment(asset_id="GOLD", position_key="gold", signed_delta_usd=25.0, cap_usd=100.0)
    assert permitted.approved is True
    assert blocked.approved is False
    assert blocked.bucket == "ANTI_DOLLAR_MACRO"

# ── Preserved regression section: test_runtime_shutdown.py ───────────────────────────────
import importlib
import logging
import signal


def test_sigterm_requests_graceful_shutdown_without_ignore_warning():
    import runtime_shutdown_guard

    mod = importlib.reload(runtime_shutdown_guard)
    logger = logging.getLogger("test.runtime_shutdown")
    received = []
    previous = signal.getsignal(signal.SIGTERM)
    try:
        mod.install_graceful_shutdown_handler(
            logger,
            "test-runtime",
            lambda signal_name: received.append(signal_name),
            signals_to_handle=[signal.SIGTERM],
        )
        signal.raise_signal(signal.SIGTERM)
        assert mod.shutdown_requested() is True
        assert received == ["SIGTERM"]
    finally:
        signal.signal(signal.SIGTERM, previous)


def test_multi_asset_external_shutdown_is_non_blocking_and_wakes_loop():
    from orchestration.multi_asset_bot import MultiAssetInstitutionalBot

    bot = MultiAssetInstitutionalBot()
    bot.running = True
    bot.request_external_shutdown("SIGTERM")

    assert bot.running is False
    assert bot._external_shutdown_requested.is_set()
    assert bot._market_wakeup.is_set()

# ── Preserved regression section: test_telegram_notifications.py ───────────────────────────────
from telegram.notifier import format_entry_alert, format_exit_alert, format_partial_exit_alert


def test_exit_alert_uses_strategy_aliases_and_keeps_lifecycle_detail():
    msg = format_exit_alert(
        side="long",
        entry=100.0,
        exit_price=110.0,
        qty=1.25,
        residual_qty=0.50,
        partial_qty=0.75,
        gross=12.5,
        fees=0.4,
        pnl=12.1,
        r_realised=2.0,
        mfe_r=2.4,
        planned_rr=2.5,
        margin_pct=12.1,
        margin_used=100.0,
        fee_source="entry exact / exit exact",
        exact_fees=True,
        reason="tp_hit",
    )

    assert "EXIT REPORT" in msg
    assert "ENTRY  $100.0000" in msg
    assert "EXIT   $110.0000" in msg
    assert "START  1.25" in msg
    assert "PART   0.75" in msg
    assert "FINAL  0.5" in msg
    assert "entry exact / exit exact" in msg


def test_partial_exit_alert_uses_fill_aliases():
    msg = format_partial_exit_alert(
        side="short",
        role="TP1",
        fill_price=95.5,
        qty_closed=0.4,
        qty_remaining=0.6,
        gross=2.0,
        fees=0.1,
        net=1.9,
        cumulative_net=1.9,
        sl=101.0,
        final_tp=90.0,
        status="FILLED",
    )

    assert "PARTIAL EXIT" in msg
    assert "TP1" in msg
    assert "FILL   $95.5000" in msg
    assert "CLOSED 0.4" in msg
    assert "LEFT   0.6" in msg
    assert "TOTAL  $+1.9000" in msg


def test_entry_alert_is_sectioned_not_dumped():
    msg = format_entry_alert(
        side="long",
        entry=100.0,
        sl=98.0,
        tp=106.0,
        qty=2.0,
        leverage=5.0,
        rr=3.0,
        context_4h="0.72 bullish",
        context_15m="0.66 bullish",
        liquidity_event_label="sell-side liquidity transfer",
        liquidity_event_price=99.0,
        liquidity_event_quality=0.8,
        displacement_atr=1.5,
        delivery_score=0.7,
        risk_usd=4.0,
        margin_used=40.0,
        fee_status="broker exact $0.02",
        decision_path="INSTITUTIONAL_PROTECTED_FLOW",
    )

    assert "ENTRY TICKET" in msg
    assert "<b>Price Map</b>" in msg
    assert "<b>Size / Risk</b>" in msg
    assert "<b>Decision Context</b>" in msg
    assert "ENTRY  $100.0000" in msg



# ── V17 HIP-3 isolated-margin commodity capability / NATGAS regressions ───────

class _FakeHyperLeverageAPI_V17:
    def __init__(self):
        self.calls = []
    def update_leverage(self, coin, leverage, is_cross=True):
        self.calls.append((coin, leverage, is_cross))
        return {"status": "ok"}


def _hl_exchange_instrument_v17(symbol: str):
    return SimpleNamespace(
        symbol=symbol, display_symbol=symbol, tick_size=0.001, lot_step=0.001,
        min_qty=0.001, max_qty=0.0,
    )


def test_hyperliquid_wti_uses_verified_isolated_margin_without_cross_retry(monkeypatch):
    from execution.order_manager import _HyperliquidAdapter
    api = _FakeHyperLeverageAPI_V17()
    adapter = _HyperliquidAdapter(api, _hl_exchange_instrument_v17("xyz:CL"))
    adapter.limiter.wait = lambda: None
    result = adapter.set_leverage(20)
    assert result["success"] is True
    assert result["margin_mode"] == "ISOLATED"
    assert result["capability_verified"] is True
    assert api.calls == [("xyz:CL", 20, False)]


def test_hyperliquid_natgas_uses_isolated_margin_and_caps_leverage_to_official_10x(monkeypatch):
    from execution.order_manager import _HyperliquidAdapter
    api = _FakeHyperLeverageAPI_V17()
    adapter = _HyperliquidAdapter(api, _hl_exchange_instrument_v17("xyz:NATGAS"))
    adapter.limiter.wait = lambda: None
    result = adapter.set_leverage(25)
    assert result["success"] is True
    assert result["requested_leverage"] == 25
    assert result["leverage"] == 10
    assert result["margin_mode"] == "ISOLATED"
    assert api.calls == [("xyz:NATGAS", 10, False)]


def test_hyperliquid_existing_cross_enabled_metals_keep_verified_cross_margin():
    from execution.order_manager import _HyperliquidAdapter
    api = _FakeHyperLeverageAPI_V17()
    adapter = _HyperliquidAdapter(api, _hl_exchange_instrument_v17("xyz:GOLD"))
    adapter.limiter.wait = lambda: None
    result = adapter.set_leverage(25)
    assert result["success"] is True
    assert result["margin_mode"] == "CROSS"
    assert api.calls == [("xyz:GOLD", 25, True)]


# ── V18 protected bracket outcome / exact fill accounting ───────────────────
from strategy.barrier_outcome import ProtectedBarrierOutcomeEngine
from strategy.predictive_flow import PreMoveOrderFlowHazardEngine, PredictiveFlowAssessment
from market_data.normalizer import VenueMicrostate
from strategy.domain import ProtectionPlan


def _barrier_microstate(mid: float, flow: float, ts: int) -> VenueMicrostate:
    return VenueMicrostate(
        venue="hyperliquid", symbol="xyz:CL", exchange_ts_ns=ts, receive_ts_ns=ts,
        feed_quality_score=1.0, best_bid=mid - 0.005, best_ask=mid + 0.005,
        mid=mid, microprice=mid + (0.002 if flow > 0 else -0.002), spread_bps=1.0,
        bid_depth_usd_by_band={"0-1": 50_000.0, "1-3": 50_000.0},
        ask_depth_usd_by_band={"0-1": 50_000.0, "1-3": 50_000.0},
        obi_by_band={}, ofi_usd_1s=flow, ofi_usd_10s=flow * 2.0, ofi_usd_60s=flow * 2.0,
        tfi_usd_1s=flow * 0.25, tfi_usd_10s=flow * 0.5, tfi_usd_60s=flow,
        basis_bps=None, funding_rate=0.0, update_latency_ms=1.0, sequence_valid=True,
        product_class="linear_perp", execution_enabled=True,
    )


def _approved_predictive_flow(*, direction="SHORT", probability=0.90, alpha_bps=8.0):
    return PredictiveFlowAssessment(
        ready=True, approved=True, model="pre_move_orderflow_hazard_v1", venue="hyperliquid",
        direction=direction, sample_count=4, prediction_horizon_sec=1.0,
        directional_move_probability=probability, required_directional_probability=0.55,
        predictive_alpha_bps=alpha_bps, queue_imbalance_directional=0.40,
        microprice_lean_directional_bps=0.30, ofi_acceleration_directional_bps=3.0,
        tfi_acceleration_directional_bps=1.0, depletion_advantage=0.45,
        displayed_support_replenishment_proxy=0.10, displayed_opposition_withdrawal_proxy=0.20,
        displayed_support_withdrawal_proxy=0.0, displayed_opposition_replenishment_proxy=0.0,
        near_bid_depth_usd=40000.0, near_ask_depth_usd=10000.0,
        cross_venue_agreement=None, leader_venue=None,
        evidence_authority="pre_move_observable_state_only_depth_changes_are_proxies", reasons=(),
    )


def test_barrier_outcome_model_approves_pre_move_predictor_without_realised_price_confirmation(monkeypatch):
    monkeypatch.setattr("strategy.barrier_outcome.config.BARRIER_OUTCOME_MIN_TARGET_BEFORE_STOP_PROBABILITY_BY_ASSET", {"OIL": 0.45}, raising=False)
    monkeypatch.setattr("strategy.barrier_outcome.config.BARRIER_OUTCOME_ROUND_TRIP_COST_MULTIPLIER", 1.0, raising=False)
    monkeypatch.setattr("strategy.barrier_outcome.config.BARRIER_OUTCOME_MIN_EXPECTED_VALUE_BPS", 0.0, raising=False)
    engine = ProtectedBarrierOutcomeEngine("OIL")
    plan = ProtectionPlan(99.44, 99.74, 98.50, "VENUE_NATIVE_BRACKET", True, [])
    assessed = engine.assess(
        venue="hyperliquid", direction=Direction.SHORT, protection=plan,
        parent_alpha_bps=-18.0, child_timing_contribution_bps=-4.0,
        uncertainty_bps=1.0, route_cost_bps=0.50, robust_volatility_bps=3.0,
        liquidity_score=1.0, execution_quality=1.0,
        predictive_flow=_approved_predictive_flow(),
    )
    assert assessed.flow_response.ready is False  # no post-move observations were required
    assert assessed.predictive_flow.approved is True
    assert assessed.target_before_stop_probability > assessed.required_target_before_stop_probability
    assert assessed.expected_value_bps > 0
    assert assessed.approved is True


def test_realised_absorption_is_telemetry_only_not_a_pre_move_entry_dependency(monkeypatch):
    monkeypatch.setattr("strategy.barrier_outcome.config.BARRIER_OUTCOME_MIN_FLOW_OBSERVATIONS", 5, raising=False)
    monkeypatch.setattr("strategy.barrier_outcome.config.BARRIER_OUTCOME_MIN_TARGET_BEFORE_STOP_PROBABILITY_BY_ASSET", {"OIL": 0.45}, raising=False)
    monkeypatch.setattr("strategy.barrier_outcome.config.BARRIER_OUTCOME_ROUND_TRIP_COST_MULTIPLIER", 1.0, raising=False)
    monkeypatch.setattr("strategy.barrier_outcome.config.BARRIER_OUTCOME_MIN_EXPECTED_VALUE_BPS", 0.0, raising=False)
    engine = ProtectedBarrierOutcomeEngine("OIL")
    for i in range(8):
        engine.observe({"hyperliquid": _barrier_microstate(100.0 + i * 0.08, -50_000.0, i + 1)})
    plan = ProtectionPlan(100.56, 100.86, 99.62, "VENUE_NATIVE_BRACKET", True, [])
    assessed = engine.assess(
        venue="hyperliquid", direction=Direction.SHORT, protection=plan,
        parent_alpha_bps=-18.0, child_timing_contribution_bps=-4.0,
        uncertainty_bps=1.0, route_cost_bps=0.50, robust_volatility_bps=3.0,
        liquidity_score=1.0, execution_quality=1.0,
        predictive_flow=_approved_predictive_flow(),
    )
    assert assessed.flow_response.absorption_probability > 0.75
    assert assessed.approved is True
    assert assessed.as_dict()["flow_response_role"] == "post_entry_toxicity_and_calibration_only"

def test_hyperliquid_exit_fill_never_uses_stop_safety_limit_as_realised_price(monkeypatch):
    from execution.order_manager import _HyperliquidAdapter

    class _API:
        def query_order(self, oid):
            return {"order": {"status": "filled", "order": {"oid": int(oid), "coin": "xyz:CL", "limitPx": "99.49", "origSz": "0.251"}}}
        def user_fills_by_time(self, **kwargs):
            return [{"oid": 123, "coin": "xyz:CL", "px": "90.445", "sz": "0.251", "fee": "0.001"}]

    inst = SimpleNamespace(symbol="xyz:CL", display_symbol="xyz:CL", tick_size=0.001, lot_step=0.001, min_qty=0.001, max_qty=1000)
    adapter = _HyperliquidAdapter(_API(), inst)
    adapter.limiter.wait = lambda: None
    resolved = adapter.resolve_order_execution("123")
    assert resolved["status"] == "FILLED"
    assert resolved["fill_price"] == 90.445
    assert resolved["fill_price"] != 99.49
    assert resolved["fill_price_authority"] == "user_fills_vwap"


def test_hyperliquid_filled_trigger_without_exact_fill_stays_unpriced(monkeypatch):
    from execution.order_manager import _HyperliquidAdapter

    class _API:
        def query_order(self, oid):
            return {"order": {"status": "filled", "order": {"oid": int(oid), "coin": "xyz:CL", "limitPx": "99.49", "origSz": "0.251"}}}
        def user_fills_by_time(self, **kwargs):
            return []

    inst = SimpleNamespace(symbol="xyz:CL", display_symbol="xyz:CL", tick_size=0.001, lot_step=0.001, min_qty=0.001, max_qty=1000)
    adapter = _HyperliquidAdapter(_API(), inst)
    adapter.limiter.wait = lambda: None
    resolved = adapter.resolve_order_execution("123")
    assert resolved["status"] == "FILLED"
    assert resolved["fill_price"] == 0.0
    assert resolved["fill_price_authority"] == "order_status_execution_fields_only"

# ── V19 causal setup-family and protected-profit-lock regressions ─────────────

from strategy.setup_classifier import InstitutionalSetupClassifier
from strategy.profit_lock import InstitutionalProfitLockEngine
from intelligence.venue_market_state import VenueMarketState
from strategy.barrier_outcome import BarrierOutcomeAssessment, FlowResponseEstimate


def _setup_barrier(*, approved=True, parent=-10.0, predictive=None):
    flow = FlowResponseEstimate(
        ready=False, sample_count=0, direction="SHORT", flow_effectiveness=0.0,
        continuation_probability=0.0, absorption_probability=1.0,
        aligned_flow_observations=0, directional_mid_displacement_bps=0.0,
        latest_directional_flow_bps=0.0, reason="post_entry_flow_warmup",
    )
    pred = predictive or _approved_predictive_flow()
    return BarrierOutcomeAssessment(
        ready=True, approved=approved, model="predictive_protected_barrier_outcome_v2", direction="SHORT",
        stop_distance_bps=25.0, target_distance_bps=55.0, route_cost_bps=3.0,
        round_trip_cost_reserve_bps=6.0, parent_alpha_bps=parent,
        child_timing_contribution_bps=-2.0, uncertainty_bps=0.5,
        robust_volatility_bps=3.0, drift_after_predictive_timing_bps=6.0,
        raw_target_before_stop_probability=0.75, target_before_stop_probability=0.70,
        required_target_before_stop_probability=0.60, expected_value_bps=12.0,
        predictive_flow=pred, flow_response=flow, reasons=(),
    )


def _setup_market_state(*, alpha=-10.0, regime="TREND", acceptance=-1.0):
    return VenueMarketState(
        venue="hyperliquid", symbol="xyz:CL", ready=True, reason="venue_local_structural_state_ready",
        signed_alpha_bps=alpha, confidence=0.80, uncertainty_bps=0.5, regime_label=regime,
        returns_bps={}, robust_one_minute_vol_bps=3.0, volatility_expansion_ratio=1.5,
        acceptance_bps=acceptance, live_impulse_bps={}, diagnostics={"parent_state_id": "state-1"},
    )


def test_setup_classifier_allows_pre_move_queue_depletion_entry(monkeypatch):
    monkeypatch.setattr("strategy.setup_classifier.config.SETUP_CLASSIFIER_MIN_DIRECTIONAL_PARENT_ALPHA_BPS", 0.5, raising=False)
    classifier = InstitutionalSetupClassifier("OIL")
    pred = _approved_predictive_flow()
    assessed = classifier.classify(
        venue="hyperliquid", direction=Direction.SHORT,
        market_state=_setup_market_state(alpha=-10.0, regime="TREND", acceptance=0.0),
        barrier=_setup_barrier(predictive=pred), predictive_flow=pred, cross_venue_evidence=None,
    )
    assert assessed.approved is True
    assert assessed.setup_family == "PREMOVE_QUEUE_DEPLETION_INITIATION"
    assert assessed.forced_flow_evidence_available is False


def test_setup_classifier_rejects_unclassified_predictive_candidate():
    classifier = InstitutionalSetupClassifier("OIL")
    rejected = PredictiveFlowAssessment(
        ready=True, approved=False, model="pre_move_orderflow_hazard_v1", venue="hyperliquid", direction="SHORT",
        sample_count=4, prediction_horizon_sec=1.0, directional_move_probability=0.42,
        required_directional_probability=0.61, predictive_alpha_bps=0.0, queue_imbalance_directional=-0.2,
        microprice_lean_directional_bps=-0.4, ofi_acceleration_directional_bps=-1.0,
        tfi_acceleration_directional_bps=-1.0, depletion_advantage=-0.3,
        displayed_support_replenishment_proxy=0.0, displayed_opposition_withdrawal_proxy=0.0,
        displayed_support_withdrawal_proxy=0.2, displayed_opposition_replenishment_proxy=0.2,
        near_bid_depth_usd=10000.0, near_ask_depth_usd=40000.0, cross_venue_agreement=None,
        leader_venue=None, evidence_authority="pre_move_observable_state_only_depth_changes_are_proxies",
        reasons=("pre_move_probability_insufficient:0.4200<0.6100",),
    )
    assessed = classifier.classify(
        venue="hyperliquid", direction=Direction.SHORT,
        market_state=_setup_market_state(alpha=-10.0, regime="TREND", acceptance=0.0),
        barrier=_setup_barrier(predictive=rejected), predictive_flow=rejected, cross_venue_evidence=None,
    )
    assert assessed.approved is False
    assert assessed.setup_family == "NO_TRADE_UNCLASSIFIED_EDGE"
    assert any("pre_move_probability_insufficient" in r for r in assessed.reasons)

def test_profit_lock_does_not_claim_initial_stop_is_profitable(monkeypatch):
    monkeypatch.setattr("strategy.profit_lock.config.PROFIT_LOCK_EXIT_SLIPPAGE_RESERVE_BPS_BY_ASSET", {"OIL": 7.0}, raising=False)
    engine = InstitutionalProfitLockEngine("OIL")
    assessed = engine.assess(
        side="short", entry_price=100.0, mark_price=99.90, initial_stop_price=100.40,
        current_stop_price=100.40, route_cost_bps=5.0, spread_bps=0.5, protection_confirmed=True,
    )
    assert assessed.should_request is False
    assert assessed.execution_envelope_only is True
    assert assessed.requested_stop_price == 100.40


def test_profit_lock_moves_short_stop_below_entry_only_after_cost_covered(monkeypatch):
    monkeypatch.setattr("strategy.profit_lock.config.PROFIT_LOCK_EXIT_SLIPPAGE_RESERVE_BPS_BY_ASSET", {"OIL": 5.0}, raising=False)
    monkeypatch.setattr("strategy.profit_lock.config.PROFIT_LOCK_MIN_ACTIVATION_R", 0.50, raising=False)
    engine = InstitutionalProfitLockEngine("OIL")
    assessed = engine.assess(
        side="short", entry_price=100.0, mark_price=99.20, initial_stop_price=100.40,
        current_stop_price=100.40, route_cost_bps=3.0, spread_bps=0.5, protection_confirmed=True,
    )
    assert assessed.should_request is True
    assert assessed.requested_stop_price < 100.0
    assert assessed.protected_net_floor_bps > 0.0
    assert assessed.locked_price_move_bps >= assessed.protected_net_floor_bps


def test_hyperliquid_profit_lock_modifies_native_trigger_without_cancel_replace():
    from execution.order_manager import _HyperliquidAdapter

    class _API:
        def __init__(self):
            self.calls = []
        def round_price(self, coin, price):
            return float(price)
        def round_size(self, coin, size):
            return float(size)
        def modify_reduce_only_trigger(self, **kwargs):
            self.calls.append(kwargs)
            return {"response": {"data": {"statuses": ["waitingForTrigger"]}}}

    api = _API()
    inst = SimpleNamespace(symbol="xyz:CL", display_symbol="xyz:CL", tick_size=0.001, lot_step=0.001, min_qty=0.001, max_qty=1000)
    adapter = _HyperliquidAdapter(api, inst)
    adapter.limiter.wait = lambda: None
    updated = adapter.edit_protective_stop(order_id="123", side="BUY", quantity=0.251, new_stop_price=90.10)
    assert updated["order_id"] == "123"
    assert updated["native_trigger_modified"] is True
    assert api.calls == [{"coin": "xyz:CL", "oid": 123, "is_buy": True, "size": 0.251, "trigger_px": 90.1, "tpsl": "sl"}]


def test_hyperliquid_api_native_trigger_modify_uses_reduce_only_trigger_payload():
    from exchanges.hyperliquid.api import HyperliquidAPI

    class _Exchange:
        def __init__(self):
            self.args = None
        def modify_order(self, *args, **kwargs):
            self.args = (args, kwargs)
            return {"response": {"data": {"statuses": ["waitingForTrigger"]}}}

    api = object.__new__(HyperliquidAPI)
    api.exchange = _Exchange()
    api.round_size = lambda coin, qty: qty
    api.round_price = lambda coin, px: px
    out = api.modify_reduce_only_trigger(coin="xyz:CL", oid=7, is_buy=True, size=0.251, trigger_px=90.10, tpsl="sl")
    args, kwargs = api.exchange.args
    assert args[:4] == (7, "xyz:CL", True, 0.251)
    assert kwargs["reduce_only"] is True
    assert kwargs["order_type"]["trigger"]["tpsl"] == "sl"
    assert out["response"]["data"]["statuses"][0] == "waitingForTrigger"


# ── V20 pre-displacement predictive entry and startup-exposure regressions ───
from dataclasses import replace as dataclass_replace


def test_pre_move_orderflow_can_approve_before_midprice_displacement(monkeypatch):
    monkeypatch.setattr("strategy.predictive_flow.config.PREDICTIVE_FLOW_MIN_DIRECTIONAL_PROBABILITY_BY_ASSET", {"OIL": 0.52}, raising=False)
    monkeypatch.setattr("strategy.predictive_flow.config.PREDICTIVE_FLOW_MIN_DEPLETION_ADVANTAGE", -1.0, raising=False)
    engine = PreMoveOrderFlowHazardEngine("OIL")
    for i in range(4):
        state = dataclass_replace(
            _barrier_microstate(100.0, 5000.0 * (i + 1), i + 1),
            microprice=100.01,
            bid_depth_usd_by_band={"0-1": 90000.0 + 10000 * i, "1-3": 40000.0},
            ask_depth_usd_by_band={"0-1": 50000.0 - 10000 * i, "1-3": 20000.0},
            tfi_usd_1s=3000.0 * (i + 1), tfi_usd_10s=3000.0,
        )
        engine.observe({"hyperliquid": state})
    assessed = engine.assess(venue="hyperliquid", direction=Direction.LONG)
    assert assessed.ready is True
    assert assessed.approved is True
    assert assessed.directional_move_probability >= assessed.required_directional_probability
    assert all(abs(row.mid - 100.0) < 1e-12 for row in engine._snapshots["hyperliquid"])


def test_pre_move_orderflow_rejects_pressure_against_parent_before_move(monkeypatch):
    monkeypatch.setattr("strategy.predictive_flow.config.PREDICTIVE_FLOW_MIN_DEPLETION_ADVANTAGE", -1.0, raising=False)
    engine = PreMoveOrderFlowHazardEngine("OIL")
    for i in range(4):
        state = dataclass_replace(
            _barrier_microstate(100.0, 8000.0 * (i + 1), i + 1),
            microprice=100.01,
            bid_depth_usd_by_band={"0-1": 120000.0, "1-3": 40000.0},
            ask_depth_usd_by_band={"0-1": 20000.0, "1-3": 10000.0},
        )
        engine.observe({"hyperliquid": state})
    assessed = engine.assess(venue="hyperliquid", direction=Direction.SHORT)
    assert assessed.ready is True
    assert assessed.approved is False


def test_startup_external_hyperliquid_locked_collateral_blocks_fresh_entries(monkeypatch):
    from orchestration.multi_asset_bot import MultiAssetInstitutionalBot
    manager = SimpleNamespace(
        symbol="xyz:CL",
        get_open_position=lambda: {"size": 0.0},
        get_open_orders=lambda symbol=None: [],
        get_balance=lambda: {"available": 14.90, "locked": 0.6455, "total": 15.55},
    )
    router = SimpleNamespace(available_exchanges=lambda: ("hyperliquid",), manager_for=lambda venue: manager)
    ctx = SimpleNamespace(ready=True, execution_router=router, instrument=SimpleNamespace(asset_id="OIL", display_symbol="xyz:CL"), startup_exposure_verified=False)
    bot = MultiAssetInstitutionalBot(); bot.contexts = [ctx]
    monkeypatch.setattr(bot, "_is_indian_options_context", lambda _ctx: False)
    assert bot._startup_external_exposure_preflight() is False
    assert bot.trading_enabled is False
    assert bot.trading_pause_reason == "STARTUP_EXTERNAL_EXPOSURE_RECONCILIATION_REQUIRED"


def test_startup_flat_verified_account_leaves_entries_enabled(monkeypatch):
    from orchestration.multi_asset_bot import MultiAssetInstitutionalBot
    manager = SimpleNamespace(
        symbol="xyz:CL", get_open_position=lambda: {"size": 0.0},
        get_open_orders=lambda symbol=None: [], get_balance=lambda: {"available": 15.55, "locked": 0.0, "total": 15.55},
    )
    router = SimpleNamespace(available_exchanges=lambda: ("hyperliquid",), manager_for=lambda venue: manager)
    ctx = SimpleNamespace(ready=True, execution_router=router, instrument=SimpleNamespace(asset_id="OIL", display_symbol="xyz:CL"), startup_exposure_verified=False)
    bot = MultiAssetInstitutionalBot(); bot.contexts = [ctx]
    monkeypatch.setattr(bot, "_is_indian_options_context", lambda _ctx: False)
    assert bot._startup_external_exposure_preflight() is True
    assert bot.trading_enabled is True


def test_slvon_route_penalty_is_explicit_in_auditable_cost_components(monkeypatch):
    monkeypatch.setattr("execution.venue_selection._cfg", lambda name, default: {
        "VENUE_ROUND_TRIP_FEE_BPS": {"delta": 3.0}, "VENUE_SLIPPAGE_IMPACT_MULTIPLIER": 0.0,
        "SILVER_DELTA_MIN_NEAR_DEPTH_USD": 50000.0, "SILVER_DELTA_ILLIQUIDITY_PENALTY_BPS": 15.0,
    }.get(name, default))
    state = _state__test_institutional_v6_exposure_feed_execution_fixes("delta", "SLVONUSD", 67.395)
    state = dataclass_replace(state, bid_depth_usd_by_band={"0-1": 50.0, "1-3": 25.0}, ask_depth_usd_by_band={"0-1": 50.0, "1-3": 25.0})
    est = estimate_venue_cost(state=state, direction="SHORT", asset_id="SILVER_SLVON", reference_mid=67.395, notional_usd=10.0, routeable=True, available_cash_usd=100.0, required_margin_usd=1.0, protection_capable=True, gross_edge_bps=30.0)
    assert est.preference_adjustment_bps == 15.0
    assert est.total_cost_bps >= est.fee_bps + est.preference_adjustment_bps

# ── V20 calibrated live-authority and predictive barrier labelling ───────────
from strategy.calibration_gate import PredictiveCalibrationAuthority
from research.store import JsonlResearchStore, PredictiveBarrierLabelWriter


def test_predictive_calibration_missing_model_is_shadow_only(monkeypatch):
    monkeypatch.setattr("strategy.calibration_gate.config.PREDICTIVE_CALIBRATION_REQUIRE_FOR_LIVE", True, raising=False)
    monkeypatch.setattr("strategy.calibration_gate.config.PREDICTIVE_CALIBRATED_LIVE_MODELS", {}, raising=False)
    assessed = PredictiveCalibrationAuthority("OIL").assess(
        venue="hyperliquid", setup_family="PREMOVE_QUEUE_DEPLETION_INITIATION",
        model_version="pre_move_orderflow_hazard_v1+predictive_bracket_viability_v3",
    )
    assert assessed.authorised_for_live is False
    assert assessed.authority == "shadow_only_until_walk_forward_calibrated"
    assert "OIL:hyperliquid:PREMOVE_QUEUE_DEPLETION_INITIATION" in assessed.reasons[0]


def test_predictive_calibration_allows_only_approved_walk_forward_model(monkeypatch):
    key = "OIL:hyperliquid:PREMOVE_QUEUE_DEPLETION_INITIATION"
    version = "pre_move_orderflow_hazard_v1+predictive_bracket_viability_v3"
    monkeypatch.setattr("strategy.calibration_gate.config.PREDICTIVE_CALIBRATION_REQUIRE_FOR_LIVE", True, raising=False)
    monkeypatch.setattr("strategy.calibration_gate.config.PREDICTIVE_CALIBRATION_MIN_OUT_OF_SAMPLE_OBSERVATIONS", 100, raising=False)
    monkeypatch.setattr("strategy.calibration_gate.config.PREDICTIVE_CALIBRATION_MIN_LOWER_CONFIDENCE_BY_ASSET", {"OIL": 0.60}, raising=False)
    monkeypatch.setattr("strategy.calibration_gate.config.PREDICTIVE_CALIBRATED_LIVE_MODELS", {
        key: {"model_version": version, "out_of_sample_observations": 250, "walk_forward_validated": True, "brier_score": 0.15, "lower_confidence_tp_before_sl": 0.64}
    }, raising=False)
    assessed = PredictiveCalibrationAuthority("OIL").assess(venue="hyperliquid", setup_family="PREMOVE_QUEUE_DEPLETION_INITIATION", model_version=version)
    assert assessed.authorised_for_live is True
    assert assessed.authority == "approved_walk_forward_calibration"


def test_predictive_barrier_label_writer_labels_shadow_candidate_tp_first(tmp_path):
    store = JsonlResearchStore(tmp_path)
    writer = PredictiveBarrierLabelWriter(store, min_spacing_sec=0.0, timeout_s=30)
    assert writer.record_candidate(
        observation_ts_ns=1_000_000_000, asset_id="OIL", venue="hyperliquid", instrument="xyz:CL",
        candidate_id="candidate-1", model_key="OIL:hyperliquid:PREMOVE_QUEUE_DEPLETION_INITIATION",
        setup_family="PREMOVE_QUEUE_DEPLETION_INITIATION", side="short", entry_price=100.0,
        stop_price=101.0, target_price=98.0, estimated_round_trip_cost_bps=7.0,
        predicted_target_before_stop_probability=0.66, predicted_directional_move_probability=0.64,
    ) is True
    paths = writer.observe(now_ts_ns=2_000_000_000, current_price=97.9)
    assert len(paths) == 1
    rows = store.read_records("predictive_barrier_labels.jsonl")
    assert rows[0]["outcome"] == "TP_FIRST"
    assert rows[0]["model_key"] == "OIL:hyperliquid:PREMOVE_QUEUE_DEPLETION_INITIATION"
    assert rows[0]["predicted_target_before_stop_probability"] == 0.66


def test_predictive_hazard_telemetry_states_calibration_authority():
    payload = _approved_predictive_flow().as_dict()
    assert payload["probability_authority"] == "analytic_hazard_score_requires_walk_forward_calibration_for_live"


def test_walk_forward_calibration_tool_promotes_only_sufficient_holdout(tmp_path):
    import importlib.util
    tool_path = PROJECT_ROOT / "tools" / "calibrate_predictive_setups.py"
    spec = importlib.util.spec_from_file_location("calibrate_predictive_setups", tool_path)
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    labels = tmp_path / "labels.jsonl"
    key = "OIL:hyperliquid:PREMOVE_QUEUE_DEPLETION_INITIATION"
    rows = []
    for i in range(300):
        rows.append({
            "model_key": key, "observation_ts_ns": i + 1,
            "outcome": "TP_FIRST" if i % 10 != 0 else "SL_FIRST",
            "predicted_target_before_stop_probability": 0.90,
        })
    labels.write_text("\n".join(__import__("json").dumps(r) for r in rows) + "\n", encoding="utf-8")
    report = module.run(labels, min_obs=100, max_brier=0.20, min_lower=0.70, holdout_fraction=0.40)
    assert key in report["approved_registry"]
    assert report["approved_registry"][key]["walk_forward_validated"] is True


def test_pre_size_route_ledger_uses_bounded_probe_not_full_slvon_broker_capacity(monkeypatch):
    monkeypatch.setattr("strategy.institutional_strategy._cfg", lambda name, default: {
        "VENUE_SELECTION_PRE_SIZE_REFERENCE_NOTIONAL_USD": 50.0,
        "VENUE_SELECTION_PRE_SIZE_REFERENCE_NOTIONAL_USD_BY_ASSET": {"SILVER_SLVON": 25.0},
        "VENUE_SELECTION_MAX_QUANTITY_REPRESENTATION_ERROR_BPS": 0.5,
        "VENUE_SELECTION_MIN_FREE_MARGIN_USD": 1.0,
        "LEVERAGE": 25.0,
        "INSTITUTIONAL_MAX_SELECTED_LEVERAGE": 25.0,
    }.get(name, default))
    strategy = object.__new__(InstitutionalStrategy)
    strategy._asset_id = "SILVER_SLVON"
    strategy._venue_min_order_notional_usd = lambda venue: 10.0
    strategy._venue_max_leverage = lambda venue: 25.0
    strategy._symbol_for_venue = lambda venue, fallback: "SLVONUSD"
    strategy._instrument_mapping = lambda symbol, venue=None: SimpleNamespace(qty_step=0.001)
    state = _state__test_institutional_v6_exposure_feed_execution_fixes("delta", "SLVONUSD", 67.395)
    qty, notionals, _margins, _diag, _exec = strategy._risk_normalised_route_inputs(
        states={"delta": state}, candidate_venues={"delta"},
        capacity_notional_by_venue={"delta": 2552.34}, approved_quantity=None,
    )
    assert qty > 0
    assert notionals["delta"] == pytest.approx(25.0)
    assert notionals["delta"] < 2552.34

# ── V21 adaptive structural TP/SL and post-fill performance exits ───────────
from strategy.position_performance import PostFillPerformanceExitEngine


def _relax_adaptive_protection_warmup(monkeypatch):
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_REQUIRE_SIGNAL_DECAY_READY", False, raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_VENUES", (), raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_VENUES", (), raising=False)
    monkeypatch.setattr(dp.config, "ADAPTIVE_PROTECTION_REGIME_STOP_MULTIPLIER", {"TREND": 1.0}, raising=False)
    monkeypatch.setattr(dp.config, "ADAPTIVE_PROTECTION_VOL_EXPANSION_STOP_SLOPE", 0.0, raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_ASSET_MIN_STOP_BPS", {}, raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_VENUE_ASSET_MIN_STOP_BPS", {}, raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_ASSET_MIN_TARGET_BPS", {}, raising=False)


def test_adaptive_long_sl_is_beyond_closed_support_and_tp_front_runs_objective(monkeypatch):
    _relax_adaptive_protection_warmup(monkeypatch)
    engine = DynamicProtectionPlanBuilder("OIL")
    plan = engine.build_plan(
        direction=Direction.LONG, entry_price=100.0, volatility_price=0.20,
        gross_edge_bps=30.0, execution_cost_bps=2.0,
        protection_type="VENUE_NATIVE_BRACKET", asset_class="commodity",
        position_notional=100.0, quantity=1.0,
        market_state={
            "asset_id": "OIL", "venue": "hyperliquid", "regime": "TREND",
            "spread_bps": 0.2, "price_tick": 0.01, "near_touch_depth_usd": 100000.0,
            "last_closed_low": 99.50, "prior_range_low": 99.00,
            "last_closed_high": 101.50, "prior_range_high": 102.00,
            "closed_anchor_source": "venue_local_closed_1m_range", "volatility_expansion_ratio": 1.0,
        },
    )
    geometry = plan.diagnostics["market_geometry"]
    assert plan.protection_feasible is True
    assert geometry["structural_anchor_price"] == pytest.approx(99.50)
    assert plan.stop_price < 99.50  # outside support, never inside it
    assert geometry["target_source"] == "front_run_nearest_closed_sell_side_objective"
    assert plan.target_price < 101.50
    assert plan.target_price > plan.entry_price


def test_adaptive_short_sl_is_beyond_closed_resistance_and_tp_front_runs_objective(monkeypatch):
    _relax_adaptive_protection_warmup(monkeypatch)
    engine = DynamicProtectionPlanBuilder("OIL")
    plan = engine.build_plan(
        direction=Direction.SHORT, entry_price=100.0, volatility_price=0.20,
        gross_edge_bps=30.0, execution_cost_bps=2.0,
        protection_type="VENUE_NATIVE_BRACKET", asset_class="commodity",
        position_notional=100.0, quantity=1.0,
        market_state={
            "asset_id": "OIL", "venue": "hyperliquid", "regime": "TREND",
            "spread_bps": 0.2, "price_tick": 0.01, "near_touch_depth_usd": 100000.0,
            "last_closed_high": 100.60, "prior_range_high": 101.20,
            "last_closed_low": 98.70, "prior_range_low": 98.00,
            "closed_anchor_source": "venue_local_closed_1m_range", "volatility_expansion_ratio": 1.0,
        },
    )
    geometry = plan.diagnostics["market_geometry"]
    assert geometry["structural_anchor_price"] == pytest.approx(100.60)
    assert plan.stop_price > 100.60
    assert geometry["target_source"] == "front_run_nearest_closed_buy_side_objective"
    assert plan.target_price > 98.70
    assert plan.target_price < plan.entry_price


def test_post_fill_performance_exit_cuts_confirmed_early_adverse_selection(monkeypatch):
    import strategy.position_performance as ppe
    monkeypatch.setattr(ppe.config, "POST_FILL_PERFORMANCE_EXIT_CONFIRMATION_INTERVAL_SEC", 0.0, raising=False)
    monkeypatch.setattr(ppe.config, "POST_FILL_PERFORMANCE_EXIT_ADVERSE_TRIGGER_R", 0.20, raising=False)
    monkeypatch.setattr(ppe.config, "POST_FILL_PERFORMANCE_EXIT_MIN_ADVERSE_BPS", 2.0, raising=False)
    engine = PostFillPerformanceExitEngine("BTC")
    kwargs = dict(
        side="long", entry_price=100.0, mark_price=99.70, initial_stop_price=99.0,
        route_cost_bps=2.0, spread_bps=0.2, protection_confirmed=True,
        same_direction_probability=0.30, opposing_direction_probability=0.80,
        same_direction_ready=True, opposing_direction_ready=True,
    )
    first = engine.assess(**kwargs)
    second = engine.assess(**kwargs)
    assert first.should_exit is False
    assert second.should_exit is True
    assert second.action == "CUT_CONFIRMED_POST_FILL_ADVERSE_SELECTION"
    assert second.protection_retained_until_flat is True


def test_post_fill_performance_exit_captures_net_profit_on_predictive_reversal(monkeypatch):
    import strategy.position_performance as ppe
    monkeypatch.setattr(ppe.config, "POST_FILL_PERFORMANCE_EXIT_CONFIRMATION_INTERVAL_SEC", 0.0, raising=False)
    engine = PostFillPerformanceExitEngine("OIL")
    kwargs = dict(
        side="short", entry_price=100.0, mark_price=99.50, initial_stop_price=101.0,
        route_cost_bps=2.0, spread_bps=0.2, protection_confirmed=True,
        same_direction_probability=0.30, opposing_direction_probability=0.82,
        same_direction_ready=True, opposing_direction_ready=True,
    )
    engine.assess(**kwargs)
    assessed = engine.assess(**kwargs)
    assert assessed.should_exit is True
    assert assessed.action == "CAPTURE_NET_PROFIT_ON_PREDICTIVE_EDGE_REVERSAL"
    assert assessed.net_mark_after_reserve_bps > 0


def test_post_fill_performance_exit_does_not_close_when_entry_hazard_still_supported(monkeypatch):
    import strategy.position_performance as ppe
    monkeypatch.setattr(ppe.config, "POST_FILL_PERFORMANCE_EXIT_CONFIRMATION_INTERVAL_SEC", 0.0, raising=False)
    engine = PostFillPerformanceExitEngine("OIL")
    assessed = engine.assess(
        side="short", entry_price=100.0, mark_price=100.40, initial_stop_price=101.0,
        route_cost_bps=2.0, spread_bps=0.2, protection_confirmed=True,
        same_direction_probability=0.72, opposing_direction_probability=0.30,
        same_direction_ready=True, opposing_direction_ready=True,
    )
    assert assessed.should_exit is False
    assert assessed.action == "HOLD"


def test_groww_bearish_thesis_long_put_uses_long_premium_protection_geometry(monkeypatch):
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_REQUIRE_SIGNAL_DECAY_READY", False, raising=False)
    strategy = InstitutionalStrategy(instrument=None)
    class _PremiumData:
        def get_execution_candles(self, timeframe, limit):
            assert timeframe == "1m"
            return [{"high": 102.0, "low": 98.0, "close": 100.0} for _ in range(20)]
    plan = strategy._option_premium_protection_plan(
        _PremiumData(), 100.0, Direction.BEARISH, 50.0, 2.0,
        option_state={"selected_option_symbol": "NIFTY_PUT", "selected_option_delta": -0.45},
    )
    assert plan is not None and plan.protection_feasible is True
    assert plan.stop_price < plan.entry_price < plan.target_price
    assert plan.diagnostics["option_state_at_entry"]["underlying_thesis_direction"] == "BEARISH"
    assert plan.diagnostics["option_state_at_entry"]["premium_position_side"] == "LONG_PREMIUM_BUY_ONLY"

# ── V22 exposure quarantine observability / re-authorisation regressions ──────
def test_external_exposure_quarantine_does_not_suppress_market_calculations(monkeypatch):
    from orchestration.multi_asset_bot import MultiAssetInstitutionalBot
    calls = []
    class _Strategy:
        def on_tick(self, data, router, risk, now_ms, event_driven=False):
            calls.append((now_ms, event_driven))
            bot.running = False
        def get_position(self):
            return None
    instrument = SimpleNamespace(asset_id="OIL", primary_exchange=SimpleNamespace(value="hyperliquid"), display_symbol="xyz:CL")
    ctx = SimpleNamespace(
        ready=True, instrument=instrument, strategy=_Strategy(), data_manager=SimpleNamespace(get_last_price=lambda: 90.0),
        execution_router=SimpleNamespace(), risk_manager=SimpleNamespace(), has_position=False,
        last_tick_time=0.0, last_analysis_sec=time.time(), last_heartbeat_sec=time.time(),
    )
    bot = MultiAssetInstitutionalBot()
    bot.contexts = [ctx]
    bot.running = True
    bot.trading_enabled = False
    bot.trading_pause_reason = "STARTUP_EXTERNAL_EXPOSURE_RECONCILIATION_REQUIRED"
    bot.guard = SimpleNamespace(
        evaluation_interval=lambda _ctx: 0.01,
        can_evaluate_entry=lambda _ctx, _contexts: (True, "portfolio slot available"),
        count_open=lambda _contexts: 0, max_open_positions=5,
        report_line=lambda _ctx: "policy=test",
    )
    monkeypatch.setattr(bot, "_is_indian_options_context", lambda _ctx: False)
    bot._run_context_worker(ctx)
    assert calls and calls[0][1] is True


def test_external_exposure_quarantine_blocks_submission_not_analytics(monkeypatch):
    from orchestration.multi_asset_bot import MultiAssetInstitutionalBot
    ctx = SimpleNamespace(
        instrument=SimpleNamespace(asset_id="GOLD_PAXG"), startup_exposure_verified=False,
        startup_exposure_reason="delta:PAXGUSD:existing_position:SHORT:0.048",
    )
    bot = MultiAssetInstitutionalBot()
    bot.trading_enabled = False
    bot.trading_pause_reason = "STARTUP_EXTERNAL_EXPOSURE_RECONCILIATION_REQUIRED"
    allowed, reason = bot._submission_gate_for_context(ctx)
    assert allowed is False
    assert reason == "STARTUP_EXTERNAL_EXPOSURE_RECONCILIATION_REQUIRED"


def test_external_exposure_recheck_reauthorises_submissions_after_position_closes(monkeypatch):
    from orchestration.multi_asset_bot import MultiAssetInstitutionalBot
    live = {"position": {"size": 0.048, "side": "SHORT"}, "orders": [{"id": "sl"}, {"id": "tp"}]}
    manager = SimpleNamespace(
        symbol="PAXGUSD",
        get_open_position=lambda: dict(live["position"]),
        get_open_orders=lambda symbol=None: list(live["orders"]),
    )
    router = SimpleNamespace(available_exchanges=lambda: ("delta",), manager_for=lambda venue: manager)
    ctx = SimpleNamespace(
        ready=True, execution_router=router,
        instrument=SimpleNamespace(asset_id="GOLD_PAXG", display_symbol="PAXGUSD"),
        startup_exposure_verified=False, startup_exposure_reason="",
    )
    bot = MultiAssetInstitutionalBot(); bot.contexts = [ctx]
    monkeypatch.setattr(bot, "_is_indian_options_context", lambda _ctx: False)
    assert bot._startup_external_exposure_preflight() is False
    assert bot.trading_enabled is False
    live["position"] = {"size": 0.0}
    live["orders"] = []
    bot._last_startup_exposure_recheck_sec = 0.0
    bot._maybe_recheck_external_exposure_pause()
    assert bot.trading_enabled is True
    assert bot.trading_pause_reason == ""
    assert ctx.startup_exposure_verified is True

# ── V23 calculation-integrity rebuild regressions ────────────────────────────
from strategy.liquidity_map import select_protection_pools


def test_v23_hyperliquid_hip3_precision_does_not_default_to_one_cent():
    report = InstrumentRegistry(execution_preference="").discover(
        hyperliquid_api=_FakeHyperCatalog(), requested=config.MULTI_ASSET_REQUESTS,
        include_exchanges=("hyperliquid",), require_primary=False, max_active=20,
    )
    natgas = next(inst for inst in report.matched if inst.asset_id == "NATGAS")
    assert natgas.primary.tick_size == pytest.approx(0.0)
    assert _hyperliquid_price_increment(3.2765, 0.01) == pytest.approx(0.0001)
    assert _hyperliquid_price_increment(90.1265, 0.01) == pytest.approx(0.001)
    from execution.order_manager import _HyperliquidAdapter
    adapter = _HyperliquidAdapter(SimpleNamespace(), SimpleNamespace(symbol="xyz:NATGAS", display_symbol="xyz:NATGAS", tick_size=0.0, lot_step=0.01, min_qty=0.0, max_qty=0.0))
    assert adapter.tick_size == pytest.approx(0.0)


def test_v23_multitimeframe_liquidity_selector_skips_too_close_tp_pool():
    pools = [
        {"side": "BSL", "price": 100.02, "strength": 2.0, "max_timeframe": "5m"},
        {"side": "BSL", "price": 101.20, "strength": 4.0, "max_timeframe": "1h"},
        {"side": "SSL", "price": 99.30, "strength": 4.0, "max_timeframe": "1h"},
    ]
    stop, target = select_protection_pools(pools, direction="LONG", entry_price=100.0, min_objective_distance=0.50)
    assert stop["price"] == pytest.approx(99.30)
    assert target["price"] == pytest.approx(101.20)


def test_v23_live_market_adaptive_protection_requires_mtf_economic_objective(monkeypatch):
    _relax_adaptive_protection_warmup(monkeypatch)
    monkeypatch.setattr(dp.config, "ADAPTIVE_PROTECTION_REQUIRE_MTF_INVALIDATION_POOL", True, raising=False)
    monkeypatch.setattr(dp.config, "ADAPTIVE_PROTECTION_REQUIRE_MTF_OBJECTIVE_POOL", True, raising=False)
    engine = DynamicProtectionPlanBuilder("NATGAS")
    plan = engine.build_plan(
        direction=Direction.SHORT, entry_price=3.2765, volatility_price=0.0030,
        gross_edge_bps=90.0, execution_cost_bps=8.0,
        protection_type="VENUE_NATIVE_BRACKET", asset_class="commodity",
        position_notional=10.0, quantity=3.0,
        market_state={
            "asset_id": "NATGAS", "venue": "hyperliquid", "regime": "TREND",
            "venue_market_state_ready": True, "spread_bps": 1.0, "price_tick": 0.0001,
            "near_touch_depth_usd": 100000.0, "volatility_expansion_ratio": 1.0,
            "liquidity_pools": [
                {"side": "BSL", "price": 3.2900, "strength": 4.0, "max_timeframe": "1h"},
                {"side": "SSL", "price": 3.2500, "strength": 4.0, "max_timeframe": "1h"},
            ],
        },
    )
    assert plan.protection_feasible is True
    geom = plan.diagnostics["market_geometry"]
    assert geom["price_tick"] == pytest.approx(0.0001)
    assert geom["target_source"].startswith("front_run_mtf_ssl_objective")
    assert plan.stop_price > plan.entry_price > plan.target_price
    assert geom["realised_target_rr"] > 1.0


def test_v23_barrier_cost_authority_does_not_double_round_trip_route_cost(monkeypatch):
    import strategy.barrier_outcome as bo
    monkeypatch.setattr(bo.config, "BARRIER_OUTCOME_EXIT_TAIL_RESERVE_BPS_BY_ASSET", {"OIL": 1.25}, raising=False)
    monkeypatch.setattr(bo.config, "BARRIER_OUTCOME_MIN_TARGET_BEFORE_STOP_PROBABILITY_BY_ASSET", {"OIL": 0.55}, raising=False)
    monkeypatch.setattr(bo.config, "BARRIER_OUTCOME_MIN_EXPECTED_VALUE_BPS", 0.0, raising=False)
    engine = ProtectedBarrierOutcomeEngine("OIL")
    plan = ProtectionPlan(100.0, 100.4, 99.0, "VENUE_NATIVE_BRACKET", True, [])
    assessed = engine.assess(
        venue="hyperliquid", direction=Direction.SHORT, protection=plan,
        parent_alpha_bps=-20.0, child_timing_contribution_bps=-2.0,
        uncertainty_bps=1.0, route_cost_bps=7.114, robust_volatility_bps=5.0,
        liquidity_score=1.0, execution_quality=1.0,
        predictive_flow=_approved_predictive_flow(probability=0.80),
    )
    assert assessed.round_trip_cost_reserve_bps == pytest.approx(8.364)
    payload = assessed.as_dict()
    assert payload["cost_authority"] == "route_total_already_includes_round_trip_fee_plus_explicit_tail_reserve"
    assert payload["score_authority"] == "analytic_bracket_viability_score_not_calibrated_probability"
    assert assessed.target_before_stop_probability == pytest.approx(0.80)


def test_v23_shadow_label_is_one_event_and_resolves_on_selected_venue_only(tmp_path):
    store = JsonlResearchStore(tmp_path)
    writer = PredictiveBarrierLabelWriter(store, min_spacing_sec=0.0, timeout_s=30)
    common = dict(
        observation_ts_ns=1_000_000_000, asset_id="BTC", venue="hyperliquid", instrument="BTC",
        model_key="BTC:hyperliquid:PREMOVE_QUEUE_DEPLETION_INITIATION", setup_family="PREMOVE_QUEUE_DEPLETION_INITIATION",
        side="long", entry_price=100.0, stop_price=99.0, target_price=102.0,
        estimated_round_trip_cost_bps=8.0, predicted_target_before_stop_probability=0.70,
        predicted_directional_move_probability=0.70, parent_state_id="state-1", event_key="state-1",
        prediction_authority="uncalibrated_analytic_bracket_score_shadow_only",
    )
    assert writer.record_candidate(candidate_id="c1", **common) is True
    assert writer.record_candidate(candidate_id="c2", **common) is False
    assert writer.observe(now_ts_ns=2_000_000_000, current_price=103.0, venue="delta", instrument="BTCUSD") == []
    paths = writer.observe(now_ts_ns=2_000_000_000, current_price=102.1, venue="hyperliquid", instrument="BTC")
    assert len(paths) == 1
    row = store.read_records("predictive_barrier_labels.jsonl")[0]
    assert row["outcome"] == "TP_FIRST"
    assert row["parent_state_id"] == "state-1"
    assert row["prediction_authority"] == "uncalibrated_analytic_bracket_score_shadow_only"


def test_v23_route_telemetry_declares_full_cycle_cost_authority(monkeypatch):
    monkeypatch.setattr("execution.venue_selection._cfg", lambda name, default: {
        "VENUE_ROUND_TRIP_FEE_BPS": {"hyperliquid": 7.0},
        "VENUE_SLIPPAGE_IMPACT_MULTIPLIER": 0.0,
    }.get(name, default))
    state = _state__test_institutional_v6_exposure_feed_execution_fixes("hyperliquid", "xyz:CL", 90.0)
    est = estimate_venue_cost(state=state, direction="SHORT", asset_id="OIL", reference_mid=90.0, notional_usd=10.0, routeable=True)
    payload = est.as_dict()
    assert payload["round_trip_fee_included"] is True
    assert payload["cost_authority"] == "full_cycle_route_reserve_round_trip_fee_included"

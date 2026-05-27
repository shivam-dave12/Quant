
import sys
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


def test_two_stage_session_book_screens_chain_then_requires_documented_quote_depth(monkeypatch):
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

    ce_shortlist = chain.shortlist_contracts_for_quote_validation(
        instrument, "long", underlying_spot=25000.0, available_funds=25852.96, limit=4
    )
    pe_shortlist = chain.shortlist_contracts_for_quote_validation(
        instrument, "short", underlying_spot=25000.0, available_funds=25852.96, limit=4
    )
    assert ce_shortlist[0]["TradingSymbol"] == ce_symbol
    assert pe_shortlist[0]["TradingSymbol"] == pe_symbol
    # An option-chain row alone is not executable because Groww documents no bid/offer depth there.
    assert chain.build_session_contract_book(
        instrument, underlying_spot=25000.0, available_funds=25852.96, commit=False
    ) is None

    quote_by_symbol = {
        ce_symbol: {"bid_price": 99.5, "offer_price": 100.0, "bid_quantity": 100, "offer_quantity": 100},
        pe_symbol: {"bid_price": 104.0, "offer_price": 104.5, "bid_quantity": 100, "offer_quantity": 100},
    }
    book = chain.build_session_contract_book(
        instrument, underlying_spot=25000.0, available_funds=25852.96,
        option_quote_by_symbol=quote_by_symbol, commit=False,
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

import os
import time
from types import SimpleNamespace

os.environ.setdefault("DELTA_API_KEY", "test")
os.environ.setdefault("DELTA_SECRET_KEY", "test")
os.environ.setdefault("BREEZE_API_KEY", "test")
os.environ.setdefault("BREEZE_SECRET_KEY", "test")

import config
from aggregator.market_aggregator import MarketAggregator
from exchanges.icici.data_manager import ICICIOptionDataManager
from exchanges.icici.live_feed import BreezeLiveFeedHub
from exchanges.icici.underlying_data_manager import ICICIUnderlyingDataManager


def _nifty_instrument():
    raw = {"stock_code": "NIFTY", "exchange_code": "NFO", "underlying_exchange_code": "NSE"}
    return SimpleNamespace(asset_id="NIFTY", primary=SimpleNamespace(raw=raw), primary_exchange=SimpleNamespace(value="icici"))


def test_icici_live_streams_are_enabled_and_mandatory_by_default():
    assert config.ICICI_INDEX_STREAM_ENABLED is True
    assert config.ICICI_INDEX_WEBSOCKET_REQUIRED is True
    assert config.ICICI_OPTION_STREAM_ENABLED is True
    assert config.ICICI_OPTION_WEBSOCKET_REQUIRED is True
    assert config.ICICI_REQUIRE_UNDERLYING_ANALYSIS_FEED is True


def test_underlying_quote_tick_builds_structural_frames_without_session_ohlc_contamination():
    dm = ICICIUnderlyingDataManager(_nifty_instrument(), api=SimpleNamespace())
    dm._on_stream_candle({
        "exchange_code": "NSE", "stock_code": "NIFTY", "last": 24000.0,
        "open": 23200.0, "high": 25000.0, "low": 22000.0, "ttq": 900000,
        "ltt": "Mon May 25 10:00:01 2026",
    })
    one = dm.get_candles("1m", 1)[-1]
    fifteen = dm.get_candles("15m", 1)[-1]
    assert one["o"] == one["h"] == one["l"] == one["c"] == 24000.0
    assert one["v"] == 0.0
    assert fifteen["c"] == 24000.0
    assert dm.is_price_fresh(30.0) is True


def test_option_quotes_refresh_execution_price_but_only_ohlcv_ticks_build_premium_bars():
    api = SimpleNamespace(_normalise_right=lambda value: str(value).lower())
    dm = ICICIOptionDataManager(_nifty_instrument(), api=api)
    dm._active_stream_contract = {"stock_code": "NIFTY", "expiry": "02-Jun-2026", "right": "call", "strike": 24100.0}
    dm._stream_subscription_ids = ["test-option-stream"]
    dm._on_option_stream_tick({
        "exchange_code": "NFO", "stock_code": "NIFTY", "right": "call", "strike_price": 24100.0,
        "last": 155.0, "open": 120.0, "high": 170.0, "low": 115.0,
        "best_bid_price": 154.95, "best_offer_price": 155.05,
    })
    assert dm.get_last_price() == 155.0
    assert dm.get_candles("1m", 1) == []
    dm._on_option_stream_tick({
        "exchange_code": "NFO", "stock_code": "NIFTY", "right": "call", "strike_price": 24100.0,
        "interval": "1minute", "last": 155.5, "open": 155.0, "high": 156.0, "low": 154.75, "volume": 100,
        "datetime": "2026-05-25 10:01:00",
    })
    one = dm.get_candles("1m", 1)[-1]
    assert one["o"] == 155.0 and one["h"] == 156.0 and one["l"] == 154.75 and one["c"] == 155.5
    assert dm.is_price_fresh(30.0) is True


def test_option_market_depth_tick_updates_executable_book_without_ltp():
    api = SimpleNamespace(_normalise_right=lambda value: str(value).lower())
    dm = ICICIOptionDataManager(_nifty_instrument(), api=api)
    dm._active_stream_contract = {"stock_code": "NIFTY", "expiry": "02-Jun-2026", "right": "call", "strike": 24100.0}
    dm._stream_subscription_ids = ["test-option-depth"]
    dm._on_option_stream_tick({
        "exchange_code": "NFO", "stock_code": "NIFTY", "right": "call", "strike_price": 24100.0,
        "depth": [{
            "BestBuyRate-1": 154.9, "BestBuyQty-1": 1800,
            "BestSellRate-1": 155.1, "BestSellQty-1": 1650,
        }],
    })
    book = dm.get_orderbook()
    assert dm.get_last_price() == 0.0
    assert book["bids"] == [[154.9, 1800.0]]
    assert book["asks"] == [[155.1, 1650.0]]
    assert book["_executable_source"] == "icici_breeze_websocket"


def test_breeze_option_subscription_uses_documented_quote_depth_and_ohlcv_channels():
    class Client:
        def __init__(self):
            self.calls = []

        def subscribe_feeds(self, **kwargs):
            self.calls.append(kwargs)
            return {"message": "ok"}

    client = Client()
    hub = BreezeLiveFeedHub(api=SimpleNamespace())
    hub._client = client
    ids = hub.subscribe_option_quotes_and_ohlcv(
        stock_code="NIFTY", expiry_date="02-Jun-2026", strike_price="24100", right="call", callback=lambda tick: None)
    assert len(ids) == 3
    quote, depth, ohlcv = client.calls
    assert quote["get_market_depth"] is False and quote["get_exchange_quotes"] is True and "interval" not in quote
    assert depth["get_market_depth"] is True and depth["get_exchange_quotes"] is False and "interval" not in depth
    assert ohlcv["get_market_depth"] is False and ohlcv["get_exchange_quotes"] is True and ohlcv["interval"] == "1minute"


def test_icici_aggregator_fails_closed_when_underlying_websocket_analysis_is_unavailable():
    primary = SimpleNamespace(start=lambda: True, is_ready=True)
    analysis = SimpleNamespace(start=lambda: False)
    agg = MarketAggregator(primary, None, instrument=_nifty_instrument(), analysis_dm=analysis)
    assert agg.start() is False


def test_session_book_arms_both_option_websocket_vehicles_before_entry(monkeypatch):
    import exchanges.icici.data_manager as dm_module

    class API:
        @staticmethod
        def _normalise_right(value):
            return str(value).lower()

    class Hub:
        def __init__(self):
            self.calls = []
        def subscribe_option_quotes_and_ohlcv(self, **kwargs):
            self.calls.append(kwargs)
            kwargs["callback"]({
                "exchange_code": "NFO", "stock_code": "NIFTY",
                "right": kwargs["right"], "strike_price": float(kwargs["strike_price"]),
                "last": 150.0 if kwargs["right"] == "call" else 190.0,
                "best_bid_price": 149.9, "best_offer_price": 150.1,
            })
            return [f"{kwargs['right']}:quote", f"{kwargs['right']}:ohlcv"]
        def unsubscribe(self, ids):
            pass

    hub = Hub()
    monkeypatch.setattr(dm_module, "hub_for_api", lambda api: hub)
    dm = ICICIOptionDataManager(_nifty_instrument(), api=API())
    call = SimpleNamespace(expiry="02-Jun-2026", right="call", strike=24100.0, selected_symbol="NIFTYCE", raw={"stock_code": "NIFTY", "expiry_date": "02-Jun-2026", "right": "call", "strike_price": 24100.0})
    put = SimpleNamespace(expiry="02-Jun-2026", right="put", strike=23950.0, selected_symbol="NIFTYPE", raw={"stock_code": "NIFTY", "expiry_date": "02-Jun-2026", "right": "put", "strike_price": 23950.0})
    assert dm._arm_session_book_streams(SimpleNamespace(call=call, put=put)) is True
    assert len(hub.calls) == 2
    assert len(dm._book_stream_state) == 2
    assert all(state["last_stream_tick_ts"] > 0 for state in dm._book_stream_state.values())


def test_session_book_can_scan_while_option_ticks_are_pending(monkeypatch):
    import exchanges.icici.data_manager as dm_module

    class API:
        @staticmethod
        def _normalise_right(value):
            return str(value).lower()

    class Hub:
        def __init__(self):
            self.calls = []
        def subscribe_option_quotes_and_ohlcv(self, **kwargs):
            self.calls.append(kwargs)
            return [f"{kwargs['right']}:quote", f"{kwargs['right']}:depth", f"{kwargs['right']}:ohlcv"]
        def unsubscribe(self, ids):
            raise AssertionError("pending first tick must keep subscriptions armed")

    hub = Hub()
    monkeypatch.setattr(dm_module, "hub_for_api", lambda api: hub)
    monkeypatch.setattr(dm_module.config, "ICICI_OPTION_WEBSOCKET_REQUIRED", True, raising=False)
    monkeypatch.setattr(dm_module.config, "ICICI_OPTION_STREAM_FIRST_TICK_TIMEOUT_SEC", 0.01, raising=False)
    monkeypatch.setattr(dm_module.config, "ICICI_SESSION_BOOK_REQUIRE_FIRST_OPTION_TICK_ON_STARTUP", False, raising=False)
    dm = ICICIOptionDataManager(_nifty_instrument(), api=API())
    call = SimpleNamespace(expiry="02-Jun-2026", right="call", strike=24100.0, selected_symbol="NIFTYCE", raw={"stock_code": "NIFTY", "expiry_date": "02-Jun-2026", "right": "call", "strike_price": 24100.0})
    put = SimpleNamespace(expiry="02-Jun-2026", right="put", strike=23950.0, selected_symbol="NIFTYPE", raw={"stock_code": "NIFTY", "expiry_date": "02-Jun-2026", "right": "put", "strike_price": 23950.0})

    assert dm._arm_session_book_streams(SimpleNamespace(call=call, put=put)) is True
    assert len(hub.calls) == 2
    assert len(dm._stream_subscription_ids) == 6
    assert all(state["last_stream_tick_ts"] == 0.0 for state in dm._book_stream_state.values())


def test_icici_option_book_request_does_not_reconnect_without_active_vehicle():
    dm = ICICIOptionDataManager(_nifty_instrument(), api=SimpleNamespace())
    calls = []
    dm._stream_subscription_ids = ["session-call", "session-put"]
    dm._live_hub = SimpleNamespace(reconnect_and_resubscribe=lambda reason: calls.append(reason) or True)

    book = dm.get_orderbook()

    assert calls == []
    assert book["bids"] == [] and book["asks"] == []

import os
import threading
import time
from types import SimpleNamespace

os.environ.setdefault("DELTA_API_KEY", "test")
os.environ.setdefault("DELTA_SECRET_KEY", "test")
os.environ.setdefault("BREEZE_API_KEY", "test")
os.environ.setdefault("BREEZE_SECRET_KEY", "test")

from exchanges.icici.live_feed import BreezeLiveFeedHub
from exchanges.icici.data_manager import ICICIOptionDataManager
from execution.order_manager import _ICICIAdapter


def _instrument():
    raw = {
        "stock_code": "NIFTY", "exchange_code": "NFO", "product_type": "options",
        "expiry_date": "02-Jun-2026", "right": "Call", "strike_price": "24100",
        "runtime_lot_size": 65, "selected_entry_premium": 150.0,
    }
    return SimpleNamespace(
        asset_id="NIFTY", symbol="NIFTY", display_symbol="NIFTY", tick_size=0.05,
        lot_step=65.0, min_qty=65.0, max_qty=0.0,
        primary=SimpleNamespace(raw=raw), primary_exchange=SimpleNamespace(value="icici"), raw=raw,
    )


class _Client:
    def __init__(self):
        self.calls = []
    def subscribe_feeds(self, **kwargs):
        self.calls.append(kwargs)
        return {"message": "ok"}


def test_shared_breeze_callback_does_not_fan_out_underlying_tick_into_option_routes():
    client = _Client()
    hub = BreezeLiveFeedHub(api=SimpleNamespace())
    hub._client = client
    received = {"underlying": [], "option": []}
    hub.subscribe_underlying_quotes(exchange_code="NSE", stock_code="NIFTY", callback=lambda t: received["underlying"].append(t))
    hub.subscribe_option_quotes_and_ohlcv(
        stock_code="NIFTY", expiry_date="02-Jun-2026", strike_price="24100", right="call",
        callback=lambda t: received["option"].append(t),
    )
    hub._dispatch({"exchange": "NSE Equity", "stock_name": "NIFTY 50", "last": 23985.0, "bPrice": 23984.9, "sPrice": 23985.1})
    assert len(received["underlying"]) == 1
    assert received["option"] == []


def test_documented_nfo_quote_and_depth_packets_route_only_to_exact_selected_option():
    client = _Client()
    hub = BreezeLiveFeedHub(api=SimpleNamespace())
    hub._client = client
    received = []
    hub.subscribe_option_quotes_and_ohlcv(
        stock_code="NIFTY", expiry_date="02-Jun-2026", strike_price="24100", right="call",
        callback=lambda t: received.append(t),
    )
    hub._dispatch({
        "symbol": "4.1!51219", "exchange": "NSE Futures & Options", "stock_name": "NIFTY 50",
        "product_type": "Options", "expiry_date": "02-Jun-2026", "strike_price": "24100",
        "right": "Call", "last": 150.0, "bPrice": 149.95, "sPrice": 150.05, "quotes": "Quotes Data",
    })
    hub._dispatch({
        "symbol": "4.2!51219", "exchange": "NSE Futures & Options", "stock_name": "NIFTY 50",
        "product_type": "Options", "expiry_date": "02-Jun-2026", "strike_price": "24100",
        "right": "Call", "quotes": "Market Depth", "depth": [{"BestBuyRate-1": 149.9, "BestSellRate-1": 150.1}],
    })
    assert len(received) == 2


def test_data_manager_accepts_documented_nse_futures_options_identity_and_depth_can_arm_vehicle():
    api = SimpleNamespace(_normalise_right=lambda v: {"ce": "call", "call": "call", "pe": "put", "put": "put"}.get(str(v).lower(), str(v).lower()))
    dm = ICICIOptionDataManager(_instrument(), api=api)
    key = ("02-Jun-2026", "call", 24100.0)
    event = threading.Event()
    dm._book_stream_state[key] = {
        "identity": {"stock_code": "NIFTY", "expiry": "02-Jun-2026", "right": "call", "strike": 24100.0},
        "event": event, "last_stream_tick_ts": 0.0, "stream_pending": True, "candles": {"1m": []},
        "last_price": 150.0, "best_bid": 0.0, "best_ask": 0.0, "best_bid_qty": 0.0, "best_ask_qty": 0.0,
        "quote_tick_ts": 0.0, "depth_tick_ts": 0.0, "ohlcv_tick_ts": 0.0,
    }
    dm._on_session_book_option_tick(key, dm._book_stream_state[key]["identity"], {
        "exchange": "NSE Futures & Options", "stock_name": "NIFTY 50", "product_type": "Options",
        "expiry_date": "02-Jun-2026", "strike_price": "24100", "right": "Call",
        "quotes": "Market Depth", "depth": [{"BestBuyRate-1": 149.9, "BestBuyQty-1": 130, "BestSellRate-1": 150.1, "BestSellQty-1": 195}],
    })
    assert event.is_set()
    assert dm._book_stream_state[key]["best_bid"] == 149.9
    assert dm._stream_unroutable_tick_count == 0


def test_option_stream_does_not_reconnect_or_block_when_shared_underlying_transport_is_live(monkeypatch):
    import exchanges.icici.data_manager as module
    monkeypatch.setattr(module, "icici_market_session_state", lambda: SimpleNamespace(is_open=True))
    dm = ICICIOptionDataManager(_instrument(), api=SimpleNamespace())
    calls = []
    dm._stream_subscription_ids = ["opt"]
    dm._live_hub = SimpleNamespace(connected=True, last_tick_ts=time.time(), reconnect_and_resubscribe=lambda reason: calls.append(reason) or True)
    start = time.perf_counter()
    assert dm._repair_option_stream_if_stale("option_freshness_gate", wait_key=("02-Jun-2026", "call", 24100.0)) is False
    assert time.perf_counter() - start < 0.05
    assert calls == []


def test_exact_contract_commit_preflight_is_short_lived_and_execution_only(monkeypatch):
    import exchanges.icici.data_manager as module
    monkeypatch.setattr(module, "breeze_throttle", lambda *a, **k: None)
    api = SimpleNamespace(
        get_quote_for_instrument=lambda inst: {"Success": {"ltp": 150.0, "bPrice": 149.95, "sPrice": 150.05, "bQty": 130, "sQty": 195}},
    )
    dm = ICICIOptionDataManager(_instrument(), api=api)
    dm._active_stream_key = ("02-Jun-2026", "call", 24100.0)
    assert dm._refresh_quote(source="EXACT_CONTRACT_REST_PREFLIGHT") is True
    assert dm._execution_price_fresh(10.0) is True
    assert dm.get_orderbook()["_executable_source"] == "icici_exact_contract_rest_preflight"


def test_icici_adapter_places_documented_cover_oco_gtt_and_never_naked_entry():
    class API:
        @staticmethod
        def _normalise_right(value):
            return "call"
        @staticmethod
        def _normalise_expiry(value):
            return str(value)
        def place_gtt_three_leg_oco(self, **kwargs):
            self.payload = kwargs
            return {"Success": {"gtt_order_id": "2026052600001234"}, "Status": 200, "Error": None}
    api = API()
    inst = _instrument()
    adapter = _ICICIAdapter(api, exchange_instrument=inst)
    result = adapter.place_bracket_limit_entry("long", 65, 150.05, 135.0, 175.0)
    assert result and result["protection_model"] == "ICICI_GTT_COVER_OCO"
    assert result["bracket_order"] is True
    assert api.payload["fresh_order_type"] == "limit"
    assert api.payload["gtt_type"] == "cover_oco"
    assert {leg["gtt_leg_type"] for leg in api.payload["order_details"]} == {"target", "stoploss"}


def test_gtt_entry_fill_resolves_official_nested_gtt_id_via_fresh_order_id():
    class API:
        @staticmethod
        def _normalise_right(value): return "call"
        @staticmethod
        def _normalise_expiry(value): return str(value)
        def get_gtt_order_book(self, **kwargs):
            return {"Success": [{
                "fresh_order_id": "20260526NFO0001", "quantity": 65,
                "order_details": [
                    {"gtt_leg_type": "Target", "gtt_order_id": "GTT123", "status": "Ordered", "trigger_price": 175.0, "limit_price": 175.0},
                    {"gtt_leg_type": "Stoploss", "gtt_order_id": "GTT123", "status": "Ordered", "trigger_price": 135.0, "limit_price": 134.95},
                ],
            }]}
        def get_order(self, **kwargs):
            assert kwargs["order_id"] == "20260526NFO0001"
            return {"Success": {"status": "Executed", "average_price": "150.05", "quantity": "65"}}
    adapter = _ICICIAdapter(API(), exchange_instrument=_instrument())
    row = adapter.get_order("GTT:GTT123")
    assert row["order_id"] == "GTT:GTT123"
    assert row["broker_order_id"] == "20260526NFO0001"
    assert adapter.extract_status(row) == "FILLED"
    assert adapter.extract_fill_price(row) == 150.05


def test_gtt_exit_leg_never_books_planned_trigger_as_execution_fill():
    class API:
        def get_gtt_order_book(self, **kwargs):
            return {"Success": [{
                "fresh_order_id": "20260526NFO0001", "quantity": 65,
                "order_details": [
                    {"gtt_leg_type": "Target", "gtt_order_id": "GTT123", "status": "Executed", "trigger_price": 175.0, "limit_price": 175.0},
                    {"gtt_leg_type": "Stoploss", "gtt_order_id": "GTT123", "status": "Cancelled", "trigger_price": 135.0, "limit_price": 134.95},
                ],
            }]}
    adapter = _ICICIAdapter(API(), exchange_instrument=_instrument())
    row = adapter.get_order("GTT:GTT123:TARGET")
    assert adapter.extract_status(row) == "FILLED"
    assert adapter.extract_fill_price(row) is None

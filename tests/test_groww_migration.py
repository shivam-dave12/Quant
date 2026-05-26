import os
from types import SimpleNamespace

os.environ.setdefault("GROWW_ACCESS_TOKEN", "test-groww-token")
os.environ.setdefault("GROWW_ENABLED", "true")
os.environ.setdefault("GROWW_DISCOVERY_ENABLED", "true")
os.environ.setdefault("EXECUTION_EXCHANGE", "groww")

from core.instruments import AssetClass, ExchangeInstrument, ExchangeName, TradableInstrument
from core.types import Exchange
from exchanges.groww.api import GrowwRestClient
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
    SMART_ORDER_TYPE_GTT = "GTT"
    TRIGGER_DIRECTION_DOWN = "DOWN"
    TRIGGER_DIRECTION_UP = "UP"

    def __init__(self):
        self.last_place_order = None
        self.last_smart_order = None

    def place_order(self, **kwargs):
        self.last_place_order = kwargs
        return {"groww_order_id": "GROWWORDER1", "order_status": "OPEN"}

    def create_smart_order(self, **kwargs):
        self.last_smart_order = kwargs
        return {"smart_order_id": "gtt_12345", "status": "ACTIVE"}


def _client() -> GrowwRestClient:
    api = GrowwRestClient(access_token="test")
    api._client = FakeGrowwSDK()
    return api


def _groww_inst() -> TradableInstrument:
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
        "runtime_lot_size": 50,
        "LotSize": 50,
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
    assert "icici" not in {ex.value for ex in nifty.by_exchange}


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


def test_groww_protected_entry_uses_smart_gtt_bracket_children():
    api = _client()
    om = OrderManager(api, exchange_name="groww", instrument=_groww_inst())
    data = om._adapter.place_bracket_limit_entry("BUY", 50, limit_price=118.75, sl_price=95.0, tp_price=160.0)
    payload = api._client.last_smart_order
    assert data["protection_model"] == "GROWW_SMART_GTT_BRACKET"
    assert payload["smart_order_type"] == "GTT"
    assert payload["segment"] == "FNO"
    assert payload["order"]["order_type"] == "LIMIT"
    assert payload["order"]["transaction_type"] == "BUY"
    assert payload["child_legs"]["target"]["order_type"] == "LIMIT"
    assert payload["child_legs"]["stop_loss"]["order_type"] == "SL"


def test_execution_router_accepts_groww_manager():
    om = OrderManager(_client(), exchange_name="groww", instrument=_groww_inst())
    router = ExecutionRouter(coinswitch_om=None, delta_om=None, groww_om=om, default="groww")
    assert router.active_exchange == "groww"
    assert router.active is om

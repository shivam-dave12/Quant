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


def test_groww_lifecycle_oco_failure_blocks_new_entries_and_emergency_exits(monkeypatch):
    monkeypatch.setattr("execution.order_manager.config.GROWW_REQUIRE_STATIC_IP_FOR_LIVE_ORDERS", False, raising=False)
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
    assert fake.place_orders[1]["order_type"] == "LIMIT"


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

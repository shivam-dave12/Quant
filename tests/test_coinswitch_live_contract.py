from __future__ import annotations

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
        return {"data": {"order_id": order_id, "status": "EXECUTED", "exec_quantity": 0.01, "avg_execution_price": 75000.0}}

    def cancel_order(self, order_id, exchange="EXCHANGE_2"):
        return {"data": {"order_id": order_id}}


def test_coinswitch_entry_arms_position_level_reduce_only_sl_and_tp():
    api = _OrderAPI()
    inst = SimpleNamespace(symbol="BTCUSDT", display_symbol="BTC/USDT", tick_size=0.1, lot_step=0.001, min_qty=0.001, max_qty=1.0)
    adapter = _CoinSwitchAdapter(api, inst)
    out = adapter.place_bracket_limit_entry("BUY", 0.01, 75000.0, 74500.0, 76000.0, timeout_sec=1.0)
    assert out["protection_confirmed"] is True
    assert [c["order_type"] for c in api.calls] == ["LIMIT", "STOP_MARKET", "TAKE_PROFIT_MARKET"]
    for call in api.calls[1:]:
        assert call["quantity"] == 0.0
        assert call["reduce_only"] is True

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

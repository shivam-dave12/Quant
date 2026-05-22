import os
import threading
from types import SimpleNamespace

os.environ.setdefault("DELTA_API_KEY", "test")
os.environ.setdefault("DELTA_SECRET_KEY", "test")
os.environ.setdefault("BREEZE_API_KEY", "test")
os.environ.setdefault("BREEZE_SECRET_KEY", "test")

from core.instruments import AssetClass, ExchangeName, ExchangeInstrument, TradableInstrument
from execution.order_manager import OrderManager
from strategy.quant_strategy import PositionPhase, PositionState, QuantStrategy
from aggregator.market_aggregator import MarketAggregator


def _instrument(exchange: ExchangeName, symbol: str, asset: str, asset_class=AssetClass.COMMODITY):
    raw = {}
    if exchange == ExchangeName.ICICI:
        raw = {
            "stock_code": "NIFTY", "strike_price": 24000, "expiry_date": "2026-06-02",
            "right": "call", "exchange_code": "NFO", "product_type": "Options",
        }
    ei = ExchangeInstrument(
        exchange=exchange, symbol=symbol, ws_symbol=symbol, display_symbol=symbol,
        asset_id=asset, asset_class=asset_class, quote_asset="INR" if exchange == ExchangeName.ICICI else "USD",
        status="active", tick_size=0.05 if exchange == ExchangeName.ICICI else 0.01,
        lot_step=1.0, min_qty=1.0, raw=raw,
    )
    return TradableInstrument(asset, asset, asset_class, exchange, {exchange: ei})


def test_icici_stop_fill_is_reconciled_from_tracked_order_and_trade_vwap():
    inst = _instrument(ExchangeName.ICICI, "NIFTY02JUN202624000CE", "NIFTY", AssetClass.OPTION)

    class API:
        def get_order(self, order_id=None, exchange_code=None):
            return {"Success": {"order_id": order_id, "status": "EXECUTED", "average_price": "65.10", "filled_quantity": "65"}}
        def get_trade_detail(self, order_id=None, exchange_code=None):
            return {"Success": [
                {"trade_price": "64.80", "quantity": "40"},
                {"trade_price": "65.20", "quantity": "25", "total_charges": "3.75"},
            ]}

    om = OrderManager(API(), exchange_name="icici", instrument=inst)
    om._adapter.limiter.wait = lambda: None
    result = om.identify_exit_order("sl-icici-1", None)
    assert result["confirmed"] is True
    assert result["exit_type"] == "sl"
    assert result["order_id"] == "sl-icici-1"
    assert abs(result["fill_price"] - ((64.80 * 40 + 65.20 * 25) / 65)) < 1e-9
    assert result["fee_paid"] == 3.75
    assert result["fee_exact"] is True


def test_position_payoff_model_is_instrument_scoped_not_global_config():
    qs = QuantStrategy.__new__(QuantStrategy)
    qs._asset_id = "GOLD"
    qs._instrument = _instrument(ExchangeName.DELTA, "PAXGUSD", "GOLD")
    assert qs._position_accounting_context()["pnl_model"] == "linear"
    qs._instrument = _instrument(ExchangeName.DELTA, "BTCUSD", "BTC", AssetClass.CRYPTO)
    assert qs._position_accounting_context()["pnl_model"] == "inverse_btcusd"
    qs._instrument = _instrument(ExchangeName.COINSWITCH, "BTCUSDT", "BTC", AssetClass.CRYPTO)
    assert qs._position_accounting_context()["pnl_model"] == "linear"


def test_unrealised_pnl_accepts_portfolio_snapshot_dictionary():
    qs = QuantStrategy.__new__(QuantStrategy)
    live_snapshot = {
        "phase": "ACTIVE", "side": "long", "entry_price": 100.0,
        "quantity": 2.0, "pnl_model": "linear",
    }
    assert qs._unrealised_pnl_usd(104.0, live_snapshot) == 8.0
    live_snapshot["phase"] = "FLAT"
    assert qs._unrealised_pnl_usd(104.0, live_snapshot) == 0.0


def test_icici_analysis_price_remains_underlying_after_option_activation():
    option_dm = SimpleNamespace(get_last_price=lambda: 146.15)
    underlying_dm = SimpleNamespace(get_last_price=lambda: 23811.70)
    agg = MarketAggregator(option_dm, None, analysis_dm=underlying_dm)
    assert agg.get_last_price() == 146.15
    assert agg.get_analysis_price() == 23811.70


def test_flat_without_resolved_exit_fill_never_books_realised_pnl(monkeypatch):
    import strategy.quant_strategy as quant_module

    qs = QuantStrategy.__new__(QuantStrategy)
    qs._instrument = _instrument(ExchangeName.DELTA, "PAXGUSD", "GOLD")
    qs._pos = PositionState(
        phase=PositionPhase.ACTIVE, side="long", quantity=1.0, entry_price=4500.0,
        sl_price=4480.0, tp_price=4540.0, sl_order_id="sl-missing", tp_order_id="tp-missing",
        entry_time=1.0, exchange="delta", execution_symbol="PAXGUSD", asset_id="GOLD",
        pnl_model="linear", currency_symbol="$", currency_code="USD",
    )
    qs._lock = threading.RLock()
    qs._exit_completed = False
    qs._om = SimpleNamespace(
        identify_exit_order=lambda **kwargs: {"confirmed": False},
        get_position=lambda: {"size": 0.0},
    )
    qs._dm = None
    messages = []
    qs._send_telegram = messages.append
    qs._record_pnl = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("unresolved exits must not record P&L"))
    monkeypatch.setattr(quant_module.time, "sleep", lambda _: None)

    qs._record_exchange_exit({"size": 0.0})
    assert qs._pos.phase == PositionPhase.EXITING
    assert qs._exit_completed is False
    assert qs._pos.unconfirmed_exit_attempts == 1
    assert messages and "PENDING EXACT RECONCILIATION" in messages[0]

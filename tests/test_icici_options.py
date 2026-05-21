import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

os.environ.setdefault("DELTA_API_KEY", "test")
os.environ.setdefault("DELTA_SECRET_KEY", "test")
os.environ.setdefault("BREEZE_API_KEY", "test")
os.environ.setdefault("BREEZE_SECRET_KEY", "test")

from core.instruments import AssetClass, ExchangeName, ExchangeInstrument, TradableInstrument, instrument_scope
from agents.icici_chain_architect import build_underlying_payload, select_contract_for_thesis, apply_contract_choice
from exchanges.icici.market_session import icici_market_session_state
from execution.order_manager import OrderManager
from strategy.quant_strategy import _icici_option_premium_levels


def _nifty_inst(chain):
    raw = build_underlying_payload("NIFTY", "ICICI_INDEX_OPTIONS", [])
    raw["chain_candidates"] = chain
    raw["chain_candidates_deferred"] = False
    ei = ExchangeInstrument(
        exchange=ExchangeName.ICICI,
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
    return TradableInstrument("NIFTY", "NIFTY options", AssetClass.OPTION, ExchangeName.ICICI, {ExchangeName.ICICI: ei})


def _chain():
    expiry = (datetime.now(timezone.utc) + timedelta(days=10)).strftime("%Y-%m-%d")
    return [
        {"TradingSymbol": "NIFTY30JUN30CE23000", "right": "Call", "strike_price": 23000, "expiry_date": expiry, "ltp": 260.0, "LotSize": 50, "stock_code": "NIFTY", "exchange_code": "NFO"},
        {"TradingSymbol": "NIFTY30JUN30CE23200", "right": "Call", "strike_price": 23200, "expiry_date": expiry, "ltp": 72.0, "LotSize": 50, "stock_code": "NIFTY", "exchange_code": "NFO"},
        {"TradingSymbol": "NIFTY30JUN30PE22600", "right": "Put", "strike_price": 22600, "expiry_date": expiry, "ltp": 78.0, "LotSize": 50, "stock_code": "NIFTY", "exchange_code": "NFO"},
    ]


def test_icici_selector_uses_available_funds_and_thesis_side():
    inst = _nifty_inst(_chain())

    bullish = select_contract_for_thesis(inst, "long", underlying_spot=23100, available_funds=10_000)
    assert bullish is not None
    assert bullish.right == "call"
    assert bullish.selected_symbol == "NIFTY30JUN30CE23200"
    assert bullish.raw["selected_contract_cost"] == 72.0 * 50

    bearish = select_contract_for_thesis(inst, "short", underlying_spot=22900, available_funds=10_000)
    assert bearish is not None
    assert bearish.right == "put"

    unaffordable = select_contract_for_thesis(inst, "long", underlying_spot=23100, available_funds=500)
    assert unaffordable is None


def test_icici_order_manager_balance_uses_fno_allocation_not_bank_total():
    inst = _nifty_inst(_chain())

    class API:
        def get_funds(self):
            return {"Success": {"allocated_fno": "10000", "block_by_trade_fno": "2500", "total_bank_balance": "999999"}}

        def get_margin(self, exchange_code="NFO"):
            return {"Success": {"cash_limit": "3000", "block_by_trade": "100"}}

    om = OrderManager(API(), exchange_name="icici", instrument=inst)
    bal = om.get_balance()
    assert bal["available"] == 7500.0
    assert bal["total"] == 10000.0
    assert bal["bank_total"] == 999999.0
    assert "allocated_fno" in bal["source"]


def test_icici_market_session_guard_indian_hours():
    ist = ZoneInfo("Asia/Kolkata")
    open_state = icici_market_session_state(datetime(2026, 5, 21, 10, 0, tzinfo=ist))
    closed_state = icici_market_session_state(datetime(2026, 5, 21, 16, 0, tzinfo=ist))
    weekend_state = icici_market_session_state(datetime(2026, 5, 23, 10, 0, tzinfo=ist))

    assert open_state.is_open
    assert not closed_state.is_open
    assert not weekend_state.is_open


def test_icici_underlying_levels_convert_to_option_premium_levels():
    inst = _nifty_inst(_chain())
    choice = select_contract_for_thesis(inst, "long", underlying_spot=23100, available_funds=10_000)
    apply_contract_choice(inst, choice)

    with instrument_scope(inst):
        sl, tp, reason = _icici_option_premium_levels(
            thesis_side="long",
            premium_entry=72.0,
            underlying_entry=23100.0,
            underlying_sl=23020.0,
            underlying_tp=23240.0,
            instrument=inst,
        )

    assert sl is not None and tp is not None
    assert sl < 72.0 < tp
    assert (tp - 72.0) / (72.0 - sl) >= 1.6
    assert "delta=" in reason


def test_icici_configured_nifty_discovery_is_auth_independent():
    from execution.instrument_registry import InstrumentRegistry

    reg = InstrumentRegistry(execution_preference="delta")
    report = reg.discover(
        delta_api=None,
        coinswitch_api=None,
        icici_api=None,
        include_exchanges="icici",
        requested=[{
            "asset_id": "NIFTY",
            "display_name": "NIFTY 50 index options",
            "asset_class": "option",
            "aliases": ["NIFTY50", "NIFTY", "CNXNIFTY"],
            "priority": 1,
        }],
        max_active=5,
        require_primary=False,
    )
    assert report.raw_counts["icici"] == 1
    assert len(report.matched) == 1
    inst = report.matched[0]
    assert inst.asset_id == "NIFTY"
    assert ExchangeName.ICICI in inst.by_exchange
    icici_inst = inst.by_exchange[ExchangeName.ICICI]
    assert icici_inst.symbol == "NIFTY"
    assert icici_inst.raw["breeze_stock_code"] == "NIFTY"
    assert icici_inst.raw["exchange_code"] == "NFO"
    assert icici_inst.raw["underlying_exchange_code"] == "NSE"

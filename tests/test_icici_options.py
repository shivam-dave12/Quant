import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from zoneinfo import ZoneInfo

os.environ.setdefault("DELTA_API_KEY", "test")
os.environ.setdefault("DELTA_SECRET_KEY", "test")
os.environ.setdefault("BREEZE_API_KEY", "test")
os.environ.setdefault("BREEZE_SECRET_KEY", "test")

from core.instruments import AssetClass, ExchangeName, ExchangeInstrument, TradableInstrument, instrument_scope
from agents.icici_chain_architect import build_underlying_payload, select_contract_for_thesis, apply_contract_choice
from exchanges.icici.market_session import icici_market_session_state
from execution.order_manager import OrderManager
from strategy.quant_strategy import _icici_ensure_adoptable_contract, _icici_option_premium_levels, _icici_selected_premium


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

    assert _icici_selected_premium(inst, fallback=0.0) == 72.0

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


def test_icici_data_managers_are_dormant_after_market_close(monkeypatch):
    import exchanges.icici.data_manager as option_dm_mod
    import exchanges.icici.underlying_data_manager as underlying_dm_mod

    closed = SimpleNamespace(is_open=False, reason="market closed after 15:30 IST")
    monkeypatch.setattr(option_dm_mod, "icici_market_session_state", lambda: closed)
    monkeypatch.setattr(underlying_dm_mod, "icici_market_session_state", lambda: closed)
    monkeypatch.setattr(option_dm_mod.config, "ICICI_ANALYZE_ONLY_DURING_MARKET_SESSION", True, raising=False)
    monkeypatch.setattr(underlying_dm_mod.config, "ICICI_ANALYZE_ONLY_DURING_MARKET_SESSION", True, raising=False)

    class API:
        def preflight_session(self):
            raise AssertionError("closed-market startup must not touch Breeze auth/data")

    inst = _nifty_inst(_chain())
    option_dm = option_dm_mod.ICICIOptionDataManager(instrument=inst, api=API())
    underlying_dm = underlying_dm_mod.ICICIUnderlyingDataManager(instrument=inst, api=API())

    assert not option_dm.start()
    assert not option_dm.is_ready
    assert not underlying_dm.start()
    assert not underlying_dm.is_ready


def test_nifty_is_excluded_from_cross_asset_overlay():
    from orchestration.multi_asset_bot import MultiAssetQuantBot

    def _inst(asset_id, exchange=ExchangeName.DELTA):
        ei = ExchangeInstrument(
            exchange=exchange,
            symbol=f"{asset_id}USD",
            ws_symbol=f"{asset_id}USD",
            display_symbol=f"{asset_id}USD",
            asset_id=asset_id,
            asset_class=AssetClass.CRYPTO if asset_id == "BTC" else AssetClass.COMMODITY,
            quote_asset="USD",
            base_asset=asset_id,
            contract_type="perpetual",
            status="active",
            tick_size=0.1,
            lot_step=1.0,
            min_qty=1.0,
            max_leverage=50.0,
        )
        return TradableInstrument(asset_id, asset_id, ei.asset_class, exchange, {exchange: ei})

    class FakeState:
        enabled = True
        def summary(self):
            return "fake"

    class FakeCrossAsset:
        def __init__(self):
            self.assets = []
            self.state = FakeState()
        def update_from_contexts(self, contexts):
            self.assets = [c.instrument.asset_id for c in contexts]
            return self.state

    class FakeStrategy:
        def __init__(self):
            self.states = []
        def set_cross_asset_state(self, state):
            self.states.append(state)

    btc_strategy = FakeStrategy()
    nifty_strategy = FakeStrategy()
    bot = MultiAssetQuantBot.__new__(MultiAssetQuantBot)
    bot.contexts = [
        SimpleNamespace(instrument=_inst("BTC"), ready=True, strategy=btc_strategy),
        SimpleNamespace(instrument=_nifty_inst(_chain()), ready=True, strategy=nifty_strategy),
    ]
    bot.cross_asset = FakeCrossAsset()
    bot._last_cross_asset_log = 10**12

    bot._update_cross_asset_overlay()

    assert bot.cross_asset.assets == ["BTC"]
    assert btc_strategy.states[-1] is bot.cross_asset.state
    assert nifty_strategy.states[-1] is None


def test_icici_position_reconcile_requires_exact_option_identity():
    from execution.order_manager import _ICICIAdapter

    raw = build_underlying_payload("NIFTY", "ICICI_INDEX_OPTIONS", [])
    exchange_inst = SimpleNamespace(
        symbol="NIFTY",
        display_symbol="NIFTY",
        tick_size=0.05,
        lot_step=1.0,
        min_qty=1.0,
        max_qty=0.0,
        raw=raw,
    )
    adapter = _ICICIAdapter(api=SimpleNamespace(), exchange_instrument=exchange_inst)
    expiry = (datetime.now(timezone.utc) + timedelta(days=10)).strftime("%Y-%m-%d")
    broker_positions = {
        "Success": [{
            "stock_code": "NIFTY",
            "exchange_code": "NFO",
            "right": "Call",
            "strike_price": "23200",
            "expiry_date": expiry,
            "quantity": 50,
            "average_price": 72.0,
        }]
    }

    detected = adapter.normalise_position(broker_positions)
    assert detected["side"] == "LONG"
    assert detected["size"] == 50
    assert detected["requires_contract_reconstruction"] is True
    assert not detected.get("unadoptable")
    assert detected["currency"] == "INR"

    raw.update({
        "selected_option_contract": {"raw": dict(broker_positions["Success"][0])},
        "right": "Call",
        "strike_price": "23200",
        "expiry_date": expiry,
        "selected_entry_premium": 72.0,
    })

    adopted = adapter.normalise_position(broker_positions)
    assert adopted["side"] == "LONG"
    assert adopted["size"] == 50
    assert adopted["contract_identity_source"] == "selected_contract_match"


def test_icici_position_reconcile_reports_unadoptable_ghost_without_identity():
    from execution.order_manager import _ICICIAdapter

    raw = build_underlying_payload("NIFTY", "ICICI_INDEX_OPTIONS", [])
    exchange_inst = SimpleNamespace(
        symbol="NIFTY",
        display_symbol="NIFTY",
        tick_size=0.05,
        lot_step=1.0,
        min_qty=1.0,
        max_qty=0.0,
        raw=raw,
    )
    adapter = _ICICIAdapter(api=SimpleNamespace(), exchange_instrument=exchange_inst)
    broker_positions = {
        "Success": [{
            "stock_code": "NIFTY",
            "exchange_code": "NFO",
            "quantity": 50,
            "average_price": 72.0,
        }]
    }

    detected = adapter.normalise_position(broker_positions)
    assert detected["size"] == 50
    assert detected["unadoptable"] is True
    assert detected["reason"] == "missing_exact_icici_option_identity"


def test_icici_reconcile_can_reconstruct_exact_option_vehicle():
    inst = _nifty_inst(_chain())
    expiry = inst.primary.raw["chain_candidates"][1]["expiry_date"]
    ex_pos = {
        "side": "LONG",
        "size": 50,
        "entry_price": 72.0,
        "raw": {
            "stock_code": "NIFTY",
            "exchange_code": "NFO",
            "right": "Call",
            "strike_price": "23200",
            "expiry_date": expiry,
            "TradingSymbol": "NIFTY30JUN30CE23200",
            "LotSize": 50,
            "average_price": 72.0,
        },
    }

    assert _icici_ensure_adoptable_contract(inst, ex_pos)
    assert inst.primary.raw["product_type"] == "options"
    assert inst.primary.raw["right"] == "Call"
    assert inst.primary.raw["strike_price"] == "23200"
    assert _icici_selected_premium(inst, fallback=0.0) == 72.0


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



def test_telegram_start_preflights_icici_token_before_bot_start(monkeypatch, tmp_path):
    import sys
    import types
    from exchanges.icici.breeze_auth import BreezeSession
    import telegram.controller as ctl

    calls = []

    class FakeSvc:
        def require_configured(self, *, for_login=False):
            calls.append(("require", for_login))
        def get_session(self, force_refresh=False, otp_getter=None, otp_code=None):
            calls.append(("get", force_refresh))
            raise RuntimeError("missing session")
        def refresh(self, *, otp_getter=None, otp_code=None):
            calls.append(("refresh", otp_getter is not None))
            assert otp_getter is not None
            return BreezeSession("api-session", "session-token", 1.0, {})
        def session_status(self, session=None):
            return {"same_trading_day": True, "reason": "ok", "valid": True}

    fake_mod = types.SimpleNamespace(BreezeTokenService=FakeSvc)
    monkeypatch.setitem(sys.modules, "exchanges.icici.breeze_auth", fake_mod)
    import exchanges.icici.token_generator as token_generator
    monkeypatch.setattr(
        token_generator,
        "assert_playwright_chromium_runtime_ready",
        lambda **kw: calls.append(("preflight", kw)) or {"browser_path": "chromium", "playwright_browsers_path": "cache"},
    )
    monkeypatch.setattr(ctl.config, "ICICI_OPTIONS_RUNTIME_ENABLED", True, raising=False)
    monkeypatch.setattr(ctl.config, "ICICI_AUTO_TOKEN_GENERATOR_ON_STARTUP", True, raising=False)
    monkeypatch.setattr(ctl.config, "ICICI_BREEZE_PREFLIGHT_ON_STARTUP", True, raising=False)
    monkeypatch.setattr(ctl.config, "ICICI_AUTH_REQUIRED_FOR_DETAILS", True, raising=False)

    c = ctl.TelegramBotController.__new__(ctl.TelegramBotController)
    c._icici_otp_cv = __import__("threading").Condition()
    c._icici_pending_otp = ""
    c._icici_waiting_for_otp = False
    c._icici_refresh_thread = None
    sent = []
    c.send_message = lambda msg, parse_mode="HTML": sent.append(msg) or True
    c._icici_otp_getter = lambda: "123456"

    c._ensure_icici_session_before_bot_start()

    assert next(i for i, c in enumerate(calls) if c[0] == "preflight") < next(i for i, c in enumerate(calls) if c[0] == "refresh")
    assert ("refresh", True) in calls
    assert any("ICICI Breeze Ready" in m for m in sent)


def test_telegram_plain_six_digit_otp_is_consumed_when_waiting(monkeypatch):
    import telegram.controller as ctl
    c = ctl.TelegramBotController.__new__(ctl.TelegramBotController)
    c._icici_otp_cv = __import__("threading").Condition()
    c._icici_pending_otp = ""
    c._icici_waiting_for_otp = True
    out = ctl.TelegramBotController.handle_command(c, "123456")
    assert "OTP received" in out
    assert c._icici_pending_otp == "123456"


def test_playwright_preflight_extracts_missing_linux_libraries():
    from exchanges.icici.token_generator import _extract_missing_shared_libraries

    msg = """
    Host system is missing dependencies to run browsers.
    Missing libraries:
        libnss3.so
        libatk-1.0.so.0
    chromium: error while loading shared libraries: libxkbcommon.so.0: cannot open shared object file
    libgbm.so.1 => not found
    """
    assert _extract_missing_shared_libraries(msg) == [
        "libatk-1.0.so.0",
        "libgbm.so.1",
        "libnss3.so",
        "libxkbcommon.so.0",
    ]


def test_entry_alert_renders_icici_option_vehicle():
    from telegram.notifier import format_entry_alert

    inst = _nifty_inst(_chain())
    choice = select_contract_for_thesis(inst, "long", underlying_spot=23100, available_funds=10_000)
    apply_contract_choice(inst, choice)
    msg = format_entry_alert(
        side="long",
        entry=72.0,
        sl=54.0,
        tp=112.0,
        qty=50,
        mode="reversion",
        tier="A",
        sl_atr=1.2,
        tp_atr=2.5,
        rr=2.2,
        instrument=inst,
        entry_leverage=1,
        risk_usd=900,
        margin_risk_pct=0.12,
    )
    assert "ENTRY TICKET" in msg
    assert "Option Vehicle" in msg
    assert "BUY CALL" in msg
    assert "NIFTY30JUN30CE23200" in msg
    assert "₹72.00" in msg
    assert "₹112.00" in msg
    assert "$" not in msg


def test_periodic_report_renders_icici_values_in_inr():
    from telegram.notifier import format_periodic_report

    inst = _nifty_inst(_chain())
    msg = format_periodic_report(
        current_price=72.0,
        balance=10_000,
        daily_pnl=125.5,
        total_pnl=250.0,
        total_trades=2,
        win_rate=50.0,
        atr=8.5,
        instrument=inst,
        position={
            "side": "LONG",
            "entry_price": 70.0,
            "sl_price": 55.0,
            "tp_price": 110.0,
            "quantity": 50,
            "unrealized_pnl": 100.0,
        },
    )
    assert "₹72.00" in msg
    assert "₹10,000.00" in msg
    assert "+₹125.50" in msg
    assert "$" not in msg

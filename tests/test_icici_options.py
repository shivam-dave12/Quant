import json
import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from zoneinfo import ZoneInfo

os.environ.setdefault("DELTA_API_KEY", "test")
os.environ.setdefault("DELTA_SECRET_KEY", "test")
os.environ.setdefault("BREEZE_API_KEY", "test")
os.environ.setdefault("BREEZE_SECRET_KEY", "test")

from core.instruments import AssetClass, ExchangeName, ExchangeInstrument, TradableInstrument, instrument_scope
from agents.icici_chain_architect import (
    build_underlying_payload, select_contract_for_thesis, apply_contract_choice,
    build_session_contract_book, select_contract_from_session_book,
    eligible_nfo_master_option_rows, merge_verified_chain_quotes,
)
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
        {"TradingSymbol": "NIFTY30JUN30CE23000", "right": "Call", "strike_price": 23000, "expiry_date": expiry, "ltp": 260.0, "best_bid_price": 259.75, "best_offer_price": 260.25, "best_bid_quantity": 200, "best_offer_quantity": 200, "LotSize": 50, "stock_code": "NIFTY", "exchange_code": "NFO", "product_type": "Options"},
        {"TradingSymbol": "NIFTY30JUN30CE23200", "right": "Call", "strike_price": 23200, "expiry_date": expiry, "ltp": 72.0, "best_bid_price": 71.75, "best_offer_price": 72.25, "best_bid_quantity": 200, "best_offer_quantity": 200, "LotSize": 50, "stock_code": "NIFTY", "exchange_code": "NFO", "product_type": "Options"},
        {"TradingSymbol": "NIFTY30JUN30PE23000", "right": "Put", "strike_price": 23000, "expiry_date": expiry, "ltp": 78.0, "best_bid_price": 77.75, "best_offer_price": 78.25, "best_bid_quantity": 200, "best_offer_quantity": 200, "LotSize": 50, "stock_code": "NIFTY", "exchange_code": "NFO", "product_type": "Options"},
    ]


class _BreezeFakeAuth:
    api_key = "app-key"
    secret_key = "secret"

    def get_session(self, force_refresh=False):
        return SimpleNamespace(session_token="session-token")

    def can_refresh_without_operator(self):
        return False


class _BreezeFakeResponse:
    status_code = 200

    def json(self):
        return {"Success": [], "Status": 200, "Error": None}


class _BreezeRecordingHTTP:
    def __init__(self):
        self.calls = []
        self.get_calls = []

    def request(self, method, url, headers=None, data=None, timeout=None):
        self.calls.append({"method": method, "url": url, "headers": headers, "data": data, "timeout": timeout})
        return _BreezeFakeResponse()

    def get(self, url, headers=None, params=None, timeout=None):
        self.get_calls.append({"method": "GET", "url": url, "headers": headers, "params": params, "timeout": timeout})
        return _BreezeFakeResponse()


def _breeze_test_client():
    from exchanges.icici.api import BreezeRestClient

    client = BreezeRestClient(auth=_BreezeFakeAuth())
    client.http = _BreezeRecordingHTTP()
    return client


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


def test_icici_selector_stores_black_scholes_iv_and_budget_metadata():
    inst = _nifty_inst(_chain())
    choice = select_contract_for_thesis(inst, "long", underlying_spot=23100, available_funds=10_000)

    assert choice is not None
    assert choice.raw["bs_volatility_source"] in {"quote_implied", "live_field", "stress_prior"}
    assert 0.03 <= choice.raw["bs_volatility"] <= 1.50
    assert choice.raw["bs_snapshot"]["delta"] == choice.delta
    assert choice.raw["selected_contract_cost"] == 72.0 * 50
    assert choice.raw["selected_max_contract_cost"] == 4200.0
    assert 0.0 < choice.raw["selected_contract_utilization"] <= 1.0
    assert any(str(reason).startswith("iv=") for reason in choice.reasons)


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


def test_no_portfolio_overlay_can_inject_alpha_into_nifty_or_any_desk():
    import inspect
    from orchestration.multi_asset_bot import MultiAssetQuantBot
    from strategy.quant_strategy import QuantStrategy

    orchestrator_src = inspect.getsource(MultiAssetQuantBot)
    strategy_src = inspect.getsource(QuantStrategy)
    assert "_update_cross_asset_overlay" not in orchestrator_src
    assert "set_cross_asset_state" not in strategy_src
    assert "CrossAssetRegimeEngine" not in orchestrator_src

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
            "segment": "fno",
            "product_type": "Options",
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
    assert detected["size"] == 0.0
    assert detected["side"] is None
    assert detected["position_scope_verified"] is True
    assert detected["ignored_non_option_rows"] == 1


def test_icici_reconcile_can_reconstruct_exact_option_vehicle():
    inst = _nifty_inst(_chain())
    expiry = inst.primary.raw["chain_candidates"][1]["expiry_date"]
    ex_pos = {
        "side": "LONG",
        "size": 50,
        "entry_price": 72.0,
        "raw": {
            "segment": "fno",
            "product_type": "Options",
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


def test_icici_security_master_fonse_nifty_rows_are_eligible():
    expiry = (datetime.now(timezone.utc) + timedelta(days=4)).strftime("%d-%b-%Y")
    rows = [{
        "Token": "35036",
        "InstrumentName": "OPTIDX",
        "ShortName": "NIFTY",
        "Series": "OPTION",
        "ExpiryDate": expiry,
        "StrikePrice": "23100",
        "OptionType": "CE",
        "LotSize": "65",
        "MinimumLotQty": "0",
        "CompanyName": "NIFTY 50",
        "ExchangeCode": "NIFTY 50",
        "ExAllowed": "NFO",
        "_source_file": "FONSEScripMaster.txt",
    }]

    eligible = eligible_nfo_master_option_rows(rows, "NIFTY")

    assert len(eligible) == 1
    assert eligible[0]["exchange_code"] == "NFO"
    assert eligible[0]["stock_code"] == "NIFTY"
    assert eligible[0]["runtime_lot_size"] == 65
    assert eligible[0]["right"] == "Call"
    assert eligible[0]["strike_price"] == 23100



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


def test_breeze_token_service_refreshes_stale_daily_session_cache(tmp_path):
    import time
    from exchanges.icici.breeze_auth import BreezeSession, BreezeTokenService

    cache = tmp_path / "icici_breeze_session.json"
    svc = BreezeTokenService(
        api_key="app-key",
        secret_key="secret",
        api_session="fresh-api-session",
        cache_path=cache,
        ttl_sec=86400 * 7,
    )
    svc._save_cache(BreezeSession("old-api-session", "old-session-token", time.time() - 2 * 86400, {}))
    calls = []

    def exchange(api_session):
        calls.append(api_session)
        return BreezeSession(api_session, "new-session-token", time.time(), {"source": "test"})

    svc.exchange_api_session = exchange
    session = svc.get_session(force_refresh=False)

    assert calls == ["fresh-api-session"]
    assert session.session_token == "new-session-token"
    assert svc.session_status(session)["same_trading_day"] is True


def test_breeze_token_service_rejects_manual_session_token_by_default(monkeypatch, tmp_path):
    from exchanges.icici import breeze_auth
    from exchanges.icici.breeze_auth import BreezeTokenService

    monkeypatch.setattr(breeze_auth.config, "ICICI_ALLOW_MANUAL_SESSION_TOKEN_OVERRIDE", False, raising=False)
    monkeypatch.setattr(breeze_auth.config, "ICICI_API_SESSION_FILE_MUST_BE_TODAY", True, raising=False)
    svc = BreezeTokenService(
        api_key="app-key",
        secret_key="secret",
        session_token="manual-session-token",
        api_session_path=tmp_path / "missing_api_session.txt",
        cache_path=tmp_path / "cache.json",
    )

    try:
        svc.refresh()
    except RuntimeError as exc:
        msg = str(exc)
    else:
        raise AssertionError("manual session token should not bypass daily API_Session generation")

    assert "BREEZE_SESSION_TOKEN" in msg
    assert "API_Session -> CustomerDetails" in msg
    assert svc.can_refresh_without_operator() is False


def test_breeze_token_service_ignores_stale_api_session_file_for_daily_generation(monkeypatch, tmp_path):
    import time
    from exchanges.icici import breeze_auth
    from exchanges.icici.breeze_auth import BreezeTokenService

    monkeypatch.setattr(breeze_auth.config, "ICICI_API_SESSION_FILE_MUST_BE_TODAY", True, raising=False)
    api_session_file = tmp_path / "icici_api_session.txt"
    api_session_file.write_text("stale-api-session\n", encoding="utf-8")
    stale_mtime = time.time() - 2 * 86400
    os.utime(api_session_file, (stale_mtime, stale_mtime))
    svc = BreezeTokenService(
        api_key="app-key",
        secret_key="secret",
        api_session_path=api_session_file,
        cache_path=tmp_path / "cache.json",
    )

    assert svc._configured_api_session() == ""
    assert svc.can_refresh_without_operator() is False


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


def test_icici_position_filter_rejects_non_fno_positive_quantity_row_as_flat():
    from execution.order_manager import _ICICIAdapter

    raw = build_underlying_payload("NIFTY", "ICICI_INDEX_OPTIONS", [])
    exchange_inst = SimpleNamespace(symbol="NIFTY", display_symbol="NIFTY", tick_size=0.05, lot_step=1.0, min_qty=1.0, max_qty=0.0, raw=raw)
    adapter = _ICICIAdapter(api=SimpleNamespace(), exchange_instrument=exchange_inst)
    payload = {"Success": [{"exchange_code": "NSE", "product_type": "Cash", "quantity": 639, "average_price": 1792.08}]}

    pos = adapter.normalise_position(payload)
    assert pos["size"] == 0.0
    assert pos["ignored_non_option_rows"] == 1


def test_icici_stoploss_payload_is_nfo_option_stoploss_and_quantity_is_floored():
    from execution.order_manager import _ICICIAdapter

    expiry = (datetime.now(timezone.utc) + timedelta(days=10)).strftime("%Y-%m-%d")
    raw = {
        "selected_option_contract": {"raw": {
            "stock_code": "NIFTY", "exchange_code": "NFO", "right": "Call",
            "strike_price": "23200", "expiry_date": expiry, "LotSize": 50,
            "selected_entry_premium": 72.0,
        }}
    }
    api = SimpleNamespace(_normalise_expiry=lambda x: x, _normalise_right=lambda x: str(x).lower())
    exchange_inst = SimpleNamespace(symbol="NIFTY", display_symbol="NIFTY", tick_size=0.05, lot_step=1.0, min_qty=1.0, max_qty=0.0, raw=raw)
    adapter = _ICICIAdapter(api=api, exchange_instrument=exchange_inst)

    body = adapter._order_body("sell", "limit_order", 99, price=65.0, trigger_price=66.0, reduce_only=True, stop_order_type="stop_loss_order")
    assert body["order_type"] == "stoploss"
    assert body["stoploss"] == "66.0"
    assert body["quantity"] == 50
    assert body["exchange_code"] == "NFO"
    assert body["product"] == "options"


def test_risk_manager_preserves_icici_fno_balance_provenance():
    from risk.risk_manager import RiskManager

    class Router:
        def get_balance(self):
            return {
                "available": 7500.0, "locked": 2500.0, "total": 10000.0,
                "currency": "INR", "segment": "FNO",
                "source": "funds.allocated_fno_minus_block_by_trade_fno",
                "fno_allocated": 10000.0, "fno_blocked": 2500.0, "nfo_cash_limit": 3000.0,
            }

    rm = RiskManager(Router())
    first = rm.get_available_balance()
    second = rm.get_available_balance()
    assert first["segment"] == "FNO"
    assert first["source"].startswith("funds.allocated_fno")
    assert first["fno_allocated"] == 10000.0
    assert second["cached"] is True and second["segment"] == "FNO"


def test_icici_breeze_market_order_guard_permits_stoploss_but_rejects_market():
    from exchanges.icici.api import BreezeRestClient

    client = BreezeRestClient.__new__(BreezeRestClient)
    captured = []
    client.request = lambda method, path, body: captured.append((method, path, body)) or {"Success": {"order_id": "x"}}
    payload = {
        "stock_code": "NIFTY", "exchange_code": "NFO", "product": "options",
        "action": "sell", "order_type": "stoploss", "quantity": 50, "price": "65",
        "validity": "day", "expiry_date": "2026-05-28", "right": "call",
        "strike_price": "23200", "stoploss": "66",
    }
    assert client.place_order(**payload)["Success"]["order_id"] == "x"
    assert captured[-1][2]["order_type"] == "stoploss"
    bad = dict(payload); bad["order_type"] = "market"
    import pytest
    with pytest.raises(RuntimeError):
        client.place_order(**bad)


def test_icici_order_recovery_accepts_exact_nfo_option_order_without_segment_field():
    from execution.order_manager import _ICICIAdapter

    expiry = (datetime.now(timezone.utc) + timedelta(days=10)).strftime("%Y-%m-%d")
    selected = {"stock_code": "NIFTY", "exchange_code": "NFO", "product_type": "Options", "right": "Call", "strike_price": "23200", "expiry_date": expiry}
    raw = {"selected_option_contract": {"raw": selected}}
    class API:
        _normalise_expiry = staticmethod(lambda x: x)
        def get_order_list(self, **kwargs):
            return {"Success": [{
                "order_id": "NFO-SL-1", "exchange_code": "NFO", "product_type": "Options",
                "stock_code": "NIFTY", "right": "Call", "strike_price": "23200",
                "expiry_date": expiry, "order_type": "Stoploss", "stoploss": "65.0",
                "status": "Ordered",
            }]}
    exchange_inst = SimpleNamespace(symbol="NIFTY", display_symbol="NIFTY", tick_size=0.05, lot_step=1.0, min_qty=1.0, max_qty=0.0, raw=raw)
    adapter = _ICICIAdapter(api=API(), exchange_instrument=exchange_inst)
    orders = adapter.get_open_orders("NIFTY")
    assert len(orders) == 1
    assert orders[0]["type"] == "STOP_LOSS"
    assert orders[0]["trigger_price"] == 65.0


def test_icici_preflight_accepts_verified_fno_flat_and_rejects_unknown_funds():
    from orchestration.multi_asset_bot import MultiAssetQuantBot

    bot = MultiAssetQuantBot.__new__(MultiAssetQuantBot)
    ctx = SimpleNamespace(
        instrument=SimpleNamespace(asset_id="NIFTY"),
        risk_manager=SimpleNamespace(get_available_balance=lambda: {
            "available": 7500.0, "segment": "FNO", "source": "funds.allocated_fno_minus_block_by_trade_fno",
            "fno_allocated": 10000.0, "fno_blocked": 2500.0, "nfo_cash_limit": 3000.0}),
        execution_router=SimpleNamespace(get_open_position=lambda: {"size": 0.0, "ignored_non_option_rows": 1}),
    )
    assert bot._icici_account_preflight(ctx) is True
    ctx.risk_manager = SimpleNamespace(get_available_balance=lambda: {"available": 100.0, "segment": "CASH", "source": "wrong"})
    assert bot._icici_account_preflight(ctx) is False


def test_icici_position_accepts_exact_nfo_option_when_portfolio_segment_is_omitted():
    from execution.order_manager import _ICICIAdapter
    raw = build_underlying_payload("NIFTY", "ICICI_INDEX_OPTIONS", [])
    exchange_inst = SimpleNamespace(symbol="NIFTY", display_symbol="NIFTY", tick_size=0.05, lot_step=1.0, min_qty=1.0, max_qty=0.0, raw=raw)
    adapter = _ICICIAdapter(api=SimpleNamespace(), exchange_instrument=exchange_inst)
    expiry = (datetime.now(timezone.utc) + timedelta(days=10)).strftime("%Y-%m-%d")
    pos = adapter.normalise_position({"Success": [{
        "exchange_code": "NFO", "product_type": "Options", "stock_code": "NIFTY",
        "right": "Call", "strike_price": "23200", "expiry_date": expiry,
        "quantity": 50, "average_price": 72.0,
    }]})
    assert pos["side"] == "LONG" and pos["size"] == 50
    assert pos["requires_contract_reconstruction"] is True


def test_icici_selector_refuses_contract_without_verified_nfo_lot_size():
    chain = [dict(row) for row in _chain()]
    for row in chain:
        row.pop("LotSize", None)
    assert select_contract_for_thesis(_nifty_inst(chain), "long", underlying_spot=23100, available_funds=10_000) is None


def test_icici_order_routing_refuses_contract_without_verified_lot_size():
    import pytest
    from execution.order_manager import _ICICIAdapter
    expiry = (datetime.now(timezone.utc) + timedelta(days=10)).strftime("%Y-%m-%d")
    raw = {"selected_option_contract": {"raw": {
        "stock_code": "NIFTY", "exchange_code": "NFO", "product_type": "Options",
        "right": "Call", "strike_price": "23200", "expiry_date": expiry,
        "selected_entry_premium": 72.0,
    }}}
    api = SimpleNamespace(_normalise_expiry=lambda x: x, _normalise_right=lambda x: str(x).lower())
    inst = SimpleNamespace(symbol="NIFTY", display_symbol="NIFTY", tick_size=0.05, lot_step=1.0, min_qty=1.0, max_qty=0.0, raw=raw)
    with pytest.raises(RuntimeError, match="verified NFO option lot size"):
        _ICICIAdapter(api=api, exchange_instrument=inst)._order_body("buy", "limit", 50, price=72.0)


def test_icici_v2_historical_fallback_uses_v2_interval_vocabulary(monkeypatch):
    from exchanges.icici.data_manager import ICICIOptionDataManager
    from exchanges.icici.underlying_data_manager import ICICIUnderlyingDataManager
    import exchanges.icici.data_manager as option_module
    import exchanges.icici.underlying_data_manager as under_module
    monkeypatch.setattr(option_module, "breeze_throttle", lambda *args, **kwargs: None)
    monkeypatch.setattr(under_module, "breeze_throttle", lambda *args, **kwargs: None)
    inst = _nifty_inst(_chain())
    apply_contract_choice(inst, select_contract_for_thesis(inst, "long", underlying_spot=23100, available_funds=10000))
    class API:
        _normalise_expiry = staticmethod(lambda x: x)
        _normalise_right = staticmethod(lambda x: str(x).lower())
        def __init__(self): self.v2 = []
        def get_historical_charts(self, **kwargs): return {"Success": []}
        def get_historical_charts_v2(self, **kwargs): self.v2.append(kwargs); return {"Success": []}
    opt_api = API(); ICICIOptionDataManager(inst, api=opt_api)._load_historical("1m")
    under_api = API(); ICICIUnderlyingDataManager(inst, api=under_api)._load_historical("1d")
    assert opt_api.v2[-1]["interval"] == "1minute"
    assert under_api.v2[-1]["interval"] == "1day"


def test_strategy_exit_path_cancels_protection_then_records_manual_exit_order():
    from execution.order_manager import CancelResult
    from strategy.quant_strategy import QuantStrategy, PositionState, PositionPhase
    qs = QuantStrategy.__new__(QuantStrategy)
    qs._instrument = _nifty_inst(_chain())
    qs._lock = __import__("threading").RLock()
    qs._exiting_since = 0.0
    qs._last_reconcile_time = 1.0
    qs._pos = PositionState(phase=PositionPhase.ACTIVE, side="long", quantity=50, entry_price=72.0, sl_price=55.0, tp_price=110.0, sl_order_id="sl-1", tp_order_id="")
    qs._cancel_tp_ladder_orders = lambda om, pos: None
    called = []
    class OM:
        def cancel_all_exit_orders(self, sl, tp): called.append(("cancel", sl, tp)); return CancelResult.SUCCESS, CancelResult.NOT_FOUND
        def place_market_order(self, side, quantity, reduce_only): called.append(("close", side, quantity, reduce_only)); return {"order_id": "exit-1"}
        def place_stop_loss(self, **kwargs): raise AssertionError("restore not required")
    assert qs._exit_trade(OM(), 111.0, "icici_tp_target_reached") is True
    assert called[0][0] == "cancel" and called[1][0] == "close"
    assert qs._pos.phase == PositionPhase.EXITING and qs._pos.manual_exit_order_id == "exit-1"


def test_icici_entry_source_does_not_place_parallel_tp_sells_with_broker_sl():
    from pathlib import Path
    src = Path(__file__).parents[1].joinpath("strategy", "quant_strategy.py").read_text(encoding="utf-8")
    assert "ICICI single-live-exit invariant" in src
    assert "ICICI TP_LADDER analytical-only" in src


def test_portfolio_pnl_keeps_usd_and_inr_ledgers_separate():
    from orchestration.multi_asset_bot import MultiAssetQuantBot
    bot = MultiAssetQuantBot.__new__(MultiAssetQuantBot)
    bot.contexts = []
    bot.guard = SimpleNamespace(max_open_positions=6, budget_mode="portfolio")
    bot._all_trade_records = lambda: [
        {"desk": "BTC", "asset": "BTC", "side": "LONG", "pnl": 10.0, "currency": "$", "reason": "tp"},
        {"desk": "ICICI_INDEX_OPTIONS", "asset": "NIFTY", "side": "LONG", "pnl": -100.0, "currency": "₹", "reason": "sl"},
    ]
    report = bot.format_portfolio_pnl_report()
    assert "<b>USD</b>" in report and "$+10.00" in report
    assert "<b>INR</b>" in report and "₹-100.00" in report
    assert "$-90.00" not in report and "₹-90.00" not in report


def test_icici_session_book_preselects_ce_and_pe_but_signal_activates_direction():
    inst = _nifty_inst(_chain())
    book = build_session_contract_book(inst, underlying_spot=23100, available_funds=10_000)
    assert book is not None
    assert book.call.right == "call" and book.put.right == "put"
    bullish, bullish_status = select_contract_from_session_book(inst, "long", underlying_spot=23100, available_funds=10_000)
    bearish, bearish_status = select_contract_from_session_book(inst, "short", underlying_spot=23100, available_funds=10_000)
    assert bullish_status == bearish_status == "session_contract_ready"
    assert bullish.right == "call" and bearish.right == "put"
    assert bullish.selected_symbol != bearish.selected_symbol


def test_icici_session_book_forces_fresh_daily_reselection_after_date_rollover():
    inst = _nifty_inst(_chain())
    assert build_session_contract_book(inst, underlying_spot=23100, available_funds=10_000) is not None
    inst.primary.raw["session_contract_book"]["trade_date_ist"] = "2000-01-01"

    choice, status = select_contract_from_session_book(inst, "long", underlying_spot=23100, available_funds=10_000)

    assert choice is None
    assert status == "session_contract_book_new_trading_day"


def test_icici_preselected_book_is_reported_before_signal_vehicle_activation():
    from exchanges.icici.data_manager import ICICIOptionDataManager
    from strategy.quant_strategy import QuantStrategy

    inst = _nifty_inst(_chain())
    assert build_session_contract_book(inst, underlying_spot=23100, available_funds=10_000) is not None
    status = ICICIOptionDataManager(inst, api=SimpleNamespace()).session_contract_book_status()

    class DM:
        def get_orderbook(self):
            return {"bids": [], "asks": [], "timestamp": 0.0}
        def get_session_contract_book_status(self):
            return status

    qs = QuantStrategy(instrument=inst)
    qs._atr_5m._atr = 50.0
    ok, cost = qs._spread_atr_gate(DM())

    assert ok is True and cost == 0.0
    assert qs._last_spread_gate_context["book_status"] == "SESSION_PRESELECTED"
    assert qs._last_spread_gate_context["session_call_symbol"]
    assert qs._last_spread_gate_context["session_put_symbol"]


def test_icici_session_book_refresh_required_after_material_spot_drift(monkeypatch):
    import agents.icici_chain_architect as arch
    monkeypatch.setattr(arch.config, "ICICI_SESSION_BOOK_MAX_SPOT_DRIFT_PCT", 0.005, raising=False)
    inst = _nifty_inst(_chain())
    assert build_session_contract_book(inst, underlying_spot=23100, available_funds=10_000) is not None
    choice, status = select_contract_from_session_book(inst, "long", underlying_spot=23400, available_funds=10_000)
    assert choice is None and status == "session_contract_spot_drift"


def test_icici_session_selector_rejects_non_executable_or_shallow_quotes():
    chain = [dict(row) for row in _chain()]
    for row in chain:
        if str(row["right"]).lower() == "call":
            row["best_bid_price"] = 0.0
            row["best_offer_price"] = 0.0
    assert select_contract_for_thesis(_nifty_inst(chain), "long", underlying_spot=23100, available_funds=10_000) is None
    chain = [dict(row) for row in _chain()]
    for row in chain:
        if str(row["right"]).lower() == "put":
            row["best_bid_quantity"] = 1
            row["best_offer_quantity"] = 1
    assert select_contract_for_thesis(_nifty_inst(chain), "short", underlying_spot=23100, available_funds=10_000) is None


def test_icici_filtered_chain_requests_use_expiry_and_right_and_master_lot(monkeypatch):
    import exchanges.icici.data_manager as dm_module
    from exchanges.icici.data_manager import ICICIOptionDataManager
    monkeypatch.setattr(dm_module, "breeze_throttle", lambda *args, **kwargs: None)
    inst = _nifty_inst([])
    expiry = (datetime.now(timezone.utc) + timedelta(days=10)).strftime("%Y-%m-%d")
    master = [
        {"TradingSymbol": "NIFTY_C", "right": "Call", "strike_price": 23200, "expiry_date": expiry, "LotSize": 50, "stock_code": "NIFTY", "exchange_code": "NFO", "product_type": "Options"},
        {"TradingSymbol": "NIFTY_P", "right": "Put", "strike_price": 23000, "expiry_date": expiry, "LotSize": 50, "stock_code": "NIFTY", "exchange_code": "NFO", "product_type": "Options"},
    ]
    class API:
        def __init__(self): self.calls = []
        def preflight_session(self): return {}
        def get_security_master_rows(self, **kwargs):
            assert kwargs["require_current_trade_date"] is True
            return master
        @staticmethod
        def _normalise_expiry(value): return value
        def get_option_chain_quotes(self, **kwargs):
            self.calls.append(kwargs)
            row = master[0] if kwargs["right"] == "call" else master[1]
            out = dict(row)
            out.pop("LotSize")
            out.update({"best_bid_price": 71.75, "best_offer_price": 72.25, "best_bid_quantity": 200, "best_offer_quantity": 200, "ltp": 72.0})
            return {"Success": [out]}
    api = API(); dm = ICICIOptionDataManager(inst, api=api)
    assert dm._hydrate_chain_candidates(force_refresh=True) is True
    assert len(api.calls) == 2 and all(x.get("expiry_date") and x.get("right") for x in api.calls)
    assert all(x["runtime_lot_size"] == 50 for x in inst.primary.raw["chain_candidates"])
    assert all(x["instrument_definition_source"] == "daily_security_master" for x in inst.primary.raw["chain_candidates"])


def test_breeze_option_chain_uses_official_sdk_route_payload_and_headers():
    client = _breeze_test_client()
    client.get_option_chain_quotes(
        stock_code="NIFTY",
        exchange_code="NFO",
        product_type="options",
        expiry_date="2026-05-26T06:00:00.000Z",
        right="call",
        strike_price="",
    )
    call = client.http.calls[0]
    assert call["method"] == "GET"
    assert call["url"].endswith("/api/v1/optionchain")
    assert not call["url"].endswith("/api/v1/OptionChain")
    body = json.loads(call["data"])
    assert body == {
        "stock_code": "NIFTY",
        "exchange_code": "NFO",
        "product_type": "options",
        "expiry_date": "2026-05-26T06:00:00.000Z",
        "right": "call",
    }
    assert call["headers"].get("User-Agent", "").startswith("Mozilla/5.0")


def test_breeze_rest_market_data_routes_match_official_sdk_payloads():
    client = _breeze_test_client()
    client.get_quotes(
        stock_code="NIFTY",
        exchange_code="NFO",
        expiry_date="2026-05-26T06:00:00.000Z",
        product_type="options",
        right="call",
        strike_price="",
    )
    client.get_historical_charts(
        interval="1minute",
        from_date="2026-05-22T09:15:00.000Z",
        to_date="2026-05-22T10:15:00.000Z",
        stock_code="NIFTY",
        exchange_code="NFO",
        product_type="options",
        expiry_date="2026-05-26T06:00:00.000Z",
        right="put",
        strike_price="23000",
    )
    client.get_historical_charts_v2(
        interval="1minute",
        from_date="2026-05-22 09:15:00",
        to_date="2026-05-22 10:15:00",
        stock_code="NIFTY",
        exchange_code="NFO",
        product_type="Options",
        right="",
    )

    quote_call, hist_call = client.http.calls
    assert quote_call["url"].endswith("/api/v1/quotes")
    assert json.loads(quote_call["data"]) == {
        "stock_code": "NIFTY",
        "exchange_code": "NFO",
        "expiry_date": "2026-05-26T06:00:00.000Z",
        "product_type": "options",
        "right": "call",
    }
    assert hist_call["url"].endswith("/api/v1/historicalcharts")
    assert json.loads(hist_call["data"]) == {
        "interval": "minute",
        "from_date": "2026-05-22T09:15:00.000Z",
        "to_date": "2026-05-22T10:15:00.000Z",
        "stock_code": "NIFTY",
        "exchange_code": "NFO",
        "product_type": "options",
        "expiry_date": "2026-05-26T06:00:00.000Z",
        "right": "put",
        "strike_price": "23000",
    }
    assert client.http.get_calls[0]["url"].endswith("/api/v2/historicalcharts")
    assert client.http.get_calls[0]["params"] == {
        "interval": "1minute",
        "from_date": "2026-05-22 09:15:00",
        "to_date": "2026-05-22 10:15:00",
        "stock_code": "NIFTY",
        "product_type": "Options",
        "exch_code": "NFO",
    }


def test_breeze_order_and_position_management_routes_match_official_sdk_payloads():
    client = _breeze_test_client()
    client.place_order(
        stock_code="nifty",
        exchange_code="nfo",
        product="Options",
        action="BUY",
        order_type="LIMIT",
        quantity=65,
        price="72.10",
        validity="DAY",
        expiry_date="2026-05-26",
        right="CE",
        strike_price="23000",
        user_remark="",
    )
    client.get_order_detail(exchange_code="NFO", order_id="OID123")
    client.get_order_list(exchange_code="NFO", from_date="2026-05-22T00:00:00.000Z", to_date="2026-05-22T23:59:59.000Z")
    client.cancel_order(order_id="OID123", exchange_code="NFO")
    client.modify_order(order_id="OID123", exchange_code="NFO", order_type="limit", price="72.25", validity="day", stoploss="")
    client.square_off(
        stock_code="NIFTY",
        exchange_code="NFO",
        quantity=65,
        price="71.50",
        action="sell",
        order_type="limit",
        validity="day",
        product="options",
        expiry_date="2026-05-26T06:00:00.000Z",
        right="put",
        strike_price="23000",
        stoploss="70.00",
    )
    client.get_trade_list(
        exchange_code="NFO",
        from_date="2026-05-22T00:00:00.000Z",
        to_date="2026-05-22T23:59:59.000Z",
        product_type="options",
        action="",
        stock_code="NIFTY",
    )
    client.get_trade_detail(exchange_code="NFO", order_id="OID123")

    calls = client.http.calls
    assert calls[0]["method"] == "POST" and calls[0]["url"].endswith("/api/v1/order")
    assert json.loads(calls[0]["data"]) == {
        "stock_code": "NIFTY",
        "exchange_code": "NFO",
        "product": "options",
        "action": "buy",
        "order_type": "limit",
        "quantity": 65,
        "price": "72.10",
        "validity": "day",
        "expiry_date": "2026-05-26T06:00:00.000Z",
        "right": "call",
        "strike_price": "23000",
    }
    assert json.loads(calls[1]["data"]) == {"exchange_code": "NFO", "order_id": "OID123"}
    assert json.loads(calls[2]["data"]) == {
        "exchange_code": "NFO",
        "from_date": "2026-05-22T00:00:00.000Z",
        "to_date": "2026-05-22T23:59:59.000Z",
    }
    assert calls[3]["method"] == "DELETE"
    assert json.loads(calls[3]["data"]) == {"exchange_code": "NFO", "order_id": "OID123"}
    assert calls[4]["method"] == "PUT"
    assert json.loads(calls[4]["data"]) == {
        "order_id": "OID123",
        "exchange_code": "NFO",
        "order_type": "limit",
        "price": "72.25",
        "validity": "day",
    }
    assert calls[5]["url"].endswith("/api/v1/squareoff")
    assert json.loads(calls[5]["data"]) == {
        "stock_code": "NIFTY",
        "exchange_code": "NFO",
        "quantity": 65,
        "price": "71.50",
        "action": "sell",
        "order_type": "limit",
        "validity": "day",
        "stoploss_price": "70.00",
        "product_type": "options",
        "expiry_date": "2026-05-26T06:00:00.000Z",
        "right": "put",
        "strike_price": "23000",
    }
    assert calls[6]["url"].endswith("/api/v1/trades")
    assert json.loads(calls[6]["data"]) == {
        "exchange_code": "NFO",
        "from_date": "2026-05-22T00:00:00.000Z",
        "to_date": "2026-05-22T23:59:59.000Z",
        "product_type": "options",
        "stock_code": "NIFTY",
    }
    assert json.loads(calls[7]["data"]) == {"exchange_code": "NFO", "order_id": "OID123"}


def test_icici_breeze_throttle_default_stays_below_official_call_rate_limit():
    import config

    assert config.ICICI_BREEZE_MIN_CALL_GAP_SEC >= 0.60


def test_icici_quotes_fallback_hydrates_when_option_chain_facility_disabled(monkeypatch):
    import exchanges.icici.data_manager as dm_module
    from exchanges.icici.data_manager import ICICIOptionDataManager
    monkeypatch.setattr(dm_module, "breeze_throttle", lambda *args, **kwargs: None)
    inst = _nifty_inst([])
    expiry = (datetime.now(timezone.utc) + timedelta(days=10)).strftime("%Y-%m-%d")
    master = [
        {"TradingSymbol": "NIFTY_C_ATM", "right": "Call", "strike_price": 23200, "expiry_date": expiry, "LotSize": 50, "stock_code": "NIFTY", "exchange_code": "NFO", "product_type": "Options"},
        {"TradingSymbol": "NIFTY_C_FAR", "right": "Call", "strike_price": 24000, "expiry_date": expiry, "LotSize": 50, "stock_code": "NIFTY", "exchange_code": "NFO", "product_type": "Options"},
        {"TradingSymbol": "NIFTY_P_ATM", "right": "Put", "strike_price": 23000, "expiry_date": expiry, "LotSize": 50, "stock_code": "NIFTY", "exchange_code": "NFO", "product_type": "Options"},
        {"TradingSymbol": "NIFTY_P_FAR", "right": "Put", "strike_price": 22000, "expiry_date": expiry, "LotSize": 50, "stock_code": "NIFTY", "exchange_code": "NFO", "product_type": "Options"},
    ]
    class API:
        def __init__(self):
            self.option_chain_calls = 0
            self.quote_calls = []
        def preflight_session(self): return {}
        def get_security_master_rows(self, **kwargs): return master
        @staticmethod
        def _normalise_expiry(value): return value
        def get_option_chain_quotes(self, **kwargs):
            self.option_chain_calls += 1
            raise RuntimeError("Breeze /OptionChain failed HTTP 401: This facility is not Enable for the User")
        def get_quote_for_instrument(self, instrument):
            raw = instrument.raw
            self.quote_calls.append((raw["right"], float(raw["strike_price"])))
            px = 72.0 if str(raw["right"]).lower() == "call" else 78.0
            return {"Success": [{"ltp": px, "best_bid_price": px - 0.25, "best_offer_price": px + 0.25, "best_bid_quantity": 200, "best_offer_quantity": 200}]}
    api = API(); dm = ICICIOptionDataManager(inst, api=api)
    assert dm._hydrate_chain_candidates(force_refresh=True, underlying_spot=23100) is True
    assert api.option_chain_calls == 2
    assert api.quote_calls
    assert inst.primary.raw["chain_source"] == "daily_security_master_plus_quotes_fallback"
    assert {x["quote_source"] for x in inst.primary.raw["chain_candidates"]} == {"breeze_quotes_contract_fallback"}
    assert all(x["runtime_lot_size"] == 50 for x in inst.primary.raw["chain_candidates"])


def test_icici_release_execution_vehicle_allows_opposite_second_entry():
    from exchanges.icici.data_manager import ICICIOptionDataManager
    inst = _nifty_inst(_chain())
    assert build_session_contract_book(inst, underlying_spot=23100, available_funds=10_000) is not None
    call, _ = select_contract_from_session_book(inst, "long", underlying_spot=23100, available_funds=10_000)
    apply_contract_choice(inst, call)
    dm = ICICIOptionDataManager(inst, api=SimpleNamespace())
    dm._underlying_route_fields = {"stock_code": "NIFTY", "exchange_code": "NFO", "product_type": "option_chain", "right": None, "option_type": None, "strike_price": None, "expiry_date": None, "TradingSymbol": None, "selected_option_contract": None, "selected_entry_premium": None}
    dm.release_execution_vehicle()
    assert inst.primary.raw.get("selected_option_contract") is None
    put, status = select_contract_from_session_book(inst, "short", underlying_spot=23100, available_funds=10_000)
    assert status == "session_contract_ready" and put.right == "put"


def test_icici_security_master_trading_session_refuses_stale_cache(tmp_path, monkeypatch):
    import pytest
    from exchanges.icici.api import BreezeRestClient
    cache = tmp_path / "security_master.zip"
    cache.write_bytes(b"old-cache")
    old = (datetime.now(timezone.utc) - timedelta(days=2)).timestamp()
    os.utime(cache, (old, old))
    client = BreezeRestClient.__new__(BreezeRestClient)
    monkeypatch.setattr(client, "_download_security_master", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("offline")))
    with pytest.raises(RuntimeError, match="stale for today's NFO session"):
        client.get_security_master_rows(cache_path=cache, require_current_trade_date=True)


def test_icici_option_quote_reads_breeze_fields_and_never_fabricates_depth(monkeypatch):
    from exchanges.icici.data_manager import ICICIOptionDataManager
    import exchanges.icici.data_manager as dm_module
    monkeypatch.setattr(dm_module, "breeze_throttle", lambda *args, **kwargs: None)
    inst = _nifty_inst(_chain())
    choice = select_contract_for_thesis(inst, "long", underlying_spot=23100, available_funds=10_000)
    apply_contract_choice(inst, choice)
    class API:
        def get_quote_for_instrument(self, instrument):
            return {"Success": [{"ltp": 72.0, "best_bid_price": 71.75, "best_offer_price": 72.25, "best_bid_quantity": 150, "best_offer_quantity": 100}]}
    dm = ICICIOptionDataManager(inst, api=API())
    dm._refresh_quote()
    book = dm.get_orderbook()
    assert book["bids"] == [[71.75, 150.0]] and book["asks"] == [[72.25, 100.0]]


def test_icici_session_vehicle_rechecks_live_affordability_before_entry(monkeypatch):
    from exchanges.icici.data_manager import ICICIOptionDataManager
    inst = _nifty_inst(_chain())
    assert build_session_contract_book(inst, underlying_spot=23100, available_funds=10_000) is not None
    dm = ICICIOptionDataManager(inst, api=SimpleNamespace())
    released = []
    def activated(choice):
        dm._last_price = 100.0  # cost = ₹5,000 for lot 50, exceeds ₹4,200 allocation
        dm._best_bid = 99.75; dm._best_ask = 100.25
        dm._best_bid_qty = 100; dm._best_ask_qty = 100
        return True
    monkeypatch.setattr(dm, "_activate_session_vehicle", activated)
    monkeypatch.setattr(dm, "release_execution_vehicle", lambda: released.append(True))
    assert dm.select_contract_for_thesis("long", underlying_spot=23100, available_funds=10_000) is None
    assert released == [True]


def test_icici_unfilled_entry_paths_are_required_to_release_option_vehicle():
    from pathlib import Path
    src = Path(__file__).parents[1].joinpath("strategy", "quant_strategy.py").read_text(encoding="utf-8")
    required = [
        "premium_sltp_conversion_rejected", "no_executable_structural_levels", "fee_floor_rejected",
        "sl_missing", "zero_stop_distance", "post_surface_zero_stop_distance", "sizing_rejected",
        "entry_order_not_filled_or_rejected",
    ]
    assert "def _release_icici_vehicle_if_unfilled" in src
    assert all(f'_release_icici_vehicle_if_unfilled("{reason}")' in src for reason in required)


def test_icici_day_start_prewarms_both_vehicles_with_minute_data_and_opposite_second_entry(monkeypatch):
    import time
    from collections import deque
    from exchanges.icici.data_manager import ICICIOptionDataManager
    inst = _nifty_inst(_chain())
    dm = ICICIOptionDataManager(inst, api=SimpleNamespace())
    monkeypatch.setattr(dm, "_hydrate_chain_candidates", lambda force_refresh=False, **kwargs: True)
    def emit_live_state():
        right = str(inst.primary.raw.get("right") or "").lower()
        px = 72.0 if right == "call" else 78.0
        dm._last_price = px
        dm._last_quote_ts = time.time()
        dm._best_bid = px - 0.25; dm._best_ask = px + 0.25
        dm._best_bid_qty = 200; dm._best_ask_qty = 200
    def warmup(historical_only=False):
        emit_live_state()
        dm._candles["1m"] = deque([{"c": dm._last_price, "h": dm._last_price + 1.0, "l": dm._last_price - 1.0} for _ in range(25)], maxlen=600)
        dm._candles["5m"] = deque([{"c": dm._last_price, "h": dm._last_price + 0.8, "l": dm._last_price - 0.8} for _ in range(25)], maxlen=600)
    monkeypatch.setattr(dm, "_warmup", warmup)
    monkeypatch.setattr(dm, "_refresh_quote", emit_live_state)
    # Activation now requires the selected CE/PE websocket; simulate its first
    # live tick so this deterministic session-book test remains network-free.
    def stream_live():
        dm._stream_subscription_ids = ["test-live-option"]
        dm._last_stream_tick_ts = time.time()
        return True
    monkeypatch.setattr(dm, "_start_selected_contract_stream", stream_live)
    monkeypatch.setattr(dm, "_arm_session_book_streams", lambda book: True)
    assert dm.prepare_session_contract_book(23100.0, 10_000.0, reason="test_open") is True
    assert len(dm._contract_snapshots) == 2
    assert all(len(snapshot["candles"]["1m"]) >= 20 for snapshot in dm._contract_snapshots.values())
    dm._running = True  # avoid spawning poll thread in the deterministic test
    call = dm.select_contract_for_thesis("long", underlying_spot=23100, available_funds=10_000)
    assert call.right == "call" and dm.get_last_price() == 72.0
    dm.release_execution_vehicle()
    assert inst.primary.raw.get("selected_option_contract") is None
    dm._running = True
    put = dm.select_contract_for_thesis("short", underlying_spot=23100, available_funds=10_000)
    assert put.right == "put" and dm.get_last_price() == 78.0


def test_icici_execution_rejects_spread_that_consumes_premium_atr(monkeypatch):
    from collections import deque
    from exchanges.icici.data_manager import ICICIOptionDataManager
    inst = _nifty_inst(_chain())
    assert build_session_contract_book(inst, underlying_spot=23100, available_funds=10_000) is not None
    dm = ICICIOptionDataManager(inst, api=SimpleNamespace())
    released = []
    def activated(choice):
        dm._last_price = 72.0
        dm._best_bid = 71.75; dm._best_ask = 72.25  # ₹0.50 spread
        dm._best_bid_qty = 200; dm._best_ask_qty = 200
        # Premium ATR ₹0.20; spread consumes 2.5 ATR and must be refused.
        dm._candles["1m"] = deque([{"c": 72.0, "h": 72.1, "l": 71.9} for _ in range(20)], maxlen=600)
        return True
    monkeypatch.setattr(dm, "_activate_session_vehicle", activated)
    monkeypatch.setattr(dm, "release_execution_vehicle", lambda: released.append(True))
    assert dm.select_contract_for_thesis("long", underlying_spot=23100, available_funds=10_000) is None
    assert released == [True]


def test_icici_failed_session_refresh_never_publishes_unverified_replacement(monkeypatch):
    from exchanges.icici.data_manager import ICICIOptionDataManager
    inst = _nifty_inst(_chain())
    dm = ICICIOptionDataManager(inst, api=SimpleNamespace())
    monkeypatch.setattr(dm, "_hydrate_chain_candidates", lambda force_refresh=False, **kwargs: True)
    # Establish previously verified book directly; refresh must not overwrite it before validation.
    initial = build_session_contract_book(inst, underlying_spot=23100, available_funds=10_000)
    old_book = dict(inst.primary.raw["session_contract_book"])
    monkeypatch.setattr(dm, "_prewarm_session_vehicle", lambda choice: False)
    assert dm.prepare_session_contract_book(23120, 10_000, force_refresh=True, reason="refresh") is False
    assert inst.primary.raw["session_contract_book"] == old_book
    assert inst.primary.raw["session_contract_book"]["underlying_spot"] == initial.underlying_spot


def test_icici_live_quote_cannot_override_verified_security_master_lot_size():
    expiry = (datetime.now(timezone.utc) + timedelta(days=10)).strftime("%Y-%m-%d")
    master = [{"TradingSymbol": "NIFTY_C", "right": "Call", "strike_price": 23200, "expiry_date": expiry, "LotSize": 50, "stock_code": "NIFTY", "exchange_code": "NFO", "product_type": "Options"}]
    quote = [{"TradingSymbol": "NIFTY_C", "right": "Call", "strike_price": 23200, "expiry_date": expiry, "LotSize": 1, "best_bid_price": 71.75, "best_offer_price": 72.25, "best_bid_quantity": 200, "best_offer_quantity": 200, "ltp": 72.0}]
    merged = merge_verified_chain_quotes(master, quote)
    inst = _nifty_inst(merged)
    selected = select_contract_for_thesis(inst, "long", underlying_spot=23100, available_funds=10_000)
    assert selected is not None
    assert selected.raw["runtime_lot_size"] == 50
    assert selected.raw["selected_contract_cost"] == 72.0 * 50


def test_icici_session_book_reselects_when_execution_delta_drifts_out_of_band(monkeypatch):
    import agents.icici_chain_architect as arch
    inst = _nifty_inst(_chain())
    assert build_session_contract_book(inst, underlying_spot=23100, available_funds=10_000) is not None
    monkeypatch.setattr(arch.config, "ICICI_SESSION_BOOK_MAX_SPOT_DRIFT_PCT", 1.0, raising=False)
    monkeypatch.setattr(arch.config, "ICICI_SESSION_BOOK_DELTA_RESELECT_BAND", 0.01, raising=False)
    choice, status = select_contract_from_session_book(inst, "long", underlying_spot=23100, available_funds=10_000)
    assert choice is None and status == "session_contract_delta_drift"


def test_icici_delta_invalidated_vehicle_uses_controlled_urgent_refresh(monkeypatch):
    import time
    import exchanges.icici.data_manager as dm_module
    import agents.icici_chain_architect as arch
    from exchanges.icici.data_manager import ICICIOptionDataManager
    inst = _nifty_inst(_chain())
    assert build_session_contract_book(inst, underlying_spot=23100, available_funds=10_000) is not None
    monkeypatch.setattr(arch.config, "ICICI_SESSION_BOOK_MAX_SPOT_DRIFT_PCT", 1.0, raising=False)
    monkeypatch.setattr(arch.config, "ICICI_SESSION_BOOK_DELTA_RESELECT_BAND", 0.01, raising=False)
    monkeypatch.setattr(dm_module.config, "ICICI_SESSION_BOOK_URGENT_REFRESH_COOLDOWN_SEC", 30.0, raising=False)
    dm = ICICIOptionDataManager(inst, api=SimpleNamespace())
    dm._session_book_last_refresh_attempt_ts = time.time() - 31.0
    attempts = []
    monkeypatch.setattr(dm, "prepare_session_contract_book", lambda *a, **k: attempts.append(k.get("reason")) or False)
    assert dm.select_contract_for_thesis("long", underlying_spot=23100, available_funds=10_000) is None
    assert attempts == ["session_contract_delta_drift"]
    attempts.clear()
    dm._session_book_last_refresh_attempt_ts = time.time()
    assert dm.select_contract_for_thesis("long", underlying_spot=23100, available_funds=10_000) is None
    assert attempts == []


def test_icici_rapid_ce_to_pe_switch_invalidates_old_quote_poll_generation(monkeypatch):
    import time
    import exchanges.icici.data_manager as dm_module
    from exchanges.icici.data_manager import ICICIOptionDataManager
    inst = _nifty_inst(_chain())
    dm = ICICIOptionDataManager(inst, api=SimpleNamespace())
    called = []
    monkeypatch.setattr(dm, "_refresh_quote", lambda: called.append(dm._poll_generation))
    monkeypatch.setattr(dm_module.config, "ICICI_OPTION_QUOTE_POLL_SEC", 0.01, raising=False)
    dm._poll_generation = 1
    dm._running = True
    # A retired CE generation exits immediately after PE activation advanced ownership.
    dm._poll_generation = 2
    dm._poll_loop(1)
    assert called == []
    dm._running = False


def test_icici_security_master_live_sources_use_https_only():
    import config
    from exchanges.icici.api import BreezeRestClient
    urls = (config.ICICI_SECURITY_MASTER_URL, BreezeRestClient.SECURITY_MASTER_URL) + tuple(BreezeRestClient.SECURITY_MASTER_FALLBACK_URLS)
    assert urls and all(url.startswith("https://") for url in urls)

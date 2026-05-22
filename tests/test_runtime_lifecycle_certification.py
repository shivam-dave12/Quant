"""Runtime certification regressions for the ICT/Liquidity lifecycle.

These tests deliberately use deterministic market/broker doubles: no network calls,
no fabricated production results. They execute the actual structural engine and the
actual protected-close/reconciliation code paths that failed in the live run.
"""
from collections import deque
from pathlib import Path
from types import SimpleNamespace
import threading
import time

import pytest

from execution.order_manager import CancelResult, OrderManager
from strategy.entry_engine import EntryEngine, EntryType
from strategy.liquidity_map import LiquidityPool, LiquidityMapSnapshot, PoolSide, PoolStatus, PoolTarget, SweepResult
from strategy.quant_strategy import DailyRiskGate, PositionPhase, PositionState, QuantStrategy

ROOT = Path(__file__).resolve().parents[1]


def _trend(step: float, n: int):
    return [{"o": 100 + i * step, "h": 101 + i * step, "l": 99 + i * step, "c": 100.7 + i * step} for i in range(n)]


def _valid_long_setup(now: float):
    c15 = _trend(0.55, 33)
    c4h = _trend(1.40, 28)
    c5 = [{"o": 99.20, "h": 99.70, "l": 99.00, "c": 99.45} for _ in range(34)]
    for i in range(8, 20):
        c5[i] = {"o": 99.30, "h": 100.00, "l": 99.10, "c": 99.60}
    c5[20] = {"o": 99.40, "h": 99.70, "l": 98.00, "c": 99.30}
    c5[21] = {"o": 99.40, "h": 100.00, "l": 99.30, "c": 99.80}
    c5[22] = {"o": 100.20, "h": 103.20, "l": 100.10, "c": 103.00}
    c5[23] = {"o": 101.30, "h": 103.40, "l": 101.00, "c": 103.10}
    for i in range(24, 33):
        c5[i] = {"o": 103.00, "h": 104.30, "l": 102.90, "c": 104.00}
    c5[33] = {"o": 100.60, "h": 100.80, "l": 100.30, "c": 100.50}
    raid_pool = LiquidityPool(99.0, PoolSide.SSL, "5m", status=PoolStatus.SWEPT, created_at=now - 40)
    raid = SweepResult(raid_pool, 20, 98.0, 1.0, 1.4, 0.91, "long", now - 30)
    bsl_pool = LiquidityPool(110.0, PoolSide.BSL, "15m", status=PoolStatus.DETECTED, created_at=now - 90, htf_count=1)
    target = PoolTarget(bsl_pool, 9.50, "long", 5.0, ["15m"])
    snap = LiquidityMapSnapshot(
        bsl_pools=[target], ssl_pools=[], primary_target=None, recent_sweeps=[raid],
        swept_bsl_levels=[], swept_ssl_levels=[], nearest_bsl_atr=999.0,
        nearest_ssl_atr=999.0, timestamp=now,
    )
    return snap, c5, c15, c4h


def _signal(now: float):
    snap, c5, c15, c4h = _valid_long_setup(now)
    engine = EntryEngine()
    engine.update(snap, 100.50, 1.0, now, candles_5m=c5, candles_15m=c15, candles_4h=c4h)
    signal = engine.get_signal()
    assert signal is not None and signal.entry_type is EntryType.ICT_LIQUIDITY
    return engine, signal


def _minimal_icici_instrument():
    raw = {"icici_underlying_desk": True, "contract_selector_mode": "session_preselected_execution"}
    primary = SimpleNamespace(raw=raw, display_symbol="NIFTY", symbol="NIFTY")
    return SimpleNamespace(asset_id="NIFTY", primary_exchange=SimpleNamespace(value="icici"), primary=primary, by_exchange={})


def _strategy_for_reconciliation(instrument):
    qs = QuantStrategy.__new__(QuantStrategy)
    qs._instrument = instrument
    qs._asset_id = getattr(instrument, "asset_id", "TEST")
    qs._lock = threading.RLock()
    qs._fee_engine = None
    qs._entry_engine = None
    qs._liq_map = None
    qs._risk_manager_ref = None
    qs._risk_gate = DailyRiskGate()
    qs._risk_gate.set_opening_balance(100000.0)
    qs._trade_history = deque(maxlen=200)
    qs._total_trades = 0; qs._winning_trades = 0; qs._total_pnl = 0.0
    qs._pnl_recorded_for = 0.0; qs._exit_completed = False
    qs._exiting_since = 0.0; qs._last_reconcile_time = 0.0; qs._last_exit_time = 0.0
    qs.current_sl_price = 0.0; qs.current_tp_price = 0.0
    qs._last_structure_fingerprint = None; qs._last_exit_side = ""
    qs._last_known_price = 0.0
    qs._send_telegram = lambda message, *args, **kwargs: None
    qs._dm = None
    return qs


class _ManualFillBroker:
    def __init__(self, fill_price: float):
        self.fill_price = fill_price
        self.events = []
        self.n = 0
    def cancel_all_exit_orders(self, sl, tp):
        self.events.append(("cancel", sl, tp))
        return CancelResult.SUCCESS, CancelResult.NOT_FOUND
    def place_market_order(self, side, quantity, reduce_only):
        self.n += 1
        oid = f"priced-close-{self.n}"
        self.events.append(("priced_close", side, quantity, reduce_only, oid))
        return {"order_id": oid}
    def get_fill_details(self, order_id):
        return {"status": "FILLED", "fill_price": self.fill_price, "paid_commission": 2.5, "paid_commission_exact": True}
    def identify_exit_order(self, **kwargs):
        return {"confirmed": False}


def test_runtime_contract_fixes_match_live_crash_sites(monkeypatch):
    source = "\n".join((ROOT / name).read_text() for name in (
        "strategy/quant_strategy.py", "main.py", "telegram/controller.py"))
    assert "analysis_info()" not in source
    import orchestration.multi_asset_bot as module
    monkeypatch.setattr(module, "icici_market_session_state", lambda: SimpleNamespace(is_open=True, reason="synthetic_open"))
    assert module.MultiAssetQuantBot._icici_market_open() == (True, "synthetic_open")


def test_icici_supervised_premium_tp_completes_two_full_cycles_with_exact_fill_ledger():
    qs = _strategy_for_reconciliation(_minimal_icici_instrument())
    notices = []
    qs._send_telegram = lambda message, *args, **kwargs: notices.append(message)
    broker = _ManualFillBroker(fill_price=112.0)
    qs._om = broker
    dm = SimpleNamespace(get_last_price=lambda: 111.0)
    for cycle in range(2):
        qs._exit_completed = False
        qs._pnl_recorded_for = 0.0
        qs._pos = PositionState(
            phase=PositionPhase.ACTIVE, side="long", quantity=50, entry_price=72.0,
            sl_price=55.0, tp_price=110.0, sl_order_id=f"sl-{cycle}", tp_order_id="",
            entry_time=time.time() + cycle + 1.0, exchange="icici", execution_symbol="NIFTYCE",
            asset_id="NIFTY", pnl_model="linear", currency_symbol="₹", currency_code="INR",
            quantity_unit="contracts", entry_fee_paid=2.0, entry_fee_exact=True, entry_leverage=1.0,
        )
        qs._manage_active(dm, broker, time.time())
        assert qs._pos.phase is PositionPhase.EXITING
        assert qs._pos.manual_exit_reason == "liquidity_tp_hit"
        qs._record_exchange_exit({"size": 0.0})
        assert qs._pos.phase is PositionPhase.FLAT
    assert qs._total_trades == 2 and qs._winning_trades == 2 and qs._total_pnl > 0
    assert [e[0] for e in broker.events] == ["cancel", "priced_close", "cancel", "priced_close"]
    assert len(qs._trade_history) == 2
    assert all(r["reason"] == "liquidity_tp_hit" and r["currency"] == "₹" for r in qs._trade_history)
    assert all(r["exact_fees"] is True and r["pnl"] > 0 for r in qs._trade_history)


def test_icici_order_manager_routes_supervised_close_as_priced_limit(monkeypatch):
    raw = {"selected_option_contract": {"right": "call", "stock_code": "NIFTY", "exchange_code": "NFO", "product_type": "Options", "strike_price": 23200, "expiry_date": "2026-06-02"}}
    inst = SimpleNamespace(asset_id="NIFTY", primary=SimpleNamespace(raw=raw), primary_exchange=SimpleNamespace(value="icici"))
    om = OrderManager(SimpleNamespace(), exchange_name="icici", instrument=inst)
    om.get_open_position = lambda: {"size": 50, "entry_price": 72.0, "raw": {"ltp": 110.0}}
    sent = []
    om.place_limit_order = lambda side, quantity, price, reduce_only=False: sent.append((side, quantity, price, reduce_only)) or {"order_id": "priced-limit"}
    monkeypatch.setattr("execution.order_manager.config.ICICI_EMERGENCY_EXIT_LIMIT_BUFFER_PCT", 0.10, raising=False)
    result = om.place_market_order("sell", 50, reduce_only=True)
    assert result["order_id"] == "priced-limit"
    assert sent and sent[0][0].lower() == "sell" and sent[0][3] is True
    assert sent[0][2] < 110.0  # marketable sell limit for immediate close attempt


def test_structural_engine_bounded_reentry_soak_10000_cycles():
    # Bounded deterministic soak: repeated executable signal -> position-close reset -> re-entry.
    # It is intentionally deterministic and network-free; it proves lifecycle stability,
    # not profitability or perpetual runtime.
    engine = EntryEngine()
    for i in range(10000):
        now = 1_800_000_000.0 + i * 600.0
        snap, c5, c15, c4h = _valid_long_setup(now)
        engine.update(snap, 100.50, 1.0, now, candles_5m=c5, candles_15m=c15, candles_4h=c4h)
        signal = engine.get_signal()
        assert signal is not None and signal.sl_price < signal.entry_price < signal.tp_price
        engine.on_entry_placed(signal)
        engine.on_position_closed()
        assert engine.state == "SCANNING"


def test_recent_trade_tape_preserves_inr_option_currency_without_usd_relabel():
    from orchestration.multi_asset_bot import MultiAssetQuantBot
    bot = MultiAssetQuantBot.__new__(MultiAssetQuantBot)
    bot._all_trade_records = lambda: [{
        "desk": "ICICI_INDEX_OPTIONS", "asset": "NIFTY", "side": "long",
        "entry": 72.0, "exit": 112.0, "pnl": 1995.5, "gross_pnl": 2000.0,
        "total_fees": 4.5, "currency": "₹", "timestamp": time.time(), "reason": "liquidity_tp_hit",
    }]
    report = bot.format_portfolio_trades_report()
    assert "₹72.00" in report and "₹112.00" in report and "₹4.50" in report
    assert "$72.00" not in report and "$4.50" not in report


class _ChildFillBroker:
    def __init__(self, fill_price: float):
        self.fill_price = fill_price
    def identify_exit_order(self, sl_order_id=None, tp_order_id=None):
        return {"confirmed": True, "exit_type": "tp", "fill_price": self.fill_price,
                "fee_paid": 0.01, "fee_exact": True, "order_id": tp_order_id or "tp"}


def _venue_instrument(exchange: str, asset: str, symbol: str):
    return SimpleNamespace(
        asset_id=asset, display_symbol=symbol,
        primary_exchange=SimpleNamespace(value=exchange),
        primary=SimpleNamespace(raw={}, display_symbol=symbol, symbol=symbol), by_exchange={})


@pytest.mark.parametrize("exchange,asset,symbol,model,currency", [
    ("delta", "BTC", "BTCUSD", "inverse_btcusd", "$"),
    ("delta", "GOLD", "PAXGUSD", "linear", "$"),
    ("delta", "SILVER", "SLVONUSD", "linear", "$"),
    ("coinswitch", "BTC", "BTCUSDT", "linear", "$"),
])
def test_broker_protected_tp_reconciliation_completes_two_cycles_for_non_icici_desks(exchange, asset, symbol, model, currency):
    qs = _strategy_for_reconciliation(_venue_instrument(exchange, asset, symbol))
    qs._om = _ChildFillBroker(fill_price=105.0)
    for cycle in range(2):
        engine, sig = _signal(1_900_000_000.0 + cycle * 600.0)
        engine.on_entry_placed(sig)
        qs._exit_completed = False
        qs._pnl_recorded_for = 0.0
        qs._pos = PositionState(
            phase=PositionPhase.ACTIVE, side="long", quantity=1.0,
            entry_price=float(sig.entry_price), sl_price=float(sig.sl_price), tp_price=float(sig.tp_price),
            sl_order_id=f"sl-{cycle}", tp_order_id=f"tp-{cycle}", entry_time=time.time() + cycle + 1.0,
            exchange=exchange, execution_symbol=symbol, asset_id=asset, pnl_model=model,
            currency_symbol=currency, currency_code="USD", quantity_unit="contracts",
            entry_fee_paid=0.01, entry_fee_exact=True, entry_leverage=1.0,
        )
        qs._record_exchange_exit({"size": 0.0})
        assert qs._pos.phase is PositionPhase.FLAT
    assert qs._total_trades == 2 and qs._winning_trades == 2
    assert len(qs._trade_history) == 2
    assert all(r["pnl_model"] == model and r["currency"] == currency and r["reason"] == "tp_hit" for r in qs._trade_history)


def test_real_strategy_handoff_executes_new_entry_engine_property_contract(monkeypatch):
    import strategy.quant_strategy as qm
    now = 1_950_000_000.0
    snap, c5, c15, c4h = _valid_long_setup(now)
    class Liquidity:
        def update(self, candles, price, atr, tick_time): return None
        def get_snapshot(self, price, atr): return snap
    class DM:
        def get_last_price(self): return 100.50
        def get_candles(self, tf, limit=None):
            return {"5m": c5, "15m": c15, "4h": c4h, "1h": [], "1d": []}.get(tf, [])
    qs = QuantStrategy.__new__(QuantStrategy)
    qs._entry_engine = EntryEngine(); qs._liq_map = Liquidity()
    qs.watchdog_trading_frozen = False; qs._last_watchdog_freeze_log = 0.0
    qs._last_data_warn = 0.0; qs._last_think_log = 0.0; qs._think_interval = 120.0
    qs._atr_5m = qm.ATREngine(); qs._force_sl = None; qs._force_tp = None
    qs._last_entry_signal = None; qs._last_execution_viability = None
    qs._spread_atr_gate = lambda dm: (True, {})
    qs._position_accounting_context = lambda: {"currency_symbol": "$"}
    qs._risk_gate = SimpleNamespace(can_trade=lambda balance: (True, "approved"))
    launched = []
    qs._launch_entry_async = lambda *args, **kwargs: launched.append((args, kwargs))
    monkeypatch.setattr(qm.QCfg, "MIN_5M_BARS", staticmethod(lambda: 20))
    risk = SimpleNamespace(get_available_balance=lambda: {"total": 100000.0, "available": 100000.0})
    qs._evaluate_entry(DM(), SimpleNamespace(), risk, now)
    assert launched and launched[0][0][3] == "long"
    assert qs._last_entry_signal is not None and qs._last_entry_signal.entry_type is EntryType.ICT_LIQUIDITY


def _nifty_session_instrument():
    from datetime import datetime, timedelta, timezone
    from agents.icici_chain_architect import build_underlying_payload
    from core.instruments import AssetClass, ExchangeInstrument, ExchangeName, TradableInstrument
    expiry = (datetime.now(timezone.utc) + timedelta(days=10)).strftime("%Y-%m-%d")
    chain = [
        {"TradingSymbol": "NIFTY30JUN30CE23200", "right": "Call", "strike_price": 23200, "expiry_date": expiry, "ltp": 72.0, "best_bid_price": 71.75, "best_offer_price": 72.25, "best_bid_quantity": 200, "best_offer_quantity": 200, "LotSize": 50, "stock_code": "NIFTY", "exchange_code": "NFO", "product_type": "Options"},
        {"TradingSymbol": "NIFTY30JUN30PE23000", "right": "Put", "strike_price": 23000, "expiry_date": expiry, "ltp": 78.0, "best_bid_price": 77.75, "best_offer_price": 78.25, "best_bid_quantity": 200, "best_offer_quantity": 200, "LotSize": 50, "stock_code": "NIFTY", "exchange_code": "NFO", "product_type": "Options"},
    ]
    raw = build_underlying_payload("NIFTY", "ICICI_INDEX_OPTIONS", [])
    raw.update({"chain_candidates": chain, "chain_candidates_deferred": False})
    ei = ExchangeInstrument(exchange=ExchangeName.ICICI, symbol="NIFTY", ws_symbol="NIFTY", display_symbol="NIFTY", asset_id="NIFTY", asset_class=AssetClass.OPTION, quote_asset="INR", base_asset="NIFTY", contract_type="option_chain", status="active", tick_size=0.05, lot_step=1.0, min_qty=1.0, max_leverage=1.0, raw=raw)
    return TradableInstrument("NIFTY", "NIFTY options", AssetClass.OPTION, ExchangeName.ICICI, {ExchangeName.ICICI: ei})


def test_actual_nifty_entry_fill_retains_old_session_book_option_execution_and_new_strategy_thesis(monkeypatch):
    import strategy.quant_strategy as qm
    from agents.icici_chain_architect import apply_contract_choice, build_session_contract_book, select_contract_from_session_book
    from core.instruments import instrument_scope
    inst = _nifty_session_instrument()
    assert build_session_contract_book(inst, underlying_spot=23100.0, available_funds=49310.96) is not None
    class DM:
        def __init__(self): self._primary = self; self.selected = False; self.released = False
        def select_contract_for_thesis(self, side, underlying_spot, available_funds):
            choice, status = select_contract_from_session_book(inst, side, underlying_spot=underlying_spot, available_funds=available_funds)
            assert status == "session_contract_ready" and choice is not None
            apply_contract_choice(inst, choice); self.selected = True; return choice
        def get_last_price(self): return 72.0 if self.selected else 23100.0
        def get_orderbook(self): return {"bids": [[71.95, 200]], "asks": [[72.05, 200]]}
        def get_execution_candles(self, tf, limit=25): return [{"h": 73.0, "l": 71.0, "c": 72.0}] * max(25, limit)
        def release_icici_execution_vehicle(self): self.released = True
    class OM:
        active_exchange = "icici"; last_order_error = None
        def __init__(self): self.entry = None; self.sl = None
        def place_bracket_limit_entry(self, **kwargs): return None
        def place_limit_entry(self, side, quantity, limit_price, timeout_sec, fallback_to_market, on_order_placed):
            self.entry = (side, quantity, limit_price); on_order_placed("entry-ce")
            return {"order_id": "entry-ce", "fill_price": limit_price, "quantity": quantity, "fill_type": "maker", "paid_commission": 2.0, "paid_commission_exact": True}
        def cancel_symbol_conditionals(self): return {}
        def place_stop_loss(self, side, quantity, trigger_price): self.sl = (side, quantity, trigger_price); return {"order_id": "sl-ce"}
        def place_take_profit(self, **kwargs): raise AssertionError("ICICI must keep one live broker exit: SL only")
    class RM:
        def get_available_balance(self): return {"total": 49310.96, "available": 49310.96, "available_raw": 49310.96}
        def set_position_open(self, state): self.state = state
    dm, om, rm = DM(), OM(), RM()
    qs = QuantStrategy(instrument=inst)
    qs._fee_engine = None
    qs._active_effective_leverage = 1.0; qs._active_margin_risk_pct = 0.0
    qs._force_sl = 23000.0; qs._force_tp = 23300.0
    qs._atr_5m._atr = 50.0
    qs._last_entry_signal = SimpleNamespace(entry_price=23100.0, delivery_probability=0.72, quality={"context_4h": 0.81, "context_15m": 0.76, "delivery_probability": 0.72, "delivery_utility_r": 1.20, "displacement_atr": 1.30}, sweep_result=None)
    qs._compute_quantity = lambda *args, **kwargs: 50.0
    qs._repair_execution_geometry = lambda side, entry_price, sl_price, tp_price, atr, use_maker_entry, delivery_probability: (sl_price, tp_price, False)
    qs._build_tp_ladder_plan = lambda **kwargs: None
    qs._send_telegram = lambda *args, **kwargs: None
    qs._risk_manager_ref = rm
    monkeypatch.setattr(qm, "_icici_market_session_open", lambda: (True, "synthetic_session"))
    with instrument_scope(inst):
        qs._enter_trade(dm, om, rm, "long", SimpleNamespace(vwap_price=0.0), prefetched_bal_info=rm.get_available_balance())
    assert qs._pos.phase is PositionPhase.ACTIVE
    assert qs._pos.thesis_side == "long" and qs._pos.side == "long"
    assert qs._pos.execution_symbol == "NIFTY30JUN30CE23200"
    assert qs._pos.currency_symbol == "₹" and qs._pos.pnl_model == "linear"
    assert om.entry[0] == "long" and om.entry[1] == 50.0 and om.entry[2] == pytest.approx(72.0)
    assert om.sl and om.sl[0] == "sell" and om.sl[1] == 50.0 and om.sl[2] < qs._pos.entry_price
    assert qs._pos.tp_order_id == "" and qs._pos.tp_price > qs._pos.entry_price


def _executable_instrument(exchange: str, asset: str, symbol: str):
    from core.instruments import AssetClass, ExchangeInstrument, ExchangeName, TradableInstrument
    ex = ExchangeName(exchange)
    ac = AssetClass.CRYPTO if asset == "BTC" else AssetClass.COMMODITY
    ei = ExchangeInstrument(
        exchange=ex, symbol=symbol, ws_symbol=symbol, display_symbol=symbol,
        asset_id=asset, asset_class=ac, tick_size=0.01, lot_step=0.001,
        min_qty=0.001, max_qty=1000.0, max_leverage=50.0, raw={},
    )
    return TradableInstrument(asset_id=asset, display_name=asset, asset_class=ac,
                              primary_exchange=ex, by_exchange={ex: ei})


class _ExecutableLifecycleData:
    def get_last_price(self): return 100.50
    def get_orderbook(self): return {"bids": [[100.45, 10]], "asks": [[100.55, 10]]}


class _ExecutableLifecycleRisk:
    def get_available_balance(self): return {"total": 10000.0, "available": 10000.0}
    def set_position_open(self, state): self.is_open = bool(state)


class _ExecutableLifecycleBroker:
    def __init__(self, exchange: str):
        self.active_exchange = exchange
        self.last_order_error = None
        self.cycle = 0
        self.events = []
    def set_leverage(self, leverage):
        self.events.append(("leverage", leverage)); return {"success": True}
    def place_bracket_limit_entry(self, **kwargs):
        if self.active_exchange != "delta": return None
        self.cycle += 1
        self.events.append(("delta_bracket", kwargs))
        kwargs["on_order_placed"](f"entry-{self.cycle}")
        return {"order_id": f"entry-{self.cycle}", "bracket_order": True,
                "bracket_child_verified": True, "fill_price": kwargs["limit_price"],
                "quantity": kwargs["quantity"], "fill_type": "maker",
                "paid_commission": 0.01, "paid_commission_exact": True,
                "bracket_sl_order_id": f"sl-{self.cycle}",
                "bracket_tp_order_id": f"tp-{self.cycle}",
                "bracket_sl_price": kwargs["sl_price"], "bracket_tp_price": kwargs["tp_price"]}
    def place_limit_entry(self, side, quantity, limit_price, timeout_sec, fallback_to_market, on_order_placed):
        self.cycle += 1
        self.events.append(("spot_entry", side, quantity, limit_price))
        on_order_placed(f"entry-{self.cycle}")
        return {"order_id": f"entry-{self.cycle}", "fill_price": limit_price,
                "quantity": quantity, "fill_type": "maker", "paid_commission": 0.01,
                "paid_commission_exact": True}
    def cancel_symbol_conditionals(self): return {}
    def place_stop_loss(self, side, quantity, trigger_price):
        self.events.append(("sl", side, quantity, trigger_price)); return {"order_id": f"sl-{self.cycle}"}
    def place_take_profit(self, side, quantity, trigger_price):
        self.events.append(("tp", side, quantity, trigger_price)); return {"order_id": f"tp-{self.cycle}"}
    def identify_exit_order(self, sl_order_id=None, tp_order_id=None):
        return {"confirmed": True, "exit_type": "tp", "fill_price": 110.0,
                "fee_paid": 0.01, "fee_exact": True, "order_id": tp_order_id or f"tp-{self.cycle}"}
    def cancel_order(self, order_id): return CancelResult.CANCELLED


@pytest.mark.parametrize("exchange,asset,symbol,model", [
    ("delta", "BTC", "BTCUSD", "inverse_btcusd"),
    ("delta", "GOLD", "PAXGUSD", "linear"),
    ("delta", "SILVER", "SLVONUSD", "linear"),
    ("coinswitch", "BTC", "BTCUSDT", "linear"),
])
def test_actual_non_icici_protected_entry_exit_reentry_runs_twice(exchange, asset, symbol, model):
    from core.instruments import instrument_scope
    inst = _executable_instrument(exchange, asset, symbol)
    broker = _ExecutableLifecycleBroker(exchange)
    risk = _ExecutableLifecycleRisk()
    dm = _ExecutableLifecycleData()
    qs = QuantStrategy(order_manager=broker, instrument=inst)
    qs._fee_engine = None
    qs._active_effective_leverage = 2.0
    qs._active_margin_risk_pct = 0.01
    qs._force_sl = 99.0; qs._force_tp = 110.0
    qs._atr_5m._atr = 1.0
    qs._compute_quantity = lambda *args, **kwargs: 1.0
    qs._repair_execution_geometry = lambda side, entry_price, sl_price, tp_price, atr, use_maker_entry, delivery_probability: (sl_price, tp_price, False)
    qs._sl_liquidation_sanity = lambda *args, **kwargs: (True, 0.0, 0.0, "synthetic-safe")
    qs._build_tp_ladder_plan = lambda **kwargs: None
    qs._place_internal_tp_ladder = lambda **kwargs: ([], [])
    qs._send_telegram = lambda *args, **kwargs: None
    qs._risk_manager_ref = risk
    for cycle in range(2):
        qs._force_sl = 99.0; qs._force_tp = 110.0
        qs._last_entry_signal = SimpleNamespace(
            entry_price=100.50, delivery_probability=0.75,
            quality={"context_4h": 0.80, "context_15m": 0.76,
                     "delivery_probability": 0.75, "delivery_utility_r": 1.45},
            sweep_result=None,
        )
        with instrument_scope(inst):
            qs._enter_trade(dm, broker, risk, "long", SimpleNamespace(vwap_price=0.0),
                            prefetched_bal_info=risk.get_available_balance())
        assert qs._pos.phase is PositionPhase.ACTIVE
        assert qs._pos.exchange == exchange and qs._pos.execution_symbol == symbol
        assert qs._pos.pnl_model == model and qs._pos.sl_order_id and qs._pos.tp_order_id
        qs._record_exchange_exit({"size": 0.0})
        assert qs._pos.phase is PositionPhase.FLAT
    assert qs._total_trades == 2 and qs._winning_trades == 2
    assert all(row["pnl_model"] == model and row["reason"] == "tp_hit" for row in qs._trade_history)
    if exchange == "delta":
        assert len([e for e in broker.events if e[0] == "delta_bracket"]) == 2
    else:
        assert len([e for e in broker.events if e[0] == "spot_entry"]) == 2
        assert len([e for e in broker.events if e[0] == "sl"]) == 2
        assert len([e for e in broker.events if e[0] == "tp"]) == 2


def test_liquidity_target_ladder_numeric_clamp_is_runtime_callable():
    qs = QuantStrategy.__new__(QuantStrategy)
    assert qs._clamp_ladder_value(2.0, 0.0, 1.0) == 1.0
    assert qs._clamp_ladder_value(-1.0, 0.0, 1.0) == 0.0


def test_actual_nifty_session_book_selection_protected_exit_and_reentry_runs_twice(monkeypatch):
    import strategy.quant_strategy as qm
    from agents.icici_chain_architect import apply_contract_choice, build_session_contract_book, select_contract_from_session_book
    from core.instruments import instrument_scope
    inst = _nifty_session_instrument()
    assert build_session_contract_book(inst, underlying_spot=23100.0, available_funds=49310.96) is not None
    class DM:
        def __init__(self): self._primary = self; self.selected = False; self.premium = 72.0
        def select_contract_for_thesis(self, side, underlying_spot, available_funds):
            choice, status = select_contract_from_session_book(inst, side, underlying_spot=underlying_spot, available_funds=available_funds)
            assert status == "session_contract_ready" and choice is not None
            apply_contract_choice(inst, choice); self.selected = True; return choice
        def get_last_price(self): return self.premium if self.selected else 23100.0
        def get_orderbook(self): return {"bids": [[self.premium - 0.05, 200]], "asks": [[self.premium + 0.05, 200]]}
        def get_execution_candles(self, tf, limit=25): return [{"h": self.premium + 1, "l": self.premium - 1, "c": self.premium}] * max(25, limit)
        def release_icici_execution_vehicle(self): self.selected = False
    class OM:
        active_exchange = "icici"; last_order_error = None
        def __init__(self): self.n = 0; self.events = []
        def place_bracket_limit_entry(self, **kwargs): return None
        def place_limit_entry(self, side, quantity, limit_price, timeout_sec, fallback_to_market, on_order_placed):
            self.n += 1; on_order_placed(f"entry-{self.n}"); self.events.append(("entry", limit_price))
            return {"order_id": f"entry-{self.n}", "fill_price": limit_price, "quantity": quantity,
                    "fill_type": "maker", "paid_commission": 2.0, "paid_commission_exact": True}
        def cancel_symbol_conditionals(self): return {}
        def place_stop_loss(self, side, quantity, trigger_price):
            self.events.append(("sl", trigger_price)); return {"order_id": f"sl-{self.n}"}
        def place_take_profit(self, **kwargs): raise AssertionError("ICICI protective SL must remain the only live exit order")
        def cancel_all_exit_orders(self, sl, tp): self.events.append(("cancel", sl, tp)); return CancelResult.SUCCESS, CancelResult.NOT_FOUND
        def place_market_order(self, side, quantity, reduce_only):
            self.events.append(("marketable_limit_close", side, quantity, reduce_only)); return {"order_id": f"close-{self.n}"}
        def get_fill_details(self, order_id):
            return {"status": "FILLED", "fill_price": 112.0, "paid_commission": 2.5, "paid_commission_exact": True}
        def identify_exit_order(self, **kwargs): return {"confirmed": False}
    class RM:
        def get_available_balance(self): return {"total": 49310.96, "available": 49310.96, "available_raw": 49310.96}
        def set_position_open(self, state): self.state = state
    dm, om, rm = DM(), OM(), RM()
    qs = QuantStrategy(order_manager=om, instrument=inst)
    qs._fee_engine = None; qs._active_effective_leverage = 1.0; qs._active_margin_risk_pct = 0.0
    qs._atr_5m._atr = 50.0; qs._compute_quantity = lambda *args, **kwargs: 50.0
    qs._repair_execution_geometry = lambda side, entry_price, sl_price, tp_price, atr, use_maker_entry, delivery_probability: (sl_price, tp_price, False)
    qs._build_tp_ladder_plan = lambda **kwargs: None; qs._send_telegram = lambda *args, **kwargs: None; qs._risk_manager_ref = rm
    monkeypatch.setattr(qm, "_icici_market_session_open", lambda: (True, "synthetic_session"))
    for cycle in range(2):
        dm.premium = 72.0
        qs._force_sl = 23000.0; qs._force_tp = 23300.0
        qs._last_entry_signal = SimpleNamespace(entry_price=23100.0, delivery_probability=0.72,
            quality={"context_4h": 0.81, "context_15m": 0.76, "delivery_probability": 0.72,
                     "delivery_utility_r": 1.20, "displacement_atr": 1.30}, sweep_result=None)
        with instrument_scope(inst):
            qs._enter_trade(dm, om, rm, "long", SimpleNamespace(vwap_price=0.0), prefetched_bal_info=rm.get_available_balance())
        assert qs._pos.phase is PositionPhase.ACTIVE
        assert qs._pos.execution_symbol == "NIFTY30JUN30CE23200" and qs._pos.currency_symbol == "₹"
        dm.premium = max(112.0, qs._pos.tp_price + 0.05)
        qs._manage_active(dm, om, time.time())
        assert qs._pos.phase is PositionPhase.EXITING and qs._pos.manual_exit_reason == "liquidity_tp_hit"
        qs._record_exchange_exit({"size": 0.0})
        assert qs._pos.phase is PositionPhase.FLAT
    assert qs._total_trades == 2 and qs._winning_trades == 2
    assert all(t["currency"] == "₹" and t["execution_symbol"] == "NIFTY30JUN30CE23200" and t["reason"] == "liquidity_tp_hit" for t in qs._trade_history)
    assert len([event for event in om.events if event[0] == "entry"]) == 2
    assert len([event for event in om.events if event[0] == "marketable_limit_close"]) == 2


def test_router_switch_to_icici_reports_inr_not_usd():
    from execution.router import ExecutionRouter
    class BalOM:
        def get_balance(self): return {"available": 49310.96}
    router = ExecutionRouter(BalOM(), BalOM(), BalOM(), default="delta")
    ok, text = router.switch("icici")
    assert ok is True and "₹49,310.96" in text and "INR NFO available" in text and "$" not in text


def test_decision_tape_emits_transition_and_slow_snapshot_without_tick_spam(caplog):
    import logging
    qs = QuantStrategy.__new__(QuantStrategy)
    qs._last_decision_fingerprint = None
    qs._last_decision_log = 0.0
    qs._decision_snapshot_sec = 60.0
    qs._last_spread_gate_context = {"spread_bps": 1.0, "spread_atr": 0.1, "size_mult": 1.0, "hard_fail": False}
    info = {
        "state": "CONTEXT_READY", "block_reason": "AWAITING_FRESH_5M_LIQUIDITY_RAID", "trigger": "WAIT",
        "context_4h": "bullish", "context_15m": "bullish", "context_aligned": True, "context_direction": "long",
        "context_4h_conf": 0.7, "context_15m_conf": 0.7, "entry_5m_atr": 1.0,
    }
    with caplog.at_level(logging.INFO, logger="strategy.quant_strategy"):
        qs._log_ict_decision_snapshot(info, 100.0, 100.0)
        qs._log_ict_decision_snapshot(info, 100.1, 101.0)  # unchanged tick: silent
        changed = dict(info, block_reason="AWAITING_5M_MSS_DISPLACEMENT", raid_side="long", raid_price=99.0)
        qs._log_ict_decision_snapshot(changed, 100.2, 102.0)  # state transition: immediate
        qs._log_ict_decision_snapshot(changed, 100.3, 170.0)  # periodic snapshot
    decision_lines = [r.message for r in caplog.records if "ICT_DECISION" in r.message]
    assert len(decision_lines) == 3
    assert "TRANSITION" in decision_lines[0] and "TRANSITION" in decision_lines[1]
    assert "SNAPSHOT" in decision_lines[2]


def test_execution_cost_gate_observes_current_completed_five_minute_atr_before_signal_approval(monkeypatch):
    import strategy.quant_strategy as qm
    now = 1_960_000_000.0
    snap, c5, c15, c4h = _valid_long_setup(now)
    class Liquidity:
        def update(self, candles, price, atr, tick_time):
            raise AssertionError("liquidity/entry evaluation must not run after a hard spread block")
        def get_snapshot(self, price, atr):
            return snap
    class DM:
        def get_last_price(self): return 100.50
        def get_candles(self, tf, limit=None):
            return {"5m": c5, "15m": c15, "4h": c4h, "1h": [], "1d": []}.get(tf, [])
    qs = QuantStrategy.__new__(QuantStrategy)
    qs._entry_engine = EntryEngine(); qs._liq_map = Liquidity()
    qs.watchdog_trading_frozen = False; qs._last_watchdog_freeze_log = 0.0
    qs._last_data_warn = 0.0; qs._atr_5m = qm.ATREngine()
    qs._last_decision_fingerprint = None; qs._last_decision_log = 0.0; qs._decision_snapshot_sec = 60.0
    qs._last_spread_gate_context = {}
    seen = []
    def hard_spread_block(dm):
        seen.append(float(qs._atr_5m.atr or 0.0))
        qs._last_spread_gate_context = {"spread_bps": 80.0, "spread_atr": 3.0, "size_mult": 0.0, "hard_fail": True}
        return False, 3.0
    qs._spread_atr_gate = hard_spread_block
    monkeypatch.setattr(qm.QCfg, "MIN_5M_BARS", staticmethod(lambda: 20))
    qs._evaluate_entry(DM(), SimpleNamespace(), SimpleNamespace(), now)
    assert seen and seen[0] > 0.0
    assert qs._entry_engine.get_signal() is None


def test_data_lineage_snapshot_formats_frame_age_at_runtime_without_parser_sensitive_nested_fstrings(caplog):
    import logging
    qs = QuantStrategy.__new__(QuantStrategy)
    qs._last_decision_fingerprint = None
    qs._last_decision_log = 0.0
    qs._decision_snapshot_sec = 60.0
    qs._last_spread_gate_context = {}
    qs._last_data_integrity_context = {
        "ok": True,
        "blockers": [],
        "lineage": {
            "analysis_source": "DeltaDataManager",
            "analysis_domain": "CONTRACT_PRICE",
            "execution_source": "DeltaDataManager",
            "execution_domain": "CONTRACT_PRICE",
        },
        "analysis_quote_fresh": True,
        "last_update_age_sec": 0.25,
        "frames": {
            "5m": {"bars": 200, "last_age_sec": 1.5, "duplicates": 0, "gaps": 0,
                   "invalid_ohlc": 0, "volume_status": "OBSERVED", "nonzero_volume_bars": 200},
            "15m": {"bars": 200, "last_age_sec": None, "duplicates": 0, "gaps": 0,
                    "invalid_ohlc": 0, "volume_status": "UNAVAILABLE", "nonzero_volume_bars": 0},
        },
    }
    qs._liq_map = SimpleNamespace(_native_atr_by_tf={"5m": 1.0, "15m": 2.0, "4h": 8.0})
    info = {
        "state": "SCANNING", "block_reason": "AWAITING_FRESH_5M_LIQUIDITY_RAID", "trigger": "WAIT",
        "context_4h": "bearish", "context_15m": "bearish", "context_aligned": True,
        "context_direction": "short", "entry_5m_atr": 1.0,
    }
    with caplog.at_level(logging.INFO, logger="strategy.quant_strategy"):
        qs._log_ict_decision_snapshot(info, 100.0, 100.0)
    lineage_lines = [r.message for r in caplog.records if "DATA_LINEAGE" in r.message]
    assert len(lineage_lines) == 1
    assert "5m:n=200 age=1.5s" in lineage_lines[0]
    assert "15m:n=200 age=N/A" in lineage_lines[0]

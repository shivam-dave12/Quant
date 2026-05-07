"""v76 entry-to-exit institutional mock matrix.

These tests do not change strategy logic.  They stress the complete mocked trade
lifecycle so duplicate filters, bad fallback paths, and unsafe protection states
are caught before running the bot live.
"""

import threading
import time
from types import SimpleNamespace

import pytest

from strategy.entry_engine import EntryEngine
from strategy.liquidity_map import LiquidityMapSnapshot, LiquidityPool, PoolSide, PoolStatus, PoolTarget
from strategy.liquidity_pool_selector import select_sl_with_report, select_tp_with_report
from strategy.liquidity_trail import LiquidityTrailResult
from strategy.quant_strategy import EntryReadinessDecision, PositionPhase, PositionState, QuantStrategy


class MockDataManager:
    def __init__(self, price=100.0, bid=99.9, ask=100.1):
        self.price = float(price)
        self.bid = float(bid)
        self.ask = float(ask)

    def get_last_price(self):
        return self.price

    def get_orderbook(self):
        return {"bids": [[self.bid, 10]], "asks": [[self.ask, 10]]}

    def get_candles(self, timeframe, limit=10):
        return [{"o": self.price, "h": self.price + 1, "l": self.price - 1, "c": self.price, "v": 1.0} for _ in range(limit)]


class MockRiskManager:
    def __init__(self, total=1_000.0):
        self.total = float(total)

    def get_available_balance(self):
        return {"total": self.total, "available": self.total}


class MockOrderManager:
    def __init__(self, *, active_exchange="delta", bracket_response=None, limit_response=None,
                 sl_response=None, tp_response=None, replace_response=None):
        self.active_exchange = active_exchange
        self._exchange_name = active_exchange
        self.bracket_response = bracket_response
        self.limit_response = limit_response
        self.sl_response = sl_response if sl_response is not None else {"order_id": "sl-live"}
        self.tp_response = tp_response if tp_response is not None else {"order_id": "tp-live"}
        self.replace_response = replace_response
        self.calls = []

    def place_bracket_limit_entry(self, **kwargs):
        self.calls.append(("bracket", kwargs))
        cb = kwargs.get("on_order_placed")
        if cb and self.bracket_response and not self.bracket_response.get("_error"):
            cb("entry-order-123456")
        return self.bracket_response

    def place_limit_entry(self, **kwargs):
        self.calls.append(("limit", kwargs))
        cb = kwargs.get("on_order_placed")
        if cb and self.limit_response:
            cb("entry-limit-123456")
        return self.limit_response

    def cancel_symbol_conditionals(self):
        self.calls.append(("cancel_symbol_conditionals", {}))
        return {}

    def place_stop_loss(self, **kwargs):
        self.calls.append(("stop_loss", kwargs))
        return self.sl_response

    def place_take_profit(self, **kwargs):
        self.calls.append(("take_profit", kwargs))
        return self.tp_response

    def cancel_order(self, order_id):
        self.calls.append(("cancel_order", {"order_id": order_id}))
        return True

    def place_market_order(self, **kwargs):
        self.calls.append(("market", kwargs))
        return {"order_id": "market-exit"}

    def emergency_flatten(self, reason=""):
        self.calls.append(("emergency_flatten", {"reason": reason}))
        return {"order_id": "emergency"}

    def cancel_all_exit_orders(self, **kwargs):
        self.calls.append(("cancel_all_exit_orders", kwargs))
        return True

    def replace_stop_loss(self, **kwargs):
        self.calls.append(("replace_stop_loss", kwargs))
        return self.replace_response


def _signal(composite=0.90, conviction=0.90, entry_price=100.0):
    return SimpleNamespace(composite=composite, vwap_price=0.0, conviction=conviction, entry_price=entry_price)


def _risk_gate():
    return SimpleNamespace(
        set_opening_balance=lambda total: None,
        record_trade_start=lambda: None,
    )


def _base_strategy(*, force_sl=98.0, force_tp=104.0, entry_price=100.0, readiness_score=0.90):
    qs = QuantStrategy.__new__(QuantStrategy)
    qs._last_execution_viability = None
    qs._atr_5m = SimpleNamespace(atr=1.0, get_percentile=lambda: 0.50)
    qs._risk_gate = _risk_gate()
    qs._fee_engine = None
    qs._last_entry_signal = SimpleNamespace(entry_price=entry_price, conviction=0.90)
    qs._last_entry_readiness = EntryReadinessDecision(
        allowed=True,
        score=readiness_score,
        floor=0.60,
        size_mult=1.0,
        reason="mock readiness pass",
        hard_rejects=[],
        penalties=[],
        allows=["mock"],
    )
    qs._force_sl = force_sl
    qs._force_tp = force_tp
    qs._lock = threading.RLock()
    qs._sl_liquidation_sanity = lambda side, entry, sl: (True, 80.0, 82.0, "ok")
    qs._execution_viability_model = lambda **kwargs: SimpleNamespace(
        allocation_allowed=True,
        fee_to_risk=0.10,
        expected_net_utility_r=1.0,
    )
    qs._repair_execution_geometry = lambda **kwargs: (kwargs["sl_price"], kwargs["tp_price"], False)
    qs._compute_quantity = lambda *args, **kwargs: 1.0
    qs._current_entry_session = lambda: "NY"
    qs._entry_engine = SimpleNamespace(
        _last_sweep_analysis={"quant_posterior": 0.90, "quant_ev": 1.2, "quant_components": {"mock": 1.0}},
        on_position_opened=lambda: None,
    )
    qs._htf = SimpleNamespace(trend_15m=0.70, trend_4h=0.40)
    qs._confirm_long = 0
    qs._confirm_short = 0
    qs._dir_engine = SimpleNamespace(clear_sweep=lambda: None)
    qs._send_telegram = lambda *args, **kwargs: None
    qs._instrument = SimpleNamespace(asset_id="BTC", display_symbol="BTCUSD")
    qs._asset_id = "BTC"
    qs._reconcile_data = None
    qs._last_reconcile_time = 0.0
    qs.current_sl_price = 0.0
    qs.current_tp_price = 0.0
    qs._exit_completed = False
    qs._pnl_recorded_for = 0.0
    qs._conviction = None
    qs._entry_order_placed_at = 0.0
    qs._entering_since = 0.0
    qs._last_exit_time = 0.0
    qs._last_tp_gate_rejection = 0.0
    qs._ps_tg_last_hash = ""
    qs._ps_tg_last_ts = 0.0
    qs._ps_tg_last_conf = -1.0
    qs._ps_tg_last_action = ""
    qs._ps_tg_last_direction = ""
    qs._pos = PositionState()
    return qs


def _enter(qs, om, side="long"):
    qs._enter_trade(
        MockDataManager(),
        om,
        MockRiskManager(),
        side,
        _signal(entry_price=100.0),
        mode="reversal",
        prefetched_bal_info={"total": 1_000.0, "available": 1_000.0},
        entry_now=time.time(),
    )


def _target(price, pool_side, *, entry=100.0, atr=1.0, sig=10.0, tf="15m", status=PoolStatus.DETECTED,
            touches=1, ob_aligned=False, fvg_aligned=False, htf_count=3):
    now = time.time()
    pool = LiquidityPool(
        price=float(price),
        side=pool_side,
        timeframe=tf,
        status=status,
        touches=touches,
        created_at=now,
        last_touch=now,
        ob_aligned=ob_aligned,
        fvg_aligned=fvg_aligned,
        htf_count=htf_count,
    )
    return PoolTarget(
        pool=pool,
        distance_atr=abs(float(price) - float(entry)) / max(float(atr), 1e-9),
        direction="above" if float(price) > float(entry) else "below",
        significance=float(sig),
        tf_sources=[tf],
    )


def _snap(bsl=None, ssl=None):
    return LiquidityMapSnapshot(
        bsl_pools=list(bsl or []),
        ssl_pools=list(ssl or []),
        primary_target=None,
        recent_sweeps=[],
        swept_bsl_levels=[],
        swept_ssl_levels=[],
        nearest_bsl_atr=1.0,
        nearest_ssl_atr=1.0,
        timestamp=time.time(),
    )


# ───────────────────────── TP/SL liquidity selectors ─────────────────────────

def test_tp_long_does_not_blindly_take_nearest_pool_when_far_pool_has_better_institutional_ev():
    near = _target(103.4, PoolSide.BSL, sig=22.0, tf="15m")
    far = _target(108.0, PoolSide.BSL, sig=14.0, tf="4h")
    tp, target, score, report = select_tp_with_report(
        _snap(bsl=[near, far]), "long", entry=100.0, sl=98.0, atr=1.0, min_rr=2.0, posterior_prob=0.90
    )
    assert tp == pytest.approx(107.5)
    assert target is far
    assert score.rr >= 3.0
    assert report.as_dict()["selected"]["pool_price"] == pytest.approx(108.0)


def test_tp_short_symmetric_reasonable_distance_selection():
    near = _target(96.6, PoolSide.SSL, sig=22.0, tf="15m")
    far = _target(92.0, PoolSide.SSL, sig=14.0, tf="4h")
    tp, target, score, _ = select_tp_with_report(
        _snap(ssl=[near, far]), "short", entry=100.0, sl=102.0, atr=1.0, min_rr=2.0, posterior_prob=0.90
    )
    assert tp == pytest.approx(92.5)
    assert target is far
    assert score.rr >= 3.0


def test_tp_rejects_near_pool_when_payoff_is_not_reasonable_after_sl_distance():
    near = _target(101.0, PoolSide.BSL, sig=50.0, tf="5m")
    snap = _snap(bsl=[near])
    tp, target, score, report = select_tp_with_report(
        snap, "long", entry=100.0, sl=98.0, atr=1.0, min_rr=2.0, posterior_prob=0.95
    )
    assert tp is None and target is None and score is None
    assert "payoff" in report.as_dict()["summary"] or "RR" in report.as_dict()["summary"]


def test_sl_long_prefers_reasonable_liquidity_shield_over_far_capital_drag_anchor():
    closer = _target(96.0, PoolSide.SSL, sig=8.0, tf="15m")
    far = _target(88.0, PoolSide.SSL, sig=20.0, tf="4h")
    sl, target, pick, report = select_sl_with_report(
        _snap(ssl=[far, closer]), "long", entry=100.0, atr=1.0, invalidation_price=97.0, max_buffer_atr=1.5, min_risk=0.5
    )
    assert target is closer
    assert sl < closer.pool.price < 100.0
    assert "outside-liquidity-zone" in pick.reasons
    assert report.as_dict()["selected"]["pool_price"] == pytest.approx(96.0)


def test_sl_short_prefers_reasonable_liquidity_shield_over_far_capital_drag_anchor():
    closer = _target(104.0, PoolSide.BSL, sig=8.0, tf="15m")
    far = _target(112.0, PoolSide.BSL, sig=20.0, tf="4h")
    sl, target, pick, _ = select_sl_with_report(
        _snap(bsl=[far, closer]), "short", entry=100.0, atr=1.0, invalidation_price=103.0, max_buffer_atr=1.5, min_risk=0.5
    )
    assert target is closer
    assert sl > closer.pool.price > 100.0
    assert "outside-liquidity-zone" in pick.reasons


def test_entry_engine_finds_one_tp_plan_and_one_sl_plan_without_selector_rescoring():
    engine = EntryEngine.__new__(EntryEngine)
    engine._last_pool_plan = None
    engine._current_quant_posterior = lambda: 0.90
    engine._record_pool_report = EntryEngine._record_pool_report.__get__(engine, EntryEngine)
    engine._format_pool_plan = lambda report: str(report.get("summary", ""))
    engine._estimated_entry_be_move = lambda price, atr: 0.10
    engine._last_selected_tp_rr_floor = EntryEngine._last_selected_tp_rr_floor.__get__(engine, EntryEngine)
    engine._ict = None
    engine._htf = None
    bsl = [_target(103.4, PoolSide.BSL, sig=22.0, tf="15m"), _target(108.0, PoolSide.BSL, sig=14.0, tf="4h")]
    ssl = [_target(96.0, PoolSide.SSL, sig=8.0, tf="15m")]
    snap = _snap(bsl=bsl, ssl=ssl)

    tp, tp_target = EntryEngine._find_tp(engine, snap, "long", 100.0, 1.0, 98.0, 2.0)
    assert tp == pytest.approx(107.5)
    assert tp_target is bsl[1]
    assert engine._last_pool_plan["role"] == "TP"
    assert engine._last_pool_plan["selected"]["pool_price"] == pytest.approx(108.0)

    sl, sl_target, pick = EntryEngine._find_sl_pool(
        engine, snap, "long", 100.0, 1.0, invalidation_price=97.0, max_buffer_atr=1.5, min_risk=0.5
    )
    assert sl_target is ssl[0]
    assert sl < 96.0
    assert pick is not None
    assert engine._last_pool_plan["role"] == "SL"


# ───────────────────────── Entry execution lifecycle ─────────────────────────

def test_enter_trade_rejects_without_entry_engine_force_levels_and_sends_no_order():
    qs = _base_strategy(force_sl=None, force_tp=None)
    om = MockOrderManager(active_exchange="delta", bracket_response={"bracket_order": True})
    _enter(qs, om)
    assert qs._pos.phase is PositionPhase.FLAT
    assert om.calls == []
    assert qs._last_tp_gate_rejection > 0


def test_delta_bracket_success_opens_active_position_with_exchange_attached_sl_tp():
    qs = _base_strategy()
    om = MockOrderManager(
        active_exchange="delta",
        bracket_response={
            "bracket_order": True,
            "fill_price": 100.0,
            "quantity": 1.0,
            "order_id": "entry-live",
            "bracket_sl_order_id": "sl-child",
            "bracket_tp_order_id": "tp-child",
            "bracket_child_verified": True,
        },
    )
    _enter(qs, om)
    assert qs._pos.phase is PositionPhase.ACTIVE
    assert qs._pos.sl_order_id == "sl-child"
    assert qs._pos.tp_order_id == "tp-child"
    assert qs.current_sl_price == pytest.approx(98.0)
    assert qs.current_tp_price == pytest.approx(104.0)
    assert [name for name, _ in om.calls].count("bracket") == 1
    assert "limit" not in [name for name, _ in om.calls]


def test_delta_bracket_timeout_is_safe_no_naked_fallback_no_position():
    qs = _base_strategy()
    om = MockOrderManager(active_exchange="delta", bracket_response={"_error": True, "_reason": "not_filled_timeout"})
    _enter(qs, om)
    call_names = [name for name, _ in om.calls]
    assert qs._pos.phase is PositionPhase.FLAT
    assert call_names == ["bracket"]
    assert qs._last_tp_gate_rejection > 0


def test_delta_rejects_non_bracket_response_and_does_not_place_standalone_exits():
    qs = _base_strategy()
    om = MockOrderManager(
        active_exchange="delta",
        bracket_response={"bracket_order": False, "fill_price": 100.0, "quantity": 1.0, "order_id": "unsafe"},
    )
    _enter(qs, om)
    call_names = [name for name, _ in om.calls]
    assert qs._pos.phase is PositionPhase.FLAT
    assert call_names == ["bracket"]
    assert qs._last_exit_time > 0


def test_delta_missing_verified_bracket_children_flattens_immediately():
    qs = _base_strategy()
    om = MockOrderManager(
        active_exchange="delta",
        bracket_response={
            "bracket_order": True,
            "fill_price": 100.0,
            "quantity": 1.0,
            "order_id": "entry-live",
            "_bracket_children_missing": True,
            "bracket_child_verified": False,
        },
    )
    _enter(qs, om)
    assert qs._pos.phase is PositionPhase.FLAT
    assert ("market", {"side": "sell", "quantity": 1.0, "reduce_only": True}) in om.calls
    assert qs._last_exit_time > 0


def test_non_delta_standalone_path_places_entry_then_sl_then_tp_once_each():
    qs = _base_strategy()
    om = MockOrderManager(
        active_exchange="coinswitch",
        bracket_response=None,
        limit_response={"fill_price": 100.0, "quantity": 1.0, "order_id": "entry-limit"},
    )
    _enter(qs, om)
    call_names = [name for name, _ in om.calls]
    assert qs._pos.phase is PositionPhase.ACTIVE
    assert call_names == ["bracket", "limit", "cancel_symbol_conditionals", "stop_loss", "take_profit"]
    assert qs._pos.sl_order_id == "sl-live"
    assert qs._pos.tp_order_id == "tp-live"


def test_non_delta_sl_order_failure_flattens_and_does_not_leave_active_position():
    qs = _base_strategy()
    om = MockOrderManager(
        active_exchange="coinswitch",
        bracket_response=None,
        limit_response={"fill_price": 100.0, "quantity": 1.0, "order_id": "entry-limit"},
        sl_response=None,
    )
    om.sl_response = None
    _enter(qs, om)
    call_names = [name for name, _ in om.calls]
    assert qs._pos.phase is PositionPhase.FLAT
    assert call_names == ["bracket", "limit", "cancel_symbol_conditionals", "stop_loss", "market"]
    assert qs._last_exit_time > 0


def test_non_delta_tp_order_failure_cancels_sl_then_flattens():
    qs = _base_strategy()
    om = MockOrderManager(
        active_exchange="coinswitch",
        bracket_response=None,
        limit_response={"fill_price": 100.0, "quantity": 1.0, "order_id": "entry-limit"},
        sl_response={"order_id": "sl-live"},
        tp_response=None,
    )
    om.tp_response = None
    _enter(qs, om)
    call_names = [name for name, _ in om.calls]
    assert qs._pos.phase is PositionPhase.FLAT
    assert call_names == ["bracket", "limit", "cancel_symbol_conditionals", "stop_loss", "take_profit", "cancel_order", "market"]
    assert qs._last_exit_time > 0


def test_adverse_slippage_invalidates_levels_and_flatten_before_position_state_is_marked_active():
    qs = _base_strategy(force_sl=98.0, force_tp=100.5, readiness_score=0.70)
    om = MockOrderManager(
        active_exchange="delta",
        bracket_response={
            "bracket_order": True,
            "fill_price": 101.2,
            "quantity": 1.0,
            "order_id": "entry-live",
            "bracket_sl_order_id": "sl-child",
            "bracket_tp_order_id": "tp-child",
            "bracket_child_verified": True,
        },
    )
    _enter(qs, om)
    assert qs._pos.phase is PositionPhase.FLAT
    assert ("market", {"side": "sell", "quantity": 1.0, "reduce_only": True}) in om.calls
    assert qs._last_exit_time > 0


# ───────────────────────── Trailing and exit lifecycle ───────────────────────

def _active_strategy_for_trailing():
    qs = _base_strategy()
    qs._pos = PositionState(
        phase=PositionPhase.ACTIVE,
        side="long",
        quantity=1.0,
        entry_price=100.0,
        sl_price=98.0,
        tp_price=106.0,
        sl_order_id="sl-old",
        tp_order_id="tp-old",
        entry_time=time.time() - 600,
        initial_risk=2.0,
        initial_sl_dist=2.0,
        entry_atr=1.0,
    )
    qs.current_sl_price = 98.0
    qs.current_tp_price = 106.0
    qs._cvd = SimpleNamespace(get_trend_signal=lambda: 0.20)
    qs._ict = None
    qs._liq_map = SimpleNamespace(get_snapshot=lambda price, atr: None)
    qs._last_trail_block_log = 0.0
    qs._profit_defense_exit_if_needed = lambda *args, **kwargs: False
    qs._record_exchange_exit = lambda *args, **kwargs: setattr(qs, "_record_exchange_exit_called", True)
    qs._trail_payoff_lock_ok = lambda pos, new_sl, atr, phase: (True, "mock payoff ok")
    qs._liq_trail_tg_last = 0.0
    return qs


def test_trail_holds_when_engine_has_no_new_liquidity_structure_stop():
    qs = _active_strategy_for_trailing()
    qs._liq_trail = SimpleNamespace(compute=lambda **kwargs: LiquidityTrailResult(new_sl=None, anchor=None, reason="hold", phase="HOLD"))
    om = MockOrderManager(replace_response={"order_id": "sl-new"})
    assert qs._update_trailing_sl(om, MockDataManager(), price=102.0, now=time.time()) is False
    assert qs._pos.sl_price == pytest.approx(98.0)
    assert "replace_stop_loss" not in [name for name, _ in om.calls]


def test_trail_blocks_non_executable_stop_that_would_cross_market():
    qs = _active_strategy_for_trailing()
    qs._liq_trail = SimpleNamespace(compute=lambda **kwargs: LiquidityTrailResult(new_sl=101.99, anchor=None, reason="too close", phase="STRUCTURE"))
    om = MockOrderManager(replace_response={"order_id": "sl-new"})
    assert qs._update_trailing_sl(om, MockDataManager(), price=102.0, now=time.time()) is False
    assert qs._pos.sl_price == pytest.approx(98.0)
    assert "replace_stop_loss" not in [name for name, _ in om.calls]


def test_trail_replace_unprotected_emergency_flattens_and_marks_exiting():
    qs = _active_strategy_for_trailing()
    qs._liq_trail = SimpleNamespace(compute=lambda **kwargs: LiquidityTrailResult(new_sl=101.0, anchor=None, reason="lock", phase="STRUCTURE"))
    om = MockOrderManager(replace_response={"error": "UNPROTECTED"})
    assert qs._update_trailing_sl(om, MockDataManager(), price=103.0, now=time.time()) is False
    assert qs._pos.phase is PositionPhase.EXITING
    assert ("emergency_flatten", {"reason": "trail_unprotected"}) in om.calls


def test_trail_replace_restored_updates_tracking_without_marking_trail_advanced():
    qs = _active_strategy_for_trailing()
    qs._liq_trail = SimpleNamespace(compute=lambda **kwargs: LiquidityTrailResult(new_sl=101.0, anchor=None, reason="lock", phase="STRUCTURE"))
    om = MockOrderManager(replace_response={"error": "PLACE_FAILED_RESTORED", "restore_order_id": "sl-restored", "restore_trigger": 98.5})
    assert qs._update_trailing_sl(om, MockDataManager(), price=103.0, now=time.time()) is False
    assert qs._pos.phase is PositionPhase.ACTIVE
    assert qs._pos.sl_order_id == "sl-restored"
    assert qs._pos.sl_price == pytest.approx(98.5)
    assert qs._pos.trail_active is False


def test_trail_success_updates_position_only_after_exchange_replace_success():
    qs = _active_strategy_for_trailing()
    qs._liq_trail = SimpleNamespace(
        compute=lambda **kwargs: LiquidityTrailResult(new_sl=101.0, anchor=None, reason="lock", phase="STRUCTURE", r_multiple=1.5)
    )
    om = MockOrderManager(replace_response={"order_id": "sl-new"})
    assert qs._update_trailing_sl(om, MockDataManager(), price=103.0, now=time.time()) is True
    assert qs._pos.phase is PositionPhase.ACTIVE
    assert qs._pos.sl_order_id == "sl-new"
    assert qs._pos.sl_price == pytest.approx(101.0)
    assert qs._pos.trail_active is True
    assert qs.current_sl_price == pytest.approx(101.0)


def test_manual_exit_tracks_reduce_only_market_exit_without_recording_fake_pnl_immediately():
    qs = _active_strategy_for_trailing()
    qs._estimate_pnl = lambda pos, price, entry_fill_type="taker": 2.0
    qs._send_telegram = lambda *args, **kwargs: None
    om = MockOrderManager()
    qs._exit_trade(om, price=103.0, reason="manual_test")
    assert qs._pos.phase is PositionPhase.EXITING
    assert qs._pos.manual_exit_order_id == "market-exit"
    assert qs._pos.manual_exit_reason == "manual_test"
    assert [name for name, _ in om.calls] == ["cancel_all_exit_orders", "market"]
    assert qs._pnl_recorded_for == 0.0

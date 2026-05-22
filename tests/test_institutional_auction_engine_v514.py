import inspect
from pathlib import Path
import threading
import time

import pytest

from strategy.auction_state import MicrostructureState, build_delivery_evidence, build_microstructure_state
from strategy.entry_engine import EntryEngine, EntryType
from strategy.liquidity_map import LiquidityMapSnapshot, LiquidityPool, PoolSide, PoolStatus, PoolTarget, SweepResult
from strategy.quant_strategy import QuantStrategy
from orchestration.multi_asset_bot import MultiAssetQuantBot
from exchanges.delta.data_manager import DeltaDataManager

ROOT = Path(__file__).resolve().parents[1]


def _trend(step: float, n: int):
    rows = []
    px = 90.0
    for _ in range(n):
        o, c = px, px + step
        rows.append({"o": o, "h": max(o, c) + 0.2, "l": min(o, c) - 0.2, "c": c})
        px = c
    return rows


def _continuation_bars():
    rows = [{"o": 99.20, "h": 99.70, "l": 99.00, "c": 99.45} for _ in range(34)]
    for i in range(8, 20):
        rows[i] = {"o": 99.30, "h": 100.00, "l": 99.10, "c": 99.60}
    rows[20] = {"o": 99.40, "h": 99.70, "l": 99.25, "c": 99.50}
    rows[21] = {"o": 99.40, "h": 100.00, "l": 99.30, "c": 99.80}
    rows[22] = {"o": 100.20, "h": 103.20, "l": 100.10, "c": 103.00}
    rows[23] = {"o": 101.30, "h": 103.40, "l": 101.00, "c": 103.10}
    for i in range(24, 33):
        rows[i] = {"o": 103.00, "h": 104.30, "l": 102.90, "c": 104.00}
    rows[33] = {"o": 100.60, "h": 100.80, "l": 100.30, "c": 100.50}
    return rows


def _snapshot(*, bsl=None, ssl=None, sweeps=None):
    return LiquidityMapSnapshot(
        bsl_pools=list(bsl or []), ssl_pools=list(ssl or []), primary_target=None,
        recent_sweeps=list(sweeps or []), swept_bsl_levels=[], swept_ssl_levels=[],
        nearest_bsl_atr=999.0, nearest_ssl_atr=999.0, timestamp=time.time(),
    )


def _long_target(now):
    pool = LiquidityPool(110.0, PoolSide.BSL, "15m", status=PoolStatus.DETECTED, created_at=now - 90, htf_count=1)
    return PoolTarget(pool, 9.50, "long", 5.0, ["15m"])


def test_microprice_and_signed_aggressive_flow_are_evidence_not_probability():
    now = time.time()
    orderbook = {"bids": [[100.0, 40.0], [99.9, 20.0]], "asks": [[100.1, 5.0], [100.2, 5.0]], "timestamp": now}
    trades = [
        {"price": 100.1, "quantity": 3.0, "side": "buy", "timestamp": now - 1},
        {"price": 100.1, "quantity": 2.0, "side": "buy", "timestamp": now - 2},
        {"price": 100.0, "quantity": 0.5, "side": "sell", "timestamp": now - 1},
    ]
    state = build_microstructure_state(orderbook, trades, atr=1.0, now=now)
    assert state.fresh and state.microprice > state.mid
    assert state.depth_imbalance > 0 and state.trade_imbalance > 0 and state.score_long > state.score_short
    assert not hasattr(state, "probability")


def test_delivery_evidence_points_to_observable_liquidity_destination():
    now = time.time()
    bsl = PoolTarget(LiquidityPool(104.0, PoolSide.BSL, "15m", status=PoolStatus.DETECTED, created_at=now, htf_count=1), 4.0, "long", 5.0, ["15m"])
    ssl = PoolTarget(LiquidityPool(85.0, PoolSide.SSL, "15m", status=PoolStatus.DETECTED, created_at=now, htf_count=1), 15.0, "short", 0.2, ["15m"])
    evidence = build_delivery_evidence(_snapshot(bsl=[bsl], ssl=[ssl]), 100.0, 1.0, ((0.7, 0.6), (0.4, 0.4)), MicrostructureState.empty(now))
    assert evidence.preferred_side == "long"
    assert evidence.support_long > evidence.support_short
    assert evidence.liquidity_pull_component > 0


def test_displacement_continuation_executes_without_manufacturing_a_raid():
    now = time.time()
    engine = EntryEngine()
    engine.update(_snapshot(bsl=[_long_target(now)]), 100.50, 1.0, now,
                  candles_5m=_continuation_bars(), candles_15m=_trend(0.55, 33), candles_4h=_trend(1.40, 28))
    signal = engine.get_signal()
    assert signal is not None and signal.entry_type is EntryType.DISPLACEMENT_CONTINUATION
    assert signal.sweep_result is None and signal.archetype == "DISPLACEMENT_CONTINUATION"
    assert signal.probability_calibrated is False and signal.delivery_probability == 0.0


def test_liquidity_expansion_retest_is_distinct_from_stop_run_reversal():
    now = time.time()
    consumed = LiquidityPool(100.0, PoolSide.BSL, "5m", status=PoolStatus.SWEPT, created_at=now - 40)
    sweep = SweepResult(consumed, 20, 100.20, 0.20, 1.4, 0.80, "short", now - 30)
    engine = EntryEngine()
    engine.update(_snapshot(bsl=[_long_target(now)], sweeps=[sweep]), 100.50, 1.0, now,
                  candles_5m=_continuation_bars(), candles_15m=_trend(0.55, 33), candles_4h=_trend(1.40, 28))
    signal = engine.get_signal()
    assert signal is not None and signal.entry_type is EntryType.LIQUIDITY_EXPANSION_RETEST
    assert signal.side == "long" and signal.sweep_result is sweep


def test_execution_viability_computes_net_r_without_false_expected_value():
    qs = QuantStrategy.__new__(QuantStrategy)
    result = qs._execution_viability_model(side="long", price=100.0, sl_price=98.0, tp_price=106.0, use_maker_entry=True, delivery_probability=None)
    assert result.net_win_r > 0.0 and result.net_loss_r > 0.0
    assert result.utility_known is False and result.delivery_probability is None


def test_websocket_event_bridge_wakes_candidate_monitor_without_order_work():
    qs = QuantStrategy.__new__(QuantStrategy)
    qs._market_event = threading.Event()
    qs._market_wakeup_cb = threading.Event().set
    qs._last_market_event_time = 0.0
    qs._last_known_price = 0.0
    qs._on_realtime_trade(101.25, 2.0, "buy")
    assert qs.consume_market_event() is True and qs._last_known_price == pytest.approx(101.25)
    qs._on_realtime_quote(101.30)
    assert qs.consume_market_event() is True and qs._last_known_price == pytest.approx(101.30)


def test_feed_and_orchestrator_have_event_driven_reprice_path():
    data_src = inspect.getsource(DeltaDataManager._on_orderbook) + inspect.getsource(DeltaDataManager._on_trade)
    loop_src = inspect.getsource(MultiAssetQuantBot.run)
    assert "_on_realtime_quote" in data_src and "_on_realtime_trade" in data_src
    assert "event_driven=urgent" in loop_src and "_market_wakeup.wait" in loop_src


def test_uncalibrated_fee_floor_is_probability_neutral_and_runtime_safe():
    from strategy.fee_engine import ExecutionCostEngine
    engine = ExecutionCostEngine()
    neutral = engine.min_required_tp_move(
        price=100.0, atr=2.0, atr_percentile=0.50,
        use_maker_entry=True, delivery_probability=None,
    )
    explicit_mid = engine.min_required_tp_move(
        price=100.0, atr=2.0, atr_percentile=0.50,
        use_maker_entry=True, delivery_probability=0.50,
    )
    assert neutral > 0.0
    assert neutral == pytest.approx(explicit_mid)


def test_all_streaming_venues_wake_single_authority_outside_feed_callbacks():
    delta_src = inspect.getsource(DeltaDataManager._on_orderbook) + inspect.getsource(DeltaDataManager._on_trade)
    coinswitch_src = (ROOT / "exchanges" / "coinswitch" / "data_manager.py").read_text()
    icici_src = (ROOT / "exchanges" / "icici" / "underlying_data_manager.py").read_text()
    assert "_on_realtime_quote" in delta_src and "_on_realtime_trade" in delta_src
    assert "_on_realtime_quote" in coinswitch_src and "_on_realtime_trade" in coinswitch_src
    assert "_on_realtime_quote" in icici_src and "_on_stream_candle" in icici_src

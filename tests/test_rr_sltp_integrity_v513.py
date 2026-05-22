from pathlib import Path
from types import SimpleNamespace
import inspect
import time

import pytest

from strategy.entry_engine import EntryEngine, _timeframe_atr
from strategy.liquidity_map import LiquidityMapSnapshot, LiquidityPool, PoolSide, PoolStatus, PoolTarget, _native_closed_atr
from strategy.quant_strategy import ATREngine, _round_structural_levels
from strategy.tp_ladder import build_tp_ladder

ROOT = Path(__file__).resolve().parents[1]


def _volatile_bars(n=80):
    rows=[]
    px=100.0
    for i in range(n):
        move=(0.25 if i % 7 else 2.0) * (1 if i % 3 else -1)
        o=px; c=px+move; h=max(o,c)+0.7+(i % 5)*0.05; l=min(o,c)-0.6-(i % 4)*0.05
        rows.append({'t': 1_900_000_000_000+i*300_000, 'o':o, 'h':h, 'l':l, 'c':c, 'v':100+i})
        px=c
    rows.append({'t': 1_900_000_000_000+n*300_000, 'o':px, 'h':px+0.1, 'l':px-0.1, 'c':px, 'v':1})  # forming
    return rows


def _snap(bsl=None):
    return LiquidityMapSnapshot(bsl_pools=list(bsl or []), ssl_pools=[], primary_target=None,
                                recent_sweeps=[], swept_bsl_levels=[], swept_ssl_levels=[],
                                nearest_bsl_atr=999.0, nearest_ssl_atr=999.0, timestamp=time.time())


def test_five_minute_atr_is_one_authoritative_wilder_measure_across_entry_and_liquidity():
    raw=_volatile_bars()
    atr_engine=ATREngine(); execution_atr=atr_engine.compute(raw)
    liquidity_atr=_native_closed_atr(raw)
    context_atr=_timeframe_atr(raw[:-1])
    assert execution_atr == pytest.approx(liquidity_atr, rel=1e-12)
    assert execution_atr == pytest.approx(context_atr, rel=1e-12)


def test_structural_level_rounding_never_tightens_stop_or_moves_tp_past_front_run(monkeypatch):
    import strategy.quant_strategy as qm
    monkeypatch.setattr(qm.QCfg, 'TICK_SIZE', staticmethod(lambda: 0.10))
    long_sl, long_tp = _round_structural_levels('long', 98.06, 109.98)
    assert long_sl == pytest.approx(98.0) and long_sl <= 98.06
    assert long_tp == pytest.approx(109.9) and long_tp <= 109.98
    short_sl, short_tp = _round_structural_levels('short', 101.94, 90.02)
    assert short_sl == pytest.approx(102.0) and short_sl >= 101.94
    assert short_tp == pytest.approx(90.1) and short_tp >= 90.02


def test_target_utility_is_net_of_venue_cost_and_rejects_negative_expected_value():
    engine=EntryEngine()
    engine.set_structural_delivery_policy(min_rr=1.0, max_rr_reference=5.0)
    engine._thesis=SimpleNamespace(context_4h=SimpleNamespace(confidence=0.20), context_15m=SimpleNamespace(confidence=0.20))
    p=LiquidityPool(103.2, PoolSide.BSL, '15m', status=PoolStatus.DETECTED, created_at=time.time(), htf_count=1)
    t=PoolTarget(p, 3.2, 'long', 0.10, ['15m'])
    engine.set_execution_cost_model(0.0, 0.0)
    assert engine._select_liquidity_target('long', 100.0, 98.0, _snap([t]), 1.0) is not None
    engine.set_execution_cost_model(2.0, 200.0)
    assert engine._select_liquidity_target('long', 100.0, 98.0, _snap([t]), 1.0) is None
    assert engine.analysis_info['target_audit']['non_positive_net_utility'] >= 1


def test_tp_ladder_cannot_generate_fibonacci_fallback_or_gap_filler_targets():
    active = inspect.getsource(build_tp_ladder)
    assert '_fib_fallback_internals' not in active
    assert '_add_fib_gap_fillers' not in active
    plan = build_tp_ladder(side='long', entry=100.0, sl=98.0, final_tp=110.0, atr=1.0,
                           total_quantity=10.0, pool_report={'candidates': []},
                           max_internal_legs=3, min_leg_fraction=0.1, roundtrip_cost_bps=2.0)
    assert len(plan.legs) == 1 and plan.legs[0].role == 'FINAL'
    assert plan.legs[0].price == pytest.approx(110.0)


def test_orderbook_staleness_is_execution_block_not_structural_state_erasure():
    src=inspect.getsource(__import__('strategy.quant_strategy', fromlist=['QuantStrategy']).QuantStrategy._evaluate_entry)
    assert src.index('self._entry_engine.update') < src.index('signal is not None and not spread_ok')
    assert 'WAIT_FOR_EXECUTABLE_BOOK' in src


def test_gross_rr_matches_realised_pnl_ratio_for_linear_and_delta_btc_inverse_models():
    from core.pnl import gross_pnl_usd
    qty = 0.37
    for inverse in (False, True):
        long_risk = abs(gross_pnl_usd('LONG', 100.0, 98.0, qty, inverse=inverse))
        long_reward = gross_pnl_usd('LONG', 100.0, 106.0, qty, inverse=inverse)
        short_risk = abs(gross_pnl_usd('SHORT', 100.0, 102.0, qty, inverse=inverse))
        short_reward = gross_pnl_usd('SHORT', 100.0, 94.0, qty, inverse=inverse)
        assert long_reward / long_risk == pytest.approx(3.0)
        assert short_reward / short_risk == pytest.approx(3.0)


def test_tp_ladder_source_contains_no_synthetic_projection_implementation():
    src = (ROOT / 'strategy' / 'tp_ladder.py').read_text()
    lowered = src.lower()
    assert 'fibonacci' not in lowered
    assert '_fib_fallback' not in lowered
    assert '_add_fib' not in lowered
    assert 'fib_score' not in lowered
    assert 'fib_confluence' not in lowered


def test_front_run_buffer_cannot_move_target_behind_entry_and_manufacture_rr():
    engine = EntryEngine()
    engine.set_structural_delivery_policy(min_rr=1.0, max_rr_reference=5.0)
    engine._thesis = SimpleNamespace(context_4h=SimpleNamespace(confidence=1.0), context_15m=SimpleNamespace(confidence=1.0))
    too_near = LiquidityPool(100.01, PoolSide.BSL, '15m', status=PoolStatus.DETECTED, created_at=time.time(), htf_count=1)
    target = PoolTarget(too_near, 5.0, 'long', 0.10, ['15m'])
    engine.set_execution_cost_model(0.0, 0.0)
    assert engine._select_liquidity_target('long', 100.0, 99.99, _snap([target]), 1.0) is None
    assert engine.analysis_info['target_audit']['tp_buffer_crossed_entry'] == 1


def test_entry_engine_contains_hard_protective_side_stop_guard():
    src = inspect.getsource(EntryEngine._try_reprice_thesis)
    assert 'INVALID_STRUCTURAL_STOP_SIDE' in src
    assert 'stop < entry' in src and 'stop > entry' in src


def test_execution_handoff_reapplies_rr_floor_after_protective_tick_rounding():
    import strategy.quant_strategy as qm
    src = inspect.getsource(qm.QuantStrategy._enter_trade)
    assert 'post_rounding_rr_below_floor' in src
    assert '_execution_min_rr' in src
    assert 'rr + 1e-12 < _execution_min_rr' in src

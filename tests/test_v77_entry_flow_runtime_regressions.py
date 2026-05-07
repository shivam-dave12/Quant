"""Runtime-log regressions for entry-flow integrity.

Keeps the same strategy criteria, but prevents duplicate application of the
same post-sweep/SL-liquidity surface that can delay entries or over-widen stops.
"""

import inspect

from strategy.direction_engine import DirectionEngine
from strategy.entry_engine import EntryEngine


def test_direction_engine_merges_duplicate_same_cluster_sweep_without_restart():
    engine = DirectionEngine()
    engine.on_sweep(
        swept_pool_price=404.2,
        pool_type="SSL",
        price=404.75,
        atr=1.0,
        now=1_000.0,
        quality=0.85,
    )
    state = engine._ps_state
    assert state is not None
    state.rev_evidence = 72.0
    state.cont_evidence = 40.9

    engine.on_sweep(
        swept_pool_price=404.2,
        pool_type="SSL",
        price=404.50,
        atr=1.0,
        now=1_031.0,
        quality=1.00,
    )

    assert engine._ps_state is state
    assert engine._ps_state.entered_at == 1_000.0
    assert engine._ps_state.rev_evidence == 72.0
    assert engine._ps_state.cont_evidence == 40.9
    assert engine._ps_state_quality == 1.00


def test_refined_entry_path_uses_single_liquidity_sl_pass():
    src = inspect.getsource(EntryEngine._evaluate_pending_refined_entry)
    before_envelope = src.split("sl, sl_reason = self._apply_institutional_sl_envelope", 1)[0]
    assert "_push_sl_behind_pools" not in before_envelope

import config

from strategy.entry_engine import EntryEngine
from strategy.quant_strategy import PositionState, QuantStrategy


def test_v76_entry_engine_derives_safe_leverage_for_structural_stop():
    old_leverage = getattr(config, "LEVERAGE", 40)
    old_dynamic = getattr(config, "DYNAMIC_LEVERAGE_FOR_STRUCTURAL_STOPS", True)
    config.LEVERAGE = 40
    config.DYNAMIC_LEVERAGE_FOR_STRUCTURAL_STOPS = True
    try:
        static_ok, _liq, _guard = EntryEngine._stop_clears_liquidation_guard(
            "long", 100.0, 94.0, leverage=40)
        lev, liq, guard, reason = EntryEngine._execution_leverage_for_stop(
            "long", 100.0, 94.0)
    finally:
        config.LEVERAGE = old_leverage
        config.DYNAMIC_LEVERAGE_FOR_STRUCTURAL_STOPS = old_dynamic

    assert static_ok is False
    assert 1.0 <= lev < 40.0
    assert liq > 0.0
    assert guard < 94.0
    assert "leverage" in reason


def test_v76_quant_strategy_uses_dynamic_leverage_for_liquidation_sanity():
    old_leverage = getattr(config, "LEVERAGE", 40)
    old_dynamic = getattr(config, "DYNAMIC_LEVERAGE_FOR_STRUCTURAL_STOPS", True)
    config.LEVERAGE = 40
    config.DYNAMIC_LEVERAGE_FOR_STRUCTURAL_STOPS = True
    strategy = QuantStrategy.__new__(QuantStrategy)
    try:
        static_ok, _static_liq, _static_guard, _ = strategy._sl_liquidation_sanity(
            "long", 100.0, 94.0, leverage=40)
        lev, _liq, _guard, _reason = strategy._execution_leverage_for_stop(
            "long", 100.0, 94.0)
        dynamic_ok, dyn_liq, dyn_guard, _ = strategy._sl_liquidation_sanity(
            "long", 100.0, 94.0, leverage=lev)
    finally:
        config.LEVERAGE = old_leverage
        config.DYNAMIC_LEVERAGE_FOR_STRUCTURAL_STOPS = old_dynamic

    assert static_ok is False
    assert dynamic_ok is True
    assert 1.0 <= lev < 40.0
    assert dyn_liq > 0.0
    assert dyn_guard < 94.0


def test_v76_position_margin_reporting_uses_execution_leverage():
    pos = PositionState(entry_price=100.0, quantity=2.0, execution_leverage=5.0)

    assert QuantStrategy._position_leverage(pos) == 5.0

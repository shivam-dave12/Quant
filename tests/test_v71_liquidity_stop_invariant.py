from types import SimpleNamespace

import config
from strategy.entry_engine import EntryEngine


def _engine_for_sl_envelope():
    e = EntryEngine.__new__(EntryEngine)
    e._atr_pctile = 0.5
    e._last_liq_snapshot = None
    e._current_quant_posterior = lambda: 0.50
    e._dominant_institutional_tp_reward = lambda snap, side, price, atr: 10.0
    # For short: guard above entry, plenty of liquidation room.
    e._liquidation_guard = lambda side, entry: (125.0, 130.0, 30.0)
    e._push_sl_behind_pools = lambda sl, side, price, atr: sl
    return e


def test_sl_envelope_refuses_to_execute_inside_required_protective_pool():
    e = _engine_for_sl_envelope()
    pick = SimpleNamespace(quality=0.10, reasons=["test protective BSL"])
    target = SimpleNamespace(pool=SimpleNamespace(price=108.0))
    e._find_sl_pool = lambda **kwargs: (110.0, target, pick)

    sl, reason = e._apply_institutional_sl_envelope(
        snap=SimpleNamespace(),
        side="short",
        price=100.0,
        atr=2.0,
        structural_sl=105.0,   # inside the BSL shield; should never route
        invalidation_price=104.0,
        label="reversal",
    )

    assert sl is None
    assert "refusing executable stop inside liquidity" in reason
    assert "protective SL pool" in reason


def test_sl_envelope_allows_stop_already_beyond_protective_pool():
    e = _engine_for_sl_envelope()
    pick = SimpleNamespace(quality=0.10, reasons=["test protective BSL"])
    target = SimpleNamespace(pool=SimpleNamespace(price=108.0))
    e._find_sl_pool = lambda **kwargs: (110.0, target, pick)

    sl, reason = e._apply_institutional_sl_envelope(
        snap=SimpleNamespace(),
        side="short",
        price=100.0,
        atr=2.0,
        structural_sl=112.0,   # already beyond pool SL
        invalidation_price=104.0,
        label="reversal",
    )

    assert sl == 112.0
    assert reason == "ok"


def test_sl_envelope_refuses_pool_beyond_liquidation_guard_instead_of_shrinking_inside():
    old_dynamic = getattr(config, "DYNAMIC_LEVERAGE_FOR_STRUCTURAL_STOPS", True)
    config.DYNAMIC_LEVERAGE_FOR_STRUCTURAL_STOPS = False
    e = _engine_for_sl_envelope()
    e._liquidation_guard = lambda side, entry: (108.0, 109.0, 9.0)
    pick = SimpleNamespace(quality=0.95, reasons=["HTF protective BSL"])
    target = SimpleNamespace(pool=SimpleNamespace(price=118.0))
    e._find_sl_pool = lambda **kwargs: (120.0, target, pick)

    try:
        sl, reason = e._apply_institutional_sl_envelope(
            snap=SimpleNamespace(),
            side="short",
            price=100.0,
            atr=2.0,
            structural_sl=105.0,
            invalidation_price=104.0,
            label="reversal",
        )
    finally:
        config.DYNAMIC_LEVERAGE_FOR_STRUCTURAL_STOPS = old_dynamic

    assert sl is None
    assert "beyond liquidation guard" in reason

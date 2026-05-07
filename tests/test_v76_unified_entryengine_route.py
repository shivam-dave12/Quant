import inspect
from types import SimpleNamespace

from strategy.entry_engine import EntryType
from strategy.quant_strategy import QuantStrategy


def _qs():
    qs = QuantStrategy.__new__(QuantStrategy)
    qs._bounded = QuantStrategy._bounded
    qs._target_pool_realism = lambda *a, **k: (0.62, ["target priced by EntryEngine"], ["target surface weak"])
    qs._target_utility_reject_reason = lambda signal: "selected TP has no positive executable utility: advisory only"
    qs._sl_liquidation_sanity = lambda side, entry, sl: (True, 0.0, 0.0, "ok")
    return qs


def _signal():
    return SimpleNamespace(
        side="long",
        entry_type=EntryType.SWEEP_REVERSAL,
        entry_price=100.0,
        sl_price=98.0,
        tp_price=104.0,
        rr_ratio=2.0,
        conviction=0.72,
        sweep_result=SimpleNamespace(quality=0.80),
        target_pool=SimpleNamespace(),
    )


def test_v76_route_uses_entry_engine_as_single_alpha_source():
    src = inspect.getsource(QuantStrategy._evaluate_entry)
    assert "_entry_engine_route_decision" in src
    assert "_institutional_decision_matrix(" not in src
    assert "_unified_entry_gate(" not in src
    assert "_entry_required_confirms" not in src


def test_v76_target_surface_is_advisory_not_hard_route_veto():
    decision = _qs()._entry_engine_route_decision(
        _signal(), ict_ctx=None, flow_state=None, liq_snapshot=None, price=100.0, atr=1.0
    )
    assert decision.allowed
    assert decision.size_mult > 0
    assert any("target_surface_advisory" in r for r in decision.allow_reasons)


def test_v76_route_still_blocks_malformed_order_geometry():
    sig = _signal()
    sig.tp_price = 99.0
    decision = _qs()._entry_engine_route_decision(
        sig, ict_ctx=None, flow_state=None, liq_snapshot=None, price=100.0, atr=1.0
    )
    assert not decision.allowed
    assert any("long levels" in r for r in decision.reject_reasons)

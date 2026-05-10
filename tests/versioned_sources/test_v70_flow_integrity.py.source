from types import SimpleNamespace


def test_entry_engine_liquidation_guard_uses_active_instrument_policy_leverage():
    import config
    from strategy.entry_engine import EntryEngine
    from core.instruments import (
        AssetClass,
        ExchangeInstrument,
        ExchangeName,
        TradableInstrument,
        instrument_scope,
    )

    # Global BTC leverage is intentionally high.  A non-BTC instrument with lower
    # exchange leverage must not inherit this inside SL/liquidation sanity.
    old_leverage = getattr(config, "LEVERAGE", 40)
    config.LEVERAGE = 40
    try:
        inst = TradableInstrument(
            asset_id="TESTX",
            display_name="Test xStock",
            asset_class=AssetClass.EQUITY,
            primary_exchange=ExchangeName.DELTA,
            by_exchange={
                ExchangeName.DELTA: ExchangeInstrument(
                    exchange=ExchangeName.DELTA,
                    symbol="TESTXUSD",
                    ws_symbol="TESTXUSD",
                    display_symbol="TESTXUSD",
                    asset_id="TESTX",
                    asset_class=AssetClass.EQUITY,
                    max_leverage=10,
                    status="active",
                )
            },
        )
        with instrument_scope(inst):
            _liq, guard, room = EntryEngine._liquidation_guard("long", 100.0)
        assert 67.0 < guard < 68.0
        assert room > 30.0
    finally:
        config.LEVERAGE = old_leverage


def test_unified_entry_gate_does_not_double_apply_readiness_sizing():
    from strategy.quant_strategy import QuantStrategy, EntryReadinessDecision

    qs = QuantStrategy.__new__(QuantStrategy)
    qs._active_institutional_size_mult = 0.50
    readiness = EntryReadinessDecision(
        allowed=True,
        score=0.80,
        floor=0.55,
        size_mult=0.60,
        reason="test pass",
        hard_rejects=[],
        penalties=[],
        allows=["test"],
    )
    qs._entry_readiness_surface = lambda *args, **kwargs: readiness
    ok, reason = qs._unified_entry_gate(
        SimpleNamespace(side="long", entry_type=SimpleNamespace(value="SWEEP_REVERSAL")),
        SimpleNamespace(),
        SimpleNamespace(),
        SimpleNamespace(),
        100.0,
        1.0,
        0.0,
    )
    assert ok is True
    assert reason == "ENTRY_READINESS_PASS"
    assert qs._active_institutional_size_mult == 0.50


def test_route_readiness_is_single_authority_for_same_signal():
    from strategy.quant_strategy import QuantStrategy, EntryReadinessDecision

    qs = QuantStrategy.__new__(QuantStrategy)
    calls = {"n": 0}
    readiness = EntryReadinessDecision(
        allowed=True,
        score=0.82,
        floor=0.55,
        size_mult=0.70,
        reason="cached pass",
        hard_rejects=[],
        penalties=[],
        allows=["cached"],
    )

    def surface(*args, **kwargs):
        calls["n"] += 1
        return readiness

    qs._entry_readiness_surface = surface
    signal = SimpleNamespace(
        side="long",
        entry_type=SimpleNamespace(value="SWEEP_REVERSAL"),
        entry_price=100.0,
    )

    first = qs._route_readiness_decision(
        signal, SimpleNamespace(), SimpleNamespace(), SimpleNamespace(), 100.0, 2.0
    )
    second = qs._route_readiness_decision(
        signal, SimpleNamespace(), SimpleNamespace(), SimpleNamespace(), 100.0, 2.0
    )

    assert first is readiness
    assert second is readiness
    assert calls["n"] == 1


def test_selector_utility_components_have_single_distance_quality_key():
    src = open("strategy/liquidity_pool_selector.py", "r", encoding="utf-8").read()
    assert src.count('"distance_quality": distance_quality') == 1

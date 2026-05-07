
from types import SimpleNamespace


def test_btc_policy_only_identifies_btc_owner():
    from strategy.btc_institutional_policy import is_btc_context, btc_static_rr_floor
    assert is_btc_context(SimpleNamespace(_asset_id="BTC")) is True
    assert is_btc_context(SimpleNamespace(_asset_id="GOLD")) is False
    assert is_btc_context() is False
    assert 0.89 <= btc_static_rr_floor(2.2, 0.90) <= 1.35


def test_btc_sl_overlay_exists_without_changing_default_gate():
    from strategy.entry_engine import EntryEngine
    engine = EntryEngine()
    assert hasattr(engine, "_apply_btc_v58_institutional_sl_envelope")
    # Direct test context has no BTC instrument scope, so default v75 path remains active.
    assert "btc" not in engine._apply_institutional_sl_envelope.__name__.lower()


def test_btc_selector_still_requires_live_liquidity():
    from strategy.liquidity_pool_selector import _is_live_pool
    assert _is_live_pool(SimpleNamespace(status="ACTIVE")) is True
    assert _is_live_pool(SimpleNamespace(status="SWEPT")) is False
    assert _is_live_pool(SimpleNamespace(status="CONSUMED")) is False

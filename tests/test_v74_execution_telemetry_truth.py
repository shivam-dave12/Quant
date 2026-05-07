import inspect

from execution.order_manager import OrderManager
from strategy.liquidity_trail import LiquidityTrailEngine
from strategy.quant_strategy import QuantStrategy


def test_v74_runtime_fingerprint_present():
    src = inspect.getsource(QuantStrategy._log_init)
    assert "v79-predictive-shelf-sl-staged-liquidity-flow" in src
    assert "v70-flow-audit" not in src


def test_v74_market_native_bracket_reports_taker_fill_type():
    src = inspect.getsource(OrderManager.place_bracket_limit_entry)
    assert 'data["fill_type"]    = "taker" if market_entry else "maker"' in src
    assert 'fill_type={data[\'fill_type\']}' in src


def test_v74_delivery_lock_preserves_r_multiple_in_trail_result():
    src = inspect.getsource(LiquidityTrailEngine._try_delivery_structure_lock)
    assert 'phase="DELIVERY_LOCK", r_multiple=r_multiple' in src
    assert 'phase="DELIVERY_LOCK", r_multiple=0.0' not in src

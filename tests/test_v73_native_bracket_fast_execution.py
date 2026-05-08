import inspect

import config
from execution.order_manager import OrderManager
from strategy.quant_strategy import QuantStrategy
from exchanges.delta.api import DeltaAPI


def test_v73_runtime_fingerprint_present():
    src = inspect.getsource(QuantStrategy._log_init)
    # v74 supersedes the v73 runtime fingerprint while preserving v73 behavior.
    assert "v74-native-bracket-fast-fee-true-flow" in src
    assert "v70-flow-audit" not in src


def test_v73_protected_cross_uses_readiness_not_raw_confidence_hard_veto():
    src = inspect.getsource(QuantStrategy._enter_trade)
    assert "ENTRY_PROTECTED_CROSS_REQUIRE_SIGNAL_CONF" in src
    assert "_sig_conf_ok_for_cross" in src
    assert "readiness_score >= protected_cross_floor" in src
    assert "native_market_bracket" in src
    assert "NATIVE-BRACKET-MARKET" in src


def test_v76_protected_cross_refuses_rr_degradation():
    src = inspect.getsource(QuantStrategy._enter_trade)
    assert "ENTRY_PROTECTED_CROSS_MAX_RR_DEGRADATION" in src
    assert "protected_cross_declined_rr" in src
    assert "maker_at_signal" in src
    assert "native_market_bracket = False" in src


def test_v73_delta_market_bracket_path_exists_and_no_naked_fallback():
    src = inspect.getsource(OrderManager.place_bracket_limit_entry)
    assert "market_entry" in src
    assert "place_bracket_market_entry" in src
    assert "not_filled_timeout" in src
    qsrc = inspect.getsource(QuantStrategy._enter_trade)
    assert "Delta native bracket rejected by exchange/API" in qsrc
    assert "no non-bracket fallback" in qsrc


def test_v73_delta_api_market_orders_do_not_send_limit_only_fields():
    src = inspect.getsource(DeltaAPI.place_order)
    assert '_LIMIT_LIKE_TYPES = {"limit_order", "stop_limit_order", "take_profit_limit_order"}' in src
    assert 'if _otype in _LIMIT_LIKE_TYPES:' in src


def test_v73_config_fast_bracket_policy():
    assert getattr(config, "ENTRY_PROTECTED_CROSS_REQUIRE_SIGNAL_CONF") is False
    assert getattr(config, "DELTA_NATIVE_BRACKET_MARKET_FOR_PROTECTED_CROSS") is True
    assert getattr(config, "PROTECTED_CROSS_FILL_TIMEOUT_SEC") <= 12.0

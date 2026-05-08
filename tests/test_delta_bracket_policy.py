import inspect
import unittest


class DeltaBracketPolicyTests(unittest.TestCase):
    def test_quant_strategy_refuses_delta_non_bracket_fallback(self):
        from strategy.quant_strategy import QuantStrategy

        src = inspect.getsource(QuantStrategy._enter_trade)
        self.assertIn("DELTA_REQUIRE_NATIVE_BRACKET", src)
        self.assertIn("Delta native bracket entry failed", src)
        self.assertIn("refusing non-bracket fallback", src)
        self.assertIn("_active_exchange == \"delta\"", src)

    def test_config_defaults_require_delta_native_bracket(self):
        import config
        self.assertTrue(getattr(config, "DELTA_REQUIRE_NATIVE_BRACKET", False))


if __name__ == "__main__":
    unittest.main()

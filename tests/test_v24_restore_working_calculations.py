from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class V24RestoreWorkingCalculationsTest(unittest.TestCase):
    def test_no_v18_structural_stop_veto_left_in_entry_engine(self):
        src = (ROOT / 'strategy' / 'entry_engine.py').read_text()
        self.assertNotIn('_first_uncovered_protective_pool', src)
        self.assertNotIn('trade must reprice/abstain', src)
        self.assertNotIn('not place SL in front of liquidity', src)

    def test_sl_pool_selector_uses_v66_liquidity_exclusion_model(self):
        src = (ROOT / 'strategy' / 'liquidity_pool_selector.py').read_text()
        self.assertIn('_SL_BUFFER_BASE_ATR        = 0.30', src)
        self.assertIn('_SL_BUFFER_MAX_ATR         = 1.35', src)
        self.assertIn('stronger liquidity → wider buffer', src)
        self.assertIn('_SL_HARD_MAX_DISTANCE_ATR  = 18.0', src)

    def test_trailing_default_is_on_in_this_repaired_trailing_on_build(self):
        import config
        self.assertTrue(bool(getattr(config, 'QUANT_TRAIL_ENABLED', False)))


if __name__ == '__main__':
    unittest.main()

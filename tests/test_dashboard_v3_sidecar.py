import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "dashboard_v3" / "agents"))
sys.path.insert(0, str(ROOT / "dashboard_v3" / "backend"))

from log_tail_agent import parse
from state_store import DashboardState


class DashboardV3SidecarTests(unittest.TestCase):
    def test_parser_extracts_analysis_tick(self):
        line = '17:26:56.885 | INFO | ⚖ RISK | [BTC|DELTA:BTCUSD] ANALYSIS_TICK asset=BTC primary=DELTA symbol=BTCUSD state=SCANNING price=80875.2500 eval_ms=47.9 slots=0/4 policy=crypto lev=40x margin=20% risk_mult=1.00 loop=0.25s'
        ev = parse(line)
        self.assertEqual(ev["type"], "scan")
        self.assertEqual(ev["asset"], "BTC")
        self.assertEqual(ev["symbol"], "BTCUSD")
        self.assertEqual(ev["price"], 80875.25)
        self.assertEqual(ev["policy"], "crypto")

    def test_parser_extracts_posterior_and_sl_deferral(self):
        post = '17:27:37.226 | INFO | • SYSTEM | [GOLD|DELTA:PAXGUSD] 🧠 POSTERIOR ACCEPTED: CONTINUATION LONG [CISD] | p=0.886 min=0.821 EV=1.161 LLR=2.05 U=0.39 ACCEPT quant posterior auction'
        ev = parse(post)
        self.assertEqual(ev["type"], "posterior")
        self.assertEqual(ev["asset"], "GOLD")
        self.assertAlmostEqual(ev["posterior"], 0.886)
        deferred = '17:27:37.228 | INFO | • SYSTEM | [GOLD|DELTA:PAXGUSD] CANDIDATE DEFERRED [sl_envelope]: SL $4538.6 is in front of protective liquidity pool $4538.4 (sig=2.6); required structural SL $4535.2; trade must reprice/abstain side=long sweep=$4551.9'
        ev2 = parse(deferred)
        self.assertEqual(ev2["type"], "candidate_deferred")
        self.assertIn("protective liquidity", ev2["reason"])

    def test_state_keeps_charts_and_asset_detail(self):
        st = DashboardState()
        st.apply({"type":"scan", "asset":"BTC", "venue":"DELTA", "symbol":"BTCUSD", "price":100.0, "state":"SCANNING"})
        st.apply({"type":"posterior", "asset":"BTC", "venue":"DELTA", "symbol":"BTCUSD", "posterior":0.8, "ev":1.2})
        snap = st.snapshot()
        self.assertIn("BTC", snap["charts"]["price"])
        self.assertIn("BTC", snap["charts"]["posterior"])
        detail = st.asset_detail("BTC")
        self.assertEqual(detail["assets"][0]["asset"], "BTC")
        self.assertEqual(detail["decisions"][0]["kind"], "posterior")


if __name__ == "__main__":
    unittest.main()

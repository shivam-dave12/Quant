from __future__ import annotations

import time
from types import SimpleNamespace

from exchanges.icici.data_manager import ICICIOptionDataManager
from strategy.entry_engine import EntryEngine, _TrendContext
from strategy.market_state import build_auction_narrative
from strategy.quant_strategy import QuantStrategy
from strategy.liquidity_map import LiquidityPool, PoolSide, PoolStatus, SweepResult


def _ctx(side: int, score: float, confidence: float = 0.8):
    return _TrendContext(side, confidence, score, 0.8, score, signed_score=score)


def test_firm_parent_liquidity_transfer_owns_side_before_local_reversal_ticket():
    now = time.time()
    parent_pool = LiquidityPool(4534.1, PoolSide.BSL, "1h", status=PoolStatus.SWEPT, created_at=now - 20)
    parent_short = SweepResult(parent_pool, 20, 4539.8, 1.0, 1.7, 0.95, "short", now - 15)
    local_pool = LiquidityPool(4522.4, PoolSide.SSL, "5m", status=PoolStatus.SWEPT, created_at=now - 8)
    local_long = SweepResult(local_pool, 30, 4521.3, 1.0, 1.4, 0.75, "long", now - 5)
    narrative = build_auction_narrative(
        {"4h": _ctx(1, 0.38), "1h": _ctx(-1, -0.72), "15m": _ctx(-1, -0.99)},
        [parent_short], [local_long], now,
    )
    assert narrative.has_firm_parent_control
    assert narrative.control_side == "short"
    decision = EntryEngine()._context_decision_for_raid("long", _ctx(1, 0.38), _ctx(-1, -0.99), 0.75, narrative)
    assert not decision.allowed
    assert decision.block == "AUCTION_CONTROL_REMAINS_OPPOSING_SIDE"


def test_icici_execution_feed_status_cannot_report_live_from_rest_prewarm_only():
    dm = ICICIOptionDataManager(instrument=SimpleNamespace(asset_id="NIFTY"), api=SimpleNamespace())
    dm._stream_subscription_ids = ["quote-a", "depth-a"]
    dm._book_stream_state = {
        ("02-Jun-2026", "call", 24150.0): {"last_stream_tick_ts": 0.0, "last_price": 153.0, "best_bid": 152.0, "best_ask": 153.0},
        ("02-Jun-2026", "put", 24000.0): {"last_stream_tick_ts": 0.0, "last_price": 152.0, "best_bid": 151.0, "best_ask": 152.0},
    }
    status = dm.execution_feed_status()
    assert status["status"] == "ARMED_PENDING_OPTION_WEBSOCKET_TICK"
    assert status["session_vehicle_stream_ready"] is False
    assert status["fresh_vehicle_count"] == 0


def test_data_lineage_reports_analysis_live_but_execution_unready_for_dual_domain():
    now = time.time()
    qs = QuantStrategy.__new__(QuantStrategy)
    rows = [{"t": int((now - 300) * 1000), "o": 100.0, "h": 101.0, "l": 99.0, "c": 100.5, "v": 1.0}] * 40
    class DM:
        def get_data_lineage(self):
            return {"analysis_source": "Underlying", "execution_source": "Option", "analysis_domain": "UNDERLYING", "execution_domain": "OPTION_PREMIUM"}
        def is_analysis_price_fresh(self, _): return True
        # Simulate the defect observed in production: REST/session premium may
        # look fresh even though no option websocket vehicle has produced a
        # routed tick yet.
        def is_execution_price_fresh(self, _): return True
        def get_analysis_last_update(self): return now
        def get_execution_last_update(self): return 0.0
        def get_execution_feed_status(self): return {"status": "ARMED_PENDING_OPTION_WEBSOCKET_TICK", "session_vehicle_stream_ready": False}
    report = qs._audit_structural_inputs(DM(), {"5m": rows, "15m": rows, "4h": rows}, 100.5, now)
    assert report["analysis_quote_fresh"] is True
    assert report["execution_snapshot_fresh"] is True
    assert report["execution_quote_fresh"] is False
    assert report["execution_ready_for_order"] is False
    assert "EXECUTION_VEHICLE_WEBSOCKET_NOT_READY" in report["execution_blockers"]
    assert "ARMED_PENDING_OPTION_WEBSOCKET_TICK" in report["execution_blockers"]


def test_expired_parent_raid_does_not_own_market_forever():
    now = time.time()
    old_pool = LiquidityPool(4534.1, PoolSide.BSL, "1h", status=PoolStatus.SWEPT, created_at=now - 10800)
    old_short = SweepResult(old_pool, 20, 4539.8, 1.0, 1.7, 0.95, "short", now - 10800)
    local_pool = LiquidityPool(4522.4, PoolSide.SSL, "5m", status=PoolStatus.SWEPT, created_at=now - 8)
    local_long = SweepResult(local_pool, 30, 4521.3, 1.0, 1.4, 0.75, "long", now - 5)
    narrative = build_auction_narrative(
        {"4h": _ctx(1, 0.65), "1h": _ctx(1, 0.45), "15m": _ctx(1, 0.72)},
        [old_short], [local_long], now,
    )
    assert not narrative.has_firm_parent_control
    decision = EntryEngine()._context_decision_for_raid("long", _ctx(1, 0.65), _ctx(1, 0.72), 0.75, narrative)
    assert decision.allowed

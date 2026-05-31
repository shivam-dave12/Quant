from __future__ import annotations

import json
import time
from pathlib import Path

import numpy as np

from btchft.orderbook import DeltaOrderBook
from btchft.models import LiveLearningModelStack
from btchft.config import Settings
from btchft.engine import FullBTCStrategyEngine
from btchft.types import BookSnapshot
from btchft.costs import parse_delta_fill, CostModel


def test_delta_checksum_algorithm_example_string():
    asks = [(100.00, 23), (100.05, 34)]
    bids = [(99.04, 87), (98.65, 102), (98.30, 16)]
    assert DeltaOrderBook.checksum_for([("100.00", "23"), ("100.05", "34")], [("99.04", "87"), ("98.65", "102"), ("98.30", "16")]) == 3599895312


def test_orderbook_sequence_gap_halts():
    ob = DeltaOrderBook("BTCUSD")
    asks = [("101.0", "10.0")]; bids = [("100.0", "10.0")]
    cs = DeltaOrderBook.checksum_for(asks, bids)
    assert ob.apply({"type":"ob_updates","action":"snapshot","sy":"BTCUSD","seq":1,"a":[["101.0","10.0"]],"b":[["100.0","10.0"]],"cs":cs,"ts":time.time_ns()}) is not None
    bad = ob.apply({"type":"ob_updates","action":"update","sy":"BTCUSD","seq":3,"a":[],"b":[["100.0","9.0"]],"cs":cs,"ts":time.time_ns()})
    assert bad is None
    assert ob.state.halted and ob.state.sequence_gaps == 1


def test_online_labels_mature_only_after_horizon(tmp_path):
    stack = LiveLearningModelStack((1000,), min_labels_to_score=1, model_dir=tmp_path)
    x = {c: 0.0 for c in __import__('btchft.features').features.FEATURE_COLUMNS}
    stack.observe_decision(1_000_000_000, 100.0, x, 1.0)
    stack.learn_matured(1_500_000_000, 101.0)
    assert stack.matured_labels == 0
    stack.learn_matured(2_000_000_001, 101.0)
    assert stack.matured_labels == 1


def test_synthetic_bootstrap_manifest_rejected(tmp_path):
    import joblib
    m = tmp_path / "m.joblib"; man = tmp_path / "manifest.json"
    joblib.dump({"type":"real_tradeflow_bootstrap_return_model_v5"}, m)
    man.write_text(json.dumps({"is_synthetic": True}), encoding="utf-8")
    stack = LiveLearningModelStack((1000,), min_labels_to_score=1, model_dir=tmp_path)
    try:
        stack.maybe_load_bootstrap(m, man)
    except RuntimeError as e:
        assert "synthetic" in str(e)
    else:
        raise AssertionError("synthetic bootstrap was not rejected")


def test_cost_model_uses_rest_fill_fee(tmp_path):
    c = CostModel(taker_fee_bps_pre_gst=5, maker_fee_bps_pre_gst=2, gst_rate=0.18, impact_floor_bps=2, min_real_fills=1, ledger_path=tmp_path/"fills.jsonl")
    fill = parse_delta_fill({"id":"f1","order_id":"o1","product_symbol":"BTCUSD","side":"buy","role":"taker","size":10,"price":"100000","commission":"1.0"}, contract_value_btc=0.001, decision_mid=99990)
    assert fill is not None
    assert c.observe_fill(fill)
    assert c.snapshot()["real_fill_count"] == 1
    assert c.one_way_fee_bps() >= 5.9



def test_product_fee_metadata_cannot_weaken_configured_fee_floor(tmp_path):
    c = CostModel(
        taker_fee_bps_pre_gst=5,
        maker_fee_bps_pre_gst=2,
        gst_rate=0.18,
        impact_floor_bps=2,
        min_real_fills=30,
        ledger_path=tmp_path/"fills.jsonl",
    )
    # Product metadata from a wrong endpoint/tier may advertise 1 bps.
    # It must not reduce the conservative India futures floor of 5 bps pre-GST.
    c.configure_from_product({"taker_commission_rate": "0.0001", "maker_commission_rate": "0.0001"})
    snap = c.snapshot()
    assert round(snap["scheduled_one_way_taker_fee_bps_with_gst"], 6) == 5.9
    assert round(snap["round_trip_bps"], 6) == 15.8


def test_product_fee_metadata_can_raise_configured_fee_floor(tmp_path):
    c = CostModel(
        taker_fee_bps_pre_gst=5,
        maker_fee_bps_pre_gst=2,
        gst_rate=0.18,
        impact_floor_bps=2,
        min_real_fills=30,
        ledger_path=tmp_path/"fills.jsonl",
    )
    # If exchange metadata reports a higher fee, use it immediately.
    c.configure_from_product({"taker_commission_rate": "0.0007"})
    assert round(c.snapshot()["scheduled_one_way_taker_fee_bps_with_gst"], 6) == 8.26


def test_engine_live_gate_blocks_without_real_labels(tmp_path):
    s = Settings(
        trading_mode="LIVE",
        allow_live=True,
        allow_unvalidated_bootstrap_live=False,
        raw_event_journal=tmp_path/"raw.jsonl.gz",
        feature_journal=tmp_path/"feat.jsonl.gz",
        model_dir=tmp_path/"models",
        execution_ledger=tmp_path/"fills.jsonl",
        state_path=tmp_path/"state.json",
    )
    e = FullBTCStrategyEngine(s)
    assert e._live_gate_reason() in {"model_not_promoted_from_real_live_labels", "insufficient_matured_live_labels"}
    e.close()


def test_v57_prequential_reports_total_and_rolling_window(tmp_path):
    stack = LiveLearningModelStack((1000,), min_labels_to_score=1, model_dir=tmp_path, rolling_window=3, min_promotion_evals=2)
    stack.prequential_total_count = 5
    stack.prequential_total_sum = 0.001
    stack.prequential_total_sq = 0.000001
    stack.prequential.extend([0.0, 0.0001, 0.0002])
    stack.eligible_prediction_total = 2
    status = stack.status(cost_bps=1.0)
    assert status["prequential_count_total"] == 5
    assert status["prequential_window_count"] == 3
    assert status["promotion_test"]["rolling_window"] == 3
    assert "failure_reasons" in status["promotion_test"]


def test_v57_cost_model_hyperliquid_shadow_profile(tmp_path):
    c = CostModel(
        taker_fee_bps_pre_gst=5,
        maker_fee_bps_pre_gst=2,
        gst_rate=0.18,
        impact_floor_bps=2,
        min_real_fills=30,
        ledger_path=tmp_path/"fills.jsonl",
        execution_cost_profile="HYPERLIQUID_TAKER",
        hyperliquid_taker_fee_bps=4.5,
        hyperliquid_maker_fee_bps=1.5,
        hyperliquid_impact_floor_bps=2,
    )
    snap = c.snapshot(spread_bps=0.0)
    assert snap["execution_cost_profile"] == "HYPERLIQUID_TAKER"
    assert round(snap["round_trip_bps"], 6) == 13.0
    assert round(snap["scenario_round_trip_bps"]["DELTA_TAKER"], 6) == 15.8
    assert snap["cost_warning"] is not None

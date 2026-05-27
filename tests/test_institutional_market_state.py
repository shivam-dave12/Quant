from pathlib import Path

from intelligence.cross_venue_btc import build_btc_composite_state
from market_data.feed_health import score_feed_health
from market_data.normalizer import (
    InstrumentMapping,
    aggregate_depth_usd_by_band,
    build_venue_microstate,
    order_book_imbalance_by_band,
    usd_notional_depth,
)
from research.store import (
    DeltaForwardLabel,
    JsonlResearchStore,
    ResearchDecisionRecord,
    ResearchExecutionRecord,
)


def _linear_mapping(venue: str, symbol: str, execution: bool) -> InstrumentMapping:
    return InstrumentMapping(
        venue=venue,
        venue_symbol=symbol,
        canonical_underlying="BTC",
        product_class="perp",
        quote_currency="USD",
        contract_multiplier=1.0,
        settlement_currency="USDT" if venue != "delta" else "USD",
        price_tick=0.5,
        qty_step=0.001,
        execution_enabled=execution,
        notional_model="linear",
    )


def _inverse_delta_mapping() -> InstrumentMapping:
    return InstrumentMapping(
        venue="delta",
        venue_symbol="BTCUSD",
        canonical_underlying="BTC",
        product_class="inverse_perp",
        quote_currency="USD",
        contract_multiplier=1.0,
        settlement_currency="BTC",
        price_tick=0.5,
        qty_step=1.0,
        execution_enabled=True,
        notional_model="inverse_usd_contract",
    )


def test_feed_health_hard_zero_and_latency_penalty_without_universal_staleness_rule():
    disconnected = score_feed_health(
        connected=False,
        heartbeat_ok=True,
        sequence_valid=True,
        snapshot_ready=True,
        exchange_timestamp_available=True,
        latency_vs_baseline_z=0.0,
    )
    assert disconnected.quality_score == 0.0

    unchanged_book = score_feed_health(
        connected=True,
        heartbeat_ok=True,
        sequence_valid=True,
        snapshot_ready=True,
        exchange_timestamp_available=True,
        latency_vs_baseline_z=0.0,
        no_change_heartbeat_valid=True,
    )
    assert unchanged_book.quality_score == 1.0
    assert unchanged_book.usable_for_decision is True

    high_latency = score_feed_health(
        connected=True,
        heartbeat_ok=True,
        sequence_valid=True,
        snapshot_ready=True,
        exchange_timestamp_available=True,
        latency_vs_baseline_z=5.0,
    )
    assert 0.0 < high_latency.quality_score < 1.0


def test_usd_depth_normalisation_respects_linear_vs_inverse_contracts():
    linear = _linear_mapping("coinswitch", "BTCUSDT", execution=False)
    inverse = _inverse_delta_mapping()

    assert usd_notional_depth(displayed_size=2.0, level_price=100000.0, mapping=linear) == 200000.0
    assert usd_notional_depth(displayed_size=2000.0, level_price=100000.0, mapping=inverse) == 2000.0


def test_depth_bands_are_distance_based_and_obi_is_band_specific():
    mapping = _linear_mapping("hyperliquid", "BTC", execution=False)
    bids = [(9999.5, 1.0), (9997.5, 2.0), (9988.0, 3.0)]
    asks = [(10000.5, 1.5), (10002.5, 1.0), (10012.0, 4.0)]
    bid_depth = aggregate_depth_usd_by_band(levels=bids, side="bid", mid=10000.0, mapping=mapping)
    ask_depth = aggregate_depth_usd_by_band(levels=asks, side="ask", mid=10000.0, mapping=mapping)
    obi = order_book_imbalance_by_band(bid_depth, ask_depth)

    assert bid_depth["0-1"] == 9999.5
    assert ask_depth["0-1"] == 10000.5 * 1.5
    assert bid_depth["1-3"] == 9997.5 * 2.0
    assert ask_depth["1-3"] == 10002.5
    assert obi["0-1"] < 0
    assert obi["1-3"] > 0


def test_btc_composite_excludes_invalid_reference_feed_and_preserves_venue_state():
    healthy = score_feed_health(
        connected=True,
        heartbeat_ok=True,
        sequence_valid=True,
        snapshot_ready=True,
        exchange_timestamp_available=True,
        latency_vs_baseline_z=0.0,
    )
    invalid = score_feed_health(
        connected=True,
        heartbeat_ok=True,
        sequence_valid=False,
        snapshot_ready=True,
        exchange_timestamp_available=True,
        latency_vs_baseline_z=0.0,
    )
    delta = build_venue_microstate(
        mapping=_inverse_delta_mapping(),
        bids=[(99999.5, 1000), (99998.0, 1000)],
        asks=[(100000.5, 1000), (100002.0, 1000)],
        feed_health=healthy,
        receive_ts_ns=1_000_000_000,
        exchange_ts_ns=999_000_000,
        ofi_usd_1s=25000,
        tfi_usd_1s=5000,
    )
    coinswitch = build_venue_microstate(
        mapping=_linear_mapping("coinswitch", "BTCUSDT", execution=False),
        bids=[(100009.0, 1.0), (100000.0, 2.0)],
        asks=[(100011.0, 1.0), (100020.0, 2.0)],
        feed_health=healthy,
        receive_ts_ns=1_000_000_000,
        exchange_ts_ns=999_500_000,
        ofi_usd_1s=10000,
        tfi_usd_1s=1000,
    )
    hyperliquid = build_venue_microstate(
        mapping=_linear_mapping("hyperliquid", "BTC", execution=False),
        bids=[(100100.0, 1.0)],
        asks=[(100102.0, 1.0)],
        feed_health=invalid,
        receive_ts_ns=1_000_000_000,
        exchange_ts_ns=999_500_000,
    )

    state = build_btc_composite_state(
        delta_state=delta,
        reference_states={"coinswitch": coinswitch, "hyperliquid": hyperliquid},
    )

    assert set(state.reference_states) == {"coinswitch"}
    assert state.excluded_reference_venues == {"hyperliquid": "feed_quality_zero_or_invalid_sequence"}
    assert state.composite_reference_mid == coinswitch.mid
    assert state.delta_dislocation_bps is not None and state.delta_dislocation_bps < 0
    assert state.flow_agreement_score == 1.0
    assert state.candidate_leader_venue == "coinswitch"
    assert 0.0 < state.delta_execution_quality_score <= 1.0


def test_research_store_records_rejections_executions_and_forward_labels(tmp_path: Path):
    store = JsonlResearchStore(tmp_path / "research")
    decision = ResearchDecisionRecord(
        observation_ts_ns=123,
        desk="DESK_A_BTC",
        venue="delta",
        instrument="BTCUSD",
        candidate_id="btc-123-long",
        decision="NO_TRADE_EXECUTION_UNSAFE",
        model_values={"net_edge_bps": 4.2, "delta_execution_quality": 0.0},
        reasons=["delta_feed_quality_zero"],
        features={"delta": {"spread_bps": 2.0}},
        model_version="deterministic-baseline-v1",
        policy_version="institutional-flow-v1",
    )
    execution = ResearchExecutionRecord(
        observation_ts_ns=124,
        desk="DESK_B_NIFTY_OPTIONS",
        venue="groww",
        instrument="NIFTY26JUN25000CE",
        candidate_id="nifty-124-ce",
        requested_order={"side": "BUY", "qty": 50},
        actual_fill={"qty": 50, "avg_price": 118.75},
        protection_state={"oco_status": "ACTIVE"},
        realised_costs={"fees": 2.5, "spread_bps": 20.0},
    )
    label = DeltaForwardLabel(
        observation_ts_ns=123,
        horizon="10s",
        side="long",
        gross_markout_bps=3.1,
        spread_cost_bps=1.0,
        fee_cost_bps=0.5,
        slippage_estimate_bps=0.7,
        net_executable_return_bps=0.9,
        favorable_excursion_bps=5.0,
        adverse_excursion_bps=1.2,
    )

    store.append_decision(decision)
    store.append_execution(execution)
    store.append_delta_forward_label(label)

    decisions = store.read_records("decisions.jsonl")
    executions = store.read_records("executions.jsonl")
    labels = store.read_records("delta_forward_labels.jsonl")

    assert decisions[0]["decision"] == "NO_TRADE_EXECUTION_UNSAFE"
    assert executions[0]["protection_state"]["oco_status"] == "ACTIVE"
    assert labels[0]["net_executable_return_bps"] == 0.9


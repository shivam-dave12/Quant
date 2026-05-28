import time

from execution.order_manager import OrderManager
from execution.venue_selection import select_execution_venue
from market_data.feed_health import score_feed_health
from market_data.normalizer import InstrumentMapping, build_venue_microstate
from strategy.institutional_strategy import InstitutionalStrategy, PositionPhase, PositionState


def _state(venue: str, symbol: str, *, mid: float = 73000.0, depth_qty: float = 100000.0):
    health = score_feed_health(
        connected=True,
        heartbeat_ok=True,
        sequence_valid=True,
        snapshot_ready=True,
        exchange_timestamp_available=True,
        latency_vs_baseline_z=0.0,
    )
    return build_venue_microstate(
        mapping=InstrumentMapping(
            venue=venue,
            venue_symbol=symbol,
            canonical_underlying="BTC",
            product_class="linear_perp",
            quote_currency="USD",
            contract_multiplier=1.0,
            settlement_currency="USDC",
            price_tick=0.5,
            qty_step=0.00001,
            execution_enabled=True,
            notional_model="linear",
        ),
        bids=[(mid - 0.5, depth_qty), (mid - 1.0, depth_qty)],
        asks=[(mid + 0.5, depth_qty), (mid + 1.0, depth_qty)],
        feed_health=health,
        receive_ts_ns=time.time_ns(),
        exchange_ts_ns=time.time_ns() - 1_000_000,
    )


class _NoBrokerIoOnTick:
    def emergency_flatten(self, *args, **kwargs):
        raise AssertionError("alpha-decay supervision must not flatten on the market tick")


class _Data:
    pass


def test_alpha_horizon_only_arms_reassessment_and_never_liquidates_without_live_confirmation(monkeypatch):
    monkeypatch.setattr(
        "strategy.institutional_strategy._cfg",
        lambda name, default: {
            "DYNAMIC_EXIT_AUTOMATED_EARLY_LIQUIDATION_ENABLED": True,
            "DYNAMIC_EXIT_CLOCK_HORIZON_IS_REASSESSMENT_ONLY": True,
        }.get(name, default),
    )
    strategy = InstitutionalStrategy(instrument=None)
    strategy._pos = PositionState(
        phase=PositionPhase.ACTIVE,
        side="long",
        quantity=0.00055,
        entry_price=73170.0,
        sl_price=72950.0,
        tp_price=73700.0,
        entry_time=time.time() - 8.0,
        exchange="hyperliquid",
        execution_symbol="BTC",
        asset_id="BTC",
        protection_confirmed=True,
        quant_components={"dynamic_protection_plan": {"signal_decay": {"optimal_hold_sec": 7.093, "half_life_sec": 3.0, "observation_interval_sec": 1.0}}},
    )

    strategy._dynamic_exit_supervision(_Data(), _NoBrokerIoOnTick())

    assert strategy._pos.dynamic_exit_requested is False
    assert strategy._pos.dynamic_exit_order_id == ""
    assert strategy._pos.phase is PositionPhase.ACTIVE
    assert strategy._position_reconcile_wakeup.is_set() is False
    assert strategy._pos.quant_components["dynamic_exit_validation"]["reassessment_armed_logged"] is True


def test_confirmed_opposing_executable_alpha_enqueues_reconciled_exit_only_after_sequence(monkeypatch):
    clock = [100.0]
    monkeypatch.setattr("strategy.institutional_strategy.time.time", lambda: clock[0])
    monkeypatch.setattr(
        "strategy.institutional_strategy._cfg",
        lambda name, default: {
            "DYNAMIC_EXIT_AUTOMATED_EARLY_LIQUIDATION_ENABLED": True,
            "DYNAMIC_EXIT_CLOCK_HORIZON_IS_REASSESSMENT_ONLY": True,
            "DYNAMIC_EXIT_MIN_CONSECUTIVE_CONFIRMATIONS": 3,
            "DYNAMIC_EXIT_CONFIRMATION_HALF_LIFE_FRACTION": 1.0,
        }.get(name, default),
    )
    strategy = InstitutionalStrategy(instrument=None)
    strategy._pos = PositionState(
        phase=PositionPhase.ACTIVE, side="short", quantity=0.00055,
        entry_price=73170.0, sl_price=73400.0, tp_price=72600.0,
        entry_time=90.0, exchange="hyperliquid", execution_symbol="BTC", asset_id="BTC",
        protection_confirmed=True,
        quant_components={"dynamic_protection_plan": {"signal_decay": {"optimal_hold_sec": 5.0, "half_life_sec": 0.2, "observation_interval_sec": 0.1}}},
    )
    strategy._dynamic_exit_live_state = lambda data: {
        "ready": True, "direction": "LONG", "opposed": True,
        "opposing_net_edge_bps": 5.0, "retained_net_edge_bps": -9.0,
        "mark_after_unwind_cost_bps": -2.0,
    }

    strategy._dynamic_exit_supervision(_Data(), _NoBrokerIoOnTick())
    assert strategy._pos.dynamic_exit_requested is False
    clock[0] += 0.11
    strategy._dynamic_exit_supervision(_Data(), _NoBrokerIoOnTick())
    assert strategy._pos.dynamic_exit_requested is False
    clock[0] += 0.11
    strategy._dynamic_exit_supervision(_Data(), _NoBrokerIoOnTick())

    assert strategy._pos.dynamic_exit_requested is True
    assert strategy._pos.dynamic_exit_reasons == ("confirmed_opposing_executable_alpha_after_horizon",)
    assert strategy._position_reconcile_wakeup.is_set()


class _SubmitManager:
    def __init__(self):
        self.calls = []

    def place_reconciled_reduce_only_exit(self, **kwargs):
        self.calls.append(kwargs)
        return {
            "order_id": "dynamic-close-1",
            "status": "SUBMITTED",
            "exit_lifecycle": "DYNAMIC_REDUCE_ONLY_CLOSE_PROTECTION_REMAINS_ARMED",
        }


def test_lifecycle_supervisor_submits_tracked_reduce_only_exit_and_retains_bracket():
    strategy = InstitutionalStrategy(instrument=None)
    strategy._pos = PositionState(
        phase=PositionPhase.ACTIVE,
        side="long",
        quantity=0.00055,
        entry_price=73170.0,
        sl_price=72950.0,
        tp_price=73700.0,
        sl_order_id="sl-live",
        tp_order_id="tp-live",
        exchange="hyperliquid",
        execution_symbol="BTC",
        asset_id="BTC",
        protection_confirmed=True,
        dynamic_exit_requested=True,
        dynamic_exit_reasons=("signal_alpha_cost_crossing_horizon_reached",),
    )
    manager = _SubmitManager()

    strategy._service_dynamic_exit_request(manager)

    assert len(manager.calls) == 1
    assert manager.calls[0]["expected_quantity"] == 0.00055
    assert strategy._pos.phase is PositionPhase.EXITING
    assert strategy._pos.dynamic_exit_order_id == "dynamic-close-1"
    assert strategy._pos.sl_order_id == "sl-live"
    assert strategy._pos.tp_order_id == "tp-live"


def test_dynamic_reduce_only_exit_closes_small_real_broker_position_instead_of_treating_it_flat():
    manager = object.__new__(OrderManager)
    submitted = {}
    manager.get_open_position = lambda: {"size": 0.00055, "side": "LONG"}

    def _place_market_order(*, side, quantity, reduce_only=False):
        submitted.update(side=side, quantity=quantity, reduce_only=reduce_only)
        return {"order_id": "dyn-order", "status": "FILLED"}

    manager.place_market_order = _place_market_order
    result = OrderManager.place_reconciled_reduce_only_exit(
        manager,
        reason="signal_alpha_cost_crossing_horizon_reached",
        expected_side="long",
        expected_quantity=0.00055,
    )

    assert result["order_id"] == "dyn-order"
    assert submitted == {"side": "SELL", "quantity": 0.00055, "reduce_only": True}
    assert result["exit_lifecycle"] == "DYNAMIC_REDUCE_ONLY_CLOSE_PROTECTION_REMAINS_ARMED"


class _ExitReconciliationManager:
    def __init__(self):
        self.dynamic_id = None
        self.swept = []

    def identify_exit_order(self, sl_order_id, tp_order_id, dynamic_exit_order_id=None):
        self.dynamic_id = dynamic_exit_order_id
        return {
            "confirmed": True,
            "exit_type": "dynamic_exit",
            "fill_price": 73105.25,
            "fee_paid": 0.0,
            "fee_exact": True,
        }

    def cancel_symbol_conditionals(self, symbol):
        self.swept.append(symbol)


class _RiskRecorder:
    def __init__(self):
        self.open_states = []
        self.trades = []

    def record_trade(self, **kwargs):
        self.trades.append(kwargs)

    def set_position_open(self, value):
        self.open_states.append(value)


def test_confirmed_dynamic_close_is_a_valid_exit_source_before_residual_protection_sweep(monkeypatch):
    monkeypatch.setattr(
        "strategy.institutional_strategy._cfg",
        lambda name, default: {
            "DYNAMIC_EXIT_CANCEL_RESIDUAL_PROTECTION_AFTER_CONFIRMED_FLAT": True,
            "COMMISSION_RATE": 0.0,
        }.get(name, default),
    )
    strategy = InstitutionalStrategy(instrument=None)
    strategy._pos = PositionState(
        phase=PositionPhase.EXITING,
        side="long",
        quantity=0.00055,
        entry_price=73170.0,
        sl_price=72950.0,
        tp_price=73700.0,
        sl_order_id="sl-live",
        tp_order_id="tp-live",
        dynamic_exit_order_id="dynamic-close-1",
        exchange="hyperliquid",
        execution_symbol="BTC",
        asset_id="BTC",
        entry_fee_exact=True,
        entry_leverage=25.0,
    )
    manager = _ExitReconciliationManager()
    risk = _RiskRecorder()

    assert strategy._finalise_confirmed_exit(manager, risk, 73105.25) is True
    assert manager.dynamic_id == "dynamic-close-1"
    assert manager.swept == ["BTC"]
    assert risk.open_states == [False]
    assert risk.trades[0]["reason"] == "dynamic_exit"


def test_common_quantity_ledger_routes_on_net_edge_not_broker_collateral_size(monkeypatch):
    monkeypatch.setattr(
        "execution.venue_selection._cfg",
        lambda name, default: {
            "VENUE_ROUND_TRIP_FEE_BPS": {"delta": 17.0, "hyperliquid": 7.0},
            "VENUE_SLIPPAGE_IMPACT_MULTIPLIER": 0.0,
            "VENUE_SELECTION_MIN_IMPROVEMENT_BPS": 0.0,
        }.get(name, default),
    )
    states = {
        "delta": _state("delta", "BTCUSD"),
        "hyperliquid": _state("hyperliquid", "BTC"),
    }
    selection = select_execution_venue(
        states=states,
        direction="LONG",
        asset_id="BTC",
        current_venue="delta",
        routeable_venues={"delta", "hyperliquid"},
        notional_usd=0.0,
        notional_by_venue={"delta": 40.0, "hyperliquid": 40.0},
        required_margin_by_venue={"delta": 2.0, "hyperliquid": 2.0},
        gross_edge_by_venue={"delta": 26.0, "hyperliquid": 26.0},
        available_cash_by_venue={"delta": 1000.0, "hyperliquid": 5.0},
        protection_capable_venues={"delta", "hyperliquid"},
        comparison_quantity=0.00055,
        comparison_notional_usd=40.0,
        snapshot_ts_ns=123456,
    )

    assert selection.selected_venue == "hyperliquid"
    assert selection.selection_mode == "RISK_NORMALISED_COMMON_QUANTITY"
    assert selection.reason == "highest_risk_normalised_expected_net_edge_route"
    assert selection.comparison_quantity == 0.00055
    assert selection.snapshot_ts_ns == 123456
    assert selection.estimates["hyperliquid"].expected_net_edge_bps > selection.estimates["delta"].expected_net_edge_bps

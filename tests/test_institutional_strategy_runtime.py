import time
from types import SimpleNamespace

from core.instruments import AssetClass, ExchangeInstrument, ExchangeName, TradableInstrument
from market_data.feed_health import score_feed_health
from market_data.normalizer import InstrumentMapping, build_venue_microstate
from strategy.domain import DecisionOutput
from strategy.institutional_strategy import InstitutionalStrategy, PositionPhase


class _Data:
    def __init__(self, prices, *, feed_ok=True, flow=2400.0):
        self.prices = list(prices)
        self.i = 0
        self.feed_ok = feed_ok
        self.flow = flow

    def get_last_price(self):
        px = self.prices[min(self.i, len(self.prices) - 1)]
        self.i += 1
        return px

    def _price(self):
        return self.prices[min(max(self.i - 1, 0), len(self.prices) - 1)]

    def get_orderbook(self):
        px = self._price()
        return {
            "bids": [(px - 0.005, 2000.0), (px - 0.02, 3000.0)],
            "asks": [(px + 0.005, 2000.0), (px + 0.02, 3000.0)],
        }

    def get_feed_reliability(self):
        return {
            "connected": self.feed_ok,
            "heartbeat_ok": self.feed_ok,
            "sequence_valid": self.feed_ok,
            "snapshot_ready": self.feed_ok,
            "exchange_timestamp_available": True,
            "latency_vs_baseline_z": 0.0,
        }

    def get_venue_microstates(self):
        px = self._price()
        health = score_feed_health(
            connected=self.feed_ok,
            heartbeat_ok=self.feed_ok,
            sequence_valid=self.feed_ok,
            snapshot_ready=self.feed_ok,
            exchange_timestamp_available=True,
            latency_vs_baseline_z=0.0,
        )
        ts = time.time_ns()
        delta = build_venue_microstate(
            mapping=InstrumentMapping(
                venue="delta", venue_symbol="BTCUSD", canonical_underlying="BTC", product_class="inverse_perp",
                quote_currency="USD", contract_multiplier=1.0, settlement_currency="BTC", price_tick=0.005,
                qty_step=1.0, execution_enabled=True, notional_model="inverse_usd_contract",
            ),
            bids=[(px - 0.005, 2000.0), (px - 0.02, 3000.0)],
            asks=[(px + 0.005, 2000.0), (px + 0.02, 3000.0)],
            feed_health=health, receive_ts_ns=ts, exchange_ts_ns=ts - 1_000_000,
            ofi_usd_1s=self.flow, ofi_usd_10s=self.flow * 0.5, tfi_usd_1s=self.flow * 0.25,
        )
        reference = build_venue_microstate(
            mapping=InstrumentMapping(
                venue="coinswitch", venue_symbol="BTCUSDT", canonical_underlying="BTC", product_class="perp",
                quote_currency="USD", contract_multiplier=1.0, settlement_currency="USDT", price_tick=0.005,
                qty_step=0.001, execution_enabled=False, notional_model="linear",
            ),
            bids=[(px - 0.005, 20.0), (px - 0.02, 20.0)],
            asks=[(px + 0.005, 20.0), (px + 0.02, 20.0)],
            feed_health=health, receive_ts_ns=ts, exchange_ts_ns=ts - 1_000_000,
            ofi_usd_1s=self.flow, ofi_usd_10s=self.flow * 0.5, tfi_usd_1s=self.flow * 0.25,
        )
        return {"delta": delta, "coinswitch": reference}


class _Risk:
    def get_available_balance(self):
        return {"available": 100000.0}


class _Orders:
    active_exchange = "delta"
    symbol = "BTCUSD"
    display_symbol = "BTCUSD"

    def __init__(self):
        self.placed = []

    def place_bracket_limit_entry(self, *args, **kwargs):
        self.placed.append((args, kwargs))
        side, qty, entry, stop, target = args[:5]
        return {
            "order_id": "entry-1", "quantity": qty, "fill_price": entry,
            "bracket_sl_order_id": "sl-1", "bracket_tp_order_id": "tp-1",
            "bracket_child_verified": True, "protection_model": "VENUE_NATIVE_BRACKET",
        }

    def get_open_position(self):
        return {"size": 1.0}

    def get_balance(self):
        return {"available": 100000.0}


def _instrument():
    ei = ExchangeInstrument(
        exchange=ExchangeName.DELTA, symbol="BTCUSD", ws_symbol="BTCUSD", display_symbol="BTCUSD", asset_id="BTC",
        asset_class=AssetClass.CRYPTO, quote_asset="USD", status="active", tick_size=0.5, lot_step=1.0,
        min_qty=1.0, raw={"contract_type": "inverse_perp", "contract_multiplier": 1.0},
    )
    return TradableInstrument("BTC", "Bitcoin", AssetClass.CRYPTO, ExchangeName.DELTA, {ExchangeName.DELTA: ei})


def _silver_instrument():
    delta = ExchangeInstrument(
        exchange=ExchangeName.DELTA, symbol="SLVONUSD", ws_symbol="SLVONUSD", display_symbol="SLVONUSD",
        asset_id="SILVER", asset_class=AssetClass.COMMODITY, quote_asset="USD", base_asset="SLVON",
        status="active", tick_size=0.01, lot_step=0.01, min_qty=0.01,
        raw={"contract_type": "linear_perp", "contract_value": 1.0, "tick_size": 0.01},
    )
    hl = ExchangeInstrument(
        exchange=ExchangeName.HYPERLIQUID, symbol="xyz:SILVER", ws_symbol="xyz:SILVER", display_symbol="xyz:SILVER",
        asset_id="SILVER", asset_class=AssetClass.COMMODITY, quote_asset="USD", base_asset="SILVER",
        status="active", tick_size=0.01, lot_step=0.01, min_qty=0.01, max_leverage=25.0,
        raw={"contract_type": "linear_perp", "tick_size": 0.01, "qty_step": 0.01},
    )
    return TradableInstrument("SILVER", "Silver token derivatives", AssetClass.COMMODITY, ExchangeName.DELTA, {ExchangeName.DELTA: delta, ExchangeName.HYPERLIQUID: hl})


class _SilverRouteData:
    def get_last_price(self):
        return 30.0

    def get_feed_reliability(self):
        return {
            "connected": True,
            "heartbeat_ok": True,
            "sequence_valid": True,
            "snapshot_ready": True,
            "exchange_timestamp_available": True,
            "latency_vs_baseline_z": 0.0,
        }

    def get_orderbook(self):
        return {"bids": [(29.995, 10.0)], "asks": [(30.005, 10.0)]}

    def get_venue_microstates(self):
        health = score_feed_health(
            connected=True,
            heartbeat_ok=True,
            sequence_valid=True,
            snapshot_ready=True,
            exchange_timestamp_available=True,
            latency_vs_baseline_z=0.0,
        )
        ts = time.time_ns()
        mapping_delta = InstrumentMapping(
            venue="delta", venue_symbol="SLVONUSD", canonical_underlying="SILVER", product_class="linear_perp",
            quote_currency="USD", contract_multiplier=1.0, settlement_currency="USD", price_tick=0.01,
            qty_step=0.01, execution_enabled=True, notional_model="linear",
        )
        mapping_hl = InstrumentMapping(
            venue="hyperliquid", venue_symbol="xyz:SILVER", canonical_underlying="SILVER", product_class="linear_perp",
            quote_currency="USD", contract_multiplier=1.0, settlement_currency="USDC", price_tick=0.01,
            qty_step=0.01, execution_enabled=True, notional_model="linear",
        )
        delta = build_venue_microstate(
            mapping=mapping_delta,
            bids=[(29.995, 25.0), (29.985, 25.0)],
            asks=[(30.005, 25.0), (30.015, 25.0)],
            feed_health=health,
            receive_ts_ns=ts,
            exchange_ts_ns=ts - 1_000_000,
            ofi_usd_1s=3000.0,
            ofi_usd_10s=1500.0,
            tfi_usd_1s=500.0,
        )
        hyperliquid = build_venue_microstate(
            mapping=mapping_hl,
            bids=[(29.995, 6000.0), (29.985, 6000.0)],
            asks=[(30.005, 6000.0), (30.015, 6000.0)],
            feed_health=health,
            receive_ts_ns=ts,
            exchange_ts_ns=ts - 1_000_000,
            ofi_usd_1s=-300000.0,
            ofi_usd_10s=-150000.0,
            tfi_usd_1s=-50000.0,
        )
        return {"delta": delta, "hyperliquid": hyperliquid}


class _MultiVenueOrders(_Orders):
    def available_exchanges(self):
        return {"delta", "hyperliquid"}

    def manager_for(self, venue):
        return self


def test_invalid_feed_produces_explicit_execution_unsafe_rejection(tmp_path, monkeypatch):
    monkeypatch.setattr("strategy.institutional_strategy._cfg", lambda name, default: str(tmp_path) if name == "RESEARCH_STORE_PATH" else default)
    strategy = InstitutionalStrategy(instrument=_instrument())
    decision = strategy.evaluate(_Data([100.0] * 40, feed_ok=False), _Orders(), _Risk(), 1)
    assert decision.decision is DecisionOutput.NO_TRADE_EXECUTION_UNSAFE
    assert decision.reasons == ["disconnected"]


def test_shadow_mode_blocks_live_order_even_when_flow_edge_is_positive(tmp_path, monkeypatch):
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_SIGNAL_DECAY_READY", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_FOR_DELTA", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_FOR_DELTA", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_VENUES", (), raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_VENUES", (), raising=False)
    def cfg(name, default):
        values = {"RESEARCH_STORE_PATH": str(tmp_path), "INSTITUTIONAL_ENABLE_LIVE_ENTRIES": False,
                  "INSTITUTIONAL_MIN_NET_EDGE_BPS": 0.1, "INSTITUTIONAL_MIN_SIGNAL_BPS": 0.01,
                      "VENUE_SELECTION_ENABLED": False}
        return values.get(name, default)
    monkeypatch.setattr("strategy.institutional_strategy._cfg", cfg)
    strategy = InstitutionalStrategy(instrument=_instrument())
    data = _Data([100.0 + i * 0.02 for i in range(80)], feed_ok=True)
    orders = _Orders()
    for i in range(80):
        strategy.on_tick(data, orders, _Risk(), i)
    assert strategy._last_decision is not None
    assert strategy._last_decision.decision is DecisionOutput.SHADOW_SIGNAL_VALIDATED
    assert "shadow_mode_live_entries_disabled" in strategy._last_decision.reasons
    assert orders.placed == []


def test_price_walk_without_venue_local_structural_state_is_not_a_trade_signal(tmp_path, monkeypatch):
    def cfg(name, default):
        values = {"RESEARCH_STORE_PATH": str(tmp_path), "INSTITUTIONAL_ENABLE_LIVE_ENTRIES": True,
                  "INSTITUTIONAL_MIN_NET_EDGE_BPS": 0.0, "INSTITUTIONAL_MIN_SIGNAL_BPS": 0.50}
        return values.get(name, default)
    monkeypatch.setattr("strategy.institutional_strategy._cfg", cfg)
    strategy = InstitutionalStrategy(instrument=_instrument())
    decision = None
    data = _Data([100.0 + i * 0.25 for i in range(80)], flow=0.0)
    for i in range(80):
        decision = strategy.evaluate(data, _Orders(), _Risk(), i)
    assert decision is not None
    assert decision.direction.value == "NO_TRADE"
    assert any(reason.startswith(("market_state_and_flow_flat", "venue_flow_disagreement")) for reason in decision.reasons)


def test_live_entry_requires_protection_confirmation(tmp_path, monkeypatch):
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_SIGNAL_DECAY_READY", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_FOR_DELTA", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_FOR_DELTA", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_VENUES", (), raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_VENUES", (), raising=False)
    def cfg(name, default):
        values = {"RESEARCH_STORE_PATH": str(tmp_path), "INSTITUTIONAL_ENABLE_LIVE_ENTRIES": True,
                  "INSTITUTIONAL_MIN_NET_EDGE_BPS": 0.1, "INSTITUTIONAL_MIN_SIGNAL_BPS": 0.01,
                  "LEVERAGE": 2.0, "VENUE_SELECTION_ENABLED": False}
        return values.get(name, default)
    monkeypatch.setattr("strategy.institutional_strategy._cfg", cfg)
    strategy = InstitutionalStrategy(instrument=_instrument())
    data = _Data([100.0 + i * 0.02 for i in range(80)], feed_ok=True)
    orders = _Orders()
    for i in range(80):
        strategy.on_tick(data, orders, _Risk(), i)
        if strategy.get_position():
            break
    pos = strategy.get_position()
    assert pos is not None
    assert pos["phase"] == PositionPhase.ACTIVE.value
    assert pos["protection_confirmed"] is True
    assert orders.placed


def test_live_entry_is_blocked_by_risk_manager_trade_gate(tmp_path, monkeypatch):
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_SIGNAL_DECAY_READY", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_FOR_DELTA", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_FOR_DELTA", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_VENUES", (), raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_VENUES", (), raising=False)

    def cfg(name, default):
        values = {
            "RESEARCH_STORE_PATH": str(tmp_path),
            "INSTITUTIONAL_ENABLE_LIVE_ENTRIES": True,
            "INSTITUTIONAL_MIN_NET_EDGE_BPS": 0.1,
            "INSTITUTIONAL_MIN_SIGNAL_BPS": 0.01,
            "LEVERAGE": 2.0,
            "VENUE_SELECTION_ENABLED": False,
        }
        return values.get(name, default)

    class _BlockedRisk(_Risk):
        def can_trade(self):
            return False, "Cooldown: 120s remaining"

    monkeypatch.setattr("strategy.institutional_strategy._cfg", cfg)
    strategy = InstitutionalStrategy(instrument=_instrument())
    data = _Data([100.0 + i * 0.02 for i in range(80)], feed_ok=True)
    orders = _Orders()
    decision = strategy.evaluate(data, orders, _BlockedRisk(), 1)
    assert decision.decision is DecisionOutput.NO_TRADE_RISK_BUDGET
    assert decision.reasons == ["risk_manager_gate:Cooldown: 120s remaining"]
    assert orders.placed == []


def test_unvalidated_opposite_venue_cannot_receive_silver_execution_route(tmp_path, monkeypatch):
    monkeypatch.setattr("strategy.institutional_strategy._cfg", lambda name, default: {
        "RESEARCH_STORE_PATH": str(tmp_path),
        "INSTITUTIONAL_ENABLE_LIVE_ENTRIES": False,
        "INSTITUTIONAL_MIN_SIGNAL_BPS": 0.01,
        "INSTITUTIONAL_MIN_NET_EDGE_BPS": 0.0,
    }.get(name, default))
    monkeypatch.setattr("execution.venue_selection._cfg", lambda name, default: {
        "VENUE_FEE_BPS": {"delta": 1.5, "hyperliquid": 4.5},
        "SILVER_HYPERLIQUID_PREFERENCE_BPS": 8.0,
        "SILVER_DELTA_ILLIQUIDITY_PENALTY_BPS": 15.0,
        "SILVER_DELTA_MIN_NEAR_DEPTH_USD": 50000.0,
        "VENUE_SELECTION_MIN_IMPROVEMENT_BPS": 0.0,
    }.get(name, default))
    strategy = InstitutionalStrategy(instrument=_silver_instrument())
    decision = strategy.evaluate(_SilverRouteData(), _MultiVenueOrders(), _Risk(), 1)
    assert decision.decision in {DecisionOutput.NO_TRADE_INSUFFICIENT_EDGE, DecisionOutput.NO_TRADE_EXECUTION_UNSAFE}
    estimates = decision.model_values.get("venue_selection", {}).get("estimates", {})
    assert "hyperliquid" in estimates
    assert estimates["hyperliquid"]["routeable"] is False
    assert decision.model_values.get("selected_execution_venue") != "hyperliquid"


def test_groww_direction_is_no_trade_until_options_volatility_context_is_available(tmp_path, monkeypatch):
    monkeypatch.setattr("strategy.institutional_strategy._cfg", lambda name, default: str(tmp_path) if name == "RESEARCH_STORE_PATH" else default)
    ei = ExchangeInstrument(
        exchange=ExchangeName.GROWW, symbol="NIFTY", ws_symbol="NIFTY", display_symbol="NIFTY", asset_id="NIFTY",
        asset_class=AssetClass.OPTION, quote_asset="INR", status="active", tick_size=0.05, lot_step=50.0, min_qty=50.0,
        raw={"stock_code": "NIFTY", "right": "call", "strike_price": 25000, "expiry_date": "2026-06-25"},
    )
    inst = TradableInstrument("NIFTY", "NIFTY options", AssetClass.OPTION, ExchangeName.GROWW, {ExchangeName.GROWW: ei})
    strategy = InstitutionalStrategy(instrument=inst)
    orders = SimpleNamespace(active_exchange="groww", symbol="NIFTY", display_symbol="NIFTY")
    decision = strategy.evaluate(_Data([24000.0] * 10, feed_ok=True), orders, _Risk(), 1)
    assert decision.desk == "DESK_B_NIFTY_OPTIONS"
    assert decision.direction.value == "NO_TRADE"
    assert "option_volatility_context_interface_missing" in decision.reasons



def test_groww_position_sizing_uses_verified_selected_contract_lot(tmp_path, monkeypatch):
    from strategy.domain import Direction, ProtectionPlan
    def cfg(name, default):
        values = {
            "RESEARCH_STORE_PATH": str(tmp_path),
            "INSTITUTIONAL_RISK_FRACTION_PER_TRADE": 1.0,
            "INSTITUTIONAL_QUARTER_KELLY": 1.0,
            "INSTITUTIONAL_TARGET_OBSERVATION_VOL_BPS": 100000.0,
        }
        return values.get(name, default)
    monkeypatch.setattr("strategy.institutional_strategy._cfg", cfg)
    raw = {
        "stock_code": "NIFTY", "selected_option_contract": {
            "raw": {"runtime_lot_size": 65}
        }
    }
    ei = ExchangeInstrument(
        exchange=ExchangeName.GROWW, symbol="NIFTY", ws_symbol="NIFTY", display_symbol="NIFTY", asset_id="NIFTY",
        asset_class=AssetClass.OPTION, quote_asset="INR", status="active", tick_size=0.05, lot_step=65.0, min_qty=65.0,
        raw=raw,
    )
    inst = TradableInstrument("NIFTY", "NIFTY options", AssetClass.OPTION, ExchangeName.GROWW, {ExchangeName.GROWW: ei})
    strategy = InstitutionalStrategy(instrument=inst)
    decision = strategy._size_position(
        "DESK_B_NIFTY_OPTIONS", "NIFTY", Direction.BULLISH, 100.0, 5000.0, 1.0,
        ProtectionPlan(100.0, 90.0, 120.0, "GROWW_OCO_AFTER_FILL", True),
        SimpleNamespace(get_available_balance=lambda: {"available": 1000000.0}),
    )
    assert decision.quantity > 0
    assert decision.quantity % 65 == 0


def test_groww_position_sizing_rejects_without_verified_lot(tmp_path, monkeypatch):
    from strategy.domain import Direction, ProtectionPlan
    monkeypatch.setattr("strategy.institutional_strategy._cfg", lambda name, default: str(tmp_path) if name == "RESEARCH_STORE_PATH" else default)
    ei = ExchangeInstrument(
        exchange=ExchangeName.GROWW, symbol="NIFTY", ws_symbol="NIFTY", display_symbol="NIFTY", asset_id="NIFTY",
        asset_class=AssetClass.OPTION, quote_asset="INR", status="active", tick_size=0.05, lot_step=1.0, min_qty=1.0,
        raw={"stock_code": "NIFTY"},
    )
    inst = TradableInstrument("NIFTY", "NIFTY options", AssetClass.OPTION, ExchangeName.GROWW, {ExchangeName.GROWW: ei})
    strategy = InstitutionalStrategy(instrument=inst)
    decision = strategy._size_position(
        "DESK_B_NIFTY_OPTIONS", "NIFTY", Direction.BULLISH, 100.0, 100.0, 1.0,
        ProtectionPlan(100.0, 90.0, 120.0, "GROWW_OCO_AFTER_FILL", True), _Risk(),
    )
    assert decision.approved is False
    assert decision.reasons == ["verified_nfo_lot_size_unavailable"]


def test_delta_contract_value_sizes_exposure_units_not_raw_contracts(tmp_path, monkeypatch):
    from execution.order_manager import _DeltaAdapter
    from strategy.domain import Direction, ProtectionPlan

    def cfg(name, default):
        values = {
            "RESEARCH_STORE_PATH": str(tmp_path),
            "INSTITUTIONAL_RISK_FRACTION_PER_TRADE": 1.0,
            "INSTITUTIONAL_QUARTER_KELLY": 1.0,
            "INSTITUTIONAL_TARGET_OBSERVATION_VOL_BPS": 100000.0,
            "LEVERAGE": 5.0,
        }
        return values.get(name, default)

    monkeypatch.setattr("strategy.institutional_strategy._cfg", cfg)
    raw = {"contract_type": "perpetual_futures", "contract_value": 0.001, "tick_size": 0.01}
    ei = ExchangeInstrument(
        exchange=ExchangeName.DELTA,
        symbol="PAXGUSD",
        ws_symbol="PAXGUSD",
        display_symbol="PAXGUSD",
        asset_id="GOLD",
        asset_class=AssetClass.COMMODITY,
        quote_asset="USD",
        base_asset="PAXG",
        status="active",
        tick_size=0.01,
        lot_step=0.001,
        min_qty=0.001,
        contract_value_btc=0.001,
        raw=raw,
    )
    inst = TradableInstrument("GOLD", "Gold token derivatives", AssetClass.COMMODITY, ExchangeName.DELTA, {ExchangeName.DELTA: ei})
    strategy = InstitutionalStrategy(instrument=inst)
    decision = strategy._size_position(
        "DESK_A_METALS",
        "PAXGUSD",
        Direction.SHORT,
        4484.55,
        100.0,
        0.90,
        ProtectionPlan(4484.55, 4491.007752, 4467.418645719423, "VENUE_NATIVE_BRACKET", True),
        SimpleNamespace(get_available_balance=lambda: {"available": 100.0}),
    )

    assert decision.approved is True
    assert decision.quantity == 0.004
    assert abs(decision.notional - decision.quantity * 4484.55) < 1e-9
    assert _DeltaAdapter(None, exchange_instrument=ei)._qty_to_contracts(decision.quantity) == 4



class _GrowwReadyData:
    def __init__(self, instrument):
        self.instrument = instrument
        self.active = False
        self.spot = 24100.0
    def get_analysis_price(self):
        return self.spot
    def get_last_price(self):
        return 100.0 if self.active else self.spot
    def get_feed_reliability(self):
        return {"connected": True, "heartbeat_ok": True, "sequence_valid": True, "snapshot_ready": True, "exchange_timestamp_available": True, "latency_vs_baseline_z": 0.0}
    def get_groww_option_volatility_context(self):
        return {"ready_for_long_premium_decision": True, "vrp": -0.05, "live_iv_coverage": 1.0, "reasons": []}
    def get_candles(self, timeframe, limit=100):
        if timeframe == "5m":
            rows = [{"open": 23990.0, "high": 24002.0, "low": 23988.0, "close": 23995.0} for _ in range(25)]
            rows[-1] = {"open": 24000.0, "high": 24102.0, "low": 23999.0, "close": 24100.0}
            return rows
        if timeframe == "15m":
            return [{"open": 23940.0, "high": 23960.0, "low": 23930.0, "close": 23950.0}] * 3 + [{"open": 23950.0, "high": 24102.0, "low": 23945.0, "close": 24100.0}]
        return []
    def activate_groww_execution_vehicle(self, thesis_side, available_funds):
        assert thesis_side == "long"
        self.active = True
        self.instrument.primary.raw["selected_option_contract"] = {"raw": {"runtime_lot_size": 65}}
        return SimpleNamespace(selected_symbol="NIFTY2660224100CE", delta=0.45)
    def get_execution_feed_status(self):
        return {"active_vehicle_ready": self.active, "status": "ACTIVE_OPTION_VEHICLE_LIVE"}
    def get_orderbook(self):
        return {"bids": [[99.99, 650]], "asks": [[100.01, 650]]}
    def get_execution_candles(self, timeframe, limit=40):
        return [{"open": 99.0, "high": 101.0, "low": 98.5, "close": 100.0} for _ in range(20)]


class _GrowwOrders(_Orders):
    active_exchange = "groww"
    symbol = "NIFTY"
    display_symbol = "NIFTY"
    def place_bracket_limit_entry(self, *args, **kwargs):
        self.placed.append((args, kwargs))
        side, qty, entry, stop, target = args[:5]
        return {"order_id": "groww-entry", "quantity": qty, "fill_price": entry, "protection_confirmed": True, "protection_model": "GROWW_OCO_AFTER_FILL"}


def _groww_instrument_for_strategy():
    raw = {"stock_code": "NIFTY"}
    ei = ExchangeInstrument(
        exchange=ExchangeName.GROWW, symbol="NIFTY", ws_symbol="NIFTY", display_symbol="NIFTY", asset_id="NIFTY",
        asset_class=AssetClass.OPTION, quote_asset="INR", status="active", tick_size=0.05, lot_step=65.0, min_qty=65.0, raw=raw,
    )
    return TradableInstrument("NIFTY", "NIFTY options", AssetClass.OPTION, ExchangeName.GROWW, {ExchangeName.GROWW: ei})


def test_groww_ready_context_activates_ce_and_reaches_shadow_decision(tmp_path, monkeypatch):
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_SIGNAL_DECAY_READY", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_FOR_DELTA", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_FOR_DELTA", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_VENUES", (), raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_VENUES", (), raising=False)
    def cfg(name, default):
        values = {"RESEARCH_STORE_PATH": str(tmp_path), "INSTITUTIONAL_ENABLE_LIVE_ENTRIES": False, "INSTITUTIONAL_MIN_NET_EDGE_BPS": 0.0,
                  "INSTITUTIONAL_RISK_FRACTION_PER_TRADE": 1.0, "INSTITUTIONAL_QUARTER_KELLY": 1.0, "INSTITUTIONAL_TARGET_OBSERVATION_VOL_BPS": 100000.0}
        return values.get(name, default)
    monkeypatch.setattr("strategy.institutional_strategy._cfg", cfg)
    inst = _groww_instrument_for_strategy()
    strategy = InstitutionalStrategy(instrument=inst)
    data = _GrowwReadyData(inst)
    decision = strategy.evaluate(data, _GrowwOrders(), SimpleNamespace(get_available_balance=lambda: {"available": 1000000.0}), 1)
    assert data.active is True
    assert decision.direction.value == "BULLISH"
    assert "shadow_mode_live_entries_disabled" in decision.reasons
    assert decision.sizing is not None and decision.sizing.quantity % 65 == 0


def test_groww_live_decision_places_buy_with_oco_only_after_all_gates(tmp_path, monkeypatch):
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_SIGNAL_DECAY_READY", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_FOR_DELTA", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_FOR_DELTA", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_VENUES", (), raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_VENUES", (), raising=False)
    def cfg(name, default):
        values = {"RESEARCH_STORE_PATH": str(tmp_path), "INSTITUTIONAL_ENABLE_LIVE_ENTRIES": True, "INSTITUTIONAL_MIN_NET_EDGE_BPS": 0.0,
                  "INSTITUTIONAL_RISK_FRACTION_PER_TRADE": 1.0, "INSTITUTIONAL_QUARTER_KELLY": 1.0, "INSTITUTIONAL_TARGET_OBSERVATION_VOL_BPS": 100000.0}
        return values.get(name, default)
    monkeypatch.setattr("strategy.institutional_strategy._cfg", cfg)
    inst = _groww_instrument_for_strategy()
    strategy = InstitutionalStrategy(instrument=inst)
    data = _GrowwReadyData(inst)
    orders = _GrowwOrders()
    strategy.on_tick(data, orders, SimpleNamespace(get_available_balance=lambda: {"available": 1000000.0}), 1)
    assert orders.placed
    assert orders.placed[0][0][0] == "BUY"
    assert strategy.get_position()["protection_model"] == "GROWW_OCO_AFTER_FILL"


def test_groww_option_edge_deducts_hold_horizon_theta_carry(tmp_path, monkeypatch):
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_SIGNAL_DECAY_READY", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_FOR_DELTA", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_FOR_DELTA", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_VENUES", (), raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_VENUES", (), raising=False)
    def cfg(name, default):
        values = {
            "RESEARCH_STORE_PATH": str(tmp_path), "INSTITUTIONAL_ENABLE_LIVE_ENTRIES": False,
            "INSTITUTIONAL_MIN_NET_EDGE_BPS": 0.0, "INSTITUTIONAL_RISK_FRACTION_PER_TRADE": 1.0,
            "INSTITUTIONAL_QUARTER_KELLY": 1.0, "INSTITUTIONAL_TARGET_OBSERVATION_VOL_BPS": 100000.0,
            "POLICY_OPTION_MAX_HOLD_SEC": 2700.0,
        }
        return values.get(name, default)
    monkeypatch.setattr("strategy.institutional_strategy._cfg", cfg)
    inst = _groww_instrument_for_strategy()
    strategy = InstitutionalStrategy(instrument=inst)
    data = _GrowwReadyData(inst)
    original = data.activate_groww_execution_vehicle
    def activate(thesis_side, available_funds):
        choice = original(thesis_side, available_funds)
        choice.theta_to_premium = 0.10
        return choice
    data.activate_groww_execution_vehicle = activate
    decision = strategy.evaluate(data, _GrowwOrders(), SimpleNamespace(get_available_balance=lambda: {"available": 1000000.0}), 1)
    expected_theta = 0.10 * (2700.0 / 86400.0) * 10000.0
    assert abs(decision.model_values["theta_carry_bps_expected_hold"] - expected_theta) < 1e-9
    assert decision.model_values["net_edge_formula"] == "premium_delta_edge_bps - total_cost_bps - theta_carry_bps_expected_hold"


def test_decision_telemetry_emits_compact_transition_not_every_tick(tmp_path, monkeypatch, caplog):
    def cfg(name, default):
        values = {
            "RESEARCH_STORE_PATH": str(tmp_path),
            "INSTITUTIONAL_DECISION_TELEMETRY_ENABLED": True,
            "INSTITUTIONAL_DECISION_TELEMETRY_HEARTBEAT_SEC": 30.0,
            "INSTITUTIONAL_DECISION_TELEMETRY_FULL_ON_TRANSITION": False,
            "INSTITUTIONAL_DECISION_TELEMETRY_DEBUG_EVERY_TICK": False,
        }
        return values.get(name, default)
    monkeypatch.setattr("strategy.institutional_strategy._cfg", cfg)
    inst = _groww_instrument_for_strategy()
    strategy = InstitutionalStrategy(instrument=inst)
    from strategy.domain import DecisionOutput, Direction, Regime
    decision = strategy._decision(
        desk="DESK_B_NIFTY_OPTIONS", venue="groww", instrument="NIFTY",
        decision=DecisionOutput.NO_TRADE_INSUFFICIENT_EDGE, direction=Direction.NO_TRADE,
        regime=Regime.BALANCE, expected_net_edge_bps=0.0, uncertainty_bps=1.0,
        liquidity_score=0.0, execution_quality_score=1.0, sizing=None, protection_plan=None,
        reasons=["audit"], model_values={"calculation": 42.0}, research_features={},
    )
    import logging
    with caplog.at_level(logging.INFO):
        strategy._log_decision_calculation(decision)
        strategy._log_decision_calculation(decision)
    assert caplog.text.count("DECISION_TRANSITION") == 1
    assert "DECISION_DETAIL" not in caplog.text
    assert "NOT_EVALUATED_UNTIL_DIRECTION_ACTIVATES_CE_OR_PE" in caplog.text
    assert '"liquidity_score":null' in caplog.text


def test_decision_telemetry_can_emit_full_detail_on_transition(tmp_path, monkeypatch, caplog):
    def cfg(name, default):
        values = {
            "RESEARCH_STORE_PATH": str(tmp_path),
            "INSTITUTIONAL_DECISION_TELEMETRY_ENABLED": True,
            "INSTITUTIONAL_DECISION_TELEMETRY_FULL_ON_TRANSITION": True,
        }
        return values.get(name, default)
    monkeypatch.setattr("strategy.institutional_strategy._cfg", cfg)
    inst = _groww_instrument_for_strategy()
    strategy = InstitutionalStrategy(instrument=inst)
    from strategy.domain import DecisionOutput, Direction, Regime
    decision = strategy._decision(
        desk="DESK_B_NIFTY_OPTIONS", venue="groww", instrument="NIFTY",
        decision=DecisionOutput.NO_TRADE_INSUFFICIENT_EDGE, direction=Direction.NO_TRADE,
        regime=Regime.BALANCE, expected_net_edge_bps=0.0, uncertainty_bps=1.0,
        liquidity_score=0.0, execution_quality_score=1.0, sizing=None, protection_plan=None,
        reasons=["audit"], model_values={"calculation": 42.0}, research_features={},
    )
    import logging
    with caplog.at_level(logging.INFO):
        strategy._log_decision_calculation(decision)
    assert "DECISION_DETAIL" in caplog.text
    assert '"calculation":42.0' in caplog.text


def test_telemetry_does_not_log_unqualified_microstructure_flips_as_transitions(tmp_path, monkeypatch, caplog):
    def cfg(name, default):
        values = {
            "RESEARCH_STORE_PATH": str(tmp_path),
            "INSTITUTIONAL_DECISION_TELEMETRY_ENABLED": True,
            "INSTITUTIONAL_DECISION_TELEMETRY_HEARTBEAT_SEC": 30.0,
            "INSTITUTIONAL_DECISION_TELEMETRY_LOG_UNQUALIFIED_SIGNAL_FLIPS": False,
        }
        return values.get(name, default)
    monkeypatch.setattr("strategy.institutional_strategy._cfg", cfg)
    inst = _groww_instrument_for_strategy()
    strategy = InstitutionalStrategy(instrument=inst)
    from strategy.domain import DecisionOutput, Direction, Regime
    def decision(direction, source):
        return strategy._decision(
            desk="DESK_A_METALS", venue="delta", instrument="PAXGUSD",
            decision=DecisionOutput.NO_TRADE_INSUFFICIENT_EDGE, direction=direction,
            regime=Regime.BALANCE, expected_net_edge_bps=-1.0, uncertainty_bps=3.0,
            liquidity_score=0.8, execution_quality_score=1.0, sizing=None, protection_plan=None,
            reasons=[source, "net_edge_does_not_clear_uncertainty_and_minimum"],
            model_values={"signal_source": source, "costs_bps": 2.5}, research_features={},
        )
    import logging
    with caplog.at_level(logging.INFO):
        strategy._log_decision_calculation(decision(Direction.LONG, "ofi_tfi_microprice_long"))
        strategy._log_decision_calculation(decision(Direction.SHORT, "ofi_tfi_microprice_short"))
    assert caplog.text.count("DECISION_TRANSITION") == 1


def test_nonactionable_blocker_does_not_emit_transition_only_for_regime_flip(tmp_path, monkeypatch, caplog):
    def cfg(name, default):
        values = {
            "RESEARCH_STORE_PATH": str(tmp_path),
            "INSTITUTIONAL_DECISION_TELEMETRY_ENABLED": True,
            "INSTITUTIONAL_DECISION_TELEMETRY_HEARTBEAT_SEC": 30.0,
        }
        return values.get(name, default)
    monkeypatch.setattr("strategy.institutional_strategy._cfg", cfg)
    strategy = InstitutionalStrategy(instrument=_groww_instrument_for_strategy())
    from strategy.domain import DecisionOutput, Direction, Regime
    def blocked(regime):
        return strategy._decision(
            desk="DESK_A_METALS", venue="delta", instrument="SLVONUSD",
            decision=DecisionOutput.NO_TRADE_INSUFFICIENT_EDGE, direction=Direction.NO_TRADE,
            regime=regime, expected_net_edge_bps=-20.0, uncertainty_bps=7.0,
            liquidity_score=0.2, execution_quality_score=1.0, sizing=None, protection_plan=None,
            reasons=["near_touch_depth_unavailable", "net_edge_does_not_clear_uncertainty_and_minimum"],
            model_values={"signal_source": "near_touch_depth_unavailable", "costs_bps": 22.0},
            research_features={},
        )
    import logging
    with caplog.at_level(logging.INFO):
        strategy._log_decision_calculation(blocked(Regime.BALANCE))
        strategy._log_decision_calculation(blocked(Regime.TREND))
    assert caplog.text.count("DECISION_TRANSITION") == 1


def test_shadow_validated_telemetry_is_not_labelled_insufficient_edge(tmp_path, monkeypatch, caplog):
    monkeypatch.setattr("strategy.institutional_strategy._cfg", lambda name, default: {
        "RESEARCH_STORE_PATH": str(tmp_path),
        "INSTITUTIONAL_DECISION_TELEMETRY_ENABLED": True,
    }.get(name, default))
    strategy = InstitutionalStrategy(instrument=_groww_instrument_for_strategy())
    from strategy.domain import DecisionOutput, Direction, Regime
    decision = strategy._decision(
        desk="DESK_A_METALS", venue="delta", instrument="PAXGUSD",
        decision=DecisionOutput.SHADOW_SIGNAL_VALIDATED, direction=Direction.LONG,
        regime=Regime.BALANCE, expected_net_edge_bps=4.8, uncertainty_bps=3.0,
        liquidity_score=0.9, execution_quality_score=1.0, sizing=None, protection_plan=None,
        reasons=["shadow_mode_live_entries_disabled"], model_values={"costs_bps": 2.7}, research_features={},
    )
    import logging
    with caplog.at_level(logging.INFO):
        strategy._log_decision_calculation(decision)
    assert "DECISION_SHADOW_VALIDATED" in caplog.text
    assert "SHADOW_SIGNAL_VALIDATED" in caplog.text


def test_microstructure_telemetry_includes_weighted_edge_components(tmp_path, monkeypatch):
    monkeypatch.setattr("strategy.institutional_strategy._cfg", lambda name, default: {
        "RESEARCH_STORE_PATH": str(tmp_path),
        "INSTITUTIONAL_REQUIRE_BTC_CROSS_VENUE": False,
        "INSTITUTIONAL_MIN_SIGNAL_BPS": 0.01,
    }.get(name, default))
    strategy = InstitutionalStrategy(instrument=_instrument())
    data = _Data([100.0] * 40, feed_ok=True, flow=2400.0)
    decision = strategy.evaluate(data, _Orders(), _Risk(), 1)
    values = decision.model_values
    assert values["near_touch_depth_usd"] > 0
    assert "weighted_signal_bps" in values
    assert "ofi_component_bps" in values
    assert "tfi_component_bps" in values
    assert abs(values["raw_microstructure_signal_bps"] - (
        values["ofi_component_bps"] + values["tfi_component_bps"]
        + values["microprice_component_bps"]
    )) < 1e-9
    assert abs(values["weighted_signal_bps"] - (
        values["robust_microstructure_alpha_bps"] + values["venue_local_market_state_alpha_bps"]
    )) < 1e-9
    assert values["dislocation_component_bps"] == 0.0


def test_selected_broker_balance_controls_final_position_size_not_delta_balance(tmp_path, monkeypatch):
    from strategy.domain import Direction, ProtectionPlan

    monkeypatch.setattr("strategy.institutional_strategy._cfg", lambda name, default: {
        "RESEARCH_STORE_PATH": str(tmp_path),
        "VENUE_SELECTION_ENABLED": True,
        "INSTITUTIONAL_RISK_FRACTION_PER_TRADE": 1.0,
        "INSTITUTIONAL_QUARTER_KELLY": 1.0,
        "INSTITUTIONAL_TARGET_OBSERVATION_VOL_BPS": 100000.0,
        "LEVERAGE": 5.0,
        "INSTITUTIONAL_MAX_SELECTED_LEVERAGE": 5.0,
    }.get(name, default))
    strategy = InstitutionalStrategy(instrument=_silver_instrument())
    delta_funded_risk = SimpleNamespace(get_available_balance=lambda: {"available": 180.31, "source": "delta"})
    selected_hyperliquid = SimpleNamespace(get_balance=lambda: {"available": 20.0, "source": "hyperliquid.user_state"})
    decision = strategy._size_position(
        "DESK_A_METALS", "xyz:SILVER", Direction.LONG, 30.0, 100.0, 1.0,
        ProtectionPlan(30.0, 29.0, 32.0, "VENUE_NATIVE_BRACKET", True), delta_funded_risk,
        venue="hyperliquid", balance_source=selected_hyperliquid,
    )
    assert decision.available_cash_used == 20.0
    assert decision.capital_venue == "hyperliquid"
    assert decision.balance_source == "hyperliquid.user_state"
    assert decision.margin_required <= 20.0
    assert decision.notional <= 20.0 * 0.20 + 1e-9
    assert decision.reasons[0] == "broker_local_cash_sizing_approved:hyperliquid"


def test_selected_broker_balance_failure_never_falls_back_to_delta_cash(tmp_path, monkeypatch):
    from strategy.domain import Direction, ProtectionPlan

    monkeypatch.setattr("strategy.institutional_strategy._cfg", lambda name, default: {
        "RESEARCH_STORE_PATH": str(tmp_path),
        "VENUE_SELECTION_ENABLED": True,
    }.get(name, default))
    strategy = InstitutionalStrategy(instrument=_silver_instrument())
    delta_funded_risk = SimpleNamespace(get_available_balance=lambda: {"available": 180.31, "source": "delta"})
    failed_hyperliquid = SimpleNamespace(get_balance=lambda: {"available": 0.0, "source": "hyperliquid.user_state"})
    decision = strategy._size_position(
        "DESK_A_METALS", "xyz:SILVER", Direction.LONG, 30.0, 100.0, 1.0,
        ProtectionPlan(30.0, 29.0, 32.0, "VENUE_NATIVE_BRACKET", True), delta_funded_risk,
        venue="hyperliquid", balance_source=failed_hyperliquid,
    )
    assert decision.approved is False
    assert decision.available_cash_used == 0.0
    assert decision.reasons == ["cash_unavailable:hyperliquid"]


def test_current_venue_cannot_trade_when_route_model_values_it_at_a_loss(tmp_path, monkeypatch):
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_SIGNAL_DECAY_READY", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_FOR_DELTA", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_FOR_DELTA", False, raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_VENUES", (), raising=False)
    monkeypatch.setattr("strategy.dynamic_protection.config.DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_VENUES", (), raising=False)

    def cfg(name, default):
        return {
            "RESEARCH_STORE_PATH": str(tmp_path),
            "INSTITUTIONAL_ENABLE_LIVE_ENTRIES": True,
            "INSTITUTIONAL_MIN_NET_EDGE_BPS": 0.1,
            "INSTITUTIONAL_MIN_SIGNAL_BPS": 0.01,
            "VENUE_SELECTION_MAX_COST_BPS": 100.0,
        }.get(name, default)

    selected = SimpleNamespace(
        selected_venue="delta", selected_symbol="BTCUSD", selected_cost_bps=523.2277,
        current_venue="delta", current_cost_bps=523.2277, improvement_bps=0.0,
        reason="selected_cost_above_soft_limit:523.23>100.00",
        estimates={"delta": SimpleNamespace(expected_net_edge_bps=-402.3366, expected_net_profit_usd=-1.8136)},
        as_dict=lambda: {"selected_venue": "delta", "selected_cost_bps": 523.2277},
    )
    monkeypatch.setattr("strategy.institutional_strategy._cfg", cfg)
    monkeypatch.setattr("strategy.institutional_strategy.select_execution_venue", lambda **kwargs: selected)
    decision = InstitutionalStrategy(instrument=_instrument()).evaluate(_Data([100.0] * 5), _Orders(), _Risk(), 1)
    assert decision.decision is DecisionOutput.NO_TRADE_EXECUTION_UNSAFE
    assert decision.reasons[0].startswith("selected_route_cost_exceeds_limit:")

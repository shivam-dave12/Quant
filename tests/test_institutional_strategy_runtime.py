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


def _instrument():
    ei = ExchangeInstrument(
        exchange=ExchangeName.DELTA, symbol="BTCUSD", ws_symbol="BTCUSD", display_symbol="BTCUSD", asset_id="BTC",
        asset_class=AssetClass.CRYPTO, quote_asset="USD", status="active", tick_size=0.5, lot_step=1.0,
        min_qty=1.0, raw={"contract_type": "inverse_perp", "contract_multiplier": 1.0},
    )
    return TradableInstrument("BTC", "Bitcoin", AssetClass.CRYPTO, ExchangeName.DELTA, {ExchangeName.DELTA: ei})


def test_invalid_feed_produces_explicit_execution_unsafe_rejection(tmp_path, monkeypatch):
    monkeypatch.setattr("strategy.institutional_strategy._cfg", lambda name, default: str(tmp_path) if name == "RESEARCH_STORE_PATH" else default)
    strategy = InstitutionalStrategy(instrument=_instrument())
    decision = strategy.evaluate(_Data([100.0] * 40, feed_ok=False), _Orders(), _Risk(), 1)
    assert decision.decision is DecisionOutput.NO_TRADE_EXECUTION_UNSAFE
    assert decision.reasons == ["disconnected"]


def test_shadow_mode_blocks_live_order_even_when_flow_edge_is_positive(tmp_path, monkeypatch):
    def cfg(name, default):
        values = {"RESEARCH_STORE_PATH": str(tmp_path), "INSTITUTIONAL_ENABLE_LIVE_ENTRIES": False,
                  "INSTITUTIONAL_MIN_NET_EDGE_BPS": 0.1, "INSTITUTIONAL_MIN_SIGNAL_BPS": 0.01}
        return values.get(name, default)
    monkeypatch.setattr("strategy.institutional_strategy._cfg", cfg)
    strategy = InstitutionalStrategy(instrument=_instrument())
    data = _Data([100.0 + i * 0.02 for i in range(80)], feed_ok=True)
    orders = _Orders()
    for i in range(80):
        strategy.on_tick(data, orders, _Risk(), i)
    assert strategy._last_decision is not None
    assert strategy._last_decision.decision is DecisionOutput.NO_TRADE_INSUFFICIENT_EDGE
    assert "shadow_mode_live_entries_disabled" in strategy._last_decision.reasons
    assert orders.placed == []


def test_momentum_without_flow_is_not_a_trade_signal(tmp_path, monkeypatch):
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
    assert any(reason.startswith(("flow_signal_flat", "venue_flow_disagreement")) for reason in decision.reasons)


def test_live_entry_requires_protection_confirmation(tmp_path, monkeypatch):
    def cfg(name, default):
        values = {"RESEARCH_STORE_PATH": str(tmp_path), "INSTITUTIONAL_ENABLE_LIVE_ENTRIES": True,
                  "INSTITUTIONAL_MIN_NET_EDGE_BPS": 0.1, "INSTITUTIONAL_MIN_SIGNAL_BPS": 0.01,
                  "LEVERAGE": 2.0}
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
    assert "options_volatility_context_required" in decision.reasons

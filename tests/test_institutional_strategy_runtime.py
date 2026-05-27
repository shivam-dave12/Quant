from types import SimpleNamespace

from core.instruments import AssetClass, ExchangeInstrument, ExchangeName, TradableInstrument
from strategy.domain import DecisionOutput
from strategy.institutional_strategy import InstitutionalStrategy, PositionPhase


class _Data:
    def __init__(self, prices, *, feed_ok=True):
        self.prices = list(prices)
        self.i = 0
        self.feed_ok = feed_ok

    def get_last_price(self):
        px = self.prices[min(self.i, len(self.prices) - 1)]
        self.i += 1
        return px

    def get_orderbook(self):
        px = self.prices[min(max(self.i - 1, 0), len(self.prices) - 1)]
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
            "order_id": "entry-1",
            "quantity": qty,
            "fill_price": entry,
            "bracket_sl_order_id": "sl-1",
            "bracket_tp_order_id": "tp-1",
            "bracket_child_verified": True,
            "protection_model": "VENUE_NATIVE_BRACKET",
        }

    def get_open_position(self):
        return {"size": 1.0}


def _instrument():
    ei = ExchangeInstrument(
        exchange=ExchangeName.DELTA,
        symbol="BTCUSD",
        ws_symbol="BTCUSD",
        display_symbol="BTCUSD",
        asset_id="BTC",
        asset_class=AssetClass.CRYPTO,
        quote_asset="USD",
        status="active",
        tick_size=0.5,
        lot_step=1.0,
        min_qty=1.0,
        raw={"contract_type": "inverse_perp", "contract_multiplier": 1.0},
    )
    return TradableInstrument("BTC", "Bitcoin", AssetClass.CRYPTO, ExchangeName.DELTA, {ExchangeName.DELTA: ei})


def test_invalid_feed_produces_explicit_execution_unsafe_rejection(tmp_path, monkeypatch):
    monkeypatch.setattr("strategy.institutional_strategy._cfg", lambda name, default: str(tmp_path) if name == "RESEARCH_STORE_PATH" else default)
    strategy = InstitutionalStrategy(instrument=_instrument())
    decision = strategy.evaluate(_Data([100.0] * 40, feed_ok=False), _Orders(), _Risk(), 1)
    assert decision.decision is DecisionOutput.NO_TRADE_EXECUTION_UNSAFE
    assert decision.reasons == ["disconnected"]


def test_shadow_mode_blocks_live_order_even_when_edge_is_positive(tmp_path, monkeypatch):
    def cfg(name, default):
        if name == "RESEARCH_STORE_PATH":
            return str(tmp_path)
        if name == "INSTITUTIONAL_ENABLE_LIVE_ENTRIES":
            return False
        if name == "INSTITUTIONAL_MIN_NET_EDGE_BPS":
            return 0.1
        return default

    monkeypatch.setattr("strategy.institutional_strategy._cfg", cfg)
    strategy = InstitutionalStrategy(instrument=_instrument())
    data = _Data([100.0 + i * 0.25 for i in range(80)], feed_ok=True)
    orders = _Orders()
    for i in range(80):
        strategy.on_tick(data, orders, _Risk(), i)
    assert strategy._last_decision is not None
    assert strategy._last_decision.decision is DecisionOutput.NO_TRADE_INSUFFICIENT_EDGE
    assert "shadow_mode_live_entries_disabled" in strategy._last_decision.reasons
    assert orders.placed == []


def test_live_entry_requires_protection_confirmation(tmp_path, monkeypatch):
    def cfg(name, default):
        if name == "RESEARCH_STORE_PATH":
            return str(tmp_path)
        if name == "INSTITUTIONAL_ENABLE_LIVE_ENTRIES":
            return True
        if name == "INSTITUTIONAL_MIN_NET_EDGE_BPS":
            return 0.1
        if name == "LEVERAGE":
            return 2.0
        return default

    monkeypatch.setattr("strategy.institutional_strategy._cfg", cfg)
    strategy = InstitutionalStrategy(instrument=_instrument())
    data = _Data([100.0 + i * 0.25 for i in range(80)], feed_ok=True)
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


def test_groww_direction_language_is_underlying_first_ce_pe_buy_only(tmp_path, monkeypatch):
    monkeypatch.setattr("strategy.institutional_strategy._cfg", lambda name, default: str(tmp_path) if name == "RESEARCH_STORE_PATH" else default)
    ei = ExchangeInstrument(
        exchange=ExchangeName.GROWW,
        symbol="NIFTY",
        ws_symbol="NIFTY",
        display_symbol="NIFTY",
        asset_id="NIFTY",
        asset_class=AssetClass.OPTION,
        quote_asset="INR",
        status="active",
        tick_size=0.05,
        lot_step=50.0,
        min_qty=50.0,
        raw={"stock_code": "NIFTY", "right": "call", "strike_price": 25000, "expiry_date": "2026-06-25"},
    )
    inst = TradableInstrument("NIFTY", "NIFTY options", AssetClass.OPTION, ExchangeName.GROWW, {ExchangeName.GROWW: ei})
    strategy = InstitutionalStrategy(instrument=inst)
    orders = SimpleNamespace(active_exchange="groww", symbol="NIFTY", display_symbol="NIFTY")
    data = _Data([24000.0 + i for i in range(40)], feed_ok=True)
    decision = None
    for i in range(40):
        decision = strategy.evaluate(data, orders, _Risk(), i)
    assert decision is not None
    assert decision.desk == "DESK_B_NIFTY_OPTIONS"
    assert decision.direction.value in {"BULLISH", "NO_TRADE"}

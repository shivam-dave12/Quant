import json
import inspect
from types import SimpleNamespace

from exchanges.hyperliquid.data_manager import HyperliquidDataManager
from strategy.institutional_strategy import InstitutionalStrategy
from aggregator.market_aggregator import MarketAggregator


def _hl_manager():
    manager = HyperliquidDataManager(coin="xyz:SILVER", execution_enabled=True)
    manager.is_streaming = True
    return manager


def test_hyperliquid_ws_candles_and_asset_context_populate_cache_without_tick_http(monkeypatch):
    manager = _hl_manager()
    manager._on_message(None, json.dumps({"channel": "candle", "data": {"t": 1000, "i": "1m", "o": 74.0, "h": 74.4, "l": 73.9, "c": 74.2, "v": 10.0}}))
    manager._on_message(None, json.dumps({"channel": "activeAssetCtx", "data": {"coin": "xyz:SILVER", "ctx": {"funding": 0.0001, "markPx": 74.2, "openInterest": 500}}}))
    rows = manager.get_candles("1m", 10)
    assert len(rows) == 1 and rows[0]["close"] == 74.2
    assert manager._funding_rate == 0.0001
    assert manager._mark_price == 74.2
    assert "candles_snapshot" not in inspect.getsource(manager.get_candles)


def test_hyperliquid_trade_and_book_tape_reaches_protection_research_state():
    manager = _hl_manager()
    manager._on_message(None, json.dumps({"channel": "l2Book", "data": {"time": 1000, "levels": [[{"px": "74.1", "sz": "12"}], [{"px": "74.2", "sz": "14"}]]}}))
    manager._on_message(None, json.dumps({"channel": "trades", "data": [{"px": "74.2", "sz": "2", "side": "B"}]}))
    research = manager.get_microstructure_research_state()
    assert research["book_events"]
    assert research["trade_events"]
    assert research["trade_events"][-1]["signed_notional_usd"] > 0


def test_strategy_market_callbacks_wake_context_without_executing_a_tick():
    strategy = InstitutionalStrategy(instrument=None)
    assert strategy.consume_market_event() is False
    strategy._on_realtime_quote(101.0)
    assert strategy.consume_market_event() is True
    strategy._on_realtime_trade(101.0, 2.0, "buy")
    assert strategy.consume_market_event() is True


class _DM:
    def __init__(self, venue):
        self.venue = venue
        self.strategy = None
    def register_strategy(self, strategy):
        self.strategy = strategy


def test_aggregator_registers_event_listener_on_every_information_venue():
    a, b, c = _DM("delta"), _DM("coinswitch"), _DM("hyperliquid")
    marker = object()
    agg = MarketAggregator(a, b, reference_dms=[c])
    agg.register_strategy(marker)
    assert a.strategy is marker and b.strategy is marker and c.strategy is marker


class _BalanceManager:
    def __init__(self):
        self.called = 0
    def get_balance(self):
        self.called += 1
        return {"available": 999.0}


class _Router:
    def __init__(self, manager):
        self.manager = manager
    def manager_for(self, venue):
        return self.manager
    def available_exchanges(self):
        return {"hyperliquid"}


def test_market_hot_path_consumes_cached_collateral_only():
    manager = _BalanceManager()
    router = _Router(manager)
    strategy = InstitutionalStrategy(instrument=None)
    strategy._runtime_order_manager = router  # refresh service intentionally not started in this unit test
    with strategy._venue_cash_lock:
        strategy._venue_cash_cache["hyperliquid"] = (10**18, 6.72, "cached")
    assert strategy._venue_available_cash(router, {"hyperliquid"}) == {"hyperliquid": 6.72}
    assert manager.called == 0


def test_parallel_runtime_has_atomic_submission_arbitration_guard():
    from orchestration.multi_asset_bot import MultiAssetInstitutionalBot
    bot = MultiAssetInstitutionalBot()
    assert hasattr(bot, "_submission_arbitration_lock")
    strategy = InstitutionalStrategy(instrument=None)
    strategy.bind_portfolio_submission_guard(bot._submission_arbitration_lock, lambda: (False, "portfolio exposure cap"))
    assert strategy._submission_lock is bot._submission_arbitration_lock

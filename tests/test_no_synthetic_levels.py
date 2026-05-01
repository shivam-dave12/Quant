from strategy.liquidity_map import LiquidityMap
from strategy.entry_engine import EntryEngine, MarketState


def test_no_trade_without_candles():
    engine = EntryEngine()
    signal = engine.evaluate(MarketState(price=100.0, candles_by_tf={}))
    assert signal is None
    assert "no synthetic" in engine.last_reject_reason or "invalid" in engine.last_reject_reason


def test_no_trade_without_liquidity():
    engine = EntryEngine()
    candles = [
        {"open": 100, "high": 101, "low": 99, "close": 100, "volume": 1, "timestamp": i}
        for i in range(5)
    ]
    signal = engine.evaluate(MarketState(price=100.0, candles_by_tf={"1m": candles}))
    assert signal is None

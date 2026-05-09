from core.instruments import AssetClass, ExchangeInstrument, ExchangeName, TradableInstrument
from core.market_policy import active_policy


def _inst(exchange, ac, max_lev=0, symbol="TEST"):
    ei = ExchangeInstrument(
        exchange=exchange,
        symbol=symbol,
        ws_symbol=symbol,
        display_symbol=symbol,
        asset_id=symbol,
        asset_class=ac,
        max_leverage=max_lev,
    )
    return TradableInstrument(symbol, symbol, ac, exchange, {exchange: ei})


def test_icici_cash_and_options_are_one_x():
    cash = _inst(ExchangeName.ICICI, AssetClass.CASH, max_lev=99, symbol="RELIANCE")
    opt = _inst(ExchangeName.ICICI, AssetClass.OPTION, max_lev=99, symbol="NIFTYOPT")
    assert active_policy(cash).leverage == 1
    assert active_policy(opt).leverage == 1
    assert active_policy(cash).margin_pct <= 0.06


def test_unknown_exchange_leverage_does_not_infer_static_cap():
    equity = _inst(ExchangeName.DELTA, AssetClass.EQUITY, max_lev=0, symbol="AAPLXUSD")
    assert active_policy(equity).leverage == 1


def test_confirmed_delta_leverage_is_used_below_cap():
    crypto = _inst(ExchangeName.DELTA, AssetClass.CRYPTO, max_lev=40, symbol="BTCUSD")
    pol = active_policy(crypto)
    assert 1 <= pol.leverage < 40
    assert pol.leverage == 22  # 40x venue cap * 55% institutional utilisation

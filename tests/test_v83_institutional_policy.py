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


def test_delta_equity_uses_configured_venue_schedule_when_product_row_omits_cap():
    equity = _inst(ExchangeName.DELTA, AssetClass.EQUITY, max_lev=0, symbol="AAPLXUSD")
    assert active_policy(equity).leverage == 8  # 25x venue schedule * 32% utilisation


def test_delta_btc_uses_institutional_slice_of_venue_cap():
    crypto = _inst(ExchangeName.DELTA, AssetClass.CRYPTO, max_lev=0, symbol="BTCUSD")
    pol = active_policy(crypto)
    assert pol.leverage == 40  # 200x venue schedule * 20% utilisation, firm-capped at 40x


def test_confirmed_delta_leverage_is_used_with_symbol_utilisation():
    crypto = _inst(ExchangeName.DELTA, AssetClass.CRYPTO, max_lev=100, symbol="ETHUSD")
    pol = active_policy(crypto)
    assert pol.leverage == 28  # 100x confirmed venue cap * 28% ETH utilisation

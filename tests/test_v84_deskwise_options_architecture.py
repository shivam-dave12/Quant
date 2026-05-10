from core.instruments import AssetClass, ExchangeInstrument, ExchangeName, TradableInstrument
from agents.desk_router import InstitutionalDeskRouter
from agents.indian_options_desk import BlackScholesModel, IndianOptionsDesk
from agents.tradable_ticker_desk import TradableTickerDesk


def _inst(asset_id, ex, ac, symbol=None, raw=None, max_leverage=0):
    ei = ExchangeInstrument(
        exchange=ex,
        symbol=symbol or asset_id,
        ws_symbol=symbol or asset_id,
        display_symbol=symbol or asset_id,
        asset_id=asset_id,
        asset_class=ac,
        max_leverage=max_leverage,
        raw=raw or {},
        status="active",
        tick_size=0.05 if ex == ExchangeName.ICICI else 0.1,
        lot_step=1,
        min_qty=1,
    )
    return TradableInstrument(asset_id=asset_id, display_name=asset_id, asset_class=ac, primary_exchange=ex, by_exchange={ex: ei})


def test_desk_router_separates_btc_crypto_us_stocks_commodities_and_options():
    r = InstitutionalDeskRouter()
    assert r.desk_id_for(_inst("BTCUSD", ExchangeName.DELTA, AssetClass.CRYPTO, "BTCUSD")) == "BTC_GLOBAL"
    assert r.desk_id_for(_inst("ETHUSD", ExchangeName.DELTA, AssetClass.CRYPTO, "ETHUSD")) == "CRYPTO_ALTS"
    assert r.desk_id_for(_inst("AAPL", ExchangeName.DELTA, AssetClass.EQUITY, "AAPLXUSD")) == "US_STOCK_DERIVATIVES"
    assert r.desk_id_for(_inst("PAXGUSD", ExchangeName.DELTA, AssetClass.COMMODITY, "PAXGUSD")) == "COMMODITIES_GLOBAL"
    opt = _inst("NIFTY_25000_C", ExchangeName.ICICI, AssetClass.OPTION, "NIFTY28MAY2625000CE", {"stock_code": "NIFTY", "right": "call", "strike_price": "25000", "expiry_date": "28-May-2026", "product_type": "OPTIDX"})
    assert r.desk_id_for(opt) == "ICICI_INDEX_OPTIONS"
    cash = _inst("RELIANCE", ExchangeName.ICICI, AssetClass.CASH, "RELIANCE", {"stock_code": "RELIANCE"})
    assert r.desk_id_for(cash) == "ICICI_REJECT_NON_OPTION"


def test_black_scholes_returns_greeks_and_theta_ratio():
    bs = BlackScholesModel.greeks("call", spot=25000, strike=25100, dte=7, rate=0.065, volatility=0.18, premium=180)
    assert bs is not None
    assert 0 < bs.delta < 1
    assert bs.gamma > 0
    assert bs.theta_to_premium >= 0


def test_icici_option_selector_scores_real_option_not_cash():
    desk = IndianOptionsDesk()
    opt = _inst("NIFTY_25000_C", ExchangeName.ICICI, AssetClass.OPTION, "NIFTY28MAY2625000CE", {"stock_code": "NIFTY", "right": "call", "strike_price": "25000", "expiry_date": "28-May-2026", "product_type": "OPTIDX"})
    score = desk.score_option(opt, {"ltp": "220", "best_bid": "219", "best_ask": "221", "underlying_spot_price": "25020", "iv": "18"})
    assert score.desk_id == "ICICI_INDEX_OPTIONS"
    assert score.bs is not None
    assert score.score > 0


def test_tradable_ticker_desk_rejects_icici_cash_before_runtime():
    cash = _inst("RELIANCE", ExchangeName.ICICI, AssetClass.CASH, "RELIANCE", {"stock_code": "RELIANCE", "exchange_code": "NSE", "product_type": "Cash"})
    sel = TradableTickerDesk().select([cash])
    assert not sel.selected
    assert any(r.reason == "icici_non_option_rejected" for r in sel.rows)


def test_icici_structural_underlying_is_rejected_without_allowlist():
    desk = IndianOptionsDesk()
    bad = _inst("BSE_26MAY2026_5200_P", ExchangeName.ICICI, AssetClass.OPTION, "BSE", {"stock_code": "BSE", "exchange_code": "BSE", "right": "put", "strike_price": "5200", "expiry_date": "26-May-2026", "product_type": "options"})
    score = desk.score_option(bad, {"ltp": "10", "best_bid": "9", "best_ask": "11", "underlying_spot_price": "5000", "iv": "20"})
    assert score.score == 0
    assert score.desk_id == "ICICI_REJECT_CORRUPT_OPTION"
    assert "structural_underlying" in score.reasons

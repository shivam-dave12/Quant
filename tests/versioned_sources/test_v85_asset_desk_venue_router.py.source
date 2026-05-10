from core.instruments import AssetClass, ExchangeInstrument, ExchangeName, TradableInstrument
from agents.desk_router import InstitutionalDeskRouter
from agents.tradable_ticker_desk import TradableTickerDesk


def _ei(asset_id, ex, symbol, ac=AssetClass.CRYPTO, max_leverage=0, raw=None):
    return ExchangeInstrument(
        exchange=ex,
        symbol=symbol,
        ws_symbol=symbol,
        display_symbol=symbol,
        asset_id=asset_id,
        asset_class=ac,
        status="active",
        max_leverage=max_leverage,
        raw=raw or {},
    )


def test_btc_is_one_asset_desk_even_when_multiple_venues_exist():
    inst = TradableInstrument(
        asset_id="BTC",
        display_name="BTC",
        asset_class=AssetClass.CRYPTO,
        primary_exchange=ExchangeName.DELTA,
        by_exchange={
            ExchangeName.DELTA: _ei("BTC", ExchangeName.DELTA, "BTCUSD", max_leverage=50),
            ExchangeName.COINSWITCH: _ei("BTC", ExchangeName.COINSWITCH, "BTCUSDT", max_leverage=0),
            ExchangeName.COINDCX: _ei("BTC", ExchangeName.COINDCX, "BTCINR", max_leverage=0),
        },
    )
    router = InstitutionalDeskRouter()
    assert router.desk_id_for(inst) == "BTC_GLOBAL"
    routes = router.venue_routes_for(inst, market_snapshots={
        ExchangeName.DELTA: {"BTCUSD": {"best_bid": 99990, "best_ask": 100000, "volume_24h": 10000, "last_price": 99995}},
        ExchangeName.COINSWITCH: {"BTCUSDT": {"best_bid": 99900, "best_ask": 100100, "volume_24h": 100, "last_price": 100000}},
    })
    assert len(routes) == 3
    assert routes[0].exchange == ExchangeName.DELTA


def test_selection_returns_single_btc_not_one_per_venue():
    inst = TradableInstrument(
        asset_id="BTC",
        display_name="BTC",
        asset_class=AssetClass.CRYPTO,
        primary_exchange=ExchangeName.DELTA,
        by_exchange={
            ExchangeName.DELTA: _ei("BTC", ExchangeName.DELTA, "BTCUSD", max_leverage=50),
            ExchangeName.COINSWITCH: _ei("BTC", ExchangeName.COINSWITCH, "BTCUSDT"),
        },
    )
    sel = TradableTickerDesk().select([inst])
    assert len(sel.selected) <= 1
    assert {r.desk_id for r in sel.rows} == {"BTC_GLOBAL"}
    assert sel.rows[0].route_exchange in {"delta", "coinswitch"}

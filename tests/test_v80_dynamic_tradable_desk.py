from core.instruments import AssetClass, ExchangeInstrument, ExchangeName, TradableInstrument
from agents.tradable_ticker_desk import TradableTickerDesk


def _inst(asset_id, symbol, asset_class=AssetClass.CRYPTO):
    ei = ExchangeInstrument(
        exchange=ExchangeName.DELTA,
        symbol=symbol,
        ws_symbol=symbol,
        display_symbol=symbol,
        asset_id=asset_id,
        asset_class=asset_class,
        status="active",
        max_leverage=100,
        raw={"symbol": symbol},
    )
    return TradableInstrument(
        asset_id=asset_id,
        display_name=asset_id,
        asset_class=asset_class,
        primary_exchange=ExchangeName.DELTA,
        by_exchange={ExchangeName.DELTA: ei},
        priority=100,
    )


class _Delta:
    def get_tickers(self, contract_types=None):
        return {"success": True, "result": [
            {"symbol": "BTCUSD", "mark_price": "100", "best_bid": "99.9", "best_ask": "100.1", "volume": "100000", "high": "104", "low": "98", "open_interest": "10000"},
            {"symbol": "DEADUSD", "mark_price": "10", "best_bid": "9", "best_ask": "11", "volume": "1", "high": "10.1", "low": "9.9", "open_interest": "1"},
            {"symbol": "ETHUSD", "mark_price": "50", "best_bid": "49.95", "best_ask": "50.05", "volume": "80000", "high": "52", "low": "49", "open_interest": "9000"},
        ]}


def test_dynamic_desk_selects_shortlist_before_any_candle_stream(monkeypatch):
    monkeypatch.setattr("config.DYNAMIC_DESK_MAX_ACTIVE_CONTEXTS", 2, raising=False)
    monkeypatch.setattr("config.DYNAMIC_DESK_MIN_SCORE", 0.01, raising=False)
    desk = TradableTickerDesk()
    selection = desk.select([
        _inst("BTC", "BTCUSD"),
        _inst("DEAD", "DEADUSD"),
        _inst("ETH", "ETHUSD"),
    ], delta_api=_Delta())
    assert len(selection.selected) == 2
    assert {x.asset_id for x in selection.selected} == {"BTC", "ETH"}
    assert "stream load avoided" in ";".join(selection.notes)

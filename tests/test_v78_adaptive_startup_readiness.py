from core.instruments import AssetClass, ExchangeName, ExchangeInstrument, TradableInstrument
from exchanges.delta.data_manager import DeltaDataManager
import config

config.DELTA_API_KEY = "unit-test-key"
config.DELTA_SECRET_KEY = "unit-test-secret"


def _instrument(asset_id: str, asset_class: AssetClass):
    ex = ExchangeInstrument(
        exchange=ExchangeName.DELTA,
        symbol=f"{asset_id}XUSD" if asset_id != "BTC" else "BTCUSD",
        ws_symbol=f"{asset_id}XUSD" if asset_id != "BTC" else "BTCUSD",
        display_symbol=asset_id,
        asset_id=asset_id,
        asset_class=asset_class,
        max_leverage=25 if asset_id != "BTC" else 40,
    )
    return TradableInstrument(
        asset_id=asset_id,
        display_name=asset_id,
        asset_class=asset_class,
        primary_exchange=ExchangeName.DELTA,
        by_exchange={ExchangeName.DELTA: ex},
    )


def test_equity_readiness_uses_adaptive_1m_floor_not_global_btc_floor():
    dm = DeltaDataManager(instrument=_instrument("AMZN", AssetClass.EQUITY))
    mins = dm._minimum_data_requirements()
    assert mins["1m"] == 40
    assert mins["5m"] <= 70
    assert mins["15m"] <= 70


def test_amzn_log_shape_from_failure_would_be_ready_under_v78_counts():
    dm = DeltaDataManager(instrument=_instrument("AMZN", AssetClass.EQUITY))
    # Match the bad v77 log: 1m=45, 5m=193, 15m=198, 4h=50, 1d=30.
    dm._candles_1m.extend([object()] * 45)
    dm._candles_5m.extend([object()] * 193)
    dm._candles_15m.extend([object()] * 198)
    dm._candles_1h.extend([object()] * 100)
    dm._candles_4h.extend([object()] * 50)
    dm._candles_1d.extend([object()] * 30)
    snap = dm.readiness_snapshot()
    assert snap["ready"] is True
    assert snap["missing"] == []


def test_aapl_log_shape_from_failure_would_be_ready_under_v78_counts():
    dm = DeltaDataManager(instrument=_instrument("AAPL", AssetClass.EQUITY))
    # Match the bad v77 log: 1m=82, 5m=185, 15m=193, 4h=50, 1d=30.
    dm._candles_1m.extend([object()] * 82)
    dm._candles_5m.extend([object()] * 185)
    dm._candles_15m.extend([object()] * 193)
    dm._candles_1h.extend([object()] * 98)
    dm._candles_4h.extend([object()] * 50)
    dm._candles_1d.extend([object()] * 30)
    snap = dm.readiness_snapshot()
    assert snap["ready"] is True
    assert snap["missing"] == []


def test_startup_timeout_uses_instrument_specific_config_not_global_120s_wait():
    src = open("orchestration/multi_asset_bot.py", encoding="utf-8").read()
    assert "READY_TIMEOUT_EQUITY_SEC" in src
    assert "desk disabled without blocking portfolio" in src
    assert "wait_until_ready(timeout_sec=ready_timeout)" in src

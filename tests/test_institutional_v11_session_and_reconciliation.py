import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from exchanges.groww.market_session import groww_market_session_state
from strategy.institutional_strategy import InstitutionalStrategy, PositionPhase, PositionState


IST = ZoneInfo("Asia/Kolkata")


def test_verified_nse_holiday_keeps_groww_nifty_dormant():
    state = groww_market_session_state(datetime(2026, 5, 28, 9, 30, tzinfo=IST))
    assert state.is_open is False
    assert state.session_code == "HOLIDAY"
    assert "NSE trading holiday" in state.reason


class _PriceData:
    def get_analysis_price(self):
        return 100.0

    def get_venue_microstates(self):
        return {}


class _Risk:
    pass


class _SlowBrokerPositionReader:
    active_exchange = "hyperliquid"
    symbol = "BTC"

    def __init__(self):
        self.entered = threading.Event()
        self.release = threading.Event()
        self.calls = 0

    def get_open_position(self):
        self.calls += 1
        self.entered.set()
        self.release.wait(timeout=2.0)
        return {"size": 1.0}


def test_active_position_broker_reconciliation_is_not_on_tick_hot_path():
    strategy = InstitutionalStrategy(instrument=None)
    strategy._pos = PositionState(
        phase=PositionPhase.ACTIVE,
        side="long",
        quantity=1.0,
        entry_price=100.0,
        sl_price=90.0,
        tp_price=110.0,
        exchange="hyperliquid",
        execution_symbol="BTC",
        asset_id="BTC",
        protection_confirmed=True,
    )
    broker = _SlowBrokerPositionReader()
    start = time.perf_counter()
    strategy._monitor_position(_PriceData(), broker, _Risk())
    elapsed = time.perf_counter() - start
    try:
        assert elapsed < 0.10
        assert broker.entered.wait(timeout=0.5)
        assert broker.calls == 1
    finally:
        broker.release.set()
        strategy.stop_runtime_services()

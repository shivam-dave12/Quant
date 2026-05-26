import os
import threading
import time
from types import SimpleNamespace

os.environ.setdefault("DELTA_API_KEY", "test")
os.environ.setdefault("DELTA_SECRET_KEY", "test")
os.environ.setdefault("BREEZE_API_KEY", "test")
os.environ.setdefault("BREEZE_SECRET_KEY", "test")

import pytest

from strategy.entry_engine import EntryEngine, EntryType, _TrendContext
from strategy.liquidity_map import LiquidityMapSnapshot, LiquidityPool, PoolSide, PoolStatus, PoolTarget, SweepResult
from strategy.quant_strategy import QuantStrategy
from exchanges.icici.data_manager import ICICIOptionDataManager


def _nifty_engine() -> EntryEngine:
    return EntryEngine(instrument=SimpleNamespace(asset_id="NIFTY", primary_exchange="icici"))


def _snapshot(*, bsl=None, ssl=None, sweeps=None):
    return LiquidityMapSnapshot(
        bsl_pools=list(bsl or []), ssl_pools=list(ssl or []), primary_target=None,
        recent_sweeps=list(sweeps or []), swept_bsl_levels=[], swept_ssl_levels=[],
        nearest_bsl_atr=999.0, nearest_ssl_atr=999.0, timestamp=time.time(),
    )


def test_nifty_phase_is_15m_led_and_permits_fast_long_sweep_scalp():
    now = time.time()
    engine = _nifty_engine()
    engine._atr_pctile = 0.50
    regime = engine._nifty_intraday_regime(
        _TrendContext(1, 0.45, 0, 0, 0, 0.32),
        _TrendContext(1, 0.65, 0, 0, 0, 0.58),
        _TrendContext(1, 0.90, 0, 0, 0, 0.92),
    )
    assert regime.side == "long"
    assert regime.aggression in {"FAST_SCALP", "AGGRESSIVE"}

    swept = LiquidityPool(100.0, PoolSide.SSL, "1m", status=PoolStatus.SWEPT, created_at=now - 30)
    sweep = SweepResult(swept, 20, 99.50, 0.50, 1.2, 0.82, "long", now - 5)
    target_pool = LiquidityPool(102.60, PoolSide.BSL, "5m", status=PoolStatus.DETECTED, created_at=now - 5, htf_count=1)
    target = PoolTarget(target_pool, 2.4, "long", 4.0, ["5m"])

    assert engine._try_nifty_trend_sweep_signal(sweep, regime, _snapshot(bsl=[target], sweeps=[sweep]), 100.20, 1.0, now)
    signal = engine.get_signal()
    assert signal is not None
    assert signal.entry_type is EntryType.NIFTY_TREND_SWEEP_SCALP
    assert signal.side == "long" and signal.sl_price < signal.entry_price < signal.tp_price
    assert signal.target_pool.pool.timeframe == "5m"
    assert engine.analysis_info["trigger"] == "EXECUTABLE_NIFTY_TREND_SWEEP_RECLAIM"
    assert engine.analysis_info["entry_sweep_timeframe"] == "1m"


def test_nifty_one_minute_trigger_expires_quickly_instead_of_entering_late():
    now = time.time()
    engine = _nifty_engine()
    swept = LiquidityPool(100.0, PoolSide.SSL, "1m", status=PoolStatus.SWEPT, created_at=now - 100)
    stale = SweepResult(swept, 20, 99.50, 0.50, 1.2, 0.82, "long", now - 95)
    assert engine._fresh_nifty_entry_sweeps(_snapshot(sweeps=[stale]), now) == []


def test_generic_engine_does_not_fade_dominant_15m_delivery_in_split_context():
    engine = EntryEngine()
    decision = engine._context_decision_for_raid(
        "long",
        _TrendContext(1, 0.40, 0, 0, 0, 0.38),
        _TrendContext(-1, 0.95, 0, 0, 0, -0.99),
        0.75,
    )
    assert decision.allowed is False
    assert decision.block == "SPLIT_HTF_TACTICAL_DELIVERY_OPPOSES_RAID"


def test_icici_ambiguous_nfo_tick_cannot_refresh_a_preselected_vehicle():
    api = SimpleNamespace(_normalise_right=lambda value: str(value).lower())
    instrument = SimpleNamespace(asset_id="NIFTY", primary=SimpleNamespace(raw={}))
    dm = ICICIOptionDataManager(instrument, api=api)
    key = ("02-Jun-2026", "call", 24150.0)
    identity = {"stock_code": "NIFTY", "expiry": "02-Jun-2026", "right": "call", "strike": 24150.0}
    dm._book_stream_state[key] = {"event": threading.Event(), "last_stream_tick_ts": 0.0, "stream_pending": True}
    dm._on_session_book_option_tick(key, identity, {"exchange_code": "NFO", "last": 153.0})
    assert dm._book_stream_state[key]["last_stream_tick_ts"] == 0.0


def test_icici_session_status_never_calls_preselected_premium_fresh_without_ws_tick():
    now = time.time()
    raw = {
        "session_contract_book_status": "READY",
        "session_contract_book": {
            "trade_date_ist": "2026-05-26", "built_at": now, "underlying": "NIFTY", "underlying_spot": 24070.0,
            "available_funds": 40000.0, "source": "test",
            "call": {"selected_symbol": "NIFTYCE", "right": "call", "strike": 24150.0, "expiry": "02-Jun-2026", "delta": 0.45, "raw": {"selected_entry_premium": 153.0, "runtime_lot_size": 65}},
            "put": {"selected_symbol": "NIFTYPE", "right": "put", "strike": 24000.0, "expiry": "02-Jun-2026", "delta": -0.42, "raw": {"selected_entry_premium": 152.0, "runtime_lot_size": 65}},
        },
    }
    instrument = SimpleNamespace(asset_id="NIFTY", primary=SimpleNamespace(raw=raw))
    dm = ICICIOptionDataManager(instrument, api=SimpleNamespace())
    dm._book_stream_state[("02-Jun-2026", "call", 24150.0)] = {
        "last_stream_tick_ts": now, "last_price": 154.0, "best_bid": 153.8, "best_ask": 154.1, "stream_pending": False
    }
    dm._book_stream_state[("02-Jun-2026", "put", 24000.0)] = {
        "last_stream_tick_ts": 0.0, "last_price": 0.0, "best_bid": 0.0, "best_ask": 0.0, "stream_pending": True
    }
    status = dm.session_contract_book_status()
    assert status["call"]["ws_fresh"] is True
    assert status["put"]["ws_fresh"] is False
    assert status["status"] == "ARMED_PENDING_WEBSOCKET_TICK"
    assert status["execution_freshness_gate"] == "WEBSOCKET_TICK_REQUIRED"


def test_nifty_sweep_scalp_uses_short_intraday_time_stop(monkeypatch):
    import config
    monkeypatch.setattr(config, "QUANT_TIME_STOP_ENABLED", True, raising=False)
    monkeypatch.setattr(config, "ICICI_NIFTY_TREND_SWEEP_MAX_HOLD_SEC", 720.0, raising=False)
    qs = QuantStrategy.__new__(QuantStrategy)
    pos = SimpleNamespace(
        asset_id="NIFTY", archetype="NIFTY_TREND_SWEEP_SCALP", side="long", entry_price=100.0,
        sl_price=99.0, initial_sl_dist=1.0, peak_profit=0.0, entry_time=time.time() - 721.0,
    )
    assert qs._time_decay_exit_reason(pos, 100.0, time.time()) == "time_stop_hard_max_hold"


def test_nifty_fast_sweep_cannot_override_live_parent_auction_control():
    from strategy.market_state import build_auction_narrative
    now = time.time()
    engine = _nifty_engine()
    regime = engine._nifty_intraday_regime(
        _TrendContext(1, 0.45, 0, 0, 0, 0.32),
        _TrendContext(1, 0.65, 0, 0, 0, 0.58),
        _TrendContext(1, 0.90, 0, 0, 0, 0.92),
    )
    parent_pool = LiquidityPool(103.0, PoolSide.BSL, "1h", status=PoolStatus.SWEPT, created_at=now - 10)
    parent_short = SweepResult(parent_pool, 5, 103.5, 0.5, 1.5, 0.92, "short", now - 5)
    swept = LiquidityPool(100.0, PoolSide.SSL, "1m", status=PoolStatus.SWEPT, created_at=now - 8)
    local_long = SweepResult(swept, 20, 99.50, 0.50, 1.2, 0.82, "long", now - 3)
    engine._market_state = build_auction_narrative(
        {"4h": _TrendContext(-1, 0.80, 0, 0, 0, -0.55), "1h": _TrendContext(-1, 0.85, 0, 0, 0, -0.72), "15m": _TrendContext(1, 0.70, 0, 0, 0, 0.60)},
        [parent_short], [local_long], now,
    )
    target_pool = LiquidityPool(102.60, PoolSide.BSL, "5m", status=PoolStatus.DETECTED, created_at=now - 5, htf_count=1)
    target = PoolTarget(target_pool, 2.4, "long", 4.0, ["5m"])
    assert not engine._try_nifty_trend_sweep_signal(local_long, regime, _snapshot(bsl=[target], sweeps=[local_long]), 100.20, 1.0, now)
    assert engine.analysis_info["block_reason"] == "NIFTY_SWEEP_OPPOSES_AUCTION_CONTROL"

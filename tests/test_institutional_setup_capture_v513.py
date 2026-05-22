"""Regression tests for structural setup capture and closed-candle lineage."""
from __future__ import annotations

import time

import pytest

import strategy.liquidity_map as lm
from strategy.entry_engine import EntryEngine, _select_mss_reference
from strategy.liquidity_map import (
    LiquidityMap,
    LiquidityMapSnapshot,
    LiquidityPool,
    PoolSide,
    PoolStatus,
    STRUCTURAL_RAID_CONFIRMATION_WINDOW_SEC,
    SWEEP_CONFIRMATION_WINDOW_SEC_BY_TF,
    SweepResult,
)
from strategy.quant_strategy import ATREngine, QCfg


def _sweep(now: float, timeframe: str = "5m", age_sec: float = 650.0) -> SweepResult:
    pool = LiquidityPool(
        price=100.0,
        side=PoolSide.BSL,
        timeframe=timeframe,
        status=PoolStatus.SWEPT,
        created_at=now - 5000.0,
        swept_at=now - age_sec,
        sweep_wick=101.0,
    )
    return SweepResult(
        pool=pool,
        sweep_candle_idx=10,
        wick_extreme=101.0,
        rejection_pct=0.75,
        volume_ratio=1.4,
        quality=0.85,
        direction="short",
        detected_at=now - age_sec,
    )


def _snapshot(sweep: SweepResult, now: float) -> LiquidityMapSnapshot:
    return LiquidityMapSnapshot(
        bsl_pools=[],
        ssl_pools=[],
        primary_target=None,
        recent_sweeps=[sweep],
        swept_bsl_levels=[],
        swept_ssl_levels=[],
        nearest_bsl_atr=0.0,
        nearest_ssl_atr=0.0,
        timestamp=now,
    )


def _bar(start: float, o: float, h: float, l: float, c: float) -> dict:
    return {"t": int(start), "o": o, "h": h, "l": l, "c": c, "v": 1.0}


def _wilder_atr(rows: list[dict], period: int) -> float:
    tr = [
        max(
            float(rows[i]["h"]) - float(rows[i]["l"]),
            abs(float(rows[i]["h"]) - float(rows[i - 1]["c"])),
            abs(float(rows[i]["l"]) - float(rows[i - 1]["c"])),
        )
        for i in range(1, len(rows))
    ]
    value = sum(tr[:period]) / period
    for item in tr[period:]:
        value = (value * (period - 1) + item) / period
    return value


def test_5m_raid_survives_more_than_one_bar_for_closed_confirmation_sequence() -> None:
    now = 2_000_000_000.0
    raid = _sweep(now, "5m", 650.0)
    liq_map = LiquidityMap()
    liq_map._recent_sweeps = [raid]

    liq_map.update({}, price=100.0, atr=1.0, now=now)

    assert raid in liq_map.get_snapshot(100.0, 1.0).recent_sweeps
    assert STRUCTURAL_RAID_CONFIRMATION_WINDOW_SEC == pytest.approx(1200.0)


def test_htf_sweep_retention_matches_declared_parent_context_horizon() -> None:
    now = 2_000_000_000.0
    htf = _sweep(now, "4h", 3600.0)
    liq_map = LiquidityMap()
    liq_map._recent_sweeps = [htf]

    liq_map.update({}, price=100.0, atr=1.0, now=now)

    assert htf in liq_map.get_snapshot(100.0, 1.0).recent_sweeps
    assert SWEEP_CONFIRMATION_WINDOW_SEC_BY_TF["4h"] >= 3600.0


def test_entry_engine_reads_structurally_live_raid_after_one_bar() -> None:
    now = 2_000_000_000.0
    raid = _sweep(now, "5m", 650.0)
    fresh = EntryEngine()._fresh_5m_sweeps(_snapshot(raid, now), now)

    assert fresh == [raid]


def test_mss_uses_latest_confirmed_internal_swing_not_window_extreme() -> None:
    lows = [95.0, 99.8, 100.1, 100.0, 99.7, 100.3, 100.5, 100.2, 99.2, 100.0, 100.4, 100.2]
    bars = [
        _bar(1_000.0 + i * 300.0, low + 0.5, low + 1.0, low, low + 0.4)
        for i, low in enumerate(lows)
    ]
    bars.append(_bar(1_000.0 + len(bars) * 300.0, 100.8, 101.5, 100.6, 101.0))

    level, source, age_bars, count = _select_mss_reference(bars, "short", 12)

    assert source == "LATEST_CONFIRMED_INTERNAL_SWING"
    assert level == pytest.approx(99.2)
    assert level > min(lows)
    assert age_bars == 3
    assert count == 12


def test_liquidity_pool_geometry_excludes_forming_tail(monkeypatch: pytest.MonkeyPatch) -> None:
    now = 2_000_000_000.0
    closed = [_bar(now - (11 - i) * 300.0, 100, 101, 99, 100) for i in range(10)]
    forming = _bar(now - 100.0, 100, 120, 80, 100)
    seen_lengths: list[int] = []

    monkeypatch.setattr(lm, "_find_swing_highs", lambda rows, lookback: seen_lengths.append(len(rows)) or [])
    monkeypatch.setattr(lm, "_find_swing_lows", lambda rows, lookback: seen_lengths.append(len(rows)) or [])
    monkeypatch.setattr(lm, "_range_extreme_cluster", lambda rows, tf, side: seen_lengths.append(len(rows)) or None)

    lm._TimeframeRegistry("5m").update(closed + [forming], atr=1.0, now=now)

    assert seen_lengths
    assert all(length == len(closed) for length in seen_lengths)


def test_atr_uses_latest_bar_when_stream_tail_is_already_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    now = 2_000_000_000.0
    monkeypatch.setattr("strategy.quant_strategy.time.time", lambda: now)
    period = QCfg.ATR_PERIOD()
    rows = [
        _bar(now - (period + 2 - i) * 300.0, 100.0, 101.0, 99.0, 100.0)
        for i in range(period + 1)
    ]
    rows.append(_bar(now - 301.0, 100.0, 130.0, 70.0, 100.0))

    observed = ATREngine().compute(rows)
    expected = _wilder_atr(rows, period)
    stale = _wilder_atr(rows[:-1], period)

    assert observed == pytest.approx(expected)
    assert observed > stale


def test_native_atr_uses_same_closed_tail_resolution_as_entry_atr() -> None:
    now = 2_000_000_000.0
    period = 14
    rows = [
        _bar(now - (period + 2 - i) * 300.0, 100.0, 101.0, 99.0, 100.0)
        for i in range(period + 1)
    ]
    rows.append(_bar(now - 301.0, 100.0, 130.0, 70.0, 100.0))

    observed = lm._native_closed_atr(rows, period=period, timeframe="5m", now=now)

    assert observed == pytest.approx(_wilder_atr(rows, period))

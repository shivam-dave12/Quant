from pathlib import Path
import inspect
import pytest

from strategy.entry_engine import EntryEngine, EntryType
from strategy.quant_strategy import QuantStrategy

ROOT = Path(__file__).resolve().parents[1]


def test_single_entry_authority_is_ict_liquidity_only():
    assert list(EntryType) == [EntryType.ICT_LIQUIDITY]
    src = inspect.getsource(QuantStrategy._evaluate_entry)
    assert "EntryEngine" not in src or "_entry_engine" in src
    assert "_compute_signals" not in inspect.getsource(QuantStrategy)
    assert "_institutional_decision_matrix" not in inspect.getsource(QuantStrategy)



def test_entry_authority_accepts_no_external_alpha_inputs():
    src = inspect.getsource(EntryEngine.update)
    assert "flow_state" not in src
    assert "ict_ctx" not in src
    assert "delivery_probability" in inspect.getsource(EntryEngine)


def test_retired_strategy_modules_are_physically_removed():
    removed = [
        "direction_engine.py", "conviction_filter.py", "expected_utility.py",
        "quantitative_models.py", "post_exit_gate.py", "post_trade_agent.py",
        "cross_asset_regime.py", "ict_engine.py", "display_engine.py",
        "market_intelligence.py", "dol_engine.py", "liquidity_pool_selector.py",
    ]
    assert all(not (ROOT / "strategy" / name).exists() for name in removed)


def test_operator_surface_contains_only_new_authority_vocabulary():
    files = [ROOT / "telegram" / "controller.py", ROOT / "telegram" / "notifier.py", ROOT / "main.py"]
    combined = "\n".join(p.read_text() for p in files)
    for retired in ("QuantPosterior", "ConvictionFilter", "DirectionEngine", "AMD", "CVD", "flow_conviction"):
        assert retired not in combined
    assert "ICT + LIQUIDITY" in combined
    assert "/flow" not in combined and "_cmd_flow" not in combined


def test_structural_tp_cannot_use_external_alpha_overlay():
    src = (ROOT / "strategy" / "tp_ladder.py").read_text()
    assert "cross_asset" not in src
    assert "posterior" not in src


from types import SimpleNamespace
import time
from strategy.liquidity_map import LiquidityPool, PoolSide, PoolStatus, PoolTarget, SweepResult, LiquidityMapSnapshot


def _snap(*, bsl=None, ssl=None, sweeps=None):
    return LiquidityMapSnapshot(
        bsl_pools=list(bsl or []), ssl_pools=list(ssl or []), primary_target=None,
        recent_sweeps=list(sweeps or []), swept_bsl_levels=[], swept_ssl_levels=[],
        nearest_bsl_atr=999.0, nearest_ssl_atr=999.0, timestamp=time.time(),
    )


def test_only_fresh_five_minute_raids_can_trigger_entry():
    now = time.time()
    fresh_pool = LiquidityPool(99.0, PoolSide.SSL, "5m", status=PoolStatus.SWEPT, created_at=now - 30)
    htf_pool = LiquidityPool(98.0, PoolSide.SSL, "15m", status=PoolStatus.SWEPT, created_at=now - 30)
    old_pool = LiquidityPool(97.0, PoolSide.SSL, "5m", status=PoolStatus.SWEPT, created_at=now - 900)
    fresh = SweepResult(fresh_pool, 5, 98.5, 0.5, 1.0, 0.8, "long", now - 20)
    htf = SweepResult(htf_pool, 5, 97.8, 0.5, 1.0, 0.8, "long", now - 20)
    stale = SweepResult(old_pool, 5, 96.8, 0.5, 1.0, 0.8, "long", now - 700)
    accepted = EntryEngine()._fresh_5m_sweeps(_snap(sweeps=[fresh, htf, stale]), now)
    assert accepted == [fresh]


def test_structural_stop_is_beyond_raided_wick_with_volatility_clearance():
    engine = EntryEngine()
    engine.set_atr_pctile(0.50)
    thesis = SimpleNamespace(side="long", sweep=SimpleNamespace(wick_extreme=98.0), fvg=SimpleNamespace(low=99.0, high=100.0))
    stop = engine._structural_stop(thesis, atr=2.0)
    assert stop is not None
    assert stop < 98.0
    assert abs(stop - (98.0 - 2.0 * (0.10 + 0.18 * 0.50))) < 1e-9


def test_delivery_target_excludes_unpromoted_five_minute_pool():
    engine = EntryEngine()
    engine._thesis = SimpleNamespace(
        context_4h=SimpleNamespace(confidence=0.90),
        context_15m=SimpleNamespace(confidence=0.90),
    )
    p5 = LiquidityPool(108.0, PoolSide.BSL, "5m", status=PoolStatus.DETECTED, created_at=time.time(), htf_count=1)
    p15 = LiquidityPool(107.0, PoolSide.BSL, "15m", status=PoolStatus.DETECTED, created_at=time.time(), htf_count=1)
    t5 = PoolTarget(p5, 8.0, "long", 9.0, ["5m"])
    t15 = PoolTarget(p15, 7.0, "long", 4.0, ["15m"])
    selected = engine._select_liquidity_target("long", entry=100.0, sl=98.0, snap=_snap(bsl=[t5, t15]), atr=1.0)
    assert selected is not None
    target, tp, rr, utility, probability = selected
    assert target.pool.timeframe == "15m"
    assert 100.0 < tp < 107.0
    assert rr > 1.0 and utility > 0.0 and 0.05 <= probability <= 0.95


def _trend_candles(step: float, n: int):
    rows = []
    for i in range(n):
        base = 100.0 + i * step
        rows.append({"o": base, "h": base + 1.0, "l": base - 1.0, "c": base + 0.70})
    return rows


def _ranging_candles(n: int, base: float = 100.0):
    return [{"o": base, "h": base + 1.0, "l": base - 1.0, "c": base + 0.10} for _ in range(n)]


def _short_raid_candles():
    c5 = [{"o": 100.60, "h": 100.90, "l": 100.10, "c": 100.45} for _ in range(34)]
    for i in range(8, 20):
        c5[i] = {"o": 100.55, "h": 101.00, "l": 100.10, "c": 100.40}
    c5[20] = {"o": 100.50, "h": 102.00, "l": 100.20, "c": 100.60}  # BSL raid
    c5[21] = {"o": 100.50, "h": 100.70, "l": 100.00, "c": 100.20}
    c5[22] = {"o": 99.80, "h": 99.90, "l": 96.80, "c": 97.00}  # displacement
    c5[23] = {"o": 98.70, "h": 99.00, "l": 96.70, "c": 96.90}  # bearish FVG below index 21 low
    for i in range(24, 33):
        c5[i] = {"o": 96.80, "h": 97.10, "l": 95.70, "c": 96.20}
    c5[33] = {"o": 99.50, "h": 99.80, "l": 99.20, "c": 99.50}  # still forming; live mark reprices FVG
    return c5


def test_context_strength_uses_each_timeframes_own_atr_not_entry_atr():
    now = time.time()
    c5 = _trend_candles(0.10, 32)
    c15 = _trend_candles(1.00, 32)
    c4h = _trend_candles(3.00, 28)
    small_entry_atr, large_entry_atr = EntryEngine(), EntryEngine()
    small_entry_atr.update(_snap(), 100.0, 1.0, now, c5, c15, c4h)
    large_entry_atr.update(_snap(), 100.0, 100.0, now, c5, c15, c4h)
    first, second = small_entry_atr.analysis_info, large_entry_atr.analysis_info
    assert first["context_4h_conf"] == pytest.approx(second["context_4h_conf"])
    assert first["context_15m_conf"] == pytest.approx(second["context_15m_conf"])
    assert first["context_4h_atr"] == pytest.approx(second["context_4h_atr"])
    assert first["context_15m_atr"] == pytest.approx(second["context_15m_atr"])
    assert first["entry_5m_atr"] != second["entry_5m_atr"]


def test_entry_engine_accepts_lifecycle_reset_reason_without_legacy_state():
    engine = EntryEngine()
    engine.force_reset("market-data stream restarted")
    assert engine.state == "SCANNING"


def test_retired_alpha_configuration_is_physically_absent():
    cfg = (ROOT / "config.py").read_text()
    for retired in ("POST_EXIT_", "QUANT_ICT_", "QUANT_W_VWAP", "QUANT_W_TICK_FLOW", "QUANT_COMPOSITE_ENTRY_MIN"):
        assert retired not in cfg


def test_full_4h_15m_5m_structural_sequence_emits_one_executable_ticket():
    now = time.time()
    c15 = _trend_candles(0.55, 33)
    c4h = _trend_candles(1.40, 28)
    c5 = [{"o": 99.20, "h": 99.70, "l": 99.00, "c": 99.45} for _ in range(34)]
    for i in range(8, 20):
        c5[i] = {"o": 99.30, "h": 100.00, "l": 99.10, "c": 99.60}
    c5[20] = {"o": 99.40, "h": 99.70, "l": 98.00, "c": 99.30}  # SSL raid
    c5[21] = {"o": 99.40, "h": 100.00, "l": 99.30, "c": 99.80}
    c5[22] = {"o": 100.20, "h": 103.20, "l": 100.10, "c": 103.00}  # displacement
    c5[23] = {"o": 101.30, "h": 103.40, "l": 101.00, "c": 103.10}  # FVG above index 21 high
    for i in range(24, 33):
        c5[i] = {"o": 103.00, "h": 104.30, "l": 102.90, "c": 104.00}
    c5[33] = {"o": 100.60, "h": 100.80, "l": 100.30, "c": 100.50}  # still forming; live mark reprices FVG
    raid_pool = LiquidityPool(99.0, PoolSide.SSL, "5m", status=PoolStatus.SWEPT, created_at=now - 40)
    raid = SweepResult(raid_pool, 20, 98.0, 1.0, 1.4, 0.91, "long", now - 30)
    bsl_pool = LiquidityPool(110.0, PoolSide.BSL, "15m", status=PoolStatus.DETECTED, created_at=now - 90, htf_count=1)
    target = PoolTarget(bsl_pool, 9.50, "long", 5.0, ["15m"])
    engine = EntryEngine()
    engine.update(_snap(bsl=[target], sweeps=[raid]), price=100.50, atr=1.0, now=now, candles_5m=c5, candles_15m=c15, candles_4h=c4h)
    signal = engine.get_signal()
    assert signal is not None
    assert signal.entry_type is EntryType.ICT_LIQUIDITY
    assert signal.side == "long" and signal.entry_price == pytest.approx(100.50)
    assert signal.sl_price < 98.0 < signal.entry_price < signal.tp_price < 110.0
    assert signal.target_pool.pool.timeframe == "15m"
    assert signal.delivery_probability > 0 and signal.rr_ratio > 1.0
    assert engine.state == "EXECUTABLE"


def test_htf_trend_misalignment_no_longer_hard_blocks_without_a_raid():
    now = time.time()
    c5 = _trend_candles(0.10, 32)
    c4h = _trend_candles(2.00, 30)
    c15 = _trend_candles(-1.00, 34)
    engine = EntryEngine()
    engine.update(_snap(), price=100.0, atr=1.0, now=now,
                  candles_5m=c5, candles_15m=c15, candles_4h=c4h)
    info = engine.analysis_info
    assert info["context_4h"] == "bullish" and info["context_15m"] == "bearish"
    assert info["context_aligned"] is False
    assert info["block_reason"] == "AWAITING_FRESH_5M_LIQUIDITY_RAID"
    assert engine.state == "CONTEXT_READY"


def test_ranging_4h_with_15m_dol_can_approve_short_external_liquidity_raid():
    now = time.time()
    c4h = _ranging_candles(30)
    c15 = _trend_candles(-0.70, 34)
    c5 = _short_raid_candles()
    raid_pool = LiquidityPool(101.0, PoolSide.BSL, "5m", status=PoolStatus.SWEPT, created_at=now - 40)
    raid = SweepResult(raid_pool, 20, 102.0, 1.0, 1.5, 0.92, "short", now - 30)
    ssl_pool = LiquidityPool(90.0, PoolSide.SSL, "15m", status=PoolStatus.DETECTED, created_at=now - 90, htf_count=1)
    target = PoolTarget(ssl_pool, 9.50, "short", 5.0, ["15m"])
    engine = EntryEngine()
    engine.update(_snap(ssl=[target], sweeps=[raid]), price=99.50, atr=1.0, now=now,
                  candles_5m=c5, candles_15m=c15, candles_4h=c4h)
    signal = engine.get_signal()
    info = engine.analysis_info
    assert signal is not None
    assert signal.side == "short" and signal.tp_price < signal.entry_price < signal.sl_price
    assert info["context_4h"] == "ranging" and info["context_15m"] == "bearish"
    assert info["context_permission"] is True
    assert info["context_bias_path"] == "PARTIAL_HTF_DOL"
    assert info["block_reason"] == "NONE"


def test_unanimous_htf_delivery_against_raid_is_rejected_before_execution():
    now = time.time()
    c4h = _trend_candles(1.00, 30)
    c15 = _trend_candles(0.70, 34)
    c5 = _short_raid_candles()
    raid_pool = LiquidityPool(101.0, PoolSide.BSL, "5m", status=PoolStatus.SWEPT, created_at=now - 40)
    raid = SweepResult(raid_pool, 20, 102.0, 1.0, 1.5, 0.92, "short", now - 30)
    ssl_pool = LiquidityPool(90.0, PoolSide.SSL, "15m", status=PoolStatus.DETECTED, created_at=now - 90, htf_count=1)
    target = PoolTarget(ssl_pool, 9.50, "short", 5.0, ["15m"])
    engine = EntryEngine()
    engine.update(_snap(ssl=[target], sweeps=[raid]), price=99.50, atr=1.0, now=now,
                  candles_5m=c5, candles_15m=c15, candles_4h=c4h)
    info = engine.analysis_info
    assert engine.get_signal() is None
    assert info["block_reason"] == "HTF_DELIVERY_OPPOSES_RAID"
    assert info["context_permission"] is False


def test_decision_snapshot_exposes_native_atr_and_waiting_gate_calculations():
    now = time.time()
    engine = EntryEngine()
    engine.update(_snap(), price=100.0, atr=1.25, now=now,
                  candles_5m=_trend_candles(0.10, 32),
                  candles_15m=_trend_candles(0.70, 34),
                  candles_4h=_trend_candles(2.00, 30))
    info = engine.analysis_info
    assert engine.state == "CONTEXT_READY"
    assert info["block_reason"] == "AWAITING_FRESH_5M_LIQUIDITY_RAID"
    assert info["context_aligned"] is True and info["context_direction"] == "long"
    for key in ("context_4h_slope_atr", "context_4h_efficiency", "context_4h_atr",
                "context_15m_slope_atr", "context_15m_efficiency", "context_15m_atr",
                "entry_5m_atr", "atr_percentile", "fresh_5m_raid_count"):
        assert key in info


def test_operator_surface_is_portfolio_wide_and_routine_logging_is_throttled():
    orchestration = (ROOT / "orchestration" / "multi_asset_bot.py").read_text()
    controller = (ROOT / "telegram" / "controller.py").read_text()
    cfg = (ROOT / "config.py").read_text()
    assert "format_portfolio_thinking_report" in orchestration
    assert "format_portfolio_status_report" in orchestration
    assert "format_portfolio_thinking_report" in controller
    assert "DESK_HEALTH" in orchestration and "ANALYSIS_TICK" not in orchestration
    assert "ICT_DECISION_SNAPSHOT_SEC = 60.0" in cfg
    assert "SCANNER_ASSET_ANALYSIS_LOG_SEC = 60.0" in cfg


def test_no_fixed_leverage_or_retired_telegram_decision_language_remains():
    root = Path(__file__).resolve().parents[1]
    active = (root / "strategy" / "quant_strategy.py").read_text()
    cfg = (root / "config.py").read_text()
    notifier = (root / "telegram" / "notifier.py").read_text()
    for retired in ("AGGRESSIVE_LEVERAGE", "_aggressive_leverage", "_roe_leverage", "_ADAPTIVE_PARAM_PROVIDER"):
        assert retired not in active + cfg
    for retired in ("QUANT POSTERIOR DECISION", 'return "POSTERIOR"', "ADAPTIVE EXIT", "POOL-GATE"):
        assert retired not in notifier


def test_structural_funding_leverage_selects_minimum_required_not_venue_cap(monkeypatch):
    from strategy.quant_strategy import QuantStrategy, QCfg
    qs = QuantStrategy.__new__(QuantStrategy)
    monkeypatch.setattr(QCfg, "LEVERAGE", staticmethod(lambda: 45))
    monkeypatch.setattr(qs, "_liquidation_safe_leverage_cap", lambda *a, **k: 45.0)
    normal = qs._structural_funding_leverage(
        price=100.0, sl_dist=2.0, configured_leverage=45, side="long", sl_price=98.0,
        target_margin_budget=10.0, risk_capital=0.20,
    )
    tight_stop = qs._structural_funding_leverage(
        price=100.0, sl_dist=0.1, configured_leverage=45, side="long", sl_price=99.9,
        target_margin_budget=10.0, risk_capital=0.20,
    )
    assert normal == 1.0
    assert tight_stop == 20.0
    assert tight_stop < 45.0

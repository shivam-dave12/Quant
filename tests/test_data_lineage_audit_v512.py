from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import inspect
import time

from aggregator.market_aggregator import MarketAggregator
from strategy.entry_engine import EntryEngine
from strategy.liquidity_map import LiquidityMap, LiquidityMapSnapshot, LiquidityPool, PoolSide, PoolStatus, PoolTarget
from strategy.quant_strategy import QuantStrategy

ROOT = Path(__file__).resolve().parents[1]


def _bars(step: float, n: int, interval: int, start: float = 1_900_000_000.0):
    rows = []
    for i in range(n):
        px = 100.0 + step * i
        rows.append({"t": int((start + i * interval) * 1000), "o": px, "h": px + abs(step) + 1.0,
                     "l": px - abs(step) - 1.0, "c": px + step * 0.5, "v": 100.0})
    return rows


def _snap(bsl=None):
    return LiquidityMapSnapshot(bsl_pools=list(bsl or []), ssl_pools=[], primary_target=None,
                                recent_sweeps=[], swept_bsl_levels=[], swept_ssl_levels=[],
                                nearest_bsl_atr=999.0, nearest_ssl_atr=999.0, timestamp=time.time())


def test_liquidity_registries_use_native_timeframe_atr_not_shared_five_minute_atr():
    lm = LiquidityMap()
    candles = {"5m": _bars(0.05, 50, 300), "15m": _bars(0.8, 50, 900), "4h": _bars(6.0, 50, 14400)}
    lm.update(candles, price=100.0, atr=1.0, now=1_900_100_000.0)
    native = lm._native_atr_by_tf
    assert native["4h"] > native["15m"] > native["5m"]
    source = inspect.getsource(LiquidityMap.update)
    assert "reg.update(candles, native_atr, now)" in source
    assert "reg.check_sweeps(candles, native_atr, now)" in source


def test_structural_target_enforces_active_desk_reward_floor():
    engine = EntryEngine()
    engine._thesis = SimpleNamespace(context_4h=SimpleNamespace(confidence=0.95), context_15m=SimpleNamespace(confidence=0.95))
    p15 = LiquidityPool(104.4, PoolSide.BSL, "15m", status=PoolStatus.DETECTED, created_at=time.time(), htf_count=1)
    target = PoolTarget(p15, 4.4, "long", 5.0, ["15m"])
    engine.set_structural_delivery_policy(min_rr=2.20, max_rr_reference=5.0)
    assert engine._select_liquidity_target("long", entry=100.0, sl=98.0, snap=_snap([target]), atr=1.0) is None
    engine.set_structural_delivery_policy(min_rr=1.50, max_rr_reference=5.0)
    assert engine._select_liquidity_target("long", entry=100.0, sl=98.0, snap=_snap([target]), atr=1.0) is not None


def test_market_aggregator_analysis_freshness_cannot_be_masked_by_option_premium_feed():
    primary = SimpleNamespace(get_last_price=lambda: 150.0, is_price_fresh=lambda secs: True, get_last_update=lambda: datetime.now(timezone.utc))
    analysis = SimpleNamespace(get_last_price=lambda: 23800.0, is_price_fresh=lambda secs: False, get_last_update=lambda: datetime.now(timezone.utc))
    agg = MarketAggregator(primary, None, analysis_dm=analysis)
    assert agg.get_last_price() == 150.0 and agg.get_analysis_price() == 23800.0
    assert agg.is_execution_price_fresh(10.0) is True
    assert agg.is_analysis_price_fresh(10.0) is False
    assert agg.is_price_fresh(10.0) is False
    assert agg.get_last_update() > 0


def test_orderbook_provenance_and_startup_leverage_are_not_fabricated():
    delta_source = (ROOT / "exchanges" / "delta" / "data_manager.py").read_text()
    coin_source = (ROOT / "exchanges" / "coinswitch" / "data_manager.py").read_text()
    main_source = (ROOT / "main.py").read_text()
    orchestration = (ROOT / "orchestration" / "multi_asset_bot.py").read_text()
    assert '"timestamp": float(self._last_orderbook_update_time or 0.0)' in delta_source
    assert '"timestamp": float(self._last_orderbook_update_time or 0.0)' in coin_source
    assert "DEFERRED_UNTIL_APPROVED_STRUCTURAL_ENTRY" in main_source
    start_block = orchestration[orchestration.index("def _start_one_context"):orchestration.index("def run", orchestration.index("def _start_one_context"))]
    assert "_set_leverage_with_backoff" not in start_block


def test_geometry_and_operator_tape_mark_unevaluated_stages_as_na_not_zero():
    qsrc = inspect.getsource(QuantStrategy._log_ict_decision_snapshot)
    osrc = (ROOT / "orchestration" / "multi_asset_bot.py").read_text()
    assert "FVG=N/A prerequisite=MSS_BREAK" in qsrc
    assert "SL=N/A prerequisite=FVG_REPRICE" in qsrc
    assert "N/A means that calculation stage has not been reached" in osrc
    assert "FVG=[{float(info.get('fvg_low',0)" not in osrc


def test_structural_evaluation_reads_analysis_domain_and_has_integrity_gate():
    src = inspect.getsource(QuantStrategy._evaluate_entry)
    assert 'get_analysis_price' in src
    assert '_audit_structural_inputs' in src
    assert 'set_structural_delivery_policy' in src
    assert 'DATA_INTEGRITY_BLOCK' in src


def test_live_stale_execution_book_is_hard_blocked_before_entry_cost_approval():
    qs = QuantStrategy.__new__(QuantStrategy)
    qs._atr_5m = SimpleNamespace(atr=1.0)
    qs._active_spread_cost_mult = 1.0
    qs._last_spread_gate_context = {}
    qs._instrument = SimpleNamespace(asset_id="BTC", asset_class=SimpleNamespace(value="crypto"), tick_size=0.1)
    dm = SimpleNamespace(
        get_data_lineage=lambda: {"analysis_source": "DeltaDataManager"},
        get_orderbook=lambda: {"bids": [[100.0, 2]], "asks": [[100.1, 3]], "timestamp": time.time() - 60.0},
    )
    ok, ratio = qs._spread_atr_gate(dm)
    assert ok is False
    assert ratio == float("inf")
    assert qs._last_spread_gate_context["hard_fail_reason"] == "STALE_EXECUTION_BOOK"
    assert qs._last_spread_gate_context["spread_bps"] > 0
    assert qs._last_spread_gate_context["spread_atr"] > 0


def test_commodity_spread_atr_is_allocation_haircut_not_execution_veto():
    qs = QuantStrategy.__new__(QuantStrategy)
    qs._atr_5m = SimpleNamespace(atr=0.10)
    qs._active_spread_cost_mult = 1.0
    qs._last_spread_gate_context = {}
    qs._instrument = SimpleNamespace(asset_id="SILVER", asset_class=SimpleNamespace(value="commodity"), tick_size=0.01)
    dm = SimpleNamespace(
        get_data_lineage=lambda: {"analysis_source": "DeltaDataManager"},
        get_orderbook=lambda: {"bids": [[100.00, 2]], "asks": [[100.30, 3]], "timestamp": time.time()},
    )

    ok, ratio = qs._spread_atr_gate(dm)

    assert ok is True
    assert ratio > 0.65
    assert qs._last_spread_gate_context["hard_fail"] is False
    assert qs._last_spread_gate_context["cost_alert"] is True
    assert 0.0 < qs._last_spread_gate_context["size_mult"] < 1.0


def test_live_stale_analysis_quote_blocks_structural_authority_even_with_valid_bars(monkeypatch):
    import strategy.quant_strategy as qm
    qs = QuantStrategy.__new__(QuantStrategy)
    qs._instrument = SimpleNamespace(primary_exchange=SimpleNamespace(value="delta"))
    dm = SimpleNamespace(
        get_data_lineage=lambda: {"analysis_source": "Underlying", "execution_source": "Option", "analysis_domain": "UNDERLYING", "execution_domain": "OPTION_PREMIUM"},
        is_analysis_price_fresh=lambda secs: False,
        get_analysis_last_update=lambda: time.time() - 180.0,
    )
    now = time.time()
    candles = {"5m": _bars(0.1, 80, 300, now - 80 * 300), "15m": _bars(0.3, 30, 900, now - 30 * 900), "4h": _bars(1.0, 30, 14400, now - 30 * 14400)}
    monkeypatch.setattr(qm.QCfg, "EXCHANGE", staticmethod(lambda: "delta"))
    monkeypatch.setattr(qm.QCfg, "MIN_5M_BARS", staticmethod(lambda: 60))
    quality = qs._audit_structural_inputs(dm, candles, 23800.0, now)
    assert quality["ok"] is False
    assert "ANALYSIS_QUOTE_STALE" in quality["blockers"]
    assert quality["lineage"]["execution_domain"] == "OPTION_PREMIUM"


def test_groww_activation_surface_discloses_live_option_fundamentals_and_execution_metrics():
    src = (ROOT / "exchanges" / "groww" / "data_manager.py").read_text()
    for field in ("delta=%+.3f", "theta/prem=%.4f", "spread/ATR1m=%.3f", "visible_depth=%.0f", "quote=FRESH execution=APPROVED"):
        assert field in src


def test_dormant_and_uninitialised_desks_never_render_zero_as_market_data():
    osrc = (ROOT / "orchestration" / "multi_asset_bot.py").read_text()
    assert 'state = "DORMANT"' in osrc
    assert 'px_txt = f"{px:,.4f}" if px is not None and px > 0.0 else "N/A"' in osrc
    assert 'unit = "NIFTYpts" if is_groww or' in osrc
    assert 'mark_txt = f"{self._esc(unit)}{mark:,.4f}" if mark is not None else "N/A"' in osrc

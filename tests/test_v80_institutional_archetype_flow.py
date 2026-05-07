
import time
from types import SimpleNamespace
from core.instruments import AssetClass, ExchangeName, ExchangeInstrument, TradableInstrument, instrument_scope
from core.market_policy import active_policy
from strategy.entry_engine import EntryEngine
from strategy.liquidity_map import LiquidityMapSnapshot, LiquidityPool, PoolSide, PoolStatus, PoolTarget
from strategy.liquidity_pool_selector import select_tp_with_report

def _instrument(asset_id: str, asset_class: AssetClass):
    ex = ExchangeInstrument(exchange=ExchangeName.DELTA, symbol=f"{asset_id}USD" if asset_id == "BTC" else f"{asset_id}XUSD", ws_symbol=f"{asset_id}USD", display_symbol=asset_id, asset_id=asset_id, asset_class=asset_class, max_leverage=40 if asset_id == "BTC" else 25)
    return TradableInstrument(asset_id=asset_id, display_name=asset_id, asset_class=asset_class, primary_exchange=ExchangeName.DELTA, by_exchange={ExchangeName.DELTA: ex})

def _target(price, pool_side, *, entry=4704.0, atr=8.0, sig=4.0, tf="15m", status=PoolStatus.CONFIRMED, touches=2):
    now = time.time()
    pool = LiquidityPool(price=float(price), side=pool_side, timeframe=tf, status=status, touches=touches, created_at=now, last_touch=now, ob_aligned=True, fvg_aligned=True, htf_count=2)
    return PoolTarget(pool=pool, distance_atr=abs(float(price)-float(entry))/max(float(atr),1e-9), direction="above" if float(price)>float(entry) else "below", significance=float(sig), tf_sources=[tf])

def _snapshot(entry=4704.0, atr=8.0, bsl=None, ssl=None):
    return LiquidityMapSnapshot(bsl_pools=list(bsl or []), ssl_pools=list(ssl or []), primary_target=None, recent_sweeps=[], swept_bsl_levels=[], swept_ssl_levels=[], nearest_bsl_atr=1.0, nearest_ssl_atr=1.0, timestamp=time.time())

def _engine():
    e = EntryEngine.__new__(EntryEngine)
    e._tf_rank_for_stop_geometry = EntryEngine._tf_rank_for_stop_geometry
    e._policy_float = EntryEngine._policy_float
    e._policy_bool = EntryEngine._policy_bool
    e._atr_pctile = 0.5
    e._ict = None
    e._last_pool_plan = None
    e._current_quant_posterior = lambda: 0.90
    return e

def test_v80_predictive_shelf_expansion_does_not_revert_to_old_sweep():
    with instrument_scope(_instrument("GOLD", AssetClass.COMMODITY)):
        e = _engine()
        snap = _snapshot(entry=4704.0, atr=8.0, ssl=[_target(4688.3, PoolSide.SSL, entry=4704.0, atr=8.0, sig=1.0, tf="5m"), _target(4686.6, PoolSide.SSL, entry=4704.0, atr=8.0, sig=1.2, tf="15m")])
        ps = SimpleNamespace(max_displacement=0.80, cisd_detected=True, ote_reached=False, ote_holding=False)
        inv, reason = e._accepted_structure_invalidation(snap, "long", 4704.0, 8.0, 4626.7, ps)
        assert 4685.0 <= inv <= 4689.0, reason
        e._last_liq_snapshot = snap
        sl, why = e._apply_institutional_sl_envelope(snap, "long", price=4704.0, atr=8.0, structural_sl=inv-1.6, invalidation_price=inv, label="v80-test")
        assert sl is not None, why
        assert 4680.0 < sl < 4688.3
        assert sl > 4626.7

def test_v80_late_gold_breakout_is_blocked_not_chased():
    with instrument_scope(_instrument("GOLD", AssetClass.COMMODITY)):
        e = _engine()
        snap = _snapshot(entry=4731.0, atr=8.0, bsl=[_target(4734.0, PoolSide.BSL, entry=4731.0, atr=8.0, sig=3.0, tf="5m")])
        blocked, reason = e._predictive_late_entry_check(snap, "long", entry=4731.0, atr=8.0, sweep_price=4626.7, sl=4686.5)
        assert blocked
        assert "late/compressed" in reason

def test_v80_staged_tp_selects_real_liquidity_path_without_synthetic_target():
    with instrument_scope(_instrument("BTC", AssetClass.CRYPTO)):
        pol = active_policy()
        snap = _snapshot(entry=100.0, atr=2.0, ssl=[_target(97.2, PoolSide.SSL, entry=100.0, atr=2.0, sig=20.0, tf="5m"), _target(95.0, PoolSide.SSL, entry=100.0, atr=2.0, sig=28.0, tf="15m")], bsl=[_target(102.0, PoolSide.BSL, entry=100.0, atr=2.0, sig=10.0, tf="5m")])
        tp, target, score, report = select_tp_with_report(snap, "short", entry=100.0, sl=101.6, atr=2.0, min_rr=pol.min_rr, posterior_prob=0.92)
        assert tp is not None, report.summary
        assert target.pool.side is PoolSide.SSL
        assert target.pool.status is not PoolStatus.SWEPT
        assert score.components.get("posterior_prob", 0) >= 0.90

import time
from types import SimpleNamespace
from pathlib import Path

from core.instruments import AssetClass, ExchangeName, ExchangeInstrument, TradableInstrument, instrument_scope
from strategy.entry_engine import EntryEngine
from strategy.liquidity_map import LiquidityMapSnapshot, LiquidityPool, PoolSide, PoolStatus, PoolTarget


def _instrument(asset_id: str, asset_class: AssetClass):
    ex = ExchangeInstrument(
        exchange=ExchangeName.DELTA,
        symbol=f"{asset_id}USD" if asset_id == "BTC" else f"{asset_id}XUSD",
        ws_symbol=f"{asset_id}USD",
        display_symbol=asset_id,
        asset_id=asset_id,
        asset_class=asset_class,
        max_leverage=40 if asset_id == "BTC" else 25,
    )
    return TradableInstrument(
        asset_id=asset_id,
        display_name=asset_id,
        asset_class=asset_class,
        primary_exchange=ExchangeName.DELTA,
        by_exchange={ExchangeName.DELTA: ex},
    )


def _target(price, pool_side, *, entry=100.0, atr=1.0, sig=4.0, tf="5m", status=PoolStatus.CONFIRMED):
    now = time.time()
    pool = LiquidityPool(
        price=float(price), side=pool_side, timeframe=tf, status=status,
        touches=2, created_at=now, last_touch=now,
        ob_aligned=True, fvg_aligned=True, htf_count=1,
    )
    return PoolTarget(
        pool=pool,
        distance_atr=abs(float(price) - float(entry)) / max(float(atr), 1e-9),
        direction="above" if float(price) > float(entry) else "below",
        significance=float(sig),
        tf_sources=[tf],
    )


def _snapshot(entry=100.0, atr=1.0, bsl=None, ssl=None):
    return LiquidityMapSnapshot(
        bsl_pools=list(bsl or []), ssl_pools=list(ssl or []), primary_target=None,
        recent_sweeps=[], swept_bsl_levels=[], swept_ssl_levels=[],
        nearest_bsl_atr=1.0, nearest_ssl_atr=1.0, timestamp=time.time(),
    )


def _engine():
    e = EntryEngine.__new__(EntryEngine)
    e._tf_rank_for_stop_geometry = EntryEngine._tf_rank_for_stop_geometry
    e._policy_float = EntryEngine._policy_float
    e._policy_bool = EntryEngine._policy_bool
    e._atr_pctile = 0.5
    e._last_pool_plan = None
    e._ict = None
    e._current_quant_posterior = lambda: 0.90
    return e


def test_v81_blocks_far_old_sweep_when_no_accepted_live_shelf():
    with instrument_scope(_instrument("META", AssetClass.EQUITY)):
        e = _engine()
        reason = e._old_sweep_invalidation_block_reason(
            "deep-sweep invalidation; no accepted live protective shelf",
            "long",
            entry=615.0,
            anchor=600.0,
            atr=0.04,
            label="reversal",
        )
        assert "no accepted live shelf" in reason
        assert "old-sweep invalidation risk" in reason
        assert "not executable" in reason


def test_v81_accepts_live_protective_shelf_even_when_old_sweep_is_wrong_side():
    with instrument_scope(_instrument("AMZN", AssetClass.EQUITY)):
        e = _engine()
        snap = _snapshot(
            entry=274.2,
            atr=0.20,
            bsl=[_target(274.55, PoolSide.BSL, entry=274.2, atr=0.20, sig=5.0, tf="5m")],
        )
        ps = SimpleNamespace(max_displacement=1.0, cisd_detected=True, ote_reached=False, ote_holding=False)
        inv, note = e._accepted_structure_invalidation(
            snap, "short", entry=274.2, atr=0.20, deep_invalidation=272.8, ps=ps
        )
        assert abs(inv - 274.55) < 1e-9
        assert "accepted shelf" in note


def test_v81_does_not_label_pre_utility_entry_engine_signal_as_executable():
    src = Path("strategy/entry_engine.py").read_text()
    assert "EXECUTABLE CANDIDATE" not in src
    assert "STRUCTURAL CANDIDATE" in src


def test_v81_runtime_fingerprint_is_unique():
    src = Path("strategy/quant_strategy.py").read_text()
    assert "v81-preflight-verified-institutional-flow" in src

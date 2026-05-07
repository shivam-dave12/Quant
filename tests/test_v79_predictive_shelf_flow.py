import time
from types import SimpleNamespace

from core.instruments import AssetClass, ExchangeName, ExchangeInstrument, TradableInstrument, instrument_scope
from core.market_policy import active_policy
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


def _target(price, pool_side, *, entry=4704.0, atr=8.0, sig=4.0, tf="15m", status=PoolStatus.CONFIRMED):
    now = time.time()
    pool = LiquidityPool(
        price=float(price), side=pool_side, timeframe=tf, status=status,
        touches=2, created_at=now, last_touch=now,
        ob_aligned=True, fvg_aligned=True, htf_count=2,
    )
    return PoolTarget(
        pool=pool,
        distance_atr=abs(float(price) - float(entry)) / max(float(atr), 1e-9),
        direction="above" if float(price) > float(entry) else "below",
        significance=float(sig),
        tf_sources=[tf],
    )


def _snapshot(entry=4704.0, atr=8.0, bsl=None, ssl=None):
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
    return e


def test_gold_predictive_shelf_uses_live_ssl_not_old_sweep_invalidation():
    with instrument_scope(_instrument("GOLD", AssetClass.COMMODITY)):
        e = _engine()
        snap = _snapshot(
            entry=4704.0,
            atr=8.0,
            ssl=[
                _target(4688.3, PoolSide.SSL, entry=4704.0, atr=8.0, sig=5.0, tf="1m"),
                _target(4686.6, PoolSide.SSL, entry=4704.0, atr=8.0, sig=6.5, tf="5m"),
            ],
        )
        ps = SimpleNamespace(max_displacement=0.80, cisd_detected=False, ote_reached=False, ote_holding=False)
        inv, reason = e._accepted_structure_invalidation(
            snap, "long", entry=4704.0, atr=8.0, deep_invalidation=4626.7, ps=ps
        )
        assert 4680.0 < inv < 4695.0
        assert "accepted shelf" in reason


def test_gold_without_delivery_keeps_old_deep_sweep_invalidation():
    with instrument_scope(_instrument("GOLD", AssetClass.COMMODITY)):
        e = _engine()
        snap = _snapshot(
            entry=4704.0, atr=8.0,
            ssl=[_target(4688.3, PoolSide.SSL, entry=4704.0, atr=8.0, sig=8.0, tf="5m")],
        )
        ps = SimpleNamespace(max_displacement=0.10, cisd_detected=False, ote_reached=False, ote_holding=False)
        inv, reason = e._accepted_structure_invalidation(
            snap, "long", entry=4704.0, atr=8.0, deep_invalidation=4626.7, ps=ps
        )
        assert inv == 4626.7
        assert "delivery not accepted" in reason


def test_predictive_late_entry_blocks_chase_when_tp_is_compressed():
    with instrument_scope(_instrument("GOLD", AssetClass.COMMODITY)):
        e = _engine()
        snap = _snapshot(
            entry=4724.0, atr=8.0,
            bsl=[_target(4727.0, PoolSide.BSL, entry=4724.0, atr=8.0, sig=4.0, tf="5m")],
        )
        blocked, reason = e._predictive_late_entry_check(
            snap, "long", entry=4724.0, atr=8.0, sweep_price=4626.7, sl=4686.0
        )
        assert blocked
        assert "late/compressed" in reason


def test_all_symbol_profiles_are_staged_but_behavior_specific():
    expected = {
        "BTC": (AssetClass.CRYPTO, "btc_staged_liquidity_runner"),
        "GOLD": (AssetClass.COMMODITY, "commodity_staged_displacement"),
        "SILVER": (AssetClass.COMMODITY, "commodity_staged_displacement"),
        "NVDA": (AssetClass.EQUITY, "high_beta_xstock_staged_spread_aware"),
        "TSLA": (AssetClass.EQUITY, "high_beta_xstock_staged_spread_aware"),
        "COIN": (AssetClass.EQUITY, "high_beta_xstock_staged_spread_aware"),
        "CRCL": (AssetClass.EQUITY, "high_beta_xstock_staged_spread_aware"),
        "AAPL": (AssetClass.EQUITY, "large_cap_xstock_staged_clean"),
        "AMZN": (AssetClass.EQUITY, "large_cap_xstock_staged_clean"),
        "META": (AssetClass.EQUITY, "large_cap_xstock_staged_clean"),
        "GOOGL": (AssetClass.EQUITY, "large_cap_xstock_staged_clean"),
        "SPY": (AssetClass.EQUITY, "large_cap_xstock_staged_clean"),
        "QQQ": (AssetClass.EQUITY, "large_cap_xstock_staged_clean"),
    }
    for aid, (asset_class, style) in expected.items():
        with instrument_scope(_instrument(aid, asset_class)):
            pol = active_policy()
            assert pol.target_style == style
            assert pol.shelf_sl_enabled is True
            assert pol.tp_path_support_lift_max > 0.0

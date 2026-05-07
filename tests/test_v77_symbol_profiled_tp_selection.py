import time
import pytest

from core.instruments import AssetClass, ExchangeName, ExchangeInstrument, TradableInstrument, instrument_scope
from core.market_policy import active_policy
from strategy.liquidity_map import LiquidityMapSnapshot, LiquidityPool, PoolSide, PoolStatus, PoolTarget
from strategy.liquidity_pool_selector import select_tp_with_report, score_tp_pools


def _instrument(asset_id: str, asset_class: AssetClass = AssetClass.CRYPTO):
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


def _target(price, pool_side, *, entry=100.0, atr=2.0, sig=10.0, tf="5m", status=PoolStatus.CONFIRMED, touches=4):
    now = time.time()
    pool = LiquidityPool(
        price=float(price), side=pool_side, timeframe=tf, status=status,
        touches=touches, created_at=now, last_touch=now,
        ob_aligned=True, fvg_aligned=True, htf_count=2,
    )
    return PoolTarget(
        pool=pool,
        distance_atr=abs(float(price) - float(entry)) / max(float(atr), 1e-9),
        direction="above" if float(price) > float(entry) else "below",
        significance=float(sig),
        tf_sources=[tf],
    )


def _snapshot(entry=100.0, atr=2.0, bsl=None, ssl=None):
    return LiquidityMapSnapshot(
        bsl_pools=list(bsl or []), ssl_pools=list(ssl or []), primary_target=None,
        recent_sweeps=[], swept_bsl_levels=[], swept_ssl_levels=[],
        nearest_bsl_atr=1.0, nearest_ssl_atr=1.0, timestamp=time.time(),
    )


def test_btc_policy_uses_staged_liquidity_profile_not_generic_crypto():
    with instrument_scope(_instrument("BTC", AssetClass.CRYPTO)):
        pol = active_policy()
        assert pol.target_style == "btc_staged_liquidity_runner"
        assert pol.tp_path_support_lift_max > 0.20
        assert pol.tp_primary_objective_min_rr < 0.80
        assert pol.min_rr <= 1.65


def test_btc_tp_selector_accepts_primary_liquidity_objective_when_positive_ev():
    # Synthetic BTC frame: a valid sweep has a protective SL and a nearby live
    # SSL objective with enough room after costs. This must not be rejected just
    # because a generic static 2R full-position target is unavailable.
    with instrument_scope(_instrument("BTC", AssetClass.CRYPTO)):
        snap = _snapshot(
            entry=100.0,
            atr=2.0,
            ssl=[_target(97.0, PoolSide.SSL, entry=100.0, atr=2.0, sig=30.0, tf="5m", touches=5)],
            bsl=[_target(103.0, PoolSide.BSL, entry=100.0, atr=2.0, sig=25.0, tf="5m")],
        )
        tp, target, score, report = select_tp_with_report(
            snap, "short", entry=100.0, sl=101.0, atr=2.0,
            min_rr=active_policy().min_rr, posterior_prob=0.90,
        )
        assert tp is not None, report.summary
        assert target.pool.side is PoolSide.SSL
        assert score.rr >= active_policy().tp_durable_rr_floor
        assert score.components["path_support_lift"] >= 0.0


def test_btc_staged_runner_gets_bounded_path_support_but_dead_pools_still_reject():
    with instrument_scope(_instrument("BTC", AssetClass.CRYPTO)):
        snap = _snapshot(
            entry=100.0,
            atr=2.0,
            ssl=[
                _target(96.5, PoolSide.SSL, entry=100.0, atr=2.0, sig=30.0, tf="5m", touches=5),
                _target(88.0, PoolSide.SSL, entry=100.0, atr=2.0, sig=60.0, tf="1h", touches=3),
                _target(91.0, PoolSide.SSL, entry=100.0, atr=2.0, sig=500.0, tf="1h", status=PoolStatus.SWEPT),
            ],
            bsl=[_target(103.0, PoolSide.BSL, entry=100.0, atr=2.0, sig=20.0)],
        )
        scores = score_tp_pools(
            snap, "short", entry=100.0, sl=102.0, atr=2.0,
            min_rr=active_policy().min_rr, posterior_prob=0.90,
        )
        assert scores
        assert all(s.target.pool.status is not PoolStatus.SWEPT for s in scores)
        # At least one deeper objective should see the near objective as path support.
        assert any(s.components.get("path_support_lift", 0.0) > 0.0 for s in scores)
        assert all(s.components.get("path_support_lift", 0.0) <= active_policy().tp_path_support_lift_max + 1e-9 for s in scores)


@pytest.mark.parametrize(
    "asset_id,asset_class,expected_style",
    [
        ("GOLD", AssetClass.COMMODITY, "commodity_staged_displacement"),
        ("SILVER", AssetClass.COMMODITY, "commodity_staged_displacement"),
        ("NVDA", AssetClass.EQUITY, "high_beta_xstock_staged_spread_aware"),
        ("TSLA", AssetClass.EQUITY, "high_beta_xstock_staged_spread_aware"),
        ("COIN", AssetClass.EQUITY, "high_beta_xstock_staged_spread_aware"),
        ("CRCL", AssetClass.EQUITY, "high_beta_xstock_staged_spread_aware"),
        ("AAPL", AssetClass.EQUITY, "large_cap_xstock_staged_clean"),
        ("AMZN", AssetClass.EQUITY, "large_cap_xstock_staged_clean"),
        ("META", AssetClass.EQUITY, "large_cap_xstock_staged_clean"),
        ("GOOGL", AssetClass.EQUITY, "large_cap_xstock_staged_clean"),
        ("SPY", AssetClass.EQUITY, "large_cap_xstock_staged_clean"),
        ("QQQ", AssetClass.EQUITY, "large_cap_xstock_staged_clean"),
    ],
)
def test_every_asset_family_gets_behavior_specific_target_profile(asset_id, asset_class, expected_style):
    with instrument_scope(_instrument(asset_id, asset_class)):
        pol = active_policy()
        assert pol.target_style == expected_style
        assert pol.tp_durable_rr_floor > 0.0
        assert pol.tp_be_move_mult > 0.0

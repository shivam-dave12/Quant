import time
import pytest

from core.instruments import (
    AssetClass,
    ExchangeInstrument,
    ExchangeName,
    TradableInstrument,
    instrument_scope,
)
from strategy.liquidity_map import (
    LiquidityMapSnapshot,
    LiquidityPool,
    PoolSide,
    PoolStatus,
    PoolTarget,
)
from strategy.liquidity_pool_selector import select_sl_with_report, select_tp_with_report


def _instrument(asset_id: str, asset_class: AssetClass) -> TradableInstrument:
    ei = ExchangeInstrument(
        exchange=ExchangeName.DELTA,
        symbol=f"{asset_id}USD",
        ws_symbol=f"{asset_id}USD",
        display_symbol=f"{asset_id}USD",
        asset_id=asset_id,
        asset_class=asset_class,
    )
    return TradableInstrument(
        asset_id=asset_id,
        display_name=asset_id,
        asset_class=asset_class,
        primary_exchange=ExchangeName.DELTA,
        by_exchange={ExchangeName.DELTA: ei},
    )


def _target(price: float, side: PoolSide, *, entry: float = 100.0, atr: float = 2.0,
            tf: str = "5m", touches: int = 8, ob: bool = True, fvg: bool = True,
            htf_count: int = 2, status: PoolStatus = PoolStatus.CONFIRMED,
            sig_boost: float = 0.0) -> PoolTarget:
    now = time.time()
    pool = LiquidityPool(
        price=float(price),
        side=side,
        timeframe=tf,
        status=status,
        touches=touches,
        created_at=now,
        last_touch=now,
        ob_aligned=ob,
        fvg_aligned=fvg,
        htf_count=htf_count,
    )
    distance_atr = abs(float(price) - float(entry)) / max(float(atr), 1e-9)
    return PoolTarget(
        pool=pool,
        distance_atr=distance_atr,
        direction="above" if float(price) > float(entry) else "below",
        significance=float(pool.significance + sig_boost),
        tf_sources=[tf],
    )


def _snapshot(*, bsl=None, ssl=None, nearest_bsl_atr=1.0, nearest_ssl_atr=1.0) -> LiquidityMapSnapshot:
    return LiquidityMapSnapshot(
        bsl_pools=list(bsl or []),
        ssl_pools=list(ssl or []),
        primary_target=None,
        recent_sweeps=[],
        swept_bsl_levels=[],
        swept_ssl_levels=[],
        nearest_bsl_atr=nearest_bsl_atr,
        nearest_ssl_atr=nearest_ssl_atr,
        timestamp=time.time(),
        market_profile={},
    )


def test_btc_full_tp_rejects_micro_pool_and_selects_external_liquidity():
    snap = _snapshot(
        bsl=[
            _target(101.20, PoolSide.BSL, entry=100.0, atr=2.0, tf="5m", sig_boost=50.0),
            _target(108.00, PoolSide.BSL, entry=100.0, atr=2.0, tf="4h", sig_boost=80.0, htf_count=4),
        ],
        ssl=[_target(96.0, PoolSide.SSL, entry=100.0, atr=2.0)],
    )

    with instrument_scope(_instrument("BTC", AssetClass.CRYPTO)):
        tp, target, score, report = select_tp_with_report(
            snap, "long", entry=100.0, sl=99.0, atr=2.0, min_rr=1.5, posterior_prob=0.70
        )

    assert tp is not None
    assert target.pool.price == pytest.approx(108.0)
    assert score.components["asset_id"] == "BTC"
    assert score.components["delivery_prob"] > 0.0
    assert any("too close for BTC full TP" in c["reason"] for c in report.as_dict()["candidates"])


def test_gold_terminal_liquidity_can_pass_when_quality_and_payoff_are_valid():
    snap = _snapshot(
        bsl=[_target(115.0, PoolSide.BSL, entry=100.0, atr=2.0, tf="1d", sig_boost=100.0, htf_count=5)],
        ssl=[_target(96.0, PoolSide.SSL, entry=100.0, atr=2.0)],
    )

    with instrument_scope(_instrument("GOLD", AssetClass.COMMODITY)):
        tp, target, score, report = select_tp_with_report(
            snap, "long", entry=100.0, sl=98.8, atr=2.0, min_rr=2.0, posterior_prob=0.65
        )

    assert tp is not None, report.summary
    assert target.pool.timeframe == "1d"
    assert score.components["asset_id"] == "GOLD"
    assert score.components["delivery_prob"] >= score.components["required_delivery_prob"]


def test_btc_sl_clearance_is_wider_than_equity_for_same_liquidity_cluster():
    snap = _snapshot(
        bsl=[_target(104.0, PoolSide.BSL, entry=100.0, atr=2.0, tf="4h", touches=9, sig_boost=80.0, htf_count=4)],
        ssl=[],
    )

    with instrument_scope(_instrument("BTC", AssetClass.CRYPTO)):
        btc_sl, _, btc_pick, _ = select_sl_with_report(
            snap, "short", entry=100.0, atr=2.0, invalidation_price=101.0,
            max_buffer_atr=2.0, min_risk=1.0,
        )
    with instrument_scope(_instrument("AMZN", AssetClass.EQUITY)):
        eq_sl, _, eq_pick, _ = select_sl_with_report(
            snap, "short", entry=100.0, atr=2.0, invalidation_price=101.0,
            max_buffer_atr=2.0, min_risk=1.0,
        )

    assert btc_pick.buffer_atr > eq_pick.buffer_atr
    assert btc_sl > eq_sl

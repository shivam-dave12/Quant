import math
import time
from types import SimpleNamespace

from strategy.cross_asset_regime import CrossAssetRegimeEngine, CrossAssetState, CrossAssetAdjustment
from strategy.liquidity_pool_selector import _apply_cross_asset_tp_overlay


def test_cross_asset_engine_identifies_silver_catchup_candidate():
    eng = CrossAssetRegimeEngine()
    now = time.time()
    base = [0.0007, 0.0011, 0.0009, 0.0014, 0.0005, 0.0012] * 7
    series = {
        "GOLD":   base + [0.0040],
        "SILVER": [x * 1.2 + 0.00005 for x in base] + [0.0010],
        "BTC":    [0.0002] * (len(base) + 1),
    }
    st = eng._build_state(series, now)
    adj = st.adjustment_for("SILVER", "long")

    assert st.enabled
    assert st.preferred_asset == "SILVER"
    assert st.silver_residual_z < -0.8
    assert adj.enabled
    assert adj.posterior_logit_adjust > 0
    assert adj.tp_aggression > 0
    assert adj.risk_multiplier > 1.0


def test_cross_asset_adjustment_changes_probability_without_hard_veto():
    adj = CrossAssetAdjustment(enabled=True, posterior_logit_adjust=0.16, risk_multiplier=1.12)
    p0 = 0.55
    p1 = adj.adjusted_probability(p0)
    assert p1 > p0
    assert 0.0 < p1 < 1.0


def test_cross_asset_tp_overlay_rewards_farther_sponsored_pool():
    adj = CrossAssetAdjustment(
        enabled=True,
        tp_aggression=0.25,
        posterior_logit_adjust=0.10,
        cluster_risk_penalty=0.0,
        reason="silver lagging gold; catch-up candidate",
    )
    near = SimpleNamespace(distance_atr=1.0, rr=2.0)
    far = SimpleNamespace(distance_atr=4.0, rr=4.0)
    near_ev = _apply_cross_asset_tp_overlay(1.0, near, adj)
    far_ev = _apply_cross_asset_tp_overlay(1.0, far, adj)
    assert far_ev > near_ev
    assert near_ev > 1.0

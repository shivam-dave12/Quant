from types import SimpleNamespace

from strategy.liquidity_fibonacci import score_liquidity_fib_confluence
from strategy.tp_ladder import build_tp_ladder


def _target(side, price, significance=4.0, tf="5m"):
    pool = SimpleNamespace(
        side=side,
        price=float(price),
        status="ACTIVE",
        touches=1,
        timeframe=tf,
        ob_aligned=False,
        fvg_aligned=False,
    )
    return SimpleNamespace(pool=pool, significance=float(significance), distance_atr=1.0, tf_sources=[tf], direction="long" if side == "BSL" else "short")


def test_fibonacci_only_scores_existing_liquidity_and_does_not_create_targets():
    snap = SimpleNamespace(
        ssl_pools=[_target("SSL", 98.0, significance=5.0)],
        bsl_pools=[_target("BSL", 103.236, significance=5.0)],
    )
    # Entry=100, anchor=SL=98, 1.618 extension = 103.236.
    aligned = score_liquidity_fib_confluence(
        snap=snap, side="long", entry=100.0, sl=98.0,
        target_price=103.236, atr=1.0, target=snap.bsl_pools[0],
    )
    off = score_liquidity_fib_confluence(
        snap=snap, side="long", entry=100.0, sl=98.0,
        target_price=104.4, atr=1.0, target=snap.bsl_pools[0],
    )

    assert aligned.score > off.score
    assert aligned.multiplier > off.multiplier
    assert aligned.nearest_ratio == 1.618
    # Still soft: even poor fib geometry is a multiplier, not a veto.
    assert 0.90 <= off.multiplier <= 1.32


def test_tp_ladder_uses_fib_confluence_as_runner_geometry_not_fixed_percent():
    qty = 100.0
    pool_report = {
        "candidates": [
            {"pool_side": "BSL", "tp_price": 101.2, "pool_price": 101.2, "quality": 0.65, "significance": 4.0, "delivery_prob": 0.70, "selection_ev": 0.50, "fib_confluence": 1.22, "fib_score": 0.70, "fib_ratio": 0.618, "fib_role": "internal_monetisation", "timeframe": "5m", "cost_r": 0.04},
            {"pool_side": "BSL", "tp_price": 102.0, "pool_price": 102.0, "quality": 0.68, "significance": 4.5, "delivery_prob": 0.63, "selection_ev": 0.45, "fib_confluence": 1.18, "fib_score": 0.55, "fib_ratio": 1.0, "fib_role": "internal_monetisation", "timeframe": "15m", "cost_r": 0.04},
            {"pool_side": "BSL", "tp_price": 103.236, "pool_price": 103.236, "quality": 0.80, "significance": 6.0, "delivery_prob": 0.45, "selection_ev": 0.40, "fib_confluence": 1.28, "fib_score": 0.82, "fib_ratio": 1.618, "fib_role": "runner_projection", "timeframe": "1h", "selected": True, "cost_r": 0.04},
        ]
    }
    plan = build_tp_ladder(
        side="long", entry=100.0, sl=98.0, final_tp=103.236, atr=1.0,
        total_quantity=qty, pool_report=pool_report,
        min_leg_fraction=1.0 / qty, max_internal_legs=99,
    )

    assert plan.has_internal_targets
    assert plan.final_fraction > 0.0
    assert plan.final_runner_model["fib_score"] >= 0.80
    assert any("liq+Fib final geometry" in n for n in plan.regime_notes)
    # Solvency still dominates: Fib support cannot leave a destructive residual.
    assert plan.solvency_checkpoint_index >= 1
    assert plan.worst_case_after_checkpoint_r >= plan.solvency_floor_r

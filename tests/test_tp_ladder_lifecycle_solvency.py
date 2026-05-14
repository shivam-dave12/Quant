from strategy.tp_ladder import build_tp_ladder


def test_paxg_style_ladder_reduces_final_residual_and_stays_solvent_after_checkpoint():
    entry = 4685.65
    sl = 4676.00
    final_tp = 4705.00
    qty = 101.0
    pool_report = {
        "candidates": [
            {"pool_side": "BSL", "tp_price": 4690.20, "pool_price": 4690.20, "quality": 0.60, "significance": 3.0, "delivery_prob": 0.65, "selection_ev": 0.40, "timeframe": "1m", "cost_r": 0.10},
            {"pool_side": "BSL", "tp_price": 4692.00, "pool_price": 4692.00, "quality": 0.70, "significance": 3.5, "delivery_prob": 0.60, "selection_ev": 0.35, "timeframe": "5m", "cost_r": 0.10},
            {"pool_side": "BSL", "tp_price": 4695.90, "pool_price": 4695.90, "quality": 0.75, "significance": 4.0, "delivery_prob": 0.55, "selection_ev": 0.30, "timeframe": "15m", "cost_r": 0.10},
            {"pool_side": "BSL", "tp_price": final_tp, "pool_price": final_tp, "quality": 0.80, "significance": 5.0, "delivery_prob": 0.40, "selection_ev": 0.25, "timeframe": "1h", "selected": True, "cost_r": 0.10},
        ]
    }
    plan = build_tp_ladder(
        side="long",
        entry=entry,
        sl=sl,
        final_tp=final_tp,
        atr=3.0,
        total_quantity=qty,
        pool_report=pool_report,
        asset_id="GOLD",
        # Lot-derived: 1 contract minimum / 101 total contracts.
        min_leg_fraction=1.0 / qty,
        max_internal_legs=100,
    )

    assert plan.has_internal_targets
    assert plan.solvency_checkpoint_index >= 1
    assert plan.worst_case_after_checkpoint_r >= plan.solvency_floor_r
    # Regression target from the bad lifecycle: old residual was 47/101 = 46.5%.
    # The final runner must be earned and materially smaller in this regime.
    assert plan.final_fraction < 47.0 / 101.0

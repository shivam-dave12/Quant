from telegram.notifier import format_entry_alert, format_exit_alert, format_partial_exit_alert


def test_exit_alert_uses_strategy_aliases_and_keeps_lifecycle_detail():
    msg = format_exit_alert(
        side="long",
        entry=100.0,
        exit_price=110.0,
        qty=1.25,
        residual_qty=0.50,
        partial_qty=0.75,
        gross=12.5,
        fees=0.4,
        pnl=12.1,
        r_realised=2.0,
        mfe_r=2.4,
        planned_rr=2.5,
        margin_pct=12.1,
        margin_used=100.0,
        fee_source="entry exact / exit exact",
        exact_fees=True,
        reason="tp_hit",
    )

    assert "EXIT REPORT" in msg
    assert "ENTRY  $100.0000" in msg
    assert "EXIT   $110.0000" in msg
    assert "START  1.25" in msg
    assert "PART   0.75" in msg
    assert "FINAL  0.5" in msg
    assert "entry exact / exit exact" in msg


def test_partial_exit_alert_uses_fill_aliases():
    msg = format_partial_exit_alert(
        side="short",
        role="TP1",
        fill_price=95.5,
        qty_closed=0.4,
        qty_remaining=0.6,
        gross=2.0,
        fees=0.1,
        net=1.9,
        cumulative_net=1.9,
        sl=101.0,
        final_tp=90.0,
        status="FILLED",
    )

    assert "PARTIAL EXIT" in msg
    assert "TP1" in msg
    assert "FILL   $95.5000" in msg
    assert "CLOSED 0.4" in msg
    assert "LEFT   0.6" in msg
    assert "TOTAL  $+1.9000" in msg


def test_entry_alert_is_sectioned_not_dumped():
    msg = format_entry_alert(
        side="long",
        entry=100.0,
        sl=98.0,
        tp=106.0,
        qty=2.0,
        leverage=5.0,
        rr=3.0,
        context_4h="0.72 bullish",
        context_15m="0.66 bullish",
        raid_label="SSL",
        raid_price=99.0,
        raid_quality=0.8,
        displacement_atr=1.5,
        delivery_score=0.7,
        risk_usd=4.0,
        margin_used=40.0,
        fee_status="broker exact $0.02",
        archetype="LIQUIDITY_RAID_REVERSAL",
    )

    assert "ENTRY TICKET" in msg
    assert "<b>Price Map</b>" in msg
    assert "<b>Size / Risk</b>" in msg
    assert "<b>Structure</b>" in msg
    assert "ENTRY  $100.0000" in msg

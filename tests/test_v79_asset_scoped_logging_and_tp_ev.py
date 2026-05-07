import io
import logging

from core.instruments import (
    AssetClass,
    ExchangeInstrument,
    ExchangeName,
    TradableInstrument,
    instrument_scope,
    install_instrument_log_filter,
)
from strategy.liquidity_pool_selector import PoolScore


def _inst():
    return TradableInstrument(
        asset_id="META",
        display_name="Meta xStock",
        asset_class=AssetClass.EQUITY,
        primary_exchange=ExchangeName.DELTA,
        by_exchange={
            ExchangeName.DELTA: ExchangeInstrument(
                exchange=ExchangeName.DELTA,
                symbol="METAXUSD",
                ws_symbol="METAXUSD",
                display_symbol="METAXUSD",
                asset_id="META",
                asset_class=AssetClass.EQUITY,
            )
        },
    )


def test_v79_asset_context_filter_prefixes_strategy_logs_once():
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter("%(message)s"))
    root = logging.getLogger()
    old_handlers = list(root.handlers)
    old_filters = list(root.filters)
    old_level = root.level
    try:
        root.handlers = [handler]
        root.filters = []
        root.setLevel(logging.INFO)
        install_instrument_log_filter(root)
        with instrument_scope(_inst()):
            logging.getLogger("strategy.entry_engine").info("RAW_TP_AUDIT: example")
            logging.getLogger("strategy.entry_engine").info("[META|DELTA:METAXUSD] already prefixed")
        out = stream.getvalue()
        assert "[META|DELTA:METAXUSD] RAW_TP_AUDIT: example" in out
        assert out.count("[META|DELTA:METAXUSD] already prefixed") == 1
    finally:
        root.handlers = old_handlers
        root.filters = old_filters
        root.setLevel(old_level)


def test_v79_poolscore_repr_uses_delivery_probability_and_ev_r():
    ps = PoolScore(
        target=object(),
        tp_price=616.3,
        distance_atr=6.5,
        rr=1.59,
        sweep_prob=0.139,
        raw_score=0.139,
        confluence=2.18,
        gauntlet_n=0,
        gauntlet_pen=1.0,
        ev=0.800,
        components={
            "delivery_prob": 0.612,
            "expected_value_r": 0.362,
            "selection_ev": 0.491,
        },
    )
    text = repr(ps)
    assert "Pdel=0.61" in text
    assert "EV_R=0.362" in text
    assert "frontier=0.491" in text

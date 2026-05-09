from types import SimpleNamespace

import pytest

from agents.portfolio_cio import PortfolioCIO
from agents.ticker_selection_agent import TickerSelectionAgent
from agents.universe_agent import UniverseAgent
from exchanges.icici.api import BreezeRestClient
from exchanges.icici.token_generator import extract_api_session
from fund.mandate import FundMandate


class _Data:
    def __init__(self, price=100.0, spread=0.02, warm=True):
        self.price = price
        self.spread = spread
        self.warm = warm

    def get_last_price(self):
        return self.price

    def get_orderbook(self):
        return {
            "bids": [[self.price - self.spread / 2, 1000]],
            "asks": [[self.price + self.spread / 2, 1000]],
            "timestamp": 1.0,
        }

    def get_candles(self, timeframe="5m", limit=100):
        n = 100 if self.warm else 8
        return [{"high": 101, "low": 99, "close": 100} for _ in range(n)]


class _Guard:
    max_open_positions = 1

    def can_evaluate_entry(self, ctx, contexts):
        return True, "ok"


def _ctx(asset_id, spread, ready=True):
    inst = SimpleNamespace(
        asset_id=asset_id,
        display_symbol=f"{asset_id}USD",
        primary_exchange=SimpleNamespace(value="delta"),
        asset_class=SimpleNamespace(value="crypto"),
    )
    strat = SimpleNamespace(
        _atr_5m=SimpleNamespace(atr=2.0, get_percentile=lambda: 0.5),
        _last_entry_signal=None,
        _last_entry_readiness=None,
        _last_institutional_decision=None,
        _entry_engine=None,
    )
    return SimpleNamespace(
        instrument=inst,
        data_manager=_Data(spread=spread),
        strategy=strat,
        ready=ready,
        has_position=False,
        phase_name="FLAT",
    )


def _mandate(audit_log_path="data/test_fund_audit.jsonl"):
    return FundMandate(
        enabled=True,
        paper_mode=True,
        top_n_execution_desks=1,
        top_n_depth_scan=2,
        min_ticker_score=0.1,
        min_execution_score=0.1,
        min_warmup_ratio=0.5,
        max_spread_bps_crypto=50.0,
        audit_log_path=str(audit_log_path),
    )


def test_extract_api_session_from_query_fragment_and_bare_fragment():
    assert extract_api_session("https://x/cb?apisession=abc123456789") == "abc123456789"
    assert extract_api_session("https://x/cb#apisession=xyz123456789") == "xyz123456789"
    assert extract_api_session("https://x/cb#baretoken12345") == "baretoken12345"


def test_ticker_selector_prefers_tighter_executable_spread():
    mandate = _mandate()
    universe = UniverseAgent(mandate)
    selector = TickerSelectionAgent(mandate)
    ranked = selector.rank(universe.diagnose_many([_ctx("TIGHT", 0.02), _ctx("WIDE", 0.40)]))
    assert ranked[0].asset_id == "TIGHT"
    assert ranked[0].score > ranked[1].score


def test_portfolio_cio_selects_only_top_execution_desk(tmp_path):
    mandate = _mandate(tmp_path / "audit.jsonl")
    report = PortfolioCIO(mandate).select_execution_queue([_ctx("A", 0.02), _ctx("B", 0.10)], _Guard())
    assert len(report.selected) == 1
    assert report.selected[0].asset_id == "A"
    assert "B" in {r.asset_id for r in report.rejected}


def test_breeze_client_blocks_market_orders_before_network():
    client = BreezeRestClient(auth=SimpleNamespace())
    with pytest.raises(RuntimeError, match="market orders are not permitted"):
        client.place_order(
            stock_code="NIFTY",
            exchange_code="NFO",
            product="options",
            action="buy",
            order_type="market",
            quantity="25",
            price="0",
            validity="day",
        )

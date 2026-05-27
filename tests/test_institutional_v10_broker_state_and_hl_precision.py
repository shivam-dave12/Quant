from types import SimpleNamespace

from execution.collateral_service import BrokerCollateralSnapshotService
from exchanges.hyperliquid.api import HyperliquidAPI
from strategy.institutional_strategy import _hyperliquid_price_increment


class _Manager:
    def __init__(self, venue, symbol, balances):
        self._adapter = SimpleNamespace(symbol=symbol)
        self._balances = list(balances)
        self.calls = 0
    def get_balance(self):
        self.calls += 1
        if not self._balances:
            return {"error": "rate_limited"}
        item = self._balances.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class _Router:
    def __init__(self, managers):
        self.managers = managers
    def available_exchanges(self):
        return tuple(self.managers)
    def manager_for(self, venue):
        return self.managers[venue]


def test_shared_collateral_service_deduplicates_wallet_authority_across_contexts():
    a = _Manager("coinswitch", "BTCUSDT", [{"available": 25.0, "source": "cs"}])
    b = _Manager("coinswitch", "XAGUSDT", [{"available": 25.0, "source": "cs"}])
    service = BrokerCollateralSnapshotService()
    service.register_router(_Router({"coinswitch": a}))
    service.register_router(_Router({"coinswitch": b}))
    assert set(service._sources) == {"coinswitch"}


def test_shared_collateral_service_retains_last_verified_snapshot_during_rate_limit():
    manager = _Manager("hyperliquid", "xyz:SILVER", [
        {"available": 6.72, "source": "verified", "balance_verified": True},
        {"error": "429"},
    ])
    router = _Router({"hyperliquid": manager})
    service = BrokerCollateralSnapshotService()
    service.register_router(router)
    service._refresh("hyperliquid:xyz", "hyperliquid", manager)
    assert service.cash_by_venue(router, {"hyperliquid"})["hyperliquid"] == 6.72
    service._refresh("hyperliquid:xyz", "hyperliquid", manager)
    assert service.cash_by_venue(router, {"hyperliquid"})["hyperliquid"] == 6.72


class _Info:
    def name_to_asset(self, coin):
        return 0
    asset_to_sz_decimals = {0: 5}


def _api():
    api = object.__new__(HyperliquidAPI)
    api.info = _Info()
    return api


def test_hyperliquid_btc_price_respects_official_significant_figure_rule():
    api = _api()
    # BTC perp uses szDecimals=5: at a five-digit dollar price, decimals
    # would exceed the five-significant-figure limit and must be removed.
    assert api.round_price("BTC", 75241.50) == 75242.0
    assert api.round_price("BTC", 75241.49) == 75241.0


def test_hyperliquid_dynamic_tick_geometry_matches_order_precision():
    assert _hyperliquid_price_increment(75241.5, 0.00001) == 1.0
    assert _hyperliquid_price_increment(90.1265, 0.01) == 0.001


def test_hyperliquid_minimum_order_notional_is_enforced_before_route_selection():
    from strategy.institutional_strategy import InstitutionalStrategy
    strategy = InstitutionalStrategy(instrument=None)
    assert strategy._venue_min_order_notional_usd("hyperliquid") == 10.0
    notionals, margins = strategy._venue_selection_budgets({"hyperliquid": 6.7278})
    # Comparison is performed at the smallest executable contract notional
    # only because the broker-local allocated margin supports that minimum.
    assert notionals["hyperliquid"] == 10.0
    assert margins["hyperliquid"] > 0.0


def test_hyperliquid_minimum_route_is_excluded_when_venue_local_margin_allocation_cannot_support_it():
    from strategy.institutional_strategy import InstitutionalStrategy
    strategy = InstitutionalStrategy(instrument=None)
    notionals, _ = strategy._venue_selection_budgets({"hyperliquid": 1.0})
    assert notionals["hyperliquid"] == 0.0


def test_non_hyperliquid_route_has_no_invented_minimum_notional():
    from strategy.institutional_strategy import InstitutionalStrategy
    strategy = InstitutionalStrategy(instrument=None)
    notionals, _ = strategy._venue_selection_budgets({"delta": 6.7278})
    assert notionals["delta"] > 0.0

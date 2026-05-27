from risk.portfolio_exposure import PortfolioExposureTracker


def test_correlated_macro_exposure_accumulates_btc_and_metals_gross_delta():
    tracker = PortfolioExposureTracker()
    tracker.record(asset_id="BTC", position_key="btc", signed_delta_usd=80.0)
    permitted = tracker.evaluate_increment(asset_id="GOLD", position_key="gold", signed_delta_usd=15.0, cap_usd=100.0)
    blocked = tracker.evaluate_increment(asset_id="GOLD", position_key="gold", signed_delta_usd=25.0, cap_usd=100.0)
    assert permitted.approved is True
    assert blocked.approved is False
    assert blocked.bucket == "ANTI_DOLLAR_MACRO"

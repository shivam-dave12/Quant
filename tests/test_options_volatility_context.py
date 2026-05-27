from datetime import datetime, timezone

from agents.indian_options_desk import build_option_volatility_context, compute_vrp, yang_zhang_realized_vol


def test_yang_zhang_and_vrp_are_derived_from_live_ohlc_and_iv():
    candles = [
        {"open": 25000, "high": 25750, "low": 24300, "close": 25300},
        {"open": 25250, "high": 26000, "low": 24600, "close": 24800},
        {"open": 24750, "high": 25800, "low": 24200, "close": 25500},
        {"open": 25500, "high": 26200, "low": 24800, "close": 25000},
    ]
    realised = yang_zhang_realized_vol(candles, window=4)
    assert realised is not None and realised > 0.15
    assert compute_vrp(atm_iv=0.15, realized_vol_yz=realised) < -0.02


def test_option_surface_does_not_invent_signed_dealer_gex_without_position_sign():
    now = datetime(2026, 5, 27, tzinfo=timezone.utc)
    candles = [
        {"open": 25000, "high": 25750, "low": 24300, "close": 25300},
        {"open": 25250, "high": 26000, "low": 24600, "close": 24800},
        {"open": 24750, "high": 25800, "low": 24200, "close": 25500},
        {"open": 25500, "high": 26200, "low": 24800, "close": 25000},
    ]
    chain = [
        {"right": "CE", "strike_price": 25000, "expiry_date": "2026-06-04", "iv": 0.15, "open_interest": 1000},
        {"right": "PE", "strike_price": 25000, "expiry_date": "2026-06-04", "iv": 0.17, "open_interest": 1200},
        {"right": "CE", "strike_price": 25500, "expiry_date": "2026-06-04", "iv": 0.14, "open_interest": 800},
        {"right": "PE", "strike_price": 24500, "expiry_date": "2026-06-04", "iv": 0.18, "open_interest": 900},
        {"right": "CE", "strike_price": 25000, "expiry_date": "2026-06-11", "iv": 0.16, "open_interest": 800},
    ]
    context = build_option_volatility_context(
        chain=chain, underlying_candles=candles, spot=25000.0, lot_size=50, now=now
    )
    assert context.atm_iv == 0.15
    assert context.vrp is not None and context.vrp < -0.02
    assert context.term_slope is not None
    assert context.gross_gamma_exposure is not None and context.gross_gamma_exposure > 0
    assert context.signed_dealer_gex is None
    assert "directed_dealer_gex_requires_position_sign_input" in context.reasons

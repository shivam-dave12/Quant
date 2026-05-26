"""Indian market session guard for Groww F&O runtime."""

from __future__ import annotations

from exchanges.icici.market_session import MarketSessionState, icici_market_session_state


def groww_market_session_state(now=None) -> MarketSessionState:
    return icici_market_session_state(now=now)


def groww_market_is_open() -> bool:
    return groww_market_session_state().is_open

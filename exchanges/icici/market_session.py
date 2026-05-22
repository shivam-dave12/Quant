"""Indian market session guard for ICICI option runtime.

The selector may build option-chain candidates outside market hours, but live
ICICI option runtime must not attempt quote/candle warmup when NSE/BSE F&O is
closed.  This avoids noisy failures on weekends/holidays and keeps selected
contracts dormant until the next tradable session.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo
from typing import Iterable, Any

try:
    import config
except Exception:  # pragma: no cover
    config = None  # type: ignore


IST = ZoneInfo("Asia/Kolkata")


def _cfg(name: str, default: Any) -> Any:
    if config is None:
        return default
    return getattr(config, name, default)


def _parse_hhmm(value: str, default: dtime) -> dtime:
    try:
        hh, mm = str(value).strip().split(":", 1)
        return dtime(int(hh), int(mm), tzinfo=IST)
    except Exception:
        return default


def _holiday_set() -> set[str]:
    raw = _cfg("ICICI_MARKET_HOLIDAYS", ())
    if isinstance(raw, str):
        return {x.strip() for x in raw.split(",") if x.strip()}
    if isinstance(raw, Iterable):
        return {str(x).strip() for x in raw if str(x).strip()}
    return set()


@dataclass(frozen=True)
class MarketSessionState:
    is_open: bool
    reason: str
    now_ist: str


def icici_market_session_state(now: datetime | None = None) -> MarketSessionState:
    """Return whether the ICICI F&O runtime should open live data adapters."""
    if not bool(_cfg("ICICI_MARKET_SESSION_GUARD_ENABLED", True)):
        return MarketSessionState(True, "session guard disabled", datetime.now(IST).isoformat(timespec="seconds"))
    current = (now or datetime.now(IST)).astimezone(IST)
    today = current.date().isoformat()
    if current.weekday() >= 5:
        return MarketSessionState(False, f"Indian F&O market closed: weekend ({today})", current.isoformat(timespec="seconds"))
    if today in _holiday_set():
        return MarketSessionState(False, f"Indian F&O market closed: configured trading holiday ({today})", current.isoformat(timespec="seconds"))
    open_t = _parse_hhmm(str(_cfg("ICICI_MARKET_OPEN_TIME", "09:15")), dtime(9, 15, tzinfo=IST))
    close_t = _parse_hhmm(str(_cfg("ICICI_MARKET_CLOSE_TIME", "15:30")), dtime(15, 30, tzinfo=IST))
    now_t = current.timetz()
    if now_t < open_t:
        return MarketSessionState(False, f"Indian F&O market not open yet: opens {open_t.strftime('%H:%M')} IST", current.isoformat(timespec="seconds"))
    if now_t > close_t:
        return MarketSessionState(False, f"Indian F&O market closed for day: closed {close_t.strftime('%H:%M')} IST", current.isoformat(timespec="seconds"))
    return MarketSessionState(True, "Indian F&O market session open", current.isoformat(timespec="seconds"))


def icici_market_is_open() -> bool:
    return icici_market_session_state().is_open

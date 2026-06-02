from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from .config import AssetProfile


@dataclass(frozen=True)
class SessionState:
    is_open: bool
    reason: str
    local_time: str
    minutes_to_close: float
    session_elapsed_pct: float


SESSION_BY_ASSET: dict[str, dict[str, str]] = {
    "nifty": {"open": "09:15", "close": "15:30", "late": "15:30"},
    "naturalgas": {"open": "09:00", "close": "23:30", "late": "15:30"},
    "crudeoil": {"open": "09:00", "close": "23:30", "late": "15:30"},
}


def _minutes(hhmm: str) -> int:
    hour, minute = [int(part) for part in hhmm.split(":", 1)]
    return hour * 60 + minute


def is_session_open(asset: AssetProfile, now: datetime | None = None) -> SessionState:
    tz = ZoneInfo(asset.session_timezone)
    local = (now or datetime.now(tz)).astimezone(tz)
    open_min = _minutes(asset.session_open)
    close_min = _minutes(asset.session_close)
    minute = local.hour * 60 + local.minute + local.second / 60.0
    duration = max(1.0, close_min - open_min)
    elapsed = (minute - open_min) / duration
    minutes_to_close = close_min - minute

    if local.weekday() not in asset.trading_weekdays:
        return SessionState(False, "outside_trading_weekday", local.isoformat(), minutes_to_close, float(np.clip(elapsed, 0, 1)))
    if minute < open_min:
        return SessionState(False, "before_session_open", local.isoformat(), minutes_to_close, float(np.clip(elapsed, 0, 1)))
    if minute > close_min:
        return SessionState(False, "after_session_close", local.isoformat(), minutes_to_close, float(np.clip(elapsed, 0, 1)))
    return SessionState(True, "open", local.isoformat(), minutes_to_close, float(np.clip(elapsed, 0, 1)))


def add_market_session_features(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "ts" not in df.columns:
        return df
    out = df.copy()
    ts = pd.to_datetime(out["ts"], errors="coerce")
    if getattr(ts.dt, "tz", None) is None:
        ts_ist = ts.dt.tz_localize("UTC", nonexistent="shift_forward", ambiguous="NaT").dt.tz_convert("Asia/Kolkata")
    else:
        ts_ist = ts.dt.tz_convert("Asia/Kolkata")
    minute = ts_ist.dt.hour * 60 + ts_ist.dt.minute + ts_ist.dt.second / 60.0

    asset_ids = out.get("asset_id", pd.Series("nifty", index=out.index)).fillna("nifty").astype(str).str.lower()
    open_min = asset_ids.map(lambda x: _minutes(SESSION_BY_ASSET.get(x, SESSION_BY_ASSET["nifty"])["open"])).astype(float)
    close_min = asset_ids.map(lambda x: _minutes(SESSION_BY_ASSET.get(x, SESSION_BY_ASSET["nifty"])["close"])).astype(float)
    late_min = asset_ids.map(lambda x: _minutes(SESSION_BY_ASSET.get(x, SESSION_BY_ASSET["nifty"])["late"])).astype(float)
    duration = (close_min - open_min).clip(lower=1.0)

    out["local_market_minute"] = minute
    out["session_elapsed_pct"] = ((minute - open_min) / duration).clip(0, 1)
    out["minutes_to_session_close"] = close_min - minute
    out["minutes_since_session_open"] = minute - open_min
    out["is_session_open"] = ((minute >= open_min) & (minute <= close_min)).astype(int)
    out["is_opening_30m"] = ((minute - open_min).between(0, 30)).astype(int)
    out["is_closing_30m"] = ((close_min - minute).between(0, 30)).astype(int)
    out["is_after_equity_close"] = (minute >= _minutes("15:30")).astype(int)
    out["is_late_session"] = ((minute >= late_min) & (minute <= close_min)).astype(int)
    out["commodity_late_session"] = (
        asset_ids.isin(["naturalgas", "crudeoil"])
        & (out["is_after_equity_close"].astype(bool))
        & (out["is_session_open"].astype(bool))
    ).astype(int)
    out["weekday"] = ts_ist.dt.weekday
    return out

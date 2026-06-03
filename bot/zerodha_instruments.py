from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import time

import pandas as pd
import requests

from .config import AssetProfile


@dataclass(frozen=True)
class ZerodhaContract:
    tradingsymbol: str
    exchange: str
    expiry: str
    strike: float
    option_type: str
    tick_size: float
    lot_size: int
    instrument_token: int | None = None


def ensure_zerodha_instruments_csv(
    path: Path,
    url: str,
    max_age_hours: float,
    force: bool = False,
) -> tuple[Path, bool, str]:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not force:
        age_hours = (time.time() - path.stat().st_mtime) / 3600.0
        if age_hours <= max_age_hours and path.stat().st_size > 0:
            return path, False, "exists_fresh"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    if "tradingsymbol" not in resp.text[:512]:
        raise RuntimeError("Zerodha instrument master download did not look like CSV.")
    path.write_text(resp.text, encoding="utf-8")
    return path, True, "downloaded"


def load_zerodha_mcx_options(path: Path, asset: AssetProfile) -> pd.DataFrame:
    data = pd.read_csv(path)
    data["name"] = data["name"].astype(str).str.upper()
    data["exchange"] = data["exchange"].astype(str).str.upper()
    data["instrument_type"] = data["instrument_type"].astype(str).str.upper()
    data["expiry"] = pd.to_datetime(data["expiry"], errors="coerce").dt.date
    data["strike"] = pd.to_numeric(data["strike"], errors="coerce")
    data["tick_size"] = pd.to_numeric(data["tick_size"], errors="coerce")
    data["lot_size"] = pd.to_numeric(data["lot_size"], errors="coerce")
    return data[
        data["exchange"].eq("MCX")
        & data["name"].eq(asset.underlying.upper())
        & data["instrument_type"].isin(["CE", "PE"])
    ].copy()


def _option_type_from_row(row: dict) -> str:
    value = str(row.get("option_type") or "").upper()
    if value in {"CE", "PE"}:
        return value
    symbol = str(row.get("trading_symbol") or "").upper()
    if symbol.endswith("CE"):
        return "CE"
    if symbol.endswith("PE"):
        return "PE"
    return value


def find_zerodha_contract(path: Path, asset: AssetProfile, row: dict) -> ZerodhaContract:
    data = load_zerodha_mcx_options(path, asset)
    expiry = pd.to_datetime(row.get("expiry"), errors="coerce")
    if pd.isna(expiry):
        raise ValueError(f"Cannot map Zerodha contract: missing expiry for {row.get('trading_symbol')}")
    option_type = _option_type_from_row(row)
    strike = float(pd.to_numeric(row.get("strike"), errors="coerce"))
    if not strike or pd.isna(strike):
        raise ValueError(f"Cannot map Zerodha contract: missing strike for {row.get('trading_symbol')}")
    candidates = data[
        data["expiry"].eq(expiry.date())
        & data["instrument_type"].eq(option_type)
    ].copy()
    if candidates.empty:
        raise ValueError(f"No Zerodha {asset.underlying} {option_type} contracts for expiry={expiry.date()}")
    candidates["strike_distance"] = (candidates["strike"] - strike).abs()
    exact = candidates[candidates["strike_distance"].le(1e-9)]
    if exact.empty:
        # Broker strike notation can occasionally differ by decimal scale. Only
        # allow a nearest fallback when it is effectively the same listed strike.
        exact = candidates.sort_values("strike_distance").head(1)
        if float(exact.iloc[0]["strike_distance"]) > max(0.01, abs(strike) * 0.0001):
            raise ValueError(
                f"No exact Zerodha contract for {asset.underlying} expiry={expiry.date()} "
                f"strike={strike:g} type={option_type}; nearest={exact.iloc[0]['tradingsymbol']}"
            )
    rec = exact.iloc[0]
    token = rec.get("instrument_token")
    try:
        instrument_token = int(token)
    except (TypeError, ValueError):
        instrument_token = None
    return ZerodhaContract(
        tradingsymbol=str(rec["tradingsymbol"]),
        exchange=str(rec["exchange"]),
        expiry=str(rec["expiry"]),
        strike=float(rec["strike"]),
        option_type=str(rec["instrument_type"]),
        tick_size=float(rec["tick_size"] or asset.default_tick_size),
        lot_size=max(1, int(float(rec["lot_size"] or 1))),
        instrument_token=instrument_token,
    )

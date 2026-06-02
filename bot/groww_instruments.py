from __future__ import annotations

import time
from pathlib import Path
from urllib.request import Request, urlopen

import pandas as pd

from .config import AssetProfile


DEFAULT_GROWW_INSTRUMENTS_URL = "https://growwapi-assets.groww.in/instruments/instrument.csv"
REQUIRED_OPTION_COLUMNS = [
    "exchange",
    "exchange_token",
    "trading_symbol",
    "groww_symbol",
    "instrument_type",
    "segment",
    "underlying_symbol",
    "expiry_date",
    "strike_price",
    "lot_size",
    "tick_size",
    "buy_allowed",
    "sell_allowed",
]


def _norm_key(value: object) -> str:
    return "".join(ch for ch in str(value or "").upper() if ch.isalnum())


def _is_stale(path: Path, max_age_hours: float) -> bool:
    if max_age_hours <= 0:
        return False
    age_seconds = time.time() - path.stat().st_mtime
    return age_seconds > max_age_hours * 3600.0


def ensure_groww_instruments_csv(
    path: str | Path,
    url: str = DEFAULT_GROWW_INSTRUMENTS_URL,
    max_age_hours: float = 24.0,
    force: bool = False,
) -> tuple[Path, bool, str]:
    path = Path(path)
    if path.exists() and not force and not _is_stale(path, max_age_hours):
        return path, False, "exists_fresh"

    if path.exists() and not force:
        refresh_reason = "stale_refresh"
    elif force:
        refresh_reason = "forced_refresh"
    else:
        refresh_reason = "missing_download"

    path.parent.mkdir(parents=True, exist_ok=True)
    req = Request(url, headers={"User-Agent": "quant-option-bot/1.0"})
    tmp = path.with_name(f"{path.name}.tmp")
    try:
        with urlopen(req, timeout=45) as resp:
            data = resp.read()
        if len(data) < 1024:
            raise ValueError(f"download too small: {len(data)} bytes")
        header = data[:512].decode("utf-8", errors="ignore").lower()
        if "trading_symbol" not in header or "exchange" not in header:
            raise ValueError("download does not look like Groww instrument CSV")
        tmp.write_bytes(data)
        tmp.replace(path)
        return path, True, refresh_reason
    except Exception:
        if tmp.exists():
            tmp.unlink(missing_ok=True)
        if path.exists():
            return path, False, f"{refresh_reason}_failed_keep_existing"
        raise


def _read_instrument_master(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(
            f"Groww instruments CSV not found: {path}. Download it from Groww Instruments docs and save it here."
        )
    df = pd.read_csv(path, low_memory=False)
    cols = {c.lower(): c for c in df.columns}

    def col(name: str):
        return cols.get(name.lower())

    missing = [c for c in REQUIRED_OPTION_COLUMNS if col(c) is None]
    if missing:
        raise ValueError(f"Groww instruments CSV missing columns: {missing}")

    out = df.rename(columns={col(c): c for c in REQUIRED_OPTION_COLUMNS}).copy()
    out["expiry_date"] = pd.to_datetime(out["expiry_date"], errors="coerce").dt.date
    out["strike_price"] = pd.to_numeric(out["strike_price"], errors="coerce")
    out["lot_size"] = pd.to_numeric(out["lot_size"], errors="coerce")
    out["tick_size"] = pd.to_numeric(out["tick_size"], errors="coerce")
    for c in ("buy_allowed", "sell_allowed"):
        raw = out[c]
        num = pd.to_numeric(raw, errors="coerce")
        text_true = raw.astype(str).str.strip().str.lower().isin({"true", "yes", "y", "allowed"})
        out[c] = num.fillna(text_true.astype(int)).fillna(0).astype(int)
    return out


def load_groww_instruments(path: str | Path, underlying: str = "NIFTY") -> pd.DataFrame:
    asset = AssetProfile(
        asset_id=underlying.lower(),
        label=underlying,
        underlying=underlying,
        exchange="NSE",
        segment="FNO",
        expiry_dates=(),
        model_strategy="index_cross_sectional_premium_expansion",
    )
    return load_option_contracts_from_groww_instruments(path, asset)


def load_option_contracts_from_groww_instruments(path: str | Path, asset: AssetProfile) -> pd.DataFrame:
    df = _read_instrument_master(path)
    underlying_key = _norm_key(asset.underlying)
    out = df[
        df["exchange"].astype(str).str.upper().eq(asset.exchange.upper())
        & df["segment"].astype(str).str.upper().eq(asset.segment.upper())
        & df["underlying_symbol"].map(_norm_key).eq(underlying_key)
        & df["instrument_type"].astype(str).str.upper().isin(["CE", "PE"])
        & df["expiry_date"].notna()
        & df["strike_price"].notna()
        & df["trading_symbol"].notna()
    ].copy()
    out["asset_id"] = asset.asset_id
    out["underlying"] = asset.underlying
    out["expiry"] = out["expiry_date"]
    out["strike"] = out["strike_price"]
    out["option_type"] = out["instrument_type"].astype(str).str.upper()
    return out.sort_values(["expiry_date", "strike_price", "instrument_type", "trading_symbol"]).reset_index(drop=True)

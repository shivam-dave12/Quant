from __future__ import annotations

from pathlib import Path
import pandas as pd


def load_groww_instruments(path: str | Path, underlying: str = "NIFTY") -> pd.DataFrame:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(
            f"Groww instruments CSV not found: {path}. Download it from Groww Instruments docs and save it here."
        )
    df = pd.read_csv(path, low_memory=False)
    cols = {c.lower(): c for c in df.columns}
    def col(name: str):
        return cols.get(name.lower())

    required = ["exchange", "exchange_token", "trading_symbol", "groww_symbol", "instrument_type", "segment", "underlying_symbol", "expiry_date", "strike_price", "lot_size", "tick_size", "buy_allowed", "sell_allowed"]
    missing = [c for c in required if col(c) is None]
    if missing:
        raise ValueError(f"Groww instruments CSV missing columns: {missing}")

    out = df[
        df[col("exchange")].astype(str).str.upper().eq("NSE")
        & df[col("segment")].astype(str).str.upper().eq("FNO")
        & df[col("underlying_symbol")].astype(str).str.upper().eq(underlying.upper())
        & df[col("instrument_type")].astype(str).str.upper().isin(["CE", "PE"])
    ].copy()
    out = out.rename(columns={col(c): c for c in required})
    out["expiry_date"] = pd.to_datetime(out["expiry_date"]).dt.date
    out["strike_price"] = pd.to_numeric(out["strike_price"], errors="coerce")
    out["lot_size"] = pd.to_numeric(out["lot_size"], errors="coerce")
    out["tick_size"] = pd.to_numeric(out["tick_size"], errors="coerce")
    out["buy_allowed"] = pd.to_numeric(out["buy_allowed"], errors="coerce").fillna(0).astype(int)
    out["sell_allowed"] = pd.to_numeric(out["sell_allowed"], errors="coerce").fillna(0).astype(int)
    return out.sort_values(["expiry_date", "strike_price", "instrument_type"]).reset_index(drop=True)

from __future__ import annotations

from pathlib import Path
import pandas as pd


def _epoch_to_date(s: pd.Series) -> pd.Series:
    # NSE MII derivative contract files use seconds from 1980-01-01, not Unix 1970.
    return (pd.Timestamp("1980-01-01") + pd.to_timedelta(pd.to_numeric(s, errors="coerce"), unit="s")).dt.date


def _scaled_price(s: pd.Series) -> pd.Series:
    x = pd.to_numeric(s, errors="coerce")
    # Strikes/prices are in paise in the MII file, e.g. 1815000 => 18150.
    return x / 100.0

def _scaled_tick(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce") / 100.0


def load_nifty_options_contracts(path: str | Path, underlying: str = "NIFTY") -> pd.DataFrame:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"NSE contract file not found: {path}")
    df = pd.read_csv(path, compression="infer", low_memory=False)
    underlying = underlying.upper()
    mask = (
        df["FinInstrmNm"].astype(str).str.upper().eq("OPTIDX")
        & df["TckrSymb"].astype(str).str.upper().eq(underlying)
        & df["OptnTp"].astype(str).str.upper().isin(["CE", "PE"])
    )
    if "DelFlg" in df.columns:
        mask &= df["DelFlg"].astype(str).str.upper().ne("Y")
    if "ElgbltyNrmlMkt" in df.columns:
        mask &= pd.to_numeric(df["ElgbltyNrmlMkt"], errors="coerce").fillna(0).astype(int).eq(1)

    out = df.loc[mask].copy()
    if out.empty:
        raise ValueError(f"No {underlying} OPTIDX CE/PE rows found in {path}")

    out["expiry"] = _epoch_to_date(out["XpryDt"])
    out["strike_price"] = _scaled_price(out["StrkPric"])
    out["lot_size"] = pd.to_numeric(out.get("NewBrdLotQty", out.get("MinLot")), errors="coerce")
    out["tick_size"] = _scaled_tick(out.get("BidIntrvl", pd.Series(index=out.index, data=5)))
    out["max_trade_qty"] = pd.to_numeric(out.get("MaxTradQty"), errors="coerce")
    out["trading_symbol"] = out.get("StockNm", out.get("SctyLngNm", "")).astype(str)
    out["exchange_instrument_id"] = pd.to_numeric(out["FinInstrmId"], errors="coerce")
    cols = [
        "exchange_instrument_id",
        "trading_symbol",
        "TckrSymb",
        "expiry",
        "strike_price",
        "OptnTp",
        "lot_size",
        "tick_size",
        "max_trade_qty",
    ]
    out = out[cols].rename(columns={"TckrSymb": "underlying", "OptnTp": "option_type"})
    return out.sort_values(["expiry", "strike_price", "option_type"]).reset_index(drop=True)


def nearest_expiry(df: pd.DataFrame, expiry_index: int = 0):
    expiries = sorted(pd.to_datetime(df["expiry"]).dt.date.unique())
    if not expiries:
        raise ValueError("No expiries available")
    idx = min(max(expiry_index, 0), len(expiries) - 1)
    return expiries[idx]


def select_near_money_universe(df: pd.DataFrame, underlying_ltp: float, expiry, strikes_each_side: int = 12) -> pd.DataFrame:
    exp = pd.to_datetime(expiry).date()
    d = df[pd.to_datetime(df["expiry"]).dt.date.eq(exp)].copy()
    if d.empty:
        raise ValueError(f"No contracts for expiry={exp}")
    strikes = sorted(d["strike_price"].dropna().unique())
    if not strikes:
        raise ValueError("No strikes available")
    atm = min(strikes, key=lambda x: abs(float(x) - float(underlying_ltp)))
    atm_idx = strikes.index(atm)
    keep_strikes = set(strikes[max(0, atm_idx - strikes_each_side): min(len(strikes), atm_idx + strikes_each_side + 1)])
    return d[d["strike_price"].isin(keep_strikes)].copy().reset_index(drop=True)

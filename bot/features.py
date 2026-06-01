from __future__ import annotations

import numpy as np
import pandas as pd

BASE_FEATURES = [
    "ltp", "open_interest", "volume", "delta", "gamma", "theta", "vega", "rho", "iv",
    "moneyness", "abs_moneyness", "extrinsic_proxy", "theta_to_premium", "gamma_theta_ratio",
    "premium_ret_1", "premium_ret_3", "premium_ret_6", "premium_accel",
    "iv_chg_1", "oi_chg_1", "vol_chg_1",
    "volume_rank", "oi_rank", "ltp_rank", "iv_rank", "gamma_theta_rank",
    "ce_pe_ltp_ratio", "same_strike_side_strength", "iv_z", "iv_resid_linear",
]


def _safe_div(a, b):
    return np.where(np.abs(b) > 1e-12, a / b, np.nan)


def build_feature_frame(raw: pd.DataFrame, label_horizon_rows: int = 12, cost_bps: float = 35.0) -> pd.DataFrame:
    if raw.empty:
        return raw.copy()
    df = raw.copy()
    df["ts"] = pd.to_datetime(df["ts"])
    df["expiry"] = pd.to_datetime(df["expiry"]).dt.date
    df = df.sort_values(["trading_symbol", "ts"]).reset_index(drop=True)
    num_cols = ["strike", "underlying_ltp", "ltp", "open_interest", "volume", "delta", "gamma", "theta", "vega", "rho", "iv"]
    for c in num_cols:
        df[c] = pd.to_numeric(df.get(c), errors="coerce")

    df["moneyness"] = _safe_div(df["strike"] - df["underlying_ltp"], df["underlying_ltp"])
    df["abs_moneyness"] = df["moneyness"].abs()
    df["extrinsic_proxy"] = df["ltp"] - np.maximum(0, np.where(df["option_type"].eq("CE"), df["underlying_ltp"] - df["strike"], df["strike"] - df["underlying_ltp"]))
    df["theta_to_premium"] = _safe_div(df["theta"].abs(), df["ltp"])
    df["gamma_theta_ratio"] = _safe_div(df["gamma"], df["theta"].abs())

    g = df.groupby("trading_symbol", group_keys=False)
    df["premium_ret_1"] = g["ltp"].pct_change(1)
    df["premium_ret_3"] = g["ltp"].pct_change(3)
    df["premium_ret_6"] = g["ltp"].pct_change(6)
    df["premium_accel"] = df["premium_ret_1"] - g["premium_ret_1"].shift(1)
    df["iv_chg_1"] = g["iv"].diff(1)
    df["oi_chg_1"] = g["open_interest"].diff(1)
    df["vol_chg_1"] = g["volume"].diff(1)

    # Cross-sectional option-only ranks within each chain snapshot.
    group_cols = ["ts", "expiry", "option_type"]
    for src, dst in [
        ("volume", "volume_rank"),
        ("open_interest", "oi_rank"),
        ("ltp", "ltp_rank"),
        ("iv", "iv_rank"),
        ("gamma_theta_ratio", "gamma_theta_rank"),
    ]:
        df[dst] = df.groupby(group_cols)[src].rank(pct=True)

    # Same-strike CE/PE relative premium strength.
    pivot = df.pivot_table(index=["ts", "expiry", "strike"], columns="option_type", values="ltp", aggfunc="last").reset_index()
    if "CE" in pivot.columns and "PE" in pivot.columns:
        pivot["ce_pe_ltp_ratio"] = _safe_div(pivot["CE"], pivot["PE"])
        df = df.merge(pivot[["ts", "expiry", "strike", "ce_pe_ltp_ratio"]], on=["ts", "expiry", "strike"], how="left")
        df["same_strike_side_strength"] = np.where(df["option_type"].eq("CE"), df["ce_pe_ltp_ratio"], _safe_div(1.0, df["ce_pe_ltp_ratio"]))
    else:
        df["ce_pe_ltp_ratio"] = np.nan
        df["same_strike_side_strength"] = np.nan

    # IV z-score and simple linear residual by timestamp/expiry/side.
    df["iv_z"] = df.groupby(group_cols)["iv"].transform(lambda x: (x - x.mean()) / (x.std(ddof=0) + 1e-9))
    df["iv_resid_linear"] = np.nan
    for _, idx in df.groupby(group_cols).groups.items():
        sub = df.loc[idx]
        ok = sub[["moneyness", "iv"]].dropna()
        if len(ok) >= 5 and ok["moneyness"].nunique() >= 3:
            coef = np.polyfit(ok["moneyness"], ok["iv"], deg=1)
            pred = np.polyval(coef, sub["moneyness"].astype(float))
            df.loc[idx, "iv_resid_linear"] = sub["iv"] - pred

    # Forward label: future executable premium expansion approximation.
    future_ltp = g["ltp"].shift(-label_horizon_rows)
    cost = cost_bps / 10000.0
    df["future_return"] = _safe_div(future_ltp - df["ltp"], df["ltp"]) - cost
    df["future_positive"] = (df["future_return"] > 0).astype(int)

    for c in BASE_FEATURES + ["future_return"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def latest_feature_rows(raw: pd.DataFrame, cfg) -> pd.DataFrame:
    feat = build_feature_frame(raw, label_horizon_rows=cfg.label_horizon_rows, cost_bps=cfg.estimated_round_trip_cost_bps)
    if feat.empty:
        return feat
    latest_ts = feat["ts"].max()
    return feat[feat["ts"].eq(latest_ts)].copy()


def clean_model_matrix(df: pd.DataFrame, feature_cols: list[str] | None = None):
    feature_cols = feature_cols or BASE_FEATURES
    missing = [c for c in feature_cols if c not in df.columns]
    for c in missing:
        df[c] = np.nan
    X = df[feature_cols].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return X.astype(float)

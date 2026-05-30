from __future__ import annotations

import hashlib
import json
import time
import zipfile
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.linear_model import SGDRegressor, LogisticRegression
from sklearn.metrics import mean_squared_error, accuracy_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from .features import FEATURE_COLUMNS


def sha256_file(path: str | Path) -> str:
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_trades(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    if path.suffix == ".zip":
        with zipfile.ZipFile(path) as z:
            names = [n for n in z.namelist() if n.lower().endswith(".csv")]
            if not names:
                raise ValueError("ZIP has no CSV")
            with z.open(names[0]) as f:
                df = pd.read_csv(f)
    else:
        df = pd.read_csv(path)
    required = {"price", "size", "timestamp"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required trade columns: {sorted(missing)}")
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp", "price", "size"]).sort_values("timestamp", kind="stable")
    df["price"] = df["price"].astype(float)
    df["size"] = df["size"].astype(float)
    role = df.get("buyer_role")
    if role is None:
        df["signed_size"] = 0.0
    else:
        r = role.astype(str).str.lower()
        df["signed_size"] = np.where(r.eq("taker"), df["size"], np.where(r.eq("maker"), -df["size"], 0.0))
    return df


def make_1s_features(path: str | Path) -> pd.DataFrame:
    df = _read_trades(path)
    idx = df.set_index("timestamp")
    out = pd.DataFrame(index=pd.date_range(idx.index.min().floor("s"), idx.index.max().ceil("s"), freq="1s", tz="UTC"))
    g = idx.resample("1s")
    out["last_price"] = g["price"].last().ffill()
    out["trade_count"] = g["price"].count().fillna(0)
    out["volume"] = g["size"].sum().fillna(0)
    out["signed_volume"] = g["signed_size"].sum().fillna(0)
    out["imbalance"] = out["signed_volume"] / out["volume"].replace(0, np.nan)
    out["imbalance"] = out["imbalance"].fillna(0)
    out["cvd"] = out["signed_volume"].cumsum()
    for w in [5, 15, 30, 60, 300]:
        vol = out["volume"].rolling(w, min_periods=max(2, w // 3)).sum()
        sig = out["signed_volume"].rolling(w, min_periods=max(2, w // 3)).sum()
        out[f"imb_{w}s"] = (sig / vol.replace(0, np.nan)).fillna(0)
        out[f"signed_vol_{w}s"] = sig.fillna(0)
        out[f"trade_count_{w}s"] = out["trade_count"].rolling(w, min_periods=max(2, w // 3)).sum().fillna(0)
    out["ret_1s_bps"] = np.log(out["last_price"]).diff().fillna(0) * 1e4
    return out.reset_index(names="timestamp")


def inspect_tradeflow_file(path: str | Path) -> dict[str, Any]:
    df = _read_trades(path)
    ts = df["timestamp"]
    return {
        "file": str(path),
        "sha256": sha256_file(path),
        "rows": int(len(df)),
        "columns": list(df.columns),
        "symbol_values": sorted(df["product_symbol"].dropna().astype(str).unique().tolist()) if "product_symbol" in df.columns else [],
        "start": str(ts.min()),
        "end": str(ts.max()),
        "duration_days": float((ts.max() - ts.min()).total_seconds() / 86400),
        "has_buyer_role": "buyer_role" in df.columns,
        "aggressive_buy_rows": int((df.get("buyer_role", pd.Series(dtype=str)).astype(str).str.lower() == "taker").sum()) if "buyer_role" in df.columns else 0,
        "aggressive_sell_rows": int((df.get("buyer_role", pd.Series(dtype=str)).astype(str).str.lower() == "maker").sum()) if "buyer_role" in df.columns else 0,
        "price_min": float(df["price"].min()),
        "price_max": float(df["price"].max()),
        "size_total": float(df["size"].sum()),
        "max_no_trade_gap_seconds": float(ts.diff().dt.total_seconds().max()),
        "classification": "public_trade_orderflow_not_l2_book",
    }


def train_bootstrap_tradeflow(path: str | Path, out_model: str | Path, out_manifest: str | Path, *, horizon_seconds: int = 5, cost_bps: float = 15.8) -> dict[str, Any]:
    feat = make_1s_features(path)
    horizon = int(horizon_seconds)
    feat["future_ret_bps"] = np.log(feat["last_price"].shift(-horizon) / feat["last_price"]) * 1e4
    feat = feat.replace([np.inf, -np.inf], np.nan).dropna().reset_index(drop=True)
    live = pd.DataFrame(index=feat.index)
    live["spread_bps"] = 0.0
    live["imbalance_1"] = 0.0
    live["imbalance_5"] = 0.0
    live["imbalance_10"] = 0.0
    live["imbalance_20"] = 0.0
    live["microprice_dev_bps"] = 0.0
    live["book_slope_bid"] = 0.0
    live["book_slope_ask"] = 0.0
    live["ofi_l1"] = 0.0
    live["trade_count_1s"] = feat["trade_count"]
    live["trade_count_5s"] = feat["trade_count_5s"]
    live["signed_vol_1s"] = feat["signed_volume"]
    live["signed_vol_5s"] = feat["signed_vol_5s"]
    live["signed_vol_15s"] = feat["signed_vol_15s"]
    live["trade_imbalance_1s"] = feat["imbalance"]
    live["trade_imbalance_5s"] = feat["imb_5s"]
    live["trade_imbalance_15s"] = feat["imb_15s"]
    live["cvd_60s"] = feat["signed_vol_60s"]
    live["large_trade_count_5s"] = 0.0
    live["latency_ms"] = 0.0
    live["regime_momentum_1h_14d"] = 0.0
    cols = list(FEATURE_COLUMNS)
    split = int(len(feat) * 0.75)
    train, test = live.iloc[:split], live.iloc[split:]
    train_y, test_y = feat["future_ret_bps"].iloc[:split], feat["future_ret_bps"].iloc[split:]
    reg = Pipeline([("scale", StandardScaler()), ("model", SGDRegressor(loss="huber", penalty="elasticnet", alpha=2e-4, l1_ratio=0.06, average=True, random_state=37, max_iter=30, tol=1e-3))])
    reg.fit(train[cols], train_y)
    pred = reg.predict(test[cols])
    rmse = float(mean_squared_error(test_y, pred) ** 0.5)
    directional_acc = float(((pred > 0) == (test_y.to_numpy() > 0)).mean())
    pseudo = np.where(np.abs(pred) > cost_bps, np.sign(pred) * test_y.to_numpy() - cost_bps, 0.0) / 1e4
    report = {
        "type": "real_tradeflow_bootstrap_return_model_v5",
        "source_file": str(path),
        "source_sha256": sha256_file(path),
        "is_synthetic": False,
        "venue": "DELTA_OR_COMPATIBLE_PUBLIC_TRADES",
        "symbol": "BTCUSD",
        "data_class": "public_trades_only_not_l2",
        "created_at_ns": time.time_ns(),
        "horizon_seconds": horizon,
        "feature_columns": cols,
        "rows": int(len(feat)),
        "train_rows": int(len(train)),
        "test_rows": int(len(test)),
        "test_rmse_bps": rmse,
        "test_directional_accuracy": directional_acc,
        "test_pseudo_net_return": float(np.prod(1.0 + pseudo) - 1.0),
        "test_pseudo_sharpe_per_second": float(np.mean(pseudo) / (np.std(pseudo, ddof=1) or 1e-12) * np.sqrt(365 * 24 * 3600)),
        "live_approved": False,
        "reason_not_live_approved": "bootstrap trained on public trades only; no l2 spread/depth/fill/funding validation",
    }
    out_model = Path(out_model); out_model.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump({**report, "model": reg}, out_model)
    out_manifest = Path(out_manifest); out_manifest.parent.mkdir(parents=True, exist_ok=True)
    out_manifest.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def replay_trades_into_engine(path: str | Path, engine: Any) -> None:
    # Offline trade-only replay is intentionally limited: it populates trade-flow windows and raw capture,
    # but cannot train L2 labels without order-book/mid observations.
    df = _read_trades(path)
    for i, row in df.iterrows():
        msg = {"type": "trades", "sy": row.get("product_symbol", "BTCUSD"), "price": float(row["price"]), "size": float(row["size"]), "buyer_role": row.get("buyer_role"), "timestamp": row["timestamp"].isoformat()}
        engine.on_trade_message(msg, int(row["timestamp"].timestamp() * 1e9))

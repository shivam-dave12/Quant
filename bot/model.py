from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error

from .config import BotConfig
from .features import BASE_FEATURES, build_feature_frame, clean_model_matrix
from .storage import Store

log = logging.getLogger(__name__)


def _make_model():
    try:
        from lightgbm import LGBMRegressor  # type: ignore
        return LGBMRegressor(
            n_estimators=450,
            learning_rate=0.035,
            num_leaves=31,
            subsample=0.9,
            colsample_bytree=0.9,
            random_state=42,
            objective="regression",
        ), "lightgbm.LGBMRegressor"
    except Exception:
        return HistGradientBoostingRegressor(
            max_iter=350,
            learning_rate=0.04,
            l2_regularization=0.05,
            random_state=42,
        ), "sklearn.HistGradientBoostingRegressor"


def _sharpe(x: pd.Series) -> float:
    x = pd.to_numeric(x, errors="coerce").dropna()
    if len(x) < 2 or x.std(ddof=0) == 0:
        return 0.0
    return float(x.mean() / x.std(ddof=0) * np.sqrt(len(x)))


@dataclass
class TrainResult:
    metrics: dict[str, Any]
    model_path: Path
    meta_path: Path


class OptionReturnTrainer:
    def __init__(self, cfg: BotConfig, store: Store):
        self.cfg = cfg
        self.store = store

    def load_training_data(self) -> pd.DataFrame:
        return self.store.query_df(
            """
            SELECT * FROM option_chain_snapshots
            WHERE trading_symbol IS NOT NULL
              AND ltp IS NOT NULL
              AND ltp BETWEEN ? AND ?
              AND volume >= ?
              AND open_interest >= ?
            ORDER BY ts
            """,
            (self.cfg.min_ltp, self.cfg.max_ltp, self.cfg.min_volume, self.cfg.min_oi),
        )

    def train(self) -> TrainResult:
        raw = self.load_training_data()
        if len(raw) < self.cfg.min_train_rows:
            raise ValueError(f"Not enough rows to train: {len(raw)} < {self.cfg.min_train_rows}. Keep collector running.")
        feat = build_feature_frame(raw, self.cfg.label_horizon_rows, self.cfg.estimated_round_trip_cost_bps)
        feat = feat.dropna(subset=["future_return"]).copy()
        feat = feat.sort_values("ts")
        if len(feat) < self.cfg.min_train_rows:
            raise ValueError(f"Not enough labelled rows: {len(feat)} < {self.cfg.min_train_rows}.")

        split = int(len(feat) * (1.0 - self.cfg.test_fraction))
        train_df = feat.iloc[:split].copy()
        test_df = feat.iloc[split:].copy()
        X_train = clean_model_matrix(train_df, BASE_FEATURES)
        y_train = train_df["future_return"].astype(float)
        X_test = clean_model_matrix(test_df, BASE_FEATURES)
        y_test = test_df["future_return"].astype(float)

        model, model_type = _make_model()
        model.fit(X_train, y_train)
        pred = model.predict(X_test)
        test_df["predicted_return"] = pred

        # Simulated contract selector: one top-ranked contract per timestamp.
        eligible = test_df[
            test_df["ltp"].between(self.cfg.min_ltp, self.cfg.max_ltp)
            & (test_df["volume"] >= self.cfg.min_volume)
            & (test_df["open_interest"] >= self.cfg.min_oi)
        ].copy()
        eligible["edge_score"] = eligible["predicted_return"] - self.cfg.uncertainty_buffer
        top = eligible.sort_values(["ts", "edge_score"], ascending=[True, False]).groupby("ts", as_index=False).head(1)
        top = top[top["edge_score"] >= self.cfg.min_edge_return].copy()

        trades = len(top)
        win_rate = float((top["future_return"] > 0).mean()) if trades else 0.0
        mean_return = float(top["future_return"].mean()) if trades else 0.0
        sharpe = _sharpe(top["future_return"]) if trades else 0.0
        universe_mean = float(eligible.groupby("ts")["future_return"].mean().mean()) if not eligible.empty else 0.0
        alpha = mean_return - universe_mean
        mae = float(mean_absolute_error(y_test, pred))
        passed = bool(
            trades >= self.cfg.min_backtest_trades
            and win_rate >= self.cfg.min_model_win_rate
            and sharpe >= self.cfg.min_model_sharpe
            and mean_return > 0
            and alpha > 0
        )
        metrics = {
            "created_at": datetime.utcnow().isoformat(),
            "model_type": model_type,
            "features": BASE_FEATURES,
            "train_rows": int(len(train_df)),
            "test_rows": int(len(test_df)),
            "selector_trades": int(trades),
            "selector_win_rate": win_rate,
            "selector_mean_return": mean_return,
            "selector_sharpe": sharpe,
            "selector_alpha_vs_universe": alpha,
            "test_mae": mae,
            "passed_live_gate": passed,
            "gates": {
                "min_trades": self.cfg.min_backtest_trades,
                "min_win_rate": self.cfg.min_model_win_rate,
                "min_sharpe": self.cfg.min_model_sharpe,
                "mean_return_positive": True,
                "alpha_positive": True,
            },
        }

        artifact = {"model": model, "features": BASE_FEATURES, "metrics": metrics}
        self.cfg.model_path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(artifact, self.cfg.model_path)
        self.cfg.model_meta_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
        self.store.append_df("backtest_metrics", pd.DataFrame([{
            "ts": datetime.utcnow(),
            "model_path": str(self.cfg.model_path),
            "trades": int(trades),
            "win_rate": win_rate,
            "mean_return": mean_return,
            "sharpe": sharpe,
            "alpha_vs_universe": alpha,
            "passed": passed,
            "raw_json": json.dumps(metrics, default=str),
        }]))
        log.info("🧠 model trained | passed=%s win_rate=%.2f sharpe=%.2f alpha=%.5f trades=%s", passed, win_rate, sharpe, alpha, trades)
        return TrainResult(metrics=metrics, model_path=self.cfg.model_path, meta_path=self.cfg.model_meta_path)


def load_model(model_path: str | Path) -> dict[str, Any]:
    return joblib.load(model_path)

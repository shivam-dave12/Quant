"""Trainable prediction models specified by the institutional decision pipeline."""
from __future__ import annotations
from pathlib import Path
from typing import Any
import joblib
import numpy as np
import pandas as pd
from lightgbm import LGBMClassifier, LGBMRegressor
from sklearn.linear_model import Ridge, HuberRegressor, LogisticRegression, QuantileRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline

class PersistableModel:
    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True); joblib.dump(self, path)
    @classmethod
    def load(cls, path: Path) -> Any:
        return joblib.load(path)

class DistributedLagRidge(PersistableModel):
    def __init__(self, lags: int = 5, alpha: float = 1.0) -> None:
        self.lags = lags; self.model = Ridge(alpha=alpha); self.fitted = False
    def _matrix(self, features: np.ndarray) -> np.ndarray:
        x = np.asarray(features, dtype=float)
        if x.ndim != 2 or x.shape[0] <= self.lags: raise ValueError("insufficient lagged observations")
        return np.asarray([x[idx-self.lags:idx+1].reshape(-1) for idx in range(self.lags, x.shape[0])])
    def fit(self, features: np.ndarray, delta_net_return: np.ndarray) -> "DistributedLagRidge":
        x = self._matrix(features); y = np.asarray(delta_net_return, dtype=float)[self.lags:]
        if y.shape[0] != x.shape[0]: raise ValueError("target length mismatch")
        self.model.fit(x, y); self.fitted = True; return self
    def predict(self, recent_features: np.ndarray) -> float:
        if not self.fitted: raise RuntimeError("DISTRIBUTED_LAG_RIDGE_NOT_FITTED")
        x = np.asarray(recent_features, dtype=float)
        if x.shape[0] < self.lags + 1: raise ValueError("lag window incomplete")
        return float(self.model.predict(x[-self.lags-1:].reshape(1, -1))[0])
    def venue_importance(self, feature_names: list[str]) -> dict[str, float]:
        if not self.fitted: raise RuntimeError("DISTRIBUTED_LAG_RIDGE_NOT_FITTED")
        coef = np.abs(self.model.coef_).reshape(self.lags + 1, -1).sum(axis=0)
        return {name: float(value) for name, value in zip(feature_names, coef)}

class KalmanDynamicLinearModel(PersistableModel):
    def __init__(self, n_features: int, process_variance: float = 1e-5, observation_variance: float = 1e-3) -> None:
        self.beta = np.zeros(n_features); self.cov = np.eye(n_features); self.q = process_variance; self.r = observation_variance; self.fitted = False
    def update(self, features: np.ndarray, target: float) -> float:
        x = np.asarray(features, dtype=float).reshape(-1); prior_cov = self.cov + np.eye(x.size) * self.q
        residual = float(target - x @ self.beta); denom = float(x @ prior_cov @ x + self.r)
        gain = prior_cov @ x / max(denom, 1e-12); self.beta = self.beta + gain * residual
        self.cov = (np.eye(x.size) - np.outer(gain, x)) @ prior_cov; self.fitted = True
        return residual
    def predict(self, features: np.ndarray) -> float:
        if not self.fitted: raise RuntimeError("KALMAN_MODEL_NOT_UPDATED")
        return float(np.asarray(features, dtype=float).reshape(-1) @ self.beta)

def _lgbm_frame(x: np.ndarray) -> pd.DataFrame:
    matrix = np.asarray(x, dtype=float)
    if matrix.ndim == 1: matrix = matrix.reshape(1, -1)
    return pd.DataFrame(matrix, columns=[f"f{i}" for i in range(matrix.shape[1])])

class LightGBMProbabilityModel(PersistableModel):
    def __init__(self, *, random_state: int = 7) -> None:
        self.model = LGBMClassifier(n_estimators=120, learning_rate=0.04, max_depth=4, num_leaves=15, random_state=random_state, verbosity=-1, n_jobs=1, deterministic=True)
        self.fitted = False
    def fit(self, x: np.ndarray, y: np.ndarray) -> "LightGBMProbabilityModel":
        y = np.asarray(y, dtype=int)
        if np.unique(y).size < 2: raise ValueError("classifier requires both outcome classes")
        self.model.fit(_lgbm_frame(x), y); self.fitted = True; return self
    def probability(self, x: np.ndarray) -> float:
        if not self.fitted: raise RuntimeError("LIGHTGBM_CLASSIFIER_NOT_FITTED")
        return float(self.model.predict_proba(_lgbm_frame(x))[0, 1])

class LightGBMReturnModel(PersistableModel):
    def __init__(self, *, random_state: int = 7) -> None:
        self.model = LGBMRegressor(n_estimators=120, learning_rate=0.04, max_depth=4, num_leaves=15, random_state=random_state, verbosity=-1, n_jobs=1, deterministic=True)
        self.fitted = False
    def fit(self, x: np.ndarray, y: np.ndarray) -> "LightGBMReturnModel": self.model.fit(_lgbm_frame(x), np.asarray(y, dtype=float)); self.fitted = True; return self
    def predict(self, x: np.ndarray) -> float:
        if not self.fitted: raise RuntimeError("LIGHTGBM_REGRESSOR_NOT_FITTED")
        return float(self.model.predict(_lgbm_frame(x))[0])

class EmpiricalSlippageModel(PersistableModel):
    def __init__(self) -> None: self.model = Pipeline([("scale", StandardScaler()), ("regress", HuberRegressor(max_iter=1000))]); self.fitted = False
    def fit(self, x: np.ndarray, realised_slippage_bps: np.ndarray) -> "EmpiricalSlippageModel": self.model.fit(x, realised_slippage_bps); self.fitted = True; return self
    def predict(self, x: np.ndarray) -> float:
        if not self.fitted: raise RuntimeError("SLIPPAGE_MODEL_NOT_FITTED")
        return float(max(0.0, self.model.predict(np.asarray(x, dtype=float).reshape(1, -1))[0]))

class ExecutionUrgencyModel(PersistableModel):
    def __init__(self) -> None: self.fill_model = LogisticRegression(max_iter=200); self.fitted = False
    def fit(self, x: np.ndarray, passive_filled: np.ndarray) -> "ExecutionUrgencyModel": self.fill_model.fit(x, passive_filled); self.fitted = True; return self
    def choose(self, features: np.ndarray, *, passive_edge_bps: float, aggressive_edge_bps: float, adverse_if_unfilled_bps: float) -> tuple[str, float]:
        if not self.fitted: raise RuntimeError("EXECUTION_URGENCY_MODEL_NOT_FITTED")
        p_fill = float(self.fill_model.predict_proba(np.asarray(features, dtype=float).reshape(1, -1))[0, 1])
        passive_ev = p_fill * passive_edge_bps - (1.0 - p_fill) * adverse_if_unfilled_bps
        if aggressive_edge_bps <= 0 and passive_ev <= 0: return "REJECT", max(passive_ev, aggressive_edge_bps)
        return ("PASSIVE_LIMIT", passive_ev) if passive_ev >= aggressive_edge_bps else ("AGGRESSIVE_LIMIT", aggressive_edge_bps)

class CompetingRiskSurvivalModel(PersistableModel):
    """Cause-specific discrete hazard models for target hit versus stop hit."""
    def __init__(self) -> None:
        self.tp = LogisticRegression(max_iter=300); self.sl = LogisticRegression(max_iter=300); self.fitted = False
    def fit(self, x: np.ndarray, tp_event: np.ndarray, sl_event: np.ndarray) -> "CompetingRiskSurvivalModel":
        self.tp.fit(x, tp_event); self.sl.fit(x, sl_event); self.fitted = True; return self
    def probability_tp_before_sl(self, x: np.ndarray) -> float:
        if not self.fitted: raise RuntimeError("COMPETING_RISK_MODEL_NOT_FITTED")
        row = np.asarray(x, dtype=float).reshape(1, -1); tp = float(self.tp.predict_proba(row)[0, 1]); sl = float(self.sl.predict_proba(row)[0, 1])
        return tp / max(tp + sl, 1e-12)

class ElasticNetDirectionModel(PersistableModel):
    def __init__(self) -> None:
        self.model = LogisticRegression(solver="saga", l1_ratio=0.35, C=1.0, max_iter=2000, random_state=7); self.fitted = False
    def fit(self, x: np.ndarray, y: np.ndarray) -> "ElasticNetDirectionModel": self.model.fit(x, y); self.fitted = True; return self
    def probabilities(self, x: np.ndarray) -> dict[int, float]:
        if not self.fitted: raise RuntimeError("INDIA_DIRECTION_MODEL_NOT_FITTED")
        probs = self.model.predict_proba(np.asarray(x, dtype=float).reshape(1, -1))[0]
        return {int(cls): float(prob) for cls, prob in zip(self.model.classes_, probs)}

class QuantileExpectedMoveModel(PersistableModel):
    def __init__(self, quantile: float = 0.65) -> None: self.model = QuantileRegressor(quantile=quantile, alpha=0.01, solver="highs"); self.fitted = False
    def fit(self, x: np.ndarray, move_points: np.ndarray) -> "QuantileExpectedMoveModel": self.model.fit(x, move_points); self.fitted = True; return self
    def predict(self, x: np.ndarray) -> float:
        if not self.fitted: raise RuntimeError("EXPECTED_MOVE_MODEL_NOT_FITTED")
        return float(max(0.0, self.model.predict(np.asarray(x, dtype=float).reshape(1, -1))[0]))

class FillHazardModel(PersistableModel):
    """Discrete hazard model for probability of fill within execution horizon."""
    def __init__(self) -> None:
        self.model = LogisticRegression(max_iter=400); self.fitted = False
    def fit(self, x: np.ndarray, filled_within_horizon: np.ndarray) -> "FillHazardModel":
        self.model.fit(np.asarray(x, dtype=float), np.asarray(filled_within_horizon, dtype=int)); self.fitted = True; return self
    def probability_fill(self, x: np.ndarray) -> float:
        if not self.fitted: raise RuntimeError("FILL_HAZARD_MODEL_NOT_FITTED")
        return float(self.model.predict_proba(np.asarray(x, dtype=float).reshape(1, -1))[0, 1])

class ExpectedIVChangeModel(PersistableModel):
    """Predicts expected IV change for the planned holding period from volatility/chain state."""
    def __init__(self, *, promoted: bool = False, version: str = "") -> None:
        self.model = LightGBMReturnModel(); self.fitted = False; self.promoted = promoted; self.version = version
    def fit(self, x: np.ndarray, iv_change: np.ndarray) -> "ExpectedIVChangeModel":
        self.model.fit(x, iv_change); self.fitted = True; return self
    def predict(self, x: np.ndarray) -> float:
        if not self.fitted: raise RuntimeError("EXPECTED_IV_CHANGE_MODEL_NOT_FITTED")
        return self.model.predict(x)

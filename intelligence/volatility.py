"""Realised-volatility models: EWMA live state and HAR-RV research challenger."""
from __future__ import annotations
from dataclasses import dataclass
import numpy as np
from sklearn.linear_model import LinearRegression

@dataclass(frozen=True)
class VolatilityEstimate:
    variance: float
    volatility: float
    observations: int

class EWMAVolatility:
    def __init__(self, decay: float = 0.94) -> None:
        if not 0.0 < decay < 1.0: raise ValueError("EWMA decay must be in (0,1)")
        self.decay = decay; self.variance = 0.0; self.observations = 0
    def update(self, log_return: float) -> VolatilityEstimate:
        squared = float(log_return) ** 2
        self.variance = squared if self.observations == 0 else self.decay * self.variance + (1.0 - self.decay) * squared
        self.observations += 1
        return VolatilityEstimate(self.variance, float(np.sqrt(max(self.variance, 0.0))), self.observations)
    def fit_transform(self, log_returns: np.ndarray) -> np.ndarray:
        return np.asarray([self.update(float(value)).volatility for value in np.asarray(log_returns, dtype=float)])

class HARRVModel:
    """HAR-RV on realised variance: daily, weekly and monthly components."""
    def __init__(self) -> None:
        self.model = LinearRegression(); self.fitted = False
    @staticmethod
    def features(realised_variance: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        rv = np.asarray(realised_variance, dtype=float)
        if rv.size < 23: raise ValueError("HAR-RV requires at least 23 realised-variance observations")
        rows, targets = [], []
        for idx in range(22, rv.size - 1):
            rows.append([rv[idx], rv[idx-4:idx+1].mean(), rv[idx-21:idx+1].mean()])
            targets.append(rv[idx + 1])
        if not rows: raise ValueError("HAR-RV requires forecastable observations")
        return np.asarray(rows), np.asarray(targets)
    def fit(self, realised_variance: np.ndarray) -> "HARRVModel":
        x, y = self.features(realised_variance); self.model.fit(x, y); self.fitted = True; return self
    def predict_next(self, realised_variance: np.ndarray) -> float:
        if not self.fitted: raise RuntimeError("HAR_RV_MODEL_NOT_FITTED")
        rv = np.asarray(realised_variance, dtype=float)
        if rv.size < 22: raise ValueError("HAR-RV prediction requires 22 observations")
        features = np.asarray([[rv[-1], rv[-5:].mean(), rv[-22:].mean()]])
        return float(max(0.0, self.model.predict(features)[0]))

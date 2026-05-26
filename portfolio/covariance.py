"""Shrunk EWMA covariance estimator for correlated exposure risk."""
from __future__ import annotations
import numpy as np
from portfolio.exposure import Exposure

class ShrunkEWMACovariance:
    def __init__(self, decay: float = 0.94, shrinkage: float = 0.20) -> None:
        if not 0 < decay < 1 or not 0 <= shrinkage <= 1: raise ValueError("invalid covariance policy")
        self.decay = decay; self.shrinkage = shrinkage; self.instruments: list[str] = []; self.covariance_: np.ndarray | None = None
    def fit(self, returns: dict[str, list[float] | np.ndarray]) -> "ShrunkEWMACovariance":
        self.instruments = sorted(returns); matrix = np.column_stack([np.asarray(returns[name], dtype=float) for name in self.instruments])
        if matrix.shape[0] < 2: raise ValueError("covariance requires returns history")
        weights = (1.0 - self.decay) * self.decay ** np.arange(matrix.shape[0] - 1, -1, -1); weights /= weights.sum()
        mean = (matrix * weights[:, None]).sum(axis=0); centred = matrix - mean
        ewma = (centred * weights[:, None]).T @ centred
        diagonal = np.diag(np.diag(ewma)); self.covariance_ = (1.0 - self.shrinkage) * ewma + self.shrinkage * diagonal
        return self
    def matrix_for(self, instruments: list[str]) -> np.ndarray:
        if self.covariance_ is None: raise RuntimeError("COVARIANCE_MODEL_NOT_FITTED")
        index = {name: i for i, name in enumerate(self.instruments)}
        if any(name not in index for name in instruments): raise RuntimeError("MISSING_INSTRUMENT_COVARIANCE_HISTORY")
        idx = [index[name] for name in instruments]; return self.covariance_[np.ix_(idx, idx)]
    def portfolio_volatility(self, rows: list[Exposure]) -> float:
        if not rows: return 0.0
        covariance = self.matrix_for([x.instrument for x in rows]); weights = np.asarray([x.signed_delta_notional for x in rows], dtype=float)
        return float(np.sqrt(max(0.0, weights @ covariance @ weights)))

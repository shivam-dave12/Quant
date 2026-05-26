"""Historical plus scenario-shock Expected Shortfall portfolio stress model."""
from __future__ import annotations
import numpy as np
from portfolio.exposure import Exposure

class ExpectedShortfallModel:
    def __init__(self, alpha: float = 0.975) -> None:
        if not 0.5 < alpha < 1.0: raise ValueError("ES alpha invalid")
        self.alpha = alpha; self.instruments: list[str] = []; self.scenarios_: np.ndarray | None = None
    def fit(self, returns: dict[str, list[float] | np.ndarray], stress_shocks: dict[str, list[float]] | None = None) -> "ExpectedShortfallModel":
        self.instruments = sorted(returns); historic = np.column_stack([np.asarray(returns[name], dtype=float) for name in self.instruments])
        if historic.shape[0] < 2: raise ValueError("ES requires returns history")
        extra: list[list[float]] = []
        if stress_shocks:
            count = max((len(values) for values in stress_shocks.values()), default=0)
            for idx in range(count): extra.append([float(stress_shocks.get(name, [0.0] * count)[idx]) if idx < len(stress_shocks.get(name, [])) else 0.0 for name in self.instruments])
        self.scenarios_ = np.vstack([historic, np.asarray(extra, dtype=float)]) if extra else historic
        return self
    def expected_shortfall(self, rows: list[Exposure]) -> float:
        if not rows: return 0.0
        if self.scenarios_ is None: raise RuntimeError("EXPECTED_SHORTFALL_MODEL_NOT_FITTED")
        index = {name: idx for idx, name in enumerate(self.instruments)}
        if any(row.instrument not in index for row in rows): raise RuntimeError("MISSING_ES_SCENARIO_INSTRUMENT")
        w = np.zeros(len(self.instruments));
        for row in rows: w[index[row.instrument]] += row.signed_delta_notional
        pnl = self.scenarios_ @ w; loss = -pnl; threshold = np.quantile(loss, self.alpha); tail = loss[loss >= threshold]
        return float(np.mean(tail) if tail.size else threshold)

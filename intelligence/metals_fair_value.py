"""Metals fair-value and local execution model: approved reference, Kalman basis, HMM and after-cost acceptance."""
from __future__ import annotations
from dataclasses import dataclass
import numpy as np
from core.identifiers import CostEstimate, Direction, MetalOpportunity, VenueMicrostate
from intelligence.edge_model import executable_edge
from intelligence.microstructure import execution_quality, venue_feature_row
from intelligence.predictive_models import LightGBMProbabilityModel, LightGBMReturnModel
from intelligence.regime_engine import GaussianHMMRegimeModel

class KalmanBasisTracker:
    def __init__(self, process_variance: float = 1e-4, observation_variance: float = 0.5) -> None:
        self.mean = 0.0; self.variance = 10.0; self.q = process_variance; self.r = observation_variance; self.ready = False
    def update(self, basis_bps: float) -> tuple[float, float, float]:
        prior_var = self.variance + self.q; gain = prior_var / (prior_var + self.r); innovation = basis_bps - self.mean
        self.mean += gain * innovation; self.variance = (1.0 - gain) * prior_var; self.ready = True
        return self.mean, float(np.sqrt(max(self.variance, 0.0))), innovation

@dataclass
class MetalsModelBundle:
    basis_tracker: KalmanBasisTracker
    reference_regime_model: GaussianHMMRegimeModel
    return_model: LightGBMReturnModel
    acceptance_model: LightGBMProbabilityModel
    promoted: bool = False
    version: str = ""
    def evaluate(self, *, asset: str, symbol: str, fair_value: float, local_state: VenueMicrostate, reference_features: np.ndarray,
                 liquidity_features: np.ndarray, cost: CostEstimate) -> MetalOpportunity:
        if not self.promoted: raise RuntimeError("METALS_MODEL_NOT_PROMOTED")
        basis = (local_state.mid / fair_value - 1.0) * 10_000.0
        normal_basis, tracking_error, dislocation = self.basis_tracker.update(basis)
        regime = self.reference_regime_model.assess(reference_features)
        features = np.concatenate([np.asarray(venue_feature_row(local_state)), reference_features, liquidity_features, [basis, normal_basis, tracking_error]])
        predicted = self.return_model.predict(features); acceptance = self.acceptance_model.probability(np.concatenate([features, [predicted]]))
        result = executable_edge(abs(predicted), CostEstimate(cost.fees_bps, cost.spread_bps, cost.slippage_bps, cost.impact_bps,
            cost.protection_cost_bps, abs(dislocation) + tracking_error, cost.uncertainty_bps))
        direction = Direction.LONG.value if predicted > 0 else Direction.SHORT.value if predicted < 0 else Direction.NO_TRADE.value
        rejection = None if acceptance >= 0.5 and result.net_edge_bps > 0 and regime.regime != "illiquid" else "REFERENCE_BASIS_LIQUIDITY_MODEL_REJECTED"
        if rejection: direction = Direction.NO_TRADE.value
        return MetalOpportunity(asset, symbol, fair_value, local_state.mid, basis, tracking_error, regime.regime, acceptance,
            execution_quality(local_state), float(np.mean(liquidity_features)) if liquidity_features.size else 0.0, direction, predicted, result.net_edge_bps, rejection)

class CointegrationKalmanSpreadResearch:
    """Research-only dynamic hedge-ratio tracker for gold/silver relative value context."""
    def __init__(self, process_variance: float = 1e-5, observation_variance: float = 1e-3) -> None:
        self.beta = 1.0; self.intercept = 0.0; self.cov = np.eye(2); self.q = process_variance; self.r = observation_variance
    def update(self, gold_price: float, silver_price: float) -> dict[str, float]:
        if min(gold_price, silver_price) <= 0: raise ValueError("positive gold and silver prices required")
        x = np.asarray([1.0, silver_price]); theta = np.asarray([self.intercept, self.beta]); prior = self.cov + np.eye(2) * self.q
        residual = gold_price - float(x @ theta); gain = prior @ x / float(x @ prior @ x + self.r); theta = theta + gain * residual
        self.cov = (np.eye(2) - np.outer(gain, x)) @ prior; self.intercept, self.beta = float(theta[0]), float(theta[1])
        return {"dynamic_hedge_ratio": self.beta, "spread_residual": residual, "research_only": 1.0}

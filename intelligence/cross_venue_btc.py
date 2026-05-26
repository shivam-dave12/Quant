"""BTC Delta-targeted cross-venue engine with trained leader/follower, regime and acceptance models."""
from __future__ import annotations
from dataclasses import dataclass
import numpy as np
from core.identifiers import BTCCompositeState, CostEstimate, Direction, VenueMicrostate
from intelligence.microstructure import execution_quality, local_flow_score, venue_feature_row
from intelligence.edge_model import executable_edge, EdgeResult
from intelligence.predictive_models import DistributedLagRidge, KalmanDynamicLinearModel, LightGBMProbabilityModel, EmpiricalSlippageModel, ExecutionUrgencyModel
from intelligence.regime_engine import GaussianHMMRegimeModel

REFERENCE_VENUES = ("coinswitch", "hyperliquid")

class BTCCompositeEngine:
    def build(self, delta: VenueMicrostate, references: dict[str, VenueMicrostate]) -> BTCCompositeState:
        usable = {name: state for name, state in references.items() if name in REFERENCE_VENUES and state.feed_quality_score > 0 and state.sequence_valid}
        weights = {name: state.feed_quality_score for name, state in usable.items()}; total = sum(weights.values())
        ref_mid = None if total <= 0 else sum(usable[k].microprice * weight for k, weight in weights.items()) / total
        dislocation = None if ref_mid is None else (delta.microprice / ref_mid - 1.0) * 10_000.0
        all_states = [delta, *usable.values()]; signs = [np.sign(local_flow_score(x)) for x in all_states if abs(local_flow_score(x)) > 1e-12]
        agreement = 0.0 if not signs else float(abs(sum(signs)) / len(signs)) * float(np.sign(sum(signs)))
        mids = np.asarray([state.microprice for state in all_states], dtype=float)
        dispersion = 0.0 if mids.size < 2 else float(np.std(mids) / np.mean(mids) * 10_000.0)
        leader = None; confidence = None
        if usable:
            leader = max(usable, key=lambda venue: abs(local_flow_score(usable[venue])))
            confidence = abs(local_flow_score(usable[leader])) * usable[leader].feed_quality_score
        return BTCCompositeState(delta, usable, ref_mid, dislocation, agreement, dispersion, leader, confidence, execution_quality(delta))
    @staticmethod
    def feature_vector(state: BTCCompositeState) -> np.ndarray:
        rows = venue_feature_row(state.delta_state)
        for venue in REFERENCE_VENUES:
            rows.extend(venue_feature_row(state.reference_states[venue]) if venue in state.reference_states else [0.0] * 10)
        rows.extend([0.0 if state.delta_dislocation_bps is None else state.delta_dislocation_bps, state.flow_agreement_score,
                     state.cross_venue_dispersion_bps, state.delta_execution_quality_score])
        return np.asarray(rows, dtype=float)

@dataclass
class BTCModelBundle:
    leader_model: DistributedLagRidge
    dynamic_leader: KalmanDynamicLinearModel
    regime_model: GaussianHMMRegimeModel
    acceptance_model: LightGBMProbabilityModel
    slippage_model: EmpiricalSlippageModel
    urgency_model: ExecutionUrgencyModel
    promoted: bool = False
    model_version: str = ""
    @staticmethod
    def current_features(composite: BTCCompositeState, ewma_volatility: float) -> np.ndarray:
        base = BTCCompositeEngine.feature_vector(composite)
        return np.concatenate([base, [ewma_volatility]])
    def signed_forecast(self, *, lagged_features: np.ndarray, composite: BTCCompositeState, ewma_volatility: float) -> tuple[float, np.ndarray, float, float]:
        if not self.promoted: raise RuntimeError("BTC_MODEL_BUNDLE_NOT_PROMOTED")
        current = self.current_features(composite, ewma_volatility)
        ridge_prediction = self.leader_model.predict(lagged_features)
        dynamic_prediction = self.dynamic_leader.predict(current)
        return 0.5 * ridge_prediction + 0.5 * dynamic_prediction, current, ridge_prediction, dynamic_prediction
    def predict(self, *, lagged_features: np.ndarray, composite: BTCCompositeState, ewma_volatility: float,
                liquidity_features: np.ndarray, cost_without_slippage: CostEstimate, reward_bps: float, risk_bps: float) -> tuple[Direction, EdgeResult, dict[str, object]]:
        gross, current, ridge_prediction, dynamic_prediction = self.signed_forecast(lagged_features=lagged_features, composite=composite, ewma_volatility=ewma_volatility)
        slippage = self.slippage_model.predict(np.concatenate([current, liquidity_features]))
        cost = CostEstimate(cost_without_slippage.fees_bps, cost_without_slippage.spread_bps, slippage, cost_without_slippage.impact_bps,
                            cost_without_slippage.protection_cost_bps, cost_without_slippage.basis_risk_bps, cost_without_slippage.uncertainty_bps)
        tp_probability = self.acceptance_model.probability(np.concatenate([current, liquidity_features, [gross]]))
        result = executable_edge(abs(gross), cost, tp_probability=tp_probability, reward_bps=reward_bps, risk_bps=risk_bps)
        action, execution_ev = self.urgency_model.choose(np.concatenate([current, liquidity_features]), passive_edge_bps=result.trade_score_bps, aggressive_edge_bps=result.trade_score_bps - cost.spread_bps / 2.0, adverse_if_unfilled_bps=risk_bps * 0.2)
        direction = Direction.LONG if gross > 0 else Direction.SHORT if gross < 0 else Direction.NO_TRADE
        regime = self.regime_model.assess(np.asarray([gross / 10_000.0, ewma_volatility, composite.delta_state.spread_bps, composite.flow_agreement_score, 1-composite.delta_execution_quality_score]))
        metrics: dict[str, object] = {"ridge_net_return_bps": ridge_prediction, "kalman_net_return_bps": dynamic_prediction,
            "predicted_gross_return_bps": gross, "predicted_slippage_bps": slippage, "tp_before_sl_probability": tp_probability,
            "regime": regime.regime, "regime_probability": regime.probability, "execution_action": action, "execution_ev_bps": execution_ev, "model_version": self.model_version}
        return direction, result, metrics

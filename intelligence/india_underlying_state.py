"""NIFTY/BANKNIFTY underlying-first direction, expected move and holding horizon models."""
from __future__ import annotations
from dataclasses import dataclass
import numpy as np
from core.identifiers import Direction, IndiaUnderlyingOpportunity
from intelligence.predictive_models import ElasticNetDirectionModel, QuantileExpectedMoveModel, CompetingRiskSurvivalModel, LightGBMProbabilityModel
from intelligence.regime_engine import GaussianHMMRegimeModel

@dataclass
class IndiaUnderlyingModelBundle:
    regime_model: GaussianHMMRegimeModel
    direction_model: ElasticNetDirectionModel
    challenger_model: LightGBMProbabilityModel | None
    move_model: QuantileExpectedMoveModel
    horizon_model: CompetingRiskSurvivalModel
    promoted: bool = False
    version: str = ""
    def evaluate(self, *, underlying: str, features: np.ndarray, regime_features: np.ndarray, current_level: float,
                 volatility_points: float, liquidity_rationale: dict[str, object]) -> IndiaUnderlyingOpportunity:
        if not self.promoted: raise RuntimeError("INDIA_UNDERLYING_MODEL_NOT_PROMOTED")
        regime = self.regime_model.assess(regime_features); probs = self.direction_model.probabilities(features)
        bull, bear = probs.get(1, 0.0), probs.get(-1, 0.0)
        if self.challenger_model is not None:
            challenger_bull = self.challenger_model.probability(features); bull = 0.5 * bull + 0.5 * challenger_bull; bear = 1.0 - bull
        confidence = max(bull, bear); expected_move = self.move_model.predict(features)
        if confidence < 0.55 or regime.regime in {"illiquid", "shock"}:
            return IndiaUnderlyingOpportunity(underlying, regime.regime, Direction.NO_TRADE.value, expected_move, 0, confidence, current_level, [], liquidity_rationale, "NO_VALID_UNDERLYING_EDGE")
        direction = Direction.BULLISH.value if bull > bear else Direction.BEARISH.value
        horizon_probability = self.horizon_model.probability_tp_before_sl(features)
        holding = 5 if horizon_probability > 0.72 else 15 if horizon_probability > 0.62 else 30 if horizon_probability > 0.53 else 60
        sign = 1.0 if direction == Direction.BULLISH.value else -1.0
        invalidation = current_level - sign * max(expected_move * 0.55, volatility_points)
        targets = [current_level + sign * expected_move * mult for mult in (0.6, 1.0, 1.5)]
        return IndiaUnderlyingOpportunity(underlying, regime.regime, direction, expected_move, holding, confidence, invalidation, targets, liquidity_rationale, None)

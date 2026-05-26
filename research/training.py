"""Real-observation training and promotion pipelines for every live model authority.

This module deliberately refuses fabricated production training.  It builds the exact bundle
artifacts consumed by ``orchestration.model_loader.PromotedAuthorityLoader`` only after
out-of-sample after-cost validation evidence is supplied.
"""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import joblib
import numpy as np
from intelligence.cross_venue_btc import BTCModelBundle
from intelligence.india_underlying_state import IndiaUnderlyingModelBundle
from intelligence.liquidity_intelligence import LiquidityModelBundle, LiquidityZoneScore, ZoneOutcome
from intelligence.metals_fair_value import KalmanBasisTracker, MetalsModelBundle
from intelligence.predictive_models import (
    CompetingRiskSurvivalModel,
    DistributedLagRidge,
    ElasticNetDirectionModel,
    EmpiricalSlippageModel,
    ExecutionUrgencyModel,
    ExpectedIVChangeModel,
    KalmanDynamicLinearModel,
    LightGBMProbabilityModel,
    LightGBMReturnModel,
    QuantileExpectedMoveModel,
)
from intelligence.regime_engine import GaussianHMMRegimeModel
from portfolio.covariance import ShrunkEWMACovariance
from portfolio.expected_shortfall import ExpectedShortfallModel
from research.model_registry import ModelRegistry, ModelVersion

@dataclass(frozen=True)
class ValidationEvidence:
    model_name: str
    version: str
    feature_schema: tuple[str, ...]
    training_observations: int
    validation_after_cost_returns: np.ndarray
    real_observations: bool
    def metric(self) -> float:
        if not self.real_observations:
            raise ValueError("PRODUCTION_TRAINING_REQUIRES_RECORDED_REAL_OBSERVATIONS")
        if self.training_observations <= 0 or self.validation_after_cost_returns.size <= 0:
            raise ValueError("OUT_OF_SAMPLE_AFTER_COST_VALIDATION_REQUIRED")
        return float(np.mean(self.validation_after_cost_returns))

class ArtifactPublisher:
    def __init__(self, model_dir: Path, registry: ModelRegistry) -> None:
        self.model_dir = Path(model_dir); self.model_dir.mkdir(parents=True, exist_ok=True); self.registry = registry
    def publish(self, filename: str, artifact: object, evidence: ValidationEvidence, *, promote: bool) -> ModelVersion:
        metric = evidence.metric(); live = bool(promote and metric > 0)
        path = self.model_dir / filename; joblib.dump(artifact, path)
        entry = ModelVersion(evidence.model_name, evidence.version, str(path), evidence.feature_schema,
            evidence.training_observations, int(evidence.validation_after_cost_returns.size), metric, live, True)
        self.registry.register(entry); return entry

class BTCTrainer:
    def __init__(self, publisher: ArtifactPublisher) -> None: self.publisher = publisher
    def train(self, *, lagged_features: np.ndarray, current_features: np.ndarray, regime_features: np.ndarray,
              liquidity_features: np.ndarray, net_return_labels: np.ndarray, tp_labels: np.ndarray,
              realised_slippage_bps: np.ndarray, passive_filled_labels: np.ndarray,
              evidence: ValidationEvidence, promote: bool = False) -> ModelVersion:
        n = current_features.shape[0]
        if min(n, lagged_features.shape[0], regime_features.shape[0], liquidity_features.shape[0]) < 20:
            raise ValueError("BTC_MODEL_REQUIRES_OBSERVATION_HISTORY")
        leader = DistributedLagRidge(lags=2).fit(lagged_features, net_return_labels)
        dynamic = KalmanDynamicLinearModel(current_features.shape[1])
        for row, label in zip(current_features, net_return_labels): dynamic.update(row, float(label))
        regime = GaussianHMMRegimeModel().fit(regime_features)
        combined = np.column_stack([current_features, liquidity_features])
        acceptance = LightGBMProbabilityModel().fit(np.column_stack([combined, net_return_labels]), tp_labels)
        slippage = EmpiricalSlippageModel().fit(combined, realised_slippage_bps)
        urgency = ExecutionUrgencyModel().fit(combined, passive_filled_labels)
        bundle = BTCModelBundle(leader, dynamic, regime, acceptance, slippage, urgency, promoted=bool(promote and evidence.metric() > 0), model_version=evidence.version)
        return self.publisher.publish("btc_model_bundle.joblib", bundle, evidence, promote=promote)

class LiquidityTrainer:
    def __init__(self, publisher: ArtifactPublisher) -> None: self.publisher = publisher
    def train(self, *, scores: list[LiquidityZoneScore], outcomes: list[ZoneOutcome], evidence: ValidationEvidence, promote: bool = False) -> ModelVersion:
        bundle = LiquidityModelBundle(promoted=bool(promote and evidence.metric() > 0), version=evidence.version).fit(scores, outcomes)
        return self.publisher.publish("liquidity_model_bundle.joblib", bundle, evidence, promote=promote)

class MetalsTrainer:
    def __init__(self, publisher: ArtifactPublisher) -> None: self.publisher = publisher
    def train(self, *, reference_regime_features: np.ndarray, combined_features: np.ndarray, local_net_return_labels: np.ndarray,
              accepted_labels: np.ndarray, evidence: ValidationEvidence, promote: bool = False) -> ModelVersion:
        bundle = MetalsModelBundle(KalmanBasisTracker(), GaussianHMMRegimeModel().fit(reference_regime_features),
            LightGBMReturnModel().fit(combined_features, local_net_return_labels),
            LightGBMProbabilityModel().fit(np.column_stack([combined_features, local_net_return_labels]), accepted_labels),
            promoted=bool(promote and evidence.metric() > 0), version=evidence.version)
        return self.publisher.publish("metals_model_bundle.joblib", bundle, evidence, promote=promote)

class IndiaTrainer:
    def __init__(self, publisher: ArtifactPublisher) -> None: self.publisher = publisher
    def train(self, *, regime_features: np.ndarray, direction_features: np.ndarray, direction_labels: np.ndarray,
              expected_move_labels: np.ndarray, favorable_event_labels: np.ndarray, adverse_event_labels: np.ndarray,
              iv_features: np.ndarray, iv_change_labels: np.ndarray, evidence: ValidationEvidence, promote: bool = False) -> tuple[ModelVersion, ModelVersion]:
        promoted = bool(promote and evidence.metric() > 0)
        india = IndiaUnderlyingModelBundle(GaussianHMMRegimeModel().fit(regime_features),
            ElasticNetDirectionModel().fit(direction_features, direction_labels),
            LightGBMProbabilityModel().fit(direction_features, (direction_labels > 0).astype(int)),
            QuantileExpectedMoveModel().fit(direction_features, expected_move_labels),
            CompetingRiskSurvivalModel().fit(direction_features, favorable_event_labels, adverse_event_labels),
            promoted=promoted, version=evidence.version)
        iv = ExpectedIVChangeModel(promoted=promoted, version=evidence.version).fit(iv_features, iv_change_labels)
        india_entry = self.publisher.publish("india_model_bundle.joblib", india, evidence, promote=promote)
        iv_evidence = ValidationEvidence("option_iv_change", evidence.version, tuple(f"iv_{idx}" for idx in range(iv_features.shape[1])), evidence.training_observations, evidence.validation_after_cost_returns, evidence.real_observations)
        iv_entry = self.publisher.publish("option_iv_change_model.joblib", iv, iv_evidence, promote=promote)
        return india_entry, iv_entry

class RiskTrainer:
    def __init__(self, publisher: ArtifactPublisher) -> None: self.publisher = publisher
    def train(self, *, returns: dict[str, np.ndarray], stress_shocks: dict[str, list[float]], evidence: ValidationEvidence, promote: bool = False) -> tuple[ModelVersion, ModelVersion]:
        covariance = ShrunkEWMACovariance().fit(returns)
        expected_shortfall = ExpectedShortfallModel().fit(returns, stress_shocks)
        covariance_entry = self.publisher.publish("covariance.joblib", covariance, evidence, promote=promote)
        es_evidence = ValidationEvidence("expected_shortfall", evidence.version, tuple(returns), evidence.training_observations, evidence.validation_after_cost_returns, evidence.real_observations)
        es_entry = self.publisher.publish("expected_shortfall.joblib", expected_shortfall, es_evidence, promote=promote)
        return covariance_entry, es_entry

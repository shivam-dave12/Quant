"""Stateful quantitative liquidity pools with trained entry/TP/SL outcome models."""
from __future__ import annotations
from dataclasses import dataclass, replace
from math import exp
import numpy as np
from core.identifiers import LiquidityZoneScore, ProtectionPlan
from intelligence.predictive_models import LightGBMProbabilityModel, CompetingRiskSurvivalModel

@dataclass(frozen=True)
class ZoneObservation:
    price_low: float
    price_high: float
    source_timeframes: list[str]
    age_seconds: float
    touch_count: int
    sweep_count: int
    displayed_liquidity_usd: float
    consumed_liquidity_usd: float
    signed_absorption_usd: float
    historical_significance: float = 0.0
    executed_volume_usd: float = 0.0

@dataclass(frozen=True)
class ZoneOutcome:
    valid_entry: int
    tp_before_sl: int
    sl_before_tp: int
    stop_swept_before_favorable_move: int

class LiquidityPoolBook:
    """Maintains multi-horizon pool history instead of rebuilding only from current L2."""
    def __init__(self) -> None: self._pools: dict[tuple[str, float, float], ZoneObservation] = {}
    def upsert(self, instrument: str, observation: ZoneObservation) -> None: self._pools[(instrument, observation.price_low, observation.price_high)] = observation
    def touch(self, instrument: str, low: float, high: float, *, consumed_usd: float, absorbed_usd: float, swept: bool) -> None:
        key = (instrument, low, high); current = self._pools[key]
        self._pools[key] = replace(current, touch_count=current.touch_count + 1, sweep_count=current.sweep_count + int(swept),
            consumed_liquidity_usd=current.consumed_liquidity_usd + consumed_usd,
            signed_absorption_usd=current.signed_absorption_usd + absorbed_usd,
            executed_volume_usd=current.executed_volume_usd + abs(consumed_usd))
    def zones(self, instrument: str) -> list[ZoneObservation]: return [value for (name, _, _), value in self._pools.items() if name == instrument]

class LiquidityFeatureBuilder:
    @staticmethod
    def raw_score(*, instrument: str, direction: str, current_price: float, volatility_points: float,
                  observation: ZoneObservation, execution_cost_bps: float, cross_market_alignment: float) -> LiquidityZoneScore:
        centre = (observation.price_low + observation.price_high) / 2.0
        distance = abs(centre - current_price) / max(volatility_points, current_price * 1e-8)
        displayed = max(1.0, observation.displayed_liquidity_usd)
        depletion = min(1.0, max(0.0, observation.consumed_liquidity_usd / displayed))
        absorption = max(-1.0, min(1.0, observation.signed_absorption_usd / displayed))
        age_weight = exp(-max(observation.age_seconds, 0.0) / (6.0 * 3600.0))
        higher_horizon = max(observation.historical_significance, min(1.0, len(set(observation.source_timeframes)) / 4.0))
        liquidity = min(1.0, np.sqrt(displayed / 50_000.0)) * age_weight * (0.5 + 0.5 * higher_horizon)
        cost_score = max(0.0, 1.0 - execution_cost_bps / 25.0)
        align = max(-1.0, min(1.0, cross_market_alignment))
        vulnerability = min(1.0, 0.20 * observation.touch_count + 0.30 * observation.sweep_count + 0.35 * depletion + 0.15 * max(0.0, -absorption))
        correct_side = (direction.upper() == "LONG" and centre <= current_price) or (direction.upper() == "SHORT" and centre >= current_price)
        entry = (0.22 * liquidity + 0.22 * max(0.0, absorption) + 0.16 * cost_score + 0.20 * max(0.0, align) + 0.20 * max(0.0, 1.0 - distance/3.0)) if correct_side else 0.0
        tp = (0.30 * liquidity + 0.20 * cost_score + 0.20 * max(0.0, align) + 0.30 * max(0.0, 1.0 - abs(distance-2.0)/4.0)) if not correct_side else 0.0
        sl = (0.40 * (1-vulnerability) + 0.20 * liquidity + 0.20 * cost_score + 0.20 * min(1.0, distance/2.0)) if correct_side else 0.0
        return LiquidityZoneScore(instrument, direction.upper(), observation.price_low, observation.price_high, observation.source_timeframes,
            observation.age_seconds, observation.touch_count, observation.sweep_count, depletion, absorption, liquidity, distance,
            cost_score, align, vulnerability, entry, tp, sl)
    @staticmethod
    def vector(zone: LiquidityZoneScore) -> np.ndarray:
        return np.asarray([zone.age_seconds, zone.touch_count, zone.sweep_count, zone.depletion_score, zone.absorption_score,
            zone.estimated_liquidity_score, zone.volatility_adjusted_distance, zone.execution_cost_score,
            zone.cross_market_alignment_score, zone.stop_vulnerability_score, len(zone.source_timeframes)], dtype=float)

class LiquidityModelBundle:
    def __init__(self, entry_model: LightGBMProbabilityModel | None = None, stop_model: LightGBMProbabilityModel | None = None,
                 target_model: CompetingRiskSurvivalModel | None = None, *, promoted: bool = False, version: str = "") -> None:
        self.entry_model = entry_model or LightGBMProbabilityModel(); self.stop_model = stop_model or LightGBMProbabilityModel()
        self.target_model = target_model or CompetingRiskSurvivalModel(); self.promoted = promoted; self.version = version
    def fit(self, zones: list[LiquidityZoneScore], outcomes: list[ZoneOutcome]) -> "LiquidityModelBundle":
        x = np.vstack([LiquidityFeatureBuilder.vector(zone) for zone in zones])
        self.entry_model.fit(x, np.asarray([o.valid_entry for o in outcomes])); self.stop_model.fit(x, np.asarray([o.stop_swept_before_favorable_move for o in outcomes]))
        self.target_model.fit(x, np.asarray([o.tp_before_sl for o in outcomes]), np.asarray([o.sl_before_tp for o in outcomes])); return self
    def predict_zone(self, zone: LiquidityZoneScore) -> LiquidityZoneScore:
        if not self.promoted: raise RuntimeError("LIQUIDITY_MODEL_NOT_PROMOTED")
        row = LiquidityFeatureBuilder.vector(zone)
        return replace(zone, predicted_valid_entry_probability=self.entry_model.probability(row),
            predicted_tp_before_sl_probability=self.target_model.probability_tp_before_sl(row),
            predicted_stop_sweep_probability=self.stop_model.probability(row))

class LiquidityIntelligence:
    def __init__(self, model_bundle: LiquidityModelBundle | None = None) -> None: self.models = model_bundle
    def score(self, **kwargs) -> LiquidityZoneScore: return LiquidityFeatureBuilder.raw_score(**kwargs)
    def model_score(self, zones: list[LiquidityZoneScore]) -> list[LiquidityZoneScore]:
        if self.models is None: raise RuntimeError("LIQUIDITY_MODELS_REQUIRED")
        return [self.models.predict_zone(zone) for zone in zones]
    def protection_plan(self, *, direction: str, proposed_entry: float, invalidation_price: float, zones: list[LiquidityZoneScore],
                        min_tp_probability: float = 0.53, max_stop_sweep_probability: float = 0.38) -> tuple[ProtectionPlan | None, dict[str, object]]:
        scored = self.model_score(zones)
        entries = [z for z in scored if (z.predicted_valid_entry_probability or 0.0) >= 0.5 and z.entry_utility_score > 0]
        targets = [z for z in scored if (z.predicted_tp_before_sl_probability or 0.0) >= min_tp_probability and z.tp_utility_score > 0]
        stops = [z for z in scored if (z.predicted_stop_sweep_probability or 1.0) <= max_stop_sweep_probability and z.sl_safety_score > 0]
        if not entries or not targets or not stops: return None, {"reason": "NO_MODEL_VALIDATED_PROTECTION_PLAN", "zones": scored}
        entry_zone = max(entries, key=lambda z: (z.predicted_valid_entry_probability or 0.0) * z.entry_utility_score)
        target = max(targets, key=lambda z: (z.predicted_tp_before_sl_probability or 0.0) * z.tp_utility_score)
        stop = max(stops, key=lambda z: (1.0 - (z.predicted_stop_sweep_probability or 1.0)) * z.sl_safety_score)
        entry = proposed_entry if entry_zone.price_low <= proposed_entry <= entry_zone.price_high else (entry_zone.price_low + entry_zone.price_high) / 2.0
        target_price = (target.price_low + target.price_high) / 2.0; stop_price = invalidation_price
        if direction.upper() == "LONG" and not stop_price < entry < target_price: return None, {"reason": "INVALID_LONG_GEOMETRY"}
        if direction.upper() == "SHORT" and not target_price < entry < stop_price: return None, {"reason": "INVALID_SHORT_GEOMETRY"}
        plan = ProtectionPlan(entry, target_price, stop_price, invalidation_price, target_probability=target.predicted_tp_before_sl_probability,
            stop_sweep_probability=stop.predicted_stop_sweep_probability, reason="model-ranked liquidity entry/target/invalidation")
        diagnostics = {"entry_zone": entry_zone, "target_zone": target, "stop_zone": stop, "model_version": self.models.version if self.models else ""}
        return plan, diagnostics

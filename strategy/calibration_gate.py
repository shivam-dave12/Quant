"""Out-of-sample calibration authority for predictive live entries.

The pre-move hazard engine emits an analytic score from observable book/flow
state.  That score is research telemetry until a walk-forward calibrated model
record has been explicitly approved for the exact asset/venue/setup family.
This module prevents hand-weighted scores being mistaken for proven hit rates in
live execution.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping

try:
    import config
except Exception:  # pragma: no cover
    config = None  # type: ignore


def _cfg(name: str, default: Any) -> Any:
    return getattr(config, name, default) if config is not None else default


@dataclass(frozen=True)
class PredictiveCalibrationAssessment:
    authorised_for_live: bool
    authority: str
    model_key: str
    asset_id: str
    venue: str
    setup_family: str
    model_version: str
    sample_count: int
    walk_forward_validated: bool
    brier_score: float | None
    lower_confidence_tp_before_sl: float | None
    reasons: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class PredictiveCalibrationAuthority:
    """Gate live predictive setup families through approved OOS statistics."""

    def __init__(self, asset_id: str) -> None:
        self.asset_id = str(asset_id or "").upper()

    def assess(self, *, venue: str, setup_family: str, model_version: str) -> PredictiveCalibrationAssessment:
        venue_key = str(venue or "").lower()
        family = str(setup_family or "NO_SETUP")
        key = f"{self.asset_id}:{venue_key}:{family}"
        require = bool(_cfg("PREDICTIVE_CALIBRATION_REQUIRE_FOR_LIVE", True))
        registry = _cfg("PREDICTIVE_CALIBRATED_LIVE_MODELS", {}) or {}
        row = registry.get(key) if isinstance(registry, Mapping) else None
        if not require:
            return PredictiveCalibrationAssessment(
                True, "calibration_not_required_by_policy", key, self.asset_id, venue_key, family,
                str(model_version), 0, False, None, None, (),
            )
        if not isinstance(row, Mapping):
            return PredictiveCalibrationAssessment(
                False, "shadow_only_until_walk_forward_calibrated", key, self.asset_id, venue_key,
                family, str(model_version), 0, False, None, None,
                (f"predictive_model_not_calibrated_for_live:{key}",),
            )
        reasons: list[str] = []
        supplied_version = str(row.get("model_version") or "")
        if supplied_version != str(model_version):
            reasons.append(f"calibration_model_version_mismatch:{supplied_version or 'missing'}!={model_version}")
        n = int(row.get("out_of_sample_observations", 0) or 0)
        min_n = int(_cfg("PREDICTIVE_CALIBRATION_MIN_OUT_OF_SAMPLE_OBSERVATIONS", 250) or 250)
        if n < min_n:
            reasons.append(f"calibration_sample_insufficient:{n}<{min_n}")
        walk = bool(row.get("walk_forward_validated", False))
        if not walk:
            reasons.append("calibration_not_walk_forward_validated")
        brier = float(row.get("brier_score")) if row.get("brier_score") is not None else None
        max_brier = float(_cfg("PREDICTIVE_CALIBRATION_MAX_BRIER_SCORE", 0.20) or 0.20)
        if brier is None or brier > max_brier:
            reasons.append(f"calibration_brier_not_acceptable:{brier if brier is not None else 'missing'}>{max_brier}")
        lower = float(row.get("lower_confidence_tp_before_sl")) if row.get("lower_confidence_tp_before_sl") is not None else None
        min_lower_map = _cfg("PREDICTIVE_CALIBRATION_MIN_LOWER_CONFIDENCE_BY_ASSET", {}) or {}
        min_lower = float(min_lower_map.get(self.asset_id, _cfg("PREDICTIVE_CALIBRATION_MIN_LOWER_CONFIDENCE", 0.58))) if isinstance(min_lower_map, Mapping) else float(_cfg("PREDICTIVE_CALIBRATION_MIN_LOWER_CONFIDENCE", 0.58))
        if lower is None or lower < min_lower:
            reasons.append(f"calibration_lower_confidence_insufficient:{lower if lower is not None else 'missing'}<{min_lower}")
        return PredictiveCalibrationAssessment(
            not reasons, "approved_walk_forward_calibration" if not reasons else "shadow_only_calibration_failed",
            key, self.asset_id, venue_key, family, str(model_version), n, walk, brier, lower, tuple(reasons),
        )

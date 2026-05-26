"""Loads only model/risk artefacts whose registry evidence authorises live use."""
from __future__ import annotations
from pathlib import Path
import joblib
from orchestration.platform import ModelAuthority
from portfolio.covariance import ShrunkEWMACovariance
from portfolio.expected_shortfall import ExpectedShortfallModel
from research.model_registry import ModelRegistry

class PromotedAuthorityLoader:
    def __init__(self, model_dir: Path) -> None: self.model_dir = Path(model_dir)
    def load(self) -> tuple[ModelAuthority, ShrunkEWMACovariance, ExpectedShortfallModel]:
        required = {
            "btc_delta_net_edge": "btc_model_bundle.joblib",
            "liquidity": "liquidity_model_bundle.joblib",
            "metals": "metals_model_bundle.joblib",
            "india": "india_model_bundle.joblib",
            "option_iv_change": "option_iv_change_model.joblib",
            "covariance": "covariance.joblib",
            "expected_shortfall": "expected_shortfall.joblib",
        }
        absent = [name for name, filename in required.items() if not (self.model_dir / filename).exists()]
        if absent:
            raise RuntimeError("PROMOTED_MODEL_ARTEFACTS_REQUIRED:" + ",".join(absent))
        registry = ModelRegistry(self.model_dir)
        for name, filename in required.items():
            evidence = registry.promoted(name)
            if Path(evidence.artifact_path).resolve() != (self.model_dir / filename).resolve():
                raise RuntimeError(f"PROMOTED_MODEL_REGISTRY_ARTIFACT_MISMATCH:{name}")
            if not evidence.trained_on_real_observations or evidence.validation_observations <= 0 or evidence.after_cost_metric <= 0:
                raise RuntimeError(f"PROMOTED_MODEL_VALIDATION_EVIDENCE_INVALID:{name}")
        authority = ModelAuthority(joblib.load(self.model_dir / required["btc_delta_net_edge"]), joblib.load(self.model_dir / required["liquidity"]),
            joblib.load(self.model_dir / required["metals"]), joblib.load(self.model_dir / required["india"]), joblib.load(self.model_dir / required["option_iv_change"]))
        if not all(getattr(bundle, "promoted", False) for bundle in (authority.btc, authority.liquidity, authority.metals, authority.india, authority.option_iv)):
            raise RuntimeError("ALL_MODEL_BUNDLES_MUST_BE_PROMOTED")
        return authority, joblib.load(self.model_dir / required["covariance"]), joblib.load(self.model_dir / required["expected_shortfall"])

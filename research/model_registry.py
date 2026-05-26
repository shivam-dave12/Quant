"""Promoted-model registry: live authority requires trained artefacts and validation evidence."""
from __future__ import annotations
from dataclasses import dataclass, asdict
from pathlib import Path
import json

@dataclass(frozen=True)
class ModelVersion:
    name: str
    version: str
    artifact_path: str
    feature_schema: tuple[str, ...]
    training_observations: int
    validation_observations: int
    after_cost_metric: float
    promoted_for_live: bool
    trained_on_real_observations: bool
    notes: str = ""

class ModelRegistry:
    def __init__(self, root: Path) -> None: self.path = Path(root) / "model_registry.json"; self.path.parent.mkdir(parents=True, exist_ok=True)
    def entries(self) -> list[ModelVersion]:
        if not self.path.exists(): return []
        return [ModelVersion(**row) for row in json.loads(self.path.read_text(encoding="utf-8"))]
    def register(self, version: ModelVersion) -> None:
        if version.promoted_for_live and (not version.trained_on_real_observations or version.validation_observations <= 0 or version.after_cost_metric <= 0):
            raise ValueError("LIVE_PROMOTION_REQUIRES_REAL_AFTER_COST_VALIDATION_EVIDENCE")
        rows = [asdict(entry) for entry in self.entries() if not (entry.name == version.name and entry.version == version.version)]
        rows.append(asdict(version)); self.path.write_text(json.dumps(rows, indent=2, sort_keys=True), encoding="utf-8")
    def promoted(self, name: str) -> ModelVersion:
        candidates = [entry for entry in self.entries() if entry.name == name and entry.promoted_for_live]
        if not candidates: raise RuntimeError(f"NO_PROMOTED_MODEL:{name}")
        return max(candidates, key=lambda x: x.validation_observations)

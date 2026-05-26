"""Shadow prediction persistence: models may predict without obtaining live order authority."""
from __future__ import annotations
from dataclasses import asdict, dataclass
from typing import Any
from core.research_store import ResearchStore

@dataclass(frozen=True)
class ShadowPrediction:
    ts_ns: int
    desk: str
    instrument: str
    model_version: str
    signed_predicted_net_return: float
    features_pointer: str
    metrics: dict[str, Any]

class ShadowPredictionRecorder:
    def __init__(self, store: ResearchStore) -> None: self.store = store
    def record(self, prediction: ShadowPrediction) -> None: self.store.append("predictions", asdict(prediction))

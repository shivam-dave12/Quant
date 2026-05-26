"""Append-only decision-time feature storage; stores rejected observations too."""
from __future__ import annotations
import json
from pathlib import Path
from threading import RLock
from typing import Any

class FeatureStore:
    def __init__(self, root: Path) -> None:
        self.path = Path(root) / "features.jsonl"; self.path.parent.mkdir(parents=True, exist_ok=True); self._lock = RLock()
    def append(self, record: dict[str, Any]) -> None:
        with self._lock, self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, default=str, sort_keys=True) + "\n")
    def read_all(self) -> list[dict[str, Any]]:
        if not self.path.exists(): return []
        return [json.loads(row) for row in self.path.read_text(encoding="utf-8").splitlines() if row.strip()]

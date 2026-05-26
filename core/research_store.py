"""Append-only observations, decisions, execution records, predictions and labels."""
from __future__ import annotations
import json
from pathlib import Path
from threading import RLock
from typing import Any

class ResearchStore:
    def __init__(self, root: Path) -> None:
        self.root = Path(root); self.root.mkdir(parents=True, exist_ok=True); self._lock = RLock()
    def append(self, stream: str, value: dict[str, Any]) -> None:
        if stream not in {"observations", "decisions", "executions", "labels", "predictions", "attribution"}:
            raise ValueError("unknown research stream")
        with self._lock, (self.root / f"{stream}.jsonl").open("a", encoding="utf-8") as f:
            f.write(json.dumps(value, default=str, sort_keys=True) + "\n")
    def read(self, stream: str) -> list[dict[str, Any]]:
        p = self.root / f"{stream}.jsonl"
        return [] if not p.exists() else [json.loads(x) for x in p.read_text(encoding="utf-8").splitlines() if x.strip()]

"""Atomic JSON state persistence for protection lifecycle and reconciliation."""
from __future__ import annotations
from dataclasses import asdict, is_dataclass
import json
from pathlib import Path
from threading import RLock
from typing import Any

class StateStore:
    def __init__(self, root: Path) -> None:
        self.root = Path(root); self.root.mkdir(parents=True, exist_ok=True); self._lock = RLock()
    def _path(self, key: str) -> Path:
        safe = "".join(c for c in key if c.isalnum() or c in "-_.")
        if not safe: raise ValueError("state key required")
        return self.root / f"{safe}.json"
    def put(self, key: str, value: Any) -> None:
        payload = asdict(value) if is_dataclass(value) else value
        target = self._path(key); tmp = target.with_suffix(".tmp")
        with self._lock:
            tmp.write_text(json.dumps(payload, default=str, indent=2, sort_keys=True), encoding="utf-8")
            tmp.replace(target)
    def get(self, key: str, default: Any = None) -> Any:
        target = self._path(key)
        if not target.exists(): return default
        with self._lock:
            return json.loads(target.read_text(encoding="utf-8"))

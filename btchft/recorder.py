from __future__ import annotations

import gzip
import json
import queue
import threading
import time
from pathlib import Path
from typing import Any


class AsyncJsonlRecorder:
    """Bounded non-blocking JSONL(.gz) writer for websocket callbacks."""

    def __init__(self, path: str | Path, metadata: dict[str, Any], max_queue: int = 250_000) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.metadata = metadata
        self.q: queue.Queue[dict[str, Any] | None] = queue.Queue(maxsize=max_queue)
        self.dropped = 0
        self.written = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name=f"recorder:{self.path.name}", daemon=True)
        self._thread.start()

    def write(self, row: dict[str, Any]) -> None:
        enriched = {"recorded_at_ns": time.time_ns(), **row}
        try:
            self.q.put_nowait(enriched)
        except queue.Full:
            self.dropped += 1

    def _open(self):
        if self.path.suffix == ".gz":
            return gzip.open(self.path, "at", encoding="utf-8")
        return self.path.open("a", encoding="utf-8")

    def _run(self) -> None:
        new_file = not self.path.exists() or self.path.stat().st_size == 0
        with self._open() as f:
            if new_file:
                f.write(json.dumps({"type": "metadata", **self.metadata}, separators=(",", ":")) + "\n")
            while not self._stop.is_set() or not self.q.empty():
                try:
                    row = self.q.get(timeout=0.25)
                except queue.Empty:
                    continue
                if row is None:
                    continue
                f.write(json.dumps(row, separators=(",", ":"), default=str) + "\n")
                self.written += 1
                if self.written % 1000 == 0:
                    f.flush()

    def close(self) -> None:
        self._stop.set()
        try:
            self.q.put_nowait(None)
        except queue.Full:
            pass
        self._thread.join(timeout=5)

    def stats(self) -> dict[str, Any]:
        return {"path": str(self.path), "written": self.written, "dropped": self.dropped, "queue_size": self.q.qsize()}

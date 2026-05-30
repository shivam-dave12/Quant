from __future__ import annotations

from collections import deque
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any
import json
import math
import time

import joblib
import numpy as np
from sklearn.linear_model import SGDClassifier, SGDRegressor
from sklearn.preprocessing import StandardScaler

from .features import FEATURE_COLUMNS
from .types import BookSnapshot, Side


@dataclass
class PendingLabel:
    ts_ns: int
    horizon_ms: int
    mid: float
    x: np.ndarray
    cost_bps: float
    prediction_bps: float | None


class OnlineReturnModel:
    """Causal online Huber regressor. A sample is learned only after future mid exists."""

    def __init__(self, name: str, feature_columns: list[str] | None = None) -> None:
        self.name = name
        self.columns = list(feature_columns or FEATURE_COLUMNS)
        self.scaler = StandardScaler()
        self.reg = SGDRegressor(loss="huber", penalty="elasticnet", alpha=2e-4, l1_ratio=0.08, average=True, random_state=17)
        self.initialized = False
        self.samples = 0
        self.residual_sq = 0.0

    def predict(self, x: np.ndarray) -> float | None:
        if not self.initialized:
            return None
        return float(self.reg.predict(self.scaler.transform(x.reshape(1, -1)))[0])

    def update(self, x: np.ndarray, y_bps: float) -> None:
        if self.initialized:
            yhat = self.predict(x)
            if yhat is not None:
                self.residual_sq += float((y_bps - yhat) ** 2)
        else:
            self.residual_sq += float(y_bps ** 2)
        xx = x.reshape(1, -1)
        self.scaler.partial_fit(xx)
        self.reg.partial_fit(self.scaler.transform(xx), np.asarray([float(y_bps)]))
        self.initialized = True
        self.samples += 1

    @property
    def residual_std_bps(self) -> float:
        return float(math.sqrt(self.residual_sq / max(1, self.samples - 1)))

    def save(self, path: str | Path, extra: dict[str, Any] | None = None) -> None:
        path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump({
            "type": "real_online_l2_tradeflow_return_model_v5",
            "name": self.name,
            "feature_columns": self.columns,
            "samples": self.samples,
            "residual_std_bps": self.residual_std_bps,
            "saved_at_ns": time.time_ns(),
            "model": self,
            **(extra or {}),
        }, path)


class OnlineEventClassifier:
    """Three-class event classifier: short/neutral/long beyond cost hurdle."""

    CLASSES = np.asarray([0, 1, 2])  # 0 short, 1 neutral, 2 long

    def __init__(self, name: str, feature_columns: list[str] | None = None) -> None:
        self.name = name
        self.columns = list(feature_columns or FEATURE_COLUMNS)
        self.scaler = StandardScaler()
        self.clf = SGDClassifier(loss="log_loss", penalty="elasticnet", alpha=1e-4, l1_ratio=0.05, average=True, random_state=29)
        self.initialized = False
        self.samples = 0
        self.correct = 0

    @staticmethod
    def label_from_move(move_bps: float, cost_bps: float) -> int:
        if move_bps > cost_bps:
            return 2
        if move_bps < -cost_bps:
            return 0
        return 1

    def predict_proba(self, x: np.ndarray) -> np.ndarray | None:
        if not self.initialized:
            return None
        return self.clf.predict_proba(self.scaler.transform(x.reshape(1, -1)))[0]

    def update(self, x: np.ndarray, y: int) -> None:
        xx = x.reshape(1, -1)
        if self.initialized:
            pred = int(self.clf.predict(self.scaler.transform(xx))[0])
            self.correct += int(pred == int(y))
        self.scaler.partial_fit(xx)
        self.clf.partial_fit(self.scaler.transform(xx), np.asarray([int(y)]), classes=self.CLASSES)
        self.initialized = True
        self.samples += 1

    @property
    def accuracy(self) -> float:
        return self.correct / max(self.samples - 1, 1)


class LiveLearningModelStack:
    """Multi-horizon model stack. It never trains on synthetic rows."""

    def __init__(self, horizons_ms: tuple[int, ...], *, min_labels_to_score: int, model_dir: str | Path, auto_promote: bool = True) -> None:
        self.horizons_ms = tuple(sorted(int(h) for h in horizons_ms))
        self.min_labels_to_score = int(min_labels_to_score)
        self.model_dir = Path(model_dir); self.model_dir.mkdir(parents=True, exist_ok=True)
        self.auto_promote = auto_promote
        self.regressors = {h: OnlineReturnModel(f"return_{h}ms") for h in self.horizons_ms}
        self.classifiers = {h: OnlineEventClassifier(f"event_{h}ms") for h in self.horizons_ms}
        self.pending: deque[PendingLabel] = deque()
        self.prequential: deque[float] = deque(maxlen=25000)
        self.promoted_horizon_ms: int | None = None
        self.matured_labels = 0
        self.last_checkpoint_labels = 0
        self.bootstrap_manifest: dict[str, Any] | None = None
        self.bootstrap_model: Any | None = None

    def maybe_load_bootstrap(self, model_path: str | Path, manifest_path: str | Path) -> None:
        model_path = Path(model_path); manifest_path = Path(manifest_path)
        if not model_path.exists() or not manifest_path.exists():
            return
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("is_synthetic", True):
            raise RuntimeError("Bootstrap model rejected: synthetic provenance flag is true/missing")
        artifact = joblib.load(model_path)
        if artifact.get("type") != "real_tradeflow_bootstrap_return_model_v5":
            raise RuntimeError("Bootstrap model rejected: unexpected artifact type")
        if artifact.get("feature_columns") != FEATURE_COLUMNS:
            raise RuntimeError("Bootstrap model rejected: feature schema mismatch")
        # This is intentionally not a live champion. It seeds shadow scoring only.
        self.bootstrap_model = artifact["model"]
        self.bootstrap_manifest = manifest

    def observe_decision(self, ts_ns: int, mid: float, feature_row: dict[str, float], cost_bps: float) -> dict[str, Any]:
        x = np.asarray([feature_row[c] for c in FEATURE_COLUMNS], dtype=float)
        out: dict[str, Any] = {"matured_labels": self.matured_labels, "per_horizon": {}}
        for h in self.horizons_ms:
            pred = self.regressors[h].predict(x)
            out["per_horizon"][str(h)] = {"predicted_move_bps": pred, "samples": self.regressors[h].samples, "residual_std_bps": self.regressors[h].residual_std_bps}
            self.pending.append(PendingLabel(ts_ns=ts_ns, horizon_ms=h, mid=float(mid), x=x.copy(), cost_bps=float(cost_bps), prediction_bps=pred))
        signal = self._signal_from_predictions(out, cost_bps)
        return {**out, "signal": signal}

    def learn_matured(self, now_ns: int, current_mid: float) -> None:
        while self.pending and now_ns - self.pending[0].ts_ns >= self.pending[0].horizon_ms * 1_000_000:
            p = self.pending.popleft()
            if p.mid <= 0:
                continue
            move = (float(current_mid) / p.mid - 1.0) * 1e4
            if p.prediction_bps is not None:
                eligible = abs(p.prediction_bps) > p.cost_bps
                signed = np.sign(p.prediction_bps) * move
                self.prequential.append((signed - p.cost_bps) / 1e4 if eligible else 0.0)
            self.regressors[p.horizon_ms].update(p.x, move)
            self.classifiers[p.horizon_ms].update(p.x, OnlineEventClassifier.label_from_move(move, p.cost_bps))
            self.matured_labels += 1
        if self.matured_labels - self.last_checkpoint_labels >= 5000:
            self.checkpoint()
        if self.auto_promote:
            self._maybe_promote()

    def _signal_from_predictions(self, pred_bundle: dict[str, Any], cost_bps: float) -> dict[str, Any] | None:
        best: tuple[int, float] | None = None
        for h, reg in self.regressors.items():
            row = pred_bundle["per_horizon"][str(h)]
            pred = row["predicted_move_bps"]
            if pred is None or reg.samples < self.min_labels_to_score:
                continue
            edge = abs(float(pred)) - float(cost_bps)
            if best is None or edge > best[1]:
                best = (h, edge)
        if best is None:
            # Cold-start shadow score from real public-trade bootstrap model. It is not live-approved.
            if self.bootstrap_model is None:
                return None
            x = None
            # latest decision x is the newest pending item; all horizons append the same vector.
            if self.pending:
                x = self.pending[-1].x.reshape(1, -1)
            if x is None:
                return None
            pred = float(self.bootstrap_model.predict(x)[0])
            edge = abs(pred) - float(cost_bps)
            if edge <= 0:
                return None
            side = "long" if pred > 0 else "short" if pred < 0 else "flat"
            return {"horizon_ms": "bootstrap_tradeflow_5s", "side": side, "expected_net_edge_bps": edge, "forecast_move_bps": pred, "confidence": 0.55, "not_live_approved": True}
        h, edge = best
        pred = float(pred_bundle["per_horizon"][str(h)]["predicted_move_bps"])
        side = "long" if pred > 0 else "short" if pred < 0 else "flat"
        std = max(self.regressors[h].residual_std_bps, 1e-9)
        confidence = float(min(0.995, max(0.5, 0.5 + min(abs(pred) / (4 * std), 0.495))))
        return {"horizon_ms": h, "side": side, "expected_net_edge_bps": edge, "forecast_move_bps": pred, "confidence": confidence}

    def _maybe_promote(self) -> None:
        if len(self.prequential) < 5000:
            return
        arr = np.asarray(self.prequential, dtype=float)
        mean = float(arr.mean()); std = float(arr.std(ddof=1) or 1e-12)
        sharpe_like = mean / std * math.sqrt(365 * 24 * 3600)  # per-decision approximation, diagnostic only
        if mean > 0 and sharpe_like > 1.0:
            # choose horizon with most labels and best absolute edge capability
            self.promoted_horizon_ms = max(self.horizons_ms, key=lambda h: self.regressors[h].samples)

    @property
    def promoted(self) -> bool:
        return self.promoted_horizon_ms is not None

    def checkpoint(self) -> None:
        for h, model in self.regressors.items():
            model.save(self.model_dir / f"return_{h}ms_shadow.joblib", {"horizon_ms": h, "matured_labels": self.matured_labels, "live_promoted_shadow": self.promoted_horizon_ms == h})
        (self.model_dir / "model_stack_state.json").write_text(json.dumps(self.status(), indent=2), encoding="utf-8")
        self.last_checkpoint_labels = self.matured_labels

    def status(self) -> dict[str, Any]:
        return {
            "type": "real_live_learning_model_stack_v5",
            "feature_columns": FEATURE_COLUMNS,
            "matured_labels": self.matured_labels,
            "pending_labels": len(self.pending),
            "promoted_horizon_ms": self.promoted_horizon_ms,
            "bootstrap_manifest_loaded": self.bootstrap_manifest,
            "regressors": {str(h): {"samples": m.samples, "residual_std_bps": m.residual_std_bps} for h, m in self.regressors.items()},
            "classifiers": {str(h): {"samples": m.samples, "accuracy": m.accuracy} for h, m in self.classifiers.items()},
            "prequential_count": len(self.prequential),
            "prequential_mean_return": float(np.mean(self.prequential)) if self.prequential else 0.0,
        }

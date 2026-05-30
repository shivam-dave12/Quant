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

    def _promotion_diagnostics(self) -> dict[str, Any]:
        arr = np.asarray(self.prequential, dtype=float) if self.prequential else np.asarray([], dtype=float)
        mean = float(arr.mean()) if len(arr) else 0.0
        std = float(arr.std(ddof=1)) if len(arr) > 1 else 0.0
        sharpe_like = float(mean / (std or 1e-12) * math.sqrt(365 * 24 * 3600)) if len(arr) > 1 else 0.0
        return {
            "prequential_evals": int(len(self.prequential)),
            "prequential_mean_return": mean,
            "prequential_sharpe_like": sharpe_like,
            "required_min_evals": 5000,
            "required_positive_mean": True,
            "required_sharpe_like_gt": 1.0,
            "passed": bool(len(self.prequential) >= 5000 and mean > 0 and sharpe_like > 1.0),
        }

    def _maybe_promote(self) -> None:
        d = self._promotion_diagnostics()
        if not d["passed"]:
            return
        # choose horizon with most labels. This is a shadow promotion until actual fills validate costs.
        self.promoted_horizon_ms = max(self.horizons_ms, key=lambda h: self.regressors[h].samples)

    def no_signal_reason(self, cost_bps: float) -> str:
        ready = [h for h, reg in self.regressors.items() if reg.samples >= self.min_labels_to_score and reg.initialized]
        if not ready:
            if self.bootstrap_model is not None:
                return "bootstrap_or_live_models_edge_below_current_cost_hurdle"
            return "cold_start_insufficient_labels_for_any_horizon"
        best_edge = None
        # Predictions are not kept here after observe_decision; this reason is intentionally conservative.
        if best_edge is None:
            return "trained_models_available_but_no_prediction_exceeded_cost_hurdle"
        return "no_signal"

    def test_matrix(self, cost_bps: float) -> dict[str, Any]:
        promotion = self._promotion_diagnostics()
        horizons: dict[str, Any] = {}
        for h in self.horizons_ms:
            reg = self.regressors[h]
            clf = self.classifiers[h]
            horizons[str(h)] = {
                "return_regressor": {
                    "model": "SGDRegressor(loss=huber, penalty=elasticnet, average=True)",
                    "target": f"future_mid_return_bps_after_{h}ms",
                    "training_status": "TRAINING" if reg.samples > 0 else "WAITING_FOR_MATURED_LABELS",
                    "samples": reg.samples,
                    "residual_std_bps": reg.residual_std_bps,
                    "ready_to_score": reg.samples >= self.min_labels_to_score,
                },
                "event_classifier": {
                    "model": "SGDClassifier(loss=log_loss, penalty=elasticnet, average=True)",
                    "target": f"short_neutral_long_after_{h}ms_vs_cost_hurdle",
                    "training_status": "TRAINING" if clf.samples > 0 else "WAITING_FOR_MATURED_LABELS",
                    "samples": clf.samples,
                    "accuracy": clf.accuracy,
                    "ready_to_score": clf.samples >= self.min_labels_to_score,
                },
            }
        return {
            "horizons": horizons,
            "current_cost_hurdle_bps": float(cost_bps),
            "promotion_test": promotion,
            "promoted_horizon_ms": self.promoted_horizon_ms,
            "bootstrap_model": {
                "loaded": self.bootstrap_model is not None,
                "live_approved": bool((self.bootstrap_manifest or {}).get("live_approved", False)),
                "data_class": (self.bootstrap_manifest or {}).get("data_class"),
                "reason_not_live_approved": (self.bootstrap_manifest or {}).get("reason_not_live_approved"),
            },
        }

    @property
    def promoted(self) -> bool:
        return self.promoted_horizon_ms is not None

    def checkpoint(self) -> None:
        for h, model in self.regressors.items():
            model.save(self.model_dir / f"return_{h}ms_shadow.joblib", {"horizon_ms": h, "matured_labels": self.matured_labels, "live_promoted_shadow": self.promoted_horizon_ms == h})
        (self.model_dir / "model_stack_state.json").write_text(json.dumps(self.status(), indent=2), encoding="utf-8")
        self.last_checkpoint_labels = self.matured_labels

    def status(self, cost_bps: float = 0.0) -> dict[str, Any]:
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
            "promotion_test": self._promotion_diagnostics(),
            "tests_running": self.test_matrix(cost_bps=cost_bps),
        }

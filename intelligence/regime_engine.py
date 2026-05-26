"""Trainable Gaussian hidden Markov regime model for balance/trend/expansion/shock/illiquid states."""
from __future__ import annotations
from dataclasses import dataclass
import numpy as np
from scipy.special import logsumexp
from sklearn.cluster import KMeans

@dataclass(frozen=True)
class RegimeAssessment:
    regime: str
    probability: float
    state_probabilities: dict[str, float]
    features: tuple[float, ...]

class GaussianHMMRegimeModel:
    """Diagonal-Gaussian HMM trained through Viterbi re-estimation with explicit state labelling."""
    def __init__(self, state_names: tuple[str, ...] = ("balance", "trend", "expansion", "shock", "illiquid"), iterations: int = 15, random_state: int = 7) -> None:
        self.state_names = state_names; self.n_states = len(state_names); self.iterations = iterations; self.random_state = random_state
        self.startprob_: np.ndarray | None = None; self.transmat_: np.ndarray | None = None; self.means_: np.ndarray | None = None; self.vars_: np.ndarray | None = None
        self.state_label_map_: dict[int, str] = {}
    def _log_emission(self, x: np.ndarray) -> np.ndarray:
        assert self.means_ is not None and self.vars_ is not None
        diff = x[:, None, :] - self.means_[None, :, :]
        return -0.5 * np.sum(np.log(2 * np.pi * self.vars_[None, :, :]) + diff * diff / self.vars_[None, :, :], axis=2)
    def _viterbi(self, x: np.ndarray) -> np.ndarray:
        emission = self._log_emission(x); t = x.shape[0]
        start = np.log(np.maximum(self.startprob_, 1e-12)); trans = np.log(np.maximum(self.transmat_, 1e-12))
        delta = np.zeros((t, self.n_states)); psi = np.zeros((t, self.n_states), dtype=int)
        delta[0] = start + emission[0]
        for i in range(1, t):
            values = delta[i-1][:, None] + trans
            psi[i] = np.argmax(values, axis=0); delta[i] = values[psi[i], np.arange(self.n_states)] + emission[i]
        states = np.zeros(t, dtype=int); states[-1] = int(np.argmax(delta[-1]))
        for i in range(t-2, -1, -1): states[i] = psi[i+1, states[i+1]]
        return states
    def fit(self, x: np.ndarray) -> "GaussianHMMRegimeModel":
        x = np.asarray(x, dtype=float)
        if x.ndim != 2 or x.shape[0] < max(20, self.n_states * 3): raise ValueError("insufficient HMM regime observations")
        labels = KMeans(n_clusters=self.n_states, n_init=10, random_state=self.random_state).fit_predict(x)
        for _ in range(self.iterations):
            self.means_ = np.vstack([x[labels == k].mean(axis=0) if np.any(labels == k) else x.mean(axis=0) for k in range(self.n_states)])
            self.vars_ = np.vstack([x[labels == k].var(axis=0) + 1e-8 if np.any(labels == k) else x.var(axis=0) + 1e-8 for k in range(self.n_states)])
            self.startprob_ = np.bincount(labels[:1], minlength=self.n_states).astype(float) + 1.0
            self.startprob_ /= self.startprob_.sum()
            transitions = np.ones((self.n_states, self.n_states), dtype=float)
            for a, b in zip(labels[:-1], labels[1:]): transitions[a, b] += 1.0
            self.transmat_ = transitions / transitions.sum(axis=1, keepdims=True)
            next_labels = self._viterbi(x)
            if np.array_equal(next_labels, labels): break
            labels = next_labels
        self._assign_semantic_labels()
        return self
    def _assign_semantic_labels(self) -> None:
        assert self.means_ is not None
        # Expected feature columns: signed return, EWMA vol, spread, flow persistence, liquidity weakness.
        available = set(range(self.n_states)); mapping: dict[int, str] = {}
        illiquid = max(available, key=lambda k: self.means_[k, 2] + self.means_[k, 4]); mapping[illiquid] = "illiquid"; available.remove(illiquid)
        shock = max(available, key=lambda k: self.means_[k, 1]); mapping[shock] = "shock"; available.remove(shock)
        trend = max(available, key=lambda k: abs(self.means_[k, 0]) + abs(self.means_[k, 3])); mapping[trend] = "trend"; available.remove(trend)
        if available:
            expansion = max(available, key=lambda k: self.means_[k, 1] + abs(self.means_[k, 0])); mapping[expansion] = "expansion"; available.remove(expansion)
        for remaining in available: mapping[remaining] = "balance"
        self.state_label_map_ = mapping
    def predict_proba(self, x: np.ndarray) -> np.ndarray:
        if self.means_ is None: raise RuntimeError("REGIME_HMM_NOT_FITTED")
        x = np.asarray(x, dtype=float); emission = self._log_emission(x)
        alpha = np.zeros((x.shape[0], self.n_states)); alpha[0] = np.log(self.startprob_) + emission[0]
        for idx in range(1, x.shape[0]): alpha[idx] = logsumexp(alpha[idx-1][:, None] + np.log(self.transmat_), axis=0) + emission[idx]
        last = np.exp(alpha[-1] - logsumexp(alpha[-1])); return last
    def assess(self, features: np.ndarray) -> RegimeAssessment:
        probs = self.predict_proba(np.asarray(features, dtype=float).reshape(1, -1)); idx = int(np.argmax(probs))
        labelled = {self.state_label_map_.get(k, self.state_names[k]): float(v) for k, v in enumerate(probs)}
        return RegimeAssessment(self.state_label_map_.get(idx, self.state_names[idx]), float(probs[idx]), labelled, tuple(float(x) for x in features))

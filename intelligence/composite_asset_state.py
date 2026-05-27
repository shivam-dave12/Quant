"""Product-aware composite intelligence for institutional multi-venue execution.

This module separates three concerns that must never be conflated:

1. Normalised *information*: every verified venue/product may publish bps-based
   structural evidence into its economic factor (BTC, GOLD, SILVER, OIL).
2. Transferable *execution alpha*: only instruments inside the same validated
   execution-equivalence group may share directional alpha for routing.
3. Correlated *factor evidence*: related but non-fungible instruments may adjust
   confidence and uncertainty, but cannot contribute raw price advantage or
   tradable return transfer without a fitted, explicitly enabled basis model.

All inputs are already venue-normalised states (returns, feed quality and bps
alpha), never raw cross-product prices or summed order books.
"""
from __future__ import annotations

import math
import threading
import time
from dataclasses import asdict, dataclass
from statistics import median
from typing import Any, Mapping

try:
    import config
except Exception:  # pragma: no cover
    config = None  # type: ignore

from market_data.normalizer import VenueMicrostate
from intelligence.venue_market_state import VenueMarketState


def _cfg(name: str, default: Any) -> Any:
    return getattr(config, name, default) if config is not None else default


def _num(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        return out if math.isfinite(out) else default
    except Exception:
        return default


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(value)))


def _sign(value: float, eps: float = 1e-9) -> int:
    return 1 if value > eps else -1 if value < -eps else 0


def _map_cfg(name: str, asset_id: str, default: str) -> str:
    raw = _cfg(name, {})
    if isinstance(raw, Mapping):
        return str(raw.get(str(asset_id).upper(), default) or default).upper()
    return default


@dataclass(frozen=True)
class ProductEvidence:
    asset_id: str
    factor_id: str
    equivalence_group: str
    transfer_mode: str
    venue: str
    symbol: str
    timestamp_monotonic: float
    structural_alpha_bps: float
    microstructure_alpha_bps: float
    confidence: float
    uncertainty_bps: float
    feed_quality_score: float
    execution_enabled: bool
    regime_label: str
    returns_bps: dict[str, float]
    diagnostics: dict[str, Any]

    @property
    def reliability_weight(self) -> float:
        return max(0.0, self.feed_quality_score) * max(0.0, self.confidence)

    @property
    def total_alpha_bps(self) -> float:
        return float(self.structural_alpha_bps + self.microstructure_alpha_bps)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CompositeAssetDecision:
    asset_id: str
    factor_id: str
    equivalence_group: str
    transfer_mode: str
    ready: bool
    reason: str
    transferable_structural_alpha_bps: float
    transferable_microstructure_alpha_bps: float
    transferable_total_alpha_bps: float
    factor_context_alpha_bps: float
    transferable_confidence: float
    factor_agreement_score: float
    factor_uncertainty_bps: float
    leader_source: str | None
    execution_sources: tuple[str, ...]
    factor_sources: tuple[str, ...]
    basis_translation_enabled: bool
    diagnostics: dict[str, Any]

    def directional_confidence_multiplier(self, direction_sign: int) -> float:
        """Confidence adjustment only; never a raw return transfer."""
        if direction_sign == 0 or not self.factor_sources:
            return 1.0
        factor_sign = _sign(self.factor_context_alpha_bps)
        if factor_sign == 0:
            return 1.0
        if factor_sign == direction_sign:
            return _clamp(1.0 + 0.10 * self.factor_agreement_score, 1.0, 1.10)
        return _clamp(1.0 - 0.12 * (1.0 - self.factor_agreement_score / 2.0), 0.84, 1.0)

    def contextual_uncertainty_for(self, direction_sign: int) -> float:
        if direction_sign == 0 or not self.factor_sources:
            return 0.0
        factor_sign = _sign(self.factor_context_alpha_bps)
        if factor_sign in (0, direction_sign):
            return max(0.0, self.factor_uncertainty_bps * 0.35)
        return max(0.0, self.factor_uncertainty_bps)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class CompositeIntelligenceBus:
    """Thread-safe factor bus shared by asset contexts.

    The bus stores normalised bps evidence only. It never combines raw prices,
    order-book quantities or balances across products. For BTC/PAXG-compatible
    routes it creates transferable structural alpha. For GOLD/SILVER related
    but non-fungible products it produces contextual confidence evidence only.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._evidence: dict[tuple[str, str, str], ProductEvidence] = {}

    @staticmethod
    def policy(asset_id: str) -> tuple[str, str, str]:
        asset = str(asset_id or "").upper()
        factor = _map_cfg("INSTITUTIONAL_FACTOR_BY_ASSET", asset, asset)
        equivalence = _map_cfg("INSTITUTIONAL_EXECUTION_EQUIVALENCE_GROUP_BY_ASSET", asset, asset)
        transfer = _map_cfg("INSTITUTIONAL_FACTOR_TRANSFER_MODE_BY_ASSET", asset, "CONFIDENCE_ONLY")
        return factor, equivalence, transfer

    @staticmethod
    def _normalised_microstructure_alpha(asset_id: str, micro: VenueMicrostate) -> float:
        near_depth = sum(
            float(micro.bid_depth_usd_by_band.get(k, 0.0) + micro.ask_depth_usd_by_band.get(k, 0.0))
            for k in ("0-1", "1-3")
        )
        if near_depth <= 0.0:
            return 0.0
        ofi_norm = (float(micro.ofi_usd_1s) + 0.50 * float(micro.ofi_usd_10s)) / near_depth
        tfi_norm = (float(micro.tfi_usd_1s) + 0.50 * float(micro.tfi_usd_10s)) / near_depth
        mid = max(float(micro.mid or 0.0), 1e-9)
        microprice_bps = (float(micro.microprice or mid) / mid - 1.0) * 10_000.0
        raw = (
            _num(_cfg("INSTITUTIONAL_FLOW_OFI_WEIGHT", 1.0), 1.0) * ofi_norm * 100.0
            + _num(_cfg("INSTITUTIONAL_FLOW_TFI_WEIGHT", 0.30), 0.30) * tfi_norm * 100.0
            + _num(_cfg("INSTITUTIONAL_FLOW_MICROPRICE_WEIGHT", 0.35), 0.35) * microprice_bps
        )
        caps = _cfg("INSTITUTIONAL_MICROSTRUCTURE_ALPHA_CAP_BPS", {})
        cap = _num(caps.get(str(asset_id).upper(), 18.0), 18.0) if isinstance(caps, Mapping) else 18.0
        return float(cap * math.tanh(raw / max(cap, 1e-9)))

    def publish(
        self,
        *,
        asset_id: str,
        market_states: Mapping[str, VenueMarketState],
        microstates: Mapping[str, VenueMicrostate],
    ) -> None:
        factor, equivalence, transfer = self.policy(asset_id)
        now = time.monotonic()
        with self._lock:
            for venue, state in market_states.items():
                micro = microstates.get(str(venue).lower())
                if not state.ready or micro is None or not micro.usable_for_decision:
                    continue
                key = (str(asset_id).upper(), str(venue).lower(), str(state.symbol).upper())
                self._evidence[key] = ProductEvidence(
                    asset_id=str(asset_id).upper(), factor_id=factor, equivalence_group=equivalence,
                    transfer_mode=transfer, venue=str(venue).lower(), symbol=str(state.symbol),
                    timestamp_monotonic=now, structural_alpha_bps=float(state.signed_alpha_bps),
                    microstructure_alpha_bps=self._normalised_microstructure_alpha(str(asset_id), micro),
                    confidence=float(state.confidence), uncertainty_bps=float(state.uncertainty_bps),
                    feed_quality_score=float(micro.feed_quality_score or 0.0),
                    execution_enabled=bool(micro.execution_enabled), regime_label=str(state.regime_label),
                    returns_bps=dict(state.returns_bps),
                    diagnostics={"source": "venue_local_closed_candle_market_state", "raw_prices_merged": False},
                )

    @staticmethod
    def _weighted_robust_alpha(rows: list[ProductEvidence], field_name: str = "structural_alpha_bps") -> tuple[float, float, float, str | None]:
        if not rows:
            return 0.0, 0.0, 0.0, None
        raw_scores = [float(getattr(row, field_name)) for row in rows]
        centre = median(raw_scores)
        abs_dev = [abs(value - centre) for value in raw_scores]
        scale = max(median(abs_dev) if abs_dev else 0.0, 0.50)
        clipped: list[tuple[float, float, ProductEvidence]] = []
        for row in rows:
            winsor = _clamp(float(getattr(row, field_name)), centre - 3.0 * scale, centre + 3.0 * scale)
            weight = max(0.01, float(row.reliability_weight))
            clipped.append((winsor, weight, row))
        total_w = sum(weight for _, weight, _ in clipped)
        alpha = sum(value * weight for value, weight, _ in clipped) / max(total_w, 1e-9)
        principal = _sign(alpha)
        agree_w = sum(weight for value, weight, _ in clipped if _sign(value) == principal and principal != 0)
        agreement = agree_w / max(total_w, 1e-9) if principal else 0.0
        uncertainty = sum(weight * abs(value - alpha) for value, weight, _ in clipped) / max(total_w, 1e-9)
        leader = max(clipped, key=lambda item: abs(item[0]) * item[1])[2]
        return float(alpha), _clamp(agreement, 0.0, 1.0), float(uncertainty), f"{leader.asset_id}:{leader.venue}:{leader.symbol}"

    def build_decision(
        self,
        *,
        asset_id: str,
        market_states: Mapping[str, VenueMarketState],
        microstates: Mapping[str, VenueMicrostate],
    ) -> CompositeAssetDecision:
        self.publish(asset_id=asset_id, market_states=market_states, microstates=microstates)
        asset = str(asset_id or "").upper()
        factor, equivalence, transfer = self.policy(asset)
        now = time.monotonic()
        stale_sec = max(0.25, _num(_cfg("INSTITUTIONAL_FACTOR_EVIDENCE_MAX_STALENESS_SEC", 8.0), 8.0))
        with self._lock:
            rows = [row for row in self._evidence.values() if row.factor_id == factor and now - row.timestamp_monotonic <= stale_sec]
        exact = [row for row in rows if row.equivalence_group == equivalence and row.execution_enabled]
        broader = [row for row in rows if row.equivalence_group != equivalence]
        execution_alpha, execution_agreement, execution_uncertainty, execution_leader = self._weighted_robust_alpha(exact, "structural_alpha_bps")
        execution_micro_alpha, micro_agreement, micro_uncertainty, micro_leader = self._weighted_robust_alpha(exact, "microstructure_alpha_bps")
        # Broader factor context combines normalised structural and order-flow
        # evidence in bps, but remains non-transferable alpha for non-fungible products.
        factor_alpha, factor_agreement, factor_uncertainty, factor_leader = self._weighted_robust_alpha(rows, "total_alpha_bps")
        translation_cfg = _cfg("INSTITUTIONAL_VALIDATED_FACTOR_TRANSLATION_MODELS", {})
        model = translation_cfg.get(asset, {}) if isinstance(translation_cfg, Mapping) else {}
        basis_translation_enabled = bool(isinstance(model, Mapping) and model.get("enabled") and model.get("validation_status") == "approved")
        # Translated cross-product alpha is deliberately off unless a separately
        # fitted/validated model is authorised in config.  Factor evidence still
        # informs confidence and uncertainty for the local tradable thesis.
        if basis_translation_enabled and broader:
            beta = _num(model.get("beta"), 0.0)
            hedge_reserve = abs(_num(model.get("basis_risk_reserve_bps"), 0.0))
            translated = beta * factor_alpha
            execution_alpha += math.copysign(max(0.0, abs(translated) - hedge_reserve), translated)
        ready = bool(exact)
        return CompositeAssetDecision(
            asset_id=asset, factor_id=factor, equivalence_group=equivalence, transfer_mode=transfer,
            ready=ready, reason="normalised_composite_ready" if ready else "no_execution_equivalent_evidence_ready",
            transferable_structural_alpha_bps=float(execution_alpha),
            transferable_microstructure_alpha_bps=float(execution_micro_alpha),
            transferable_total_alpha_bps=float(execution_alpha + execution_micro_alpha),
            factor_context_alpha_bps=float(factor_alpha),
            transferable_confidence=float(execution_agreement), factor_agreement_score=float(factor_agreement),
            factor_uncertainty_bps=float(factor_uncertainty), leader_source=execution_leader or factor_leader,
            execution_sources=tuple(sorted(f"{row.asset_id}:{row.venue}:{row.symbol}" for row in exact)),
            factor_sources=tuple(sorted(f"{row.asset_id}:{row.venue}:{row.symbol}" for row in rows)),
            basis_translation_enabled=basis_translation_enabled,
            diagnostics={
                "normalisation_domain": "bps_returns_and_usd_normalised_microstate",
                "raw_price_routing": False,
                "order_books_merged": False,
                "execution_equivalent_source_count": len(exact),
                "correlated_context_source_count": len(broader),
                "execution_agreement_score": execution_agreement,
                "execution_uncertainty_bps": execution_uncertainty + micro_uncertainty,
                "microstructure_agreement_score": micro_agreement,
                "execution_structural_alpha_bps": execution_alpha,
                "execution_microstructure_alpha_bps": execution_micro_alpha,
                "execution_total_alpha_bps": execution_alpha + execution_micro_alpha,
                "microstructure_leader_source": micro_leader,
                "factor_translation_policy": "approved_model" if basis_translation_enabled else "confidence_only_no_alpha_transfer",
            },
        )

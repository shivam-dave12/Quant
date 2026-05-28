"""Institutional setup classification with predictive pre-move entry authority.

The parent structural engine establishes the only permitted direction.  This
classifier then demands an observable *pre-displacement* execution setup rather
than waiting for the move to occur.  It does not claim liquidation, spoofing or
hidden inventory evidence when those feeds are absent.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from intelligence.venue_market_state import CrossVenueEvidence, VenueMarketState
from strategy.barrier_outcome import BarrierOutcomeAssessment
from strategy.domain import Direction
from strategy.predictive_flow import PredictiveFlowAssessment

try:
    import config
except Exception:  # pragma: no cover
    config = None  # type: ignore


def _cfg(name: str, default: Any) -> Any:
    return getattr(config, name, default) if config is not None else default


def _sign(direction: Direction) -> int:
    return 1 if direction in {Direction.LONG, Direction.BULLISH} else -1 if direction in {Direction.SHORT, Direction.BEARISH} else 0


@dataclass(frozen=True)
class InstitutionalSetupAssessment:
    approved: bool
    model: str
    setup_family: str
    direction: str
    thesis: str
    parent_alpha_directional_bps: float
    acceptance_directional_bps: float
    predictive_directional_probability: float
    required_predictive_probability: float
    predictive_alpha_bps: float
    depletion_advantage: float
    cross_venue_agreement: float | None
    leader_venue: str | None
    forced_flow_evidence_available: bool
    reasons: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class InstitutionalSetupClassifier:
    """Approve only classified predictive setups before order submission.

    Current admissible families use only wired evidence:
      * PREMOVE_QUEUE_DEPLETION_INITIATION: a directional parent and current
        queue-depletion/order-flow hazard favour imminent displacement.
      * PREMOVE_CROSS_VENUE_LEAD_LAG: the above with coherent multi-venue
        leader/follower evidence for equivalent execution products.
      * PREMOVE_CLOSED_ACCEPTANCE_EXPANSION: a closed structural acceptance
        exists and fresh pre-move pressure indicates a new continuation leg.

    Realised post-flow displacement is telemetry for toxicity/calibration only;
    it is never required before these entries.
    """

    def __init__(self, asset_id: str) -> None:
        self.asset_id = str(asset_id or "").upper()

    def classify(
        self,
        *,
        venue: str,
        direction: Direction,
        market_state: VenueMarketState | None,
        barrier: BarrierOutcomeAssessment,
        predictive_flow: PredictiveFlowAssessment | None,
        cross_venue_evidence: CrossVenueEvidence | None = None,
    ) -> InstitutionalSetupAssessment:
        sign = _sign(direction)
        parent_directional = sign * float(barrier.parent_alpha_bps)
        acceptance_directional = sign * float(market_state.acceptance_bps if market_state is not None else 0.0)
        p = float(predictive_flow.directional_move_probability if predictive_flow is not None else 0.0)
        p_req = float(predictive_flow.required_directional_probability if predictive_flow is not None else 1.0)
        predictive_alpha = float(predictive_flow.predictive_alpha_bps if predictive_flow is not None else 0.0)
        depletion = float(predictive_flow.depletion_advantage if predictive_flow is not None else 0.0)
        reasons: list[str] = []
        if sign == 0:
            reasons.append("setup_direction_unavailable")
        if not barrier.approved:
            reasons.append("predictive_protected_barrier_outcome_not_approved")
        if market_state is None or not bool(market_state.ready):
            reasons.append("closed_structural_state_unavailable")
        minimum_parent = float(_cfg("SETUP_CLASSIFIER_MIN_DIRECTIONAL_PARENT_ALPHA_BPS", 0.50))
        if parent_directional < minimum_parent:
            reasons.append(f"setup_parent_alpha_insufficient:{parent_directional:.3f}<{minimum_parent:.3f}")
        if predictive_flow is None or not predictive_flow.ready:
            reasons.append("pre_move_orderflow_evidence_not_ready")
        elif not predictive_flow.approved:
            reasons.extend(list(predictive_flow.reasons))
        if reasons:
            return InstitutionalSetupAssessment(
                False, "predictive_setup_classifier_v2", "NO_TRADE_UNCLASSIFIED_EDGE", direction.value,
                "No pre-displacement institutional setup passed live submission authority.",
                parent_directional, acceptance_directional, p, p_req, predictive_alpha, depletion,
                float(cross_venue_evidence.agreement_score) if cross_venue_evidence is not None else None,
                str(cross_venue_evidence.leader_venue) if cross_venue_evidence is not None else None,
                False, tuple(dict.fromkeys(reasons)),
            )

        agreement: float | None = None
        leader: str | None = None
        regime = str(market_state.regime_label or "UNKNOWN").upper() if market_state is not None else "UNKNOWN"
        acceptance_min = float(_cfg("SETUP_CLASSIFIER_MIN_CLOSED_ACCEPTANCE_BPS", 0.25))
        family = "PREMOVE_QUEUE_DEPLETION_INITIATION"
        thesis = "Closed parent direction is paired with current queue-depletion and order-flow pressure predicting imminent displacement before it is realised."
        if acceptance_directional >= acceptance_min and regime in {"TREND", "EXPANSION"}:
            family = "PREMOVE_CLOSED_ACCEPTANCE_EXPANSION"
            thesis = "Closed structural acceptance exists and new pre-move queue pressure supports a further executable expansion leg."
        if cross_venue_evidence is not None:
            agreement = float(cross_venue_evidence.agreement_score)
            leader = str(cross_venue_evidence.leader_venue or "") or None
            threshold = float(_cfg("SETUP_CLASSIFIER_CROSS_VENUE_CONFIRMATION_MIN_AGREEMENT", 0.60))
            if leader and agreement >= threshold:
                family = "PREMOVE_CROSS_VENUE_LEAD_LAG"
                thesis = "Parent thesis and pre-move local hazard are corroborated by live cross-venue leader/follower evidence."
        return InstitutionalSetupAssessment(
            True, "predictive_setup_classifier_v2", family, direction.value, thesis,
            parent_directional, acceptance_directional, p, p_req, predictive_alpha, depletion,
            agreement, leader, False, (),
        )

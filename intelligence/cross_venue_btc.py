"""BTC cross-venue composite context.

This module deliberately preserves venue-specific microstates. Composite fields
are contextual features for models and risk controls, not a hand-weighted OFI
trade trigger.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Mapping

from market_data.normalizer import VenueMicrostate


@dataclass
class BTCCompositeState:
    delta_state: VenueMicrostate
    reference_states: dict[str, VenueMicrostate]
    composite_reference_mid: float | None
    delta_dislocation_bps: float | None
    flow_agreement_score: float
    cross_venue_dispersion_bps: float
    candidate_leader_venue: str | None
    leader_confidence: float | None
    delta_execution_quality_score: float
    excluded_reference_venues: dict[str, str]


def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    try:
        if not math.isfinite(float(value)):
            return lo
        return max(lo, min(hi, float(value)))
    except Exception:
        return lo


def _sign(value: float) -> int:
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0


def venue_flow_signal(state: VenueMicrostate) -> float:
    """Transparent baseline flow signal used only for diagnostics/features."""

    return (
        float(state.ofi_usd_1s)
        + 0.50 * float(state.ofi_usd_10s)
        + 0.20 * float(state.tfi_usd_1s)
        + 0.10 * float(state.tfi_usd_10s)
    )


def delta_execution_quality_score(state: VenueMicrostate) -> float:
    near_depth = (
        float(state.bid_depth_usd_by_band.get("0-1", 0.0))
        + float(state.ask_depth_usd_by_band.get("0-1", 0.0))
    )
    depth_score = _clamp(math.log10(max(near_depth, 1.0)) / 7.0)
    spread_score = _clamp(1.0 - max(float(state.spread_bps), 0.0) / 20.0)
    return _clamp(float(state.feed_quality_score) * (0.55 * spread_score + 0.45 * depth_score))


def build_btc_composite_state(
    *,
    delta_state: VenueMicrostate,
    reference_states: Mapping[str, VenueMicrostate],
) -> BTCCompositeState:
    excluded: dict[str, str] = {}
    healthy_refs: dict[str, VenueMicrostate] = {}
    for venue, state in reference_states.items():
        if not state.usable_for_decision:
            excluded[str(venue)] = "feed_quality_zero_or_invalid_sequence"
            continue
        if state.mid <= 0:
            excluded[str(venue)] = "invalid_mid"
            continue
        healthy_refs[str(venue)] = state

    composite_mid = None
    if healthy_refs:
        weight_sum = sum(max(s.feed_quality_score, 0.0) for s in healthy_refs.values())
        if weight_sum > 0:
            composite_mid = sum(s.mid * max(s.feed_quality_score, 0.0) for s in healthy_refs.values()) / weight_sum

    delta_dislocation = None
    if composite_mid and composite_mid > 0:
        delta_dislocation = (float(delta_state.microprice) / composite_mid - 1.0) * 10_000.0

    flow_terms: list[tuple[str, float, int]] = []
    for venue, state in {"delta": delta_state, **healthy_refs}.items():
        signal = venue_flow_signal(state)
        weight = max(float(state.feed_quality_score), 0.0)
        flow_terms.append((venue, weight, _sign(signal)))
    total_weight = sum(w for _, w, s in flow_terms if s != 0)
    if total_weight > 0:
        flow_agreement = abs(sum(w * s for _, w, s in flow_terms)) / total_weight
    else:
        flow_agreement = 0.0

    mids = [delta_state.mid] + [s.mid for s in healthy_refs.values()]
    dispersion = 0.0
    if len(mids) > 1 and sum(mids) > 0:
        mean_mid = sum(mids) / len(mids)
        dispersion = (max(mids) - min(mids)) / mean_mid * 10_000.0

    leader_venue = None
    leader_confidence = None
    reference_abs = {
        venue: abs(venue_flow_signal(state)) * max(state.feed_quality_score, 0.0)
        for venue, state in healthy_refs.items()
    }
    total_abs = sum(reference_abs.values())
    if total_abs > 0:
        leader_venue = max(reference_abs, key=reference_abs.get)
        leader_confidence = _clamp(reference_abs[leader_venue] / total_abs)

    return BTCCompositeState(
        delta_state=delta_state,
        reference_states=dict(healthy_refs),
        composite_reference_mid=composite_mid,
        delta_dislocation_bps=delta_dislocation,
        flow_agreement_score=_clamp(flow_agreement),
        cross_venue_dispersion_bps=max(0.0, dispersion),
        candidate_leader_venue=leader_venue,
        leader_confidence=leader_confidence,
        delta_execution_quality_score=delta_execution_quality_score(delta_state),
        excluded_reference_venues=excluded,
    )


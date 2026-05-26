"""Auction-control state model for institutional structural execution.

The entry engine must not treat a local five-minute pattern as the market's
whole intent.  This module establishes an always-on auction narrative from:

* directional delivery across 1D/4H/1H/15m structure;
* the freshest meaningful higher-timeframe liquidity raid; and
* whether five-minute delivery has actually transferred control.

It is deliberately not a stack of independent filters.  It answers one core
question before a ticket can exist: which side presently owns price delivery,
and what risk posture is warranted while that ownership persists?
"""
from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence

_EPS = 1e-12
_TF_WEIGHT = {"15m": 0.46, "1h": 0.76, "4h": 0.93, "1d": 1.00}
_TF_SECONDS = {"15m": 900.0, "1h": 3600.0, "4h": 14400.0, "1d": 86400.0}
_CONTEXT_WEIGHT = {"15m": 0.18, "1h": 0.23, "4h": 0.32, "1d": 0.27}


def _f(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        return out if math.isfinite(out) else default
    except Exception:
        return default


def _sign_side(side: str) -> float:
    return 1.0 if str(side).lower() == "long" else (-1.0 if str(side).lower() == "short" else 0.0)


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, _f(value, low)))


def _parent_control_live(timeframe: str, age_sec: float) -> bool:
    """A completed raid owns delivery only while its timeframe can still deliver.

    This is not an arbitrary trade veto: it prevents an old raid from holding
    market ownership indefinitely after its natural delivery horizon expires.
    """
    seconds = _TF_SECONDS.get(str(timeframe or "").lower(), 0.0)
    return seconds > 0.0 and max(0.0, _f(age_sec)) <= 2.0 * seconds


@dataclass(frozen=True)
class AuctionNarrative:
    phase: str
    control_side: str
    control_score: float
    clarity: float
    posture: str
    risk_scalar: float
    context_drive: float
    parent_pressure: float
    parent_side: str
    parent_timeframe: str
    parent_quality: float
    parent_age_sec: float
    child_transfer_side: str
    thesis: str

    @property
    def has_firm_parent_control(self) -> bool:
        return (
            self.parent_side in ("long", "short")
            and self.parent_timeframe in ("1h", "4h", "1d")
            and self.parent_quality >= 0.60
            and _parent_control_live(self.parent_timeframe, self.parent_age_sec)
        )

    def owns(self, side: str) -> bool:
        return self.control_side in ("long", "short") and self.control_side == str(side or "").lower()

    def payload(self) -> Dict[str, Any]:
        return {
            "market_phase": self.phase,
            "auction_control_side": self.control_side,
            "auction_control_score": self.control_score,
            "auction_control_clarity": self.clarity,
            "execution_posture": self.posture,
            "auction_risk_scalar": self.risk_scalar,
            "market_context_drive": self.context_drive,
            "market_parent_pressure": self.parent_pressure,
            "controlling_parent_side": self.parent_side,
            "controlling_parent_tf": self.parent_timeframe,
            "controlling_parent_quality": self.parent_quality,
            "controlling_parent_age_sec": self.parent_age_sec,
            "child_transfer_side": self.child_transfer_side,
            "market_state_thesis": self.thesis,
        }


def build_auction_narrative(
    contexts: Mapping[str, Any],
    parent_sweeps: Sequence[Any],
    child_sweeps: Sequence[Any],
    now: float,
    *,
    aggressive_min_clarity: float = 0.62,
    parent_min_quality: float = 0.60,
) -> AuctionNarrative:
    """Build one market owner and posture from multi-timeframe delivery evidence.

    Context and parent raids are not additive trade filters.  They jointly form
    auction ownership. A five-minute reversal against strong 1H+ parent control
    remains observation until there is an actual transfer of delivery.
    """
    available_weight = 0.0
    drive = 0.0
    for tf, weight in _CONTEXT_WEIGHT.items():
        ctx = contexts.get(tf)
        if ctx is None:
            continue
        score = _f(getattr(ctx, "signed_score", 0.0))
        # A zero context is still meaningful ranging state, but it must not
        # consume direction-weight when no bars existed.
        confidence = _f(getattr(ctx, "confidence", 0.0))
        if abs(score) <= _EPS and confidence <= _EPS:
            continue
        drive += weight * max(-1.0, min(1.0, score))
        available_weight += weight
    context_drive = drive / available_weight if available_weight > _EPS else 0.0

    parent = None
    parent_strength = -1.0
    for sweep in list(parent_sweeps or []):
        pool = getattr(sweep, "pool", None)
        tf = str(getattr(pool, "timeframe", "") or "").lower()
        quality = _clamp(getattr(sweep, "quality", 0.0))
        age = max(0.0, _f(now) - _f(getattr(sweep, "detected_at", now), now))
        # Recent control matters more, without deleting contextual raids.
        decay = math.exp(-age / max(90.0, 7200.0 * _TF_WEIGHT.get(tf, 0.40)))
        strength = _TF_WEIGHT.get(tf, 0.30) * quality * (0.62 + 0.38 * decay)
        if strength > parent_strength:
            parent, parent_strength = sweep, strength

    parent_side = "none"
    parent_tf = "none"
    parent_quality = 0.0
    parent_age = 0.0
    parent_pressure = 0.0
    if parent is not None:
        pool = getattr(parent, "pool", None)
        parent_side = str(getattr(parent, "direction", "") or "").lower()
        parent_tf = str(getattr(pool, "timeframe", "") or "").lower()
        parent_quality = _clamp(getattr(parent, "quality", 0.0))
        parent_age = max(0.0, _f(now) - _f(getattr(parent, "detected_at", now), now))
        parent_pressure = _sign_side(parent_side) * max(0.0, parent_strength)

    child_side = "none"
    child_quality = 0.0
    for sweep in list(child_sweeps or []):
        quality = _clamp(getattr(sweep, "quality", 0.0))
        if quality > child_quality:
            child_quality = quality
            child_side = str(getattr(sweep, "direction", "") or "").lower()

    firm_parent = (
        parent_side in ("long", "short")
        and parent_tf in ("1h", "4h", "1d")
        and parent_quality >= parent_min_quality
        and _parent_control_live(parent_tf, parent_age)
    )
    transfer_confirmed = firm_parent and child_side == parent_side and child_quality >= 0.50

    # Parent liquidity transfer owns the auction if a fresh significant parent
    # exists.  Lower-timeframe context contributes, but cannot flip ownership
    # before a true transfer sequence appears.
    if firm_parent:
        blended = 0.34 * context_drive + 0.66 * parent_pressure
    elif parent_side in ("long", "short"):
        blended = 0.52 * context_drive + 0.48 * parent_pressure
    else:
        blended = context_drive
    blended = max(-1.0, min(1.0, blended))
    clarity = _clamp(abs(blended))
    threshold = 0.12
    control_side = "long" if blended > threshold else ("short" if blended < -threshold else "none")

    if firm_parent and transfer_confirmed:
        phase = "LIQUIDITY_TRANSFER_CONFIRMED"
    elif firm_parent:
        phase = "HTF_RAID_AWAITING_TRANSFER"
    elif abs(context_drive) >= 0.42:
        phase = "DIRECTIONAL_DELIVERY"
    elif parent_side in ("long", "short"):
        phase = "LIQUIDITY_PROBE"
    else:
        phase = "BALANCED_REPRICING"

    if control_side == "none":
        posture, scalar = "OBSERVE", 0.35
    elif phase == "LIQUIDITY_TRANSFER_CONFIRMED" and clarity >= aggressive_min_clarity:
        posture, scalar = "AGGRESSIVE", 1.00
    elif phase in ("DIRECTIONAL_DELIVERY", "LIQUIDITY_TRANSFER_CONFIRMED"):
        posture, scalar = "PROACTIVE", 0.82
    elif phase == "HTF_RAID_AWAITING_TRANSFER":
        posture, scalar = "PATIENT", 0.48
    else:
        posture, scalar = "REDUCED", 0.60

    thesis = (
        f"phase={phase}; controller={control_side}; clarity={clarity:.2f}; "
        f"parent={parent_side}[{parent_tf}] q={parent_quality:.2f}; child={child_side}; posture={posture}"
    )
    return AuctionNarrative(
        phase=phase, control_side=control_side, control_score=blended,
        clarity=clarity, posture=posture, risk_scalar=scalar,
        context_drive=context_drive, parent_pressure=parent_pressure,
        parent_side=parent_side, parent_timeframe=parent_tf,
        parent_quality=parent_quality, parent_age_sec=parent_age,
        child_transfer_side=child_side, thesis=thesis,
    )

"""Venue feed-health scoring.

The score is deterministic and venue-baseline aware. A healthy connection with
unchanged order-book state is not penalised when the venue heartbeat remains
valid, while structural data failures force the score to zero.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, replace
from typing import Any


def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    try:
        if not math.isfinite(float(value)):
            return lo
        return max(lo, min(hi, float(value)))
    except Exception:
        return lo


@dataclass(frozen=True)
class FeedHealth:
    connected: bool
    heartbeat_ok: bool
    sequence_valid: bool
    snapshot_ready: bool
    exchange_timestamp_available: bool
    latency_vs_baseline_z: float | None
    no_change_heartbeat_valid: bool
    quality_score: float
    reason: str = ""

    @property
    def usable_for_decision(self) -> bool:
        return self.quality_score > 0.0


def score_feed_health(
    *,
    connected: bool,
    heartbeat_ok: bool,
    sequence_valid: bool,
    snapshot_ready: bool,
    exchange_timestamp_available: bool,
    latency_vs_baseline_z: float | None,
    no_change_heartbeat_valid: bool = False,
) -> FeedHealth:
    """Return a deterministic 0..1 quality score for a venue feed.

    Hard-zero conditions are limited to structural feed failures: disconnect,
    invalid sequence/checksum/book reconstruction, or missing snapshot. Latency
    is penalised relative to the feed's own empirical baseline, not with a
    universal millisecond threshold.
    """

    if not connected:
        return FeedHealth(
            connected=connected,
            heartbeat_ok=heartbeat_ok,
            sequence_valid=sequence_valid,
            snapshot_ready=snapshot_ready,
            exchange_timestamp_available=exchange_timestamp_available,
            latency_vs_baseline_z=latency_vs_baseline_z,
            no_change_heartbeat_valid=no_change_heartbeat_valid,
            quality_score=0.0,
            reason="disconnected",
        )
    if not sequence_valid:
        return FeedHealth(
            connected=connected,
            heartbeat_ok=heartbeat_ok,
            sequence_valid=sequence_valid,
            snapshot_ready=snapshot_ready,
            exchange_timestamp_available=exchange_timestamp_available,
            latency_vs_baseline_z=latency_vs_baseline_z,
            no_change_heartbeat_valid=no_change_heartbeat_valid,
            quality_score=0.0,
            reason="invalid_sequence",
        )
    if not snapshot_ready:
        return FeedHealth(
            connected=connected,
            heartbeat_ok=heartbeat_ok,
            sequence_valid=sequence_valid,
            snapshot_ready=snapshot_ready,
            exchange_timestamp_available=exchange_timestamp_available,
            latency_vs_baseline_z=latency_vs_baseline_z,
            no_change_heartbeat_valid=no_change_heartbeat_valid,
            quality_score=0.0,
            reason="snapshot_missing",
        )

    score = 1.0
    reasons: list[str] = []
    if not heartbeat_ok:
        score *= 0.35
        reasons.append("heartbeat_missing")
    if not exchange_timestamp_available:
        score *= 0.90
        reasons.append("exchange_ts_missing")
    if latency_vs_baseline_z is not None:
        z = max(0.0, float(latency_vs_baseline_z))
        if z > 1.0:
            # 1 sigma is normal, 4 sigma is materially degraded, 7+ is severe.
            score *= _clamp(1.0 - (z - 1.0) / 7.5, 0.15, 1.0)
            reasons.append(f"latency_z={z:.2f}")
    if no_change_heartbeat_valid and heartbeat_ok:
        reasons.append("book_unchanged_heartbeat_valid")

    return FeedHealth(
        connected=connected,
        heartbeat_ok=heartbeat_ok,
        sequence_valid=sequence_valid,
        snapshot_ready=snapshot_ready,
        exchange_timestamp_available=exchange_timestamp_available,
        latency_vs_baseline_z=latency_vs_baseline_z,
        no_change_heartbeat_valid=no_change_heartbeat_valid,
        quality_score=_clamp(score),
        reason=";".join(reasons) or "healthy",
    )


def with_zero_quality(health: FeedHealth, reason: str) -> FeedHealth:
    return replace(health, quality_score=0.0, reason=reason)


def feed_health_from_mapping(raw: dict[str, Any]) -> FeedHealth:
    return score_feed_health(
        connected=bool(raw.get("connected")),
        heartbeat_ok=bool(raw.get("heartbeat_ok", True)),
        sequence_valid=bool(raw.get("sequence_valid", True)),
        snapshot_ready=bool(raw.get("snapshot_ready")),
        exchange_timestamp_available=bool(raw.get("exchange_timestamp_available")),
        latency_vs_baseline_z=raw.get("latency_vs_baseline_z"),
        no_change_heartbeat_valid=bool(raw.get("no_change_heartbeat_valid", False)),
    )


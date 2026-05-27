"""Portfolio-level exposure controls for correlated macro risk buckets."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping


@dataclass(frozen=True)
class ExposureDecision:
    approved: bool
    bucket: str
    existing_gross_delta_usd: float
    proposed_gross_delta_usd: float
    cap_usd: float
    reason: str


@dataclass
class PortfolioExposureTracker:
    """Tracks delta-dollar exposure without assuming assets are independent.

    The anti-dollar bucket deliberately groups BTC and precious metals. This
    is a conservative gross-exposure limit: it does not claim a stable hedge
    ratio, and it can later be replaced with rolling covariance estimates.
    """

    positions: dict[str, tuple[str, float]] = field(default_factory=dict)

    @staticmethod
    def bucket_for(asset_id: str) -> str:
        asset = str(asset_id or "").upper()
        if asset == "BTC" or asset.startswith("GOLD") or asset.startswith("SILVER") or asset in {"XAUT", "XAG"}:
            return "ANTI_DOLLAR_MACRO"
        if asset in {"NIFTY", "BANKNIFTY", "SENSEX"}:
            return "INDIA_EQUITY_INDEX"
        return asset or "UNKNOWN"

    def gross_delta_usd(self, bucket: str) -> float:
        return sum(abs(delta) for pos_bucket, delta in self.positions.values() if pos_bucket == bucket)

    def evaluate_increment(self, *, asset_id: str, position_key: str, signed_delta_usd: float, cap_usd: float) -> ExposureDecision:
        bucket = self.bucket_for(asset_id)
        existing_without_key = sum(
            abs(delta) for key, (pos_bucket, delta) in self.positions.items()
            if key != position_key and pos_bucket == bucket
        )
        proposed = existing_without_key + abs(float(signed_delta_usd))
        approved = proposed <= max(float(cap_usd), 0.0)
        return ExposureDecision(
            approved=approved, bucket=bucket, existing_gross_delta_usd=existing_without_key,
            proposed_gross_delta_usd=proposed, cap_usd=float(cap_usd),
            reason="correlated_exposure_within_cap" if approved else "correlated_exposure_cap_exceeded",
        )

    def record(self, *, asset_id: str, position_key: str, signed_delta_usd: float) -> None:
        self.positions[str(position_key)] = (self.bucket_for(asset_id), float(signed_delta_usd))

    def remove(self, position_key: str) -> None:
        self.positions.pop(str(position_key), None)

    def snapshot(self) -> Mapping[str, float]:
        buckets = {bucket for bucket, _ in self.positions.values()}
        return {bucket: self.gross_delta_usd(bucket) for bucket in buckets}

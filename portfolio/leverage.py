"""Deterministic dynamic leverage selection for Delta directional contracts."""
from __future__ import annotations

from dataclasses import dataclass
import math


@dataclass(frozen=True)
class LeverageDecision:
    approved: bool
    selected_leverage: float | None
    unit_margin: float
    liquidation_distance_fraction: float
    required_buffer_fraction: float
    reason: str


class DynamicLeverageSelector:
    """Selects leverage below venue maximum using stress and execution state.

    This is not return maximisation by maximal leverage. The selected leverage must
    leave a liquidation-distance proxy greater than a multiple of the current
    invalidation/volatility stress before portfolio sizing may use it.
    """

    def __init__(self, *, stress_buffer_multiple: float = 3.0, leverage_step: float = 0.1) -> None:
        if stress_buffer_multiple <= 1 or leverage_step <= 0:
            raise ValueError("VALID_LEVERAGE_POLICY_PARAMETERS_REQUIRED")
        self.stress_buffer_multiple = float(stress_buffer_multiple)
        self.leverage_step = float(leverage_step)

    def select(self, *, maximum_leverage: float | None, unit_notional: float,
               risk_to_invalidation: float, volatility_fraction: float,
               execution_quality: float, expected_net_edge_bps: float) -> LeverageDecision:
        if maximum_leverage is None or maximum_leverage < 1:
            return LeverageDecision(False, None, 0.0, 0.0, 0.0, "VERIFIED_MAXIMUM_LEVERAGE_REQUIRED")
        if min(unit_notional, risk_to_invalidation, volatility_fraction, execution_quality, expected_net_edge_bps) <= 0:
            return LeverageDecision(False, None, 0.0, 0.0, 0.0, "INVALID_DYNAMIC_LEVERAGE_INPUTS")
        invalidation_fraction = risk_to_invalidation / unit_notional
        stress_fraction = max(invalidation_fraction, volatility_fraction * 3.0)
        required_buffer = stress_fraction * self.stress_buffer_multiple
        max_safe_by_buffer = 1.0 / required_buffer if required_buffer > 0 else 0.0
        max_safe = min(float(maximum_leverage), max_safe_by_buffer)
        if max_safe < 1.0:
            return LeverageDecision(False, None, 0.0, 0.0, required_buffer, "LIQUIDATION_BUFFER_CANNOT_BE_MAINTAINED")
        edge_strength = min(1.0, max(0.05, expected_net_edge_bps / 50.0))
        quality_strength = min(1.0, max(0.05, execution_quality))
        raw = 1.0 + (max_safe - 1.0) * edge_strength * quality_strength
        selected = max(1.0, math.floor(raw / self.leverage_step + 1e-12) * self.leverage_step)
        liquidation_distance = 1.0 / selected
        if liquidation_distance < required_buffer:
            return LeverageDecision(False, None, 0.0, liquidation_distance, required_buffer, "DYNAMIC_LEVERAGE_BUFFER_CHECK_FAILED")
        return LeverageDecision(True, selected, unit_notional / selected, liquidation_distance, required_buffer,
                                "DYNAMIC_LEVERAGE_WITHIN_VERIFIED_MAXIMUM_AND_STRESS_BUFFER")

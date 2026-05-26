"""Venue-specific margin and premium-at-risk policy gates."""
from __future__ import annotations
class MarginPolicy:
    @staticmethod
    def groww_long_option(premium: float, quantity: int, fees: float, stress_slippage: float, available_cash: float) -> tuple[float, bool]:
        risk = float(premium) * int(quantity) + float(fees) + float(stress_slippage)
        return risk, risk > 0 and risk <= available_cash
    @staticmethod
    def leveraged_contract(notional: float, selected_leverage: float, available_margin: float, liquidation_buffer_ok: bool) -> tuple[float, bool]:
        if selected_leverage <= 0 or not liquidation_buffer_ok: return 0.0, False
        margin = notional / selected_leverage; return margin, 0 < margin <= available_margin

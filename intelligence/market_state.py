"""Shared feature construction for underlying, metal-reference and restriction state."""
from __future__ import annotations
from dataclasses import dataclass
import numpy as np
from market_data.candles import CandleSeries
from intelligence.volatility import EWMAVolatility

@dataclass(frozen=True)
class TradingRestrictionState:
    market_open: bool
    data_healthy: bool
    exchange_healthy: bool
    event_restricted: bool
    reason: str = ""
    @property
    def tradable(self) -> bool:
        return self.market_open and self.data_healthy and self.exchange_healthy and not self.event_restricted

class MarketStateFeatureBuilder:
    """Builds non-visual quantitative features; it never returns a trade decision."""
    def underlying_features(self, series: CandleSeries, *, spread_bps: float, liquidity_weakness: float, flow_persistence: float) -> tuple[np.ndarray, np.ndarray]:
        returns = series.returns(); ewma = EWMAVolatility(); vol = 0.0
        for value in returns:
            vol = ewma.update(float(value)).volatility
        signed_return = float(returns[-1]) if returns.size else 0.0
        regime = np.asarray([signed_return, vol, spread_bps, flow_persistence, liquidity_weakness], dtype=float)
        direction = np.asarray([signed_return, vol, spread_bps, flow_persistence, liquidity_weakness, series.directional_efficiency(), series.average_range()], dtype=float)
        return direction, regime
    def reference_features(self, reference_returns: np.ndarray, *, spread_bps: float = 0.0, liquidity_weakness: float = 0.0) -> np.ndarray:
        rows = np.asarray(reference_returns, dtype=float); ewma = EWMAVolatility(); vol = 0.0
        for value in rows: vol = ewma.update(float(value)).volatility
        momentum = float(rows[-1]) if rows.size else 0.0; persistence = float(np.sign(rows).mean()) if rows.size else 0.0
        return np.asarray([momentum, vol, spread_bps, persistence, liquidity_weakness], dtype=float)

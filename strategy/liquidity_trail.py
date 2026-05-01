"""
strategy/liquidity_trail.py

Institutional exit/trailing manager.

Rules:
- Bracket SL/TP remains source of truth.
- Trailing only improves SL; never worsens it.
- Breakeven requires structure confirmation and meaningful MFE.
- No blind R-based retail trailing.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional
import math


def _f(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else default
    except Exception:
        return default


@dataclass
class PositionState:
    side: str
    entry_price: float
    sl_price: float
    tp_price: float
    qty: float
    initial_sl_price: float
    peak_favorable: float = 0.0
    peak_adverse: float = 0.0
    thesis_id: str = ""


@dataclass(frozen=True)
class ExitContext:
    mark_price: float
    atr: float
    structure_trail_price: float = 0.0
    cisd_followthrough: bool = False
    opposing_pool_progress: bool = False
    inside_noise_zone: bool = False
    reached_route_midpoint: bool = False


@dataclass(frozen=True)
class TrailDecision:
    should_update: bool
    reason: str
    new_sl: float = 0.0


@dataclass(frozen=True)
class TrailConfig:
    be_min_r: float = 0.85
    structure_lock_min_r: float = 1.20
    atr_noise_buffer: float = 0.18


class LiquidityTrail:
    def __init__(self, config: Optional[TrailConfig] = None) -> None:
        self.cfg = config or TrailConfig()

    def update_excursions(self, pos: PositionState, mark_price: float) -> None:
        if pos.side.lower() == "long":
            favorable = max(0.0, mark_price - pos.entry_price)
            adverse = max(0.0, pos.entry_price - mark_price)
        else:
            favorable = max(0.0, pos.entry_price - mark_price)
            adverse = max(0.0, mark_price - pos.entry_price)
        pos.peak_favorable = max(pos.peak_favorable, favorable)
        pos.peak_adverse = max(pos.peak_adverse, adverse)

    def current_r(self, pos: PositionState, mark_price: float) -> float:
        initial_risk = abs(pos.entry_price - pos.initial_sl_price)
        if initial_risk <= 0:
            return 0.0
        pnl_pts = mark_price - pos.entry_price if pos.side.lower() == "long" else pos.entry_price - mark_price
        return pnl_pts / initial_risk

    def evaluate(self, pos: PositionState, ctx: ExitContext) -> TrailDecision:
        mark = _f(ctx.mark_price)
        atr = _f(ctx.atr)
        if mark <= 0 or atr <= 0:
            return TrailDecision(False, "invalid mark/ATR; no trail")

        self.update_excursions(pos, mark)
        r_now = self.current_r(pos, mark)

        if ctx.inside_noise_zone:
            return TrailDecision(False, "inside noise zone; do not compress SL")

        # Structure-first lock. Only use a real structure trail price from market engine.
        if ctx.structure_trail_price > 0 and r_now >= self.cfg.structure_lock_min_r:
            proposed = self._buffer_structure(pos.side, ctx.structure_trail_price, atr)
            if self._improves(pos.side, pos.sl_price, proposed) and self._not_crossing_mark(pos.side, proposed, mark):
                return TrailDecision(True, f"structure trail after {r_now:.2f}R", proposed)

        # BE only if market has earned it structurally.
        if r_now >= self.cfg.be_min_r and (ctx.cisd_followthrough or ctx.opposing_pool_progress or ctx.reached_route_midpoint):
            proposed = pos.entry_price
            if self._improves(pos.side, pos.sl_price, proposed) and self._not_crossing_mark(pos.side, proposed, mark):
                return TrailDecision(True, f"structure-confirmed breakeven after {r_now:.2f}R", proposed)

        return TrailDecision(False, f"hold SL; r={r_now:.2f}")

    def _buffer_structure(self, side: str, structure_price: float, atr: float) -> float:
        if side.lower() == "long":
            return structure_price - atr * self.cfg.atr_noise_buffer
        return structure_price + atr * self.cfg.atr_noise_buffer

    def _improves(self, side: str, old_sl: float, new_sl: float) -> bool:
        return new_sl > old_sl if side.lower() == "long" else new_sl < old_sl

    def _not_crossing_mark(self, side: str, sl: float, mark: float) -> bool:
        return sl < mark if side.lower() == "long" else sl > mark

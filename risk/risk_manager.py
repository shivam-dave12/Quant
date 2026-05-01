"""
risk/risk_manager.py

Institutional risk manager.

Rules:
- Position size is risk-based first, margin/leverage constrained second.
- No fake balance, no fake leverage, no fallback size.
- Rejects if exchange constraints cannot be satisfied.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional
import math
import logging

logger = logging.getLogger(__name__)


def _f(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else default
    except Exception:
        return default


@dataclass(frozen=True)
class AccountState:
    available_balance: float
    total_balance: float
    currency: str = "USD"


@dataclass(frozen=True)
class ExchangeConstraints:
    min_qty: float = 0.001
    qty_step: float = 0.001
    min_notional: float = 5.0
    max_leverage: float = 40.0
    contract_value: float = 1.0


@dataclass(frozen=True)
class PositionSizeResult:
    accepted: bool
    reason: str
    qty: float = 0.0
    notional: float = 0.0
    margin_required: float = 0.0
    risk_usd: float = 0.0
    risk_pct: float = 0.0


@dataclass(frozen=True)
class RiskConfig:
    risk_per_trade_pct: float = 0.0035
    max_margin_usage_pct: float = 0.20
    max_daily_loss_pct: float = 0.025
    max_consecutive_losses: int = 3
    leverage: float = 40.0
    conviction_min_multiplier: float = 0.50
    conviction_max_multiplier: float = 1.25


class RiskManager:
    def __init__(self, config: Optional[RiskConfig] = None, constraints: Optional[ExchangeConstraints] = None) -> None:
        self.cfg = config or RiskConfig()
        self.constraints = constraints or ExchangeConstraints()
        self.session_realized_pnl = 0.0
        self.consecutive_losses = 0

    def register_closed_trade(self, pnl: float) -> None:
        pnl = _f(pnl)
        self.session_realized_pnl += pnl
        self.consecutive_losses = self.consecutive_losses + 1 if pnl < 0 else 0

    def trading_allowed(self, account: AccountState) -> tuple[bool, str]:
        bal = _f(account.total_balance)
        if bal <= 0:
            return False, "account total balance invalid"
        if self.session_realized_pnl <= -bal * self.cfg.max_daily_loss_pct:
            return False, "daily loss limit reached"
        if self.consecutive_losses >= self.cfg.max_consecutive_losses:
            return False, "consecutive loss limit reached"
        return True, "risk allowed"

    def _round_qty(self, qty: float) -> float:
        step = max(_f(self.constraints.qty_step, 0.001), 1e-12)
        return math.floor(qty / step) * step

    def calculate_position_size(
        self,
        *,
        account: AccountState,
        entry_price: float,
        sl_price: float,
        side: str,
        conviction: float,
        size_multiplier: float = 1.0,
    ) -> PositionSizeResult:
        allowed, reason = self.trading_allowed(account)
        if not allowed:
            return PositionSizeResult(False, reason)

        available = _f(account.available_balance)
        total = _f(account.total_balance)
        entry = _f(entry_price)
        sl = _f(sl_price)
        if available <= 0 or total <= 0:
            return PositionSizeResult(False, "available/total balance invalid")
        if entry <= 0 or sl <= 0:
            return PositionSizeResult(False, "entry/sl invalid")

        risk_per_unit = abs(entry - sl) * max(_f(self.constraints.contract_value, 1.0), 1e-12)
        if risk_per_unit <= 0:
            return PositionSizeResult(False, "risk per unit <= 0")

        conviction = max(0.0, min(1.0, _f(conviction)))
        conv_mult = self.cfg.conviction_min_multiplier + conviction * (self.cfg.conviction_max_multiplier - self.cfg.conviction_min_multiplier)
        risk_usd = total * self.cfg.risk_per_trade_pct * conv_mult * max(0.0, _f(size_multiplier, 1.0))
        if risk_usd <= 0:
            return PositionSizeResult(False, "risk budget <= 0")

        risk_qty = risk_usd / risk_per_unit

        leverage = min(_f(self.cfg.leverage), _f(self.constraints.max_leverage))
        if leverage <= 0:
            return PositionSizeResult(False, "leverage invalid")
        max_margin = available * self.cfg.max_margin_usage_pct
        max_notional = max_margin * leverage
        max_qty_by_margin = max_notional / (entry * max(_f(self.constraints.contract_value, 1.0), 1e-12))

        qty = min(risk_qty, max_qty_by_margin)
        qty = self._round_qty(qty)
        notional = qty * entry * max(_f(self.constraints.contract_value, 1.0), 1e-12)
        margin_required = notional / leverage if leverage > 0 else 0.0

        if qty < self.constraints.min_qty:
            return PositionSizeResult(False, f"qty {qty:.8f} below exchange min {self.constraints.min_qty:.8f}")
        if notional < self.constraints.min_notional:
            return PositionSizeResult(False, f"notional {notional:.2f} below exchange min {self.constraints.min_notional:.2f}")
        if margin_required > available:
            return PositionSizeResult(False, "margin required exceeds available balance")

        actual_risk = qty * risk_per_unit
        return PositionSizeResult(True, "size accepted", qty, notional, margin_required, actual_risk, actual_risk / total)

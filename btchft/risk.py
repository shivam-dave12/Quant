from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np

from .types import AlphaDecision, BookSnapshot, BracketPlan, Side


@dataclass
class AccountState:
    equity_usd: float
    day_start_equity_usd: float
    open_contracts: int = 0
    gross_notional_usd: float = 0.0
    halted_reason: str | None = None


class AdaptiveBracketPolicy:
    """Learns TP/SL from realised post-signal paths, with conservative cold-start fallback.

    Cold-start fallback is allowed in SHADOW/PAPER. In LIVE it is blocked unless the
    operator explicitly sets Settings.allow_unvalidated_bootstrap_live=True in config.py.
    This keeps the bot complete while preventing fake validated execution.
    """

    def __init__(self) -> None:
        self.mfe: list[float] = []
        self.mae: list[float] = []
        self.closed_outcomes: list[float] = []

    def observe_path(self, signed_path_bps: list[float]) -> None:
        if not signed_path_bps:
            return
        self.mfe.append(max(0.0, float(max(signed_path_bps))))
        self.mae.append(max(0.0, -float(min(signed_path_bps))))
        self.closed_outcomes.append(float(signed_path_bps[-1]))
        self.mfe = self.mfe[-10000:]; self.mae = self.mae[-10000:]; self.closed_outcomes = self.closed_outcomes[-10000:]

    @property
    def sample_count(self) -> int:
        return len(self.mfe)

    def barriers_bps(self, cost_bps: float, allow_cold_start: bool) -> tuple[float, float, dict[str, Any]] | None:
        if self.sample_count >= 500:
            target = max(cost_bps + 1.5, float(np.quantile(np.asarray(self.mfe), 0.45)))
            stop = max(2.0, float(np.quantile(np.asarray(self.mae), 0.80)))
            return target, stop, {"source": "learned_real_post_signal_excursion", "samples": self.sample_count}
        if allow_cold_start:
            # Conservative bootstrap: not promoted alpha, but protective bracket exists for paper/explicit live override.
            target = max(cost_bps + 2.0, 12.0)
            stop = max(6.0, min(target * 0.75, 18.0))
            return target, stop, {"source": "cold_start_protective_fallback_not_validated", "samples": self.sample_count}
        return None


class RiskEngine:
    def __init__(self, *, max_risk_per_trade: float, max_gross_leverage: float, daily_drawdown_halt: float, min_net_edge_bps: float, max_open_contracts: int, contract_value_btc: float = 0.001) -> None:
        self.max_risk_per_trade = float(max_risk_per_trade)
        self.max_gross_leverage = float(max_gross_leverage)
        self.daily_drawdown_halt = float(daily_drawdown_halt)
        self.min_net_edge_bps = float(min_net_edge_bps)
        self.max_open_contracts = int(max_open_contracts)
        self.contract_value_btc = float(contract_value_btc)
        self.brackets = AdaptiveBracketPolicy()

    def halted(self, account: AccountState) -> bool:
        return bool(account.halted_reason) or account.equity_usd <= account.day_start_equity_usd * (1.0 - self.daily_drawdown_halt)

    def build_bracket_plan(self, decision: AlphaDecision, book: BookSnapshot, account: AccountState, cost_bps: float, *, allow_cold_start_bracket: bool) -> BracketPlan | None:
        if self.halted(account) or decision.side is Side.FLAT:
            return None
        if decision.expected_net_edge_bps < self.min_net_edge_bps:
            return None
        if abs(account.open_contracts) >= self.max_open_contracts:
            return None
        barriers = self.brackets.barriers_bps(cost_bps, allow_cold_start=allow_cold_start_bracket)
        if barriers is None:
            return None
        target_bps, stop_bps, bracket_meta = barriers
        if target_bps <= cost_bps + 1.0:
            return None
        entry = book.mid
        if decision.side is Side.LONG:
            stop = entry * (1.0 - stop_bps / 1e4)
            target = entry * (1.0 + target_bps / 1e4)
        else:
            stop = entry * (1.0 + stop_bps / 1e4)
            target = entry * (1.0 - target_bps / 1e4)
        stop_distance = abs(entry - stop) / max(entry, 1e-12)
        risk_usd = account.equity_usd * self.max_risk_per_trade * float(np.clip((decision.confidence - 0.5) / 0.25, 0.25, 1.0))
        notional_by_risk = risk_usd / max(stop_distance, 1e-12)
        remaining_notional = max(0.0, account.equity_usd * self.max_gross_leverage - account.gross_notional_usd)
        notional = min(notional_by_risk, remaining_notional)
        contracts = int(np.floor(notional / (entry * self.contract_value_btc)))
        if contracts < 1:
            return None
        return BracketPlan(
            side=decision.side, entry_price=entry, stop_price=stop, take_profit_price=target,
            quantity_contracts=contracts, risk_usd=risk_usd,
            expected_net_edge_bps=decision.expected_net_edge_bps, confidence=decision.confidence,
            rationale={"target_bps": target_bps, "stop_bps": stop_bps, "bracket_policy": bracket_meta, "cost_bps": cost_bps},
        )

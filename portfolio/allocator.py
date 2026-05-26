"""Expected-Shortfall constrained allocation and venue-valid per-trade sizing."""
from __future__ import annotations
import math
import numpy as np
from scipy.optimize import minimize
from core.identifiers import PositionSizingDecision
from portfolio.exposure import Exposure, ExposureBook
from portfolio.covariance import ShrunkEWMACovariance
from portfolio.expected_shortfall import ExpectedShortfallModel
from portfolio.drawdown import DrawdownController

class PortfolioAllocator:
    def __init__(self, *, exposure_book: ExposureBook, covariance: ShrunkEWMACovariance, expected_shortfall: ExpectedShortfallModel,
                 drawdown: DrawdownController, portfolio_risk_cap: float, portfolio_es_cap: float, max_risk_per_opportunity: float,
                 group_caps: dict[str, float] | None = None) -> None:
        self.book = exposure_book; self.covariance = covariance; self.expected_shortfall = expected_shortfall; self.drawdown = drawdown
        self.portfolio_risk_cap = float(portfolio_risk_cap); self.portfolio_es_cap = float(portfolio_es_cap); self.max_risk_per_opportunity = float(max_risk_per_opportunity)
        self.group_caps = group_caps or {"CRYPTO_BETA": portfolio_risk_cap, "METAL_BETA": portfolio_risk_cap, "INDIA_INDEX_DELTA": portfolio_risk_cap}
    @staticmethod
    def _round_down(value: float, step: float) -> float:
        if step <= 0: raise ValueError("venue step must be positive")
        return math.floor(value / step + 1e-12) * step
    def _risk(self, exposures: list[Exposure]) -> tuple[float, float]:
        return self.covariance.portfolio_volatility(exposures), self.expected_shortfall.expected_shortfall(exposures)
    def optimise_budgets(self, expected_net_edges: np.ndarray, liquidity_penalties: np.ndarray, base_exposures: list[Exposure], candidate_templates: list[Exposure], risk_aversion: float = 1.0, liquidity_aversion: float = 0.25) -> np.ndarray:
        n = len(candidate_templates); edges = np.asarray(expected_net_edges, dtype=float); penalties = np.asarray(liquidity_penalties, dtype=float)
        objective = lambda weights: -(float(weights @ edges) - risk_aversion * self.expected_shortfall.expected_shortfall([*base_exposures, *[Exposure(t.instrument,t.desk,t.risk_group,t.currency,t.notional*w,t.signed_delta_notional*w,t.risk_to_invalidation*w,t.stress_slippage_risk*w,t.margin_used*w) for t,w in zip(candidate_templates, weights) if w>0]]) - liquidity_aversion * float(weights @ penalties))
        constraints = [{"type": "ineq", "fun": lambda w: self.portfolio_es_cap - self.expected_shortfall.expected_shortfall([*base_exposures, *[Exposure(t.instrument,t.desk,t.risk_group,t.currency,t.notional*x,t.signed_delta_notional*x,t.risk_to_invalidation*x,t.stress_slippage_risk*x,t.margin_used*x) for t,x in zip(candidate_templates, w) if x>0]])}]
        result = minimize(objective, np.zeros(n), method="SLSQP", bounds=[(0.0, 1.0)] * n, constraints=constraints)
        if not result.success: raise RuntimeError(f"PORTFOLIO_OPTIMISATION_FAILED:{result.message}")
        return result.x
    def size(self, *, desk: str, risk_group: str, currency: str, instrument: str, expected_net_edge: float, unit_invalidation_risk: float,
             unit_stress_slippage_risk: float, unit_notional: float, unit_margin: float, liquidity_capacity_qty: float, available_margin: float,
             venue_step: float, max_qty: float | None = None, leverage_selected: float | None = None) -> PositionSizingDecision:
        existing = self.book.items(currency=currency); before_risk, before_es = self._risk(existing); reasons: list[str] = []
        def reject(why: str, after_risk: float = before_risk, after_es: float = before_es) -> PositionSizingDecision:
            return PositionSizingDecision(desk, instrument, False, 0.0, 0.0, 0.0, leverage_selected, 0.0, 0.0, expected_net_edge,
                liquidity_capacity_qty, before_risk, after_risk, before_es, after_es, (why,))
        if self.book.identical_contract_open(instrument): return reject("ONE_DIRECTIONAL_POSITION_PER_IDENTICAL_CONTRACT")
        unit_loss = unit_invalidation_risk + unit_stress_slippage_risk
        if expected_net_edge <= 0 or min(unit_loss, unit_notional, unit_margin, available_margin, liquidity_capacity_qty) <= 0: return reject("INVALID_EDGE_RISK_MARGIN_OR_LIQUIDITY_INPUT")
        allowed_group = self.group_caps.get(risk_group, self.portfolio_risk_cap) - self.book.group_risk(risk_group, currency)
        risk_budget = min(self.max_risk_per_opportunity, max(0.0, self.portfolio_risk_cap - before_risk), max(0.0, allowed_group))
        raw = min(risk_budget / unit_loss, available_margin / unit_margin, liquidity_capacity_qty, max_qty if max_qty is not None else float("inf"))
        quantity = self._round_down(raw, venue_step)
        if quantity <= 0: return reject("NO_VENUE_VALID_QUANTITY_IN_RISK_ENVELOPE")
        candidate = Exposure(instrument, desk, risk_group, currency, quantity * unit_notional, quantity * unit_notional,
            quantity * unit_invalidation_risk, quantity * unit_stress_slippage_risk, quantity * unit_margin)
        after_risk, after_es = self._risk([*existing, candidate]); allowed, why = self.drawdown.permit_new_risk(desk, candidate.risk_to_invalidation + candidate.stress_slippage_risk)
        if not allowed: return reject(why, after_risk, after_es)
        if after_risk > self.portfolio_risk_cap: return reject("CORRELATED_PORTFOLIO_RISK_CAP", after_risk, after_es)
        if after_es > self.portfolio_es_cap: return reject("EXPECTED_SHORTFALL_CAP", after_risk, after_es)
        reasons.append("EXPECTED_SHORTFALL_COVARIANCE_MARGIN_LIQUIDITY_AND_DRAWDOWN_APPROVED")
        return PositionSizingDecision(desk, instrument, True, quantity, candidate.notional, candidate.margin_used, leverage_selected,
            candidate.risk_to_invalidation, candidate.stress_slippage_risk, expected_net_edge, liquidity_capacity_qty, before_risk, after_risk, before_es, after_es, tuple(reasons))

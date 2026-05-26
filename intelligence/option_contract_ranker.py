"""Underlying-directed long CE/PE ranker using Black-76, independent IV/Greeks and expected premium edge."""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, UTC
import numpy as np
from core.identifiers import IndiaUnderlyingOpportunity, LongOptionCandidateScore
from intelligence.option_pricing import black76_greeks, implied_volatility
from intelligence.predictive_models import LightGBMReturnModel, LightGBMProbabilityModel, FillHazardModel

@dataclass(frozen=True)
class OptionChainCandidate:
    trading_symbol: str
    option_type: str
    expiry: str
    strike: float
    bid: float
    ask: float
    depth_quantity: int
    volume: int
    open_interest: int
    lot_size: int
    protection_feasible: bool

@dataclass
class OptionRankingModels:
    net_premium_return_ranker: LightGBMReturnModel | None = None
    tp_before_sl_classifier: LightGBMProbabilityModel | None = None
    fill_hazard_model: FillHazardModel | None = None
    promoted: bool = False
    version: str = "deterministic-black76-v1"

class LongOptionRanker:
    def __init__(self, *, max_spread_bps: float, max_premium_risk: float, minimum_liquidity_score: float, risk_free_rate: float = 0.065,
                 models: OptionRankingModels | None = None) -> None:
        self.max_spread_bps = max_spread_bps; self.max_premium_risk = max_premium_risk; self.minimum_liquidity_score = minimum_liquidity_score
        self.risk_free_rate = risk_free_rate; self.models = models or OptionRankingModels()
    def rank(self, *, opportunity: IndiaUnderlyingOpportunity, candidates: list[OptionChainCandidate], forward_price: float,
             expected_iv_change: float, fee_per_lot: float, stress_slippage_per_lot: float, now: datetime | None = None) -> list[LongOptionCandidateScore]:
        if opportunity.direction not in {"BULLISH", "BEARISH"}: return []
        required_type = "CE" if opportunity.direction == "BULLISH" else "PE"; now = now or datetime.now(UTC)
        results: list[LongOptionCandidateScore] = []
        for candidate in candidates:
            reason = None
            if candidate.option_type.upper() != required_type: continue
            expiry_dt = datetime.fromisoformat(candidate.expiry).replace(tzinfo=UTC) if "T" not in candidate.expiry else datetime.fromisoformat(candidate.expiry)
            time_years = max((expiry_dt - now).total_seconds(), 0.0) / (365.0 * 24.0 * 3600.0)
            premium = (candidate.bid + candidate.ask) / 2.0
            if time_years <= 0 or premium <= 0: reason = "EXPIRED_OR_INVALID_PREMIUM"
            spread_bps = float("inf") if premium <= 0 else (candidate.ask - candidate.bid) / premium * 10_000.0
            liquidity = min(1.0, candidate.depth_quantity / max(candidate.lot_size * 5, 1)) * min(1.0, np.log1p(candidate.volume + candidate.open_interest) / 14.0)
            iv = theoretical = delta = gamma = theta = vega = 0.0
            try:
                iv = implied_volatility(premium, forward_price, candidate.strike, self.risk_free_rate, time_years, candidate.option_type)
                greeks = black76_greeks(forward_price, candidate.strike, self.risk_free_rate, time_years, iv, candidate.option_type)
                theoretical, delta, gamma, theta, vega = greeks.price, greeks.delta, greeks.gamma, greeks.theta, greeks.vega
            except ValueError:
                reason = reason or "BLACK76_IV_NOT_SOLVABLE"
            direction_move = opportunity.expected_move_points if required_type == "CE" else -opportunity.expected_move_points
            delta_v = delta * direction_move + 0.5 * gamma * opportunity.expected_move_points ** 2 + vega * expected_iv_change + theta * (opportunity.expected_holding_minutes / (365.0 * 24.0 * 60.0))
            total_cost = (candidate.ask - candidate.bid) * candidate.lot_size + fee_per_lot + stress_slippage_per_lot
            expected_net = delta_v * candidate.lot_size - total_cost
            premium_risk = candidate.ask * candidate.lot_size + fee_per_lot + stress_slippage_per_lot
            if spread_bps > self.max_spread_bps: reason = reason or "SPREAD_UNEXECUTABLE"
            if liquidity < self.minimum_liquidity_score: reason = reason or "LIQUIDITY_UNEXECUTABLE"
            if premium_risk > self.max_premium_risk: reason = reason or "PREMIUM_RISK_EXCEEDED"
            if not candidate.protection_feasible: reason = reason or "OCO_NOT_FEASIBLE"
            base_features = np.asarray([candidate.strike / forward_price - 1.0, premium, spread_bps, delta, gamma, theta, vega, iv, liquidity,
                opportunity.expected_move_points, opportunity.expected_holding_minutes, expected_iv_change], dtype=float)
            probability = None
            fill_probability = None
            model_component = 0.0
            if self.models.promoted and self.models.net_premium_return_ranker and self.models.tp_before_sl_classifier and self.models.fill_hazard_model:
                model_component = self.models.net_premium_return_ranker.predict(base_features)
                probability = self.models.tp_before_sl_classifier.probability(base_features)
                fill_probability = self.models.fill_hazard_model.probability_fill(base_features)
            else:
                probability = max(0.0, min(1.0, 0.50 + expected_net / max(2.0 * premium_risk, 1.0)))
                fill_probability = max(0.0, min(1.0, liquidity * max(0.0, 1.0 - spread_bps / max(self.max_spread_bps, 1.0))))
            score = ((expected_net + model_component) / max(premium_risk, 1e-12)) * probability * fill_probability * liquidity if reason is None else -1e9
            results.append(LongOptionCandidateScore(candidate.trading_symbol, candidate.option_type, candidate.expiry, candidate.strike, premium,
                spread_bps, delta, gamma, theta, vega, iv, theoretical, iv, expected_iv_change, expected_net, probability, fill_probability,
                -theta * opportunity.expected_holding_minutes / (365.0 * 24.0 * 60.0), liquidity, candidate.protection_feasible, score, reason, candidate.lot_size))
        return sorted(results, key=lambda row: row.total_score, reverse=True)

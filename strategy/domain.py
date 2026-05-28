"""Institutional strategy domain objects.

The strategy package now exposes one decision language for every desk:
validated data, market state, regime, liquidity, edge, portfolio allocation,
protected execution, and research attribution.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class DecisionOutput(str, Enum):
    NO_TRADE_INSUFFICIENT_EDGE = "NO_TRADE_INSUFFICIENT_EDGE"
    NO_TRADE_EXECUTION_UNSAFE = "NO_TRADE_EXECUTION_UNSAFE"
    NO_TRADE_RISK_BUDGET = "NO_TRADE_RISK_BUDGET"
    SHADOW_SIGNAL_VALIDATED = "SHADOW_SIGNAL_VALIDATED"
    TRADE_APPROVED_WITH_PROTECTION_PLAN = "TRADE_APPROVED_WITH_PROTECTION_PLAN"


class Direction(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    NO_TRADE = "NO_TRADE"
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"


class DeskId(str, Enum):
    BTC = "DESK_A_BTC"
    METALS = "DESK_A_METALS"
    COMMODITIES = "DESK_A_COMMODITIES"
    INDIA_OPTIONS = "DESK_B_NIFTY_OPTIONS"


class Regime(str, Enum):
    BALANCE = "balance"
    EXPANSION = "expansion"
    TREND = "trend"
    SHOCK = "shock"
    ILLIQUID = "illiquid"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class LiquidityZoneScore:
    instrument: str
    direction_context: str
    price_low: float
    price_high: float
    source_timeframes: list[str]
    age_seconds: float
    touch_count: int
    sweep_count: int
    depletion_score: float
    absorption_score: float
    estimated_liquidity_score: float
    volatility_adjusted_distance: float
    execution_cost_score: float
    cross_market_alignment_score: float
    stop_vulnerability_score: float
    entry_utility_score: float
    tp_utility_score: float
    sl_safety_score: float


@dataclass(frozen=True)
class PositionSizingDecision:
    desk: str
    instrument: str
    approved: bool
    quantity: float
    notional: float
    margin_required: float
    leverage_selected: float | None
    risk_to_invalidation: float
    expected_net_edge: float
    liquidity_capacity_cap: float
    portfolio_risk_before: float
    portfolio_risk_after: float
    reasons: list[str]
    # Capital attribution is mandatory in multi-venue execution: the approved
    # quantity must be traceable to the same broker that receives the order.
    capital_venue: str = ""
    available_cash_used: float = 0.0
    balance_source: str = ""


@dataclass(frozen=True)
class ProtectionPlan:
    entry_price: float
    stop_price: float
    target_price: float
    protection_type: str
    protection_feasible: bool
    reasons: list[str] = field(default_factory=list)
    diagnostics: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class OpportunityDecision:
    desk: str
    venue: str
    instrument: str
    decision: DecisionOutput
    direction: Direction
    regime: Regime
    expected_net_edge_bps: float
    uncertainty_bps: float
    liquidity_score: float
    execution_quality_score: float
    sizing: PositionSizingDecision | None
    protection_plan: ProtectionPlan | None
    reasons: list[str]
    model_values: dict[str, Any]
    research_features: dict[str, Any]

    @property
    def approved(self) -> bool:
        return self.decision is DecisionOutput.TRADE_APPROVED_WITH_PROTECTION_PLAN


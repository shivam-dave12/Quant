"""Typed immutable domain objects shared by all desks and model pipelines."""
from __future__ import annotations
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Literal

class DecisionCode(str, Enum):
    NO_TRADE_INSUFFICIENT_EDGE = "NO_TRADE_INSUFFICIENT_EDGE"
    NO_TRADE_EXECUTION_UNSAFE = "NO_TRADE_EXECUTION_UNSAFE"
    NO_TRADE_RISK_BUDGET = "NO_TRADE_RISK_BUDGET"
    TRADE_APPROVED_WITH_PROTECTION_PLAN = "TRADE_APPROVED_WITH_PROTECTION_PLAN"

class Direction(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    NO_TRADE = "NO_TRADE"

class ProtectionState(str, Enum):
    CANDIDATE_SELECTED = "CANDIDATE_SELECTED"
    ENTRY_SUBMITTED = "ENTRY_SUBMITTED"
    ENTRY_PARTIAL_OR_FILLED = "ENTRY_PARTIAL_OR_FILLED"
    PROTECTION_SUBMITTED_FOR_FILLED_QTY = "PROTECTION_SUBMITTED_FOR_FILLED_QTY"
    PROTECTION_CONFIRMED = "PROTECTION_CONFIRMED"
    ACTIVE_PROTECTED_POSITION = "ACTIVE_PROTECTED_POSITION"
    EXITED_TARGET = "EXITED_TARGET"
    EXITED_STOP = "EXITED_STOP"
    EXITED_THESIS_FAILURE = "EXITED_THESIS_FAILURE"
    EXITED_THETA_IV_FAILURE = "EXITED_THETA_IV_FAILURE"
    MANUAL_EMERGENCY_EXIT = "MANUAL_EMERGENCY_EXIT"
    UNPROTECTED_POSITION_EMERGENCY = "UNPROTECTED_POSITION_EMERGENCY"
    RECONCILIATION_REQUIRED = "RECONCILIATION_REQUIRED"

@dataclass(frozen=True)
class InstrumentMapping:
    venue: str
    venue_symbol: str
    canonical_underlying: str
    product_class: str
    quote_currency: str
    contract_multiplier: float
    settlement_currency: str
    price_tick: float
    qty_step: float
    execution_enabled: bool
    notional_formula: Literal["linear_base", "inverse_usd", "quote_notional"] = "linear_base"
    metadata: dict[str, Any] = field(default_factory=dict)
    def validate(self) -> None:
        if not all((self.venue, self.venue_symbol, self.canonical_underlying, self.product_class)):
            raise ValueError("instrument identifiers are required")
        if min(self.contract_multiplier, self.price_tick, self.qty_step) <= 0:
            raise ValueError("multiplier/tick/quantity step must be positive")
        if self.notional_formula not in {"linear_base", "inverse_usd", "quote_notional"}:
            raise ValueError("unsupported notional formula")

@dataclass(frozen=True)
class BookLevel:
    price: float
    displayed_size: float

@dataclass(frozen=True)
class FeedHealth:
    connected: bool
    heartbeat_ok: bool
    sequence_valid: bool
    snapshot_ready: bool
    exchange_timestamp_available: bool
    latency_vs_baseline_z: float | None
    no_change_heartbeat_valid: bool
    quality_score: float
    reason: str = ""

@dataclass(frozen=True)
class VenueMicrostate:
    venue: str
    symbol: str
    exchange_ts_ns: int | None
    receive_ts_ns: int
    feed_quality_score: float
    best_bid: float
    best_ask: float
    mid: float
    microprice: float
    spread_bps: float
    bid_depth_usd_by_band: dict[str, float]
    ask_depth_usd_by_band: dict[str, float]
    obi_by_band: dict[str, float]
    ofi_usd_1s: float
    ofi_usd_10s: float
    ofi_usd_60s: float
    tfi_usd_1s: float
    tfi_usd_10s: float
    tfi_usd_60s: float
    basis_bps: float | None
    funding_rate: float | None
    update_latency_ms: float | None
    sequence_valid: bool

@dataclass(frozen=True)
class BTCCompositeState:
    delta_state: VenueMicrostate
    reference_states: dict[str, VenueMicrostate]
    composite_reference_mid: float | None
    delta_dislocation_bps: float | None
    flow_agreement_score: float
    cross_venue_dispersion_bps: float
    candidate_leader_venue: str | None
    leader_confidence: float | None
    delta_execution_quality_score: float

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
    predicted_valid_entry_probability: float | None = None
    predicted_tp_before_sl_probability: float | None = None
    predicted_stop_sweep_probability: float | None = None

@dataclass(frozen=True)
class ProtectionPlan:
    entry_price: float
    target_price: float
    stop_price: float
    invalidation_price: float
    quantity: float | None = None
    target_probability: float | None = None
    stop_sweep_probability: float | None = None
    reason: str = ""

@dataclass(frozen=True)
class CostEstimate:
    fees_bps: float
    spread_bps: float
    slippage_bps: float
    impact_bps: float
    protection_cost_bps: float = 0.0
    basis_risk_bps: float = 0.0
    uncertainty_bps: float = 0.0
    @property
    def total_bps(self) -> float:
        return self.fees_bps + self.spread_bps + self.slippage_bps + self.impact_bps + self.protection_cost_bps + self.basis_risk_bps + self.uncertainty_bps

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
    stress_slippage_risk: float
    expected_net_edge: float
    liquidity_capacity_cap: float
    portfolio_risk_before: float
    portfolio_risk_after: float
    expected_shortfall_before: float
    expected_shortfall_after: float
    reasons: tuple[str, ...]

@dataclass(frozen=True)
class OpportunityDecision:
    desk: str
    instrument: str
    direction: str
    code: DecisionCode
    net_edge_bps: float
    execution_quality_score: float
    size_approved: bool
    protection_plan: ProtectionPlan | None
    reasons: tuple[str, ...]
    metrics: dict[str, Any] = field(default_factory=dict)
    sizing: PositionSizingDecision | None = None
    @property
    def approved(self) -> bool:
        return self.code is DecisionCode.TRADE_APPROVED_WITH_PROTECTION_PLAN
    def as_dict(self) -> dict[str, Any]:
        output = asdict(self); output["code"] = self.code.value; return output

@dataclass(frozen=True)
class MetalOpportunity:
    asset: str
    tradable_symbol: str
    fair_value_reference: float
    local_price: float
    basis_bps: float
    tracking_error_bps: float
    reference_regime: str
    fair_value_alignment_score: float
    local_execution_score: float
    liquidity_plan_score: float
    direction: str
    predicted_move_bps: float
    net_edge_bps: float
    rejection_reason: str | None

@dataclass(frozen=True)
class IndiaUnderlyingOpportunity:
    underlying: str
    regime: str
    direction: str
    expected_move_points: float
    expected_holding_minutes: int
    confidence: float
    invalidation_level: float
    target_levels: list[float]
    liquidity_rationale: dict[str, Any]
    rejection_reason: str | None

@dataclass(frozen=True)
class LongOptionCandidateScore:
    trading_symbol: str
    option_type: str
    expiry: str
    strike: float
    premium: float
    spread_bps: float
    delta: float
    gamma: float
    theta: float
    vega: float
    iv: float
    black76_theoretical_price: float
    independently_calculated_iv: float
    expected_iv_change: float
    expected_premium_return_after_cost: float
    probability_tp_before_sl: float | None
    fill_probability: float | None
    theta_cost_for_expected_hold: float
    liquidity_score: float
    protection_feasible: bool
    total_score: float
    rejection_reason: str | None
    lot_size: int = 0

@dataclass(frozen=True)
class ExecutionReceipt:
    execution_id: str
    desk: str
    instrument: str
    state: ProtectionState
    entry_order_id: str | None = None
    protection_order_id: str | None = None
    requested_quantity: float = 0.0
    filled_quantity: float = 0.0
    average_fill_price: float = 0.0
    emergency_order_id: str | None = None
    fees_paid: float | None = None
    realised_slippage: float | None = None
    reason: str = ""

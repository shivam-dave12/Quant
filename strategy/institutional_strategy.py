"""Institutional multi-desk strategy runtime.

This module replaces the retired discretionary strategy with a deterministic
decision pipeline:

verified data -> normalised state -> regime -> liquidity -> edge -> allocation
-> protected execution -> reconciliation/research attribution.

The baseline is deliberately conservative. If a desk cannot build the required
state or protection plan, it emits an explicit no-trade decision with numeric
reasons instead of falling back to older entry logic.
"""

from __future__ import annotations

import json
import logging
import math
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Callable, Mapping, Optional

from core.instruments import ExchangeName, TradableInstrument
from intelligence.cross_venue_btc import BTCCompositeState, build_btc_composite_state
from market_data.feed_health import FeedHealth, score_feed_health
from market_data.normalizer import (
    InstrumentMapping,
    VenueMicrostate,
    build_venue_microstate,
)
from research.store import ForwardLabelWriter, JsonlResearchStore, ResearchDecisionRecord, ResearchExecutionRecord
from strategy.domain import (
    DecisionOutput,
    DeskId,
    Direction,
    LiquidityZoneScore,
    OpportunityDecision,
    PositionSizingDecision,
    ProtectionPlan,
    Regime,
)

logger = logging.getLogger(__name__)

try:
    import config
except Exception:  # pragma: no cover
    config = None  # type: ignore


def _cfg(name: str, default: Any) -> Any:
    return getattr(config, name, default) if config is not None else default


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        if isinstance(value, str):
            value = value.strip().replace(",", "")
            if not value:
                return default
        out = float(value)
        return out if math.isfinite(out) else default
    except Exception:
        return default


class PositionPhase(str, Enum):
    FLAT = "FLAT"
    ENTERING = "ENTERING"
    ACTIVE = "ACTIVE"
    EXITING = "EXITING"
    RECONCILIATION_REQUIRED = "RECONCILIATION_REQUIRED"


@dataclass
class PositionState:
    phase: PositionPhase = PositionPhase.FLAT
    side: str = ""
    quantity: float = 0.0
    entry_price: float = 0.0
    sl_price: float = 0.0
    tp_price: float = 0.0
    entry_order_id: str = ""
    sl_order_id: str = ""
    tp_order_id: str = ""
    entry_time: float = 0.0
    exchange: str = ""
    execution_symbol: str = ""
    asset_id: str = ""
    pnl_model: str = "linear"
    currency_symbol: str = "$"
    currency_code: str = "USD"
    quantity_unit: str = "contracts"
    entry_fee_paid: float = 0.0
    entry_fee_exact: bool = False
    entry_leverage: float = 1.0
    manual_exit_reason: str = ""
    protection_confirmed: bool = False
    protection_model: str = ""
    quant_components: dict[str, Any] = field(default_factory=dict)

    def is_flat(self) -> bool:
        return self.phase is PositionPhase.FLAT or self.quantity <= 0

    def to_public_dict(self) -> dict[str, Any] | None:
        if self.is_flat():
            return None
        return {
            "phase": self.phase.value,
            "side": self.side,
            "quantity": self.quantity,
            "entry_price": self.entry_price,
            "sl_price": self.sl_price,
            "tp_price": self.tp_price,
            "exchange": self.exchange,
            "execution_symbol": self.execution_symbol,
            "asset_id": self.asset_id,
            "pnl_model": self.pnl_model,
            "currency": self.currency_symbol,
            "protection_confirmed": self.protection_confirmed,
            "protection_model": self.protection_model,
        }


class DailyRiskGate:
    """Small deterministic daily loss gate used by the runtime watchdog."""

    def __init__(self) -> None:
        self.opening_balance = 0.0
        self.daily_realized_pnl = 0.0
        self.daily_trades = 0
        self.loss_lockout_until = 0.0

    def set_opening_balance(self, value: float) -> None:
        self.opening_balance = max(0.0, float(value or 0.0))

    def record_trade_start(self) -> None:
        self.daily_trades += 1

    def record_trade_result(self, pnl: float) -> None:
        self.daily_realized_pnl += float(pnl or 0.0)
        if pnl < 0:
            self.loss_lockout_until = time.time() + float(_cfg("INSTITUTIONAL_LOCKOUT_AFTER_LOSS_SEC", 180.0))

    def can_trade(self, equity: float) -> tuple[bool, str]:
        now = time.time()
        if now < self.loss_lockout_until:
            return False, "post_loss_lockout"
        if self.opening_balance > 0:
            dd = (float(equity or 0.0) / self.opening_balance - 1.0) * 100.0
            max_dd_pct = -abs(float(_cfg("PORTFOLIO_DAILY_DRAWDOWN_LIMIT_PCT", 3.0)))
            if dd <= max_dd_pct:
                return False, f"daily_drawdown_limit {dd:.2f}% <= {max_dd_pct:.2f}%"
        return True, "ok"


class InstitutionalStrategy:
    """Runtime adapter exposing the bot-facing strategy API."""

    def __init__(self, order_manager=None, *, instrument: TradableInstrument | None = None) -> None:
        self._om = order_manager
        self._instrument = instrument
        self._asset_id = getattr(instrument, "asset_id", str(_cfg("SYMBOL", "BTC")))
        self._pos = PositionState(asset_id=self._asset_id)
        self._risk_gate = DailyRiskGate()
        self._trade_history: deque[dict[str, Any]] = deque(maxlen=1000)
        self._last_decision: OpportunityDecision | None = None
        self._last_decision_ts = 0.0
        self._last_tick_time = 0.0
        self._price_window: deque[float] = deque(maxlen=240)
        self._market_wakeup: Callable[[], Any] | None = None
        store_root = str(_cfg("RESEARCH_STORE_PATH", "research_output"))
        self._research_store = JsonlResearchStore(store_root)
        self._forward_labels = ForwardLabelWriter(self._research_store)
        self._last_candidate_id = ""
        # Operator telemetry is state/change driven. The model can evaluate on
        # every market event without emitting an INFO-scale JSON payload per tick.
        self._telemetry_last_signature: tuple[Any, ...] | None = None
        self._telemetry_last_emit_ts: float = 0.0

    def bind_market_wakeup(self, callback: Callable[[], Any]) -> None:
        self._market_wakeup = callback

    def consume_market_event(self) -> bool:
        return False

    def has_urgent_structural_monitor(self) -> bool:
        return False

    def get_position(self) -> dict[str, Any] | None:
        return self._pos.to_public_dict()

    def get_stats(self) -> dict[str, Any]:
        wins = sum(1 for row in self._trade_history if _num(row.get("pnl"), 0.0) > 0)
        trades = len(self._trade_history)
        return {
            "strategy": "institutional_multi_desk",
            "asset_id": self._asset_id,
            "position_phase": self._pos.phase.value,
            "trades": trades,
            "wins": wins,
            "win_rate": wins / trades if trades else 0.0,
            "last_decision": self._last_decision.decision.value if self._last_decision else "NONE",
        }

    def format_status_report(self) -> str:
        if self._last_decision is None:
            return "<b>Institutional Strategy</b>\n<code>No decision produced yet.</code>"
        d = self._last_decision
        return (
            "<b>Institutional Strategy</b>\n"
            f"<code>{d.desk} {d.venue}:{d.instrument}</code>\n"
            f"<code>decision={d.decision.value} direction={d.direction.value} regime={d.regime.value}</code>\n"
            f"<code>edge={d.expected_net_edge_bps:.2f}bps uncertainty={d.uncertainty_bps:.2f}bps "
            f"liq={d.liquidity_score:.2f} exec={d.execution_quality_score:.2f}</code>\n"
            f"<code>reasons={'; '.join(d.reasons[:4])}</code>"
        )

    def on_tick(self, data_manager, order_manager, risk_manager, now_ms: int, *, event_driven: bool = False) -> None:
        _ = event_driven
        self._last_tick_time = time.time()
        if order_manager is not None:
            self._om = order_manager
        if not self._pos.is_flat():
            self._monitor_position(data_manager, order_manager, risk_manager)
            return
        decision = self.evaluate(data_manager, order_manager, risk_manager, now_ms)
        self._last_decision = decision
        self._last_decision_ts = time.time()
        self._log_decision_calculation(decision)
        self._persist_decision(decision, now_ms)
        if decision.approved:
            self._execute_approved(decision, order_manager, risk_manager)

    @staticmethod
    def _round_or_none(value: Any, digits: int = 3) -> float | None:
        try:
            out = float(value)
            return round(out, digits) if math.isfinite(out) else None
        except Exception:
            return None

    def _decision_telemetry_signature(self, decision: OpportunityDecision) -> tuple[Any, ...]:
        model = decision.model_values or {}
        option_context = model.get("option_volatility_context", {}) if isinstance(model.get("option_volatility_context"), dict) else {}
        execution_feed = model.get("execution_feed", {}) if isinstance(model.get("execution_feed"), dict) else {}
        session_book = model.get("session_execution_book", {}) if isinstance(model.get("session_execution_book"), dict) else {}
        reasons = list(decision.reasons[:2])
        signal_source = str(model.get("signal_source", ""))
        direction_key = decision.direction.value
        raw_non_actionable = {"ofi_tfi_microprice_long", "ofi_tfi_microprice_short", "flow_signal_flat"}
        collapse_unqualified = (
            not bool(_cfg("INSTITUTIONAL_DECISION_TELEMETRY_LOG_UNQUALIFIED_SIGNAL_FLIPS", False))
            and decision.decision is DecisionOutput.NO_TRADE_INSUFFICIENT_EDGE
            and reasons and reasons[0] in raw_non_actionable
            and "net_edge_does_not_clear_uncertainty_and_minimum" in reasons
        )
        if collapse_unqualified:
            # Raw flow may alternate LONG/SHORT on every event while still far below
            # executable edge. Keep the live value in periodic summaries, but do not
            # treat non-actionable flips as operational state transitions.
            reasons = ["unqualified_signal_below_required_edge", "net_edge_does_not_clear_uncertainty_and_minimum"]
            signal_source = "unqualified_signal"
            direction_key = "UNQUALIFIED_SIGNAL"
        return (
            decision.decision.value, direction_key, decision.regime.value, tuple(reasons),
            signal_source, str(model.get("thesis_reason", "")),
            bool(option_context.get("ready_for_long_premium_decision", False)),
            str(execution_feed.get("status", "")), str(session_book.get("status", "")),
            str(model.get("selected_option_symbol", "")),
        )

    @staticmethod
    def _compact_session_execution_book(status: Mapping[str, Any] | None) -> dict[str, Any]:
        status = dict(status or {})
        def side(name: str) -> dict[str, Any]:
            row = status.get(name, {}) if isinstance(status.get(name), dict) else {}
            return {
                "symbol": row.get("symbol", ""), "premium": row.get("live_premium") or row.get("premium"),
                "bid": row.get("live_bid"), "ask": row.get("live_ask"), "delta": row.get("delta"),
                "iv": row.get("iv"), "lot": row.get("lot"), "fresh": bool(row.get("ws_fresh", False)),
            }
        return {
            "status": status.get("status", "UNKNOWN"),
            "execution_feed_status": status.get("execution_feed_status", "UNKNOWN"),
            "call": side("call"), "put": side("put"),
        }

    def _compact_decision_payload(self, decision: OpportunityDecision, event: str) -> dict[str, Any]:
        model = decision.model_values or {}
        payload: dict[str, Any] = {
            "event": event, "desk": decision.desk, "instrument": decision.instrument,
            "decision": decision.decision.value, "direction": decision.direction.value,
            "regime": decision.regime.value, "net_edge_bps": self._round_or_none(decision.expected_net_edge_bps),
            "required_edge_bps": self._round_or_none(max(float(_cfg("INSTITUTIONAL_MIN_NET_EDGE_BPS", 3.0)), decision.uncertainty_bps)),
            "liquidity_score": self._round_or_none(decision.liquidity_score),
            "execution_quality": self._round_or_none(decision.execution_quality_score),
            "reason": list(decision.reasons[:2]),
        }
        if decision.desk == DeskId.INDIA_OPTIONS.value:
            vol = model.get("option_volatility_context", {}) if isinstance(model.get("option_volatility_context"), dict) else {}
            payload["domains"] = model.get("pricing_domains", {"signal": "NIFTY_UNDERLYING", "execution": "OPTION_PREMIUM"})
            payload["underlying"] = {
                "spot": self._round_or_none(model.get("underlying_price"), 2),
                "alignment_15m_bps": self._round_or_none(model.get("alignment_15m_bps")),
                "break_up_bps": self._round_or_none(model.get("break_up_bps")),
                "break_down_bps": self._round_or_none(model.get("break_down_bps")),
                "atr_5m": self._round_or_none(model.get("atr_5m"), 2),
            }
            payload["volatility"] = {
                "atm_iv_pct": self._round_or_none(_num(vol.get("atm_iv"), 0.0) * 100.0, 2),
                "realized_vol_pct": self._round_or_none(_num(vol.get("realized_vol_yang_zhang"), 0.0) * 100.0, 2),
                "vrp_pct": self._round_or_none(_num(vol.get("vrp"), 0.0) * 100.0, 2),
                "skew_25d_pct": self._round_or_none(_num(vol.get("skew_25d"), 0.0) * 100.0, 3),
                "term_slope_pct": self._round_or_none(_num(vol.get("term_slope"), 0.0) * 100.0, 3),
                "iv_coverage_pct": self._round_or_none(_num(vol.get("live_iv_coverage"), 0.0) * 100.0, 1),
                "long_premium_context_ready": bool(vol.get("ready_for_long_premium_decision", False)),
            }
            session_book = model.get("session_execution_book")
            if isinstance(session_book, dict) and session_book:
                payload["session_book"] = session_book
            if model.get("selected_option_symbol"):
                payload["active_option"] = {
                    "symbol": model.get("selected_option_symbol"),
                    "premium": self._round_or_none(model.get("selected_option_premium"), 2),
                    "delta": self._round_or_none(model.get("selected_option_delta"), 4),
                    "premium_edge_bps": self._round_or_none(model.get("premium_delta_edge_bps")),
                    "theta_hold_bps": self._round_or_none(model.get("theta_carry_bps_expected_hold")),
                    "cost_bps": self._round_or_none(model.get("costs_bps")),
                }
            if decision.liquidity_score == 0.0 and decision.direction is Direction.NO_TRADE:
                payload["liquidity_score_scope"] = "NO_DIRECTION_ACTIVATED_OPTION_YET; SEE_SESSION_BOOK_FOR_LIVE_CE_PE_BOOKS"
        else:
            costs = model.get("execution_cost_components", {}) if isinstance(model.get("execution_cost_components"), dict) else {}
            payload["microstructure"] = {
                "source": model.get("signal_source", ""),
                "edge_bps": self._round_or_none(model.get("directional_edge_bps")),
                "spread_bps": self._round_or_none(model.get("spread_bps", costs.get("spread_bps"))),
                "total_cost_bps": self._round_or_none(model.get("costs_bps")),
                "ofi_1s_usd": self._round_or_none(model.get("ofi_usd_1s"), 2),
                "ofi_10s_usd": self._round_or_none(model.get("ofi_usd_10s"), 2),
                "tfi_1s_usd": self._round_or_none(model.get("tfi_usd_1s"), 2),
                "tfi_10s_usd": self._round_or_none(model.get("tfi_usd_10s"), 2),
            }
        if decision.sizing is not None:
            payload["sizing"] = asdict(decision.sizing)
        if decision.protection_plan is not None:
            payload["protection"] = asdict(decision.protection_plan)
        return payload

    def _log_decision_calculation(self, decision: OpportunityDecision) -> None:
        """Emit live, decision-useful telemetry without INFO log flooding.

        The strategy may evaluate several times per second. INFO output is emitted
        immediately on state/signature transitions, periodically for a stable
        state, and with full details only for approved trades or when explicitly
        configured for troubleshooting. The append-only research record remains
        independent from operator log throttling.
        """
        if not bool(_cfg("INSTITUTIONAL_DECISION_TELEMETRY_ENABLED", True)):
            return
        now = time.time()
        signature = self._decision_telemetry_signature(decision)
        transition = signature != self._telemetry_last_signature
        heartbeat_sec = max(5.0, float(_cfg("INSTITUTIONAL_DECISION_TELEMETRY_HEARTBEAT_SEC", 30.0)))
        debug_every_tick = bool(_cfg("INSTITUTIONAL_DECISION_TELEMETRY_DEBUG_EVERY_TICK", False))
        if not transition and not decision.approved and not debug_every_tick and now - self._telemetry_last_emit_ts < heartbeat_sec:
            return
        event = "APPROVED" if decision.approved else ("TRANSITION" if transition else "HEARTBEAT")
        try:
            compact = self._compact_decision_payload(decision, event)
            logger.info("🧮 DECISION_%s %s", event, json.dumps(compact, sort_keys=True, separators=(",", ":"), default=str))
            full_detail = bool(decision.approved or debug_every_tick or (transition and _cfg("INSTITUTIONAL_DECISION_TELEMETRY_FULL_ON_TRANSITION", False)))
            if full_detail:
                sizing = asdict(decision.sizing) if decision.sizing is not None else None
                protection = asdict(decision.protection_plan) if decision.protection_plan is not None else None
                payload = {
                    "desk": decision.desk, "venue": decision.venue, "instrument": decision.instrument,
                    "decision": decision.decision.value, "direction": decision.direction.value,
                    "regime": decision.regime.value, "net_edge_bps": decision.expected_net_edge_bps,
                    "uncertainty_bps": decision.uncertainty_bps, "liquidity_score": decision.liquidity_score,
                    "execution_quality": decision.execution_quality_score, "reasons": decision.reasons,
                    "model": decision.model_values, "sizing": sizing, "protection": protection,
                }
                logger.info("🧾 DECISION_DETAIL %s", json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str))
        except Exception as exc:
            logger.warning("DECISION telemetry serialization failed: %s", exc)
        self._telemetry_last_signature = signature
        self._telemetry_last_emit_ts = now

    def evaluate(self, data_manager, order_manager, risk_manager, now_ms: int) -> OpportunityDecision:
        venue = self._venue(order_manager)
        instrument = self._symbol(data_manager, order_manager)
        desk = self._desk_id(venue, instrument)
        price = self._safe_price(data_manager)
        if price > 0:
            self._price_window.append(price)
        regime = self._regime()
        feed_health = self._feed_health(data_manager)
        execution_quality = feed_health.quality_score
        if price > 0:
            self._forward_labels.observe(now_ts_ns=time.time_ns(), current_price=price)
        if feed_health.quality_score <= 0:
            return self._decision(
                desk=desk,
                venue=venue,
                instrument=instrument,
                decision=DecisionOutput.NO_TRADE_EXECUTION_UNSAFE,
                direction=Direction.NO_TRADE,
                regime=regime,
                expected_net_edge_bps=0.0,
                uncertainty_bps=999.0,
                liquidity_score=0.0,
                execution_quality_score=0.0,
                sizing=None,
                protection_plan=None,
                reasons=[feed_health.reason],
                model_values={"feed_quality": feed_health.quality_score},
                research_features={"price": price, "feed_health": asdict(feed_health)},
            )

        if desk == DeskId.INDIA_OPTIONS.value:
            return self._evaluate_india_options(
                data_manager=data_manager, order_manager=order_manager, risk_manager=risk_manager,
                instrument=instrument, underlying_price=price, regime=regime, feed_health=feed_health,
            )

        execution_state, btc_composite = self._microstructure_context(data_manager, venue, instrument, feed_health)
        liquidity_score, zones = self._liquidity_score(data_manager, instrument, price, execution_state=execution_state)
        if btc_composite is not None:
            execution_quality = btc_composite.delta_execution_quality_score
        direction, directional_edge_bps, direction_reason = self._direction_and_edge(
            desk, price, liquidity_score, execution_state=execution_state, btc_composite=btc_composite
        )
        cost_components = self._execution_cost_components(data_manager)
        costs_bps = cost_components["total_cost_bps"]
        uncertainty_bps = self._uncertainty_bps(regime, liquidity_score, execution_quality)
        net_edge = directional_edge_bps - costs_bps
        model_values = {
            "directional_edge_bps": directional_edge_bps,
            "costs_bps": costs_bps,
            "execution_cost_components": cost_components,
            "net_edge_bps": net_edge,
            "uncertainty_bps": uncertainty_bps,
            "liquidity_score": liquidity_score,
            "execution_quality": execution_quality,
            "regime": regime.value,
            "signal_source": direction_reason,
        }
        if execution_state is not None:
            model_values.update({
                "ofi_usd_1s": execution_state.ofi_usd_1s,
                "ofi_usd_10s": execution_state.ofi_usd_10s,
                "tfi_usd_1s": execution_state.tfi_usd_1s,
                "tfi_usd_10s": execution_state.tfi_usd_10s,
                "microprice": execution_state.microprice,
                "spread_bps": execution_state.spread_bps,
            })
        if btc_composite is not None:
            model_values.update({
                "flow_agreement_score": btc_composite.flow_agreement_score,
                "cross_venue_dispersion_bps": btc_composite.cross_venue_dispersion_bps,
                "delta_dislocation_bps": btc_composite.delta_dislocation_bps,
                "delta_execution_quality": btc_composite.delta_execution_quality_score,
                "candidate_leader_venue": btc_composite.candidate_leader_venue,
            })
        features = {
            "price": price,
            "feed_health": asdict(feed_health),
            "liquidity_zones": [asdict(z) for z in zones],
            "price_window_count": len(self._price_window),
        }
        min_edge = float(_cfg("INSTITUTIONAL_MIN_NET_EDGE_BPS", 3.0))
        if direction is Direction.NO_TRADE or net_edge <= max(min_edge, uncertainty_bps):
            return self._decision(
                desk=desk,
                venue=venue,
                instrument=instrument,
                decision=DecisionOutput.NO_TRADE_INSUFFICIENT_EDGE,
                direction=direction,
                regime=regime,
                expected_net_edge_bps=net_edge,
                uncertainty_bps=uncertainty_bps,
                liquidity_score=liquidity_score,
                execution_quality_score=execution_quality,
                sizing=None,
                protection_plan=None,
                reasons=[direction_reason, "net_edge_does_not_clear_uncertainty_and_minimum"],
                model_values=model_values,
                research_features=features,
            )

        protection = self._protection_plan(desk, direction, price, liquidity_score)
        if protection is None or not protection.protection_feasible:
            return self._decision(
                desk=desk,
                venue=venue,
                instrument=instrument,
                decision=DecisionOutput.NO_TRADE_EXECUTION_UNSAFE,
                direction=direction,
                regime=regime,
                expected_net_edge_bps=net_edge,
                uncertainty_bps=uncertainty_bps,
                liquidity_score=liquidity_score,
                execution_quality_score=execution_quality,
                sizing=None,
                protection_plan=protection,
                reasons=(protection.reasons if protection else ["protection_plan_unavailable"]),
                model_values=model_values,
                research_features=features,
            )

        sizing = self._size_position(desk, instrument, direction, price, net_edge, liquidity_score, protection, risk_manager)
        if not sizing.approved:
            return self._decision(
                desk=desk,
                venue=venue,
                instrument=instrument,
                decision=DecisionOutput.NO_TRADE_RISK_BUDGET,
                direction=direction,
                regime=regime,
                expected_net_edge_bps=net_edge,
                uncertainty_bps=uncertainty_bps,
                liquidity_score=liquidity_score,
                execution_quality_score=execution_quality,
                sizing=sizing,
                protection_plan=protection,
                reasons=sizing.reasons,
                model_values=model_values,
                research_features=features,
            )

        if not bool(_cfg("INSTITUTIONAL_ENABLE_LIVE_ENTRIES", False)):
            return self._decision(
                desk=desk,
                venue=venue,
                instrument=instrument,
                decision=DecisionOutput.NO_TRADE_INSUFFICIENT_EDGE,
                direction=direction,
                regime=regime,
                expected_net_edge_bps=net_edge,
                uncertainty_bps=uncertainty_bps,
                liquidity_score=liquidity_score,
                execution_quality_score=execution_quality,
                sizing=sizing,
                protection_plan=protection,
                reasons=["shadow_mode_live_entries_disabled"],
                model_values=model_values,
                research_features=features,
            )

        return self._decision(
            desk=desk,
            venue=venue,
            instrument=instrument,
            decision=DecisionOutput.TRADE_APPROVED_WITH_PROTECTION_PLAN,
            direction=direction,
            regime=regime,
            expected_net_edge_bps=net_edge,
            uncertainty_bps=uncertainty_bps,
            liquidity_score=liquidity_score,
            execution_quality_score=execution_quality,
            sizing=sizing,
            protection_plan=protection,
            reasons=["edge_risk_execution_and_protection_validated"],
            model_values=model_values,
            research_features=features,
        )

    def _evaluate_india_options(
        self, *, data_manager, order_manager, risk_manager, instrument: str,
        underlying_price: float, regime: Regime, feed_health: FeedHealth,
    ) -> OpportunityDecision:
        """Evaluate NIFTY long-premium execution without mixing price domains.

        Direction is derived only in NIFTY-underlying units; the selected CE/PE
        vehicle is activated afterwards and all execution, cost, SL/TP and sizing
        calculations use the option premium and its own documented live book.
        """
        venue = "groww"
        context_getter = getattr(data_manager, "get_groww_option_volatility_context", None)
        volatility_context = dict(context_getter() or {}) if callable(context_getter) else {
            "ready_for_long_premium_decision": False, "reasons": ["option_volatility_context_interface_missing"]
        }
        model_values: dict[str, Any] = {
            "underlying_price": underlying_price,
            "feed_quality": feed_health.quality_score,
            "option_volatility_context": volatility_context,
            "pricing_domains": {"signal": "NIFTY_UNDERLYING", "execution": "OPTION_PREMIUM"},
        }
        session_status_getter = getattr(data_manager, "get_session_contract_book_status", None)
        if callable(session_status_getter):
            try:
                model_values["session_execution_book"] = self._compact_session_execution_book(session_status_getter() or {})
            except Exception as exc:
                model_values["session_execution_book"] = {"status": "STATUS_ERROR", "error": str(exc)}
        features: dict[str, Any] = {"underlying_price": underlying_price, "feed_health": asdict(feed_health), "option_volatility_context": volatility_context}
        if not bool(volatility_context.get("ready_for_long_premium_decision", False)):
            reasons = list(volatility_context.get("reasons") or ["options_volatility_context_not_ready"])
            return self._decision(
                desk=DeskId.INDIA_OPTIONS.value, venue=venue, instrument=instrument,
                decision=DecisionOutput.NO_TRADE_INSUFFICIENT_EDGE, direction=Direction.NO_TRADE,
                regime=regime, expected_net_edge_bps=0.0, uncertainty_bps=999.0,
                liquidity_score=0.0, execution_quality_score=feed_health.quality_score,
                sizing=None, protection_plan=None, reasons=reasons,
                model_values=model_values, research_features=features,
            )
        thesis, underlying_edge_bps, thesis_reason, thesis_features = self._india_underlying_structural_thesis(data_manager, underlying_price)
        model_values.update(thesis_features)
        if thesis is Direction.NO_TRADE:
            return self._decision(
                desk=DeskId.INDIA_OPTIONS.value, venue=venue, instrument=instrument,
                decision=DecisionOutput.NO_TRADE_INSUFFICIENT_EDGE, direction=Direction.NO_TRADE,
                regime=regime, expected_net_edge_bps=0.0, uncertainty_bps=8.0,
                liquidity_score=0.0, execution_quality_score=feed_health.quality_score,
                sizing=None, protection_plan=None, reasons=[thesis_reason],
                model_values=model_values, research_features=features,
            )
        try:
            balance = risk_manager.get_available_balance() or {}
            available_funds = _num(balance.get("available"), 0.0)
        except Exception:
            available_funds = 0.0
        activator = getattr(data_manager, "activate_groww_execution_vehicle", None)
        thesis_side = "long" if thesis is Direction.BULLISH else "short"
        choice = activator(thesis_side, available_funds) if callable(activator) else None
        if choice is None:
            return self._decision(
                desk=DeskId.INDIA_OPTIONS.value, venue=venue, instrument=instrument,
                decision=DecisionOutput.NO_TRADE_EXECUTION_UNSAFE, direction=thesis,
                regime=regime, expected_net_edge_bps=0.0, uncertainty_bps=999.0,
                liquidity_score=0.0, execution_quality_score=0.0,
                sizing=None, protection_plan=None,
                reasons=["direction_specific_groww_vehicle_not_fresh_or_not_executable"],
                model_values=model_values, research_features=features,
            )
        execution_feed_getter = getattr(data_manager, "get_execution_feed_status", None)
        execution_feed = dict(execution_feed_getter() or {}) if callable(execution_feed_getter) else {}
        if not bool(execution_feed.get("active_vehicle_ready", False)):
            return self._decision(
                desk=DeskId.INDIA_OPTIONS.value, venue=venue, instrument=instrument,
                decision=DecisionOutput.NO_TRADE_EXECUTION_UNSAFE, direction=thesis,
                regime=regime, expected_net_edge_bps=0.0, uncertainty_bps=999.0,
                liquidity_score=0.0, execution_quality_score=0.0,
                sizing=None, protection_plan=None, reasons=["active_option_ltp_depth_stream_not_fresh"],
                model_values={**model_values, "execution_feed": execution_feed}, research_features=features,
            )
        try:
            option_price = _num(data_manager.get_last_price(), 0.0)
        except Exception:
            option_price = 0.0
        if option_price <= 0:
            return self._decision(
                desk=DeskId.INDIA_OPTIONS.value, venue=venue, instrument=instrument,
                decision=DecisionOutput.NO_TRADE_EXECUTION_UNSAFE, direction=thesis,
                regime=regime, expected_net_edge_bps=0.0, uncertainty_bps=999.0,
                liquidity_score=0.0, execution_quality_score=0.0,
                sizing=None, protection_plan=None, reasons=["active_option_premium_unavailable"],
                model_values=model_values, research_features=features,
            )
        liquidity_score, zones = self._liquidity_score(data_manager, instrument, option_price)
        delta = abs(_num(getattr(choice, "delta", 0.0), 0.0))
        if delta <= 0:
            return self._decision(
                desk=DeskId.INDIA_OPTIONS.value, venue=venue, instrument=instrument,
                decision=DecisionOutput.NO_TRADE_EXECUTION_UNSAFE, direction=thesis,
                regime=regime, expected_net_edge_bps=0.0, uncertainty_bps=999.0,
                liquidity_score=liquidity_score, execution_quality_score=feed_health.quality_score,
                sizing=None, protection_plan=None, reasons=["selected_option_live_delta_unavailable"],
                model_values=model_values, research_features=features,
            )
        # First-order premium edge: Delta * expected underlying move, converted
        # into option-premium bps. Gamma is not credited before it materialises.
        premium_edge_bps = underlying_edge_bps * delta * underlying_price / max(option_price, 1e-9)
        expected_hold_sec = max(1.0, float(_cfg("POLICY_OPTION_MAX_HOLD_SEC", 2700.0)))
        theta_day_ratio = abs(_num(getattr(choice, "theta_to_premium", 0.0), 0.0))
        theta_carry_bps = theta_day_ratio * (expected_hold_sec / 86400.0) * 10000.0
        cost_components = self._execution_cost_components(data_manager)
        costs_bps = cost_components["total_cost_bps"]
        net_edge = premium_edge_bps - costs_bps - theta_carry_bps
        uncertainty_bps = self._uncertainty_bps(regime, liquidity_score, feed_health.quality_score)
        model_values.update({
            "thesis": thesis.value, "thesis_reason": thesis_reason,
            "underlying_edge_bps": underlying_edge_bps, "selected_option_symbol": getattr(choice, "selected_symbol", ""),
            "selected_option_delta": delta, "selected_option_premium": option_price,
            "premium_delta_edge_bps": premium_edge_bps, "execution_cost_components": cost_components,
            "costs_bps": costs_bps, "theta_to_premium_per_day": theta_day_ratio,
            "theta_expected_hold_sec": expected_hold_sec, "theta_carry_bps_expected_hold": theta_carry_bps,
            "net_edge_formula": "premium_delta_edge_bps - total_cost_bps - theta_carry_bps_expected_hold",
            "net_edge_bps": net_edge, "uncertainty_bps": uncertainty_bps,
            "execution_feed": execution_feed, "liquidity_score": liquidity_score,
        })
        features["liquidity_zones"] = [asdict(z) for z in zones]
        min_edge = float(_cfg("INSTITUTIONAL_MIN_NET_EDGE_BPS", 3.0))
        if net_edge <= max(min_edge, uncertainty_bps):
            return self._decision(
                desk=DeskId.INDIA_OPTIONS.value, venue=venue, instrument=instrument,
                decision=DecisionOutput.NO_TRADE_INSUFFICIENT_EDGE, direction=thesis,
                regime=regime, expected_net_edge_bps=net_edge, uncertainty_bps=uncertainty_bps,
                liquidity_score=liquidity_score, execution_quality_score=feed_health.quality_score,
                sizing=None, protection_plan=None,
                reasons=[thesis_reason, "option_premium_edge_does_not_clear_cost_and_uncertainty"],
                model_values=model_values, research_features=features,
            )
        protection = self._option_premium_protection_plan(data_manager, option_price)
        if protection is None or not protection.protection_feasible:
            return self._decision(
                desk=DeskId.INDIA_OPTIONS.value, venue=venue, instrument=instrument,
                decision=DecisionOutput.NO_TRADE_EXECUTION_UNSAFE, direction=thesis,
                regime=regime, expected_net_edge_bps=net_edge, uncertainty_bps=uncertainty_bps,
                liquidity_score=liquidity_score, execution_quality_score=feed_health.quality_score,
                sizing=None, protection_plan=protection,
                reasons=(protection.reasons if protection else ["option_premium_protection_unavailable"]),
                model_values=model_values, research_features=features,
            )
        model_values["protection_plan"] = asdict(protection)
        sizing = self._size_position(DeskId.INDIA_OPTIONS.value, instrument, thesis, option_price, net_edge, liquidity_score, protection, risk_manager)
        model_values["sizing_decision"] = asdict(sizing)
        if not sizing.approved:
            return self._decision(
                desk=DeskId.INDIA_OPTIONS.value, venue=venue, instrument=instrument,
                decision=DecisionOutput.NO_TRADE_RISK_BUDGET, direction=thesis,
                regime=regime, expected_net_edge_bps=net_edge, uncertainty_bps=uncertainty_bps,
                liquidity_score=liquidity_score, execution_quality_score=feed_health.quality_score,
                sizing=sizing, protection_plan=protection, reasons=sizing.reasons,
                model_values=model_values, research_features=features,
            )
        if not bool(_cfg("INSTITUTIONAL_ENABLE_LIVE_ENTRIES", False)):
            return self._decision(
                desk=DeskId.INDIA_OPTIONS.value, venue=venue, instrument=instrument,
                decision=DecisionOutput.NO_TRADE_INSUFFICIENT_EDGE, direction=thesis,
                regime=regime, expected_net_edge_bps=net_edge, uncertainty_bps=uncertainty_bps,
                liquidity_score=liquidity_score, execution_quality_score=feed_health.quality_score,
                sizing=sizing, protection_plan=protection, reasons=["shadow_mode_live_entries_disabled"],
                model_values=model_values, research_features=features,
            )
        return self._decision(
            desk=DeskId.INDIA_OPTIONS.value, venue=venue, instrument=instrument,
            decision=DecisionOutput.TRADE_APPROVED_WITH_PROTECTION_PLAN, direction=thesis,
            regime=regime, expected_net_edge_bps=net_edge, uncertainty_bps=uncertainty_bps,
            liquidity_score=liquidity_score, execution_quality_score=feed_health.quality_score,
            sizing=sizing, protection_plan=protection, reasons=["groww_long_premium_thesis_execution_protection_validated"],
            model_values=model_values, research_features=features,
        )

    def _india_underlying_structural_thesis(self, data_manager, spot: float) -> tuple[Direction, float, str, dict[str, Any]]:
        """Generate an underlying-domain thesis from multi-timeframe displacement.

        An index has no order book of its own; this routine therefore uses only
        official index candles and rejects entries unless 5m displacement breaks
        established liquidity range with 15m directional alignment.
        """
        try:
            bars_5m = list(data_manager.get_candles("5m", 40) or [])
            bars_15m = list(data_manager.get_candles("15m", 30) or [])
        except Exception:
            return Direction.NO_TRADE, 0.0, "nifty_underlying_candles_unavailable", {}
        def close(row):
            return _num(row.get("close") if isinstance(row, Mapping) else None, _num(row.get("c") if isinstance(row, Mapping) else None, 0.0))
        def high(row):
            return _num(row.get("high") if isinstance(row, Mapping) else None, _num(row.get("h") if isinstance(row, Mapping) else None, 0.0))
        def low(row):
            return _num(row.get("low") if isinstance(row, Mapping) else None, _num(row.get("l") if isinstance(row, Mapping) else None, 0.0))
        if spot <= 0 or len(bars_5m) < 22 or len(bars_15m) < 4:
            return Direction.NO_TRADE, 0.0, "nifty_underlying_structure_warmup", {"bars_5m": len(bars_5m), "bars_15m": len(bars_15m)}
        history = bars_5m[-21:-1]
        swing_high = max(high(x) for x in history)
        swing_low = min(low(x) for x in history)
        tr = []
        prev = close(history[0])
        for bar in history[1:]:
            h, l, c = high(bar), low(bar), close(bar)
            if min(h, l, c, prev) > 0:
                tr.append(max(h - l, abs(h - prev), abs(l - prev)))
            prev = c
        atr = sum(tr) / len(tr) if tr else 0.0
        prior_15 = close(bars_15m[-4])
        current_15 = close(bars_15m[-1]) or spot
        align_bps = math.log(current_15 / prior_15) * 10_000.0 if min(current_15, prior_15) > 0 else 0.0
        buffer = max(atr * float(_cfg("GROWW_STRUCTURAL_BREAK_BUFFER_ATR", 0.15)), spot * 0.00005)
        break_up_bps = (spot - (swing_high + buffer)) / spot * 10_000.0
        break_down_bps = ((swing_low - buffer) - spot) / spot * 10_000.0
        min_alignment = float(_cfg("GROWW_STRUCTURAL_MIN_ALIGNMENT_BPS", 2.0))
        metrics = {"swing_high": swing_high, "swing_low": swing_low, "atr_5m": atr, "alignment_15m_bps": align_bps, "break_up_bps": break_up_bps, "break_down_bps": break_down_bps}
        if break_up_bps > 0 and align_bps >= min_alignment:
            return Direction.BULLISH, break_up_bps + 0.25 * align_bps, "nifty_liquidity_break_displacement_bullish", metrics
        if break_down_bps > 0 and align_bps <= -min_alignment:
            return Direction.BEARISH, break_down_bps + 0.25 * abs(align_bps), "nifty_liquidity_break_displacement_bearish", metrics
        return Direction.NO_TRADE, 0.0, "nifty_no_valid_structural_displacement", metrics

    def _option_premium_protection_plan(self, data_manager, premium: float) -> ProtectionPlan | None:
        try:
            candles = list(data_manager.get_execution_candles("1m", 40) or [])
        except Exception:
            candles = []
        if premium <= 0 or len(candles) < int(_cfg("GROWW_OPTION_PROTECTION_MIN_ATR_BARS", 10)):
            return ProtectionPlan(premium, premium, premium, "GROWW_OCO_AFTER_FILL", False, ["option_premium_atr_warmup_unavailable"])
        ranges: list[float] = []
        previous = None
        for row in candles[-20:]:
            if not isinstance(row, Mapping):
                continue
            h = _num(row.get("high") or row.get("h"), 0.0)
            l = _num(row.get("low") or row.get("l"), 0.0)
            c = _num(row.get("close") or row.get("c"), 0.0)
            if min(h, l, c) <= 0:
                continue
            ranges.append(max(h - l, abs(h - previous), abs(l - previous)) if previous else h - l)
            previous = c
        if len(ranges) < 5:
            return ProtectionPlan(premium, premium, premium, "GROWW_OCO_AFTER_FILL", False, ["option_premium_atr_invalid"])
        atr = sum(ranges[-14:]) / min(len(ranges), 14)
        min_risk = premium * float(_cfg("GROWW_OPTION_MIN_PREMIUM_RISK_PCT", 0.14))
        max_risk = premium * float(_cfg("GROWW_OPTION_MAX_PREMIUM_RISK_PCT", 0.58))
        stop_distance = min(max(max(atr * float(_cfg("GROWW_OPTION_SLTP_DELTA_MULT", 1.0)), min_risk), 0.05), max_risk)
        if stop_distance <= 0 or stop_distance >= premium:
            return ProtectionPlan(premium, premium, premium, "GROWW_OCO_AFTER_FILL", False, ["option_premium_stop_not_executable"])
        rr = float(_cfg("GROWW_OPTION_TARGET_RR", 1.60))
        target_distance = max(stop_distance * rr, premium * float(_cfg("GROWW_OPTION_MIN_TP_PREMIUM_PCT", 0.18)))
        return ProtectionPlan(
            entry_price=premium, stop_price=max(0.05, premium - stop_distance), target_price=premium + target_distance,
            protection_type="GROWW_OCO_AFTER_FILL", protection_feasible=True,
            reasons=[f"premium_domain_atr_protection atr={atr:.4f} stop_dist={stop_distance:.4f} target_dist={target_distance:.4f} rr={rr:.3f}", "groww_long_option_oco_required"],
        )

    def _decision(self, **kwargs: Any) -> OpportunityDecision:
        return OpportunityDecision(**kwargs)

    def _persist_decision(self, decision: OpportunityDecision, now_ms: int) -> None:
        try:
            candidate_id = f"{decision.desk}:{decision.instrument}:{now_ms}"
            self._last_candidate_id = candidate_id
            self._research_store.append_decision(
                ResearchDecisionRecord(
                    observation_ts_ns=int(now_ms) * 1_000_000,
                    desk=decision.desk,
                    venue=decision.venue,
                    instrument=decision.instrument,
                    candidate_id=candidate_id,
                    decision=decision.decision.value,
                    model_values=decision.model_values,
                    reasons=decision.reasons,
                    features=decision.research_features,
                    model_version="observable-groww-carry-aware-v2.9",
                    policy_version="official-feed-protected-execution-v2.9",
                )
            )
        except Exception:
            pass

    def _execute_approved(self, decision: OpportunityDecision, order_manager, risk_manager) -> None:
        _ = risk_manager
        sizing = decision.sizing
        protection = decision.protection_plan
        if sizing is None or protection is None:
            return
        side = "BUY" if decision.direction in {Direction.LONG, Direction.BULLISH} else "SELL"
        if decision.venue == "groww":
            side = "BUY"
        result = order_manager.place_bracket_limit_entry(
            side,
            sizing.quantity,
            protection.entry_price,
            protection.stop_price,
            protection.target_price,
        )
        if not result:
            return
        fill_price = float(result.get("fill_price") or protection.entry_price)
        fill_ts_ns = time.time_ns()
        try:
            self._research_store.append_execution(ResearchExecutionRecord(
                observation_ts_ns=fill_ts_ns, desk=decision.desk, venue=decision.venue, instrument=decision.instrument,
                candidate_id=self._last_candidate_id or f"{decision.desk}:{decision.instrument}:{fill_ts_ns}",
                requested_order={"side": side, "quantity": sizing.quantity, "entry": protection.entry_price, "stop": protection.stop_price, "target": protection.target_price},
                actual_fill={"quantity": float(result.get("quantity") or sizing.quantity), "fill_price": fill_price, "order_id": str(result.get("order_id") or "")},
                protection_state={"confirmed": bool(result.get("bracket_child_verified") or result.get("protection_confirmed")), "model": str(result.get("protection_model") or protection.protection_type)},
                realised_costs={"expected_execution_cost_bps": float(decision.model_values.get("costs_bps", 0.0))},
            ))
            if decision.venue == "delta":
                all_costs = float(decision.model_values.get("costs_bps", 0.0))
                self._forward_labels.record_fill(
                    fill_ts_ns=fill_ts_ns, side=side, candidate_id=self._last_candidate_id, fill_price=fill_price,
                    spread_cost_bps=float(decision.model_values.get("spread_bps", 0.0)),
                    fee_cost_bps=float(_cfg("INSTITUTIONAL_DEFAULT_FEE_BPS", 1.5)),
                    slippage_estimate_bps=max(0.0, all_costs - float(decision.model_values.get("spread_bps", 0.0)) - float(_cfg("INSTITUTIONAL_DEFAULT_FEE_BPS", 1.5))),
                )
        except Exception:
            pass
        self._risk_gate.record_trade_start()
        record_exposure = getattr(risk_manager, "record_open_exposure", None)
        if callable(record_exposure):
            signed_delta = sizing.notional if side == "BUY" else -sizing.notional
            record_exposure(asset_id=self._asset_id, position_key=f"{decision.desk}:{decision.instrument}", signed_delta_usd=signed_delta)
        self._pos = PositionState(
            phase=PositionPhase.ACTIVE,
            side="long" if side == "BUY" else "short",
            quantity=float(result.get("quantity") or sizing.quantity),
            entry_price=fill_price,
            sl_price=protection.stop_price,
            tp_price=protection.target_price,
            entry_order_id=str(result.get("order_id") or ""),
            sl_order_id=str(result.get("bracket_sl_order_id") or ""),
            tp_order_id=str(result.get("bracket_tp_order_id") or ""),
            entry_time=time.time(),
            exchange=decision.venue,
            execution_symbol=decision.instrument,
            asset_id=self._asset_id,
            pnl_model="inverse_btcusd" if decision.venue == "delta" and decision.instrument.upper() == "BTCUSD" else "linear",
            currency_symbol="₹" if decision.venue == "groww" else "$",
            currency_code="INR" if decision.venue == "groww" else "USD",
            entry_leverage=float(sizing.leverage_selected or 1.0),
            protection_confirmed=bool(result.get("bracket_child_verified") or result.get("protection_confirmed")),
            protection_model=str(result.get("protection_model") or protection.protection_type),
            quant_components=decision.model_values,
        )

    def _monitor_position(self, data_manager, order_manager, risk_manager) -> None:
        price = self._safe_price(data_manager)
        if price <= 0:
            return
        self._forward_labels.observe(now_ts_ns=time.time_ns(), current_price=price)
        if self._pos.side.lower() == "long":
            target_hit = price >= self._pos.tp_price > 0
            stop_hit = price <= self._pos.sl_price if self._pos.sl_price > 0 else False
        else:
            target_hit = price <= self._pos.tp_price if self._pos.tp_price > 0 else False
            stop_hit = price >= self._pos.sl_price > 0
        if target_hit or stop_hit:
            self._pos.phase = PositionPhase.EXITING
            self._pos.manual_exit_reason = "target_reached" if target_hit else "stop_reached"
            return
        try:
            broker_pos = order_manager.get_open_position()
            if isinstance(broker_pos, Mapping) and _num(broker_pos.get("size"), 0.0) <= 0:
                remove_exposure = getattr(risk_manager, "remove_open_exposure", None)
                if callable(remove_exposure):
                    remove_exposure(f"{self._desk_id(self._pos.exchange, self._pos.execution_symbol)}:{self._pos.execution_symbol}")
                self._pos = PositionState(asset_id=self._asset_id)
        except Exception:
            pass

    def _safe_price(self, data_manager) -> float:
        for name in ("get_analysis_price", "get_last_price"):
            getter = getattr(data_manager, name, None)
            if callable(getter):
                try:
                    px = _num(getter(), 0.0)
                    if px > 0:
                        return px
                except Exception:
                    pass
        return 0.0

    def _feed_health(self, data_manager) -> FeedHealth:
        raw = {}
        for name in ("get_feed_reliability", "get_data_quality"):
            getter = getattr(data_manager, name, None)
            if callable(getter):
                try:
                    val = getter()
                    if isinstance(val, Mapping):
                        raw.update(val)
                except Exception:
                    pass
        connected = bool(raw.get("connected", raw.get("ok", raw.get("alive", True))))
        heartbeat_ok = bool(raw.get("heartbeat_ok", raw.get("alive", connected)))
        sequence_valid = bool(raw.get("sequence_valid", True))
        snapshot_ready = bool(raw.get("snapshot_ready", raw.get("book_ready", raw.get("ok", connected))))
        latency_z = raw.get("latency_vs_baseline_z")
        if latency_z is None and raw.get("latency_ms") is not None:
            latency_z = max(0.0, (_num(raw.get("latency_ms"), 0.0) - _num(raw.get("latency_baseline_ms"), 0.0)) / max(_num(raw.get("latency_sigma_ms"), 1.0), 1.0))
        return score_feed_health(
            connected=connected,
            heartbeat_ok=heartbeat_ok,
            sequence_valid=sequence_valid,
            snapshot_ready=snapshot_ready,
            exchange_timestamp_available=bool(raw.get("exchange_timestamp_available", True)),
            latency_vs_baseline_z=latency_z,
            no_change_heartbeat_valid=bool(raw.get("no_change_heartbeat_valid", False)),
        )

    def _liquidity_score(self, data_manager, instrument: str, price: float, *, execution_state: VenueMicrostate | None = None) -> tuple[float, list[LiquidityZoneScore]]:
        orderbook_getter = getattr(data_manager, "get_orderbook", None)
        bid_depth = ask_depth = 0.0
        spread_bps = 999.0
        try:
            state = execution_state
            if state is None:
                book = orderbook_getter() if callable(orderbook_getter) else {}
                bids = book.get("bids") if isinstance(book, Mapping) else []
                asks = book.get("asks") if isinstance(book, Mapping) else []
                if bids and asks and price > 0:
                    state = build_venue_microstate(
                        mapping=self._instrument_mapping(instrument), bids=bids, asks=asks,
                        feed_health=score_feed_health(connected=True, heartbeat_ok=True, sequence_valid=True, snapshot_ready=True, exchange_timestamp_available=True, latency_vs_baseline_z=0.0),
                        receive_ts_ns=time.time_ns(),
                    )
            if state is not None:
                bid_depth = state.bid_depth_usd_by_band.get("0-1", 0.0) + state.bid_depth_usd_by_band.get("1-3", 0.0)
                ask_depth = state.ask_depth_usd_by_band.get("0-1", 0.0) + state.ask_depth_usd_by_band.get("1-3", 0.0)
                spread_bps = state.spread_bps
        except Exception:
            pass
        depth_score = min(1.0, math.log10(max(bid_depth + ask_depth, 1.0)) / 7.0)
        spread_score = max(0.0, 1.0 - spread_bps / 50.0)
        score = max(0.0, min(1.0, 0.55 * depth_score + 0.45 * spread_score))
        zone = LiquidityZoneScore(
            instrument=instrument,
            direction_context="neutral",
            price_low=price * 0.999 if price > 0 else 0.0,
            price_high=price * 1.001 if price > 0 else 0.0,
            source_timeframes=["live_book"],
            age_seconds=0.0,
            touch_count=0,
            sweep_count=0,
            depletion_score=max(0.0, 1.0 - depth_score),
            absorption_score=0.0,
            estimated_liquidity_score=score,
            volatility_adjusted_distance=1.0,
            execution_cost_score=spread_score,
            cross_market_alignment_score=0.0,
            stop_vulnerability_score=1.0 - spread_score,
            entry_utility_score=score,
            tp_utility_score=score * 0.75,
            sl_safety_score=spread_score,
        )
        return score, [zone]

    def _microstructure_context(
        self, data_manager, venue: str, instrument: str, feed_health: FeedHealth
    ) -> tuple[VenueMicrostate | None, BTCCompositeState | None]:
        states: dict[str, VenueMicrostate] = {}
        getter = getattr(data_manager, "get_venue_microstates", None)
        if callable(getter):
            try:
                states = {str(k).lower(): v for k, v in dict(getter() or {}).items() if isinstance(v, VenueMicrostate)}
            except Exception:
                states = {}
        if not states:
            getter = getattr(data_manager, "get_venue_microstate", None)
            if callable(getter):
                try:
                    state = getter()
                    if isinstance(state, VenueMicrostate):
                        states[state.venue.lower()] = state
                except Exception:
                    pass
        execution_state = states.get(str(venue).lower())
        if execution_state is None:
            try:
                book = data_manager.get_orderbook()
                bids, asks = book.get("bids", []), book.get("asks", [])
                if bids and asks:
                    flows_getter = getattr(data_manager, "get_microstructure_flow", None)
                    flows = dict(flows_getter() or {}) if callable(flows_getter) else {}
                    execution_state = build_venue_microstate(
                        mapping=self._instrument_mapping(instrument), bids=bids, asks=asks, feed_health=feed_health,
                        receive_ts_ns=time.time_ns(), **{k: float(flows.get(k, 0.0)) for k in ("ofi_usd_1s", "ofi_usd_10s", "ofi_usd_60s", "tfi_usd_1s", "tfi_usd_10s", "tfi_usd_60s")},
                    )
            except Exception:
                execution_state = None
        composite = None
        if self._asset_id.upper() == "BTC" and "delta" in states:
            refs = {k: v for k, v in states.items() if k != "delta"}
            if refs:
                composite = build_btc_composite_state(delta_state=states["delta"], reference_states=refs)
        return execution_state, composite

    def _direction_and_edge(
        self, desk: str, price: float, liquidity_score: float, *, execution_state: VenueMicrostate | None, btc_composite: BTCCompositeState | None
    ) -> tuple[Direction, float, str]:
        if price <= 0 or execution_state is None:
            return Direction.NO_TRADE, 0.0, "microstructure_state_unavailable"
        if not execution_state.usable_for_decision:
            return Direction.NO_TRADE, 0.0, "execution_microstate_unhealthy"
        execution_quality = btc_composite.delta_execution_quality_score if btc_composite else execution_state.feed_quality_score
        if execution_quality < float(_cfg("INSTITUTIONAL_MIN_EXECUTION_QUALITY", 0.40)):
            return Direction.NO_TRADE, 0.0, f"execution_quality_low:{execution_quality:.3f}"
        if desk == DeskId.BTC.value and bool(_cfg("INSTITUTIONAL_REQUIRE_BTC_CROSS_VENUE", True)):
            if btc_composite is None or not btc_composite.reference_states:
                return Direction.NO_TRADE, 0.0, "btc_reference_microstate_unavailable"
            if btc_composite.flow_agreement_score < float(_cfg("INSTITUTIONAL_MIN_FLOW_AGREEMENT", 0.55)):
                return Direction.NO_TRADE, 0.0, f"venue_flow_disagreement:{btc_composite.flow_agreement_score:.3f}"
            if btc_composite.cross_venue_dispersion_bps > float(_cfg("INSTITUTIONAL_MAX_CROSS_VENUE_DISPERSION_BPS", 15.0)):
                return Direction.NO_TRADE, 0.0, f"cross_venue_dispersion_high:{btc_composite.cross_venue_dispersion_bps:.3f}"
        near_depth = sum(float(execution_state.bid_depth_usd_by_band.get(k, 0.0) + execution_state.ask_depth_usd_by_band.get(k, 0.0)) for k in ("0-1", "1-3"))
        if near_depth <= 0:
            return Direction.NO_TRADE, 0.0, "near_touch_depth_unavailable"
        ofi_norm = (execution_state.ofi_usd_1s + 0.50 * execution_state.ofi_usd_10s) / near_depth
        tfi_norm = (execution_state.tfi_usd_1s + 0.50 * execution_state.tfi_usd_10s) / near_depth
        micro_deviation_bps = (execution_state.microprice / max(execution_state.mid, 1e-9) - 1.0) * 10_000.0
        dislocation_bps = float(btc_composite.delta_dislocation_bps or 0.0) if btc_composite else 0.0
        signal_bps = (
            float(_cfg("INSTITUTIONAL_FLOW_OFI_WEIGHT", 1.0)) * ofi_norm * 100.0
            + float(_cfg("INSTITUTIONAL_FLOW_TFI_WEIGHT", 0.30)) * tfi_norm * 100.0
            + float(_cfg("INSTITUTIONAL_FLOW_MICROPRICE_WEIGHT", 0.35)) * micro_deviation_bps
            - float(_cfg("INSTITUTIONAL_FLOW_DISLOCATION_WEIGHT", 0.50)) * dislocation_bps
        )
        edge = abs(signal_bps) * max(0.0, min(1.0, execution_quality)) * max(0.0, min(1.0, liquidity_score))
        threshold = float(_cfg("INSTITUTIONAL_MIN_SIGNAL_BPS", 0.50))
        if signal_bps > threshold:
            return Direction.LONG, edge, "ofi_tfi_microprice_long"
        if signal_bps < -threshold:
            return Direction.SHORT, edge, "ofi_tfi_microprice_short"
        return Direction.NO_TRADE, 0.0, "flow_signal_flat"

    def _regime(self) -> Regime:
        if len(self._price_window) < 20:
            return Regime.UNKNOWN
        returns = []
        prev = None
        for px in self._price_window:
            if prev and prev > 0:
                returns.append(math.log(px / prev))
            prev = px
        if not returns:
            return Regime.UNKNOWN
        vol_bps = math.sqrt(sum(r * r for r in returns[-60:]) / max(1, min(len(returns), 60))) * 10_000.0
        trend_bps = abs(sum(returns[-12:]) * 10_000.0)
        if vol_bps > 80:
            return Regime.SHOCK
        if trend_bps > max(8.0, vol_bps * 0.75):
            return Regime.TREND
        if vol_bps > 25:
            return Regime.EXPANSION
        return Regime.BALANCE

    def _execution_cost_components(self, data_manager) -> dict[str, float]:
        spread = 2.0
        try:
            book = data_manager.get_orderbook()
            bids = book.get("bids") if isinstance(book, Mapping) else []
            asks = book.get("asks") if isinstance(book, Mapping) else []
            if bids and asks:
                bid = _num(bids[0][0] if not isinstance(bids[0], Mapping) else bids[0].get("price") or bids[0].get("limit_price"), 0.0)
                ask = _num(asks[0][0] if not isinstance(asks[0], Mapping) else asks[0].get("price") or asks[0].get("limit_price"), 0.0)
                mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else 0.0
                spread = (ask - bid) / mid * 10_000.0 if mid > 0 else spread
        except Exception:
            pass
        fee = float(_cfg("INSTITUTIONAL_DEFAULT_FEE_BPS", 1.5))
        slippage = float(_cfg("INSTITUTIONAL_STRESS_SLIPPAGE_BPS", 1.0))
        return {"spread_bps": max(0.0, spread), "fee_bps": max(0.0, fee), "slippage_bps": max(0.0, slippage), "total_cost_bps": max(0.0, spread + fee + slippage)}

    def _execution_cost_bps(self, data_manager) -> float:
        return self._execution_cost_components(data_manager)["total_cost_bps"]

    def _uncertainty_bps(self, regime: Regime, liquidity_score: float, execution_quality: float) -> float:
        base = {
            Regime.BALANCE: 2.0,
            Regime.EXPANSION: 4.0,
            Regime.TREND: 3.0,
            Regime.SHOCK: 12.0,
            Regime.ILLIQUID: 20.0,
            Regime.UNKNOWN: 8.0,
        }[regime]
        return base + (1.0 - liquidity_score) * 6.0 + (1.0 - execution_quality) * 8.0

    def _protection_plan(self, desk: str, direction: Direction, price: float, liquidity_score: float) -> ProtectionPlan | None:
        if price <= 0 or liquidity_score <= 0:
            return None
        vol = self._realized_vol_price()
        stop_dist = max(price * 0.0015, vol * 1.25)
        target_dist = max(stop_dist * 1.6, price * 0.0025)
        if direction in {Direction.LONG, Direction.BULLISH}:
            stop = price - stop_dist
            target = price + target_dist
        elif direction in {Direction.SHORT, Direction.BEARISH}:
            stop = price + stop_dist
            target = price - target_dist
        else:
            return None
        protection_type = "GROWW_OCO_AFTER_FILL" if desk == DeskId.INDIA_OPTIONS.value else "VENUE_NATIVE_BRACKET"
        return ProtectionPlan(
            entry_price=price,
            stop_price=max(0.01, stop),
            target_price=max(0.01, target),
            protection_type=protection_type,
            protection_feasible=True,
            reasons=[],
        )

    def _size_position(
        self, desk: str, instrument: str, direction: Direction, price: float, net_edge: float, liquidity_score: float, protection: ProtectionPlan, risk_manager,
    ) -> PositionSizingDecision:
        _ = direction
        try:
            bal = risk_manager.get_available_balance() or {}
            cash = _num(bal.get("available"), 0.0)
        except Exception:
            cash = _num(_cfg("INITIAL_BALANCE", 0.0), 0.0)
        if cash <= 0:
            return PositionSizingDecision(desk, instrument, False, 0.0, 0.0, 0.0, None, 0.0, net_edge, 0.0, 0.0, 0.0, ["cash_unavailable"])
        mapping = self._instrument_mapping(instrument)
        stop_distance_pct = abs(price - protection.stop_price) / max(price, 1e-9)
        if stop_distance_pct <= 0:
            return PositionSizingDecision(desk, instrument, False, 0.0, 0.0, 0.0, None, 0.0, net_edge, 0.0, 0.0, 0.0, ["invalid_stop_distance"])
        inverse = "inverse" in mapping.notional_model.lower()
        unit_notional = mapping.contract_multiplier if inverse else price * mapping.contract_multiplier
        risk_per_unit = unit_notional * stop_distance_pct
        risk_budget = cash * float(_cfg("INSTITUTIONAL_RISK_FRACTION_PER_TRADE", 0.0025))
        liquidity_cap = max(0.0, cash * min(1.0, liquidity_score) * 0.20)
        edge_pct = max(0.0, net_edge) / 10_000.0
        kelly_notional = cash * (edge_pct / max(stop_distance_pct, 1e-9)) * float(_cfg("INSTITUTIONAL_QUARTER_KELLY", 0.25))
        observation_vol_bps = self._realized_vol_log() * 10_000.0
        vol_scalar = min(1.0, float(_cfg("INSTITUTIONAL_TARGET_OBSERVATION_VOL_BPS", 10.0)) / max(observation_vol_bps, 1e-6))
        risk_limited_notional = risk_budget / max(stop_distance_pct, 1e-9)
        target_notional = min(liquidity_cap, risk_limited_notional, kelly_notional * vol_scalar)
        if desk == DeskId.INDIA_OPTIONS.value:
            # Never size NFO options from a configured/default lot.  The lot
            # must be the exact value joined from Groww's official instrument
            # master for the direction-specific session vehicle.
            raw = getattr(getattr(self._instrument, "primary", None), "raw", {}) if self._instrument is not None else {}
            selected = raw.get("selected_option_contract") if isinstance(raw, dict) else None
            selected_raw = selected.get("raw") if isinstance(selected, dict) and isinstance(selected.get("raw"), dict) else {}
            lot = int(round(_num(selected_raw.get("runtime_lot_size"), 0.0)))
            if lot <= 0:
                return PositionSizingDecision(desk, instrument, False, 0.0, 0.0, 0.0, None, 0.0, net_edge, liquidity_cap, 0.0, 0.0, ["verified_nfo_lot_size_unavailable"])
            qty = float(max(0, math.floor(target_notional / max(unit_notional * lot, 1e-9)) * lot))
            leverage = None
        else:
            step = max(float(mapping.qty_step or 0.0), 1e-12)
            raw_qty = target_notional / max(unit_notional, 1e-9)
            qty = math.floor(raw_qty / step) * step
            leverage = min(float(_cfg("LEVERAGE", 1.0)), 5.0)
        notional = qty * unit_notional
        margin = notional if leverage is None else notional / max(leverage, 1.0)
        risk_after = qty * risk_per_unit
        approved = qty > 0 and margin <= cash and net_edge > 0 and risk_after <= risk_budget + 1e-9
        reasons = ["quarter_kelly_vol_scaled_risk_approved"] if approved else ["kelly_vol_liquidity_or_margin_rejected"]
        exposure_check = getattr(risk_manager, "can_add_exposure", None)
        if approved and callable(exposure_check):
            signed_delta = notional if direction in {Direction.LONG, Direction.BULLISH} else -notional
            exposure_ok, exposure_reason, _exposure_meta = exposure_check(
                asset_id=self._asset_id, position_key=f"{desk}:{instrument}", signed_delta_usd=signed_delta, available_cash=cash
            )
            if not exposure_ok:
                approved = False
                reasons = [str(exposure_reason)]
            else:
                reasons.append(str(exposure_reason))
        return PositionSizingDecision(
            desk=desk, instrument=instrument, approved=approved, quantity=qty, notional=notional, margin_required=margin, leverage_selected=leverage,
            risk_to_invalidation=risk_after, expected_net_edge=net_edge, liquidity_capacity_cap=liquidity_cap, portfolio_risk_before=0.0, portfolio_risk_after=risk_after, reasons=reasons,
        )

    def _realized_vol_log(self, window: int = 60) -> float:
        prices = list(self._price_window)[-(window + 1):]
        if len(prices) < 3:
            return 0.002
        returns = [math.log(prices[i] / prices[i - 1]) for i in range(1, len(prices)) if prices[i] > 0 and prices[i - 1] > 0]
        if not returns:
            return 0.002
        return math.sqrt(sum(r * r for r in returns) / len(returns))

    def _realized_vol_price(self) -> float:
        price = self._price_window[-1] if self._price_window else 0.0
        return max(price * self._realized_vol_log(), price * 0.00025)

    def _instrument_mapping(self, instrument: str) -> InstrumentMapping:
        venue = "delta"
        try:
            if self._instrument is not None:
                venue = self._instrument.primary_exchange.value
        except Exception:
            pass
        raw = getattr(getattr(self._instrument, "primary", None), "raw", {}) if self._instrument is not None else {}
        contract_multiplier = _num(raw.get("contract_multiplier") or raw.get("contract_value") or raw.get("contract_value_btc"), 1.0)
        notional_model = "inverse_usd_contract" if venue == "delta" and instrument.upper() == "BTCUSD" else "linear"
        return InstrumentMapping(
            venue=venue,
            venue_symbol=instrument,
            canonical_underlying=self._asset_id,
            product_class=str(raw.get("contract_type") or raw.get("product_type") or ""),
            quote_currency=str(raw.get("quote_asset") or "USD").upper(),
            contract_multiplier=max(contract_multiplier, 1e-12),
            settlement_currency=str(raw.get("settlement_currency") or raw.get("settling_asset") or "USD").upper(),
            price_tick=_num(raw.get("tick_size"), 0.01),
            qty_step=_num(raw.get("qty_step") or raw.get("lot_step"), 1.0),
            execution_enabled=True,
            notional_model=notional_model,
        )

    def _desk_id(self, venue: str, instrument: str) -> str:
        if venue == "groww":
            return DeskId.INDIA_OPTIONS.value
        asset = self._asset_id.upper()
        if asset in {"GOLD", "SILVER"} or any(x in instrument.upper() for x in ("PAXG", "XAUT", "SLV", "XAG")):
            return DeskId.METALS.value
        return DeskId.BTC.value

    def _venue(self, order_manager) -> str:
        for attr in ("active_exchange", "_exchange_name"):
            val = getattr(order_manager, attr, None)
            if val:
                return str(val).lower()
        try:
            if self._instrument is not None:
                return self._instrument.primary_exchange.value
        except Exception:
            pass
        return str(_cfg("EXECUTION_EXCHANGE", "delta")).lower()

    def _symbol(self, data_manager, order_manager) -> str:
        for obj in (order_manager, data_manager):
            for attr in ("display_symbol", "symbol"):
                val = getattr(obj, attr, None)
                if val:
                    return str(val)
        if self._instrument is not None:
            return self._instrument.display_symbol
        return str(_cfg("SYMBOL", "BTCUSD"))

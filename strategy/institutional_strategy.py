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

import math
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Callable, Mapping, Optional

from core.instruments import ExchangeName, TradableInstrument
from market_data.feed_health import FeedHealth, score_feed_health
from market_data.normalizer import (
    InstrumentMapping,
    VenueMicrostate,
    build_venue_microstate,
)
from research.store import JsonlResearchStore, ResearchDecisionRecord
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
            self._monitor_position(data_manager, order_manager)
            return
        decision = self.evaluate(data_manager, order_manager, risk_manager, now_ms)
        self._last_decision = decision
        self._last_decision_ts = time.time()
        self._persist_decision(decision, now_ms)
        if decision.approved:
            self._execute_approved(decision, order_manager, risk_manager)

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

        liquidity_score, zones = self._liquidity_score(data_manager, instrument, price)
        direction, directional_edge_bps, direction_reason = self._direction_and_edge(desk, price, liquidity_score)
        costs_bps = self._execution_cost_bps(data_manager)
        uncertainty_bps = self._uncertainty_bps(regime, liquidity_score, execution_quality)
        net_edge = directional_edge_bps - costs_bps
        model_values = {
            "directional_edge_bps": directional_edge_bps,
            "costs_bps": costs_bps,
            "net_edge_bps": net_edge,
            "uncertainty_bps": uncertainty_bps,
            "liquidity_score": liquidity_score,
            "execution_quality": execution_quality,
            "regime": regime.value,
        }
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

    def _decision(self, **kwargs: Any) -> OpportunityDecision:
        return OpportunityDecision(**kwargs)

    def _persist_decision(self, decision: OpportunityDecision, now_ms: int) -> None:
        try:
            self._research_store.append_decision(
                ResearchDecisionRecord(
                    observation_ts_ns=int(now_ms) * 1_000_000,
                    desk=decision.desk,
                    venue=decision.venue,
                    instrument=decision.instrument,
                    candidate_id=f"{decision.desk}:{decision.instrument}:{now_ms}",
                    decision=decision.decision.value,
                    model_values=decision.model_values,
                    reasons=decision.reasons,
                    features=decision.research_features,
                    model_version="deterministic-institutional-baseline-v1",
                    policy_version="multi-desk-protected-flow-v1",
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
        self._risk_gate.record_trade_start()
        self._pos = PositionState(
            phase=PositionPhase.ACTIVE,
            side="long" if side == "BUY" else "short",
            quantity=float(result.get("quantity") or sizing.quantity),
            entry_price=float(result.get("fill_price") or protection.entry_price),
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

    def _monitor_position(self, data_manager, order_manager) -> None:
        price = self._safe_price(data_manager)
        if price <= 0:
            return
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

    def _liquidity_score(self, data_manager, instrument: str, price: float) -> tuple[float, list[LiquidityZoneScore]]:
        orderbook_getter = getattr(data_manager, "get_orderbook", None)
        bid_depth = ask_depth = 0.0
        spread_bps = 999.0
        try:
            book = orderbook_getter() if callable(orderbook_getter) else {}
            bids = book.get("bids") if isinstance(book, Mapping) else []
            asks = book.get("asks") if isinstance(book, Mapping) else []
            if bids and asks and price > 0:
                mapping = self._instrument_mapping(instrument)
                state = build_venue_microstate(
                    mapping=mapping,
                    bids=bids,
                    asks=asks,
                    feed_health=score_feed_health(
                        connected=True,
                        heartbeat_ok=True,
                        sequence_valid=True,
                        snapshot_ready=True,
                        exchange_timestamp_available=True,
                        latency_vs_baseline_z=0.0,
                    ),
                    receive_ts_ns=int(time.time() * 1_000_000_000),
                )
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

    def _direction_and_edge(self, desk: str, price: float, liquidity_score: float) -> tuple[Direction, float, str]:
        if price <= 0 or len(self._price_window) < 30:
            return Direction.NO_TRADE, 0.0, "insufficient_market_history"
        returns = []
        last = None
        for px in self._price_window:
            if last and last > 0 and px > 0:
                returns.append(math.log(px / last))
            last = px
        if not returns:
            return Direction.NO_TRADE, 0.0, "insufficient_return_history"
        short = sum(returns[-6:]) if len(returns) >= 6 else sum(returns)
        vol = math.sqrt(sum(r * r for r in returns[-60:]) / max(1, min(len(returns), 60))) * 10_000.0
        signal_bps = short * 10_000.0
        edge = abs(signal_bps) * max(0.0, liquidity_score) - 0.15 * vol
        if desk == DeskId.INDIA_OPTIONS.value:
            if signal_bps > 0:
                return Direction.BULLISH, edge, "underlying_positive_return_state"
            if signal_bps < 0:
                return Direction.BEARISH, edge, "underlying_negative_return_state"
        else:
            if signal_bps > 0:
                return Direction.LONG, edge, "positive_return_state"
            if signal_bps < 0:
                return Direction.SHORT, edge, "negative_return_state"
        return Direction.NO_TRADE, 0.0, "flat_return_state"

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

    def _execution_cost_bps(self, data_manager) -> float:
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
        return max(0.0, spread + fee + slippage)

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
        self,
        desk: str,
        instrument: str,
        direction: Direction,
        price: float,
        net_edge: float,
        liquidity_score: float,
        protection: ProtectionPlan,
        risk_manager,
    ) -> PositionSizingDecision:
        _ = direction
        cash = 0.0
        try:
            bal = risk_manager.get_available_balance()
            cash = _num((bal or {}).get("available"), 0.0)
        except Exception:
            cash = _num(_cfg("INITIAL_BALANCE", 0.0), 0.0)
        if cash <= 0:
            return PositionSizingDecision(desk, instrument, False, 0.0, 0.0, 0.0, None, 0.0, net_edge, 0.0, 0.0, 0.0, ["cash_unavailable"])
        risk_budget = cash * float(_cfg("INSTITUTIONAL_RISK_FRACTION_PER_TRADE", 0.0025))
        risk_per_unit = abs(price - protection.stop_price)
        if risk_per_unit <= 0:
            return PositionSizingDecision(desk, instrument, False, 0.0, 0.0, 0.0, None, 0.0, net_edge, 0.0, 0.0, 0.0, ["invalid_stop_distance"])
        liquidity_cap = max(0.0, cash * min(1.0, liquidity_score) * 0.20)
        if desk == DeskId.INDIA_OPTIONS.value:
            lot = int(_cfg("GROWW_OPTION_DEFAULT_LOT_SIZE", 0) or 0)
            lot = lot if lot > 0 else 1
            lots = math.floor(min(risk_budget, liquidity_cap) / max(price * lot, 1e-9))
            qty = float(max(0, lots * lot))
            notional = qty * price
            leverage = None
            margin = notional
        else:
            raw_qty = min(risk_budget / risk_per_unit, liquidity_cap / max(price, 1e-9))
            qty = max(0.0, raw_qty)
            notional = qty * price
            leverage = min(float(_cfg("LEVERAGE", 1.0)), 5.0)
            margin = notional / max(leverage, 1.0)
        approved = qty > 0 and margin <= cash and net_edge > 0
        reasons = ["approved"] if approved else ["quantity_or_margin_rejected"]
        return PositionSizingDecision(
            desk=desk,
            instrument=instrument,
            approved=approved,
            quantity=qty,
            notional=notional,
            margin_required=margin,
            leverage_selected=leverage,
            risk_to_invalidation=qty * risk_per_unit,
            expected_net_edge=net_edge,
            liquidity_capacity_cap=liquidity_cap,
            portfolio_risk_before=0.0,
            portfolio_risk_after=qty * risk_per_unit,
            reasons=reasons,
        )

    def _realized_vol_price(self) -> float:
        if len(self._price_window) < 10:
            px = self._price_window[-1] if self._price_window else 0.0
            return px * 0.002
        diffs = [self._price_window[i] - self._price_window[i - 1] for i in range(1, len(self._price_window))]
        n = min(60, len(diffs))
        tail = diffs[-n:]
        return math.sqrt(sum(x * x for x in tail) / max(1, n))

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
            contract_multiplier=max(contract_multiplier, 1.0),
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

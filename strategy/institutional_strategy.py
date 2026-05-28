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
import threading
from collections import deque
from dataclasses import asdict, dataclass, field, replace
from enum import Enum
from typing import Any, Callable, Mapping, Optional

from core.instruments import ExchangeName, TradableInstrument
from core.market_policy import active_policy
from core.pnl import gross_pnl_usd
from execution.venue_selection import select_execution_venue
from execution.collateral_service import BrokerCollateralSnapshotService
from intelligence.cross_venue_btc import BTCCompositeState, build_btc_composite_state
from intelligence.composite_asset_state import CompositeAssetDecision, CompositeIntelligenceBus
from intelligence.venue_market_state import (
    CrossVenueEvidence, VenueMarketState, VenueMarketStateEngine,
    build_continuous_cross_venue_evidence,
)
from market_data.feed_health import FeedHealth, score_feed_health
from market_data.normalizer import (
    InstrumentMapping,
    VenueMicrostate,
    build_venue_microstate,
)
from research.store import ForwardLabelWriter, JsonlResearchStore, ResearchDecisionRecord, ResearchExecutionRecord
from strategy.dynamic_protection import DynamicProtectionPlanBuilder
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


def _hyperliquid_price_increment(price: float, qty_step: float) -> float:
    """Compute the current valid Hyperliquid perp price increment without API I/O."""
    value = max(0.0, float(price or 0.0))
    if value <= 0:
        return 0.0
    step = max(float(qty_step or 0.00001), 1e-12)
    size_decimals = max(0, min(6, int(round(-math.log10(step))) if step < 1 else 0))
    decimal_cap = max(0, 6 - size_decimals)
    integer_digits = math.floor(math.log10(value)) + 1
    decimals = min(decimal_cap, max(0, 5 - integer_digits))
    return float(10 ** (-decimals))


def _live_routing_permission(venue: str) -> tuple[bool, str]:
    """Return live-order permission from the code-owned control plane.

    Analysis and shadow validation continue on every configured data venue.
    An order can route only when the master switch is enabled and this exact
    execution venue is explicitly allow-listed in config.py.
    """
    master = bool(_cfg("LIVE_TRADING_ENABLED", _cfg("INSTITUTIONAL_ENABLE_LIVE_ENTRIES", False)))
    if not master:
        return False, "shadow_mode_live_entries_disabled"
    raw = _cfg("LIVE_EXECUTION_VENUES", ("delta", "coinswitch", "groww"))
    allowed = {str(v).strip().lower() for v in (raw if isinstance(raw, (tuple, list, set)) else str(raw).split(",")) if str(v).strip()}
    venue_key = str(venue or "").strip().lower()
    if venue_key not in allowed:
        return False, f"live_execution_venue_not_authorised:{venue_key or 'unknown'}"
    return True, "live_execution_authorised"


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
    # State-dependent exits are executed by the off-hot-path lifecycle worker.
    # Bracket protection remains broker-live until the close fill is reconciled.
    dynamic_exit_requested: bool = False
    dynamic_exit_reasons: tuple[str, ...] = field(default_factory=tuple)
    dynamic_exit_requested_at: float = 0.0
    dynamic_exit_order_id: str = ""
    dynamic_exit_attempts: int = 0
    dynamic_exit_last_error: str = ""

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
            "dynamic_exit_requested": self.dynamic_exit_requested,
            "dynamic_exit_order_id": self.dynamic_exit_order_id,
            "dynamic_exit_reasons": list(self.dynamic_exit_reasons),
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

    def __init__(self, order_manager=None, *, instrument: TradableInstrument | None = None, intelligence_bus: CompositeIntelligenceBus | None = None, collateral_service: BrokerCollateralSnapshotService | None = None) -> None:
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
        # Venue events wake only this context worker; evaluation is never run on
        # a WebSocket callback thread. This preserves market-data ingestion speed.
        self._market_event = threading.Event()
        self._latest_event_price = 0.0
        self._latest_event_ts = 0.0
        store_root = str(_cfg("RESEARCH_STORE_PATH", "research_output"))
        self._research_store = JsonlResearchStore(store_root)
        self._forward_labels = ForwardLabelWriter(self._research_store)
        self._last_candidate_id = ""
        self._protection_engine = DynamicProtectionPlanBuilder(asset_id=self._asset_id)
        self._market_state_engine = VenueMarketStateEngine(asset_id=self._asset_id)
        # Shared normalised factor bus: executable alpha transfers only inside
        # validated equivalence groups; related products remain context-only.
        self._composite_bus = intelligence_bus or CompositeIntelligenceBus()
        # Portfolio-owned broker-state service deduplicates REST balance authority reads across desks.
        self._collateral_service = collateral_service
        # Operator telemetry is state/change driven. The model can evaluate on
        # every market event without emitting an INFO-scale JSON payload per tick.
        self._telemetry_last_signature: tuple[Any, ...] | None = None
        self._telemetry_last_emit_ts: float = 0.0
        # Per-venue collateral snapshots prevent the route model selecting a
        # cheaper-looking exchange that cannot actually fund the approved trade.
        self._venue_cash_cache: dict[str, tuple[float, float, str]] = {}
        self._venue_cash_lock = threading.RLock()
        self._venue_cash_refresh_stop = threading.Event()
        self._venue_cash_refresh_thread: threading.Thread | None = None
        self._runtime_order_manager = None
        self._submission_lock = None
        self._portfolio_submission_gate = None
        # Protected entry confirmation may wait for fill/bracket acknowledgements.
        # It must never block fresh market evaluation on the strategy thread.
        self._entry_lock = threading.RLock()
        self._entry_thread: threading.Thread | None = None
        self._runtime_stop_requested = threading.Event()
        # Broker position/order endpoints are reconciliation I/O, never market-evaluation I/O.
        # Native venue protection remains armed at the broker while this supervisor
        # confirms fills/exits independently of quote-driven signal latency.
        self._position_reconcile_lock = threading.RLock()
        self._position_reconcile_wakeup = threading.Event()
        self._position_reconcile_thread: threading.Thread | None = None
        self._position_reconcile_manager = None
        self._position_reconcile_risk_manager = None

    def bind_market_wakeup(self, callback: Callable[[], Any]) -> None:
        self._market_wakeup = callback

    def bind_portfolio_submission_guard(self, lock, gate: Callable[[], tuple[bool, str]]) -> None:
        self._submission_lock = lock
        self._portfolio_submission_gate = gate

    def start_runtime_services(self, order_manager) -> None:
        """Register broker balance authority with the shared asynchronous cache.

        Live market decisions consume immutable collateral snapshots only.  A
        portfolio-owned service deduplicates REST reads across every strategy
        context so CoinSwitch/Hyperliquid account endpoints are not flooded.
        """
        self._runtime_order_manager = order_manager
        self._runtime_stop_requested.clear()
        if self._collateral_service is not None:
            self._collateral_service.register_router(order_manager)
            self._collateral_service.start()
            return
        # Compatibility path for isolated tests/single-strategy callers.
        if self._venue_cash_refresh_thread is not None and self._venue_cash_refresh_thread.is_alive():
            return
        self._venue_cash_refresh_stop.clear()
        self._venue_cash_refresh_thread = threading.Thread(
            target=self._venue_cash_refresh_loop, name=f"venue-collateral-{self._asset_id}", daemon=True
        )
        self._venue_cash_refresh_thread.start()

    def stop_runtime_services(self) -> None:
        self._runtime_stop_requested.set()
        self._position_reconcile_wakeup.set()
        # Shared portfolio service is owned and stopped by the orchestrator.
        if self._collateral_service is None:
            self._venue_cash_refresh_stop.set()

    def _ensure_position_reconciliation_worker(self, order_manager, risk_manager) -> None:
        """Keep broker reconciliation off the quote-driven decision worker.

        Once venue-native protection has been verified, market events only update
        mark/risk diagnostics. Position and closing-order REST/RPC confirmation is
        a distinct lifecycle service: otherwise each BTC/metal quote blocks on a
        broker network round trip and destroys event latency.
        """
        if self._pos.is_flat() or self._pos.phase is PositionPhase.ENTERING:
            return
        manager = self._execution_manager_for(order_manager, self._pos.exchange)
        with self._position_reconcile_lock:
            self._position_reconcile_manager = manager
            self._position_reconcile_risk_manager = risk_manager
            if self._position_reconcile_thread is not None and self._position_reconcile_thread.is_alive():
                return
            self._position_reconcile_wakeup.clear()
            self._position_reconcile_thread = threading.Thread(
                target=self._position_reconciliation_loop,
                daemon=True,
                name=f"position-reconcile-{self._asset_id}-{self._pos.exchange}",
            )
            self._position_reconcile_thread.start()
        logger.info(
            "🧵 POSITION_RECONCILIATION_SUPERVISOR launched asset=%s venue=%s; broker I/O removed from on_tick hot path",
            self._asset_id, self._pos.exchange,
        )

    def _service_dynamic_exit_request(self, manager) -> None:
        """Submit a model-directed close only from the lifecycle supervisor.

        Hard SL/TP protection is intentionally left live while the new
        reduce-only close is outstanding.  The order identifier is captured so
        broker-flat reconciliation can attribute the closing fill exactly, even
        if the dynamic close races with an already-armed protective order.
        """
        with self._entry_lock:
            if self._pos.is_flat() or not self._pos.dynamic_exit_requested or self._pos.dynamic_exit_order_id:
                return
            if self._pos.phase not in {PositionPhase.ACTIVE, PositionPhase.EXITING}:
                return
            max_attempts = max(1, int(_cfg("DYNAMIC_EXIT_MAX_SUBMISSION_ATTEMPTS", 5)))
            if self._pos.dynamic_exit_attempts >= max_attempts:
                components = self._pos.quant_components if isinstance(self._pos.quant_components, dict) else {}
                if not components.get("dynamic_exit_submission_exhausted_logged"):
                    components["dynamic_exit_submission_exhausted_logged"] = True
                    logger.critical(
                        "DYNAMIC_EXIT_SUBMISSION_EXHAUSTED asset=%s venue=%s attempts=%s; native hard protection remains active",
                        self._pos.asset_id, self._pos.exchange, self._pos.dynamic_exit_attempts,
                    )
                return
            reason = "dynamic_exit:" + ",".join(self._pos.dynamic_exit_reasons)
            expected_side = self._pos.side
            expected_qty = self._pos.quantity
            self._pos.dynamic_exit_attempts += 1
            attempt = self._pos.dynamic_exit_attempts
        submitter = getattr(manager, "place_reconciled_reduce_only_exit", None)
        if not callable(submitter):
            with self._entry_lock:
                self._pos.dynamic_exit_last_error = "reduce_only_exit_interface_unavailable"
            logger.critical(
                "DYNAMIC_EXIT_NOT_EXECUTABLE asset=%s venue=%s reason=reduce_only_exit_interface_unavailable; native hard protection remains active",
                self._asset_id, self._pos.exchange,
            )
            return
        try:
            result = submitter(reason=reason, expected_side=expected_side, expected_quantity=expected_qty)
        except Exception as exc:
            result = None
            with self._entry_lock:
                self._pos.dynamic_exit_last_error = f"submission_exception:{exc}"
            logger.exception("Dynamic reduce-only close submission failed asset=%s attempt=%s", self._asset_id, attempt)
        if not isinstance(result, Mapping):
            with self._entry_lock:
                self._pos.dynamic_exit_last_error = self._pos.dynamic_exit_last_error or "submission_returned_no_order"
            logger.error(
                "DYNAMIC_EXIT_SUBMISSION_DEFERRED asset=%s venue=%s attempt=%s; native hard protection remains active",
                self._asset_id, self._pos.exchange, attempt,
            )
            return
        order_id = str(result.get("order_id") or "")
        lifecycle = str(result.get("exit_lifecycle") or result.get("status") or "")
        with self._entry_lock:
            if order_id:
                self._pos.dynamic_exit_order_id = order_id
                self._pos.phase = PositionPhase.EXITING
                self._pos.manual_exit_reason = reason
                self._pos.dynamic_exit_last_error = ""
            elif lifecycle.upper().startswith("ALREADY_FLAT"):
                # A protective order may have won the race before the alpha exit
                # was sent. Attribute that exact fill before resetting state.
                self._pos.phase = PositionPhase.RECONCILIATION_REQUIRED
                self._pos.manual_exit_reason = reason
        if order_id:
            logger.warning(
                "DYNAMIC_EXIT_REDUCE_ONLY_SUBMITTED asset=%s venue=%s order=%s attempt=%s protection_retained_until_flat_reconciled=true",
                self._asset_id, self._pos.exchange, order_id, attempt,
            )
        elif lifecycle.upper().startswith("ALREADY_FLAT"):
            logger.info(
                "DYNAMIC_EXIT_RACE_BROKER_ALREADY_FLAT asset=%s venue=%s; resolving tracked exit fill",
                self._asset_id, self._pos.exchange,
            )

    def _position_reconciliation_loop(self) -> None:
        interval = max(0.25, float(_cfg("POSITION_RECONCILIATION_REFRESH_SEC", 1.0)))
        flat_retry = max(interval, float(_cfg("POSITION_RECONCILIATION_FLAT_UNCONFIRMED_SEC", 2.0)))
        try:
            while not self._runtime_stop_requested.is_set():
                with self._entry_lock:
                    if self._pos.is_flat() or self._pos.phase is PositionPhase.ENTERING:
                        break
                    mark_price = float(self._latest_event_price or self._pos.entry_price or 0.0)
                with self._position_reconcile_lock:
                    manager = self._position_reconcile_manager
                    risk_manager = self._position_reconcile_risk_manager
                if manager is None or risk_manager is None:
                    break
                next_interval = interval
                try:
                    with self._entry_lock:
                        dynamic_exit_pending = bool(self._pos.dynamic_exit_requested and not self._pos.dynamic_exit_order_id)
                    if dynamic_exit_pending:
                        self._service_dynamic_exit_request(manager)
                        next_interval = min(next_interval, max(0.25, float(_cfg("DYNAMIC_EXIT_REDUCE_ONLY_RETRY_SEC", 1.0))))
                    broker_pos = manager.get_open_position()
                    if isinstance(broker_pos, Mapping) and _num(broker_pos.get("size"), 0.0) <= 0:
                        if not self._finalise_confirmed_exit(manager, risk_manager, mark_price):
                            with self._entry_lock:
                                if not self._pos.is_flat():
                                    self._pos.phase = PositionPhase.RECONCILIATION_REQUIRED
                            self._notify_exit_reconciliation_required(mark_price)
                            next_interval = flat_retry
                        else:
                            remove_exposure = getattr(risk_manager, "remove_open_exposure", None)
                            if callable(remove_exposure):
                                remove_exposure(f"{self._desk_id(self._pos.exchange, self._pos.execution_symbol)}:{self._pos.execution_symbol}")
                            with self._entry_lock:
                                self._pos = PositionState(asset_id=self._asset_id)
                            if callable(self._market_wakeup):
                                try:
                                    self._market_wakeup()
                                except Exception:
                                    pass
                            break
                except Exception as exc:
                    logger.debug("Position reconciliation query deferred asset=%s venue=%s: %s", self._asset_id, self._pos.exchange, exc)
                self._position_reconcile_wakeup.wait(timeout=next_interval)
                self._position_reconcile_wakeup.clear()
        finally:
            with self._position_reconcile_lock:
                self._position_reconcile_thread = None

    def _venue_cash_refresh_loop(self) -> None:
        interval = max(15.0, float(_cfg("VENUE_BALANCE_REFRESH_SEC", 30.0)))
        while not self._venue_cash_refresh_stop.is_set():
            order_manager = self._runtime_order_manager
            if order_manager is not None:
                for venue in sorted(self._routeable_venues(order_manager)):
                    manager = self._execution_manager_for(order_manager, venue)
                    try:
                        bal = manager.get_balance() if manager is not None and hasattr(manager, "get_balance") else {}
                        if isinstance(bal, dict) and not bal.get("error") and bal.get("balance_verified") is not False:
                            available = max(0.0, _num((bal or {}).get("available"), 0.0))
                            source = str((bal or {}).get("source") or f"{venue}_live_balance")
                            with self._venue_cash_lock:
                                self._venue_cash_cache[str(venue).lower()] = (time.monotonic(), available, source)
                    except Exception as exc:
                        logger.debug("async venue collateral refresh unavailable venue=%s: %s", venue, exc)
            self._venue_cash_refresh_stop.wait(interval)

    def _signal_market_event(self, price: float = 0.0) -> None:
        if price > 0:
            self._latest_event_price = float(price)
        self._latest_event_ts = time.time()
        self._market_event.set()
        if callable(self._market_wakeup):
            try:
                self._market_wakeup()
            except Exception:
                pass

    def _on_realtime_quote(self, price: float) -> None:
        self._signal_market_event(float(price or 0.0))

    def _on_realtime_trade(self, price: float, quantity: float = 0.0, side: str = "") -> None:
        _ = quantity, side
        self._signal_market_event(float(price or 0.0))

    def consume_market_event(self) -> bool:
        signalled = self._market_event.is_set()
        if signalled:
            self._market_event.clear()
        return bool(signalled)

    def wait_for_market_event(self, timeout: float) -> bool:
        signalled = self._market_event.wait(max(0.0, float(timeout)))
        if signalled:
            self._market_event.clear()
        return bool(signalled)

    def has_urgent_structural_monitor(self) -> bool:
        return bool(not self._pos.is_flat())

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
        if self._pos.phase is PositionPhase.ENTERING:
            # The execution supervisor owns fill/protection reconciliation while
            # this candidate is pending; no duplicate entry is allowed.
            return
        if not self._pos.is_flat():
            self._monitor_position(data_manager, order_manager, risk_manager)
            return
        decision = self.evaluate(data_manager, order_manager, risk_manager, now_ms)
        if decision.approved and self._submission_lock is not None and callable(self._portfolio_submission_gate):
            with self._submission_lock:
                allowed, reason = self._portfolio_submission_gate()
                if not allowed:
                    decision = replace(
                        decision, decision=DecisionOutput.NO_TRADE_RISK_BUDGET,
                        reasons=[f"atomic_portfolio_submission_gate:{reason}"],
                        model_values={**dict(decision.model_values or {}), "atomic_submission_recheck": reason},
                    )
                else:
                    self._last_decision = decision
                    self._last_decision_ts = time.time()
                    self._log_decision_calculation(decision)
                    self._persist_decision(decision, now_ms)
                    if bool(_cfg("EXECUTION_ASYNC_ENTRY_LIFECYCLE_ENABLED", False)) and decision.venue != "groww":
                        self._submit_approved_async(decision, order_manager, risk_manager)
                    else:
                        self._execute_approved(decision, order_manager, risk_manager)
                    return
        self._last_decision = decision
        self._last_decision_ts = time.time()
        self._log_decision_calculation(decision)
        self._persist_decision(decision, now_ms)
        if decision.approved:
            if bool(_cfg("EXECUTION_ASYNC_ENTRY_LIFECYCLE_ENABLED", False)) and decision.venue != "groww":
                self._submit_approved_async(decision, order_manager, risk_manager)
            else:
                self._execute_approved(decision, order_manager, risk_manager)

    @staticmethod
    def _round_or_none(value: Any, digits: int = 3) -> float | None:
        try:
            out = float(value)
            return round(out, digits) if math.isfinite(out) else None
        except Exception:
            return None

    @staticmethod
    def _telemetry_reason_key(reason: Any) -> str:
        text = str(reason or "").strip()
        if not text:
            return ""
        return text.split(":", 1)[0].strip() or text

    def _decision_telemetry_signature(self, decision: OpportunityDecision) -> tuple[Any, ...]:
        model = decision.model_values or {}
        option_context = model.get("option_volatility_context", {}) if isinstance(model.get("option_volatility_context"), dict) else {}
        execution_feed = model.get("execution_feed", {}) if isinstance(model.get("execution_feed"), dict) else {}
        session_book = model.get("session_execution_book", {}) if isinstance(model.get("session_execution_book"), dict) else {}
        reasons = [self._telemetry_reason_key(reason) for reason in decision.reasons[:2]]
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
        regime_key = decision.regime.value
        stable_non_actionable_reasons = {
            "btc_reference_microstate_unavailable",
            "near_touch_depth_unavailable",
            "nifty_no_valid_structural_displacement",
            "unqualified_signal_below_required_edge",
            "flow_signal_flat",
            "active_option_ltp_depth_stream_not_fresh",
            "direction_specific_groww_vehicle_not_fresh_or_not_executable",
            "net_edge_does_not_clear_uncertainty_and_minimum",
            "option_premium_edge_does_not_clear_cost_and_uncertainty",
            "order_notional_below_venue_minimum",
            "policy_margin_budget_exceeded",
            "quantity_rounds_to_zero_after_venue_step",
            "selected_route_cost_exceeds_limit",
            "selected_route_nonpositive_expected_net_edge",
            "selected_route_nonpositive_expected_net_profit",
            "signal_horizon_below_protected_execution_min",
            "stop_risk_budget_exceeded",
        }
        if (
            decision.decision in {
                DecisionOutput.NO_TRADE_INSUFFICIENT_EDGE,
                DecisionOutput.NO_TRADE_EXECUTION_UNSAFE,
                DecisionOutput.NO_TRADE_RISK_BUDGET,
            }
            and reasons and reasons[0] in stable_non_actionable_reasons
        ):
            # Regime stays in the heartbeat payload but cannot cause operational
            # transition spam while the same execution/edge blocker is unchanged.
            regime_key = "HEARTBEAT_ONLY_WHILE_NON_ACTIONABLE"
            direction_key = "HEARTBEAT_ONLY_WHILE_NON_ACTIONABLE"
            signal_source = f"non_actionable:{reasons[0]}"
        return (
            decision.decision.value, direction_key, regime_key, tuple(reasons),
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
                # No direction means a CE or PE has not been activated for order
                # measurement. Do not report this internal sentinel as zero liquidity.
                payload["liquidity_score"] = None
                payload["liquidity_score_scope"] = "NOT_EVALUATED_UNTIL_DIRECTION_ACTIVATES_CE_OR_PE"
                payload["preselected_ce_pe_book_live"] = bool(
                    isinstance(session_book, dict) and session_book.get("status") == "READY"
                )
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
                "near_touch_depth_usd": self._round_or_none(model.get("near_touch_depth_usd"), 2),
                "weighted_signal_bps": self._round_or_none(model.get("weighted_signal_bps")),
                "signal_architecture": model.get("signal_architecture"),
                "parent_structural_alpha_bps": self._round_or_none(model.get("parent_structural_alpha_bps")),
                "parent_structural_uncertainty_bps": self._round_or_none(model.get("parent_structural_uncertainty_bps")),
                "parent_state_id": model.get("parent_state_id"),
                "child_timing_alpha_bps": self._round_or_none(model.get("child_timing_alpha_bps")),
                "child_timing_contribution_bps": self._round_or_none(model.get("child_timing_contribution_bps")),
                "microstructure_cannot_originate_or_flip_thesis": bool(model.get("microstructure_cannot_originate_or_flip_thesis", False)),
                "signal_components_bps": {
                    "ofi": self._round_or_none(model.get("ofi_component_bps")),
                    "tfi": self._round_or_none(model.get("tfi_component_bps")),
                    "microprice": self._round_or_none(model.get("microprice_component_bps")),
                    "dislocation": self._round_or_none(model.get("dislocation_component_bps")),
                },
            }
            composite = model.get("composite_asset_intelligence", {}) if isinstance(model.get("composite_asset_intelligence"), dict) else {}
            if composite:
                payload["composite"] = {
                    "factor_id": composite.get("factor_id"),
                    "equivalence_group": composite.get("equivalence_group"),
                    "execution_sources": composite.get("execution_sources", []),
                    "factor_sources": composite.get("factor_sources", []),
                    "transferable_structural_alpha_bps": self._round_or_none(composite.get("transferable_structural_alpha_bps")),
                    "transferable_microstructure_alpha_bps": self._round_or_none(composite.get("transferable_microstructure_alpha_bps")),
                    "factor_context_alpha_bps": self._round_or_none(composite.get("factor_context_alpha_bps")),
                    "basis_translation_enabled": bool(composite.get("basis_translation_enabled", False)),
                }
            venue_selection = model.get("venue_selection", {}) if isinstance(model.get("venue_selection"), dict) else {}
            if venue_selection:
                selected_venue_key = str(venue_selection.get("selected_venue", "")).lower()
                estimates = venue_selection.get("estimates", {}) if isinstance(venue_selection.get("estimates"), dict) else {}
                selected_estimate = estimates.get(selected_venue_key, {}) if isinstance(estimates.get(selected_venue_key), dict) else {}
                payload["route"] = {
                    "selected_venue": venue_selection.get("selected_venue"),
                    "selected_symbol": venue_selection.get("selected_symbol"),
                    "selected_cost_bps": self._round_or_none(venue_selection.get("selected_cost_bps")),
                    "reason": venue_selection.get("reason"),
                    "diagnostics": {
                        "proposed_notional_usd": self._round_or_none(selected_estimate.get("proposed_notional_usd"), 2),
                        "available_cash_usd": self._round_or_none(selected_estimate.get("available_cash_usd"), 2),
                        "required_margin_usd": self._round_or_none(selected_estimate.get("required_margin_usd"), 2),
                        "directional_near_depth_usd": self._round_or_none(selected_estimate.get("near_depth_usd"), 2),
                        "effective_touch_bps": self._round_or_none(selected_estimate.get("effective_touch_bps")),
                        "fee_bps": self._round_or_none(selected_estimate.get("fee_bps")),
                        "impact_bps": self._round_or_none(selected_estimate.get("impact_bps")),
                        "funding_cost_bps": self._round_or_none(selected_estimate.get("funding_cost_bps")),
                        "latency_penalty_bps": self._round_or_none(selected_estimate.get("latency_penalty_bps")),
                        "quality_penalty_bps": self._round_or_none(selected_estimate.get("quality_penalty_bps")),
                        "liquidity_penalty_bps": self._round_or_none(selected_estimate.get("liquidity_penalty_bps")),
                        "protection_activation_penalty_bps": self._round_or_none(selected_estimate.get("protection_activation_penalty_bps")),
                    },
                }
            venue_state = model.get("venue_market_state", {}) if isinstance(model.get("venue_market_state"), dict) else {}
            selected_state = venue_state.get(str(decision.venue).lower(), {}) if isinstance(venue_state, dict) else {}
            if isinstance(selected_state, dict) and selected_state:
                payload["selected_venue_market_state"] = {
                    "venue": decision.venue, "symbol": decision.instrument,
                    "signed_alpha_bps": self._round_or_none(selected_state.get("signed_alpha_bps")),
                    "confidence": self._round_or_none(selected_state.get("confidence")),
                    "regime": selected_state.get("regime_label"),
                    "reason": selected_state.get("reason"),
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
        shadow_validated = decision.decision is DecisionOutput.SHADOW_SIGNAL_VALIDATED
        event = "APPROVED" if decision.approved else ("SHADOW_VALIDATED" if shadow_validated and transition else ("TRANSITION" if transition else "HEARTBEAT"))
        try:
            compact = self._compact_decision_payload(decision, event)
            logger.info("🧮 DECISION_%s %s", event, json.dumps(compact, sort_keys=True, separators=(",", ":"), default=str))
            full_detail = bool(
                decision.approved or (shadow_validated and transition) or debug_every_tick
                or (transition and _cfg("INSTITUTIONAL_DECISION_TELEMETRY_FULL_ON_TRANSITION", False))
            )
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

        states = self._venue_states(data_manager)
        execution_state, btc_composite = self._microstructure_context(data_manager, venue, instrument, feed_health, states=states)
        market_states: dict[str, VenueMarketState] = {}
        if bool(_cfg("INSTITUTIONAL_ENABLE_MARKET_STATE_ALPHA", True)) and states:
            market_states = self._market_state_engine.build(data_manager, states)
        cross_venue_evidence = (
            build_continuous_cross_venue_evidence(self._asset_id, market_states, states)
            if self._asset_id.upper() == "BTC" and market_states else None
        )
        composite_decision = (
            self._composite_bus.build_decision(asset_id=self._asset_id, market_states=market_states, microstates=states)
            if bool(_cfg("INSTITUTIONAL_COMPOSITE_INTELLIGENCE_ENABLED", True)) and market_states else None
        )
        liquidity_score, zones = self._liquidity_score(data_manager, instrument, price, execution_state=execution_state)
        direction, directional_edge_bps, direction_reason, signal_breakdown = self._direction_and_edge(
            desk, price, liquidity_score, execution_state=execution_state, btc_composite=btc_composite,
            market_state=market_states.get(str(venue).lower()), cross_venue_evidence=cross_venue_evidence,
            composite_decision=composite_decision,
        )
        signal_origin_candidates: list[dict[str, Any]] = []
        if bool(_cfg("INDEPENDENT_VENUE_SIGNAL_ORIGINATION_ENABLED", False)) and states:
            best_origin, signal_origin_candidates = self._best_originating_venue_signal(
                data_manager, states, btc_composite, instrument, market_states, cross_venue_evidence, composite_decision
            )
            if best_origin is not None:
                venue = str(best_origin["venue"])
                instrument = str(best_origin["instrument"])
                desk = str(best_origin["desk"])
                price = float(best_origin["price"])
                execution_state = best_origin["state"]
                liquidity_score = float(best_origin["liquidity_score"])
                zones = best_origin["zones"]
                execution_quality = float(best_origin["execution_quality"])
                direction = best_origin["direction"]
                directional_edge_bps = float(best_origin["edge_bps"])
                direction_reason = f"independent_venue_origin:{venue}:{best_origin['reason']}"
                signal_breakdown = dict(best_origin["breakdown"])
                regime = self._market_state_regime(market_states.get(venue), regime)
        venue_selection = None
        venue_available_cash: dict[str, float] = {}
        # Broker-local capacities are kept as eligibility telemetry only. The
        # selection score itself is produced from a common base exposure.
        venue_candidate_notional: dict[str, float] = {}
        venue_required_margin: dict[str, float] = {}
        venue_comparison_notional: dict[str, float] = {}
        venue_comparison_margin: dict[str, float] = {}
        venue_comparison_quantity: float = 0.0
        venue_quantity_representation: dict[str, Any] = {}
        venue_selection_snapshot_ts_ns: int = 0
        routeable_candidates: set[str] = set()
        validated_edge_by_venue: dict[str, float] = {}
        if (
            bool(_cfg("VENUE_SELECTION_ENABLED", True))
            and direction is not Direction.NO_TRADE
            and states
        ):
            try:
                routeable = self._routeable_venues(order_manager)
                if signal_origin_candidates:
                    for row in signal_origin_candidates:
                        row_venue = str(row.get("venue", "")).lower()
                        if str(row.get("direction", "")) == direction.value and _num(row.get("edge_bps"), 0.0) > 0.0:
                            validated_edge_by_venue[row_venue] = _num(row.get("edge_bps"), 0.0)
                    # Execution may route only to a venue that independently
                    # validated the same directional thesis. No broker inherits
                    # another venue's alpha absent a fitted lead/lag transfer model.
                    if validated_edge_by_venue:
                        routeable = routeable.intersection(validated_edge_by_venue)
                if not validated_edge_by_venue:
                    validated_edge_by_venue[str(venue).lower()] = float(directional_edge_bps)
                    routeable = routeable.intersection({str(venue).lower()})
                routeable_candidates = set(routeable)
                venue_available_cash = self._venue_available_cash(order_manager, routeable_candidates)
                venue_candidate_notional, venue_required_margin = self._venue_selection_budgets(venue_available_cash)
                venue_selection_snapshot_ts_ns = time.time_ns()
                (
                    venue_comparison_quantity,
                    venue_comparison_notional,
                    venue_comparison_margin,
                    venue_quantity_representation,
                    routeable_candidates,
                ) = self._risk_normalised_route_inputs(
                    states=states,
                    candidate_venues=routeable_candidates,
                    capacity_notional_by_venue=venue_candidate_notional,
                    approved_quantity=None,
                )
                venue_selection = select_execution_venue(
                    states=states,
                    direction=direction,
                    asset_id=self._asset_id,
                    current_venue=venue,
                    routeable_venues=routeable_candidates,
                    notional_usd=0.0,
                    available_cash_by_venue=venue_available_cash,
                    required_margin_usd=0.0,
                    protection_capable_venues=self._protection_capable_venues(order_manager, routeable_candidates),
                    gross_edge_bps=directional_edge_bps,
                    gross_edge_by_venue=validated_edge_by_venue,
                    notional_by_venue=venue_comparison_notional,
                    required_margin_by_venue=venue_comparison_margin,
                    comparison_quantity=venue_comparison_quantity,
                    comparison_notional_usd=min(venue_comparison_notional.values()) if venue_comparison_notional else 0.0,
                    snapshot_ts_ns=venue_selection_snapshot_ts_ns,
                )
                if venue_selection.reason == "no_funded_protected_route_candidate":
                    direction = Direction.NO_TRADE
                    directional_edge_bps = 0.0
                    direction_reason = "no_funded_protected_route_candidate"
                selected_state = (
                    None if venue_selection.reason == "no_funded_protected_route_candidate"
                    else states.get(str(venue_selection.selected_venue).lower())
                )
                if selected_state is not None and venue_selection.selected_venue:
                    venue = str(venue_selection.selected_venue).lower()
                    instrument = self._symbol_for_venue(venue, instrument)
                    desk = self._desk_id(venue, instrument)
                    execution_state = selected_state
                    if float(selected_state.mid or 0.0) > 0:
                        price = float(selected_state.mid)
                    liquidity_score, zones = self._liquidity_score(data_manager, instrument, price, execution_state=execution_state)
                    execution_quality = min(execution_quality, float(selected_state.feed_quality_score or 0.0))
                    selected_composite = btc_composite
                    if self._asset_id.upper() == "BTC" and "delta" in states:
                        refs = {k: v for k, v in states.items() if k != "delta"}
                        selected_composite = build_btc_composite_state(delta_state=states["delta"], reference_states=refs) if refs else None
                    venue_direction, venue_edge_bps, venue_reason, venue_breakdown = self._direction_and_edge(
                        desk, price, liquidity_score, execution_state=execution_state, btc_composite=selected_composite,
                        market_state=market_states.get(venue), cross_venue_evidence=cross_venue_evidence,
                        composite_decision=composite_decision,
                    )
                    regime = self._market_state_regime(market_states.get(venue), regime)
                    if venue_direction is Direction.NO_TRADE:
                        direction = Direction.NO_TRADE
                        directional_edge_bps = 0.0
                        direction_reason = f"selected_venue_signal_reject:{venue}:{venue_reason}"
                        signal_breakdown = venue_breakdown
                    elif venue_direction is not direction:
                        original_direction = direction.value
                        direction = Direction.NO_TRADE
                        directional_edge_bps = 0.0
                        direction_reason = f"selected_venue_signal_disagreement:{original_direction}->{venue_direction.value}:{venue}"
                        signal_breakdown = {
                            **venue_breakdown,
                            "selected_venue_signal_reason": venue_reason,
                            "pre_selection_direction": original_direction,
                        }
                    else:
                        direction = venue_direction
                        directional_edge_bps = venue_edge_bps
                        direction_reason = f"selected_venue_validated:{venue}:{venue_reason}"
                        signal_breakdown = {
                            **venue_breakdown,
                            "selected_venue_signal_reason": venue_reason,
                        }
                        btc_composite = selected_composite
            except Exception as exc:
                logger.debug("venue selection unavailable: %s", exc)
                venue_selection = None
        cost_components = self._execution_cost_components(data_manager, execution_state=execution_state)
        route_governance_block = ""
        selected_route_estimate = None
        if venue_selection is not None and venue_selection.selected_venue:
            selected_route_estimate = venue_selection.estimates.get(str(venue_selection.selected_venue).lower())
            selected_route_cost_bps = float(venue_selection.selected_cost_bps)
            cost_components["venue_selection_cost_bps"] = selected_route_cost_bps
            # The venue-selection cost is authoritative even when the selected
            # venue is the current venue.  This prevents approval from using a
            # cheap local spread estimate after the route model finds a loss.
            cost_components["total_cost_bps"] = max(0.0, selected_route_cost_bps)
            max_route_cost_bps = float(_cfg("VENUE_SELECTION_MAX_COST_BPS", 100.0))
            route_expected_edge = getattr(selected_route_estimate, "expected_net_edge_bps", None)
            route_expected_profit = getattr(selected_route_estimate, "expected_net_profit_usd", None)
            if selected_route_cost_bps < 0.0:
                route_governance_block = f"selected_route_invalid_negative_cost:{selected_route_cost_bps:.3f}"
            elif selected_route_cost_bps > max_route_cost_bps:
                route_governance_block = f"selected_route_cost_exceeds_limit:{selected_route_cost_bps:.2f}>{max_route_cost_bps:.2f}"
            elif route_expected_edge is not None and float(route_expected_edge) <= 0.0:
                route_governance_block = f"selected_route_nonpositive_expected_net_edge:{float(route_expected_edge):.3f}"
            elif route_expected_profit is not None and float(route_expected_profit) <= 0.0:
                route_governance_block = f"selected_route_nonpositive_expected_net_profit:{float(route_expected_profit):.6f}"
        costs_bps = cost_components["total_cost_bps"]
        uncertainty_bps = self._uncertainty_bps(regime, liquidity_score, execution_quality)
        uncertainty_bps += max(0.0, _num(signal_breakdown.get("market_state_uncertainty_bps"), 0.0))
        uncertainty_bps += max(0.0, _num(signal_breakdown.get("cross_venue_uncertainty_bps"), 0.0))
        net_edge = directional_edge_bps - costs_bps
        venue_research_getter = getattr(data_manager, "get_venue_microstructure_research_state", None)
        if callable(venue_research_getter):
            research_state = venue_research_getter(venue) or {}
        else:
            research_state_getter = getattr(data_manager, "get_microstructure_research_state", None)
            research_state = research_state_getter() if callable(research_state_getter) else {}
        self._protection_engine.observe(
            signal_bps=_num(signal_breakdown.get("weighted_signal_bps"), 0.0),
            timestamp_s=time.time(), research_state=research_state,
        )
        decay_state = self._protection_engine.signal_decay(current_edge_bps=directional_edge_bps, total_exit_cost_bps=costs_bps)
        impact_state = self._protection_engine.kyle_impact(exit_notional=0.0)
        toxicity_state = self._protection_engine.vpin()
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
            "selected_execution_venue": venue,
            "selected_execution_symbol": instrument,
            "signal_origin_candidates": signal_origin_candidates,
            "exposure_equivalence_group": self._asset_id,
            "venue_market_state": {k: v.as_dict() for k, v in market_states.items()},
            "composite_asset_intelligence": composite_decision.as_dict() if composite_decision is not None else {},
            "cross_venue_evidence": cross_venue_evidence.as_dict() if cross_venue_evidence is not None else {},
            "protection_research_venue": venue,
            "exit_model_state": {
                "signal_decay": asdict(decay_state),
                "kyle_impact": asdict(impact_state),
                "vpin": asdict(toxicity_state),
            },
        }
        model_values.update(signal_breakdown)
        if venue_selection is not None:
            model_values["venue_selection"] = venue_selection.as_dict()
            model_values["venue_available_cash_usd"] = dict(venue_available_cash)
            model_values["venue_capacity_notional_usd"] = dict(venue_candidate_notional)
            model_values["venue_capacity_required_margin_usd"] = dict(venue_required_margin)
            model_values["venue_comparison_quantity"] = float(venue_comparison_quantity)
            model_values["venue_comparison_notional_usd"] = dict(venue_comparison_notional)
            model_values["venue_comparison_required_margin_usd"] = dict(venue_comparison_margin)
            model_values["venue_quantity_representation"] = dict(venue_quantity_representation)
            model_values["venue_selection_snapshot_ts_ns"] = int(venue_selection_snapshot_ts_ns)
            model_values["selected_venue_available_cash_usd"] = float(venue_available_cash.get(str(venue).lower(), 0.0))
            model_values["venue_validated_directional_edge_bps"] = dict(validated_edge_by_venue)
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
        if route_governance_block and direction is not Direction.NO_TRADE:
            model_values["route_governance_block"] = route_governance_block
            return self._decision(
                desk=desk, venue=venue, instrument=instrument,
                decision=DecisionOutput.NO_TRADE_EXECUTION_UNSAFE, direction=direction, regime=regime,
                expected_net_edge_bps=net_edge, uncertainty_bps=uncertainty_bps,
                liquidity_score=liquidity_score, execution_quality_score=execution_quality,
                sizing=None, protection_plan=None, reasons=[route_governance_block],
                model_values=model_values, research_features=features,
            )

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

        protection = self._protection_plan(
            desk, direction, price, liquidity_score, data_manager=data_manager,
            gross_edge_bps=directional_edge_bps, costs_bps=costs_bps,
            venue=venue, instrument=instrument, execution_state=execution_state, regime=regime,
            venue_market_state=market_states.get(str(venue).lower()),
        )
        model_values["dynamic_protection_plan"] = protection.diagnostics if protection is not None else {}
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

        sizing = self._size_position(
            desk, instrument, direction, price, net_edge, liquidity_score, protection, risk_manager,
            venue=venue, balance_source=self._execution_manager_for(order_manager, venue),
            venue_market_state=market_states.get(str(venue).lower()),
            available_cash_snapshot=(venue_available_cash.get(str(venue).lower()) if venue_available_cash else None),
            balance_source_label=(f"shared_verified_collateral_snapshot:{str(venue).lower()}" if venue_available_cash else None),
        )
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

        # Final execution authorisation is based on one exact approved base
        # quantity replayed across every direction-validated route at one
        # snapshot. Broker balance controls eligibility only; it never changes
        # the quantity used to rank Delta/CoinSwitch/Hyperliquid.
        if (
            bool(_cfg("VENUE_SELECTION_RISK_NORMALISED_LEDGER_ENABLED", True))
            and venue_selection is not None
            and states
            and validated_edge_by_venue
        ):
            final_candidate_venues = self._routeable_venues(order_manager).intersection(set(validated_edge_by_venue))
            (
                final_comparison_quantity,
                final_comparison_notional,
                final_comparison_margin,
                final_quantity_representation,
                final_routeable_candidates,
            ) = self._risk_normalised_route_inputs(
                states=states,
                candidate_venues=final_candidate_venues,
                capacity_notional_by_venue=venue_candidate_notional,
                approved_quantity=float(sizing.quantity),
            )
            final_ledger = select_execution_venue(
                states=states, direction=direction, asset_id=self._asset_id, current_venue=venue,
                routeable_venues=final_routeable_candidates, notional_usd=0.0,
                available_cash_by_venue=venue_available_cash, required_margin_usd=0.0,
                protection_capable_venues=self._protection_capable_venues(order_manager, final_routeable_candidates),
                gross_edge_bps=directional_edge_bps, gross_edge_by_venue=validated_edge_by_venue,
                notional_by_venue=final_comparison_notional, required_margin_by_venue=final_comparison_margin,
                comparison_quantity=final_comparison_quantity,
                comparison_notional_usd=min(final_comparison_notional.values()) if final_comparison_notional else 0.0,
                snapshot_ts_ns=time.time_ns(),
            )
            final_ledger_dict = final_ledger.as_dict()
            final_ledger_dict["quantity_representation"] = final_quantity_representation
            final_ledger_dict["capacity_notional_usd"] = dict(venue_candidate_notional)
            model_values["venue_selection_ledger"] = final_ledger_dict
            logger.info(
                "🧾 VENUE_SELECTION_LEDGER %s",
                json.dumps({
                    "asset": self._asset_id, "direction": direction.value, "approved_quantity": sizing.quantity,
                    "selected_venue": final_ledger.selected_venue, "selected_cost_bps": final_ledger.selected_cost_bps,
                    "reason": final_ledger.reason, "snapshot_ts_ns": final_ledger.snapshot_ts_ns,
                    "quantity_representation": final_quantity_representation,
                    "estimates": {k: v.as_dict() for k, v in final_ledger.estimates.items()},
                }, sort_keys=True, separators=(",", ":"), default=str),
            )
            if str(venue).lower() not in final_routeable_candidates:
                return self._decision(
                    desk=desk, venue=venue, instrument=instrument, decision=DecisionOutput.NO_TRADE_EXECUTION_UNSAFE,
                    direction=direction, regime=regime, expected_net_edge_bps=net_edge, uncertainty_bps=uncertainty_bps,
                    liquidity_score=liquidity_score, execution_quality_score=execution_quality, sizing=sizing,
                    protection_plan=protection, reasons=[f"selected_venue_cannot_execute_risk_normalised_quantity:{venue}"],
                    model_values=model_values, research_features=features,
                )
            if str(final_ledger.selected_venue).lower() != str(venue).lower():
                return self._decision(
                    desk=desk, venue=venue, instrument=instrument, decision=DecisionOutput.NO_TRADE_EXECUTION_UNSAFE,
                    direction=direction, regime=regime, expected_net_edge_bps=net_edge, uncertainty_bps=uncertainty_bps,
                    liquidity_score=liquidity_score, execution_quality_score=execution_quality, sizing=sizing,
                    protection_plan=protection,
                    reasons=[f"risk_normalised_route_changed_after_final_sizing:{venue}->{final_ledger.selected_venue}"],
                    model_values=model_values, research_features=features,
                )
            final_selected_estimate = final_ledger.estimates.get(str(venue).lower())
            if final_selected_estimate is None:
                return self._decision(
                    desk=desk, venue=venue, instrument=instrument, decision=DecisionOutput.NO_TRADE_EXECUTION_UNSAFE,
                    direction=direction, regime=regime, expected_net_edge_bps=net_edge, uncertainty_bps=uncertainty_bps,
                    liquidity_score=liquidity_score, execution_quality_score=execution_quality, sizing=sizing,
                    protection_plan=protection, reasons=["risk_normalised_selected_route_estimate_missing"],
                    model_values=model_values, research_features=features,
                )
            costs_bps = float(final_selected_estimate.total_cost_bps)
            cost_components["venue_selection_cost_bps"] = costs_bps
            cost_components["total_cost_bps"] = costs_bps
            net_edge = directional_edge_bps - costs_bps
            model_values["execution_cost_components"] = dict(cost_components)
            model_values["costs_bps"] = costs_bps
            model_values["net_edge_bps"] = net_edge
            if net_edge <= max(float(_cfg("INSTITUTIONAL_MIN_NET_EDGE_BPS", 3.0)), uncertainty_bps):
                return self._decision(
                    desk=desk, venue=venue, instrument=instrument, decision=DecisionOutput.NO_TRADE_INSUFFICIENT_EDGE,
                    direction=direction, regime=regime, expected_net_edge_bps=net_edge, uncertainty_bps=uncertainty_bps,
                    liquidity_score=liquidity_score, execution_quality_score=execution_quality, sizing=sizing,
                    protection_plan=protection, reasons=["risk_normalised_final_quantity_net_edge_not_sufficient"],
                    model_values=model_values, research_features=features,
                )

        protection = self._protection_plan(
            desk, direction, price, liquidity_score, data_manager=data_manager,
            gross_edge_bps=directional_edge_bps, costs_bps=costs_bps,
            position_notional=sizing.notional, quantity=sizing.quantity,
            venue=venue, instrument=instrument, execution_state=execution_state, regime=regime,
            venue_market_state=market_states.get(str(venue).lower()),
        )
        model_values["dynamic_protection_plan"] = protection.diagnostics if protection is not None else {}
        if protection is None or not protection.protection_feasible:
            return self._decision(
                desk=desk, venue=venue, instrument=instrument, decision=DecisionOutput.NO_TRADE_EXECUTION_UNSAFE,
                direction=direction, regime=regime, expected_net_edge_bps=net_edge, uncertainty_bps=uncertainty_bps,
                liquidity_score=liquidity_score, execution_quality_score=execution_quality, sizing=sizing,
                protection_plan=protection, reasons=(protection.reasons if protection else ["sized_dynamic_protection_unavailable"]),
                model_values=model_values, research_features=features,
            )

        live_allowed, live_reason = _live_routing_permission(venue)
        if not live_allowed:
            return self._decision(
                desk=desk,
                venue=venue,
                instrument=instrument,
                decision=DecisionOutput.SHADOW_SIGNAL_VALIDATED,
                direction=direction,
                regime=regime,
                expected_net_edge_bps=net_edge,
                uncertainty_bps=uncertainty_bps,
                liquidity_score=liquidity_score,
                execution_quality_score=execution_quality,
                sizing=sizing,
                protection_plan=protection,
                reasons=[live_reason],
                model_values=model_values,
                research_features=features,
            )

        gate_allowed, gate_reason = self._entry_risk_gate(
            risk_manager,
            balance_source=self._execution_manager_for(order_manager, venue),
            cached_equity=float(getattr(sizing, "available_cash_used", 0.0) or 0.0),
        )
        if not gate_allowed:
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
                reasons=[gate_reason],
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
        # Keep the decay estimator live before a trade triggers: the structural pressure
        # series is continuous, while the executable BULLISH/BEARISH thesis is discrete.
        signed_thesis_edge = (
            _num(thesis_features.get("break_up_bps"), 0.0)
            - _num(thesis_features.get("break_down_bps"), 0.0)
            + 0.25 * _num(thesis_features.get("alignment_15m_bps"), 0.0)
        )
        self._protection_engine.observe(signal_bps=signed_thesis_edge, timestamp_s=time.time(), research_state={})
        model_values["underlying_structural_pressure_bps"] = signed_thesis_edge
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
        option_state = {
            "selected_option_symbol": getattr(choice, "selected_symbol", ""),
            "selected_option_delta": delta,
            "theta_to_premium_per_day": theta_day_ratio,
            "volatility_context": volatility_context,
        }
        protection = self._option_premium_protection_plan(
            data_manager, option_price, thesis, premium_edge_bps, costs_bps + theta_carry_bps,
            option_state=option_state,
        )
        model_values["dynamic_protection_plan"] = protection.diagnostics if protection is not None else {}
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
        protection = self._option_premium_protection_plan(
            data_manager, option_price, thesis, premium_edge_bps, costs_bps + theta_carry_bps,
            position_notional=sizing.notional, quantity=sizing.quantity, option_state=option_state,
        )
        model_values["dynamic_protection_plan"] = protection.diagnostics if protection is not None else {}
        if protection is None or not protection.protection_feasible:
            return self._decision(
                desk=DeskId.INDIA_OPTIONS.value, venue=venue, instrument=instrument,
                decision=DecisionOutput.NO_TRADE_EXECUTION_UNSAFE, direction=thesis, regime=regime,
                expected_net_edge_bps=net_edge, uncertainty_bps=uncertainty_bps, liquidity_score=liquidity_score,
                execution_quality_score=feed_health.quality_score, sizing=sizing, protection_plan=protection,
                reasons=(protection.reasons if protection else ["sized_option_dynamic_protection_unavailable"]),
                model_values=model_values, research_features=features,
            )

        live_allowed, live_reason = _live_routing_permission(venue)
        if not live_allowed:
            return self._decision(
                desk=DeskId.INDIA_OPTIONS.value, venue=venue, instrument=instrument,
                decision=DecisionOutput.SHADOW_SIGNAL_VALIDATED, direction=thesis,
                regime=regime, expected_net_edge_bps=net_edge, uncertainty_bps=uncertainty_bps,
                liquidity_score=liquidity_score, execution_quality_score=feed_health.quality_score,
                sizing=sizing, protection_plan=protection, reasons=[live_reason],
                model_values=model_values, research_features=features,
            )
        gate_allowed, gate_reason = self._entry_risk_gate(risk_manager)
        if not gate_allowed:
            return self._decision(
                desk=DeskId.INDIA_OPTIONS.value, venue=venue, instrument=instrument,
                decision=DecisionOutput.NO_TRADE_RISK_BUDGET, direction=thesis,
                regime=regime, expected_net_edge_bps=net_edge, uncertainty_bps=uncertainty_bps,
                liquidity_score=liquidity_score, execution_quality_score=feed_health.quality_score,
                sizing=sizing, protection_plan=protection, reasons=[gate_reason],
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

    def _option_premium_protection_plan(
        self, data_manager, premium: float, direction: Direction, gross_edge_bps: float, execution_cost_bps: float,
        *, position_notional: float = 0.0, quantity: float = 0.0, option_state: Mapping[str, Any] | None = None,
    ) -> ProtectionPlan | None:
        try:
            candles = list(data_manager.get_execution_candles("1m", 40) or [])
        except Exception:
            candles = []
        if premium <= 0 or len(candles) < int(_cfg("GROWW_OPTION_PROTECTION_MIN_ATR_BARS", 10)):
            return ProtectionPlan(premium, premium, premium, "GROWW_OCO_AFTER_FILL", False, ["option_premium_dynamic_volatility_warmup_unavailable"], diagnostics={})
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
            return ProtectionPlan(premium, premium, premium, "GROWW_OCO_AFTER_FILL", False, ["option_premium_dynamic_volatility_invalid"], diagnostics={})
        premium_realized_range = sum(ranges[-14:]) / min(len(ranges), 14)
        return self._protection_engine.build_plan(
            direction=direction, entry_price=premium, volatility_price=premium_realized_range,
            gross_edge_bps=gross_edge_bps, execution_cost_bps=execution_cost_bps,
            protection_type="GROWW_OCO_AFTER_FILL", asset_class="option",
            position_notional=position_notional, quantity=quantity, option_state=option_state,
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
                    model_version="dynamic-state-dependent-protection-v2.12",
                    policy_version="config-owned-protected-execution-v2.12",
                )
            )
        except Exception:
            pass

    def _submit_approved_async(self, decision: OpportunityDecision, order_manager, risk_manager) -> None:
        if self._runtime_stop_requested.is_set():
            logger.info("Protected entry skipped during runtime stop asset=%s venue=%s instrument=%s", self._asset_id, decision.venue, decision.instrument)
            return
        sizing = decision.sizing
        protection = decision.protection_plan
        if sizing is None or protection is None:
            return
        with self._entry_lock:
            if self._pos.phase is not PositionPhase.FLAT:
                return
            self._pos = PositionState(
                phase=PositionPhase.ENTERING,
                side="long" if decision.direction in {Direction.LONG, Direction.BULLISH} else "short",
                quantity=float(sizing.quantity), entry_price=float(protection.entry_price),
                sl_price=float(protection.stop_price), tp_price=float(protection.target_price),
                exchange=decision.venue, execution_symbol=decision.instrument, asset_id=self._asset_id,
                protection_model=protection.protection_type, protection_confirmed=False,
                quant_components=decision.model_values,
            )
            self._entry_thread = threading.Thread(
                target=self._execute_approved_worker, args=(decision, order_manager, risk_manager),
                daemon=True, name=f"protected-entry-{self._asset_id}-{decision.venue}",
            )
            self._entry_thread.start()
            logger.info("🧵 PROTECTED_ENTRY_SUPERVISOR launched asset=%s venue=%s instrument=%s; signal loop remains live", self._asset_id, decision.venue, decision.instrument)

    def _execute_approved_worker(self, decision: OpportunityDecision, order_manager, risk_manager) -> None:
        try:
            self._execute_approved(decision, order_manager, risk_manager)
        except Exception as exc:
            logger.exception("Protected entry supervisor failed for %s/%s: %s", decision.venue, decision.instrument, exc)
        finally:
            with self._entry_lock:
                if self._pos.phase is PositionPhase.ENTERING:
                    self._pos = PositionState(asset_id=self._asset_id)
                self._entry_thread = None
            if callable(self._market_wakeup):
                try:
                    self._market_wakeup()
                except Exception:
                    pass

    def _execute_approved(self, decision: OpportunityDecision, order_manager, risk_manager) -> None:
        if self._runtime_stop_requested.is_set():
            logger.info("Protected entry aborted before submission during runtime stop asset=%s venue=%s instrument=%s", self._asset_id, decision.venue, decision.instrument)
            return
        sizing = decision.sizing
        protection = decision.protection_plan
        if sizing is None or protection is None:
            return
        side = "BUY" if decision.direction in {Direction.LONG, Direction.BULLISH} else "SELL"
        if decision.venue == "groww":
            side = "BUY"
        execution_manager = self._execution_manager_for(order_manager, decision.venue)
        set_leverage = getattr(execution_manager, "set_leverage", None)
        if decision.venue != "groww" and sizing.leverage_selected and callable(set_leverage):
            try:
                lev = max(1, int(round(float(sizing.leverage_selected))))
                lev_res = set_leverage(lev)
                ok = isinstance(lev_res, Mapping) and bool(lev_res.get("success", True)) and not lev_res.get("_error")
                if not ok:
                    try:
                        execution_manager.last_order_error = {
                            "stage": "set_leverage",
                            "status_code": 0,
                            "reason": str(lev_res)[:300],
                            "raw": lev_res,
                        }
                    except Exception:
                        pass
                    self._notify_order_error(decision, execution_manager)
                    logger.error("%s leverage set failed before protected entry: %s", decision.venue.upper(), lev_res)
                    return
            except Exception as exc:
                try:
                    execution_manager.last_order_error = {
                        "stage": "set_leverage",
                        "status_code": 0,
                        "reason": str(exc),
                        "raw": {"error": str(exc)},
                    }
                except Exception:
                    pass
                self._notify_order_error(decision, execution_manager)
                logger.error("%s leverage set exception before protected entry: %s", decision.venue.upper(), exc, exc_info=True)
                return
        if self._runtime_stop_requested.is_set():
            logger.info("Protected entry aborted after preflight during runtime stop asset=%s venue=%s instrument=%s", self._asset_id, decision.venue, decision.instrument)
            return
        result = execution_manager.place_bracket_limit_entry(
            side,
            sizing.quantity,
            protection.entry_price,
            protection.stop_price,
            protection.target_price,
        )
        if not result:
            err = getattr(execution_manager, "last_order_error", None) or {}
            reason = str(err.get("reason") or "")
            if "hyperliquid_entry_cancel_reconciliation_unresolved" in reason:
                # A timed-out entry with unverified cancellation is not FLAT. Lock
                # this asset from new entry decisions until broker state is proven.
                with self._entry_lock:
                    self._pos.phase = PositionPhase.RECONCILIATION_REQUIRED
                    self._pos.manual_exit_reason = "entry_cancel_reconciliation_unresolved"
                self._ensure_position_reconciliation_worker(execution_manager, risk_manager)
                logger.critical(
                    "Hyperliquid entry state unresolved after cancel reconciliation; asset=%s locked in RECONCILIATION_REQUIRED until broker state is verified",
                    self._asset_id,
                )
            self._notify_order_error(decision, execution_manager)
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
        for method_name, arg in (("notify_entry_placed", None), ("set_position_open", True)):
            method = getattr(risk_manager, method_name, None)
            if callable(method):
                try:
                    method() if arg is None else method(arg)
                except Exception:
                    pass
        record_exposure = getattr(risk_manager, "record_open_exposure", None)
        if callable(record_exposure):
            signed_delta = sizing.notional if side == "BUY" else -sizing.notional
            record_exposure(asset_id=self._asset_id, position_key=f"{decision.desk}:{decision.instrument}", signed_delta_usd=signed_delta)
        quantity_filled = float(result.get("quantity") or sizing.quantity)
        self._pos = PositionState(
            phase=PositionPhase.ACTIVE,
            side="long" if side == "BUY" else "short",
            quantity=quantity_filled,
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
            quantity_unit=self._quantity_unit_label(decision.venue),
            entry_leverage=float(sizing.leverage_selected or 1.0),
            entry_fee_paid=float(result.get("paid_commission", 0.0) or 0.0),
            entry_fee_exact=bool(result.get("paid_commission_exact", False)),
            protection_confirmed=bool(result.get("bracket_child_verified") or result.get("protection_confirmed")),
            protection_model=str(result.get("protection_model") or protection.protection_type),
            quant_components=decision.model_values,
        )
        self._ensure_position_reconciliation_worker(execution_manager, risk_manager)
        self._notify_entry_opened(decision, sizing, protection, result, fill_price, quantity_filled)

    def _monitor_position(self, data_manager, order_manager, risk_manager) -> None:
        price = self._safe_price(data_manager)
        try:
            state = self._venue_states(data_manager).get(str(self._pos.exchange).lower())
            if state is not None and float(state.mid or 0.0) > 0:
                price = float(state.mid)
        except Exception:
            pass
        if price <= 0:
            return
        order_manager = self._execution_manager_for(order_manager, self._pos.exchange)
        self._ensure_position_reconciliation_worker(order_manager, risk_manager)
        self._forward_labels.observe(now_ts_ns=time.time_ns(), current_price=price)
        self._dynamic_exit_supervision(data_manager, order_manager)
        if self._pos.side.lower() == "long":
            target_hit = price >= self._pos.tp_price > 0
            stop_hit = price <= self._pos.sl_price if self._pos.sl_price > 0 else False
        else:
            target_hit = price <= self._pos.tp_price if self._pos.tp_price > 0 else False
            stop_hit = price >= self._pos.sl_price > 0
        if (target_hit or stop_hit) and self._pos.phase is PositionPhase.ACTIVE:
            self._pos.phase = PositionPhase.EXITING
            self._pos.manual_exit_reason = "target_reached" if target_hit else "stop_reached"
            self._notify_exit_level_hit("tp_hit" if target_hit else "sl_hit", price)
            # Venue-native TP/SL is already armed; immediately request broker
            # reconciliation without issuing network I/O on this market tick.
            self._position_reconcile_wakeup.set()

    def _quantity_unit_label(self, venue: str) -> str:
        try:
            ei = self._exchange_instrument(venue)
            base = str(getattr(ei, "base_asset", "") or "").upper()
            if base:
                return base
        except Exception:
            pass
        if str(venue or "").lower() == "groww":
            return "NFO_UNITS"
        return str(self._asset_id or "units").upper()

    def _notify_order_error(self, decision: OpportunityDecision, order_manager) -> None:
        try:
            from telegram.notifier import send_telegram_message
            err = getattr(order_manager, "last_order_error", None) or {}
            sizing = decision.sizing
            protection = decision.protection_plan
            lines = [
                "<b>ORDER ERROR</b>",
                f"<code>{decision.venue.upper()}:{decision.instrument}</code>",
                f"<code>stage={err.get('stage', 'order_submission')} status={err.get('status_code', '-')}</code>",
                f"<code>reason={err.get('reason', 'unknown')}</code>",
            ]
            if sizing is not None:
                lines.append(
                    f"<code>qty={sizing.quantity:.8g} notional={sizing.notional:.4f} "
                    f"margin={sizing.margin_required:.4f} risk={sizing.risk_to_invalidation:.4f}</code>"
                )
            if protection is not None:
                lines.append(
                    f"<code>entry={protection.entry_price:.4f} sl={protection.stop_price:.4f} "
                    f"tp={protection.target_price:.4f}</code>"
                )
            send_telegram_message("\n".join(lines), instrument=self._instrument, event_type="ORDER ERROR")
        except Exception:
            pass

    def _notify_entry_opened(
        self,
        decision: OpportunityDecision,
        sizing: PositionSizingDecision,
        protection: ProtectionPlan,
        result: Mapping[str, Any],
        fill_price: float,
        quantity_filled: float,
    ) -> None:
        try:
            from telegram.notifier import format_entry_alert, send_telegram_message
            risk = abs(quantity_filled * (fill_price - protection.stop_price))
            rr = abs(protection.target_price - fill_price) / max(abs(fill_price - protection.stop_price), 1e-12)
            msg = format_entry_alert(
                side="long" if decision.direction in {Direction.LONG, Direction.BULLISH} else "short",
                entry=fill_price,
                sl=protection.stop_price,
                tp=protection.target_price,
                qty=quantity_filled,
                leverage=float(sizing.leverage_selected or 1.0),
                rr=rr,
                risk_usd=risk,
                margin_used=float(sizing.margin_required or 0.0),
                fee_status=(
                    f"broker exact {self._pos.currency_symbol}{float(result.get('paid_commission', 0.0) or 0.0):.4f}"
                    if bool(result.get("paid_commission_exact", False)) else "broker fee pending"
                ),
                decision_path=decision.decision.value,
                instrument=self._instrument,
            )
            send_telegram_message(
                msg,
                instrument=self._instrument,
                event_type="ENTRY FILL",
                context={"entry_leverage": sizing.leverage_selected, "price": fill_price, "state": self._pos.phase.value},
            )
        except Exception:
            pass

    def _estimated_gross_pnl(self, exit_price: float) -> float:
        return gross_pnl_usd(
            side=self._pos.side,
            entry_price=self._pos.entry_price,
            exit_price=exit_price,
            quantity_btc=self._pos.quantity,
            inverse=str(self._pos.pnl_model or "").lower() == "inverse_btcusd",
        )

    def _notify_exit_level_hit(self, reason: str, price: float) -> None:
        components = self._pos.quant_components if isinstance(self._pos.quant_components, dict) else {}
        key = f"exit_level_notified:{reason}"
        if components.get(key):
            return
        components[key] = True
        try:
            from telegram.notifier import format_exit_alert, send_telegram_message
            gross = self._estimated_gross_pnl(price)
            msg = format_exit_alert(
                side=self._pos.side,
                entry_price=self._pos.entry_price,
                exit_price=price,
                pnl=gross - float(self._pos.entry_fee_paid or 0.0),
                reason=f"{reason}_pending_broker_confirmation",
                venue=self._pos.exchange,
                qty=self._pos.quantity,
                gross=gross,
                fees=float(self._pos.entry_fee_paid or 0.0),
                exact_fees=False,
                pnl_provisional=True,
            )
            send_telegram_message(msg, instrument=self._instrument, event_type=reason.upper(), context={"price": price, "state": self._pos.phase.value})
        except Exception:
            pass

    def _notify_exit_reconciliation_required(self, price: float) -> None:
        components = self._pos.quant_components if isinstance(self._pos.quant_components, dict) else {}
        if components.get("exit_reconciliation_required_notified"):
            return
        components["exit_reconciliation_required_notified"] = True
        try:
            from telegram.notifier import send_telegram_message
            send_telegram_message(
                "\n".join([
                    "<b>EXIT UNCONFIRMED</b>",
                    f"<code>{self._pos.exchange.upper()}:{self._pos.execution_symbol}</code>",
                    "<code>broker flat but tracked exit fill (SL/TP/dynamic close) could not be confirmed</code>",
                    f"<code>mark={price:.4f} entry={self._pos.entry_price:.4f} qty={self._pos.quantity:.8g}</code>",
                ]),
                instrument=self._instrument,
                event_type="EXIT RECONCILIATION",
                context={"price": price, "state": PositionPhase.RECONCILIATION_REQUIRED.value},
            )
        except Exception:
            pass

    def _finalise_confirmed_exit(self, order_manager, risk_manager, mark_price: float) -> bool:
        identifier = getattr(order_manager, "identify_exit_order", None)
        if not callable(identifier):
            return False
        try:
            details = identifier(
                self._pos.sl_order_id, self._pos.tp_order_id,
                dynamic_exit_order_id=self._pos.dynamic_exit_order_id,
            )
        except TypeError:
            # Compatibility for test/simulation managers that pre-date tracked
            # model-directed close identifiers. Production OrderManager accepts it.
            details = identifier(self._pos.sl_order_id, self._pos.tp_order_id)
        if not isinstance(details, Mapping) or not bool(details.get("confirmed", False)):
            return False
        exit_price = _num(details.get("fill_price"), 0.0)
        if exit_price <= 0:
            return False
        exit_type = str(details.get("exit_type") or self._pos.manual_exit_reason or "exit")
        gross = self._estimated_gross_pnl(exit_price)
        entry_fee = float(self._pos.entry_fee_paid or 0.0)
        exit_fee = float(details.get("fee_paid", 0.0) or 0.0)
        fees_exact = bool(self._pos.entry_fee_exact and details.get("fee_exact", False))
        known_fees = entry_fee + exit_fee
        if fees_exact:
            fees = known_fees
        else:
            fee_rate = max(0.0, float(_cfg("COMMISSION_RATE", 0.00055) or 0.0))
            conservative_fee_estimate = (self._pos.entry_price + exit_price) * self._pos.quantity * fee_rate
            fees = max(known_fees, conservative_fee_estimate)
        net = gross - fees
        # Only after a confirmed close fill and broker-flat state may residual
        # bracket conditionals be swept. During submission/fill races they remain
        # live protection and must never be cancelled pre-emptively.
        if bool(_cfg("DYNAMIC_EXIT_CANCEL_RESIDUAL_PROTECTION_AFTER_CONFIRMED_FLAT", True)):
            sweeper = getattr(order_manager, "cancel_symbol_conditionals", None)
            if callable(sweeper):
                try:
                    sweeper(self._pos.execution_symbol)
                except Exception as exc:
                    logger.warning("Residual protection sweep deferred after confirmed flat asset=%s: %s", self._asset_id, exc)
        try:
            recorder = getattr(risk_manager, "record_trade", None)
            if callable(recorder):
                recorder(
                    side=self._pos.side,
                    entry_price=self._pos.entry_price,
                    exit_price=exit_price,
                    quantity=self._pos.quantity,
                    reason=exit_type,
                    pnl_override=net,
                    entry_leverage=self._pos.entry_leverage,
                    pnl_model=self._pos.pnl_model,
                    currency_code=self._pos.currency_code,
                    quantity_unit=self._pos.quantity_unit,
                )
            state_setter = getattr(risk_manager, "set_position_open", None)
            if callable(state_setter):
                state_setter(False)
        except Exception:
            pass
        self._risk_gate.record_trade_result(net)
        self._trade_history.append({
            "ts": time.time(),
            "side": self._pos.side,
            "entry": self._pos.entry_price,
            "exit": exit_price,
            "qty": self._pos.quantity,
            "pnl": net,
            "reason": exit_type,
        })
        try:
            from telegram.notifier import format_exit_alert, send_telegram_message
            msg = format_exit_alert(
                side=self._pos.side,
                entry_price=self._pos.entry_price,
                exit_price=exit_price,
                pnl=net,
                reason=exit_type,
                venue=self._pos.exchange,
                qty=self._pos.quantity,
                residual_qty=0.0,
                partial_qty=self._pos.quantity,
                gross=gross,
                fees=fees,
                margin_used=(self._pos.entry_price * self._pos.quantity) / max(self._pos.entry_leverage, 1.0),
                exact_fees=fees_exact,
                pnl_provisional=not fees_exact,
                fee_source="entry exact / exit exact" if fees_exact else "broker fill exact / conservative fee estimate",
            )
            send_telegram_message(msg, instrument=self._instrument, event_type="EXIT FILL", context={"price": mark_price, "state": "FLAT"})
        except Exception:
            pass
        return True

    def _dynamic_exit_live_state(self, data_manager) -> dict[str, Any]:
        """Measure whether the live, executable thesis still supports an open position.

        This is deliberately a read-only market-state calculation. It consumes the
        already-normalised in-memory venue microstates and candle caches used for entry
        decisions; it does not call broker position/order endpoints and cannot issue an
        order on the quote-processing thread.
        """
        venue = str(self._pos.exchange or "").lower()
        instrument = str(self._pos.execution_symbol or self._asset_id)
        states = self._venue_states(data_manager)
        if venue not in states:
            return {"ready": False, "reason": "selected_venue_live_microstate_unavailable", "venue": venue}
        feed_health = self._feed_health(data_manager)
        execution_state, btc_composite = self._microstructure_context(
            data_manager, venue, instrument, feed_health, states=states,
        )
        if execution_state is None or not execution_state.usable_for_decision:
            return {"ready": False, "reason": "selected_venue_execution_microstate_unhealthy", "venue": venue}
        market_states: dict[str, VenueMarketState] = {}
        if bool(_cfg("INSTITUTIONAL_ENABLE_MARKET_STATE_ALPHA", True)):
            market_states = self._market_state_engine.build(data_manager, states)
        cross_venue_evidence = (
            build_continuous_cross_venue_evidence(self._asset_id, market_states, states)
            if self._asset_id.upper() == "BTC" and market_states else None
        )
        composite_decision = (
            self._composite_bus.build_decision(asset_id=self._asset_id, market_states=market_states, microstates=states)
            if bool(_cfg("INSTITUTIONAL_COMPOSITE_INTELLIGENCE_ENABLED", True)) and market_states else None
        )
        mark = float(execution_state.mid or 0.0) or self._safe_price(data_manager)
        if mark <= 0:
            return {"ready": False, "reason": "selected_venue_mark_unavailable", "venue": venue}
        liquidity_score, _ = self._liquidity_score(data_manager, instrument, mark, execution_state=execution_state)
        direction, edge_bps, direction_reason, breakdown = self._direction_and_edge(
            self._desk_id(venue, instrument), mark, liquidity_score,
            execution_state=execution_state, btc_composite=btc_composite,
            market_state=market_states.get(venue), cross_venue_evidence=cross_venue_evidence,
            composite_decision=composite_decision,
        )
        position_direction = Direction.LONG if str(self._pos.side).lower() == "long" else Direction.SHORT
        aligned = direction is position_direction
        opposed = direction not in (Direction.NO_TRADE,) and direction is not position_direction
        live_cost = float(self._execution_cost_components(data_manager, execution_state=execution_state).get("total_cost_bps", 0.0))
        recorded_costs = self._pos.quant_components.get("execution_cost_components", {}) if isinstance(self._pos.quant_components, dict) else {}
        recorded_exit_proxy = _num(recorded_costs.get("total_cost_bps"), 0.0) if isinstance(recorded_costs, Mapping) else 0.0
        # Use the more conservative of the live touch estimate and the recorded
        # selected-route cost proxy; an early exit must earn its incremental cost.
        unwind_cost_bps = max(live_cost, recorded_exit_proxy)
        regime = self._market_state_regime(market_states.get(venue), self._regime())
        uncertainty_bps = self._uncertainty_bps(regime, liquidity_score, float(execution_state.feed_quality_score or 0.0))
        uncertainty_bps += max(0.0, _num(breakdown.get("cross_venue_uncertainty_bps"), 0.0))
        side_sign = 1.0 if position_direction is Direction.LONG else -1.0
        mark_move_bps = side_sign * ((mark / max(self._pos.entry_price, 1e-9)) - 1.0) * 10_000.0
        retained_net_edge_bps = float(edge_bps) - unwind_cost_bps - uncertainty_bps if aligned else -unwind_cost_bps - uncertainty_bps
        opposing_net_edge_bps = float(edge_bps) - unwind_cost_bps - uncertainty_bps if opposed else 0.0
        selected_parent = market_states.get(venue)
        parent_structural_alpha_bps = (
            float(composite_decision.transferable_structural_alpha_bps)
            if composite_decision is not None and composite_decision.ready
            else float(selected_parent.signed_alpha_bps) if selected_parent is not None and selected_parent.ready else 0.0
        )
        parent_structural_uncertainty_bps = (
            float(composite_decision.diagnostics.get("execution_uncertainty_bps", 0.0) or 0.0)
            if composite_decision is not None and composite_decision.ready
            else float(selected_parent.uncertainty_bps) if selected_parent is not None and selected_parent.ready else 0.0
        )
        parent_sign = 1 if parent_structural_alpha_bps > 0 else -1 if parent_structural_alpha_bps < 0 else 0
        position_sign = 1 if position_direction is Direction.LONG else -1
        parent_opposed = bool(parent_sign and parent_sign == -position_sign and abs(parent_structural_alpha_bps) > parent_structural_uncertainty_bps)
        parent_opposing_net_edge_bps = (
            abs(parent_structural_alpha_bps) - unwind_cost_bps - parent_structural_uncertainty_bps
            if parent_opposed else 0.0
        )
        parent_state_id = ""
        if selected_parent is not None and isinstance(selected_parent.diagnostics, Mapping):
            parent_state_id = str(selected_parent.diagnostics.get("parent_state_id") or "")
        return {
            "ready": True,
            "reason": direction_reason,
            "venue": venue,
            "mark": mark,
            "direction": direction.value,
            "position_direction": position_direction.value,
            "aligned": bool(aligned),
            "opposed": bool(opposed),
            "gross_edge_bps": float(edge_bps),
            "retained_net_edge_bps": retained_net_edge_bps,
            "opposing_net_edge_bps": opposing_net_edge_bps,
            "parent_state_id": parent_state_id,
            "parent_structural_alpha_bps": parent_structural_alpha_bps,
            "parent_structural_uncertainty_bps": parent_structural_uncertainty_bps,
            "parent_structure_opposed": parent_opposed,
            "parent_opposing_net_edge_bps": parent_opposing_net_edge_bps,
            "microstructure_only_invalidation_disabled": True,
            "mark_move_bps": mark_move_bps,
            "mark_after_unwind_cost_bps": mark_move_bps - unwind_cost_bps,
            "unwind_cost_bps": unwind_cost_bps,
            "uncertainty_bps": uncertainty_bps,
        }

    def _dynamic_exit_supervision(self, data_manager, order_manager) -> None:
        """Supervise a live position against its parent structural thesis.

        Directional underlying positions cannot be closed by elapsed micro-alpha
        horizon, queue-flow reversal, or repeated evaluation of one intrabar state.
        Native SL/TP owns immediate downside protection. A discretionary reduce-only
        exit is permitted only when distinct CLOSED parent-state observations
        establish an executable opposing structural thesis.
        """
        if self._pos.is_flat() or self._pos.phase is not PositionPhase.ACTIVE:
            return
        components = self._pos.quant_components if isinstance(self._pos.quant_components, dict) else {}
        plan = components.get("dynamic_protection_plan") or {}
        elapsed = max(0.0, time.time() - float(self._pos.entry_time or time.time()))
        actionable_reasons: list[str] = []
        decay = plan.get("signal_decay") if isinstance(plan, Mapping) else {}
        optimal_hold = _num((decay or {}).get("optimal_hold_sec"), 0.0) if isinstance(decay, Mapping) else 0.0
        confirmation_state = components.setdefault("dynamic_exit_validation", {})
        live_state: dict[str, Any] = {}

        parent_exit_assets_raw = _cfg(
            "DYNAMIC_EXIT_PARENT_STRUCTURE_ASSETS",
            ("BTC", "GOLD_PAXG", "GOLD_HL", "SILVER_SLVON", "SILVER_XAG", "SILVER_HL", "OIL"),
        )
        parent_exit_assets = {
            str(x).upper() for x in (
                parent_exit_assets_raw
                if isinstance(parent_exit_assets_raw, (tuple, list, set))
                else str(parent_exit_assets_raw).split(",")
            )
        }
        parent_structure_only = bool(_cfg("DYNAMIC_EXIT_PARENT_STRUCTURE_ONLY", True)) and self._asset_id.upper() in parent_exit_assets
        if parent_structure_only:
            if not confirmation_state.get("structural_monitor_logged"):
                confirmation_state["structural_monitor_logged"] = True
                logger.info(
                    "🧮 DYNAMIC_EXIT_STRUCTURAL_MONITOR_ACTIVE %s",
                    json.dumps({
                        "asset": self._pos.asset_id, "venue": self._pos.exchange,
                        "symbol": self._pos.execution_symbol, "side": self._pos.side,
                        "elapsed_sec": round(elapsed, 3),
                        "model_horizon_sec_telemetry_only": round(optimal_hold, 3) if optimal_hold > 0 else None,
                        "microstructure_only_liquidation_disabled": True,
                        "clock_only_liquidation_disabled": True,
                        "native_protection_authority": "venue_attached_sl_tp",
                    }, sort_keys=True, separators=(",", ":"), default=str),
                )
            live_state = self._dynamic_exit_live_state(data_manager)
            confirmation_state["latest_live_state"] = live_state
            if bool(live_state.get("ready", False)):
                evidence_id = str(live_state.get("parent_state_id") or "")
                parent_invalidated = bool(live_state.get("parent_structure_opposed")) and _num(live_state.get("parent_opposing_net_edge_bps"), 0.0) > 0.0
                observations = confirmation_state.setdefault("distinct_parent_observations", [])
                if parent_invalidated and evidence_id:
                    if not observations or str(observations[-1].get("parent_state_id")) != evidence_id:
                        observations.append({"ts": time.time(), "parent_state_id": evidence_id, "live": live_state})
                        del observations[:-max(2, int(_cfg("DYNAMIC_EXIT_MIN_DISTINCT_PARENT_OBSERVATIONS", 2)))]
                elif evidence_id and (not parent_invalidated):
                    # A fresh parent state still supporting, or not invalidating,
                    # the position invalidates any previous reversal sequence.
                    if not observations or str(observations[-1].get("parent_state_id")) != evidence_id:
                        observations.clear()
                required = max(2, int(_cfg("DYNAMIC_EXIT_MIN_DISTINCT_PARENT_OBSERVATIONS", 2)))
                confirmation_state["required_distinct_parent_observations"] = required
                confirmation_state["observed_distinct_parent_invalidations"] = len(observations)
                if len(observations) >= required:
                    actionable_reasons.append("confirmed_parent_structural_invalidation")
            elif not confirmation_state.get("parent_state_unavailable_logged"):
                confirmation_state["parent_state_unavailable_logged"] = True
                logger.warning(
                    "DYNAMIC_EXIT_STRUCTURAL_MONITOR_PENDING asset=%s venue=%s reason=%s; native hard protection remains active",
                    self._pos.asset_id, self._pos.exchange, str(live_state.get("reason") or "parent_state_not_ready"),
                )
        else:
            # Non-directional desks (currently options) keep their specialised
            # Greek/volatility lifecycle. Microstructure-only invalidation is never
            # reused by any underlying directional desk.
            live_state = {}

        option_diag: dict[str, Any] = {}
        if str(self._pos.exchange or "").lower() == "groww":
            getter = getattr(data_manager, "get_active_option_risk_state", None)
            if callable(getter):
                try:
                    live = dict(getter() or {})
                    entry_state = plan.get("option_state_at_entry", {}) if isinstance(plan, Mapping) else {}
                    vol_ctx = entry_state.get("volatility_context", {}) if isinstance(entry_state, Mapping) else {}
                    current_delta = live.get("current_delta")
                    abs_delta = abs(float(current_delta)) if current_delta is not None and math.isfinite(float(current_delta)) else None
                    diag = self._protection_engine.option_exit_diagnostics(
                        abs_delta=abs_delta, current_iv=live.get("current_iv"),
                        entry_iv=entry_state.get("iv") if isinstance(entry_state, Mapping) else None,
                        theta_to_premium_per_day=live.get("current_theta_to_premium"),
                        dte=live.get("dte"), vrp=vol_ctx.get("vrp") if isinstance(vol_ctx, Mapping) else None,
                    )
                    option_diag = asdict(diag)
                    components["dynamic_option_exit_state"] = option_diag
                    actionable_reasons.extend(list(diag.reasons) if diag.exit_required else [])
                except Exception as exc:
                    option_diag = {"ready": False, "reason": f"option_exit_diagnostics_error:{exc}"}
                    components["dynamic_option_exit_state"] = option_diag

        if not actionable_reasons:
            return
        deduped_reasons = sorted(set(actionable_reasons))
        key = "dynamic_exit_alerted:" + "|".join(deduped_reasons)
        if components.get(key):
            return
        components[key] = True
        auto_enabled = bool(_cfg("DYNAMIC_EXIT_AUTOMATED_EARLY_LIQUIDATION_ENABLED", False))
        payload = {
            "asset": self._pos.asset_id, "venue": self._pos.exchange,
            "symbol": self._pos.execution_symbol, "side": self._pos.side,
            "elapsed_sec": round(elapsed, 3),
            "model_horizon_sec": round(optimal_hold, 3) if optimal_hold > 0 else None,
            "reasons": deduped_reasons, "live_confirmation": live_state,
            "option_exit": option_diag, "automated_early_liquidation_enabled": auto_enabled,
            "hard_protection_remains_active": True,
        }
        logger.warning("🧮 DYNAMIC_EXIT_SIGNAL %s", json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str))
        if auto_enabled:
            with self._entry_lock:
                if not self._pos.is_flat() and self._pos.phase is PositionPhase.ACTIVE:
                    self._pos.dynamic_exit_requested = True
                    self._pos.dynamic_exit_reasons = tuple(deduped_reasons)
                    self._pos.dynamic_exit_requested_at = time.time()
                    self._pos.manual_exit_reason = "dynamic_exit:" + ",".join(deduped_reasons)
            self._position_reconcile_wakeup.set()
            logger.warning(
                "DYNAMIC_EXIT_ENQUEUED asset=%s venue=%s reasons=%s protection_retained=true",
                self._pos.asset_id, self._pos.exchange, ",".join(deduped_reasons),
            )

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
        self, data_manager, venue: str, instrument: str, feed_health: FeedHealth, states: dict[str, VenueMicrostate] | None = None
    ) -> tuple[VenueMicrostate | None, BTCCompositeState | None]:
        states = dict(states or {})
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

    def _venue_states(self, data_manager) -> dict[str, VenueMicrostate]:
        states: dict[str, VenueMicrostate] = {}
        getter = getattr(data_manager, "get_venue_microstates", None)
        if callable(getter):
            try:
                states = {str(k).lower(): v for k, v in dict(getter() or {}).items() if isinstance(v, VenueMicrostate)}
            except Exception:
                states = {}
        return states

    def _best_originating_venue_signal(
        self, data_manager, states: dict[str, VenueMicrostate], btc_composite: BTCCompositeState | None,
        fallback_instrument: str, market_states: dict[str, VenueMarketState] | None = None,
        cross_venue_evidence: CrossVenueEvidence | None = None,
        composite_decision: CompositeAssetDecision | None = None,
    ) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
        """Choose alpha origin from separately-priced, venue-local candidates.

        The selected origin is never dictated by configured primary order.  A
        broker with a live structural move can lead; peer disagreement enters
        confidence and uncertainty rather than acting as a retail-style veto.
        """
        candidates: list[dict[str, Any]] = []
        best: dict[str, Any] | None = None
        market_states = market_states or {}
        for candidate_venue, state in states.items():
            if not isinstance(state, VenueMicrostate) or not state.usable_for_decision or float(state.mid or 0.0) <= 0.0:
                continue
            candidate_instrument = self._symbol_for_venue(candidate_venue, fallback_instrument)
            candidate_desk = self._desk_id(candidate_venue, candidate_instrument)
            candidate_price = float(state.mid)
            liq, zones = self._liquidity_score(data_manager, candidate_instrument, candidate_price, execution_state=state)
            market_state = market_states.get(str(candidate_venue).lower())
            direction, edge, reason, breakdown = self._direction_and_edge(
                candidate_desk, candidate_price, liq, execution_state=state, btc_composite=btc_composite,
                market_state=market_state, cross_venue_evidence=cross_venue_evidence,
                composite_decision=composite_decision,
            )
            local_cost = float(self._execution_cost_components(data_manager, execution_state=state).get("total_cost_bps", 0.0))
            screened_net = float(edge) - local_cost - max(0.0, _num(breakdown.get("market_state_uncertainty_bps"), 0.0)) - max(0.0, _num(breakdown.get("cross_venue_uncertainty_bps"), 0.0))
            public = {
                "venue": candidate_venue, "instrument": candidate_instrument,
                "direction": direction.value, "edge_bps": float(edge),
                "local_cost_bps": local_cost, "screened_net_edge_bps": screened_net,
                "reason": reason, "usable": True,
                "market_state": market_state.as_dict() if market_state is not None else {},
            }
            candidates.append(public)
            if direction is Direction.NO_TRADE:
                continue
            candidate = {
                **public, "desk": candidate_desk, "price": candidate_price, "state": state,
                "liquidity_score": liq, "zones": zones,
                "execution_quality": float(state.feed_quality_score or 0.0),
                "direction": direction, "breakdown": breakdown,
            }
            if best is None or screened_net > float(best["screened_net_edge_bps"]):
                best = candidate
        return best, candidates

    def _direction_and_edge(
        self, desk: str, price: float, liquidity_score: float, *, execution_state: VenueMicrostate | None,
        btc_composite: BTCCompositeState | None, market_state: VenueMarketState | None = None,
        cross_venue_evidence: CrossVenueEvidence | None = None,
        composite_decision: CompositeAssetDecision | None = None,
    ) -> tuple[Direction, float, str, dict[str, Any]]:
        if price <= 0 or execution_state is None:
            return Direction.NO_TRADE, 0.0, "microstructure_state_unavailable", {}
        if not execution_state.usable_for_decision:
            return Direction.NO_TRADE, 0.0, "execution_microstate_unhealthy", {}
        # Execution quality is strictly venue-local. Delta may contribute BTC
        # contextual evidence, but never scale a Hyperliquid/CoinSwitch order.
        execution_quality = float(execution_state.feed_quality_score or 0.0)
        venue_key = str(execution_state.venue or "").lower()
        breakdown: dict[str, Any] = {
            "edge_calculation_formula": "abs(venue_timing_alpha + normalised_execution_equivalence_composite_alpha) * venue_feed_quality * liquidity * contextual_factor_confidence",
            "venue_local_execution_quality_multiplier": execution_quality,
            "liquidity_score_multiplier": liquidity_score,
            "signal_venue": venue_key,
        }
        if execution_quality < float(_cfg("INSTITUTIONAL_MIN_EXECUTION_QUALITY", 0.40)):
            return Direction.NO_TRADE, 0.0, f"execution_quality_low:{execution_quality:.3f}", breakdown
        near_depth = sum(float(execution_state.bid_depth_usd_by_band.get(k, 0.0) + execution_state.ask_depth_usd_by_band.get(k, 0.0)) for k in ("0-1", "1-3"))
        breakdown["near_touch_depth_usd"] = near_depth
        if near_depth <= 0:
            return Direction.NO_TRADE, 0.0, "near_touch_depth_unavailable", breakdown
        ofi_norm = (execution_state.ofi_usd_1s + 0.50 * execution_state.ofi_usd_10s) / near_depth
        tfi_norm = (execution_state.tfi_usd_1s + 0.50 * execution_state.tfi_usd_10s) / near_depth
        micro_deviation_bps = (execution_state.microprice / max(execution_state.mid, 1e-9) - 1.0) * 10_000.0
        ofi_component_bps = float(_cfg("INSTITUTIONAL_FLOW_OFI_WEIGHT", 1.0)) * ofi_norm * 100.0
        tfi_component_bps = float(_cfg("INSTITUTIONAL_FLOW_TFI_WEIGHT", 0.30)) * tfi_norm * 100.0
        microprice_component_bps = float(_cfg("INSTITUTIONAL_FLOW_MICROPRICE_WEIGHT", 0.35)) * micro_deviation_bps
        # Relative-value/dislocation alpha remains disabled without a separately
        # validated fungibility/basis model; it is telemetry only.
        dislocation_diagnostic_bps = float(btc_composite.delta_dislocation_bps or 0.0) if btc_composite and venue_key == "delta" else 0.0
        raw_micro_signal_bps = ofi_component_bps + tfi_component_bps + microprice_component_bps
        caps = _cfg("INSTITUTIONAL_MICROSTRUCTURE_ALPHA_CAP_BPS", {})
        cap = float(caps.get(self._asset_id.upper(), 24.0) if isinstance(caps, Mapping) else 24.0)
        robust_micro_signal_bps = cap * math.tanh(raw_micro_signal_bps / max(cap, 1e-9))
        structural_alpha_bps = 0.0
        market_uncertainty_bps = 0.0
        if composite_decision is not None and composite_decision.ready:
            # The collective thesis aggregates both confirmed structural motion
            # and USD-normalised flow from execution-equivalent venues. The local
            # book below controls executability/timing; it is not added twice as alpha.
            structural_alpha_bps = float(composite_decision.transferable_structural_alpha_bps)
            market_uncertainty_bps = float(composite_decision.diagnostics.get("execution_uncertainty_bps", 0.0) or 0.0)
            breakdown["composite_asset_intelligence"] = composite_decision.as_dict()
            breakdown["structural_alpha_authority"] = "normalised_execution_equivalence_composite"
        elif market_state is not None and market_state.ready and self._asset_id.upper() in set(_cfg("INSTITUTIONAL_MARKET_STATE_ASSETS", ("BTC", "GOLD_PAXG", "GOLD_HL", "SILVER_SLVON", "SILVER_XAG", "SILVER_HL", "OIL"))):
            structural_alpha_bps = float(market_state.signed_alpha_bps)
            market_uncertainty_bps = float(market_state.uncertainty_bps)
            breakdown["venue_market_state"] = market_state.as_dict()
            breakdown["structural_alpha_authority"] = "venue_local_fallback"
        # Parent/child alpha hierarchy for every directional underlying asset.
        # A live OFI/TFI burst is a child timing observation, not an investable
        # thesis. Only confirmed closed-bar structural alpha may originate or
        # reverse a position; options retain their dedicated underlying/Greek path.
        parent_assets_raw = _cfg(
            "INSTITUTIONAL_PARENT_THESIS_ASSETS",
            ("BTC", "GOLD_PAXG", "GOLD_HL", "SILVER_SLVON", "SILVER_XAG", "SILVER_HL", "OIL"),
        )
        parent_assets = {str(x).upper() for x in (parent_assets_raw if isinstance(parent_assets_raw, (tuple, list, set)) else str(parent_assets_raw).split(","))}
        parent_model_enabled = bool(_cfg("INSTITUTIONAL_PARENT_THESIS_EXECUTION_MODEL_ENABLED", True)) and self._asset_id.upper() in parent_assets
        if parent_model_enabled:
            parent_alpha_bps = float(structural_alpha_bps)
            timing_alpha_bps = (
                float(composite_decision.transferable_microstructure_alpha_bps)
                if composite_decision is not None and composite_decision.ready
                else float(robust_micro_signal_bps)
            )
            parent_sign = 1 if parent_alpha_bps > 0 else -1 if parent_alpha_bps < 0 else 0
            parent_uncertainty_bps = max(0.0, float(market_uncertainty_bps))
            if market_state is not None and market_state.ready:
                parent_uncertainty_bps = max(parent_uncertainty_bps, float(market_state.uncertainty_bps or 0.0))
            cap_fraction = max(0.0, min(0.95, float(_cfg("INSTITUTIONAL_PARENT_TIMING_CONTRIBUTION_CAP_FRACTION", 0.35))))
            timing_cap_bps = abs(parent_alpha_bps) * cap_fraction
            signed_timing_support_bps = parent_sign * timing_alpha_bps if parent_sign else 0.0
            bounded_support_bps = max(-timing_cap_bps, min(timing_cap_bps, signed_timing_support_bps))
            timing_contribution_bps = parent_sign * bounded_support_bps if parent_sign else 0.0
            combined_signal_bps = parent_alpha_bps + timing_contribution_bps
            parent_state_id = ""
            if market_state is not None and isinstance(market_state.diagnostics, Mapping):
                parent_state_id = str(market_state.diagnostics.get("parent_state_id") or "")
            breakdown.update({
                "signal_architecture": "parent_structural_thesis_child_execution_timing_v1",
                "parent_structural_alpha_bps": parent_alpha_bps,
                "parent_structural_uncertainty_bps": parent_uncertainty_bps,
                "parent_state_id": parent_state_id,
                "child_timing_alpha_bps": timing_alpha_bps,
                "child_timing_contribution_bps": timing_contribution_bps,
                "child_timing_cap_bps": timing_cap_bps,
                "microstructure_cannot_originate_or_flip_thesis": True,
                "raw_microstructure_signal_bps": raw_micro_signal_bps,
                "robust_microstructure_alpha_bps": robust_micro_signal_bps,
                "venue_local_market_state_alpha_bps": structural_alpha_bps,
                "collective_transferable_microstructure_alpha_bps": float(composite_decision.transferable_microstructure_alpha_bps) if composite_decision is not None and composite_decision.ready else 0.0,
                "collective_transferable_total_alpha_bps": combined_signal_bps,
                "weighted_signal_bps": combined_signal_bps,
                "ofi_component_bps": ofi_component_bps,
                "tfi_component_bps": tfi_component_bps,
                "microprice_component_bps": microprice_component_bps,
                "dislocation_diagnostic_bps": dislocation_diagnostic_bps,
                "dislocation_component_bps": 0.0,
                "market_state_uncertainty_bps": parent_uncertainty_bps,
            })
            if parent_sign == 0 or abs(parent_alpha_bps) <= parent_uncertainty_bps:
                breakdown["directional_edge_after_quality_liquidity_bps"] = 0.0
                return Direction.NO_TRADE, 0.0, "parent_structural_thesis_not_established", breakdown
            cross_confidence = 1.0
            cross_uncertainty_bps = 0.0
            if composite_decision is not None and composite_decision.ready:
                cross_confidence = composite_decision.directional_confidence_multiplier(parent_sign)
                cross_uncertainty_bps = composite_decision.contextual_uncertainty_for(parent_sign)
                breakdown["collective_factor_policy"] = {
                    "factor_id": composite_decision.factor_id,
                    "execution_equivalence_group": composite_decision.equivalence_group,
                    "factor_translation_enabled": composite_decision.basis_translation_enabled,
                    "related_products_are_context_only": not composite_decision.basis_translation_enabled,
                }
            elif desk == DeskId.BTC.value and cross_venue_evidence is not None:
                cross_confidence = cross_venue_evidence.confidence_for(venue_key, parent_sign)
                cross_uncertainty_bps = cross_venue_evidence.uncertainty_for(venue_key)
                breakdown["cross_venue_evidence"] = cross_venue_evidence.as_dict()
            breakdown["cross_venue_confidence_multiplier"] = cross_confidence
            breakdown["cross_venue_uncertainty_bps"] = cross_uncertainty_bps
            edge = abs(combined_signal_bps) * max(0.0, min(1.0, execution_quality)) * max(0.0, min(1.0, liquidity_score)) * cross_confidence
            breakdown["directional_edge_after_quality_liquidity_bps"] = edge
            threshold = float(_cfg("INSTITUTIONAL_MIN_SIGNAL_BPS", 0.50))
            if parent_sign > 0 and combined_signal_bps > threshold:
                return Direction.LONG, edge, "parent_structural_thesis_long_child_timing_validated", breakdown
            if parent_sign < 0 and combined_signal_bps < -threshold:
                return Direction.SHORT, edge, "parent_structural_thesis_short_child_timing_validated", breakdown
            return Direction.NO_TRADE, 0.0, "parent_structural_thesis_below_executable_strength", breakdown

        local_timing_confidence = 1.0
        local_timing_uncertainty_bps = 0.0
        if composite_decision is not None and composite_decision.ready:
            combined_signal_bps = float(composite_decision.transferable_total_alpha_bps)
            collective_sign = 1 if combined_signal_bps > 0 else -1 if combined_signal_bps < 0 else 0
            local_sign = 1 if robust_micro_signal_bps > 0 else -1 if robust_micro_signal_bps < 0 else 0
            if collective_sign and local_sign == collective_sign:
                local_timing_confidence = 1.04
            elif collective_sign and local_sign == -collective_sign:
                # A venue may execute a collective thesis only with an explicit
                # uncertainty charge when its immediate book/tape opposes it.
                local_timing_confidence = 0.84
                local_timing_uncertainty_bps = min(abs(robust_micro_signal_bps) * 0.50, 6.0)
            breakdown["local_execution_timing_alpha_bps"] = robust_micro_signal_bps
            breakdown["local_execution_timing_confidence_multiplier"] = local_timing_confidence
            breakdown["local_execution_timing_uncertainty_bps"] = local_timing_uncertainty_bps
        else:
            combined_signal_bps = robust_micro_signal_bps + structural_alpha_bps
        signal_sign = 1 if combined_signal_bps > 0 else -1 if combined_signal_bps < 0 else 0
        cross_confidence = 1.0
        cross_uncertainty_bps = 0.0
        if composite_decision is not None and composite_decision.ready and signal_sign != 0:
            cross_confidence = composite_decision.directional_confidence_multiplier(signal_sign) * local_timing_confidence
            cross_uncertainty_bps = composite_decision.contextual_uncertainty_for(signal_sign) + local_timing_uncertainty_bps
            breakdown["collective_factor_policy"] = {
                "factor_id": composite_decision.factor_id,
                "execution_equivalence_group": composite_decision.equivalence_group,
                "factor_translation_enabled": composite_decision.basis_translation_enabled,
                "related_products_are_context_only": not composite_decision.basis_translation_enabled,
            }
        elif desk == DeskId.BTC.value and cross_venue_evidence is not None and signal_sign != 0:
            cross_confidence = cross_venue_evidence.confidence_for(venue_key, signal_sign)
            cross_uncertainty_bps = cross_venue_evidence.uncertainty_for(venue_key)
            breakdown["cross_venue_evidence"] = cross_venue_evidence.as_dict()
        elif desk == DeskId.BTC.value:
            cross_confidence = 0.72
            cross_uncertainty_bps = float(_cfg("INSTITUTIONAL_CROSS_VENUE_UNCERTAINTY_MAX_BPS", 6.0)) * 0.5
        edge = abs(combined_signal_bps) * max(0.0, min(1.0, execution_quality)) * max(0.0, min(1.0, liquidity_score)) * cross_confidence
        breakdown.update({
            "ofi_component_bps": ofi_component_bps, "tfi_component_bps": tfi_component_bps,
            "microprice_component_bps": microprice_component_bps,
            "raw_microstructure_signal_bps": raw_micro_signal_bps,
            "robust_microstructure_alpha_bps": robust_micro_signal_bps,
            "venue_local_market_state_alpha_bps": structural_alpha_bps,
            "collective_transferable_microstructure_alpha_bps": float(composite_decision.transferable_microstructure_alpha_bps) if composite_decision is not None and composite_decision.ready else 0.0,
            "collective_transferable_total_alpha_bps": float(composite_decision.transferable_total_alpha_bps) if composite_decision is not None and composite_decision.ready else combined_signal_bps,
            "dislocation_diagnostic_bps": dislocation_diagnostic_bps,
            "dislocation_component_bps": 0.0,
            "cross_venue_confidence_multiplier": cross_confidence,
            "cross_venue_uncertainty_bps": cross_uncertainty_bps,
            "market_state_uncertainty_bps": market_uncertainty_bps,
            "weighted_signal_bps": combined_signal_bps,
            "directional_edge_after_quality_liquidity_bps": edge,
        })
        threshold = float(_cfg("INSTITUTIONAL_MIN_SIGNAL_BPS", 0.50))
        if combined_signal_bps > threshold:
            return Direction.LONG, edge, "market_state_flow_long", breakdown
        if combined_signal_bps < -threshold:
            return Direction.SHORT, edge, "market_state_flow_short", breakdown
        return Direction.NO_TRADE, 0.0, "market_state_and_flow_flat", breakdown

    @staticmethod
    def _market_state_regime(market_state: VenueMarketState | None, fallback: Regime) -> Regime:
        if market_state is None or not market_state.ready:
            return fallback
        mapping = {"EXPANSION": Regime.EXPANSION, "TREND": Regime.TREND, "BALANCE": Regime.BALANCE, "SHOCK": Regime.SHOCK}
        return mapping.get(str(market_state.regime_label).upper(), fallback)

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

    def _execution_cost_components(self, data_manager, execution_state: VenueMicrostate | None = None) -> dict[str, float]:
        spread = 2.0
        if execution_state is not None:
            spread = float(execution_state.spread_bps or spread)
        else:
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

    def _protection_plan(
        self, desk: str, direction: Direction, price: float, liquidity_score: float, *, data_manager,
        gross_edge_bps: float, costs_bps: float, position_notional: float = 0.0, quantity: float = 0.0,
        venue: str | None = None, instrument: str = "", execution_state: VenueMicrostate | None = None,
        regime: Regime | None = None, venue_market_state: VenueMarketState | None = None,
    ) -> ProtectionPlan | None:
        if price <= 0 or liquidity_score <= 0:
            return None
        asset_class = "crypto" if desk == DeskId.BTC.value else "commodity"
        mapping = self._instrument_mapping(instrument or self._asset_id, venue=venue)
        near_depth = 0.0
        spread_bps = 0.0
        if execution_state is not None:
            spread_bps = float(execution_state.spread_bps or 0.0)
            near_depth = sum(
                float(execution_state.bid_depth_usd_by_band.get(k, 0.0) + execution_state.ask_depth_usd_by_band.get(k, 0.0))
                for k in ("0-1", "1-3")
            )
        try:
            policy = active_policy(self._instrument)
        except Exception:
            policy = None
        executable_price_tick = float(mapping.price_tick or 0.0)
        if str(venue or "").lower() == "hyperliquid":
            executable_price_tick = max(executable_price_tick, _hyperliquid_price_increment(price, mapping.qty_step))
        market_state = {
            "asset_id": self._asset_id,
            "venue": str(venue or "").lower(),
            "instrument": instrument or self._asset_id,
            "spread_bps": spread_bps,
            "price_tick": executable_price_tick,
            "near_touch_depth_usd": near_depth,
            "liquidity_score": liquidity_score,
            "regime": getattr(regime, "value", regime) if regime is not None else "",
            "policy_min_rr": float(getattr(policy, "min_rr", 0.0) or 0.0),
            "policy_max_rr": float(getattr(policy, "max_rr", 0.0) or 0.0),
            "venue_market_state_ready": bool(venue_market_state is not None and venue_market_state.ready),
            "venue_local_robust_vol_bps": float(venue_market_state.robust_one_minute_vol_bps) if venue_market_state is not None and venue_market_state.ready else None,
        }
        if venue_market_state is not None and venue_market_state.ready:
            volatility_price = max(price * float(venue_market_state.robust_one_minute_vol_bps) / 10_000.0, executable_price_tick)
            market_state["volatility_source"] = f"venue_local_confirmed_candles:{str(venue or '').lower()}"
        else:
            volatility_price = self._realized_vol_price()
            market_state["volatility_source"] = "context_fallback_price_window"
        return self._protection_engine.build_plan(
            direction=direction, entry_price=price, volatility_price=volatility_price,
            gross_edge_bps=gross_edge_bps, execution_cost_bps=costs_bps,
            protection_type="VENUE_NATIVE_BRACKET", asset_class=asset_class,
            position_notional=position_notional, quantity=quantity,
            market_state=market_state,
        )

    def _size_position(
        self, desk: str, instrument: str, direction: Direction, price: float, net_edge: float, liquidity_score: float, protection: ProtectionPlan, risk_manager, venue: str | None = None, balance_source=None, venue_market_state: VenueMarketState | None = None,
        available_cash_snapshot: float | None = None, balance_source_label: str | None = None,
    ) -> PositionSizingDecision:
        _ = direction
        capital_venue = str(venue or "").strip().lower()
        bal: Mapping[str, Any] = {}
        balance_label = ""
        try:
            if available_cash_snapshot is not None:
                cash = max(0.0, float(available_cash_snapshot))
                balance_label = str(balance_source_label or f"verified_collateral_snapshot:{capital_venue or 'selected'}")
                bal = {"available": cash, "source": balance_label}
            elif balance_source is not None and hasattr(balance_source, "get_balance"):
                raw_balance = balance_source.get_balance() or {}
                bal = raw_balance if isinstance(raw_balance, Mapping) else {}
                balance_label = str(bal.get("source") or f"{capital_venue or 'selected'}_live_balance")
                cash = _num(bal.get("available"), 0.0)
            elif capital_venue and bool(_cfg("VENUE_SELECTION_ENABLED", True)):
                # A selected multi-venue route may never silently borrow the
                # primary/default broker balance merely because its adapter is
                # unavailable. Fail closed and surface the wiring defect.
                return PositionSizingDecision(
                    desk, instrument, False, 0.0, 0.0, 0.0, None, 0.0, net_edge, 0.0, 0.0, 0.0,
                    [f"selected_venue_balance_source_unavailable:{capital_venue}"], capital_venue=capital_venue,
                )
            else:
                raw_balance = risk_manager.get_available_balance() or {}
                bal = raw_balance if isinstance(raw_balance, Mapping) else {}
                balance_label = str(bal.get("source") or "single_venue_risk_manager_balance")
            cash = _num(bal.get("available"), 0.0)
        except Exception as exc:
            if capital_venue:
                return PositionSizingDecision(
                    desk, instrument, False, 0.0, 0.0, 0.0, None, 0.0, net_edge, 0.0, 0.0, 0.0,
                    [f"selected_venue_balance_fetch_failed:{capital_venue}"], capital_venue=capital_venue,
                    balance_source=str(exc)[:120],
                )
            cash = _num(_cfg("INITIAL_BALANCE", 0.0), 0.0)
            balance_label = "configured_initial_balance_fallback"
        if cash <= 0:
            return PositionSizingDecision(
                desk, instrument, False, 0.0, 0.0, 0.0, None, 0.0, net_edge, 0.0, 0.0, 0.0,
                [f"cash_unavailable:{capital_venue}" if capital_venue else "cash_unavailable"],
                capital_venue=capital_venue, available_cash_used=cash, balance_source=balance_label,
            )
        mapping = self._instrument_mapping(instrument, venue=venue)
        try:
            policy = active_policy(self._instrument)
        except Exception:
            policy = None
        stop_distance_pct = abs(price - protection.stop_price) / max(price, 1e-9)
        if stop_distance_pct <= 0:
            return PositionSizingDecision(desk, instrument, False, 0.0, 0.0, 0.0, None, 0.0, net_edge, 0.0, 0.0, 0.0, ["invalid_stop_distance"])
        # Strategy/order-manager quantity is exposure quantity, not raw venue
        # contract count. Delta's adapter is the sole boundary that converts
        # exposure quantity to integer contracts using contract_value.
        unit_notional = price
        risk_per_unit = unit_notional * stop_distance_pct
        risk_cash = _num(bal.get("risk_available"), cash)
        if risk_cash <= 0:
            risk_cash = cash
        risk_multiplier = _num(getattr(policy, "risk_multiplier", None), 1.0)
        risk_fraction = max(0.0, float(_cfg("INSTITUTIONAL_RISK_FRACTION_PER_TRADE", 0.0025))) * max(0.0, risk_multiplier)
        risk_budget = risk_cash * risk_fraction
        policy_margin_fraction = _num(getattr(policy, "margin_pct", None), float(_cfg("INSTITUTIONAL_MARGIN_PCT", 0.20)))
        margin_fraction = max(0.0, min(1.0, policy_margin_fraction))
        margin_budget = cash * margin_fraction
        edge_pct = max(0.0, net_edge) / 10_000.0
        observation_vol_bps = (
            float(venue_market_state.robust_one_minute_vol_bps)
            if venue_market_state is not None and venue_market_state.ready
            else self._realized_vol_log() * 10_000.0
        )
        vol_scalar = min(1.0, float(_cfg("INSTITUTIONAL_TARGET_OBSERVATION_VOL_BPS", 10.0)) / max(observation_vol_bps, 1e-6))
        risk_limited_notional = risk_budget / max(stop_distance_pct, 1e-9)
        kelly_fraction = float(_cfg("INSTITUTIONAL_FRACTIONAL_KELLY", _cfg("INSTITUTIONAL_QUARTER_KELLY", 0.25)))
        edge_to_stop = edge_pct / max(stop_distance_pct, 1e-9)
        min_kelly_scalar = max(0.0, float(_cfg("INSTITUTIONAL_MIN_KELLY_DEPLOYMENT_SCALAR", 0.0)))
        max_kelly_scalar = max(min_kelly_scalar, float(_cfg("INSTITUTIONAL_MAX_KELLY_DEPLOYMENT_SCALAR", 1.0)))
        kelly_scalar = min(max_kelly_scalar, max(min_kelly_scalar, edge_to_stop * max(0.0, kelly_fraction)))
        if desk == DeskId.INDIA_OPTIONS.value:
            # Never size NFO options from a configured/default lot.  The lot
            # must be the exact value joined from Groww's official instrument
            # master for the direction-specific session vehicle.
            leverage = None
            margin_cap_notional = margin_budget
            liquidity_cap = max(0.0, margin_cap_notional * min(1.0, max(0.0, liquidity_score)))
            kelly_notional = margin_cap_notional * kelly_scalar * vol_scalar
            target_notional = min(liquidity_cap, risk_limited_notional, kelly_notional)
            raw = getattr(getattr(self._instrument, "primary", None), "raw", {}) if self._instrument is not None else {}
            selected = raw.get("selected_option_contract") if isinstance(raw, dict) else None
            selected_raw = selected.get("raw") if isinstance(selected, dict) and isinstance(selected.get("raw"), dict) else {}
            lot = int(round(_num(selected_raw.get("runtime_lot_size"), 0.0)))
            if lot <= 0:
                return PositionSizingDecision(desk, instrument, False, 0.0, 0.0, 0.0, None, 0.0, net_edge, liquidity_cap, 0.0, 0.0, ["verified_nfo_lot_size_unavailable"])
            qty = float(max(0, math.floor(target_notional / max(unit_notional * lot, 1e-9)) * lot))
        else:
            step = max(float(mapping.qty_step or 0.0), 1e-12)
            configured_lev = float(_cfg("LEVERAGE", 1.0))
            code_cap = float(_cfg("INSTITUTIONAL_MAX_SELECTED_LEVERAGE", configured_lev))
            venue_cap = self._venue_max_leverage(venue)
            caps = [configured_lev, code_cap]
            if venue_cap > 0:
                caps.append(venue_cap)
            leverage = max(1.0, min(caps))
            margin_cap_notional = max(0.0, margin_budget * leverage)
            liquidity_scalar = min(1.0, max(0.0, liquidity_score))
            liquidity_cap = max(0.0, margin_cap_notional * liquidity_scalar)
            kelly_notional = margin_cap_notional * kelly_scalar * vol_scalar
            hard_cap_notional = min(margin_cap_notional, liquidity_cap, risk_limited_notional)
            min_order_notional = self._venue_min_order_notional_usd(capital_venue)
            exchange_instrument = self._exchange_instrument(venue)
            raw = getattr(exchange_instrument, "raw", {}) if exchange_instrument is not None else {}
            min_qty = max(0.0, _num(
                raw.get("min_qty")
                or raw.get("min_base_quantity")
                or raw.get("minQuantity")
                or raw.get("min_size")
                or getattr(exchange_instrument, "min_qty", 0.0),
                0.0,
            ))
            max_qty = max(0.0, _num(
                raw.get("max_qty")
                or raw.get("max_base_quantity")
                or raw.get("maxQuantity")
                or getattr(exchange_instrument, "max_qty", 0.0),
                0.0,
            ))
            min_executable_notional = max(min_order_notional, min_qty * unit_notional)
            target_notional = min(liquidity_cap, risk_limited_notional, kelly_notional)
            if (
                min_executable_notional > 0.0
                and target_notional + 1e-9 < min_executable_notional
                and min_executable_notional <= hard_cap_notional + 1e-9
            ):
                target_notional = min_executable_notional

            def _floor_to_step(value: float) -> float:
                return math.floor(max(0.0, value) / step) * step

            def _ceil_to_step(value: float) -> float:
                return math.ceil(max(0.0, value) / step) * step

            raw_qty = target_notional / max(unit_notional, 1e-9)
            qty = _floor_to_step(raw_qty)
            if min_executable_notional > 0.0 and min_executable_notional <= hard_cap_notional + 1e-9:
                min_exec_qty = max(min_qty, min_order_notional / max(unit_notional, 1e-9))
                if qty * unit_notional + 1e-9 < min_executable_notional:
                    raised_qty = _ceil_to_step(min_exec_qty)
                    if raised_qty * unit_notional <= hard_cap_notional + 1e-9:
                        qty = raised_qty
            if max_qty > 0.0:
                qty = min(qty, _floor_to_step(max_qty))
        notional = qty * unit_notional
        margin = notional if leverage is None else notional / max(leverage, 1.0)
        risk_after = qty * risk_per_unit
        min_order_notional = self._venue_min_order_notional_usd(capital_venue)
        if qty > 0 and min_order_notional > 0.0 and notional + 1e-9 < min_order_notional:
            return PositionSizingDecision(
                desk=desk, instrument=instrument, approved=False, quantity=qty, notional=notional,
                margin_required=margin, leverage_selected=leverage, risk_to_invalidation=risk_after,
                expected_net_edge=net_edge, liquidity_capacity_cap=liquidity_cap,
                portfolio_risk_before=0.0, portfolio_risk_after=risk_after,
                reasons=[f"order_notional_below_venue_minimum:{capital_venue}:{notional:.4f}<{min_order_notional:.4f}"],
                capital_venue=capital_venue, available_cash_used=cash, balance_source=balance_label,
            )
        approved = (
            qty > 0
            and margin <= cash + 1e-9
            and margin <= margin_budget + 1e-9
            and net_edge > 0
            and risk_after <= risk_budget + 1e-9
        )
        reasons = ([f"broker_local_cash_sizing_approved:{capital_venue or 'single_venue'}"] if approved
                   else [])
        if not approved:
            if qty <= 0:
                reasons.append("quantity_rounds_to_zero_after_venue_step")
            if margin > cash + 1e-9:
                reasons.append(f"free_cash_margin_exceeded:{margin:.4f}>{cash:.4f}")
            if margin > margin_budget + 1e-9:
                reasons.append(f"policy_margin_budget_exceeded:{margin:.4f}>{margin_budget:.4f}")
            if net_edge <= 0:
                reasons.append(f"nonpositive_net_edge:{net_edge:.3f}")
            if risk_after > risk_budget + 1e-9:
                reasons.append(f"stop_risk_budget_exceeded:{risk_after:.4f}>{risk_budget:.4f}")
            if not reasons:
                reasons.append(f"broker_local_cash_sizing_rejected:{capital_venue or 'single_venue'}")
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
            capital_venue=capital_venue, available_cash_used=cash, balance_source=balance_label,
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

    def _exchange_instrument(self, venue: str | None = None):
        if self._instrument is None:
            return None
        try:
            if venue:
                return self._instrument.by_exchange.get(ExchangeName(str(venue).lower()))
        except Exception:
            pass
        try:
            return self._instrument.primary
        except Exception:
            return None

    def _venue_max_leverage(self, venue: str | None = None) -> float:
        ei = self._exchange_instrument(venue)
        try:
            return float(getattr(ei, "max_leverage", 0.0) or 0.0)
        except Exception:
            return 0.0

    def _instrument_mapping(self, instrument: str, venue: str | None = None) -> InstrumentMapping:
        venue_name = str(venue).lower() if venue else "delta"
        try:
            if not venue and self._instrument is not None:
                venue_name = self._instrument.primary_exchange.value
        except Exception:
            pass
        primary = self._exchange_instrument(venue_name)
        raw = getattr(primary, "raw", {}) if primary is not None else {}
        contract_multiplier = _num(
            raw.get("contract_multiplier")
            or raw.get("contract_value")
            or raw.get("contract_value_btc")
            or getattr(primary, "contract_value_btc", 0.0),
            1.0,
        )
        qty_step = _num(
            raw.get("qty_step")
            or raw.get("lot_step")
            or getattr(primary, "lot_step", 0.0)
            or (contract_multiplier if venue_name == "delta" else 0.0),
            1.0,
        )
        notional_model = "inverse_usd_contract" if venue_name == "delta" and instrument.upper() == "BTCUSD" else "linear"
        return InstrumentMapping(
            venue=venue_name,
            venue_symbol=str(getattr(primary, "symbol", instrument) or instrument),
            canonical_underlying=self._asset_id,
            product_class=str(raw.get("contract_type") or raw.get("product_type") or ""),
            quote_currency=str(raw.get("quote_asset") or getattr(primary, "quote_asset", "") or "USD").upper(),
            contract_multiplier=max(contract_multiplier, 1e-12),
            settlement_currency=str(raw.get("settlement_currency") or raw.get("settling_asset") or "USD").upper(),
            price_tick=_num(raw.get("tick_size") or getattr(primary, "tick_size", 0.0), 0.01),
            qty_step=max(qty_step, 1e-12),
            execution_enabled=True,
            notional_model=notional_model,
        )

    def _desk_id(self, venue: str, instrument: str) -> str:
        if venue == "groww":
            return DeskId.INDIA_OPTIONS.value
        asset = self._asset_id.upper()
        if asset.startswith("GOLD") or asset.startswith("SILVER") or any(x in instrument.upper() for x in ("PAXG", "XAUT", "SLV", "XAG", "SILVER", "GOLD")):
            return DeskId.METALS.value
        if asset == "OIL" or any(x in instrument.upper() for x in ("CL", "WTI", "OIL", "CRUDE")):
            return DeskId.COMMODITIES.value
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
        # In live multi-venue routing, missing venue identity is unsafe: an
        # implicit Delta fallback can mis-size or mis-route another broker.
        return "unresolved"

    def _routeable_venues(self, order_manager) -> set[str]:
        getter = getattr(order_manager, "available_exchanges", None)
        if callable(getter):
            try:
                return {str(v).lower() for v in getter()}
            except Exception:
                pass
        return {self._venue(order_manager)}

    def _execution_manager_for(self, order_manager, venue: str):
        getter = getattr(order_manager, "manager_for", None)
        if callable(getter):
            try:
                return getter(str(venue).lower())
            except Exception:
                pass
        return order_manager

    def _venue_available_cash(self, order_manager, venues: set[str] | None = None) -> dict[str, float]:
        """Return asynchronous broker-local collateral snapshots only.

        This method is part of the market-decision hot path and therefore never
        performs broker HTTP/RPC calls. A missing or stale snapshot makes that
        venue non-routeable for this decision rather than blocking every desk.
        """
        if self._runtime_order_manager is None:
            self.start_runtime_services(order_manager)
        requested = {str(v).lower() for v in (venues or self._routeable_venues(order_manager))}
        if self._collateral_service is not None:
            return self._collateral_service.cash_by_venue(order_manager, requested)
        now = time.monotonic()
        max_age = max(5.0, float(_cfg("BROKER_COLLATERAL_SNAPSHOT_MAX_AGE_SEC", _cfg("VENUE_BALANCE_SNAPSHOT_MAX_AGE_SEC", 120.0))))
        out: dict[str, float] = {}
        for venue in sorted(requested):
            with self._venue_cash_lock:
                cached = self._venue_cash_cache.get(venue)
            if cached is None or now - float(cached[0]) > max_age:
                out[venue] = 0.0
                continue
            out[venue] = max(0.0, float(cached[1]))
        return out

    def _protection_capable_venues(self, order_manager, venues: set[str] | None = None) -> set[str]:
        protected: set[str] = set()
        for venue in (venues or self._routeable_venues(order_manager)):
            manager = self._execution_manager_for(order_manager, venue)
            adapter = getattr(manager, "_adapter", None)
            # Production OrderManager delegates to its adapter; light-weight
            # simulation managers may implement the protected lifecycle directly.
            if callable(getattr(manager, "place_bracket_limit_entry", None)) or callable(getattr(adapter, "place_bracket_limit_entry", None)):
                protected.add(str(venue).lower())
        return protected

    def _venue_min_order_notional_usd(self, venue: str | None) -> float:
        """Return hard broker-contract minimum order notional for execution gating.

        Minimum order values are execution constraints, not an invitation to
        increase size beyond liquidity/risk budgets.  A route that cannot meet
        its broker's minimum at the model-approved size is rejected fail-closed.
        """
        key = str(venue or "").strip().lower()
        if key == "hyperliquid":
            return max(10.0, float(_cfg("HYPERLIQUID_MIN_ORDER_NOTIONAL_USD", 10.0) or 10.0))
        return 0.0

    def _venue_selection_budgets(self, available_cash_by_venue: Mapping[str, float]) -> tuple[dict[str, float], dict[str, float]]:
        """Build cost-comparison sizes from each venue's own free collateral.

        Price/fee/depth comparison is invalid when every candidate is costed at
        a notional funded by Delta. Each executable venue is therefore assessed
        only at the provisional notional its own live available balance can
        support; final size is recomputed after selection using full SL geometry.
        """
        fraction = max(0.0, min(1.0, float(_cfg(
            "VENUE_SELECTION_MARGIN_FRACTION",
            _cfg("VENUE_SELECTION_NOTIONAL_FRACTION", 0.25),
        ))))
        min_margin = max(0.0, float(_cfg("VENUE_SELECTION_MIN_FREE_MARGIN_USD", 1.0)))
        configured_lev = max(1.0, float(_cfg("LEVERAGE", 1.0) or 1.0))
        code_cap = max(1.0, float(_cfg("INSTITUTIONAL_MAX_SELECTED_LEVERAGE", configured_lev) or configured_lev))
        notionals: dict[str, float] = {}
        margins: dict[str, float] = {}
        for venue, raw_cash in available_cash_by_venue.items():
            key = str(venue).lower()
            cash = max(0.0, _num(raw_cash, 0.0))
            venue_cap = self._venue_max_leverage(key)
            caps = [configured_lev, code_cap]
            if venue_cap > 0:
                caps.append(venue_cap)
            leverage = max(1.0, min(caps))
            margin_allocation = cash * fraction
            notional = margin_allocation * leverage
            min_order_notional = self._venue_min_order_notional_usd(key)
            if min_order_notional > 0.0 and 0.0 < notional + 1e-9 < min_order_notional:
                # Cost the venue at its smallest executable contract value only
                # when that minimum can be funded within the same broker-local
                # margin allocation.  This permits a genuinely feasible route
                # to compete while final SL/risk sizing remains authoritative.
                min_required_margin = min_order_notional / leverage
                notional = min_order_notional if min_required_margin <= margin_allocation + 1e-9 else 0.0
            notionals[key] = notional
            margins[key] = max(min_margin, notional / leverage) if notional > 0 else min_margin
        return notionals, margins

    def _risk_normalised_route_inputs(
        self, *, states: Mapping[str, VenueMicrostate], candidate_venues: set[str],
        capacity_notional_by_venue: Mapping[str, float], approved_quantity: float | None,
    ) -> tuple[float, dict[str, float], dict[str, float], dict[str, Any], set[str]]:
        """Build a one-quantity venue comparison ledger.

        Broker-local collateral determines whether a route can participate, never
        the exposure at which it receives a score. Before sizing, the smallest
        funded candidate capacity determines a common benchmark quantity. After
        sizing, the exact approved base quantity is replayed across all candidates;
        venues that cannot represent or fund that identical exposure are recorded
        as ineligible rather than being scored at an incomparable size.
        """
        venues = {str(v).lower() for v in candidate_venues}
        positive_capacity_qty: list[float] = []
        for venue in sorted(venues):
            state = states.get(venue)
            mid = float(getattr(state, "mid", 0.0) or 0.0) if state is not None else 0.0
            cap_notional = max(0.0, _num(capacity_notional_by_venue.get(venue), 0.0))
            if mid > 0.0 and cap_notional > 0.0:
                positive_capacity_qty.append(cap_notional / mid)
        quantity = max(0.0, float(approved_quantity or 0.0)) if approved_quantity is not None else (min(positive_capacity_qty) if positive_capacity_qty else 0.0)
        max_error_bps = max(0.0, float(_cfg("VENUE_SELECTION_MAX_QUANTITY_REPRESENTATION_ERROR_BPS", 0.50)))
        min_margin = max(0.0, float(_cfg("VENUE_SELECTION_MIN_FREE_MARGIN_USD", 1.0)))
        configured_lev = max(1.0, float(_cfg("LEVERAGE", 1.0) or 1.0))
        code_cap = max(1.0, float(_cfg("INSTITUTIONAL_MAX_SELECTED_LEVERAGE", configured_lev) or configured_lev))
        notionals: dict[str, float] = {}
        margins: dict[str, float] = {}
        diagnostics: dict[str, Any] = {}
        executable: set[str] = set()
        for venue in sorted(venues):
            state = states.get(venue)
            mid = float(getattr(state, "mid", 0.0) or 0.0) if state is not None else 0.0
            cap_notional = max(0.0, _num(capacity_notional_by_venue.get(venue), 0.0))
            notional = quantity * mid if quantity > 0.0 and mid > 0.0 else 0.0
            venue_cap = self._venue_max_leverage(venue)
            caps = [configured_lev, code_cap]
            if venue_cap > 0.0:
                caps.append(venue_cap)
            leverage = max(1.0, min(caps))
            margin = max(min_margin, notional / leverage) if notional > 0.0 else min_margin
            mapping = self._instrument_mapping(self._symbol_for_venue(venue, self._asset_id), venue=venue)
            step = max(float(mapping.qty_step or 0.0), 1e-12)
            represented_qty = math.floor((quantity + 1e-15) / step) * step if quantity > 0.0 else 0.0
            representation_error_bps = (max(0.0, quantity - represented_qty) / quantity * 10_000.0) if quantity > 0.0 else math.inf
            exact_quantity = approved_quantity is None or representation_error_bps <= max_error_bps
            min_notional = self._venue_min_order_notional_usd(venue)
            broker_minimum_ok = notional + 1e-9 >= min_notional
            capacity_ok = notional > 0.0 and notional <= cap_notional + 1e-9
            eligible = bool(mid > 0.0 and capacity_ok and broker_minimum_ok and exact_quantity)
            if eligible:
                executable.add(venue)
            notionals[venue] = notional
            margins[venue] = margin
            diagnostics[venue] = {
                "quantity": quantity,
                "represented_quantity": represented_qty,
                "quantity_step": step,
                "representation_error_bps": representation_error_bps,
                "exact_executable_quantity": exact_quantity,
                "notional_usd": notional,
                "broker_local_capacity_notional_usd": cap_notional,
                "capacity_ok": capacity_ok,
                "broker_minimum_notional_usd": min_notional,
                "broker_minimum_ok": broker_minimum_ok,
                "route_eligible_at_common_quantity": eligible,
            }
        return quantity, notionals, margins, diagnostics, executable

    def _entry_risk_gate(self, risk_manager, *, balance_source=None, cached_equity: float | None = None) -> tuple[bool, str]:
        """Gate live entries through the same risk controls that record entries.

        For multi-venue runtime the approved sizing calculation already consumed a
        verified, freshness-gated broker-local collateral snapshot.  Reuse that
        conservative available-cash value rather than generating another REST call
        immediately before order submission.
        """
        if not self._pos.is_flat():
            return False, "position_already_active"
        gate = getattr(risk_manager, "can_trade", None)
        if callable(gate):
            try:
                result = gate()
                allowed = bool(result[0]) if isinstance(result, tuple) and result else bool(result)
                reason = str(result[1]) if isinstance(result, tuple) and len(result) > 1 else ("OK" if allowed else "risk_manager_rejected")
                if not allowed:
                    return False, f"risk_manager_gate:{reason}"
            except Exception as exc:
                return False, f"risk_manager_gate_error:{exc}"
        equity = max(0.0, float(cached_equity or 0.0)) if cached_equity is not None else 0.0
        try:
            if cached_equity is None:
                if balance_source is not None and hasattr(balance_source, "get_balance"):
                    bal = balance_source.get_balance() or {}
                else:
                    bal = risk_manager.get_available_balance() or {}
                equity = max(
                    _num(bal.get("equity"), 0.0),
                    _num(bal.get("total"), 0.0),
                    _num(bal.get("available"), 0.0),
                )
        except Exception:
            equity = _num(_cfg("INITIAL_BALANCE", 0.0), 0.0)
        if equity > 0 and self._risk_gate.opening_balance <= 0:
            self._risk_gate.set_opening_balance(equity)
        allowed, reason = self._risk_gate.can_trade(equity)
        if not allowed:
            return False, f"strategy_daily_gate:{reason}"
        return True, "entry_frequency_and_daily_risk_validated"

    def _symbol_for_venue(self, venue: str, fallback: str) -> str:
        try:
            ei = self._exchange_instrument(venue)
            sym = str(getattr(ei, "symbol", "") or "").strip()
            if sym:
                return sym
        except Exception:
            pass
        return str(fallback)

    def _symbol(self, data_manager, order_manager) -> str:
        for obj in (order_manager, data_manager):
            for attr in ("display_symbol", "symbol"):
                val = getattr(obj, attr, None)
                if val:
                    return str(val)
        if self._instrument is not None:
            return self._instrument.display_symbol
        return str(_cfg("SYMBOL", "BTCUSD"))

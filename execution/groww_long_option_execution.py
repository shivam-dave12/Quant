"""Groww long-option execution lifecycle.

This module implements the mandatory fill-first protection sequence for NSE
F&O long options:

1. Submit a BUY entry for one valid CE/PE contract.
2. Confirm actual filled quantity and average fill price.
3. Submit an OCO SELL smart order for the filled quantity only.
4. Confirm the OCO is active before the position is considered protected.

No short-option entries are supported here.
"""

from __future__ import annotations

import logging
import math
import time
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Callable, Mapping, Optional

try:
    import config
except Exception:  # pragma: no cover - config is always present in runtime/tests
    config = None  # type: ignore


logger = logging.getLogger(__name__)


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
        return float(value)
    except Exception:
        return default


def _first_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        for key in ("Success", "success", "data", "result"):
            nested = value.get(key)
            if isinstance(nested, Mapping):
                return dict(nested)
            if isinstance(nested, list) and nested and isinstance(nested[0], Mapping):
                return dict(nested[0])
        return dict(value)
    if isinstance(value, list) and value and isinstance(value[0], Mapping):
        return dict(value[0])
    return {}


def _extract_id(resp: Any, *keys: str) -> str:
    data = _first_mapping(resp)
    for key in keys:
        val = data.get(key)
        if val not in (None, ""):
            return str(val).strip()
    if isinstance(resp, str):
        return resp.strip()
    return ""


def _const(api: Any, name: str, default: str) -> str:
    getter = getattr(api, "const", None)
    if callable(getter):
        try:
            return str(getter(name, default) or default)
        except Exception:
            return str(default)
    client = getattr(api, "client", api)
    try:
        return str(getattr(client, name, default) or default)
    except Exception:
        return str(default)


def _reference_id(api: Any, prefix: str) -> str:
    getter = getattr(api, "reference_id", None)
    if callable(getter):
        try:
            ref = str(getter(prefix) or "").strip()
            if ref:
                return ref
        except Exception:
            pass
    raw = f"{prefix}-{int(time.time() * 1000) % 10_000_000_000}"
    return raw[:20]


class GrowwLongOptionExecutionState(str, Enum):
    CANDIDATE_SELECTED = "CANDIDATE_SELECTED"
    ENTRY_SUBMITTED = "ENTRY_SUBMITTED"
    ENTRY_PARTIAL_OR_FILLED = "ENTRY_PARTIAL_OR_FILLED"
    PROTECTION_SUBMITTED_FOR_FILLED_QTY = "PROTECTION_SUBMITTED_FOR_FILLED_QTY"
    PROTECTION_CONFIRMED = "PROTECTION_CONFIRMED"
    ACTIVE_PROTECTED_POSITION = "ACTIVE_PROTECTED_POSITION"
    EXITED_TARGET = "EXITED_TARGET"
    EXITED_STOP = "EXITED_STOP"
    MANUAL_EMERGENCY_EXIT = "MANUAL_EMERGENCY_EXIT"
    RECONCILIATION_REQUIRED = "RECONCILIATION_REQUIRED"
    UNPROTECTED_POSITION_EMERGENCY = "UNPROTECTED_POSITION_EMERGENCY"
    REJECTED = "REJECTED"


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
    expected_premium_return_after_cost: float
    probability_tp_before_sl: float | None
    theta_cost_for_expected_hold: float
    liquidity_score: float
    protection_feasible: bool
    total_score: float
    rejection_reason: str | None = None
    lot_size: int | None = None
    underlying: str = "NIFTY"


@dataclass(frozen=True)
class GrowwProtectionPlan:
    target_price: float
    stop_trigger_price: float
    stop_limit_price: float | None = None
    underlying_invalidation_level: float | None = None
    underlying_target_levels: tuple[float, ...] = ()
    thesis_side: str = ""
    premium_target_source: str = "option_premium_target"
    premium_stop_source: str = "option_premium_stop"


@dataclass(frozen=True)
class GrowwStaticIpValidation:
    required: bool
    approved: bool
    configured_ip: str = ""
    observed_ip: str = ""
    reason: str = ""


@dataclass
class GrowwLongOptionLifecycleResult:
    state: GrowwLongOptionExecutionState
    approved: bool
    entry_order_id: str = ""
    filled_quantity: int = 0
    average_fill_price: float = 0.0
    smart_order_id: str = ""
    protection_confirmed: bool = False
    emergency_order_id: str = ""
    blocked_new_entries: bool = False
    reasons: list[str] = field(default_factory=list)
    audit_trail: list[dict[str, Any]] = field(default_factory=list)
    raw_entry_response: dict[str, Any] = field(default_factory=dict)
    raw_protection_response: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        out = asdict(self)
        out["state"] = self.state.value
        return out


OrderBodyBuilder = Callable[[str, str, float], dict[str, Any]]


class GrowwLongOptionExecutor:
    """Deterministic executor for Groww long-option entries and OCO exits."""

    ACTIVE_SMART_STATUSES = {"ACTIVE", "OPEN", "TRIGGER_PENDING", "TRIGGERED", "PLACED"}
    FILL_STATUSES = {"EXECUTED", "FILLED", "COMPLETE", "COMPLETED"}
    PARTIAL_STATUSES = {"PARTIALLY_FILLED", "PARTIAL_FILL", "PARTIAL"}
    DEAD_STATUSES = {"CANCELLED", "CANCELED", "REJECTED", "FAILED", "EXPIRED"}

    def __init__(
        self,
        api: Any,
        *,
        order_body_factory: Callable[..., dict[str, Any]] | None = None,
        notifier: Callable[[str], Any] | None = None,
        static_ip_validator: Callable[[], Any] | None = None,
        tick_size: float = 0.05,
        sleep: Callable[[float], Any] = time.sleep,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.api = api
        self.order_body_factory = order_body_factory
        self.notifier = notifier
        self.static_ip_validator = static_ip_validator
        self.tick_size = max(float(tick_size or 0.05), 0.01)
        self.sleep = sleep
        self.clock = clock
        self.blocked_new_entries = False

    def execute(
        self,
        *,
        candidate: LongOptionCandidateScore,
        quantity: int | float,
        limit_price: float,
        protection: GrowwProtectionPlan,
        fill_timeout_sec: float = 30.0,
        poll_interval_sec: float = 1.0,
        require_static_ip: bool = False,
        require_algo_confirmation: bool = False,
    ) -> GrowwLongOptionLifecycleResult:
        audit: list[dict[str, Any]] = []
        reasons: list[str] = []

        def step(state: GrowwLongOptionExecutionState, **fields: Any) -> None:
            audit.append({"state": state.value, "ts": self.clock(), **fields})

        step(GrowwLongOptionExecutionState.CANDIDATE_SELECTED, candidate=asdict(candidate))
        static_check = self.validate_static_ip(require_static_ip=require_static_ip)
        if static_check.required and not static_check.approved:
            reason = f"GROWW_STATIC_IP_FAIL_CLOSED: {static_check.reason or 'not approved'}"
            reasons.append(reason)
            step(GrowwLongOptionExecutionState.REJECTED, reason=reason, static_ip=asdict(static_check))
            return GrowwLongOptionLifecycleResult(
                state=GrowwLongOptionExecutionState.REJECTED,
                approved=False,
                blocked_new_entries=True,
                reasons=reasons,
                audit_trail=audit,
            )

        if require_algo_confirmation and not bool(_cfg("GROWW_SEBI_ALGO_REGISTRATION_CONFIRMED", False)):
            reason = "GROWW_ALGO_REGISTRATION_UNCONFIRMED: broker confirmation required before live API order"
            reasons.append(reason)
            step(GrowwLongOptionExecutionState.REJECTED, reason=reason)
            return GrowwLongOptionLifecycleResult(
                state=GrowwLongOptionExecutionState.REJECTED, approved=False, blocked_new_entries=True,
                reasons=reasons, audit_trail=audit,
            )

        validation_error = self._validate(candidate, quantity, limit_price, protection)
        if validation_error:
            reasons.append(validation_error)
            step(GrowwLongOptionExecutionState.REJECTED, reason=validation_error)
            return GrowwLongOptionLifecycleResult(
                state=GrowwLongOptionExecutionState.REJECTED,
                approved=False,
                reasons=reasons,
                audit_trail=audit,
            )

        entry_body = self._entry_body(candidate, int(quantity), limit_price)
        entry_resp = self._place_order(entry_body)
        entry_id = _extract_id(entry_resp, "groww_order_id", "order_id", "id")
        if not entry_id:
            reason = "GROWW_ENTRY_SUBMIT_FAILED"
            reasons.append(reason)
            step(GrowwLongOptionExecutionState.REJECTED, reason=reason, raw=entry_resp)
            return GrowwLongOptionLifecycleResult(
                state=GrowwLongOptionExecutionState.REJECTED,
                approved=False,
                reasons=reasons,
                audit_trail=audit,
                raw_entry_response=dict(entry_resp or {}) if isinstance(entry_resp, Mapping) else {},
            )

        step(GrowwLongOptionExecutionState.ENTRY_SUBMITTED, order_id=entry_id, payload=entry_body)
        fill = self._wait_for_fill(
            order_id=entry_id,
            requested_qty=int(quantity),
            timeout_sec=fill_timeout_sec,
            poll_interval_sec=poll_interval_sec,
        )
        status = str(fill.get("status") or "").upper()
        filled_qty = int(_num(fill.get("filled_quantity"), 0.0))
        avg_price = _num(fill.get("average_price"), 0.0)

        if status not in self.FILL_STATUSES | self.PARTIAL_STATUSES or filled_qty <= 0:
            reason = f"GROWW_ENTRY_NOT_FILLED: status={status or 'UNKNOWN'}"
            reasons.append(reason)
            self._cancel_order(entry_id)
            step(GrowwLongOptionExecutionState.REJECTED, reason=reason, fill=fill)
            return GrowwLongOptionLifecycleResult(
                state=GrowwLongOptionExecutionState.REJECTED,
                approved=False,
                entry_order_id=entry_id,
                reasons=reasons,
                audit_trail=audit,
                raw_entry_response=dict(entry_resp or {}) if isinstance(entry_resp, Mapping) else {},
            )

        if status in self.PARTIAL_STATUSES or filled_qty < int(quantity):
            self._cancel_order(entry_id)
            reasons.append("PARTIAL_FILL_REMAINDER_CANCELLED")

        step(
            GrowwLongOptionExecutionState.ENTRY_PARTIAL_OR_FILLED,
            order_id=entry_id,
            status=status,
            filled_quantity=filled_qty,
            average_price=avg_price,
        )

        oco_payload = self._oco_body(candidate, filled_qty, protection)
        oco_resp: dict[str, Any] = {}
        smart_id = ""
        try:
            oco_resp = self._create_smart_order(oco_payload)
            smart_id = _extract_id(oco_resp, "smart_order_id", "id", "order_id")
            step(
                GrowwLongOptionExecutionState.PROTECTION_SUBMITTED_FOR_FILLED_QTY,
                smart_order_id=smart_id,
                payload=oco_payload,
                raw=oco_resp,
            )
            oco_confirmed = bool(smart_id) and self._confirm_oco(smart_id, candidate, filled_qty)
        except Exception as exc:
            logger.exception("Groww OCO submission/status verification failed after filled entry")
            reasons.append(f"GROWW_OCO_API_ERROR: {exc}")
            oco_confirmed = False

        if not oco_confirmed:
            return self._emergency_result(
                candidate=candidate,
                entry_order_id=entry_id,
                filled_qty=filled_qty,
                average_price=avg_price,
                reasons=reasons,
                audit=audit,
                raw_entry_response=dict(entry_resp or {}) if isinstance(entry_resp, Mapping) else {},
                raw_protection_response=dict(oco_resp or {}) if isinstance(oco_resp, Mapping) else {},
                reason="GROWW_OCO_PROTECTION_CONFIRMATION_FAILED",
            )

        step(GrowwLongOptionExecutionState.PROTECTION_CONFIRMED, smart_order_id=smart_id)
        step(GrowwLongOptionExecutionState.ACTIVE_PROTECTED_POSITION, smart_order_id=smart_id)
        return GrowwLongOptionLifecycleResult(
            state=GrowwLongOptionExecutionState.ACTIVE_PROTECTED_POSITION,
            approved=True,
            entry_order_id=entry_id,
            filled_quantity=filled_qty,
            average_fill_price=avg_price,
            smart_order_id=smart_id,
            protection_confirmed=True,
            reasons=reasons,
            audit_trail=audit,
            raw_entry_response=dict(entry_resp or {}) if isinstance(entry_resp, Mapping) else {},
            raw_protection_response=dict(oco_resp or {}) if isinstance(oco_resp, Mapping) else {},
        )

    def validate_static_ip(self, *, require_static_ip: bool) -> GrowwStaticIpValidation:
        if not require_static_ip:
            return GrowwStaticIpValidation(required=False, approved=True, reason="not_required")
        if self.static_ip_validator is None:
            return GrowwStaticIpValidation(required=True, approved=False, reason="validator_missing")
        try:
            raw = self.static_ip_validator()
        except Exception as exc:
            return GrowwStaticIpValidation(required=True, approved=False, reason=str(exc))
        if isinstance(raw, GrowwStaticIpValidation):
            return raw
        if isinstance(raw, Mapping):
            return GrowwStaticIpValidation(
                required=True,
                approved=bool(raw.get("approved")),
                configured_ip=str(raw.get("configured_ip") or ""),
                observed_ip=str(raw.get("observed_ip") or ""),
                reason=str(raw.get("reason") or ""),
            )
        if isinstance(raw, bool):
            return GrowwStaticIpValidation(required=True, approved=raw, reason="callable_bool")
        return GrowwStaticIpValidation(required=True, approved=False, reason="invalid_validator_result")

    def _validate(
        self,
        candidate: LongOptionCandidateScore,
        quantity: int | float,
        limit_price: float,
        protection: GrowwProtectionPlan,
    ) -> str:
        right = str(candidate.option_type or "").strip().upper()
        if right not in {"CE", "PE"}:
            return f"GROWW_LONG_OPTION_ONLY_REJECTED: option_type={candidate.option_type!r}"
        if not str(candidate.trading_symbol or "").strip():
            return "GROWW_MISSING_TRADING_SYMBOL"
        if candidate.rejection_reason:
            return f"GROWW_CANDIDATE_REJECTED: {candidate.rejection_reason}"
        if not candidate.protection_feasible:
            return "GROWW_PROTECTION_NOT_FEASIBLE"
        qty = int(float(quantity or 0))
        if qty <= 0:
            return "GROWW_INVALID_QUANTITY"
        if candidate.lot_size:
            lot = int(candidate.lot_size)
            if lot <= 0 or qty % lot != 0:
                return f"GROWW_QUANTITY_NOT_LOT_ALIGNED: qty={qty} lot={lot}"
        if float(limit_price or 0.0) <= 0:
            return "GROWW_INVALID_ENTRY_LIMIT_PRICE"
        target = float(protection.target_price or 0.0)
        stop_trigger = float(protection.stop_trigger_price or 0.0)
        stop_limit = float(protection.stop_limit_price or stop_trigger or 0.0)
        if not (stop_limit > 0 and stop_trigger > 0 and target > 0):
            return "GROWW_INVALID_OCO_PRICES"
        if not (stop_limit <= stop_trigger < float(limit_price) < target):
            return (
                "GROWW_INVALID_LONG_OPTION_OCO_GEOMETRY: "
                f"stop_limit={stop_limit} stop_trigger={stop_trigger} entry={limit_price} target={target}"
            )
        return ""

    def _entry_body(self, candidate: LongOptionCandidateScore, quantity: int, limit_price: float) -> dict[str, Any]:
        if self.order_body_factory is not None:
            return self.order_body_factory("BUY", "LIMIT", quantity, price=limit_price, reduce_only=False)
        return {
            "trading_symbol": candidate.trading_symbol,
            "quantity": int(quantity),
            "validity": _const(self.api, "VALIDITY_DAY", "DAY"),
            "exchange": _const(self.api, "EXCHANGE_NSE", "NSE"),
            "segment": _const(self.api, "SEGMENT_FNO", "FNO"),
            "product": str(_cfg("GROWW_OPTION_PRODUCT_TYPE", "NRML") or "NRML").upper(),
            "order_type": _const(self.api, "ORDER_TYPE_LIMIT", "LIMIT"),
            "transaction_type": _const(self.api, "TRANSACTION_TYPE_BUY", "BUY"),
            "price": f"{float(limit_price):.2f}",
            "order_reference_id": _reference_id(self.api, str(_cfg("GROWW_SEBI_STRATEGY_PREFIX", "instv2"))),
        }

    def _oco_body(
        self,
        candidate: LongOptionCandidateScore,
        quantity: int,
        protection: GrowwProtectionPlan,
    ) -> dict[str, Any]:
        target = self._round_price(protection.target_price)
        stop_trigger = self._round_price(protection.stop_trigger_price)
        stop_limit = self._round_price(protection.stop_limit_price or max(self.tick_size, stop_trigger - self.tick_size))
        return {
            "smart_order_type": _const(self.api, "SMART_ORDER_TYPE_OCO", "OCO"),
            "reference_id": _reference_id(self.api, "oco"),
            "segment": _const(self.api, "SEGMENT_FNO", "FNO"),
            "trading_symbol": candidate.trading_symbol,
            "quantity": int(quantity),
            "product_type": str(_cfg("GROWW_OPTION_PRODUCT_TYPE", "NRML") or "NRML").upper(),
            "exchange": _const(self.api, "EXCHANGE_NSE", "NSE"),
            "duration": _const(self.api, "VALIDITY_DAY", "DAY"),
            "net_position_quantity": int(quantity),
            "transaction_type": _const(self.api, "TRANSACTION_TYPE_SELL", "SELL"),
            "target": {
                "trigger_price": f"{target:.2f}",
                "order_type": _const(self.api, "ORDER_TYPE_LIMIT", "LIMIT"),
                "price": f"{target:.2f}",
            },
            "stop_loss": {
                "trigger_price": f"{stop_trigger:.2f}",
                "order_type": _const(self.api, "ORDER_TYPE_STOP_LOSS", "SL"),
                "price": f"{stop_limit:.2f}",
            },
        }

    def _place_order(self, body: dict[str, Any]) -> dict[str, Any]:
        resp = self.api.place_order(**{k: v for k, v in body.items() if v not in (None, "")})
        return dict(resp or {}) if isinstance(resp, Mapping) else {"groww_order_id": str(resp)}

    def _create_smart_order(self, body: dict[str, Any]) -> dict[str, Any]:
        resp = self.api.create_smart_order(**{k: v for k, v in body.items() if v not in (None, "")})
        return dict(resp or {}) if isinstance(resp, Mapping) else {"smart_order_id": str(resp)}

    def _wait_for_fill(
        self,
        *,
        order_id: str,
        requested_qty: int,
        timeout_sec: float,
        poll_interval_sec: float,
    ) -> dict[str, Any]:
        deadline = self.clock() + max(float(timeout_sec or 0.0), 0.0)
        polls = 0
        while True:
            row = self._get_order(order_id)
            status = self._normalise_order_status(row)
            filled = int(_num(row.get("filled_quantity") or row.get("executed_quantity"), 0.0))
            if filled <= 0 and status in self.FILL_STATUSES:
                filled = requested_qty
            avg = self._average_fill_price(row)
            if status in self.FILL_STATUSES | self.PARTIAL_STATUSES | self.DEAD_STATUSES:
                trade_fill = self._get_trade_fill(order_id) if filled > 0 else {}
                if trade_fill:
                    filled = int(_num(trade_fill.get("filled_quantity"), filled) or filled)
                    avg = _num(trade_fill.get("average_price"), avg)
                    row["fill_source"] = trade_fill.get("fill_source")
                row["status"] = status
                row["filled_quantity"] = filled
                row["average_price"] = avg
                return row
            polls += 1
            if self.clock() >= deadline or polls > 500:
                row["status"] = status or "TIMEOUT"
                row["filled_quantity"] = filled
                row["average_price"] = avg
                return row
            self.sleep(max(float(poll_interval_sec or 0.0), 0.0))

    def _get_order(self, order_id: str) -> dict[str, Any]:
        """Read the order through Groww's documented get_order_detail endpoint only."""
        getter = getattr(self.api, "get_order_detail", None)
        if not callable(getter):
            raise RuntimeError("Groww SDK get_order_detail is mandatory for live FNO execution.")
        resp = getter(
            groww_order_id=order_id,
            segment=_const(self.api, "SEGMENT_FNO", "FNO"),
        )
        row = _first_mapping(resp)
        if not row:
            raise RuntimeError(f"Groww get_order_detail returned no order row for {order_id}.")
        return row

    def _get_trade_fill(self, order_id: str) -> dict[str, Any]:
        """Use Groww's documented trade list to compute actual filled VWAP."""
        getter = getattr(self.api, "get_trade_detail", None)
        if not callable(getter):
            return {}
        try:
            resp = getter(
                groww_order_id=order_id,
                segment=_const(self.api, "SEGMENT_FNO", "FNO"),
            )
        except TypeError:
            try:
                resp = getter(
                    order_id=order_id,
                    segment=_const(self.api, "SEGMENT_FNO", "FNO"),
                )
            except Exception as exc:
                logger.debug("Groww get_trade_detail failed for %s: %s", order_id, exc)
                return {}
        except Exception as exc:
            logger.debug("Groww get_trade_detail failed for %s: %s", order_id, exc)
            return {}

        rows = self._trade_rows(resp)
        num = den = 0.0
        for trade in rows:
            status = str(trade.get("trade_status") or trade.get("status") or "").strip().upper()
            if status and status not in {"EXECUTED", "FILLED", "COMPLETE", "COMPLETED"}:
                continue
            qty = _num(
                trade.get("quantity")
                or trade.get("qty")
                or trade.get("filled_quantity")
                or trade.get("executed_quantity"),
                0.0,
            )
            px = _num(
                trade.get("price")
                or trade.get("trade_price")
                or trade.get("average_fill_price")
                or trade.get("average_price")
                or trade.get("execution_price"),
                0.0,
            )
            if qty > 0 and px > 0:
                num += qty * px
                den += qty
        if den <= 0:
            return {}
        return {
            "filled_quantity": int(den),
            "average_price": num / den,
            "fill_source": "groww.get_trade_list_for_order",
        }

    @staticmethod
    def _trade_rows(value: Any) -> list[Mapping[str, Any]]:
        if isinstance(value, list):
            return [row for row in value if isinstance(row, Mapping)]
        if not isinstance(value, Mapping):
            return []
        rows: list[Mapping[str, Any]] = []
        for key in ("trade_list", "trades", "orders", "data", "result", "Success", "success"):
            nested = value.get(key)
            if isinstance(nested, list):
                rows.extend(row for row in nested if isinstance(row, Mapping))
            elif isinstance(nested, Mapping):
                rows.extend(GrowwLongOptionExecutor._trade_rows(nested))
        if rows:
            return rows
        return [value]

    def _cancel_order(self, order_id: str) -> dict[str, Any]:
        """Cancel through Groww's documented cancel_order endpoint only."""
        canceller = getattr(self.api, "cancel_order", None)
        if not callable(canceller):
            raise RuntimeError("Groww SDK cancel_order is mandatory for live FNO execution.")
        resp = canceller(
            groww_order_id=order_id,
            segment=_const(self.api, "SEGMENT_FNO", "FNO"),
        )
        return dict(resp or {}) if isinstance(resp, Mapping) else {}

    def _confirm_oco(
        self,
        smart_order_id: str,
        candidate: LongOptionCandidateScore,
        quantity: int,
    ) -> bool:
        """Confirm OCO state from Groww's documented smart-order status call only."""
        getter = getattr(self.api, "get_smart_order", None)
        if not callable(getter):
            raise RuntimeError("Groww SDK get_smart_order is mandatory for OCO verification.")
        resp = getter(
            smart_order_id=smart_order_id,
            segment=_const(self.api, "SEGMENT_FNO", "FNO"),
            smart_order_type=_const(self.api, "SMART_ORDER_TYPE_OCO", "OCO"),
        )
        row = _first_mapping(resp)
        if not row:
            return False
        status = str(row.get("status") or row.get("smart_order_status") or "").upper()
        if status not in self.ACTIVE_SMART_STATUSES:
            return False
        symbol = str(row.get("trading_symbol") or "").strip().upper()
        if symbol and symbol != candidate.trading_symbol.upper():
            return False
        row_qty = int(_num(row.get("quantity") or row.get("net_position_quantity"), quantity))
        if row_qty < int(quantity):
            return False
        tx = str(row.get("transaction_type") or "").strip().upper()
        return not tx or tx == _const(self.api, "TRANSACTION_TYPE_SELL", "SELL")

    def _emergency_result(
        self,
        *,
        candidate: LongOptionCandidateScore,
        entry_order_id: str,
        filled_qty: int,
        average_price: float,
        reasons: list[str],
        audit: list[dict[str, Any]],
        raw_entry_response: dict[str, Any],
        raw_protection_response: dict[str, Any],
        reason: str,
    ) -> GrowwLongOptionLifecycleResult:
        reasons.append(reason)
        self.blocked_new_entries = True
        alert = (
            "CRITICAL Groww long-option protection failed; blocking new entries "
            f"and attempting emergency exit. symbol={candidate.trading_symbol} qty={filled_qty} reason={reason}"
        )
        if self.notifier is not None:
            try:
                self.notifier(alert)
            except Exception:
                logger.exception("Groww protection failure notifier raised")
        logger.critical(alert)
        emergency_id = ""
        try:
            emergency_id = self._emergency_exit(candidate, filled_qty, average_price)
        except Exception as exc:
            reasons.append(f"GROWW_EMERGENCY_EXIT_SUBMIT_FAILED: {exc}")
            logger.exception("Groww emergency exit submission failed")
        audit.append(
            {
                "state": GrowwLongOptionExecutionState.UNPROTECTED_POSITION_EMERGENCY.value,
                "ts": self.clock(),
                "entry_order_id": entry_order_id,
                "filled_quantity": filled_qty,
                "emergency_order_id": emergency_id,
                "reason": reason,
            }
        )
        return GrowwLongOptionLifecycleResult(
            state=GrowwLongOptionExecutionState.UNPROTECTED_POSITION_EMERGENCY,
            approved=False,
            entry_order_id=entry_order_id,
            filled_quantity=filled_qty,
            average_fill_price=average_price,
            emergency_order_id=emergency_id,
            blocked_new_entries=True,
            reasons=reasons,
            audit_trail=audit,
            raw_entry_response=raw_entry_response,
            raw_protection_response=raw_protection_response,
        )

    def _emergency_exit(self, candidate: LongOptionCandidateScore, quantity: int, average_price: float) -> str:
        """Flatten an already-filled unprotected long option using Groww MARKET SELL.

        This is not a substitute entry route. It is mandatory loss containment after an
        entry fill exists but the required OCO protection cannot be verified.
        It intentionally does not fabricate a LIMIT price from a requested entry.
        """
        _ = average_price
        body = {
            "trading_symbol": candidate.trading_symbol,
            "quantity": int(quantity),
            "validity": _const(self.api, "VALIDITY_DAY", "DAY"),
            "exchange": _const(self.api, "EXCHANGE_NSE", "NSE"),
            "segment": _const(self.api, "SEGMENT_FNO", "FNO"),
            "product": str(_cfg("GROWW_OPTION_PRODUCT_TYPE", "NRML") or "NRML").upper(),
            "order_type": _const(self.api, "ORDER_TYPE_MARKET", "MARKET"),
            "transaction_type": _const(self.api, "TRANSACTION_TYPE_SELL", "SELL"),
            "order_reference_id": _reference_id(self.api, "emg"),
        }
        resp = self._place_order(body)
        return _extract_id(resp, "groww_order_id", "order_id", "id")

    @staticmethod
    def _normalise_order_status(row: Mapping[str, Any]) -> str:
        raw = str(
            row.get("order_status")
            or row.get("status")
            or row.get("orderStatus")
            or row.get("state")
            or ""
        ).strip().upper()
        if raw in {"EXECUTED", "FILLED", "COMPLETE", "COMPLETED"}:
            return "FILLED"
        if raw in {"PARTIALLY_FILLED", "PARTIAL_FILL", "PARTIAL"}:
            return "PARTIAL_FILL"
        if raw in {"CANCELLED", "CANCELED", "REJECTED", "FAILED", "EXPIRED"}:
            return "CANCELLED"
        if raw in {"OPEN", "PENDING", "NEW", "PLACED"}:
            return "PENDING"
        return raw or "UNKNOWN"

    @staticmethod
    def _average_fill_price(row: Mapping[str, Any]) -> float:
        for key in ("average_fill_price", "average_price", "avg_price", "execution_price", "price"):
            px = _num(row.get(key), 0.0)
            if px > 0:
                return px
        trades = row.get("trades")
        if isinstance(trades, list):
            num = den = 0.0
            for trade in trades:
                if not isinstance(trade, Mapping):
                    continue
                qty = _num(trade.get("quantity") or trade.get("qty"), 0.0)
                px = _num(trade.get("trade_price") or trade.get("price"), 0.0)
                if qty > 0 and px > 0:
                    num += qty * px
                    den += qty
            if den > 0:
                return num / den
        return 0.0

    def _round_price(self, value: float, *, mode: str = "nearest") -> float:
        raw = float(value or 0.0)
        if mode == "floor":
            rounded = math.floor(raw / self.tick_size) * self.tick_size
        elif mode == "ceil":
            rounded = math.ceil(raw / self.tick_size) * self.tick_size
        else:
            rounded = round(raw / self.tick_size) * self.tick_size
        return round(max(self.tick_size, rounded), 2)

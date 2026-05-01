"""
strategy/institutional_lifecycle.py

Institutional trade lifecycle planner.

This file is the single gate between a raw entry thesis and a live executable order.

Rules:
- Raw TP is display-only until routeability is positive.
- Missing target/liquidity/ATR/order-flow does not create fallback trades.
- SL must be structurally protected and not inside market noise.
- RR/EV are cost-adjusted.
- The plan object is immutable and must be the only input to execution.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional
import math
import time
import uuid


def finite(value: Any, default: float = 0.0) -> float:
    try:
        x = float(value)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def round_to_tick(price: float, tick_size: float) -> float:
    tick = max(finite(tick_size, 0.1), 1e-12)
    return round(round(price / tick) * tick, 10)


def reward_points(side: str, entry: float, tp: float) -> float:
    return tp - entry if side.lower() == "long" else entry - tp


def risk_points(side: str, entry: float, sl: float) -> float:
    return entry - sl if side.lower() == "long" else sl - entry


def improves_sl(side: str, old_sl: float, new_sl: float) -> bool:
    return new_sl > old_sl if side.lower() == "long" else new_sl < old_sl


@dataclass(frozen=True)
class PullbackDiagnostics:
    raw_pullback_atr: float
    delivery_atr: float
    delivery_retrace_atr: float
    risk_compression: float
    reason: str


@dataclass(frozen=True)
class RouteabilityResult:
    executable: bool
    reason: str
    target_price: float = 0.0
    target_label: str = ""
    target_probability: float = 0.0
    gross_rr: float = 0.0
    net_ev_r: float = 0.0
    utility: float = 0.0
    cost_r: float = 0.0


@dataclass(frozen=True)
class RiskEnvelope:
    valid: bool
    reason: str
    side: str
    entry: float
    structural_sl: float
    noise_floor_sl: float
    liquidation_guard_sl: float
    final_sl: float
    risk_points: float
    risk_atr: float


@dataclass(frozen=True)
class ExecutableTradePlan:
    executable: bool
    reason: str
    plan_id: str
    thesis_id: str
    side: str
    entry: float
    sl: float
    tp: float
    qty: float
    rr: float
    conviction: float
    raw_tp: float
    raw_tp_label: str
    routeability: RouteabilityResult
    risk: RiskEnvelope
    diagnostics: Dict[str, Any] = field(default_factory=dict)

    def as_order_payload(self) -> Dict[str, Any]:
        if not self.executable:
            raise ValueError(f"Cannot route non-executable plan: {self.reason}")
        return {
            "client_order_id": self.plan_id,
            "side": self.side,
            "entry_price": self.entry,
            "sl_price": self.sl,
            "tp_price": self.tp,
            "qty": self.qty,
            "rr": self.rr,
            "thesis_id": self.thesis_id,
        }


class InstitutionalLifecycle:
    def __init__(
        self,
        *,
        tick_size: float,
        min_rr: float,
        min_target_probability: float,
        min_net_ev_r: float,
        min_route_utility: float,
        min_risk_atr: float,
        max_risk_atr: float,
        round_trip_fee_rate: float,
        slippage_atr: float,
    ) -> None:
        self.tick_size = float(tick_size)
        self.min_rr = float(min_rr)
        self.min_target_probability = float(min_target_probability)
        self.min_net_ev_r = float(min_net_ev_r)
        self.min_route_utility = float(min_route_utility)
        self.min_risk_atr = float(min_risk_atr)
        self.max_risk_atr = float(max_risk_atr)
        self.round_trip_fee_rate = float(round_trip_fee_rate)
        self.slippage_atr = float(slippage_atr)

    def thesis_id(self, *, side: str, sweep: Any, entry: float) -> str:
        pool = getattr(sweep, "pool", None)
        pool_side = getattr(pool, "side", getattr(sweep, "side", "NA"))
        pool_tf = getattr(pool, "timeframe", "NA")
        pool_price = finite(getattr(pool, "price", 0.0))
        ts = int(finite(getattr(sweep, "candle_timestamp", time.time())))
        return f"{side.lower()}:{pool_side}:{pool_tf}:{pool_price:.1f}:{entry:.1f}:{ts}"

    def pullback_diagnostics(
        self,
        *,
        side: str,
        current_price: float,
        original_entry: float,
        original_sl: float,
        best_extension_price: float,
        sweep_price: float,
        atr: float,
    ) -> PullbackDiagnostics:
        a = max(finite(atr), 1e-12)
        side = side.lower()

        if side == "long":
            raw_pullback = max(0.0, best_extension_price - current_price) / a
            delivery = max(0.0, best_extension_price - sweep_price) / a
            delivery_retrace = raw_pullback if delivery > 0 else 0.0
            refined_risk = max(0.0, current_price - original_sl)
        else:
            raw_pullback = max(0.0, current_price - best_extension_price) / a
            delivery = max(0.0, sweep_price - best_extension_price) / a
            delivery_retrace = raw_pullback if delivery > 0 else 0.0
            refined_risk = max(0.0, original_sl - current_price)

        original_risk = abs(original_entry - original_sl)
        compression = 0.0 if original_risk <= 0 else max(-1.0, min(1.0, 1.0 - refined_risk / original_risk))

        return PullbackDiagnostics(
            raw_pullback_atr=raw_pullback,
            delivery_atr=delivery,
            delivery_retrace_atr=delivery_retrace,
            risk_compression=compression,
            reason=(
                f"raw_pullback={raw_pullback:.2f}ATR "
                f"delivery={delivery:.2f}ATR "
                f"delivery_retrace={delivery_retrace:.2f}ATR "
                f"risk_compression={compression:.0%}"
            ),
        )

    def build_risk(
        self,
        *,
        side: str,
        entry: float,
        atr: float,
        structural_sl: float,
        noise_floor_sl: float,
        liquidation_guard_sl: Optional[float] = None,
    ) -> RiskEnvelope:
        side = side.lower()
        entry = round_to_tick(finite(entry), self.tick_size)
        structural_sl = finite(structural_sl)
        noise_floor_sl = finite(noise_floor_sl)
        liquidation_guard_sl = finite(liquidation_guard_sl, structural_sl)
        a = finite(atr)

        if a <= 0:
            return RiskEnvelope(False, "ATR missing/invalid; no trade", side, entry, structural_sl, noise_floor_sl, liquidation_guard_sl, 0.0, 0.0, 0.0)

        if side == "long":
            candidates = [x for x in [structural_sl, noise_floor_sl, liquidation_guard_sl] if x > 0 and x < entry]
            if not candidates:
                return RiskEnvelope(False, "no valid long SL below entry", side, entry, structural_sl, noise_floor_sl, liquidation_guard_sl, 0.0, 0.0, 0.0)
            final_sl = min(candidates)
        else:
            candidates = [x for x in [structural_sl, noise_floor_sl, liquidation_guard_sl] if x > entry]
            if not candidates:
                return RiskEnvelope(False, "no valid short SL above entry", side, entry, structural_sl, noise_floor_sl, liquidation_guard_sl, 0.0, 0.0, 0.0)
            final_sl = max(candidates)

        final_sl = round_to_tick(final_sl, self.tick_size)
        rp = risk_points(side, entry, final_sl)
        risk_atr = rp / a if a > 0 else 0.0

        if rp <= 0:
            return RiskEnvelope(False, "negative/zero risk geometry", side, entry, structural_sl, noise_floor_sl, liquidation_guard_sl, final_sl, rp, risk_atr)
        if risk_atr < self.min_risk_atr:
            return RiskEnvelope(False, f"SL too tight: {risk_atr:.2f}ATR < {self.min_risk_atr:.2f}ATR", side, entry, structural_sl, noise_floor_sl, liquidation_guard_sl, final_sl, rp, risk_atr)
        if risk_atr > self.max_risk_atr:
            return RiskEnvelope(False, f"SL too wide: {risk_atr:.2f}ATR > {self.max_risk_atr:.2f}ATR", side, entry, structural_sl, noise_floor_sl, liquidation_guard_sl, final_sl, rp, risk_atr)

        return RiskEnvelope(True, "risk ok", side, entry, structural_sl, noise_floor_sl, liquidation_guard_sl, final_sl, rp, risk_atr)

    def evaluate_routeability(
        self,
        *,
        side: str,
        entry: float,
        sl: float,
        raw_tp: float,
        atr: float,
        target_probability: float,
        expected_value_r: float,
        route_utility: float,
        target_label: str,
    ) -> RouteabilityResult:
        side = side.lower()
        risk = risk_points(side, entry, sl)
        reward = reward_points(side, entry, raw_tp)
        if risk <= 0:
            return RouteabilityResult(False, "invalid risk for routeability")
        if reward <= 0:
            return RouteabilityResult(False, "target is on wrong side of entry")

        gross_rr = reward / risk
        a = max(finite(atr), 1e-12)
        cost_points = abs(entry) * self.round_trip_fee_rate + a * self.slippage_atr
        cost_r = cost_points / risk
        net_ev = finite(expected_value_r) - cost_r

        if gross_rr < self.min_rr:
            return RouteabilityResult(False, f"RR {gross_rr:.2f} below minimum {self.min_rr:.2f}", raw_tp, target_label, target_probability, gross_rr, net_ev, route_utility, cost_r)
        if target_probability < self.min_target_probability:
            return RouteabilityResult(False, f"target probability {target_probability:.2f} below minimum {self.min_target_probability:.2f}", raw_tp, target_label, target_probability, gross_rr, net_ev, route_utility, cost_r)
        if net_ev < self.min_net_ev_r:
            return RouteabilityResult(False, f"net EV {net_ev:.2f}R below minimum {self.min_net_ev_r:.2f}R after costs", raw_tp, target_label, target_probability, gross_rr, net_ev, route_utility, cost_r)
        if route_utility < self.min_route_utility:
            return RouteabilityResult(False, f"route utility {route_utility:.2f} below minimum {self.min_route_utility:.2f}", raw_tp, target_label, target_probability, gross_rr, net_ev, route_utility, cost_r)

        return RouteabilityResult(True, "routeable", round_to_tick(raw_tp, self.tick_size), target_label, target_probability, gross_rr, net_ev, route_utility, cost_r)

    def build_plan(
        self,
        *,
        side: str,
        entry: float,
        atr: float,
        structural_sl: float,
        noise_floor_sl: float,
        raw_tp: float,
        raw_tp_label: str,
        target_probability: float,
        expected_value_r: float,
        route_utility: float,
        conviction: float,
        qty: float,
        thesis_id: str,
        liquidation_guard_sl: Optional[float] = None,
        diagnostics: Optional[Dict[str, Any]] = None,
    ) -> ExecutableTradePlan:
        side = side.lower()
        risk = self.build_risk(
            side=side,
            entry=entry,
            atr=atr,
            structural_sl=structural_sl,
            noise_floor_sl=noise_floor_sl,
            liquidation_guard_sl=liquidation_guard_sl,
        )

        plan_id = f"liq-{uuid.uuid4().hex[:18]}"
        if not risk.valid:
            return ExecutableTradePlan(False, f"risk reject: {risk.reason}", plan_id, thesis_id, side, round_to_tick(entry, self.tick_size), risk.final_sl, 0.0, 0.0, 0.0, conviction, raw_tp, raw_tp_label, RouteabilityResult(False, risk.reason), risk, diagnostics or {})

        route = self.evaluate_routeability(
            side=side,
            entry=risk.entry,
            sl=risk.final_sl,
            raw_tp=raw_tp,
            atr=atr,
            target_probability=target_probability,
            expected_value_r=expected_value_r,
            route_utility=route_utility,
            target_label=raw_tp_label,
        )

        if not route.executable:
            return ExecutableTradePlan(False, f"target reject: {route.reason}", plan_id, thesis_id, side, risk.entry, risk.final_sl, 0.0, 0.0, 0.0, conviction, raw_tp, raw_tp_label, route, risk, diagnostics or {})

        if qty <= 0:
            return ExecutableTradePlan(False, "size reject: qty <= 0", plan_id, thesis_id, side, risk.entry, risk.final_sl, route.target_price, 0.0, route.gross_rr, conviction, raw_tp, raw_tp_label, route, risk, diagnostics or {})

        return ExecutableTradePlan(
            executable=True,
            reason="approved",
            plan_id=plan_id,
            thesis_id=thesis_id,
            side=side,
            entry=risk.entry,
            sl=risk.final_sl,
            tp=route.target_price,
            qty=qty,
            rr=route.gross_rr,
            conviction=max(0.0, min(1.0, conviction)),
            raw_tp=raw_tp,
            raw_tp_label=raw_tp_label,
            routeability=route,
            risk=risk,
            diagnostics=diagnostics or {},
        )

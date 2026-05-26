"""Automatic outcome labels for observations and approved protection plans; never train only on executions."""
from __future__ import annotations
from dataclasses import dataclass, asdict
from collections import deque
from typing import Any
from core.identifiers import CostEstimate, ProtectionPlan
from core.research_store import ResearchStore

@dataclass(frozen=True)
class DeltaForwardLabel:
    observation_ts_ns: int
    horizon: str
    side: str
    gross_markout_bps: float
    spread_cost_bps: float
    fee_cost_bps: float
    slippage_estimate_bps: float
    impact_cost_bps: float
    protection_cost_bps: float
    net_executable_return_bps: float
    favorable_excursion_bps: float
    adverse_excursion_bps: float

@dataclass(frozen=True)
class LiquidityOutcomeLabel:
    observation_ts_ns: int
    instrument: str
    zone_id: str
    tp_before_sl: int
    sl_swept_before_favourable_move: int
    favorable_excursion_bps: float
    adverse_excursion_bps: float
    elapsed_seconds: float

@dataclass(frozen=True)
class UnderlyingOutcomeLabel:
    observation_ts_ns: int
    underlying: str
    horizon: str
    side: str
    move_before_invalidation: int
    realised_move_points: float
    favorable_excursion_points: float
    adverse_excursion_points: float


def calculate_delta_forward_label(*, observation_ts_ns: int, observation_mid: float, future_prices: list[float], horizon: str, side: str, cost: CostEstimate) -> DeltaForwardLabel:
    if observation_mid <= 0 or not future_prices:
        raise ValueError("observation and future prices are required")
    direction = 1.0 if side.upper() == "LONG" else -1.0
    returns = [direction * (price / observation_mid - 1.0) * 10_000.0 for price in future_prices]
    gross = returns[-1]
    return DeltaForwardLabel(observation_ts_ns, horizon, side.upper(), gross, cost.spread_bps, cost.fees_bps, cost.slippage_bps,
        cost.impact_bps, cost.protection_cost_bps, gross - cost.total_bps, max(returns), min(returns))

class ForwardLabelCoordinator:
    """Registers every valid Delta observation and emits after-cost labels as future ticks arrive."""
    def __init__(self, store: ResearchStore, horizons_seconds: tuple[int, ...] = (1, 10, 60, 300)) -> None:
        self.store = store
        self.horizons_ns = tuple(int(seconds * 1_000_000_000) for seconds in horizons_seconds)
        self.pending: deque[dict[str, Any]] = deque()
    def register_delta_observation(self, *, ts_ns: int, mid: float, cost: CostEstimate) -> None:
        for side in ("LONG", "SHORT"):
            self.pending.append({"ts": ts_ns, "mid": mid, "side": side, "cost": cost, "prices": [(ts_ns, mid)], "pending": set(self.horizons_ns)})
    def on_delta_mid(self, ts_ns: int, mid: float) -> list[DeltaForwardLabel]:
        emitted: list[DeltaForwardLabel] = []
        retained: deque[dict[str, Any]] = deque()
        while self.pending:
            row = self.pending.popleft()
            row["prices"].append((ts_ns, mid))
            due = [h for h in row["pending"] if ts_ns >= row["ts"] + h]
            for horizon in due:
                future = [price for tick, price in row["prices"] if tick <= row["ts"] + horizon]
                label = calculate_delta_forward_label(observation_ts_ns=row["ts"], observation_mid=row["mid"], future_prices=future,
                    horizon=f"{int(horizon/1_000_000_000)}s", side=row["side"], cost=row["cost"])
                self.store.append("labels", asdict(label))
                emitted.append(label)
                row["pending"].remove(horizon)
            if row["pending"]:
                retained.append(row)
        self.pending = retained
        return emitted

class OutcomeCoordinator:
    """Emits liquidity TP/SL and underlying direction labels from later observed prices."""
    def __init__(self, store: ResearchStore, max_plan_horizon_seconds: int = 3600) -> None:
        self.store = store
        self.max_ns = int(max_plan_horizon_seconds * 1_000_000_000)
        self.plans: deque[dict[str, Any]] = deque()
        self.underlying: deque[dict[str, Any]] = deque()
    def register_liquidity_plan(self, *, ts_ns: int, instrument: str, direction: str, zone_id: str, plan: ProtectionPlan) -> None:
        self.plans.append({"ts": ts_ns, "instrument": instrument, "direction": direction, "zone_id": zone_id, "plan": plan, "prices": [plan.entry_price]})
    def on_price(self, *, ts_ns: int, instrument: str, price: float) -> list[LiquidityOutcomeLabel]:
        output: list[LiquidityOutcomeLabel] = []
        kept: deque[dict[str, Any]] = deque()
        for row in self.plans:
            if row["instrument"] != instrument:
                kept.append(row)
                continue
            row["prices"].append(price)
            sign = 1.0 if row["direction"] in {"LONG", "BULLISH"} else -1.0
            plan: ProtectionPlan = row["plan"]
            returns = [sign * (p / plan.entry_price - 1.0) * 10_000.0 for p in row["prices"]]
            tp = price >= plan.target_price if sign > 0 else price <= plan.target_price
            sl = price <= plan.stop_price if sign > 0 else price >= plan.stop_price
            expired = ts_ns >= row["ts"] + self.max_ns
            if tp or sl or expired:
                label = LiquidityOutcomeLabel(row["ts"], instrument, row["zone_id"], int(tp and not sl), int(sl and max(returns) <= 0), max(returns), min(returns), (ts_ns-row["ts"])/1e9)
                self.store.append("labels", asdict(label))
                output.append(label)
            else:
                kept.append(row)
        self.plans = kept
        return output
    def register_underlying(self, *, ts_ns: int, underlying: str, side: str, entry: float, invalidation: float, target: float, horizon_seconds: int) -> None:
        self.underlying.append({"ts":ts_ns,"underlying":underlying,"side":side,"entry":entry,"invalidation":invalidation,"target":target,"horizon":int(horizon_seconds*1e9),"prices":[entry]})
    def on_underlying_price(self, *, ts_ns: int, underlying: str, price: float) -> list[UnderlyingOutcomeLabel]:
        output: list[UnderlyingOutcomeLabel] = []
        kept: deque[dict[str, Any]] = deque()
        for row in self.underlying:
            if row["underlying"] != underlying:
                kept.append(row)
                continue
            row["prices"].append(price)
            sign = 1.0 if row["side"] == "BULLISH" else -1.0
            pnl = [sign * (p-row["entry"]) for p in row["prices"]]
            hit_target = price >= row["target"] if sign > 0 else price <= row["target"]
            hit_invalid = price <= row["invalidation"] if sign > 0 else price >= row["invalidation"]
            due = ts_ns >= row["ts"] + row["horizon"]
            if hit_target or hit_invalid or due:
                label = UnderlyingOutcomeLabel(row["ts"], underlying, f"{int(row['horizon']/1e9)}s", row["side"], int(hit_target and not hit_invalid), sign*(price-row["entry"]), max(pnl), min(pnl))
                self.store.append("labels", asdict(label))
                output.append(label)
            else:
                kept.append(row)
        self.underlying = kept
        return output

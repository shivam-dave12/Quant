"""Institutional state-dependent protection geometry.

This module owns exit geometry only.  It never invents market observations and
never routes an unprotected order.  The strategy supplies live signal/market
observations; the builder returns a venue-compatible hard protective plan plus
transparent diagnostics for alpha decay, price impact and flow toxicity.

Models implemented:
* AR(1) signal-decay half life and cost-crossing optimal hold horizon.
* Kyle-style linear impact regression: microprice return (bps) on signed OFI USD.
* VPIN-style equal-volume bucket imbalance from signed trade notional USD.
* Almgren-Chriss-style liquidation trajectory diagnostics for a known size.
* Long-option Greek exit diagnostics (delta/IV/theta/VRP), without mixing the
  NIFTY-underlying signal domain with option-premium protection prices.
"""
from __future__ import annotations

from collections import deque
from dataclasses import asdict, dataclass
import math
import statistics
import time
from typing import Any, Iterable, Mapping

from strategy.domain import Direction, ProtectionPlan

try:
    import config
except Exception:  # pragma: no cover
    config = None  # type: ignore


def _cfg(name: str, default: Any) -> Any:
    return getattr(config, name, default) if config is not None else default


def _num(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        return out if math.isfinite(out) else default
    except Exception:
        return default


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, float(value)))


def _lookup_float_map(name: str, keys: Iterable[str], default: float = 0.0) -> float:
    raw = _cfg(name, {})
    if not isinstance(raw, Mapping):
        return float(default)
    for key in keys:
        if key in raw:
            return _num(raw.get(key), default)
    return float(default)


@dataclass(frozen=True)
class SignalDecayEstimate:
    ready: bool
    sample_count: int
    phi: float | None = None
    observation_interval_sec: float | None = None
    half_life_sec: float | None = None
    optimal_hold_sec: float | None = None
    reason: str = ""


@dataclass(frozen=True)
class KyleImpactEstimate:
    ready: bool
    sample_count: int
    lambda_bps_per_usd: float | None = None
    exit_notional: float = 0.0
    expected_exit_impact_bps: float | None = None
    reason: str = ""


@dataclass(frozen=True)
class VpinToxicityEstimate:
    ready: bool
    bucket_count: int
    bucket_volume_usd: float | None = None
    vpin: float | None = None
    stop_multiplier: float = 1.0
    reason: str = ""


@dataclass(frozen=True)
class LiquidationSlice:
    delay_sec: float
    quantity: float
    order_style: str


@dataclass(frozen=True)
class AlmgrenChrissPlan:
    ready: bool
    urgency: float
    horizon_sec: float
    slices: tuple[LiquidationSlice, ...]
    estimated_impact_bps: float | None = None
    reason: str = ""


@dataclass(frozen=True)
class OptionExitDiagnostics:
    exit_required: bool
    reasons: tuple[str, ...]
    abs_delta: float | None
    iv_change_abs: float | None
    theta_to_premium_per_day: float | None
    theta_limit_per_day: float | None
    vrp: float | None


class DynamicProtectionPlanBuilder:
    """Per-instrument stateful estimator and hard-protection-plan builder."""

    def __init__(self, asset_id: str = "") -> None:
        self.asset_id = str(asset_id or "")
        max_samples = int(_cfg("DYNAMIC_PROTECTION_MAX_SIGNAL_SAMPLES", 360))
        self._signal_samples: deque[tuple[float, float]] = deque(maxlen=max(30, max_samples))
        self._book_samples: deque[dict[str, float]] = deque(maxlen=1200)
        self._trade_events: deque[dict[str, float]] = deque(maxlen=4000)
        self._book_seen: set[tuple[float, float, float]] = set()
        self._trade_seen: set[tuple[float, float]] = set()
        self._last_signal_sample_ts = 0.0

    def observe(
        self,
        *,
        signal_bps: float | None,
        timestamp_s: float | None = None,
        research_state: Mapping[str, Any] | None = None,
    ) -> None:
        """Record actual live observations; no synthetic fill/tape values are used."""
        now = float(timestamp_s or time.time())
        interval = max(0.05, float(_cfg("DYNAMIC_PROTECTION_SIGNAL_SAMPLE_SEC", 1.0)))
        if signal_bps is not None and math.isfinite(float(signal_bps)) and now - self._last_signal_sample_ts >= interval:
            self._signal_samples.append((now, float(signal_bps)))
            self._last_signal_sample_ts = now
        if not isinstance(research_state, Mapping):
            return
        for row in research_state.get("book_events", []) or []:
            if not isinstance(row, Mapping):
                continue
            ts = _num(row.get("timestamp_s"), 0.0)
            micro = _num(row.get("microprice"), 0.0)
            signed_ofi = _num(row.get("signed_ofi_usd"), 0.0)
            key = (ts, micro, signed_ofi)
            if ts > 0 and micro > 0 and key not in self._book_seen:
                self._book_seen.add(key)
                self._book_samples.append({"timestamp_s": ts, "microprice": micro, "signed_ofi_usd": signed_ofi})
        for row in research_state.get("trade_events", []) or []:
            if not isinstance(row, Mapping):
                continue
            ts = _num(row.get("timestamp_s"), 0.0)
            signed_usd = _num(row.get("signed_notional_usd"), 0.0)
            key = (ts, signed_usd)
            if ts > 0 and signed_usd and key not in self._trade_seen:
                self._trade_seen.add(key)
                self._trade_events.append({"timestamp_s": ts, "signed_notional_usd": signed_usd})

    def signal_decay(self, *, current_edge_bps: float, total_exit_cost_bps: float) -> SignalDecayEstimate:
        samples = list(self._signal_samples)
        minimum = max(4, int(_cfg("DYNAMIC_PROTECTION_MIN_SIGNAL_OBSERVATIONS", 12)))
        if len(samples) < minimum:
            return SignalDecayEstimate(False, len(samples), reason=f"signal_half_life_warmup:{len(samples)}/{minimum}")
        xs = [row[1] for row in samples]
        mean = statistics.fmean(xs)
        prior = [x - mean for x in xs[:-1]]
        nxt = [x - mean for x in xs[1:]]
        denom = sum(x * x for x in prior)
        if denom <= 1e-12:
            return SignalDecayEstimate(False, len(samples), reason="signal_half_life_variance_unavailable")
        phi = sum(x * y for x, y in zip(prior, nxt)) / denom
        dts = [samples[i][0] - samples[i - 1][0] for i in range(1, len(samples)) if samples[i][0] > samples[i - 1][0]]
        dt = statistics.median(dts) if dts else float(_cfg("DYNAMIC_PROTECTION_SIGNAL_SAMPLE_SEC", 1.0))
        minimum_half_life = float(_cfg("DYNAMIC_PROTECTION_MIN_HALF_LIFE_SEC", 2.0))
        maximum_half_life = float(_cfg("DYNAMIC_PROTECTION_MAX_HALF_LIFE_SEC", 3600.0))
        if phi <= 0.0:
            # A sign-reversing/nonpersistent signal decays inside one sampling interval.
            half_life = minimum_half_life
            estimate_reason = "ar1_nonpersistent_signal"
        elif phi >= 0.999999:
            # Persistence beyond the rolling window is represented conservatively by
            # the configured maximum horizon rather than incorrectly blocking a trade.
            half_life = maximum_half_life
            estimate_reason = "ar1_persistent_signal_lower_bound"
        else:
            half_life = -math.log(2.0) * dt / math.log(phi)
            half_life = _clamp(half_life, minimum_half_life, maximum_half_life)
            estimate_reason = "ar1_cost_crossing_horizon"
        cost = max(float(total_exit_cost_bps), 1e-9)
        edge = max(float(current_edge_bps), 0.0)
        hold = 0.0 if edge <= cost else (half_life / math.log(2.0)) * math.log(edge / cost)
        hold = _clamp(hold, 0.0, float(_cfg("DYNAMIC_PROTECTION_MAX_OPTIMAL_HOLD_SEC", 3600.0)))
        return SignalDecayEstimate(True, len(samples), phi=phi, observation_interval_sec=dt, half_life_sec=half_life, optimal_hold_sec=hold, reason=estimate_reason)

    def kyle_impact(self, *, exit_notional: float) -> KyleImpactEstimate:
        rows = list(self._book_samples)
        minimum = max(6, int(_cfg("DYNAMIC_PROTECTION_MIN_KYLE_OBSERVATIONS", 20)))
        if len(rows) < minimum:
            return KyleImpactEstimate(False, len(rows), exit_notional=float(exit_notional or 0.0), reason=f"kyle_lambda_warmup:{len(rows)}/{minimum}")
        x: list[float] = []
        y: list[float] = []
        for prev, cur in zip(rows[:-1], rows[1:]):
            prev_px = prev["microprice"]
            cur_px = cur["microprice"]
            if prev_px <= 0:
                continue
            x.append(cur["signed_ofi_usd"])
            y.append((cur_px / prev_px - 1.0) * 10000.0)
        if len(x) < minimum - 1:
            return KyleImpactEstimate(False, len(x), exit_notional=float(exit_notional or 0.0), reason="kyle_lambda_insufficient_pairs")
        mx, my = statistics.fmean(x), statistics.fmean(y)
        denom = sum((v - mx) ** 2 for v in x)
        if denom <= 1e-12:
            return KyleImpactEstimate(False, len(x), exit_notional=float(exit_notional or 0.0), reason="kyle_lambda_ofi_variance_unavailable")
        slope = sum((vx - mx) * (vy - my) for vx, vy in zip(x, y)) / denom
        # Adverse impact is a cost; do not turn a negative/noisy coefficient into alpha.
        lambda_bps_per_usd = max(0.0, slope)
        impact = lambda_bps_per_usd * max(0.0, float(exit_notional or 0.0))
        return KyleImpactEstimate(True, len(x), lambda_bps_per_usd=lambda_bps_per_usd, exit_notional=float(exit_notional or 0.0), expected_exit_impact_bps=impact, reason="microprice_bps_on_signed_ofi_usd")

    def vpin(self) -> VpinToxicityEstimate:
        trades = list(self._trade_events)
        minimum_buckets = max(3, int(_cfg("DYNAMIC_PROTECTION_MIN_VPIN_BUCKETS", 5)))
        if len(trades) < minimum_buckets * 2:
            return VpinToxicityEstimate(False, 0, reason="vpin_trade_tape_warmup")
        abs_sizes = [abs(row["signed_notional_usd"]) for row in trades if abs(row["signed_notional_usd"]) > 0]
        if not abs_sizes:
            return VpinToxicityEstimate(False, 0, reason="vpin_trade_tape_empty")
        target_bucket_count = max(minimum_buckets, int(_cfg("DYNAMIC_PROTECTION_VPIN_WINDOW_BUCKETS", 20)))
        bucket_volume = max(statistics.median(abs_sizes) * 4.0, sum(abs_sizes) / max(target_bucket_count, 1))
        buckets: list[float] = []
        buy = sell = filled = 0.0
        for row in trades:
            remaining = abs(row["signed_notional_usd"])
            side_buy = row["signed_notional_usd"] > 0
            while remaining > 1e-9:
                allocation = min(remaining, bucket_volume - filled)
                if side_buy:
                    buy += allocation
                else:
                    sell += allocation
                filled += allocation
                remaining -= allocation
                if filled >= bucket_volume - 1e-9:
                    buckets.append(abs(buy - sell) / max(buy + sell, 1e-9))
                    buy = sell = filled = 0.0
        if len(buckets) < minimum_buckets:
            return VpinToxicityEstimate(False, len(buckets), bucket_volume_usd=bucket_volume, reason=f"vpin_bucket_warmup:{len(buckets)}/{minimum_buckets}")
        window = buckets[-target_bucket_count:]
        vpin = statistics.fmean(window)
        min_mult = float(_cfg("DYNAMIC_PROTECTION_VPIN_STOP_MULT_MIN", 0.80))
        max_mult = float(_cfg("DYNAMIC_PROTECTION_VPIN_STOP_MULT_MAX", 1.80))
        # Neutral toxicity is close to one; genuinely imbalanced flow widens disaster protection.
        stop_mult = _clamp(0.75 + 1.25 * vpin, min_mult, max_mult)
        return VpinToxicityEstimate(True, len(window), bucket_volume_usd=bucket_volume, vpin=vpin, stop_multiplier=stop_mult, reason="equal_volume_signed_notional_buckets")

    def almgren_chriss(self, *, quantity: float, half_life_sec: float | None, expected_impact_bps: float | None) -> AlmgrenChrissPlan:
        qty = max(0.0, float(quantity or 0.0))
        if qty <= 0 or half_life_sec is None or half_life_sec <= 0:
            return AlmgrenChrissPlan(False, 0.0, 0.0, (), reason="liquidation_size_or_half_life_unavailable")
        horizon = min(float(half_life_sec), float(_cfg("DYNAMIC_PROTECTION_MAX_LIQUIDATION_HORIZON_SEC", 300.0)))
        step_sec = max(1.0, float(_cfg("DYNAMIC_PROTECTION_LIQUIDATION_STEP_SEC", 15.0)))
        steps = max(1, min(int(_cfg("DYNAMIC_PROTECTION_MAX_LIQUIDATION_STEPS", 8)), int(math.ceil(horizon / step_sec))))
        risk_aversion = max(1e-9, float(_cfg("DYNAMIC_PROTECTION_AC_RISK_AVERSION", 1.0)))
        impact = max(0.0, float(expected_impact_bps or 0.0))
        urgency = _clamp((step_sec * steps) / max(half_life_sec, step_sec) + risk_aversion * impact / 10.0, 0.0, 1.0)
        kappa = max(1e-6, urgency * 2.0)
        raw_weights = [math.exp(-kappa * i / max(steps - 1, 1)) for i in range(steps)]
        total = sum(raw_weights)
        style = "AGGRESSIVE_LIMIT_OR_MARKETABLE" if urgency >= 0.70 else "PASSIVE_LIMIT_WITH_CANCEL_REPRICE"
        slices = tuple(LiquidationSlice(delay_sec=i * horizon / steps, quantity=qty * w / total, order_style=style) for i, w in enumerate(raw_weights))
        return AlmgrenChrissPlan(True, urgency, horizon, slices, estimated_impact_bps=expected_impact_bps, reason="impact_risk_liquidation_schedule")

    def build_plan(
        self,
        *,
        direction: Direction,
        entry_price: float,
        volatility_price: float,
        gross_edge_bps: float,
        execution_cost_bps: float,
        protection_type: str,
        asset_class: str,
        position_notional: float = 0.0,
        quantity: float = 0.0,
        option_state: Mapping[str, Any] | None = None,
        market_state: Mapping[str, Any] | None = None,
    ) -> ProtectionPlan:
        price = max(0.0, float(entry_price or 0.0))
        if price <= 0 or volatility_price <= 0:
            return ProtectionPlan(price, price, price, protection_type, False, ["dynamic_protection_volatility_unavailable"], diagnostics={})
        market = dict(market_state or {})
        asset_key = str(market.get("asset_id") or self.asset_id or asset_class or "").upper()
        venue_key = str(market.get("venue") or "").lower()
        impact = self.kyle_impact(exit_notional=position_notional) if asset_class != "option" else KyleImpactEstimate(False, 0, exit_notional=position_notional, reason="option_feed_has_no_signed_ofi_impact_estimator")
        impact_bps = float(impact.expected_exit_impact_bps or 0.0)
        total_exit_cost_bps = max(1e-9, float(execution_cost_bps or 0.0) + impact_bps)
        decay = self.signal_decay(current_edge_bps=gross_edge_bps, total_exit_cost_bps=total_exit_cost_bps)
        toxicity = self.vpin() if asset_class != "option" else VpinToxicityEstimate(False, 0, reason="option_feed_has_no_signed_trade_tape_vpin")
        require_decay = bool(_cfg("DYNAMIC_PROTECTION_REQUIRE_SIGNAL_DECAY_READY", True))
        if require_decay and not decay.ready:
            diagnostics = {"signal_decay": asdict(decay), "kyle_impact": asdict(impact), "vpin": asdict(toxicity)}
            return ProtectionPlan(price, price, price, protection_type, False, [decay.reason], diagnostics=diagnostics)
        # Kyle and VPIN readiness are venue-specific. The previous implementation
        # named these policies *_FOR_DELTA but accidentally blocked Hyperliquid and
        # CoinSwitch forever when their selected feed had no Delta-style event tape.
        kyle_venues_raw = _cfg("DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_VENUES", ("delta",))
        toxic_venues_raw = _cfg("DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_VENUES", ("delta",))
        kyle_venues = {str(v).strip().lower() for v in (kyle_venues_raw if isinstance(kyle_venues_raw, (tuple, list, set)) else str(kyle_venues_raw).split(",")) if str(v).strip()}
        toxic_venues = {str(v).strip().lower() for v in (toxic_venues_raw if isinstance(toxic_venues_raw, (tuple, list, set)) else str(toxic_venues_raw).split(",")) if str(v).strip()}
        require_kyle_here = asset_class != "option" and venue_key in kyle_venues
        require_toxicity_here = asset_class != "option" and venue_key in toxic_venues
        if require_kyle_here and not impact.ready:
            diagnostics = {"signal_decay": asdict(decay), "kyle_impact": asdict(impact), "vpin": asdict(toxicity), "required_kyle_venues": sorted(kyle_venues)}
            return ProtectionPlan(price, price, price, protection_type, False, [impact.reason], diagnostics=diagnostics)
        if require_toxicity_here and not toxicity.ready:
            diagnostics = {"signal_decay": asdict(decay), "kyle_impact": asdict(impact), "vpin": asdict(toxicity), "required_toxicity_venues": sorted(toxic_venues)}
            return ProtectionPlan(price, price, price, protection_type, False, [toxicity.reason], diagnostics=diagnostics)
        stop_mult = toxicity.stop_multiplier if toxicity.ready else 1.0
        geometry_enabled = bool(_cfg("DYNAMIC_PROTECTION_MARKET_AWARE_GEOMETRY_ENABLED", True))
        base_min_stop_bps = float(_cfg("DYNAMIC_PROTECTION_MIN_STOP_BPS", 8.0))
        asset_min_stop_bps = _lookup_float_map("DYNAMIC_PROTECTION_ASSET_MIN_STOP_BPS", (asset_key,), 0.0)
        venue_min_stop_bps = _lookup_float_map(
            "DYNAMIC_PROTECTION_VENUE_ASSET_MIN_STOP_BPS",
            (f"{venue_key}:{asset_key}", venue_key, asset_key),
            0.0,
        )
        min_stop_bps = max(base_min_stop_bps, asset_min_stop_bps, venue_min_stop_bps)
        vol_floor = float(volatility_price) * float(_cfg("DYNAMIC_PROTECTION_VOL_STOP_MULT", 1.25))
        min_bps_floor = price * min_stop_bps / 10000.0
        spread_bps = max(0.0, _num(market.get("spread_bps"), 0.0))
        tick_size = max(0.0, _num(market.get("price_tick"), 0.0))
        near_depth = max(0.0, _num(market.get("near_touch_depth_usd"), 0.0))
        spread_floor = price * spread_bps * float(_cfg("DYNAMIC_PROTECTION_SPREAD_STOP_MULT", 6.0)) / 10000.0 if geometry_enabled else 0.0
        tick_floor = tick_size * float(_cfg("DYNAMIC_PROTECTION_MIN_STOP_TICKS", 12.0)) if geometry_enabled else 0.0
        cost_floor = price * total_exit_cost_bps * float(_cfg("DYNAMIC_PROTECTION_COST_STOP_MULT", 2.5)) / 10000.0 if geometry_enabled else 0.0
        depth_floor = 0.0
        if geometry_enabled and asset_class != "option" and position_notional > 0:
            min_coverage = max(1e-9, float(_cfg("DYNAMIC_PROTECTION_DEPTH_STRESS_MIN_COVERAGE", 4.0)))
            stress_bps = max(0.0, float(_cfg("DYNAMIC_PROTECTION_DEPTH_STRESS_STOP_BPS", 18.0)))
            coverage = near_depth / max(float(position_notional), 1e-9)
            if coverage < min_coverage:
                depth_floor = price * stress_bps * (1.0 - max(0.0, coverage) / min_coverage) / 10000.0
        base_stop = max(vol_floor, min_bps_floor, spread_floor, tick_floor, cost_floor, depth_floor)
        stop_distance = base_stop * stop_mult
        floor_rr = float(_cfg("DYNAMIC_PROTECTION_OPTION_RR_FLOOR", 1.10) if asset_class == "option" else _cfg("DYNAMIC_PROTECTION_RR_FLOOR", 1.15))
        policy_min_rr = _num(market.get("policy_min_rr"), 0.0)
        if policy_min_rr > 0:
            floor_rr = max(floor_rr, policy_min_rr)
        ceiling_rr = float(_cfg("DYNAMIC_PROTECTION_RR_CAP", 5.0))
        policy_max_rr = _num(market.get("policy_max_rr"), 0.0)
        if policy_max_rr > 0:
            ceiling_rr = min(ceiling_rr, max(policy_max_rr, floor_rr))
        ceiling_rr = max(floor_rr, ceiling_rr)
        edge_scalar = _clamp(float(gross_edge_bps or 0.0) / total_exit_cost_bps, floor_rr, ceiling_rr)
        min_target_bps = _lookup_float_map("DYNAMIC_PROTECTION_ASSET_MIN_TARGET_BPS", (asset_key,), 0.0)
        min_target_rr = (price * min_target_bps / 10000.0) / max(stop_distance, 1e-9) if min_target_bps > 0 else 0.0
        target_rr = _clamp(max(edge_scalar, min_target_rr), floor_rr, ceiling_rr)
        target_distance = stop_distance * target_rr
        if direction in {Direction.LONG, Direction.BULLISH}:
            stop_price = price - stop_distance
            target_price = price + target_distance
        elif direction in {Direction.SHORT, Direction.BEARISH}:
            stop_price = price + stop_distance
            target_price = price - target_distance
        else:
            return ProtectionPlan(price, price, price, protection_type, False, ["dynamic_protection_direction_unavailable"], diagnostics={})
        liquidation = self.almgren_chriss(quantity=quantity, half_life_sec=decay.half_life_sec, expected_impact_bps=impact.expected_exit_impact_bps)
        diagnostics: dict[str, Any] = {
            "model": "dynamic_exit_state_v1",
            "asset_class": asset_class,
            "base_volatility_stop_distance": base_stop,
            "stop_distance": stop_distance,
            "target_distance": target_distance,
            "edge_scalar_rr": edge_scalar,
            "target_rr": target_rr,
            "gross_edge_bps": gross_edge_bps,
            "execution_cost_bps": execution_cost_bps,
            "total_exit_cost_including_impact_bps": total_exit_cost_bps,
            "market_geometry": {
                "enabled": geometry_enabled,
                "asset_id": asset_key,
                "venue": venue_key,
                "spread_bps": spread_bps,
                "price_tick": tick_size,
                "near_touch_depth_usd": near_depth,
                "min_stop_bps": min_stop_bps,
                "volatility_floor_distance": vol_floor,
                "min_bps_floor_distance": min_bps_floor,
                "spread_floor_distance": spread_floor,
                "tick_floor_distance": tick_floor,
                "cost_floor_distance": cost_floor,
                "depth_floor_distance": depth_floor,
                "policy_min_rr": policy_min_rr,
                "policy_max_rr": policy_max_rr,
                "asset_min_target_bps": min_target_bps,
            },
            "signal_decay": asdict(decay),
            "kyle_impact": asdict(impact),
            "vpin": asdict(toxicity),
            "almgren_chriss": asdict(liquidation),
        }
        if option_state:
            diagnostics["option_state_at_entry"] = dict(option_state)
        return ProtectionPlan(
            entry_price=price,
            stop_price=max(0.01, stop_price),
            target_price=max(0.01, target_price),
            protection_type=protection_type,
            protection_feasible=stop_price > 0 and target_price > 0,
            reasons=["dynamic_state_dependent_protection", "hard_protection_required_at_entry"],
            diagnostics=diagnostics,
        )

    @staticmethod
    def option_exit_diagnostics(
        *,
        abs_delta: float | None,
        current_iv: float | None,
        entry_iv: float | None,
        theta_to_premium_per_day: float | None,
        dte: float | None,
        vrp: float | None,
    ) -> OptionExitDiagnostics:
        reasons: list[str] = []
        delta = None if abs_delta is None else abs(float(abs_delta))
        if delta is not None and delta < float(_cfg("DYNAMIC_OPTION_EXIT_MIN_ABS_DELTA", 0.10)):
            reasons.append("option_delta_exposure_collapsed")
        iv_change = None
        if current_iv is not None and entry_iv is not None:
            iv_change = float(entry_iv) - float(current_iv)
            if iv_change >= float(_cfg("DYNAMIC_OPTION_EXIT_IV_COLLAPSE_ABS", 0.02)):
                reasons.append("option_iv_collapse")
        theta_limit = float(_cfg("DYNAMIC_OPTION_EXIT_THETA_TO_PREMIUM_PER_DAY", 0.08))
        if vrp is not None and float(vrp) <= float(_cfg("DYNAMIC_OPTION_CHEAP_VRP_THRESHOLD", -0.10)):
            theta_limit = float(_cfg("DYNAMIC_OPTION_CHEAP_VRP_THETA_LIMIT", 0.12))
        theta = None if theta_to_premium_per_day is None else abs(float(theta_to_premium_per_day))
        if theta is not None and dte is not None and float(dte) < float(_cfg("DYNAMIC_OPTION_THETA_DTE_LIMIT", 5.0)) and theta > theta_limit:
            reasons.append("option_theta_carry_structurally_excessive")
        return OptionExitDiagnostics(bool(reasons), tuple(reasons), delta, iv_change, theta, theta_limit, None if vrp is None else float(vrp))

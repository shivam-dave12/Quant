"""
strategy/entry_engine.py

Full institutional entry engine.

Rules:
- No synthetic levels.
- No fallback target.
- No momentum-only entries.
- No approach entries.
- Only post-sweep reversal/continuation with explicit confirmation.
- Refined entry cannot survive into a new sweep thesis.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
import logging
import math
import time

try:
    from strategy.liquidity_map import LiquidityMap, LiquidityMapSnapshot, SweepEvent, LiquidityPool
    from strategy.institutional_lifecycle import InstitutionalLifecycle, PullbackDiagnostics, finite, risk_points, reward_points
except Exception:
    from .liquidity_map import LiquidityMap, LiquidityMapSnapshot, SweepEvent, LiquidityPool
    from .institutional_lifecycle import InstitutionalLifecycle, PullbackDiagnostics, finite, risk_points, reward_points

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MarketState:
    price: float
    candles_by_tf: Dict[str, List[Any]]
    timestamp: float = 0.0
    session: str = ""
    killzone: str = ""
    order_flow_score: float = 0.0
    cvd_score: float = 0.0
    tick_score: float = 0.0
    htf_bias_score: float = 0.0
    pd_position: float = 0.5
    cisd_confirmed: bool = False
    ote_confirmed: bool = False
    displacement_atr: float = 0.0
    acceptance_confirmed: bool = False
    toxicity_score: float = 0.0


@dataclass
class EntrySignal:
    side: str
    entry_type: str
    entry_price: float
    sl_price: float
    tp_price: float
    rr_ratio: float
    target_pool: LiquidityPool
    sweep_result: SweepEvent
    conviction: float
    reason: str
    ict_validation: str
    thesis_id: str
    raw_tp_label: str
    route_probability: float
    route_ev_r: float
    route_utility: float
    diagnostics: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PendingRefinedEntry:
    signal: EntrySignal
    created_at: float
    expires_at: float
    best_extension_price: float
    attempts: int = 0
    last_eval_at: float = 0.0
    last_reason: str = ""


@dataclass(frozen=True)
class EntryEngineConfig:
    tick_size: float = 0.1
    sl_buffer_atr: float = 0.12
    noise_floor_atr: float = 0.45
    min_sweep_quality: float = 0.52
    min_conviction: float = 0.55
    min_rr: float = 1.8
    min_target_probability: float = 0.18
    min_net_ev_r: float = 0.05
    min_route_utility: float = 0.0
    min_risk_atr: float = 0.45
    max_risk_atr: float = 3.50
    round_trip_fee_rate: float = 0.00110
    slippage_atr: float = 0.03
    max_chase_atr_without_ote: float = 1.15
    require_cisd_or_ote: bool = True
    allow_displacement_without_ote: bool = False
    refined_ttl_sec: float = 900.0
    refined_retry_sec: float = 8.0
    refined_max_attempts: int = 18
    refined_min_pullback_atr: float = 0.25
    refined_min_delivery_atr: float = 0.35
    refined_min_risk_compression: float = 0.18


class EntryEngine:
    def __init__(self, config: Optional[EntryEngineConfig] = None, liquidity_map: Optional[LiquidityMap] = None) -> None:
        self.cfg = config or EntryEngineConfig()
        self.liquidity_map = liquidity_map or LiquidityMap()
        self.lifecycle = InstitutionalLifecycle(
            tick_size=self.cfg.tick_size,
            min_rr=self.cfg.min_rr,
            min_target_probability=self.cfg.min_target_probability,
            min_net_ev_r=self.cfg.min_net_ev_r,
            min_route_utility=self.cfg.min_route_utility,
            min_risk_atr=self.cfg.min_risk_atr,
            max_risk_atr=self.cfg.max_risk_atr,
            round_trip_fee_rate=self.cfg.round_trip_fee_rate,
            slippage_atr=self.cfg.slippage_atr,
        )
        self._active_thesis_id = ""
        self._pending_refined: Optional[PendingRefinedEntry] = None
        self.last_snapshot: Optional[LiquidityMapSnapshot] = None
        self.last_reject_reason: str = ""

    def evaluate(self, market: MarketState) -> Optional[EntrySignal]:
        snapshot = self.liquidity_map.build(market.candles_by_tf)
        self.last_snapshot = snapshot

        if snapshot.price <= 0 or snapshot.atr <= 0:
            return self._reject("market data/ATR invalid; no synthetic fallback")

        if not snapshot.pools:
            return self._reject("no real liquidity pools available; no trade")

        refined = self.evaluate_refined(market, snapshot)
        if refined is not None:
            return refined

        if not snapshot.sweeps:
            return self._reject("no confirmed sweep")

        sweep = self._select_sweep(snapshot.sweeps, market)
        if sweep is None:
            return self._reject("no sweep passed quality/context filter")

        side = sweep.reversal_side
        thesis_id = self.lifecycle.thesis_id(side=side, sweep=sweep, entry=market.price)
        self._on_new_thesis(thesis_id)

        signal = self._build_signal_from_sweep(market, snapshot, sweep, side, thesis_id)
        if signal is None:
            return None
        return signal

    # Compatibility aliases for existing bots.
    process = evaluate
    generate_signal = evaluate
    get_signal = evaluate

    def _reject(self, reason: str) -> None:
        self.last_reject_reason = reason
        logger.debug("ENTRY_REJECT %s", reason)
        return None

    def _on_new_thesis(self, thesis_id: str) -> None:
        if self._active_thesis_id and self._active_thesis_id != thesis_id and self._pending_refined is not None:
            logger.info(
                "REFINE_WATCH_INVALIDATED reason=new_sweep_context old=%s new=%s",
                self._pending_refined.signal.thesis_id,
                thesis_id,
            )
            self._pending_refined = None
        self._active_thesis_id = thesis_id

    def _select_sweep(self, sweeps: List[SweepEvent], market: MarketState) -> Optional[SweepEvent]:
        candidates = [s for s in sweeps if s.quality >= self.cfg.min_sweep_quality]
        if not candidates:
            return None

        def score(s: SweepEvent) -> float:
            side = s.reversal_side
            flow_align = self._flow_alignment(side, market)
            htf_align = self._htf_alignment(side, market)
            pd_align = self._pd_alignment(side, market)
            toxicity_penalty = max(0.0, min(1.0, market.toxicity_score)) * 0.30
            return s.quality * 0.50 + flow_align * 0.18 + htf_align * 0.15 + pd_align * 0.12 - toxicity_penalty

        best = max(candidates, key=score)
        return best

    def _flow_alignment(self, side: str, market: MarketState) -> float:
        raw = finite(market.order_flow_score) * 0.45 + finite(market.cvd_score) * 0.35 + finite(market.tick_score) * 0.20
        signed = raw if side == "long" else -raw
        return max(0.0, min(1.0, 0.5 + signed * 0.5))

    def _htf_alignment(self, side: str, market: MarketState) -> float:
        htf = finite(market.htf_bias_score)
        signed = htf if side == "long" else -htf
        return max(0.0, min(1.0, 0.5 + signed * 0.5))

    def _pd_alignment(self, side: str, market: MarketState) -> float:
        pd = max(0.0, min(1.0, finite(market.pd_position, 0.5)))
        # Longs preferred in discount, shorts in premium.
        return 1.0 - pd if side == "long" else pd

    def _build_signal_from_sweep(
        self,
        market: MarketState,
        snapshot: LiquidityMapSnapshot,
        sweep: SweepEvent,
        side: str,
        thesis_id: str,
    ) -> Optional[EntrySignal]:
        entry = finite(market.price)
        atr = finite(snapshot.atr)
        if entry <= 0 or atr <= 0:
            return self._reject("invalid entry/ATR")

        target = snapshot.target_for_side(side, entry)
        if target is None:
            return self._reject(f"no real opposite liquidity target for {side}; no synthetic TP")

        structural_sl = self._structural_sl(side, sweep, atr)
        noise_floor_sl = self._noise_floor_sl(side, entry, atr)
        raw_tp = target.price

        risk = risk_points(side, entry, structural_sl)
        reward = reward_points(side, entry, raw_tp)
        rr = reward / risk if risk > 0 else 0.0
        if risk <= 0 or reward <= 0:
            return self._reject("invalid entry/SL/TP geometry")

        confirmation_ok, confirmation_reason = self._confirmation_ok(side, market, sweep, snapshot)
        if not confirmation_ok:
            raw = EntrySignal(
                side=side,
                entry_type="post_sweep_wait",
                entry_price=entry,
                sl_price=structural_sl,
                tp_price=raw_tp,
                rr_ratio=rr,
                target_pool=target,
                sweep_result=sweep,
                conviction=0.0,
                reason=confirmation_reason,
                ict_validation="WAIT",
                thesis_id=thesis_id,
                raw_tp_label=f"{target.side}:{target.timeframe}:{target.price:.1f}",
                route_probability=0.0,
                route_ev_r=-1.0,
                route_utility=-1.0,
                diagnostics={"atr": atr, "confirmation": confirmation_reason},
            )
            self.arm_refined_watch(raw, reason=confirmation_reason)
            return self._reject(confirmation_reason)

        conviction = self._conviction(side, market, sweep, target, snapshot)
        if conviction < self.cfg.min_conviction:
            return self._reject(f"conviction {conviction:.2f} below minimum {self.cfg.min_conviction:.2f}")

        route_probability, route_ev_r, route_utility = self._route_stats(side, entry, structural_sl, raw_tp, conviction, target, snapshot)

        signal = EntrySignal(
            side=side,
            entry_type="post_sweep_reversal",
            entry_price=entry,
            sl_price=structural_sl,
            tp_price=raw_tp,
            rr_ratio=rr,
            target_pool=target,
            sweep_result=sweep,
            conviction=conviction,
            reason=f"{side.upper()} after {sweep.side} sweep; {confirmation_reason}",
            ict_validation=confirmation_reason,
            thesis_id=thesis_id,
            raw_tp_label=f"{target.side}:{target.timeframe}:{target.price:.1f}",
            route_probability=route_probability,
            route_ev_r=route_ev_r,
            route_utility=route_utility,
            diagnostics={
                "atr": atr,
                "sweep_quality": sweep.quality,
                "flow_alignment": self._flow_alignment(side, market),
                "htf_alignment": self._htf_alignment(side, market),
                "pd_alignment": self._pd_alignment(side, market),
                "structural_sl": structural_sl,
                "noise_floor_sl": noise_floor_sl,
            },
        )
        return signal

    def _structural_sl(self, side: str, sweep: SweepEvent, atr: float) -> float:
        buffer = atr * self.cfg.sl_buffer_atr
        if side == "long":
            return sweep.wick_price - buffer
        return sweep.wick_price + buffer

    def _noise_floor_sl(self, side: str, entry: float, atr: float) -> float:
        if side == "long":
            return entry - atr * self.cfg.noise_floor_atr
        return entry + atr * self.cfg.noise_floor_atr

    def _confirmation_ok(self, side: str, market: MarketState, sweep: SweepEvent, snapshot: LiquidityMapSnapshot) -> tuple[bool, str]:
        chase_atr = abs(finite(market.price) - sweep.pool.price) / max(snapshot.atr, 1e-12)
        if chase_atr > self.cfg.max_chase_atr_without_ote and not market.ote_confirmed:
            return False, f"extended chase {chase_atr:.2f}ATR without OTE"

        if self.cfg.require_cisd_or_ote:
            if market.cisd_confirmed:
                return True, "CISD"
            if market.ote_confirmed:
                return True, "OTE"
            if self.cfg.allow_displacement_without_ote and market.displacement_atr >= 1.25 and chase_atr <= self.cfg.max_chase_atr_without_ote:
                return True, "DISPLACEMENT_CONFIRMED"
            return False, "waiting for CISD/OTE confirmation"

        return True, "CONFIRMED"

    def _conviction(self, side: str, market: MarketState, sweep: SweepEvent, target: LiquidityPool, snapshot: LiquidityMapSnapshot) -> float:
        flow = self._flow_alignment(side, market)
        htf = self._htf_alignment(side, market)
        pd = self._pd_alignment(side, market)
        distance_atr = target.distance_atr(snapshot.price, snapshot.atr)
        target_quality = min(1.0, target.strength / 8.0)
        distance_quality = max(0.0, min(1.0, distance_atr / 2.0))
        toxicity_penalty = max(0.0, min(1.0, market.toxicity_score)) * 0.25

        return max(0.0, min(1.0, (
            sweep.quality * 0.30 +
            flow * 0.20 +
            htf * 0.15 +
            pd * 0.15 +
            target_quality * 0.10 +
            distance_quality * 0.10 -
            toxicity_penalty
        )))

    def _route_stats(
        self,
        side: str,
        entry: float,
        sl: float,
        tp: float,
        conviction: float,
        target: LiquidityPool,
        snapshot: LiquidityMapSnapshot,
    ) -> tuple[float, float, float]:
        risk = risk_points(side, entry, sl)
        reward = reward_points(side, entry, tp)
        if risk <= 0 or reward <= 0:
            return 0.0, -1.0, -1.0

        rr = reward / risk
        distance_atr = abs(tp - entry) / max(snapshot.atr, 1e-12)
        target_quality = min(1.0, target.strength / 8.0)

        probability = max(0.0, min(1.0, 0.10 + conviction * 0.45 + target_quality * 0.25 - max(0.0, distance_atr - 3.0) * 0.04))
        ev_r = probability * rr - (1.0 - probability)
        utility = ev_r * (0.65 + conviction * 0.35)
        return probability, ev_r, utility

    def arm_refined_watch(self, signal: EntrySignal, reason: str) -> None:
        now = time.time()
        best_extension = signal.entry_price
        self._pending_refined = PendingRefinedEntry(
            signal=signal,
            created_at=now,
            expires_at=now + self.cfg.refined_ttl_sec,
            best_extension_price=best_extension,
            last_reason=reason,
        )
        logger.info(
            "REFINE_WATCH_ARMED thesis=%s side=%s entry=%.1f sl=%.1f raw_tp=%.1f reason=%s",
            signal.thesis_id, signal.side, signal.entry_price, signal.sl_price, signal.tp_price, reason,
        )

    def evaluate_refined(self, market: MarketState, snapshot: LiquidityMapSnapshot) -> Optional[EntrySignal]:
        pending = self._pending_refined
        if pending is None:
            return None

        now = time.time()
        if now >= pending.expires_at:
            logger.info("REFINE_WATCH_INVALIDATED reason=expired thesis=%s", pending.signal.thesis_id)
            self._pending_refined = None
            return None

        if pending.signal.thesis_id != self._active_thesis_id:
            logger.info("REFINE_WATCH_INVALIDATED reason=stale thesis=%s active=%s", pending.signal.thesis_id, self._active_thesis_id)
            self._pending_refined = None
            return None

        if pending.attempts >= self.cfg.refined_max_attempts:
            logger.info("REFINE_WATCH_INVALIDATED reason=max_attempts thesis=%s", pending.signal.thesis_id)
            self._pending_refined = None
            return None

        if now - pending.last_eval_at < self.cfg.refined_retry_sec:
            return None

        side = pending.signal.side
        price = finite(market.price)
        if side == "long":
            pending.best_extension_price = max(pending.best_extension_price, price)
            sweep_price = pending.signal.sweep_result.pool.price
        else:
            pending.best_extension_price = min(pending.best_extension_price, price)
            sweep_price = pending.signal.sweep_result.pool.price

        diag = self.lifecycle.pullback_diagnostics(
            side=side,
            current_price=price,
            original_entry=pending.signal.entry_price,
            original_sl=pending.signal.sl_price,
            best_extension_price=pending.best_extension_price,
            sweep_price=sweep_price,
            atr=snapshot.atr,
        )

        pending.attempts += 1
        pending.last_eval_at = now
        pending.last_reason = diag.reason

        raw_ok = diag.raw_pullback_atr >= self.cfg.refined_min_pullback_atr
        delivery_ok = diag.delivery_retrace_atr >= self.cfg.refined_min_pullback_atr and diag.delivery_atr >= self.cfg.refined_min_delivery_atr
        compression_ok = diag.risk_compression >= self.cfg.refined_min_risk_compression

        if not ((delivery_ok and compression_ok) or (raw_ok and compression_ok and diag.delivery_atr >= self.cfg.refined_min_delivery_atr)):
            logger.info("REFINE_WAIT thesis=%s %s", pending.signal.thesis_id, diag.reason)
            return None

        original = pending.signal
        risk = risk_points(side, price, original.sl_price)
        reward = reward_points(side, price, original.tp_price)
        rr = reward / risk if risk > 0 else 0.0
        if rr < self.cfg.min_rr:
            logger.info("REFINE_WAIT thesis=%s rr %.2f below %.2f after pullback", original.thesis_id, rr, self.cfg.min_rr)
            return None

        refined = EntrySignal(
            side=original.side,
            entry_type="refined_post_sweep",
            entry_price=price,
            sl_price=original.sl_price,
            tp_price=original.tp_price,
            rr_ratio=rr,
            target_pool=original.target_pool,
            sweep_result=original.sweep_result,
            conviction=original.conviction,
            reason=f"refined entry accepted; {diag.reason}",
            ict_validation=original.ict_validation,
            thesis_id=original.thesis_id,
            raw_tp_label=original.raw_tp_label,
            route_probability=original.route_probability,
            route_ev_r=original.route_ev_r,
            route_utility=original.route_utility,
            diagnostics={**original.diagnostics, "pullback": diag.__dict__},
        )
        self._pending_refined = None
        logger.info("REFINED_ENTRY_READY thesis=%s side=%s entry=%.1f rr=%.2f %s", refined.thesis_id, refined.side, refined.entry_price, refined.rr_ratio, diag.reason)
        return refined

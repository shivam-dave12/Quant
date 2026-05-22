"""
ict_liquidity_entry.py — single-authority ICT + Liquidity execution engine.

This module is intentionally the only alpha authority for new positions.
It expresses one institutional auction model:

    4H/15m draw-on-liquidity context ->
    5m external-liquidity raid -> 5m displacement/MSS ->
    5m FVG retracement -> structural invalidation -> opposing HTF liquidity TP.

It does not consume secondary score layers or non-structural directional overlays,
session quotas, approach/momentum entries, or trend-permission gates. Risk, lot
sizing, bracket protection and reconciliation remain downstream mechanical
controls in the execution/risk modules.
"""
from __future__ import annotations

import logging
import math
import statistics
import time
try:
    import config
except Exception:  # pragma: no cover - test stubs may not load runtime config
    config = None
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

try:
    from strategy.liquidity_map import LiquidityMapSnapshot, PoolTarget, SweepResult, TF_HIERARCHY, _last_closed_candle_idx
except ImportError:  # pragma: no cover
    from liquidity_map import LiquidityMapSnapshot, PoolTarget, SweepResult, TF_HIERARCHY, _last_closed_candle_idx  # type: ignore

logger = logging.getLogger(__name__)


class EngineState(Enum):
    SCANNING = "SCANNING"
    CONTEXT_READY = "CONTEXT_READY"
    LIQUIDITY_RAID = "LIQUIDITY_RAID"
    EXECUTABLE = "EXECUTABLE"
    ENTERING = "ENTERING"
    IN_POSITION = "IN_POSITION"


class EntryType(Enum):
    ICT_LIQUIDITY = "ICT_LIQUIDITY"


@dataclass
class EntrySignal:
    side: str
    entry_type: EntryType
    entry_price: float
    sl_price: float
    tp_price: float
    rr_ratio: float
    target_pool: Optional[PoolTarget]
    sweep_result: Optional[SweepResult] = None
    delivery_probability: float = 0.0
    reason: str = ""
    structural_validation: str = ""
    created_at: float = field(default_factory=time.time)
    quality: Dict[str, float] = field(default_factory=dict)
    analysis_domain: str = "UNDERLYING"


@dataclass(frozen=True)
class _TrendContext:
    side: int
    confidence: float
    slope_atr: float
    efficiency: float
    structure: float
    signed_score: float = 0.0
    slope_component: float = 0.0
    structure_component: float = 0.0

    @property
    def label(self) -> str:
        return "bullish" if self.side > 0 else ("bearish" if self.side < 0 else "ranging")


@dataclass(frozen=True)
class _FVG:
    side: str
    low: float
    high: float
    index: int
    displacement_atr: float

    @property
    def equilibrium(self) -> float:
        return (self.low + self.high) / 2.0


@dataclass(frozen=True)
class _ContextDecision:
    side: str
    allowed: bool
    path: str
    block: str
    delivery_score: float
    strict_aligned: bool
    supporting_tfs: int
    opposing_tfs: int
    ranging_tfs: int


@dataclass
class _Thesis:
    sweep_key: tuple
    side: str
    sweep: SweepResult
    formed_at: float
    context_4h: _TrendContext
    context_15m: _TrendContext
    context_path: str
    context_delivery_score: float
    mss_level: float
    displacement_atr: float
    fvg: _FVG
    last_reason: str = "waiting for FVG repricing"


_EPS = 1e-12
_TF_15M_RANK = TF_HIERARCHY.get("15m", 3)
_CONTEXT_DIRECTION_THRESHOLD = 0.18


def _f(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else default
    except Exception:
        return default


def _closed(candles: Optional[Sequence[Dict]], minimum: int = 0,
            timeframe: str = "5m", now: Optional[float] = None) -> List[Dict]:
    rows = list(candles or [])
    idx = _last_closed_candle_idx(rows, timeframe, time.time() if now is None else float(now))
    rows = rows[:idx + 1] if idx >= 0 else []
    return rows if len(rows) >= minimum else []


def _median(values: Iterable[float], default: float = 0.0) -> float:
    clean = [float(v) for v in values if math.isfinite(float(v))]
    return statistics.median(clean) if clean else default


def _true_range(c: Dict, prev_close: float) -> float:
    h, l = _f(c.get("h")), _f(c.get("l"))
    return max(h - l, abs(h - prev_close), abs(l - prev_close))


def _timeframe_atr(candles: Sequence[Dict], period: int = 14) -> float:
    """Canonical closed-bar Wilder ATR for structural interpretation.

    `candles` is already a closed-bar series.  The same Wilder/RMA estimator is
    used by execution ATR and liquidity-map ATR, so a 5m raid, its stop
    clearance and its spread/R calculation cannot be normalised by different
    volatility numbers.
    """
    rows = list(candles or [])
    if len(rows) < period + 1:
        return 0.0
    tr = [_true_range(rows[i], _f(rows[i - 1].get("c"))) for i in range(1, len(rows))]
    if len(tr) < period:
        return 0.0
    atr = sum(tr[:period]) / period
    for value in tr[period:]:
        atr = (atr * (period - 1) + value) / period
    return atr


def _robust_trend(candles: Sequence[Dict], atr: float, window: int) -> _TrendContext:
    rows = list(candles[-window:])
    if len(rows) < max(10, window // 3) or atr <= _EPS:
        return _TrendContext(0, 0.0, 0.0, 0.0, 0.0)
    closes = [_f(c.get("c")) for c in rows]
    if min(closes) <= 0:
        return _TrendContext(0, 0.0, 0.0, 0.0, 0.0)
    logs = [math.log(x) for x in closes]
    # Theil-Sen slope is robust to a single sweep wick/outlier bar.
    slopes = [(logs[j] - logs[i]) / (j - i)
              for i in range(len(logs) - 1)
              for j in range(i + 1, len(logs)) if j - i >= 2]
    slope = _median(slopes)
    slope_points = slope * closes[-1]
    slope_atr = slope_points / atr
    gross = sum(abs(closes[i] - closes[i - 1]) for i in range(1, len(closes)))
    efficiency = abs(closes[-1] - closes[0]) / max(gross, _EPS)
    q = max(2, min(5, len(rows) // 6))
    earlier_high = max(_f(x.get("h")) for x in rows[-2 * q:-q])
    earlier_low = min(_f(x.get("l")) for x in rows[-2 * q:-q])
    latest_high = max(_f(x.get("h")) for x in rows[-q:])
    latest_low = min(_f(x.get("l")) for x in rows[-q:])
    structure = 1.0 if latest_high > earlier_high and latest_low > earlier_low else (
        -1.0 if latest_high < earlier_high and latest_low < earlier_low else 0.0)
    slope_component = 0.68 * math.tanh(slope_atr * window / 3.0)
    structure_component = 0.32 * structure
    signed_strength = slope_component + structure_component
    strength = abs(signed_strength) * (0.55 + 0.45 * efficiency)
    side = 1 if signed_strength > _CONTEXT_DIRECTION_THRESHOLD else (
        -1 if signed_strength < -_CONTEXT_DIRECTION_THRESHOLD else 0)
    return _TrendContext(side, min(1.0, strength), slope_atr, efficiency, structure,
                         signed_strength, slope_component, structure_component)


def _find_fvg(candles: Sequence[Dict], side: str, start: int, atr: float) -> Optional[_FVG]:
    best: Optional[_FVG] = None
    for i in range(max(2, start), len(candles)):
        before, impulse, after = candles[i - 2], candles[i - 1], candles[i]
        if side == "long":
            low, high = _f(before.get("h")), _f(after.get("l"))
            displacement = (_f(impulse.get("c")) - _f(impulse.get("o"))) / max(atr, _EPS)
            if high > low and displacement > 0:
                best = _FVG(side, low, high, i, displacement)
        else:
            low, high = _f(after.get("h")), _f(before.get("l"))
            displacement = (_f(impulse.get("o")) - _f(impulse.get("c"))) / max(atr, _EPS)
            if high > low and displacement > 0:
                best = _FVG(side, low, high, i, displacement)
    return best


def _sweep_key(sweep: SweepResult) -> tuple:
    pool = getattr(sweep, "pool", None)
    side = str(getattr(getattr(pool, "side", None), "value", "") or "")
    return (round(_f(getattr(pool, "price", 0.0)), 8), side, round(_f(getattr(sweep, "detected_at", 0.0)), 3))


class ICTLiquidityEntryEngine:
    """Single institutional entry authority; all desks share this structure model."""

    def __init__(self, on_self_recovery=None) -> None:
        self._state = EngineState.SCANNING
        self._state_entered = time.time()
        self._on_self_recovery = on_self_recovery
        self._signal: Optional[EntrySignal] = None
        self._active_signal: Optional[EntrySignal] = None
        self._thesis: Optional[_Thesis] = None
        self._processed: Dict[tuple, float] = {}
        self._last_analysis: Dict[str, Any] = {}
        self._last_pool_plan: Optional[Dict[str, Any]] = None
        self._last_scan_skip: Dict[str, int] = {}
        self._atr_pctile: float = 0.5
        self._min_structural_rr: float = 1.0
        self._max_structural_rr_reference: float = 0.0
        self._execution_cost_points: float = 0.0
        self._execution_cost_bps: float = 0.0
        self._stop_clearance_base_atr: float = float(getattr(config, "ICT_STOP_CLEARANCE_BASE_ATR", 0.10) if config is not None else 0.10)
        self._stop_clearance_pctile_slope_atr: float = float(getattr(config, "ICT_STOP_CLEARANCE_PCTL_SLOPE_ATR", 0.18) if config is not None else 0.18)

    # ---------- public interface retained for execution lifecycle ----------

    def set_atr_pctile(self, pctile: float) -> None:
        self._atr_pctile = max(0.0, min(1.0, _f(pctile, 0.5)))

    def set_structural_delivery_policy(self, min_rr: float, max_rr_reference: float = 0.0) -> None:
        """Receive instrument-scoped execution policy without adding an alpha overlay.

        The liquidity pool remains the target source.  This policy only prevents
        accepting a target whose structural payoff is below the desk's declared
        minimum reward for one unit of invalidation risk.
        """
        self._min_structural_rr = max(1.0, _f(min_rr, 1.0))
        self._max_structural_rr_reference = max(0.0, _f(max_rr_reference, 0.0))

    def set_execution_cost_model(self, round_trip_cost_points: float = 0.0, round_trip_cost_bps: float = 0.0) -> None:
        """Set observable, venue-derived execution cost in analysis-price units.

        The structural target remains an actual liquidity pool.  Cost is used
        only to calculate net R and reject non-positive net expectancy.  NIFTY
        underlying thesis passes zero here and is checked after premium conversion.
        """
        self._execution_cost_points = max(0.0, _f(round_trip_cost_points, 0.0))
        self._execution_cost_bps = max(0.0, _f(round_trip_cost_bps, 0.0))

    def _record_block(self, reason: str, **values: Any) -> None:
        """Record a transparent structural decision without changing entry geometry."""
        self._last_scan_skip = {str(reason).lower(): 1}
        payload = {"block_reason": str(reason), "trigger": "WAIT"}
        payload.update(values)
        self._last_analysis.update(payload)

    @staticmethod
    def _direction_int(side: str) -> int:
        side = str(side or "").lower()
        return 1 if side == "long" else (-1 if side == "short" else 0)

    def _context_decision_for_raid(self, side: str, ctx4: _TrendContext,
                                   ctx15: _TrendContext, sweep_quality: float) -> _ContextDecision:
        """Classify HTF context without using trend alignment as an entry gate.

        The executable evidence is still the 5m raid -> MSS/displacement -> FVG
        repricing sequence.  4H/15m context supplies draw-on-liquidity bias and
        probability.  Only a unanimous, explicit HTF delivery against the raid is
        blocked before the 5m proof sequence can finish.
        """
        direction = self._direction_int(side)
        if direction == 0:
            return _ContextDecision(side, False, "INVALID_RAID_DIRECTION",
                                    "INVALID_RAID_DIRECTION", 0.0, False, 0, 0, 0)
        contexts = (ctx4, ctx15)
        supporting = sum(1 for ctx in contexts if ctx.side == direction)
        opposing = sum(1 for ctx in contexts if ctx.side == -direction)
        ranging = sum(1 for ctx in contexts if ctx.side == 0)
        support_strength = sum(ctx.confidence for ctx in contexts if ctx.side == direction)
        opposing_strength = sum(ctx.confidence for ctx in contexts if ctx.side == -direction)
        range_balance = sum(
            max(0.0, 1.0 - min(1.0, abs(ctx.signed_score) / _CONTEXT_DIRECTION_THRESHOLD))
            for ctx in contexts if ctx.side == 0
        )
        strict = supporting == 2
        if opposing == 2:
            score = max(0.0, 0.10 + 0.10 * _f(sweep_quality) - 0.35 * opposing_strength)
            return _ContextDecision(side, False, "HTF_DELIVERY_OPPOSES_RAID",
                                    "HTF_DELIVERY_OPPOSES_RAID", score, False,
                                    supporting, opposing, ranging)
        if strict:
            path = "STRICT_4H_15M_DOL"
        elif supporting == 1 and ranging == 1:
            path = "PARTIAL_HTF_DOL"
        elif supporting == 1 and opposing == 1:
            path = "MITIGATION_RAID_WITH_SPLIT_HTF"
        elif ranging == 2:
            path = "BALANCED_RANGE_EXTERNAL_RAID"
        elif ranging == 1 and opposing == 1:
            path = "COUNTER_DELIVERY_RAID_REQUIRES_5M_PROOF"
        else:
            path = "LIQUIDITY_RAID_DOL"
        score = (
            0.26
            + 0.24 * min(1.0, support_strength)
            + 0.16 * min(1.0, range_balance)
            + 0.20 * max(0.0, min(1.0, _f(sweep_quality)))
            - 0.18 * min(1.0, opposing_strength)
        )
        return _ContextDecision(side, True, path, "NONE",
                                max(0.05, min(0.95, score)), strict,
                                supporting, opposing, ranging)

    def update(self, liq_snapshot: LiquidityMapSnapshot, price: float, atr: float, now: float,
               candles_5m: Optional[List[Dict]] = None,
               candles_15m: Optional[List[Dict]] = None,
               candles_4h: Optional[List[Dict]] = None) -> None:
        self._last_scan_skip = {}
        if atr <= _EPS or price <= 0:
            self._last_analysis = {
                "model": "ICT_LIQUIDITY_4H_15M_5M", "state": self._state.value,
                "price": price, "entry_5m_atr": atr, "block_reason": "INVALID_PRICE_OR_5M_ATR",
                "trigger": "WAIT", "authority": "STRUCTURAL_ONLY",
            }
            self._last_scan_skip = {"invalid_price_or_5m_atr": 1}
            return
        self._expire(now)
        c5 = _closed(candles_5m, 24, "5m", now)
        c15 = _closed(candles_15m, 24, "15m", now)
        c4h = _closed(candles_4h, 20, "4h", now)
        if not c5 or not c15 or not c4h:
            self._last_analysis = {
                "model": "ICT_LIQUIDITY_4H_15M_5M", "state": self._state.value,
                "price": price, "entry_5m_atr": atr, "bars_5m": len(c5),
                "bars_15m": len(c15), "bars_4h": len(c4h),
                "block_reason": "TIMEFRAME_WARMUP", "trigger": "WAIT",
                "authority": "STRUCTURAL_ONLY",
            }
            self._last_scan_skip = {"timeframe_warmup": 1}
            return

        atr4h = _timeframe_atr(c4h)
        atr15m = _timeframe_atr(c15)
        if atr4h <= _EPS or atr15m <= _EPS:
            self._last_analysis = {
                "model": "ICT_LIQUIDITY_4H_15M_5M", "state": self._state.value,
                "price": price, "entry_5m_atr": atr, "context_4h_atr": atr4h,
                "context_15m_atr": atr15m, "block_reason": "TIMEFRAME_ATR_WARMUP",
                "trigger": "WAIT", "authority": "STRUCTURAL_ONLY",
            }
            self._last_scan_skip = {"timeframe_atr_warmup": 1}
            return
        ctx4 = _robust_trend(c4h, atr4h, min(32, len(c4h)))
        ctx15 = _robust_trend(c15, atr15m, min(56, len(c15)))
        aligned_side = ctx4.side if ctx4.side != 0 and ctx4.side == ctx15.side else 0
        aligned_label = "long" if aligned_side > 0 else ("short" if aligned_side < 0 else "none")
        self._last_analysis = {
            "model": "ICT_LIQUIDITY_4H_15M_5M",
            "state": self._state.value,
            "price": price,
            "context_4h": ctx4.label, "context_4h_conf": ctx4.confidence,
            "context_4h_slope_atr": ctx4.slope_atr, "context_4h_efficiency": ctx4.efficiency,
            "context_4h_structure": ctx4.structure, "context_4h_score": ctx4.signed_score,
            "context_4h_slope_component": ctx4.slope_component, "context_4h_structure_component": ctx4.structure_component,
            "context_4h_atr": atr4h,
            "context_15m": ctx15.label, "context_15m_conf": ctx15.confidence,
            "context_15m_slope_atr": ctx15.slope_atr, "context_15m_efficiency": ctx15.efficiency,
            "context_15m_structure": ctx15.structure, "context_15m_score": ctx15.signed_score,
            "context_15m_slope_component": ctx15.slope_component, "context_15m_structure_component": ctx15.structure_component,
            "context_15m_atr": atr15m, "context_direction_threshold": _CONTEXT_DIRECTION_THRESHOLD,
            "min_structural_rr": self._min_structural_rr,
            "max_structural_rr_reference": self._max_structural_rr_reference,
            "context_aligned": bool(aligned_side), "context_direction": aligned_label,
            "context_permission": False, "context_bias_path": "AWAITING_5M_DOL",
            "context_delivery_score": 0.0,
            "entry_5m_atr": atr, "atr_percentile": self._atr_pctile,
            "bars_5m": len(c5), "bars_15m": len(c15), "bars_4h": len(c4h),
            "authority": "STRUCTURAL_ONLY", "trigger": "WAIT", "block_reason": "EVALUATING",
        }
        if self._state in (EngineState.ENTERING, EngineState.IN_POSITION):
            self._record_block("POSITION_LIFECYCLE_ACTIVE", trigger=self._state.value)
            return
        if self._signal is not None:
            self._last_analysis.update({"state": EngineState.EXECUTABLE.value, "trigger": "SIGNAL_PENDING_EXECUTION", "block_reason": "NONE"})
            return

        if self._thesis is not None:
            if now - self._thesis.formed_at > 900.0:
                self._last_analysis.update({"expired_thesis_age_sec": now - self._thesis.formed_at})
                self._thesis = None
                self._state = EngineState.SCANNING
            else:
                self._try_reprice_thesis(self._thesis, liq_snapshot, price, atr, now)
                return

        fresh = self._fresh_5m_sweeps(liq_snapshot, now)
        parent_htf = self._fresh_parent_htf_sweeps(liq_snapshot, now)
        parent_best = parent_htf[0] if parent_htf else None
        parent_pool = getattr(parent_best, "pool", None) if parent_best is not None else None
        self._last_analysis.update({"fresh_5m_raid_count": len(fresh), "aligned_5m_raid_count": 0,
                                    "opposed_5m_raid_count": 0, "invalid_5m_raid_count": 0,
                                    "parent_htf_raid_count": len(parent_htf),
                                    "parent_htf_raid_side": str(getattr(parent_best, "direction", "") or ""),
                                    "parent_htf_raid_tf": str(getattr(parent_pool, "timeframe", "") or ""),
                                    "parent_htf_raid_price": _f(getattr(parent_pool, "price", 0.0)),
                                    "parent_htf_raid_wick": _f(getattr(parent_best, "wick_extreme", 0.0)),
                                    "parent_htf_raid_quality": _f(getattr(parent_best, "quality", 0.0)),
                                    "parent_htf_raid_age_sec": (max(0.0, now - _f(getattr(parent_best, "detected_at", now)))
                                                                if parent_best is not None else 0.0)})
        self._state = EngineState.CONTEXT_READY
        self._last_analysis["state"] = self._state.value
        if not fresh:
            if parent_htf:
                self._record_block("AWAITING_FRESH_5M_CONFIRMATION_AFTER_HTF_RAID",
                                   trigger="WAIT_FOR_5M_RAID_MSS_FVG")
            else:
                self._record_block("AWAITING_FRESH_5M_LIQUIDITY_RAID")
            return
        for sweep in sorted(fresh, key=lambda sw: _f(getattr(sw, "quality", 0.0)), reverse=True):
            side = str(getattr(sweep, "direction", "") or "").lower()
            direction = self._direction_int(side)
            pool = getattr(sweep, "pool", None)
            context_decision = self._context_decision_for_raid(
                side, ctx4, ctx15, _f(getattr(sweep, "quality", 0.0)))
            candidate = {
                "candidate_raid_side": side or "unknown",
                "candidate_raid_pool_side": str(getattr(getattr(pool, "side", None), "value", "") or ""),
                "candidate_raid_price": _f(getattr(pool, "price", 0.0)),
                "candidate_raid_wick": _f(getattr(sweep, "wick_extreme", 0.0)),
                "candidate_raid_quality": _f(getattr(sweep, "quality", 0.0)),
                "candidate_raid_age_sec": max(0.0, now - _f(getattr(sweep, "detected_at", now))),
                "candidate_context_bias_path": context_decision.path,
                "candidate_context_delivery_score": context_decision.delivery_score,
            }
            self._last_analysis.update(candidate)
            if direction == 0:
                self._last_analysis["invalid_5m_raid_count"] += 1
                self._record_block("INVALID_RAID_DIRECTION")
                continue
            if not context_decision.allowed:
                self._last_analysis["opposed_5m_raid_count"] += 1
                self._record_block(context_decision.block,
                                   context_bias_path=context_decision.path,
                                   context_delivery_score=context_decision.delivery_score,
                                   context_direction=side or "none",
                                   context_permission=False)
                continue
            self._last_analysis["aligned_5m_raid_count"] += 1
            self._last_analysis.update({k.replace("candidate_", ""): v for k, v in candidate.items()})
            self._last_analysis.update({
                "context_permission": True,
                "context_bias_path": context_decision.path,
                "context_delivery_score": context_decision.delivery_score,
                "context_direction": side,
                "context_aligned": context_decision.strict_aligned,
                "context_supporting_tfs": context_decision.supporting_tfs,
                "context_opposing_tfs": context_decision.opposing_tfs,
                "context_ranging_tfs": context_decision.ranging_tfs,
            })
            thesis = self._build_thesis(sweep, side, ctx4, ctx15, context_decision, c5, atr, now)
            if thesis is None:
                continue
            self._thesis = thesis
            self._state = EngineState.LIQUIDITY_RAID
            self._last_analysis["state"] = self._state.value
            self._try_reprice_thesis(thesis, liq_snapshot, price, atr, now)
            return
        if self._last_analysis.get("block_reason") == "EVALUATING":
            self._record_block("NO_EXECUTABLE_INSTITUTIONAL_RAID")

    def get_signal(self) -> Optional[EntrySignal]:
        return self._signal

    def consume_signal(self) -> Optional[EntrySignal]:
        sig, self._signal = self._signal, None
        return sig

    def on_entry_placed(self, signal: Optional[EntrySignal] = None) -> None:
        self._active_signal = signal or self._signal
        self._signal = None
        self._state = EngineState.ENTERING
        self._state_entered = time.time()
        if self._thesis is not None:
            self._processed[self._thesis.sweep_key] = time.time() + 1800.0

    def on_position_opened(self) -> None:
        self._state = EngineState.IN_POSITION
        self._state_entered = time.time()

    def on_entry_failed(self, reason: str = "ENTRY_FAILED_RESET") -> None:
        self._active_signal = None
        self._signal = None
        self._thesis = None
        self._state = EngineState.SCANNING
        self._state_entered = time.time()
        current_reason = str(self._last_analysis.get("block_reason", "") or "")
        final_reason = current_reason if current_reason.startswith("PRE_ORDER_") else reason
        self._last_analysis.update({
            "state": self._state.value,
            "trigger": "WAIT",
            "block_reason": final_reason,
            "context_permission": False,
        })

    def on_entry_cancelled(self) -> None:
        self.on_entry_failed()

    def on_position_closed(self) -> None:
        self.on_entry_failed()

    def force_reset(self, reason: str = "operator_reset") -> None:
        del reason
        self.on_entry_failed()
        self._processed.clear()

    def mark_pre_order_rejected(self, signal: Optional[EntrySignal] = None,
                                cooldown_sec: float = 30.0,
                                execution_context: Optional[Dict[str, Any]] = None) -> None:
        del execution_context
        sig = signal or self._active_signal or self._signal
        sw = getattr(sig, "sweep_result", None) if sig is not None else None
        if sw is not None:
            self._processed[_sweep_key(sw)] = time.time() + max(5.0, float(cooldown_sec))
        self._last_analysis.update({
            "state": EngineState.SCANNING.value,
            "trigger": "WAIT",
            "block_reason": "PRE_ORDER_EXECUTION_REJECTED",
        })

    def mark_signal_deferred(self, side: str, reason_prefix: str,
                             cooldown_sec: float = 30.0) -> None:
        del side, reason_prefix
        if self._thesis is not None:
            self._processed[self._thesis.sweep_key] = time.time() + max(5.0, float(cooldown_sec))
        self._signal = None
        self._thesis = None
        self._state = EngineState.SCANNING

    def mark_gate_blocked(self, side: str, reason_prefix: str, cooldown_sec: float = 30.0) -> None:
        self.mark_signal_deferred(side, reason_prefix, cooldown_sec)

    def invalidate_sweep_locks(self, reason: str = "context_changed") -> None:
        del reason
        self._thesis = None
        self._signal = None
        if self._state not in (EngineState.ENTERING, EngineState.IN_POSITION):
            self._state = EngineState.SCANNING

    @property
    def state(self) -> str:
        return self._state.value

    @property
    def tracking_info(self) -> Optional[Dict[str, Any]]:
        if self._thesis is None:
            return None
        return {
            "mode": "ICT_LIQUIDITY",
            "direction": self._thesis.side,
            "target": f"FVG {self._thesis.fvg.low:,.4f}-{self._thesis.fvg.high:,.4f}",
            "reason": self._thesis.last_reason,
            "age_sec": max(0.0, time.time() - self._thesis.formed_at),
        }

    @property
    def analysis_info(self) -> Dict[str, Any]:
        return dict(self._last_analysis)

    @property
    def scan_skip_info(self) -> Optional[Dict[str, Dict[str, int]]]:
        return {"ict_liquidity": dict(self._last_scan_skip)} if self._last_scan_skip else None

    @property
    def pool_plan_info(self) -> Optional[Dict[str, Any]]:
        return dict(self._last_pool_plan) if self._last_pool_plan else None

    # ---------- model internals ----------
    def _expire(self, now: float) -> None:
        self._processed = {k: expiry for k, expiry in self._processed.items() if expiry > now}

    def _fresh_5m_sweeps(self, snap: LiquidityMapSnapshot, now: float) -> List[SweepResult]:
        out = []
        for sw in list(getattr(snap, "recent_sweeps", []) or []):
            pool = getattr(sw, "pool", None)
            if str(getattr(pool, "timeframe", "") or "").lower() != "5m":
                continue
            age = max(0.0, now - _f(getattr(sw, "detected_at", 0.0)))
            if age > 600.0 or _sweep_key(sw) in self._processed:
                continue
            out.append(sw)
        return out

    def _fresh_parent_htf_sweeps(self, snap: LiquidityMapSnapshot, now: float) -> List[SweepResult]:
        out = []
        max_age_by_tf = {
            "15m": 1800.0,
            "30m": 3600.0,
            "1h": 7200.0,
            "2h": 14400.0,
            "4h": 28800.0,
            "1d": 86400.0,
        }
        for sw in list(getattr(snap, "recent_sweeps", []) or []):
            pool = getattr(sw, "pool", None)
            tf = str(getattr(pool, "timeframe", "") or "").lower()
            max_age = max_age_by_tf.get(tf)
            if max_age is None:
                continue
            age = max(0.0, now - _f(getattr(sw, "detected_at", 0.0)))
            if age > max_age or _sweep_key(sw) in self._processed:
                continue
            out.append(sw)
        return sorted(out, key=lambda sw: _f(getattr(sw, "quality", 0.0)), reverse=True)

    def _build_thesis(self, sweep: SweepResult, side: str, ctx4: _TrendContext,
                      ctx15: _TrendContext, context_decision: _ContextDecision,
                      candles_5m: List[Dict], atr: float,
                      now: float) -> Optional[_Thesis]:
        idx = int(getattr(sweep, "sweep_candle_idx", len(candles_5m) - 4) or 0)
        if idx >= len(candles_5m):
            idx = len(candles_5m) - 1
        idx = max(3, min(idx, len(candles_5m) - 1))
        recent_close = _f(candles_5m[-1].get("c"))
        wick = _f(getattr(sweep, "wick_extreme", 0.0), recent_close)
        pre = candles_5m[max(0, idx - 12):idx]
        if len(pre) < 4:
            self._record_block("INSUFFICIENT_PRE_RAID_STRUCTURE", pre_raid_bars=len(pre))
            return None
        if side == "long":
            mss = max(_f(c.get("h")) for c in pre)
            displacement = (recent_close - wick) / max(atr, _EPS)
            mss_broken = recent_close > mss
        else:
            mss = min(_f(c.get("l")) for c in pre)
            displacement = (wick - recent_close) / max(atr, _EPS)
            mss_broken = recent_close < mss
        self._last_analysis.update({
            "mss_level": mss, "mss_broken": bool(mss_broken),
            "displacement_atr": displacement, "post_raid_close": recent_close,
        })
        if not mss_broken or displacement <= 0:
            self._record_block("AWAITING_5M_MSS_DISPLACEMENT")
            return None
        fvg = _find_fvg(candles_5m, side, idx + 1, atr)
        if fvg is None or fvg.displacement_atr <= 0:
            self._record_block("AWAITING_5M_DISPLACEMENT_FVG")
            return None
        self._last_analysis.update({
            "fvg_low": fvg.low, "fvg_high": fvg.high, "fvg_equilibrium": fvg.equilibrium,
            "fvg_displacement_atr": fvg.displacement_atr,
        })
        return _Thesis(
            sweep_key=_sweep_key(sweep), side=side, sweep=sweep, formed_at=now,
            context_4h=ctx4, context_15m=ctx15,
            context_path=context_decision.path,
            context_delivery_score=context_decision.delivery_score,
            mss_level=mss,
            displacement_atr=displacement, fvg=fvg,
        )

    def _try_reprice_thesis(self, thesis: _Thesis, snap: LiquidityMapSnapshot,
                            price: float, atr: float, now: float) -> None:
        zone_tol = 0.08 * atr
        low_bound, high_bound = thesis.fvg.low - zone_tol, thesis.fvg.high + zone_tol
        in_reprice = low_bound <= price <= high_bound
        distance_to_zone = 0.0 if in_reprice else min(abs(price - low_bound), abs(price - high_bound))
        self._last_analysis.update({
            "state": self._state.value, "side": thesis.side,
            "context_permission": True,
            "context_bias_path": thesis.context_path,
            "context_delivery_score": thesis.context_delivery_score,
            "context_direction": thesis.side,
            "fvg_low": thesis.fvg.low, "fvg_high": thesis.fvg.high,
            "fvg_equilibrium": thesis.fvg.equilibrium, "fvg_tolerance": zone_tol,
            "fvg_distance_atr": distance_to_zone / max(atr, _EPS),
            "displacement_atr": thesis.displacement_atr,
            "thesis_age_sec": max(0.0, now - thesis.formed_at),
        })
        if not in_reprice:
            thesis.last_reason = "MSS confirmed; awaiting 5m FVG rebalance"
            self._record_block("AWAITING_FVG_REBALANCE", trigger="AWAITING_FVG_REBALANCE")
            return
        entry = price
        stop = self._structural_stop(thesis, atr)
        wick = _f(getattr(thesis.sweep, "wick_extreme", 0.0))
        clearance = abs(stop - wick) if stop is not None else 0.0
        self._last_analysis.update({
            "entry": entry, "structural_stop": stop, "stop_clearance": clearance,
            "stop_clearance_atr": clearance / max(atr, _EPS),
        })
        if stop is None:
            thesis.last_reason = "invalid structural stop geometry"
            self._record_block("INVALID_STRUCTURAL_STOP")
            return
        stop_protective = ((thesis.side == "long" and stop < entry) or
                           (thesis.side == "short" and stop > entry))
        if not stop_protective:
            thesis.last_reason = "structural stop is not on the invalidation side of entry"
            self._record_block("INVALID_STRUCTURAL_STOP_SIDE")
            return
        target = self._select_liquidity_target(thesis.side, entry, stop, snap, atr)
        if target is None:
            thesis.last_reason = "no opposing HTF liquidity with positive delivery utility"
            self._record_block("AWAITING_POSITIVE_UTILITY_HTF_TARGET")
            return
        target_obj, tp, rr, utility, delivery_p = target
        quality = {
            "context_4h": thesis.context_4h.confidence,
            "context_15m": thesis.context_15m.confidence,
            "context_delivery_score": thesis.context_delivery_score,
            "raid_quality": _f(getattr(thesis.sweep, "quality", 0.0)),
            "displacement_atr": thesis.displacement_atr,
            "delivery_probability": delivery_p,
            "delivery_utility_r": utility,
        }
        explanation = (
            f"4H={thesis.context_4h.label}({thesis.context_4h.confidence:.2f}) | "
            f"15m={thesis.context_15m.label}({thesis.context_15m.confidence:.2f}) | "
            f"bias={thesis.context_path}({thesis.context_delivery_score:.2f}) | "
            f"5m raid={getattr(getattr(thesis.sweep, 'pool', None), 'side', '')} "
            f"MSS/FVG | displacement={thesis.displacement_atr:.2f}ATR | "
            f"deliveryP={delivery_p:.2f} utility={utility:+.2f}R"
        )
        self._signal = EntrySignal(
            side=thesis.side, entry_type=EntryType.ICT_LIQUIDITY, entry_price=entry,
            sl_price=stop, tp_price=tp, rr_ratio=rr, target_pool=target_obj,
            sweep_result=thesis.sweep, delivery_probability=delivery_p,
            reason=explanation, structural_validation="4H→15m→5m liquidity raid / MSS / FVG repricing",
            quality=quality,
        )
        self._state = EngineState.EXECUTABLE
        self._state_entered = now
        self._last_scan_skip = {}
        self._last_analysis.update({
            "state": self._state.value, "side": thesis.side, "trigger": "EXECUTABLE_FVG_REPRICE",
            "block_reason": "NONE", "displacement_atr": thesis.displacement_atr,
            "entry": entry, "sl": stop, "tp": tp, "rr": rr,
            "gross_rr": rr,
            "delivery_probability": delivery_p, "delivery_utility_r": utility,
            "context_bias_path": thesis.context_path,
            "context_delivery_score": thesis.context_delivery_score,
            "target_timeframe": str(getattr(target_obj.pool, "timeframe", "")),
            "target_pool_price": _f(getattr(target_obj.pool, "price", 0.0)),
            "target_significance": _f(getattr(target_obj, "significance", 0.0)),
        })
        logger.info("ICT_LIQUIDITY ENTRY READY %s @ %.4f | SL=%.4f TP=%.4f grossRR=%.2f netWinR=%s netEU=%s | %s",
                    thesis.side.upper(), entry, stop, tp, rr,
                    f"{self._last_analysis.get('target_net_win_r'):.2f}" if self._last_analysis.get('target_net_win_r') is not None else "N/A",
                    f"{utility:+.2f}R", explanation)

    def _structural_stop(self, thesis: _Thesis, atr: float) -> Optional[float]:
        wick = _f(getattr(thesis.sweep, "wick_extreme", 0.0))
        if wick <= 0:
            return None
        # The stop is behind the raided liquidity extreme; volatility only sizes clearance.
        regime_clearance = atr * (self._stop_clearance_base_atr + self._stop_clearance_pctile_slope_atr * self._atr_pctile)
        self._last_analysis.update({
            "stop_clearance_base_atr": self._stop_clearance_base_atr,
            "stop_clearance_pctile_slope_atr": self._stop_clearance_pctile_slope_atr,
            "stop_clearance_model_atr": regime_clearance / max(atr, _EPS),
        })
        if thesis.side == "long":
            sl = wick - regime_clearance
            return sl if sl < thesis.fvg.low else None
        sl = wick + regime_clearance
        return sl if sl > thesis.fvg.high else None

    def _select_liquidity_target(self, side: str, entry: float, sl: float,
                                 snap: LiquidityMapSnapshot, atr: float
                                 ) -> Optional[Tuple[PoolTarget, float, float, float, float]]:
        risk = abs(entry - sl)
        audit = {"pool_total": 0, "wrong_side": 0, "below_timeframe": 0,
                 "tp_buffer_crossed_entry": 0, "gross_rr_below_floor": 0,
                 "non_positive_net_utility": 0, "eligible": 0, "positive": 0}
        if risk <= _EPS:
            self._last_analysis.update({"target_audit": audit, "target_block": "ZERO_STRUCTURAL_RISK"})
            return None
        pools = snap.bsl_pools if side == "long" else snap.ssl_pools
        audit["pool_total"] = len(list(pools or []))
        candidates = []
        for t in list(pools or []):
            pool = getattr(t, "pool", None)
            px = _f(getattr(pool, "price", 0.0))
            if px <= 0 or (side == "long" and px <= entry) or (side == "short" and px >= entry):
                audit["wrong_side"] += 1
                continue
            tf_rank = TF_HIERARCHY.get(str(getattr(pool, "timeframe", "1m")), 1)
            promoted = int(getattr(pool, "htf_count", 0) or 0) >= 2
            if tf_rank < _TF_15M_RANK and not promoted:
                audit["below_timeframe"] += 1
                continue
            audit["eligible"] += 1
            dist = abs(px - entry)
            significance = max(0.01, _f(getattr(t, "significance", 0.0), 0.01))
            buffer = min(0.28 * atr, max(0.04 * atr, 0.04 * atr * math.log1p(significance)))
            tp = px - buffer if side == "long" else px + buffer
            profitable_tp = ((side == "long" and tp > entry) or
                             (side == "short" and tp < entry))
            if not profitable_tp:
                audit["tp_buffer_crossed_entry"] += 1
                continue
            reward = abs(tp - entry)
            rr = reward / risk
            if rr < self._min_structural_rr:
                audit["gross_rr_below_floor"] += 1
                continue
            distance_atr = dist / max(atr, _EPS)
            if self._thesis is not None:
                context = _f(getattr(self._thesis, "context_delivery_score", 0.0), 0.0)
                if context <= 0.0:
                    context = (
                        0.50 * _f(getattr(getattr(self._thesis, "context_4h", None), "confidence", 0.0))
                        + 0.50 * _f(getattr(getattr(self._thesis, "context_15m", None), "confidence", 0.0))
                    )
            else:
                context = 0.0
            sig_term = math.tanh(significance / 5.0)
            dist_decay = math.exp(-max(0.0, distance_atr - 1.0) / 8.0)
            p = max(0.05, min(0.95, 0.18 + 0.34 * context + 0.30 * sig_term + 0.18 * dist_decay))
            cost_r = self._execution_cost_points / max(risk, _EPS)
            net_win_r = rr - cost_r
            net_loss_r = 1.0 + cost_r
            utility = p * net_win_r - (1.0 - p) * net_loss_r
            if utility > 0:
                audit["positive"] += 1
                candidates.append((utility, significance, -distance_atr, t, tp, rr, p, buffer, distance_atr, cost_r, net_win_r, net_loss_r))
            else:
                audit["non_positive_net_utility"] += 1
        audit["min_structural_rr"] = self._min_structural_rr
        audit["round_trip_cost_points"] = self._execution_cost_points
        audit["round_trip_cost_bps"] = self._execution_cost_bps
        self._last_analysis["target_audit"] = audit
        if not candidates:
            self._last_pool_plan = {"ts": time.time(), "role": "TP", "side": side,
                                    "summary": "no positive-utility opposing 15m+ pool"}
            self._last_analysis["target_block"] = "NO_POSITIVE_UTILITY_OPPOSING_15M_PLUS_POOL"
            return None
        best = max(candidates, key=lambda x: (x[0], x[1], x[2]))
        utility, significance, _, target, tp, rr, p, buffer, distance_atr, cost_r, net_win_r, net_loss_r = best
        self._last_pool_plan = {
            "ts": time.time(), "role": "TP", "side": side,
            "summary": f"{target.pool.timeframe} {target.pool.side.value}@{target.pool.price:.4f} grossRR={rr:.2f} netWinR={net_win_r:.2f} P={p:.2f} netEU={utility:+.2f}R",
        }
        self._last_analysis.update({
            "target_timeframe": str(getattr(target.pool, "timeframe", "")),
            "target_pool_price": _f(getattr(target.pool, "price", 0.0)),
            "target_tp_buffer": buffer, "target_distance_atr": distance_atr,
            "target_significance": significance, "target_rr": rr,
            "target_gross_rr": rr, "target_cost_r": cost_r,
            "target_net_win_r": net_win_r, "target_net_loss_r": net_loss_r,
            "delivery_probability": p, "delivery_utility_r": utility,
        })
        return target, tp, rr, utility, p


# The external strategy imports EntryEngine; the alias names the only entry authority.
EntryEngine = ICTLiquidityEntryEngine

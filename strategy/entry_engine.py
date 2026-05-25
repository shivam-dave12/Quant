"""
institutional_auction_entry.py — unified structural opportunity authority.

The strategy represents institutional auction behaviour as multiple explicit,
auditable structural archetypes under one order/risk authority:

    • liquidity-raid reversal: external stop run -> MSS -> FVG rebalance;
    • displacement continuation: directional delivery -> abnormal displacement /
      protected-swing break -> FVG mitigation; and
    • liquidity expansion retest: external liquidity consumed in delivery
      direction -> displacement imbalance -> retest.

Multi-timeframe liquidity destinations, robust structure and observable
microstructure state rank competing theses.  Microstructure is execution evidence,
not an unvalidated standalone alpha switch.  No score is presented as a win
probability unless it has been calibrated on labelled replay data.
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
    from strategy.auction_state import DeliveryEvidence, MicrostructureState, build_delivery_evidence, robust_displacement_body_threshold
except ImportError:  # pragma: no cover
    from auction_state import DeliveryEvidence, MicrostructureState, build_delivery_evidence, robust_displacement_body_threshold  # type: ignore

try:
    from strategy.liquidity_map import (
        LiquidityMapSnapshot, PoolTarget, SweepResult, TF_HIERARCHY,
        SWEEP_CONFIRMATION_WINDOW_SEC_BY_TF, STRUCTURAL_RAID_CONFIRMATION_WINDOW_SEC,
        _last_closed_candle_idx,
    )
except ImportError:  # pragma: no cover
    from liquidity_map import (  # type: ignore
        LiquidityMapSnapshot, PoolTarget, SweepResult, TF_HIERARCHY,
        SWEEP_CONFIRMATION_WINDOW_SEC_BY_TF, STRUCTURAL_RAID_CONFIRMATION_WINDOW_SEC,
        _last_closed_candle_idx,
    )

logger = logging.getLogger(__name__)


class EngineState(Enum):
    SCANNING = "SCANNING"
    CONTEXT_READY = "CONTEXT_READY"
    LIQUIDITY_RAID = "LIQUIDITY_RAID"
    EXECUTABLE = "EXECUTABLE"
    ENTERING = "ENTERING"
    IN_POSITION = "IN_POSITION"


class EntryType(Enum):
    LIQUIDITY_RAID_REVERSAL = "LIQUIDITY_RAID_REVERSAL"
    DISPLACEMENT_CONTINUATION = "DISPLACEMENT_CONTINUATION"
    LIQUIDITY_EXPANSION_RETEST = "LIQUIDITY_EXPANSION_RETEST"
    # Compatibility name retained for historical trade records only.
    ICT_LIQUIDITY = "LIQUIDITY_RAID_REVERSAL"


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
    delivery_probability: float = 0.0  # populated only by a calibrated replay model
    delivery_score: float = 0.0
    probability_calibrated: bool = False
    archetype: str = ""
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
    sweep: Optional[SweepResult]
    formed_at: float
    context_4h: _TrendContext
    context_15m: _TrendContext
    context_path: str
    context_delivery_score: float
    mss_level: float
    displacement_atr: float
    fvg: _FVG
    entry_type: EntryType = EntryType.LIQUIDITY_RAID_REVERSAL
    invalidation_anchor: float = 0.0
    evidence_score: float = 0.0
    structural_origin: str = "RAID_WICK"
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


def _cfg_float(name: str, default: float) -> float:
    if config is None:
        return float(default)
    return _f(getattr(config, name, default), default)


def _cfg_bool(name: str, default: bool = False) -> bool:
    if config is None:
        return bool(default)
    raw = getattr(config, name, default)
    if isinstance(raw, str):
        return raw.strip().lower() in ("1", "true", "yes", "on", "enabled")
    return bool(raw)


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


_MSS_INTERNAL_SWING_LOOKBACK_BARS = 20


def _select_mss_reference(candles: Sequence[Dict], side: str, raid_idx: int) -> Tuple[Optional[float], str, int, int]:
    """Return the latest confirmed internal swing invalidated by delivery.

    MSS is a break of the most recent protected internal swing preceding the
    raid, not a break of the lowest/highest price anywhere in an arbitrary
    window.  Requiring a confirmed pivot prevents manufacturing a permissive
    threshold when structure is not actually observable.
    """
    start = max(0, int(raid_idx) - _MSS_INTERNAL_SWING_LOOKBACK_BARS)
    pre = list(candles[start:int(raid_idx)])
    if len(pre) < 4:
        return None, "INSUFFICIENT_PRE_RAID_STRUCTURE", -1, len(pre)

    short_side = str(side or "").lower() == "short"
    pivots: List[Tuple[int, float]] = []
    for i in range(1, len(pre) - 1):
        previous, current, following = pre[i - 1], pre[i], pre[i + 1]
        if short_side:
            level = _f(current.get("l"))
            if level < _f(previous.get("l")) and level <= _f(following.get("l")):
                pivots.append((i, level))
        else:
            level = _f(current.get("h"))
            if level > _f(previous.get("h")) and level >= _f(following.get("h")):
                pivots.append((i, level))

    if not pivots:
        return None, "NO_CONFIRMED_PRE_RAID_INTERNAL_SWING", -1, len(pre)

    pivot_idx, level = pivots[-1]
    return level, "LATEST_CONFIRMED_INTERNAL_SWING", len(pre) - 1 - pivot_idx, len(pre)


def _sweep_key(sweep: SweepResult) -> tuple:
    pool = getattr(sweep, "pool", None)
    side = str(getattr(getattr(pool, "side", None), "value", "") or "")
    return (round(_f(getattr(pool, "price", 0.0)), 8), side, round(_f(getattr(sweep, "detected_at", 0.0)), 3))


class ICTLiquidityEntryEngine:
    """Unified institutional structural authority; all desks share one candidate ledger."""

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
        self._last_microstructure: MicrostructureState = MicrostructureState.empty()
        self._delivery_evidence: Optional[DeliveryEvidence] = None
        self._candidate_replacements: int = 0
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
        delivery evidence. Only a unanimous, explicit HTF delivery against the raid is
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
            if (_cfg_bool("ICT_SELECTIVITY_MODE", True)
                    and not _cfg_bool("ICT_ALLOW_COUNTER_DELIVERY_RAIDS", False)):
                score = max(0.0, 0.10 + 0.20 * _f(sweep_quality) - 0.25 * opposing_strength)
                return _ContextDecision(side, False, path,
                                        "COUNTER_DELIVERY_RAID_BLOCKED_BY_SELECTIVITY",
                                        score, False, supporting, opposing, ranging)
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
               candles_4h: Optional[List[Dict]] = None,
               candles_1h: Optional[List[Dict]] = None,
               micro_state: Optional[MicrostructureState] = None) -> None:
        self._last_scan_skip = {}
        if atr <= _EPS or price <= 0:
            self._last_analysis = {
                "model": "INSTITUTIONAL_AUCTION_4H_1H_15M_5M", "state": self._state.value,
                "price": price, "entry_5m_atr": atr, "block_reason": "INVALID_PRICE_OR_5M_ATR",
                "trigger": "WAIT", "authority": "UNIFIED_STRUCTURAL_AUCTION",
            }
            self._last_scan_skip = {"invalid_price_or_5m_atr": 1}
            return
        self._expire(now)
        c5 = _closed(candles_5m, 24, "5m", now)
        c15 = _closed(candles_15m, 24, "15m", now)
        c1h = _closed(candles_1h, 20, "1h", now) if candles_1h else []
        c4h = _closed(candles_4h, 20, "4h", now)
        self._last_microstructure = micro_state if micro_state is not None else MicrostructureState.empty(now)
        if not c5 or not c15 or not c4h:
            self._last_analysis = {
                "model": "INSTITUTIONAL_AUCTION_4H_1H_15M_5M", "state": self._state.value,
                "price": price, "entry_5m_atr": atr, "bars_5m": len(c5),
                "bars_15m": len(c15), "bars_4h": len(c4h),
                "block_reason": "TIMEFRAME_WARMUP", "trigger": "WAIT",
                "authority": "UNIFIED_STRUCTURAL_AUCTION",
            }
            self._last_scan_skip = {"timeframe_warmup": 1}
            return

        atr4h = _timeframe_atr(c4h)
        atr15m = _timeframe_atr(c15)
        if atr4h <= _EPS or atr15m <= _EPS:
            self._last_analysis = {
                "model": "INSTITUTIONAL_AUCTION_4H_1H_15M_5M", "state": self._state.value,
                "price": price, "entry_5m_atr": atr, "context_4h_atr": atr4h,
                "context_15m_atr": atr15m, "block_reason": "TIMEFRAME_ATR_WARMUP",
                "trigger": "WAIT", "authority": "UNIFIED_STRUCTURAL_AUCTION",
            }
            self._last_scan_skip = {"timeframe_atr_warmup": 1}
            return
        ctx4 = _robust_trend(c4h, atr4h, min(32, len(c4h)))
        ctx15 = _robust_trend(c15, atr15m, min(56, len(c15)))
        atr1h = _timeframe_atr(c1h) if c1h else 0.0
        ctx1h = _robust_trend(c1h, atr1h, min(44, len(c1h))) if atr1h > _EPS else _TrendContext(0, 0.0, 0.0, 0.0, 0.0)
        self._delivery_evidence = build_delivery_evidence(
            liq_snapshot, price, atr,
            ((ctx4.signed_score, 0.50), (ctx1h.signed_score, 0.20), (ctx15.signed_score, 0.30)),
            self._last_microstructure,
        )
        aligned_side = ctx4.side if ctx4.side != 0 and ctx4.side == ctx15.side else 0
        aligned_label = "long" if aligned_side > 0 else ("short" if aligned_side < 0 else "none")
        self._last_analysis = {
            "model": "INSTITUTIONAL_AUCTION_4H_1H_15M_5M",
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
            "context_15m_atr": atr15m, "context_1h": ctx1h.label,
            "context_1h_score": ctx1h.signed_score, "context_1h_atr": atr1h,
            "context_direction_threshold": _CONTEXT_DIRECTION_THRESHOLD,
            "min_structural_rr": self._min_structural_rr,
            "max_structural_rr_reference": self._max_structural_rr_reference,
            "context_aligned": bool(aligned_side), "context_direction": aligned_label,
            "context_permission": False, "context_bias_path": "AWAITING_5M_DOL",
            "context_delivery_score": 0.0,
            "entry_5m_atr": atr, "atr_percentile": self._atr_pctile,
            "bars_5m": len(c5), "bars_15m": len(c15), "bars_4h": len(c4h),
            "authority": "UNIFIED_STRUCTURAL_AUCTION", "trigger": "WAIT", "block_reason": "EVALUATING",
            "delivery_score": self._delivery_evidence.signed_score,
            "delivery_preferred_side": self._delivery_evidence.preferred_side,
            "delivery_trend_component": self._delivery_evidence.trend_component,
            "delivery_liquidity_pull_component": self._delivery_evidence.liquidity_pull_component,
            "delivery_microstructure_component": self._delivery_evidence.microstructure_component,
            "micro_book_fresh": self._last_microstructure.fresh,
            "micro_book_age_sec": self._last_microstructure.book_age_sec,
            "micro_depth_imbalance": self._last_microstructure.depth_imbalance,
            "micro_trade_imbalance": self._last_microstructure.trade_imbalance,
            "microprice_edge_atr": self._last_microstructure.microprice_edge_atr,
            "probability_calibrated": False,
        }
        if self._state in (EngineState.ENTERING, EngineState.IN_POSITION):
            self._record_block("POSITION_LIFECYCLE_ACTIVE", trigger=self._state.value)
            return
        if self._signal is not None:
            self._last_analysis.update({"state": EngineState.EXECUTABLE.value, "trigger": "SIGNAL_PENDING_EXECUTION", "block_reason": "NONE"})
            return

        if self._thesis is not None:
            if now - self._thesis.formed_at > 1800.0:
                self._last_analysis.update({"expired_thesis_age_sec": now - self._thesis.formed_at})
                self._thesis = None
                self._state = EngineState.SCANNING
            else:
                self._try_reprice_thesis(self._thesis, liq_snapshot, price, atr, now)
                if self._signal is not None:
                    return
                # A waiting reprice is a live candidate, not a global lock.  Continue
                # scanning so a stronger/newer structural opportunity can replace it.
                self._last_analysis["candidate_ledger_active"] = True

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
            # A delivery displacement without a contemporary stop raid is an
            # independent continuation archetype. When a live raid exists below,
            # the raid-specific models own attribution and invalidation geometry.
            continuation = self._build_displacement_continuation_thesis(ctx4, ctx15, c5, atr, now)
            if self._adopt_candidate(continuation) and self._thesis is not None:
                self._try_reprice_thesis(self._thesis, liq_snapshot, price, atr, now)
                if self._signal is not None:
                    return
            # Do not overwrite an active displacement/retest thesis with a raid-only
            # status.  The candidate ledger must preserve the actual reason an
            # institutional setup is still waiting for execution.
            if self._thesis is not None:
                self._last_analysis.update({
                    "candidate_ledger_active": True,
                    "candidate_archetype": self._thesis.entry_type.value,
                })
                return
            if parent_htf:
                self._record_block("AWAITING_FRESH_5M_CONFIRMATION_AFTER_HTF_RAID",
                                   trigger="WAIT_FOR_5M_RAID_MSS_FVG")
            else:
                self._record_block("AWAITING_STRUCTURAL_OPPORTUNITY")
            return
        for sweep in sorted(fresh, key=lambda sw: _f(getattr(sw, "quality", 0.0)), reverse=True):
            expansion = self._build_liquidity_expansion_thesis(sweep, ctx4, ctx15, c5, atr, now)
            if self._adopt_candidate(expansion) and self._thesis is not None:
                self._try_reprice_thesis(self._thesis, liq_snapshot, price, atr, now)
                if self._signal is not None:
                    return
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
            if self._adopt_candidate(thesis) and self._thesis is not None:
                self._last_analysis["state"] = self._state.value
                self._try_reprice_thesis(self._thesis, liq_snapshot, price, atr, now)
                if self._signal is not None:
                    return
        if self._last_analysis.get("block_reason") == "EVALUATING" and self._thesis is None:
            self._record_block("NO_EXECUTABLE_STRUCTURAL_OPPORTUNITY")

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
            "mode": "INSTITUTIONAL_AUCTION",
            "archetype": self._thesis.entry_type.value,
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
            if age > STRUCTURAL_RAID_CONFIRMATION_WINDOW_SEC or _sweep_key(sw) in self._processed:
                continue
            out.append(sw)
        return out

    def _fresh_parent_htf_sweeps(self, snap: LiquidityMapSnapshot, now: float) -> List[SweepResult]:
        out = []
        max_age_by_tf = {
            tf: horizon for tf, horizon in SWEEP_CONFIRMATION_WINDOW_SEC_BY_TF.items()
            if tf != "5m"
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

        # MSS must reference the latest confirmed protected internal swing.  The
        # former absolute 12-bar high/low demanded an unrelated range-extreme
        # break and suppressed otherwise valid raid reversals/continuations.
        mss, mss_source, mss_age_bars, pre_bars = _select_mss_reference(candles_5m, side, idx)
        self._last_analysis.update({
            "mss_source": mss_source, "mss_age_bars": mss_age_bars,
            "mss_pre_raid_bars": pre_bars,
        })
        if mss is None:
            self._record_block(mss_source, pre_raid_bars=pre_bars,
                               mss_source=mss_source, mss_age_bars=mss_age_bars)
            return None
        if side == "long":
            displacement = (recent_close - wick) / max(atr, _EPS)
            mss_broken = recent_close > mss
        else:
            displacement = (wick - recent_close) / max(atr, _EPS)
            mss_broken = recent_close < mss
        self._last_analysis.update({
            "mss_level": mss, "mss_broken": bool(mss_broken),
            "mss_source": mss_source, "mss_age_bars": mss_age_bars,
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
        evidence_score = (self._delivery_evidence.score_for(side)
                          if self._delivery_evidence is not None else context_decision.delivery_score)
        return _Thesis(
            sweep_key=_sweep_key(sweep), side=side, sweep=sweep, formed_at=now,
            context_4h=ctx4, context_15m=ctx15,
            context_path=context_decision.path,
            context_delivery_score=context_decision.delivery_score,
            mss_level=mss,
            displacement_atr=displacement, fvg=fvg,
            entry_type=EntryType.LIQUIDITY_RAID_REVERSAL,
            invalidation_anchor=wick, evidence_score=evidence_score,
            structural_origin="RAID_WICK",
        )

    @staticmethod
    def _candle_key(row: Dict, fallback: int) -> int:
        try:
            return int(float(row.get("t", fallback)))
        except Exception:
            return int(fallback)

    def _candidate_priority(self, thesis: _Thesis) -> float:
        # Displacement is structural proof; evidence ranks competing valid theses.
        return max(0.0, thesis.evidence_score) + 0.25 * math.tanh(max(0.0, thesis.displacement_atr))

    def _selectivity_block(self, thesis: _Thesis, rr: float, rank_score: float,
                           delivery_score: float) -> Optional[Tuple[str, Dict[str, Any]]]:
        if not _cfg_bool("ICT_SELECTIVITY_MODE", True):
            return None
        path = str(getattr(thesis, "context_path", "") or "")
        archetype = thesis.entry_type
        displacement = max(0.0, _f(getattr(thesis, "displacement_atr", 0.0), 0.0))
        context_score = max(0.0, _f(getattr(thesis, "context_delivery_score", 0.0), 0.0))

        details = {
            "selectivity_archetype": archetype.value,
            "selectivity_path": path,
            "selectivity_displacement_atr": displacement,
            "selectivity_context_delivery_score": context_score,
            "selectivity_delivery_score": delivery_score,
            "selectivity_target_rank_score": rank_score,
            "selectivity_rr": rr,
        }

        if (path == "COUNTER_DELIVERY_RAID_REQUIRES_5M_PROOF"
                and not _cfg_bool("ICT_ALLOW_COUNTER_DELIVERY_RAIDS", False)):
            return "COUNTER_DELIVERY_RAID_BLOCKED_BY_SELECTIVITY", details

        min_context = _cfg_float("ICT_ENTRY_MIN_CONTEXT_DELIVERY_SCORE", 0.25)
        if context_score < min_context:
            details["selectivity_required_context_delivery_score"] = min_context
            return "WEAK_CONTEXT_DELIVERY_BLOCKED_BY_SELECTIVITY", details

        min_delivery = _cfg_float("ICT_ENTRY_MIN_DELIVERY_SCORE", 0.55)
        min_rank = _cfg_float("ICT_ENTRY_MIN_TARGET_RANK_SCORE", 1.50)
        if archetype == EntryType.DISPLACEMENT_CONTINUATION:
            min_delivery = max(min_delivery, _cfg_float("ICT_CONTINUATION_MIN_DELIVERY_SCORE", 0.58))
            min_rank = max(min_rank, _cfg_float("ICT_CONTINUATION_MIN_TARGET_RANK_SCORE", 2.40))
            min_disp = _cfg_float("ICT_ENTRY_MIN_DISPLACEMENT_ATR_CONTINUATION", 1.25)
        else:
            min_disp = _cfg_float("ICT_ENTRY_MIN_DISPLACEMENT_ATR_RAID", 1.20)

        if displacement < min_disp:
            details["selectivity_required_displacement_atr"] = min_disp
            return "DISPLACEMENT_BELOW_SELECTIVITY_FLOOR", details
        if delivery_score < min_delivery:
            details["selectivity_required_delivery_score"] = min_delivery
            return "DELIVERY_SCORE_BELOW_SELECTIVITY_FLOOR", details
        if rank_score < min_rank:
            details["selectivity_required_target_rank_score"] = min_rank
            return "TARGET_RANK_BELOW_SELECTIVITY_FLOOR", details

        if path == "PARTIAL_HTF_DOL":
            partial_delivery = _cfg_float("ICT_PARTIAL_HTF_MIN_DELIVERY_SCORE", 0.70)
            partial_disp = _cfg_float("ICT_PARTIAL_HTF_MIN_DISPLACEMENT_ATR", 2.00)
            if delivery_score < partial_delivery or displacement < partial_disp:
                details.update({
                    "selectivity_required_partial_delivery_score": partial_delivery,
                    "selectivity_required_partial_displacement_atr": partial_disp,
                })
                return "PARTIAL_HTF_DOL_REQUIRES_EXCEPTIONAL_PROOF", details

        return None

    def _adopt_candidate(self, candidate: Optional[_Thesis]) -> bool:
        if candidate is None:
            return False
        if self._thesis is None:
            self._thesis = candidate
            self._state = EngineState.LIQUIDITY_RAID
            return True
        if self._thesis.sweep_key == candidate.sweep_key:
            return True
        current_priority = self._candidate_priority(self._thesis)
        candidate_priority = self._candidate_priority(candidate)
        if candidate_priority > current_priority:
            self._candidate_replacements += 1
            self._last_analysis.update({
                "candidate_replaced": str(self._thesis.entry_type.value),
                "candidate_replacement_priority_old": current_priority,
                "candidate_replacement_priority_new": candidate_priority,
                "candidate_replacements": self._candidate_replacements,
            })
            self._thesis = candidate
            self._state = EngineState.LIQUIDITY_RAID
            return True
        return False

    def _latest_structural_displacement(self, candles_5m: List[Dict], side: str, atr: float,
                                        earliest_idx: int = 2) -> Optional[Tuple[_FVG, float, float, float, str]]:
        """Return a statistically abnormal displacement/FVG and its invalidation anchor.

        Thresholds are estimated from prior bodies in the same instrument/tape,
        avoiding a hardcoded momentum multiplier.  A continuation must also
        close beyond the latest protected internal swing before its FVG is valid.
        """
        start = max(2, int(earliest_idx), len(candles_5m) - 14)
        for i in range(len(candles_5m) - 1, start - 1, -1):
            before, impulse, after = candles_5m[i - 2], candles_5m[i - 1], candles_5m[i]
            if side == "long":
                low, high = _f(before.get("h")), _f(after.get("l"))
                body = (_f(impulse.get("c")) - _f(impulse.get("o"))) / max(atr, _EPS)
            else:
                low, high = _f(after.get("h")), _f(before.get("l"))
                body = (_f(impulse.get("o")) - _f(impulse.get("c"))) / max(atr, _EPS)
            if high <= low or body <= 0:
                continue
            history = candles_5m[max(0, i - 43):i - 2]
            threshold = robust_displacement_body_threshold(history, atr)
            if body <= threshold:
                continue
            mss, source, _, _ = _select_mss_reference(candles_5m, side, i - 1)
            if mss is None:
                continue
            broken = (_f(impulse.get("c")) > mss) if side == "long" else (_f(impulse.get("c")) < mss)
            if not broken:
                continue
            anchor_window = candles_5m[max(0, i - 3):i + 1]
            anchor = min(_f(row.get("l")) for row in anchor_window) if side == "long" else max(_f(row.get("h")) for row in anchor_window)
            return _FVG(side, low, high, i, body), mss, anchor, threshold, source
        return None

    def _build_displacement_continuation_thesis(self, ctx4: _TrendContext, ctx15: _TrendContext,
                                                candles_5m: List[Dict], atr: float, now: float) -> Optional[_Thesis]:
        evidence = self._delivery_evidence
        if evidence is None or evidence.preferred_side not in ("long", "short"):
            return None
        side = evidence.preferred_side
        result = self._latest_structural_displacement(candles_5m, side, atr)
        if result is None:
            return None
        fvg, mss, anchor, threshold, source = result
        context_score = max(0.0, evidence.score_for(side))
        if _cfg_bool("ICT_SELECTIVITY_MODE", True):
            min_disp = _cfg_float("ICT_ENTRY_MIN_DISPLACEMENT_ATR_CONTINUATION", 1.25)
            if fvg.displacement_atr < min_disp:
                self._record_block(
                    "CONTINUATION_DISPLACEMENT_BELOW_SELECTIVITY_FLOOR",
                    continuation_body_atr=fvg.displacement_atr,
                    continuation_required_body_atr=min_disp,
                )
                return None
            min_context = _cfg_float("ICT_CONTINUATION_MIN_CONTEXT_SCORE", 0.25)
            if context_score < min_context:
                self._record_block(
                    "CONTINUATION_WEAK_CONTEXT_DELIVERY_BLOCKED_BY_SELECTIVITY",
                    continuation_context_delivery_score=context_score,
                    continuation_required_context_score=min_context,
                )
                return None
        key = ("DISPLACEMENT_CONTINUATION", side, self._candle_key(candles_5m[fvg.index], fvg.index))
        self._last_analysis.update({
            "candidate_archetype": EntryType.DISPLACEMENT_CONTINUATION.value,
            "continuation_mss_source": source, "continuation_mss_level": mss,
            "continuation_body_atr": fvg.displacement_atr,
            "continuation_dynamic_body_threshold_atr": threshold,
        })
        return _Thesis(
            sweep_key=key, side=side, sweep=None, formed_at=now, context_4h=ctx4, context_15m=ctx15,
            context_path="LIQUIDITY_DESTINATION_DISPLACEMENT",
            context_delivery_score=context_score, mss_level=mss,
            displacement_atr=fvg.displacement_atr, fvg=fvg,
            entry_type=EntryType.DISPLACEMENT_CONTINUATION, invalidation_anchor=anchor,
            evidence_score=context_score, structural_origin="DISPLACEMENT_ORIGIN",
        )

    def _build_liquidity_expansion_thesis(self, sweep: SweepResult, ctx4: _TrendContext, ctx15: _TrendContext,
                                          candles_5m: List[Dict], atr: float, now: float) -> Optional[_Thesis]:
        pool = getattr(sweep, "pool", None)
        pool_side = str(getattr(getattr(pool, "side", None), "value", "") or "").upper()
        # A BSL run that holds above the pool is a long expansion; an SSL run
        # that holds below the pool is a short expansion, distinct from reversal.
        side = "long" if pool_side == "BSL" else ("short" if pool_side == "SSL" else "")
        evidence = self._delivery_evidence
        if not side or evidence is None or evidence.score_for(side) <= 0:
            return None
        pool_px = _f(getattr(pool, "price", 0.0))
        recent_close = _f(candles_5m[-1].get("c"))
        held_through = recent_close > pool_px if side == "long" else recent_close < pool_px
        if pool_px <= 0 or not held_through:
            return None
        idx = max(2, int(getattr(sweep, "sweep_candle_idx", len(candles_5m) - 3) or 2))
        result = self._latest_structural_displacement(candles_5m, side, atr, earliest_idx=idx + 1)
        if result is None:
            return None
        fvg, mss, anchor, threshold, source = result
        context_score = max(0.0, evidence.score_for(side))
        if _cfg_bool("ICT_SELECTIVITY_MODE", True):
            min_disp = _cfg_float("ICT_ENTRY_MIN_DISPLACEMENT_ATR_RAID", 1.20)
            min_context = _cfg_float("ICT_ENTRY_MIN_CONTEXT_DELIVERY_SCORE", 0.25)
            if fvg.displacement_atr < min_disp:
                self._record_block(
                    "EXPANSION_DISPLACEMENT_BELOW_SELECTIVITY_FLOOR",
                    expansion_body_atr=fvg.displacement_atr,
                    expansion_required_body_atr=min_disp,
                )
                return None
            if context_score < min_context:
                self._record_block(
                    "EXPANSION_WEAK_CONTEXT_DELIVERY_BLOCKED_BY_SELECTIVITY",
                    expansion_context_delivery_score=context_score,
                    expansion_required_context_score=min_context,
                )
                return None
        key = ("LIQUIDITY_EXPANSION_RETEST", side, round(pool_px, 8), self._candle_key(candles_5m[fvg.index], fvg.index))
        self._last_analysis.update({
            "candidate_archetype": EntryType.LIQUIDITY_EXPANSION_RETEST.value,
            "expansion_pool_price": pool_px, "expansion_pool_side": pool_side,
            "expansion_mss_source": source, "expansion_body_atr": fvg.displacement_atr,
            "expansion_dynamic_body_threshold_atr": threshold,
        })
        return _Thesis(
            sweep_key=key, side=side, sweep=sweep, formed_at=now, context_4h=ctx4, context_15m=ctx15,
            context_path="LIQUIDITY_EXPANSION_DELIVERY",
            context_delivery_score=context_score, mss_level=mss,
            displacement_atr=fvg.displacement_atr, fvg=fvg,
            entry_type=EntryType.LIQUIDITY_EXPANSION_RETEST, invalidation_anchor=anchor,
            evidence_score=context_score, structural_origin="EXPANSION_ORIGIN",
        )

    def _try_reprice_thesis(self, thesis: _Thesis, snap: LiquidityMapSnapshot,
                            price: float, atr: float, now: float) -> None:
        zone_tol = 0.08 * atr
        low_bound, high_bound = thesis.fvg.low - zone_tol, thesis.fvg.high + zone_tol
        in_reprice_zone = low_bound <= price <= high_bound
        eq_tol = 0.02 * atr
        if thesis.side == "long":
            fvg_rebalanced = in_reprice_zone and price <= thesis.fvg.equilibrium + eq_tol
            rebalance_trigger = thesis.fvg.equilibrium + eq_tol
        else:
            fvg_rebalanced = in_reprice_zone and price >= thesis.fvg.equilibrium - eq_tol
            rebalance_trigger = thesis.fvg.equilibrium - eq_tol
        distance_to_zone = 0.0 if in_reprice_zone else min(abs(price - low_bound), abs(price - high_bound))
        self._last_analysis.update({
            "state": self._state.value, "side": thesis.side,
            "context_permission": True,
            "context_bias_path": thesis.context_path,
            "context_delivery_score": thesis.context_delivery_score,
            "context_direction": thesis.side,
            "fvg_low": thesis.fvg.low, "fvg_high": thesis.fvg.high,
            "fvg_equilibrium": thesis.fvg.equilibrium, "fvg_tolerance": zone_tol,
            "fvg_rebalanced": bool(fvg_rebalanced),
            "fvg_rebalance_trigger": rebalance_trigger,
            "fvg_distance_atr": distance_to_zone / max(atr, _EPS),
            "displacement_atr": thesis.displacement_atr,
            "thesis_age_sec": max(0.0, now - thesis.formed_at),
        })
        if not in_reprice_zone:
            thesis.last_reason = "MSS confirmed; awaiting 5m FVG rebalance"
            self._record_block("AWAITING_FVG_REBALANCE", trigger="AWAITING_FVG_REBALANCE")
            return
        if not fvg_rebalanced:
            thesis.last_reason = "FVG touched; awaiting equilibrium rebalance"
            self._record_block("AWAITING_FVG_EQUILIBRIUM_REBALANCE", trigger="AWAITING_FVG_EQUILIBRIUM_REBALANCE")
            return
        entry = price
        stop = self._structural_stop(thesis, atr)
        anchor = _f(getattr(thesis, "invalidation_anchor", 0.0))
        if anchor <= 0 and thesis.sweep is not None:
            anchor = _f(getattr(thesis.sweep, "wick_extreme", 0.0))
        clearance = abs(stop - anchor) if stop is not None else 0.0
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
            thesis.last_reason = "no opposing higher-timeframe liquidity destination with positive net R"
            self._record_block("AWAITING_POSITIVE_NET_R_LIQUIDITY_TARGET")
            return
        target_obj, tp, rr, rank_score, delivery_score = target
        selectivity_block = self._selectivity_block(thesis, rr, rank_score, delivery_score)
        if selectivity_block is not None:
            reason, details = selectivity_block
            thesis.last_reason = f"institutional selectivity rejected setup: {reason}"
            self._record_block(reason, trigger="SELECTIVITY_FILTER", **details)
            return
        quality = {
            "context_4h": thesis.context_4h.confidence,
            "context_15m": thesis.context_15m.confidence,
            "context_delivery_score": thesis.context_delivery_score,
            "raid_quality": _f(getattr(thesis.sweep, "quality", 0.0)) if thesis.sweep is not None else 0.0,
            "displacement_atr": thesis.displacement_atr,
            "delivery_score": delivery_score,
            "target_rank_score": rank_score,
            "probability_calibrated": False,
            "archetype": thesis.entry_type.value,
        }
        explanation = (
            f"4H={thesis.context_4h.label}({thesis.context_4h.confidence:.2f}) | "
            f"15m={thesis.context_15m.label}({thesis.context_15m.confidence:.2f}) | "
            f"bias={thesis.context_path}({thesis.context_delivery_score:.2f}) | "
            f"archetype={thesis.entry_type.value} MSS/FVG | displacement={thesis.displacement_atr:.2f}ATR | "
            f"deliveryScore={delivery_score:.2f} rankScore={rank_score:.2f} (uncalibrated)"
        )
        self._signal = EntrySignal(
            side=thesis.side, entry_type=thesis.entry_type, entry_price=entry,
            sl_price=stop, tp_price=tp, rr_ratio=rr, target_pool=target_obj,
            sweep_result=thesis.sweep, delivery_probability=0.0, delivery_score=delivery_score,
            probability_calibrated=False, archetype=thesis.entry_type.value,
            reason=explanation, structural_validation=f"{thesis.entry_type.value}: protected structure / displacement / FVG repricing / HTF liquidity target",
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
            "delivery_probability": None, "delivery_score": delivery_score,
            "probability_calibrated": False, "target_rank_score": rank_score,
            "delivery_utility_r": None,
            "context_bias_path": thesis.context_path,
            "context_delivery_score": thesis.context_delivery_score,
            "target_timeframe": str(getattr(target_obj.pool, "timeframe", "")),
            "target_pool_price": _f(getattr(target_obj.pool, "price", 0.0)),
            "target_significance": _f(getattr(target_obj, "significance", 0.0)),
        })
        logger.info("INSTITUTIONAL_AUCTION ENTRY READY archetype=%s %s @ %.4f | SL=%.4f TP=%.4f grossRR=%.2f netWinR=%s rank=%s calibratedP=N/A | %s",
                    thesis.entry_type.value, thesis.side.upper(), entry, stop, tp, rr,
                    f"{self._last_analysis.get('target_net_win_r'):.2f}" if self._last_analysis.get('target_net_win_r') is not None else "N/A",
                    f"{rank_score:+.2f}", explanation)

    def _structural_stop(self, thesis: _Thesis, atr: float) -> Optional[float]:
        wick = _f(getattr(thesis, "invalidation_anchor", 0.0))
        if wick <= 0 and thesis.sweep is not None:
            wick = _f(getattr(thesis.sweep, "wick_extreme", 0.0))
        if wick <= 0:
            return None
        # The stop is behind the thesis-specific structural invalidation anchor;
        # volatility sizes clearance but never substitutes for structure.
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
                 "gross_rr_above_policy_cap": 0, "non_positive_net_reward": 0,
                 "eligible": 0, "positive": 0}
        max_rr_cap = self._max_structural_rr_reference
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
            if max_rr_cap > 0.0 and rr > max_rr_cap:
                audit["gross_rr_above_policy_cap"] += 1
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
            dist_reachability = math.exp(-max(0.0, distance_atr - 1.0) / 8.0)
            delivery_score = max(0.0, min(0.99, 0.46 * context + 0.34 * sig_term + 0.20 * dist_reachability))
            cost_r = self._execution_cost_points / max(risk, _EPS)
            net_win_r = rr - cost_r
            # A target is selected by structural score and net R; the score is
            # explicitly not converted into an uncalibrated win probability.
            rank_score = delivery_score * max(0.0, net_win_r)
            if net_win_r > 0.0 and rank_score > 0.0:
                audit["positive"] += 1
                candidates.append({
                    "rank_score": rank_score,
                    "significance": significance,
                    "tf_rank": tf_rank,
                    "target": t,
                    "tp": tp,
                    "rr": rr,
                    "delivery_score": delivery_score,
                    "buffer": buffer,
                    "distance_atr": distance_atr,
                    "cost_r": cost_r,
                    "net_win_r": net_win_r,
                })
            else:
                audit["non_positive_net_reward"] += 1
        audit["min_structural_rr"] = self._min_structural_rr
        audit["max_structural_rr_reference"] = max_rr_cap
        audit["target_selection_model"] = "LIQUIDITY_GRAPH_NET_R_RANK"
        audit["probability_calibrated"] = False
        audit["round_trip_cost_points"] = self._execution_cost_points
        audit["round_trip_cost_bps"] = self._execution_cost_bps
        self._last_analysis["target_audit"] = audit
        if not candidates:
            if audit.get("gross_rr_above_policy_cap"):
                summary = f"no opposing 15m+ pool inside policy maxRR={max_rr_cap:.2f}"
                block = "NO_POLICY_BOUNDED_OPPOSING_15M_PLUS_POOL"
            else:
                summary = "no positive-net-R opposing 15m+ liquidity destination"
                block = "NO_POSITIVE_NET_R_OPPOSING_15M_PLUS_POOL"
            self._last_pool_plan = {"ts": time.time(), "role": "TP", "side": side,
                                    "summary": summary}
            self._last_analysis["target_block"] = block
            return None
        best = max(
            candidates,
            key=lambda x: (
                x["rank_score"],
                x["tf_rank"],
                x["significance"],
                -x["distance_atr"],
            ),
        )
        rank_score = float(best["rank_score"])
        significance = float(best["significance"])
        target = best["target"]
        tp = float(best["tp"])
        rr = float(best["rr"])
        delivery_score = float(best["delivery_score"])
        buffer = float(best["buffer"])
        distance_atr = float(best["distance_atr"])
        cost_r = float(best["cost_r"])
        net_win_r = float(best["net_win_r"])
        self._last_pool_plan = {
            "ts": time.time(), "role": "TP", "side": side,
            "summary": f"LIQUIDITY_GRAPH_NET_R_RANK {target.pool.timeframe} {target.pool.side.value}@{target.pool.price:.4f} grossRR={rr:.2f} netWinR={net_win_r:.2f} deliveryScore={delivery_score:.2f} rank={rank_score:+.2f}",
        }
        self._last_analysis.update({
            "target_timeframe": str(getattr(target.pool, "timeframe", "")),
            "target_pool_price": _f(getattr(target.pool, "price", 0.0)),
            "target_tp_buffer": buffer, "target_distance_atr": distance_atr,
            "target_significance": significance, "target_rr": rr,
            "target_gross_rr": rr, "target_cost_r": cost_r,
            "target_net_win_r": net_win_r,
            "target_selection_model": "LIQUIDITY_GRAPH_NET_R_RANK",
            "target_policy_max_rr": max_rr_cap,
            "delivery_probability": None, "probability_calibrated": False,
            "delivery_score": delivery_score, "target_rank_score": rank_score,
            "delivery_utility_r": None,
        })
        return target, tp, rr, rank_score, delivery_score


# The external strategy imports EntryEngine; this alias names the unified structural authority.
EntryEngine = ICTLiquidityEntryEngine

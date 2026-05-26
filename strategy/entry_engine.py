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
from datetime import datetime, timedelta, timezone
try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore
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
    from strategy.market_state import AuctionNarrative, build_auction_narrative
except ImportError:  # pragma: no cover
    from market_state import AuctionNarrative, build_auction_narrative  # type: ignore

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
    # GROWW NIFTY profile: intraday trend pullback is entered at a freshly
    # reclaimed liquidity sweep and monetised into the nearest live pool.
    NIFTY_TREND_SWEEP_SCALP = "NIFTY_TREND_SWEEP_SCALP"
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
class _IntradayRegime:
    label: str
    side: str
    signed_score: float
    strength: float
    aggression: str
    reason: str


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
    market_phase: str = "UNCLASSIFIED"
    auction_control_side: str = "none"
    auction_control_score: float = 0.0
    execution_posture: str = "OBSERVE"
    auction_risk_scalar: float = 0.35
    market_state_thesis: str = ""
    last_reason: str = "waiting for FVG repricing"


@dataclass(frozen=True)
class _PDArrayConfluence:
    score: float
    block: str
    dealing_low: float
    dealing_high: float
    dealing_mid: float
    pd_position: float
    pd_zone: str
    ote_retracement: float
    ote_score: float
    fvg_score: float
    order_block_low: float
    order_block_high: float
    order_block_score: float
    killzone_label: str
    killzone_score: float


@dataclass(frozen=True)
class _EntryZoneCandidate:
    """Observable repricing area ranked inside the current auction thesis.

    A zone is not accepted merely because an FVG or candle-labelled order block
    exists.  It must explain where displacement originated, whether the recent
    liquidity raid funded that move, whether an order-block origin overlaps the
    imbalance, and whether price is actually repricing there.  The score is a
    relative structural ranking, never a win probability.
    """
    fvg: _FVG
    timeframe: str
    order_block_low: float
    order_block_high: float
    displacement_score: float
    order_block_overlap: float
    higher_tf_overlap: float
    raid_fuel_score: float
    freshness_score: float
    phase_alignment: float
    reprice_score: float
    structural_score: float
    classification: str
    reason: str

    @property
    def low(self) -> float:
        return self.fvg.low

    @property
    def high(self) -> float:
        return self.fvg.high

    @property
    def equilibrium(self) -> float:
        return self.fvg.equilibrium

    def payload(self, selected: bool = False) -> Dict[str, Any]:
        return {
            "zone_type": "FVG_OB_REPRICING_ZONE",
            "timeframe": self.timeframe,
            "side": self.fvg.side,
            "low": self.low,
            "high": self.high,
            "equilibrium": self.equilibrium,
            "order_block_low": self.order_block_low,
            "order_block_high": self.order_block_high,
            "displacement_atr": self.fvg.displacement_atr,
            "displacement_score": self.displacement_score,
            "order_block_overlap": self.order_block_overlap,
            "higher_tf_overlap": self.higher_tf_overlap,
            "raid_fuel_score": self.raid_fuel_score,
            "freshness_score": self.freshness_score,
            "phase_alignment": self.phase_alignment,
            "reprice_score": self.reprice_score,
            "structural_score": self.structural_score,
            "classification": self.classification,
            "reason": self.reason,
            "selected": bool(selected),
        }


@dataclass(frozen=True)
class _StopPlan:
    price: float
    structural_anchor: float
    outer_protected_liquidity: float
    clearance: float
    protected_cluster_mass: float
    selected_pools: Tuple[Dict[str, Any], ...]
    noise_pools: Tuple[Dict[str, Any], ...]
    model: str = "LIQUIDITY_PROTECTED_INVALIDATION_CLUSTER"

    def payload(self) -> Dict[str, Any]:
        return {
            "stop_selection_model": self.model,
            "structural_stop": self.price,
            "stop_structural_anchor": self.structural_anchor,
            "stop_outer_protected_liquidity": self.outer_protected_liquidity,
            "stop_outer_clearance": self.clearance,
            "stop_clearance": abs(self.price - self.structural_anchor),
            "stop_protected_cluster_mass": self.protected_cluster_mass,
            "stop_protected_pool_count": len(self.selected_pools),
            "stop_noise_pool_count": len(self.noise_pools),
            "stop_protected_pools": list(self.selected_pools),
            "stop_noise_pools": list(self.noise_pools),
        }


@dataclass(frozen=True)
class _SetupQualityDossier:
    score: float
    grade: str
    block: str
    context_score: float
    displacement_score: float
    pd_array_score: float
    target_delivery_score: float
    target_rank_score: float
    net_r_score: float
    gauntlet_score: float
    microstructure_score: float
    cost_score: float

    def as_payload(self) -> Dict[str, Any]:
        return {
            "setup_dossier_score": self.score,
            "setup_grade": self.grade,
            "setup_dossier_block": self.block,
            "setup_context_score": self.context_score,
            "setup_displacement_score": self.displacement_score,
            "setup_pd_array_score": self.pd_array_score,
            "setup_target_delivery_score": self.target_delivery_score,
            "setup_target_rank_score": self.target_rank_score,
            "setup_net_r_score": self.net_r_score,
            "setup_gauntlet_score": self.gauntlet_score,
            "setup_microstructure_score": self.microstructure_score,
            "setup_cost_score": self.cost_score,
            "setup_dossier_model": "STRUCTURE_PD_ARRAY_LIQUIDITY_NETR_GAUNTLET_FLOW",
        }


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


def _clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, _f(x, lo)))


def _row_ts_sec(row: Dict[str, Any]) -> float:
    raw = row.get("t", row.get("timestamp", 0.0)) if isinstance(row, dict) else 0.0
    val = _f(raw)
    return val / 1000.0 if val > 1e11 else val


class ICTLiquidityEntryEngine:
    """Unified institutional structural authority; all desks share one candidate ledger."""

    def __init__(self, on_self_recovery=None, instrument=None) -> None:
        self._instrument = instrument
        self._asset_id = str(getattr(instrument, "asset_id", "") or "").upper()
        exchange_obj = getattr(instrument, "primary_exchange", "")
        self._exchange = str(getattr(exchange_obj, "value", exchange_obj) or "").lower()
        self._nifty_trend_sweep_profile = bool(
            _cfg_bool("GROWW_NIFTY_TREND_SWEEP_ENABLED", True)
            and self._asset_id in {"NIFTY", "NIFTY50", "CNXNIFTY"}
            and (not self._exchange or self._exchange == "groww")
        )
        # Every instrument uses the same auction-control / structural-zone
        # authority.  BTC and SILVER are named explicitly in telemetry because
        # their 24x7 and thin-book behaviour made the former one-pattern path
        # especially misleading in live diagnosis.
        self._zone_graph_profile = bool(_cfg_bool("ICT_STRUCTURAL_ZONE_GRAPH_ENABLED", True))
        self._desk_profile = (
            "NIFTY_TREND_SWEEP_ZONE_GRAPH" if self._nifty_trend_sweep_profile else
            "BTC_AUCTION_CONTROL_ZONE_GRAPH" if self._asset_id == "BTC" else
            "SILVER_AUCTION_CONTROL_ZONE_GRAPH" if self._asset_id == "SILVER" else
            "AUCTION_CONTROL_ZONE_GRAPH"
        )
        self._state = EngineState.SCANNING
        self._state_entered = time.time()
        self._on_self_recovery = on_self_recovery
        self._signal: Optional[EntrySignal] = None
        self._active_signal: Optional[EntrySignal] = None
        self._thesis: Optional[_Thesis] = None
        self._processed: Dict[tuple, float] = {}
        self._last_analysis: Dict[str, Any] = {}
        self._last_pool_plan: Optional[Dict[str, Any]] = None
        self._last_entry_zone_plan: Optional[Dict[str, Any]] = None
        self._last_scan_skip: Dict[str, int] = {}
        self._atr_pctile: float = 0.5
        self._min_structural_rr: float = 1.0
        self._max_structural_rr_reference: float = 0.0
        self._execution_cost_points: float = 0.0
        self._execution_cost_bps: float = 0.0
        self._last_microstructure: MicrostructureState = MicrostructureState.empty()
        self._delivery_evidence: Optional[DeliveryEvidence] = None
        self._market_state: Optional[AuctionNarrative] = None
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

    def _max_thesis_age_sec(self, thesis: Optional[_Thesis] = None) -> float:
        base = _cfg_float("ICT_THESIS_MAX_AGE_SEC", 900.0)
        if thesis is not None and thesis.entry_type == EntryType.DISPLACEMENT_CONTINUATION:
            base = _cfg_float("ICT_CONTINUATION_THESIS_MAX_AGE_SEC", base)
        return max(120.0, base)

    @staticmethod
    def _direction_int(side: str) -> int:
        side = str(side or "").lower()
        return 1 if side == "long" else (-1 if side == "short" else 0)

    def _nifty_intraday_regime(self, ctx4: _TrendContext, ctx1h: _TrendContext,
                               ctx15: _TrendContext) -> _IntradayRegime:
        """Execution-time market phase for NIFTY long-premium trading.

        NIFTY options are an intraday vehicle: 15m delivery is the tempo, 1h
        is the intraday auction backdrop, and 4h supplies only background
        location.  This prevents a distant 4h opinion from forcing a slow
        counter-tempo reversal in a choppy session.
        """
        if not self._nifty_trend_sweep_profile:
            return _IntradayRegime("GENERIC", "", 0.0, 0.0, "STANDARD", "generic multi-asset profile")
        if ctx1h.side:
            score = 0.56 * ctx15.signed_score + 0.29 * ctx1h.signed_score + 0.15 * ctx4.signed_score
        else:
            score = 0.70 * ctx15.signed_score + 0.30 * ctx4.signed_score
        strength = abs(score)
        threshold = _cfg_float("GROWW_NIFTY_TREND_SWEEP_MIN_PHASE_SCORE", 0.30)
        aggressive_threshold = _cfg_float("GROWW_NIFTY_TREND_SWEEP_AGGRESSIVE_PHASE_SCORE", 0.58)
        direction = "long" if score >= threshold else ("short" if score <= -threshold else "")
        if not direction or ctx15.side == 0:
            return _IntradayRegime("BALANCE_OR_TRANSITION", "", score, strength, "WAIT", "15m delivery not directional")
        direction_int = self._direction_int(direction)
        if ctx15.side != direction_int:
            return _IntradayRegime("TRANSITION", "", score, strength, "WAIT", "15m tempo conflicts with composite phase")
        # A forceful opposing 4h trend means the intraday move is corrective;
        # keep the phase visible but do not chase an aggressive option scalp.
        if ctx4.side == -direction_int and abs(ctx4.signed_score) >= 0.70 and strength < aggressive_threshold:
            return _IntradayRegime("HTF_COUNTERTREND_CORRECTION", "", score, strength, "WAIT", "opposing 4h delivery dominates")
        aggressive = strength >= aggressive_threshold and (ctx1h.side in (0, direction_int))
        return _IntradayRegime(
            "DIRECTIONAL_EXPANSION" if aggressive else "ORDERFLOW_TREND", direction, score, strength,
            "AGGRESSIVE" if aggressive else "FAST_SCALP",
            "15m-led intraday delivery aligned with composite auction phase",
        )

    def _select_nifty_fast_target(self, side: str, entry: float, sl: float,
                                  snap: LiquidityMapSnapshot, atr: float
                                  ) -> Optional[Tuple[PoolTarget, float, float, float, float]]:
        """Select the nearest real intraday liquidity cash-out for NIFTY.

        Unlike the swing/crypto model, the options scalp does not hold for a
        remote 4h/1d pool while theta and chop accumulate.  Target remains an
        observable pool, but 5m pools are eligible and nearest executable
        delivery outranks distant theoretical payoff.
        """
        risk = abs(entry - sl)
        if risk <= _EPS:
            return None
        pools = list(snap.bsl_pools if side == "long" else snap.ssl_pools)
        min_rr = max(1.0, _cfg_float("GROWW_NIFTY_TREND_SWEEP_MIN_RR", 1.15))
        max_rr = max(min_rr, _cfg_float("GROWW_NIFTY_TREND_SWEEP_MAX_RR", 2.40))
        max_dist_atr = max(0.75, _cfg_float("GROWW_NIFTY_TREND_SWEEP_MAX_TARGET_ATR", 2.75))
        rows: List[Dict[str, Any]] = []
        for target in pools:
            pool = getattr(target, "pool", None)
            px = _f(getattr(pool, "price", 0.0))
            tf = str(getattr(pool, "timeframe", "") or "")
            rank = TF_HIERARCHY.get(tf, 1)
            if rank < TF_HIERARCHY.get("5m", 2):
                continue
            if px <= 0 or (side == "long" and px <= entry) or (side == "short" and px >= entry):
                continue
            distance = abs(px - entry)
            if distance / max(atr, _EPS) > max_dist_atr:
                continue
            sig = max(0.01, _f(getattr(target, "significance", 0.0), 0.01))
            buffer = min(0.10 * atr, max(0.02 * atr, 0.025 * atr * math.log1p(sig)))
            tp = px - buffer if side == "long" else px + buffer
            rr = abs(tp - entry) / risk
            if not ((side == "long" and tp > entry) or (side == "short" and tp < entry)):
                continue
            if rr < min_rr:
                continue
            # max_rr is a ranking reference, not a hard veto. A nearby real
            # liquidity pool must not be rejected solely because compact sweep
            # invalidation creates unusually high payoff geometry.
            delivery = _clamp(0.55 + 0.25 * min(1.0, sig / 6.0) + 0.20 * (1.0 - min(1.0, distance / max(atr * max_dist_atr, _EPS))))
            cluster = self._target_cluster_metrics(target, pools, atr)
            target_quality = _clamp(0.62 * delivery + 0.38 * float(cluster["cluster_score"]))
            noise_penalty = 0.62 if bool(cluster["noise"]) else 1.0
            rank_score = target_quality * min(rr, max_rr) * noise_penalty / (1.0 + 0.08 * distance / max(atr, _EPS))
            rows.append({"target": target, "tp": tp, "rr": rr, "distance": distance, "distance_atr": distance / max(atr, _EPS), "delivery": target_quality, "rank_score": rank_score, "buffer": buffer,
                         "cluster_mass": float(cluster["cluster_mass"]), "cluster_score": float(cluster["cluster_score"]),
                         "cluster_count": int(cluster["cluster_count"]), "classification": "NOISE" if cluster["noise"] else "VALID_TARGET_LIQUIDITY",
                         "noise_reason": str(cluster["noise_reason"])})
        if not rows:
            self._last_analysis.update({
                "target_block": "NO_NEARBY_NIFTY_INTRADAY_LIQUIDITY_CASHOUT",
                "target_selection_model": "NIFTY_NEAREST_REAL_LIQUIDITY_CASHOUT",
                "target_min_rr": min_rr, "target_max_rr_reference": max_rr, "target_max_atr": max_dist_atr,
            })
            return None
        best = max(rows, key=lambda row: (row["rank_score"], row["cluster_mass"], -row["distance"]))
        target = best["target"]
        self._last_pool_plan = {
            "ts": time.time(), "role": "TP", "side": side,
            "summary": f"NIFTY_INTRADAY_LIQUIDITY_CONCENTRATION {target.pool.timeframe}@{target.pool.price:.4f} mass={best['cluster_mass']:.2f} grossRR={best['rr']:.2f}",
            "selected": {"pool_price": target.pool.price, "timeframe": target.pool.timeframe, "tp_price": best["tp"], "gross_rr": best["rr"], "structural_liquidity_mass": best["cluster_mass"], "selected": True},
            "candidates": [{"pool_price": r["target"].pool.price, "timeframe": r["target"].pool.timeframe, "tp_price": r["tp"], "gross_rr": r["rr"], "distance_atr": r["distance_atr"], "structural_liquidity_mass": r["cluster_mass"], "liquidity_cluster_score": r["cluster_score"], "classification": r["classification"], "noise_reason": r["noise_reason"], "selected": r is best} for r in rows],
        }
        self._last_analysis.update({
            "target_timeframe": str(target.pool.timeframe), "target_pool_price": _f(target.pool.price),
            "target_tp_buffer": best["buffer"], "target_distance_atr": best["distance_atr"],
            "target_significance": _f(getattr(target, "significance", 0.0)), "target_rr": best["rr"],
            "target_gross_rr": best["rr"], "target_net_win_r": best["rr"],
            "target_selection_model": "NIFTY_NEAREST_REAL_LIQUIDITY_CASHOUT",
            "target_selection_authority": "NIFTY_INTRADAY_LIQUIDITY_CONCENTRATION_GRAPH",
            "target_structural_liquidity_mass": best["cluster_mass"],
            "target_liquidity_cluster_score": best["cluster_score"],
            "target_liquidity_cluster_count": best["cluster_count"],
            "delivery_score": best["delivery"], "target_rank_score": best["rank_score"],
            "probability_calibrated": False,
        })
        return target, float(best["tp"]), float(best["rr"]), float(best["rank_score"]), float(best["delivery"])

    def _sweep_reclaim_protected_stop(self, side: str, entry: float, wick: float, raw_stop: float,
                                      snap: LiquidityMapSnapshot, atr: float) -> _StopPlan:
        """Fast-profile stop that still refuses to sit on live liquidity."""
        pools = list(snap.ssl_pools if side == "long" else snap.bsl_pools)
        clarity = _f(getattr(self._market_state, "clarity", 0.0), 0.0)
        envelope_atr = 0.32 + 0.55 * self._atr_pctile + 0.22 * clarity
        selected_rows: List[Dict[str, Any]] = []
        noise_rows: List[Dict[str, Any]] = []
        mass, outer = 0.0, wick
        for target in pools:
            pool = getattr(target, "pool", None)
            px = _f(getattr(pool, "price", 0.0), 0.0)
            sig = max(0.0, _f(getattr(target, "significance", 0.0), 0.0))
            on_stop_side = (side == "long" and px < entry) or (side == "short" and px > entry)
            if not on_stop_side or px <= 0:
                continue
            distance_atr = abs(px - wick) / max(atr, _EPS)
            relevance = math.tanh(sig / 4.0) * math.exp(-distance_atr / max(envelope_atr, _EPS))
            row = {"pool_price": px, "timeframe": str(getattr(pool, "timeframe", "") or ""),
                   "significance": sig, "distance_from_anchor_atr": distance_atr, "relevance": relevance}
            if distance_atr <= envelope_atr and relevance >= 0.10:
                row["classification"] = "PROTECTED_BEYOND_STOP"
                selected_rows.append(row)
                mass += sig * max(0.05, relevance)
                outer = min(outer, px) if side == "long" else max(outer, px)
            else:
                row["classification"] = "NOISE_OUTSIDE_INVALIDATION_CLUSTER"
                noise_rows.append(row)
        base_clearance = abs(raw_stop - wick)
        clearance = base_clearance + atr * min(0.16, 0.04 * math.log1p(max(0.0, mass)))
        stop = min(raw_stop, outer - clearance) if side == "long" else max(raw_stop, outer + clearance)
        return _StopPlan(price=stop, structural_anchor=wick, outer_protected_liquidity=outer,
                         clearance=clearance, protected_cluster_mass=mass,
                         selected_pools=tuple(selected_rows), noise_pools=tuple(noise_rows),
                         model="NIFTY_SWEEP_RECLAIM_LIQUIDITY_PROTECTED_STOP")

    def _try_nifty_trend_sweep_signal(self, sweep: SweepResult, regime: _IntradayRegime,
                                      snap: LiquidityMapSnapshot, price: float, atr: float, now: float) -> bool:
        side = str(getattr(sweep, "direction", "") or "").lower()
        if not regime.side or side != regime.side:
            return False
        # NIFTY is faster, not blind. A local pullback sweep may trigger entry
        # inside the intraday trend, but cannot override a live higher-timeframe
        # liquidity-transfer owner without an actual control transfer.
        if (self._market_state is not None and self._market_state.has_firm_parent_control
                and not self._market_state.owns(side)):
            self._record_block(
                "NIFTY_SWEEP_OPPOSES_AUCTION_CONTROL", trigger="WAIT_FOR_CONTROL_TRANSFER",
                controlling_side=self._market_state.control_side,
                controlling_parent_tf=self._market_state.parent_timeframe,
                controlling_parent_quality=self._market_state.parent_quality,
            )
            return False
        pool = getattr(sweep, "pool", None)
        swept_level = _f(getattr(pool, "price", 0.0))
        wick = _f(getattr(sweep, "wick_extreme", 0.0))
        if swept_level <= 0 or wick <= 0:
            return False
        reclaim = (side == "long" and price > swept_level) or (side == "short" and price < swept_level)
        extension_atr = abs(price - swept_level) / max(atr, _EPS)
        max_extension = max(0.20, _cfg_float("GROWW_NIFTY_TREND_SWEEP_MAX_RECLAIM_EXTENSION_ATR", 0.65))
        if not reclaim:
            self._record_block("NIFTY_TREND_SWEEP_NOT_RECLAIMED", trigger="WAIT_FOR_SWEEP_RECLAIM")
            return False
        if extension_atr > max_extension:
            self._record_block("NIFTY_TREND_SWEEP_RECLAIM_TOO_EXTENDED", trigger="WAIT_FOR_NEW_PULLBACK_SWEEP", reclaim_extension_atr=extension_atr)
            return False
        clearance = atr * (_cfg_float("GROWW_NIFTY_TREND_SWEEP_STOP_BASE_ATR", 0.08) + _cfg_float("GROWW_NIFTY_TREND_SWEEP_STOP_PCTL_SLOPE_ATR", 0.10) * self._atr_pctile)
        raw_sl = wick - clearance if side == "long" else wick + clearance
        stop_plan = self._sweep_reclaim_protected_stop(side, price, wick, raw_sl, snap, atr)
        sl = stop_plan.price
        self._last_analysis.update(stop_plan.payload())
        if not ((side == "long" and sl < price) or (side == "short" and sl > price)):
            return False
        selected = self._select_nifty_fast_target(side, price, sl, snap, atr)
        if selected is None:
            self._record_block("AWAITING_NEARBY_NIFTY_INTRADAY_LIQUIDITY_TARGET", trigger="FAST_TP_ZONE_REQUIRED")
            return False
        target_obj, tp, rr, rank_score, delivery_score = selected
        narrative = self._market_state
        quality = {
            "context_delivery_score": regime.strength, "delivery_score": delivery_score,
            "target_rank_score": rank_score, "raid_quality": _f(getattr(sweep, "quality", 0.0)),
            "nifty_intraday_phase_score": regime.signed_score, "fast_exit_profile": 1.0,
            "probability_calibrated": False, "archetype": EntryType.NIFTY_TREND_SWEEP_SCALP.value,
            "market_phase": narrative.phase if narrative else regime.label,
            "auction_control_side": narrative.control_side if narrative else regime.side,
            "auction_control_score": narrative.control_score if narrative else regime.signed_score,
            "execution_posture": narrative.posture if narrative else regime.aggression,
            "auction_risk_scalar": narrative.risk_scalar if narrative else (1.0 if regime.aggression == "AGGRESSIVE" else 0.72),
        }
        explanation = (
            f"NIFTY phase={regime.label}/{regime.aggression} score={regime.signed_score:+.2f} | "
            f"fresh {str(getattr(pool, 'timeframe', 'LTF')).upper()} {side.upper()} pullback liquidity sweep reclaimed @ {swept_level:.2f} | "
            f"entry at reclaim; nearest live {target_obj.pool.timeframe} cash-out @ {target_obj.pool.price:.2f}; grossRR={rr:.2f}"
        )
        self._signal = EntrySignal(
            side=side, entry_type=EntryType.NIFTY_TREND_SWEEP_SCALP, entry_price=price, sl_price=sl, tp_price=tp,
            rr_ratio=rr, target_pool=target_obj, sweep_result=sweep, delivery_probability=0.0,
            delivery_score=delivery_score, probability_calibrated=False, archetype=EntryType.NIFTY_TREND_SWEEP_SCALP.value,
            reason=explanation, structural_validation="NIFTY_TREND_SWEEP_SCALP: live intraday trend phase / fresh same-direction pullback sweep reclaim / nearest real liquidity cash-out",
            quality=quality, analysis_domain="UNDERLYING",
        )
        self._state = EngineState.EXECUTABLE
        self._state_entered = now
        self._last_analysis.update({
            "state": self._state.value, "side": side, "archetype": EntryType.NIFTY_TREND_SWEEP_SCALP.value,
            "candidate_archetype": EntryType.NIFTY_TREND_SWEEP_SCALP.value, "trigger": "EXECUTABLE_NIFTY_TREND_SWEEP_RECLAIM",
            "block_reason": "NONE", "raid_side": side, "raid_price": swept_level, "raid_wick": wick,
            "raid_quality": _f(getattr(sweep, "quality", 0.0)), "raid_age_sec": max(0.0, now - _f(getattr(sweep, "detected_at", now))),
            "entry": price, "sl": sl, "tp": tp, "rr": rr, "gross_rr": rr,
            "context_bias_path": "NIFTY_INTRADAY_TREND_SWEEP", "context_direction": side,
            "context_delivery_score": regime.strength, "context_permission": True, "context_aligned": True,
            "entry_sweep_timeframe": str(getattr(pool, "timeframe", "") or ""),
            "delivery_score": delivery_score, "target_rank_score": rank_score, "probability_calibrated": False,
            "entry_zone_selection_model": "NIFTY_TREND_SWEEP_RECLAIM_ZONE",
            "entry_zone_selected_tf": str(getattr(pool, "timeframe", "") or ""),
            "entry_zone_low": min(wick, swept_level), "entry_zone_high": max(wick, swept_level),
            "entry_zone_equilibrium": swept_level, "entry_zone_score": _f(getattr(sweep, "quality", 0.0)),
            "entry_zone_order_block_overlap": 0.0, "entry_zone_raid_fuel_score": _f(getattr(sweep, "quality", 0.0)),
            "entry_zone_candidate_count": 1, "entry_zone_noise_count": 0,
            "stop_clearance": abs(sl - wick), "stop_clearance_atr": abs(sl - wick) / max(atr, _EPS),
            "market_phase": quality.get("market_phase", "UNCLASSIFIED"),
            "auction_control_side": quality.get("auction_control_side", "none"),
            "auction_control_score": quality.get("auction_control_score", 0.0),
            "execution_posture": quality.get("execution_posture", "FAST_SCALP"),
            "auction_risk_scalar": quality.get("auction_risk_scalar", 0.72),
        })
        logger.info("NIFTY TREND-SWEEP ENTRY READY %s @ %.4f | trigger=%s sweep phase=%s/%s score=%+.3f SL=%.4f TP=%.4f grossRR=%.2f target=%s@%.4f", side.upper(), price, str(getattr(pool, "timeframe", "LTF")).upper(), regime.label, regime.aggression, regime.signed_score, sl, tp, rr, target_obj.pool.timeframe, target_obj.pool.price)
        return True

    def _context_decision_for_raid(self, side: str, ctx4: _TrendContext,
                                   ctx15: _TrendContext, sweep_quality: float,
                                   narrative: Optional[AuctionNarrative] = None) -> _ContextDecision:
        """Classify structural raid context under the active auction owner.

        The generic executable evidence remains 5m raid -> MSS/displacement ->
        FVG repricing. 4H/15m context supplies draw-on-liquidity bias, but a
        strongly dominant opposing 15m auction cannot be faded merely because
        the slower 4h location points the other way.
        """
        direction = self._direction_int(side)
        if direction == 0:
            return _ContextDecision(side, False, "INVALID_RAID_DIRECTION",
                                    "INVALID_RAID_DIRECTION", 0.0, False, 0, 0, 0)
        # Market-state ownership replaces the former filter stack as the core
        # decision. A local 5m reversal cannot override a fresh 1H+ liquidity
        # transfer until the delivery process actually changes owner.
        if (narrative is not None and narrative.has_firm_parent_control
                and narrative.control_side in ("long", "short")
                and side != narrative.control_side):
            score = max(0.0, 0.15 * _f(sweep_quality) + 0.10 * (1.0 - narrative.clarity))
            return _ContextDecision(
                side, False, "COUNTER_AUCTION_WAIT_CONTROL_TRANSFER",
                "AUCTION_CONTROL_REMAINS_OPPOSING_SIDE", score, False, 0, 1, 0,
            )
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
            # Split context is a transition phase, not blanket permission to fade
            # the active intraday auction. A dominant 15m delivery owns timing;
            # 4h location alone must not validate a counter-tempo reversal.
            if ctx15.side == -direction and abs(ctx15.signed_score) >= max(0.42, abs(ctx4.signed_score) + 0.15):
                score = max(0.0, 0.12 + 0.18 * _f(sweep_quality) - 0.22 * ctx15.confidence)
                return _ContextDecision(side, False, "SPLIT_HTF_TACTICAL_DELIVERY_OPPOSES_RAID",
                                        "SPLIT_HTF_TACTICAL_DELIVERY_OPPOSES_RAID", score, False,
                                        supporting, opposing, ranging)
            path = "MITIGATION_RAID_WITH_SPLIT_HTF"
        elif ranging == 2:
            path = "BALANCED_RANGE_EXTERNAL_RAID"
        elif ranging == 1 and opposing == 1:
            path = "COUNTER_DELIVERY_RAID_REQUIRES_5M_PROOF"
            if (_cfg_bool("ICT_LEGACY_FILTER_COMPATIBILITY_ENABLED", False)
                    and _cfg_bool("ICT_SELECTIVITY_MODE", True)
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
        if narrative is not None and narrative.owns(side) and narrative.has_firm_parent_control:
            path = f"{narrative.phase}_WITH_CONTROL"
            score += 0.12 * narrative.clarity
        return _ContextDecision(side, True, path, "NONE",
                                max(0.05, min(0.95, score)), strict,
                                supporting, opposing, ranging)

    def update(self, liq_snapshot: LiquidityMapSnapshot, price: float, atr: float, now: float,
               candles_5m: Optional[List[Dict]] = None,
               candles_15m: Optional[List[Dict]] = None,
               candles_4h: Optional[List[Dict]] = None,
               candles_1h: Optional[List[Dict]] = None,
               candles_1d: Optional[List[Dict]] = None,
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
        c1d = _closed(candles_1d, 10, "1d", now) if candles_1d else []
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
        atr1d = _timeframe_atr(c1d) if c1d else 0.0
        ctx1d = _robust_trend(c1d, atr1d, min(30, len(c1d))) if atr1d > _EPS else _TrendContext(0, 0.0, 0.0, 0.0, 0.0)
        fresh = self._fresh_5m_sweeps(liq_snapshot, now)
        parent_htf = self._fresh_parent_htf_sweeps(liq_snapshot, now)
        self._market_state = build_auction_narrative(
            {"1d": ctx1d, "4h": ctx4, "1h": ctx1h, "15m": ctx15},
            parent_htf, fresh, now,
            aggressive_min_clarity=_cfg_float("MARKET_STATE_AGGRESSIVE_MIN_CLARITY", 0.62),
            parent_min_quality=_cfg_float("MARKET_STATE_FIRM_PARENT_MIN_QUALITY", 0.60),
        )
        self._delivery_evidence = build_delivery_evidence(
            liq_snapshot, price, atr,
            ((ctx1d.signed_score, 0.15), (ctx4.signed_score, 0.35), (ctx1h.signed_score, 0.20), (ctx15.signed_score, 0.30)),
            self._last_microstructure,
        )
        nifty_regime = self._nifty_intraday_regime(ctx4, ctx1h, ctx15)
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
            "context_1d": ctx1d.label, "context_1d_score": ctx1d.signed_score, "context_1d_atr": atr1d,
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
            "nifty_intraday_phase": nifty_regime.label,
            "nifty_intraday_direction": nifty_regime.side or "none",
            "nifty_intraday_score": nifty_regime.signed_score,
            "nifty_intraday_strength": nifty_regime.strength,
            "nifty_intraday_aggression": nifty_regime.aggression,
            "nifty_intraday_reason": nifty_regime.reason,
            "execution_profile": self._desk_profile,
            "structural_zone_graph_enabled": self._zone_graph_profile,
        }
        self._last_analysis.update(self._market_state.payload())
        if self._state in (EngineState.ENTERING, EngineState.IN_POSITION):
            self._record_block("POSITION_LIFECYCLE_ACTIVE", trigger=self._state.value)
            return
        if self._signal is not None:
            self._last_analysis.update({"state": EngineState.EXECUTABLE.value, "trigger": "SIGNAL_PENDING_EXECUTION", "block_reason": "NONE"})
            return

        if (self._thesis is not None and self._market_state is not None
                and self._market_state.has_firm_parent_control
                and not self._market_state.owns(self._thesis.side)):
            invalidated_side = self._thesis.side
            self._thesis = None
            self._state = EngineState.CONTEXT_READY
            self._record_block(
                "AUCTION_CONTROL_TRANSFERRED_AGAINST_THESIS",
                trigger="MARKET_STATE_OWNER",
                invalidated_side=invalidated_side,
                controlling_side=self._market_state.control_side,
                controlling_parent_tf=self._market_state.parent_timeframe,
                controlling_parent_quality=self._market_state.parent_quality,
            )
        if self._thesis is not None:
            if now - self._thesis.formed_at > self._max_thesis_age_sec(self._thesis):
                self._last_analysis.update({"expired_thesis_age_sec": now - self._thesis.formed_at})
                self._thesis = None
                self._state = EngineState.SCANNING
            else:
                self._try_reprice_thesis(self._thesis, liq_snapshot, price, atr, now, c5, c15, c4h)
                if self._signal is not None:
                    return
                # A waiting reprice is a live candidate, not a global lock.  Continue
                # scanning so a stronger/newer structural opportunity can replace it.
                self._last_analysis["candidate_ledger_active"] = True

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

        # NIFTY is traded as a fast long-premium intraday profile: entry is the
        # reclaimed liquidity sweep in the live trend direction. It does not
        # inherit the slower swing-reversal MSS/FVG waiting chain used for 24x7
        # token venues.
        if self._nifty_trend_sweep_profile:
            nifty_fresh = self._fresh_nifty_entry_sweeps(liq_snapshot, now)
            self._last_analysis.update({
                "fresh_nifty_trigger_sweep_count": len(nifty_fresh),
                "nifty_trigger_timeframes": "1m,5m",
                "fresh_5m_raid_count": sum(1 for sw in nifty_fresh if str(getattr(getattr(sw, "pool", None), "timeframe", "")).lower() == "5m"),
            })
            if not nifty_regime.side:
                self._record_block("NIFTY_MARKET_PHASE_NOT_DIRECTIONAL", trigger="WAIT_FOR_INTRADAY_TREND_PHASE",
                                   context_bias_path=nifty_regime.label, context_direction="none",
                                   context_delivery_score=nifty_regime.strength)
                return
            if not nifty_fresh:
                self._record_block("AWAITING_NIFTY_TREND_DIRECTION_LIQUIDITY_SWEEP", trigger="WAIT_FOR_1M_OR_5M_TREND_PULLBACK_SWEEP",
                                   context_bias_path="NIFTY_INTRADAY_TREND_SWEEP", context_direction=nifty_regime.side,
                                   context_delivery_score=nifty_regime.strength, context_permission=True)
                return
            same_direction = [sw for sw in nifty_fresh if str(getattr(sw, "direction", "") or "").lower() == nifty_regime.side]
            for sweep in same_direction:
                if self._try_nifty_trend_sweep_signal(sweep, nifty_regime, liq_snapshot, price, atr, now):
                    return
            if not same_direction:
                self._record_block("NIFTY_SWEEP_OPPOSES_INTRADAY_TREND", trigger="WAIT_FOR_SAME_DIRECTION_1M_OR_5M_SWEEP",
                                   context_bias_path="NIFTY_INTRADAY_TREND_SWEEP", context_direction=nifty_regime.side,
                                   context_delivery_score=nifty_regime.strength, context_permission=True)
            return

        if not fresh:
            # A delivery displacement without a contemporary stop raid is an
            # independent continuation archetype. When a live raid exists below,
            # the raid-specific models own attribution and invalidation geometry.
            continuation = self._build_displacement_continuation_thesis(ctx4, ctx15, c5, atr, now)
            if self._adopt_candidate(continuation) and self._thesis is not None:
                self._try_reprice_thesis(self._thesis, liq_snapshot, price, atr, now, c5, c15, c4h)
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
                self._try_reprice_thesis(self._thesis, liq_snapshot, price, atr, now, c5, c15, c4h)
                if self._signal is not None:
                    return
            side = str(getattr(sweep, "direction", "") or "").lower()
            direction = self._direction_int(side)
            pool = getattr(sweep, "pool", None)
            context_decision = self._context_decision_for_raid(
                side, ctx4, ctx15, _f(getattr(sweep, "quality", 0.0)), self._market_state)
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
                self._try_reprice_thesis(self._thesis, liq_snapshot, price, atr, now, c5, c15, c4h)
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

    @property
    def entry_zone_plan_info(self) -> Optional[Dict[str, Any]]:
        return dict(self._last_entry_zone_plan) if self._last_entry_zone_plan else None

    # ---------- model internals ----------
    def _expire(self, now: float) -> None:
        self._processed = {k: expiry for k, expiry in self._processed.items() if expiry > now}

    def _fresh_nifty_entry_sweeps(self, snap: LiquidityMapSnapshot, now: float) -> List[SweepResult]:
        """Return only immediate 1m/5m NIFTY sweep-reclaim triggers.

        The NIFTY option vehicle cannot wait through the generic 5m
        raid/MSS/FVG cycle. A one-minute stop raid can be the executable entry
        event once it is reclaimed in an already observed intraday trend.
        """
        age_limit = {
            "1m": max(30.0, _cfg_float("GROWW_NIFTY_TREND_SWEEP_1M_MAX_AGE_SEC", 90.0)),
            "5m": max(60.0, _cfg_float("GROWW_NIFTY_TREND_SWEEP_5M_MAX_AGE_SEC", 360.0)),
        }
        out: List[SweepResult] = []
        for sw in list(getattr(snap, "recent_sweeps", []) or []):
            tf = str(getattr(getattr(sw, "pool", None), "timeframe", "") or "").lower()
            if tf not in age_limit:
                continue
            age = max(0.0, now - _f(getattr(sw, "detected_at", 0.0)))
            if age > age_limit[tf] or _sweep_key(sw) in self._processed:
                continue
            out.append(sw)
        return sorted(out, key=lambda sw: (_f(getattr(sw, "detected_at", 0.0)), _f(getattr(sw, "quality", 0.0))), reverse=True)

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
            market_phase=(self._market_state.phase if self._market_state else "UNCLASSIFIED"),
            auction_control_side=(self._market_state.control_side if self._market_state else "none"),
            auction_control_score=(self._market_state.control_score if self._market_state else 0.0),
            execution_posture=(self._market_state.posture if self._market_state else "OBSERVE"),
            auction_risk_scalar=(self._market_state.risk_scalar if self._market_state else 0.35),
            market_state_thesis=(self._market_state.thesis if self._market_state else ""),
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

        micro = getattr(self, "_last_microstructure", None)
        if _cfg_bool("ICT_MICROSTRUCTURE_GUARD_ENABLED", True) and micro is not None and getattr(micro, "fresh", False):
            micro_score = float(micro.aligned_score(thesis.side))
            min_micro = _cfg_float("ICT_MIN_FRESH_MICROSTRUCTURE_SCORE", -0.20)
            details.update({
                "selectivity_microstructure_score": micro_score,
                "selectivity_required_microstructure_score": min_micro,
                "selectivity_micro_spread_atr": _f(getattr(micro, "spread_atr", 0.0)),
                "selectivity_micro_book_age_sec": _f(getattr(micro, "book_age_sec", 0.0)),
            })
            if micro_score < min_micro:
                return "ADVERSE_EXECUTION_FLOW_BLOCKED_BY_SELECTIVITY", details

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

    def _setup_quality_dossier(self, thesis: _Thesis, rr: float, rank_score: float,
                               delivery_score: float,
                               pd_confluence: Optional[_PDArrayConfluence]) -> _SetupQualityDossier:
        """Single trade-quality authority after structural geometry is known.

        Earlier gates prove each prerequisite independently.  This dossier asks
        the desk question that matters before a ticket is allowed to exist:
        does the whole setup, net of target quality and execution conditions,
        deserve risk right now?  It remains an evidence score, not a claimed win
        probability.
        """
        archetype = thesis.entry_type
        if archetype == EntryType.DISPLACEMENT_CONTINUATION:
            disp_floor = _cfg_float("ICT_ENTRY_MIN_DISPLACEMENT_ATR_CONTINUATION", 1.50)
        else:
            disp_floor = _cfg_float("ICT_ENTRY_MIN_DISPLACEMENT_ATR_RAID", 1.35)
        context_score = _clamp(thesis.context_delivery_score)
        displacement_score = _clamp(thesis.displacement_atr / max(disp_floor * 1.75, _EPS))
        pd_score = _clamp(pd_confluence.score if pd_confluence is not None else 0.70)
        target_score = _clamp(delivery_score)

        rank_norm = max(0.50, _cfg_float("ICT_SETUP_DOSSIER_RANK_NORM", 3.0))
        target_rank_norm = _clamp(math.tanh(max(0.0, rank_score) / rank_norm))
        net_r = _f(self._last_analysis.get("target_net_win_r", rr), rr)
        net_r_score = _clamp(math.tanh(max(0.0, net_r) / max(self._min_structural_rr, 1.0)))
        gauntlet_score = _clamp(_f(self._last_analysis.get("target_gauntlet_penalty", 1.0), 1.0))
        cost_r = _f(self._last_analysis.get("target_cost_r", 0.0), 0.0)
        cost_score = _clamp(1.0 - cost_r / max(0.35, _cfg_float("FEE_TO_RISK_NO_ALLOC", 1.25)))

        micro = getattr(self, "_last_microstructure", None)
        if micro is not None and getattr(micro, "fresh", False):
            micro_raw = _clamp(float(micro.aligned_score(thesis.side)), -1.0, 1.0)
            micro_score = _clamp((micro_raw + 1.0) / 2.0)
        else:
            micro_score = 0.55

        liquidity_score = _clamp(0.48 * target_score + 0.28 * target_rank_norm + 0.16 * net_r_score + 0.08 * gauntlet_score)
        execution_score = _clamp(0.60 * micro_score + 0.40 * cost_score)
        score = _clamp(
            0.18 * context_score
            + 0.18 * displacement_score
            + 0.22 * pd_score
            + 0.27 * liquidity_score
            + 0.15 * execution_score
        )
        if score >= 0.82:
            grade = "S"
        elif score >= 0.72:
            grade = "A"
        elif score >= 0.60:
            grade = "B"
        elif score >= 0.50:
            grade = "C"
        else:
            grade = "D"
        min_score = _cfg_float("ICT_SETUP_DOSSIER_MIN_SCORE", 0.60)
        block = "NONE" if score >= min_score else "SETUP_DOSSIER_BELOW_FLOOR"
        return _SetupQualityDossier(
            score=score, grade=grade, block=block,
            context_score=context_score, displacement_score=displacement_score,
            pd_array_score=pd_score, target_delivery_score=target_score,
            target_rank_score=target_rank_norm, net_r_score=net_r_score,
            gauntlet_score=gauntlet_score, microstructure_score=micro_score,
            cost_score=cost_score,
        )

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
        if (self._market_state is not None and self._market_state.has_firm_parent_control
                and not self._market_state.owns(side)):
            self._record_block(
                "CONTINUATION_NOT_OWNED_BY_AUCTION_CONTROL",
                trigger="MARKET_STATE_OWNER",
                candidate_side=side,
                controlling_side=self._market_state.control_side,
            )
            return None
        if _cfg_bool("ICT_SELECTIVITY_MODE", True) and _cfg_bool("ICT_LEGACY_FILTER_COMPATIBILITY_ENABLED", False):
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
            market_phase=(self._market_state.phase if self._market_state else "UNCLASSIFIED"),
            auction_control_side=(self._market_state.control_side if self._market_state else "none"),
            auction_control_score=(self._market_state.control_score if self._market_state else 0.0),
            execution_posture=(self._market_state.posture if self._market_state else "OBSERVE"),
            auction_risk_scalar=(self._market_state.risk_scalar if self._market_state else 0.35),
            market_state_thesis=(self._market_state.thesis if self._market_state else ""),
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
        if (self._market_state is not None and self._market_state.has_firm_parent_control
                and not self._market_state.owns(side)):
            self._record_block(
                "EXPANSION_NOT_OWNED_BY_AUCTION_CONTROL",
                trigger="MARKET_STATE_OWNER",
                candidate_side=side,
                controlling_side=self._market_state.control_side,
            )
            return None
        if _cfg_bool("ICT_SELECTIVITY_MODE", True) and _cfg_bool("ICT_LEGACY_FILTER_COMPATIBILITY_ENABLED", False):
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
            market_phase=(self._market_state.phase if self._market_state else "UNCLASSIFIED"),
            auction_control_side=(self._market_state.control_side if self._market_state else "none"),
            auction_control_score=(self._market_state.control_score if self._market_state else 0.0),
            execution_posture=(self._market_state.posture if self._market_state else "OBSERVE"),
            auction_risk_scalar=(self._market_state.risk_scalar if self._market_state else 0.35),
            market_state_thesis=(self._market_state.thesis if self._market_state else ""),
        )

    def _dealing_range(self, candles_15m: List[Dict], candles_4h: List[Dict]) -> Tuple[float, float, float]:
        lookback = max(12, int(_cfg_float("ICT_DEALING_RANGE_LOOKBACK_15M", 48)))
        rows = list(candles_15m[-lookback:] or [])
        if len(rows) < 12:
            rows = list(candles_4h[-20:] or [])
        highs = [_f(row.get("h")) for row in rows if _f(row.get("h")) > 0]
        lows = [_f(row.get("l")) for row in rows if _f(row.get("l")) > 0]
        if not highs or not lows:
            return 0.0, 0.0, 0.0
        low, high = min(lows), max(highs)
        return low, high, (low + high) / 2.0 if high > low else 0.0

    def _killzone_quality(self, candles_5m: List[Dict], now: float) -> Tuple[float, str]:
        if not _cfg_bool("ICT_PD_KILLZONE_WEIGHT_ENABLED", True):
            return 0.55, "DISABLED"
        ts = _row_ts_sec(candles_5m[-1]) if candles_5m else 0.0
        ts = ts if ts > 0 else now
        try:
            if ZoneInfo is not None:
                dt = datetime.fromtimestamp(ts, timezone.utc).astimezone(ZoneInfo("America/New_York"))
            else:  # pragma: no cover
                dt = datetime.fromtimestamp(ts, timezone.utc) - timedelta(hours=5)
        except Exception:
            return 0.55, "UNKNOWN"
        minutes = dt.hour * 60 + dt.minute
        windows = (
            ("LONDON_KILLZONE", 2 * 60, 5 * 60),
            ("NY_AM_KILLZONE", 8 * 60 + 30, 11 * 60),
            ("NY_LUNCH_MACRO", 12 * 60, 13 * 60 + 30),
            ("NY_PM_KILLZONE", 13 * 60 + 30, 16 * 60),
        )
        for label, start, end in windows:
            if start <= minutes <= end:
                return 1.0, label
        near = min(min(abs(minutes - start), abs(minutes - end)) for _, start, end in windows)
        if near <= 30:
            return 0.72, "KILLZONE_ADJACENT"
        return 0.45, "OUTSIDE_KILLZONE"

    @staticmethod
    def _zone_overlap_ratio(low_a: float, high_a: float, low_b: float, high_b: float) -> float:
        if high_a <= low_a or high_b <= low_b:
            return 0.0
        overlap = max(0.0, min(high_a, high_b) - max(low_a, low_b))
        return _clamp(overlap / max(min(high_a - low_a, high_b - low_b), _EPS))

    def _order_block_for_fvg(self, side: str, fvg: _FVG, candles: List[Dict]) -> Tuple[float, float]:
        """Find the last opposing candle that funded a displacement imbalance."""
        if not candles:
            return 0.0, 0.0
        impulse_idx = max(1, min(len(candles) - 1, int(fvg.index) - 1))
        start = max(0, impulse_idx - 8)
        for row in reversed(candles[start:impulse_idx]):
            opened, closed = _f(row.get("o")), _f(row.get("c"))
            if opened <= 0 or closed <= 0:
                continue
            if side == "long" and closed < opened:
                return _f(row.get("l")), _f(row.get("h"))
            if side == "short" and closed > opened:
                return _f(row.get("l")), _f(row.get("h"))
        origin = candles[max(0, impulse_idx - 1)]
        return _f(origin.get("l")), _f(origin.get("h"))

    def _order_block_zone(self, thesis: _Thesis, candles_5m: List[Dict]) -> Tuple[float, float]:
        return self._order_block_for_fvg(thesis.side, thesis.fvg, candles_5m)

    def _candidate_fvgs(self, side: str, candles: List[Dict], atr: float,
                        timeframe: str, recent_bars: int) -> List[_FVG]:
        """Enumerate visible imbalance zones; do not assume the latest is best."""
        if atr <= _EPS or len(candles) < 3:
            return []
        start = max(2, len(candles) - max(3, int(recent_bars)))
        zones: List[_FVG] = []
        for i in range(start, len(candles)):
            before, impulse, after = candles[i - 2], candles[i - 1], candles[i]
            if side == "long":
                low, high = _f(before.get("h")), _f(after.get("l"))
                body = (_f(impulse.get("c")) - _f(impulse.get("o"))) / max(atr, _EPS)
            else:
                low, high = _f(after.get("h")), _f(before.get("l"))
                body = (_f(impulse.get("o")) - _f(impulse.get("c"))) / max(atr, _EPS)
            if high > low and body > 0:
                zones.append(_FVG(side=side, low=low, high=high, index=i, displacement_atr=body))
        return zones

    def _rank_entry_zones(self, thesis: _Thesis, snap: LiquidityMapSnapshot, price: float,
                          atr: float, candles_5m: List[Dict], candles_15m: List[Dict]
                          ) -> Tuple[_FVG, Dict[str, Any]]:
        """Rank FVG/OB repricing zones in the active market-owned direction.

        The output labels weak imbalances as noise rather than manufacturing a
        separate veto stack.  Price may execute only through the selected zone;
        the rest remain observability data or future repricing alternatives.
        """
        if not self._zone_graph_profile:
            return thesis.fvg, {"zone_selection_model": "LEGACY_SINGLE_FVG", "candidates": []}
        all_fvgs: List[Tuple[_FVG, str, List[Dict], int]] = [(thesis.fvg, "5m", candles_5m, 0)]
        # Executable entry zones must belong to the already-confirmed delivery
        # sequence, never an unrelated old gap that predates the raid/MSS.
        min_trigger_index = max(2, int(thesis.fvg.index) - 1)
        all_fvgs.extend((f, "5m", candles_5m, len(candles_5m) - f.index)
                        for f in self._candidate_fvgs(thesis.side, candles_5m, atr, "5m", 52)
                        if int(f.index) >= min_trigger_index)
        # 15m imbalances are higher-timeframe location confluence for a 5m
        # trigger; they do not independently manufacture an execution trigger.
        overlay_fvgs = self._candidate_fvgs(thesis.side, candles_15m, atr, "15m", 28)

        deduped: List[Tuple[_FVG, str, List[Dict], int]] = []
        for fvg, tf, rows, age in all_fvgs:
            duplicate = False
            for prior, prior_tf, _, _ in deduped:
                if tf == prior_tf and abs(prior.low - fvg.low) <= 1e-9 and abs(prior.high - fvg.high) <= 1e-9:
                    duplicate = True
                    break
            if not duplicate:
                deduped.append((fvg, tf, rows, age))

        wick = _f(getattr(getattr(thesis, "sweep", None), "wick_extreme", 0.0), thesis.invalidation_anchor)
        control_aligned = (
            self._market_state is None
            or self._market_state.control_side in ("none", thesis.side)
        )
        candidates: List[_EntryZoneCandidate] = []
        for fvg, tf, rows, age in deduped:
            width_atr = (fvg.high - fvg.low) / max(atr, _EPS)
            ob_low, ob_high = self._order_block_for_fvg(thesis.side, fvg, rows)
            ob_overlap = self._zone_overlap_ratio(fvg.low, fvg.high, ob_low, ob_high)
            htf_overlap = max((self._zone_overlap_ratio(fvg.low, fvg.high, higher.low, higher.high)
                               for higher in overlay_fvgs), default=0.0)
            impulse_score = _clamp(math.tanh(max(0.0, fvg.displacement_atr) / 1.25))
            # A real entry zone should be close enough to the stop raid or the
            # displacement invalidation origin to explain why orders transacted.
            origin = wick if wick > 0 else thesis.invalidation_anchor
            origin_edge = fvg.low if thesis.side == "long" else fvg.high
            raid_fuel = math.exp(-abs(origin_edge - origin) / max(2.25 * atr, _EPS)) if origin > 0 else 0.45
            freshness = math.exp(-max(0, age) / (15.0 if tf == "5m" else 7.0))
            midpoint = fvg.equilibrium
            reprice = math.exp(-abs(price - midpoint) / max(1.15 * atr, _EPS))
            phase_alignment = 1.0 if control_aligned else 0.18
            tf_weight = 0.80 + 0.20 * htf_overlap
            score = _clamp(
                0.24 * impulse_score + 0.18 * ob_overlap + 0.12 * htf_overlap + 0.20 * raid_fuel
                + 0.13 * freshness + 0.09 * reprice + 0.04 * tf_weight
            ) * phase_alignment
            noise_reasons = []
            if width_atr < 0.025:
                noise_reasons.append("MICRO_GAP_WITHOUT_MEANINGFUL_REPRICE_CAPACITY")
            if fvg.displacement_atr < 0.16 and fvg != thesis.fvg:
                noise_reasons.append("WEAK_DISPLACEMENT")
            if ob_overlap <= 0.02 and raid_fuel < 0.22 and fvg != thesis.fvg:
                noise_reasons.append("NO_RAID_OR_ORDER_BLOCK_ORIGIN")
            classification = "NOISE" if noise_reasons else "VALID"
            candidates.append(_EntryZoneCandidate(
                fvg=fvg, timeframe=tf, order_block_low=ob_low, order_block_high=ob_high,
                displacement_score=impulse_score, order_block_overlap=ob_overlap, higher_tf_overlap=htf_overlap,
                raid_fuel_score=raid_fuel, freshness_score=freshness,
                phase_alignment=phase_alignment, reprice_score=reprice,
                structural_score=score, classification=classification,
                reason=";".join(noise_reasons) if noise_reasons else "DISPLACEMENT_FUNDED_REPRICE_ZONE",
            ))
        valid = [z for z in candidates if z.classification != "NOISE"] or candidates
        if not valid:
            return thesis.fvg, {"zone_selection_model": "STRUCTURAL_ZONE_GRAPH", "candidates": []}
        selected = max(valid, key=lambda z: (z.structural_score, z.timeframe == "15m", z.displacement_score, z.freshness_score))
        candidate_payload = [z.payload(selected=(z is selected)) for z in sorted(candidates, key=lambda x: x.structural_score, reverse=True)]
        plan = {
            "ts": time.time(), "role": "ENTRY", "side": thesis.side,
            "zone_selection_model": "STRUCTURAL_ZONE_GRAPH_FVG_OB_RAID_ORIGIN",
            "summary": (
                f"selected {selected.timeframe} FVG/OB {selected.low:.4f}-{selected.high:.4f} "
                f"score={selected.structural_score:.2f}; noise={sum(1 for z in candidates if z.classification == 'NOISE')}"
            ),
            "selected": selected.payload(selected=True), "candidates": candidate_payload,
        }
        self._last_entry_zone_plan = plan
        self._last_analysis.update({
            "entry_zone_selection_model": plan["zone_selection_model"],
            "entry_zone_selected_tf": selected.timeframe,
            "entry_zone_low": selected.low, "entry_zone_high": selected.high,
            "entry_zone_equilibrium": selected.equilibrium,
            "entry_zone_score": selected.structural_score,
            "entry_zone_order_block_overlap": selected.order_block_overlap,
            "entry_zone_higher_tf_overlap": selected.higher_tf_overlap,
            "entry_zone_raid_fuel_score": selected.raid_fuel_score,
            "entry_zone_candidate_count": len(candidates),
            "entry_zone_noise_count": sum(1 for z in candidates if z.classification == "NOISE"),
        })
        return selected.fvg, plan

    def _liquidity_protected_stop(self, thesis: _Thesis, entry: float, snap: LiquidityMapSnapshot,
                                  atr: float) -> Optional[_StopPlan]:
        """Place invalidation beyond the relevant stop-liquidity cluster.

        The old stop sat only beyond a wick.  This method recognises when a
        nearby live same-side pool would make that stop itself the easy draw on
        liquidity, then protects beyond the outer structurally-related cluster.
        Distant/weak pools are kept as noise telemetry instead of widening risk.
        """
        raw_stop = self._structural_stop(thesis, atr)
        if raw_stop is None:
            return None
        anchor = _f(getattr(thesis, "invalidation_anchor", 0.0), 0.0)
        if anchor <= 0 and thesis.sweep is not None:
            anchor = _f(getattr(thesis.sweep, "wick_extreme", 0.0), raw_stop)
        if anchor <= 0:
            anchor = raw_stop
        selected_zone = ((self._last_entry_zone_plan or {}).get("selected") or {})
        ob_low = _f(selected_zone.get("order_block_low", 0.0), 0.0)
        ob_high = _f(selected_zone.get("order_block_high", 0.0), 0.0)
        # Invalidation must also sit behind the selected institutional origin;
        # otherwise a normal order-block mitigation can stop the ticket before
        # the thesis is invalidated.
        if ob_high > ob_low > 0:
            anchor = min(anchor, ob_low) if thesis.side == "long" else max(anchor, ob_high)
        pools = list(snap.ssl_pools if thesis.side == "long" else snap.bsl_pools)
        # Structural envelope expands when volatility is high and when auction
        # control is clear; it does not chase remote targets into bad R geometry.
        clarity = _f(getattr(self._market_state, "clarity", 0.0), 0.0)
        envelope_atr = 0.45 + 0.80 * self._atr_pctile + 0.35 * clarity
        selected_rows: List[Dict[str, Any]] = []
        noise_rows: List[Dict[str, Any]] = []
        mass = 0.0
        outer = anchor
        for target in pools:
            pool = getattr(target, "pool", None)
            px = _f(getattr(pool, "price", 0.0), 0.0)
            if px <= 0:
                continue
            on_stop_side = (thesis.side == "long" and px < entry) or (thesis.side == "short" and px > entry)
            if not on_stop_side:
                continue
            dist_anchor_atr = abs(px - anchor) / max(atr, _EPS)
            sig = max(0.0, _f(getattr(target, "significance", 0.0), 0.0))
            tf = str(getattr(pool, "timeframe", "") or "")
            relevance = math.tanh(sig / 4.0) * math.exp(-dist_anchor_atr / max(envelope_atr, _EPS))
            row = {"pool_price": px, "timeframe": tf, "significance": sig,
                   "distance_from_anchor_atr": dist_anchor_atr, "relevance": relevance}
            if dist_anchor_atr <= envelope_atr and relevance >= 0.10:
                row["classification"] = "PROTECTED_BEYOND_STOP"
                selected_rows.append(row)
                mass += sig * max(relevance, 0.05)
                outer = min(outer, px) if thesis.side == "long" else max(outer, px)
            else:
                row["classification"] = "NOISE_OUTSIDE_INVALIDATION_CLUSTER"
                noise_rows.append(row)
        base_clearance = atr * (self._stop_clearance_base_atr + self._stop_clearance_pctile_slope_atr * self._atr_pctile)
        mass_buffer = atr * min(0.22, 0.045 * math.log1p(max(0.0, mass)))
        clearance = base_clearance + mass_buffer
        stop = min(raw_stop, anchor - clearance, outer - clearance) if thesis.side == "long" else max(raw_stop, anchor + clearance, outer + clearance)
        return _StopPlan(
            price=stop, structural_anchor=anchor, outer_protected_liquidity=outer,
            clearance=clearance, protected_cluster_mass=mass,
            selected_pools=tuple(selected_rows), noise_pools=tuple(noise_rows),
        )

    def _target_cluster_metrics(self, selected: PoolTarget, pools: List[PoolTarget], atr: float) -> Dict[str, Any]:
        """Estimate relative stop-liquidity concentration around a TP pool."""
        pool = getattr(selected, "pool", None)
        px = _f(getattr(pool, "price", 0.0))
        base_sig = max(0.01, _f(getattr(selected, "significance", 0.0), 0.01))
        radius_atr = 0.16 + 0.08 * self._atr_pctile + 0.06 * math.tanh(base_sig / 4.0)
        members: List[Dict[str, Any]] = []
        mass = 0.0
        sources = set()
        for other in list(pools or []):
            other_pool = getattr(other, "pool", None)
            opx = _f(getattr(other_pool, "price", 0.0))
            dist_atr = abs(opx - px) / max(atr, _EPS)
            if opx <= 0 or dist_atr > radius_atr:
                continue
            sig = max(0.01, _f(getattr(other, "significance", 0.0), 0.01))
            tf_sources = {str(x) for x in list(getattr(other, "tf_sources", []) or []) if str(x)}
            tf_sources.add(str(getattr(other_pool, "timeframe", "") or ""))
            htf_count = max(1, int(getattr(other_pool, "htf_count", 1) or 1))
            confluence_mult = 1.0 + 0.16 * max(0, len(tf_sources) - 1) + 0.07 * max(0, htf_count - 1)
            weighted = sig * confluence_mult * math.exp(-dist_atr / max(radius_atr, _EPS))
            mass += weighted
            tf = str(getattr(other_pool, "timeframe", "") or "")
            sources.add(tf)
            members.append({"pool_price": opx, "timeframe": tf, "significance": sig, "distance_atr": dist_atr,
                            "tf_sources": sorted(tf_sources), "confluence_multiplier": confluence_mult})
        concentration = _clamp(math.tanh(mass / 6.0) * (0.84 + min(0.16, 0.05 * max(0, len(sources) - 1))))
        is_noise = len(members) == 1 and base_sig < 1.15 and TF_HIERARCHY.get(str(getattr(pool, "timeframe", "1m")), 1) <= _TF_15M_RANK
        return {
            "cluster_mass": mass, "cluster_score": concentration,
            "cluster_count": len(members), "cluster_timeframes": sorted(sources),
            "cluster_radius_atr": radius_atr, "cluster_members": members,
            "noise": bool(is_noise),
            "noise_reason": "ISOLATED_LOW_MASS_POOL" if is_noise else "NONE",
        }

    def _path_imbalance_impedance(self, side: str, entry: float, tp: float, atr: float,
                                  candles_5m: Optional[List[Dict]], candles_15m: Optional[List[Dict]]) -> Dict[str, Any]:
        """Measure opposing FVG/OB repricing zones encountered before a TP.

        A target behind a substantial opposing imbalance can still be chosen,
        but it is ranked below an equally liquid objective with a cleaner route.
        This is delivery geometry, not a new permission gate.
        """
        barrier_side = "short" if side == "long" else "long"
        low_route, high_route = sorted((entry, tp))
        barriers: List[Dict[str, Any]] = []
        for tf, rows, lookback, tf_weight in (
            ("5m", list(candles_5m or []), 52, 0.78),
            ("15m", list(candles_15m or []), 28, 1.00),
        ):
            for fvg in self._candidate_fvgs(barrier_side, rows, atr, tf, lookback):
                mid = fvg.equilibrium
                if not (low_route < mid < high_route):
                    continue
                ob_low, ob_high = self._order_block_for_fvg(barrier_side, fvg, rows)
                overlap = self._zone_overlap_ratio(fvg.low, fvg.high, ob_low, ob_high)
                structural_mass = tf_weight * (
                    0.55 * math.tanh(max(0.0, fvg.displacement_atr) / 1.25) + 0.45 * overlap
                )
                barriers.append({
                    "timeframe": tf, "low": fvg.low, "high": fvg.high,
                    "equilibrium": mid, "displacement_atr": fvg.displacement_atr,
                    "order_block_overlap": overlap, "structural_mass": structural_mass,
                })
        total_mass = sum(float(row["structural_mass"]) for row in barriers)
        penalty = 1.0 / (1.0 + 0.24 * total_mass)
        return {"path_imbalance_count": len(barriers), "path_imbalance_mass": total_mass,
                "path_imbalance_penalty": penalty, "path_imbalance_zones": barriers}

    def _pd_array_confluence(self, thesis: _Thesis, entry: float, atr: float, now: float,
                             candles_5m: List[Dict], candles_15m: List[Dict],
                             candles_4h: List[Dict]) -> _PDArrayConfluence:
        dealing_low, dealing_high, dealing_mid = self._dealing_range(candles_15m, candles_4h)
        width = max(dealing_high - dealing_low, _EPS)
        pd_position = _clamp((entry - dealing_low) / width) if dealing_high > dealing_low else 0.50
        pd_zone = "DISCOUNT" if pd_position < 0.50 else ("PREMIUM" if pd_position > 0.50 else "EQUILIBRIUM")
        if thesis.side == "long":
            pd_score = 1.0 if pd_position <= 0.50 else _clamp(1.0 - (pd_position - 0.50) / 0.35)
            impulse_extreme = max([_f(row.get("h")) for row in candles_5m[max(0, thesis.fvg.index - 2):]] or [entry])
            retracement = (impulse_extreme - entry) / max(impulse_extreme - thesis.invalidation_anchor, _EPS)
        else:
            pd_score = 1.0 if pd_position >= 0.50 else _clamp(1.0 - (0.50 - pd_position) / 0.35)
            impulse_extreme = min([_f(row.get("l")) for row in candles_5m[max(0, thesis.fvg.index - 2):] if _f(row.get("l")) > 0] or [entry])
            retracement = (entry - impulse_extreme) / max(thesis.invalidation_anchor - impulse_extreme, _EPS)

        ote_min = _cfg_float("ICT_OTE_MIN_RETRACEMENT", 0.50)
        ote_max = _cfg_float("ICT_OTE_MAX_RETRACEMENT", 0.79)
        if ote_min <= retracement <= ote_max:
            center = (ote_min + ote_max) / 2.0
            half = max((ote_max - ote_min) / 2.0, _EPS)
            ote_score = 1.0 - 0.18 * min(1.0, abs(retracement - center) / half)
        else:
            ote_score = math.exp(-min(abs(retracement - ote_min), abs(retracement - ote_max)) / 0.18)

        gap_half = max((thesis.fvg.high - thesis.fvg.low) / 2.0, 0.05 * atr, _EPS)
        fvg_score = _clamp(1.0 - abs(entry - thesis.fvg.equilibrium) / (gap_half * 1.25))

        ob_low, ob_high = self._order_block_zone(thesis, candles_5m)
        if ob_low > 0 and ob_high > ob_low:
            if ob_low <= entry <= ob_high:
                ob_score = 1.0
            else:
                ob_distance = min(abs(entry - ob_low), abs(entry - ob_high))
                ob_score = 0.90 * math.exp(-ob_distance / max(0.75 * atr, _EPS))
        else:
            ob_score = 0.55

        kz_score, kz_label = self._killzone_quality(candles_5m, now)
        score = _clamp(0.26 * pd_score + 0.24 * ote_score + 0.22 * fvg_score + 0.18 * ob_score + 0.10 * kz_score)
        block = "NONE"
        if (_cfg_bool("ICT_PD_ARRAY_STRICT_PREMIUM_DISCOUNT", True)
                and thesis.entry_type == EntryType.LIQUIDITY_RAID_REVERSAL):
            if thesis.side == "long" and pd_position > 0.50:
                block = "LONG_NOT_IN_DISCOUNT_PD_ARRAY"
            elif thesis.side == "short" and pd_position < 0.50:
                block = "SHORT_NOT_IN_PREMIUM_PD_ARRAY"
        min_score = _cfg_float("ICT_PD_ARRAY_MIN_SCORE", 0.58)
        if block == "NONE" and score < min_score:
            block = "PD_ARRAY_CONFLUENCE_BELOW_FLOOR"
        return _PDArrayConfluence(
            score=score, block=block, dealing_low=dealing_low, dealing_high=dealing_high,
            dealing_mid=dealing_mid, pd_position=pd_position, pd_zone=pd_zone,
            ote_retracement=_f(retracement), ote_score=_clamp(ote_score), fvg_score=fvg_score,
            order_block_low=ob_low, order_block_high=ob_high, order_block_score=_clamp(ob_score),
            killzone_label=kz_label, killzone_score=kz_score,
        )

    def _try_reprice_thesis(self, thesis: _Thesis, snap: LiquidityMapSnapshot,
                            price: float, atr: float, now: float,
                            candles_5m: Optional[List[Dict]] = None,
                            candles_15m: Optional[List[Dict]] = None,
                            candles_4h: Optional[List[Dict]] = None) -> None:
        if (self._market_state is not None and self._market_state.has_firm_parent_control
                and not self._market_state.owns(thesis.side)):
            thesis.last_reason = "auction control remains with opposing higher-timeframe liquidity transfer"
            self._record_block(
                "AUCTION_CONTROL_REMAINS_OPPOSING_SIDE",
                trigger="MARKET_STATE_OWNER",
                thesis_side=thesis.side,
                controlling_side=self._market_state.control_side,
                controlling_parent_tf=self._market_state.parent_timeframe,
                controlling_parent_quality=self._market_state.parent_quality,
            )
            return
        # Select one executable repricing zone from all visible FVG / order-
        # block origins in the market-owned direction.  This makes BTC and
        # SILVER use the same broader auction intelligence as the new core,
        # rather than executing whichever latest gap happened to be observed.
        selected_fvg, _entry_zone_plan = self._rank_entry_zones(
            thesis, snap, price, atr,
            list(candles_5m or []), list(candles_15m or []),
        )
        thesis.fvg = selected_fvg
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
            max_extension = _cfg_float("ICT_MAX_FVG_EXTENSION_BEFORE_REPRICE_ATR", 3.25)
            directional_extension = (
                (thesis.side == "long" and price > high_bound)
                or (thesis.side == "short" and price < low_bound)
            )
            if directional_extension and (distance_to_zone / max(atr, _EPS)) > max_extension:
                self._thesis = None
                self._state = EngineState.SCANNING
                thesis.last_reason = "displacement travelled too far before repricing"
                self._record_block(
                    "FVG_REPRICE_TOO_EXTENDED_RESET",
                    fvg_extension_atr=distance_to_zone / max(atr, _EPS),
                    fvg_max_extension_atr=max_extension,
                )
                return
            thesis.last_reason = "MSS confirmed; awaiting 5m FVG rebalance"
            self._record_block("AWAITING_FVG_REBALANCE", trigger="AWAITING_FVG_REBALANCE")
            return
        if not fvg_rebalanced:
            thesis.last_reason = "FVG touched; awaiting equilibrium rebalance"
            self._record_block("AWAITING_FVG_EQUILIBRIUM_REBALANCE", trigger="AWAITING_FVG_EQUILIBRIUM_REBALANCE")
            return
        entry = price
        pd_confluence: Optional[_PDArrayConfluence] = None
        if _cfg_bool("ICT_PD_ARRAY_GUARD_ENABLED", True):
            pd_confluence = self._pd_array_confluence(
                thesis, entry, atr, now,
                list(candles_5m or []), list(candles_15m or []), list(candles_4h or []),
            )
            pd_payload = {
                "pd_array_score": pd_confluence.score,
                "pd_array_block": pd_confluence.block,
                "pd_array_zone": pd_confluence.pd_zone,
                "pd_array_position": pd_confluence.pd_position,
                "pd_dealing_low": pd_confluence.dealing_low,
                "pd_dealing_high": pd_confluence.dealing_high,
                "pd_dealing_mid": pd_confluence.dealing_mid,
                "pd_ote_retracement": pd_confluence.ote_retracement,
                "pd_ote_score": pd_confluence.ote_score,
                "pd_fvg_ce_score": pd_confluence.fvg_score,
                "pd_order_block_low": pd_confluence.order_block_low,
                "pd_order_block_high": pd_confluence.order_block_high,
                "pd_order_block_score": pd_confluence.order_block_score,
                "pd_killzone": pd_confluence.killzone_label,
                "pd_killzone_score": pd_confluence.killzone_score,
                "pd_array_model": "PREMIUM_DISCOUNT_OTE_FVG_CE_OB_KILLZONE",
            }
            self._last_analysis.update(pd_payload)
            if pd_confluence.block != "NONE":
                self._last_analysis.update({"pd_array_advisory": pd_confluence.block})
                if _cfg_bool("ICT_LEGACY_FILTER_COMPATIBILITY_ENABLED", False):
                    thesis.last_reason = f"legacy PD-array compatibility rejected setup: {pd_confluence.block}"
                    self._record_block(pd_confluence.block, trigger="LEGACY_PD_ARRAY_FILTER", **pd_payload)
                    return
        stop_plan = self._liquidity_protected_stop(thesis, entry, snap, atr)
        stop = stop_plan.price if stop_plan is not None else None
        anchor = stop_plan.structural_anchor if stop_plan is not None else _f(getattr(thesis, "invalidation_anchor", 0.0))
        clearance = abs(stop - anchor) if stop is not None else 0.0
        self._last_analysis.update({"entry": entry, "stop_clearance_atr": clearance / max(atr, _EPS)})
        if stop_plan is not None:
            self._last_analysis.update(stop_plan.payload())
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
        target = self._select_liquidity_target(
            thesis.side, entry, stop, snap, atr,
            candles_5m=list(candles_5m or []), candles_15m=list(candles_15m or []),
        )
        if target is None:
            thesis.last_reason = "no opposing higher-timeframe liquidity destination with positive net R"
            self._record_block("AWAITING_POSITIVE_NET_R_LIQUIDITY_TARGET")
            return
        target_obj, tp, rr, rank_score, delivery_score = target
        selectivity_block = self._selectivity_block(thesis, rr, rank_score, delivery_score)
        if selectivity_block is not None:
            reason, details = selectivity_block
            self._last_analysis.update({"legacy_selectivity_advisory": reason, **details})
            if _cfg_bool("ICT_LEGACY_FILTER_COMPATIBILITY_ENABLED", False):
                thesis.last_reason = f"legacy selectivity compatibility rejected setup: {reason}"
                self._record_block(reason, trigger="LEGACY_SELECTIVITY_FILTER", **details)
                return
        dossier: Optional[_SetupQualityDossier] = None
        if _cfg_bool("ICT_SETUP_DOSSIER_ENABLED", True):
            dossier = self._setup_quality_dossier(thesis, rr, rank_score, delivery_score, pd_confluence)
            dossier_payload = dossier.as_payload()
            self._last_analysis.update(dossier_payload)
            if dossier.block != "NONE":
                self._last_analysis.update({"setup_dossier_advisory": dossier.block})
                if _cfg_bool("ICT_LEGACY_FILTER_COMPATIBILITY_ENABLED", False):
                    thesis.last_reason = f"legacy dossier compatibility rejected setup: {dossier.block}"
                    self._record_block(dossier.block, trigger="LEGACY_SETUP_DOSSIER_FILTER", **dossier_payload)
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
            "market_phase": thesis.market_phase,
            "auction_control_side": thesis.auction_control_side,
            "auction_control_score": thesis.auction_control_score,
            "execution_posture": thesis.execution_posture,
            "auction_risk_scalar": thesis.auction_risk_scalar,
        }
        if pd_confluence is not None:
            quality.update({
                "pd_array_score": pd_confluence.score,
                "pd_array_position": pd_confluence.pd_position,
                "ote_retracement": pd_confluence.ote_retracement,
                "order_block_score": pd_confluence.order_block_score,
                "killzone_score": pd_confluence.killzone_score,
            })
        if dossier is not None:
            quality.update(dossier.as_payload())
        explanation = (
            f"4H={thesis.context_4h.label}({thesis.context_4h.confidence:.2f}) | "
            f"15m={thesis.context_15m.label}({thesis.context_15m.confidence:.2f}) | "
            f"bias={thesis.context_path}({thesis.context_delivery_score:.2f}) | "
            f"phase={thesis.market_phase} control={thesis.auction_control_side} posture={thesis.execution_posture} | "
            f"archetype={thesis.entry_type.value} MSS/FVG | displacement={thesis.displacement_atr:.2f}ATR | "
            f"deliveryScore={delivery_score:.2f} rankScore={rank_score:.2f} (uncalibrated)"
        )
        if pd_confluence is not None:
            explanation += (
                f" | PD={pd_confluence.pd_zone}({pd_confluence.score:.2f}) "
                f"OTE={pd_confluence.ote_retracement:.2f} OB={pd_confluence.order_block_score:.2f} "
                f"KZ={pd_confluence.killzone_label}"
            )
        if dossier is not None:
            explanation += f" | setupGrade={dossier.grade}({dossier.score:.2f})"
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
            "market_phase": thesis.market_phase,
            "auction_control_side": thesis.auction_control_side,
            "auction_control_score": thesis.auction_control_score,
            "execution_posture": thesis.execution_posture,
            "auction_risk_scalar": thesis.auction_risk_scalar,
        })
        if dossier is not None:
            self._last_analysis.update(dossier.as_payload())
        if pd_confluence is not None:
            self._last_analysis.update({
                "pd_array_score": pd_confluence.score,
                "pd_array_block": pd_confluence.block,
                "pd_array_zone": pd_confluence.pd_zone,
                "pd_array_position": pd_confluence.pd_position,
                "pd_dealing_low": pd_confluence.dealing_low,
                "pd_dealing_high": pd_confluence.dealing_high,
                "pd_dealing_mid": pd_confluence.dealing_mid,
                "pd_ote_retracement": pd_confluence.ote_retracement,
                "pd_ote_score": pd_confluence.ote_score,
                "pd_fvg_ce_score": pd_confluence.fvg_score,
                "pd_order_block_low": pd_confluence.order_block_low,
                "pd_order_block_high": pd_confluence.order_block_high,
                "pd_order_block_score": pd_confluence.order_block_score,
                "pd_killzone": pd_confluence.killzone_label,
                "pd_killzone_score": pd_confluence.killzone_score,
                "pd_array_model": "PREMIUM_DISCOUNT_OTE_FVG_CE_OB_KILLZONE",
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
                                 snap: LiquidityMapSnapshot, atr: float,
                                 candles_5m: Optional[List[Dict]] = None,
                                 candles_15m: Optional[List[Dict]] = None
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
        all_candidate_rows: List[Dict[str, Any]] = []
        opposing_pools = snap.ssl_pools if side == "long" else snap.bsl_pools
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
            cluster = self._target_cluster_metrics(t, list(pools or []), atr)
            sig_term = math.tanh(significance / 5.0)
            dist_reachability = math.exp(-max(0.0, distance_atr - 1.0) / 8.0)
            # Selection follows the highest quality observable liquidity
            # concentration, not just the closest labelled swing.  An isolated
            # low-mass print stays visible as noise and naturally loses priority.
            delivery_score = max(0.0, min(
                0.99,
                0.34 * context + 0.20 * sig_term + 0.29 * float(cluster["cluster_score"])
                + 0.17 * dist_reachability,
            ))
            cost_r = self._execution_cost_points / max(risk, _EPS)
            net_win_r = rr - cost_r
            lo, hi = sorted((entry, tp))
            gauntlet_n = 0
            gauntlet_sig = 0.0
            for opp in list(opposing_pools or []):
                opp_pool = getattr(opp, "pool", None)
                opp_px = _f(getattr(opp_pool, "price", 0.0))
                if opp_px <= lo or opp_px >= hi:
                    continue
                opp_sig = max(0.0, _f(getattr(opp, "significance", 0.0)))
                if opp_sig >= max(1.0, significance * 0.45):
                    gauntlet_n += 1
                    gauntlet_sig += opp_sig
            gauntlet_penalty = 1.0 / (1.0 + 0.22 * gauntlet_n + 0.035 * gauntlet_sig)
            path_impedance = self._path_imbalance_impedance(
                side, entry, tp, atr, candles_5m, candles_15m,
            )
            # A target is selected by structural score and net R; the score is
            # explicitly not converted into an uncalibrated win probability.
            noise_penalty = 0.58 if bool(cluster["noise"]) else 1.0
            rank_score = (
                delivery_score * max(0.0, net_win_r) * gauntlet_penalty
                * noise_penalty * float(path_impedance["path_imbalance_penalty"])
            )
            row = {
                "pool_price": px,
                "tp_price": tp,
                "pool_side": str(getattr(getattr(pool, "side", None), "value", "") or ""),
                "timeframe": str(getattr(pool, "timeframe", "") or ""),
                "tf_sources": list(getattr(t, "tf_sources", []) or []),
                "significance": significance,
                "distance_atr": distance_atr,
                "gross_rr": rr,
                "cost_r": cost_r,
                "net_win_r": net_win_r,
                "delivery_score": delivery_score,
                "rank_score": rank_score,
                "gauntlet_n": gauntlet_n,
                "gauntlet_sig": gauntlet_sig,
                "gauntlet_penalty": gauntlet_penalty,
                "structural_liquidity_mass": float(cluster["cluster_mass"]),
                "liquidity_cluster_score": float(cluster["cluster_score"]),
                "liquidity_cluster_count": int(cluster["cluster_count"]),
                "liquidity_cluster_timeframes": list(cluster["cluster_timeframes"]),
                "liquidity_cluster_radius_atr": float(cluster["cluster_radius_atr"]),
                "liquidity_cluster_members": list(cluster["cluster_members"]),
                "classification": "NOISE" if cluster["noise"] else "VALID_TARGET_LIQUIDITY",
                "noise_reason": str(cluster["noise_reason"]),
                "noise_penalty": noise_penalty,
                **path_impedance,
                "buffer": buffer,
                "selected": False,
                "reason": "opposing HTF liquidity candidate",
            }
            all_candidate_rows.append(row)
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
                    "gauntlet_n": gauntlet_n,
                    "gauntlet_sig": gauntlet_sig,
                    "gauntlet_penalty": gauntlet_penalty,
                    "cluster_mass": float(cluster["cluster_mass"]),
                    "cluster_score": float(cluster["cluster_score"]),
                    "cluster_count": int(cluster["cluster_count"]),
                    "noise_penalty": noise_penalty,
                    "path_imbalance_penalty": float(path_impedance["path_imbalance_penalty"]),
                    "path_imbalance_mass": float(path_impedance["path_imbalance_mass"]),
                    "path_imbalance_count": int(path_impedance["path_imbalance_count"]),
                    "row": row,
                })
            else:
                audit["non_positive_net_reward"] += 1
        audit["min_structural_rr"] = self._min_structural_rr
        audit["max_structural_rr_reference"] = max_rr_cap
        audit["target_selection_model"] = "LIQUIDITY_GRAPH_NET_R_RANK"
        audit["target_selection_authority"] = "LIQUIDITY_CONCENTRATION_GRAPH_NET_R_RANK"
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
                                    "summary": summary, "candidates": all_candidate_rows}
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
        selected_row = dict(best.get("row") or {})
        selected_row["selected"] = True
        for row in all_candidate_rows:
            if (abs(_f(row.get("pool_price")) - _f(selected_row.get("pool_price"))) <= 1e-9
                    and str(row.get("timeframe", "")) == str(selected_row.get("timeframe", ""))):
                row["selected"] = True
        self._last_pool_plan = {
            "ts": time.time(), "role": "TP", "side": side,
            "summary": f"LIQUIDITY_CONCENTRATION_GRAPH {target.pool.timeframe} {target.pool.side.value}@{target.pool.price:.4f} mass={float(best.get('cluster_mass', 0.0)):.2f} grossRR={rr:.2f} netWinR={net_win_r:.2f} deliveryScore={delivery_score:.2f} rank={rank_score:+.2f}",
            "selected": selected_row,
            "candidates": all_candidate_rows,
        }
        self._last_analysis.update({
            "target_timeframe": str(getattr(target.pool, "timeframe", "")),
            "target_pool_price": _f(getattr(target.pool, "price", 0.0)),
            "target_tp_buffer": buffer, "target_distance_atr": distance_atr,
            "target_significance": significance, "target_rr": rr,
            "target_gross_rr": rr, "target_cost_r": cost_r,
            "target_net_win_r": net_win_r,
            "target_gauntlet_n": int(best.get("gauntlet_n", 0) or 0),
            "target_gauntlet_penalty": float(best.get("gauntlet_penalty", 1.0) or 1.0),
            "target_structural_liquidity_mass": float(best.get("cluster_mass", 0.0) or 0.0),
            "target_liquidity_cluster_score": float(best.get("cluster_score", 0.0) or 0.0),
            "target_liquidity_cluster_count": int(best.get("cluster_count", 0) or 0),
            "target_path_imbalance_penalty": float(best.get("path_imbalance_penalty", 1.0) or 1.0),
            "target_path_imbalance_mass": float(best.get("path_imbalance_mass", 0.0) or 0.0),
            "target_path_imbalance_count": int(best.get("path_imbalance_count", 0) or 0),
            "target_selection_model": "LIQUIDITY_GRAPH_NET_R_RANK",
            "target_selection_authority": "LIQUIDITY_CONCENTRATION_GRAPH_NET_R_RANK",
            "target_policy_max_rr": max_rr_cap,
            "delivery_probability": None, "probability_calibrated": False,
            "delivery_score": delivery_score, "target_rank_score": rank_score,
            "delivery_utility_r": None,
        })
        return target, tp, rr, rank_score, delivery_score


# The external strategy imports EntryEngine; this alias names the unified structural authority.
EntryEngine = ICTLiquidityEntryEngine

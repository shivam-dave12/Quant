"""
ict_liquidity_entry.py — single-authority ICT + Liquidity execution engine.

This module is intentionally the only alpha authority for new positions.
It expresses one institutional auction model:

    4H delivery context -> 15m dealing-range confirmation ->
    5m external-liquidity raid -> 5m displacement/MSS ->
    5m FVG retracement -> structural invalidation -> opposing HTF liquidity TP.

It does not consume secondary score layers or non-structural directional overlays,
session quotas, approach/momentum entries, or layered confirmation stacks. Risk,
lot sizing, bracket protection and reconciliation remain downstream mechanical
controls in the execution/risk modules.
"""
from __future__ import annotations

import logging
import math
import statistics
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

try:
    from strategy.liquidity_map import LiquidityMapSnapshot, PoolTarget, SweepResult, TF_HIERARCHY
except ImportError:  # pragma: no cover
    from liquidity_map import LiquidityMapSnapshot, PoolTarget, SweepResult, TF_HIERARCHY  # type: ignore

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


@dataclass
class _Thesis:
    sweep_key: tuple
    side: str
    sweep: SweepResult
    formed_at: float
    context_4h: _TrendContext
    context_15m: _TrendContext
    mss_level: float
    displacement_atr: float
    fvg: _FVG
    last_reason: str = "waiting for FVG repricing"


_EPS = 1e-12
_TF_15M_RANK = TF_HIERARCHY.get("15m", 3)


def _f(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else default
    except Exception:
        return default


def _closed(candles: Optional[Sequence[Dict]], minimum: int = 0) -> List[Dict]:
    rows = list(candles or [])
    # Data managers deliver the latest still-forming candle; structure never reads it.
    rows = rows[:-1] if len(rows) > 1 else []
    return rows if len(rows) >= minimum else []


def _median(values: Iterable[float], default: float = 0.0) -> float:
    clean = [float(v) for v in values if math.isfinite(float(v))]
    return statistics.median(clean) if clean else default


def _true_range(c: Dict, prev_close: float) -> float:
    h, l = _f(c.get("h")), _f(c.get("l"))
    return max(h - l, abs(h - prev_close), abs(l - prev_close))


def _timeframe_atr(candles: Sequence[Dict], period: int = 14) -> float:
    """Closed-bar ATR for the timeframe being interpreted; prevents cross-TF unit distortion."""
    rows = list(candles or [])
    if len(rows) < 2:
        return 0.0
    rows = rows[-min(len(rows), period + 1):]
    tr = [_true_range(rows[i], _f(rows[i - 1].get("c"))) for i in range(1, len(rows))]
    return sum(tr) / len(tr) if tr else 0.0


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
    signed_strength = 0.68 * math.tanh(slope_atr * window / 3.0) + 0.32 * structure
    strength = abs(signed_strength) * (0.55 + 0.45 * efficiency)
    side = 1 if signed_strength > 0.18 else (-1 if signed_strength < -0.18 else 0)
    return _TrendContext(side, min(1.0, strength), slope_atr, efficiency, structure)


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

    # ---------- public interface retained for execution lifecycle ----------

    def set_atr_pctile(self, pctile: float) -> None:
        self._atr_pctile = max(0.0, min(1.0, _f(pctile, 0.5)))

    def update(self, liq_snapshot: LiquidityMapSnapshot, price: float, atr: float, now: float,
               candles_5m: Optional[List[Dict]] = None,
               candles_15m: Optional[List[Dict]] = None,
               candles_4h: Optional[List[Dict]] = None) -> None:
        if atr <= _EPS or price <= 0:
            return
        self._expire(now)
        c5 = _closed(candles_5m, 24)
        c15 = _closed(candles_15m, 24)
        c4h = _closed(candles_4h, 20)
        if not c5 or not c15 or not c4h:
            self._last_scan_skip = {"warmup": 1}
            return

        atr4h = _timeframe_atr(c4h)
        atr15m = _timeframe_atr(c15)
        if atr4h <= _EPS or atr15m <= _EPS:
            self._last_scan_skip = {"timeframe_atr_warmup": 1}
            return
        ctx4 = _robust_trend(c4h, atr4h, min(32, len(c4h)))
        ctx15 = _robust_trend(c15, atr15m, min(56, len(c15)))
        self._last_analysis = {
            "model": "ICT_LIQUIDITY_4H_15M_5M",
            "state": self._state.value,
            "context_4h": ctx4.label,
            "context_4h_conf": ctx4.confidence,
            "context_15m": ctx15.label,
            "context_15m_conf": ctx15.confidence,
            "context_4h_atr": atr4h,
            "context_15m_atr": atr15m,
            "entry_5m_atr": atr,
            "authority": "STRUCTURAL_ONLY",
        }
        if self._state in (EngineState.ENTERING, EngineState.IN_POSITION):
            return
        if self._signal is not None:
            return

        if self._thesis is not None:
            if now - self._thesis.formed_at > 900.0:
                self._thesis = None
                self._state = EngineState.SCANNING
            else:
                self._try_reprice_thesis(self._thesis, liq_snapshot, price, atr, now)
                if self._signal is not None:
                    return

        self._state = EngineState.CONTEXT_READY if (ctx4.side != 0 and ctx15.side != 0) else EngineState.SCANNING
        fresh = self._fresh_5m_sweeps(liq_snapshot, now)
        if not fresh:
            self._last_scan_skip = {"no_fresh_5m_raid": 1}
            return
        for sweep in sorted(fresh, key=lambda s: _f(getattr(s, "quality", 0.0)), reverse=True):
            side = str(getattr(sweep, "direction", "") or "").lower()
            direction = 1 if side == "long" else (-1 if side == "short" else 0)
            if direction == 0:
                continue
            # Context is directional permission: 4H and 15m must support delivery.
            if ctx4.side != direction or ctx15.side != direction:
                self._last_scan_skip = {"htf_not_aligned": 1}
                continue
            thesis = self._build_thesis(sweep, side, ctx4, ctx15, c5, atr, now)
            if thesis is None:
                continue
            self._thesis = thesis
            self._state = EngineState.LIQUIDITY_RAID
            self._try_reprice_thesis(thesis, liq_snapshot, price, atr, now)
            return

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

    def on_entry_failed(self) -> None:
        self._active_signal = None
        self._signal = None
        self._thesis = None
        self._state = EngineState.SCANNING
        self._state_entered = time.time()

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

    def _build_thesis(self, sweep: SweepResult, side: str, ctx4: _TrendContext,
                      ctx15: _TrendContext, candles_5m: List[Dict], atr: float,
                      now: float) -> Optional[_Thesis]:
        idx = int(getattr(sweep, "sweep_candle_idx", len(candles_5m) - 4) or 0)
        idx = max(3, min(idx, len(candles_5m) - 2))
        recent_close = _f(candles_5m[-1].get("c"))
        wick = _f(getattr(sweep, "wick_extreme", 0.0), recent_close)
        pre = candles_5m[max(0, idx - 12):idx]
        if len(pre) < 4:
            self._last_scan_skip = {"insufficient_pre_raid_structure": 1}
            return None
        if side == "long":
            mss = max(_f(c.get("h")) for c in pre)
            displacement = (recent_close - wick) / max(atr, _EPS)
            mss_broken = recent_close > mss
        else:
            mss = min(_f(c.get("l")) for c in pre)
            displacement = (wick - recent_close) / max(atr, _EPS)
            mss_broken = recent_close < mss
        if not mss_broken or displacement <= 0:
            self._last_scan_skip = {"awaiting_5m_mss_displacement": 1}
            return None
        fvg = _find_fvg(candles_5m, side, idx + 1, atr)
        if fvg is None or fvg.displacement_atr <= 0:
            self._last_scan_skip = {"no_5m_displacement_fvg": 1}
            return None
        return _Thesis(
            sweep_key=_sweep_key(sweep), side=side, sweep=sweep, formed_at=now,
            context_4h=ctx4, context_15m=ctx15, mss_level=mss,
            displacement_atr=displacement, fvg=fvg,
        )

    def _try_reprice_thesis(self, thesis: _Thesis, snap: LiquidityMapSnapshot,
                            price: float, atr: float, now: float) -> None:
        zone_tol = 0.08 * atr
        in_reprice = thesis.fvg.low - zone_tol <= price <= thesis.fvg.high + zone_tol
        if not in_reprice:
            thesis.last_reason = "MSS confirmed; awaiting 5m FVG rebalance"
            self._last_analysis.update({
                "state": self._state.value,
                "trigger": "AWAITING_FVG_REBALANCE",
                "side": thesis.side,
                "displacement_atr": thesis.displacement_atr,
                "fvg_low": thesis.fvg.low,
                "fvg_high": thesis.fvg.high,
            })
            return
        entry = price
        stop = self._structural_stop(thesis, atr)
        if stop is None:
            thesis.last_reason = "invalid structural stop geometry"
            return
        target = self._select_liquidity_target(thesis.side, entry, stop, snap, atr)
        if target is None:
            thesis.last_reason = "no opposing HTF liquidity with positive delivery utility"
            return
        target_obj, tp, rr, utility, delivery_p = target
        quality = {
            "context_4h": thesis.context_4h.confidence,
            "context_15m": thesis.context_15m.confidence,
            "raid_quality": _f(getattr(thesis.sweep, "quality", 0.0)),
            "displacement_atr": thesis.displacement_atr,
            "delivery_probability": delivery_p,
            "delivery_utility_r": utility,
        }
        explanation = (
            f"4H={thesis.context_4h.label}({thesis.context_4h.confidence:.2f}) | "
            f"15m={thesis.context_15m.label}({thesis.context_15m.confidence:.2f}) | "
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
        self._last_analysis.update({
            "state": self._state.value, "side": thesis.side, "trigger": "EXECUTABLE_FVG_REPRICE",
            "displacement_atr": thesis.displacement_atr, "entry": entry, "sl": stop, "tp": tp,
            "rr": rr, "delivery_probability": delivery_p, "delivery_utility_r": utility,
        })
        logger.info("ICT_LIQUIDITY ENTRY READY %s @ %.4f | SL=%.4f TP=%.4f RR=%.2f | %s",
                    thesis.side.upper(), entry, stop, tp, rr, explanation)

    def _structural_stop(self, thesis: _Thesis, atr: float) -> Optional[float]:
        wick = _f(getattr(thesis.sweep, "wick_extreme", 0.0))
        if wick <= 0:
            return None
        # The stop is behind the raided liquidity extreme; volatility only sizes clearance.
        regime_clearance = atr * (0.10 + 0.18 * self._atr_pctile)
        if thesis.side == "long":
            sl = wick - regime_clearance
            return sl if sl < thesis.fvg.low else None
        sl = wick + regime_clearance
        return sl if sl > thesis.fvg.high else None

    def _select_liquidity_target(self, side: str, entry: float, sl: float,
                                 snap: LiquidityMapSnapshot, atr: float
                                 ) -> Optional[Tuple[PoolTarget, float, float, float, float]]:
        risk = abs(entry - sl)
        if risk <= _EPS:
            return None
        pools = snap.bsl_pools if side == "long" else snap.ssl_pools
        candidates = []
        for t in list(pools or []):
            pool = getattr(t, "pool", None)
            px = _f(getattr(pool, "price", 0.0))
            if px <= 0 or (side == "long" and px <= entry) or (side == "short" and px >= entry):
                continue
            # Entry is 5m; delivery target must be 15m+ liquidity or promoted confluence.
            tf_rank = TF_HIERARCHY.get(str(getattr(pool, "timeframe", "1m")), 1)
            promoted = int(getattr(pool, "htf_count", 0) or 0) >= 2
            if tf_rank < _TF_15M_RANK and not promoted:
                continue
            dist = abs(px - entry)
            # Rest the TP inside the pool so execution does not require a perfect touch.
            significance = max(0.01, _f(getattr(t, "significance", 0.0), 0.01))
            buffer = min(0.28 * atr, max(0.04 * atr, 0.04 * atr * math.log1p(significance)))
            tp = px - buffer if side == "long" else px + buffer
            reward = abs(tp - entry)
            rr = reward / risk
            if rr <= 1.0:
                continue
            distance_atr = dist / max(atr, _EPS)
            # Delivery probability is a structural reach model, not an entry vote.
            context = 0.50 * (self._thesis.context_4h.confidence if self._thesis else 0.0) + 0.50 * (self._thesis.context_15m.confidence if self._thesis else 0.0)
            sig_term = math.tanh(significance / 5.0)
            dist_decay = math.exp(-max(0.0, distance_atr - 1.0) / 8.0)
            p = max(0.05, min(0.95, 0.18 + 0.34 * context + 0.30 * sig_term + 0.18 * dist_decay))
            utility = p * rr - (1.0 - p)
            if utility > 0:
                candidates.append((utility, significance, -distance_atr, t, tp, rr, p))
        if not candidates:
            self._last_pool_plan = {"ts": time.time(), "role": "TP", "side": side,
                                    "summary": "no positive-utility opposing 15m+ pool"}
            return None
        best = max(candidates, key=lambda x: (x[0], x[1], x[2]))
        utility, _, _, target, tp, rr, p = best
        self._last_pool_plan = {
            "ts": time.time(), "role": "TP", "side": side,
            "summary": f"{target.pool.timeframe} {target.pool.side.value}@{target.pool.price:.4f} RR={rr:.2f} P={p:.2f} U={utility:+.2f}R",
        }
        return target, tp, rr, utility, p


# The external strategy imports EntryEngine; the alias names the only entry authority.
EntryEngine = ICTLiquidityEntryEngine

# -*- coding: utf-8 -*-
"""
strategy/market_intelligence.py — Adaptive Market Context Layer
================================================================

Single source of truth for dynamic regime-aware thresholds.

The strategy layer should not hard-code execution behaviour such as
"0.75 ATR displacement is always enough" or "420 seconds cooldown is always
right". Those values are only base priors. This module converts live market
state into adaptive thresholds used by entry, conviction, liquidity, trailing,
post-exit and reporting engines.

Inputs are intentionally generic so every strategy file can use this module
without creating circular dependencies.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, Iterable, Optional, Tuple
import math

try:
    import config
except Exception:  # pragma: no cover
    config = None  # type: ignore


def _cfg(name: str, default: Any) -> Any:
    if config is None:
        return default
    val = getattr(config, name, None)
    return default if val is None else val


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
        if math.isfinite(x):
            return x
    except Exception:
        pass
    return default


def _side_sign(side: str) -> int:
    s = str(side or "").lower()
    if s == "long":
        return 1
    if s == "short":
        return -1
    return 0


def _iter_pools(snap: Any) -> Iterable[Any]:
    if snap is None:
        return []
    pools = []
    try:
        pools.extend(list(getattr(snap, "bsl_pools", []) or []))
    except Exception:
        pass
    try:
        pools.extend(list(getattr(snap, "ssl_pools", []) or []))
    except Exception:
        pass
    return pools


def _pool_price(pool_or_target: Any) -> float:
    obj = getattr(pool_or_target, "pool", pool_or_target)
    return _safe_float(getattr(obj, "price", 0.0), 0.0)


def _pool_sig(pool_or_target: Any) -> float:
    for attr in ("significance", "score", "quality"):
        try:
            val = getattr(pool_or_target, attr)
            if callable(val):
                val = val()
            f = _safe_float(val, 0.0)
            if f:
                return f
        except Exception:
            pass
    obj = getattr(pool_or_target, "pool", pool_or_target)
    for attr in ("significance", "quality"):
        try:
            val = getattr(obj, attr)
            if callable(val):
                val = val()
            f = _safe_float(val, 0.0)
            if f:
                return f
        except Exception:
            pass
    return 0.0


@dataclass(frozen=True)
class MarketProfile:
    regime: str
    volatility: str
    structure: str
    session: str
    atr_pct: float
    liquidity_density: float
    liquidity_quality: float
    spread_bps: float
    flow_abs: float
    flow_side: str
    htf_alignment: float
    premium_discount: float
    selectivity: float
    breathing_mult: float
    notes: Tuple[str, ...] = ()

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def compact(self) -> str:
        n = ",".join(self.notes[:3])
        return (
            f"{self.regime}/{self.volatility}/{self.structure} "
            f"sel={self.selectivity:.2f} breathe={self.breathing_mult:.2f} "
            f"liq={self.liquidity_density:.1f} pd={self.premium_discount:.2f}"
            + (f" [{n}]" if n else "")
        )

    # ── Generic scaling ----------------------------------------------------

    def tighten(self, base: float, min_v: Optional[float] = None,
                max_v: Optional[float] = None) -> float:
        v = float(base) * self.selectivity
        if min_v is not None:
            v = max(float(min_v), v)
        if max_v is not None:
            v = min(float(max_v), v)
        return v

    def loosen(self, base: float, min_v: Optional[float] = None,
               max_v: Optional[float] = None) -> float:
        # Higher breathing_mult = wider structural breathing, lower false stops.
        v = float(base) * self.breathing_mult
        if min_v is not None:
            v = max(float(min_v), v)
        if max_v is not None:
            v = min(float(max_v), v)
        return v

    # ── Entry / conviction -------------------------------------------------

    def entry_score_threshold(self, base: float) -> float:
        return self.tighten(base, base * 0.90, base * 1.35)

    def evidence_gap(self, base: float) -> float:
        return self.tighten(base, base * 0.85, base * 1.45)

    def displacement_min(self, base: float) -> float:
        # Expanded / stress markets need more accepted displacement;
        # compressed markets can accept a lower raw displacement only if other
        # structure confirms.
        v = float(base)
        if self.volatility in ("expanded", "stress"):
            v *= 1.10
        elif self.volatility == "compressed":
            v *= 0.92
        if self.structure == "trend":
            v *= 1.05
        if self.regime in ("manipulation", "news_stress"):
            v *= 1.15
        return max(0.10, min(v, max(base * 1.60, 0.10)))

    def strong_displacement(self, base: float) -> float:
        v = float(base) * (1.06 if self.volatility in ("expanded", "stress") else 0.96)
        if self.liquidity_density > 5.0:
            v *= 1.06
        return max(self.displacement_min(base * 0.65), v)

    def chase_limit(self, base: float) -> float:
        # In fast delivery, chase distance can be slightly larger only when
        # structural proof exists. Raw use must still require CISD/OTE.
        v = float(base)
        if self.volatility in ("expanded", "stress"):
            v *= 0.85
        elif self.structure == "trend" and self.htf_alignment > 0.35:
            v *= 1.10
        return max(0.35, min(v, base * 1.25))

    def flow_opposition_threshold(self, base: float) -> float:
        v = float(base)
        if self.flow_abs > 0.55:
            v *= 0.85
        if self.volatility in ("expanded", "stress"):
            v *= 0.90
        return max(0.18, min(v, base))

    def conviction_score_floor(self, base: float) -> float:
        v = float(base) * self.selectivity
        return max(base * 0.92, min(v, 0.92))

    def product_core_floor(self, base: float) -> float:
        v = float(base)
        if self.volatility in ("expanded", "stress"):
            v *= 1.05
        if self.liquidity_density < 1.0:
            v *= 1.06
        return max(base * 0.95, min(v, 0.86))

    def pool_tf_rank_floor(self, base: int) -> int:
        # Dense liquidity and expanded volatility require higher quality pools.
        v = int(base)
        if self.volatility in ("expanded", "stress") and self.liquidity_density > 2.5:
            v = max(v, 3)
        return v

    def min_rr(self, base: float) -> float:
        # R:R is only an expectancy sanity check, not an entry timing source.
        # Keep it adaptive but bounded; do not use it for trailing.
        v = float(base)
        if self.spread_bps > 6.0:
            v += 0.10
        if self.volatility == "stress":
            v += 0.15
        return max(base, min(v, base + 0.35))

    def dealing_range_bounds(self, long_max: float, short_min: float) -> Tuple[float, float]:
        # In strong trend, permit slightly less perfect PD only with proof.
        width = 0.02 if self.structure == "trend" and self.htf_alignment > 0.45 else 0.0
        return min(0.70, long_max + width), max(0.30, short_min - width)

    # ── Liquidity / targets ------------------------------------------------

    def pool_significance_floor(self, base: float) -> float:
        v = float(base)
        if self.volatility in ("expanded", "stress"):
            v *= 1.08
        if self.liquidity_density > 4:
            v *= 1.06
        return max(0.1, v)

    def target_reach_penalty(self, distance_atr: float, tf: str = "") -> float:
        d = max(0.0, float(distance_atr))
        if d <= 0:
            return 1.0
        # Far targets are not vetoed; probability decays with regime context.
        horizon = 3.0
        tf_l = str(tf or "").lower()
        if tf_l in ("1h", "4h", "1d"):
            horizon *= 2.2
        if self.structure == "trend":
            horizon *= 1.35
        if self.volatility in ("expanded", "stress"):
            horizon *= 1.20
        return max(0.05, min(1.0, math.exp(-max(0.0, d - horizon) / max(horizon, 1e-9))))

    # ── Trailing / post-exit -----------------------------------------------

    def trail_breathing_atr(self, base: float) -> float:
        return self.loosen(base, base * 0.90, base * 1.70)

    def delivery_lock_min_mfe_atr(self, base: float) -> float:
        v = float(base)
        if self.volatility in ("expanded", "stress"):
            v *= 1.20
        elif self.volatility == "compressed":
            v *= 0.90
        if self.structure == "trend":
            v *= 1.10
        return max(0.8, min(v, base * 1.60))

    def post_exit_cooldown_mult(self) -> float:
        if self.volatility == "stress":
            return 1.8
        if self.regime == "manipulation":
            return 1.35
        if self.structure == "trend":
            return 0.90
        return 1.0

    def hunt_thresholds(self, on_base: float, off_base: float) -> Tuple[float, float]:
        on = float(on_base) * (1.08 if self.volatility in ("expanded", "stress") else 1.0)
        if self.liquidity_density < 1:
            on *= 1.08
        off = float(off_base) * (1.02 if self.volatility in ("expanded", "stress") else 0.98)
        return max(off + 0.02, min(on, on_base * 1.35)), max(0.02, min(off, on - 0.01))


def build_market_profile(
    *,
    price: float = 0.0,
    atr: float = 0.0,
    candles_by_tf: Optional[Dict[str, Any]] = None,
    candles_1m: Any = None,
    candles_5m: Any = None,
    liq_snapshot: Any = None,
    snap: Any = None,
    ict: Any = None,
    ict_engine: Any = None,
    flow: Any = None,
    orderbook: Optional[Dict[str, Any]] = None,
    side: str = "",
    session: str = "",
) -> MarketProfile:
    """Build a market profile from whatever context the caller has."""
    snap = snap if snap is not None else liq_snapshot
    ict = ict if ict is not None else ict_engine
    p = max(_safe_float(price, 0.0), 0.0)
    a = max(_safe_float(atr, 0.0), 1e-9)
    atr_pct = (a / p * 100.0) if p > 0 else 0.0

    # Volatility state from ATR% of instrument price. Bounds are intentionally
    # broad and then refined by structural context.
    if atr_pct <= _safe_float(_cfg("MI_COMPRESSED_ATR_PCT", 0.08), 0.08):
        vol = "compressed"
    elif atr_pct >= _safe_float(_cfg("MI_STRESS_ATR_PCT", 0.55), 0.55):
        vol = "stress"
    elif atr_pct >= _safe_float(_cfg("MI_EXPANDED_ATR_PCT", 0.28), 0.28):
        vol = "expanded"
    else:
        vol = "normal"

    # Session.
    sess = str(session or getattr(ict, "_session", "") or getattr(ict, "kill_zone", "") or "").upper()
    if not sess:
        sess = "UNKNOWN"

    # Structure and HTF alignment.
    s15 = str(getattr(ict, "structure_15m", "") or getattr(getattr(ict, "_tf_15m", None), "bias", "") or "").lower()
    s4h = str(getattr(ict, "structure_4h", "") or getattr(getattr(ict, "_tf_4h", None), "bias", "") or "").lower()
    side_sgn = _side_sign(side)
    htf = 0.0
    for s in (s15, s4h):
        if "bull" in s:
            htf += 0.5
        elif "bear" in s:
            htf -= 0.5
    if side_sgn:
        htf *= side_sgn

    if ("bull" in s15 and "bull" in s4h) or ("bear" in s15 and "bear" in s4h):
        structure = "trend"
    elif "range" in s15 or "range" in s4h or "ranging" in s15 or "ranging" in s4h:
        structure = "range"
    else:
        structure = "mixed"

    # AMD / regime.
    amd_phase = str(getattr(ict, "amd_phase", "") or getattr(getattr(ict, "_amd", None), "phase", "") or "").lower()
    if "manip" in amd_phase:
        regime = "manipulation"
    elif "distrib" in amd_phase or "redist" in amd_phase:
        regime = "delivery"
    elif "accum" in amd_phase or "reacc" in amd_phase:
        regime = "accumulation"
    elif vol == "stress":
        regime = "news_stress"
    else:
        regime = "balanced"

    # Liquidity density and quality.
    near = 0
    q_sum = 0.0
    for pool in _iter_pools(snap):
        px = _pool_price(pool)
        if px <= 0:
            continue
        d_atr = abs(px - p) / a if p > 0 else 999.0
        if d_atr <= _safe_float(_cfg("MI_LIQ_DENSITY_RADIUS_ATR", 4.0), 4.0):
            near += 1
            q_sum += _pool_sig(pool)
    liq_density = float(near)
    liq_quality = (q_sum / near) if near else 0.0

    # Order flow.
    flow_val = _safe_float(getattr(flow, "conviction", 0.0), 0.0)
    flow_abs = abs(flow_val)
    flow_side = "long" if flow_val > 0.05 else "short" if flow_val < -0.05 else "neutral"

    # Spread.
    spread_bps = 0.0
    try:
        bids = list((orderbook or {}).get("bids", []) or [])
        asks = list((orderbook or {}).get("asks", []) or [])
        def _px(lvl):
            if isinstance(lvl, dict):
                return _safe_float(lvl.get("limit_price") or lvl.get("price"), 0.0)
            if isinstance(lvl, (list, tuple)) and lvl:
                return _safe_float(lvl[0], 0.0)
            return 0.0
        bid = _px(bids[0]) if bids else 0.0
        ask = _px(asks[0]) if asks else 0.0
        mid = (bid + ask) / 2.0 if bid and ask else 0.0
        if mid > 0 and ask >= bid:
            spread_bps = (ask - bid) / mid * 10000.0
    except Exception:
        spread_bps = 0.0

    pd = _safe_float(getattr(ict, "dealing_range_pd", 0.5), 0.5)
    try:
        dr = getattr(ict, "_dealing_range", None)
        if dr is not None:
            pd = _safe_float(getattr(dr, "current_pd", pd), pd)
    except Exception:
        pass

    notes = []
    selectivity = 1.0
    breathing = 1.0

    if vol == "compressed":
        notes.append("compressed")
        selectivity *= 0.96
        breathing *= 0.95
    elif vol == "expanded":
        notes.append("expanded")
        selectivity *= 1.08
        breathing *= 1.18
    elif vol == "stress":
        notes.append("stress")
        selectivity *= 1.18
        breathing *= 1.35

    if regime == "manipulation":
        notes.append("manip")
        selectivity *= 1.10
        breathing *= 1.22
    elif regime == "delivery":
        notes.append("delivery")
        selectivity *= 1.02

    if structure == "trend":
        notes.append("trend")
        selectivity *= 1.04
        breathing *= 1.10
    elif structure == "range":
        notes.append("range")
        selectivity *= 1.08
        breathing *= 1.08

    if liq_density >= 5:
        notes.append("dense-liq")
        selectivity *= 1.06
        breathing *= 1.08
    elif liq_density <= 0:
        notes.append("thin-liq")
        selectivity *= 1.08

    if spread_bps > 0 and spread_bps >= _safe_float(_cfg("MI_WIDE_SPREAD_BPS", 5.0), 5.0):
        notes.append("wide-spread")
        selectivity *= 1.08
        breathing *= 1.08

    selectivity = max(0.90, min(selectivity, 1.45))
    breathing = max(0.85, min(breathing, 1.85))

    return MarketProfile(
        regime=regime,
        volatility=vol,
        structure=structure,
        session=sess,
        atr_pct=round(atr_pct, 5),
        liquidity_density=liq_density,
        liquidity_quality=round(liq_quality, 4),
        spread_bps=round(spread_bps, 4),
        flow_abs=round(flow_abs, 4),
        flow_side=flow_side,
        htf_alignment=round(htf, 4),
        premium_discount=round(pd, 4),
        selectivity=round(selectivity, 4),
        breathing_mult=round(breathing, 4),
        notes=tuple(notes),
    )

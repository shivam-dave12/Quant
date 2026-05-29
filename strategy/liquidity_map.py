"""Observable multi-timeframe liquidity pools for protected trade geometry.

The module derives candidate external/internal liquidity pools only from closed
venue-local candles.  It does not invent liquidity from the current midpoint.
Pools remain product/venue local and are used for SL invalidation and TP
objective selection, not as an entry signal by themselves.
"""
from __future__ import annotations

import math
from typing import Any, Mapping


def _num(value: Any, default: float = 0.0) -> float:
    try:
        value = float(value)
        return value if math.isfinite(value) else default
    except Exception:
        return default


def _high(row: Mapping[str, Any]) -> float:
    return _num(row.get("high", row.get("h", 0.0)))


def _low(row: Mapping[str, Any]) -> float:
    return _num(row.get("low", row.get("l", 0.0)))


_TIMEFRAME_WEIGHT = {"1m": 0.75, "5m": 1.50, "15m": 2.50, "1h": 4.00, "4h": 6.00}


def _raw_pools(rows: list[dict[str, Any]], timeframe: str) -> list[dict[str, Any]]:
    """Return still-unswept swing and external-range pools from closed bars."""
    if len(rows) < 5:
        return []
    weight = _TIMEFRAME_WEIGHT.get(timeframe, 1.0)
    recent = rows[-min(len(rows), 100):]
    pools: list[dict[str, Any]] = []
    # Pivot liquidity: a swing must have two closed bars on either side.
    for idx in range(2, len(recent) - 2):
        h = _high(recent[idx]); l = _low(recent[idx])
        before = recent[idx - 2:idx]; after = recent[idx + 1: idx + 3]
        subsequent = recent[idx + 1:]
        if h > 0 and h >= max((_high(x) for x in before + after), default=0.0):
            swept = max((_high(x) for x in subsequent), default=0.0) > h * (1.0 + 1e-9)
            if not swept:
                pools.append({"side": "BSL", "price": h, "timeframe": timeframe, "source": "closed_swing_high", "strength": weight, "swept": False})
        if l > 0 and l <= min((_low(x) for x in before + after if _low(x) > 0), default=float("inf")):
            swept = min((_low(x) for x in subsequent if _low(x) > 0), default=float("inf")) < l * (1.0 - 1e-9)
            if not swept:
                pools.append({"side": "SSL", "price": l, "timeframe": timeframe, "source": "closed_swing_low", "strength": weight, "swept": False})
    # External range endpoints are higher-authority anchors even when not pivots.
    window = recent[-min(len(recent), 20):]
    external_high = max((_high(x) for x in window), default=0.0)
    external_low = min((_low(x) for x in window if _low(x) > 0), default=0.0)
    if external_high > 0:
        pools.append({"side": "BSL", "price": external_high, "timeframe": timeframe, "source": "closed_external_range_high", "strength": weight * 1.35, "swept": False})
    if external_low > 0:
        pools.append({"side": "SSL", "price": external_low, "timeframe": timeframe, "source": "closed_external_range_low", "strength": weight * 1.35, "swept": False})
    return pools


def build_liquidity_pools(
    closed_candles_by_timeframe: Mapping[str, list[dict[str, Any]]],
    *,
    reference_price: float,
    merge_bps: float = 2.0,
) -> list[dict[str, Any]]:
    """Build clustered unswept pools across closed timeframes.

    Pools within ``merge_bps`` on the same side are merged into a single price
    weighted by their structural strength.  Higher-timeframe confluence therefore
    outranks an isolated one-minute candle extreme.
    """
    ref = max(_num(reference_price), 1e-9)
    raw: list[dict[str, Any]] = []
    for timeframe, rows in closed_candles_by_timeframe.items():
        raw.extend(_raw_pools(list(rows or []), str(timeframe)))
    clusters: list[dict[str, Any]] = []
    for pool in sorted(raw, key=lambda p: (p["side"], float(p["price"]))):
        matched = None
        for cluster in clusters:
            if cluster["side"] != pool["side"]:
                continue
            if abs(float(cluster["price"]) - float(pool["price"])) / ref * 10_000.0 <= float(merge_bps):
                matched = cluster
                break
        if matched is None:
            clusters.append({
                "side": pool["side"], "price": float(pool["price"]), "strength": float(pool["strength"]),
                "timeframes": [pool["timeframe"]], "sources": [pool["source"]], "swept": False,
            })
        else:
            total = float(matched["strength"]) + float(pool["strength"])
            matched["price"] = (float(matched["price"]) * float(matched["strength"]) + float(pool["price"]) * float(pool["strength"])) / total
            matched["strength"] = total
            if pool["timeframe"] not in matched["timeframes"]:
                matched["timeframes"].append(pool["timeframe"])
            if pool["source"] not in matched["sources"]:
                matched["sources"].append(pool["source"])
    rank = {tf: index for index, tf in enumerate(("1m", "5m", "15m", "1h", "4h"))}
    for cluster in clusters:
        cluster["max_timeframe"] = max(cluster["timeframes"], key=lambda tf: rank.get(tf, -1))
        cluster["distance_bps"] = abs(float(cluster["price"]) / ref - 1.0) * 10_000.0
    return sorted(clusters, key=lambda p: (p["side"], float(p["distance_bps"]), -float(p["strength"])))


def select_protection_pools(
    pools: list[dict[str, Any]], *, direction: str, entry_price: float, min_objective_distance: float
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Select invalidation and adequate objective pools for a planned direction."""
    entry = float(entry_price)
    long_side = str(direction).upper() in {"LONG", "BULLISH", "BUY"}
    stop_side = "SSL" if long_side else "BSL"
    tp_side = "BSL" if long_side else "SSL"
    stop_candidates = [p for p in pools if p.get("side") == stop_side and ((float(p["price"]) < entry) if long_side else (float(p["price"]) > entry))]
    objective_candidates = [p for p in pools if p.get("side") == tp_side and ((float(p["price"]) > entry) if long_side else (float(p["price"]) < entry))]
    # Prefer a meaningful, nearest confluence invalidation rather than a lone tiny 1m wick.
    meaningful = [p for p in stop_candidates if float(p.get("strength", 0.0)) >= _TIMEFRAME_WEIGHT["5m"]]
    stop_pool = min(meaningful or stop_candidates, key=lambda p: abs(float(p["price"]) - entry), default=None)
    adequate = [p for p in objective_candidates if abs(float(p["price"]) - entry) >= float(min_objective_distance)]
    # Select the nearest adequate target; stronger clustered pools win ties.
    objective_pool = min(adequate, key=lambda p: (abs(float(p["price"]) - entry), -float(p.get("strength", 0.0))), default=None)
    return stop_pool, objective_pool


"""BTC-only institutional overlay for the v75 rollback line.

The user asked to keep every non-BTC ticker untouched and make BTC follow the
attached v58 strategy behavior with institutional TP/SL math.  This helper is
therefore intentionally isolated and only becomes active when the current
multi-asset instrument scope or strategy owner is BTC.
"""
from __future__ import annotations
from typing import Any
import math


def _norm(v: Any) -> str:
    return "".join(ch for ch in str(v or "").upper() if ch.isalnum())


def current_asset_id(default: str = "") -> str:
    try:
        from core.instruments import current_instrument
        inst = current_instrument()
        asset = getattr(inst, "asset_id", "") if inst is not None else ""
        if asset:
            return _norm(asset)
    except Exception:
        pass
    return _norm(default)


def is_btc_context(owner: Any = None) -> bool:
    try:
        inst = getattr(owner, "_instrument", None)
        asset = _norm(getattr(inst, "asset_id", "") or getattr(owner, "_asset_id", ""))
        if asset:
            return asset == "BTC"
    except Exception:
        pass
    asset = current_asset_id()
    return asset in ("BTC", "BTCUSD", "BTCUSDT", "XBTUSD")


def btc_static_rr_floor(static_min_rr: float, posterior_prob: float = 0.0) -> float:
    base = max(0.01, float(static_min_rr or 0.0))
    p = max(0.0, min(0.95, float(posterior_prob or 0.0)))
    if p <= 0.0:
        return min(base, 1.35)
    ev_floor = ((1.0 - p) / max(p, 1e-9)) + 0.35
    return max(0.90, min(base, ev_floor, 1.35))


def btc_durable_rr_floor(default_floor: float, be_move: float, risk: float) -> float:
    risk = max(float(risk or 0.0), 1e-9)
    be_r = max(float(be_move or 0.0), 0.0) / risk
    return max(0.85, min(float(default_floor or 1.35), 1.25), 1.20 * be_r)


def btc_sl_buffer_limits(max_buffer_atr: float) -> float:
    return max(0.45, min(float(max_buffer_atr or 2.0), 1.15))


def finite_price(x: Any) -> float:
    try:
        f = float(x)
        return f if math.isfinite(f) else 0.0
    except Exception:
        return 0.0

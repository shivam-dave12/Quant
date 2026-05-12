"""Institutional ICICI option-chain architecture.

The Indian options desk owns an *underlying thesis* first.  Individual option
contracts are execution vehicles selected after the underlying chart produces a
bullish/bearish thesis.  This module therefore keeps NIFTY/BANKNIFTY/SENSEX or
large-cap stocks as desk assets, while retaining the complete viable option
chain for strike/expiry selection.
"""
from __future__ import annotations

import math
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Optional

try:
    import config
except Exception:  # pragma: no cover
    config = None  # type: ignore

from core.instruments import normalise_symbol
from fund.types import clamp, safe_float
from .indian_options_desk import BlackScholesModel


def _cfg(name: str, default: Any) -> Any:
    return getattr(config, name, default) if config is not None else default


def _right(raw: Mapping[str, Any]) -> str:
    v = normalise_symbol(raw.get("right") or raw.get("option_type") or raw.get("OptionType") or raw.get("Right") or raw.get("CallPut") or "")
    if v in {"C", "CE", "CALL"}:
        return "call"
    if v in {"P", "PE", "PUT"}:
        return "put"
    return ""


def _expiry_dt(value: Any) -> Optional[datetime]:
    txt = str(value or "").strip()
    if not txt:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d", "%d-%b-%Y", "%d-%B-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(txt, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            pass
    return None


def _dte(raw: Mapping[str, Any]) -> float:
    dt = _expiry_dt(raw.get("expiry_date") or raw.get("ExpiryDate") or raw.get("expiry") or raw.get("Expiry"))
    return max(0.0, (dt.timestamp() - time.time()) / 86400.0) if dt else 0.0


def _strike(raw: Mapping[str, Any]) -> float:
    return safe_float(raw.get("strike_price") or raw.get("StrikePrice") or raw.get("strike") or raw.get("Strike"), 0.0)


@dataclass(frozen=True)
class ICICIContractChoice:
    score: float
    underlying: str
    thesis_side: str
    selected_symbol: str
    right: str
    strike: float
    expiry: str
    dte: float
    delta: float
    theta_to_premium: float
    moneyness: float
    reasons: tuple[str, ...]
    raw: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["raw"] = dict(self.raw)
        return d


def chain_quality(chain: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    rows = [dict(x) for x in chain]
    strikes = {_strike(r) for r in rows if _strike(r) > 0}
    expiries = {str(r.get("expiry_date") or r.get("ExpiryDate") or r.get("expiry") or "") for r in rows if str(r.get("expiry_date") or r.get("ExpiryDate") or r.get("expiry") or "")}
    rights = {_right(r) for r in rows if _right(r)}
    dtes = [_dte(r) for r in rows if _dte(r) > 0]
    nearest = min(dtes) if dtes else 9999.0
    q = (
        0.30 * min(1.0, len(rows) / 160.0)
        + 0.22 * min(1.0, len(expiries) / 6.0)
        + 0.20 * (1.0 if {"call", "put"}.issubset(rights) else 0.35)
        + 0.20 * min(1.0, len(strikes) / 40.0)
        + 0.08 * max(0.0, 1.0 - nearest / 35.0)
    )
    return {
        "rows": len(rows),
        "strikes": len(strikes),
        "expiries": len(expiries),
        "has_call_put": {"call", "put"}.issubset(rights),
        "nearest_dte": nearest if nearest < 9999 else 0.0,
        "score": q,
    }


def build_underlying_payload(underlying: str, desk_id: str, rows: list[Any]) -> dict[str, Any]:
    chain_raw = [dict(getattr(r, "raw", {}) or {}) for r in rows]
    quality = chain_quality(chain_raw)
    sample = chain_raw[0] if chain_raw else {}
    return {
        "icici_underlying_desk": True,
        "contract_selector_mode": "post_thesis",
        "underlying": normalise_symbol(underlying),
        "stock_code": normalise_symbol(underlying),
        "desk_id": desk_id,
        "exchange_code": "BFO" if desk_id == "ICICI_INDEX_OPTIONS" and normalise_symbol(sample.get("exchange_code")) == "BFO" else "NFO",
        "product_type": "option_chain",
        "chain_quality": quality,
        "chain_candidates": chain_raw,
        "selected_option_contract": None,
    }


def is_chain_instrument(instrument: Any) -> bool:
    raw = getattr(getattr(instrument, "primary", None), "raw", {}) or {}
    return bool(
        raw.get("icici_underlying_desk")
        and str(raw.get("contract_selector_mode") or "").lower() == "post_thesis"
    )


def select_contract_for_thesis(
    instrument: Any,
    thesis_side: str,
    *,
    underlying_spot: float = 0.0,
    option_quote_by_symbol: Optional[Mapping[str, Mapping[str, Any]]] = None,
) -> Optional[ICICIContractChoice]:
    """Select the option contract after the underlying thesis is known.

    bullish thesis -> buy call; bearish thesis -> buy put.  Selection balances
    DTE, strike proximity, Black-Scholes Greeks and theta decay.  No option is
    shorted and no hardcoded underlying list is used.
    """
    raw = getattr(getattr(instrument, "primary", None), "raw", {}) or {}
    chain = [dict(x) for x in (raw.get("chain_candidates") or []) if isinstance(x, Mapping)]
    if not chain:
        return None
    side = str(thesis_side or "").lower()
    desired = "call" if side == "long" else "put" if side == "short" else ""
    if not desired:
        return None
    spot = float(underlying_spot or safe_float(raw.get("underlying_spot_price") or raw.get("spot_price"), 0.0))
    min_dte = float(_cfg("ICICI_OPTION_MIN_DTE", 2.0))
    max_dte = float(_cfg("ICICI_OPTION_MAX_DTE", 21.0))
    target_delta = float(_cfg("ICICI_INDEX_OPTION_TARGET_ABS_DELTA", 0.45) if raw.get("desk_id") == "ICICI_INDEX_OPTIONS" else _cfg("ICICI_STOCK_OPTION_TARGET_ABS_DELTA", 0.50))
    delta_band = float(_cfg("ICICI_OPTION_DELTA_BAND", 0.22))
    max_theta = float(_cfg("ICICI_OPTION_MAX_THETA_TO_PREMIUM", 0.08))
    iv = float(_cfg("ICICI_OPTION_IV_STRESS_PRIOR", 0.24))
    rate = float(_cfg("INDIA_RISK_FREE_RATE", 0.065))
    choices: list[ICICIContractChoice] = []
    quotes = option_quote_by_symbol or {}
    for c in chain:
        if _right(c) != desired:
            continue
        dte = _dte(c)
        strike = _strike(c)
        if strike <= 0 or dte < min_dte or dte > max_dte:
            continue
        symbol = normalise_symbol(c.get("TradingSymbol") or c.get("trading_symbol") or c.get("symbol") or f"{raw.get('stock_code')}_{c.get('expiry_date')}_{strike}_{desired[:1].upper()}")
        q = dict(quotes.get(symbol, {}) or {})
        prem = safe_float(q.get("ltp") or q.get("last_price") or q.get("close") or q.get("price"), 0.0)
        local_spot = safe_float(q.get("underlying_spot_price") or q.get("underlying_ltp"), 0.0) or spot
        if local_spot <= 0:
            # before live quote/underlying warmup, use strike-ladder proximity
            # instead of fabricating a price.  This keeps the candidate eligible
            # for later quote validation but discounts the score.
            local_spot = strike
        bs = BlackScholesModel.greeks(desired, local_spot, strike, dte, rate, iv, premium=prem)
        if bs:
            delta_score = clamp(1.0 - abs(abs(bs.delta) - target_delta) / max(delta_band, 1e-6))
            theta_score = clamp(1.0 - bs.theta_to_premium / max_theta)
            moneyness_score = clamp(1.0 - abs(bs.moneyness - 1.0) / 0.10)
            bs_score = 0.42 * delta_score + 0.36 * theta_score + 0.22 * moneyness_score
            delta = bs.delta; theta = bs.theta_to_premium; mon = bs.moneyness
        else:
            prox = clamp(1.0 - abs(local_spot - strike) / max(abs(local_spot) * 0.12, 1.0))
            bs_score = 0.35 * prox
            delta = 0.0; theta = 0.0; mon = local_spot / strike if strike else 0.0
        dte_mid = (min_dte + max_dte) / 2.0
        dte_score = clamp(1.0 - abs(dte - dte_mid) / max(1.0, max_dte - min_dte))
        live_score = 1.0 if q else 0.55
        score = clamp(0.50 * bs_score + 0.25 * dte_score + 0.25 * live_score)
        reasons = [f"thesis={side}", f"buy_{desired}", f"dte={dte:.1f}", f"strike={strike:g}"]
        if q:
            reasons.append("live_quote")
        if bs:
            reasons.extend([f"delta={delta:+.2f}", f"theta/prem={theta:.2%}"])
        choices.append(ICICIContractChoice(score, normalise_symbol(raw.get("stock_code") or raw.get("underlying") or ""), side, symbol, desired, strike, str(c.get("expiry_date") or c.get("ExpiryDate") or ""), dte, delta, theta, mon, tuple(reasons), c))
    choices.sort(key=lambda x: x.score, reverse=True)
    return choices[0] if choices else None


def apply_contract_choice(instrument: Any, choice: ICICIContractChoice) -> None:
    """Mutate the instrument raw payload so existing Breeze order adapter routes
    the exact selected option contract.  Dataclasses are frozen, but the raw dict
    is intentionally mutable runtime metadata.
    """
    raw = getattr(getattr(instrument, "primary", None), "raw", None)
    if not isinstance(raw, dict):
        return
    c = dict(choice.raw)
    raw["selected_option_contract"] = choice.as_dict()
    raw["stock_code"] = c.get("stock_code") or c.get("StockCode") or c.get("underlying") or raw.get("stock_code")
    raw["exchange_code"] = c.get("exchange_code") or c.get("ExchangeCode") or raw.get("exchange_code") or "NFO"
    raw["product_type"] = "options"
    raw["right"] = "Call" if choice.right == "call" else "Put"
    raw["option_type"] = raw["right"]
    raw["strike_price"] = str(choice.strike)
    raw["expiry_date"] = choice.expiry
    raw["TradingSymbol"] = choice.selected_symbol

"""Institutional Indian options selection and Black-Scholes diagnostics.

This module never fabricates prices or Greeks. When Breeze does not provide a
quote/chain row, the option receives a low/coverage score instead of synthetic
market data. Greeks are computed only from actual strike/expiry/underlying/option
price inputs and are used as risk diagnostics for theta, gamma and moneyness.
"""
from __future__ import annotations

import math
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Mapping, Optional

try:
    import config
except Exception:  # pragma: no cover
    config = None  # type: ignore

from core.instruments import ExchangeInstrument, TradableInstrument, normalise_symbol
from fund.types import clamp, safe_float


def _cfg(name: str, default: Any) -> Any:
    if config is None:
        return default
    return getattr(config, name, default)


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def _parse_expiry(value: Any) -> Optional[datetime]:
    txt = str(value or "").strip()
    if not txt:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d", "%d-%b-%Y", "%d-%B-%Y", "%d/%m/%Y"):
        try:
            dt = datetime.strptime(txt, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None


def _right(raw: Mapping[str, Any]) -> str:
    v = str(raw.get("right") or raw.get("OptionType") or raw.get("option_type") or raw.get("Right") or raw.get("CallPut") or "").lower()
    if v in {"c", "ce", "call"}:
        return "call"
    if v in {"p", "pe", "put"}:
        return "put"
    return v


@dataclass(frozen=True)
class BlackScholesSnapshot:
    option_type: str
    spot: float
    strike: float
    dte: float
    rate: float
    volatility: float
    theoretical_price: float
    delta: float
    gamma: float
    theta_per_day: float
    vega_per_vol_point: float
    intrinsic: float
    extrinsic: float
    moneyness: float
    theta_to_premium: float

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class OptionSelectionScore:
    score: float
    desk_id: str
    underlying: str
    option_type: str
    strike: float
    expiry: str
    dte: float
    bs: Optional[BlackScholesSnapshot]
    reasons: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["bs"] = self.bs.as_dict() if self.bs else None
        return d


class BlackScholesModel:
    @staticmethod
    def greeks(option_type: str, spot: float, strike: float, dte: float, rate: float, volatility: float, premium: float = 0.0) -> Optional[BlackScholesSnapshot]:
        s = float(spot or 0.0); k = float(strike or 0.0); days = float(dte or 0.0)
        sigma = float(volatility or 0.0); r = float(rate or 0.0)
        if s <= 0 or k <= 0 or days <= 0 or sigma <= 0:
            return None
        t = max(days / 365.0, 1e-6)
        d1 = (math.log(s / k) + (r + 0.5 * sigma * sigma) * t) / (sigma * math.sqrt(t))
        d2 = d1 - sigma * math.sqrt(t)
        if option_type == "put":
            theo = k * math.exp(-r * t) * _norm_cdf(-d2) - s * _norm_cdf(-d1)
            delta = _norm_cdf(d1) - 1.0
            theta = (-(s * _norm_pdf(d1) * sigma) / (2.0 * math.sqrt(t)) + r * k * math.exp(-r * t) * _norm_cdf(-d2)) / 365.0
            intrinsic = max(0.0, k - s)
        else:
            theo = s * _norm_cdf(d1) - k * math.exp(-r * t) * _norm_cdf(d2)
            delta = _norm_cdf(d1)
            theta = (-(s * _norm_pdf(d1) * sigma) / (2.0 * math.sqrt(t)) - r * k * math.exp(-r * t) * _norm_cdf(d2)) / 365.0
            intrinsic = max(0.0, s - k)
        gamma = _norm_pdf(d1) / (s * sigma * math.sqrt(t))
        vega = s * _norm_pdf(d1) * math.sqrt(t) / 100.0
        used_premium = premium if premium > 0 else theo
        extrinsic = max(0.0, used_premium - intrinsic)
        return BlackScholesSnapshot(
            option_type=option_type,
            spot=s,
            strike=k,
            dte=days,
            rate=r,
            volatility=sigma,
            theoretical_price=max(0.0, theo),
            delta=delta,
            gamma=gamma,
            theta_per_day=theta,
            vega_per_vol_point=vega,
            intrinsic=intrinsic,
            extrinsic=extrinsic,
            moneyness=s / k if k > 0 else 0.0,
            theta_to_premium=abs(theta) / used_premium if used_premium > 0 else 1.0,
        )


class IndianOptionsDesk:
    INDEX_UNDERLYINGS = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"}

    def desk_id_for_underlying(self, underlying: str) -> str:
        return "ICICI_INDEX_OPTIONS" if normalise_symbol(underlying) in self.INDEX_UNDERLYINGS else "ICICI_STOCK_OPTIONS"

    def score_option(self, inst: TradableInstrument, quote: Optional[Mapping[str, Any]] = None) -> OptionSelectionScore:
        raw = dict(getattr(getattr(inst, "primary", None), "raw", {}) or {})
        if quote:
            raw.update(dict(quote))
        underlying = normalise_symbol(raw.get("stock_code") or raw.get("underlying") or raw.get("ShortName") or getattr(inst, "asset_id", ""))
        right = _right(raw)
        strike = safe_float(raw.get("strike_price") or raw.get("StrikePrice") or raw.get("strike") or raw.get("Strike"), 0.0)
        expiry_raw = raw.get("expiry_date") or raw.get("ExpiryDate") or raw.get("expiry") or raw.get("Expiry") or ""
        expiry_dt = _parse_expiry(expiry_raw)
        dte = ((expiry_dt.timestamp() - time.time()) / 86400.0) if expiry_dt else 0.0
        spot = self._first_float(raw, ("underlying_spot_price", "spot_price", "spotPrice", "ltp_underlying", "underlying_ltp", "underlying_price"))
        option_px = self._first_float(raw, ("ltp", "last_price", "lastPrice", "close", "best_bid", "bid", "price"))
        bid = self._first_float(raw, ("best_bid", "bid", "bid_price"))
        ask = self._first_float(raw, ("best_ask", "ask", "ask_price"))
        mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else option_px
        if mid > 0:
            option_px = mid
        iv = self._first_float(raw, ("iv", "implied_volatility", "impliedVolatility"))
        if iv > 3.0:
            iv = iv / 100.0
        # Do not invent IV. If absent, use configurable desk prior only as a risk
        # stress input, and mark the reason so it cannot be mistaken for live IV.
        iv_is_live = iv > 0
        if iv <= 0:
            iv = float(_cfg("ICICI_OPTION_IV_STRESS_PRIOR", 0.24))
        rate = float(_cfg("INDIA_RISK_FREE_RATE", 0.065))
        bs = BlackScholesModel.greeks(right or "call", spot, strike, dte, rate, iv, premium=option_px)

        min_dte = float(_cfg("ICICI_OPTION_MIN_DTE", 2.0))
        max_dte = float(_cfg("ICICI_OPTION_MAX_DTE", 21.0))
        target_delta = float(_cfg("ICICI_INDEX_OPTION_TARGET_ABS_DELTA", 0.45 if self.desk_id_for_underlying(underlying) == "ICICI_INDEX_OPTIONS" else 0.50))
        delta_band = float(_cfg("ICICI_OPTION_DELTA_BAND", 0.22))
        max_theta_premium = float(_cfg("ICICI_OPTION_MAX_THETA_TO_PREMIUM", 0.08))
        max_spread_bps = float(_cfg("ICICI_OPTION_MAX_SPREAD_BPS", 180.0))

        dte_score = clamp(1.0 - abs(((min_dte + max_dte) / 2.0) - dte) / max(1.0, (max_dte - min_dte))) if dte > 0 else 0.0
        spread_bps = ((ask - bid) / mid * 10000.0) if ask > 0 and bid > 0 and mid > 0 else 0.0
        spread_score = 0.55 if spread_bps <= 0 else clamp(1.0 - spread_bps / max_spread_bps)
        bs_score = 0.0
        theta_score = 0.0
        delta_score = 0.0
        if bs:
            abs_delta = abs(bs.delta)
            delta_score = clamp(1.0 - abs(abs_delta - target_delta) / max(delta_band, 1e-6))
            theta_score = clamp(1.0 - bs.theta_to_premium / max_theta_premium)
            # Avoid deep OTM lottery and deep ITM capital lock; prefer tradable ATM/near-ATM alpha.
            moneyness_score = clamp(1.0 - abs(bs.moneyness - 1.0) / 0.08)
            bs_score = 0.42 * delta_score + 0.36 * theta_score + 0.22 * moneyness_score
        live_score = 1.0 if quote else 0.45
        score = clamp(0.35 * bs_score + 0.25 * dte_score + 0.20 * spread_score + 0.20 * live_score)

        reasons: list[str] = [self.desk_id_for_underlying(underlying).lower()]
        if dte <= 0:
            reasons.append("expiry_unknown")
        elif dte < min_dte:
            reasons.append("theta_decay_too_close")
        elif dte > max_dte:
            reasons.append("dte_too_far")
        else:
            reasons.append(f"dte={dte:.1f}")
        if bs:
            reasons.append(f"delta={bs.delta:+.2f}")
            reasons.append(f"theta/prem={bs.theta_to_premium:.2%}")
        if not iv_is_live:
            reasons.append("iv_stress_prior")
        if spread_bps > max_spread_bps:
            reasons.append("wide_option_spread")
        if quote:
            reasons.append("breeze_quote")

        return OptionSelectionScore(
            score=score,
            desk_id=self.desk_id_for_underlying(underlying),
            underlying=underlying,
            option_type=right,
            strike=strike,
            expiry=str(expiry_raw or ""),
            dte=max(0.0, dte),
            bs=bs,
            reasons=tuple(reasons),
        )

    @staticmethod
    def _first_float(row: Mapping[str, Any], names: tuple[str, ...]) -> float:
        wanted = {x.lower() for x in names}
        for k, v in row.items():
            if str(k).lower() in wanted:
                f = safe_float(v, 0.0)
                if f > 0:
                    return f
        return 0.0

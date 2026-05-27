"""Institutional Indian options selection and Black-Scholes diagnostics.

This module never fabricates prices or Greeks. When Groww does not provide a
quote/chain row, the option receives a low/coverage score instead of synthetic
market data. Greeks are computed only from actual strike/expiry/underlying/option
price inputs and are used as risk diagnostics for theta, gamma and moneyness.
"""
from __future__ import annotations

import math
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Optional, Sequence

try:
    import config
except Exception:  # pragma: no cover
    config = None  # type: ignore

from core.instruments import ExchangeInstrument, TradableInstrument, normalise_symbol


def clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    try:
        v = float(value)
    except Exception:
        v = low
    return max(low, min(high, v))


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        v = float(value)
        if math.isfinite(v):
            return v
    except Exception:
        pass
    return default


def _cfg(name: str, default: Any) -> Any:
    if config is None:
        return default
    return getattr(config, name, default)


def _csv_symbols(raw: Any) -> set[str]:
    if isinstance(raw, str):
        vals = raw.replace(";", ",").split(",")
    elif raw is None:
        vals = []
    else:
        vals = list(raw)
    return {normalise_symbol(str(x)) for x in vals if normalise_symbol(str(x))}


def _structural_tokens(raw: Mapping[str, Any]) -> set[str]:
    """Tokens that describe venue/product structure, not a tradable thesis.

    This is intentionally not an approved-symbol list. It is a data-integrity
    check: an option underlying cannot be the exchange segment, product type,
    option right, strike, or expiry token from the same security-master row.
    """
    vals: set[str] = set()
    for name in (
        "exchange_code", "ExchangeCode", "Exchange", "exchange",
        "segment", "Segment", "product_type", "ProductType", "Product",
        "InstrumentType", "InstrumentName", "Series", "right", "Right",
        "option_type", "OptionType", "CallPut", "strike_price", "StrikePrice",
        "Strike", "expiry_date", "ExpiryDate", "Expiry",
    ):
        v = raw.get(name)
        n = normalise_symbol(str(v or ""))
        if n:
            vals.add(n)
    vals.update({"OPTION", "OPTIONS", "CALL", "PUT", "CE", "PE", "C", "P"})
    return vals


def _is_index_option_row(raw: Mapping[str, Any]) -> bool:
    text = normalise_symbol(" ".join(str(raw.get(k, "")) for k in (
        "product_type", "ProductType", "InstrumentType", "InstrumentName",
        "Series", "segment", "Segment", "exchange_code", "ExchangeCode",
        "TradingSymbol", "Symbol", "SecurityName", "Security Name",
    )))
    if "OPTIDX" in text or "INDEXOPTION" in text or "INDEXOPTIONS" in text:
        return True
    if "OPTSTK" in text or "STOCKOPTION" in text or "STOCKOPTIONS" in text:
        return False
    # BFO listed options are index derivatives in the GROWW master; NFO can be
    # index or stock and should be resolved by product/instrument metadata.
    return "BFO" in text


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
    def price(option_type: str, spot: float, strike: float, dte: float, rate: float, volatility: float) -> Optional[float]:
        s = float(spot or 0.0); k = float(strike or 0.0); days = float(dte or 0.0)
        sigma = float(volatility or 0.0); r = float(rate or 0.0)
        if s <= 0 or k <= 0 or days <= 0 or sigma <= 0:
            return None
        t = max(days / 365.0, 1e-6)
        d1 = (math.log(s / k) + (r + 0.5 * sigma * sigma) * t) / (sigma * math.sqrt(t))
        d2 = d1 - sigma * math.sqrt(t)
        if str(option_type or "").lower() == "put":
            theo = k * math.exp(-r * t) * _norm_cdf(-d2) - s * _norm_cdf(-d1)
        else:
            theo = s * _norm_cdf(d1) - k * math.exp(-r * t) * _norm_cdf(d2)
        return max(0.0, theo)

    @staticmethod
    def implied_volatility(
        option_type: str,
        spot: float,
        strike: float,
        dte: float,
        rate: float,
        premium: float,
        *,
        min_vol: float = 0.03,
        max_vol: float = 1.50,
        tolerance: float = 1e-4,
        iterations: int = 80,
    ) -> Optional[float]:
        s = float(spot or 0.0); k = float(strike or 0.0); days = float(dte or 0.0)
        target = float(premium or 0.0)
        if s <= 0 or k <= 0 or days <= 0 or target <= 0:
            return None
        option_type = str(option_type or "").lower()
        intrinsic = max(0.0, k - s) if option_type == "put" else max(0.0, s - k)
        if target + tolerance < intrinsic:
            return None
        low = max(1e-4, float(min_vol or 0.03))
        high = max(low * 2.0, float(max_vol or 1.50))
        high_price = BlackScholesModel.price(option_type, s, k, days, rate, high)
        if high_price is None or high_price < target:
            return None
        for _ in range(max(1, int(iterations))):
            mid = (low + high) / 2.0
            mid_price = BlackScholesModel.price(option_type, s, k, days, rate, mid)
            if mid_price is None:
                return None
            if abs(mid_price - target) <= tolerance:
                return mid
            if mid_price < target:
                low = mid
            else:
                high = mid
        return (low + high) / 2.0

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
    def desk_id_for_row(self, raw: Mapping[str, Any]) -> str:
        return "GROWW_INDEX_OPTIONS" if _is_index_option_row(raw) else "GROWW_STOCK_OPTIONS"

    def desk_id_for_underlying(self, underlying: str, raw: Optional[Mapping[str, Any]] = None) -> str:
        return self.desk_id_for_row(raw or {})

    def score_option(self, inst: TradableInstrument, quote: Optional[Mapping[str, Any]] = None) -> OptionSelectionScore:
        raw = dict(getattr(getattr(inst, "primary", None), "raw", {}) or {})
        if quote:
            raw.update(dict(quote))
        underlying = normalise_symbol(raw.get("stock_code") or raw.get("underlying") or raw.get("Underlying") or raw.get("UnderlyingSymbol") or getattr(inst, "asset_id", ""))
        if raw.get("groww_underlying_desk"):
            quality = raw.get("chain_quality") if isinstance(raw.get("chain_quality"), dict) else {}
            q = float(quality.get("score", raw.get("underlying_chain_quality_score", 0.0)) or 0.0)
            rows = int(quality.get("rows", len(raw.get("chain_candidates") or [])) or 0)
            strikes = int(quality.get("strikes", 0) or 0)
            expiries = int(quality.get("expiries", 0) or 0)
            desk_id = str(raw.get("desk_id") or self.desk_id_for_underlying(underlying, raw))
            reasons = (
                desk_id.lower(),
                "underlying_first",
                "daily_ce_pe_book_preselected_signal_activates_side",
                f"underlying={underlying}",
                f"chain_rows={rows}",
                f"strikes={strikes}",
                f"expiries={expiries}",
            )
            return OptionSelectionScore(
                score=clamp(0.35 + 0.65 * q),
                desk_id=desk_id,
                underlying=underlying,
                option_type="chain",
                strike=0.0,
                expiry="session_preselected",
                dte=0.0,
                bs=None,
                reasons=reasons,
            )
        structural = _structural_tokens(raw)
        if bool(_cfg("GROWW_OPTION_REJECT_STRUCTURAL_UNDERLYING", True)) and (not underlying or underlying in structural):
            return OptionSelectionScore(
                score=0.0,
                desk_id="GROWW_REJECT_CORRUPT_OPTION",
                underlying=underlying,
                option_type=_right(raw),
                strike=safe_float(raw.get("strike_price") or raw.get("StrikePrice") or raw.get("strike") or raw.get("Strike"), 0.0),
                expiry=str(raw.get("expiry_date") or raw.get("ExpiryDate") or raw.get("expiry") or raw.get("Expiry") or ""),
                dte=0.0,
                bs=None,
                reasons=("structural_underlying", f"underlying={underlying or 'missing'}"),
            )
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
            iv = float(_cfg("GROWW_OPTION_IV_STRESS_PRIOR", 0.24))
        rate = float(_cfg("INDIA_RISK_FREE_RATE", 0.065))
        bs = BlackScholesModel.greeks(right or "call", spot, strike, dte, rate, iv, premium=option_px)

        min_dte = float(_cfg("GROWW_OPTION_MIN_DTE", 2.0))
        max_dte = float(_cfg("GROWW_OPTION_MAX_DTE", 21.0))
        desk_id = self.desk_id_for_underlying(underlying, raw)
        target_delta = float(_cfg("GROWW_INDEX_OPTION_TARGET_ABS_DELTA", 0.45) if desk_id == "GROWW_INDEX_OPTIONS" else _cfg("GROWW_STOCK_OPTION_TARGET_ABS_DELTA", 0.50))
        delta_band = float(_cfg("GROWW_OPTION_DELTA_BAND", 0.22))
        expected_hold_sec = max(1.0, float(_cfg("POLICY_OPTION_MAX_HOLD_SEC", 2700.0)))
        theta_carry_reference_bps = max(1.0, float(_cfg("GROWW_OPTION_SELECTION_CARRY_REFERENCE_BPS", 100.0)))
        max_spread_bps = float(_cfg("GROWW_OPTION_MAX_SPREAD_BPS", 180.0))

        dte_score = clamp(1.0 - abs(((min_dte + max_dte) / 2.0) - dte) / max(1.0, (max_dte - min_dte))) if dte > 0 else 0.0
        spread_bps = ((ask - bid) / mid * 10000.0) if ask > 0 and bid > 0 and mid > 0 else 0.0
        spread_score = 0.55 if spread_bps <= 0 else clamp(1.0 - spread_bps / max_spread_bps)
        bs_score = 0.0
        theta_carry_score = 0.0
        theta_carry_bps = 0.0
        delta_score = 0.0
        if bs:
            abs_delta = abs(bs.delta)
            delta_score = clamp(1.0 - abs(abs_delta - target_delta) / max(delta_band, 1e-6))
            # Theta is a hold-horizon carry cost. It is scored and later deducted from
            # expected trade edge, never used as a hidden session-start veto.
            theta_carry_bps = bs.theta_to_premium * (expected_hold_sec / 86400.0) * 10000.0
            theta_carry_score = clamp(1.0 - theta_carry_bps / theta_carry_reference_bps)
            # Avoid deep OTM lottery and deep ITM capital lock; prefer tradable ATM/near-ATM alpha.
            moneyness_score = clamp(1.0 - abs(bs.moneyness - 1.0) / 0.08)
            bs_score = 0.42 * delta_score + 0.36 * theta_carry_score + 0.22 * moneyness_score
        live_score = 1.0 if quote else 0.0 if bool(_cfg("GROWW_OPTION_REQUIRE_LIVE_QUOTE", True)) else 0.45
        score = clamp(0.35 * bs_score + 0.25 * dte_score + 0.20 * spread_score + 0.20 * live_score)
        if bool(_cfg("GROWW_OPTION_REQUIRE_LIVE_QUOTE", True)) and not quote:
            score = 0.0

        reasons: list[str] = [desk_id.lower(), f"underlying={underlying}"]
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
            reasons.append(f"theta/prem/day={bs.theta_to_premium:.2%}")
            reasons.append(f"theta_carry_hold={theta_carry_bps:.2f}bps@{expected_hold_sec:.0f}s")
        if not iv_is_live:
            reasons.append("iv_stress_prior")
        if spread_bps > max_spread_bps:
            reasons.append("wide_option_spread")
        if quote:
            reasons.append("groww_quote")
        elif bool(_cfg("GROWW_OPTION_REQUIRE_LIVE_QUOTE", True)):
            reasons.append("live_quote_required")

        return OptionSelectionScore(
            score=score,
            desk_id=desk_id,
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


@dataclass(frozen=True)
class OptionVolatilityContext:
    """Live option-chain valuation state; never treats an IV prior as market data."""

    atm_iv: float | None
    realized_vol_yang_zhang: float | None
    vrp: float | None
    skew_25d: float | None
    term_slope: float | None
    gross_gamma_exposure: float | None
    signed_dealer_gex: float | None
    live_iv_coverage: float
    ready_for_long_premium_decision: bool
    reasons: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def yang_zhang_realized_vol(
    candles: Sequence[Mapping[str, Any]], *, window: int = 10, periods_per_year: float = 252.0
) -> float | None:
    """Annualised Yang-Zhang realised volatility using genuine OHLC observations."""
    rows = list(candles or [])[-max(int(window), 2):]
    if len(rows) < 2:
        return None
    parsed: list[tuple[float, float, float, float]] = []
    for row in rows:
        o = safe_float(row.get("open") or row.get("Open"), 0.0)
        h = safe_float(row.get("high") or row.get("High"), 0.0)
        l = safe_float(row.get("low") or row.get("Low"), 0.0)
        c = safe_float(row.get("close") or row.get("Close"), 0.0)
        if min(o, h, l, c) <= 0 or h < l:
            continue
        parsed.append((o, h, l, c))
    if len(parsed) < 2:
        return None
    overnight = [math.log(parsed[i][0] / parsed[i - 1][3]) for i in range(1, len(parsed))]
    close_open = [math.log(c / o) for o, _, _, c in parsed]
    rogers_satchell = [
        math.log(h / o) * math.log(h / c) + math.log(l / o) * math.log(l / c)
        for o, h, l, c in parsed
    ]
    n = len(parsed)
    k = 0.34 / (1.34 + (n + 1.0) / max(n - 1.0, 1.0))
    overnight_var = sum(x * x for x in overnight) / max(len(overnight), 1)
    open_close_var = sum(x * x for x in close_open) / n
    rs_var = sum(rogers_satchell) / n
    per_period_var = max(0.0, overnight_var + k * open_close_var + (1.0 - k) * rs_var)
    return math.sqrt(per_period_var * float(periods_per_year))


def compute_vrp(*, atm_iv: float | None, realized_vol_yz: float | None) -> float | None:
    """Volatility risk premium: positive means implied volatility exceeds realised volatility."""
    if atm_iv is None or realized_vol_yz is None:
        return None
    return float(atm_iv) - float(realized_vol_yz)


def _row_iv(row: Mapping[str, Any]) -> float | None:
    iv = safe_float(row.get("iv") or row.get("implied_volatility") or row.get("impliedVolatility"), 0.0)
    if iv > 3.0:
        iv /= 100.0
    return iv if iv > 0 else None


def _row_expiry(row: Mapping[str, Any]) -> datetime | None:
    return _parse_expiry(row.get("expiry_date") or row.get("ExpiryDate") or row.get("expiry") or row.get("Expiry"))


def _row_strike(row: Mapping[str, Any]) -> float:
    return safe_float(row.get("strike_price") or row.get("StrikePrice") or row.get("strike") or row.get("Strike"), 0.0)


def build_option_volatility_context(
    *, chain: Iterable[Mapping[str, Any]], underlying_candles: Sequence[Mapping[str, Any]], spot: float,
    lot_size: int, risk_free_rate: float = 0.065, now: datetime | None = None,
) -> OptionVolatilityContext:
    """Derive VRP, skew, term structure and gamma exposure only from live chain inputs.

    Gross gamma exposure is computable from open interest. Directed dealer GEX is returned
    only when the input row supplies `dealer_position_sign`; it is not invented from option right.
    """
    if float(spot) <= 0:
        return OptionVolatilityContext(None, None, None, None, None, None, None, 0.0, False, ("invalid_spot",))
    now = now or datetime.now(timezone.utc)
    rows = [dict(row) for row in chain or []]
    live_rows: list[dict[str, Any]] = []
    gross_gex = 0.0
    signed_gex = 0.0
    signed_available = False
    for row in rows:
        iv = _row_iv(row)
        strike = _row_strike(row)
        expiry = _row_expiry(row)
        right = _right(row)
        oi = safe_float(row.get("open_interest") or row.get("openInterest") or row.get("oi"), 0.0)
        if iv is None or strike <= 0 or expiry is None or right not in {"call", "put"}:
            continue
        dte = max((expiry - now).total_seconds() / 86400.0, 0.0)
        bs = BlackScholesModel.greeks(right, float(spot), strike, dte, float(risk_free_rate), iv)
        if bs is None:
            continue
        enriched = {**row, "_iv": iv, "_strike": strike, "_expiry": expiry, "_right": right, "_bs": bs}
        live_rows.append(enriched)
        if oi > 0:
            exposure = bs.gamma * oi * max(int(lot_size), 1) * float(spot) * float(spot) * 0.01
            gross_gex += exposure
            provided_sign = row.get("dealer_position_sign")
            if provided_sign is not None:
                sign = 1.0 if safe_float(provided_sign, 0.0) > 0 else -1.0 if safe_float(provided_sign, 0.0) < 0 else 0.0
                signed_gex += exposure * sign
                signed_available = True
    coverage = len(live_rows) / max(len(rows), 1)
    if not live_rows:
        rv = yang_zhang_realized_vol(underlying_candles)
        return OptionVolatilityContext(None, rv, None, None, None, None, None, coverage, False, ("live_iv_chain_unavailable",))
    expiries = sorted({r["_expiry"] for r in live_rows})
    front_expiry = expiries[0]
    front = [r for r in live_rows if r["_expiry"] == front_expiry]
    atm = min(front, key=lambda r: abs(r["_strike"] - float(spot)))
    atm_iv = float(atm["_iv"])
    next_atm_iv = None
    if len(expiries) > 1:
        next_rows = [r for r in live_rows if r["_expiry"] == expiries[1]]
        if next_rows:
            next_atm_iv = float(min(next_rows, key=lambda r: abs(r["_strike"] - float(spot)))["_iv"])
    calls = [r for r in front if r["_right"] == "call"]
    puts = [r for r in front if r["_right"] == "put"]
    call_25 = min(calls, key=lambda r: abs(abs(r["_bs"].delta) - 0.25)) if calls else None
    put_25 = min(puts, key=lambda r: abs(abs(r["_bs"].delta) - 0.25)) if puts else None
    skew = float(put_25["_iv"] - call_25["_iv"]) if put_25 and call_25 else None
    term = float(atm_iv - next_atm_iv) if next_atm_iv is not None else None
    rv = yang_zhang_realized_vol(underlying_candles)
    vrp = compute_vrp(atm_iv=atm_iv, realized_vol_yz=rv)
    min_coverage = float(_cfg("GROWW_OPTION_MIN_LIVE_IV_COVERAGE", 0.60))
    max_long_vrp = float(_cfg("GROWW_OPTION_LONG_MAX_VRP", -0.02))
    reasons: list[str] = []
    if rv is None:
        reasons.append("yang_zhang_realized_vol_unavailable")
    if coverage < min_coverage:
        reasons.append("insufficient_live_iv_coverage")
    if vrp is None:
        reasons.append("vrp_unavailable")
    elif vrp > max_long_vrp:
        reasons.append("long_premium_not_cheap_on_vrp")
    ready = rv is not None and coverage >= min_coverage and vrp is not None and vrp <= max_long_vrp
    if signed_available is False:
        reasons.append("directed_dealer_gex_requires_position_sign_input")
    return OptionVolatilityContext(
        atm_iv=atm_iv, realized_vol_yang_zhang=rv, vrp=vrp, skew_25d=skew, term_slope=term,
        gross_gamma_exposure=gross_gex if gross_gex > 0 else None,
        signed_dealer_gex=signed_gex if signed_available else None,
        live_iv_coverage=coverage, ready_for_long_premium_decision=ready, reasons=tuple(reasons),
    )

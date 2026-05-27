"""Institutional GROWW option-chain architecture.

The Indian options desk owns an *underlying thesis* for direction, while its
execution universe is prepared in advance.  At session start the module selects
one verified CE vehicle and one verified PE vehicle from the current NFO master
and live quotes; a later bullish/bearish thesis activates only its corresponding
preselected vehicle.  NIFTY/BANKNIFTY/SENSEX or large-cap stocks remain desk
assets for structural analysis.
"""
from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from typing import Any, Iterable, Mapping, Optional

try:
    import config
except Exception:  # pragma: no cover
    config = None  # type: ignore

from core.instruments import normalise_symbol
from .indian_options_desk import BlackScholesModel

logger = logging.getLogger(__name__)


def _cfg(name: str, default: Any) -> Any:
    return getattr(config, name, default) if config is not None else default


def clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    try:
        v = float(value)
    except Exception:
        v = low
    return max(low, min(high, v))


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        if isinstance(value, str):
            value = value.strip().replace(",", "")
            if not value:
                return default
        v = float(value)
        return v if math.isfinite(v) else default
    except Exception:
        return default


def _right(raw: Mapping[str, Any]) -> str:
    v = normalise_symbol(raw.get("right") or raw.get("option_type") or raw.get("OptionType") or raw.get("Right") or raw.get("CallPut") or "")
    if v in {"C", "CE", "CALL"}:
        return "call"
    if v in {"P", "PE", "PUT"}:
        return "put"
    return ""


def _exchange_segment(raw: Mapping[str, Any]) -> str:
    """Return the executable exchange segment for mixed GROWW master schemas.

    FONSEScripMaster uses ``ExchangeCode`` for the underlying display name
    (for example ``NIFTY 50``) and ``ExAllowed`` for the actual segment
    (``NFO``).  Older/test payloads use ``exchange_code``/``ExchangeCode`` as
    the segment directly.
    """
    for key in (
        "exchange_code", "exchange", "segment", "ExchangeSegment",
        "ExAllowed", "ex_allowed", "AllowedExchange",
        "ExchangeCode", "Exchange", "Exch",
    ):
        val = normalise_symbol(raw.get(key) or "")
        if val in {"NFO", "BFO", "NSE", "BSE"}:
            return val
    source = normalise_symbol(raw.get("_source_file") or "")
    if source.startswith("FONSE"):
        return "NFO"
    if source.startswith("FOBSE"):
        return "BFO"
    return ""


def _stock_code(raw: Mapping[str, Any]) -> str:
    for key in (
        "stock_code", "StockCode", "ShortName", "underlying", "Underlying",
        "AssetName", "Symbol",
    ):
        val = normalise_symbol(raw.get(key) or "")
        if val:
            return val
    company = normalise_symbol(raw.get("CompanyName") or raw.get("ExchangeCode") or "")
    aliases = {
        "NIFTY50": "NIFTY",
        "NIFTYFIFTY": "NIFTY",
        "NIFTYBANK": "BANKNIFTY",
        "NIFTYBANKINDEX": "BANKNIFTY",
    }
    return aliases.get(company, company)


def _product_kind(raw: Mapping[str, Any]) -> str:
    return normalise_symbol(
        raw.get("product_type") or raw.get("ProductType")
        or raw.get("InstrumentType") or raw.get("InstrumentName")
        or raw.get("Series") or ""
    )


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


def _bid_ask(raw: Mapping[str, Any], quote: Mapping[str, Any] | None = None) -> tuple[float, float]:
    """Read a two-sided executable quote, including Groww OptionChain fields."""
    for src in (quote or {}, raw):
        bid = safe_float(
            src.get("best_bid_price") or src.get("best_bid") or src.get("bid")
            or src.get("bid_price") or src.get("bPrice"), 0.0)
        ask = safe_float(
            src.get("best_offer_price") or src.get("best_ask_price") or src.get("best_ask")
            or src.get("ask") or src.get("ask_price") or src.get("offer_price") or src.get("sPrice"), 0.0)
        if bid > 0 and ask > 0 and ask >= bid:
            return bid, ask
    return 0.0, 0.0


def _premium(raw: Mapping[str, Any], quote: Mapping[str, Any] | None = None) -> float:
    bid, ask = _bid_ask(raw, quote)
    if bid > 0 and ask > 0:
        return (bid + ask) / 2.0
    for src in (quote or {}, raw):
        for key in ("ltp", "last_price", "lastPrice", "close", "price", "settlement_price"):
            px = safe_float(src.get(key), 0.0)
            if px > 0:
                return px
    return 0.0


def _lot_size(raw: Mapping[str, Any]) -> float:
    for key in (
        # runtime_lot_size is populated only from the verified daily Security
        # Master join and must override any lot-like field in a live quote row.
        "runtime_lot_size", "LotSize", "lot_size", "lotSize", "MinimumLotQty", "minimum_lot_qty",
        "min_qty", "quantity_in_lot", "QuantityInLot",
    ):
        lot = safe_float(raw.get(key), 0.0)
        if lot > 0:
            return lot
    # No synthetic default lots for NFO options; exact broker/security-master
    # lot size is required to price affordability and risk correctly.
    fallback = safe_float(_cfg("GROWW_OPTION_DEFAULT_LOT_SIZE", 0.0), 0.0)
    return max(0.0, fallback)


def _configured_vol_bounds() -> tuple[float, float]:
    low = max(0.0001, safe_float(_cfg("GROWW_OPTION_MIN_IMPLIED_VOL", 0.03), 0.03))
    high = max(low * 2.0, safe_float(_cfg("GROWW_OPTION_MAX_IMPLIED_VOL", 1.50), 1.50))
    return low, high


def _raw_iv(raw: Mapping[str, Any], quote: Mapping[str, Any] | None = None) -> float:
    for src in (quote or {}, raw):
        for key in (
            "iv", "IV", "implied_volatility", "impliedVolatility",
            "implied_vol", "ImpliedVolatility",
        ):
            value = safe_float(src.get(key), 0.0)
            if value > 0:
                if value > 3.0:
                    value /= 100.0
                low, high = _configured_vol_bounds()
                return max(low, min(high, value))
    return 0.0


def _market_volatility(
    option_type: str,
    spot: float,
    strike: float,
    dte: float,
    rate: float,
    premium: float,
    raw: Mapping[str, Any],
    quote: Mapping[str, Any] | None = None,
) -> tuple[float, str]:
    live_iv = _raw_iv(raw, quote)
    if live_iv > 0:
        return live_iv, "live_field"
    low, high = _configured_vol_bounds()
    implied = BlackScholesModel.implied_volatility(
        option_type, spot, strike, dte, rate, premium,
        min_vol=low, max_vol=high,
    )
    if implied and implied > 0:
        return implied, "quote_implied"
    return max(low, min(high, safe_float(_cfg("GROWW_OPTION_IV_STRESS_PRIOR", 0.24), 0.24))), "stress_prior"


@dataclass(frozen=True)
class GrowwContractChoice:
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


_IST = timezone(timedelta(hours=5, minutes=30))


@dataclass(frozen=True)
class GrowwSessionContractBook:
    """Daily NFO execution vehicles selected before intraday alpha decisions.

    The book always holds both directional vehicles: CE for a later bullish
    underlying thesis and PE for a later bearish underlying thesis.  It is an
    execution-universe object, never a directional forecast.
    """
    trade_date_ist: str
    built_at: float
    underlying: str
    underlying_spot: float
    available_funds: float
    call: GrowwContractChoice
    put: GrowwContractChoice
    source: str = "session_start_option_chain"

    def as_dict(self) -> dict[str, Any]:
        return {
            "trade_date_ist": self.trade_date_ist,
            "built_at": self.built_at,
            "underlying": self.underlying,
            "underlying_spot": self.underlying_spot,
            "available_funds": self.available_funds,
            "source": self.source,
            "call": self.call.as_dict(),
            "put": self.put.as_dict(),
        }


def _session_date_ist(now_ts: Optional[float] = None) -> str:
    return datetime.fromtimestamp(float(now_ts or time.time()), tz=_IST).date().isoformat()


def _choice_from_dict(value: Any) -> Optional[GrowwContractChoice]:
    if not isinstance(value, Mapping):
        return None
    try:
        return GrowwContractChoice(
            score=float(value.get("score", 0.0) or 0.0),
            underlying=str(value.get("underlying") or ""),
            thesis_side=str(value.get("thesis_side") or ""),
            selected_symbol=str(value.get("selected_symbol") or ""),
            right=str(value.get("right") or ""),
            strike=float(value.get("strike", 0.0) or 0.0),
            expiry=str(value.get("expiry") or ""),
            dte=float(value.get("dte", 0.0) or 0.0),
            delta=float(value.get("delta", 0.0) or 0.0),
            theta_to_premium=float(value.get("theta_to_premium", 0.0) or 0.0),
            moneyness=float(value.get("moneyness", 0.0) or 0.0),
            reasons=tuple(str(x) for x in (value.get("reasons") or ())),
            raw=dict(value.get("raw") or {}),
        )
    except Exception:
        return None


def contract_key(raw: Mapping[str, Any]) -> tuple[str, str, float]:
    expiry = _expiry_dt(raw.get("expiry_date") or raw.get("ExpiryDate") or raw.get("expiry") or raw.get("Expiry"))
    exp = expiry.strftime("%Y-%m-%d") if expiry else ""
    return exp, _right(raw), round(_strike(raw), 6)


def eligible_nfo_master_option_rows(rows: Iterable[Mapping[str, Any]], underlying: str) -> list[dict[str, Any]]:
    """Return only exact, tradable NFO option master rows for an underlying.

    Lot size is intentionally mandatory: a chain quote is market data, while the
    daily Security Master is the instrument-definition source used for routing and
    sizing.  Missing identity or lot size fails closed.
    """
    target = normalise_symbol(underlying)
    out: list[dict[str, Any]] = []
    min_dte = float(_cfg("GROWW_OPTION_MIN_DTE", 1.0))
    max_dte = float(_cfg("GROWW_OPTION_MAX_DTE", 21.0))
    for source in rows:
        if not isinstance(source, Mapping):
            continue
        row = dict(source)
        exchange = _exchange_segment(row)
        if exchange != "NFO":
            continue
        stock = _stock_code(row)
        if not stock or stock != target:
            continue
        product = _product_kind(row)
        if product and product not in {"OPTION", "OPTIONS", "OPTIDX", "OPTSTK", "CE", "PE"} and not _right(row):
            continue
        if not _right(row) or _strike(row) <= 0 or _lot_size(row) <= 0:
            continue
        dte = _dte(row)
        if dte < min_dte or dte > max_dte:
            continue
        expiry = row.get("expiry_date") or row.get("ExpiryDate") or row.get("expiry") or row.get("Expiry")
        strike = _strike(row)
        right = _right(row)
        row["stock_code"] = target
        row["exchange_code"] = "NFO"
        row.setdefault("product_type", "Options")
        row.setdefault("expiry_date", expiry)
        row.setdefault("strike_price", strike)
        row.setdefault("right", "Call" if right == "call" else "Put")
        row.setdefault("TradingSymbol", f"{target}_{expiry}_{strike:g}_{'CE' if right == 'call' else 'PE'}")
        row.setdefault("runtime_lot_size", _lot_size(row))
        row.setdefault("instrument_definition_source", "daily_security_master")
        out.append(row)
    return out


def merge_verified_chain_quotes(master_rows: Iterable[Mapping[str, Any]], quote_rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Join live OptionChain quotes to Security Master-defined contracts only."""
    verified = {contract_key(row): dict(row) for row in master_rows if contract_key(row)[0] and contract_key(row)[1]}
    merged: list[dict[str, Any]] = []
    for quote in quote_rows:
        if not isinstance(quote, Mapping):
            continue
        key = contract_key(quote)
        master = verified.get(key)
        if master is None:
            continue
        row = dict(master)
        row.update(dict(quote))
        row["runtime_lot_size"] = _lot_size(master)
        row["instrument_definition_source"] = "daily_security_master"
        row["quote_source"] = quote.get("quote_source") or "groww_option_chain_filtered"
        merged.append(row)
    return merged



def shortlist_contracts_for_stream_validation(
    instrument: Any,
    thesis_side: str,
    *,
    underlying_spot: float,
    available_funds: float,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Rank chain-defined vehicles for official live-stream validation.

    Groww's documented ``get_option_chain`` response contains LTP, Greeks, open
    interest and volume.  It is used to rank a bounded candidate universe only.
    The next stage subscribes that universe to the official FNO LTP and market-
    depth feeds; only two-sided streamed books may enter the session contract book.
    """
    raw = getattr(getattr(instrument, "primary", None), "raw", {}) or {}
    chain = [dict(x) for x in (raw.get("chain_candidates") or []) if isinstance(x, Mapping)]
    side = str(thesis_side or "").lower()
    desired = "call" if side == "long" else "put" if side == "short" else ""
    if not chain or not desired:
        return []
    spot = max(0.0, safe_float(underlying_spot or raw.get("underlying_spot_price") or raw.get("spot_price"), 0.0))
    min_dte = float(_cfg("GROWW_OPTION_MIN_DTE", 2.0))
    max_dte = float(_cfg("GROWW_OPTION_MAX_DTE", 21.0))
    target_delta = float(_cfg("GROWW_INDEX_OPTION_TARGET_ABS_DELTA", 0.45) if raw.get("desk_id") == "GROWW_INDEX_OPTIONS" else _cfg("GROWW_STOCK_OPTION_TARGET_ABS_DELTA", 0.50))
    max_fraction = clamp(safe_float(_cfg("GROWW_OPTION_MAX_FUNDS_FRACTION_PER_TRADE", 0.42), 0.42), 0.01, 1.0)
    cash_buffer = max(0.0, safe_float(_cfg("GROWW_OPTION_MIN_CASH_BUFFER_INR", 0.0), 0.0))
    max_cost = max(0.0, (safe_float(available_funds, 0.0) - cash_buffer) * max_fraction)
    max_rows = max(1, int(limit if limit is not None else _cfg("GROWW_SESSION_BOOK_STREAM_CANDIDATES_PER_SIDE", 12)))
    ranked: list[tuple[float, dict[str, Any]]] = []
    for candidate in chain:
        if _right(candidate) != desired:
            continue
        dte = _dte(candidate)
        strike = _strike(candidate)
        lot = _lot_size(candidate)
        premium = _premium(candidate)
        if strike <= 0 or lot <= 0 or premium <= 0 or dte < min_dte or dte > max_dte:
            continue
        cost = premium * lot
        if max_cost <= 0 or cost > max_cost:
            continue
        greeks = candidate.get("greeks") if isinstance(candidate.get("greeks"), Mapping) else {}
        signed_delta = safe_float(candidate.get("delta") or greeks.get("delta"), 0.0)
        abs_delta = abs(signed_delta)
        # An official chain row without a reported delta remains eligible but ranks
        # below otherwise comparable rows; do not manufacture a broker Greek.
        delta_score = clamp(1.0 - abs(abs_delta - target_delta) / max(target_delta, 1e-9)) if abs_delta > 0 else 0.15
        dte_mid = (min_dte + max_dte) / 2.0
        dte_score = clamp(1.0 - abs(dte - dte_mid) / max(1.0, max_dte - min_dte))
        affordability = clamp(1.0 - abs(cost / max(max_cost, 1e-9) - 0.58) / 0.58)
        volume = max(0.0, safe_float(candidate.get("volume"), 0.0))
        open_interest = max(0.0, safe_float(candidate.get("open_interest"), 0.0))
        activity = 0.55 * clamp(math.log1p(volume) / math.log1p(100000.0)) + 0.45 * clamp(math.log1p(open_interest) / math.log1p(100000.0))
        if spot > 0:
            proximity = clamp(1.0 - abs(strike - spot) / max(spot * 0.12, 1.0))
        else:
            proximity = 0.0
        score = 0.40 * delta_score + 0.18 * activity + 0.17 * affordability + 0.15 * dte_score + 0.10 * proximity
        enriched = dict(candidate)
        enriched["shortlist_score"] = score
        enriched["shortlist_basis"] = "groww_option_chain_rank_then_live_fno_depth"
        enriched["shortlist_contract_cost"] = cost
        enriched["runtime_lot_size"] = lot
        ranked.append((score, enriched))
    ranked.sort(key=lambda item: item[0], reverse=True)
    return [row for _, row in ranked[:max_rows]]


def build_session_contract_book(
    instrument: Any,
    *,
    underlying_spot: float,
    available_funds: float,
    option_quote_by_symbol: Optional[Mapping[str, Mapping[str, Any]]] = None,
    now_ts: Optional[float] = None,
    commit: bool = True,
    diagnostics: Optional[dict[str, Any]] = None,
) -> Optional[GrowwSessionContractBook]:
    """Select an executable CE and PE from the official live candidate universe.

    This is execution preparation only; it does not assert direction.  Critically,
    theta is not a static session-start veto.  Carry is measured for each vehicle
    and charged against signal edge at decision time, when expected holding horizon
    and direction are known.
    """
    call_diag: dict[str, Any] = {}
    put_diag: dict[str, Any] = {}
    call = select_contract_for_thesis(
        instrument, "long", underlying_spot=underlying_spot,
        option_quote_by_symbol=option_quote_by_symbol, available_funds=available_funds,
        diagnostics=call_diag)
    put = select_contract_for_thesis(
        instrument, "short", underlying_spot=underlying_spot,
        option_quote_by_symbol=option_quote_by_symbol, available_funds=available_funds,
        diagnostics=put_diag)
    if diagnostics is not None:
        diagnostics.update({"call": call_diag, "put": put_diag})
    if call is None or put is None:
        return None
    raw = getattr(getattr(instrument, "primary", None), "raw", {}) or {}
    book = GrowwSessionContractBook(
        trade_date_ist=_session_date_ist(now_ts),
        built_at=float(now_ts or time.time()),
        underlying=normalise_symbol(raw.get("stock_code") or raw.get("underlying") or getattr(instrument, "asset_id", "")),
        underlying_spot=float(underlying_spot or 0.0),
        available_funds=float(available_funds or 0.0),
        call=call, put=put,
        source=str(raw.get("chain_source") or "session_start_option_chain"),
    )
    if commit and isinstance(raw, dict):
        raw["session_contract_book"] = book.as_dict()
        raw["session_contract_book_status"] = "READY"
        raw["session_contract_book_mode"] = "preselected_call_and_put_live_direction"
        raw["session_contract_diagnostics"] = {"call": call_diag, "put": put_diag}
    return book


def select_contract_from_session_book(
    instrument: Any, thesis_side: str, *, underlying_spot: float, available_funds: float, now_ts: Optional[float] = None
) -> tuple[Optional[GrowwContractChoice], str]:
    """Return the preselected direction-specific vehicle or a refresh reason."""
    raw = getattr(getattr(instrument, "primary", None), "raw", {}) or {}
    book = raw.get("session_contract_book") if isinstance(raw, dict) else None
    if not isinstance(book, Mapping):
        return None, "session_contract_book_missing"
    if str(book.get("trade_date_ist") or "") != _session_date_ist(now_ts):
        return None, "session_contract_book_new_trading_day"
    side = str(thesis_side or "").lower()
    key = "call" if side == "long" else "put" if side == "short" else ""
    if not key:
        return None, "invalid_thesis_side"
    choice = _choice_from_dict(book.get(key))
    if choice is None or _lot_size(choice.raw) <= 0 or _premium(choice.raw) <= 0:
        return None, "session_contract_vehicle_invalid"
    book_spot = safe_float(book.get("underlying_spot"), 0.0)
    spot = safe_float(underlying_spot, 0.0)
    max_drift = max(0.0, safe_float(_cfg("GROWW_SESSION_BOOK_MAX_SPOT_DRIFT_PCT", 0.008), 0.008))
    if book_spot > 0 and spot > 0 and abs(spot - book_spot) / book_spot > max_drift:
        return None, "session_contract_spot_drift"
    # Primary refresh logic: retain a preselected vehicle only while its current
    # delta remains inside the institutional execution band. This detects a CE/PE
    # that has become too ITM/OTM as NIFTY moves, instead of relying only on a
    # static spot percentage.
    if spot > 0 and choice.strike > 0:
        dte = _dte(choice.raw) or choice.dte
        rate = float(_cfg("INDIA_RISK_FREE_RATE", 0.065))
        premium = _premium(choice.raw)
        iv = safe_float(choice.raw.get("bs_volatility"), 0.0)
        if iv <= 0:
            iv, _ = _market_volatility(choice.right, spot, choice.strike, dte, rate, premium, choice.raw)
        greeks = BlackScholesModel.greeks(choice.right, spot, choice.strike, dte, rate, iv, premium=premium)
        if greeks is not None:
            target_delta = float(_cfg("GROWW_INDEX_OPTION_TARGET_ABS_DELTA", 0.45))
            delta_band = max(0.01, float(_cfg("GROWW_SESSION_BOOK_DELTA_RESELECT_BAND", 0.18)))
            if abs(abs(greeks.delta) - target_delta) > delta_band:
                return None, "session_contract_delta_drift"
    funds = max(0.0, safe_float(available_funds, 0.0))
    max_fraction = clamp(safe_float(_cfg("GROWW_OPTION_MAX_FUNDS_FRACTION_PER_TRADE", 0.42), 0.42), 0.01, 1.0)
    cash_buffer = max(0.0, safe_float(_cfg("GROWW_OPTION_MIN_CASH_BUFFER_INR", 0.0), 0.0))
    max_cost = max(0.0, (funds - cash_buffer) * max_fraction) if funds > 0 else 0.0
    cost = safe_float(choice.raw.get("selected_contract_cost"), _premium(choice.raw) * _lot_size(choice.raw))
    if funds <= 0 or max_cost <= 0 or cost <= 0 or cost > max_cost:
        return None, "session_contract_no_longer_affordable"
    return choice, "session_contract_ready"


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
        "groww_underlying_desk": True,
        "contract_selector_mode": "session_preselected_execution",
        "underlying": normalise_symbol(underlying),
        "stock_code": normalise_symbol(underlying),
        "desk_id": desk_id,
        "exchange_code": "BFO" if desk_id == "GROWW_INDEX_OPTIONS" and normalise_symbol(sample.get("exchange_code")) == "BFO" else "NFO",
        "product_type": "option_chain",
        "chain_quality": quality,
        "chain_candidates": chain_raw,
        "selected_option_contract": None,
    }


def is_chain_instrument(instrument: Any) -> bool:
    raw = getattr(getattr(instrument, "primary", None), "raw", {}) or {}
    return bool(
        raw.get("groww_underlying_desk")
        and str(raw.get("contract_selector_mode") or "").lower() == "session_preselected_execution"
    )


def select_contract_for_thesis(
    instrument: Any,
    thesis_side: str,
    *,
    underlying_spot: float = 0.0,
    option_quote_by_symbol: Optional[Mapping[str, Mapping[str, Any]]] = None,
    available_funds: float = 0.0,
    diagnostics: Optional[dict[str, Any]] = None,
) -> Optional[GrowwContractChoice]:
    """Rank a live executable option vehicle for one possible underlying thesis.

    Selection uses verified lot/identity, official streamed bid/offer, affordability,
    delta and live-IV Black-Scholes diagnostics.  Theta is a *measured carry cost*,
    not a hard session-start veto: the strategy deducts carry over its actual hold
    horizon from net expected edge before any order can be approved.
    """
    raw = getattr(getattr(instrument, "primary", None), "raw", {}) or {}
    chain = [dict(x) for x in (raw.get("chain_candidates") or []) if isinstance(x, Mapping)]
    side = str(thesis_side or "").lower()
    desired = "call" if side == "long" else "put" if side == "short" else ""
    rejected: dict[str, int] = {}
    audit_rows: list[dict[str, Any]] = []
    def reject(name: str) -> None:
        rejected[name] = rejected.get(name, 0) + 1
    if not chain or not desired:
        if diagnostics is not None:
            diagnostics.update({"side": side, "desired": desired, "chain_rows": len(chain), "rejected": {"invalid_request_or_empty_chain": 1}, "accepted": 0})
        return None
    spot = float(underlying_spot or safe_float(raw.get("underlying_spot_price") or raw.get("spot_price"), 0.0))
    min_dte = float(_cfg("GROWW_OPTION_MIN_DTE", 2.0))
    max_dte = float(_cfg("GROWW_OPTION_MAX_DTE", 21.0))
    target_delta = float(_cfg("GROWW_INDEX_OPTION_TARGET_ABS_DELTA", 0.45) if raw.get("desk_id") == "GROWW_INDEX_OPTIONS" else _cfg("GROWW_STOCK_OPTION_TARGET_ABS_DELTA", 0.50))
    delta_band = float(_cfg("GROWW_OPTION_DELTA_BAND", 0.22))
    iv_prior = float(_cfg("GROWW_OPTION_IV_STRESS_PRIOR", 0.24))
    rate = float(_cfg("INDIA_RISK_FREE_RATE", 0.065))
    expected_hold_sec = max(1.0, float(_cfg("POLICY_OPTION_MAX_HOLD_SEC", 2700.0)))
    carry_reference_bps = max(1.0, float(_cfg("GROWW_OPTION_SELECTION_CARRY_REFERENCE_BPS", 100.0)))
    funds = max(0.0, safe_float(available_funds, 0.0))
    cash_buffer = max(0.0, safe_float(_cfg("GROWW_OPTION_MIN_CASH_BUFFER_INR", 0.0), 0.0))
    funds_fraction = clamp(safe_float(_cfg("GROWW_OPTION_MAX_FUNDS_FRACTION_PER_TRADE", 0.42), 0.42), 0.01, 1.0)
    max_contract_cost = max(0.0, (funds - cash_buffer) * funds_fraction) if funds > 0 else 0.0
    choices: list[GrowwContractChoice] = []
    quotes = option_quote_by_symbol or {}
    for c in chain:
        if _right(c) != desired:
            continue
        dte = _dte(c); strike = _strike(c)
        symbol = normalise_symbol(c.get("TradingSymbol") or c.get("trading_symbol") or c.get("symbol") or f"{raw.get('stock_code')}_{c.get('expiry_date')}_{strike}_{desired[:1].upper()}")
        q = dict(quotes.get(symbol, {}) or {})
        row_audit: dict[str, Any] = {"symbol": symbol, "right": desired, "strike": strike, "dte": round(dte, 3)}
        if strike <= 0 or dte < min_dte or dte > max_dte:
            reject("expiry_or_strike_outside_policy"); continue
        prem = _premium(c, q); lot = _lot_size(c)
        row_audit.update({"premium": round(prem, 4), "lot": lot})
        if lot <= 0:
            reject("verified_lot_missing"); continue
        bid, ask = _bid_ask(c, q)
        require_two_sided = bool(_cfg("GROWW_SESSION_BOOK_REQUIRE_TWO_SIDED_QUOTE", True))
        if require_two_sided and not (bid > 0 and ask >= bid):
            reject("official_two_sided_book_missing"); continue
        spread_bps = ((ask - bid) / max((ask + bid) / 2.0, 1e-9) * 10000.0) if bid > 0 and ask > 0 else float("inf")
        max_spread_bps = max(1.0, safe_float(_cfg("GROWW_OPTION_MAX_SELECTION_SPREAD_BPS", 120.0), 120.0))
        row_audit.update({"bid": round(bid, 4), "ask": round(ask, 4), "spread_bps": round(spread_bps, 2) if math.isfinite(spread_bps) else None})
        if require_two_sided and spread_bps > max_spread_bps:
            reject("spread_exceeds_execution_limit"); continue
        bid_qty = safe_float(c.get("best_bid_quantity") or q.get("best_bid_quantity") or c.get("bid_quantity") or q.get("bid_quantity"), 0.0)
        ask_qty = safe_float(c.get("best_offer_quantity") or q.get("best_offer_quantity") or c.get("offer_quantity") or q.get("offer_quantity") or c.get("ask_quantity") or q.get("ask_quantity"), 0.0)
        min_book_lots = max(0.0, safe_float(_cfg("GROWW_OPTION_MIN_BOOK_LOTS", 1.0), 1.0))
        visible_depth = min(bid_qty, ask_qty)
        row_audit["visible_depth"] = visible_depth
        if require_two_sided and visible_depth < lot * min_book_lots:
            reject("visible_depth_below_one_lot"); continue
        contract_cost = prem * lot if prem > 0 else 0.0
        row_audit.update({"contract_cost": round(contract_cost, 2), "max_contract_cost": round(max_contract_cost, 2)})
        if funds > 0 and (contract_cost <= 0 or contract_cost > max_contract_cost):
            reject("contract_cost_exceeds_budget"); continue
        local_spot = safe_float(q.get("underlying_spot_price") or q.get("underlying_ltp"), 0.0) or spot or strike
        iv, iv_source = _market_volatility(desired, local_spot, strike, dte, rate, prem, c, q)
        if iv <= 0:
            iv = iv_prior; iv_source = "stress_prior"
        bs = BlackScholesModel.greeks(desired, local_spot, strike, dte, rate, iv, premium=prem)
        if bs:
            session_delta_band = max(0.01, float(_cfg("GROWW_SESSION_BOOK_DELTA_RESELECT_BAND", delta_band)))
            row_audit.update({"delta": round(bs.delta, 4), "theta_premium_day": round(bs.theta_to_premium, 6), "iv": round(iv, 6), "iv_source": iv_source})
            if abs(abs(bs.delta) - target_delta) > session_delta_band:
                reject("delta_outside_vehicle_band"); continue
            theta_carry_bps = bs.theta_to_premium * (expected_hold_sec / 86400.0) * 10000.0
            carry_score = clamp(1.0 - theta_carry_bps / carry_reference_bps)
            delta_score = clamp(1.0 - abs(abs(bs.delta) - target_delta) / max(delta_band, 1e-6))
            moneyness_score = clamp(1.0 - abs(bs.moneyness - 1.0) / 0.10)
            model_edge_bps = abs(bs.theoretical_price - prem) / max(prem, 1e-9) * 10000.0 if prem > 0 else 0.0
            edge_score = clamp(1.0 - model_edge_bps / 2500.0)
            bs_score = 0.36 * delta_score + 0.28 * carry_score + 0.20 * moneyness_score + 0.16 * edge_score
            delta = bs.delta; theta = bs.theta_to_premium; mon = bs.moneyness
            row_audit["theta_carry_bps_expected_hold"] = round(theta_carry_bps, 3)
        else:
            reject("bs_snapshot_unavailable"); continue
        dte_mid = (min_dte + max_dte) / 2.0
        dte_score = clamp(1.0 - abs(dte - dte_mid) / max(1.0, max_dte - min_dte))
        live_score = 1.0 if (q or prem > 0) else 0.35
        utilization = contract_cost / max(max_contract_cost, 1e-9) if max_contract_cost > 0 and contract_cost > 0 else 0.0
        affordability_score = clamp(1.0 - abs(utilization - 0.58) / 0.58) if max_contract_cost > 0 and contract_cost > 0 else 0.0
        spread_score = clamp(1.0 - spread_bps / max_spread_bps)
        depth_score = clamp(visible_depth / max(lot * max(1.0, min_book_lots) * 4.0, 1.0))
        liquidity_score = 0.70 * spread_score + 0.30 * depth_score
        score = clamp(0.36 * bs_score + 0.14 * dte_score + 0.25 * liquidity_score + 0.10 * live_score + 0.15 * affordability_score)
        reasons = [f"thesis={side}", f"buy_{desired}", f"dte={dte:.1f}", f"strike={strike:g}", f"spread={spread_bps:.1f}bps", f"depth={visible_depth:.0f}", f"cost={contract_cost:.0f}", f"funds_fit={contract_cost:.0f}/{max_contract_cost:.0f}", f"delta={delta:+.2f}", f"theta/day={theta:.2%}", f"theta_hold={theta_carry_bps:.1f}bps", f"iv={iv:.1%}", f"vol={iv_source}"]
        enriched = dict(c)
        enriched.update({
            "runtime_lot_size": lot, "selected_entry_premium": prem,
            "selected_contract_cost": contract_cost, "selected_max_contract_cost": max_contract_cost,
            "selected_contract_utilization": utilization, "bs_volatility": iv,
            "bs_volatility_source": iv_source, "bs_model_edge_bps": model_edge_bps,
            "theta_carry_horizon_sec": expected_hold_sec, "theta_carry_bps_expected_hold": theta_carry_bps,
            "selection_liquidity_score": liquidity_score, "selection_spread_bps": spread_bps,
            "selection_visible_depth": visible_depth,
            "bs_snapshot": bs.as_dict(), "bs_theoretical_price": bs.theoretical_price,
            "bs_delta": bs.delta, "bs_gamma": bs.gamma, "bs_theta_per_day": bs.theta_per_day,
            "bs_theta_to_premium": bs.theta_to_premium,
        })
        choices.append(GrowwContractChoice(score, normalise_symbol(raw.get("stock_code") or raw.get("underlying") or ""), side, symbol, desired, strike, str(c.get("expiry_date") or c.get("ExpiryDate") or ""), dte, delta, theta, mon, tuple(reasons), enriched))
        row_audit.update({"accepted": True, "score": round(score, 5), "liquidity_score": round(liquidity_score, 5), "theta_carry_bps_expected_hold": round(theta_carry_bps, 3)})
        audit_rows.append(row_audit)
    choices.sort(key=lambda choice: choice.score, reverse=True)
    if diagnostics is not None:
        diagnostics.update({
            "side": side, "desired": desired, "chain_rows": len(chain), "live_book_rows": len(quotes),
            "accepted": len(choices), "rejected": rejected,
            "expected_hold_sec": expected_hold_sec, "carry_policy": "theta_charged_to_signal_edge_not_static_veto",
            "top_accepted": audit_rows[:5],
            "selected": choices[0].as_dict() if choices else None,
        })
    return choices[0] if choices else None

def apply_contract_choice(instrument: Any, choice: GrowwContractChoice) -> None:
    """Mutate the instrument raw payload so existing Groww order adapter routes
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
    raw["runtime_lot_size"] = c.get("runtime_lot_size") or _lot_size(c)
    raw["selected_entry_premium"] = c.get("selected_entry_premium")
    raw["selected_contract_cost"] = c.get("selected_contract_cost")

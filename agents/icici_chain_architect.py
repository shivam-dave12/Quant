"""Institutional ICICI option-chain architecture.

The Indian options desk owns an *underlying thesis* for direction, while its
execution universe is prepared in advance.  At session start the module selects
one verified CE vehicle and one verified PE vehicle from the current NFO master
and live quotes; a later bullish/bearish thesis activates only its corresponding
preselected vehicle.  NIFTY/BANKNIFTY/SENSEX or large-cap stocks remain desk
assets for structural analysis.
"""
from __future__ import annotations

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
    """Return the executable exchange segment for mixed ICICI master schemas.

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
    """Read a two-sided executable quote, including Breeze OptionChain fields."""
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
    fallback = safe_float(_cfg("ICICI_OPTION_DEFAULT_LOT_SIZE", 0.0), 0.0)
    return max(0.0, fallback)


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


_IST = timezone(timedelta(hours=5, minutes=30))


@dataclass(frozen=True)
class ICICISessionContractBook:
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
    call: ICICIContractChoice
    put: ICICIContractChoice
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


def _choice_from_dict(value: Any) -> Optional[ICICIContractChoice]:
    if not isinstance(value, Mapping):
        return None
    try:
        return ICICIContractChoice(
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
    min_dte = float(_cfg("ICICI_OPTION_MIN_DTE", 1.0))
    max_dte = float(_cfg("ICICI_OPTION_MAX_DTE", 21.0))
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
        row["quote_source"] = "breeze_option_chain_filtered"
        merged.append(row)
    return merged


def build_session_contract_book(
    instrument: Any,
    *,
    underlying_spot: float,
    available_funds: float,
    option_quote_by_symbol: Optional[Mapping[str, Mapping[str, Any]]] = None,
    now_ts: Optional[float] = None,
    commit: bool = True,
) -> Optional[ICICISessionContractBook]:
    """Preselect one executable CE and one executable PE for the session.

    Direction remains entirely with the intraday underlying thesis.  This function
    reduces execution latency by choosing execution vehicles before a signal rather
    than rescanning the chain at the moment of entry.
    """
    call = select_contract_for_thesis(
        instrument, "long", underlying_spot=underlying_spot,
        option_quote_by_symbol=option_quote_by_symbol, available_funds=available_funds)
    put = select_contract_for_thesis(
        instrument, "short", underlying_spot=underlying_spot,
        option_quote_by_symbol=option_quote_by_symbol, available_funds=available_funds)
    if call is None or put is None:
        return None
    raw = getattr(getattr(instrument, "primary", None), "raw", {}) or {}
    book = ICICISessionContractBook(
        trade_date_ist=_session_date_ist(now_ts),
        built_at=float(now_ts or time.time()),
        underlying=normalise_symbol(raw.get("stock_code") or raw.get("underlying") or getattr(instrument, "asset_id", "")),
        underlying_spot=float(underlying_spot or 0.0),
        available_funds=float(available_funds or 0.0),
        call=call, put=put,
    )
    if commit and isinstance(raw, dict):
        raw["session_contract_book"] = book.as_dict()
        raw["session_contract_book_status"] = "READY"
        raw["session_contract_book_mode"] = "preselected_call_and_put_live_direction"
    return book


def select_contract_from_session_book(
    instrument: Any, thesis_side: str, *, underlying_spot: float, available_funds: float, now_ts: Optional[float] = None
) -> tuple[Optional[ICICIContractChoice], str]:
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
    max_drift = max(0.0, safe_float(_cfg("ICICI_SESSION_BOOK_MAX_SPOT_DRIFT_PCT", 0.008), 0.008))
    if book_spot > 0 and spot > 0 and abs(spot - book_spot) / book_spot > max_drift:
        return None, "session_contract_spot_drift"
    # Primary refresh logic: retain a preselected vehicle only while its current
    # delta remains inside the institutional execution band. This detects a CE/PE
    # that has become too ITM/OTM as NIFTY moves, instead of relying only on a
    # static spot percentage.
    if spot > 0 and choice.strike > 0:
        dte = _dte(choice.raw) or choice.dte
        iv = float(_cfg("ICICI_OPTION_IV_STRESS_PRIOR", 0.24))
        rate = float(_cfg("INDIA_RISK_FREE_RATE", 0.065))
        premium = _premium(choice.raw)
        greeks = BlackScholesModel.greeks(choice.right, spot, choice.strike, dte, rate, iv, premium=premium)
        if greeks is not None:
            target_delta = float(_cfg("ICICI_INDEX_OPTION_TARGET_ABS_DELTA", 0.45))
            delta_band = max(0.01, float(_cfg("ICICI_SESSION_BOOK_DELTA_RESELECT_BAND", 0.18)))
            if abs(abs(greeks.delta) - target_delta) > delta_band:
                return None, "session_contract_delta_drift"
    funds = max(0.0, safe_float(available_funds, 0.0))
    max_fraction = clamp(safe_float(_cfg("ICICI_OPTION_MAX_FUNDS_FRACTION_PER_TRADE", 0.42), 0.42), 0.01, 1.0)
    cash_buffer = max(0.0, safe_float(_cfg("ICICI_OPTION_MIN_CASH_BUFFER_INR", 0.0), 0.0))
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
        "icici_underlying_desk": True,
        "contract_selector_mode": "session_preselected_execution",
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
        and str(raw.get("contract_selector_mode") or "").lower() == "session_preselected_execution"
    )


def select_contract_for_thesis(
    instrument: Any,
    thesis_side: str,
    *,
    underlying_spot: float = 0.0,
    option_quote_by_symbol: Optional[Mapping[str, Mapping[str, Any]]] = None,
    available_funds: float = 0.0,
) -> Optional[ICICIContractChoice]:
    """Score one directional execution vehicle while preparing the session book.

    The function is invoked once for CE and once for PE before live alpha
    decisions begin.  The intraday thesis only activates the preselected side;
    it does not rescan the option chain on every signal.
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
    funds = max(0.0, safe_float(available_funds, 0.0))
    cash_buffer = max(0.0, safe_float(_cfg("ICICI_OPTION_MIN_CASH_BUFFER_INR", 0.0), 0.0))
    funds_fraction = clamp(safe_float(_cfg("ICICI_OPTION_MAX_FUNDS_FRACTION_PER_TRADE", 0.42), 0.42), 0.01, 1.0)
    max_contract_cost = max(0.0, (funds - cash_buffer) * funds_fraction) if funds > 0 else 0.0
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
        prem = _premium(c, q)
        lot = _lot_size(c)
        if lot <= 0:
            # A contract without verified lot size cannot be safely selected,
            # sized or routed as an NFO execution vehicle.
            continue
        bid, ask = _bid_ask(c, q)
        require_two_sided = bool(_cfg("ICICI_SESSION_BOOK_REQUIRE_TWO_SIDED_QUOTE", True))
        if require_two_sided and not (bid > 0 and ask >= bid):
            continue
        spread_bps = ((ask - bid) / max((ask + bid) / 2.0, 1e-9) * 10000.0) if bid > 0 and ask > 0 else float("inf")
        max_spread_bps = max(1.0, safe_float(_cfg("ICICI_OPTION_MAX_SELECTION_SPREAD_BPS", 120.0), 120.0))
        if require_two_sided and spread_bps > max_spread_bps:
            continue
        bid_qty = safe_float(c.get("best_bid_quantity") or q.get("best_bid_quantity") or c.get("bid_quantity") or q.get("bid_quantity"), 0.0)
        ask_qty = safe_float(c.get("best_offer_quantity") or q.get("best_offer_quantity") or c.get("ask_quantity") or q.get("ask_quantity"), 0.0)
        min_book_lots = max(0.0, safe_float(_cfg("ICICI_OPTION_MIN_BOOK_LOTS", 1.0), 1.0))
        if require_two_sided and min(bid_qty, ask_qty) < lot * min_book_lots:
            continue
        contract_cost = prem * lot if prem > 0 else 0.0
        if funds > 0 and (contract_cost <= 0 or contract_cost > max_contract_cost):
            continue
        local_spot = safe_float(q.get("underlying_spot_price") or q.get("underlying_ltp"), 0.0) or spot
        if local_spot <= 0:
            # before live quote/underlying warmup, use strike-ladder proximity
            # instead of fabricating a price.  This keeps the candidate eligible
            # for later quote validation but discounts the score.
            local_spot = strike
        bs = BlackScholesModel.greeks(desired, local_spot, strike, dte, rate, iv, premium=prem)
        if bs:
            # A session execution vehicle must already lie within the intended
            # delta band; do not publish a CE/PE book that immediately fails its
            # own intraday revalidation at the same underlying spot.
            session_delta_band = max(0.01, float(_cfg("ICICI_SESSION_BOOK_DELTA_RESELECT_BAND", delta_band)))
            if abs(abs(bs.delta) - target_delta) > session_delta_band:
                continue
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
        live_score = 1.0 if (q or prem > 0) else 0.35
        if max_contract_cost > 0 and contract_cost > 0:
            utilization = contract_cost / max(max_contract_cost, 1e-9)
            affordability_score = clamp(1.0 - abs(utilization - 0.58) / 0.58)
        else:
            affordability_score = 0.55 if funds <= 0 else 0.0
        if bid > 0 and ask > 0:
            spread_score = clamp(1.0 - spread_bps / max_spread_bps)
            depth_score = clamp(min(bid_qty, ask_qty) / max(lot * max(1.0, min_book_lots) * 4.0, 1.0))
            liquidity_score = 0.70 * spread_score + 0.30 * depth_score
        else:
            liquidity_score = 0.0
        # Executability is a first-class criterion: Greek quality without a
        # two-sided, adequately deep quote is not a tradable contract.
        score = clamp(0.36 * bs_score + 0.14 * dte_score + 0.25 * liquidity_score + 0.10 * live_score + 0.15 * affordability_score)
        reasons = [f"thesis={side}", f"buy_{desired}", f"dte={dte:.1f}", f"strike={strike:g}"]
        if bid > 0 and ask > 0:
            reasons.extend([f"spread={spread_bps:.1f}bps", f"depth={min(bid_qty, ask_qty):.0f}"])
        if q:
            reasons.append("live_quote")
        if contract_cost > 0:
            reasons.append(f"cost={contract_cost:.0f}")
        if max_contract_cost > 0:
            reasons.append(f"funds_fit={contract_cost:.0f}/{max_contract_cost:.0f}")
        if bs:
            reasons.extend([f"delta={delta:+.2f}", f"theta/prem={theta:.2%}"])
        enriched = dict(c)
        enriched.setdefault("runtime_lot_size", lot)
        enriched.setdefault("selected_entry_premium", prem)
        enriched.setdefault("selected_contract_cost", contract_cost)
        choices.append(ICICIContractChoice(score, normalise_symbol(raw.get("stock_code") or raw.get("underlying") or ""), side, symbol, desired, strike, str(c.get("expiry_date") or c.get("ExpiryDate") or ""), dte, delta, theta, mon, tuple(reasons), enriched))
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
    raw["runtime_lot_size"] = c.get("runtime_lot_size") or _lot_size(c)
    raw["selected_entry_premium"] = c.get("selected_entry_premium")
    raw["selected_contract_cost"] = c.get("selected_contract_cost")

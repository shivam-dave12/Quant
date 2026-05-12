"""
execution/instrument_registry.py — live catalog discovery and filtering
======================================================================

The registry never creates synthetic executable contracts. It reads venue
catalogs first, normalises only contracts returned by the exchange, then builds
a cross-venue universe. Explicit asset intents are optional overlays, not the
default universe.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from core.instruments import (
    AssetClass, AssetIntent, ExchangeInstrument, ExchangeName, TradableInstrument,
    configured_asset_intents, first_positive, normalise_symbol, slash_symbol,
)
from agents.icici_chain_architect import build_underlying_payload

logger = logging.getLogger(__name__)


def _safe_float(v, default: float = 0.0) -> float:
    try:
        f = float(v)
        return f if f > 0 else default
    except Exception:
        return default


def _safe_int(v, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _parse_icici_expiry(value: str) -> datetime | None:
    """Parse common ICICI security-master expiry formats without assuming one schema."""
    raw = str(value or "").strip()
    if not raw:
        return None
    raw2 = raw.replace("/", "-").replace(".", "-").strip()
    fmts = (
        "%Y-%m-%d", "%d-%m-%Y", "%d-%b-%Y", "%d%b%Y", "%d-%B-%Y",
        "%Y%m%d", "%d%m%Y", "%d-%m-%y", "%d%b%y",
    )
    for fmt in fmts:
        try:
            return datetime.strptime(raw2.upper(), fmt).replace(tzinfo=timezone.utc)
        except Exception:
            pass
    try:
        # Some files expose Excel serial dates.
        f = float(raw2)
        if f > 20000:
            return datetime(1899, 12, 30, tzinfo=timezone.utc) + __import__("datetime").timedelta(days=f)
    except Exception:
        pass
    return None


def _icici_dte(expiry: str, now: datetime | None = None) -> float:
    dt = _parse_icici_expiry(expiry)
    if dt is None:
        return 9999.0
    now = now or datetime.now(timezone.utc)
    return max(0.0, (dt - now).total_seconds() / 86400.0)


def _icici_option_desk_id(inst: ExchangeInstrument) -> str:
    raw = inst.raw or {}
    text = normalise_symbol(" ".join(str(raw.get(k, "")) for k in (
        "option_kind", "product_type", "ProductType", "InstrumentType", "InstrumentName",
        "Series", "segment", "Segment", "exchange_code", "ExchangeCode",
        "TradingSymbol", "Symbol", "SecurityName", "Security Name",
    )))
    # Do not use a hardcoded underlying allow-list.  Desk identity comes from
    # venue/product metadata: BFO/OPTIDX are index derivatives; OPTSTK is stock.
    if "OPTIDX" in text or "INDEXOPTION" in text or "BFO" in text:
        return "ICICI_INDEX_OPTIONS"
    return "ICICI_STOCK_OPTIONS"


def _icici_option_chain_quality(rows: list[tuple[ExchangeInstrument, str, str, float, float]]) -> float:
    """Score an ICICI underlying from its *own* option-chain structure.

    This is a large-cap/liquidity proxy without a hardcoded stock list: large,
    institutionally tradable underlyings tend to have richer strike ladders,
    both call/put sides, multiple expiries and valid near-term contracts.  Live
    Breeze quotes later confirm executable liquidity before runtime trading.
    """
    if not rows:
        return 0.0
    expiries = {r[1] for r in rows}
    rights = {r[2] for r in rows}
    strikes = {round(r[3], 4) for r in rows}
    nearest = min((r[4] for r in rows), default=9999.0)
    row_score = min(1.0, len(rows) / 160.0)
    expiry_score = min(1.0, len(expiries) / 6.0)
    rights_score = 1.0 if {"C", "P"}.issubset(rights) else 0.45
    strike_score = min(1.0, len(strikes) / 40.0)
    near_score = max(0.0, 1.0 - nearest / 35.0)
    return 0.30 * row_score + 0.22 * expiry_score + 0.20 * rights_score + 0.20 * strike_score + 0.08 * near_score


def _select_icici_options_for_runtime(options: list[ExchangeInstrument]) -> list[ExchangeInstrument]:
    """Return ICICI *underlying desk assets*, not individual option strikes.

    Institutional design:
      1. Evaluate the full Security Master option universe.
      2. Group contracts by true underlying and desk (index-options/stock-options).
      3. Rank underlyings by option-chain depth/coverage/near-term availability.
      4. Expose one runtime asset per underlying; keep the viable chain in raw
         metadata for post-thesis strike/expiry/right selection.

    This intentionally avoids treating NIFTY_19MAY2026_22800_C and
    NIFTY_19MAY2026_22900_C as two independent strategy desks.  NIFTY is the
    strategy desk asset; the call/put/strike/expiry is selected after the
    underlying chart creates an entry thesis.
    """
    try:
        import config as _cfg  # type: ignore
        index_quota = max(0, int(getattr(_cfg, "DESK_ICICI_INDEX_OPTIONS_MAX_ACTIVE", 10)))
        stock_quota = max(0, int(getattr(_cfg, "DESK_ICICI_STOCK_OPTIONS_MAX_ACTIVE", 10)))
        min_dte = float(getattr(_cfg, "ICICI_OPTION_MIN_DTE", 2.0))
        max_dte = float(getattr(_cfg, "ICICI_OPTION_MAX_DTE", 21.0))
    except Exception:
        index_quota, stock_quota, min_dte, max_dte = 10, 10, 2.0, 21.0

    now = datetime.now(timezone.utc)
    desk_under: dict[str, dict[str, list[ExchangeInstrument]]] = {
        "ICICI_INDEX_OPTIONS": {},
        "ICICI_STOCK_OPTIONS": {},
    }
    rejected_bad_metadata = 0
    for inst in options:
        raw = inst.raw or {}
        underlying = normalise_symbol(raw.get("stock_code") or raw.get("underlying") or raw.get("Underlying") or inst.asset_id)
        expiry = str(raw.get("expiry_date") or raw.get("expiry") or raw.get("ExpiryDate") or raw.get("Expiry") or "")
        right_raw = normalise_symbol(raw.get("right") or raw.get("option_type") or raw.get("OptionType") or "")
        right = "C" if right_raw in {"C", "CE", "CALL"} else "P" if right_raw in {"P", "PE", "PUT"} else ""
        strike = _safe_float(raw.get("strike_price") or raw.get("strike") or raw.get("StrikePrice") or raw.get("Strike"), 0.0)
        dte = _icici_dte(expiry, now)
        structural = {normalise_symbol(str(raw.get(k, ""))) for k in (
            "exchange_code", "ExchangeCode", "Exchange", "segment", "Segment", "product_type",
            "ProductType", "Product", "InstrumentType", "InstrumentName", "right", "option_type",
            "OptionType", "strike_price", "StrikePrice", "expiry_date", "ExpiryDate",
        )}
        structural.update({"BSE", "NSE", "NFO", "BFO", "OPTION", "OPTIONS", "OPTIDX", "OPTSTK", "C", "P", "CE", "PE", "CALL", "PUT"})
        if not underlying or underlying in structural or not expiry or not right or strike <= 0 or dte < min_dte or dte > max_dte:
            rejected_bad_metadata += 1
            continue
        desk = _icici_option_desk_id(inst)
        desk_under.setdefault(desk, {}).setdefault(underlying, []).append(inst)

    selected_underlyings: list[ExchangeInstrument] = []
    diagnostics: dict[str, dict[str, int | float]] = {}
    for desk, quota in (("ICICI_INDEX_OPTIONS", index_quota), ("ICICI_STOCK_OPTIONS", stock_quota)):
        under_map = desk_under.get(desk, {})
        if quota <= 0 or not under_map:
            diagnostics[desk] = {"underlyings": len(under_map), "selected_underlyings": 0, "contracts_indexed": sum(len(v) for v in under_map.values())}
            continue
        scored: list[tuple[float, str, list[ExchangeInstrument]]] = []
        for underlying, rows in under_map.items():
            # Reuse the existing chain quality model. It is data-derived: chain
            # breadth, expiries, strikes, both rights and near-term coverage.
            tuples = []
            for inst in rows:
                raw = inst.raw or {}
                exp = str(raw.get("expiry_date") or raw.get("ExpiryDate") or "")
                rr = normalise_symbol(raw.get("right") or raw.get("option_type") or raw.get("OptionType") or "")
                right = "C" if rr in {"C", "CE", "CALL"} else "P" if rr in {"P", "PE", "PUT"} else ""
                strike = _safe_float(raw.get("strike_price") or raw.get("StrikePrice") or raw.get("Strike"), 0.0)
                tuples.append((inst, exp, right, strike, _icici_dte(exp, now)))
            q = _icici_option_chain_quality(tuples)
            scored.append((q, underlying, rows))
        scored.sort(reverse=True, key=lambda x: x[0])

        for quality, underlying, rows in scored[:quota]:
            # One runtime instrument per underlying.  The full viable chain for
            # that underlying remains attached for later option selection.
            raw = build_underlying_payload(underlying, desk, rows)
            raw["underlying_chain_quality_score"] = quality
            sample = rows[0]
            ei = ExchangeInstrument(
                exchange=ExchangeName.ICICI,
                symbol=normalise_symbol(underlying),
                ws_symbol=normalise_symbol(underlying),
                display_symbol=normalise_symbol(underlying),
                asset_id=normalise_symbol(underlying),
                asset_class=AssetClass.OPTION,
                product_id=None,
                quote_asset="INR",
                base_asset=normalise_symbol(underlying),
                contract_type="option_chain",
                status="active",
                tick_size=sample.tick_size,
                lot_step=sample.lot_step,
                min_qty=sample.min_qty,
                raw=raw,
            )
            selected_underlyings.append(ei)
        diagnostics[desk] = {
            "underlyings": len(under_map),
            "selected_underlyings": min(quota, len(scored)),
            "contracts_indexed": sum(len(v) for v in under_map.values()),
            "quota": quota,
        }

    logger.info(
        "ICICI underlying-desk candidate build: contracts=%d selected_underlyings=%d rejected_bad_metadata=%d index=%s stock=%s",
        len(options), len(selected_underlyings), rejected_bad_metadata,
        diagnostics.get("ICICI_INDEX_OPTIONS", {}), diagnostics.get("ICICI_STOCK_OPTIONS", {}),
    )
    return selected_underlyings

def _deep_first_float(obj, names) -> float:
    """Find the first positive numeric field in a nested exchange payload."""
    names_l = {str(n).lower() for n in names}
    if isinstance(obj, dict):
        for k, v in obj.items():
            if str(k).lower() in names_l:
                f = _safe_float(v)
                if f > 0:
                    return f
        for v in obj.values():
            f = _deep_first_float(v, names_l)
            if f > 0:
                return f
    elif isinstance(obj, list):
        for v in obj:
            f = _deep_first_float(v, names_l)
            if f > 0:
                return f
    return 0.0


def _pick(row: dict, *names: str) -> str:
    wanted = {n.lower().replace(" ", "").replace("_", "") for n in names}
    for k, v in (row or {}).items():
        nk = str(k).lower().replace(" ", "").replace("_", "")
        if nk in wanted and v not in (None, ""):
            return str(v)
    return ""


def _icici_right(value: str) -> str:
    n = normalise_symbol(value)
    if n in {"CE", "CALL", "C"}:
        return "Call"
    if n in {"PE", "PUT", "P"}:
        return "Put"
    return str(value or "")


def _is_icici_option_row(row: dict, ex_code: str, product: str, right: str, strike: str, expiry: str) -> bool:
    text = normalise_symbol(" ".join(str(x) for x in [ex_code, product, right, strike, expiry, *list((row or {}).values())[:8]]))
    if any(x in text for x in ("OPTIDX", "OPTSTK", "OPTION", "OPTIONS")):
        return True
    if normalise_symbol(right) in {"CE", "PE", "CALL", "PUT", "C", "P"} and _safe_float(strike) > 0 and bool(str(expiry).strip()):
        return True
    return False

def _market_key_like(value: str) -> bool:
    n = normalise_symbol(str(value))
    if len(n) < 5:
        return False
    return n.endswith(("USDT", "USD", "INR"))


def _strip_quote(value: str) -> tuple[str, str]:
    n = normalise_symbol(value)
    for q in ("USDT", "USD", "INR"):
        if n.endswith(q) and len(n) > len(q):
            return n[:-len(q)], q
    return n, ""


def _commodity_family_from_symbol(symbol: str, base: str = "") -> str:
    """Classify venue-returned tokens into commodity families.

    This is taxonomy, not an approved-symbol list: instruments still have to be
    returned/validated by the exchange. It recognises commodity roots such as
    XAU/XAUT/gold, SLV/XAG/silver and CL/WTI/Brent/oil even when the exchange
    labels every contract as a generic future.
    """
    sym_base, _ = _strip_quote(symbol)
    b = normalise_symbol(base) or sym_base
    joined = f"{sym_base} {b}"
    if any(x in joined for x in ("XAUT", "XAU", "PAXG", "GOLD")):
        return "gold"
    if any(x in joined for x in ("SLVON", "SLV", "XAG", "SILVER")):
        return "silver"
    if any(x in joined for x in ("CLUS", "CL", "WTI", "BRENT", "CRUDE", "OIL")):
        return "crude_oil"
    if any(x in joined for x in ("NATGAS", "NATURALGAS", "NGAS")):
        return "natural_gas"
    return ""


def _is_delta_tokenised_equity(symbol: str, base: str, row: dict) -> bool:
    """Detect Delta xStock-style contracts without misclassifying AVAXUSD.

    Delta US stock derivatives usually look like BASE + XUSD where BASE is the
    equity ticker, e.g. AAPLXUSD and base AAPL. Crypto like AVAXUSD is BASE+USD,
    not BASE+XUSD.
    """
    nsym = normalise_symbol(symbol)
    nbase = normalise_symbol(base)
    if nbase and nsym == f"{nbase}XUSD":
        return True
    text = normalise_symbol(" ".join(str(v) for v in (row or {}).values() if not isinstance(v, (dict, list))))
    return any(tag in text for tag in ("XSTOCK", "TOKENIZEDSTOCK", "STOCK", "EQUITY", "SHARE"))


def _asset_default_max_leverage(asset_class: AssetClass) -> float:
    # No inferred leverage caps. If a live product row does not publish leverage,
    # the runtime policy treats the product as cash/1x until the venue confirms a
    # higher cap. This prevents hidden BTC/xStock assumptions from entering order
    # sizing or exchange leverage calls.
    return 0.0


def _asset_class_from_text(text: str, default: AssetClass = AssetClass.CRYPTO) -> AssetClass:
    n = normalise_symbol(text)
    if any(x in n for x in ("OPTION", "OPTIONS", "OPT")):
        return AssetClass.OPTION
    if any(x in n for x in ("FUTURE", "FUTURES", "FUT")):
        return AssetClass.FUTURE
    if any(x in n for x in ("INDEX", "NIFTY", "SENSEX", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY")):
        return AssetClass.INDEX
    if any(x in n for x in ("GOLD", "SILVER", "CRUDE", "BRENT", "NATURALGAS", "NATGAS", "PAXG", "XAUT", "SLVON", "XAU", "XAG", "OIL")):
        return AssetClass.COMMODITY
    if any(x in n for x in ("EQUITY", "STOCK", "SHARE", "XSTOCK")):
        return AssetClass.EQUITY
    return default


def _infer_delta_asset_class(row: dict, symbol: str, base: str) -> AssetClass:
    # Venue catalogs often label everything as futures. Desk identity must come
    # from instrument identity first, then text metadata.
    if _is_delta_tokenised_equity(symbol, base, row):
        return AssetClass.EQUITY
    if _commodity_family_from_symbol(symbol, base):
        return AssetClass.COMMODITY
    text = normalise_symbol(" ".join(str(v) for v in (row or {}).values() if not isinstance(v, (dict, list))))
    nsym = normalise_symbol(symbol)
    nbase = normalise_symbol(base)
    ac = _asset_class_from_text(f"{text} {nbase} {nsym}", AssetClass.CRYPTO)
    if ac == AssetClass.FUTURE:
        # 'future' describes contract form, not desk family. If the symbol is not
        # a commodity/equity/index, it stays a crypto perpetual/future.
        return AssetClass.CRYPTO
    return ac


def _canonical_asset_id(inst: ExchangeInstrument) -> str:
    """Canonical key for cross-venue grouping without enumerating a fixed basket."""
    sym = normalise_symbol(inst.symbol)
    base = normalise_symbol(inst.base_asset or inst.asset_id or sym)
    if inst.asset_class == AssetClass.COMMODITY:
        fam = _commodity_family_from_symbol(inst.symbol, inst.base_asset)
        return fam.upper() if fam else (base or sym)
    if inst.asset_class == AssetClass.FUTURE:
        return sym or base
    if inst.asset_class == AssetClass.EQUITY:
        if base.endswith("X") and len(base) > 1:
            return base[:-1]
        if sym.endswith("XUSD") and len(sym) > 4:
            return sym[:-4]
    if inst.asset_class == AssetClass.OPTION:
        raw = inst.raw or {}
        stock = normalise_symbol(raw.get("stock_code") or raw.get("underlying") or base)
        expiry = normalise_symbol(raw.get("expiry_date") or raw.get("expiry") or raw.get("expiryDate") or "")
        right = normalise_symbol(raw.get("right") or raw.get("option_type") or raw.get("optionType") or "")
        strike = normalise_symbol(str(raw.get("strike_price") or raw.get("strike") or ""))
        parts = [x for x in (stock, expiry, strike, right[:1]) if x]
        return "_".join(parts) if parts else sym or base
    for quote in ("USDT", "USD", "INR"):
        if sym.endswith(quote) and len(sym) > len(quote):
            return sym[:-len(quote)]
    return base or sym


def _display_name_for(inst: ExchangeInstrument, asset_id: str) -> str:
    if inst.asset_class == AssetClass.OPTION:
        raw = inst.raw or {}
        right = str(raw.get("right") or raw.get("option_type") or "").strip()
        strike = str(raw.get("strike_price") or raw.get("strike") or "").strip()
        expiry = str(raw.get("expiry_date") or raw.get("expiry") or "").strip()
        stock = str(raw.get("stock_code") or raw.get("underlying") or asset_id).strip()
        bits = [stock, expiry, strike, right]
        return " ".join(x for x in bits if x)
    return str(inst.display_symbol or asset_id)


def _parse_csv_set(value: str | Iterable[str] | None) -> Set[str]:
    if value is None:
        return set()
    if isinstance(value, str):
        raw = value.replace(";", ",").split(",")
    else:
        raw = list(value)
    return {str(x).strip().lower() for x in raw if str(x).strip()}


def _parse_csv_symbols(value: str | Iterable[str] | None) -> Set[str]:
    if value is None:
        return set()
    if isinstance(value, str):
        raw = value.replace(";", ",").split(",")
    else:
        raw = list(value)
    return {normalise_symbol(str(x)) for x in raw if normalise_symbol(str(x))}


def _icici_structural_tokens(row: dict, ex_code: str = "", product: str = "", right: str = "", strike: str = "", expiry: str = "") -> Set[str]:
    """Return row-local metadata tokens that cannot be an underlying.

    This is not a symbol allow-list. It only prevents corrupt security-master
    rows such as underlying=BSE/NFO/OPTIDX/CE/5200 from entering the universe.
    """
    vals = {ex_code, product, right, strike, expiry}
    for name in (
        "ExchangeCode", "exchange_code", "Exchange", "exchange", "Segment", "ProductType",
        "product_type", "InstrumentType", "Instrument Name", "InstrumentName", "Series",
        "Right", "OptionType", "CallPut", "StrikePrice", "Strike", "ExpiryDate", "Expiry",
    ):
        v = _pick(row, name)
        if v:
            vals.add(v)
    out = {normalise_symbol(str(x)) for x in vals if normalise_symbol(str(x))}
    out.update({"OPTION", "OPTIONS", "CALL", "PUT", "CE", "PE", "C", "P"})
    return out


def _icici_option_kind(row: dict, ex_code: str, product: str) -> str:
    text = normalise_symbol(" ".join(str(x) for x in [
        ex_code, product,
        _pick(row, "InstrumentType", "InstrumentName", "Instrument Name", "Series", "Segment", "ProductType"),
        _pick(row, "TradingSymbol", "Symbol", "SecurityName", "Security Name"),
    ]))
    if "OPTIDX" in text or "INDEXOPTION" in text or "INDEXOPTIONS" in text:
        return "index"
    if "OPTSTK" in text or "STOCKOPTION" in text or "STOCKOPTIONS" in text:
        return "stock"
    if normalise_symbol(ex_code) == "BFO":
        return "index"
    return "stock"


def _derive_icici_underlying(row: dict, stock: str, ex_code: str, product: str, symbol: str) -> str:
    """Derive underlying from ICICI row fields without approved-symbol lists."""
    structural = _icici_structural_tokens(row, ex_code, product, "", "", "")
    candidates: list[str] = []
    for name in (
        "Underlying", "UnderlyingSymbol", "UnderlyingName", "Underlying Asset", "underlying",
        "StockCode", "stock_code", "CompanyName", "Company Name", "ScripName",
        "SC_SYMBOL", "SC_NAME", "ShortName", "SecurityName", "Security Name",
    ):
        v = _pick(row, name)
        if v:
            candidates.append(v)
    if stock:
        candidates.append(stock)
    if symbol:
        candidates.append(symbol)
    for c in candidates:
        n = normalise_symbol(str(c))
        if not n or n in structural:
            continue
        if _market_key_like(n) or any(tok in n for tok in ("CE", "PE", "CALL", "PUT", "OPTIDX", "OPTSTK")):
            continue
        return n
    return ""


def _allow_value(value: str, allowed: Set[str]) -> bool:
    return not allowed or "all" in allowed or str(value or "").lower() in allowed


def _asset_default_step(asset_class: AssetClass, symbol: str) -> float:
    # Do not let BTC's 0.001 contract convention leak into xStock/RWA contracts.
    # If Delta omits size/contract_value fields, non-crypto token contracts are
    # treated as integer-contract products until the live product row says otherwise.
    if asset_class in (AssetClass.EQUITY, AssetClass.COMMODITY, AssetClass.INDEX):
        return 1.0
    return 0.0


def _unwrap_list(resp) -> List[dict]:
    if not isinstance(resp, dict):
        return []
    data = resp.get("result", resp.get("data", resp))
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        for key in ("result", "products", "instruments", "symbols", "data", "ticker_data"):
            v = data.get(key)
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]
        # Some CoinSwitch endpoints return {exchange: {symbol: specs}}.
        # Guard this strictly: ticker payloads also contain dicts with fields such
        # as lowPrice24h/highPrice24h. Those field names are NOT symbols.
        rows: List[dict] = []
        for ex_val in data.values():
            if isinstance(ex_val, dict):
                for sym, spec in ex_val.items():
                    if not _market_key_like(sym):
                        continue
                    row = dict(spec) if isinstance(spec, dict) else {}
                    row.setdefault("symbol", sym)
                    rows.append(row)
        return rows
    return []


def _unwrap_one(resp) -> Optional[dict]:
    """Return one market-data row from mixed exchange response shapes."""
    rows = _unwrap_list(resp)
    if rows:
        return rows[0]
    if isinstance(resp, dict):
        data = resp.get("result", resp.get("data"))
        if isinstance(data, dict):
            return data
    return None


def _ordered_aliases(intent: AssetIntent) -> List[str]:
    """Preserve config priority; sets are unsafe for choosing among PAXG/XAUT etc."""
    raw = [intent.asset_id, intent.display_name, *list(intent.aliases or ())]
    out: List[str] = []
    seen = set()
    for x in raw:
        n = normalise_symbol(str(x))
        if n and n not in seen:
            out.append(n); seen.add(n)
    return out


def _row_symbol(row: dict) -> str:
    for k in ("symbol", "s", "m", "market", "pair", "product_symbol", "instrument", "instrument_name"):
        v = row.get(k)
        if v:
            return str(v).upper()
    # Common CoinSwitch nested payloads: {"exchange": "EXCHANGE_2", "data": {"BTCUSDT": {...}}}
    for k, v in row.items():
        if isinstance(v, dict) and normalise_symbol(str(k)).endswith(("USDT", "USD", "INR")):
            return str(k).upper()
    return ""


@dataclass
class DiscoveryReport:
    requested: List[AssetIntent] = field(default_factory=list)
    matched: List[TradableInstrument] = field(default_factory=list)
    unavailable: Dict[str, str] = field(default_factory=dict)
    raw_counts: Dict[str, int] = field(default_factory=dict)

    def terminal_lines(self) -> List[str]:
        lines = ["📡 MULTI-ASSET LIVE CATALOG DISCOVERY"]
        lines.append("   raw products: " + ", ".join(f"{k}={v}" for k, v in self.raw_counts.items()))
        if self.matched:
            lines.append("   activated:")
            for inst in self.matched:
                exs = ", ".join(f"{ex.value}:{ei.display_symbol}" for ex, ei in inst.by_exchange.items())
                lines.append(f"     ✅ {inst.asset_id:<8} primary={inst.primary_exchange.value:<10} {exs}")
        if self.unavailable:
            lines.append("   unavailable / skipped:")
            for aid, reason in self.unavailable.items():
                lines.append(f"     ⚪ {aid:<8} {reason}")
        return lines

    def telegram_html(self) -> str:
        def esc(x):
            return str(x).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        parts = ["📡 <b>MULTI-ASSET LIVE CATALOG DISCOVERY</b>"]
        parts.append("Raw products: " + esc(", ".join(f"{k}={v}" for k, v in self.raw_counts.items())))
        if self.matched:
            parts.append("\n<b>Activated:</b>")
            for inst in self.matched:
                parts.append(f"✅ <b>{esc(inst.asset_id)}</b> — primary {esc(inst.primary_exchange.value.upper())} / {esc(inst.display_symbol)}")
        if self.unavailable:
            parts.append("\n<b>Unavailable / skipped:</b>")
            for aid, reason in self.unavailable.items():
                parts.append(f"⚪ <b>{esc(aid)}</b> — {esc(reason)}")
        return "\n".join(parts)


def _discovery_report_terminal_lines(self: DiscoveryReport, preview: int = 80) -> List[str]:
    lines = ["MULTI-ASSET LIVE CATALOG DISCOVERY"]
    lines.append("   raw products: " + ", ".join(f"{k}={v}" for k, v in self.raw_counts.items()))
    if self.matched:
        shown = self.matched[:max(0, int(preview))]
        lines.append(f"   discovered: {len(self.matched)} instruments; preview={len(shown)}")
        for inst in shown:
            exs = ", ".join(f"{ex.value}:{ei.display_symbol}" for ex, ei in inst.by_exchange.items())
            lines.append(f"     OK {inst.asset_id:<18} {inst.asset_class.value:<9} primary={inst.primary_exchange.value:<10} {exs}")
        if len(self.matched) > len(shown):
            lines.append(f"     ... {len(self.matched) - len(shown)} more instruments in discovery report")
    if self.unavailable:
        lines.append("   unavailable / skipped:")
        for aid, reason in self.unavailable.items():
            lines.append(f"     SKIP {aid:<18} {reason}")
    return lines


def _discovery_report_telegram_html(self: DiscoveryReport, preview: int = 60) -> str:
    def esc(x):
        return str(x).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    parts = ["<b>MULTI-ASSET LIVE CATALOG DISCOVERY</b>"]
    parts.append("Raw products: " + esc(", ".join(f"{k}={v}" for k, v in self.raw_counts.items())))
    if self.matched:
        shown = self.matched[:max(0, int(preview))]
        parts.append(f"\n<b>Discovered:</b> {len(self.matched)} instruments; showing {len(shown)}")
        for inst in shown:
            parts.append(f"OK <b>{esc(inst.asset_id)}</b> - {esc(inst.asset_class.value)} - primary {esc(inst.primary_exchange.value.upper())} / {esc(inst.display_symbol)}")
        if len(self.matched) > len(shown):
            parts.append(f"... {len(self.matched) - len(shown)} more instruments")
    if self.unavailable:
        parts.append("\n<b>Unavailable / skipped:</b>")
        for aid, reason in self.unavailable.items():
            parts.append(f"SKIP <b>{esc(aid)}</b> - {esc(reason)}")
    return "\n".join(parts)


DiscoveryReport.terminal_lines = _discovery_report_terminal_lines  # type: ignore[method-assign]
DiscoveryReport.telegram_html = _discovery_report_telegram_html  # type: ignore[method-assign]


class InstrumentRegistry:
    def __init__(self, execution_preference: str = "delta") -> None:
        try:
            self.execution_preference = ExchangeName(str(execution_preference).lower())
        except Exception:
            self.execution_preference = ExchangeName.DELTA
        if str(execution_preference).lower() in {"hyperliquid", "hl", "hyper"}:
            self.execution_preference = ExchangeName.HYPERLIQUID
        self.hyperliquid: Dict[str, ExchangeInstrument] = {}
        self.delta: Dict[str, ExchangeInstrument] = {}
        self.coinswitch: Dict[str, ExchangeInstrument] = {}
        self.icici: Dict[str, ExchangeInstrument] = {}
        self.report = DiscoveryReport()

    # ──────────────────────────────────────────────────────────────────────
    # Exchange catalog fetchers
    # ──────────────────────────────────────────────────────────────────────
    def load_hyperliquid(self, api) -> Dict[str, ExchangeInstrument]:
        out: Dict[str, ExchangeInstrument] = {}
        if api is None:
            self.hyperliquid = out
            return out
        try:
            resp = api.get_products()
            rows = _unwrap_list(resp)
            for p in rows:
                sym = normalise_symbol(p.get("symbol") or p.get("name") or "")
                if not sym or sym.startswith("#"):
                    continue
                base = normalise_symbol(p.get("base") or sym)
                sz_dec = _safe_int(p.get("szDecimals"), 4)
                lot_step = 10 ** (-max(0, min(8, sz_dec)))
                try:
                    import config as _cfg_mod  # type: ignore
                    default_lev = getattr(_cfg_mod, "HYPERLIQUID_DEFAULT_MAX_LEVERAGE", 40.0)
                except Exception:
                    default_lev = 40.0
                ei = ExchangeInstrument(
                    exchange=ExchangeName.HYPERLIQUID,
                    symbol=sym,
                    ws_symbol=sym,
                    display_symbol=f"{sym}-PERP",
                    asset_id=base,
                    asset_class=AssetClass.CRYPTO,
                    quote_asset=str(p.get("quote") or "USDC"),
                    base_asset=base,
                    contract_type="perpetual_futures",
                    status=str(p.get("status") or "active"),
                    tick_size=first_positive(_safe_float(p.get("tick_size")), 0.01),
                    lot_step=lot_step,
                    min_qty=lot_step,
                    max_leverage=first_positive(_safe_float(p.get("maxLeverage")), _safe_float(p.get("max_leverage")), _safe_float(default_lev)),
                    raw=p,
                )
                out[sym] = ei
        except Exception as e:
            logger.warning("Hyperliquid product discovery failed: %s", e, exc_info=True)
        self.hyperliquid = out
        return out

    def load_delta(self, api) -> Dict[str, ExchangeInstrument]:
        out: Dict[str, ExchangeInstrument] = {}
        if api is None:
            return out
        try:
            resp = api.get_products(contract_types=["perpetual_futures", "futures"])
            rows = _unwrap_list(resp)
            for p in rows:
                sym = str(p.get("symbol") or p.get("product_symbol") or "").upper()
                if not sym:
                    continue
                base = str((p.get("underlying_asset") or {}).get("symbol") if isinstance(p.get("underlying_asset"), dict) else p.get("underlying_asset") or p.get("base_asset") or "").upper()
                quote = str((p.get("quoting_asset") or {}).get("symbol") if isinstance(p.get("quoting_asset"), dict) else p.get("quoting_asset") or p.get("quote_asset") or "").upper()
                tick = first_positive(
                    _safe_float(p.get("tick_size")),
                    _safe_float(p.get("price_increment")),
                    _safe_float(p.get("minimum_tick_size")),
                )
                inferred_class = _infer_delta_asset_class(p, sym, base)

                step = first_positive(
                    _safe_float(p.get("contract_value")),
                    _safe_float(p.get("lot_size")),
                    _safe_float(p.get("size_increment")),
                    _safe_float(p.get("contract_unit")),
                    _safe_float(p.get("min_size")),
                    _asset_default_step(inferred_class, sym),
                )
                specs = p.get("product_specs") if isinstance(p.get("product_specs"), dict) else {}
                max_lev = first_positive(
                    _safe_float(p.get("max_leverage")),
                    _safe_float(p.get("maximum_leverage")),
                    _safe_float(p.get("leverage")),
                    _safe_float(specs.get("max_leverage")),
                    _safe_float(specs.get("maximum_leverage")),
                    _deep_first_float(p, ("max_leverage", "maximum_leverage", "maxLeverage")),
                    _asset_default_max_leverage(inferred_class),
                )
                ei = ExchangeInstrument(
                    exchange=ExchangeName.DELTA,
                    symbol=sym,
                    ws_symbol=sym,
                    display_symbol=sym,
                    asset_id=normalise_symbol(base or sym),
                    asset_class=inferred_class,
                    product_id=_safe_int(p.get("id") or p.get("product_id"), 0) or None,
                    quote_asset=quote,
                    base_asset=base,
                    contract_type=str(p.get("contract_type") or p.get("product_type") or ""),
                    status=str(p.get("state") or p.get("status") or "active"),
                    tick_size=tick,
                    lot_step=step,
                    min_qty=first_positive(_safe_float(p.get("min_size")), _safe_float(p.get("minimum_order_size"))),
                    max_qty=_safe_float(p.get("max_size")),
                    contract_value_btc=_safe_float(p.get("contract_value")),
                    max_leverage=max_lev,
                    raw=p,
                )
                out[normalise_symbol(sym)] = ei
        except Exception as e:
            logger.warning("Delta product discovery failed: %s", e, exc_info=True)
        commodity_count = sum(1 for x in out.values() if x.asset_class == AssetClass.COMMODITY)
        if commodity_count > 0:
            logger.info("Delta commodity products discovered from live catalog: %d", commodity_count)
        self.delta = out
        return out

    def load_coinswitch(self, api) -> Dict[str, ExchangeInstrument]:
        out: Dict[str, ExchangeInstrument] = {}
        if api is None:
            return out
        rows: List[dict] = []
        try:
            if hasattr(api, "get_instrument_info"):
                rows = _unwrap_list(api.get_instrument_info(exchange="EXCHANGE_2"))
            # CoinSwitch ticker endpoint requires an explicit symbol; never call
            # the generic ticker URL because it returns 422 "Input symbol is missing"
            # and wastes startup time.  Missing all-instrument rows are handled by
            # _augment_coinswitch_from_requested(), which probes exact symbols.
        except Exception as e:
            logger.warning("CoinSwitch instrument discovery failed: %s", e, exc_info=True)
            rows = []
        for r in rows:
            sym = _row_symbol(r)
            if not sym:
                continue
            rest_sym = normalise_symbol(sym)
            ws_sym = slash_symbol(sym)
            base = rest_sym
            quote = ""
            for q in ("USDT", "USD", "INR"):
                if rest_sym.endswith(q):
                    base, quote = rest_sym[:-len(q)], q
                    break
            c_family = _commodity_family_from_symbol(rest_sym, base)
            c_asset_class = AssetClass.COMMODITY if c_family else AssetClass.CRYPTO
            ei = ExchangeInstrument(
                exchange=ExchangeName.COINSWITCH,
                symbol=rest_sym,
                ws_symbol=ws_sym,
                display_symbol=ws_sym,
                asset_id=normalise_symbol(c_family.upper() if c_family else (base or rest_sym)),
                asset_class=c_asset_class,
                quote_asset=quote,
                base_asset=base,
                contract_type=str(r.get("contract_type") or r.get("type") or "perpetual_futures"),
                status=str(r.get("status") or r.get("state") or "active"),
                tick_size=first_positive(_safe_float(r.get("tick_size")), _safe_float(r.get("quote_precision"))),
                lot_step=first_positive(_safe_float(r.get("lot_size")), _safe_float(r.get("quantity_precision"))),
                min_qty=first_positive(_safe_float(r.get("min_qty")), _safe_float(r.get("minQuantity")), _safe_float(r.get("min_size"))),
                max_qty=first_positive(_safe_float(r.get("max_qty")), _safe_float(r.get("maxQuantity"))),
                max_leverage=first_positive(_safe_float(r.get("max_leverage")), _safe_float(r.get("leverage")), _safe_float(r.get("maxLeverage"))),
                raw=r,
            )
            out[normalise_symbol(rest_sym)] = ei
            out[normalise_symbol(ws_sym)] = ei
        self.coinswitch = out
        return out

    def load_icici(self, api, *, security_master_url: str | None = None) -> Dict[str, ExchangeInstrument]:
        out: Dict[str, ExchangeInstrument] = {}
        if api is None or not hasattr(api, "get_security_master_rows"):
            self.icici = out
            return out
        try:
            try:
                import config as _config  # type: ignore
                cache_path = getattr(_config, "ICICI_SECURITY_MASTER_CACHE_PATH", None)
            except Exception:
                cache_path = None
            try:
                rows = api.get_security_master_rows(url=security_master_url, cache_path=cache_path)
            except TypeError:
                # Older/fake clients used in tests may not accept cache_path.
                rows = api.get_security_master_rows(url=security_master_url)
            logger.info("ICICI security master rows loaded: %d", len(rows))
        except Exception as e:
            logger.warning("ICICI security master discovery failed: %s", e, exc_info=True)
            rows = []
        icici_seen = icici_option_rows = icici_added = icici_reject_structural = icici_reject_non_option = 0
        option_candidates: list[ExchangeInstrument] = []
        for r in rows:
            icici_seen += 1
            ex_raw = str(
                r.get("ExchangeCode") or r.get("exchange_code") or r.get("Exchange") or r.get("exchange") or
                r.get("Segment") or r.get("segment") or r.get("ProductType") or r.get("InstrumentType") or ""
            ).upper()
            ex_norm = normalise_symbol(ex_raw)
            if "NFO" in ex_norm:
                ex_code = "NFO"
            elif "BFO" in ex_norm:
                ex_code = "BFO"
            elif "BSE" in ex_norm:
                ex_code = "BSE"
            elif "NSE" in ex_norm:
                ex_code = "NSE"
            else:
                text_probe = normalise_symbol(" ".join(str(v) for v in list((r or {}).values())[:16]))
                if any(x in text_probe for x in ("OPTIDX", "OPTSTK", "OPTION", "CE", "PE")):
                    ex_code = "NFO"
                else:
                    continue
            try:
                import config as _cfg_mod  # type: ignore
                _icici_options_only = bool(getattr(_cfg_mod, "ICICI_OPTIONS_ONLY", True))
            except Exception:
                _icici_options_only = True
            # ICICI security-master headers vary across files/days.  Accept the
            # common NSE/NFO/BSE spellings instead of relying on one exact schema.
            stock = str(_pick(
                r, "ShortName", "StockCode", "stock_code", "CompanyName", "Company Name",
                "ScripName", "Underlying", "UnderlyingSymbol", "Symbol", "symbol",
                "Name", "SC_SYMBOL", "SC_NAME", "SecurityName", "Security Name"
            )).upper()
            token = str(_pick(
                r, "Token", "token", "ScripCode", "scrip_code", "ExchangeToken",
                "exchange_token", "TokenNumber", "Token Number", "InstrumentToken"
            ))
            product = str(_pick(
                r, "ProductType", "product_type", "InstrumentType", "instrument_type",
                "Series", "series", "InstrumentName", "Instrument Name", "InstType",
                "Segment", "Product", "ProductCode"
            ))
            right = _icici_right(_pick(
                r, "OptionType", "option_type", "Right", "right", "CallPut", "call_put",
                "Option", "OptionName", "CPType", "PutCall"
            ))
            strike = str(_pick(
                r, "StrikePrice", "strike_price", "Strike", "strike", "StrikeRate",
                "strike_rate", "Strike Price", "STRIKE_PR", "ExercisePrice"
            ))
            expiry = str(_pick(
                r, "ExpiryDate", "expiry_date", "Expiry", "expiry", "ExpiryDt",
                "expiry_dt", "Expiry Date", "EXPIRY_DATE", "ContractExpiry"
            ))
            if not stock and not token:
                continue
            icici_stock_alias = {
                "CNXBAN": "BANKNIFTY",
                "NIFTY50": "NIFTY",
                "NIFTYBANK": "BANKNIFTY",
                "SENSEX50": "SENSEX",
            }
            stock = icici_stock_alias.get(normalise_symbol(stock), stock)
            symbol_for_underlying = str(r.get("TradingSymbol") or r.get("trading_symbol") or r.get("Symbol") or stock or token)
            derived_stock = _derive_icici_underlying(r, stock, ex_code, product, symbol_for_underlying)
            if derived_stock:
                stock = icici_stock_alias.get(normalise_symbol(derived_stock), derived_stock)
            text = f"{stock} {product} {right} {strike} {expiry} {ex_code}"
            ac = AssetClass.CASH
            is_option = _is_icici_option_row(r, ex_code, product, right, strike, expiry)
            if is_option:
                icici_option_rows += 1
            if ex_code in {"NFO", "BFO"}:
                ac = AssetClass.OPTION if is_option else AssetClass.FUTURE
            elif ex_code == "BSE" and is_option:
                # BSE is the cash exchange segment. Listed BSE derivatives should
                # arrive as BFO rows. Do not manufacture an option from BSE cash
                # rows even if strike/right columns are present.
                ac = AssetClass.CASH
            elif _asset_class_from_text(text, AssetClass.EQUITY) == AssetClass.INDEX:
                ac = AssetClass.INDEX
            if ac == AssetClass.OPTION:
                nstock = normalise_symbol(stock)
                structural = _icici_structural_tokens(r, ex_code, product, right, strike, expiry)
                if bool(getattr(__import__('config'), 'ICICI_OPTION_REJECT_STRUCTURAL_UNDERLYING', True)) and (not nstock or nstock in structural):
                    icici_reject_structural += 1
                    logger.debug("ICICI option rejected corrupt structural underlying=%s ex=%s symbol=%s product=%s", stock, ex_code, symbol_for_underlying, product)
                    continue
            if _icici_options_only and ac != AssetClass.OPTION:
                # Indian-market mandate: only listed options are eligible for the
                # trading universe. Cash/future/index rows are not silently traded
                # or used as synthetic substitutes for options.
                icici_reject_non_option += 1
                continue
            symbol = normalise_symbol(
                r.get("TradingSymbol") or r.get("trading_symbol") or r.get("Symbol") or stock or token
            )
            if not symbol:
                symbol = normalise_symbol(f"{stock}{expiry}{strike}{right}")
            raw = dict(r)
            raw.setdefault("stock_code", stock)
            raw.setdefault("exchange_code", ex_code)
            raw.setdefault("product_type", "options" if ac == AssetClass.OPTION else ("futures" if ac == AssetClass.FUTURE else "cash"))
            if ac == AssetClass.OPTION:
                raw.setdefault("option_kind", _icici_option_kind(r, ex_code, product))
            raw.setdefault("right", right)
            raw.setdefault("option_type", right)
            raw.setdefault("strike_price", strike)
            raw.setdefault("expiry_date", expiry)
            inst = ExchangeInstrument(
                exchange=ExchangeName.ICICI,
                symbol=symbol,
                ws_symbol=str(token or symbol),
                display_symbol=str(symbol or stock),
                asset_id=normalise_symbol(stock or symbol),
                asset_class=ac,
                product_id=_safe_int(token, 0) or None,
                quote_asset="INR",
                base_asset=normalise_symbol(stock or symbol),
                contract_type=raw["product_type"],
                status=str(r.get("Status") or r.get("status") or "active"),
                tick_size=first_positive(_safe_float(r.get("TickSize")), _safe_float(r.get("tick_size")), 0.05),
                lot_step=first_positive(_safe_float(r.get("LotSize")), _safe_float(r.get("lot_size")), _safe_float(r.get("MinimumLotQty"))),
                min_qty=first_positive(_safe_float(r.get("LotSize")), _safe_float(r.get("lot_size"))),
                raw=raw,
            )
            option_candidates.append(inst)
        pruned = _select_icici_options_for_runtime(option_candidates)
        for inst in pruned:
            key = normalise_symbol(inst.symbol)
            if key in out and normalise_symbol(str(out[key].product_id or "")) != normalise_symbol(str(inst.product_id or "")):
                key = f"{key}_{normalise_symbol(str(inst.product_id or len(out)))}"
            out[key] = inst
        icici_added = len(out)
        logger.info(
            "ICICI options discovery summary: rows=%d option_like=%d candidates=%d added=%d rejected_structural=%d rejected_non_option=%d",
            icici_seen, icici_option_rows, len(option_candidates), icici_added, icici_reject_structural, icici_reject_non_option
        )
        if len(option_candidates) > len(out):
            logger.info(
                "ICICI chain-desk candidates retained for quote probing/runtime: %d/%d; full security master was evaluated by desk/underlying quality",
                len(out), len(option_candidates)
            )
        self.icici = out
        return out

    def _augment_coinswitch_from_requested(self, out: Dict[str, ExchangeInstrument], api, intents: List[AssetIntent]) -> Dict[str, ExchangeInstrument]:
        """Validate configured crypto symbols against CoinSwitch live ticker endpoint.

        CoinSwitch docs expose per-symbol futures ticker/orderbook/klines endpoints.
        Some accounts return only a small instrument_info subset, so BTCUSDT can be
        tradable even when the all-instrument response is incomplete.  This is not
        synthetic: a symbol is added only after CoinSwitch replies successfully for
        that exact symbol.
        """
        if api is None:
            return out
        seen = set(out.keys())
        for intent in intents:
            # CoinSwitch exact-symbol validation is allowed for crypto and
            # commodity tokens. The symbol is activated only after the exchange
            # responds with live market data for that exact token.
            if intent.asset_class not in (AssetClass.CRYPTO, AssetClass.COMMODITY):
                continue
            candidates: List[str] = []
            for a in _ordered_aliases(intent):
                if a.endswith("USDT"):
                    candidates.append(a)
            base = normalise_symbol(intent.asset_id)
            if base and f"{base}USDT" not in candidates:
                candidates.append(f"{base}USDT")
            for sym in candidates:
                if sym in seen:
                    continue
                try:
                    fn = getattr(api, "get_futures_ticker", None) or getattr(api, "get_ticker", None)
                    if not callable(fn):
                        continue
                    try:
                        resp = fn(symbol=sym, exchange="EXCHANGE_2")
                    except TypeError:
                        resp = fn(sym)
                    row = _unwrap_one(resp)
                    if not row or str(row.get("error") or ""):
                        continue
                    # Require some live-market field so a generic error wrapper cannot activate it.
                    if not any(k in row for k in ("symbol", "last_price", "lastPrice", "mark_price", "markPrice", "best_bid", "bestBid", "best_ask", "bestAsk", "funding_rate", "fundingRate", "open_interest", "openInterest")):
                        continue
                    returned_symbol = normalise_symbol(row.get("symbol") or row.get("s") or row.get("pair") or sym)
                    # Exact-symbol validation: never let ticker field names like LOWPRICE24H
                    # become activated instruments.
                    if returned_symbol and returned_symbol != normalise_symbol(sym):
                        logger.debug("CoinSwitch ticker returned %s while validating %s — ignoring", returned_symbol, sym)
                        continue
                    rest_sym = normalise_symbol(sym)
                    ws_sym = slash_symbol(rest_sym)
                    base2, quote = rest_sym, ""
                    for q in ("USDT", "USD", "INR"):
                        if rest_sym.endswith(q):
                            base2, quote = rest_sym[:-len(q)], q
                            break
                    ei = ExchangeInstrument(
                        exchange=ExchangeName.COINSWITCH, symbol=rest_sym, ws_symbol=ws_sym,
                        display_symbol=ws_sym, asset_id=normalise_symbol((_commodity_family_from_symbol(rest_sym, base2).upper() or base2)),
                        asset_class=(AssetClass.COMMODITY if _commodity_family_from_symbol(rest_sym, base2) else AssetClass.CRYPTO), quote_asset=quote, base_asset=base2,
                        contract_type="perpetual_futures", status="active", raw=row,
                    )
                    out[normalise_symbol(rest_sym)] = ei
                    out[normalise_symbol(ws_sym)] = ei
                    seen.add(normalise_symbol(rest_sym)); seen.add(normalise_symbol(ws_sym))
                    logger.info("CoinSwitch live ticker validated: %s", rest_sym)
                    break
                except Exception as e:
                    logger.debug("CoinSwitch live validation failed for %s: %s", sym, e)
        return out

    def _augment_delta_from_seed_symbols(self, out: Dict[str, ExchangeInstrument], api, seed_symbols: Iterable[str]) -> Dict[str, ExchangeInstrument]:
        if api is None:
            return out
        seen = set(out.keys())
        for raw_sym in seed_symbols or []:
            sym = normalise_symbol(str(raw_sym))
            if not sym or sym in seen:
                continue
            row = None
            try:
                if hasattr(api, "get_product"):
                    resp = api.get_product(sym)
                    row = _unwrap_one(resp)
                if not row and hasattr(api, "get_ticker"):
                    resp = api.get_ticker(sym)
                    row = _unwrap_one(resp)
                if not row:
                    continue
                returned = normalise_symbol(row.get("symbol") or row.get("product_symbol") or row.get("contract_symbol") or sym)
                if returned and returned != sym:
                    logger.debug("Delta seed %s returned %s — ignoring", sym, returned)
                    continue
                base, quote = _strip_quote(sym)
                fam = _commodity_family_from_symbol(sym, base)
                ac = AssetClass.COMMODITY if fam else _infer_delta_asset_class(row, sym, base)
                ei = ExchangeInstrument(
                    exchange=ExchangeName.DELTA, symbol=sym, ws_symbol=sym, display_symbol=sym,
                    asset_id=normalise_symbol(fam.upper() if fam else base), asset_class=ac,
                    product_id=_safe_int(row.get("id") or row.get("product_id"), 0) or None,
                    quote_asset=quote, base_asset=base,
                    contract_type=str(row.get("contract_type") or row.get("product_type") or ""),
                    status=str(row.get("state") or row.get("status") or "active"),
                    tick_size=first_positive(_safe_float(row.get("tick_size")), _safe_float(row.get("price_increment")), _safe_float(row.get("minimum_tick_size"))),
                    lot_step=first_positive(_safe_float(row.get("contract_value")), _safe_float(row.get("lot_size")), _safe_float(row.get("size_increment")), _asset_default_step(ac, sym)),
                    min_qty=first_positive(_safe_float(row.get("min_size")), _safe_float(row.get("minimum_order_size"))),
                    max_qty=_safe_float(row.get("max_size")),
                    max_leverage=first_positive(_safe_float(row.get("max_leverage")), _safe_float(row.get("maximum_leverage")), _deep_first_float(row, ("max_leverage", "maximum_leverage", "maxLeverage"))),
                    raw=row,
                )
                out[sym] = ei
                seen.add(sym)
                logger.info("Delta exact-symbol discovery validated: %s (%s)", sym, ac.value)
            except Exception as e:
                logger.debug("Delta exact-symbol validation failed for %s: %s", sym, e)
        return out

    def _augment_coinswitch_from_seed_symbols(self, out: Dict[str, ExchangeInstrument], api, seed_symbols: Iterable[str]) -> Dict[str, ExchangeInstrument]:
        if api is None:
            return out
        seen = set(out.keys())
        for raw_sym in seed_symbols or []:
            sym = normalise_symbol(str(raw_sym))
            if not sym or sym in seen:
                continue
            try:
                fn = getattr(api, "get_futures_ticker", None) or getattr(api, "get_ticker", None)
                if not callable(fn):
                    continue
                try:
                    resp = fn(symbol=sym, exchange="EXCHANGE_2")
                except TypeError:
                    resp = fn(sym)
                row = _unwrap_one(resp)
                if not row or str(row.get("error") or ""):
                    continue
                if not any(k in row for k in ("symbol", "last_price", "lastPrice", "mark_price", "markPrice", "best_bid", "bestBid", "best_ask", "bestAsk", "open_interest", "openInterest")):
                    continue
                returned = normalise_symbol(row.get("symbol") or row.get("s") or row.get("pair") or sym)
                if returned and returned != sym:
                    logger.debug("CoinSwitch seed %s returned %s — ignoring", sym, returned)
                    continue
                base, quote = _strip_quote(sym)
                fam = _commodity_family_from_symbol(sym, base)
                ac = AssetClass.COMMODITY if fam else AssetClass.CRYPTO
                ws_sym = slash_symbol(sym)
                ei = ExchangeInstrument(
                    exchange=ExchangeName.COINSWITCH, symbol=sym, ws_symbol=ws_sym, display_symbol=ws_sym,
                    asset_id=normalise_symbol(fam.upper() if fam else base), asset_class=ac,
                    quote_asset=quote, base_asset=base, contract_type="perpetual_futures", status="active", raw=row,
                )
                out[sym] = ei; out[normalise_symbol(ws_sym)] = ei
                seen.add(sym); seen.add(normalise_symbol(ws_sym))
                logger.info("CoinSwitch exact-symbol discovery validated: %s (%s)", sym, ac.value)
            except Exception as e:
                logger.debug("CoinSwitch exact-symbol validation failed for %s: %s", sym, e)
        return out

    # ──────────────────────────────────────────────────────────────────────
    # Matching
    # ──────────────────────────────────────────────────────────────────────
    def discover(self, delta_api=None, coinswitch_api=None, hyperliquid_api=None, requested=None,
                 max_active: int = 0, require_primary: bool = True,
                 discovery_mode: str = "dynamic", include_asset_classes=None,
                 include_exchanges=None, icici_api=None,
                 icici_security_master_url: str | None = None) -> DiscoveryReport:
        mode = str(discovery_mode or "dynamic").lower()
        intents = configured_asset_intents(requested)
        include_classes = _parse_csv_set(include_asset_classes)
        include_exs = _parse_csv_set(include_exchanges)

        hyper = self.load_hyperliquid(hyperliquid_api) if _allow_value("hyperliquid", include_exs) else {}
        delta = self.load_delta(delta_api) if _allow_value("delta", include_exs) else {}
        coins = self.load_coinswitch(coinswitch_api) if _allow_value("coinswitch", include_exs) else {}
        try:
            import config as _cfg_mod  # type: ignore
            delta_seeds = _parse_csv_symbols(getattr(_cfg_mod, "DELTA_EXACT_DISCOVERY_SYMBOLS", ""))
            coinswitch_seeds = _parse_csv_symbols(getattr(_cfg_mod, "COINSWITCH_EXACT_DISCOVERY_SYMBOLS", ""))
        except Exception:
            delta_seeds, coinswitch_seeds = set(), set()
        if _allow_value("delta", include_exs):
            delta = self._augment_delta_from_seed_symbols(delta, delta_api, delta_seeds)
        if intents and _allow_value("coinswitch", include_exs):
            coins = self._augment_coinswitch_from_requested(coins, coinswitch_api, intents)
        if _allow_value("coinswitch", include_exs):
            coins = self._augment_coinswitch_from_seed_symbols(coins, coinswitch_api, coinswitch_seeds)
        icici = self.load_icici(icici_api, security_master_url=icici_security_master_url) if _allow_value("icici", include_exs) else {}

        self.report = DiscoveryReport(requested=intents, raw_counts={
            "hyperliquid": len({id(v) for v in hyper.values()}),
            "delta": len({id(v) for v in delta.values()}),
            "coinswitch": len({id(v) for v in coins.values()}),
            "icici": len({id(v) for v in icici.values()}),
        })

        if mode in {"static", "requested", "legacy"}:
            matched = self._discover_requested(intents, delta, coins, hyper, require_primary=require_primary)
        else:
            matched = self._discover_dynamic(hyper, delta, coins, icici, intents, include_classes=include_classes)

        matched.sort(key=lambda x: (
            x.priority,
            str(x.asset_class.value),
            str(x.primary_exchange.value),
            str(x.asset_id),
        ))
        self.report.matched = matched if int(max_active or 0) <= 0 else matched[:max(1, int(max_active))]
        return self.report

    def _discover_requested(self, intents: List[AssetIntent], delta, coins, hyper, *, require_primary: bool) -> List[TradableInstrument]:
        matched: List[TradableInstrument] = []
        for intent in sorted(intents, key=lambda x: x.priority):
            aliases = _ordered_aliases(intent)
            by_ex: Dict[ExchangeName, ExchangeInstrument] = {}
            hmatch = self._match_one(hyper, aliases)
            dmatch = self._match_one(delta, aliases)
            cmatch = self._match_one(coins, aliases)
            if hmatch is not None:
                by_ex[ExchangeName.HYPERLIQUID] = self._retag(hmatch, intent)
            if dmatch is not None:
                by_ex[ExchangeName.DELTA] = self._retag(dmatch, intent)
            if cmatch is not None:
                by_ex[ExchangeName.COINSWITCH] = self._retag(cmatch, intent)
            if not by_ex:
                self.report.unavailable[intent.asset_id] = "not present in live Hyperliquid/Delta/CoinSwitch catalog; not traded"
                continue
            primary = self.execution_preference if self.execution_preference in by_ex else next(iter(by_ex.keys()))
            if require_primary and self.execution_preference not in by_ex:
                pass
            matched.append(TradableInstrument(
                asset_id=intent.asset_id,
                display_name=intent.display_name,
                asset_class=intent.asset_class,
                primary_exchange=primary,
                by_exchange=by_ex,
                priority=intent.priority,
            ))
        return matched

    def _discover_dynamic(self, hyper, delta, coins, icici, intents: List[AssetIntent], *, include_classes: Set[str]) -> List[TradableInstrument]:
        overlay: Dict[str, AssetIntent] = {}
        for intent in intents:
            for alias in _ordered_aliases(intent):
                overlay[alias] = intent
            overlay[normalise_symbol(intent.asset_id)] = intent

        grouped: Dict[str, Dict[ExchangeName, ExchangeInstrument]] = {}
        meta: Dict[str, Tuple[str, AssetClass, int]] = {}
        seen_ids: set[int] = set()
        for catalog in (hyper, delta, coins, icici):
            for inst in catalog.values():
                if id(inst) in seen_ids or not inst.is_active:
                    continue
                seen_ids.add(id(inst))
                intent = overlay.get(normalise_symbol(inst.symbol)) or overlay.get(normalise_symbol(inst.base_asset)) or overlay.get(normalise_symbol(inst.asset_id))
                effective = self._retag(inst, intent) if intent else inst
                if not _allow_value(effective.asset_class.value, include_classes):
                    continue
                asset_id = intent.asset_id if intent else _canonical_asset_id(effective)
                existing = grouped.get(asset_id, {}).get(effective.exchange)
                if existing is not None and normalise_symbol(existing.symbol) != normalise_symbol(effective.symbol):
                    asset_id = f"{asset_id}_{normalise_symbol(effective.symbol)}"
                grouped.setdefault(asset_id, {})[effective.exchange] = ExchangeInstrument(
                    exchange=effective.exchange,
                    symbol=effective.symbol,
                    ws_symbol=effective.ws_symbol,
                    display_symbol=effective.display_symbol,
                    asset_id=asset_id,
                    asset_class=effective.asset_class,
                    product_id=effective.product_id,
                    quote_asset=effective.quote_asset,
                    base_asset=effective.base_asset,
                    contract_type=effective.contract_type,
                    status=effective.status,
                    tick_size=effective.tick_size,
                    lot_step=effective.lot_step,
                    min_qty=effective.min_qty,
                    max_qty=effective.max_qty,
                    contract_value_btc=effective.contract_value_btc,
                    max_leverage=effective.max_leverage,
                    raw=effective.raw,
                )
                prev = meta.get(asset_id)
                priority = intent.priority if intent else 10_000
                display = intent.display_name if intent else _display_name_for(effective, asset_id)
                ac = intent.asset_class if intent else effective.asset_class
                if prev is None or priority < prev[2]:
                    meta[asset_id] = (display, ac, priority)

        out: List[TradableInstrument] = []
        for asset_id, by_ex in grouped.items():
            primary = self.execution_preference if self.execution_preference in by_ex else next(iter(by_ex.keys()))
            display, ac, priority = meta.get(asset_id, (asset_id, next(iter(by_ex.values())).asset_class, 10_000))
            out.append(TradableInstrument(
                asset_id=asset_id,
                display_name=display,
                asset_class=ac,
                primary_exchange=primary,
                by_exchange=by_ex,
                priority=priority,
            ))
        return out

    def _match_one(self, catalog: Dict[str, ExchangeInstrument], aliases) -> Optional[ExchangeInstrument]:
        # exact match first, preserving configured alias priority
        alias_list = list(aliases)
        for a in alias_list:
            if a in catalog:
                return catalog[a]
        # then exact display/base matches, but avoid accidental tiny strings
        aliases2 = [a for a in alias_list if len(a) >= 3]
        for key, inst in catalog.items():
            ndisp = normalise_symbol(inst.display_symbol)
            nbase = normalise_symbol(inst.base_asset)
            for a in aliases2:
                if key == a or ndisp == a or nbase == a:
                    return inst
        return None

    def _retag(self, inst: ExchangeInstrument, intent: AssetIntent) -> ExchangeInstrument:
        return ExchangeInstrument(
            exchange=inst.exchange,
            symbol=inst.symbol,
            ws_symbol=inst.ws_symbol,
            display_symbol=inst.display_symbol,
            asset_id=intent.asset_id,
            asset_class=intent.asset_class,
            product_id=inst.product_id,
            quote_asset=inst.quote_asset,
            base_asset=inst.base_asset,
            contract_type=inst.contract_type,
            status=inst.status,
            tick_size=inst.tick_size,
            lot_step=inst.lot_step,
            min_qty=inst.min_qty,
            max_qty=inst.max_qty,
            contract_value_btc=inst.contract_value_btc,
            max_leverage=inst.max_leverage,
            raw=inst.raw,
        )

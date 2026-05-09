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
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from core.instruments import (
    AssetClass, AssetIntent, ExchangeInstrument, ExchangeName, TradableInstrument,
    configured_asset_intents, first_positive, normalise_symbol, slash_symbol,
)

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
    text = " ".join(str(row.get(k, "")) for k in ("symbol", "description", "name", "contract_type", "product_type"))
    nsym = normalise_symbol(symbol)
    nbase = normalise_symbol(base)
    if nsym.endswith("XUSD") or nbase.endswith("X"):
        return AssetClass.EQUITY
    return _asset_class_from_text(f"{text} {nbase} {nsym}", AssetClass.CRYPTO)


def _canonical_asset_id(inst: ExchangeInstrument) -> str:
    """Canonical key for cross-venue grouping without enumerating a fixed basket."""
    sym = normalise_symbol(inst.symbol)
    base = normalise_symbol(inst.base_asset or inst.asset_id or sym)
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
        self.delta: Dict[str, ExchangeInstrument] = {}
        self.coinswitch: Dict[str, ExchangeInstrument] = {}
        self.icici: Dict[str, ExchangeInstrument] = {}
        self.report = DiscoveryReport()

    # ──────────────────────────────────────────────────────────────────────
    # Exchange catalog fetchers
    # ──────────────────────────────────────────────────────────────────────
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
            ei = ExchangeInstrument(
                exchange=ExchangeName.COINSWITCH,
                symbol=rest_sym,
                ws_symbol=ws_sym,
                display_symbol=ws_sym,
                asset_id=normalise_symbol(base or rest_sym),
                asset_class=AssetClass.CRYPTO,
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
        for r in rows:
            ex_code = str(
                r.get("ExchangeCode") or r.get("exchange_code") or r.get("Exchange") or r.get("exchange") or ""
            ).upper()
            if ex_code not in {"NSE", "NFO", "BSE", "BFO"}:
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
            text = f"{stock} {product} {right} {strike} {expiry} {ex_code}"
            ac = AssetClass.CASH
            is_option = _is_icici_option_row(r, ex_code, product, right, strike, expiry)
            if ex_code in {"NFO", "BFO"}:
                ac = AssetClass.OPTION if is_option else AssetClass.FUTURE
            elif ex_code == "BSE" and is_option:
                ac = AssetClass.OPTION
            elif ex_code == "BSE" and normalise_symbol(stock) in {"SENSEX", "BANKEX"}:
                ac = AssetClass.INDEX
            elif _asset_class_from_text(text, AssetClass.EQUITY) == AssetClass.INDEX:
                ac = AssetClass.INDEX
            if _icici_options_only and ac != AssetClass.OPTION:
                # Indian-market mandate: only listed options are eligible for the
                # trading universe. Cash/future/index rows are not silently traded
                # or used as synthetic substitutes for options.
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
            key = normalise_symbol(symbol)
            if key in out and normalise_symbol(str(out[key].product_id or "")) != normalise_symbol(str(inst.product_id or "")):
                key = f"{key}_{normalise_symbol(str(inst.product_id or len(out)))}"
            out[key] = inst
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
            # CoinSwitch is crypto futures only in the current API/page. Do not
            # probe commodities, indices or equity-token aliases there.
            if intent.asset_class != AssetClass.CRYPTO:
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
                        display_symbol=ws_sym, asset_id=normalise_symbol(base2),
                        asset_class=AssetClass.CRYPTO, quote_asset=quote, base_asset=base2,
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

    # ──────────────────────────────────────────────────────────────────────
    # Matching
    # ──────────────────────────────────────────────────────────────────────
    def discover(self, delta_api=None, coinswitch_api=None, requested=None,
                 max_active: int = 0, require_primary: bool = True,
                 discovery_mode: str = "dynamic", include_asset_classes=None,
                 include_exchanges=None, icici_api=None,
                 icici_security_master_url: str | None = None) -> DiscoveryReport:
        mode = str(discovery_mode or "dynamic").lower()
        intents = configured_asset_intents(requested)
        include_classes = _parse_csv_set(include_asset_classes)
        include_exs = _parse_csv_set(include_exchanges)

        delta = self.load_delta(delta_api) if _allow_value("delta", include_exs) else {}
        coins = self.load_coinswitch(coinswitch_api) if _allow_value("coinswitch", include_exs) else {}
        if intents and _allow_value("coinswitch", include_exs):
            coins = self._augment_coinswitch_from_requested(coins, coinswitch_api, intents)
        icici = self.load_icici(icici_api, security_master_url=icici_security_master_url) if _allow_value("icici", include_exs) else {}

        self.report = DiscoveryReport(requested=intents, raw_counts={
            "delta": len({id(v) for v in delta.values()}),
            "coinswitch": len({id(v) for v in coins.values()}),
            "icici": len({id(v) for v in icici.values()}),
        })

        if mode in {"static", "requested", "legacy"}:
            matched = self._discover_requested(intents, delta, coins, require_primary=require_primary)
        else:
            matched = self._discover_dynamic(delta, coins, icici, intents, include_classes=include_classes)

        matched.sort(key=lambda x: (
            x.priority,
            str(x.asset_class.value),
            str(x.primary_exchange.value),
            str(x.asset_id),
        ))
        self.report.matched = matched if int(max_active or 0) <= 0 else matched[:max(1, int(max_active))]
        return self.report

    def _discover_requested(self, intents: List[AssetIntent], delta, coins, *, require_primary: bool) -> List[TradableInstrument]:
        matched: List[TradableInstrument] = []
        for intent in sorted(intents, key=lambda x: x.priority):
            aliases = _ordered_aliases(intent)
            by_ex: Dict[ExchangeName, ExchangeInstrument] = {}
            dmatch = self._match_one(delta, aliases)
            cmatch = self._match_one(coins, aliases)
            if dmatch is not None:
                by_ex[ExchangeName.DELTA] = self._retag(dmatch, intent)
            if cmatch is not None:
                by_ex[ExchangeName.COINSWITCH] = self._retag(cmatch, intent)
            if not by_ex:
                self.report.unavailable[intent.asset_id] = "not present in live Delta/CoinSwitch catalog; not traded"
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

    def _discover_dynamic(self, delta, coins, icici, intents: List[AssetIntent], *, include_classes: Set[str]) -> List[TradableInstrument]:
        overlay: Dict[str, AssetIntent] = {}
        for intent in intents:
            for alias in _ordered_aliases(intent):
                overlay[alias] = intent
            overlay[normalise_symbol(intent.asset_id)] = intent

        grouped: Dict[str, Dict[ExchangeName, ExchangeInstrument]] = {}
        meta: Dict[str, Tuple[str, AssetClass, int]] = {}
        seen_ids: set[int] = set()
        for catalog in (delta, coins, icici):
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

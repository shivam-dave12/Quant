"""
execution/instrument_registry.py — live catalog discovery and filtering
======================================================================

The registry never creates synthetic executable contracts.  It reads Delta's
/v2/products and CoinSwitch's futures instrument/ticker endpoints, normalises
only contracts returned by the exchange, and then matches requested asset
intents against those confirmed symbols.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple

from core.instruments import (
    AssetClass, AssetIntent, ExchangeInstrument, ExchangeName, TradableInstrument,
    configured_asset_intents, first_positive, normalise_symbol, slash_symbol,
)
try:
    import config
except Exception:  # pragma: no cover
    config = None  # type: ignore
try:
    from agents.icici_chain_architect import build_underlying_payload
except Exception:  # pragma: no cover
    build_underlying_payload = None  # type: ignore

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


def _cfg(name: str, default):
    return getattr(config, name, default) if config is not None else default


def _csv_symbols(raw) -> list[str]:
    if isinstance(raw, str):
        vals = raw.replace(";", ",").split(",")
    elif raw is None:
        vals = []
    else:
        vals = list(raw)
    out = []
    seen = set()
    for value in vals:
        sym = normalise_symbol(str(value))
        if sym and sym not in seen:
            out.append(sym)
            seen.add(sym)
    return out


def _parse_csv_set(value) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, str):
        vals = value.replace(";", ",").split(",")
    else:
        vals = list(value)
    return {str(x).strip().lower() for x in vals if str(x).strip()}


def _allow_value(value: str, allowed: set[str]) -> bool:
    return not allowed or "all" in allowed or str(value or "").lower() in allowed


def _icici_breeze_code(underlying: str) -> str:
    key = normalise_symbol(underlying)
    raw = _cfg("ICICI_INDEX_BREEZE_STOCK_CODE_BY_UNDERLYING", {})
    if isinstance(raw, dict):
        for k, v in raw.items():
            if normalise_symbol(str(k)) == key and normalise_symbol(str(v)):
                return normalise_symbol(str(v))
    # ICICI Breeze uses stock_code="NIFTY" for the NIFTY 50 index and NFO
    # option chain.  Keep operator-facing aliases accepted, but never send
    # NIFTY50/CNXNIFTY as the Breeze stock_code.
    if key in {"NIFTY50", "CNXNIFTY", "NIFTYINDEX"}:
        return "NIFTY"
    return key


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


def _market_key_like(value: str) -> bool:
    n = normalise_symbol(str(value))
    if len(n) < 5:
        return False
    return n.endswith(("USDT", "USD", "INR"))


def _asset_default_max_leverage(asset_class: AssetClass) -> float:
    # Conservative fallback when the product row omits max_leverage.
    # Delta xStock/RWA contracts displayed in the UI are 25x; BTC remains governed
    # by the actual product row/config.  These are caps, not trade triggers.
    if asset_class == AssetClass.EQUITY:
        return 25.0
    if asset_class in (AssetClass.COMMODITY, AssetClass.INDEX):
        return 25.0
    return 0.0


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
                # Derive asset class before sizing/leverage defaults. Delta
                # xStock symbols end in XUSD (AAPLXUSD, NVDAXUSD, ...); tokenised
                # commodity contracts are explicitly requested via their aliases.
                inferred_class = AssetClass.CRYPTO
                nsym = normalise_symbol(sym)
                if nsym.endswith("XUSD") or nsym in {"SPYXUSD", "QQQXUSD", "CRCLXUSD", "COINXUSD"}:
                    inferred_class = AssetClass.EQUITY
                elif nsym in {"PAXGUSD", "XAUTUSD", "SLVONUSD"}:
                    inferred_class = AssetClass.COMMODITY

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
        """Load configured ICICI index-option desk instruments.

        v508 uses the underlying-first path for NIFTY: one desk instrument is
        discovered now, and the exact CE/PE strike/expiry is selected after the
        strategy produces a bullish/bearish NIFTY thesis.

        This discovery stage is intentionally auth-free.  Breeze protected
        endpoints are touched only when the ICICI data managers start, so NIFTY
        must not disappear from the universe just because the runtime session
        token has not been generated yet.
        """
        out: Dict[str, ExchangeInstrument] = {}
        config_only = bool(_cfg("ICICI_INDEX_OPTIONS_FROM_CONFIG_ONLY", True))
        discovery_enabled = bool(_cfg("ICICI_DISCOVERY_ENABLED", False))
        runtime_enabled = bool(_cfg("ICICI_ENABLED", False) or _cfg("ICICI_OPTIONS_RUNTIME_ENABLED", False))
        if not (discovery_enabled or runtime_enabled):
            self.icici = out
            return out
        # Configured-index discovery is intentionally auth-independent. This is
        # the V83-compatible guardrail that prevents NIFTY from vanishing just
        # because the Breeze API_Session has not been generated yet.
        if api is None and not config_only:
            self.icici = out
            return out
        underlyings = _csv_symbols(_cfg("ICICI_INDEX_UNDERLYINGS", "NIFTY"))
        if not underlyings:
            underlyings = ["NIFTY"]
        if build_underlying_payload is None:
            logger.warning("ICICI discovery skipped: agents.icici_chain_architect unavailable")
            self.icici = out
            return out
        for priority, underlying in enumerate(underlyings, 1):
            breeze_code = _icici_breeze_code(underlying)
            raw = build_underlying_payload(breeze_code, "ICICI_INDEX_OPTIONS", [])
            raw["underlying_display"] = underlying
            raw["configured_underlying"] = underlying
            raw["breeze_stock_code"] = breeze_code
            raw["stock_code"] = breeze_code
            raw["underlying_stock_code"] = breeze_code
            raw["underlying_exchange_code"] = "NSE"
            raw["exchange_code"] = "NFO"
            raw["chain_source"] = "configured_index"
            raw["chain_candidates_deferred"] = True
            ei = ExchangeInstrument(
                exchange=ExchangeName.ICICI,
                symbol=breeze_code,
                ws_symbol=breeze_code,
                display_symbol=breeze_code,
                asset_id=breeze_code,
                asset_class=AssetClass.OPTION,
                product_id=None,
                quote_asset="INR",
                base_asset=breeze_code,
                contract_type="option_chain",
                status="active",
                tick_size=float(_cfg("ICICI_OPTION_TICK_SIZE", 0.05)),
                lot_step=1.0,
                min_qty=1.0,
                max_leverage=1.0,
                raw={**raw, "configured_priority": priority},
            )
            # Match both the operator-facing config alias (e.g. NIFTY50) and the
            # real Breeze/NFO stock_code (NIFTY).  Both keys point to the same
            # object, so raw_counts still reports one ICICI instrument.
            for key in {normalise_symbol(underlying), normalise_symbol(breeze_code), "NIFTY50" if breeze_code == "NIFTY" else ""}:
                if key:
                    out[key] = ei
        self.icici = out
        logger.info("ICICI configured-index discovery active: underlyings=%s", ",".join(out.keys()) or "none")
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
                 max_active: int = 12, require_primary: bool = True,
                 include_exchanges=None, icici_api=None,
                 icici_security_master_url: str | None = None) -> DiscoveryReport:
        intents = configured_asset_intents(requested)
        include_exs = _parse_csv_set(include_exchanges)
        delta = self.load_delta(delta_api) if _allow_value("delta", include_exs) else {}
        coins = self.load_coinswitch(coinswitch_api) if _allow_value("coinswitch", include_exs) else {}
        if _allow_value("coinswitch", include_exs):
            coins = self._augment_coinswitch_from_requested(coins, coinswitch_api, intents)
        icici = self.load_icici(icici_api, security_master_url=icici_security_master_url) if _allow_value("icici", include_exs) else {}
        self.report = DiscoveryReport(requested=intents, raw_counts={
            "delta": len(delta),
            "coinswitch": len({id(v) for v in coins.values()}),
            "icici": len({id(v) for v in icici.values()}),
        })

        matched: List[TradableInstrument] = []
        for intent in sorted(intents, key=lambda x: x.priority):
            aliases = _ordered_aliases(intent)
            by_ex: Dict[ExchangeName, ExchangeInstrument] = {}
            dmatch = self._match_one(delta, aliases)
            cmatch = self._match_one(coins, aliases)
            imatch = self._match_one(icici, aliases)
            if dmatch is not None:
                by_ex[ExchangeName.DELTA] = self._retag(dmatch, intent)
            if cmatch is not None:
                by_ex[ExchangeName.COINSWITCH] = self._retag(cmatch, intent)
            if imatch is not None:
                by_ex[ExchangeName.ICICI] = self._retag(imatch, intent)
            if not by_ex:
                self.report.unavailable[intent.asset_id] = "not present in live Delta/CoinSwitch/ICICI catalog; not traded"
                continue
            primary = self.execution_preference if self.execution_preference in by_ex else next(iter(by_ex.keys()))
            if require_primary and self.execution_preference not in by_ex:
                # still activate on fallback if explicit config allows; default false is handled by caller
                pass
            matched.append(TradableInstrument(
                asset_id=intent.asset_id,
                display_name=intent.display_name,
                asset_class=intent.asset_class,
                primary_exchange=primary,
                by_exchange=by_ex,
                priority=intent.priority,
            ))

        self.report.matched = matched[:max(1, int(max_active))]
        return self.report

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

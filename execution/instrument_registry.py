"""
execution/instrument_registry.py — live catalog discovery and filtering
======================================================================

The registry never creates synthetic executable contracts. It reads Delta's
/v2/products catalog, normalises only contracts returned by the exchange, and
then matches requested asset intents against those confirmed symbols.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple

from core.instruments import (
    AssetClass, AssetIntent, ExchangeInstrument, ExchangeName, TradableInstrument,
    configured_asset_intents, first_positive, normalise_symbol,
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

    # ──────────────────────────────────────────────────────────────────────
    # Matching
    # ──────────────────────────────────────────────────────────────────────
    def discover(self, delta_api=None, requested=None, max_active: int = 12, require_primary: bool = True) -> DiscoveryReport:
        """Discover tradable instruments from Delta only.

        Alternate exchange discovery was removed from this build because it was not
        returning usable data and only added startup latency plus routing debt.
        """
        intents = configured_asset_intents(requested)
        delta = self.load_delta(delta_api)
        self.report = DiscoveryReport(requested=intents, raw_counts={"delta": len(delta)})

        matched: List[TradableInstrument] = []
        for intent in sorted(intents, key=lambda x: x.priority):
            aliases = _ordered_aliases(intent)
            dmatch = self._match_one(delta, aliases)
            if dmatch is None:
                self.report.unavailable[intent.asset_id] = "not present in live Delta catalog; not traded"
                continue
            by_ex: Dict[ExchangeName, ExchangeInstrument] = {
                ExchangeName.DELTA: self._retag(dmatch, intent)
            }
            matched.append(TradableInstrument(
                asset_id=intent.asset_id,
                display_name=intent.display_name,
                asset_class=intent.asset_class,
                primary_exchange=ExchangeName.DELTA,
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

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
        # Some CoinSwitch endpoints return {exchange: {symbol: specs}}
        rows: List[dict] = []
        for ex_val in data.values():
            if isinstance(ex_val, dict):
                for sym, spec in ex_val.items():
                    row = dict(spec) if isinstance(spec, dict) else {}
                    row.setdefault("symbol", sym)
                    rows.append(row)
        return rows
    return []


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
                step = first_positive(
                    _safe_float(p.get("contract_value")),
                    _safe_float(p.get("lot_size")),
                    _safe_float(p.get("size_increment")),
                )
                ei = ExchangeInstrument(
                    exchange=ExchangeName.DELTA,
                    symbol=sym,
                    ws_symbol=sym,
                    display_symbol=sym,
                    asset_id=normalise_symbol(base or sym),
                    asset_class=AssetClass.CRYPTO,
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
            if not rows and hasattr(api, "get_futures_tickers"):
                rows = _unwrap_list(api.get_futures_tickers(exchange="EXCHANGE_2"))
        except Exception as e:
            logger.warning("CoinSwitch instrument discovery failed: %s", e, exc_info=True)
            rows = []
        for r in rows:
            sym = str(r.get("symbol") or r.get("m") or r.get("market") or r.get("pair") or "").upper()
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
                raw=r,
            )
            out[normalise_symbol(rest_sym)] = ei
            out[normalise_symbol(ws_sym)] = ei
        self.coinswitch = out
        return out

    # ──────────────────────────────────────────────────────────────────────
    # Matching
    # ──────────────────────────────────────────────────────────────────────
    def discover(self, delta_api=None, coinswitch_api=None, requested=None,
                 max_active: int = 12, require_primary: bool = True) -> DiscoveryReport:
        intents = configured_asset_intents(requested)
        delta = self.load_delta(delta_api)
        coins = self.load_coinswitch(coinswitch_api)
        self.report = DiscoveryReport(requested=intents, raw_counts={
            "delta": len(delta), "coinswitch": len({id(v) for v in coins.values()})
        })

        matched: List[TradableInstrument] = []
        for intent in sorted(intents, key=lambda x: x.priority):
            aliases = intent.alias_set()
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

    def _match_one(self, catalog: Dict[str, ExchangeInstrument], aliases: set[str]) -> Optional[ExchangeInstrument]:
        # exact match first
        for a in aliases:
            if a in catalog:
                return catalog[a]
        # then containment, but avoid accidental tiny strings
        aliases2 = [a for a in aliases if len(a) >= 3]
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
            raw=inst.raw,
        )

"""Asset-desk router with venue routing.

Institutional desks are organised around the traded thesis/instrument family, not
around a broker venue. BTC on Delta, CoinSwitch or CoinDCX is one BTC desk with
multiple possible execution/data venues.  The desk owns the alpha view; the venue
router decides where, if anywhere, that view may be expressed.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, Mapping, Sequence

try:
    import config
except Exception:  # pragma: no cover
    config = None  # type: ignore

from core.instruments import AssetClass, ExchangeName, TradableInstrument, normalise_symbol
from fund.types import clamp, safe_float


def _cfg(name: str, default):
    if config is None:
        return default
    return getattr(config, name, default)


def _csv_map(raw: str | None) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for part in str(raw or "").replace(";", ",").split(","):
        if not part.strip() or ":" not in part:
            continue
        k, v = part.split(":", 1)
        try:
            out[k.strip().upper()] = max(0, int(v.strip()))
        except Exception:
            continue
    return out


def _venue_order(raw: str | None) -> list[ExchangeName]:
    out: list[ExchangeName] = []
    for part in str(raw or "delta,coindcx,coinswitch,icici").replace(";", ",").split(","):
        p = part.strip().lower()
        if not p:
            continue
        try:
            ex = ExchangeName(p)
            if ex not in out:
                out.append(ex)
        except Exception:
            continue
    return out or [ExchangeName.DELTA, ExchangeName.COINDCX, ExchangeName.COINSWITCH, ExchangeName.ICICI]


@dataclass(frozen=True)
class DeskProfile:
    desk_id: str
    label: str
    venue: str
    family: str
    quota: int
    min_score: float
    notes: str = ""


@dataclass(frozen=True)
class VenueRoute:
    exchange: ExchangeName
    symbol: str
    score: float
    reason: str
    executable: bool
    spread_bps: float = 0.0
    turnover_score: float = 0.0
    max_leverage: float = 0.0


class InstitutionalDeskRouter:
    """Assigns a cross-venue instrument to its institutional asset desk."""

    def __init__(self) -> None:
        configured = _csv_map(_cfg("DESK_MAX_ACTIVE_BY_ID", ""))
        # Quotas are now by strategy/asset desk, never by venue. Venue-level
        # limits belong in the execution router, not the alpha/ticker selector.
        self.default_quotas = {
            "BTC_GLOBAL": int(_cfg("DESK_BTC_GLOBAL_MAX_ACTIVE", 1)),
            "CRYPTO_ALTS": int(_cfg("DESK_CRYPTO_ALTS_MAX_ACTIVE", 7)),
            "US_STOCK_DERIVATIVES": int(_cfg("DESK_US_STOCK_DERIVATIVES_MAX_ACTIVE", 2)),
            "COMMODITIES_GLOBAL": int(_cfg("DESK_COMMODITIES_GLOBAL_MAX_ACTIVE", 2)),
            "ICICI_INDEX_OPTIONS": int(_cfg("DESK_ICICI_INDEX_OPTIONS_MAX_ACTIVE", 4)),
            "ICICI_STOCK_OPTIONS": int(_cfg("DESK_ICICI_STOCK_OPTIONS_MAX_ACTIVE", 6)),
        }
        self.default_quotas.update(configured)
        self.default_min_score = float(_cfg("DYNAMIC_DESK_MIN_SCORE", 0.38))
        self.venue_preference = _venue_order(_cfg("VENUE_ROUTE_PREFERENCE", "delta,coindcx,coinswitch,icici"))

    def desk_id_for(self, inst: TradableInstrument) -> str:
        ac = getattr(inst, "asset_class", None)
        aid = normalise_symbol(getattr(inst, "asset_id", ""))
        sym = normalise_symbol(getattr(inst, "display_symbol", ""))
        raw = getattr(getattr(inst, "primary", None), "raw", {}) or {}
        underlying = normalise_symbol(raw.get("stock_code") or raw.get("underlying") or raw.get("ShortName") or aid)
        exchanges = set(getattr(inst, "by_exchange", {}) or {})

        if ExchangeName.ICICI in exchanges or getattr(inst, "primary_exchange", None) == ExchangeName.ICICI:
            if ac != AssetClass.OPTION:
                return "ICICI_REJECT_NON_OPTION"
            kind = normalise_symbol(raw.get("option_kind") or raw.get("product_type") or raw.get("ProductType") or raw.get("InstrumentType") or raw.get("InstrumentName") or raw.get("Series") or "")
            if "IDX" in kind or "INDEX" in kind:
                return "ICICI_INDEX_OPTIONS"
            return "ICICI_STOCK_OPTIONS"

        if aid in {"BTC", "BTCUSD", "BTCUSDT", "BTCINR"} or sym.startswith("BTC"):
            return "BTC_GLOBAL"
        if ac == AssetClass.EQUITY:
            return "US_STOCK_DERIVATIVES"
        if ac == AssetClass.COMMODITY:
            return "COMMODITIES_GLOBAL"
        return "CRYPTO_ALTS"

    def profile_for(self, desk_id: str) -> DeskProfile:
        desk = str(desk_id or "UNKNOWN").upper()
        labels = {
            "BTC_GLOBAL": ("Global BTC command desk", "multi", "btc"),
            "CRYPTO_ALTS": ("Global alt-crypto relative-value desk", "multi", "crypto_alts"),
            "US_STOCK_DERIVATIVES": ("US stock-derivatives desk", "multi", "us_stock_derivatives"),
            "COMMODITIES_GLOBAL": ("Global commodities desk", "multi", "commodities"),
            "ICICI_INDEX_OPTIONS": ("ICICI index-options desk", "icici", "index_options"),
            "ICICI_STOCK_OPTIONS": ("ICICI stock-options desk", "icici", "stock_options"),
        }
        label, venue, family = labels.get(desk, (desk, "unknown", "unknown"))
        return DeskProfile(
            desk_id=desk,
            label=label,
            venue=venue,
            family=family,
            quota=max(0, int(self.default_quotas.get(desk, 0))),
            min_score=float(_cfg(f"{desk}_MIN_SCORE", self.default_min_score)),
            notes="asset thesis desk with venue routing; no venue-duplicated crypto desks",
        )

    def allowed_desks(self) -> set[str]:
        raw = str(_cfg("DESK_ENABLED_IDS", "")).strip()
        if not raw:
            return {k for k, q in self.default_quotas.items() if q > 0}
        return {x.strip().upper() for x in raw.replace(";", ",").split(",") if x.strip()}

    def venue_routes_for(
        self,
        inst: TradableInstrument,
        *,
        market_snapshots: Mapping[ExchangeName, Mapping[str, Mapping]] | None = None,
    ) -> tuple[VenueRoute, ...]:
        """Rank execution venues for one asset without creating duplicate desks."""
        market_snapshots = market_snapshots or {}
        routes: list[VenueRoute] = []
        for ex, ex_inst in (getattr(inst, "by_exchange", {}) or {}).items():
            snapshot_book = market_snapshots.get(ex, {}) or {}
            snap = dict(snapshot_book.get(normalise_symbol(ex_inst.symbol), {}) or {})
            raw = {**(ex_inst.raw or {}), **snap}
            bid = self._first_float(raw, ("best_bid", "bestBid", "bid", "bid_price", "best_bid_price"))
            ask = self._first_float(raw, ("best_ask", "bestAsk", "ask", "ask_price", "best_ask_price"))
            price = self._first_float(raw, ("mark_price", "markPrice", "last_price", "lastPrice", "close", "price", "ltp"))
            mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else price
            spread_bps = ((ask - bid) / mid * 10000.0) if ask > 0 and bid > 0 and mid > 0 else 0.0
            spread_score = 0.60 if spread_bps <= 0 else clamp(1.0 - spread_bps / 80.0)
            turnover = self._turnover_proxy(raw, price)
            turnover_score = clamp(__import__("math").log10(max(1.0, turnover)) / 9.0)
            configured_cap = self._configured_route_leverage_cap(ex, ex_inst.symbol, ex_inst.asset_class)
            leverage_confirmed = safe_float(getattr(ex_inst, "max_leverage", 0.0), 0.0) > 0 or configured_cap > 0 or ex == ExchangeName.ICICI
            status_ok = getattr(ex_inst, "is_active", False)
            data_score = 1.0 if snap else (0.72 if raw else 0.35)
            venue_pref = self._venue_preference_score(ex)
            executable = bool(status_ok and ex_inst.symbol)
            score = clamp(0.28 * spread_score + 0.25 * turnover_score + 0.18 * data_score + 0.17 * venue_pref + 0.12 * (1.0 if leverage_confirmed else 0.45))
            reasons = ["route"]
            if snap: reasons.append("live_snapshot")
            if turnover_score >= 0.55: reasons.append("liquid")
            if spread_bps > 80: reasons.append("wide_spread")
            if configured_cap > 0 and safe_float(getattr(ex_inst, "max_leverage", 0.0), 0.0) <= 0: reasons.append("configured_leverage_cap")
            if not leverage_confirmed and ex != ExchangeName.ICICI: reasons.append("unconfirmed_leverage")
            routes.append(VenueRoute(
                exchange=ex,
                symbol=ex_inst.symbol,
                score=score,
                reason=",".join(reasons),
                executable=executable,
                spread_bps=max(0.0, spread_bps),
                turnover_score=turnover_score,
                max_leverage=safe_float(getattr(ex_inst, "max_leverage", 0.0), configured_cap),
            ))
        routes.sort(key=lambda r: (r.executable, r.score, self._venue_preference_score(r.exchange)), reverse=True)
        return tuple(routes)


    def _configured_route_leverage_cap(self, ex: ExchangeName, symbol: str, asset_class: AssetClass) -> float:
        if ex != ExchangeName.DELTA:
            return 0.0
        keys = []
        base = normalise_symbol(symbol)
        for k in (base, base.replace("USDT", "USD"), base.replace("USD", "")):
            if k and k not in keys:
                keys.append(k)
        raw = _cfg("DELTA_SYMBOL_MAX_LEVERAGE", {})
        if isinstance(raw, dict):
            for k in keys:
                try:
                    v = float(raw.get(k, 0.0) or 0.0)
                    if v > 0:
                        return v
                except Exception:
                    pass
        by_class = _cfg("DELTA_ASSET_CLASS_MAX_LEVERAGE", {})
        if isinstance(by_class, dict):
            try:
                v = float(by_class.get(str(asset_class.value), 0.0) or 0.0)
                if v > 0:
                    return v
            except Exception:
                pass
        if asset_class in (AssetClass.FUTURE, AssetClass.CRYPTO):
            try:
                return float(_cfg("DELTA_DEFAULT_FUTURE_MAX_LEVERAGE", 0.0) or 0.0)
            except Exception:
                return 0.0
        return 0.0

    def preferred_exchange_for(self, inst: TradableInstrument, *, market_snapshots: Mapping[ExchangeName, Mapping[str, Mapping]] | None = None) -> ExchangeName:
        routes = self.venue_routes_for(inst, market_snapshots=market_snapshots)
        return routes[0].exchange if routes else inst.primary_exchange

    def _venue_preference_score(self, ex: ExchangeName) -> float:
        try:
            idx = self.venue_preference.index(ex)
            return clamp(1.0 - idx * 0.18)
        except Exception:
            return 0.35

    @staticmethod
    def _first_float(row: Mapping, names: Iterable[str]) -> float:
        names_l = {str(n).lower() for n in names}
        for k, v in (row or {}).items():
            if str(k).lower() in names_l:
                f = safe_float(v, 0.0)
                if f > 0:
                    return f
        for v in (row or {}).values():
            if isinstance(v, Mapping):
                f = InstitutionalDeskRouter._first_float(v, names_l)
                if f > 0:
                    return f
        return 0.0

    @staticmethod
    def _turnover_proxy(row: Mapping, price: float) -> float:
        direct = InstitutionalDeskRouter._first_float(row, ("turnover", "turnover_usd", "turnoverUsd", "quote_volume", "quoteVolume", "volume_usd", "volumeUsd", "notional_volume", "notionalVolume"))
        if direct > 0:
            return direct
        vol = InstitutionalDeskRouter._first_float(row, ("volume", "volume_24h", "volume24h", "base_volume", "baseVolume", "total_quantity_traded", "total_traded_quantity"))
        if vol > 0 and price > 0:
            return vol * price
        oi = InstitutionalDeskRouter._first_float(row, ("open_interest", "openInterest", "oi", "oi_value", "oiValue"))
        if oi > 0 and price > 0:
            return oi * price * 0.25
        return 0.0

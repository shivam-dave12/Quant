"""Desk-wise institutional tradable selection.

Discovery can see the full venue catalog, but this desk decides which assets
are allowed to own expensive live candle/orderbook/trade streams. Selection is
asset-desk based, not venue-desk based: BTC is one global command desk and
Delta/CoinSwitch/CoinDCX are ranked as execution/data routes under that desk.
"""

from __future__ import annotations

import logging
import math
import time
from collections import defaultdict
from dataclasses import dataclass, asdict
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

try:
    import config
except Exception:  # pragma: no cover
    config = None  # type: ignore

from core.instruments import AssetClass, ExchangeName, TradableInstrument, normalise_symbol
from fund.types import clamp, safe_float
from .desk_router import InstitutionalDeskRouter
from .indian_options_desk import IndianOptionsDesk

logger = logging.getLogger(__name__)


def _cfg(name: str, default: Any) -> Any:
    if config is None:
        return default
    return getattr(config, name, default)


def _parse_csv(value: str | Iterable[str] | None) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, str):
        raw = value.replace(";", ",").split(",")
    else:
        raw = list(value)
    return {normalise_symbol(x) for x in raw if normalise_symbol(x)}


@dataclass(frozen=True)
class TradableDeskRow:
    asset_id: str
    symbol: str
    venue: str
    asset_class: str
    score: float
    route_exchange: str = ""
    route_score: float = 0.0
    route_reason: str = ""
    rank: int = 0
    selected: bool = False
    reason: str = ""
    price: float = 0.0
    spread_bps: float = 0.0
    turnover_score: float = 0.0
    volatility_score: float = 0.0
    execution_score: float = 0.0
    desk_id: str = "UNKNOWN"
    desk_rank: int = 0
    icici_details: str = ""
    option_score: float = 0.0
    option_greeks: str = ""

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class TradableDeskSelection:
    timestamp: float
    selected: Tuple[TradableInstrument, ...]
    parked: Tuple[TradableInstrument, ...]
    rows: Tuple[TradableDeskRow, ...]
    notes: Tuple[str, ...] = ()

    def selected_ids(self) -> set[str]:
        return {x.asset_id for x in self.selected}

    def compact_text(self, limit: int = 12) -> str:
        lines = ["ASSET-DESK INSTITUTIONAL SELECTION + VENUE ROUTING"]
        lines.append(f"selected={len(self.selected)} parked={len(self.parked)}")
        by_desk: dict[str, list[TradableDeskRow]] = defaultdict(list)
        for row in self.rows:
            if row.selected or len(by_desk[row.desk_id]) < max(1, limit // 3):
                by_desk[row.desk_id].append(row)
        shown = 0
        for desk_id in sorted(by_desk.keys()):
            if shown >= limit:
                break
            lines.append(f"[{desk_id}]")
            for row in by_desk[desk_id]:
                if shown >= limit:
                    break
                tag = "ACTIVE" if row.selected else "PARK"
                extra = f" opt={row.option_score:.2f} {row.option_greeks}" if row.option_score > 0 else ""
                lines.append(
                    f"{tag} {row.rank:02d}/{row.desk_rank:02d} {row.asset_id:<22} {row.venue.upper():<10} "
                    f"score={row.score:.2f} route={row.route_exchange.upper() or row.venue.upper()}:{row.route_score:.2f} "
                    f"spread={row.spread_bps:.1f}bps liq={row.turnover_score:.2f} vol={row.volatility_score:.2f}{extra} "
                    f"{row.reason} route_reason={row.route_reason}"
                )
                shown += 1
        if self.notes:
            lines.append("notes=" + "; ".join(self.notes[:6]))
        return "\n".join(lines)


class TradableTickerDesk:
    """Ranks catalog instruments before any candle stream is opened."""

    def __init__(self) -> None:
        self.min_score = float(_cfg("DYNAMIC_DESK_MIN_SCORE", 0.38))
        self.max_active = max(1, int(_cfg("DYNAMIC_DESK_MAX_ACTIVE_CONTEXTS", 12)))
        self.always_include = _parse_csv(_cfg("DYNAMIC_DESK_ALWAYS_INCLUDE", ""))
        self.router = InstitutionalDeskRouter()
        self.options = IndianOptionsDesk()

    def select(
        self,
        instruments: Sequence[TradableInstrument],
        *,
        delta_api: Any = None,
        coinswitch_api: Any = None,
        icici_api: Any = None,
        active_ids: Iterable[str] = (),
        protected_ids: Iterable[str] = (),
    ) -> TradableDeskSelection:
        active_set = {str(x) for x in active_ids}
        protected_set = {str(x) for x in protected_ids}
        notes: list[str] = []
        allowed_desks = self.router.allowed_desks()

        delta_tickers = self._load_delta_tickers(delta_api) if bool(_cfg("DYNAMIC_DESK_DELTA_BULK_TICKERS", True)) else {}
        if delta_api is not None and not delta_tickers:
            notes.append("delta bulk ticker unavailable; using catalog metadata")
        # Venue snapshots are route inputs only. They must never create separate
        # BTC/crypto desks for each venue.
        market_snapshots = {ExchangeName.DELTA: delta_tickers}

        icici_quotes = self._load_icici_quote_probes(instruments, icici_api, notes)

        grouped: dict[str, list[tuple[TradableInstrument, TradableDeskRow]]] = defaultdict(list)
        rejected_rows: list[tuple[TradableInstrument, TradableDeskRow]] = []
        for inst in instruments:
            desk_id = self.router.desk_id_for(inst)
            routes = self.router.venue_routes_for(inst, market_snapshots=market_snapshots)
            row = self._score_instrument(
                inst,
                delta_tickers=delta_tickers,
                icici_quote=icici_quotes.get(str(inst.asset_id)),
                desk_id=desk_id,
                venue_routes=routes,
            )
            # Indian market mandate: options only. Cash/futures/index rows stay in
            # discovery but are not eligible for runtime or option quote probes.
            if desk_id == "ICICI_REJECT_NON_OPTION":
                rejected_rows.append((inst, self._replace_row(row, score=0.0, reason="icici_non_option_rejected")))
                continue
            if desk_id not in allowed_desks:
                rejected_rows.append((inst, self._replace_row(row, score=0.0, reason=f"desk_disabled:{desk_id}")))
                continue
            grouped[desk_id].append((inst, row))

        selected: list[TradableInstrument] = []
        selected_ids: set[str] = set()
        out_rows: list[TradableDeskRow] = []

        global_rank_items: list[tuple[TradableInstrument, TradableDeskRow]] = []
        monitor_all_ids = {
            x.strip().upper()
            for x in str(_cfg("DESK_MONITOR_ALL_IDS", "US_STOCK_DERIVATIVES,COMMODITIES_GLOBAL")).replace(";", ",").split(",")
            if x.strip()
        }
        incumbent_min_score = float(_cfg("DYNAMIC_DESK_INCUMBENT_MIN_SCORE", 0.25))
        replace_margin = max(0.0, float(_cfg("DYNAMIC_DESK_REPLACE_MARGIN_PCT", 0.12)))

        def _row_valid_for_desk(row: TradableDeskRow, profile: Any, monitor_all: bool) -> bool:
            if monitor_all:
                return row.score > 0.0
            return row.score >= max(profile.min_score, self.min_score)

        for desk_id, rows in grouped.items():
            profile = self.router.profile_for(desk_id)
            rows.sort(key=lambda x: x[1].score, reverse=True)
            quota = max(0, int(profile.quota))
            monitor_all = desk_id in monitor_all_ids
            desk_selected = 0
            desk_replaced = 0
            desk_retained = 0
            desk_ranked: list[tuple[TradableInstrument, TradableDeskRow]] = []

            # 1) Institutional incumbent retention. Existing live contexts keep
            # their analysis state while still valid. They are not destroyed just
            # because a refresh ran and ranks shuffled slightly.
            incumbents: list[tuple[TradableInstrument, TradableDeskRow]] = []
            challengers: list[tuple[TradableInstrument, TradableDeskRow]] = []
            for inst, row in rows:
                asset_id = str(inst.asset_id)
                if asset_id in active_set or asset_id in protected_set:
                    incumbents.append((inst, row))
                else:
                    challengers.append((inst, row))
            incumbents.sort(key=lambda x: x[1].score, reverse=True)
            challengers.sort(key=lambda x: x[1].score, reverse=True)

            retained: list[tuple[TradableInstrument, TradableDeskRow, str]] = []
            for inst, row in incumbents:
                asset_id = str(inst.asset_id)
                protected = asset_id in protected_set
                forced = normalise_symbol(asset_id) in self.always_include
                still_valid = protected or forced or row.score >= incumbent_min_score or _row_valid_for_desk(row, profile, monitor_all)
                if still_valid and len(retained) < quota:
                    reason = "protected_position" if protected else ("forced_include" if forced else "incumbent_retained")
                    retained.append((inst, row, reason))
                    desk_retained += 1
                else:
                    challengers.append((inst, row))

            # 2) Fill open slots from best new candidates. If the desk is already
            # full, replace only the weakest idle incumbent when the challenger is
            # materially better. This is the difference between institutional desk
            # rotation and retail reset-on-every-refresh churn.
            for inst, row in challengers:
                asset_id = str(inst.asset_id)
                forced = normalise_symbol(asset_id) in self.always_include
                valid = forced or _row_valid_for_desk(row, profile, monitor_all)
                if not valid:
                    continue
                if len(retained) < quota:
                    retained.append((inst, row, "forced_include" if forced else row.reason))
                    continue
                if not retained:
                    continue
                # Do not replace protected/forced incumbents; choose weakest idle.
                weakest_idx = None
                weakest_score = math.inf
                for i, (_old_inst, old_row, old_reason) in enumerate(retained):
                    if old_reason in {"protected_position", "forced_include"}:
                        continue
                    if old_row.score < weakest_score:
                        weakest_idx = i
                        weakest_score = old_row.score
                if weakest_idx is None:
                    continue
                if row.score >= weakest_score * (1.0 + replace_margin):
                    old_inst, old_row, _old_reason = retained[weakest_idx]
                    retained[weakest_idx] = (inst, row, f"rotation_replaced:{old_inst.asset_id}")
                    desk_replaced += 1
                    # Old incumbent becomes ranked but not selected.
                    desk_ranked.append((old_inst, self._replace_row(old_row, reason=f"rotated_out_by:{asset_id}")))

            # 3) Apply global cap only to new non-monitor desks. Already-running
            # incumbents are stateful assets and should not be force-reset by cap
            # math. Monitor-all desks bypass the cap by design.
            for inst, row, reason in retained:
                asset_id = str(inst.asset_id)
                already_live = asset_id in active_set
                bypass_global_cap = monitor_all or already_live or asset_id in protected_set
                if not bypass_global_cap and len(selected) >= self.max_active:
                    desk_ranked.append((inst, self._replace_row(row, reason="global_cap_deferred")))
                    continue
                if asset_id not in selected_ids:
                    selected.append(inst)
                    selected_ids.add(asset_id)
                    desk_selected += 1
                desk_ranked.append((inst, self._replace_row(row, selected=True, reason=reason)))

            selected_local = {str(inst.asset_id) for inst, row, _reason in retained if str(inst.asset_id) in selected_ids}
            # Add all non-selected rows for visibility.
            for inst, row in rows:
                if str(inst.asset_id) in selected_local:
                    continue
                if any(str(inst.asset_id) == str(existing.asset_id) for existing, _ in desk_ranked):
                    continue
                desk_ranked.append((inst, row))

            for desk_rank, (inst, row) in enumerate(desk_ranked, 1):
                global_rank_items.append((inst, self._replace_row(
                    row,
                    desk_rank=desk_rank,
                    selected=str(inst.asset_id) in selected_ids,
                )))
            notes.append(
                f"{desk_id}: quota={quota} selected={desk_selected}/{len(rows)} "
                f"retained={desk_retained} replaced={desk_replaced}"
            )

        global_rank_items.extend(rejected_rows)
        global_rank_items.sort(key=lambda x: (x[1].selected, x[1].score), reverse=True)
        for rank, (inst, row) in enumerate(global_rank_items, 1):
            out_rows.append(self._replace_row(row, rank=rank, selected=inst.asset_id in selected_ids))

        parked = [inst for inst, _ in global_rank_items if inst.asset_id not in selected_ids]
        if len(instruments) > len(selected):
            notes.append(f"stream load avoided for {len(instruments) - len(selected)} catalog instruments")
        if active_set:
            notes.append(f"previous_active={len(active_set)}")
        return TradableDeskSelection(time.time(), tuple(selected), tuple(parked), tuple(out_rows), tuple(notes))

    @staticmethod
    def _replace_row(row: TradableDeskRow, **updates: Any) -> TradableDeskRow:
        data = row.as_dict()
        data.update(updates)
        return TradableDeskRow(**data)

    def _load_delta_tickers(self, delta_api: Any) -> Dict[str, dict]:
        if delta_api is None:
            return {}
        try:
            resp = delta_api.get_tickers(contract_types=["perpetual_futures", "futures"])
            raw = resp.get("result") if isinstance(resp, dict) else None
            if isinstance(raw, dict):
                for key in ("result", "data", "tickers"):
                    if isinstance(raw.get(key), list):
                        raw = raw[key]
                        break
            if not isinstance(raw, list):
                return {}
            out: Dict[str, dict] = {}
            for row in raw:
                if not isinstance(row, dict):
                    continue
                sym = normalise_symbol(row.get("symbol") or row.get("product_symbol") or row.get("contract_symbol") or "")
                if sym:
                    out[sym] = row
            logger.info("TradableDesk: loaded %d Delta ticker snapshots without candle subscriptions", len(out))
            return out
        except Exception as exc:
            logger.warning("TradableDesk: Delta bulk ticker snapshot failed: %s", exc)
            return {}

    def _load_icici_quote_probes(self, instruments: Sequence[TradableInstrument], icici_api: Any, notes: list[str]) -> Dict[str, dict]:
        if icici_api is None or not bool(_cfg("DYNAMIC_DESK_ICICI_DETAILS_ENABLED", True)):
            return {}
        limit = max(0, int(_cfg("DYNAMIC_DESK_ICICI_QUOTE_PROBES", 8)))
        if limit <= 0:
            return {}
        candidates = [
            inst for inst in instruments
            if inst.primary_exchange == ExchangeName.ICICI and inst.asset_class == AssetClass.OPTION
        ]
        # Quote probes are not symbol-approved. They are prioritised by row quality:
        # derivative segment, non-structural underlying, usable strike/expiry/right,
        # and index-vs-stock desk balance. The final selection still requires live
        # Breeze quote data.
        candidates.sort(key=self._icici_quote_probe_priority, reverse=True)
        out: Dict[str, dict] = {}
        failures = 0
        for inst in candidates[:limit]:
            try:
                if hasattr(icici_api, "get_quote_for_instrument"):
                    out[str(inst.asset_id)] = icici_api.get_quote_for_instrument(inst.primary)
            except Exception as exc:
                failures += 1
                if failures == 1:
                    notes.append(f"icici option quote probe skipped/failed: {exc}")
                continue
        if out:
            notes.append(f"icici authenticated option quote probes={len(out)}")
            notes.append(f"icici authenticated quote probes={len(out)}")
        return out

    def _icici_quote_probe_priority(self, inst: TradableInstrument) -> float:
        raw = dict(getattr(getattr(inst, "primary", None), "raw", {}) or {})
        underlying = normalise_symbol(raw.get("stock_code") or raw.get("underlying") or raw.get("Underlying") or "")
        ex = normalise_symbol(raw.get("exchange_code") or raw.get("ExchangeCode") or "")
        product = normalise_symbol(raw.get("product_type") or raw.get("ProductType") or raw.get("InstrumentType") or raw.get("option_kind") or "")
        right = normalise_symbol(raw.get("right") or raw.get("option_type") or raw.get("OptionType") or "")
        strike = safe_float(raw.get("strike_price") or raw.get("StrikePrice") or raw.get("Strike"), 0.0)
        expiry = str(raw.get("expiry_date") or raw.get("ExpiryDate") or raw.get("Expiry") or "")
        structural = {ex, product, right, normalise_symbol(expiry), normalise_symbol(str(strike))}
        score = 0.0
        if ex in {"NFO", "BFO"}:
            score += 2.0
        if underlying and underlying not in structural:
            score += 2.0
        if strike > 0:
            score += 1.0
        if expiry:
            score += 1.0
        if right in {"CALL", "PUT", "CE", "PE", "C", "P"}:
            score += 0.8
        if self.router.desk_id_for(inst) == "ICICI_INDEX_OPTIONS":
            score += 0.4
        return score

    def _score_instrument(self, inst: TradableInstrument, *, delta_tickers: Mapping[str, dict], icici_quote: Optional[Mapping[str, Any]] = None, desk_id: str = "UNKNOWN", venue_routes: Sequence[Any] = ()) -> TradableDeskRow:
        primary = inst.primary
        venue = primary.exchange.value
        symbol = primary.symbol
        raw = dict(primary.raw or {})
        ticker = dict(delta_tickers.get(normalise_symbol(symbol), {})) if primary.exchange == ExchangeName.DELTA else {}
        quote = dict(icici_quote or {}) if primary.exchange == ExchangeName.ICICI else {}
        quote_success = quote.get("Success") if isinstance(quote.get("Success"), dict) else quote
        if isinstance(quote_success, list) and quote_success:
            quote_success = quote_success[0]
        if not isinstance(quote_success, dict):
            quote_success = {}
        merged = {**raw, **ticker, **quote_success}

        price = self._first_float(merged, ("mark_price", "markPrice", "spot_price", "last_price", "lastPrice", "close", "close_price", "price", "ltp"))
        bid = self._first_float(merged, ("best_bid", "bestBid", "bid", "bid_price", "best_bid_price"))
        ask = self._first_float(merged, ("best_ask", "bestAsk", "ask", "ask_price", "best_ask_price"))
        mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else price
        spread_bps = ((ask - bid) / mid * 10_000.0) if ask > 0 and bid > 0 and mid > 0 else 0.0

        turnover = self._turnover_proxy(merged, price)
        turnover_score = clamp(math.log10(max(1.0, turnover)) / 9.0)
        volatility_score = self._volatility_score(merged, price)
        spread_score = 0.68 if spread_bps <= 0 else clamp(1.0 - spread_bps / self._max_spread(inst))
        execution_score = self._execution_score(inst)
        class_score = self._asset_class_score(inst.asset_class)
        priority_score = clamp(1.0 - max(0.0, float(getattr(inst, "priority", 10_000))) / 10_000.0)
        live_score = 1.0 if (ticker or quote_success) else (0.72 if raw else 0.35)

        score = clamp(
            0.25 * turnover_score +
            0.18 * volatility_score +
            0.16 * spread_score +
            0.16 * execution_score +
            0.10 * live_score +
            0.07 * class_score +
            0.04 * priority_score
        )
        option_score = 0.0
        option_greeks = ""
        if primary.exchange == ExchangeName.ICICI and inst.asset_class == AssetClass.OPTION:
            opt = self.options.score_option(inst, quote_success)
            option_score = opt.score
            score = clamp(0.62 * opt.score + 0.18 * execution_score + 0.12 * spread_score + 0.08 * live_score)
            if opt.bs:
                option_greeks = f"Δ={opt.bs.delta:+.2f} θ/prem={opt.bs.theta_to_premium:.1%} dte={opt.dte:.1f}"
            raw["_option_selection"] = opt.as_dict()
            if bool(_cfg("ICICI_OPTION_REQUIRE_LIVE_QUOTE", True)) and not quote_success:
                score = 0.0

        details = self._icici_detail_text({**primary.raw, **quote_success}) if primary.exchange == ExchangeName.ICICI else ""
        preferred_route = venue_routes[0] if venue_routes else None
        route_exchange = str(getattr(getattr(preferred_route, "exchange", None), "value", "") or "")
        route_score = safe_float(getattr(preferred_route, "score", 0.0), 0.0)
        route_reason = str(getattr(preferred_route, "reason", "") or "")
        # Multi-venue crypto assets receive an execution-route quality boost;
        # venue availability improves execution quality, not alpha desk count.
        route_bonus = min(0.08, max(0.0, route_score - 0.50) * 0.16) if preferred_route else 0.0
        score = clamp(score + route_bonus)

        reasons = [desk_id.lower()]
        if ticker:
            reasons.append("live_bulk_ticker")
        if quote_success:
            reasons.append("icici_option_quote")
        if turnover_score >= 0.55:
            reasons.append("liquid")
        if volatility_score >= 0.55:
            reasons.append("moving")
        if spread_bps > self._max_spread(inst):
            reasons.append("wide_spread")
        if primary.exchange == ExchangeName.ICICI:
            reasons.append("options_only" if inst.asset_class == AssetClass.OPTION else "icici_non_option")

        return TradableDeskRow(
            asset_id=str(inst.asset_id),
            symbol=str(symbol),
            venue=venue,
            asset_class=str(getattr(inst.asset_class, "value", inst.asset_class)),
            route_exchange=route_exchange or venue,
            route_score=route_score,
            route_reason=route_reason,
            score=score,
            reason=",".join(reasons),
            price=price,
            spread_bps=max(0.0, spread_bps),
            turnover_score=turnover_score,
            volatility_score=volatility_score,
            execution_score=execution_score,
            desk_id=desk_id,
            icici_details=details,
            option_score=option_score,
            option_greeks=option_greeks,
        )

    @staticmethod
    def _first_float(row: Mapping[str, Any], names: Iterable[str]) -> float:
        for name in names:
            if name in row:
                f = safe_float(row.get(name), 0.0)
                if f > 0:
                    return f
        wanted = {str(x).lower() for x in names}
        for val in row.values():
            if isinstance(val, dict):
                for k, v in val.items():
                    if str(k).lower() in wanted:
                        f = safe_float(v, 0.0)
                        if f > 0:
                            return f
        return 0.0

    def _turnover_proxy(self, row: Mapping[str, Any], price: float) -> float:
        direct = self._first_float(row, ("turnover", "turnover_usd", "turnoverUsd", "quote_volume", "quoteVolume", "volume_usd", "volumeUsd", "notional_volume", "notionalVolume"))
        if direct > 0:
            return direct
        vol = self._first_float(row, ("volume", "volume_24h", "volume24h", "base_volume", "baseVolume", "total_quantity_traded", "total_traded_quantity"))
        if vol > 0 and price > 0:
            return vol * price
        oi = self._first_float(row, ("open_interest", "openInterest", "oi", "oi_value", "oiValue"))
        if oi > 0 and price > 0:
            return oi * price * 0.25
        return 0.0

    def _volatility_score(self, row: Mapping[str, Any], price: float) -> float:
        high = self._first_float(row, ("high", "high_price", "highPrice", "high_24h", "high24h"))
        low = self._first_float(row, ("low", "low_price", "lowPrice", "low_24h", "low24h"))
        if high > 0 and low > 0 and price > 0 and high > low:
            rng = (high - low) / price
            return clamp(1.0 - abs(rng - 0.035) / 0.07)
        change = abs(self._first_float(row, ("price_change_percent", "priceChangePercent", "change_percent", "changePercent", "change")))
        if change > 0:
            return clamp(change / 8.0)
        return 0.35

    def _execution_score(self, inst: TradableInstrument) -> float:
        exs = set(inst.by_exchange.keys())
        primary = inst.primary_exchange
        if primary in (ExchangeName.DELTA, ExchangeName.COINSWITCH, ExchangeName.COINDCX):
            base = 1.0
        elif primary == ExchangeName.ICICI:
            base = 0.84 if bool(_cfg("ICICI_ENABLED", False)) else 0.58
        else:
            base = 0.35
        if len(exs) > 1:
            base += 0.08
        return clamp(base)

    def _max_spread(self, inst: TradableInstrument) -> float:
        ac = str(getattr(inst.asset_class, "value", inst.asset_class) or "").lower()
        if "option" in ac:
            return float(_cfg("ICICI_OPTION_MAX_SPREAD_BPS", _cfg("FUND_MAX_SPREAD_BPS_OPTION", 180.0)))
        if "future" in ac:
            return float(_cfg("FUND_MAX_SPREAD_BPS_FUTURE", 65.0))
        if "equity" in ac or "index" in ac:
            return float(_cfg("FUND_MAX_SPREAD_BPS_EQUITY", 45.0))
        if "commodity" in ac:
            return float(_cfg("FUND_MAX_SPREAD_BPS_COMMODITY", 55.0))
        return float(_cfg("FUND_MAX_SPREAD_BPS_CRYPTO", 18.0))

    @staticmethod
    def _asset_class_score(asset_class: AssetClass | str) -> float:
        ac = str(getattr(asset_class, "value", asset_class) or "").lower()
        if ac in ("crypto", "commodity"):
            return 0.92
        if ac in ("future", "index"):
            return 0.82
        if ac == "equity":
            return 0.72
        if ac == "option":
            return 0.70
        return 0.50

    @staticmethod
    def _icici_detail_text(raw: Mapping[str, Any]) -> str:
        if not raw:
            return ""
        stock = raw.get("stock_code") or raw.get("StockCode") or raw.get("ShortName") or raw.get("symbol") or ""
        ex = raw.get("exchange_code") or raw.get("ExchangeCode") or raw.get("Exchange") or ""
        product = raw.get("product_type") or raw.get("ProductType") or raw.get("Series") or ""
        expiry = raw.get("expiry_date") or raw.get("ExpiryDate") or raw.get("Expiry") or ""
        strike = raw.get("strike_price") or raw.get("StrikePrice") or raw.get("Strike") or ""
        right = raw.get("right") or raw.get("OptionType") or raw.get("Right") or ""
        bits = [str(x) for x in (ex, stock, product, expiry, strike, right) if str(x or "").strip()]
        return " ".join(bits)[:120]

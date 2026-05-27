"""Cross-venue price and order-book normalisation."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Iterable, Mapping, Sequence

from market_data.feed_health import FeedHealth


DEFAULT_BANDS_BPS: tuple[tuple[float, float], ...] = (
    (0.0, 1.0),
    (1.0, 3.0),
    (3.0, 10.0),
    (10.0, 25.0),
)


def band_name(low_bps: float, high_bps: float) -> str:
    low = int(low_bps) if float(low_bps).is_integer() else low_bps
    high = int(high_bps) if float(high_bps).is_integer() else high_bps
    return f"{low}-{high}"


@dataclass(frozen=True)
class InstrumentMapping:
    venue: str
    venue_symbol: str
    canonical_underlying: str
    product_class: str
    quote_currency: str
    contract_multiplier: float
    settlement_currency: str
    price_tick: float
    qty_step: float
    execution_enabled: bool
    notional_model: str = "linear"

    def comparable_to(self, other: "InstrumentMapping") -> bool:
        return (
            self.canonical_underlying.upper() == other.canonical_underlying.upper()
            and self.quote_currency.upper() == other.quote_currency.upper()
            and self.settlement_currency.upper() == other.settlement_currency.upper()
        )


@dataclass(frozen=True)
class BookLevel:
    price: float
    displayed_size: float


@dataclass
class VenueMicrostate:
    venue: str
    symbol: str
    exchange_ts_ns: int | None
    receive_ts_ns: int
    feed_quality_score: float
    best_bid: float
    best_ask: float
    mid: float
    microprice: float
    spread_bps: float
    bid_depth_usd_by_band: dict[str, float]
    ask_depth_usd_by_band: dict[str, float]
    obi_by_band: dict[str, float]
    ofi_usd_1s: float
    ofi_usd_10s: float
    ofi_usd_60s: float
    tfi_usd_1s: float
    tfi_usd_10s: float
    tfi_usd_60s: float
    basis_bps: float | None
    funding_rate: float | None
    update_latency_ms: float | None
    sequence_valid: bool
    product_class: str = ""
    execution_enabled: bool = False
    metadata: dict[str, object] = field(default_factory=dict)

    @property
    def usable_for_decision(self) -> bool:
        return self.feed_quality_score > 0.0 and self.sequence_valid


def _num(value: object, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        if isinstance(value, str):
            value = value.strip().replace(",", "")
            if not value:
                return default
        out = float(value)
        return out if math.isfinite(out) else default
    except Exception:
        return default


def parse_levels(levels: Iterable[BookLevel | Sequence[object] | Mapping[str, object]]) -> list[BookLevel]:
    out: list[BookLevel] = []
    for level in levels or []:
        if isinstance(level, BookLevel):
            price = level.price
            size = level.displayed_size
        elif isinstance(level, Mapping):
            price = _num(level.get("price") or level.get("limit_price") or level.get("px"), 0.0)
            size = _num(level.get("size") or level.get("quantity") or level.get("qty") or level.get("displayed_size"), 0.0)
        else:
            vals = list(level)
            price = _num(vals[0] if len(vals) > 0 else None, 0.0)
            size = _num(vals[1] if len(vals) > 1 else None, 0.0)
        if price > 0 and size > 0:
            out.append(BookLevel(price=float(price), displayed_size=float(size)))
    return out


def mid_price(best_bid: float, best_ask: float) -> float:
    bid = float(best_bid)
    ask = float(best_ask)
    if bid <= 0 or ask <= 0 or ask < bid:
        raise ValueError(f"invalid top of book bid={best_bid} ask={best_ask}")
    return (bid + ask) / 2.0


def spread_bps(best_bid: float, best_ask: float) -> float:
    mid = mid_price(best_bid, best_ask)
    return (float(best_ask) - float(best_bid)) / mid * 10_000.0


def microprice(best_bid: float, best_ask: float, bid_qty: float, ask_qty: float) -> float:
    bq = max(float(bid_qty), 0.0)
    aq = max(float(ask_qty), 0.0)
    total = bq + aq
    if total <= 0:
        return mid_price(best_bid, best_ask)
    return (float(best_ask) * bq + float(best_bid) * aq) / total


def basis_bps(local_price: float, reference_price: float) -> float | None:
    if float(reference_price or 0.0) <= 0:
        return None
    return (float(local_price) / float(reference_price) - 1.0) * 10_000.0


def usd_notional_depth(
    *,
    displayed_size: float,
    level_price: float,
    mapping: InstrumentMapping,
    fx_conversion: float = 1.0,
) -> float:
    """Convert displayed book size to comparable USD notional.

    Linear products use qty * multiplier * price. Inverse contracts use the
    documented USD contract value: contracts * multiplier. Quanto contracts are
    rejected unless explicitly normalised upstream because a venue-specific
    payoff formula is required.
    """

    qty = abs(float(displayed_size))
    px = float(level_price)
    multiplier = float(mapping.contract_multiplier or 1.0)
    fx = float(fx_conversion or 1.0)
    model = str(mapping.notional_model or mapping.product_class or "linear").lower()
    if "quanto" in model:
        raise ValueError(f"quanto notional requires venue formula: {mapping.venue}:{mapping.venue_symbol}")
    if "inverse" in model:
        return qty * multiplier * fx
    return qty * multiplier * px * fx


def aggregate_depth_usd_by_band(
    *,
    levels: Iterable[BookLevel | Sequence[object] | Mapping[str, object]],
    side: str,
    mid: float,
    mapping: InstrumentMapping,
    bands_bps: tuple[tuple[float, float], ...] = DEFAULT_BANDS_BPS,
    fx_conversion: float = 1.0,
) -> dict[str, float]:
    m = float(mid)
    if m <= 0:
        raise ValueError("mid must be positive")
    out = {band_name(low, high): 0.0 for low, high in bands_bps}
    side_l = str(side or "").lower()
    for level in parse_levels(levels):
        if side_l == "bid":
            distance_bps = (m - level.price) / m * 10_000.0
        elif side_l == "ask":
            distance_bps = (level.price - m) / m * 10_000.0
        else:
            raise ValueError("side must be bid or ask")
        if distance_bps < -1e-9:
            continue
        for low, high in bands_bps:
            if low <= distance_bps < high:
                out[band_name(low, high)] += usd_notional_depth(
                    displayed_size=level.displayed_size,
                    level_price=level.price,
                    mapping=mapping,
                    fx_conversion=fx_conversion,
                )
                break
    return out


def order_book_imbalance_by_band(
    bid_depth_usd_by_band: Mapping[str, float],
    ask_depth_usd_by_band: Mapping[str, float],
) -> dict[str, float]:
    keys = sorted(set(bid_depth_usd_by_band) | set(ask_depth_usd_by_band))
    out: dict[str, float] = {}
    for key in keys:
        bid = max(float(bid_depth_usd_by_band.get(key, 0.0) or 0.0), 0.0)
        ask = max(float(ask_depth_usd_by_band.get(key, 0.0) or 0.0), 0.0)
        denom = bid + ask
        out[key] = (bid - ask) / denom if denom > 0 else 0.0
    return out


def build_venue_microstate(
    *,
    mapping: InstrumentMapping,
    bids: Iterable[BookLevel | Sequence[object] | Mapping[str, object]],
    asks: Iterable[BookLevel | Sequence[object] | Mapping[str, object]],
    feed_health: FeedHealth,
    receive_ts_ns: int,
    exchange_ts_ns: int | None = None,
    fx_conversion: float = 1.0,
    reference_price: float | None = None,
    ofi_usd_1s: float = 0.0,
    ofi_usd_10s: float = 0.0,
    ofi_usd_60s: float = 0.0,
    tfi_usd_1s: float = 0.0,
    tfi_usd_10s: float = 0.0,
    tfi_usd_60s: float = 0.0,
    funding_rate: float | None = None,
    metadata: Mapping[str, object] | None = None,
    bands_bps: tuple[tuple[float, float], ...] = DEFAULT_BANDS_BPS,
) -> VenueMicrostate:
    bid_levels = sorted(parse_levels(bids), key=lambda x: x.price, reverse=True)
    ask_levels = sorted(parse_levels(asks), key=lambda x: x.price)
    if not bid_levels or not ask_levels:
        raise ValueError(f"empty top of book for {mapping.venue}:{mapping.venue_symbol}")
    best_bid = bid_levels[0].price
    best_ask = ask_levels[0].price
    mid = mid_price(best_bid, best_ask)
    bid_band = aggregate_depth_usd_by_band(
        levels=bid_levels,
        side="bid",
        mid=mid,
        mapping=mapping,
        bands_bps=bands_bps,
        fx_conversion=fx_conversion,
    )
    ask_band = aggregate_depth_usd_by_band(
        levels=ask_levels,
        side="ask",
        mid=mid,
        mapping=mapping,
        bands_bps=bands_bps,
        fx_conversion=fx_conversion,
    )
    latency_ms = None
    if exchange_ts_ns is not None:
        latency_ms = max(0.0, (int(receive_ts_ns) - int(exchange_ts_ns)) / 1_000_000.0)
    ref_basis = basis_bps(mid, reference_price) if reference_price and reference_price > 0 else None
    return VenueMicrostate(
        venue=mapping.venue,
        symbol=mapping.venue_symbol,
        exchange_ts_ns=exchange_ts_ns,
        receive_ts_ns=int(receive_ts_ns),
        feed_quality_score=float(feed_health.quality_score),
        best_bid=best_bid,
        best_ask=best_ask,
        mid=mid,
        microprice=microprice(best_bid, best_ask, bid_levels[0].displayed_size, ask_levels[0].displayed_size),
        spread_bps=spread_bps(best_bid, best_ask),
        bid_depth_usd_by_band=bid_band,
        ask_depth_usd_by_band=ask_band,
        obi_by_band=order_book_imbalance_by_band(bid_band, ask_band),
        ofi_usd_1s=float(ofi_usd_1s),
        ofi_usd_10s=float(ofi_usd_10s),
        ofi_usd_60s=float(ofi_usd_60s),
        tfi_usd_1s=float(tfi_usd_1s),
        tfi_usd_10s=float(tfi_usd_10s),
        tfi_usd_60s=float(tfi_usd_60s),
        basis_bps=ref_basis,
        funding_rate=funding_rate,
        update_latency_ms=latency_ms,
        sequence_valid=bool(feed_health.sequence_valid),
        product_class=mapping.product_class,
        execution_enabled=bool(mapping.execution_enabled),
        metadata={"feed_health_reason": feed_health.reason},
    )


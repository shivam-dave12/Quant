"""Venue-independent price and notional normalisation using verified instrument mappings."""
from __future__ import annotations
from core.identifiers import BookLevel, InstrumentMapping, VenueMicrostate

BANDS = ((0.0, 1.0), (1.0, 3.0), (3.0, 10.0), (10.0, 25.0))

def band_name(low: float, high: float) -> str:
    return f"{low:g}-{high:g}"

def usd_notional(level: BookLevel, mapping: InstrumentMapping, fx_conversion: float = 1.0) -> float:
    mapping.validate()
    if level.price <= 0 or level.displayed_size < 0 or fx_conversion <= 0:
        raise ValueError("invalid order-book level or FX conversion")
    if mapping.notional_formula == "linear_base":
        return level.displayed_size * mapping.contract_multiplier * level.price * fx_conversion
    if mapping.notional_formula == "inverse_usd":
        # Inverse USD contract sizes are already quote-dollar contracts.
        return level.displayed_size * mapping.contract_multiplier * fx_conversion
    if mapping.notional_formula == "quote_notional":
        return level.displayed_size * fx_conversion
    raise ValueError("unsupported product notional formula")

def aggregate_depth(levels: list[BookLevel], mid: float, mapping: InstrumentMapping, fx_conversion: float = 1.0) -> dict[str, float]:
    if mid <= 0:
        raise ValueError("mid must be positive")
    out = {band_name(a, b): 0.0 for a, b in BANDS}
    for level in levels:
        distance_bps = abs(level.price / mid - 1.0) * 10_000.0
        for low, high in BANDS:
            if low <= distance_bps < high:
                out[band_name(low, high)] += usd_notional(level, mapping, fx_conversion)
                break
    return out

def microprice(best_bid: BookLevel, best_ask: BookLevel) -> float:
    denominator = best_bid.displayed_size + best_ask.displayed_size
    if denominator <= 0:
        return (best_bid.price + best_ask.price) / 2.0
    return (best_ask.price * best_bid.displayed_size + best_bid.price * best_ask.displayed_size) / denominator

def build_microstate(*, mapping: InstrumentMapping, bids: list[BookLevel], asks: list[BookLevel], receive_ts_ns: int,
                     exchange_ts_ns: int | None, feed_quality_score: float, sequence_valid: bool,
                     ofi: dict[str, float], tfi: dict[str, float], funding_rate: float | None = None,
                     basis_bps: float | None = None, fx_conversion: float = 1.0) -> VenueMicrostate:
    if not bids or not asks or bids[0].price >= asks[0].price:
        raise ValueError("valid uncrossed bid/ask snapshot required")
    bid, ask = bids[0], asks[0]
    mid = (bid.price + ask.price) / 2.0
    bid_depth = aggregate_depth(bids, mid, mapping, fx_conversion)
    ask_depth = aggregate_depth(asks, mid, mapping, fx_conversion)
    obi: dict[str, float] = {}
    for key in bid_depth:
        total = bid_depth[key] + ask_depth[key]
        obi[key] = 0.0 if total <= 0 else (bid_depth[key] - ask_depth[key]) / total
    latency_ms = None if exchange_ts_ns is None else max(0.0, (receive_ts_ns - exchange_ts_ns) / 1_000_000.0)
    return VenueMicrostate(
        mapping.venue, mapping.venue_symbol, exchange_ts_ns, receive_ts_ns, feed_quality_score,
        bid.price, ask.price, mid, microprice(bid, ask), (ask.price - bid.price) / mid * 10_000.0,
        bid_depth, ask_depth, obi, ofi.get("1s", 0.0), ofi.get("10s", 0.0), ofi.get("60s", 0.0),
        tfi.get("1s", 0.0), tfi.get("10s", 0.0), tfi.get("60s", 0.0), basis_bps, funding_rate,
        latency_ms, sequence_valid,
    )

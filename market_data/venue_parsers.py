"""Strict parsers that convert documented venue payloads into canonical market events."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any
from core.identifiers import BookLevel, InstrumentMapping
from market_data.orderbook import BookSnapshot
from market_data.trade_tape import Trade

@dataclass(frozen=True)
class ParsedVenueUpdate:
    snapshot: BookSnapshot | None = None
    trade: Trade | None = None
    funding_rate: float | None = None
    heartbeat: bool = False


def _ns(value: Any, *, unit: str) -> int | None:
    if value is None: return None
    raw = int(float(value))
    return raw * (1_000 if unit == "us" else 1_000_000 if unit == "ms" else 1)

def _levels(rows: Any) -> list[BookLevel]:
    return [BookLevel(float(row[0]), float(row[1])) for row in (rows or []) if len(row) >= 2 and float(row[0]) > 0 and float(row[1]) >= 0]

def parse_delta(raw: dict[str, Any], mapping: InstrumentMapping, receive_ts_ns: int) -> ParsedVenueUpdate:
    kind = str(raw.get("type", ""))
    if kind == "heartbeat": return ParsedVenueUpdate(heartbeat=True)
    if kind == "ob_l2" and str(raw.get("sy", "")).upper() == mapping.venue_symbol.upper():
        bids, asks = _levels(raw.get("b")), _levels(raw.get("a"))
        if not bids or not asks: raise ValueError("DELTA_OB_L2_SNAPSHOT_MISSING_SIDES")
        return ParsedVenueUpdate(snapshot=BookSnapshot(mapping, bids, asks, _ns(raw.get("ts") or raw.get("lts"), unit="us"), receive_ts_ns, True))
    if kind == "trades" and str(raw.get("sy", "")).upper() == mapping.venue_symbol.upper():
        # Delta buyer role: taker means aggressive buy; maker means aggressive sell.
        side = "BUY" if str(raw.get("r", "")).lower() == "t" else "SELL"
        return ParsedVenueUpdate(trade=Trade(_ns(raw.get("t") or raw.get("ts"), unit="us") or receive_ts_ns, float(raw["p"]), float(raw["s"]), side))
    if kind == "funding_rate" and str(raw.get("symbol", raw.get("sy", ""))).upper() == mapping.venue_symbol.upper():
        return ParsedVenueUpdate(funding_rate=float(raw.get("fr", raw.get("funding_rate", raw.get("r", 0))) or 0))
    return ParsedVenueUpdate()

def parse_hyperliquid(raw: dict[str, Any], mapping: InstrumentMapping, receive_ts_ns: int) -> ParsedVenueUpdate:
    channel, data = str(raw.get("channel", raw.get("type", ""))), raw.get("data", raw)
    if channel == "l2Book" and str(data.get("coin", "")).upper() == mapping.venue_symbol.upper():
        rows = data.get("levels") or [[], []]
        bids = [BookLevel(float(x["px"]), float(x["sz"])) for x in rows[0]]
        asks = [BookLevel(float(x["px"]), float(x["sz"])) for x in rows[1]]
        if not bids or not asks: raise ValueError("HYPERLIQUID_L2_SNAPSHOT_MISSING_SIDES")
        return ParsedVenueUpdate(snapshot=BookSnapshot(mapping, bids, asks, _ns(data.get("time"), unit="ms"), receive_ts_ns, True))
    if channel == "trades":
        rows = data if isinstance(data, list) else [data]
        for row in rows:
            if str(row.get("coin", "")).upper() == mapping.venue_symbol.upper():
                side = "BUY" if str(row.get("side", "")).upper() in {"B", "BUY"} else "SELL"
                return ParsedVenueUpdate(trade=Trade(_ns(row.get("time"), unit="ms") or receive_ts_ns, float(row["px"]), float(row["sz"]), side))
    return ParsedVenueUpdate()

def parse_coinswitch(raw: dict[str, Any], mapping: InstrumentMapping, receive_ts_ns: int) -> ParsedVenueUpdate:
    event, payload = str(raw.get("event", "")), raw.get("data", {})
    if event == "FETCH_ORDER_BOOK_CS_PRO":
        data = payload.get("data", payload)
        if str(data.get("symbol", "")).upper() != mapping.venue_symbol.upper(): return ParsedVenueUpdate()
        bids, asks = _levels(data.get("bids")), _levels(data.get("asks"))
        if not bids or not asks: raise ValueError("COINSWITCH_ORDERBOOK_SNAPSHOT_MISSING_SIDES")
        return ParsedVenueUpdate(snapshot=BookSnapshot(mapping, bids, asks, _ns(data.get("timestamp"), unit="ms"), receive_ts_ns, True))
    if event == "FETCH_TRADES_CS_PRO":
        rows = payload.get("data", payload) if isinstance(payload, dict) else payload
        for row in (rows if isinstance(rows, list) else [rows]):
            if str(row.get("s", "")).upper() == mapping.venue_symbol.upper():
                return ParsedVenueUpdate(trade=Trade(_ns(row.get("E"), unit="ms") or receive_ts_ns, float(row["p"]), float(row["q"]), "SELL" if bool(row.get("m")) else "BUY"))
    if event == "FETCH_TICKER_INFO_CS_PRO":
        row = payload.get(mapping.venue_symbol) or payload.get(mapping.venue_symbol.upper()) or {}
        if row: return ParsedVenueUpdate(funding_rate=float(row.get("r", 0) or 0))
    return ParsedVenueUpdate()

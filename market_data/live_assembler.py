"""Normalised real-time venue state assembly and quantitative L2 liquidity-zone extraction."""
from __future__ import annotations
from dataclasses import dataclass
from core.identifiers import InstrumentMapping, VenueMicrostate
from intelligence.liquidity_intelligence import ZoneObservation
from market_data.feed_health import FeedHealthMonitor
from market_data.normalizer import usd_notional
from market_data.orderbook import BookSnapshot, OrderBookState
from market_data.trade_tape import Trade, TradeTape, OrderFlowTracker

@dataclass(frozen=True)
class LiveVenueStatus:
    microstate: VenueMicrostate | None
    snapshot: BookSnapshot | None
    quality: float

class NormalisedVenueAssembler:
    def __init__(self) -> None:
        self.books = OrderBookState(); self.health = FeedHealthMonitor(); self.snapshots: dict[tuple[str, str], BookSnapshot] = {}
        self.tapes: dict[tuple[str, str], TradeTape] = {}; self.ofi: dict[tuple[str, str], OrderFlowTracker] = {}
        self.funding: dict[tuple[str, str], float] = {}; self._previous_depth: dict[tuple[str, str], float] = {}
    def apply_snapshot(self, snapshot: BookSnapshot, *, latency_ms: float | None, connected: bool = True, heartbeat_ok: bool = True) -> LiveVenueStatus:
        key = (snapshot.mapping.venue, snapshot.mapping.venue_symbol)
        current_depth = sum(usd_notional(x, snapshot.mapping) for x in snapshot.bids[:5]) - sum(usd_notional(x, snapshot.mapping) for x in snapshot.asks[:5])
        tracker = self.ofi.setdefault(key, OrderFlowTracker()); tracker.append(snapshot.receive_ts_ns, current_depth - self._previous_depth.get(key, current_depth)); self._previous_depth[key] = current_depth
        self.books.update(snapshot); self.snapshots[key] = snapshot; self.tapes.setdefault(key, TradeTape(snapshot.mapping))
        quality = self.health.assess(snapshot.mapping.venue, connected=connected, heartbeat_ok=heartbeat_ok, sequence_valid=snapshot.sequence_valid,
            snapshot_ready=True, exchange_timestamp_available=snapshot.exchange_ts_ns is not None, latency_ms=latency_ms, no_change_heartbeat_valid=heartbeat_ok)
        micro = self.books.microstate(snapshot.mapping.venue, snapshot.mapping.venue_symbol, feed_quality_score=quality.quality_score,
            ofi=tracker.ofi(snapshot.receive_ts_ns), tfi=self.tapes[key].tfi(snapshot.receive_ts_ns), funding_rate=self.funding.get(key))
        return LiveVenueStatus(micro, snapshot, quality.quality_score)
    def apply_trade(self, mapping: InstrumentMapping, trade: Trade) -> None:
        self.tapes.setdefault((mapping.venue, mapping.venue_symbol), TradeTape(mapping)).append(trade)
    def apply_funding(self, mapping: InstrumentMapping, funding_rate: float) -> None:
        self.funding[(mapping.venue, mapping.venue_symbol)] = funding_rate
    @staticmethod
    def liquidity_zones(snapshot: BookSnapshot, *, max_levels: int = 8) -> list[ZoneObservation]:
        zones: list[ZoneObservation] = []
        for level in [*snapshot.bids[:max_levels], *snapshot.asks[:max_levels]]:
            width = max(snapshot.mapping.price_tick, level.price * 0.00001)
            zones.append(ZoneObservation(level.price - width / 2, level.price + width / 2, ["L2_REALTIME"], 0.0, 0, 0,
                usd_notional(level, snapshot.mapping), 0.0, 0.0))
        return zones

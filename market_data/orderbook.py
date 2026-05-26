"""Order-book snapshot holder that never merges non-equivalent venue books."""
from __future__ import annotations
from dataclasses import dataclass
from core.identifiers import BookLevel, InstrumentMapping, VenueMicrostate
from market_data.normalizer import build_microstate

@dataclass(frozen=True)
class BookSnapshot:
    mapping: InstrumentMapping
    bids: list[BookLevel]
    asks: list[BookLevel]
    exchange_ts_ns: int | None
    receive_ts_ns: int
    sequence_valid: bool = True

class OrderBookState:
    def __init__(self) -> None:
        self._snapshots: dict[tuple[str, str], BookSnapshot] = {}
    def update(self, snapshot: BookSnapshot) -> None:
        if not snapshot.sequence_valid: raise ValueError("invalid sequence snapshots are not accepted")
        self._snapshots[(snapshot.mapping.venue, snapshot.mapping.venue_symbol)] = snapshot
    def microstate(self, venue: str, symbol: str, *, feed_quality_score: float, ofi: dict[str, float] | None = None,
                   tfi: dict[str, float] | None = None, basis_bps: float | None = None, funding_rate: float | None = None) -> VenueMicrostate:
        row = self._snapshots[(venue, symbol)]
        return build_microstate(mapping=row.mapping, bids=row.bids, asks=row.asks, exchange_ts_ns=row.exchange_ts_ns,
                                receive_ts_ns=row.receive_ts_ns, feed_quality_score=feed_quality_score,
                                sequence_valid=row.sequence_valid, ofi=ofi, tfi=tfi, basis_bps=basis_bps, funding_rate=funding_rate)

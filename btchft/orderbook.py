from __future__ import annotations

import time
import zlib
from dataclasses import dataclass
from typing import Any

from .types import BookSnapshot


def _as_exchange_str(x: float | str | int) -> str:
    # Delta checksum uses the string form in the book. Preserve strings from payload.
    if isinstance(x, str):
        return x
    if isinstance(x, int):
        return str(x)
    return f"{float(x):.12f}".rstrip("0").rstrip(".")


@dataclass
class BookIntegrityState:
    snapshots: int = 0
    updates: int = 0
    checksum_failures: int = 0
    sequence_gaps: int = 0
    crossed_books: int = 0
    last_seq: int | None = None
    halted: bool = False
    halt_reason: str | None = None


class DeltaOrderBook:
    """Delta ob_updates local book with sequence and CRC32 checksum validation."""

    def __init__(self, symbol: str = "BTCUSD") -> None:
        self.symbol = symbol.upper()
        # price -> (size_float, price_str, size_str)
        self.asks: dict[float, tuple[float, str, str]] = {}
        self.bids: dict[float, tuple[float, str, str]] = {}
        self.state = BookIntegrityState()

    @staticmethod
    def checksum_for(asks: list[tuple[float | str, float | str]], bids: list[tuple[float | str, float | str]]) -> int:
        ask_s = ",".join(f"{_as_exchange_str(p)}:{_as_exchange_str(s)}" for p, s in asks[:10])
        bid_s = ",".join(f"{_as_exchange_str(p)}:{_as_exchange_str(s)}" for p, s in bids[:10])
        return zlib.crc32(f"{ask_s}|{bid_s}".encode("utf-8")) & 0xFFFFFFFF

    def _sorted_float_levels(self, side: dict[float, tuple[float, str, str]], reverse: bool = False) -> list[tuple[float, float]]:
        return [(float(p), float(v[0])) for p, v in sorted(side.items(), key=lambda x: x[0], reverse=reverse)]

    def _sorted_checksum_levels(self, side: dict[float, tuple[float, str, str]], reverse: bool = False) -> list[tuple[str, str]]:
        return [(v[1], v[2]) for _, v in sorted(side.items(), key=lambda x: x[0], reverse=reverse)]

    def _apply_levels(self, side: dict[float, tuple[float, str, str]], rows: list[list[str | float | int]]) -> None:
        for row in rows or []:
            if len(row) < 2:
                continue
            price_s = _as_exchange_str(row[0]); size_s = _as_exchange_str(row[1])
            price = float(price_s); size = float(size_s)
            if size <= 0:
                side.pop(price, None)
            else:
                side[price] = (size, price_s, size_s)

    def apply(self, msg: dict[str, Any], receive_ts_ns: int | None = None) -> BookSnapshot | None:
        if self.state.halted:
            return None
        if msg.get("type") != "ob_updates":
            return None
        sy = str(msg.get("sy", msg.get("symbol", self.symbol))).upper()
        if sy != self.symbol:
            return None
        action = str(msg.get("action", "")).lower()
        seq = int(msg.get("seq")) if msg.get("seq") is not None else None
        if action == "snapshot":
            self.asks.clear(); self.bids.clear()
            self._apply_levels(self.asks, msg.get("a") or [])
            self._apply_levels(self.bids, msg.get("b") or [])
            self.state.snapshots += 1
            self.state.last_seq = seq
        elif action == "update":
            if self.state.last_seq is None:
                self.state.sequence_gaps += 1
                self._halt("update_before_snapshot")
                return None
            if seq is not None and seq != self.state.last_seq + 1:
                self.state.sequence_gaps += 1
                self._halt(f"sequence_gap expected={self.state.last_seq + 1} got={seq}")
                return None
            self._apply_levels(self.asks, msg.get("a") or [])
            self._apply_levels(self.bids, msg.get("b") or [])
            self.state.updates += 1
            self.state.last_seq = seq
        elif action == "error":
            self._halt(f"exchange_orderbook_error:{msg.get('msg')}")
            return None
        else:
            return None

        asks_float = self._sorted_float_levels(self.asks, reverse=False)
        bids_float = self._sorted_float_levels(self.bids, reverse=True)
        if not asks_float or not bids_float:
            return None
        if bids_float[0][0] >= asks_float[0][0]:
            self.state.crossed_books += 1
            self._halt(f"crossed_book bid={bids_float[0][0]} ask={asks_float[0][0]}")
            return None
        cs = msg.get("cs")
        if cs is not None:
            ask_cs = self._sorted_checksum_levels(self.asks, reverse=False)
            bid_cs = self._sorted_checksum_levels(self.bids, reverse=True)
            if self.checksum_for(ask_cs, bid_cs) != int(cs):
                self.state.checksum_failures += 1
                self._halt("checksum_failure")
                return None
        ex_ts = msg.get("ts")
        ex_ts_ns = int(ex_ts) * 1000 if ex_ts and int(ex_ts) < 10**17 else int(ex_ts or time.time_ns())
        return BookSnapshot(
            venue="DELTA", symbol=self.symbol,
            best_bid=bids_float[0][0], best_ask=asks_float[0][0],
            bid_size_1=bids_float[0][1], ask_size_1=asks_float[0][1],
            bids=bids_float[:50], asks=asks_float[:50],
            exchange_ts_ns=ex_ts_ns, receive_ts_ns=int(receive_ts_ns or time.time_ns()),
            seq=seq, checksum=int(cs) if cs is not None else None,
        )

    def _halt(self, reason: str) -> None:
        self.state.halted = True
        self.state.halt_reason = reason

    def integrity(self) -> dict[str, Any]:
        return self.state.__dict__.copy()

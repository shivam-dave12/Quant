from __future__ import annotations

from dataclasses import dataclass, asdict
from enum import Enum
from typing import Any


class Side(str, Enum):
    LONG = "long"
    SHORT = "short"
    FLAT = "flat"


@dataclass(frozen=True)
class TradeTick:
    venue: str
    symbol: str
    price: float
    size_contracts: float
    buyer_role: str | None
    exchange_ts_ns: int
    receive_ts_ns: int
    raw: dict[str, Any] | None = None

    @property
    def signed_size(self) -> float:
        # Delta public trades: buyer_role=taker means aggressive buy; buyer_role=maker means aggressive sell.
        role = (self.buyer_role or "").lower()
        if role == "taker":
            return float(self.size_contracts)
        if role == "maker":
            return -float(self.size_contracts)
        return 0.0


@dataclass(frozen=True)
class BookSnapshot:
    venue: str
    symbol: str
    best_bid: float
    best_ask: float
    bid_size_1: float
    ask_size_1: float
    bids: list[tuple[float, float]]
    asks: list[tuple[float, float]]
    exchange_ts_ns: int
    receive_ts_ns: int
    seq: int | None = None
    checksum: int | None = None

    @property
    def mid(self) -> float:
        return (self.best_bid + self.best_ask) / 2.0

    @property
    def spread_bps(self) -> float:
        return (self.best_ask - self.best_bid) / max(self.mid, 1e-12) * 1e4


@dataclass(frozen=True)
class AlphaDecision:
    ts_ns: int
    side: Side
    expected_net_edge_bps: float
    confidence: float
    source: str
    features: dict[str, Any]
    diagnostics: dict[str, Any]


@dataclass(frozen=True)
class BracketPlan:
    side: Side
    entry_price: float
    stop_price: float
    take_profit_price: float
    quantity_contracts: int
    risk_usd: float
    expected_net_edge_bps: float
    confidence: float
    rationale: dict[str, Any]


@dataclass(frozen=True)
class FillRecord:
    fill_id: str
    order_id: str
    symbol: str
    side: str
    role: str
    size_contracts: int
    price: float
    commission_settling: float
    receive_ts_ns: int
    exchange_created_at: str | None = None
    decision_mid: float | None = None
    contract_value_btc: float = 0.001

    @property
    def notional_usd(self) -> float:
        return abs(self.size_contracts) * self.contract_value_btc * self.price

    @property
    def fee_bps(self) -> float:
        return abs(self.commission_settling) / max(self.notional_usd, 1e-12) * 1e4

    @property
    def slippage_bps(self) -> float | None:
        if self.decision_mid is None or self.decision_mid <= 0:
            return None
        direction = 1.0 if self.side.lower() == "buy" else -1.0
        return direction * (self.price / self.decision_mid - 1.0) * 1e4

    def to_json(self) -> dict[str, Any]:
        row = asdict(self)
        row["notional_usd"] = self.notional_usd
        row["fee_bps"] = self.fee_bps
        row["slippage_bps"] = self.slippage_bps
        return row

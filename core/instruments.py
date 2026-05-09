"""
core/instruments.py — live-exchange instrument model and symbol context
========================================================================

No symbol in this module is treated as executable by itself.  Alias lists are
only search keys used to match the live product catalogs returned by Delta and
CoinSwitch.  An instrument becomes tradeable only when the exchange confirms it
through its own product/instrument endpoint.
"""
from __future__ import annotations

import contextlib
import contextvars
import math
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Iterable, Iterator, List, Optional, Tuple


class AssetClass(str, Enum):
    CRYPTO = "crypto"
    COMMODITY = "commodity"
    INDEX = "index"
    EQUITY = "equity"
    OPTION = "option"
    FUTURE = "future"
    CASH = "cash"


class ExchangeName(str, Enum):
    DELTA = "delta"
    COINSWITCH = "coinswitch"
    ICICI = "icici"
    COINDCX = "coindcx"


@dataclass(frozen=True)
class AssetIntent:
    """Requested market family; not executable until matched to exchange catalog."""
    asset_id: str
    display_name: str
    asset_class: AssetClass
    aliases: Tuple[str, ...]
    priority: int = 100

    def alias_set(self) -> set[str]:
        out = {normalise_symbol(self.asset_id), normalise_symbol(self.display_name)}
        out.update(normalise_symbol(a) for a in self.aliases)
        return {a for a in out if a}


@dataclass(frozen=True)
class ExchangeInstrument:
    """A contract confirmed by one exchange's live catalog."""
    exchange: ExchangeName
    symbol: str                    # execution REST symbol, e.g. BTCUSD / BTCUSDT
    ws_symbol: str                 # websocket symbol, e.g. BTCUSD / BTC/USDT
    display_symbol: str            # human display
    asset_id: str
    asset_class: AssetClass
    product_id: Optional[int] = None
    quote_asset: str = ""
    base_asset: str = ""
    contract_type: str = ""
    status: str = ""
    tick_size: float = 0.0
    lot_step: float = 0.0
    min_qty: float = 0.0
    max_qty: float = 0.0
    contract_value_btc: float = 0.0
    max_leverage: float = 0.0
    raw: Dict = field(default_factory=dict, compare=False, repr=False)

    @property
    def is_active(self) -> bool:
        st = str(self.status or "").lower()
        return st in ("", "active", "live", "trading", "enabled", "open")


@dataclass(frozen=True)
class TradableInstrument:
    """Cross-exchange view for a requested asset."""
    asset_id: str
    display_name: str
    asset_class: AssetClass
    primary_exchange: ExchangeName
    by_exchange: Dict[ExchangeName, ExchangeInstrument]
    priority: int = 100

    def for_exchange(self, exchange: str | ExchangeName) -> Optional[ExchangeInstrument]:
        ex = exchange if isinstance(exchange, ExchangeName) else ExchangeName(str(exchange).lower())
        return self.by_exchange.get(ex)

    @property
    def primary(self) -> ExchangeInstrument:
        return self.by_exchange[self.primary_exchange]

    @property
    def display_symbol(self) -> str:
        return self.primary.display_symbol

    @property
    def execution_symbol(self) -> str:
        return self.primary.symbol

    @property
    def tick_size(self) -> float:
        return first_positive(self.primary.tick_size, 0.0)

    @property
    def lot_step(self) -> float:
        return first_positive(self.primary.lot_step, 0.0)

    @property
    def min_qty(self) -> float:
        return first_positive(self.primary.min_qty, 0.0)

    @property
    def max_qty(self) -> float:
        return first_positive(self.primary.max_qty, 0.0)

    @property
    def max_leverage(self) -> float:
        vals = [first_positive(ei.max_leverage, 0.0) for ei in self.by_exchange.values()]
        vals = [v for v in vals if v > 0]
        primary = first_positive(self.primary.max_leverage, 0.0)
        return primary if primary > 0 else (max(vals) if vals else 0.0)


_CURRENT_INSTRUMENT: contextvars.ContextVar[Optional[TradableInstrument]] = (
    contextvars.ContextVar("current_tradable_instrument", default=None)
)


def current_instrument() -> Optional[TradableInstrument]:
    return _CURRENT_INSTRUMENT.get()


@contextlib.contextmanager
def instrument_scope(instrument: Optional[TradableInstrument]) -> Iterator[None]:
    token = _CURRENT_INSTRUMENT.set(instrument)
    try:
        yield
    finally:
        _CURRENT_INSTRUMENT.reset(token)


def normalise_symbol(value: str) -> str:
    return "".join(ch for ch in str(value or "").upper() if ch.isalnum())


def slash_symbol(value: str) -> str:
    v = str(value or "").upper().replace("/", "").replace("-", "")
    for quote in ("USDT", "USD", "INR"):
        if v.endswith(quote) and len(v) > len(quote):
            return f"{v[:-len(quote)]}/{quote}"
    return str(value or "").upper()


def first_positive(*values: float) -> float:
    for v in values:
        try:
            f = float(v)
            if math.isfinite(f) and f > 0:
                return f
        except Exception:
            continue
    return 0.0


def default_asset_intents() -> List[AssetIntent]:
    """Legacy explicit requests.

    The production universe is now discovered from venue catalogs. This returns
    an empty list so the scanner cannot silently fall back to an old fixed
    symbol basket when no explicit request list is provided.
    """
    return []

def configured_asset_intents(raw: Iterable[dict] | None = None) -> List[AssetIntent]:
    if not raw:
        return default_asset_intents()
    out: List[AssetIntent] = []
    for i, item in enumerate(raw):
        try:
            ac = AssetClass(str(item.get("asset_class", item.get("class", "crypto"))).lower())
        except Exception:
            ac = AssetClass.CRYPTO
        aliases = item.get("aliases") or []
        out.append(AssetIntent(
            asset_id=str(item.get("asset_id") or item.get("id") or item.get("symbol") or "").upper(),
            display_name=str(item.get("display_name") or item.get("name") or item.get("asset_id") or ""),
            asset_class=ac,
            aliases=tuple(str(a) for a in aliases),
            priority=int(item.get("priority", i + 100)),
        ))
    return [x for x in out if x.asset_id]

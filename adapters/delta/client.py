"""Delta India metadata and native-protected execution client with fill reconciliation."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Protocol
import requests
from core.identifiers import InstrumentMapping

class DeltaProtectedTransport(Protocol):
    def place_protected_order(self, *, product_id: int, product_symbol: str, side: str, quantity: float, entry_price: float, stop_price: float, target_price: float) -> dict[str, Any]: ...
    def get_order(self, order_id: str) -> dict[str, Any]: ...
    def get_position(self, product_id: int) -> dict[str, Any]: ...

@dataclass(frozen=True)
class DeltaProduct:
    product_id: int
    mapping: InstrumentMapping
    maximum_leverage: float | None
    raw: dict[str, Any]

class DeltaAdapter:
    BASE_URL = "https://api.india.delta.exchange/v2"
    def __init__(self, *, session: Any | None = None, protected_transport: DeltaProtectedTransport | None = None, timeout: float = 10.0) -> None:
        self.session = session or requests.Session(); self.protected_transport = protected_transport; self.timeout = timeout
    @classmethod
    def with_signed_native_brackets(cls, api_key: str, api_secret: str, *, session: Any | None = None, timeout: float = 10.0) -> "DeltaAdapter":
        from adapters.delta.execution import DeltaSignedBracketTransport
        return cls(session=session, protected_transport=DeltaSignedBracketTransport(api_key, api_secret, session=session), timeout=timeout)
    def _get(self, path: str) -> Any:
        response = self.session.get(f"{self.BASE_URL}{path}", timeout=self.timeout, headers={"Accept": "application/json"}); response.raise_for_status(); payload = response.json()
        if not payload.get("success", False): raise RuntimeError(f"DELTA_API_REJECTED:{payload.get('error')}")
        return payload.get("result")
    def products(self) -> list[dict[str, Any]]: return list(self._get("/products") or [])
    def executable_product(self, symbol: str) -> DeltaProduct:
        for row in self.products():
            if str(row.get("symbol", "")).upper() != symbol.upper(): continue
            pid = int(row.get("id", 0) or 0); tick = float(row.get("tick_size", 0) or 0); step = float(row.get("minimum_order_size", 0) or 0)
            multiplier = float(row.get("contract_value", row.get("contract_multiplier", 0)) or 0)
            if min(pid, tick, step, multiplier) <= 0: raise RuntimeError(f"INCOMPLETE_DELTA_PRODUCT_METADATA:{symbol}")
            underlying = row.get("underlying_asset", {}); underlying = underlying.get("symbol", symbol) if isinstance(underlying, dict) else str(underlying)
            mapping = InstrumentMapping("delta", symbol.upper(), underlying.upper(), str(row.get("contract_type", "future")).lower(), "USD", multiplier,
                str((row.get("settling_asset") or {}).get("symbol", "USD")) if isinstance(row.get("settling_asset"), dict) else "USD",
                tick, step, True, str(row.get("notional_formula", "linear_base")), {"product_id": pid, **row})
            mapping.validate(); leverage = row.get("maximum_leverage") or row.get("max_leverage")
            return DeltaProduct(pid, mapping, float(leverage) if leverage else None, dict(row))
        raise RuntimeError(f"DELTA_PRODUCT_NOT_VERIFIED:{symbol}")
    def place_protected_order(self, **kwargs: Any) -> dict[str, Any]:
        if self.protected_transport is None: raise RuntimeError("DELTA_PROTECTED_TRANSPORT_NOT_VALIDATED")
        return self.protected_transport.place_protected_order(**kwargs)
    def get_order(self, order_id: str) -> dict[str, Any]:
        if self.protected_transport is None: raise RuntimeError("DELTA_PROTECTED_TRANSPORT_NOT_VALIDATED")
        return self.protected_transport.get_order(order_id)
    def get_position(self, product_id: int) -> dict[str, Any]:
        if self.protected_transport is None: raise RuntimeError("DELTA_PROTECTED_TRANSPORT_NOT_VALIDATED")
        return self.protected_transport.get_position(product_id)

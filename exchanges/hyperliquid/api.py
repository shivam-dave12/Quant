"""Hyperliquid SDK adapter.

The repo uses the official ``hyperliquid-python-sdk`` for signing.  Direct
signature construction is intentionally avoided because wrong EIP-712 payloads
can look like intermittent exchange failures.
"""

from __future__ import annotations

import logging
import math
from typing import Any, Dict, Iterable, List, Optional

try:
    import config
except Exception:  # pragma: no cover
    config = None  # type: ignore

try:
    from eth_account import Account
    from hyperliquid.exchange import Exchange
    from hyperliquid.info import Info
    from hyperliquid.utils import constants
except Exception:  # pragma: no cover
    Account = None  # type: ignore
    Exchange = None  # type: ignore
    Info = None  # type: ignore
    constants = None  # type: ignore

logger = logging.getLogger(__name__)


def _cfg(name: str, default: Any) -> Any:
    return getattr(config, name, default) if config is not None else default


def _perp_dexs(raw: Any = None) -> list[str]:
    raw = _cfg("HYPERLIQUID_PERP_DEXS", ("", "xyz", "km")) if raw is None else raw
    vals = raw.replace(";", ",").split(",") if isinstance(raw, str) else list(raw or [])
    out: list[str] = []
    for value in vals:
        v = str(value or "").strip()
        if v not in out:
            out.append(v)
    return out or [""]


class HyperliquidAPI:
    """Small production wrapper around the official Hyperliquid SDK."""

    venue = "hyperliquid"

    def __init__(
        self,
        *,
        private_key: str = "",
        account_address: str = "",
        api_wallet_address: str = "",
        testnet: bool | None = None,
        perp_dexs: Optional[Iterable[str]] = None,
        timeout: float | None = None,
    ) -> None:
        if Info is None or constants is None:
            raise RuntimeError("hyperliquid-python-sdk is not installed")
        use_testnet = bool(_cfg("HYPERLIQUID_TESTNET", False) if testnet is None else testnet)
        self.base_url = constants.TESTNET_API_URL if use_testnet else constants.MAINNET_API_URL
        self.perp_dexs = _perp_dexs(perp_dexs)
        self.info = Info(
            self.base_url,
            skip_ws=True,
            perp_dexs=self.perp_dexs,
            timeout=timeout or float(_cfg("REQUEST_TIMEOUT", 30.0)),
        )
        self.wallet = None
        self.exchange = None
        self.account_address = str(account_address or "").strip()
        self.api_wallet_address = str(api_wallet_address or "").strip()

        key = str(private_key or "").strip()
        if key:
            if Account is None or Exchange is None:
                raise RuntimeError("eth-account/hyperliquid SDK dependencies are not installed")
            if not key.startswith("0x"):
                key = "0x" + key
            self.wallet = Account.from_key(key)
            derived = str(getattr(self.wallet, "address", "") or "").lower()
            if self.api_wallet_address and derived and derived != self.api_wallet_address.lower():
                logger.warning(
                    "Hyperliquid API wallet address mismatch: env=%s derived=%s",
                    self.api_wallet_address,
                    derived,
                )
            if not self.account_address:
                self.account_address = str(getattr(self.wallet, "address", "") or "")
            self.exchange = Exchange(
                self.wallet,
                base_url=self.base_url,
                account_address=self.account_address or None,
                perp_dexs=self.perp_dexs,
                timeout=timeout or float(_cfg("REQUEST_TIMEOUT", 30.0)),
            )
        logger.info(
            "HyperliquidAPI initialised testnet=%s account=%s api_wallet=%s dexs=%s trading=%s",
            use_testnet,
            self.account_address[:10] + "..." if self.account_address else "none",
            self.api_wallet_address[:10] + "..." if self.api_wallet_address else "derived",
            ",".join(self.perp_dexs),
            bool(self.exchange is not None),
        )

    @classmethod
    def from_config(cls) -> "HyperliquidAPI":
        return cls(
            private_key=str(_cfg("HYPERLIQUID_PRIVATE_KEY", "") or ""),
            account_address=str(_cfg("HYPERLIQUID_MAIN_API_KEY", "") or ""),
            api_wallet_address=str(_cfg("HYPERLIQUID_WALLET_API_KEY", "") or ""),
            testnet=bool(_cfg("HYPERLIQUID_TESTNET", False)),
            perp_dexs=_perp_dexs(),
        )

    def _require_exchange(self):
        if self.exchange is None:
            raise RuntimeError("Hyperliquid private key is not configured; execution is unavailable")
        return self.exchange

    def meta_by_dex(self) -> dict[str, dict]:
        out: dict[str, dict] = {}
        for dex in self.perp_dexs:
            try:
                out[dex] = self.info.meta(dex=dex)
            except Exception as exc:
                logger.warning("Hyperliquid meta fetch failed dex=%r: %s", dex, exc)
        return out

    def all_mids(self) -> Dict[str, Any]:
        try:
            return dict(self.info.all_mids() or {})
        except Exception:
            return {}

    def size_decimals(self, coin: str) -> int:
        try:
            asset = self.info.name_to_asset(str(coin))
            return int(self.info.asset_to_sz_decimals.get(asset, 5))
        except Exception:
            return 5

    def round_size(self, coin: str, size: float) -> float:
        decimals = max(0, min(8, self.size_decimals(coin)))
        scale = 10 ** decimals
        rounded = math.floor(max(0.0, float(size or 0.0)) * scale) / scale
        return float(f"{rounded:.{decimals}f}")

    @staticmethod
    def _statuses(resp: Any) -> list:
        if not isinstance(resp, dict):
            return []
        data = (((resp.get("response") or {}).get("data") or {}) if isinstance(resp.get("response"), dict) else {})
        statuses = data.get("statuses") if isinstance(data, dict) else None
        return list(statuses or [])

    @classmethod
    def first_order_result(cls, resp: Any) -> dict[str, Any]:
        statuses = cls._statuses(resp)
        if not statuses:
            return {"ok": False, "error": str(resp)[:500], "raw": resp}
        first = statuses[0]
        if isinstance(first, dict) and "error" in first:
            return {"ok": False, "error": str(first.get("error")), "raw": resp}
        if isinstance(first, dict) and isinstance(first.get("resting"), dict):
            return {"ok": True, "status": "RESTING", "oid": int(first["resting"].get("oid")), "raw": resp}
        if isinstance(first, dict) and isinstance(first.get("filled"), dict):
            filled = first["filled"]
            return {
                "ok": True,
                "status": "FILLED",
                "oid": int(filled.get("oid")),
                "avg_px": float(filled.get("avgPx") or 0.0),
                "total_sz": float(filled.get("totalSz") or 0.0),
                "raw": resp,
            }
        return {"ok": False, "error": str(first)[:500], "raw": resp}

    @classmethod
    def child_order_ids(cls, resp: Any) -> list[int]:
        oids: list[int] = []
        for row in cls._statuses(resp):
            if isinstance(row, dict) and isinstance(row.get("resting"), dict):
                try:
                    oids.append(int(row["resting"].get("oid")))
                except Exception:
                    pass
            elif isinstance(row, dict) and isinstance(row.get("filled"), dict):
                try:
                    oids.append(int(row["filled"].get("oid")))
                except Exception:
                    pass
        return oids

    def place_limit_order(self, *, coin: str, is_buy: bool, size: float, limit_px: float, reduce_only: bool = False, tif: str = "Gtc") -> Any:
        ex = self._require_exchange()
        return ex.order(
            str(coin),
            bool(is_buy),
            self.round_size(coin, size),
            float(limit_px),
            order_type={"limit": {"tif": str(tif)}},
            reduce_only=bool(reduce_only),
        )

    def place_reduce_only_tpsl(
        self,
        *,
        coin: str,
        is_buy: bool,
        size: float,
        stop_px: float,
        target_px: float,
    ) -> Any:
        ex = self._require_exchange()
        rounded_size = self.round_size(coin, size)
        orders = [
            {
                "coin": str(coin),
                "is_buy": bool(is_buy),
                "sz": rounded_size,
                "limit_px": float(stop_px),
                "order_type": {"trigger": {"triggerPx": float(stop_px), "isMarket": True, "tpsl": "sl"}},
                "reduce_only": True,
            },
            {
                "coin": str(coin),
                "is_buy": bool(is_buy),
                "sz": rounded_size,
                "limit_px": float(target_px),
                "order_type": {"trigger": {"triggerPx": float(target_px), "isMarket": True, "tpsl": "tp"}},
                "reduce_only": True,
            },
        ]
        return ex.bulk_orders(orders, grouping="normalTpsl")

    def query_order(self, oid: str | int) -> Any:
        return self.info.query_order_by_oid(self.account_address, int(oid))

    def user_state(self) -> Dict[str, Any]:
        return dict(self.info.user_state(self.account_address) or {})

    def open_orders(self) -> list:
        rows = self.info.open_orders(self.account_address)
        return rows if isinstance(rows, list) else []

    def cancel_order(self, coin: str, oid: str | int) -> Any:
        return self._require_exchange().cancel(str(coin), int(oid))

    def market_close(self, coin: str, size: float | None = None, slippage: float | None = None) -> Any:
        return self._require_exchange().market_close(
            str(coin),
            sz=None if size is None else self.round_size(coin, float(size)),
            slippage=float(slippage if slippage is not None else _cfg("HYPERLIQUID_PROTECTION_FAILURE_CLOSE_SLIPPAGE_PCT", 0.05)),
        )

    def update_leverage(self, coin: str, leverage: int, is_cross: bool = True) -> Any:
        return self._require_exchange().update_leverage(int(leverage), str(coin), is_cross=bool(is_cross))


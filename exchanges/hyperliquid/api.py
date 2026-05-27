"""Hyperliquid SDK adapter.

The repo uses the official ``hyperliquid-python-sdk`` for signing.  Direct
signature construction is intentionally avoided because wrong EIP-712 payloads
can look like intermittent exchange failures.
"""

from __future__ import annotations

import logging
import math
import json
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

    @staticmethod
    def dex_for_coin(coin: str | None) -> str:
        """Return HIP-3 perp DEX prefix for ``xyz:SILVER`` style coins."""
        raw = str(coin or "").strip()
        return raw.split(":", 1)[0] if ":" in raw else ""

    @staticmethod
    def _num(value: Any, default: float = 0.0) -> float:
        try:
            out = float(value)
            return out if math.isfinite(out) else float(default)
        except Exception:
            return float(default)

    def account_abstraction_state(self) -> dict[str, Any]:
        """Read official account abstraction state used to choose balance authority."""
        out: dict[str, Any] = {}
        for key, method_name, payload_type in (
            ("user_abstraction", "query_user_abstraction_state", "userAbstraction"),
            ("user_dex_abstraction", "query_user_dex_abstraction_state", "userDexAbstraction"),
        ):
            try:
                fn = getattr(self.info, method_name, None)
                out[key] = fn(self.account_address) if callable(fn) else self.info.post(
                    "/info", {"type": payload_type, "user": self.account_address}
                )
            except Exception as exc:
                out[key] = {"query_error": str(exc)}
        return out

    @staticmethod
    def _abstraction_mode(state: dict[str, Any]) -> str:
        # The user-account mode is authoritative. Do not infer DEX abstraction
        # merely because a secondary diagnostic response contains the word dex.
        primary = json.dumps(state.get("user_abstraction", {}), sort_keys=True, default=str).lower()
        combined = json.dumps(state, sort_keys=True, default=str).lower()
        text = primary if primary and "query_error" not in primary else combined
        if "portfolio" in text:
            return "portfolio_margin"
        if "unified" in text:
            return "unified_account"
        if "standard" in text or "classic" in text:
            return "standard"
        if "dex" in text and not any(x in text for x in ("false", "disabled", "null")):
            return "dex_abstraction"
        return "unresolved"

    def spot_user_state(self) -> dict[str, Any]:
        try:
            fn = getattr(self.info, "spot_user_state", None)
            raw = fn(self.account_address) if callable(fn) else self.info.post(
                "/info", {"type": "spotClearinghouseState", "user": self.account_address}
            )
            return dict(raw or {})
        except Exception as exc:
            logger.warning("Hyperliquid spot clearinghouse state failed: %s", exc)
            return {}

    def user_state(self, coin: str | None = None, *, dex: str | None = None) -> Dict[str, Any]:
        """Read official perp clearinghouseState for the relevant DEX.

        HIP-3 products such as ``xyz:SILVER`` must query ``dex='xyz'`` rather
        than silently reading the native/default perp DEX account.
        """
        resolved_dex = self.dex_for_coin(coin) if dex is None else str(dex or "")
        return dict(self.info.user_state(self.account_address, resolved_dex) or {})

    def get_balance(self, coin: str | None = None) -> Dict[str, Any]:
        """Return routable collateral from the official authoritative account state.

        Unified account / portfolio-margin balances live in spotClearinghouseState;
        otherwise perp/HIP-3 collateral is read from clearinghouseState for the
        coin's DEX.  A positive spot balance is never silently applied to a HIP-3
        trade unless the official abstraction state says it is shared.
        """
        dex = self.dex_for_coin(coin)
        abstraction = self.account_abstraction_state()
        mode = self._abstraction_mode(abstraction)
        if dex and mode == "unresolved":
            out = {
                "available": 0.0, "locked": 0.0, "total": 0.0, "currency": "USDC",
                "source": f"hyperliquid_balance_authority_unresolved:dex={dex}",
                "dex": dex, "account_mode": mode, "abstraction_state": abstraction,
                "balance_verified": False,
                "reason": "hip3_balance_requires_confirmed_standard_or_unified_portfolio_account_mode",
            }
            logger.error("Hyperliquid HIP-3 balance is unverifiable for dex=%s: account abstraction mode unresolved; venue is fail-closed", dex)
            return out
        if mode in {"unified_account", "portfolio_margin"}:
            spot = self.spot_user_state()
            usdc = next(
                (row for row in list(spot.get("balances") or []) if str((row or {}).get("coin") or "").upper() == "USDC"),
                {},
            )
            total = self._num(usdc.get("total"), 0.0)
            hold = self._num(usdc.get("hold"), 0.0)
            available = max(0.0, total - hold)
            out = {
                "available": available, "locked": max(0.0, hold), "total": total, "currency": "USDC",
                "source": f"hyperliquid_spot_clearinghouse_state:{mode}",
                "dex": dex or "main", "account_mode": mode, "abstraction_state": abstraction,
                "balance_verified": True,
            }
            logger.info("Hyperliquid balance source=%s dex=%s available=$%.4f locked=$%.4f total=$%.4f", out["source"], out["dex"], available, hold, total)
            return out

        state = self.user_state(coin=coin)
        summary = state.get("marginSummary") or state.get("crossMarginSummary") or {}
        total = self._num(summary.get("accountValue"), 0.0) if isinstance(summary, dict) else 0.0
        available = self._num(state.get("withdrawable"), 0.0)
        locked = max(0.0, total - available)
        out = {
            "available": max(0.0, available), "locked": locked, "total": max(0.0, total), "currency": "USDC",
            "source": f"hyperliquid_clearinghouse_state:dex={dex or 'main'}",
            "dex": dex or "main", "account_mode": mode, "abstraction_state": abstraction,
            "balance_verified": mode in {"standard", "dex_abstraction"} or not dex,
        }
        if dex and available <= 0:
            spot = self.spot_user_state()
            usdc = next((row for row in list(spot.get("balances") or []) if str((row or {}).get("coin") or "").upper() == "USDC"), {})
            spot_available = max(0.0, self._num(usdc.get("total"), 0.0) - self._num(usdc.get("hold"), 0.0))
            if spot_available > 0:
                out["diagnostic"] = "spot_usdc_present_but_not_applied_without_confirmed_unified_or_portfolio_mode"
                out["spot_available_usdc_diagnostic"] = spot_available
        logger.info("Hyperliquid balance source=%s dex=%s mode=%s available=$%.4f locked=$%.4f total=$%.4f diagnostic=%s", out["source"], out["dex"], mode, out["available"], out["locked"], out["total"], out.get("diagnostic", "none"))
        return out

    def open_orders(self, coin: str | None = None, *, dex: str | None = None) -> list[dict[str, Any]]:
        resolved_dex = self.dex_for_coin(coin) if dex is None else str(dex or "")
        return list(self.info.open_orders(self.account_address, resolved_dex) or [])

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

    def current_asset_context(self, coin: str) -> Dict[str, Any]:
        """Return live mark/funding/OI context from official metaAndAssetCtxs."""
        target = str(coin or "").upper()
        for dex in self.perp_dexs:
            try:
                raw = self.info.post("/info", {"type": "metaAndAssetCtxs", "dex": dex})
                if not isinstance(raw, (list, tuple)) or len(raw) < 2:
                    continue
                meta, ctxs = raw[0], raw[1]
                universe = meta.get("universe", []) if isinstance(meta, dict) else []
                for i, item in enumerate(universe):
                    name = str((item or {}).get("name") or "").upper() if isinstance(item, dict) else ""
                    if name == target or (dex and f"{dex.upper()}:{name}" == target):
                        return dict(ctxs[i]) if isinstance(ctxs, list) and i < len(ctxs) and isinstance(ctxs[i], dict) else {}
            except Exception as exc:
                logger.debug("Hyperliquid asset context fetch failed dex=%r coin=%s: %s", dex, coin, exc)
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


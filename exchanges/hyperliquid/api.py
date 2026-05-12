"""Hyperliquid API wrapper used by discovery, data, and execution."""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

import config

logger = logging.getLogger(__name__)


def _float(value: Any, default: float = 0.0) -> float:
    try:
        f = float(value)
        return f if f == f else default
    except Exception:
        return default


class HyperliquidAPI:
    def __init__(
        self,
        *,
        account_address: str | None = None,
        secret_key: str | None = None,
        vault_address: str | None = None,
        testnet: bool | None = None,
        base_url: str | None = None,
        timeout: float = 10.0,
    ) -> None:
        self.timeout = float(timeout or 10.0)
        try:
            from hyperliquid.info import Info
            from hyperliquid.utils import constants
            self.sdk_available = True
        except Exception:
            Info = None  # type: ignore
            constants = None  # type: ignore
            self.sdk_available = False

        self.account_address = (account_address or getattr(config, "HYPERLIQUID_ACCOUNT_ADDRESS", "") or "").strip()
        self.secret_key = (secret_key or getattr(config, "HYPERLIQUID_SECRET_KEY", "") or "").strip()
        self.vault_address = (vault_address or getattr(config, "HYPERLIQUID_VAULT_ADDRESS", "") or "").strip() or None
        self.testnet = bool(getattr(config, "HYPERLIQUID_TESTNET", False) if testnet is None else testnet)
        default_base_url = (
            constants.TESTNET_API_URL if self.testnet else constants.MAINNET_API_URL
        ) if constants is not None else (
            "https://api.hyperliquid-testnet.xyz" if self.testnet else "https://api.hyperliquid.xyz"
        )
        self.base_url = (
            base_url
            or getattr(config, "HYPERLIQUID_BASE_URL", "")
            or default_base_url
        )
        self.info = Info(self.base_url, skip_ws=True, timeout=self.timeout) if Info is not None else None
        self.exchange = None
        self.signed_error = ""
        self._last_call = 0.0
        self._meta_cache: Optional[dict] = None
        self._ctx_cache: Optional[list] = None

        if self.secret_key:
            try:
                from eth_account import Account
                from hyperliquid.exchange import Exchange
                wallet = Account.from_key(self.secret_key)
                self.exchange = Exchange(
                    wallet,
                    base_url=self.base_url,
                    account_address=self.account_address or None,
                    vault_address=self.vault_address,
                    timeout=timeout,
                )
            except Exception as exc:
                self.signed_error = f"Hyperliquid signed exchange init failed: {exc}"
                logger.warning(self.signed_error)

        logger.info(
            "HyperliquidAPI initialised base=%s sdk=%s signed=%s account=%s",
            self.base_url,
            bool(self.sdk_available),
            bool(self.exchange),
            self.account_address[:8] + "..." if self.account_address else "unset",
        )

    @property
    def can_trade(self) -> bool:
        return bool(self.exchange is not None and self.account_address and self.secret_key)

    def _throttle(self) -> None:
        gap = max(0.05, float(getattr(config, "HYPERLIQUID_MIN_CALL_GAP_SEC", 0.20) or 0.20))
        now = time.time()
        wait = gap - (now - self._last_call)
        if wait > 0:
            time.sleep(wait)
        self._last_call = time.time()

    def _require_signed(self):
        if self.exchange is None:
            detail = self.signed_error or "set HYPERLIQUID_SECRET_KEY and HYPERLIQUID_ACCOUNT_ADDRESS and install hyperliquid-python-sdk"
            raise RuntimeError(f"Hyperliquid signed trading disabled: {detail}")
        return self.exchange

    def _post_info(self, payload: Dict[str, Any]) -> Any:
        try:
            import requests
        except Exception as exc:
            raise RuntimeError("requests is required for Hyperliquid public /info fallback") from exc
        self._throttle()
        resp = requests.post(f"{self.base_url.rstrip('/')}/info", json=payload, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def meta_and_asset_ctxs(self) -> tuple[dict, list]:
        if self._meta_cache is not None and self._ctx_cache is not None:
            return self._meta_cache, self._ctx_cache
        raw = self.info.meta_and_asset_ctxs() if self.info is not None else self._post_info({"type": "metaAndAssetCtxs"})
        if not isinstance(raw, list) or len(raw) < 2:
            raise RuntimeError(f"Unexpected Hyperliquid meta response: {type(raw).__name__}")
        self._meta_cache = raw[0] if isinstance(raw[0], dict) else {}
        self._ctx_cache = raw[1] if isinstance(raw[1], list) else []
        return self._meta_cache, self._ctx_cache

    def get_products(self) -> Dict[str, Any]:
        meta, ctxs = self.meta_and_asset_ctxs()
        universe = list(meta.get("universe") or [])
        rows = []
        for idx, coin in enumerate(universe):
            if not isinstance(coin, dict):
                continue
            ctx = ctxs[idx] if idx < len(ctxs) and isinstance(ctxs[idx], dict) else {}
            name = str(coin.get("name") or "").upper()
            if not name:
                continue
            mid = _float(ctx.get("midPx") or ctx.get("markPx") or ctx.get("oraclePx"))
            rows.append({
                "symbol": name,
                "name": name,
                "base": name,
                "quote": "USDC",
                "status": "active",
                "szDecimals": coin.get("szDecimals"),
                "maxLeverage": coin.get("maxLeverage"),
                "markPx": ctx.get("markPx"),
                "midPx": ctx.get("midPx"),
                "oraclePx": ctx.get("oraclePx"),
                "openInterest": ctx.get("openInterest"),
                "dayNtlVlm": ctx.get("dayNtlVlm"),
                "dayBaseVlm": ctx.get("dayBaseVlm"),
                "funding": ctx.get("funding"),
                "last_price": mid,
                "_raw_meta": coin,
                "_raw_ctx": ctx,
            })
        return {"success": True, "result": rows}

    def get_tickers(self, *_, **__) -> Dict[str, Any]:
        return self.get_products()

    def get_l2_book(self, symbol: str) -> Dict[str, Any]:
        if self.info is not None:
            self._throttle()
            return self.info.l2_snapshot(str(symbol).upper())
        out = self._post_info({"type": "l2Book", "coin": str(symbol).upper()})
        return out if isinstance(out, dict) else {}

    def get_candles(self, symbol: str, interval: str, start_ms: int, end_ms: int) -> List[dict]:
        if self.info is not None:
            self._throttle()
            rows = self.info.candles_snapshot(str(symbol).upper(), interval, int(start_ms), int(end_ms))
        else:
            rows = self._post_info({
                "type": "candleSnapshot",
                "req": {
                    "coin": str(symbol).upper(),
                    "interval": str(interval),
                    "startTime": int(start_ms),
                    "endTime": int(end_ms),
                },
            })
        return rows if isinstance(rows, list) else []

    def all_mids(self) -> Dict[str, str]:
        if self.info is not None:
            self._throttle()
            out = self.info.all_mids()
        else:
            out = self._post_info({"type": "allMids"})
        return out if isinstance(out, dict) else {}

    def get_balance(self, currency: str = "USDC") -> Dict[str, Any]:
        if not self.account_address:
            return {"available": 0.0, "error": "HYPERLIQUID_ACCOUNT_ADDRESS missing"}
        if self.info is not None:
            self._throttle()
            state = self.info.user_state(self.account_address)
        else:
            state = self._post_info({"type": "clearinghouseState", "user": self.account_address})
        margin = state.get("marginSummary", {}) if isinstance(state, dict) else {}
        withdrawable = _float((state or {}).get("withdrawable")) if isinstance(state, dict) else 0.0
        account_value = _float(margin.get("accountValue"))
        total_margin = _float(margin.get("totalMarginUsed"))
        available = withdrawable if withdrawable > 0 else max(0.0, account_value - total_margin)
        return {
            "available": available,
            "available_raw": available,
            "total": account_value,
            "currency": currency,
            "raw": state,
        }

    def get_positions(self, symbol: str | None = None) -> List[dict]:
        if not self.account_address:
            return []
        if self.info is not None:
            self._throttle()
            state = self.info.user_state(self.account_address)
        else:
            state = self._post_info({"type": "clearinghouseState", "user": self.account_address})
        rows = state.get("assetPositions", []) if isinstance(state, dict) else []
        out = []
        wanted = str(symbol or "").upper()
        for row in rows:
            pos = row.get("position", row) if isinstance(row, dict) else {}
            if not isinstance(pos, dict):
                continue
            coin = str(pos.get("coin") or "").upper()
            if wanted and coin != wanted:
                continue
            out.append(pos)
        return out

    def get_open_orders(self, symbol: str | None = None) -> List[dict]:
        if not self.account_address:
            return []
        if self.info is not None:
            self._throttle()
            rows = self.info.open_orders(self.account_address)
        else:
            rows = self._post_info({"type": "openOrders", "user": self.account_address})
        wanted = str(symbol or "").upper()
        out = []
        for row in rows if isinstance(rows, list) else []:
            if wanted and str(row.get("coin") or "").upper() != wanted:
                continue
            out.append(row)
        return out

    def query_order(self, oid: str | int) -> Dict[str, Any]:
        if not self.account_address:
            return {}
        try:
            if self.info is not None:
                self._throttle()
                return self.info.query_order_by_oid(self.account_address, int(oid)) or {}
            out = self._post_info({"type": "orderStatus", "user": self.account_address, "oid": int(oid)})
            return out if isinstance(out, dict) else {}
        except Exception:
            return {}

    def place_order(
        self,
        *,
        symbol: str,
        side: str,
        order_type: str,
        quantity: float,
        price: float | None = None,
        trigger_price: float | None = None,
        reduce_only: bool = False,
        stop_order_type: str | None = None,
    ) -> Dict[str, Any]:
        ex = self._require_signed()
        coin = str(symbol).upper()
        is_buy = str(side).upper() in {"BUY", "LONG"}
        qty = float(quantity)
        ot = str(order_type or "").lower()
        self._throttle()
        if stop_order_type:
            tpsl = "tp" if "take" in str(stop_order_type).lower() or "profit" in str(stop_order_type).lower() else "sl"
            order_type_wire = {"trigger": {"triggerPx": float(trigger_price or price or 0.0), "isMarket": True, "tpsl": tpsl}}
            return ex.order(coin, is_buy, qty, float(price or trigger_price or 0.0), order_type_wire, reduce_only=reduce_only)
        if ot == "market":
            slip = float(getattr(config, "HYPERLIQUID_SLIPPAGE", 0.015) or 0.015)
            if reduce_only:
                return ex.market_close(coin, sz=qty, px=price, slippage=slip)
            return ex.market_open(coin, is_buy, qty, px=price, slippage=slip)
        return ex.order(coin, is_buy, qty, float(price or 0.0), {"limit": {"tif": "Gtc"}}, reduce_only=reduce_only)

    def cancel_order(self, symbol: str, order_id: str | int) -> Dict[str, Any]:
        ex = self._require_signed()
        self._throttle()
        return ex.cancel(str(symbol).upper(), int(order_id))

    def set_leverage(self, symbol: str, leverage: int, is_cross: bool = True) -> Dict[str, Any]:
        ex = self._require_signed()
        self._throttle()
        return ex.update_leverage(int(leverage), str(symbol).upper(), is_cross=bool(is_cross))

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from .config import BotConfig
from .groww_adapter import GrowwAdapter
from .storage import Store

log = logging.getLogger(__name__)


def now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def flatten_option_chain(payload: dict[str, Any], expiry: str) -> pd.DataFrame:
    ts = now_utc()
    underlying_ltp = payload.get("underlying_ltp")
    strikes = payload.get("strikes") or {}
    rows: list[dict[str, Any]] = []
    for strike, sides in strikes.items():
        for opt_type in ("CE", "PE"):
            node = (sides or {}).get(opt_type)
            if not node:
                continue
            greeks = node.get("greeks") or {}
            rows.append(
                {
                    "ts": ts,
                    "trade_date": ts.date(),
                    "expiry": pd.to_datetime(expiry).date(),
                    "strike": float(strike),
                    "option_type": opt_type,
                    "trading_symbol": node.get("trading_symbol"),
                    "underlying_ltp": float(underlying_ltp) if underlying_ltp is not None else None,
                    "ltp": node.get("ltp"),
                    "open_interest": node.get("open_interest"),
                    "volume": node.get("volume"),
                    "delta": greeks.get("delta"),
                    "gamma": greeks.get("gamma"),
                    "theta": greeks.get("theta"),
                    "vega": greeks.get("vega"),
                    "rho": greeks.get("rho"),
                    "iv": greeks.get("iv"),
                    "raw_json": json.dumps(node, default=str),
                    "source": "groww_option_chain",
                }
            )
    df = pd.DataFrame(rows)
    if not df.empty:
        num_cols = ["strike", "underlying_ltp", "ltp", "open_interest", "volume", "delta", "gamma", "theta", "vega", "rho", "iv"]
        for c in num_cols:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def flatten_quote(trading_symbol: str, payload: dict[str, Any]) -> pd.DataFrame:
    ts = now_utc()
    bid = payload.get("bid_price")
    ask = payload.get("offer_price")
    try:
        spread = float(ask) - float(bid) if ask is not None and bid is not None else None
        mid = (float(ask) + float(bid)) / 2 if ask is not None and bid is not None else None
        spread_pct = spread / mid if mid and mid > 0 else None
    except Exception:
        spread = None
        spread_pct = None
    row = {
        "ts": ts,
        "trade_date": ts.date(),
        "trading_symbol": trading_symbol,
        "last_price": payload.get("last_price"),
        "bid_price": payload.get("bid_price"),
        "bid_quantity": payload.get("bid_quantity"),
        "offer_price": payload.get("offer_price"),
        "offer_quantity": payload.get("offer_quantity"),
        "spread": spread,
        "spread_pct": spread_pct,
        "open_interest": payload.get("open_interest"),
        "volume": payload.get("volume"),
        "implied_volatility": payload.get("implied_volatility"),
        "total_buy_quantity": payload.get("total_buy_quantity"),
        "total_sell_quantity": payload.get("total_sell_quantity"),
        "last_trade_quantity": payload.get("last_trade_quantity"),
        "last_trade_time": payload.get("last_trade_time"),
        "depth_json": json.dumps(payload.get("depth"), default=str),
        "raw_json": json.dumps(payload, default=str),
        "source": "groww_quote",
    }
    df = pd.DataFrame([row])
    for c in df.columns:
        if c not in {"ts", "trade_date", "trading_symbol", "depth_json", "raw_json"}:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


class OptionDataCollector:
    def __init__(self, cfg: BotConfig, store: Store, adapter: GrowwAdapter):
        self.cfg = cfg
        self.store = store
        self.adapter = adapter

    def collect_option_chain_once(self, expiry: str) -> int:
        payload = self.adapter.get_option_chain(self.cfg.underlying, expiry)
        df = flatten_option_chain(payload, expiry)
        n = self.store.append_df("option_chain_snapshots", df)
        log.info("📥 option_chain saved | expiry=%s rows=%s", expiry, n)
        return n

    def collect_quotes_for_symbols(self, symbols: list[str]) -> int:
        total = 0
        for sym in symbols[: self.cfg.max_quote_symbols]:
            try:
                q = self.adapter.get_quote(sym)
                df = flatten_quote(sym, q)
                total += self.store.append_df("quote_snapshots", df)
            except Exception:
                log.exception("quote failed | symbol=%s", sym)
        log.info("📥 quotes saved | rows=%s", total)
        return total

    def collect_loop(self, expiry: str, quote_top_symbols: bool = True) -> None:
        log.info("🚀 collector loop started | underlying=%s expiry=%s interval=%ss", self.cfg.underlying, expiry, self.cfg.option_chain_interval_seconds)
        while True:
            try:
                self.collect_option_chain_once(expiry)
                if quote_top_symbols:
                    latest = self.store.query_df(
                        """
                        SELECT trading_symbol
                        FROM option_chain_snapshots
                        WHERE ts = (SELECT max(ts) FROM option_chain_snapshots)
                          AND trading_symbol IS NOT NULL
                          AND ltp BETWEEN ? AND ?
                          AND volume >= ?
                          AND open_interest >= ?
                        ORDER BY volume DESC, open_interest DESC
                        LIMIT ?
                        """,
                        (self.cfg.min_ltp, self.cfg.max_ltp, self.cfg.min_volume, self.cfg.min_oi, self.cfg.max_quote_symbols),
                    )
                    self.collect_quotes_for_symbols(latest["trading_symbol"].dropna().astype(str).tolist())
            except KeyboardInterrupt:
                raise
            except Exception:
                log.exception("collector loop error")
            time.sleep(self.cfg.option_chain_interval_seconds)

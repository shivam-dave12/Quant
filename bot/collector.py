from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from .config import AssetProfile, BotConfig
from .groww_adapter import GrowwAdapter
from .groww_instruments import ensure_groww_instruments_csv, load_option_contracts_from_groww_instruments
from .risk import normalize_quote
from .session import is_session_open
from .storage import Store

log = logging.getLogger(__name__)
INSTRUMENT_QUOTE_CHAIN_SOURCE = "groww_instrument_quote_chain"


def now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _is_rate_limit_error(exc: Exception) -> bool:
    return "rate limit" in str(exc).lower()


def flatten_option_chain(payload: dict[str, Any], expiry: str, asset: AssetProfile | None = None) -> pd.DataFrame:
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
                    "asset_id": asset.asset_id if asset else "nifty",
                    "underlying": asset.underlying if asset else "NIFTY",
                    "exchange": asset.exchange if asset else "NSE",
                    "segment": asset.segment if asset else "FNO",
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
        for col in num_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def flatten_quote(trading_symbol: str, payload: dict[str, Any], asset: AssetProfile | None = None, ts: datetime | None = None) -> pd.DataFrame:
    ts = ts or now_utc()
    raw_payload = dict(payload or {})
    normalized = normalize_quote(raw_payload)
    row = {
        "ts": ts,
        "trade_date": ts.date(),
        "asset_id": asset.asset_id if asset else "nifty",
        "underlying": asset.underlying if asset else "NIFTY",
        "exchange": asset.exchange if asset else "NSE",
        "segment": asset.segment if asset else "FNO",
        "trading_symbol": trading_symbol,
        "last_price": normalized.get("last_price"),
        "bid_price": normalized.get("bid_price"),
        "bid_quantity": normalized.get("bid_quantity"),
        "offer_price": normalized.get("offer_price"),
        "offer_quantity": normalized.get("offer_quantity"),
        "spread": normalized.get("spread"),
        "spread_pct": normalized.get("spread_pct"),
        "open_interest": normalized.get("open_interest"),
        "volume": normalized.get("volume"),
        "implied_volatility": normalized.get("implied_volatility"),
        "total_buy_quantity": normalized.get("total_buy_quantity"),
        "total_sell_quantity": normalized.get("total_sell_quantity"),
        "last_trade_quantity": normalized.get("last_trade_quantity"),
        "last_trade_time": normalized.get("last_trade_time"),
        "depth_json": json.dumps(normalized.get("depth"), default=str),
        "raw_json": json.dumps(raw_payload, default=str),
        "source": "groww_quote",
    }
    df = pd.DataFrame([row])
    for col in df.columns:
        if col not in {"ts", "trade_date", "asset_id", "underlying", "exchange", "segment", "trading_symbol", "depth_json", "raw_json", "source"}:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def flatten_instrument_quote_contract(
    contract: pd.Series | dict[str, Any],
    payload: dict[str, Any],
    expiry: str,
    asset: AssetProfile,
    ts: datetime | None = None,
) -> pd.DataFrame:
    ts = ts or now_utc()
    contract_row = contract.to_dict() if isinstance(contract, pd.Series) else dict(contract)
    raw_payload = dict(payload or {})
    normalized = normalize_quote(raw_payload)
    bid = normalized.get("bid_price")
    ask = normalized.get("offer_price")
    ltp = normalized.get("last_price") or normalized.get("ltp")
    if ltp is None and bid is not None and ask is not None:
        ltp = (float(bid) + float(ask)) / 2.0
    underlying_ltp = (
        normalized.get("underlying_ltp")
        or normalized.get("underlyingLtp")
        or normalized.get("underlying_price")
        or normalized.get("underlyingPrice")
        or normalized.get("spot_price")
        or normalized.get("spotPrice")
    )
    row = {
        "ts": ts,
        "trade_date": ts.date(),
        "asset_id": asset.asset_id,
        "underlying": asset.underlying,
        "exchange": asset.exchange,
        "segment": asset.segment,
        "expiry": pd.to_datetime(expiry).date(),
        "strike": contract_row.get("strike") or contract_row.get("strike_price"),
        "option_type": str(contract_row.get("option_type") or contract_row.get("instrument_type") or "").upper(),
        "trading_symbol": contract_row.get("trading_symbol"),
        "underlying_ltp": underlying_ltp,
        "ltp": ltp,
        "open_interest": normalized.get("open_interest"),
        "volume": normalized.get("volume"),
        "delta": normalized.get("delta"),
        "gamma": normalized.get("gamma"),
        "theta": normalized.get("theta"),
        "vega": normalized.get("vega"),
        "rho": normalized.get("rho"),
        "iv": normalized.get("implied_volatility") or normalized.get("iv"),
        "raw_json": json.dumps({"contract": contract_row, "quote": raw_payload}, default=str),
        "source": INSTRUMENT_QUOTE_CHAIN_SOURCE,
    }
    df = pd.DataFrame([row])
    num_cols = ["strike", "underlying_ltp", "ltp", "open_interest", "volume", "delta", "gamma", "theta", "vega", "rho", "iv"]
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


class OptionDataCollector:
    def __init__(self, cfg: BotConfig, store: Store, adapter: GrowwAdapter, asset: AssetProfile | None = None):
        self.cfg = cfg
        self.store = store
        self.adapter = adapter
        self.asset = asset or cfg.get_asset_profile("nifty")
        self.last_status: dict[str, Any] = {}
        self._instrument_contracts_cache: pd.DataFrame | None = None

    @staticmethod
    def _evenly_spaced_strikes(strikes: list[float], max_count: int) -> set[float]:
        if max_count <= 0 or len(strikes) <= max_count:
            return set(strikes)
        if max_count == 1:
            return {strikes[len(strikes) // 2]}
        last = len(strikes) - 1
        indexes = {round(i * last / (max_count - 1)) for i in range(max_count)}
        return {strikes[i] for i in sorted(indexes)}

    def _historical_active_strikes(self, expiry: str, limit: int) -> list[float]:
        if limit <= 0:
            return []
        try:
            rows = self.store.query_df(
                """
                SELECT c.strike,
                       count(*) AS executable_rows,
                       max(c.ts) AS last_ts,
                       avg(c.ltp) AS avg_ltp
                FROM option_chain_snapshots c
                JOIN quote_snapshots q
                  ON c.trading_symbol = q.trading_symbol
                 AND c.asset_id = q.asset_id
                WHERE c.asset_id = ?
                  AND c.expiry = ?
                  AND c.ltp BETWEEN ? AND ?
                  AND q.bid_price > 0
                  AND q.offer_price > 0
                  AND q.spread_pct <= ?
                GROUP BY c.strike
                ORDER BY last_ts DESC, executable_rows DESC
                LIMIT ?
                """,
                (
                    self.asset.asset_id,
                    str(pd.to_datetime(expiry).date()),
                    float(self.asset.min_ltp),
                    float(self.asset.max_ltp),
                    float(self.asset.max_spread_pct),
                    int(limit),
                ),
            )
        except Exception:
            return []
        if rows.empty or "strike" not in rows.columns:
            return []
        return pd.to_numeric(rows["strike"], errors="coerce").dropna().astype(float).tolist()

    @staticmethod
    def _neighbor_strikes(strikes: list[float], anchors: list[float], max_count: int) -> set[float]:
        if not strikes or not anchors or max_count <= 0:
            return set()
        selected: set[float] = set()
        for anchor in anchors:
            nearest = min(range(len(strikes)), key=lambda idx: abs(strikes[idx] - anchor))
            for radius in range(0, len(strikes)):
                for idx in (nearest - radius, nearest + radius):
                    if 0 <= idx < len(strikes):
                        selected.add(strikes[idx])
                        if len(selected) >= max_count:
                            return selected
        return selected

    def _select_contracts_for_quote_scan(self, contracts: pd.DataFrame, expiry: str | None = None) -> pd.DataFrame:
        if contracts.empty or len(contracts) <= self.asset.max_quote_symbols:
            return contracts
        max_strikes = max(1, self.asset.max_quote_symbols // 2)
        strikes = sorted(pd.to_numeric(contracts["strike"], errors="coerce").dropna().unique().tolist())
        active = self._historical_active_strikes(expiry, max_strikes) if expiry else []
        keep_strikes = self._neighbor_strikes(strikes, active, max_strikes)
        if len(keep_strikes) < max_strikes:
            keep_strikes |= self._evenly_spaced_strikes(strikes, max_strikes - len(keep_strikes))
        selected = contracts[contracts["strike"].isin(keep_strikes)].copy()
        return selected.sort_values(["strike", "option_type", "trading_symbol"]).head(self.asset.max_quote_symbols)

    def _instrument_contracts_for_expiry(self, expiry: str) -> pd.DataFrame:
        if self._instrument_contracts_cache is None:
            try:
                path, refreshed, refresh_reason = ensure_groww_instruments_csv(
                    self.cfg.groww_instruments_csv,
                    self.cfg.groww_instruments_url,
                    self.cfg.groww_instruments_max_age_hours,
                )
                if refreshed:
                    log.info("instrument master refreshed | asset=%s path=%s reason=%s", self.asset.asset_id, path, refresh_reason)
                self._instrument_contracts_cache = load_option_contracts_from_groww_instruments(self.cfg.groww_instruments_csv, self.asset)
            except Exception as exc:
                reason = f"instrument_master_unavailable:{exc}"
                self.last_status = {"collected": False, "blocking": True, "reason": reason}
                self._instrument_contracts_cache = pd.DataFrame()
                log.warning("instrument quote collection skipped | asset=%s reason=%s", self.asset.asset_id, reason)
                return pd.DataFrame()
        contracts = self._instrument_contracts_cache
        expiry_date = pd.to_datetime(expiry).date()
        contracts = contracts[pd.to_datetime(contracts["expiry"]).dt.date.eq(expiry_date)].copy()
        if "buy_allowed" in contracts.columns:
            buy_allowed = pd.to_numeric(contracts["buy_allowed"], errors="coerce").fillna(0).gt(0)
            if self.asset.allow_short_option_entries and "sell_allowed" in contracts.columns:
                sell_allowed = pd.to_numeric(contracts["sell_allowed"], errors="coerce").fillna(0).gt(0)
                contracts = contracts[buy_allowed | sell_allowed].copy()
            else:
                contracts = contracts[buy_allowed].copy()
        if contracts.empty:
            reason = f"no_buyable_instrument_contracts:{self.asset.exchange}:{self.asset.underlying}:{expiry_date}"
            self.last_status = {"collected": False, "blocking": True, "reason": reason}
            log.warning("instrument quote collection skipped | asset=%s reason=%s", self.asset.asset_id, reason)
        return contracts

    def collect_option_chain_once(self, expiry: str) -> int:
        if self.asset.option_chain_mode.lower() == "instrument_quotes":
            return self.collect_instrument_quote_chain_once(expiry)
        payload = self.adapter.get_option_chain(self.asset.underlying, expiry, exchange=self.asset.exchange)
        df = flatten_option_chain(payload, expiry, self.asset)
        rows = self.store.append_df("option_chain_snapshots", df)
        self.last_status = {
            "collected": rows > 0,
            "blocking": False,
            "reason": "ok" if rows > 0 else "empty_option_chain_snapshot",
            "mode": self.asset.option_chain_mode,
            "rows": rows,
        }
        log.info("option_chain saved | asset=%s expiry=%s rows=%s", self.asset.asset_id, expiry, rows)
        return rows

    def collect_instrument_quote_chain_once(self, expiry: str) -> int:
        contracts = self._instrument_contracts_for_expiry(expiry)
        if contracts.empty:
            return 0
        selected = self._select_contracts_for_quote_scan(contracts, expiry)
        scan_ts = now_utc()
        quote_frames: list[pd.DataFrame] = []
        chain_frames: list[pd.DataFrame] = []
        failures = 0
        for _, contract in selected.iterrows():
            sym = str(contract.get("trading_symbol"))
            try:
                quote = self._get_quote_with_backoff(sym)
                quote_frames.append(flatten_quote(sym, quote, self.asset, ts=scan_ts))
                chain_frames.append(flatten_instrument_quote_contract(contract, quote, expiry, self.asset, ts=scan_ts))
            except Exception as exc:
                failures += 1
                if _is_rate_limit_error(exc):
                    log.warning("instrument quote rate-limited | asset=%s symbol=%s error=%s", self.asset.asset_id, sym, exc)
                else:
                    log.exception("instrument quote failed | asset=%s symbol=%s", self.asset.asset_id, sym)
        chain_rows = self.store.append_df("option_chain_snapshots", pd.concat(chain_frames, ignore_index=True) if chain_frames else pd.DataFrame())
        quote_rows = self.store.append_df("quote_snapshots", pd.concat(quote_frames, ignore_index=True) if quote_frames else pd.DataFrame())
        reason = "ok" if chain_rows > 0 else "no_instrument_quotes_collected"
        self.last_status = {
            "collected": chain_rows > 0,
            "blocking": chain_rows == 0,
            "reason": reason,
            "mode": self.asset.option_chain_mode,
            "expiry": str(pd.to_datetime(expiry).date()),
            "available_contracts": int(len(contracts)),
            "quoted_contracts": int(len(selected)),
            "chain_rows": int(chain_rows),
            "quote_rows": int(quote_rows),
            "quote_failures": int(failures),
        }
        log.info(
            "instrument quote chain saved | asset=%s expiry=%s chain_rows=%s quote_rows=%s failures=%s",
            self.asset.asset_id,
            expiry,
            chain_rows,
            quote_rows,
            failures,
        )
        return chain_rows + quote_rows

    def collect_quotes_for_symbols(self, symbols: list[str]) -> int:
        total = 0
        for sym in symbols[: self.asset.max_quote_symbols]:
            try:
                quote = self._get_quote_with_backoff(sym)
                total += self.store.append_df("quote_snapshots", flatten_quote(sym, quote, self.asset))
            except Exception as exc:
                if _is_rate_limit_error(exc):
                    log.warning("quote rate-limited | asset=%s symbol=%s error=%s", self.asset.asset_id, sym, exc)
                else:
                    log.exception("quote failed | asset=%s symbol=%s", self.asset.asset_id, sym)
        log.info("quotes saved | asset=%s rows=%s", self.asset.asset_id, total)
        return total

    def _get_quote_with_backoff(self, symbol: str) -> dict[str, Any]:
        attempts = max(1, int(self.cfg.groww_quote_retry_attempts))
        last_exc: Exception | None = None
        for attempt in range(attempts):
            if attempt > 0:
                time.sleep(max(0.0, float(self.cfg.groww_quote_rate_limit_backoff_seconds)) * attempt)
            try:
                quote = self.adapter.get_quote(symbol, segment=self.asset.segment, exchange=self.asset.exchange)
                delay = max(0.0, float(self.cfg.groww_quote_delay_seconds))
                if delay:
                    time.sleep(delay)
                return quote
            except Exception as exc:
                last_exc = exc
                if not _is_rate_limit_error(exc) or attempt == attempts - 1:
                    raise
                log.warning(
                    "quote retry after rate limit | asset=%s symbol=%s attempt=%s/%s backoff=%.2fs",
                    self.asset.asset_id,
                    symbol,
                    attempt + 1,
                    attempts,
                    float(self.cfg.groww_quote_rate_limit_backoff_seconds) * (attempt + 1),
                )
        raise last_exc or RuntimeError(f"quote failed: {symbol}")

    def _latest_symbols_for_expiry(self, expiry: str) -> list[str]:
        expiry_date = pd.to_datetime(expiry).date()
        latest = self.store.query_df(
            """
            SELECT trading_symbol
            FROM option_chain_snapshots
            WHERE asset_id = ?
              AND expiry = ?
              AND ts = (SELECT max(ts) FROM option_chain_snapshots WHERE asset_id = ? AND expiry = ?)
              AND trading_symbol IS NOT NULL
              AND ltp BETWEEN ? AND ?
              AND coalesce(volume, 0) >= ?
              AND coalesce(open_interest, 0) >= ?
            ORDER BY volume DESC, open_interest DESC
            LIMIT ?
            """,
            (
                self.asset.asset_id,
                expiry_date,
                self.asset.asset_id,
                expiry_date,
                self.asset.min_ltp,
                self.asset.max_ltp,
                self.asset.min_volume,
                self.asset.min_oi,
                self.asset.max_quote_symbols,
            ),
        )
        symbols = latest["trading_symbol"].dropna().astype(str).tolist()
        if symbols:
            return symbols

        # Some Groww option-chain snapshots have usable LTP/strike data but blank
        # volume/OI. Still quote the near-ATM book so live scoring can use the
        # executable quote payload instead of getting stuck on stale snapshots.
        fallback = self.store.query_df(
            """
            SELECT trading_symbol
            FROM option_chain_snapshots
            WHERE asset_id = ?
              AND expiry = ?
              AND ts = (SELECT max(ts) FROM option_chain_snapshots WHERE asset_id = ? AND expiry = ?)
              AND trading_symbol IS NOT NULL
              AND ltp BETWEEN ? AND ?
            ORDER BY abs(coalesce(strike, 0) - coalesce(underlying_ltp, strike, 0)) ASC,
                     ltp DESC
            LIMIT ?
            """,
            (
                self.asset.asset_id,
                expiry_date,
                self.asset.asset_id,
                expiry_date,
                self.asset.min_ltp,
                self.asset.max_ltp,
                self.asset.max_quote_symbols,
            ),
        )
        symbols = fallback["trading_symbol"].dropna().astype(str).tolist()
        if symbols:
            log.info(
                "quote symbol fallback used | asset=%s expiry=%s symbols=%s reason=missing_chain_liquidity_fields",
                self.asset.asset_id,
                expiry,
                len(symbols),
            )
        return symbols

    def collect_expiries_once(self, expiries: list[str], quote_top_symbols: bool = True) -> dict[str, int]:
        session = is_session_open(self.asset)
        if not session.is_open:
            log.info(
                "collector skipped | asset=%s reason=%s local_time=%s",
                self.asset.asset_id,
                session.reason,
                session.local_time,
            )
            return {expiry: 0 for expiry in expiries}
        rows_by_expiry: dict[str, int] = {}
        for expiry in expiries:
            chain_rows = self.collect_option_chain_once(expiry)
            quote_rows = 0
            if quote_top_symbols and self.asset.option_chain_mode.lower() != "instrument_quotes":
                quote_rows = self.collect_quotes_for_symbols(self._latest_symbols_for_expiry(expiry))
            rows_by_expiry[expiry] = chain_rows + quote_rows
        self.last_status = {**self.last_status, "rows_by_expiry": rows_by_expiry}
        return rows_by_expiry

    def collect_loop(self, expiries: list[str], quote_top_symbols: bool = True) -> None:
        log.info(
            "collector loop started | asset=%s underlying=%s expiries=%s interval=%ss",
            self.asset.asset_id,
            self.asset.underlying,
            ",".join(expiries),
            self.cfg.option_chain_interval_seconds,
        )
        while True:
            try:
                self.collect_expiries_once(expiries, quote_top_symbols=quote_top_symbols)
            except KeyboardInterrupt:
                raise
            except Exception:
                log.exception("collector loop error | asset=%s", self.asset.asset_id)
            time.sleep(self.cfg.option_chain_interval_seconds)

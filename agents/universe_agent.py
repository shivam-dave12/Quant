"""Universe discovery and market diagnostics agent."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Iterable, List

try:
    import config
except Exception:  # pragma: no cover
    config = None  # type: ignore

from fund.mandate import FundMandate
from fund.types import MarketDiagnostics, clamp, safe_float


def _cfg(name: str, default: Any) -> Any:
    if config is None:
        return default
    return getattr(config, name, default)


class UniverseAgent:
    """Builds normalized diagnostics for each asset desk.

    This agent does not produce alpha. It asks whether a desk is operationally
    worth evaluating: live data, warm candles, executable book, tolerable spread,
    and a confirmed instrument.
    """

    def __init__(self, mandate: FundMandate | None = None) -> None:
        self.mandate = mandate or FundMandate.from_config()

    def diagnose_many(self, contexts: Iterable[Any]) -> List[MarketDiagnostics]:
        return [self.diagnose_context(ctx) for ctx in contexts]

    def diagnose_context(self, ctx: Any) -> MarketDiagnostics:
        inst = getattr(ctx, "instrument", None)
        data = getattr(ctx, "data_manager", None)
        strategy = getattr(ctx, "strategy", None)
        now = time.time()

        asset_id = str(getattr(inst, "asset_id", "UNKNOWN") or "UNKNOWN")
        display_symbol = str(getattr(inst, "display_symbol", asset_id) or asset_id)
        primary_exchange = str(getattr(getattr(inst, "primary_exchange", ""), "value", getattr(inst, "primary_exchange", "")) or "")
        asset_class = str(getattr(getattr(inst, "asset_class", ""), "value", getattr(inst, "asset_class", "")) or "")

        price = self._safe_call(data, "get_last_price", 0.0)
        book = self._safe_call(data, "get_orderbook", {}) or {}
        bids = list(book.get("bids", []) or book.get("buy", []) or [])
        asks = list(book.get("asks", []) or book.get("sell", []) or [])
        bid = safe_float(bids[0][0], 0.0) if bids else 0.0
        ask = safe_float(asks[0][0], 0.0) if asks else 0.0
        mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else price
        spread = max(0.0, ask - bid) if ask > 0 and bid > 0 else 0.0
        spread_bps = (spread / mid * 10_000.0) if mid > 0 else 0.0
        depth = self._book_depth_usd(bids, asks, mid)
        imbalance = self._book_imbalance(bids, asks)

        candles_1m = self._safe_call(data, "get_candles", [], "1m", 300) or []
        candles_5m = self._safe_call(data, "get_candles", [], "5m", 120) or []
        min_1m = int(_cfg("MIN_CANDLES_1M", 80))
        min_5m = int(_cfg("MIN_CANDLES_5M", 60))
        warmup_1m = clamp(len(candles_1m) / max(1, min_1m))
        warmup_5m = clamp(len(candles_5m) / max(1, min_5m))

        atr = safe_float(getattr(getattr(strategy, "_atr_5m", None), "atr", 0.0), 0.0)
        if atr <= 0:
            atr = self._approx_atr(candles_5m)
        atr_pctile = safe_float(
            self._safe_call(getattr(strategy, "_atr_5m", None), "get_percentile", 0.5),
            0.5,
        )
        spread_atr = spread / atr if atr > 0 else 0.0

        data_age = self._data_age_sec(data, now)
        phase = str(getattr(ctx, "phase_name", "UNKNOWN") or "UNKNOWN")
        has_position = bool(getattr(ctx, "has_position", False))
        ready = bool(getattr(ctx, "ready", False))
        tradable = bool(inst is not None and data is not None)

        notes: list[str] = []
        if not ready:
            notes.append("desk_not_ready")
        if price <= 0:
            notes.append("no_valid_price")
        if not bids or not asks:
            notes.append("empty_orderbook")
        if data_age > self.mandate.max_data_age_sec:
            notes.append(f"stale_data_{data_age:.0f}s")
        if min(warmup_1m, warmup_5m) < self.mandate.min_warmup_ratio:
            notes.append("candle_warmup_incomplete")
        if spread_bps > self.mandate.max_spread_for_class(asset_class):
            notes.append(f"wide_spread_{spread_bps:.1f}bps")

        return MarketDiagnostics(
            asset_id=asset_id,
            display_symbol=display_symbol,
            primary_exchange=primary_exchange,
            asset_class=asset_class,
            price=price,
            atr_5m=atr,
            atr_pctile=clamp(atr_pctile),
            spread_bps=spread_bps,
            spread_atr=spread_atr,
            book_depth_usd=depth,
            book_imbalance=imbalance,
            data_age_sec=data_age,
            warmup_1m=warmup_1m,
            warmup_5m=warmup_5m,
            ready=ready,
            has_position=has_position,
            phase=phase,
            tradable=tradable,
            notes=tuple(notes),
        )

    @staticmethod
    def _safe_call(obj: Any, name: str, default: Any, *args) -> Any:
        try:
            if obj is None:
                return default
            fn = getattr(obj, name, None)
            if not callable(fn):
                return default
            return fn(*args)
        except Exception:
            return default

    @staticmethod
    def _book_depth_usd(bids: list, asks: list, mid: float, levels: int = 5) -> float:
        total = 0.0
        for row in list(bids[:levels]) + list(asks[:levels]):
            try:
                px = safe_float(row[0], mid)
                qty = safe_float(row[1], 0.0)
                total += max(0.0, px * qty)
            except Exception:
                continue
        return total

    @staticmethod
    def _book_imbalance(bids: list, asks: list, levels: int = 5) -> float:
        bid_qty = sum(max(0.0, safe_float(r[1], 0.0)) for r in bids[:levels])
        ask_qty = sum(max(0.0, safe_float(r[1], 0.0)) for r in asks[:levels])
        total = bid_qty + ask_qty
        return (bid_qty - ask_qty) / total if total > 0 else 0.0

    @staticmethod
    def _data_age_sec(data: Any, now: float) -> float:
        last = UniverseAgent._safe_call(data, "get_last_update", None)
        if last is None:
            ts = safe_float(getattr(data, "last_update", 0.0), 0.0)
            return max(0.0, now - ts) if ts > 0 else 0.0
        if isinstance(last, datetime):
            dt = last if last.tzinfo else last.replace(tzinfo=timezone.utc)
            return max(0.0, now - dt.timestamp())
        ts = safe_float(last, 0.0)
        return max(0.0, now - ts) if ts > 0 else 0.0

    @staticmethod
    def _approx_atr(candles: list, period: int = 14) -> float:
        rows = candles[-max(2, period + 1):]
        trs: list[float] = []
        prev_close = 0.0
        for c in rows:
            high = safe_float(c.get("high") if isinstance(c, dict) else getattr(c, "high", 0.0), 0.0)
            low = safe_float(c.get("low") if isinstance(c, dict) else getattr(c, "low", 0.0), 0.0)
            close = safe_float(c.get("close") if isinstance(c, dict) else getattr(c, "close", 0.0), 0.0)
            if high <= 0 or low <= 0:
                continue
            if prev_close > 0:
                trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
            else:
                trs.append(high - low)
            prev_close = close
        return sum(trs[-period:]) / max(1, len(trs[-period:]))

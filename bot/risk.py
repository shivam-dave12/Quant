from __future__ import annotations

import math
from typing import Any


def _as_positive_float(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(out) or out <= 0:
        return None
    return out


def _first_positive(payload: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = _as_positive_float(payload.get(key))
        if value is not None:
            return value
    return None


def _book_levels(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, dict):
        values = list(value.values())
    elif isinstance(value, list):
        values = value
    else:
        values = []
    return [item for item in values if isinstance(item, dict)]


def _level_price(level: dict[str, Any], side: str) -> float | None:
    keys = ("price", "bidPrice", "buyPrice") if side == "bid" else ("price", "askPrice", "offerPrice", "sellPrice")
    for key in keys:
        px = _as_positive_float(level.get(key))
        if px is not None:
            return px
    return None


def _level_quantity(level: dict[str, Any], side: str) -> float | None:
    keys = ("quantity", "qty", "volume", "bidQty", "buyQty") if side == "bid" else ("quantity", "qty", "volume", "askQty", "offerQty", "sellQty")
    for key in keys:
        qty = _as_positive_float(level.get(key))
        if qty is not None:
            return qty
    return None


def _best_level(depth: Any, side: str) -> tuple[float | None, float | None]:
    if not isinstance(depth, dict):
        return None, None
    if side == "bid":
        book = depth.get("buy") or depth.get("buyBook") or depth.get("bids") or depth.get("bid") or depth.get("BUY")
    else:
        book = depth.get("sell") or depth.get("sellBook") or depth.get("asks") or depth.get("ask") or depth.get("SELL")
    for level in _book_levels(book):
        price = _level_price(level, side)
        quantity = _level_quantity(level, side)
        if price is not None:
            return price, quantity
    return None, None


def normalize_quote(quote: dict[str, Any]) -> dict[str, Any]:
    """Return a quote with executable bid/ask fields populated from top book depth.

    Groww's quote payload documents both top-level bid/offer fields and a
    depth book. In practice, either source can be missing, so the execution
    path normalizes once and then uses a single contract.
    """
    out = dict(quote or {})
    depth = out.get("depth") or out.get("market_depth") or out.get("order_book")
    if not depth:
        buy_book = out.get("buyBook") or out.get("buy") or out.get("bids") or out.get("bid")
        sell_book = out.get("sellBook") or out.get("sell") or out.get("asks") or out.get("ask")
        depth = {"buy": buy_book, "sell": sell_book} if buy_book or sell_book else {}
    out["depth"] = depth
    last_price = _first_positive(out, "last_price", "lastPrice", "ltp", "last_traded_price", "lastTradedPrice")
    bid = _as_positive_float(out.get("bid_price") or out.get("bidPrice"))
    ask = _as_positive_float(out.get("offer_price") or out.get("ask_price") or out.get("askPrice") or out.get("offerPrice"))
    bid_qty = _as_positive_float(out.get("bid_quantity") or out.get("bidQty"))
    ask_qty = _as_positive_float(out.get("offer_quantity") or out.get("ask_quantity") or out.get("askQty") or out.get("offerQty"))

    depth_bid, depth_bid_qty = _best_level(depth, "bid")
    depth_ask, depth_ask_qty = _best_level(depth, "ask")
    bid = bid if bid is not None else depth_bid
    ask = ask if ask is not None else depth_ask
    bid_qty = bid_qty if bid_qty is not None else depth_bid_qty
    ask_qty = ask_qty if ask_qty is not None else depth_ask_qty

    out["last_price"] = last_price
    out["bid_price"] = bid
    out["offer_price"] = ask
    out["bid_quantity"] = bid_qty
    out["offer_quantity"] = ask_qty
    alias_map = {
        "open_interest": ("open_interest", "openInterest", "oi"),
        "volume": ("volume", "volume_traded", "volumeTraded", "day_volume", "dayVolume"),
        "implied_volatility": ("implied_volatility", "impliedVolatility", "iv"),
        "total_buy_quantity": ("total_buy_quantity", "totalBuyQuantity", "total_buy_qty"),
        "total_sell_quantity": ("total_sell_quantity", "totalSellQuantity", "total_sell_qty"),
        "last_trade_quantity": ("last_trade_quantity", "lastTradeQuantity", "last_traded_quantity", "lastTradedQuantity"),
        "last_trade_time": ("last_trade_time", "lastTradeTime", "last_traded_time", "lastTradedTime"),
    }
    for dst, keys in alias_map.items():
        value = _first_positive(out, *keys)
        if value is not None:
            out[dst] = value
    if bid is not None and ask is not None and ask >= bid:
        spread = ask - bid
        mid = (ask + bid) / 2.0
        out["spread"] = spread
        out["spread_pct"] = spread / mid if mid > 0 else None
    else:
        out["spread"] = None
        out["spread_pct"] = None
    return out


def round_to_tick(price: float, tick: float = 0.05) -> float:
    if tick <= 0:
        tick = 0.05
    return round(round(price / tick) * tick, 2)


def option_buy_quantity(
    premium: float,
    lot_size: int,
    account_capital: float,
    risk_pct: float,
    max_premium_value: float,
    sl_pct: float | None = None,
) -> int:
    if premium <= 0 or lot_size <= 0:
        return 0
    risk_rupees = account_capital * risk_pct
    premium_per_lot = premium * lot_size
    premium_lots = math.floor(max_premium_value / premium_per_lot) if max_premium_value > 0 else 0
    if sl_pct is not None and sl_pct > 0:
        loss_per_lot = premium_per_lot * sl_pct
        risk_lots = math.floor(risk_rupees / loss_per_lot) if loss_per_lot > 0 else 0
        lots = min(premium_lots, risk_lots)
    else:
        lots = premium_lots
    return max(0, lots * lot_size)


def limit_price_from_quote(quote: dict, tick_size: float = 0.05, max_cross_ticks: int = 1) -> float | None:
    normalized = normalize_quote(quote)
    ask = _as_positive_float(normalized.get("offer_price"))
    ltp = _as_positive_float(normalized.get("last_price") or normalized.get("ltp"))
    px = ask if ask is not None else ltp
    if px is None:
        return None
    return round_to_tick(px + tick_size * max_cross_ticks, tick_size)


def limit_price_from_quote_for_side(quote: dict, side: str = "BUY", tick_size: float = 0.05, max_cross_ticks: int = 1) -> float | None:
    normalized = normalize_quote(quote)
    side = str(side or "BUY").upper()
    if side == "SELL":
        bid = _as_positive_float(normalized.get("bid_price"))
        ltp = _as_positive_float(normalized.get("last_price") or normalized.get("ltp"))
        px = bid if bid is not None else ltp
        if px is None:
            return None
        return round_to_tick(max(tick_size, px - tick_size * max_cross_ticks), tick_size)
    return limit_price_from_quote(normalized, tick_size=tick_size, max_cross_ticks=max_cross_ticks)


def quote_is_executable(quote: dict, max_spread_pct: float = 0.025, min_offer_qty: float = 1) -> tuple[bool, str]:
    return quote_is_executable_for_side(quote, side="BUY", max_spread_pct=max_spread_pct, min_qty=min_offer_qty)


def quote_is_executable_for_side(quote: dict, side: str = "BUY", max_spread_pct: float = 0.025, min_qty: float = 1) -> tuple[bool, str]:
    normalized = normalize_quote(quote)
    side = str(side or "BUY").upper()
    bid = _as_positive_float(normalized.get("bid_price"))
    ask = _as_positive_float(normalized.get("offer_price"))
    offer_qty = _as_positive_float(normalized.get("offer_quantity")) or 0.0
    bid_qty = _as_positive_float(normalized.get("bid_quantity")) or 0.0
    if bid is None or ask is None:
        return False, "missing_bid_or_offer"
    if ask < bid:
        return False, "invalid_bid_offer"
    mid = (bid + ask) / 2
    spread_pct = (ask - bid) / mid if mid > 0 else 999
    if spread_pct > max_spread_pct:
        return False, f"spread_too_wide:{spread_pct:.4f}"
    if side == "SELL" and bid_qty < min_qty:
        return False, "insufficient_bid_qty"
    if side != "SELL" and offer_qty < min_qty:
        return False, "insufficient_offer_qty"
    return True, "ok"

from __future__ import annotations

import math


def round_to_tick(price: float, tick: float = 0.05) -> float:
    if tick <= 0:
        tick = 0.05
    return round(round(price / tick) * tick, 2)


def option_buy_quantity(premium: float, lot_size: int, account_capital: float, risk_pct: float, max_premium_value: float) -> int:
    if premium <= 0 or lot_size <= 0:
        return 0
    risk_rupees = account_capital * risk_pct
    budget = min(risk_rupees, max_premium_value)
    lots = math.floor(budget / (premium * lot_size))
    return max(0, lots * lot_size)


def limit_price_from_quote(quote: dict, tick_size: float = 0.05, max_cross_ticks: int = 1) -> float | None:
    ask = quote.get("offer_price") or quote.get("ask_price")
    ltp = quote.get("last_price")
    px = ask if ask and float(ask) > 0 else ltp
    if not px:
        return None
    return round_to_tick(float(px) + tick_size * max_cross_ticks, tick_size)


def quote_is_executable(quote: dict, max_spread_pct: float = 0.025, min_offer_qty: float = 1) -> tuple[bool, str]:
    bid = quote.get("bid_price")
    ask = quote.get("offer_price")
    offer_qty = quote.get("offer_quantity") or 0
    if not bid or not ask:
        return False, "missing_bid_or_offer"
    bid = float(bid); ask = float(ask)
    if bid <= 0 or ask <= 0 or ask < bid:
        return False, "invalid_bid_offer"
    mid = (bid + ask) / 2
    spread_pct = (ask - bid) / mid if mid > 0 else 999
    if spread_pct > max_spread_pct:
        return False, f"spread_too_wide:{spread_pct:.4f}"
    if float(offer_qty) < min_offer_qty:
        return False, "insufficient_offer_qty"
    return True, "ok"

"""Authoritative PnL formulas shared by strategy and risk accounting."""

from __future__ import annotations


def linear_pnl_usd(side: str, entry_price: float, exit_price: float, quantity_btc: float) -> float:
    side_u = str(side).upper()
    if side_u == "LONG":
        return (float(exit_price) - float(entry_price)) * float(quantity_btc)
    if side_u == "SHORT":
        return (float(entry_price) - float(exit_price)) * float(quantity_btc)
    raise ValueError(f"invalid side: {side!r}")


def inverse_pnl_usd(side: str, entry_price: float, exit_price: float, quantity_btc: float) -> float:
    """
    USD PnL for BTCUSD inverse exposure when order quantity is tracked in BTC.

    The BTC-settled inverse formula converts back to USD at exit. With quantity
    represented as BTC exposure at entry, the USD result is:
      LONG  = qty * entry * (1/entry - 1/exit) * exit
      SHORT = qty * entry * (1/exit - 1/entry) * exit
    which simplifies to the same USD delta as linear BTC exposure, but keeping
    this function explicit prevents the common missing-*exit_price bug.
    """
    entry = float(entry_price)
    exit_ = float(exit_price)
    qty = float(quantity_btc)
    if entry <= 0 or exit_ <= 0:
        raise ValueError("entry_price and exit_price must be positive")
    side_u = str(side).upper()
    if side_u == "LONG":
        return qty * entry * ((1.0 / entry) - (1.0 / exit_)) * exit_
    if side_u == "SHORT":
        return qty * entry * ((1.0 / exit_) - (1.0 / entry)) * exit_
    raise ValueError(f"invalid side: {side!r}")


def gross_pnl_usd(side: str, entry_price: float, exit_price: float,
                  quantity_btc: float, inverse: bool = False) -> float:
    if inverse:
        return inverse_pnl_usd(side, entry_price, exit_price, quantity_btc)
    return linear_pnl_usd(side, entry_price, exit_price, quantity_btc)

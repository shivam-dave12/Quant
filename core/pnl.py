"""Authoritative PnL formulas shared by strategy and risk accounting.

Quantity is expressed in the strategy/execution unit for the active contract:
BTC exposure for BTC inverse products, contract/share units for xStocks/RWA/
commodities, or whatever the exchange adapter normalises to.  Do not hardcode
BTC in accounting labels; the adapter owns contract translation.
"""

from __future__ import annotations

from typing import Optional


def _qty(quantity_units: Optional[float] = None, quantity_btc: Optional[float] = None) -> float:
    # ``quantity_btc`` is accepted for old call sites/tests, but the accounting
    # engine is unit-generic.  New code should pass quantity_units.
    if quantity_units is None:
        quantity_units = quantity_btc
    if quantity_units is None:
        raise ValueError("quantity_units is required")
    return float(quantity_units)


def linear_pnl_usd(side: str, entry_price: float, exit_price: float,
                   quantity_units: Optional[float] = None,
                   quantity_btc: Optional[float] = None) -> float:
    qty = _qty(quantity_units, quantity_btc)
    side_u = str(side).upper()
    if side_u == "LONG":
        return (float(exit_price) - float(entry_price)) * qty
    if side_u == "SHORT":
        return (float(entry_price) - float(exit_price)) * qty
    raise ValueError(f"invalid side: {side!r}")


def inverse_pnl_usd(side: str, entry_price: float, exit_price: float,
                    quantity_units: Optional[float] = None,
                    quantity_btc: Optional[float] = None) -> float:
    """
    USD PnL for BTCUSD inverse exposure when strategy quantity is BTC exposure.

    For Delta BTCUSD, the adapter converts raw contracts into BTC exposure before
    this function is called.  For non-BTC Delta products, callers should pass
    inverse=False; the linear function uses the same quantity unit as execution.
    """
    entry = float(entry_price)
    exit_ = float(exit_price)
    qty = _qty(quantity_units, quantity_btc)
    if entry <= 0 or exit_ <= 0:
        raise ValueError("entry_price and exit_price must be positive")
    side_u = str(side).upper()
    if side_u == "LONG":
        return qty * entry * ((1.0 / entry) - (1.0 / exit_)) * exit_
    if side_u == "SHORT":
        return qty * entry * ((1.0 / exit_) - (1.0 / entry)) * exit_
    raise ValueError(f"invalid side: {side!r}")


def gross_pnl_usd(side: str, entry_price: float, exit_price: float,
                  quantity_units: Optional[float] = None,
                  inverse: bool = False,
                  quantity_btc: Optional[float] = None) -> float:
    qty = _qty(quantity_units, quantity_btc)
    if inverse:
        return inverse_pnl_usd(side, entry_price, exit_price, quantity_units=qty)
    return linear_pnl_usd(side, entry_price, exit_price, quantity_units=qty)

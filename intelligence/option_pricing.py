"""Black-76 index-option valuation, implied volatility and analytic Greeks."""
from __future__ import annotations
from dataclasses import dataclass
from math import exp, log, sqrt, pi
from scipy.optimize import brentq
from scipy.stats import norm

@dataclass(frozen=True)
class Black76Greeks:
    price: float
    delta: float
    gamma: float
    theta: float
    vega: float
    iv: float

def black76_price(forward: float, strike: float, rate: float, time_years: float, sigma: float, option_type: str) -> float:
    if min(forward, strike, time_years, sigma) <= 0: raise ValueError("positive Black-76 inputs required")
    discount = exp(-rate * time_years); root_t = sqrt(time_years)
    d1 = (log(forward / strike) + 0.5 * sigma * sigma * time_years) / (sigma * root_t); d2 = d1 - sigma * root_t
    if option_type.upper() == "CE": return discount * (forward * norm.cdf(d1) - strike * norm.cdf(d2))
    if option_type.upper() == "PE": return discount * (strike * norm.cdf(-d2) - forward * norm.cdf(-d1))
    raise ValueError("option type must be CE or PE")

def implied_volatility(price: float, forward: float, strike: float, rate: float, time_years: float, option_type: str) -> float:
    if price <= 0: raise ValueError("premium must be positive")
    intrinsic = exp(-rate * time_years) * max(0.0, forward - strike if option_type.upper() == "CE" else strike - forward)
    if price < intrinsic - 1e-8: raise ValueError("premium below discounted intrinsic")
    func = lambda sigma: black76_price(forward, strike, rate, time_years, sigma, option_type) - price
    try:
        return float(brentq(func, 1e-5, 5.0, maxiter=200))
    except ValueError as exc:
        raise ValueError("no Black-76 implied volatility solution") from exc

def black76_greeks(forward: float, strike: float, rate: float, time_years: float, sigma: float, option_type: str) -> Black76Greeks:
    price = black76_price(forward, strike, rate, time_years, sigma, option_type); discount = exp(-rate * time_years); root_t = sqrt(time_years)
    d1 = (log(forward / strike) + 0.5 * sigma * sigma * time_years) / (sigma * root_t); density = exp(-0.5 * d1 * d1) / sqrt(2 * pi)
    call_delta = discount * norm.cdf(d1); delta = call_delta if option_type.upper() == "CE" else -discount * norm.cdf(-d1)
    gamma = discount * density / (forward * sigma * root_t)
    vega = discount * forward * density * root_t
    theta_core = -discount * forward * density * sigma / (2 * root_t)
    # Calendar-time theta: analytical diffusion decay plus discount contribution.
    theta = theta_core + rate * price
    return Black76Greeks(price, delta, gamma, theta, vega, sigma)

"""Indian Index Conditional Barrier Model (IICBM).

ICICI index options are traded as long-premium instruments.  The model does not
try to predict an index direction in isolation.  It evaluates whether the
selected option premium is likely to hit an upper premium barrier before a lower
premium barrier inside an allowed holding window, after spread, slippage, theta
and confidence penalties.

The implementation is deliberately dependency-free so it can run inside the live
bot and in offline unit tests without sklearn/lightgbm.  The live model can later
replace the deterministic hazard estimator with a calibrated ML model behind the
same interface.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
import math
from typing import Any, Mapping, Sequence, Optional


def _f(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        if isinstance(value, str):
            s = value.strip().replace(",", "")
            if not s or s.lower() in {"none", "null", "nan", "na"}:
                return default
            return float(s)
        return float(value)
    except Exception:
        return default


def _clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, float(x)))


def _sigmoid(x: float) -> float:
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


def _right(row: Mapping[str, Any]) -> str:
    v = str(row.get("right") or row.get("option_type") or row.get("OptionType") or row.get("Right") or row.get("CallPut") or "").strip().lower()
    if v in {"c", "ce", "call"}:
        return "call"
    if v in {"p", "pe", "put"}:
        return "put"
    return v


@dataclass(frozen=True)
class IICBMConfig:
    min_probability: float = 0.82
    expiry_day_min_probability: float = 0.86
    min_ev_after_cost_r: float = 0.08
    min_liquidity_score: float = 0.65
    target_delta_min: float = 0.38
    target_delta_max: float = 0.58
    target_delta: float = 0.45
    max_spread_to_premium: float = 0.035
    max_theta_to_premium_per_hold: float = 0.12
    min_entry_premium: float = 5.0
    min_dte: float = 1.0
    max_dte: float = 21.0
    default_hold_minutes: int = 24
    max_hold_minutes: int = 45
    safety_r: float = 0.04
    conformal_penalty: float = 0.035
    disorder_probability_cap: float = 0.42
    no_fresh_entry_after_minutes_to_close: int = 25

    @classmethod
    def from_config_module(cls, config_module: Any) -> "IICBMConfig":
        g = lambda name, default: getattr(config_module, name, default)
        return cls(
            min_probability=float(g("INDIAN_IICBM_MIN_PROBABILITY", cls.min_probability)),
            expiry_day_min_probability=float(g("INDIAN_IICBM_EXPIRY_DAY_MIN_PROBABILITY", cls.expiry_day_min_probability)),
            min_ev_after_cost_r=float(g("INDIAN_IICBM_MIN_EV_AFTER_COST_R", cls.min_ev_after_cost_r)),
            min_liquidity_score=float(g("INDIAN_IICBM_MIN_LIQUIDITY_SCORE", cls.min_liquidity_score)),
            target_delta_min=float(g("INDIAN_TARGET_DELTA_MIN", cls.target_delta_min)),
            target_delta_max=float(g("INDIAN_TARGET_DELTA_MAX", cls.target_delta_max)),
            target_delta=float(g("ICICI_INDEX_OPTION_TARGET_ABS_DELTA", cls.target_delta)),
            max_spread_to_premium=float(g("INDIAN_MAX_SPREAD_TO_PREMIUM", cls.max_spread_to_premium)),
            max_theta_to_premium_per_hold=float(g("INDIAN_MAX_THETA_TO_PREMIUM_PER_HOLD", cls.max_theta_to_premium_per_hold)),
            min_entry_premium=float(g("INDIAN_MIN_OPTION_PREMIUM", cls.min_entry_premium)),
            min_dte=float(g("INDIAN_MIN_DTE", getattr(config_module, "ICICI_OPTION_MIN_DTE", cls.min_dte))),
            max_dte=float(g("INDIAN_MAX_DTE", getattr(config_module, "ICICI_OPTION_MAX_DTE", cls.max_dte))),
            default_hold_minutes=int(g("INDIAN_IICBM_DEFAULT_HOLD_MIN", cls.default_hold_minutes)),
            max_hold_minutes=int(g("INDIAN_IICBM_MAX_HOLD_MIN", cls.max_hold_minutes)),
            safety_r=float(g("INDIAN_IICBM_SAFETY_R", cls.safety_r)),
            conformal_penalty=float(g("INDIAN_IICBM_CONFORMAL_PENALTY", cls.conformal_penalty)),
            disorder_probability_cap=float(g("INDIAN_IICBM_DISORDER_PROB_CAP", cls.disorder_probability_cap)),
            no_fresh_entry_after_minutes_to_close=int(g("INDIAN_NO_FRESH_ENTRY_AFTER_CLOSE_BUFFER_MIN", cls.no_fresh_entry_after_minutes_to_close)),
        )


@dataclass(frozen=True)
class AuctionFeatures:
    efficiency: float
    persistence: float
    impulse: float
    realized_vol: float
    disorder_probability: float
    confidence: float
    note: str = ""

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ChainFeatures:
    pressure: float
    convexity: float
    pinning_risk: float
    liquidity_score: float
    confidence: float
    note: str = ""

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SelectedOption:
    symbol: str
    option_type: str
    strike: float
    expiry: str
    dte: float
    delta: float
    gamma: float
    theta_to_premium_per_hold: float
    bid: float
    ask: float
    entry_premium: float
    spread_to_premium: float
    liquidity_score: float
    raw: Mapping[str, Any]

    def as_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["raw"] = dict(self.raw)
        return d


@dataclass(frozen=True)
class BarrierDecision:
    decision: str
    side: str
    reason: str
    p_hit: float
    p_hit_lower_bound: float
    p_required: float
    ev_after_cost_r: float
    target_r: float
    stop_r: float
    hold_minutes: int
    entry_premium: float
    sl_premium: float
    tp_premium: float
    selected_option: Optional[SelectedOption]
    auction: AuctionFeatures
    chain: ChainFeatures
    diagnostics: Mapping[str, Any]

    def as_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["selected_option"] = self.selected_option.as_dict() if self.selected_option else None
        d["auction"] = self.auction.as_dict()
        d["chain"] = self.chain.as_dict()
        d["diagnostics"] = dict(self.diagnostics)
        return d


def required_probability(target_r: float, *, cost_r: float = 0.0, theta_r: float = 0.0, slippage_r: float = 0.0, safety_r: float = 0.04) -> float:
    """Break-even hurdle for target-before-stop probability.

    Formula: (loss_R + costs + theta + slippage + safety) / (loss_R + target_R)
    with loss_R normalised to 1.0.
    """
    target = max(float(target_r), 1e-9)
    return _clamp((1.0 + max(0.0, cost_r) + max(0.0, theta_r) + max(0.0, slippage_r) + max(0.0, safety_r)) / (1.0 + target), 0.0, 0.99)


def candle_impulse_activity(candle: Mapping[str, Any], *, min_activity: float = 1.0) -> float:
    """Activity proxy for index candles where Breeze volume may be zero/missing.

    We never fabricate volume.  If volume is present, use it.  If volume is zero,
    use OHLC movement activity so flow/confirmation calculations do not collapse
    to false 0.00 for index underlyings.
    """
    vol = _f(candle.get("volume", candle.get("v", 0.0)), 0.0)
    if vol > 0:
        return vol
    o = _f(candle.get("open", candle.get("o", 0.0)), 0.0)
    h = _f(candle.get("high", candle.get("h", 0.0)), 0.0)
    l = _f(candle.get("low", candle.get("l", 0.0)), 0.0)
    c = _f(candle.get("close", candle.get("c", 0.0)), 0.0)
    if h <= 0 or l <= 0 or c <= 0:
        return 0.0
    body = abs(c - (o or c))
    wick_range = max(h - l, 0.0)
    # Movement-only activity in index points; bounded below when there is a real candle.
    return max(min_activity, body + 0.35 * wick_range)


def build_auction_features(candles: Sequence[Mapping[str, Any]]) -> AuctionFeatures:
    rows = [c for c in candles if isinstance(c, Mapping)]
    if len(rows) < 8:
        return AuctionFeatures(0.0, 0.0, 0.0, 0.0, 1.0, 0.0, "NA_DATA:not_enough_candles")
    closes: list[float] = []
    signed: list[float] = []
    effs: list[float] = []
    acts: list[float] = []
    prev_c = 0.0
    for c in rows[-48:]:
        o = _f(c.get("open", c.get("o", 0.0)), 0.0)
        h = _f(c.get("high", c.get("h", 0.0)), 0.0)
        l = _f(c.get("low", c.get("l", 0.0)), 0.0)
        cl = _f(c.get("close", c.get("c", 0.0)), 0.0)
        if h <= 0 or l <= 0 or cl <= 0 or h < l:
            continue
        rng = max(h - l, 1e-9)
        body = cl - (o or cl)
        effs.append(abs(body) / rng)
        signed.append(1.0 if (prev_c and cl > prev_c) or body > 0 else -1.0 if (prev_c and cl < prev_c) or body < 0 else 0.0)
        closes.append(cl)
        acts.append(candle_impulse_activity(c))
        prev_c = cl
    if len(closes) < 8:
        return AuctionFeatures(0.0, 0.0, 0.0, 0.0, 1.0, 0.0, "NA_DATA:bad_candles")
    returns = [abs(closes[i] - closes[i - 1]) / max(abs(closes[i - 1]), 1e-9) for i in range(1, len(closes))]
    rv = math.sqrt(sum(r * r for r in returns[-24:]) / max(1, len(returns[-24:]))) * math.sqrt(375.0)
    efficiency = _clamp(sum(effs[-16:]) / max(1, len(effs[-16:])))
    recent = signed[-10:]
    directional_balance = abs(sum(recent)) / max(1, len(recent))
    persistence = _clamp(0.35 + 0.65 * directional_balance)
    act_med = sorted(acts)[len(acts) // 2] if acts else 0.0
    act_now = sum(acts[-6:]) / max(1, len(acts[-6:]))
    impulse = _clamp(0.5 + 0.5 * math.tanh((act_now - act_med) / max(act_med, 1.0)))
    disorder = _clamp(1.0 - (0.45 * efficiency + 0.35 * persistence + 0.20 * impulse))
    confidence = _clamp(min(1.0, len(closes) / 32.0) * (0.65 + 0.35 * (1.0 - disorder)))
    note = "VOLUME_MISSING_USING_PROXY" if all(_f(c.get("volume", c.get("v", 0.0)), 0.0) <= 0 for c in rows[-12:]) else "OK"
    return AuctionFeatures(efficiency, persistence, impulse, rv, disorder, confidence, note)


def build_chain_features(chain: Sequence[Mapping[str, Any]], spot: float) -> ChainFeatures:
    rows = [r for r in chain if isinstance(r, Mapping)]
    if spot <= 0 or not rows:
        return ChainFeatures(0.0, 0.0, 1.0, 0.0, 0.0, "NA_DATA:missing_chain_or_spot")
    call_pressure = put_pressure = total_liq = near_oi = total_oi = 0.0
    nearest_wall_dist = float("inf")
    for r in rows:
        strike = _f(r.get("strike", r.get("strike_price", r.get("StrikePrice", 0.0))), 0.0)
        if strike <= 0:
            continue
        mny_dist = abs(strike - spot) / max(spot, 1.0)
        if mny_dist > 0.08:
            continue
        oi = max(0.0, _f(r.get("oi", r.get("open_interest", r.get("openInterest", 0.0))), 0.0))
        doi = _f(r.get("oi_change", r.get("change_in_oi", r.get("ChangeinOpenInterest", 0.0))), 0.0)
        vol = max(0.0, _f(r.get("volume", r.get("Volume", 0.0)), 0.0))
        bid = _f(r.get("bid", r.get("best_bid", 0.0)), 0.0)
        ask = _f(r.get("ask", r.get("best_ask", 0.0)), 0.0)
        mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else _f(r.get("ltp", r.get("last_price", 0.0)), 0.0)
        spread_score = _clamp(1.0 - ((ask - bid) / max(mid, 1e-9) if bid > 0 and ask > 0 else 0.04) / 0.06)
        weight = (oi + 2.0 * max(doi, 0.0) + 1.5 * vol) * (1.0 - min(1.0, mny_dist / 0.08))
        if _right(r) == "call":
            call_pressure += weight
        elif _right(r) == "put":
            put_pressure += weight
        total_liq += spread_score * min(1.0, (vol + oi) / 5000.0)
        total_oi += oi
        if mny_dist <= 0.015:
            near_oi += oi
            nearest_wall_dist = min(nearest_wall_dist, mny_dist)
    denom = max(call_pressure + put_pressure, 1e-9)
    pressure = _clamp((call_pressure - put_pressure) / denom, -1.0, 1.0)
    convexity = _clamp((abs(call_pressure - put_pressure) / denom) * 0.60 + (1.0 - min(nearest_wall_dist if nearest_wall_dist != float("inf") else 0.08, 0.08) / 0.08) * 0.20 + min(1.0, total_oi / 100000.0) * 0.20)
    pinning_risk = _clamp((near_oi / max(total_oi, 1e-9)) * 1.35)
    liquidity = _clamp(total_liq / max(1.0, min(len(rows), 24)))
    conf = _clamp(min(1.0, len(rows) / 24.0) * (0.55 + 0.45 * liquidity))
    return ChainFeatures(pressure, convexity, pinning_risk, liquidity, conf, "OK")


def _spread_to_premium(row: Mapping[str, Any]) -> tuple[float, float, float, float]:
    bid = _f(row.get("bid", row.get("best_bid", row.get("bid_price", 0.0))), 0.0)
    ask = _f(row.get("ask", row.get("best_ask", row.get("ask_price", 0.0))), 0.0)
    ltp = _f(row.get("ltp", row.get("last_price", row.get("price", row.get("close", 0.0)))), 0.0)
    entry = ask if ask > 0 else ltp
    mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else ltp
    spread = max(0.0, ask - bid) if bid > 0 and ask > 0 else max(0.0, entry * 0.02)
    return bid, ask, entry, spread / max(mid or entry, 1e-9)


def select_option_contract(contracts: Sequence[Mapping[str, Any]], thesis_side: str, spot: float, cfg: IICBMConfig = IICBMConfig()) -> tuple[Optional[SelectedOption], str]:
    desired = "call" if str(thesis_side).lower() in {"long", "buy_call", "call", "bullish"} else "put" if str(thesis_side).lower() in {"short", "buy_put", "put", "bearish"} else ""
    if not desired:
        return None, "BAD_THESIS_SIDE"
    best: tuple[float, SelectedOption] | None = None
    reject_reasons: list[str] = []
    for r in contracts:
        if not isinstance(r, Mapping) or _right(r) != desired:
            continue
        strike = _f(r.get("strike", r.get("strike_price", r.get("StrikePrice", 0.0))), 0.0)
        dte = _f(r.get("dte", r.get("DTE", r.get("days_to_expiry", 0.0))), 0.0)
        delta = abs(_f(r.get("delta", r.get("Delta", 0.0)), 0.0))
        gamma = max(0.0, _f(r.get("gamma", r.get("Gamma", 0.0)), 0.0))
        theta = abs(_f(r.get("theta_to_premium_per_hold", r.get("theta_to_premium", 0.0)), 0.0))
        bid, ask, entry, spread_ratio = _spread_to_premium(r)
        if entry < cfg.min_entry_premium:
            reject_reasons.append("premium_too_low")
            continue
        if dte < cfg.min_dte or dte > cfg.max_dte:
            reject_reasons.append("dte_out_of_range")
            continue
        if delta <= 0:
            # If live Greeks are absent, approximate by moneyness for ranking only.
            if spot > 0 and strike > 0:
                mny = abs(strike - spot) / max(spot, 1.0)
                delta = _clamp(cfg.target_delta - mny * 4.0, 0.20, 0.65)
            else:
                reject_reasons.append("delta_missing")
                continue
        if not (cfg.target_delta_min <= delta <= cfg.target_delta_max):
            reject_reasons.append("delta_out_of_band")
            continue
        if spread_ratio > cfg.max_spread_to_premium:
            reject_reasons.append("spread_too_wide")
            continue
        if theta > cfg.max_theta_to_premium_per_hold:
            reject_reasons.append("theta_too_high")
            continue
        oi = max(0.0, _f(r.get("oi", r.get("open_interest", 0.0)), 0.0))
        vol = max(0.0, _f(r.get("volume", 0.0), 0.0))
        liquidity = _clamp(0.50 * (1.0 - spread_ratio / max(cfg.max_spread_to_premium, 1e-9)) + 0.30 * min(1.0, vol / 2500.0) + 0.20 * min(1.0, oi / 50000.0))
        delta_score = _clamp(1.0 - abs(delta - cfg.target_delta) / max(cfg.target_delta_max - cfg.target_delta_min, 1e-9))
        convexity_score = _clamp(0.65 * delta_score + 0.20 * liquidity + 0.15 * min(1.0, gamma * max(spot, 1.0) * 100.0))
        opt = SelectedOption(
            symbol=str(r.get("symbol") or r.get("trading_symbol") or r.get("TradingSymbol") or ""),
            option_type=desired,
            strike=strike,
            expiry=str(r.get("expiry") or r.get("expiry_date") or r.get("ExpiryDate") or ""),
            dte=dte,
            delta=delta if desired == "call" else -delta,
            gamma=gamma,
            theta_to_premium_per_hold=theta,
            bid=bid,
            ask=ask,
            entry_premium=entry,
            spread_to_premium=spread_ratio,
            liquidity_score=liquidity,
            raw=r,
        )
        score = 0.55 * convexity_score + 0.30 * liquidity + 0.15 * _clamp(1.0 - theta / max(cfg.max_theta_to_premium_per_hold, 1e-9))
        if best is None or score > best[0]:
            best = (score, opt)
    if best:
        return best[1], "OK"
    return None, "NO_CONTRACT:" + (reject_reasons[-1] if reject_reasons else "no_matching_right")


def estimate_hit_probability(*, thesis_side: str, option: SelectedOption, auction: AuctionFeatures, chain: ChainFeatures, target_r: float, hold_minutes: int, minutes_to_close: int) -> float:
    side_sign = 1.0 if str(thesis_side).lower() in {"long", "buy_call", "call", "bullish"} else -1.0
    chain_alignment = 0.5 + 0.5 * side_sign * chain.pressure
    time_safety = _clamp(minutes_to_close / max(hold_minutes + 5.0, 1.0))
    rr_penalty = max(0.0, target_r - 1.15) * 0.36
    spread_penalty = option.spread_to_premium * 5.0
    theta_penalty = option.theta_to_premium_per_hold * 2.25
    pin_penalty = chain.pinning_risk * 0.40
    disorder_penalty = auction.disorder_probability * 0.80
    liquidity_bonus = option.liquidity_score * 0.45 + chain.liquidity_score * 0.25
    raw = (
        -1.05
        + 1.30 * auction.efficiency
        + 1.15 * auction.persistence
        + 0.95 * auction.impulse
        + 1.05 * chain_alignment
        + 0.70 * chain.convexity
        + liquidity_bonus
        + 0.35 * time_safety
        - rr_penalty
        - spread_penalty
        - theta_penalty
        - pin_penalty
        - disorder_penalty
    )
    return _clamp(_sigmoid(raw), 0.01, 0.97)


def build_barrier_decision(
    *,
    index: str,
    thesis_side: str,
    candles: Sequence[Mapping[str, Any]],
    option_chain: Sequence[Mapping[str, Any]],
    candidate_contracts: Sequence[Mapping[str, Any]],
    spot: float,
    minutes_to_close: int,
    cfg: IICBMConfig = IICBMConfig(),
) -> BarrierDecision:
    auction = build_auction_features(candles)
    chain = build_chain_features(option_chain, spot)
    null_auction = auction
    null_chain = chain
    if minutes_to_close < cfg.no_fresh_entry_after_minutes_to_close:
        return BarrierDecision("REJECT", thesis_side, "SESSION_TOO_LATE", 0.0, 0.0, 0.99, -1.0, 0.0, 1.0, 0, 0.0, 0.0, 0.0, None, null_auction, null_chain, {"index": index, "minutes_to_close": minutes_to_close})
    if auction.confidence <= 0 or chain.confidence <= 0:
        return BarrierDecision("REJECT", thesis_side, "NA_DATA", 0.0, 0.0, 0.99, -1.0, 0.0, 1.0, 0, 0.0, 0.0, 0.0, None, null_auction, null_chain, {"index": index, "auction_note": auction.note, "chain_note": chain.note})
    if auction.disorder_probability > cfg.disorder_probability_cap:
        return BarrierDecision("REJECT", thesis_side, "DISORDER_TOO_HIGH", 0.0, 0.0, 0.99, -1.0, 0.0, 1.0, 0, 0.0, 0.0, 0.0, None, auction, chain, {"index": index, "disorder": auction.disorder_probability})
    option, reason = select_option_contract(candidate_contracts, thesis_side, spot, cfg)
    if not option:
        return BarrierDecision("REJECT", thesis_side, reason, 0.0, 0.0, 0.99, -1.0, 0.0, 1.0, 0, 0.0, 0.0, 0.0, None, auction, chain, {"index": index})
    if option.liquidity_score < cfg.min_liquidity_score:
        return BarrierDecision("REJECT", thesis_side, "OPTION_LIQUIDITY_TOO_LOW", 0.0, 0.0, 0.99, -1.0, 0.0, 1.0, 0, option.entry_premium, 0.0, 0.0, option, auction, chain, {"index": index, "liq": option.liquidity_score})

    cost_r = option.spread_to_premium * 0.55
    theta_r = option.theta_to_premium_per_hold
    slippage_r = max(0.01, option.spread_to_premium * 0.35)
    hold = int(min(cfg.max_hold_minutes, max(5, cfg.default_hold_minutes, minutes_to_close - cfg.no_fresh_entry_after_minutes_to_close)))
    best: Optional[tuple[float, float, float, float, float, float]] = None
    # (utility, p, p_lb, p_req, target_r, stop_frac)
    for stop_frac in (0.18, 0.22, 0.28, 0.35, 0.42):
        for target_r in (0.55, 0.75, 0.95, 1.15, 1.35, 1.60):
            p = estimate_hit_probability(thesis_side=thesis_side, option=option, auction=auction, chain=chain, target_r=target_r, hold_minutes=hold, minutes_to_close=minutes_to_close)
            uncertainty = cfg.conformal_penalty + (1.0 - min(auction.confidence, chain.confidence, option.liquidity_score)) * 0.065
            p_lb = _clamp(p - uncertainty)
            p_req = required_probability(target_r, cost_r=cost_r, theta_r=theta_r, slippage_r=slippage_r, safety_r=cfg.safety_r)
            utility = p * target_r - (1.0 - p) * 1.0 - cost_r - theta_r - slippage_r
            if best is None or utility > best[0]:
                best = (utility, p, p_lb, p_req, target_r, stop_frac)
    assert best is not None
    utility, p, p_lb, p_req, target_r, stop_frac = best
    min_prob = cfg.expiry_day_min_probability if option.dte <= 1.05 else cfg.min_probability
    threshold = max(p_req, min_prob)
    risk_premium = max(option.entry_premium * stop_frac, 0.05)
    sl = max(0.05, option.entry_premium - risk_premium)
    tp = option.entry_premium + risk_premium * target_r
    decision = "EXECUTE" if p_lb >= threshold and utility >= cfg.min_ev_after_cost_r else "REJECT"
    if decision == "EXECUTE":
        reason = "EXECUTE_IICBM"
    elif p_lb < threshold:
        reason = "P_HIT_LOWER_BOUND_TOO_LOW"
    else:
        reason = "EV_AFTER_COST_TOO_LOW"
    return BarrierDecision(
        decision=decision,
        side="BUY_CALL" if _right({"right": option.option_type}) == "call" else "BUY_PUT",
        reason=reason,
        p_hit=round(p, 6),
        p_hit_lower_bound=round(p_lb, 6),
        p_required=round(p_req, 6),
        ev_after_cost_r=round(utility, 6),
        target_r=round(target_r, 4),
        stop_r=1.0,
        hold_minutes=hold,
        entry_premium=round(option.entry_premium, 4),
        sl_premium=round(sl, 4),
        tp_premium=round(tp, 4),
        selected_option=option,
        auction=auction,
        chain=chain,
        diagnostics={
            "index": index,
            "minutes_to_close": minutes_to_close,
            "cost_r": round(cost_r, 6),
            "theta_r": round(theta_r, 6),
            "slippage_r": round(slippage_r, 6),
            "min_probability_threshold": round(threshold, 6),
        },
    )

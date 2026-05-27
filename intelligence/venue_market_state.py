"""Venue-local market-state alpha and continuous cross-venue evidence.

The signal engine in this module is deliberately *not* a price-change trigger.
It produces a shrunk expected-continuation estimate from confirmed venue-local
candles, current executable microstate and acceptance/expansion diagnostics.
Every venue is evaluated in its own price domain; cross-venue evidence may
adjust confidence and uncertainty but never creates raw price alpha or a hard
veto.
"""
from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import asdict, dataclass
from statistics import median
from typing import Any, Mapping

try:
    import config
except Exception:  # pragma: no cover
    config = None  # type: ignore

from market_data.normalizer import VenueMicrostate


def _cfg(name: str, default: Any) -> Any:
    return getattr(config, name, default) if config is not None else default


def _num(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        return out if math.isfinite(out) else default
    except Exception:
        return default


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(value)))


def _sign(value: float, eps: float = 1e-12) -> int:
    return 1 if value > eps else -1 if value < -eps else 0


def _close(row: Mapping[str, Any]) -> float:
    return _num(row.get("close", row.get("c", 0.0)), 0.0)


def _high(row: Mapping[str, Any]) -> float:
    return _num(row.get("high", row.get("h", 0.0)), 0.0)


def _low(row: Mapping[str, Any]) -> float:
    return _num(row.get("low", row.get("l", 0.0)), 0.0)


def _return_bps(newer: float, older: float) -> float:
    if newer <= 0 or older <= 0:
        return 0.0
    return math.log(newer / older) * 10_000.0


def _median_abs(values: list[float], floor: float = 0.25) -> float:
    valid = [abs(float(v)) for v in values if math.isfinite(float(v))]
    return max(float(floor), median(valid) if valid else float(floor))


@dataclass(frozen=True)
class VenueMarketState:
    venue: str
    symbol: str
    ready: bool
    reason: str
    signed_alpha_bps: float
    confidence: float
    uncertainty_bps: float
    regime_label: str
    returns_bps: dict[str, float]
    robust_one_minute_vol_bps: float
    volatility_expansion_ratio: float
    acceptance_bps: float
    live_impulse_bps: dict[str, float]
    diagnostics: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CrossVenueEvidence:
    asset_id: str
    agreement_score: float
    dispersion_bps: float
    leader_venue: str | None
    leader_confidence: float
    participating_venues: tuple[str, ...]
    signed_alpha_by_venue: dict[str, float]

    def confidence_for(self, venue: str, signed_direction: int) -> float:
        local = _sign(self.signed_alpha_by_venue.get(str(venue).lower(), 0.0))
        matches = local != 0 and local == signed_direction
        leader = str(venue).lower() == str(self.leader_venue or "").lower()
        if leader and matches:
            # A leader venue is permitted to originate alpha before all slower
            # venues agree; lack of confirmation increases risk, not a veto.
            return _clamp(0.78 + 0.22 * self.agreement_score, 0.55, 1.0)
        if matches:
            return _clamp(0.62 + 0.38 * self.agreement_score, 0.45, 1.0)
        return _clamp(0.45 + 0.35 * self.agreement_score, 0.30, 0.85)

    def uncertainty_for(self, venue: str) -> float:
        extra_max = float(_cfg("INSTITUTIONAL_CROSS_VENUE_UNCERTAINTY_MAX_BPS", 6.0))
        leader_discount = 0.65 if str(venue).lower() == str(self.leader_venue or "").lower() else 1.0
        return max(0.0, (1.0 - self.agreement_score) * extra_max * leader_discount)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class VenueMarketStateEngine:
    """Build venue-local structural continuation estimates without book merging."""

    def __init__(self, asset_id: str) -> None:
        self.asset_id = str(asset_id or "").upper()
        self._live_marks: dict[str, deque[tuple[float, float]]] = {}
        self._candle_cache: dict[tuple[str, str], tuple[float, list[dict[str, Any]]]] = {}

    def _params(self) -> dict[str, float]:
        raw = _cfg("INSTITUTIONAL_MARKET_STATE_PARAMETERS", {})
        specific = raw.get(self.asset_id, {}) if isinstance(raw, Mapping) else {}
        generic = raw.get("DEFAULT", {}) if isinstance(raw, Mapping) else {}
        merged = {**generic, **specific}
        return {
            "capture_rate": _num(merged.get("capture_rate"), 0.22),
            "max_alpha_bps": _num(merged.get("max_alpha_bps"), 30.0),
            "acceptance_weight": _num(merged.get("acceptance_weight"), 0.30),
            "live_impulse_weight": _num(merged.get("live_impulse_weight"), 0.20),
            "min_confidence": _num(merged.get("min_confidence"), 0.25),
        }

    @staticmethod
    def _venue_candle_fetcher(data_manager):
        return getattr(data_manager, "get_venue_candles", None)

    def _candles(self, data_manager, venue: str, timeframe: str, limit: int) -> list[dict[str, Any]]:
        key = (str(venue).lower(), timeframe)
        now = time.time()
        refresh = 3.0 if timeframe == "1m" else 20.0
        cached = self._candle_cache.get(key)
        if cached and now - cached[0] < refresh:
            return cached[1]
        getter = self._venue_candle_fetcher(data_manager)
        rows: list[dict[str, Any]] = []
        if callable(getter):
            try:
                rows = [dict(x) for x in (getter(venue, timeframe, limit) or []) if isinstance(x, Mapping)]
            except Exception:
                rows = []
        elif str(venue).lower() == str(getattr(data_manager, "venue", "")).lower():
            local_getter = getattr(data_manager, "get_candles", None)
            if callable(local_getter):
                try:
                    rows = [dict(x) for x in (local_getter(timeframe, limit) or []) if isinstance(x, Mapping)]
                except Exception:
                    rows = []
        self._candle_cache[key] = (now, rows)
        return rows

    def _observe_live(self, venue: str, mid: float) -> dict[str, float]:
        now = time.time()
        key = str(venue).lower()
        history = self._live_marks.setdefault(key, deque(maxlen=3000))
        if mid > 0 and (not history or now - history[-1][0] >= 0.10 or history[-1][1] != mid):
            history.append((now, mid))
        while history and now - history[0][0] > 180.0:
            history.popleft()
        out: dict[str, float] = {}
        for seconds in (5, 30, 60):
            anchor = None
            for ts, px in history:
                if now - ts >= seconds:
                    anchor = px
                else:
                    break
            out[f"{seconds}s"] = _return_bps(mid, anchor) if anchor else 0.0
        return out

    @staticmethod
    def _closed_candles(candles: list[dict[str, Any]]) -> list[dict[str, Any]]:
        '''Exclude the currently forming venue candle from structural alpha.

        Live midpoint observations deliberately cover intrabar timing; using
        the active bar again in structural returns would double count an
        unfinished impulse and can promote transient spikes into trend alpha.
        All supported data managers maintain the newest bar as a mutable bar.
        '''
        return list(candles[:-1]) if len(candles) >= 2 else []

    @staticmethod
    def _horizon_return(candles: list[dict[str, Any]], bars: int) -> float:
        closes = [_close(row) for row in candles if _close(row) > 0]
        if len(closes) <= bars:
            return 0.0
        return _return_bps(closes[-1], closes[-1 - bars])

    def build(self, data_manager, states: Mapping[str, VenueMicrostate]) -> dict[str, VenueMarketState]:
        results: dict[str, VenueMarketState] = {}
        params = self._params()
        for venue, state in states.items():
            venue_key = str(venue).lower()
            symbol = str(getattr(state, "symbol", getattr(getattr(state, "mapping", None), "venue_symbol", "")) or "")
            live = self._observe_live(venue_key, float(state.mid or 0.0))
            raw_1m = self._candles(data_manager, venue_key, "1m", 81)
            raw_5m = self._candles(data_manager, venue_key, "5m", 41)
            raw_15m = self._candles(data_manager, venue_key, "15m", 25)
            candles_1m = self._closed_candles(raw_1m)
            candles_5m = self._closed_candles(raw_5m)
            candles_15m = self._closed_candles(raw_15m)
            closes_1m = [_close(row) for row in candles_1m if _close(row) > 0]
            if len(closes_1m) < 20:
                results[venue_key] = VenueMarketState(
                    venue_key, symbol, False, "venue_local_candle_history_warmup",
                    0.0, 0.0, 8.0, "UNKNOWN", {}, 0.0, 1.0, 0.0, live,
                    {"closed_candle_counts": {"1m": len(candles_1m), "5m": len(candles_5m), "15m": len(candles_15m)},
                     "active_bar_excluded": True},
                )
                continue
            one_min_returns = [_return_bps(closes_1m[i], closes_1m[i - 1]) for i in range(1, len(closes_1m))]
            robust_vol = _median_abs(one_min_returns[-30:], floor=0.25)
            r_1m = self._horizon_return(candles_1m, 1)
            r_5m = self._horizon_return(candles_1m, 5)
            r_15m = self._horizon_return(candles_1m, 15)
            # 5m/15m bars are used as independent venue-local confirmation,
            # not concatenated with another venue's history.
            r_bar_5m = self._horizon_return(candles_5m, 1)
            r_bar_15m = self._horizon_return(candles_15m, 1)
            weighted_drift = (
                0.22 * r_1m + 0.26 * r_5m + 0.20 * r_15m
                + 0.18 * r_bar_5m + 0.14 * r_bar_15m
            )
            signed_terms = [r_1m, r_5m, r_15m, r_bar_5m, r_bar_15m]
            weights = [0.22, 0.26, 0.20, 0.18, 0.14]
            principal_sign = _sign(weighted_drift)
            sign_agreement = sum(w for w, term in zip(weights, signed_terms) if _sign(term) == principal_sign and principal_sign != 0)
            recent_vol = _median_abs(one_min_returns[-5:], floor=0.25)
            expansion_ratio = _clamp(recent_vol / max(robust_vol, 1e-9), 0.25, 4.0)
            prev_rows = candles_1m[-21:-1] if len(candles_1m) >= 21 else candles_1m[:-1]
            prior_high = max((_high(row) for row in prev_rows), default=0.0)
            prior_low = min((_low(row) for row in prev_rows if _low(row) > 0), default=0.0)
            acceptance_bps = 0.0
            mid = float(state.mid or 0.0)
            if prior_high > 0 and mid > prior_high:
                acceptance_bps = min(_return_bps(mid, prior_high), params["max_alpha_bps"])
            elif prior_low > 0 and mid < prior_low:
                acceptance_bps = -min(abs(_return_bps(mid, prior_low)), params["max_alpha_bps"])
            live_blend = 0.55 * live.get("30s", 0.0) + 0.45 * live.get("60s", 0.0)
            # Bayesian-style shrinkage: directional persistence and volatility
            # expansion increase the fraction of observed drift treated as
            # executable continuation; noisy disagreement shrinks toward zero.
            persistence = _clamp(sign_agreement, 0.0, 1.0)
            expansion_confidence = _clamp(0.65 + 0.20 * (expansion_ratio - 1.0), 0.45, 1.0)
            confidence = _clamp(persistence * expansion_confidence, params["min_confidence"], 1.0)
            structural_alpha = params["capture_rate"] * weighted_drift * confidence
            structural_alpha += params["acceptance_weight"] * acceptance_bps
            structural_alpha += params["live_impulse_weight"] * live_blend * confidence
            alpha = _clamp(structural_alpha, -params["max_alpha_bps"], params["max_alpha_bps"])
            z_drift = abs(weighted_drift) / max(robust_vol, 1e-9)
            if expansion_ratio >= 1.35 and z_drift >= 2.0:
                regime = "EXPANSION"
            elif persistence >= 0.60 and z_drift >= 1.0:
                regime = "TREND"
            else:
                regime = "BALANCE"
            uncertainty = max(0.0, (1.0 - confidence) * robust_vol * 0.35)
            results[venue_key] = VenueMarketState(
                venue=venue_key, symbol=symbol, ready=True, reason="venue_local_structural_state_ready",
                signed_alpha_bps=alpha, confidence=confidence, uncertainty_bps=uncertainty,
                regime_label=regime,
                returns_bps={"1m": r_1m, "5m": r_5m, "15m": r_15m, "bar_5m": r_bar_5m, "bar_15m": r_bar_15m},
                robust_one_minute_vol_bps=robust_vol, volatility_expansion_ratio=expansion_ratio,
                acceptance_bps=acceptance_bps, live_impulse_bps=live,
                diagnostics={"weighted_drift_bps": weighted_drift, "sign_agreement": sign_agreement, "z_drift": z_drift,
                             "active_bar_excluded": True,
                             "closed_candle_counts": {"1m": len(candles_1m), "5m": len(candles_5m), "15m": len(candles_15m)}},
            )
        return results


def build_continuous_cross_venue_evidence(asset_id: str, states: Mapping[str, VenueMarketState], microstates: Mapping[str, VenueMicrostate]) -> CrossVenueEvidence | None:
    """Produce continuous BTC peer evidence from structure, or live flow while candles warm.

    The fallback is not an entry alpha source: it is used only to avoid
    declaring peer evidence absent when multiple healthy venue tapes already
    agree before venue-local structural candles have populated the cache.
    """
    scored: dict[str, float] = {}
    quality_by_venue: dict[str, float] = {}
    for venue, micro in microstates.items():
        if not micro.usable_for_decision or float(micro.mid or 0.0) <= 0.0:
            continue
        market = states.get(venue)
        if market is not None and market.ready:
            score = float(market.signed_alpha_bps)
        else:
            near = sum(float(micro.bid_depth_usd_by_band.get(k, 0.0) + micro.ask_depth_usd_by_band.get(k, 0.0)) for k in ("0-1", "1-3"))
            if near <= 0:
                continue
            raw = (float(micro.ofi_usd_1s) + 0.50 * float(micro.ofi_usd_10s) + 0.20 * float(micro.tfi_usd_1s) + 0.10 * float(micro.tfi_usd_10s)) / near * 100.0
            score = 8.0 * math.tanh(raw / 8.0)
        scored[str(venue).lower()] = score
        quality_by_venue[str(venue).lower()] = max(float(micro.feed_quality_score or 0.0), 0.0)
    if len(scored) < 2:
        return None
    weights = {venue: max(quality_by_venue[venue] * max(abs(score), 0.25), 0.01) for venue, score in scored.items()}
    total_weight = sum(weights.values())
    directional_weight = sum(weights[v] for v in scored if _sign(scored[v]) != 0)
    net_signed = sum(weights[v] * _sign(scored[v]) for v in scored if _sign(scored[v]) != 0)
    agreement = abs(net_signed) / max(directional_weight, 1e-9)
    leader = max(scored, key=lambda v: abs(scored[v]) * quality_by_venue[v])
    leader_conf = weights[leader] / max(total_weight, 1e-9)
    mids = [float(microstates[v].mid) for v in scored if float(microstates[v].mid or 0.0) > 0]
    dispersion = 0.0
    if mids:
        med = median(mids)
        dispersion = (max(mids) - min(mids)) / max(med, 1e-9) * 10_000.0
    return CrossVenueEvidence(
        asset_id=str(asset_id), agreement_score=_clamp(agreement, 0.0, 1.0), dispersion_bps=max(0.0, dispersion),
        leader_venue=leader, leader_confidence=_clamp(leader_conf, 0.0, 1.0),
        participating_venues=tuple(sorted(scored)), signed_alpha_by_venue=scored,
    )

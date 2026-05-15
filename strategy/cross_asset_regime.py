"""
strategy/cross_asset_regime.py — institutional cross-asset regime overlay
===========================================================================

Portfolio-level BTC/GOLD/SILVER context.  This module deliberately does not
create entries and never vetoes an otherwise executable liquidity thesis.  It
computes a probability/sizing/TP/SL overlay that is consumed by QuantStrategy,
EntryEngine and PortfolioManager.

Core design:
    • Liquidity-first strategy remains the alpha owner.
    • Correlation is context, not a signal.
    • Relative-value residuals decide GOLD vs SILVER preference.
    • Portfolio covariance controls exposure so correlated positions do not
      masquerade as diversification.
"""
from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

try:  # config is optional for standalone unit tests
    import config  # type: ignore
except Exception:  # pragma: no cover
    config = None  # type: ignore

logger = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────────────────────
# Small numeric helpers
# ────────────────────────────────────────────────────────────────────────────

def _cfg(name: str, default: Any) -> Any:
    try:
        if config is None:
            return default
        return getattr(config, name, default)
    except Exception:
        return default


def _clamp(v: Any, lo: float, hi: float) -> float:
    try:
        f = float(v)
        if not math.isfinite(f):
            return float(lo)
        return max(float(lo), min(float(hi), f))
    except Exception:
        return float(lo)


def _safe_close(candle: Any) -> float:
    try:
        if isinstance(candle, dict):
            return float(candle.get("close") or candle.get("c") or 0.0)
        return float(getattr(candle, "close", 0.0) or 0.0)
    except Exception:
        return 0.0


def _log_returns(candles: Sequence[Any]) -> List[float]:
    closes = [_safe_close(c) for c in (candles or [])]
    closes = [c for c in closes if c > 0 and math.isfinite(c)]
    out: List[float] = []
    for a, b in zip(closes[:-1], closes[1:]):
        if a > 0 and b > 0:
            out.append(math.log(b / a))
    return out


def _ewma_weights(n: int, half_life: float) -> List[float]:
    if n <= 0:
        return []
    hl = max(float(half_life), 1.0)
    # Newest observation gets the largest weight.
    raw = [0.5 ** ((n - 1 - i) / hl) for i in range(n)]
    s = sum(raw) or 1.0
    return [x / s for x in raw]


def _weighted_mean(x: Sequence[float], w: Sequence[float]) -> float:
    return sum(float(a) * float(b) for a, b in zip(x, w))


def _weighted_corr(x: Sequence[float], y: Sequence[float], half_life: float) -> float:
    n = min(len(x), len(y))
    if n < 8:
        return 0.0
    xs = list(x[-n:])
    ys = list(y[-n:])
    w = _ewma_weights(n, half_life)
    mx = _weighted_mean(xs, w)
    my = _weighted_mean(ys, w)
    cov = sum(wi * (xi - mx) * (yi - my) for xi, yi, wi in zip(xs, ys, w))
    vx = sum(wi * (xi - mx) ** 2 for xi, wi in zip(xs, w))
    vy = sum(wi * (yi - my) ** 2 for yi, wi in zip(ys, w))
    if vx <= 1e-18 or vy <= 1e-18:
        return 0.0
    return _clamp(cov / math.sqrt(vx * vy), -1.0, 1.0)


def _weighted_beta(y: Sequence[float], x: Sequence[float], half_life: float) -> float:
    """Beta of y vs x using EWMA covariance/variance."""
    n = min(len(x), len(y))
    if n < 8:
        return 1.0
    xs = list(x[-n:])
    ys = list(y[-n:])
    w = _ewma_weights(n, half_life)
    mx = _weighted_mean(xs, w)
    my = _weighted_mean(ys, w)
    cov = sum(wi * (xi - mx) * (yi - my) for xi, yi, wi in zip(xs, ys, w))
    vx = sum(wi * (xi - mx) ** 2 for xi, wi in zip(xs, w))
    if vx <= 1e-18:
        return 1.0
    return _clamp(cov / vx, -5.0, 5.0)


def _zscore_latest(values: Sequence[float], half_life: float) -> float:
    n = len(values)
    if n < 10:
        return 0.0
    xs = list(values[-n:])
    w = _ewma_weights(n, half_life)
    mu = _weighted_mean(xs, w)
    var = sum(wi * (xi - mu) ** 2 for xi, wi in zip(xs, w))
    sd = math.sqrt(max(var, 1e-18))
    return _clamp((xs[-1] - mu) / sd, -6.0, 6.0)


def _sum_tail(values: Sequence[float], n: int) -> float:
    if not values:
        return 0.0
    k = max(1, min(int(n), len(values)))
    return float(sum(values[-k:]))


def _sigmoid(x: float) -> float:
    x = _clamp(x, -30.0, 30.0)
    return 1.0 / (1.0 + math.exp(-x))


def logit_adjust_probability(p: float, logit_delta: float) -> float:
    p = _clamp(p, 0.001, 0.999)
    l = math.log(p / (1.0 - p)) + float(logit_delta)
    return _clamp(_sigmoid(l), 0.001, 0.999)


# ────────────────────────────────────────────────────────────────────────────
# Public dataclasses
# ────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class CrossAssetAdjustment:
    enabled: bool = False
    asset: str = ""
    side: str = ""
    posterior_logit_adjust: float = 0.0
    risk_multiplier: float = 1.0
    tp_aggression: float = 0.0
    sl_buffer_multiplier: float = 1.0
    cluster_risk_penalty: float = 0.0
    preferred_asset: str = ""
    regime: str = "INSUFFICIENT_DATA"
    entry_allowed: bool = True
    block_reason: str = ""
    reason: str = ""

    def adjusted_probability(self, base_probability: float) -> float:
        if not self.enabled or base_probability <= 0.0:
            return float(base_probability or 0.0)
        return logit_adjust_probability(base_probability, self.posterior_logit_adjust)


@dataclass
class CrossAssetState:
    enabled: bool = False
    ts: float = 0.0
    timeframe: str = "5m"
    window: int = 48
    assets_seen: List[str] = field(default_factory=list)
    returns: Dict[str, float] = field(default_factory=dict)
    corr: Dict[str, float] = field(default_factory=dict)
    beta_silver_gold: float = 1.0
    silver_residual_z: float = 0.0
    gold_silver_ratio_z: float = 0.0
    metals_regime: str = "INSUFFICIENT_DATA"
    btc_macro_role: str = "INSUFFICIENT_DATA"
    preferred_asset: str = ""
    relationship_quality: float = 0.0
    cluster_risk_score: float = 0.0
    positions: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    notes: List[str] = field(default_factory=list)
    _asset_adj: Dict[Tuple[str, str], CrossAssetAdjustment] = field(default_factory=dict)

    def age_sec(self) -> float:
        return max(0.0, time.time() - float(self.ts or 0.0))

    def is_fresh(self) -> bool:
        max_age = float(_cfg("CROSS_ASSET_MAX_STATE_AGE_SEC", 20.0))
        return self.enabled and self.ts > 0 and self.age_sec() <= max_age

    def adjustment_for(self, asset: str, side: str) -> CrossAssetAdjustment:
        asset_u = str(asset or "").upper()
        side_l = str(side or "").lower()
        if not self.is_fresh():
            return CrossAssetAdjustment(asset=asset_u, side=side_l, regime=self.metals_regime)
        return self._asset_adj.get(
            (asset_u, side_l),
            CrossAssetAdjustment(enabled=True, asset=asset_u, side=side_l,
                                 preferred_asset=self.preferred_asset,
                                 regime=self.metals_regime,
                                 reason="fresh cross-asset state; no material adjustment"),
        )

    # Compatibility hook used by TP-ladder sizing.  Earlier builds called this
    # name but CrossAssetState did not expose it, causing the ladder to fall back
    # to weak regime-only sponsorship.
    def adjustment_for_signal(self, asset: str, side: str) -> CrossAssetAdjustment:
        return self.adjustment_for(asset, side)

    def summary(self) -> str:
        if not self.enabled:
            return "cross-asset insufficient"
        gs = self.corr.get("GOLD:SILVER", 0.0)
        bg = self.corr.get("BTC:GOLD", 0.0)
        bs = self.corr.get("BTC:SILVER", 0.0)
        pref = self.preferred_asset or "none"
        return (f"metals={self.metals_regime} pref={pref} relQ={self.relationship_quality:.2f} "
                f"GSρ={gs:+.2f} BTC/Gρ={bg:+.2f} BTC/Sρ={bs:+.2f} "
                f"AgRVz={self.silver_residual_z:+.2f} cluster={self.cluster_risk_score:.2f}")

    def as_dict(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled,
            "ts": self.ts,
            "timeframe": self.timeframe,
            "window": self.window,
            "assets_seen": list(self.assets_seen),
            "returns": dict(self.returns),
            "corr": dict(self.corr),
            "beta_silver_gold": self.beta_silver_gold,
            "silver_residual_z": self.silver_residual_z,
            "gold_silver_ratio_z": self.gold_silver_ratio_z,
            "metals_regime": self.metals_regime,
            "btc_macro_role": self.btc_macro_role,
            "preferred_asset": self.preferred_asset,
            "relationship_quality": self.relationship_quality,
            "cluster_risk_score": self.cluster_risk_score,
            "positions": dict(self.positions),
            "notes": list(self.notes),
            "summary": self.summary(),
        }


# ────────────────────────────────────────────────────────────────────────────
# Regime engine
# ────────────────────────────────────────────────────────────────────────────

class CrossAssetRegimeEngine:
    """Computes BTC/GOLD/SILVER regime state from live candles."""

    def __init__(self) -> None:
        self.enabled = bool(_cfg("CROSS_ASSET_OVERLAY_ENABLED", True))
        self.timeframe = str(_cfg("CROSS_ASSET_TIMEFRAME", "5m"))
        self.window = int(_cfg("CROSS_ASSET_WINDOW", 48))
        self.horizon = int(_cfg("CROSS_ASSET_HORIZON_BARS", 6))
        self.half_life = float(_cfg("CROSS_ASSET_EWMA_HALFLIFE", 16.0))
        self.min_returns = int(_cfg("CROSS_ASSET_MIN_RETURNS", 24))
        self.update_interval_sec = float(_cfg("CROSS_ASSET_UPDATE_INTERVAL_SEC", 2.0))
        self._last_update = 0.0
        self.state = CrossAssetState(enabled=False, timeframe=self.timeframe, window=self.window)

    @staticmethod
    def _canonical_asset(asset_id: str) -> str:
        aid = str(asset_id or "").upper()
        if aid in {"BTC", "BTCUSD", "XBT"}:
            return "BTC"
        if aid in {"GOLD", "PAXG", "PAXGUSD", "XAU", "XAUT"}:
            return "GOLD"
        if aid in {"SILVER", "SLVON", "SLVONUSD", "XAG", "SLV"}:
            return "SILVER"
        return aid

    def _read_returns_by_asset(self, contexts: Iterable[Any]) -> Dict[str, List[float]]:
        out: Dict[str, List[float]] = {}
        limit = max(self.window + 5, self.min_returns + 5)
        for ctx in list(contexts or []):
            try:
                inst = getattr(ctx, "instrument", None)
                asset = self._canonical_asset(getattr(inst, "asset_id", ""))
                if asset not in {"BTC", "GOLD", "SILVER"}:
                    continue
                dm = getattr(ctx, "data_manager", None)
                if dm is None:
                    continue
                candles = dm.get_candles(self.timeframe, limit)
                rets = _log_returns(candles)[-self.window:]
                if len(rets) >= self.min_returns:
                    out[asset] = rets
            except Exception as e:
                logger.debug("cross-asset candle read failed: %s", e)
        return out

    def _read_positions_by_asset(self, contexts: Iterable[Any]) -> Dict[str, Dict[str, Any]]:
        """Read live BTC/GOLD/SILVER position sides for cross-desk exposure control."""
        out: Dict[str, Dict[str, Any]] = {}
        for ctx in list(contexts or []):
            try:
                inst = getattr(ctx, "instrument", None)
                asset = self._canonical_asset(getattr(inst, "asset_id", ""))
                if asset not in {"BTC", "GOLD", "SILVER"}:
                    continue
                strat = getattr(ctx, "strategy", None)
                pos = strat.get_position() if strat is not None and hasattr(strat, "get_position") else None
                if not isinstance(pos, dict):
                    continue
                side = str(pos.get("side", "") or "").lower()
                qty = _clamp(pos.get("quantity", 0.0), 0.0, 1e18)
                if side in {"long", "short"} and qty > 0:
                    out[asset] = {"side": side, "quantity": qty, "entry": _clamp(pos.get("entry_price", 0.0), 0.0, 1e18)}
            except Exception as e:
                logger.debug("cross-asset position read failed: %s", e)
        return out

    def update_from_contexts(self, contexts: Iterable[Any], *, force: bool = False) -> CrossAssetState:
        now = time.time()
        if not self.enabled:
            self.state = CrossAssetState(enabled=False, ts=now, timeframe=self.timeframe, window=self.window)
            return self.state
        if not force and now - self._last_update < self.update_interval_sec:
            return self.state
        self._last_update = now

        ctx_list = list(contexts or [])
        series = self._read_returns_by_asset(ctx_list)
        state = self._build_state(series, now)
        state.positions = self._read_positions_by_asset(ctx_list)
        # Position-aware exposure adjustments must be built after live positions
        # are known.  This prevents simultaneous unsound metal pair exposure.
        state._asset_adj = self._build_adjustments(state)
        state.enabled = bool(len(state._asset_adj) > 0 or state.metals_regime != "INSUFFICIENT_DATA")
        self.state = state
        return state

    def _build_state(self, series: Dict[str, List[float]], now: float) -> CrossAssetState:
        assets_seen = sorted(series.keys())
        state = CrossAssetState(
            enabled=False,
            ts=now,
            timeframe=self.timeframe,
            window=self.window,
            assets_seen=assets_seen,
        )
        if not {"BTC", "GOLD", "SILVER"}.intersection(series.keys()):
            state.notes.append("no BTC/GOLD/SILVER candles available")
            return state

        for asset, vals in series.items():
            state.returns[asset] = _sum_tail(vals, self.horizon)

        g = series.get("GOLD", [])
        s = series.get("SILVER", [])
        b = series.get("BTC", [])
        if len(g) >= self.min_returns and len(s) >= self.min_returns:
            state.corr["GOLD:SILVER"] = _weighted_corr(g, s, self.half_life)
            # Baseline correlation excludes the latest bar so one catch-up/divergence
            # print does not erase the structural relationship we are trying to price.
            if len(g) > 10 and len(s) > 10:
                state.corr["GOLD:SILVER_BASE"] = _weighted_corr(g[:-1], s[:-1], self.half_life)
            state.beta_silver_gold = _weighted_beta(s, g, self.half_life)
            n = min(len(g), len(s))
            beta = state.beta_silver_gold
            residuals = [sv - beta * gv for sv, gv in zip(s[-n:], g[-n:])]
            state.silver_residual_z = _zscore_latest(residuals, self.half_life)
            ratio_proxy = [gv - sv for gv, sv in zip(g[-n:], s[-n:])]
            state.gold_silver_ratio_z = _zscore_latest(ratio_proxy, self.half_life)
        if len(b) >= self.min_returns and len(g) >= self.min_returns:
            state.corr["BTC:GOLD"] = _weighted_corr(b, g, self.half_life)
        if len(b) >= self.min_returns and len(s) >= self.min_returns:
            state.corr["BTC:SILVER"] = _weighted_corr(b, s, self.half_life)

        state.relationship_quality = self._relationship_quality(state)
        state.metals_regime = self._classify_metals(state)
        state.btc_macro_role = self._classify_btc_role(state)
        state.preferred_asset = self._preferred_asset(state)
        state.cluster_risk_score = self._cluster_risk_score(state)
        state._asset_adj = self._build_adjustments(state)
        state.enabled = bool(len(state._asset_adj) > 0 or state.metals_regime != "INSUFFICIENT_DATA")
        if state.enabled:
            state.notes.append(state.summary())
        return state

    def _relationship_quality(self, st: CrossAssetState) -> float:
        gs = max(abs(st.corr.get("GOLD:SILVER", 0.0)), abs(st.corr.get("GOLD:SILVER_BASE", 0.0)))
        # Relationship quality is deliberately conservative.  Relative-value
        # signals are only valid when the metal pair is statistically coupled;
        # otherwise a large residual is just de-correlated noise, not alpha.
        return _clamp((gs - 0.20) / 0.45, 0.0, 1.0)

    def _classify_metals(self, st: CrossAssetState) -> str:
        gs = max(st.corr.get("GOLD:SILVER", 0.0), st.corr.get("GOLD:SILVER_BASE", 0.0))
        gr = st.returns.get("GOLD", 0.0)
        sr = st.returns.get("SILVER", 0.0)
        if "GOLD" not in st.returns or "SILVER" not in st.returns:
            return "INSUFFICIENT_DATA"
        strong_corr = gs >= float(_cfg("CROSS_ASSET_METALS_CORR_STRONG", 0.55))
        rv_corr = gs >= float(_cfg("CROSS_ASSET_RV_MIN_RELATIONSHIP_CORR", 0.35))
        if strong_corr and gr > 0 and sr > 0:
            return "COHERENT_UPTREND"
        if strong_corr and gr < 0 and sr < 0:
            return "COHERENT_DOWNTREND"
        if rv_corr and abs(st.silver_residual_z) >= 1.0:
            return "RELATIVE_VALUE_DIVERGENCE"
        if abs(st.silver_residual_z) >= 1.0 and not rv_corr:
            return "DECORRELATED_NOISE"
        if abs(gs) < 0.25:
            return "NOISE"
        return "MIXED"

    def _classify_btc_role(self, st: CrossAssetState) -> str:
        br = st.returns.get("BTC", 0.0)
        gr = st.returns.get("GOLD", 0.0)
        sr = st.returns.get("SILVER", 0.0)
        bg = st.corr.get("BTC:GOLD", 0.0)
        bs = st.corr.get("BTC:SILVER", 0.0)
        if "BTC" not in st.returns:
            return "INSUFFICIENT_DATA"
        metals_ret = (gr + sr) / 2.0 if ("GOLD" in st.returns and "SILVER" in st.returns) else 0.0
        if br > 0 and metals_ret > 0 and (bg + bs) / 2.0 > 0.25:
            return "BROAD_LIQUIDITY_EXPANSION"
        if br > 0 and metals_ret <= 0:
            return "RISK_ON_OR_CRYPTO_SPECIFIC"
        if br < 0 and metals_ret > 0:
            return "RISK_OFF_DIVERGENCE"
        return "IDIOSYNCRATIC_CRYPTO_FLOW"

    def _preferred_asset(self, st: CrossAssetState) -> str:
        gs = max(st.corr.get("GOLD:SILVER", 0.0), st.corr.get("GOLD:SILVER_BASE", 0.0))
        gr = st.returns.get("GOLD", 0.0)
        sr = st.returns.get("SILVER", 0.0)
        rv = st.silver_residual_z
        if st.relationship_quality <= 0.0:
            return ""
        if gs >= 0.35 and gr > 0 and rv < -0.80:
            return "SILVER"
        if gs >= 0.35 and gr > 0 and rv > 0.95:
            return "GOLD"
        if gs >= 0.35 and gr < 0 and rv > 0.80:
            return "SILVER"  # relative short candidate: silver rich vs falling gold
        if gs >= 0.35 and sr < 0 and rv < -0.95:
            return "GOLD"
        return ""

    def _cluster_risk_score(self, st: CrossAssetState) -> float:
        gs = max(abs(st.corr.get("GOLD:SILVER", 0.0)), abs(st.corr.get("GOLD:SILVER_BASE", 0.0)))
        bg = abs(st.corr.get("BTC:GOLD", 0.0))
        bs = abs(st.corr.get("BTC:SILVER", 0.0))
        metals_same = 1.0 if st.returns.get("GOLD", 0.0) * st.returns.get("SILVER", 0.0) > 0 else 0.0
        btc_same_g = 1.0 if st.returns.get("BTC", 0.0) * st.returns.get("GOLD", 0.0) > 0 else 0.0
        btc_same_s = 1.0 if st.returns.get("BTC", 0.0) * st.returns.get("SILVER", 0.0) > 0 else 0.0
        raw = 0.55 * gs * metals_same + 0.225 * bg * btc_same_g + 0.225 * bs * btc_same_s
        return _clamp(raw, 0.0, 0.85)

    def _build_adjustments(self, st: CrossAssetState) -> Dict[Tuple[str, str], CrossAssetAdjustment]:
        out: Dict[Tuple[str, str], CrossAssetAdjustment] = {}
        gs = max(st.corr.get("GOLD:SILVER", 0.0), st.corr.get("GOLD:SILVER_BASE", 0.0))
        gr = st.returns.get("GOLD", 0.0)
        sr = st.returns.get("SILVER", 0.0)
        br = st.returns.get("BTC", 0.0)
        rv = st.silver_residual_z
        cluster = st.cluster_risk_score
        metals_conf = _clamp((gs - 0.35) / 0.50, 0.0, 1.0)

        def put(asset: str, side: str, logit: float = 0.0, risk: float = 1.0,
                tp: float = 0.0, sl_mult: float = 1.0, penalty: float = 0.0,
                reason: str = "", entry_allowed: bool = True, block_reason: str = "") -> None:
            # Cluster penalty applies to sizing but never to the posterior itself.
            risk_f = _clamp(risk * (1.0 - max(penalty, 0.0)), 0.25, 1.25)
            out[(asset, side)] = CrossAssetAdjustment(
                enabled=True,
                asset=asset,
                side=side,
                posterior_logit_adjust=_clamp(logit, -0.45, 0.45),
                risk_multiplier=risk_f,
                tp_aggression=_clamp(tp, -0.35, 0.35),
                sl_buffer_multiplier=_clamp(sl_mult, 0.75, 1.35),
                cluster_risk_penalty=_clamp(penalty, 0.0, 0.75),
                preferred_asset=st.preferred_asset,
                regime=st.metals_regime,
                entry_allowed=bool(entry_allowed),
                block_reason=str(block_reason or ""),
                reason=reason,
            )

        # GOLD/SILVER coherent metals trend: slight posterior boost, but size is
        # cluster-aware.  The bot can still take either instrument if its own
        # liquidity thesis is strong.
        if st.metals_regime == "COHERENT_UPTREND":
            base = 0.08 * metals_conf
            put("GOLD", "long", logit=base, risk=1.03, tp=0.08, sl_mult=1.04,
                penalty=0.20 * cluster, reason="coherent metals uptrend")
            put("SILVER", "long", logit=base, risk=1.03, tp=0.10, sl_mult=1.05,
                penalty=0.20 * cluster, reason="coherent metals uptrend")
            put("GOLD", "short", logit=-0.07 * metals_conf, risk=0.75, tp=-0.12,
                penalty=0.10 * cluster, reason="short against coherent metals uptrend")
            put("SILVER", "short", logit=-0.07 * metals_conf, risk=0.75, tp=-0.12,
                penalty=0.10 * cluster, reason="short against coherent metals uptrend")
        elif st.metals_regime == "COHERENT_DOWNTREND":
            base = 0.08 * metals_conf
            put("GOLD", "short", logit=base, risk=1.03, tp=0.08, sl_mult=1.04,
                penalty=0.20 * cluster, reason="coherent metals downtrend")
            put("SILVER", "short", logit=base, risk=1.03, tp=0.10, sl_mult=1.05,
                penalty=0.20 * cluster, reason="coherent metals downtrend")
            put("GOLD", "long", logit=-0.07 * metals_conf, risk=0.75, tp=-0.12,
                penalty=0.10 * cluster, reason="long against coherent metals downtrend")
            put("SILVER", "long", logit=-0.07 * metals_conf, risk=0.75, tp=-0.12,
                penalty=0.10 * cluster, reason="long against coherent metals downtrend")

        # Relative-value overlay.  Silver is preferred only when the residual and
        # liquidity direction agree; this allows farther TPs on catch-up but avoids
        # chasing overextension.
        if st.relationship_quality > 0.0 and gs >= 0.35 and gr > 0 and rv < -0.80:
            put("SILVER", "long", logit=0.16, risk=1.12, tp=0.26, sl_mult=1.10,
                penalty=0.12 * cluster, reason="silver lagging gold; catch-up candidate")
            put("GOLD", "long", logit=0.04, risk=0.92, tp=0.02, sl_mult=1.00,
                penalty=0.10 * cluster, reason="gold leader; silver has better RV")
        if st.relationship_quality > 0.0 and gs >= 0.35 and gr > 0 and rv > 0.95:
            put("SILVER", "long", logit=-0.16, risk=0.62, tp=-0.22, sl_mult=0.92,
                penalty=0.10 * cluster, reason="silver overextended versus gold")
            put("GOLD", "long", logit=0.08, risk=1.04, tp=0.08, sl_mult=1.03,
                penalty=0.12 * cluster, reason="gold cleaner than overextended silver")
        if st.relationship_quality > 0.0 and gs >= 0.35 and gr < 0 and rv > 0.80:
            put("SILVER", "short", logit=0.14, risk=1.08, tp=0.20, sl_mult=1.08,
                penalty=0.12 * cluster, reason="silver rich while gold leads lower")
        if st.relationship_quality > 0.0 and gs >= 0.35 and sr < 0 and rv < -0.95:
            put("SILVER", "short", logit=-0.13, risk=0.65, tp=-0.20, sl_mult=0.92,
                penalty=0.10 * cluster, reason="silver already overextended lower")

        # BTC role is separate from metals; only adjust BTC when macro role is clear.
        if "BTC" in st.returns:
            if st.btc_macro_role == "BROAD_LIQUIDITY_EXPANSION" and br > 0:
                put("BTC", "long", logit=0.07, risk=1.04, tp=0.08, sl_mult=1.02,
                    penalty=0.15 * cluster, reason="broad liquidity expansion")
                put("BTC", "short", logit=-0.06, risk=0.80, tp=-0.08,
                    penalty=0.05 * cluster, reason="short against broad liquidity expansion")
            elif st.btc_macro_role == "RISK_OFF_DIVERGENCE":
                put("BTC", "long", logit=-0.10, risk=0.70, tp=-0.14, sl_mult=0.95,
                    penalty=0.05 * cluster, reason="BTC falling while metals bid; risk-off divergence")
                put("GOLD", "long", logit=0.06, risk=1.02, tp=0.08, sl_mult=1.03,
                    penalty=0.12 * cluster, reason="risk-off metals bid")
                put("SILVER", "long", logit=0.04, risk=0.96, tp=0.04, sl_mult=1.02,
                    penalty=0.15 * cluster, reason="risk-off metals bid; silver beta controlled")
            elif st.btc_macro_role == "RISK_ON_OR_CRYPTO_SPECIFIC" and br > 0:
                put("BTC", "long", logit=0.05, risk=1.02, tp=0.06, sl_mult=1.00,
                    penalty=0.05 * cluster, reason="BTC risk-on/crypto-specific strength")


        # Position-aware metal-pair governance.  Opposite GOLD/SILVER exposure is
        # a relative-value pair trade.  It is only allowed when the statistical
        # relationship is valid and the preferred asset/residual direction explains
        # the pair.  Otherwise it is an unintended hedge/contradiction that consumes
        # margin and corrupts P&L attribution.
        positions = getattr(st, "positions", {}) or {}
        def block_pair(asset: str, side: str, other: str, other_side: str, why: str) -> None:
            put(asset, side, logit=-0.45, risk=0.25, tp=-0.35, sl_mult=0.92,
                penalty=0.35, reason=why, entry_allowed=False, block_reason=why)

        for asset, other in (("GOLD", "SILVER"), ("SILVER", "GOLD")):
            other_pos = positions.get(other)
            if not other_pos:
                continue
            other_side = str(other_pos.get("side", "") or "").lower()
            if other_side not in {"long", "short"}:
                continue
            for candidate_side in ("long", "short"):
                if candidate_side == other_side:
                    # Same-direction metal stacking is allowed but size is cluster-aware.
                    old = out.get((asset, candidate_side))
                    if old is None:
                        put(asset, candidate_side, risk=max(0.45, 1.0 - 0.45 * cluster), tp=-0.05,
                            penalty=0.20 * cluster, reason=f"same-direction metal cluster with active {other} {other_side}")
                    continue
                # Opposite direction requires a true RV pair thesis.
                pair_ok = False
                if st.metals_regime == "RELATIVE_VALUE_DIVERGENCE" and st.relationship_quality > 0.0:
                    if asset == "GOLD" and candidate_side == "long" and other == "SILVER" and other_side == "short" and rv > 0.95 and gr >= 0:
                        pair_ok = True
                    if asset == "SILVER" and candidate_side == "long" and other == "GOLD" and other_side == "short" and rv < -0.80 and sr >= 0:
                        pair_ok = True
                    if asset == "GOLD" and candidate_side == "short" and other == "SILVER" and other_side == "long" and rv < -0.95 and gr <= 0:
                        pair_ok = True
                    if asset == "SILVER" and candidate_side == "short" and other == "GOLD" and other_side == "long" and rv > 0.80 and sr <= 0:
                        pair_ok = True
                if not pair_ok:
                    block_pair(asset, candidate_side, other, other_side,
                               f"blocked unsponsored metal pair: active {other} {other_side}, candidate {asset} {candidate_side}; regime={st.metals_regime} relQ={st.relationship_quality:.2f}")

        return out

"""
mtf_pool_probability.py — Multi-Timeframe Liquidity Pool Sweep Probability Engine
==================================================================================
ISSUE-1 FIX: Direction engine lacked per-pool sweep probability across timeframes.

PROBLEM:
  predict_hunt() used a single "pool asymmetry" factor (0.09 weight) that summed
  all BSL vs SSL significance. This is a MAGNITUDE comparison, not a PROBABILITY.
  It told you which SIDE had more liquidity, not WHICH POOL gets swept FIRST or
  HOW LIKELY each pool is to be swept. A 1D pool 8 ATR away scored higher than a
  2m EQH 0.4 ATR away — completely backwards.

SOLUTION — Two-layer probability model:
  LAYER 1: Per-pool sweep probability (per tick, per TF)
    prob(pool_i) = distance_decay(d_i) × tf_base_prob(tf_i) × significance(sig_i)
                 × recency_bonus(age_i) × touch_momentum(t_i)

    distance_decay: exponential e^(-d/λ) where λ = TF-specific half-life.
      2m  pools: λ = 0.5 ATR  (very short — must be nearly touching)
      15m pools: λ = 1.2 ATR
      1h  pools: λ = 2.5 ATR
      4h  pools: λ = 4.0 ATR
      1d  pools: λ = 6.0 ATR

    tf_base_prob: the prior probability that THIS timeframe pool gets swept first.
      2m  → 0.35  (smallest, swept most often by institutional spoofing)
      15m → 0.55  (London/NY open manipulation target)
      1h  → 0.70  (session highs/lows, most reliable)
      4h  → 0.80  (highest quality sweep = strongest reversal)
      1d  → 0.65  (daily often respected not swept intraday)

  LAYER 2: BSL vs SSL aggregate prediction
    BSL_prob = Σ prob(bsl_i)   ; SSL_prob = Σ prob(ssl_i)
    normalise, apply confidence cap from sample size.

DIRECTIONAL RULE (ICT-aligned):
  Higher BSL probability → price MORE likely to hunt stops above → BSL sweep next.
  Higher SSL probability → price MORE likely to hunt stops below → SSL sweep next.

This module exposes:
  1. MTFPoolProbability — standalone class (use in quant_strategy for display/logging)
  2. compute_mtf_pool_factor(liq_snapshot, price, atr, now) → float in [-1, +1]
     Drop-in replacement for Factor 5 in direction_engine.predict_hunt().

HOW TO WIRE INTO direction_engine.py:
  In DirectionEngine.predict_hunt(), replace the FACTOR 5 block with:

    from mtf_pool_probability import compute_mtf_pool_factor
    factors.pool_asymmetry = compute_mtf_pool_factor(
        liq_snapshot = liq_snapshot,
        price        = price,
        atr          = a,
        now          = now_ms / 1000.0,
        ict_engine   = ict_engine,   # optional — adds 2m pool fallback
    )
"""

from __future__ import annotations

import logging
import math
import time
from datetime import datetime, time as dt_time, timezone, timedelta
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

try:
    from strategy.market_intelligence import build_market_profile, MarketProfile
except Exception:  # pragma: no cover - standalone tests
    from market_intelligence import build_market_profile, MarketProfile  # type: ignore


# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS — calibrated to BTC perpetual intraday micro-structure
# ─────────────────────────────────────────────────────────────────────────────

# Distance decay half-life per TF (in ATR units).
# Smaller λ = steep decay = pool must be VERY close to matter.
_DECAY_LAMBDA: Dict[str, float] = {
    "2m":  0.50,
    "5m":  0.80,
    "15m": 1.20,
    "1h":  2.50,
    "4h":  4.00,
    "1d":  6.00,
}

# Base sweep probability prior per TF (independent of distance).
# ICT logic: 1h/4h sweeps are the PRIMARY institutional targets.
# 2m sweeps are noise/spoofing; 1d rarely swept intraday.
_TF_BASE_PROB: Dict[str, float] = {
    "2m":  0.35,
    "5m":  0.45,
    "15m": 0.55,
    "1h":  0.70,
    "4h":  0.80,
    "1d":  0.65,
}

# Maximum distance (ATR) beyond which a pool is considered out-of-range.
# Even a 1D pool > 12 ATR away is irrelevant for the current session.
_MAX_REACH_ATR: Dict[str, float] = {
    "2m":  1.5,
    "5m":  3.0,
    "15m": 5.0,
    "1h":  8.0,
    "4h":  12.0,
    "1d":  18.0,
}

# Significance normalisation cap (pools above this are treated as max sig).
_SIG_CAP = 12.0

# Recency bonus: pools formed within this many seconds get a small premium.
_RECENCY_WINDOW_SEC: Dict[str, float] = {
    "2m":   600,     # 10 min
    "5m":  1800,     # 30 min
    "15m": 3600,     # 1 hour
    "1h":  14400,    # 4 hours
    "4h":  86400,    # 1 day
    "1d":  604800,   # 1 week
}
_RECENCY_BONUS = 0.20    # +20% probability for fresh pools

# Touch momentum: each additional touch that doesn't sweep adds conviction.
_TOUCH_BONUS_PER = 0.10  # +10% per extra touch, capped at +40%
_TOUCH_BONUS_CAP = 0.40

# Confidence scaling vs sample size (prevent over-confidence from 1–2 pools).
_MIN_CONFIDENCE_POOLS = 2    # below this → confidence capped at 0.5

# Session-based multiplier applied to BSL vs SSL independently.
# London: tends to sweep BSL first (Judas swing UP) → amplify BSL probs
# NY:     depends on London direction, but often reversal → slight SSL bias
# Asia:   consolidation → reduce all probs, stay neutral
_SESSION_MULT: Dict[str, Dict[str, float]] = {
    "LONDON": {"BSL": 1.20, "SSL": 0.85},
    "NY":     {"BSL": 1.05, "SSL": 1.10},
    "ASIA":   {"BSL": 0.80, "SSL": 0.80},
    "":       {"BSL": 1.00, "SSL": 1.00},
}


# Indian index/F&O options are not BTC perpetuals.  They trade in a finite NSE
# session, gap at the open, show clustered intraday stop runs around 5m/15m
# structures, and the bot executes option premium after analysing the
# underlying index.  Keep the BTC maps as the default, but switch to this
# profile automatically when the active instrument context is ICICI/NFO.
_INDIAN_INDEX_DECAY_LAMBDA: Dict[str, float] = {
    "1m": 0.85, "2m": 0.80, "3m": 0.90,
    "5m": 1.25, "15m": 2.00, "30m": 2.75,
    "1h": 3.75, "2h": 4.75, "4h": 6.25, "1d": 8.00,
}
_INDIAN_INDEX_TF_BASE_PROB: Dict[str, float] = {
    "1m": 0.42, "2m": 0.40, "3m": 0.42,
    "5m": 0.58, "15m": 0.68, "30m": 0.70,
    "1h": 0.74, "2h": 0.72, "4h": 0.64, "1d": 0.48,
}
_INDIAN_INDEX_MAX_REACH_ATR: Dict[str, float] = {
    "1m": 2.0, "2m": 2.0, "3m": 2.25,
    "5m": 3.75, "15m": 6.50, "30m": 8.00,
    "1h": 10.00, "2h": 12.00, "4h": 14.00, "1d": 18.00,
}
_INDIAN_SESSION_MULT: Dict[str, Dict[str, float]] = {
    # NSE options flows are two-sided intraday; do not import London/NY bias.
    "NSE_OPEN":  {"BSL": 1.08, "SSL": 1.08},
    "NSE":       {"BSL": 1.00, "SSL": 1.00},
    "NSE_CLOSE": {"BSL": 1.06, "SSL": 1.06},
}
_IST = timezone(timedelta(hours=5, minutes=30))


def _current_instrument_context():
    try:
        from core.instruments import current_instrument  # type: ignore
        return current_instrument()
    except Exception:
        return None


def _is_indian_index_option_context() -> bool:
    inst = _current_instrument_context()
    if inst is None:
        return False
    try:
        ex = str(getattr(getattr(inst, "primary_exchange", None), "value", getattr(inst, "primary_exchange", ""))).lower()
        ac = str(getattr(getattr(inst, "asset_class", None), "value", getattr(inst, "asset_class", ""))).lower()
        sym = str(getattr(inst, "symbol", "") or getattr(inst, "underlying", "")).upper()
    except Exception:
        return False
    index_like = any(x in sym for x in ("NIFTY", "BANKNIFTY", "NIF", "SENSEX", "MIDCAP", "CNXBAN"))
    return ex == "icici" or (index_like and ac in {"option", "index", "derivative"})


def _decay_lambda_for_tf(tf: str) -> float:
    maps = _INDIAN_INDEX_DECAY_LAMBDA if _is_indian_index_option_context() else _DECAY_LAMBDA
    t = str(tf)
    return float(maps.get(t, maps.get(t.lower(), _DECAY_LAMBDA.get(t, 2.0))))


def _tf_base_prob_for_tf(tf: str) -> float:
    maps = _INDIAN_INDEX_TF_BASE_PROB if _is_indian_index_option_context() else _TF_BASE_PROB
    t = str(tf)
    return float(maps.get(t, maps.get(t.lower(), _TF_BASE_PROB.get(t, 0.50))))


def _max_reach_atr_for_tf(tf: str) -> float:
    maps = _INDIAN_INDEX_MAX_REACH_ATR if _is_indian_index_option_context() else _MAX_REACH_ATR
    t = str(tf)
    return float(maps.get(t, maps.get(t.lower(), _MAX_REACH_ATR.get(t, 6.0))))


def _session_mult_for_key(session_key: str) -> Dict[str, float]:
    key = str(session_key or "").upper()
    if _is_indian_index_option_context() and key.startswith("NSE"):
        return _INDIAN_SESSION_MULT.get(key, _INDIAN_SESSION_MULT["NSE"])
    return _SESSION_MULT.get(key, _SESSION_MULT[""])


def _nse_session_from_now(now: Optional[float] = None) -> str:
    try:
        dt = datetime.fromtimestamp(float(now if now is not None else time.time()), tz=_IST)
        if dt.weekday() >= 5:
            return ""
        t = dt.time()
        if t < dt_time(9, 15) or t > dt_time(15, 30):
            return ""
        if t <= dt_time(10, 15):
            return "NSE_OPEN"
        if t >= dt_time(14, 30):
            return "NSE_CLOSE"
        return "NSE"
    except Exception:
        return "NSE"


# ─────────────────────────────────────────────────────────────────────────────
# DATA STRUCTURES
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class PoolProbability:
    """Sweep probability for a single liquidity pool."""
    price:          float
    side:           str        # "BSL" | "SSL"
    timeframe:      str
    distance_atr:   float
    raw_prob:       float      # before normalisation
    components: Dict[str, float] = field(default_factory=dict)
    # components keys: "distance", "tf_base", "significance", "recency", "touch"


@dataclass
class MTFPoolForecast:
    """Aggregate MTF pool sweep forecast."""
    bsl_prob:         float           # normalised BSL sweep probability [0, 1]
    ssl_prob:         float           # normalised SSL sweep probability [0, 1]
    net_score:        float           # bsl_prob - ssl_prob, in [-1, +1]
    confidence:       float           # certainty based on sample size + separation
    predicted_target: str             # "BSL" | "SSL" | "NEUTRAL"
    bsl_pools:        List[PoolProbability] = field(default_factory=list)
    ssl_pools:        List[PoolProbability] = field(default_factory=list)
    top_bsl:          Optional[PoolProbability] = None   # highest prob BSL pool
    top_ssl:          Optional[PoolProbability] = None   # highest prob SSL pool
    session:          str = ""
    reason:           str = ""


# ─────────────────────────────────────────────────────────────────────────────
# CORE PROBABILITY ENGINE
# ─────────────────────────────────────────────────────────────────────────────

class MTFPoolProbability:
    """
    Multi-timeframe pool sweep probability calculator.

    Usage:
        engine = MTFPoolProbability()
        forecast = engine.compute(liq_snapshot, price, atr, now, session_str)
        factor   = forecast.net_score   # [-1, +1] for direction_engine Factor 5
    """

    def compute(
        self,
        liq_snapshot,             # LiquidityMapSnapshot from liquidity_map.py
        price:    float,
        atr:      float,
        now:      float,          # epoch seconds
        session:  str   = "",     # "LONDON" | "NY" | "ASIA" | ""
        ict_engine      = None,   # optional — adds 2m pool detection
    ) -> MTFPoolForecast:
        """
        Compute MTF sweep probability forecast.

        Returns MTFPoolForecast with bsl_prob, ssl_prob, net_score, confidence.
        net_score > 0 → BSL more likely swept next (price going UP)
        net_score < 0 → SSL more likely swept next (price going DOWN)
        """
        if atr < 1e-10:
            return self._neutral("atr=0")

        a = max(atr, 1e-9)
        profile = build_market_profile(
            price=price,
            atr=atr,
            liq_snapshot=liq_snapshot,
            ict=ict_engine,
            session=session,
        )

        # Detect active session for multipliers
        sess_key = self._detect_session(session, ict_engine, now)

        # Collect all pool candidates
        bsl_candidates: List[PoolProbability] = []
        ssl_candidates: List[PoolProbability] = []

        # ── Source 1: LiquidityMap snapshot (primary — richest data) ─────────
        if liq_snapshot is not None:
            for pt in getattr(liq_snapshot, 'bsl_pools', []):
                pp = self._score_pool(pt.pool, price, a, now, "BSL", pt, profile)
                if pp is not None:
                    bsl_candidates.append(pp)
            for pt in getattr(liq_snapshot, 'ssl_pools', []):
                pp = self._score_pool(pt.pool, price, a, now, "SSL", pt, profile)
                if pp is not None:
                    ssl_candidates.append(pp)

        # ── Source 2: ICT engine 2m/5m pools (intraday micro-structure) ──────
        # These are short-lived — often sweeps before a real move.
        if ict_engine is not None:
            self._add_ict_micro_pools(ict_engine, price, a, now,
                                      bsl_candidates, ssl_candidates)

        # ── Apply session multipliers ─────────────────────────────────────────
        sm = _SESSION_MULT.get(sess_key, _SESSION_MULT[""])
        for pp in bsl_candidates:
            pp.raw_prob *= sm["BSL"]
        for pp in ssl_candidates:
            pp.raw_prob *= sm["SSL"]

        # ── Aggregate ─────────────────────────────────────────────────────────
        bsl_total = sum(pp.raw_prob for pp in bsl_candidates)
        ssl_total = sum(pp.raw_prob for pp in ssl_candidates)
        grand     = bsl_total + ssl_total

        if grand < 1e-10:
            return self._neutral("no_qualified_pools")

        bsl_norm = bsl_total / grand
        ssl_norm = ssl_total / grand
        net      = bsl_norm - ssl_norm   # [-1, +1]

        # ── Confidence: separation × sample size ─────────────────────────────
        n_pools  = len(bsl_candidates) + len(ssl_candidates)
        sep      = abs(net)
        size_scl = min(1.0, n_pools / max(_MIN_CONFIDENCE_POOLS, 1))
        conf     = sep * size_scl

        # Prediction
        if sep < 0.15:
            pred = "NEUTRAL"
        elif net > 0:
            pred = "BSL"
        else:
            pred = "SSL"

        bsl_candidates.sort(key=lambda p: p.raw_prob, reverse=True)
        ssl_candidates.sort(key=lambda p: p.raw_prob, reverse=True)

        reason_parts = []
        if bsl_candidates:
            top_b = bsl_candidates[0]
            reason_parts.append(
                f"TopBSL ${top_b.price:.0f}@{top_b.timeframe} "
                f"d={top_b.distance_atr:.1f}ATR p={top_b.raw_prob:.2f}")
        if ssl_candidates:
            top_s = ssl_candidates[0]
            reason_parts.append(
                f"TopSSL ${top_s.price:.0f}@{top_s.timeframe} "
                f"d={top_s.distance_atr:.1f}ATR p={top_s.raw_prob:.2f}")

        return MTFPoolForecast(
            bsl_prob         = round(bsl_norm, 4),
            ssl_prob         = round(ssl_norm, 4),
            net_score        = round(net, 4),
            confidence       = round(conf, 4),
            predicted_target = pred,
            bsl_pools        = bsl_candidates[:10],
            ssl_pools        = ssl_candidates[:10],
            top_bsl          = bsl_candidates[0] if bsl_candidates else None,
            top_ssl          = ssl_candidates[0] if ssl_candidates else None,
            session          = sess_key,
            reason           = " | ".join(reason_parts),
        )

    def _score_pool(
        self,
        pool,           # LiquidityPool
        price:  float,
        atr:    float,
        now:    float,
        side:   str,
        pt      = None,  # PoolTarget (has adjusted_sig())
        profile = None,
    ) -> Optional[PoolProbability]:
        """
        Score a single pool's sweep probability.

        Returns None if pool is out of effective range or already swept.
        """
        tf = getattr(pool, 'timeframe', '5m')
        pool_price = float(getattr(pool, 'price', 0.0))

        # Skip swept/consumed pools
        status = str(getattr(pool, 'status', '')).upper()
        if 'SWEPT' in status or 'CONSUMED' in status:
            return None

        # Distance in ATR
        dist_atr = abs(pool_price - price) / atr

        # Skip pools outside max reach for their TF
        max_reach = _max_reach_atr_for_tf(tf)
        if profile is not None:
            max_reach *= max(0.85, min(1.35, profile.breathing_mult))
        if dist_atr > max_reach:
            return None

        # ── Component 1: Distance decay ───────────────────────────────
        λ = _decay_lambda_for_tf(tf)
        if profile is not None:
            λ *= max(0.80, min(1.40, profile.breathing_mult))
        c_distance = math.exp(-dist_atr / λ)

        # ── Component 2: TF base probability ─────────────────────────
        c_tf = _tf_base_prob_for_tf(tf)

        # ── Component 3: Significance ─────────────────────────────────
        # Use PoolTarget.adjusted_sig() if available (richest data)
        if pt is not None and hasattr(pt, 'adjusted_sig'):
            sig = pt.adjusted_sig()
        else:
            sig = float(getattr(pool, 'significance', 1.0))
        c_sig = min(sig / _SIG_CAP, 1.0)

        # ── Component 4: Recency bonus ────────────────────────────────
        created_at = float(getattr(pool, 'created_at', 0.0))
        age_sec    = max(0.0, now - created_at) if created_at > 0 else 9999.0
        window     = _RECENCY_WINDOW_SEC.get(tf, 3600.0)
        c_recency  = 1.0 + _RECENCY_BONUS * max(0.0, 1.0 - age_sec / window)

        # ── Component 5: Touch momentum ───────────────────────────────
        touches   = int(getattr(pool, 'touches', 1))
        c_touch   = 1.0 + min(_TOUCH_BONUS_PER * max(0, touches - 1),
                               _TOUCH_BONUS_CAP)

        # ── Combine ───────────────────────────────────────────────────
        # Geometric mean of distance and tf_base × significance boost
        prob = (c_distance * c_tf) * (1.0 + c_sig * 0.6) * c_recency * c_touch

        # Direction consistency check (BSL must be above price, SSL below)
        if side == "BSL" and pool_price <= price:
            return None
        if side == "SSL" and pool_price >= price:
            return None

        return PoolProbability(
            price        = pool_price,
            side         = side,
            timeframe    = tf,
            distance_atr = round(dist_atr, 3),
            raw_prob     = round(prob, 5),
            components   = {
                "distance":     round(c_distance, 4),
                "tf_base":      c_tf,
                "significance": round(c_sig, 4),
                "recency":      round(c_recency, 4),
                "touch":        round(c_touch, 4),
            },
        )

    def _add_ict_micro_pools(
        self,
        ict_engine,
        price:  float,
        atr:    float,
        now:    float,
        bsl_out: List[PoolProbability],
        ssl_out: List[PoolProbability],
    ) -> None:
        """
        Extract 2m equal-highs/lows from ICT engine short-term structure.

        ICT engine doesn't store a "2m" registry, but _tf.get("5m") contains
        recent swing highs/lows from micro-structure. We treat these as 2m-level
        pools since they represent the most recent internal structure before the
        next 5m candle completes.

        Also reads ict_engine.liquidity_pools with timeframe in {"2m","5m"}.

        MTF-4 FIX: dedupe against pools already present in bsl_out/ssl_out
        (which were populated from liq_snapshot). Without dedup, the same
        physical pool appears twice (once from LiquidityMap, once from ICT),
        inflating grand total and mis-reporting confidence.
        """
        # MTF-4: precompute existing prices for O(1) proximity lookup
        _existing_bsl = [pp.price for pp in bsl_out]
        _existing_ssl = [pp.price for pp in ssl_out]
        _dedupe_tol_atr = 0.20      # treat within 0.2 ATR as same pool
        try:
            _all_pools = list(getattr(ict_engine, 'liquidity_pools', []))
            for p in _all_pools:
                tf = getattr(p, 'timeframe', '5m')
                if tf not in ('2m', '5m'):
                    continue
                if getattr(p, 'swept', False):
                    continue
                pool_price = float(getattr(p, 'price', 0.0))
                dist_atr   = abs(pool_price - price) / atr
                if dist_atr > _max_reach_atr_for_tf(tf):
                    continue

                λ        = _decay_lambda_for_tf(tf)
                c_dist   = math.exp(-dist_atr / λ)
                c_tf     = _tf_base_prob_for_tf(tf)
                touches  = int(getattr(p, 'touch_count', 1))
                c_touch  = 1.0 + min(_TOUCH_BONUS_PER * max(0, touches - 1),
                                      _TOUCH_BONUS_CAP)
                prob     = c_dist * c_tf * c_touch

                lt = str(getattr(p, 'level_type', '')).upper()
                # MTF-4: skip if a pool already exists within dedupe tolerance
                _dedupe_px = atr * _dedupe_tol_atr
                if 'BSL' in lt and pool_price > price:
                    if any(abs(ep - pool_price) < _dedupe_px for ep in _existing_bsl):
                        continue
                    bsl_out.append(PoolProbability(
                        price=pool_price, side="BSL", timeframe=tf,
                        distance_atr=round(dist_atr, 3),
                        raw_prob=round(prob, 5),
                        components={"distance": round(c_dist, 4),
                                    "tf_base": c_tf, "touch": round(c_touch, 4)},
                    ))
                    _existing_bsl.append(pool_price)
                elif 'SSL' in lt and pool_price < price:
                    if any(abs(ep - pool_price) < _dedupe_px for ep in _existing_ssl):
                        continue
                    ssl_out.append(PoolProbability(
                        price=pool_price, side="SSL", timeframe=tf,
                        distance_atr=round(dist_atr, 3),
                        raw_prob=round(prob, 5),
                        components={"distance": round(c_dist, 4),
                                    "tf_base": c_tf, "touch": round(c_touch, 4)},
                    ))
                    _existing_ssl.append(pool_price)
        except Exception as e:
            logger.debug(f"MTF micro-pool extraction non-fatal: {e}")

    @staticmethod
    def _detect_session(session_hint: str, ict_engine, now: Optional[float] = None) -> str:
        """
        Resolve session string from hint or ICT engine.

        MOD-6 FIX: Previously fell back only to ict._killzone, which is empty
        outside kill-zone hours even while the session is active. Now checks
        ict._session (canonical, full-window string) first so probability
        weights apply for the entire London/NY/Asia session, not just KZ windows.
        """
        if _is_indian_index_option_context():
            if session_hint:
                su = session_hint.upper()
                if "NSE" in su or "ICICI" in su or "NFO" in su:
                    sess = _nse_session_from_now(now)
                    return sess or "NSE"
            sess = _nse_session_from_now(now)
            if sess:
                return sess

        # Priority 1: session_hint from caller
        if session_hint:
            su = session_hint.upper()
            if 'LONDON' in su:
                return 'LONDON'
            if 'NY' in su or 'NEW YORK' in su or 'NEW_YORK' in su:
                return 'NY'
            if 'ASIA' in su:
                return 'ASIA'
        # Priority 2: canonical _session from ICT engine (full-window)
        if ict_engine is not None:
            _sess = str(getattr(ict_engine, '_session', '') or '').upper()
            if _sess in ('LONDON', 'LONDON_NY'):
                return 'LONDON'
            if _sess in ('NY', 'NEW_YORK'):
                return 'NY'
            if _sess == 'ASIA':
                return 'ASIA'
            # Priority 3: _killzone string (KZ-window only, last resort)
            kz = str(getattr(ict_engine, '_killzone', '') or '').upper()
            if 'LONDON' in kz:
                return 'LONDON'
            if 'NY' in kz:
                return 'NY'
            if 'ASIA' in kz:
                return 'ASIA'
        return ''

    @staticmethod
    def _neutral(reason: str) -> MTFPoolForecast:
        return MTFPoolForecast(
            bsl_prob=0.5, ssl_prob=0.5, net_score=0.0,
            confidence=0.0, predicted_target="NEUTRAL",
            reason=reason,
        )


# ─────────────────────────────────────────────────────────────────────────────
# DROP-IN REPLACEMENT FOR direction_engine.py FACTOR 5
# ─────────────────────────────────────────────────────────────────────────────

_mtf_engine = MTFPoolProbability()


def compute_mtf_pool_factor(
    liq_snapshot,
    price:    float,
    atr:      float,
    now:      float = 0.0,       # epoch seconds; falls back to time.time()
    ict_engine      = None,
    session:  str   = "",
) -> float:
    """
    Compute the MTF pool probability factor for direction_engine Factor 5.

    Returns float in [-1, +1]:
      +1.0 = very strong BSL sweep probability (price going UP)
      -1.0 = very strong SSL sweep probability (price going DOWN)
       0.0 = neutral / balanced

    USAGE IN direction_engine.py predict_hunt():
    ─────────────────────────────────────────────
    Replace the entire FACTOR 5 block with:

        from mtf_pool_probability import compute_mtf_pool_factor
        factors.pool_asymmetry = compute_mtf_pool_factor(
            liq_snapshot = liq_snapshot,
            price        = price,
            atr          = a,
            now          = now_ms / 1000.0,
            ict_engine   = ict_engine,
        )

    The weight for this factor remains 0.09 (unchanged in _FACTOR_WEIGHTS).
    But the QUALITY of the signal is dramatically improved because it now
    considers:
      • 2m pools (high noise, close range — often the Judas sweep target)
      • 15m pools (primary session manipulation targets)
      • 1h pools (institutional session high/low — strongest signal)
      • 4h pools (macro structural targets — high significance, low frequency)
      • 1d pools (daily levels — very rare intraday sweeps, weighted conservatively)
      • Distance decay per TF (close pools dominate over distant ones)
      • Touch count momentum (repeated tests = higher probability)
      • Session context (London BSL bias, NY reversal bias, Asia neutral)
    """
    now_ = now if now > 1e6 else time.time()
    forecast = _mtf_engine.compute(
        liq_snapshot = liq_snapshot,
        price        = price,
        atr          = atr,
        now          = now_,
        session      = session,
        ict_engine   = ict_engine,
    )
    return forecast.net_score   # already in [-1, +1]


def get_mtf_pool_forecast(
    liq_snapshot,
    price:    float,
    atr:      float,
    now:      float = 0.0,
    ict_engine      = None,
    session:  str   = "",
) -> MTFPoolForecast:
    """
    Full forecast for display / Telegram reporting.

    Exposes per-pool breakdown, top BSL/SSL candidates, and session context.
    Use in controller.py for the /thinking command.
    """
    now_ = now if now > 1e6 else time.time()
    return _mtf_engine.compute(
        liq_snapshot = liq_snapshot,
        price        = price,
        atr          = atr,
        now          = now_,
        session      = session,
        ict_engine   = ict_engine,
    )


# ─────────────────────────────────────────────────────────────────────────────
# CHANGES NEEDED IN direction_engine.py
# ─────────────────────────────────────────────────────────────────────────────
# 
# STEP 1: At the top of direction_engine.py, add:
#
#   try:
#       from mtf_pool_probability import compute_mtf_pool_factor
#       _MTF_PROB_AVAILABLE = True
#   except ImportError:
#       _MTF_PROB_AVAILABLE = False
#
# STEP 2: In DirectionEngine.predict_hunt(), find the FACTOR 5 block
#   (lines ~525–568) and replace it entirely with:
#
#   # ─────────────────────────────────────────────────────────────────────
#   # FACTOR 5: MTF Pool Sweep Probability  (weight 0.09)
#   # ─────────────────────────────────────────────────────────────────────
#   # Uses distance-decay × TF base probability × significance across
#   # 2m, 15m, 1h, 4h, 1d pools.  Closer pools dominate; 1h/4h carry
#   # highest institutional weight.  Session context applied per TF.
#   if _MTF_PROB_AVAILABLE:
#       factors.pool_asymmetry = compute_mtf_pool_factor(
#           liq_snapshot = liq_snapshot,
#           price        = price,
#           atr          = a,
#           now          = now_ms / 1000.0,
#           ict_engine   = ict_engine,
#       )
#   else:
#       # [keep existing fallback code here — lines 532–568]
#       ...
#
# STEP 3: No weight changes needed. The factor weight stays at 0.09.
#   The improvement is in SIGNAL QUALITY, not magnitude.
# ─────────────────────────────────────────────────────────────────────────────

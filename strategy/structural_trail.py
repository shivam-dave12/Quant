"""
structural_trail.py — Market-Structure Dynamic Trailing SL Engine v1.0
======================================================================
REPLACES: dynamic_trail_engine.py (AMD-phase-based) and the legacy
R-multiple ratchet system in quant_strategy.py.

PHILOSOPHY: The trailing SL should be placed and moved based on WHERE
the market's structure actually IS — not on theoretical phase models
or fixed R-multiple tables.

This engine uses ONLY market data and price structure:
  1. SWING STRUCTURE — Higher lows (long) / lower highs (short) define the trail
  2. REALIZED VOLATILITY — Trail distance adapts to actual volatility, not ATR multiples
  3. ORDERBOOK WALLS — Large liquidity walls provide structural defense
  4. VOLUME PROFILE — High Volume Nodes act as support/resistance anchors
  5. STRUCTURE BREAKS — BOS events trigger aggressive trailing
  6. MOMENTUM DECAY — Declining momentum → tighten trail preemptively

NO AMD phases. NO fixed R-multiple tables. NO phase-based freeze logic.
Every decision is derived from what the market is DOING right now.

PUBLIC API:
    new_sl = StructuralTrail.compute(
        pos_side, price, entry_price, current_sl, atr,
        initial_sl_dist, peak_profit, peak_price_abs,
        hold_seconds, candles_1m, candles_5m, orderbook,
        ict_engine=None, entry_vol=0, trade_mode="",
        tick_size=0.1
    )
    Returns: new_sl (float) or None (hold current SL)
"""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION — derived from market data, not hardcoded
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class TrailConfig:
    """Computed trail parameters from market conditions."""
    # Activation threshold (profit in R-multiples before trailing starts)
    activation_r: float = 0.40
    # Minimum distance from price (ATR-based, adapts to vol)
    min_distance_atr: float = 1.0
    # Maximum distance from price (cap to prevent SL from lagging too far)
    max_distance_atr: float = 3.0
    # Structure search radius (how far to look for anchors)
    structure_search_atr: float = 2.0
    # Tightening rate (how aggressively trail follows at high profit)
    tighten_factor: float = 1.0


@dataclass
class TrailCandidate:
    """A potential SL level with its structural justification."""
    price: float
    score: float         # 0-1 quality of this anchor
    source: str          # human-readable source label
    distance_atr: float  # distance from current price in ATR units


# ═══════════════════════════════════════════════════════════════════════════════
# INTERNAL: SWING STRUCTURE FINDER
# ═══════════════════════════════════════════════════════════════════════════════

def _find_swings(candles: list, lookback: int = 3
                 ) -> Tuple[List[float], List[float]]:
    """
    Find swing highs and swing lows from candle data.
    Returns (swing_highs, swing_lows) as price lists.
    """
    if len(candles) < 2 * lookback + 1:
        return [], []

    highs_list = []
    lows_list = []

    for i in range(lookback, len(candles) - lookback):
        h = _safe_f(candles[i], "h")
        l = _safe_f(candles[i], "l")

        is_swing_high = all(
            h >= _safe_f(candles[j], "h")
            for j in range(i - lookback, i + lookback + 1) if j != i
        )
        is_swing_low = all(
            l <= _safe_f(candles[j], "l")
            for j in range(i - lookback, i + lookback + 1) if j != i
        )

        if is_swing_high:
            highs_list.append((h, i))  # FIX-ST2: store index for recency
        if is_swing_low:
            lows_list.append((l, i))

    return highs_list, lows_list


def _safe_f(candle, key: str, default: float = 0.0) -> float:
    """Safely extract float from candle dict or object."""
    try:
        return float(candle[key])
    except (KeyError, TypeError, IndexError):
        try:
            return float(getattr(candle, key, default))
        except (TypeError, ValueError):
            return default


def _round_tick(price: float, tick: float = 0.1) -> float:
    """Round price to nearest tick."""
    if tick <= 0:
        return price
    return round(round(price / tick) * tick, 10)


# ═══════════════════════════════════════════════════════════════════════════════
# INTERNAL: REALIZED VOLATILITY ESTIMATOR
# ═══════════════════════════════════════════════════════════════════════════════

def _estimate_realized_vol(candles: list) -> float:
    """
    Estimate current realized volatility from recent candles.
    Returns average true range as a price value.
    Uses Parkinson estimator (high-low) which is more efficient than close-close.
    """
    if len(candles) < 5:
        return 0.0

    recent = candles[-20:] if len(candles) >= 20 else candles
    ranges = []
    for c in recent:
        h = _safe_f(c, "h")
        l = _safe_f(c, "l")
        if h > l > 0:
            ranges.append(h - l)

    if not ranges:
        return 0.0

    # Parkinson volatility: σ = sqrt(1/(4n·ln2) × Σ(ln(H/L))²)
    # Simplified: just use mean of high-low ranges (practical approximation)
    return sum(ranges) / len(ranges)


# ═══════════════════════════════════════════════════════════════════════════════
# INTERNAL: MOMENTUM DECAY DETECTOR
# ═══════════════════════════════════════════════════════════════════════════════

def _detect_momentum_decay(candles: list, pos_side: str) -> float:
    """
    Detect if directional momentum is fading.
    Returns 0.0 (strong momentum) to 1.0 (momentum exhausted).

    Uses three factors:
      1. Body-to-range ratio declining (doji formation)
      2. Volume declining while price extends
      3. Consecutive candles with shrinking bodies
    """
    if len(candles) < 8:
        return 0.0

    recent = candles[-6:]
    decay_score = 0.0

    # Factor 1: Body-to-range ratio declining
    body_ratios = []
    for c in recent:
        o = _safe_f(c, "o"); cl = _safe_f(c, "c")
        h = _safe_f(c, "h"); l = _safe_f(c, "l")
        rng = h - l
        body = abs(cl - o)
        body_ratios.append(body / rng if rng > 0 else 0.0)

    if len(body_ratios) >= 4:
        early_avg = sum(body_ratios[:3]) / 3
        late_avg = sum(body_ratios[-3:]) / 3
        if early_avg > 0.1:
            ratio_decline = 1.0 - (late_avg / early_avg)
            decay_score += max(0, min(ratio_decline, 1.0)) * 0.40

    # Factor 2: Volume declining
    vols = [_safe_f(c, "v") for c in recent]
    if len(vols) >= 4 and sum(vols[:3]) > 0:
        early_vol = sum(vols[:3]) / 3
        late_vol = sum(vols[-3:]) / 3
        if early_vol > 0:
            vol_decline = 1.0 - (late_vol / early_vol)
            decay_score += max(0, min(vol_decline, 1.0)) * 0.35

    # Factor 3: Candle direction opposing position
    opposing = 0
    for c in recent[-3:]:
        o = _safe_f(c, "o"); cl = _safe_f(c, "c")
        if pos_side == "long" and cl < o:
            opposing += 1
        elif pos_side == "short" and cl > o:
            opposing += 1
    decay_score += (opposing / 3.0) * 0.25

    return min(decay_score, 1.0)


# ═══════════════════════════════════════════════════════════════════════════════
# INTERNAL: VOLUME PROFILE SUPPORT/RESISTANCE
# ═══════════════════════════════════════════════════════════════════════════════

def _find_hvn_levels(candles: list, n_buckets: int = 40,
                     threshold_pctile: float = 0.70) -> List[Tuple[float, float]]:
    """
    Find High Volume Nodes — price levels where significant volume traded.
    These act as natural support/resistance because institutional positions
    are concentrated there.

    Returns: [(price, volume_fraction), ...] sorted by volume descending.
    """
    if len(candles) < 10:
        return []

    all_highs = [_safe_f(c, "h") for c in candles]
    all_lows = [_safe_f(c, "l") for c in candles]
    price_min = min(all_lows)
    price_max = max(all_highs)
    rng = price_max - price_min
    if rng < 1e-10:
        return []

    bucket_size = rng / n_buckets
    buckets = [0.0] * n_buckets

    for c in candles:
        h = _safe_f(c, "h"); l = _safe_f(c, "l"); v = _safe_f(c, "v")
        if h <= l or v <= 0:
            continue
        # Distribute volume across price range of candle
        lo_bucket = max(0, min(n_buckets - 1, int((l - price_min) / bucket_size)))  # FIX-ST3
        hi_bucket = min(n_buckets - 1, int((h - price_min) / bucket_size))
        n_span = hi_bucket - lo_bucket + 1
        vol_per_bucket = v / n_span if n_span > 0 else 0.0
        for b in range(lo_bucket, hi_bucket + 1):
            buckets[b] += vol_per_bucket

    total_vol = sum(buckets)
    if total_vol < 1e-10:
        return []

    # Find buckets above threshold
    sorted_vols = sorted(buckets)
    threshold = sorted_vols[int(len(sorted_vols) * threshold_pctile)]

    hvn_levels = []
    for i, vol in enumerate(buckets):
        if vol >= threshold:
            level_price = price_min + (i + 0.5) * bucket_size
            hvn_levels.append((level_price, vol / total_vol))

    hvn_levels.sort(key=lambda x: x[1], reverse=True)
    return hvn_levels[:10]  # top 10 HVN levels


# ═══════════════════════════════════════════════════════════════════════════════
# INTERNAL: ORDERBOOK WALL DETECTOR
# ═══════════════════════════════════════════════════════════════════════════════

def _find_ob_walls(orderbook: dict, price: float, atr: float,
                   side: str) -> List[Tuple[float, float]]:
    """
    Find significant orderbook walls that provide structural defense.
    Returns: [(wall_price, relative_size), ...] sorted by distance from price.

    For LONG positions: look for bid walls (buy defense) below price.
    For SHORT positions: look for ask walls (sell defense) above price.
    """
    if not orderbook or atr < 1e-10:
        return []

    if side == "long":
        levels = orderbook.get("bids", [])
    else:
        levels = orderbook.get("asks", [])

    if not levels:
        return []

    # Parse levels
    parsed = []
    for lvl in levels:
        if isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
            p, q = float(lvl[0]), float(lvl[1])
        elif isinstance(lvl, dict):
            p = float(lvl.get("limit_price", lvl.get("price", 0)))
            q = float(lvl.get("size", lvl.get("quantity", lvl.get("depth", 0))))
        else:
            continue
        if p > 0 and q > 0:
            parsed.append((p, q))

    if len(parsed) < 5:
        return []

    # Compute mean and std of quantities
    qtys = [q for _, q in parsed]
    mean_q = sum(qtys) / len(qtys)
    std_q = math.sqrt(sum((q - mean_q) ** 2 for q in qtys) / len(qtys))
    if std_q < 1e-10:
        return []

    # Find walls (>2σ above mean)
    wall_threshold = mean_q + 2.0 * std_q
    walls = []
    for p, q in parsed:
        if q >= wall_threshold:
            dist = abs(p - price) / atr
            if dist < 4.0:  # within 4 ATR
                walls.append((p, q / wall_threshold))  # relative size

    walls.sort(key=lambda x: abs(x[0] - price))
    return walls[:5]


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN ENGINE: STRUCTURAL TRAIL
# ═══════════════════════════════════════════════════════════════════════════════

class StructuralTrail:
    """
    Market-structure-based dynamic trailing SL.

    Every trail decision is grounded in observable market structure.
    No theoretical phases. No fixed tables. Pure market data.

    TRAIL HIERARCHY (checked in order, first valid candidate wins):
      1. Structure break detection → aggressive trail to last structure
      2. Swing structure (HL for long, LH for short) → natural trail anchor
      3. Orderbook wall defense → institutional intent
      4. Volume profile HVN → historical support/resistance
      5. Volatility-adaptive chandelier → pure vol-based fallback

    ACTIVATION: Profit must exceed activation_r × initial_sl_dist.
    Below this: HOLD current SL. The trade needs room to work.

    MIN DISTANCE: Scales with realized volatility, not fixed ATR multiples.
    High vol → wider minimum distance. Low vol → tighter.

    TIGHTENING: As profit grows, the search radius and min distance
    progressively shrink. At 3R+, the trail aggressively follows structure.
    """

    @staticmethod
    def compute(
        pos_side: str,
        price: float,
        entry_price: float,
        current_sl: float,
        atr: float,
        initial_sl_dist: float,
        peak_profit: float,
        peak_price_abs: float,
        hold_seconds: float,
        candles_1m: list,
        candles_5m: list,
        orderbook: dict,
        ict_engine=None,
        entry_vol: float = 0.0,
        trade_mode: str = "",
        tick_size: float = 0.1,
        # Optional context
        atr_percentile: float = 0.5,
        adx: float = 0.0,
        hold_reason: Optional[List[str]] = None,
        **kwargs,
    ) -> Optional[float]:
        """
        Compute new trailing SL based on market structure.

        Returns: new_sl (float) if trail should move, None if hold.
        Guarantees: new_sl is always an improvement (tighter for long, wider for short).
        """
        if hold_reason is None:
            hold_reason = []

        if atr < 1e-10 or price < 1.0 or entry_price < 1.0:
            hold_reason.append("invalid_inputs")
            return None

        init_dist = initial_sl_dist if initial_sl_dist > 1e-10 else atr
        profit = (price - entry_price) if pos_side == "long" else (entry_price - price)
        profit_r = profit / init_dist if init_dist > 0 else 0.0

        # ── Compute adaptive trail configuration ──
        config = StructuralTrail._compute_config(
            profit_r, atr, atr_percentile, adx, hold_seconds)

        # ── Check activation ──
        if profit_r < config.activation_r:
            hold_reason.append(
                f"below_activation({profit_r:.2f}R<{config.activation_r:.2f}R)")
            return None

        # ── Momentum decay detection ──
        momentum_decay = _detect_momentum_decay(candles_5m, pos_side)
        if momentum_decay > 0.70:
            # Momentum exhausting → tighten config
            config.min_distance_atr *= 0.75
            config.tighten_factor *= 1.4
            hold_reason.append(f"mom_decay={momentum_decay:.2f}→tighten")

        # ── Collect trail candidates ──
        candidates: List[TrailCandidate] = []

        # ── 1. ICT Structure break detection ──
        if ict_engine is not None:
            bos_candidate = StructuralTrail._check_structure_break(
                ict_engine, pos_side, price, entry_price, current_sl, atr)
            if bos_candidate is not None:
                candidates.append(bos_candidate)

        # ── 2. Swing structure ──
        swing_candidates = StructuralTrail._find_swing_anchors(
            candles_5m, candles_1m, pos_side, price, current_sl, atr, config)
        candidates.extend(swing_candidates)

        # ── 3. ICT Order Block anchors ──
        if ict_engine is not None:
            ob_candidates = StructuralTrail._find_ict_ob_anchors(
                ict_engine, pos_side, price, current_sl, atr, config)
            candidates.extend(ob_candidates)

        # ── 4. Orderbook wall defense ──
        wall_candidates = StructuralTrail._find_wall_anchors(
            orderbook, pos_side, price, current_sl, atr, config)
        candidates.extend(wall_candidates)

        # ── 5. Volume profile HVN ──
        hvn_candidates = StructuralTrail._find_hvn_anchors(
            candles_5m, pos_side, price, current_sl, atr, config)
        candidates.extend(hvn_candidates)

        # ── 6. Volatility chandelier fallback ──
        chandelier = StructuralTrail._chandelier_fallback(
            pos_side, price, peak_price_abs, atr, atr_percentile,
            profit_r, config)
        if chandelier is not None:
            candidates.append(chandelier)

        # ── Select best candidate ──
        if not candidates:
            hold_reason.append("no_candidates")
            return None

        # Filter: must be between current_sl and price
        valid = []
        for c in candidates:
            if pos_side == "long":
                if c.price > current_sl and c.price < price:
                    valid.append(c)
            else:
                if c.price < current_sl and c.price > price:
                    valid.append(c)

        if not valid:
            hold_reason.append("no_valid_candidates_after_filter")
            return None

        # Enforce minimum distance from price
        min_dist = config.min_distance_atr * atr
        final_valid = []
        for c in valid:
            dist = abs(price - c.price)
            if dist >= min_dist:
                final_valid.append(c)

        if not final_valid:
            # Cap at min distance
            if pos_side == "long":
                capped = price - min_dist
                if capped > current_sl:
                    hold_reason.append(f"min_dist_cap({min_dist:.0f}pts)")
                    return _round_tick(capped, tick_size)
            else:
                capped = price + min_dist
                if capped < current_sl:
                    hold_reason.append(f"min_dist_cap({min_dist:.0f}pts)")
                    return _round_tick(capped, tick_size)
            hold_reason.append("min_dist_blocks_all")
            return None

        # Score-weighted selection: highest score wins
        best = max(final_valid, key=lambda c: c.score)
        new_sl = _round_tick(best.price, tick_size)

        # Final improvement check
        is_improvement = ((pos_side == "long" and new_sl > current_sl) or
                          (pos_side == "short" and new_sl < current_sl))

        if not is_improvement:
            hold_reason.append("not_improvement")
            return None

        # Minimum improvement threshold (avoid churning API for 0.1 pt moves)
        min_improvement = max(0.08 * atr, tick_size * 2)
        actual_improvement = abs(new_sl - current_sl)
        if actual_improvement < min_improvement:
            hold_reason.append(
                f"below_min_improvement({actual_improvement:.1f}<{min_improvement:.1f})")
            return None

        hold_reason.append(f"TRAIL→{best.source} [{best.score:.2f}]")
        return new_sl

    # ── Config computation ────────────────────────────────────────────────────

    @staticmethod
    def _compute_config(profit_r: float, atr: float,
                        atr_percentile: float, adx: float,
                        hold_seconds: float) -> TrailConfig:
        """Compute trail parameters from market conditions."""
        cfg = TrailConfig()

        # ── Activation: lower in trending markets, higher in ranging ──
        if adx > 30:
            cfg.activation_r = 0.30  # trending — start trailing earlier
        elif adx < 18:
            cfg.activation_r = 0.50  # ranging — give more room
        else:
            cfg.activation_r = 0.40

        # ── Min distance: scales with realized vol percentile ──
        if atr_percentile > 0.85:
            cfg.min_distance_atr = 1.5   # extreme vol — wide buffer
        elif atr_percentile > 0.65:
            cfg.min_distance_atr = 1.2
        elif atr_percentile < 0.20:
            cfg.min_distance_atr = 0.7   # quiet market — tight is fine
        else:
            cfg.min_distance_atr = 1.0

        # ── Progressive tightening with profit ──
        if profit_r >= 3.0:
            cfg.min_distance_atr *= 0.60
            cfg.tighten_factor = 2.0
            cfg.structure_search_atr = 1.0
        elif profit_r >= 2.0:
            cfg.min_distance_atr *= 0.75
            cfg.tighten_factor = 1.5
            cfg.structure_search_atr = 1.5
        elif profit_r >= 1.0:
            cfg.min_distance_atr *= 0.90
            cfg.tighten_factor = 1.2
            cfg.structure_search_atr = 2.0
        else:
            cfg.tighten_factor = 1.0
            cfg.structure_search_atr = 2.5

        # ── Time decay: longer hold → slightly tighter (trade shouldn't last forever) ──
        if hold_seconds > 2400:  # >40 min
            cfg.min_distance_atr *= 0.85
            cfg.tighten_factor *= 1.1

        return cfg

    # ── Candidate finders ─────────────────────────────────────────────────────

    @staticmethod
    def _check_structure_break(ict_engine, pos_side: str, price: float,
                                entry_price: float, current_sl: float,
                                atr: float) -> Optional[TrailCandidate]:
        """
        Check for BOS (Break of Structure) that invalidates the trade direction.
        If structure breaks AGAINST the position, trail aggressively.
        """
        try:
            tf_5m = ict_engine._tf.get("5m")
            if tf_5m is None:
                return None

            bos_dir = getattr(tf_5m, "bos_direction", "")
            bos_level = getattr(tf_5m, "bos_level", 0.0)

            if not bos_dir or bos_level < 1.0:
                return None

            # Counter-BOS: structure broke against our position
            counter_bos = False
            if pos_side == "long" and bos_dir == "bearish" and bos_level < entry_price:
                counter_bos = True
            elif pos_side == "short" and bos_dir == "bullish" and bos_level > entry_price:
                counter_bos = True

            if counter_bos:
                # Aggressive trail: move SL to just behind the BOS level
                buffer = 0.3 * atr
                if pos_side == "long":
                    candidate_price = bos_level - buffer
                    if candidate_price > current_sl:
                        return TrailCandidate(
                            price=candidate_price,
                            score=0.95,  # highest priority
                            source=f"COUNTER_BOS_5m@{bos_level:.0f}",
                            distance_atr=abs(price - candidate_price) / atr,
                        )
                else:
                    candidate_price = bos_level + buffer
                    if candidate_price < current_sl:
                        return TrailCandidate(
                            price=candidate_price,
                            score=0.95,
                            source=f"COUNTER_BOS_5m@{bos_level:.0f}",
                            distance_atr=abs(price - candidate_price) / atr,
                        )
        except Exception:
            pass

        return None

    @staticmethod
    def _find_swing_anchors(candles_5m: list, candles_1m: list,
                            pos_side: str, price: float,
                            current_sl: float, atr: float,
                            config: TrailConfig) -> List[TrailCandidate]:
        """Find swing structure anchors (primary trail method)."""
        candidates = []
        buffer = 0.25 * atr
        search_dist = config.structure_search_atr * atr

        # 5m swings (stronger anchors)
        if len(candles_5m) >= 8:
            closed_5m = candles_5m[:-1]  # exclude forming candle
            highs_5m, lows_5m = _find_swings(closed_5m, lookback=2)

            if pos_side == "long" and lows_5m:
                # Trail long: anchor to swing lows (higher lows = trend intact)
                for sl_level in lows_5m:
                    candidate = sl_level - buffer
                    dist = price - candidate
                    if (candidate > current_sl and dist < search_dist
                            and dist > 0):
                        candidates.append(TrailCandidate(
                            price=candidate,
                            score=0.80,
                            source=f"5m_SL@{sl_level:.0f}",
                            distance_atr=dist / atr,
                        ))

            elif pos_side == "short" and highs_5m:
                for sh_level in highs_5m:
                    candidate = sh_level + buffer
                    dist = candidate - price
                    if (candidate < current_sl and dist < search_dist
                            and dist > 0):
                        candidates.append(TrailCandidate(
                            price=candidate,
                            score=0.80,
                            source=f"5m_SH@{sh_level:.0f}",
                            distance_atr=dist / atr,
                        ))

        # 1m swings (tighter anchors for aggressive trailing at high R)
        if len(candles_1m) >= 10 and config.tighten_factor >= 1.3:
            closed_1m = candles_1m[:-1]
            highs_1m, lows_1m = _find_swings(closed_1m, lookback=3)

            if pos_side == "long" and lows_1m:
                for sl_level in lows_1m[-5:]:  # most recent 5 swings only
                    candidate = sl_level - buffer * 0.7
                    dist = price - candidate
                    if (candidate > current_sl and
                            dist < search_dist and dist > 0):
                        candidates.append(TrailCandidate(
                            price=candidate,
                            score=0.65,
                            source=f"1m_SL@{sl_level:.0f}",
                            distance_atr=dist / atr,
                        ))

            elif pos_side == "short" and highs_1m:
                for sh_level in highs_1m[-5:]:
                    candidate = sh_level + buffer * 0.7
                    dist = candidate - price
                    if (candidate < current_sl and
                            dist < search_dist and dist > 0):
                        candidates.append(TrailCandidate(
                            price=candidate,
                            score=0.65,
                            source=f"1m_SH@{sh_level:.0f}",
                            distance_atr=dist / atr,
                        ))

        return candidates

    @staticmethod
    def _find_ict_ob_anchors(ict_engine, pos_side: str, price: float,
                              current_sl: float, atr: float,
                              config: TrailConfig) -> List[TrailCandidate]:
        """Find ICT Order Block anchors for trailing."""
        candidates = []
        try:
            now_ms = int(time.time() * 1000)
            obs = (ict_engine.order_blocks_bull if pos_side == "long"
                   else ict_engine.order_blocks_bear)

            buffer = 0.3 * atr
            for ob in obs:
                if not ob.is_active(now_ms):
                    continue

                if pos_side == "long":
                    candidate = ob.low - buffer
                    dist = price - candidate
                    if (candidate > current_sl and
                            dist < config.structure_search_atr * atr and
                            dist > 0):
                        # Score by OB strength
                        strength_score = min(ob.strength / 100.0, 1.0)
                        candidates.append(TrailCandidate(
                            price=candidate,
                            score=0.75 * strength_score,
                            source=f"OB_{ob.timeframe}@{ob.midpoint:.0f}",
                            distance_atr=dist / atr,
                        ))
                else:
                    candidate = ob.high + buffer
                    dist = candidate - price
                    if (candidate < current_sl and
                            dist < config.structure_search_atr * atr and
                            dist > 0):
                        strength_score = min(ob.strength / 100.0, 1.0)
                        candidates.append(TrailCandidate(
                            price=candidate,
                            score=0.75 * strength_score,
                            source=f"OB_{ob.timeframe}@{ob.midpoint:.0f}",
                            distance_atr=dist / atr,
                        ))
        except Exception:
            pass

        return candidates

    @staticmethod
    def _find_wall_anchors(orderbook: dict, pos_side: str, price: float,
                           current_sl: float, atr: float,
                           config: TrailConfig) -> List[TrailCandidate]:
        """Find orderbook wall anchors."""
        candidates = []
        walls = _find_ob_walls(orderbook, price, atr, pos_side)

        for wall_price, relative_size in walls:
            buffer = 0.2 * atr
            if pos_side == "long":
                candidate = wall_price - buffer
                if candidate > current_sl and candidate < price:
                    candidates.append(TrailCandidate(
                        price=candidate,
                        score=0.55 * min(relative_size, 1.5),
                        source=f"WALL@{wall_price:.0f}({relative_size:.1f}x)",
                        distance_atr=abs(price - candidate) / atr,
                    ))
            else:
                candidate = wall_price + buffer
                if candidate < current_sl and candidate > price:
                    candidates.append(TrailCandidate(
                        price=candidate,
                        score=0.55 * min(relative_size, 1.5),
                        source=f"WALL@{wall_price:.0f}({relative_size:.1f}x)",
                        distance_atr=abs(price - candidate) / atr,
                    ))

        return candidates

    @staticmethod
    def _find_hvn_anchors(candles_5m: list, pos_side: str, price: float,
                          current_sl: float, atr: float,
                          config: TrailConfig) -> List[TrailCandidate]:
        """Find Volume Profile HVN anchors."""
        candidates = []
        hvn_levels = _find_hvn_levels(candles_5m, n_buckets=40, threshold_pctile=0.70)

        buffer = 0.25 * atr
        for hvn_price, vol_frac in hvn_levels:
            if pos_side == "long":
                candidate = hvn_price - buffer
                if (candidate > current_sl and candidate < price
                        and abs(price - candidate) < config.structure_search_atr * atr):
                    candidates.append(TrailCandidate(
                        price=candidate,
                        score=0.50 * min(vol_frac * 20, 1.0),
                        source=f"HVN@{hvn_price:.0f}({vol_frac:.2%}vol)",
                        distance_atr=abs(price - candidate) / atr,
                    ))
            else:
                candidate = hvn_price + buffer
                if (candidate < current_sl and candidate > price
                        and abs(price - candidate) < config.structure_search_atr * atr):
                    candidates.append(TrailCandidate(
                        price=candidate,
                        score=0.50 * min(vol_frac * 20, 1.0),
                        source=f"HVN@{hvn_price:.0f}({vol_frac:.2%}vol)",
                        distance_atr=abs(price - candidate) / atr,
                    ))

        return candidates

    @staticmethod
    def _chandelier_fallback(pos_side: str, price: float,
                              peak_price_abs: float, atr: float,
                              atr_percentile: float, profit_r: float,
                              config: TrailConfig) -> Optional[TrailCandidate]:
        """
        Volatility-adaptive chandelier stop as fallback.
        Chandelier = peak_price - N × ATR, where N adapts to conditions.
        """
        # FIX-ST1: use entry_price as fallback if peak never set
        if peak_price_abs < 1.0:
            peak_price_abs = price  # fallback to current price
        if peak_price_abs < 1.0:
            return None

        # N starts at 2.5 and tightens with profit
        base_n = 2.5
        if profit_r >= 3.0:
            n = 1.2
        elif profit_r >= 2.0:
            n = 1.5
        elif profit_r >= 1.0:
            n = 2.0
        else:
            n = base_n

        # Vol adjustment
        if atr_percentile > 0.85:
            n *= 1.3  # wider in extreme vol
        elif atr_percentile < 0.20:
            n *= 0.8  # tighter in quiet markets

        if pos_side == "long":
            candidate = peak_price_abs - n * atr
        else:
            candidate = peak_price_abs + n * atr

        if candidate < 1.0:
            return None

        return TrailCandidate(
            price=candidate,
            score=0.40,  # lowest priority — pure vol fallback
            source=f"CHANDELIER(n={n:.1f})",
            distance_atr=abs(price - candidate) / atr,
        )

"""
ict_engine.py — Standalone ICT/SMC Structure Detection Engine
==============================================================
Extracted from AdvancedICTStrategy v11 for integration with Quant Bot.

Provides:
  - Order Block detection (bullish/bearish with OTE scoring)
  - Fair Value Gap detection (with fill tracking)
  - Liquidity Pool detection (equal highs/lows)
  - Liquidity Sweep detection (stop hunts)
  - Session / Killzone awareness (London, NY, Asia)
  - Unified ICT confluence scoring

Called by quant_strategy.py as an additional signal layer.
The ICT score adds structural context to the quant bot's order-flow signals.

PRINCIPLE: Order flow tells you WHO is buying/selling right now.
           ICT structure tells you WHERE smart money placed its orders.
           Combined: enter WHERE smart money is positioned + WHEN flow confirms.
"""

import logging
import time
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple, NamedTuple
from collections import deque
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ══════════════════════════════════════════════════════════════════════

@dataclass
class OrderBlock:
    """ICT Order Block — last opposite candle before a strong impulse."""
    low: float
    high: float
    timestamp: int           # ms
    direction: str           # "bullish" or "bearish"
    strength: float = 50.0   # 0-100 score
    visit_count: int = 0
    bos_confirmed: bool = False
    has_displacement: bool = False
    has_wick_rejection: bool = False
    max_age_ms: int = 86_400_000  # 24h default
    _last_visit_time: int = 0

    @property
    def midpoint(self) -> float:
        return (self.low + self.high) / 2.0

    @property
    def size(self) -> float:
        return self.high - self.low

    def contains_price(self, price: float) -> bool:
        return self.low <= price <= self.high

    def in_optimal_zone(self, price: float) -> bool:
        """OTE = 50%-79% retracement into OB zone (ICT optimal entry)."""
        if self.size < 1e-10:
            return False
        if self.direction == "bullish":
            # Bullish OB: OTE is the lower portion (discount within the OB)
            ote_top = self.high - 0.50 * self.size
            ote_bot = self.high - 0.79 * self.size
            return ote_bot <= price <= ote_top
        else:
            # Bearish OB: OTE is the upper portion (premium within the OB)
            ote_bot = self.low + 0.50 * self.size
            ote_top = self.low + 0.79 * self.size
            return ote_bot <= price <= ote_top

    def is_active(self, now_ms: int) -> bool:
        if now_ms - self.timestamp > self.max_age_ms:
            return False
        return self.visit_count < 3  # invalidate after 3 revisits

    def virgin_multiplier(self) -> float:
        """Virgin OB (0 visits) = full weight. Visited = decaying weight."""
        if self.visit_count == 0:
            return 1.0
        elif self.visit_count == 1:
            return 0.70
        else:
            return 0.40


@dataclass
class FairValueGap:
    """ICT Fair Value Gap — imbalance between candle 1 and candle 3."""
    bottom: float
    top: float
    timestamp: int
    direction: str            # "bullish" or "bearish"
    fill_percentage: float = 0.0
    filled: bool = False
    max_age_ms: int = 86_400_000  # 24h

    @property
    def midpoint(self) -> float:
        return (self.bottom + self.top) / 2.0

    @property
    def size(self) -> float:
        return self.top - self.bottom

    def is_price_in_gap(self, price: float) -> bool:
        return self.bottom <= price <= self.top

    def update_fill(self, candles: List[Dict]) -> None:
        """Track how much of the FVG has been filled by subsequent candles."""
        if self.filled:
            return
        gap_size = self.size
        if gap_size < 1e-10:
            self.filled = True
            return
        for c in candles:
            ts = int(c.get('t', 0))
            if ts <= self.timestamp:
                continue
            h, l = float(c['h']), float(c['l'])
            if self.direction == "bullish":
                # Bullish FVG fills from bottom up
                if l <= self.bottom:
                    self.fill_percentage = 1.0
                    self.filled = True
                    return
                elif l < self.top:
                    fill = (self.top - l) / gap_size
                    self.fill_percentage = max(self.fill_percentage, fill)
            else:
                # Bearish FVG fills from top down
                if h >= self.top:
                    self.fill_percentage = 1.0
                    self.filled = True
                    return
                elif h > self.bottom:
                    fill = (h - self.bottom) / gap_size
                    self.fill_percentage = max(self.fill_percentage, fill)

    def is_active(self, now_ms: int) -> bool:
        if now_ms - self.timestamp > self.max_age_ms:
            return False
        return not self.filled


@dataclass
class LiquidityPool:
    """ICT Liquidity Pool — cluster of equal highs/lows where stops rest."""
    price: float
    pool_type: str           # "EQH" (equal highs) or "EQL" (equal lows)
    touch_count: int = 2
    swept: bool = False
    sweep_timestamp: int = 0
    wick_rejection: bool = False
    displacement_confirmed: bool = False


class ICTConfluence(NamedTuple):
    """Result of ICT confluence analysis for a given side + price."""
    ob_score: float          # 0-1: price in OB (bonus for OTE, virgin, BOS)
    fvg_score: float         # 0-1: price in FVG (bonus for freshness)
    sweep_score: float       # 0-1: recent liquidity sweep in direction
    session_score: float     # 0-1: killzone timing bonus
    total: float             # weighted combination
    active_ob: Optional[OrderBlock] = None
    active_fvg: Optional[FairValueGap] = None
    session_name: str = ""
    killzone: str = ""
    details: str = ""


# ══════════════════════════════════════════════════════════════════════
# ICT ENGINE
# ══════════════════════════════════════════════════════════════════════

class ICTEngine:
    """
    Standalone ICT/SMC structure detection engine.

    Call flow:
      1. update(candles_5m, candles_15m, price, now_ms) — detect structures
      2. get_confluence(side, price, now_ms) → ICTConfluence — score for entry
      3. get_ob_sl_level(side, price, atr) → float — OB-aware SL level
    """

    # ── Config defaults (overrideable via config module) ──────────────
    OB_MIN_IMPULSE_PCT = 0.50
    OB_MIN_BODY_RATIO = 0.50
    OB_IMPULSE_SIZE_MULT = 1.30
    OB_MAX_AGE_MS = 86_400_000
    FVG_MIN_SIZE_PCT = 0.020
    FVG_MAX_AGE_MS = 86_400_000
    LIQ_TOUCH_TOL_PCT = 0.0020
    SWEEP_DISP_MIN = 0.40
    SWEEP_MAX_AGE_MS = 7_200_000  # 2h

    # Killzone times (New York hours)
    KZ_ASIA_START = 20; KZ_ASIA_END = 24
    KZ_LONDON_START = 2; KZ_LONDON_END = 5
    KZ_NY_START = 7; KZ_NY_END = 10

    def __init__(self):
        self.order_blocks_bull: deque = deque(maxlen=20)
        self.order_blocks_bear: deque = deque(maxlen=20)
        self.fvgs_bull: deque = deque(maxlen=30)
        self.fvgs_bear: deque = deque(maxlen=30)
        self.liquidity_pools: deque = deque(maxlen=30)
        self._registered_sweeps: deque = deque(maxlen=100)
        self._swing_highs: List[float] = []
        self._swing_lows: List[float] = []
        self._last_update = 0.0
        self._UPDATE_INTERVAL = 5.0  # 5 seconds
        self._session = ""
        self._killzone = ""
        self._initialized = False

        # Try to load config overrides
        self._load_config()

    def _load_config(self):
        """Load ICT params from config module if available."""
        try:
            import config as cfg
            self.OB_MIN_IMPULSE_PCT = getattr(cfg, 'OB_MIN_IMPULSE_PCT', self.OB_MIN_IMPULSE_PCT)
            self.OB_MIN_BODY_RATIO = getattr(cfg, 'OB_MIN_BODY_RATIO', self.OB_MIN_BODY_RATIO)
            self.OB_IMPULSE_SIZE_MULT = getattr(cfg, 'OB_IMPULSE_SIZE_MULTIPLIER', self.OB_IMPULSE_SIZE_MULT)
            self.OB_MAX_AGE_MS = getattr(cfg, 'OB_MAX_AGE_MINUTES', 1440) * 60_000
            self.FVG_MIN_SIZE_PCT = getattr(cfg, 'FVG_MIN_SIZE_PCT', self.FVG_MIN_SIZE_PCT)
            self.FVG_MAX_AGE_MS = getattr(cfg, 'FVG_MAX_AGE_MINUTES', 1440) * 60_000
            self.SWEEP_MAX_AGE_MS = getattr(cfg, 'SWEEP_MAX_AGE_MINUTES', 120) * 60_000
            self.SWEEP_DISP_MIN = getattr(cfg, 'SWEEP_DISPLACEMENT_MIN', self.SWEEP_DISP_MIN)
            self.LIQ_TOUCH_TOL_PCT = getattr(cfg, 'LIQ_TOUCH_TOLERANCE_PCT', 0.20) / 100.0
            self.KZ_ASIA_START = getattr(cfg, 'KZ_ASIA_NY_START', 20)
            self.KZ_LONDON_START = getattr(cfg, 'KZ_LONDON_NY_START', 2)
            self.KZ_LONDON_END = getattr(cfg, 'KZ_LONDON_NY_END', 5)
            self.KZ_NY_START = getattr(cfg, 'KZ_NY_NY_START', 7)
            self.KZ_NY_END = getattr(cfg, 'KZ_NY_NY_END', 10)
        except ImportError:
            pass

    def reset_state(self):
        """Clear all detected structures (called after stream restart)."""
        self.order_blocks_bull.clear()
        self.order_blocks_bear.clear()
        self.fvgs_bull.clear()
        self.fvgs_bear.clear()
        self.liquidity_pools.clear()
        self._registered_sweeps.clear()
        self._swing_highs.clear()
        self._swing_lows.clear()
        self._initialized = False

    # ══════════════════════════════════════════════════════════════════
    # MAIN UPDATE — call from quant_strategy._evaluate_entry
    # ══════════════════════════════════════════════════════════════════

    def update(self, candles_5m: List[Dict], candles_15m: List[Dict],
               price: float, now_ms: int) -> None:
        """Update all ICT structures. Throttled to every 5 seconds."""
        now_s = now_ms / 1000.0
        if now_s - self._last_update < self._UPDATE_INTERVAL:
            return
        self._last_update = now_s

        if len(candles_5m) < 10:
            return

        self._update_session(now_ms)
        self._detect_swing_points(candles_5m, candles_15m)
        self._detect_order_blocks(candles_5m, price, now_ms)
        self._detect_fvgs(candles_5m, price, now_ms)
        self._update_fvg_fills(candles_5m)
        self._detect_liquidity_pools(price, now_ms)
        self._detect_liquidity_sweeps(candles_5m, candles_15m, price, now_ms)
        self._update_ob_visits(price, now_ms)
        self._initialized = True

    # ══════════════════════════════════════════════════════════════════
    # SWING POINT DETECTION
    # ══════════════════════════════════════════════════════════════════

    def _detect_swing_points(self, candles_5m: List[Dict],
                              candles_15m: List[Dict]) -> None:
        """Detect fractal swing highs/lows for OB reference and liquidity."""
        self._swing_highs.clear()
        self._swing_lows.clear()

        for candles in [candles_5m, candles_15m]:
            if len(candles) < 7:
                continue
            lb_left = 5
            lb_right = 3
            for i in range(lb_left, len(candles) - lb_right):
                h = float(candles[i]['h'])
                l = float(candles[i]['l'])
                # Swing high
                if all(h >= float(candles[j]['h']) for j in range(i - lb_left, i)) and \
                   all(h >= float(candles[j]['h']) for j in range(i + 1, i + lb_right + 1)):
                    self._swing_highs.append(h)
                # Swing low
                if all(l <= float(candles[j]['l']) for j in range(i - lb_left, i)) and \
                   all(l <= float(candles[j]['l']) for j in range(i + 1, i + lb_right + 1)):
                    self._swing_lows.append(l)

    # ══════════════════════════════════════════════════════════════════
    # ORDER BLOCK DETECTION
    # ══════════════════════════════════════════════════════════════════

    def _detect_order_blocks(self, candles: List[Dict], price: float,
                              now_ms: int) -> None:
        """OB = last opposite candle before a strong impulse move."""
        if len(candles) < 5:
            return

        tol = price * 0.001
        prior_highs = sorted(self._swing_highs, reverse=True)[:5]
        prior_lows = sorted(self._swing_lows)[:5]

        for i in range(2, len(candles) - 1):
            cur = candles[i]
            nxt = candles[i + 1]

            cur_o, cur_c = float(cur['o']), float(cur['c'])
            cur_h, cur_l = float(cur['h']), float(cur['l'])
            nxt_o, nxt_c = float(nxt['o']), float(nxt['c'])
            nxt_h, nxt_l = float(nxt['h']), float(nxt['l'])
            cur_ts = int(cur.get('t', now_ms))

            nxt_range = nxt_h - nxt_l
            nxt_body = abs(nxt_c - nxt_o)

            impulse_up = (nxt_c > nxt_o and
                         (nxt_c - nxt_o) / max(nxt_o, 1) * 100 >= self.OB_MIN_IMPULSE_PCT and
                         nxt_body / max(nxt_range, 1e-9) >= self.OB_MIN_BODY_RATIO)
            impulse_down = (nxt_c < nxt_o and
                           (nxt_o - nxt_c) / max(nxt_o, 1) * 100 >= self.OB_MIN_IMPULSE_PCT and
                           nxt_body / max(nxt_range, 1e-9) >= self.OB_MIN_BODY_RATIO)

            # Bullish OB: bearish candle before bullish impulse
            if impulse_up and cur_c < cur_o:
                bos_ok = any(nxt_h > ph for ph in prior_highs[:3]) if prior_highs else False
                has_disp = nxt_range > 0 and nxt_body / nxt_range >= self.SWEEP_DISP_MIN
                body_low = min(cur_o, cur_c)
                wick = body_low - cur_l if body_low > cur_l else 0.0
                wick_rej = (cur_h - cur_l) > 0 and wick / (cur_h - cur_l) >= 0.20

                strength = 40.0
                if bos_ok: strength += 20.0
                if has_disp: strength += 15.0
                if wick_rej: strength += 10.0
                if nxt_range >= self.OB_IMPULSE_SIZE_MULT * (cur_h - cur_l):
                    strength += 15.0
                strength = min(strength, 100.0)

                if not any(abs(ob.low - cur_l) <= tol and abs(ob.high - cur_h) <= tol
                           for ob in self.order_blocks_bull):
                    self.order_blocks_bull.append(OrderBlock(
                        low=cur_l, high=cur_h, timestamp=cur_ts,
                        direction="bullish", strength=strength,
                        bos_confirmed=bos_ok, has_displacement=has_disp,
                        has_wick_rejection=wick_rej, max_age_ms=self.OB_MAX_AGE_MS))

            # Bearish OB: bullish candle before bearish impulse
            if impulse_down and cur_c > cur_o:
                bos_ok = any(nxt_l < pl for pl in prior_lows[:3]) if prior_lows else False
                has_disp = nxt_range > 0 and nxt_body / nxt_range >= self.SWEEP_DISP_MIN
                body_top = max(cur_o, cur_c)
                wick = cur_h - body_top if cur_h > body_top else 0.0
                wick_rej = (cur_h - cur_l) > 0 and wick / (cur_h - cur_l) >= 0.20

                strength = 40.0
                if bos_ok: strength += 20.0
                if has_disp: strength += 15.0
                if wick_rej: strength += 10.0
                if nxt_range >= self.OB_IMPULSE_SIZE_MULT * (cur_h - cur_l):
                    strength += 15.0
                strength = min(strength, 100.0)

                if not any(abs(ob.low - cur_l) <= tol and abs(ob.high - cur_h) <= tol
                           for ob in self.order_blocks_bear):
                    self.order_blocks_bear.append(OrderBlock(
                        low=cur_l, high=cur_h, timestamp=cur_ts,
                        direction="bearish", strength=strength,
                        bos_confirmed=bos_ok, has_displacement=has_disp,
                        has_wick_rejection=wick_rej, max_age_ms=self.OB_MAX_AGE_MS))

    # ══════════════════════════════════════════════════════════════════
    # FVG DETECTION
    # ══════════════════════════════════════════════════════════════════

    def _detect_fvgs(self, candles: List[Dict], price: float,
                      now_ms: int) -> None:
        """FVG = gap between candle 1's extreme and candle 3's extreme."""
        if len(candles) < 3:
            return
        min_gap = price * self.FVG_MIN_SIZE_PCT / 100.0
        tol = price * 0.0005

        for i in range(len(candles) - 2):
            c1, c2, c3 = candles[i], candles[i + 1], candles[i + 2]
            c1_h, c1_l = float(c1['h']), float(c1['l'])
            c3_h, c3_l = float(c3['h']), float(c3['l'])
            ts = int(c2.get('t', now_ms))

            # Bullish FVG: gap between c1 high and c3 low
            gap_bot, gap_top = c1_h, c3_l
            if gap_top > gap_bot and (gap_top - gap_bot) >= min_gap:
                if not any(abs(f.bottom - gap_bot) <= tol and abs(f.top - gap_top) <= tol
                           for f in self.fvgs_bull):
                    self.fvgs_bull.append(FairValueGap(
                        bottom=gap_bot, top=gap_top, timestamp=ts,
                        direction="bullish", max_age_ms=self.FVG_MAX_AGE_MS))

            # Bearish FVG: gap between c3 high and c1 low
            gap_bot, gap_top = c3_h, c1_l
            if gap_top > gap_bot and (gap_top - gap_bot) >= min_gap:
                if not any(abs(f.bottom - gap_bot) <= tol and abs(f.top - gap_top) <= tol
                           for f in self.fvgs_bear):
                    self.fvgs_bear.append(FairValueGap(
                        bottom=gap_bot, top=gap_top, timestamp=ts,
                        direction="bearish", max_age_ms=self.FVG_MAX_AGE_MS))

    def _update_fvg_fills(self, candles: List[Dict]) -> None:
        for fvg in list(self.fvgs_bull) + list(self.fvgs_bear):
            if not fvg.filled:
                fvg.update_fill(candles)

    # ══════════════════════════════════════════════════════════════════
    # LIQUIDITY POOL DETECTION
    # ══════════════════════════════════════════════════════════════════

    def _detect_liquidity_pools(self, price: float, now_ms: int) -> None:
        """Detect equal highs (EQH) and equal lows (EQL) from swing points."""
        tol = price * self.LIQ_TOUCH_TOL_PCT
        self._cluster_levels(self._swing_highs, "EQH", tol, price, now_ms)
        self._cluster_levels(self._swing_lows, "EQL", tol, price, now_ms)

    def _cluster_levels(self, prices: List[float], pool_type: str,
                         tol: float, current_price: float, now_ms: int) -> None:
        if len(prices) < 2:
            return
        sorted_p = sorted(prices)
        used = set()
        for i, p1 in enumerate(sorted_p):
            if i in used:
                continue
            cluster = [p1]
            for j in range(i + 1, len(sorted_p)):
                if j in used:
                    continue
                if abs(sorted_p[j] - p1) <= tol:
                    cluster.append(sorted_p[j])
                    used.add(j)
            if len(cluster) >= 2:
                avg_price = sum(cluster) / len(cluster)
                # Don't duplicate existing pools
                if not any(abs(lp.price - avg_price) <= tol and lp.pool_type == pool_type
                           for lp in self.liquidity_pools):
                    self.liquidity_pools.append(LiquidityPool(
                        price=avg_price, pool_type=pool_type,
                        touch_count=len(cluster)))

    # ══════════════════════════════════════════════════════════════════
    # LIQUIDITY SWEEP DETECTION
    # ══════════════════════════════════════════════════════════════════

    def _detect_liquidity_sweeps(self, candles_5m: List[Dict],
                                  candles_15m: List[Dict],
                                  price: float, now_ms: int) -> None:
        """Detect when price sweeps through a liquidity pool then reverses."""
        recent = candles_5m[-20:] + candles_15m[-10:]

        for pool in list(self.liquidity_pools):
            if pool.swept:
                continue
            for c in recent:
                h, l = float(c['h']), float(c['l'])
                cl, op = float(c['c']), float(c['o'])
                body = abs(cl - op)
                rng = h - l
                dedup = (round(pool.price, 0), int(c.get('t', 0)))
                if dedup in self._registered_sweeps:
                    continue

                if pool.pool_type == "EQH" and h > pool.price:
                    wick_ok = cl < pool.price
                    disp_ok = rng > 0 and (body / rng) >= self.SWEEP_DISP_MIN
                    if wick_ok:
                        pool.swept = True
                        pool.sweep_timestamp = now_ms
                        pool.wick_rejection = True
                        pool.displacement_confirmed = disp_ok
                        self._registered_sweeps.append(dedup)
                        logger.info(f"💧 ICT: EQH swept @ ${pool.price:.0f} disp={disp_ok}")
                        break

                elif pool.pool_type == "EQL" and l < pool.price:
                    wick_ok = cl > pool.price
                    disp_ok = rng > 0 and (body / rng) >= self.SWEEP_DISP_MIN
                    if wick_ok:
                        pool.swept = True
                        pool.sweep_timestamp = now_ms
                        pool.wick_rejection = True
                        pool.displacement_confirmed = disp_ok
                        self._registered_sweeps.append(dedup)
                        logger.info(f"💧 ICT: EQL swept @ ${pool.price:.0f} disp={disp_ok}")
                        break

    # ══════════════════════════════════════════════════════════════════
    # OB VISIT TRACKING
    # ══════════════════════════════════════════════════════════════════

    def _update_ob_visits(self, price: float, now_ms: int) -> None:
        visit_cooldown = 300_000  # 5 min between visits
        for ob in list(self.order_blocks_bull) + list(self.order_blocks_bear):
            if ob.is_active(now_ms) and ob.contains_price(price):
                if now_ms - ob._last_visit_time >= visit_cooldown:
                    ob.visit_count += 1
                    ob._last_visit_time = now_ms

    # ══════════════════════════════════════════════════════════════════
    # SESSION / KILLZONE
    # ══════════════════════════════════════════════════════════════════

    def _update_session(self, now_ms: int) -> None:
        """Detect ICT session and killzone from current time."""
        try:
            dt = datetime.fromtimestamp(now_ms / 1000.0, tz=timezone.utc)
            utc_h = dt.hour + dt.minute / 60.0

            # Approximate NY time (UTC-5 standard, UTC-4 DST)
            # Use March-November DST approximation
            month = dt.month
            is_dst = 3 <= month <= 10  # rough DST estimate
            ny_offset = -4.0 if is_dst else -5.0
            ny_h = (utc_h + ny_offset) % 24.0

            weekday = dt.weekday()
            is_weekend = weekday >= 5

            # Killzone detection
            asia_kz = not is_weekend and ny_h >= self.KZ_ASIA_START
            london_kz = not is_weekend and self.KZ_LONDON_START <= ny_h < self.KZ_LONDON_END
            ny_kz = not is_weekend and self.KZ_NY_START <= ny_h < self.KZ_NY_END

            if ny_kz:
                self._killzone = "NY_KZ"
            elif london_kz:
                self._killzone = "LONDON_KZ"
            elif asia_kz:
                self._killzone = "ASIA_KZ"
            else:
                self._killzone = ""

            # Session
            if is_weekend:
                self._session = "WEEKEND"
            elif 12.0 <= utc_h < 21.0:
                self._session = "NEW_YORK"
            elif 7.0 <= utc_h < 17.0:
                self._session = "LONDON"
            elif 0.0 <= utc_h < 9.0:
                self._session = "ASIA"
            else:
                self._session = "OFF_HOURS"
        except Exception:
            self._session = "UNKNOWN"
            self._killzone = ""

    # ══════════════════════════════════════════════════════════════════
    # CONFLUENCE SCORING — called by quant strategy
    # ══════════════════════════════════════════════════════════════════

    def get_confluence(self, side: str, price: float, now_ms: int) -> ICTConfluence:
        """
        Score ICT structural confluence for a given trade direction.

        Returns ICTConfluence with 0-1 normalized scores for each factor.
        The quant strategy uses total as a BOOST to its composite score.

        Scoring:
          OB:      0.0-1.0 (in OB body=0.6, OTE=0.9, virgin+BOS=1.0)
          FVG:     0.0-1.0 (in gap=0.5, fresh=0.8, OB+FVG overlap=1.0)
          Sweep:   0.0-1.0 (recent sweep + displacement=1.0)
          Session: 0.0-1.0 (killzone=0.8, liquid session=0.5, off-hours=0.0)
        """
        if not self._initialized:
            return ICTConfluence(0, 0, 0, 0, 0, details="ICT not initialized")

        details = []
        ob_score = 0.0
        fvg_score = 0.0
        sweep_score = 0.0
        session_score = 0.0
        active_ob = None
        active_fvg = None

        # ── 1. Order Block scoring ────────────────────────────────────
        obs = self.order_blocks_bull if side == "long" else self.order_blocks_bear
        for ob in sorted([o for o in obs if o.is_active(now_ms)],
                         key=lambda x: x.strength, reverse=True):
            if ob.contains_price(price) or ob.in_optimal_zone(price):
                in_ote = ob.in_optimal_zone(price)
                base = 0.9 if in_ote else 0.6
                vm = ob.virgin_multiplier()
                bos_bonus = 0.1 if ob.bos_confirmed else 0.0
                ob_score = min(base * vm + bos_bonus, 1.0)
                active_ob = ob
                tag = "OTE" if in_ote else "BODY"
                details.append(f"OB_{tag} ${ob.low:.0f}-${ob.high:.0f} s={ob.strength:.0f} v={ob.visit_count}")
                break

        # ── 2. FVG scoring ────────────────────────────────────────────
        fvgs = self.fvgs_bull if side == "long" else self.fvgs_bear
        for fvg in [f for f in fvgs if f.is_active(now_ms)]:
            if fvg.is_price_in_gap(price):
                freshness = 1.0 - fvg.fill_percentage
                fvg_score = 0.5 + 0.3 * freshness
                active_fvg = fvg
                # OB + FVG overlap bonus
                if active_ob is not None:
                    fvg_score = min(fvg_score + 0.2, 1.0)
                    details.append(f"FVG+OB overlap")
                else:
                    details.append(f"FVG ${fvg.bottom:.0f}-${fvg.top:.0f} fill={fvg.fill_percentage:.0%}")
                break

        # ── 3. Liquidity sweep scoring ────────────────────────────────
        for pool in reversed(list(self.liquidity_pools)):
            if not pool.swept:
                continue
            if (now_ms - pool.sweep_timestamp) > self.SWEEP_MAX_AGE_MS:
                continue
            if (side == "long" and pool.pool_type == "EQL") or \
               (side == "short" and pool.pool_type == "EQH"):
                sweep_score = 0.6
                if pool.displacement_confirmed:
                    sweep_score += 0.2
                if pool.wick_rejection:
                    sweep_score += 0.2
                sweep_score = min(sweep_score, 1.0)
                details.append(f"Sweep {pool.pool_type} ${pool.price:.0f}")
                break

        # ── 4. Session scoring ────────────────────────────────────────
        if self._killzone:
            session_score = 0.8
            details.append(f"KZ={self._killzone}")
        elif self._session in ("NEW_YORK", "LONDON"):
            session_score = 0.5
            details.append(f"Session={self._session}")
        elif self._session == "ASIA":
            session_score = 0.3
        elif self._session == "WEEKEND":
            session_score = 0.0
            details.append("WEEKEND")
        else:
            session_score = 0.1

        # ── Weighted total ────────────────────────────────────────────
        # OB has highest weight because it's the most reliable ICT structure
        total = (ob_score * 0.35 +
                 fvg_score * 0.25 +
                 sweep_score * 0.25 +
                 session_score * 0.15)

        return ICTConfluence(
            ob_score=ob_score,
            fvg_score=fvg_score,
            sweep_score=sweep_score,
            session_score=session_score,
            total=total,
            active_ob=active_ob,
            active_fvg=active_fvg,
            session_name=self._session,
            killzone=self._killzone,
            details=" | ".join(details) if details else "no ICT structure"
        )

    # ══════════════════════════════════════════════════════════════════
    # OB-AWARE SL PLACEMENT — called by quant SL computation
    # ══════════════════════════════════════════════════════════════════

    def get_ob_sl_level(self, side: str, price: float, atr: float,
                         now_ms: int) -> Optional[float]:
        """
        Find the best OB-based SL level for the given side.

        For LONG: SL goes below the nearest bullish OB low - buffer
        For SHORT: SL goes above the nearest bearish OB high + buffer

        Returns None if no suitable OB found.
        """
        buffer = 0.3 * atr
        obs = self.order_blocks_bull if side == "long" else self.order_blocks_bear
        candidates = []

        for ob in obs:
            if not ob.is_active(now_ms):
                continue
            if side == "long" and ob.low < price:
                sl_level = ob.low - buffer
                dist = price - sl_level
                if 0.5 * atr < dist < 3.0 * atr:
                    candidates.append((sl_level, ob.strength))
            elif side == "short" and ob.high > price:
                sl_level = ob.high + buffer
                dist = sl_level - price
                if 0.5 * atr < dist < 3.0 * atr:
                    candidates.append((sl_level, ob.strength))

        if not candidates:
            return None

        # Pick the closest OB with highest strength
        if side == "long":
            # Highest SL (closest to price) among strong OBs
            candidates.sort(key=lambda x: (-x[1], -x[0]))
        else:
            candidates.sort(key=lambda x: (-x[1], x[0]))

        return candidates[0][0]

    # ══════════════════════════════════════════════════════════════════
    # STATUS — for logging / telegram
    # ══════════════════════════════════════════════════════════════════

    def get_status(self) -> Dict:
        """Return counts of all detected structures for display."""
        return {
            "ob_bull": len([ob for ob in self.order_blocks_bull if ob.visit_count < 3]),
            "ob_bear": len([ob for ob in self.order_blocks_bear if ob.visit_count < 3]),
            "fvg_bull": len([f for f in self.fvgs_bull if not f.filled]),
            "fvg_bear": len([f for f in self.fvgs_bear if not f.filled]),
            "liq_pools": len(self.liquidity_pools),
            "sweeps_active": len([p for p in self.liquidity_pools if p.swept]),
            "session": self._session,
            "killzone": self._killzone or "none",
        }

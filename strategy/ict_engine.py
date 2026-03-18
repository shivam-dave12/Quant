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
    OB_MIN_IMPULSE_PCT = 0.15
    OB_MIN_BODY_RATIO = 0.40
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

        # Proximity / bonus defaults (overridden by _load_config)
        self.ICT_REQUIRE_OB_OR_FVG = False
        self.OB_PROXIMITY_ATR      = 1.5   # max ATR dist for partial OB credit
        self.FVG_PROXIMITY_ATR     = 0.8   # max ATR dist for partial FVG credit
        self.SWEEP_DISP_BONUS      = 0.12  # confirmed-displacement sweep bonus

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
            # Issue fix: load the config key that was previously dead (never read here)
            self.ICT_REQUIRE_OB_OR_FVG = getattr(cfg, 'ICT_REQUIRE_OB_OR_FVG', False)
            # Proximity scoring: max distance (in ATR) to give partial OB/FVG credit
            self.OB_PROXIMITY_ATR = getattr(cfg, 'ICT_OB_PROXIMITY_ATR', 1.5)
            self.FVG_PROXIMITY_ATR = getattr(cfg, 'ICT_FVG_PROXIMITY_ATR', 0.8)
            # Sweep displacement bonus: confirmed sweep adds this to total
            self.SWEEP_DISP_BONUS = getattr(cfg, 'ICT_SWEEP_DISP_BONUS', 0.12)
        except ImportError:
            self.ICT_REQUIRE_OB_OR_FVG = False
            self.OB_PROXIMITY_ATR   = 1.5
            self.FVG_PROXIMITY_ATR  = 0.8
            self.SWEEP_DISP_BONUS   = 0.12

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
               price: float, now_ms: int,
               candles_1m: Optional[List[Dict]] = None) -> None:
        """
        Update all ICT structures. Throttled to every 5 seconds.

        candles_1m is optional — when supplied (e.g. during trail management)
        it enables 1m OB detection which gives the trail engine fresh micro-structure
        to anchor against. Called with 1m data from _update_trailing_sl().
        """
        now_s = now_ms / 1000.0
        if now_s - self._last_update < self._UPDATE_INTERVAL:
            return
        self._last_update = now_s

        if len(candles_5m) < 10:
            return

        self._update_session(now_ms)
        self._detect_swing_points(candles_5m, candles_15m)
        self._detect_order_blocks(candles_5m, price, now_ms)
        # Issue 3 fix: 15m OBs carry higher institutional weight for SL/TP anchoring
        if len(candles_15m) >= 5:
            self._detect_order_blocks_htf(candles_15m, price, now_ms)
        # 1m OBs for trail precision — detect fresh micro-OBs formed after entry
        # so the trailing SL can anchor to the most recent institutional footprint
        if candles_1m and len(candles_1m) >= 5:
            self._detect_order_blocks_1m(candles_1m, price, now_ms)
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
                # Swing high: center candle must be strictly higher than all neighbors
                if all(h > float(candles[j]['h']) for j in range(i - lb_left, i)) and \
                   all(h > float(candles[j]['h']) for j in range(i + 1, i + lb_right + 1)):
                    self._swing_highs.append(h)
                # Swing low: center candle must be strictly lower than all neighbors
                if all(l < float(candles[j]['l']) for j in range(i - lb_left, i)) and \
                   all(l < float(candles[j]['l']) for j in range(i + 1, i + lb_right + 1)):
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

    def _detect_order_blocks_htf(self, candles: List[Dict], price: float,
                                  now_ms: int) -> None:
        """
        Detect Order Blocks from 15m (higher timeframe) candles.

        Issue 3 fix: 15m OBs are far more significant than 5m OBs as institutional
        SL/TP anchors. They carry higher base strength (75 vs 40), age slower
        (2× max_age), and are always marked bos_confirmed + has_displacement to
        ensure they rank above 5m candidates in scored selection.

        Called every update() cycle alongside _detect_order_blocks(5m).
        Deduplication check prevents double-counting OBs that appear on both TFs.
        """
        if len(candles) < 5:
            return

        tol = price * 0.001  # 0.1% tolerance for dedup check

        for i in range(2, len(candles) - 1):
            cur = candles[i]
            nxt = candles[i + 1]

            cur_o, cur_c = float(cur['o']), float(cur['c'])
            cur_h, cur_l = float(cur['h']), float(cur['l'])
            nxt_o, nxt_c = float(nxt['o']), float(nxt['c'])
            nxt_h, nxt_l = float(nxt['h']), float(nxt['l'])
            cur_ts = int(cur.get('t', now_ms))

            nxt_range = nxt_h - nxt_l
            nxt_body  = abs(nxt_c - nxt_o)

            impulse_up = (nxt_c > nxt_o and
                          (nxt_c - nxt_o) / max(nxt_o, 1) * 100 >= self.OB_MIN_IMPULSE_PCT and
                          nxt_body / max(nxt_range, 1e-9) >= self.OB_MIN_BODY_RATIO)
            impulse_down = (nxt_c < nxt_o and
                            (nxt_o - nxt_c) / max(nxt_o, 1) * 100 >= self.OB_MIN_IMPULSE_PCT and
                            nxt_body / max(nxt_range, 1e-9) >= self.OB_MIN_BODY_RATIO)

            # Bullish HTF OB: bearish candle before bullish 15m impulse
            if impulse_up and cur_c < cur_o:
                if not any(abs(ob.low - cur_l) <= tol and abs(ob.high - cur_h) <= tol
                           for ob in self.order_blocks_bull):
                    # HTF strength: 75 base (vs 40 for 5m). BOS + displacement
                    # always marked True — a 15m impulse is structural by definition.
                    htf_strength = min(75.0 + (15.0 if nxt_body / max(nxt_range, 1e-9) >= 0.60 else 0.0), 100.0)
                    self.order_blocks_bull.append(OrderBlock(
                        low=cur_l, high=cur_h, timestamp=cur_ts,
                        direction="bullish", strength=htf_strength,
                        bos_confirmed=True, has_displacement=True,
                        has_wick_rejection=False,
                        max_age_ms=self.OB_MAX_AGE_MS * 2))  # 15m OBs stay valid 2× longer
                    logger.debug(f"📦 HTF OB BULL ${cur_l:.0f}–${cur_h:.0f} str={htf_strength:.0f}")

            # Bearish HTF OB: bullish candle before bearish 15m impulse
            if impulse_down and cur_c > cur_o:
                if not any(abs(ob.low - cur_l) <= tol and abs(ob.high - cur_h) <= tol
                           for ob in self.order_blocks_bear):
                    htf_strength = min(75.0 + (15.0 if nxt_body / max(nxt_range, 1e-9) >= 0.60 else 0.0), 100.0)
                    self.order_blocks_bear.append(OrderBlock(
                        low=cur_l, high=cur_h, timestamp=cur_ts,
                        direction="bearish", strength=htf_strength,
                        bos_confirmed=True, has_displacement=True,
                        has_wick_rejection=False,
                        max_age_ms=self.OB_MAX_AGE_MS * 2))
                    logger.debug(f"📦 HTF OB BEAR ${cur_l:.0f}–${cur_h:.0f} str={htf_strength:.0f}")

    def _detect_order_blocks_1m(self, candles: List[Dict], price: float,
                                 now_ms: int) -> None:
        """
        Detect Order Blocks from 1m candles for trail SL precision.

        These are the freshest institutional footprints — OBs that formed AFTER
        the entry, representing where smart money re-entered to defend the position.
        The trailing SL ICT OB anchor (get_ob_sl_level) will use these to keep the
        SL behind the most recent 1m institutional level rather than a stale 5m OB.

        1m OBs: base strength 50 (lower than 5m), max_age_ms = 30min.
        Short-lived by design — 1m structure is micro, only relevant for the trail.
        """
        if len(candles) < 5:
            return

        # Only look at the last 30 candles (30 minutes of 1m data)
        recent = candles[-30:]
        tol = price * 0.0005  # tighter dedup tolerance for 1m

        for i in range(2, len(recent) - 1):
            cur = recent[i]
            nxt = recent[i + 1]

            cur_o, cur_c = float(cur['o']), float(cur['c'])
            cur_h, cur_l = float(cur['h']), float(cur['l'])
            nxt_o, nxt_c = float(nxt['o']), float(nxt['c'])
            nxt_h, nxt_l = float(nxt['h']), float(nxt['l'])
            cur_ts = int(cur.get('t', now_ms))

            nxt_range = nxt_h - nxt_l
            nxt_body  = abs(nxt_c - nxt_o)

            # Use slightly relaxed thresholds for 1m (impulses are smaller)
            min_impulse = self.OB_MIN_IMPULSE_PCT * 0.6  # 60% of 5m threshold
            impulse_up = (nxt_c > nxt_o and
                          (nxt_c - nxt_o) / max(nxt_o, 1) * 100 >= min_impulse and
                          nxt_body / max(nxt_range, 1e-9) >= self.OB_MIN_BODY_RATIO)
            impulse_down = (nxt_c < nxt_o and
                            (nxt_o - nxt_c) / max(nxt_o, 1) * 100 >= min_impulse and
                            nxt_body / max(nxt_range, 1e-9) >= self.OB_MIN_BODY_RATIO)

            # Bullish 1m OB: bearish candle before bullish 1m impulse
            if impulse_up and cur_c < cur_o:
                if not any(abs(ob.low - cur_l) <= tol and abs(ob.high - cur_h) <= tol
                           for ob in self.order_blocks_bull):
                    self.order_blocks_bull.append(OrderBlock(
                        low=cur_l, high=cur_h, timestamp=cur_ts,
                        direction="bullish", strength=50.0,
                        bos_confirmed=False, has_displacement=False,
                        has_wick_rejection=False,
                        max_age_ms=1_800_000))   # 30 min — micro OBs expire fast
                    logger.debug(f"📦 1m OB BULL ${cur_l:.0f}–${cur_h:.0f}")

            # Bearish 1m OB: bullish candle before bearish 1m impulse
            if impulse_down and cur_c > cur_o:
                if not any(abs(ob.low - cur_l) <= tol and abs(ob.high - cur_h) <= tol
                           for ob in self.order_blocks_bear):
                    self.order_blocks_bear.append(OrderBlock(
                        low=cur_l, high=cur_h, timestamp=cur_ts,
                        direction="bearish", strength=50.0,
                        bos_confirmed=False, has_displacement=False,
                        has_wick_rejection=False,
                        max_age_ms=1_800_000))
                    logger.debug(f"📦 1m OB BEAR ${cur_l:.0f}–${cur_h:.0f}")

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
        """
        Track how many times price has revisited each OB.

        Visit cooldown is quality-adaptive:
          BOS + displacement confirmed: 600s (10 min)
            Rationale: a BOS+DISP OB is institutional accumulation/distribution.
            Price consolidating IN it for 5-10 minutes is normal and healthy —
            it doesn't degrade the OB. Using a 5-min cooldown causes a 10-min
            consolidation to accumulate 2 visits, halving the scoring and making
            the ICT gate unreachable for the highest-quality setups.
          BOS only or DISP only: 450s (7.5 min)
          RAW OB: 300s (5 min) — aggressive degradation for unconfirmed OBs
        """
        for ob in list(self.order_blocks_bull) + list(self.order_blocks_bear):
            if not ob.is_active(now_ms) or not ob.contains_price(price):
                continue
            # Quality-adaptive cooldown
            if ob.bos_confirmed and ob.has_displacement:
                visit_cooldown = 600_000   # 10 min — institutional OB
            elif ob.bos_confirmed or ob.has_displacement:
                visit_cooldown = 450_000   # 7.5 min — partially confirmed
            else:
                visit_cooldown = 300_000   # 5 min — raw, degrade quickly
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

    def get_confluence(self, side: str, price: float, now_ms: int,
                       atr: float = 0.0) -> ICTConfluence:
        """
        Score ICT structural confluence for a given trade direction.

        Returns ICTConfluence with 0-1 normalized scores for each factor.
        The quant strategy uses total as a BOOST to its composite score.

        Scoring:
          OB:      0.0-1.0 (in-zone: OTE=0.85, body=0.55; proximity: up to 0.40)
          FVG:     0.0-1.0 (in-gap=0.5-0.8; proximity: up to 0.35)
          Sweep:   0.0-1.0 (recent sweep + displacement=1.0; disp bonus adds 0.12)
          Session: 0.0-1.0 (killzone=0.8, liquid session=0.5, off-hours=0.0)

        v5.0 FIX — Proximity scoring:
          Previously OB/FVG only scored when price was physically INSIDE the zone.
          During mean-reversion entries price has already bounced FROM the OB (just
          above it) or hasn't yet reached a nearby OB. Both give 0 with in-zone
          only scoring, making the 0.45 gate arithmetically unreachable.

          Now: nearest OB/FVG within self.OB_PROXIMITY_ATR also scores (decaying
          linearly with distance). At 0 ATR from OB edge → full proximity score
          (0.40). At OB_PROXIMITY_ATR away → 0.0. This correctly models the
          institutional concept that price near a demand/supply zone is significant
          even before entering it.

        v5.0 FIX — Sweep displacement bonus:
          A displacement-confirmed + wick-rejection sweep is the canonical ICT entry
          trigger ("liquidity sweep then entry"). Previously it maxed at 0.25 weight,
          so sweep+session = 0.30 < min_score=0.45. Now confirmed sweeps add a 0.12
          bonus to total AFTER weighting, enabling high-quality sweep setups to pass
          without requiring an OB/FVG overlap.
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
        _confirmed_sweep = False   # tracks displacement+wick sweep for bonus

        # ── 1. Order Block scoring ────────────────────────────────────
        obs = self.order_blocks_bull if side == "long" else self.order_blocks_bear
        active_obs = sorted([o for o in obs if o.is_active(now_ms)],
                            key=lambda x: x.strength, reverse=True)

        for ob in active_obs:
            if ob.contains_price(price) or ob.in_optimal_zone(price):
                in_ote = ob.in_optimal_zone(price)
                base = 0.85 if in_ote else 0.55
                vm = ob.virgin_multiplier()
                visit_penalty = max(0.5, 1.0 - ob.visit_count * 0.25)
                bos_bonus = 0.15 if ob.bos_confirmed else 0.0
                disp_bonus = 0.10 if ob.has_displacement else 0.0
                ob_score = min(base * vm * visit_penalty + bos_bonus + disp_bonus, 1.0)
                active_ob = ob
                tag = "OTE" if in_ote else "BODY"
                quality = "BOS+DISP" if (ob.bos_confirmed and ob.has_displacement) else \
                          ("BOS" if ob.bos_confirmed else ("DISP" if ob.has_displacement else "RAW"))
                details.append(f"OB_{tag}_{quality} ${ob.low:.0f}-${ob.high:.0f} s={ob.strength:.0f} v={ob.visit_count} score={ob_score:.2f}")
                break

        # ── 1b. Proximity OB scoring (price near but not inside OB) ──
        # Root cause fix: mean-reversion entries fire AFTER price bounces from an OB
        # (price is just ABOVE the OB high for longs, just BELOW the OB low for shorts).
        # Give partial credit decaying linearly with distance up to OB_PROXIMITY_ATR.
        if ob_score < 0.01 and atr > 1e-10:
            prox_atr = self.OB_PROXIMITY_ATR
            best_prox = (0.0, None)   # (score, ob)
            for ob in active_obs:
                if side == "long":
                    # Bullish OB: price above OB high (just departed upward after bounce)
                    if ob.high < price:
                        dist_atr = (price - ob.high) / atr
                        if dist_atr <= prox_atr:
                            pf = 1.0 - dist_atr / prox_atr        # 1.0 at edge → 0 at limit
                            vm = ob.virgin_multiplier()
                            bos_b = 0.08 if ob.bos_confirmed else 0.0
                            s = min(0.40 * pf * vm + bos_b, 0.45)
                            if s > best_prox[0]:
                                best_prox = (s, ob)
                    # Also: price approaching OB from above (within proximity)
                    elif ob.low <= price <= ob.high:
                        pass   # handled by contains_price above
                else:  # short
                    # Bearish OB: price below OB low (just departed downward after test)
                    if ob.low > price:
                        dist_atr = (ob.low - price) / atr
                        if dist_atr <= prox_atr:
                            pf = 1.0 - dist_atr / prox_atr
                            vm = ob.virgin_multiplier()
                            bos_b = 0.08 if ob.bos_confirmed else 0.0
                            s = min(0.40 * pf * vm + bos_b, 0.45)
                            if s > best_prox[0]:
                                best_prox = (s, ob)
            if best_prox[0] > 0.01:
                ob_score = best_prox[0]
                active_ob = best_prox[1]
                ob = best_prox[1]
                dist_lbl = f"{(abs(price - ob.high) if side == 'long' else abs(ob.low - price)) / atr:.1f}ATR"
                details.append(f"OB_PROX({dist_lbl}) ${ob.low:.0f}-${ob.high:.0f} v={ob.visit_count} score={ob_score:.2f}")

        # ── 2. FVG scoring ────────────────────────────────────────────
        fvgs = self.fvgs_bull if side == "long" else self.fvgs_bear
        active_fvgs = [f for f in fvgs if f.is_active(now_ms)]
        for fvg in active_fvgs:
            if fvg.is_price_in_gap(price):
                freshness = 1.0 - fvg.fill_percentage
                fvg_score = 0.5 + 0.3 * freshness
                active_fvg = fvg
                if active_ob is not None:
                    fvg_score = min(fvg_score + 0.2, 1.0)
                    details.append("FVG+OB overlap")
                else:
                    details.append(f"FVG ${fvg.bottom:.0f}-${fvg.top:.0f} fill={fvg.fill_percentage:.0%}")
                break

        # ── 2b. Proximity FVG scoring (price near but not inside FVG) ─
        if fvg_score < 0.01 and atr > 1e-10:
            prox_atr_fvg = self.FVG_PROXIMITY_ATR
            for fvg in active_fvgs:
                if side == "long" and fvg.bottom > price:
                    # Bullish FVG above price: price approaching it (reversion up)
                    dist_atr = (fvg.bottom - price) / atr
                    if dist_atr <= prox_atr_fvg:
                        pf = 1.0 - dist_atr / prox_atr_fvg
                        freshness = 1.0 - fvg.fill_percentage
                        fvg_score = min(0.35 * pf * freshness, 0.35)
                        active_fvg = fvg
                        details.append(f"FVG_PROX {dist_atr:.1f}ATR ${fvg.bottom:.0f}-${fvg.top:.0f}")
                        break
                elif side == "short" and fvg.top < price:
                    # Bearish FVG below price: price approaching it (reversion down)
                    dist_atr = (price - fvg.top) / atr
                    if dist_atr <= prox_atr_fvg:
                        pf = 1.0 - dist_atr / prox_atr_fvg
                        freshness = 1.0 - fvg.fill_percentage
                        fvg_score = min(0.35 * pf * freshness, 0.35)
                        active_fvg = fvg
                        details.append(f"FVG_PROX {dist_atr:.1f}ATR ${fvg.bottom:.0f}-${fvg.top:.0f}")
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
                # Track confirmed sweep for displacement bonus below
                if pool.displacement_confirmed and pool.wick_rejection:
                    _confirmed_sweep = True
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
        # Weights reflect institutional significance:
        #   OB:      0.45 — highest conviction: where orders were placed
        #   FVG:     0.25 — imbalance that attracts price
        #   Sweep:   0.25 — liquidity raid confirming reversal intent
        #   Session: 0.05 — timing context only, not structure
        total = (ob_score    * 0.45 +
                 fvg_score   * 0.25 +
                 sweep_score * 0.25 +
                 session_score * 0.05)

        # ── Sweep displacement bonus ──────────────────────────────────
        # A confirmed sweep (displacement + wick rejection) is the canonical ICT
        # "liquidity sweep then entry" setup. Without this bonus, sweep+session
        # maxes at 0.30, which is mathematically below ICT_MIN_SCORE_FOR_ENTRY=0.45.
        # The bonus allows high-quality sweep setups to pass the gate even without
        # a visible OB/FVG at current price.
        if _confirmed_sweep:
            total = min(total + self.SWEEP_DISP_BONUS, 1.0)
            details.append(f"SWEEP_DISP_BONUS+{self.SWEEP_DISP_BONUS:.2f}")

        # Structural presence guard: if no OB AND no sweep, cap total at 0.30
        # so that FVG-only or FVG+session setups don't trigger the entry gate.
        if ob_score < 0.05 and sweep_score < 0.05:
            total = min(total, 0.30)

        # ICT_REQUIRE_OB_OR_FVG enforcement: if enabled and neither OB nor FVG
        # has scored (even via proximity), hard-cap at 0.20 to block entry.
        if self.ICT_REQUIRE_OB_OR_FVG and ob_score < 0.05 and fvg_score < 0.05:
            total = min(total, 0.20)
            if "REQUIRE_OB_OR_FVG_CAP" not in " ".join(details):
                details.append("REQUIRE_OB_OR_FVG_CAP")

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
                         now_ms: int,
                         htf_only: bool = False) -> Optional[float]:
        """
        OB-based SL placement — immune to FVG and liquidity-pool traps.

        htf_only=True (for initial entry SL): only consider 15m OBs (strength ≥ 70).
          These are set from _detect_order_blocks_htf with base strength 75-90.
          Using only 15m OBs ensures the entry SL is anchored to macro structure.

        htf_only=False (for trail OB anchor): consider all active OBs.
          The nearest OB to current price is used — this will often be a fresh
          1m OB that formed after entry, giving the tightest valid anchor.

        For LONG:  SL = bullish OB low - buffer.
        For SHORT: SL = bearish OB high + buffer.

        Critical structural safety passes (in order):

        1. FVG ESCAPE — if the computed SL falls inside an opposing FVG, the
           FVG will be filled and the SL hunted. Push the SL to the far side
           of the FVG plus a 0.2×ATR clearance.
           Example: SHORT SL at $73,573 lands inside Bear FVG $73,492–$73,611.
           Escape pushes SL to $73,611 + 0.2×ATR = $73,637 — above the trap.

        2. LIQUIDITY POOL ESCAPE — if the computed SL is within 0.5×ATR of an
           EQL (for longs) or EQH (for shorts), price will sweep that pool and
           blow through the SL. Push SL past the pool + 0.3×ATR clearance.

        3. DISTANCE GUARD — final candidate must be within [0.5, 4.0] × ATR
           of entry. Reject if out of range (too tight = noise, too wide = bad
           risk management).
        """
        buffer   = 0.3 * atr
        fvg_buf  = 0.2 * atr
        liq_buf  = 0.3 * atr
        max_dist = 4.0 * atr
        min_dist = 0.5 * atr

        obs = self.order_blocks_bull if side == "long" else self.order_blocks_bear
        candidates = []

        # HTF threshold: 15m OBs have base strength 75 (set in _detect_order_blocks_htf).
        # 5m OBs = 40, 1m OBs = 50. Using 70 as the floor captures 15m cleanly.
        _htf_thresh = 70.0

        for ob in obs:
            if not ob.is_active(now_ms):
                continue
            if htf_only and ob.strength < _htf_thresh:
                continue   # skip non-HTF OBs for entry SL

            if side == "long" and ob.low < price:
                sl_level = ob.low - buffer
            elif side == "short" and ob.high > price:
                sl_level = ob.high + buffer
            else:
                continue

            # ── Pass 1: Escape any opposing FVG the SL lands inside ──────────
            # For SHORT, opposing FVGs are bullish (below price, SL above price).
            # Actually the SL for SHORT is above price, so dangerous FVGs are
            # BEARISH FVGs above price — if SL is inside one, price will fill
            # the gap and stop us out on the way up.
            if side == "short":
                for fvg in self.fvgs_bear:
                    if fvg.filled or not fvg.is_active(now_ms):
                        continue
                    if fvg.bottom <= sl_level <= fvg.top:
                        # SL is inside a bearish FVG — push above the FVG
                        sl_level = fvg.top + fvg_buf
                        break
            else:  # long
                for fvg in self.fvgs_bull:
                    if fvg.filled or not fvg.is_active(now_ms):
                        continue
                    if fvg.bottom <= sl_level <= fvg.top:
                        # SL is inside a bullish FVG — push below the FVG
                        sl_level = fvg.bottom - fvg_buf
                        break

            # ── Pass 2: Escape nearby liquidity pools ─────────────────────────
            # For SHORT: SL is above price. Nearby EQH above price = stop-hunt
            # magnet. If SL is within liq_buf of an EQH, push SL above the pool.
            for pool in self.liquidity_pools:
                if pool.swept:
                    continue
                if side == "short" and pool.pool_type == "EQH":
                    if abs(sl_level - pool.price) < liq_buf and sl_level < pool.price:
                        # SL is just below an EQH — price will sweep the EQH
                        # and blow through SL. Push SL above pool + clearance.
                        sl_level = pool.price + liq_buf
                elif side == "long" and pool.pool_type == "EQL":
                    if abs(sl_level - pool.price) < liq_buf and sl_level > pool.price:
                        sl_level = pool.price - liq_buf

            # ── Pass 3: Distance guard ────────────────────────────────────────
            dist = abs(sl_level - price)
            if not (min_dist <= dist <= max_dist):
                continue

            candidates.append((sl_level, ob.strength, dist))

        if not candidates:
            return None

        # Prefer: highest strength, then tightest (closest to price)
        if side == "long":
            candidates.sort(key=lambda x: (-x[1], x[2]))   # highest SL closest to price
        else:
            candidates.sort(key=lambda x: (-x[1], x[2]))   # lowest SL closest to price

        sl_final = candidates[0][0]

        # Final FVG/pool sweep check on selected candidate
        # (secondary OBs after first loop may have shifted things)
        if side == "short" and sl_final > price:
            return sl_final
        if side == "long" and sl_final < price:
            return sl_final
        return None

    def get_structural_tp_targets(self, side: str, price: float, atr: float,
                                   now_ms: int, min_dist: float,
                                   max_dist: float,
                                   htf_only: bool = False) -> List[Tuple[float, float, str]]:
        """
        Return ICT structural TP candidates for the given trade direction.

        htf_only=True (for initial entry TP): only consider 15m OBs (strength ≥ 70).
          FVGs and swept liquidity pools are not tagged by timeframe so they are
          always included (they use 5m+15m candle data for detection).
          OB candidates are filtered to strength ≥ 70 (15m-derived).

        htf_only=False (default): all OBs included.

        Candidates (price_level, score, label):
          6.0  Swept liquidity origin — primary delivery target after sweep-reverse
          5.0  Unfilled FVG — imbalance filling (not TF-filtered)
          4.0  Virgin OB in path — institutional footprint (htf_only filters here)

        All candidates are filtered to [min_dist, max_dist] from entry price.
        """
        _htf_thresh = 70.0
        candidates: List[Tuple[float, float, str]] = []

        # ── 1. Swept liquidity origins ─────────────────────────────────────
        for pool in self.liquidity_pools:
            if not pool.swept:
                continue
            # After sweep, price targets the sweep origin
            # For SHORT: swept EQH below price = price was pushed UP through it,
            #            then reversed — target the origin (the EQH level)
            # For LONG:  swept EQL above price = price was pushed DOWN through it,
            #            then reversed — target the origin
            sweep_age_ms = now_ms - pool.sweep_timestamp
            if sweep_age_ms > self.SWEEP_MAX_AGE_MS:
                continue

            level = pool.price
            dist  = price - level if side == "short" else level - price

            if not (min_dist <= dist <= max_dist):
                continue
            if side == "short" and pool.pool_type == "EQH" and level < price:
                freshness = max(0.0, 1.0 - sweep_age_ms / self.SWEEP_MAX_AGE_MS)
                score = 6.0 * (0.7 + 0.3 * freshness)
                if pool.displacement_confirmed:
                    score += 0.5
                candidates.append((level, score, f"SweepOrigin_EQH@${level:,.0f}"))
            elif side == "long" and pool.pool_type == "EQL" and level > price:
                freshness = max(0.0, 1.0 - sweep_age_ms / self.SWEEP_MAX_AGE_MS)
                score = 6.0 * (0.7 + 0.3 * freshness)
                if pool.displacement_confirmed:
                    score += 0.5
                candidates.append((level, score, f"SweepOrigin_EQL@${level:,.0f}"))

        # ── 2. Open FVGs in trade direction ────────────────────────────────
        # For SHORT: bullish FVGs BELOW price are TP targets (price fills them)
        # For LONG:  bearish FVGs ABOVE price are TP targets
        target_fvgs = self.fvgs_bull if side == "short" else self.fvgs_bear
        for fvg in target_fvgs:
            if fvg.filled or not fvg.is_active(now_ms):
                continue
            # Near edge of FVG (first touch = partial fill, conservative TP)
            near_edge = fvg.top if side == "short" else fvg.bottom
            far_edge  = fvg.bottom if side == "short" else fvg.top

            dist_near = price - near_edge if side == "short" else near_edge - price
            dist_far  = price - far_edge  if side == "short" else far_edge  - price

            if not (min_dist <= dist_near <= max_dist):
                continue

            # Score: larger gap = more imbalance = higher conviction target
            # Fresh FVG (low fill%) = higher score
            freshness   = 1.0 - fvg.fill_percentage
            size_factor = min(fvg.size / max(atr * 0.5, 1.0), 2.0)
            score       = 5.0 * freshness * (0.6 + 0.4 * size_factor)
            candidates.append((near_edge, score,
                                f"FVG_near@${near_edge:,.0f}(fill={fvg.fill_percentage:.0%})"))

            # Also offer the far edge as a second candidate (full fill)
            if min_dist <= dist_far <= max_dist:
                candidates.append((far_edge, score * 0.85,
                                   f"FVG_far@${far_edge:,.0f}"))

        # ── 3. Virgin OBs in trade direction ───────────────────────────────
        # For SHORT: bullish OBs below price that haven't been visited
        # For LONG:  bearish OBs above price
        # htf_only=True: only 15m OBs (strength ≥ 70) for entry TP placement
        target_obs = self.order_blocks_bull if side == "short" else self.order_blocks_bear
        for ob in target_obs:
            if not ob.is_active(now_ms) or ob.visit_count > 0:
                continue
            if htf_only and ob.strength < _htf_thresh:
                continue   # skip non-HTF OBs for entry TP
            # Target the OB midpoint
            level = ob.midpoint
            dist  = price - level if side == "short" else level - price
            if not (min_dist <= dist <= max_dist):
                continue
            score = 4.0 * ob.virgin_multiplier() * (ob.strength / 100.0)
            if ob.bos_confirmed:
                score += 0.5
            candidates.append((level, score, f"VirginOB@${level:,.0f}_str={ob.strength:.0f}"))

        # ── 4. Swing lows/highs in trade direction ─────────────────────────
        # Already handled in quant_strategy.py's compute_tp, but provide our
        # version filtered through ICT structural context
        swing_levels = (self._swing_lows if side == "short" else self._swing_highs)
        for level in sorted(swing_levels):
            dist = price - level if side == "short" else level - price
            if not (min_dist <= dist <= max_dist):
                continue
            # Lower score — swings without OB/FVG context are weaker
            candidates.append((level, 3.0, f"Swing@${level:,.0f}"))

        return candidates

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

    def get_full_status(self, price: float, atr: float, now_ms: int) -> Dict:
        """
        Comprehensive status for Telegram /structures command.

        Returns detailed information about all detected ICT structures
        including order blocks, FVGs, liquidity pools, sweeps, and swings
        with distance/ATR metrics relative to current price.
        """
        atr_safe = max(atr, 1e-9)

        # ── Helper: build OB detail dict ─────────────────────────────────
        def _ob_detail(ob: OrderBlock) -> Dict:
            mid = ob.midpoint
            dist = mid - price  # positive = above price
            age_ms = now_ms - ob.timestamp
            tags = []
            if ob.has_displacement:
                tags.append("DISP")
            if ob.has_wick_rejection:
                tags.append("WR")
            if ob.visit_count == 0:
                tags.append("VIRGIN")
            return {
                "low": ob.low,
                "high": ob.high,
                "midpoint": mid,
                "strength": ob.strength,
                "visit_count": ob.visit_count,
                "bos": ob.bos_confirmed,
                "in_ob": ob.contains_price(price),
                "in_ote": ob.in_optimal_zone(price),
                "dist_pts": dist,
                "dist_atr": abs(dist) / atr_safe,
                "age_min": age_ms / 60_000.0,
                "tags": tags,
            }

        # ── Helper: build FVG detail dict ────────────────────────────────
        def _fvg_detail(fvg) -> Dict:
            mid = fvg.midpoint
            dist = mid - price
            age_ms = now_ms - fvg.timestamp
            return {
                "direction": fvg.direction,
                "bottom": fvg.bottom,
                "top": fvg.top,
                "size": fvg.size,
                "fill_pct": fvg.fill_percentage,
                "in_gap": fvg.is_price_in_gap(price),
                "dist_pts": dist,
                "dist_atr": abs(dist) / atr_safe,
                "age_min": age_ms / 60_000.0,
            }

        # ── Active OBs (sorted by distance to price) ────────────────────
        bull_obs = sorted(
            [_ob_detail(ob) for ob in self.order_blocks_bull
             if ob.is_active(now_ms)],
            key=lambda x: abs(x["dist_pts"]))
        bear_obs = sorted(
            [_ob_detail(ob) for ob in self.order_blocks_bear
             if ob.is_active(now_ms)],
            key=lambda x: abs(x["dist_pts"]))

        # ── Active FVGs ──────────────────────────────────────────────────
        bull_fvgs = sorted(
            [_fvg_detail(f) for f in self.fvgs_bull
             if f.is_active(now_ms)],
            key=lambda x: abs(x["dist_pts"]))
        bear_fvgs = sorted(
            [_fvg_detail(f) for f in self.fvgs_bear
             if f.is_active(now_ms)],
            key=lambda x: abs(x["dist_pts"]))

        # ── Liquidity pools ──────────────────────────────────────────────
        liq_active = []
        liq_swept = []
        for pool in self.liquidity_pools:
            entry = {
                "pool_type": pool.pool_type,
                "price": pool.price,
                "touch_count": pool.touch_count,
                "dist_pts": pool.price - price,
            }
            if pool.swept:
                entry["displacement"] = pool.displacement_confirmed
                entry["wick_rejection"] = pool.wick_rejection
                sweep_age = (now_ms - pool.sweep_timestamp) / 60_000.0 if pool.sweep_timestamp else None
                entry["sweep_age_min"] = sweep_age
                liq_swept.append(entry)
            else:
                liq_active.append(entry)

        liq_active.sort(key=lambda x: abs(x["dist_pts"]))
        liq_swept.sort(key=lambda x: abs(x.get("sweep_age_min") or 9999))

        # ── Swing levels (nearest to price) ──────────────────────────────
        swing_highs = sorted(
            [h for h in self._swing_highs if h > price],
            key=lambda h: h - price)[:6]
        swing_lows = sorted(
            [l for l in self._swing_lows if l < price],
            key=lambda l: price - l)[:6]

        return {
            "counts": {
                "ob_bull": len(bull_obs),
                "ob_bear": len(bear_obs),
                "fvg_bull": len(bull_fvgs),
                "fvg_bear": len(bear_fvgs),
                "liq_active": len(liq_active),
                "liq_swept": len(liq_swept),
            },
            "session": self._session,
            "killzone": self._killzone or "",
            "bull_obs": bull_obs,
            "bear_obs": bear_obs,
            "bull_fvgs": bull_fvgs,
            "bear_fvgs": bear_fvgs,
            "liq_active": liq_active,
            "liq_swept": liq_swept,
            "swing_highs": swing_highs,
            "swing_lows": swing_lows,
        }
    
    # ══════════════════════════════════════════════════════════════════
    # v5.0: ICT-AWARE TRAILING SL PROTECTION
    # ══════════════════════════════════════════════════════════════════
    
    def check_sl_path_for_structure(self, pos_side: str, current_sl: float, 
                                     new_sl: float, now_ms: int,
                                     max_ob_visits: int = 1, 
                                     max_fvg_fill: float = 0.30) -> Tuple[bool, str]:
        """
        v5.0 INSTITUTIONAL: Check if moving SL would cross virgin ICT structure.
        
        CRITICAL: Never trail SL through virgin OB/FVG that supports the trade.
        
        Args:
            pos_side: "long" or "short"
            current_sl: Current stop loss price
            new_sl: Proposed new stop loss price
            now_ms: Current timestamp
            max_ob_visits: Only protect OBs with ≤N visits (0=virgin only, 1=virgin+once)
            max_fvg_fill: Only protect FVGs with ≤N% fill
            
        Returns:
            (blocked: bool, reason: str)
            - blocked=True means SL should NOT move (structure protection)
            - blocked=False means SL can move (no conflict)
        """
        if pos_side == "long":
            # LONG: Check for bullish OBs/FVGs between current_sl and new_sl
            # These support the trade — price bouncing here is GOOD
            
            # Check bullish OBs
            for ob in self.order_blocks_bull:
                if not ob.is_active(now_ms) or ob.visit_count > max_ob_visits:
                    continue
                # OB between current and new SL?
                if current_sl < ob.low < new_sl or current_sl < ob.high < new_sl:
                    return True, f"Virgin bullish OB @ ${ob.midpoint:.0f} (visits={ob.visit_count})"
            
            # Check bullish FVGs
            for fvg in self.fvgs_bull:
                if not fvg.is_active(now_ms) or fvg.fill_percentage > max_fvg_fill:
                    continue
                # FVG between current and new SL?
                if current_sl < fvg.bottom < new_sl or current_sl < fvg.top < new_sl:
                    return True, f"Fresh bullish FVG @ ${fvg.midpoint:.0f} (fill={fvg.fill_percentage:.0%})"
        
        else:  # SHORT
            # SHORT: Check for bearish OBs/FVGs between new_sl and current_sl
            
            # Check bearish OBs
            for ob in self.order_blocks_bear:
                if not ob.is_active(now_ms) or ob.visit_count > max_ob_visits:
                    continue
                # OB between new and current SL?
                if new_sl < ob.low < current_sl or new_sl < ob.high < current_sl:
                    return True, f"Virgin bearish OB @ ${ob.midpoint:.0f} (visits={ob.visit_count})"
            
            # Check bearish FVGs
            for fvg in self.fvgs_bear:
                if not fvg.is_active(now_ms) or fvg.fill_percentage > max_fvg_fill:
                    continue
                # FVG between new and current SL?
                if new_sl < fvg.bottom < current_sl or new_sl < fvg.top < current_sl:
                    return True, f"Fresh bearish FVG @ ${fvg.midpoint:.0f} (fill={fvg.fill_percentage:.0%})"
        
        return False, ""  # No structure blocking the move

"""
ict_engine.py — Industry-Grade ICT/SMC Analysis Engine v6.0
============================================================
Full rewrite: AMD cycle, multi-timeframe structure, complete PD array stack.

ICT Concepts Implemented
─────────────────────────
1. AMD Cycle (Accumulation → Manipulation → Distribution)
   Smart money accumulates in a range, runs stops in one direction
   (Judas swing / manipulation), then delivers price the opposite way
   (distribution). All entries align with the expected distribution leg.

2. Multi-Timeframe Market Structure (1D → 4H → 1H → 15M → 5M → 1M)
   Per timeframe:
   - Swing sequence (HH/HL = bullish, LH/LL = bearish)
   - BOS  — Break of Structure: close beyond last significant swing
   - CHoCH — Change of Character: first opposing structural break
   - Premium/Discount: price position in the recent H-L range
   - Equilibrium (50% of range)

3. PD Array Stack (delivery/reversal zones)
   OB  — Order Block (last opposite candle before strong impulse)
   FVG — Fair Value Gap (3-candle imbalance)
   BSL — Buy-Side Liquidity (equal highs / buy stops above)
   SSL — Sell-Side Liquidity (equal lows / sell stops below)

4. Liquidity Sweep Detection
   Price wicks THROUGH a pool and CLOSES on the opposite side.
   Displacement (strong body) = institutional confirmation.

5. Kill Zones + Sessions (Asia / London / NY)

6. Component-Based Confluence Scoring
   Structure alignment + AMD phase + PD array + Liquidity + Session
   (independent components, never a blended guess)

Backward-Compatible Public API
───────────────────────────────
update(candles_5m, candles_15m, price, now_ms,
       candles_1m=None, candles_1h=None, candles_4h=None, candles_1d=None)
get_confluence(side, price, now_ms, atr) → ICTConfluence
get_ob_sl_level(side, price, atr, now_ms, htf_only=False) → Optional[float]
get_structural_tp_targets(side, price, atr, now_ms, min_dist, max_dist,
                           htf_only=False) → List[(price, score, label)]
get_amd_state() → AMDState
get_market_bias() → MarketBias
get_status() → Dict
get_full_status(price, atr, now_ms) → Dict
check_sl_path_for_structure(pos_side, current_sl, new_sl, now_ms) → (bool, str)
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple, Deque, NamedTuple
from collections import deque
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class OrderBlock:
    """ICT Order Block — last opposite candle before a strong impulse."""
    low:       float
    high:      float
    timestamp: int            # epoch ms
    direction: str            # "bullish" | "bearish"
    timeframe: str            # "1m"|"5m"|"15m"|"1h"|"4h"|"1d"
    strength:  float = 50.0  # 0–100; higher TF = higher base
    visit_count:      int   = 0
    bos_confirmed:    bool  = False
    has_displacement: bool  = False
    has_wick_rejection: bool = False
    max_age_ms: int = 86_400_000   # 24 h default (HTF gets 72 h)
    _last_visit_time: int = 0
    # BUG-OB-MITIGATION FIX: ICT defines an OB as MITIGATED when a candle
    # CLOSES beyond the OB's far extreme (not just touches/wicks through it).
    # visit_count alone is an incomplete proxy.  mitigated=True is set in
    # _update_ob_mitigation() when a confirmed close breaches the OB extreme.
    mitigated: bool = False

    @property
    def midpoint(self) -> float:
        return (self.low + self.high) / 2.0

    @property
    def size(self) -> float:
        return self.high - self.low

    def contains_price(self, price: float) -> bool:
        return self.low <= price <= self.high

    def in_optimal_zone(self, price: float) -> bool:
        """OTE = 61.8%–78.6% retracement into OB body (ICT standard Fibonacci).

        BUG-OB-OTE-FIB FIX: old code used 50%–79%.  ICT defines OTE as the
        61.8%–78.6% Fibonacci retracement of the DISPLACEMENT MOVE.  When
        applied to the OB body as a proxy, the correct bounds are 61.8%–78.6%
        of the body depth (not 50%), matching the fib levels used in
        ICTSweepDetector.OTE_LOWER_FIB / OTE_UPPER_FIB.
        """
        if self.size < 1e-10:
            return False
        if self.direction == "bullish":
            # Bullish OB body = [low, high]; OTE = deep into body from top
            top = self.high - 0.618 * self.size   # 61.8% from top
            bot = self.high - 0.786 * self.size   # 78.6% from top
        else:
            # Bearish OB body = [low, high]; OTE = deep into body from bottom
            bot = self.low + 0.618 * self.size    # 61.8% from bottom
            top = self.low + 0.786 * self.size    # 78.6% from bottom
        return bot <= price <= top

    def is_active(self, now_ms: int) -> bool:
        return (not self.mitigated and
                now_ms - self.timestamp <= self.max_age_ms and
                self.visit_count < 3)

    def virgin_multiplier(self) -> float:
        if   self.visit_count == 0: return 1.0
        elif self.visit_count == 1: return 0.70
        else:                       return 0.40


@dataclass
class FairValueGap:
    """ICT FVG — 3-candle imbalance (candle[i-1].extremity vs candle[i+1].extremity)."""
    bottom:    float
    top:       float
    timestamp: int
    direction: str   # "bullish" | "bearish"
    timeframe: str
    fill_percentage: float = 0.0
    filled:    bool = False
    max_age_ms: int = 86_400_000

    @property
    def midpoint(self) -> float:
        return (self.bottom + self.top) / 2.0

    @property
    def size(self) -> float:
        return self.top - self.bottom

    def is_price_in_gap(self, price: float) -> bool:
        return self.bottom <= price <= self.top

    def is_active(self, now_ms: int) -> bool:
        return (not self.filled and
                now_ms - self.timestamp <= self.max_age_ms)

    def update_fill(self, candles: List[Dict]) -> None:
        """Track how much of the FVG has been filled by CLOSED candles.

        BUG-FVG-FILL-WICK FIX: the old code measured wick overlap (h/l range)
        against the gap.  In ICT, a candle's WICK touching the FVG is NOT a
        fill — only a CLOSE into the gap counts.  The fill percentage now
        measures how deeply the deepest CLOSE has penetrated the gap:

          Bullish FVG (gap sits below current action, price fills downward):
            fill = (top - best_close_inside) / size   (close moving down)
          Bearish FVG (gap sits above current action, price fills upward):
            fill = (best_close_inside - bottom) / size (close moving up)

        ICT mitigation threshold: 50% fill (close reached or passed midpoint).
        """
        if self.filled or self.size < 1e-10:
            self.filled = True
            return
        best_pen = 0.0
        for c in candles:
            cl = float(c['c'])
            if self.direction == "bullish":
                # Bullish FVG fills from TOP downward as price retraces into it
                if cl <= self.top:
                    pen = min(self.top - cl, self.size) / self.size
                    best_pen = max(best_pen, pen)
            else:
                # Bearish FVG fills from BOTTOM upward as price retraces into it
                if cl >= self.bottom:
                    pen = min(cl - self.bottom, self.size) / self.size
                    best_pen = max(best_pen, pen)
        self.fill_percentage = min(1.0, best_pen)
        if self.fill_percentage >= 0.50:   # ICT standard: 50% fill = mitigated
            self.filled = True


@dataclass
class LiquidityLevel:
    """BSL = Buy-Side Liquidity (equal highs, buy stops above).
       SSL = Sell-Side Liquidity (equal lows, sell stops below)."""
    price:       float
    level_type:  str   # "BSL" | "SSL"
    touch_count: int
    swept:       bool = False
    sweep_timestamp:        int  = 0
    displacement_confirmed: bool = False
    wick_rejection:         bool = False
    timeframe: str = "5m"

    @property
    def pool_type(self) -> str:
        """Backward-compat alias: EQH=BSL, EQL=SSL."""
        return "EQH" if self.level_type == "BSL" else "EQL"


@dataclass
class TFStructure:
    """Per-timeframe market structure snapshot."""
    timeframe:     str
    trend:         str   = "ranging"  # "bullish"|"bearish"|"ranging"
    last_sh:       float = 0.0        # last confirmed swing high
    last_sl_:      float = 0.0        # last confirmed swing low
    prev_sh:       float = 0.0
    prev_sl_:      float = 0.0
    bos_level:     float = 0.0
    bos_direction: str   = ""
    choch_level:   float = 0.0
    choch_bar_index: int = -1    # candle index (within the lb-slice) where CHoCH was detected; -1 = none
    range_high:    float = 0.0
    range_low:     float = 0.0
    equilibrium:   float = 0.0
    premium_discount: float = 0.5    # 0=deep discount, 1=deep premium


@dataclass
class AMDState:
    """Accumulation-Manipulation-Distribution cycle state."""
    phase:           str              # "ACCUMULATION"|"MANIPULATION"|"DISTRIBUTION"|"REACCUMULATION"|"REDISTRIBUTION"
    bias:            str              # "bullish"|"bearish"|"neutral"
    confidence:      float            # 0–1
    sweep_origin:    Optional[float] = None
    delivery_target: Optional[float] = None
    time_in_phase_ms: int = 0
    sweep_type:      str = ""         # "BSL" (ran buy stops) | "SSL" (ran sell stops)
    details:         str = ""


@dataclass
class MarketBias:
    """Multi-timeframe directional bias summary."""
    direction:  str              # "bullish"|"bearish"|"neutral"
    strength:   float            # 0–1
    tf_1d:      str  = "neutral"
    tf_4h:      str  = "neutral"
    tf_1h:      str  = "neutral"
    tf_15m:     str  = "neutral"
    pd_1d:      float = 0.5      # premium/discount on daily (0=discount)
    pd_4h:      float = 0.5
    amd_phase:  str  = "ACCUMULATION"
    amd_bias:   str  = "neutral"
    details:    str  = ""


@dataclass
class ICTConfluence:
    """Full ICT confluence result — all components explicit."""
    # Component scores (sum ≈ total before guards)
    structure_score: float = 0.0   # MTF trend alignment (0–0.30)
    amd_score:       float = 0.0   # AMD phase alignment  (0–0.25)
    pd_array_score:  float = 0.0   # PD array proximity   (0–0.25)
    liquidity_score: float = 0.0   # Sweep + pool stack   (0–0.15)
    session_score:   float = 0.0   # Kill zone / session  (0–0.05)
    total:           float = 0.0   # final (guarded)

    # Legacy fields — kept for backward compat with quant_strategy.py callers
    ob_score:       float = 0.0
    fvg_score:      float = 0.0
    sweep_score:    float = 0.0

    # Active objects
    active_ob:  Optional[OrderBlock]   = None
    active_fvg: Optional[FairValueGap] = None

    # Session context
    session_name: str = ""
    killzone:     str = ""

    # AMD / MTF context
    amd_phase:   str  = "ACCUMULATION"
    amd_bias:    str  = "neutral"
    in_discount: bool = False
    in_premium:  bool = False
    mtf_aligned: bool = False

    details: str = ""


# ═══════════════════════════════════════════════════════════════════════════
# MAIN ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class ICTEngine:
    """
    Full multi-timeframe ICT/SMC engine.

    Detects structure on 1D/4H/1H/15M/5M/1M simultaneously.
    AMD phase is derived from liquidity sweeps + structure.
    PD arrays (OB, FVG) scored by timeframe strength.
    """

    # ── Defaults (overridden by config) ───────────────────────────────────
    OB_MIN_IMPULSE_PCT    = 0.15   # minimum % body move to qualify as impulse
    OB_MIN_BODY_RATIO     = 0.40   # body/range ratio for impulse candle
    OB_IMPULSE_SIZE_MULT  = 1.30   # impulse must be N× larger than OB candle
    OB_MAX_AGE_MS         = 86_400_000     # 24 h   (5m/15m OBs)
    HTF_OB_MAX_AGE_MS     = 259_200_000    # 72 h   (1h/4h OBs)
    DAILY_OB_MAX_AGE_MS   = 1_296_000_000  # 15 days (1d OBs)
    FVG_MIN_SIZE_PCT       = 0.020   # minimum gap size as % of price
    FVG_MAX_AGE_MS         = 86_400_000
    LIQ_TOUCH_TOL_PCT      = 0.0020  # 0.20% tolerance for equal highs/lows
    SWEEP_DISP_MIN         = 0.40    # min body/range for displacement
    SWEEP_MAX_AGE_MS       = 7_200_000    # 2 h — swept pool is "fresh"
    AMD_MANIP_WINDOW_MS    = 900_000      # 15 min — manipulation phase
    AMD_DISTRIB_WINDOW_MS  = 5_400_000    # 90 min — distribution delivery

    # TF-specific OB base strengths (reflects institutional significance)
    TF_BASE_STRENGTH = {
        "1m": 45.0, "5m": 50.0, "15m": 75.0,
        "1h": 82.0, "4h": 90.0, "1d": 97.0,
    }

    # HTF OB threshold for htf_only filtering
    HTF_STRENGTH_THRESHOLD = 70.0

    # Kill zone hours (NY time = UTC + offset)
    KZ_ASIA_START   = 20; KZ_ASIA_END   = 24
    KZ_LONDON_START = 2;  KZ_LONDON_END  = 5
    KZ_NY_START     = 7;  KZ_NY_END     = 10

    def __init__(self):
        self.order_blocks_bull: Deque[OrderBlock]   = deque(maxlen=60)
        self.order_blocks_bear: Deque[OrderBlock]   = deque(maxlen=60)
        self.fvgs_bull:         Deque[FairValueGap] = deque(maxlen=60)
        self.fvgs_bear:         Deque[FairValueGap] = deque(maxlen=60)
        self.liquidity_pools:   Deque[LiquidityLevel] = deque(maxlen=80)
        self._registered_sweeps: Deque[Tuple] = deque(maxlen=300)

        # Per-TF structure snapshots
        self._tf: Dict[str, TFStructure] = {
            tf: TFStructure(timeframe=tf)
            for tf in ("1m", "5m", "15m", "1h", "4h", "1d")
        }

        self._amd = AMDState(phase="ACCUMULATION", bias="neutral", confidence=0.3)

        # Combined swing levels (for liquidity detection)
        self._swing_highs: List[float] = []
        self._swing_lows:  List[float] = []

        self._session  = ""
        self._killzone = ""

        self._last_update    = 0.0
        self._UPDATE_INTERVAL = 5.0
        self._initialized    = False

        # Config overrides
        self.ICT_REQUIRE_OB_OR_FVG = False
        self.OB_PROXIMITY_ATR      = 1.5
        self.FVG_PROXIMITY_ATR     = 0.8
        self.SWEEP_DISP_BONUS      = 0.12

        self._load_config()

    # ─────────────────────────────────────────────────────────────────────
    # CONFIG LOAD
    # ─────────────────────────────────────────────────────────────────────

    def _load_config(self):
        try:
            import config as cfg
            self.OB_MIN_IMPULSE_PCT   = getattr(cfg, 'OB_MIN_IMPULSE_PCT',          self.OB_MIN_IMPULSE_PCT)
            self.OB_MIN_BODY_RATIO    = getattr(cfg, 'OB_MIN_BODY_RATIO',           self.OB_MIN_BODY_RATIO)
            self.OB_IMPULSE_SIZE_MULT = getattr(cfg, 'OB_IMPULSE_SIZE_MULTIPLIER',  self.OB_IMPULSE_SIZE_MULT)
            self.OB_MAX_AGE_MS        = getattr(cfg, 'OB_MAX_AGE_MINUTES', 1440)    * 60_000
            self.HTF_OB_MAX_AGE_MS    = getattr(cfg, 'HTF_OB_MAX_AGE_MINUTES', 4320) * 60_000
            self.FVG_MIN_SIZE_PCT     = getattr(cfg, 'FVG_MIN_SIZE_PCT',             self.FVG_MIN_SIZE_PCT)
            self.FVG_MAX_AGE_MS       = getattr(cfg, 'FVG_MAX_AGE_MINUTES', 1440)   * 60_000
            self.LIQ_TOUCH_TOL_PCT    = getattr(cfg, 'LIQ_TOUCH_TOLERANCE_PCT', 0.20) / 100.0
            self.SWEEP_DISP_MIN       = getattr(cfg, 'SWEEP_DISPLACEMENT_MIN',       self.SWEEP_DISP_MIN)
            self.SWEEP_MAX_AGE_MS     = getattr(cfg, 'SWEEP_MAX_AGE_MINUTES', 120)  * 60_000
            self.KZ_ASIA_START        = getattr(cfg, 'KZ_ASIA_NY_START',    20)
            self.KZ_LONDON_START      = getattr(cfg, 'KZ_LONDON_NY_START',   2)
            self.KZ_LONDON_END        = getattr(cfg, 'KZ_LONDON_NY_END',     5)
            self.KZ_NY_START          = getattr(cfg, 'KZ_NY_NY_START',       7)
            self.KZ_NY_END            = getattr(cfg, 'KZ_NY_NY_END',        10)
            self.ICT_REQUIRE_OB_OR_FVG = getattr(cfg, 'ICT_REQUIRE_OB_OR_FVG', False)
            self.OB_PROXIMITY_ATR     = getattr(cfg, 'ICT_OB_PROXIMITY_ATR',    1.5)
            self.FVG_PROXIMITY_ATR    = getattr(cfg, 'ICT_FVG_PROXIMITY_ATR',   0.8)
            self.SWEEP_DISP_BONUS     = getattr(cfg, 'ICT_SWEEP_DISP_BONUS',   0.12)
        except ImportError:
            pass

    # ─────────────────────────────────────────────────────────────────────
    # RESET
    # ─────────────────────────────────────────────────────────────────────

    def reset_state(self):
        self.order_blocks_bull.clear()
        self.order_blocks_bear.clear()
        self.fvgs_bull.clear()
        self.fvgs_bear.clear()
        self.liquidity_pools.clear()
        self._registered_sweeps.clear()
        self._swing_highs.clear()
        self._swing_lows.clear()
        for tf in self._tf:
            self._tf[tf] = TFStructure(timeframe=tf)
        self._amd = AMDState(phase="ACCUMULATION", bias="neutral", confidence=0.3)
        self._initialized = False

    # ─────────────────────────────────────────────────────────────────────
    # MAIN UPDATE — called every 5s
    # ─────────────────────────────────────────────────────────────────────

    def update(self, candles_5m: List[Dict], candles_15m: List[Dict],
               price: float, now_ms: int,
               candles_1m:  Optional[List[Dict]] = None,
               candles_1h:  Optional[List[Dict]] = None,
               candles_4h:  Optional[List[Dict]] = None,
               candles_1d:  Optional[List[Dict]] = None) -> None:
        """
        Update all ICT structures from up to 6 timeframes.
        Throttled to once every 5 seconds.
        """
        now_s = now_ms / 1000.0
        if now_s - self._last_update < self._UPDATE_INTERVAL:
            return
        self._last_update = now_s

        if len(candles_5m) < 10:
            return

        # ── Session / Kill Zone ────────────────────────────────────────
        self._update_session(now_ms)

        # ── Per-TF Market Structure ────────────────────────────────────
        # Each TF analyzed independently — trend/BOS/CHoCH/PD
        if candles_1d and len(candles_1d) >= 6:
            self._tf["1d"] = self._analyze_structure(candles_1d, "1d", price)
        if candles_4h and len(candles_4h) >= 6:
            self._tf["4h"] = self._analyze_structure(candles_4h, "4h", price)
        if candles_1h and len(candles_1h) >= 6:
            self._tf["1h"] = self._analyze_structure(candles_1h, "1h", price)
        if len(candles_15m) >= 6:
            self._tf["15m"] = self._analyze_structure(candles_15m, "15m", price)
        if len(candles_5m) >= 6:
            self._tf["5m"] = self._analyze_structure(candles_5m, "5m", price)
        if candles_1m and len(candles_1m) >= 6:
            self._tf["1m"] = self._analyze_structure(candles_1m, "1m", price)

        # ── Combined swing points (for liquidity clustering) ───────────
        self._detect_swing_points(candles_5m, candles_15m)

        # ── Order Blocks — all timeframes ─────────────────────────────
        # 1m: micro-OBs for trail anchoring (short lookback, short life)
        if candles_1m and len(candles_1m) >= 5:
            self._detect_obs(candles_1m[-40:], price, now_ms, tf="1m",
                             base_str=45.0, max_age=1_800_000)
        # 5m: primary scalp structure
        self._detect_obs(candles_5m, price, now_ms, tf="5m",
                         base_str=50.0, max_age=self.OB_MAX_AGE_MS)
        # 15m: HTF entry anchor
        if len(candles_15m) >= 5:
            self._detect_obs(candles_15m, price, now_ms, tf="15m",
                             base_str=75.0, max_age=self.OB_MAX_AGE_MS * 2)
        # 1h: macro structure
        if candles_1h and len(candles_1h) >= 5:
            self._detect_obs(candles_1h, price, now_ms, tf="1h",
                             base_str=82.0, max_age=self.HTF_OB_MAX_AGE_MS)
        # 4h: institutional positioning
        if candles_4h and len(candles_4h) >= 5:
            self._detect_obs(candles_4h, price, now_ms, tf="4h",
                             base_str=90.0, max_age=self.HTF_OB_MAX_AGE_MS * 2)
        # 1d: highest-conviction structural levels
        if candles_1d and len(candles_1d) >= 5:
            self._detect_obs(candles_1d, price, now_ms, tf="1d",
                             base_str=97.0, max_age=self.DAILY_OB_MAX_AGE_MS)

        # ── Fair Value Gaps — key timeframes ──────────────────────────
        self._detect_fvgs(candles_5m, "5m", price, now_ms, self.OB_MAX_AGE_MS)
        if len(candles_15m) >= 5:
            self._detect_fvgs(candles_15m, "15m", price, now_ms, self.OB_MAX_AGE_MS * 2)
        if candles_1h and len(candles_1h) >= 5:
            self._detect_fvgs(candles_1h, "1h", price, now_ms, self.HTF_OB_MAX_AGE_MS)
        if candles_4h and len(candles_4h) >= 5:
            self._detect_fvgs(candles_4h, "4h", price, now_ms, self.HTF_OB_MAX_AGE_MS * 2)

        # ── FVG fill tracking ─────────────────────────────────────────
        self._update_fvg_fills(candles_5m)
        if candles_1m:
            self._update_fvg_fills(candles_1m[-30:])

        # ── Liquidity pool detection + sweep ─────────────────────────
        self._detect_liquidity_pools(price, now_ms)
        self._detect_sweeps(candles_5m, candles_15m, price, now_ms,
                            candles_1h=candles_1h)

        # ── OB mitigation tracking ────────────────────────────────────
        self._update_ob_mitigation(candles_5m)
        if candles_15m:
            self._update_ob_mitigation(candles_15m)

        # ── OB visit tracking ─────────────────────────────────────────
        self._update_ob_visits(price, now_ms)

        # ── AMD phase ─────────────────────────────────────────────────
        self._update_amd(price, now_ms)

        self._initialized = True

    # ─────────────────────────────────────────────────────────────────────
    # PER-TF MARKET STRUCTURE ANALYSIS
    # ─────────────────────────────────────────────────────────────────────

    def _analyze_structure(self, candles: List[Dict],
                            tf: str, price: float) -> TFStructure:
        """
        Swing → trend + BOS/CHoCH + premium/discount for one timeframe.

        Lookback: up to 60 candles (= 15h on 15m, 10 days on 4h).
        Swing detection: fractal with left=2, right=2.
        Only confirmed swings (candle[i+2] already closed) used.
        """
        out = TFStructure(timeframe=tf)
        if len(candles) < 8:
            return out

        lb = min(60, len(candles))
        recent = candles[-lb:]

        # ── Fractal swing detection ───────────────────────────────────
        highs: List[Tuple[int, float]] = []
        lows:  List[Tuple[int, float]] = []
        for i in range(2, len(recent) - 2):
            h = float(recent[i]['h'])
            l = float(recent[i]['l'])
            # BUG-SWING-1 FIX: all four neighbours must be strict >.
            # The old hybrid (>= on three, + tiny epsilon on one) lets
            # two identical adjacent highs both qualify, inflating swing
            # counts 2-5× in flat/choppy markets → wrong trend labels.
            if (h > float(recent[i-1]['h']) and h > float(recent[i-2]['h']) and
                    h > float(recent[i+1]['h']) and h > float(recent[i+2]['h'])):
                highs.append((i, h))
            if (l < float(recent[i-1]['l']) and l < float(recent[i-2]['l']) and
                    l < float(recent[i+1]['l']) and l < float(recent[i+2]['l'])):
                lows.append((i, l))

        # ── Trend from swing sequence ─────────────────────────────────
        sh = [s[1] for s in highs[-4:]]
        sl = [s[1] for s in lows[-4:]]

        trend = "ranging"
        if len(sh) >= 2 and len(sl) >= 2:
            hh = sh[-1] > sh[-2]
            hl = sl[-1] > sl[-2]
            lh = sh[-1] < sh[-2]
            ll = sl[-1] < sl[-2]
            if hh and hl:
                trend = "bullish"
            elif lh and ll:
                trend = "bearish"
        out.trend = trend

        # ── Last/prev swing levels ────────────────────────────────────
        if highs:
            out.last_sh = highs[-1][1]
            out.prev_sh = highs[-2][1] if len(highs) >= 2 else 0.0
        if lows:
            out.last_sl_ = lows[-1][1]
            out.prev_sl_ = lows[-2][1] if len(lows) >= 2 else 0.0

        # ── BOS (Break of Structure) ──────────────────────────────────
        # Use the last CLOSED candle (recent[-2]), not the still-forming
        # candle (recent[-1]).  Using recent[-1] causes premature BOS
        # signals on every tick of the live candle whose close hasn't
        # been confirmed yet.
        last_close = float(recent[-2]['c'])
        if out.last_sh > 0 and last_close > out.last_sh:
            out.bos_level     = out.last_sh
            out.bos_direction = "bullish"
        elif out.last_sl_ > 0 and last_close < out.last_sl_:
            out.bos_level     = out.last_sl_
            out.bos_direction = "bearish"

        # ── CHoCH (Change of Character) ───────────────────────────────
        # First higher-low in a downtrend or lower-high in an uptrend.
        # choch_bar_index is the candle index inside `recent` (0-based from the
        # start of the lb-slice) so HTFTrendFilter can compute staleness in bars.
        if trend == "bearish" and len(lows) >= 2 and lows[-1][1] > lows[-2][1]:
            out.choch_level     = lows[-1][1]
            out.choch_bar_index = lows[-1][0]   # fractal index within recent[]
        elif trend == "bullish" and len(highs) >= 2 and highs[-1][1] < highs[-2][1]:
            out.choch_level     = highs[-1][1]
            out.choch_bar_index = highs[-1][0]

        # ── Premium / Discount ────────────────────────────────────────
        h_max = max(float(c['h']) for c in recent)
        l_min = min(float(c['l']) for c in recent)
        out.range_high   = h_max
        out.range_low    = l_min
        out.equilibrium  = (h_max + l_min) / 2.0
        rng = h_max - l_min
        out.premium_discount = (price - l_min) / rng if rng > 1e-9 else 0.5

        return out

    # ─────────────────────────────────────────────────────────────────────
    # AMD PHASE DETECTION
    # ─────────────────────────────────────────────────────────────────────

    def _update_amd(self, price: float, now_ms: int) -> None:
        """
        AMD phase from liquidity sweep recency + structure.

        MANIPULATION  = sweep just happened (< 15 min)
        DISTRIBUTION  = delivering after sweep, WITH 5m BOS confirmation
        DISTRIBUTION* = 15-90 min but 5m BOS not yet printed → extended manip
        REACCUM/REDIS = mid-trend pause (BOS on 15m but 5m ranging)
        ACCUMULATION  = no recent sweep, price consolidating

        BUG-AMD-DIST-NO-BOS FIX: previously DISTRIBUTION was declared purely
        by elapsed time (15-90 min after sweep).  In ICT, the DISTRIBUTION
        phase is confirmed when the execution timeframe (5m) prints a BOS in
        the direction of the expected delivery.  Without that BOS, smart money
        is still in the Judas-swing / manipulation phase and counter-direction
        entries would be fighting the institutional move.

        BUG-AMD-DELIVERY-NEAREST FIX: delivery target now selects the most
        SIGNIFICANT opposing pool (weighted by touch_count and proximity),
        not simply the nearest one.  Higher touch_count = more clustered stops
        = stronger institutional delivery magnet.
        """
        swept = [p for p in self.liquidity_pools if p.swept]
        if not swept:
            self._amd = AMDState(
                phase="ACCUMULATION", bias="neutral", confidence=0.35,
                details="No sweep detected")
            return

        swept.sort(key=lambda p: p.sweep_timestamp, reverse=True)
        latest     = swept[0]
        age_ms     = now_ms - latest.sweep_timestamp
        sweep_type = latest.level_type

        bias = "bullish" if sweep_type == "SSL" else "bearish"

        # Confidence from sweep quality
        conf = 0.50
        if latest.displacement_confirmed: conf += 0.20
        if latest.wick_rejection:         conf += 0.10
        freshness = max(0.0, 1.0 - age_ms / (self.AMD_DISTRIB_WINDOW_MS * 2))
        conf = min(0.95, conf + freshness * 0.15)

        # 5m BOS check: DISTRIBUTION requires a confirmed break of structure
        # in the delivery direction on the execution timeframe.
        tf5m = self._tf.get("5m", TFStructure(timeframe="5m"))
        bos_in_delivery_dir = (
            (bias == "bullish" and tf5m.bos_direction == "bullish") or
            (bias == "bearish" and tf5m.bos_direction == "bearish")
        )

        # Phase determination
        if age_ms < self.AMD_MANIP_WINDOW_MS:
            phase   = "MANIPULATION"
            details = f"Judas swing {sweep_type} @ ${latest.price:.0f} | {age_ms//1000:.0f}s ago"
        elif age_ms < self.AMD_DISTRIB_WINDOW_MS:
            if bos_in_delivery_dir:
                phase   = "DISTRIBUTION"
                details = (f"Delivering after {sweep_type} sweep @ ${latest.price:.0f} | "
                           f"{age_ms//60000:.0f}m ago | 5m_BOS={tf5m.bos_direction}")
            else:
                # Sweep happened but 5m hasn't printed BOS yet — still in extended manip
                phase   = "MANIPULATION"
                conf    = max(conf - 0.10, 0.40)
                details = (f"Post-sweep {sweep_type} @ ${latest.price:.0f} | "
                           f"{age_ms//60000:.0f}m ago | awaiting 5m BOS")
        else:
            st = self._tf.get("15m", TFStructure(timeframe="15m"))
            s5 = self._tf.get("5m",  TFStructure(timeframe="5m"))
            if st.trend != "ranging" and s5.trend == "ranging":
                phase   = "REACCUMULATION" if st.trend == "bullish" else "REDISTRIBUTION"
                conf    = max(conf - 0.15, 0.30)
                details = f"Mid-trend pause | 15m:{st.trend} 5m:ranging"
            else:
                phase   = "ACCUMULATION"
                # AMD-ACCUMULATION-DECAY FIX (from previous session)
                excess_ms = age_ms - self.AMD_DISTRIB_WINDOW_MS
                decay     = max(0.0, 1.0 - excess_ms / (self.AMD_DISTRIB_WINDOW_MS * 2))
                conf      = max(conf * decay - 0.20, 0.20)
                if decay < 0.15:
                    bias = "neutral"
                details = f"Old {sweep_type} sweep {age_ms//60000:.0f}m ago"

        # Delivery target: most SIGNIFICANT opposing unswept pool.
        # BUG-AMD-DELIVERY-NEAREST FIX: weight by touch_count and proximity.
        # More clustered stops = stronger institutional magnet; nearer is also
        # preferred but high touch_count can outweigh extra distance.
        target = None
        unswept = [p for p in self.liquidity_pools if not p.swept]
        safe_atr = max(self._tf.get("5m", TFStructure("5m")).range_high -
                       self._tf.get("5m", TFStructure("5m")).range_low, 1.0) * 0.02

        def _pool_score(p: 'LiquidityLevel') -> float:
            dist = abs(p.price - price)
            return float(p.touch_count) * 2.0 / (1.0 + dist / max(safe_atr, 1.0))

        if bias == "bullish":
            bsl = [p for p in unswept if p.level_type == "BSL" and p.price > price]
            if bsl:
                target = max(bsl, key=_pool_score).price
        elif bias == "bearish":
            ssl = [p for p in unswept if p.level_type == "SSL" and p.price < price]
            if ssl:
                target = max(ssl, key=_pool_score).price

        self._amd = AMDState(
            phase=phase, bias=bias, confidence=conf,
            sweep_origin=latest.price, delivery_target=target,
            time_in_phase_ms=age_ms, sweep_type=sweep_type, details=details)

    # ─────────────────────────────────────────────────────────────────────
    # OB DETECTION (unified for all timeframes)
    # ─────────────────────────────────────────────────────────────────────

    def _detect_obs(self, candles: List[Dict], price: float, now_ms: int,
                    tf: str, base_str: float, max_age: int) -> None:
        """
        Unified OB detection for any timeframe.
        OB = last opposite-color candle immediately before a strong impulse.

        Strength = base_str (TF) + quality bonuses (BOS, displacement, wick).
        1D base=97, 4H=90, 1H=82, 15M=75, 5M=50, 1M=45.
        """
        if len(candles) < 5:
            return
        tol = price * 0.001
        min_impulse = self.OB_MIN_IMPULSE_PCT * (0.60 if tf == "1m" else 1.0)

        # Rolling prior highs/lows for BOS check
        prior_h: List[float] = []
        prior_l: List[float] = []
        for c in candles[:-2]:
            prior_h.append(float(c['h']))
            prior_l.append(float(c['l']))
        prior_h.sort(reverse=True)
        prior_l.sort()

        for i in range(2, len(candles) - 1):
            cur = candles[i]
            co, cc = float(cur['o']), float(cur['c'])
            ch, cl = float(cur['h']), float(cur['l'])
            ts = int(cur.get('t', now_ms))

            # BUG-OB-SINGLE-CANDLE-IMPULSE FIX: check up to 3 candles ahead.
            # Original code only looked at candles[i+1]; if the impulse arrived
            # 2-3 bars later (OB → doji → big move) the OB was silently missed.
            imp_up   = False
            imp_down = False
            nh = nl = no = nc = nr = nb = 0.0
            for _k in range(1, min(4, len(candles) - i)):
                nxt = candles[i + _k]
                no, nc = float(nxt['o']), float(nxt['c'])
                nh, nl = float(nxt['h']), float(nxt['l'])
                nr = nh - nl
                nb = abs(nc - no)
                if (nc > no and
                        (nc - no) / max(no, 1) * 100 >= min_impulse and
                        nb / max(nr, 1e-9) >= self.OB_MIN_BODY_RATIO):
                    imp_up = True
                    break
                if (nc < no and
                        (no - nc) / max(no, 1) * 100 >= min_impulse and
                        nb / max(nr, 1e-9) >= self.OB_MIN_BODY_RATIO):
                    imp_down = True
                    break

            def _score(bos, disp, wick, large) -> float:
                s = base_str
                if bos:   s += 10.0
                if disp:  s += 8.0
                if wick:  s += 5.0
                if large: s += 7.0
                return min(s, 100.0)

            # ── Bullish OB: bearish candle before bullish impulse ──────
            if imp_up and cc < co:
                bos  = bool(prior_h and any(nh > ph for ph in prior_h[:5]))
                disp = nr > 0 and nb / nr >= self.SWEEP_DISP_MIN
                wk   = cl < min(co, cc) and (ch - cl) > 0 and \
                       (min(co, cc) - cl) / (ch - cl) >= 0.20
                big  = nr >= self.OB_IMPULSE_SIZE_MULT * (ch - cl)
                s    = _score(bos, disp, wk, big)
                # BUG-OB-ZONE FIX: store the candle BODY (open-to-close),
                # not the full wick range.  Using wick-to-wick meant
                # contains_price() fired on wick touches above the body —
                # a non-event in ICT.  Bullish OB body = [close, open]
                # because this is a bearish candle (cc < co).
                ob_low, ob_high = cc, co
                if not any(abs(ob.low - ob_low) <= tol and abs(ob.high - ob_high) <= tol
                           for ob in self.order_blocks_bull):
                    self.order_blocks_bull.append(OrderBlock(
                        low=ob_low, high=ob_high, timestamp=ts, direction="bullish",
                        timeframe=tf, strength=s, bos_confirmed=bos,
                        has_displacement=disp, has_wick_rejection=wk,
                        max_age_ms=max_age))

            # ── Bearish OB: bullish candle before bearish impulse ──────
            if imp_down and cc > co:
                bos  = bool(prior_l and any(nl < pl for pl in prior_l[:5]))
                disp = nr > 0 and nb / nr >= self.SWEEP_DISP_MIN
                wk   = ch > max(co, cc) and (ch - cl) > 0 and \
                       (ch - max(co, cc)) / (ch - cl) >= 0.20
                big  = nr >= self.OB_IMPULSE_SIZE_MULT * (ch - cl)
                s    = _score(bos, disp, wk, big)
                # BUG-OB-ZONE FIX: store body [open, close] for bullish
                # candle (co < cc).
                ob_low, ob_high = co, cc
                if not any(abs(ob.low - ob_low) <= tol and abs(ob.high - ob_high) <= tol
                           for ob in self.order_blocks_bear):
                    self.order_blocks_bear.append(OrderBlock(
                        low=ob_low, high=ob_high, timestamp=ts, direction="bearish",
                        timeframe=tf, strength=s, bos_confirmed=bos,
                        has_displacement=disp, has_wick_rejection=wk,
                        max_age_ms=max_age))

    # ─────────────────────────────────────────────────────────────────────
    # FVG DETECTION
    # ─────────────────────────────────────────────────────────────────────

    def _detect_fvgs(self, candles: List[Dict], tf: str,
                     price: float, now_ms: int, max_age: int) -> None:
        """
        3-candle FVG:
          Bullish FVG (upward impulse): c3.low > c1.high
            → gap_bot = c1.high (h1), gap_top = c3.low (l3)
            → price attracted back DOWN to fill this upward imbalance.
          Bearish FVG (downward impulse): c1.low > c3.high
            → gap_bot = c3.high (h3), gap_top = c1.low (l1)
            → price attracted back UP to fill this downward imbalance.

        BUG-FVG-DIRECTION FIX: the original code had both definitions
        completely swapped.  It stored downward-move gaps (l1 > h3) in
        fvgs_bull and upward-move gaps (l3 > h1) in fvgs_bear, so every
        FVG confluence score pointed the wrong direction.
        """
        if len(candles) < 3:
            return
        min_sz = price * self.FVG_MIN_SIZE_PCT / 100.0
        tol    = min_sz * 0.5

        for i in range(1, len(candles) - 1):
            c1, c2, c3 = candles[i-1], candles[i], candles[i+1]
            h1, l1 = float(c1['h']), float(c1['l'])
            h3, l3 = float(c3['h']), float(c3['l'])
            ts = int(c2.get('t', now_ms))

            # Bullish FVG: upward impulse left a gap above c1 and below c3
            gap_bot = h1
            gap_top = l3
            if gap_top > gap_bot + min_sz:
                if not any(abs(f.bottom - gap_bot) < tol and abs(f.top - gap_top) < tol
                           for f in self.fvgs_bull):
                    self.fvgs_bull.append(FairValueGap(
                        bottom=gap_bot, top=gap_top, timestamp=ts,
                        direction="bullish", timeframe=tf, max_age_ms=max_age))

            # Bearish FVG: downward impulse left a gap below c1 and above c3
            gap_bot2 = h3
            gap_top2 = l1
            if gap_top2 > gap_bot2 + min_sz:
                if not any(abs(f.bottom - gap_bot2) < tol and abs(f.top - gap_top2) < tol
                           for f in self.fvgs_bear):
                    self.fvgs_bear.append(FairValueGap(
                        bottom=gap_bot2, top=gap_top2, timestamp=ts,
                        direction="bearish", timeframe=tf, max_age_ms=max_age))

    # ─────────────────────────────────────────────────────────────────────
    # FVG FILL TRACKING
    # ─────────────────────────────────────────────────────────────────────

    def _update_fvg_fills(self, candles: List[Dict]) -> None:
        check = candles[-10:] if len(candles) >= 10 else candles
        for fvg in list(self.fvgs_bull) + list(self.fvgs_bear):
            if not fvg.filled:
                fvg.update_fill(check)

    # ─────────────────────────────────────────────────────────────────────
    # SWING POINTS (combined for liquidity detection)
    # ─────────────────────────────────────────────────────────────────────

    def _detect_swing_points(self, candles_5m: List[Dict],
                              candles_15m: List[Dict]) -> None:
        """Fractal swings on 5m+15m for equal high/low liquidity clustering.

        BUG-SWING-LOOKBACK FIX: the original used a 5-bar left / 3-bar right
        asymmetric fractal.  A 5-bar left requirement is so restrictive that
        nearly all real ICT swing highs/lows fail to qualify → _swing_highs
        and _swing_lows stay nearly empty → _cluster_liq never finds 2+
        touches → liquidity_pools stays empty → no sweeps → AMD stuck on
        ACCUMULATION forever.  Fixed: symmetric 3/3, matching standard ICT
        swing identification.
        """
        self._swing_highs.clear()
        self._swing_lows.clear()
        for candles in (candles_5m, candles_15m):
            if len(candles) < 7:
                continue
            for i in range(3, len(candles) - 3):
                h = float(candles[i]['h'])
                l = float(candles[i]['l'])
                if (all(h > float(candles[j]['h']) for j in range(i-3, i)) and
                        all(h > float(candles[j]['h']) for j in range(i+1, i+4))):
                    self._swing_highs.append(h)
                if (all(l < float(candles[j]['l']) for j in range(i-3, i)) and
                        all(l < float(candles[j]['l']) for j in range(i+1, i+4))):
                    self._swing_lows.append(l)

    # ─────────────────────────────────────────────────────────────────────
    # LIQUIDITY POOL DETECTION
    # ─────────────────────────────────────────────────────────────────────

    def _detect_liquidity_pools(self, price: float, now_ms: int) -> None:
        """Cluster equal highs (BSL) and equal lows (SSL).

        BUG-LIQ-POOL-NO-EXPIRY FIX: the old code only appended to the deque
        without ever removing stale entries.  Unswept pools from days ago
        accumulated and distorted TP targeting and confluence scoring.

        Rebuilt each cycle:
          - Unswept pools: reconstructed from the CURRENT swing data (naturally
            bounded to recent candle history).  Old swings that fell out of the
            lookback window stop appearing in swing lists, so their pools are
            naturally dropped.
          - Swept pools: preserved for AMD delivery targeting, but expire after
            3× the distribution window (~4.5 hours) from the sweep timestamp.
        """
        tol = price * self.LIQ_TOUCH_TOL_PCT

        # Preserve swept pools within age limit (AMD needs them)
        max_swept_age = self.AMD_DISTRIB_WINDOW_MS * 3
        swept_keep = [p for p in self.liquidity_pools
                      if p.swept and now_ms - p.sweep_timestamp <= max_swept_age]

        # Rebuild from scratch
        self.liquidity_pools.clear()
        for p in swept_keep:
            self.liquidity_pools.append(p)

        # Fresh unswept pools from current swing data
        self._cluster_liq(self._swing_highs, "BSL", tol, price)
        self._cluster_liq(self._swing_lows,  "SSL", tol, price)

    def _cluster_liq(self, prices: List[float], kind: str,
                     tol: float, ref: float) -> None:
        if len(prices) < 2:
            return
        sp   = sorted(prices)
        used = set()
        for i, p1 in enumerate(sp):
            if i in used:
                continue
            cluster = [p1]
            for j in range(i + 1, len(sp)):
                if j not in used and abs(sp[j] - p1) <= tol:
                    cluster.append(sp[j])
                    used.add(j)
            if len(cluster) >= 2:
                avg = sum(cluster) / len(cluster)
                if not any(abs(lp.price - avg) <= tol and lp.level_type == kind
                           for lp in self.liquidity_pools):
                    self.liquidity_pools.append(LiquidityLevel(
                        price=avg, level_type=kind, touch_count=len(cluster)))

    # ─────────────────────────────────────────────────────────────────────
    # LIQUIDITY SWEEP DETECTION
    # ─────────────────────────────────────────────────────────────────────

    def _detect_sweeps(self, candles_5m: List[Dict], candles_15m: List[Dict],
                       price: float, now_ms: int,
                       candles_1h: Optional[List[Dict]] = None) -> None:
        """
        A sweep = wick THROUGH a liquidity pool + close on the OPPOSITE side.
        The wick proves stops were harvested. Displacement (strong body) =
        institutional confirmation that the sweep was intentional.
        """
        all_c = list(candles_5m[-25:]) + list(candles_15m[-10:])
        if candles_1h:
            all_c += list(candles_1h[-5:])

        for pool in list(self.liquidity_pools):
            if pool.swept:
                continue
            for c in all_c:
                h, l = float(c['h']), float(c['l'])
                cl, op = float(c['c']), float(c['o'])
                body = abs(cl - op)
                rng  = h - l
                key  = (round(pool.price, 0), int(c.get('t', 0)))
                if key in self._registered_sweeps:
                    continue

                if pool.level_type == "BSL" and h > pool.price and cl < pool.price:
                    disp = rng > 0 and body / rng >= self.SWEEP_DISP_MIN
                    pool.swept = True
                    # BUG-SWEEP-TS FIX: Use the CANDLE'S timestamp, not now_ms.
                    # Setting pool.sweep_timestamp = now_ms (wall-clock) makes
                    # every sweep detected during REST warmup appear brand-new
                    # regardless of when it actually happened on-chart.
                    # Consequence: a BSL sweep from 90+ minutes ago appears to
                    # be <15 minutes old → AMD phase = MANIPULATION → bot enters
                    # MANIP_no_confirmed_sweep lockout for the entire session
                    # because price has already moved past the OTE zone.
                    # Fix: use the candle's open-time so age_ms is correct.
                    pool.sweep_timestamp = int(c.get('t', now_ms))
                    pool.wick_rejection  = True
                    pool.displacement_confirmed = disp
                    self._registered_sweeps.append(key)
                    logger.info(
                        f"🔱 ICT BSL SWEPT @ ${pool.price:.0f} disp={disp} → BEARISH BIAS")
                    break

                elif pool.level_type == "SSL" and l < pool.price and cl > pool.price:
                    disp = rng > 0 and body / rng >= self.SWEEP_DISP_MIN
                    pool.swept = True
                    pool.sweep_timestamp = int(c.get('t', now_ms))  # candle ts, not now_ms
                    pool.wick_rejection  = True
                    pool.displacement_confirmed = disp
                    self._registered_sweeps.append(key)
                    logger.info(
                        f"🔱 ICT SSL SWEPT @ ${pool.price:.0f} disp={disp} → BULLISH BIAS")
                    break

    # ─────────────────────────────────────────────────────────────────────
    # OB MITIGATION TRACKING
    # ─────────────────────────────────────────────────────────────────────

    def _update_ob_mitigation(self, candles: List[Dict]) -> None:
        """
        Mark an OB as MITIGATED when a candle CLOSES beyond its far extreme.

        BUG-OB-MITIGATION-NO-CLOSE-CHECK FIX: the original code only used
        visit_count as a proxy for mitigation.  In ICT, an OB is MITIGATED
        when a candle CLOSES through the OB's opposite extreme — not when
        price merely touches or wicks through it.

          Bullish OB (body = [low, high], low=close, high=open of the bear candle):
            Mitigated when a candle CLOSES BELOW the OB low (bearish close
            pierces the bottom of the OB body).

          Bearish OB (body = [low, high], low=open, high=close of the bull candle):
            Mitigated when a candle CLOSES ABOVE the OB high (bullish close
            pierces the top of the OB body).

        Uses the last CLOSED candle ([-2]) — not the forming candle.
        """
        if len(candles) < 2:
            return
        last_close = float(candles[-2]['c'])

        for ob in self.order_blocks_bull:
            if ob.mitigated:
                continue
            if last_close < ob.low:
                ob.mitigated = True
                logger.debug(
                    f"OB mitigated: bull ${ob.low:.0f}-${ob.high:.0f} "
                    f"tf={ob.timeframe} close=${last_close:.0f} < ob.low")

        for ob in self.order_blocks_bear:
            if ob.mitigated:
                continue
            if last_close > ob.high:
                ob.mitigated = True
                logger.debug(
                    f"OB mitigated: bear ${ob.low:.0f}-${ob.high:.0f} "
                    f"tf={ob.timeframe} close=${last_close:.0f} > ob.high")

    # ─────────────────────────────────────────────────────────────────────
    # OB VISIT TRACKING
    # ─────────────────────────────────────────────────────────────────────

    def _update_ob_visits(self, price: float, now_ms: int) -> None:
        cooldown = {(True, True): 600_000, (True, False): 450_000,
                    (False, True): 450_000, (False, False): 300_000}
        for ob in list(self.order_blocks_bull) + list(self.order_blocks_bear):
            if not ob.is_active(now_ms) or not ob.contains_price(price):
                continue
            cd = cooldown[(ob.bos_confirmed, ob.has_displacement)]
            if now_ms - ob._last_visit_time >= cd:
                ob.visit_count       += 1
                ob._last_visit_time   = now_ms

    # ─────────────────────────────────────────────────────────────────────
    # SESSION / KILL ZONE
    # ─────────────────────────────────────────────────────────────────────

    def _update_session(self, now_ms: int) -> None:
        try:
            dt  = datetime.fromtimestamp(now_ms / 1000.0, tz=timezone.utc)
            uh  = dt.hour + dt.minute / 60.0
            dst = 3 <= dt.month <= 10
            ny  = (uh + (-4.0 if dst else -5.0)) % 24.0
            wd  = dt.weekday()

            self._killzone = ""
            if wd < 5:
                if ny >= self.KZ_ASIA_START or ny < 1.0:
                    self._killzone = "ASIA_KZ"
                elif self.KZ_LONDON_START <= ny < self.KZ_LONDON_END:
                    self._killzone = "LONDON_KZ"
                elif self.KZ_NY_START <= ny < self.KZ_NY_END:
                    self._killzone = "NY_KZ"

            # BUG-SESSION-TIMES FIX: correct UTC session windows.
            # Old: NY=12–21 UTC, London=7–17 UTC (wrong, heavily overlapping).
            # Fixed: NY=13:30–21:00 UTC, London=07:00–15:00 UTC,
            #        Asia=23:00–07:00 UTC (wraps midnight).
            if wd >= 5:
                self._session = "WEEKEND"
            elif 13.5 <= uh < 21.0:
                self._session = "NEW_YORK"
            elif 7.0  <= uh < 15.0:
                self._session = "LONDON"
            elif uh >= 23.0 or uh < 7.0:
                self._session = "ASIA"
            else:
                self._session = "OFF_HOURS"
        except Exception:
            self._session  = "UNKNOWN"
            self._killzone = ""

    # ─────────────────────────────────────────────────────────────────────
    # PUBLIC: MARKET BIAS
    # ─────────────────────────────────────────────────────────────────────

    def get_market_bias(self) -> MarketBias:
        """Consolidated MTF + AMD directional bias."""
        t1d  = self._tf["1d"]
        t4h  = self._tf["4h"]
        t1h  = self._tf["1h"]
        t15m = self._tf["15m"]

        def score(trend: str, side: str) -> float:
            if trend == side:       return 1.0
            elif trend == "ranging":return 0.5
            else:                   return 0.0

        bull = (score(t1d.trend, "bullish")  * 0.30 +
                score(t4h.trend, "bullish")  * 0.25 +
                score(t1h.trend, "bullish")  * 0.25 +
                score(t15m.trend,"bullish")  * 0.20)
        bear = (score(t1d.trend, "bearish")  * 0.30 +
                score(t4h.trend, "bearish")  * 0.25 +
                score(t1h.trend, "bearish")  * 0.25 +
                score(t15m.trend,"bearish")  * 0.20)

        ab = self._amd.bias
        if ab == "bullish": bull = min(1.0, bull + self._amd.confidence * 0.20)
        elif ab == "bearish": bear = min(1.0, bear + self._amd.confidence * 0.20)

        if bull > bear + 0.10:
            direction, strength = "bullish", bull
        elif bear > bull + 0.10:
            direction, strength = "bearish", bear
        else:
            direction, strength = "neutral", max(bull, bear)

        return MarketBias(
            direction=direction, strength=strength,
            tf_1d=t1d.trend, tf_4h=t4h.trend, tf_1h=t1h.trend, tf_15m=t15m.trend,
            pd_1d=t1d.premium_discount, pd_4h=t4h.premium_discount,
            amd_phase=self._amd.phase, amd_bias=ab,
            details=(f"1D:{t1d.trend[:4]} 4H:{t4h.trend[:4]} "
                     f"1H:{t1h.trend[:4]} 15M:{t15m.trend[:4]} "
                     f"AMD:{self._amd.phase[:4]}({ab[:4]}) "
                     f"conf={self._amd.confidence:.2f}"))

    def get_amd_state(self) -> AMDState:
        return self._amd

    # ─────────────────────────────────────────────────────────────────────
    # PUBLIC: CONFLUENCE SCORING
    # ─────────────────────────────────────────────────────────────────────

    def get_confluence(self, side: str, price: float,
                       now_ms: int, atr: float = 0.0) -> ICTConfluence:
        """
        Industry-grade ICT confluence score — 5 independent components.

        1. STRUCTURE ALIGNMENT (0–0.30)
           Per major TF (4H, 1H, 15M): +0.10 each if trend = side direction.
           1D alignment adds +0.05 bonus. Ranging TFs add +0.04 each (neutral).

        2. AMD PHASE ALIGNMENT (0–0.25)
           DISTRIBUTION + matching bias:    +0.25
           MANIPULATION + matching bias:    +0.20
           REACCUM/REDIS + matching bias:   +0.12
           Baseline:                        +0.05
           AMD actively opposing trade:     −0.05 penalty

        3. PD ARRAY PROXIMITY (0–0.25)
           Inside OB (OTE):                +0.15 × quality_multiplier
           Inside OB (body):               +0.10 × quality_multiplier
           Near OB (within OB_PROXIMITY):  partial decaying credit
           Inside FVG:                     +0.10 × freshness
           FVG+OB overlap bonus:           +0.05

        4. LIQUIDITY STACK (0–0.15)
           Recent sweep aligning with trade: up to +0.15 (quality-weighted)
           3+ stacked unswept pools ahead:  +0.03 bonus

        5. SESSION / KILL ZONE (0–0.05)
           Active KZ:   +0.05
           London/NY:   +0.03
           Asia:        +0.01

        Guards:
           No OB AND no sweep → cap total at 0.30
           ICT_REQUIRE_OB_OR_FVG AND neither → cap at 0.20
        """
        if not self._initialized:
            return ICTConfluence(total=0.0, details="not initialized")

        details: List[str] = []
        out = ICTConfluence()

        t4h  = self._tf["4h"]
        t1h  = self._tf["1h"]
        t15m = self._tf["15m"]
        t1d  = self._tf["1d"]

        # ── 1. Structure ──────────────────────────────────────────────
        ss = 0.0
        for tf_s, weight, tf_obj in (("4H", 0.10, t4h),
                                      ("1H", 0.10, t1h),
                                      ("15M",0.10, t15m)):
            if tf_obj.trend == side:
                ss += weight
                details.append(f"{tf_s}:{side[:4]}")
            elif tf_obj.trend == "ranging":
                ss += 0.04
        if t1d.trend == side:
            ss = min(0.30, ss + 0.05)
            details.append(f"1D:{side[:4]}")
        out.structure_score = min(0.30, ss)

        # ── 2. AMD ────────────────────────────────────────────────────
        amd = self._amd
        matches = ((side == "long"  and amd.bias == "bullish") or
                   (side == "short" and amd.bias == "bearish"))
        if matches:
            if   amd.phase == "DISTRIBUTION":     out.amd_score = 0.25
            elif amd.phase == "MANIPULATION":      out.amd_score = 0.20
            elif amd.phase in ("REACCUMULATION",
                               "REDISTRIBUTION"):  out.amd_score = 0.12
            else:                                  out.amd_score = 0.05
            details.append(f"AMD:{amd.phase[:5]}({amd.bias[:4]})")
        elif amd.phase in ("DISTRIBUTION", "MANIPULATION"):
            out.amd_score = -0.05   # actively opposing → small penalty

        # ── 3. PD Array ───────────────────────────────────────────────
        pd = 0.0
        active_ob  = None
        active_fvg = None
        conf_sweep = False

        # OB scoring — find best active OB
        obs_dir = self.order_blocks_bull if side == "long" else self.order_blocks_bear
        active_obs = sorted([o for o in obs_dir if o.is_active(now_ms)],
                            key=lambda o: o.strength, reverse=True)

        for ob in active_obs:
            if ob.contains_price(price) or ob.in_optimal_zone(price):
                in_ote = ob.in_optimal_zone(price)
                base   = 0.85 if in_ote else 0.55
                vm     = ob.virgin_multiplier()
                vpen   = max(0.5, 1.0 - ob.visit_count * 0.25)
                bos_b  = 0.15 if ob.bos_confirmed else 0.0
                disp_b = 0.10 if ob.has_displacement else 0.0
                raw    = min(base * vm * vpen + bos_b + disp_b, 1.0)
                pd     = max(pd, raw * 0.15)
                active_ob = ob
                tag = "OTE" if in_ote else "BODY"
                q   = ("BOS+DISP" if (ob.bos_confirmed and ob.has_displacement)
                       else ("BOS" if ob.bos_confirmed
                             else ("DISP" if ob.has_displacement else "RAW")))
                details.append(
                    f"OB_{tag}_{q} ${ob.low:.0f}-${ob.high:.0f} "
                    f"s={ob.strength:.0f} v={ob.visit_count} tf={ob.timeframe}")
                break

        # Proximity OB (price near but not inside)
        if active_ob is None and atr > 1e-10:
            for ob in active_obs:
                if side == "long":
                    if ob.high < price:
                        da = (price - ob.high) / atr
                        if da <= self.OB_PROXIMITY_ATR:
                            pf = 1.0 - da / self.OB_PROXIMITY_ATR
                            s  = min(0.40 * pf * ob.virgin_multiplier(), 0.12)
                            if s > pd:
                                pd = s; active_ob = ob
                                details.append(
                                    f"OB_PROX({da:.1f}ATR) ${ob.low:.0f}-${ob.high:.0f} tf={ob.timeframe}")
                else:
                    if ob.low > price:
                        da = (ob.low - price) / atr
                        if da <= self.OB_PROXIMITY_ATR:
                            pf = 1.0 - da / self.OB_PROXIMITY_ATR
                            s  = min(0.40 * pf * ob.virgin_multiplier(), 0.12)
                            if s > pd:
                                pd = s; active_ob = ob
                                details.append(
                                    f"OB_PROX({da:.1f}ATR) ${ob.low:.0f}-${ob.high:.0f} tf={ob.timeframe}")

        # FVG scoring
        fvgs_dir = self.fvgs_bull if side == "long" else self.fvgs_bear
        act_fvgs = [f for f in fvgs_dir if f.is_active(now_ms)]
        for fvg in act_fvgs:
            if fvg.is_price_in_gap(price):
                fresh = 1.0 - fvg.fill_percentage
                fvg_r = 0.50 + 0.30 * fresh
                if active_ob is not None:
                    fvg_r = min(fvg_r + 0.20, 1.0)
                    details.append(f"FVG+OB tf={fvg.timeframe}")
                else:
                    details.append(f"FVG ${fvg.bottom:.0f}-${fvg.top:.0f} tf={fvg.timeframe}")
                pd += fvg_r * 0.10
                active_fvg = fvg
                break

        # Proximity FVG — price is approaching but hasn't entered yet
        # BUG-FVG-PROX-DIR FIX: the old code had the direction inverted.
        #   LONG entry: look for bullish FVG BELOW current price (price is
        #     above the FVG, approaching from above — the FVG acts as support
        #     for the retracement entry).  Check: fvg.top < price.
        #   SHORT entry: look for bearish FVG ABOVE current price (price is
        #     below the FVG, approaching from below — FVG acts as resistance).
        #     Check: fvg.bottom > price.
        # The original code had these REVERSED, scoring confluence for FVGs
        # that were on the WRONG side of price entirely.
        if active_fvg is None and atr > 1e-10:
            for fvg in act_fvgs:
                if side == "long" and fvg.top < price:
                    # FVG is below — price is above it, could retrace into it
                    da = (price - fvg.top) / atr
                    if da <= self.FVG_PROXIMITY_ATR:
                        pf = 1.0 - da / self.FVG_PROXIMITY_ATR
                        s  = min(0.35 * pf * (1 - fvg.fill_percentage) * 0.10, 0.035)
                        pd += s; active_fvg = fvg
                        details.append(f"FVG_PROX({da:.1f}ATR) tf={fvg.timeframe}")
                        break
                elif side == "short" and fvg.bottom > price:
                    # FVG is above — price is below it, could retrace into it
                    da = (fvg.bottom - price) / atr
                    if da <= self.FVG_PROXIMITY_ATR:
                        pf = 1.0 - da / self.FVG_PROXIMITY_ATR
                        s  = min(0.35 * pf * (1 - fvg.fill_percentage) * 0.10, 0.035)
                        pd += s; active_fvg = fvg
                        details.append(f"FVG_PROX({da:.1f}ATR) tf={fvg.timeframe}")
                        break

        out.pd_array_score = min(0.25, pd)
        out.active_ob  = active_ob
        out.active_fvg = active_fvg
        # Legacy compat
        out.ob_score  = (out.pd_array_score / 0.15) if active_ob else 0.0
        out.fvg_score = (1.0 - active_fvg.fill_percentage) if active_fvg else 0.0

        # ── 4. Liquidity ──────────────────────────────────────────────
        liq = 0.0
        for pool in reversed(list(self.liquidity_pools)):
            if not pool.swept:
                continue
            age = now_ms - pool.sweep_timestamp
            if age > self.SWEEP_MAX_AGE_MS:
                continue
            fresh = max(0.0, 1.0 - age / self.SWEEP_MAX_AGE_MS)
            aligns = ((side == "long"  and pool.level_type == "SSL") or
                      (side == "short" and pool.level_type == "BSL"))
            base = 0.10 * fresh
            if pool.displacement_confirmed: base += 0.03
            if pool.wick_rejection:         base += 0.02
            if aligns:
                base += 0.05
                details.append(
                    f"Sweep {pool.level_type} ${pool.price:.0f} "
                    f"{'disp' if pool.displacement_confirmed else ''}")
                conf_sweep = pool.displacement_confirmed and pool.wick_rejection
            liq = max(liq, base)
            break   # only the most recent sweep

        # Stacked pools ahead
        unswept = [p for p in self.liquidity_pools if not p.swept]
        pools_ahead = [p for p in unswept
                       if ((side == "long"  and p.level_type == "BSL" and p.price > price) or
                           (side == "short" and p.level_type == "SSL" and p.price < price))]
        if len(pools_ahead) >= 3:
            liq = min(0.15, liq + 0.03)
            details.append(f"Stacked {len(pools_ahead)} BSL/SSL pools ahead")

        out.liquidity_score = min(0.15, liq)
        out.sweep_score = liq  # legacy

        # ── 5. Session ────────────────────────────────────────────────
        if self._killzone:
            out.session_score = 0.05
            details.append(f"KZ={self._killzone}")
        elif self._session in ("NEW_YORK", "LONDON"):
            out.session_score = 0.03
            details.append(f"Session={self._session}")
        elif self._session == "ASIA":
            out.session_score = 0.01
        out.session_name = self._session
        out.killzone     = self._killzone

        # ── Total ─────────────────────────────────────────────────────
        # BUG-AMD-OPPOSING-PENALTY-NULLIFIED FIX: the old code used
        # max(0.0, out.amd_score) which silently floored the -0.05 penalty
        # to zero, making opposing AMD have NO effect on the confluence total.
        # The penalty is intentional — actively opposing AMD should reduce
        # the total score to discourage entries against the delivery.
        raw = (out.structure_score +
               out.amd_score +       # allow negative AMD penalty to apply
               out.pd_array_score +
               out.liquidity_score +
               out.session_score)

        if conf_sweep:
            raw = min(raw + self.SWEEP_DISP_BONUS, 1.0)
            details.append(f"SWEEP_DISP_BONUS+{self.SWEEP_DISP_BONUS:.2f}")

        # Guards
        has_ob    = active_ob  is not None
        has_sweep = out.liquidity_score > 0.05
        if not has_ob and not has_sweep:
            raw = min(raw, 0.30)
        if self.ICT_REQUIRE_OB_OR_FVG and not has_ob and active_fvg is None:
            raw = min(raw, 0.20)
            details.append("REQUIRE_OB_OR_FVG_CAP")

        out.total = min(1.0, raw)

        # AMD / premium-discount context
        out.amd_phase   = amd.phase
        out.amd_bias    = amd.bias
        out.in_discount = t4h.premium_discount < 0.40
        out.in_premium  = t4h.premium_discount > 0.60
        out.mtf_aligned = sum([
            1 if t1d.trend  == side else (0.5 if t1d.trend  == "ranging" else 0),
            1 if t4h.trend  == side else (0.5 if t4h.trend  == "ranging" else 0),
            1 if t1h.trend  == side else (0.5 if t1h.trend  == "ranging" else 0),
            1 if t15m.trend == side else (0.5 if t15m.trend == "ranging" else 0),
        ]) >= 2.5

        out.details = " | ".join(details) if details else "no ICT structure"
        return out

    # ─────────────────────────────────────────────────────────────────────
    # PUBLIC: OB SL LEVEL
    # ─────────────────────────────────────────────────────────────────────

    def get_ob_sl_level(self, side: str, price: float, atr: float,
                         now_ms: int,
                         htf_only: bool = False) -> Optional[float]:
        """
        OB-anchored SL placement.

        htf_only=True (entry): 15m+ OBs (strength ≥ 70), visit_count ≤ 1.
          A v=2 OB is a consumed zone — SL placed there fires on the breakdown.
        htf_only=False (trail): all active OBs, sorted by proximity to price.

        FVG escape + liquidity-pool escape applied before returning.
        """
        buf     = 0.30 * atr
        fvg_buf = 0.20 * atr
        liq_buf = 0.30 * atr
        max_d   = 4.0 * atr
        min_d   = 0.5 * atr

        obs   = self.order_blocks_bull if side == "long" else self.order_blocks_bear
        cands: List[Tuple[float, float, float]] = []

        for ob in obs:
            if not ob.is_active(now_ms):
                continue
            if htf_only and ob.strength < self.HTF_STRENGTH_THRESHOLD:
                continue
            if htf_only and ob.visit_count >= 2:
                logger.debug(
                    f"OB SL skip v={ob.visit_count} ${ob.low:.0f}-${ob.high:.0f} tf={ob.timeframe}")
                continue

            if side == "long"  and ob.low < price:
                sl = ob.low  - buf
            elif side == "short" and ob.high > price:
                sl = ob.high + buf
            else:
                continue

            # FVG escape
            trap_fvgs = self.fvgs_bear if side == "short" else self.fvgs_bull
            for fvg in trap_fvgs:
                if fvg.filled or not fvg.is_active(now_ms):
                    continue
                if fvg.bottom <= sl <= fvg.top:
                    sl = (fvg.top + fvg_buf if side == "short"
                          else fvg.bottom - fvg_buf)
                    break

            # Pool escape
            for pool in self.liquidity_pools:
                if pool.swept:
                    continue
                if side == "short" and pool.level_type == "BSL":
                    if abs(sl - pool.price) < liq_buf and sl < pool.price:
                        sl = pool.price + liq_buf
                elif side == "long" and pool.level_type == "SSL":
                    if abs(sl - pool.price) < liq_buf and sl > pool.price:
                        sl = pool.price - liq_buf

            dist = abs(sl - price)
            if min_d <= dist <= max_d:
                cands.append((sl, ob.strength, dist))

        if not cands:
            return None

        cands.sort(key=lambda x: (-x[1], x[2]))
        sl_f = cands[0][0]
        if (side == "long" and sl_f < price) or (side == "short" and sl_f > price):
            return sl_f
        return None

    # ─────────────────────────────────────────────────────────────────────
    # PUBLIC: STRUCTURAL TP TARGETS
    # ─────────────────────────────────────────────────────────────────────

    def get_structural_tp_targets(self, side: str, price: float, atr: float,
                                   now_ms: int, min_dist: float, max_dist: float,
                                   htf_only: bool = False) -> List[Tuple[float, float, str]]:
        """
        ICT TP candidates in conviction order:
          6.0+  Swept liquidity origin (delivery target after manipulation)
          5.0+  Unfilled FVG (imbalance magnetism)
          4.0+  Virgin OB in path (institutional footprint)
          3.5+  Unswept liquidity pool (stop-hunt magnet)
        """
        _htf = self.HTF_STRENGTH_THRESHOLD
        cands: List[Tuple[float, float, str]] = []

        # ── Swept liquidity origins ───────────────────────────────────
        for pool in self.liquidity_pools:
            if not pool.swept:
                continue
            age = now_ms - pool.sweep_timestamp
            if age > self.SWEEP_MAX_AGE_MS:
                continue
            level = pool.price
            dist  = (price - level if side == "short" else level - price)
            if not (min_dist <= dist <= max_dist):
                continue
            if ((side == "short" and pool.level_type == "BSL" and level < price) or
                    (side == "long"  and pool.level_type == "SSL" and level > price)):
                fresh = max(0.0, 1.0 - age / self.SWEEP_MAX_AGE_MS)
                score = 6.0 * (0.7 + 0.3 * fresh)
                if pool.displacement_confirmed: score += 0.5
                cands.append((level, score, f"SweepOrigin_{pool.level_type}@${level:.0f}"))

        # ── Open FVGs in trade direction ──────────────────────────────
        fvgs_t = self.fvgs_bull if side == "short" else self.fvgs_bear
        for fvg in fvgs_t:
            if fvg.filled or not fvg.is_active(now_ms):
                continue
            ne = fvg.top    if side == "short" else fvg.bottom
            fe = fvg.bottom if side == "short" else fvg.top
            dn = price - ne if side == "short" else ne - price
            df = price - fe if side == "short" else fe - price
            if not (min_dist <= dn <= max_dist):
                continue
            fresh = 1.0 - fvg.fill_percentage
            sf    = min(fvg.size / max(atr * 0.5, 1.0), 2.0)
            score = 5.0 * fresh * (0.6 + 0.4 * sf)
            cands.append((ne, score,
                          f"FVG_near@${ne:.0f}(fill={fvg.fill_percentage:.0%}) tf={fvg.timeframe}"))
            if min_dist <= df <= max_dist:
                cands.append((fe, score * 0.85, f"FVG_far@${fe:.0f} tf={fvg.timeframe}"))

        # ── Virgin OBs in path ────────────────────────────────────────
        obs_t = self.order_blocks_bull if side == "short" else self.order_blocks_bear
        for ob in obs_t:
            if not ob.is_active(now_ms) or ob.visit_count > 0:
                continue
            if htf_only and ob.strength < _htf:
                continue
            level = ob.midpoint
            dist  = price - level if side == "short" else level - price
            if not (min_dist <= dist <= max_dist):
                continue
            score = 4.0 * ob.virgin_multiplier() * (ob.strength / 100.0)
            if ob.bos_confirmed: score += 0.5
            cands.append((level, score,
                          f"VirginOB@${level:.0f} s={ob.strength:.0f} tf={ob.timeframe}"))

        # ── Unswept liquidity pools as targets ────────────────────────
        for pool in self.liquidity_pools:
            if pool.swept:
                continue
            level = pool.price
            dist  = price - level if side == "short" else level - price
            if not (min_dist <= dist <= max_dist):
                continue
            if side == "short" and pool.level_type == "SSL" and level < price:
                score = 3.5 + min(pool.touch_count * 0.3, 1.0)
                cands.append((level, score, f"SSL_pool@${level:.0f} t={pool.touch_count}"))
            elif side == "long" and pool.level_type == "BSL" and level > price:
                score = 3.5 + min(pool.touch_count * 0.3, 1.0)
                cands.append((level, score, f"BSL_pool@${level:.0f} t={pool.touch_count}"))

        return cands

    # ─────────────────────────────────────────────────────────────────────
    # PUBLIC: TRAIL SL PATH CHECK
    # ─────────────────────────────────────────────────────────────────────

    def check_sl_path_for_structure(self, pos_side: str, current_sl: float,
                                     new_sl: float, now_ms: int,
                                     max_ob_visits: int = 1,
                                     max_fvg_fill: float = 0.30) -> Tuple[bool, str]:
        """Block trailing SL from crossing fresh virgin ICT structure."""
        if pos_side == "long":
            for ob in self.order_blocks_bull:
                if not ob.is_active(now_ms) or ob.visit_count > max_ob_visits:
                    continue
                if current_sl < ob.low < new_sl or current_sl < ob.high < new_sl:
                    return True, f"Virgin bull OB @ ${ob.midpoint:.0f} tf={ob.timeframe}"
            for fvg in self.fvgs_bull:
                if not fvg.is_active(now_ms) or fvg.fill_percentage > max_fvg_fill:
                    continue
                if current_sl < fvg.bottom < new_sl or current_sl < fvg.top < new_sl:
                    return True, f"Fresh bull FVG @ ${fvg.midpoint:.0f} tf={fvg.timeframe}"
        else:
            for ob in self.order_blocks_bear:
                if not ob.is_active(now_ms) or ob.visit_count > max_ob_visits:
                    continue
                if new_sl < ob.low < current_sl or new_sl < ob.high < current_sl:
                    return True, f"Virgin bear OB @ ${ob.midpoint:.0f} tf={ob.timeframe}"
            for fvg in self.fvgs_bear:
                if not fvg.is_active(now_ms) or fvg.fill_percentage > max_fvg_fill:
                    continue
                if new_sl < fvg.bottom < current_sl or new_sl < fvg.top < current_sl:
                    return True, f"Fresh bear FVG @ ${fvg.midpoint:.0f} tf={fvg.timeframe}"
        return False, ""

    # ─────────────────────────────────────────────────────────────────────
    # PUBLIC: STATUS
    # ─────────────────────────────────────────────────────────────────────

    def get_status(self) -> Dict:
        return {
            "ob_bull":       len([o for o in self.order_blocks_bull if o.visit_count < 3]),
            "ob_bear":       len([o for o in self.order_blocks_bear if o.visit_count < 3]),
            "fvg_bull":      len([f for f in self.fvgs_bull if not f.filled]),
            "fvg_bear":      len([f for f in self.fvgs_bear if not f.filled]),
            "liq_pools":     len(self.liquidity_pools),
            "sweeps_active": len([p for p in self.liquidity_pools if p.swept]),
            "session":       self._session,
            "killzone":      self._killzone or "none",
            "amd_phase":     self._amd.phase,
            "amd_bias":      self._amd.bias,
            "amd_conf":      round(self._amd.confidence, 2),
        }

    def get_full_status(self, price: float, atr: float, now_ms: int) -> Dict:
        """Full snapshot for /structures Telegram command."""
        a = max(atr, 1e-9)

        def _ob(ob: OrderBlock) -> Dict:
            dist = ob.midpoint - price
            _mid = ob.midpoint
            _vc  = ob.visit_count
            return {
                # Primary keys (new canonical names)
                "low": ob.low, "high": ob.high,
                "midpoint": _mid, "mid": _mid,          # both aliases — controller uses midpoint
                "strength": ob.strength,
                "visit_count": _vc, "visits": _vc,      # both aliases — controller uses visit_count
                "tf": ob.timeframe, "bos": ob.bos_confirmed,
                "in_ob": ob.contains_price(price), "in_ote": ob.in_optimal_zone(price),
                "dist_pts": dist, "dist_atr": round(abs(dist)/a, 2),
                "age_min":  round((now_ms - ob.timestamp)/60_000, 1),
                "tags": (["DISP"] if ob.has_displacement else []) +
                        (["WR"]   if ob.has_wick_rejection else []) +
                        (["VRGN"] if ob.visit_count == 0 else []),
            }

        def _fvg(fvg: FairValueGap) -> Dict:
            dist = fvg.midpoint - price
            _bot  = fvg.bottom
            _sz   = round(fvg.size, 1)
            _fill = round(fvg.fill_percentage, 2)
            return {
                # Both old short names and full canonical names for controller compat
                "direction": fvg.direction, "dir": fvg.direction,
                "tf": fvg.timeframe,
                "bottom": _bot, "bot": _bot,
                "top": fvg.top,
                "size": _sz, "sz": _sz,
                "fill_pct": _fill, "fill": _fill,
                "in_gap": fvg.is_price_in_gap(price),
                "dist_pts": dist, "dist_atr": round(abs(dist)/a, 2),
                "age_min": round((now_ms - fvg.timestamp)/60_000, 1),
            }

        # Build MTF summary
        mtf = {}
        for tf_k, st in self._tf.items():
            mtf[tf_k] = {
                "trend": st.trend,
                "pd":    round(st.premium_discount, 2),
                "eq":    round(st.equilibrium, 1),
                "range": f"${st.range_low:.0f}-${st.range_high:.0f}",
                "bos":   (f"${st.bos_level:.0f} {st.bos_direction}"
                          if st.bos_level > 0 else "none"),
                "choch": (f"${st.choch_level:.0f}" if st.choch_level > 0 else "none"),
            }

        bull_obs  = sorted([_ob(o) for o in self.order_blocks_bull if o.is_active(now_ms)],
                           key=lambda x: abs(x["dist_pts"]))
        bear_obs  = sorted([_ob(o) for o in self.order_blocks_bear if o.is_active(now_ms)],
                           key=lambda x: abs(x["dist_pts"]))
        bull_fvgs = sorted([_fvg(f) for f in self.fvgs_bull if f.is_active(now_ms)],
                           key=lambda x: abs(x["dist_pts"]))
        bear_fvgs = sorted([_fvg(f) for f in self.fvgs_bear if f.is_active(now_ms)],
                           key=lambda x: abs(x["dist_pts"]))

        liq_a, liq_s = [], []
        for p in self.liquidity_pools:
            _dist_val = round(p.price - price, 1)
            # Include both old short names and canonical names for controller compat
            e = {
                "type":       p.level_type,
                "pool_type":  p.pool_type,          # "EQH" / "EQL" — controller uses pool_type
                "price":      p.price,
                "touches":    p.touch_count,
                "touch_count": p.touch_count,       # controller uses touch_count
                "dist":       _dist_val,
                "dist_pts":   _dist_val,             # controller uses dist_pts
            }
            if p.swept:
                _age = round((now_ms - p.sweep_timestamp)/60_000, 1) if p.sweep_timestamp else None
                e.update({
                    "disp":         p.displacement_confirmed,
                    "displacement": p.displacement_confirmed,   # controller uses displacement
                    "wick":         p.wick_rejection,
                    "wick_rejection": p.wick_rejection,         # controller uses wick_rejection
                    "age_min":      _age,
                    "sweep_age_min": _age,                      # controller uses sweep_age_min
                })
                liq_s.append(e)
            else:
                liq_a.append(e)

        return {
            "counts": {"ob_bull": len(bull_obs), "ob_bear": len(bear_obs),
                       "fvg_bull": len(bull_fvgs), "fvg_bear": len(bear_fvgs),
                       "liq_active": len(liq_a), "liq_swept": len(liq_s)},
            "session":      self._session,
            "killzone":     self._killzone or "",
            "amd": {"phase": self._amd.phase, "bias": self._amd.bias,
                    "conf":  round(self._amd.confidence, 2),
                    "sweep_origin": self._amd.sweep_origin,
                    "delivery_target": self._amd.delivery_target,
                    "details": self._amd.details},
            "mtf": mtf,
            "bull_obs":  bull_obs[:6], "bear_obs":  bear_obs[:6],
            "bull_fvgs": bull_fvgs[:6], "bear_fvgs": bear_fvgs[:6],
            "liq_active": sorted(liq_a, key=lambda x: abs(x["dist"]))[:10],
            "liq_swept":  sorted(liq_s, key=lambda x: abs(x.get("age_min") or 9999))[:5],
            "swing_highs": sorted([h for h in self._swing_highs if h > price])[:6],
            "swing_lows":  sorted([l for l in self._swing_lows  if l < price],
                                  reverse=True)[:6],
        }

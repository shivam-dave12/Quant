"""
QUANT STRATEGY v4.9 — ICT-ANCHORED TRAILING SL + STRUCTURAL TP
==============================================================================
Entry: Wait for overextension from VWAP + order flow divergence, then fade.
SL: Behind swing structure + ICT OB anchor. TP: ICT structural targets + VWAP.
Trail: ICT zone-aware — frozen at OBs/FVGs. OB-anchored. Liq-ceiling capped.

v4.9 TRAIL REWRITE — solves "stopped during pullback, TP fires without us":

  ROOT CAUSE: Trail started at 0.3R with 1.0×ATR min-distance. A healthy BTC
  pullback of 1.2-1.8×ATR from the swing high hit the trailed SL. Price then
  continued to TP. Classic retail stop-hunt pattern we were self-inflicting.

  FIX 1 — ICT ZONE FREEZE:
    If price tests an active Order Block or sits inside an FVG, trail is FROZEN.
    These are institutional zones. A pullback INTO an OB is the trade working —
    smart money is defending exactly there. Tightening SL during an OB test is
    the primary source of the reported problem. Now completely prevented.

  FIX 2 — LATER TRAIL START + WIDER DISTANCES:
    TRAIL_BE_R: 0.3R → 0.50R. Trade must earn half the SL distance before trail.
    Phase 1 min-dist: 1.0→1.5×ATR. Phase 2: 0.7→1.1×ATR. Phase 3: 0.5→0.7×ATR.
    A healthy BTC pullback of 1.2×ATR no longer hits the trailed SL.

  FIX 3 — ICT OB ANCHOR (additional SL candidate):
    Trailing SL gets a candidate: OB.low - 0.35×ATR (for longs). This is WHERE
    institutional orders sit. The SL is structurally valid here — price bounces
    off OBs by design. Stronger signal than chandelier or arbitrary ATR levels.

  FIX 4 — LIQUIDITY POOL CEILING:
    Trail cannot advance past an unswept EQL/EQH pool. Smart money sweeps those
    stops first. Staying 0.5×ATR beyond the pool keeps us in the trade.

  FIX 5 — ICT STRUCTURAL TP TARGETS:
    compute_tp now accepts ict_engine and queries get_structural_tp_targets().
    Swept liquidity origins (score 6+), unfilled FVGs (score 5+), and virgin OBs
    (score 4+) beat all quant-only candidates in the scored pool. TP is placed
    at WHERE smart money is delivering price, not just VWAP fractions.
"""

from __future__ import annotations
import logging, math, time, threading
from collections import deque
from dataclasses import dataclass, field
from datetime import date
from enum import Enum, auto
from typing import Dict, List, Optional, Tuple

import config
from telegram_notifier import send_telegram_message
from order_manager import CancelResult
try:
    from ict_engine import ICTEngine, ICTConfluence
    _ICT_AVAILABLE = True
except ImportError:
    _ICT_AVAILABLE = False
try:
    from fee_engine import ExecutionCostEngine
except ImportError:
    ExecutionCostEngine = None   # fee_engine.py not yet present — graceful fallback

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
# CONFIG ACCESSOR
# ═══════════════════════════════════════════════════════════════
def _cfg(name: str, default):
    val = getattr(config, name, None)
    return default if val is None else val

class QCfg:
    @staticmethod
    def SYMBOL() -> str: return str(config.SYMBOL)
    @staticmethod
    def EXCHANGE() -> str: return str(config.EXCHANGE)
    @staticmethod
    def LEVERAGE() -> int: return int(_cfg("LEVERAGE", 30))
    @staticmethod
    def MARGIN_PCT() -> float: return float(_cfg("QUANT_MARGIN_PCT", 0.20))
    @staticmethod
    def LOT_STEP() -> float: return float(_cfg("LOT_STEP_SIZE", 0.001))
    @staticmethod
    def MIN_QTY() -> float: return float(_cfg("MIN_POSITION_SIZE", 0.001))
    @staticmethod
    def MAX_QTY() -> float: return float(_cfg("MAX_POSITION_SIZE", 1.0))
    @staticmethod
    def MIN_MARGIN_USDT() -> float: return float(_cfg("MIN_MARGIN_PER_TRADE", 4.0))
    @staticmethod
    def COMMISSION_RATE() -> float: return float(_cfg("COMMISSION_RATE", 0.00055))
    @staticmethod
    def TICK_SIZE() -> float: return float(_cfg("TICK_SIZE", 0.1))
    @staticmethod
    def SLIPPAGE_TOL() -> float: return float(_cfg("QUANT_SLIPPAGE_TOLERANCE", 0.0005))
    @staticmethod
    def VWAP_ENTRY_ATR_MULT() -> float: return float(_cfg("QUANT_VWAP_ENTRY_ATR_MULT", 1.2))
    @staticmethod
    def COMPOSITE_ENTRY_MIN() -> float: return float(_cfg("QUANT_COMPOSITE_ENTRY_MIN", 0.30))
    @staticmethod
    def EXIT_REVERSAL_THRESH() -> float: return float(_cfg("QUANT_EXIT_REVERSAL_THRESH", 0.40))
    @staticmethod
    def CONFIRM_TICKS() -> int: return int(_cfg("QUANT_CONFIRM_TICKS", 2))
    @staticmethod
    def SL_SWING_LOOKBACK() -> int: return int(_cfg("QUANT_SL_SWING_LOOKBACK", 12))
    @staticmethod
    def SL_BUFFER_ATR_MULT() -> float: return float(_cfg("QUANT_SL_BUFFER_ATR_MULT", 0.4))
    @staticmethod
    def TP_VWAP_FRACTION() -> float: return float(_cfg("QUANT_TP_VWAP_FRACTION", 0.50))
    @staticmethod
    def VP_BUCKET_COUNT() -> int: return int(_cfg("QUANT_VP_BUCKET_COUNT", 50))
    @staticmethod
    def VP_HVN_THRESHOLD() -> float: return float(_cfg("QUANT_VP_HVN_THRESHOLD", 0.70))
    @staticmethod
    def OB_WALL_DEPTH() -> int: return int(_cfg("QUANT_OB_WALL_DEPTH", 20))
    @staticmethod
    def OB_WALL_MULT() -> float: return float(_cfg("QUANT_OB_WALL_MULT", 2.5))
    @staticmethod
    def TRAIL_SWING_BARS() -> int: return int(_cfg("QUANT_TRAIL_SWING_BARS", 5))
    @staticmethod
    def TRAIL_VOL_DECAY_MULT() -> float: return float(_cfg("QUANT_TRAIL_VOL_DECAY_MULT", 0.6))
    @staticmethod
    def MIN_SL_PCT() -> float: return float(_cfg("MIN_SL_DISTANCE_PCT", 0.003))
    @staticmethod
    def MAX_SL_PCT() -> float: return float(_cfg("MAX_SL_DISTANCE_PCT", 0.035))
    @staticmethod
    def MIN_RR_RATIO() -> float: return float(_cfg("MIN_RISK_REWARD_RATIO", 0.8))
    @staticmethod
    def ATR_PERIOD() -> int: return int(_cfg("SL_ATR_PERIOD", 14))
    @staticmethod
    def TRAIL_ENABLED() -> bool: return bool(_cfg("QUANT_TRAIL_ENABLED", True))
    @staticmethod
    def TRAIL_BE_R() -> float: return float(_cfg("QUANT_TRAIL_BE_R", 0.3))
    @staticmethod
    def TRAIL_LOCK_R() -> float: return float(_cfg("QUANT_TRAIL_LOCK_R", 0.8))
    @staticmethod
    def TRAIL_INTERVAL_S() -> int: return int(_cfg("TRAILING_SL_CHECK_INTERVAL", 10))
    @staticmethod
    def TRAIL_MIN_MOVE_ATR() -> float: return float(_cfg("SL_MIN_IMPROVEMENT_ATR_MULT", 0.08))
    @staticmethod
    def CVD_WINDOW() -> int: return int(_cfg("QUANT_CVD_WINDOW", 20))
    @staticmethod
    def CVD_HIST_MULT() -> int: return int(_cfg("QUANT_CVD_HIST_MULT", 15))
    @staticmethod
    def VWAP_WINDOW() -> int: return int(_cfg("QUANT_VWAP_WINDOW", 50))
    @staticmethod
    def EMA_FAST() -> int: return int(_cfg("QUANT_EMA_FAST", 8))
    @staticmethod
    def EMA_SLOW() -> int: return int(_cfg("QUANT_EMA_SLOW", 21))
    @staticmethod
    def MIN_1M_BARS() -> int: return int(_cfg("MIN_CANDLES_1M", 80))
    @staticmethod
    def MIN_5M_BARS() -> int: return int(_cfg("MIN_CANDLES_5M", 60))
    @staticmethod
    def ATR_PCTILE_WINDOW() -> int: return int(_cfg("QUANT_ATR_PCTILE_WINDOW", 100))
    @staticmethod
    def ATR_MIN_PCTILE() -> float: return float(_cfg("QUANT_ATR_MIN_PCTILE", 0.05))
    @staticmethod
    def ATR_MAX_PCTILE() -> float: return float(_cfg("QUANT_ATR_MAX_PCTILE", 0.97))
    @staticmethod
    def MAX_HOLD_SEC() -> int: return int(_cfg("QUANT_MAX_HOLD_SEC", 2400))
    @staticmethod
    def COOLDOWN_SEC() -> int: return int(_cfg("QUANT_COOLDOWN_SEC", 180))
    @staticmethod
    def LOSS_LOCKOUT_SEC() -> int: return int(_cfg("QUANT_LOSS_LOCKOUT_SEC", 3600))
    @staticmethod
    def TICK_EVAL_SEC() -> float: return float(_cfg("ENTRY_EVALUATION_INTERVAL_SECONDS", 1))
    @staticmethod
    def POS_SYNC_SEC() -> float: return float(_cfg("QUANT_POS_SYNC_SEC", 30))
    @staticmethod
    def MAX_DAILY_TRADES() -> int: return int(_cfg("MAX_DAILY_TRADES", 8))
    @staticmethod
    def MAX_CONSEC_LOSSES() -> int: return int(_cfg("MAX_CONSECUTIVE_LOSSES", 3))
    @staticmethod
    def MAX_DAILY_LOSS_PCT() -> float: return float(_cfg("MAX_DAILY_LOSS_PCT", 5.0))
    @staticmethod
    def W_VWAP_DEV() -> float: return float(_cfg("QUANT_W_VWAP_DEV", 0.30))
    @staticmethod
    def W_CVD_DIV() -> float: return float(_cfg("QUANT_W_CVD_DIV", 0.25))
    @staticmethod
    def W_OB() -> float: return float(_cfg("QUANT_W_OB", 0.20))
    @staticmethod
    def W_TICK_FLOW() -> float: return float(_cfg("QUANT_W_TICK_FLOW", 0.15))
    @staticmethod
    def W_VOL_EXHAUSTION() -> float: return float(_cfg("QUANT_W_VOL_EXHAUSTION", 0.10))
    @staticmethod
    def HTF_ENABLED() -> bool: return bool(_cfg("QUANT_HTF_ENABLED", True))
    @staticmethod
    def HTF_VETO_STRENGTH() -> float: return float(_cfg("QUANT_HTF_VETO_STRENGTH", 0.70))
    @staticmethod
    def OB_DEPTH_LEVELS() -> int: return int(_cfg("QUANT_OB_DEPTH_LEVELS", 5))
    @staticmethod
    def OB_HIST_LEN() -> int: return int(_cfg("QUANT_OB_HIST_LEN", 60))
    @staticmethod
    def TICK_AGG_WINDOW_SEC() -> float: return float(_cfg("QUANT_TICK_AGG_WINDOW_SEC", 30.0))
    # ── New v4.1 accessors ──────────────────────────────────────
    @staticmethod
    def TP_MAX_RR() -> float: return float(_cfg("QUANT_TP_MAX_RR", 3.5))
    @staticmethod
    def SL_SWING_DENSITY_WINDOW() -> float: return float(_cfg("QUANT_SL_SWING_DENSITY_WINDOW", 0.30))
    @staticmethod
    def TRAIL_CHANDELIER_N_START() -> float: return float(_cfg("QUANT_TRAIL_CHANDELIER_N_START", 2.5))
    @staticmethod
    def TRAIL_CHANDELIER_N_END() -> float: return float(_cfg("QUANT_TRAIL_CHANDELIER_N_END", 1.2))
    @staticmethod
    def TRAIL_HVN_SNAP_THRESH() -> float: return float(_cfg("QUANT_TRAIL_HVN_SNAP_THRESH", 0.55))
    # ── v4.2: Trend-following mode ──────────────────────────────
    @staticmethod
    def ADX_PERIOD() -> int: return int(_cfg("QUANT_ADX_PERIOD", 14))
    @staticmethod
    def ADX_TREND_THRESH() -> float: return float(_cfg("QUANT_ADX_TREND_THRESH", 25.0))
    @staticmethod
    def ADX_RANGE_THRESH() -> float: return float(_cfg("QUANT_ADX_RANGE_THRESH", 20.0))
    @staticmethod
    def ATR_EXPANSION_THRESH() -> float: return float(_cfg("QUANT_ATR_EXPANSION_THRESH", 1.30))
    @staticmethod
    def TREND_PULLBACK_ATR_MIN() -> float: return float(_cfg("QUANT_TREND_PULLBACK_ATR_MIN", 0.10))
    @staticmethod
    def TREND_PULLBACK_ATR_MAX() -> float: return float(_cfg("QUANT_TREND_PULLBACK_ATR_MAX", 2.00))
    @staticmethod
    def TREND_CVD_MIN() -> float: return float(_cfg("QUANT_TREND_CVD_MIN", -0.20))
    @staticmethod
    def TREND_TP_ATR_MULT() -> float: return float(_cfg("QUANT_TREND_TP_ATR_MULT", 2.5))
    @staticmethod
    def TREND_COMPOSITE_MIN() -> float: return float(_cfg("QUANT_TREND_COMPOSITE_MIN", 0.35))
    @staticmethod
    def TREND_CONFIRM_TICKS() -> int: return int(_cfg("QUANT_TREND_CONFIRM_TICKS", 3))
    @staticmethod
    def TREND_CHANDELIER_N() -> float: return float(_cfg("QUANT_TREND_CHANDELIER_N", 1.5))
    # ── v4.4: Mode-aware R:R ────────────────────────────────────
    @staticmethod
    def REVERSION_MIN_RR() -> float: return float(_cfg("QUANT_REVERSION_MIN_RR", 1.5))
    @staticmethod
    def REVERSION_MAX_RR() -> float: return float(_cfg("QUANT_REVERSION_MAX_RR", 3.0))
    @staticmethod
    def TREND_MIN_RR() -> float: return float(_cfg("QUANT_TREND_MIN_RR", 3.0))
    @staticmethod
    def TREND_MAX_RR() -> float: return float(_cfg("QUANT_TREND_MAX_RR", 5.0))
    # ── v4.5: Institutional trail params ────────────────────────
    @staticmethod
    def TRAIL_AGGRESSIVE_R() -> float: return float(_cfg("QUANT_TRAIL_AGGRESSIVE_R", 1.5))
    @staticmethod
    def TRAIL_MIN_DIST_ATR_P1() -> float: return float(_cfg("QUANT_TRAIL_MIN_DIST_ATR_P1", 1.0))
    @staticmethod
    def TRAIL_MIN_DIST_ATR_P2() -> float: return float(_cfg("QUANT_TRAIL_MIN_DIST_ATR_P2", 0.7))
    @staticmethod
    def TRAIL_MIN_DIST_ATR_P3() -> float: return float(_cfg("QUANT_TRAIL_MIN_DIST_ATR_P3", 0.5))
    @staticmethod
    def TRAIL_PULLBACK_FREEZE() -> bool: return bool(_cfg("QUANT_TRAIL_PULLBACK_FREEZE", True))
    @staticmethod
    def TRAIL_PB_VOL_RATIO() -> float: return float(_cfg("QUANT_TRAIL_PB_VOL_RATIO", 0.60))
    @staticmethod
    def TRAIL_PB_DEPTH_ATR() -> float: return float(_cfg("QUANT_TRAIL_PB_DEPTH_ATR", 0.80))
    @staticmethod
    def TRAIL_REV_MIN_SIGNALS() -> int: return int(_cfg("QUANT_TRAIL_REV_MIN_SIGNALS", 3))
    # ── v4.4: Smart max-hold exit ───────────────────────────────
    @staticmethod
    def SMART_MAX_HOLD() -> bool: return bool(_cfg("QUANT_SMART_MAX_HOLD", True))
    @staticmethod
    def MAX_HOLD_PROFIT_SL_ATR() -> float: return float(_cfg("QUANT_MAX_HOLD_PROFIT_SL_ATR", 0.5))
    # ── v4.6: Thesis-aware max-hold extension ────────────────────
    @staticmethod
    def MAX_HOLD_EXTENSIONS() -> int: return int(_cfg("QUANT_MAX_HOLD_EXTENSIONS", 3))
    @staticmethod
    def HOLD_EXTENSION_SEC() -> int: return int(_cfg("QUANT_HOLD_EXTENSION_SEC", 1200))
    @staticmethod
    def THESIS_MAX_DRAWDOWN_PCT() -> float: return float(_cfg("QUANT_THESIS_MAX_DRAWDOWN_PCT", 0.70))
    # ── v4.6: Natural TP + SL ATR cap ───────────────────────────
    @staticmethod
    def TP_MIN_ATR_MULT() -> float: return float(_cfg("QUANT_TP_MIN_ATR_MULT", 0.5))
    @staticmethod
    def TP_MAX_ATR_MULT() -> float: return float(_cfg("QUANT_TP_MAX_ATR_MULT", 6.0))
    @staticmethod
    def REVERSION_REJECT_RR() -> float: return float(_cfg("QUANT_REVERSION_REJECT_RR", 0.20))
    @staticmethod
    def SL_MAX_ATR_MULT() -> float: return float(_cfg("QUANT_SL_MAX_ATR_MULT", 4.0))
    # ── v4.9: ICT-anchored trailing SL ─────────────────────────────
    @staticmethod
    def ICT_ZONE_FREEZE_ENABLED() -> bool: return bool(_cfg("QUANT_ICT_ZONE_FREEZE_ENABLED", True))
    @staticmethod
    def ICT_ZONE_FREEZE_ATR() -> float: return float(_cfg("QUANT_ICT_ZONE_FREEZE_ATR", 0.40))
    @staticmethod
    def ICT_OB_SL_ANCHOR() -> bool: return bool(_cfg("QUANT_ICT_OB_SL_ANCHOR", True))
    @staticmethod
    def ICT_OB_SL_BUFFER_ATR() -> float: return float(_cfg("QUANT_ICT_OB_SL_BUFFER_ATR", 0.35))
    @staticmethod
    def ICT_LIQ_CEILING_ENABLED() -> bool: return bool(_cfg("QUANT_ICT_LIQ_CEILING_ENABLED", True))
    @staticmethod
    def ICT_LIQ_POOL_BUFFER_ATR() -> float: return float(_cfg("QUANT_ICT_LIQ_POOL_BUFFER_ATR", 0.50))

def _round_to_tick(price: float) -> float:
    tick = QCfg.TICK_SIZE()
    return round(round(price / tick) * tick, 10) if tick > 0 else price

def _sigmoid(z: float, steepness: float = 1.0) -> float:
    return max(-1.0, min(1.0, z * steepness / (1.0 + abs(z * steepness) * 0.5)))

# ═══════════════════════════════════════════════════════════════
# ENGINE 1: VWAP DEVIATION — Primary Mean-Reversion Signal
# ═══════════════════════════════════════════════════════════════
class VWAPEngine:
    """
    VWAP Deviation — Primary Mean-Reversion Signal.

    v4.8 REWRITE — 3 critical bugs fixed:

    BUG 1: DEAD ZONE — Signal returned 0.0 unless |dev| > 0.72 ATR.
           In ranging markets (ADX<25), price oscillates ±0.3-0.5 ATR.
           The VWAP signal (30% weight) was PERMANENTLY ZERO, crippling
           the composite score. 90% of the time the highest-weighted
           signal contributed nothing.
           FIX: Smooth sigmoid from ANY deviation. No dead zone.
           At 0.3 ATR: signal ≈ ±0.25. At 0.7 ATR: signal ≈ ±0.65.

    BUG 2: OVEREXTENDED GATE — Required 1.2×ATR ($233) from VWAP.
           In ranging market, price never reaches this. Gate blocked ALL
           entries even when Σ=+0.557 with 4/6 confluence.
           FIX: Regime-adaptive threshold:
             Ranging (ADX<25):  0.5×ATR (~$97)
             Transitioning:     0.7×ATR (~$136)
             Trending (ADX>25): 1.0×ATR (~$194)

    BUG 3: SIGMOID TOO FLAT — _sigmoid(-dev / (entry_thresh * 2.0), 1.5)
           With entry_thresh=1.2, sigmoid input at 0.5 ATR = 0.21.
           Output after sigmoid: ~0.15. Barely contributes to composite.
           FIX: Steeper sigmoid with direct ATR-normalized input.
    """
    def __init__(self):
        self._vwap = 0.0
        self._std = 0.0
        self._deviation_atr = 0.0

    def update(self, candles: List[Dict], atr: float) -> None:
        window = QCfg.VWAP_WINDOW()
        if len(candles) < window: return
        recent = candles[-window:]
        tp_vol = sum((float(c['h'])+float(c['l'])+float(c['c']))/3.0*float(c['v']) for c in recent)
        vol_sum = sum(float(c['v']) for c in recent)
        if vol_sum < 1e-12: return
        self._vwap = tp_vol / vol_sum
        var_sum = sum(float(c['v'])*((float(c['h'])+float(c['l'])+float(c['c']))/3.0-self._vwap)**2 for c in recent)
        self._std = math.sqrt(var_sum / vol_sum)
        if atr > 1e-10:
            self._deviation_atr = (float(candles[-1]['c']) - self._vwap) / atr

    def get_reversion_signal(self, price: float, atr: float) -> float:
        """
        v4.8: Smooth reversion signal with NO dead zone.

        Returns [-1, +1]: negative = price above VWAP (short bias),
                           positive = price below VWAP (long bias).

        Signal magnitude scales with deviation:
          0.2 ATR → ±0.15 (weak)
          0.5 ATR → ±0.40 (moderate)
          0.8 ATR → ±0.65 (strong)
          1.2 ATR → ±0.85 (very strong)
          2.0 ATR → ±0.97 (extreme)
        """
        if self._vwap < 1e-10 or atr < 1e-10: return 0.0
        dev = (price - self._vwap) / atr
        # Smooth sigmoid — reversion signal opposes the deviation
        # Steepness 1.2 gives good sensitivity: starts producing meaningful
        # signal at 0.2 ATR, saturates around 2.0 ATR
        return max(-1.0, min(1.0, _sigmoid(-dev, 1.2)))

    def is_overextended(self, price: float, atr: float, adx: float = 0.0) -> bool:
        """
        v4.8: Regime-adaptive overextension check.

        In ranging markets (low ADX), price reverts from smaller deviations.
        In trending markets, it takes a larger deviation to be "overextended"
        because the trend creates sustained VWAP distance.

          ADX < 25 (ranging):       0.5×ATR threshold
          25 ≤ ADX < 35 (transit):  0.7×ATR threshold
          ADX ≥ 35 (trending):      1.0×ATR threshold
        """
        if self._vwap < 1e-10 or atr < 1e-10: return False
        dev_abs = abs(price - self._vwap) / atr
        # Regime-adaptive threshold
        if adx < 25.0:
            thresh = 0.4   # ranging: enter at 0.4 ATR from VWAP
        elif adx < 35.0:
            thresh = 0.6   # transitioning
        else:
            thresh = 0.9   # trending: need bigger deviation
        return dev_abs >= thresh

    def reversion_side(self, price: float) -> str:
        return "short" if price > self._vwap else "long"

    def tp_target(self, price: float) -> float:
        return price + (self._vwap - price) * QCfg.TP_VWAP_FRACTION()

    @property
    def vwap(self) -> float: return self._vwap
    @property
    def vwap_std(self) -> float: return self._std
    @property
    def deviation_atr(self) -> float: return self._deviation_atr

# ═══════════════════════════════════════════════════════════════
# ENGINE 2: CVD DIVERGENCE — Exhaustion Detection
# ═══════════════════════════════════════════════════════════════
class CVDEngine:
    def __init__(self):
        self._deltas: deque = deque(maxlen=QCfg.CVD_WINDOW() * QCfg.CVD_HIST_MULT())
        self._last_bar_ts: int = 0

    def reset_state(self):
        """Reset timestamps so warmup data is reprocessed after stream restart."""
        self._last_bar_ts = 0
        self._deltas.clear()

    def update(self, candles: List[Dict]) -> None:
        if not candles: return
        new_start = 0
        if self._last_bar_ts > 0:
            for i, c in enumerate(candles):
                if int(c['t']) > self._last_bar_ts:
                    new_start = i; break
            else:
                if candles:
                    c = candles[-1]; hi=float(c['h']); lo=float(c['l']); cl=float(c['c']); vol=float(c['v'])
                    rng = hi - lo
                    if self._deltas:
                        self._deltas[-1] = vol * ((2.0*cl-hi-lo)/rng if rng > 1e-10 else 0.0)
                return
        for c in candles[new_start:]:
            hi=float(c['h']); lo=float(c['l']); cl=float(c['c']); vol=float(c['v'])
            rng = hi - lo
            self._deltas.append(vol * ((2.0*cl-hi-lo)/rng if rng > 1e-10 else 0.0))
            self._last_bar_ts = int(c['t'])

    def get_divergence_signal(self, candles: List[Dict]) -> float:
        w = QCfg.CVD_WINDOW(); arr = list(self._deltas); n = len(arr)
        if n < w + 10 or len(candles) < w: return 0.0
        recent_cvd = sum(arr[-w//2:]); earlier_cvd = sum(arr[-w:-w//2])
        cvd_slope = recent_cvd - earlier_cvd
        closes = [float(c['c']) for c in candles[-w:]]
        mid = w // 2
        price_slope = sum(closes[mid:])/max(len(closes[mid:]),1) - sum(closes[:mid])/max(len(closes[:mid]),1)
        if abs(price_slope) < 1e-10: return 0.0
        all_sums = []; running = sum(arr[:w//2]); all_sums.append(running)
        for i in range(w//2, n - w//2):
            running += arr[i] - arr[i - w//2]; all_sums.append(running)
        if len(all_sums) < 5: return 0.0
        mu = sum(all_sums)/len(all_sums)
        std = math.sqrt(sum((s-mu)**2 for s in all_sums)/max(len(all_sums)-1,1))
        if std < 1e-12: return 0.0
        cvd_z = cvd_slope / std
        price_dir = 1.0 if price_slope > 0 else -1.0
        if (1.0 if cvd_z > 0 else -1.0) == price_dir: return 0.0
        return -price_dir * min(abs(cvd_z), 3.0) / 3.0

    def get_trend_signal(self) -> float:
        """
        Directional CVD bias for trend-following mode.

        Unlike get_divergence_signal (which looks for CVD diverging from price),
        this asks: is net order-flow consistently in one direction?

        Returns +1.0 = sustained net buying, -1.0 = sustained net selling.
        Used to confirm that trend trades are aligned with actual order flow.
        """
        arr = list(self._deltas); n = len(arr)
        w = QCfg.CVD_WINDOW()
        if n < w + 10: return 0.0
        # Rolling sums over window 'w' to build a distribution
        sums = []
        for i in range(n - w * 2, n - w + 1):
            if i >= 0:
                sums.append(sum(arr[i:i + w]))
        if len(sums) < 5: return 0.0
        recent_sum = sum(arr[-w:])
        mu  = sum(sums) / len(sums)
        std = math.sqrt(sum((s - mu) ** 2 for s in sums) / max(len(sums) - 1, 1))
        if std < 1e-12:
            return _sigmoid(recent_sum / (abs(mu) + 1e-10), 0.5)
        return _sigmoid((recent_sum - mu) / std, 0.7)

# ═══════════════════════════════════════════════════════════════
# ENGINE 3: ORDERBOOK IMBALANCE
# ═══════════════════════════════════════════════════════════════
class OrderbookEngine:
    def __init__(self):
        self._imbalance_hist: deque = deque(maxlen=QCfg.OB_HIST_LEN())
        self._last_imbalance = 0.0; self._spread_ratio = 0.0

    def update(self, orderbook: Dict, price: float) -> None:
        bids = orderbook.get("bids",[]); asks = orderbook.get("asks",[])
        depth = QCfg.OB_DEPTH_LEVELS()
        if not bids or not asks or price < 1.0: return
        bid_depth = sum(float(l[1]) for l in bids[:depth] if isinstance(l,(list,tuple)) and len(l)>1)
        ask_depth = sum(float(l[1]) for l in asks[:depth] if isinstance(l,(list,tuple)) and len(l)>1)
        total = bid_depth + ask_depth
        if total < 1e-12: return
        self._last_imbalance = (bid_depth - ask_depth) / total
        self._imbalance_hist.append(self._last_imbalance)
        try:
            bb = float(bids[0][0]); ba = float(asks[0][0])
            if bb > 0 and ba > 0: self._spread_ratio = (ba - bb) / ((bb + ba) / 2.0)
        except Exception: pass

    def get_signal(self) -> float:
        hist = list(self._imbalance_hist)
        if len(hist) < 15: return 0.0
        current = hist[-1]; baseline = hist[:-1]
        mu = sum(baseline)/len(baseline)
        std = math.sqrt(sum((x-mu)**2 for x in baseline)/max(len(baseline)-1,1))
        if std < 1e-12: return _sigmoid(current * 3.0, 0.8)
        z = (current - mu) / std
        sm = max(0.5, min(1.0, 1.0 - (self._spread_ratio - 0.0002) * 100.0))
        return _sigmoid(z, 0.6) * sm

# ═══════════════════════════════════════════════════════════════
# ENGINE 4: TICK FLOW
# ═══════════════════════════════════════════════════════════════
class TickFlowEngine:
    def __init__(self):
        self._buy_vol: deque = deque(maxlen=600); self._sell_vol: deque = deque(maxlen=600)
        self._flow_hist: deque = deque(maxlen=120); self._last_signal = 0.0

    def on_trade(self, price: float, qty: float, is_buyer: bool, ts: float) -> None:
        (self._buy_vol if is_buyer else self._sell_vol).append((ts, price * qty))

    def compute_signal(self) -> float:
        now = time.time(); cutoff = now - QCfg.TICK_AGG_WINDOW_SEC()
        bt = sum(dv for ts,dv in self._buy_vol if ts >= cutoff)
        st = sum(dv for ts,dv in self._sell_vol if ts >= cutoff)
        total = bt + st
        if total < 1e-10: return 0.0
        fr = (bt - st) / total; self._flow_hist.append(fr)
        hist = list(self._flow_hist)
        if len(hist) < 10: return _sigmoid(fr * 2.0, 0.8)
        mu = sum(hist[:-1])/len(hist[:-1])
        std = math.sqrt(sum((x-mu)**2 for x in hist[:-1])/max(len(hist[:-1])-1,1))
        if std < 1e-12: return _sigmoid(fr * 2.0, 0.8)
        self._last_signal = _sigmoid((fr - mu) / std, 0.5)
        return self._last_signal

    def get_signal(self) -> float: return self._last_signal

# ═══════════════════════════════════════════════════════════════
# ENGINE 5: VOLUME EXHAUSTION
# ═══════════════════════════════════════════════════════════════
class VolumeExhaustionEngine:
    def __init__(self): self._last_signal = 0.0

    def compute(self, candles: List[Dict]) -> float:
        if len(candles) < 20: return 0.0
        recent = candles[-10:]; earlier = candles[-20:-10]
        # v4.3 Bug 4 fix: use average close of each window instead of single endpoints
        avg_recent = sum(float(c['c']) for c in recent) / len(recent)
        avg_earlier = sum(float(c['c']) for c in earlier) / len(earlier)
        pc = avg_recent - avg_earlier
        pd = 1.0 if pc > 0 else -1.0
        rv = sum(float(c['v']) for c in recent); ev = sum(float(c['v']) for c in earlier)
        if ev < 1e-10: return 0.0
        vr = rv / ev
        if vr < 0.7: self._last_signal = -pd * min((0.7 - vr) / 0.4, 1.0)
        elif vr < 0.9: self._last_signal = -pd * (0.9 - vr) / 0.4 * 0.5
        else: self._last_signal = 0.0
        return self._last_signal

# ═══════════════════════════════════════════════════════════════
# ADX ENGINE — Wilder's Average Directional Index
# ═══════════════════════════════════════════════════════════════
class ADXEngine:
    """
    Proper Wilder ADX(14) with +DI/-DI.

    Seeding: requires at least 2×period candles to bootstrap Wilder smoothing.
    Incremental: each new candle updates the Wilder-smoothed TR, +DM, -DM, then
    computes DX and Wilder-smooths it into ADX.

    Interpretation:
      ADX < 20  → no trend (ranging)
      ADX 20-25 → transitional / weak trend
      ADX > 25  → established trend
      ADX > 40  → strong trend
      +DI > -DI → bullish pressure dominant
      -DI > +DI → bearish pressure dominant
    """
    def __init__(self):
        self._adx               = 0.0
        self._plus_di           = 0.0
        self._minus_di          = 0.0
        self._smoothed_plus_dm  = 0.0
        self._smoothed_minus_dm = 0.0
        self._smoothed_tr       = 0.0
        self._seeded            = False
        self._last_ts           = -1

    def reset_state(self):
        """Force full re-seed after stream restart."""
        self._seeded = False
        self._last_ts = -1

    def compute(self, candles: List[Dict]) -> float:
        if len(candles) < 2: return self._adx
        period  = QCfg.ADX_PERIOD()
        last_ts = int(candles[-1].get('t', 0))
        if last_ts == self._last_ts and self._seeded: return self._adx
        if len(candles) < period * 2 + 1: return self._adx

        if not self._seeded:
            plus_dms: List[float] = []
            minus_dms: List[float] = []
            trs: List[float] = []
            for i in range(1, len(candles)):
                h  = float(candles[i]['h']);   l  = float(candles[i]['l'])
                ph = float(candles[i-1]['h']); pl = float(candles[i-1]['l'])
                pc = float(candles[i-1]['c'])
                up = h - ph; dn = pl - l
                plus_dms.append(up  if up  > dn and up  > 0 else 0.0)
                minus_dms.append(dn if dn  > up and dn  > 0 else 0.0)
                trs.append(max(h - l, abs(h - pc), abs(l - pc)))

            # Bootstrap Wilder sum with straight sum of first 'period' bars
            sp = sum(plus_dms[:period])
            sm = sum(minus_dms[:period])
            st = sum(trs[:period])

            dxs: List[float] = []
            for i in range(period, len(plus_dms)):
                sp = sp - sp / period + plus_dms[i]
                sm = sm - sm / period + minus_dms[i]
                st = st - st / period + trs[i]
                if st < 1e-10: continue
                pdi = 100.0 * sp / st
                mdi = 100.0 * sm / st
                denom = pdi + mdi
                dxs.append(100.0 * abs(pdi - mdi) / denom if denom > 1e-10 else 0.0)
                self._plus_di = pdi; self._minus_di = mdi

            self._smoothed_plus_dm  = sp
            self._smoothed_minus_dm = sm
            self._smoothed_tr       = st

            if not dxs: return self._adx
            n_seed = min(period, len(dxs))
            adx    = sum(dxs[:n_seed]) / n_seed
            for dx in dxs[n_seed:]:
                adx = (adx * (period - 1) + dx) / period
            self._adx     = adx
            self._seeded  = True
            self._last_ts = last_ts
            return self._adx

        # Incremental update — single new bar
        h  = float(candles[-1]['h']); l  = float(candles[-1]['l'])
        ph = float(candles[-2]['h']); pl = float(candles[-2]['l'])
        pc = float(candles[-2]['c'])
        up = h - ph; dn = pl - l
        plus_dm  = up if up  > dn and up  > 0 else 0.0
        minus_dm = dn if dn  > up and dn  > 0 else 0.0
        tr = max(h - l, abs(h - pc), abs(l - pc))

        self._smoothed_plus_dm  = self._smoothed_plus_dm  - self._smoothed_plus_dm  / period + plus_dm
        self._smoothed_minus_dm = self._smoothed_minus_dm - self._smoothed_minus_dm / period + minus_dm
        self._smoothed_tr       = self._smoothed_tr       - self._smoothed_tr       / period + tr

        if self._smoothed_tr > 1e-10:
            self._plus_di  = 100.0 * self._smoothed_plus_dm  / self._smoothed_tr
            self._minus_di = 100.0 * self._smoothed_minus_dm / self._smoothed_tr
            denom = self._plus_di + self._minus_di
            dx    = 100.0 * abs(self._plus_di - self._minus_di) / denom if denom > 1e-10 else 0.0
            self._adx = (self._adx * (period - 1) + dx) / period

        self._last_ts = last_ts
        return self._adx

    @property
    def adx(self) -> float: return self._adx
    @property
    def plus_di(self) -> float: return self._plus_di
    @property
    def minus_di(self) -> float: return self._minus_di

    def trend_direction(self) -> str:
        """'up', 'down', or 'neutral' based on +DI vs -DI spread (>4 pt gap required)."""
        diff = self._plus_di - self._minus_di
        if abs(diff) < 4.0: return "neutral"
        return "up" if diff > 0 else "down"

    def is_trending(self) -> bool:
        return self._seeded and self._adx >= QCfg.ADX_TREND_THRESH()

    def is_ranging(self) -> bool:
        return self._seeded and self._adx < QCfg.ADX_RANGE_THRESH()


# ═══════════════════════════════════════════════════════════════
# MARKET REGIME + CLASSIFIER
# ═══════════════════════════════════════════════════════════════
class MarketRegime(Enum):
    RANGING       = "RANGING"       # consolidation — reversion mode is primary
    TRANSITIONING = "TRANSITIONING" # unclear — reversion with tighter gates
    TRENDING_UP   = "TRENDING_UP"   # directional up — trend entries only
    TRENDING_DOWN = "TRENDING_DOWN" # directional down — trend entries only


class RegimeClassifier:
    """
    Multi-factor regime detection.

    Inputs and weights:
      ADX(14) on 5m (50%):  Wilder's trend strength. > 25 = trending.
      ATR expansion (30%):  current_atr / mean(atr[-20]). > 1.3 = directional vol.
      HTF alignment (20%):  4h×0.6 + 15m×0.4 trend score magnitude. Macro confirms.

    Regime thresholds:
      TRENDING   ← confidence ≥ 0.55 AND ADX confirms AND +DI/-DI direction clear
      RANGING    ← confidence < 0.30
      TRANSITIONING ← otherwise

    Direction requires ADX's +DI/-DI to broadly agree with the HTF composite.
    This prevents regime flip on a single-candle spike.
    """
    def __init__(self):
        self._regime     = MarketRegime.RANGING
        self._confidence = 0.5
        self._direction  = "neutral"

    def update(self, adx: ADXEngine, atr: ATREngine, htf: HTFTrendFilter) -> MarketRegime:
        adx_val    = adx.adx
        trend_dir  = adx.trend_direction()
        trend_thr  = QCfg.ADX_TREND_THRESH()
        range_thr  = QCfg.ADX_RANGE_THRESH()

        # ADX score
        if adx_val >= trend_thr:
            adx_score    = min((adx_val - trend_thr) / 20.0, 1.0)
            adx_trending = True
        elif adx_val < range_thr:
            adx_score    = 0.0
            adx_trending = False
        else:
            adx_score    = (adx_val - range_thr) / (trend_thr - range_thr) * 0.5
            adx_trending = False

        # ATR expansion score
        # BUG FIX: original code used hist[-20:] which INCLUDES hist[-1] (current ATR)
        # as part of the baseline mean.  Dividing current by a mean that already
        # contains current understates expansion (self-reference bias).
        # Fix: use hist[-21:-1] — the 20 bars BEFORE the current bar.
        hist = list(atr._atr_hist)
        expansion = 1.0
        if len(hist) >= 21:
            baseline = sum(hist[-21:-1]) / 20.0
            if baseline > 1e-10:
                expansion = hist[-1] / baseline
        elif len(hist) >= 2:
            # Not enough history for a full 20-bar baseline — use what we have,
            # still excluding the current value.
            prior = hist[:-1]
            baseline = sum(prior) / len(prior)
            if baseline > 1e-10:
                expansion = hist[-1] / baseline
        exp_thr         = QCfg.ATR_EXPANSION_THRESH()
        expansion_score = min(max((expansion - 1.0) / (exp_thr - 1.0), 0.0), 1.0)

        # HTF alignment score
        htf_composite = htf.trend_4h * 0.60 + htf.trend_15m * 0.40
        htf_score     = min(abs(htf_composite), 1.0)
        htf_up        = htf_composite > 0

        confidence = adx_score * 0.50 + expansion_score * 0.30 + htf_score * 0.20

        if confidence >= 0.55 and adx_trending:
            di_up = (trend_dir == "up")
            if di_up and htf_up:
                regime = MarketRegime.TRENDING_UP
            elif not di_up and not htf_up:
                regime = MarketRegime.TRENDING_DOWN
            elif trend_dir == "up":
                regime = MarketRegime.TRENDING_UP    # ADX clear; HTF still catching up
            elif trend_dir == "down":
                regime = MarketRegime.TRENDING_DOWN
            else:
                regime = MarketRegime.TRANSITIONING  # DI spread neutral
        elif confidence < 0.30:
            regime = MarketRegime.RANGING
        else:
            regime = MarketRegime.TRANSITIONING

        self._regime     = regime
        self._confidence = confidence
        self._direction  = trend_dir
        return regime

    @property
    def regime(self) -> MarketRegime: return self._regime
    @property
    def confidence(self) -> float:   return self._confidence
    @property
    def direction(self) -> str:      return self._direction

    def is_trending(self) -> bool:
        return self._regime in (MarketRegime.TRENDING_UP, MarketRegime.TRENDING_DOWN)

    def trend_side(self) -> Optional[str]:
        if self._regime == MarketRegime.TRENDING_UP:   return "long"
        if self._regime == MarketRegime.TRENDING_DOWN: return "short"
        return None

    def allows_reversion(self, reversion_side: str) -> bool:
        """
        Hard-veto reversion trades that are counter to an established trend.
        Fading a trend flush = stop-loss machine.
        """
        if self._regime == MarketRegime.TRENDING_UP   and reversion_side == "short": return False
        if self._regime == MarketRegime.TRENDING_DOWN and reversion_side == "long":  return False
        return True


# ═══════════════════════════════════════════════════════════════
# BREAKOUT DETECTOR — Adaptive multi-evidence scoring (v4.7)
# ═══════════════════════════════════════════════════════════════
class BreakoutDetector:
    """
    Institutional breakout detection with adaptive evidence weighting.

    Key insight: A real breakout has MULTIPLE confirming signals simultaneously.
    A fake spike has only one (the candle body). So we weigh evidence, not count
    candles. One explosive candle with volume confirmation = real. Without = noise.

    Scoring (5 factors, direction-aware):
      F1: Candle body magnitude (uses last CLOSED candle only — no wick noise)
          body > 1.5×ATR = 3pts | > 1.0×ATR = 2pts | > 0.5×ATR = 1pt
      F2: Volume confirmation (breakout candle volume vs recent average)
          vol > 2.0× avg = 2pts | > 1.5× avg = 1pt
      F3: ATR expansion (current vs baseline)
          expansion > 1.5× = 1pt
      F4: Price displacement from VWAP (institutional anchor)
          |price - VWAP| > 2.0×ATR = 2pts | > 1.5×ATR = 1pt
      F5: Follow-through (current price holds above breakout candle midpoint)
          Holds above 50% of breakout candle body = 1pt

    Min score: 4 (one explosive candle + volume = 4, passes without waiting)

    After breakout fires → tracks state for retest entry:
      - Records breakout extreme + midpoint
      - Tracks pullback low/high for retest entry trigger
    """
    def __init__(self):
        self._breakout_active = False
        self._breakout_dir = ""
        self._breakout_until = 0.0
        self._last_check = 0.0
        self._CHECK_INTERVAL = 5.0

        # Retest tracking state
        self._bo_extreme = 0.0
        self._bo_midpoint = 0.0
        self._retest_low = 0.0
        self._retest_high = 0.0
        self._retest_started = False
        self._retest_ready = False
        self._retest_invalidated = False   # v4.7: once pullback too deep, stays dead
        self._retest_timeout = 0.0
        self._bo_atr = 0.0

        # v4.7 FLAW 4 FIX: Directional cooldown after expiry
        self._last_expired_dir = ""        # direction of last expired breakout
        self._last_expired_time = 0.0      # when it expired

    def reset_state(self):
        self._breakout_active = False
        self._breakout_dir = ""
        self._breakout_until = 0.0
        self._retest_started = False
        self._retest_ready = False
        self._retest_invalidated = False
        self._last_expired_dir = ""
        self._last_expired_time = 0.0

    def update(self, candles_5m: List[Dict], atr_engine: 'ATREngine',
               price: float, now: float, vwap_price: float = 0.0) -> None:
        """Check for breakout conditions. Called from _evaluate_entry."""
        if now - self._last_check < self._CHECK_INTERVAL:
            return
        self._last_check = now

        # If block expired, clear it and record direction for cooldown
        if self._breakout_active and now > self._breakout_until:
            self._last_expired_dir = self._breakout_dir  # v4.7: remember direction
            self._last_expired_time = now
            self._breakout_active = False
            self._breakout_dir = ""
            self._retest_started = False
            self._retest_ready = False
            self._retest_invalidated = False
            logger.info("🔓 Breakout block expired — reversion entries re-enabled")

        if len(candles_5m) < 10:
            return

        atr = atr_engine.atr
        if atr < 1e-10:
            return

        # ── Update retest tracking for active breakout ────────────────────
        if self._breakout_active:
            self._track_retest(price, atr, now)
            return  # don't re-detect while active

        # v4.7 FLAW 4 FIX: Cooldown before detecting OPPOSITE direction
        # After breakout UP expires and price pulls back, the correction candle
        # looks like a breakout DOWN — but it's just mean reversion. Block opposite
        # direction detection for 10 minutes after expiry.
        opposite_cooldown = 600.0  # 10 minutes

        score_up = 0
        score_down = 0
        details = {}

        # ── Factor 1: Candle body magnitude (CLOSED candle only) ──────────
        last = candles_5m[-1]
        body = float(last['c']) - float(last['o'])
        body_abs = abs(body)
        body_atr = body_abs / atr
        is_bullish = body > 0

        if body_atr >= 1.5:
            (score_up if is_bullish else score_down)  # don't modify — use ternary below
            if is_bullish: score_up += 3
            else: score_down += 3
            details['candle'] = f'{body_atr:.1f}ATR(3)'
        elif body_atr >= 1.0:
            if is_bullish: score_up += 2
            else: score_down += 2
            details['candle'] = f'{body_atr:.1f}ATR(2)'
        elif body_atr >= 0.5:
            if is_bullish: score_up += 1
            else: score_down += 1
            details['candle'] = f'{body_atr:.1f}ATR(1)'

        # ── Factor 2: Volume confirmation ─────────────────────────────────
        vols = [float(c.get('v', 0)) for c in candles_5m[-8:]]
        if len(vols) >= 6:
            avg_vol = sum(vols[:-1]) / max(len(vols) - 1, 1)
            last_vol = vols[-1]
            if avg_vol > 1e-10:
                vol_ratio = last_vol / avg_vol
                if vol_ratio >= 2.0:
                    score_up += 2; score_down += 2
                    details['vol'] = f'{vol_ratio:.1f}x(2)'
                elif vol_ratio >= 1.5:
                    score_up += 1; score_down += 1
                    details['vol'] = f'{vol_ratio:.1f}x(1)'

        # ── Factor 3: ATR expansion ───────────────────────────────────────
        hist = list(atr_engine._atr_hist)
        if len(hist) >= 10:
            baseline = sum(hist[-10:-1]) / 9.0
            if baseline > 1e-10:
                expansion = hist[-1] / baseline
                if expansion >= 1.5:
                    score_up += 1; score_down += 1
                    details['atr_exp'] = f'{expansion:.1f}x(1)'

        # ── Factor 4: VWAP displacement ───────────────────────────────────
        if vwap_price > 1e-10:
            vwap_disp = (price - vwap_price) / atr
            vwap_abs = abs(vwap_disp)
            if vwap_abs >= 2.0:
                if vwap_disp > 0: score_up += 2
                else: score_down += 2
                details['vwap'] = f'{vwap_disp:+.1f}ATR(2)'
            elif vwap_abs >= 1.5:
                if vwap_disp > 0: score_up += 1
                else: score_down += 1
                details['vwap'] = f'{vwap_disp:+.1f}ATR(1)'

        # ── Factor 5: Follow-through ──────────────────────────────────────
        # Current price holds above/below breakout candle midpoint
        mid = (float(last['o']) + float(last['c'])) / 2.0
        if is_bullish and price > mid:
            score_up += 1
            details['hold'] = 'above_mid(1)'
        elif not is_bullish and body_abs > 1e-10 and price < mid:
            score_down += 1
            details['hold'] = 'below_mid(1)'

        # ── Evaluate ─────────────────────────────────────────────────────
        min_score = int(_cfg("QUANT_BO_MIN_SCORE", 4))
        block_sec = float(_cfg("QUANT_BO_BLOCK_SEC", 900))
        retest_timeout = float(_cfg("QUANT_BO_RETEST_TIMEOUT", 900))

        def _fire(direction, score, extreme, midpt):
            self._breakout_active = True
            self._breakout_dir = direction
            self._breakout_until = now + block_sec
            self._bo_extreme = extreme
            self._bo_midpoint = midpt
            self._bo_atr = atr
            self._retest_low = extreme if direction == "up" else 1e18
            self._retest_high = extreme if direction == "down" else 0.0
            self._retest_started = False
            self._retest_ready = False
            self._retest_invalidated = False   # v4.7: fresh breakout
            self._retest_timeout = now + retest_timeout
            detail_str = " | ".join(f"{k}={v}" for k, v in details.items())
            logger.info(
                f"🚀 BREAKOUT {direction.upper()} (score={score}/{min_score}) | "
                f"{detail_str} | Block {block_sec/60:.0f}min | "
                f"Waiting for retest entry")

        # v4.7 FLAW 4: Apply directional cooldown
        def _cooled_down(direction):
            """Returns False if this direction is in cooldown from a recent expiry."""
            if not self._last_expired_dir:
                return True
            # Opposite direction of recently expired breakout — apply cooldown
            opposite = {"up": "down", "down": "up"}
            if direction == opposite.get(self._last_expired_dir):
                if now - self._last_expired_time < opposite_cooldown:
                    return False
            return True

        if score_up >= min_score and score_up > score_down and _cooled_down("up"):
            extreme = float(last['h'])
            mid = (float(last['o']) + float(last['c'])) / 2.0
            _fire("up", score_up, extreme, mid)

        elif score_down >= min_score and score_down > score_up and _cooled_down("down"):
            extreme = float(last['l'])
            mid = (float(last['o']) + float(last['c'])) / 2.0
            _fire("down", score_down, extreme, mid)

    def _track_retest(self, price: float, atr: float, now: float):
        """Track pullback/retest state after breakout is detected.

        v4.7 FIXES:
          - FLAW 2: Only logs on state transitions, not every tick
          - FLAW 3: Once retrace > pullback_max, permanently invalidated
                    (was re-arming on next tick because retrace >= pullback_min)
        """
        if now > self._retest_timeout:
            return

        # v4.7 FLAW 3: Once invalidated, stays dead. No re-arming.
        if self._retest_invalidated:
            return

        pullback_min = 0.3 * self._bo_atr
        pullback_max = 2.0 * self._bo_atr  # v4.7: raised from 1.5 to 2.0 (BTC moves big)

        if self._breakout_dir == "up":
            if price < self._retest_low:
                self._retest_low = price
            retrace = self._bo_extreme - price

            # Invalidate if retrace too deep — BEFORE checking start/ready
            if retrace > pullback_max:
                if not self._retest_invalidated:
                    self._retest_invalidated = True
                    logger.info(f"❌ Retest invalidated: retrace {retrace:.0f} > max {pullback_max:.0f}")
                return

            # State transition: pullback started
            if retrace >= pullback_min and not self._retest_started:
                self._retest_started = True
                logger.info(f"📐 Retest pullback started: retrace {retrace:.0f} "
                           f"from ${self._bo_extreme:,.2f}")

            # State transition: pullback complete (bounce from low)
            if self._retest_started and not self._retest_ready:
                bounce = price - self._retest_low
                if bounce > 0.2 * self._bo_atr:
                    self._retest_ready = True
                    logger.info(f"✅ Retest ready: low ${self._retest_low:,.2f} "
                               f"→ bounce ${price:,.2f} (+{bounce:.0f})")

        else:  # breakout down
            if price > self._retest_high:
                self._retest_high = price
            retrace = price - self._bo_extreme

            if retrace > pullback_max:
                if not self._retest_invalidated:
                    self._retest_invalidated = True
                    logger.info(f"❌ Retest invalidated: retrace {retrace:.0f} > max {pullback_max:.0f}")
                return

            if retrace >= pullback_min and not self._retest_started:
                self._retest_started = True
                logger.info(f"📐 Retest pullback started: retrace {retrace:.0f} "
                           f"from ${self._bo_extreme:,.2f}")

            if self._retest_started and not self._retest_ready:
                bounce = self._retest_high - price
                if bounce > 0.2 * self._bo_atr:
                    self._retest_ready = True
                    logger.info(f"✅ Retest ready: high ${self._retest_high:,.2f} "
                               f"→ bounce ${price:,.2f} (+{bounce:.0f})")

    @property
    def is_active(self) -> bool:
        return self._breakout_active

    @property
    def direction(self) -> str:
        return self._breakout_dir

    @property
    def retest_ready(self) -> bool:
        return self._retest_ready

    @property
    def retest_sl(self) -> float:
        """SL for retest entry — below pullback low (long) / above pullback high (short)."""
        buf = self._bo_atr * 0.3 if self._bo_atr > 0 else 5.0
        if self._breakout_dir == "up":
            return self._retest_low - buf
        else:
            return self._retest_high + buf

    def blocks_reversion(self, side: str) -> bool:
        if not self._breakout_active:
            return False
        if self._breakout_dir == "up" and side == "short":
            return True
        if self._breakout_dir == "down" and side == "long":
            return True
        return False

    def allows_momentum_entry(self, side: str) -> bool:
        if not self._breakout_active:
            return False
        if self._breakout_dir == "up" and side == "long":
            return True
        if self._breakout_dir == "down" and side == "short":
            return True
        return False
class InstitutionalLevels:
    """
    Computes SL/TP/Trail levels using:
    1. Volume Profile (from candles) — find High-Volume Nodes where price consolidates
    2. Orderbook Liquidity Walls — find where large resting orders cluster
    3. Swing Structure — recent pivot highs/lows on multiple timeframes
    4. VWAP bands (±1σ, ±2σ) — institutional reference levels

    SL: Behind the strongest protective level (wall / HVN / swing)
    TP: At the nearest attraction level toward VWAP (HVN / wall / VWAP itself)
    Trail: Follow micro-swings on 1m, tighten when volume decays or wall disappears
    """

    @staticmethod
    def build_volume_profile(candles: List[Dict], bucket_count: int = 50) -> List[Tuple[float, float, float]]:
        """Build volume-at-price profile. Returns [(price_low, price_high, volume), ...] sorted by price."""
        if len(candles) < 10:
            return []
        all_highs = [float(c['h']) for c in candles]
        all_lows = [float(c['l']) for c in candles]
        price_min = min(all_lows)
        price_max = max(all_highs)
        rng = price_max - price_min
        if rng < 1e-10:
            return []
        bucket_size = rng / bucket_count
        buckets = [0.0] * bucket_count
        for c in candles:
            hi = float(c['h']); lo = float(c['l']); vol = float(c['v'])
            if vol < 1e-10:
                continue
            # Distribute volume across price buckets the candle spans
            lo_idx = max(0, int((lo - price_min) / bucket_size))
            hi_idx = min(bucket_count - 1, int((hi - price_min) / bucket_size))
            span = max(hi_idx - lo_idx + 1, 1)
            vol_per_bucket = vol / span
            for i in range(lo_idx, hi_idx + 1):
                if 0 <= i < bucket_count:
                    buckets[i] += vol_per_bucket
        result = []
        for i in range(bucket_count):
            bp_low = price_min + i * bucket_size
            bp_high = bp_low + bucket_size
            result.append((bp_low, bp_high, buckets[i]))
        return result

    @staticmethod
    def find_hvn_levels(profile: List[Tuple[float, float, float]], threshold_pctile: float = 0.70) -> List[float]:
        """Find high-volume node price levels (midpoints of top-percentile buckets)."""
        if not profile:
            return []
        volumes = [v for _, _, v in profile]
        if max(volumes) < 1e-10:
            return []
        sorted_vols = sorted(volumes)
        cutoff_idx = int(len(sorted_vols) * threshold_pctile)
        cutoff_vol = sorted_vols[min(cutoff_idx, len(sorted_vols) - 1)]
        hvns = []
        for lo, hi, vol in profile:
            if vol >= cutoff_vol:
                hvns.append((lo + hi) / 2.0)
        return hvns

    @staticmethod
    def find_orderbook_walls(orderbook: Dict, side: str, depth: int = 20, wall_mult: float = 2.5) -> List[Tuple[float, float]]:
        """
        Find price levels where resting liquidity is wall_mult × average.
        Returns [(price, qty), ...] sorted by qty descending.
        side='bid' for support walls, 'ask' for resistance walls.
        """
        levels = orderbook.get("bids" if side == "bid" else "asks", [])
        if not levels or len(levels) < 3:
            return []
        parsed = []
        for lvl in levels[:depth]:
            try:
                if isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
                    parsed.append((float(lvl[0]), float(lvl[1])))
            except (ValueError, TypeError):
                continue
        if not parsed:
            return []
        avg_qty = sum(q for _, q in parsed) / len(parsed)
        if avg_qty < 1e-12:
            return []
        walls = [(p, q) for p, q in parsed if q >= avg_qty * wall_mult]
        walls.sort(key=lambda x: x[1], reverse=True)
        return walls

    @staticmethod
    def find_swing_extremes(candles: List[Dict], lookback: int = 12) -> Tuple[List[float], List[float]]:
        """Find swing highs and swing lows from candle data.
        A swing high: c[i] high > c[i-1] high AND c[i] high > c[i+1] high.
        Returns (swing_highs, swing_lows) as price lists."""
        if len(candles) < 3:
            return [], []
        recent = candles[-lookback:] if len(candles) >= lookback else candles
        highs = []
        lows = []
        for i in range(1, len(recent) - 1):
            h = float(recent[i]['h'])
            l = float(recent[i]['l'])
            if h > float(recent[i-1]['h']) and h > float(recent[i+1]['h']):
                highs.append(h)
            if l < float(recent[i-1]['l']) and l < float(recent[i+1]['l']):
                lows.append(l)
        return highs, lows

    @staticmethod
    def _swing_cluster_score(level: float, all_swings: List[float], atr: float) -> float:
        """
        Score a structural level by how many other swing extremes cluster near it.
        Clustered swings = contested zone = stronger barrier = better SL anchor.

        Returns score in [1.0, 2.0]:
          1.0 = isolated swing (weakest)
          2.0 = four or more nearby swings (institutional zone)
        """
        if not all_swings or atr < 1e-10:
            return 1.0
        radius = QCfg.SL_SWING_DENSITY_WINDOW() * atr
        nearby = sum(1 for s in all_swings if abs(s - level) <= radius and abs(s - level) > 1e-10)
        return 1.0 + min(nearby / 4.0, 1.0)

    @staticmethod
    def compute_sl(price: float, side: str, atr: float,
                   candles_5m: List[Dict], candles_1m: List[Dict],
                   orderbook: Dict, vwap: float, vwap_std: float,
                   atr_pctile: float = 0.5,
                   candles_15m: Optional[List[Dict]] = None) -> float:
        """
        Institutional SL placement — v4.1.

        Multi-timeframe swing density scoring + adaptive ATR buffer + VP HVN + OB walls.

        Key improvements over v4.0:
          - Three timeframe inputs: 15m (macro), 5m (local), 1m (micro)
          - Each candidate scored by structural density (clustered swings = stronger zone)
          - Buffer adapts to ATR percentile: high-vol = tighter, low-vol = wider
          - Candidates scored independently; best-scored within valid range wins
          - No silent fallback to arbitrary ATR multiples

        Score components:
          15m swing: 4.0 × cluster_density  (macro structure — highest weight)
          5m  swing: 3.0 × cluster_density  (local structure)
          OB walls:  2.5 × wall_size_norm   (resting liquidity)
          VP HVN:    2.0                     (historical volume gravity)
          VWAP bands: 1.5 / 2.0 / 1.5       (statistical reference)
        """
        # Adaptive buffer: high-vol regime already has wide ranges baked in,
        # needs less extra room. Low-vol = sticky structure, needs wider clearance.
        # buf_mult ∈ [0.25×, 0.40×, 0.57×] at pctile=[1.0, 0.5, 0.0]
        buf_mult = QCfg.SL_BUFFER_ATR_MULT() * (1.4 - 0.8 * min(max(atr_pctile, 0.0), 1.0))
        buffer   = buf_mult * atr
        min_dist = price * QCfg.MIN_SL_PCT()
        max_dist = price * QCfg.MAX_SL_PCT()

        # ── Collect swing extremes from all available timeframes ──────────────
        sh_5m,  sl_5m  = [], []
        sh_15m, sl_15m = [], []
        sh_1m,  sl_1m  = [], []

        lb = QCfg.SL_SWING_LOOKBACK()
        if len(candles_5m) >= 3:
            sh_5m, sl_5m = InstitutionalLevels.find_swing_extremes(candles_5m, lb)
        if candles_15m and len(candles_15m) >= 3:
            sh_15m, sl_15m = InstitutionalLevels.find_swing_extremes(
                candles_15m, min(lb, len(candles_15m) - 2))
        if len(candles_1m) >= 3:
            sh_1m, sl_1m = InstitutionalLevels.find_swing_extremes(
                candles_1m, min(20, len(candles_1m) - 2))

        # All-timeframe pools for cluster density scoring
        all_lows  = sl_5m  + sl_15m  + sl_1m
        all_highs = sh_5m  + sh_15m  + sh_1m

        # ── OB walls (scored by relative wall size) ───────────────────────────
        wall_side = "bid" if side == "long" else "ask"
        walls = InstitutionalLevels.find_orderbook_walls(
            orderbook, wall_side, QCfg.OB_WALL_DEPTH(), QCfg.OB_WALL_MULT())
        total_wall_qty = sum(q for _, q in walls) if walls else 0.0

        # ── Volume profile HVN ────────────────────────────────────────────────
        hvn_levels: List[float] = []
        if len(candles_1m) >= 30:
            profile = InstitutionalLevels.build_volume_profile(
                candles_1m[-150:], QCfg.VP_BUCKET_COUNT())
            hvn_levels = InstitutionalLevels.find_hvn_levels(profile, QCfg.VP_HVN_THRESHOLD())

        # ── Candidate pool: (sl_level, score) ────────────────────────────────
        scored: List[Tuple[float, float]] = []

        def add(level: float, score: float) -> None:
            dist = abs(price - level)
            if dist < min_dist or dist > max_dist:
                return
            if side == "long" and level >= price:
                return
            if side == "short" and level <= price:
                return
            scored.append((level, score))

        if side == "long":
            # 15m swing lows — macro structure (highest weight)
            for lvl in sl_15m:
                if lvl < price:
                    cs = InstitutionalLevels._swing_cluster_score(lvl, all_lows, atr)
                    add(lvl - buffer * 0.80, 4.0 * cs)

            # 5m swing lows — local structure
            for lvl in sl_5m:
                if lvl < price:
                    cs = InstitutionalLevels._swing_cluster_score(lvl, all_lows, atr)
                    add(lvl - buffer, 3.0 * cs)

            # 1m micro-swing lows — fine structure (lower weight)
            for lvl in sl_1m:
                if lvl < price:
                    cs = InstitutionalLevels._swing_cluster_score(lvl, all_lows, atr)
                    add(lvl - buffer * 0.50, 1.5 * cs)

            # OB bid walls below price
            for wp, wq in walls:
                if wp < price:
                    wscore = (wq / total_wall_qty * len(walls)) if total_wall_qty > 0 else 1.0
                    add(wp - buffer * 0.40, 2.5 * min(wscore, 2.0))

            # Volume profile HVN below price
            for h in hvn_levels:
                if h < price - min_dist * 0.4:
                    add(h - buffer * 0.25, 2.0)

            # VWAP ± σ bands
            if vwap > 0 and vwap_std > 0:
                for mult, bscore in [(1.5, 1.5), (2.0, 2.0), (2.5, 1.5)]:
                    add(vwap - mult * vwap_std - buffer * 0.20, bscore)

        else:  # side == "short"
            for lvl in sh_15m:
                if lvl > price:
                    cs = InstitutionalLevels._swing_cluster_score(lvl, all_highs, atr)
                    add(lvl + buffer * 0.80, 4.0 * cs)

            for lvl in sh_5m:
                if lvl > price:
                    cs = InstitutionalLevels._swing_cluster_score(lvl, all_highs, atr)
                    add(lvl + buffer, 3.0 * cs)

            for lvl in sh_1m:
                if lvl > price:
                    cs = InstitutionalLevels._swing_cluster_score(lvl, all_highs, atr)
                    add(lvl + buffer * 0.50, 1.5 * cs)

            for wp, wq in walls:
                if wp > price:
                    wscore = (wq / total_wall_qty * len(walls)) if total_wall_qty > 0 else 1.0
                    add(wp + buffer * 0.40, 2.5 * min(wscore, 2.0))

            for h in hvn_levels:
                if h > price + min_dist * 0.4:
                    add(h + buffer * 0.25, 2.0)

            if vwap > 0 and vwap_std > 0:
                for mult, bscore in [(1.5, 1.5), (2.0, 2.0), (2.5, 1.5)]:
                    add(vwap + mult * vwap_std + buffer * 0.20, bscore)

        # ── Select best-scored candidate ──────────────────────────────────────
        if scored:
            # Primary sort: highest score. Tiebreak: closest to price (least risk).
            if side == "long":
                scored.sort(key=lambda x: (-x[1], price - x[0]))
            else:
                scored.sort(key=lambda x: (-x[1], x[0] - price))
            sl = scored[0][0]
        else:
            # No structural candidate found — minimal ATR-based floor
            sl = (price - max(min_dist, min(max_dist, 1.5 * atr))
                  if side == "long" else
                  price + max(min_dist, min(max_dist, 1.5 * atr)))

        # Final hard clamp to [min_dist, max_dist]
        dist = abs(price - sl)
        if dist < min_dist:
            sl = (price - min_dist) if side == "long" else (price + min_dist)
        elif dist > max_dist:
            sl = (price - max_dist) if side == "long" else (price + max_dist)

        return sl

    @staticmethod
    def compute_tp(price: float, side: str, atr: float, sl_price: float,
                   candles_1m: List[Dict], orderbook: Dict,
                   vwap: float, vwap_std: float,
                   candles_5m: Optional[List[Dict]] = None,
                   ict_engine=None,
                   now_ms: int = 0) -> float:
        """
        Institutional TP placement — v4.9 (ICT structural targets added).

        v4.9 ADDITION: ICT structural targets integrated as highest-conviction
        candidates. In priority order:
          6.0+ Swept liquidity origin — after sweep-and-reverse, price strongly
               targets the sweep level. This is the primary ICT delivery target.
          5.0+ Unfilled FVG in trade direction — imbalances attract price.
               Near edge = conservative partial fill. Far edge = full fill.
          4.0+ Virgin OB in trade direction — institutional order magnet.
          5.0  OB liquidity wall (orderbook) directly in path
          4.0  VP High-Volume Node between entry and VWAP
          3.5  VWAP itself (full mean-reversion target)
          3.0  VWAP ± σ bands
          2.5  5m swing extreme in direction of reversion
          2.0  VWAP partial fraction

        v4.6: SL is now ATR-capped upstream (_compute_sl_tp caps to SL_MAX_ATR_MULT×ATR).
        This means sl_dist is proportional to ATR, so R:R-based bounds are naturally
        reasonable. min_tp ≈ 4×ATR × 1.5 = 6×ATR (reachable) instead of 10×ATR × 1.5 = 15×ATR.
        """
        sl_dist = abs(price - sl_price)
        if sl_dist < 1e-10:
            return price + atr if side == "long" else price - atr

        min_tp_dist  = sl_dist * QCfg.REVERSION_MIN_RR()
        max_tp_dist  = sl_dist * QCfg.REVERSION_MAX_RR()

        # ── Volume profile: single pass, shared across candidate generation ──
        hvn_levels: List[float] = []
        if len(candles_1m) >= 30:
            profile = InstitutionalLevels.build_volume_profile(
                candles_1m[-150:], QCfg.VP_BUCKET_COUNT())
            hvn_levels = InstitutionalLevels.find_hvn_levels(profile, QCfg.VP_HVN_THRESHOLD())

        # ── OB walls in direction of trade ───────────────────────────────────
        wall_side = "ask" if side == "long" else "bid"
        walls = InstitutionalLevels.find_orderbook_walls(
            orderbook, wall_side, QCfg.OB_WALL_DEPTH(), QCfg.OB_WALL_MULT())
        total_wall_qty = sum(q for _, q in walls) if walls else 0.0

        # ── 5m swing targets in reversion direction ───────────────────────────
        sh_5m: List[float] = []
        sl_5m: List[float] = []
        if candles_5m and len(candles_5m) >= 3:
            sh_5m, sl_5m = InstitutionalLevels.find_swing_extremes(
                candles_5m, QCfg.SL_SWING_LOOKBACK())

        # ── Scored candidate pool: (tp_level, score) ─────────────────────────
        scored: List[Tuple[float, float]] = []

        def add_tp(level: float, score: float) -> None:
            dist = abs(level - price)
            if dist < min_tp_dist or dist > max_tp_dist:
                return
            if side == "long" and level <= price:
                return
            if side == "short" and level >= price:
                return
            scored.append((level, score))

        if side == "long":
            # OB ask walls: hard resistance — stop just below the wall
            for wp, wq in walls:
                if wp > price + min_tp_dist:
                    w_rel = (wq / total_wall_qty * len(walls)) if total_wall_qty > 0 else 1.0
                    add_tp(wp - atr * 0.08, 5.0 * min(w_rel, 1.5))

            # VP HVN between entry and VWAP (gravity nodes)
            for h in hvn_levels:
                if price < h and (vwap <= 0 or h <= vwap * 1.002):
                    add_tp(h, 4.0)

            # VWAP full reversion
            if vwap > price:
                add_tp(vwap, 3.5)

            # VWAP sigma bands
            if vwap > 0 and vwap_std > 0:
                for mult, bscore in [(0.5, 3.0), (1.0, 3.0), (1.5, 2.5)]:
                    lvl = vwap - mult * vwap_std
                    if lvl > price:
                        add_tp(lvl, bscore)

            # 5m swing highs in reversion zone (potential supply)
            for sh in sh_5m:
                if sh > price + min_tp_dist and (vwap <= 0 or sh <= vwap * 1.005):
                    add_tp(sh - atr * 0.10, 2.5)

            # Fractional VWAP reversion
            if vwap > price:
                partial = price + (vwap - price) * QCfg.TP_VWAP_FRACTION()
                add_tp(partial, 2.0)

        else:  # short
            for wp, wq in walls:
                if wp < price - min_tp_dist:
                    w_rel = (wq / total_wall_qty * len(walls)) if total_wall_qty > 0 else 1.0
                    add_tp(wp + atr * 0.08, 5.0 * min(w_rel, 1.5))

            for h in hvn_levels:
                if price > h and (vwap <= 0 or h >= vwap * 0.998):
                    add_tp(h, 4.0)

            if vwap < price:
                add_tp(vwap, 3.5)

            if vwap > 0 and vwap_std > 0:
                for mult, bscore in [(0.5, 3.0), (1.0, 3.0), (1.5, 2.5)]:
                    lvl = vwap + mult * vwap_std
                    if lvl < price:
                        add_tp(lvl, bscore)

            for sl_v in sl_5m:
                if sl_v < price - min_tp_dist and (vwap <= 0 or sl_v >= vwap * 0.995):
                    add_tp(sl_v + atr * 0.10, 2.5)

            if vwap < price:
                partial = price + (vwap - price) * QCfg.TP_VWAP_FRACTION()
                add_tp(partial, 2.0)

        # ── v4.9: ICT Structural TP targets ──────────────────────────────────
        # Swept liquidity origins, unfilled FVGs, and virgin OBs in trade direction.
        # These carry the highest institutional conviction — they are WHERE smart
        # money is delivering price to. Add them to the scored pool with priority
        # scores that beat every quant-only candidate.
        if ict_engine is not None:
            try:
                _ict_now_ms = now_ms if now_ms > 0 else int(time.time() * 1000)
                _ict_targets = ict_engine.get_structural_tp_targets(
                    side, price, atr, _ict_now_ms, min_tp_dist, max_tp_dist)
                for _lvl, _sc, _lbl in _ict_targets:
                    add_tp(_lvl, _sc)
                    logger.debug(f"ICT TP candidate: ${_lvl:,.1f} score={_sc:.1f} [{_lbl}]")
            except Exception as _ict_e:
                logger.debug(f"ICT TP targets error (non-fatal): {_ict_e}")

        # ── Tiered selection ──────────────────────────────────────────────────
        if scored:
            # Tier-A: score ≥ 3.5 → nearest (maximises win-rate)
            tier_a = [(lvl, sc) for lvl, sc in scored if sc >= 3.5]
            if tier_a:
                tp = min(tier_a, key=lambda x: abs(x[0] - price))[0]
            else:
                # Tier-B: below 3.5 → highest-scored (maximises expected value)
                scored.sort(key=lambda x: -x[1])
                tp = scored[0][0]
        else:
            # No structural target found — enforce minimum R:R floor only
            tp = (price + min_tp_dist) if side == "long" else (price - min_tp_dist)

        return tp

    @staticmethod
    def compute_tp_trend(price: float, side: str, atr: float, sl_price: float,
                         candles_5m: List[Dict], orderbook: Dict,
                         swing_lookback: int = 12) -> float:
        """
        Trend-following TP placement — v4.2.

        In a trending market VWAP is behind price and is NOT the target.
        Targets in order of preference:

          1. Previous swing high/low (the one that started the current pullback)
             — price broke that level → it becomes a continuation target
          2. ATR channel extension: entry + TREND_TP_ATR_MULT × ATR
             — the standard measured-move projection
          3. Next orderbook resistance/support wall in trend direction
             — where resting liquidity will absorb momentum

        All candidates must satisfy TREND_MIN_RR and are capped at TREND_MAX_RR.
        If no structural target is found, ATR channel is used as the hard floor.
        """
        sl_dist     = abs(price - sl_price)
        if sl_dist < 1e-10:
            return price + atr if side == "long" else price - atr

        min_tp_dist = sl_dist * QCfg.TREND_MIN_RR()     # v4.4: was MIN_RR_RATIO
        max_tp_dist = sl_dist * QCfg.TREND_MAX_RR()      # v4.4: was TP_MAX_RR

        # ── ATR-channel baseline (always valid) ───────────────────────────────
        atr_tp_dist  = atr * QCfg.TREND_TP_ATR_MULT()
        atr_tp_dist  = max(atr_tp_dist, min_tp_dist)   # at least MIN_RR
        atr_tp_dist  = min(atr_tp_dist, max_tp_dist)   # capped at MAX_RR
        atr_tp       = (price + atr_tp_dist) if side == "long" else (price - atr_tp_dist)

        # ── Swing target: recent pivot that started the pullback ──────────────
        swing_tp: Optional[float] = None
        if len(candles_5m) >= 3:
            sh, sl_list = InstitutionalLevels.find_swing_extremes(candles_5m, swing_lookback)
            if side == "long" and sh:
                # Highs above price that are within max_tp_dist
                valid = [h for h in sh if price + min_tp_dist < h <= price + max_tp_dist]
                if valid:
                    swing_tp = min(valid) - 0.05 * atr   # just below the level
            elif side == "short" and sl_list:
                valid = [l for l in sl_list if price - max_tp_dist <= l < price - min_tp_dist]
                if valid:
                    swing_tp = max(valid) + 0.05 * atr

        # ── OB wall in trend direction ─────────────────────────────────────────
        wall_side = "ask" if side == "long" else "bid"
        walls     = InstitutionalLevels.find_orderbook_walls(
            orderbook, wall_side, QCfg.OB_WALL_DEPTH(), QCfg.OB_WALL_MULT())
        wall_tp: Optional[float] = None
        if walls:
            if side == "long":
                valid = [(p, q) for p, q in walls
                         if price + min_tp_dist < p <= price + max_tp_dist]
                if valid:
                    best = min(valid, key=lambda x: x[0])   # nearest wall
                    wall_tp = best[0] - 0.08 * atr
            else:
                valid = [(p, q) for p, q in walls
                         if price - max_tp_dist <= p < price - min_tp_dist]
                if valid:
                    best = max(valid, key=lambda x: x[0])
                    wall_tp = best[0] + 0.08 * atr

        # ── Select: swing > ATR-channel > wall (prefer structural levels) ──────
        # For trend trades we prefer the ATR channel over swing when the swing is
        # very close (< 1.5× sl_dist) — that would be a weak R:R.
        candidates: List[float] = []
        if swing_tp is not None:
            dist = abs(swing_tp - price)
            if dist >= sl_dist * 1.5:   # at least 1.5:1 from structural target
                candidates.append(swing_tp)
        candidates.append(atr_tp)       # ATR channel always included
        if wall_tp is not None:
            candidates.append(wall_tp)

        if side == "long":
            valid_c = [c for c in candidates if price + min_tp_dist < c <= price + max_tp_dist]
            return max(valid_c) if valid_c else atr_tp   # take farthest valid (trend continuation)
        else:
            valid_c = [c for c in candidates if price - max_tp_dist <= c < price - min_tp_dist]
            return min(valid_c) if valid_c else atr_tp

    @staticmethod
    def _classify_pullback_vs_reversal(
            pos_side: str, price: float, entry_price: float, atr: float,
            candles_1m: List[Dict], candles_5m: List[Dict],
            orderbook: Dict, entry_vol: float,
            peak_price_abs: float) -> Tuple[bool, int, str]:
        """
        Institutional pullback-vs-reversal classifier — v4.5.

        Evaluates 6 independent signals. Returns:
          (is_pullback: bool, reversal_count: int, detail: str)

        A pullback is a healthy retracement within the trend/reversion move.
        A reversal is a structural shift where the trade thesis is invalidated.

        The 6 signals (each scores 0 or 1 toward reversal):

        1. VOLUME PROFILE: Is pullback volume expanding relative to impulse?
           Healthy pullback = declining volume. Reversal = expanding.

        2. RETRACE DEPTH: How deep is the pullback relative to ATR?
           Shallow (< PB_DEPTH_ATR) = pullback. Deep = reversal warning.

        3. CANDLE CHARACTER: Are retrace candles large-bodied vs impulse?
           Small-bodied retrace = pullback. Large opposing candles = reversal.

        4. ORDERBOOK SHIFT: Has bid/ask imbalance flipped against the trade?
           Still favoring trade direction = pullback. Flipped = reversal.

        5. SWING STRUCTURE (5m): Has price broken the last confirmed 5m swing?
           Swing holding = pullback. Swing broken = reversal.

        6. MOMENTUM STALLING: Is price making lower highs (long) or higher lows (short)
           over the last 5+ candles? Momentum continuation = pullback. Stalling = reversal.
        """
        reversal_signals = 0
        details = []

        if atr < 1e-10 or len(candles_1m) < 10:
            return True, 0, "insufficient data"

        recent = candles_1m[-10:]
        profit = (price - entry_price) if pos_side == "long" else (entry_price - price)
        retrace_from_peak = abs(peak_price_abs - price) if peak_price_abs > 1e-10 else 0.0

        # ── Signal 1: Volume profile ─────────────────────────────────
        # Compare last 3 candles vol (retrace) vs previous 5 candles (impulse)
        if len(candles_1m) >= 10:
            retrace_vol = sum(float(c['v']) for c in candles_1m[-3:]) / 3.0
            impulse_vol = sum(float(c['v']) for c in candles_1m[-8:-3]) / 5.0
            if impulse_vol > 1e-10:
                vol_ratio = retrace_vol / impulse_vol
                if vol_ratio > QCfg.TRAIL_PB_VOL_RATIO():
                    reversal_signals += 1
                    details.append(f"vol_expand({vol_ratio:.2f})")
                else:
                    details.append(f"vol_decline({vol_ratio:.2f})")

        # ── Signal 2: Retrace depth ──────────────────────────────────
        if retrace_from_peak > QCfg.TRAIL_PB_DEPTH_ATR() * atr:
            reversal_signals += 1
            details.append(f"deep_retrace({retrace_from_peak/atr:.1f}ATR)")
        else:
            details.append(f"shallow({retrace_from_peak/atr:.1f}ATR)")

        # ── Signal 3: Candle character ───────────────────────────────
        # Large opposing candles = reversal signal
        if len(candles_1m) >= 8:
            impulse_bodies = [abs(float(c['c']) - float(c['o'])) for c in candles_1m[-8:-3]]
            retrace_bodies = [abs(float(c['c']) - float(c['o'])) for c in candles_1m[-3:]]
            avg_impulse = sum(impulse_bodies) / max(len(impulse_bodies), 1)
            avg_retrace = sum(retrace_bodies) / max(len(retrace_bodies), 1)
            if avg_impulse > 1e-10 and avg_retrace / avg_impulse > 0.80:
                reversal_signals += 1
                details.append("large_retrace_candles")

        # ── Signal 4: Orderbook shift ────────────────────────────────
        bids = orderbook.get("bids", [])
        asks = orderbook.get("asks", [])
        if bids and asks:
            bid_depth = sum(float(b[1]) for b in bids[:5]) if len(bids) >= 5 else 0
            ask_depth = sum(float(a[1]) for a in asks[:5]) if len(asks) >= 5 else 0
            total = bid_depth + ask_depth
            if total > 1e-10:
                imbalance = (bid_depth - ask_depth) / total
                # For long: negative imbalance (more asks) = reversal
                # For short: positive imbalance (more bids) = reversal
                if pos_side == "long" and imbalance < -0.15:
                    reversal_signals += 1
                    details.append(f"ob_shift({imbalance:+.2f})")
                elif pos_side == "short" and imbalance > 0.15:
                    reversal_signals += 1
                    details.append(f"ob_shift({imbalance:+.2f})")

        # ── Signal 5: 5m swing structure break ───────────────────────
        if candles_5m and len(candles_5m) >= 5:
            sh_5m, sl_5m = InstitutionalLevels.find_swing_extremes(
                candles_5m[:-1], min(8, len(candles_5m) - 2))
            if pos_side == "long" and sl_5m:
                # Last 5m swing low below entry — if price breaks below it
                relevant = [l for l in sl_5m if l > entry_price - 0.5 * atr]
                if relevant and price < min(relevant):
                    reversal_signals += 1
                    details.append("5m_swing_broken")
            elif pos_side == "short" and sh_5m:
                relevant = [h for h in sh_5m if h < entry_price + 0.5 * atr]
                if relevant and price > max(relevant):
                    reversal_signals += 1
                    details.append("5m_swing_broken")

        # ── Signal 6: Momentum stalling ──────────────────────────────
        # Last 5 candle highs declining (long) or lows rising (short)
        if len(candles_1m) >= 6:
            last5 = candles_1m[-5:]
            if pos_side == "long":
                highs = [float(c['h']) for c in last5]
                declining = all(highs[i] <= highs[i-1] for i in range(1, len(highs)))
                if declining:
                    reversal_signals += 1
                    details.append("momentum_stalling")
            else:
                lows = [float(c['l']) for c in last5]
                rising = all(lows[i] >= lows[i-1] for i in range(1, len(lows)))
                if rising:
                    reversal_signals += 1
                    details.append("momentum_stalling")

        is_pullback = reversal_signals < QCfg.TRAIL_REV_MIN_SIGNALS()
        return is_pullback, reversal_signals, "|".join(details)

    @staticmethod
    def compute_trail_sl(pos_side: str, price: float, entry_price: float,
                         current_sl: float, atr: float,
                         candles_1m: List[Dict], orderbook: Dict,
                         peak_profit: float, entry_vol: float,
                         hold_seconds: float = 0.0,
                         peak_price_abs: float = 0.0,
                         trade_mode: str = "reversion",
                         candles_5m: Optional[List[Dict]] = None,
                         initial_sl_dist: float = 0.0,
                         ict_engine=None,
                         now_ms: int = 0) -> Optional[float]:
        """
        Institutional trailing SL — v4.9 (ICT-anchored, zone-aware).

        v4.9 ADDITIONS on top of v4.8's 7-bug rewrite:

        NEW 1: ICT ZONE FREEZE
               If price is testing an active Order Block or sitting inside an FVG,
               the trail is COMPLETELY FROZEN. These are institutional zones where
               smart money placed orders. A test of an OB is not a reversal —
               it is the setup completing. Freezing the trail here eliminates the
               "SL hit during pullback, then TP fires without us" pattern.

        NEW 2: ICT OB ANCHOR (additional SL candidate)
               If an active OB exists between current_sl and price, compute SL =
               OB.low - ICT_OB_SL_BUFFER_ATR × ATR (for long). This is the most
               institutionally valid SL level — that is literally WHERE the orders
               are sitting. The structure holds or it doesn't. OB anchor > chandelier.

        NEW 3: LIQUIDITY POOL CEILING
               After all candidates are built, cap the trail SL so it cannot advance
               PAST an unswept liquidity pool (EQL for long, EQH for short) in the
               direction of price. Smart money sweeps those pools — trailing SL right
               at the pool means the sweep takes us out before the reversal.
               Keep SL safely BEYOND the pool level by ICT_LIQ_POOL_BUFFER_ATR × ATR.

        Returns None if no improvement qualifies, if in ICT zone freeze, or if
        the pullback classifier says to hold.
        """
        if candles_5m is None:
            candles_5m = []

        # v4.8 BUG 1 FIX: Use ORIGINAL SL distance, not current
        # After trail moves SL from $73,320 to $73,600, using current SL
        # makes init_dist=$200→$80, inflating tier from 0.6 to 1.5 instantly.
        init_dist = initial_sl_dist if initial_sl_dist > 1e-10 else (
            abs(entry_price - current_sl) if abs(entry_price - current_sl) > 1e-10 else atr)

        profit = (price - entry_price) if pos_side == "long" else (entry_price - price)

        # v4.8 BUG 2 FIX: Phase is determined by PEAK profit, not current.
        # During a healthy pullback from +2R to +1R, tier drops and Phase 2
        # demotes to Phase 1, losing chandelier exactly when needed.
        # Phase RATCHETS UP — once earned, never lost.
        tier_profit = max(profit, peak_profit)
        tier = tier_profit / init_dist if init_dist > 1e-10 else 0.0

        if atr < 1e-10:
            return None

        # ═══ PHASE 0: HANDS OFF ═══════════════════════════════════════════
        if tier < QCfg.TRAIL_BE_R():
            return None

        # ═══ DETERMINE PHASE AND MIN DISTANCE ═════════════════════════════
        if tier >= QCfg.TRAIL_AGGRESSIVE_R():
            phase = 3
            min_dist = QCfg.TRAIL_MIN_DIST_ATR_P3() * atr
        elif tier >= QCfg.TRAIL_LOCK_R():
            phase = 2
            min_dist = QCfg.TRAIL_MIN_DIST_ATR_P2() * atr
        else:
            phase = 1
            min_dist = QCfg.TRAIL_MIN_DIST_ATR_P1() * atr

        # ═══ v4.9: ICT ZONE FREEZE ════════════════════════════════════════
        # If price is testing an active OB or sitting inside an FVG, FREEZE.
        # These are institutional accumulation/distribution zones. A pullback
        # into an OB is the setup working as intended — not a reversal signal.
        # Tightening the SL during an OB test causes the "stopped out, then TP
        # fires without us" pattern that was the primary reported problem.
        if QCfg.ICT_ZONE_FREEZE_ENABLED() and ict_engine is not None:
            try:
                _ict_now_ms = now_ms if now_ms > 0 else int(time.time() * 1000)
                _freeze_atr  = QCfg.ICT_ZONE_FREEZE_ATR() * atr

                # Check active Order Blocks in trade direction
                _obs = (ict_engine.order_blocks_bull if pos_side == "long"
                        else ict_engine.order_blocks_bear)
                for _ob in _obs:
                    if not _ob.is_active(_ict_now_ms):
                        continue
                    # Freeze zone: OB.low - freeze_atr to OB.high + freeze_atr
                    _zone_lo = _ob.low  - _freeze_atr
                    _zone_hi = _ob.high + _freeze_atr
                    if _zone_lo <= price <= _zone_hi:
                        logger.debug(
                            f"Trail: ICT OB ZONE FREEZE — price ${price:,.1f} "
                            f"in OB ${_ob.low:,.1f}–${_ob.high:,.1f} "
                            f"(±{_freeze_atr:.0f} buffer). Trade thesis intact.")
                        return None

                # Check active FVGs in trade direction (price inside gap = fill in progress)
                _fvgs = (ict_engine.fvgs_bull if pos_side == "long"
                         else ict_engine.fvgs_bear)
                for _fvg in _fvgs:
                    if not _fvg.is_active(_ict_now_ms):
                        continue
                    # FVG fill: price returned into the imbalance zone
                    _fvg_lo = _fvg.bottom - _freeze_atr * 0.5
                    _fvg_hi = _fvg.top    + _freeze_atr * 0.5
                    if _fvg_lo <= price <= _fvg_hi:
                        logger.debug(
                            f"Trail: ICT FVG ZONE FREEZE — price ${price:,.1f} "
                            f"in FVG ${_fvg.bottom:,.1f}–${_fvg.top:,.1f}. "
                            f"FVG fill in progress — holding SL.")
                        return None
            except Exception as _ict_e:
                logger.debug(f"Trail ICT zone check error (non-fatal): {_ict_e}")

        # ═══ PULLBACK DETECTION (Phases 1-3) ══════════════════════════════
        if QCfg.TRAIL_PULLBACK_FREEZE():
            is_pb, rev_count, pb_detail = InstitutionalLevels._classify_pullback_vs_reversal(
                pos_side, price, entry_price, atr,
                candles_1m, candles_5m, orderbook, entry_vol, peak_price_abs)
            if is_pb:
                logger.debug(
                    f"Trail: PULLBACK ({rev_count}/{QCfg.TRAIL_REV_MIN_SIGNALS()} "
                    f"reversal) — FROZEN [{pb_detail}]")
                return None

        # ═══ BUILD CANDIDATE SL LEVELS ════════════════════════════════════
        # v4.9 INSTITUTIONAL PRIORITY — structure first, chandelier last resort:
        #   1. Profit floor      — fee-adjusted breakeven (all phases)
        #   2. ICT OB anchor     — WHERE institutional orders are (all phases)
        #   3. 5m swing low/high — market's confirmed structure (all phases)
        #   4. 1m micro-swing    — tighter confirmed structure (Phase 2+)
        #   5. Chandelier        — Phase 3 ONLY when no structure found
        #   6. HVN               — volume-based support (Phase 3 only)
        #   7. OB wall           — orderbook depth support (Phase 3 only)
        #
        # Chandelier was previously Phase 2+, causing stops on normal OB pullbacks.
        # Structure must lead. Chandelier is the fallback for featureless markets.
        candidates: List[float] = []

        # ── 1. Profit floor (all phases) ─────────────────────────────────
        rt_fee_per_btc   = entry_price * QCfg.COMMISSION_RATE() * 2.0
        profit_floor_buf = rt_fee_per_btc + 0.3 * atr
        profit_floor = (entry_price + profit_floor_buf if pos_side == "long"
                        else entry_price - profit_floor_buf)
        candidates.append(profit_floor)

        # ── 2. ICT OB anchor (all phases) ────────────────────────────────
        # Highest-conviction structural level: SL just below an active bullish OB
        # (for long). That is literally WHERE smart money placed its buy orders.
        # The market bounces off OBs by design; SL below it survives OB tests.
        if QCfg.ICT_OB_SL_ANCHOR() and ict_engine is not None:
            try:
                _now_ms_ob = now_ms if now_ms > 0 else int(time.time() * 1000)
                _ob_buf    = QCfg.ICT_OB_SL_BUFFER_ATR() * atr
                _obs = (ict_engine.order_blocks_bull if pos_side == "long"
                        else ict_engine.order_blocks_bear)
                for _ob in sorted([o for o in _obs if o.is_active(_now_ms_ob)],
                                   key=lambda x: abs(x.midpoint - price)):
                    if pos_side == "long":
                        if current_sl < _ob.low - _ob_buf < price - min_dist:
                            candidates.append(_ob.low - _ob_buf)
                            logger.debug(
                                f"Trail: OB anchor ${_ob.low - _ob_buf:,.1f} "
                                f"(OB ${_ob.low:,.1f}–${_ob.high:,.1f} str={_ob.strength:.0f})")
                            break
                    else:
                        if price + min_dist < _ob.high + _ob_buf < current_sl:
                            candidates.append(_ob.high + _ob_buf)
                            logger.debug(
                                f"Trail: OB anchor ${_ob.high + _ob_buf:,.1f} "
                                f"(OB ${_ob.low:,.1f}–${_ob.high:,.1f} str={_ob.strength:.0f})")
                            break
            except Exception as _e:
                logger.debug(f"Trail OB anchor error: {_e}")

        # ── 3. 5m confirmed swing structure (all phases) ─────────────────
        # PRIMARY structural trail. Only CLOSED candles (confirmed, not forming).
        # A higher 5m swing low (long) = market proved a higher low → trail up.
        if candles_5m and len(candles_5m) >= 4:
            closed_5m    = candles_5m[:-1]
            sh_5m, sl_5m = InstitutionalLevels.find_swing_extremes(
                closed_5m, min(QCfg.SL_SWING_LOOKBACK(), len(closed_5m) - 2))
            swing_buf = max(0.25 * atr, QCfg.SL_BUFFER_ATR_MULT() * atr)

            if pos_side == "long" and sl_5m:
                valid = [l for l in sl_5m
                         if current_sl + 0.05 * atr < l < price - min_dist]
                if valid:
                    best_sw = max(valid)
                    candidates.append(best_sw - swing_buf)
                    logger.debug(
                        f"Trail: 5m swing low ${best_sw:,.1f} → SL ${best_sw - swing_buf:,.1f}")
            elif pos_side == "short" and sh_5m:
                valid = [h for h in sh_5m
                         if price + min_dist < h < current_sl - 0.05 * atr]
                if valid:
                    best_sw = min(valid)
                    candidates.append(best_sw + swing_buf)
                    logger.debug(
                        f"Trail: 5m swing high ${best_sw:,.1f} → SL ${best_sw + swing_buf:,.1f}")

        # ── 4. 1m micro-swing (Phase 2+) — tighter structural trail ─────
        # After sufficient profit, also trail to confirmed 1m structure.
        # Tighter buffer because 1m swings are smaller.
        if phase >= 2 and len(candles_1m) >= 8:
            closed_1m    = candles_1m[:-1]
            sh_1m, sl_1m = InstitutionalLevels.find_swing_extremes(
                closed_1m, min(QCfg.TRAIL_SWING_BARS() + 4, len(closed_1m) - 2))
            micro_buf = max(0.15 * atr, min_dist * 0.25)

            if pos_side == "long" and sl_1m:
                valid = [l for l in sl_1m if current_sl < l < price - min_dist]
                if valid:
                    best_m = max(valid)
                    candidates.append(best_m - micro_buf)
                    logger.debug(
                        f"Trail: 1m micro ${best_m:,.1f} → SL ${best_m - micro_buf:,.1f}")
            elif pos_side == "short" and sh_1m:
                valid = [h for h in sh_1m if price + min_dist < h < current_sl]
                if valid:
                    best_m = min(valid)
                    candidates.append(best_m + micro_buf)

        # ── 5. Chandelier exit (Phase 3 ONLY — last resort) ──────────────
        # Used ONLY when no structural candidate exists (featureless market, tight
        # consolidation with no confirmed swings). NOT the primary driver.
        # Previously was Phase 2+, which caused the main reported problem.
        if phase >= 3 and peak_price_abs > 1e-10:
            if trade_mode in ("trend", "momentum"):
                n_chandelier = QCfg.TREND_CHANDELIER_N()
            else:
                n_start  = QCfg.TRAIL_CHANDELIER_N_START()
                n_end    = QCfg.TRAIL_CHANDELIER_N_END()
                max_hold = float(_cfg("QUANT_MAX_HOLD_SEC", 2400))
                t_frac   = min(hold_seconds / max_hold, 1.0) if max_hold > 0 else 0.0
                n_chandelier = n_start + (n_end - n_start) * t_frac

            if pos_side == "long":
                chand = peak_price_abs - n_chandelier * atr
                if current_sl < chand < price - min_dist:
                    candidates.append(chand)
            else:
                chand = peak_price_abs + n_chandelier * atr
                if price + min_dist < chand < current_sl:
                    candidates.append(chand)

        # ── 6. HVN snap (Phase 3 only) ──────────────────────────────────
        if phase >= 3 and len(candles_1m) >= 30:
            profile = InstitutionalLevels.build_volume_profile(
                candles_1m[-150:], QCfg.VP_BUCKET_COUNT())
            for hvn in InstitutionalLevels.find_hvn_levels(
                    profile, QCfg.TRAIL_HVN_SNAP_THRESH()):
                if pos_side == "long" and current_sl < hvn < price - min_dist:
                    candidates.append(hvn - 0.15 * atr)
                elif pos_side == "short" and price + min_dist < hvn < current_sl:
                    candidates.append(hvn + 0.15 * atr)

        # ── 7. OB wall snap (Phase 3 only) ──────────────────────────────
        if phase >= 3:
            wall_side = "bid" if pos_side == "long" else "ask"
            walls = InstitutionalLevels.find_orderbook_walls(
                orderbook, wall_side, QCfg.OB_WALL_DEPTH(), QCfg.OB_WALL_MULT())
            if walls:
                if pos_side == "long":
                    vw = [(p, q) for p, q in walls if current_sl < p < price - min_dist]
                    if vw:
                        candidates.append(max(vw, key=lambda x: x[1])[0] - 0.10 * atr)
                else:
                    vw = [(p, q) for p, q in walls if price + min_dist < p < current_sl]
                    if vw:
                        candidates.append(min(vw, key=lambda x: x[1])[0] + 0.10 * atr)

        if not candidates:
            return None

        # ═══ SELECT BEST CANDIDATE ════════════════════════════════════════
        if pos_side == "long":
            new_sl = max(candidates)
        else:
            new_sl = min(candidates)

        # ── Vol-decay tightening (Phase 3 only) ──────────────────────
        if phase >= 3 and len(candles_1m) >= 10 and entry_vol > 1e-10:
            recent_vol = sum(float(c['v']) for c in candles_1m[-5:]) / 5.0
            vol_ratio  = recent_vol / entry_vol
            decay_mult = QCfg.TRAIL_VOL_DECAY_MULT()
            if vol_ratio < decay_mult:
                tighten = 0.35 * atr * (1.0 - vol_ratio / decay_mult)
                if pos_side == "long":
                    new_sl = min(new_sl + tighten, price - min_dist)
                else:
                    new_sl = max(new_sl - tighten, price + min_dist)

        # ── v4.9: LIQUIDITY POOL CEILING CAP ─────────────────────────
        # Keep the trailing SL on the FAR SIDE of any unswept liquidity pool
        # between current_sl and price. Smart money sweeps those pools — if our
        # SL is right at or past the pool, the sweep hunts us before reversal.
        # Cap the SL at: pool.price - ICT_LIQ_POOL_BUFFER (long) or pool.price + buffer (short).
        if QCfg.ICT_LIQ_CEILING_ENABLED() and ict_engine is not None:
            try:
                _ict_now_ms = now_ms if now_ms > 0 else int(time.time() * 1000)
                _liq_buf    = QCfg.ICT_LIQ_POOL_BUFFER_ATR() * atr
                for _pool in ict_engine.liquidity_pools:
                    if _pool.swept:
                        continue
                    if pos_side == "long" and _pool.pool_type == "EQL":
                        # EQL below price: SL must stay BELOW the pool minus buffer
                        # i.e., don't trail SL ABOVE the pool level
                        _ceiling = _pool.price - _liq_buf
                        if current_sl < _ceiling < new_sl:
                            # SL candidate crossed above pool — cap it
                            logger.debug(
                                f"Trail: LIQ CEILING — capping SL at ${_ceiling:,.1f} "
                                f"(EQL pool @ ${_pool.price:,.1f}, SL was ${new_sl:,.1f})")
                            new_sl = _ceiling
                    elif pos_side == "short" and _pool.pool_type == "EQH":
                        # EQH above price: SL must stay ABOVE the pool plus buffer
                        _floor = _pool.price + _liq_buf
                        if current_sl > _floor > new_sl:
                            logger.debug(
                                f"Trail: LIQ FLOOR — capping SL at ${_floor:,.1f} "
                                f"(EQH pool @ ${_pool.price:,.1f}, SL was ${new_sl:,.1f})")
                            new_sl = _floor
            except Exception as _liq_e:
                logger.debug(f"Trail liq ceiling error (non-fatal): {_liq_e}")

        # ═══ MINIMUM DISTANCE ENFORCEMENT (absolute guard) ════════════════
        if pos_side == "long":
            max_allowed = price - min_dist
            if new_sl > max_allowed:
                new_sl = max_allowed
        else:
            min_allowed = price + min_dist
            if new_sl < min_allowed:
                new_sl = min_allowed

        # ═══ RATCHET: SL may only improve ════════════════════════════════
        min_move = QCfg.TRAIL_MIN_MOVE_ATR() * atr
        if pos_side == "long":
            if new_sl <= current_sl + min_move:
                return None
        else:
            if new_sl >= current_sl - min_move:
                return None

        return new_sl

# ATR ENGINE
# ═══════════════════════════════════════════════════════════════
class ATREngine:
    def __init__(self):
        self._atr = 0.0; self._atr_hist: deque = deque(maxlen=QCfg.ATR_PCTILE_WINDOW())
        self._last_ts = -1; self._seeded = False

    def reset_state(self):
        """Force full re-seed from next candle batch after stream restart."""
        self._seeded = False
        self._last_ts = -1
        self._atr_hist.clear()
        self._atr = 0.0

    @staticmethod
    def _pctile_rank_window() -> int:
        return int(_cfg("ATR_PCTILE_RANK_WINDOW", 30))

    def compute(self, candles: List[Dict]) -> float:
        if not candles: return self._atr
        period = QCfg.ATR_PERIOD(); last_ts = int(candles[-1].get('t', 0))
        if last_ts == self._last_ts and self._seeded: return self._atr
        if len(candles) < period + 1: return self._atr
        if not self._seeded:
            trs = [max(float(candles[i]['h'])-float(candles[i]['l']),
                       abs(float(candles[i]['h'])-float(candles[i-1]['c'])),
                       abs(float(candles[i]['l'])-float(candles[i-1]['c'])))
                   for i in range(1, len(candles))]
            if len(trs) < period: return self._atr
            atr = sum(trs[:period]) / period
            for tr in trs[period:]:
                atr = (atr * (period - 1) + tr) / period
            # v4.3 CRITICAL FIX: Only keep the FINAL ATR value from warmup.
            # Old code kept 20-35 historical values that poisoned the percentile
            # ranking — if warmup came from a different vol regime, the current
            # ATR ranked at 0% against all of them, permanently blocking trades.
            # Now: keep ONLY the latest value. Percentile returns 0.5 (neutral)
            # until we accumulate enough LIVE bars to rank meaningfully.
            self._atr_hist.clear()
            self._atr_hist.append(atr)
            self._atr = atr; self._seeded = True
            self._last_ts = last_ts
            return self._atr
        else:
            hi=float(candles[-1]['h']); lo=float(candles[-1]['l']); prc=float(candles[-2]['c'])
            self._atr = (self._atr*(period-1)+max(hi-lo,abs(hi-prc),abs(lo-prc)))/period
        self._atr_hist.append(self._atr); self._last_ts = last_ts
        return self._atr

    @property
    def atr(self) -> float: return self._atr

    def get_percentile(self) -> float:
        hist = list(self._atr_hist)
        n = len(hist)
        # v4.3 FIX: Need at least half a rank window of LIVE data before
        # departing from neutral. This prevents warmup data from locking
        # the percentile at extreme values during the first ~75 min.
        min_samples = max(5, self._pctile_rank_window() // 2)
        if n < min_samples: return 0.5
        window = hist[max(0, n - self._pctile_rank_window()):]
        if len(window) < 2: return 0.5
        cur = window[-1]
        return sum(1 for h in window[:-1] if h <= cur) / (len(window) - 1)

    def regime_valid(self) -> bool:
        p = self.get_percentile()
        return QCfg.ATR_MIN_PCTILE() <= p <= QCfg.ATR_MAX_PCTILE()

    def regime_penalty(self) -> float:
        return 1.0 if self.regime_valid() else 0.0

# ═══════════════════════════════════════════════════════════════
# HTF TREND FILTER — VETO ONLY
# ═══════════════════════════════════════════════════════════════
class HTFTrendFilter:
    def __init__(self): self._trend_15m = 0.0; self._trend_4h = 0.0

    @staticmethod
    def _ema_series(values, period):
        if len(values) < period: return []
        k = 2.0/(period+1); ema = sum(values[:period])/period; out = [ema]
        for v in values[period:]: ema = v*k+ema*(1.0-k); out.append(ema)
        return out

    def update(self, candles_15m, candles_4h, atr_5m):
        fast = QCfg.EMA_FAST()
        if len(candles_15m) > fast+5 and atr_5m > 1e-10:
            ema15 = self._ema_series([float(c['c']) for c in candles_15m], fast)
            self._trend_15m = _sigmoid((ema15[-1]-ema15[-3])/atr_5m, 0.8) if len(ema15)>=4 else 0.0
        else: self._trend_15m = 0.0
        slow = QCfg.EMA_SLOW()
        if len(candles_4h) > slow+3 and atr_5m > 1e-10:
            ema4h = self._ema_series([float(c['c']) for c in candles_4h], slow)
            self._trend_4h = _sigmoid((ema4h[-1]-ema4h[-2])/(atr_5m*4.0), 0.8) if len(ema4h)>=3 else 0.0
        else: self._trend_4h = 0.0

    def vetoes_trade(self, side: str) -> bool:
        if not QCfg.HTF_ENABLED(): return False
        htf = self._trend_4h * 0.60 + self._trend_15m * 0.40
        if side == "long" and htf < -QCfg.HTF_VETO_STRENGTH(): return True
        if side == "short" and htf > QCfg.HTF_VETO_STRENGTH(): return True
        return False

    @property
    def trend_15m(self): return self._trend_15m
    @property
    def trend_4h(self): return self._trend_4h

# ═══════════════════════════════════════════════════════════════
# SIGNAL BREAKDOWN
# ═══════════════════════════════════════════════════════════════
@dataclass
class SignalBreakdown:
    vwap_dev: float = 0.0; cvd_div: float = 0.0; orderbook: float = 0.0
    tick_flow: float = 0.0; vol_exhaust: float = 0.0; composite: float = 0.0
    atr: float = 0.0; atr_pct: float = 0.0; regime_ok: bool = False
    regime_penalty: float = 1.0; htf_veto: bool = False
    overextended: bool = False; vwap_price: float = 0.0
    deviation_atr: float = 0.0; reversion_side: str = ""
    n_confirming: int = 0; threshold_used: float = 0.0
    market_regime: str = "RANGING"  # MarketRegime.value for display
    adx: float = 0.0               # raw ADX value for display
    trend_score: float = 0.0       # trend-following composite score
    # v4.8: ICT/SMC structural confluence
    ict_ob: float = 0.0            # OB score 0-1
    ict_fvg: float = 0.0           # FVG score 0-1
    ict_sweep: float = 0.0         # Sweep score 0-1
    ict_session: float = 0.0       # Session/KZ score 0-1
    ict_total: float = 0.0         # Weighted ICT total 0-1
    ict_details: str = ""          # Human-readable ICT detail string

    def __str__(self):
        ict_str = f" ICT={self.ict_total:.2f}" if self.ict_total > 0.01 else ""
        return (f"VWAP={self.vwap_dev:+.3f} CVD={self.cvd_div:+.3f} "
                f"OB={self.orderbook:+.3f} TF={self.tick_flow:+.3f} "
                f"VEX={self.vol_exhaust:+.3f} -> Σ={self.composite:+.4f}{ict_str} "
                f"dev={self.deviation_atr:+.1f}ATR confirm={self.n_confirming}/5")

# ═══════════════════════════════════════════════════════════════
# POSITION STATE
# ═══════════════════════════════════════════════════════════════
class PositionPhase(Enum):
    FLAT = auto(); ENTERING = auto(); ACTIVE = auto(); EXITING = auto()

@dataclass
class PositionState:
    phase: PositionPhase = PositionPhase.FLAT
    side: str = ""; quantity: float = 0.0; entry_price: float = 0.0
    sl_price: float = 0.0; tp_price: float = 0.0
    sl_order_id: Optional[str] = None; tp_order_id: Optional[str] = None
    entry_order_id: Optional[str] = None; entry_time: float = 0.0
    initial_risk: float = 0.0; initial_sl_dist: float = 0.0
    trail_active: bool = False; last_trail_time: float = 0.0
    entry_signal: Optional[SignalBreakdown] = None
    peak_profit: float = 0.0; entry_atr: float = 0.0; entry_vol: float = 0.0
    peak_price_abs: float = 0.0  # actual peak price hit (highest for long, lowest for short)
    trade_mode: str = "reversion"  # "reversion" | "trend" | "momentum"
    entry_fill_type: str = "taker"  # v4.3: "maker" | "taker" — for correct PnL fee calc
    trail_override: Optional[bool] = None  # v4.3: None=use config, True=force on, False=force off
    hold_extensions: int = 0  # v4.6: how many times max-hold has been extended

    def is_active(self): return self.phase == PositionPhase.ACTIVE
    def is_flat(self): return self.phase == PositionPhase.FLAT
    def to_dict(self):
        return {"side": self.side, "quantity": self.quantity,
                "entry_price": self.entry_price,
                "sl_price": self.sl_price, "tp_price": self.tp_price}

# ═══════════════════════════════════════════════════════════════
# DAILY RISK GATE with consecutive loss lockout
# ═══════════════════════════════════════════════════════════════
class DailyRiskGate:
    def __init__(self):
        self._today = date.today(); self._daily_trades = 0; self._consec_losses = 0
        self._daily_pnl = 0.0; self._daily_open_bal = 0.0
        self._loss_lockout_until = 0.0; self._lock = threading.Lock()

    def _reset_if_new_day(self):
        today = date.today()
        if today != self._today:
            self._today = today; self._daily_trades = 0; self._daily_pnl = 0.0
            self._daily_open_bal = 0.0; self._consec_losses = 0; self._loss_lockout_until = 0.0

    def set_opening_balance(self, balance):
        with self._lock:
            self._reset_if_new_day()
            if self._daily_open_bal < 1e-10 and balance > 0: self._daily_open_bal = balance

    def can_trade(self, current_balance) -> Tuple[bool, str]:
        with self._lock:
            self._reset_if_new_day(); now = time.time()
            if now < self._loss_lockout_until:
                return False, f"Loss lockout: {int(self._loss_lockout_until - now)}s remaining"
            if self._daily_trades >= QCfg.MAX_DAILY_TRADES():
                return False, f"Daily cap: {self._daily_trades}/{QCfg.MAX_DAILY_TRADES()}"
            if self._consec_losses >= QCfg.MAX_CONSEC_LOSSES():
                self._loss_lockout_until = now + QCfg.LOSS_LOCKOUT_SEC()
                return False, f"Consec loss cap → {QCfg.LOSS_LOCKOUT_SEC()}s lockout"
            if self._daily_open_bal > 1e-10:
                lp = -self._daily_pnl / self._daily_open_bal * 100.0
                if lp >= QCfg.MAX_DAILY_LOSS_PCT():
                    return False, f"Daily loss cap: {lp:.1f}%"
            return True, ""

    def record_trade_start(self):
        with self._lock: self._reset_if_new_day(); self._daily_trades += 1

    def record_trade_result(self, pnl):
        with self._lock:
            self._daily_pnl += pnl
            if pnl < 0: self._consec_losses += 1
            else: self._consec_losses = 0

    @property
    def daily_trades(self):
        with self._lock: return self._daily_trades
    @property
    def consec_losses(self):
        with self._lock: return self._consec_losses

# ═══════════════════════════════════════════════════════════════
# MAIN STRATEGY CLASS
# ═══════════════════════════════════════════════════════════════
class QuantStrategy:
    def __init__(self, order_manager=None):
        self._om = order_manager; self._lock = threading.RLock()
        self._vwap = VWAPEngine(); self._cvd = CVDEngine()
        self._ob_eng = OrderbookEngine(); self._tick_eng = TickFlowEngine()
        self._vol_exh = VolumeExhaustionEngine()
        # ── Execution cost engine (PATCH 2) ──────────────────────────────────────
        self._fee_engine = ExecutionCostEngine() if ExecutionCostEngine is not None else None
        self._prev_price_for_urgency: float = 0.0
        self._atr_1m = ATREngine(); self._atr_5m = ATREngine()
        self._htf = HTFTrendFilter()
        self._adx = ADXEngine()
        self._regime = RegimeClassifier()
        self._breakout = BreakoutDetector()  # v4.6: fast breakout detection
        # v4.8: ICT/SMC structural confluence engine
        self._ict = ICTEngine() if _ICT_AVAILABLE else None
        self._confirm_trend_long = 0; self._confirm_trend_short = 0
        self._pos = PositionState(); self._last_sig = SignalBreakdown()
        self._risk_gate = DailyRiskGate()
        self._confirm_long = 0; self._confirm_short = 0
        self._last_eval_time = 0.0; self._last_exit_time = 0.0
        self._last_momentum_attempt = 0.0   # cooldown after any retest attempt (pass or fail)
        self._last_tp_gate_rejection = 0.0  # tracks last TP gate rejection time
        self._tp_gate_rejection_mode = ""   # "reversion" | "momentum" | "trend" for per-mode logging
        self._last_pos_sync = 0.0; self._last_exit_sync = 0.0
        self._last_exit_side = ""; self._last_think_log = 0.0; self._think_interval = 30.0
        self._last_fed_trade_ts = 0.0
        self._last_reconcile_time = 0.0; self._RECONCILE_SEC = 30.0
        self._reconcile_pending = False; self._reconcile_data = None
        self._total_trades = 0; self._winning_trades = 0; self._total_pnl = 0.0
        self._trade_history: List[Dict] = []   # persistent per-session trade log
        self.current_sl_price = 0.0; self.current_tp_price = 0.0
        # Track last known price for PnL fallback
        self._last_known_price = 0.0
        self._log_init()

    def _log_init(self):
        logger.info("=" * 72)
        logger.info("⚡ QuantStrategy v4.9 — ORDER FLOW + ICT STRUCTURE")
        logger.info(f"   {QCfg.SYMBOL()} | {QCfg.LEVERAGE()}x | {QCfg.MARGIN_PCT():.0%} margin")
        logger.info(f"   Entry: VWAP deviation > {QCfg.VWAP_ENTRY_ATR_MULT()}×ATR | Confirm: {QCfg.CONFIRM_TICKS()} ticks")
        logger.info(f"   SL: swing + {QCfg.SL_BUFFER_ATR_MULT()}×ATR buffer | TP: ICT structural + {QCfg.TP_VWAP_FRACTION():.0%} VWAP fraction")
        logger.info(f"   Trail: BE@{QCfg.TRAIL_BE_R()}R Lock@{QCfg.TRAIL_LOCK_R()}R Aggr@{QCfg.TRAIL_AGGRESSIVE_R()}R")
        logger.info(f"   Trail P1/P2/P3 min-dist: {QCfg.TRAIL_MIN_DIST_ATR_P1()}/{QCfg.TRAIL_MIN_DIST_ATR_P2()}/{QCfg.TRAIL_MIN_DIST_ATR_P3()}×ATR")
        ict_status = "DISABLED (ict_engine.py not found)"
        if self._ict:
            ict_status = (
                f"ENABLED | ZoneFreeze={QCfg.ICT_ZONE_FREEZE_ENABLED()} "
                f"OBanchor={QCfg.ICT_OB_SL_ANCHOR()} "
                f"LiqCeiling={QCfg.ICT_LIQ_CEILING_ENABLED()}"
            )
        logger.info(f"   ICT: {ict_status}")
        logger.info(f"   Weights: VWAP={QCfg.W_VWAP_DEV()} CVD={QCfg.W_CVD_DIV()} OB={QCfg.W_OB()} "
                    f"TF={QCfg.W_TICK_FLOW()} VEX={QCfg.W_VOL_EXHAUSTION()}")
        logger.info("=" * 72)

    def get_position(self) -> Optional[Dict]:
        with self._lock: return None if self._pos.is_flat() else self._pos.to_dict()

    def on_stream_restart(self):
        """v4.3 Bug 2 fix: Called by data_manager after restart_streams().
        Resets all engine timestamps so they reprocess warmup data."""
        with self._lock:
            self._cvd.reset_state()
            self._atr_1m.reset_state()
            self._atr_5m.reset_state()
            self._adx.reset_state()
            self._breakout.reset_state()  # v4.6
            if self._ict: self._ict.reset_state()  # v4.8
            logger.info("♻️ Strategy engines reset after stream restart")

    def set_trail_override(self, enabled: Optional[bool]):
        """v4.3: Telegram command to override trailing SL on/off, even mid-position.
        None = use config default, True = force on, False = force off."""
        with self._lock:
            self._pos.trail_override = enabled
            if enabled is None:
                logger.info("Trail override cleared → using config default")
            else:
                logger.info(f"Trail override set → {'ENABLED' if enabled else 'DISABLED'}")

    def get_trail_enabled(self) -> bool:
        """Check if trailing is enabled considering override."""
        override = self._pos.trail_override
        if override is not None:
            return override
        return QCfg.TRAIL_ENABLED()

    def _spread_atr_gate(self, data_manager) -> tuple:
        """v4.3 Solution 5: Reject entries when spread cost is too large relative to ATR.
        During low-liquidity hours, spread widens 3-5x making the cost-to-move ratio untenable."""
        if self._fee_engine is None:
            return True, 0.0
        try:
            spread_bps = self._fee_engine._spread.median_bps()
            atr = self._atr_5m.atr
            price = data_manager.get_last_price()
            if atr < 1e-10 or price < 1.0:
                return True, 0.0
            spread_price = spread_bps / 10_000.0 * price
            ratio = spread_price / atr
            max_ratio = float(getattr(config, "QUANT_MAX_SPREAD_ATR_RATIO", 0.08))
            if ratio > max_ratio:
                logger.info(
                    f"⛔ Spread/ATR gate: {ratio:.3f} > {max_ratio} "
                    f"(spread={spread_bps:.1f}bps, ATR=${atr:.1f}) — too expensive")
                return False, ratio
            return True, ratio
        except Exception:
            return True, 0.0

    def on_tick(self, data_manager, order_manager, risk_manager, timestamp_ms: int) -> None:
        with self._lock:
            now = timestamp_ms / 1000.0; self._om = order_manager
            if now - self._last_eval_time < QCfg.TICK_EVAL_SEC(): return
            self._last_eval_time = now
            phase = self._pos.phase
            self._feed_microstructure(data_manager)
            # ── Feed orderbook to fee engine (PATCH 3) ───────────────────────────
            try:
                ob = data_manager.get_orderbook()
                price = data_manager.get_last_price()
                if self._fee_engine is not None:
                    self._fee_engine.update_orderbook(ob, price)
            except Exception:
                pass
            # Update last known price
            try:
                p = data_manager.get_last_price()
                if p > 1.0: self._last_known_price = p
            except Exception: pass
            # Reconciliation
            if self._reconcile_data is not None:
                data = self._reconcile_data; self._reconcile_data = None
                self._reconcile_apply(order_manager, data); phase = self._pos.phase
            if now - self._last_reconcile_time >= self._RECONCILE_SEC and not self._reconcile_pending:
                self._last_reconcile_time = now; self._reconcile_pending = True
                threading.Thread(target=self._reconcile_query_thread, args=(order_manager,), daemon=True).start()
            if phase == PositionPhase.ACTIVE:
                if now - self._last_pos_sync > QCfg.POS_SYNC_SEC():
                    self._sync_position(order_manager); self._last_pos_sync = now
                    if self._pos.is_flat(): return
            elif phase == PositionPhase.EXITING:
                if now - self._last_exit_sync > QCfg.POS_SYNC_SEC():
                    self._sync_position(order_manager); self._last_exit_sync = now
                return
            if phase == PositionPhase.FLAT:
                if now - self._last_exit_time < float(QCfg.COOLDOWN_SEC()): return
                self._evaluate_entry(data_manager, order_manager, risk_manager, now)
            elif phase == PositionPhase.ACTIVE:
                self._manage_active(data_manager, order_manager, now)

    def _feed_microstructure(self, data_manager):
        try:
            ob = data_manager.get_orderbook(); price = data_manager.get_last_price()
            if ob and price > 1.0: self._ob_eng.update(ob, price)
        except Exception: pass
        try:
            trades = data_manager.get_recent_trades_raw()
            cutoff_ts = self._last_fed_trade_ts; max_ts = cutoff_ts
            for t in trades:
                ts = t.get("timestamp", 0.0)
                if ts > cutoff_ts:
                    self._tick_eng.on_trade(t.get("price",0.0), t.get("quantity",0.0), t.get("side")=="buy", ts)
                    if ts > max_ts: max_ts = ts
            if max_ts > cutoff_ts: self._last_fed_trade_ts = max_ts
            self._tick_eng.compute_signal()
        except Exception: pass

    def _compute_signals(self, data_manager) -> Optional[SignalBreakdown]:
        candles_1m = data_manager.get_candles("1m", limit=300)
        candles_5m = data_manager.get_candles("5m", limit=100)
        if len(candles_1m) < QCfg.MIN_1M_BARS() or len(candles_5m) < QCfg.MIN_5M_BARS(): return None
        atr_1m = self._atr_1m.compute(candles_1m); atr_5m = self._atr_5m.compute(candles_5m)
        if atr_5m < 1e-10: return None
        price = data_manager.get_last_price()
        if price < 1.0: return None
        self._vwap.update(candles_1m, atr_5m); self._cvd.update(candles_1m)  # v4.3: was atr_1m — Bug 5 fix
        try:
            c15 = data_manager.get_candles("15m", limit=100); c4h = data_manager.get_candles("4h", limit=50)
            self._htf.update(c15, c4h, atr_5m)
        except Exception: pass

        # ── Regime classification ─────────────────────────────────────────────
        self._adx.compute(candles_5m)
        regime = self._regime.update(self._adx, self._atr_5m, self._htf)

        # ── Mean-reversion signals ────────────────────────────────────────────
        vs = self._vwap.get_reversion_signal(price, atr_5m)  # v4.3: was atr_1m — Bug 5 fix
        cs = self._cvd.get_divergence_signal(candles_1m)
        obs = self._ob_eng.get_signal(); ts = self._tick_eng.get_signal()
        ve = self._vol_exh.compute(candles_1m)
        comp = vs*QCfg.W_VWAP_DEV() + cs*QCfg.W_CVD_DIV() + obs*QCfg.W_OB() + ts*QCfg.W_TICK_FLOW() + ve*QCfg.W_VOL_EXHAUSTION()
        comp = max(-1.0, min(1.0, comp))
        direction = 1.0 if comp >= 0 else -1.0
        nc = sum(1 for s in [vs,cs,obs,ts,ve] if s*direction > 0.05)

        # ── Trend-following score (used only in TRENDING regime) ──────────────
        # Composite of: HTF alignment + CVD directional bias + OB imbalance direction
        # Positive = long-biased, negative = short-biased
        htf_comp     = self._htf.trend_4h * 0.60 + self._htf.trend_15m * 0.40
        cvd_trend    = self._cvd.get_trend_signal()
        trend_score  = htf_comp * 0.50 + cvd_trend * 0.30 + obs * 0.20
        trend_score  = max(-1.0, min(1.0, trend_score))

        sig = SignalBreakdown(
            vwap_dev=vs, cvd_div=cs, orderbook=obs, tick_flow=ts, vol_exhaust=ve,
            composite=comp, atr=atr_5m, atr_pct=self._atr_5m.get_percentile(),
            regime_ok=self._atr_5m.regime_valid(), regime_penalty=self._atr_5m.regime_penalty(),
            htf_veto=self._htf.vetoes_trade(self._vwap.reversion_side(price)),
            overextended=self._vwap.is_overextended(price, atr_5m, adx=self._adx.adx),
            vwap_price=self._vwap.vwap, deviation_atr=self._vwap.deviation_atr,
            reversion_side=self._vwap.reversion_side(price), n_confirming=nc,
            threshold_used=QCfg.COMPOSITE_ENTRY_MIN(),
            market_regime=regime.value, adx=self._adx.adx, trend_score=trend_score)
        self._last_sig = sig; return sig

    def _log_thinking(self, sig, price, now):
        if now - self._last_think_log < self._think_interval: return
        self._last_think_log = now
        def bar(v, w=12):
            h=w//2; f=min(int(abs(v)*h+0.5),h)
            return (" "*h+"█"*f+"░"*(h-f)) if v>=0 else ("░"*(h-f)+"█"*f+" "*h)
        def fmt(l,v):
            a = "▲" if v>0.05 else ("▼" if v<-0.05 else "─")
            return f"  {l:<6} {bar(v)} {a} {v:+.3f}"
        c=sig.composite; thr=QCfg.COMPOSITE_ENTRY_MIN()
        regime_lbl = sig.market_regime
        gates = [
            f"{'✅' if sig.overextended else '❌'} Overextended ({sig.deviation_atr:+.1f} ATR)",
            f"{'✅' if sig.regime_ok else '❌'} Regime ({sig.atr_pct:.0%})",
            f"{'✅' if not sig.htf_veto else '❌'} HTF (15m={self._htf.trend_15m:+.2f} 4h={self._htf.trend_4h:+.2f})",
            f"{'✅' if sig.n_confirming>=3 else '❌'} Confluence ({sig.n_confirming}/{'6' if self._ict else '5'})",
            f"{'✅' if abs(c)>=thr else '❌'} Composite ({c:+.3f} vs ±{thr:.3f})",
            f"📊 Market: {regime_lbl} | ADX={sig.adx:.1f} | TrendΣ={sig.trend_score:+.3f}"]
        if sig.ict_total > 0.01:
            ict_lbl = f"🏛️ ICT: {sig.ict_total:.2f} (OB={sig.ict_ob:.1f} FVG={sig.ict_fvg:.1f} Sweep={sig.ict_sweep:.1f} KZ={sig.ict_session:.1f})"
            if sig.ict_details:
                ict_lbl += f" [{sig.ict_details}]"
            gates.append(ict_lbl)
        ap = sig.overextended and sig.regime_ok and not sig.htf_veto and sig.n_confirming>=3 and abs(c)>=thr
        cd = max(0.0, QCfg.COOLDOWN_SEC()-(now-self._last_exit_time))
        lines = [f"┌─── 🧠 v4 REVERSION  ${price:,.2f}  VWAP=${sig.vwap_price:,.2f}  ATR={sig.atr:.1f} ────",
                 fmt("VWAP",sig.vwap_dev), fmt("CVD",sig.cvd_div), fmt("OB",sig.orderbook),
                 fmt("TICK",sig.tick_flow), fmt("VEX",sig.vol_exhaust), f"  {'─'*42}",
                 f"  Σ={c:+.4f} | Side: {sig.reversion_side.upper()}", f"  ── GATES ──"]
        for g in gates: lines.append(f"  {g}")
        lines.append(f"  {'🎯 ALL PASS — confirming' if ap else '👀 Watching'}")
        lines.append(f"  Cooldown: {f'{cd:.0f}s' if cd>0 else 'ready'}")
        lines.append(f"└{'─'*66}")
        logger.info("\n"+"\n".join(lines))

    def _evaluate_entry(self, data_manager, order_manager, risk_manager, now):
        # v4.3 Solution 5: Spread cost gate
        spread_ok, spread_ratio = self._spread_atr_gate(data_manager)
        if not spread_ok:
            self._confirm_long = self._confirm_short = self._confirm_trend_long = self._confirm_trend_short = 0
            return
        sig = self._compute_signals(data_manager)
        if sig is None: self._confirm_long = self._confirm_short = self._confirm_trend_long = self._confirm_trend_short = 0; return
        price = data_manager.get_last_price(); self._log_thinking(sig, price, now)

        # ── v4.8: ICT/SMC structural confluence ──────────────────────────
        # Update ICT structures (OB, FVG, liquidity, session) every 5s
        # Then score ICT confluence for the current reversion side
        # ICT score is used to: (a) boost composite, (b) count as confirming signal
        if self._ict is not None:
            try:
                candles_5m = data_manager.get_candles("5m", limit=100)
                candles_15m = data_manager.get_candles("15m", limit=50)
                now_ms = int(now * 1000) if now < 1e12 else int(now)
                self._ict.update(candles_5m, candles_15m, price, now_ms)
                ict_side = sig.reversion_side if sig.reversion_side else ("long" if sig.composite > 0 else "short")
                ict_conf = self._ict.get_confluence(ict_side, price, now_ms)
                sig.ict_ob = ict_conf.ob_score
                sig.ict_fvg = ict_conf.fvg_score
                sig.ict_sweep = ict_conf.sweep_score
                sig.ict_session = ict_conf.session_score
                sig.ict_total = ict_conf.total
                sig.ict_details = ict_conf.details
                # Boost composite: ICT structural alignment strengthens the signal
                # max boost = 0.15 (ICT total=1.0 × 0.15 = strongest possible)
                ict_boost = ict_conf.total * 0.15
                sig.composite = max(-1.0, min(1.0, sig.composite + (ict_boost if sig.composite > 0 else -ict_boost)))
                # ICT counts as a confirming signal if total > 0.3
                if ict_conf.total >= 0.30:
                    sig.n_confirming = min(sig.n_confirming + 1, 6)
            except Exception as e:
                logger.debug(f"ICT update error: {e}")

        # ── v4.6: Fast breakout detection (runs BEFORE regime routing) ─────
        # This is the single most important defense against bleeding in trends.
        # ADX takes 70+ min to reach 25 during a breakout. By then, 3 reversion
        # trades have been stopped out. The breakout detector fires in <5 candles.
        try:
            candles_5m = data_manager.get_candles("5m", limit=20)
        except Exception:
            candles_5m = []
        self._breakout.update(candles_5m, self._atr_5m, price, now,
                             vwap_price=self._vwap.vwap)

        # ── Route by regime + breakout ─────────────────────────────────────
        # v4.7 FIX: Old routing had breakout.is_active as exclusive elif
        # which blocked ALL other entries (including with-direction reversion)
        # when retest wasn't ready. Bot sat idle for 45 min in TRENDING_UP.
        #
        # New priority:
        #   1. Breakout retest ready → momentum entry (highest priority)
        #   2. TRENDING regime → trend pullback entry
        #   3. Normal → reversion entry (blocks_reversion handles counter-dir veto)
        if self._breakout.is_active and self._breakout.retest_ready:
            self._confirm_long = self._confirm_short = 0
            self._evaluate_momentum_entry(data_manager, order_manager, risk_manager, sig, price, now)
        elif self._regime.is_trending():
            self._confirm_long = self._confirm_short = 0
            self._evaluate_trend_entry(data_manager, order_manager, risk_manager, sig, price, now)
        else:
            self._confirm_trend_long = self._confirm_trend_short = 0
            self._evaluate_reversion_entry(data_manager, order_manager, risk_manager, sig, price, now)

    def _evaluate_reversion_entry(self, data_manager, order_manager, risk_manager, sig, price, now):
        """
        Mean-reversion entry — active in RANGING and TRANSITIONING regimes.
        Blocked in TRENDING regime for counter-trend direction.
        In TRANSITIONING: all existing gates must pass + regime allows this side.
        v4.6: Also blocked when BreakoutDetector detects directional momentum.
        """
        c = sig.composite; thr = QCfg.COMPOSITE_ENTRY_MIN(); side = sig.reversion_side

        # Hard veto: if market is trending against this reversion trade, skip
        if not self._regime.allows_reversion(side):
            self._confirm_long = self._confirm_short = 0; return

        # v4.6: Breakout veto — NEVER fade a detected breakout
        if self._breakout.blocks_reversion(side):
            self._confirm_long = self._confirm_short = 0
            # v4.8: Throttle this log — was firing every 1s tick (600+ lines per breakout)
            if not hasattr(self, '_last_bo_block_log') or now - self._last_bo_block_log >= 60.0:
                self._last_bo_block_log = now
                logger.info(f"🚫 Breakout blocks {side.upper()} reversion (breakout {self._breakout.direction})")
            return

        if not (sig.overextended and sig.regime_ok and not sig.htf_veto and sig.n_confirming >= 3):
            self._confirm_long = self._confirm_short = 0; return
        if self._last_exit_side and self._last_exit_side != side:
            if now - self._last_exit_time < QCfg.COOLDOWN_SEC() * 1.5: return
        if side == "long" and c >= thr: self._confirm_long += 1; self._confirm_short = 0
        elif side == "short" and c <= -thr: self._confirm_short += 1; self._confirm_long = 0
        else: self._confirm_long = self._confirm_short = 0; return
        cn = QCfg.CONFIRM_TICKS()
        if self._confirm_long >= cn:
            self._confirm_long = self._confirm_short = 0
            self._enter_trade(data_manager, order_manager, risk_manager, "long", sig, mode="reversion")
        elif self._confirm_short >= cn:
            self._confirm_long = self._confirm_short = 0
            self._enter_trade(data_manager, order_manager, risk_manager, "short", sig, mode="reversion")

    def _evaluate_trend_entry(self, data_manager, order_manager, risk_manager, sig, price, now):
        """
        Trend-following pullback entry — active only in TRENDING_UP / TRENDING_DOWN.

        Entry logic (institutional pullback-to-EMA):
          1. Market must be in an established trend (RegimeClassifier confirms)
          2. Price has pulled back into the EMA(8) zone (not chasing the breakout)
          3. Pullback depth: TREND_PULLBACK_ATR_MIN ≤ dist(price, ema8) ≤ TREND_PULLBACK_ATR_MAX
             — too shallow = not a real pullback; too deep = trend may be reversing
          4. CVD trend bias not strongly opposed (prevents buying into distribution)
          5. Tick flow in trend direction (live order flow confirming the move)
          6. Composite trend score ≥ TREND_COMPOSITE_MIN
          7. TREND_CONFIRM_TICKS consecutive confirming evaluations (slightly more
             patient than reversion to avoid catching the start of a pullback)

        TP: ATR-channel extension (not VWAP — VWAP is behind price in a trend).
        SL: Behind the pullback swing low/high using existing multi-TF compute_sl.
        Trail: Tight chandelier (TREND_CHANDELIER_N) — trends reverse sharply.
        """
        trend_side = self._regime.trend_side()
        if trend_side is None: return

        # CVD directional filter: don't buy if order flow is strongly opposed
        cvd_bias = self._cvd.get_trend_signal()
        if trend_side == "long"  and cvd_bias < QCfg.TREND_CVD_MIN(): return
        if trend_side == "short" and cvd_bias > -QCfg.TREND_CVD_MIN(): return

        # Tick flow must broadly agree with trend direction
        tf = self._tick_eng.get_signal()
        if trend_side == "long"  and tf < -0.30: return
        if trend_side == "short" and tf >  0.30: return

        # Composite trend score gate
        if abs(sig.trend_score) < QCfg.TREND_COMPOSITE_MIN(): return
        if trend_side == "long"  and sig.trend_score <= 0: return
        if trend_side == "short" and sig.trend_score >= 0: return

        # Pullback-to-EMA depth check
        try: candles_5m = data_manager.get_candles("5m", limit=30)
        except Exception: return
        if len(candles_5m) < 10: return
        closes = [float(c['c']) for c in candles_5m]
        period = QCfg.EMA_FAST()
        k = 2.0 / (period + 1)
        ema = sum(closes[:period]) / period
        for v in closes[period:]: ema = v * k + ema * (1.0 - k)
        atr = self._atr_5m.atr
        ema_dist = (ema - price) if trend_side == "long" else (price - ema)
        pb_min = QCfg.TREND_PULLBACK_ATR_MIN() * atr
        pb_max = QCfg.TREND_PULLBACK_ATR_MAX() * atr
        if not (pb_min <= ema_dist <= pb_max): return

        # Confirmation counter
        if trend_side == "long":
            self._confirm_trend_long += 1; self._confirm_trend_short = 0
        else:
            self._confirm_trend_short += 1; self._confirm_trend_long = 0

        cn = QCfg.TREND_CONFIRM_TICKS()
        if trend_side == "long" and self._confirm_trend_long >= cn:
            self._confirm_trend_long = self._confirm_trend_short = 0
            self._enter_trade(data_manager, order_manager, risk_manager, "long", sig, mode="trend")
        elif trend_side == "short" and self._confirm_trend_short >= cn:
            self._confirm_trend_long = self._confirm_trend_short = 0
            self._enter_trade(data_manager, order_manager, risk_manager, "short", sig, mode="trend")

    def _evaluate_momentum_entry(self, data_manager, order_manager, risk_manager, sig, price, now):
        """
        v4.7: Break-and-retest entry — institutional momentum entry.

        Instead of chasing the breakout (gets caught at the top) or waiting
        for a 5m pullback to EMA (never comes), this uses the RETEST pattern:

        1. Breakout detector fires → records extreme + midpoint
        2. WAIT for micro-pullback (0.3-1.0 × ATR retrace from extreme)
        3. WAIT for bounce from pullback (price moves 0.2 × ATR off the low)
        4. ENTER on the bounce with tight SL below the pullback low

        Why this works:
          - You buy the pullback, not the top
          - SL is tight (below retest low) → small risk
          - Confirmation is built in (bounce = buyers still there)
          - If breakout was fake, the pullback low breaks → no entry

        Timeout: If no retest within 15 min, breakout was too impulsive.
        The opportunity is gone — move on.
        """
        bo_dir = self._breakout.direction
        if not bo_dir:
            return

        side = "long" if bo_dir == "up" else "short"

        # ── Phase 1: Retest not ready yet — just wait ─────────────────────
        if not self._breakout.retest_ready:
            # Reset confirmation counters while waiting
            self._confirm_trend_long = self._confirm_trend_short = 0
            return

        # ── Retest attempt cooldown ───────────────────────────────────────
        # Without this, after each _enter_trade call (whether it places an order
        # OR is rejected by TP gate), the confirm counters reset to 0 and the
        # momentum path re-fires every 2 seconds indefinitely.
        # Allow a new attempt only after QUANT_RETEST_RETRY_SEC seconds.
        retry_sec = float(_cfg("QUANT_RETEST_RETRY_SEC", 30.0))
        if now - self._last_momentum_attempt < retry_sec:
            return

        # ── Phase 2: Retest ready — apply entry gates ─────────────────────

        # Gate 1: Tick flow must agree with breakout direction
        tf = self._tick_eng.get_signal()
        if side == "long" and tf < -0.15:
            return
        if side == "short" and tf > 0.15:
            return

        # Gate 2: Don't chase exhaustion (>4×ATR from VWAP)
        atr = self._atr_5m.atr
        if atr > 1e-10 and self._vwap.vwap > 0:
            dev_atr = abs(price - self._vwap.vwap) / atr
            if dev_atr > 4.0:
                return

        # Gate 3: Price must still be above breakout midpoint (long) /
        #         below breakout midpoint (short) — breakout structure intact
        if side == "long" and price < self._breakout._bo_midpoint:
            return
        if side == "short" and price > self._breakout._bo_midpoint:
            return

        # ── Phase 3: Confirmation counter ─────────────────────────────────
        if side == "long":
            self._confirm_trend_long += 1
            self._confirm_trend_short = 0
        else:
            self._confirm_trend_short += 1
            self._confirm_trend_long = 0

        cn = QCfg.CONFIRM_TICKS()
        if side == "long" and self._confirm_trend_long >= cn:
            self._confirm_trend_long = self._confirm_trend_short = 0
            self._last_momentum_attempt = now   # prevent re-fire for retry_sec
            retest_sl = self._breakout.retest_sl
            logger.info(
                f"🚀 RETEST ENTRY — {side.upper()} (breakout {bo_dir}) | "
                f"retest_low=${self._breakout._retest_low:,.2f} | "
                f"SL=${retest_sl:,.2f}")
            self._enter_trade(data_manager, order_manager, risk_manager, side, sig, mode="momentum")
        elif side == "short" and self._confirm_trend_short >= cn:
            self._confirm_trend_long = self._confirm_trend_short = 0
            self._last_momentum_attempt = now   # prevent re-fire for retry_sec
            retest_sl = self._breakout.retest_sl
            logger.info(
                f"🚀 RETEST ENTRY — {side.upper()} (breakout {bo_dir}) | "
                f"retest_high=${self._breakout._retest_high:,.2f} | "
                f"SL=${retest_sl:,.2f}")
            self._enter_trade(data_manager, order_manager, risk_manager, side, sig, mode="momentum")

    def _compute_sl_tp(self, data_manager, price, side, atr, mode="reversion",
                       signal_confidence=0.5, use_maker_entry=False):
        """
        Institutional SL/TP — mode-aware: reversion uses VWAP targets, trend uses ATR channel.

        PATCH 4: Adds a fee-normalized TP floor gate before returning.
          - Asks _fee_engine for the minimum gross TP distance that clears ALL execution
            costs (spread + slippage + commission) with a regime-adaptive buffer.
          - If computed TP is closer than that minimum, the setup is rejected
            (returns None, None) before any order is placed.
          - When _fee_engine is unavailable (fee_engine.py not yet deployed), the gate
            is silently skipped and original behaviour is preserved.

        Args:
            signal_confidence: composite score mapped to [0, 1], passed from _enter_trade
            use_maker_entry:   whether we intend to use a limit order for entry
        """
        try: candles_5m = data_manager.get_candles("5m", limit=QCfg.SL_SWING_LOOKBACK()+5)
        except Exception: candles_5m = []
        try: candles_1m = data_manager.get_candles("1m", limit=150)
        except Exception: candles_1m = []
        try: candles_15m = data_manager.get_candles("15m", limit=30)
        except Exception: candles_15m = []
        try: orderbook = data_manager.get_orderbook()
        except Exception: orderbook = {"bids": [], "asks": []}
        vwap = self._vwap.vwap; vwap_std = self._vwap.vwap_std
        atr_pctile = self._atr_5m.get_percentile()

        sl_price = InstitutionalLevels.compute_sl(
            price, side, atr, candles_5m, candles_1m, orderbook, vwap, vwap_std,
            atr_pctile=atr_pctile, candles_15m=candles_15m)
        sl_price = _round_to_tick(sl_price)

        # ── v4.8: ICT OB-aware SL enhancement ─────────────────────────────
        # If an Order Block exists between current SL and price, use it as SL
        # instead. OBs are institutional footprints — price is more likely to
        # bounce off an OB than a random swing low. Only UPGRADE (tighter) SL.
        if self._ict is not None:
            try:
                now_ms = int(time.time() * 1000)
                ob_sl = self._ict.get_ob_sl_level(side, price, atr, now_ms)
                if ob_sl is not None:
                    if side == "long" and ob_sl > sl_price and ob_sl < price:
                        logger.info(f"🏛️ ICT OB SL upgrade: ${sl_price:,.2f} → ${ob_sl:,.2f} (OB support)")
                        sl_price = _round_to_tick(ob_sl)
                    elif side == "short" and ob_sl < sl_price and ob_sl > price:
                        logger.info(f"🏛️ ICT OB SL upgrade: ${sl_price:,.2f} → ${ob_sl:,.2f} (OB resistance)")
                        sl_price = _round_to_tick(ob_sl)
            except Exception as e:
                logger.debug(f"ICT OB SL error: {e}")

        # ── v4.7: Mode-aware SL sizing ────────────────────────────────────
        #
        # REVERSION: Keep structural swing SL. These trades enter near range
        # boundaries where the swing defines the thesis. A dip that doesn't
        # break the swing is expected — ATR-capping killed winners before.
        #
        # TREND/MOMENTUM: Use ATR-based SL, capped at 2×ATR from entry.
        # These trades enter mid-move. The structural swing is hours old
        # and irrelevant. With 40x leverage, a 6×ATR SL = 80% of margin
        # at risk, and the TP (3× SL distance) becomes unreachable.
        #
        # The math: margin=$47, qty=0.025, 2×ATR=$470 → risk=$11.75 (25% of margin)
        # That's a proper risk-managed scalp, not a liquidation waiting to happen.
        #
        if mode in ("trend", "momentum"):
            max_sl_atr = float(_cfg("QUANT_TREND_SL_ATR_MULT", 2.0))
            max_sl_dist = max_sl_atr * atr
            current_dist = abs(price - sl_price)
            if current_dist > max_sl_dist:
                if side == "long":
                    sl_price = _round_to_tick(price - max_sl_dist)
                else:
                    sl_price = _round_to_tick(price + max_sl_dist)
                logger.info(
                    f"📏 Trend SL capped: {current_dist:.0f} → {max_sl_dist:.0f} "
                    f"({max_sl_atr:.1f}×ATR) | SL=${sl_price:,.2f}")

            # Also enforce liquidation safety: SL must be at least 0.5×ATR
            # ABOVE liquidation price (which is ~entry ± entry/leverage)
            liq_buffer = 0.5 * atr
            if side == "long":
                liq_price = price - (price / QCfg.LEVERAGE()) * 0.95  # ~95% of margin
                if sl_price < liq_price + liq_buffer:
                    sl_price = _round_to_tick(liq_price + liq_buffer)
                    logger.info(f"⚠️ SL raised above liquidation: SL=${sl_price:,.2f} liq≈${liq_price:,.2f}")
            else:
                liq_price = price + (price / QCfg.LEVERAGE()) * 0.95
                if sl_price > liq_price - liq_buffer:
                    sl_price = _round_to_tick(liq_price - liq_buffer)
                    logger.info(f"⚠️ SL lowered below liquidation: SL=${sl_price:,.2f} liq≈${liq_price:,.2f}")

        if mode in ("trend", "momentum"):
            tp_price = InstitutionalLevels.compute_tp_trend(
                price, side, atr, sl_price, candles_5m, orderbook,
                swing_lookback=QCfg.SL_SWING_LOOKBACK())
        else:
            tp_price = InstitutionalLevels.compute_tp(
                price, side, atr, sl_price, candles_1m, orderbook, vwap, vwap_std,
                candles_5m=candles_5m,
                ict_engine=self._ict,
                now_ms=int(time.time() * 1000))
        tp_price = _round_to_tick(tp_price)

        # ── Basic direction sanity (unchanged) ───────────────────────────────────
        if side == "long"  and (sl_price >= price or tp_price <= price): return None, None
        if side == "short" and (sl_price <= price or tp_price >= price): return None, None

        # ── Fee-normalized TP floor gate (PATCH 4) ────────────────────────────────
        # Only active when ExecutionCostEngine is available AND warmed up.
        # During warmup the engine returns hardcoded defaults which can be
        # incorrectly tight or loose. Skip the gate for the first ~10s of operation.
        if self._fee_engine is not None and self._fee_engine.is_warmed_up():
            tp_distance = abs(tp_price - price)
            sl_distance = abs(sl_price - price)
            try:
                min_tp = self._fee_engine.min_required_tp_move(
                    price             = price,
                    atr               = atr,
                    atr_percentile    = atr_pctile,
                    use_maker_entry   = use_maker_entry,
                    signal_confidence = signal_confidence,
                )
                if tp_distance < min_tp:
                    snap = self._fee_engine.diagnostic_snapshot()
                    actual_rr = tp_distance / sl_distance if sl_distance > 1e-10 else 0.0
                    min_rr    = min_tp / sl_distance if sl_distance > 1e-10 else 0.0
                    logger.info(
                        f"⛔ TP gate: tp=${tp_price:,.1f}({tp_distance:.0f}pts/{tp_distance/atr:.1f}ATR) "
                        f"R:R={actual_rr:.2f} < required={min_rr:.2f} "
                        f"[fee_floor=${min_tp:.0f} pctile={atr_pctile:.2f} "
                        f"spread={snap['spread_median_bps']:.1f}bps "
                        f"rt={snap['rt_cost_taker_bps']:.1f}bps]"
                    )
                    return None, None
                logger.debug(
                    f"✅ TP gate passed: tp_dist=${tp_distance:.2f} ≥ min=${min_tp:.2f} "
                    f"R:R={tp_distance/sl_distance:.2f} (pctile={atr_pctile:.2f})"
                )
            except Exception as e:
                logger.debug(f"Fee gate error (non-fatal): {e}")
        elif self._fee_engine is not None and not self._fee_engine.is_warmed_up():
            logger.debug("Fee gate skipped — engine not yet warmed up (< 5 spread samples)")

        return sl_price, tp_price

    def _enter_trade(self, data_manager, order_manager, risk_manager, side, sig, mode="reversion"):
        """
        Position entry — PATCH 5.

        Key changes vs original:
          a) signal_confidence derived from composite score before any order
          b) compute_signal_urgency() called to gauge how quickly price is moving
          c) fee_engine.decide_entry_type() routes to maker (limit) or taker (market)
          d) order_manager.place_limit_entry() used for maker path
          e) _compute_sl_tp() receives use_maker_entry + signal_confidence for TP gate
          f) fee_engine.record_fill() called for slippage tracking
          g) execution cost snapshot logged on every entry
          
        When _fee_engine is None (fee_engine.py not deployed), falls back to the
        original plain-market-order path so the bot continues to operate normally.
        """
        price = data_manager.get_last_price()
        if price < 1.0: return
        atr = self._atr_5m.atr
        if atr < 1e-10: return

        # ── Risk gate ─────────────────────────────────────────────────────────────
        bal_info = risk_manager.get_available_balance()
        if bal_info is None: return
        total_bal = float(bal_info.get("total", bal_info.get("available", 0.0)))
        self._risk_gate.set_opening_balance(total_bal)
        allowed, reason = self._risk_gate.can_trade(total_bal)
        if not allowed:
            logger.info(f"Entry blocked: {reason}")
            return

        qty = self._compute_quantity(risk_manager, price)
        if qty is None or qty < QCfg.MIN_QTY(): return

        # ── Map composite score → signal_confidence [0, 1] (PATCH 5a) ───────────
        raw_composite     = abs(sig.composite) if sig.composite is not None else 0.0
        signal_confidence = min(1.0, raw_composite / 0.6)   # 0.6 composite = full confidence

        # ── Maker vs taker decision (PATCH 5b/5c) ────────────────────────────────
        use_maker  = False
        limit_px   = price
        mt_reason  = "fee_engine unavailable — market"

        if self._fee_engine is not None:
            try:
                orderbook = data_manager.get_orderbook()
            except Exception:
                orderbook = {"bids": [], "asks": []}

            urgency = order_manager.compute_signal_urgency(
                price_now           = price,
                price_prev          = self._prev_price_for_urgency,
                atr                 = atr,
                side                = side,
                vwap_dev_atr        = self._vwap.deviation_atr,
                entry_threshold_atr = QCfg.VWAP_ENTRY_ATR_MULT(),
            )
            self._prev_price_for_urgency = price

            try:
                # Only use maker/taker decision when engine is warmed up.
                # During warmup, fee_engine uses default spread (2.0bps) which
                # may over-estimate savings and post a limit at a bad price.
                if self._fee_engine.is_warmed_up():
                    use_maker, limit_px, mt_reason = self._fee_engine.decide_entry_type(
                        side           = side,
                        quantity       = qty,
                        price          = price,
                        orderbook      = orderbook,
                        signal_urgency = urgency,
                    )
                else:
                    use_maker  = False
                    mt_reason  = "fee_engine_warming_up→taker"
                    logger.debug("FeeEngine not warmed up — defaulting to taker entry")
            except Exception as e:
                logger.debug(f"decide_entry_type error (non-fatal): {e}")
                use_maker = False

        logger.info(f"Entry routing: {'MAKER' if use_maker else 'TAKER'} | {mt_reason}")

        # ── TP/SL viability check BEFORE placing the entry (PATCH 5d) ────────────
        # Check fees against the planned TP before taking the position.
        # If the setup doesn't clear the fee floor, skip entirely.
        sl_price, tp_price = self._compute_sl_tp(
            data_manager, price, side, atr, mode=mode,
            signal_confidence=signal_confidence,
            use_maker_entry=use_maker,
        )
        if sl_price is None: return   # TP floor rejected the setup

        sd = abs(price - sl_price)
        td = abs(price - tp_price)
        if sd < 1e-10: return
        rr = td / sd
        logger.info(
            f"🎯 ENTERING {side.upper()} @ ${price:,.2f} | qty={qty} | "
            f"SL=${sl_price:,.2f} TP=${tp_price:,.2f} R:R=1:{rr:.2f} | "
            f"{'maker' if use_maker else 'taker'} | VWAP=${sig.vwap_price:,.2f} | {sig}"
        )

        # ── Place entry order (PATCH 5e) ──────────────────────────────────────────
        if use_maker:
            limit_timeout = float(getattr(config, 'LIMIT_ORDER_FILL_TIMEOUT_SEC', 25.0))
            entry_data = order_manager.place_limit_entry(
                side=side, quantity=qty,
                limit_price=limit_px,
                timeout_sec=limit_timeout,
                fallback_to_market=True,
            )
        else:
            entry_data = order_manager.place_market_order(side=side, quantity=qty)
            if entry_data:
                entry_data["fill_type"] = "taker"

        if not entry_data:
            logger.error("❌ Entry order failed")
            return

        self._risk_gate.record_trade_start()

        # ── Extract fill price ────────────────────────────────────────────────────
        fill_price = (
            float(entry_data.get("fill_price")          or 0)
            or float(entry_data.get("average_price")    or 0)
            or float(entry_data.get("avg_execution_price") or 0)
            or float(entry_data.get("price")            or 0)
            or price
        )
        actual_fill_type = entry_data.get("fill_type", "taker")

        # v4.6 BUG FIX #8: Use actual filled quantity for partial fills
        # order_manager.place_limit_entry returns adjusted quantity on partial fill
        filled_qty = float(entry_data.get("quantity", 0)) if "quantity" in entry_data else 0
        if filled_qty > 0 and filled_qty != qty:
            logger.info(f"⚠️ Partial fill: {filled_qty:.4f} of {qty:.4f} — using filled qty")
            qty = filled_qty

        # ── Record slippage for fee engine (PATCH 5f) ─────────────────────────────
        if self._fee_engine is not None:
            try:
                self._fee_engine.record_fill(price, fill_price)
            except Exception as e:
                logger.debug(f"record_fill error (non-fatal): {e}")

        # ── Recompute SL/TP from actual fill only on ADVERSE slippage ────────────
        # CRITICAL BUG FIX: The old code used abs(fill_price - price) which fired
        # on FAVORABLE fills too. A SHORT limit at $73,629 filling at $73,683
        # (market moved up, maker got better price) is NOT slippage — it's
        # favorable execution. Recomputing in that case then hit pctile=0.00
        # (ATR percentile drops in the seconds between decision and fill) and
        # the fee floor rejected the now-open position, instantly closing it.
        #
        # Adverse slippage definition:
        #   LONG:  fill_price > price (paid more than the market snapshot)
        #   SHORT: fill_price < price (sold for less than the market snapshot)
        #
        # Favorable execution (market moved in our direction between decision
        # and fill) should NOT trigger recompute — the SL/TP from the original
        # decision are still valid or better.
        is_adverse_slip = (
            (side == "long"  and fill_price > price) or
            (side == "short" and fill_price < price)
        )
        adverse_slip_pct = (abs(fill_price - price) / price) if is_adverse_slip else 0.0

        if is_adverse_slip and adverse_slip_pct > QCfg.SLIPPAGE_TOL():
            logger.info(
                f"⚠️ Adverse slippage {adverse_slip_pct:.4%} > tol {QCfg.SLIPPAGE_TOL():.4%} "
                f"— recomputing SL/TP from fill price ${fill_price:,.2f}")
            new_sl, new_tp = self._compute_sl_tp(
                data_manager, fill_price, side, atr, mode=mode,
                signal_confidence=signal_confidence,
                use_maker_entry=(actual_fill_type == "maker"),
            )
            if new_sl is None:
                # After adverse slippage, trade no longer clears fee floor — abort
                logger.warning(
                    f"❌ Post-slippage TP gate rejected — aborting trade "
                    f"(adverse slip={adverse_slip_pct:.4%})")
                exit_side = "sell" if side == "long" else "buy"
                order_manager.place_market_order(side=exit_side, quantity=qty, reduce_only=True)
                self._last_exit_time = time.time()
                return
            sl_price, tp_price = new_sl, new_tp
        elif not is_adverse_slip and abs(fill_price - price) / price > QCfg.SLIPPAGE_TOL():
            # Favorable fill: market moved our way. Log it but keep original SL/TP.
            fav_pct = abs(fill_price - price) / price
            logger.info(
                f"✅ Favorable fill: ${fill_price:,.2f} vs snapshot ${price:,.2f} "
                f"(+{fav_pct:.4%} in our direction) — keeping original SL/TP")

        # ── Place SL/TP ───────────────────────────────────────────────────────────
        exit_side = "sell" if side == "long" else "buy"
        sweep = order_manager.cancel_symbol_conditionals()
        if sweep:
            # v4.6 BUG FIX #3: Wait for exchange to process cancellations
            # Without this, old SL/TP can fire against the new position instantly
            # (Trade 3 in logs: entry + reconcile FLAT in same second)
            time.sleep(1.5)
            filled = [
                oid for oid, r in sweep.items()
                if r in (CancelResult.ALREADY_FILLED, CancelResult.PARTIAL_FILL)
            ]
            if filled:
                self._last_reconcile_time = 0.0
                return

        sl_data = order_manager.place_stop_loss(side=exit_side, quantity=qty, trigger_price=sl_price)
        if not sl_data:
            order_manager.place_market_order(side=exit_side, quantity=qty, reduce_only=True)
            self._last_exit_time = time.time()
            return

        tp_data = order_manager.place_take_profit(side=exit_side, quantity=qty, trigger_price=tp_price)
        if not tp_data:
            order_manager.cancel_order(sl_data["order_id"])
            order_manager.place_market_order(side=exit_side, quantity=qty, reduce_only=True)
            self._last_exit_time = time.time()
            return

        # ── Log execution cost snapshot (PATCH 5g) ────────────────────────────────
        if self._fee_engine is not None:
            try:
                snap = self._fee_engine.diagnostic_snapshot()
                sdf  = abs(fill_price - sl_price)
                logger.info(
                    f"📊 ExecCost | spread={snap['spread_median_bps']:.1f}bps "
                    f"slip={snap['slippage_ewma_bps']:.1f}bps "
                    f"rt_cost_{'maker' if actual_fill_type == 'maker' else 'taker'}"
                    f"={snap['rt_cost_maker_bps' if actual_fill_type == 'maker' else 'rt_cost_taker_bps']:.1f}bps "
                    f"fill_type={actual_fill_type}"
                )
            except Exception as e:
                logger.debug(f"ExecCost snapshot error (non-fatal): {e}")

        sdf = abs(fill_price - sl_price)
        ir  = sdf * qty

        # ── Build entry volume for trailing vol-decay detection ───────────────────
        try:
            c1m       = data_manager.get_candles("1m", limit=10)
            entry_vol = sum(float(c['v']) for c in c1m[-5:]) / 5.0 if len(c1m) >= 5 else 0.0
        except Exception:
            entry_vol = 0.0

        # ── Update position state ─────────────────────────────────────────────────
        self._pos = PositionState(
            phase           = PositionPhase.ACTIVE,
            side            = side,
            quantity        = qty,
            entry_price     = fill_price,
            sl_price        = sl_price,
            tp_price        = tp_price,
            sl_order_id     = sl_data["order_id"],
            tp_order_id     = tp_data["order_id"],
            entry_order_id  = entry_data.get("order_id"),
            entry_time      = time.time(),
            initial_risk    = ir,
            initial_sl_dist = sdf,
            entry_signal    = sig,
            entry_atr       = self._atr_5m.atr,
            entry_vol       = entry_vol,
            trade_mode      = mode,
            entry_fill_type = actual_fill_type,  # v4.3: for correct PnL fee calc
        )
        # ── Reconcile safety: discard any in-flight reconcile data ────────────────
        self._reconcile_data        = None
        self._last_reconcile_time   = time.time()
        self.current_sl_price       = sl_price
        self.current_tp_price       = tp_price
        # NOTE: _total_trades incremented at CLOSE (not here) so win-rate denominator
        # only counts completed trades, not abandoned/rejected ones.
        self._confirm_long          = self._confirm_short = 0

        # ── Build clean entry notification ───────────────────────────────────────
        # R:R = (TP distance) / (SL distance) — both measured from FILL price
        sl_dist_pts = abs(fill_price - sl_price)
        tp_dist_pts = abs(fill_price - tp_price)
        rr_a        = tp_dist_pts / sl_dist_pts if sl_dist_pts > 1e-10 else 0.0
        dollar_risk = sl_dist_pts * qty          # max loss in USDT (before fees)
        # ICT context line
        ict_line = ""
        if sig.ict_total > 0.01:
            ict_line = f"\nICT:      Σ={sig.ict_total:.2f} [{sig.ict_details}]"
        mode_icon = "🚀" if mode == "momentum" else ("📈📈" if mode == "trend" else ("📈" if side == "long" else "📉"))
        send_telegram_message(
            f"{mode_icon} <b>NEW TRADE — {side.upper()} [{mode.upper()}]</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Entry:    <b>${fill_price:,.2f}</b>\n"
            f"SL:       ${sl_price:,.2f}  (−${sl_dist_pts:.1f} / {sl_dist_pts/max(self._atr_5m.atr,1):.2f}×ATR)\n"
            f"TP:       ${tp_price:,.2f}  (+${tp_dist_pts:.1f} / {tp_dist_pts/max(self._atr_5m.atr,1):.2f}×ATR)\n"
            f"R:R:      1:{rr_a:.2f}  |  Risk: ${dollar_risk:.2f} USDT\n"
            f"Qty:      {qty:.4f} BTC\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Regime:   {sig.market_regime} | ADX={sig.adx:.1f} | Mode: {mode.capitalize()}\n"
            f"VWAP:     ${sig.vwap_price:,.2f} ({sig.deviation_atr:+.1f}×ATR)\n"
            f"Signals:  {sig.n_confirming}/5 agree | Σ={sig.composite:+.3f}{ict_line}"
        )
        logger.info(
            f"✅ ACTIVE {side.upper()} [{mode}] @ ${fill_price:,.2f} | "
            f"SL=${sl_price:,.2f} TP=${tp_price:,.2f} | R:R=1:{rr_a:.2f}"
        )

    def _manage_active(self, data_manager, order_manager, now):
        pos = self._pos; price = data_manager.get_last_price()
        if price < 1.0: return

        # ── Compute signals FIRST (needed for thesis check) ──────────────────
        sig = self._compute_signals(data_manager)
        if sig is not None:
            self._log_thinking(sig, price, now)

            if pos.trade_mode == "trend":
                regime_flipped = not self._regime.is_trending() or (
                    (pos.side == "long"  and self._regime.regime == MarketRegime.TRENDING_DOWN) or
                    (pos.side == "short" and self._regime.regime == MarketRegime.TRENDING_UP))
                if regime_flipped:
                    logger.info(f"🔄 Regime flip → exit {pos.side.upper()} [{pos.trade_mode}]")
                    self._exit_trade(order_manager, price, "regime_flip"); return

            # v4.6: Momentum trades do NOT exit on regime flip.
            # The whole point is they fire BEFORE regime catches up.
            # They exit via: SL, TP, trailing SL, or max-hold only.
            # But if breakout expires AND composite flips against us, exit.
            if pos.trade_mode == "momentum":
                if not self._breakout.is_active:
                    # Breakout expired — check if composite still agrees
                    if pos.side == "long" and sig.composite < -0.25:
                        logger.info(f"🔄 Breakout expired + composite bearish → exit LONG [momentum]")
                        self._exit_trade(order_manager, price, "breakout_expired"); return
                    if pos.side == "short" and sig.composite > 0.25:
                        logger.info(f"🔄 Breakout expired + composite bullish → exit SHORT [momentum]")
                        self._exit_trade(order_manager, price, "breakout_expired"); return

        # ── Thesis-aware max-hold (v4.6) ─────────────────────────────────────
        #
        # v4.4 BUG: blind 40-min timer killed LONG @ $71,452 at $71,349.
        #           Price then rallied to $71,900. TP at $71,806 would have hit.
        #           The trade thesis was CORRECT — timer destroyed the edge.
        #
        # v4.6 FIX: When timer fires, ASK THE MARKET if the trade should continue:
        #   1. In profit  → tighten SL, let SL manage the exit (NO forced dump)
        #   2. Underwater + thesis valid → EXTEND hold (max N extensions)
        #   3. Underwater + thesis broken → EXIT
        #
        # Thesis valid means:
        #   a) Composite signal still agrees with trade direction
        #   b) Price still between SL and TP (not breached)
        #   c) Not deeply underwater (< THESIS_MAX_DRAWDOWN_PCT of SL distance)
        #   d) Reversion: price still on "wrong" side of VWAP (hasn't reverted past it)
        #
        hold_time = now - pos.entry_time
        base_hold = QCfg.MAX_HOLD_SEC()
        extension_sec = QCfg.HOLD_EXTENSION_SEC()
        total_allowed = base_hold + pos.hold_extensions * extension_sec

        if hold_time > total_allowed:
            # v4.6 FIX: Throttle max-hold checks to every 30s
            # (was every 1s tick → 900 log lines in 15 min)
            if not hasattr(self, '_last_maxhold_check'):
                self._last_maxhold_check = 0.0
            if now - self._last_maxhold_check < 30.0:
                pass  # skip — trailing SL handles it below
            else:
                self._last_maxhold_check = now
                profit = (price - pos.entry_price) if pos.side == "long" else (pos.entry_price - price)
                atr = self._atr_5m.atr

                # CASE 1: IN PROFIT → tighten SL, let trailing manage exit
                if QCfg.SMART_MAX_HOLD() and profit > 0 and atr > 1e-10 and pos.sl_order_id is not None:
                    tight_mult = QCfg.MAX_HOLD_PROFIT_SL_ATR()
                    if pos.side == "long":
                        tight_sl = _round_to_tick(price - tight_mult * atr)
                        # v4.6 BUG FIX: SL must be BELOW current price for LONG
                        if tight_sl >= price:
                            tight_sl = _round_to_tick(price - max(atr * 0.2, 1.0))
                    else:
                        tight_sl = _round_to_tick(price + tight_mult * atr)
                        # v4.6 BUG FIX: SL must be ABOVE current price for SHORT
                        if tight_sl <= price:
                            tight_sl = _round_to_tick(price + max(atr * 0.2, 1.0))

                    improves = (pos.side == "long" and tight_sl > pos.sl_price) or \
                               (pos.side == "short" and tight_sl < pos.sl_price)
                    sl_sane = (pos.side == "long" and tight_sl < price) or \
                              (pos.side == "short" and tight_sl > price)

                    if improves and sl_sane:
                        logger.info(
                            f"⏰ Max hold + ${profit:.2f} profit → tightening SL "
                            f"${pos.sl_price:,.2f} → ${tight_sl:,.2f} (price - {tight_mult}×ATR)")
                        es = "sell" if pos.side == "long" else "buy"
                        result = order_manager.replace_stop_loss(
                            existing_sl_order_id=pos.sl_order_id,
                            side=es, quantity=pos.quantity,
                            new_trigger_price=tight_sl)
                        if result is None:
                            logger.warning("🚨 SL fired during tighten")
                            self._record_exchange_exit(None)
                            return
                        if result and isinstance(result, dict) and "error" not in result:
                            self._pos.sl_price = tight_sl
                            self._pos.sl_order_id = result.get("order_id", pos.sl_order_id)
                            self.current_sl_price = tight_sl
                            if not pos.trail_active:
                                self._pos.trail_active = True
                            send_telegram_message(
                                f"⏰ <b>MAX HOLD — TIGHTENED SL</b>\n"
                                f"Profit: ${profit:+.2f} → protecting with tight SL\n"
                                f"SL: ${tight_sl:,.2f} (ATR×{tight_mult} from price)\n"
                                f"<i>Trade rides with tight SL — no forced exit</i>")
                    # Fall through to trailing SL — do NOT return

                else:
                    # CASE 2: UNDERWATER — check thesis
                    max_ext = QCfg.MAX_HOLD_EXTENSIONS()
                    if QCfg.SMART_MAX_HOLD() and pos.hold_extensions < max_ext and sig is not None:
                        thesis_ok, reason = self._check_thesis(pos, price, sig, atr)
                        if thesis_ok:
                            self._pos.hold_extensions += 1
                            ext_min = extension_sec // 60
                            total_min = (base_hold + pos.hold_extensions * extension_sec) // 60
                            logger.info(
                                f"⏰ Max hold: THESIS VALID → extending +{ext_min}m "
                                f"({pos.hold_extensions}/{max_ext}) | total allowed: {total_min}m | {reason}")
                            send_telegram_message(
                                f"⏰ <b>MAX HOLD — THESIS EXTENSION</b>\n"
                                f"Trade: {pos.side.upper()} @ ${pos.entry_price:,.2f}\n"
                                f"Current: ${price:,.2f} ({'+' if profit > 0 else ''}{profit:.2f} pts)\n"
                                f"Extension: {pos.hold_extensions}/{max_ext} (+{ext_min}min)\n"
                                f"Reason: {reason}\n"
                                f"<i>Thesis still valid — letting it work</i>")
                            return
                        else:
                            logger.info(f"⏰ Max hold: THESIS BROKEN → exit | {reason}")

                    # CASE 3: No extensions left OR thesis broken
                    logger.info(f"⏰ Max hold → exit")
                    self._exit_trade(order_manager, price, "max_hold_time")
                    return

        # ── Trailing SL ──────────────────────────────────────────────────────
        if self.get_trail_enabled() and now - pos.last_trail_time >= QCfg.TRAIL_INTERVAL_S():
            self._pos.last_trail_time = now
            if self._update_trailing_sl(order_manager, data_manager, price, now): return

    def _check_thesis(self, pos, price, sig, atr) -> Tuple[bool, str]:
        """
        v4.6: Check if the original trade thesis is still valid.
        Returns (thesis_valid, reason_string).

        A trade thesis is valid when:
          1. Price is still between SL and TP (not breached either)
          2. Composite signal has not flipped against the trade
          3. Not deeply underwater (< THESIS_MAX_DRAWDOWN_PCT of SL distance)
          4. For reversion: price hasn't passed VWAP (reversion hasn't completed)

        If ANY check fails → thesis broken → force exit.
        If ALL pass → thesis valid → grant extension.
        """
        reasons = []

        # 1. Price between SL and TP
        if pos.side == "long":
            if price <= pos.sl_price:
                return False, "price at/below SL"
            if price >= pos.tp_price:
                return False, "price at/above TP"
        else:
            if price >= pos.sl_price:
                return False, "price at/above SL"
            if price <= pos.tp_price:
                return False, "price at/below TP"

        # 2. Composite signal still agrees (or at least neutral)
        # For LONG: composite should not be deeply negative
        # For SHORT: composite should not be deeply positive
        comp = sig.composite
        if pos.side == "long" and comp < -0.15:
            return False, f"composite flipped bearish ({comp:+.3f})"
        if pos.side == "short" and comp > 0.15:
            return False, f"composite flipped bullish ({comp:+.3f})"
        reasons.append(f"Σ={comp:+.3f}")

        # 3. Not deeply underwater
        drawdown = (pos.entry_price - price) if pos.side == "long" else (price - pos.entry_price)
        # v4.6 BUG FIX #2: Use ORIGINAL SL distance, not current (may be tightened by trail)
        # When trail moves SL from $71,200 to $72,900, using current SL makes DD look like 100%+
        sl_dist = pos.initial_sl_dist if pos.initial_sl_dist > 0 else abs(pos.entry_price - pos.sl_price)
        if sl_dist > 0:
            dd_pct = drawdown / sl_dist
            max_dd = QCfg.THESIS_MAX_DRAWDOWN_PCT()
            if dd_pct > max_dd:
                return False, f"drawdown {dd_pct:.0%} > {max_dd:.0%} of SL"
            reasons.append(f"DD={dd_pct:.0%}")
        
        # 4. Reversion: check if price has blown through VWAP significantly
        # v4.6 NOTE: Do NOT exit just because price crossed VWAP.
        # TP is often BEYOND VWAP (e.g., VWAP + 1.5×SL_dist). Exiting at VWAP
        # would kill winning trades that are on their way to TP.
        # Only flag this for information, not for thesis break.
        vwap = self._vwap.vwap
        if pos.trade_mode == "reversion" and vwap > 0 and atr > 1e-10:
            vwap_dist_atr = abs(price - vwap) / atr
            if pos.side == "long":
                if price > vwap:
                    reasons.append(f"past VWAP ✅ (+{vwap_dist_atr:.1f}ATR)")
                else:
                    reasons.append(f"VWAP={vwap_dist_atr:.1f}ATR away")
            else:
                if price < vwap:
                    reasons.append(f"past VWAP ✅ (+{vwap_dist_atr:.1f}ATR)")
                else:
                    reasons.append(f"VWAP={vwap_dist_atr:.1f}ATR away")

        # 5. Bonus: if signals are strong in our direction, thesis is robust
        if pos.side == "long" and comp > 0.2:
            reasons.append("signals STRONG ✅")
        elif pos.side == "short" and comp < -0.2:
            reasons.append("signals STRONG ✅")

        return True, " | ".join(reasons)

    def _update_trailing_sl(self, order_manager, data_manager, price, now) -> bool:
        """Institutional trail v4.8: 7-bug rewrite."""
        pos = self._pos; atr = self._atr_5m.atr
        if atr < 1e-10: return False
        if pos.entry_price < 1.0:
            logger.warning("Trail: entry_price invalid (%.2f) — skipping", pos.entry_price)
            return False
        if pos.sl_order_id is None:
            logger.debug("Trail: sl_order_id unknown — skipping")
            return False
        profit = (price-pos.entry_price) if pos.side=="long" else (pos.entry_price-price)
        if profit > pos.peak_profit: pos.peak_profit = profit

        # Track absolute peak price (used by chandelier)
        if pos.side == "long":
            if price > pos.peak_price_abs:
                pos.peak_price_abs = price
        else:
            if pos.peak_price_abs < 1e-10 or price < pos.peak_price_abs:
                pos.peak_price_abs = price

        try: candles_1m = data_manager.get_candles("1m", limit=60)
        except Exception: candles_1m = []
        try: candles_5m = data_manager.get_candles("5m", limit=30)
        except Exception: candles_5m = []
        try: orderbook = data_manager.get_orderbook()
        except Exception: orderbook = {"bids": [], "asks": []}

        hold_secs = now - pos.entry_time

        # v4.9: Pass ict_engine + now_ms so trail can use ICT zone freeze,
        # OB anchor SL, and liquidity pool ceiling — the three new protections
        # that eliminate the "stopped during pullback, TP fires without us" pattern.
        new_sl = InstitutionalLevels.compute_trail_sl(
            pos.side, price, pos.entry_price, pos.sl_price, atr,
            candles_1m, orderbook, pos.peak_profit, pos.entry_vol,
            hold_seconds=hold_secs, peak_price_abs=pos.peak_price_abs,
            trade_mode=pos.trade_mode, candles_5m=candles_5m,
            initial_sl_dist=pos.initial_sl_dist,
            ict_engine=self._ict,
            now_ms=int(now * 1000))

        if new_sl is None:
            return False

        new_sl_tick = _round_to_tick(new_sl)

        # v4.8: Use initial_sl_dist for tier, and peak_profit for phase (ratchet up)
        init_dist = pos.initial_sl_dist if pos.initial_sl_dist > 1e-10 else atr
        tier = max(profit, pos.peak_profit) / init_dist if init_dist > 1e-10 else 0.0
        if tier < QCfg.TRAIL_BE_R():
            phase_label = "⬜ P0 (hands off)"
        elif tier < QCfg.TRAIL_LOCK_R():
            phase_label = "🟡 P1 (structure)"
        elif tier < QCfg.TRAIL_AGGRESSIVE_R():
            phase_label = "🟠 P2 (chandelier)"
        else:
            phase_label = "🟢 P3 (full)"

        min_d = QCfg.TRAIL_MIN_DIST_ATR_P1() * atr if tier < QCfg.TRAIL_LOCK_R() else (
                QCfg.TRAIL_MIN_DIST_ATR_P2() * atr if tier < QCfg.TRAIL_AGGRESSIVE_R() else
                QCfg.TRAIL_MIN_DIST_ATR_P3() * atr)
        hm = (now - pos.entry_time) / 60.0
        logger.info(
            f"🔒 Trail [{phase_label}] ${pos.sl_price:,.1f} → ${new_sl_tick:,.1f} | "
            f"R={tier:.2f} MFE={pos.peak_profit:.1f}pts hold={hm:.0f}m "
            f"min_dist=${min_d:.0f}")
        send_telegram_message(
            f"🔒 <b>TRAIL SL</b> [{phase_label}]\n"
            f"${pos.sl_price:,.2f} → ${new_sl_tick:,.2f}\n"
            f"R: {tier:.2f} | MFE: {pos.peak_profit:.1f} pts | Hold: {hm:.0f}m\n"
            f"Min dist: ${min_d:.0f} ({min_d/atr:.1f}×ATR)")

        es = "sell" if pos.side=="long" else "buy"
        result = order_manager.replace_stop_loss(existing_sl_order_id=pos.sl_order_id, side=es, quantity=pos.quantity, new_trigger_price=new_sl_tick)
        if result is None:
            logger.warning("🚨 SL already fired"); self._record_exchange_exit(None); return True
        if isinstance(result, dict) and "error" in result: return False
        if result and isinstance(result, dict):
            self._pos.sl_price = new_sl_tick; self._pos.sl_order_id = result.get("order_id", pos.sl_order_id)
            self.current_sl_price = new_sl_tick
            if not pos.trail_active:
                self._pos.trail_active = True; logger.info("✅ Trailing SL active")
                send_telegram_message("✅ Trailing SL now active")
        return False

    def _exit_trade(self, order_manager, price, reason):
        pos = self._pos
        if pos.phase != PositionPhase.ACTIVE: return
        logger.info(f"🚪 EXIT {pos.side.upper()} @ ${price:,.2f} | {reason}")
        self._pos.phase = PositionPhase.EXITING
        order_manager.cancel_all_exit_orders(sl_order_id=pos.sl_order_id, tp_order_id=pos.tp_order_id)
        es = "sell" if pos.side=="long" else "buy"
        order_manager.place_market_order(side=es, quantity=pos.quantity, reduce_only=True)
        fill_type = getattr(pos, 'entry_fill_type', 'taker')
        pnl = self._estimate_pnl(pos, price, entry_fill_type=fill_type)
        self._record_pnl(pnl, exit_reason=reason, exit_price=price)

        hold_min     = (time.time() - pos.entry_time) / 60.0
        init_sl_dist = pos.initial_sl_dist if pos.initial_sl_dist > 1e-10 else abs(pos.entry_price - pos.sl_price)
        raw_pts      = (price - pos.entry_price) if pos.side == "long" else (pos.entry_price - price)
        achieved_r   = raw_pts / init_sl_dist if init_sl_dist > 1e-10 else 0.0
        tp_dist      = abs(pos.tp_price - pos.entry_price) if pos.tp_price > 0 else 0.0
        planned_rr   = tp_dist / init_sl_dist if init_sl_dist > 1e-10 else 0.0
        result_icon  = "✅" if pnl > 0 else "❌"

        send_telegram_message(
            f"{result_icon} <b>{'WIN' if pnl>0 else 'LOSS'} — MANUAL EXIT</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Side:     {pos.side.upper()} [{pos.trade_mode.upper()}]\n"
            f"Reason:   {reason}\n"
            f"Entry:    ${pos.entry_price:,.2f}\n"
            f"Exit:     <b>${price:,.2f}</b>  ({'+' if raw_pts>=0 else ''}{raw_pts:.1f} pts)\n"
            f"PnL:      <b>${pnl:+.2f} USDT</b>\n"
            f"R:        {achieved_r:+.2f}R  (planned 1:{planned_rr:.2f}R)\n"
            f"Hold:     {hold_min:.1f}m\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>Session: {self._total_trades}T | WR: {self._win_rate():.0%} | Total PnL: ${self._total_pnl:+.2f}</i>"
        )
        self._last_exit_side = pos.side
        self._finalise_exit()

    def _record_exchange_exit(self, ex_pos):
        """
        v4.9: Correct TP/SL detection, accurate PnL, reformatted notification.

        BUG FIXES:
        1. _last_known_price between SL and TP was used as exit_price — gave wrong PnL
           when SL fired during a pullback recovery. Now uses sl_price/tp_price directly.
        2. Win counted as pnl>0, but pnl could be positive due to wrong exit_price.
           Fixed by using actual sl_price when SL fired.
        3. _total_trades now incremented HERE (at close) not at entry — win-rate
           denominator only counts completed trades.
        4. Full trade record stored in _trade_history for /trades command.
        """
        pos = self._pos
        if pos.phase == PositionPhase.FLAT:
            logger.debug("_record_exchange_exit skipped — already FLAT")
            return

        # ─── Step 1: Determine exit type (SL or TP) and exit price ──────────────
        # Priority: if last known price is at/past TP → TP hit.
        #           if last known price is at/before SL → SL hit.
        #           Otherwise default to SL (conservative — avoids false wins).
        exit_reason = "exchange_exit"
        exit_price  = 0.0
        is_tp_hit   = False
        is_sl_hit   = False

        if pos.entry_price > 0 and pos.quantity > 0:
            lkp = self._last_known_price if self._last_known_price > 1.0 else 0.0
            sp  = pos.sl_price
            tp  = pos.tp_price
            trailed = pos.trail_active

            if pos.side == "long":
                if tp > 0 and lkp >= tp * 0.9995:   # price at or above TP (5 pip grace)
                    exit_price  = tp
                    exit_reason = "tp_hit"
                    is_tp_hit   = True
                elif sp > 0 and lkp <= sp * 1.0005:  # price at or below SL
                    exit_price  = sp
                    exit_reason = "trail_sl_hit" if trailed else "sl_hit"
                    is_sl_hit   = True
                else:
                    # Ambiguous: price between SL and TP when we detected exit.
                    # Default to SL price — conservative (avoids false win PnL).
                    exit_price  = sp if sp > 0 else (lkp if lkp > 0 else pos.entry_price)
                    exit_reason = "trail_sl_hit" if trailed else "sl_hit"
                    is_sl_hit   = True
            else:  # short
                if tp > 0 and lkp <= tp * 1.0005:
                    exit_price  = tp
                    exit_reason = "tp_hit"
                    is_tp_hit   = True
                elif sp > 0 and lkp >= sp * 0.9995:
                    exit_price  = sp
                    exit_reason = "trail_sl_hit" if trailed else "sl_hit"
                    is_sl_hit   = True
                else:
                    exit_price  = sp if sp > 0 else (lkp if lkp > 0 else pos.entry_price)
                    exit_reason = "trail_sl_hit" if trailed else "sl_hit"
                    is_sl_hit   = True

        # ─── Step 2: Compute PnL ─────────────────────────────────────────────────
        pnl = 0.0
        if exit_price > 0:
            fill_type = getattr(pos, 'entry_fill_type', 'taker')
            pnl = self._estimate_pnl(pos, exit_price, entry_fill_type=fill_type)
            logger.info(
                f"📊 Exit price=${exit_price:,.2f} reason={exit_reason} "
                f"entry=${pos.entry_price:,.2f} pnl=${pnl:+.2f}"
            )

        # ─── Step 3: Record PnL and trade history ────────────────────────────────
        self._record_pnl(pnl, exit_reason=exit_reason, exit_price=exit_price)

        # ─── Step 4: Build notification ──────────────────────────────────────────
        hold_min = (time.time() - pos.entry_time) / 60.0 if pos.entry_time > 0 else 0.0
        init_sl_dist = pos.initial_sl_dist if pos.initial_sl_dist > 1e-10 else abs(pos.entry_price - pos.sl_price)

        # Determine actual R achieved
        raw_pts = ((exit_price - pos.entry_price) if pos.side == "long"
                   else (pos.entry_price - exit_price))
        achieved_r = raw_pts / init_sl_dist if init_sl_dist > 1e-10 else 0.0

        if is_tp_hit:
            result_icon  = "🎯"
            result_label = "TP HIT"
            result_color = "WIN ✅"
        elif is_sl_hit and pnl > 0:
            result_icon  = "🔒"
            result_label = "TRAIL SL" if pos.trail_active else "SL HIT (profitable)"
            result_color = "WIN ✅"
        else:
            result_icon  = "🛑"
            result_label = "SL HIT"
            result_color = "LOSS ❌"

        mfe_r   = pos.peak_profit / init_sl_dist if init_sl_dist > 1e-10 else 0.0
        tp_dist = abs(pos.tp_price - pos.entry_price) if pos.tp_price > 0 else 0.0
        planned_rr = tp_dist / init_sl_dist if init_sl_dist > 1e-10 else 0.0

        send_telegram_message(
            f"{result_icon} <b>{result_color} — {result_label}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Side:     {pos.side.upper()} [{pos.trade_mode.upper()}]\n"
            f"Entry:    ${pos.entry_price:,.2f}\n"
            f"Exit:     <b>${exit_price:,.2f}</b>  ({'+' if raw_pts>=0 else ''}{raw_pts:.1f} pts)\n"
            f"PnL:      <b>${pnl:+.2f} USDT</b>\n"
            f"R:        {achieved_r:+.2f}R  (planned 1:{planned_rr:.2f}R)\n"
            f"MFE:      {mfe_r:.2f}R  |  Hold: {hold_min:.1f}m\n"
            f"Trail:    {'✅ was active' if pos.trail_active else '— not activated'}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>Session: {self._total_trades}T | WR: {self._win_rate():.0%} | Total PnL: ${self._total_pnl:+.2f}</i>"
        )
        self._last_exit_side = pos.side
        self._finalise_exit()

    def _record_pnl(self, pnl: float, exit_reason: str = "unknown",
                    exit_price: float = 0.0):
        """
        Record a completed trade. _total_trades incremented HERE (at close),
        not at entry — ensures win-rate denominator only counts closed trades.
        """
        pos = self._pos
        self._total_trades += 1
        self._total_pnl    += pnl
        is_win = pnl > 0
        if is_win:
            self._winning_trades += 1
        self._risk_gate.record_trade_result(pnl)

        # Full trade record for /trades command
        init_sl_dist = getattr(pos, 'initial_sl_dist', 0.0)
        self._trade_history.append({
            "ts":           time.time(),
            "side":         getattr(pos, 'side', '?'),
            "mode":         getattr(pos, 'trade_mode', '?'),
            "entry":        getattr(pos, 'entry_price', 0.0),
            "exit":         exit_price,
            "qty":          getattr(pos, 'quantity', 0.0),
            "sl":           getattr(pos, 'sl_price', 0.0),
            "tp":           getattr(pos, 'tp_price', 0.0),
            "init_sl_dist": init_sl_dist,
            "pnl":          pnl,
            "is_win":       is_win,
            "reason":       exit_reason,
            "hold_min":     (time.time() - pos.entry_time) / 60.0 if getattr(pos,'entry_time',0) > 0 else 0.0,
            "trailed":      getattr(pos, 'trail_active', False),
            "mfe_r":        (getattr(pos,'peak_profit',0.0) / init_sl_dist
                             if init_sl_dist > 1e-10 else 0.0),
        })
        # Keep last 200 trades in memory
        if len(self._trade_history) > 200:
            self._trade_history = self._trade_history[-200:]

    def _finalise_exit(self):
        self._pos = PositionState(); self._last_exit_time = time.time()
        self.current_sl_price = 0.0; self.current_tp_price = 0.0
        logger.info("Position closed — FLAT")

    def _compute_quantity(self, risk_manager, price):
        bal = risk_manager.get_available_balance()
        if bal is None: return None
        available = float(bal.get("available", 0.0))
        if available < QCfg.MIN_MARGIN_USDT(): return None
        margin_alloc = available * QCfg.MARGIN_PCT()
        if margin_alloc < QCfg.MIN_MARGIN_USDT(): return None
        notional = margin_alloc * QCfg.LEVERAGE(); qty_raw = notional / price
        step = QCfg.LOT_STEP(); qty = math.floor(qty_raw/step)*step; qty = round(qty,8)
        qty = max(QCfg.MIN_QTY(), min(QCfg.MAX_QTY(), qty))
        if (qty*price)/QCfg.LEVERAGE() > margin_alloc*1.02: return None
        logger.info(f"Sizing → avail=${available:.2f} | alloc={QCfg.MARGIN_PCT():.0%} | margin=${margin_alloc:.2f} | {QCfg.LEVERAGE()}x | qty={qty}")
        return qty

    @staticmethod
    def _estimate_pnl(pos, exit_price, entry_fill_type="taker"):
        """v4.3 Bug 12 fix: uses correct fee rate based on actual entry fill type."""
        gross = ((exit_price-pos.entry_price) if pos.side=="long" else (pos.entry_price-exit_price))*pos.quantity
        # Entry fee: maker or taker based on how we actually entered
        if entry_fill_type == "maker":
            entry_rate = float(getattr(config, "COMMISSION_RATE_MAKER", QCfg.COMMISSION_RATE() * 0.40))
        else:
            entry_rate = QCfg.COMMISSION_RATE()
        # Exit fee: always taker (SL/TP are market orders)
        exit_rate = QCfg.COMMISSION_RATE()
        entry_fee = pos.entry_price * pos.quantity * entry_rate
        exit_fee = exit_price * pos.quantity * exit_rate
        return gross - entry_fee - exit_fee

    def _win_rate(self): return self._winning_trades/self._total_trades if self._total_trades else 0.0

    def get_stats(self):
        """Returns stats based on CLOSED trades only — correct win-rate denominator."""
        return {
            "total_trades":   self._total_trades,
            "winning_trades": self._winning_trades,
            "win_rate":       f"{self._win_rate():.1%}",
            "total_pnl":      round(self._total_pnl, 2),
            "daily_trades":   self._risk_gate.daily_trades,
            "consec_losses":  self._risk_gate.consec_losses,
            "current_phase":  self._pos.phase.name,
            "last_signal":    str(self._last_sig),
            "atr_5m":         round(self._atr_5m.atr, 2),
            "atr_1m":         round(self._atr_1m.atr, 2),
            "atr_pctile":     f"{self._atr_5m.get_percentile():.0%}",
            "regime_ok":      self._atr_5m.regime_valid(),
        }

    def format_status_report(self):
        """
        v4.9: Status report reads from _trade_history (ground truth).
        Correct win-rate, expectancy, and live position R display.
        """
        p   = self._pos
        atr = self._atr_5m.atr

        # ── Session stats from _trade_history ────────────────────────────
        hist     = self._trade_history
        total_t  = self._total_trades
        wins     = self._winning_trades
        losses   = total_t - wins
        wr       = wins / total_t * 100.0 if total_t > 0 else 0.0
        total_pnl = self._total_pnl

        win_pnls  = [t['pnl'] for t in hist if t.get('is_win')]
        loss_pnls = [t['pnl'] for t in hist if not t.get('is_win')]
        avg_w  = sum(win_pnls)  / len(win_pnls)  if win_pnls  else 0.0
        avg_l  = sum(loss_pnls) / len(loss_pnls) if loss_pnls else 0.0
        expect = (wr/100 * avg_w) + ((1 - wr/100) * avg_l) if total_t > 0 else 0.0

        lines = [
            "📊 <b>QUANT v4.9 STATUS</b>", "",
            f"Regime: {self._regime.regime.value if self._regime else '?'} | "
            f"ATR: ${atr:.1f} ({self._atr_5m.get_percentile():.0%}pctile)",
            f"HTF:  15m={self._htf.trend_15m:+.2f}  4h={self._htf.trend_4h:+.2f}",
            f"VWAP: ${self._vwap.vwap:,.2f}  (dev={self._vwap.deviation_atr:+.1f}ATR)",
            "",
            "<b>Session P&L</b>",
            f"  Trades: {total_t}  W:{wins} L:{losses}  WR: {wr:.0f}%",
            f"  Total PnL: ${total_pnl:+.2f} USDT",
            f"  Avg W: ${avg_w:+.2f}  Avg L: ${avg_l:+.2f}",
            f"  Expectancy: ${expect:+.2f}/trade",
            f"  Daily: {self._risk_gate.daily_trades}/{QCfg.MAX_DAILY_TRADES()} "
            f"| ConsecL: {self._risk_gate.consec_losses}/{QCfg.MAX_CONSEC_LOSSES()}",
        ]

        # ── Execution cost diagnostics ──────────────────────────────────
        if self._fee_engine is not None:
            try:
                snap = self._fee_engine.diagnostic_snapshot()
                warmed = snap.get('engine_warmed', False)
                warmup_tag = "✅" if warmed else f"⏳ warming ({snap.get('spread_samples',0)} samples)"
                lines += [
                    "",
                    f"<b>Execution costs</b> [{warmup_tag}]",
                    f"  Spread: {snap['spread_median_bps']:.1f}bps "
                    f"(p90: {snap['spread_p90_bps']:.1f})",
                    f"  Slip EWMA: {snap['slippage_ewma_bps']:.1f}bps",
                    f"  RT taker: {snap['rt_cost_taker_bps']:.1f}bps "
                    f"| maker: {snap['rt_cost_maker_bps']:.1f}bps "
                    f"| saving: {snap['maker_saving_bps']:.1f}bps",
                ]
            except Exception:
                pass

        # ── Active position ──────────────────────────────────────────────
        if not p.is_flat():
            price    = self._last_known_price
            hm       = (time.time() - p.entry_time) / 60.0
            init_sl  = p.initial_sl_dist if p.initial_sl_dist > 1e-10 else abs(p.entry_price - p.sl_price)
            raw_pts  = (price - p.entry_price) if p.side == "long" else (p.entry_price - price)
            curr_r   = raw_pts / init_sl if init_sl > 1e-10 else 0.0
            mfe_r    = p.peak_profit / init_sl if init_sl > 1e-10 else 0.0
            tp_dist  = abs(p.tp_price - p.entry_price) if p.tp_price > 0 else 0.0
            planned  = tp_dist / init_sl if init_sl > 1e-10 else 0.0
            upnl     = raw_pts * p.quantity
            lines += [
                "",
                f"<b>🟢 Active {p.side.upper()} [{p.trade_mode.upper()}]</b>",
                f"  Entry: ${p.entry_price:,.2f}  |  Now: ${price:,.2f}",
                f"  SL: ${p.sl_price:,.2f}  |  TP: ${p.tp_price:,.2f}",
                f"  uPnL: ${upnl:+.2f}  |  R: {curr_r:+.2f}R  |  MFE: {mfe_r:.2f}R",
                f"  Planned R:R: 1:{planned:.2f}  |  Hold: {hm:.1f}m",
                f"  Trail: {'✅ active' if p.trail_active else '⏳ waiting'}",
            ]
        return "\n".join(lines)

    # ─── RECONCILIATION (unchanged logic, fixed PnL) ───
    def _reconcile_query_thread(self, order_manager):
        try:
            ex_pos = order_manager.get_open_position()
            if ex_pos is None: return
            ex_size = float(ex_pos.get("size",0.0)); open_orders = None
            if ex_size >= float(getattr(config,"MIN_POSITION_SIZE",0.001)):
                try: open_orders = order_manager.get_open_orders()
                except Exception: pass
            with self._lock: self._reconcile_data = {"ex_pos":ex_pos,"open_orders":open_orders}
        except Exception as e: logger.warning(f"Reconcile error: {e}")
        finally: self._reconcile_pending = False

    def _reconcile_apply(self, order_manager, data):
        ex_pos=data["ex_pos"]; open_orders=data.get("open_orders")
        ex_size=float(ex_pos.get("size",0.0)); ex_side=str(ex_pos.get("side") or "").upper()
        phase = self._pos.phase
        if phase==PositionPhase.FLAT and ex_size>=QCfg.MIN_QTY():
            ex_entry=float(ex_pos.get("entry_price",0.0)); ex_upnl=float(ex_pos.get("unrealized_pnl",0.0))
            # Guard: CoinSwitch sometimes returns entry_price=0 for a position that
            # has been filled but not yet fully settled in the position feed.
            # Adopting it with entry_price=0 produces tier=~800 in the trail engine
            # (profit = current_price − 0 = ~70k), fires the chandelier, and then
            # tries to place a duplicate SL that the exchange rejects with 400.
            # The resulting None from replace_stop_loss was misread as "SL fired" →
            # false FLAT → re-adoption loop. Reject and wait for the next cycle.
            if ex_entry < 1.0:
                logger.warning(
                    f"Reconcile: skipping adoption of {ex_side} size={ex_size} "
                    f"— entry_price={ex_entry:.2f} not yet settled on exchange")
                return
            sl_oid=tp_oid=None; sl_p=tp_p=0.0
            if open_orders:
                for o in open_orders:
                    ot=o.get("type","").upper()
                    if ot in ("STOP_MARKET","STOP","STOP_LOSS_MARKET"): sl_oid=o["order_id"]; sl_p=float(o.get("trigger_price") or 0)
                    elif ot in ("TAKE_PROFIT_MARKET","TAKE_PROFIT"): tp_oid=o["order_id"]; tp_p=float(o.get("trigger_price") or 0)
            iside = "long" if ex_side=="LONG" else "short"
            self._pos = PositionState(phase=PositionPhase.ACTIVE, side=iside, quantity=ex_size,
                entry_price=ex_entry, sl_price=sl_p, tp_price=tp_p, sl_order_id=sl_oid,
                tp_order_id=tp_oid, entry_time=time.time(), initial_sl_dist=abs(ex_entry-sl_p) if sl_p>0 else 0.0,
                entry_atr=self._atr_5m.atr)
            self.current_sl_price=sl_p; self.current_tp_price=tp_p
            self._confirm_long=self._confirm_short=0
            logger.warning(f"⚡ RECONCILE: adopted {ex_side} @ ${ex_entry:,.2f}")
            send_telegram_message(f"⚡ <b>POSITION ADOPTED</b>\nSide: {ex_side} | Size: {ex_size}\nEntry: ${ex_entry:,.2f} | uPnL: ${ex_upnl:+.2f}")
            return
        if phase==PositionPhase.ACTIVE and ex_size<QCfg.MIN_QTY():
            logger.info("📡 Reconcile: exchange FLAT → TP/SL fired")
            self._record_exchange_exit(ex_pos); return
        if phase==PositionPhase.ACTIVE and ex_size>=QCfg.MIN_QTY():
            if (not self._pos.sl_order_id or not self._pos.tp_order_id) and open_orders:
                for o in open_orders:
                    ot=o.get("type","").upper()
                    if not self._pos.sl_order_id and ot in ("STOP_MARKET","STOP","STOP_LOSS_MARKET"): self._pos.sl_order_id=o["order_id"]
                    elif not self._pos.tp_order_id and ot in ("TAKE_PROFIT_MARKET","TAKE_PROFIT"): self._pos.tp_order_id=o["order_id"]

    def _sync_position(self, order_manager):
        try: ex_pos = order_manager.get_open_position()
        except Exception: return
        if ex_pos is None: return
        ex_size = float(ex_pos.get("size",0.0))
        if self._pos.phase==PositionPhase.ACTIVE:
            if ex_size<QCfg.MIN_QTY():
                logger.info("📡 Sync: exchange FLAT → TP/SL fired")
                self._record_exchange_exit(ex_pos)
        elif self._pos.phase==PositionPhase.EXITING:
            if ex_size<QCfg.MIN_QTY(): self._finalise_exit()

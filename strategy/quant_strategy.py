"""
QUANT STRATEGY v5.0 — ICT-COMMANDER / QUANT-SCOUT ARCHITECTURE
==============================================================
Architecture:
  ICT Engine  = Commander  — defines what to trade, where SL/TP go, when structure is valid
  Quant Engine = Scout     — provides order-flow timing signals to execute within ICT setups

Entry tiers (via ICTEntryGate):
  Tier-S: ICT Sweep-and-Go in OTE zone (AMD sweep + displacement + 61.8%-78.6% retrace)
    → SL at sweep wick extreme (0.5-0.8×ATR), TP at AMD delivery target (opposing pool)
    → Confirm ticks: 1-2 | Expected edge: 58-66% WR, 1:2.5-4.0 R:R
  Tier-A: ICT structural alignment (DISTRIBUTION/REACCUMULATION context)
    → 2 confirm ticks | Expected edge: 52-58% WR, 1:2.0-3.5 R:R
  Tier-B: Standard quant + ICT confluence
    → 3 confirm ticks | Expected edge: 48-54% WR, 1:1.8-2.5 R:R

Quant signals used as entry helpers (NOT primary gates):
  VWAP deviation, CVD, tick flow, orderbook imbalance, volume exhaustion
  → For Tier-S/A: soft veto only (tick flow not strongly opposing)
  → For Tier-B:   hard requirements (overextended, n_confirming≥3, composite≥threshold)

SL: ICTSLEngine 4-tier (sweep wick → disp OB → 15m ICT OB → 15m swing)
TP: ICTTPEngine delivery target (opposing pool → structural → VWAP)
Trail: _ICTStructureTrail — BOS/CHoCH/OB/FVG/swing inline engine (no external deps)
"""

from __future__ import annotations
import logging, math, time, threading
from collections import deque
import dataclasses
from dataclasses import dataclass, field
from datetime import date, datetime, timezone, timedelta
from enum import Enum, auto
from typing import Dict, List, Optional, Tuple

import sys, os as _os; sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
import config
from telegram.notifier import send_telegram_message
from execution.order_manager import CancelResult
try:
    from strategy.ict_engine import ICTEngine, ICTConfluence
    _ICT_AVAILABLE = True
except ImportError:
    _ICT_AVAILABLE = False
try:
    from strategy.fee_engine import ExecutionCostEngine
except ImportError:
    ExecutionCostEngine = None   # fee_engine.py not yet present — graceful fallback

_HUNT_AVAILABLE = False  # LiquidityHunter removed; v9 uses LiquidityMap + EntryEngine

# ── ICT Institutional Trade Engine — fully inlined; external module removed ─
# All ICTEntryGate / ICTSweepDetector / ICTSLEngine / ICTTPEngine / ICTTrailEngine
# logic is embedded directly in this file.  ict_trade_engine.py is no longer
# needed and the import attempt has been removed to eliminate the startup warning.
_ICT_TRADE_ENGINE_AVAILABLE = False

logger = logging.getLogger(__name__)

# -- v9.0: Liquidity-First Entry Engine ------------------------------------
try:
    from strategy.liquidity_map import LiquidityMap
    _LIQ_MAP_AVAILABLE = True
except ImportError:
    try:
        from liquidity_map import LiquidityMap
        _LIQ_MAP_AVAILABLE = True
    except ImportError:
        _LIQ_MAP_AVAILABLE = False

try:
    from strategy.entry_engine import (
        EntryEngine, ICTTrailManager, OrderFlowState, ICTContext,
        EntryType, ICTSweepEvent,
    )
    _ENTRY_ENGINE_AVAILABLE = True
except ImportError:
    try:
        from entry_engine import (
            EntryEngine, ICTTrailManager, OrderFlowState, ICTContext,
            EntryType, ICTSweepEvent,
        )
        _ENTRY_ENGINE_AVAILABLE = True
    except ImportError:
        _ENTRY_ENGINE_AVAILABLE = False


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
    def TRAIL_INTERVAL_S() -> int:
        """DEPRECATED v6.0: Time-based trail interval eliminated.
        Trail is now structure-event-driven. This accessor is kept for
        backward compat only — it is NOT used in any trail logic."""
        return int(_cfg("TRAILING_SL_CHECK_INTERVAL", 10))
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
    def MAX_DAILY_TRADES() -> int: return int(_cfg("MAX_DAILY_TRADES", 20))
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
    # ── v5.0: ICT Sweep Engine params ────────────────────────────────────────
    @staticmethod
    def ICT_SWEEP_ENTRY_ENABLED() -> bool: return bool(_cfg("QUANT_ICT_SWEEP_ENTRY_ENABLED", True))
    @staticmethod
    def ICT_SWEEP_AMD_CONF_TIER_S() -> float: return float(_cfg("QUANT_ICT_SWEEP_AMD_CONF_TIER_S", 0.62))
    @staticmethod
    def ICT_SWEEP_AMD_CONF_TIER_A() -> float: return float(_cfg("QUANT_ICT_SWEEP_AMD_CONF_TIER_A", 0.50))
    @staticmethod
    def ICT_SWEEP_ICT_MIN_TIER_S() -> float: return float(_cfg("QUANT_ICT_SWEEP_ICT_MIN_TIER_S", 0.60))
    @staticmethod
    def ICT_SWEEP_ICT_MIN_TIER_A() -> float: return float(_cfg("QUANT_ICT_SWEEP_ICT_MIN_TIER_A", 0.55))
    @staticmethod
    def ICT_SWEEP_ICT_MIN_TIER_B() -> float: return float(_cfg("QUANT_ICT_SWEEP_ICT_MIN_TIER_B", 0.40))
    @staticmethod
    def ICT_TRAIL_AMD_PHASE_AWARE() -> bool: return bool(_cfg("QUANT_ICT_TRAIL_AMD_PHASE_AWARE", True))
    @staticmethod
    def ICT_TRAIL_MANIP_FREEZE_R() -> float: return float(_cfg("QUANT_ICT_TRAIL_MANIP_FREEZE_R", 1.5))
    @staticmethod
    def ICT_TP_MIN_RR_REVERSION() -> float: return float(_cfg("QUANT_ICT_TP_MIN_RR_REVERSION", 1.8))
    @staticmethod
    def ICT_TP_MIN_RR_TREND() -> float: return float(_cfg("QUANT_ICT_TP_MIN_RR_TREND", 2.5))
    # ── v5.1: CHoCH staleness expiry ─────────────────────────────────────────
    @staticmethod
    def CHOCH_EXPIRY_BARS() -> int: return int(_cfg("QUANT_CHOCH_EXPIRY_BARS", 10))

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

          ADX < 25 (ranging):       0.4×ATR threshold
          25 ≤ ADX < 35 (transit):  0.6×ATR threshold
          ADX ≥ 35 (trending):      0.9×ATR threshold
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
# ENGINE 2: CVD DIVERGENCE + TRUE TICK DELTA
# ═══════════════════════════════════════════════════════════════
class CVDEngine:
    """
    Cumulative Volume Delta engine — v6.0 (true tick tape + candle fallback).

    Two data paths:
    1. TRUE CVD (preferred): running sum of (buy_qty - sell_qty) per real trade tick.
       Sourced from TickFlowEngine.on_trade() calls via _feed_microstructure.
       Provides genuine institutional buy/sell pressure — no approximation.
    2. CANDLE CVD (fallback): (2C-H-L)/(H-L) × V per bar when real ticks
       are unavailable (warmup period or stream gap).

    Both paths feed the same divergence and trend signal computations.
    True CVD is preferred; candle path takes over automatically if tick history
    is insufficient (<50 ticks).
    """
    def __init__(self):
        # Candle-based delta history (OHLCV approximation — fallback)
        self._deltas: deque = deque(maxlen=QCfg.CVD_WINDOW() * QCfg.CVD_HIST_MULT())
        self._last_bar_ts: int = 0
        # True tick-tape CVD — (buy_vol - sell_vol) per trade, running window
        self._tick_cvd: deque = deque(maxlen=2000)   # 2000 most recent tick deltas
        self._tick_ts:  deque = deque(maxlen=2000)   # timestamps for windowing
        self._tick_count: int = 0                     # total ticks received
        self._tick_cvd_lock = threading.Lock()

    def reset_state(self) -> None:
        """Reset timestamps so warmup data is reprocessed after stream restart."""
        self._last_bar_ts = 0
        self._deltas.clear()
        # BUG-CVD-TICK-COUNT-STALE FIX: reset _tick_count so _get_true_cvd_array
        # doesn't pass the ">= 50 ticks" warmup check using stale pre-restart data.
        # The deques are preserved (their data may still be valid), but the count
        # must reflect the actual number of ticks in the current window.
        # Recount from the deque to preserve correctness after a reconnect.
        with self._tick_cvd_lock:
            self._tick_count = len(self._tick_cvd)

    def update_from_tick(self, price: float, qty: float, is_buy: bool) -> None:
        """
        Feed a real trade tick into the true CVD accumulator.
        Called from _feed_microstructure on every new raw trade.
        Dollar-volume delta: positive = buy pressure, negative = sell pressure.
        """
        dollar_delta = price * qty * (1.0 if is_buy else -1.0)
        ts = time.time()
        with self._tick_cvd_lock:
            self._tick_cvd.append(dollar_delta)
            self._tick_ts.append(ts)
            self._tick_count += 1

    def update(self, candles: List[Dict]) -> None:
        """Update candle-based OHLCV delta (fallback path)."""
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

    def _get_true_cvd_array(self, window_sec: float = 600.0) -> Optional[List[float]]:
        """
        Return rolling true CVD values over the last window_sec seconds.
        Returns None if insufficient tick data (<50 ticks in window).
        Each value is the running cumulative sum at that tick.
        """
        with self._tick_cvd_lock:
            if self._tick_count < 50:
                return None
            now   = time.time()
            cutoff = now - window_sec
            arr = list(self._tick_cvd)
            tss = list(self._tick_ts)
        # Build running sum over window
        running = []
        acc = 0.0
        for i, ts in enumerate(tss):
            if ts >= cutoff:
                acc += arr[i]
                running.append(acc)
        if len(running) < 20:
            return None
        return running

    def get_divergence_signal(self, candles: List[Dict]) -> float:
        """
        CVD divergence: detect when order flow disagrees with price direction.

        Prefers true tick CVD when ≥50 ticks available; falls back to candle OHLCV.

        Returns [-1, +1]:
          Positive = CVD rising while price falling (bullish divergence — buy signal)
          Negative = CVD falling while price rising (bearish divergence — sell signal)
          Zero     = CVD and price agree (no divergence)
        """
        w = QCfg.CVD_WINDOW()

        # ── Path 1: True tick CVD (preferred) ─────────────────────────
        true_cvd = self._get_true_cvd_array(window_sec=max(w * 60, 600.0))
        if true_cvd is not None and len(true_cvd) >= w + 10:
            arr = true_cvd; n = len(arr)
            # FIX 6: arr is a running cumulative sum. The old code did
            # sum(arr[-(w//2):]) which summed already-cumulative values —
            # a "sum of sums" that measures nothing about rate of change.
            # Correct: use level differences to extract the actual CVD
            # change over each half-window.
            midpoint   = n - w // 2
            recent_cvd  = arr[-1] - arr[midpoint - 1]          # CVD Δ in recent half
            # FIX: max(0, n-w)-1 can be -1 when n==w, wrapping to last element.
            _start_idx  = max(0, n - w)
            _start_val  = arr[_start_idx] if _start_idx > 0 else 0.0
            earlier_cvd = arr[midpoint - 1] - _start_val  # earlier half
            cvd_slope   = recent_cvd - earlier_cvd
            closes      = [float(c['c']) for c in candles[-w:]] if len(candles) >= w else []
            if len(closes) < w: return 0.0
            mid = w // 2
            price_slope = sum(closes[mid:])/max(len(closes[mid:]),1) - sum(closes[:mid])/max(len(closes[:mid]),1)
            if abs(price_slope) < 1e-10: return 0.0
            # Z-score: build distribution of slopes (differences), not cumulative sums
            slopes = []
            for i in range(w, n):
                # Each slope = CVD change over recent half minus CVD change over earlier half
                # within a rolling window ending at position i.
                _recent_half  = arr[i] - arr[i - w // 2]
                _earlier_half = arr[i - w // 2] - arr[i - w]
                slopes.append(_recent_half - _earlier_half)
            if len(slopes) < 5: return 0.0
            mu  = sum(slopes)/len(slopes)
            std = math.sqrt(sum((s-mu)**2 for s in slopes)/max(len(slopes)-1,1))
            if std < 1e-12: return 0.0
            cvd_z     = (cvd_slope - mu) / std
            price_dir = 1.0 if price_slope > 0 else -1.0
            if (1.0 if cvd_z > 0 else -1.0) == price_dir: return 0.0
            return -price_dir * min(abs(cvd_z), 3.0) / 3.0

        # ── Path 2: Candle OHLCV approximation (fallback) ─────────────
        # FIX 10: The old code built all_sums as a rolling-sum distribution and
        # divided cvd_slope (a difference of half-window sums) by its std.  These
        # have different statistical distributions — difference-of-sums has variance
        # 2σ², making the Z-score ~40% too small → CVD signal chronically underweighted.
        # Fix: build the distribution from the SAME statistic as cvd_slope (slopes),
        # so the Z-score is standardised against an identical distribution.
        arr = list(self._deltas); n = len(arr)
        if n < w + 10 or len(candles) < w: return 0.0
        recent_cvd  = sum(arr[-w//2:])
        earlier_cvd = sum(arr[-w:-w//2])
        cvd_slope   = recent_cvd - earlier_cvd
        closes      = [float(c['c']) for c in candles[-w:]]
        mid = w // 2
        price_slope = sum(closes[mid:])/max(len(closes[mid:]),1) - sum(closes[:mid])/max(len(closes[:mid]),1)
        if abs(price_slope) < 1e-10: return 0.0
        # Build rolling slope distribution matching cvd_slope's statistic exactly
        slopes = []
        for i in range(w, n):
            s_recent  = sum(arr[i - w//2 : i])
            s_earlier = sum(arr[i - w    : i - w//2])
            slopes.append(s_recent - s_earlier)
        if len(slopes) < 5: return 0.0
        mu  = sum(slopes) / len(slopes)
        std = math.sqrt(sum((s - mu)**2 for s in slopes) / max(len(slopes) - 1, 1))
        if std < 1e-12: return 0.0
        cvd_z     = (cvd_slope - mu) / std
        price_dir = 1.0 if price_slope > 0 else -1.0
        if (1.0 if cvd_z > 0 else -1.0) == price_dir: return 0.0
        return -price_dir * min(abs(cvd_z), 3.0) / 3.0

    def get_trend_signal(self) -> float:
        """
        Directional CVD bias for trend-following mode.

        Prefers true tick CVD; falls back to candle OHLCV.
        Returns +1.0 = sustained net buying, -1.0 = sustained net selling.
        """
        w = QCfg.CVD_WINDOW()

        # ── Path 1: True tick CVD ────────────────────────────────────
        # BUG-CVD-TREND-CUMSUM FIX: arr is a running cumulative sum.
        # The old code computed sum(arr[i:i+w]) which is the sum of already-
        # cumulative values — a completely different statistic.  The correct
        # measure of "how much did CVD change over window w?" is:
        #     arr[i+w-1] - arr[i-1]   (delta = end - start of window)
        # Using the wrong statistic made high-conviction buyside periods
        # look identical to neutral periods because cumulative sums grow
        # monotonically and the sum of a growing series always appears "high".
        true_cvd = self._get_true_cvd_array(window_sec=max(w * 90, 900.0))
        if true_cvd is not None and len(true_cvd) >= w + 10:
            arr = true_cvd; n = len(arr)
            # Build distribution of per-window CVD deltas
            deltas = []
            for i in range(1, n - w + 1):
                start_val = arr[i - 1]
                end_val   = arr[i + w - 1]
                deltas.append(end_val - start_val)
            if len(deltas) < 5:
                return 0.0
            # Most-recent window delta
            recent_delta = arr[-1] - arr[max(0, n - w - 1)]
            mu  = sum(deltas) / len(deltas)
            std = math.sqrt(sum((d - mu) ** 2 for d in deltas) / max(len(deltas) - 1, 1))
            if std < 1e-12:
                return _sigmoid(recent_delta / (abs(mu) + 1e-10), 0.5)
            return _sigmoid((recent_delta - mu) / std, 0.7)

        # ── Path 2: Candle OHLCV fallback ────────────────────────────
        arr = list(self._deltas); n = len(arr)
        if n < w + 10: return 0.0
        sums = []
        for i in range(n - w * 2, n - w + 1):
            if i >= 0:
                sums.append(sum(arr[i:i + w]))
        if len(sums) < 5: return 0.0
        recent_sum = sum(arr[-w:])
        mu  = sum(sums) / len(sums)
        std = math.sqrt(sum((s-mu)**2 for s in sums)/max(len(sums)-1, 1))
        if std < 1e-12:
            return _sigmoid(recent_sum / (abs(mu) + 1e-10), 0.5)
        return _sigmoid((recent_sum - mu) / std, 0.7)

    @property
    def tick_count(self) -> int:
        """Number of real trade ticks received since startup."""
        return self._tick_count

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
        def _qty(lvl):
            if isinstance(lvl,(list,tuple)) and len(lvl)>=2: return float(lvl[1])
            if isinstance(lvl,dict): return float(lvl.get("size") or lvl.get("quantity") or lvl.get("depth") or 0)
            return 0.0
        bid_depth = sum(_qty(l) for l in bids[:depth])
        ask_depth = sum(_qty(l) for l in asks[:depth])
        total = bid_depth + ask_depth
        if total < 1e-12: return
        self._last_imbalance = (bid_depth - ask_depth) / total
        self._imbalance_hist.append(self._last_imbalance)
        try:
            def _px(lvl):
                if isinstance(lvl,(list,tuple)): return float(lvl[0])
                if isinstance(lvl,dict): return float(lvl.get("limit_price") or lvl.get("price") or 0)
                return 0.0
            bb = _px(bids[0]); ba = _px(asks[0])
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
# ENGINE 4: TICK FLOW (regime-adaptive window)
# ═══════════════════════════════════════════════════════════════
class TickFlowEngine:
    """
    Real-time trade flow engine — v6.0 (regime-adaptive window).

    Window adapts to ATR percentile regime:
      Low vol  (ATR pctile < 30%): 60s — accumulate more signal in quiet markets
      Normal   (30%–70%):          30s — baseline
      High vol (ATR pctile > 70%): 15s — faster response in trending/volatile markets
      Extreme  (ATR pctile > 90%): 10s — highest responsiveness

    Z-score normalised against rolling history so absolute volume differences
    across sessions do not bias the signal.
    """
    def __init__(self):
        self._buy_vol:   deque = deque(maxlen=1200)
        self._sell_vol:  deque = deque(maxlen=1200)
        self._flow_hist: deque = deque(maxlen=120)
        self._last_signal = 0.0
        self._atr_pctile: float = 0.5   # updated from outside

    def set_atr_pctile(self, pctile: float) -> None:
        """Allow ATREngine to update regime context each tick."""
        self._atr_pctile = max(0.0, min(1.0, pctile))

    def _adaptive_window_sec(self) -> float:
        """Return window duration based on current ATR percentile."""
        p = self._atr_pctile
        if   p > 0.90: return 10.0
        elif p > 0.70: return 15.0
        elif p > 0.30: return 30.0
        else:          return 60.0

    def on_trade(self, price: float, qty: float, is_buyer: bool, ts: float) -> None:
        (self._buy_vol if is_buyer else self._sell_vol).append((ts, price * qty))

    def compute_signal(self) -> float:
        now    = time.time()
        cutoff = now - self._adaptive_window_sec()
        bt = sum(dv for ts, dv in self._buy_vol  if ts >= cutoff)
        st = sum(dv for ts, dv in self._sell_vol if ts >= cutoff)
        total = bt + st
        if total < 1e-10: return 0.0
        fr = (bt - st) / total
        self._flow_hist.append(fr)
        hist = list(self._flow_hist)
        if len(hist) < 10: return _sigmoid(fr * 2.0, 0.8)
        mu  = sum(hist[:-1]) / len(hist[:-1])
        std = math.sqrt(sum((x-mu)**2 for x in hist[:-1]) / max(len(hist[:-1])-1, 1))
        if std < 1e-12: return _sigmoid(fr * 2.0, 0.8)
        self._last_signal = _sigmoid((fr - mu) / std, 0.5)
        return self._last_signal

    def get_signal(self) -> float:
        return self._last_signal

# ═══════════════════════════════════════════════════════════════
# ENGINE 5: VOLUME EXHAUSTION
# ═══════════════════════════════════════════════════════════════
class VolumeExhaustionEngine:
    def __init__(self): self._last_signal = 0.0

    def compute(self, candles: List[Dict]) -> float:
        if len(candles) < 20:
            # FIX 11: update _last_signal so get_signal() returns 0.0 not stale value
            self._last_signal = 0.0
            return 0.0
        recent = candles[-10:]; earlier = candles[-20:-10]
        # v4.3 Bug 4 fix: use average close of each window instead of single endpoints
        avg_recent = sum(float(c['c']) for c in recent) / len(recent)
        avg_earlier = sum(float(c['c']) for c in earlier) / len(earlier)
        pc = avg_recent - avg_earlier
        pd = 1.0 if pc > 0 else -1.0
        rv = sum(float(c['v']) for c in recent); ev = sum(float(c['v']) for c in earlier)
        if ev < 1e-10:
            # FIX 11: zero-volume early session — reset signal, don't leave stale value
            self._last_signal = 0.0
            return 0.0
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
        period = QCfg.ADX_PERIOD()

        def _ts(c) -> int:
            try:
                return int(c['t'])
            except (KeyError, TypeError):
                pass
            try:
                return int(getattr(c, 'timestamp', 0) * 1000)
            except Exception:
                return 0

        # ADX ROOT-CAUSE FIX: dedup on the LAST CLOSED candle (candles[-2]),
        # NOT the forming candle (candles[-1]).
        #
        # candles[-1] is the live forming bar whose open-time is constant for
        # the full 5-minute bar.  The old dedup fired every tick for 5 minutes;
        # the incremental path only ran once at bar open when candles[-1] was
        # brand-new (H=L=C≈open, flat) → DM≈0, TR≈0 → Wilder smoothing just
        # decayed prior values by 13/14 → ADX appeared frozen.
        #
        # Correct contract: dedup on candles[-2]['t'] (last closed bar).
        # That timestamp changes exactly once per 5m close.
        # Incremental step uses candles[-2] vs candles[-3] (both fully formed).
        last_ts = _ts(candles[-2]) if len(candles) >= 2 else _ts(candles[-1])
        if last_ts == self._last_ts and self._seeded: return self._adx
        # BUG-6 FIX: was period*2+1. With that guard, len(candles)=period*2+1=29:
        #   closed = candles[:-1] has 28 bars -> loop produces 27 DM values
        #   inner check (len(plus_dms) < period*2) -> 27 < 28 -> True -> returns early.
        # Outer gate passed but inner seed gate failed — silent 5-min extra delay.
        # Fix: require period*2+2 so closed has period*2+1 bars -> period*2 DM values
        # -> inner check (period*2 < period*2) -> False -> seeds correctly.
        if len(candles) < period * 2 + 2: return self._adx

        if not self._seeded:
            # Seed on CLOSED bars only — exclude candles[-1] (forming).
            closed = candles[:-1]
            plus_dms: List[float] = []
            minus_dms: List[float] = []
            trs: List[float] = []
            for i in range(1, len(closed)):
                h  = float(closed[i]['h']);   l  = float(closed[i]['l'])
                ph = float(closed[i-1]['h']); pl = float(closed[i-1]['l'])
                pc = float(closed[i-1]['c'])
                up = h - ph; dn = pl - l
                plus_dms.append(up  if up  > dn and up  > 0 else 0.0)
                minus_dms.append(dn if dn  > up and dn  > 0 else 0.0)
                trs.append(max(h - l, abs(h - pc), abs(l - pc)))

            if len(plus_dms) < period * 2:
                return self._adx

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

        # Incremental: candles[-2] = just-closed bar, candles[-3] = prior closed.
        # candles[-1] (live forming bar) is intentionally excluded.
        if len(candles) < 3:
            return self._adx
        h  = float(candles[-2]['h']); l  = float(candles[-2]['l'])
        ph = float(candles[-3]['h']); pl = float(candles[-3]['l'])
        pc = float(candles[-3]['c'])
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

    def update(self, adx: 'ADXEngine', atr: 'ATREngine', htf: 'HTFTrendFilter',
               vwap_dev_atr: float = 0.0, breakout_active: bool = False,
               breakout_dir: str = "") -> 'MarketRegime':
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
        # BUG-ATR-EXPANSION-SELF-REF FIX: the old code used hist[-20:] which
        # INCLUDES hist[-1] (current ATR value) in the baseline mean.  This
        # creates a self-reference: dividing current ATR by a mean that already
        # contains it always understates true expansion (ratio drifts toward 1.0).
        # Fix: use hist[-21:-1] — the 20 bars BEFORE the current bar — as the
        # baseline, matching standard ATR-expansion calculation practice.
        hist = list(atr._atr_hist)
        expansion = 1.0
        if len(hist) >= 21:
            baseline = sum(hist[-21:-1]) / 20.0
            if baseline > 1e-10:
                expansion = hist[-1] / baseline
        elif len(hist) >= 2:
            # Insufficient history for a full 20-bar baseline — use prior bars only
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

        # ── v5.1: DI NEUTRAL OVERRIDE ─────────────────────────────────────
        # ROOT CAUSE of missed crash: ADX=42 (strong trend) but +DI/-DI spread
        # < 4pts → trend_dir="neutral" → regime stuck at TRANSITIONING.
        # During flash crashes, Wilder-smoothed DI takes several candles to
        # separate because it's an EMA of directional movement.
        #
        # Fix: when ADX >= 35 but DI is neutral, use 15m structure as the
        # fast direction indicator. 15m BOS/CHoCH updates on confirmed swings.
        # Also: ATR expansion >= 1.5× = volatility event, use 15m direction.
        _di_override_dir = trend_dir
        if trend_dir == "neutral" and adx_val >= 35.0:
            if htf.trend_15m < -0.30:
                _di_override_dir = "down"
            elif htf.trend_15m > 0.30:
                _di_override_dir = "up"
        if expansion >= 1.50 and _di_override_dir == "neutral":
            if htf.trend_15m < -0.20:
                _di_override_dir = "down"
            elif htf.trend_15m > 0.20:
                _di_override_dir = "up"

        if confidence >= 0.55 and adx_trending:
            di_up = (_di_override_dir == "up")
            if di_up and htf_up:
                regime = MarketRegime.TRENDING_UP
            elif not di_up and not htf_up:
                regime = MarketRegime.TRENDING_DOWN
            elif _di_override_dir == "up":
                regime = MarketRegime.TRENDING_UP
            elif _di_override_dir == "down":
                regime = MarketRegime.TRENDING_DOWN
            else:
                regime = MarketRegime.TRANSITIONING
        elif confidence < 0.30:
            regime = MarketRegime.RANGING
        else:
            regime = MarketRegime.TRANSITIONING

        # ── v6.0: BREAKOUT FAST-TRIGGER OVERLAY ──────────────────────────
        # ADX is a lagging indicator (EMA of directional movement). In a fresh
        # breakout, ADX takes 10-15 candles to react. By then the move is over.
        #
        # Fast-trigger: when breakout detector fires AND VWAP deviation exceeds
        # 2.0×ATR, immediately promote to TRENDING regardless of ADX.
        # This unlocks _evaluate_trend_entry within 1-2 candles of the move.
        if breakout_active and abs(vwap_dev_atr) >= 2.0:
            if breakout_dir == "up" and regime != MarketRegime.TRENDING_UP:
                regime = MarketRegime.TRENDING_UP
                confidence = max(confidence, 0.60)
                _di_override_dir = "up"
                logger.debug(
                    f"🚀 Regime FAST-TRIGGER: → TRENDING_UP "
                    f"(breakout_up + VWAP_dev={vwap_dev_atr:+.1f}ATR)")
            elif breakout_dir == "down" and regime != MarketRegime.TRENDING_DOWN:
                regime = MarketRegime.TRENDING_DOWN
                confidence = max(confidence, 0.60)
                _di_override_dir = "down"
                logger.debug(
                    f"🚀 Regime FAST-TRIGGER: → TRENDING_DOWN "
                    f"(breakout_down + VWAP_dev={vwap_dev_atr:+.1f}ATR)")

        self._regime     = regime
        self._confidence = confidence
        self._direction  = _di_override_dir
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

        # FIX 12: Removed dead no-op `(score_up if is_bullish else score_down)`.
        # Pure expression that evaluated and discarded the result — did nothing.
        if body_atr >= 1.5:
            if is_bullish: score_up   += 3
            else:          score_down += 3
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
        # v5.1: Use max(breakout ATR, current ATR) for pullback_max.
        # During crash volatility, ATR expands 2-3× but bo_atr was captured
        # pre-crash. Using stale bo_atr causes retests to invalidate at
        # 154pts vs max 138pts — missed by 16pts (actual retrace from logs).
        # max() widens the threshold with expanding vol, never shrinks it.
        _effective_atr = max(self._bo_atr, atr) if atr > 1e-10 else self._bo_atr
        pullback_max = 2.5 * _effective_atr

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
                elif isinstance(lvl, dict):
                    _px = float(lvl.get("limit_price") or lvl.get("price") or 0)
                    _qty = float(lvl.get("size") or lvl.get("quantity") or lvl.get("depth") or 0)
                    if _px > 0:
                        parsed.append((_px, _qty))
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
        Initial SL placement — v5.0 (15m structural swings only).

        NOTE: This function is called from _compute_sl_tp as Step 2 (fallback)
        when no ICT 15m OB is found. Step 1 (primary) queries get_ob_sl_level
        directly. This function provides the 15m swing structural level.

        Uses 15m swing extremes ONLY. 5m/1m swings, VWAP, HVN, OB walls all
        removed — they are not 15m ICT structure.

        Lookback: min(40, len-2) candles = up to 10 hours of 15m structure.
        Institutional swing levels persist for hours, not 3 candles.

        Called only by _compute_sl_tp. htf_only ICT OB SL takes priority.
        ATR fallback fires in _compute_sl_tp if this returns None implicitly
        (via the caller's Step 3).

        Signature kept unchanged for backward compatibility.
        """
        # v5.0: 15m structural swings only.
        # This function is Step 2 in _compute_sl_tp (fallback if no ICT OB).
        # Lookback: 40 candles = 10 hours (vs old 12 = 3 hours).
        buf_mult  = QCfg.SL_BUFFER_ATR_MULT() * (1.4 - 0.8 * min(max(atr_pctile, 0.0), 1.0))
        buffer    = buf_mult * atr
        min_dist  = max(price * QCfg.MIN_SL_PCT(), 0.40 * atr)
        max_dist  = price * QCfg.MAX_SL_PCT()

        # 15m swings — wide lookback for institutional structure
        sh_15m, sl_15m = [], []
        if candles_15m and len(candles_15m) >= 3:
            _lb = min(40, len(candles_15m) - 2)   # 10 hours of 15m candles
            sh_15m, sl_15m = InstitutionalLevels.find_swing_extremes(candles_15m, _lb)

        scored: List[Tuple[float, float, str]] = []

        def add(level: float, score: float, label: str = "") -> None:
            dist = abs(price - level)
            if dist > max_dist:
                return
            if side == "long" and level >= price:
                return
            if side == "short" and level <= price:
                return
            # 15m swings bypass min_dist — structure IS the validity criterion
            scored.append((level, score, label))

        if side == "long":
            all_lows = sl_15m
            for lvl in sl_15m:
                if lvl < price:
                    cs = InstitutionalLevels._swing_cluster_score(lvl, all_lows, atr)
                    add(lvl - buffer * 0.80, 8.0 * cs, f"15m_low@{lvl:.0f}")
        else:
            all_highs = sh_15m
            for lvl in sh_15m:
                if lvl > price:
                    cs = InstitutionalLevels._swing_cluster_score(lvl, all_highs, atr)
                    add(lvl + buffer * 0.80, 8.0 * cs, f"15m_high@{lvl:.0f}")

        if scored:
            if side == "long":
                scored.sort(key=lambda x: (-x[1], price - x[0]))
            else:
                scored.sort(key=lambda x: (-x[1], x[0] - price))
            sl = scored[0][0]
            logger.info(
                f"📌 15m Swing SL: ${sl:,.2f} [{scored[0][2]}] score={scored[0][1]:.1f} "
                f"dist={abs(price-sl):.1f}pts/{abs(price-sl)/max(atr,1e-10):.2f}ATR "
                f"| {len(scored)} 15m candidates (lb={min(40, len(candles_15m)-2) if candles_15m else 0})")
            dist = abs(price - sl)
            if dist > max_dist:
                sl = (price - max_dist) if side == "long" else (price + max_dist)
            return sl

        # No 15m structure — return None signal to caller (ATR fallback in _compute_sl_tp)
        # We return the ATR fallback here so the function signature is unchanged
        _atr_dist = max(min_dist, min(max_dist, 1.5 * atr))
        return (price - _atr_dist if side == "long" else price + _atr_dist)

    @staticmethod
    def compute_tp(price: float, side: str, atr: float, sl_price: float,
                   candles_1m: List[Dict], orderbook: Dict,
                   vwap: float, vwap_std: float,
                   candles_5m: Optional[List[Dict]] = None,
                   ict_engine=None,
                   now_ms: int = 0,
                   candles_15m: Optional[List[Dict]] = None,
                   liq_map=None) -> Optional[float]:
        """
        Initial TP placement — v7.0 INSTITUTIONAL PRIORITY.

        HIERARCHY (all candidates scored; highest wins):

          TIER-S  score ≥ 7.0  Liquidity pool (LiquidityMap)
                                The LiquidityMap has the richest multi-TF pool data.
                                BSL above (for LONG) / SSL below (for SHORT) is
                                WHERE price is magnetically attracted to — stop
                                clusters draw price like gravity. This is always
                                the PRIMARY TP in ICT methodology.

          TIER-A  score ≥ 6.0  ICT swept liquidity origin
                                After a sweep-and-reverse, price delivers back to
                                the raid origin. Mandatory when present.

          TIER-B  score ≥ 5.0  ICT structural (FVG, virgin OB, ict_engine pools)
                                Imbalances and institutional footprints in the
                                delivery direction.

          TIER-C  score ≥ 4.0  15m swing extremes
                                Confirmed structural swing levels.

          TIER-D  score ≥ 3.5  VWAP / σ-bands
                                Statistical reference levels.

          REJECT  If NO candidate survives the R:R gate → return None.
                  The caller must NOT enter this trade — no naked R-floor.

        CRITICAL: There is NO R-floor fallback. If no structural target
        exists that satisfies the minimum R:R, the trade is rejected.
        Entering without a real target is guessing, not ICT.
        """
        sl_dist = abs(price - sl_price)
        if sl_dist < 1e-10:
            return None

        _ict_now_ms  = now_ms if now_ms > 0 else int(time.time() * 1000)
        min_tp_dist  = sl_dist * QCfg.REVERSION_MIN_RR()
        max_tp_dist  = sl_dist * QCfg.REVERSION_MAX_RR()
        _min_rr_gate = QCfg.REVERSION_MIN_RR()

        # ── scored candidates pool ─────────────────────────────────────────────
        scored: List[Tuple[float, float, str]] = []   # (level, score, label)

        def _valid(level: float, min_dist: float = None) -> bool:
            dist = abs(level - price)
            lo   = min_dist if min_dist is not None else min_tp_dist
            if dist < lo or dist > max_tp_dist:
                return False
            if side == "long"  and level <= price: return False
            if side == "short" and level >= price: return False
            return True

        def add(level: float, score: float, label: str, min_dist: float = None):
            if _valid(level, min_dist):
                scored.append((level, score, label))

        # ══ TIER-S: LiquidityMap pools (primary target — richest data) ═══════
        # LiquidityMap tracks equal highs/lows across all TFs with clustering,
        # HTF confluence promotion, and proximity weighting. These are the real
        # liquidity clusters smart money hunts.
        if liq_map is not None:
            try:
                liq_snap = liq_map.get_snapshot(price, atr)
                pool_list = liq_snap.bsl_pools if side == "long" else liq_snap.ssl_pools
                for pt in pool_list:
                    pool = pt.pool
                    # Score = 7 + significance bonus + HTF confluence bonus
                    # Proximity-weighted significance ensures near pools beat far ones
                    _adj_sig = pool.proximity_adjusted_sig(pt.distance_atr)
                    _score   = 7.0 + min(_adj_sig * 0.15, 2.0)
                    # HTF confluence multiplier
                    if pool.htf_count >= 2:
                        _score += 0.5
                    # Touch count bonus — more touches = deeper stop cluster
                    _score += min(pool.touches * 0.1, 0.5)
                    # Target is just BEFORE the pool (so we don't trigger the stops,
                    # we exit into the liquidity that attracts price there)
                    _target = pool.price - 0.05 * atr if side == "long" else pool.price + 0.05 * atr
                    add(_target, _score, f"LIQ_POOL[{pool.timeframe}]@${pool.price:,.0f}(tc={pool.touches})")
            except Exception as _le:
                logger.debug(f"LiqMap TP scan error: {_le}")

        # ══ TIER-A: ICT swept liquidity origin ═══════════════════════════════
        # After a sweep-and-reverse, AMD delivery target is the most important level.
        if ict_engine is not None:
            try:
                _amd = ict_engine.get_amd_state()
                if _amd.delivery_target is not None:
                    add(_amd.delivery_target, 6.5, "AMD_DELIVERY_TARGET")
            except Exception:
                pass

        # ══ TIER-B: ICT structural targets (FVGs, OBs, ict_engine pools) ════
        if ict_engine is not None:
            try:
                _ict_min_dist = max(sl_dist * 1.0, atr * 0.5)
                _ict_targets  = ict_engine.get_structural_tp_targets(
                    side, price, atr, _ict_now_ms, _ict_min_dist, max_tp_dist,
                    htf_only=False)
                for _lvl, _sc, _lbl in _ict_targets:
                    add(_lvl, 5.0 + min(_sc * 0.1, 1.5), f"ICT_{_lbl}", _ict_min_dist)
                if _ict_targets:
                    logger.debug(
                        f"ICT TP pool: {len(_ict_targets)} candidates "
                        f"[{', '.join(f'${t[0]:,.0f}(s={t[1]:.1f})' for t in _ict_targets[:3])}]")
            except Exception as _ie:
                logger.debug(f"ICT structural TP error: {_ie}")

            # ict_engine.liquidity_pools (LiquidityLevel objects from ICT engine)
            try:
                for pool in ict_engine.liquidity_pools:
                    if pool.swept:
                        continue
                    _score = 5.5 + min(pool.touch_count * 0.25, 1.5)
                    _tf_bonus = {"1d": 1.0, "4h": 0.5, "1h": 0.25}.get(
                        getattr(pool, "timeframe", "5m"), 0.0)
                    _score += _tf_bonus
                    if side == "long" and pool.level_type == "BSL" and pool.price > price:
                        add(pool.price - 0.05 * atr, _score,
                            f"ICT_BSL@${pool.price:,.0f}",
                            max(sl_dist * 1.0, atr * 0.5))
                    elif side == "short" and pool.level_type == "SSL" and pool.price < price:
                        add(pool.price + 0.05 * atr, _score,
                            f"ICT_SSL@${pool.price:,.0f}",
                            max(sl_dist * 1.0, atr * 0.5))
            except Exception:
                pass

        # ══ TIER-C: 15m swing extremes ════════════════════════════════════════
        if candles_15m and len(candles_15m) >= 3:
            _lb   = min(40, len(candles_15m) - 2)
            sh_15, sl_15 = InstitutionalLevels.find_swing_extremes(candles_15m, _lb)
            _buf = atr * 0.08
            if side == "long":
                for sh in sh_15:
                    if sh > price + min_tp_dist:
                        add(sh - _buf, 4.0, f"15m_SWING_HIGH@${sh:,.0f}")
            else:
                for sl_v in sl_15:
                    if sl_v < price - min_tp_dist:
                        add(sl_v + _buf, 4.0, f"15m_SWING_LOW@${sl_v:,.0f}")

        # ══ TIER-D: VWAP / σ-bands ════════════════════════════════════════════
        if vwap > 0:
            if side == "long" and vwap > price:
                add(vwap, 3.5, "VWAP")
                if vwap_std > 0:
                    for mult, sc in [(0.5, 3.0), (1.0, 3.0), (1.5, 2.5)]:
                        add(vwap - mult * vwap_std, sc, f"VWAP-{mult}σ")
            elif side == "short" and vwap < price:
                add(vwap, 3.5, "VWAP")
                if vwap_std > 0:
                    for mult, sc in [(0.5, 3.0), (1.0, 3.0), (1.5, 2.5)]:
                        add(vwap + mult * vwap_std, sc, f"VWAP+{mult}σ")

        # ══ TIERED SELECTION ══════════════════════════════════════════════════
        tp = None
        if scored:
            for tier_min, tier_lbl in [
                (7.0, "LIQ_POOL"),
                (6.0, "SWEEP_ORIGIN"),
                (5.0, "ICT_STRUCTURAL"),
                (4.0, "SWING_15M"),
                (3.5, "VWAP"),
                (0.0, "BEST_AVAILABLE"),
            ]:
                tier_cands = [(lvl, sc, lb) for lvl, sc, lb in scored if sc >= tier_min]
                if not tier_cands:
                    continue
                # Score-first; nearest as tiebreaker within same score
                tier_cands.sort(key=lambda x: (-x[1], abs(x[0] - price)))
                for cand_lvl, cand_sc, cand_lb in tier_cands:
                    rr = abs(cand_lvl - price) / max(sl_dist, 1e-10)
                    if rr >= _min_rr_gate - 1e-9:
                        tp = cand_lvl
                        logger.info(
                            f"🎯 TP [{tier_lbl}] ${tp:,.2f} ({cand_lb}) "
                            f"score={cand_sc:.1f} "
                            f"dist={abs(tp-price):.1f}pts/{abs(tp-price)/max(atr,1e-10):.2f}ATR "
                            f"R:R=1:{rr:.2f} | {len(scored)} candidates total")
                        break
                    else:
                        logger.debug(
                            f"   TP candidate ${cand_lvl:,.1f} [{cand_lb}] "
                            f"score={cand_sc:.1f} R:R={rr:.2f} < {_min_rr_gate:.1f} — skip")
                if tp is not None:
                    break

        if tp is None:
            # Every structural candidate failed minimum R:R.
            # DO NOT return a naked R-floor — that is not a trade, it is gambling.
            _best = max((abs(c[0]-price)/max(sl_dist,1e-10) for c in scored), default=0.0)
            logger.info(
                f"⛔ TP: NO VALID TARGET for {side.upper()} "
                f"— {len(scored)} candidates, best R:R={_best:.2f} < {_min_rr_gate:.1f} required. "
                f"Trade rejected — no liquidity zone in reach.")
            return None

        return tp
    @staticmethod
    def compute_tp_trend(price: float, side: str, atr: float, sl_price: float,
                         candles_5m: List[Dict], orderbook: Dict,
                         swing_lookback: int = 12,
                         candles_15m: Optional[List[Dict]] = None) -> float:
        """
        Trend/momentum TP placement — v5.0 (15m ICT structure).

        Uses 15m swing extremes as structural targets. 5m swings and OB walls removed.
        In a trending market the 15m swing above/below price is the next institutional
        delivery target — the level where the prior move stalled.
        ATR channel is always included as a fallback.
        """
        sl_dist = abs(price - sl_price)
        if sl_dist < 1e-10:
            return price + atr if side == "long" else price - atr

        min_tp_dist = sl_dist * QCfg.TREND_MIN_RR()
        max_tp_dist = sl_dist * QCfg.TREND_MAX_RR()

        atr_tp_dist = atr * QCfg.TREND_TP_ATR_MULT()
        atr_tp_dist = max(atr_tp_dist, min_tp_dist)
        atr_tp_dist = min(atr_tp_dist, max_tp_dist)
        atr_tp      = (price + atr_tp_dist) if side == "long" else (price - atr_tp_dist)

        # ── 15m swing target — institutional level ahead of price ────────────
        swing_tp: Optional[float] = None
        _c15 = candles_15m if (candles_15m and len(candles_15m) >= 3) else []
        if _c15:
            _lb  = min(40, len(_c15) - 2)
            sh15, sl15 = InstitutionalLevels.find_swing_extremes(_c15, _lb)
            if side == "long" and sh15:
                valid = [h for h in sh15 if price + min_tp_dist < h <= price + max_tp_dist]
                if valid:
                    swing_tp = min(valid) - 0.08 * atr
            elif side == "short" and sl15:
                valid = [l for l in sl15 if price - max_tp_dist <= l < price - min_tp_dist]
                if valid:
                    swing_tp = max(valid) + 0.08 * atr

        candidates: List[float] = []
        if swing_tp is not None and abs(swing_tp - price) >= sl_dist * 1.5:
            candidates.append(swing_tp)
        candidates.append(atr_tp)

        if side == "long":
            valid_c = [c for c in candidates if price + min_tp_dist < c <= price + max_tp_dist]
            return max(valid_c) if valid_c else atr_tp
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
            def _ob_qty(lvl):
                if isinstance(lvl,(list,tuple)) and len(lvl)>=2: return float(lvl[1])
                if isinstance(lvl,dict): return float(lvl.get("size") or lvl.get("quantity") or 0)
                return 0.0
            bid_depth = sum(_ob_qty(b) for b in bids[:5]) if len(bids) >= 5 else 0
            ask_depth = sum(_ob_qty(a) for a in asks[:5]) if len(asks) >= 5 else 0
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
                         now_ms: int = 0,
                         hold_reason: Optional[List[str]] = None) -> Optional[float]:
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
            if hold_reason is not None:
                hold_reason.append("ATR=0")
            return None

        # ═══ PHASE 0: HANDS OFF ═══════════════════════════════════════════
        if tier < QCfg.TRAIL_BE_R():
            if hold_reason is not None:
                hold_reason.append(f"PHASE0 tier={tier:.2f}R < BE_R={QCfg.TRAIL_BE_R():.1f}R")
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

        # ═══ BREAK-EVEN UNLOCKED FLAG ════════════════════════════════════
        # Defined here — before zone freeze — to fix the v4.9 forward-reference
        # NameError that silently crashed every trail tick.
        _be_price = (entry_price + 0.3 * atr if pos_side == "long"
                     else entry_price - 0.3 * atr)
        _be_is_unlocked = (
            (pos_side == "long"  and current_sl >= _be_price) or
            (pos_side == "short" and current_sl <= _be_price)
        )

        # ═══ v5.0: FVG ZONE FREEZE (time-limited + SL-proximity-gated) ════
        # OB zone freeze removed — it permanently blocked trailing whenever
        # overlapping OBs covered the entire trade range.
        # FVG freeze kept with two hard guards:
        #   GUARD 1 — TIME: release unconditionally after 10 minutes
        #   GUARD 2 — SL PROXIMITY: only freeze when SL is within 1.5×ATR
        #             of the FVG boundary (SL must have trailed to the zone)
        _FVG_MAX_FREEZE_SEC = 600.0
        if QCfg.ICT_ZONE_FREEZE_ENABLED() and ict_engine is not None and _be_is_unlocked:
            if hold_seconds < _FVG_MAX_FREEZE_SEC:
                try:
                    _ict_now_ms = now_ms if now_ms > 0 else int(time.time() * 1000)
                    _freeze_atr  = QCfg.ICT_ZONE_FREEZE_ATR() * atr
                    _fvgs = (ict_engine.fvgs_bull if pos_side == "long"
                             else ict_engine.fvgs_bear)
                    for _fvg in _fvgs:
                        if not _fvg.is_active(_ict_now_ms):
                            continue
                        _fvg_lo = _fvg.bottom - _freeze_atr * 0.5
                        _fvg_hi = _fvg.top    + _freeze_atr * 0.5
                        if not (_fvg_lo <= price <= _fvg_hi):
                            continue
                        if pos_side == "long":
                            _sl_near_fvg = current_sl >= _fvg.bottom - 1.5 * atr
                        else:
                            _sl_near_fvg = current_sl <= _fvg.top + 1.5 * atr
                        if not _sl_near_fvg:
                            logger.debug(
                                f"Trail: FVG freeze SKIPPED — SL ${current_sl:,.0f} "
                                f"not near FVG ${_fvg.bottom:.0f}–${_fvg.top:.0f}")
                            continue
                        if hold_reason is not None:
                            hold_reason.append(
                                f"ICT_FVG_FREEZE FVG=${_fvg.bottom:.0f}-${_fvg.top:.0f}")
                        return None
                except Exception as _ict_e:
                    logger.debug(f"Trail ICT FVG zone check error (non-fatal): {_ict_e}")

                # ═══ PULLBACK DETECTION (Phases 1-3) ══════════════════════════════
        # NOTE: _be_price and _be_is_unlocked defined above in phase block.
        if QCfg.TRAIL_PULLBACK_FREEZE():
            is_pb, rev_count, pb_detail = InstitutionalLevels._classify_pullback_vs_reversal(
                pos_side, price, entry_price, atr,
                candles_1m, candles_5m, orderbook, entry_vol, peak_price_abs)
            if is_pb and _be_is_unlocked:
                # BE locked — full pullback freeze engaged
                logger.debug(
                    f"Trail: PULLBACK ({rev_count}/{QCfg.TRAIL_REV_MIN_SIGNALS()} "
                    f"reversal) BE locked — FROZEN [{pb_detail}]")
                if hold_reason is not None:
                    hold_reason.append(f"PULLBACK({rev_count}sig) [{pb_detail}]")
                return None
            elif is_pb and not _be_is_unlocked:
                # BE not yet locked — allow BE move, but log the pullback status
                logger.debug(
                    f"Trail: PULLBACK ({rev_count}/{QCfg.TRAIL_REV_MIN_SIGNALS()} rev) "
                    f"— BE not locked, allowing BE move [{pb_detail}]")

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

        # ── 3. 1m confirmed swing structure (all phases) — PRIMARY ─────────
        # v5.0: 1m closed swing lows/highs are the primary trail driver.
        # 15m ICT structure sets the SL/TP at entry. During the trade, 1m
        # structure captures the freshest institutional footprints every minute.
        if len(candles_1m) >= 6:
            closed_1m_p = candles_1m[:-1]
            sh_1m_p, sl_1m_p = InstitutionalLevels.find_swing_extremes(
                closed_1m_p, min(10, len(closed_1m_p) - 2))
            swing_buf_1m = max(0.10 * atr, QCfg.SL_BUFFER_ATR_MULT() * atr * 0.40)

            if pos_side == "long" and sl_1m_p:
                valid = [l for l in sl_1m_p
                         if current_sl + 0.05 * atr < l < price - min_dist]
                if valid:
                    best_sw = max(valid)
                    candidates.append(best_sw - swing_buf_1m)
                    logger.debug(
                        f"Trail: 1m swing low ${best_sw:,.1f} → SL ${best_sw-swing_buf_1m:,.1f}")
            elif pos_side == "short" and sh_1m_p:
                valid = [h for h in sh_1m_p
                         if price + min_dist < h < current_sl - 0.05 * atr]
                if valid:
                    best_sw = min(valid)
                    candidates.append(best_sw + swing_buf_1m)
                    logger.debug(
                        f"Trail: 1m swing high ${best_sw:,.1f} → SL ${best_sw+swing_buf_1m:,.1f}")

        # ── 4. 1m tighter micro-swing (Phase 2+) ─────────────────────────
        if phase >= 2 and len(candles_1m) >= 8:
            closed_1m    = candles_1m[:-1]
            sh_1m, sl_1m = InstitutionalLevels.find_swing_extremes(
                closed_1m, min(QCfg.TRAIL_SWING_BARS(), len(closed_1m) - 2))
            micro_buf = max(0.08 * atr, swing_buf_1m * 0.60)

            if pos_side == "long" and sl_1m:
                valid = [l for l in sl_1m if current_sl < l < price - min_dist]
                if valid:
                    best_m = max(valid)
                    candidates.append(best_m - micro_buf)
                    logger.debug(
                        f"Trail: 1m micro ${best_m:,.1f} → SL ${best_m-micro_buf:,.1f}")
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
                # v5.0: fixed N — no hold-time taper (max-hold timer removed)
                n_chandelier = QCfg.TRAIL_CHANDELIER_N_START()

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
            if hold_reason is not None:
                hold_reason.append("NO_CANDIDATES")
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
                if hold_reason is not None:
                    hold_reason.append(f"RATCHET new={new_sl:.1f} <= sl+min_move={current_sl+min_move:.1f}")
                return None
        else:
            if new_sl >= current_sl - min_move:
                if hold_reason is not None:
                    hold_reason.append(f"RATCHET new={new_sl:.1f} >= sl-min_move={current_sl-min_move:.1f}")
                return None

        return new_sl

# ═══════════════════════════════════════════════════════════════
# ICT STRUCTURE TRAIL — inline, no external dependency
# Pure ICT logic: BOS, CHoCH, OBs, FVGs, swing structure only.
# No AMD phase logic, no external file, always available.
# ═══════════════════════════════════════════════════════════════

def _ict_find_swings_inline(candles: list, lookback: int):
    """Find swing highs and lows from a candle list."""
    if len(candles) < 3 or lookback < 1:
        return [], []
    lb = min(lookback, len(candles) - 2)
    highs, lows = [], []
    def _sf(c, k):
        try: return float(c[k])
        except Exception:
            try: return float(getattr(c, k, 0.0))
            except Exception: return 0.0
    for i in range(lb, len(candles) - lb):
        h = _sf(candles[i], 'h'); l = _sf(candles[i], 'l')
        if all(h >= _sf(candles[j], 'h') for j in range(i - lb, i + lb + 1) if j != i):
            highs.append(h)
        if all(l <= _sf(candles[j], 'l') for j in range(i - lb, i + lb + 1) if j != i):
            lows.append(l)
    return highs, lows


def _ict_atr_inline(candles: list, period: int) -> float:
    """Compute ATR over last `period` bars."""
    n = min(period, len(candles))
    if n < 1:
        return 0.0
    trs = []
    for i in range(1, n + 1):
        idx = len(candles) - i
        def sf(c, k):
            try: return float(c[k])
            except Exception:
                try: return float(getattr(c, k, 0.0))
                except Exception: return 0.0
        h = sf(candles[idx], 'h'); l = sf(candles[idx], 'l')
        pc = sf(candles[idx - 1], 'c') if idx > 0 else 0.0
        tr = max(h - l, abs(h - pc), abs(l - pc)) if pc > 1e-10 else h - l
        trs.append(tr)
    return sum(trs) / len(trs) if trs else 0.0


class _ICTStructureTrail:
    """
    Institutional Chandelier-Structure Hybrid Trailing SL v2.0
    ==========================================================
    COMPLETE REWRITE — replaces the old phase-gated structure-only trail.

    DESIGN PRINCIPLES:
      1. Chandelier ATR envelope from PEAK PRICE provides continuous
         price-following (the trade breathes, but SL always tracks profit).
      2. Structural anchors (ICT OB, swing, BOS, CHoCH, FVG) SNAP the SL
         to the nearest institutional defense level when one exists.
      3. Volatility regime dynamically adjusts the ATR multiplier.
      4. Liquidity pool avoidance prevents SL placement at stop clusters.
      5. Session awareness widens during thin-liquidity sessions.

    ACTIVATION (no more dead SL):
      tier < 0.15R:  NO TRAIL — let trade establish direction
      tier >= 0.15R: Chandelier activates (wide: 3.0xATR from peak)
      tier >= 0.40R: BE floor guaranteed (fees + 0.10xATR buffer)
      tier >= 0.80R: Structural snapping begins (OB/swing anchors)
      tier >= 1.50R: Aggressive structural trail (tight to 5m/1m structure)

    CHANDELIER FORMULA:
      multiplier = max(min_mult, base_mult - decay_rate * tier)
      For LONG:  chandelier_sl = peak_price_abs - multiplier * atr
      For SHORT: chandelier_sl = peak_price_abs + multiplier * atr

      base_mult decays from 3.0 -> 0.80 as R-multiple grows 0 -> 3.0R.
      Vol regime scales: EXPANDING x1.25, CONTRACTING x0.80.
      Session scales: Asia x1.40, Off x1.30, London x1.10, NY x1.0.

    STRUCTURAL OVERRIDE:
      If an ICT OB edge or significant swing level sits BETWEEN the
      chandelier SL and current SL, AND it is in a valid protection zone,
      it overrides the chandelier — structural defense > mathematical distance.

    OUTPUT:
      new_sl = max(chandelier_sl, structural_sl, be_floor)  [LONG]
      new_sl = min(chandelier_sl, structural_sl, be_floor)  [SHORT]
      Must improve on current_sl (ratchet-only).
      Must maintain minimum distance from price (breathing room).
    """

    @staticmethod
    def _sf(c, k):
        try: return float(c[k])
        except Exception:
            try: return float(getattr(c, k, 0.0))
            except Exception: return 0.0

    # ── ICT Structure Readers ─────────────────────────────────────────────

    @staticmethod
    def _bos_count(ict_engine, pos_side: str, now_ms: int = 0) -> int:
        if ict_engine is None:
            return 0
        count = 0
        max_age_ms = 6_000_000
        for tf in ("1m", "5m", "15m"):
            try:
                st = ict_engine._tf.get(tf)
                if st is None:
                    continue
                d   = getattr(st, "bos_direction", None)
                bts = getattr(st, "bos_timestamp", 0)
                if now_ms > 0 and bts > 0 and (now_ms - bts) > max_age_ms:
                    continue
                if pos_side == "long"  and d == "bullish":
                    count += 1
                elif pos_side == "short" and d == "bearish":
                    count += 1
            except Exception:
                pass
        return count

    @staticmethod
    def _counter_bos(ict_engine, pos_side: str, now_ms: int = 0) -> bool:
        """Detect BOS AGAINST the trade direction — immediate danger signal."""
        if ict_engine is None:
            return False
        max_age_ms = 3_000_000  # 50 min — only recent counter-BOS matters
        for tf in ("5m", "15m"):
            try:
                st = ict_engine._tf.get(tf)
                if st is None:
                    continue
                d   = getattr(st, "bos_direction", None)
                bts = getattr(st, "bos_timestamp", 0)
                if now_ms > 0 and bts > 0 and (now_ms - bts) > max_age_ms:
                    continue
                if pos_side == "long"  and d == "bearish":
                    return True
                if pos_side == "short" and d == "bullish":
                    return True
            except Exception:
                pass
        return False

    @staticmethod
    def _bos_level(ict_engine, tf: str, pos_side: str):
        if ict_engine is None:
            return None
        try:
            st = ict_engine._tf.get(tf)
            if st is None:
                return None
            d = getattr(st, 'bos_direction', None)
            lvl = getattr(st, 'bos_level', 0.0)
            if not d or not lvl:
                return None
            if pos_side == "long" and d == "bullish":
                return float(lvl)
            if pos_side == "short" and d == "bearish":
                return float(lvl)
        except Exception:
            pass
        return None

    @staticmethod
    def _choch(ict_engine, pos_side: str):
        if ict_engine is None:
            return None, 0.0
        for tf in ("5m", "1m"):
            try:
                st = ict_engine._tf.get(tf)
                if st is None:
                    continue
                d = getattr(st, 'choch_direction', None)
                lvl = float(getattr(st, 'choch_level', 0.0))
                if not d or not lvl:
                    continue
                if pos_side == "long" and d == "bearish":
                    return tf, lvl
                if pos_side == "short" and d == "bullish":
                    return tf, lvl
            except Exception:
                pass
        return None, 0.0

    # ── Volatility & Session ──────────────────────────────────────────────

    @staticmethod
    def _vol_regime(candles: list, atr: float):
        if atr < 1e-10 or len(candles) < 7:
            return 1.0, "NORMAL"
        sh = _ict_atr_inline(candles, 5)
        if sh < 1e-10:
            return 1.0, "NORMAL"
        r = sh / atr
        if r > 1.25:
            return r, "EXPANDING"
        if r < 0.75:
            return r, "CONTRACTING"
        return r, "NORMAL"

    @staticmethod
    def _session_mult() -> float:
        """Session-aware trail width multiplier. Wider in thin liquidity."""
        try:
            from config import SESSION_TRAIL_WIDTH_MULT
            from datetime import datetime, timezone
            _dt  = datetime.now(timezone.utc)
            _dst = 3 <= _dt.month <= 10
            _ny  = (_dt.hour + _dt.minute / 60.0 + (-4.0 if _dst else -5.0)) % 24.0
            if   _ny >= 20.0 or _ny < 2.0:  sess = "asia"
            elif 2.0  <= _ny < 7.0:          sess = "london"
            elif 7.0  <= _ny < 11.0:         sess = "ny"
            elif 11.0 <= _ny < 16.0:         sess = "late_ny"
            else:                             sess = "off"
            return SESSION_TRAIL_WIDTH_MULT.get(sess, 1.0)
        except (ImportError, AttributeError):
            return 1.0

    # ── Chandelier Core ───────────────────────────────────────────────────

    @staticmethod
    def _chandelier_mult(tier: float, vol_ratio: float, vol_regime: str,
                         counter_bos: bool, choch: bool,
                         adx: float = 15.0, bos_cascade: int = 0,
                         amd_phase: str = "", pos_side: str = "",
                         amd_bias: str = "") -> float:
        """
        Institutional Dynamic ATR Multiplier v6.0
        ==========================================
        Chandelier envelope width is NOT a static number. It adapts to:
          - R-multiple progression (base decay)
          - Structural danger signals (counter-BOS, CHoCH)
          - Volatility regime (expanding/contracting)
          - ADX trend strength (trending markets = tighter trail)
          - Multi-TF BOS cascade (structure confirming = tighten aggressively)
          - AMD phase (manipulation = widen, delivery = tighten)
          - Session liquidity

        v6.0 ADDITIONS:
          1. ADX SCALING: High ADX (>30) = strong trend = tighter trail (0.85x).
             Ranging markets (ADX<15) get wider trail (1.15x) to avoid whipsaws.
             This is how institutional desks manage trail width — trend conviction
             determines how much room you give the trade.

          2. BOS CASCADE SCALING: When BOS fires on multiple timeframes in the
             same direction, structure is strongly confirming the move. Each
             additional TF BOS tightens by 10%. 3-TF cascade (1m+5m+15m) = 0.70x.
             This is the "structure is leading" signal — price is breaking through
             levels with confirmation, SL should follow closely.

          3. AMD PHASE SCALING:
             - MANIPULATION phase: Trail WIDENS by 1.25x. The move is the fake-out.
               Smart money is accumulating — price will retrace. Wider trail survives
               the manipulation sweep without getting stopped.
             - DISTRIBUTION/DELIVERY phase: Trail TIGHTENS by 0.85x. The real move
               is underway. SL should protect profits aggressively.
             - ACCUMULATION: neutral (1.0x) — still building the position.

          4. LOWER FLOOR: 0.40 ATR (was 0.50). Institutional desks use tighter
             stops in confirmed trending markets. 0.50 floor was too wide for
             high-ADX environments with multi-TF BOS cascade.
        """
        # Base decay: 3.0 at 0R -> 1.8 at 1R -> 1.2 at 2R -> 0.80 at 3R+
        base = max(0.80, 3.0 - 0.73 * min(tier, 3.0))

        # ── Counter-BOS: immediate tightening (cut mult by 40%) ──────────
        if counter_bos:
            base *= 0.60

        # ── CHoCH against position: moderate tightening (cut by 25%) ─────
        if choch:
            base *= 0.75

        # ── ADX trend strength scaling (v6.0) ────────────────────────────
        # ADX > 30: strong trend → trail tighter (0.85x)
        # ADX 20-30: developing trend → slight tighten (0.92x)
        # ADX < 15: ranging → widen trail (1.15x)
        if adx > 40.0:
            base *= 0.80  # very strong trend — aggressive trail
        elif adx > 30.0:
            base *= 0.85
        elif adx > 20.0:
            base *= 0.92
        elif adx < 12.0:
            base *= 1.20  # low ADX = choppy, give more room
        elif adx < 15.0:
            base *= 1.15

        # ── Multi-TF BOS cascade scaling (v6.0) ──────────────────────────
        # bos_cascade = number of timeframes where BOS aligns with trade direction
        # 1 TF: normal, 2 TF: tighten 10%, 3 TF: tighten 30%
        if bos_cascade >= 3:
            base *= 0.70
        elif bos_cascade >= 2:
            base *= 0.85
        # 0-1: no cascade adjustment

        # ── AMD phase scaling (v6.0) ─────────────────────────────────────
        _amd_phase_lower = amd_phase.lower() if amd_phase else ""
        if _amd_phase_lower in ("manipulation", "manip"):
            # Check if AMD bias aligns with position — if contra, widen more
            _amd_contra = False
            if amd_bias:
                if pos_side == "long" and amd_bias.lower() == "bearish":
                    _amd_contra = True
                elif pos_side == "short" and amd_bias.lower() == "bullish":
                    _amd_contra = True
            if _amd_contra:
                base *= 1.35  # contra manipulation — very wide trail
            else:
                base *= 1.20  # aligned manipulation — moderate widen
        elif _amd_phase_lower in ("distribution", "delivery"):
            base *= 0.85  # delivery phase — tighten, protect profits

        # ── Volatility regime scaling ────────────────────────────────────
        if vol_regime == "EXPANDING":
            base *= min(vol_ratio, 1.35)
        elif vol_regime == "CONTRACTING":
            base *= max(vol_ratio, 0.70)

        # ── Session scaling ──────────────────────────────────────────────
        base *= _ICTStructureTrail._session_mult()

        # Hard floor: 0.40 ATR (v6.0: lowered from 0.50 for institutional precision)
        return max(0.40, base)

    # ── Liquidity Avoidance ───────────────────────────────────────────────

    @staticmethod
    def _avoid_liquidity(sl: float, pos_side: str, atr: float,
                         ict_engine=None) -> float:
        """Move SL away from known liquidity pools and round numbers."""
        if atr < 1e-10:
            return sl
        zone = 0.15 * atr
        offset = 0.30 * atr
        adj = sl

        if ict_engine is not None:
            try:
                for pool in ict_engine.liquidity_pools:
                    if getattr(pool, 'swept', False):
                        continue
                    pp = float(pool.price)
                    if abs(adj - pp) < zone:
                        adj = (min(adj, pp - offset) if pos_side == "long"
                               else max(adj, pp + offset))
            except Exception:
                pass

        nearest_500 = round(adj / 500.0) * 500.0
        if abs(adj - nearest_500) < zone:
            adj = nearest_500 - offset if pos_side == "long" else nearest_500 + offset

        nearest_100 = round(adj / 100.0) * 100.0
        if abs(adj - nearest_100) < zone * 0.5:
            adj = nearest_100 - offset * 0.5 if pos_side == "long" else nearest_100 + offset * 0.5

        return adj

    # ── Structural Anchor Search ──────────────────────────────────────────

    @staticmethod
    def _find_structural_anchors(
        pos_side: str, price: float, current_sl: float,
        atr: float, tier: float,
        candles_1m: list, candles_5m: list, candles_15m: list,
        ict_engine, now_ms: int,
    ) -> list:
        """
        Find all valid structural SL anchors between current_sl and price.
        Returns list of (sl_price, label) tuples.
        v6.0: Structural anchors activate at 0.50R (was 0.80R).
        Institutional desks anchor to structure as soon as profit justifies it.
        Waiting until 0.80R meant structure formed between 0.50-0.80R was ignored,
        forcing the chandelier to be the only protection layer — exactly the
        scenario where SL gets hit during a healthy pullback to an OB.
        """
        if tier < 0.50:
            return []

        candidates = []

        # Swing buffer based on tier
        if tier >= 2.0:
            sw_buf = 0.15 * atr
        elif tier >= 1.5:
            sw_buf = 0.20 * atr
        elif tier >= 1.0:
            sw_buf = 0.30 * atr
        else:
            sw_buf = 0.40 * atr

        # ── ICT Order Block anchors ───────────────────────────────────────
        if ict_engine is not None:
            try:
                ob_buf = 0.20 * atr
                obs = (ict_engine.order_blocks_bull if pos_side == "long"
                       else ict_engine.order_blocks_bear)
                t_ms = now_ms or int(time.time() * 1000)
                active = [o for o in obs if o.is_active(t_ms)]
                added = 0
                if pos_side == "long":
                    for ob in sorted(
                            [o for o in active if o.low > current_sl],
                            key=lambda x: (x.strength, x.low), reverse=True):
                        cand = ob.low - ob_buf
                        if cand > current_sl:
                            candidates.append(
                                (cand, f"OB@${ob.midpoint:.0f}({ob.timeframe})"))
                            added += 1
                            if added >= 3:
                                break
                else:
                    for ob in sorted(
                            [o for o in active if o.high < current_sl],
                            key=lambda x: (x.strength, -x.high), reverse=True):
                        cand = ob.high + ob_buf
                        if cand < current_sl:
                            candidates.append(
                                (cand, f"OB@${ob.midpoint:.0f}({ob.timeframe})"))
                            added += 1
                            if added >= 3:
                                break
            except Exception:
                pass

        # ── BOS levels (5m, 15m) ──────────────────────────────────────────
        if ict_engine is not None:
            for tf in ("5m", "15m"):
                bos_lvl = _ICTStructureTrail._bos_level(ict_engine, tf, pos_side)
                if bos_lvl is not None:
                    bb = 0.15 * atr
                    if pos_side == "long":
                        cand = bos_lvl - bb
                        if cand > current_sl:
                            candidates.append((cand, f"BOS_{tf}@${bos_lvl:.0f}"))
                    else:
                        cand = bos_lvl + bb
                        if cand < current_sl:
                            candidates.append((cand, f"BOS_{tf}@${bos_lvl:.0f}"))

        # ── 15m swing structure (primary institutional anchor) ────────────
        if candles_15m and len(candles_15m) >= 6:
            try:
                cl15 = candles_15m[:-1] if len(candles_15m) > 1 else candles_15m
                highs_15m, lows_15m = _ict_find_swings_inline(
                    cl15, min(6, len(cl15) - 2))
                if pos_side == "long" and lows_15m:
                    for l in lows_15m:
                        cand = l - sw_buf
                        if cand > current_sl:
                            candidates.append((cand, f"15m_SW@${l:.0f}"))
                elif pos_side == "short" and highs_15m:
                    for h in highs_15m:
                        cand = h + sw_buf
                        if cand < current_sl:
                            candidates.append((cand, f"15m_SW@${h:.0f}"))
            except Exception:
                pass

        # ── 5m swing structure ────────────────────────────────────────────
        if candles_5m and len(candles_5m) >= 6:
            try:
                cl5 = candles_5m[:-1] if len(candles_5m) > 1 else candles_5m
                _5m_lb = min(6, len(cl5) - 2)
                highs_5m, lows_5m = _ict_find_swings_inline(cl5, _5m_lb)
                if pos_side == "long" and lows_5m:
                    for l in lows_5m:
                        cand = l - sw_buf
                        if cand > current_sl:
                            candidates.append((cand, f"5m_SW@${l:.0f}"))
                elif pos_side == "short" and highs_5m:
                    for h in highs_5m:
                        cand = h + sw_buf
                        if cand < current_sl:
                            candidates.append((cand, f"5m_SW@${h:.0f}"))
            except Exception:
                pass

        # ── 1m micro-structure (v6.0: activates at 1.0R, was 1.5R) ────────
        # Institutional desks use 1m structure for precision trailing once
        # the trade is established. 1.0R = SL is at breakeven, safe to tighten.
        if tier >= 1.0 and candles_1m and len(candles_1m) >= 6:
            try:
                cl1 = candles_1m[:-1] if len(candles_1m) > 1 else candles_1m
                highs_1m, lows_1m = _ict_find_swings_inline(
                    cl1, min(4, len(cl1) - 2))
                micro_buf = 0.10 * atr
                if pos_side == "long" and lows_1m:
                    for l in lows_1m:
                        cand = l - micro_buf
                        if cand > current_sl:
                            candidates.append((cand, f"1m_SW@${l:.0f}"))
                elif pos_side == "short" and highs_1m:
                    for h in highs_1m:
                        cand = h + micro_buf
                        if cand < current_sl:
                            candidates.append((cand, f"1m_SW@${h:.0f}"))
            except Exception:
                pass

        # ── CHoCH level (aggressive tighten) ──────────────────────────────
        choch_tf, choch_lvl = _ICTStructureTrail._choch(ict_engine, pos_side)
        if choch_tf is not None and choch_lvl > 0:
            cb = 0.10 * atr
            if pos_side == "long":
                cand = choch_lvl - cb
                if cand > current_sl:
                    candidates.append((cand, f"CHoCH_{choch_tf}@${choch_lvl:.0f}"))
            else:
                cand = choch_lvl + cb
                if cand < current_sl:
                    candidates.append((cand, f"CHoCH_{choch_tf}@${choch_lvl:.0f}"))

        # ── FVG fill lock ─────────────────────────────────────────────────
        if ict_engine is not None:
            try:
                fvgs = (ict_engine.fvgs_bear if pos_side == "long"
                        else ict_engine.fvgs_bull)
                t_ms = now_ms or int(time.time() * 1000)
                for fvg in fvgs:
                    if not fvg.is_active(t_ms) or fvg.fill_percentage < 0.70:
                        continue
                    lock = (fvg.top + 0.10 * atr if pos_side == "long"
                            else fvg.bottom - 0.10 * atr)
                    if pos_side == "long" and lock > current_sl:
                        candidates.append((lock, f"FVG@${fvg.midpoint:.0f}"))
                    elif pos_side == "short" and lock < current_sl:
                        candidates.append((lock, f"FVG@${fvg.midpoint:.0f}"))
            except Exception:
                pass

        return candidates

    # ── Main Compute ──────────────────────────────────────────────────────

    @staticmethod
    def compute(pos_side: str, price: float, entry_price: float,
                current_sl: float, atr: float,
                initial_sl_dist: float, peak_profit: float,
                peak_price_abs: float, hold_seconds: float,
                candles_1m: list, candles_5m: list,
                orderbook: dict, entry_vol: float, trade_mode: str,
                ict_engine=None, now_ms: int = 0,
                hold_reason=None,
                atr_percentile: float = 0.50,
                adx: float = 15.0,
                tick_size: float = 0.1,
                candles_15m: list = None,
                anchor_out: "Optional[List[str]]" = None) -> "Optional[float]":
        """
        Compute next trailing SL using Chandelier-Structure Hybrid.
        Returns new SL price or None if no improvement qualifies.
        """
        if atr < 1e-10:
            return None

        # ── Compute profit metrics ────────────────────────────────────────
        init_dist = (initial_sl_dist if initial_sl_dist > 1e-10
                     else max(abs(entry_price - current_sl), atr))
        profit = (price - entry_price) if pos_side == "long" else (entry_price - price)
        tier = max(profit, peak_profit) / init_dist if init_dist > 1e-10 else 0.0

        # ── Activation gate: 0.10R minimum (v6.0: lowered from 0.15R) ─────
        # Institutional desks begin trailing earlier to protect capital.
        # At 0.10R the chandelier is very wide (3.0×ATR) — it won't interfere
        # with the trade, but it establishes the trailing floor early.
        if tier < 0.10:
            if hold_reason is not None:
                hold_reason.append(f"TIER={tier:.2f}R<0.10R")
            return None

        # ── Break-even floor ──────────────────────────────────────────────
        try:
            from config import COMMISSION_RATE as _cr
        except Exception:
            _cr = 0.00055
        be_buf = entry_price * _cr * 2.0 + 0.10 * atr
        be_price = (entry_price + be_buf if pos_side == "long"
                    else entry_price - be_buf)

        # ── Volatility regime ─────────────────────────────────────────────
        vol_ratio, vol_regime = _ICTStructureTrail._vol_regime(candles_1m, atr)

        # ── Structure events ──────────────────────────────────────────────
        bos = _ICTStructureTrail._bos_count(ict_engine, pos_side, now_ms)
        counter_bos = _ICTStructureTrail._counter_bos(ict_engine, pos_side, now_ms)
        choch_tf, choch_lvl = _ICTStructureTrail._choch(ict_engine, pos_side)
        has_choch = choch_tf is not None

        # ── v6.0: Extract AMD phase for trail width scaling ───────────────
        _amd_phase_str = ""
        _amd_bias_str = ""
        if ict_engine is not None:
            try:
                _amd_obj = getattr(ict_engine, '_amd', None)
                if _amd_obj is not None:
                    _amd_phase_str = getattr(_amd_obj, 'phase', '') or ''
                    _amd_bias_str = getattr(_amd_obj, 'bias', '') or ''
            except Exception:
                pass

        # ── v6.0: Multi-TF BOS cascade count ──────────────────────────────
        # Count how many timeframes have BOS in the trade direction.
        # 3-TF cascade = strongest structural confirmation possible.
        _bos_cascade = bos  # _bos_count already counts aligned TFs

        # ══════════════════════════════════════════════════════════════════
        # LAYER 1: CHANDELIER ENVELOPE (always active once tier >= 0.10R)
        # v6.0: Now passes ADX, BOS cascade, and AMD phase for institutional
        # width scaling. The chandelier is the continuous price-following layer.
        # ══════════════════════════════════════════════════════════════════
        mult = _ICTStructureTrail._chandelier_mult(
            tier, vol_ratio, vol_regime, counter_bos, has_choch,
            adx=adx, bos_cascade=_bos_cascade,
            amd_phase=_amd_phase_str, pos_side=pos_side,
            amd_bias=_amd_bias_str)

        if pos_side == "long":
            chandelier_sl = peak_price_abs - mult * atr
        else:
            chandelier_sl = peak_price_abs + mult * atr

        # ══════════════════════════════════════════════════════════════════
        # LAYER 2: BE FLOOR (tier >= 0.40R)
        # ══════════════════════════════════════════════════════════════════
        be_floor = None
        if tier >= 0.40:
            be_floor = be_price

        # ══════════════════════════════════════════════════════════════════
        # LAYER 3: STRUCTURAL ANCHORS (tier >= 0.80R)
        # ══════════════════════════════════════════════════════════════════
        structural_anchors = _ICTStructureTrail._find_structural_anchors(
            pos_side, price, current_sl,
            atr, tier,
            candles_1m, candles_5m, candles_15m or [],
            ict_engine, now_ms,
        )

        # ══════════════════════════════════════════════════════════════════
        # COMBINE: Take the best (most protective) candidate
        # ══════════════════════════════════════════════════════════════════
        all_candidates = [(chandelier_sl, "CHANDELIER")]
        if be_floor is not None:
            all_candidates.append((be_floor, "BE_FLOOR"))
        all_candidates.extend(structural_anchors)

        if pos_side == "long":
            new_sl, anchor = max(all_candidates, key=lambda x: x[0])
        else:
            new_sl, anchor = min(all_candidates, key=lambda x: x[0])

        # ══════════════════════════════════════════════════════════════════
        # GUARDS: Minimum distance, liquidity avoidance, ratchet
        # ══════════════════════════════════════════════════════════════════

        # ── Minimum distance from price (breathing room) ──────────────────
        if tier >= 2.0:
            min_dist_mult = 0.60
        elif tier >= 1.5:
            min_dist_mult = 0.80
        elif tier >= 1.0:
            min_dist_mult = 1.00
        elif tier >= 0.50:
            min_dist_mult = 1.20
        else:
            min_dist_mult = 1.50

        if vol_regime == "EXPANDING":
            min_dist_mult *= min(vol_ratio, 1.30)
        elif vol_regime == "CONTRACTING":
            min_dist_mult *= max(vol_ratio, 0.70)

        # v6.0: ADX-aware minimum distance. Strong trends need tighter min_dist
        # to allow SL to follow structure closely. Ranging markets need wider.
        if adx > 35.0:
            min_dist_mult *= 0.80  # strong trend: allow tighter following
        elif adx > 25.0:
            min_dist_mult *= 0.90
        elif adx < 12.0:
            min_dist_mult *= 1.15  # choppy: more breathing room

        min_dist_mult *= _ICTStructureTrail._session_mult()
        min_dist = max(min_dist_mult * atr, 0.35 * atr)

        if pos_side == "long":
            new_sl = min(new_sl, price - min_dist)
        else:
            new_sl = max(new_sl, price + min_dist)

        # ── Liquidity avoidance ───────────────────────────────────────────
        new_sl = _ICTStructureTrail._avoid_liquidity(
            new_sl, pos_side, atr, ict_engine)

        if pos_side == "long":
            new_sl = min(new_sl, price - min_dist)
        else:
            new_sl = max(new_sl, price + min_dist)

        # ── Liquidity pool ceiling ────────────────────────────────────────
        if ict_engine is not None:
            try:
                lb = 0.35 * atr
                for pool in ict_engine.liquidity_pools:
                    if getattr(pool, 'swept', False):
                        continue
                    if pos_side == "long" and getattr(pool, 'level_type', '') == "SSL":
                        ceil = pool.price - lb
                        if current_sl < ceil < new_sl:
                            new_sl = ceil
                    elif pos_side == "short" and getattr(pool, 'level_type', '') == "BSL":
                        fl = pool.price + lb
                        if current_sl > fl > new_sl:
                            new_sl = fl
            except Exception:
                pass

        # ── Ratchet: SL may ONLY improve ──────────────────────────────────
        if pos_side == "long":
            if new_sl <= current_sl:
                if hold_reason is not None:
                    hold_reason.append(
                        f"NO_IMPROV {new_sl:.1f}<={current_sl:.1f} "
                        f"[{anchor}] tier={tier:.2f}R mult={mult:.2f}")
                return None
        else:
            if new_sl >= current_sl:
                if hold_reason is not None:
                    hold_reason.append(
                        f"NO_IMPROV {new_sl:.1f}>={current_sl:.1f} "
                        f"[{anchor}] tier={tier:.2f}R mult={mult:.2f}")
                return None

        # ── Minimum meaningful move ───────────────────────────────────────
        min_mv = 0.08 * atr
        if pos_side == "long":
            if new_sl < current_sl + min_mv:
                if hold_reason is not None:
                    hold_reason.append(
                        f"MIN_MOVE {new_sl - current_sl:.1f}<{min_mv:.1f}")
                return None
        else:
            if new_sl > current_sl - min_mv:
                if hold_reason is not None:
                    hold_reason.append(
                        f"MIN_MOVE {current_sl - new_sl:.1f}<{min_mv:.1f}")
                return None

        # ── Tick rounding ─────────────────────────────────────────────────
        ts = tick_size if tick_size > 0 else 0.1
        rounded = round(round(new_sl / ts) * ts, 10)
        # v6.0: Output the winning anchor label for caller logging
        if anchor_out is not None:
            # Build descriptive trail reason
            _tier_label = ("P3-AGGR" if tier >= 2.0 else
                          "P2-LOCK" if tier >= 1.0 else
                          "P1-BE" if tier >= 0.40 else "P0-CHAND")
            anchor_out.append(f"{anchor}|{_tier_label}|{tier:.2f}R|m={mult:.2f}")
        logger.debug(
            "ChandelierTrail %s: $%.1f->$%.1f [%s] tier=%.2fR mult=%.2f "
            "bos=%d cbos=%s choch=%s vol=%s",
            pos_side.upper(), current_sl, rounded, anchor, tier, mult,
            bos, counter_bos, has_choch, vol_regime)
        return rounded



_DYNAMIC_TRAIL_AVAILABLE = True   # inline class — always available


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

    def soft_reset(self):
        """
        Issue 1 fix: Use this instead of reset_state() after stream restart.

        Resets the seeding flag so the ATR will be fully recomputed from the
        next candle batch, but PRESERVES the last computed ATR value and history.

        Why: reset_state() sets self._atr = 0.0, which causes _compute_signals
        to return None every tick for up to 75 minutes (the 5m re-seed time).
        During this window all entry gates return None with zero logging, so the
        bot appears dead. soft_reset() keeps the last valid ATR so signals
        continue to work immediately after reconnect, while still triggering a
        proper full re-seed from the fresh candle batch.
        """
        self._seeded = False
        self._last_ts = -1
        # _atr and _atr_hist intentionally preserved

    @staticmethod
    def _pctile_rank_window() -> int:
        return int(_cfg("ATR_PCTILE_RANK_WINDOW", 30))

    def compute(self, candles: List[Dict]) -> float:
        if not candles: return self._atr
        period = QCfg.ATR_PERIOD()

        # Same closed-candle fix as ADXEngine: dedup on candles[-2] (last
        # closed bar), not candles[-1] (forming bar with partial H/L/C).
        def _ts(c) -> int:
            try:
                return int(c['t'])
            except (KeyError, TypeError):
                pass
            try:
                return int(getattr(c, 'timestamp', 0) * 1000)
            except Exception:
                return 0

        last_ts = _ts(candles[-2]) if len(candles) >= 2 else _ts(candles[-1])
        if last_ts == self._last_ts and self._seeded: return self._atr
        if len(candles) < period + 1: return self._atr

        if not self._seeded:
            # Seed on closed bars only — exclude forming candles[-1]
            closed = candles[:-1]
            trs = [max(float(closed[i]['h'])-float(closed[i]['l']),
                       abs(float(closed[i]['h'])-float(closed[i-1]['c'])),
                       abs(float(closed[i]['l'])-float(closed[i-1]['c'])))
                   for i in range(1, len(closed))]
            if len(trs) < period: return self._atr
            atr = sum(trs[:period]) / period
            for tr in trs[period:]:
                atr = (atr * (period - 1) + tr) / period
            # Only keep the final seeded ATR — prevents warmup-era volatility
            # from poisoning live percentile ranking.
            self._atr_hist.clear()
            self._atr_hist.append(atr)
            self._atr = atr; self._seeded = True
            self._last_ts = last_ts
            return self._atr
        else:
            # Incremental: candles[-2] = just-closed, candles[-3] = prior closed
            if len(candles) < 3: return self._atr
            hi  = float(candles[-2]['h'])
            lo  = float(candles[-2]['l'])
            prc = float(candles[-3]['c'])
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
    """
    HTF Trend Filter — v7.0 (ICT structure-primary, EMA fallback).

    v7.0 REWRITE — replaces naive EMA slope with ICT swing-structure scores.

    ROOT CAUSE of old veto instability:
      The EMA(8) slope was normalised by 5m ATR. A single large candle shifts
      the EMA meaningfully — the veto flipped on/off tick-by-tick in volatile
      markets, randomly blocking entries mid-setup.

    NEW APPROACH — two-layer score per timeframe:

    LAYER 1: ICT Swing Structure (primary — from ICTEngine._tf)
      Score from -1.0 to +1.0 built from:
        (a) Swing sequence: HH/HL = +1.0, LH/LL = -1.0, ranging = 0.0
        (b) BOS direction: bullish break = +0.4 bonus, bearish = -0.4 bonus
        (c) CHoCH signal: character change adds ±0.3 early warning
      This is pure price structure — it does not flip on a single candle
      because fractal swings require 2 bars on each side to confirm.

    LAYER 2: EMA slope (secondary — used ONLY when ICT not initialised)
      Same as v6 but normalised to 4h ATR equivalent for better scaling.
      Acts as a bridge during the first 10–20 minutes of warmup.

    VETO LOGIC (unchanged contract with quant_strategy):
      LONG  veto: 15m < -HTF_15M_VETO(0.35) OR (15m < -0.20 AND 4h < -0.20)
      SHORT veto: 15m > +HTF_15M_VETO(0.35) OR (15m > +0.20 AND 4h > +0.20)

    The thresholds remain the same — the INPUTS are now structurally stable.
    """
    def __init__(self):
        self._trend_15m  = 0.0
        self._trend_4h   = 0.0
        self._ict_source = False   # True when scores came from ICT structure
        # Keep last EMA series for fallback display
        self._ema_15m_raw = 0.0
        self._ema_4h_raw  = 0.0

    @staticmethod
    def _ema_series(values, period):
        if len(values) < period: return []
        k = 2.0/(period+1); ema = sum(values[:period])/period; out = [ema]
        for v in values[period:]: ema = v*k+ema*(1.0-k); out.append(ema)
        return out

    @staticmethod
    def _ict_structure_score(tf_struct, n_candles: int = 0) -> float:
        """
        Convert a TFStructure object into a [-1, +1] directional score.

        Component weights:
          Swing trend:  ±0.60  (dominant — multi-bar confirmation)
          BOS:          ±0.25  (structural break confirmation)
          CHoCH:        ±0.15  (early character-change warning — expires after
                                CHOCH_EXPIRY_BARS candles; stale CHoCH ignored)

        Ranging markets with no clear swing sequence return near-zero,
        which does NOT trigger a veto — correct behaviour since ranging
        means no strong directional bias, not a contrary signal.
        """
        if tf_struct is None:
            return 0.0
        score = 0.0
        trend = tf_struct.trend  # "bullish" | "bearish" | "ranging"
        if trend == "bullish":
            score += 0.60
        elif trend == "bearish":
            score -= 0.60
        # BOS adds conviction only in the direction of the break
        if tf_struct.bos_level > 0:
            if tf_struct.bos_direction == "bullish":
                score += 0.25
            elif tf_struct.bos_direction == "bearish":
                score -= 0.25
        # CHoCH is an early reversal signal — only apply when recent (within
        # CHOCH_EXPIRY_BARS bars).  A CHoCH from 50+ candles ago indicates
        # the trend long since resumed; applying it indefinitely softens an
        # established HTF score with stale information.
        if tf_struct.choch_level > 0 and tf_struct.choch_bar_index >= 0:
            bars_ago = max(0, (n_candles - 1) - tf_struct.choch_bar_index)
            if bars_ago <= QCfg.CHOCH_EXPIRY_BARS():
                if trend == "bullish":
                    score -= 0.15
                elif trend == "bearish":
                    score += 0.15
        # BUG-STALE-CHOCH FIX: when choch_bar_index == -1 (no bar index, e.g. old
        # serialised state) we have no way to know how stale this CHoCH is.
        # Applying it unconditionally can permanently soften an established HTF
        # score by ±0.15 — enough to flip the veto threshold on the 15m TF.
        # Resolution: skip CHoCH entirely when the bar index is unavailable.
        # The swing trend (±0.60) and BOS (±0.25) are sufficient without it.
        return max(-1.0, min(1.0, score))

    def update(self, candles_15m, candles_4h, atr_5m, ict_engine=None):
        """
        Update HTF scores. Prefers ICT structure; falls back to EMA slope.

        Args:
            candles_15m:  15-minute candle list
            candles_4h:   4-hour candle list
            atr_5m:       current 5m ATR (for EMA fallback normalisation)
            ict_engine:   ICTEngine instance (None = use EMA fallback)
        """
        # ── PRIMARY: ICT swing-structure scores ──────────────────────
        if ict_engine is not None and getattr(ict_engine, '_initialized', False):
            tf_15m = ict_engine._tf.get("15m")
            tf_4h  = ict_engine._tf.get("4h")
            self._trend_15m  = self._ict_structure_score(tf_15m, n_candles=len(candles_15m))
            self._trend_4h   = self._ict_structure_score(tf_4h,  n_candles=len(candles_4h))
            self._ict_source = True
            return

        # ── FALLBACK: EMA slope (ICT not yet initialised) ─────────────
        self._ict_source = False
        fast = QCfg.EMA_FAST()
        if len(candles_15m) > fast + 5 and atr_5m > 1e-10:
            ema15 = self._ema_series([float(c['c']) for c in candles_15m], fast)
            if len(ema15) >= 4:
                raw = (ema15[-1] - ema15[-3]) / atr_5m
                self._ema_15m_raw = raw
                self._trend_15m   = _sigmoid(raw, 0.8)
            else:
                self._trend_15m = 0.0
        else:
            self._trend_15m = 0.0

        slow = QCfg.EMA_SLOW()
        if len(candles_4h) > slow + 3 and atr_5m > 1e-10:
            ema4h = self._ema_series([float(c['c']) for c in candles_4h], slow)
            if len(ema4h) >= 3:
                raw = (ema4h[-1] - ema4h[-2]) / (atr_5m * 4.0)
                self._ema_4h_raw = raw
                self._trend_4h   = _sigmoid(raw, 0.8)
            else:
                self._trend_4h = 0.0
        else:
            self._trend_4h = 0.0

    def vetoes_trade(self, side: str) -> bool:
        """
        Per-timeframe HTF veto.

        LONG  veto: 15m < -HTF_15M_VETO OR (15m < -HTF_BOTH AND 4h < -HTF_BOTH)
        SHORT veto: 15m > +HTF_15M_VETO OR (15m > +HTF_BOTH AND 4h > +HTF_BOTH)

        Thresholds unchanged — only the inputs are now structure-stable.
        """
        if not QCfg.HTF_ENABLED():
            return False
        t15       = self._trend_15m
        t4h       = self._trend_4h
        veto_15m  = float(_cfg("QUANT_HTF_15M_VETO",  0.35))
        veto_both = float(_cfg("QUANT_HTF_BOTH_VETO", 0.20))

        if side == "long":
            if t15 < -veto_15m:
                return True
            if t15 < -veto_both and t4h < -veto_both:
                return True
        elif side == "short":
            if t15 > veto_15m:
                return True
            if t15 > veto_both and t4h > veto_both:
                return True
        return False

    @property
    def trend_15m(self) -> float:
        return self._trend_15m
    @property
    def trend_4h(self) -> float:
        return self._trend_4h
    @property
    def ict_source(self) -> bool:
        """True if scores came from ICT structure; False if EMA fallback."""
        return self._ict_source


# ═══════════════════════════════════════════════════════════════
# WEIGHT SCHEDULER — regime-adaptive signal weights
# ═══════════════════════════════════════════════════════════════
class WeightScheduler:
    """
    Regime-adaptive signal weight scheduler — v7.0.

    Signal mix that maximises edge varies by market regime:

    RANGING:
      VWAP deviation is the dominant edge — price reverts reliably to VWAP.
      OrderBook imbalance provides tight structural validation.
      CVD divergence confirms exhaustion at extremes.
      W: VWAP=0.40, OB=0.25, CVD=0.20, TICK=0.10, VEX=0.05

    TRANSITIONING:
      Equal weighting reflects uncertainty. VWAP and CVD both matter.
      W: VWAP=0.30, CVD=0.25, OB=0.20, TICK=0.15, VEX=0.10

    TRENDING_UP / TRENDING_DOWN:
      CVD trend signal and tick flow dominate — they show active directional
      participation. VWAP is lagging (price is above/below it by design).
      W: CVD=0.35, TICK=0.25, OB=0.20, VWAP=0.15, VEX=0.05

    BREAKOUT / HIGH_VOLATILITY:
      Tick flow is most real-time. CVD confirms direction.
      OB wall detection matters more than VWAP (now far below/above).
      W: TICK=0.35, CVD=0.30, OB=0.20, VWAP=0.10, VEX=0.05
    """

    # (W_VWAP, W_CVD, W_OB, W_TICK, W_VEX)
    _WEIGHTS = {
        MarketRegime.RANGING:       (0.40, 0.20, 0.25, 0.10, 0.05),
        MarketRegime.TRANSITIONING: (0.30, 0.25, 0.20, 0.15, 0.10),
        MarketRegime.TRENDING_UP:   (0.15, 0.35, 0.20, 0.25, 0.05),
        MarketRegime.TRENDING_DOWN: (0.15, 0.35, 0.20, 0.25, 0.05),
    }

    @classmethod
    def get(cls, regime: MarketRegime) -> Tuple[float, float, float, float, float]:
        """
        Return (w_vwap, w_cvd, w_ob, w_tick, w_vex) for the given regime.
        Falls back to config-static values if the regime is unrecognised.
        """
        if regime in cls._WEIGHTS:
            return cls._WEIGHTS[regime]
        # Static config fallback (backward compat)
        return (
            QCfg.W_VWAP_DEV(),
            QCfg.W_CVD_DIV(),
            QCfg.W_OB(),
            QCfg.W_TICK_FLOW(),
            QCfg.W_VOL_EXHAUSTION(),
        )

    @classmethod
    def log_weights(cls, regime: MarketRegime) -> str:
        """Human-readable weight string for logging."""
        w = cls.get(regime)
        return (f"VWAP={w[0]:.2f} CVD={w[1]:.2f} OB={w[2]:.2f} "
                f"TICK={w[3]:.2f} VEX={w[4]:.2f}")


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
    # v4.8: ICT/SMC structural confluence (component scores)
    ict_ob: float = 0.0            # PD array score (OB proximity)
    ict_fvg: float = 0.0           # FVG score
    ict_sweep: float = 0.0         # Liquidity score
    ict_session: float = 0.0       # Session/KZ score
    ict_total: float = 0.0         # Total ICT confluence 0-1
    ict_details: str = ""          # Human-readable detail string
    ict_boost_signed: float = 0.0  # Signed composite contribution (+/-); B1 fix
    ict_direction: float = 0.0     # +1.0=long / -1.0=short resolved ICT side; B1 fix
    # v6.0: AMD phase + MTF context
    amd_phase: str = "ACCUMULATION"  # AMD cycle phase
    amd_bias: str = "neutral"         # AMD directional bias
    amd_conf: float = 0.0             # AMD confidence 0-1
    mtf_aligned: bool = False         # True if ≥3 of 4 major TFs agree
    in_discount: bool = False         # Price in 4H discount zone (<40% PD)
    in_premium:  bool = False         # Price in 4H premium zone (>60% PD)
    mtf_details: str = ""             # MTF structure summary
    # v7.0: Regime-adaptive weights applied (for trade attribution logging)
    w_vwap: float = 0.30; w_cvd: float = 0.25; w_ob: float = 0.20
    w_tick: float = 0.15; w_vex: float = 0.10
    # v7.0: ICT entry tier that was active at entry (for signal attribution)
    ict_entry_tier: str = ""          # "S" | "A" | "B" | "" if no ICT gate
    htf_ict_source: bool = False      # True if HTF score came from ICT structure
    cvd_tick_count: int = 0           # Number of real trade ticks in CVD accumulator
    # v8.0: Advanced ICT delivery + structural context
    ict_delivery_target:     float = 0.0   # AMD delivery target price (0 = none)
    ict_delivery_conf:       float = 0.0   # 0-1 confidence reaching delivery target
    ict_pd_grade:            str   = "EQ"  # "PREMIUM"|"EQ"|"DISCOUNT" on 4H
    ict_pd_matrix:           str   = ""    # e.g. "1D:DISC 4H:DISC 1H:EQ 15M:PREM"
    ict_htf_reversal_risk:   float = 0.0   # 0-1 risk of HTF zone opposing trade
    ict_mtf_ob_count:        int   = 0     # TFs with active OB at current price
    ict_fvg_stack_count:     int   = 0     # TFs with unfilled FVG at current price
    ict_judas_active:        bool  = False # True if in Judas swing territory
    ict_nearest_bsl_atr:     float = 0.0   # Nearest BSL above price in ATR
    ict_nearest_ssl_atr:     float = 0.0   # Nearest SSL below price in ATR
    ict_session_entry_q:     str   = ""    # "HIGH"|"MEDIUM"|"LOW"|"AVOID"
    ict_chain_score:         float = 0.0   # delivery profile chain conviction 0-1
    ict_htf_rev_zone_near:   bool  = False # True if within 1 ATR of HTF reversal zone

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
    entry_fee_paid: float = 0.0    # v8.1: exact paid_commission from Delta entry order (0 = use estimate)
    trail_override: Optional[bool] = None  # v4.3: None=use config, True=force on, False=force off
    hold_extensions: int = 0  # v4.6: how many times max-hold has been extended
    consecutive_trail_holds: int = 0  # v5.1: structural trail tracking
    be_ratchet_applied: bool = False  # v5.1: counter-BOS BE already forced
    last_ratchet_r: float = 0.0      # v6.1: last R-level ratcheted (prevents re-fire)
    ict_entry_tier: str = ""  # v7.0: "S" | "A" | "B" | "" — ICT confluence tier at entry
    # FIX 8: store actual HTF scores at entry time for post-trade attribution.
    # Previously deviation_atr was stored under "htf_15m" key — all HTF analytics were wrong.
    entry_htf_15m: float = 0.0
    entry_htf_4h:  float = 0.0

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
    # BUG-TZ FIX: Trading day boundary must be midnight IST (UTC+5:30), not
    # midnight UTC.  date.today() on a cloud server (UTC) flips at midnight
    # UTC = 05:30 IST.  A trade opening at 05:29 IST has record_trade_start()
    # increment day N; if it closes at 05:31 IST, _reset_if_new_day() fires
    # first (UTC midnight passed), zeroes _daily_pnl, then record_trade_result()
    # adds PnL to day N+1.  Day N: trades=1 pnl=0. Day N+1: trades=0 pnl=+X.
    # The daily loss cap is also corrupted.  Fix: IST-aware date comparison.
    _IST = timezone(timedelta(hours=5, minutes=30))

    @staticmethod
    def _today_ist() -> date:
        return datetime.now(DailyRiskGate._IST).date()

    def __init__(self):
        self._today = self._today_ist(); self._daily_trades = 0; self._consec_losses = 0
        self._daily_pnl = 0.0; self._daily_open_bal = 0.0
        self._loss_lockout_until = 0.0; self._lock = threading.Lock()

    def _reset_if_new_day(self):
        today = self._today_ist()
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
            # FIX 2: Reset consec_losses when lockout expires so the bot can
            # actually trade again.  Without this the lockout re-arms on every
            # call after expiry because consec_losses is still ≥ MAX — infinite loop.
            elif self._loss_lockout_until > 0 and now >= self._loss_lockout_until:
                self._consec_losses = 0
                self._loss_lockout_until = 0.0
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

    def undo_trade_start(self):
        """
        Reverse a record_trade_start() when an entry is aborted before any
        order reaches the exchange (TP-gate rejection, fee-gate, SL sanity).
        Prevents aborted entries from consuming the daily trade cap.
        """
        with self._lock:
            if self._daily_trades > 0:
                self._daily_trades -= 1

    def record_trade_result(self, pnl):
        with self._lock:
            self._daily_pnl += pnl
            if pnl < 0: self._consec_losses += 1
            else: self._consec_losses = 0

    def force_reset(self, reset_consec: bool = True, reset_daily: bool = False) -> str:
        """
        Manual override reset — callable from Telegram /resetrisk.

        reset_consec: clears consecutive_losses + loss_lockout (default True)
        reset_daily:  also clears daily_pnl + daily_trades counter (opt-in only)

        Returns a human-readable summary of what was cleared.
        """
        with self._lock:
            parts = []
            if reset_consec:
                prev_cl = self._consec_losses
                prev_lo = self._loss_lockout_until
                self._consec_losses       = 0
                self._loss_lockout_until  = 0.0
                parts.append(f"consec_losses {prev_cl}→0")
                if prev_lo > 0:
                    import time as _t
                    remaining = max(0, int(prev_lo - _t.time()))
                    parts.append(f"lockout cleared ({remaining}s was remaining)")
            if reset_daily:
                prev_dt  = self._daily_trades
                prev_dp  = self._daily_pnl
                self._daily_trades = 0
                self._daily_pnl    = 0.0
                parts.append(f"daily_trades {prev_dt}→0")
                parts.append(f"daily_pnl ${prev_dp:+.2f}→$0.00")
            return "; ".join(parts) if parts else "nothing to reset"

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
        # v5.0: ICT Sweep-and-Go institutional engine
        self._sweep_detector: Optional[object] = (
            ICTSweepDetector() if _ICT_TRADE_ENGINE_AVAILABLE else None)
        self._active_sweep_setup: Optional[object] = None  # ICTSweepSetup | None
        self._last_sweep_log = 0.0   # throttle sweep-status log spam
        self._pending_hunt_signal = None   # hunt signal for _compute_sl_tp (Tier-L path)
        self._confirm_trend_long = 0; self._confirm_trend_short = 0
        self._confirm_flow_long = 0; self._confirm_flow_short = 0
        self._last_flow_entry_time = 0.0
        self._flow_tick_streak = 0       # consecutive extreme tick flow readings
        self._flow_tick_direction = ""   # "long" | "short" | ""
        self._pos = PositionState(); self._last_sig = SignalBreakdown()
        self._risk_gate = DailyRiskGate()
        self._confirm_long = 0; self._confirm_short = 0
        # v5.2 Bug-C fix: dedicated confirm counters for hunt entries.
        # These are intentionally separate from _confirm_long/_confirm_short so
        # that ICT OTE routing (which resets the general counters every tick it
        # fires) cannot wipe the hunt's 2-tick confirmation progress.
        # They are only reset when: (a) a hunt entry fires, (b) the hunt signal
        # is None / expired (checked in the routing block each tick), or
        # (c) _enter_trade() resets all state after a confirmed fill.
        self._confirm_hunt_long = 0; self._confirm_hunt_short = 0
        self._last_eval_time = 0.0; self._last_exit_time = 0.0
        self._last_momentum_attempt = 0.0   # cooldown after any retest attempt (pass or fail)
        self._last_tp_gate_rejection = 0.0  # tracks last TP gate rejection time
        self._tp_gate_rejection_mode = ""   # "reversion" | "momentum" | "trend" for per-mode logging
        self._last_pos_sync = 0.0; self._last_exit_sync = 0.0; self._exiting_since = 0.0
        self._entering_since = 0.0  # timestamp when ENTERING phase started (watchdog)
        # Concurrency guards: position sync runs in a background thread so the
        # main loop (trail, heartbeat, signals) is never blocked waiting for REST.
        self._pos_sync_in_progress  = False   # ACTIVE sync thread running
        self._exit_sync_in_progress = False   # EXITING sync thread running
        self._trail_in_progress     = False   # trail REST call running in background
        self._last_exit_side = ""; self._last_think_log = 0.0; self._think_interval = 30.0
        self._last_fed_trade_ts = 0.0

        self._last_pd_gate_log: dict = {}  # throttle P/D zone gate log
        self._last_reconcile_time = 0.0; self._RECONCILE_SEC = 30.0
        self._reconcile_pending = False; self._reconcile_data = None
        self._total_trades = 0; self._winning_trades = 0; self._total_pnl = 0.0
        self._trade_history: List[Dict] = []   # persistent per-session trade log
        self.current_sl_price = 0.0; self.current_tp_price = 0.0
        # DUPLICATE P&L GUARD v2: two-layer protection.
        #
        # Layer 1: _exit_completed (bool) — set True the moment ANY exit path
        #   finishes recording PnL.  Checked at the TOP of _record_exchange_exit()
        #   BEFORE any telegram sends.  Never reset until a new position opens.
        #   This prevents both double-counting AND double-reporting.
        #
        # Layer 2: _pnl_recorded_for (float) — stores the entry_time of the
        #   position whose close has been recorded.  Checked inside _record_pnl()
        #   as a secondary guard.  NOT reset in _finalise_exit() — only reset
        #   when a new position enters ACTIVE phase.
        self._exit_completed: bool = False
        self._pnl_recorded_for: float = 0.0
        # Track last known price for PnL fallback
        self._last_known_price = 0.0
        # Bug 6 fix: pre-declare all hasattr-guarded attrs so the very first
        # evaluation tick behaves identically to every subsequent one.
        self._last_data_warn       = 0.0
        self._last_atr_warn        = 0.0
        self._last_price_warn      = 0.0
        self._last_bo_block_log    = 0.0
        self._last_ict_gate_log    = 0.0
        self._last_trail_block_log = 0.0
        # v6.0: Structure-event-driven trail variables
        self._last_trail_check_price    = 0.0   # price at last trail computation
        self._last_trail_rest_time      = 0.0   # timestamp of last successful trail REST move
        self._last_structure_fingerprint = None  # structural state fingerprint for change detection
        self._last_maxhold_check   = 0.0
        # Bug-3 fix: path-specific ICT gate block timers.
        # The original single shared pair (_ict_gate_start_time / _ict_gate_alerted)
        # was used by reversion, momentum, and trend paths simultaneously.
        # When the reversion path passed and reset the timer, the momentum path's
        # accumulated block time was silently lost, preventing the 15-min Telegram
        # alert from ever firing when the bot was stuck for hours.
        self._ict_gate_start_time_rev   = 0.0   # reversion path
        self._ict_gate_alerted_rev      = False
        self._ict_gate_start_time_mom   = 0.0   # momentum retest path
        self._ict_gate_alerted_mom      = False
        self._ict_gate_start_time_trend = 0.0   # trend pullback path
        self._ict_gate_alerted_trend    = False

        # -- v9.0: New liquidity-first engines --
        self._liq_map = LiquidityMap() if _LIQ_MAP_AVAILABLE else None
        self._entry_engine = EntryEngine() if _ENTRY_ENGINE_AVAILABLE else None
        self._ict_trail = ICTTrailManager() if _ENTRY_ENGINE_AVAILABLE else None
        self._flow_streak_dir_v2 = ""
        self._flow_streak_count_v2 = 0
        # BUG-FIX-3: These attrs are read by main.py heartbeat via getattr().
        # Without explicit assignment they're missing → heartbeat always shows
        # "Flow: neutral(+0.00)" regardless of actual order-flow state.
        self._flow_conviction: float = 0.0
        self._flow_direction:  str   = ""
        self._last_think_log_v2 = 0.0
        self._force_sl = None
        self._force_tp = None

        self._log_init()

    def _log_init(self):
        logger.info("=" * 72)
        logger.info("⚡ QuantStrategy v9.0 — LIQUIDITY-FIRST")
        logger.info(f"   {QCfg.SYMBOL()} | {QCfg.LEVERAGE()}x | {QCfg.MARGIN_PCT():.0%} margin")
        # Entry engine
        entry_status = "ACTIVE (LiquidityMap → FlowDetector → EntryEngine)" if _ENTRY_ENGINE_AVAILABLE else "LEGACY (entry_engine.py not found)"
        logger.info(f"   Entry Engine: {entry_status}")
        # Liquidity map
        liq_status = "ACTIVE (multi-TF pool scanner, pool priority, sweep detection)" if _LIQ_MAP_AVAILABLE else "UNAVAILABLE (liquidity_map.py not found)"
        logger.info(f"   LiquidityMap: {liq_status}")
        # ICT engine
        ict_status = "DISABLED (ict_engine.py not found)"
        if self._ict:
            ict_status = (
                f"ENABLED | ZoneFreeze={QCfg.ICT_ZONE_FREEZE_ENABLED()} "
                f"OBanchor={QCfg.ICT_OB_SL_ANCHOR()} "
                f"LiqCeiling={QCfg.ICT_LIQ_CEILING_ENABLED()}"
            )
        logger.info(f"   ICT Engine: {ict_status}")
        # Trail
        trail_status = "_ICTStructureTrail (inline ICT BOS/CHoCH/OB/FVG engine)"
        logger.info(f"   Trail: {trail_status}")
        logger.info("=" * 72)

    def get_position(self) -> Optional[Dict]:
        with self._lock: return None if self._pos.is_flat() else self._pos.to_dict()

    def on_stream_restart(self):
        """
        Issue 1 fix: Called by data_manager after restart_streams().
        Resets all engine timestamps so they reprocess warmup data.

        ATR engines now use soft_reset() instead of reset_state().
        reset_state() zeroed self._atr = 0.0 which caused _compute_signals
        to return None for up to 75 minutes (5m re-seed window) with zero
        log output — the bot appeared completely dead after every reconnect.
        soft_reset() preserves the last valid ATR value so signals continue
        working immediately while the engine re-seeds from fresh candles.
        """
        with self._lock:
            self._cvd.reset_state()
            self._atr_1m.soft_reset()   # preserves ATR value; re-seeds from next batch
            self._atr_5m.soft_reset()   # same — avoids 75-min silence after reconnect
            self._adx.reset_state()
            self._breakout.reset_state()
            if self._ict: self._ict.reset_state()
            if self._sweep_detector is not None:
                self._sweep_detector.invalidate()
            self._active_sweep_setup = None
            self._pending_hunt_signal = None
            logger.info("♻️ Strategy engines soft-reset after stream restart (ATR values preserved)")

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

    def _get_quant_helpers(self, sig, side: str) -> Optional['QuantHelperSignals']:
        """
        Build a QuantHelperSignals snapshot from the current SignalBreakdown.

        This packages the quant signals that ICTEntryGate uses as SOFT HELPERS
        (not hard gates) when evaluating ICT sweep entries. For standard Tier-B
        entries, these signals become required gates within ICTEntryGate itself.

        Returns None if _ICT_TRADE_ENGINE_AVAILABLE is False.
        """
        if not _ICT_TRADE_ENGINE_AVAILABLE:
            return None
        try:
            # BUG-HTF-VETO-DIRECTION FIX: sig.htf_veto is computed at signal time
            # for self._vwap.reversion_side(price) — the VWAP direction.  When
            # side is a sweep direction (opposite of VWAP), passing sig.htf_veto
            # gives the wrong direction's veto to ICTEntryGate. Recompute for
            # the actual entry side so the gate sees the correct structural opposition.
            _htf_veto_for_side = self._htf.vetoes_trade(side)
            return QuantHelperSignals(
                tick_flow    = self._tick_eng.get_signal(),
                cvd_trend    = self._cvd.get_trend_signal(),
                vwap_dev     = sig.deviation_atr,
                n_confirming = sig.n_confirming,
                composite    = sig.composite,
                regime_ok    = sig.regime_ok,
                htf_veto     = _htf_veto_for_side,
                adx          = sig.adx,
                overextended = sig.overextended,
                htf_15m      = self._htf.trend_15m,
                htf_4h       = self._htf.trend_4h,
            )
        except Exception:
            return None

    def on_tick(self, data_manager, order_manager, risk_manager, timestamp_ms: int) -> None:
        # ── Bug 1 fix: locked section is non-blocking — only state reads/writes.
        # All exchange API calls (_sync_position, _evaluate_entry, _manage_active,
        # _finalise_exit) happen AFTER the lock is released so trailing-SL
        # replace_stop_loss, bracket fill polls, and reconcile writes can never
        # freeze each other or the health-check thread.
        now = timestamp_ms / 1000.0
        with self._lock:
            self._om = order_manager
            if now - self._last_eval_time < QCfg.TICK_EVAL_SEC():
                return
            self._last_eval_time = now

            # Local data feeds — all in-process reads, no I/O
            self._feed_microstructure(data_manager)
            try:
                ob = data_manager.get_orderbook()
                price = data_manager.get_last_price()
                if self._fee_engine is not None:
                    self._fee_engine.update_orderbook(ob, price)
            except Exception:
                pass
            try:
                p = data_manager.get_last_price()
                if p > 1.0:
                    self._last_known_price = p
            except Exception:
                pass

            # Apply any pending reconcile result (written by background thread)
            if self._reconcile_data is not None:
                _rdata = self._reconcile_data; self._reconcile_data = None
                self._reconcile_apply(order_manager, _rdata)

            # Spawn reconcile background thread if due (non-blocking)
            if not self._reconcile_pending and now - self._last_reconcile_time >= self._RECONCILE_SEC:
                self._last_reconcile_time = now; self._reconcile_pending = True
                threading.Thread(
                    target=self._reconcile_query_thread,
                    args=(order_manager,), daemon=True,
                ).start()

            # Snapshot all decision-relevant state while locked
            phase             = self._pos.phase
            need_pos_sync     = (phase == PositionPhase.ACTIVE  and now - self._last_pos_sync  > QCfg.POS_SYNC_SEC())
            need_exit_sync    = (phase == PositionPhase.EXITING and now - self._last_exit_sync > QCfg.POS_SYNC_SEC())
            exiting_stuck     = (phase == PositionPhase.EXITING and (now - self._exiting_since) > 120.0)
            cooldown_ok       = (now - self._last_exit_time >= float(QCfg.COOLDOWN_SEC()))

        # ── All blocking exchange I/O below — lock is NOT held ───────────────────

        if phase == PositionPhase.ACTIVE:
            if need_pos_sync and not self._pos_sync_in_progress:
                # Dispatch position sync to a background thread.
                # _sync_position calls get_open_position() → Delta REST with a 30s timeout.
                # Running it in the main thread blocks on_tick, trail management, and the
                # heartbeat for up to 30s every 30s (100% duty cycle = permanently frozen).
                self._pos_sync_in_progress = True
                with self._lock:
                    self._last_pos_sync = now   # stamp immediately so we don't re-trigger

                def _bg_sync_active(om=order_manager):
                    try:
                        self._sync_position(om)
                    except Exception as _e:
                        logger.error("_sync_position (ACTIVE) error: %s", _e, exc_info=True)
                    finally:
                        self._pos_sync_in_progress = False

                threading.Thread(target=_bg_sync_active, daemon=True,
                                 name="pos-sync-active").start()

            self._manage_active(data_manager, order_manager, now)

        elif phase == PositionPhase.EXITING:
            if need_exit_sync and not self._exit_sync_in_progress:
                self._exit_sync_in_progress = True
                with self._lock:
                    self._last_exit_sync = now

                def _bg_sync_exit(om=order_manager):
                    try:
                        self._sync_position(om)
                    except Exception as _e:
                        logger.error("_sync_position (EXITING) error: %s", _e, exc_info=True)
                    finally:
                        self._exit_sync_in_progress = False

                threading.Thread(target=_bg_sync_exit, daemon=True,
                                 name="pos-sync-exit").start()
            if exiting_stuck:
                # v8.0: check if exit was already completed by sync/reconcile thread
                if self._exit_completed:
                    logger.info("EXITING stuck >120s but exit already completed — finalising")
                    with self._lock:
                        self._finalise_exit()
                else:
                    logger.warning("⚠️ EXITING stuck >120s — recording PnL then force-finalising")
                    send_telegram_message(
                        "⚠️ <b>EXITING TIMEOUT</b>\n"
                        "Stuck in EXITING phase for >120s.\n"
                        "Recording PnL=0 (unconfirmed) and resetting to FLAT.\n"
                        "<b>Check exchange for open position!</b>")
                    self._record_pnl(0.0, exit_reason="exiting_timeout", exit_price=0.0,
                                     fee_breakdown=None)
                    with self._lock:
                        self._finalise_exit()

        elif phase == PositionPhase.ENTERING:
            # Bracket fill is being polled by a background thread.
            # This phase blocks re-entry on every tick until fill confirmed (→ACTIVE)
            # or the entry aborts (→FLAT via finally in _launch_entry_async).
            # Safety watchdog: force FLAT if background thread dies silently.
            _entry_timeout = float(getattr(config, 'LIMIT_ORDER_FILL_TIMEOUT_SEC', 120.0))
            if (now - self._entering_since) > (_entry_timeout + 30.0):
                with self._lock:
                    if self._pos.phase == PositionPhase.ENTERING:
                        logger.warning(
                            "⚠️ ENTERING watchdog: >150s without fill confirmation "
                            "— forcing FLAT (check exchange for orphaned position)")
                        send_telegram_message(
                            "⚠️ <b>ENTERING TIMEOUT</b>\n"
                            "Bracket order fill not confirmed after 150s.\n"
                            "State reset to FLAT.\n"
                            "<b>Check exchange for open position!</b>")
                        self._pos.phase = PositionPhase.FLAT
                        self._last_exit_time = now
                        # CRITICAL: Reset entry engine too — otherwise it stays
                        # stuck in EngineState.ENTERING forever and the bot
                        # cannot generate new signals (brain-dead state).
                        if self._entry_engine is not None:
                            self._entry_engine.on_entry_failed()
                            logger.info("🔄 Entry engine reset to SCANNING after watchdog")
                        # CRITICAL: Reset entry engine state machine too.
                        # Without this, the entry engine stays in ENTERING
                        # forever — update() skips all processing, no new
                        # signals can be produced, and the bot is paralyzed.
                        if self._entry_engine is not None:
                            self._entry_engine.on_entry_cancelled()

        elif phase == PositionPhase.FLAT:
            if cooldown_ok:
                self._evaluate_entry(data_manager, order_manager, risk_manager, now)

    def _launch_entry_async(self, data_manager, order_manager, risk_manager,
                             side: str, sig, mode: str,
                             ict_tier: str = "") -> None:
        """
        Non-blocking entry: sets ENTERING phase immediately, then runs
        _enter_trade in a daemon thread so the main on_tick loop is never
        blocked by the bracket fill-polling sleep loop (up to 45s).

        ict_tier: "S" | "A" | "B" | "" — passed through to _enter_trade so
        confidence-weighted position sizing can scale size by conviction tier.

        The try/finally guarantees any abort path inside _enter_trade
        (TP gate rejection, SL failure, partial-fill abort, etc.) resets phase
        to FLAT so entry evaluation resumes after cooldown.
        """
        with self._lock:
            self._pos.phase      = PositionPhase.ENTERING
            self._entering_since = time.time()

        _dm, _om, _rm = data_manager, order_manager, risk_manager

        def _bg():
            try:
                self._enter_trade(_dm, _om, _rm, side, sig, mode=mode,
                                  ict_tier=ict_tier)
            except Exception as _e:
                logger.error(
                    f"_enter_trade background thread error ({mode}/{side}): {_e}",
                    exc_info=True)
            finally:
                with self._lock:
                    if self._pos.phase == PositionPhase.ENTERING:
                        # Distinguish pre-trade gate rejection (no order placed)
                        # from a real order-level failure (order placed but aborted).
                        #
                        # Gate rejections (TP R:R, SL/TP sanity, fee floor) set
                        # _last_tp_gate_rejection right before returning from
                        # _enter_trade.  If that timestamp is within the last 5s
                        # we know no order was ever sent — do NOT engage the
                        # cooldown.  Signals resume immediately on the next tick.
                        #
                        # Real failures (exchange error, partial fill abort, etc.)
                        # do not touch _last_tp_gate_rejection, so the gate will
                        # be more than 5s old → full cooldown applies as before.
                        _gate_reject = (time.time() - self._last_tp_gate_rejection) < 5.0
                        if _gate_reject:
                            logger.info(
                                f"⚪ Entry gate rejected (mode={mode} side={side}) "
                                f"— resetting to FLAT, no cooldown (signals resume immediately)")
                        else:
                            logger.warning(
                                f"⚠️ Entry thread exited without activation "
                                f"(mode={mode} side={side}) — resetting to FLAT")
                            self._last_exit_time = time.time()
                        self._pos.phase = PositionPhase.FLAT
                    # CRITICAL: Always reset entry engine when thread exits
                    # without opening a position. If the order failed/timed out,
                    # the entry engine is stuck in ENTERING state with no handler
                    # in the state machine — it will never recover on its own.
                    if (self._entry_engine is not None
                            and self._pos.phase != PositionPhase.ACTIVE):
                        self._entry_engine.on_entry_failed()
                        # CRITICAL: Reset entry engine from ENTERING → SCANNING.
                        # Without this, entry engine stays in ENTERING state
                        # and update() skips all processing forever.
                        if self._entry_engine is not None:
                            self._entry_engine.on_entry_cancelled()

        threading.Thread(
            target=_bg, daemon=True, name=f"enter-{mode}-{side}"
        ).start()

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
                    _price = t.get("price", 0.0)
                    _qty   = t.get("quantity", 0.0)
                    _buy   = t.get("side") == "buy"
                    # ── Wire to TickFlowEngine ──────────────────────────────
                    self._tick_eng.on_trade(_price, _qty, _buy, ts)
                    # ── Wire to CVDEngine true tick path ────────────────────
                    # This enables true cumulative volume delta — sum of actual
                    # (buy - sell) dollar volume per tick, not OHLCV approximation
                    if _price > 0 and _qty > 0:
                        self._cvd.update_from_tick(_price, _qty, _buy)
                    if ts > max_ts: max_ts = ts
            if max_ts > cutoff_ts: self._last_fed_trade_ts = max_ts
            # Update tick engine ATR percentile so window adapts to regime
            _atr_pct = self._atr_5m.get_percentile() if self._atr_5m.atr > 1e-10 else 0.5
            self._tick_eng.set_atr_pctile(_atr_pct)
            self._tick_eng.compute_signal()
        except Exception: pass

    def _compute_signals(self, data_manager) -> Optional[SignalBreakdown]:
        candles_1m = data_manager.get_candles("1m", limit=300)
        candles_5m = data_manager.get_candles("5m", limit=100)
        # Issue 1 fix: log WHY signals are blocked instead of silently returning None
        if len(candles_1m) < QCfg.MIN_1M_BARS():
            _now = time.time()
            if _now - self._last_data_warn >= 30.0:
                self._last_data_warn = _now
                logger.info(
                    f"⏳ Signals blocked: 1m candles={len(candles_1m)}/{QCfg.MIN_1M_BARS()} "
                    f"(waiting for warmup)")
            return None
        if len(candles_5m) < QCfg.MIN_5M_BARS():
            _now = time.time()
            if _now - self._last_data_warn >= 30.0:
                self._last_data_warn = _now
                logger.info(
                    f"⏳ Signals blocked: 5m candles={len(candles_5m)}/{QCfg.MIN_5M_BARS()} "
                    f"(waiting for warmup)")
            return None
        atr_1m = self._atr_1m.compute(candles_1m); atr_5m = self._atr_5m.compute(candles_5m)
        if atr_5m < 1e-10:
            _now = time.time()
            if _now - self._last_atr_warn >= 30.0:
                self._last_atr_warn = _now
                logger.info(
                    "⏳ Signals blocked: ATR not seeded yet — stream reconnect recovery. "
                    f"1m_atr={atr_1m:.2f} 5m_atr={atr_5m:.2f} "
                    f"(need {QCfg.ATR_PERIOD()} candles of live data)")
            return None
        price = data_manager.get_last_price()
        if price < 1.0:
            _now = time.time()
            if _now - self._last_price_warn >= 30.0:
                self._last_price_warn = _now
                logger.info("⏳ Signals blocked: no valid price from data manager")
            return None

        self._vwap.update(candles_1m, atr_5m)
        self._cvd.update(candles_1m)   # candle path fallback; tick path fed in _feed_microstructure

        # ── HTF filter — PRIMARY: ICT structure, FALLBACK: EMA slope ─────────
        # Pass the ICT engine so HTFTrendFilter can read BOS/CHoCH swing structure
        # directly instead of using a fragile EMA slope that flips on single candles.
        try:
            c15 = data_manager.get_candles("15m", limit=100)
            c4h = data_manager.get_candles("4h", limit=50)
            self._htf.update(c15, c4h, atr_5m, ict_engine=self._ict)
        except Exception:
            pass

        # ── Regime classification ─────────────────────────────────────────────
        self._adx.compute(candles_5m)
        regime = self._regime.update(
            self._adx, self._atr_5m, self._htf,
            vwap_dev_atr=self._vwap.deviation_atr if hasattr(self._vwap, 'deviation_atr') else 0.0,
            breakout_active=self._breakout.is_active if hasattr(self, '_breakout') else False,
            breakout_dir=self._breakout.direction if hasattr(self, '_breakout') and self._breakout.is_active else "",
        )

        # ── Regime-adaptive weights (v7.0) ────────────────────────────────────
        # Signal weights shift based on market regime so the composite score
        # reflects what actually matters in each regime:
        #   Ranging  → VWAP dominates; OB second
        #   Trending → CVD + TICK dominate; VWAP deprioritised (lagging in trends)
        #   Breakout → TICK first; CVD second
        w_vwap, w_cvd, w_ob, w_tick, w_vex = WeightScheduler.get(regime)

        # ── Mean-reversion signals ────────────────────────────────────────────
        vs  = self._vwap.get_reversion_signal(price, atr_5m)
        obs = self._ob_eng.get_signal()
        ts  = self._tick_eng.get_signal()
        ve  = self._vol_exh.compute(candles_1m)

        # ── v6.0 FIX: CVD signal = blend of DIVERGENCE + TREND ───────────────
        # PROBLEM: get_divergence_signal() returns 0 when CVD and price AGREE.
        # In a trend with strong buying AND rising price, CVD divergence = 0.
        # The strongest possible order flow confirmation was invisible.
        #
        # FIX: Blend both signals with regime-adaptive mixing:
        #   RANGING:    80% divergence + 20% trend (reversal detection primary)
        #   TRENDING:   20% divergence + 80% trend (directional flow primary)
        #   TRANSITION: 50% divergence + 50% trend (balanced)
        _cvd_div   = self._cvd.get_divergence_signal(candles_1m)
        _cvd_trend = self._cvd.get_trend_signal()
        if regime in (MarketRegime.TRENDING_UP, MarketRegime.TRENDING_DOWN):
            cs = _cvd_div * 0.20 + _cvd_trend * 0.80
        elif regime == MarketRegime.TRANSITIONING:
            cs = _cvd_div * 0.50 + _cvd_trend * 0.50
        else:
            cs = _cvd_div * 0.80 + _cvd_trend * 0.20

        # ── v6.0 FIX: Cap VWAP deviation influence ───────────────────────────
        # PROBLEM: VWAP deviation maxes at -1.0 whenever price is >1.2 ATR away.
        # In a trend, VWAP stays behind permanently, so vs = -1.0 ALWAYS.
        # This turns 30-40% of the composite into a permanent directional bias
        # that measures DISTANCE, not selling pressure.
        #
        # FIX: In trending markets AND during AMD MANIPULATION, cap VWAP
        # influence at ±0.5. During MANIPULATION, VWAP distance is irrelevant —
        # the swept pool IS the signal. VWAP can point the opposite direction
        # (price below VWAP = LONG signal) while AMD says SHORT (BSL swept).
        # Without this cap, VWAP=+1.0 at 0.30 weight = +0.30 in composite,
        # which alone clears the ±0.30 entry threshold in the WRONG direction.
        _vs_capped = vs
        if regime in (MarketRegime.TRENDING_UP, MarketRegime.TRENDING_DOWN):
            _vs_capped = max(-0.50, min(0.50, vs))

        comp = (_vs_capped*w_vwap + cs*w_cvd + obs*w_ob + ts*w_tick + ve*w_vex)
        comp = max(-1.0, min(1.0, comp))
        direction = 1.0 if comp >= 0 else -1.0
        nc = sum(1 for s in [_vs_capped, cs, obs, ts, ve] if s * direction > 0.05)

        # ── Trend-following score (TRENDING regime) ───────────────────────────
        # B4 FIX: The original formula used obs (Level-2 bid/ask z-score) at 20%
        # weight.  obs updates every orderbook snapshot — a momentary bid spike adds
        # +0.10 to trend_score, enough to flip a borderline TREND_COMPOSITE_MIN check
        # and block a valid SHORT with no logged reason.
        # Replacement: ADX +DI/-DI spread — structural and candle-frequency stable.
        # Normalised to [-1, +1] using the sum of both DI values as denominator
        # (dynamic, avoids the arbitrary /50 that underweighted the signal).
        _di_sum = max(self._adx.plus_di + self._adx.minus_di, 20.0)
        _di_spread = (self._adx.plus_di - self._adx.minus_di) / _di_sum
        _di_spread = max(-1.0, min(1.0, _di_spread))
        htf_comp    = self._htf.trend_4h * 0.60 + self._htf.trend_15m * 0.40
        cvd_trend   = self._cvd.get_trend_signal()
        trend_score = htf_comp * 0.50 + cvd_trend * 0.30 + _di_spread * 0.20
        trend_score = max(-1.0, min(1.0, trend_score))

        # ── Build signal breakdown with full attribution ───────────────────────
        # FIX 7: Store the raw order-flow composite BEFORE any ICT boost so
        # the reversion entry can restore it when computing for the opposite side.
        # Un-applying the boost from the clamped value gives a wrong pre-boost
        # composite, systematically making shorts ~0.10 less negative than warranted.
        sig = SignalBreakdown(
            vwap_dev=vs, cvd_div=cs, orderbook=obs, tick_flow=ts, vol_exhaust=ve,
            composite=comp, atr=atr_5m, atr_pct=self._atr_5m.get_percentile(),
            regime_ok=self._atr_5m.regime_valid(), regime_penalty=self._atr_5m.regime_penalty(),
            htf_veto=self._htf.vetoes_trade(self._vwap.reversion_side(price)),
            overextended=self._vwap.is_overextended(price, atr_5m, adx=self._adx.adx),
            vwap_price=self._vwap.vwap, deviation_atr=self._vwap.deviation_atr,
            reversion_side=self._vwap.reversion_side(price), n_confirming=nc,
            threshold_used=QCfg.COMPOSITE_ENTRY_MIN(),
            market_regime=regime.value, adx=self._adx.adx, trend_score=trend_score,
            # v7.0: weight attribution
            w_vwap=w_vwap, w_cvd=w_cvd, w_ob=w_ob, w_tick=w_tick, w_vex=w_vex,
            htf_ict_source=self._htf.ict_source,
            cvd_tick_count=self._cvd.tick_count,
        )
        sig._raw_composite = comp  # pre-boost composite (used in _evaluate_reversion_entry)
        self._last_sig = sig
        return sig

    def _log_thinking(self, sig, price, now):
        if now - self._last_think_log < self._think_interval: return
        self._last_think_log = now
        def bar(v, w=12):
            h=w//2; f=min(int(abs(v)*h+0.5),h)
            return (" "*h+"█"*f+"░"*(h-f)) if v>=0 else ("░"*(h-f)+"█"*f+" "*h)
        def fmt(l,v):
            a = "▲" if v>0.05 else ("▼" if v<-0.05 else "─")
            return f"  {l:<6} {bar(v)} {a} {v:+.3f}"

        c   = sig.composite
        thr = QCfg.COMPOSITE_ENTRY_MIN()
        regime_lbl = sig.market_regime

        # ── Determine the active routing path (B2/B8 fix) ────────────────────
        # _log_thinking previously computed only the reversion all-pass check,
        # then displayed "👀 Watching" regardless of which path was actually
        # active.  In TRENDING regime the bot routes to _evaluate_trend_entry —
        # the reversion composite gate shown was never the actual blocker.
        _has_sweep_entry = (
            _ICT_TRADE_ENGINE_AVAILABLE and
            QCfg.ICT_SWEEP_ENTRY_ENABLED() and
            self._active_sweep_setup is not None and
            self._active_sweep_setup.status == "OTE_READY"
        )
        if _has_sweep_entry:
            _routing_path = "sweep"
        elif self._breakout.is_active and self._breakout.retest_ready:
            _routing_path = "momentum"
        elif self._regime.is_trending():
            _routing_path = "trend"
        else:
            _routing_path = "reversion"

        # ── Sweep setup display ───────────────────────────────────────────
        _sweep_line = ""
        if self._active_sweep_setup is not None:
            ss = self._active_sweep_setup
            _sweep_line = (
                f"🏛️ SWEEP {ss.side.upper()} [{ss.status}] "
                f"OTE=[${ss.ote_entry_zone_low:.0f}–${ss.ote_entry_zone_high:.0f}] "
                f"SL=${ss.sl_sweep_candle:.0f} "
                f"AMD_conf={ss.amd_confidence:.2f} "
                f"q={ss.quality_score():.2f}")

        # ── Quant helper signals (display only — not gate logic here) ─────
        # Bug-5/7 fix: the Composite gate line used QCfg.COMPOSITE_ENTRY_MIN()
        # (0.350) as its displayed threshold, but the actual Tier-B gate in
        # ICTEntryGate uses TIER_B_COMPOSITE_MIN (0.30). Logs showed entries
        # "blocked at ±0.350" while the real block was at 0.30 — diagnostic
        # confusion.  Use the real gate constant here for the display tick mark.
        _tierb_thr = (ICTEntryGate.TIER_B_COMPOSITE_MIN
                      if _ICT_TRADE_ENGINE_AVAILABLE else thr)
        gates = [
            f"{'✅' if sig.overextended else '⚪'} Overextended ({sig.deviation_atr:+.1f} ATR)  [quant-scout: VWAP deviation]",
            f"{'✅' if sig.regime_ok else '❌'} Regime ({sig.atr_pct:.0%})  [ATR percentile gate]",
            f"⚪ HTF (15m={self._htf.trend_15m:+.2f} 4h={self._htf.trend_4h:+.2f}) [info only — not a gate]",
            f"{'✅' if sig.n_confirming>=3 else '⚪'} Quant Confluence ({sig.n_confirming}/5) [scout: n_conf]",
            f"{'✅' if abs(c)>=_tierb_thr else '⚪'} Composite ({c:+.3f} vs ±{_tierb_thr:.3f}) [scout: order-flow]",
        ]

        # ── ICT tier gate display (B1 + B6 fix) ──────────────────────────
        if self._ict is not None:
            if self._ict._initialized:
                _ict_scores_valid = (sig.ict_ob + sig.ict_fvg + sig.ict_sweep + sig.ict_session) > 0.0
                if _ict_scores_valid:
                    # G FIX: sig.ict_* may hold scores computed for the OPPOSITE
                    # direction (e.g. AMD bearish → ict_side=SHORT, but we are
                    # evaluating a LONG reversion).  If we pass those scores to
                    # ICTEntryGate.evaluate("long", sig, ...) the gate reads SHORT
                    # confluence as LONG support — potentially passing a tier check
                    # that should fail, or showing a misleading BLOCKED reason.
                    #
                    # Fix: when ict_direction opposes reversion_side, re-compute
                    # ICT confluence for reversion_side locally for this display call.
                    # We patch sig.ict_* temporarily (single-threaded tick call),
                    # evaluate the gate, then restore the originals.  sig.composite
                    # and sig.ict_boost_signed are NOT touched — they reflect the
                    # directional boost correctly applied in _evaluate_entry.
                    _disp_rev_side = sig.reversion_side or "long"
                    _ict_dir_vs_vwap = sig.ict_direction  # +1=long, -1=short, 0=unset
                    _ict_opposes_disp = (
                        (_ict_dir_vs_vwap < 0 and _disp_rev_side == "long") or
                        (_ict_dir_vs_vwap > 0 and _disp_rev_side == "short")
                    )

                    # Save originals for restore; also initialise display values to
                    # originals so the no-recompute path falls through unchanged.
                    _orig_ict_ob    = sig.ict_ob
                    _orig_ict_fvg   = sig.ict_fvg
                    _orig_ict_sweep = sig.ict_sweep
                    _orig_ict_sess  = sig.ict_session
                    _orig_ict_total = sig.ict_total
                    _orig_ict_det   = sig.ict_details
                    _disp_note      = ""

                    # H FIX: Track display scores separately so we can show the
                    # re-computed values in the label AFTER restoring originals.
                    # Without this, ict_gate_lbl used sig.ict_total (restored SHORT)
                    # while the tier label came from LONG evaluation — displaying
                    # "Σ=0.52 [disp re-comp for LONG]" where 0.52 was the SHORT score.
                    _disp_ict_total = _orig_ict_total
                    _disp_ict_ob    = _orig_ict_ob
                    _disp_ict_fvg   = _orig_ict_fvg
                    _disp_ict_sweep = _orig_ict_sweep
                    _disp_ict_sess  = _orig_ict_sess

                    # STACK/RISK DISPLAY FIX: Stack and RevRisk display lines read
                    # from sig.ict_mtf_ob_count / sig.ict_fvg_stack_count which were
                    # set from the PRIMARY (ICT-side) confluence call. When ICT and
                    # VWAP directions conflict and _rc is computed for the VWAP side,
                    # the gate display shows re-computed FVG=0.67 while the Stack
                    # still shows 0FVG from the original side — contradictory.
                    # Fix: track display versions of all advanced fields and update
                    # them from _rc when re-computing, without touching sig fields
                    # that should remain from the authoritative primary computation.
                    _disp_mtf_ob        = sig.ict_mtf_ob_count
                    _disp_fvg_stack     = sig.ict_fvg_stack_count
                    _disp_rev_risk      = sig.ict_htf_reversal_risk
                    _disp_rev_zone_near = sig.ict_htf_rev_zone_near
                    _disp_chain_score   = sig.ict_chain_score
                    _disp_ssl_atr       = sig.ict_nearest_ssl_atr
                    _disp_bsl_atr       = sig.ict_nearest_bsl_atr

                    if _ict_opposes_disp:
                        try:
                            _now_ms_d = int(now * 1000) if now < 1e12 else int(now)
                            _rc = self._ict.get_confluence(
                                _disp_rev_side, price, _now_ms_d, atr=sig.atr)
                            # Patch sig so the ICTEntryGate evaluation reads
                            # correctly-directed scores
                            sig.ict_ob      = _rc.ob_score
                            sig.ict_fvg     = _rc.fvg_score
                            sig.ict_sweep   = _rc.sweep_score
                            sig.ict_session = _rc.session_score
                            sig.ict_total   = _rc.total
                            sig.ict_details = _rc.details
                            # H FIX: save re-computed values for display — these
                            # will survive the restore below
                            _disp_ict_total = _rc.total
                            _disp_ict_ob    = _rc.ob_score
                            _disp_ict_fvg   = _rc.fvg_score
                            _disp_ict_sweep = _rc.sweep_score
                            _disp_ict_sess  = _rc.session_score
                            _disp_note = f" [disp re-comp for {_disp_rev_side.upper()}]"
                            # STACK/RISK DISPLAY: update display vars from _rc
                            # so Stack, RevRisk, and RevZone reflect the same side
                            # as the FVG/OB scores shown in the gate line
                            _disp_mtf_ob        = _rc.mtf_ob_count
                            _disp_fvg_stack     = _rc.fvg_stack_count
                            _disp_rev_risk      = _rc.htf_reversal_risk
                            _disp_rev_zone_near = (
                                getattr(_rc, 'judas_swing_active', False) or
                                _rc.htf_reversal_risk > (0.90 if sig.adx < 25.0 else 0.55)
                            )
                            _disp_chain_score   = sig.ict_chain_score  # chain = AMD side, unchanged
                            _disp_ssl_atr       = _rc.nearest_ssl_dist_atr
                            _disp_bsl_atr       = _rc.nearest_bsl_dist_atr
                        except Exception:
                            pass  # fall through: display original scores with a warning

                    _tier_display = "B"
                    _tier_reason_d = ""
                    if _ICT_TRADE_ENGINE_AVAILABLE:
                        try:
                            _qh_d = self._get_quant_helpers(sig, _disp_rev_side)
                            _td, _, _tdr = ICTEntryGate.evaluate(
                                _disp_rev_side, sig,
                                self._active_sweep_setup, price, _qh_d,
                                mode="reversion", market_regime=sig.market_regime,
                                ict_engine=self._ict)
                            _tier_display  = _td
                            _tier_reason_d = _tdr
                        except Exception:
                            _tier_display  = "?"
                            _tier_reason_d = ""

                    # Restore originals — must happen before any further sig use
                    sig.ict_ob      = _orig_ict_ob
                    sig.ict_fvg     = _orig_ict_fvg
                    sig.ict_sweep   = _orig_ict_sweep
                    sig.ict_session = _orig_ict_sess
                    sig.ict_total   = _orig_ict_total
                    sig.ict_details = _orig_ict_det

                    _tier_icons = {"S": "🥇", "A": "🥈", "B": "🥉",
                                   "BLOCKED": "⛔", "?": "❓"}

                    # B1 FIX: show the signed ICT direction (from _evaluate_entry)
                    # so the arrow reflects the composite contribution direction.
                    _ict_dir_arrow = ("▲LONG"  if sig.ict_direction > 0
                                      else ("▼SHORT" if sig.ict_direction < 0
                                            else "─UNSET"))
                    _boost_str = f"{sig.ict_boost_signed:+.3f}" if sig.ict_direction != 0 else "n/a"

                    # B6 FIX + H FIX: use _disp_ict_* which holds either the
                    # re-computed entry-side scores (when recompute succeeded) or
                    # the original scores (when recompute was skipped / failed).
                    # This ensures Σ and sub-scores always match the tier label
                    # direction — previously the restored SHORT total was shown
                    # next to a LONG tier label after re-computation.
                    _ob_norm  = min(_disp_ict_ob  / 2.0, 1.0)
                    _fvg_norm = min(_disp_ict_fvg / 1.5, 1.0)

                    ict_gate_lbl = (
                        f"{_tier_icons.get(_tier_display,'❓')} ICT [{_tier_display}]{_disp_note} "
                        f"Σ={_disp_ict_total:.2f} ({_ict_dir_arrow} boost={_boost_str}) "
                        f"[OB={_ob_norm:.2f}({_disp_ict_ob:.1f}raw) "
                        f"FVG={_fvg_norm:.2f}({_disp_ict_fvg:.1f}raw) "
                        f"Swp={_disp_ict_sweep:.1f} KZ={_disp_ict_sess:.1f}]"
                    )
                    if _tier_reason_d:
                        ict_gate_lbl += f"\n    {_tier_reason_d}"
                else:
                    ict_gate_lbl = "📋 ICT (ACTIVE position — not updated)"
                gates.append(ict_gate_lbl)
            else:
                gates.append("⏳ ICT (initializing...)")

        # ── AMD phase + MTF ───────────────────────────────────────────────
        if self._ict is not None and self._ict._initialized:
            amd_icon = {"DISTRIBUTION": "🎯", "MANIPULATION": "⚡",
                        "REACCUMULATION": "🔄", "REDISTRIBUTION": "🔄",
                        "ACCUMULATION": "💤"}.get(sig.amd_phase, "❓")
            bias_icon = "🟢" if sig.amd_bias == "bullish" else (
                        "🔴" if sig.amd_bias == "bearish" else "⚪")
            # AMD line — include delivery target and Judas flag if active
            _amd_line = (
                f"{amd_icon} AMD: {sig.amd_phase} {bias_icon}{sig.amd_bias} "
                f"conf={sig.amd_conf:.2f} | {sig.mtf_details}")
            if sig.ict_delivery_target > 0:
                _dt_dist_atr = (abs(sig.ict_delivery_target - price) / sig.atr
                                if sig.atr > 0 else 0.0)
                _amd_line += (
                    f" | 🎯TARGET=${sig.ict_delivery_target:,.0f}"
                    f"({_dt_dist_atr:.1f}ATR,conf={sig.ict_delivery_conf:.2f})")
            if sig.ict_judas_active:
                _amd_line += " | ⚡JUDAS_ACTIVE"
            gates.append(_amd_line)

            # PD matrix — premium/discount across all TFs
            pd_icon = "💰DISC" if sig.in_discount else (
                      "💎PREM" if sig.in_premium  else "〰️EQ")
            _pd_line = f"🗺️ MTF: {'✅ALIGNED' if sig.mtf_aligned else '❌SPLIT'} {pd_icon}"
            if sig.ict_pd_matrix:
                _pd_line += f"  PD:[{sig.ict_pd_matrix}]"
            gates.append(_pd_line)

            # Advanced structural context line (session quality, chain, reversal risk)
            _adv_parts = []
            if sig.ict_session_entry_q:
                _eq_icon = {"HIGH": "🟢", "MEDIUM": "🟡", "LOW": "🟠",
                            "AVOID": "🔴"}.get(sig.ict_session_entry_q, "⚪")
                _adv_parts.append(f"Session:{_eq_icon}{sig.ict_session_entry_q}")
            # Use _disp_* variables which reflect the re-computed side when applicable.
            # This ensures Stack, RevRisk and SSL/BSL distances match the same side
            # as the FVG/OB scores in the gate line (no more "FVG=0.67 but Stack:0FVG").
            # Bug-4 fix: use locals() instead of dir().  dir() includes class/instance
            # attrs and has implementation-defined semantics for locals — locals() is
            # the correct stdlib idiom to probe whether a name was assigned in the
            # current frame.  The _disp_* vars are set unconditionally inside the
            # _ict_scores_valid block above, so they are always present here when
            # that block ran; the fallback path is only hit when ICT is uninitialized.
            _d_chain   = locals().get('_disp_chain_score',   sig.ict_chain_score)
            _d_ob      = locals().get('_disp_mtf_ob',        sig.ict_mtf_ob_count)
            _d_fvg     = locals().get('_disp_fvg_stack',     sig.ict_fvg_stack_count)
            _d_rr      = locals().get('_disp_rev_risk',      sig.ict_htf_reversal_risk)
            _d_rz      = locals().get('_disp_rev_zone_near', sig.ict_htf_rev_zone_near)
            _d_ssl     = locals().get('_disp_ssl_atr',       sig.ict_nearest_ssl_atr)
            _d_bsl     = locals().get('_disp_bsl_atr',       sig.ict_nearest_bsl_atr)
            if _d_chain > 0:
                _adv_parts.append(f"Chain={_d_chain:.2f}")
            if _d_ob > 0 or _d_fvg > 0:
                _adv_parts.append(f"Stack:{_d_ob}OB+{_d_fvg}FVG")
            if _d_rr > 0.40:
                _adv_parts.append(f"⚠️RevRisk={_d_rr:.2f}")
            if _d_rz:
                _adv_parts.append("🚧HTF_REV_ZONE<1ATR")
            if _d_ssl > 0:
                _adv_parts.append(f"SSL↓{_d_ssl:.1f}ATR")
            if _d_bsl > 0:
                _adv_parts.append(f"BSL↑{_d_bsl:.1f}ATR")
            if _adv_parts:
                gates.append(f"🔬 ICT: {' | '.join(_adv_parts)}")

        gates.append(f"📊 {regime_lbl} | ADX={sig.adx:.1f} | TrendΣ={sig.trend_score:+.3f}")

        # ── B2/B8 FIX: routing-aware all-pass and status label ────────────
        # Previously the all-pass check and "👀 Watching" label always showed
        # reversion gates regardless of which path (_evaluate_trend_entry,
        # _evaluate_momentum_entry, etc.) was actually active.  Now we surface
        # the real blocker for the path currently being evaluated.
        _ict_init = self._ict is not None and self._ict._initialized
        ap       = False
        _status  = "👀 Watching"

        if _routing_path == "trend":
            t_side   = self._regime.trend_side() or "?"
            cvd_bias = self._cvd.get_trend_signal()
            tf_sig   = self._tick_eng.get_signal()
            _cvd_ok  = (cvd_bias >= QCfg.TREND_CVD_MIN()
                        if t_side == "long"
                        else cvd_bias <= -QCfg.TREND_CVD_MIN())
            _tick_ok = (tf_sig > -0.30 if t_side == "long" else tf_sig < 0.30)
            _ts_ok   = (abs(sig.trend_score) >= QCfg.TREND_COMPOSITE_MIN() and
                        (sig.trend_score > 0 if t_side == "long"
                         else sig.trend_score < 0))
            _ict_ok_t = (not _ict_init) or (sig.ict_total >= 0.40)
            gates.append(f"🔀 ACTIVE PATH: TREND [{t_side.upper()}]")
            gates.append(
                f"  {'✅' if _cvd_ok  else '❌'} CVD trend "
                f"({cvd_bias:+.3f} vs ±{QCfg.TREND_CVD_MIN():.2f})")
            gates.append(
                f"  {'✅' if _tick_ok else '❌'} Tick flow "
                f"({tf_sig:+.3f} vs ±0.30)")
            gates.append(
                f"  {'✅' if _ts_ok   else '❌'} Trend score "
                f"({sig.trend_score:+.3f} vs ±{QCfg.TREND_COMPOSITE_MIN():.2f})")
            ap = _cvd_ok and _tick_ok and _ts_ok and _ict_ok_t
            _status = ("🎯 ENTRY READY" if ap
                       else f"⏳ TREND WAIT [{t_side.upper()}] — EMA pullback")

        elif _routing_path == "momentum":
            gates.append(
                f"🔀 ACTIVE PATH: MOMENTUM RETEST [{self._breakout.direction}]")
            ap      = True   # momentum path manages its own confirm counter
            _status = f"⏳ MOMENTUM RETEST ({self._breakout.direction})"

        elif _routing_path == "sweep":
            gates.append(
                f"🔀 ACTIVE PATH: SWEEP OTE [{self._active_sweep_setup.side.upper()}]")
            ap      = True
            _status = (f"🏛️ SWEEP OTE "
                       f"[{self._active_sweep_setup.side.upper()}] "
                       f"q={self._active_sweep_setup.quality_score():.2f}")

        else:
            # Reversion path
            # E FIX: also detect ICT/VWAP direction mismatch so the display is
            # explicit about the conflict instead of silently confusing.
            _ict_dir_disp = sig.ict_direction   # +1=long, -1=short, 0=unset
            _ict_rev_side = sig.reversion_side or "long"
            _ict_vs_vwap_conflict = (
                (_ict_dir_disp < 0 and _ict_rev_side == "long") or
                (_ict_dir_disp > 0 and _ict_rev_side == "short")
            )
            if _ict_vs_vwap_conflict:
                _conflict_lbl = (
                    "▼SHORT" if _ict_dir_disp < 0 else "▲LONG")
                gates.append(
                    f"⚠️  ICT ({_conflict_lbl}) vs VWAP "
                    f"({_ict_rev_side.upper()}) — opposite directions; "
                    f"ICT scores re-computed for {_ict_rev_side.upper()}")

            _ap_reason = ""
            if _ict_init and _ICT_TRADE_ENGINE_AVAILABLE:
                try:
                    _qh_ap = self._get_quant_helpers(sig, _ict_rev_side)
                    # D FIX: capture reason so we can surface it in _status
                    # instead of the generic "👀 Watching" that obscures ICT blocks.
                    _ap_tier, _, _ap_reason = ICTEntryGate.evaluate(
                        _ict_rev_side, sig,
                        self._active_sweep_setup, price, _qh_ap,
                        mode="reversion", market_regime=sig.market_regime,
                        ict_engine=self._ict)
                    ap = _ap_tier in ("S", "A", "B")
                    # FIX Bug-3: ICTEntryGate passed but breakout would block in
                    # _evaluate_entry — display must reflect the same gate.
                    # Without this, "🎯 ENTRY READY" appeared even when the next
                    # line logged "🚫 Breakout blocks SHORT/LONG reversion".
                    if ap and self._breakout.blocks_reversion(_ict_rev_side):
                        ap = False
                        _bo_dir = self._breakout.direction.upper()
                        _ap_reason = f"BREAKOUT_BLOCK_{_bo_dir}"

                    # DISPLAY-SYNC FIX: mirror the HTF_REV_ZONE pre-gate that fires
                    # first in _evaluate_reversion_entry(). Without this, the status
                    # box shows "MANIP_no_confirmed_sweep" while the actual first block
                    # executed is HTF_REV_ZONE — misleading to read in the logs.
                    # Note: _is_ote_sweep check matches the condition in _evaluate_entry.
                    _disp_is_ote = (
                        _ICT_TRADE_ENGINE_AVAILABLE and
                        self._active_sweep_setup is not None and
                        self._active_sweep_setup.status == "OTE_READY"
                    )
                    if (not ap and
                            sig.ict_htf_rev_zone_near and
                            not _disp_is_ote and
                            sig.ict_htf_reversal_risk > (0.90 if sig.adx < 25.0 else 0.55)):
                        _ap_reason = (
                            f"HTF_REV_ZONE(risk={sig.ict_htf_reversal_risk:.2f},"
                            f"zone<1ATR)")
                except Exception:
                    ap = False
            else:
                # FIX: Use the minimum ADX-adaptive floor (0.12) for display consistency.
                # HTF veto removed from gate — informational only.
                _ict_ok = (not _ict_init) or (sig.ict_total >= 0.12)
                ap = (sig.overextended and sig.regime_ok and
                      sig.n_confirming >= 3 and abs(c) >= thr and _ict_ok)
                # FIX Bug-3 (non-ICT path): same breakout gate
                if ap and self._breakout.blocks_reversion(_ict_rev_side):
                    ap = False
                    _ap_reason = f"BREAKOUT_BLOCK_{self._breakout.direction.upper()}"

            # D FIX: surface the real block reason — "👀 Watching" implied
            # passive state; when ICT hard-blocks the status must say so.
            if ap:
                _status = "🎯 ENTRY READY"
            elif _ap_reason and _ap_reason not in ("no_ict", ""):
                _status = f"⛔ ICT BLOCKED — {_ap_reason}"
            else:
                _status = "👀 Watching"

        cd = max(0.0, QCfg.COOLDOWN_SEC() - (now - self._last_exit_time))
        _in_pos = self._pos.is_active()

        if _in_pos:
            # ════════════════════════════════════════════════════════════════
            # IN-POSITION MONITOR — shows ONLY what is actually managing the
            # position: _ICTStructureTrail (BOS/CHoCH/phase/R-multiples).
            # Old quant-scout gates (VWAP/CVD/composite/n_confirming) are NOT
            # shown here because they play zero role once we are in a trade.
            # ════════════════════════════════════════════════════════════════
            # BUG-FIX: _log_thinking has no `atr` parameter; pull it from the
            # signal object so every downstream reference is defined.
            atr       = sig.atr if sig.atr > 1e-10 else 1.0
            pos       = self._pos
            profit_pts = ((price - pos.entry_price) if pos.side == "long"
                          else (pos.entry_price - price))
            init_dist  = pos.initial_sl_dist if pos.initial_sl_dist > 1e-10 else atr
            cur_r      = profit_pts / init_dist if init_dist > 1e-10 else 0.0
            peak_r     = pos.peak_profit / init_dist if init_dist > 1e-10 else 0.0
            mfe_r      = max(cur_r, peak_r)

            # ── BOS count and CHoCH from the live ICT engine ─────────────
            _now_ms_disp = int(now * 1000) if now > 0 else int(time.time() * 1000)
            bos_cnt   = 0
            choch_tf  = None
            choch_lvl = 0.0
            if self._ict is not None:
                try:
                    bos_cnt = _ICTStructureTrail._bos_count(
                        self._ict, pos.side, _now_ms_disp)
                    choch_tf, choch_lvl = _ICTStructureTrail._choch(
                        self._ict, pos.side)
                except Exception:
                    pass
            choch_active = choch_tf is not None and choch_lvl > 0.0

            # ── Break-even lock ───────────────────────────────────────────
            _fee_buf  = pos.entry_price * QCfg.COMMISSION_RATE() * 2.0 + 0.10 * atr
            _be_price = (pos.entry_price + _fee_buf if pos.side == "long"
                         else pos.entry_price - _fee_buf)
            be_locked = ((pos.side == "long"  and pos.sl_price >= _be_price) or
                         (pos.side == "short" and pos.sl_price <= _be_price))

            # ── Trail phase (v6.0: derived from tier, not old _phase method) ─
            # The _ICTStructureTrail uses continuous tier-based logic, not discrete
            # phases. Map tier to meaningful labels for display.
            if mfe_r >= 2.0:
                trail_phase = 3
            elif mfe_r >= 1.0:
                trail_phase = 2
            elif mfe_r >= 0.40:
                trail_phase = 1
            elif mfe_r >= 0.10:
                trail_phase = 0  # chandelier active but early
            else:
                trail_phase = -1  # below activation threshold

            # v6.0: Dynamic phase labels reflecting actual trail state
            if trail_phase == 3:
                phase_lbl = f"🟢 PHASE 3 — AGGRESSIVE STRUCTURE TRAIL (R={mfe_r:.2f}R, tight to 1m/5m)"
            elif trail_phase == 2:
                if choch_active:
                    phase_lbl = f"🟠 PHASE 2 — CHoCH TIGHTEN  (CHoCH@${choch_lvl:.0f} [{choch_tf}])"
                elif bos_cnt >= 2:
                    phase_lbl = f"🟠 PHASE 2 — BOS SWING TRAIL (bos={bos_cnt})"
                else:
                    phase_lbl = f"🟠 PHASE 2 — STRUCTURE + CHANDELIER (R={mfe_r:.2f}R)"
            elif trail_phase == 1:
                phase_lbl = f"🟡 PHASE 1 — BE FLOOR + CHANDELIER (bos={bos_cnt}, R={mfe_r:.2f}R)"
            elif trail_phase == 0:
                phase_lbl = f"⬜ PHASE 0 — CHANDELIER ENVELOPE ACTIVE (R={mfe_r:.2f}R, wide trail)"
            else:
                phase_lbl = f"⬜ PHASE 0 — HANDS OFF (R={mfe_r:.2f}R < 0.10R activation)"

            # ── Margin % P&L ───────────────────────────────────────────────
            _margin_pnl_pct_disp = 0.0
            _margin_used_disp = 0.0
            try:
                if pos.entry_price > 0 and pos.quantity > 0:
                    _notional_d = pos.entry_price * pos.quantity
                    _lev_d = QCfg.LEVERAGE()
                    _margin_used_disp = _notional_d / _lev_d if _lev_d > 0 else _notional_d
                    if _margin_used_disp > 1e-10:
                        _unrealised_d = profit_pts * pos.quantity
                        _margin_pnl_pct_disp = (_unrealised_d / _margin_used_disp) * 100.0
            except Exception:
                pass

            # ── Ratchet milestone reached ─────────────────────────────────
            _last_ratchet_r = getattr(pos, 'last_ratchet_r', 0.0)
            _ratchet_milestones = [2.50, 2.00, 1.50, 1.00, 0.50]
            _next_ratchet = next(
                (m for m in _ratchet_milestones if mfe_r < m), None)
            _next_ratchet_str = (f"{_next_ratchet:.2f}R" if _next_ratchet
                                 else "all milestones hit")

            # ── Progress bar ──────────────────────────────────────────────
            _bar_filled = min(int(mfe_r * 4), 16)
            _prog_bar   = "█" * _bar_filled + "░" * (16 - _bar_filled)

            # ── SL / TP distances ─────────────────────────────────────────
            sl_dist_atr = abs(price - pos.sl_price) / max(atr, 1)
            tp_dist_atr = abs(pos.tp_price - price) / max(atr, 1)

            # ── AMD and dealing range at a glance ─────────────────────────
            _amd_brief = ""
            _dr_brief  = ""
            if self._ict is not None and self._ict._initialized:
                try:
                    _amd_brief = (
                        f"{self._ict._amd.phase}  bias={self._ict._amd.bias}"
                        f"  conf={self._ict._amd.confidence:.2f}")
                except Exception:
                    pass
                try:
                    _dr = getattr(self._ict, '_dealing_range', None)
                    if _dr:
                        _pd = getattr(_dr, 'current_pd', 0.5)
                        _pd_lbl = ("DEEP DISC" if _pd < 0.25 else
                                   "DISCOUNT"  if _pd < 0.40 else
                                   "EQ"        if _pd < 0.60 else
                                   "PREMIUM"   if _pd < 0.75 else "DEEP PREM")
                        _dr_brief = f"{_pd_lbl} ({_pd:.0%})  range=[${_dr.low:,.0f}–${_dr.high:,.0f}]"
                except Exception:
                    pass

            # ── Pool TP target (stored from entry signal) ─────────────────
            _pool_tp_str = ""
            _es = getattr(self, '_last_entry_signal', None)
            if _es is not None:
                try:
                    _pt = _es.target_pool
                    if _pt and hasattr(_pt, 'pool'):
                        _pool_tp_str = (
                            f"{_pt.pool.side.value} @ ${_pt.pool.price:,.0f}"
                            f"  sig={_pt.significance:.2f}")
                except Exception:
                    pass

            lines = [
                f"┌─── 📊 IN-POSITION MONITOR [{pos.side.upper()}] ────────────────────────",
                f"  Price  ${price:,.2f}  │  ATR={atr:.1f}  │  Hold={now - pos.entry_time:.0f}s",
                f"  Entry  ${pos.entry_price:,.2f}  SL ${pos.sl_price:,.2f}  TP ${pos.tp_price:,.2f}",
                f"  SL dist: {sl_dist_atr:.1f}ATR  │  TP dist: {tp_dist_atr:.1f}ATR",
            ]
            if _pool_tp_str:
                lines.append(f"  Pool TP: {_pool_tp_str}")
            lines += [
                f"  {'─'*60}",
                f"  R-PROGRESS:  current={cur_r:+.2f}R  peak(MFE)={mfe_r:.2f}R",
                f"  [{_prog_bar}] {mfe_r:.2f}R  │  next ratchet @ {_next_ratchet_str}",
                f"  MARGIN PnL: {_margin_pnl_pct_disp:+.1f}% on ${_margin_used_disp:.2f} margin",
                f"  {'─'*60}",
                f"  TRAIL ENGINE: {phase_lbl}",
                f"  BOS confirmed: {bos_cnt}  │  CHoCH: "
                + (f"{choch_tf} @ ${choch_lvl:,.0f}" if choch_active else "none"),
                f"  Break-even:  "
                + ("✅ LOCKED — risk-free trade" if be_locked
                   else f"❌ not yet  (SL needs to reach ${_be_price:,.2f})"),
            ]
            if _amd_brief:
                lines.append(f"  AMD: {_amd_brief}")
            if _dr_brief:
                lines.append(f"  Zone: {_dr_brief}")
            lines.append(f"└{'─'*66}")
            logger.info("\n" + "\n".join(lines))

        else:
            # ── SCANNING / IDLE — show quant-scout gate table (unchanged) ──
            _ict_side_lbl = getattr(sig, '_ict_evaluated_side', sig.reversion_side) or "?"
            _header = (
                f"┌─── 🧠 v9 ICT-COMMANDER  ${price:,.2f}  VWAP=${sig.vwap_price:,.2f}"
                f"  ATR={sig.atr:.1f}  ICT-side={_ict_side_lbl.upper()} ────")
            lines = [_header]
            if _sweep_line:
                lines.append(f"  {_sweep_line}")
            lines += [fmt("VWAP", sig.vwap_dev), fmt("CVD", sig.cvd_div),
                      fmt("OB", sig.orderbook), fmt("TICK", sig.tick_flow),
                      fmt("VEX", sig.vol_exhaust), f"  {'─'*42}",
                      f"  Σ={c:+.4f} | VWAP-side: {sig.reversion_side.upper()}",
                      f"  ── GATES (⚪=quant-scout, ❌=hard-block) ──"]
            for g in gates:
                lines.append(f"  {g}")
            lines.append(f"  {_status}")
            lines.append(f"  Cooldown: {f'{cd:.0f}s' if cd > 0 else 'ready'}")
            lines.append(f"└{'─'*66}")
            logger.info("\n" + "\n".join(lines))


    @staticmethod
    def _resolve_ict_side_from_structure(ict_engine, fallback_side: str,
                                          now_ms: int) -> str:
        """
        B3 FIX: Derive ICT direction from the dominant active OB structure when
        AMD is neutral.  Previously this fell back to sig.reversion_side (VWAP),
        causing the ICT boost to work against the OB score it was computed from.

        Logic:
          - Sum quality of active bull OBs  vs  active bear OBs.
          - If one side dominates by > 0.1 quality points, use that side.
          - Otherwise fall through to fallback_side (VWAP or composite sign).

        The 0.1 threshold prevents noise from a single low-quality OB overriding
        VWAP when structure is genuinely ambiguous.
        """
        try:
            bull_quality = sum(
                ob.strength / 100.0
                for ob in ict_engine.order_blocks_bull
                if ob.is_active(now_ms)
            )
            bear_quality = sum(
                ob.strength / 100.0
                for ob in ict_engine.order_blocks_bear
                if ob.is_active(now_ms)
            )
            if bull_quality > bear_quality + 0.1:
                return "long"
            if bear_quality > bull_quality + 0.1:
                return "short"
        except Exception:
            pass
        return fallback_side


    def _evaluate_entry(self, data_manager, order_manager, risk_manager, now):
        """
        v9.0 -- Liquidity-First Entry Engine.
        Single decision flow. Falls back to legacy if new engine unavailable.
        """
        if not _ENTRY_ENGINE_AVAILABLE or self._entry_engine is None or self._liq_map is None:
            logger.error("EntryEngine or LiquidityMap unavailable — no entry evaluation")
            return

        # Step 1: Spread gate
        spread_ok, spread_ratio = self._spread_atr_gate(data_manager)
        if not spread_ok:
            return

        price = data_manager.get_last_price()
        if price < 1.0:
            return

        now_ms = int(now * 1000) if now < 1e12 else int(now)

        # Step 2: Gather candles (all timeframes)
        # v3.0: Request full buffer depth for structural timeframes.
        # The data manager returns whatever it has — on fresh startup
        # this will be ~200 (REST warmup limit), but after continuous
        # operation the WebSocket feed accumulates 7 days of history.
        # This gives the liquidity map progressively deeper structure
        # detection as the bot runs longer.
        candles_by_tf = {}
        for tf, limit in [("1m", 300), ("5m", 2100), ("15m", 700),
                          ("1h", 200), ("4h", 50), ("1d", 30)]:
            try:
                candles_by_tf[tf] = data_manager.get_candles(tf, limit=limit)
            except Exception:
                candles_by_tf[tf] = []

        # ── BUG-FIX-1: v9 path NEVER called _compute_signals(), which was the
        # ONLY place ATREngine, VWAPEngine, CVDEngine, ADXEngine, and HTFTrendFilter
        # were updated.  Result: atr_5m.atr == 0.0 forever → every tick exits at
        # the ATR gate → _liq_map.update() never runs → "no pools in range" + "ATR: —"
        # permanently.  Fix: update all engines from the freshly-fetched candles
        # RIGHT HERE, before any logic that reads self._atr_5m.atr.
        try:
            _c5m  = candles_by_tf.get("5m",  [])
            _c1m  = candles_by_tf.get("1m",  [])
            _c15m = candles_by_tf.get("15m", [])
            _c4h  = candles_by_tf.get("4h",  [])
            if len(_c5m) >= QCfg.MIN_5M_BARS():
                self._atr_5m.compute(_c5m)
            if len(_c1m) >= QCfg.MIN_1M_BARS():
                self._atr_1m.compute(_c1m)
            if len(_c5m) >= 20:
                self._adx.compute(_c5m)
            _vwap_window = max(QCfg.VWAP_WINDOW(), 20)
            if len(_c1m) >= _vwap_window:
                self._vwap.update(_c1m, self._atr_5m.atr)
                self._cvd.update(_c1m)
            if len(_c15m) >= 10 and len(_c4h) >= 5:
                self._htf.update(_c15m, _c4h, self._atr_5m.atr,
                                 ict_engine=self._ict)
        except Exception as _eng_e:
            logger.debug(f"v9 engine update error (non-fatal): {_eng_e}")

        atr = self._atr_5m.atr
        if atr < 1e-10:
            _now_ts = time.time()
            if _now_ts - self._last_atr_warn >= 30.0:
                self._last_atr_warn = _now_ts
                logger.info(
                    f"⏳ v9 entry: ATR not seeded yet "
                    f"({len(candles_by_tf.get('5m', []))} 5m candles, "
                    f"need {QCfg.MIN_5M_BARS()} — waiting for warmup)")
            return

        # Step 3: Update ICT engine (preserved -- provides structural context)
        if self._ict is not None:
            try:
                self._ict.update(
                    candles_by_tf.get("5m", []),
                    candles_by_tf.get("15m", []),
                    price, now_ms,
                    candles_1m=candles_by_tf.get("1m"),
                    candles_1h=candles_by_tf.get("1h"),
                    candles_4h=candles_by_tf.get("4h"),
                    candles_1d=candles_by_tf.get("1d"),
                )
                if hasattr(self._ict, 'set_order_flow_data'):
                    tf_now = self._tick_eng.get_signal() if self._tick_eng else 0.0
                    cvd_now = self._cvd.get_trend_signal() if self._cvd else 0.0
                    self._ict.set_order_flow_data(tf_now, cvd_now)
            except Exception as e:
                logger.debug(f"ICT update error: {e}")

        # Step 4: Update liquidity map
        self._liq_map.update(
            candles_by_tf=candles_by_tf,
            price=price, atr=atr, now=now,
            ict_engine=self._ict,
        )
        liq_snapshot = self._liq_map.get_snapshot(price, atr)

        # Step 5: Build orderflow state
        tick_flow = self._tick_eng.get_signal() if self._tick_eng else 0.0
        cvd_trend = self._cvd.get_trend_signal() if self._cvd else 0.0
        cvd_div = 0.0
        try:
            cvd_div = self._cvd.get_divergence_signal(
                candles_by_tf.get("1m", []))
        except Exception:
            pass

        if tick_flow > 0.4:
            if self._flow_streak_dir_v2 == "long":
                self._flow_streak_count_v2 += 1
            else:
                self._flow_streak_dir_v2 = "long"
                self._flow_streak_count_v2 = 1
        elif tick_flow < -0.4:
            if self._flow_streak_dir_v2 == "short":
                self._flow_streak_count_v2 += 1
            else:
                self._flow_streak_dir_v2 = "short"
                self._flow_streak_count_v2 = 1
        else:
            self._flow_streak_count_v2 = max(0, self._flow_streak_count_v2 - 1)
            if self._flow_streak_count_v2 == 0:
                self._flow_streak_dir_v2 = ""

        ob_imbalance = 0.0
        try:
            ob = data_manager.get_orderbook()
            if ob and ob.get("bids") and ob.get("asks"):
                bid_vol = sum(float(b[1]) for b in ob["bids"][:10])
                ask_vol = sum(float(a[1]) for a in ob["asks"][:10])
                total = bid_vol + ask_vol
                if total > 0:
                    ob_imbalance = (bid_vol - ask_vol) / total
        except Exception:
            pass

        flow_state = OrderFlowState(
            tick_flow=tick_flow,
            cvd_trend=cvd_trend,
            cvd_divergence=cvd_div,
            ob_imbalance=ob_imbalance,
            tick_streak=self._flow_streak_count_v2,
            streak_direction=self._flow_streak_dir_v2,
        )
        # BUG-FIX-3: Persist conviction/direction for main.py heartbeat display.
        # getattr(strat, '_flow_conviction', 0.0) in heartbeat always returned 0
        # because these attrs were never written in the v9 path.
        self._flow_conviction = flow_state.conviction
        self._flow_direction  = flow_state.direction

        # Step 6: Build ICT context
        ict_ctx = ICTContext()
        if self._ict is not None and getattr(self._ict, '_initialized', False):
            try:
                amd = self._ict.get_amd_state()
                ict_ctx.amd_phase = getattr(amd, 'phase', "")
                ict_ctx.amd_bias = getattr(amd, 'bias', "")
                ict_ctx.amd_confidence = getattr(amd, 'confidence', 0.0)
                mb = self._ict.get_market_bias()
                ict_ctx.in_premium = getattr(mb, 'in_premium', False)
                ict_ctx.in_discount = getattr(mb, 'in_discount', False)
                tf_5m = self._ict._tf.get("5m")
                if tf_5m:
                    # BUG FIX: TFStructure uses 'trend' not 'structure'
                    ict_ctx.structure_5m = getattr(tf_5m, 'trend', "ranging")
                    ict_ctx.bos_5m = getattr(tf_5m, 'bos_direction', "")
                    ict_ctx.choch_5m = getattr(tf_5m, 'choch_direction', "")
                tf_15m = self._ict._tf.get("15m")
                if tf_15m:
                    ict_ctx.structure_15m = getattr(tf_15m, 'trend', "ranging")
                # v10: 4H structure for HTF trend analysis
                tf_4h = self._ict._tf.get("4h")
                if tf_4h:
                    ict_ctx.structure_4h = getattr(tf_4h, 'trend', "ranging")
                # v10: Dealing range position
                _dr = getattr(self._ict, '_dealing_range', None)
                if _dr:
                    ict_ctx.dealing_range_pd = getattr(_dr, 'current_pd', 0.5)
                else:
                    # Fallback: use 15m premium/discount
                    if tf_15m:
                        ict_ctx.dealing_range_pd = getattr(tf_15m, 'premium_discount', 0.5)
                try:
                    ob_sl_long = self._ict.get_ob_sl_level("long", price, atr, now_ms)
                    if ob_sl_long:
                        ict_ctx.nearest_ob_price = ob_sl_long
                except Exception:
                    pass
                # BUG-FIX-CRITICAL: Also fetch SHORT-side OB (above price).
                # Without this, _compute_sl in entry_engine ALWAYS returned None
                # for shorts because nearest_ob_price was always below price.
                try:
                    ob_sl_short = self._ict.get_ob_sl_level("short", price, atr, now_ms)
                    if ob_sl_short:
                        ict_ctx.nearest_ob_price_short = ob_sl_short
                except Exception:
                    pass
                try:
                    sess = self._ict.get_amd_session_context(now_ms)
                    ict_ctx.kill_zone = sess.get("session", "")
                except Exception:
                    pass
            except Exception as e:
                logger.debug(f"ICT context build error: {e}")

        # Step 6b: Bridge ICT sweeps into entry engine context
        # The ICT engine detects sweeps on its own liquidity_pools via
        # _detect_sweeps(). These sweeps are INVISIBLE to the LiquidityMap's
        # check_sweeps() because the two systems track separate pool registries.
        # Without this bridge, 34+ ICT sweeps per session are completely lost
        # and the post-sweep pipeline never fires.
        if (self._ict is not None
                and hasattr(self._ict, 'liquidity_pools')
                and _ENTRY_ENGINE_AVAILABLE):
            try:
                _sweep_age_limit = now_ms - 30_000  # sweeps from last 30s
                for pool in self._ict.liquidity_pools:
                    if pool.swept and pool.sweep_timestamp > _sweep_age_limit:
                        c5 = candles_by_tf.get("5m", [])
                        _ch = float(c5[-2]['h']) if len(c5) >= 2 else price
                        _cl = float(c5[-2]['l']) if len(c5) >= 2 else price
                        _cc = float(c5[-2]['c']) if len(c5) >= 2 else price
                        ict_ctx.ict_sweeps.append(ICTSweepEvent(
                            pool_price=pool.price,
                            pool_type=pool.level_type,
                            sweep_ts=pool.sweep_timestamp,
                            displacement=pool.displacement_confirmed,
                            disp_score=pool.displacement_score,
                            wick_reject=pool.wick_rejection,
                            candle_high=_ch,
                            candle_low=_cl,
                            candle_close=_cc,
                        ))
            except Exception as e:
                logger.debug(f"ICT sweep bridge error: {e}")

        # Step 7: Feed to entry engine
        self._entry_engine.update(
            liq_snapshot=liq_snapshot,
            flow_state=flow_state,
            ict_ctx=ict_ctx,
            price=price, atr=atr, now=now,
            candles_1m=candles_by_tf.get("1m"),
            candles_5m=candles_by_tf.get("5m"),
        )

        # Step 8: Check for signal and execute
        signal = self._entry_engine.get_signal()
        if signal is not None:
            bal_info = risk_manager.get_available_balance()
            total_bal = float((bal_info or {}).get("total", 0))
            allowed, reason = self._risk_gate.can_trade(total_bal)
            if not allowed:
                logger.info(f"Signal blocked by risk manager: {reason}")
                self._entry_engine.consume_signal()
                return

            logger.info(
                f"SIGNAL: {signal.entry_type.value} {signal.side.upper()} "
                f"@ ${signal.entry_price:,.1f} | "
                f"SL=${signal.sl_price:,.1f} TP=${signal.tp_price:,.1f} "
                f"R:R={signal.rr_ratio:.1f} | {signal.reason}")

            _min_sig = self._last_sig if self._last_sig is not None else SignalBreakdown()
            _min_sig.atr = atr

            self._force_sl = signal.sl_price
            self._force_tp = signal.tp_price

            _tier_map = {
                EntryType.SWEEP_REVERSAL: "S",
                EntryType.PRE_SWEEP_APPROACH: "A",
                EntryType.SWEEP_CONTINUATION: "B",
                EntryType.DISPLACEMENT_MOMENTUM: "A",
            }
            _tier = _tier_map.get(signal.entry_type, "A")

            # v9: capture full EntrySignal before the thread consumes it
            self._last_entry_signal = signal

            self._launch_entry_async(
                data_manager, order_manager, risk_manager,
                side=signal.side, sig=_min_sig,
                mode=signal.entry_type.value.lower(),
                ict_tier=_tier,
            )
            self._entry_engine.consume_signal()
            self._entry_engine.on_entry_placed()

        # Step 9: Periodic thinking log — institutional context
        if now - self._last_think_log_v2 >= 30.0:
            self._last_think_log_v2 = now
            state = self._entry_engine.state
            flow_dir = flow_state.direction or "neutral"
            conv = flow_state.conviction

            # Core state
            parts = [f"State={state}", f"Flow={flow_dir}({conv:+.2f})",
                     f"CVD={cvd_trend:+.2f}"]

            # Target
            if liq_snapshot.primary_target:
                t = liq_snapshot.primary_target
                parts.append(f"Target={t.direction}->${t.pool.price:,.0f}"
                             f"({t.distance_atr:.1f}ATR)")

            # AMD + session
            if ict_ctx.amd_phase:
                parts.append(f"AMD={ict_ctx.amd_phase[:4]}")
            if ict_ctx.amd_bias:
                parts.append(f"Bias={ict_ctx.amd_bias[:4]}")

            # Pool distances
            parts.append(f"BSL={liq_snapshot.nearest_bsl_atr:.1f}ATR")
            parts.append(f"SSL={liq_snapshot.nearest_ssl_atr:.1f}ATR")

            # Structure
            _s15 = ""
            _s4h = ""
            if self._ict:
                try:
                    _tf = getattr(self._ict, '_tf', {})
                    if '15m' in _tf: _s15 = getattr(_tf['15m'], 'trend', '')
                    if '4h' in _tf:  _s4h = getattr(_tf['4h'], 'trend', '')
                except Exception:
                    pass
            if _s15 or _s4h:
                parts.append(f"Struct=15m:{_s15 or '?'}/4H:{_s4h or '?'}")

            # Dealing range
            _dr_pd = getattr(ict_ctx, 'dealing_range_pd', 0.5)
            pd_l = ("DD" if _dr_pd < 0.25 else "D" if _dr_pd < 0.40 else
                    "EQ" if _dr_pd < 0.60 else "P" if _dr_pd < 0.75 else "DP")
            parts.append(f"DR={pd_l}({_dr_pd:.0%})")

            # Session
            if ict_ctx.kill_zone:
                parts.append(f"KZ={ict_ctx.kill_zone}")

            # Tracking
            tracking = self._entry_engine.tracking_info
            if tracking:
                parts.append(f"Track={tracking['direction']}->{tracking['target']}")

            # Sweep analysis score (if in POST_SWEEP)
            if state == "POST_SWEEP":
                _sa = getattr(self._entry_engine, '_last_sweep_analysis', None)
                if _sa:
                    rs = _sa.get('rev_score', 0)
                    cs = _sa.get('cont_score', 0)
                    parts.append(f"SweepScore=R{rs:.0f}/C{cs:.0f}")

            logger.info(f"[THINK] {' | '.join(parts)}")


    def _evaluate_entry_legacy(self, *args, **kwargs):
        """Removed — v9 LiquidityMap + EntryEngine is always used."""
        logger.warning("_evaluate_entry_legacy called but has been removed; ensure EntryEngine and LiquidityMap are available")

    
    def _evaluate_hunt_entry(
        self,
        data_manager,
        order_manager,
        risk_manager,
        sig,
        price:       float,
        now:         float,
        hunt_signal,          # hunt signal (side/sl/tp/rr/pool prices)
    ) -> None:
        """
        v5.2 — Liquidity Hunt Entry.

        Triggered when a BSL or SSL sweep signal is confirmed by the ICT engine
        and the trade to the opposing pool meets the minimum R:R gate.

        Entry rules:
          • ATR regime must be valid (not extreme volatility)
          • HTF structure must not strongly oppose the hunt direction
          • Scenario-adaptive confirm ticks (1-3 based on post-sweep prediction)

        SL/TP sourced from the hunt_signal object (pre-computed by EntryEngine):
          SL  = behind swept pool wick + 0.5×ATR buffer
          TP  = opposing pool level − 0.1×ATR buffer
        """
        side = hunt_signal.side   # "long" | "short"
        price = data_manager.get_last_price()

        # ── Scenario prediction (drives confirm count + timing) ──────────
        # Before ANY gate, query ICT for post-hunt scenario. This determines
        # whether we enter immediately (CONTINUATION), at OTE (REVERSAL), or
        # after pullback confirmation (PULLBACK_CONT).
        _hunt_scenario     = "REVERSAL"    # ICT default is OTE after sweep
        _hunt_scenario_conf = 0.0
        _hunt_timing        = "WAIT_OTE"
        _hunt_opposing_pool = hunt_signal.target_pool_price
        _hunt_swept_pool    = hunt_signal.swept_pool_price

        if self._ict is not None and hasattr(self._ict, 'get_hunt_scenario'):
            try:
                _atr_h   = getattr(self, '_atr_val', None) or 1.0
                _now_ms_h = int(now * 1000) if now < 1e12 else int(now)
                _sc = self._ict.get_hunt_scenario(price, _atr_h, _now_ms_h)
                _hunt_scenario      = _sc.get("scenario", "REVERSAL")
                _hunt_scenario_conf = _sc.get("confidence", 0.0)
                _hunt_timing        = _sc.get("entry_timing", "WAIT_OTE")
                logger.info(
                    f"🎣 HUNT SCENARIO: {_hunt_scenario} "
                    f"(conf={_hunt_scenario_conf:.2f}) timing={_hunt_timing} | "
                    f"{_sc.get('details','')}"
                )
            except Exception as _e:
                logger.debug(f"Hunt scenario error: {_e}")

        # Also check if we have an updated opposing pool from the ICT engine
        if self._ict is not None:
            try:
                _hp = getattr(self._ict, '_last_hunt_pred', {}) or {}
                if _hp.get('opposing_pool') is not None:
                    _hunt_opposing_pool = _hp['opposing_pool']
                    # Update the hunt signal TP to match ICT engine's target.
                    # BUG-4 FIX: The original code used type(hunt_signal)(**{**hunt_signal.__dict__, …})
                    # dataclasses.replace() preserves created_at and any future fields.
                    if ((side == "long"  and _hunt_opposing_pool > price) or
                            (side == "short" and _hunt_opposing_pool < price)):
                        hunt_signal = dataclasses.replace(
                            hunt_signal,
                            tp_price          = _hunt_opposing_pool,
                            target_pool_price = _hunt_opposing_pool,
                        )
            except Exception:
                pass

        # Gate 1: ATR regime
        if not sig.regime_ok:
            logger.debug("🎣 Hunt gate: ATR regime invalid — skipping")
            return

        # Gate 2: HTF must not be strongly opposed (informational threshold only)
        if self._htf.vetoes_trade(side):
            # HTF veto in hunt is soft — only block when score is extreme
            _htf_block = (
                (side == "long"  and self._htf.trend_15m < -0.50) or
                (side == "short" and self._htf.trend_15m > +0.50)
            )
            if _htf_block:
                logger.info(
                    f"🎣 Hunt gate: HTF strongly opposes {side.upper()} "
                    f"(15m={self._htf.trend_15m:+.2f}) — skipping sweep entry")
                return

        # Gate 3: Scenario-adaptive confirm ticks
        #
        # The number of confirm ticks is determined by the post-hunt scenario:
        #   CONTINUATION  (price blasting through, no retrace expected): 1 tick
        #   REVERSAL      (classic ICT sweep-and-go, OTE retrace):        2 ticks
        #   PULLBACK_CONT (retrace first, then continue):                  3 ticks
        #   UNCERTAIN     (no strong prediction):                          2 ticks
        #
        # High scenario confidence reduces the count by 1 (floor 1).
        # Uses dedicated _confirm_hunt_long / _confirm_hunt_short counters
        # (Bug-C fix) so that ICT OTE routing — which resets _confirm_long /
        # _confirm_short on every tick it fires — cannot interrupt hunt
        # confirmation progress.
        _cn_base = {"CONTINUATION": 1, "REVERSAL": 2,
                    "PULLBACK_CONT": 3, "UNCERTAIN": 2}.get(_hunt_scenario, 2)
        # High confidence reduces requirement by 1 (never below 1)
        if _hunt_scenario_conf >= 0.70:
            _cn_base = max(1, _cn_base - 1)
        _cn = _cn_base

        if side == "long":
            self._confirm_hunt_long += 1
            self._confirm_hunt_short = 0
            if self._confirm_hunt_long < _cn:
                logger.debug(
                    f"🎣 Hunt confirm {self._confirm_hunt_long}/{_cn} "
                    f"({_hunt_scenario} conf={_hunt_scenario_conf:.2f})")
                return
        else:
            self._confirm_hunt_short += 1
            self._confirm_hunt_long = 0
            if self._confirm_hunt_short < _cn:
                logger.debug(
                    f"🎣 Hunt confirm {self._confirm_hunt_short}/{_cn} "
                    f"({_hunt_scenario} conf={_hunt_scenario_conf:.2f})")
                return

        # ── CONFIRMED — store signal and launch entry ────────────────────
        self._confirm_hunt_long = self._confirm_hunt_short = 0
        self._pending_hunt_signal = hunt_signal


        swept_label = (
            f"SSL@${hunt_signal.swept_pool_price:,.0f}"
            if side == "long"
            else f"BSL@${hunt_signal.swept_pool_price:,.0f}"
        )
        # Determine ICT tier from scenario confidence
        # CONTINUATION + high conf = Tier-S equivalent (immediate, tight SL)
        # REVERSAL = Tier-A (OTE is the entry, institutional sweep confirmed)
        # PULLBACK_CONT = Tier-B equivalent (need pullback confirmation)
        _hunt_ict_tier = "A"
        if _hunt_scenario == "CONTINUATION" and _hunt_scenario_conf >= 0.65:
            _hunt_ict_tier = "S"
        elif _hunt_scenario == "PULLBACK_CONT":
            _hunt_ict_tier = "B"

        logger.info(
            f"🎣 HUNT ENTRY {side.upper()} confirmed  "
            f"swept={swept_label}  "
            f"target=${hunt_signal.target_pool_price:,.0f}  "
            f"SL=${hunt_signal.sl_price:,.1f}  "
            f"TP=${hunt_signal.tp_price:,.1f}  "
            f"R:R=1:{hunt_signal.rr:.2f}  "
            f"pred_score={hunt_signal.prediction_score:+.3f}  "
            f"scenario={_hunt_scenario}(conf={_hunt_scenario_conf:.2f})  "
            f"tier={_hunt_ict_tier}"
        )
        self._launch_entry_async(
            data_manager, order_manager, risk_manager,
            side, sig, mode="hunt", ict_tier=_hunt_ict_tier
        )

    def _evaluate_reversion_entry(self, data_manager, order_manager, risk_manager, sig, price, now):
        """
        v5.0 — ICT-Commander / Quant-Scout Entry Evaluation.

        PATH SPLIT:
          ICT SWEEP PATH (Tier-S/A): ICT structure defines the trade.
            • Entry side = sweep_setup.side (NOT VWAP reversion side)
            • VWAP overextension NOT required (price is at OTE, not VWAP extreme)
            • n_confirming NOT required (ICT sweep is the signal)
            • HTF is informational only — sweep geometry defines direction
            • Quant signals used as soft confirmation only:
              - Tick flow not STRONGLY opposing (−0.30 threshold)
              - ATR regime not extreme
            • Confirm counter uses SWEEP SIDE, not VWAP side

          STANDARD QUANT PATH (Tier-B): Order flow + ICT confluence.
            • Gates: overextended, regime_ok, n_confirming ≥ 3, composite ≥ 0.30
            • HTF: informational only — NOT a gate (removed by design)
            • P/D zone gate active (long not in 4H premium, short not in discount)
            • ICT floor: regime-adaptive (0.12 RANGING / 0.20 TRANSITIONING / 0.35 TRENDING)
            • Confirm counter uses VWAP reversion side
        """
        c    = sig.composite
        thr  = QCfg.COMPOSITE_ENTRY_MIN()

        # ── BUG-A FIX: Override `side` to sweep_setup.side for sweep entries ─
        # The docstring stated "overridden for sweep path" but this override was
        # never implemented.  Consequence: every sweep OTE_READY entry used the
        # VWAP reversion side (often the OPPOSITE of the sweep direction), causing
        # three cascading failures:
        #
        #   1. allows_reversion() vetoed the sweep: a LONG sweep with price above
        #      VWAP gives side="short"; TRENDING_UP then blocked it outright.
        #
        #   2. _ict_opposes_side triggered and zeroed sig.ict_total for the LONG
        #      sweep's confluence scores, because ICT direction (+1 LONG) opposed
        #      the VWAP side ("short").  Gate then saw ict_total=0.0.
        #
        #   3. ICTEntryGate.evaluate("short", ..., sweep_setup.side="long") failed
        #      Tier-S (sweep_setup.side != side) and hit MANIP hard-block.
        #
        # Fix: detect OTE_READY sweep setup and switch `side` to sweep direction
        # BEFORE any gate evaluation. Regime and breakout veto are skipped for
        # sweep entries — they are DIRECTIONAL institutional setups, not fades.
        _is_ote_sweep = (
            _ICT_TRADE_ENGINE_AVAILABLE and
            self._active_sweep_setup is not None and
            self._active_sweep_setup.status == "OTE_READY"
        )
        if _is_ote_sweep:
            side = self._active_sweep_setup.side  # "long" | "short"
        else:
            side = sig.reversion_side   # VWAP-based direction for standard reversion path

        # ── Hard veto: trending against reversion trade ──────────────────
        # Skipped for sweep entries — institutional sweep setups are valid IN
        # any regime; the AMD phase + ICT gate is the structural filter here.
        if not _is_ote_sweep and not self._regime.allows_reversion(side):
            self._confirm_long = self._confirm_short = 0; return

        # ── v6.0 MTF DIRECTIONAL OVERRIDE ─────────────────────────────────
        # INSTITUTIONAL PRINCIPLE: don't fade 3+ aligned timeframes.
        # When 1D, 1H, 15M are all bullish and you're trying to SHORT, you're
        # fighting the entire institutional order flow. The composite may say
        # SHORT because VWAP deviation = -1.0 (distance, not selling), but
        # selling into 3+ bullish TFs is how retail gets destroyed.
        #
        # Skipped for sweep entries — sweeps are institutional setups that
        # can be counter-trend by design (sweep of SSL in a downtrend).
        if not _is_ote_sweep:
            try:
                _trend_scores = []
                if hasattr(sig, 'trend_1d'): _trend_scores.append(sig.trend_1d)
                if hasattr(sig, 'trend_4h'): _trend_scores.append(sig.trend_4h)
                if hasattr(sig, 'trend_1h'): _trend_scores.append(sig.trend_1h)
                if hasattr(sig, 'trend_15m'): _trend_scores.append(sig.trend_15m)
                # Fallback: use HTF filter if individual trend scores not available
                if not _trend_scores and hasattr(self, '_htf'):
                    _h4 = self._htf.trend_4h
                    _h15 = self._htf.trend_15m
                    if side == "short" and _h4 > 0.3 and _h15 > 0.3:
                        self._confirm_long = self._confirm_short = 0; return
                    if side == "long" and _h4 < -0.3 and _h15 < -0.3:
                        self._confirm_long = self._confirm_short = 0; return
                elif _trend_scores:
                    _bull_count = sum(1 for s in _trend_scores if s > 0.3)
                    _bear_count = sum(1 for s in _trend_scores if s < -0.3)
                    if side == "short" and _bull_count >= 3:
                        self._confirm_long = self._confirm_short = 0; return
                    if side == "long" and _bear_count >= 3:
                        self._confirm_long = self._confirm_short = 0; return
            except Exception:
                pass

        # ── Breakout veto: never fade a detected breakout ─────────────────
        # Skipped for sweep entries — a LONG sweep entry WITH the breakout
        # direction is not a fade; blocks_reversion only protects counter-trend.
        if not _is_ote_sweep and self._breakout.blocks_reversion(side):
            self._confirm_long = self._confirm_short = 0
            if now - self._last_bo_block_log >= 60.0:
                self._last_bo_block_log = now
                logger.info(f"🚫 Breakout blocks {side.upper()} reversion ({self._breakout.direction})")
            return

        # ── Build quant helper signals for ICTEntryGate ───────────────────
        quant_helpers = self._get_quant_helpers(sig, side)

        # ── C FIX: Re-compute ICT confluence for the entry side when it was ──
        # evaluated for the OPPOSITE direction in _evaluate_entry.
        #
        # _evaluate_entry computes ICT confluence for `ict_side` (driven by AMD
        # bias or dominant OB structure).  When AMD is bearish and VWAP says LONG,
        # ict_side="short" and sig.ict_total=0.53 is SHORT confluence strength.
        # Passing that into ICTEntryGate.evaluate("long",...) is structurally wrong:
        #   • SHORT strength 0.65 looks like strong LONG ICT support to the gate
        #   • Gate can pass a LONG with zero real ICT backing
        #   • Conversely, a high SHORT score blocks LONG for wrong reasons
        #
        # For sweep entries, `side` is now sweep_setup.side. If ict_direction
        # (AMD bias) disagrees with the sweep side (shouldn't happen — sweep
        # detector requires AMD bias to match sweep direction), still recompute
        # so the gate always receives correctly-directed scores.
        #
        # Bug-2 fix: the original code added a signed boost in _evaluate_entry()
        # using the AMD/ICT side (e.g. LONG) and then did NOT reverse it when
        # evaluating the VWAP reversion side (e.g. SHORT). The ICT LONG boost
        # (+0.086 to +0.108) was persistently making sig.composite LESS negative,
        # so SHORT entries consistently fell short of the ±0.350 composite gate.
        # Fix: when the ICT side opposes `side`, un-apply the original boost and
        # apply a correctly-directed one for the actual entry side being evaluated.
        _ict_dir = sig.ict_direction   # +1.0=long, -1.0=short, 0.0=unset (B1 field)
        _ict_opposes_side = (
            (_ict_dir < 0 and side == "long") or
            (_ict_dir > 0 and side == "short")
        )
        if (_ict_opposes_side and
                self._ict is not None and self._ict._initialized):
            try:
                _now_ms_rg = int(now * 1000) if now < 1e12 else int(now)
                _recomp    = self._ict.get_confluence(side, price, _now_ms_rg,
                                                       atr=sig.atr)
                sig.ict_ob      = _recomp.ob_score
                sig.ict_fvg     = _recomp.fvg_score
                sig.ict_sweep   = _recomp.sweep_score
                sig.ict_session = _recomp.session_score
                sig.ict_total   = _recomp.total
                sig.ict_details = _recomp.details
                # ADVANCED FIELD PROPAGATION FIX: original code updated only the
                # 6 basic scores. All 12 advanced fields remained from the primary
                # (ICT-side) computation, so gate decisions used correct totals
                # but display fields (Stack, RevRisk, RevZone) reflected the wrong
                # side. Propagate the full recomputed result now.
                sig.ict_mtf_ob_count      = _recomp.mtf_ob_count
                sig.ict_fvg_stack_count   = _recomp.fvg_stack_count
                sig.ict_htf_reversal_risk = _recomp.htf_reversal_risk
                sig.ict_pd_grade          = _recomp.pd_grade
                sig.ict_pd_matrix         = _recomp.pd_matrix
                sig.ict_judas_active      = _recomp.judas_swing_active
                sig.ict_nearest_bsl_atr   = _recomp.nearest_bsl_dist_atr
                sig.ict_nearest_ssl_atr   = _recomp.nearest_ssl_dist_atr
                if _recomp.delivery_target:
                    sig.ict_delivery_target = _recomp.delivery_target
                    sig.ict_delivery_conf   = _recomp.delivery_confidence
                # HTF reversal zone: recompute for the new side
                if sig.atr > 0:
                    try:
                        _rz = self._ict.get_htf_reversal_zones(price, sig.atr, _now_ms_rg)
                        sig.ict_htf_rev_zone_near = any(
                            z["dist_atr"] < 1.0 and z["direction"] != side
                            for z in _rz)
                    except Exception:
                        pass
                # Bug-2 fix: un-apply the original opposite-direction boost and
                # substitute a correctly-directed one for `side`.  Without this the
                # original LONG boost (e.g. +0.097) stays embedded in sig.composite
                # even when evaluating a SHORT entry, making the composite 0.19 less
                # negative than the raw order-flow signals warrant and systematically
                # blocking SHORT entries that would otherwise clear the ±0.350 gate.
                # FIX 7c: use stored raw composite (pre-boost) instead of
                # un-applying the boost from the clamped value. The clamped
                # composite makes un-apply give wrong results for SHORT entries
                # (composite 0.05–0.15 less negative → systematically below ±0.30 gate).
                _new_dir     = 1.0 if side == "long" else -1.0
                _new_boost   = _recomp.total * 0.15 * _new_dir
                _old_boost   = sig.ict_boost_signed  # snapshot before overwrite
                _raw_comp    = getattr(sig, '_raw_composite', sig.composite - sig.ict_boost_signed)
                sig.composite        = max(-1.0, min(1.0, _raw_comp + _new_boost))
                sig.ict_boost_signed = _new_boost
                sig.ict_direction    = _new_dir
                _prev_dir_lbl = "SHORT" if _ict_dir < 0 else "LONG"
                logger.debug(
                    f"🔄 ICT re-computed for {side.upper()} "
                    f"(was {_prev_dir_lbl}): "
                    f"Σ={_recomp.total:.2f} "
                    f"[OB={_recomp.ob_score:.2f} "
                    f"FVG={_recomp.fvg_score:.2f}] "
                    f"boost_corrected: {_old_boost:+.3f} → {_new_boost:+.3f} "
                    f"composite_now={sig.composite:+.4f}")
            except Exception as _rce:
                logger.debug(f"ICT side-recompute error (non-fatal): {_rce}")
                # Zero out so gate cannot mistake opposite-side scores as support
                sig.ict_total = 0.0

        # ── ICT Tier-Based Gate ───────────────────────────────────────────
        _effective_cn = QCfg.CONFIRM_TICKS()   # default; overridden by tier
        _tier         = "BLOCKED"
        _tier_reason  = "no_ict"
        _is_sweep_path = False

        # ── v8.0: Advanced ICT pre-gates (applied before ICTEntryGate) ───
        # HTF reversal zone proximity: if a high-conviction opposing HTF OB+FVG
        # zone is within 1 ATR, price is likely to stall or reverse there.
        # Only block Tier-B entries — sweep setups (Tier-S/A) have their own
        # structural invalidation via the OTE zone.
        #
        # ADX-ADAPTIVE THRESHOLD: In ranging markets (ADX < 25), opposing OBs
        # within 1 ATR are NORMAL — they are the range boundaries that price
        # bounces between.  RevRisk 0.70-0.85 is typical ranging structure,
        # not a genuine reversal threat.  Only extreme risk (>0.90) blocks.
        # In trending markets (ADX >= 25), price is leaving these zones behind
        # so even moderate risk (>0.55) is meaningful.
        _adx_val = getattr(sig, 'adx', 25.0)
        _rev_zone_threshold = 0.90 if _adx_val < 25.0 else 0.55
        if (sig.ict_htf_rev_zone_near and
                not _is_ote_sweep and
                sig.ict_htf_reversal_risk > _rev_zone_threshold):
            if now - self._last_ict_gate_log >= 30.0:
                self._last_ict_gate_log = now
                logger.info(
                    f"⛔ HTF_REV_ZONE [{side.upper()}]: "
                    f"opposing reversal zone <1ATR (risk={sig.ict_htf_reversal_risk:.2f})")
            self._confirm_long = self._confirm_short = 0
            return

        if (self._ict is not None and self._ict._initialized and
                _ICT_TRADE_ENGINE_AVAILABLE):
            try:
                _tier, _cn_override, _tier_reason = ICTEntryGate.evaluate(
                    side, sig, self._active_sweep_setup, price, quant_helpers,
                    mode="reversion", market_regime=sig.market_regime,
                    ict_engine=self._ict)

                if _tier == "BLOCKED":
                    if self._ict_gate_start_time_rev == 0.0:
                        self._ict_gate_start_time_rev = now
                    if now - self._last_ict_gate_log >= 30.0:
                        self._last_ict_gate_log = now
                        logger.info(
                            f"⛔ ICTEntryGate BLOCKED [{side.upper()}]: {_tier_reason}")
                    if not self._ict_gate_alerted_rev and (now - self._ict_gate_start_time_rev) >= 900.0:
                        self._ict_gate_alerted_rev = True
                        send_telegram_message(
                            f"⛔ <b>ICT GATE — 15 MIN BLOCK</b>\n"
                            f"No ICT confluence for ≥15 min.\n"
                            f"Side: {side.upper()} | Reason: {_tier_reason}\n"
                            f"HTF: 15m={self._htf.trend_15m:+.2f}  "
                            f"4H={self._htf.trend_4h:+.2f}  "
                            f"(info-only — HTF is not a gate)\n"
                            f"<i>Bot alive — waiting for institutional setup.</i>")
                    self._confirm_long = self._confirm_short = 0
                    return

                self._ict_gate_start_time_rev = 0.0; self._ict_gate_alerted_rev = False
                _effective_cn  = _cn_override

                # ── Tier-L: Liquidity-Hunt Driven entry ──────────────────
                # Tier-L fires when predict_next_hunt() is very confident even
                # without a formal ICTSweepSetup. Route via the hunt path for
                # proper SL/TP using the opposing liquidity pool as target.
                if _tier == "L":
                    logger.info(
                        f"🎣 TIER-L LIQUIDITY HUNT [{side.upper()}]: "
                        f"{_tier_reason}")
                    # Build a minimal hunt signal from ICT engine data
                    if self._ict is not None:
                        try:
                            _hp_l    = getattr(self._ict, '_last_hunt_pred', {}) or {}
                            _opp_l   = _hp_l.get('opposing_pool')
                            _swept_l = _hp_l.get('swept_pool')
                            _atr_l   = getattr(self, '_atr_val', None) or 1.0
                            _now_l   = int(now * 1000) if now < 1e12 else int(now)
                            if _opp_l is not None and _swept_l is not None:
                                # ict_trade_engine is no longer a separate module;
                                # ICTSLEngine / ICTTPEngine are inlined into quant_strategy.
                                # The Tier-L path without a formal sweep setup falls through
                                # to Tier-A entry which uses the existing SL/TP pipeline.
                                pass
                        except Exception as _le:
                            logger.debug(f"Tier-L build error: {_le}")
                    # Fallback if we can't build the hunt signal: treat as Tier-A
                    _tier = "A"

                _is_sweep_path = (_tier in ("S", "A") and
                                  self._active_sweep_setup is not None and
                                  self._active_sweep_setup.status == "OTE_READY")

                logger.debug(
                    f"✅ Tier-{_tier} [{side.upper()}]: {_tier_reason} | "
                    f"cn={_cn_override} sweep_path={_is_sweep_path}")

            except Exception as _ieg_e:
                logger.debug(f"ICTEntryGate error (non-fatal): {_ieg_e}")
                # Fall through to legacy path below

        elif self._ict is not None and self._ict._initialized:
            # ── Legacy ICT gate (no ict_trade_engine) ────────────────────
            _ict_min_base = float(getattr(config, 'ICT_MIN_SCORE_FOR_ENTRY', 0.45))
            _ict_min_ob   = float(getattr(config, 'ICT_OB_MIN_SCORE_FOR_ENTRY', 0.35))
            _ict_min = _ict_min_ob if sig.ict_ob >= 0.55 else _ict_min_base
            if sig.ict_total < _ict_min:
                if self._ict_gate_start_time_rev == 0.0:
                    self._ict_gate_start_time_rev = now
                if now - self._last_ict_gate_log >= 30.0:
                    self._last_ict_gate_log = now
                    logger.info(
                        f"⛔ ICT gate [{side.upper()} REVERSION]: "
                        f"score={sig.ict_total:.2f} < min={_ict_min:.2f} [{sig.ict_details}]")
                if not self._ict_gate_alerted_rev and (now - self._ict_gate_start_time_rev) >= 900.0:
                    self._ict_gate_alerted_rev = True
                    send_telegram_message(
                        f"⛔ <b>ICT GATE — 15 MIN BLOCK</b>\n"
                        f"Score: {sig.ict_total:.2f} (min={_ict_min:.2f})\n"
                        f"{sig.ict_details}")
                self._confirm_long = self._confirm_short = 0
                return
            self._ict_gate_start_time_rev = 0.0; self._ict_gate_alerted_rev = False

        # ══════════════════════════════════════════════════════════════════
        # PATH SPLIT: ICT SWEEP vs STANDARD QUANT
        # ══════════════════════════════════════════════════════════════════

        if _is_sweep_path:
            # ── ICT SWEEP PATH (Tier-S / Tier-A) ─────────────────────────
            # ICT structure is the commander.
            # Quant helpers are already embedded in ICTEntryGate.evaluate().
            # Entry side = SWEEP SETUP SIDE (not VWAP reversion side).
            entry_side = self._active_sweep_setup.side

            # ATR regime is the only hard quant gate here
            # (already checked in ICTEntryGate, but double-check)
            if not sig.regime_ok:
                self._confirm_long = self._confirm_short = 0; return

            # Confirm counter uses SWEEP SIDE
            if entry_side == "long":
                self._confirm_long += 1; self._confirm_short = 0
                if self._confirm_long >= _effective_cn:
                    self._confirm_long = self._confirm_short = 0
                    logger.info(
                        f"🏛️ ICT SWEEP LONG Tier-{_tier} confirmed "
                        f"(cn={_effective_cn} AMD={sig.amd_phase} "
                        f"OTE=[${self._active_sweep_setup.ote_entry_zone_low:.0f}–"
                        f"${self._active_sweep_setup.ote_entry_zone_high:.0f}])")
                    self._launch_entry_async(
                        data_manager, order_manager, risk_manager,
                        "long", sig, mode="reversion", ict_tier=_tier)
            else:
                self._confirm_short += 1; self._confirm_long = 0
                if self._confirm_short >= _effective_cn:
                    self._confirm_long = self._confirm_short = 0
                    logger.info(
                        f"🏛️ ICT SWEEP SHORT Tier-{_tier} confirmed "
                        f"(cn={_effective_cn} AMD={sig.amd_phase} "
                        f"OTE=[${self._active_sweep_setup.ote_entry_zone_low:.0f}–"
                        f"${self._active_sweep_setup.ote_entry_zone_high:.0f}])")
                    self._launch_entry_async(
                        data_manager, order_manager, risk_manager,
                        "short", sig, mode="reversion", ict_tier=_tier)

        else:
            # ── STANDARD QUANT PATH (Tier-B or no ICT) ───────────────────
            # All legacy quant gates required.
            # Entry side = VWAP reversion side.

            # Full gate stack: overextended + regime + confluence
            # HTF veto REMOVED — direction is encoded by ICT structure + order flow.
            # Keeping HTF here would block VWAP reversion longs when 15m is bearish —
            # exactly the setup the strategy is designed to take.
            if not (sig.overextended and sig.regime_ok and
                    sig.n_confirming >= 3):
                self._confirm_long = self._confirm_short = 0; return

            # Opposite-side cooldown after recent exit
            if self._last_exit_side and self._last_exit_side != side:
                if now - self._last_exit_time < QCfg.COOLDOWN_SEC() * 1.5:
                    return

            # P/D zone gate (only for standard path — sweep path is already gated)
            # B7 FIX: skip entirely in a trending regime.  The PD zone logic is
            # designed for mean-reversion (short from premium, long from discount).
            # In TRENDING_DOWN, price IS in the lower half of the 4H range — that
            # is the delivery zone, not a reason to block the SHORT.  Applying the
            # gate unconditionally blocked the trade precisely where it should fire.
            if self._ict is not None and self._ict._initialized and not self._regime.is_trending():
                _tf4h = self._ict._tf.get("4h")
                _pd4h = _tf4h.premium_discount if _tf4h is not None else 0.5
                if side == "long" and sig.in_premium and not sig.in_discount:
                    if now - self._last_pd_gate_log.get("long", 0) >= 60.0:
                        self._last_pd_gate_log["long"] = now
                        logger.debug(f"📊 PD gate: LONG blocked — 4H premium (pd={_pd4h:.2f})")
                    self._confirm_long = self._confirm_short = 0; return
                if side == "short" and sig.in_discount and not sig.in_premium:
                    if now - self._last_pd_gate_log.get("short", 0) >= 60.0:
                        self._last_pd_gate_log["short"] = now
                        logger.debug(f"📊 PD gate: SHORT blocked — 4H discount (pd={_pd4h:.2f})")
                    self._confirm_long = self._confirm_short = 0; return

            # Composite threshold gate (VWAP direction)
            if side == "long" and c >= thr:
                self._confirm_long += 1; self._confirm_short = 0
            elif side == "short" and c <= -thr:
                self._confirm_short += 1; self._confirm_long = 0
            else:
                self._confirm_long = self._confirm_short = 0; return

            cn = _effective_cn
            if self._confirm_long >= cn:
                self._confirm_long = self._confirm_short = 0
                self._launch_entry_async(
                    data_manager, order_manager, risk_manager,
                    "long", sig, mode="reversion", ict_tier=_tier)
            elif self._confirm_short >= cn:
                self._confirm_long = self._confirm_short = 0
                self._launch_entry_async(
                    data_manager, order_manager, risk_manager,
                    "short", sig, mode="reversion", ict_tier=_tier)

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
        # B5 FIX: each early return now emits a debug log so the real gate
        # blocker is visible instead of the display showing stale reversion info.
        cvd_bias = self._cvd.get_trend_signal()
        if trend_side == "long" and cvd_bias < QCfg.TREND_CVD_MIN():
            logger.debug(
                f"⛔ TREND gate [CVD opposing]: {cvd_bias:+.3f} < {QCfg.TREND_CVD_MIN():.2f}")
            self._confirm_trend_long = self._confirm_trend_short = 0; return
        if trend_side == "short" and cvd_bias > -QCfg.TREND_CVD_MIN():
            logger.debug(
                f"⛔ TREND gate [CVD opposing]: {cvd_bias:+.3f} > {-QCfg.TREND_CVD_MIN():.2f}")
            self._confirm_trend_long = self._confirm_trend_short = 0; return

        # Tick flow must broadly agree with trend direction
        tf = self._tick_eng.get_signal()
        if trend_side == "long" and tf < -0.30:
            logger.debug(f"⛔ TREND gate [tick opposing]: {tf:+.3f} < -0.30")
            self._confirm_trend_long = self._confirm_trend_short = 0; return
        if trend_side == "short" and tf > 0.30:
            logger.debug(f"⛔ TREND gate [tick opposing]: {tf:+.3f} > +0.30")
            self._confirm_trend_long = self._confirm_trend_short = 0; return

        # Composite trend score gate
        if abs(sig.trend_score) < QCfg.TREND_COMPOSITE_MIN():
            logger.debug(
                f"⛔ TREND gate [trend_score weak]: "
                f"|{sig.trend_score:+.3f}| < {QCfg.TREND_COMPOSITE_MIN():.2f}")
            self._confirm_trend_long = self._confirm_trend_short = 0; return
        if trend_side == "long" and sig.trend_score <= 0:
            logger.debug(
                f"⛔ TREND gate [trend_score wrong sign]: "
                f"LONG but score={sig.trend_score:+.3f}")
            self._confirm_trend_long = self._confirm_trend_short = 0; return
        if trend_side == "short" and sig.trend_score >= 0:
            logger.debug(
                f"⛔ TREND gate [trend_score wrong sign]: "
                f"SHORT but score={sig.trend_score:+.3f}")
            self._confirm_trend_long = self._confirm_trend_short = 0; return

        # ── ICT gate for trend entries ───────────────────────────────────────
        _t  = "B"   # Bug-4 fix: declare before conditional block so confirm-
        _cn = QCfg.TREND_CONFIRM_TICKS()  # counter below can reference directly
        _tr = ""
        if _ICT_TRADE_ENGINE_AVAILABLE and self._ict is not None and self._ict._initialized:
            try:
                _qh = self._get_quant_helpers(sig, trend_side)
                _t, _cn, _tr = ICTEntryGate.evaluate(
                    trend_side, sig, None, price, _qh,
                    mode="trend", market_regime=sig.market_regime,
                    ict_engine=self._ict)
                if _t == "BLOCKED":
                    if self._ict_gate_start_time_trend == 0.0:
                        self._ict_gate_start_time_trend = now
                    if now - self._last_ict_gate_log >= 30.0:
                        self._last_ict_gate_log = now
                        logger.info(
                            f"⛔ ICT gate [TREND {trend_side.upper()}]: {_tr}")
                    if not self._ict_gate_alerted_trend and (now - self._ict_gate_start_time_trend) >= 900.0:
                        self._ict_gate_alerted_trend = True
                        send_telegram_message(
                            f"⛔ <b>ICT GATE — 15 MIN BLOCK</b>\n"
                            f"TREND {trend_side.upper()}: {_tr}\n"
                            f"<i>Bot alive — waiting for structure.</i>")
                    self._confirm_trend_long = self._confirm_trend_short = 0
                    return
                self._ict_gate_start_time_trend = 0.0; self._ict_gate_alerted_trend = False
            except Exception as _tge:
                logger.debug(f"Trend ICTEntryGate error (non-fatal): {_tge}")
        elif self._ict is not None and self._ict._initialized:
            # Legacy flat threshold gate
            _ict_min = float(getattr(config, 'ICT_MIN_SCORE_FOR_ENTRY', 0.45))
            if sig.ict_total < _ict_min:
                if self._ict_gate_start_time_trend == 0.0:
                    self._ict_gate_start_time_trend = now
                if now - self._last_ict_gate_log >= 30.0:
                    self._last_ict_gate_log = now
                    logger.info(
                        f"⛔ ICT gate [TREND {trend_side.upper()}]: "
                        f"score={sig.ict_total:.2f} < min={_ict_min:.2f}")
                if not self._ict_gate_alerted_trend and (now - self._ict_gate_start_time_trend) >= 900.0:
                    self._ict_gate_alerted_trend = True
                    send_telegram_message(
                        f"⛔ <b>ICT GATE — 15 MIN BLOCK</b>\n"
                        f"Score: {sig.ict_total:.2f} (min={_ict_min:.2f})\n"
                        f"{sig.ict_details}")
                self._confirm_trend_long = self._confirm_trend_short = 0
                return
            self._ict_gate_start_time_trend = 0.0; self._ict_gate_alerted_trend = False

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
        if not (pb_min <= ema_dist <= pb_max):
            logger.debug(
                f"⛔ TREND gate [EMA pullback out of window]: "
                f"dist={ema_dist/atr:+.2f}ATR "
                f"window=[{pb_min/atr:.2f}–{pb_max/atr:.2f}ATR] "
                f"ema={ema:.2f} price={price:.2f}")
            return

        # Confirmation counter
        if trend_side == "long":
            self._confirm_trend_long += 1; self._confirm_trend_short = 0
        else:
            self._confirm_trend_short += 1; self._confirm_trend_long = 0

        cn = QCfg.TREND_CONFIRM_TICKS()
        if trend_side == "long" and self._confirm_trend_long >= cn:
            self._confirm_trend_long = self._confirm_trend_short = 0
            # Bug-4 fix: _t is initialised to "B" before the gate block above
            _trend_tier = _t
            self._launch_entry_async(data_manager, order_manager, risk_manager, "long", sig, mode="trend", ict_tier=_trend_tier)
        elif trend_side == "short" and self._confirm_trend_short >= cn:
            self._confirm_trend_long = self._confirm_trend_short = 0
            _trend_tier = _t   # Bug-4 fix: always defined, see above
            self._launch_entry_async(data_manager, order_manager, risk_manager, "short", sig, mode="trend", ict_tier=_trend_tier)

    def _evaluate_flow_entry(self, data_manager, order_manager, risk_manager,
                              sig, price, now, flow_side: str):
        """
        v5.1 — ICT Displacement Entry (institutional order flow + structure).

        ICT MODEL: Sweep → Displacement → Distribution.

        Institutions sweep liquidity (BSL/SSL), displace price with aggressive
        volume (the displacement candle creates OBs and FVGs), then distribute
        in the displacement direction. This IS the delivery — not a reaction.

        PRIMARY signal: Recent ICT liquidity sweep with displacement confirmed.
        CONFIRMATION: Order flow (tick + CVD) shows institutional aggression.
        STRUCTURAL: 5m BOS confirms the break.

        The sweet spot is WHERE sweep + displacement + order flow converge:
          BSL swept + disp + bearish BOS + tick < -0.55 + CVD bearish → SHORT
          SSL swept + disp + bullish BOS + tick > +0.55 + CVD bullish → LONG

        SL: Behind the sweep candle (where liquidity was taken — institutional
            orders sit behind that level; if price reclaims it, thesis is dead).
        TP: Nearest opposing liquidity pool (where the next stops are — that's
            where price is being delivered).
        """
        # ── Gate 1: Recent ICT sweep with displacement ────────────────────
        # The sweep must be fresh (<5min) and displacement-confirmed.
        # This is the institutional footprint — not a wick, but a close
        # through the level with body > ATR (displacement).
        _sweep_confirmed = False
        _sweep_pool = None
        _sweep_age_max_ms = 300_000  # 5 minutes

        if self._ict is not None and getattr(self._ict, '_initialized', False):
            try:
                _now_ms = int(now * 1000) if now < 1e12 else int(now)
                for pool in self._ict.liquidity_pools:
                    if not pool.swept or not pool.displacement_confirmed:
                        continue
                    _age = _now_ms - pool.sweep_timestamp
                    if _age > _sweep_age_max_ms or _age < 0:
                        continue
                    # BSL swept + displacement = bearish (smart money sold through buy stops)
                    if pool.level_type == "BSL" and flow_side == "short":
                        _sweep_confirmed = True
                        _sweep_pool = pool
                        break
                    # SSL swept + displacement = bullish (smart money bought through sell stops)
                    elif pool.level_type == "SSL" and flow_side == "long":
                        _sweep_confirmed = True
                        _sweep_pool = pool
                        break
            except Exception:
                pass

        if not _sweep_confirmed:
            # No fresh displacement sweep — fall back to structural-only flow.
            # Still require 5m BOS + extreme order flow, but with wider
            # confirmation (5 ticks instead of 2) since there's no sweep anchor.
            _FLOW_CONFIRM_NO_SWEEP = 5
            if flow_side == "long":
                self._confirm_flow_long += 1
                self._confirm_flow_short = 0
                if self._confirm_flow_long < _FLOW_CONFIRM_NO_SWEEP:
                    return
            else:
                self._confirm_flow_short += 1
                self._confirm_flow_long = 0
                if self._confirm_flow_short < _FLOW_CONFIRM_NO_SWEEP:
                    return

            self._confirm_flow_long = self._confirm_flow_short = 0
            self._last_flow_entry_time = now

            logger.info(
                f"⚡ FLOW DISPLACEMENT (no sweep anchor) — {flow_side.upper()} | "
                f"tick={self._tick_eng.get_signal():+.2f} "
                f"streak={self._flow_tick_streak} | "
                f"CVD_trend={self._cvd.get_trend_signal():+.2f} | "
                f"BOS=5m confirmed")

            # Invalidate conflicting sweep setups
            if (self._active_sweep_setup is not None and
                    self._active_sweep_setup.side != flow_side):
                if self._sweep_detector is not None:
                    self._sweep_detector.invalidate()
                self._active_sweep_setup = None

            self._launch_entry_async(
                data_manager, order_manager, risk_manager,
                flow_side, sig, mode="flow", ict_tier="B")
            return

        # ── Gate 2: AMD phase alignment ───────────────────────────────────
        # AMD should be MANIPULATION (Judas swing in progress) or early
        # DISTRIBUTION (delivery has started). ACCUMULATION = no edge.
        _amd = self._ict.get_amd_state()
        _amd_ok = _amd.phase in ("MANIPULATION", "DISTRIBUTION",
                                   "REDISTRIBUTION", "REACCUMULATION")
        _bias_matches = (
            (flow_side == "short" and _amd.bias == "bearish") or
            (flow_side == "long" and _amd.bias == "bullish") or
            _amd.bias == "neutral"  # neutral doesn't oppose
        )

        if not (_amd_ok or _bias_matches):
            # AMD actively opposes — only proceed if sweep is very strong
            if not (_sweep_pool and _sweep_pool.displacement_confirmed and
                    _sweep_pool.wick_rejection):
                self._confirm_flow_long = self._confirm_flow_short = 0
                return

        # ── Gate 3: Fast confirmation (sweep-backed = 2 ticks) ────────────
        _FLOW_CONFIRM_SWEEP = 2
        if flow_side == "long":
            self._confirm_flow_long += 1
            self._confirm_flow_short = 0
            if self._confirm_flow_long < _FLOW_CONFIRM_SWEEP:
                return
        else:
            self._confirm_flow_short += 1
            self._confirm_flow_long = 0
            if self._confirm_flow_short < _FLOW_CONFIRM_SWEEP:
                return

        # ── CONFIRMED: sweep + displacement + flow + BOS ──────────────────
        self._confirm_flow_long = self._confirm_flow_short = 0
        self._last_flow_entry_time = now

        # Invalidate conflicting sweep setups
        if (self._active_sweep_setup is not None and
                self._active_sweep_setup.side != flow_side):
            logger.info(
                f"🔄 Displacement {flow_side.upper()} invalidates "
                f"{self._active_sweep_setup.side.upper()} sweep setup")
            if self._sweep_detector is not None:
                self._sweep_detector.invalidate()
            self._active_sweep_setup = None

        _sweep_label = (f"{_sweep_pool.level_type}@${_sweep_pool.price:,.0f}"
                        if _sweep_pool else "none")
        logger.info(
            f"⚡ ICT DISPLACEMENT ENTRY — {flow_side.upper()} | "
            f"sweep={_sweep_label} disp=True | "
            f"AMD={_amd.phase}({_amd.bias},conf={_amd.confidence:.2f}) | "
            f"tick={self._tick_eng.get_signal():+.2f} "
            f"streak={self._flow_tick_streak} | "
            f"CVD_trend={self._cvd.get_trend_signal():+.2f}")

        # Tier-A when sweep-backed (institutional footprint confirmed)
        # Tier-B when structure-only (BOS + flow but no sweep anchor)
        _tier = "A" if _sweep_confirmed else "B"

        self._launch_entry_async(
            data_manager, order_manager, risk_manager,
            flow_side, sig, mode="flow", ict_tier=_tier)

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

        # ── Gate 4: ICT structural confluence ───────────────────────────────
        # Momentum retest requires institutional structure in the retest zone.
        # A bounce without OBs/FVGs is noise — use ICTEntryGate for full check.
        _t_mo     = "B"   # Bug-4 fix: declare before conditional block so
        _tr_mo    = ""    # locals() check below always finds them, not dir()
        _cn_mo    = QCfg.CONFIRM_TICKS()
        if _ICT_TRADE_ENGINE_AVAILABLE and self._ict is not None and self._ict._initialized:
            try:
                _qh_mo = self._get_quant_helpers(sig, side)
                _t_mo, _cn_mo, _tr_mo = ICTEntryGate.evaluate(
                    side, sig, self._active_sweep_setup, price, _qh_mo,
                    mode="momentum", market_regime=sig.market_regime,
                    ict_engine=self._ict)
                if _t_mo == "BLOCKED":
                    if self._ict_gate_start_time_mom == 0.0:
                        self._ict_gate_start_time_mom = now
                    if now - self._last_ict_gate_log >= 30.0:
                        self._last_ict_gate_log = now
                        logger.info(
                            f"⛔ ICT gate [MOMENTUM {side.upper()}]: {_tr_mo}")
                    if not self._ict_gate_alerted_mom and (now - self._ict_gate_start_time_mom) >= 900.0:
                        self._ict_gate_alerted_mom = True
                        send_telegram_message(
                            f"⛔ <b>ICT GATE — 15 MIN BLOCK</b>\n"
                            f"MOMENTUM {side.upper()}: {_tr_mo}")
                    self._confirm_trend_long = self._confirm_trend_short = 0
                    return
                self._ict_gate_start_time_mom = 0.0; self._ict_gate_alerted_mom = False
            except Exception as _mge:
                logger.debug(f"Momentum ICTEntryGate error (non-fatal): {_mge}")
        elif self._ict is not None and self._ict._initialized:
            _ict_min = float(getattr(config, 'ICT_MIN_SCORE_FOR_ENTRY', 0.45))
            if sig.ict_total < _ict_min:
                if self._ict_gate_start_time_mom == 0.0:
                    self._ict_gate_start_time_mom = now
                if now - self._last_ict_gate_log >= 30.0:
                    self._last_ict_gate_log = now
                    logger.info(
                        f"⛔ ICT gate [MOMENTUM {side.upper()}]: "
                        f"score={sig.ict_total:.2f} < min={_ict_min:.2f}")
                if not self._ict_gate_alerted_mom and (now - self._ict_gate_start_time_mom) >= 900.0:
                    self._ict_gate_alerted_mom = True
                    send_telegram_message(
                        f"⛔ <b>ICT GATE — 15 MIN BLOCK</b>\n"
                        f"Score: {sig.ict_total:.2f} (min={_ict_min:.2f})\n"
                        f"{sig.ict_details}")
                self._confirm_trend_long = self._confirm_trend_short = 0
                return
            self._ict_gate_start_time_mom = 0.0; self._ict_gate_alerted_mom = False

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
            self._last_momentum_attempt = now
            retest_sl = self._breakout.retest_sl
            # Bug-4 fix: _t_mo is initialised to "B" before the ICT gate block
            # above, so it is always defined here — no dir()/locals() probe needed.
            _mo_tier = _t_mo
            logger.info(
                f"🚀 RETEST ENTRY — {side.upper()} (breakout {bo_dir}) | "
                f"retest_low=${self._breakout._retest_low:,.2f} | "
                f"SL=${retest_sl:,.2f}")
            self._launch_entry_async(data_manager, order_manager, risk_manager, side, sig, mode="momentum", ict_tier=_mo_tier)
        elif side == "short" and self._confirm_trend_short >= cn:
            self._confirm_trend_long = self._confirm_trend_short = 0
            self._last_momentum_attempt = now
            retest_sl = self._breakout.retest_sl
            _mo_tier = _t_mo   # Bug-4 fix: always defined, see initialisation above
            logger.info(
                f"🚀 RETEST ENTRY — {side.upper()} (breakout {bo_dir}) | "
                f"retest_high=${self._breakout._retest_high:,.2f} | "
                f"SL=${retest_sl:,.2f}")
            self._launch_entry_async(data_manager, order_manager, risk_manager, side, sig, mode="momentum", ict_tier=_mo_tier)

    def _compute_sl_tp(self, data_manager, price, side, atr, mode="reversion",
                       signal_confidence=0.5, use_maker_entry=False):
        """
        Institutional SL/TP — v5.0 ICT Sweep Engine primary path.

        v5.0 HIERARCHY:
          PATH-A (Sweep Engine available + sweep setup active):
            SL → ICTSLEngine (sweep-wick → disp-OB → 15m-OB → 15m-swing)
            TP → ICTTPEngine (AMD delivery → opposing pool → structural → VWAP)
            Hard R:R gate: reversion ≥1.8R, trend ≥2.5R — reject if not met.

          PATH-B (Legacy — no sweep engine or no active setup):
            SL → existing ICT OB → 15m swing → ATR fallback
            TP → existing ICT structural tiered selection
            Fee-normalized TP floor gate (PATCH 4).

        Both paths share the fee-engine gate at the end for consistency.

        Args:
            signal_confidence: composite score [0,1] for fee gate tuning
            use_maker_entry:   limit-entry flag for fee gate
        """
        try: candles_5m = data_manager.get_candles("5m", limit=QCfg.SL_SWING_LOOKBACK()+5)
        except Exception: candles_5m = []
        try: candles_1m = data_manager.get_candles("1m", limit=150)
        except Exception: candles_1m = []
        try: candles_15m = data_manager.get_candles("15m", limit=60)
        except Exception: candles_15m = []
        try: orderbook = data_manager.get_orderbook()
        except Exception: orderbook = {"bids": [], "asks": []}
        vwap = self._vwap.vwap; vwap_std = self._vwap.vwap_std
        atr_pctile = self._atr_5m.get_percentile()


        # ══════════════════════════════════════════════════════════════════════
        # v5.0 PATH-A — ICT SWEEP ENGINE (highest conviction, best R:R)
        # ══════════════════════════════════════════════════════════════════════
        # Use ICTSLEngine + ICTTPEngine when:
        #   • ict_trade_engine.py is available, AND
        #   • An active sweep setup (OTE_READY) is present
        #
        # Both return values are validated. If ICTTPEngine returns None
        # (no target meets the R:R gate), PATH-A is rejected — do NOT lower
        # the bar. Fall through to PATH-B for the legacy path.
        sl_price    = None
        tp_price    = None
        _sl_source  = "none"
        _used_path  = "B"   # "A" | "B" | "C" — for logging

        now_ms_slatp = int(time.time() * 1000)

        # ══════════════════════════════════════════════════════════════════════
        # PATH-C — LIQUIDITY HUNT (pre-computed SL/TP from Tier-L EntryEngine path)
        # ══════════════════════════════════════════════════════════════════════
        # When mode=="hunt", _pending_hunt_signal holds SL/TP from the hunter.
        # These levels are structurally superior: SL behind swept pool wick,
        # TP at opposing liquidity pool — exactly where price is being delivered.
        # Skip PATH-A and PATH-B entirely; apply the fee gate at the end.
        if mode == "hunt" and self._pending_hunt_signal is not None:
            _hs = self._pending_hunt_signal
            _hs_sl  = _round_to_tick(_hs.sl_price)
            _hs_tp  = _round_to_tick(_hs.tp_price)
            # Direction sanity
            _sl_ok = (side == "long"  and _hs_sl < price) or (side == "short" and _hs_sl > price)
            _tp_ok = (side == "long"  and _hs_tp > price) or (side == "short" and _hs_tp < price)
            if _sl_ok and _tp_ok:
                # BUG-2 FIX: _evaluate_hunt_entry may mutate tp_price (ICT opposing-pool
                # update) AFTER the original hunt signal R:R was validated.
                # The mutated TP could place the ratio below _MIN_RR (e.g. 0.9R).
                # Re-validate here using the ACTUAL prices PATH-C will use.
                _sl_dist = abs(price - _hs_sl)
                _tp_dist = abs(price - _hs_tp)
                _path_c_rr = _tp_dist / _sl_dist if _sl_dist > 1e-10 else 0.0
                _min_rr_c  = QCfg.REVERSION_MIN_RR() if mode not in ("trend", "momentum") \
                             else QCfg.TREND_MIN_RR()
                if _path_c_rr < _min_rr_c:
                    logger.info(
                        f"⛔ PATH-C R:R gate: {_path_c_rr:.2f} < {_min_rr_c:.1f} "
                        f"(SL={_sl_dist:.0f}pts TP={_tp_dist:.0f}pts) "
                        f"— hunt signal R:R degraded after ICT TP update, rejecting"
                    )
                    # Clear the stale hunt signal so it is not re-evaluated
                    self._pending_hunt_signal = None
                    # Fall through to PATH-A / PATH-B
                else:
                    sl_price   = _hs_sl
                    tp_price   = _hs_tp
                    _sl_source = "hunt_swept_pool"
                    _used_path = "C"
                    logger.info(
                        f"🎣 PATH-C (HUNT): SL=${sl_price:,.1f}  TP=${tp_price:,.1f}  "
                        f"R:R=1:{_path_c_rr:.2f}  "
                        f"swept=${_hs.swept_pool_price:,.0f}  target=${_hs.target_pool_price:,.0f}"
                    )
                    # Apply only the fee-gate at the end — skip all structural SL/TP logic

        if (sl_price is None and  # PATH-C not active
                _ICT_TRADE_ENGINE_AVAILABLE and
                QCfg.ICT_SWEEP_ENTRY_ENABLED() and
                self._active_sweep_setup is not None and
                self._active_sweep_setup.status == "OTE_READY" and
                self._active_sweep_setup.side == side):

            try:
                sl_price, _sl_source = ICTSLEngine.compute(
                    side, price, atr,
                    sweep_setup  = self._active_sweep_setup,
                    ict_engine   = self._ict,
                    candles_15m  = candles_15m,
                    atr_pctile   = atr_pctile,
                    mode         = mode,
                    market_regime = self._regime.regime.value if hasattr(self._regime, 'regime') else "RANGING",
                )
                tp_price = ICTTPEngine.compute(
                    side, price, atr, sl_price,
                    sweep_setup  = self._active_sweep_setup,
                    ict_engine   = self._ict,
                    candles_15m  = candles_15m,
                    vwap         = vwap,
                    mode         = mode,
                    now_ms       = now_ms_slatp,
                )
                if tp_price is None:
                    # Hard R:R gate rejected — no valid target → PATH-B
                    logger.info(
                        f"⛔ PATH-A R:R gate: no valid TP target for "
                        f"{side.upper()} sweep setup — falling through to PATH-B")
                    sl_price   = None
                    _sl_source = "none"
                else:
                    _used_path = "A"
                    logger.info(
                        f"🏛️ PATH-A ACTIVE: sweep SL=${sl_price:,.2f}({_sl_source}) "
                        f"TP=${tp_price:,.2f} "
                        f"R:R=1:{abs(tp_price-price)/max(abs(price-sl_price),1):.2f} "
                        f"AMD={getattr(self._active_sweep_setup,'amd_confidence',0):.2f}"
                    )
            except Exception as _pa_e:
                logger.warning(f"PATH-A sweep engine error (falling to PATH-B): {_pa_e}")
                sl_price = None; tp_price = None; _sl_source = "none"

        # ══════════════════════════════════════════════════════════════════════
        # v5.0 PATH-B — LEGACY ICT HIERARCHY (v6.0: regime-adaptive + liq guard)
        # ══════════════════════════════════════════════════════════════════════
        if sl_price is None:
            # v6.0: regime-adaptive min_dist for PATH-B
            _regime_str = self._regime.regime.value if hasattr(self._regime, 'regime') else "RANGING"
            if _regime_str in ("TRENDING_UP", "TRENDING_DOWN"):
                _path_b_min_dist = max(price * 0.005, 1.0 * atr)
            elif _regime_str == "TRANSITIONING":
                _path_b_min_dist = max(price * 0.004, 0.7 * atr)
            else:
                _path_b_min_dist = max(price * QCfg.MIN_SL_PCT(), 0.40 * atr)

            # ── Step 1: ICT 15m OB ────────────────────────────────────────────
            _ob_min_dist = max(0.5 * atr, _path_b_min_dist)
            _ob_max_dist = price * QCfg.MAX_SL_PCT()
            if self._ict is not None:
                try:
                    ob_sl = self._ict.get_ob_sl_level(
                        side, price, atr, now_ms_slatp, htf_only=True)
                    if ob_sl is not None:
                        ob_dist      = abs(price - ob_sl)
                        ob_valid_dir = ((side == "long"  and ob_sl < price) or
                                        (side == "short" and ob_sl > price))
                        if ob_valid_dir and _ob_min_dist <= ob_dist <= _ob_max_dist:
                            sl_price   = _round_to_tick(ob_sl)
                            _sl_source = "ICT_OB"
                            logger.info(
                                f"🏛️ ICT OB SL: ${sl_price:,.2f} "
                                f"({ob_dist:.1f}pts / {ob_dist/atr:.2f}ATR)")
                        elif ob_valid_dir and ob_dist < _ob_min_dist:
                            logger.debug(
                                f"ICT OB SL too close ({ob_dist:.1f}pts < "
                                f"{_ob_min_dist:.1f}min) — proceeding to 15m swing")
                except Exception as _e:
                    logger.debug(f"ICT OB SL error: {_e}")

            # ── Step 2: 15m swing structure ───────────────────────────────────
            if sl_price is None and candles_15m and len(candles_15m) >= 3:
                _lb_15m = min(40, len(candles_15m) - 2)
                _sh_15m, _sl_15m = InstitutionalLevels.find_swing_extremes(
                    candles_15m, _lb_15m)
                buf_mult = QCfg.SL_BUFFER_ATR_MULT() * (
                    1.4 - 0.8 * min(max(atr_pctile, 0.0), 1.0))
                _sl_buf  = buf_mult * atr
                _min_dist = _path_b_min_dist
                _max_dist = price * QCfg.MAX_SL_PCT()
                _candidates: List[Tuple[float, float]] = []

                if side == "long":
                    for lvl in _sl_15m:
                        if lvl < price:
                            dist = price - lvl
                            if dist <= _max_dist:
                                _candidates.append((lvl - _sl_buf * 0.80,
                                                    1.0 / max(dist, 1.0)))
                else:
                    for lvl in _sh_15m:
                        if lvl > price:
                            dist = lvl - price
                            if dist <= _max_dist:
                                _candidates.append((lvl + _sl_buf * 0.80,
                                                    1.0 / max(dist, 1.0)))

                if _candidates:
                    best = max(_candidates, key=lambda x: x[1])
                    _swing_sl = _round_to_tick(best[0])
                    sl_dist = abs(price - _swing_sl)
                    if sl_dist >= _min_dist:
                        sl_price   = _swing_sl
                        _sl_source = "15m_swing"
                        logger.info(
                            f"📐 15m Swing SL: ${sl_price:,.2f} "
                            f"({sl_dist:.0f}pts / {sl_dist/atr:.2f}ATR)")

            # ── Step 3: ATR fallback ───────────────────────────────────────────
            if sl_price is None:
                _min_dist   = _path_b_min_dist
                _max_dist   = price * QCfg.MAX_SL_PCT()
                _atr_sl_dist = max(_min_dist, min(_max_dist, 1.5 * atr))
                sl_price   = _round_to_tick(
                    price - _atr_sl_dist if side == "long" else price + _atr_sl_dist)
                _sl_source = "ATR_fallback"
                logger.warning(
                    f"⚠️ SL ATR fallback: ${sl_price:,.2f} "
                    f"({_atr_sl_dist:.0f}pts / {_atr_sl_dist/atr:.2f}ATR) — no 15m structure found")

            # ── v6.0: PATH-B LIQUIDITY PROXIMITY GUARD ─────────────────────────
            # Same logic as ICTSLEngine — if SL sits near a BSL/SSL cluster,
            # move it behind the pool so the sweep doesn't take us out.
            if sl_price is not None and self._ict is not None:
                try:
                    _liq_guard_dist = 0.6 * atr
                    _liq_buffer     = 0.35 * atr
                    _max_dist_liq   = price * QCfg.MAX_SL_PCT()
                    for pool in self._ict.liquidity_pools:
                        if pool.swept:
                            continue
                        dist_sl_to_pool = abs(sl_price - pool.price)
                        if dist_sl_to_pool > _liq_guard_dist:
                            continue
                        if side == "long" and pool.level_type == "SSL" and pool.price < price:
                            new_sl = pool.price - _liq_buffer
                            if abs(price - new_sl) <= _max_dist_liq:
                                logger.info(
                                    f"🛡️ PATH-B LIQ GUARD: SL ${sl_price:,.2f} → "
                                    f"${new_sl:,.2f} (behind SSL@${pool.price:,.0f})")
                                sl_price = _round_to_tick(new_sl)
                                _sl_source += "(liq_guard)"
                        elif side == "short" and pool.level_type == "BSL" and pool.price > price:
                            new_sl = pool.price + _liq_buffer
                            if abs(new_sl - price) <= _max_dist_liq:
                                logger.info(
                                    f"🛡️ PATH-B LIQ GUARD: SL ${sl_price:,.2f} → "
                                    f"${new_sl:,.2f} (behind BSL@${pool.price:,.0f})")
                                sl_price = _round_to_tick(new_sl)
                                _sl_source += "(liq_guard)"
                except Exception:
                    pass

        # ── Mode-aware SL sizing (trend/momentum ATR cap) ──────────────────
        if mode in ("trend", "momentum"):
            max_sl_atr = float(_cfg("QUANT_TREND_SL_ATR_MULT", 3.0))
            max_sl_dist = max_sl_atr * atr
            current_dist = abs(price - sl_price)
            if current_dist > max_sl_dist:
                sl_price = _round_to_tick(
                    price - max_sl_dist if side == "long" else price + max_sl_dist)
                logger.info(
                    f"📏 Trend SL capped: {current_dist:.0f}→{max_sl_dist:.0f}pts "
                    f"({max_sl_atr:.1f}×ATR) | SL=${sl_price:,.2f}")
            # Liquidation safety buffer
            liq_buffer = 0.5 * atr
            if side == "long":
                liq_price = price - (price / QCfg.LEVERAGE()) * 0.95
                if sl_price < liq_price + liq_buffer:
                    sl_price = _round_to_tick(liq_price + liq_buffer)
            else:
                liq_price = price + (price / QCfg.LEVERAGE()) * 0.95
                if sl_price > liq_price - liq_buffer:
                    sl_price = _round_to_tick(liq_price - liq_buffer)

        # ══════════════════════════════════════════════════════════════════════
        # TP COMPUTATION (PATH-B only — PATH-A already computed tp_price)
        # ══════════════════════════════════════════════════════════════════════
        if tp_price is None:
            if mode in ("trend", "momentum"):
                tp_price = InstitutionalLevels.compute_tp_trend(
                    price, side, atr, sl_price, candles_5m, orderbook,
                    swing_lookback=QCfg.SL_SWING_LOOKBACK(),
                    candles_15m=candles_15m)
            else:
                tp_price = InstitutionalLevels.compute_tp(
                    price, side, atr, sl_price, candles_1m, orderbook, vwap, vwap_std,
                    candles_5m=candles_5m,
                    ict_engine=self._ict,
                    now_ms=now_ms_slatp,
                    candles_15m=candles_15m,
                    liq_map=self._liq_map)

        # BUG-1 FIX: compute_tp returns None when R:R gate hard-rejects the setup.
        # Calling _round_to_tick(None) raises TypeError: unsupported operand type(s)
        # for /: 'NoneType' and 'float'.  Gate the None explicitly here so the
        # traceback never propagates past this function.
        if tp_price is None:
            _tp_gate_rr = (QCfg.REVERSION_MIN_RR() if mode not in ("trend","momentum")
                           else QCfg.TREND_MIN_RR())
            logger.info(
                f"⛔ TP gate (PATH-B): no valid TP target for {side.upper()} "
                f"— minimum R:R={_tp_gate_rr:.1f} not achievable with current structure "
                f"— rejecting trade (no entry)")
            return None, None

        tp_price = _round_to_tick(tp_price)

        # ── Basic direction sanity ────────────────────────────────────────────
        if side == "long"  and (sl_price >= price or tp_price <= price): return None, None
        if side == "short" and (sl_price <= price or tp_price >= price): return None, None

        # ── PATH-A: additional R:R sanity check ───────────────────────────────
        if _used_path == "A":
            rr_actual = abs(tp_price - price) / max(abs(price - sl_price), 1e-10)
            min_rr    = (QCfg.ICT_TP_MIN_RR_TREND()     if mode in ("trend", "momentum")
                         else QCfg.ICT_TP_MIN_RR_REVERSION())
            if rr_actual < min_rr:
                logger.info(
                    f"⛔ PATH-A R:R final check: {rr_actual:.2f} < {min_rr:.1f} "
                    f"— rejecting sweep setup entry")
                return None, None

        # ── Fee-normalized TP floor gate (PATCH 4) ───────────────────────────
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

    def _enter_trade(self, data_manager, order_manager, risk_manager, side, sig, mode="reversion",
                     ict_tier: str = ""):
        """
        Position entry — v7.0 (confidence-weighted sizing via ict_tier).

        ict_tier: "S" | "A" | "B" | "" — controls position size multiplier:
          Tier-S: 1.00× base margin  (full conviction — confirmed sweep + AMD)
          Tier-A: 0.80× base margin  (high conviction — structural alignment)
          Tier-B: 0.65× base margin  (standard quant + ICT gate)
          "":     0.50× base margin  (minimal exposure — no ICT gate)

        Additionally modulated by composite score (±10%) and AMD confidence (±8%).
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

        # ── Map composite score → signal_confidence [0, 1] (PATCH 5a) ───────────
        # NOTE: moved BEFORE _compute_sl_tp so signal_confidence is available.
        raw_composite     = abs(sig.composite) if sig.composite is not None else 0.0
        signal_confidence = min(1.0, raw_composite / 0.6)   # 0.6 composite = full confidence

        # ── Always limit (maker) entry — price from live orderbook ─────────────
        use_maker = True
        tick      = QCfg.TICK_SIZE()
        offset    = float(getattr(config, 'LIMIT_ORDER_OFFSET_TICKS', 3)) * tick

        try:
            orderbook = data_manager.get_orderbook()
            bids = (orderbook or {}).get("bids", [])
            asks = (orderbook or {}).get("asks", [])
            if bids and asks:
                def _best_px(lvl):
                    if isinstance(lvl,(list,tuple)): return float(lvl[0])
                    if isinstance(lvl,dict): return float(lvl.get("limit_price") or lvl.get("price") or 0)
                    return 0.0
                if side == "long":
                    limit_px  = round(_best_px(bids[0]), 1)
                    mt_reason = f"limit_long@bid={limit_px:.1f}"
                else:
                    limit_px  = round(_best_px(asks[0]), 1)
                    mt_reason = f"limit_short@ask={limit_px:.1f}"
            else:
                raise ValueError("empty book")
        except Exception:
            if side == "long":
                limit_px = round(price - offset, 1)
            else:
                limit_px = round(price + offset, 1)
            mt_reason = f"limit_{side}_offset={offset:.1f}pts (no book)"

        # Keep fee engine updated for diagnostics and TP gate
        if self._fee_engine is not None:
            try:
                ob = data_manager.get_orderbook()
                if ob:
                    self._fee_engine.update_orderbook(ob, price)
            except Exception:
                pass

        logger.info(f"Entry routing: LIMIT | {mt_reason}")

        # ── FIX Bug-B STEP 1: Compute SL/TP FIRST ────────────────────────────────
        # SL/TP computation does not depend on position size — it uses price, ATR,
        # mode, and signal_confidence only.  Computing it first lets us pass the
        # ACTUAL SL distance (not an ATR proxy) into position sizing, which is the
        # correct industry-grade approach: risk-in-dollars / SL-distance = quantity.

        # -- v9.0: Use force SL/TP from entry engine if available --
        # BUG-4 FIX: When the v9 entry engine provides force_sl/force_tp,
        # use them DIRECTLY. The old code set sl_price/tp_price here but
        # then immediately called _compute_sl_tp() which OVERWROTE them
        # with PATH-B values (widening SL from 53pts to 162pts).
        _force_sl = getattr(self, '_force_sl', None)
        _force_tp = getattr(self, '_force_tp', None)
        _using_force_levels = False
        if _force_sl is not None and _force_tp is not None and _force_sl > 0 and _force_tp > 0:
            _fsl = _round_to_tick(_force_sl)
            _ftp = _round_to_tick(_force_tp)
            _dir_ok = False
            if side == "long" and _fsl < price and _ftp > price:
                _dir_ok = True
            elif side == "short" and _fsl > price and _ftp < price:
                _dir_ok = True
            if _dir_ok:
                sl_price = _fsl
                tp_price = _ftp
                _using_force_levels = True
                logger.info(f"v9.0 force SL/TP: SL=${sl_price:,.1f} TP=${tp_price:,.1f} (skipping PATH-B)")
            self._force_sl = None
            self._force_tp = None

        if not _using_force_levels:
            sl_price, tp_price = self._compute_sl_tp(
                data_manager, price, side, atr, mode=mode,
                signal_confidence=signal_confidence,
                use_maker_entry=use_maker,
            )
        else:
            # Force levels active — still validate with fee engine if available
            if self._fee_engine is not None and self._fee_engine.is_warmed_up():
                try:
                    _tp_dist = abs(tp_price - price)
                    _min_tp = self._fee_engine.min_required_tp_move(
                        price=price, atr=atr,
                        atr_percentile=self._atr_5m.get_percentile(),
                        use_maker_entry=use_maker,
                        signal_confidence=signal_confidence)
                    if _tp_dist < _min_tp:
                        logger.info(f"⛔ Force TP fee gate: {_tp_dist:.0f} < {_min_tp:.0f}")
                        sl_price = None
                except Exception:
                    pass
        if sl_price is None:
            with self._lock:
                self._last_tp_gate_rejection = time.time()
            return

        sd = abs(price - sl_price)
        td = abs(price - tp_price)
        if sd < 1e-10: return
        rr = td / sd

        # ── FIX Bug-B STEP 2: Size using actual SL distance ──────────────────────
        # Now that sl_price is known, pass it to _compute_quantity so the base size
        # comes from risk_manager.calculate_position_size (dollar-risk / SL-dist).
        # The tier/composite/AMD multiplier is applied on top of that base.
        qty = self._compute_quantity(
            risk_manager, price, sig=sig, ict_tier=ict_tier, sl_price=sl_price
        )
        if qty is None or qty < QCfg.MIN_QTY(): return
        logger.info(
            f"🎯 ENTERING {side.upper()} @ ${price:,.2f} | qty={qty} | "
            f"SL=${sl_price:,.2f} TP=${tp_price:,.2f} R:R=1:{rr:.2f} | "
            f"{'maker' if use_maker else 'taker'} | VWAP=${sig.vwap_price:,.2f} | {sig}"
        )

        # ── Place entry ────────────────────────────────────────────────────────────
        # Delta: bracket limit order (entry + SL + TP in one API call).
        #   Avoids bad_schema from separate stop/take-profit order placement.
        # CoinSwitch: standard limit entry, SL/TP placed separately after fill.
        limit_timeout = float(getattr(config, 'LIMIT_ORDER_FILL_TIMEOUT_SEC', 45.0))
        is_bracket = False
        entry_data = order_manager.place_bracket_limit_entry(
            side=side, quantity=qty,
            limit_price=limit_px,
            sl_price=sl_price, tp_price=tp_price,
            timeout_sec=limit_timeout,
        )
        if entry_data is not None:
            is_bracket = entry_data.get("bracket_order", False)
        else:
            # Fallback: standard limit entry (CoinSwitch, or bracket unavailable)
            entry_data = order_manager.place_limit_entry(
                side=side, quantity=qty,
                limit_price=limit_px,
                timeout_sec=limit_timeout,
                fallback_to_market=False,
            )

        if not entry_data:
            logger.error("❌ Entry order failed")
            self._last_exit_time = time.time()  # engage cooldown — prevents hammer-retrying
            return

        # ── Extract fill price ────────────────────────────────────────────────────
        fill_price = (
            float(entry_data.get("fill_price")          or 0)
            or float(entry_data.get("average_price")    or 0)
            or float(entry_data.get("avg_execution_price") or 0)
            or float(entry_data.get("price")            or 0)
            or price
        )
        actual_fill_type = entry_data.get("fill_type", "taker")
        # v8.1: exact entry fee from Delta paid_commission (propagated by order_manager)
        entry_fee_paid = float(entry_data.get("paid_commission", 0) or 0)
        if entry_fee_paid > 0:
            logger.info(f"💰 Entry fee (exact): ${entry_fee_paid:.4f}")

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

        # ── Place SL/TP (or retrieve bracket child order IDs) ───────────────────
        exit_side = "sell" if side == "long" else "buy"

        if is_bracket:
            # Delta bracket: SL and TP were embedded in the entry order.
            # Delta auto-created the child SL/TP orders on fill.
            sl_order_id_raw = entry_data.get("bracket_sl_order_id", "")
            tp_order_id_raw = entry_data.get("bracket_tp_order_id", "")
            # Use bracket prices if we have them (queried from open_orders),
            # otherwise fall back to the computed prices
            bsl = entry_data.get("bracket_sl_price", 0.0)
            btp = entry_data.get("bracket_tp_price", 0.0)
            if bsl > 0:
                sl_price = bsl
            if btp > 0:
                tp_price = btp
            sl_data = {"order_id": sl_order_id_raw} if sl_order_id_raw else None
            tp_data = {"order_id": tp_order_id_raw} if tp_order_id_raw else None
            if sl_order_id_raw:
                logger.info(f"✅ Bracket SL order: {sl_order_id_raw} @ ${sl_price:,.2f}")
            if tp_order_id_raw:
                logger.info(f"✅ Bracket TP order: {tp_order_id_raw} @ ${tp_price:,.2f}")
            if not sl_order_id_raw or not tp_order_id_raw:
                logger.warning(
                    "⚠️ Bracket child order IDs not found after fill — "
                    "trailing SL may not work. Check open orders manually.")
        else:
            # CoinSwitch (and non-bracket) path: place SL/TP as separate orders
            sweep = order_manager.cancel_symbol_conditionals()
            if sweep:
                # v4.6 BUG FIX #3: Wait for exchange to process cancellations
                # Without this, old SL/TP can fire against the new position instantly
                time.sleep(1.5)
                filled = [
                    oid for oid, r in sweep.items()
                    if r in (CancelResult.ALREADY_FILLED, CancelResult.PARTIAL_FILL)
                ]
                if filled:
                    self._last_reconcile_time = 0.0
                    return

            sl_data = order_manager.place_stop_loss(
                side=exit_side, quantity=qty, trigger_price=sl_price)
            if not sl_data:
                order_manager.place_market_order(side=exit_side, quantity=qty, reduce_only=True)
                self._last_exit_time = time.time()
                return

            tp_data = order_manager.place_take_profit(
                side=exit_side, quantity=qty, trigger_price=tp_price)
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
            sl_order_id     = (sl_data or {}).get("order_id", ""),
            tp_order_id     = (tp_data or {}).get("order_id", ""),
            entry_order_id  = entry_data.get("order_id"),
            entry_time      = time.time(),
            initial_risk    = ir,
            initial_sl_dist = sdf,
            entry_signal    = sig,
            entry_atr       = self._atr_5m.atr,
            entry_vol       = entry_vol,
            trade_mode      = mode,
            entry_fill_type = actual_fill_type,  # v4.3: for correct PnL fee calc
            entry_fee_paid  = entry_fee_paid,     # v8.1: exact from Delta paid_commission
            ict_entry_tier  = ict_tier,           # v7.0: confidence tier for analytics
            # FIX 8b: capture actual HTF scores at entry so _record_pnl can log them correctly
            entry_htf_15m   = self._htf.trend_15m,
            entry_htf_4h    = self._htf.trend_4h,
        )
        # ── Reconcile safety: discard any in-flight reconcile data ────────────────
        self._reconcile_data        = None
        self._last_reconcile_time   = time.time()
        self.current_sl_price       = sl_price
        self.current_tp_price       = tp_price
        self._confirm_long          = self._confirm_short = 0
        self._confirm_hunt_long     = self._confirm_hunt_short = 0
        # Reset duplicate guards for the new position
        self._exit_completed        = False
        self._pnl_recorded_for     = 0.0
        # FIX Bug-C: record_trade_start() AFTER confirmed fill, not before.
        # Moving it here prevents aborted entries (TP-gate, fee-gate, exchange
        # error) from consuming the daily trade cap with no actual order sent.
        self._risk_gate.record_trade_start()
        if hasattr(self, '_entry_engine') and self._entry_engine is not None:
            self._entry_engine.on_position_opened()

        # ── v5.0: Invalidate sweep detector — setup consumed ─────────────────────
        if self._sweep_detector is not None:
            self._sweep_detector.invalidate()
        _sweep_was_active = self._active_sweep_setup is not None
        self._active_sweep_setup = None
        # ── v5.2: Clear hunt signal — consumed by fill ────────────────────────
        _hunt_was_active = self._pending_hunt_signal is not None
        self._pending_hunt_signal = None

        # ── v9 Entry Telegram notification — pool-first, not quant-scout ─────────
        sl_dist_pts = abs(fill_price - sl_price)
        tp_dist_pts = abs(fill_price - tp_price)
        rr_a        = tp_dist_pts / sl_dist_pts if sl_dist_pts > 1e-10 else 0.0
        dollar_risk = sl_dist_pts * qty

        # ── Entry type label (from EntrySignal.entry_type) ───────────────────────
        _et_labels = {
            "sweep_reversal":     "🏛️ SWEEP REVERSAL",
            "pre_sweep_approach": "⚡ PRE-SWEEP APPROACH",
            "sweep_continuation": "📈 SWEEP CONTINUATION",
        }
        if _sweep_was_active:
            _et_label = "🏛️ SWEEP-AND-GO REVERSAL"
        elif locals().get('_hunt_was_active', False) or mode == "hunt":
            _et_label = "🎣 LIQUIDITY HUNT"
        else:
            _et_label = _et_labels.get(mode, mode.upper())

        # ── Pool target, flow conviction, entry reason — from EntrySignal ────────
        # _last_entry_signal is stored on self by the v9 tick loop just before
        # _launch_entry_async is called, so it is always available here.
        _es = getattr(self, '_last_entry_signal', None)
        _pool_tp_str   = "—"
        _swept_str     = ""
        _flow_conv_str = "—"
        _entry_reason  = "—"
        _ict_val_str   = ""

        if _es is not None:
            try:
                # Pool being targeted → this IS the TP origin
                _pt = _es.target_pool
                if _pt and hasattr(_pt, 'pool'):
                    _pool_tp_str = (
                        f"{'BSL ▲' if _pt.pool.side.value == 'BSL' else 'SSL ▼'}"
                        f" @ ${_pt.pool.price:,.0f}"
                        f"  (sig={_pt.significance:.2f}"
                        f"  x{_pt.pool.touches} touches)")
            except Exception:
                pass
            try:
                # Sweep that triggered the entry
                if _es.sweep_result is not None:
                    _sw = _es.sweep_result
                    _sw_side  = _sw.pool.side.value if hasattr(_sw, 'pool') else "?"
                    _sw_px    = _sw.pool.price      if hasattr(_sw, 'pool') else 0.0
                    _sw_qual  = getattr(_sw, 'quality', 0.0)
                    _sw_disp  = "✅DISP" if getattr(_sw, 'displacement_confirmed', False) else "⚠️weak"
                    _swept_str = (
                        f"\nSwept:    {_sw_side} @ ${_sw_px:,.0f}"
                        f"  quality={_sw_qual:.0%}  {_sw_disp}")
            except Exception:
                pass
            try:
                _flow_conv_str = f"{_es.conviction:+.3f}"
            except Exception:
                pass
            try:
                if _es.reason:
                    _entry_reason = _es.reason
            except Exception:
                pass
            try:
                if _es.ict_validation:
                    _ict_val_str = _es.ict_validation
            except Exception:
                pass

        # ── ICT / AMD context ─────────────────────────────────────────────────────
        _amd_str = ""
        if getattr(sig, 'amd_phase', '') and getattr(sig, 'amd_conf', 0.0) > 0.01:
            _amd_icons = {"DISTRIBUTION": "🎯", "MANIPULATION": "⚡",
                          "REACCUMULATION": "🔄", "REDISTRIBUTION": "🔄",
                          "ACCUMULATION": "💤"}
            _amd_i   = _amd_icons.get(sig.amd_phase, "❓")
            _bias_i  = "🟢" if getattr(sig,'amd_bias','') == "bullish" else ("🔴" if getattr(sig,'amd_bias','') == "bearish" else "⚪")
            _amd_str = (
                f"\nAMD:      {_amd_i} {sig.amd_phase}"
                f"  {_bias_i}{getattr(sig,'amd_bias','?')}"
                f"  conf={sig.amd_conf:.2f}")

        # OB / FVG in OTE (from active sweep setup at time of entry)
        _ict_in_ote_str = ""
        if getattr(sig, 'ict_total', 0.0) > 0.01:
            _ob_n  = min(getattr(sig, 'ict_ob', 0.0) / 2.0, 1.0)
            _fvg_n = min(getattr(sig, 'ict_fvg', 0.0) / 1.5, 1.0)
            _ict_in_ote_str = (
                f"\nICT:      Σ={sig.ict_total:.2f}"
                f"  OB={_ob_n:.2f}  FVG={_fvg_n:.2f}"
                f"  Swp={getattr(sig,'ict_sweep',0.0):.2f}")
            if _ict_val_str:
                _ict_in_ote_str += f"\n          {_ict_val_str}"

        # HTF MTF context
        _htf_str = (
            f"\nHTF:      15m={self._htf.trend_15m:+.2f}"
            f"  4H={self._htf.trend_4h:+.2f}"
            f"  (structure context)")

        # Tier
        _tier_labels = {"S": "🥇 Tier-S — OTE Sweep-and-Go",
                        "A": "🥈 Tier-A — ICT Structural",
                        "B": "🥉 Tier-B — Quant+ICT Confluence",
                        "":  "⚪ No ICT tier"}
        _tier_badge = _tier_labels.get(ict_tier, f"Tier-{ict_tier}")

        # Side icon
        _side_icon = "🟢" if side == "long" else "🔴"

        # Trail plan for "what's next"
        _trail_plan = (
            "BOS confirmed → P1 swing trail"
            " → CHoCH tighten (P2)"
            " → 15m structure (P3 at 1.5R+)")
        _ratchet_plan = "BE @0.5R → +0.15R@1R → +0.5R@1.5R → +1R@2R → trailing@2.5R+"

        send_telegram_message(
            f"{_side_icon} <b>{side.upper()} ENTERED — {_et_label}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>💰 LEVELS</b>\n"
            f"Entry:    <b>${fill_price:,.2f}</b>\n"
            f"SL:       ${sl_price:,.2f}"
            f"  (−${sl_dist_pts:.1f} / {sl_dist_pts/max(self._atr_5m.atr,1):.2f}×ATR)\n"
            f"TP:       ${tp_price:,.2f}"
            f"  (+${tp_dist_pts:.1f} / {tp_dist_pts/max(self._atr_5m.atr,1):.2f}×ATR)\n"
            f"R:R:      1:{rr_a:.2f}  │  Risk: ${dollar_risk:.2f} USDT\n"
            f"Qty:      {qty:.4f} BTC\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>🎯 WHY WE ENTERED</b>\n"
            f"Pool TP:  {_pool_tp_str}"
            f"{_swept_str}\n"
            f"Reason:   {_entry_reason}\n"
            f"Flow:     conviction={_flow_conv_str}\n"
            f"Tier:     {_tier_badge}"
            f"{_amd_str}"
            f"{_ict_in_ote_str}"
            f"{_htf_str}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>⏩ WHAT'S NEXT</b>\n"
            f"Trail:    {_trail_plan}\n"
            f"Ratchet:  {_ratchet_plan}\n"
            f"Exits:    SL hit │ TP (pool) hit │ Regime flip │ Max-hold\n"
            f"Monitor:  /position  or  /thinking"
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

            # v6.0: Momentum trades exit via SL, TP, or trailing SL ONLY.
            # BREAKOUT_EXPIRED exit REMOVED — it was closing positions right before
            # TP hit because breakout timer expired while the move was still in progress.
            # Momentum trades are structurally managed: SL trails via ICT structure,
            # TP is at the opposing liquidity pool. No premature composite-based exit.

            # v5.1: Flow trades exit when order flow structurally reverses.
            # Not on a single tick flip — on sustained counter-flow + BOS reversal.
            if pos.trade_mode == "flow":
                # BUG-FIX: compute profit here; it was used below without being defined
                _flow_profit = ((price - pos.entry_price) if pos.side == "long"
                                else (pos.entry_price - price))
                _tick_now = self._tick_eng.get_signal()
                _bos_reversed = False
                if self._ict is not None and getattr(self._ict, '_initialized', False):
                    try:
                        _tf5 = self._ict._tf.get("5m")
                        if _tf5 is not None:
                            if (pos.side == "long" and _tf5.bos_direction == "bearish"):
                                _bos_reversed = True
                            elif (pos.side == "short" and _tf5.bos_direction == "bullish"):
                                _bos_reversed = True
                    except Exception:
                        pass
                # Exit: BOS reversed AND tick flow now opposing
                if _bos_reversed:
                    _flow_opposing = (
                        (pos.side == "long" and _tick_now < -0.40) or
                        (pos.side == "short" and _tick_now > 0.40))
                    if _flow_opposing and _flow_profit > 0:
                        logger.info(
                            f"🔄 Flow reversal: 5m BOS + opposing tick "
                            f"({_tick_now:+.2f}) → exit {pos.side.upper()} [flow]")
                        self._exit_trade(order_manager, price, "flow_reversal")
                        return

        # v5.0: max-hold time exit REMOVED.
        # Trades exit via SL, TP, trailing SL, regime flip, or breakout expiry only.
        # A timer cannot know if the trade is working.

                # ── Trailing SL — v6.0 STRUCTURE-EVENT-DRIVEN ──────────────────────
        # ARCHITECTURE CHANGE: Time-based TRAIL_INTERVAL_S (10s timer) REMOVED.
        #
        # Old problem: The 10s timer missed critical structure events. A BOS could
        # form at t=1s, but trail wouldn't check until t=10s — by which time price
        # had already reversed past the structure level. The timer also fired during
        # quiet periods when nothing changed, wasting REST calls.
        #
        # New approach: STRUCTURE-EVENT-DRIVEN trailing.
        #   1. On EVERY tick: detect if ICT structure state has changed since last
        #      trail computation (new BOS, CHoCH, swing, OB, or significant price move).
        #   2. If structure changed OR price made new high/low: compute new trail SL
        #      locally (pure math, no REST call — sub-millisecond).
        #   3. Only dispatch REST call to exchange when computation yields an actual
        #      SL improvement. One-in-flight guard prevents duplicate edits.
        #   4. Minimum 3s cooldown between successful REST trail moves to prevent
        #      exchange rate-limit exhaustion during rapid structural cascades.
        #
        # This gives us:
        #   - ZERO missed structure events (every BOS/CHoCH/swing detected immediately)
        #   - ZERO wasted REST calls (only fires when SL actually needs to move)
        #   - Institutional-grade responsiveness to market structure shifts
        if self.get_trail_enabled():
            # ── Step 1: Detect structure change or new price extreme ──────
            _structure_changed = self._detect_structure_change(data_manager, price, pos, now)
            _new_extreme = False
            if pos.side == "long" and price > pos.peak_price_abs:
                _new_extreme = True
            elif pos.side == "short" and (pos.peak_price_abs < 1e-10 or price < pos.peak_price_abs):
                _new_extreme = True

            # Also trigger on significant price moves (>0.15 ATR since last trail check)
            _atr_now = self._atr_5m.atr if self._atr_5m else 0.0
            _price_moved = False
            if _atr_now > 1e-10:
                _last_trail_px = getattr(self, '_last_trail_check_price', 0.0)
                if abs(price - _last_trail_px) > 0.15 * _atr_now:
                    _price_moved = True

            _should_trail = _structure_changed or _new_extreme or _price_moved

            if _should_trail:
                self._last_trail_check_price = price
                # ── Step 2: Check minimum REST cooldown (3s between moves) ────
                _min_trail_rest_cd = 3.0
                _last_trail_success = getattr(self, '_last_trail_rest_time', 0.0)
                _rest_ok = (now - _last_trail_success) >= _min_trail_rest_cd

                if _rest_ok:
                    # ── Step 3: One-in-flight guard ───────────────────────────
                    with self._lock:
                        if self._trail_in_progress:
                            return
                        self._trail_in_progress = True

                    _snap_om  = order_manager
                    _snap_dm  = data_manager
                    _snap_px  = price
                    _snap_now = now

                    def _bg_trail():
                        try:
                            live_price = _snap_dm.get_last_price()
                            live_now   = time.time()
                            if live_price < 1.0:
                                live_price = _snap_px
                            moved = self._update_trailing_sl(_snap_om, _snap_dm, live_price, live_now)
                            if moved:
                                self._last_trail_rest_time = time.time()
                        except Exception as _te:
                            logger.error("Trail background error: %s", _te, exc_info=True)
                        finally:
                            self._trail_in_progress = False

                    threading.Thread(target=_bg_trail, daemon=True,
                                     name="trail-sl").start()

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

    def _detect_structure_change(self, data_manager, price: float,
                                  pos: 'PositionState', now: float) -> bool:
        """
        Structure-Event Detector for Trail Triggering v6.0
        ===================================================
        Detects whether ICT market structure has CHANGED since the last trail
        computation. This replaces the old time-based TRAIL_INTERVAL_S gate.

        TRACKED STRUCTURE EVENTS (any one = True):
          1. BOS direction change on 1m, 5m, or 15m
          2. New CHoCH on any timeframe
          3. New Order Block formation (count change)
          4. New FVG formation (count change)
          5. New confirmed swing high/low on 1m or 5m
          6. BOS level change (same direction but new level = structure advanced)
          7. Liquidity pool swept (changes defense landscape)

        FINGERPRINT APPROACH:
          Build a structural fingerprint (tuple of key state values) on each call.
          If fingerprint differs from last stored fingerprint → structure changed.
          This is O(1) comparison regardless of how many structures exist.

        Returns True if structure has changed, False otherwise.
        """
        # Build current structural fingerprint
        _fp_parts = []

        # ── ICT engine structure state ────────────────────────────────────
        if self._ict is not None and getattr(self._ict, '_initialized', False):
            try:
                for _tf_name in ("1m", "5m", "15m"):
                    _tf_st = self._ict._tf.get(_tf_name)
                    if _tf_st is None:
                        _fp_parts.append((_tf_name, None, None, 0.0))
                        continue
                    _bos_d = getattr(_tf_st, 'bos_direction', None)
                    _bos_l = getattr(_tf_st, 'bos_level', 0.0)
                    _bos_ts = getattr(_tf_st, 'bos_timestamp', 0)
                    _choch_d = getattr(_tf_st, 'choch_direction', None)
                    _choch_l = getattr(_tf_st, 'choch_level', 0.0)
                    # Round level to tick to avoid float noise triggering false changes
                    _bos_l_r = round(_bos_l, 1) if _bos_l else 0.0
                    _choch_l_r = round(_choch_l, 1) if _choch_l else 0.0
                    _fp_parts.append((_tf_name, _bos_d, _bos_l_r, _bos_ts,
                                      _choch_d, _choch_l_r))
            except Exception:
                _fp_parts.append(("ict_err",))

            # OB and FVG counts (new formation = count change)
            try:
                _n_ob_bull = len([o for o in self._ict.order_blocks_bull
                                  if o.is_active(int(now * 1000))])
                _n_ob_bear = len([o for o in self._ict.order_blocks_bear
                                  if o.is_active(int(now * 1000))])
                _n_fvg_bull = len([f for f in self._ict.fvgs_bull
                                   if f.is_active(int(now * 1000))])
                _n_fvg_bear = len([f for f in self._ict.fvgs_bear
                                   if f.is_active(int(now * 1000))])
                _fp_parts.append(("ob_fvg", _n_ob_bull, _n_ob_bear,
                                  _n_fvg_bull, _n_fvg_bear))
            except Exception:
                pass

            # Liquidity pool sweep events
            try:
                _n_swept = sum(1 for p in self._ict.liquidity_pools
                               if getattr(p, 'swept', False))
                _fp_parts.append(("swept", _n_swept))
            except Exception:
                pass

        # ── 1m swing structure (new swing = structure changed) ────────────
        try:
            _c1m = data_manager.get_candles("1m", limit=15)
            if _c1m and len(_c1m) >= 6:
                _cl = _c1m[:-1]
                _sh, _sl = _ict_find_swings_inline(_cl, min(3, len(_cl) - 2))
                # Use last 2 swing values as fingerprint
                _last_sh = round(_sh[-1], 1) if _sh else 0.0
                _last_sl = round(_sl[-1], 1) if _sl else 0.0
                _fp_parts.append(("sw1m", _last_sh, _last_sl))
        except Exception:
            pass

        # ── 5m swing structure ────────────────────────────────────────────
        try:
            _c5m = data_manager.get_candles("5m", limit=15)
            if _c5m and len(_c5m) >= 6:
                _cl5 = _c5m[:-1]
                _sh5, _sl5 = _ict_find_swings_inline(_cl5, min(4, len(_cl5) - 2))
                _last_sh5 = round(_sh5[-1], 1) if _sh5 else 0.0
                _last_sl5 = round(_sl5[-1], 1) if _sl5 else 0.0
                _fp_parts.append(("sw5m", _last_sh5, _last_sl5))
        except Exception:
            pass

        # ── Build fingerprint and compare ─────────────────────────────────
        _current_fp = tuple(_fp_parts)
        _last_fp = getattr(self, '_last_structure_fingerprint', None)
        self._last_structure_fingerprint = _current_fp

        if _last_fp is None:
            # First call — no previous state to compare
            return True

        return _current_fp != _last_fp

    def _update_trailing_sl(self, order_manager, data_manager, price, now) -> bool:
        """Institutional trail v4.9 + Issue-2 fix: ICT refreshed from live candles."""
        pos = self._pos; atr = self._atr_5m.atr
        if atr < 1e-10: return False
        if pos.entry_price < 1.0:
            logger.warning("Trail: entry_price invalid (%.2f) — skipping", pos.entry_price)
            return False
        if not pos.sl_order_id:
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
        now_ms = int(now * 1000)

        # ── Issue 2 fix: Refresh ICT engine with live 1m/5m structure ────────
        # ROOT CAUSE: ICT update() only ran in _evaluate_entry() (FLAT phase).
        # During an active position, _manage_active() runs instead, so the ICT
        # engine never saw new OBs, BOS events, or FVGs formed after entry.
        # Zone-freeze was comparing price against entry-time OBs — some of which
        # might permanently overlap the trail zone, freezing the trail forever.
        # Fix: force ICT update here with live candles before every trail decision.
        # Now also passes candles_1m so fresh 1m OBs are detected for trail anchoring.
        _trail_candles_15m = None   # v8.0: captured for trail engine Phase 3
        if self._ict is not None:
            try:
                # Trail ICT update MUST use the same history depth as the
                # main entry-phase update. Using limit=30 for 5m (2.5h) while
                # 5m OBs live 24h meant the trail engine had a completely
                # different (and much shallower) view of structure than the
                # entry engine — causing zone-freeze to compare trail price
                # against OBs the entry engine had but the trail engine dropped.
                candles_15m = data_manager.get_candles("15m", limit=200)
                _trail_candles_15m = candles_15m   # v8.0: save for trail engine
                _trail_5m   = data_manager.get_candles("5m",  limit=300)
                _trail_1m   = data_manager.get_candles("1m",  limit=120)
                self._ict.update(_trail_5m, candles_15m, price, now_ms,
                                 candles_1m=_trail_1m,
                                 candles_1h=data_manager.get_candles("1h", limit=100),
                                 candles_4h=data_manager.get_candles("4h", limit=50),
                                 candles_1d=data_manager.get_candles("1d", limit=30))
            except Exception as _ict_refresh_e:
                logger.debug(f"Trail ICT refresh error (non-fatal): {_ict_refresh_e}")

        # ══════════════════════════════════════════════════════════════════
        # RATCHET REMOVED — v2.0 Chandelier trail handles all trailing.
        # Keep metrics for display/heartbeat compatibility.
        # ══════════════════════════════════════════════════════════════════
        init_dist_r = pos.initial_sl_dist if pos.initial_sl_dist > 1e-10 else atr
        mfe_r = pos.peak_profit / init_dist_r if init_dist_r > 1e-10 else 0.0
        _fee_buf = pos.entry_price * QCfg.COMMISSION_RATE() * 2.0 + 0.15 * atr
        _be_floor = (pos.entry_price + _fee_buf if pos.side == "long"
                     else pos.entry_price - _fee_buf)

        # ══════════════════════════════════════════════════════════════════
        # v5.1: COUNTER-TREND BOS STRUCTURAL INVALIDATION
        # ══════════════════════════════════════════════════════════════════
        if self._ict is not None and not pos.be_ratchet_applied:
            try:
                _tf_5m = self._ict._tf.get("5m")
                if _tf_5m is not None:
                    _counter_bos = False
                    if (pos.side == "long" and _tf_5m.bos_direction == "bearish"
                            and _tf_5m.bos_level < pos.entry_price):
                        _counter_bos = True
                    elif (pos.side == "short" and _tf_5m.bos_direction == "bullish"
                            and _tf_5m.bos_level > pos.entry_price):
                        _counter_bos = True
                    if _counter_bos:
                        _sl_worse = ((pos.side == "long" and pos.sl_price < _be_floor) or
                                     (pos.side == "short" and pos.sl_price > _be_floor))
                        if _sl_worse and profit > 0:
                            _be_tick = _round_to_tick(_be_floor)
                            if pos.side == "long" and _be_tick >= price:
                                _be_tick = _round_to_tick(price - 0.5 * atr)
                            elif pos.side == "short" and _be_tick <= price:
                                _be_tick = _round_to_tick(price + 0.5 * atr)
                            _is_better = ((pos.side == "long" and _be_tick > pos.sl_price) or
                                          (pos.side == "short" and _be_tick < pos.sl_price))
                            if _is_better:
                                logger.warning(
                                    f"🚨 COUNTER-BOS: 5m {_tf_5m.bos_direction} "
                                    f"@ ${_tf_5m.bos_level:,.0f} → BE ${_be_tick:,.1f}")
                                es = "sell" if pos.side == "long" else "buy"
                                result = order_manager.replace_stop_loss(
                                    existing_sl_order_id=pos.sl_order_id,
                                    side=es, quantity=pos.quantity,
                                    new_trigger_price=_be_tick)
                                if result is None:
                                    self._record_exchange_exit(None); return True
                                if isinstance(result, dict) and "error" not in result:
                                    with self._lock:
                                        pos.sl_price = _be_tick
                                        pos.sl_order_id = result.get("order_id") or pos.sl_order_id
                                        pos.trail_active = True
                                        pos.be_ratchet_applied = True
                                        self.current_sl_price = _be_tick
                                    send_telegram_message(
                                        f"🚨 <b>COUNTER-BOS → BE</b>\n"
                                        f"5m BOS {_tf_5m.bos_direction} @ ${_tf_5m.bos_level:,.0f}\n"
                                        f"SL → ${_be_tick:,.2f}")
                                return False
            except Exception as _bos_e:
                logger.debug(f"Counter-BOS check error (non-fatal): {_bos_e}")

        # ══════════════════════════════════════════════════════════════════
        # v6.0: DYNAMIC TRAIL ENGINE — replaces static ICTTrailEngine.
        # Every call reads live AMD phase, ATR percentile, OBs/FVGs,
        # swing structure, liquidity pools, and delivery target proximity.
        # No hardcoded phases, no fixed ATR multiples.
        # ══════════════════════════════════════════════════════════════════
        # v8.0: fetch candles_15m for trail engine if not already fetched via ICT refresh
        if _trail_candles_15m is None:
            try:
                _trail_candles_15m = data_manager.get_candles("15m", limit=100)
            except Exception:
                _trail_candles_15m = None

        _hold_reason: List[str] = []
        _anchor_out: List[str] = []
        _atr_pctile = self._atr_5m.get_percentile()
        _adx_now    = self._adx.adx

        new_sl = _ICTStructureTrail.compute(
            pos_side        = pos.side,
            price           = price,
            entry_price     = pos.entry_price,
            current_sl      = pos.sl_price,
            atr             = atr,
            initial_sl_dist = pos.initial_sl_dist,
            peak_profit     = pos.peak_profit,
            peak_price_abs  = pos.peak_price_abs,
            hold_seconds    = hold_secs,
            candles_1m      = candles_1m,
            candles_5m      = candles_5m,
            orderbook       = orderbook,
            entry_vol       = pos.entry_vol,
            trade_mode      = pos.trade_mode,
            ict_engine      = self._ict,
            now_ms          = now_ms,
            hold_reason     = _hold_reason,
            atr_percentile  = _atr_pctile,
            adx             = _adx_now,
            tick_size       = QCfg.TICK_SIZE(),
            candles_15m     = _trail_candles_15m,
            anchor_out      = _anchor_out,
        )

        if new_sl is None:
            pos.consecutive_trail_holds += 1
            _trail_log_interval = 30.0
            if now - self._last_trail_block_log >= _trail_log_interval:
                self._last_trail_block_log = now
                init_dist = pos.initial_sl_dist if pos.initial_sl_dist > 1e-10 else atr
                tier      = max(profit, pos.peak_profit) / init_dist if init_dist > 1e-10 else 0.0
                hm        = (now - pos.entry_time) / 60.0
                _reason_str = " | ".join(_hold_reason) if _hold_reason else f"tier={tier:.2f}R<ACT"
                _vol_label  = f"pctile={_atr_pctile:.0%}"
                logger.info(
                    f"🔒 Trail HOLD | {_reason_str} | "
                    f"profit={profit:.1f}pts peak={pos.peak_profit:.1f}pts | "
                    f"SL=${pos.sl_price:,.1f} | hold={hm:.0f}m | {_vol_label}")
            return False

        new_sl_tick = _round_to_tick(new_sl)

        # ── Guard: skip API call if tick-rounded value is identical to current SL ──
        # ICTTrailEngine.compute() guarantees new_sl > current_sl (for SHORT: <) before
        # rounding, but _round_to_tick() can snap it back to the same tick value as
        # pos.sl_price. Without this guard, the trail fires ~10 REST calls/minute to the
        # exchange setting the SL to the value it already is — burning rate limit budget.
        if abs(new_sl_tick - pos.sl_price) < 1e-6:
            return False

        # Phase label derived from R-multiple
        init_dist = pos.initial_sl_dist if pos.initial_sl_dist > 1e-10 else atr
        tier = max(profit, pos.peak_profit) / init_dist if init_dist > 1e-10 else 0.0

        # v6.0: Extract anchor reason from compute() output
        _anchor_label = ""
        if _anchor_out:
            _anchor_label = f" [{_anchor_out[0]}]"

        # Tier label derived from R-multiple (matches external engine's phase logic)
        if tier >= 2.0:
            phase_label = "🟢 P3"
        elif tier >= 1.0:
            phase_label = "🟠 P2"
        elif tier >= 0.45:
            phase_label = "🟡 P1"
        else:
            phase_label = "⬜ P0"

        # Vol regime label (inline — no dependency on embedded methods)
        _vol_tag = ("extreme" if _atr_pctile > 0.90 else
                    "elevated" if _atr_pctile > 0.70 else
                    "normal"   if _atr_pctile > 0.30 else "quiet")
        hm = (now - pos.entry_time) / 60.0
        _improvement = new_sl_tick - pos.sl_price if pos.side == "long" else pos.sl_price - new_sl_tick

        # v6.0: Compute margin % P&L for reporting
        _margin_pnl_pct = 0.0
        _margin_used = 0.0
        try:
            if pos.entry_price > 0 and pos.quantity > 0:
                _notional = pos.entry_price * pos.quantity
                _leverage = QCfg.LEVERAGE()
                _margin_used = _notional / _leverage if _leverage > 0 else _notional
                if _margin_used > 1e-10:
                    # Current unrealised P&L in USD
                    _unrealised_pnl = profit * pos.quantity
                    _margin_pnl_pct = (_unrealised_pnl / _margin_used) * 100.0
        except Exception:
            pass

        # v6.0: Compute what SL lock means for worst-case margin %
        _sl_locked_pnl_pct = 0.0
        try:
            _new_sl_dist = abs(new_sl_tick - pos.entry_price)
            _new_sl_profit = (pos.entry_price - new_sl_tick) if pos.side == "short" else (new_sl_tick - pos.entry_price)
            if _margin_used > 1e-10 and pos.quantity > 0:
                _sl_locked_pnl_pct = (_new_sl_profit * pos.quantity / _margin_used) * 100.0
        except Exception:
            pass

        logger.info(
            f"🔒 Trail [{phase_label}]{_anchor_label} "
            f"${pos.sl_price:,.1f} → ${new_sl_tick:,.1f} (+{_improvement:.1f}pts) | "
            f"R={tier:.2f}R MFE={pos.peak_profit:.1f}pts hold={hm:.0f}m | "
            f"margin%={_margin_pnl_pct:+.1f}% SL-lock={_sl_locked_pnl_pct:+.1f}% vol={_vol_tag}")
        send_telegram_message(
            f"🔒 <b>TRAIL SL</b> [{phase_label}]{_anchor_label}\n"
            f"${pos.sl_price:,.2f} → ${new_sl_tick:,.2f} (+{_improvement:.1f}pts)\n"
            f"R: {tier:.2f}R | MFE: {pos.peak_profit:.1f}pts | Hold: {hm:.0f}m\n"
            f"Margin PnL: {_margin_pnl_pct:+.1f}% | SL locks: {_sl_locked_pnl_pct:+.1f}%\n"
            f"Vol: {_vol_tag} ({_atr_pctile:.0%} pctile) | ATR: ${atr:.1f} | ADX: {_adx_now:.0f}")

        es = "sell" if pos.side=="long" else "buy"
        result = order_manager.replace_stop_loss(existing_sl_order_id=pos.sl_order_id, side=es, quantity=pos.quantity, new_trigger_price=new_sl_tick)
        if result is None:
            logger.warning("🚨 SL already fired"); self._record_exchange_exit(None); return True
        if isinstance(result, dict) and "error" in result: return False
        if result and isinstance(result, dict):
            # FIX 3b: acquire lock before writing shared position state.
            # The trail thread and main thread both read sl_price/sl_order_id
            # — a lock-free write produces torn reads (old ID with new price).
            with self._lock:
                self._pos.sl_price = new_sl_tick
                self._pos.sl_order_id = result.get("order_id", pos.sl_order_id)
                self.current_sl_price = new_sl_tick
                self._pos.consecutive_trail_holds = 0
                if not pos.trail_active:
                    self._pos.trail_active = True
            if not pos.trail_active:
                logger.info("✅ Trailing SL active")
                send_telegram_message("✅ Trailing SL now active")
        return True  # v6.0: SL moved — signal success for REST cooldown tracking

    def _exit_trade(self, order_manager, price, reason):
        pos = self._pos
        if pos.phase != PositionPhase.ACTIVE: return
        logger.info(f"🚪 EXIT {pos.side.upper()} @ ${price:,.2f} | {reason}")
        self._pos.phase = PositionPhase.EXITING
        self._exiting_since = time.time()
        order_manager.cancel_all_exit_orders(sl_order_id=pos.sl_order_id, tp_order_id=pos.tp_order_id)
        es = "sell" if pos.side=="long" else "buy"
        order_manager.place_market_order(side=es, quantity=pos.quantity, reduce_only=True)

        # FIX Bug-D: do NOT call _record_pnl here with an estimated PnL.
        # _record_exchange_exit() (called by the reconcile / sync path once the
        # exchange confirms the position is flat) will record the exact PnL.
        # Calling _record_pnl here first created a guaranteed duplicate: this
        # estimated entry fires immediately, then the exact entry fires ~1–30 s
        # later when _sync_position or _reconcile_apply confirms the close.
        # The _pnl_recorded_for guard would drop the second call — but the first
        # call was the ESTIMATED one. Now we always record the exact value first.
        #
        # If the exchange confirmation never arrives (network failure), the
        # EXITING watchdog fires after 120s and calls _finalise_exit() which
        # records PnL=0 via the "unconfirmed" path — acceptable fallback.
        # Telegram message still uses local price estimate for immediacy.
        fill_type = getattr(pos, 'entry_fill_type', 'taker')
        pnl_est = self._estimate_pnl(pos, price, entry_fill_type=fill_type)

        hold_min     = (time.time() - pos.entry_time) / 60.0
        init_sl_dist = pos.initial_sl_dist if pos.initial_sl_dist > 1e-10 else abs(pos.entry_price - pos.sl_price)
        raw_pts      = (price - pos.entry_price) if pos.side == "long" else (pos.entry_price - price)
        achieved_r   = raw_pts / init_sl_dist if init_sl_dist > 1e-10 else 0.0
        tp_dist      = abs(pos.tp_price - pos.entry_price) if pos.tp_price > 0 else 0.0
        planned_rr   = tp_dist / init_sl_dist if init_sl_dist > 1e-10 else 0.0
        result_icon  = "✅" if pnl_est > 0 else "❌"

        send_telegram_message(
            f"🚪 <b>CLOSING POSITION — {reason.upper()}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Side:     {pos.side.upper()} [{pos.trade_mode.upper()}]\n"
            f"Entry:    ${pos.entry_price:,.2f}\n"
            f"Est exit: ~${price:,.2f}  ({'+' if raw_pts>=0 else ''}{raw_pts:.1f} pts)\n"
            f"Est PnL:  ~${pnl_est:+.2f} USDT\n"
            f"<i>Awaiting exchange confirmation...</i>"
        )
        self._last_exit_side = pos.side
        # _finalise_exit() is NOT called here — let _sync_position / _reconcile_apply
        # confirm the position is flat on the exchange and then call _record_exchange_exit,
        # which records exact PnL and calls _finalise_exit().  The EXITING watchdog (120s)
        # is the safety net if exchange confirmation never arrives.

    def _record_exchange_exit(self, ex_pos):
        """
        v5.1: Exchange-confirmed exit only. No price heuristics. No estimated fees.

        Calls identify_exit_order() which queries GET /v2/orders/{id} for both
        the SL and TP order IDs directly — state:"closed" + paid_commission from
        the exchange response. One retry after 1 s if both orders still show open
        (covers the sub-second propagation window between fill and state update).

        If after the retry the exchange still cannot confirm which order closed:
          - Position state is finalised to FLAT (mandatory — prevents orphaned state)
          - PnL recorded as 0.0 with confirmed=False in the trade record
          - Telegram alert sent with both order IDs for manual reconciliation
          - Operator should verify on the Delta dashboard

        When confirmed (normal path):
          - exit_type, fill_price, fee_paid all from exchange (exact)
          - Gross PnL computed from actual fill price using exact inverse-perp formula
          - Exit fee = paid_commission from Delta (exact USD)
          - Entry fee = commission_rate × entry_notional (exact rate, estimated value
            because we do not yet store paid_commission at entry order placement)
          - fee_breakdown.exact_fees = True signals that exit side is exact
        """
        pos = self._pos
        if pos.phase == PositionPhase.FLAT:
            logger.debug("_record_exchange_exit skipped — already FLAT")
            return

        # ── ATOMIC EXIT CLAIM ──────────────────────────────────────────────
        # ROOT CAUSE OF DOUBLE NOTIFICATION (observed in logs):
        #   11:47:18.152  sync thread    → enters _record_exchange_exit, sees _exit_completed=False
        #   11:47:18.775  reconcile thread → enters _record_exchange_exit, sees _exit_completed=False
        #   11:47:18.906  sync thread    → finishes identify_exit_order, logs, sends telegram, records PnL
        #   11:47:19.659  reconcile thread → finishes identify_exit_order, logs AGAIN, sends telegram AGAIN
        #
        # The old guard (checking _exit_completed without a lock) was non-atomic:
        # both threads read False before either set True.  _exit_completed was only
        # set inside _record_pnl() which runs AFTER identify_exit_order() (~1s of I/O).
        #
        # FIX: Atomic claim under the lock. The FIRST thread to arrive sets
        # _exit_completed=True and proceeds. All others bail immediately.
        # This happens BEFORE any I/O, logging, or telegram sends.
        with self._lock:
            if self._exit_completed:
                logger.info(
                    "_record_exchange_exit skipped — exit already claimed by another thread "
                    f"(phase={pos.phase.name})")
                return
            self._exit_completed = True   # CLAIM: this thread owns the exit

        # ─── Step 1: Get exchange-confirmed exit data ──────────────────────────
        # Query both order IDs directly. One retry after 1 s.
        exit_info: Dict = {"confirmed": False}

        if self._om is not None:
            try:
                exit_info = self._om.identify_exit_order(
                    sl_order_id  = pos.sl_order_id,
                    tp_order_id  = pos.tp_order_id,
                    trail_active = pos.trail_active,
                )
            except Exception as e:
                logger.error(f"identify_exit_order (attempt 1) error: {e}", exc_info=True)

            # v6.0: Exponential backoff retry — 4 additional attempts (1s, 2s, 3s, 5s)
            # Exchange state propagation can take up to 5-8s under load.
            # Old single 1s retry missed ~40% of confirmations (observed in prod logs).
            _retry_delays = [1.0, 2.0, 3.0, 5.0]
            for _retry_idx, _delay in enumerate(_retry_delays):
                if exit_info.get("confirmed"):
                    break
                time.sleep(_delay)
                try:
                    exit_info = self._om.identify_exit_order(
                        sl_order_id  = pos.sl_order_id,
                        tp_order_id  = pos.tp_order_id,
                        trail_active = pos.trail_active,
                    )
                    if exit_info.get("confirmed"):
                        logger.info(f"✅ Exit confirmed on retry {_retry_idx + 2} (after {_delay}s)")
                except Exception as e:
                    logger.error(f"identify_exit_order (retry {_retry_idx + 2}) error: {e}", exc_info=True)

        if not exit_info.get("confirmed"):
            # v6.0: Final fallback — query exchange position directly.
            # If position is flat on exchange, we know SL or TP fired even if
            # individual order state queries failed.
            _try_position_fallback = False
            if self._om is not None:
                try:
                    _ex_pos = self._om.get_position()
                    if _ex_pos is not None:
                        _ex_qty = abs(float(_ex_pos.get("size", _ex_pos.get("quantity", 0))))
                        if _ex_qty < 1e-10:
                            _try_position_fallback = True
                            logger.info("Position is FLAT on exchange — exit occurred, reconstructing")
                except Exception as _pos_e:
                    logger.debug(f"Position fallback check error: {_pos_e}")

            if _try_position_fallback:
                # Position is confirmed flat — reconstruct exit from available data
                _last_price = 0.0
                try:
                    _last_price = self._dm.get_last_price() if self._dm else 0.0
                except Exception:
                    pass
                _approx_pnl = 0.0
                if _last_price > 0 and pos.entry_price > 0:
                    if pos.side == "long":
                        _approx_pnl = (_last_price - pos.entry_price) * pos.quantity
                    else:
                        _approx_pnl = (pos.entry_price - _last_price) * pos.quantity
                logger.warning(
                    f"⚠️ EXIT CONFIRMED via position check (order state unavailable). "
                    f"Approx PnL: ${_approx_pnl:.2f}")
                send_telegram_message(
                    f"⚠️ <b>EXIT CONFIRMED (position fallback)</b>\n"
                    f"Individual order state unavailable but position is FLAT.\n"
                    f"Approx PnL: ${_approx_pnl:.2f}\n"
                    f"Entry: ${pos.entry_price:,.2f}")
                self._record_pnl(_approx_pnl, exit_reason="confirmed_via_position",
                                 exit_price=_last_price, fee_breakdown=None)
                self._last_exit_side = pos.side
                self._finalise_exit()
                return

            # Truly unconfirmed after all retries — record zero PnL
            _sl_disp = str(pos.sl_order_id or "unknown")
            _tp_disp = str(pos.tp_order_id or "unknown")
            logger.warning(
                f"⚠️ EXIT UNCONFIRMED after {len(_retry_delays)+1} attempts — closing FLAT with pnl=0. "
                f"SL order={_sl_disp} TP order={_tp_disp}"
            )
            send_telegram_message(
                f"⚠️ <b>EXIT UNCONFIRMED</b>\n"
                f"Exchange did not confirm after {len(_retry_delays)+1} attempts ({sum(_retry_delays)+0:.0f}s).\n"
                f"PnL recorded as $0.00 — verify on Delta dashboard.\n"
                f"Entry: ${pos.entry_price:,.2f} | "
                f"SL: {_sl_disp} | TP: {_tp_disp}"
            )
            self._record_pnl(0.0, exit_reason="unconfirmed", exit_price=0.0,
                             fee_breakdown=None)
            self._last_exit_side = pos.side
            self._finalise_exit()
            return

        # ─── Exchange-confirmed ─────────────────────────────────────────────────
        exit_type  = exit_info["exit_type"]          # "tp" | "sl" | "trail_sl"
        fill_price = float(exit_info["fill_price"])  # exact execution price
        fee_paid   = float(exit_info["fee_paid"])    # paid_commission from Delta
        fired_id   = exit_info["order_id"]

        if exit_type == "tp":
            exit_reason = "tp_hit";       is_tp_hit = True;  is_sl_hit = False
        elif exit_type == "trail_sl":
            exit_reason = "trail_sl_hit"; is_tp_hit = False; is_sl_hit = True
        else:
            exit_reason = "sl_hit";       is_tp_hit = False; is_sl_hit = True

        _disp = (fired_id[:10] + "…") if len(fired_id) > 10 else fired_id
        logger.info(
            f"✅ Exit confirmed: {exit_reason} @ ${fill_price:,.2f} "
            f"fee=${fee_paid:.4f} order={_disp}"
        )

        # ─── Step 2: PnL — exact gross from actual fill, exact exit fee ────────
        import config as _cfg_x
        _is_delta = (
            getattr(_cfg_x, "EXECUTION_EXCHANGE", "").lower() == "delta"
            and getattr(_cfg_x, "DELTA_SYMBOL", "BTCUSD").upper() == "BTCUSD"
        )

        if _is_delta and fill_price > 0:
            # Exact inverse-perpetual formula for Delta BTCUSD (1 USD per contract)
            usd_contracts = pos.quantity * pos.entry_price
            if pos.side == "long":
                gross_btc = usd_contracts * (1.0 / pos.entry_price - 1.0 / fill_price)
            else:
                gross_btc = usd_contracts * (1.0 / fill_price - 1.0 / pos.entry_price)
            gross = gross_btc * fill_price
        else:
            # Linear (USDT-margined) — CoinSwitch or fill_price unavailable
            gross = ((fill_price - pos.entry_price) if pos.side == "long"
                     else (pos.entry_price - fill_price)) * pos.quantity

        # Entry fee: prefer exact paid_commission captured at entry (v8.1).
        # Fallback: commission_rate × entry_notional (rate-exact, value estimated).
        _entry_fee_exact = getattr(pos, "entry_fee_paid", 0.0) or 0.0
        entry_fee_is_exact = _entry_fee_exact > 0
        if entry_fee_is_exact:
            entry_fee = _entry_fee_exact
        else:
            fill_type  = getattr(pos, "entry_fill_type", "taker")
            # FIX Bug-A: use exchange-specific maker rate.
            # Delta maker = rebate (negative); CoinSwitch maker = positive cost.
            if fill_type == "maker":
                if _is_delta:
                    entry_rate = float(getattr(_cfg_x, "DELTA_COMMISSION_RATE_MAKER", -0.00020))
                else:
                    entry_rate = float(getattr(_cfg_x, "COMMISSION_RATE_MAKER",
                                               QCfg.COMMISSION_RATE() * 0.40))
            else:
                entry_rate = (float(getattr(_cfg_x, "DELTA_COMMISSION_RATE", 0.00050))
                              if _is_delta else QCfg.COMMISSION_RATE())
            entry_fee = pos.entry_price * pos.quantity * entry_rate
        exit_fee  = fee_paid   # EXACT from Delta paid_commission

        pnl = gross - entry_fee - exit_fee

        _entry_tag = "exact" if entry_fee_is_exact else "rate-est"
        fee_breakdown: Dict = {
            "gross_pnl":  round(gross, 4),
            "entry_fee":  round(entry_fee, 4),
            "exit_fee":   round(exit_fee, 4),    # exact from paid_commission
            "total_fees": round(entry_fee + exit_fee, 4),
            "net_pnl":    round(pnl, 4),
            "exact_fees": entry_fee_is_exact,  # True = both sides exact
        }

        logger.info(
            f"📊 Exit price=${fill_price:,.2f} reason={exit_reason} "
            f"entry=${pos.entry_price:,.2f} gross=${gross:+.4f} "
            f"entry_fee=${entry_fee:.4f}({_entry_tag}) exit_fee=${exit_fee:.4f}(exact) "
            f"net=${pnl:+.4f}"
        )

        # ─── Step 3: Record PnL and trade history ─────────────────────────────
        self._record_pnl(pnl, exit_reason=exit_reason, exit_price=fill_price,
                         fee_breakdown=fee_breakdown)

        # ─── Step 4: Telegram notification ────────────────────────────────────
        hold_min     = (time.time() - pos.entry_time) / 60.0 if pos.entry_time > 0 else 0.0
        init_sl_dist = (pos.initial_sl_dist if pos.initial_sl_dist > 1e-10
                        else abs(pos.entry_price - pos.sl_price))
        raw_pts      = ((fill_price - pos.entry_price) if pos.side == "long"
                        else (pos.entry_price - fill_price))
        achieved_r   = raw_pts / init_sl_dist if init_sl_dist > 1e-10 else 0.0

        if is_tp_hit:
            result_icon = "🎯"; result_label = "TP HIT";   result_color = "WIN ✅"
        elif is_sl_hit and pnl > 0:
            result_icon = "🔒"
            result_label = "TRAIL SL (profitable)" if pos.trail_active else "SL HIT (profitable)"
            result_color = "WIN ✅"
        elif is_sl_hit and pos.trail_active:
            result_icon = "🔒"; result_label = "TRAIL SL"; result_color = "LOSS ❌"
        else:
            result_icon = "🛑"; result_label = "SL HIT";   result_color = "LOSS ❌"

        mfe_r      = pos.peak_profit / init_sl_dist if init_sl_dist > 1e-10 else 0.0
        tp_dist    = abs(pos.tp_price - pos.entry_price) if pos.tp_price > 0 else 0.0
        planned_rr = tp_dist / init_sl_dist if init_sl_dist > 1e-10 else 0.0
        _orig_sl   = ((pos.entry_price - init_sl_dist) if pos.side == "long"
                      else (pos.entry_price + init_sl_dist))
        _trail_imp = abs(pos.sl_price - _orig_sl) if pos.trail_active else 0.0

        # v6.0: Margin-based P&L %
        _exit_margin_pct = 0.0
        _exit_margin_used = 0.0
        try:
            if pos.entry_price > 0 and pos.quantity > 0:
                _exit_notional = pos.entry_price * pos.quantity
                _exit_lev = QCfg.LEVERAGE()
                _exit_margin_used = _exit_notional / _exit_lev if _exit_lev > 0 else _exit_notional
                if _exit_margin_used > 1e-10:
                    _exit_margin_pct = (pnl / _exit_margin_used) * 100.0
        except Exception:
            pass

        send_telegram_message(
            f"{result_icon} <b>{result_color} — {result_label}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Side:     {pos.side.upper()} [{pos.trade_mode.upper()}]\n"
            f"Entry:    ${pos.entry_price:,.2f}\n"
            f"Exit:     <b>${fill_price:,.2f}</b>  ({'+' if raw_pts>=0 else ''}{raw_pts:.1f} pts)\n"
            f"Gross:    ${gross:+.4f}\n"
            f"Fees:     ${entry_fee + exit_fee:.4f} "
            f"(exit exact ${exit_fee:.4f} + entry {_entry_tag} ${entry_fee:.4f})\n"
            f"PnL:      <b>${pnl:+.2f} USDT</b>  ({_exit_margin_pct:+.1f}% on ${_exit_margin_used:.2f} margin)\n"
            f"R:        {achieved_r:+.2f}R  (planned 1:{planned_rr:.2f}R)\n"
            f"MFE:      {mfe_r:.2f}R  |  Hold: {hold_min:.1f}m\n"
            + (f"Trail:    ✅ SL moved {_trail_imp:+.1f}pts vs orig\n"
               if pos.trail_active else "Trail:    — not activated\n") +
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>Session: {self._total_trades}T | WR: {self._win_rate():.0%} | "
            f"Total PnL: ${self._total_pnl:+.2f}</i>"
        )
        self._last_exit_side = pos.side
        self._finalise_exit()

    def _record_pnl(self, pnl: float, exit_reason: str = "unknown",
                    exit_price: float = 0.0,
                    fee_breakdown: Optional[Dict] = None) -> bool:
        """
        Record a completed trade. Returns True if recorded, False if duplicate.

        _total_trades incremented HERE (at close), not at entry — ensures
        win-rate denominator only counts closed trades.

        IDEMPOTENCY: entry_time-based guard. Each position's PnL is recorded
        exactly once. The _exit_completed flag is NOT checked here — it is used
        as an atomic entry barrier in _record_exchange_exit() to prevent
        concurrent threads from both entering that function.

        Returns False (no-op) if this position's PnL was already recorded.
        """
        pos = self._pos
        pos_entry_time = getattr(pos, 'entry_time', 0.0)

        # Entry_time idempotency: prevents double-counting if somehow called twice
        if pos_entry_time > 0 and abs(self._pnl_recorded_for - pos_entry_time) < 0.001:
            logger.warning(
                f"_record_pnl: duplicate call for position entry_time={pos_entry_time:.3f} "
                f"(exit_reason={exit_reason}, pnl={pnl:+.4f}) — skipped to prevent double-count"
            )
            return False

        # ── Record the trade ───────────────────────────────────────────────────
        self._pnl_recorded_for = pos_entry_time

        self._total_trades += 1
        self._total_pnl    += pnl
        is_win = pnl > 0
        if is_win:
            self._winning_trades += 1
        self._risk_gate.record_trade_result(pnl)

        # Full trade record for /trades command
        init_sl_dist = getattr(pos, 'initial_sl_dist', 0.0)
        _fb = fee_breakdown or {}
        self._trade_history.append({
            # ── Core trade data ────────────────────────────────────────────
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
            # ── v6.0: Margin-based P&L % ─────────────────────────────────────
            "margin_pnl_pct": 0.0,  # filled below
            # ── Fee breakdown (exact from Delta /v2/fills, estimated otherwise) ──
            "gross_pnl":    _fb.get("gross_pnl",  pnl),
            "entry_fee":    _fb.get("entry_fee",  0.0),
            "exit_fee":     _fb.get("exit_fee",   0.0),
            "total_fees":   _fb.get("total_fees", 0.0),
            "exact_fees":   _fb.get("exact_fees", False),
            # ── v7.0: Signal attribution — enables post-trade analysis ─────
            # Which tier / signals drove this trade? Track these to learn
            # which combinations actually produce wins vs losses.
            "ict_tier":     getattr(pos, 'ict_entry_tier', ''),
            "regime":       (pos.entry_signal.market_regime
                             if pos.entry_signal else ''),
            "composite":    (round(pos.entry_signal.composite, 4)
                             if pos.entry_signal else 0.0),
            "ict_total":    (round(pos.entry_signal.ict_total, 4)
                             if pos.entry_signal else 0.0),
            "amd_phase":    (pos.entry_signal.amd_phase
                             if pos.entry_signal else ''),
            "amd_bias":     (pos.entry_signal.amd_bias
                             if pos.entry_signal else ''),
            "amd_conf":     (round(pos.entry_signal.amd_conf, 3)
                             if pos.entry_signal else 0.0),
            # BUG-5 FIX: was storing deviation_atr (VWAP distance) under the key
            # "htf_15m" — makes the entire HTF attribution analytics meaningless.
            # Now reads the actual HTF scores captured at entry time from PositionState.
            # entry_htf_15m and entry_htf_4h are set in _enter_trade from self._htf.
            "htf_15m":      round(getattr(pos, 'entry_htf_15m', 0.0), 3),
            "htf_4h":       round(getattr(pos, 'entry_htf_4h',  0.0), 3),
            "vwap_dev_atr": (round(pos.entry_signal.deviation_atr, 3)
                             if pos.entry_signal else 0.0),
            "adx":          (round(pos.entry_signal.adx, 1)
                             if pos.entry_signal else 0.0),
            "n_conf":       (pos.entry_signal.n_confirming
                             if pos.entry_signal else 0),
            "htf_veto":     (pos.entry_signal.htf_veto
                             if pos.entry_signal else False),
            "w_vwap":       (round(pos.entry_signal.w_vwap, 2)
                             if pos.entry_signal and hasattr(pos.entry_signal, 'w_vwap') else 0.0),
            "cvd_ticks":    (pos.entry_signal.cvd_tick_count
                             if pos.entry_signal and hasattr(pos.entry_signal, 'cvd_tick_count') else 0),
            "htf_ict_src":  (pos.entry_signal.htf_ict_source
                             if pos.entry_signal and hasattr(pos.entry_signal, 'htf_ict_source') else False),
        })
        # Keep last 200 trades in memory — in-place trim avoids allocating a new list
        if len(self._trade_history) > 200:
            del self._trade_history[:-200]

        # v6.0: Compute margin-based P&L % and update the record
        _margin_pnl_pct_final = 0.0
        _margin_used_final = 0.0
        try:
            _entry_px = getattr(pos, 'entry_price', 0.0)
            _qty = getattr(pos, 'quantity', 0.0)
            if _entry_px > 0 and _qty > 0:
                _notional_f = _entry_px * _qty
                _lev_f = QCfg.LEVERAGE()
                _margin_used_final = _notional_f / _lev_f if _lev_f > 0 else _notional_f
                if _margin_used_final > 1e-10:
                    _margin_pnl_pct_final = (pnl / _margin_used_final) * 100.0
                    if self._trade_history:
                        self._trade_history[-1]["margin_pnl_pct"] = round(_margin_pnl_pct_final, 2)
        except Exception:
            pass

        # v6.0: Log margin % P&L
        _wl = "WIN" if is_win else "LOSS"
        logger.info(
            f"📊 TRADE {_wl}: PnL=${pnl:+.4f} | margin%={_margin_pnl_pct_final:+.1f}% "
            f"on ${_margin_used_final:.2f} margin | reason={exit_reason} | "
            f"trades={self._total_trades} WR={self._winning_trades}/{self._total_trades} "
            f"session=${self._total_pnl:+.4f}")

        return True

    def _finalise_exit(self):
        if hasattr(self, '_entry_engine') and self._entry_engine is not None:
            self._entry_engine.on_position_closed()
        # CRITICAL: do NOT reset _pnl_recorded_for or _exit_completed here.
        # These guards must persist until a new position opens (in _enter_trade).
        # Resetting them here was the v7.0 root cause of double-counting:
        # _finalise_exit() ran, reset the guard, then a late sync/reconcile
        # thread called _record_pnl() and the guard was open → duplicate.
        self._pos = PositionState(); self._last_exit_time = time.time()
        self.current_sl_price = 0.0; self.current_tp_price = 0.0
        # v6.0: Reset structure-event trail state for next trade
        self._last_structure_fingerprint = None
        self._last_trail_check_price = 0.0
        self._last_trail_rest_time = 0.0
        logger.info("Position closed — FLAT")

    def _compute_quantity(self, risk_manager, price,
                           sig: Optional[SignalBreakdown] = None,
                           ict_tier: str = "",
                           sl_price: Optional[float] = None) -> Optional[float]:
        """
        Confidence-weighted position sizing — v8.1 (QUANT_MARGIN_PCT primary).

        QUANT_MARGIN_PCT is the single controlling parameter for trade size.
        Changing it in config.py takes immediate effect on the next trade.

        Formula:
          margin_alloc = available_balance × QUANT_MARGIN_PCT × total_mult
          qty          = (margin_alloc × LEVERAGE) / price

        Example: balance=$500, QUANT_MARGIN_PCT=0.50, LEVERAGE=30, BTC=$85,000
          margin_alloc = 500 × 0.50 × total_mult = $250 × total_mult
          qty          = (250 × 30) / 85,000 = 0.0882 BTC (at total_mult=1.0)

        Confidence multiplier (total_mult) is applied on top — it scales the
        margin allocation up or down based on ICT tier and signal quality.
        total_mult is always clamped to [0.40, 1.05].

        ICT tier base:
          Tier-S: 1.00×  (full conviction — OTE sweep + AMD confirmed)
          Tier-A: 0.80×  (high conviction — ICT structural alignment)
          Tier-B: 0.65×  (standard quant + ICT confluence gate)
          "":     0.50×  (no ICT tier — reduced exposure)

        Composite score modifier (additive):
          |composite| ≥ 0.70 → +0.10
          |composite| ≥ 0.50 → +0.05
          |composite| <  0.35 → −0.10

        AMD confidence modifier (additive):
          amd_conf ≥ 0.85 → +0.08
          amd_conf ≥ 0.70 → +0.04
          amd_conf <  0.50 → −0.05

        sl_price is used only for informational dollar-risk logging.
        It does not alter the computed quantity.
        """
        step = QCfg.LOT_STEP()

        # ── Tier multiplier ───────────────────────────────────────────────────
        tier_mult = {"S": 1.00, "A": 0.80, "B": 0.65}.get(ict_tier, 0.50)

        # ── Composite score modifier ──────────────────────────────────────────
        comp_mod = 0.0
        if sig is not None:
            abs_comp = abs(sig.composite)
            if   abs_comp >= 0.70: comp_mod = +0.10
            elif abs_comp >= 0.50: comp_mod = +0.05
            elif abs_comp <  0.35: comp_mod = -0.10

        # ── AMD confidence modifier ───────────────────────────────────────────
        amd_mod = 0.0
        if sig is not None and sig.amd_conf > 0:
            if   sig.amd_conf >= 0.85: amd_mod = +0.08
            elif sig.amd_conf >= 0.70: amd_mod = +0.04
            elif sig.amd_conf <  0.50: amd_mod = -0.05

        total_mult = max(0.40, min(1.05, tier_mult + comp_mod + amd_mod))

        # ── Available balance ─────────────────────────────────────────────────
        bal = risk_manager.get_available_balance()
        if bal is None:
            logger.warning("_compute_quantity: get_available_balance returned None")
            return None
        available = float(bal.get("available", 0.0))
        if available < QCfg.MIN_MARGIN_USDT():
            logger.warning(
                f"_compute_quantity: available ${available:.2f} < "
                f"MIN_MARGIN_USDT ${QCfg.MIN_MARGIN_USDT():.2f}"
            )
            return None

        # ── Margin-pct sizing — QUANT_MARGIN_PCT is the sole controlling param ─
        margin_alloc = available * QCfg.MARGIN_PCT() * total_mult
        if margin_alloc < QCfg.MIN_MARGIN_USDT():
            logger.warning(
                f"_compute_quantity: margin_alloc ${margin_alloc:.2f} < "
                f"MIN_MARGIN_USDT ${QCfg.MIN_MARGIN_USDT():.2f} "
                f"(available=${available:.2f} MARGIN_PCT={QCfg.MARGIN_PCT():.2%} "
                f"mult={total_mult:.2f})"
            )
            return None
        qty_raw = (margin_alloc * QCfg.LEVERAGE()) / price

        # ── Lot-step + hard limits ────────────────────────────────────────────
        qty = math.floor(qty_raw / step) * step
        qty = round(qty, 8)
        qty = max(QCfg.MIN_QTY(), min(QCfg.MAX_QTY(), qty))

        # ── Margin guard: ensure required margin never exceeds available ───────
        required_margin = qty * price / QCfg.LEVERAGE()
        bal2 = risk_manager.get_available_balance()
        _avail2 = float((bal2 or {}).get("available", 0.0))
        if _avail2 > 0 and required_margin > _avail2 * 1.01:
            logger.warning(
                f"Sizing guard: margin ${required_margin:.2f} > available ${_avail2:.2f} — scaling down"
            )
            qty = math.floor((_avail2 * QCfg.LEVERAGE() / price) / step) * step
            qty = max(QCfg.MIN_QTY(), min(QCfg.MAX_QTY(), round(qty, 8)))
            if qty < QCfg.MIN_QTY():
                return None

        if qty < QCfg.MIN_QTY():
            return None

        # ── Dollar-risk logging (informational — does not alter qty) ──────────
        sl_dist     = abs(price - sl_price) if (sl_price is not None and sl_price > 0) else 0.0
        dollar_risk = sl_dist * qty if sl_dist > 0 else 0.0
        risk_pct    = dollar_risk / available * 100.0 if (dollar_risk > 0 and available > 0) else 0.0

        logger.info(
            f"✅ Sizing [margin_pct] | MARGIN_PCT={QCfg.MARGIN_PCT():.2%} | "
            f"tier={ict_tier or 'none'} "
            f"mult={total_mult:.2f} (t={tier_mult:.2f} c={comp_mod:+.2f} a={amd_mod:+.2f}) | "
            f"alloc=${margin_alloc:.2f} | margin=${qty * price / QCfg.LEVERAGE():.2f}"
            + (f" | SL-dist={sl_dist:.1f}pts $risk=${dollar_risk:.2f} ({risk_pct:.2f}%)"
               if sl_dist > 0 else "")
            + f" | qty={qty}"
        )
        return qty

    @staticmethod
    def _estimate_pnl(pos, exit_price, entry_fill_type="taker"):
        """
        Corrected PnL formula — v5.1.

        ROOT CAUSE OF PREVIOUS BUG:
        The old Delta branch computed:
            contracts = pos.quantity / DELTA_CONTRACT_VALUE_BTC   # e.g. 0.005/0.001 = 5
        But Delta BTCUSD inverse perp has 1 USD per contract — NOT 0.001 BTC per contract.
        To hold 0.005 BTC exposure at $68,856, you need 0.005 × 68,856 = 344 USD contracts.
        Dividing by 0.001 gave 5 contracts = $5 notional instead of $344 notional.
        Result: gross PnL was ~68× too small; net was always dominated by fees → showed loss
        even when trailing SL locked 98 points of profit.

        FIX:
        For Delta BTCUSD inverse perp, convert BTC quantity to USD contracts by
        multiplying by entry_price (the correct economic relationship):
            usd_contracts = pos.quantity × pos.entry_price
        Then apply the standard inverse-perp formula.

        Mathematical note: for moves < 3% (all our trades), the inverse-perp formula
        is equivalent to the linear formula to 3 significant figures:
            gross ≈ pos.quantity × |exit_price − entry_price|
        We use the exact inverse formula for correctness, but the linear approximation
        is included as a sanity check in debug logs.

        Both Delta and CoinSwitch paths now produce identical results for small moves
        because the inverse-perp formula converges to linear.

        Fee basis: notional is measured at entry price (standard industry practice).
        """
        import config as _cfg_pnl
        _is_delta = (getattr(_cfg_pnl, 'EXECUTION_EXCHANGE', 'coinswitch').lower() == 'delta'
                     and getattr(_cfg_pnl, 'DELTA_SYMBOL', 'BTCUSD').upper() == 'BTCUSD')

        # FIX Bug-A: use exchange-specific fee rates.
        # Delta maker rate is NEGATIVE (rebate = -0.02%); CoinSwitch maker rate is
        # positive (0.02%).  The old code always read COMMISSION_RATE_MAKER from config
        # which is set to the CoinSwitch value (+0.00020), costing Delta maker entries
        # 0.04% of notional instead of receiving the rebate.
        if entry_fill_type == "maker":
            if _is_delta:
                entry_rate = float(getattr(_cfg_pnl, "DELTA_COMMISSION_RATE_MAKER",
                                           -0.00020))   # Delta rebate (negative = income)
            else:
                entry_rate = float(getattr(_cfg_pnl, "COMMISSION_RATE_MAKER",
                                           QCfg.COMMISSION_RATE() * 0.40))
        else:
            entry_rate = QCfg.COMMISSION_RATE()

        # Exit is always taker (stop or TP market order)
        exit_rate = (float(getattr(_cfg_pnl, "DELTA_COMMISSION_RATE", 0.00050))
                     if _is_delta else QCfg.COMMISSION_RATE())

        if _is_delta:
            # Exact inverse-perpetual PnL for Delta BTCUSD (1 USD per contract)
            # usd_contracts = how many $1 contracts needed to hold qty_btc exposure
            usd_contracts = pos.quantity * pos.entry_price
            if pos.side == "long":
                # LONG: profit when exit > entry
                gross_btc = usd_contracts * (1.0 / pos.entry_price - 1.0 / exit_price)
            else:
                # SHORT: profit when exit < entry
                gross_btc = usd_contracts * (1.0 / exit_price - 1.0 / pos.entry_price)
            gross = gross_btc * exit_price   # BTC profit → USD at exit price
            # Fee basis: qty_btc × price (standard, matches exchange invoice)
            entry_fee = pos.entry_price * pos.quantity * entry_rate
            exit_fee  = exit_price      * pos.quantity * exit_rate
            # Sanity: verify against linear approximation (should differ by < 0.1% for |Δ|<3%)
            _linear = ((exit_price - pos.entry_price) if pos.side == "long"
                       else (pos.entry_price - exit_price)) * pos.quantity
            if abs(_linear) > 1e-10:
                _discrepancy_pct = abs(gross - _linear) / abs(_linear)
                if _discrepancy_pct > 0.005:   # > 0.5% discrepancy → log as warning
                    logger.warning(
                        f"PnL sanity: inverse={gross:.4f} linear={_linear:.4f} "
                        f"discrepancy={_discrepancy_pct:.3%} — large move detected")
        else:
            # Linear (USDT-margined, CoinSwitch) — standard formula
            gross     = ((exit_price - pos.entry_price) if pos.side == "long"
                         else (pos.entry_price - exit_price)) * pos.quantity
            entry_fee = pos.entry_price * pos.quantity * entry_rate
            exit_fee  = exit_price      * pos.quantity * exit_rate

        net_pnl = gross - entry_fee - exit_fee
        logger.debug(
            f"PnL calc: {pos.side} qty={pos.quantity} entry=${pos.entry_price:,.2f} "
            f"exit=${exit_price:,.2f} gross=${gross:.4f} fees=${entry_fee+exit_fee:.4f} "
            f"net=${net_pnl:.4f} [{'delta_inv' if _is_delta else 'linear'}]")
        return net_pnl

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
        v10: Institutional status report for 15-minute Telegram notification.
        Passes full ICT/pool/flow context to format_periodic_report.
        """
        from telegram.notifier import format_periodic_report
        p   = self._pos
        atr = self._atr_5m.atr
        price = self._last_known_price

        # ── Session stats from _trade_history ────────────────────────────
        total_t  = self._total_trades
        wins     = self._winning_trades
        wr       = wins / total_t * 100.0 if total_t > 0 else 0.0
        total_pnl = self._total_pnl
        daily_pnl = getattr(self, '_daily_pnl', total_pnl)

        # ── Balance ──────────────────────────────────────────────────────
        balance = 0.0
        try:
            if hasattr(self, '_risk_manager') and self._risk_manager:
                balance = self._risk_manager.current_balance
        except Exception:
            pass

        # ── ICT context ──────────────────────────────────────────────────
        session      = ""
        kill_zone    = ""
        amd_phase    = ""
        amd_bias     = ""
        dr_pd        = 0.5
        s15m         = ""
        s4h          = ""
        regime       = ""

        if self._ict is not None:
            try:
                session   = getattr(self._ict, '_session', '')
                kill_zone = getattr(self._ict, '_killzone', '')
                _amd = getattr(self._ict, '_amd', None)
                if _amd:
                    amd_phase = getattr(_amd, 'phase', '')
                    amd_bias  = getattr(_amd, 'bias', '')
                _tf = getattr(self._ict, '_tf', {})
                if '15m' in _tf:
                    s15m = getattr(_tf['15m'], 'trend', '')
                if '4h' in _tf:
                    s4h = getattr(_tf['4h'], 'trend', '')
                _dr = getattr(self._ict, '_dealing_range', None)
                if _dr:
                    dr_pd = getattr(_dr, 'current_pd', 0.5)
            except Exception:
                pass

        if self._regime:
            try:
                regime = self._regime.regime.value
            except Exception:
                regime = str(self._regime.regime) if self._regime else ""

        # ── HTF bias ─────────────────────────────────────────────────────
        htf_bias = ""
        if self._htf:
            try:
                htf_bias = f"15m={self._htf.trend_15m:+.1f} 4h={self._htf.trend_4h:+.1f}"
            except Exception:
                pass

        # ── Pool map summary ─────────────────────────────────────────────
        n_bsl = 0
        n_ssl = 0
        target_str = "—"
        flow_conv = getattr(self, '_flow_conviction', 0.0)
        flow_dir  = getattr(self, '_flow_direction', '')
        nearest_bsl = None
        nearest_ssl = None
        sweep_anal = None

        if hasattr(self, '_liq_map') and self._liq_map is not None:
            try:
                snap = self._liq_map.get_snapshot(price, atr)
                n_bsl = len([p for p in snap.bsl_pools if p.pool.price > price])
                n_ssl = len([p for p in snap.ssl_pools if p.pool.price < price])
                pt = snap.primary_target
                if pt:
                    direction = "BSL ▲" if pt.pool.side.value == "BSL" else "SSL ▼"
                    target_str = (f"{direction} ${pt.pool.price:,.0f} "
                                  f"({pt.distance_atr:.1f}ATR sig={pt.significance:.0f})")

                # Nearest pools for display
                bsl_near = sorted([p for p in snap.bsl_pools if p.pool.price > price],
                                  key=lambda x: x.pool.price)
                ssl_near = sorted([p for p in snap.ssl_pools if p.pool.price < price],
                                  key=lambda x: x.pool.price, reverse=True)
                if bsl_near:
                    bp = bsl_near[0]
                    nearest_bsl = {
                        "price": bp.pool.price,
                        "dist_atr": bp.distance_atr,
                        "significance": bp.significance,
                        "timeframe": bp.pool.timeframe,
                    }
                if ssl_near:
                    sp = ssl_near[0]
                    nearest_ssl = {
                        "price": sp.pool.price,
                        "dist_atr": sp.distance_atr,
                        "significance": sp.significance,
                        "timeframe": sp.pool.timeframe,
                    }
            except Exception:
                pass

        # ── Sweep analysis ───────────────────────────────────────────────
        if hasattr(self, '_entry_engine') and self._entry_engine is not None:
            try:
                sweep_anal = getattr(self._entry_engine, '_last_sweep_analysis', None)
            except Exception:
                pass

        # ── Engine state ─────────────────────────────────────────────────
        engine_state = "SCANNING"
        if hasattr(self, '_entry_engine') and self._entry_engine is not None:
            engine_state = self._entry_engine.state

        # ── Position dict ────────────────────────────────────────────────
        pos_dict = None
        pos_entry = None
        pos_sl = None
        pos_tp = None
        be_moved = False
        locked_r = 0.0
        if not p.is_flat():
            pos_dict = {
                "side": p.side,
                "entry_price": p.entry_price,
                "quantity": p.quantity,
                "peak_profit": p.peak_profit,
                "trail_active": p.trail_active,
            }
            pos_entry = p.entry_price
            pos_sl = p.sl_price
            pos_tp = p.tp_price
            init_sl = p.initial_sl_dist if p.initial_sl_dist > 1e-10 else abs(p.entry_price - p.sl_price)
            if init_sl > 1e-10:
                raw_pts = (price - p.entry_price) if p.side == "long" else (p.entry_price - price)
                locked_r = max(0, raw_pts / init_sl) if raw_pts > 0 else 0.0
            _fee_buf = p.entry_price * QCfg.COMMISSION_RATE() * 2.0 + 0.10 * atr
            _be_price = (p.entry_price + _fee_buf if p.side == "long"
                         else p.entry_price - _fee_buf)
            be_moved = ((p.side == "long" and p.sl_price >= _be_price) or
                        (p.side == "short" and p.sl_price <= _be_price))

        # ── Build extra lines (execution costs + expectancy) ─────────────
        extra = []
        hist = self._trade_history
        if total_t > 0:
            win_pnls  = [t['pnl'] for t in hist if t.get('is_win')]
            loss_pnls = [t['pnl'] for t in hist if not t.get('is_win')]
            avg_w = sum(win_pnls) / len(win_pnls) if win_pnls else 0.0
            avg_l = sum(loss_pnls) / len(loss_pnls) if loss_pnls else 0.0
            expect = (wr/100 * avg_w) + ((1 - wr/100) * avg_l)
            extra.append(f"  Avg W: ${avg_w:+.2f} | Avg L: ${avg_l:+.2f}")
            extra.append(f"  Expectancy: ${expect:+.2f}/trade")

            # v6.0: Margin-based P&L %
            _m_pcts = [t.get('margin_pnl_pct', 0.0) for t in hist if abs(t.get('margin_pnl_pct', 0.0)) > 0.001]
            if _m_pcts:
                _m_total = sum(_m_pcts)
                _m_avg = _m_total / len(_m_pcts)
                extra.append(f"  Margin PnL: {_m_total:+.1f}% total ({_m_avg:+.1f}%/trade)")

        # v6.0: Unrealised margin % if in position
        if not p.is_flat() and p.entry_price > 0 and p.quantity > 0:
            try:
                _rpt_notional = p.entry_price * p.quantity
                _rpt_lev = QCfg.LEVERAGE()
                _rpt_margin = _rpt_notional / _rpt_lev if _rpt_lev > 0 else _rpt_notional
                if _rpt_margin > 1e-10:
                    _rpt_profit = (price - p.entry_price) if p.side == "long" else (p.entry_price - price)
                    _rpt_upnl = _rpt_profit * p.quantity
                    _rpt_pct = (_rpt_upnl / _rpt_margin) * 100.0
                    extra.append(f"  Open P&L: {_rpt_pct:+.1f}% on ${_rpt_margin:.2f} margin")
            except Exception:
                pass

        if self._fee_engine is not None:
            try:
                snap = self._fee_engine.diagnostic_snapshot()
                warmed = snap.get('engine_warmed', False)
                tag = "✅" if warmed else f"⏳ ({snap.get('spread_samples',0)} samples)"
                extra.append(f"  Costs {tag}: spread={snap['spread_median_bps']:.1f}bps "
                             f"slip={snap['slippage_ewma_bps']:.1f}bps")
            except Exception:
                pass

        extra.append(f"  ATR: ${atr:.1f} ({self._atr_5m.get_percentile():.0%} pctile)")
        extra.append(f"  VWAP: ${self._vwap.vwap:,.0f} (dev={self._vwap.deviation_atr:+.1f}ATR)")

        return format_periodic_report(
            current_price=price,
            balance=balance,
            total_trades=total_t,
            win_rate=wr,
            daily_pnl=daily_pnl,
            total_pnl=total_pnl,
            consecutive_losses=self._risk_gate.consec_losses,
            bot_state=engine_state,
            n_bsl_pools=n_bsl,
            n_ssl_pools=n_ssl,
            primary_target_str=target_str,
            flow_conviction=flow_conv,
            flow_direction=flow_dir,
            amd_phase=amd_phase,
            session=session,
            in_killzone=bool(kill_zone),
            regime=regime,
            position=pos_dict,
            current_sl=pos_sl,
            current_tp=pos_tp,
            entry_price=pos_entry,
            breakeven_moved=be_moved,
            profit_locked_pct=locked_r,
            extra_lines=extra,
            # v10 extended
            atr=atr,
            htf_bias=htf_bias,
            dealing_range_pd=dr_pd,
            structure_15m=s15m,
            structure_4h=s4h,
            amd_bias=amd_bias,
            nearest_bsl=nearest_bsl,
            nearest_ssl=nearest_ssl,
            sweep_analysis=sweep_anal,
        )

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

        # Delta bracket child order type names (covers both bracket and standalone):
        # "stop_market_order", "stop_loss_order", "STOP_MARKET", "STOP", "STOP_LOSS_MARKET"
        def _is_sl(ot):
            return (ot in ("STOP_MARKET","STOP","STOP_LOSS_MARKET",
                           "STOP_MARKET_ORDER","STOP_LOSS_ORDER") or
                    ("STOP" in ot and "PROFIT" not in ot and "TAKE" not in ot))
        def _is_tp(ot):
            return (ot in ("TAKE_PROFIT_MARKET","TAKE_PROFIT",
                           "TAKE_PROFIT_MARKET_ORDER","TAKE_PROFIT_ORDER") or
                    ("PROFIT" in ot or "TAKE_PROFIT" in ot))
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
                    ot=(o.get("type") or (o.get("raw") or {}).get("order_type") or "").upper().replace(" ","_").replace("-","_")
                    trig=float(o.get("trigger_price") or (o.get("raw") or {}).get("stop_price") or 0)
                    if _is_sl(ot): sl_oid=o["order_id"]; sl_p=trig
                    elif _is_tp(ot): tp_oid=o["order_id"]; tp_p=trig
            iside = "long" if ex_side=="LONG" else "short"
            self._pos = PositionState(phase=PositionPhase.ACTIVE, side=iside, quantity=ex_size,
                entry_price=ex_entry, sl_price=sl_p, tp_price=tp_p, sl_order_id=sl_oid,
                tp_order_id=tp_oid, entry_time=time.time(), initial_sl_dist=abs(ex_entry-sl_p) if sl_p>0 else 0.0,
                entry_atr=self._atr_5m.atr)
            self.current_sl_price=sl_p; self.current_tp_price=tp_p
            self._confirm_long=self._confirm_short=0
            # Reset duplicate guards for the newly adopted position
            self._exit_completed = False
            self._pnl_recorded_for = 0.0
            logger.warning(f"⚡ RECONCILE: adopted {ex_side} @ ${ex_entry:,.2f}")
            send_telegram_message(f"⚡ <b>POSITION ADOPTED</b>\nSide: {ex_side} | Size: {ex_size}\nEntry: ${ex_entry:,.2f} | uPnL: ${ex_upnl:+.2f}")
            return
        if phase==PositionPhase.ACTIVE and ex_size<QCfg.MIN_QTY():
            logger.info("📡 Reconcile: exchange FLAT → TP/SL fired")
            self._record_exchange_exit(ex_pos); return
        if phase==PositionPhase.ACTIVE and ex_size>=QCfg.MIN_QTY():
            if (not self._pos.sl_order_id or not self._pos.tp_order_id) and open_orders:
                for o in open_orders:
                    ot=(o.get("type") or (o.get("raw") or {}).get("order_type") or "").upper().replace(" ","_").replace("-","_")
                    trig=float(o.get("trigger_price") or (o.get("raw") or {}).get("stop_price") or 0)
                    if not self._pos.sl_order_id and _is_sl(ot):
                        self._pos.sl_order_id=o["order_id"]; self.current_sl_price=trig
                        logger.info(f"Reconcile: recovered SL order {o['order_id'][:8]}… @ ${trig:.2f}")
                    elif not self._pos.tp_order_id and _is_tp(ot):
                        self._pos.tp_order_id=o["order_id"]; self.current_tp_price=trig
                        logger.info(f"Reconcile: recovered TP order {o['order_id'][:8]}… @ ${trig:.2f}")

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
            if ex_size<QCfg.MIN_QTY():
                # v8.0 FIX: call _record_exchange_exit, NOT _finalise_exit.
                # The old code skipped PnL recording entirely for the normal
                # EXITING→flat sync path.  _exit_trade sends estimated PnL via
                # telegram but defers actual recording to this confirmation.
                # Calling _finalise_exit directly meant PnL was never recorded.
                logger.info("📡 Sync: EXITING confirmed FLAT → recording exit")
                self._record_exchange_exit(ex_pos)

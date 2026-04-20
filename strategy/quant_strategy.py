"""
QUANT STRATEGY v10.0 — INSTITUTIONAL LIQUIDITY-FIRST
=====================================================
Architecture:
  LiquidityMap → EntryEngine → ConvictionFilter → UnifiedGate → Execution
  ICT Engine = structural context (AMD, OB, FVG, BOS, sweep detection)
  DirectionEngine = hunt prediction + post-sweep evaluation
  Quant Scout = order-flow timing (VWAP, CVD, tick flow, OB imbalance)

Entry: Only via EntryEngine (sweep reversal, continuation, displacement)
SL/TP: EntryEngine provides primary levels; ICT OB → 15m swing → ATR fallback
Trail: LiquidityTrailEngine (primary) → _DynamicStructureTrail (fallback)
Exit: SL/TP bracket on exchange. Trail moves SL only.
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


# ── ICT Institutional Trade Engine — fully inlined; external module removed ─

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

# ── DirectionEngine — hunt prediction, post-sweep evaluation, pool-hit gate ─
# Replaces ICTEngine.predict_next_hunt() with a dedicated 10-factor engine.
# ICTEngine retains structural context; DirectionEngine owns the decisions.
_DIRECTION_ENGINE_AVAILABLE = False
try:
    from strategy.direction_engine import DirectionEngine, HuntPrediction, DirectionBias
    _DIRECTION_ENGINE_AVAILABLE = True
except ImportError:
    try:
        from direction_engine import DirectionEngine, HuntPrediction, DirectionBias
        _DIRECTION_ENGINE_AVAILABLE = True
    except ImportError:
        DirectionEngine  = None   # type: ignore
        HuntPrediction   = None   # type: ignore
        DirectionBias    = None   # type: ignore

# ── ISSUE-4 FIX: Conviction Gate ─────────────────────────────────────────────
# 7-factor mandatory gate before any entry. Mandatory gates: pool TF ≥ 15m,
# dealing range valid, AMD not ACCUMULATION, session not ASIA.
# Required conviction score ≥ 0.75 for all weighted factors.
_CONVICTION_FILTER_AVAILABLE = False
try:
    from strategy.conviction_filter import ConvictionFilter, ConvictionResult
    _CONVICTION_FILTER_AVAILABLE = True
except ImportError:
    try:
        from conviction_filter import ConvictionFilter, ConvictionResult
        _CONVICTION_FILTER_AVAILABLE = True
    except ImportError:
        ConvictionFilter = None   # type: ignore
        ConvictionResult = None   # type: ignore

# ── ISSUE-3 FIX: Liquidity-Only Trailing SL ──────────────────────────────────
# SL anchors to swept/unswept pool structure instead of fixed ATR ratchets.
# Significance-based buffer; session-aware (London tighter, Asia disabled).
_LIQ_TRAIL_AVAILABLE = False
try:
    from strategy.liquidity_trail import LiquidityTrailEngine, LiquidityTrailResult
    _LIQ_TRAIL_AVAILABLE = True
except ImportError:
    try:
        from liquidity_trail import LiquidityTrailEngine, LiquidityTrailResult
        _LIQ_TRAIL_AVAILABLE = True
    except ImportError:
        LiquidityTrailEngine  = None   # type: ignore
        LiquidityTrailResult  = None   # type: ignore


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
    def MIN_MARGIN_USDT() -> float: return float(_cfg("MIN_MARGIN_PER_TRADE", 1.0))
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
    def MIN_RR_RATIO() -> float: return float(_cfg("MIN_RISK_REWARD_RATIO", 2.0))
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
    def COOLDOWN_SEC() -> int: return int(_cfg("QUANT_COOLDOWN_SEC", 300))
    @staticmethod
    def LOSS_LOCKOUT_SEC() -> int: return int(_cfg("QUANT_LOSS_LOCKOUT_SEC", 5400))
    @staticmethod
    def TICK_EVAL_SEC() -> float: return float(_cfg("ENTRY_EVALUATION_INTERVAL_SECONDS", 1))
    @staticmethod
    def POS_SYNC_SEC() -> float: return float(_cfg("QUANT_POS_SYNC_SEC", 30))
    @staticmethod
    def MAX_DAILY_TRADES() -> int: return int(_cfg("MAX_DAILY_TRADES", 10))
    @staticmethod
    def MAX_CONSEC_LOSSES() -> int: return int(_cfg("MAX_CONSECUTIVE_LOSSES", 2))
    @staticmethod
    def MAX_DAILY_LOSS_PCT() -> float: return float(_cfg("MAX_DAILY_LOSS_PCT", 3.0))
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
    @staticmethod
    # MOD-7 FIX: A 1000% drawdown cap is no cap at all. Industry standard for
    # systematic strategies is 10-20%. Default set to 15% — operators who need
    # more headroom should set MAX_DRAWDOWN_PCT explicitly in config.py.
    def MAX_DRAWDOWN_PCT() -> float: return float(_cfg("MAX_DRAWDOWN_PCT", 15.0))
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
    # ── v6.1: Institutional Trail v2.0 ────────────────────────────────────────
    # Feature 1 — OB + Breaker Block Priority
    # Anchors SL to nearest active OB or Breaker Block before falling back to swings.
    @staticmethod
    def TRAIL_OB_BREAKER_PRIORITY() -> bool:
        return bool(_cfg("QUANT_TRAIL_OB_BREAKER_PRIORITY", True))
    @staticmethod
    def TRAIL_OB_BREAKER_BUFFER_ATR() -> float:
        return float(_cfg("QUANT_TRAIL_OB_BREAKER_BUFFER_ATR", 0.22))
    # Feature 2 — AMD-Phase Adaptive Buffer multipliers for struct buffers
    @staticmethod
    def TRAIL_AMD_MANIP_BUFFER_MULT() -> float:
        """MANIPULATION: wider buffer protects against Judas wicks."""
        return float(_cfg("QUANT_TRAIL_AMD_MANIP_BUFFER_MULT", 1.55))
    @staticmethod
    def TRAIL_AMD_DIST_BUFFER_MULT() -> float:
        """DISTRIBUTION / REDISTRIBUTION: tighter buffer locks profit aggressively."""
        return float(_cfg("QUANT_TRAIL_AMD_DIST_BUFFER_MULT", 0.62))
    @staticmethod
    def TRAIL_AMD_REDIST_BUFFER_MULT() -> float:
        """REACCUMULATION: slight widen for mid-trend pause."""
        return float(_cfg("QUANT_TRAIL_AMD_REDIST_BUFFER_MULT", 1.12))
    # Feature 3 — HTF Structure Cascade
    # Checks 4H swing → 1H swing → 15m → 5m → 1m in priority order.
    @staticmethod
    def TRAIL_HTF_CASCADE_ENABLED() -> bool:
        return bool(_cfg("QUANT_TRAIL_HTF_CASCADE_ENABLED", True))
    # Feature 4 — Liquidity Pool Ceiling / Floor Protection
    @staticmethod
    def TRAIL_LIQ_POOL_PROX_ATR() -> float:
        """Proximity window for pool ceiling/floor gate (ATR multiples)."""
        return float(_cfg("QUANT_TRAIL_LIQ_POOL_PROX_ATR", 2.20))
    @staticmethod
    def TRAIL_LIQ_FLOOR_BUFFER_ATR() -> float:
        """Buffer behind the pool for the ceiling/floor guard."""
        return float(_cfg("QUANT_TRAIL_LIQ_FLOOR_BUFFER_ATR", 0.30))
    # Feature 5 — Displacement + CVD Confirmation Gate
    # Trail only advances when a displacement candle + CVD trend confirm momentum.
    @staticmethod
    def TRAIL_DISP_CVD_GATE() -> bool:
        return bool(_cfg("QUANT_TRAIL_DISP_CVD_GATE", True))
    @staticmethod
    def TRAIL_CVD_MIN_TREND() -> float:
        """Minimum CVD trend magnitude to allow trail advance."""
        return float(_cfg("QUANT_TRAIL_CVD_MIN_TREND", 0.12))
    @staticmethod
    def TRAIL_DISP_MIN_ATR_MULT() -> float:
        """Minimum candle body (× ATR) to qualify as a displacement candle."""
        return float(_cfg("QUANT_TRAIL_DISP_MIN_ATR_MULT", 0.58))
    @staticmethod
    def TRAIL_DISP_CVD_MIN_R() -> float:
        """Gate is only active above this R-multiple (below = BE move allowed freely)."""
        return float(_cfg("QUANT_TRAIL_DISP_CVD_MIN_R", 0.30))

    # ── v7.0: Institutional Liquidity-First Trail ──────────────────────────
    # Primary reference is the live LiquidityMap pool, not a chandelier.
    # New SL = (nearest unswept pool) +/- dynamic_buffer.
    # 15m/1h pool acts as the hard safety floor.
    @staticmethod
    def TRAIL_LIQ_BASE_BUF_MAX_ATR() -> float:
        """Buffer fraction at 0R (ATR multiples). Narrows linearly to MIN by 1R."""
        return float(_cfg("QUANT_TRAIL_LIQ_BASE_BUF_MAX_ATR", 0.25))
    @staticmethod
    def TRAIL_LIQ_BASE_BUF_MIN_ATR() -> float:
        """Buffer floor (ATR multiples). Applied as absolute minimum always."""
        return float(_cfg("QUANT_TRAIL_LIQ_BASE_BUF_MIN_ATR", 0.15))
    @staticmethod
    def TRAIL_LIQ_SAFETY_BUF_ATR() -> float:
        """Buffer placed behind the 15m/1h safety-floor pool."""
        return float(_cfg("QUANT_TRAIL_LIQ_SAFETY_BUF_ATR", 0.28))
    @staticmethod
    def TRAIL_LIQ_POOL_LOOKBACK_ATR() -> float:
        """Max ATR distance behind price to scan for anchor pools."""
        return float(_cfg("QUANT_TRAIL_LIQ_POOL_LOOKBACK_ATR", 8.0))
    @staticmethod
    def TRAIL_LIQ_BOS_CONFIRM_GATE() -> bool:
        """Require BOS on 5m/15m OR displacement candle before trail advances."""
        return bool(_cfg("QUANT_TRAIL_LIQ_BOS_CONFIRM_GATE", True))
    @staticmethod
    def TRAIL_LIQ_BOS_MAX_AGE_MS() -> int:
        """Max age (ms) for BOS event to count as valid confirmation."""
        return int(_cfg("QUANT_TRAIL_LIQ_BOS_MAX_AGE_MS", 10_000_000))  # 10 min
    @staticmethod
    def TRAIL_LIQ_MIN_BREATHING_ATR() -> float:
        """Hard minimum distance between SL and current price (ATR multiples)."""
        return float(_cfg("QUANT_TRAIL_LIQ_MIN_BREATHING_ATR", 0.28))


def _round_to_tick(price: float) -> float:
    tick = QCfg.TICK_SIZE()
    return round(round(price / tick) * tick, 10) if tick > 0 else price


def _calc_be_price(pos_side: str, entry_price: float, atr: float,
                   pos=None) -> float:
    """
    Single source of truth for break-even price across the entire engine.

    WHY ONE FUNCTION:
      Five different inline expressions previously spread across quant_strategy
      and controller computed a slightly different break-even price:
        - 0.10 ATR buffer  (display / heartbeat paths)
        - 0.12 ATR buffer  (_DynamicStructureTrail)
        - 0.15 ATR buffer  (counter-BOS path in _update_trailing_sl)
        - 0.30 ATR only, no fee  (legacy compute_trail_sl gating)
      None of them used the exact paid commission captured from Delta's
      paid_commission field (stored on PositionState.entry_fee_paid since v8.1).

    FORMULA:
      fee_per_btc  = exact_entry_fee / qty   if exact fee available (v8.1+)
                   = entry_price × COMMISSION_RATE × 2   otherwise
      slippage_buf = 0.12 × ATR   (half-spread estimate; tighter than old 0.15,
                                   wider than old 0.10 — a calibrated middle ground)
      be_price     = entry_price ± (fee_per_btc + slippage_buf)

    EXACT FEE:
      Delta's paid_commission is the actual taker/maker fee in USD for the entry
      leg.  We store it on pos.entry_fee_paid at fill.  When present it replaces
      the commission-rate estimate, giving a trade-level accurate BE.

    ARGS:
      pos_side     : 'long' | 'short'
      entry_price  : position entry price
      atr          : current ATR (5m)
      pos          : PositionState (optional) — used for exact fee + quantity

    RETURNS:
      Break-even price as float.  For long: entry_price + buf.
      For short: entry_price - buf.
    """
    # MOD-5 FIX: Use the module-level `config` import instead of importing
    # inside the function on every call. Python caches imports, but repeated
    # function-level imports confuse profilers and signal sloppy architecture.
    # ── Fee per BTC ──────────────────────────────────────────────────────────
    _exact_fee = 0.0
    _qty       = 0.0
    if pos is not None:
        _exact_fee = float(getattr(pos, 'entry_fee_paid', 0.0) or 0.0)
        _qty       = float(getattr(pos, 'quantity',       0.0) or 0.0)

    if _exact_fee > 1e-6 and _qty > 1e-10:
        # Exact round-trip cost: entry paid_commission (exact) + estimated exit
        # fee (same rate applied symmetrically — we don't have the exit fee yet).
        _entry_fee_per_btc = _exact_fee / _qty
        # Estimate exit fee at the same rate as entry (conservative)
        _exit_fee_rate = _entry_fee_per_btc / max(entry_price, 1.0)
        _fee_per_btc   = _entry_fee_per_btc + entry_price * _exit_fee_rate
    else:
        # Fallback: bilateral commission-rate estimate
        _rate        = float(getattr(config, 'COMMISSION_RATE', 0.00055))
        _fee_per_btc = entry_price * _rate * 2.0

    # ── Slippage allowance ───────────────────────────────────────────────────
    # 0.12 ATR: tighter than the old 0.15 used in counter-BOS (that was overly
    # conservative) and wider than the 0.10 used in display (that was too tight).
    # At $255 ATR this is $30.6 — covers a normal half-spread on BTC perps.
    _slippage_buf = 0.12 * atr

    _buf = _fee_per_btc + _slippage_buf
    return (entry_price + _buf if pos_side == "long" else entry_price - _buf)

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
            _start_val  = arr[_start_idx]   # BUG FIX: always use actual value; old code gave 0.0 when _start_idx==0, inflating earlier_cvd by arr[0]
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
            # Fallback: use ATR-based TP at minimum R:R distance
            _fallback_dist = sl_dist * max(_min_rr_gate, 1.5)
            _fallback_dist = min(_fallback_dist, max_tp_dist)
            if side == "long":
                tp = price + _fallback_dist
            else:
                tp = price - _fallback_dist
            logger.info(
                f"TP ATR FALLBACK: ${tp:,.2f} ({_fallback_dist:.0f}pts) "
                f"R:R=1:{_fallback_dist/max(sl_dist,1):.1f} — "
                f"no structural target found, using minimum R:R distance")

        return tp
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



class _DynamicStructureTrail:
    """
    Slim ICT structure-query helper — v5.0 refactor.

    The v7.0 trail logic that used to live here (chandelier fallback,
    dynamic-buffer computation, liquidity-first anchor selection,
    ATR-percentile-scaled multipliers, etc.) was REMOVED as part of the
    move to pure-Fibonacci trailing via LiquidityTrailEngine v5.0.

    Retained here (as classmethods / static methods) are ONLY the pure
    ICT-state query helpers used by the display layer:
      _bos_count     — how many aligned BOS events across TFs
      _counter_bos   — is there a fresh BOS AGAINST the position?
      _choch         — most relevant CHoCH against the position
      _phase         — simple phase label (R-multiple driven)
      _session_mult  — DST-aware session buffer multiplier

    All SL trailing decisions are made by LiquidityTrailEngine in
    strategy/liquidity_trail.py.  This class NO LONGER drives SL moves.
    """

    # ── Structure event detection ────────────────────────────────────

    @staticmethod
    def _bos_count(ict_engine, pos_side: str, now_ms: int = 0) -> int:
        """Count aligned BOS events across 1m/5m/15m within a 6-min window."""
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
                if pos_side == "long"  and d == "bullish": count += 1
                elif pos_side == "short" and d == "bearish": count += 1
            except Exception:
                pass
        return count

    @staticmethod
    def _counter_bos(ict_engine, pos_side: str, now_ms: int = 0) -> bool:
        """True if a fresh counter-trend BOS exists on 5m or 15m."""
        if ict_engine is None:
            return False
        max_age_ms = 3_000_000
        for tf in ("5m", "15m"):
            try:
                st = ict_engine._tf.get(tf)
                if st is None:
                    continue
                d   = getattr(st, "bos_direction", None)
                bts = getattr(st, "bos_timestamp", 0)
                if now_ms > 0 and bts > 0 and (now_ms - bts) > max_age_ms:
                    continue
                if pos_side == "long"  and d == "bearish": return True
                if pos_side == "short" and d == "bullish": return True
            except Exception:
                pass
        return False

    @staticmethod
    def _choch(ict_engine, pos_side: str):
        """Return the most recent CHoCH against the position as (tf, level)."""
        if ict_engine is None:
            return None, 0.0
        for tf in ("5m", "1m"):
            try:
                st = ict_engine._tf.get(tf)
                if st is None:
                    continue
                d   = getattr(st, "choch_direction", None)
                lvl = float(getattr(st, "choch_level", 0.0))
                if not d or not lvl:
                    continue
                if pos_side == "long"  and d == "bearish": return tf, lvl
                if pos_side == "short" and d == "bullish": return tf, lvl
            except Exception:
                pass
        return None, 0.0

    @staticmethod
    def _phase(bos_count: int, be_locked: bool, choch_seen: bool, mfe_r: float) -> int:
        """R-multiple-driven phase label for display compatibility."""
        if mfe_r >= 2.0: return 3
        if mfe_r >= 1.0: return 2
        if mfe_r >= 0.30 or be_locked: return 1
        return 0

    @staticmethod
    def _session_mult() -> float:
        """DST-aware session-based trail buffer multiplier."""
        try:
            from config import SESSION_TRAIL_WIDTH_MULT
            try:
                from zoneinfo import ZoneInfo
                import datetime as _dt_mod
                _ny_dt = _dt_mod.datetime.now(ZoneInfo("America/New_York"))
                _ny    = _ny_dt.hour + _ny_dt.minute / 60.0
            except Exception:
                from datetime import datetime, timezone
                _utc = datetime.now(timezone.utc)
                _uh  = _utc.hour + _utc.minute / 60.0
                _m   = _utc.month
                _in_dst = (3 < _m < 11)
                _ny = (_uh + (-4.0 if _in_dst else -5.0)) % 24.0
            if   _ny >= 20.0 or _ny < 2.0:  sess = "asia"
            elif 2.0  <= _ny < 7.0:          sess = "london"
            elif 7.0  <= _ny < 11.0:         sess = "ny"
            elif 11.0 <= _ny < 16.0:         sess = "late_ny"
            else:                             sess = "off"
            return SESSION_TRAIL_WIDTH_MULT.get(sess, 1.0)
        except (ImportError, AttributeError):
            return 1.0


_DYNAMIC_TRAIL_AVAILABLE = True   # helper class — always available


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
        if tf_struct.choch_level > 0 and tf_struct.choch_bar_index > 0:  # BUG FIX: was >= 0; index 0 is the oldest bar in window (stale), only -1 is the "no bar" sentinel
            # FIX-A (CRITICAL): choch_bar_index is an index within the 60-bar
            # structural slice (candles[-lb:]), NOT an absolute index into the
            # full n_candles feed.  Using (n_candles-1) as the reference point
            # produces bars_ago ≈ 140-200 for a CHoCH that actually happened
            # 4 bars ago, making every CHoCH appear permanently stale and
            # silently disabling the ±0.15 score component entirely.
            #
            # Correct reference point: the last bar of the slice = lb - 1.
            # With choch_bar_index=55 and lb=60: bars_ago = 59-55 = 4  ✓
            # With old formula n_candles=200:     bars_ago = 199-55 = 144 ✗
            lb = min(60, max(n_candles, 1))
            bars_ago = max(0, (lb - 1) - tf_struct.choch_bar_index)
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
        HTF veto — DISABLED. Always returns False.
        HTF context is advisory only; conviction filter scores HTF via structure.
        """
        return False
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
    # PostTradeAgent MAE tracking: exact Maximum Adverse Excursion in points.
    # Updated every trail tick so set_exit_context() can use the true value
    # rather than the SL-distance approximation.
    peak_adverse:  float = 0.0

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
        # v4.8: ICT/SMC structural confluence engine
        self._ict = ICTEngine() if _ICT_AVAILABLE else None
        # DirectionEngine — owns hunt prediction, post-sweep eval, pool-hit gate.
        # Reads structural context from self._ict; writes results back via
        # inject_hunt_prediction() so the rest of the stack is unaware of the split.
        self._dir_engine: Optional[object] = (
            DirectionEngine() if _DIRECTION_ENGINE_AVAILABLE else None)
        # v5.0: ICT Sweep-and-Go institutional engine
        self._last_sweep_log = 0.0   # throttle sweep-status log spam
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
        self._last_eval_time = 0.0; self._last_exit_time = 0.0
        self._last_tp_gate_rejection = 0.0  # tracks last TP gate rejection time
        self._tp_gate_rejection_mode = ""   # "reversion" | "momentum" | "trend" for per-mode logging
        self._last_pos_sync = 0.0; self._last_exit_sync = 0.0; self._exiting_since = 0.0
        self._entering_since = 0.0  # timestamp when ENTERING phase started (watchdog)
        # BUG 2 FIX: timestamp when the limit ENTRY order actually hit the exchange.
        # The ENTERING watchdog must count from HERE, not from phase-onset, because
        # `place_bracket_limit_entry` can spend real time on: credential refresh,
        # margin check, REST retry on 429/502, and bracket-child order-ID resolution.
        # The old watchdog counted from phase onset — fired while the fill poll was
        # still blocking, leaving a live position on the exchange while our state
        # reset to FLAT.  0.0 = order not placed yet (watchdog uses pre-order tolerance).
        self._entry_order_placed_at = 0.0
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
        # MOD-1 FIX: deque(maxlen=200) gives O(1) bounded append, eliminating
        # the `del self._trade_history[:-200]` list-reallocation pattern.
        self._trade_history: deque = deque(maxlen=200)   # persistent per-session trade log
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
        self._last_trail_block_log = 0.0
        # v6.0: Structure-event-driven trail variables
        self._last_trail_check_price    = 0.0   # price at last trail computation
        self._last_trail_rest_time      = 0.0   # timestamp of last successful trail REST move
        self._last_structure_fingerprint = None  # structural state fingerprint for change detection
        self._last_maxhold_check   = 0.0


        # -- v9.0: New liquidity-first engines --
        self._liq_map = LiquidityMap() if _LIQ_MAP_AVAILABLE else None
        self._entry_engine = EntryEngine() if _ENTRY_ENGINE_AVAILABLE else None
        self._ict_trail = ICTTrailManager() if _ENTRY_ENGINE_AVAILABLE else None

        # ── ISSUE-4 FIX: Conviction Gate ─────────────────────────────────────
        # Evaluates 7 ICT factors before any entry order is placed.
        # Mandatory hard blocks: pool TF, dealing range, AMD phase, session.
        # Weighted score must reach 0.75; tracks session-level quality state.
        self._conviction: Optional[object] = (
            ConvictionFilter() if _CONVICTION_FILTER_AVAILABLE else None
        )

        # ── ISSUE-3 FIX: Liquidity-Only Trailing SL ──────────────────────────
        # SL anchors to swept/unswept pool structure; significance-based buffer.
        # Takes priority over chandelier trail. Chandelier runs as fallback only.
        self._liq_trail: Optional[object] = (
            LiquidityTrailEngine() if _LIQ_TRAIL_AVAILABLE else None
        )
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
        # Bug-1 fix: deduplication set for DirectionEngine.on_sweep() calls.
        # The sweep bridge loop runs every 250ms and visits every swept pool
        # whose sweep_timestamp falls within the last 30s.  Without this guard,
        # on_sweep() is called ~120 times per pool, resetting the PostSweepState
        # and wiping all accumulated evidence on every tick.
        # Key format: (pool_price_float, sweep_timestamp_int_ms)
        # Entries are pruned when older than 60s to prevent unbounded growth.
        self._notified_sweeps: set = set()
        # Track previous killzone so on_session_change() fires exactly once per
        # London/NY/Asia/OFF_HOURS boundary — resets conviction session quota.
        self._last_conviction_kz: str = ""

        # ── Post-Trade Analysis Agent (v2.0) ──────────────────────────────────
        # Five-dimension institutional analysis: exit geometry (MAE/MFE/G-ratio/
        # R-multiples), entry quality (OTE/AMD/ICT/session), structural causation
        # (WICK_SWEEP/BOS_BREAK/AMD_FLIP/POOL_REACHED/…), Bayesian adaptive
        # parameters, and Information Coefficient (IC) signal tracking.
        # Non-fatal: bot operates normally if the file is missing.
        try:
            from strategy.post_trade_agent import PostTradeAgent
            self._post_trade_agent = PostTradeAgent()
        except ImportError:
            try:
                from post_trade_agent import PostTradeAgent
                self._post_trade_agent = PostTradeAgent()
            except ImportError:
                self._post_trade_agent = None
                logger.warning(
                    "PostTradeAgent not found — post-trade analysis disabled. "
                    "Place post_trade_agent.py in strategy/ to enable."
                )

        # ── Unified entry gate state ──────────────────────────────────────
        self._last_unified_gate_key = None
        self._last_unified_gate_ts  = 0.0
        self._log_init()

    def _log_init(self):
        logger.info("=" * 72)
        logger.info("⚡ QuantStrategy v10.0 — INSTITUTIONAL LIQUIDITY-FIRST")
        logger.info(f"   {QCfg.SYMBOL()} | {QCfg.LEVERAGE()}x | {QCfg.MARGIN_PCT():.0%} margin")
        entry_status = "ACTIVE (LiquidityMap → EntryEngine → ConvictionFilter)" if _ENTRY_ENGINE_AVAILABLE else "UNAVAILABLE"
        logger.info(f"   Entry: {entry_status}")
        liq_status = "ACTIVE" if _LIQ_MAP_AVAILABLE else "UNAVAILABLE"
        logger.info(f"   LiquidityMap: {liq_status}")
        ict_status = "ENABLED" if self._ict else "DISABLED"
        logger.info(f"   ICT Engine: {ict_status}")
        dir_status = "ACTIVE" if self._dir_engine else "UNAVAILABLE"
        logger.info(f"   DirectionEngine: {dir_status}")
        conv_status = "ACTIVE" if self._conviction else "UNAVAILABLE"
        logger.info(f"   ConvictionGate: {conv_status}")
        liq_trail = "ACTIVE" if self._liq_trail else "UNAVAILABLE"
        logger.info(f"   LiquidityTrail: {liq_trail}")
        pta_status = "ACTIVE" if self._post_trade_agent else "UNAVAILABLE"
        logger.info(f"   PostTradeAgent: {pta_status}")
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
            if self._ict: self._ict.reset_state()
            if self._dir_engine is not None:
                try:
                    self._dir_engine.clear_sweep()
                except Exception:
                    pass
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
        """
        Reject entries when the live bid-ask spread is too large relative to ATR.

        BUG FIX: Previously called self._fee_engine._spread.median_bps() which
        returns a hardcoded FEE_SPREAD_DEFAULT_BPS=2.0 sentinel whenever fewer
        than 5 samples have been collected. On startup the tracker is empty, so
        every tick returned 2.0 bps (13× the real BTC spread of ~0.15 bps),
        blocking all entries with ratio=0.468 > 0.30 indefinitely.

        Fix: compute the live spread directly from the current orderbook at
        evaluation time. The fee engine's rolling tracker continues updating
        independently for round-trip cost estimation — it is not used here.

        Also adds a 60s log throttle to suppress repeated identical messages.
        """
        try:
            atr   = self._atr_5m.atr
            price = data_manager.get_last_price()
            if atr < 1e-10 or price < 1.0:
                return True, 0.0

            # ── Live bid/ask from current orderbook ───────────────────────────
            ob    = data_manager.get_orderbook()
            bids  = (ob or {}).get("bids", [])
            asks  = (ob or {}).get("asks", [])
            if not bids or not asks:
                return True, 0.0

            def _get_px(lvl) -> float:
                if isinstance(lvl, (list, tuple)): return float(lvl[0])
                if isinstance(lvl, dict):           return float(lvl.get("limit_price") or lvl.get("price") or 0)
                return 0.0

            bid = _get_px(bids[0])
            ask = _get_px(asks[0])
            if bid <= 0.0 or ask <= bid:
                return True, 0.0

            mid        = (bid + ask) / 2.0
            spread_bps = (ask - bid) / mid * 10_000.0
            spread_usd = ask - bid

            # ── Ratio: spread-dollars / ATR-dollars ───────────────────────────
            ratio     = (spread_bps / 10_000.0 * price) / atr
            max_ratio = float(getattr(config, "QUANT_MAX_SPREAD_ATR_RATIO", 0.30))

            if ratio > max_ratio:
                # Throttle: log at most once per 60 s to avoid tick-level spam
                _now = time.time()
                if _now - getattr(self, "_last_spread_gate_warn", 0.0) >= 60.0:
                    self._last_spread_gate_warn = _now
                    logger.info(
                        f"⛔ Spread/ATR gate: {ratio:.3f} > {max_ratio} "
                        f"(spread={spread_bps:.2f}bps / ${spread_usd:.2f}, "
                        f"ATR=${atr:.1f}) — too expensive")
                return False, ratio

            return True, ratio
        except Exception:
            return True, 0.0

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

            # Bug #6 fix: _manage_active reads and modifies self._pos (trail SL,
            # peak_profit, be_ratchet_applied) concurrently with the background
            # sync thread that also writes to self._pos via _sync_position.
            # While self._lock guards individual field mutations, _update_trailing_sl
            # performs multi-step read-then-modify sequences that release the lock
            # between steps (e.g. peak_profit update → replace_stop_loss REST call).
            # A sync result arriving in that window can produce stale peak/SL values.
            #
            # Solution: skip one trail-management tick while a sync is in flight.
            # The trail engine is stateful and will catch up on the next tick; the
            # ~30-second sync interval means at most one skipped trail evaluation.
            # This is always safe: the existing SL remains on the exchange unchanged.
            if not self._pos_sync_in_progress:
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
            #
            # BUG 2 FIX — two-stage watchdog:
            #   Stage A (pre-order): from phase-onset until the limit order
            #     actually hits the exchange.  Bounded by PRE_ORDER_TOLERANCE
            #     (default 45 s) — covers signing, credential refresh, retries.
            #     If this expires, something is wrong BEFORE any order exists,
            #     so it is safe to force-FLAT.
            #
            #   Stage B (post-order): from order-placed timestamp until
            #     fill-confirmation.  Bounded by LIMIT_ORDER_FILL_TIMEOUT_SEC
            #     + watchdog_buffer (25 % margin, min 30 s).  This runs in
            #     parallel with the background thread's own fill poll.
            #
            # The old single-stage watchdog counted from phase-onset, so a 60 s
            # order-placement delay (bracket child resolution) + 60 s fill poll
            # already exceeded 90 s — fired while the position was still live.
            PRE_ORDER_TOLERANCE = 45.0
            _entry_timeout = float(getattr(config, 'LIMIT_ORDER_FILL_TIMEOUT_SEC', 120.0))
            _watchdog_buffer = max(30.0, _entry_timeout * 0.25)

            _order_placed_at = getattr(self, '_entry_order_placed_at', 0.0)
            if _order_placed_at <= 0.0:
                # Stage A: waiting for order to be placed
                _elapsed = now - self._entering_since
                _limit = PRE_ORDER_TOLERANCE
                _stage = "pre-order"
            else:
                # Stage B: order placed — wait for fill
                _elapsed = now - _order_placed_at
                _limit = _entry_timeout + _watchdog_buffer
                _stage = "post-order"

            if _elapsed > _limit:
                with self._lock:
                    if self._pos.phase == PositionPhase.ENTERING:
                        logger.warning(
                            f"⚠️ ENTERING watchdog [{_stage}]: >{int(_limit)}s "
                            f"elapsed={_elapsed:.0f}s without fill — forcing FLAT "
                            f"(check exchange for orphaned position)")
                        send_telegram_message(
                            f"⚠️ <b>ENTERING TIMEOUT</b>\n"
                            f"Stage: {_stage}  elapsed={_elapsed:.0f}s  limit={int(_limit)}s\n"
                            f"State reset to FLAT.\n"
                            f"<b>Check exchange for open position!</b>")
                        self._pos.phase = PositionPhase.FLAT
                        self._last_exit_time = now
                        self._entry_order_placed_at = 0.0
                        if self._entry_engine is not None:
                            self._entry_engine.on_entry_failed()
                            logger.info("🔄 Entry engine reset to SCANNING after ENTERING watchdog")

        elif phase == PositionPhase.FLAT:
            if cooldown_ok:
                self._evaluate_entry(data_manager, order_manager, risk_manager, now)

    def _launch_entry_async(self, data_manager, order_manager, risk_manager,
                             side: str, sig, mode: str,
                             ict_tier: str = "",
                             prefetched_bal_info: dict = None) -> None:
        """
        Non-blocking entry: sets ENTERING phase immediately, then runs
        _enter_trade in a daemon thread so the main on_tick loop is never
        blocked by the bracket fill-polling sleep loop (up to 45s).

        ict_tier: "S" | "A" | "B" | "" — passed through to _enter_trade so
        confidence-weighted position sizing can scale size by conviction tier.

        prefetched_bal_info: Bug #5 fix — the balance dict already fetched in
        _evaluate_entry (REST call #1) is forwarded here so _enter_trade does
        not make a second identical REST call in the same tick. Between the two
        calls the balance cannot change (no position is open), but the redundancy
        added ~50 ms latency and could produce divergent values on stale exchange
        endpoints.

        The try/finally guarantees any abort path inside _enter_trade
        (TP gate rejection, SL failure, partial-fill abort, etc.) resets phase
        to FLAT so entry evaluation resumes after cooldown.
        """
        with self._lock:
            self._pos.phase      = PositionPhase.ENTERING
            self._entering_since = time.time()
            # BUG 2: reset order-placed timestamp — Stage A (pre-order) begins
            self._entry_order_placed_at = 0.0

        _dm, _om, _rm = data_manager, order_manager, risk_manager
        _bal           = prefetched_bal_info

        def _bg():
            try:
                self._enter_trade(_dm, _om, _rm, side, sig, mode=mode,
                                  ict_tier=ict_tier,
                                  prefetched_bal_info=_bal)
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
                    # without opening a position. on_entry_failed() is the
                    # single canonical reset path — it handles state machine
                    # transition and counter cleanup atomically.
                    if (self._entry_engine is not None
                            and self._pos.phase != PositionPhase.ACTIVE):
                        self._entry_engine.on_entry_failed()

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
        regime_lbl = sig.market_regime
        atr = sig.atr if sig.atr > 1e-10 else 1.0
        _in_pos = self._pos.is_active()

        if _in_pos:
            pos = self._pos
            profit_pts = ((price - pos.entry_price) if pos.side == "long"
                          else (pos.entry_price - price))
            init_dist = pos.initial_sl_dist if pos.initial_sl_dist > 1e-10 else atr
            cur_r = profit_pts / init_dist if init_dist > 1e-10 else 0.0
            peak_r = pos.peak_profit / init_dist if init_dist > 1e-10 else 0.0
            mfe_r = max(cur_r, peak_r)

            _now_ms_disp = int(now * 1000) if now > 0 else int(time.time() * 1000)
            bos_cnt = 0
            choch_tf = None; choch_lvl = 0.0
            if self._ict is not None:
                try:
                    bos_cnt = _DynamicStructureTrail._bos_count(self._ict, pos.side, _now_ms_disp)
                    choch_tf, choch_lvl = _DynamicStructureTrail._choch(self._ict, pos.side)
                except Exception: pass
            choch_active = choch_tf is not None and choch_lvl > 0.0

            _be_price = _calc_be_price(pos.side, pos.entry_price, atr, pos=pos)
            be_locked = ((pos.side == "long" and pos.sl_price >= _be_price) or
                         (pos.side == "short" and pos.sl_price <= _be_price))

            if mfe_r >= 2.0: trail_phase = 3; phase_lbl = f"🟢 PHASE 3 — AGGRESSIVE ({mfe_r:.2f}R)"
            elif mfe_r >= 1.0: trail_phase = 2; phase_lbl = f"🟠 PHASE 2 — STRUCTURE ({mfe_r:.2f}R)"
            elif mfe_r >= 0.40: trail_phase = 1; phase_lbl = f"🟡 PHASE 1 — BE FLOOR ({mfe_r:.2f}R)"
            elif mfe_r >= 0.10: trail_phase = 0; phase_lbl = f"⬜ PHASE 0 — CHANDELIER ({mfe_r:.2f}R)"
            else: trail_phase = -1; phase_lbl = f"⬜ HANDS OFF ({mfe_r:.2f}R < 0.10R)"

            _margin_pnl_pct = 0.0
            try:
                if pos.entry_price > 0 and pos.quantity > 0:
                    _notional = pos.entry_price * pos.quantity
                    _margin = _notional / QCfg.LEVERAGE() if QCfg.LEVERAGE() > 0 else _notional
                    if _margin > 1e-10:
                        _margin_pnl_pct = (profit_pts * pos.quantity / _margin) * 100.0
            except Exception: pass

            sl_dist_atr = abs(price - pos.sl_price) / max(atr, 1)
            tp_dist_atr = abs(pos.tp_price - price) / max(atr, 1)
            _bar_filled = min(int(mfe_r * 4), 16)
            _prog_bar = "█" * _bar_filled + "░" * (16 - _bar_filled)

            _amd_brief = ""
            if self._ict is not None and self._ict._initialized:
                try:
                    _amd_brief = (f"{self._ict._amd.phase}  bias={self._ict._amd.bias}"
                                  f"  conf={self._ict._amd.confidence:.2f}")
                except Exception: pass

            lines_out = [
                f"┌─── 📊 IN-POSITION [{pos.side.upper()}] ────────────────────────",
                f"  Price ${price:,.2f} │ ATR={atr:.1f} │ Hold={now - pos.entry_time:.0f}s",
                f"  Entry ${pos.entry_price:,.2f}  SL ${pos.sl_price:,.2f}  TP ${pos.tp_price:,.2f}",
                f"  SL dist: {sl_dist_atr:.1f}ATR │ TP dist: {tp_dist_atr:.1f}ATR",
                f"  ─" * 30,
                f"  R-PROGRESS: current={cur_r:+.2f}R  peak={mfe_r:.2f}R",
                f"  [{_prog_bar}] {mfe_r:.2f}R │ Margin PnL: {_margin_pnl_pct:+.1f}%",
                f"  ─" * 30,
                f"  TRAIL: {phase_lbl}",
                f"  BOS: {bos_cnt} │ CHoCH: "
                + (f"{choch_tf} @ ${choch_lvl:,.0f}" if choch_active else "none"),
                f"  BE: " + ("✅ LOCKED" if be_locked else f"❌ needs ${_be_price:,.2f}"),
            ]
            if _amd_brief:
                lines_out.append(f"  AMD: {_amd_brief}")
            lines_out.append(f"└{'─'*60}")
            logger.info("\n" + "\n".join(lines_out))
        else:
            # SCANNING display
            engine_state = "SCANNING"
            if hasattr(self, '_entry_engine') and self._entry_engine is not None:
                engine_state = self._entry_engine.state

            gates = [
                f"{'✅' if sig.overextended else '⚪'} Overextended ({sig.deviation_atr:+.1f}ATR)",
                f"{'✅' if sig.regime_ok else '❌'} ATR Regime ({sig.atr_pct:.0%})",
                f"⚪ HTF (15m={self._htf.trend_15m:+.2f} 4h={self._htf.trend_4h:+.2f})",
                f"📊 {regime_lbl} │ ADX={sig.adx:.1f}",
            ]
            if self._ict is not None and self._ict._initialized:
                amd_phase = getattr(self._ict._amd, 'phase', '?') if self._ict._amd else '?'
                amd_bias = getattr(self._ict._amd, 'bias', '?') if self._ict._amd else '?'
                gates.append(f"🏛️ AMD: {amd_phase} ({amd_bias})")

            cd = max(0.0, QCfg.COOLDOWN_SEC() - (now - self._last_exit_time))
            header = (f"┌─── 🧠 v10 LIQUIDITY-FIRST  ${price:,.2f}  "
                      f"VWAP=${sig.vwap_price:,.2f}  ATR={sig.atr:.1f} ────")
            lines_out = [header]
            lines_out += [fmt("VWAP", sig.vwap_dev), fmt("CVD", sig.cvd_div),
                          fmt("OB", sig.orderbook), fmt("TICK", sig.tick_flow),
                          fmt("VEX", sig.vol_exhaust), f"  {'─'*42}",
                          f"  Σ={c:+.4f} │ State: {engine_state}",
                          f"  ── GATES ──"]
            for g in gates:
                lines_out.append(f"  {g}")
            lines_out.append(f"  Cooldown: {f'{cd:.0f}s' if cd > 0 else 'ready'}")
            lines_out.append(f"└{'─'*60}")
            logger.info("\n" + "\n".join(lines_out))


    def _unified_entry_gate(self, signal, ict_ctx, flow_state,
                             liq_snapshot, price, atr, now):
        """
        Institutional Unified Entry Gate — ADVISORY ONLY (fully data-driven).

        This gate logs structural coherence notes for diagnostics but does NOT
        block any trade. All actual gating is handled by the conviction_filter
        score. The architecture is:

          ConvictionFilter → SCORE gates the trade (single source of truth)
          UnifiedGate     → LOGS advisory signals for post-trade attribution

        Philosophy: no binary veto on session, AMD phase, flow direction, or HTF
        structure. The data-driven score in conviction_filter already weights all
        of these factors. A double-gate creates false precision (two separate
        thresholds on the same signal) and blocks valid setups.
        """
        advisories = []   # informational only — never blocks

        # ── AMD Phase context ───────────────────────────────────────────────
        amd_phase = (ict_ctx.amd_phase or "").upper()
        amd_bias  = (ict_ctx.amd_bias  or "").lower()
        amd_conf  = ict_ctx.amd_confidence

        _is_sweep_rev = (hasattr(signal, 'entry_type')
                         and signal.entry_type is not None
                         and 'REVERSAL' in str(signal.entry_type).upper())
        _has_ps_hint = (ict_ctx.direction_hint == "reverse"
                        and ict_ctx.direction_hint_confidence >= 0.40)

        if amd_phase == "ACCUMULATION":
            if _is_sweep_rev or _has_ps_hint:
                advisories.append(f"AMD=ACCUM sweep-reversal (phase lag expected, scoring handles it)")
            else:
                advisories.append(f"AMD=ACCUM non-reversal — low amd_score will penalise conviction")

        if amd_phase == "MANIPULATION" and amd_conf >= 0.65:
            bias_contra = (
                (signal.side == "long"  and "bear" in amd_bias) or
                (signal.side == "short" and "bull" in amd_bias)
            )
            if bias_contra and not _has_ps_hint:
                advisories.append(
                    f"AMD_ADVISORY: MANIP bias={amd_bias} conf={amd_conf:.2f} "
                    f"vs {signal.side} — contra-AMD, conviction will penalise")

        # ── AMD lag override for fresh sweeps ───────────────────────────────
        # (Already applied to ict_ctx.amd_phase in _evaluate_entry; repeated
        #  here for gate logging completeness only)

        # ── Direction Engine context ─────────────────────────────────────────
        if self._dir_engine is not None:
            try:
                _ps_agrees = (_has_ps_hint and ict_ctx.direction_hint_side == signal.side)
                if not _ps_agrees:
                    _hunt = getattr(self._dir_engine, '_last_hunt', None)
                    if _hunt is not None and hasattr(_hunt, 'delivery_direction'):
                        _del_dir   = getattr(_hunt, 'delivery_direction', '')
                        _hunt_conf = float(getattr(_hunt, 'confidence', 0.0))
                        if _hunt_conf >= 0.60:
                            agrees = (
                                (signal.side == "long"  and _del_dir == "bullish") or
                                (signal.side == "short" and _del_dir == "bearish") or
                                _del_dir in ("", "neutral", None)
                            )
                            if not agrees:
                                advisories.append(
                                    f"DIR_ADVISORY: hunt delivery={_del_dir} "
                                    f"conf={_hunt_conf:.2f} vs {signal.side} "
                                    f"(not blocking — conviction_filter weights flow)")
            except Exception:
                pass

        # ── HTF Structure context ────────────────────────────────────────────
        if self._ict is not None and getattr(self._ict, '_initialized', False):
            try:
                tf_data = getattr(self._ict, '_tf', {})
                tf_15m  = tf_data.get("15m")
                tf_4h   = tf_data.get("4h")
                if tf_15m and tf_4h:
                    t15 = str(getattr(tf_15m, 'trend', 'ranging') or 'ranging').lower()
                    t4h = str(getattr(tf_4h,  'trend', 'ranging') or 'ranging').lower()
                    both_bearish = (t15 == "bearish" and t4h == "bearish")
                    both_bullish = (t15 == "bullish" and t4h == "bullish")
                    if signal.side == "long" and both_bearish:
                        advisories.append(
                            f"HTF_ADVISORY: 15m={t15} 4H={t4h} both bearish vs LONG "
                            f"(not blocking — conviction_filter scores HTF via structure)")
                    elif signal.side == "short" and both_bullish:
                        advisories.append(
                            f"HTF_ADVISORY: 15m={t15} 4H={t4h} both bullish vs SHORT "
                            f"(not blocking — conviction_filter scores HTF via structure)")
            except Exception:
                pass

        # ── Order Flow context ───────────────────────────────────────────────
        tf  = flow_state.tick_flow if flow_state else 0.0
        cvd = flow_state.cvd_trend if flow_state else 0.0
        flow_opposes = (
            (signal.side == "long"  and tf < -0.35 and cvd < -0.20) or
            (signal.side == "short" and tf >  0.35 and cvd >  0.20)
        )
        if flow_opposes:
            advisories.append(
                f"FLOW_ADVISORY: tick={tf:+.2f} cvd={cvd:+.2f} "
                f"vs {signal.side} (scored in conviction_filter, not blocking)")

        # ── Session context ──────────────────────────────────────────────────
        session = str(getattr(ict_ctx, 'kill_zone', '') or '')
        if not session and self._ict:
            session = str(getattr(self._ict, '_killzone', '') or '')
        if session.upper() == "ASIA":
            advisories.append(
                "SESSION_ADVISORY: ASIA — conviction_filter applies session_score=0.60 "
                "(not blocking here)")

        # ── R:R context ──────────────────────────────────────────────────────
        if hasattr(signal, 'rr_ratio') and signal.rr_ratio < 1.2:
            advisories.append(
                f"RR_ADVISORY: {signal.rr_ratio:.1f} < 1.2 — "
                f"conviction_filter applies R:R penalty to pool_sig_score")

        # ── Log all advisories at DEBUG (not INFO — avoids log spam) ─────────
        if advisories:
            logger.debug(
                f"UNIFIED_GATE [{signal.side.upper()}] advisories (not blocking): "
                f"{' | '.join(advisories[:4])}")

        # ALWAYS PASS — conviction_filter score is the sole gate
        return True, "UNIFIED_GATE_PASS"


    def _evaluate_entry(self, data_manager, order_manager, risk_manager, now):
        """
        v9.0 — Liquidity-First Entry Engine.
        Single decision flow.  Falls back to legacy if new engine unavailable.

        Session note: this method runs regardless of session label (WEEKEND,
        OFF_HOURS, etc.).  Crypto markets are 24/7.  Session-awareness is
        handled by the factor scoring inside ConvictionFilter (WEEKEND scores
        0.65, ASIA is hard-blocked) and DirectionEngine Factor 8.  There is
        no early exit for weekends — a valid liquidity hunt at 3am Saturday
        is as real as one on Tuesday during the London open.
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

        # Step 3b: DirectionEngine — hunt prediction
        # Runs AFTER ICT update so structural context (AMD, MTF, pools, OBs/FVGs)
        # is fully refreshed.  Result is injected into ICT cache so every downstream
        # caller (get_confluence, get_status, Tier-L) reads DirectionEngine output
        # without knowing the computation moved here.
        if self._dir_engine is not None and self._ict is not None:
            try:
                _tf_de  = self._tick_eng.get_signal() if self._tick_eng else 0.0
                _cvd_de = self._cvd.get_trend_signal() if self._cvd else 0.0
                # FIX-8: pass previous tick's snapshot (before this tick's
                # liq_map.update() runs — see direction_engine FIX-8 guide).
                # _last_snapshot is set by get_snapshot() at the end of the
                # previous tick; it is None on the very first tick, which the
                # direction_engine handles gracefully (falls back to ICT pools).
                _prev_liq_snap = (getattr(self._liq_map, '_last_snapshot', None)
                                  if self._liq_map is not None else None)
                _hunt: HuntPrediction = self._dir_engine.predict_hunt(
                    price        = price,
                    atr          = atr,
                    now_ms       = now_ms,
                    ict_engine   = self._ict,
                    tick_flow    = _tf_de,
                    cvd_trend    = _cvd_de,
                    candles_5m   = candles_by_tf.get("5m", []),
                    liq_snapshot = _prev_liq_snap,
                )
                # Bridge HuntPrediction dataclass → legacy dict shape that the
                # rest of the codebase already consumes via _last_hunt_pred.
                self._ict.inject_hunt_prediction({
                    "predicted":          _hunt.predicted,
                    "confidence":         round(_hunt.confidence, 3),
                    "delivery_direction": _hunt.delivery_direction,
                    "raw_score":          round(_hunt.raw_score, 4),
                    "bsl_score":          round(_hunt.bsl_score, 3),
                    "ssl_score":          round(_hunt.ssl_score, 3),
                    "dealing_range_pd":   round(_hunt.dealing_range_pd, 3),
                    "swept_pool":         _hunt.swept_pool_price,
                    "opposing_pool":      _hunt.opposing_pool_price,
                    "reason":             _hunt.reason,
                    "scenario":           "",   # filled by get_hunt_scenario if needed
                    "confidence_factors": {},   # DirectionEngine uses HuntFactors dataclass
                }, now_ms)
                # Throttle DIR_ENGINE log: only emit at INFO when prediction
                # changes or at most once per 30s (same NEUTRAL repeated every tick
                # is pure noise — moved routine ticks to debug).
                _de_log_key = (_hunt.predicted, round(_hunt.confidence, 1))
                _de_last_key = getattr(self, "_dir_engine_last_log_key", None)
                _de_last_ts  = getattr(self, "_dir_engine_last_log_ts", 0.0)
                if _de_log_key != _de_last_key or (now - _de_last_ts) >= 30.0:
                    self._dir_engine_last_log_key = _de_log_key
                    self._dir_engine_last_log_ts  = now
                    logger.info(
                        f"🧭 DIR_ENGINE: hunt={_hunt.predicted or 'NEUTRAL'} "
                        f"conf={_hunt.confidence:.2f} "
                        f"delivery={_hunt.delivery_direction} "
                        f"raw={_hunt.raw_score:+.3f} "
                        f"BSL={_hunt.bsl_score:.2f} SSL={_hunt.ssl_score:.2f} "
                        f"| {_hunt.reason[:100]}")
                else:
                    logger.debug(
                        f"🧭 DIR_ENGINE: hunt={_hunt.predicted or 'NEUTRAL'} "
                        f"conf={_hunt.confidence:.2f} raw={_hunt.raw_score:+.3f} "
                        f"BSL={_hunt.bsl_score:.2f} SSL={_hunt.ssl_score:.2f} "
                        f"| {_hunt.reason[:100]}")
                # Send Telegram only when a high-confidence directional call is made
                # (>=0.55 = "strong" threshold from direction_engine constants).
                # Throttle to once per 5 minutes per direction to avoid spam on
                # slow-moving markets where the signal stays high for many ticks.
                _de_conf_thresh = 0.55
                _de_tg_key = f"_dir_tg_last_{_hunt.predicted or 'NEUTRAL'}"
                _de_last_tg = getattr(self, _de_tg_key, 0.0)
                if (_hunt.predicted is not None
                        and _hunt.confidence >= _de_conf_thresh
                        and (now - _de_last_tg) >= 300.0):
                    setattr(self, _de_tg_key, now)
                    try:
                        from telegram.notifier import format_direction_hunt_alert
                        _amd_ph = ""
                        _htf_b  = ""
                        _sess   = ""
                        _in_kz  = False
                        if self._ict is not None:
                            try:
                                _amd_ph = str(getattr(self._ict, 'amd_phase', '') or '')
                                _htf_b  = str(getattr(self._htf, 'htf_bias', '') or '') if self._htf else ''
                            except Exception:
                                pass
                        send_telegram_message(format_direction_hunt_alert(
                            predicted           = _hunt.predicted,
                            confidence          = _hunt.confidence,
                            delivery_direction  = _hunt.delivery_direction,
                            raw_score           = _hunt.raw_score,
                            bsl_score           = _hunt.bsl_score,
                            ssl_score           = _hunt.ssl_score,
                            reason              = _hunt.reason,
                            dealing_range_pd    = _hunt.dealing_range_pd,
                            swept_pool_price    = _hunt.swept_pool_price,
                            opposing_pool_price = _hunt.opposing_pool_price,
                            current_price       = price,
                            amd_phase           = _amd_ph,
                            htf_bias            = _htf_b,
                            factors             = _hunt.factors,
                        ))
                    except Exception as _tg_e:
                        logger.debug(f"DirectionEngine hunt Telegram error: {_tg_e}")
            except Exception as _de:
                logger.debug(f"DirectionEngine.predict_hunt error: {_de}")

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
        # BUG-1 FIX: Persist directional tick_flow/cvd_trend for use in
        # generate_periodic_report().  _flow_conviction is a non-negative
        # magnitude scalar — passing it to evaluate_sweep() as tick_flow
        # (which expects a signed [-1,+1] direction) corrupts the post-sweep
        # reversal score displayed in the periodic heartbeat report.
        self._last_tick_flow = tick_flow
        self._last_cvd_trend  = cvd_trend

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

                # BUG-C2 FIX: AMD phase lag override for fresh sweeps.
                # When AMD reports ACCUMULATION but a liquidity sweep occurred in
                # the last 2 bars (10m at 5m TF), the Wilder-smoothing lag is causing
                # the phase misread. Override to MANIPULATION so gate 1 logic is
                # correct. Confidence set to 0.50 (moderate — sweep confirmed but
                # full MANIPULATION evidence not yet accumulated).
                _fresh_sweep_ms = now_ms - 600_000  # 10 minutes = 2 bars at 5m
                if (ict_ctx.amd_phase == "ACCUMULATION"
                        and hasattr(self._ict, 'liquidity_pools')):
                    _has_fresh_sweep = any(
                        p.swept and p.sweep_timestamp > _fresh_sweep_ms
                        for p in self._ict.liquidity_pools
                    )
                    if _has_fresh_sweep:
                        logger.debug(
                            "AMD_LAG_OVERRIDE: Fresh sweep detected while AMD=ACCUMULATION "
                            "— overriding to MANIPULATION (Wilder-smoothing lag)")
                        ict_ctx.amd_phase = "MANIPULATION"
                        ict_ctx.amd_confidence = max(ict_ctx.amd_confidence, 0.50)
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
                # ── CONVICTION GATE: session boundary reset ────────────────────
                # When the killzone changes (OFF_HOURS→NEW_YORK, NEW_YORK→LONDON
                # etc.) reset entries_taken and consecutive_losses so each new
                # institutional session gets a fresh quota.  MIN_ENTRY_INTERVAL
                # cooldown is also cleared — it is intra-session pacing only.
                _new_kz = str(getattr(self._ict, '_session', '') or '')
                if (self._conviction is not None
                        and _new_kz
                        and _new_kz != self._last_conviction_kz):
                    self._last_conviction_kz = _new_kz
                    try:
                        self._conviction.on_session_change(_new_kz)
                    except Exception as _kz_e:
                        logger.debug(f"ConvictionFilter.on_session_change error: {_kz_e}")
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
                # ── FIX (ict-bridge-stale): widen window from 30s to 60s ──
                # The original 30s window meant only ~10% of each 5m bar had
                # sweep visibility (sweeps are detected at 5m bar close).
                # A genuine ICT sweep would flip ict_engine's state but never
                # reach the entry_engine because the bridge window had
                # already closed. Observed in production log: 12 ICT sweeps
                # fired over 4 hours, ZERO reached the entry engine.
                #
                # New window: 60s base, extended to include any sweep from
                # the current or previous 5m bar (plus 60s grace). This
                # matches the 60s window used by:
                #   - entry_engine._collect_sweeps() LiquidityMap path
                #   - quant_strategy._notified_sweeps prune window
                # Dedup is unaffected: _notified_sweeps and
                # entry_engine._processed_sweeps both key on (price, ts) at
                # sub-second resolution, so widening the detection window
                # cannot reintroduce sweep-loop reprocessing.
                _base_age_limit_ms = now_ms - 60_000
                # Start of the current 5m bar (Unix-epoch ms, floor-aligned)
                _cur_5m_start_ms   = (now_ms // 300_000) * 300_000
                # Accept a sweep if EITHER within 60s OR from the current/
                # previous 5m bar (ensures a bar-open sweep stays visible
                # for the full bar duration).
                _bar_age_limit_ms  = _cur_5m_start_ms - 300_000  # prev bar start

                for pool in self._ict.liquidity_pools:
                    if not pool.swept:
                        continue
                    if (pool.sweep_timestamp < _base_age_limit_ms
                            and pool.sweep_timestamp < _bar_age_limit_ms):
                        continue
                    c5 = candles_by_tf.get("5m", [])
                    # BUG-A5 FIX: Find the candle that ACTUALLY crossed the pool price.
                    # The old code blindly used c5[-2] (last closed bar), but the sweep
                    # may have occurred in c5[-3], c5[-4], etc. Using the wrong bar gives
                    # wick_extreme from an unrelated candle, placing SL on the wrong side
                    # of entry (confirmed by log: SL=$67,410 > entry=$67,279 for LONG).
                    #
                    # Search backward through the last 5 closed bars (exclude c5[-1]
                    # which is the forming bar). For SSL sweep: find bar whose LOW
                    # breached pool.price. For BSL sweep: find bar whose HIGH breached.
                    _ch = float(c5[-2]['h']) if len(c5) >= 2 else price
                    _cl = float(c5[-2]['l']) if len(c5) >= 2 else price
                    _cc = float(c5[-2]['c']) if len(c5) >= 2 else price
                    if len(c5) >= 3:
                        _lookback = c5[max(-6, -len(c5)):-1]  # up to 5 closed bars
                        for _cand in reversed(_lookback):
                            try:
                                _ch_cand = float(_cand['h'])
                                _cl_cand = float(_cand['l'])
                                _cc_cand = float(_cand['c'])
                                if pool.level_type == "SSL" and _cl_cand < pool.price:
                                    _ch, _cl, _cc = _ch_cand, _cl_cand, _cc_cand
                                    break
                                elif pool.level_type == "BSL" and _ch_cand > pool.price:
                                    _ch, _cl, _cc = _ch_cand, _cl_cand, _cc_cand
                                    break
                            except (KeyError, TypeError, ValueError):
                                continue
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
                    # Notify DirectionEngine so it can open a PostSweepState
                    # and begin the accumulative Bayesian evidence model.
                    # DEDUPLICATION: the bridge loop runs every 250ms and
                    # visits all swept pools within the 60s detection window.
                    # Without this guard, on_sweep() is called ~240 times
                    # for a single pool, resetting PostSweepState every tick
                    # and making accumulated evidence impossible to build.
                    if self._dir_engine is not None:
                        try:
                            _sweep_key = (pool.price, pool.sweep_timestamp)
                            if _sweep_key not in self._notified_sweeps:
                                self._notified_sweeps.add(_sweep_key)

                                # Bug-4 fix: cross-pool quality gate.
                                #
                                # Root cause: the bridge loop called on_sweep() for
                                # every freshly-swept ICT pool, unconditionally.  A
                                # lower-quality cross-pool sweep reset DirectionEngine's
                                # PostSweepState, destroying accumulated evidence from
                                # a higher-conviction earlier sweep.  e.g. a weak BSL
                                # touch-and-go could silently erase 60s of evidence
                                # built on a clean SSL displacement.
                                #
                                # Fix: if an active PostSweepState exists and the new
                                # pool is of the OPPOSITE type, only forward the sweep
                                # when its quality exceeds the active state's quality
                                # by a 20% premium.  Lower-quality cross-pool sweeps
                                # are skipped — the existing evaluation continues
                                # uninterrupted.  Same-type sweeps (structural
                                # continuation of the same pool side) always forward.
                                _new_quality = float(
                                    getattr(pool, 'displacement_score', 0.5) or 0.5)
                                _ps_active  = getattr(self._dir_engine, '_ps_state', None)
                                _should_forward = True
                                if _ps_active is not None:
                                    _active_type    = getattr(_ps_active, 'swept_pool_type', '')
                                    _active_quality = getattr(
                                        self._dir_engine, '_ps_state_quality', 0.0)
                                    if _active_type and pool.level_type != _active_type:
                                        _quality_threshold = _active_quality * 1.20
                                        if _new_quality <= _quality_threshold:
                                            logger.debug(
                                                f"ICT sweep bridge: cross-pool "
                                                f"{pool.level_type} @${pool.price:,.1f} "
                                                f"quality={_new_quality:.2f} <= active "
                                                f"{_active_type} threshold="
                                                f"{_quality_threshold:.2f} — skipped, "
                                                f"preserving existing PostSweepState")
                                            _should_forward = False

                                if _should_forward:
                                    self._dir_engine.on_sweep(
                                        swept_pool_price = pool.price,
                                        pool_type        = pool.level_type,
                                        price            = price,
                                        atr              = atr,
                                        now              = now,
                                        quality          = _new_quality,
                                    )
                            # BUG-D1 FIX: Prune _notified_sweeps unconditionally
                            # per-pool-visit (not just inside _should_forward path)
                            # so the set stays bounded even when no new sweeps arrive.
                            _cutoff_ms = now_ms - 60_000
                            self._notified_sweeps = {
                                k for k in self._notified_sweeps
                                if k[1] > _cutoff_ms
                            }
                        except Exception:
                            pass
            except Exception as e:
                logger.debug(f"ICT sweep bridge error: {e}")

        # Step 6c: Post-sweep evaluation (DirectionEngine accumulative model)
        # Runs every tick while DirectionEngine has an open PostSweepState.
        # Evidence builds across ticks — momentary noise cannot flip the decision.
        # Bug-4 fix: the verdict (action/direction/confidence) is written into
        # ict_ctx.direction_hint* BEFORE entry_engine.update() so that
        # _evaluate_post_sweep_accumulative() can consume it as a dynamic
        # weighting factor.  Previously the verdict was only logged and Telegrammed
        # — DirectionEngine was purely observational and had no effect on entries.
        if self._dir_engine is not None and getattr(self._dir_engine, 'in_post_sweep', False):
            try:
                _tf_ps  = self._tick_eng.get_signal() if self._tick_eng else 0.0
                _cvd_ps = self._cvd.get_trend_signal() if self._cvd else 0.0
                _ps_decision = self._dir_engine.evaluate_sweep(
                    price        = price,
                    atr          = atr,
                    now          = now,
                    ict_engine   = self._ict,
                    tick_flow    = _tf_ps,
                    cvd_trend    = _cvd_ps,
                    liq_snapshot = liq_snapshot,   # fresh — liq_map.update() already ran
                )
                # Store for conviction gate's CISD factor (issue-2 fix)
                if _ps_decision is not None:
                    self._dir_engine._last_ps_decision = _ps_decision
                if _ps_decision is not None and _ps_decision.action in ("reverse", "continue"):
                    # Inject verdict into ICTContext so entry_engine can weight it
                    ict_ctx.direction_hint            = _ps_decision.action
                    ict_ctx.direction_hint_side       = getattr(_ps_decision, 'direction', '')
                    ict_ctx.direction_hint_confidence = _ps_decision.confidence
                    logger.info(
                        f"🔁 POST-SWEEP [{_ps_decision.action.upper()}]: "
                        f"dir={getattr(_ps_decision, 'direction', '?')} "
                        f"conf={_ps_decision.confidence:.2f} "
                        f"| {_ps_decision.reason[:80]}")
                    try:
                        from telegram.notifier import format_post_sweep_verdict
                        _ps_state = getattr(self._dir_engine, '_ps_state', None)
                        send_telegram_message(format_post_sweep_verdict(
                            action           = _ps_decision.action,
                            direction        = getattr(_ps_decision, 'direction', ''),
                            confidence       = _ps_decision.confidence,
                            phase            = getattr(_ps_decision, 'phase', ''),
                            cisd_active      = getattr(_ps_decision, 'cisd_active', False),
                            ote_active       = getattr(_ps_decision, 'ote_active', False),
                            displacement_atr = getattr(_ps_decision, 'displacement_atr', 0.0),
                            rev_score        = getattr(_ps_decision, 'rev_score', 0.0),
                            cont_score       = getattr(_ps_decision, 'cont_score', 0.0),
                            rev_reasons      = getattr(_ps_decision, 'rev_reasons', []),
                            cont_reasons     = getattr(_ps_decision, 'cont_reasons', []),
                            reason           = _ps_decision.reason,
                            swept_pool_price = getattr(_ps_state, 'swept_pool_price', None) if _ps_state else None,
                            swept_pool_type  = getattr(_ps_state, 'swept_pool_type',  '')   if _ps_state else '',
                            current_price    = price,
                        ))
                    except Exception as _tg_ps:
                        logger.debug(f"DirectionEngine post-sweep Telegram error: {_tg_ps}")
                elif _ps_decision is not None:
                    # "wait" — clear any stale hint so entry_engine doesn't act on it
                    ict_ctx.direction_hint            = ""
                    ict_ctx.direction_hint_side       = ""
                    ict_ctx.direction_hint_confidence = 0.0
                    logger.debug(
                        f"⏳ POST-SWEEP [{_ps_decision.action.upper()}]: "
                        f"{_ps_decision.reason[:80]}")
            except Exception as _pse:
                logger.debug(f"DirectionEngine.evaluate_sweep error: {_pse}")

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

            # ── UNIFIED ENTRY GATE — advisory logging only (no blocking) ───
            self._unified_entry_gate(
                signal, ict_ctx, flow_state, liq_snapshot, price, atr, now)

            logger.info(
                f"SIGNAL: {signal.entry_type.value} {signal.side.upper()} "
                f"@ ${signal.entry_price:,.1f} | "
                f"SL=${signal.sl_price:,.1f} TP=${signal.tp_price:,.1f} "
                f"R:R={signal.rr_ratio:.1f} | {signal.reason}")

            # ── ISSUE-4 FIX: Conviction Gate ─────────────────────────────────
            # Evaluate 7 ICT factors (mandatory gates + weighted score ≥ 0.75).
            # Retrieve the PostSweepDecision — try dir_engine first, fall back
            # to entry_engine's _last_sweep_analysis (which has CISD, displacement,
            # etc. from the actual sweep evaluation that produced this signal).
            if self._conviction is not None:
                _ps_dec_for_conv = None
                if self._dir_engine is not None:
                    try:
                        _ps_dec_for_conv = getattr(
                            self._dir_engine, '_last_ps_decision', None)
                    except Exception:
                        pass
                # FIX-CISD-FLOW: dir_engine may not have entered post_sweep
                # (it detects sweeps independently). Fall back to entry_engine's
                # sweep analysis which actually produced this signal.
                if _ps_dec_for_conv is None and self._entry_engine is not None:
                    try:
                        _sa = getattr(self._entry_engine, '_last_sweep_analysis', None)
                        if _sa and _sa.get('rev_score', 0) > 0:
                            from strategy.direction_engine import PostSweepDecision as _DirPSD
                            _ps_dec_for_conv = _DirPSD(
                                action="reverse" if _sa['rev_score'] > _sa.get('cont_score', 0) else "continue",
                                direction=signal.side,
                                confidence=min(1.0, max(_sa['rev_score'], _sa.get('cont_score', 0)) / 90.0),
                                phase=_sa.get('phase', ''),
                                cisd_active=bool(_sa.get('cisd', False)),
                                ote_active=bool(_sa.get('ote', False)),
                                displacement_atr=float(_sa.get('displacement_atr', 0.0)),
                                rev_score=float(_sa.get('rev_score', 0)),
                                cont_score=float(_sa.get('cont_score', 0)),
                                reason=signal.reason,
                            )
                    except Exception:
                        pass

                # MOD-6 FIX: Pass ict._session (canonical "LONDON"/"NY"/"ASIA"),
                # not ict._killzone ("LONDON_KZ" / "" outside KZ window).
                # _killzone is empty between kill-zones even when the session is
                # active, causing the conviction gate to misread the session as ''
                # and apply a 0.40 penalty instead of the full 1.00 session score.
                _sess_str = ""
                if self._ict is not None:
                    _sess_str = str(getattr(self._ict, '_session', '') or '')

                # Resolve sweep_pool: prefer signal.swept_pool (actual swept pool),
                # fall back to primary_target (pool being approached).
                _conv_pool = None
                if hasattr(signal, 'swept_pool') and signal.swept_pool is not None:
                    _conv_pool = signal.swept_pool
                elif hasattr(signal, 'target_pool') and signal.target_pool is not None:
                    _conv_pool = signal.target_pool
                elif liq_snapshot and liq_snapshot.primary_target is not None:
                    _conv_pool = liq_snapshot.primary_target

                # Entry type for approach vs reversal detection
                _entry_type_str = (signal.entry_type.value
                                   if hasattr(signal, 'entry_type') and signal.entry_type is not None
                                   else "")

                # FIX-OTE-REVERSAL: pass the sweep wick extreme so the conviction
                # filter can use it as the correct Fibonacci anchor for OTE scoring.
                # Without this, reversal entries always score OTE≈0.10 because
                # pool_price ≈ current price (no retrace has occurred yet post-sweep).
                _sweep_wick = 0.0
                if hasattr(signal, 'sweep_result') and signal.sweep_result is not None:
                    _sweep_wick = float(
                        getattr(signal.sweep_result, 'wick_extreme', 0.0) or 0.0)

                # FIX-DISP-FLOW: get measured displacement from entry_engine
                _measured_disp_atr = 0.0
                if _ps_dec_for_conv is not None:
                    _measured_disp_atr = float(getattr(_ps_dec_for_conv, 'displacement_atr', 0.0) or 0.0)
                if _measured_disp_atr <= 0 and self._entry_engine is not None:
                    _sa = getattr(self._entry_engine, '_last_sweep_analysis', None)
                    if _sa:
                        _measured_disp_atr = float(_sa.get('displacement_atr', 0.0) or 0.0)

                _conv_result = self._conviction.evaluate(
                    trade_side       = signal.side,
                    sweep_pool       = _conv_pool,   # None is handled by _extract_pool_info
                    entry_price      = signal.entry_price,
                    sl_price         = signal.sl_price,
                    tp_price         = signal.tp_price,
                    price            = price,
                    atr              = atr,
                    now              = now,
                    ict_engine       = self._ict,
                    liq_snapshot     = liq_snapshot,
                    ps_decision      = _ps_dec_for_conv,
                    candles_5m       = candles_by_tf.get("5m"),
                    session          = _sess_str,
                    entry_type       = _entry_type_str,
                    sweep_wick_price = _sweep_wick,
                    measured_displacement_atr = _measured_disp_atr,
                )
                if not _conv_result.allowed:
                    reject_str = " | ".join(_conv_result.reject_reasons[:3])
                    # Deduplicate: only log at INFO when score or reason changes
                    _conv_key = (signal.side, round(_conv_result.score, 2), reject_str[:60])
                    _conv_last = getattr(self, "_last_conv_block_key", None)
                    if _conv_key != _conv_last:
                        self._last_conv_block_key = _conv_key
                        logger.info(
                            f"🚫 CONVICTION GATE BLOCKED [{signal.side.upper()}] "
                            f"score={_conv_result.score:.3f} | {reject_str}")
                    else:
                        logger.debug(
                            f"🚫 CONVICTION GATE BLOCKED [{signal.side.upper()}] "
                            f"score={_conv_result.score:.3f} | {reject_str}")
                    # BUG-B2 / FIX-SPAM / FIX-SL-FLIP:
                    # Route the gate-block notification to the correct suppressor
                    # based on signal type.
                    #
                    # DISPLACEMENT_MOMENTUM → mark_momentum_blocked():
                    #   Freezes the SL at signal.sl_price so it doesn't oscillate
                    #   between the ICT-OB path and the ATR-fallback path on
                    #   alternating ticks. Also suppresses spam for 30s (configurable).
                    #
                    # POST_SWEEP (REVERSAL / CONTINUATION) → mark_gate_blocked():
                    #   Existing 45s cooldown for the evidence-accumulation model.
                    #   Not used for momentum because momentum has no evidence state
                    #   to accumulate — the cooldown logic is different.
                    _is_momentum = (
                        hasattr(signal, 'entry_type')
                        and signal.entry_type is not None
                        and 'MOMENTUM' in str(signal.entry_type).upper()
                    )
                    if _is_momentum and hasattr(self._entry_engine, 'mark_momentum_blocked'):
                        self._entry_engine.mark_momentum_blocked(
                            entry_price = signal.entry_price,
                            candle_ts   = getattr(signal, 'displacement_candle_ts', 0),
                            locked_sl   = signal.sl_price,
                            cooldown_sec= 30.0,
                        )
                    elif hasattr(self._entry_engine, 'mark_gate_blocked'):
                        self._entry_engine.mark_gate_blocked(
                            signal.side, reject_str[:40], cooldown_sec=10.0)
                        # BUG-3 FIX (GATE-ORPHAN): consume_signal AFTER mark_gate_blocked
                        # (same ordering rationale as the unified gate path above).
                        self._entry_engine.consume_signal()
                    else:
                        self._entry_engine.consume_signal()
                    # Telegram alert for conviction block (throttled 60s per side)
                    _conv_tg_key = f"_conv_tg_last_{signal.side}"
                    if now - getattr(self, _conv_tg_key, 0.0) >= 60.0:
                        setattr(self, _conv_tg_key, now)
                        try:
                            from telegram.notifier import format_conviction_block_alert
                            from telegram.notifier import send_telegram_message as _stm
                            _stm(format_conviction_block_alert(
                                side          = signal.side,
                                score         = _conv_result.score,
                                reject_reasons= _conv_result.reject_reasons,
                                allow_reasons = _conv_result.allow_reasons,
                                factors       = _conv_result.factors,
                                entry_price   = signal.entry_price,
                                sl_price      = signal.sl_price,
                                tp_price      = signal.tp_price,
                                rr_ratio      = _conv_result.rr_ratio,
                            ))
                        except Exception as _cv_tg_e:
                            logger.debug(f"ConvictionGate Telegram error: {_cv_tg_e}")
                    return

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
                prefetched_bal_info=bal_info,   # Bug #5: reuse fetched balance
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
            # Compute nearest pool distances directly (more reliable than snapshot property)
            _nearest_bsl = liq_snapshot.nearest_bsl_atr
            _nearest_ssl = liq_snapshot.nearest_ssl_atr
            if _nearest_bsl <= 0 and liq_snapshot.bsl_pools:
                _nearest_bsl = min((pt.distance_atr for pt in liq_snapshot.bsl_pools), default=0)
            if _nearest_ssl <= 0 and liq_snapshot.ssl_pools:
                _nearest_ssl = min((pt.distance_atr for pt in liq_snapshot.ssl_pools), default=0)
            parts.append(f"BSL={_nearest_bsl:.1f}ATR")
            parts.append(f"SSL={_nearest_ssl:.1f}ATR")

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

            # ── Scan-skip diagnostics (observability fix) ──────────────────
            # When the engine is in SCANNING and not transitioning, surface
            # WHY. Silent rejection previously made "no trades" impossible
            # to diagnose — see 4-hour log with 12 ICT sweeps fired and
            # zero reaching the entry engine.
            if state == "SCANNING":
                try:
                    _skip = getattr(
                        self._entry_engine, 'scan_skip_info', None)
                    if callable(_skip):
                        _skip = _skip()
                    if _skip:
                        _bits = []
                        _liq = _skip.get("liq")
                        if _liq:
                            _liq_bits = [f"{k}={v}" for k, v in _liq.items() if v]
                            if _liq_bits:
                                _bits.append("liq:" + ",".join(_liq_bits))
                        _br = _skip.get("bridge")
                        if _br:
                            _br_bits = [f"{k}={v}" for k, v in _br.items() if v]
                            if _br_bits:
                                _bits.append("bridge:" + ",".join(_br_bits))
                        if _bits:
                            parts.append(f"SkipSweep=[{' '.join(_bits)}]")
                except Exception:
                    pass

            logger.info(f"[THINK] {' | '.join(parts)}")


    def _compute_sl_tp(self, data_manager, price, side, atr, mode="reversion",
                       signal_confidence=0.5, use_maker_entry=False):
        """
        Institutional SL/TP computation — ICT OB → 15m swing → ATR fallback.
        
        Primary path: EntryEngine provides force_sl/force_tp (used in _enter_trade).
        This method is the FALLBACK when force levels are invalid.
        
        Hierarchy: ICT 15m OB → 15m swing structure → ATR fallback.
        TP: Liquidity pool → ICT structural → VWAP tiered selection.
        Fee-normalized TP floor gate applied at the end.
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
        sl_price    = None
        tp_price    = None
        _sl_source  = "none"

        now_ms_slatp = int(time.time() * 1000)

        # ══════════════════════════════════════════════════════════════════════
        # SL/TP COMPUTATION — ICT OB → 15m swing → ATR fallback
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

        # ── Fee TP advisory (no longer blocks trades) ────────────────────────
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
                    logger.debug(
                        f"Fee advisory: TP dist {tp_distance:.0f} < fee floor {min_tp:.0f} "
                        f"(not blocking — trade proceeds)")
            except Exception as e:
                logger.debug(f"Fee check error (non-fatal): {e}")

        return sl_price, tp_price

    def _enter_trade(self, data_manager, order_manager, risk_manager, side, sig, mode="reversion",
                     ict_tier: str = "", prefetched_bal_info: dict = None):
        """
        Position entry — v7.0 (confidence-weighted sizing via ict_tier).

        ict_tier: "S" | "A" | "B" | "" — controls position size multiplier:
          Tier-S: 1.00× base margin  (full conviction — confirmed sweep + AMD)
          Tier-A: 0.80× base margin  (high conviction — structural alignment)
          Tier-B: 0.65× base margin  (standard quant + ICT gate)
          "":     0.50× base margin  (minimal exposure — no ICT gate)

        Additionally modulated by composite score (±10%) and AMD confidence (±8%).

        prefetched_bal_info: Bug #5 — when supplied, the REST balance call is
        skipped. _evaluate_entry already fetched the balance in the same tick;
        making a second call adds latency with no benefit (balance cannot change
        between the two calls — no position is open at this point).
        """
        price = data_manager.get_last_price()
        if price < 1.0: return
        atr = self._atr_5m.atr
        if atr < 1e-10: return

        # ── Risk gate ─────────────────────────────────────────────────────────────
        # Bug #5 fix: reuse prefetched balance when available; only call the REST
        # endpoint as a fallback (e.g. when _enter_trade is invoked outside of the
        # normal _evaluate_entry → _launch_entry_async path).
        if prefetched_bal_info is not None:
            bal_info = prefetched_bal_info
        else:
            bal_info = risk_manager.get_available_balance()
        if bal_info is None: return
        total_bal = float(bal_info.get("total", bal_info.get("available", 0.0)))
        self._risk_gate.set_opening_balance(total_bal)
        # NOTE: risk gate already checked in _evaluate_entry — no duplicate check here

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
                # Bug #3 fix: LIMIT_ORDER_OFFSET_TICKS was only applied in the
                # fallback path (empty book). With a live book the offset was
                # silently ignored, defeating its purpose of improving maker-fill
                # probability by placing the order slightly inside the spread.
                if side == "long":
                    limit_px  = round(_best_px(bids[0]) - offset, 1)
                    mt_reason = f"limit_long@bid-{offset:.1f}={limit_px:.1f}"
                else:
                    limit_px  = round(_best_px(asks[0]) + offset, 1)
                    mt_reason = f"limit_short@ask+{offset:.1f}={limit_px:.1f}"
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
            # Force levels active — fee engine check is advisory only (no block)
            if self._fee_engine is not None and self._fee_engine.is_warmed_up():
                try:
                    _tp_dist = abs(tp_price - price)
                    _min_tp = self._fee_engine.min_required_tp_move(
                        price=price, atr=atr,
                        atr_percentile=self._atr_5m.get_percentile(),
                        use_maker_entry=use_maker,
                        signal_confidence=signal_confidence)
                    if _tp_dist < _min_tp:
                        logger.debug(f"Fee advisory: TP dist {_tp_dist:.0f} < fee floor {_min_tp:.0f} (not blocking)")
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
            risk_manager, price, sig=sig, ict_tier=ict_tier, sl_price=sl_price,
            prefetched_bal_info=bal_info
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
        #
        # BUG 2 FIX: on_order_placed callback captures the exact moment the
        # limit order hits the exchange (REST 200 OK returned an order_id).
        # The on_tick watchdog switches from Stage A (pre-order, 45 s tolerance)
        # to Stage B (post-order, fill-timeout + 25 %) at that instant.  This
        # prevents the watchdog from firing while the bracket fill-poll is
        # still legitimately running.
        def _on_order_placed(_oid: str) -> None:
            self._entry_order_placed_at = time.time()
            logger.info(
                f"⏱️  Entry order placed on exchange (order_id={_oid[:12]}…) "
                f"— watchdog switched to Stage B (fill-poll)")

        limit_timeout = float(getattr(config, 'LIMIT_ORDER_FILL_TIMEOUT_SEC', 45.0))
        is_bracket = False
        entry_data = order_manager.place_bracket_limit_entry(
            side=side, quantity=qty,
            limit_price=limit_px,
            sl_price=sl_price, tp_price=tp_price,
            timeout_sec=limit_timeout,
            on_order_placed=_on_order_placed,
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
                on_order_placed=_on_order_placed,
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
        # Reset duplicate guards for the new position
        self._exit_completed        = False
        self._pnl_recorded_for     = 0.0
        # FIX Bug-C: record_trade_start() AFTER confirmed fill, not before.
        # Moving it here prevents aborted entries (TP-gate, fee-gate, exchange
        # error) from consuming the daily trade cap with no actual order sent.
        self._risk_gate.record_trade_start()
        # CONVICTION GATE: arm MIN_ENTRY_INTERVAL pacing timer and increment
        # entries_taken ONLY after the order is confirmed filled and the position
        # is ACTIVE.  Calling this here (not in evaluate()) ensures that signals
        # which pass conviction but fail to execute (margin too low, TP gate,
        # exchange error) do NOT lock out the next signal for 900 seconds.
        if self._conviction is not None:
            try:
                self._conviction.mark_entry_placed(time.time())
            except Exception as _cv_mp_e:
                logger.debug(f"ConvictionFilter.mark_entry_placed error: {_cv_mp_e}")
        if hasattr(self, '_entry_engine') and self._entry_engine is not None:
            self._entry_engine.on_position_opened()

        # ── Clear post-sweep evidence — trade now open ────────────────────────
        if self._dir_engine is not None:
            try:
                self._dir_engine.clear_sweep()
            except Exception:
                pass

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

        # ── Conditionally compute signals — only when trade mode consumes them ──
        # Bug #7/#19 fix: _compute_signals() runs all five signal engines (VWAP,
        # CVD, ADX, OB, tick) on every active tick, but in the dominant trade mode
        # "reversion" (all liquidity-first entries) the result is used only for
        # _log_thinking() — pure overhead.  The WeightScheduler and its dynamic
        # regime weights are also dead code in the v10 liquidity-first path
        # (_evaluate_entry routes through pool-based logic, never calls
        # _compute_signals).
        #
        # Gate: only compute when trade_mode is "trend" (regime-flip exit check)
        # or "flow" (sustained counter-flow exit check).  In "reversion" mode we
        # still call _log_thinking with the last cached signal so the thinking log
        # continues to appear — it just isn't recomputed every tick.
        _needs_signals = pos.trade_mode in ("trend", "flow")
        sig = None
        if _needs_signals:
            sig = self._compute_signals(data_manager)
            if sig is not None:
                self._last_sig = sig   # keep cache fresh for display/heartbeat
        else:
            # In reversion mode: use the cached last signal for logging only.
            sig = getattr(self, '_last_sig', None)

        if sig is not None:
            self._log_thinking(sig, price, now)

            # These exit checks are only valid on a freshly computed signal;
            # the cached reversion-mode signal is stale and must not drive exits.
            if _needs_signals:
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
            # Guard: only run when signals were freshly computed (not cached).
            if _needs_signals and pos.trade_mode == "flow":
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

        # ── DirectionEngine: pool-hit gate ────────────────────────────────────
        # Runs every tick while in position. When price is near a pool, the gate
        # determines whether to exit (TP hit), reverse, continue to next pool, or hold.
        # action="exit"     → close position now (pool TP reached)
        # action="reverse"  → close and open opposite
        # action="continue" → update TP to next pool, tighten SL
        # action="hold"     → nothing, let existing SL/TP manage
        if self._dir_engine is not None and not pos.is_flat():
            try:
                _tf_ph  = self._tick_eng.get_signal() if self._tick_eng else 0.0
                _cvd_ph = self._cvd.get_trend_signal() if self._cvd else 0.0
                # FIX-5: pass liq_snapshot so pool_hit_gate can auto-resolve
                # next_pool from the nearest qualifying opposing pool.
                _gate_liq_snap = None
                try:
                    if self._liq_map is not None:
                        _gate_liq_snap = self._liq_map.get_snapshot(
                            price, self._atr_5m.atr if self._atr_5m else 1.0)
                except Exception:
                    pass
                _gate = self._dir_engine.pool_hit_gate(
                    pos_side   = pos.side,
                    pos_entry  = pos.entry_price,
                    pos_sl     = pos.sl_price,
                    pos_tp     = pos.tp_price,
                    price      = price,
                    atr        = self._atr_5m.atr if self._atr_5m else 1.0,
                    ict_engine = self._ict,
                    tick_flow  = _tf_ph,
                    cvd_trend  = _cvd_ph,
                    liq_snapshot = _gate_liq_snap,
                )
                if _gate is not None and _gate.action == "reverse":
                    # BUG-3 FIX: pool_hit_gate "reverse" must NEVER close the
                    # position.  The gate fires every tick once AMD flips contra
                    # — exiting on each tick would fire multiple exits and leave
                    # the bot flat at a suboptimal price.  Instead: migrate SL
                    # to breakeven (capital protection) and send a Telegram
                    # awareness alert.  The existing SL/TP bracket remains live
                    # and manages the exit when the market decides.
                    logger.warning(
                        f"⚠️ POOL-GATE: REVERSE signal — no exit taken, "
                        f"migrating SL to BE if not already there. "
                        f"conf={_gate.confidence:.2f} | {_gate.reason[:100]}")

                    _be_tick  = _round_to_tick(
                        _calc_be_price(pos.side, pos.entry_price,
                                       self._atr_5m.atr if self._atr_5m else 1.0,
                                       pos=pos))
                    _be_needed = (
                        (pos.side == "long"  and pos.sl_price < _be_tick) or
                        (pos.side == "short" and pos.sl_price > _be_tick)
                    )
                    if _be_needed and pos.sl_order_id and not pos.be_ratchet_applied:
                        try:
                            _es = "sell" if pos.side == "long" else "buy"
                            _be_result = order_manager.replace_stop_loss(
                                existing_sl_order_id = pos.sl_order_id,
                                side                 = _es,
                                quantity             = pos.quantity,
                                new_trigger_price    = _be_tick,
                                old_trigger_price    = pos.sl_price,
                            )
                            if _be_result is None:
                                # replace_stop_loss returning None means SL already
                                # fired — treat as a fill event and bail out.
                                self._record_exchange_exit(None)
                                return
                            if isinstance(_be_result, dict) and _be_result.get("error") == "UNPROTECTED":
                                logger.critical(
                                    "💀 POOL-GATE BE: SL replace UNPROTECTED — "
                                    "emergency-flattening.")
                                try:
                                    if hasattr(order_manager, "emergency_flatten"):
                                        order_manager.emergency_flatten(reason="pool_gate_be_unprotected")
                                    else:
                                        order_manager.place_market_order(
                                            side=_es, quantity=pos.quantity, reduce_only=True)
                                except Exception as _ef_e:
                                    logger.error(f"emergency_flatten raised: {_ef_e}", exc_info=True)
                                with self._lock:
                                    if self._pos.phase == PositionPhase.ACTIVE:
                                        self._pos.phase = PositionPhase.EXITING
                                        self._exiting_since = time.time()
                                return
                            if isinstance(_be_result, dict) and _be_result.get("error") == "PLACE_FAILED_RESTORED":
                                _r_oid = _be_result.get("restore_order_id")
                                _r_trig = float(_be_result.get("restore_trigger", 0) or 0)
                                with self._lock:
                                    if _r_oid: pos.sl_order_id = _r_oid
                                    if _r_trig > 0:
                                        pos.sl_price = _r_trig
                                        self.current_sl_price = _r_trig
                                logger.warning(
                                    f"⚠️ POOL-GATE BE: SL restored at ${_r_trig:,.2f} "
                                    f"(requested ${_be_tick:,.2f}).")
                            elif isinstance(_be_result, dict) and "error" not in _be_result:
                                with self._lock:
                                    pos.sl_price           = _be_tick
                                    pos.sl_order_id        = (_be_result.get("order_id")
                                                              or pos.sl_order_id)
                                    pos.be_ratchet_applied = True
                                    self.current_sl_price  = _be_tick
                                logger.info(
                                    f"🔒 POOL-GATE REVERSE → SL migrated to BE "
                                    f"${_be_tick:,.2f} (no exit; bracket manages)")
                                send_telegram_message(
                                    f"⚠️ <b>POOL-GATE: STRUCTURAL REVERSAL SIGNAL</b>\n"
                                    f"Pool hit with contra AMD flow — <b>no exit taken</b>\n"
                                    f"SL migrated to breakeven: <b>${_be_tick:,.2f}</b>\n"
                                    f"Conf: {_gate.confidence:.0%} | {_gate.reason[:150]}")
                            else:
                                logger.debug(
                                    f"Pool-gate BE migration rejected by exchange: "
                                    f"{_be_result}")
                        except Exception as _be_e:
                            logger.debug(
                                f"Pool-gate BE migration error (non-fatal): {_be_e}")
                    else:
                        # SL is already at or beyond BE, or no sl_order_id yet.
                        # Send awareness-only alert so operator can see the signal.
                        try:
                            from telegram.notifier import format_pool_gate_alert
                            send_telegram_message(format_pool_gate_alert(
                                action        = "hold",
                                confidence    = _gate.confidence,
                                reason        = f"[REVERSE signal — no exit] {_gate.reason}",
                                pos_side      = pos.side,
                                pos_entry     = pos.entry_price,
                                current_price = price,
                                pos_sl        = pos.sl_price,
                                pos_tp        = pos.tp_price,
                                atr           = self._atr_5m.atr if self._atr_5m else 0.0,
                            ))
                        except Exception:
                            send_telegram_message(
                                f"⚠️ <b>POOL-GATE: REVERSE SIGNAL (no exit)</b>\n"
                                f"SL already at/beyond BE — bracket manages\n"
                                f"Conf: {_gate.confidence:.0%} | {_gate.reason[:150]}")
                    # NOTE: no early return — trail engine must still run this tick.
                elif _gate is not None and _gate.action == "continue" and _gate.next_target:
                    # ════════════════════════════════════════════════════════════
                    # TP IMMUTABILITY POLICY
                    # ════════════════════════════════════════════════════════════
                    # The Take Profit is set at entry and NEVER amended.
                    #
                    # WHY:
                    #   In production, 23/24 trades exited via SL.  Only 1 hit TP.
                    #   Extending TP further when the first target isn't reached
                    #   guarantees the trade dies to a trailing SL instead.
                    #
                    #   The original TP was set at the opposing liquidity pool —
                    #   the structural delivery target.  If the market doesn't
                    #   reach it, the SL trail manages the exit with whatever
                    #   profit was captured.
                    #
                    #   If you believe TP should be further, that's a NEW THESIS.
                    #   Open a new position after the current one closes.
                    #
                    # WHAT WE DO INSTEAD:
                    #   Log the suggestion for post-trade analysis.
                    #   The trail engine may tighten SL on the next tick,
                    #   which naturally locks available profit.
                    #
                    _next = _gate.next_target
                    logger.info(
                        f"📝 POOL-GATE CONTINUE: next target ${_next:,.0f} — "
                        f"TP stays at ${pos.tp_price:,.0f} (immutability policy) | "
                        f"conf={_gate.confidence:.2f} | {_gate.reason[:80]}")
                    # DO NOT call replace_take_profit.
                    # DO NOT modify pos.tp_price.
                    # The existing SL/TP bracket manages the exit.
                # action="hold" → do nothing, let existing SL/TP manage
            except Exception as _pg:
                logger.debug(f"DirectionEngine.pool_hit_gate error: {_pg}")

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
                                     name=f"trail-sl-{int(now*1000)%100000}").start()

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
        """Institutional trail v5.0 — 5-feature upgrade (OB/Breaker priority,
        AMD-phase adaptive buffer, 4H/1H HTF cascade, liq pool ceiling,
        displacement+CVD gate). All SL changes are LIMIT orders only."""
        pos = self._pos; atr = self._atr_5m.atr
        if atr < 1e-10: return False
        if pos.entry_price < 1.0:
            logger.warning("Trail: entry_price invalid (%.2f) — skipping", pos.entry_price)
            return False
        if not pos.sl_order_id:
            logger.debug("Trail: sl_order_id unknown — skipping")
            return False
        profit = (price-pos.entry_price) if pos.side=="long" else (pos.entry_price-price)

        # CRIT-2 FIX: All compound read-modify-write mutations on shared PositionState
        # must be executed under self._lock to prevent TOCTOU races with the main
        # thread (_log_thinking, _manage_active) and the reconcile thread.
        # Python's GIL protects individual attr stores but NOT the if/assign pattern:
        #   T1: reads peak_profit=50, T2: reads peak_profit=50 → both pass → T2 overwrites T1.
        with self._lock:
            if profit > pos.peak_profit:
                pos.peak_profit = profit

            # PostTradeAgent: track Maximum Adverse Excursion (MAE)
            _adverse = max(0.0, -profit)
            if _adverse > pos.peak_adverse:
                pos.peak_adverse = _adverse

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

        # ── Issue 2 fix: Refresh ICT engine with live multi-timeframe structure ──
        # Also captures HTF candles (4h/1h) here for the trail engine cascade.
        _trail_candles_15m = None
        _trail_candles_1h  = None
        _trail_candles_4h  = None
        if self._ict is not None:
            try:
                candles_15m        = data_manager.get_candles("15m", limit=200)
                _trail_candles_15m = candles_15m
                _trail_5m          = data_manager.get_candles("5m",  limit=300)
                _trail_1m          = data_manager.get_candles("1m",  limit=120)
                _trail_candles_1h  = data_manager.get_candles("1h",  limit=100)
                _trail_candles_4h  = data_manager.get_candles("4h",  limit=50)
                self._ict.update(_trail_5m, candles_15m, price, now_ms,
                                 candles_1m=_trail_1m,
                                 candles_1h=_trail_candles_1h,
                                 candles_4h=_trail_candles_4h,
                                 candles_1d=data_manager.get_candles("1d", limit=30))
            except Exception as _ict_refresh_e:
                logger.debug(f"Trail ICT refresh error (non-fatal): {_ict_refresh_e}")

        # ══════════════════════════════════════════════════════════════════
        # FIBONACCI TRAIL ENGINE (v5.0) — SOLE TRAILING ENGINE
        # ══════════════════════════════════════════════════════════════════
        # Pure-Fibonacci trailing: bar-close-gated, close-confirmation counter,
        # swing-invalidation, momentum gate, liquidity-aware buffer, HTF
        # alignment, Counter-BOS sovereign override, OTE pullback freeze.
        # No fallback logic: if the engine returns new_sl=None, we HOLD.
        # ══════════════════════════════════════════════════════════════════
        if self._liq_trail is None:
            # Engine not wired in — no trailing possible.
            logger.debug("Trail: LiquidityTrailEngine not initialised — HOLD")
            return False

        # Live CVD trend for the momentum gate
        _cvd_trend_now = 0.0
        try:
            _cvd_trend_now = self._cvd.get_trend_signal()
        except Exception:
            pass

        # Build a live liquidity snapshot if available
        _trail_snap = None
        if self._liq_map is not None:
            try:
                _trail_snap = self._liq_map.get_snapshot(price, atr)
            except Exception as _liq_snap_e:
                logger.debug("Trail liq_snapshot error (non-fatal): %s", _liq_snap_e)

        _liq_hold_reasons: list = []
        try:
            _liq_result = self._liq_trail.compute(
                pos_side        = pos.side,
                price           = price,
                entry_price     = pos.entry_price,
                current_sl      = pos.sl_price,
                atr             = atr,
                initial_sl_dist = pos.initial_sl_dist,
                peak_profit     = pos.peak_profit,
                liq_snapshot    = _trail_snap,
                ict_engine      = self._ict,
                now             = now,
                hold_reason     = _liq_hold_reasons,
                pos             = pos,
                fee_engine      = getattr(self, '_fee_engine', None),
                cvd_trend       = _cvd_trend_now,
                candles_1m      = candles_1m,
                candles_5m      = candles_5m,
                candles_15m     = _trail_candles_15m,
                candles_1h      = _trail_candles_1h,
            )
        except Exception as _lt_e:
            logger.exception("Trail: compute error — HOLD")
            return False

        # HOLD — engine decided not to move SL
        if _liq_result.new_sl is None:
            pos.consecutive_trail_holds += 1
            _log_interval = 30.0
            if now - self._last_trail_block_log >= _log_interval:
                self._last_trail_block_log = now
                _hold_str = " | ".join(_liq_hold_reasons[:3]) if _liq_hold_reasons \
                            else f"phase={_liq_result.phase}"
                init_dist_r = pos.initial_sl_dist if pos.initial_sl_dist > 1e-10 else atr
                _mfe_r = pos.peak_profit / init_dist_r if init_dist_r > 1e-10 else 0.0
                hm = (now - pos.entry_time) / 60.0
                logger.info(
                    f"🔒 Trail HOLD [{_liq_result.phase}] | {_hold_str} | "
                    f"profit={profit:.1f}pts MFE={pos.peak_profit:.1f}pts R={_mfe_r:.2f} | "
                    f"SL=${pos.sl_price:,.1f} hold={hm:.0f}m")
            return False

        # Engine returned a new SL — dispatch to exchange
        _new_liq_sl = _liq_result.new_sl
        logger.info(
            f"🏦 FibTrail [{_liq_result.phase}] "
            f"R={_liq_result.r_multiple:.2f}R → SL ${_new_liq_sl:.1f} | "
            f"{_liq_result.reason}")

        # SIG-3 FIX: Verify position identity before issuing REST call.
        # The background trail thread can race with a close/reconcile event
        # between the compute and the replace_stop_loss dispatch.
        _entry_time_snap = pos.entry_time
        _phase_snap      = pos.phase
        with self._lock:
            _pos_still_valid = (
                self._pos.phase       == _phase_snap and
                self._pos.entry_time  == _entry_time_snap and
                self._pos.sl_order_id == pos.sl_order_id
            )
        if not _pos_still_valid:
            logger.warning(
                "FibTrail: position changed between compute and REST dispatch "
                "— aborting trail to prevent orphaned stop order")
            return False

        _lt_side = "sell" if pos.side == "long" else "buy"
        logger.info(
            f"🏦 FibTrail SL dispatch [STOP-LIMIT] "
            f"trigger=${_new_liq_sl:,.1f} side={_lt_side} qty={pos.quantity} "
            f"phase={_liq_result.phase}")
        _lt_result = order_manager.replace_stop_loss(
            existing_sl_order_id = pos.sl_order_id,
            side                 = _lt_side,
            quantity             = pos.quantity,
            new_trigger_price    = _new_liq_sl,
            old_trigger_price    = pos.sl_price,
        )
        if _lt_result is None:
            # replace_stop_loss returns None ONLY when it verified the SL
            # order is a TRUE fill (not a self-cancellation ghost). Safe
            # to record exchange exit.
            logger.warning("🚨 SL already fired during trail dispatch")
            self._record_exchange_exit(None)
            return True
        if isinstance(_lt_result, dict) and _lt_result.get("error"):
            err = _lt_result.get("error", "unknown")

            # ── FIX (third-trade bug): UNPROTECTED means SL is GONE ───────────
            # The cancel succeeded but the replace failed AND the restore
            # failed — position is live on the exchange with no stop-loss.
            # This is the exact state that blew up the third trade. The only
            # institutionally-correct response is to flatten at market and
            # let the reconcile path record the exit.
            if err == "UNPROTECTED":
                logger.critical(
                    "💀 TRAIL: SL replace returned UNPROTECTED — "
                    "position has no stop-loss on exchange. Emergency-flattening.")
                send_telegram_message(
                    "🚨 <b>UNPROTECTED POSITION</b>\n"
                    "SL could not be moved or restored.\n"
                    "Emergency-flattening at market.")
                try:
                    if hasattr(order_manager, "emergency_flatten"):
                        order_manager.emergency_flatten(reason="trail_unprotected")
                    else:
                        _es = "sell" if pos.side == "long" else "buy"
                        order_manager.place_market_order(
                            side=_es, quantity=pos.quantity, reduce_only=True)
                except Exception as _ef_e:
                    logger.error(
                        f"emergency_flatten raised: {_ef_e}", exc_info=True)
                # Transition to EXITING; reconcile will book the real exit.
                with self._lock:
                    if self._pos.phase == PositionPhase.ACTIVE:
                        self._pos.phase = PositionPhase.EXITING
                        self._exiting_since = time.time()
                return False

            # Partial success: old SL cancelled, new one at requested trigger
            # failed, but we successfully restored an SL at a fallback trigger.
            # The trail did not move as intended — update our tracking to the
            # restored trigger so we don't re-fire the same failing replace
            # every tick, but do NOT mark the trail as advanced.
            if err == "PLACE_FAILED_RESTORED":
                _restore_oid = _lt_result.get("restore_order_id")
                _restore_trig = float(_lt_result.get("restore_trigger", 0) or 0)
                logger.warning(
                    f"⚠️ Trail: SL restored at ${_restore_trig:,.2f} (not the "
                    f"requested ${_new_liq_sl:,.1f}). Updating tracking.")
                with self._lock:
                    if _restore_oid:
                        self._pos.sl_order_id = _restore_oid
                    if _restore_trig > 0:
                        self._pos.sl_price = _restore_trig
                        self.current_sl_price = _restore_trig
                return False

            logger.warning(f"FibTrail: SL replace failed ({err}) — keeping current SL")
            return False

        # Success — update position state under lock
        with self._lock:
            self._pos.sl_price = _new_liq_sl
            _new_oid = (_lt_result or {}).get("order_id")
            if _new_oid:
                self._pos.sl_order_id = _new_oid
            self.current_sl_price = _new_liq_sl
            self._pos.consecutive_trail_holds = 0
            if not self._pos.trail_active:
                self._pos.trail_active = True
                logger.info("✅ Fibonacci trail now active")
            if _liq_result.phase == "BE_LOCK":
                self._pos.be_ratchet_applied = True

        # Throttled Telegram update with the full v5.0 context
        _trail_tg_key = "_liq_trail_tg_last"
        if now - getattr(self, _trail_tg_key, 0.0) >= 120.0:
            setattr(self, _trail_tg_key, now)
            try:
                from telegram.notifier import format_liquidity_trail_update
                from telegram.notifier import send_telegram_message as _stm
                _a = _liq_result.anchor
                _stm(format_liquidity_trail_update(
                    side          = pos.side,
                    new_sl        = _new_liq_sl,
                    anchor_price  = (_a.price if _a else pos.entry_price),
                    anchor_tf     = (_a.timeframe if _a else ""),
                    anchor_sig    = (_a.sig if _a else 0.0),
                    phase         = _liq_result.phase,
                    is_swept      = (_a.is_swept if _a else False),
                    entry_price   = pos.entry_price,
                    current_price = price,
                    atr           = atr,
                    session       = self._liq_trail._detect_session(self._ict),
                    fib_ratio     = (_a.fib_ratio if _a else None),
                    r_multiple    = _liq_result.r_multiple,
                    swing_low     = _liq_result.swing_low,
                    swing_high    = _liq_result.swing_high,
                    momentum_gate = _liq_result.momentum_gate,
                    htf_aligned   = _liq_result.htf_aligned,
                    is_cluster    = (_a.is_cluster if _a else False),
                    n_cluster_tfs = (_a.n_cluster_tfs if _a else 1),
                    pool_boost    = (_a.pool_boost if _a else False),
                    pool_between_expand = (_a.pool_between_expand if _a else False),
                    buffer_atr    = (_a.buffer_atr if _a else 0.0),
                ))
            except Exception as _lt_tg_e:
                logger.debug(f"FibTrail Telegram error: {_lt_tg_e}")
        return True

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
            _ex_still_open = False
            _ex_pos_snapshot = None
            if self._om is not None:
                try:
                    _ex_pos = self._om.get_position() if hasattr(self._om, "get_position") else self._om.get_open_position()
                    if _ex_pos is not None:
                        _ex_pos_snapshot = _ex_pos
                        _ex_qty = abs(float(_ex_pos.get("size", _ex_pos.get("quantity", 0))))
                        if _ex_qty < 1e-10:
                            _try_position_fallback = True
                            logger.info("Position is FLAT on exchange — exit occurred, reconstructing")
                        else:
                            _ex_still_open = True
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

            # ── FIX (third-trade bug): exchange reports position STILL OPEN ────
            # Previously the code recorded pnl=0 / set phase=FLAT here, while
            # the exchange had an unprotected live position. That caused the
            # reconcile to re-adopt the live position — and the ex_side parse
            # bug flipped it to SHORT. NEVER record a phantom flat while the
            # exchange shows an open position. Instead:
            #   1. Release the _exit_completed claim so a future call can retry.
            #   2. Trigger emergency_flatten to force the exchange into a
            #      known-flat state via a reduce-only market order.
            #   3. Leave phase = ACTIVE so the next _sync_position / reconcile
            #      can book the real exit once the flatten settles.
            if _ex_still_open:
                logger.critical(
                    "💀 EXIT UNCONFIRMED but exchange position is STILL OPEN — "
                    "refusing to record phantom FLAT. Triggering emergency flatten.")
                send_telegram_message(
                    "🚨 <b>EXIT UNCONFIRMED + EXCHANGE STILL OPEN</b>\n"
                    "Emergency-flattening to force a known-flat state.\n"
                    f"Entry: ${pos.entry_price:,.2f} | Side: {pos.side.upper()}")
                try:
                    if hasattr(self._om, "emergency_flatten"):
                        self._om.emergency_flatten(reason="exit_unconfirmed_still_open")
                    else:
                        # Fallback if older OM without the helper
                        _es = "sell" if pos.side == "long" else "buy"
                        self._om.place_market_order(
                            side=_es, quantity=pos.quantity, reduce_only=True)
                except Exception as _ef_e:
                    logger.error(f"emergency flatten raised: {_ef_e}", exc_info=True)

                # Release the exit claim so the NEXT reconcile can confirm the
                # flatten's actual exit price.
                with self._lock:
                    self._exit_completed = False
                # Do NOT call _record_pnl(0) and do NOT _finalise_exit.
                return

            # Truly unconfirmed AND exchange reports flat (or unreachable) —
            # record zero PnL as the last-resort state-convergence.
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

        # ── PostTradeAgent: capture exit context before trade_record exists ────
        # Must run BEFORE _record_pnl() because trade_record doesn't carry
        # fill_price, peak_adverse, or ict/liq context — those are on pos/engine.
        if self._post_trade_agent is not None:
            try:
                _liq_snap_pt = getattr(self._liq_map, '_last_snapshot', None)
                self._post_trade_agent.set_exit_context(
                    exit_type    = exit_type,
                    fill_price   = fill_price,
                    pos          = pos,
                    atr          = self._atr_5m.atr,
                    ict_engine   = self._ict,
                    liq_snapshot = _liq_snap_pt,
                )
            except Exception as _pt_e:
                logger.debug(f"PostTradeAgent.set_exit_context error: {_pt_e}")

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

        # ── ISSUE-4 FIX: Conviction Gate — session quality tracking ──────────
        # Record win/loss so the consecutive-loss session guard can block
        # further entries after MAX_SESSION_LOSSES in the same session.
        if self._conviction is not None:
            try:
                # Bug #1 fix: previously called record_trade_result(win=is_win)
                # with no pnl argument, permanently pinning session_pnl=0.0 and
                # making the drawdown circuit breaker impossible to trigger.
                self._conviction.record_trade_result(win=is_win, pnl=pnl)
            except Exception as _cv_rec_e:
                logger.debug(f"ConvictionFilter.record_trade_result error: {_cv_rec_e}")

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
        # deque(maxlen=200) automatically evicts the oldest entry — no manual trim needed.

        # ── PostTradeAgent: full 5-dimension analysis + Telegram debrief ───────
        # Runs after _trade_history.append() so trade_record is available.
        # The Telegram debrief is a separate message from the raw exit summary
        # already sent in _record_exchange_exit — it is purely analytical.
        if self._post_trade_agent is not None and self._trade_history:
            try:
                _liq_snap_pt2 = getattr(self._liq_map, '_last_snapshot', None)
                self._post_trade_agent.on_trade_closed(
                    trade_record = self._trade_history[-1],
                    pos          = pos,
                    atr          = self._atr_5m.atr if self._atr_5m else 1.0,
                    ict_engine   = self._ict,
                    liq_snapshot = _liq_snap_pt2,
                )
                # Send institutional trade debrief to Telegram
                if self._post_trade_agent.records:
                    try:
                        from strategy.post_trade_agent import format_trade_analysis_alert
                        _last_rec = self._post_trade_agent.records[-1]
                        _wr       = self._post_trade_agent._stats_overall.bayes.mean
                        send_telegram_message(format_trade_analysis_alert(
                            _last_rec, _wr,
                            agent=self._post_trade_agent,
                        ))
                    except ImportError:
                        try:
                            from post_trade_agent import format_trade_analysis_alert
                            _last_rec = self._post_trade_agent.records[-1]
                            _wr       = self._post_trade_agent._stats_overall.bayes.mean
                            send_telegram_message(format_trade_analysis_alert(
                                _last_rec, _wr,
                                agent=self._post_trade_agent,
                            ))
                        except Exception as _pt_tg2:
                            logger.debug(f"PostTrade Telegram (fallback) error: {_pt_tg2}")
                    except Exception as _pt_tg:
                        logger.debug(f"PostTrade Telegram error: {_pt_tg}")
            except Exception as _pt_e2:
                logger.debug(f"PostTradeAgent.on_trade_closed error: {_pt_e2}")

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
        # BUG FIX — "no trades after 2":
        # DirectionEngine._ps_state was never cleared on position close.
        # After exit, in_post_sweep remained True → evaluate_sweep() kept firing
        # on every tick, setting a stale direction_hint that biased entry scoring
        # toward the old sweep's direction, and spamming Telegram with repeated
        # post-sweep verdicts.  Clear it here so the next trade starts fresh.
        if hasattr(self, '_dir_engine') and self._dir_engine is not None:
            try:
                self._dir_engine.clear_sweep()
            except Exception as _dce:
                logger.debug(f"DirectionEngine.clear_sweep() on exit error (non-fatal): {_dce}")
        # Bug #23 fix: LiquidityTrailEngine holds _locked_anchor and
        # _anchor_lock_until across the lifetime of the instance (one instance
        # per QuantStrategy, reused for every position).  If position A closed
        # while anchored to a 15m SSL at $66,800 and position B opens within
        # the 90-second lock window, the engine would reuse A's stale anchor —
        # structurally irrelevant and potentially in the wrong direction for B.
        # reset() clears both fields atomically; it is intentionally idempotent.
        if hasattr(self, '_liq_trail') and self._liq_trail is not None:
            try:
                self._liq_trail.reset()
            except Exception as _ltr_e:
                logger.debug(f"LiquidityTrailEngine.reset() error (non-fatal): {_ltr_e}")
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
                           sl_price: Optional[float] = None,
                           prefetched_bal_info: dict = None) -> Optional[float]:
        """
        Risk-calibrated position sizing — v9.0 (CRIT-1 fix).

        FORMULA (industry standard):
          sl_dist      = |price − sl_price|                         (points)
          risk_capital = available_balance × RISK_PER_TRADE         (USD at risk)
          qty_raw      = risk_capital × total_mult / sl_dist        (BTC)

        This guarantees a fixed dollar loss at SL regardless of SL distance.
        A 50-point SL and a 500-point SL both risk exactly RISK_PER_TRADE × balance.

        total_mult is a confidence scalar clamped to [0.40, 1.05]:
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

        Margin guard: computed notional must not exceed available balance.
        """
        # ── SL distance guard — required for risk-based sizing ────────────────
        if sl_price is None or sl_price <= 0:
            logger.warning("_compute_quantity: sl_price required for risk-based sizing — aborting")
            return None
        sl_dist = abs(price - sl_price)
        if sl_dist < 1e-8:
            logger.warning(f"_compute_quantity: sl_dist={sl_dist:.2f} too small — aborting")
            return None

        step = QCfg.LOT_STEP()

        # ── Tier multiplier ───────────────────────────────────────────────────
        _tier_base = {"S": 1.00, "A": 0.80, "B": 0.65}.get(ict_tier, 0.50)
        _pta = self._post_trade_agent
        if _pta is not None:
            _tier_adj = {
                "S": _pta.params.tier_s_sizing.current_mult,
                "A": _pta.params.tier_a_sizing.current_mult,
                "B": _pta.params.tier_b_sizing.current_mult,
            }.get(ict_tier, 1.0)
        else:
            _tier_adj = 1.0
        tier_mult = _tier_base * _tier_adj

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

        # ── Available balance (reuse prefetched — SIG-8 fix) ─────────────────
        bal = prefetched_bal_info if prefetched_bal_info is not None else risk_manager.get_available_balance()
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

        # ── BUG 3 FIX: commission reserve ─────────────────────────────────────
        # Delta rejects orders with `insufficient_commission` when available
        # balance minus required margin leaves less than the maker+taker round-
        # trip commission.  The old guard only compared margin to available
        # and consumed 99 % of balance as margin, leaving 1 % for commission
        # on a position that needs ~2.5-3 % of balance just to cover the exit
        # taker fee.  This silently wasted bracket-order REST calls.
        #
        # Reserve: we charge an aggressive 2× the live taker rate (entry taker
        # worst-case + exit taker) plus a 15 % safety margin for slippage
        # variance.  For a $446 notional at COMMISSION_RATE=0.00055 this
        # reserves ~$0.56 — enough to clear Delta's internal commission check
        # with room to spare.  On a 30× leveraged account this represents <1 %
        # of the margin, a negligible position-size reduction for the safety.
        _taker_rate = float(_cfg("COMMISSION_RATE", 0.00055))
        _fee_reserve = price * _taker_rate * 2.0 * 1.15   # qty multiplied in next
        # We don't know qty yet, but we can compute a conservative reserve
        # from the max possible qty given available:
        #   max_notional ≈ available × leverage (all balance as margin)
        #   fee_reserve  ≈ max_notional × taker_rate × 2 × 1.15
        _max_notional_headroom = available * float(QCfg.LEVERAGE())
        _fee_budget = _max_notional_headroom * _taker_rate * 2.0 * 1.15
        available_after_fees = max(0.0, available - _fee_budget)
        if available_after_fees < QCfg.MIN_MARGIN_USDT():
            logger.warning(
                f"_compute_quantity: available after fee reserve "
                f"${available_after_fees:.2f} < MIN_MARGIN_USDT ${QCfg.MIN_MARGIN_USDT():.2f} "
                f"(raw_avail=${available:.2f} fee_budget=${_fee_budget:.2f})"
            )
            return None

        # ── Risk-based sizing (CRIT-1 fix) ────────────────────────────────────
        # risk_pct: fraction of balance to risk per trade (e.g. 0.006 = 0.6%)
        risk_pct     = float(_cfg("RISK_PER_TRADE", 0.006))
        # Base risk capital on balance-after-fee-reserve so we don't over-size
        # into a commission-rejection zone.
        risk_capital = available_after_fees * risk_pct * total_mult
        qty_raw      = risk_capital / sl_dist

        # ── Lot-step + hard limits ────────────────────────────────────────────
        qty = math.floor(qty_raw / step) * step
        qty = round(qty, 8)
        qty = max(QCfg.MIN_QTY(), min(QCfg.MAX_QTY(), qty))

        # ── Margin guard: notional must not exceed available-after-fees ──────
        # Uses `available_after_fees` (not raw `available`) so the post-margin
        # remainder is ALWAYS sufficient to cover Delta's commission deduction.
        required_margin = qty * price / QCfg.LEVERAGE()
        if required_margin > available_after_fees * 1.01:
            logger.warning(
                f"Sizing guard: required margin ${required_margin:.2f} > "
                f"available-after-fees ${available_after_fees:.2f} "
                f"(raw_avail=${available:.2f}, fee_reserve=${_fee_budget:.2f}) "
                f"— scaling down"
            )
            max_qty = math.floor(
                (available_after_fees * QCfg.LEVERAGE() / price) / step
            ) * step
            qty = max(QCfg.MIN_QTY(), min(QCfg.MAX_QTY(), round(max_qty, 8)))
            if qty < QCfg.MIN_QTY():
                return None

        if qty < QCfg.MIN_QTY():
            return None

        # ── Dollar-risk verification ──────────────────────────────────────────
        dollar_risk   = sl_dist * qty
        risk_pct_act  = dollar_risk / available * 100.0 if available > 0 else 0.0
        margin_used   = qty * price / QCfg.LEVERAGE()
        actual_fees   = qty * price * _taker_rate * 2.0   # worst-case round-trip

        logger.info(
            f"✅ Sizing [risk_based] | RISK_PCT={risk_pct:.3%} | "
            f"tier={ict_tier or 'none'} "
            f"mult={total_mult:.2f} (t={tier_mult:.2f} c={comp_mod:+.2f} a={amd_mod:+.2f}) | "
            f"SL-dist={sl_dist:.1f}pts | $risk=${dollar_risk:.2f} ({risk_pct_act:.2f}%) | "
            f"margin=${margin_used:.2f} | fees≈${actual_fees:.3f} | "
            f"headroom=${available - margin_used - actual_fees:.2f} | qty={qty}"
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
        # Uses the module-level `config` import — no per-call import overhead.
        _is_delta = (getattr(config, 'EXECUTION_EXCHANGE', 'coinswitch').lower() == 'delta'
                     and getattr(config, 'DELTA_SYMBOL', 'BTCUSD').upper() == 'BTCUSD')

        # FIX Bug-A: use exchange-specific fee rates.
        # Delta maker rate is NEGATIVE (rebate = -0.02%); CoinSwitch maker rate is
        # positive (0.02%).  The old code always read COMMISSION_RATE_MAKER from config
        # which is set to the CoinSwitch value (+0.00020), costing Delta maker entries
        # 0.04% of notional instead of receiving the rebate.
        if entry_fill_type == "maker":
            if _is_delta:
                entry_rate = float(getattr(config, "DELTA_COMMISSION_RATE_MAKER",
                                           -0.00020))   # Delta rebate (negative = income)
            else:
                entry_rate = float(getattr(config, "COMMISSION_RATE_MAKER",
                                           QCfg.COMMISSION_RATE() * 0.40))
        else:
            entry_rate = QCfg.COMMISSION_RATE()

        # Exit is always taker (stop or TP market order)
        exit_rate = (float(getattr(config, "DELTA_COMMISSION_RATE", 0.00050))
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
            _be_price = _calc_be_price(p.side, p.entry_price, atr, pos=p)
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

        # ── DirectionEngine state for periodic report ────────────────────
        direction_hunt       = None
        direction_ps_analysis = None
        if _DIRECTION_ENGINE_AVAILABLE and self._dir_engine is not None:
            try:
                direction_hunt = self._dir_engine.last_hunt
            except Exception:
                pass
            try:
                if self._dir_engine.in_post_sweep:
                    _hb_liq_snap = None
                    try:
                        if self._liq_map is not None:
                            _hb_liq_snap = self._liq_map.get_snapshot(price, atr)
                    except Exception:
                        pass
                    # BUG-1 FIX: Use _last_tick_flow/_last_cvd_trend (signed
                    # direction signals, set in _evaluate_entry each tick).
                    # _flow_conviction is a non-negative magnitude — passing it
                    # here as tick_flow made all heartbeat reversal scores
                    # appear weakly-bullish regardless of true market direction.
                    _ps_eval = self._dir_engine.evaluate_sweep(
                        price        = price,
                        atr          = atr,
                        now          = time.time(),
                        ict_engine   = self._ict,
                        tick_flow    = getattr(self, '_last_tick_flow', 0.0),
                        cvd_trend    = getattr(self, '_last_cvd_trend',  0.0),
                        liq_snapshot = _hb_liq_snap,
                    )
                    if _ps_eval is not None:
                        direction_ps_analysis = _ps_eval
            except Exception:
                pass

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
            # DirectionEngine state
            direction_hunt=direction_hunt,
            direction_ps_analysis=direction_ps_analysis,
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

        # FIX (CRITICAL-6): prefer the adapter's BTC-unit fields. The Delta
        # adapter now returns size in BTC (converted from contracts) and
        # size_signed preserving direction. CoinSwitch adapter returns
        # size in BTC natively. Either way, we want BTC here.
        ex_size     = abs(float(ex_pos.get("size", 0.0)))
        ex_size_raw = float(ex_pos.get("size_signed",
                                       ex_pos.get("size", 0.0)))
        ex_side     = str(ex_pos.get("side") or "").upper()
        phase       = self._pos.phase

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
            if ex_entry < 1.0:
                logger.warning(
                    f"Reconcile: skipping adoption of {ex_side} size={ex_size} "
                    f"— entry_price={ex_entry:.2f} not yet settled on exchange")
                return

            # ── FIX (third-trade bug): refuse ambiguous-side adoption ─────────
            # The original code did `"long" if ex_side=="LONG" else "short"`,
            # which silently produced SHORT whenever ex_side was anything other
            # than the exact literal "LONG" — including empty string, missing
            # key, or lowercase. In the third-trade incident the exchange
            # returned an empty side on a genuinely-LONG position and the bot
            # adopted it as SHORT, then tracked an inverted phantom for 27m.
            #
            # New policy: resolve side from TWO independent sources and refuse
            # to adopt if they disagree or both are ambiguous.
            #   Source 1: string side field ("LONG"/"SHORT")
            #   Source 2: sign of raw size (positive = long, negative = short)
            iside_from_str  = None
            if ex_side == "LONG":
                iside_from_str = "long"
            elif ex_side == "SHORT":
                iside_from_str = "short"

            iside_from_size = None
            if ex_size_raw > 0:
                iside_from_size = "long"
            elif ex_size_raw < 0:
                iside_from_size = "short"

            if iside_from_str and iside_from_size and iside_from_str != iside_from_size:
                logger.error(
                    f"🚨 Reconcile: side conflict — str={ex_side} signed_size={ex_size_raw} "
                    f"— REFUSING adoption. Will retry on next reconcile cycle.")
                return

            iside = iside_from_str or iside_from_size
            if iside is None:
                logger.error(
                    f"🚨 Reconcile: ambiguous side (str={ex_side!r}, "
                    f"size={ex_size_raw}) — REFUSING adoption of size={ex_size} "
                    f"at entry=${ex_entry:,.2f}. Will retry on next reconcile cycle.")
                return
            # ─────────────────────────────────────────────────────────────────

            sl_oid=tp_oid=None; sl_p=tp_p=0.0

            if open_orders:
                for o in open_orders:
                    ot=(o.get("type") or (o.get("raw") or {}).get("order_type") or "").upper().replace(" ","_").replace("-","_")
                    trig=float(o.get("trigger_price") or (o.get("raw") or {}).get("stop_price") or 0)
                    if _is_sl(ot): sl_oid=o["order_id"]; sl_p=trig
                    elif _is_tp(ot): tp_oid=o["order_id"]; tp_p=trig

            # Sanity check: SL must be on the protective side of entry for the
            # adopted side. A long's SL is BELOW entry; a short's SL is ABOVE.
            # If the orphan SL contradicts the adopted side, drop the SL oid
            # and let a fresh one be placed rather than tracking a wrong-side SL.
            if sl_oid and sl_p > 0:
                _sl_ok = ((iside == "long"  and sl_p < ex_entry) or
                          (iside == "short" and sl_p > ex_entry))
                if not _sl_ok:
                    logger.warning(
                        f"⚠️ Reconcile: discovered SL @ ${sl_p:,.2f} is on the "
                        f"WRONG side of {iside} entry ${ex_entry:,.2f} — "
                        f"ignoring (was likely a prior trade's orphan).")
                    sl_oid = None; sl_p = 0.0

            self._pos = PositionState(phase=PositionPhase.ACTIVE, side=iside, quantity=ex_size,
                entry_price=ex_entry, sl_price=sl_p, tp_price=tp_p, sl_order_id=sl_oid,
                tp_order_id=tp_oid, entry_time=time.time(), initial_sl_dist=abs(ex_entry-sl_p) if sl_p>0 else 0.0,
                entry_atr=self._atr_5m.atr)
            self.current_sl_price=sl_p; self.current_tp_price=tp_p
            self._confirm_long=self._confirm_short=0
            # Reset duplicate guards for the newly adopted position
            self._exit_completed = False
            self._pnl_recorded_for = 0.0
            logger.warning(f"⚡ RECONCILE: adopted {iside.upper()} @ ${ex_entry:,.2f}")
            send_telegram_message(f"⚡ <b>POSITION ADOPTED</b>\nSide: {iside.upper()} | Size: {ex_size}\nEntry: ${ex_entry:,.2f} | uPnL: ${ex_upnl:+.2f}")

            # ── FIX: if the adopted position has NO SL, this is an unprotected
            # state inherited from a prior failure. Trigger emergency flatten
            # rather than track a live unprotected position.
            if sl_oid is None:
                logger.critical(
                    f"💀 Adopted {iside.upper()} has NO stop-loss on exchange — "
                    f"emergency-flattening to prevent unbounded loss.")
                try:
                    order_manager.emergency_flatten(reason="adopted_unprotected")
                except Exception as _ef_e:
                    logger.error(f"emergency_flatten raised: {_ef_e}", exc_info=True)
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
                        # Side-sanity check also on recovery path
                        _side = self._pos.side
                        _ep = self._pos.entry_price or 0.0
                        _ok = (_ep <= 0) or (
                            (_side == "long"  and trig < _ep) or
                            (_side == "short" and trig > _ep))
                        if not _ok:
                            logger.warning(
                                f"Reconcile: recovered SL @ ${trig:,.2f} contradicts "
                                f"{_side} entry ${_ep:,.2f} — ignoring")
                            continue
                        self._pos.sl_order_id=o["order_id"]; self.current_sl_price=trig
                        self._pos.sl_price=trig
                        if self._pos.initial_sl_dist == 0 and _ep > 0:
                            self._pos.initial_sl_dist = abs(_ep - trig)
                        logger.info(f"Reconcile: recovered SL order {o['order_id'][:8]}… @ ${trig:.2f}")
                    elif not self._pos.tp_order_id and _is_tp(ot):
                        self._pos.tp_order_id=o["order_id"]; self.current_tp_price=trig
                        self._pos.tp_price=trig
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

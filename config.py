"""
config.py — Single source of truth for all bot parameters.
============================================================
Quant Bot v3 — Institutional Multi-Factor Momentum + Order Flow
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────
# 1. CREDENTIALS
# ─────────────────────────────────────────────
COINSWITCH_API_KEY    = os.getenv("COINSWITCH_API_KEY")
COINSWITCH_SECRET_KEY = os.getenv("COINSWITCH_SECRET_KEY")
TELEGRAM_BOT_TOKEN    = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID      = os.getenv("TELEGRAM_CHAT_ID")

if not COINSWITCH_API_KEY or not COINSWITCH_SECRET_KEY:
    raise ValueError("Missing API credentials in .env")

# ─────────────────────────────────────────────
# 2. EXCHANGE / SYMBOL
# ─────────────────────────────────────────────
SYMBOL   = "BTCUSDT"
EXCHANGE = "EXCHANGE_2"
LEVERAGE = 30

# ─────────────────────────────────────────────
# 3. POSITION SIZING
# ─────────────────────────────────────────────
BALANCE_USAGE_PERCENTAGE = 60
MIN_MARGIN_PER_TRADE     = 4
MAX_MARGIN_PER_TRADE     = 10_000
MIN_POSITION_SIZE        = 0.001
MAX_POSITION_SIZE        = 1.0
LOT_STEP_SIZE            = 0.001
REMAINDER_MIN_QTY        = 0.001

# ─────────────────────────────────────────────
# 4. RISK MANAGEMENT
# ─────────────────────────────────────────────
RISK_PER_TRADE          = 0.60
MAX_DAILY_LOSS          = 400
MAX_DAILY_LOSS_PCT      = 5.0
MAX_DRAWDOWN_PCT        = 15.0
MAX_CONSECUTIVE_LOSSES  = 4
MAX_DAILY_TRADES        = 14
ONE_POSITION_AT_A_TIME  = True
MIN_TIME_BETWEEN_TRADES = 5
TRADE_COOLDOWN_SECONDS  = 600
MIN_RISK_REWARD_RATIO   = 1.5
TARGET_RISK_REWARD_RATIO= 2.5
MAX_RR_RATIO            = 12.0

# ─────────────────────────────────────────────
# 5. ORDER EXECUTION
# ─────────────────────────────────────────────
TICK_SIZE                = 0.1
LIMIT_ORDER_OFFSET_TICKS = 5
ORDER_TIMEOUT_SECONDS    = 600
MAX_ORDER_RETRIES        = 2
MAX_CONSECUTIVE_TIMEOUTS = 2
TIMEOUT_EXTENDED_LOCKOUT_SEC = 1800
SNIPER_MAX_DISTANCE_ATR  = 1.0

# ─────────────────────────────────────────────
# 6. DATA / READINESS
# ─────────────────────────────────────────────
READY_TIMEOUT_SEC    = 120.0
MIN_CANDLES_1M       = 100
MIN_CANDLES_5M       = 100
MIN_CANDLES_15M      = 100
MIN_CANDLES_1H       = 20
MIN_CANDLES_4H       = 40
MIN_CANDLES_1D       = 7
LOOKBACK_CANDLES_1M  = 100
LOOKBACK_CANDLES_5M  = 100
LOOKBACK_CANDLES_15M = 100
LOOKBACK_CANDLES_4H  = 50
CANDLE_TIMEFRAMES    = ["1m", "5m", "15m", "4h"]
PRIMARY_TIMEFRAME    = "5m"
HTF_TIMEFRAME        = "4h"

# ─────────────────────────────────────────────
# 7. HEALTH / SUPERVISOR
# ─────────────────────────────────────────────
WS_STALE_SECONDS                   = 35.0
HEALTH_CHECK_INTERVAL_SEC          = 12.0
PRICE_STALE_SECONDS                = 90.0
BALANCE_CACHE_TTL_SEC              = 35.0
STRUCTURE_UPDATE_INTERVAL_SECONDS  = 30
ENTRY_EVALUATION_INTERVAL_SECONDS  = 1
ENTRY_PENDING_TIMEOUT_SECONDS      = ORDER_TIMEOUT_SECONDS

# ─────────────────────────────────────────────
# 8. LOGGING / REPORTING
# ─────────────────────────────────────────────
LOG_LEVEL                    = "INFO"
TELEGRAM_REPORT_INTERVAL_SEC = 900
OUTLOOK_INTERVAL_SECONDS     = 900

# ─────────────────────────────────────────────
# 9. FEES
# ─────────────────────────────────────────────
COMMISSION_RATE = 0.00055

# ─────────────────────────────────────────────
# RATE LIMITING
# ─────────────────────────────────────────────
GLOBAL_API_MIN_INTERVAL  = 3.0
RATE_LIMIT_ORDERS        = 15
REQUEST_TIMEOUT          = 30

# ─────────────────────────────────────────────
# SL INFRASTRUCTURE (shared with order_manager.py)
# ─────────────────────────────────────────────
SL_BUFFER_TICKS              = 5
MIN_SL_DISTANCE_PCT          = 0.003
MAX_SL_DISTANCE_PCT          = 0.035
SL_MIN_IMPROVEMENT_PCT       = 0.001
SL_RATCHET_ONLY              = True
SL_ATR_PERIOD                = 14
SL_ATR_BUFFER_MULT           = 0.75
SL_MIN_CLEARANCE_ATR_MULT    = 1.5
SL_MIN_IMPROVEMENT_ATR_MULT  = 0.08
TRAILING_SL_CHECK_INTERVAL   = 15
TRAIL_SWING_MAX_AGE_MS       = 14_400_000

# ═══════════════════════════════════════════════════════════════════
# 10. QUANT STRATEGY PARAMETERS v3 — INSTITUTIONAL GRADE
#     All read live by QCfg static methods — no restart needed.
# ═══════════════════════════════════════════════════════════════════

# 10a. Sizing
QUANT_MARGIN_PCT            = 0.20
QUANT_SLIPPAGE_TOLERANCE    = 0.0005

# 10b. Signal Thresholds — ADAPTIVE (base values, dynamic logic adjusts)
QUANT_LONG_THRESHOLD        = 0.40
QUANT_SHORT_THRESHOLD       = 0.40
QUANT_EXIT_FLIP             = 0.22
QUANT_CONFIRM_TICKS         = 1

# 10c. ATR / SL / TP
QUANT_SL_ATR_MULT           = 1.4
QUANT_TP_ATR_MULT           = 2.5

# 10d. Trailing SL — aggressive
QUANT_TRAIL_ENABLED         = True
QUANT_TRAIL_ACTIVATE_R      = 0.5
QUANT_TRAIL_ATR_MULT        = 0.9

# 10e. Indicator Windows
QUANT_CVD_WINDOW            = 20
QUANT_CVD_HIST_MULT         = 15
QUANT_VWAP_WINDOW           = 50
QUANT_VWAP_SLOPE_BARS       = 8
QUANT_EMA_FAST              = 8
QUANT_EMA_SLOW              = 21
QUANT_EMA_SIGNAL_BARS       = 5
QUANT_BB_WINDOW             = 20
QUANT_BB_STD                = 2.0
QUANT_KC_ATR_MULT           = 1.5
QUANT_SQUEEZE_BREAKOUT_BARS = 8
QUANT_VOL_FLOW_WINDOW       = 10

# 10g. Regime Filter — WIDE GATE (let the signal quality decide)
QUANT_ATR_PCTILE_WINDOW     = 100
QUANT_ATR_MIN_PCTILE        = 0.05
QUANT_ATR_MAX_PCTILE        = 0.97

# 10h. Timing — FAST for quant-style execution
QUANT_MAX_HOLD_SEC          = 2400
QUANT_COOLDOWN_SEC          = 20
QUANT_POS_SYNC_SEC          = 30

# 10j. Signal Weights — must sum to 1.0
#      When a signal returns 0.0 (inactive), its weight is
#      dynamically redistributed to active signals.
QUANT_W_CVD                 = 0.22
QUANT_W_VWAP                = 0.18
QUANT_W_MOM                 = 0.22
QUANT_W_SQUEEZE             = 0.06
QUANT_W_VOL                 = 0.06
QUANT_W_ORDERBOOK           = 0.14
QUANT_W_TICK_FLOW           = 0.12
# Sum = 1.00

# 10k. Multi-Timeframe Trend Filter
QUANT_HTF_ENABLED           = True
QUANT_HTF_VETO_STRENGTH     = 0.65
QUANT_HTF_BOOST             = 0.12

# 10l. Adaptive Threshold
QUANT_AGREEMENT_DISCOUNT    = 0.07
QUANT_MIN_AGREE_SIGNALS     = 3
QUANT_STRONG_SIGNAL_LEVEL   = 0.35

# 10m. Orderbook Imbalance
QUANT_OB_DEPTH_LEVELS       = 5
QUANT_OB_HIST_LEN           = 60

# 10n. Tick Flow Aggregation
QUANT_TICK_AGG_WINDOW_SEC   = 30.0
QUANT_TICK_SURGE_MULT       = 2.5

# 10o. Momentum Divergence
QUANT_PRICE_MOM_LOOKBACK    = 12
QUANT_DIVERGENCE_THRESHOLD  = 0.3

# ─────────────────────────────────────────────────────────────────
# TUNING GUIDE (all live — no restart needed):
#   More selective entries  → QUANT_LONG_THRESHOLD  = 0.55
#   Tighter SL              → QUANT_SL_ATR_MULT     = 1.0
#   Swing mode              → QUANT_MAX_HOLD_SEC     = 3600
#   More order-flow weight  → QUANT_W_CVD            = 0.35
#   Require confirmation    → QUANT_CONFIRM_TICKS    = 2
#   Wider regime gate       → QUANT_ATR_MIN_PCTILE   = 0.03
#   Lower threshold when    → QUANT_AGREEMENT_DISCOUNT = 0.10
#     multiple signals align
# ─────────────────────────────────────────────────────────────────

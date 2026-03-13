"""
config.py — Single source of truth for all bot parameters.
============================================================
Quant Bot — Institutional Multi-Factor Momentum + Order Flow
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
MAX_DAILY_LOSS_PCT      = 10.0
MAX_DRAWDOWN_PCT        = 30.0
MAX_CONSECUTIVE_LOSSES  = 3
MAX_DAILY_TRADES        = 30
ONE_POSITION_AT_A_TIME  = True
MIN_TIME_BETWEEN_TRADES = 10
TRADE_COOLDOWN_SECONDS  = 600
MIN_RISK_REWARD_RATIO   = 1.5
TARGET_RISK_REWARD_RATIO= 3.0
MAX_RR_RATIO            = 50.0

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
ENTRY_EVALUATION_INTERVAL_SECONDS  = 5
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
MIN_SL_DISTANCE_PCT          = 0.004
MAX_SL_DISTANCE_PCT          = 0.03
SL_MIN_IMPROVEMENT_PCT       = 0.001
SL_RATCHET_ONLY              = True
SL_ATR_PERIOD                = 14
SL_ATR_BUFFER_MULT           = 0.75
SL_MIN_CLEARANCE_ATR_MULT    = 1.5
SL_MIN_IMPROVEMENT_ATR_MULT  = 0.1
TRAILING_SL_CHECK_INTERVAL   = 30
TRAIL_SWING_MAX_AGE_MS       = 14_400_000

# ═══════════════════════════════════════════════════════════════════
# 10. QUANT STRATEGY PARAMETERS
#     All read live by QCfg static methods — no restart needed.
# ═══════════════════════════════════════════════════════════════════

# 10a. Sizing
QUANT_MARGIN_PCT            = 0.20
QUANT_SLIPPAGE_TOLERANCE    = 0.0005

# 10b. Signal Thresholds
QUANT_LONG_THRESHOLD        = 0.55
QUANT_SHORT_THRESHOLD       = 0.55
QUANT_EXIT_FLIP             = 0.30
QUANT_CONFIRM_TICKS         = 2

# 10c. ATR / SL / TP
QUANT_SL_ATR_MULT           = 1.5
QUANT_TP_ATR_MULT           = 2.5

# 10d. Trailing SL
QUANT_TRAIL_ENABLED         = True
QUANT_TRAIL_ACTIVATE_R      = 1.0
QUANT_TRAIL_ATR_MULT        = 1.0

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
QUANT_SQUEEZE_BREAKOUT_BARS = 5
QUANT_VOL_FLOW_WINDOW       = 10

# 10f. Minimum Data (MIN_CANDLES_1M / 5M shared above, >= required values)

# 10g. Regime Filter
QUANT_ATR_PCTILE_WINDOW     = 100
QUANT_ATR_MIN_PCTILE        = 0.15
QUANT_ATR_MAX_PCTILE        = 0.90

# 10h. Timing
QUANT_MAX_HOLD_SEC          = 1800
QUANT_COOLDOWN_SEC          = 60
QUANT_POS_SYNC_SEC          = 30

# 10i. Risk Limits (MAX_DAILY_TRADES/LOSSES/LOSS_PCT shared above)

# 10j. Signal Weights — must sum to 1.0
QUANT_W_CVD                 = 0.30
QUANT_W_VWAP                = 0.25
QUANT_W_MOM                 = 0.25
QUANT_W_SQUEEZE             = 0.10
QUANT_W_VOL                 = 0.10
# Sum = 1.00

# ─────────────────────────────────────────────────────────────────
# TUNING GUIDE (all live — no restart needed):
#   More selective entries  → QUANT_LONG_THRESHOLD  = 0.65
#   Tighter SL              → QUANT_SL_ATR_MULT     = 1.0
#   Swing mode              → QUANT_MAX_HOLD_SEC     = 3600
#   More order-flow weight  → QUANT_W_CVD            = 0.40
#   Require confirmation    → QUANT_CONFIRM_TICKS    = 3
# ─────────────────────────────────────────────────────────────────

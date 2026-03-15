"""
config.py — Single source of truth for all bot parameters.
============================================================
Quant Bot v4.5 — Institutional Multi-Factor Momentum + Order Flow

CHANGES from v4.4:
  - COMPLETE TRAIL REWRITE: v4.4 time-triggered BE moved SL into noise → whipsaw loss
  - Removed TIME_BE_SECONDS / TIME_TRAIL_SECONDS (time-based BE is a retail concept)
  - Removed "breakeven" concept entirely (entry price is irrelevant to market)
  - TRAIL_BE_R raised to 1.0R (trade must PROVE itself before ANY SL movement)
  - TRAIL_LOCK_R raised to 2.0R, added TRAIL_AGGRESSIVE_R=3.0R
  - Added per-phase minimum distance: 1.5/1.0/0.7 × ATR (kills whipsaw)
  - Added 6-factor pullback-vs-reversal classifier (freezes SL during pullbacks)
  - SL only moves behind CONFIRMED 5m swing structure, not arbitrary prices
  - Smart max-hold ATR widened to 0.5 (was 0.3)

CHANGES from v4.3:
  - Added QUANT_REVERSION_MIN_RR=1.5 / QUANT_REVERSION_MAX_RR=3.0 (mode-aware R:R)
  - Added QUANT_TREND_MIN_RR=3.0 / QUANT_TREND_MAX_RR=5.0 (trend keeps high R:R)
  - Added QUANT_SMART_MAX_HOLD=True (tighten SL instead of market-dumping profitable trades)

CHANGES from v4:
  - ATR_SEED_RETAIN set to 1 (CRITICAL FIX: warmup data was poisoning percentile → 0% → blocked all trades)
  - QUANT_TRAIL_BE_R raised to 1.0 (Trail fix: was 0.4 — too aggressive)
  - QUANT_TRAIL_LOCK_R raised to 1.5 (Trail fix: was 0.8)
  - QUANT_TRAIL_CHANDELIER_N_START raised to 3.0 (wider breathing room)
  - Added QUANT_MAX_SPREAD_ATR_RATIO (Solution 5: time-of-day filter)
  - Added QUANT_TP_VWAP_FRACTION bumped to 0.65 (wider TP for fee coverage)
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
LEVERAGE = 40

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
MAX_CONSECUTIVE_LOSSES  = 3
MAX_DAILY_TRADES        = 8
ONE_POSITION_AT_A_TIME  = True
MIN_TIME_BETWEEN_TRADES = 5
TRADE_COOLDOWN_SECONDS  = 600
MIN_RISK_REWARD_RATIO   = 3.0
TARGET_RISK_REWARD_RATIO= 4.0
MAX_RR_RATIO            = 20.0

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
COMMISSION_RATE       = 0.00055
COMMISSION_RATE_MAKER = COMMISSION_RATE * 0.40

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
# 10. QUANT STRATEGY PARAMETERS v4.3
# ═══════════════════════════════════════════════════════════════════

# 10a. Sizing
QUANT_MARGIN_PCT            = 0.20
QUANT_SLIPPAGE_TOLERANCE    = 0.0005

# 10b. Entry — MEAN-REVERSION (hard confluence gates)
QUANT_VWAP_ENTRY_ATR_MULT   = 1.2
QUANT_CVD_DIVERGENCE_MIN    = 0.15
QUANT_OB_CONFIRM_MIN        = 0.10
QUANT_COMPOSITE_ENTRY_MIN   = 0.30
QUANT_EXIT_REVERSAL_THRESH  = 0.40
QUANT_CONFIRM_TICKS         = 2

# 10c. SL/TP — INSTITUTIONAL LEVEL PLACEMENT
QUANT_SL_SWING_LOOKBACK     = 12
QUANT_SL_BUFFER_ATR_MULT    = 0.4
QUANT_TP_VWAP_FRACTION      = 0.65     # v4.3: was 0.50 — wider TP for fee coverage
QUANT_VP_BUCKET_COUNT       = 50
QUANT_VP_HVN_THRESHOLD      = 0.70
QUANT_OB_WALL_DEPTH         = 20
QUANT_OB_WALL_MULT          = 2.5
QUANT_TRAIL_SWING_BARS      = 5
QUANT_TRAIL_VOL_DECAY_MULT  = 0.6

# 10d. Trailing SL — v4.5 INSTITUTIONAL REWRITE
#
#      v4.4 FAILURE: time-triggered BE moved SL to $71,723 (0.43×ATR from price).
#      Normal $34 pullback clipped stop → turned $77 winner into -$0.58 loss.
#      "Breakeven" concept is flawed: your entry price is irrelevant to the market.
#      SL must live behind STRUCTURE, not anchored to entry.
#
#      INSTITUTIONAL PRINCIPLES:
#        1. No movement until trade PROVES itself (1.0R minimum).
#        2. SL only behind confirmed 5m swing structure.
#        3. Pullback classifier prevents tightening during healthy retracements.
#        4. Minimum distance = 1.5×ATR (Phase 1) → noise can never clip you.
#
QUANT_TRAIL_ENABLED            = True
QUANT_TRAIL_BE_R               = 1.0   # Phase 0→1 at 1.0R (trade must PROVE itself)
QUANT_TRAIL_LOCK_R             = 2.0   # Phase 1→2 at 2.0R (chandelier engages)
QUANT_TRAIL_AGGRESSIVE_R       = 3.0   # Phase 2→3 at 3.0R (full mechanisms)
QUANT_TRAIL_MIN_DIST_ATR_P1    = 1.5   # Phase 1: min SL distance = 1.5×ATR
QUANT_TRAIL_MIN_DIST_ATR_P2    = 1.0   # Phase 2: 1.0×ATR
QUANT_TRAIL_MIN_DIST_ATR_P3    = 0.7   # Phase 3: 0.7×ATR
QUANT_TRAIL_PULLBACK_FREEZE    = True  # Freeze SL during healthy pullbacks
QUANT_TRAIL_PB_VOL_RATIO       = 0.60  # Pullback vol < 60% of impulse → healthy
QUANT_TRAIL_PB_DEPTH_ATR       = 0.80  # Pullback < 0.8×ATR → healthy
QUANT_TRAIL_REV_MIN_SIGNALS    = 3     # Need ≥3 of 6 reversal signals to tighten

# 10e. Indicator Windows
QUANT_CVD_WINDOW            = 20
QUANT_CVD_HIST_MULT         = 15
QUANT_VWAP_WINDOW           = 50
QUANT_EMA_FAST              = 8
QUANT_EMA_SLOW              = 21
QUANT_VOL_FLOW_WINDOW       = 10

# 10f. Regime Filter
QUANT_ATR_PCTILE_WINDOW     = 100
QUANT_ATR_MIN_PCTILE        = 0.05
QUANT_ATR_MAX_PCTILE        = 0.97

# 10g. Timing — PATIENT
QUANT_MAX_HOLD_SEC          = 2400
QUANT_COOLDOWN_SEC          = 180
QUANT_LOSS_LOCKOUT_SEC      = 3600
QUANT_POS_SYNC_SEC          = 30

# 10h. Signal Weights (sum = 1.0)
QUANT_W_VWAP_DEV            = 0.30
QUANT_W_CVD_DIV             = 0.25
QUANT_W_OB                  = 0.20
QUANT_W_TICK_FLOW           = 0.15
QUANT_W_VOL_EXHAUSTION      = 0.10

# 10i. HTF Filter — VETO ONLY
QUANT_HTF_ENABLED           = True
QUANT_HTF_VETO_STRENGTH     = 0.70

# 10j. Orderbook / Tick
QUANT_OB_DEPTH_LEVELS       = 5
QUANT_OB_HIST_LEN           = 60
QUANT_TICK_AGG_WINDOW_SEC   = 30.0

# 10k. Institutional SL/TP/Trail — v4.3
QUANT_TP_MAX_RR                = 3.5
QUANT_SL_SWING_DENSITY_WINDOW  = 0.30
QUANT_TRAIL_CHANDELIER_N_START = 3.0   # v4.3: was 2.5 — wider breathing room at start
QUANT_TRAIL_CHANDELIER_N_END   = 1.8   # v4.3: was 1.5 — still tightens but not as aggressively
QUANT_TRAIL_HVN_SNAP_THRESH    = 0.55

# 10l. Trend-following mode (v4.2)
QUANT_ADX_PERIOD               = 14
QUANT_ADX_TREND_THRESH         = 25.0
QUANT_ADX_RANGE_THRESH         = 20.0
QUANT_ATR_EXPANSION_THRESH     = 1.30
QUANT_TREND_PULLBACK_ATR_MIN   = 0.10
QUANT_TREND_PULLBACK_ATR_MAX   = 2.00
QUANT_TREND_CVD_MIN            = -0.20
QUANT_TREND_TP_ATR_MULT        = 2.5
QUANT_TREND_COMPOSITE_MIN      = 0.35
QUANT_TREND_CONFIRM_TICKS      = 3
QUANT_TREND_CHANDELIER_N       = 1.5

# 10m. Spread/ATR gate — v4.3 (Solution 5)
QUANT_MAX_SPREAD_ATR_RATIO     = 0.08  # Skip entries when spread > 8% of ATR

# 10n. Mode-aware R:R — v4.4 FIX
#      v4.3 used MIN_RISK_REWARD_RATIO=3.0 for ALL trades. For mean-reversion
#      this pushed TP to 3x SL distance — far beyond the natural VWAP target.
#      Result: TP was unreachable, trade timed out, profit evaporated.
#      Fix: separate R:R bounds per mode. Reversion edge = win rate (60-70%),
#      not R:R. Trend edge = letting winners run, so higher R:R.
QUANT_REVERSION_MIN_RR         = 1.5   # Reversion: closer TP, win-rate driven
QUANT_REVERSION_MAX_RR         = 3.0   # Cap reversion TP distance
QUANT_TREND_MIN_RR             = 3.0   # Trend: let winners run
QUANT_TREND_MAX_RR             = 5.0   # Cap trend TP distance

# 10o. Smart max-hold exit — v4.4 FIX
QUANT_SMART_MAX_HOLD           = True  # Tighten SL instead of market-dumping profitable trades
QUANT_MAX_HOLD_PROFIT_SL_ATR   = 0.5   # v4.5: 0.5×ATR from price (was 0.3 — too tight)

# ═══════════════════════════════════════════════════════════════════
# 11. FEE ENGINE
# ═══════════════════════════════════════════════════════════════════

# 11a. SpreadTracker
FEE_SPREAD_HIST_MAXLEN      = 500
FEE_SPREAD_DEFAULT_BPS      = 2.0

# 11b. SlippageTracker
FEE_SLIP_ALPHA              = 0.25
FEE_SLIP_DEFAULT_BPS        = 1.5
FEE_SLIP_MIN_BPS            = 0.5

# 11c. ProfitFloorModel — sigmoid multiplier curve
FEE_FLOOR_MULT_LOW          = 5.5
FEE_FLOOR_MULT_HIGH         = 1.8
FEE_FLOOR_INFLECT           = 0.45
FEE_FLOOR_STEEPNESS         = 6.0
FEE_FLOOR_ABS_MIN_MULT      = 1.4

# 11d. ProfitFloorModel — spread penalty
FEE_SPREAD_ATR_WARN         = 0.06
FEE_SPREAD_PENALTY_K        = 4.0

# 11e. ProfitFloorModel — signal confidence discount
FEE_CONF_NEUTRAL            = 0.5
FEE_CONF_MAX_DISCOUNT       = 0.30

# 11f. MakerTakerDecision
FEE_MAKER_MIN_SAVING_BPS    = 0.8
FEE_MAKER_URGENCY_CUTOFF    = 0.72
FEE_MAKER_DEPTH_LEVELS      = 5
FEE_MAKER_DEPTH_MAX_FRAC    = 0.25
FEE_MAKER_DEPTH_FILL_FLOOR  = 0.2
FEE_MAKER_OPP_COST_WEIGHT   = 0.5

# ═══════════════════════════════════════════════════════════════════
# 12. ATR ENGINE REGIME TUNING
# ═══════════════════════════════════════════════════════════════════
ATR_SEED_RETAIN             = 1      # v4.3: Only keep final ATR from warmup.
                                     # Old values (20, 35) poisoned percentile ranking
                                     # with stale high-vol warmup data → 0% pctile → blocked all trades.
ATR_PCTILE_RANK_WINDOW      = 30

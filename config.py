"""
config.py — Unified configuration for Dual-Exchange Quant Bot
=============================================================
Single source of truth. All exchange-specific params are namespaced.
Hot-reloadable at runtime via Telegram /set commands.
"""
import os
from dotenv import load_dotenv
load_dotenv()

# ── Exchange routing ──────────────────────────────────────────────────────────
EXECUTION_EXCHANGE = os.getenv("EXECUTION_EXCHANGE", "delta").lower()

# ── Credentials ───────────────────────────────────────────────────────────────
DELTA_API_KEY             = os.getenv("DELTA_API_KEY",    "")
DELTA_SECRET_KEY          = os.getenv("DELTA_SECRET_KEY", "")
DELTA_TESTNET             = os.getenv("DELTA_TESTNET", "false").lower() == "true"
COINSWITCH_API_KEY        = os.getenv("COINSWITCH_API_KEY",    "")
COINSWITCH_SECRET_KEY     = os.getenv("COINSWITCH_SECRET_KEY", "")
TELEGRAM_BOT_TOKEN        = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID          = os.getenv("TELEGRAM_CHAT_ID",   "")

if not DELTA_API_KEY and not COINSWITCH_API_KEY:
    raise ValueError("No exchange credentials in .env. Set DELTA_API_KEY or COINSWITCH_API_KEY.")

# ── Symbol / Leverage ─────────────────────────────────────────────────────────
SYMBOL                   = "BTCUSDT"
LEVERAGE                 = 40
DELTA_SYMBOL             = "BTCUSD"         # Delta India perpetual
DELTA_CONTRACT_VALUE_BTC = 0.001
DELTA_BALANCE_CURRENCY   = "USD"             # Delta India is USD-settled
COINSWITCH_SYMBOL        = "BTCUSDT"
COINSWITCH_EXCHANGE      = "EXCHANGE_2"

# ── Position sizing ───────────────────────────────────────────────────────────
BALANCE_USAGE_PERCENTAGE = 60
MIN_MARGIN_PER_TRADE     = 1
MAX_MARGIN_PER_TRADE     = 10_000
MIN_POSITION_SIZE        = 0.001
MAX_POSITION_SIZE        = 1.0
LOT_STEP_SIZE            = 0.001
REMAINDER_MIN_QTY        = 0.001

# ── Risk management ───────────────────────────────────────────────────────────
RISK_PER_TRADE          = 0.60
MAX_DAILY_LOSS          = 400
MAX_DAILY_LOSS_PCT      = 5.0
MAX_DRAWDOWN_PCT        = 15.0
MAX_CONSECUTIVE_LOSSES  = 5
MAX_DAILY_TRADES        = 8
ONE_POSITION_AT_A_TIME  = True
MIN_TIME_BETWEEN_TRADES = 5
TRADE_COOLDOWN_SECONDS  = 600
MIN_RISK_REWARD_RATIO   = 3.0
TARGET_RISK_REWARD_RATIO= 4.0
MAX_RR_RATIO            = 20.0

# ── Order execution ───────────────────────────────────────────────────────────
TICK_SIZE                        = 0.1
LIMIT_ORDER_OFFSET_TICKS         = 3      # fallback when book unavailable (3 × $0.1 = $0.30)
ORDER_TIMEOUT_SECONDS            = 600
MAX_ORDER_RETRIES                = 2
MAX_CONSECUTIVE_TIMEOUTS         = 2
TIMEOUT_EXTENDED_LOCKOUT_SEC     = 1800
SNIPER_MAX_DISTANCE_ATR          = 1.0
LIMIT_ORDER_FILL_TIMEOUT_SEC     = 120.0  # 120s for reversion fill; expired = cancel + cooldown
REQUEST_TIMEOUT                  = 30

# ── Data / Readiness ──────────────────────────────────────────────────────────
READY_TIMEOUT_SEC    = 120.0
MIN_CANDLES_1M       = 100
MIN_CANDLES_5M       = 100
MIN_CANDLES_15M      = 100
MIN_CANDLES_1H       = 20
MIN_CANDLES_4H       = 40
MIN_CANDLES_1D       = 7
# LOOKBACK_CANDLES_* must be >= the highest limit= argument passed to
# get_candles() in quant_strategy.py for that timeframe.  These values
# also drive _WARMUP_CONFIG in data_manager.py so both must stay in sync.
# Mismatched values (100 < 300) caused get_candles("5m", limit=300) to
# silently cap at the warmup fetch size and return only 200 candles at
# startup, starving ICT OB/FVG detection and producing count instability.
LOOKBACK_CANDLES_1M  = 150   # strategy requests up to 120; +30 headroom
LOOKBACK_CANDLES_5M  = 300   # strategy requests 300 (main ICT + trail + sweep)
LOOKBACK_CANDLES_15M = 200   # strategy requests 200
LOOKBACK_CANDLES_1H  = 100   # 1H liquidity crucial for trend/reversal levels
LOOKBACK_CANDLES_4H  = 50    # unchanged
LOOKBACK_CANDLES_1D  = 30    # 1D for macro dealing range + IPDA levels
CANDLE_TIMEFRAMES    = ["1m", "5m", "15m", "1h", "4h", "1d"]
PRIMARY_TIMEFRAME    = "15m"     # 15m is primary for SL/TP structure
ENTRY_TIMEFRAME      = "5m"      # 5m/1m for entry timing + trail micro-structure
HTF_TIMEFRAME        = "4h"

# ── Session configuration (NY-hour equivalents, UTC-based) ────────────────
# BTC session behaviour:
#   Asia (20:00-01:00 NY / 01:30-06:30 IST)  → Low vol, range-bound, stop hunts
#   London (02:00-05:00 NY / 07:30-10:30 IST) → Volatility expansion, trend start
#   NY (07:00-11:00 NY / 12:30-16:30 IST)    → Highest vol, institutional delivery
#   Late NY (11:00-16:00 NY / 16:30-21:30 IST) → Consolidation, reversal risk
SESSION_TRAIL_WIDTH_MULT = {
    "asia":     1.60,   # Wide trail — stop hunts in thin liquidity
    "london":   1.20,   # Moderate — trending but volatile
    "ny":       1.00,   # Standard — highest conviction moves
    "late_ny":  1.30,   # Wider — reversals and consolidation
    "off":      1.50,   # Wide — low conviction
}
SESSION_ENTRY_QUALITY = {
    "asia":     "LOW",      # Avoid entries unless Tier-S sweep setup
    "london":   "HIGH",     # Prime entry window
    "ny":       "HIGH",     # Prime entry window
    "late_ny":  "MEDIUM",   # Acceptable with strong confluence
    "off":      "LOW",      # Avoid
}

# ── Health / Supervisor ───────────────────────────────────────────────────────
WS_STALE_SECONDS                   = 35.0
HEALTH_CHECK_INTERVAL_SEC          = 12.0
PRICE_STALE_SECONDS                = 90.0
BALANCE_CACHE_TTL_SEC              = 35.0
STRUCTURE_UPDATE_INTERVAL_SECONDS  = 30
ENTRY_EVALUATION_INTERVAL_SECONDS  = 1
ENTRY_PENDING_TIMEOUT_SECONDS      = ORDER_TIMEOUT_SECONDS

# ── Logging / Reporting ───────────────────────────────────────────────────────
LOG_LEVEL                    = "INFO"
TELEGRAM_REPORT_INTERVAL_SEC = 900
OUTLOOK_INTERVAL_SECONDS     = 900

# ── Fees ──────────────────────────────────────────────────────────────────────
COMMISSION_RATE              = 0.00055
COMMISSION_RATE_MAKER        = 0.00020
DELTA_COMMISSION_RATE        = 0.00050
DELTA_COMMISSION_RATE_MAKER  = -0.00020   # rebate

# ── Rate limiting ─────────────────────────────────────────────────────────────
GLOBAL_API_MIN_INTERVAL  = 3.0
DELTA_API_MIN_INTERVAL   = 0.25
RATE_LIMIT_ORDERS        = 15

# ── SL infrastructure ─────────────────────────────────────────────────────────
# Trailing SL order type: stop-limit instead of stop-market.
# Stop-limit = triggers at SL price, executes as limit order at limit_price.
# Advantages: edit-in-place atomic (no cancel+replace cycle), maker fee rebate.
# SL_LIMIT_OFFSET_TICKS: number of ticks of limit buffer past the stop trigger.
#   SHORT SL (buy to close): limit_price = stop_price + offset (max buy price)
#   LONG  SL (sell to close): limit_price = stop_price - offset (min sell price)
# 20 ticks = $2.00 — covers normal BTC spread + slippage on structural breaks.
# Bracket entry SL is always stop-market (guaranteed crash protection).
SL_LIMIT_OFFSET_TICKS    = 20   # ticks of limit buffer on trailing stop-limit
SL_BUFFER_TICKS              = 5
MIN_SL_DISTANCE_PCT          = 0.001  # legacy pct floor; actual SL uses 1.0×ATR minimum
MAX_SL_DISTANCE_PCT          = 0.035
SL_MIN_IMPROVEMENT_PCT       = 0.001
SL_RATCHET_ONLY              = True
SL_ATR_PERIOD                = 14
SL_ATR_BUFFER_MULT           = 0.75
SL_MIN_CLEARANCE_ATR_MULT    = 1.5
SL_MIN_IMPROVEMENT_ATR_MULT  = 0.08
TRAILING_SL_CHECK_INTERVAL   = 10
TRAIL_SWING_MAX_AGE_MS       = 14_400_000

# ── Aggregator ────────────────────────────────────────────────────────────────
AGG_PRIMARY_WEIGHT   = 0.55
AGG_SECONDARY_WEIGHT = 0.45
AGG_OB_DEPTH_LEVELS  = 10
AGG_TRADE_WINDOW_SEC = 30.0

# ── Quant Strategy v4.9 ───────────────────────────────────────────────────────
QUANT_MARGIN_PCT               = 0.20
QUANT_SLIPPAGE_TOLERANCE       = 0.0005
QUANT_VWAP_ENTRY_ATR_MULT      = 1.2
QUANT_CVD_DIVERGENCE_MIN       = 0.15
QUANT_OB_CONFIRM_MIN           = 0.10
QUANT_COMPOSITE_ENTRY_MIN      = 0.35  # post-boost composite; pre-boost gate is QUANT_MIN_RAW_COMPOSITE
QUANT_EXIT_REVERSAL_THRESH     = 0.40
QUANT_CONFIRM_TICKS            = 3    # code enforces max(CONFIRM_TICKS,5) for reversion
QUANT_SL_SWING_LOOKBACK        = 12
QUANT_SL_BUFFER_ATR_MULT       = 0.4
QUANT_TP_VWAP_FRACTION         = 0.65
QUANT_VP_BUCKET_COUNT          = 50
QUANT_VP_HVN_THRESHOLD         = 0.70
QUANT_OB_WALL_DEPTH            = 20
QUANT_OB_WALL_MULT             = 2.5
QUANT_TRAIL_SWING_BARS         = 5
QUANT_TRAIL_VOL_DECAY_MULT     = 0.6
QUANT_TRAIL_ENABLED            = True
QUANT_TRAIL_BE_R               = 0.30   # BE lock starts at 0.30R (was 0.40)
QUANT_TRAIL_LOCK_R             = 0.80   # Profit lock starts at 0.80R
QUANT_TRAIL_AGGRESSIVE_R       = 1.80   # Aggressive tightening at 1.80R (was 2.00)
QUANT_TRAIL_MIN_DIST_ATR_P1    = 2.00   # Phase 1: 2.0 ATR min distance (was 1.50 — too tight)
QUANT_TRAIL_MIN_DIST_ATR_P2    = 1.50   # Phase 2: 1.5 ATR (was 1.10)
QUANT_TRAIL_MIN_DIST_ATR_P3    = 1.00   # Phase 3: 1.0 ATR (was 0.70)
QUANT_TRAIL_PULLBACK_FREEZE    = True
QUANT_TRAIL_PB_VOL_RATIO       = 0.65
QUANT_TRAIL_PB_DEPTH_ATR       = 1.20
QUANT_TRAIL_REV_MIN_SIGNALS    = 2     # was 4 — 2 reversal signals sufficient to unfreeze trail

# ── 15m-FIRST Trail Anchoring ────────────────────────────────────────────
# 15m is the PRIMARY SL anchor — wider structures survive stop hunts
# 5m/1m used for ENTRY timing and aggressive phase tightening only
QUANT_TRAIL_15M_PRIORITY       = True   # 15m swings/OBs take precedence over 5m
QUANT_TRAIL_15M_BUFFER_ATR     = 0.35   # Buffer below 15m swing for SL
QUANT_TRAIL_5M_BUFFER_ATR      = 0.25   # Buffer below 5m swing (tighter, phase 2+)
QUANT_TRAIL_1M_BUFFER_ATR      = 0.15   # Buffer below 1m swing (phase 3 only)
QUANT_TRAIL_LIQ_CLEARANCE_ATR  = 0.50   # SL must clear liquidity pools by 0.5 ATR

# ── Phase Entry Thresholds (CRITICAL FIX) ────────────────────────────────
# Old: Phase 1 required bos>=1 OR be_locked OR tier>=0.8
# Problem: In ranging markets BOS may never fire, and tier peaked at 0.76R
# New: Profit-based phase advancement ensures trail activates
QUANT_TRAIL_PHASE1_TIER        = 0.40   # Phase 1 at 0.40R profit (was implicit 0.8R)
QUANT_TRAIL_PHASE2_TIER        = 1.00   # Phase 2 at 1.00R
QUANT_TRAIL_PHASE3_TIER        = 2.00   # Phase 3 at 2.00R
QUANT_ICT_ZONE_FREEZE_ENABLED  = True
QUANT_ICT_ZONE_FREEZE_ATR      = 0.40
QUANT_ICT_OB_SL_ANCHOR         = True
QUANT_ICT_OB_SL_BUFFER_ATR     = 0.35
QUANT_ICT_LIQ_CEILING_ENABLED  = True
QUANT_ICT_LIQ_POOL_BUFFER_ATR  = 0.50
QUANT_CVD_WINDOW               = 20
QUANT_CVD_HIST_MULT            = 15
QUANT_VWAP_WINDOW              = 50
QUANT_EMA_FAST                 = 8
QUANT_EMA_SLOW                 = 21
QUANT_VOL_FLOW_WINDOW          = 10
QUANT_ATR_PCTILE_WINDOW        = 100
QUANT_ATR_MIN_PCTILE           = 0.00
QUANT_ATR_MAX_PCTILE           = 0.97
QUANT_MAX_HOLD_SEC             = 2400
QUANT_COOLDOWN_SEC             = 180
QUANT_LOSS_LOCKOUT_SEC         = 300
QUANT_POS_SYNC_SEC             = 30
QUANT_W_VWAP_DEV               = 0.30
QUANT_W_CVD_DIV                = 0.25
QUANT_W_OB                     = 0.20
QUANT_W_TICK_FLOW              = 0.15
QUANT_W_VOL_EXHAUSTION         = 0.10
QUANT_HTF_ENABLED              = True
QUANT_HTF_VETO_STRENGTH        = 0.35  # composite veto (trend entries); reversion uses per-TF
QUANT_OB_DEPTH_LEVELS          = 5
QUANT_OB_HIST_LEN              = 60
QUANT_TICK_AGG_WINDOW_SEC      = 30.0
QUANT_TP_MAX_RR                = 3.5
QUANT_SL_SWING_DENSITY_WINDOW  = 0.30
QUANT_TRAIL_CHANDELIER_N_START = 3.00
QUANT_TRAIL_CHANDELIER_N_END   = 1.50
QUANT_TRAIL_HVN_SNAP_THRESH    = 0.55
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
QUANT_MAX_SPREAD_ATR_RATIO     = 0.30
QUANT_REVERSION_MIN_RR         = 1.5
QUANT_REVERSION_MAX_RR         = 3.0
QUANT_TREND_MIN_RR             = 2.0
QUANT_TREND_MAX_RR             = 3.0
QUANT_TREND_SL_ATR_MULT        = 2.0
QUANT_TP_MIN_ATR_MULT          = 0.5
QUANT_TP_MAX_ATR_MULT          = 6.0
QUANT_REVERSION_REJECT_RR      = 0.20
QUANT_SL_MAX_ATR_MULT          = 4.0
QUANT_BO_MIN_SCORE             = 4
QUANT_BO_BLOCK_SEC             = 900
QUANT_BO_RETEST_TIMEOUT        = 900
QUANT_RETEST_RETRY_SEC         = 30
QUANT_SMART_MAX_HOLD           = True
QUANT_MAX_HOLD_PROFIT_SL_ATR   = 0.5
QUANT_MAX_HOLD_EXTENSIONS      = 5
QUANT_HOLD_EXTENSION_SEC       = 1200
QUANT_THESIS_MAX_DRAWDOWN_PCT  = 0.70

# ── Fee engine ────────────────────────────────────────────────────────────────
FEE_SPREAD_HIST_MAXLEN      = 500
FEE_SPREAD_DEFAULT_BPS      = 2.0
FEE_SLIP_ALPHA              = 0.25
FEE_SLIP_DEFAULT_BPS        = 1.5
FEE_SLIP_MIN_BPS            = 0.5
FEE_FLOOR_MULT_LOW          = 2.5
FEE_FLOOR_MULT_HIGH         = 1.2
FEE_FLOOR_MAX_ATR_MULT      = 2.0
FEE_FLOOR_INFLECT           = 0.45
FEE_FLOOR_STEEPNESS         = 6.0
FEE_FLOOR_ABS_MIN_MULT      = 1.4
FEE_SPREAD_ATR_WARN         = 0.06
FEE_SPREAD_PENALTY_K        = 4.0
FEE_CONF_NEUTRAL            = 0.5
FEE_CONF_MAX_DISCOUNT       = 0.30
FEE_MAKER_MIN_SAVING_BPS    = 0.5
FEE_MAKER_URGENCY_CUTOFF    = 0.82
FEE_MAKER_DEPTH_LEVELS      = 5
FEE_MAKER_DEPTH_MAX_FRAC    = 0.25
FEE_MAKER_DEPTH_FILL_FLOOR  = 0.35
FEE_MAKER_OPP_COST_WEIGHT   = 0.5

# ── ATR engine ────────────────────────────────────────────────────────────────
ATR_SEED_RETAIN         = 1
ATR_PCTILE_RANK_WINDOW  = 30

# ── ICT/SMC ───────────────────────────────────────────────────────────────────
OB_MIN_IMPULSE_PCT          = 0.15
OB_MIN_BODY_RATIO           = 0.40
OB_IMPULSE_SIZE_MULTIPLIER  = 1.30
OB_MAX_AGE_MINUTES          = 1440
FVG_MIN_SIZE_PCT            = 0.020
FVG_MAX_AGE_MINUTES         = 1440
LIQ_TOUCH_TOLERANCE_PCT     = 0.20
SWEEP_DISPLACEMENT_MIN      = 0.40
SWEEP_MAX_AGE_MINUTES       = 120
KZ_ASIA_NY_START            = 20
KZ_LONDON_NY_START          = 2
KZ_LONDON_NY_END            = 5
KZ_NY_NY_START              = 7
KZ_NY_NY_END                = 10

# ── ICT Gate ──────────────────────────────────────────────────────────────────
# Minimum ICT structural confluence score required before any trade entry.
# Ensures the bot never enters purely on quant signals without ICT structure.
# Set to 0.0 to disable (quant-only mode).
ICT_MIN_SCORE_FOR_ENTRY     = 0.45   # base gate (no OB credit) — session alone cannot pass
ICT_OB_MIN_SCORE_FOR_ENTRY  = 0.35   # reduced gate when price is at/in an active OB
                                      # (sig.ict_ob > 0.10). Rationale: in-zone OB entries
                                      # have structural backing that justifies a lower bar.
                                      # A twice-visited OTE BOS+DISP OB + KZ = ~0.35 → PASS.
ICT_REQUIRE_OB_OR_FVG       = False  # proximity scoring handles this intent; True = hard
                                      # require price physically inside an OB or FVG
# v5.0: Proximity scoring — partial OB/FVG credit when price is near but not inside.
# Mean-reversion entries fire after price bounces FROM an OB (just above it), so
# contains_price() is always False → OB score = 0. These decay windows control the range.
ICT_OB_PROXIMITY_ATR        = 1.5   # ATR distance within which an OB gets partial credit
ICT_FVG_PROXIMITY_ATR       = 0.8   # ATR distance within which an FVG gets partial credit
# v5.0: Confirmed displacement sweep bonus — added to weighted total post-scoring.
# Without bonus: sweep+session caps at 0.30, below every threshold.
ICT_SWEEP_DISP_BONUS        = 0.12

# HTF veto — per-timeframe (reversion entries)
# LONG veto:  15m < -0.35  OR  (15m < -0.20 AND 4h < -0.20)
# SHORT veto: 15m > +0.35  OR  (15m > +0.20 AND 4h > +0.20)
QUANT_HTF_15M_VETO           = 0.35  # 15m threshold for single-TF veto
QUANT_HTF_BOTH_VETO          = 0.20  # threshold when both TFs align against trade

# Quant signal quality gates (Gate C)
QUANT_MIN_RAW_COMPOSITE      = 0.35  # pre-ICT-boost composite floor
QUANT_MIN_CONFIRMING         = 4     # minimum confirming signals (of 5 quant + ICT)

# ── Legacy aliases (strategy code reads these) ────────────────────────────────
EXCHANGE = COINSWITCH_EXCHANGE   # used by CoinSwitch order_manager path

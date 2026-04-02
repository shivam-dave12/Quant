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
MAX_DAILY_LOSS          = 10000
MAX_DAILY_LOSS_PCT      = 3.0      # was 100.0 — institutional day circuit breaker (ANALYSIS.md §12)
MAX_DRAWDOWN_PCT        = 1000.0
MAX_CONSECUTIVE_LOSSES  = 2        # was 100 — 2 losses = thesis wrong, stop (config_overrides.py)
MAX_DAILY_TRADES        = 10       # was 1000 — institutional max per day (config_overrides.py)
ONE_POSITION_AT_A_TIME  = True
MIN_TIME_BETWEEN_TRADES = 3
TRADE_COOLDOWN_SECONDS  = 100
MIN_RISK_REWARD_RATIO   = 1.5
TARGET_RISK_REWARD_RATIO= 3.0
MAX_RR_RATIO            = 20.0

# ── Order execution ───────────────────────────────────────────────────────────
TICK_SIZE                        = 0.1
LIMIT_ORDER_OFFSET_TICKS         = 3      # fallback when book unavailable (3 × $0.1 = $0.30)
ORDER_TIMEOUT_SECONDS            = 600
MAX_ORDER_RETRIES                = 2
MAX_CONSECUTIVE_TIMEOUTS         = 2
TIMEOUT_EXTENDED_LOCKOUT_SEC     = 1800
SNIPER_MAX_DISTANCE_ATR          = 1.0
LIMIT_ORDER_FILL_TIMEOUT_SEC     = 60.0  # 60 for reversion fill; expired = cancel + cooldown
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
# get_candles() in quant_strategy.py for that timeframe.
# Delta API returns max ~200 candles per REST call — but the data manager
# accumulates candles via WebSocket after startup. Over 7 days of
# continuous operation, the buffer will hold the full structural history.
# These limits set the BUFFER SIZE, not the REST call size.
LOOKBACK_CANDLES_1M  = 300   # 5 hours of 1m (micro-structure + CVD)
LOOKBACK_CANDLES_5M  = 2100  # 7 days of 5m = 2016 candles (intraday pools)
LOOKBACK_CANDLES_15M = 700   # 7 days of 15m = 672 candles (key structure)
LOOKBACK_CANDLES_1H  = 200   # 8.3 days of 1h = 200 candles (hourly pivots)
LOOKBACK_CANDLES_4H  = 50    # unchanged — 50 × 4h = 8.3 days, sufficient
LOOKBACK_CANDLES_1D  = 30    # unchanged — 30 days of daily
CANDLE_TIMEFRAMES    = ["1m", "5m", "15m", "1h", "4h", "1d"]
PRIMARY_TIMEFRAME    = "15m"     # 15m is primary for SL/TP structure
ENTRY_TIMEFRAME      = "5m"      # 5m/1m for entry timing
HTF_TIMEFRAME        = "4h"

# ── Session config (BTC behaves differently per session) ─────────────
SESSION_TRAIL_WIDTH_MULT = {
    "asia":     1.60,   # Wide — stop hunts in thin liquidity
    "london":   1.20,   # Moderate — trending but volatile
    "ny":       1.00,   # Standard — highest conviction
    "late_ny":  1.30,   # Wider — reversals and consolidation
    "off":      1.50,   # Wide — low conviction
}
SESSION_ENTRY_QUALITY = {
    "asia":     "LOW",
    "london":   "HIGH",
    "ny":       "HIGH",
    "late_ny":  "MEDIUM",
    "off":      "LOW",
}

# ── Health / Supervisor ───────────────────────────────────────────────────────
WS_STALE_SECONDS                   = 35.0
HEALTH_CHECK_INTERVAL_SEC          = 12.0
PRICE_STALE_SECONDS                = 90.0
BALANCE_CACHE_TTL_SEC              = 35.0
STRUCTURE_UPDATE_INTERVAL_SECONDS  = 30
ENTRY_EVALUATION_INTERVAL_SECONDS  = 2   # was 1 — don't process every 250ms tick (config_overrides.py)
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
MIN_SL_DISTANCE_PCT          = 0.004  # was 0.001 — BTC@$66K: 0.004=$264 min SL dist (config_overrides.py)
MAX_SL_DISTANCE_PCT          = 0.035
SL_MIN_IMPROVEMENT_PCT       = 0.001
SL_RATCHET_ONLY              = True
SL_ATR_PERIOD                = 14
SL_ATR_BUFFER_MULT           = 0.75
SL_MIN_CLEARANCE_ATR_MULT    = 1.5
SL_MIN_IMPROVEMENT_ATR_MULT  = 0.20   # was 0.08 — prevents micro SL updates (config_overrides.py)
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
QUANT_TRAIL_BE_R               = 1.00   # was 0.30 — BE lock at 1.0R (config_overrides.py)
QUANT_TRAIL_LOCK_R             = 2.00   # was 0.80 — structural trail starts at 2.0R (config_overrides.py)
QUANT_TRAIL_AGGRESSIVE_R       = 3.50   # was 1.80 — aggressive trail at 3.5R (config_overrides.py)
QUANT_TRAIL_LIQ_MIN_BREATHING_ATR = 1.00  # minimum ATR gap between SL and price (config_overrides.py)
QUANT_TRAIL_MIN_DIST_ATR_P1    = 2.00   # Phase 1: 2.0 ATR min (was 1.50 — death zone)
QUANT_TRAIL_MIN_DIST_ATR_P2    = 1.50   # Phase 2: 1.5 ATR (was 1.10)
QUANT_TRAIL_MIN_DIST_ATR_P3    = 1.00   # Phase 3: 1.0 ATR (was 0.70)
QUANT_TRAIL_PULLBACK_FREEZE    = True
QUANT_TRAIL_PB_VOL_RATIO       = 0.65
QUANT_TRAIL_PB_DEPTH_ATR       = 1.20
QUANT_TRAIL_REV_MIN_SIGNALS    = 2
# Trail phase activation thresholds (profit-based — guarantees trail activates)
QUANT_TRAIL_PHASE1_TIER        = 0.40   # Phase 1 at 0.40R (was implicit 0.8R — never triggered)
QUANT_TRAIL_PHASE2_TIER        = 1.00
QUANT_TRAIL_PHASE3_TIER        = 2.00
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
QUANT_COOLDOWN_SEC             = 300    # was 180 — 5 min between trades (config_overrides.py)
QUANT_LOSS_LOCKOUT_SEC         = 5400   # was 300 — 90 min lockout after consec losses (config_overrides.py)
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
# Kill-zone windows in NY time (EST/EDT — ICTEngine applies DST automatically).
# KZ_ASIA_NY_END wraps across midnight: Asia KZ = ny >= KZ_ASIA_NY_START OR ny < KZ_ASIA_NY_END.
# The old default of KZ_ASIA_END=24 in the class attribute caused a latent always-True
# bug when config import failed.  Explicitly setting 1 here prevents that.
KZ_ASIA_NY_START            = 20   # 8 PM NY
KZ_ASIA_NY_END              = 1    # 1 AM NY  (wraps midnight)
KZ_LONDON_NY_START          = 2    # 2 AM NY
KZ_LONDON_NY_END            = 5    # 5 AM NY
KZ_NY_NY_START              = 7    # 7 AM NY
KZ_NY_NY_END                = 10   # 10 AM NY

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

# ── Conviction Filter ─────────────────────────────────────────────────────────
# All numeric thresholds consumed by conviction_filter.py.
# conviction_filter.py imports these via `from config import CONVICTION_*`.
#
# INSTITUTIONAL CALIBRATION v3.0 — calibrated for 65-75% WR, 3-6 trades/session.
# Source: config_overrides.py + ANALYSIS.md root cause analysis.
# ─────────────────────────────────────────────────────────────────────────────
# Score gate: weighted sum of 6 factors must reach this to allow entry.
# 0.82 = only highest-conviction setups (ANALYSIS.md §11.2). Was 0.55.
CONVICTION_MIN_SCORE               = 0.82

# Pool effective timeframe rank floor (1m=1, 5m=2, 15m=3, 1h=4, 4h=5, 1d=6).
# 1m/5m pools are noise — they form and dissolve every few minutes. Was 2.
CONVICTION_POOL_MIN_TF_RANK        = 3

# Displacement: minimum candle body / ATR ratio to count as meaningful.
# Proves institutional intent. Weak displacement = noise sweep. Was 0.55.
CONVICTION_DISPLACEMENT_BODY_ATR   = 0.70

# OTE (Optimal Trade Entry) Fibonacci retracement band.
CONVICTION_OTE_FIB_LOW             = 0.500   # 50% retrace
CONVICTION_OTE_FIB_HIGH            = 0.786   # 78.6% retrace (golden pocket)

# Hard R:R floor enforced as mandatory gate (early-return, no score computed).
CONVICTION_MIN_RR                  = 2.0

# Per-session consecutive-loss circuit breaker. Was MAX_DAILY_LOSS (effectively unlimited).
CONVICTION_MAX_SESSION_LOSSES      = 2

# Minimum seconds between consecutive entries. Was TRADE_COOLDOWN_SECONDS (100s).
# 300s = 5 minutes. Institutional pace: one trade per 15-30 min is normal.
CONVICTION_MIN_ENTRY_INTERVAL_SEC  = 300

# Maximum entries per kill zone session (London/NY/Asia).
# Was MAX_DAILY_TRADES (effectively unlimited). 5 = institutional norm.
CONVICTION_MAX_ENTRIES_PER_SESSION = 5

# ── Legacy aliases (strategy code reads these) ────────────────────────────────
EXCHANGE = COINSWITCH_EXCHANGE   # used by CoinSwitch order_manager path

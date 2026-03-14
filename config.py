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
MAX_CONSECUTIVE_LOSSES  = 3
MAX_DAILY_TRADES        = 8
ONE_POSITION_AT_A_TIME  = True
MIN_TIME_BETWEEN_TRADES = 5
TRADE_COOLDOWN_SECONDS  = 600
MIN_RISK_REWARD_RATIO   = 1.5
TARGET_RISK_REWARD_RATIO= 1.5
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
COMMISSION_RATE       = 0.00055
# Set COMMISSION_RATE_MAKER explicitly if your exchange has maker/taker tiers.
# Default: 40% of taker rate (typical tiered exchange).
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
# 10. QUANT STRATEGY PARAMETERS v4 — MEAN-REVERSION + ORDER FLOW
#     All read live by QCfg static methods — no restart needed.
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
QUANT_TP_VWAP_FRACTION      = 0.50
QUANT_VP_BUCKET_COUNT       = 50      # Volume profile resolution (price buckets)
QUANT_VP_HVN_THRESHOLD      = 0.70    # Top 30% volume = high-volume node
QUANT_OB_WALL_DEPTH         = 20      # Orderbook levels to scan for walls
QUANT_OB_WALL_MULT          = 2.5     # Qty > 2.5x avg = wall
QUANT_TRAIL_SWING_BARS      = 5       # 1m candle lookback for micro-swing trail
QUANT_TRAIL_VOL_DECAY_MULT  = 0.6     # Tighten trail when vol < 60% of entry vol

# 10d. Trailing SL
QUANT_TRAIL_ENABLED         = True
QUANT_TRAIL_BE_R            = 0.4
QUANT_TRAIL_LOCK_R          = 0.8

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

# 10k. Institutional SL/TP/Trail — new in v4.1
QUANT_TP_MAX_RR                = 3.5   # cap TP at 3.5:1 (don't chase impossible targets)
QUANT_SL_SWING_DENSITY_WINDOW  = 0.30  # ATR fraction radius for swing cluster detection
QUANT_TRAIL_CHANDELIER_N_START = 2.5   # chandelier: initial peak - N×ATR (wide, breathes)
QUANT_TRAIL_CHANDELIER_N_END   = 1.5   # chandelier: final N after hold time decays (tighter)
QUANT_TRAIL_HVN_SNAP_THRESH    = 0.55  # HVN volume percentile to trigger lock-in snap

# 10l. Trend-following mode (v4.2)
#      Activated when market regime is strongly directional.
#      Reversion mode remains active in ranging/transitioning regimes.
QUANT_ADX_PERIOD               = 14    # Wilder ADX period (5m candles)
QUANT_ADX_TREND_THRESH         = 25.0  # ADX above this → trending
QUANT_ADX_RANGE_THRESH         = 20.0  # ADX below this → ranging
QUANT_ATR_EXPANSION_THRESH     = 1.30  # current_atr / mean(atr[-20]) above this → expanding
QUANT_TREND_PULLBACK_ATR_MIN   = 0.10  # price must have pulled at least this much from EMA
QUANT_TREND_PULLBACK_ATR_MAX   = 2.00  # price must not be deeper than this below/above EMA
QUANT_TREND_CVD_MIN            = -0.20 # CVD trend bias floor (avoid buying into selling)
QUANT_TREND_TP_ATR_MULT        = 2.5   # trend TP = entry ± N×ATR (ATR-channel target)
QUANT_TREND_COMPOSITE_MIN      = 0.35  # min trend entry score
QUANT_TREND_CONFIRM_TICKS      = 3     # confirmation ticks for trend entries
QUANT_TREND_CHANDELIER_N       = 1.5   # trail in trend mode tighter (trend reversals are sharp)

# ═══════════════════════════════════════════════════════════════════
# 11. FEE ENGINE — ExecutionCostEngine, SpreadTracker, SlippageTracker,
#                  ProfitFloorModel, MakerTakerDecision
#     All values read live — change here, restart to apply.
# ═══════════════════════════════════════════════════════════════════

# 11a. SpreadTracker
FEE_SPREAD_HIST_MAXLEN      = 500    # rolling sample window (~8 min at 1 obs/s)
FEE_SPREAD_DEFAULT_BPS      = 2.0    # fallback spread when < 5 samples collected

# 11b. SlippageTracker
FEE_SLIP_ALPHA              = 0.25   # EWMA decay (α); ~4-trade half-life
FEE_SLIP_DEFAULT_BPS        = 1.5    # warm-up slippage before first real fill
FEE_SLIP_MIN_BPS            = 0.5    # floor on reported slippage (never report 0)

# 11c. ProfitFloorModel — sigmoid multiplier curve
#      Multiplier determines how many × the round-trip cost the TP must cover.
#      High = conservative (requires bigger move); Low = aggressive (smaller move OK).
FEE_FLOOR_MULT_LOW          = 5.5    # mult at ATR pctile ≤ 0.10 (quiet market, fees dominate)
FEE_FLOOR_MULT_HIGH         = 1.8    # mult at ATR pctile ≥ 0.85 (fat vol, fees are noise)
FEE_FLOOR_INFLECT           = 0.45   # ATR percentile at sigmoid midpoint
FEE_FLOOR_STEEPNESS         = 6.0    # sigmoid steepness (higher = sharper regime transition)
FEE_FLOOR_ABS_MIN_MULT      = 1.4    # hard floor on multiplier regardless of signal strength

# 11d. ProfitFloorModel — spread penalty
#      Applied when spread/ATR ratio > threshold (expensive market relative to its moves)
FEE_SPREAD_ATR_WARN         = 0.06   # spread/ATR fraction above which penalty applies (6%)
FEE_SPREAD_PENALTY_K        = 4.0    # penalty growth rate per unit above threshold

# 11e. ProfitFloorModel — signal confidence discount
#      High-conviction signals earn a slightly lower TP floor (max FEE_CONF_MAX_DISCOUNT)
FEE_CONF_NEUTRAL            = 0.5    # confidence below this = no discount
FEE_CONF_MAX_DISCOUNT       = 0.30   # fraction of floor that full-confidence can save (30%)

# 11f. MakerTakerDecision
FEE_MAKER_MIN_SAVING_BPS    = 0.8    # min risk-adjusted saving (bps) to justify posting limit
FEE_MAKER_URGENCY_CUTOFF    = 0.72   # urgency above this → always take market (time-sensitive)
FEE_MAKER_DEPTH_LEVELS      = 5      # orderbook levels to aggregate for depth check
FEE_MAKER_DEPTH_MAX_FRAC    = 0.25   # if order > this fraction of top-N depth → go market
FEE_MAKER_DEPTH_FILL_FLOOR  = 0.2    # min fill-probability when book depth is unknown/thin
FEE_MAKER_OPP_COST_WEIGHT   = 0.5    # weight on spread×(1-fill_p) as opportunity cost

# ═══════════════════════════════════════════════════════════════════
# 12. ATR ENGINE REGIME TUNING
#     Controls how the ATR percentile is computed and how quickly
#     it recovers after high-volatility warmup windows.
# ═══════════════════════════════════════════════════════════════════
ATR_SEED_RETAIN             = 20     # warmup seed values to keep (rest trimmed to avoid
                                     # stale high-vol values locking percentile at 0%)
ATR_PCTILE_RANK_WINDOW      = 30     # bars used for percentile ranking (shorter = faster
                                     # recovery after regime shift; ~2.5h at 5m cadence)

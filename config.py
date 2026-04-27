"""
config.py — Unified Configuration v10.0
=========================================
Single source of truth. All institutional parameters inline.
No config_overrides.py — everything lives here.

Calibrated for 65-75% WR, 3-6 trades per session.
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
DELTA_SYMBOL             = "BTCUSD"
DELTA_CONTRACT_VALUE_BTC = 0.001
DELTA_BALANCE_CURRENCY   = "USD"
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

# ── Risk management ──────────────────────────────────────────────────────────
# RISK_PER_TRADE: FRACTION of available balance risked per trade (NOT percent).
#   0.006 = 0.6% risk per trade.
#   Previous value 0.60 was interpreted as percent by risk_manager (÷100 = 0.006 → 0.6%)
#   but as FRACTION by quant_strategy._compute_quantity (× direct = 0.60 → 60%).
#   The inconsistency caused 100× over-sizing (entire balance at risk per trade),
#   triggering the "required margin > available — scaling down" warnings in logs.
#   Fix: one convention (fraction), both consumers agree. See risk_manager.py line 266.
RISK_PER_TRADE           = 0.03    # 3% of available balance per trade
MAX_DAILY_LOSS           = 10000
MAX_DAILY_LOSS_PCT       = 3.0       # day circuit breaker
MAX_DRAWDOWN_PCT         = 15.0      # realistic drawdown limit
MAX_CONSECUTIVE_LOSSES   = 3
MAX_DAILY_TRADES         = 10        # institutional selectivity over frequency
ONE_POSITION_AT_A_TIME   = True
MIN_TIME_BETWEEN_TRADES  = 5.0       # minutes; legacy alias for 300 seconds
MIN_TIME_BETWEEN_TRADES_SEC = 300.0
TRADE_COOLDOWN_SECONDS   = 300       # 5m cooldown after loss
MIN_RISK_REWARD_RATIO    = 2.0       # hard floor; reject thin R:R setups
TARGET_RISK_REWARD_RATIO = 3.0
MAX_RR_RATIO             = 20.0

# ── Order execution ───────────────────────────────────────────────────────────
TICK_SIZE                        = 0.5 if EXECUTION_EXCHANGE == "delta" else 0.1
TICK_SIZE_DELTA                  = 0.5
TICK_SIZE_COINSWITCH             = 0.1
LIMIT_ORDER_OFFSET_TICKS         = 3
ORDER_TIMEOUT_SECONDS            = 600
MAX_ORDER_RETRIES                = 2
MAX_CONSECUTIVE_TIMEOUTS         = 2
TIMEOUT_EXTENDED_LOCKOUT_SEC     = 1800
SNIPER_MAX_DISTANCE_ATR          = 1.0
LIMIT_ORDER_FILL_TIMEOUT_SEC     = 60.0
REQUEST_TIMEOUT                  = 30

# ── Data / Readiness ──────────────────────────────────────────────────────────
READY_TIMEOUT_SEC    = 120.0
MIN_CANDLES_1M       = 100
MIN_CANDLES_5M       = 100
MIN_CANDLES_15M      = 100
MIN_CANDLES_1H       = 20
MIN_CANDLES_4H       = 40
MIN_CANDLES_1D       = 7
LOOKBACK_CANDLES_1M  = 300
LOOKBACK_CANDLES_5M  = 2100
LOOKBACK_CANDLES_15M = 700
LOOKBACK_CANDLES_1H  = 200
LOOKBACK_CANDLES_4H  = 50
LOOKBACK_CANDLES_1D  = 30
CANDLE_TIMEFRAMES    = ["1m", "5m", "15m", "1h", "4h", "1d"]
PRIMARY_TIMEFRAME    = "15m"
ENTRY_TIMEFRAME      = "5m"
HTF_TIMEFRAME        = "4h"

# ── Session config ────────────────────────────────────────────────────────────
SESSION_TRAIL_WIDTH_MULT = {
    "asia":     1.60,
    "london":   1.20,
    "ny":       1.00,
    "late_ny":  1.30,
    "off":      1.50,
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
ENTRY_EVALUATION_INTERVAL_SECONDS  = 0.5    # evaluate more frequently
ENTRY_PENDING_TIMEOUT_SECONDS      = ORDER_TIMEOUT_SECONDS

# ── Logging / Reporting ───────────────────────────────────────────────────────
LOG_LEVEL                    = "INFO"
TELEGRAM_REPORT_INTERVAL_SEC = 900
OUTLOOK_INTERVAL_SECONDS     = 900

# ── Fees ──────────────────────────────────────────────────────────────────────
COMMISSION_RATE              = 0.00055
COMMISSION_RATE_MAKER        = 0.00020
DELTA_COMMISSION_RATE        = 0.00050
DELTA_COMMISSION_RATE_MAKER  = -0.00020
# Stop/SL exits are treated as taker risk-exits for true net breakeven.
STOP_EXIT_COMMISSION_RATE   = 0.00055

# ── Rate limiting ─────────────────────────────────────────────────────────────
GLOBAL_API_MIN_INTERVAL  = 3.0
DELTA_API_MIN_INTERVAL   = 0.25
RATE_LIMIT_ORDERS        = 15

# ── SL infrastructure ─────────────────────────────────────────────────────────
SL_LIMIT_OFFSET_TICKS    = 20
# Institutional SL sizing:
#   1. Anchor to invalidation structure (sweep wick, OB, swing, or pushed pool).
#   2. Clear live noise with an ATR-regime floor and wick-depth clearance.
#   3. Permit wide structural stops, then shrink quantity by dollar risk.
#   4. Reject only when the stop crosses the liquidation guard.

def get_tick_size(exchange: str | None = None) -> float:
    """Authoritative tick-size lookup for execution-sensitive price rounding."""
    ex = (exchange or EXECUTION_EXCHANGE or "").lower()
    if ex == "delta":
        return float(TICK_SIZE_DELTA)
    if ex == "coinswitch":
        return float(TICK_SIZE_COINSWITCH)
    return float(TICK_SIZE)


def validate_config() -> None:
    """Fail fast on inconsistent trading-risk configuration."""
    errors = []
    if MIN_RISK_REWARD_RATIO < 1.5:
        errors.append("MIN_RISK_REWARD_RATIO must be >= 1.5")
    if abs(CONVICTION_MIN_RR - MIN_RISK_REWARD_RATIO) > 1e-9:
        errors.append("CONVICTION_MIN_RR must match MIN_RISK_REWARD_RATIO")
    if abs(QUANT_REVERSION_MIN_RR - MIN_RISK_REWARD_RATIO) > 1e-9:
        errors.append("QUANT_REVERSION_MIN_RR must match MIN_RISK_REWARD_RATIO")
    if get_tick_size() <= 0:
        errors.append("tick size must be positive")
    if MAX_DAILY_LOSS_PCT <= 0 or MAX_DAILY_LOSS_PCT > 10:
        errors.append("MAX_DAILY_LOSS_PCT must be in (0, 10]")
    if errors:
        raise ValueError("Invalid config: " + "; ".join(errors))


SL_BUFFER_TICKS          = 5
# ATR-regime noise floor for sweep/momentum entries.
SL_MIN_ATR_MULT              = 0.20   # SL < 0.20 ATR is inside spread/noise, reject
# Structural wick clearance: SL must extend at least this fraction of wick_depth
# PAST the wick tip (not inside the wick body).
# 0.10 = 10% of wick depth as extra clearance (e.g., 7pt wick → 0.7pt)
SL_SWEEP_WICK_CLEARANCE_MULT = 0.10
# ATR-regime adaptation slope: scales SL buffer by current ATR percentile rank.
# regime_mult = 0.60 + SL_REGIME_BUFF_SLOPE * atr_pctile
# Low-vol (p=0): mult=0.60 — tight; Normal (p=0.5): mult=1.00; High-vol (p=1): mult=1.40
SL_REGIME_BUFF_SLOPE         = 0.80
SL_MIN_IMPROVEMENT_PCT   = 0.001
SL_RATCHET_ONLY          = True
SL_ATR_PERIOD            = 14
SL_ATR_BUFFER_MULT       = 0.75      # trail manager buffer (separate from entry SL)
SL_MIN_CLEARANCE_ATR_MULT    = 1.5
SL_MIN_IMPROVEMENT_ATR_MULT  = 0.20   # prevents micro SL updates
TRAILING_SL_CHECK_INTERVAL   = 10
TRAIL_SWING_MAX_AGE_MS       = 14_400_000

# ── Aggregator ────────────────────────────────────────────────────────────────
AGG_PRIMARY_WEIGHT   = 0.55
AGG_SECONDARY_WEIGHT = 0.45
AGG_OB_DEPTH_LEVELS  = 10
AGG_TRADE_WINDOW_SEC = 30.0

# ── Quant Strategy ────────────────────────────────────────────────────────────
QUANT_MARGIN_PCT               = 0.20
QUANT_SLIPPAGE_TOLERANCE       = 0.0005
QUANT_VWAP_ENTRY_ATR_MULT      = 1.2
QUANT_CVD_DIVERGENCE_MIN       = 0.15
QUANT_OB_CONFIRM_MIN           = 0.10
QUANT_COMPOSITE_ENTRY_MIN      = 0.35
QUANT_EXIT_REVERSAL_THRESH     = 0.40
QUANT_CONFIRM_TICKS            = 2        # require sustained confirmation
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
QUANT_TRAIL_BE_R               = 1.00     # BE lock at 1.0R
QUANT_TRAIL_LOCK_R             = 1.00     # Fib trail begins after BE checkpoint
QUANT_TRAIL_AGGRESSIVE_R       = 3.50     # aggressive trail at 3.5R
QUANT_TRAIL_LIQ_MIN_BREATHING_ATR = 1.00
QUANT_TRAIL_MIN_DIST_ATR_P1    = 2.00
QUANT_TRAIL_MIN_DIST_ATR_P2    = 1.50
QUANT_TRAIL_MIN_DIST_ATR_P3    = 1.00
QUANT_TRAIL_PULLBACK_FREEZE    = True
QUANT_TRAIL_PB_VOL_RATIO       = 0.65
QUANT_TRAIL_PB_DEPTH_ATR       = 1.20
QUANT_TRAIL_REV_MIN_SIGNALS    = 2
QUANT_TRAIL_PHASE1_TIER        = 0.40
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
QUANT_ATR_MIN_PCTILE           = 0.05
QUANT_ATR_MAX_PCTILE           = 0.97
QUANT_MAX_HOLD_SEC             = 3600      # 60 min max hold
QUANT_COOLDOWN_SEC             = 300       # 5m between trades
QUANT_LOSS_LOCKOUT_SEC         = 1800      # 30 min lockout after consec losses
QUANT_POS_SYNC_SEC             = 30
RECONCILE_POST_EXIT_SETTLE_SEC = 15.0      # ignore stale position feed right after local exit

# ─────────────────────────────────────────────────────────────────────────────
# POST-EXIT RE-ENTRY GATE (strategy/post_exit_gate.py)
# ─────────────────────────────────────────────────────────────────────────────
# Six-lens gate that replaces the flat 30s cooldown with regime-aware logic.
# Goal: stop the "exit → re-enter in 30s → take another stop" failure mode.
#
# Each constant tunes one lens. Defaults are conservative for BTC perps on a
# 1m/5m/15m liquidity-first stack; relax with caution.

POST_EXIT_BASE_SEC                = 60.0   # absolute floor on time-since-exit
POST_EXIT_LOSS_DECAY_FACTOR       = 2.0    # 2^(N-1) cooldown after N losses
POST_EXIT_LOSS_DECAY_CAP_SEC      = 900.0  # 15-min ceiling on loss decay
POST_EXIT_FLIP_BASE_SEC           = 120.0  # min time before opposite-side after SL
POST_EXIT_FLIP_MIN_ATR_FROM_EXIT  = 1.5    # opposite side needs ≥1.5 ATR distance
POST_EXIT_FLIP_REQUIRES_BOS       = True   # opposite side also needs BOS/CHoCH
POST_EXIT_TP_SAMESIDE_BASE_SEC    = 90.0   # min time before same-side after TP
POST_EXIT_TP_SAMESIDE_PULLBACK_PCT = 0.50  # need 50% retrace of prior MFE
POST_EXIT_ATR_SHOCK_PCT           = 0.40   # ±40% ATR change = regime shock
POST_EXIT_ATR_SHOCK_PENALTY_SEC   = 180.0  # extra cooldown on ATR shock
POST_EXIT_STRUCTURE_PROOF_REQUIRED = True  # require BOS/CHoCH/sweep/displacement
POST_EXIT_STRUCTURE_PROOF_TIMEOUT  = 240.0 # gate self-relaxes after 4 min
POST_EXIT_LOSS_SAMESIDE_DEAD_SEC  = 300.0  # reserved (not used by current lenses)
QUANT_W_VWAP_DEV               = 0.30
QUANT_W_CVD_DIV                = 0.25
QUANT_W_OB                     = 0.20
QUANT_W_TICK_FLOW              = 0.15
QUANT_W_VOL_EXHAUSTION         = 0.10
QUANT_HTF_ENABLED              = True
QUANT_HTF_VETO_STRENGTH        = 0.70
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
QUANT_MAX_SPREAD_ATR_RATIO     = 0.50     # more spread tolerance
QUANT_REVERSION_MIN_RR         = 2.0      # single authoritative R:R floor
QUANT_REVERSION_MAX_RR         = 5.0
QUANT_TREND_MIN_RR             = 2.0
QUANT_TREND_MAX_RR             = 5.0
QUANT_TREND_SL_ATR_MULT        = 2.0
QUANT_TP_MIN_ATR_MULT          = 0.5
QUANT_TP_MAX_ATR_MULT          = 6.0
QUANT_REVERSION_REJECT_RR      = 0.20
QUANT_SMART_MAX_HOLD           = True
QUANT_MAX_HOLD_PROFIT_SL_ATR   = 0.5
QUANT_MAX_HOLD_EXTENSIONS      = 5
QUANT_HOLD_EXTENSION_SEC       = 1200
QUANT_THESIS_MAX_DRAWDOWN_PCT  = 0.70
QUANT_MIN_RAW_COMPOSITE        = 0.35
QUANT_MIN_CONFIRMING           = 3

# ── Fee engine ────────────────────────────────────────────────────────────────
FEE_SPREAD_HIST_MAXLEN      = 500
# CFG-2 fix: 0.20 matches fee_engine code-level default (line 115 comment says
# "Warmup default: 0.20 bps — realistic for BTC inverse perp (actual ~0.15 bps).
# The old default of 2.0 bps was 13× too wide, causing fee-floor over-rejection
# during the first ~5 seconds of each session.")
FEE_SPREAD_DEFAULT_BPS      = 0.20
FEE_SLIP_ALPHA              = 0.25
FEE_SLIP_DEFAULT_BPS        = 1.5
FEE_SLIP_MIN_BPS            = 0.5
FEE_FLOOR_MULT_LOW          = 2.5
FEE_FLOOR_MULT_HIGH         = 1.2
FEE_FLOOR_MAX_ATR_MULT      = 2.0
FEE_FLOOR_INFLECT           = 0.45
FEE_FLOOR_STEEPNESS         = 6.0
# CFG-3 fix: fee_engine code comment (line 239) says "was 1.4" — lowered to 1.2
FEE_FLOOR_ABS_MIN_MULT      = 1.2
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
KZ_ASIA_NY_END              = 1
KZ_LONDON_NY_START          = 2
KZ_LONDON_NY_END            = 5
KZ_NY_NY_START              = 7
KZ_NY_NY_END                = 10

# ── ICT Gate ──────────────────────────────────────────────────────────────────
ICT_MIN_SCORE_FOR_ENTRY     = 0.25     # lowered ICT score gate
ICT_OB_MIN_SCORE_FOR_ENTRY  = 0.20     # lowered OB score gate
ICT_REQUIRE_OB_OR_FVG       = False
ICT_OB_PROXIMITY_ATR        = 1.5
ICT_FVG_PROXIMITY_ATR       = 0.8
ICT_SWEEP_DISP_BONUS        = 0.12

# ── HTF context ───────────────────────────────────────────────────────────────
QUANT_HTF_15M_VETO           = 0.35
QUANT_HTF_BOTH_VETO          = 0.20

# ── Conviction Filter ─────────────────────────────────────────────────────────
CONVICTION_MIN_SCORE               = 0.68
CONVICTION_POOL_MIN_TF_RANK        = 3       # 15m+ pool or HTF-promoted 5m
CONVICTION_DISPLACEMENT_BODY_ATR   = 0.70
CONVICTION_OTE_FIB_LOW             = 0.500
CONVICTION_OTE_FIB_HIGH            = 0.786
CONVICTION_MIN_RR                  = 2.0     # match risk management R:R
CONVICTION_PRODUCT_MIN_CORE        = 0.60    # pool/displacement/CISD must each be real
CONVICTION_MAX_SESSION_LOSSES      = 2
CONVICTION_MIN_ENTRY_INTERVAL_SEC  = 300
CONVICTION_MAX_ENTRIES_PER_SESSION = 6

# ── Trail (liquidity-first) ───────────────────────────────────────────────────
QUANT_TRAIL_LIQ_BASE_BUF_MAX_ATR  = 0.25
QUANT_TRAIL_LIQ_BASE_BUF_MIN_ATR  = 0.15
QUANT_TRAIL_LIQ_SAFETY_BUF_ATR    = 0.28
QUANT_TRAIL_LIQ_POOL_LOOKBACK_ATR = 8.0
QUANT_TRAIL_LIQ_BOS_CONFIRM_GATE  = True
QUANT_TRAIL_LIQ_BOS_MAX_AGE_MS    = 10_000_000
QUANT_TRAIL_DISP_CVD_GATE         = True
QUANT_TRAIL_CVD_MIN_TREND          = 0.12
QUANT_TRAIL_DISP_MIN_ATR_MULT     = 0.58
QUANT_TRAIL_DISP_CVD_MIN_R        = 0.30
QUANT_TRAIL_OB_BREAKER_PRIORITY    = True
QUANT_TRAIL_OB_BREAKER_BUFFER_ATR  = 0.22
QUANT_TRAIL_AMD_MANIP_BUFFER_MULT  = 1.55
QUANT_TRAIL_AMD_DIST_BUFFER_MULT   = 0.62
QUANT_TRAIL_AMD_REDIST_BUFFER_MULT = 1.12
QUANT_TRAIL_HTF_CASCADE_ENABLED    = True
QUANT_TRAIL_LIQ_POOL_PROX_ATR     = 2.20
QUANT_TRAIL_LIQ_FLOOR_BUFFER_ATR   = 0.30

# Institutional profit defense: not aggressive trailing.
# Triggers only after meaningful delivery + large giveback + adverse evidence.
PROFIT_DEFENSE_MIN_MFE_R       = 1.20
PROFIT_DEFENSE_GIVEBACK_FRAC   = 0.65
PROFIT_DEFENSE_COUNTER_CVD     = 0.30
PROFIT_DEFENSE_BOS_MAX_AGE_MS  = 720_000
PROFIT_DEFENSE_BE_CUSHION_ATR  = 0.05
PROFIT_DEFENSE_MIN_INTERVAL_SEC = 60.0
PROFIT_DEFENSE_POOL_GATE_MAX_AGE_SEC = 180.0

# ── CHoCH expiry ──────────────────────────────────────────────────────────────
QUANT_CHOCH_EXPIRY_BARS = 10

# ── Legacy alias ──────────────────────────────────────────────────────────────
EXCHANGE = COINSWITCH_EXCHANGE

validate_config()

# ── Pydantic schema validation (Arch fix — structured, typed, cross-field) ──────
# Runs after validate_config() so both passes see the same constant values.
# Raises ValueError with a precise field-level message on any inconsistency.
# Downstream modules can use: from config_schema import cfg
try:
    from config_schema import cfg as _cfg_validated  # noqa: F401
except ImportError:
    pass  # config_schema.py not present; schema validation skipped

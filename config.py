"""
config.py — Unified Configuration v10.0
=========================================
Single source of truth. All institutional parameters inline.
No config_overrides.py — everything lives here.

Calibrated for 65-75% WR, 3-6 trades per session.
"""
import os
try:
    from dotenv import load_dotenv
except ImportError:  # production image may not ship python-dotenv
    def load_dotenv(*_a, **_kw):
        return False
load_dotenv()

def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return bool(default)
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def _env_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))

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

REQUIRE_EXCHANGE_CREDENTIALS = os.getenv("REQUIRE_EXCHANGE_CREDENTIALS", "false").lower() in {"1", "true", "yes", "on"}

# Keep module imports/test tooling/dashboard safe without live secrets. The bot
# runtime still refuses to trade unless a configured exchange has both key and
# secret in main.initialize(). Set REQUIRE_EXCHANGE_CREDENTIALS=true for a
# deployment-time fail-fast check.
if REQUIRE_EXCHANGE_CREDENTIALS and not (DELTA_API_KEY or COINSWITCH_API_KEY):
    raise ValueError("No exchange credentials in .env. Set DELTA_API_KEY/DELTA_SECRET_KEY or COINSWITCH_API_KEY/COINSWITCH_SECRET_KEY.")

# ── Symbol / leverage policy ───────────────────────────────────────────────────
SYMBOL                   = "BTCUSDT"
# LEVERAGE is retained as a compatibility display knob only. v83 sizing uses
# core.market_policy.MAX_POLICY_LEVERAGE plus each venue's confirmed product cap.
LEVERAGE                 = 1
MAX_POLICY_LEVERAGE      = _env_float("MAX_POLICY_LEVERAGE", 40.0)
POLICY_CRYPTO_LEVERAGE_UTIL = _env_float("POLICY_CRYPTO_LEVERAGE_UTIL", 0.55)
POLICY_FUTURE_LEVERAGE_UTIL = _env_float("POLICY_FUTURE_LEVERAGE_UTIL", 0.50)
POLICY_COMMODITY_LEVERAGE_UTIL = _env_float("POLICY_COMMODITY_LEVERAGE_UTIL", 0.40)
POLICY_EQUITY_LEVERAGE_UTIL = _env_float("POLICY_EQUITY_LEVERAGE_UTIL", 0.32)
POLICY_INDEX_LEVERAGE_UTIL = _env_float("POLICY_INDEX_LEVERAGE_UTIL", 0.35)
POLICY_GENERIC_LEVERAGE_UTIL = _env_float("POLICY_GENERIC_LEVERAGE_UTIL", 0.35)
POLICY_ICICI_CASH_MARGIN_PCT = _env_float("POLICY_ICICI_CASH_MARGIN_PCT", 0.06)
DELTA_SYMBOL             = "BTCUSD"
DELTA_CONTRACT_VALUE_BTC = 0.001
DELTA_BALANCE_CURRENCY   = "USD"
COINSWITCH_SYMBOL        = "BTCUSDT"
COINSWITCH_EXCHANGE      = "EXCHANGE_2"

# ── Position sizing ───────────────────────────────────────────────────────────
BALANCE_USAGE_PERCENTAGE = 60
MAX_ENTRY_MARGIN_USAGE_PCT = BALANCE_USAGE_PERCENTAGE  # single-trade margin ceiling
MIN_MARGIN_PER_TRADE     = 1
MAX_MARGIN_PER_TRADE     = 10_000
MIN_POSITION_SIZE        = 0.001
MAX_POSITION_SIZE        = 1.0
LOT_STEP_SIZE            = 0.001
REMAINDER_MIN_QTY        = 0.001

# ── Risk management ──────────────────────────────────────────────────────────
# RISK_PER_TRADE: FRACTION of available balance risked per trade (NOT percent).
#   0.005 = 0.5% risk per trade.
#   Previous value 0.60 was interpreted as percent by risk_manager (÷100 = 0.006 → 0.6%)
#   but as FRACTION by quant_strategy._compute_quantity (× direct = 0.60 → 60%).
#   The inconsistency caused 100× over-sizing (entire balance at risk per trade),
#   triggering the "required margin > available — scaling down" warnings in logs.
#   Fix: one convention (fraction), both consumers agree. See risk_manager.py line 266.
RISK_PER_TRADE           = 0.005   # 0.5% of available balance per trade
MAX_DAILY_LOSS           = 10000
MAX_DAILY_LOSS_PCT       = 3.0       # day circuit breaker
MAX_DRAWDOWN_PCT         = 15.0      # realistic drawdown limit
MAX_CONSECUTIVE_LOSSES   = 2
ALLOW_TIME_BASED_CONSEC_LOSS_RESET = False
CONSEC_LOSS_AUTO_RESET_HOURS = 2.0
MAX_DAILY_TRADES         = 10        # institutional selectivity over frequency
ONE_POSITION_AT_A_TIME   = True
MIN_TIME_BETWEEN_TRADES  = 5.0       # minutes; compatibility alias for 300 seconds
MIN_TIME_BETWEEN_TRADES_SEC = 300.0
TRADE_COOLDOWN_SECONDS   = 300       # 5m cooldown after loss
MIN_RISK_REWARD_RATIO    = 2.0       # expected-utility reference; thin R:R reduces size/EV
TARGET_RISK_REWARD_RATIO = 3.0
MAX_RR_RATIO             = 20.0

# ── Institutional dynamic execution audit ────────────────────────────────────
# Quality signals are priced into the execution decision. Mechanical defects
# always stop routing; weak delivery/coherence scales exposure dynamically.
INSTITUTIONAL_DYNAMIC_SCORE_REFERENCE = 0.66
INSTITUTIONAL_TARGET_REALISM_REFERENCE = 0.52
INSTITUTIONAL_MIN_DECISION_SCORE   = INSTITUTIONAL_DYNAMIC_SCORE_REFERENCE
INSTITUTIONAL_MIN_TARGET_REALISM   = INSTITUTIONAL_TARGET_REALISM_REFERENCE
ENTRY_ENGINE_SIGNAL_COOLDOWN_SEC   = 10.0
IC_IMPAIRMENT_SIZE_MULT            = 0.35
POST_EXIT_IMPAIRMENT_SIZE_MULT     = 0.40


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
# v73: high-readiness accepted sweeps must not sit as passive maker orders.
# Use the final EntryReadiness surface, not raw composite signal confidence, to
# trigger fast native-bracket execution. This is still exchange-attached TP/SL.
ENTRY_PROTECTED_CROSS_READINESS  = 0.78
ENTRY_PROTECTED_CROSS_EDGE       = 0.06
ENTRY_PROTECTED_CROSS_REQUIRE_SIGNAL_CONF = False
ENTRY_PROTECTED_CROSS_MIN_CONF   = 0.70
ENTRY_PROTECTED_CROSS_TICKS      = 2.0
DELTA_NATIVE_BRACKET_MARKET_FOR_PROTECTED_CROSS = True
PROTECTED_CROSS_FILL_TIMEOUT_SEC = 12.0
REQUEST_TIMEOUT                  = 30
# Delta multi-asset protection policy: every Delta entry must use native bracket
# placement (entry + SL + TP in one exchange transaction). If bracket placement
# fails, the strategy aborts the entry instead of falling back to naked limit
# + standalone conditionals. CoinSwitch still uses standalone SL/TP because it
# has no Delta-style native bracket endpoint.
DELTA_REQUIRE_NATIVE_BRACKET      = True

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
# Fee burden is priced as an execution-cost surface, not a retail filter.
# Above SOFT_MAX the bot cuts allocation; above NO_ALLOC the unit economics are
# negative per unit of risk, so the allocator returns no capital.
FEE_TO_RISK_SOFT_MAX        = 0.35
FEE_TO_RISK_NO_ALLOC        = 0.75

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
# Deprecated compatibility only: the live trail is liquidity/structure based.
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
QUANT_MAX_SPREAD_ATR_RATIO     = 0.50     # crypto/BTC hard spread/ATR cap
# Asset-aware spread gate (v8): xStock/RWA products have coarse tick geometry;
# a normal 1-4 tick spread can exceed the current 5m ATR in quiet windows.
# These caps hard-block genuinely broken books while converting normal wide
# tokenised-equity spreads into allocation haircuts handled by sizing/EV.
QUANT_SPREAD_SOFT_ATR_RATIO_CRYPTO    = 0.30
QUANT_MAX_SPREAD_BPS_CRYPTO           = 12.0
QUANT_MAX_SPREAD_TICKS_CRYPTO         = 10.0
QUANT_SPREAD_SOFT_ATR_RATIO_EQUITY    = 0.50
QUANT_MAX_SPREAD_ATR_RATIO_EQUITY     = 4.00
QUANT_MAX_SPREAD_BPS_EQUITY           = 35.0
QUANT_MAX_SPREAD_TICKS_EQUITY         = 8.0
QUANT_SPREAD_SOFT_ATR_RATIO_COMMODITY = 0.50
QUANT_MAX_SPREAD_ATR_RATIO_COMMODITY  = 2.00
QUANT_MAX_SPREAD_BPS_COMMODITY        = 45.0
QUANT_MAX_SPREAD_TICKS_COMMODITY      = 10.0
QUANT_SPREAD_MIN_SIZE_MULT            = 0.35
QUANT_SPREAD_SIZE_HAIRCUT_MAX         = 0.55
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
CONVICTION_MIN_SCORE               = 0.74
CONVICTION_POOL_MIN_TF_RANK        = 3       # 15m+ pool or HTF-promoted 5m
CONVICTION_DISPLACEMENT_BODY_ATR   = 0.85
CONVICTION_OTE_FIB_LOW             = 0.500
CONVICTION_OTE_FIB_HIGH            = 0.786
CONVICTION_MIN_RR                  = 2.0     # match risk management R:R
CONVICTION_PRODUCT_MIN_CORE        = 0.68    # pool/displacement/CISD must each be real
CONVICTION_MAX_SESSION_LOSSES      = 2
CONVICTION_MIN_ENTRY_INTERVAL_SEC  = 420
CONVICTION_MAX_ENTRIES_PER_SESSION = 3


# ── Institutional Dynamic Entry Quality References ───────────────────────────
# These are adaptive scoring references for the executable entry surface. Weak
# structure first lowers quality/size; critical defects pause routing until the
# market provides accepted delivery.
ENTRY_DYNAMIC_MIN_DISPLACEMENT_ATR       = 0.75
ENTRY_HARD_MIN_DISPLACEMENT_ATR          = ENTRY_DYNAMIC_MIN_DISPLACEMENT_ATR  # compatibility alias
ENTRY_STRONG_DISPLACEMENT_ATR            = 1.25
ENTRY_MAX_CHASE_ATR_WITHOUT_OTE          = 1.15
ENTRY_REVERSAL_PD_LONG_MAX               = 0.62
ENTRY_REVERSAL_PD_SHORT_MIN              = 0.38
ENTRY_CONTINUATION_MIN_ACCEPTANCE_ATR    = 0.55
ENTRY_FLOW_HARD_OPPOSE_THRESHOLD         = 0.40  # compatibility name; dynamic penalty reference
ENTRY_CVD_HARD_OPPOSE_THRESHOLD          = 0.30  # compatibility name; dynamic penalty reference
ENTRY_GATE_LOG_INTERVAL_SEC              = 12.0
ENTRY_EXECUTION_QUALITY_MIN              = 0.52
ENTRY_CRITICAL_DEFECT_FLOOR              = 0.62
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
# Triggers only after meaningful volatility-adjusted delivery + large giveback
# + adverse evidence. These thresholds are ATR/structure based, not fixed R.
PROFIT_DEFENSE_MIN_MFE_ATR       = 1.80
PROFIT_DEFENSE_GIVEBACK_FRAC     = 0.72
PROFIT_DEFENSE_COUNTER_CVD       = 0.30
PROFIT_DEFENSE_BOS_MAX_AGE_MS    = 720_000
PROFIT_DEFENSE_BE_CUSHION_ATR    = 0.05
PROFIT_DEFENSE_MIN_INTERVAL_SEC  = 90.0
PROFIT_DEFENSE_POOL_GATE_MAX_AGE_SEC = 180.0

# Liquidity-delivery trailing.
# The original SL distance is used for sizing and risk control only. Once live,
# stop movement is based on accepted market structure: delivered internal
# liquidity, confirmed swings, true net BE, and ATR breathing room.
TRAIL_PHASE0_MAX_DELIVERY_ATR        = 0.75
TRAIL_STRUCTURE_MIN_DELIVERY_ATR     = 1.10
TRAIL_AGGRESSIVE_MIN_DELIVERY_ATR    = 2.60
TRAIL_DELIVERY_LOCK_MIN_MFE_ATR      = 1.80
TRAIL_DELIVERY_POOL_MIN_SIG          = 3.0
TRAIL_DELIVERY_POOL_BUFFER_ATR       = 0.30
TRAIL_DELIVERY_SWING_BUFFER_ATR      = 0.30
TRAIL_DELIVERY_SWING_LOOKBACK_BARS   = 36
TRAIL_DELIVERY_LOCK_MIN_BREATHING_ATR = 0.85
TRAIL_DELIVERY_LOCK_MIN_IMPROVEMENT_ATR = 0.25

# Failed-delivery defense. At this point the move is no longer a small
# pullback; it has given back most of the delivered leg. It may flatten at
# market only if the estimated exit remains meaningfully net-profitable.
PROFIT_DEFENSE_FAILED_DELIVERY_MIN_MFE_ATR = 2.50
PROFIT_DEFENSE_FAILED_DELIVERY_GIVEBACK_FRAC = 0.78
PROFIT_DEFENSE_MIN_NET_ATR_TO_EXIT = 0.35

# Payoff-quality floor for structural trail updates. BE/counter-BOS protection is
# still allowed immediately; this only stops "profit locks" that would book a
# fee-dragged scratch instead of a meaningful institutional win.
PAYOFF_TRAIL_MIN_NET_R = 0.50
PAYOFF_TRAIL_MIN_COST_MULT = 0.75

# ── CHoCH expiry ──────────────────────────────────────────────────────────────
QUANT_CHOCH_EXPIRY_BARS = 10

# ── Compatibility alias ───────────────────────────────────────────────────────
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

# ── Institutional exit accounting / anti-whipsaw controls ────────────────
# Market/profit-defense exits must be reconciled from their own reduce-only
# order id, not from cancelled SL/TP child orders. While that order is still
# propagating, defer PnL booking instead of recording $0.00.
EXIT_MANUAL_CONFIRM_MAX_WAIT_SEC = 120.0

# Do not flatten just because a trade gave back profit. Institutions treat
# giveback as a watch condition; actual exit requires counter-flow, counter-BOS,
# pool-gate reversal, or an explicit override.
PROFIT_DEFENSE_ALLOW_GIVEBACK_ONLY_EXIT = False

# More breathing room before BE / delivery-lock moves. Prevents being stopped
# on normal pullbacks while still preventing fee-adjusted loss accounting.
TRAIL_BE_MIN_BREATHING_ATR = 0.75


# ─────────────────────────────────────────────────────────────────────────────
# MARKET INTELLIGENCE ADAPTIVE PROFILE
# ─────────────────────────────────────────────────────────────────────────────
# These are not trading triggers. They are broad regime-boundary priors used by
# strategy/market_intelligence.py to convert live ATR, liquidity density, spread,
# HTF structure, AMD phase and flow into adaptive thresholds. The strategy logic
# should read dynamic profile values rather than embedding fixed cutoffs in each
# engine.
MI_COMPRESSED_ATR_PCT = 0.08
MI_EXPANDED_ATR_PCT   = 0.28
MI_STRESS_ATR_PCT     = 0.55
MI_LIQ_DENSITY_RADIUS_ATR = 4.0
MI_WIDE_SPREAD_BPS = 5.0
MI_ENABLE_DYNAMIC_ENTRY_GATES = True
MI_ENABLE_DYNAMIC_CONVICTION_GATES = True
MI_ENABLE_DYNAMIC_TRAILING_GATES = True
MI_ENABLE_DYNAMIC_POST_EXIT_GATES = True

# ─────────────────────────────────────────────────────────────────────────────
# MULTI-ASSET LIVE CATALOG SCANNER
# ─────────────────────────────────────────────────────────────────────────────
# The scanner does NOT trade aliases.  It queries Delta/CoinSwitch live product
# catalogs and activates only contracts actually returned by the exchange.
MULTI_ASSET_ENABLED = True
SCANNER_MAX_ACTIVE_INSTRUMENTS = _env_int("SCANNER_MAX_ACTIVE_INSTRUMENTS", 0)
SCANNER_TICK_SLEEP_SEC = 0.25
SCANNER_ASSET_HEARTBEAT_SEC = 60.0
SCANNER_ASSET_ANALYSIS_LOG_SEC = 15.0  # per-contract proof-of-analysis log cadence
# Portfolio slots: the bot may hold multiple contracts at once, but each
# contract gets only one ENTERING/ACTIVE/EXITING slot.  The existing BTC-style
# risk model is preserved by giving each contract a slot-scoped balance view
# before QuantStrategy applies RISK_PER_TRADE and BALANCE_USAGE_PERCENTAGE.
PORTFOLIO_MAX_OPEN_POSITIONS = 6
PORTFOLIO_MAX_OPEN_PER_CONTRACT = 1
PORTFOLIO_MAX_OPEN_PER_ASSET_CLASS = 6
PORTFOLIO_BUDGET_MODE = "equal_slots"   # equal_slots | active_equal_slots
# In multi-asset mode, margin/cash is slot-scoped but dollar-risk must remain
# portfolio-aware.  This prevents BTC min-lot rejection when a valid minimum
# order is inside the portfolio risk cap but above the confidence-haircut target.
PORTFOLIO_RISK_BUDGET_MODE = "portfolio_equity"  # portfolio_equity | slot_equity
PORTFOLIO_MIN_LOT_MAX_RISK_MULT = 1.15
PORTFOLIO_MAX_AGGREGATE_RISK_PCT = 3.0  # six slots × 0.5% risk; aggregate account risk cap
PORTFOLIO_REPORT_CAPITAL_WEIGHTED_METRICS = True

# Agentic institutional fund runtime.
# Agents select which desks may run entry logic. QuantStrategy still owns alpha
# validation; Risk/Execution remain deterministic final authorities.
AGENTIC_FUND_ENABLED = _env_bool("AGENTIC_FUND_ENABLED", True)
FUND_PAPER_MODE = _env_bool("FUND_PAPER_MODE", True)
FUND_LIVE_ORDERING_ENABLED = _env_bool("FUND_LIVE_ORDERING_ENABLED", False)
FUND_TOP_N_EXECUTION_DESKS = _env_int("FUND_TOP_N_EXECUTION_DESKS", 0)
FUND_TOP_N_DEPTH_SCAN = _env_int("FUND_TOP_N_DEPTH_SCAN", 0)
FUND_MIN_TICKER_SCORE = _env_float("FUND_MIN_TICKER_SCORE", 0.52)
FUND_MIN_EXECUTION_SCORE = _env_float("FUND_MIN_EXECUTION_SCORE", 0.58)
FUND_MIN_SETUP_SCORE = _env_float("FUND_MIN_SETUP_SCORE", 0.50)
FUND_MAX_SPREAD_BPS_CRYPTO = _env_float("FUND_MAX_SPREAD_BPS_CRYPTO", 18.0)
FUND_MAX_SPREAD_BPS_EQUITY = _env_float("FUND_MAX_SPREAD_BPS_EQUITY", 45.0)
FUND_MAX_SPREAD_BPS_COMMODITY = _env_float("FUND_MAX_SPREAD_BPS_COMMODITY", 55.0)
FUND_MAX_SPREAD_BPS_OPTION = _env_float("FUND_MAX_SPREAD_BPS_OPTION", 120.0)
FUND_MAX_SPREAD_BPS_FUTURE = _env_float("FUND_MAX_SPREAD_BPS_FUTURE", 65.0)
FUND_MAX_DATA_AGE_SEC = _env_float("FUND_MAX_DATA_AGE_SEC", 90.0)
FUND_MIN_WARMUP_RATIO = _env_float("FUND_MIN_WARMUP_RATIO", 0.88)
FUND_AUDIT_LOG_PATH = os.getenv("FUND_AUDIT_LOG_PATH", "data/fund_audit.jsonl")
FUND_CIO_DECISION_SEC = _env_float("FUND_CIO_DECISION_SEC", 3.0)
FUND_CIO_REPORT_SEC = _env_float("FUND_CIO_REPORT_SEC", 30.0)

# Optional venues. Delta/CoinSwitch are the current execution stack. ICICI and
# CoinDCX are institutional adapters; enable only after credentials, static IP,
# and paper-mode validation are complete.
ICICI_ENABLED = _env_bool("ICICI_ENABLED", False)
BREEZE_API_KEY = os.getenv("BREEZE_API_KEY", "")
BREEZE_SECRET_KEY = os.getenv("BREEZE_SECRET_KEY", "")
ICICI_CLIENT_ID = os.getenv("ICICI_CLIENT_ID", "")
ICICI_PASSWORD = os.getenv("ICICI_PASSWORD", "")
ICICI_API_SESSION_PATH = os.getenv("ICICI_API_SESSION_PATH", "data/icici_api_session.txt")
# Optional manual tokens. Prefer BREEZE_API_SESSION/ICICI_API_SESSION because
# CustomerDetails then returns the signed-request session_token.
BREEZE_API_SESSION = os.getenv("BREEZE_API_SESSION", os.getenv("ICICI_API_SESSION", ""))
BREEZE_SESSION_TOKEN = os.getenv("BREEZE_SESSION_TOKEN", os.getenv("ICICI_SESSION_TOKEN", ""))
ICICI_SESSION_CACHE_PATH = os.getenv("ICICI_SESSION_CACHE_PATH", "data/icici_breeze_session.json")
ICICI_SESSION_TTL_SEC = float(os.getenv("ICICI_SESSION_TTL_SEC", str(6 * 60 * 60)))
ICICI_DEBUG_DIR = os.getenv("ICICI_DEBUG_DIR", "data/icici_debug")
ICICI_BREEZE_PREFLIGHT_ON_STARTUP = _env_bool("ICICI_BREEZE_PREFLIGHT_ON_STARTUP", True)
# When the bot is started from Telegram and a Breeze API_Session is missing,
# launch the token-generator browser flow automatically, ask the approved
# Telegram operator for OTP, scrape the API_Session from the redirect, then
# exchange it through CustomerDetails before ICICI protected endpoints are used.
ICICI_AUTO_TOKEN_GENERATOR_ON_STARTUP = _env_bool("ICICI_AUTO_TOKEN_GENERATOR_ON_STARTUP", True)
ICICI_TOKEN_GENERATOR_HEADLESS = _env_bool("ICICI_TOKEN_GENERATOR_HEADLESS", True)
ICICI_PLAYWRIGHT_AUTO_INSTALL = _env_bool("ICICI_PLAYWRIGHT_AUTO_INSTALL", True)
ICICI_OTP_WAIT_SEC = _env_float("ICICI_OTP_WAIT_SEC", 180.0)
ICICI_AUTH_REQUIRED_FOR_DETAILS = _env_bool("ICICI_AUTH_REQUIRED_FOR_DETAILS", False)

COINDCX_ENABLED = _env_bool("COINDCX_ENABLED", False)
COINDCX_API_KEY = os.getenv("COINDCX_API_KEY", "")
COINDCX_SECRET_KEY = os.getenv("COINDCX_SECRET_KEY", "")

# Dynamic universe discovery. The scanner reads live venue catalogs by default;
# this list is intentionally empty so old fixed baskets cannot cap coverage.
UNIVERSE_DISCOVERY_MODE = os.getenv("UNIVERSE_DISCOVERY_MODE", "dynamic").lower()
UNIVERSE_INCLUDE_EXCHANGES = os.getenv("UNIVERSE_INCLUDE_EXCHANGES", "delta,coinswitch,icici,coindcx")
UNIVERSE_INCLUDE_ASSET_CLASSES = os.getenv("UNIVERSE_INCLUDE_ASSET_CLASSES", "crypto,commodity,index,equity,option,future,cash")
MULTI_ASSET_REQUESTS = []
DISCOVERY_REPORT_PREVIEW = _env_int("DISCOVERY_REPORT_PREVIEW", 80)
ICICI_DISCOVERY_ENABLED = _env_bool("ICICI_DISCOVERY_ENABLED", True)
ICICI_SECURITY_MASTER_URL = os.getenv("ICICI_SECURITY_MASTER_URL", "http://directlink.icicidirect.com/NewSecurityMaster/SecurityMaster.zip")


# ─────────────────────────────────────────────────────────────────────────────
# v9 Institutional multi-asset runtime policy
# Centralised per-asset policy removes BTC-config leakage into xStocks/metals.
SCANNER_START_PARALLELISM = 4
SCANNER_POSITION_TICK_SEC = 0.25
PORTFOLIO_BALANCE_CACHE_TTL_SEC = 2.0

POLICY_CRYPTO_RISK_MULT = 1.00
POLICY_CRYPTO_LOOP_INTERVAL_SEC = 0.25

POLICY_COMMODITY_RISK_MULT = 0.70
POLICY_COMMODITY_MARGIN_PCT = 0.14
POLICY_COMMODITY_MIN_MARGIN_USD = 0.50
POLICY_COMMODITY_TICK_EVAL_SEC = 0.50
POLICY_COMMODITY_LOOP_INTERVAL_SEC = 0.50
POLICY_COMMODITY_MIN_1M_BARS = 85
POLICY_COMMODITY_MIN_5M_BARS = 65
POLICY_COMMODITY_MIN_RR = 1.85
POLICY_COMMODITY_MAX_RR = 5.0
POLICY_COMMODITY_MAX_HOLD_SEC = 4800
POLICY_COMMODITY_COOLDOWN_SEC = 210
POLICY_COMMODITY_SL_BUFFER_ATR = 0.50

POLICY_EQUITY_RISK_MULT = 0.55
POLICY_EQUITY_MARGIN_PCT = 0.12
POLICY_EQUITY_MIN_MARGIN_USD = 0.50
POLICY_EQUITY_TICK_EVAL_SEC = 0.75
POLICY_EQUITY_LOOP_INTERVAL_SEC = 0.75
POLICY_EQUITY_MIN_1M_BARS = 90
POLICY_EQUITY_MIN_5M_BARS = 70
POLICY_EQUITY_MIN_RR = 1.75
POLICY_EQUITY_MAX_RR = 5.0
POLICY_EQUITY_MAX_HOLD_SEC = 5400
POLICY_EQUITY_COOLDOWN_SEC = 180
POLICY_EQUITY_SL_BUFFER_ATR = 0.55

# v13 multi-asset protection invariants
# A Delta bracket fill is not considered safe until the bot verifies the SL and
# TP children belong to the SAME product and are near the intended SL/TP prices.
DELTA_BRACKET_CHILD_VERIFY_TIMEOUT_SEC = 18.0
DELTA_BRACKET_CHILD_PRICE_TOL_TICKS = 6.0
DELTA_BRACKET_CHILD_PRICE_TOL_PCT = 0.0025
DELTA_EMERGENCY_FLATTEN_ON_BRACKET_MISMATCH = True
TELEGRAM_ALERT_PROTECTION_FAILURE = True

# Dashboard telemetry: non-blocking direct feed to local dashboard backend.
# If the dashboard is offline, events are dropped; trading is never blocked.
DASHBOARD_ENABLED = os.getenv("DASHBOARD_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
DASHBOARD_URL = os.getenv("DASHBOARD_URL", "http://127.0.0.1:8000")
DASHBOARD_QUEUE_MAX = int(os.getenv("DASHBOARD_QUEUE_MAX", "2000"))
DASHBOARD_TIMEOUT_SEC = float(os.getenv("DASHBOARD_TIMEOUT_SEC", "0.8"))
DASHBOARD_HEARTBEAT_SEC = float(os.getenv("DASHBOARD_HEARTBEAT_SEC", "5"))
DASHBOARD_SCAN_UPDATE_SEC = float(os.getenv("DASHBOARD_SCAN_UPDATE_SEC", "5"))
DASHBOARD_POSITION_UPDATE_SEC = float(os.getenv("DASHBOARD_POSITION_UPDATE_SEC", "1"))

# ─────────────────────────────────────────────────────────────────────────────
# v80 DYNAMIC INSTITUTIONAL TRADABLE-SELECTION DESK
# ─────────────────────────────────────────────────────────────────────────────
# Discovery can see the full venue catalog, but live candle/orderbook/trade
# subscriptions are opened only for the desk-selected shortlist.  This prevents
# Delta from being flooded with 100+ simultaneous candle streams at startup.
DYNAMIC_TRADABLE_DESK_ENABLED = _env_bool("DYNAMIC_TRADABLE_DESK_ENABLED", True)
DYNAMIC_DESK_MAX_ACTIVE_CONTEXTS = _env_int("DYNAMIC_DESK_MAX_ACTIVE_CONTEXTS", 12)
DYNAMIC_DESK_MIN_SCORE = _env_float("DYNAMIC_DESK_MIN_SCORE", 0.38)
DYNAMIC_DESK_REFRESH_SEC = _env_float("DYNAMIC_DESK_REFRESH_SEC", 180.0)
DYNAMIC_DESK_MIN_RESIDENCY_SEC = _env_float("DYNAMIC_DESK_MIN_RESIDENCY_SEC", 600.0)
DYNAMIC_DESK_DELTA_BULK_TICKERS = _env_bool("DYNAMIC_DESK_DELTA_BULK_TICKERS", True)
DYNAMIC_DESK_ICICI_DETAILS_ENABLED = _env_bool("DYNAMIC_DESK_ICICI_DETAILS_ENABLED", True)
DYNAMIC_DESK_ICICI_QUOTE_PROBES = _env_int("DYNAMIC_DESK_ICICI_QUOTE_PROBES", 8)
DYNAMIC_DESK_ALWAYS_INCLUDE = os.getenv("DYNAMIC_DESK_ALWAYS_INCLUDE", "")
DYNAMIC_DESK_LOG_TOP_N = _env_int("DYNAMIC_DESK_LOG_TOP_N", 12)

# Candle streaming policy for activated desks only.  HTF candles may still be
# warmed through REST, but live WS candles should stay lean.  Empty value means
# use the DeltaDataManager default map.
DELTA_ACTIVE_CANDLE_STREAMS = os.getenv("DELTA_ACTIVE_CANDLE_STREAMS", "1m,5m,15m,1h")
DELTA_REST_WARMUP_TIMEFRAMES = os.getenv("DELTA_REST_WARMUP_TIMEFRAMES", "1m,5m,15m,1h,4h,1d")

# ICICI security master cache.  Discovery should not fail just because the
# public master URL is slow/unreachable during startup.
ICICI_SECURITY_MASTER_CACHE_PATH = os.getenv("ICICI_SECURITY_MASTER_CACHE_PATH", "data/icici_security_master.zip")

# ─────────────────────────────────────────────────────────────────────────────
# v85 ASSET-DESK + VENUE-ROUTER INSTITUTIONAL ARCHITECTURE
# ─────────────────────────────────────────────────────────────────────────────
# Alpha desks are organised by asset/instrument thesis, not by broker venue.
# Example: BTC is one global BTC desk; Delta/CoinSwitch/CoinDCX are venue routes
# under that desk. Venue fragmentation must not create duplicate BTC strategies.
DESK_ENABLED_IDS = os.getenv("DESK_ENABLED_IDS", "")
DESK_MAX_ACTIVE_BY_ID = os.getenv("DESK_MAX_ACTIVE_BY_ID", "")
DESK_BTC_GLOBAL_MAX_ACTIVE = _env_int("DESK_BTC_GLOBAL_MAX_ACTIVE", 1)
DESK_CRYPTO_ALTS_MAX_ACTIVE = _env_int("DESK_CRYPTO_ALTS_MAX_ACTIVE", 7)
DESK_US_STOCK_DERIVATIVES_MAX_ACTIVE = _env_int("DESK_US_STOCK_DERIVATIVES_MAX_ACTIVE", 2)
DESK_COMMODITIES_GLOBAL_MAX_ACTIVE = _env_int("DESK_COMMODITIES_GLOBAL_MAX_ACTIVE", 2)
DESK_ICICI_INDEX_OPTIONS_MAX_ACTIVE = _env_int("DESK_ICICI_INDEX_OPTIONS_MAX_ACTIVE", 4)
DESK_ICICI_STOCK_OPTIONS_MAX_ACTIVE = _env_int("DESK_ICICI_STOCK_OPTIONS_MAX_ACTIVE", 6)
VENUE_ROUTE_PREFERENCE = os.getenv("VENUE_ROUTE_PREFERENCE", "delta,coindcx,coinswitch,icici")

# Backward-compatible env aliases only. Do not use these names in new logic;
# they are preserved so old .env files don't crash while v85 migrates configs.
DESK_DELTA_BTC_MAX_ACTIVE = DESK_BTC_GLOBAL_MAX_ACTIVE
DESK_DELTA_CRYPTO_MAX_ACTIVE = DESK_CRYPTO_ALTS_MAX_ACTIVE
DESK_DELTA_US_STOCKS_MAX_ACTIVE = DESK_US_STOCK_DERIVATIVES_MAX_ACTIVE
DESK_DELTA_COMMODITIES_MAX_ACTIVE = DESK_COMMODITIES_GLOBAL_MAX_ACTIVE
DESK_COINSWITCH_BTC_MAX_ACTIVE = 0
DESK_COINSWITCH_CRYPTO_MAX_ACTIVE = 0
DESK_COINDCX_BTC_MAX_ACTIVE = 0
DESK_COINDCX_CRYPTO_MAX_ACTIVE = 0

# Indian market mandate: only NFO/BFO options are eligible. Cash/futures rows may
# be discovered for reference but are rejected before runtime subscription.
ICICI_OPTIONS_ONLY = _env_bool("ICICI_OPTIONS_ONLY", True)
ICICI_OPTIONS_RUNTIME_ENABLED = _env_bool("ICICI_OPTIONS_RUNTIME_ENABLED", True)
ICICI_OPTION_MIN_DTE = _env_float("ICICI_OPTION_MIN_DTE", 2.0)
ICICI_OPTION_MAX_DTE = _env_float("ICICI_OPTION_MAX_DTE", 21.0)
ICICI_OPTION_MAX_THETA_TO_PREMIUM = _env_float("ICICI_OPTION_MAX_THETA_TO_PREMIUM", 0.08)
ICICI_OPTION_MAX_SPREAD_BPS = _env_float("ICICI_OPTION_MAX_SPREAD_BPS", 180.0)
ICICI_INDEX_OPTION_TARGET_ABS_DELTA = _env_float("ICICI_INDEX_OPTION_TARGET_ABS_DELTA", 0.45)
ICICI_STOCK_OPTION_TARGET_ABS_DELTA = _env_float("ICICI_STOCK_OPTION_TARGET_ABS_DELTA", 0.50)
ICICI_OPTION_DELTA_BAND = _env_float("ICICI_OPTION_DELTA_BAND", 0.22)
ICICI_OPTION_IV_STRESS_PRIOR = _env_float("ICICI_OPTION_IV_STRESS_PRIOR", 0.24)
INDIA_RISK_FREE_RATE = _env_float("INDIA_RISK_FREE_RATE", 0.065)

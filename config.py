"""
config.py — Unified Configuration v10.0
=========================================
Single source of truth. All institutional parameters inline.
No config_overrides.py — everything lives here.

Risk-controlled configuration for structural ICT/Liquidity execution.
"""
import os
try:
    from dotenv import load_dotenv
except ImportError:  # production image may not ship python-dotenv
    def load_dotenv(*_a, **_kw):
        return False
load_dotenv()

# ── Exchange routing ──────────────────────────────────────────────────────────
EXECUTION_EXCHANGE = os.getenv("EXECUTION_EXCHANGE", "delta").lower()

# ── Credentials ───────────────────────────────────────────────────────────────
DELTA_API_KEY             = os.getenv("DELTA_API_KEY",    "")
DELTA_SECRET_KEY          = os.getenv("DELTA_SECRET_KEY", "")
DELTA_TESTNET             = os.getenv("DELTA_TESTNET", "false").lower() == "true"
COINSWITCH_API_KEY        = os.getenv("COINSWITCH_API_KEY",    "")
COINSWITCH_SECRET_KEY     = os.getenv("COINSWITCH_SECRET_KEY", "")

def _first_env(*names: str) -> str:
    for name in names:
        value = os.getenv(name, "")
        if str(value or "").strip():
            return str(value).strip()
    return ""

BREEZE_API_KEY = _first_env(
    "BREEZE_API_KEY",
    "ICICI_API_KEY",
    "ICICI_BREEZE_API_KEY",
    "BREEZE_APP_KEY",
    "ICICI_APP_KEY",
)
BREEZE_SECRET_KEY = _first_env(
    "BREEZE_SECRET_KEY",
    "ICICI_SECRET_KEY",
    "ICICI_API_SECRET",
    "BREEZE_API_SECRET",
    "BREEZE_SECRET",
    "ICICI_BREEZE_SECRET_KEY",
    "ICICI_APP_SECRET",
)
ICICI_CLIENT_ID           = os.getenv("ICICI_CLIENT_ID", "")
ICICI_PASSWORD            = os.getenv("ICICI_PASSWORD", "")
ICICI_ENABLED             = os.getenv("ICICI_ENABLED", "true" if BREEZE_API_KEY else "false").lower() in ("1", "true", "yes", "on")
TELEGRAM_BOT_TOKEN        = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID          = os.getenv("TELEGRAM_CHAT_ID",   "")

if not DELTA_API_KEY and not COINSWITCH_API_KEY and not BREEZE_API_KEY:
    raise ValueError("No exchange credentials in .env. Set DELTA_API_KEY, COINSWITCH_API_KEY, or BREEZE_API_KEY.")

# ── Symbol / Leverage ─────────────────────────────────────────────────────────
SYMBOL                   = "BTCUSDT"
LEVERAGE                 = 45
DELTA_SYMBOL             = "BTCUSD"
DELTA_CONTRACT_VALUE_BTC = 0.001
DELTA_BALANCE_CURRENCY   = "USD"
COINSWITCH_SYMBOL        = "BTCUSDT"
COINSWITCH_EXCHANGE      = "EXCHANGE_2"

# ── Position sizing ───────────────────────────────────────────────────────────
MIN_MARGIN_PER_TRADE     = 0       # 0 = no arbitrary dollar floor; exchange min_qty/step controls executability
MIN_POSITION_SIZE        = 0.001
MAX_POSITION_SIZE        = 100.0
LOT_STEP_SIZE            = 0.001
REMAINDER_MIN_QTY        = 0.001

# ── Risk management ──────────────────────────────────────────────────────────
# RISK_PER_TRADE: FRACTION of available balance risked per trade (NOT percent).
#   0.05 = 5.0% risk per trade.
#   Previous value 0.60 was interpreted as percent by risk_manager (÷100 = 0.006 → 0.6%)
#   but as FRACTION by quant_strategy._compute_quantity (× direct = 0.60 → 60%).
#   The inconsistency caused 100× over-sizing (entire balance at risk per trade),
#   triggering the "required margin > available — scaling down" warnings in logs.
#   Fix: one convention (fraction), both consumers agree. See risk_manager.py line 266.
RISK_PER_TRADE           = 0.025  # 2.5% stop-loss risk ceiling; aggressive mode still bounded by SL, margin and liquidation safety
MAX_DAILY_LOSS           = 10000
MAX_DAILY_LOSS_PCT       = 10.0      # day circuit breaker
MAX_DRAWDOWN_PCT         = 25.0      # portfolio circuit breaker
MAX_CONSECUTIVE_LOSSES   = 4
ALLOW_TIME_BASED_CONSEC_LOSS_RESET = False
CONSEC_LOSS_AUTO_RESET_HOURS = 1.0
MAX_DAILY_TRADES         = 12
ONE_POSITION_AT_A_TIME   = True
MIN_TIME_BETWEEN_TRADES_SEC = 180.0
TRADE_COOLDOWN_SECONDS   = 180
MIN_RISK_REWARD_RATIO    = 2.00      # structural R:R floor; execution costs shape size, not thesis existence
TARGET_RISK_REWARD_RATIO = 3.00
MAX_RR_RATIO             = 20.0


# ── Order execution ───────────────────────────────────────────────────────────
TICK_SIZE                        = 0.5 if EXECUTION_EXCHANGE == "delta" else 0.1
TICK_SIZE_DELTA                  = 0.5
TICK_SIZE_COINSWITCH             = 0.1
TICK_SIZE_ICICI                  = 0.05
LIMIT_ORDER_OFFSET_TICKS         = 3
ORDER_TIMEOUT_SECONDS            = 600
MAX_ORDER_RETRIES                = 2
MAX_CONSECUTIVE_TIMEOUTS         = 2
TIMEOUT_EXTENDED_LOCKOUT_SEC     = 1800
SNIPER_MAX_DISTANCE_ATR          = 1.0
LIMIT_ORDER_FILL_TIMEOUT_SEC     = 60.0
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


# ── Health / Supervisor ───────────────────────────────────────────────────────
WS_STALE_SECONDS                   = 35.0
HEALTH_CHECK_INTERVAL_SEC          = 12.0
PRICE_STALE_SECONDS                = 90.0
DATA_INTEGRITY_REQUIRE_FRESH_QUOTES = True
DATA_INTEGRITY_MAX_CLOSED_BAR_AGE_MULT = 3.25
DATA_INTEGRITY_MAX_GAP_MULT_CONTINUOUS = 2.25
DATA_INTEGRITY_LOG_SEC = 60.0
EXECUTION_BOOK_MAX_STALE_SEC = 5.0
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
FEE_TO_RISK_NO_ALLOC        = 1.25

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
    if ex == "icici":
        return float(TICK_SIZE_ICICI)
    return float(TICK_SIZE)


def validate_config() -> None:
    """Fail fast on inconsistent trading-risk configuration."""
    errors = []
    if MIN_RISK_REWARD_RATIO < 1.5:
        errors.append("MIN_RISK_REWARD_RATIO must be >= 1.5")
    if get_tick_size() <= 0:
        errors.append("tick size must be positive")
    if MAX_DAILY_LOSS_PCT <= 0 or MAX_DAILY_LOSS_PCT > 10:
        errors.append("MAX_DAILY_LOSS_PCT must be in (0, 10]")
    if errors:
        raise ValueError("Invalid config: " + "; ".join(errors))


SL_BUFFER_TICKS          = 5
SL_ATR_PERIOD            = 14
# ICT structural invalidation: the protective stop remains beyond the raided wick.
# These are risk-model parameters, surfaced in ICT_GEOMETRY logs and used directly
# by strategy.entry_engine; they are not trailing-stop or alpha filters.
ICT_STOP_CLEARANCE_BASE_ATR = 0.10
ICT_STOP_CLEARANCE_PCTL_SLOPE_ATR = 0.18

# ── Aggregator ────────────────────────────────────────────────────────────────
AGG_PRIMARY_WEIGHT   = 0.55
AGG_SECONDARY_WEIGHT = 0.45
AGG_OB_DEPTH_LEVELS  = 10
AGG_TRADE_WINDOW_SEC = 30.0

# ── Quant Strategy ────────────────────────────────────────────────────────────
QUANT_MARGIN_PCT               = 0.50
QUANT_SLIPPAGE_TOLERANCE       = 0.0005
QUANT_SL_BUFFER_ATR_MULT       = 0.4
QUANT_ATR_PCTILE_WINDOW        = 100
QUANT_ATR_MIN_PCTILE           = 0.05
QUANT_ATR_MAX_PCTILE           = 0.97
QUANT_MAX_HOLD_SEC             = 3600      # 60 min max hold
QUANT_LOSS_LOCKOUT_SEC         = 600       # max-consecutive-loss lockout
QUANT_LOCKOUT_AFTER_LOSS_SEC   = 180       # short post-loss auction reset, not a no-trade freeze
QUANT_POS_SYNC_SEC             = 30
RECONCILE_EXIT_SETTLE_SEC = 15.0      # protect exact-fill accounting from stale position feed after a local close


# ─────────────────────────────────────────────────────────────────────────────
# Six-lens gate that replaces the flat 30s cooldown with regime-aware logic.
# Goal: stop the "exit → re-enter in 30s → take another stop" failure mode.
#
# Each constant tunes one lens. Defaults are conservative for BTC perps on a
# 1m/5m/15m liquidity-first stack; relax with caution.

QUANT_TP_MAX_RR                = 3.5
QUANT_MAX_SPREAD_ATR_RATIO     = 2.50
# Asset-aware spread gate (v8): xStock/RWA products have coarse tick geometry;
# a normal 1-4 tick spread can exceed the current 5m ATR in quiet windows.
# These caps hard-block genuinely broken books while converting normal wide
# tokenised-equity spreads into allocation haircuts handled by sizing/EV.
QUANT_SPREAD_SOFT_ATR_RATIO_CRYPTO    = 0.40
QUANT_MAX_SPREAD_BPS_CRYPTO           = 40.0
QUANT_MAX_SPREAD_TICKS_CRYPTO         = 30.0
QUANT_SPREAD_SOFT_ATR_RATIO_EQUITY    = 0.50
QUANT_MAX_SPREAD_ATR_RATIO_EQUITY     = 4.00
QUANT_MAX_SPREAD_BPS_EQUITY           = 35.0
QUANT_MAX_SPREAD_TICKS_EQUITY         = 8.0
QUANT_SPREAD_SOFT_ATR_RATIO_COMMODITY = 0.60
QUANT_MAX_SPREAD_ATR_RATIO_COMMODITY  = 2.50
QUANT_MAX_SPREAD_BPS_COMMODITY        = 60.0
QUANT_MAX_SPREAD_TICKS_COMMODITY      = 30.0
QUANT_SPREAD_MIN_SIZE_MULT            = 0.70
QUANT_SPREAD_SIZE_HAIRCUT_MAX         = 0.30

# Institutional selectivity mode:
# The engine still waits for actual raid/MSS/FVG/liquidity structure, but weak
# context, counter-delivery raids, adverse fresh flow and low-rank targets do
# not receive capital. These are execution gates, not win-rate guarantees.
ICT_SELECTIVITY_MODE = True
ICT_ALLOW_COUNTER_DELIVERY_RAIDS = False
ICT_ENTRY_MIN_CONTEXT_DELIVERY_SCORE = 0.25
ICT_ENTRY_MIN_DELIVERY_SCORE = 0.50
ICT_ENTRY_MIN_TARGET_RANK_SCORE = 1.25
ICT_ENTRY_MIN_DISPLACEMENT_ATR_RAID = 1.35
ICT_ENTRY_MIN_DISPLACEMENT_ATR_CONTINUATION = 1.50
ICT_CONTINUATION_MIN_CONTEXT_SCORE = 0.35
ICT_CONTINUATION_MIN_DELIVERY_SCORE = 0.60
ICT_CONTINUATION_MIN_TARGET_RANK_SCORE = 2.60
ICT_PARTIAL_HTF_MIN_DELIVERY_SCORE = 0.62
ICT_PARTIAL_HTF_MIN_DISPLACEMENT_ATR = 2.00
ICT_MICROSTRUCTURE_GUARD_ENABLED = True
ICT_MIN_FRESH_MICROSTRUCTURE_SCORE = -0.20
ICT_MIN_TARGET_REALISM_SCORE = 0.60
ICT_SETUP_DOSSIER_ENABLED = True
ICT_SETUP_DOSSIER_MIN_SCORE = 0.60
ICT_SETUP_DOSSIER_RANK_NORM = 3.0
ICT_THESIS_MAX_AGE_SEC = 900.0
ICT_CONTINUATION_THESIS_MAX_AGE_SEC = 600.0
ICT_MAX_FVG_EXTENSION_BEFORE_REPRICE_ATR = 3.25
ICT_PD_ARRAY_GUARD_ENABLED = True
ICT_PD_ARRAY_MIN_SCORE = 0.58
ICT_PD_ARRAY_STRICT_PREMIUM_DISCOUNT = True
ICT_DEALING_RANGE_LOOKBACK_15M = 48
ICT_OTE_MIN_RETRACEMENT = 0.50
ICT_OTE_MAX_RETRACEMENT = 0.79
ICT_PD_KILLZONE_WEIGHT_ENABLED = True

# Time-decay exit: use the configured desk max-hold as a thesis half-life.
# Healthy winners can still reach exchange targets; dead auctions are flattened
# before they burn the full structural stop by time alone.
QUANT_TIME_STOP_ENABLED = True
QUANT_TIME_STOP_EARLY_FRACTION = 0.55
QUANT_TIME_STOP_FAILED_AUCTION_R = -0.35
QUANT_TIME_STOP_FAILED_AUCTION_MAX_MFE_R = 0.50
QUANT_TIME_STOP_MIN_PROGRESS_R = 0.20
QUANT_TIME_STOP_HARD_MAX_MULT = 1.35

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

# ── HTF context ───────────────────────────────────────────────────────────────



# ── Institutional Dynamic Entry Quality References ───────────────────────────
# Structural entry thresholds are owned only by the ICT/Liquidity engine.





# ── CHoCH expiry ──────────────────────────────────────────────────────────────

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

# ── Exact execution reconciliation ───────────────────────────────────────
# A broker-supervised liquidity exit is booked only after its own tracked
# reduce-only close resolves to an exact fill; unresolved propagation cannot
# create an artificial realised result.
EXIT_MANUAL_CONFIRM_MAX_WAIT_SEC = 120.0

# ─────────────────────────────────────────────────────────────────────────────
# MULTI-ASSET LIVE CATALOG SCANNER
# ─────────────────────────────────────────────────────────────────────────────
# The scanner does NOT trade aliases.  It queries Delta/CoinSwitch live product
# catalogs and activates only contracts actually returned by the exchange.
MULTI_ASSET_ENABLED = True
SCANNER_MAX_ACTIVE_INSTRUMENTS = 14
SCANNER_TICK_SLEEP_SEC = 0.25
# Observability cadence: decision transitions log immediately in QuantStrategy; these
# slow cadences only prove scanner health and suppress repetitive per-tick noise.
ICT_DECISION_SNAPSHOT_SEC = 60.0
SCANNER_ASSET_HEARTBEAT_SEC = 300.0
SCANNER_ASSET_ANALYSIS_LOG_SEC = 60.0  # execution-health audit, not alpha reasoning

# Desk suspension policy.  Suspended desks are excluded before live catalog
# discovery, so they cannot subscribe, analyse, size, or route orders.
# This is a desk-level trading suspension, not a removal of instrument metadata.
STOCK_DESK_TRADING_ENABLED = False
SUSPENDED_TRADING_DESKS = ("STOCKS",)
SUSPENDED_ASSET_CLASSES = ("equity", "index")

# ICICI / Indian index options desk. Only long premium options are routed:
# bullish NIFTY thesis -> buy CE, bearish NIFTY thesis -> buy PE.
ICICI_LONG_PREMIUM_ONLY = True
ICICI_OPTIONS_ONLY = True
# Discovery is config-backed and auth-independent, matching the working V83
# ICICI desk design: NIFTY must enter the universe before Breeze session
# generation. Protected Breeze endpoints are touched later by the ICICI runtime
# data/execution adapters.
ICICI_DISCOVERY_ENABLED = os.getenv("ICICI_DISCOVERY_ENABLED", "true").lower() in ("1", "true", "yes", "on")
ICICI_OPTIONS_RUNTIME_ENABLED = ICICI_ENABLED
ICICI_INDEX_OPTIONS_FROM_CONFIG_ONLY = True
# Official Breeze stock_code for NIFTY 50 is NIFTY. Keep common aliases only in
# the mapping layer; do not let .env override the institutional desk universe.
ICICI_INDEX_UNDERLYINGS = "NIFTY"
# Breeze's documented NIFTY 50 code is stock_code="NIFTY".  Accept common
# aliases in config, but always route Breeze underlying/option-chain calls with
# the ICICI stock_code expected by historicalcharts/OptionChain.
ICICI_INDEX_BREEZE_STOCK_CODE_BY_UNDERLYING = {
    "NIFTY": "NIFTY",
    "NIFTY50": "NIFTY",
    "CNXNIFTY": "NIFTY",
}
ICICI_API_SESSION_PATH = os.getenv("ICICI_API_SESSION_PATH", "data/icici_api_session.txt")
BREEZE_API_SESSION = os.getenv("BREEZE_API_SESSION", os.getenv("ICICI_API_SESSION", ""))
BREEZE_SESSION_TOKEN = os.getenv("BREEZE_SESSION_TOKEN", "")
ICICI_ALLOW_MANUAL_SESSION_TOKEN_OVERRIDE = os.getenv("ICICI_ALLOW_MANUAL_SESSION_TOKEN_OVERRIDE", "false").lower() in ("1", "true", "yes", "on")
ICICI_API_SESSION_FILE_MUST_BE_TODAY = os.getenv("ICICI_API_SESSION_FILE_MUST_BE_TODAY", "true").lower() in ("1", "true", "yes", "on")
ICICI_SESSION_CACHE_PATH = os.getenv("ICICI_SESSION_CACHE_PATH", "data/icici_breeze_session.json")
ICICI_SESSION_TTL_SEC = 6 * 60 * 60
ICICI_SESSION_EXPIRES_DAILY = True
ICICI_SESSION_TIMEZONE_OFFSET_MIN = 330
# Telegram /start must generate/validate the Breeze session before ICICI
# data managers touch protected Breeze endpoints. API_Session/SessionToken
# are runtime artifacts, not .env requirements.
ICICI_BREEZE_PREFLIGHT_ON_STARTUP = True
ICICI_AUTO_TOKEN_GENERATOR_ON_STARTUP = True
ICICI_AUTH_REQUIRED_FOR_DETAILS = True
ICICI_TOKEN_GENERATOR_HEADLESS = os.getenv("ICICI_TOKEN_GENERATOR_HEADLESS", "true").lower() in ("1", "true", "yes", "on")
ICICI_PLAYWRIGHT_AUTO_INSTALL = os.getenv("ICICI_PLAYWRIGHT_AUTO_INSTALL", "true").lower() in ("1", "true", "yes", "on")
ICICI_OTP_WAIT_SEC = 180.0
ICICI_STARTUP_TOKEN_WAIT_SEC = 300.0
ICICI_PREMARKET_TOKEN_REFRESH_ENABLED = os.getenv("ICICI_PREMARKET_TOKEN_REFRESH_ENABLED", "true").lower() in ("1", "true", "yes", "on")
ICICI_PREMARKET_TOKEN_REFRESH_TIME = os.getenv("ICICI_PREMARKET_TOKEN_REFRESH_TIME", "08:30")
ICICI_PREMARKET_TOKEN_REFRESH_WINDOW_MIN = float(os.getenv("ICICI_PREMARKET_TOKEN_REFRESH_WINDOW_MIN", "90.0"))
ICICI_DORMANT_START_RETRY_SEC = float(os.getenv("ICICI_DORMANT_START_RETRY_SEC", "30.0"))
ICICI_FAILED_START_RETRY_SEC = float(os.getenv("ICICI_FAILED_START_RETRY_SEC", "180.0"))
ICICI_DEBUG_DIR = os.getenv("ICICI_DEBUG_DIR", "data/icici_debug")
ICICI_MARKET_SESSION_GUARD_ENABLED = True
ICICI_MARKET_OPEN_TIME = "09:15"
ICICI_MARKET_CLOSE_TIME = "15:30"
ICICI_MARKET_HOLIDAYS = tuple(x.strip() for x in os.getenv("ICICI_MARKET_HOLIDAYS", "").split(",") if x.strip())
ICICI_ANALYZE_ONLY_DURING_MARKET_SESSION = os.getenv("ICICI_ANALYZE_ONLY_DURING_MARKET_SESSION", "true").lower() in ("1", "true", "yes", "on")
ICICI_BREEZE_THROTTLE_ENABLED = True
# Breeze docs publish 100 calls/minute account-wide; keep a small buffer under
# that ceiling because option-chain, quote fallback and historical warmups share
# the same API key.
ICICI_BREEZE_MIN_CALL_GAP_SEC = 0.65
ICICI_SECURITY_MASTER_CACHE_PATH = os.getenv("ICICI_SECURITY_MASTER_CACHE_PATH", "data/icici_security_master.zip")
ICICI_SECURITY_MASTER_URL = os.getenv("ICICI_SECURITY_MASTER_URL", "https://directlink.icicidirect.com/NewSecurityMaster/SecurityMaster.zip")
ICICI_ALLOW_CLOSED_MARKET_HISTORICAL_WARMUP = True
ICICI_ALLOW_CLOSED_MARKET_WARMUP = False
ICICI_CLOSED_MARKET_QUOTE_PROBE = False
ICICI_HISTORICAL_V2_FALLBACK = True
# Live NIFTY data is mandatory for the ICICI options desk.  Breeze WebSocket is
# the primary signal transport; REST remains startup/reconciliation only.
ICICI_INDEX_STREAM_ENABLED = os.getenv("ICICI_INDEX_STREAM_ENABLED", "true").lower() in ("1", "true", "yes", "on")
ICICI_INDEX_WEBSOCKET_REQUIRED = os.getenv("ICICI_INDEX_WEBSOCKET_REQUIRED", "true").lower() in ("1", "true", "yes", "on")
ICICI_INDEX_STREAM_FIRST_TICK_TIMEOUT_SEC = float(os.getenv("ICICI_INDEX_STREAM_FIRST_TICK_TIMEOUT_SEC", "12.0"))
ICICI_INDEX_STREAM_MAX_STALE_SEC = float(os.getenv("ICICI_INDEX_STREAM_MAX_STALE_SEC", "15.0"))
ICICI_WEBSOCKET_RECONNECT_COOLDOWN_SEC = float(os.getenv("ICICI_WEBSOCKET_RECONNECT_COOLDOWN_SEC", "5.0"))
ICICI_REQUIRE_UNDERLYING_ANALYSIS_FEED = True
# Official Breeze SDK resolves tokens dynamically from exchange/stock descriptors;
# static script-code maps are deliberately not used.
ICICI_INDEX_STREAM_CHANNELS = "LIVE_QUOTE"
ICICI_INDEX_STREAM_SCRIPT_CODES = {}
ICICI_OPTION_STREAM_ENABLED = os.getenv("ICICI_OPTION_STREAM_ENABLED", "true").lower() in ("1", "true", "yes", "on")
ICICI_OPTION_WEBSOCKET_REQUIRED = os.getenv("ICICI_OPTION_WEBSOCKET_REQUIRED", "true").lower() in ("1", "true", "yes", "on")
# Startup may scan the live NIFTY underlying while CE/PE option sockets are still
# waiting for their first tick; actual order activation remains hard-blocked by
# ICICI_OPTION_WEBSOCKET_REQUIRED until the selected premium vehicle is fresh.
ICICI_SESSION_BOOK_REQUIRE_FIRST_OPTION_TICK_ON_STARTUP = os.getenv("ICICI_SESSION_BOOK_REQUIRE_FIRST_OPTION_TICK_ON_STARTUP", "false").lower() in ("1", "true", "yes", "on")
ICICI_OPTION_STREAM_FIRST_TICK_TIMEOUT_SEC = float(os.getenv("ICICI_OPTION_STREAM_FIRST_TICK_TIMEOUT_SEC", "12.0"))
ICICI_OPTION_STREAM_MAX_STALE_SEC = float(os.getenv("ICICI_OPTION_STREAM_MAX_STALE_SEC", "15.0"))
ICICI_OPTION_TICK_SIZE = 0.05
# Safety invariant: never assume a one-unit NFO option lot.  Contract routing
# is disabled until Breeze/security-master supplies the exact current lot size.
ICICI_OPTION_DEFAULT_LOT_SIZE = 0.0
ICICI_OPTION_MIN_DTE = 1.0
ICICI_OPTION_MAX_DTE = 21.0
ICICI_INDEX_OPTION_TARGET_ABS_DELTA = 0.45
ICICI_STOCK_OPTION_TARGET_ABS_DELTA = 0.50
ICICI_OPTION_DELTA_BAND = 0.22
ICICI_OPTION_MAX_THETA_TO_PREMIUM = 0.08
ICICI_OPTION_IV_STRESS_PRIOR = 0.24
ICICI_OPTION_MIN_IMPLIED_VOL = 0.03
ICICI_OPTION_MAX_IMPLIED_VOL = 1.50
INDIA_RISK_FREE_RATE = 0.065
ICICI_OPTION_MAX_FUNDS_FRACTION_PER_TRADE = 0.42
ICICI_OPTION_MIN_CASH_BUFFER_INR = 0.0
ICICI_OPTION_MIN_READY_1M_BARS = 20
ICICI_UNDERLYING_MIN_READY_1M_BARS = 20
# REST quote pull is reconciliation only; websocket supplies live option prices.
ICICI_OPTION_QUOTE_POLL_SEC = 30.0
# Session-start execution universe: preselect one verified CE and one verified PE
# after F&O funds + NIFTY underlying warmup.  Direction remains live-thesis driven.
ICICI_SESSION_CONTRACT_BOOK_ENABLED = True
ICICI_SESSION_BOOK_MAX_EXPIRIES = 2
ICICI_SESSION_BOOK_PREWARM_EXECUTION_DATA = True
# Circuit-breaker only; normal intraday reselection is delta-band driven.
ICICI_SESSION_BOOK_MAX_SPOT_DRIFT_PCT = 0.008
ICICI_SESSION_BOOK_DELTA_RESELECT_BAND = 0.18
ICICI_SESSION_BOOK_MIN_REFRESH_SEC = 900.0
# Delta/spot-invalidated vehicles may refresh sooner, with anti-thrash protection.
ICICI_SESSION_BOOK_URGENT_REFRESH_COOLDOWN_SEC = 30.0
# A session-book vehicle must be executable now, not just theoretically cheap.
ICICI_SESSION_BOOK_REQUIRE_TWO_SIDED_QUOTE = True
ICICI_OPTION_MAX_SELECTION_SPREAD_BPS = 120.0
ICICI_OPTION_MIN_BOOK_LOTS = 1.0
ICICI_SESSION_BOOK_QUOTES_FALLBACK_ENABLED = True
ICICI_SESSION_BOOK_QUOTE_FALLBACK_STRIKES_PER_SIDE = 10
ICICI_SESSION_BOOK_QUOTE_FALLBACK_MAX_CONTRACTS = 60
# Dynamic execution-cost guard: the live spread cannot consume more than this
# share of the vehicle's observed 1-minute premium ATR.
ICICI_OPTION_EXECUTION_ATR_PERIOD = 14
ICICI_OPTION_MAX_SPREAD_TO_1M_ATR = 0.35
ICICI_SECURITY_MASTER_REQUIRE_TODAY = True
ICICI_OPTION_MAX_QUOTE_STALE_SEC = 10.0
ICICI_UNDERLYING_REST_REFRESH_SEC = 30.0
# Slow authoritative REST reconciliation while Breeze websocket is healthy; faster REST repair runs only during faults.
ICICI_UNDERLYING_REST_RECONCILE_SEC = 900.0
ICICI_OPTION_SLTP_DELTA_MULT = 1.00
ICICI_OPTION_MIN_PREMIUM_RISK_PCT = 0.14
ICICI_OPTION_MAX_PREMIUM_RISK_PCT = 0.58
ICICI_OPTION_MIN_TP_PREMIUM_PCT = 0.18
ICICI_OPTION_PREMIUM_TP_CONVEXITY_BONUS = 0.08
INDIAN_NO_FRESH_ENTRY_AFTER_CLOSE_BUFFER_MIN = 25
UNIVERSE_INCLUDE_EXCHANGES = os.getenv(
    "UNIVERSE_INCLUDE_EXCHANGES",
    "delta,coinswitch,icici" if (ICICI_ENABLED or ICICI_DISCOVERY_ENABLED) else "delta,coinswitch",
)

# Portfolio slots: the bot may hold multiple contracts at once, but each
# contract gets only one ENTERING/ACTIVE/EXITING slot.  Sizing is not divided
# into fixed equal buckets.  Each candidate sees live free cash from the
# exchange and then applies its own desk/instrument margin_pct dynamically.
PORTFOLIO_MAX_OPEN_POSITIONS = 5
PORTFOLIO_MAX_OPEN_PER_CONTRACT = 1
PORTFOLIO_MAX_OPEN_PER_ASSET_CLASS = 3
PORTFOLIO_BUDGET_MODE = "available_funds"   # available_funds | equal_slots | active_equal_slots
# In multi-asset mode, margin/cash is live-free-cash aware while dollar-risk
# remains portfolio-aware.  This prevents stale equal-slot caps while still
# keeping true risk anchored to account equity.
PORTFOLIO_RISK_BUDGET_MODE = "portfolio_equity"  # portfolio_equity | slot_equity
PORTFOLIO_MIN_LOT_MAX_RISK_MULT = 1.15

# Requested universe.  Commodity/index/equity entries are discovery requests;
# if neither exchange lists them, they remain unavailable and are not traded.
MULTI_ASSET_REQUESTS = [
    {"asset_id": "BTC", "display_name": "Bitcoin", "asset_class": "crypto", "aliases": ["BTCUSD", "BTCUSDT", "BTC/USDT", "XBTUSD"], "priority": 0},
    {"asset_id": "NIFTY", "display_name": "NIFTY 50 index options", "asset_class": "option", "aliases": ["NIFTY", "NIFTY50", "CNXNIFTY"], "priority": 5},

    # Commodity exposure available on Delta is tokenised/RWA futures, not physical spot futures.
    {"asset_id": "OIL", "display_name": "Crude Oil / WTI", "asset_class": "commodity", "aliases": ["OIL", "WTI", "CL", "USOIL", "CRUDE", "CRUDEOIL", "OILUSD", "OILUSDT", "WTIUSDT"], "priority": 10},
    {"asset_id": "GOLD", "display_name": "Gold token derivatives", "asset_class": "commodity", "aliases": ["PAXGUSD", "XAUTUSD", "PAXG", "PAXGUSDT", "XAUT", "XAUTUSDT", "GOLD", "XAU", "XAUUSD"], "priority": 11},
    {"asset_id": "SILVER", "display_name": "Silver token derivatives", "asset_class": "commodity", "aliases": ["SLVONUSD", "SLVON", "SILVER", "XAG", "XAGUSD", "SLV", "SILVERUSDT"], "priority": 12},

    # Important: Delta SPXUSD is SPX6900 crypto, NOT S&P 500. Do not alias it here.
    {"asset_id": "SPX_INDEX", "display_name": "S&P 500 index", "asset_class": "index", "aliases": ["SPX500USD", "US500", "SP500", "S&P500"], "priority": 20},
    # xStock index/ETF-like token derivatives visible in Delta's market table.
    {"asset_id": "SPY", "display_name": "SP500 xStock token derivative", "asset_class": "equity", "aliases": ["SPYXUSD", "SPYX", "SPY", "SPYUSD", "SPYUSDT"], "priority": 21},
    {"asset_id": "QQQ", "display_name": "Nasdaq xStock token derivative", "asset_class": "equity", "aliases": ["QQQXUSD", "QQQX", "QQQ", "QQQUSD", "QQQUSDT"], "priority": 22},

    # Delta US equity exposure is through xStock/RWA token perpetuals, not direct shares.
    {"asset_id": "AAPL", "display_name": "Apple xStock token derivative", "asset_class": "equity", "aliases": ["AAPLXUSD", "AAPLX", "AAPL", "AAPLUSD", "AAPLUSDT"], "priority": 30},
    {"asset_id": "MSFT", "display_name": "Microsoft xStock token derivative", "asset_class": "equity", "aliases": ["MSFTXUSD", "MSFTX", "MSFT", "MSFTUSD", "MSFTUSDT"], "priority": 31},
    {"asset_id": "NVDA", "display_name": "NVIDIA xStock token derivative", "asset_class": "equity", "aliases": ["NVDAXUSD", "NVDAX", "NVDA", "NVDAUSD", "NVDAUSDT"], "priority": 32},
    {"asset_id": "TSLA", "display_name": "Tesla xStock token derivative", "asset_class": "equity", "aliases": ["TSLAXUSD", "TSLAX", "TSLA", "TSLAUSD", "TSLAUSDT"], "priority": 33},
    {"asset_id": "AMZN", "display_name": "Amazon xStock token derivative", "asset_class": "equity", "aliases": ["AMZNXUSD", "AMZNX", "AMZN", "AMZNUSD", "AMZNUSDT"], "priority": 34},
    {"asset_id": "META", "display_name": "Meta xStock token derivative", "asset_class": "equity", "aliases": ["METAXUSD", "METAX", "META", "METAUSD", "METAUSDT"], "priority": 35},
    {"asset_id": "COIN", "display_name": "Coinbase xStock token derivative", "asset_class": "equity", "aliases": ["COINXUSD", "COINX", "COIN", "COINUSD", "COINUSDT"], "priority": 36},
    {"asset_id": "CRCL", "display_name": "Circle xStock token derivative", "asset_class": "equity", "aliases": ["CRCLXUSD", "CRCLX", "CRCL", "CRCLUSD", "CRCLUSDT"], "priority": 37},
    {"asset_id": "GOOGL", "display_name": "Alphabet xStock token derivative", "asset_class": "equity", "aliases": ["GOOGLXUSD", "GOOGLX", "GOOGL", "GOOG", "GOOGLUSD", "GOOGLUSDT"], "priority": 38},
]


# ─────────────────────────────────────────────────────────────────────────────
# v9 Institutional multi-asset runtime policy
# Centralised per-asset policy removes BTC-config leakage into xStocks/metals.
SCANNER_START_PARALLELISM = 4
SCANNER_POSITION_TICK_SEC = 0.25
PORTFOLIO_BALANCE_CACHE_TTL_SEC = 2.0

POLICY_CRYPTO_RISK_MULT = 1.00
POLICY_CRYPTO_LOOP_INTERVAL_SEC = 0.25

POLICY_COMMODITY_RISK_MULT = 1.00
POLICY_COMMODITY_MARGIN_PCT = 0.36
POLICY_COMMODITY_MIN_MARGIN_USD = 0.00
POLICY_COMMODITY_TICK_EVAL_SEC = 0.50
POLICY_COMMODITY_LOOP_INTERVAL_SEC = 0.50
POLICY_COMMODITY_MIN_1M_BARS = 85
POLICY_COMMODITY_MIN_5M_BARS = 65
POLICY_COMMODITY_MIN_RR = 2.20
POLICY_COMMODITY_MAX_RR = 5.0
POLICY_COMMODITY_MAX_HOLD_SEC = 4800
POLICY_COMMODITY_COOLDOWN_SEC = 180
POLICY_COMMODITY_SL_BUFFER_ATR = 0.50

POLICY_EQUITY_RISK_MULT = 0.80
POLICY_EQUITY_MARGIN_PCT = 0.30
POLICY_EQUITY_MIN_MARGIN_USD = 0.00
POLICY_EQUITY_TICK_EVAL_SEC = 0.75
POLICY_EQUITY_LOOP_INTERVAL_SEC = 0.75
POLICY_EQUITY_MIN_1M_BARS = 90
POLICY_EQUITY_MIN_5M_BARS = 70
POLICY_EQUITY_MIN_RR = 1.75
POLICY_EQUITY_MAX_RR = 5.0
POLICY_EQUITY_MAX_HOLD_SEC = 5400
POLICY_EQUITY_COOLDOWN_SEC = 180
POLICY_EQUITY_SL_BUFFER_ATR = 0.55

POLICY_OPTION_RISK_MULT = 1.00
POLICY_OPTION_MARGIN_PCT = 0.42
POLICY_OPTION_MIN_MARGIN_USD = 0.0
POLICY_OPTION_TICK_EVAL_SEC = 0.50
POLICY_OPTION_LOOP_INTERVAL_SEC = 0.50
POLICY_OPTION_MIN_1M_BARS = 45
POLICY_OPTION_MIN_5M_BARS = 35
POLICY_OPTION_MIN_RR = 1.60
POLICY_OPTION_MAX_RR = 4.00
POLICY_OPTION_MAX_HOLD_SEC = 2700
POLICY_OPTION_COOLDOWN_SEC = 120
POLICY_OPTION_LOSS_LOCKOUT_SEC = 300
POLICY_OPTION_SL_BUFFER_ATR = 0.75
POLICY_OPTION_ATR_MIN_PCTILE = 0.02
POLICY_OPTION_ATR_MAX_PCTILE = 0.995
POLICY_OPTION_SLIPPAGE_TOL = 0.0030
POLICY_OPTION_OB_DEPTH_LEVELS = 3
POLICY_OPTION_TICK_AGG_WINDOW_SEC = 30.0
POLICY_OPTION_VWAP_WINDOW = 35

# Desk model: every desk uses the unified structural auction authority.
# Archetypes compete under one execution/risk authority; evidence is not probability.
# Desks differ only in venue execution, risk, cost and lot-size policy.
STRATEGY_CORE_NAME = "INSTITUTIONAL_AUCTION_V514"
TELEGRAM_RECENT_TRADES_LIMIT = 30

TRADING_DESKS = {
    "BTC": {
        "display_name": "BTC Desk",
        "strategy": STRATEGY_CORE_NAME,
        "asset_ids": ("BTC",),
        "asset_classes": ("crypto",),
        "risk_multiplier": POLICY_CRYPTO_RISK_MULT,
        "margin_pct": QUANT_MARGIN_PCT,
        "tick_eval_sec": ENTRY_EVALUATION_INTERVAL_SECONDS,
        "loop_interval_sec": POLICY_CRYPTO_LOOP_INTERVAL_SEC,
        "min_1m_bars": MIN_CANDLES_1M,
        "min_5m_bars": MIN_CANDLES_5M,
        "min_rr": MIN_RISK_REWARD_RATIO,
        "max_rr": QUANT_TP_MAX_RR,
        "max_hold_sec": QUANT_MAX_HOLD_SEC,
        "cooldown_sec": MIN_TIME_BETWEEN_TRADES_SEC,
        "sl_buffer_atr": QUANT_SL_BUFFER_ATR_MULT,
    },
    "COMMODITIES": {
        "display_name": "Commodities Desk",
        "strategy": STRATEGY_CORE_NAME,
        "asset_classes": ("commodity",),
        "risk_multiplier": POLICY_COMMODITY_RISK_MULT,
        "margin_pct": POLICY_COMMODITY_MARGIN_PCT,
        "tick_eval_sec": 0.65,
        "loop_interval_sec": 0.65,
        "min_1m_bars": 100,
        "min_5m_bars": 80,
        "min_rr": POLICY_COMMODITY_MIN_RR,
        "max_rr": 5.50,
        "max_hold_sec": 3600,
        "cooldown_sec": POLICY_COMMODITY_COOLDOWN_SEC,
        "sl_buffer_atr": 0.65,
    },
    "STOCKS": {
        "enabled": STOCK_DESK_TRADING_ENABLED,
        "display_name": "Stocks Desk",
        "strategy": STRATEGY_CORE_NAME,
        "asset_classes": ("equity", "index"),
        "risk_multiplier": POLICY_EQUITY_RISK_MULT,
        "margin_pct": POLICY_EQUITY_MARGIN_PCT,
        "tick_eval_sec": 0.90,
        "loop_interval_sec": 0.90,
        "min_1m_bars": 110,
        "min_5m_bars": 85,
        "min_rr": 2.40,
        "max_rr": 6.00,
        "max_hold_sec": 3000,
        "cooldown_sec": 360,
        "sl_buffer_atr": 0.75,
    },
    "OPTIONS": {
        "enabled": ICICI_OPTIONS_RUNTIME_ENABLED,
        "display_name": "NIFTY Options Desk",
        "strategy": STRATEGY_CORE_NAME,
        "asset_ids": ("NIFTY",),
        "asset_classes": ("option",),
        "risk_multiplier": POLICY_OPTION_RISK_MULT,
        "margin_pct": POLICY_OPTION_MARGIN_PCT,
        "tick_eval_sec": POLICY_OPTION_TICK_EVAL_SEC,
        "loop_interval_sec": POLICY_OPTION_LOOP_INTERVAL_SEC,
        "min_1m_bars": POLICY_OPTION_MIN_1M_BARS,
        "min_5m_bars": POLICY_OPTION_MIN_5M_BARS,
        "min_rr": POLICY_OPTION_MIN_RR,
        "max_rr": POLICY_OPTION_MAX_RR,
        "max_hold_sec": POLICY_OPTION_MAX_HOLD_SEC,
        "cooldown_sec": POLICY_OPTION_COOLDOWN_SEC,
        "sl_buffer_atr": POLICY_OPTION_SL_BUFFER_ATR,
    },
}

# v13 multi-asset protection invariants
# A Delta bracket fill is not considered safe until the bot verifies the SL and
# TP children belong to the SAME product and are near the intended SL/TP prices.
DELTA_BRACKET_CHILD_VERIFY_TIMEOUT_SEC = 18.0
DELTA_BRACKET_CHILD_PRICE_TOL_TICKS = 6.0
DELTA_BRACKET_CHILD_PRICE_TOL_PCT = 0.0025
DELTA_EMERGENCY_FLATTEN_ON_BRACKET_MISMATCH = True
TELEGRAM_ALERT_PROTECTION_FAILURE = True

# Telegram command-channel network resilience
def _float_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return float(default)

TELEGRAM_GETUPDATES_BACKOFF_BASE_SEC = _float_env("TELEGRAM_GETUPDATES_BACKOFF_BASE_SEC", 2.0)
TELEGRAM_GETUPDATES_BACKOFF_MAX_SEC = _float_env("TELEGRAM_GETUPDATES_BACKOFF_MAX_SEC", 30.0)

"""
config.py — Unified Configuration v10.0
=========================================
Single source of truth. All institutional parameters inline.
No config_overrides.py — everything lives here.

Risk-controlled configuration for structural institutional liquidity execution.
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

# Official Groww credential modes:
#   TOTP: GROWW_TOTP_TOKEN + GROWW_TOTP_SECRET
#   API key approval flow: GROWW_API_KEY + GROWW_API_SECRET
#   Explicit generated bearer token: GROWW_ACCESS_TOKEN
# Do not store a Groww TOTP token in GROWW_ACCESS_TOKEN.
GROWW_ACCESS_TOKEN        = _first_env("GROWW_ACCESS_TOKEN")
GROWW_TOTP_TOKEN          = _first_env("GROWW_TOTP_TOKEN")
GROWW_TOTP_SECRET         = _first_env("GROWW_TOTP_SECRET")
GROWW_API_KEY             = _first_env("GROWW_API_KEY")
GROWW_API_SECRET          = _first_env("GROWW_API_SECRET")
GROWW_AUTH_CONFIGURED     = bool(
    GROWW_ACCESS_TOKEN
    or (GROWW_TOTP_TOKEN and GROWW_TOTP_SECRET)
    or (GROWW_API_KEY and GROWW_API_SECRET)
)
GROWW_ENABLED             = os.getenv(
    "GROWW_ENABLED", "true" if GROWW_AUTH_CONFIGURED else "false"
).lower() in ("1", "true", "yes", "on")
TELEGRAM_BOT_TOKEN        = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID          = os.getenv("TELEGRAM_CHAT_ID",   "")

if not DELTA_API_KEY and not COINSWITCH_API_KEY and not GROWW_AUTH_CONFIGURED:
    raise ValueError("No exchange credentials in .env. For Groww TOTP set GROWW_TOTP_TOKEN and GROWW_TOTP_SECRET.")

# ── Symbol / Leverage ─────────────────────────────────────────────────────────
SYMBOL                   = "BTCUSDT"
LEVERAGE                 = 5   # hard execution ceiling; do not expose the account to 45x config drift
DELTA_SYMBOL             = "BTCUSD"
DELTA_CONTRACT_VALUE_BTC = 0.001
DELTA_BALANCE_CURRENCY   = "USD"
COINSWITCH_SYMBOL        = "BTCUSDT"
COINSWITCH_EXCHANGE      = "EXCHANGE_2"
# Read-only reference feed for BTC leader/follower research; it never routes orders.
HYPERLIQUID_REFERENCE_ENABLED = os.getenv("HYPERLIQUID_REFERENCE_ENABLED", "false").lower() in ("1", "true", "yes", "on")
HYPERLIQUID_TESTNET          = os.getenv("HYPERLIQUID_TESTNET", "false").lower() in ("1", "true", "yes", "on")
HYPERLIQUID_RECONNECT_SEC    = float(os.getenv("HYPERLIQUID_RECONNECT_SEC", "3.0"))

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
#   but as FRACTION by institutional_strategy._compute_quantity (× direct = 0.60 → 60%).
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
TICK_SIZE_GROWW                  = 0.05
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
#   1. Anchor to invalidation structure (quantitative invalidation level, liquidity zone, or execution-capacity boundary).
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
    if ex == "groww":
        return float(TICK_SIZE_GROWW)
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
# Institutional structural invalidation: the protective stop remains beyond the liquidity_evented wick.
# These are risk-model parameters, surfaced in Institutional_GEOMETRY logs and used directly
# by strategy.entry_engine; they are not trailing-stop or alpha filters.
Institutional_STOP_CLEARANCE_BASE_ATR = 0.10
Institutional_STOP_CLEARANCE_PCTL_SLOPE_ATR = 0.18

# ── Aggregator ────────────────────────────────────────────────────────────────
AGG_PRIMARY_WEIGHT   = 0.55
AGG_SECONDARY_WEIGHT = 0.45
AGG_OB_DEPTH_LEVELS  = 10
AGG_TRADE_WINDOW_SEC = 30.0

# ── Microstructure alpha baseline (kept in shadow mode until forward labels validate it) ──
INSTITUTIONAL_ENABLE_LIVE_ENTRIES = os.getenv("INSTITUTIONAL_ENABLE_LIVE_ENTRIES", "false").lower() in ("1", "true", "yes", "on")
INSTITUTIONAL_REQUIRE_BTC_CROSS_VENUE = os.getenv("INSTITUTIONAL_REQUIRE_BTC_CROSS_VENUE", "true").lower() in ("1", "true", "yes", "on")
INSTITUTIONAL_MIN_EXECUTION_QUALITY = 0.40
INSTITUTIONAL_MIN_FLOW_AGREEMENT = 0.55
INSTITUTIONAL_MAX_CROSS_VENUE_DISPERSION_BPS = 15.0
INSTITUTIONAL_MIN_SIGNAL_BPS = 0.50
INSTITUTIONAL_MIN_NET_EDGE_BPS = 3.0
INSTITUTIONAL_FLOW_OFI_WEIGHT = 1.0
INSTITUTIONAL_FLOW_TFI_WEIGHT = 0.30
INSTITUTIONAL_FLOW_MICROPRICE_WEIGHT = 0.35
INSTITUTIONAL_FLOW_DISLOCATION_WEIGHT = 0.50
INSTITUTIONAL_RISK_FRACTION_PER_TRADE = 0.0025
INSTITUTIONAL_QUARTER_KELLY = 0.25
INSTITUTIONAL_TARGET_OBSERVATION_VOL_BPS = 10.0
INSTITUTIONAL_CORRELATED_EXPOSURE_CAP_FRACTION = float(os.getenv("INSTITUTIONAL_CORRELATED_EXPOSURE_CAP_FRACTION", "0.35"))
RESEARCH_STORE_PATH = os.getenv("RESEARCH_STORE_PATH", "research_output")

# ── Institutional Strategy ────────────────────────────────────────────────────────────
INSTITUTIONAL_MARGIN_PCT               = 0.50
INSTITUTIONAL_SLIPPAGE_TOLERANCE       = 0.0005
INSTITUTIONAL_SL_BUFFER_ATR_MULT       = 0.4
INSTITUTIONAL_ATR_PCTILE_WINDOW        = 100
INSTITUTIONAL_ATR_MIN_PCTILE           = 0.05
INSTITUTIONAL_ATR_MAX_PCTILE           = 0.97
INSTITUTIONAL_MAX_HOLD_SEC             = 3600      # 60 min max hold
INSTITUTIONAL_LOSS_LOCKOUT_SEC         = 600       # max-consecutive-loss lockout
INSTITUTIONAL_LOCKOUT_AFTER_LOSS_SEC   = 180       # short post-loss market_state reset, not a no-trade freeze
INSTITUTIONAL_POS_SYNC_SEC             = 30
RECONCILE_EXIT_SETTLE_SEC = 15.0      # protect exact-fill accounting from stale position feed after a local close


# ─────────────────────────────────────────────────────────────────────────────
# Six-lens gate that replaces the flat 30s cooldown with regime-aware logic.
# Goal: stop the "exit → re-enter in 30s → take another stop" failure mode.
#
# Each constant tunes one lens. Defaults are conservative for BTC perps on a
# 1m/5m/15m liquidity-first stack; relax with caution.

INSTITUTIONAL_TP_MAX_RR                = 3.5
INSTITUTIONAL_MAX_SPREAD_ATR_RATIO     = 2.50
# Asset-aware spread gate (v8): xStock/RWA products have coarse tick geometry;
# a normal 1-4 tick spread can exceed the current 5m ATR in quiet windows.
# These caps hard-block genuinely broken books while converting normal wide
# tokenised-equity spreads into allocation haircuts handled by sizing/EV.
INSTITUTIONAL_SPREAD_SOFT_ATR_RATIO_CRYPTO    = 0.40
INSTITUTIONAL_MAX_SPREAD_BPS_CRYPTO           = 40.0
INSTITUTIONAL_MAX_SPREAD_TICKS_CRYPTO         = 30.0
INSTITUTIONAL_SPREAD_SOFT_ATR_RATIO_EQUITY    = 0.50
INSTITUTIONAL_MAX_SPREAD_ATR_RATIO_EQUITY     = 4.00
INSTITUTIONAL_MAX_SPREAD_BPS_EQUITY           = 35.0
INSTITUTIONAL_MAX_SPREAD_TICKS_EQUITY         = 8.0
INSTITUTIONAL_SPREAD_SOFT_ATR_RATIO_COMMODITY = 0.60
INSTITUTIONAL_MAX_SPREAD_ATR_RATIO_COMMODITY  = 2.50
INSTITUTIONAL_MAX_SPREAD_BPS_COMMODITY        = 60.0
INSTITUTIONAL_MAX_SPREAD_TICKS_COMMODITY      = 30.0
INSTITUTIONAL_SPREAD_MIN_SIZE_MULT            = 0.70
INSTITUTIONAL_SPREAD_SIZE_HAIRCUT_MAX         = 0.30

# market_state-control market_state authority:
# A trade is owned by the active multi-timeframe delivery process, not by an
# isolated short-window observation.  1D/4H/1H/15m delivery plus higher-timeframe
# liquidity transfer establishes phase, controlling side and capital posture.
# Local liquidity_events against firm 1H+ control remain observations until control transfers.
MARKET_STATE_ENGINE_ENABLED = True
MARKET_STATE_FIRM_PARENT_MIN_QUALITY = 0.60
MARKET_STATE_AGGRESSIVE_MIN_CLARITY = 0.62

# Quantitative liquidity model is the common entry/SL/TP authority for BTC, SILVER,
# commodities and the GROWW profile. It ranks competing liquidity_gap/liquidity-zone zones,
# protects SL beyond relevant same-side liquidity clusters and selects TP by
# observable multi-timeframe liquidity concentration. It does not add a stack
# of arbitrary trade filters; weak zones remain visible as noise telemetry.
Institutional_STRUCTURAL_ZONE_GRAPH_ENABLED = True

# Compatibility scoring surfaces remain logged as diagnostics for audit/replay only.
# They are not permitted to stack hard filters in the live core by default.
Institutional_LEGACY_FILTER_COMPATIBILITY_ENABLED = False
Institutional_SELECTIVITY_MODE = True
Institutional_ALLOW_COUNTER_DELIVERY_liquidity_eventS = False
Institutional_ENTRY_MIN_CONTEXT_DELIVERY_SCORE = 0.25
Institutional_ENTRY_MIN_DELIVERY_SCORE = 0.50
Institutional_ENTRY_MIN_TARGET_RANK_SCORE = 1.25
Institutional_ENTRY_MIN_DISPLACEMENT_ATR_liquidity_event = 1.35
Institutional_ENTRY_MIN_DISPLACEMENT_ATR_CONTINUATION = 1.50
Institutional_CONTINUATION_MIN_CONTEXT_SCORE = 0.35
Institutional_CONTINUATION_MIN_DELIVERY_SCORE = 0.60
Institutional_CONTINUATION_MIN_TARGET_RANK_SCORE = 2.60
Institutional_PARTIAL_HTF_MIN_DELIVERY_SCORE = 0.62
Institutional_PARTIAL_HTF_MIN_DISPLACEMENT_ATR = 2.00
Institutional_MICROSTRUCTURE_GUARD_ENABLED = True
Institutional_MIN_FRESH_MICROSTRUCTURE_SCORE = -0.20
Institutional_MIN_TARGET_REALISM_SCORE = 0.60
Institutional_SETUP_DOSSIER_ENABLED = True
Institutional_SETUP_DOSSIER_MIN_SCORE = 0.60
Institutional_SETUP_DOSSIER_RANK_NORM = 3.0
Institutional_THESIS_MAX_AGE_SEC = 900.0
Institutional_CONTINUATION_THESIS_MAX_AGE_SEC = 600.0
Institutional_MAX_liquidity_gap_EXTENSION_BEFORE_REPRICE_ATR = 3.25
INSTITUTIONAL_PRICE_LOCATION_GUARD_ENABLED = True
INSTITUTIONAL_PRICE_LOCATION_MIN_SCORE = 0.58
INSTITUTIONAL_PRICE_LOCATION_STRICT_CONTEXT = True
Institutional_DEALING_RANGE_LOOKBACK_15M = 48
INSTITUTIONAL_RETRACEMENT_MIN = 0.50
INSTITUTIONAL_RETRACEMENT_MAX = 0.79
INSTITUTIONAL_SESSION_CONTEXT_WEIGHT_ENABLED = True

# Time-decay exit: use the configured desk max-hold as a thesis half-life.
# Healthy winners can still reach exchange targets; dead market_states are flattened
# before they burn the full structural stop by time alone.
INSTITUTIONAL_TIME_STOP_ENABLED = True
INSTITUTIONAL_TIME_STOP_EARLY_FRACTION = 0.55
INSTITUTIONAL_TIME_STOP_FAILED_market_state_R = -0.35
INSTITUTIONAL_TIME_STOP_FAILED_market_state_MAX_MFE_R = 0.50
INSTITUTIONAL_TIME_STOP_MIN_PROGRESS_R = 0.20
INSTITUTIONAL_TIME_STOP_HARD_MAX_MULT = 1.35

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

# ── Institutional Market State ───────────────────────────────────────────────────────────────────
OB_MIN_IMPULSE_PCT          = 0.15
OB_MIN_BODY_RATIO           = 0.40
OB_IMPULSE_SIZE_MULTIPLIER  = 1.30
OB_MAX_AGE_MINUTES          = 1440
liquidity_gap_MIN_SIZE_PCT            = 0.020
liquidity_gap_MAX_AGE_MINUTES         = 1440
LIQ_TOUCH_TOLERANCE_PCT     = 0.20
liquidity_event_DISPLACEMENT_MIN      = 0.40
liquidity_event_MAX_AGE_MINUTES       = 120
SESSION_ASIA_NY_START            = 20
SESSION_ASIA_NY_END              = 1
SESSION_LONDON_NY_START          = 2
SESSION_LONDON_NY_END            = 5
SESSION_NY_NY_START              = 7
SESSION_NY_NY_END                = 10

# ── Institutional Gate ──────────────────────────────────────────────────────────────────

# ── HTF context ───────────────────────────────────────────────────────────────



# ── Institutional Dynamic Entry Quality References ───────────────────────────
# Structural entry thresholds are owned only by the institutional liquidity engine.





# ── State-change expiry ──────────────────────────────────────────────────────────────

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
# Observability cadence: decision transitions log immediately in InstitutionalStrategy; these
# slow cadences only prove scanner health and suppress repetitive per-tick noise.
Institutional_DECISION_SNAPSHOT_SEC = 60.0
SCANNER_ASSET_HEARTBEAT_SEC = 300.0
SCANNER_ASSET_ANALYSIS_LOG_SEC = 60.0  # execution-health audit, not alpha reasoning

# Desk suspension policy.  Suspended desks are excluded before live catalog
# discovery, so they cannot subscribe, analyse, size, or route orders.
# This is a desk-level trading suspension, not a removal of instrument metadata.
STOCK_DESK_TRADING_ENABLED = False
SUSPENDED_TRADING_DESKS = ("STOCKS",)
SUSPENDED_ASSET_CLASSES = ("equity", "index")

# Groww / Indian index options desk. Strategy flow remains unchanged:
# bullish NIFTY thesis buys CE, bearish NIFTY thesis buys PE. Authentication
# follows Groww's official SDK: generated access token, TOTP token+secret, or API key+secret.
GROWW_LONG_PREMIUM_ONLY = True
GROWW_OPTIONS_ONLY = True
GROWW_DISCOVERY_ENABLED = os.getenv("GROWW_DISCOVERY_ENABLED", "true").lower() in ("1", "true", "yes", "on")
GROWW_OPTIONS_RUNTIME_ENABLED = GROWW_ENABLED
GROWW_INDEX_OPTIONS_FROM_CONFIG_ONLY = True
GROWW_INDEX_UNDERLYINGS = os.getenv("GROWW_INDEX_UNDERLYINGS", "NIFTY")
GROWW_INDEX_STOCK_CODE_BY_UNDERLYING = {"NIFTY": "NIFTY", "NIFTY50": "NIFTY", "CNXNIFTY": "NIFTY"}
GROWW_OPTION_PRODUCT_TYPE = os.getenv("GROWW_OPTION_PRODUCT_TYPE", "NRML").upper()
GROWW_OPTION_TICK_SIZE = 0.05
GROWW_MIN_CALL_GAP_SEC = float(os.getenv("GROWW_MIN_CALL_GAP_SEC", "0.25"))
GROWW_INSTRUMENTS_CSV_URL = os.getenv("GROWW_INSTRUMENTS_CSV_URL", "https://growwapi-assets.groww.in/instruments/instrument.csv")
GROWW_INSTRUMENT_CACHE_TTL_SEC = float(os.getenv("GROWW_INSTRUMENT_CACHE_TTL_SEC", "1800.0"))
GROWW_SECURITY_MASTER_CACHE_PATH = os.getenv("GROWW_SECURITY_MASTER_CACHE_PATH", "data/groww_instruments.csv")
GROWW_SECURITY_MASTER_REQUIRE_TODAY = True
GROWW_MARKET_SESSION_GUARD_ENABLED = True
GROWW_MARKET_OPEN_TIME = "09:15"
GROWW_MARKET_CLOSE_TIME = "15:30"
GROWW_MARKET_HOLIDAYS = tuple(x.strip() for x in os.getenv("GROWW_MARKET_HOLIDAYS", "").split(",") if x.strip())
GROWW_ANALYZE_ONLY_DURING_MARKET_SESSION = os.getenv("GROWW_ANALYZE_ONLY_DURING_MARKET_SESSION", "true").lower() in ("1", "true", "yes", "on")
GROWW_ALLOW_CLOSED_MARKET_HISTORICAL_WARMUP = True
GROWW_ALLOW_CLOSED_MARKET_WARMUP = False
GROWW_INDEX_STREAM_ENABLED = os.getenv("GROWW_INDEX_STREAM_ENABLED", "true").lower() in ("1", "true", "yes", "on")
GROWW_INDEX_WEBSOCKET_REQUIRED = os.getenv("GROWW_INDEX_WEBSOCKET_REQUIRED", "true").lower() in ("1", "true", "yes", "on")
GROWW_INDEX_STREAM_FIRST_TICK_TIMEOUT_SEC = float(os.getenv("GROWW_INDEX_STREAM_FIRST_TICK_TIMEOUT_SEC", "12.0"))
GROWW_INDEX_STREAM_MAX_STALE_SEC = float(os.getenv("GROWW_INDEX_STREAM_MAX_STALE_SEC", "15.0"))
GROWW_WEBSOCKET_RECONNECT_COOLDOWN_SEC = float(os.getenv("GROWW_WEBSOCKET_RECONNECT_COOLDOWN_SEC", "30.0"))
GROWW_SHARED_TRANSPORT_MAX_STALE_SEC = float(os.getenv("GROWW_SHARED_TRANSPORT_MAX_STALE_SEC", "20.0"))
GROWW_REQUIRE_UNDERLYING_ANALYSIS_FEED = True
GROWW_INDEX_STREAM_CHANNELS = "LIVE_QUOTE"
GROWW_INDEX_STREAM_SCRIPT_CODES = {}
GROWW_OPTION_STREAM_ENABLED = os.getenv("GROWW_OPTION_STREAM_ENABLED", "true").lower() in ("1", "true", "yes", "on")
GROWW_OPTION_WEBSOCKET_REQUIRED = os.getenv("GROWW_OPTION_WEBSOCKET_REQUIRED", "true").lower() in ("1", "true", "yes", "on")
GROWW_SESSION_BOOK_REQUIRE_FIRST_OPTION_TICK_ON_STARTUP = os.getenv("GROWW_SESSION_BOOK_REQUIRE_FIRST_OPTION_TICK_ON_STARTUP", "false").lower() in ("1", "true", "yes", "on")
GROWW_OPTION_STREAM_FIRST_TICK_TIMEOUT_SEC = float(os.getenv("GROWW_OPTION_STREAM_FIRST_TICK_TIMEOUT_SEC", "12.0"))
GROWW_OPTION_STREAM_MAX_STALE_SEC = float(os.getenv("GROWW_OPTION_STREAM_MAX_STALE_SEC", "15.0"))
GROWW_REQUIRE_STATIC_IP_FOR_LIVE_ORDERS = os.getenv("GROWW_REQUIRE_STATIC_IP_FOR_LIVE_ORDERS", "true").lower() in ("1", "true", "yes", "on")
GROWW_APPROVED_STATIC_IPS = tuple(x.strip() for x in os.getenv("GROWW_APPROVED_STATIC_IPS", "").split(",") if x.strip())
GROWW_OUTBOUND_IP_CHECK_URL = os.getenv("GROWW_OUTBOUND_IP_CHECK_URL", "https://api.ipify.org?format=json")
GROWW_OUTBOUND_IP_OVERRIDE = os.getenv("GROWW_OUTBOUND_IP_OVERRIDE", "").strip()
# Groww SDK exposes order_reference_id (8-20 chars) for traceability. Broker-side
# algo registration/whitelisting must be confirmed before live India execution.
GROWW_REQUIRE_SEBI_ALGO_CONFIRMATION_FOR_LIVE_ORDERS = os.getenv("GROWW_REQUIRE_SEBI_ALGO_CONFIRMATION_FOR_LIVE_ORDERS", "true").lower() in ("1", "true", "yes", "on")
GROWW_SEBI_ALGO_REGISTRATION_CONFIRMED = os.getenv("GROWW_SEBI_ALGO_REGISTRATION_CONFIRMED", "false").lower() in ("1", "true", "yes", "on")
GROWW_SEBI_STRATEGY_PREFIX = os.getenv("GROWW_SEBI_STRATEGY_PREFIX", "instv2")[:6]
GROWW_OPTION_MIN_LIVE_IV_COVERAGE = float(os.getenv("GROWW_OPTION_MIN_LIVE_IV_COVERAGE", "0.60"))
GROWW_OPTION_LONG_MAX_VRP = float(os.getenv("GROWW_OPTION_LONG_MAX_VRP", "-0.02"))

GROWW_NIFTY_TREND_liquidity_event_ENABLED = os.getenv("GROWW_NIFTY_TREND_liquidity_event_ENABLED", "true").lower() in ("1", "true", "yes", "on")
GROWW_NIFTY_TREND_liquidity_event_MIN_PHASE_SCORE = float(os.getenv("GROWW_NIFTY_TREND_liquidity_event_MIN_PHASE_SCORE", "0.30"))
GROWW_NIFTY_TREND_liquidity_event_AGGRESSIVE_PHASE_SCORE = float(os.getenv("GROWW_NIFTY_TREND_liquidity_event_AGGRESSIVE_PHASE_SCORE", "0.58"))
GROWW_NIFTY_TREND_liquidity_event_MIN_RR = float(os.getenv("GROWW_NIFTY_TREND_liquidity_event_MIN_RR", "1.15"))
GROWW_NIFTY_TREND_liquidity_event_MAX_RR = float(os.getenv("GROWW_NIFTY_TREND_liquidity_event_MAX_RR", "2.40"))
GROWW_NIFTY_TREND_liquidity_event_MAX_TARGET_ATR = float(os.getenv("GROWW_NIFTY_TREND_liquidity_event_MAX_TARGET_ATR", "2.75"))
GROWW_NIFTY_TREND_liquidity_event_MAX_RECLAIM_EXTENSION_ATR = float(os.getenv("GROWW_NIFTY_TREND_liquidity_event_MAX_RECLAIM_EXTENSION_ATR", "0.65"))
GROWW_NIFTY_TREND_liquidity_event_1M_MAX_AGE_SEC = float(os.getenv("GROWW_NIFTY_TREND_liquidity_event_1M_MAX_AGE_SEC", "90.0"))
GROWW_NIFTY_TREND_liquidity_event_5M_MAX_AGE_SEC = float(os.getenv("GROWW_NIFTY_TREND_liquidity_event_5M_MAX_AGE_SEC", "360.0"))
GROWW_NIFTY_TREND_liquidity_event_STOP_BASE_ATR = float(os.getenv("GROWW_NIFTY_TREND_liquidity_event_STOP_BASE_ATR", "0.08"))
GROWW_NIFTY_TREND_liquidity_event_STOP_PCTL_SLOPE_ATR = float(os.getenv("GROWW_NIFTY_TREND_liquidity_event_STOP_PCTL_SLOPE_ATR", "0.10"))
GROWW_NIFTY_TREND_liquidity_event_MIN_TARGET_REALISM = float(os.getenv("GROWW_NIFTY_TREND_liquidity_event_MIN_TARGET_REALISM", "0.42"))
GROWW_NIFTY_TREND_liquidity_event_MAX_HOLD_SEC = float(os.getenv("GROWW_NIFTY_TREND_liquidity_event_MAX_HOLD_SEC", "720.0"))
GROWW_NIFTY_TREND_liquidity_event_FAILED_market_state_FRACTION = float(os.getenv("GROWW_NIFTY_TREND_liquidity_event_FAILED_market_state_FRACTION", "0.35"))
GROWW_NIFTY_TREND_liquidity_event_FAILED_market_state_R = float(os.getenv("GROWW_NIFTY_TREND_liquidity_event_FAILED_market_state_R", "-0.15"))
GROWW_NIFTY_TREND_liquidity_event_FAILED_market_state_MAX_MFE_R = float(os.getenv("GROWW_NIFTY_TREND_liquidity_event_FAILED_market_state_MAX_MFE_R", "0.30"))
GROWW_NIFTY_TREND_liquidity_event_MIN_PROGRESS_R = float(os.getenv("GROWW_NIFTY_TREND_liquidity_event_MIN_PROGRESS_R", "0.15"))
GROWW_NIFTY_TREND_liquidity_event_HARD_MAX_MULT = float(os.getenv("GROWW_NIFTY_TREND_liquidity_event_HARD_MAX_MULT", "1.0"))

GROWW_OPTION_DEFAULT_LOT_SIZE = 0.0
GROWW_OPTION_MIN_DTE = 1.0
GROWW_OPTION_MAX_DTE = 21.0
GROWW_INDEX_OPTION_TARGET_ABS_DELTA = 0.45
GROWW_STOCK_OPTION_TARGET_ABS_DELTA = 0.50
GROWW_OPTION_DELTA_BAND = 0.22
GROWW_OPTION_MAX_THETA_TO_PREMIUM = 0.08
GROWW_OPTION_IV_STRESS_PRIOR = 0.24
GROWW_OPTION_MIN_IMPLIED_VOL = 0.03
GROWW_OPTION_MAX_IMPLIED_VOL = 1.50
INDIA_RISK_FREE_RATE = 0.065
GROWW_OPTION_MAX_FUNDS_FRACTION_PER_TRADE = 0.42
GROWW_OPTION_MIN_CASH_BUFFER_INR = 0.0
GROWW_OPTION_MIN_READY_1M_BARS = 20
GROWW_UNDERLYING_MIN_READY_1M_BARS = 20
GROWW_SESSION_CONTRACT_BOOK_ENABLED = True
GROWW_SESSION_BOOK_MAX_EXPIRIES = 2
# Official Groww universe: option-chain ranking -> temporary live FNO depth discovery -> dedicated execution feeds.
GROWW_SESSION_BOOK_STREAM_CANDIDATES_PER_SIDE = 12
GROWW_SESSION_BOOK_STREAM_DISCOVERY_TIMEOUT_SEC = 15.0
GROWW_SESSION_BOOK_PREWARM_EXECUTION_DATA = True
GROWW_SESSION_BOOK_MAX_SPOT_DRIFT_PCT = 0.008
GROWW_SESSION_BOOK_DELTA_RESELECT_BAND = 0.18
GROWW_SESSION_BOOK_MIN_REFRESH_SEC = 900.0
GROWW_SESSION_BOOK_URGENT_REFRESH_COOLDOWN_SEC = 30.0
GROWW_SESSION_BOOK_REQUIRE_TWO_SIDED_QUOTE = True
GROWW_OPTION_MAX_SELECTION_SPREAD_BPS = 120.0
GROWW_OPTION_MIN_BOOK_LOTS = 1.0
GROWW_OPTION_EXECUTION_ATR_PERIOD = 14
GROWW_OPTION_MAX_SPREAD_TO_1M_ATR = 0.35
GROWW_OPTION_MAX_QUOTE_STALE_SEC = 10.0
GROWW_EXECUTION_SIGNAL_DEFER_COOLDOWN_SEC = float(os.getenv("GROWW_EXECUTION_SIGNAL_DEFER_COOLDOWN_SEC", "5.0"))
GROWW_UNDERLYING_REST_REFRESH_SEC = 30.0
GROWW_UNDERLYING_REST_RECONCILE_SEC = 900.0
GROWW_OPTION_SLTP_DELTA_MULT = 1.00
GROWW_OPTION_MIN_PREMIUM_RISK_PCT = 0.14
GROWW_OPTION_MAX_PREMIUM_RISK_PCT = 0.58
GROWW_OPTION_MIN_TP_PREMIUM_PCT = 0.18
GROWW_OPTION_PREMIUM_TP_CONVEXITY_BONUS = 0.08

INDIAN_NO_FRESH_ENTRY_AFTER_CLOSE_BUFFER_MIN = 25
UNIVERSE_INCLUDE_EXCHANGES = os.getenv(
    "UNIVERSE_INCLUDE_EXCHANGES",
    "delta,coinswitch,groww" if (GROWW_ENABLED or GROWW_DISCOVERY_ENABLED) else "delta,coinswitch",
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

# Desk model: every desk uses the unified structural market_state authority.
# Archetypes compete under one execution/risk authority; evidence is not probability.
# Desks differ only in venue execution, risk, cost and lot-size policy.
STRATEGY_CORE_NAME = "INSTITUTIONAL_market_state_V514"
TELEGRAM_RECENT_TRADES_LIMIT = 30

TRADING_DESKS = {
    "BTC": {
        "display_name": "BTC Desk",
        "strategy": STRATEGY_CORE_NAME,
        "asset_ids": ("BTC",),
        "asset_classes": ("crypto",),
        "risk_multiplier": POLICY_CRYPTO_RISK_MULT,
        "margin_pct": INSTITUTIONAL_MARGIN_PCT,
        "tick_eval_sec": ENTRY_EVALUATION_INTERVAL_SECONDS,
        "loop_interval_sec": POLICY_CRYPTO_LOOP_INTERVAL_SEC,
        "min_1m_bars": MIN_CANDLES_1M,
        "min_5m_bars": MIN_CANDLES_5M,
        "min_rr": MIN_RISK_REWARD_RATIO,
        "max_rr": INSTITUTIONAL_TP_MAX_RR,
        "max_hold_sec": INSTITUTIONAL_MAX_HOLD_SEC,
        "cooldown_sec": MIN_TIME_BETWEEN_TRADES_SEC,
        "sl_buffer_atr": INSTITUTIONAL_SL_BUFFER_ATR_MULT,
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
        "enabled": GROWW_OPTIONS_RUNTIME_ENABLED,
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
TELEGRAM_LONG_POLL_TIMEOUT_SEC = _float_env("TELEGRAM_LONG_POLL_TIMEOUT_SEC", 2.0)  # fast graceful container stop


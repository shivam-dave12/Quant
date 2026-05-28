"""
config.py — Unified Configuration v11.0
=========================================
Single source of truth. All non-secret runtime and trading policy is inline.
The .env file contains credentials only; it cannot change live/shadow mode,
trading venues, risk, feed requirements, models, or log cadence.

Risk-controlled configuration for structural institutional liquidity execution.
"""
import os
try:
    from dotenv import load_dotenv
except ImportError:  # production image may not ship python-dotenv
    def load_dotenv(*_a, **_kw):
        return False
load_dotenv()

# ── OPERATOR CONTROL PANEL — change policy only here, never in .env ───────────
# LIVE TRADING MASTER SWITCH. This build authorises protected crypto execution
# only on the three cross-venue routes below. It still fails closed at import if
# a listed venue has no credentials or lacks a protected-order lifecycle.
LIVE_TRADING_ENABLED = True

# Data is collected from all three crypto venues plus the Groww/NIFTY analysis
# feed. Groww remains available for NIFTY analysis but is NOT live-authorised in
# this three-crypto-venue release until its separate compliance prerequisites are enabled.
ANALYSIS_DATA_VENUES = ("delta", "coinswitch", "hyperliquid", "groww")
LIVE_EXECUTION_VENUES = ("delta", "coinswitch", "hyperliquid")
# Legacy execution default is retained only for old one-venue callers. It must not
# make Delta the signal origin or route preference in the multi-venue scanner.
EXECUTION_EXCHANGE = "delta"
DISCOVERY_PRIMARY_EXCHANGE = ""  # no configured broker gets signal/routing priority
INDEPENDENT_VENUE_SIGNAL_ORIGINATION_ENABLED = True
EXECUTION_ASYNC_ENTRY_LIFECYCLE_ENABLED = True
CROSS_VENUE_RAW_PRICE_ROUTING_ENABLED = False
VENUE_ROUTE_PRICE_ADVANTAGE_CREDIT_ENABLED = False
COINSWITCH_REQUIRE_LIVE_ORDERBOOK_FOR_EXECUTION = True

# Venue activation / environments are runtime policy, not secrets.
DELTA_TESTNET = False
GROWW_ENABLED = True
HYPERLIQUID_REFERENCE_ENABLED = True
HYPERLIQUID_EXECUTION_ENABLED = True
COINSWITCH_EXECUTION_ENABLED = True
HYPERLIQUID_TESTNET = False
HYPERLIQUID_RECONNECT_SEC = 3.0
HYPERLIQUID_PERP_DEXS = ("", "xyz", "km")
HYPERLIQUID_REFERENCE_COIN_BY_ASSET = {
    "BTC": "BTC",
    # These are distinct exposure groups; no raw-price substitution is permitted.
    "GOLD_HL": "xyz:GOLD",
    "SILVER_HL": "xyz:SILVER",
    "NATGAS": "xyz:NATGAS",
}
# No permanent venue preference: route from live executable economics,
# collateral and protection feasibility on each approved candidate.
PREFERRED_EXECUTION_VENUE_BY_ASSET = {}

# ── Credentials — .env may contain ONLY values in this section ────────────────
DELTA_API_KEY             = os.getenv("DELTA_API_KEY",    "")
DELTA_SECRET_KEY          = os.getenv("DELTA_SECRET_KEY", "")
COINSWITCH_API_KEY        = os.getenv("COINSWITCH_API_KEY",    "")
COINSWITCH_SECRET_KEY     = os.getenv("COINSWITCH_SECRET_KEY", "")

def _first_env(*names: str) -> str:
    for name in names:
        value = os.getenv(name, "")
        if str(value or "").strip():
            return str(value).strip()
    return ""

HYPERLIQUID_MAIN_API_KEY   = _first_env("HYPERLIQUID_MAIN_API_KEY", "HYPERLIQUID_ACCOUNT_ADDRESS")
HYPERLIQUID_WALLET_API_KEY = _first_env("HYPERLIQUID_WALLET_API_KEY", "HYPERLIQUID_API_WALLET_ADDRESS")
HYPERLIQUID_PRIVATE_KEY    = _first_env("HYPERLIQUID_PRIVATE_KEY")

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
TELEGRAM_BOT_TOKEN        = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID          = os.getenv("TELEGRAM_CHAT_ID",   "")

if not DELTA_API_KEY and not COINSWITCH_API_KEY and not GROWW_AUTH_CONFIGURED and not HYPERLIQUID_PRIVATE_KEY:
    raise ValueError("No exchange credentials in .env. For Groww TOTP set GROWW_TOTP_TOKEN and GROWW_TOTP_SECRET.")

# ── Symbol / Leverage ─────────────────────────────────────────────────────────
SYMBOL                   = "BTCUSDT"
LEVERAGE                 = 25  # aggressive, still bounded by live product caps, margin budget and SL-risk sizing
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

# Hyperliquid protected-entry lifecycle. Entry is a priced limit; after an
# acknowledged fill the adapter submits reduce-only TP/SL trigger orders. If
# protection cannot be armed, the adapter sends a reduce-only market close when
# enabled below and reports the failure to Telegram through the strategy.
HYPERLIQUID_ENTRY_FILL_TIMEOUT_SEC = 45.0
HYPERLIQUID_ENTRY_POLL_SEC = 1.0
# Protected-position reconciliation is broker I/O and runs off the market-decision
# worker. Venue-native TP/SL remains responsible for immediate protection.
POSITION_RECONCILIATION_REFRESH_SEC = 1.0
POSITION_RECONCILIATION_FLAT_UNCONFIRMED_SEC = 2.0
HYPERLIQUID_ENTRY_CANCEL_RECONCILE_SEC = 3.0
HYPERLIQUID_ENTRY_CANCEL_RECONCILE_POLL_SEC = 0.25
HYPERLIQUID_TRIGGER_MARKET_SLIPPAGE_PCT = 0.10
HYPERLIQUID_EMERGENCY_CLOSE_ON_PROTECTION_FAILURE = True
HYPERLIQUID_PROTECTION_FAILURE_CLOSE_SLIPPAGE_PCT = 0.05
# Default applies only to native/unprofiled products. HIP-3 commodity products
# must use the exchange/deployer-verified capability profile below.
HYPERLIQUID_USE_CROSS_MARGIN = True
HYPERLIQUID_PRODUCT_CAPABILITIES = {
    # Verified against trade[XYZ] Specification Index (2026-05-28).
    # Normal Isolated allows isolated margin; cross margin is not permitted.
    "xyz:CL": {"margin_mode": "isolated", "max_leverage": 20, "capability_source": "trade_xyz_specification_index"},
    "xyz:NATGAS": {"margin_mode": "isolated", "max_leverage": 10, "capability_source": "trade_xyz_specification_index"},
    # Existing HIP-3 metals are explicitly cross-enabled by the same specification.
    "xyz:GOLD": {"margin_mode": "cross", "max_leverage": 25, "capability_source": "trade_xyz_specification_index"},
    "xyz:SILVER": {"margin_mode": "cross", "max_leverage": 25, "capability_source": "trade_xyz_specification_index"},
}

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
INSTITUTIONAL_ENABLE_LIVE_ENTRIES = LIVE_TRADING_ENABLED  # backwards-compatible internal alias
INSTITUTIONAL_REQUIRE_BTC_CROSS_VENUE = False  # legacy hard veto retired; cross-venue evidence is continuous risk input
INSTITUTIONAL_MIN_EXECUTION_QUALITY = 0.40
INSTITUTIONAL_MIN_FLOW_AGREEMENT = 0.55  # retained for telemetry only; never a binary entry veto
INSTITUTIONAL_MAX_CROSS_VENUE_DISPERSION_BPS = 15.0
INSTITUTIONAL_MIN_SIGNAL_BPS = 0.50
INSTITUTIONAL_MIN_NET_EDGE_BPS = 3.0
INSTITUTIONAL_FLOW_OFI_WEIGHT = 1.0
INSTITUTIONAL_FLOW_TFI_WEIGHT = 0.30
INSTITUTIONAL_FLOW_MICROPRICE_WEIGHT = 0.35
INSTITUTIONAL_FLOW_DISLOCATION_WEIGHT = 0.00  # raw venue dislocation is diagnostic unless an independently validated RV model exists
# Venue-local market-state alpha: robust shrinkage of confirmed displacement,
# range acceptance and live impulse; it is additive alpha, not a loose filter.
INSTITUTIONAL_ENABLE_MARKET_STATE_ALPHA = True
INSTITUTIONAL_MARKET_STATE_ASSETS = ("BTC", "GOLD_PAXG", "GOLD_HL", "SILVER_SLVON", "SILVER_XAG", "SILVER_HL", "OIL", "NATGAS")
INSTITUTIONAL_MARKET_STATE_PARAMETERS = {
    "DEFAULT": {"capture_rate": 0.18, "max_alpha_bps": 18.0, "acceptance_weight": 0.25, "live_impulse_weight": 0.15, "min_confidence": 0.25},
    "BTC": {"capture_rate": 0.24, "max_alpha_bps": 32.0, "acceptance_weight": 0.30, "live_impulse_weight": 0.22, "min_confidence": 0.25},
    "GOLD_PAXG": {"capture_rate": 0.28, "max_alpha_bps": 34.0, "acceptance_weight": 0.35, "live_impulse_weight": 0.15, "min_confidence": 0.30},
    "GOLD_HL": {"capture_rate": 0.28, "max_alpha_bps": 34.0, "acceptance_weight": 0.35, "live_impulse_weight": 0.15, "min_confidence": 0.30},
    "SILVER_SLVON": {"capture_rate": 0.20, "max_alpha_bps": 28.0, "acceptance_weight": 0.30, "live_impulse_weight": 0.12, "min_confidence": 0.32},
    "SILVER_XAG": {"capture_rate": 0.24, "max_alpha_bps": 32.0, "acceptance_weight": 0.32, "live_impulse_weight": 0.14, "min_confidence": 0.30},
    "SILVER_HL": {"capture_rate": 0.24, "max_alpha_bps": 32.0, "acceptance_weight": 0.32, "live_impulse_weight": 0.14, "min_confidence": 0.30},
    "OIL": {"capture_rate": 0.22, "max_alpha_bps": 32.0, "acceptance_weight": 0.30, "live_impulse_weight": 0.15, "min_confidence": 0.30},
    # New energy contract inherits the uncalibrated energy commodity prior until live research approves a bespoke model.
    "NATGAS": {"capture_rate": 0.22, "max_alpha_bps": 32.0, "acceptance_weight": 0.30, "live_impulse_weight": 0.15, "min_confidence": 0.30},
}
INSTITUTIONAL_MICROSTRUCTURE_ALPHA_CAP_BPS = {"BTC": 22.0, "GOLD_PAXG": 18.0, "GOLD_HL": 18.0, "SILVER_SLVON": 15.0, "SILVER_XAG": 18.0, "SILVER_HL": 18.0, "OIL": 18.0, "NATGAS": 18.0}
# Parent/child signal hierarchy for every live directional underlying desk.
# Closed-candle structural state originates or reverses a position; order-book /
# tape flow may time execution only in that parent direction. This is not a veto
# layer: it defines the investable thesis and prevents microstructure-only churn.
INSTITUTIONAL_PARENT_THESIS_EXECUTION_MODEL_ENABLED = True
INSTITUTIONAL_PARENT_THESIS_ASSETS = (
    "BTC", "GOLD_PAXG", "GOLD_HL", "SILVER_SLVON", "SILVER_XAG", "SILVER_HL", "OIL", "NATGAS",
)
INSTITUTIONAL_PARENT_TIMING_CONTRIBUTION_CAP_FRACTION = 0.35
# Venue disagreement raises uncertainty and scales confidence continuously. A
# leader venue may still trade a genuine impulse before followers converge.
INSTITUTIONAL_CROSS_VENUE_MODE = "continuous_confidence"
INSTITUTIONAL_CROSS_VENUE_UNCERTAINTY_MAX_BPS = 6.0
INSTITUTIONAL_RELATIVE_VALUE_ALPHA_ENABLED = False
# Product-aware composite intelligence policy. Data from all verified venues may
# contribute normalised factor evidence, but execution alpha transfers only
# inside a validated fungible/execution-equivalence group. Related products such
# as PAXG vs HIP-3 GOLD and SLVON vs XAG/SILVER are context-only until a fitted,
# formally approved basis/hedge-ratio model is installed below.
INSTITUTIONAL_FACTOR_BY_ASSET = {
    "BTC": "BTC", "OIL": "OIL", "NATGAS": "NATGAS",
    "GOLD_PAXG": "GOLD", "GOLD_HL": "GOLD",
    "SILVER_SLVON": "SILVER", "SILVER_XAG": "SILVER", "SILVER_HL": "SILVER",
}
INSTITUTIONAL_EXECUTION_EQUIVALENCE_GROUP_BY_ASSET = {
    "BTC": "BTC_LINEAR_PERP",
    "OIL": "OIL_HL_ONLY",
    "NATGAS": "NATGAS_HL_ONLY",
    "GOLD_PAXG": "PAXG_TOKEN_PERP",
    "GOLD_HL": "GOLD_HIP3_ONLY",
    "SILVER_SLVON": "SLVON_TOKEN_PERP_ONLY",
    "SILVER_XAG": "XAG_PERP_ONLY",
    "SILVER_HL": "SILVER_HIP3_ONLY",
}
INSTITUTIONAL_FACTOR_TRANSFER_MODE_BY_ASSET = {
    "BTC": "TRANSFERABLE_EXECUTION_ALPHA",
    "OIL": "TRANSFERABLE_EXECUTION_ALPHA",
    "NATGAS": "TRANSFERABLE_EXECUTION_ALPHA",
    "GOLD_PAXG": "CONFIDENCE_ONLY",
    "GOLD_HL": "CONFIDENCE_ONLY",
    "SILVER_SLVON": "CONFIDENCE_ONLY",
    "SILVER_XAG": "CONFIDENCE_ONLY",
    "SILVER_HL": "CONFIDENCE_ONLY",
}
INSTITUTIONAL_FACTOR_EVIDENCE_MAX_STALENESS_SEC = 8.0
INSTITUTIONAL_VALIDATED_FACTOR_TRANSLATION_MODELS = {}  # disabled until fitted/approved research exists
INSTITUTIONAL_COMPOSITE_INTELLIGENCE_ENABLED = True
INSTITUTIONAL_RISK_FRACTION_PER_TRADE = 0.025
INSTITUTIONAL_FRACTIONAL_KELLY = 0.60
INSTITUTIONAL_QUARTER_KELLY = INSTITUTIONAL_FRACTIONAL_KELLY  # legacy name used by tests/extensions
INSTITUTIONAL_TARGET_OBSERVATION_VOL_BPS = 10.0
INSTITUTIONAL_MAX_SELECTED_LEVERAGE = 25.0
INSTITUTIONAL_CORRELATED_EXPOSURE_CAP_FRACTION = 8.0
RESEARCH_STORE_PATH = "research_output"

# Cross-venue execution venue selection. Books remain isolated per exchange;
# selection prices actual side/touch, depth impact, round-trip fees, funding,
# latency, available collateral and confirmed hard-protection capability.
VENUE_SELECTION_ENABLED = True
VENUE_SELECTION_MIN_IMPROVEMENT_BPS = 0.50
VENUE_SELECTION_MARGIN_FRACTION = 0.85
VENUE_SELECTION_NOTIONAL_FRACTION = VENUE_SELECTION_MARGIN_FRACTION  # legacy alias; now interpreted as broker-local margin fraction
VENUE_SELECTION_MIN_FREE_MARGIN_USD = 1.00
VENUE_BALANCE_CACHE_TTL_SEC = 8.0
VENUE_SELECTION_MAX_COST_BPS = 100.0
# Route approval must compare exchange candidates at one base exposure quantity
# from one decision snapshot; any broker-local size/capital values are eligibility
# telemetry only and cannot distort the cross-venue cost ranking.
VENUE_SELECTION_RISK_NORMALISED_LEDGER_ENABLED = True
VENUE_SELECTION_MAX_QUANTITY_REPRESENTATION_ERROR_BPS = 0.50
# Expected round-trip cost: maker entry plus protected-market exit. Replace
# these approved-account assumptions when a venue/account fee tier changes.
VENUE_ROUND_TRIP_FEE_BPS = {
    "delta": 3.00,
    # CoinSwitch instrument_info fee fields override this value live; 13 bps is
    # the conservative taker+taker fallback from the documented BTCUSDT sample.
    "coinswitch": 13.00,  # CoinSwitch instrument_info default 2.4bp maker + 6.5bp taker
    "hyperliquid": 7.00,
}
# Backwards-compatible alias for telemetry/readers still expecting this name.
VENUE_FEE_BPS = VENUE_ROUND_TRIP_FEE_BPS
VENUE_SLIPPAGE_IMPACT_MULTIPLIER = 35.0
VENUE_FUNDING_INTERVAL_HOURS = 8.0
VENUE_EXPECTED_HOLDING_HOURS = 8.0
VENUE_ALLOW_FUNDING_CREDIT = False
VENUE_MAX_FUNDING_CREDIT_BPS = 5.0
VENUE_LATENCY_TOLERANCE_MS = 250.0
VENUE_LATENCY_PENALTY_BPS_PER_SEC = 2.0
VENUE_MARKET_META_REFRESH_SEC = 30.0
# CoinSwitch documents position-level TP/SL after a filled position, not an
# atomic attached-entry bracket. This reserve prices the protection activation
# window into venue selection without altering signal quality or direction.
VENUE_NON_ATOMIC_PROTECTION_RISK_RESERVE_BPS = {"coinswitch": 5.0}
# Hard venue preference is disabled; a venue is penalised only when its live
# displayed depth cannot cover the intended execution notional.
SILVER_HYPERLIQUID_PREFERENCE_BPS = 0.0
SILVER_DELTA_ILLIQUIDITY_PENALTY_BPS = 15.0
SILVER_DELTA_MIN_NEAR_DEPTH_USD = 50000.0
# CoinSwitch protected entry lifecycle: limit entry -> confirmed fill ->
# documented position-level STOP_MARKET + TAKE_PROFIT_MARKET reduce-only arms.
COINSWITCH_ENTRY_FILL_TIMEOUT_SEC = 45.0
COINSWITCH_ENTRY_POLL_SEC = 1.0
COINSWITCH_EMERGENCY_CLOSE_ON_PROTECTION_FAILURE = True

# ── Dynamic state-dependent TP/SL and exit model ─────────────────────────────
# One protection authority for BTC, Delta commodities and Groww long options.
# Hard exchange protection is always attached; model readiness is required before
# a live entry can be approved. No fixed ATR×RR protection geometry remains.
DYNAMIC_PROTECTION_ENABLED = True
DYNAMIC_PROTECTION_SIGNAL_SAMPLE_SEC = 1.0
DYNAMIC_PROTECTION_MAX_SIGNAL_SAMPLES = 360
DYNAMIC_PROTECTION_MIN_SIGNAL_OBSERVATIONS = 12
DYNAMIC_PROTECTION_MIN_HALF_LIFE_SEC = 2.0
DYNAMIC_PROTECTION_MAX_HALF_LIFE_SEC = 3600.0
DYNAMIC_PROTECTION_MAX_OPTIMAL_HOLD_SEC = 3600.0
DYNAMIC_PROTECTION_REQUIRE_SIGNAL_DECAY_READY = True
DYNAMIC_PROTECTION_MIN_KYLE_OBSERVATIONS = 20
DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_FOR_DELTA = True  # legacy alias
DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_VENUES = ("delta",)
DYNAMIC_PROTECTION_MIN_VPIN_BUCKETS = 5
DYNAMIC_PROTECTION_VPIN_WINDOW_BUCKETS = 20
DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_FOR_DELTA = True  # legacy alias
DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_VENUES = ("delta",)
DYNAMIC_PROTECTION_VPIN_STOP_MULT_MIN = 0.80
DYNAMIC_PROTECTION_VPIN_STOP_MULT_MAX = 1.80
DYNAMIC_PROTECTION_VOL_STOP_MULT = 1.25
DYNAMIC_PROTECTION_MIN_STOP_BPS = 8.0
DYNAMIC_PROTECTION_MARKET_AWARE_GEOMETRY_ENABLED = True
DYNAMIC_PROTECTION_SPREAD_STOP_MULT = 6.0
DYNAMIC_PROTECTION_MIN_STOP_TICKS = 12.0
DYNAMIC_PROTECTION_COST_STOP_MULT = 2.5
DYNAMIC_PROTECTION_DEPTH_STRESS_MIN_COVERAGE = 4.0
DYNAMIC_PROTECTION_DEPTH_STRESS_STOP_BPS = 18.0
DYNAMIC_PROTECTION_ASSET_MIN_STOP_BPS = {
    "BTC": 10.0,
    "GOLD_PAXG": 22.0,
    "GOLD_HL": 22.0,
    "SILVER_SLVON": 65.0,
    "SILVER_XAG": 55.0,
    "SILVER_HL": 55.0,
}
DYNAMIC_PROTECTION_VENUE_ASSET_MIN_STOP_BPS = {
    "hyperliquid:SILVER_HL": 55.0,
    "delta:SILVER_SLVON": 65.0,
}
DYNAMIC_PROTECTION_ASSET_MIN_TARGET_BPS = {
    "BTC": 20.0,
    "GOLD_PAXG": 45.0,
    "GOLD_HL": 45.0,
    "SILVER_SLVON": 100.0,
    "SILVER_XAG": 100.0,
    "SILVER_HL": 100.0,
}
# A short-lived OFI/TFI burst cannot justify a wide structural bracket. Entries
# fail closed when the estimated alpha life is shorter than protected execution.
DYNAMIC_PROTECTION_MIN_EXECUTABLE_HOLD_SEC_BY_ASSET = {
    "BTC": 5.0,
    "OIL": 20.0,
    "NATGAS": 20.0,
    "GOLD_PAXG": 30.0,
    "GOLD_HL": 20.0,
    "SILVER_SLVON": 60.0,
    "SILVER_XAG": 30.0,
    "SILVER_HL": 30.0,
}
DYNAMIC_PROTECTION_RR_FLOOR = 1.15
DYNAMIC_PROTECTION_OPTION_RR_FLOOR = 1.10
DYNAMIC_PROTECTION_RR_CAP = 5.0
DYNAMIC_PROTECTION_MAX_LIQUIDATION_HORIZON_SEC = 300.0
DYNAMIC_PROTECTION_LIQUIDATION_STEP_SEC = 15.0
DYNAMIC_PROTECTION_MAX_LIQUIDATION_STEPS = 8
DYNAMIC_PROTECTION_AC_RISK_AVERSION = 1.0
# Groww long-option Greek exit diagnostics; live close routing remains protected
# and must not cancel an OCO without an acknowledged replacement/close path.
DYNAMIC_OPTION_EXIT_MIN_ABS_DELTA = 0.10
DYNAMIC_OPTION_EXIT_IV_COLLAPSE_ABS = 0.02
DYNAMIC_OPTION_EXIT_THETA_TO_PREMIUM_PER_DAY = 0.08
DYNAMIC_OPTION_CHEAP_VRP_THRESHOLD = -0.10
DYNAMIC_OPTION_CHEAP_VRP_THETA_LIMIT = 0.12
DYNAMIC_OPTION_THETA_DTE_LIMIT = 5.0
# A modelled alpha horizon is a re-evaluation timestamp, never a market-close
# instruction by itself.  A live position is closed early only after post-fill,
# same-direction economics have been invalidated by repeated executable evidence
# (or profitable residual-edge exhaustion) while native SL/TP remains armed.
DYNAMIC_EXIT_AUTOMATED_EARLY_LIQUIDATION_ENABLED = True
# The entry-time AR(1) estimate is telemetry for research and protective-order
# geometry only. It is never an early-liquidation clock or a parent-thesis gate.
DYNAMIC_EXIT_CLOCK_HORIZON_IS_REASSESSMENT_ONLY = True
DYNAMIC_EXIT_REQUIRE_LIVE_THESIS_CONFIRMATION = True
DYNAMIC_EXIT_PARENT_STRUCTURE_ONLY = True
# Dynamic discretionary exits follow the same parent-thesis authority as entry.
# Options retain their separate Greek/volatility lifecycle; every directional
# underlying desk below requires distinct closed-state structural invalidation.
DYNAMIC_EXIT_PARENT_STRUCTURE_ASSETS = INSTITUTIONAL_PARENT_THESIS_ASSETS
DYNAMIC_EXIT_ALLOW_MICROSTRUCTURE_ONLY_INVALIDATION = False
DYNAMIC_EXIT_ALLOW_RESIDUAL_ALPHA_PROFIT_CAPTURE = False
# Each confirmation must represent a distinct closed parent-state observation,
# never repeated evaluation of the same intrabar impulse. Native SL/TP protects
# adverse motion while the structural thesis is being re-estimated.
DYNAMIC_EXIT_MIN_DISTINCT_PARENT_OBSERVATIONS = 2
DYNAMIC_EXIT_MIN_CONSECUTIVE_CONFIRMATIONS = 2
DYNAMIC_EXIT_CONFIRMATION_HALF_LIFE_FRACTION = 0.0
DYNAMIC_EXIT_PROFIT_CAPTURE_REQUIRES_COST_COVERAGE = True
DYNAMIC_EXIT_REDUCE_ONLY_RETRY_SEC = 1.0
DYNAMIC_EXIT_MAX_SUBMISSION_ATTEMPTS = 5
DYNAMIC_EXIT_CANCEL_RESIDUAL_PROTECTION_AFTER_CONFIRMED_FLAT = True

# ── Institutional Strategy ────────────────────────────────────────────────────────────
INSTITUTIONAL_MARGIN_PCT               = 0.85
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
GROWW_DISCOVERY_ENABLED = True
GROWW_OPTIONS_RUNTIME_ENABLED = GROWW_ENABLED
GROWW_INDEX_OPTIONS_FROM_CONFIG_ONLY = True
GROWW_INDEX_UNDERLYINGS = "NIFTY"
GROWW_INDEX_STOCK_CODE_BY_UNDERLYING = {"NIFTY": "NIFTY", "NIFTY50": "NIFTY", "CNXNIFTY": "NIFTY"}
GROWW_OPTION_PRODUCT_TYPE = "NRML"
GROWW_OPTION_TICK_SIZE = 0.05
GROWW_MIN_CALL_GAP_SEC = 0.25
GROWW_INSTRUMENTS_CSV_URL = "https://growwapi-assets.groww.in/instruments/instrument.csv"
GROWW_INSTRUMENT_CACHE_TTL_SEC = 1800.0
GROWW_SECURITY_MASTER_CACHE_PATH = "data/groww_instruments.csv"
GROWW_SECURITY_MASTER_REQUIRE_TODAY = True
GROWW_MARKET_SESSION_GUARD_ENABLED = True
GROWW_MARKET_OPEN_TIME = "09:15"
GROWW_MARKET_CLOSE_TIME = "15:30"
# Verified exchange reference data: NSE Equity & Equity Derivatives trading holidays, 2026.
# Source: NSE India Market Timings & Holidays calendar, verified 2026-05-28.
# This belongs to the session control-plane: a closed exchange must never be
# diagnosed as a broken Groww feed or enter broker preflight/retry loops.
GROWW_MARKET_HOLIDAY_CALENDAR_SOURCE = "NSE India Equity/Equity Derivatives trading holidays 2026; verified 2026-05-28"
GROWW_MARKET_HOLIDAYS: tuple[str, ...] = (
    "2026-01-15",  # Municipal Corporation Election - Maharashtra
    "2026-01-26",  # Republic Day
    "2026-03-03",  # Holi
    "2026-03-26",  # Shri Ram Navami
    "2026-03-31",  # Shri Mahavir Jayanti
    "2026-04-03",  # Good Friday
    "2026-04-14",  # Dr. Baba Saheb Ambedkar Jayanti
    "2026-05-01",  # Maharashtra Day
    "2026-05-28",  # Bakri Id
    "2026-06-26",  # Muharram
    "2026-09-14",  # Ganesh Chaturthi
    "2026-10-02",  # Mahatma Gandhi Jayanti
    "2026-10-20",  # Dussehra
    "2026-11-10",  # Diwali-Balipratipada
    "2026-11-24",  # Prakash Gurpurb Sri Guru Nanak Dev
    "2026-12-25",  # Christmas
)
GROWW_ANALYZE_ONLY_DURING_MARKET_SESSION = True
GROWW_ALLOW_CLOSED_MARKET_HISTORICAL_WARMUP = True
GROWW_ALLOW_CLOSED_MARKET_WARMUP = False
GROWW_INDEX_STREAM_ENABLED = True
GROWW_INDEX_WEBSOCKET_REQUIRED = True
GROWW_INDEX_STREAM_FIRST_TICK_TIMEOUT_SEC = 12.0
GROWW_INDEX_STREAM_MAX_STALE_SEC = 15.0
GROWW_WEBSOCKET_RECONNECT_COOLDOWN_SEC = 30.0
GROWW_SHARED_TRANSPORT_MAX_STALE_SEC = 20.0
GROWW_REQUIRE_UNDERLYING_ANALYSIS_FEED = True
GROWW_INDEX_STREAM_CHANNELS = "LIVE_QUOTE"
GROWW_INDEX_STREAM_SCRIPT_CODES = {}
GROWW_OPTION_STREAM_ENABLED = True
GROWW_OPTION_WEBSOCKET_REQUIRED = True
GROWW_SESSION_BOOK_REQUIRE_FIRST_OPTION_TICK_ON_STARTUP = False
GROWW_OPTION_STREAM_FIRST_TICK_TIMEOUT_SEC = 12.0
GROWW_OPTION_STREAM_MAX_STALE_SEC = 15.0
GROWW_REQUIRE_STATIC_IP_FOR_LIVE_ORDERS = True
GROWW_APPROVED_STATIC_IPS: tuple[str, ...] = ("13.51.193.234",)  # SET TO ("<YOUR_EC2_ELASTIC_IP>",) BEFORE GROWW LIVE
GROWW_OUTBOUND_IP_CHECK_URL = "https://api.ipify.org?format=json"
GROWW_OUTBOUND_IP_OVERRIDE = ""  # testing only; leave empty in production
# Groww SDK exposes order_reference_id (8-20 chars) for traceability. Broker-side
# algo registration/whitelisting must be confirmed before live India execution.
GROWW_REQUIRE_SEBI_ALGO_CONFIRMATION_FOR_LIVE_ORDERS = True
GROWW_SEBI_ALGO_REGISTRATION_CONFIRMED = True  # set True only after broker confirmation
GROWW_SEBI_STRATEGY_PREFIX = "instv2"[:6]
GROWW_OPTION_MIN_LIVE_IV_COVERAGE = 0.60
GROWW_OPTION_LONG_MAX_VRP = -0.02

GROWW_NIFTY_TREND_liquidity_event_ENABLED = True
GROWW_NIFTY_TREND_liquidity_event_MIN_PHASE_SCORE = 0.30
GROWW_NIFTY_TREND_liquidity_event_AGGRESSIVE_PHASE_SCORE = 0.58
GROWW_NIFTY_TREND_liquidity_event_MIN_RR = 1.15
GROWW_NIFTY_TREND_liquidity_event_MAX_RR = 2.40
GROWW_NIFTY_TREND_liquidity_event_MAX_TARGET_ATR = 2.75
GROWW_NIFTY_TREND_liquidity_event_MAX_RECLAIM_EXTENSION_ATR = 0.65
GROWW_NIFTY_TREND_liquidity_event_1M_MAX_AGE_SEC = 90.0
GROWW_NIFTY_TREND_liquidity_event_5M_MAX_AGE_SEC = 360.0
GROWW_NIFTY_TREND_liquidity_event_STOP_BASE_ATR = 0.08
GROWW_NIFTY_TREND_liquidity_event_STOP_PCTL_SLOPE_ATR = 0.10
GROWW_NIFTY_TREND_liquidity_event_MIN_TARGET_REALISM = 0.42
GROWW_NIFTY_TREND_liquidity_event_MAX_HOLD_SEC = 720.0
GROWW_NIFTY_TREND_liquidity_event_FAILED_market_state_FRACTION = 0.35
GROWW_NIFTY_TREND_liquidity_event_FAILED_market_state_R = -0.15
GROWW_NIFTY_TREND_liquidity_event_FAILED_market_state_MAX_MFE_R = 0.30
GROWW_NIFTY_TREND_liquidity_event_MIN_PROGRESS_R = 0.15
GROWW_NIFTY_TREND_liquidity_event_HARD_MAX_MULT = 1.0

GROWW_OPTION_DEFAULT_LOT_SIZE = 0.0
GROWW_OPTION_MIN_DTE = 1.0
GROWW_OPTION_MAX_DTE = 21.0
GROWW_INDEX_OPTION_TARGET_ABS_DELTA = 0.45
GROWW_STOCK_OPTION_TARGET_ABS_DELTA = 0.50
GROWW_OPTION_DELTA_BAND = 0.22
# Observability/selection policy: theta is measured as hold-horizon carry, not a static session veto.
GROWW_OPTION_SELECTION_CARRY_REFERENCE_BPS = 100.0
GROWW_SESSION_BOOK_RESCAN_SEC = 30.0
# Live operator telemetry: transition-first and periodic, never a full JSON dump per scan tick.
INSTITUTIONAL_DECISION_TELEMETRY_ENABLED = True
INSTITUTIONAL_DECISION_TELEMETRY_HEARTBEAT_SEC = 60.0
INSTITUTIONAL_DECISION_TELEMETRY_FULL_ON_TRANSITION = False
INSTITUTIONAL_DECISION_TELEMETRY_LOG_UNQUALIFIED_SIGNAL_FLIPS = False
INSTITUTIONAL_DECISION_TELEMETRY_DEBUG_EVERY_TICK = False
GROWW_SESSION_MODEL_AUDIT_FULL_INFO = False
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
GROWW_EXECUTION_SIGNAL_DEFER_COOLDOWN_SEC = 5.0
GROWW_UNDERLYING_REST_REFRESH_SEC = 30.0
GROWW_UNDERLYING_REST_RECONCILE_SEC = 900.0
GROWW_OPTION_SLTP_DELTA_MULT = 1.00
# NIFTY signal stays in underlying/index units; execution risk stays in option-premium units.
GROWW_STRUCTURAL_BREAK_BUFFER_ATR = 0.15
GROWW_STRUCTURAL_MIN_ALIGNMENT_BPS = 2.0
GROWW_OPTION_PROTECTION_MIN_ATR_BARS = 10

INDIAN_NO_FRESH_ENTRY_AFTER_CLOSE_BUFFER_MIN = 25
UNIVERSE_INCLUDE_EXCHANGES = ",".join(ANALYSIS_DATA_VENUES)

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
    {"asset_id": "NATGAS", "display_name": "Henry Hub Natural Gas", "asset_class": "commodity", "aliases": ["xyz:NATGAS", "NATGAS", "NATURALGAS", "NATURAL GAS", "HENRYHUB", "NG"], "priority": 16},
    # Exposure-equivalence groups.  Do not route raw prices between tokenised
    # ETF derivatives and commodity/HIP-3 products without a validated basis model.
    {"asset_id": "GOLD_PAXG", "display_name": "PAXG token derivatives", "asset_class": "commodity", "aliases": ["PAXGUSD", "PAXGUSDT", "PAXG"], "priority": 11},
    {"asset_id": "GOLD_HL", "display_name": "Hyperliquid GOLD HIP-3", "asset_class": "commodity", "aliases": ["xyz:GOLD"], "priority": 12},
    {"asset_id": "SILVER_SLVON", "display_name": "SLV Ondo token derivative", "asset_class": "commodity", "aliases": ["SLVONUSD", "SLVON"], "priority": 13},
    {"asset_id": "SILVER_XAG", "display_name": "CoinSwitch XAG perpetual", "asset_class": "commodity", "aliases": ["XAGUSDT", "XAG/USDT", "XAG"], "priority": 14},
    {"asset_id": "SILVER_HL", "display_name": "Hyperliquid SILVER HIP-3", "asset_class": "commodity", "aliases": ["xyz:SILVER"], "priority": 15},

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
POLICY_COMMODITY_MARGIN_PCT = 0.65
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
POLICY_EQUITY_MARGIN_PCT = 0.50
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
STRATEGY_CORE_NAME = "INSTITUTIONAL_COMPOSITE_FACTOR_EXECUTION_V8"
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

# Telegram command-channel network resilience (policy lives in code, not .env)
TELEGRAM_GETUPDATES_BACKOFF_BASE_SEC = 2.0
TELEGRAM_GETUPDATES_BACKOFF_MAX_SEC = 30.0
TELEGRAM_LONG_POLL_TIMEOUT_SEC = 2.0  # fast graceful container stop


def validate_live_control_plane() -> None:
    """Fail closed on live-order policy before any desk can route an order."""
    errors: list[str] = []
    allowed = {"delta", "coinswitch", "groww", "hyperliquid"}
    live_venues = {str(v).strip().lower() for v in LIVE_EXECUTION_VENUES}
    unknown = live_venues - allowed
    if unknown:
        errors.append(f"unknown LIVE_EXECUTION_VENUES={sorted(unknown)}")
    if LIVE_TRADING_ENABLED and not live_venues:
        errors.append("LIVE_TRADING_ENABLED=True requires at least one LIVE_EXECUTION_VENUE")
    if LIVE_TRADING_ENABLED and "delta" in live_venues:
        if not DELTA_API_KEY or not DELTA_SECRET_KEY:
            errors.append("Delta live requires DELTA_API_KEY and DELTA_SECRET_KEY in .env")
    if LIVE_TRADING_ENABLED and "coinswitch" in live_venues:
        if not COINSWITCH_EXECUTION_ENABLED:
            errors.append("CoinSwitch live requires COINSWITCH_EXECUTION_ENABLED=True in config.py")
        if not COINSWITCH_API_KEY or not COINSWITCH_SECRET_KEY:
            errors.append("CoinSwitch live requires COINSWITCH_API_KEY and COINSWITCH_SECRET_KEY in .env")
    if LIVE_TRADING_ENABLED and "groww" in live_venues:
        if GROWW_REQUIRE_STATIC_IP_FOR_LIVE_ORDERS and not GROWW_APPROVED_STATIC_IPS:
            errors.append("Groww live requires GROWW_APPROVED_STATIC_IPS configured in config.py")
        if GROWW_REQUIRE_SEBI_ALGO_CONFIRMATION_FOR_LIVE_ORDERS and not GROWW_SEBI_ALGO_REGISTRATION_CONFIRMED:
            errors.append("Groww live requires GROWW_SEBI_ALGO_REGISTRATION_CONFIRMED=True after broker confirmation")
    if LIVE_TRADING_ENABLED and "hyperliquid" in live_venues:
        if not HYPERLIQUID_EXECUTION_ENABLED:
            errors.append("Hyperliquid live requires HYPERLIQUID_EXECUTION_ENABLED=True in config.py")
        if not HYPERLIQUID_PRIVATE_KEY:
            errors.append("Hyperliquid live requires HYPERLIQUID_PRIVATE_KEY in .env")
        if not HYPERLIQUID_MAIN_API_KEY:
            errors.append("Hyperliquid live requires HYPERLIQUID_MAIN_API_KEY/account address in .env")
    if errors:
        raise ValueError("Invalid live control plane: " + "; ".join(errors))


validate_live_control_plane()

# ── Institutional streaming/runtime architecture v9 ─────────────────────────
# Live signal evaluation must be I/O-free: REST is allowed only in async startup
# warmup or background state-refresh services. Hyperliquid publishes these live
# feeds through its official WebSocket subscription contract.
HYPERLIQUID_WS_CANDLE_INTERVALS = ("1m", "5m", "15m", "1h", "4h", "1d")
# One shared collateral authority service refreshes each broker independently.
# It retains the last verified snapshot through transient 429 responses and
# fails closed only after freshness expires; market ticks never query balances.
VENUE_BALANCE_REFRESH_SEC = 30.0  # isolated compatibility path only
VENUE_BALANCE_SNAPSHOT_MAX_AGE_SEC = 120.0  # legacy alias
BROKER_COLLATERAL_REFRESH_DELTA_SEC = 15.0
BROKER_COLLATERAL_REFRESH_COINSWITCH_SEC = 45.0
BROKER_COLLATERAL_REFRESH_HYPERLIQUID_SEC = 20.0
BROKER_COLLATERAL_REFRESH_DEFAULT_SEC = 30.0
BROKER_COLLATERAL_SNAPSHOT_MAX_AGE_SEC = 120.0
BROKER_COLLATERAL_MAX_BACKOFF_SEC = 300.0
BROKER_COLLATERAL_ERROR_LOG_THROTTLE_SEC = 60.0
HYPERLIQUID_ACCOUNT_MODE_CACHE_SEC = 900.0
HYPERLIQUID_SPOT_STATE_REFRESH_SEC = 15.0
HYPERLIQUID_SPOT_STATE_STALE_MAX_SEC = 120.0
# Hyperliquid rejects orders below USD 10 notional; routes smaller than this
# are non-executable and must be rejected before protected-order submission.
HYPERLIQUID_MIN_ORDER_NOTIONAL_USD = 10.0
SCANNER_CONTEXT_WORKERS_ENABLED = True
SCANNER_MARKET_TICK_MAX_LATENCY_MULTIPLIER = 2.0
INSTITUTIONAL_TELEMETRY_SELECTED_VENUE_REQUIRED = True

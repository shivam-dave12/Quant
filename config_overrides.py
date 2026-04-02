"""
config_overrides.py — Institutional Parameter Set v3.0
======================================================
ADD or REPLACE these values in your root config.py.

These are NOT tweaks — they are the institutional calibration that matches
the logic in the rewritten conviction_filter.py and liquidity_trail.py.

The v1 parameters were calibrated for a high-frequency scalping bot.
These parameters are calibrated for an institutional ICT/SMC bot that
takes 3-6 high-conviction trades per session with 65-75% hit rate.
"""

# ═══════════════════════════════════════════════════════════════════════════
# CONVICTION FILTER — institutional calibration
# ═══════════════════════════════════════════════════════════════════════════

# Minimum weighted conviction score to allow entry.
# Calibrated for 65-75% WR.  Lower = more trades + lower WR.
CONVICTION_MIN_SCORE = 0.72

# Pool must be 15m+ effective timeframe for entry.
# 1m/5m pools are noise — they form and dissolve every few minutes.
# TF ranks: 1m=1, 5m=2, 15m=3, 1h=4, 4h=5, 1d=6
CONVICTION_POOL_MIN_TF_RANK = 3

# Displacement candle body must be >= this fraction of ATR.
# This is the proof of institutional intent.  Weak displacement = noise sweep.
CONVICTION_DISPLACEMENT_BODY_ATR = 0.70

# OTE (Optimal Trade Entry) Fibonacci zone.
# Golden pocket: 61.8% - 78.6% is where institutions fill.
# Standard OTE: 50% - 78.6%.
CONVICTION_OTE_FIB_LOW = 0.500
CONVICTION_OTE_FIB_HIGH = 0.786

# Minimum Risk:Reward ratio.
# Institutions require 2:1 minimum.  Below this, the edge is too thin
# to survive commission, slippage, and the occasional noise stop-out.
CONVICTION_MIN_RR = 2.0

# Maximum consecutive losses before session circuit breaker.
# 2 consecutive losses = your thesis for this session is wrong.  Stop.
CONVICTION_MAX_SESSION_LOSSES = 2

# Minimum seconds between entries.
# 300s = 5 minutes.  This prevents revenge trading and signal spam.
# Institutional pace: one trade per 15-30 minutes is normal.
CONVICTION_MIN_ENTRY_INTERVAL_SEC = 300

# Maximum entries per kill zone session (London / NY / Asia).
# 5 trades per session is the institutional norm.
# More than this = overtrading = edge dilution.
CONVICTION_MAX_ENTRIES_PER_SESSION = 5


# ═══════════════════════════════════════════════════════════════════════════
# RISK MANAGEMENT — institutional limits
# ═══════════════════════════════════════════════════════════════════════════

# Max trades per calendar day across ALL sessions.
MAX_DAILY_TRADES = 10

# Max consecutive losses before time-based lockout.
MAX_CONSECUTIVE_LOSSES = 2

# Max daily P&L drawdown (%) before full lockout.
MAX_DAILY_LOSS_PCT = 3.0

# Cooldown after any position exit (seconds).
QUANT_COOLDOWN_SEC = 300

# Time lockout after MAX_CONSECUTIVE_LOSSES hit (seconds).
QUANT_LOSS_LOCKOUT_SEC = 5400   # 90 minutes


# ═══════════════════════════════════════════════════════════════════════════
# ENTRY ENGINE — institutional thresholds
# ═══════════════════════════════════════════════════════════════════════════

# Minimum R:R for any entry type.
MIN_RISK_REWARD_RATIO = 1.50

# Evaluation interval — don't process every 250ms tick.
ENTRY_EVALUATION_INTERVAL_SECONDS = 2

# SL minimum distance from entry (percentage of price).
# BTC at $66K: 0.004 = $264 minimum SL distance = ~2.3 ATR.
MIN_SL_DISTANCE_PCT = 0.004

# SL maximum distance from entry.
MAX_SL_DISTANCE_PCT = 0.025


# ═══════════════════════════════════════════════════════════════════════════
# TRAILING SL — aligned with liquidity_trail.py v3.0
# ═══════════════════════════════════════════════════════════════════════════

# These are used by the chandelier fallback path (when liquidity trail
# cannot find a structural anchor).  The liquidity trail engine has its
# own internal constants for phase-based trailing.

QUANT_TRAIL_BE_R = 1.0        # Break-even lock at 1.0R (was 0.3)
QUANT_TRAIL_LOCK_R = 2.0      # Structural trail starts at 2.0R (was 0.8)
QUANT_TRAIL_AGGRESSIVE_R = 3.5 # Aggressive trail at 3.5R+ (was 1.5)

# Minimum SL improvement to trigger exchange API call (ATR multiples).
SL_MIN_IMPROVEMENT_ATR_MULT = 0.20

# Minimum breathing room between SL and current price.
QUANT_TRAIL_LIQ_MIN_BREATHING_ATR = 1.00

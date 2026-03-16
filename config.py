"""
config.py — Single source of truth for all bot parameters.
============================================================
Quant Bot v4.6 — Thesis-Aware Exit + Gate Fixes

CHANGES from v4.5 (v4.6 — CRITICAL WIN-RATE FIX):
  ROOT CAUSE: Max-hold timer (40 min) killed correct trades before they could
  reach TP. Example: LONG @ $71,452, price dipped to $71,318, timer fired
  at $71,349 → exited for loss. Price then rallied to $71,900 → TP hit.
  The trade thesis was CORRECT the entire time — timer destroyed the edge.

  FIX 1: THESIS-AWARE MAX-HOLD (the primary fix)
    - When max-hold fires, CHECK THE MARKET instead of blind exit
    - If in profit → tighten SL, let trade ride (no forced exit)
    - If underwater + thesis valid → EXTEND hold (up to 5 × 20 min)
    - If thesis broken → exit
    - Thesis valid = composite in direction + not deeply underwater + VWAP relation holds
    - New: MAX_HOLD_EXTENSIONS=5, HOLD_EXTENSION_SEC=1200, THESIS_MAX_DRAWDOWN_PCT=0.70
    - Total possible hold: 40 + 5×20 = 140 min (2.3 hours) for valid thesis

  FIX 2: LOSS LOCKOUT REDUCED
    - LOSS_LOCKOUT_SEC=300 (was 3600 — blocked Σ=+0.717 signals at bottom)
    - MAX_CONSECUTIVE_LOSSES=5 (was 3)

  FIX 3: REGIME GATE FIXED
    - ATR_MIN_PCTILE=0.00 (was 0.05 — blocked entries when ATR contracted)

  FIX 4: SPREAD GATE WIDENED
    - MAX_SPREAD_ATR_RATIO=0.30 (was 0.08 — blocked all entries in low-vol)

  NOTE: SL stays at structural level (5m swing). Capping SL at 4×ATR was
  tested but REJECTED — it moved SL from $71,216 to $71,363, which was hit
  during a temporary dip that the structural SL survived. Structure > ATR.

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
MAX_CONSECUTIVE_LOSSES  = 5   # v4.6: was 3 — too aggressive, locked out best signals
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
COMMISSION_RATE_MAKER = 0.00020  # v4.6: explicit CoinSwitch maker rate (was 40% of taker = 0.00022)

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
QUANT_ATR_MIN_PCTILE        = 0.00  # v4.6: was 0.05 — blocked entries when ATR contracted
QUANT_ATR_MAX_PCTILE        = 0.97

# 10g. Timing — PATIENT
QUANT_MAX_HOLD_SEC          = 2400
QUANT_COOLDOWN_SEC          = 180
QUANT_LOSS_LOCKOUT_SEC      = 300   # v4.6: was 3600 — blocked Σ=+0.717 signals at the bottom
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
#      v4.6 FIX: was 0.08 — when ATR contracts to $20, spread/ATR=0.18 and
#      blocks ALL entries. ATR-normalized spread is a bad gate in low-vol.
QUANT_MAX_SPREAD_ATR_RATIO     = 0.30  # v4.6: was 0.08 — far too tight for low-vol

# 10n. Mode-aware R:R — v4.6 COMPLETE REWRITE
#      v4.4 used min_tp_dist = SL × REVERSION_MIN_RR. When ATR=$22 but SL was
#      $220 behind swing structure (10×ATR), TP was forced to $330 (15×ATR).
#      Mean reversion NEVER moves 15×ATR. Every trade timed out at max_hold.
#
#      v4.6 FIX: TP is placed at the NATURAL structural target (VWAP, HVN, OB).
#      min_tp_dist is fee clearance + small ATR floor — NOT SL-linked.
#      If natural target gives R:R < REJECT threshold, trade is SKIPPED.
#      Don't inflate TP to unreachable levels — reject the setup.
QUANT_REVERSION_MIN_RR         = 1.5   # LEGACY — no longer used for TP floor
QUANT_REVERSION_MAX_RR         = 3.0   # Still caps max TP distance
QUANT_TREND_MIN_RR             = 3.0   # Trend: let winners run
QUANT_TREND_MAX_RR             = 5.0   # Cap trend TP distance

# 10n-v46. Natural TP placement params
QUANT_TP_MIN_ATR_MULT          = 0.5   # TP floor: at least 0.5×ATR from entry
QUANT_TP_MAX_ATR_MULT          = 6.0   # TP ceiling: never more than 6×ATR (reversion)
QUANT_REVERSION_REJECT_RR      = 0.20  # Reject trade if R:R < 0.20 (fee-negative)

# 10n-v46. SL ATR cap — prevents SL from being 10×ATR behind entry
#          When ATR contracts, MIN_SL_PCT ($214) stays huge relative to ATR.
#          SL must be max SL_MAX_ATR_MULT × ATR from entry for reversion trades.
QUANT_SL_MAX_ATR_MULT          = 4.0   # Reversion SL: max 4×ATR from entry

# ═══════════════════════════════════════════════════════════════════
# 10p. BREAKOUT DETECTOR — v4.6
# ═══════════════════════════════════════════════════════════════════
# Fast directional momentum detection that fires BEFORE ADX catches up.
# When triggered, blocks reversion entries and enables momentum entries.
QUANT_BO_CONSEC_CANDLES        = 3     # Min consecutive directional 5m candles
QUANT_BO_ATR_EXPANSION         = 1.5   # ATR must be 1.5× baseline for vol expansion
QUANT_BO_VOL_SURGE             = 1.8   # Volume must be 1.8× recent average
QUANT_BO_DISP_ATR              = 2.0   # Price must move 2×ATR from N bars ago
QUANT_BO_DISP_LOOKBACK         = 6     # Lookback for displacement (6 × 5m = 30min)
QUANT_BO_MIN_SCORE             = 3     # Min score to trigger breakout (out of ~6)
QUANT_BO_BLOCK_SEC             = 600   # Block reversion for 10 min after breakout

# 10o. Smart max-hold exit — v4.6 COMPLETE REWRITE
#      v4.4: tighten SL for profitable trades. Still force-exited underwater trades.
#      v4.6: thesis-aware extension. When timer fires:
#        - If in profit → tighten SL, let it ride (don't force exit)
#        - If underwater but thesis valid → EXTEND hold (max N times)
#        - If thesis broken → exit
#      This prevents killing winning trades (chart: $71,452 → $71,900 missed)
QUANT_SMART_MAX_HOLD           = True  # Enable thesis-aware max-hold
QUANT_MAX_HOLD_PROFIT_SL_ATR   = 0.5   # Tighten SL to 0.5×ATR from price when in profit
QUANT_MAX_HOLD_EXTENSIONS      = 5     # Max times to extend when thesis valid (total: 40+5×20=140min)
QUANT_HOLD_EXTENSION_SEC       = 1200  # Each extension adds 20 min
QUANT_THESIS_MAX_DRAWDOWN_PCT  = 0.70  # Exit if underwater > 70% of SL distance

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

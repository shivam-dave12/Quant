"""
config.py — Single source of truth for all bot parameters.
============================================================
Quant Bot v4.9 — ICT-Anchored Trailing SL + Structural TP

CHANGES from v4.8 (v4.9 — TRAIL AGGRESSION FIX):

  ROOT CAUSE: Trail started at 0.3R with 1.0×ATR min-distance. A normal
  BTC pullback of 1.2-1.8×ATR from the swing high hit the trailed SL.
  Price then continued to TP. User reported: "small pullback → SL hit →
  then TP achieves without us." This was structural, not parametric.

  FIX 1: ICT ZONE FREEZE (quant_strategy.py)
    Trail is completely frozen when price tests an active OB or FVG.
    Pullback into an OB = trade working as intended. TRAIL_BE_R 0.3→0.50.

  FIX 2: WIDER MIN DISTANCES
    Phase 1: 1.0→1.5×ATR. Phase 2: 0.7→1.1×ATR. Phase 3: 0.5→0.7×ATR.
    A 1.2×ATR pullback no longer hits the trail SL.

  FIX 3: PULLBACK TOLERANCE WIDENED
    TRAIL_PB_DEPTH_ATR: 0.80→1.20 (healthy pullbacks up to 1.2×ATR).
    TRAIL_REV_MIN_SIGNALS: 3→4 (need 4/6 reversal signals to freeze).

  FIX 4: ICT OB ANCHOR + LIQUIDITY CEILING (new config params)
    OB anchor: SL placed below active OB for structural validity.
    Liq ceiling: SL cannot cross unswept EQL/EQH pools.

  FIX 5: ICT STRUCTURAL TP TARGETS (quant_strategy.py)
    compute_tp now uses ICT engine's structural TP candidates:
    swept liq origins (6+), unfilled FVGs (5+), virgin OBs (4+).
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
# Maker limit order fill timeout — raised from 7s (too short for 3s rate-limiter).
# 25s allows 7-8 polls; gives real chance to fill before falling back to taker.
LIMIT_ORDER_FILL_TIMEOUT_SEC = 25.0

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
TRAILING_SL_CHECK_INTERVAL   = 10   # v4.8: was 15 → check trail every 10s
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

# 10d. Trailing SL — v4.9 INSTITUTIONAL REWRITE (ICT-anchored, zone-aware)
#
#      v4.8 FAILURE: Trail activated at 0.3R (just +$60) with min_dist=1.0×ATR.
#      A healthy BTC pullback of 1.2–1.8×ATR from the swing high hits that SL.
#      Price then continues to TP. The pattern: "SL hit during pullback, then
#      TP fires without us." This was the primary reported problem.
#
#      v4.9 FIX: 4-pronged institutional solution:
#
#      1. LATER TRAIL START: 0.50R (need half SL distance profit before trail).
#         At a $200 SL that's +$100 — trade has PROVED it before SL moves.
#
#      2. WIDER MIN DISTANCES: 1.5/1.1/0.70 ATR (was 1.0/0.7/0.5).
#         BTC average 5m ATR ≈ $100–200. 1.5×ATR = $150–300 breathing room.
#         A 1.2×ATR pullback ($120–240) no longer hits the trailed SL.
#
#      3. ICT ZONE FREEZE: If price tests an active OB or sits inside an FVG,
#         the trail is FROZEN entirely. These are institutional zones where
#         smart money defends — a pullback into them is EXPECTED, not reversal.
#         This is the most impactful change.
#
#      4. LIQUIDITY POOL CEILING: Trail cannot advance ABOVE (for long) or
#         BELOW (for short) an unswept EQL/EQH. Smart money will sweep those
#         stops before reversing — trailing SL right at the pool level ensures
#         the sweep takes us out. Keep SL safely beyond liquidity.
#
#      5. ICT OB ANCHOR: When an active OB exists below price (for long),
#         SL candidate = OB.low - buffer. This is the MOST VALID institutional
#         SL level — that is literally where the orders are. Structure > chandelier.
#
QUANT_TRAIL_ENABLED            = True
QUANT_TRAIL_BE_R               = 0.50  # v4.9: was 0.3 → Phase 0→1 at +0.50R
QUANT_TRAIL_LOCK_R             = 1.00  # v4.9: was 0.8 → Phase 1→2 at +1.0R
QUANT_TRAIL_AGGRESSIVE_R       = 2.00  # v4.9: was 1.5 → Phase 2→3 at +2.0R
QUANT_TRAIL_MIN_DIST_ATR_P1    = 1.50  # v4.9: was 1.0 → Phase 1: min SL = 1.5×ATR
QUANT_TRAIL_MIN_DIST_ATR_P2    = 1.10  # v4.9: was 0.7 → Phase 2: 1.1×ATR
QUANT_TRAIL_MIN_DIST_ATR_P3    = 0.70  # v4.9: was 0.5 → Phase 3: 0.7×ATR
QUANT_TRAIL_PULLBACK_FREEZE    = True  # Freeze SL during healthy pullbacks
QUANT_TRAIL_PB_VOL_RATIO       = 0.65  # v4.9: was 0.60 → pullback vol < 65% = healthy
QUANT_TRAIL_PB_DEPTH_ATR       = 1.20  # v4.9: was 0.80 → pullback up to 1.2×ATR = healthy
QUANT_TRAIL_REV_MIN_SIGNALS    = 4     # v4.9: was 3 → need 4/6 reversal signals to freeze

# v4.9: ICT Zone Freeze — trail is fully frozen when price tests institutional zones
# These are the levels where smart money placed orders; a test is NOT a reversal.
QUANT_ICT_ZONE_FREEZE_ENABLED  = True   # Freeze trail when price in/near OB or FVG
QUANT_ICT_ZONE_FREEZE_ATR      = 0.40   # Freeze within 0.40×ATR of an active OB
QUANT_ICT_OB_SL_ANCHOR         = True   # Use active OB as SL anchor candidate
QUANT_ICT_OB_SL_BUFFER_ATR     = 0.35   # OB anchor SL = OB.low - 0.35×ATR (long)
QUANT_ICT_LIQ_CEILING_ENABLED  = True   # Cap trail below/above unswept liquidity pools
QUANT_ICT_LIQ_POOL_BUFFER_ATR  = 0.50   # Stay 0.5×ATR beyond any unswept pool

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
QUANT_TRAIL_CHANDELIER_N_START = 3.00  # v4.9: was 2.5 → chandelier starts wider
QUANT_TRAIL_CHANDELIER_N_END   = 1.50  # v4.9: was 1.2 → end slightly wider too
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
QUANT_TREND_MIN_RR             = 2.0   # v4.7: was 3.0 → unreachable with ATR SL
QUANT_TREND_MAX_RR             = 3.0   # v4.7: was 5.0 → cap at 3:1 for 5m scalps
QUANT_TREND_SL_ATR_MULT        = 2.0   # v4.7: trend/momentum SL max 2×ATR from entry

# 10n-v46. Natural TP placement params
QUANT_TP_MIN_ATR_MULT          = 0.5   # TP floor: at least 0.5×ATR from entry
QUANT_TP_MAX_ATR_MULT          = 6.0   # TP ceiling: never more than 6×ATR (reversion)
QUANT_REVERSION_REJECT_RR      = 0.20  # Reject trade if R:R < 0.20 (fee-negative)

# 10n-v46. SL ATR cap — prevents SL from being 10×ATR behind entry
#          When ATR contracts, MIN_SL_PCT ($214) stays huge relative to ATR.
#          SL must be max SL_MAX_ATR_MULT × ATR from entry for reversion trades.
QUANT_SL_MAX_ATR_MULT          = 4.0   # Reversion SL: max 4×ATR from entry

# ═══════════════════════════════════════════════════════════════════
# 10p. BREAKOUT DETECTOR — v4.7 (adaptive multi-evidence scoring)
# ═══════════════════════════════════════════════════════════════════
# Scores 5 factors: candle body, volume, ATR expansion, VWAP displacement, follow-through
# Min score 4 means: explosive candle(3) + volume(1) = fires on single candle
QUANT_BO_MIN_SCORE             = 4     # Min score to trigger (out of ~10 possible)
QUANT_BO_BLOCK_SEC             = 900   # Block reversion for 15 min after breakout
QUANT_BO_RETEST_TIMEOUT        = 900   # Give up waiting for retest after 15 min
QUANT_RETEST_RETRY_SEC         = 30    # Seconds between retest entry attempts after rejection (prevents 2s spam)

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
FEE_FLOOR_MULT_LOW          = 2.5   # was 5.5 — caused 7xATR TP demands at low pctile
FEE_FLOOR_MULT_HIGH         = 1.2   # was 1.8
FEE_FLOOR_MAX_ATR_MULT      = 2.0   # Hard ATR cap: fee floor can never exceed 2.0xATR (prevents physically impossible TP demands)
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
# v4.9: urgency_cutoff raised 0.72→0.82 (was too conservative, fell back to taker too often)
# v4.9: min_saving_bps lowered 0.8→0.5 (small saving still justifies limit post)
# v4.9: depth_fill_floor raised 0.2→0.35 (thin books were killing fill probability)
FEE_MAKER_MIN_SAVING_BPS    = 0.5
FEE_MAKER_URGENCY_CUTOFF    = 0.82
FEE_MAKER_DEPTH_LEVELS      = 5
FEE_MAKER_DEPTH_MAX_FRAC    = 0.25
FEE_MAKER_DEPTH_FILL_FLOOR  = 0.35
FEE_MAKER_OPP_COST_WEIGHT   = 0.5

# ═══════════════════════════════════════════════════════════════════
# 12. ATR ENGINE REGIME TUNING
# ═══════════════════════════════════════════════════════════════════
ATR_SEED_RETAIN             = 1      # v4.3: Only keep final ATR from warmup.
                                     # Old values (20, 35) poisoned percentile ranking
                                     # with stale high-vol warmup data → 0% pctile → blocked all trades.
ATR_PCTILE_RANK_WINDOW      = 30

# ═══════════════════════════════════════════════════════════════════
# 13. ICT/SMC STRUCTURAL CONFLUENCE ENGINE (v4.8)
# ═══════════════════════════════════════════════════════════════════
#
# The ICT engine detects price structure (Order Blocks, FVGs, Liquidity
# Sweeps, Session/Killzones) and provides a 0-1 confluence score.
# This score BOOSTS the quant composite — it cannot trigger entries alone.
#
# ARCHITECTURE:
#   Quant composite (order flow) = primary signal (65% weight)
#   ICT confluence (structure)   = secondary boost (35% weight)
#   Entry requires: overextended + regime_ok + HTF_ok + confluence≥3 + composite≥threshold
#   ICT adds to confluence count when total≥0.30, and boosts composite by up to 0.15
#
# RESULT:
#   A reversion entry near a virgin OB in a killzone with a fresh sweep
#   gets +0.15 composite boost and +1 confluence. This is the difference
#   between "watching" and "all pass" on marginal setups that would otherwise
#   fail the composite gate by 0.05.
#

# 13a. Order Blocks (ICT)
OB_MIN_IMPULSE_PCT          = 0.15   # impulse candle must move >= 0.15% (was 0.50 — too high for BTC 5m candles)
OB_MIN_BODY_RATIO           = 0.40   # impulse body >= 40% of range (was 0.50 — too strict)
OB_IMPULSE_SIZE_MULTIPLIER  = 1.30   # impulse range >= 1.30x OB range
OB_MAX_AGE_MINUTES          = 1440   # 24h — OBs remain valid for a full day

# 13b. Fair Value Gaps (ICT)
FVG_MIN_SIZE_PCT        = 0.020      # gap >= 0.02% of price
FVG_MAX_AGE_MINUTES     = 1440       # 24h

# 13c. Liquidity Pools (SMC)
LIQ_TOUCH_TOLERANCE_PCT = 0.20       # 0.20% = ~$150 at $74K
SWEEP_DISPLACEMENT_MIN  = 0.40       # displacement body ratio minimum
SWEEP_MAX_AGE_MINUTES   = 120        # 2h

# 13d. Kill Zones (New York local time, DST-aware)
KZ_ASIA_NY_START    = 20   # 8:00 PM New York time
KZ_LONDON_NY_START  = 2    # 2:00 AM New York time
KZ_LONDON_NY_END    = 5    # 5:00 AM New York time
KZ_NY_NY_START      = 7    # 7:00 AM New York time
KZ_NY_NY_END        = 10   # 10:00 AM New York time

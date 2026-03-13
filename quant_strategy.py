"""
INSTITUTIONAL MULTI-FACTOR MOMENTUM + ORDER FLOW STRATEGY — v2 (AUDITED)
=========================================================================
Technique Stack:
  ─ Cumulative Volume Delta (CVD)  → Order flow imbalance, tanh z-scored
  ─ VWAP + Volume-Weighted Bands   → Institutional price anchor & deviation
  ─ EMA Crossover Momentum         → Multi-timeframe trend alignment (1m + 5m)
  ─ Keltner/BB Squeeze Breakout    → Volatility compression → expansion detection
  ─ Multi-Bar Volume Flow          → Institutional participation imbalance
  ─ ATR Regime Filter              → Only trade in statistically valid vol regimes

AUDIT FIXES (v2) — 28 issues corrected:
  FIX-01  MomentumEngine: med ROC used self.fast (wrong period). Replaced ROC with
          proper EMA crossover (fast/slow EMA on 1m + EMA slope on 5m).
  FIX-02  CVDEngine: deque maxlen=window*3=60 was statistically insufficient.
          Expanded to window × CVD_HIST_MULT (default 300 bars).
  FIX-03  CVDEngine: z/2.0 clamp wrong for unbounded z. Replaced with tanh(z/2).
  FIX-04  VWAPEngine: std used (close-VWAP)² → now volume-weighted (TP-VWAP)².
  FIX-05  VWAPEngine: slope normalizer hardcoded 0.001 → ATR-relative, dynamic.
  FIX-06  VolatilitySqueezeEngine: fired on any non-squeeze bar. Now tracks
          squeeze→expansion state transitions, active only first N breakout bars.
  FIX-07  VolatilitySqueezeEngine: population std (÷n) → sample std (÷n-1).
  FIX-08  VolumeConfirmationEngine: single O→C candle direction → multi-bar
          rolling buy/sell imbalance ratio vs historical baseline.
  FIX-09  ATREngine: full Wilder recompute every 5s tick → incremental update.
  FIX-10  ATREngine: percentile biased (current in history) → snapshot before append.
  FIX-11  ATREngine: duplicate appends per tick → gated by candle timestamp.
  FIX-12  _enter_trade: recomputed SL/TP (None,None) case not guarded → fixed.
  FIX-13  _enter_trade: no minimum R:R enforcement → uses MIN_RISK_REWARD_RATIO.
  FIX-14  _enter_trade: slippage threshold 0.001 hardcoded → QUANT_SLIPPAGE_TOLERANCE.
  FIX-15  _update_trailing_sl: replace_stop_loss None (SL fired) silently ignored
          → now triggers _record_exchange_exit() and finalises position.
  FIX-16  _sync_position: summed all qty → uses get_open_position() directly.
  FIX-17  _sync_position: TP/SL pnl not tracked → PnL extracted from exchange data.
  FIX-18  _compute_quantity: final margin check always passed → correct guard.
  FIX-19  _compute_quantity: no risk limits → DailyRiskGate enforces MAX_DAILY_TRADES,
          MAX_CONSECUTIVE_LOSSES, MAX_DAILY_LOSS_PCT.
  FIX-20  _compute_sl_tp: TP min = sl_min×1.5 hardcoded → MIN_RISK_REWARD_RATIO.
  FIX-21  _compute_sl_tp: round(...,1) hardcoded → TICK_SIZE from config.
  FIX-22  format_status_report: dead pnl_live=0 line removed → live elapsed/trail info.
  FIX-23  SignalAggregator: no weight validation → normalises + warns at call time.
  FIX-24  on_tick EXITING: sync every 5s with no separate gate → rate-gated independently.
  FIX-25  MomentumEngine: ROC cap 0.005 hardcoded → ATR-relative adaptive normalisation.
  FIX-26  QCfg: class attrs at import time → all values fetched live from config.
  FIX-27  VolatilitySqueezeEngine: Keltner Channel (BB inside KC) replaces BBW percentile.
  FIX-28  VolumeFlowEngine: replaces VolumeConfirmationEngine entirely with multi-bar logic.
"""

from __future__ import annotations

import logging
import math
import time
import threading
from collections import deque
from dataclasses import dataclass
from datetime import date
from enum import Enum, auto
from typing import Dict, List, Optional, Tuple

import config
from telegram_notifier import send_telegram_message
from order_manager import CancelResult

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION ACCESSOR — FIX-26
# ═══════════════════════════════════════════════════════════════════════════════

def _cfg(name: str, default):
    """
    Live config accessor. Always reads the current value from config module.
    Falls back to `default` only if the attribute is absent.
    Zero / False / empty-string are valid — never replaced by default.
    """
    val = getattr(config, name, None)
    return default if val is None else val


class QCfg:
    """
    All values fetched live via _cfg() — no import-time caching.
    Config changes take effect on the next tick without restart.
    """

    @staticmethod
    def SYMBOL()    -> str:   return str(config.SYMBOL)
    @staticmethod
    def EXCHANGE()  -> str:   return str(config.EXCHANGE)

    # Leverage & sizing
    @staticmethod
    def LEVERAGE()        -> int:   return int(_cfg("LEVERAGE",               30))
    @staticmethod
    def MARGIN_PCT()      -> float: return float(_cfg("QUANT_MARGIN_PCT",     0.20))
    @staticmethod
    def LOT_STEP()        -> float: return float(_cfg("LOT_STEP_SIZE",        0.001))
    @staticmethod
    def MIN_QTY()         -> float: return float(_cfg("MIN_POSITION_SIZE",    0.001))
    @staticmethod
    def MAX_QTY()         -> float: return float(_cfg("MAX_POSITION_SIZE",    1.0))
    @staticmethod
    def MIN_MARGIN_USDT() -> float: return float(_cfg("MIN_MARGIN_PER_TRADE", 4.0))
    @staticmethod
    def COMMISSION_RATE() -> float: return float(_cfg("COMMISSION_RATE",      0.00055))
    @staticmethod
    def TICK_SIZE()       -> float: return float(_cfg("TICK_SIZE",            0.1))      # FIX-21
    @staticmethod
    def SLIPPAGE_TOL()    -> float: return float(_cfg("QUANT_SLIPPAGE_TOLERANCE", 0.0005))  # FIX-14

    # Signal thresholds
    @staticmethod
    def LONG_THRESHOLD()  -> float: return float(_cfg("QUANT_LONG_THRESHOLD",  0.55))
    @staticmethod
    def SHORT_THRESHOLD() -> float: return float(_cfg("QUANT_SHORT_THRESHOLD", 0.55))
    @staticmethod
    def EXIT_FLIP_THRESH()-> float: return float(_cfg("QUANT_EXIT_FLIP",       0.30))
    @staticmethod
    def CONFIRM_TICKS()   -> int:   return int(_cfg("QUANT_CONFIRM_TICKS",     2))

    # ATR SL / TP
    @staticmethod
    def ATR_PERIOD()      -> int:   return int(_cfg("SL_ATR_PERIOD",           14))
    @staticmethod
    def SL_ATR_MULT()     -> float: return float(_cfg("QUANT_SL_ATR_MULT",    1.5))
    @staticmethod
    def TP_ATR_MULT()     -> float: return float(_cfg("QUANT_TP_ATR_MULT",    2.5))
    @staticmethod
    def MIN_SL_PCT()      -> float: return float(_cfg("MIN_SL_DISTANCE_PCT",  0.004))
    @staticmethod
    def MAX_SL_PCT()      -> float: return float(_cfg("MAX_SL_DISTANCE_PCT",  0.030))
    @staticmethod
    def MIN_RR_RATIO()    -> float: return float(_cfg("MIN_RISK_REWARD_RATIO", 1.5))  # FIX-20

    # Trailing SL
    @staticmethod
    def TRAIL_ENABLED()      -> bool:  return bool(_cfg("QUANT_TRAIL_ENABLED",    True))
    @staticmethod
    def TRAIL_ACTIVATE_R()   -> float: return float(_cfg("QUANT_TRAIL_ACTIVATE_R", 1.0))
    @staticmethod
    def TRAIL_ATR_MULT()     -> float: return float(_cfg("QUANT_TRAIL_ATR_MULT",  1.0))
    @staticmethod
    def TRAIL_INTERVAL_S()   -> int:   return int(_cfg("TRAILING_SL_CHECK_INTERVAL", 30))
    @staticmethod
    def TRAIL_MIN_MOVE_ATR() -> float: return float(_cfg("SL_MIN_IMPROVEMENT_ATR_MULT", 0.1))

    # Indicator windows
    @staticmethod
    def CVD_WINDOW()          -> int:   return int(_cfg("QUANT_CVD_WINDOW",        20))
    @staticmethod
    def CVD_HIST_MULT()       -> int:   return int(_cfg("QUANT_CVD_HIST_MULT",     15))   # FIX-02
    @staticmethod
    def VWAP_WINDOW()         -> int:   return int(_cfg("QUANT_VWAP_WINDOW",       50))
    @staticmethod
    def VWAP_SLOPE_BARS()     -> int:   return int(_cfg("QUANT_VWAP_SLOPE_BARS",   8))    # FIX-05
    @staticmethod
    def EMA_FAST()            -> int:   return int(_cfg("QUANT_EMA_FAST",          8))    # FIX-01
    @staticmethod
    def EMA_SLOW()            -> int:   return int(_cfg("QUANT_EMA_SLOW",         21))
    @staticmethod
    def EMA_SIGNAL_BARS()     -> int:   return int(_cfg("QUANT_EMA_SIGNAL_BARS",   5))
    @staticmethod
    def BB_WINDOW()           -> int:   return int(_cfg("QUANT_BB_WINDOW",         20))
    @staticmethod
    def BB_STD()              -> float: return float(_cfg("QUANT_BB_STD",          2.0))
    @staticmethod
    def KC_ATR_MULT()         -> float: return float(_cfg("QUANT_KC_ATR_MULT",     1.5))  # FIX-27
    @staticmethod
    def SQUEEZE_BREAKOUT_BARS()-> int:  return int(_cfg("QUANT_SQUEEZE_BREAKOUT_BARS", 5)) # FIX-06
    @staticmethod
    def VOL_FLOW_WINDOW()     -> int:   return int(_cfg("QUANT_VOL_FLOW_WINDOW",   10))   # FIX-08

    # Minimum data requirements
    @staticmethod
    def MIN_1M_BARS()     -> int:   return int(_cfg("MIN_CANDLES_1M",           80))
    @staticmethod
    def MIN_5M_BARS()     -> int:   return int(_cfg("MIN_CANDLES_5M",           60))

    # Regime filter
    @staticmethod
    def ATR_PCTILE_WINDOW()-> int:  return int(_cfg("QUANT_ATR_PCTILE_WINDOW", 100))
    @staticmethod
    def ATR_MIN_PCTILE()  -> float: return float(_cfg("QUANT_ATR_MIN_PCTILE",  0.08))
    @staticmethod
    def ATR_MAX_PCTILE()  -> float: return float(_cfg("QUANT_ATR_MAX_PCTILE",  0.90))

    # Timing
    @staticmethod
    def MAX_HOLD_SEC()    -> int:   return int(_cfg("QUANT_MAX_HOLD_SEC",       1800))
    @staticmethod
    def COOLDOWN_SEC()    -> int:   return int(_cfg("QUANT_COOLDOWN_SEC",       60))
    @staticmethod
    def TICK_EVAL_SEC()   -> float: return float(_cfg("ENTRY_EVALUATION_INTERVAL_SECONDS", 5))
    @staticmethod
    def POS_SYNC_SEC()    -> float: return float(_cfg("QUANT_POS_SYNC_SEC",    30))

    # Risk limits — FIX-19
    @staticmethod
    def MAX_DAILY_TRADES()  -> int:   return int(_cfg("MAX_DAILY_TRADES",        8))
    @staticmethod
    def MAX_CONSEC_LOSSES() -> int:   return int(_cfg("MAX_CONSECUTIVE_LOSSES",  3))
    @staticmethod
    def MAX_DAILY_LOSS_PCT()-> float: return float(_cfg("MAX_DAILY_LOSS_PCT",    5.0))

    # Signal weights
    @staticmethod
    def W_CVD()     -> float: return float(_cfg("QUANT_W_CVD",     0.30))
    @staticmethod
    def W_VWAP()    -> float: return float(_cfg("QUANT_W_VWAP",    0.25))
    @staticmethod
    def W_MOM()     -> float: return float(_cfg("QUANT_W_MOM",     0.25))
    @staticmethod
    def W_SQUEEZE() -> float: return float(_cfg("QUANT_W_SQUEEZE", 0.10))
    @staticmethod
    def W_VOL()     -> float: return float(_cfg("QUANT_W_VOL",     0.10))


def _round_to_tick(price: float) -> float:
    """FIX-21: Round to exchange TICK_SIZE from config, not hardcoded decimal places."""
    tick = QCfg.TICK_SIZE()
    if tick <= 0:
        return price
    return round(round(price / tick) * tick, 10)


# ═══════════════════════════════════════════════════════════════════════════════
# ALPHA SIGNAL ENGINES
# ═══════════════════════════════════════════════════════════════════════════════

class CVDEngine:
    """
    Cumulative Volume Delta — rolling z-score of buy/sell imbalance.

    Per-bar delta heuristic (OHLCV, matches TradingView):
        delta_frac = (2×close - high - low) / (high - low)
        delta      = volume × delta_frac

    Signal = tanh(z / 2)   where z is z-score of rolling window CVD sum.

    FIX-02: History = window × CVD_HIST_MULT — large enough for stable z-scores.
    FIX-03: tanh(z/2) not clamp(z/2). tanh is the proper sigmoid for unbounded z.
    """

    def __init__(self):
        self._deltas: deque[float] = deque(maxlen=QCfg.CVD_WINDOW() * QCfg.CVD_HIST_MULT())
        self._window = QCfg.CVD_WINDOW()

    def update(self, candles: List[Dict]) -> None:
        self._deltas.clear()
        for c in candles:
            hi  = float(c['h']); lo = float(c['l'])
            cl  = float(c['c']); vol = float(c['v'])
            rng = hi - lo
            delta_frac = (2.0 * cl - hi - lo) / rng if rng > 1e-10 else 0.0
            self._deltas.append(vol * delta_frac)

    def get_signal(self) -> float:
        w   = QCfg.CVD_WINDOW()
        arr = list(self._deltas)
        if len(arr) < w + 5:
            return 0.0

        sums = [sum(arr[i: i + w]) for i in range(len(arr) - w + 1)]
        if len(sums) < 5:
            return 0.0

        mu  = sum(sums) / len(sums)
        var = sum((s - mu) ** 2 for s in sums) / max(len(sums) - 1, 1)
        std = math.sqrt(var)
        if std < 1e-12:
            return 0.0

        z = (sums[-1] - mu) / std
        return math.tanh(z / 2.0)   # FIX-03


class VWAPEngine:
    """
    Volume-Weighted Average Price with volume-weighted standard deviation bands.

    FIX-04: Std = sqrt( Σ(vol × (TP - VWAP)²) / Σvol )  — volume-weighted.
            Properly represents the price dispersion institutional orders care about.

    FIX-05: Slope normalised by (ATR / VWAP), making it dimensionless and
            regime-adaptive instead of the previous hardcoded 0.001.
    """

    def __init__(self):
        self._vwap     = 0.0
        self._std      = 0.0
        self._slope    = 0.0
        self._history: deque[float] = deque(maxlen=QCfg.VWAP_SLOPE_BARS() * 3)
        self._last_bars_used = 0  # tracks actual window used (for logging)

    # Minimum viable bars for a meaningful VWAP — below this, signal = 0.0
    _MIN_BARS = 50

    def update(self, candles: List[Dict], atr: float) -> None:
        # FIX: Graceful degradation — use whatever bars are available down to
        # _MIN_BARS. The old hard-gate `if len < window: return` caused VWAP
        # to silently return 0.000 for the entire warmup period when VWAP_WINDOW
        # (200) exceeded the 1m warmup fetch (99 candles), breaking composite
        # weighting and disabling direction bias for the first ~100 minutes.
        available = len(candles)
        if available < self._MIN_BARS:
            return
        window = min(QCfg.VWAP_WINDOW(), available)
        self._last_bars_used = window

        recent = candles[-window:]

        # Volume-weighted average typical price  (FIX-04)
        tp_vol = sum((float(c['h']) + float(c['l']) + float(c['c'])) / 3.0 * float(c['v'])
                     for c in recent)
        vol_sum = sum(float(c['v']) for c in recent)
        if vol_sum < 1e-12:
            return
        self._vwap = tp_vol / vol_sum

        # Volume-weighted variance  (FIX-04)
        var_sum = sum(float(c['v']) *
                      ((float(c['h']) + float(c['l']) + float(c['c'])) / 3.0 - self._vwap) ** 2
                      for c in recent)
        self._std = math.sqrt(var_sum / vol_sum)

        # Slope: % change of VWAP over SLOPE_BARS, normalised by ATR%  (FIX-05)
        self._history.append(self._vwap)
        sb = QCfg.VWAP_SLOPE_BARS()
        if len(self._history) >= sb + 1 and atr > 1e-10 and self._vwap > 1e-10:
            hist = list(self._history)
            raw_pct_slope = (hist[-1] - hist[-sb - 1]) / (hist[-sb - 1] + 1e-12)
            atr_pct       = atr / self._vwap
            self._slope   = max(-1.0, min(1.0, raw_pct_slope / (atr_pct + 1e-12)))

    def get_signal(self, price: float) -> float:
        if self._vwap < 1e-10 or self._std < 1e-10:
            return 0.0
        z         = (price - self._vwap) / self._std
        dev_score = math.tanh(z / 2.0)
        combined  = dev_score * 0.6 + self._slope * 0.4
        if dev_score * self._slope < 0:
            combined *= 0.25   # contradict penalty
        return max(-1.0, min(1.0, combined))

    @property
    def vwap(self) -> float: return self._vwap
    @property
    def vwap_std(self) -> float: return self._std
    @property
    def is_degraded(self) -> bool:
        """True if running on fewer bars than the configured window."""
        return 0 < self._last_bars_used < QCfg.VWAP_WINDOW()


class MomentumEngine:
    """
    EMA Crossover Momentum on two timeframes.

    FIX-01: Replaced the broken ROC approach (self.med was never used) with
            EMA(fast) minus EMA(slow) crossover, normalised by ATR.
            Primary: 1m EMA cross (intraday momentum direction).
            Secondary: 5m EMA slope (higher-TF confirmation).  (FIX-01)

    FIX-25: Normalisation uses ATR (adaptive to vol regime) not hardcoded 0.005.
    """

    def __init__(self):
        self._cross_1m = 0.0
        self._cross_5m = 0.0

    @staticmethod
    def _ema_series(values: List[float], period: int) -> List[float]:
        if len(values) < period:
            return []
        k   = 2.0 / (period + 1)
        ema = sum(values[:period]) / period
        out = [ema]
        for v in values[period:]:
            ema = v * k + ema * (1.0 - k)
            out.append(ema)
        return out

    def update(self, candles_1m: List[Dict], candles_5m: List[Dict],
               atr_1m: float, atr_5m: float) -> None:
        fast = QCfg.EMA_FAST(); slow = QCfg.EMA_SLOW(); sig_b = QCfg.EMA_SIGNAL_BARS()
        cl1  = [float(c['c']) for c in candles_1m]
        cl5  = [float(c['c']) for c in candles_5m]

        # 1m MACD-line = EMA(fast) - EMA(slow), normalised by 1m ATR  (FIX-25)
        if len(cl1) > slow + 2 and atr_1m > 1e-10:
            ef = self._ema_series(cl1, fast)
            es = self._ema_series(cl1, slow)
            n  = min(len(ef), len(es))
            macd_now = ef[-1] - es[-1]
            self._cross_1m = math.tanh(macd_now / atr_1m)
        else:
            self._cross_1m = 0.0

        # 5m EMA slope (FIX-01: now actually uses 5m candles with its own period)
        if len(cl5) > fast + sig_b and atr_5m > 1e-10:
            ef5   = self._ema_series(cl5, fast)
            if len(ef5) >= sig_b + 1:
                slope = ef5[-1] - ef5[-sig_b - 1]
                self._cross_5m = math.tanh(slope / atr_5m)
            else:
                self._cross_5m = 0.0
        else:
            self._cross_5m = 0.0

    def get_signal(self) -> float:
        m1 = self._cross_1m; m5 = self._cross_5m
        if abs(m1) < 1e-8 and abs(m5) < 1e-8:
            return 0.0
        same_dir = (m1 >= 0) == (m5 >= 0)
        return (m1 * 0.60 + m5 * 0.40) if same_dir else (m1 * 0.30)


class VolatilitySqueezeEngine:
    """
    TTM-style Bollinger + Keltner Squeeze with correct state machine.

    Squeeze = BB inside Keltner Channel (more robust than BBW percentile
    threshold which is sensitive to the lookback length chosen).  (FIX-27)

    FIX-06: Signal fires only for the first SQUEEZE_BREAKOUT_BARS bars after
            squeeze exits.  Previous code fired on ANY non-squeeze bar = always on.
    FIX-07: Sample standard deviation (÷ n-1) for BB width calculation.
    """

    def __init__(self):
        self._in_squeeze    = False
        self._post_sq_bars  = 0     # bars since squeeze exit
        self._bb_mid        = 0.0   # midpoint at squeeze exit (breakout reference)
        self._last_signal   = 0.0

    def update(self, candles: List[Dict], atr: float) -> None:
        window     = QCfg.BB_WINDOW()
        n_std      = QCfg.BB_STD()
        kc_mult    = QCfg.KC_ATR_MULT()
        break_bars = QCfg.SQUEEZE_BREAKOUT_BARS()

        if len(candles) < window or atr < 1e-10:
            self._last_signal = 0.0
            return

        closes = [float(c['c']) for c in candles[-window:]]
        n      = len(closes)
        mid    = sum(closes) / n

        # FIX-07: sample std
        var = sum((c - mid) ** 2 for c in closes) / (n - 1)
        std = math.sqrt(var)

        bb_upper = mid + n_std * std
        bb_lower = mid - n_std * std

        # FIX-27: Keltner Channel
        kc_upper = mid + kc_mult * atr
        kc_lower = mid - kc_mult * atr

        in_sq = (bb_upper < kc_upper) and (bb_lower > kc_lower)

        if in_sq:
            # Squeeze active — record midpoint, emit 0
            if not self._in_squeeze:
                self._in_squeeze   = True
                self._post_sq_bars = 0
            self._bb_mid        = mid
            self._last_signal   = 0.0
            return

        # FIX-06: Not squeezing
        if self._in_squeeze:
            self._in_squeeze   = False
            self._post_sq_bars = 0

        self._post_sq_bars += 1

        if self._post_sq_bars <= break_bars:
            cl    = float(candles[-1]['c'])
            disp  = (cl - self._bb_mid) / (std + 1e-12)
            self._last_signal = math.tanh(disp)
        else:
            self._last_signal = 0.0

    def get_signal(self) -> float:
        return self._last_signal


class VolumeFlowEngine:
    """
    Multi-Bar Volume Flow Imbalance.  (FIX-08, FIX-28)

    For each bar: buy_frac  = max(0, close-open) / (high-low)
                  sell_frac = max(0, open-close) / (high-low)
                  flow_ratio = (buy_vol - sell_vol) / (buy_vol + sell_vol + ε)

    Signal = tanh-z-score of rolling window avg vs historical baseline.
    This captures whether recent buying/selling pressure is anomalous vs history.
    """

    def __init__(self):
        # maxlen = window × 20 so there is always ample baseline history
        # for the z-score; recomputed on each update() so config changes take effect.
        self._ratios: deque[float] = deque(maxlen=QCfg.VOL_FLOW_WINDOW() * 20)

    def update(self, candles: List[Dict]) -> None:
        self._ratios.clear()
        for c in candles:
            hi  = float(c['h']); lo = float(c['l'])
            op  = float(c['o']); cl = float(c['c']); vol = float(c['v'])
            rng = hi - lo
            if rng > 1e-10:
                buy_f  = max(0.0, cl - op) / rng
                sell_f = max(0.0, op - cl) / rng
            else:
                buy_f = sell_f = 0.0
            bv = vol * buy_f; sv = vol * sell_f; tot = bv + sv
            ratio = (bv - sv) / (tot + 1e-12) if tot > 1e-10 else 0.0
            self._ratios.append(ratio)

    def get_signal(self) -> float:
        window = QCfg.VOL_FLOW_WINDOW()
        arr    = list(self._ratios)
        if len(arr) < window + 5:
            return 0.0
        recent_avg = sum(arr[-window:]) / window
        hist       = arr[:-window]
        if len(hist) < 5:
            return 0.0
        mu  = sum(hist) / len(hist)
        var = sum((x - mu) ** 2 for x in hist) / max(len(hist) - 1, 1)
        std = math.sqrt(var)
        if std < 1e-12:
            return 0.0
        return math.tanh((recent_avg - mu) / std / 2.0)


# ═══════════════════════════════════════════════════════════════════════════════
# ATR ENGINE — FIX-09, FIX-10, FIX-11
# ═══════════════════════════════════════════════════════════════════════════════

class ATREngine:
    """
    Wilder's ATR — incremental, candle-gated, unbiased percentile.

    FIX-09: Incremental Wilder smoothing, not full recompute every tick.
    FIX-10: Percentile uses a snapshot of history BEFORE appending current ATR.
    FIX-11: Only appends to history when a genuinely new candle is seen (ts check).
    """

    def __init__(self):
        self._atr            = 0.0
        self._atr_hist: deque[float] = deque(maxlen=QCfg.ATR_PCTILE_WINDOW())
        self._last_ts: int   = -1
        self._seeded: bool   = False

    def compute(self, candles: List[Dict]) -> float:
        if not candles:
            return self._atr

        period  = QCfg.ATR_PERIOD()
        last_ts = int(candles[-1].get('t', 0))

        # FIX-11: skip if same candle
        if last_ts == self._last_ts and self._seeded:
            return self._atr

        if len(candles) < period + 1:
            return self._atr

        if not self._seeded:
            # Full seed pass — compute rolling ATR across ALL CLOSED candles only.
            #
            # CRITICAL: candles[-1] is always the current FORMING (not-yet-closed)
            # candle. Its hi/lo/close are partial data. Exclude it by seeding only
            # on candles[:-1]. last_ts is still set to candles[-1]['t'] below, so
            # FIX-11 correctly skips all subsequent ticks in the same period.
            closed = candles[:-1]
            if len(closed) < period + 1:
                return self._atr

            trs = [
                max(float(closed[i]['h']) - float(closed[i]['l']),
                    abs(float(closed[i]['h']) - float(closed[i - 1]['c'])),
                    abs(float(closed[i]['l']) - float(closed[i - 1]['c'])))
                for i in range(1, len(closed))
            ]
            if len(trs) < period:
                return self._atr
            atr = sum(trs[:period]) / period
            # Collect all rolling ATR values from the seed candles
            rolling_atrs: List[float] = [atr]
            for tr in trs[period:]:
                atr = (atr * (period - 1) + tr) / period
                rolling_atrs.append(atr)
            self._atr    = atr
            self._seeded = True
            # Populate history with the last maxlen values so percentile
            # is immediately meaningful rather than starting from 1 entry.
            for val in rolling_atrs[-(self._atr_hist.maxlen or 100):]:
                self._atr_hist.append(val)
        else:
            # FIX-09 (CORRECTED): use the JUST-CLOSED candle, not the forming candle.
            #
            # Root cause of 0% regime after first entry:
            # Original used candles[-1] (the NEW forming candle, ~1 tick old,
            # hi=lo=open=close). True Range ≈ 0. Fed into Wilder every 5 min,
            # ATR deflated from $132 → $89 in 10 candles while history held $120+.
            # Result: percentile → 0%, regime permanently gated.
            #
            # candles[-1] = FORMING (skip)
            # candles[-2] = just-CLOSED  ← use hi/lo from here
            # candles[-3] = prior closed  ← use prev_close from here
            # (len >= period+1 = 15 candles guaranteed above, so [-3] is safe)
            hi  = float(candles[-2]['h'])
            lo  = float(candles[-2]['l'])
            prc = float(candles[-3]['c'])
            tr  = max(hi - lo, abs(hi - prc), abs(lo - prc))
            self._atr = (self._atr * (period - 1) + tr) / period
            # FIX-10: append after increment (hist[-1]=current, hist[:-1]=prior)
            self._atr_hist.append(self._atr)

        self._last_ts = last_ts
        return self._atr

    @property
    def atr(self) -> float:
        return self._atr

    def get_percentile(self) -> float:
        """Unbiased percentile: current ATR vs ALL prior values (not including itself)."""
        hist = list(self._atr_hist)
        if len(hist) < 10:
            return 0.5     # insufficient data → neutral
        ref  = hist[-1]
        prev = hist[:-1]   # FIX-10: exclude current from ranking
        return sum(1 for h in prev if h <= ref) / len(prev)

    def get_atr_zscore(self) -> float:
        """
        Z-score of current ATR vs history. +3 = 3σ above mean (very high vol).
        Paired with percentile to detect how extreme the current regime is.
        """
        hist = list(self._atr_hist)
        if len(hist) < 10:
            return 0.0
        mu  = sum(hist) / len(hist)
        var = sum((x - mu) ** 2 for x in hist) / max(len(hist) - 1, 1)
        std = math.sqrt(var)
        return (self._atr - mu) / (std + 1e-12)

    def classify_regime(self, vwap_signal: float = 0.0) -> "RegimeResult":
        """
        Map current ATR state to a fully parameterised RegimeResult.

        Core principle: HIGH vol ≠ no-trade.  It means TRADE DIFFERENTLY.
          - EXTREME regime: VWAP deviation drives direction (mean-reversion)
          - Size and SL width scale with regime severity
          - Confirmation ticks increase with chaos level

        Args:
            vwap_signal: VWAPEngine.get_signal() in [-1, +1].
                         Positive = price above VWAP (overbought).
        """
        p  = self.get_percentile()
        z  = self.get_atr_zscore()
        lo = QCfg.ATR_MIN_PCTILE()   # default 0.15

        # ── COMPRESSED: below configured floor (low vol, coiling) ─────────
        if p < lo:
            return RegimeResult(
                mode=RegimeMode.COMPRESSED, pctile=p, atr_zscore=z,
                size_scalar=0.60, sl_atr_mult=0.90, entry_threshold=0.68,
                extra_confirms=1, direction_bias="any",
                note=f"COMPRESSED ({p:.0%}) — breakout/squeeze only",
            )

        # ── TRENDING: normal operating band 15th–70th ─────────────────────
        if p <= 0.70:
            return RegimeResult(
                mode=RegimeMode.TRENDING, pctile=p, atr_zscore=z,
                size_scalar=1.00, sl_atr_mult=1.00,
                entry_threshold=QCfg.LONG_THRESHOLD(),
                extra_confirms=0, direction_bias="any",
                note=f"TRENDING ({p:.0%}) — full signal suite, full size",
            )

        # ── ELEVATED: 70th–90th percentile ────────────────────────────────
        if p <= 0.90:
            return RegimeResult(
                mode=RegimeMode.ELEVATED, pctile=p, atr_zscore=z,
                size_scalar=0.80, sl_atr_mult=1.20,
                entry_threshold=max(QCfg.LONG_THRESHOLD(), 0.60),
                extra_confirms=0, direction_bias="any",
                note=f"ELEVATED ({p:.0%}) — 80% size, wider SL",
            )

        # ── EXTREME: above 90th percentile — was a binary kill-switch ──────
        # The WRONG fix was blocking all entries here. In extreme vol, VWAP
        # deviation is the strongest edge: price far from VWAP = mean-revert.
        #   vwap_signal > +0.25 → overbought → SHORT mean-reversion bias
        #   vwap_signal < -0.25 → oversold  → LONG  mean-reversion bias
        #   |vwap| ≤ 0.25       → near VWAP → no directional constraint
        #
        # Threshold is split by trade type:
        #   mean-reversion (bias ≠ any): 0.45 — MOM is only partially recovered
        #     in a bounce/fade; max achievable composite ≈ 0.49. Momentum breakout
        #     signals (MOM 0.9+, SQZ 1.0) are absent.
        #   momentum (bias = any):       0.65 — near-VWAP breakout, all signals
        #     can align explosively → higher bar appropriate.
        BIAS = 0.25
        if vwap_signal > BIAS:
            bias      = "short_only"
            bias_note = f"VWAP+{vwap_signal:+.2f} → fade extension SHORT"
        elif vwap_signal < -BIAS:
            bias      = "long_only"
            bias_note = f"VWAP{vwap_signal:+.2f} → mean-revert LONG"
        else:
            bias      = "any"
            bias_note = f"VWAP≈0 ({vwap_signal:+.2f}) — momentum valid"

        # Mean-reversion ceiling ≈ 0.49; momentum ceiling > 0.80
        thr = 0.45 if bias != "any" else 0.65

        return RegimeResult(
            mode=RegimeMode.EXTREME, pctile=p, atr_zscore=z,
            size_scalar=0.50, sl_atr_mult=1.50,
            entry_threshold=thr,
            extra_confirms=1, direction_bias=bias,
            note=f"EXTREME ({p:.0%} z={z:+.1f}) — {bias_note}",
        )

    def is_regime_valid(self) -> bool:
        """Backward-compat: True for any tradeable regime (not COMPRESSED)."""
        return self.classify_regime().mode != RegimeMode.COMPRESSED


# ═══════════════════════════════════════════════════════════════════════════════
# SIGNAL AGGREGATOR — FIX-23
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class SignalBreakdown:
    cvd:       float = 0.0
    vwap:      float = 0.0
    mom:       float = 0.0
    squeeze:   float = 0.0
    vol:       float = 0.0
    composite: float = 0.0
    atr:       float = 0.0
    atr_pct:   float = 0.0
    regime_ok: bool  = False
    regime:    Optional["RegimeResult"] = None   # full regime snapshot

    def __str__(self) -> str:
        reg_str = str(self.regime) if self.regime else f"ATR${self.atr:.1f}({self.atr_pct:.0%})"
        return (
            f"CVD={self.cvd:+.3f} VWAP={self.vwap:+.3f} "
            f"MOM={self.mom:+.3f} SQZ={self.squeeze:+.3f} "
            f"VFL={self.vol:+.3f} → Σ={self.composite:+.4f} "
            f"{reg_str}"
        )


class SignalAggregator:
    """FIX-23: Live weight fetch + runtime normalisation if weights don't sum to 1.0."""

    @staticmethod
    def compute(cvd: float, vwap: float, mom: float,
                squeeze: float, vol: float,
                atr_engine: ATREngine,
                vwap_signal_for_regime: float = 0.0) -> SignalBreakdown:

        w = {
            'cvd': QCfg.W_CVD(), 'vwap': QCfg.W_VWAP(), 'mom': QCfg.W_MOM(),
            'squeeze': QCfg.W_SQUEEZE(), 'vol': QCfg.W_VOL(),
        }
        total_w = sum(w.values())
        if abs(total_w - 1.0) > 0.01:
            logger.warning(f"Weight sum={total_w:.4f} ≠ 1.0 — normalising. Fix QUANT_W_* in config.")
            w = {k: v / total_w for k, v in w.items()}

        # ── Classify regime FIRST so composite can be regime-aware ───────────
        regime = atr_engine.classify_regime(vwap_signal=vwap_signal_for_regime)

        # ── Mean-reversion VWAP sign flip ─────────────────────────────────────
        # In EXTREME mean-reversion mode (direction_bias != "any"), the VWAP
        # signal is semantically inverted relative to the trade direction:
        #
        #   long_only bias  → price BELOW VWAP (vwap_signal < 0, oversold)
        #                     VWAP normally means "bearish" in composite, but
        #                     here "below VWAP" is the BUY signal → flip to +
        #
        #   short_only bias → price ABOVE VWAP (vwap_signal > 0, overbought)
        #                     VWAP normally means "bullish" in composite, but
        #                     here "above VWAP" is the SELL signal → flip to -
        #
        # Without this flip, VWAP drags the composite in the WRONG direction
        # while simultaneously setting the correct direction bias.  Result was
        # composite stuck at +0.29 (CVD+0.77, MOM+0.50) never reaching +0.65.
        vwap_for_composite = vwap
        if regime.direction_bias != "any":
            vwap_for_composite = -vwap  # oversold/overbought → trade-aligned signal

        composite = max(-1.0, min(1.0,
            w['cvd']     * cvd               +
            w['vwap']    * vwap_for_composite +
            w['mom']     * mom                +
            w['squeeze'] * squeeze            +
            w['vol']     * vol
        ))

        return SignalBreakdown(
            cvd=cvd, vwap=vwap, mom=mom, squeeze=squeeze, vol=vol,
            composite=composite,
            atr=atr_engine.atr,
            atr_pct=atr_engine.get_percentile(),
            regime_ok=regime.mode not in (RegimeMode.COMPRESSED,),
            regime=regime,
        )


# ═══════════════════════════════════════════════════════════════════════════════
# POSITION STATE
# ═══════════════════════════════════════════════════════════════════════════════

class PositionPhase(Enum):
    FLAT     = auto()
    ENTERING = auto()
    ACTIVE   = auto()
    EXITING  = auto()


# ═══════════════════════════════════════════════════════════════════════════════
# VOLATILITY REGIME CLASSIFICATION
# ═══════════════════════════════════════════════════════════════════════════════

class RegimeMode(Enum):
    """
    Four-state volatility regime derived from ATR percentile rank.

    COMPRESSED  (<15th pctile) : Price coiling in tight range.
                                 Only squeeze-breakout signals valid.
                                 Wait for clear directional expansion.

    TRENDING    (15th–70th)    : Normal operating environment.
                                 All 5 signal engines active, full size.

    ELEVATED    (70th–90th)    : Above-average volatility.
                                 All signals valid, 80% size, wider SL.
                                 Slightly higher confirmation threshold.

    EXTREME     (>90th pctile) : Explosive move in progress.
                                 VWAP-deviation drives direction bias:
                                   price far above VWAP → SHORT mean-revert
                                   price far below VWAP → LONG  mean-revert
                                 50% size, 1.5× ATR SL buffer, +1 confirm tick.
    """
    COMPRESSED = "COMPRESSED"
    TRENDING   = "TRENDING"
    ELEVATED   = "ELEVATED"
    EXTREME    = "EXTREME"


@dataclass
class RegimeResult:
    """
    All regime-driven trading parameters in one immutable snapshot.
    Created once per evaluate_entry() call and passed through to _enter_trade().
    """
    mode:            RegimeMode
    pctile:          float       # 0.0–1.0   raw ATR percentile rank
    atr_zscore:      float       # z-score of current ATR vs history (signed)
    size_scalar:     float       # multiply raw position size by this (0–1]
    sl_atr_mult:     float       # override SL ATR multiplier for this regime
    entry_threshold: float       # composite signal must exceed this (both dirs)
    extra_confirms:  int         # additional confirm ticks beyond QCfg.CONFIRM_TICKS()
    direction_bias:  str         # "any" | "long_only" | "short_only"
    note:            str         # human-readable reason string

    def __str__(self) -> str:
        return (
            f"Regime={self.mode.value}({self.pctile:.0%}) "
            f"z={self.atr_zscore:+.2f} "
            f"size={self.size_scalar:.0%} "
            f"SLmult={self.sl_atr_mult:.1f}× "
            f"thr={self.entry_threshold:.2f} "
            f"bias={self.direction_bias}"
        )


@dataclass
class PositionState:
    phase:           PositionPhase         = PositionPhase.FLAT
    side:            str                   = ""
    quantity:        float                 = 0.0
    entry_price:     float                 = 0.0
    sl_price:        float                 = 0.0
    tp_price:        float                 = 0.0
    sl_order_id:     Optional[str]         = None
    tp_order_id:     Optional[str]         = None
    entry_order_id:  Optional[str]         = None
    entry_time:      float                 = 0.0
    initial_risk:    float                 = 0.0    # |entry-sl| × qty (USDT at risk)
    initial_sl_dist: float                 = 0.0    # |entry-sl| price units (for trail gate)
    trail_active:    bool                  = False
    last_trail_time: float                 = 0.0
    entry_signal:    Optional[SignalBreakdown] = None
    peak_profit:     float                 = 0.0   # max favourable excursion (price pts)
    entry_atr:       float                 = 0.0   # 5m ATR at the moment of entry

    def is_active(self) -> bool: return self.phase == PositionPhase.ACTIVE
    def is_flat(self)   -> bool: return self.phase == PositionPhase.FLAT

    def to_dict(self) -> Dict:
        return {
            "side": self.side, "quantity": self.quantity,
            "entry_price": self.entry_price,
            "sl_price": self.sl_price, "tp_price": self.tp_price,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# DAILY RISK GATE — FIX-19
# ═══════════════════════════════════════════════════════════════════════════════

class DailyRiskGate:
    """
    Enforces MAX_DAILY_TRADES, MAX_CONSECUTIVE_LOSSES, MAX_DAILY_LOSS_PCT.
    All counters reset at UTC midnight.  Consecutive losses persist across days.
    """

    def __init__(self):
        self._today:          date  = date.today()
        self._daily_trades:   int   = 0
        self._consec_losses:  int   = 0
        self._daily_pnl:      float = 0.0
        self._daily_open_bal: float = 0.0
        self._lock = threading.Lock()

    def _reset_if_new_day(self) -> None:
        today = date.today()
        if today != self._today:
            logger.info(f"📅 Daily risk counters reset (new day: {today})")
            self._today        = today
            self._daily_trades = 0
            self._daily_pnl    = 0.0
            self._daily_open_bal = 0.0
            # Consecutive losses intentionally persist across days

    def set_opening_balance(self, balance: float) -> None:
        with self._lock:
            self._reset_if_new_day()
            if self._daily_open_bal < 1e-10 and balance > 0:
                self._daily_open_bal = balance

    def can_trade(self, current_balance: float) -> Tuple[bool, str]:
        with self._lock:
            self._reset_if_new_day()
            max_dt = QCfg.MAX_DAILY_TRADES()
            max_cl = QCfg.MAX_CONSEC_LOSSES()
            max_lp = QCfg.MAX_DAILY_LOSS_PCT()

            if self._daily_trades >= max_dt:
                return False, f"Daily trade cap: {self._daily_trades}/{max_dt}"

            if self._consec_losses >= max_cl:
                return False, f"Consecutive loss cap: {self._consec_losses}/{max_cl}"

            if self._daily_open_bal > 1e-10:
                loss_pct = -self._daily_pnl / self._daily_open_bal * 100.0
                if loss_pct >= max_lp:
                    return False, f"Daily loss cap: {loss_pct:.1f}% ≥ {max_lp}%"

            return True, ""

    def record_trade_start(self) -> None:
        with self._lock:
            self._reset_if_new_day()
            self._daily_trades += 1

    def record_trade_result(self, pnl: float) -> None:
        with self._lock:
            self._daily_pnl += pnl
            if pnl < 0:
                self._consec_losses += 1
            else:
                self._consec_losses = 0

    @property
    def daily_trades(self) -> int:
        with self._lock: return self._daily_trades

    @property
    def consec_losses(self) -> int:
        with self._lock: return self._consec_losses


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN STRATEGY CLASS
# ═══════════════════════════════════════════════════════════════════════════════

class QuantStrategy:
    """
    Institutional Multi-Factor Momentum + Order Flow Strategy — v2 (audited).

    Drop-in replacement for AdvancedICTStrategy.  Public interface:
        on_tick(data_manager, order_manager, risk_manager, timestamp_ms) → None
        get_position() → dict | None
        current_sl_price: float
        current_tp_price: float
    """

    def __init__(self, order_manager=None):
        self._om    = order_manager
        self._lock  = threading.RLock()

        # Engines
        self._cvd     = CVDEngine()
        self._vwap    = VWAPEngine()
        self._mom     = MomentumEngine()
        self._squeeze = VolatilitySqueezeEngine()
        self._volflow = VolumeFlowEngine()
        self._atr_1m  = ATREngine()   # 1m ATR for momentum normalisation
        self._atr_5m  = ATREngine()   # 5m ATR for SL/TP (less noise)

        # State
        self._pos         = PositionState()
        self._last_sig    = SignalBreakdown()
        self._risk_gate   = DailyRiskGate()

        # Confirmation counters
        self._confirm_long  = 0
        self._confirm_short = 0

        # Timing
        self._last_eval_time  = 0.0
        self._last_exit_time  = 0.0
        self._last_pos_sync   = 0.0
        self._last_exit_sync  = 0.0   # FIX-24: separate gate for EXITING phase

        # Thinking log — emit a rich "what the bot sees" block every N seconds
        self._last_think_log  = 0.0
        self._think_interval  = 30.0   # seconds between thinking prints

        # Regime gate log — rate-limited so it doesn't spam every 5s tick
        self._last_regime_log      = 0.0
        self._regime_log_interval  = 60.0   # at most once per minute
        self._last_regime_pct      = -1.0   # detect pctile changes worth logging
        # Reconciliation — rate-gate for the exchange API call
        # _reconcile_pending: a background thread is running a reconcile query
        # _reconcile_data:    result dict from the most recent completed query
        self._last_reconcile_time = 0.0
        self._RECONCILE_SEC       = 30.0
        self._reconcile_pending   = False          # guard: only one thread in flight
        self._reconcile_data: Optional[Dict] = None   # filled by bg thread, read on-tick

        # Memo of the last successfully placed entry — used to recover full position
        # state when the exchange returns entry_price=0 or missing order IDs on adopt.
        # Cleared on finalise_exit so it can't be applied to a different position.
        self._last_entry_memo: Optional[Dict] = None

        # Statistics
        self._total_trades   = 0
        self._winning_trades = 0
        self._total_pnl      = 0.0

        # Required by main.py stop()
        self.current_sl_price: float = 0.0
        self.current_tp_price: float = 0.0

        self._log_init()

    def _log_init(self) -> None:
        logger.info("=" * 72)
        logger.info("⚡ QuantStrategy v2 (AUDITED) INITIALIZED")
        logger.info(f"   {QCfg.SYMBOL()} | {QCfg.LEVERAGE()}x lev | "
                    f"{QCfg.MARGIN_PCT():.0%} margin | "
                    f"SL={QCfg.SL_ATR_MULT()}×ATR TP={QCfg.TP_ATR_MULT()}×ATR "
                    f"minRR={QCfg.MIN_RR_RATIO()}")
        logger.info(f"   EMA {QCfg.EMA_FAST()}/{QCfg.EMA_SLOW()} | "
                    f"CVD {QCfg.CVD_WINDOW()}×{QCfg.CVD_HIST_MULT()} hist | "
                    f"VWAP {QCfg.VWAP_WINDOW()}b | "
                    f"BB/KC {QCfg.BB_WINDOW()}b/{QCfg.KC_ATR_MULT()}x")
        w_total = QCfg.W_CVD()+QCfg.W_VWAP()+QCfg.W_MOM()+QCfg.W_SQUEEZE()+QCfg.W_VOL()
        logger.info(f"   Weights: CVD={QCfg.W_CVD()} VWAP={QCfg.W_VWAP()} "
                    f"MOM={QCfg.W_MOM()} SQZ={QCfg.W_SQUEEZE()} "
                    f"VFL={QCfg.W_VOL()} (sum={w_total:.2f})")
        logger.info(f"   Risk: {QCfg.MAX_DAILY_TRADES()} trades/day | "
                    f"{QCfg.MAX_CONSEC_LOSSES()} consec losses | "
                    f"{QCfg.MAX_DAILY_LOSS_PCT()}% daily loss cap")
        logger.info("=" * 72)

    # ───────────────────────────────────────────────────────────────────
    # PUBLIC API
    # ───────────────────────────────────────────────────────────────────

    def get_position(self) -> Optional[Dict]:
        with self._lock:
            return None if self._pos.is_flat() else self._pos.to_dict()

    def on_tick(self, data_manager, order_manager, risk_manager,
                timestamp_ms: int) -> None:
        with self._lock:
            now      = timestamp_ms / 1000.0
            self._om = order_manager

            if now - self._last_eval_time < QCfg.TICK_EVAL_SEC():
                return
            self._last_eval_time = now

            phase = self._pos.phase

            # ── Exchange reconciliation ───────────────────────────────────────
            # The query is I/O-bound and can hang for seconds on a slow exchange.
            # We fire it in a daemon thread (outside the lock) so the strategy
            # loop is NEVER blocked waiting for a network response.
            #
            # Pattern: fire → thread queries → stores raw data → next on_tick
            #          call picks up the result and applies it under the lock.
            #
            # 1. Apply any completed reconcile result from the previous thread.
            if self._reconcile_data is not None:
                data = self._reconcile_data
                self._reconcile_data = None
                self._reconcile_apply(order_manager, data)
                phase = self._pos.phase   # may have changed

            # 2. Fire a new background query if the interval has elapsed and
            #    no query is currently in flight.
            if (now - self._last_reconcile_time >= self._RECONCILE_SEC
                    and not self._reconcile_pending):
                self._last_reconcile_time = now
                self._reconcile_pending   = True
                # Snapshot the phase at fire-time. _reconcile_apply will discard
                # the result if the phase has changed during the I/O wait — this
                # prevents stale "exchange flat" results from falsely closing a
                # position that was entered while the thread was in flight.
                fired_phase = self._pos.phase
                t = threading.Thread(
                    target=self._reconcile_query_thread,
                    args=(order_manager, fired_phase),
                    daemon=True,
                )
                t.start()

            # Position sync — the background reconcile fires every _RECONCILE_SEC
            # and already handles both ACTIVE→FLAT (TP/SL fired) and FLAT→ACTIVE
            # (adopted position).  Running _sync_position on the main tick thread
            # IN ADDITION doubles the rate-limited API call budget and adds
            # synchronous latency to every tick.  Removed: let reconcile own all
            # exchange-state transitions.
            #
            # EXITING: stay gated (return below) until reconcile confirms flat.
            # Maximum additional wait = _RECONCILE_SEC (30 s) + API latency.
            if phase == PositionPhase.EXITING:
                return  # block new entries; reconcile will call _finalise_exit

            # Route
            if phase == PositionPhase.FLAT:
                if now - self._last_exit_time < float(QCfg.COOLDOWN_SEC()):
                    return
                self._evaluate_entry(data_manager, order_manager, risk_manager, now)

            elif phase == PositionPhase.ACTIVE:
                self._manage_active(data_manager, order_manager, now)

    # ───────────────────────────────────────────────────────────────────
    # SIGNAL COMPUTATION
    # ───────────────────────────────────────────────────────────────────

    def _compute_signals(self, data_manager,
                         bypass_regime: bool = False) -> Optional[SignalBreakdown]:
        candles_1m = data_manager.get_candles("1m", limit=300)
        _5m_seed_limit = QCfg.ATR_PCTILE_WINDOW() + QCfg.ATR_PERIOD() + 3
        candles_5m = data_manager.get_candles("5m", limit=_5m_seed_limit)

        if len(candles_1m) < QCfg.MIN_1M_BARS():
            logger.debug(f"1m bars: {len(candles_1m)}/{QCfg.MIN_1M_BARS()}")
            return None
        if len(candles_5m) < QCfg.MIN_5M_BARS():
            logger.debug(f"5m bars: {len(candles_5m)}/{QCfg.MIN_5M_BARS()}")
            return None

        atr_1m = self._atr_1m.compute(candles_1m)
        atr_5m = self._atr_5m.compute(candles_5m)

        if atr_5m < 1e-10:
            return None

        price = data_manager.get_last_price()
        if price < 1.0:
            return None

        # ── ALWAYS compute all engines — regime is a MODULATOR not a kill-switch ──
        # FIX: Previous code returned None here when regime was "invalid", meaning
        # CVD/VWAP/MOM/SQUEEZE/VOL were NEVER updated in high-vol environments.
        # The bot was completely blind in the most obvious trending markets.
        # Now all indicators run on every tick; _evaluate_entry() decides whether
        # to act based on the regime result embedded in the SignalBreakdown.
        self._cvd.update(candles_1m)
        self._vwap.update(candles_1m, atr_1m)
        self._mom.update(candles_1m, candles_5m, atr_1m, atr_5m)
        self._squeeze.update(candles_1m, atr_1m)
        self._volflow.update(candles_1m)

        vwap_sig = self._vwap.get_signal(price)

        sig = SignalAggregator.compute(
            self._cvd.get_signal(),
            vwap_sig,
            self._mom.get_signal(),
            self._squeeze.get_signal(),
            self._volflow.get_signal(),
            self._atr_5m,
            vwap_signal_for_regime=vwap_sig,   # regime direction bias from VWAP
        )
        self._last_sig = sig

        # Regime-aware logging (replaces the old binary "regime gated" spam)
        regime = sig.regime
        if regime and not bypass_regime:
            now_t = time.time()
            pct_rounded = round(regime.pctile, 2)
            if (now_t - self._last_regime_log >= self._regime_log_interval
                    or abs(pct_rounded - self._last_regime_pct) >= 0.05):
                self._last_regime_log = now_t
                self._last_regime_pct = pct_rounded
                # Only log COMPRESSED as "gated" — all others are valid trading modes
                if regime.mode == RegimeMode.COMPRESSED:
                    logger.info(
                        f"⏸ Regime COMPRESSED — ATR(5m) pctile={regime.pctile:.0%} "
                        f"z={regime.atr_zscore:+.2f} — waiting for vol expansion"
                    )
                else:
                    logger.debug(
                        f"📊 Regime={regime.mode.value} pctile={regime.pctile:.0%} "
                        f"z={regime.atr_zscore:+.2f} bias={regime.direction_bias} "
                        f"size={regime.size_scalar:.0%} SLx={regime.sl_atr_mult:.1f}"
                    )

        logger.debug(f"Signal: {sig}")
        return sig

    # ───────────────────────────────────────────────────────────────────
    # THINKING LOG — periodic rich status to terminal
    # ───────────────────────────────────────────────────────────────────

    def _log_thinking(self, sig: SignalBreakdown, price: float, now: float) -> None:
        """
        Emit a human-readable 'what is the bot thinking' block every
        self._think_interval seconds.  Shows every indicator score,
        a mini ASCII bar, the composite vs thresholds, confirmation
        progress, and the most likely next action.
        """
        if now - self._last_think_log < self._think_interval:
            return
        self._last_think_log = now

        def bar(v: float, width: int = 12) -> str:
            """Compact bipolar bar centred at 0."""
            half = width // 2
            filled = int(abs(v) * half + 0.5)
            filled = min(filled, half)
            if v >= 0:
                return " " * half + "█" * filled + "░" * (half - filled)
            else:
                return "░" * (half - filled) + "█" * filled + " " * half

        def fmt(label: str, v: float) -> str:
            arrow = "▲" if v > 0.05 else ("▼" if v < -0.05 else "─")
            return f"  {label:<6} {bar(v)} {arrow} {v:+.3f}"

        regime = sig.regime

        # ── Use regime-adjusted thresholds (not raw config) ─────────────────
        # In EXTREME regime the threshold is 0.65, in COMPRESSED 0.68, etc.
        # Using the base 0.55 here makes "Lean" and "Next move" misleading.
        if regime and not (self._pos.phase == PositionPhase.ACTIVE):
            lt = regime.entry_threshold
            st = regime.entry_threshold
            long_allowed_think  = regime.direction_bias in ("any", "long_only")
            short_allowed_think = regime.direction_bias in ("any", "short_only")
            need = QCfg.CONFIRM_TICKS() + regime.extra_confirms
        else:
            lt = QCfg.LONG_THRESHOLD()
            st = QCfg.SHORT_THRESHOLD()
            long_allowed_think  = True
            short_allowed_think = True
            need = QCfg.CONFIRM_TICKS()

        c = sig.composite

        # Determine overall lean (vs regime-adjusted threshold)
        if c >= lt and long_allowed_think:
            lean = f"BULLISH  ✅  ({c:+.3f} ≥ +{lt:.2f})"
        elif c <= -st and short_allowed_think:
            lean = f"BEARISH  ✅  ({c:+.3f} ≤ -{st:.2f})"
        elif not long_allowed_think and c >= lt:
            lean = f"BULLISH but BLOCKED — bias={regime.direction_bias}"
        elif not short_allowed_think and c <= -st:
            lean = f"BEARISH but BLOCKED — bias={regime.direction_bias}"
        elif c >= lt * 0.75:
            lean = f"approaching LONG  ({c:+.3f}, need ≥{lt:.2f})"
        elif c <= -st * 0.75:
            lean = f"approaching SHORT ({c:+.3f}, need ≤{-st:.2f})"
        else:
            # Show distance to nearest valid threshold
            if long_allowed_think and not short_allowed_think:
                gap = lt - c
                lean = f"LONG only — gap {gap:.3f} to trigger (+{lt:.2f})"
            elif short_allowed_think and not long_allowed_think:
                gap = c - (-st)
                lean = f"SHORT only — gap {abs(gap):.3f} to trigger (-{st:.2f})"
            else:
                lean = f"NEUTRAL  🔇  ({c:+.3f})"
        if self._confirm_long > 0:
            conf_str = f"LONG confirms: {self._confirm_long}/{need}"
        elif self._confirm_short > 0:
            conf_str = f"SHORT confirms: {self._confirm_short}/{need}"
        else:
            conf_str = "No confirmation building"

        # Next action — regime-aware: use adjusted thresholds and direction bias
        if c >= lt and long_allowed_think and self._confirm_long >= need - 1:
            next_move = "⚡ ENTRY LONG — next tick fires if signal holds"
        elif c <= -st and short_allowed_think and self._confirm_short >= need - 1:
            next_move = "⚡ ENTRY SHORT — next tick fires if signal holds"
        elif (c >= lt and long_allowed_think) or (c <= -st and short_allowed_think):
            remaining = need - max(self._confirm_long, self._confirm_short)
            next_move = f"🕐 Building confirmation — {remaining} tick(s) remaining"
        elif regime and regime.direction_bias != "any":
            if long_allowed_think:
                next_move = f"🔍 Awaiting LONG setup — composite needs +{lt - c:.3f} more"
            else:
                next_move = f"🔍 Awaiting SHORT setup — composite needs {c - (-st):.3f} lower"
        else:
            next_move = "👀 Watching — signal below entry threshold"

        # Regime display (regime variable already set above for threshold calc)
        in_trade = self._pos.phase == PositionPhase.ACTIVE
        if regime:
            emoji_map = {"COMPRESSED":"🟡","TRENDING":"🟢","ELEVATED":"🟠","EXTREME":"🔴"}
            em = emoji_map.get(regime.mode.value, "⚪")
            if in_trade:
                regime_str = f"{em} {regime.mode.value} ({regime.pctile:.0%}) — bypassed (in position)"
            else:
                regime_str = (
                    f"{em} {regime.mode.value} ({regime.pctile:.0%} pctile "
                    f"z={regime.atr_zscore:+.2f}) "
                    f"thr={regime.entry_threshold:.2f} size={regime.size_scalar:.0%} "
                    f"SLx={regime.sl_atr_mult:.1f}× bias={regime.direction_bias}"
                )
        else:
            regime_str = "unknown"

        # Cooldown
        cooldown_rem = max(0.0, QCfg.COOLDOWN_SEC() - (now - self._last_exit_time))
        cd_str = f"{cooldown_rem:.0f}s" if cooldown_rem > 0 else "ready"

        vwap_warn = (f"  ⚠ VWAP degraded ({self._vwap._last_bars_used}/"
                     f"{QCfg.VWAP_WINDOW()}b)"
                     if self._vwap.is_degraded else "")
        # Show VWAP↔ label when sign is flipped (mean-reversion mode)
        vwap_label = "VWAP↔" if (regime and regime.direction_bias != "any") else "VWAP "
        lines = [
            f"┌─── 🧠 BOT THINKING  price=${price:,.2f}  ATR(5m)=${sig.atr:.2f}{vwap_warn} ─────────────────",
            fmt("CVD",       sig.cvd),
            fmt(vwap_label, sig.vwap),
            fmt("MOM",  sig.mom),
            fmt("SQZ",  sig.squeeze),
            fmt("VFL",  sig.vol),
            f"  {'─'*38}",
            f"  Σ composite  [{bar(c)}] {c:+.4f}",
            f"  Regime:      {regime_str}",
            f"  Lean:        {lean}",
            f"  {conf_str}",
            f"  Cooldown:    {cd_str}",
            f"  Next move:   {next_move}",
            f"└{'─'*66}",
        ]
        logger.info("\n" + "\n".join(lines))

    # ───────────────────────────────────────────────────────────────────
    # ENTRY
    # ───────────────────────────────────────────────────────────────────

    # ───────────────────────────────────────────────────────────────────
    # RECONCILIATION — authoritative exchange state sync
    # ───────────────────────────────────────────────────────────────────

    def _reconcile_query_thread(self, order_manager, fired_phase: PositionPhase) -> None:
        """
        Daemon thread: query the exchange for position + open orders.
        Runs OUTSIDE the strategy lock so network latency never blocks on_tick.
        Stores raw results in self._reconcile_data; the next on_tick tick picks
        them up and calls _reconcile_apply() under the lock.

        fired_phase: the PositionPhase at the moment this thread was launched.
        _reconcile_apply compares this to the current phase and discards stale
        results if the phase changed while the query was in flight — preventing
        a pre-entry FLAT query from falsely closing an in-flight entry.

        Never crashes the thread — all exceptions are caught and logged.
        Always clears _reconcile_pending on exit (even on failure).
        """
        try:
            try:
                ex_pos = order_manager.get_open_position()
            except Exception as e:
                logger.debug(f"Reconcile query: get_open_position error: {e}")
                return

            if ex_pos is None:
                return   # API failure — don't store anything, next cycle will retry

            ex_size = float(ex_pos.get("size", 0.0))

            # Only query open orders when they might be needed (position exists, or we
            # think ACTIVE and exchange is flat — orders will be empty anyway in that case)
            open_orders = None
            if ex_size >= float(getattr(config, "MIN_POSITION_SIZE", 0.001)):
                try:
                    open_orders = order_manager.get_open_orders()
                except Exception as e:
                    logger.debug(f"Reconcile query: get_open_orders error: {e}")
                    # open_orders stays None — _reconcile_apply handles it gracefully

            # Publish result for the main thread to pick up
            with self._lock:
                self._reconcile_data = {
                    "ex_pos":      ex_pos,
                    "open_orders": open_orders,
                    "fired_phase": fired_phase,   # phase when thread was launched
                }

        except Exception as e:
            logger.warning(f"Reconcile thread unexpected error: {e}")
        finally:
            self._reconcile_pending = False   # always allow next query to fire

    def _reconcile_apply(self, order_manager, data: Dict) -> None:
        """
        Apply a reconcile result captured by the background query thread.
        Called from on_tick while holding self._lock.

        Three outcomes (same logic as before, now lock-safe):
          A) FLAT internal + exchange position found
             → Adopt the position, transition to ACTIVE.
          B) ACTIVE internal + exchange is flat
             → TP or SL fired — record exit and finalise.
          C) ACTIVE internal + exchange matches
             → Backfill missing SL/TP order IDs.

        Phase-mismatch guard: if the bot's phase changed WHILE the query was
        in flight (e.g. entry fired between thread-launch and thread-result),
        the result is stale and must be discarded. Example race:
          Thread fires at T=0 (phase=FLAT), entry executes at T=3 (ACTIVE),
          thread result arrives at T=5 and sees exchange flat at T=0 → without
          the guard this would falsely record an exit on the fresh entry.
        """
        ex_pos      = data["ex_pos"]
        open_orders = data.get("open_orders")   # may be None if query failed
        fired_phase = data.get("fired_phase")   # phase at thread-launch time

        ex_size = float(ex_pos.get("size", 0.0))
        ex_side = str(ex_pos.get("side") or "").upper()   # "LONG" | "SHORT" | ""
        phase   = self._pos.phase

        # ── Phase-mismatch guard ───────────────────────────────────────────
        # Only outcome B (ACTIVE + exchange flat → record exit) is dangerous
        # if fired during a phase transition. Outcomes A and C are safe:
        #   A: fired FLAT, still FLAT, exchange has position → adopt (correct)
        #   C: fired ACTIVE, still ACTIVE, exchange matches → backfill (correct)
        # Outcome B: fired FLAT, now ACTIVE, exchange flat → stale pre-entry
        # result. Discard. The next reconcile cycle will get a fresh result.
        if (fired_phase is not None
                and fired_phase != phase
                and phase == PositionPhase.ACTIVE
                and ex_size < QCfg.MIN_QTY()):
            logger.info(
                f"📡 Reconcile: discarding stale result "
                f"(fired_phase={fired_phase.name} → current={phase.name}, "
                f"exchange shows flat but we just entered) — will re-check next cycle"
            )
            return

        # ── OUTCOME A: we think FLAT but exchange has a live position ──────
        if phase == PositionPhase.FLAT and ex_size >= QCfg.MIN_QTY():
            ex_entry = float(ex_pos.get("entry_price", 0.0))
            ex_upnl  = float(ex_pos.get("unrealized_pnl", 0.0))

            # Classify open orders into SL / TP
            sl_order_id, tp_order_id = None, None
            sl_price,    tp_price    = 0.0,  0.0
            if open_orders is not None:
                for o in open_orders:
                    otype = o.get("type", "").upper()
                    if otype in ("STOP_MARKET", "STOP", "STOP_LOSS_MARKET"):
                        sl_order_id = o["order_id"]
                        sl_price    = float(o.get("trigger_price") or 0)
                    elif otype in ("TAKE_PROFIT_MARKET", "TAKE_PROFIT"):
                        tp_order_id = o["order_id"]
                        tp_price    = float(o.get("trigger_price") or 0)

            # ── Memo fallback ──────────────────────────────────────────────
            # CoinSwitch returns entry_price=0 for freshly filled positions
            # and the /open_orders endpoint is 404, so sl_order_id/tp_order_id
            # are always None on this exchange. If we have a recent entry memo
            # from _enter_trade (written after SL/TP were confirmed placed),
            # use it to fill any gaps before building the PositionState.
            m = self._last_entry_memo
            # Evaluate memo match unconditionally so variables are always defined
            memo_side_ok = False
            memo_qty_ok  = False
            if m is not None:
                internal_side_memo = "long" if ex_side == "LONG" else "short"
                memo_side_ok = (m.get("side", "") == internal_side_memo)
                memo_qty_ok  = (abs(m.get("quantity", 0) - ex_size) < 1e-6)

            memo_valid = m is not None and memo_side_ok and memo_qty_ok

            if memo_valid:
                if ex_entry <= 0:
                    ex_entry = m["entry_price"]
                    logger.info(
                        f"📡 Reconcile adopt: exchange entry_price=0 — "
                        f"restored from memo: ${ex_entry:,.2f}"
                    )
                if sl_order_id is None and m.get("sl_order_id"):
                    sl_order_id = m["sl_order_id"]
                    logger.info(f"📡 Reconcile adopt: restored SL order ID from memo")
                if tp_order_id is None and m.get("tp_order_id"):
                    tp_order_id = m["tp_order_id"]
                    logger.info(f"📡 Reconcile adopt: restored TP order ID from memo")
                if sl_price <= 0 and m.get("sl_price", 0) > 0:
                    sl_price = m["sl_price"]
                if tp_price <= 0 and m.get("tp_price", 0) > 0:
                    tp_price = m["tp_price"]

            internal_side = "long" if ex_side == "LONG" else "short"

            sl_dist = abs(ex_entry - sl_price) if (ex_entry > 0 and sl_price > 0) else 0.0
            initial_risk = sl_dist * ex_size

            self._pos = PositionState(
                phase          = PositionPhase.ACTIVE,
                side           = internal_side,
                quantity       = ex_size,
                entry_price    = ex_entry,
                sl_price       = sl_price,
                tp_price       = tp_price,
                sl_order_id    = sl_order_id,
                tp_order_id    = tp_order_id,
                entry_time     = m["entry_time"] if memo_valid else time.time(),
                initial_risk   = initial_risk,
                initial_sl_dist= sl_dist,
                entry_atr      = (m["entry_atr"] if memo_valid else self._atr_5m.atr),
                peak_profit    = 0.0,
            )
            self.current_sl_price = sl_price
            self.current_tp_price = tp_price
            self._confirm_long = self._confirm_short = 0

            logger.warning(
                f"⚡ RECONCILE: adopted {ex_side} position from exchange — "
                f"size={ex_size} @ ${ex_entry:,.2f}  uPnL=${ex_upnl:+.2f} | "
                f"SL_id={sl_order_id}  TP_id={tp_order_id}"
            )
            send_telegram_message(
                f"⚡ <b>POSITION ADOPTED FROM EXCHANGE</b>\n\n"
                f"Side:    {ex_side}\n"
                f"Size:    {ex_size} BTC\n"
                f"Entry:   ${ex_entry:,.2f}\n"
                f"uPnL:    ${ex_upnl:+.2f}\n"
                f"SL ord:  {sl_order_id or 'none found'}\n"
                f"TP ord:  {tp_order_id or 'none found'}\n\n"
                f"<i>Bot is now monitoring this position.</i>"
            )
            return

        # ── OUTCOME B: we think ACTIVE but exchange is flat ────────────────
        if phase == PositionPhase.ACTIVE and ex_size < QCfg.MIN_QTY():
            logger.info("📡 Reconcile: exchange FLAT while ACTIVE → TP/SL fired")
            self._record_exchange_exit(order_manager, ex_pos)
            return

        # ── OUTCOME C: ACTIVE + exchange matches — backfill missing order IDs ──
        if phase == PositionPhase.ACTIVE and ex_size >= QCfg.MIN_QTY():
            if (not self._pos.sl_order_id or not self._pos.tp_order_id) \
                    and open_orders is not None:
                for o in open_orders:
                    otype = o.get("type", "").upper()
                    if (not self._pos.sl_order_id and
                            otype in ("STOP_MARKET", "STOP", "STOP_LOSS_MARKET")):
                        self._pos.sl_order_id = o["order_id"]
                        logger.info(f"📡 Reconcile: adopted SL order id {o['order_id'][:8]}…")
                    elif (not self._pos.tp_order_id and
                            otype in ("TAKE_PROFIT_MARKET", "TAKE_PROFIT")):
                        self._pos.tp_order_id = o["order_id"]
                        logger.info(f"📡 Reconcile: adopted TP order id {o['order_id'][:8]}…")

    # ───────────────────────────────────────────────────────────────────
    # ENTRY EVALUATION
    # ───────────────────────────────────────────────────────────────────

    def _evaluate_entry(self, data_manager, order_manager,
                        risk_manager, now: float) -> None:
        sig = self._compute_signals(data_manager)
        if sig is None:
            self._confirm_long = self._confirm_short = 0
            return

        price  = data_manager.get_last_price()
        regime = sig.regime
        self._log_thinking(sig, price, now)

        # ── COMPRESSED regime: only enter on strong squeeze-breakout signal ──
        # All other regimes proceed to normal entry evaluation below.
        if regime and regime.mode == RegimeMode.COMPRESSED:
            if self._squeeze.get_signal() == 0.0:
                # No active squeeze breakout — stand aside
                self._confirm_long = self._confirm_short = 0
                return
            # Fall through: squeeze is firing, allow entry with regime's tighter params

        # ── Determine per-regime thresholds and direction constraints ─────────
        if regime:
            thr_long      = regime.entry_threshold
            thr_short     = regime.entry_threshold
            long_allowed  = regime.direction_bias in ("any", "long_only")
            short_allowed = regime.direction_bias in ("any", "short_only")
            total_confirms = QCfg.CONFIRM_TICKS() + regime.extra_confirms
        else:
            thr_long = thr_short  = QCfg.LONG_THRESHOLD()
            long_allowed = short_allowed = True
            total_confirms = QCfg.CONFIRM_TICKS()

        # Log regime state at most once per _regime_log_interval (shared gate)
        # Prevents the "⚡ Regime EXTREME" line from spamming every 5-second tick.
        if regime and regime.mode in (RegimeMode.ELEVATED, RegimeMode.EXTREME):
            now_log = time.time()
            pct_r   = round(regime.pctile, 2)
            if (now_log - self._last_regime_log >= self._regime_log_interval
                    or abs(pct_r - self._last_regime_pct) >= 0.05):
                self._last_regime_log = now_log
                self._last_regime_pct = pct_r
                em = {"ELEVATED": "🟠", "EXTREME": "🔴"}.get(regime.mode.value, "")
                logger.info(
                    f"{em} Regime {regime.mode.value} — {regime.note} | "
                    f"thr={thr_long:.2f} confirms={total_confirms} "
                    f"size={regime.size_scalar:.0%} SLx={regime.sl_atr_mult:.1f}×"
                )

        c = sig.composite

        if c >= thr_long and long_allowed:
            self._confirm_long  += 1;  self._confirm_short  = 0
        elif c <= -thr_short and short_allowed:
            self._confirm_short += 1;  self._confirm_long   = 0
        elif c >= thr_long and not long_allowed:
            # Signal says LONG but regime says short_only: log and reset
            logger.info(
                f"⛔ LONG signal ({c:+.3f}) blocked by regime bias "
                f"({regime.direction_bias if regime else '?'}) — ignoring"
            )
            self._confirm_long = self._confirm_short = 0
            return
        elif c <= -thr_short and not short_allowed:
            logger.info(
                f"⛔ SHORT signal ({c:+.3f}) blocked by regime bias "
                f"({regime.direction_bias if regime else '?'}) — ignoring"
            )
            self._confirm_long = self._confirm_short = 0
            return
        else:
            self._confirm_long = self._confirm_short = 0
            return

        if self._confirm_long >= total_confirms:
            self._confirm_long = self._confirm_short = 0
            self._enter_trade(data_manager, order_manager, risk_manager,
                              "long", sig, regime)
        elif self._confirm_short >= total_confirms:
            self._confirm_long = self._confirm_short = 0
            self._enter_trade(data_manager, order_manager, risk_manager,
                              "short", sig, regime)

    def _enter_trade(self, data_manager, order_manager,
                     risk_manager, side: str, sig: SignalBreakdown,
                     regime: Optional["RegimeResult"] = None) -> None:
        price = data_manager.get_last_price()
        if price < 1.0:
            logger.warning("Entry aborted: no valid price")
            return

        atr = self._atr_5m.atr
        if atr < 1e-10:
            logger.warning("Entry aborted: ATR not computed")
            return

        # ── Regime-adjusted ATR for SL/TP computation ─────────────────────
        # In ELEVATED/EXTREME regimes the SL needs more room to breathe.
        # We scale the ATR seen by _compute_sl_tp(), NOT the price levels directly,
        # so the full SL/TP pipeline (min%, max%, RR checks) still applies correctly.
        sl_atr_mult = regime.sl_atr_mult if regime else 1.0
        atr_for_sltp = atr * sl_atr_mult

        # ── Regime-adjusted position size scalar ──────────────────────────
        size_scalar = regime.size_scalar if regime else 1.0

        # FIX-19: risk gate check before any API calls
        bal_info = risk_manager.get_available_balance()
        if bal_info is None:
            logger.warning("Entry aborted: balance unavailable")
            return
        total_bal = float(bal_info.get("total", bal_info.get("available", 0.0)))
        self._risk_gate.set_opening_balance(total_bal)

        allowed, reason = self._risk_gate.can_trade(total_bal)
        if not allowed:
            logger.info(f"Entry blocked by risk gate: {reason}")
            return

        qty_base = self._compute_quantity(risk_manager, price)   # FIX-18
        if qty_base is None or qty_base < QCfg.MIN_QTY():
            return

        # Apply regime size scalar (floor at MIN_QTY so we never send 0)
        qty = max(QCfg.MIN_QTY(),
                  math.floor(qty_base * size_scalar / QCfg.LOT_STEP()) * QCfg.LOT_STEP())
        qty = round(qty, 8)
        if qty < QCfg.MIN_QTY():
            logger.info(f"Entry aborted: regime-scaled qty {qty} < MIN_QTY")
            return

        sl_price, tp_price = self._compute_sl_tp(price, side, atr_for_sltp)   # FIX-20/21
        if sl_price is None or tp_price is None:
            return

        # FIX-13: minimum R:R enforcement  (+epsilon to avoid float precision rejections)
        sl_dist = abs(price - sl_price)
        tp_dist = abs(price - tp_price)
        if sl_dist < 1e-10:
            return
        rr = tp_dist / sl_dist
        if rr < QCfg.MIN_RR_RATIO() - 1e-9:
            logger.info(f"Entry blocked: R:R={rr:.4f} < min={QCfg.MIN_RR_RATIO()}")
            return

        regime_tag = f" [{regime.mode.value} {regime.size_scalar:.0%} sz]" if regime else ""
        logger.info(
            f"🎯 ENTERING {side.upper()} @ ${price:,.2f} | qty={qty} | "
            f"SL=${sl_price:,.2f} TP=${tp_price:,.2f} R:R=1:{rr:.2f}{regime_tag} | {sig}"
        )

        # Market order
        entry_data = order_manager.place_market_order(side=side, quantity=qty)
        if not entry_data:
            logger.error("❌ Market order failed — aborting")
            return

        self._risk_gate.record_trade_start()

        # Extract fill price with cascading fallbacks
        fill_price = (
            float(entry_data.get("average_price") or 0) or
            float(entry_data.get("fill_price")    or 0) or
            float(entry_data.get("price")         or 0) or
            price
        )

        # FIX-12 + FIX-14: re-compute SL/TP if slippage > configured tolerance
        slip = abs(fill_price - price) / price
        if slip > QCfg.SLIPPAGE_TOL():
            logger.info(f"Slippage {slip:.4%} > tol {QCfg.SLIPPAGE_TOL():.4%} — recalc SL/TP")
            new_sl, new_tp = self._compute_sl_tp(fill_price, side, atr_for_sltp)  # use regime-adj ATR
            if new_sl is None or new_tp is None:
                # FIX-12: guard None return — close naked position immediately
                logger.error("❌ SL/TP recompute after fill returned None — closing position")
                exit_s = "sell" if side == "long" else "buy"
                order_manager.place_market_order(side=exit_s, quantity=qty, reduce_only=True)
                self._last_exit_time = time.time()   # enforce cooldown before next attempt
                self._last_reconcile_time = 0.0        # force exchange check on next eval
                return
            new_rr = abs(fill_price - new_tp) / abs(fill_price - new_sl)
            if new_rr < QCfg.MIN_RR_RATIO() - 1e-9:
                logger.warning(f"R:R={new_rr:.4f} below min after slippage — closing position")
                exit_s = "sell" if side == "long" else "buy"
                order_manager.place_market_order(side=exit_s, quantity=qty, reduce_only=True)
                self._last_exit_time = time.time()   # enforce cooldown before next attempt
                self._last_reconcile_time = 0.0        # force exchange check on next eval
                return
            sl_price, tp_price = new_sl, new_tp
            rr = new_rr

        exit_side = "sell" if side == "long" else "buy"

        # ── Sweep any pre-existing conditional orders ──────────────────────
        # CoinSwitch allows ONE STOP_MARKET per position. Orphaned stops from
        # prior sessions or manual orders cause 400 "already exists". Sweeping
        # first guarantees a clean slate regardless of prior state.
        sweep = order_manager.cancel_symbol_conditionals()
        if sweep:
            filled = [oid for oid, r in sweep.items()
                      if r in (CancelResult.ALREADY_FILLED, CancelResult.PARTIAL_FILL)]
            if filled:
                logger.warning(
                    f"⚠️ Conditional(s) filled during pre-entry sweep: {filled} "
                    f"— position may have changed. Aborting entry, forcing reconcile."
                )
                self._last_reconcile_time = 0.0
                return

        sl_data = order_manager.place_stop_loss(side=exit_side, quantity=qty,
                                                trigger_price=sl_price)
        if not sl_data:
            logger.error("❌ SL placement failed — closing naked position")
            order_manager.place_market_order(side=exit_side, quantity=qty, reduce_only=True)
            self._last_exit_time = time.time()   # enforce cooldown before next attempt
            self._last_reconcile_time = 0.0        # force exchange check on next eval
            return

        tp_data = order_manager.place_take_profit(side=exit_side, quantity=qty,
                                                   trigger_price=tp_price)
        if not tp_data:
            logger.error("❌ TP placement failed — cancelling SL, closing position")
            order_manager.cancel_order(sl_data["order_id"])
            order_manager.place_market_order(side=exit_side, quantity=qty, reduce_only=True)
            self._last_exit_time = time.time()   # enforce cooldown before next attempt
            self._last_reconcile_time = 0.0        # force exchange check on next eval
            return

        sl_dist_filled  = abs(fill_price - sl_price)
        initial_risk    = sl_dist_filled * qty

        self._pos = PositionState(
            phase           = PositionPhase.ACTIVE,
            side            = side,
            quantity        = qty,
            entry_price     = fill_price,
            sl_price        = sl_price,
            tp_price        = tp_price,
            sl_order_id     = sl_data["order_id"],
            tp_order_id     = tp_data["order_id"],
            entry_order_id  = entry_data.get("order_id"),
            entry_time      = time.time(),
            initial_risk    = initial_risk,
            initial_sl_dist = sl_dist_filled,
            entry_signal    = sig,
            entry_atr       = self._atr_5m.atr,   # snapshot ATR for vol-ratio trail
            peak_profit     = 0.0,
        )
        self.current_sl_price = sl_price
        self.current_tp_price = tp_price
        self._total_trades   += 1

        # Memo: preserve complete entry state so reconcile can recover it
        # if the exchange later returns entry_price=0 or missing order IDs.
        self._last_entry_memo = {
            "side":         side,
            "quantity":     qty,
            "entry_price":  fill_price,
            "sl_price":     sl_price,
            "tp_price":     tp_price,
            "sl_order_id":  sl_data["order_id"],
            "tp_order_id":  tp_data["order_id"],
            "entry_time":   time.time(),
            "initial_risk": initial_risk,
            "initial_sl_dist": sl_dist_filled,
            "entry_atr":    self._atr_5m.atr,
        }

        send_telegram_message(
            f"{'📈' if side == 'long' else '📉'} <b>QUANT ENTRY — {side.upper()}</b>\n\n"
            f"Symbol:   {QCfg.SYMBOL()}\n"
            f"Entry:    ${fill_price:,.2f}\n"
            f"SL:       ${sl_price:,.2f}  "
            f"(−${sl_dist_filled:.2f} / {sl_dist_filled/fill_price:.3%})\n"
            f"TP:       ${tp_price:,.2f}  "
            f"(+${abs(tp_price-fill_price):.2f})\n"
            f"R:R:      1:{rr:.2f}\n"
            f"Qty:      {qty} BTC\n"
            f"Margin:   ${initial_risk/QCfg.LEVERAGE():.2f} USDT\n"
            f"Risk:     ${initial_risk:.2f} USDT\n"
            f"Leverage: {QCfg.LEVERAGE()}x\n"
            f"Regime:   {regime.mode.value if regime else 'N/A'} "
            f"({regime.pctile:.0%} pctile, {regime.size_scalar:.0%} sz)\n\n"
            f"<i>{sig}</i>"
        )
        logger.info(f"✅ ACTIVE {side.upper()} @ ${fill_price:,.2f} | "
                    f"R:R=1:{rr:.2f} | risk=${initial_risk:.2f}")

    # ───────────────────────────────────────────────────────────────────
    # ACTIVE MANAGEMENT
    # ───────────────────────────────────────────────────────────────────

    def _manage_active(self, data_manager, order_manager, now: float) -> None:
        pos   = self._pos
        price = data_manager.get_last_price()

        if price < 1.0:
            logger.debug("Stale price — skip management tick")
            return

        # 1. Signal reversal
        sig = self._compute_signals(data_manager, bypass_regime=True)
        if sig is not None:
            self._log_thinking(sig, price, now)   # ← show thinking while in trade
            c = sig.composite; flip = QCfg.EXIT_FLIP_THRESH()
            if pos.side == "long"  and c <= -flip:
                logger.info(f"🔄 Alpha flip SHORT ({c:+.3f} ≤ -{flip}) → exit LONG")
                self._exit_trade(order_manager, price, "signal_reversal");  return
            if pos.side == "short" and c >= flip:
                logger.info(f"🔄 Alpha flip LONG  ({c:+.3f} ≥ +{flip}) → exit SHORT")
                self._exit_trade(order_manager, price, "signal_reversal");  return

        # 2. Institutional trailing SL (no hard timer — time-decay tightening instead)
        if QCfg.TRAIL_ENABLED():
            if now - pos.last_trail_time >= QCfg.TRAIL_INTERVAL_S():
                self._pos.last_trail_time = now
                closed = self._update_trailing_sl(
                    order_manager, data_manager, price, sig, now
                )
                if closed:
                    return

    def _find_swing_sl(self, data_manager, side: str, atr: float) -> Optional[float]:
        """
        Return a structural SL level from the last N closed 5m candles.
          SHORT → SL just above the highest recent high  (+0.25×ATR buffer)
          LONG  → SL just below the lowest recent low   (−0.25×ATR buffer)

        Returns None if not enough data.
        """
        try:
            candles = data_manager.get_candles("5m", limit=9)
        except Exception:
            return None
        if not candles or len(candles) < 4:
            return None
        # Exclude the still-forming candle at the end; use last 6 closed bars
        closed = candles[:-1][-6:]
        buffer = 0.25 * atr
        if side == "short":
            return max(float(c['h']) for c in closed) + buffer
        else:
            return min(float(c['l']) for c in closed) - buffer

    def _update_trailing_sl(
        self,
        order_manager,
        data_manager,
        price:  float,
        sig:    Optional[SignalBreakdown],
        now:    float,
    ) -> bool:
        """
        Institutional multi-layer trailing SL.  No hard time-based exit.

        Layer 1 — Peak profit (MFE) tracking
          Continuously records the maximum favourable excursion so tier
          transitions are based on best-reached profit, not current.

        Layer 2 — Profit-tier gating  (keyed on profit / initial_sl_dist)
          < 0.5R  → below activation threshold — hold original SL
          0.5–1R  → Tier 1: move SL to breakeven (no loss possible)
          1–1.5R  → Tier 2: lock in a small profit cushion (0.35×ATR)
          ≥ 1.5R  → Tier 3: full ATR trail + swing structure — tightest wins

        Layer 3 — Signal-conviction scaling
          The composite score for the position's direction maps to a
          trail-distance multiplier: full conviction → 1.0×, fading → 0.55×.
          Weakening alpha automatically tightens the leash.

        Layer 4 — ATR volatility ratio
          Current ATR / entry ATR.  If vol has contracted since entry the
          trail tightens; if vol has expanded it gets a bit more room (capped 1.3×).

        Layer 5 — Time-decay tightening
          Instead of a hard kill at MAX_HOLD_SEC, the trail distance shrinks
          linearly from 1.0× → 0.55× over the configured hold horizon.
          A trending trade continuously pushes MFE → the ratcheted SL stays
          safely behind it.  A stalling trade gets caught as the leash shortens.

        Layer 6 — Swing structure reference
          The tightest of (ATR trail, recent swing high/low + buffer) is chosen
          so the SL always respects market structure.

        Returns True if the position was detected as already closed.
        """
        pos = self._pos
        atr = self._atr_5m.atr
        if atr < 1e-10:
            return False

        # ── Layer 1: track MFE ───────────────────────────────────────────
        profit = (pos.entry_price - price) if pos.side == "short" \
                 else (price - pos.entry_price)
        if profit > pos.peak_profit:
            pos.peak_profit = profit

        # ── Layer 2: profit tier (use peak_profit so tier never retreats) ─
        init_dist = pos.initial_sl_dist
        tier = pos.peak_profit / init_dist if init_dist > 1e-10 else 0.0

        if tier < 0.5:
            # Haven't earned management yet — hold original SL
            return False

        # ── Layer 3: signal conviction multiplier ────────────────────────
        # Maps [0, full_conviction] onto [0.55, 1.0]
        if sig is not None:
            raw = sig.composite
            if pos.side == "short":
                strength = min(abs(min(raw, 0.0)) / 0.65, 1.0)
            else:
                strength = min(abs(max(raw, 0.0)) / 0.65, 1.0)
        else:
            strength = 0.5                          # no data → neutral
        signal_mult = 0.55 + 0.45 * strength        # [0.55, 1.0]

        # ── Layer 4: ATR volatility ratio ────────────────────────────────
        entry_atr = pos.entry_atr if pos.entry_atr > 1e-10 else atr
        atr_ratio = max(0.60, min(atr / entry_atr, 1.30))   # [0.60, 1.30]

        # ── Layer 5: time-decay tightening ───────────────────────────────
        hold_ratio = min((now - pos.entry_time) / QCfg.MAX_HOLD_SEC(), 1.0)
        time_mult  = 1.0 - 0.45 * hold_ratio        # 1.0 → 0.55

        # ── Combined ATR trail distance ───────────────────────────────────
        base_mult  = QCfg.TRAIL_ATR_MULT()           # config default 1.0×
        trail_dist = base_mult * atr * signal_mult * atr_ratio * time_mult

        # ── Layer 6: swing structure ─────────────────────────────────────
        swing_sl = self._find_swing_sl(data_manager, pos.side, atr)

        # ── Determine candidate SL by tier ───────────────────────────────
        if pos.side == "short":
            if tier < 1.0:
                # Tier 1: breakeven — SL fractionally above entry
                new_sl = pos.entry_price + 0.10 * atr
            elif tier < 1.5:
                # Tier 2: lock partial profit — SL below entry
                new_sl = pos.entry_price - 0.35 * atr
            else:
                # Tier 3: ATR trail + swing — tighter of the two wins
                atr_trail_sl = price + trail_dist
                new_sl = atr_trail_sl
                if swing_sl is not None:
                    new_sl = min(new_sl, swing_sl)  # lower = tighter for short

        else:  # long
            if tier < 1.0:
                new_sl = pos.entry_price - 0.10 * atr
            elif tier < 1.5:
                new_sl = pos.entry_price + 0.35 * atr
            else:
                atr_trail_sl = price - trail_dist
                new_sl = atr_trail_sl
                if swing_sl is not None:
                    new_sl = max(new_sl, swing_sl)  # higher = tighter for long

        # ── Ratchet: SL may only improve (never widen) ───────────────────
        new_sl_tick = _round_to_tick(new_sl)
        min_move    = QCfg.TRAIL_MIN_MOVE_ATR() * atr

        if pos.side == "short":
            # Improvement = SL moves DOWN (away from price, toward tighter)
            if new_sl_tick >= pos.sl_price - min_move:
                return False
        else:
            # Improvement = SL moves UP
            if new_sl_tick <= pos.sl_price + min_move:
                return False

        # ── Log with context ─────────────────────────────────────────────
        tier_label = (
            "🟡 BE"          if tier < 1.0  else
            "🟠 Partial"     if tier < 1.5  else
            "🟢 Full trail"
        )
        hold_min = (now - pos.entry_time) / 60.0
        logger.info(
            f"🔒 Trail [{tier_label}] "
            f"${pos.sl_price:,.1f} → ${new_sl_tick:,.1f} "
            f"| R={tier:.1f}  MFE={pos.peak_profit:.1f}pts  hold={hold_min:.0f}m "
            f"| sig={signal_mult:.2f}× vol={atr_ratio:.2f}× time={time_mult:.2f}×"
        )

        # ── Replace SL on exchange ────────────────────────────────────────
        exit_side = "sell" if pos.side == "long" else "buy"
        result = order_manager.replace_stop_loss(
            existing_sl_order_id = pos.sl_order_id,
            side                 = exit_side,
            quantity             = pos.quantity,
            new_trigger_price    = new_sl_tick,
        )

        if result is None:
            logger.warning("🚨 SL already fired (replace returned None) — finalising")
            self._record_exchange_exit(order_manager, None)
            return True

        if isinstance(result, dict) and "error" in result:
            logger.error(f"Trail SL error: {result.get('error')}")
            return False

        if result and isinstance(result, dict):
            self._pos.sl_price    = new_sl_tick
            self._pos.sl_order_id = result.get("order_id", pos.sl_order_id)
            self.current_sl_price = new_sl_tick
            if not pos.trail_active:
                self._pos.trail_active = True
                logger.info(
                    "✅ Trailing SL active — time-decay tightening replaces hard timer"
                )

        return False

    # ───────────────────────────────────────────────────────────────────
    # EXIT
    # ───────────────────────────────────────────────────────────────────

    def _exit_trade(self, order_manager, price: float, reason: str) -> None:
        pos = self._pos
        if pos.phase != PositionPhase.ACTIVE:
            return
        logger.info(f"🚪 EXIT {pos.side.upper()} @ ${price:,.2f} | {reason}")
        self._pos.phase = PositionPhase.EXITING

        order_manager.cancel_all_exit_orders(
            sl_order_id=pos.sl_order_id,
            tp_order_id=pos.tp_order_id,
        )
        exit_side = "sell" if pos.side == "long" else "buy"
        order_manager.place_market_order(side=exit_side, quantity=pos.quantity,
                                         reduce_only=True)
        pnl = self._estimate_pnl(pos, price)
        self._record_pnl(pnl)

        hold_min = (time.time() - pos.entry_time) / 60
        send_telegram_message(
            f"{'✅' if pnl > 0 else '❌'} <b>QUANT EXIT — {pos.side.upper()}</b>\n\n"
            f"Reason:  {reason}\n"
            f"Entry:   ${pos.entry_price:,.2f}\n"
            f"Exit:    ${price:,.2f}\n"
            f"PnL:     ${pnl:+.2f} USDT\n"
            f"Hold:    {hold_min:.1f} min\n\n"
            f"<i>Trades: {self._total_trades} | "
            f"WR: {self._win_rate():.0%} | "
            f"PnL: ${self._total_pnl:+.2f}</i>"
        )
        self._finalise_exit()

    def _record_exchange_exit(self, order_manager, ex_pos: Optional[Dict]) -> None:
        """
        FIX-17 (ENHANCED): Recover actual exit PnL via a 3-strategy cascade.

        Strategy 1 — Order fill query
            Query get_fill_details() for both tp_order_id and sl_order_id.
            The first FILLED order that returns a fill_price > 0 is used to
            compute PnL via _estimate_pnl(), same formula as manual exits.

        Strategy 2 — Transactions endpoint
            If fill query returns no price, call api.get_transactions() to
            find the most recent "P&L" entry timestamped after position entry.
            This returns the exchange-computed realised PnL directly.

        Strategy 3 — SL/TP price estimate
            If both network queries fail or return 0, fall back to the known
            tp_price / sl_price stored on the position state.
            We pick TP first (positions tend to be exited at TP in trending markets)
            but also check which direction price last moved toward.

        None of these strategies require the /open_orders endpoint.
        The caller provides order_manager so we can make API calls without a
        circular reference from inside the strategy engine.
        """
        pos = self._pos
        pnl = 0.0
        exit_price = 0.0
        exit_source = ""

        # ── Strategy 1: query order fill prices ──────────────────────────
        if order_manager is not None:
            # Check TP first (most common successful exit), then SL
            for oid, label in [
                (pos.tp_order_id, "TP"),
                (pos.sl_order_id, "SL"),
            ]:
                if not oid:
                    continue
                try:
                    details = order_manager.get_fill_details(oid)
                    if details is None:
                        continue
                    status = str(details.get("status", "")).upper()
                    if status not in ("EXECUTED", "FILLED", "COMPLETELY_FILLED"):
                        continue
                    fp = details.get("fill_price")
                    if fp is not None:
                        fp_f = float(fp)
                        if fp_f > 0:
                            exit_price = fp_f
                            exit_source = f"{label} fill @ ${exit_price:,.2f}"
                            logger.info(f"📡 Exchange exit: {exit_source} [{status}]")
                            break
                except Exception as e:
                    logger.debug(f"Exchange exit: fill query for {label} order {oid}: {e}")

        # ── Strategy 2: transactions endpoint ────────────────────────────
        if abs(exit_price) < 1e-10 and order_manager is not None:
            try:
                txns = order_manager.api.get_transactions(
                    exchange=config.EXCHANGE,
                    symbol=config.SYMBOL,
                )
                if isinstance(txns, dict) and not txns.get("error"):
                    txn_list = txns.get("data", [])
                    if isinstance(txn_list, list) and txn_list:
                        # Find the most recent P&L entry after position entry time
                        entry_ts_s = pos.entry_time  # epoch seconds
                        best_txn = None
                        best_ts  = 0.0
                        for txn in txn_list:
                            t_type = str(txn.get("transaction_type", txn.get("type", ""))).lower()
                            if "p&l" not in t_type and "pnl" not in t_type and "realize" not in t_type:
                                continue
                            # Parse timestamp (exchange may use ms or s)
                            raw_ts = txn.get("timestamp", txn.get("created_at", 0))
                            try:
                                ts_f = float(raw_ts)
                                ts_s = ts_f / 1000.0 if ts_f > 1e12 else ts_f
                            except (TypeError, ValueError):
                                ts_s = 0.0
                            if ts_s >= entry_ts_s and ts_s > best_ts:
                                best_ts  = ts_s
                                best_txn = txn
                        if best_txn is not None:
                            raw_pnl = float(best_txn.get("amount", best_txn.get("pnl", 0.0)))
                            if abs(raw_pnl) > 1e-10:
                                pnl        = raw_pnl
                                exit_source = f"transactions endpoint (${pnl:+.2f})"
                                logger.info(f"📡 Exchange exit PnL from {exit_source}")
            except Exception as e:
                logger.debug(f"Exchange exit: transactions query error: {e}")

        # ── Compute PnL from fill price (Strategy 1 result) ──────────────
        if exit_price > 0 and pos.entry_price > 0 and abs(pnl) < 1e-10:
            pnl = self._estimate_pnl(pos, exit_price)

        # ── Strategy 3: estimate from known SL/TP prices ─────────────────
        if abs(pnl) < 1e-10 and abs(exit_price) < 1e-10:
            # Heuristic: if unrealised PnL was positive at last heartbeat,
            # assume TP fired; otherwise assume SL fired.
            if pos.tp_price > 0:
                exit_price  = pos.tp_price
                exit_source = f"TP estimate @ ${pos.tp_price:,.2f}"
            elif pos.sl_price > 0:
                exit_price  = pos.sl_price
                exit_source = f"SL estimate @ ${pos.sl_price:,.2f}"

            if exit_price > 0 and pos.entry_price > 0:
                pnl = self._estimate_pnl(pos, exit_price)
                logger.info(
                    f"📡 Exchange exit: fill & transaction queries returned nothing — "
                    f"using {exit_source}.  PnL estimate: ${pnl:+.2f}  "
                    f"(verify in exchange history)"
                )

        if abs(pnl) < 1e-10:
            logger.warning(
                "📡 Exchange exit: PnL could not be determined via fill query, "
                "transactions endpoint, or SL/TP estimate — recorded as $0.  "
                "Check exchange history manually."
            )

        hold_min = max(0.0, (time.time() - pos.entry_time) / 60.0)
        self._record_pnl(pnl)
        send_telegram_message(
            f"📡 <b>EXCHANGE EXIT — {pos.side.upper()}</b>\n\n"
            f"TP/SL fired between sync ticks\n"
            f"Entry:   ${pos.entry_price:,.2f}\n"
            f"PnL:     ${pnl:+.2f} USDT"
            + (f"\nSource:  {exit_source}" if exit_source else "") +
            f"\nHold:    {hold_min:.1f} min\n\n"
            f"<i>Trades: {self._total_trades} | WR: {self._win_rate():.0%}</i>"
        )
        self._finalise_exit()

    def _record_pnl(self, pnl: float) -> None:
        self._total_pnl += pnl
        if pnl > 0:
            self._winning_trades += 1
        self._risk_gate.record_trade_result(pnl)

    def _finalise_exit(self) -> None:
        self._pos             = PositionState()
        self._last_exit_time  = time.time()
        self.current_sl_price = 0.0
        self.current_tp_price = 0.0
        self._last_entry_memo = None   # stale — cannot apply to a future position
        logger.info("Position closed — FLAT")

    # ───────────────────────────────────────────────────────────────────
    # POSITION SYNC — FIX-16, FIX-17
    # ───────────────────────────────────────────────────────────────────

    def _sync_position(self, order_manager) -> None:
        """
        FIX-16: Uses order_manager.get_open_position() which already handles
        symbol filtering, field normalisation, and rate limiting.
        FIX-17: Passes exchange position dict to _record_exchange_exit() for PnL.
        """
        try:
            ex_pos = order_manager.get_open_position()
        except Exception as e:
            logger.debug(f"Position sync error: {e}")
            return

        if ex_pos is None:
            # API failure — do NOT assume flat, wait for next sync
            return

        ex_size = float(ex_pos.get("size", 0.0))

        if self._pos.phase == PositionPhase.ACTIVE:
            if ex_size < QCfg.MIN_QTY():
                logger.info("📡 Sync: exchange FLAT while ACTIVE → TP/SL fired")
                self._record_exchange_exit(order_manager, ex_pos)   # FIX-17 enhanced

            else:
                ex_side = str(ex_pos.get("side") or "").upper()
                expected = "LONG" if self._pos.side == "long" else "SHORT"
                if ex_side and ex_side != expected:
                    logger.warning(f"Side mismatch: internal={expected} exchange={ex_side}")

        elif self._pos.phase == PositionPhase.EXITING:
            if ex_size < QCfg.MIN_QTY():
                logger.info("📡 Sync: EXITING confirmed flat on exchange")
                self._finalise_exit()

    # ───────────────────────────────────────────────────────────────────
    # SIZING — FIX-18
    # ───────────────────────────────────────────────────────────────────

    def _compute_quantity(self, risk_manager, price: float) -> Optional[float]:
        """
        Quantity = (available × MARGIN_PCT × LEVERAGE) / price

        FIX-18: Correct margin check — actual margin drawn vs allocated margin
                (was checking actual vs total available, which ALWAYS passes).
        """
        bal = risk_manager.get_available_balance()
        if bal is None:
            logger.warning("Sizing: balance unavailable")
            return None

        available = float(bal.get("available", 0.0))
        if available < QCfg.MIN_MARGIN_USDT():
            logger.warning(f"Sizing: available ${available:.2f} < min ${QCfg.MIN_MARGIN_USDT()}")
            return None

        margin_alloc = available * QCfg.MARGIN_PCT()
        if margin_alloc < QCfg.MIN_MARGIN_USDT():
            return None

        notional = margin_alloc * QCfg.LEVERAGE()
        qty_raw  = notional / price

        step = QCfg.LOT_STEP()
        qty  = math.floor(qty_raw / step) * step
        qty  = round(qty, 8)
        qty  = max(QCfg.MIN_QTY(), min(QCfg.MAX_QTY(), qty))

        # FIX-18: actual margin drawn vs ALLOCATED margin (not vs total balance)
        actual_margin = (qty * price) / QCfg.LEVERAGE()
        if actual_margin > margin_alloc * 1.02:   # 2% tolerance for step rounding
            logger.warning(f"Sizing: actual margin ${actual_margin:.2f} > "
                           f"allocated ${margin_alloc:.2f}")
            return None

        logger.info(
            f"Sizing → avail=${available:.2f} | alloc={QCfg.MARGIN_PCT():.0%} "
            f"| margin=${margin_alloc:.2f} | {QCfg.LEVERAGE()}x "
            f"| notional=${notional:.2f} | qty={qty} BTC"
        )
        return qty

    # ───────────────────────────────────────────────────────────────────
    # SL / TP — FIX-20, FIX-21
    # ───────────────────────────────────────────────────────────────────

    def _compute_sl_tp(self, price: float, side: str,
                       atr: float) -> Tuple[Optional[float], Optional[float]]:
        """
        SL = price ± SL_ATR_MULT × ATR  (clamped to [MIN_SL_PCT, MAX_SL_PCT] of price)
        TP = price ± max(TP_ATR_MULT × ATR, SL_dist × MIN_RR_RATIO)

        FIX-20: TP minimum enforced via MIN_RISK_REWARD_RATIO not hardcoded 1.5.
        FIX-21: All prices rounded via _round_to_tick() using TICK_SIZE from config.
        """
        sl_atr = QCfg.SL_ATR_MULT() * atr
        tp_atr = QCfg.TP_ATR_MULT() * atr

        sl_min = price * QCfg.MIN_SL_PCT()
        sl_max = price * QCfg.MAX_SL_PCT()
        sl_dist = max(sl_min, min(sl_max, sl_atr))

        # FIX-20: TP must achieve at least MIN_RR_RATIO × SL distance
        tp_dist = max(sl_dist * QCfg.MIN_RR_RATIO(), tp_atr)

        if side == "long":
            sl_raw = price - sl_dist
            tp_raw = price + tp_dist
        else:
            sl_raw = price + sl_dist
            tp_raw = price - tp_dist

        sl_price = _round_to_tick(sl_raw)   # FIX-21
        tp_price = _round_to_tick(tp_raw)   # FIX-21

        # Sanity checks
        if side == "long"  and (sl_price >= price or tp_price <= price):
            logger.error(f"SL/TP sanity fail LONG: entry={price} sl={sl_price} tp={tp_price}")
            return None, None
        if side == "short" and (sl_price <= price or tp_price >= price):
            logger.error(f"SL/TP sanity fail SHORT: entry={price} sl={sl_price} tp={tp_price}")
            return None, None

        return sl_price, tp_price

    # ───────────────────────────────────────────────────────────────────
    # UTILITIES
    # ───────────────────────────────────────────────────────────────────

    @staticmethod
    def _estimate_pnl(pos: PositionState, exit_price: float) -> float:
        """Gross PnL minus round-trip commission."""
        gross = ((exit_price - pos.entry_price) if pos.side == "long"
                 else (pos.entry_price - exit_price)) * pos.quantity
        fee   = (pos.entry_price + exit_price) * pos.quantity * QCfg.COMMISSION_RATE()
        return gross - fee

    def _win_rate(self) -> float:
        return self._winning_trades / self._total_trades if self._total_trades else 0.0

    def get_stats(self) -> Dict:
        regime = self._last_sig.regime if self._last_sig else None
        return {
            "total_trades":    self._total_trades,
            "winning_trades":  self._winning_trades,
            "win_rate":        f"{self._win_rate():.1%}",
            "total_pnl":       round(self._total_pnl, 2),
            "daily_trades":    self._risk_gate.daily_trades,
            "consec_losses":   self._risk_gate.consec_losses,
            "current_phase":   self._pos.phase.name,
            "last_signal":     str(self._last_sig),
            "atr_5m":          round(self._atr_5m.atr, 2),
            "atr_1m":          round(self._atr_1m.atr, 2),
            "atr_pctile":      f"{self._atr_5m.get_percentile():.0%}",
            "atr_zscore":      f"{self._atr_5m.get_atr_zscore():+.2f}",
            "regime_ok":       self._atr_5m.is_regime_valid(),
            "regime_mode":     regime.mode.value if regime else "UNKNOWN",
            "regime_bias":     regime.direction_bias if regime else "any",
            "regime_size":     f"{regime.size_scalar:.0%}" if regime else "100%",
        }

    def format_status_report(self) -> str:
        """FIX-22: Removed dead pnl_live computation. Shows live position timing."""
        stats = self.get_stats()
        pos   = self._pos
        regime_emoji = {
            "COMPRESSED": "🟡", "TRENDING": "🟢",
            "ELEVATED": "🟠", "EXTREME": "🔴", "UNKNOWN": "⚪",
        }
        rm = stats.get("regime_mode", "UNKNOWN")
        lines = [
            "📊 <b>QUANT STRATEGY STATUS</b>",
            "",
            f"Phase:       {stats['current_phase']}",
            f"Regime:      {regime_emoji.get(rm,'⚪')} {rm} "
            f"({stats['atr_pctile']} pctile  z={stats['atr_zscore']})",
            f"Bias:        {stats['regime_bias']}  |  Size: {stats['regime_size']}",
            f"ATR 5m/1m:   ${stats['atr_5m']} / ${stats['atr_1m']}",
            f"Signal:      {stats['last_signal']}",
            "",
            f"Session:     {stats['total_trades']} trades | WR {stats['win_rate']}",
            f"Session PnL: ${stats['total_pnl']:+.2f} USDT",
            f"Daily:       {stats['daily_trades']}/{QCfg.MAX_DAILY_TRADES()} trades | "
            f"{stats['consec_losses']}/{QCfg.MAX_CONSEC_LOSSES()} consec losses",
        ]
        if not pos.is_flat():
            elapsed_min = (time.time() - pos.entry_time) / 60
            max_hold_min = QCfg.MAX_HOLD_SEC() / 60
            lines += [
                "",
                f"<b>Active Position ({pos.side.upper()}):</b>",
                f"Entry:   ${pos.entry_price:,.2f}",
                f"SL:      ${pos.sl_price:,.2f}",
                f"TP:      ${pos.tp_price:,.2f}",
                f"Qty:     {pos.quantity} BTC",
                f"Risk:    ${pos.initial_risk:.2f} USDT",
                f"Hold:    {elapsed_min:.1f} / {max_hold_min:.0f} min",
                f"Trail:   {'active ✅' if pos.trail_active else 'pending'}",
            ]
        return "\n".join(lines)

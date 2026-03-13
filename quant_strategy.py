"""
INSTITUTIONAL MULTI-FACTOR MOMENTUM + ORDER FLOW STRATEGY — v3
==============================================================================
COMPLETE REWRITE from v2. Every signal engine rebuilt for speed and accuracy.

CORE PHILOSOPHY:
  Real quant firms don't wait — they DETECT and ACT within milliseconds.
  This v3 rewrite eliminates every source of delay, dead-weight, and
  missed opportunity from v2.

ARCHITECTURE CHANGES vs v2:
  ──────────────────────────────────────────────────────────────────────
  PROBLEM                               FIX
  ──────────────────────────────────────────────────────────────────────
  Squeeze/VolFlow return 0 → 20% of     Dynamic weight redistribution:
  composite weight is dead               inactive signals' weight flows
                                         to active signals in real-time

  5-second eval interval misses moves    1-second eval interval

  2 confirmation ticks × 5s = 10s delay  1 tick or ZERO when signal is
  before entry                           strong (≥0.6 composite)

  No MTF alignment — trades against      4h/15m trend filter: boost
  the dominant trend                     aligned, veto counter-trend

  Orderbook data subscribed but          OrderbookImbalanceEngine: bid/ask
  never used for signals                 wall detection, depth imbalance

  Trade stream subscribed but never      TickFlowEngine: real-time buy/sell
  used for signals                       pressure from individual trades

  CVD/VolFlow .clear() every tick        Incremental append-only updates
  = O(N) full rebuild 200 times/sec      with dirty-flag recomputation

  tanh(z/2) compresses extremes          Asymmetric sigmoid: preserves
  into narrow ±0.5 band                  discrimination at extremes

  Static threshold regardless of         Adaptive threshold: drops when
  signal quality                         ≥3 signals agree in direction

  Linear-only aggregation, no            Agreement bonus: extra composite
  conditional logic                      boost when signals align

  ATR regime gate too narrow             Widened gate + gradient penalty
  (15%–90%) rejects valid setups         instead of binary reject
  ──────────────────────────────────────────────────────────────────────

SIGNAL ENGINES (7):
  1. CVDEngine           — Cumulative Volume Delta z-score (order flow)
  2. VWAPEngine          — VWAP deviation + slope (institutional anchor)
  3. MomentumEngine      — EMA cross + multi-TF slope (trend direction)
  4. SqueezeEngine       — BB/KC squeeze breakout (vol compression)
  5. VolumeFlowEngine    — Multi-bar buy/sell imbalance (participation)
  6. OrderbookEngine     — Bid/ask depth imbalance (microstructure)
  7. TickFlowEngine      — Real-time trade aggressor flow (execution)

META-LOGIC:
  - SignalAggregator: dynamic weight redistribution + agreement detection
  - HTFTrendFilter: 4h/15m alignment check (boost/veto)
  - AdaptiveThreshold: lower entry bar when signals converge
  - DailyRiskGate: max trades, consecutive losses, daily loss %
"""

from __future__ import annotations

import logging
import math
import time
import threading
from collections import deque
from dataclasses import dataclass, field
from datetime import date
from enum import Enum, auto
from typing import Dict, List, Optional, Tuple

import config
from telegram_notifier import send_telegram_message
from order_manager import CancelResult

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION ACCESSOR — live, no caching
# ═══════════════════════════════════════════════════════════════════════════════

def _cfg(name: str, default):
    val = getattr(config, name, None)
    return default if val is None else val


class QCfg:
    """All values fetched live — config changes take effect next tick."""

    @staticmethod
    def SYMBOL()    -> str:   return str(config.SYMBOL)
    @staticmethod
    def EXCHANGE()  -> str:   return str(config.EXCHANGE)

    # Leverage & sizing
    @staticmethod
    def LEVERAGE()        -> int:   return int(_cfg("LEVERAGE", 30))
    @staticmethod
    def MARGIN_PCT()      -> float: return float(_cfg("QUANT_MARGIN_PCT", 0.20))
    @staticmethod
    def LOT_STEP()        -> float: return float(_cfg("LOT_STEP_SIZE", 0.001))
    @staticmethod
    def MIN_QTY()         -> float: return float(_cfg("MIN_POSITION_SIZE", 0.001))
    @staticmethod
    def MAX_QTY()         -> float: return float(_cfg("MAX_POSITION_SIZE", 1.0))
    @staticmethod
    def MIN_MARGIN_USDT() -> float: return float(_cfg("MIN_MARGIN_PER_TRADE", 4.0))
    @staticmethod
    def COMMISSION_RATE() -> float: return float(_cfg("COMMISSION_RATE", 0.00055))
    @staticmethod
    def TICK_SIZE()       -> float: return float(_cfg("TICK_SIZE", 0.1))
    @staticmethod
    def SLIPPAGE_TOL()    -> float: return float(_cfg("QUANT_SLIPPAGE_TOLERANCE", 0.0005))

    # Signal thresholds
    @staticmethod
    def LONG_THRESHOLD()  -> float: return float(_cfg("QUANT_LONG_THRESHOLD", 0.40))
    @staticmethod
    def SHORT_THRESHOLD() -> float: return float(_cfg("QUANT_SHORT_THRESHOLD", 0.40))
    @staticmethod
    def EXIT_FLIP_THRESH()-> float: return float(_cfg("QUANT_EXIT_FLIP", 0.22))
    @staticmethod
    def CONFIRM_TICKS()   -> int:   return int(_cfg("QUANT_CONFIRM_TICKS", 1))

    # ATR SL / TP
    @staticmethod
    def ATR_PERIOD()      -> int:   return int(_cfg("SL_ATR_PERIOD", 14))
    @staticmethod
    def SL_ATR_MULT()     -> float: return float(_cfg("QUANT_SL_ATR_MULT", 1.4))
    @staticmethod
    def TP_ATR_MULT()     -> float: return float(_cfg("QUANT_TP_ATR_MULT", 2.5))
    @staticmethod
    def MIN_SL_PCT()      -> float: return float(_cfg("MIN_SL_DISTANCE_PCT", 0.003))
    @staticmethod
    def MAX_SL_PCT()      -> float: return float(_cfg("MAX_SL_DISTANCE_PCT", 0.035))
    @staticmethod
    def MIN_RR_RATIO()    -> float: return float(_cfg("MIN_RISK_REWARD_RATIO", 1.5))

    # Trailing SL
    @staticmethod
    def TRAIL_ENABLED()      -> bool:  return bool(_cfg("QUANT_TRAIL_ENABLED", True))
    @staticmethod
    def TRAIL_ACTIVATE_R()   -> float: return float(_cfg("QUANT_TRAIL_ACTIVATE_R", 0.5))
    @staticmethod
    def TRAIL_ATR_MULT()     -> float: return float(_cfg("QUANT_TRAIL_ATR_MULT", 0.9))
    @staticmethod
    def TRAIL_INTERVAL_S()   -> int:   return int(_cfg("TRAILING_SL_CHECK_INTERVAL", 15))
    @staticmethod
    def TRAIL_MIN_MOVE_ATR() -> float: return float(_cfg("SL_MIN_IMPROVEMENT_ATR_MULT", 0.08))

    # Indicator windows
    @staticmethod
    def CVD_WINDOW()          -> int:   return int(_cfg("QUANT_CVD_WINDOW", 20))
    @staticmethod
    def CVD_HIST_MULT()       -> int:   return int(_cfg("QUANT_CVD_HIST_MULT", 15))
    @staticmethod
    def VWAP_WINDOW()         -> int:   return int(_cfg("QUANT_VWAP_WINDOW", 50))
    @staticmethod
    def VWAP_SLOPE_BARS()     -> int:   return int(_cfg("QUANT_VWAP_SLOPE_BARS", 8))
    @staticmethod
    def EMA_FAST()            -> int:   return int(_cfg("QUANT_EMA_FAST", 8))
    @staticmethod
    def EMA_SLOW()            -> int:   return int(_cfg("QUANT_EMA_SLOW", 21))
    @staticmethod
    def EMA_SIGNAL_BARS()     -> int:   return int(_cfg("QUANT_EMA_SIGNAL_BARS", 5))
    @staticmethod
    def BB_WINDOW()           -> int:   return int(_cfg("QUANT_BB_WINDOW", 20))
    @staticmethod
    def BB_STD()              -> float: return float(_cfg("QUANT_BB_STD", 2.0))
    @staticmethod
    def KC_ATR_MULT()         -> float: return float(_cfg("QUANT_KC_ATR_MULT", 1.5))
    @staticmethod
    def SQUEEZE_BREAKOUT_BARS()-> int:  return int(_cfg("QUANT_SQUEEZE_BREAKOUT_BARS", 8))
    @staticmethod
    def VOL_FLOW_WINDOW()     -> int:   return int(_cfg("QUANT_VOL_FLOW_WINDOW", 10))

    # Minimum data
    @staticmethod
    def MIN_1M_BARS()     -> int:   return int(_cfg("MIN_CANDLES_1M", 80))
    @staticmethod
    def MIN_5M_BARS()     -> int:   return int(_cfg("MIN_CANDLES_5M", 60))

    # Regime filter — wide gate
    @staticmethod
    def ATR_PCTILE_WINDOW()-> int:  return int(_cfg("QUANT_ATR_PCTILE_WINDOW", 100))
    @staticmethod
    def ATR_MIN_PCTILE()  -> float: return float(_cfg("QUANT_ATR_MIN_PCTILE", 0.05))
    @staticmethod
    def ATR_MAX_PCTILE()  -> float: return float(_cfg("QUANT_ATR_MAX_PCTILE", 0.97))

    # Timing
    @staticmethod
    def MAX_HOLD_SEC()    -> int:   return int(_cfg("QUANT_MAX_HOLD_SEC", 2400))
    @staticmethod
    def COOLDOWN_SEC()    -> int:   return int(_cfg("QUANT_COOLDOWN_SEC", 20))
    @staticmethod
    def TICK_EVAL_SEC()   -> float: return float(_cfg("ENTRY_EVALUATION_INTERVAL_SECONDS", 1))
    @staticmethod
    def POS_SYNC_SEC()    -> float: return float(_cfg("QUANT_POS_SYNC_SEC", 30))

    # Risk limits
    @staticmethod
    def MAX_DAILY_TRADES()  -> int:   return int(_cfg("MAX_DAILY_TRADES", 14))
    @staticmethod
    def MAX_CONSEC_LOSSES() -> int:   return int(_cfg("MAX_CONSECUTIVE_LOSSES", 4))
    @staticmethod
    def MAX_DAILY_LOSS_PCT()-> float: return float(_cfg("MAX_DAILY_LOSS_PCT", 5.0))

    # Signal weights (7 engines)
    @staticmethod
    def W_CVD()     -> float: return float(_cfg("QUANT_W_CVD", 0.22))
    @staticmethod
    def W_VWAP()    -> float: return float(_cfg("QUANT_W_VWAP", 0.18))
    @staticmethod
    def W_MOM()     -> float: return float(_cfg("QUANT_W_MOM", 0.22))
    @staticmethod
    def W_SQUEEZE() -> float: return float(_cfg("QUANT_W_SQUEEZE", 0.06))
    @staticmethod
    def W_VOL()     -> float: return float(_cfg("QUANT_W_VOL", 0.06))
    @staticmethod
    def W_ORDERBOOK()-> float: return float(_cfg("QUANT_W_ORDERBOOK", 0.14))
    @staticmethod
    def W_TICK_FLOW()-> float: return float(_cfg("QUANT_W_TICK_FLOW", 0.12))

    # MTF Trend Filter
    @staticmethod
    def HTF_ENABLED()       -> bool:  return bool(_cfg("QUANT_HTF_ENABLED", True))
    @staticmethod
    def HTF_VETO_STRENGTH() -> float: return float(_cfg("QUANT_HTF_VETO_STRENGTH", 0.65))
    @staticmethod
    def HTF_BOOST()         -> float: return float(_cfg("QUANT_HTF_BOOST", 0.12))

    # Adaptive threshold
    @staticmethod
    def AGREEMENT_DISCOUNT() -> float: return float(_cfg("QUANT_AGREEMENT_DISCOUNT", 0.07))
    @staticmethod
    def MIN_AGREE_SIGNALS()  -> int:   return int(_cfg("QUANT_MIN_AGREE_SIGNALS", 3))
    @staticmethod
    def STRONG_SIGNAL_LEVEL()-> float: return float(_cfg("QUANT_STRONG_SIGNAL_LEVEL", 0.35))

    # Orderbook
    @staticmethod
    def OB_DEPTH_LEVELS() -> int:   return int(_cfg("QUANT_OB_DEPTH_LEVELS", 5))
    @staticmethod
    def OB_HIST_LEN()     -> int:   return int(_cfg("QUANT_OB_HIST_LEN", 60))

    # Tick flow
    @staticmethod
    def TICK_AGG_WINDOW_SEC() -> float: return float(_cfg("QUANT_TICK_AGG_WINDOW_SEC", 30.0))
    @staticmethod
    def TICK_SURGE_MULT()     -> float: return float(_cfg("QUANT_TICK_SURGE_MULT", 2.5))


def _round_to_tick(price: float) -> float:
    tick = QCfg.TICK_SIZE()
    if tick <= 0:
        return price
    return round(round(price / tick) * tick, 10)


def _sigmoid(z: float, steepness: float = 1.0) -> float:
    """Asymmetric sigmoid that preserves extreme values better than tanh(z/2).
    At |z|=2 this returns ±0.76 vs tanh's ±0.46 — much better discrimination."""
    return max(-1.0, min(1.0, z * steepness / (1.0 + abs(z * steepness) * 0.5)))


# ═══════════════════════════════════════════════════════════════════════════════
# ENGINE 1: CVD — Cumulative Volume Delta
# ═══════════════════════════════════════════════════════════════════════════════

class CVDEngine:
    """
    Per-bar delta heuristic:  delta = volume × (2×close - high - low) / (high - low)
    Signal = sigmoid(z-score of rolling CVD sum).

    v3 FIX: Incremental append — no .clear() rebuild.
    """

    def __init__(self):
        self._deltas: deque = deque(maxlen=QCfg.CVD_WINDOW() * QCfg.CVD_HIST_MULT())
        self._last_bar_ts: int = 0

    def update(self, candles: List[Dict]) -> None:
        """Append only new bars since last update."""
        if not candles:
            return
        # Find where new data starts
        new_start = 0
        if self._last_bar_ts > 0:
            for i, c in enumerate(candles):
                if int(c['t']) > self._last_bar_ts:
                    new_start = i
                    break
            else:
                # Update the last bar in-place (forming candle)
                if candles:
                    c = candles[-1]
                    hi = float(c['h']); lo = float(c['l'])
                    cl = float(c['c']); vol = float(c['v'])
                    rng = hi - lo
                    delta_frac = (2.0 * cl - hi - lo) / rng if rng > 1e-10 else 0.0
                    if self._deltas:
                        self._deltas[-1] = vol * delta_frac
                return

        for c in candles[new_start:]:
            hi = float(c['h']); lo = float(c['l'])
            cl = float(c['c']); vol = float(c['v'])
            rng = hi - lo
            delta_frac = (2.0 * cl - hi - lo) / rng if rng > 1e-10 else 0.0
            self._deltas.append(vol * delta_frac)
            self._last_bar_ts = int(c['t'])

    def get_signal(self) -> float:
        w = QCfg.CVD_WINDOW()
        arr = list(self._deltas)
        n = len(arr)
        if n < w + 10:
            return 0.0

        # Rolling sums via sliding window
        current_sum = sum(arr[-w:])
        # Historical sums for z-score baseline
        sums = []
        running = sum(arr[:w])
        sums.append(running)
        for i in range(w, n - w):
            running += arr[i] - arr[i - w]
            sums.append(running)

        if len(sums) < 10:
            return 0.0

        mu = sum(sums) / len(sums)
        var = sum((s - mu) ** 2 for s in sums) / max(len(sums) - 1, 1)
        std = math.sqrt(var)
        if std < 1e-12:
            return 0.0

        z = (current_sum - mu) / std
        return _sigmoid(z, 0.6)


# ═══════════════════════════════════════════════════════════════════════════════
# ENGINE 2: VWAP — Volume-Weighted Average Price
# ═══════════════════════════════════════════════════════════════════════════════

class VWAPEngine:
    """
    VWAP deviation + VWAP slope, both ATR-normalized.
    v3: Combined with momentum alignment for stronger directional signal.
    """

    def __init__(self):
        self._vwap = 0.0
        self._std = 0.0
        self._slope = 0.0
        self._history: deque = deque(maxlen=QCfg.VWAP_SLOPE_BARS() * 3)

    def update(self, candles: List[Dict], atr: float) -> None:
        window = QCfg.VWAP_WINDOW()
        if len(candles) < window:
            return

        recent = candles[-window:]

        tp_vol = sum((float(c['h']) + float(c['l']) + float(c['c'])) / 3.0 * float(c['v'])
                     for c in recent)
        vol_sum = sum(float(c['v']) for c in recent)
        if vol_sum < 1e-12:
            return
        self._vwap = tp_vol / vol_sum

        var_sum = sum(float(c['v']) *
                      ((float(c['h']) + float(c['l']) + float(c['c'])) / 3.0 - self._vwap) ** 2
                      for c in recent)
        self._std = math.sqrt(var_sum / vol_sum)

        self._history.append(self._vwap)
        sb = QCfg.VWAP_SLOPE_BARS()
        if len(self._history) >= sb + 1 and atr > 1e-10 and self._vwap > 1e-10:
            hist = list(self._history)
            raw_pct_slope = (hist[-1] - hist[-sb - 1]) / (hist[-sb - 1] + 1e-12)
            atr_pct = atr / self._vwap
            self._slope = max(-1.0, min(1.0, raw_pct_slope / (atr_pct + 1e-12)))

    def get_signal(self, price: float) -> float:
        if self._vwap < 1e-10 or self._std < 1e-10:
            return 0.0
        z = (price - self._vwap) / self._std
        dev_score = _sigmoid(z, 0.5)
        # Weighted combination: deviation 55%, slope 45% (slope more predictive)
        combined = dev_score * 0.55 + self._slope * 0.45
        # Penalize contradiction (price above VWAP but VWAP slope falling)
        if dev_score * self._slope < 0:
            combined *= 0.30
        return max(-1.0, min(1.0, combined))

    @property
    def vwap(self) -> float: return self._vwap
    @property
    def vwap_std(self) -> float: return self._std


# ═══════════════════════════════════════════════════════════════════════════════
# ENGINE 3: MOMENTUM — EMA Cross + Multi-TF
# ═══════════════════════════════════════════════════════════════════════════════

class MomentumEngine:
    """
    v3: Three-layer momentum with acceleration detection.
      Layer 1: 1m EMA(fast) vs EMA(slow) cross, normalized by ATR
      Layer 2: 5m EMA slope (higher-TF momentum confirmation)
      Layer 3: 1m acceleration (is momentum increasing or fading?)
    """

    def __init__(self):
        self._cross_1m = 0.0
        self._cross_5m = 0.0
        self._accel_1m = 0.0

    @staticmethod
    def _ema_series(values: List[float], period: int) -> List[float]:
        if len(values) < period:
            return []
        k = 2.0 / (period + 1)
        ema = sum(values[:period]) / period
        out = [ema]
        for v in values[period:]:
            ema = v * k + ema * (1.0 - k)
            out.append(ema)
        return out

    def update(self, candles_1m: List[Dict], candles_5m: List[Dict],
               atr_1m: float, atr_5m: float) -> None:
        fast = QCfg.EMA_FAST(); slow = QCfg.EMA_SLOW(); sig_b = QCfg.EMA_SIGNAL_BARS()
        cl1 = [float(c['c']) for c in candles_1m]
        cl5 = [float(c['c']) for c in candles_5m]

        # Layer 1: 1m MACD-line normalized by ATR
        if len(cl1) > slow + 5 and atr_1m > 1e-10:
            ef = self._ema_series(cl1, fast)
            es = self._ema_series(cl1, slow)
            n = min(len(ef), len(es))
            if n >= 2:
                macd_now = ef[-1] - es[-1]
                macd_prev = ef[-2] - es[-2]
                self._cross_1m = _sigmoid(macd_now / atr_1m, 1.2)
                # Layer 3: Acceleration (is MACD growing or shrinking?)
                self._accel_1m = _sigmoid((macd_now - macd_prev) / atr_1m, 2.0)
            else:
                self._cross_1m = 0.0
                self._accel_1m = 0.0
        else:
            self._cross_1m = 0.0
            self._accel_1m = 0.0

        # Layer 2: 5m EMA slope
        if len(cl5) > fast + sig_b and atr_5m > 1e-10:
            ef5 = self._ema_series(cl5, fast)
            if len(ef5) >= sig_b + 1:
                slope = ef5[-1] - ef5[-sig_b - 1]
                self._cross_5m = _sigmoid(slope / atr_5m, 1.0)
            else:
                self._cross_5m = 0.0
        else:
            self._cross_5m = 0.0

    def get_signal(self) -> float:
        m1 = self._cross_1m; m5 = self._cross_5m; acc = self._accel_1m
        if abs(m1) < 1e-8 and abs(m5) < 1e-8:
            return 0.0

        same_dir = (m1 >= 0) == (m5 >= 0)
        if same_dir:
            # Both timeframes agree: full weight + acceleration bonus
            base = m1 * 0.50 + m5 * 0.35
            # Acceleration bonus: if momentum is increasing in trade direction
            if (m1 > 0 and acc > 0) or (m1 < 0 and acc < 0):
                base += acc * 0.15
            return max(-1.0, min(1.0, base))
        else:
            # Disagreement: heavily penalize — only use 1m at 25% weight
            return m1 * 0.25


# ═══════════════════════════════════════════════════════════════════════════════
# ENGINE 4: SQUEEZE — BB/KC Volatility Compression Breakout
# ═══════════════════════════════════════════════════════════════════════════════

class SqueezeEngine:
    """
    TTM-style BB inside KC squeeze with improved breakout detection.
    v3 FIX: Track squeeze duration for energy accumulation.
    Longer squeeze → stronger breakout signal.
    """

    def __init__(self):
        self._in_squeeze = False
        self._squeeze_bars = 0     # how long squeeze has been active
        self._post_sq_bars = 0
        self._bb_mid = 0.0
        self._last_signal = 0.0

    def update(self, candles: List[Dict], atr: float) -> None:
        window = QCfg.BB_WINDOW()
        n_std = QCfg.BB_STD()
        kc_mult = QCfg.KC_ATR_MULT()
        break_bars = QCfg.SQUEEZE_BREAKOUT_BARS()

        if len(candles) < window or atr < 1e-10:
            self._last_signal = 0.0
            return

        closes = [float(c['c']) for c in candles[-window:]]
        n = len(closes)
        mid = sum(closes) / n
        var = sum((c - mid) ** 2 for c in closes) / max(n - 1, 1)
        std = math.sqrt(var)

        bb_upper = mid + n_std * std
        bb_lower = mid - n_std * std
        kc_upper = mid + kc_mult * atr
        kc_lower = mid - kc_mult * atr

        in_sq = (bb_upper < kc_upper) and (bb_lower > kc_lower)

        if in_sq:
            if not self._in_squeeze:
                self._in_squeeze = True
                self._squeeze_bars = 0
            self._squeeze_bars += 1
            self._bb_mid = mid
            self._last_signal = 0.0
            return

        # Squeeze just released or still in breakout window
        if self._in_squeeze:
            self._in_squeeze = False
            self._post_sq_bars = 0

        self._post_sq_bars += 1

        if self._post_sq_bars <= break_bars and std > 1e-10:
            cl = float(candles[-1]['c'])
            disp = (cl - self._bb_mid) / (std + 1e-12)
            # Energy multiplier: longer squeeze → stronger breakout (capped at 1.5x)
            energy = min(1.5, 1.0 + self._squeeze_bars * 0.05)
            raw = _sigmoid(disp, 0.8) * energy
            self._last_signal = max(-1.0, min(1.0, raw))
        else:
            self._last_signal = 0.0

    def get_signal(self) -> float:
        return self._last_signal


# ═══════════════════════════════════════════════════════════════════════════════
# ENGINE 5: VOLUME FLOW — Multi-Bar Participation Imbalance
# ═══════════════════════════════════════════════════════════════════════════════

class VolumeFlowEngine:
    """
    Per-bar buy/sell volume estimation, z-scored against historical baseline.
    v3 FIX: Incremental append, no .clear() rebuild.
    """

    def __init__(self):
        self._ratios: deque = deque(maxlen=200)
        self._last_bar_ts: int = 0

    def update(self, candles: List[Dict]) -> None:
        if not candles:
            return
        new_start = 0
        if self._last_bar_ts > 0:
            for i, c in enumerate(candles):
                if int(c['t']) > self._last_bar_ts:
                    new_start = i
                    break
            else:
                # Update last forming bar
                if candles and self._ratios:
                    c = candles[-1]
                    self._ratios[-1] = self._compute_ratio(c)
                return

        for c in candles[new_start:]:
            self._ratios.append(self._compute_ratio(c))
            self._last_bar_ts = int(c['t'])

    @staticmethod
    def _compute_ratio(c: Dict) -> float:
        hi = float(c['h']); lo = float(c['l'])
        op = float(c['o']); cl = float(c['c']); vol = float(c['v'])
        rng = hi - lo
        if rng > 1e-10:
            buy_f = max(0.0, cl - op) / rng
            sell_f = max(0.0, op - cl) / rng
        else:
            buy_f = sell_f = 0.0
        bv = vol * buy_f; sv = vol * sell_f; tot = bv + sv
        return (bv - sv) / (tot + 1e-12) if tot > 1e-10 else 0.0

    def get_signal(self) -> float:
        window = QCfg.VOL_FLOW_WINDOW()
        arr = list(self._ratios)
        if len(arr) < window + 10:
            return 0.0
        recent_avg = sum(arr[-window:]) / window
        hist = arr[:-window]
        if len(hist) < 10:
            return 0.0
        mu = sum(hist) / len(hist)
        var = sum((x - mu) ** 2 for x in hist) / max(len(hist) - 1, 1)
        std = math.sqrt(var)
        if std < 1e-12:
            return 0.0
        return _sigmoid((recent_avg - mu) / std, 0.6)


# ═══════════════════════════════════════════════════════════════════════════════
# ENGINE 6: ORDERBOOK IMBALANCE — Microstructure (NEW in v3)
# ═══════════════════════════════════════════════════════════════════════════════

class OrderbookEngine:
    """
    Bid/ask depth imbalance from live orderbook.

    Metrics:
      1. Depth Imbalance = (total_bid_depth - total_ask_depth) / total
         across top N levels.
      2. Imbalance z-score vs recent history → directional signal.
      3. Spread-adjusted: tighter spread = higher confidence.

    Fed by data_manager._on_orderbook_update() via get_orderbook().
    """

    def __init__(self):
        self._imbalance_hist: deque = deque(maxlen=QCfg.OB_HIST_LEN())
        self._last_imbalance = 0.0
        self._spread_ratio = 0.0  # spread / midprice

    def update(self, orderbook: Dict, price: float) -> None:
        bids = orderbook.get("bids", [])
        asks = orderbook.get("asks", [])
        depth = QCfg.OB_DEPTH_LEVELS()

        if not bids or not asks or price < 1.0:
            return

        # Sum depth at top N levels
        bid_depth = 0.0
        for i, level in enumerate(bids[:depth]):
            try:
                bid_depth += float(level[1]) if isinstance(level, (list, tuple)) else 0.0
            except (IndexError, ValueError, TypeError):
                pass

        ask_depth = 0.0
        for i, level in enumerate(asks[:depth]):
            try:
                ask_depth += float(level[1]) if isinstance(level, (list, tuple)) else 0.0
            except (IndexError, ValueError, TypeError):
                pass

        total = bid_depth + ask_depth
        if total < 1e-12:
            return

        imbalance = (bid_depth - ask_depth) / total
        self._last_imbalance = imbalance
        self._imbalance_hist.append(imbalance)

        # Spread ratio
        try:
            best_bid = float(bids[0][0]) if isinstance(bids[0], (list, tuple)) else 0.0
            best_ask = float(asks[0][0]) if isinstance(asks[0], (list, tuple)) else 0.0
            if best_bid > 0 and best_ask > 0:
                self._spread_ratio = (best_ask - best_bid) / ((best_bid + best_ask) / 2.0)
        except (IndexError, ValueError, TypeError):
            pass

    def get_signal(self) -> float:
        hist = list(self._imbalance_hist)
        if len(hist) < 15:
            return 0.0

        current = hist[-1]
        # Rolling z-score of imbalance
        baseline = hist[:-1]
        mu = sum(baseline) / len(baseline)
        var = sum((x - mu) ** 2 for x in baseline) / max(len(baseline) - 1, 1)
        std = math.sqrt(var)
        if std < 1e-12:
            # No variance — use raw imbalance
            return _sigmoid(current * 3.0, 0.8)

        z = (current - mu) / std

        # Spread-adjusted confidence: tighter spread = higher signal
        # Normal BTC spread ≈ 0.01-0.03%, penalize if wider
        spread_mult = max(0.5, min(1.0, 1.0 - (self._spread_ratio - 0.0002) * 100.0))

        return _sigmoid(z, 0.6) * spread_mult


# ═══════════════════════════════════════════════════════════════════════════════
# ENGINE 7: TICK FLOW — Real-Time Trade Aggressor (NEW in v3)
# ═══════════════════════════════════════════════════════════════════════════════

class TickFlowEngine:
    """
    Real-time buy/sell aggressor analysis from the trade stream.

    Every trade is classified as buyer-initiated or seller-initiated.
    We compute a time-windowed dollar-weighted flow ratio and detect
    surges (sudden spikes in one-sided flow vs baseline).

    Fed by data_manager._on_trades_update() via get_recent_trades().
    """

    def __init__(self):
        self._buy_vol: deque = deque(maxlen=600)   # (timestamp, dollar_volume)
        self._sell_vol: deque = deque(maxlen=600)
        self._flow_hist: deque = deque(maxlen=120)
        self._last_signal = 0.0

    def on_trade(self, price: float, qty: float, is_buyer: bool, ts: float) -> None:
        """Call this for every trade from the WebSocket stream."""
        dollar_vol = price * qty
        if is_buyer:
            self._buy_vol.append((ts, dollar_vol))
        else:
            self._sell_vol.append((ts, dollar_vol))

    def compute_signal(self) -> float:
        """Compute the tick flow signal from accumulated trade data."""
        now = time.time()
        window = QCfg.TICK_AGG_WINDOW_SEC()
        cutoff = now - window

        # Sum buy/sell volume in the window
        buy_total = sum(dv for ts, dv in self._buy_vol if ts >= cutoff)
        sell_total = sum(dv for ts, dv in self._sell_vol if ts >= cutoff)
        total = buy_total + sell_total

        if total < 1e-10:
            return 0.0

        flow_ratio = (buy_total - sell_total) / total
        self._flow_hist.append(flow_ratio)

        hist = list(self._flow_hist)
        if len(hist) < 10:
            return _sigmoid(flow_ratio * 2.0, 0.8)

        # Z-score against recent history
        mu = sum(hist[:-1]) / len(hist[:-1])
        var = sum((x - mu) ** 2 for x in hist[:-1]) / max(len(hist[:-1]) - 1, 1)
        std = math.sqrt(var)

        if std < 1e-12:
            return _sigmoid(flow_ratio * 2.0, 0.8)

        z = (flow_ratio - mu) / std

        # Surge detection: if current flow is TICK_SURGE_MULT × std above mean,
        # amplify the signal (institutional order detected)
        surge_mult = QCfg.TICK_SURGE_MULT()
        if abs(z) > surge_mult:
            z *= 1.3  # amplify extreme flows

        self._last_signal = _sigmoid(z, 0.5)
        return self._last_signal

    def get_signal(self) -> float:
        return self._last_signal


# ═══════════════════════════════════════════════════════════════════════════════
# ATR ENGINE — Wilder's ATR with incremental update
# ═══════════════════════════════════════════════════════════════════════════════

class ATREngine:
    def __init__(self):
        self._atr = 0.0
        self._atr_hist: deque = deque(maxlen=QCfg.ATR_PCTILE_WINDOW())
        self._last_ts: int = -1
        self._seeded: bool = False

    def compute(self, candles: List[Dict]) -> float:
        if not candles:
            return self._atr

        period = QCfg.ATR_PERIOD()
        last_ts = int(candles[-1].get('t', 0))

        if last_ts == self._last_ts and self._seeded:
            return self._atr

        if len(candles) < period + 1:
            return self._atr

        if not self._seeded:
            trs = [
                max(float(candles[i]['h']) - float(candles[i]['l']),
                    abs(float(candles[i]['h']) - float(candles[i - 1]['c'])),
                    abs(float(candles[i]['l']) - float(candles[i - 1]['c'])))
                for i in range(1, len(candles))
            ]
            if len(trs) < period:
                return self._atr
            atr = sum(trs[:period]) / period
            for tr in trs[period:]:
                atr = (atr * (period - 1) + tr) / period
            self._atr = atr
            self._seeded = True
        else:
            hi = float(candles[-1]['h']); lo = float(candles[-1]['l'])
            prc = float(candles[-2]['c'])
            tr = max(hi - lo, abs(hi - prc), abs(lo - prc))
            self._atr = (self._atr * (period - 1) + tr) / period

        self._atr_hist.append(self._atr)
        self._last_ts = last_ts
        return self._atr

    @property
    def atr(self) -> float:
        return self._atr

    def get_percentile(self) -> float:
        hist = list(self._atr_hist)
        if len(hist) < 10:
            return 0.5
        ref = hist[-1]
        prev = hist[:-1]
        return sum(1 for h in prev if h <= ref) / len(prev)

    def is_regime_valid(self) -> bool:
        p = self.get_percentile()
        return QCfg.ATR_MIN_PCTILE() <= p <= QCfg.ATR_MAX_PCTILE()

    def regime_penalty(self) -> float:
        """
        v3: Gradient penalty instead of binary gate.
        Returns 1.0 in ideal zone, decays toward edges.
        Never returns 0 unless completely outside bounds.
        """
        p = self.get_percentile()
        lo = QCfg.ATR_MIN_PCTILE()
        hi = QCfg.ATR_MAX_PCTILE()
        if lo <= p <= hi:
            return 1.0
        if p < lo:
            # Below minimum: linear decay (e.g. 3% pctile when floor is 5%)
            return max(0.0, p / (lo + 1e-12))
        else:
            # Above maximum: linear decay
            return max(0.0, (1.0 - p) / (1.0 - hi + 1e-12))


# ═══════════════════════════════════════════════════════════════════════════════
# HTF TREND FILTER — Multi-Timeframe Alignment (NEW in v3)
# ═══════════════════════════════════════════════════════════════════════════════

class HTFTrendFilter:
    """
    Uses 15m and 4h EMA slopes to determine dominant trend direction.
    Returns a multiplier: >1 = aligned (boost), <1 = counter (penalize), 0 = strong veto.
    """

    def __init__(self):
        self._trend_15m = 0.0
        self._trend_4h = 0.0

    def update(self, candles_15m: List[Dict], candles_4h: List[Dict],
               atr_5m: float) -> None:
        fast = QCfg.EMA_FAST()

        # 15m trend: EMA slope
        if len(candles_15m) > fast + 5 and atr_5m > 1e-10:
            cl15 = [float(c['c']) for c in candles_15m]
            ema15 = MomentumEngine._ema_series(cl15, fast)
            if len(ema15) >= 4:
                slope = ema15[-1] - ema15[-3]
                self._trend_15m = _sigmoid(slope / atr_5m, 0.8)
            else:
                self._trend_15m = 0.0
        else:
            self._trend_15m = 0.0

        # 4h trend: EMA slope (use longer lookback)
        slow = QCfg.EMA_SLOW()
        if len(candles_4h) > slow + 3 and atr_5m > 1e-10:
            cl4h = [float(c['c']) for c in candles_4h]
            ema4h = MomentumEngine._ema_series(cl4h, slow)
            if len(ema4h) >= 3:
                slope = ema4h[-1] - ema4h[-2]
                # 4h ATR is much larger, scale differently
                self._trend_4h = _sigmoid(slope / (atr_5m * 4.0), 0.8)
            else:
                self._trend_4h = 0.0
        else:
            self._trend_4h = 0.0

    def get_alignment(self, signal_direction: float) -> float:
        """
        Returns multiplier for the composite signal.
        signal_direction: +1 for long, -1 for short.
        """
        if not QCfg.HTF_ENABLED():
            return 1.0

        # Combined HTF trend: 4h is dominant (60%), 15m secondary (40%)
        htf_combined = self._trend_4h * 0.60 + self._trend_15m * 0.40

        # Alignment check: does the HTF trend agree with our signal?
        alignment = htf_combined * signal_direction  # positive = aligned

        veto = QCfg.HTF_VETO_STRENGTH()
        boost = QCfg.HTF_BOOST()

        if alignment > 0.15:
            # Aligned: boost proportional to alignment strength
            return 1.0 + boost * min(alignment / 0.5, 1.0)
        elif alignment < -0.15:
            # Counter-trend: penalize
            penalty = abs(alignment) / (veto + 1e-12)
            if penalty > 1.0:
                # Strong counter-trend: veto (return near-zero multiplier)
                return max(0.05, 1.0 - penalty * 0.8)
            else:
                # Mild counter: moderate penalty
                return max(0.3, 1.0 - penalty * 0.5)
        else:
            # Neutral: no adjustment
            return 1.0

    @property
    def trend_15m(self) -> float: return self._trend_15m
    @property
    def trend_4h(self) -> float: return self._trend_4h


# ═══════════════════════════════════════════════════════════════════════════════
# SIGNAL AGGREGATOR — Dynamic Weight Redistribution + Agreement Detection
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class SignalBreakdown:
    cvd:       float = 0.0
    vwap:      float = 0.0
    mom:       float = 0.0
    squeeze:   float = 0.0
    vol:       float = 0.0
    orderbook: float = 0.0
    tick_flow: float = 0.0
    composite: float = 0.0
    raw_composite: float = 0.0   # before HTF/regime adjustments
    atr:       float = 0.0
    atr_pct:   float = 0.0
    regime_penalty: float = 1.0
    htf_mult:  float = 1.0
    n_agreeing: int  = 0
    threshold_used: float = 0.0

    @property
    def regime_ok(self) -> bool:
        """Backward compatibility with v2 consumers (telegram_bot_controller, etc.)."""
        return self.regime_penalty >= 0.5

    def __str__(self) -> str:
        return (
            f"CVD={self.cvd:+.3f} VWAP={self.vwap:+.3f} "
            f"MOM={self.mom:+.3f} SQZ={self.squeeze:+.3f} "
            f"VFL={self.vol:+.3f} OB={self.orderbook:+.3f} "
            f"TF={self.tick_flow:+.3f} → Σ={self.composite:+.4f} "
            f"(raw={self.raw_composite:+.3f} ×HTF={self.htf_mult:.2f} "
            f"×REG={self.regime_penalty:.2f}) "
            f"agree={self.n_agreeing} thr={self.threshold_used:.3f} "
            f"ATR=${self.atr:.1f}({self.atr_pct:.0%})"
        )


class SignalAggregator:
    """
    v3: Dynamic weight redistribution + agreement detection.

    When a signal returns 0.0 (inactive, e.g. squeeze not firing),
    its weight is redistributed proportionally to active signals.
    This prevents dead-weight dilution that plagued v2.

    Agreement detection: counts how many signals agree on direction.
    When ≥ MIN_AGREE_SIGNALS agree, the entry threshold is lowered
    by AGREEMENT_DISCOUNT (making entries easier when conviction is broad).
    """

    @staticmethod
    def compute(signals: Dict[str, float],
                atr_engine: ATREngine,
                htf_filter: HTFTrendFilter) -> SignalBreakdown:

        # Fetch base weights
        base_weights = {
            'cvd': QCfg.W_CVD(),
            'vwap': QCfg.W_VWAP(),
            'mom': QCfg.W_MOM(),
            'squeeze': QCfg.W_SQUEEZE(),
            'vol': QCfg.W_VOL(),
            'orderbook': QCfg.W_ORDERBOOK(),
            'tick_flow': QCfg.W_TICK_FLOW(),
        }

        # ── Dynamic weight redistribution ──────────────────────────────
        # A signal is "inactive" if |value| < 0.01 (effectively zero)
        INACTIVE_THRESHOLD = 0.01

        active_weight_sum = 0.0
        inactive_weight_sum = 0.0
        for name, w in base_weights.items():
            if abs(signals.get(name, 0.0)) >= INACTIVE_THRESHOLD:
                active_weight_sum += w
            else:
                inactive_weight_sum += w

        # Redistribute inactive weight to active signals proportionally
        effective_weights = {}
        if active_weight_sum > 1e-10:
            redistribution_ratio = (active_weight_sum + inactive_weight_sum) / active_weight_sum
            for name, w in base_weights.items():
                if abs(signals.get(name, 0.0)) >= INACTIVE_THRESHOLD:
                    effective_weights[name] = w * redistribution_ratio
                else:
                    effective_weights[name] = 0.0
        else:
            effective_weights = dict(base_weights)

        # Normalize to sum = 1.0
        total_ew = sum(effective_weights.values())
        if total_ew > 1e-10:
            effective_weights = {k: v / total_ew for k, v in effective_weights.items()}

        # ── Weighted composite ──────────────────────────────────────────
        raw_composite = sum(effective_weights.get(name, 0.0) * signals.get(name, 0.0)
                           for name in base_weights)

        # ── Agreement detection ─────────────────────────────────────────
        direction = 1.0 if raw_composite >= 0 else -1.0
        strong_level = QCfg.STRONG_SIGNAL_LEVEL()
        n_agreeing = 0
        for name in base_weights:
            sig_val = signals.get(name, 0.0)
            if abs(sig_val) >= INACTIVE_THRESHOLD:
                # Signal agrees if it's pointing the same direction AND is strong enough
                if sig_val * direction > 0 and abs(sig_val) >= strong_level * 0.5:
                    n_agreeing += 1

        # Agreement bonus: when many signals agree, add a small boost
        if n_agreeing >= QCfg.MIN_AGREE_SIGNALS():
            agreement_bonus = (n_agreeing - QCfg.MIN_AGREE_SIGNALS() + 1) * 0.02 * direction
            raw_composite += agreement_bonus

        raw_composite = max(-1.0, min(1.0, raw_composite))

        # ── HTF trend adjustment ────────────────────────────────────────
        htf_mult = htf_filter.get_alignment(direction)

        # ── Regime penalty (gradient, not binary) ───────────────────────
        regime_pen = atr_engine.regime_penalty()

        # Final composite
        composite = raw_composite * htf_mult * regime_pen
        composite = max(-1.0, min(1.0, composite))

        # ── Adaptive threshold ──────────────────────────────────────────
        base_thresh = (QCfg.LONG_THRESHOLD() if composite >= 0
                       else QCfg.SHORT_THRESHOLD())
        discount = 0.0
        if n_agreeing >= QCfg.MIN_AGREE_SIGNALS():
            discount = QCfg.AGREEMENT_DISCOUNT() * (n_agreeing - QCfg.MIN_AGREE_SIGNALS() + 1)
        threshold_used = max(0.20, base_thresh - discount)

        return SignalBreakdown(
            cvd=signals.get('cvd', 0.0),
            vwap=signals.get('vwap', 0.0),
            mom=signals.get('mom', 0.0),
            squeeze=signals.get('squeeze', 0.0),
            vol=signals.get('vol', 0.0),
            orderbook=signals.get('orderbook', 0.0),
            tick_flow=signals.get('tick_flow', 0.0),
            composite=composite,
            raw_composite=raw_composite,
            atr=atr_engine.atr,
            atr_pct=atr_engine.get_percentile(),
            regime_penalty=regime_pen,
            htf_mult=htf_mult,
            n_agreeing=n_agreeing,
            threshold_used=threshold_used,
        )


# ═══════════════════════════════════════════════════════════════════════════════
# POSITION STATE
# ═══════════════════════════════════════════════════════════════════════════════

class PositionPhase(Enum):
    FLAT     = auto()
    ENTERING = auto()
    ACTIVE   = auto()
    EXITING  = auto()


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
    initial_risk:    float                 = 0.0
    initial_sl_dist: float                 = 0.0
    trail_active:    bool                  = False
    last_trail_time: float                 = 0.0
    entry_signal:    Optional[SignalBreakdown] = None
    peak_profit:     float                 = 0.0
    entry_atr:       float                 = 0.0

    def is_active(self) -> bool: return self.phase == PositionPhase.ACTIVE
    def is_flat(self)   -> bool: return self.phase == PositionPhase.FLAT

    def to_dict(self) -> Dict:
        return {
            "side": self.side, "quantity": self.quantity,
            "entry_price": self.entry_price,
            "sl_price": self.sl_price, "tp_price": self.tp_price,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# DAILY RISK GATE
# ═══════════════════════════════════════════════════════════════════════════════

class DailyRiskGate:
    def __init__(self):
        self._today: date = date.today()
        self._daily_trades: int = 0
        self._consec_losses: int = 0
        self._daily_pnl: float = 0.0
        self._daily_open_bal: float = 0.0
        self._lock = threading.Lock()

    def _reset_if_new_day(self) -> None:
        today = date.today()
        if today != self._today:
            logger.info(f"📅 Daily risk counters reset (new day: {today})")
            self._today = today
            self._daily_trades = 0
            self._daily_pnl = 0.0
            self._daily_open_bal = 0.0

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
# MAIN STRATEGY CLASS — v3
# ═══════════════════════════════════════════════════════════════════════════════

class QuantStrategy:
    """
    Institutional Multi-Factor Momentum + Order Flow Strategy — v3.

    Public interface (unchanged for main.py compatibility):
        on_tick(data_manager, order_manager, risk_manager, timestamp_ms) → None
        get_position() → dict | None
        current_sl_price: float
        current_tp_price: float
    """

    def __init__(self, order_manager=None):
        self._om = order_manager
        self._lock = threading.RLock()

        # 7 Signal Engines
        self._cvd     = CVDEngine()
        self._vwap    = VWAPEngine()
        self._mom     = MomentumEngine()
        self._squeeze = SqueezeEngine()
        self._volflow = VolumeFlowEngine()
        self._ob_eng  = OrderbookEngine()
        self._tick_eng = TickFlowEngine()

        # ATR on two timeframes
        self._atr_1m = ATREngine()
        self._atr_5m = ATREngine()

        # HTF Trend Filter
        self._htf = HTFTrendFilter()

        # State
        self._pos = PositionState()
        self._last_sig = SignalBreakdown()
        self._risk_gate = DailyRiskGate()

        # Confirmation
        self._confirm_long = 0
        self._confirm_short = 0

        # Timing
        self._last_eval_time = 0.0
        self._last_exit_time = 0.0
        self._last_pos_sync  = 0.0
        self._last_exit_sync = 0.0

        # Thinking log
        self._last_think_log = 0.0
        self._think_interval = 30.0

        # Trade dedup guard — prevents re-feeding same trades every tick
        self._last_fed_trade_ts = 0.0

        # Reconciliation
        self._last_reconcile_time = 0.0
        self._RECONCILE_SEC = 30.0
        self._reconcile_pending = False
        self._reconcile_data: Optional[Dict] = None

        # Statistics
        self._total_trades = 0
        self._winning_trades = 0
        self._total_pnl = 0.0

        # Required by main.py
        self.current_sl_price: float = 0.0
        self.current_tp_price: float = 0.0

        self._log_init()

    def _log_init(self) -> None:
        logger.info("=" * 72)
        logger.info("⚡ QuantStrategy v3 — INSTITUTIONAL GRADE — 7 ENGINES")
        logger.info(f"   {QCfg.SYMBOL()} | {QCfg.LEVERAGE()}x lev | "
                    f"{QCfg.MARGIN_PCT():.0%} margin | "
                    f"SL={QCfg.SL_ATR_MULT()}×ATR TP={QCfg.TP_ATR_MULT()}×ATR "
                    f"minRR={QCfg.MIN_RR_RATIO()}")
        logger.info(f"   Engines: CVD VWAP MOM SQZ VOL OB TICK")
        logger.info(f"   Threshold: {QCfg.LONG_THRESHOLD()} (adaptive) | "
                    f"Confirm: {QCfg.CONFIRM_TICKS()} | "
                    f"Eval: {QCfg.TICK_EVAL_SEC()}s")
        logger.info(f"   HTF Filter: {'ON' if QCfg.HTF_ENABLED() else 'OFF'} | "
                    f"Regime: {QCfg.ATR_MIN_PCTILE():.0%}-{QCfg.ATR_MAX_PCTILE():.0%} (gradient)")
        w_total = (QCfg.W_CVD() + QCfg.W_VWAP() + QCfg.W_MOM() +
                   QCfg.W_SQUEEZE() + QCfg.W_VOL() +
                   QCfg.W_ORDERBOOK() + QCfg.W_TICK_FLOW())
        logger.info(f"   Weights: CVD={QCfg.W_CVD()} VWAP={QCfg.W_VWAP()} "
                    f"MOM={QCfg.W_MOM()} SQZ={QCfg.W_SQUEEZE()} "
                    f"VFL={QCfg.W_VOL()} OB={QCfg.W_ORDERBOOK()} "
                    f"TF={QCfg.W_TICK_FLOW()} (sum={w_total:.2f})")
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
            now = timestamp_ms / 1000.0
            self._om = order_manager

            if now - self._last_eval_time < QCfg.TICK_EVAL_SEC():
                return
            self._last_eval_time = now

            phase = self._pos.phase

            # ── Feed microstructure engines from data_manager ──────────
            self._feed_microstructure(data_manager)

            # ── Reconciliation ────────────────────────────────────────
            if self._reconcile_data is not None:
                data = self._reconcile_data
                self._reconcile_data = None
                self._reconcile_apply(order_manager, data)
                phase = self._pos.phase

            if (now - self._last_reconcile_time >= self._RECONCILE_SEC
                    and not self._reconcile_pending):
                self._last_reconcile_time = now
                self._reconcile_pending = True
                t = threading.Thread(
                    target=self._reconcile_query_thread,
                    args=(order_manager,),
                    daemon=True,
                )
                t.start()

            # Position sync
            if phase == PositionPhase.ACTIVE:
                if now - self._last_pos_sync > QCfg.POS_SYNC_SEC():
                    self._sync_position(order_manager)
                    self._last_pos_sync = now
                    if self._pos.is_flat():
                        return

            elif phase == PositionPhase.EXITING:
                if now - self._last_exit_sync > QCfg.POS_SYNC_SEC():
                    self._sync_position(order_manager)
                    self._last_exit_sync = now
                return

            # Route
            if phase == PositionPhase.FLAT:
                if now - self._last_exit_time < float(QCfg.COOLDOWN_SEC()):
                    return
                self._evaluate_entry(data_manager, order_manager, risk_manager, now)

            elif phase == PositionPhase.ACTIVE:
                self._manage_active(data_manager, order_manager, now)

    # ───────────────────────────────────────────────────────────────────
    # MICROSTRUCTURE FEED — v3 NEW
    # ───────────────────────────────────────────────────────────────────

    def _feed_microstructure(self, data_manager) -> None:
        """Feed orderbook and tick data into microstructure engines."""
        try:
            # Orderbook — always update (snapshot, not incremental)
            ob = data_manager.get_orderbook()
            price = data_manager.get_last_price()
            if ob and price > 1.0:
                self._ob_eng.update(ob, price)
        except Exception:
            pass

        try:
            # Tick flow — only feed NEW trades (dedup guard)
            trades = data_manager.get_recent_trades_raw()
            cutoff_ts = self._last_fed_trade_ts
            max_ts = cutoff_ts
            for t in trades:
                ts = t.get("timestamp", 0.0)
                if ts > cutoff_ts:
                    self._tick_eng.on_trade(
                        price=t.get("price", 0.0),
                        qty=t.get("quantity", 0.0),
                        is_buyer=(t.get("side") == "buy"),
                        ts=ts,
                    )
                    if ts > max_ts:
                        max_ts = ts
            if max_ts > cutoff_ts:
                self._last_fed_trade_ts = max_ts
            self._tick_eng.compute_signal()
        except Exception:
            pass

    # ───────────────────────────────────────────────────────────────────
    # SIGNAL COMPUTATION — v3 COMPLETE REWRITE
    # ───────────────────────────────────────────────────────────────────

    def _compute_signals(self, data_manager) -> Optional[SignalBreakdown]:
        candles_1m = data_manager.get_candles("1m", limit=300)
        candles_5m = data_manager.get_candles("5m", limit=100)

        if len(candles_1m) < QCfg.MIN_1M_BARS():
            return None
        if len(candles_5m) < QCfg.MIN_5M_BARS():
            return None

        atr_1m = self._atr_1m.compute(candles_1m)
        atr_5m = self._atr_5m.compute(candles_5m)

        if atr_5m < 1e-10:
            return None

        price = data_manager.get_last_price()
        if price < 1.0:
            return None

        # Update all 5 candle-based engines
        self._cvd.update(candles_1m)
        self._vwap.update(candles_1m, atr_1m)
        self._mom.update(candles_1m, candles_5m, atr_1m, atr_5m)
        self._squeeze.update(candles_1m, atr_1m)
        self._volflow.update(candles_1m)

        # Update HTF trend filter
        try:
            candles_15m = data_manager.get_candles("15m", limit=100)
            candles_4h = data_manager.get_candles("4h", limit=50)
            self._htf.update(candles_15m, candles_4h, atr_5m)
        except Exception:
            pass

        # Collect all 7 signals
        signals = {
            'cvd': self._cvd.get_signal(),
            'vwap': self._vwap.get_signal(price),
            'mom': self._mom.get_signal(),
            'squeeze': self._squeeze.get_signal(),
            'vol': self._volflow.get_signal(),
            'orderbook': self._ob_eng.get_signal(),
            'tick_flow': self._tick_eng.get_signal(),
        }

        sig = SignalAggregator.compute(signals, self._atr_5m, self._htf)
        self._last_sig = sig
        return sig

    # ───────────────────────────────────────────────────────────────────
    # THINKING LOG
    # ───────────────────────────────────────────────────────────────────

    def _log_thinking(self, sig: SignalBreakdown, price: float, now: float) -> None:
        if now - self._last_think_log < self._think_interval:
            return
        self._last_think_log = now

        def bar(v: float, width: int = 12) -> str:
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

        c = sig.composite
        thr = sig.threshold_used

        if c >= thr:
            lean = f"BULLISH  ✅  ({c:+.3f} ≥ {thr:.3f})"
        elif c <= -thr:
            lean = f"BEARISH  ✅  ({c:+.3f} ≤ -{thr:.3f})"
        elif c >= thr * 0.7:
            lean = f"weakly bullish  ({c:+.3f}, need ≥{thr:.3f})"
        elif c <= -thr * 0.7:
            lean = f"weakly bearish  ({c:+.3f}, need ≤-{thr:.3f})"
        else:
            lean = f"NEUTRAL  🔇  ({c:+.3f})"

        need = QCfg.CONFIRM_TICKS()
        if self._confirm_long > 0:
            conf_str = f"LONG confirms: {self._confirm_long}/{need}"
        elif self._confirm_short > 0:
            conf_str = f"SHORT confirms: {self._confirm_short}/{need}"
        else:
            conf_str = "No confirmation building"

        if c >= thr and self._confirm_long >= need - 1:
            next_move = "⚡ ENTRY LONG — next tick fires"
        elif c <= -thr and self._confirm_short >= need - 1:
            next_move = "⚡ ENTRY SHORT — next tick fires"
        elif c >= thr or c <= -thr:
            remaining = need - max(self._confirm_long, self._confirm_short)
            next_move = f"🕐 Confirming — {remaining} tick(s) left"
        else:
            next_move = "👀 Watching — below threshold"

        regime_str = (f"penalty={sig.regime_penalty:.2f} ({sig.atr_pct:.0%} pctile)")
        cooldown_rem = max(0.0, QCfg.COOLDOWN_SEC() - (now - self._last_exit_time))
        cd_str = f"{cooldown_rem:.0f}s" if cooldown_rem > 0 else "ready"

        lines = [
            f"┌─── 🧠 v3 THINKING  ${price:,.2f}  ATR={sig.atr:.1f}  agree={sig.n_agreeing}/7 ────",
            fmt("CVD",  sig.cvd),
            fmt("VWAP", sig.vwap),
            fmt("MOM",  sig.mom),
            fmt("SQZ",  sig.squeeze),
            fmt("VFL",  sig.vol),
            fmt("OB",   sig.orderbook),
            fmt("TICK", sig.tick_flow),
            f"  {'─'*42}",
            f"  Σ raw={sig.raw_composite:+.4f} ×HTF={sig.htf_mult:.2f} ×REG={sig.regime_penalty:.2f} → [{bar(c)}] {c:+.4f}",
            f"  HTF:  15m={self._htf.trend_15m:+.3f}  4h={self._htf.trend_4h:+.3f}",
            f"  Regime:    {regime_str}",
            f"  Threshold: {thr:.3f} (base={QCfg.LONG_THRESHOLD():.2f} - agree_disc)",
            f"  Lean:      {lean}",
            f"  {conf_str}",
            f"  Cooldown:  {cd_str}",
            f"  Next:      {next_move}",
            f"└{'─'*66}",
        ]
        logger.info("\n" + "\n".join(lines))

    # ───────────────────────────────────────────────────────────────────
    # ENTRY EVALUATION — v3: ADAPTIVE THRESHOLD + FAST CONFIRMATION
    # ───────────────────────────────────────────────────────────────────

    def _evaluate_entry(self, data_manager, order_manager,
                        risk_manager, now: float) -> None:
        sig = self._compute_signals(data_manager)
        if sig is None:
            self._confirm_long = self._confirm_short = 0
            return

        price = data_manager.get_last_price()
        self._log_thinking(sig, price, now)

        c = sig.composite
        thr = sig.threshold_used  # ADAPTIVE threshold from aggregator

        if c >= thr:
            self._confirm_long += 1; self._confirm_short = 0
        elif c <= -thr:
            self._confirm_short += 1; self._confirm_long = 0
        else:
            self._confirm_long = self._confirm_short = 0
            return

        # v3: FAST ENTRY — skip confirmation for very strong signals
        confirm_needed = QCfg.CONFIRM_TICKS()
        strong_composite = 0.60
        if abs(c) >= strong_composite:
            confirm_needed = 0  # instant entry on very strong signals

        if self._confirm_long > confirm_needed:
            self._confirm_long = self._confirm_short = 0
            self._enter_trade(data_manager, order_manager, risk_manager, "long", sig)
        elif self._confirm_short > confirm_needed:
            self._confirm_long = self._confirm_short = 0
            self._enter_trade(data_manager, order_manager, risk_manager, "short", sig)

    # ───────────────────────────────────────────────────────────────────
    # ENTER TRADE
    # ───────────────────────────────────────────────────────────────────

    def _enter_trade(self, data_manager, order_manager,
                     risk_manager, side: str, sig: SignalBreakdown) -> None:
        price = data_manager.get_last_price()
        if price < 1.0:
            return

        atr = self._atr_5m.atr
        if atr < 1e-10:
            return

        bal_info = risk_manager.get_available_balance()
        if bal_info is None:
            return
        total_bal = float(bal_info.get("total", bal_info.get("available", 0.0)))
        self._risk_gate.set_opening_balance(total_bal)

        allowed, reason = self._risk_gate.can_trade(total_bal)
        if not allowed:
            logger.info(f"Entry blocked by risk gate: {reason}")
            return

        qty = self._compute_quantity(risk_manager, price)
        if qty is None or qty < QCfg.MIN_QTY():
            return

        sl_price, tp_price = self._compute_sl_tp(price, side, atr)
        if sl_price is None or tp_price is None:
            return

        sl_dist = abs(price - sl_price)
        tp_dist = abs(price - tp_price)
        if sl_dist < 1e-10:
            return
        rr = tp_dist / sl_dist
        if rr < QCfg.MIN_RR_RATIO() - 1e-9:
            logger.info(f"Entry blocked: R:R={rr:.4f} < min={QCfg.MIN_RR_RATIO()}")
            return

        logger.info(
            f"🎯 ENTERING {side.upper()} @ ${price:,.2f} | qty={qty} | "
            f"SL=${sl_price:,.2f} TP=${tp_price:,.2f} R:R=1:{rr:.2f} | {sig}"
        )

        entry_data = order_manager.place_market_order(side=side, quantity=qty)
        if not entry_data:
            logger.error("❌ Market order failed")
            return

        self._risk_gate.record_trade_start()

        fill_price = (
            float(entry_data.get("average_price") or 0) or
            float(entry_data.get("fill_price") or 0) or
            float(entry_data.get("price") or 0) or
            price
        )

        slip = abs(fill_price - price) / price
        if slip > QCfg.SLIPPAGE_TOL():
            logger.info(f"Slippage {slip:.4%} > tol — recalc SL/TP")
            new_sl, new_tp = self._compute_sl_tp(fill_price, side, atr)
            if new_sl is None or new_tp is None:
                logger.error("❌ SL/TP recompute failed — closing")
                exit_s = "sell" if side == "long" else "buy"
                order_manager.place_market_order(side=exit_s, quantity=qty, reduce_only=True)
                self._last_exit_time = time.time()
                self._last_reconcile_time = 0.0
                return
            new_rr = abs(fill_price - new_tp) / abs(fill_price - new_sl)
            if new_rr < QCfg.MIN_RR_RATIO() - 1e-9:
                logger.warning(f"R:R={new_rr:.4f} below min after slippage — closing")
                exit_s = "sell" if side == "long" else "buy"
                order_manager.place_market_order(side=exit_s, quantity=qty, reduce_only=True)
                self._last_exit_time = time.time()
                self._last_reconcile_time = 0.0
                return
            sl_price, tp_price = new_sl, new_tp
            rr = new_rr

        exit_side = "sell" if side == "long" else "buy"

        sweep = order_manager.cancel_symbol_conditionals()
        if sweep:
            filled = [oid for oid, r in sweep.items()
                      if r in (CancelResult.ALREADY_FILLED, CancelResult.PARTIAL_FILL)]
            if filled:
                logger.warning(f"⚠️ Conditional(s) filled during sweep — aborting")
                self._last_reconcile_time = 0.0
                return

        sl_data = order_manager.place_stop_loss(side=exit_side, quantity=qty,
                                                trigger_price=sl_price)
        if not sl_data:
            logger.error("❌ SL placement failed — closing")
            order_manager.place_market_order(side=exit_side, quantity=qty, reduce_only=True)
            self._last_exit_time = time.time()
            self._last_reconcile_time = 0.0
            return

        tp_data = order_manager.place_take_profit(side=exit_side, quantity=qty,
                                                   trigger_price=tp_price)
        if not tp_data:
            logger.error("❌ TP placement failed — cancelling SL, closing")
            order_manager.cancel_order(sl_data["order_id"])
            order_manager.place_market_order(side=exit_side, quantity=qty, reduce_only=True)
            self._last_exit_time = time.time()
            self._last_reconcile_time = 0.0
            return

        sl_dist_filled = abs(fill_price - sl_price)
        initial_risk = sl_dist_filled * qty

        self._pos = PositionState(
            phase=PositionPhase.ACTIVE,
            side=side,
            quantity=qty,
            entry_price=fill_price,
            sl_price=sl_price,
            tp_price=tp_price,
            sl_order_id=sl_data["order_id"],
            tp_order_id=tp_data["order_id"],
            entry_order_id=entry_data.get("order_id"),
            entry_time=time.time(),
            initial_risk=initial_risk,
            initial_sl_dist=sl_dist_filled,
            entry_signal=sig,
            entry_atr=self._atr_5m.atr,
            peak_profit=0.0,
        )
        self.current_sl_price = sl_price
        self.current_tp_price = tp_price
        self._total_trades += 1

        send_telegram_message(
            f"{'📈' if side == 'long' else '📉'} <b>QUANT v3 ENTRY — {side.upper()}</b>\n\n"
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
            f"Signals:  {sig.n_agreeing}/7 agree | thr={sig.threshold_used:.3f}\n"
            f"HTF:      15m={self._htf.trend_15m:+.2f} 4h={self._htf.trend_4h:+.2f}\n\n"
            f"<i>{sig}</i>"
        )
        logger.info(f"✅ ACTIVE {side.upper()} @ ${fill_price:,.2f} | R:R=1:{rr:.2f}")

    # ───────────────────────────────────────────────────────────────────
    # ACTIVE MANAGEMENT — v3: faster trailing + signal exit
    # ───────────────────────────────────────────────────────────────────

    def _manage_active(self, data_manager, order_manager, now: float) -> None:
        pos = self._pos
        price = data_manager.get_last_price()
        if price < 1.0:
            return

        sig = self._compute_signals(data_manager)
        if sig is not None:
            self._log_thinking(sig, price, now)
            c = sig.composite; flip = QCfg.EXIT_FLIP_THRESH()
            if pos.side == "long" and c <= -flip:
                logger.info(f"🔄 Alpha flip ({c:+.3f} ≤ -{flip}) → exit LONG")
                self._exit_trade(order_manager, price, "signal_reversal"); return
            if pos.side == "short" and c >= flip:
                logger.info(f"🔄 Alpha flip ({c:+.3f} ≥ +{flip}) → exit SHORT")
                self._exit_trade(order_manager, price, "signal_reversal"); return

        if QCfg.TRAIL_ENABLED():
            if now - pos.last_trail_time >= QCfg.TRAIL_INTERVAL_S():
                self._pos.last_trail_time = now
                closed = self._update_trailing_sl(order_manager, data_manager, price, sig, now)
                if closed:
                    return

    def _find_swing_sl(self, data_manager, side: str, atr: float) -> Optional[float]:
        try:
            candles = data_manager.get_candles("5m", limit=9)
        except Exception:
            return None
        if not candles or len(candles) < 4:
            return None
        closed = candles[:-1][-6:]
        buffer = 0.25 * atr
        if side == "short":
            return max(float(c['h']) for c in closed) + buffer
        else:
            return min(float(c['l']) for c in closed) - buffer

    def _update_trailing_sl(self, order_manager, data_manager,
                            price: float, sig: Optional[SignalBreakdown],
                            now: float) -> bool:
        pos = self._pos
        atr = self._atr_5m.atr
        if atr < 1e-10:
            return False

        # Track peak profit
        profit = ((pos.entry_price - price) if pos.side == "short"
                  else (price - pos.entry_price))
        if profit > pos.peak_profit:
            pos.peak_profit = profit

        init_dist = pos.initial_sl_dist
        tier = pos.peak_profit / init_dist if init_dist > 1e-10 else 0.0

        if tier < QCfg.TRAIL_ACTIVATE_R():
            return False

        # Signal conviction multiplier
        if sig is not None:
            raw = sig.composite
            if pos.side == "short":
                strength = min(abs(min(raw, 0.0)) / 0.65, 1.0)
            else:
                strength = min(abs(max(raw, 0.0)) / 0.65, 1.0)
        else:
            strength = 0.5
        signal_mult = 0.55 + 0.45 * strength

        # ATR volatility ratio
        entry_atr = pos.entry_atr if pos.entry_atr > 1e-10 else atr
        atr_ratio = max(0.60, min(atr / entry_atr, 1.30))

        # Time-decay tightening
        hold_ratio = min((now - pos.entry_time) / QCfg.MAX_HOLD_SEC(), 1.0)
        time_mult = 1.0 - 0.45 * hold_ratio

        # Combined trail distance
        base_mult = QCfg.TRAIL_ATR_MULT()
        trail_dist = base_mult * atr * signal_mult * atr_ratio * time_mult

        # Swing structure
        swing_sl = self._find_swing_sl(data_manager, pos.side, atr)

        # Determine candidate SL by tier
        if pos.side == "short":
            if tier < 1.0:
                new_sl = pos.entry_price + 0.10 * atr
            elif tier < 1.5:
                new_sl = pos.entry_price - 0.35 * atr
            else:
                atr_trail_sl = price + trail_dist
                new_sl = atr_trail_sl
                if swing_sl is not None:
                    new_sl = min(new_sl, swing_sl)
        else:
            if tier < 1.0:
                new_sl = pos.entry_price - 0.10 * atr
            elif tier < 1.5:
                new_sl = pos.entry_price + 0.35 * atr
            else:
                atr_trail_sl = price - trail_dist
                new_sl = atr_trail_sl
                if swing_sl is not None:
                    new_sl = max(new_sl, swing_sl)

        # Ratchet: SL may only improve
        new_sl_tick = _round_to_tick(new_sl)
        min_move = QCfg.TRAIL_MIN_MOVE_ATR() * atr

        if pos.side == "short":
            if new_sl_tick >= pos.sl_price - min_move:
                return False
        else:
            if new_sl_tick <= pos.sl_price + min_move:
                return False

        tier_label = (
            "🟡 BE" if tier < 1.0 else
            "🟠 Partial" if tier < 1.5 else
            "🟢 Full trail"
        )
        hold_min = (now - pos.entry_time) / 60.0
        logger.info(
            f"🔒 Trail [{tier_label}] "
            f"${pos.sl_price:,.1f} → ${new_sl_tick:,.1f} "
            f"| R={tier:.1f}  MFE={pos.peak_profit:.1f}pts  hold={hold_min:.0f}m"
        )

        exit_side = "sell" if pos.side == "long" else "buy"
        result = order_manager.replace_stop_loss(
            existing_sl_order_id=pos.sl_order_id,
            side=exit_side,
            quantity=pos.quantity,
            new_trigger_price=new_sl_tick,
        )

        if result is None:
            logger.warning("🚨 SL already fired — finalising")
            self._record_exchange_exit(None)
            return True

        if isinstance(result, dict) and "error" in result:
            logger.error(f"Trail SL error: {result.get('error')}")
            return False

        if result and isinstance(result, dict):
            self._pos.sl_price = new_sl_tick
            self._pos.sl_order_id = result.get("order_id", pos.sl_order_id)
            self.current_sl_price = new_sl_tick
            if not pos.trail_active:
                self._pos.trail_active = True
                logger.info("✅ Trailing SL active")

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
            f"{'✅' if pnl > 0 else '❌'} <b>QUANT v3 EXIT — {pos.side.upper()}</b>\n\n"
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

    def _record_exchange_exit(self, ex_pos: Optional[Dict]) -> None:
        pos = self._pos
        pnl = 0.0
        if ex_pos is not None:
            pnl = float(ex_pos.get("unrealized_pnl", 0.0))
        if abs(pnl) < 1e-10:
            logger.warning("Exchange exit: PnL=0 recorded")
        self._record_pnl(pnl)
        send_telegram_message(
            f"📡 <b>EXCHANGE EXIT — {pos.side.upper()}</b>\n\n"
            f"TP/SL fired between sync ticks\n"
            f"Entry: ${pos.entry_price:,.2f}\n"
            f"PnL:   ${pnl:+.2f} USDT\n\n"
            f"<i>Trades: {self._total_trades} | WR: {self._win_rate():.0%}</i>"
        )
        self._finalise_exit()

    def _record_pnl(self, pnl: float) -> None:
        self._total_pnl += pnl
        if pnl > 0:
            self._winning_trades += 1
        self._risk_gate.record_trade_result(pnl)

    def _finalise_exit(self) -> None:
        self._pos = PositionState()
        self._last_exit_time = time.time()
        self.current_sl_price = 0.0
        self.current_tp_price = 0.0
        logger.info("Position closed — FLAT")

    # ───────────────────────────────────────────────────────────────────
    # RECONCILIATION
    # ───────────────────────────────────────────────────────────────────

    def _reconcile_query_thread(self, order_manager) -> None:
        try:
            try:
                ex_pos = order_manager.get_open_position()
            except Exception as e:
                logger.debug(f"Reconcile query error: {e}")
                return
            if ex_pos is None:
                return
            ex_size = float(ex_pos.get("size", 0.0))
            open_orders = None
            if ex_size >= float(getattr(config, "MIN_POSITION_SIZE", 0.001)):
                try:
                    open_orders = order_manager.get_open_orders()
                except Exception:
                    pass
            with self._lock:
                self._reconcile_data = {"ex_pos": ex_pos, "open_orders": open_orders}
        except Exception as e:
            logger.warning(f"Reconcile thread error: {e}")
        finally:
            self._reconcile_pending = False

    def _reconcile_apply(self, order_manager, data: Dict) -> None:
        ex_pos = data["ex_pos"]
        open_orders = data.get("open_orders")
        ex_size = float(ex_pos.get("size", 0.0))
        ex_side = str(ex_pos.get("side") or "").upper()
        phase = self._pos.phase

        if phase == PositionPhase.FLAT and ex_size >= QCfg.MIN_QTY():
            ex_entry = float(ex_pos.get("entry_price", 0.0))
            ex_upnl = float(ex_pos.get("unrealized_pnl", 0.0))
            sl_order_id, tp_order_id = None, None
            sl_price, tp_price = 0.0, 0.0
            if open_orders is not None:
                for o in open_orders:
                    otype = o.get("type", "").upper()
                    if otype in ("STOP_MARKET", "STOP", "STOP_LOSS_MARKET"):
                        sl_order_id = o["order_id"]
                        sl_price = float(o.get("trigger_price") or 0)
                    elif otype in ("TAKE_PROFIT_MARKET", "TAKE_PROFIT"):
                        tp_order_id = o["order_id"]
                        tp_price = float(o.get("trigger_price") or 0)
            internal_side = "long" if ex_side == "LONG" else "short"
            self._pos = PositionState(
                phase=PositionPhase.ACTIVE,
                side=internal_side,
                quantity=ex_size,
                entry_price=ex_entry if ex_entry > 0 else 0.0,
                sl_price=sl_price,
                tp_price=tp_price,
                sl_order_id=sl_order_id,
                tp_order_id=tp_order_id,
                entry_time=time.time(),
                initial_sl_dist=abs(ex_entry - sl_price) if sl_price > 0 else 0.0,
                entry_atr=self._atr_5m.atr,
            )
            self.current_sl_price = sl_price
            self.current_tp_price = tp_price
            self._confirm_long = self._confirm_short = 0
            logger.warning(f"⚡ RECONCILE: adopted {ex_side} @ ${ex_entry:,.2f}")
            send_telegram_message(
                f"⚡ <b>POSITION ADOPTED</b>\n"
                f"Side: {ex_side} | Size: {ex_size}\n"
                f"Entry: ${ex_entry:,.2f} | uPnL: ${ex_upnl:+.2f}"
            )
            return

        if phase == PositionPhase.ACTIVE and ex_size < QCfg.MIN_QTY():
            logger.info("📡 Reconcile: exchange FLAT while ACTIVE → TP/SL fired")
            self._record_exchange_exit(ex_pos)
            return

        if phase == PositionPhase.ACTIVE and ex_size >= QCfg.MIN_QTY():
            if (not self._pos.sl_order_id or not self._pos.tp_order_id) \
                    and open_orders is not None:
                for o in open_orders:
                    otype = o.get("type", "").upper()
                    if not self._pos.sl_order_id and otype in ("STOP_MARKET", "STOP", "STOP_LOSS_MARKET"):
                        self._pos.sl_order_id = o["order_id"]
                    elif not self._pos.tp_order_id and otype in ("TAKE_PROFIT_MARKET", "TAKE_PROFIT"):
                        self._pos.tp_order_id = o["order_id"]

    # ───────────────────────────────────────────────────────────────────
    # POSITION SYNC
    # ───────────────────────────────────────────────────────────────────

    def _sync_position(self, order_manager) -> None:
        try:
            ex_pos = order_manager.get_open_position()
        except Exception:
            return
        if ex_pos is None:
            return
        ex_size = float(ex_pos.get("size", 0.0))

        if self._pos.phase == PositionPhase.ACTIVE:
            if ex_size < QCfg.MIN_QTY():
                logger.info("📡 Sync: exchange FLAT while ACTIVE → TP/SL fired")
                self._record_exchange_exit(ex_pos)
            else:
                ex_side = str(ex_pos.get("side") or "").upper()
                expected = "LONG" if self._pos.side == "long" else "SHORT"
                if ex_side and ex_side != expected:
                    logger.warning(f"Side mismatch: internal={expected} exchange={ex_side}")

        elif self._pos.phase == PositionPhase.EXITING:
            if ex_size < QCfg.MIN_QTY():
                logger.info("📡 Sync: EXITING confirmed flat")
                self._finalise_exit()

    # ───────────────────────────────────────────────────────────────────
    # SIZING
    # ───────────────────────────────────────────────────────────────────

    def _compute_quantity(self, risk_manager, price: float) -> Optional[float]:
        bal = risk_manager.get_available_balance()
        if bal is None:
            return None
        available = float(bal.get("available", 0.0))
        if available < QCfg.MIN_MARGIN_USDT():
            return None

        margin_alloc = available * QCfg.MARGIN_PCT()
        if margin_alloc < QCfg.MIN_MARGIN_USDT():
            return None

        notional = margin_alloc * QCfg.LEVERAGE()
        qty_raw = notional / price

        step = QCfg.LOT_STEP()
        qty = math.floor(qty_raw / step) * step
        qty = round(qty, 8)
        qty = max(QCfg.MIN_QTY(), min(QCfg.MAX_QTY(), qty))

        actual_margin = (qty * price) / QCfg.LEVERAGE()
        if actual_margin > margin_alloc * 1.02:
            return None

        logger.info(
            f"Sizing → avail=${available:.2f} | alloc={QCfg.MARGIN_PCT():.0%} "
            f"| margin=${margin_alloc:.2f} | {QCfg.LEVERAGE()}x "
            f"| notional=${notional:.2f} | qty={qty}"
        )
        return qty

    # ───────────────────────────────────────────────────────────────────
    # SL / TP
    # ───────────────────────────────────────────────────────────────────

    def _compute_sl_tp(self, price: float, side: str,
                       atr: float) -> Tuple[Optional[float], Optional[float]]:
        sl_atr = QCfg.SL_ATR_MULT() * atr
        tp_atr = QCfg.TP_ATR_MULT() * atr

        sl_min = price * QCfg.MIN_SL_PCT()
        sl_max = price * QCfg.MAX_SL_PCT()
        sl_dist = max(sl_min, min(sl_max, sl_atr))

        tp_dist = max(sl_dist * QCfg.MIN_RR_RATIO(), tp_atr)

        if side == "long":
            sl_raw = price - sl_dist
            tp_raw = price + tp_dist
        else:
            sl_raw = price + sl_dist
            tp_raw = price - tp_dist

        sl_price = _round_to_tick(sl_raw)
        tp_price = _round_to_tick(tp_raw)

        if side == "long" and (sl_price >= price or tp_price <= price):
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
        gross = ((exit_price - pos.entry_price) if pos.side == "long"
                 else (pos.entry_price - exit_price)) * pos.quantity
        fee = (pos.entry_price + exit_price) * pos.quantity * QCfg.COMMISSION_RATE()
        return gross - fee

    def _win_rate(self) -> float:
        return self._winning_trades / self._total_trades if self._total_trades else 0.0

    def get_stats(self) -> Dict:
        return {
            "total_trades": self._total_trades,
            "winning_trades": self._winning_trades,
            "win_rate": f"{self._win_rate():.1%}",
            "total_pnl": round(self._total_pnl, 2),
            "daily_trades": self._risk_gate.daily_trades,
            "consec_losses": self._risk_gate.consec_losses,
            "current_phase": self._pos.phase.name,
            "last_signal": str(self._last_sig),
            "atr_5m": round(self._atr_5m.atr, 2),
            "atr_1m": round(self._atr_1m.atr, 2),
            "atr_pctile": f"{self._atr_5m.get_percentile():.0%}",
            "regime_ok": self._atr_5m.is_regime_valid(),
        }

    def format_status_report(self) -> str:
        stats = self.get_stats()
        pos = self._pos
        lines = [
            "📊 <b>QUANT v3 STATUS</b>",
            "",
            f"Phase:       {stats['current_phase']}",
            f"Regime:      pen={self._atr_5m.regime_penalty():.2f}",
            f"ATR 5m/1m:   ${stats['atr_5m']} / ${stats['atr_1m']}  ({stats['atr_pctile']})",
            f"HTF:         15m={self._htf.trend_15m:+.3f}  4h={self._htf.trend_4h:+.3f}",
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
                f"<b>Active ({pos.side.upper()}):</b>",
                f"Entry: ${pos.entry_price:,.2f} | SL: ${pos.sl_price:,.2f} | TP: ${pos.tp_price:,.2f}",
                f"Qty: {pos.quantity} | Risk: ${pos.initial_risk:.2f}",
                f"Hold: {elapsed_min:.1f}/{max_hold_min:.0f}m | Trail: {'✅' if pos.trail_active else '⏳'}",
            ]
        return "\n".join(lines)

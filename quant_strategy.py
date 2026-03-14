"""
QUANT STRATEGY v4 — MEAN-REVERSION + ORDER FLOW CONFLUENCE
==============================================================================
COMPLETE ARCHITECTURAL REDESIGN from v3.

WHY v3 FAILED (see logs 2026-03-14 00:18–01:19):
  1. Momentum chasing on 1m/5m = buying tops, selling bottoms
  2. Regime penalty zeroed signals for 25 min while in profitable trade
  3. EXIT_FLIP=0.22 caused 4 whipsaws in 1 hour
  4. ATR-only SL/TP placed stops in noise zone ($10 on BTC)
  5. 20-second cooldown = revenge trading

CORE PHILOSOPHY CHANGE:
  v3: "Detect momentum and chase it" → LOW win rate by design
  v4: "Wait for overextension, fade it back to equilibrium" → HIGH win rate

  On 1m/5m timeframes, price reverts to VWAP ~70% of the time.
  We enter ONLY when price is significantly overextended AND
  order flow confirms exhaustion of the move.

ENTRY CONDITIONS (ALL must be true — hard confluence gate):
  1. VWAP DEVIATION: Price > 1.2× ATR from VWAP (overextended)
  2. CVD DIVERGENCE:  Volume delta NOT confirming the move (exhaustion)
  3. ORDERBOOK LEAN:  Book imbalance favors reversion direction
  4. HTF ALIGNMENT:   Higher timeframe not strongly against us
  5. REGIME VALID:    ATR in tradeable range (not dead flat)

SL/TP PHILOSOPHY:
  - SL: Behind recent swing extreme + buffer (structure, not ATR)
  - TP: Tight — 50% of distance back to VWAP (high win rate target)
  - Trail: Breakeven at 0.4R, then ratchet to lock profit

ANTI-WHIPSAW:
  - 180-second cooldown after exit (3 minutes, not 20 seconds)
  - Directional bias: after exiting, don't immediately flip direction
  - Max 8 trades/day (was 14)
  - Max 3 consecutive losses pause (1-hour lockout)
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

    # ── v4 MEAN-REVERSION THRESHOLDS ──
    @staticmethod
    def VWAP_ENTRY_ATR_MULT() -> float: return float(_cfg("QUANT_VWAP_ENTRY_ATR_MULT", 1.2))
    @staticmethod
    def CVD_DIVERGENCE_MIN()  -> float: return float(_cfg("QUANT_CVD_DIVERGENCE_MIN", 0.15))
    @staticmethod
    def OB_CONFIRM_MIN()      -> float: return float(_cfg("QUANT_OB_CONFIRM_MIN", 0.10))
    @staticmethod
    def COMPOSITE_ENTRY_MIN() -> float: return float(_cfg("QUANT_COMPOSITE_ENTRY_MIN", 0.30))
    @staticmethod
    def EXIT_REVERSAL_THRESH()-> float: return float(_cfg("QUANT_EXIT_REVERSAL_THRESH", 0.40))
    @staticmethod
    def CONFIRM_TICKS()   -> int:   return int(_cfg("QUANT_CONFIRM_TICKS", 2))

    # ── SL/TP v4: Structure-Based ──
    @staticmethod
    def SL_SWING_LOOKBACK()  -> int:   return int(_cfg("QUANT_SL_SWING_LOOKBACK", 12))
    @staticmethod
    def SL_BUFFER_ATR_MULT() -> float: return float(_cfg("QUANT_SL_BUFFER_ATR_MULT", 0.4))
    @staticmethod
    def TP_VWAP_FRACTION()   -> float: return float(_cfg("QUANT_TP_VWAP_FRACTION", 0.50))
    @staticmethod
    def MIN_SL_PCT()      -> float: return float(_cfg("MIN_SL_DISTANCE_PCT", 0.003))
    @staticmethod
    def MAX_SL_PCT()      -> float: return float(_cfg("MAX_SL_DISTANCE_PCT", 0.035))
    @staticmethod
    def MIN_RR_RATIO()    -> float: return float(_cfg("MIN_RISK_REWARD_RATIO", 0.8))
    @staticmethod
    def ATR_PERIOD()      -> int:   return int(_cfg("SL_ATR_PERIOD", 14))

    # ── Trailing v4 ──
    @staticmethod
    def TRAIL_ENABLED()      -> bool:  return bool(_cfg("QUANT_TRAIL_ENABLED", True))
    @staticmethod
    def TRAIL_BE_R()         -> float: return float(_cfg("QUANT_TRAIL_BE_R", 0.4))
    @staticmethod
    def TRAIL_LOCK_R()       -> float: return float(_cfg("QUANT_TRAIL_LOCK_R", 0.8))
    @staticmethod
    def TRAIL_INTERVAL_S()   -> int:   return int(_cfg("TRAILING_SL_CHECK_INTERVAL", 10))
    @staticmethod
    def TRAIL_MIN_MOVE_ATR() -> float: return float(_cfg("SL_MIN_IMPROVEMENT_ATR_MULT", 0.08))

    # ── Indicator Windows ──
    @staticmethod
    def CVD_WINDOW()          -> int:   return int(_cfg("QUANT_CVD_WINDOW", 20))
    @staticmethod
    def CVD_HIST_MULT()       -> int:   return int(_cfg("QUANT_CVD_HIST_MULT", 15))
    @staticmethod
    def VWAP_WINDOW()         -> int:   return int(_cfg("QUANT_VWAP_WINDOW", 50))
    @staticmethod
    def EMA_FAST()            -> int:   return int(_cfg("QUANT_EMA_FAST", 8))
    @staticmethod
    def EMA_SLOW()            -> int:   return int(_cfg("QUANT_EMA_SLOW", 21))
    @staticmethod
    def VOL_FLOW_WINDOW()     -> int:   return int(_cfg("QUANT_VOL_FLOW_WINDOW", 10))

    # ── Minimum Data ──
    @staticmethod
    def MIN_1M_BARS()     -> int:   return int(_cfg("MIN_CANDLES_1M", 80))
    @staticmethod
    def MIN_5M_BARS()     -> int:   return int(_cfg("MIN_CANDLES_5M", 60))

    # ── Regime ──
    @staticmethod
    def ATR_PCTILE_WINDOW()-> int:  return int(_cfg("QUANT_ATR_PCTILE_WINDOW", 100))
    @staticmethod
    def ATR_MIN_PCTILE()  -> float: return float(_cfg("QUANT_ATR_MIN_PCTILE", 0.05))
    @staticmethod
    def ATR_MAX_PCTILE()  -> float: return float(_cfg("QUANT_ATR_MAX_PCTILE", 0.97))

    # ── Timing v4 ──
    @staticmethod
    def MAX_HOLD_SEC()    -> int:   return int(_cfg("QUANT_MAX_HOLD_SEC", 2400))
    @staticmethod
    def COOLDOWN_SEC()    -> int:   return int(_cfg("QUANT_COOLDOWN_SEC", 180))
    @staticmethod
    def LOSS_LOCKOUT_SEC()-> int:   return int(_cfg("QUANT_LOSS_LOCKOUT_SEC", 3600))
    @staticmethod
    def TICK_EVAL_SEC()   -> float: return float(_cfg("ENTRY_EVALUATION_INTERVAL_SECONDS", 1))
    @staticmethod
    def POS_SYNC_SEC()    -> float: return float(_cfg("QUANT_POS_SYNC_SEC", 30))

    # ── Risk ──
    @staticmethod
    def MAX_DAILY_TRADES()  -> int:   return int(_cfg("MAX_DAILY_TRADES", 8))
    @staticmethod
    def MAX_CONSEC_LOSSES() -> int:   return int(_cfg("MAX_CONSECUTIVE_LOSSES", 3))
    @staticmethod
    def MAX_DAILY_LOSS_PCT()-> float: return float(_cfg("MAX_DAILY_LOSS_PCT", 5.0))

    # ── Signal Weights (v4: reversion-weighted) ──
    @staticmethod
    def W_VWAP_DEV()  -> float: return float(_cfg("QUANT_W_VWAP_DEV", 0.30))
    @staticmethod
    def W_CVD_DIV()   -> float: return float(_cfg("QUANT_W_CVD_DIV", 0.25))
    @staticmethod
    def W_OB()        -> float: return float(_cfg("QUANT_W_OB", 0.20))
    @staticmethod
    def W_TICK_FLOW() -> float: return float(_cfg("QUANT_W_TICK_FLOW", 0.15))
    @staticmethod
    def W_VOL_EXHAUSTION() -> float: return float(_cfg("QUANT_W_VOL_EXHAUSTION", 0.10))

    # ── HTF Filter ──
    @staticmethod
    def HTF_ENABLED()       -> bool:  return bool(_cfg("QUANT_HTF_ENABLED", True))
    @staticmethod
    def HTF_VETO_STRENGTH() -> float: return float(_cfg("QUANT_HTF_VETO_STRENGTH", 0.70))

    # ── Orderbook / Tick ──
    @staticmethod
    def OB_DEPTH_LEVELS() -> int:   return int(_cfg("QUANT_OB_DEPTH_LEVELS", 5))
    @staticmethod
    def OB_HIST_LEN()     -> int:   return int(_cfg("QUANT_OB_HIST_LEN", 60))
    @staticmethod
    def TICK_AGG_WINDOW_SEC() -> float: return float(_cfg("QUANT_TICK_AGG_WINDOW_SEC", 30.0))


def _round_to_tick(price: float) -> float:
    tick = QCfg.TICK_SIZE()
    if tick <= 0:
        return price
    return round(round(price / tick) * tick, 10)


def _sigmoid(z: float, steepness: float = 1.0) -> float:
    return max(-1.0, min(1.0, z * steepness / (1.0 + abs(z * steepness) * 0.5)))


# ═══════════════════════════════════════════════════════════════════════════════
# ENGINE 1: VWAP DEVIATION — Primary Mean-Reversion Signal
# ═══════════════════════════════════════════════════════════════════════════════

class VWAPEngine:
    """
    v4: VWAP deviation is the PRIMARY signal, not secondary.
    We compute how far price has deviated from VWAP in ATR units.
    Signal is INVERTED: price above VWAP → NEGATIVE (short opportunity)
                       price below VWAP → POSITIVE (long opportunity)
    This is MEAN-REVERSION: we fade the deviation.
    """

    def __init__(self):
        self._vwap = 0.0
        self._std = 0.0
        self._deviation_atr = 0.0  # how many ATRs away from VWAP

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

        if atr > 1e-10:
            price = float(candles[-1]['c'])
            self._deviation_atr = (price - self._vwap) / atr

    def get_reversion_signal(self, price: float, atr: float) -> float:
        """
        Returns mean-reversion signal.
        Positive = price below VWAP → long opportunity (fade down)
        Negative = price above VWAP → short opportunity (fade up)
        
        Only fires when deviation > entry threshold ATR.
        """
        if self._vwap < 1e-10 or atr < 1e-10:
            return 0.0

        dev = (price - self._vwap) / atr
        entry_thresh = QCfg.VWAP_ENTRY_ATR_MULT()

        # Only signal when sufficiently overextended
        if abs(dev) < entry_thresh * 0.6:
            return 0.0

        # INVERT: price above VWAP → negative signal (short/fade)
        #         price below VWAP → positive signal (long/fade)
        raw = -dev / (entry_thresh * 2.0)
        return max(-1.0, min(1.0, _sigmoid(raw, 1.5)))

    def is_overextended(self, price: float, atr: float) -> bool:
        """Hard gate: price must be > VWAP_ENTRY_ATR_MULT × ATR from VWAP."""
        if self._vwap < 1e-10 or atr < 1e-10:
            return False
        dev = abs(price - self._vwap) / atr
        return dev >= QCfg.VWAP_ENTRY_ATR_MULT()

    def reversion_side(self, price: float) -> str:
        """Which side would a reversion trade be?"""
        if price > self._vwap:
            return "short"  # price above VWAP → fade by shorting
        return "long"       # price below VWAP → fade by going long

    def tp_target(self, price: float) -> float:
        """TP target: fraction of distance back to VWAP."""
        frac = QCfg.TP_VWAP_FRACTION()
        return price + (self._vwap - price) * frac

    @property
    def vwap(self) -> float: return self._vwap
    @property
    def vwap_std(self) -> float: return self._std
    @property
    def deviation_atr(self) -> float: return self._deviation_atr


# ═══════════════════════════════════════════════════════════════════════════════
# ENGINE 2: CVD DIVERGENCE — Exhaustion Detection
# ═══════════════════════════════════════════════════════════════════════════════

class CVDEngine:
    """
    v4: CVD is used for DIVERGENCE detection, not raw momentum.
    
    Divergence = price made a new extreme but CVD didn't confirm.
    Example: Price pushed to new high, but CVD is flat/falling = 
    buyers are exhausted → SHORT (fade the high).
    """

    def __init__(self):
        self._deltas: deque = deque(maxlen=QCfg.CVD_WINDOW() * QCfg.CVD_HIST_MULT())
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

    def get_divergence_signal(self, candles: List[Dict]) -> float:
        """
        v4: Divergence-based signal.
        
        Compares price momentum direction with CVD momentum direction.
        When they disagree → exhaustion → fade signal.
        
        Returns: positive = bullish divergence (price fell but CVD rising → long)
                 negative = bearish divergence (price rose but CVD falling → short)
        """
        w = QCfg.CVD_WINDOW()
        arr = list(self._deltas)
        n = len(arr)
        if n < w + 10 or len(candles) < w:
            return 0.0

        # CVD slope: sum of recent deltas vs earlier deltas
        recent_cvd = sum(arr[-w//2:])
        earlier_cvd = sum(arr[-w:-w//2])
        cvd_slope = recent_cvd - earlier_cvd

        # Price slope: recent closes vs earlier closes
        closes = [float(c['c']) for c in candles[-w:]]
        mid = w // 2
        recent_price = sum(closes[mid:]) / max(len(closes[mid:]), 1)
        earlier_price = sum(closes[:mid]) / max(len(closes[:mid]), 1)
        price_slope = recent_price - earlier_price

        if abs(price_slope) < 1e-10:
            return 0.0

        # Normalize CVD slope
        all_sums = []
        running = sum(arr[:w//2])
        all_sums.append(running)
        for i in range(w//2, n - w//2):
            running += arr[i] - arr[i - w//2]
            all_sums.append(running)
        if len(all_sums) < 5:
            return 0.0
        mu = sum(all_sums) / len(all_sums)
        var = sum((s - mu) ** 2 for s in all_sums) / max(len(all_sums) - 1, 1)
        std = math.sqrt(var)
        if std < 1e-12:
            return 0.0

        cvd_z = cvd_slope / std

        # Divergence: price going UP but CVD going DOWN (or vice versa)
        # This is a FADE signal
        price_dir = 1.0 if price_slope > 0 else -1.0
        cvd_dir = 1.0 if cvd_z > 0 else -1.0

        if price_dir == cvd_dir:
            # No divergence — CVD confirms the move → weak/no signal
            return 0.0

        # Divergence detected! Return signal in REVERSION direction
        # price_dir > 0 (price rising) + cvd_dir < 0 (CVD falling) → bearish divergence → short
        divergence_strength = min(abs(cvd_z), 3.0) / 3.0
        return -price_dir * divergence_strength  # negative of price dir = fade dir

    def get_raw_signal(self) -> float:
        """Raw CVD z-score for logging."""
        w = QCfg.CVD_WINDOW()
        arr = list(self._deltas)
        n = len(arr)
        if n < w + 10:
            return 0.0
        current_sum = sum(arr[-w:])
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
        return _sigmoid((current_sum - mu) / std, 0.6)


# ═══════════════════════════════════════════════════════════════════════════════
# ENGINE 3: ORDERBOOK IMBALANCE
# ═══════════════════════════════════════════════════════════════════════════════

class OrderbookEngine:
    """Same as v3 but signal interpretation inverted for mean-reversion."""

    def __init__(self):
        self._imbalance_hist: deque = deque(maxlen=QCfg.OB_HIST_LEN())
        self._last_imbalance = 0.0
        self._spread_ratio = 0.0

    def update(self, orderbook: Dict, price: float) -> None:
        bids = orderbook.get("bids", [])
        asks = orderbook.get("asks", [])
        depth = QCfg.OB_DEPTH_LEVELS()

        if not bids or not asks or price < 1.0:
            return

        bid_depth = 0.0
        for level in bids[:depth]:
            try:
                bid_depth += float(level[1]) if isinstance(level, (list, tuple)) else 0.0
            except (IndexError, ValueError, TypeError):
                pass

        ask_depth = 0.0
        for level in asks[:depth]:
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

        try:
            best_bid = float(bids[0][0]) if isinstance(bids[0], (list, tuple)) else 0.0
            best_ask = float(asks[0][0]) if isinstance(asks[0], (list, tuple)) else 0.0
            if best_bid > 0 and best_ask > 0:
                self._spread_ratio = (best_ask - best_bid) / ((best_bid + best_ask) / 2.0)
        except (IndexError, ValueError, TypeError):
            pass

    def get_signal(self) -> float:
        """
        Positive = more bids than asks = bullish support
        Negative = more asks than bids = bearish pressure
        """
        hist = list(self._imbalance_hist)
        if len(hist) < 15:
            return 0.0

        current = hist[-1]
        baseline = hist[:-1]
        mu = sum(baseline) / len(baseline)
        var = sum((x - mu) ** 2 for x in baseline) / max(len(baseline) - 1, 1)
        std = math.sqrt(var)
        if std < 1e-12:
            return _sigmoid(current * 3.0, 0.8)

        z = (current - mu) / std
        spread_mult = max(0.5, min(1.0, 1.0 - (self._spread_ratio - 0.0002) * 100.0))
        return _sigmoid(z, 0.6) * spread_mult


# ═══════════════════════════════════════════════════════════════════════════════
# ENGINE 4: TICK FLOW
# ═══════════════════════════════════════════════════════════════════════════════

class TickFlowEngine:
    """Real-time buy/sell aggressor analysis from the trade stream."""

    def __init__(self):
        self._buy_vol: deque = deque(maxlen=600)
        self._sell_vol: deque = deque(maxlen=600)
        self._flow_hist: deque = deque(maxlen=120)
        self._last_signal = 0.0

    def on_trade(self, price: float, qty: float, is_buyer: bool, ts: float) -> None:
        dollar_vol = price * qty
        if is_buyer:
            self._buy_vol.append((ts, dollar_vol))
        else:
            self._sell_vol.append((ts, dollar_vol))

    def compute_signal(self) -> float:
        now = time.time()
        window = QCfg.TICK_AGG_WINDOW_SEC()
        cutoff = now - window

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

        mu = sum(hist[:-1]) / len(hist[:-1])
        var = sum((x - mu) ** 2 for x in hist[:-1]) / max(len(hist[:-1]) - 1, 1)
        std = math.sqrt(var)

        if std < 1e-12:
            return _sigmoid(flow_ratio * 2.0, 0.8)

        z = (flow_ratio - mu) / std
        self._last_signal = _sigmoid(z, 0.5)
        return self._last_signal

    def get_signal(self) -> float:
        return self._last_signal


# ═══════════════════════════════════════════════════════════════════════════════
# ENGINE 5: VOLUME EXHAUSTION — New in v4
# ═══════════════════════════════════════════════════════════════════════════════

class VolumeExhaustionEngine:
    """
    Detects when volume is declining during a price move.
    Declining volume on a trend = exhaustion = reversion incoming.
    
    Returns signal in REVERSION direction.
    """

    def __init__(self):
        self._last_signal = 0.0

    def compute(self, candles: List[Dict]) -> float:
        if len(candles) < 20:
            return 0.0

        recent = candles[-10:]
        earlier = candles[-20:-10]

        # Price direction
        price_change = float(recent[-1]['c']) - float(earlier[0]['c'])
        price_dir = 1.0 if price_change > 0 else -1.0

        # Volume trend
        recent_vol = sum(float(c['v']) for c in recent)
        earlier_vol = sum(float(c['v']) for c in earlier)

        if earlier_vol < 1e-10:
            return 0.0

        vol_ratio = recent_vol / earlier_vol

        # Exhaustion: price moving but volume declining
        if vol_ratio < 0.7:
            # Strong volume decline — exhaustion signal
            strength = min((0.7 - vol_ratio) / 0.4, 1.0)
            self._last_signal = -price_dir * strength  # fade direction
        elif vol_ratio < 0.9:
            # Mild decline
            strength = (0.9 - vol_ratio) / 0.4
            self._last_signal = -price_dir * strength * 0.5
        else:
            self._last_signal = 0.0

        return self._last_signal


# ═══════════════════════════════════════════════════════════════════════════════
# ATR ENGINE
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
    def atr(self) -> float: return self._atr

    def get_percentile(self) -> float:
        hist = list(self._atr_hist)
        if len(hist) < 10:
            return 0.5
        ref = hist[-1]
        prev = hist[:-1]
        return sum(1 for h in prev if h <= ref) / len(prev)

    def regime_valid(self) -> bool:
        """v4: Binary regime check (not gradient). Must be in tradeable range."""
        p = self.get_percentile()
        return QCfg.ATR_MIN_PCTILE() <= p <= QCfg.ATR_MAX_PCTILE()

    def regime_penalty(self) -> float:
        """For backward compat with logging. 1.0 = valid, 0.0 = invalid."""
        return 1.0 if self.regime_valid() else 0.0


# ═══════════════════════════════════════════════════════════════════════════════
# HTF TREND FILTER v4
# ═══════════════════════════════════════════════════════════════════════════════

class HTFTrendFilter:
    """
    v4: Used as VETO only, not boost. If HTF is strongly trending, we
    don't take mean-reversion trades AGAINST it — the trend might just
    continue and run our stops.
    """

    def __init__(self):
        self._trend_15m = 0.0
        self._trend_4h = 0.0

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

    def update(self, candles_15m: List[Dict], candles_4h: List[Dict],
               atr_5m: float) -> None:
        fast = QCfg.EMA_FAST()

        if len(candles_15m) > fast + 5 and atr_5m > 1e-10:
            cl15 = [float(c['c']) for c in candles_15m]
            ema15 = self._ema_series(cl15, fast)
            if len(ema15) >= 4:
                slope = ema15[-1] - ema15[-3]
                self._trend_15m = _sigmoid(slope / atr_5m, 0.8)
            else:
                self._trend_15m = 0.0
        else:
            self._trend_15m = 0.0

        slow = QCfg.EMA_SLOW()
        if len(candles_4h) > slow + 3 and atr_5m > 1e-10:
            cl4h = [float(c['c']) for c in candles_4h]
            ema4h = self._ema_series(cl4h, slow)
            if len(ema4h) >= 3:
                slope = ema4h[-1] - ema4h[-2]
                self._trend_4h = _sigmoid(slope / (atr_5m * 4.0), 0.8)
            else:
                self._trend_4h = 0.0
        else:
            self._trend_4h = 0.0

    def vetoes_trade(self, side: str) -> bool:
        """
        v4: VETO if HTF is strongly trending AGAINST our reversion trade.
        Example: if 4h is strongly bullish and we want to SHORT (fade up),
        HTF vetoes — the up move might just be trend continuation.
        """
        if not QCfg.HTF_ENABLED():
            return False

        veto_strength = QCfg.HTF_VETO_STRENGTH()
        htf_combined = self._trend_4h * 0.60 + self._trend_15m * 0.40

        if side == "long" and htf_combined < -veto_strength:
            return True  # HTF strongly bearish, don't go long
        if side == "short" and htf_combined > veto_strength:
            return True  # HTF strongly bullish, don't go short
        return False

    @property
    def trend_15m(self) -> float: return self._trend_15m
    @property
    def trend_4h(self) -> float: return self._trend_4h


# ═══════════════════════════════════════════════════════════════════════════════
# SIGNAL BREAKDOWN (for logging & telegram)
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class SignalBreakdown:
    vwap_dev:     float = 0.0   # VWAP deviation signal (reversion)
    cvd_div:      float = 0.0   # CVD divergence signal
    orderbook:    float = 0.0   # Orderbook imbalance
    tick_flow:    float = 0.0   # Tick aggressor flow
    vol_exhaust:  float = 0.0   # Volume exhaustion
    composite:    float = 0.0   # Weighted composite
    atr:          float = 0.0
    atr_pct:      float = 0.0
    regime_ok:    bool  = False
    htf_veto:     bool  = False
    overextended: bool  = False
    vwap_price:   float = 0.0
    deviation_atr: float = 0.0
    reversion_side: str = ""
    n_confirming: int   = 0     # how many sub-signals confirm
    threshold_used: float = 0.0
    regime_penalty: float = 1.0  # backward compat

    def __str__(self) -> str:
        return (
            f"VWAP={self.vwap_dev:+.3f} CVD={self.cvd_div:+.3f} "
            f"OB={self.orderbook:+.3f} TF={self.tick_flow:+.3f} "
            f"VEX={self.vol_exhaust:+.3f} → Σ={self.composite:+.4f} "
            f"dev={self.deviation_atr:+.1f}ATR "
            f"confirm={self.n_confirming}/5 "
            f"ATR=${self.atr:.1f}({self.atr_pct:.0%})"
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
# DAILY RISK GATE v4 — with consecutive loss lockout
# ═══════════════════════════════════════════════════════════════════════════════

class DailyRiskGate:
    def __init__(self):
        self._today: date = date.today()
        self._daily_trades: int = 0
        self._consec_losses: int = 0
        self._daily_pnl: float = 0.0
        self._daily_open_bal: float = 0.0
        self._loss_lockout_until: float = 0.0  # v4: timestamp when lockout expires
        self._lock = threading.Lock()

    def _reset_if_new_day(self) -> None:
        today = date.today()
        if today != self._today:
            logger.info(f"📅 Daily risk counters reset (new day: {today})")
            self._today = today
            self._daily_trades = 0
            self._daily_pnl = 0.0
            self._daily_open_bal = 0.0
            self._consec_losses = 0
            self._loss_lockout_until = 0.0

    def set_opening_balance(self, balance: float) -> None:
        with self._lock:
            self._reset_if_new_day()
            if self._daily_open_bal < 1e-10 and balance > 0:
                self._daily_open_bal = balance

    def can_trade(self, current_balance: float) -> Tuple[bool, str]:
        with self._lock:
            self._reset_if_new_day()
            now = time.time()

            # v4: Lockout after consecutive losses
            if now < self._loss_lockout_until:
                remaining = int(self._loss_lockout_until - now)
                return False, f"Loss lockout: {remaining}s remaining"

            max_dt = QCfg.MAX_DAILY_TRADES()
            max_cl = QCfg.MAX_CONSEC_LOSSES()
            max_lp = QCfg.MAX_DAILY_LOSS_PCT()

            if self._daily_trades >= max_dt:
                return False, f"Daily trade cap: {self._daily_trades}/{max_dt}"
            if self._consec_losses >= max_cl:
                self._loss_lockout_until = now + QCfg.LOSS_LOCKOUT_SEC()
                return False, f"Consec loss cap: {self._consec_losses}/{max_cl} → {QCfg.LOSS_LOCKOUT_SEC()}s lockout"
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
# MAIN STRATEGY CLASS — v4 MEAN-REVERSION
# ═══════════════════════════════════════════════════════════════════════════════

class QuantStrategy:
    """
    v4 Mean-Reversion + Order Flow Confluence Strategy.

    Public interface (unchanged for main.py compatibility):
        on_tick(data_manager, order_manager, risk_manager, timestamp_ms) → None
        get_position() → dict | None
        current_sl_price: float
        current_tp_price: float
    """

    def __init__(self, order_manager=None):
        self._om = order_manager
        self._lock = threading.RLock()

        # v4: 5 Signal Engines (reversion-focused)
        self._vwap    = VWAPEngine()
        self._cvd     = CVDEngine()
        self._ob_eng  = OrderbookEngine()
        self._tick_eng = TickFlowEngine()
        self._vol_exh = VolumeExhaustionEngine()

        # ATR on two timeframes
        self._atr_1m = ATREngine()
        self._atr_5m = ATREngine()

        # HTF Trend Filter (veto only)
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
        self._last_exit_side = ""  # v4: anti-whipsaw directional memory

        # Thinking log
        self._last_think_log = 0.0
        self._think_interval = 30.0

        # Trade dedup
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
        logger.info("⚡ QuantStrategy v4 — MEAN-REVERSION + ORDER FLOW")
        logger.info(f"   {QCfg.SYMBOL()} | {QCfg.LEVERAGE()}x lev | "
                    f"{QCfg.MARGIN_PCT():.0%} margin | "
                    f"VWAP entry > {QCfg.VWAP_ENTRY_ATR_MULT()}×ATR")
        logger.info(f"   SL: swing structure + {QCfg.SL_BUFFER_ATR_MULT()}×ATR buffer")
        logger.info(f"   TP: {QCfg.TP_VWAP_FRACTION():.0%} back to VWAP")
        logger.info(f"   Cooldown: {QCfg.COOLDOWN_SEC()}s | Loss lockout: {QCfg.LOSS_LOCKOUT_SEC()}s")
        logger.info(f"   Confirm: {QCfg.CONFIRM_TICKS()} ticks | Max trades: {QCfg.MAX_DAILY_TRADES()}/day")
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

            # Feed microstructure engines
            self._feed_microstructure(data_manager)

            # Reconciliation
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
    # MICROSTRUCTURE FEED
    # ───────────────────────────────────────────────────────────────────

    def _feed_microstructure(self, data_manager) -> None:
        try:
            ob = data_manager.get_orderbook()
            price = data_manager.get_last_price()
            if ob and price > 1.0:
                self._ob_eng.update(ob, price)
        except Exception:
            pass

        try:
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
    # SIGNAL COMPUTATION — v4 MEAN-REVERSION
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

        # Update engines
        self._vwap.update(candles_1m, atr_1m)
        self._cvd.update(candles_1m)

        # Update HTF
        try:
            candles_15m = data_manager.get_candles("15m", limit=100)
            candles_4h = data_manager.get_candles("4h", limit=50)
            self._htf.update(candles_15m, candles_4h, atr_5m)
        except Exception:
            pass

        # Collect signals
        vwap_sig = self._vwap.get_reversion_signal(price, atr_1m)
        cvd_sig = self._cvd.get_divergence_signal(candles_1m)
        ob_sig = self._ob_eng.get_signal()
        tick_sig = self._tick_eng.get_signal()
        vol_exh = self._vol_exh.compute(candles_1m)

        # Weighted composite
        composite = (
            vwap_sig * QCfg.W_VWAP_DEV() +
            cvd_sig * QCfg.W_CVD_DIV() +
            ob_sig * QCfg.W_OB() +
            tick_sig * QCfg.W_TICK_FLOW() +
            vol_exh * QCfg.W_VOL_EXHAUSTION()
        )
        composite = max(-1.0, min(1.0, composite))

        # Count confirming signals (same direction as composite)
        direction = 1.0 if composite >= 0 else -1.0
        n_confirm = 0
        for s in [vwap_sig, cvd_sig, ob_sig, tick_sig, vol_exh]:
            if s * direction > 0.05:
                n_confirm += 1

        overextended = self._vwap.is_overextended(price, atr_1m)
        rev_side = self._vwap.reversion_side(price)
        regime_ok = self._atr_5m.regime_valid()
        htf_veto = self._htf.vetoes_trade(rev_side)

        sig = SignalBreakdown(
            vwap_dev=vwap_sig,
            cvd_div=cvd_sig,
            orderbook=ob_sig,
            tick_flow=tick_sig,
            vol_exhaust=vol_exh,
            composite=composite,
            atr=atr_5m,
            atr_pct=self._atr_5m.get_percentile(),
            regime_ok=regime_ok,
            regime_penalty=1.0 if regime_ok else 0.0,
            htf_veto=htf_veto,
            overextended=overextended,
            vwap_price=self._vwap.vwap,
            deviation_atr=self._vwap.deviation_atr,
            reversion_side=rev_side,
            n_confirming=n_confirm,
            threshold_used=QCfg.COMPOSITE_ENTRY_MIN(),
        )
        self._last_sig = sig
        return sig

    # ───────────────────────────────────────────────────────────────────
    # THINKING LOG v4
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
        thr = QCfg.COMPOSITE_ENTRY_MIN()

        # Entry gate status
        gates = []
        gates.append(f"{'✅' if sig.overextended else '❌'} Overextended ({sig.deviation_atr:+.1f} ATR)")
        gates.append(f"{'✅' if sig.regime_ok else '❌'} Regime ({sig.atr_pct:.0%} pctile)")
        gates.append(f"{'✅' if not sig.htf_veto else '❌'} HTF (15m={self._htf.trend_15m:+.2f} 4h={self._htf.trend_4h:+.2f})")
        gates.append(f"{'✅' if sig.n_confirming >= 3 else '❌'} Confluence ({sig.n_confirming}/5 confirm)")
        gates.append(f"{'✅' if abs(c) >= thr else '❌'} Composite ({c:+.3f} vs ±{thr:.3f})")

        all_pass = (sig.overextended and sig.regime_ok and not sig.htf_veto
                    and sig.n_confirming >= 3 and abs(c) >= thr)

        cd_rem = max(0.0, QCfg.COOLDOWN_SEC() - (now - self._last_exit_time))
        cd_str = f"{cd_rem:.0f}s" if cd_rem > 0 else "ready"

        lines = [
            f"┌─── 🧠 v4 REVERSION  ${price:,.2f}  VWAP=${sig.vwap_price:,.2f}  ATR={sig.atr:.1f} ────",
            fmt("VWAP", sig.vwap_dev),
            fmt("CVD", sig.cvd_div),
            fmt("OB", sig.orderbook),
            fmt("TICK", sig.tick_flow),
            fmt("VEX", sig.vol_exhaust),
            f"  {'─'*42}",
            f"  Σ composite: {c:+.4f}  |  Side: {sig.reversion_side.upper()}",
            f"  ──── ENTRY GATES ────",
        ]
        for g in gates:
            lines.append(f"  {g}")
        lines.append(f"  ────────────────────")
        if all_pass:
            lines.append(f"  🎯 ALL GATES PASS — confirming ({self._confirm_long + self._confirm_short}/{QCfg.CONFIRM_TICKS()})")
        else:
            lines.append(f"  👀 Watching — gates not met")
        lines.append(f"  Cooldown: {cd_str}")
        lines.append(f"└{'─'*66}")
        logger.info("\n" + "\n".join(lines))

    # ───────────────────────────────────────────────────────────────────
    # ENTRY EVALUATION — v4: HARD CONFLUENCE GATE
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
        thr = QCfg.COMPOSITE_ENTRY_MIN()
        side = sig.reversion_side

        # ── v4: HARD CONFLUENCE GATES (ALL must pass) ──
        if not sig.overextended:
            self._confirm_long = self._confirm_short = 0
            return
        if not sig.regime_ok:
            self._confirm_long = self._confirm_short = 0
            return
        if sig.htf_veto:
            self._confirm_long = self._confirm_short = 0
            return
        if sig.n_confirming < 3:
            self._confirm_long = self._confirm_short = 0
            return

        # v4: Anti-whipsaw — don't immediately flip direction
        if self._last_exit_side and self._last_exit_side != side:
            extra_cooldown = QCfg.COOLDOWN_SEC() * 0.5  # 50% extra for direction flip
            if now - self._last_exit_time < QCfg.COOLDOWN_SEC() + extra_cooldown:
                return

        # Composite must exceed threshold in the reversion direction
        if side == "long" and c >= thr:
            self._confirm_long += 1; self._confirm_short = 0
        elif side == "short" and c <= -thr:
            self._confirm_short += 1; self._confirm_long = 0
        else:
            self._confirm_long = self._confirm_short = 0
            return

        confirm_needed = QCfg.CONFIRM_TICKS()

        if self._confirm_long >= confirm_needed:
            self._confirm_long = self._confirm_short = 0
            self._enter_trade(data_manager, order_manager, risk_manager, "long", sig)
        elif self._confirm_short >= confirm_needed:
            self._confirm_long = self._confirm_short = 0
            self._enter_trade(data_manager, order_manager, risk_manager, "short", sig)

    # ───────────────────────────────────────────────────────────────────
    # STRUCTURE-BASED SL/TP — v4
    # ───────────────────────────────────────────────────────────────────

    def _compute_sl_tp(self, data_manager, price: float, side: str,
                       atr: float) -> Tuple[Optional[float], Optional[float]]:
        """
        v4: SL behind swing structure. TP toward VWAP.
        
        SL = recent swing extreme + buffer (in direction against us)
        TP = fraction of distance back to VWAP (tight target for high WR)
        """
        # Get recent candles for swing detection
        try:
            candles_5m = data_manager.get_candles("5m", limit=QCfg.SL_SWING_LOOKBACK() + 2)
        except Exception:
            candles_5m = []

        if len(candles_5m) < 5:
            # Fallback: ATR-based
            sl_dist = max(price * QCfg.MIN_SL_PCT(), min(price * QCfg.MAX_SL_PCT(), 2.0 * atr))
            if side == "long":
                sl_price = price - sl_dist
            else:
                sl_price = price + sl_dist
        else:
            closed = candles_5m[:-1][-QCfg.SL_SWING_LOOKBACK():]
            buffer = QCfg.SL_BUFFER_ATR_MULT() * atr

            if side == "long":
                # SL below recent swing low
                swing_low = min(float(c['l']) for c in closed)
                sl_price = swing_low - buffer
            else:
                # SL above recent swing high
                swing_high = max(float(c['h']) for c in closed)
                sl_price = swing_high + buffer

        # Enforce min/max SL distance
        sl_dist = abs(price - sl_price)
        min_sl = price * QCfg.MIN_SL_PCT()
        max_sl = price * QCfg.MAX_SL_PCT()

        if sl_dist < min_sl:
            if side == "long":
                sl_price = price - min_sl
            else:
                sl_price = price + min_sl
            sl_dist = min_sl
        elif sl_dist > max_sl:
            if side == "long":
                sl_price = price - max_sl
            else:
                sl_price = price + max_sl
            sl_dist = max_sl

        # TP: tight target toward VWAP (for high win rate)
        tp_price = self._vwap.tp_target(price)

        # Ensure TP is on correct side
        if side == "long" and tp_price <= price:
            tp_price = price + sl_dist * QCfg.MIN_RR_RATIO()
        elif side == "short" and tp_price >= price:
            tp_price = price - sl_dist * QCfg.MIN_RR_RATIO()

        # Minimum R:R check
        tp_dist = abs(price - tp_price)
        if sl_dist > 1e-10:
            rr = tp_dist / sl_dist
            if rr < QCfg.MIN_RR_RATIO():
                # Widen TP to meet minimum R:R
                if side == "long":
                    tp_price = price + sl_dist * QCfg.MIN_RR_RATIO()
                else:
                    tp_price = price - sl_dist * QCfg.MIN_RR_RATIO()

        sl_price = _round_to_tick(sl_price)
        tp_price = _round_to_tick(tp_price)

        # Sanity check
        if side == "long" and (sl_price >= price or tp_price <= price):
            return None, None
        if side == "short" and (sl_price <= price or tp_price >= price):
            return None, None

        return sl_price, tp_price

    # ───────────────────────────────────────────────────────────────────
    # ENTER TRADE — v4
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

        sl_price, tp_price = self._compute_sl_tp(data_manager, price, side, atr)
        if sl_price is None or tp_price is None:
            return

        sl_dist = abs(price - sl_price)
        tp_dist = abs(price - tp_price)
        if sl_dist < 1e-10:
            return
        rr = tp_dist / sl_dist

        logger.info(
            f"🎯 ENTERING {side.upper()} @ ${price:,.2f} | qty={qty} | "
            f"SL=${sl_price:,.2f} TP=${tp_price:,.2f} R:R=1:{rr:.2f} | "
            f"VWAP=${sig.vwap_price:,.2f} dev={sig.deviation_atr:+.1f}ATR | {sig}"
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
            new_sl, new_tp = self._compute_sl_tp(data_manager, fill_price, side, atr)
            if new_sl is None or new_tp is None:
                exit_s = "sell" if side == "long" else "buy"
                order_manager.place_market_order(side=exit_s, quantity=qty, reduce_only=True)
                self._last_exit_time = time.time()
                return
            sl_price, tp_price = new_sl, new_tp

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
            return

        tp_data = order_manager.place_take_profit(side=exit_side, quantity=qty,
                                                   trigger_price=tp_price)
        if not tp_data:
            logger.error("❌ TP placement failed — cancelling SL, closing")
            order_manager.cancel_order(sl_data["order_id"])
            order_manager.place_market_order(side=exit_side, quantity=qty, reduce_only=True)
            self._last_exit_time = time.time()
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

        rr_actual = abs(fill_price - tp_price) / sl_dist_filled if sl_dist_filled > 0 else 0

        send_telegram_message(
            f"{'📈' if side == 'long' else '📉'} <b>QUANT v4 REVERSION — {side.upper()}</b>\n\n"
            f"Symbol:   {QCfg.SYMBOL()}\n"
            f"Entry:    ${fill_price:,.2f}\n"
            f"VWAP:     ${sig.vwap_price:,.2f} ({sig.deviation_atr:+.1f} ATR away)\n"
            f"SL:       ${sl_price:,.2f} (swing structure)\n"
            f"TP:       ${tp_price:,.2f} ({QCfg.TP_VWAP_FRACTION():.0%} to VWAP)\n"
            f"R:R:      1:{rr_actual:.2f}\n"
            f"Qty:      {qty} BTC\n"
            f"Confirm:  {sig.n_confirming}/5 signals agree\n"
        )
        logger.info(f"✅ ACTIVE {side.upper()} @ ${fill_price:,.2f} | R:R=1:{rr_actual:.2f}")

    # ───────────────────────────────────────────────────────────────────
    # ACTIVE MANAGEMENT — v4: Trail + Max Hold + Signal Exit
    # ───────────────────────────────────────────────────────────────────

    def _manage_active(self, data_manager, order_manager, now: float) -> None:
        pos = self._pos
        price = data_manager.get_last_price()
        if price < 1.0:
            return

        # Max hold time check
        hold_sec = now - pos.entry_time
        if hold_sec > QCfg.MAX_HOLD_SEC():
            logger.info(f"⏰ Max hold time ({QCfg.MAX_HOLD_SEC()}s) → exiting")
            self._exit_trade(order_manager, price, "max_hold_time")
            return

        # v4: Signal-based exit ONLY when position is significantly against us
        # Don't exit just because signal flipped mildly
        sig = self._compute_signals(data_manager)
        if sig is not None:
            self._log_thinking(sig, price, now)
            c = sig.composite
            exit_thr = QCfg.EXIT_REVERSAL_THRESH()

            # Only exit on STRONG reversal signal (not mild flip)
            if pos.side == "long" and c <= -exit_thr:
                logger.info(f"🔄 Strong reversal ({c:+.3f} ≤ -{exit_thr}) → exit LONG")
                self._exit_trade(order_manager, price, "strong_reversal")
                return
            if pos.side == "short" and c >= exit_thr:
                logger.info(f"🔄 Strong reversal ({c:+.3f} ≥ +{exit_thr}) → exit SHORT")
                self._exit_trade(order_manager, price, "strong_reversal")
                return

        # Trailing SL
        if QCfg.TRAIL_ENABLED():
            if now - pos.last_trail_time >= QCfg.TRAIL_INTERVAL_S():
                self._pos.last_trail_time = now
                closed = self._update_trailing_sl(order_manager, data_manager, price, now)
                if closed:
                    return

    def _update_trailing_sl(self, order_manager, data_manager,
                            price: float, now: float) -> bool:
        """v4: Simple, aggressive trailing. BE at 0.4R, lock at 0.8R, ratchet from there."""
        pos = self._pos
        atr = self._atr_5m.atr
        if atr < 1e-10:
            return False

        profit = ((price - pos.entry_price) if pos.side == "long"
                  else (pos.entry_price - price))
        if profit > pos.peak_profit:
            pos.peak_profit = profit

        init_dist = pos.initial_sl_dist
        tier = pos.peak_profit / init_dist if init_dist > 1e-10 else 0.0

        be_r = QCfg.TRAIL_BE_R()
        lock_r = QCfg.TRAIL_LOCK_R()

        if tier < be_r:
            return False  # Not enough profit to trail

        # Determine new SL
        if tier < lock_r:
            # Breakeven zone: move SL to entry ± small buffer
            if pos.side == "long":
                new_sl = pos.entry_price + 0.05 * atr
            else:
                new_sl = pos.entry_price - 0.05 * atr
        else:
            # Lock zone: trail behind price
            trail_dist = 0.8 * atr
            if pos.side == "long":
                new_sl = price - trail_dist
            else:
                new_sl = price + trail_dist

        new_sl_tick = _round_to_tick(new_sl)
        min_move = QCfg.TRAIL_MIN_MOVE_ATR() * atr

        # Ratchet: SL may only improve
        if pos.side == "long":
            if new_sl_tick <= pos.sl_price + min_move:
                return False
        else:
            if new_sl_tick >= pos.sl_price - min_move:
                return False

        tier_label = "🟡 BE" if tier < lock_r else "🟢 Lock"
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
            f"{'✅' if pnl > 0 else '❌'} <b>QUANT v4 EXIT — {pos.side.upper()}</b>\n\n"
            f"Reason:  {reason}\n"
            f"Entry:   ${pos.entry_price:,.2f}\n"
            f"Exit:    ${price:,.2f}\n"
            f"PnL:     ${pnl:+.2f} USDT\n"
            f"Hold:    {hold_min:.1f} min\n\n"
            f"<i>Trades: {self._total_trades} | "
            f"WR: {self._win_rate():.0%} | "
            f"PnL: ${self._total_pnl:+.2f}</i>"
        )
        self._last_exit_side = pos.side
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
        self._last_exit_side = pos.side
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
            "regime_ok": self._atr_5m.regime_valid(),
        }

    def format_status_report(self) -> str:
        stats = self.get_stats()
        pos = self._pos
        lines = [
            "📊 <b>QUANT v4 STATUS — MEAN-REVERSION</b>",
            "",
            f"Phase:       {stats['current_phase']}",
            f"Regime:      {'✅' if stats['regime_ok'] else '❌'}",
            f"ATR 5m/1m:   ${stats['atr_5m']} / ${stats['atr_1m']}  ({stats['atr_pctile']})",
            f"HTF:         15m={self._htf.trend_15m:+.3f}  4h={self._htf.trend_4h:+.3f}",
            f"VWAP:        ${self._vwap.vwap:,.2f} (dev={self._vwap.deviation_atr:+.1f} ATR)",
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

    # ───────────────────────────────────────────────────────────────────
    # RECONCILIATION (unchanged from v3 — same logic)
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

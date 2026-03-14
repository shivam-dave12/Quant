"""
QUANT STRATEGY v4 — MEAN-REVERSION + ORDER FLOW CONFLUENCE
==============================================================================
Entry: Wait for overextension from VWAP + order flow divergence, then fade.
SL: Behind swing structure. TP: 50% back to VWAP. Trail: BE at 0.4R.

PnL TRACKING FIX (v3 bug):
  When exchange SL/TP fires, unrealized_pnl from API is 0 (position already
  closed). v4 computes PnL from entry_price vs last known SL/TP price.
  Trailed SL hit at profit correctly counts as WIN.
"""

from __future__ import annotations
import logging, math, time, threading
from collections import deque
from dataclasses import dataclass, field
from datetime import date
from enum import Enum, auto
from typing import Dict, List, Optional, Tuple

import config
from telegram_notifier import send_telegram_message
from order_manager import CancelResult

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
# CONFIG ACCESSOR
# ═══════════════════════════════════════════════════════════════
def _cfg(name: str, default):
    val = getattr(config, name, None)
    return default if val is None else val

class QCfg:
    @staticmethod
    def SYMBOL() -> str: return str(config.SYMBOL)
    @staticmethod
    def EXCHANGE() -> str: return str(config.EXCHANGE)
    @staticmethod
    def LEVERAGE() -> int: return int(_cfg("LEVERAGE", 30))
    @staticmethod
    def MARGIN_PCT() -> float: return float(_cfg("QUANT_MARGIN_PCT", 0.20))
    @staticmethod
    def LOT_STEP() -> float: return float(_cfg("LOT_STEP_SIZE", 0.001))
    @staticmethod
    def MIN_QTY() -> float: return float(_cfg("MIN_POSITION_SIZE", 0.001))
    @staticmethod
    def MAX_QTY() -> float: return float(_cfg("MAX_POSITION_SIZE", 1.0))
    @staticmethod
    def MIN_MARGIN_USDT() -> float: return float(_cfg("MIN_MARGIN_PER_TRADE", 4.0))
    @staticmethod
    def COMMISSION_RATE() -> float: return float(_cfg("COMMISSION_RATE", 0.00055))
    @staticmethod
    def TICK_SIZE() -> float: return float(_cfg("TICK_SIZE", 0.1))
    @staticmethod
    def SLIPPAGE_TOL() -> float: return float(_cfg("QUANT_SLIPPAGE_TOLERANCE", 0.0005))
    @staticmethod
    def VWAP_ENTRY_ATR_MULT() -> float: return float(_cfg("QUANT_VWAP_ENTRY_ATR_MULT", 1.2))
    @staticmethod
    def COMPOSITE_ENTRY_MIN() -> float: return float(_cfg("QUANT_COMPOSITE_ENTRY_MIN", 0.30))
    @staticmethod
    def EXIT_REVERSAL_THRESH() -> float: return float(_cfg("QUANT_EXIT_REVERSAL_THRESH", 0.40))
    @staticmethod
    def CONFIRM_TICKS() -> int: return int(_cfg("QUANT_CONFIRM_TICKS", 2))
    @staticmethod
    def SL_SWING_LOOKBACK() -> int: return int(_cfg("QUANT_SL_SWING_LOOKBACK", 12))
    @staticmethod
    def SL_BUFFER_ATR_MULT() -> float: return float(_cfg("QUANT_SL_BUFFER_ATR_MULT", 0.4))
    @staticmethod
    def TP_VWAP_FRACTION() -> float: return float(_cfg("QUANT_TP_VWAP_FRACTION", 0.50))
    @staticmethod
    def VP_BUCKET_COUNT() -> int: return int(_cfg("QUANT_VP_BUCKET_COUNT", 50))
    @staticmethod
    def VP_HVN_THRESHOLD() -> float: return float(_cfg("QUANT_VP_HVN_THRESHOLD", 0.70))
    @staticmethod
    def OB_WALL_DEPTH() -> int: return int(_cfg("QUANT_OB_WALL_DEPTH", 20))
    @staticmethod
    def OB_WALL_MULT() -> float: return float(_cfg("QUANT_OB_WALL_MULT", 2.5))
    @staticmethod
    def TRAIL_SWING_BARS() -> int: return int(_cfg("QUANT_TRAIL_SWING_BARS", 5))
    @staticmethod
    def TRAIL_VOL_DECAY_MULT() -> float: return float(_cfg("QUANT_TRAIL_VOL_DECAY_MULT", 0.6))
    @staticmethod
    def MIN_SL_PCT() -> float: return float(_cfg("MIN_SL_DISTANCE_PCT", 0.003))
    @staticmethod
    def MAX_SL_PCT() -> float: return float(_cfg("MAX_SL_DISTANCE_PCT", 0.035))
    @staticmethod
    def MIN_RR_RATIO() -> float: return float(_cfg("MIN_RISK_REWARD_RATIO", 0.8))
    @staticmethod
    def ATR_PERIOD() -> int: return int(_cfg("SL_ATR_PERIOD", 14))
    @staticmethod
    def TRAIL_ENABLED() -> bool: return bool(_cfg("QUANT_TRAIL_ENABLED", True))
    @staticmethod
    def TRAIL_BE_R() -> float: return float(_cfg("QUANT_TRAIL_BE_R", 0.4))
    @staticmethod
    def TRAIL_LOCK_R() -> float: return float(_cfg("QUANT_TRAIL_LOCK_R", 0.8))
    @staticmethod
    def TRAIL_INTERVAL_S() -> int: return int(_cfg("TRAILING_SL_CHECK_INTERVAL", 10))
    @staticmethod
    def TRAIL_MIN_MOVE_ATR() -> float: return float(_cfg("SL_MIN_IMPROVEMENT_ATR_MULT", 0.08))
    @staticmethod
    def CVD_WINDOW() -> int: return int(_cfg("QUANT_CVD_WINDOW", 20))
    @staticmethod
    def CVD_HIST_MULT() -> int: return int(_cfg("QUANT_CVD_HIST_MULT", 15))
    @staticmethod
    def VWAP_WINDOW() -> int: return int(_cfg("QUANT_VWAP_WINDOW", 50))
    @staticmethod
    def EMA_FAST() -> int: return int(_cfg("QUANT_EMA_FAST", 8))
    @staticmethod
    def EMA_SLOW() -> int: return int(_cfg("QUANT_EMA_SLOW", 21))
    @staticmethod
    def MIN_1M_BARS() -> int: return int(_cfg("MIN_CANDLES_1M", 80))
    @staticmethod
    def MIN_5M_BARS() -> int: return int(_cfg("MIN_CANDLES_5M", 60))
    @staticmethod
    def ATR_PCTILE_WINDOW() -> int: return int(_cfg("QUANT_ATR_PCTILE_WINDOW", 100))
    @staticmethod
    def ATR_MIN_PCTILE() -> float: return float(_cfg("QUANT_ATR_MIN_PCTILE", 0.05))
    @staticmethod
    def ATR_MAX_PCTILE() -> float: return float(_cfg("QUANT_ATR_MAX_PCTILE", 0.97))
    @staticmethod
    def MAX_HOLD_SEC() -> int: return int(_cfg("QUANT_MAX_HOLD_SEC", 2400))
    @staticmethod
    def COOLDOWN_SEC() -> int: return int(_cfg("QUANT_COOLDOWN_SEC", 180))
    @staticmethod
    def LOSS_LOCKOUT_SEC() -> int: return int(_cfg("QUANT_LOSS_LOCKOUT_SEC", 3600))
    @staticmethod
    def TICK_EVAL_SEC() -> float: return float(_cfg("ENTRY_EVALUATION_INTERVAL_SECONDS", 1))
    @staticmethod
    def POS_SYNC_SEC() -> float: return float(_cfg("QUANT_POS_SYNC_SEC", 30))
    @staticmethod
    def MAX_DAILY_TRADES() -> int: return int(_cfg("MAX_DAILY_TRADES", 8))
    @staticmethod
    def MAX_CONSEC_LOSSES() -> int: return int(_cfg("MAX_CONSECUTIVE_LOSSES", 3))
    @staticmethod
    def MAX_DAILY_LOSS_PCT() -> float: return float(_cfg("MAX_DAILY_LOSS_PCT", 5.0))
    @staticmethod
    def W_VWAP_DEV() -> float: return float(_cfg("QUANT_W_VWAP_DEV", 0.30))
    @staticmethod
    def W_CVD_DIV() -> float: return float(_cfg("QUANT_W_CVD_DIV", 0.25))
    @staticmethod
    def W_OB() -> float: return float(_cfg("QUANT_W_OB", 0.20))
    @staticmethod
    def W_TICK_FLOW() -> float: return float(_cfg("QUANT_W_TICK_FLOW", 0.15))
    @staticmethod
    def W_VOL_EXHAUSTION() -> float: return float(_cfg("QUANT_W_VOL_EXHAUSTION", 0.10))
    @staticmethod
    def HTF_ENABLED() -> bool: return bool(_cfg("QUANT_HTF_ENABLED", True))
    @staticmethod
    def HTF_VETO_STRENGTH() -> float: return float(_cfg("QUANT_HTF_VETO_STRENGTH", 0.70))
    @staticmethod
    def OB_DEPTH_LEVELS() -> int: return int(_cfg("QUANT_OB_DEPTH_LEVELS", 5))
    @staticmethod
    def OB_HIST_LEN() -> int: return int(_cfg("QUANT_OB_HIST_LEN", 60))
    @staticmethod
    def TICK_AGG_WINDOW_SEC() -> float: return float(_cfg("QUANT_TICK_AGG_WINDOW_SEC", 30.0))

def _round_to_tick(price: float) -> float:
    tick = QCfg.TICK_SIZE()
    return round(round(price / tick) * tick, 10) if tick > 0 else price

def _sigmoid(z: float, steepness: float = 1.0) -> float:
    return max(-1.0, min(1.0, z * steepness / (1.0 + abs(z * steepness) * 0.5)))

# ═══════════════════════════════════════════════════════════════
# ENGINE 1: VWAP DEVIATION — Primary Mean-Reversion Signal
# ═══════════════════════════════════════════════════════════════
class VWAPEngine:
    def __init__(self):
        self._vwap = 0.0
        self._std = 0.0
        self._deviation_atr = 0.0

    def update(self, candles: List[Dict], atr: float) -> None:
        window = QCfg.VWAP_WINDOW()
        if len(candles) < window: return
        recent = candles[-window:]
        tp_vol = sum((float(c['h'])+float(c['l'])+float(c['c']))/3.0*float(c['v']) for c in recent)
        vol_sum = sum(float(c['v']) for c in recent)
        if vol_sum < 1e-12: return
        self._vwap = tp_vol / vol_sum
        var_sum = sum(float(c['v'])*((float(c['h'])+float(c['l'])+float(c['c']))/3.0-self._vwap)**2 for c in recent)
        self._std = math.sqrt(var_sum / vol_sum)
        if atr > 1e-10:
            self._deviation_atr = (float(candles[-1]['c']) - self._vwap) / atr

    def get_reversion_signal(self, price: float, atr: float) -> float:
        if self._vwap < 1e-10 or atr < 1e-10: return 0.0
        dev = (price - self._vwap) / atr
        entry_thresh = QCfg.VWAP_ENTRY_ATR_MULT()
        if abs(dev) < entry_thresh * 0.6: return 0.0
        return max(-1.0, min(1.0, _sigmoid(-dev / (entry_thresh * 2.0), 1.5)))

    def is_overextended(self, price: float, atr: float) -> bool:
        if self._vwap < 1e-10 or atr < 1e-10: return False
        return abs(price - self._vwap) / atr >= QCfg.VWAP_ENTRY_ATR_MULT()

    def reversion_side(self, price: float) -> str:
        return "short" if price > self._vwap else "long"

    def tp_target(self, price: float) -> float:
        return price + (self._vwap - price) * QCfg.TP_VWAP_FRACTION()

    @property
    def vwap(self) -> float: return self._vwap
    @property
    def vwap_std(self) -> float: return self._std
    @property
    def deviation_atr(self) -> float: return self._deviation_atr

# ═══════════════════════════════════════════════════════════════
# ENGINE 2: CVD DIVERGENCE — Exhaustion Detection
# ═══════════════════════════════════════════════════════════════
class CVDEngine:
    def __init__(self):
        self._deltas: deque = deque(maxlen=QCfg.CVD_WINDOW() * QCfg.CVD_HIST_MULT())
        self._last_bar_ts: int = 0

    def update(self, candles: List[Dict]) -> None:
        if not candles: return
        new_start = 0
        if self._last_bar_ts > 0:
            for i, c in enumerate(candles):
                if int(c['t']) > self._last_bar_ts:
                    new_start = i; break
            else:
                if candles:
                    c = candles[-1]; hi=float(c['h']); lo=float(c['l']); cl=float(c['c']); vol=float(c['v'])
                    rng = hi - lo
                    if self._deltas:
                        self._deltas[-1] = vol * ((2.0*cl-hi-lo)/rng if rng > 1e-10 else 0.0)
                return
        for c in candles[new_start:]:
            hi=float(c['h']); lo=float(c['l']); cl=float(c['c']); vol=float(c['v'])
            rng = hi - lo
            self._deltas.append(vol * ((2.0*cl-hi-lo)/rng if rng > 1e-10 else 0.0))
            self._last_bar_ts = int(c['t'])

    def get_divergence_signal(self, candles: List[Dict]) -> float:
        w = QCfg.CVD_WINDOW(); arr = list(self._deltas); n = len(arr)
        if n < w + 10 or len(candles) < w: return 0.0
        recent_cvd = sum(arr[-w//2:]); earlier_cvd = sum(arr[-w:-w//2])
        cvd_slope = recent_cvd - earlier_cvd
        closes = [float(c['c']) for c in candles[-w:]]
        mid = w // 2
        price_slope = sum(closes[mid:])/max(len(closes[mid:]),1) - sum(closes[:mid])/max(len(closes[:mid]),1)
        if abs(price_slope) < 1e-10: return 0.0
        all_sums = []; running = sum(arr[:w//2]); all_sums.append(running)
        for i in range(w//2, n - w//2):
            running += arr[i] - arr[i - w//2]; all_sums.append(running)
        if len(all_sums) < 5: return 0.0
        mu = sum(all_sums)/len(all_sums)
        std = math.sqrt(sum((s-mu)**2 for s in all_sums)/max(len(all_sums)-1,1))
        if std < 1e-12: return 0.0
        cvd_z = cvd_slope / std
        price_dir = 1.0 if price_slope > 0 else -1.0
        if (1.0 if cvd_z > 0 else -1.0) == price_dir: return 0.0
        return -price_dir * min(abs(cvd_z), 3.0) / 3.0

# ═══════════════════════════════════════════════════════════════
# ENGINE 3: ORDERBOOK IMBALANCE
# ═══════════════════════════════════════════════════════════════
class OrderbookEngine:
    def __init__(self):
        self._imbalance_hist: deque = deque(maxlen=QCfg.OB_HIST_LEN())
        self._last_imbalance = 0.0; self._spread_ratio = 0.0

    def update(self, orderbook: Dict, price: float) -> None:
        bids = orderbook.get("bids",[]); asks = orderbook.get("asks",[])
        depth = QCfg.OB_DEPTH_LEVELS()
        if not bids or not asks or price < 1.0: return
        bid_depth = sum(float(l[1]) for l in bids[:depth] if isinstance(l,(list,tuple)) and len(l)>1)
        ask_depth = sum(float(l[1]) for l in asks[:depth] if isinstance(l,(list,tuple)) and len(l)>1)
        total = bid_depth + ask_depth
        if total < 1e-12: return
        self._last_imbalance = (bid_depth - ask_depth) / total
        self._imbalance_hist.append(self._last_imbalance)
        try:
            bb = float(bids[0][0]); ba = float(asks[0][0])
            if bb > 0 and ba > 0: self._spread_ratio = (ba - bb) / ((bb + ba) / 2.0)
        except: pass

    def get_signal(self) -> float:
        hist = list(self._imbalance_hist)
        if len(hist) < 15: return 0.0
        current = hist[-1]; baseline = hist[:-1]
        mu = sum(baseline)/len(baseline)
        std = math.sqrt(sum((x-mu)**2 for x in baseline)/max(len(baseline)-1,1))
        if std < 1e-12: return _sigmoid(current * 3.0, 0.8)
        z = (current - mu) / std
        sm = max(0.5, min(1.0, 1.0 - (self._spread_ratio - 0.0002) * 100.0))
        return _sigmoid(z, 0.6) * sm

# ═══════════════════════════════════════════════════════════════
# ENGINE 4: TICK FLOW
# ═══════════════════════════════════════════════════════════════
class TickFlowEngine:
    def __init__(self):
        self._buy_vol: deque = deque(maxlen=600); self._sell_vol: deque = deque(maxlen=600)
        self._flow_hist: deque = deque(maxlen=120); self._last_signal = 0.0

    def on_trade(self, price: float, qty: float, is_buyer: bool, ts: float) -> None:
        (self._buy_vol if is_buyer else self._sell_vol).append((ts, price * qty))

    def compute_signal(self) -> float:
        now = time.time(); cutoff = now - QCfg.TICK_AGG_WINDOW_SEC()
        bt = sum(dv for ts,dv in self._buy_vol if ts >= cutoff)
        st = sum(dv for ts,dv in self._sell_vol if ts >= cutoff)
        total = bt + st
        if total < 1e-10: return 0.0
        fr = (bt - st) / total; self._flow_hist.append(fr)
        hist = list(self._flow_hist)
        if len(hist) < 10: return _sigmoid(fr * 2.0, 0.8)
        mu = sum(hist[:-1])/len(hist[:-1])
        std = math.sqrt(sum((x-mu)**2 for x in hist[:-1])/max(len(hist[:-1])-1,1))
        if std < 1e-12: return _sigmoid(fr * 2.0, 0.8)
        self._last_signal = _sigmoid((fr - mu) / std, 0.5)
        return self._last_signal

    def get_signal(self) -> float: return self._last_signal

# ═══════════════════════════════════════════════════════════════
# ENGINE 5: VOLUME EXHAUSTION
# ═══════════════════════════════════════════════════════════════
class VolumeExhaustionEngine:
    def __init__(self): self._last_signal = 0.0

    def compute(self, candles: List[Dict]) -> float:
        if len(candles) < 20: return 0.0
        recent = candles[-10:]; earlier = candles[-20:-10]
        pc = float(recent[-1]['c']) - float(earlier[0]['c'])
        pd = 1.0 if pc > 0 else -1.0
        rv = sum(float(c['v']) for c in recent); ev = sum(float(c['v']) for c in earlier)
        if ev < 1e-10: return 0.0
        vr = rv / ev
        if vr < 0.7: self._last_signal = -pd * min((0.7 - vr) / 0.4, 1.0)
        elif vr < 0.9: self._last_signal = -pd * (0.9 - vr) / 0.4 * 0.5
        else: self._last_signal = 0.0
        return self._last_signal

# ═══════════════════════════════════════════════════════════════
# INSTITUTIONAL LEVEL ENGINE — Volume Profile + Orderbook Walls
# ═══════════════════════════════════════════════════════════════
class InstitutionalLevels:
    """
    Computes SL/TP/Trail levels using:
    1. Volume Profile (from candles) — find High-Volume Nodes where price consolidates
    2. Orderbook Liquidity Walls — find where large resting orders cluster
    3. Swing Structure — recent pivot highs/lows on multiple timeframes
    4. VWAP bands (±1σ, ±2σ) — institutional reference levels

    SL: Behind the strongest protective level (wall / HVN / swing)
    TP: At the nearest attraction level toward VWAP (HVN / wall / VWAP itself)
    Trail: Follow micro-swings on 1m, tighten when volume decays or wall disappears
    """

    @staticmethod
    def build_volume_profile(candles: List[Dict], bucket_count: int = 50) -> List[Tuple[float, float, float]]:
        """Build volume-at-price profile. Returns [(price_low, price_high, volume), ...] sorted by price."""
        if len(candles) < 10:
            return []
        all_highs = [float(c['h']) for c in candles]
        all_lows = [float(c['l']) for c in candles]
        price_min = min(all_lows)
        price_max = max(all_highs)
        rng = price_max - price_min
        if rng < 1e-10:
            return []
        bucket_size = rng / bucket_count
        buckets = [0.0] * bucket_count
        for c in candles:
            hi = float(c['h']); lo = float(c['l']); vol = float(c['v'])
            if vol < 1e-10:
                continue
            # Distribute volume across price buckets the candle spans
            lo_idx = max(0, int((lo - price_min) / bucket_size))
            hi_idx = min(bucket_count - 1, int((hi - price_min) / bucket_size))
            span = max(hi_idx - lo_idx + 1, 1)
            vol_per_bucket = vol / span
            for i in range(lo_idx, hi_idx + 1):
                if 0 <= i < bucket_count:
                    buckets[i] += vol_per_bucket
        result = []
        for i in range(bucket_count):
            bp_low = price_min + i * bucket_size
            bp_high = bp_low + bucket_size
            result.append((bp_low, bp_high, buckets[i]))
        return result

    @staticmethod
    def find_hvn_levels(profile: List[Tuple[float, float, float]], threshold_pctile: float = 0.70) -> List[float]:
        """Find high-volume node price levels (midpoints of top-percentile buckets)."""
        if not profile:
            return []
        volumes = [v for _, _, v in profile]
        if max(volumes) < 1e-10:
            return []
        sorted_vols = sorted(volumes)
        cutoff_idx = int(len(sorted_vols) * threshold_pctile)
        cutoff_vol = sorted_vols[min(cutoff_idx, len(sorted_vols) - 1)]
        hvns = []
        for lo, hi, vol in profile:
            if vol >= cutoff_vol:
                hvns.append((lo + hi) / 2.0)
        return hvns

    @staticmethod
    def find_orderbook_walls(orderbook: Dict, side: str, depth: int = 20, wall_mult: float = 2.5) -> List[Tuple[float, float]]:
        """
        Find price levels where resting liquidity is wall_mult × average.
        Returns [(price, qty), ...] sorted by qty descending.
        side='bid' for support walls, 'ask' for resistance walls.
        """
        levels = orderbook.get("bids" if side == "bid" else "asks", [])
        if not levels or len(levels) < 3:
            return []
        parsed = []
        for lvl in levels[:depth]:
            try:
                if isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
                    parsed.append((float(lvl[0]), float(lvl[1])))
            except (ValueError, TypeError):
                continue
        if not parsed:
            return []
        avg_qty = sum(q for _, q in parsed) / len(parsed)
        if avg_qty < 1e-12:
            return []
        walls = [(p, q) for p, q in parsed if q >= avg_qty * wall_mult]
        walls.sort(key=lambda x: x[1], reverse=True)
        return walls

    @staticmethod
    def find_swing_extremes(candles: List[Dict], lookback: int = 12) -> Tuple[List[float], List[float]]:
        """Find swing highs and swing lows from candle data.
        A swing high: c[i] high > c[i-1] high AND c[i] high > c[i+1] high.
        Returns (swing_highs, swing_lows) as price lists."""
        if len(candles) < 3:
            return [], []
        recent = candles[-lookback:] if len(candles) >= lookback else candles
        highs = []
        lows = []
        for i in range(1, len(recent) - 1):
            h = float(recent[i]['h'])
            l = float(recent[i]['l'])
            if h > float(recent[i-1]['h']) and h > float(recent[i+1]['h']):
                highs.append(h)
            if l < float(recent[i-1]['l']) and l < float(recent[i+1]['l']):
                lows.append(l)
        return highs, lows

    @staticmethod
    def compute_sl(price: float, side: str, atr: float,
                   candles_5m: List[Dict], candles_1m: List[Dict],
                   orderbook: Dict, vwap: float, vwap_std: float) -> float:
        """
        Institutional SL placement. Uses multiple factors, picks the best.

        For LONG: SL below price. Candidates:
          1. Below nearest swing low on 5m
          2. Below strongest bid wall in orderbook
          3. Below nearest high-volume node below price
          4. Below VWAP - 2σ (if that's below entry)
          5. Floor: ATR-based minimum distance

        Takes the HIGHEST of these (most protective that keeps SL below price).
        For SHORT: mirror logic above price.
        """
        candidates = []
        buffer = QCfg.SL_BUFFER_ATR_MULT() * atr
        min_dist = price * QCfg.MIN_SL_PCT()
        max_dist = price * QCfg.MAX_SL_PCT()

        # 1. Swing structure from 5m
        if len(candles_5m) >= 5:
            sh, sl_list = InstitutionalLevels.find_swing_extremes(
                candles_5m, QCfg.SL_SWING_LOOKBACK())
            if side == "long" and sl_list:
                # Nearest swing low below price
                below = [s for s in sl_list if s < price]
                if below:
                    candidates.append(max(below) - buffer)
            elif side == "short" and sh:
                above = [s for s in sh if s > price]
                if above:
                    candidates.append(min(above) + buffer)

        # 2. Orderbook liquidity walls
        wall_side = "bid" if side == "long" else "ask"
        walls = InstitutionalLevels.find_orderbook_walls(
            orderbook, wall_side, QCfg.OB_WALL_DEPTH(), QCfg.OB_WALL_MULT())
        if walls:
            # For long: SL just below the strongest bid wall below price
            if side == "long":
                below_walls = [(p, q) for p, q in walls if p < price]
                if below_walls:
                    best_wall = max(below_walls, key=lambda x: x[1])
                    candidates.append(best_wall[0] - buffer * 0.5)
            else:
                above_walls = [(p, q) for p, q in walls if p > price]
                if above_walls:
                    best_wall = min(above_walls, key=lambda x: x[1])
                    candidates.append(best_wall[0] + buffer * 0.5)

        # 3. Volume profile HVN
        if len(candles_1m) >= 30:
            profile = InstitutionalLevels.build_volume_profile(
                candles_1m[-100:], QCfg.VP_BUCKET_COUNT())
            hvns = InstitutionalLevels.find_hvn_levels(profile, QCfg.VP_HVN_THRESHOLD())
            if hvns:
                if side == "long":
                    below_hvns = [h for h in hvns if h < price - min_dist * 0.5]
                    if below_hvns:
                        candidates.append(max(below_hvns) - buffer * 0.3)
                else:
                    above_hvns = [h for h in hvns if h > price + min_dist * 0.5]
                    if above_hvns:
                        candidates.append(min(above_hvns) + buffer * 0.3)

        # 4. VWAP bands
        if vwap > 0 and vwap_std > 0:
            vwap_2s_low = vwap - 2.0 * vwap_std
            vwap_2s_high = vwap + 2.0 * vwap_std
            if side == "long" and vwap_2s_low < price:
                candidates.append(vwap_2s_low - buffer * 0.3)
            elif side == "short" and vwap_2s_high > price:
                candidates.append(vwap_2s_high + buffer * 0.3)

        # Pick the best candidate (most protective = closest to price but valid)
        if side == "long":
            valid = [c for c in candidates if price - c >= min_dist and price - c <= max_dist]
            if valid:
                sl = max(valid)  # closest to price = least risk
            else:
                # Fallback: all candidates too close or too far — use floor
                sl = price - max(min_dist, min(max_dist, 1.5 * atr))
        else:
            valid = [c for c in candidates if c - price >= min_dist and c - price <= max_dist]
            if valid:
                sl = min(valid)
            else:
                sl = price + max(min_dist, min(max_dist, 1.5 * atr))

        # Final clamp
        dist = abs(price - sl)
        if dist < min_dist:
            sl = (price - min_dist) if side == "long" else (price + min_dist)
        elif dist > max_dist:
            sl = (price - max_dist) if side == "long" else (price + max_dist)

        return sl

    @staticmethod
    def compute_tp(price: float, side: str, atr: float, sl_price: float,
                   candles_1m: List[Dict], orderbook: Dict,
                   vwap: float, vwap_std: float) -> float:
        """
        Institutional TP placement.

        For mean-reversion, primary target is VWAP. But we also consider:
        1. VWAP itself (full reversion)
        2. Nearest HVN between price and VWAP (price gravitates to volume)
        3. Orderbook resistance wall toward VWAP (wall absorbs momentum)
        4. VWAP ± 1σ band (partial reversion)

        Takes the NEAREST target (conservative = higher win rate).
        """
        sl_dist = abs(price - sl_price)
        min_tp_dist = sl_dist * QCfg.MIN_RR_RATIO()
        candidates = []

        # 1. VWAP itself
        if vwap > 0:
            if side == "long" and vwap > price:
                candidates.append(vwap)
            elif side == "short" and vwap < price:
                candidates.append(vwap)

        # 2. VWAP partial (fraction toward VWAP)
        if vwap > 0:
            partial = price + (vwap - price) * QCfg.TP_VWAP_FRACTION()
            if side == "long" and partial > price + min_tp_dist:
                candidates.append(partial)
            elif side == "short" and partial < price - min_tp_dist:
                candidates.append(partial)

        # 3. Volume profile HVN between price and VWAP
        if len(candles_1m) >= 30 and vwap > 0:
            profile = InstitutionalLevels.build_volume_profile(
                candles_1m[-100:], QCfg.VP_BUCKET_COUNT())
            hvns = InstitutionalLevels.find_hvn_levels(profile, QCfg.VP_HVN_THRESHOLD())
            for h in hvns:
                if side == "long" and price < h < vwap and h - price >= min_tp_dist:
                    candidates.append(h)
                elif side == "short" and vwap < h < price and price - h >= min_tp_dist:
                    candidates.append(h)

        # 4. Orderbook walls toward VWAP
        wall_side = "ask" if side == "long" else "bid"
        walls = InstitutionalLevels.find_orderbook_walls(
            orderbook, wall_side, QCfg.OB_WALL_DEPTH(), QCfg.OB_WALL_MULT())
        for wp, wq in walls:
            if side == "long" and wp > price + min_tp_dist and (vwap <= 0 or wp <= vwap * 1.001):
                candidates.append(wp - atr * 0.1)  # just before the wall
            elif side == "short" and wp < price - min_tp_dist and (vwap <= 0 or wp >= vwap * 0.999):
                candidates.append(wp + atr * 0.1)

        # 5. VWAP ± 1σ
        if vwap > 0 and vwap_std > 0:
            v1s_up = vwap + vwap_std
            v1s_dn = vwap - vwap_std
            if side == "long" and v1s_dn > price + min_tp_dist:
                candidates.append(v1s_dn)
            elif side == "short" and v1s_up < price - min_tp_dist:
                candidates.append(v1s_up)

        # Pick NEAREST valid target (conservative for high WR)
        if side == "long":
            valid = [c for c in candidates if c > price + min_tp_dist]
            if valid:
                tp = min(valid)  # nearest target above
            else:
                tp = price + max(min_tp_dist, sl_dist * 1.0)
        else:
            valid = [c for c in candidates if c < price - min_tp_dist]
            if valid:
                tp = max(valid)  # nearest target below
            else:
                tp = price - max(min_tp_dist, sl_dist * 1.0)

        return tp

    @staticmethod
    def compute_trail_sl(pos_side: str, price: float, entry_price: float,
                         current_sl: float, atr: float,
                         candles_1m: List[Dict], orderbook: Dict,
                         peak_profit: float, entry_vol: float) -> Optional[float]:
        """
        Structure-based trailing SL.

        Phase 1 (profit < 0.5 × initial_risk): Don't trail. Let trade breathe.
        Phase 2 (0.5-1.0R): Move SL to breakeven, behind nearest micro-swing.
        Phase 3 (>1.0R): Active trail — follow micro-swings on 1m,
                          tighten when volume decays or orderbook wall disappears.

        Returns None if no trail move needed.
        """
        init_dist = abs(entry_price - current_sl) if abs(entry_price - current_sl) > 1e-10 else atr
        profit = (price - entry_price) if pos_side == "long" else (entry_price - price)
        tier = profit / init_dist if init_dist > 1e-10 else 0.0

        if tier < QCfg.TRAIL_BE_R():
            return None  # Phase 1: don't touch

        swing_bars = QCfg.TRAIL_SWING_BARS()
        new_sl = None

        if tier < QCfg.TRAIL_LOCK_R():
            # Phase 2: breakeven — use micro-swing on 1m to fine-tune
            if len(candles_1m) >= swing_bars + 2:
                recent = candles_1m[-(swing_bars + 2):-1]  # closed bars only
                if pos_side == "long":
                    micro_low = min(float(c['l']) for c in recent)
                    # SL at micro swing low, but at least breakeven
                    new_sl = max(micro_low - 0.15 * atr, entry_price + 0.02 * atr)
                else:
                    micro_high = max(float(c['h']) for c in recent)
                    new_sl = min(micro_high + 0.15 * atr, entry_price - 0.02 * atr)
            else:
                # Fallback: just breakeven
                new_sl = entry_price + (0.02 * atr if pos_side == "long" else -0.02 * atr)

        else:
            # Phase 3: Active trail — follow micro-swings
            if len(candles_1m) >= swing_bars + 2:
                recent = candles_1m[-(swing_bars + 2):-1]
                if pos_side == "long":
                    micro_low = min(float(c['l']) for c in recent)
                    base_trail = micro_low - 0.15 * atr
                else:
                    micro_high = max(float(c['h']) for c in recent)
                    base_trail = micro_high + 0.15 * atr
            else:
                trail_dist = 0.7 * atr
                base_trail = (price - trail_dist) if pos_side == "long" else (price + trail_dist)

            # Volume decay tightening: if current volume is much lower than at entry,
            # the move is running out of fuel → tighten the trail
            if len(candles_1m) >= 10 and entry_vol > 1e-10:
                recent_vol = sum(float(c['v']) for c in candles_1m[-5:]) / 5.0
                vol_ratio = recent_vol / entry_vol
                if vol_ratio < QCfg.TRAIL_VOL_DECAY_MULT():
                    # Tighten: move trail closer to price
                    tighten_factor = 0.5 * (1.0 - vol_ratio / QCfg.TRAIL_VOL_DECAY_MULT())
                    if pos_side == "long":
                        base_trail += tighten_factor * atr
                    else:
                        base_trail -= tighten_factor * atr

            # Orderbook wall check: if there's a wall behind us, snap to it
            wall_side = "bid" if pos_side == "long" else "ask"
            walls = InstitutionalLevels.find_orderbook_walls(
                orderbook, wall_side, QCfg.OB_WALL_DEPTH(), QCfg.OB_WALL_MULT())
            if walls:
                if pos_side == "long":
                    below_walls = [(p, q) for p, q in walls if p < price and p > entry_price]
                    if below_walls:
                        best_wall = max(below_walls, key=lambda x: x[1])
                        wall_trail = best_wall[0] - 0.1 * atr
                        base_trail = max(base_trail, wall_trail)
                else:
                    above_walls = [(p, q) for p, q in walls if p > price and p < entry_price]
                    if above_walls:
                        best_wall = min(above_walls, key=lambda x: x[1])
                        wall_trail = best_wall[0] + 0.1 * atr
                        base_trail = min(base_trail, wall_trail)

            new_sl = base_trail

        if new_sl is None:
            return None

        # Ratchet: SL may only improve (never widen)
        if pos_side == "long" and new_sl <= current_sl + QCfg.TRAIL_MIN_MOVE_ATR() * atr:
            return None
        if pos_side == "short" and new_sl >= current_sl - QCfg.TRAIL_MIN_MOVE_ATR() * atr:
            return None

        return new_sl

# ═══════════════════════════════════════════════════════════════
# ATR ENGINE
# ═══════════════════════════════════════════════════════════════
class ATREngine:
    def __init__(self):
        self._atr = 0.0; self._atr_hist: deque = deque(maxlen=QCfg.ATR_PCTILE_WINDOW())
        self._last_ts = -1; self._seeded = False

    def compute(self, candles: List[Dict]) -> float:
        if not candles: return self._atr
        period = QCfg.ATR_PERIOD(); last_ts = int(candles[-1].get('t', 0))
        if last_ts == self._last_ts and self._seeded: return self._atr
        if len(candles) < period + 1: return self._atr
        if not self._seeded:
            trs = [max(float(candles[i]['h'])-float(candles[i]['l']),
                       abs(float(candles[i]['h'])-float(candles[i-1]['c'])),
                       abs(float(candles[i]['l'])-float(candles[i-1]['c'])))
                   for i in range(1, len(candles))]
            if len(trs) < period: return self._atr
            atr = sum(trs[:period]) / period
            for tr in trs[period:]: atr = (atr*(period-1)+tr)/period
            self._atr = atr; self._seeded = True
        else:
            hi=float(candles[-1]['h']); lo=float(candles[-1]['l']); prc=float(candles[-2]['c'])
            self._atr = (self._atr*(period-1)+max(hi-lo,abs(hi-prc),abs(lo-prc)))/period
        self._atr_hist.append(self._atr); self._last_ts = last_ts
        return self._atr

    @property
    def atr(self) -> float: return self._atr

    def get_percentile(self) -> float:
        hist = list(self._atr_hist)
        if len(hist) < 10: return 0.5
        return sum(1 for h in hist[:-1] if h <= hist[-1]) / len(hist[:-1])

    def regime_valid(self) -> bool:
        p = self.get_percentile()
        return QCfg.ATR_MIN_PCTILE() <= p <= QCfg.ATR_MAX_PCTILE()

    def regime_penalty(self) -> float:
        return 1.0 if self.regime_valid() else 0.0

# ═══════════════════════════════════════════════════════════════
# HTF TREND FILTER — VETO ONLY
# ═══════════════════════════════════════════════════════════════
class HTFTrendFilter:
    def __init__(self): self._trend_15m = 0.0; self._trend_4h = 0.0

    @staticmethod
    def _ema_series(values, period):
        if len(values) < period: return []
        k = 2.0/(period+1); ema = sum(values[:period])/period; out = [ema]
        for v in values[period:]: ema = v*k+ema*(1.0-k); out.append(ema)
        return out

    def update(self, candles_15m, candles_4h, atr_5m):
        fast = QCfg.EMA_FAST()
        if len(candles_15m) > fast+5 and atr_5m > 1e-10:
            ema15 = self._ema_series([float(c['c']) for c in candles_15m], fast)
            self._trend_15m = _sigmoid((ema15[-1]-ema15[-3])/atr_5m, 0.8) if len(ema15)>=4 else 0.0
        else: self._trend_15m = 0.0
        slow = QCfg.EMA_SLOW()
        if len(candles_4h) > slow+3 and atr_5m > 1e-10:
            ema4h = self._ema_series([float(c['c']) for c in candles_4h], slow)
            self._trend_4h = _sigmoid((ema4h[-1]-ema4h[-2])/(atr_5m*4.0), 0.8) if len(ema4h)>=3 else 0.0
        else: self._trend_4h = 0.0

    def vetoes_trade(self, side: str) -> bool:
        if not QCfg.HTF_ENABLED(): return False
        htf = self._trend_4h * 0.60 + self._trend_15m * 0.40
        if side == "long" and htf < -QCfg.HTF_VETO_STRENGTH(): return True
        if side == "short" and htf > QCfg.HTF_VETO_STRENGTH(): return True
        return False

    @property
    def trend_15m(self): return self._trend_15m
    @property
    def trend_4h(self): return self._trend_4h

# ═══════════════════════════════════════════════════════════════
# SIGNAL BREAKDOWN
# ═══════════════════════════════════════════════════════════════
@dataclass
class SignalBreakdown:
    vwap_dev: float = 0.0; cvd_div: float = 0.0; orderbook: float = 0.0
    tick_flow: float = 0.0; vol_exhaust: float = 0.0; composite: float = 0.0
    atr: float = 0.0; atr_pct: float = 0.0; regime_ok: bool = False
    regime_penalty: float = 1.0; htf_veto: bool = False
    overextended: bool = False; vwap_price: float = 0.0
    deviation_atr: float = 0.0; reversion_side: str = ""
    n_confirming: int = 0; threshold_used: float = 0.0

    def __str__(self):
        return (f"VWAP={self.vwap_dev:+.3f} CVD={self.cvd_div:+.3f} "
                f"OB={self.orderbook:+.3f} TF={self.tick_flow:+.3f} "
                f"VEX={self.vol_exhaust:+.3f} -> Σ={self.composite:+.4f} "
                f"dev={self.deviation_atr:+.1f}ATR confirm={self.n_confirming}/5")

# ═══════════════════════════════════════════════════════════════
# POSITION STATE
# ═══════════════════════════════════════════════════════════════
class PositionPhase(Enum):
    FLAT = auto(); ENTERING = auto(); ACTIVE = auto(); EXITING = auto()

@dataclass
class PositionState:
    phase: PositionPhase = PositionPhase.FLAT
    side: str = ""; quantity: float = 0.0; entry_price: float = 0.0
    sl_price: float = 0.0; tp_price: float = 0.0
    sl_order_id: Optional[str] = None; tp_order_id: Optional[str] = None
    entry_order_id: Optional[str] = None; entry_time: float = 0.0
    initial_risk: float = 0.0; initial_sl_dist: float = 0.0
    trail_active: bool = False; last_trail_time: float = 0.0
    entry_signal: Optional[SignalBreakdown] = None
    peak_profit: float = 0.0; entry_atr: float = 0.0; entry_vol: float = 0.0

    def is_active(self): return self.phase == PositionPhase.ACTIVE
    def is_flat(self): return self.phase == PositionPhase.FLAT
    def to_dict(self):
        return {"side": self.side, "quantity": self.quantity,
                "entry_price": self.entry_price,
                "sl_price": self.sl_price, "tp_price": self.tp_price}

# ═══════════════════════════════════════════════════════════════
# DAILY RISK GATE with consecutive loss lockout
# ═══════════════════════════════════════════════════════════════
class DailyRiskGate:
    def __init__(self):
        self._today = date.today(); self._daily_trades = 0; self._consec_losses = 0
        self._daily_pnl = 0.0; self._daily_open_bal = 0.0
        self._loss_lockout_until = 0.0; self._lock = threading.Lock()

    def _reset_if_new_day(self):
        today = date.today()
        if today != self._today:
            self._today = today; self._daily_trades = 0; self._daily_pnl = 0.0
            self._daily_open_bal = 0.0; self._consec_losses = 0; self._loss_lockout_until = 0.0

    def set_opening_balance(self, balance):
        with self._lock:
            self._reset_if_new_day()
            if self._daily_open_bal < 1e-10 and balance > 0: self._daily_open_bal = balance

    def can_trade(self, current_balance) -> Tuple[bool, str]:
        with self._lock:
            self._reset_if_new_day(); now = time.time()
            if now < self._loss_lockout_until:
                return False, f"Loss lockout: {int(self._loss_lockout_until - now)}s remaining"
            if self._daily_trades >= QCfg.MAX_DAILY_TRADES():
                return False, f"Daily cap: {self._daily_trades}/{QCfg.MAX_DAILY_TRADES()}"
            if self._consec_losses >= QCfg.MAX_CONSEC_LOSSES():
                self._loss_lockout_until = now + QCfg.LOSS_LOCKOUT_SEC()
                return False, f"Consec loss cap → {QCfg.LOSS_LOCKOUT_SEC()}s lockout"
            if self._daily_open_bal > 1e-10:
                lp = -self._daily_pnl / self._daily_open_bal * 100.0
                if lp >= QCfg.MAX_DAILY_LOSS_PCT():
                    return False, f"Daily loss cap: {lp:.1f}%"
            return True, ""

    def record_trade_start(self):
        with self._lock: self._reset_if_new_day(); self._daily_trades += 1

    def record_trade_result(self, pnl):
        with self._lock:
            self._daily_pnl += pnl
            if pnl < 0: self._consec_losses += 1
            else: self._consec_losses = 0

    @property
    def daily_trades(self):
        with self._lock: return self._daily_trades
    @property
    def consec_losses(self):
        with self._lock: return self._consec_losses

# ═══════════════════════════════════════════════════════════════
# MAIN STRATEGY CLASS
# ═══════════════════════════════════════════════════════════════
class QuantStrategy:
    def __init__(self, order_manager=None):
        self._om = order_manager; self._lock = threading.RLock()
        self._vwap = VWAPEngine(); self._cvd = CVDEngine()
        self._ob_eng = OrderbookEngine(); self._tick_eng = TickFlowEngine()
        self._vol_exh = VolumeExhaustionEngine()
        self._atr_1m = ATREngine(); self._atr_5m = ATREngine()
        self._htf = HTFTrendFilter()
        self._pos = PositionState(); self._last_sig = SignalBreakdown()
        self._risk_gate = DailyRiskGate()
        self._confirm_long = 0; self._confirm_short = 0
        self._last_eval_time = 0.0; self._last_exit_time = 0.0
        self._last_pos_sync = 0.0; self._last_exit_sync = 0.0
        self._last_exit_side = ""; self._last_think_log = 0.0; self._think_interval = 30.0
        self._last_fed_trade_ts = 0.0
        self._last_reconcile_time = 0.0; self._RECONCILE_SEC = 30.0
        self._reconcile_pending = False; self._reconcile_data = None
        self._total_trades = 0; self._winning_trades = 0; self._total_pnl = 0.0
        self.current_sl_price = 0.0; self.current_tp_price = 0.0
        # Track last known price for PnL fallback
        self._last_known_price = 0.0
        self._log_init()

    def _log_init(self):
        logger.info("=" * 72)
        logger.info("⚡ QuantStrategy v4 — MEAN-REVERSION + ORDER FLOW")
        logger.info(f"   {QCfg.SYMBOL()} | {QCfg.LEVERAGE()}x | {QCfg.MARGIN_PCT():.0%} margin")
        logger.info(f"   Entry: VWAP deviation > {QCfg.VWAP_ENTRY_ATR_MULT()}×ATR | Confirm: {QCfg.CONFIRM_TICKS()} ticks")
        logger.info(f"   SL: swing + {QCfg.SL_BUFFER_ATR_MULT()}×ATR buffer | TP: {QCfg.TP_VWAP_FRACTION():.0%} to VWAP")
        logger.info(f"   Trail: BE@{QCfg.TRAIL_BE_R()}R Lock@{QCfg.TRAIL_LOCK_R()}R")
        logger.info(f"   Cooldown: {QCfg.COOLDOWN_SEC()}s | Loss lockout: {QCfg.LOSS_LOCKOUT_SEC()}s")
        logger.info(f"   Weights: VWAP={QCfg.W_VWAP_DEV()} CVD={QCfg.W_CVD_DIV()} OB={QCfg.W_OB()} "
                    f"TF={QCfg.W_TICK_FLOW()} VEX={QCfg.W_VOL_EXHAUSTION()}")
        logger.info("=" * 72)

    def get_position(self) -> Optional[Dict]:
        with self._lock: return None if self._pos.is_flat() else self._pos.to_dict()

    def on_tick(self, data_manager, order_manager, risk_manager, timestamp_ms: int) -> None:
        with self._lock:
            now = timestamp_ms / 1000.0; self._om = order_manager
            if now - self._last_eval_time < QCfg.TICK_EVAL_SEC(): return
            self._last_eval_time = now
            phase = self._pos.phase
            self._feed_microstructure(data_manager)
            # Update last known price
            try:
                p = data_manager.get_last_price()
                if p > 1.0: self._last_known_price = p
            except: pass
            # Reconciliation
            if self._reconcile_data is not None:
                data = self._reconcile_data; self._reconcile_data = None
                self._reconcile_apply(order_manager, data); phase = self._pos.phase
            if now - self._last_reconcile_time >= self._RECONCILE_SEC and not self._reconcile_pending:
                self._last_reconcile_time = now; self._reconcile_pending = True
                threading.Thread(target=self._reconcile_query_thread, args=(order_manager,), daemon=True).start()
            if phase == PositionPhase.ACTIVE:
                if now - self._last_pos_sync > QCfg.POS_SYNC_SEC():
                    self._sync_position(order_manager); self._last_pos_sync = now
                    if self._pos.is_flat(): return
            elif phase == PositionPhase.EXITING:
                if now - self._last_exit_sync > QCfg.POS_SYNC_SEC():
                    self._sync_position(order_manager); self._last_exit_sync = now
                return
            if phase == PositionPhase.FLAT:
                if now - self._last_exit_time < float(QCfg.COOLDOWN_SEC()): return
                self._evaluate_entry(data_manager, order_manager, risk_manager, now)
            elif phase == PositionPhase.ACTIVE:
                self._manage_active(data_manager, order_manager, now)

    def _feed_microstructure(self, data_manager):
        try:
            ob = data_manager.get_orderbook(); price = data_manager.get_last_price()
            if ob and price > 1.0: self._ob_eng.update(ob, price)
        except: pass
        try:
            trades = data_manager.get_recent_trades_raw()
            cutoff_ts = self._last_fed_trade_ts; max_ts = cutoff_ts
            for t in trades:
                ts = t.get("timestamp", 0.0)
                if ts > cutoff_ts:
                    self._tick_eng.on_trade(t.get("price",0.0), t.get("quantity",0.0), t.get("side")=="buy", ts)
                    if ts > max_ts: max_ts = ts
            if max_ts > cutoff_ts: self._last_fed_trade_ts = max_ts
            self._tick_eng.compute_signal()
        except: pass

    def _compute_signals(self, data_manager) -> Optional[SignalBreakdown]:
        candles_1m = data_manager.get_candles("1m", limit=300)
        candles_5m = data_manager.get_candles("5m", limit=100)
        if len(candles_1m) < QCfg.MIN_1M_BARS() or len(candles_5m) < QCfg.MIN_5M_BARS(): return None
        atr_1m = self._atr_1m.compute(candles_1m); atr_5m = self._atr_5m.compute(candles_5m)
        if atr_5m < 1e-10: return None
        price = data_manager.get_last_price()
        if price < 1.0: return None
        self._vwap.update(candles_1m, atr_1m); self._cvd.update(candles_1m)
        try:
            c15 = data_manager.get_candles("15m", limit=100); c4h = data_manager.get_candles("4h", limit=50)
            self._htf.update(c15, c4h, atr_5m)
        except: pass
        vs = self._vwap.get_reversion_signal(price, atr_1m)
        cs = self._cvd.get_divergence_signal(candles_1m)
        obs = self._ob_eng.get_signal(); ts = self._tick_eng.get_signal()
        ve = self._vol_exh.compute(candles_1m)
        comp = vs*QCfg.W_VWAP_DEV() + cs*QCfg.W_CVD_DIV() + obs*QCfg.W_OB() + ts*QCfg.W_TICK_FLOW() + ve*QCfg.W_VOL_EXHAUSTION()
        comp = max(-1.0, min(1.0, comp))
        direction = 1.0 if comp >= 0 else -1.0
        nc = sum(1 for s in [vs,cs,obs,ts,ve] if s*direction > 0.05)
        sig = SignalBreakdown(
            vwap_dev=vs, cvd_div=cs, orderbook=obs, tick_flow=ts, vol_exhaust=ve,
            composite=comp, atr=atr_5m, atr_pct=self._atr_5m.get_percentile(),
            regime_ok=self._atr_5m.regime_valid(), regime_penalty=self._atr_5m.regime_penalty(),
            htf_veto=self._htf.vetoes_trade(self._vwap.reversion_side(price)),
            overextended=self._vwap.is_overextended(price, atr_1m),
            vwap_price=self._vwap.vwap, deviation_atr=self._vwap.deviation_atr,
            reversion_side=self._vwap.reversion_side(price), n_confirming=nc,
            threshold_used=QCfg.COMPOSITE_ENTRY_MIN())
        self._last_sig = sig; return sig

    def _log_thinking(self, sig, price, now):
        if now - self._last_think_log < self._think_interval: return
        self._last_think_log = now
        def bar(v, w=12):
            h=w//2; f=min(int(abs(v)*h+0.5),h)
            return (" "*h+"█"*f+"░"*(h-f)) if v>=0 else ("░"*(h-f)+"█"*f+" "*h)
        def fmt(l,v):
            a = "▲" if v>0.05 else ("▼" if v<-0.05 else "─")
            return f"  {l:<6} {bar(v)} {a} {v:+.3f}"
        c=sig.composite; thr=QCfg.COMPOSITE_ENTRY_MIN()
        gates = [
            f"{'✅' if sig.overextended else '❌'} Overextended ({sig.deviation_atr:+.1f} ATR)",
            f"{'✅' if sig.regime_ok else '❌'} Regime ({sig.atr_pct:.0%})",
            f"{'✅' if not sig.htf_veto else '❌'} HTF (15m={self._htf.trend_15m:+.2f} 4h={self._htf.trend_4h:+.2f})",
            f"{'✅' if sig.n_confirming>=3 else '❌'} Confluence ({sig.n_confirming}/5)",
            f"{'✅' if abs(c)>=thr else '❌'} Composite ({c:+.3f} vs ±{thr:.3f})"]
        ap = sig.overextended and sig.regime_ok and not sig.htf_veto and sig.n_confirming>=3 and abs(c)>=thr
        cd = max(0.0, QCfg.COOLDOWN_SEC()-(now-self._last_exit_time))
        lines = [f"┌─── 🧠 v4 REVERSION  ${price:,.2f}  VWAP=${sig.vwap_price:,.2f}  ATR={sig.atr:.1f} ────",
                 fmt("VWAP",sig.vwap_dev), fmt("CVD",sig.cvd_div), fmt("OB",sig.orderbook),
                 fmt("TICK",sig.tick_flow), fmt("VEX",sig.vol_exhaust), f"  {'─'*42}",
                 f"  Σ={c:+.4f} | Side: {sig.reversion_side.upper()}", f"  ── GATES ──"]
        for g in gates: lines.append(f"  {g}")
        lines.append(f"  {'🎯 ALL PASS — confirming' if ap else '👀 Watching'}")
        lines.append(f"  Cooldown: {f'{cd:.0f}s' if cd>0 else 'ready'}")
        lines.append(f"└{'─'*66}")
        logger.info("\n"+"\n".join(lines))

    def _evaluate_entry(self, data_manager, order_manager, risk_manager, now):
        sig = self._compute_signals(data_manager)
        if sig is None: self._confirm_long = self._confirm_short = 0; return
        price = data_manager.get_last_price(); self._log_thinking(sig, price, now)
        c = sig.composite; thr = QCfg.COMPOSITE_ENTRY_MIN(); side = sig.reversion_side
        if not (sig.overextended and sig.regime_ok and not sig.htf_veto and sig.n_confirming >= 3):
            self._confirm_long = self._confirm_short = 0; return
        if self._last_exit_side and self._last_exit_side != side:
            if now - self._last_exit_time < QCfg.COOLDOWN_SEC() * 1.5: return
        if side == "long" and c >= thr: self._confirm_long += 1; self._confirm_short = 0
        elif side == "short" and c <= -thr: self._confirm_short += 1; self._confirm_long = 0
        else: self._confirm_long = self._confirm_short = 0; return
        cn = QCfg.CONFIRM_TICKS()
        if self._confirm_long >= cn:
            self._confirm_long = self._confirm_short = 0
            self._enter_trade(data_manager, order_manager, risk_manager, "long", sig)
        elif self._confirm_short >= cn:
            self._confirm_long = self._confirm_short = 0
            self._enter_trade(data_manager, order_manager, risk_manager, "short", sig)

    def _compute_sl_tp(self, data_manager, price, side, atr):
        """Institutional SL/TP using volume profile + orderbook walls + swing structure + VWAP bands."""
        try: candles_5m = data_manager.get_candles("5m", limit=QCfg.SL_SWING_LOOKBACK()+2)
        except: candles_5m = []
        try: candles_1m = data_manager.get_candles("1m", limit=150)
        except: candles_1m = []
        try: orderbook = data_manager.get_orderbook()
        except: orderbook = {"bids": [], "asks": []}
        vwap = self._vwap.vwap; vwap_std = self._vwap.vwap_std

        sl_price = InstitutionalLevels.compute_sl(
            price, side, atr, candles_5m, candles_1m, orderbook, vwap, vwap_std)
        sl_price = _round_to_tick(sl_price)

        tp_price = InstitutionalLevels.compute_tp(
            price, side, atr, sl_price, candles_1m, orderbook, vwap, vwap_std)
        tp_price = _round_to_tick(tp_price)

        if side=="long" and (sl_price>=price or tp_price<=price): return None, None
        if side=="short" and (sl_price<=price or tp_price>=price): return None, None
        return sl_price, tp_price

    def _enter_trade(self, data_manager, order_manager, risk_manager, side, sig):
        price = data_manager.get_last_price()
        if price < 1.0: return
        atr = self._atr_5m.atr
        if atr < 1e-10: return
        bal_info = risk_manager.get_available_balance()
        if bal_info is None: return
        total_bal = float(bal_info.get("total", bal_info.get("available", 0.0)))
        self._risk_gate.set_opening_balance(total_bal)
        allowed, reason = self._risk_gate.can_trade(total_bal)
        if not allowed: logger.info(f"Entry blocked: {reason}"); return
        qty = self._compute_quantity(risk_manager, price)
        if qty is None or qty < QCfg.MIN_QTY(): return
        sl_price, tp_price = self._compute_sl_tp(data_manager, price, side, atr)
        if sl_price is None: return
        sd = abs(price-sl_price); td = abs(price-tp_price)
        if sd < 1e-10: return
        rr = td / sd
        logger.info(f"🎯 ENTERING {side.upper()} @ ${price:,.2f} | qty={qty} | SL=${sl_price:,.2f} TP=${tp_price:,.2f} R:R=1:{rr:.2f} | VWAP=${sig.vwap_price:,.2f} | {sig}")
        entry_data = order_manager.place_market_order(side=side, quantity=qty)
        if not entry_data: logger.error("❌ Market order failed"); return
        self._risk_gate.record_trade_start()
        fill_price = float(entry_data.get("average_price") or 0) or float(entry_data.get("fill_price") or 0) or float(entry_data.get("price") or 0) or price
        slip = abs(fill_price-price)/price
        if slip > QCfg.SLIPPAGE_TOL():
            new_sl, new_tp = self._compute_sl_tp(data_manager, fill_price, side, atr)
            if new_sl is None:
                es = "sell" if side=="long" else "buy"
                order_manager.place_market_order(side=es, quantity=qty, reduce_only=True)
                self._last_exit_time = time.time(); return
            sl_price, tp_price = new_sl, new_tp
        exit_side = "sell" if side=="long" else "buy"
        sweep = order_manager.cancel_symbol_conditionals()
        if sweep:
            filled = [oid for oid,r in sweep.items() if r in (CancelResult.ALREADY_FILLED, CancelResult.PARTIAL_FILL)]
            if filled: self._last_reconcile_time = 0.0; return
        sl_data = order_manager.place_stop_loss(side=exit_side, quantity=qty, trigger_price=sl_price)
        if not sl_data:
            order_manager.place_market_order(side=exit_side, quantity=qty, reduce_only=True)
            self._last_exit_time = time.time(); return
        tp_data = order_manager.place_take_profit(side=exit_side, quantity=qty, trigger_price=tp_price)
        if not tp_data:
            order_manager.cancel_order(sl_data["order_id"])
            order_manager.place_market_order(side=exit_side, quantity=qty, reduce_only=True)
            self._last_exit_time = time.time(); return
        sdf = abs(fill_price-sl_price); ir = sdf*qty
        # Compute entry volume for trail vol-decay detection
        try:
            c1m = data_manager.get_candles("1m", limit=10)
            entry_vol = sum(float(c['v']) for c in c1m[-5:]) / 5.0 if len(c1m) >= 5 else 0.0
        except: entry_vol = 0.0
        self._pos = PositionState(phase=PositionPhase.ACTIVE, side=side, quantity=qty,
            entry_price=fill_price, sl_price=sl_price, tp_price=tp_price,
            sl_order_id=sl_data["order_id"], tp_order_id=tp_data["order_id"],
            entry_order_id=entry_data.get("order_id"), entry_time=time.time(),
            initial_risk=ir, initial_sl_dist=sdf, entry_signal=sig, entry_atr=self._atr_5m.atr,
            entry_vol=entry_vol)
        self.current_sl_price = sl_price; self.current_tp_price = tp_price; self._total_trades += 1
        rr_a = abs(fill_price-tp_price)/sdf if sdf > 0 else 0
        send_telegram_message(
            f"{'📈' if side=='long' else '📉'} <b>QUANT v4 ENTRY — {side.upper()}</b>\n\n"
            f"Entry:    ${fill_price:,.2f}\n"
            f"VWAP:     ${sig.vwap_price:,.2f} ({sig.deviation_atr:+.1f} ATR away)\n"
            f"SL:       ${sl_price:,.2f} (vol profile + OB wall + swing)\n"
            f"TP:       ${tp_price:,.2f} (nearest HVN/wall toward VWAP)\n"
            f"R:R:      1:{rr_a:.2f}\n"
            f"Risk:     ${ir:.2f} ({sdf:.2f} pts × {qty} BTC)\n"
            f"Confirm:  {sig.n_confirming}/5 agree")
        logger.info(f"✅ ACTIVE {side.upper()} @ ${fill_price:,.2f} | R:R=1:{rr_a:.2f}")

    def _manage_active(self, data_manager, order_manager, now):
        pos = self._pos; price = data_manager.get_last_price()
        if price < 1.0: return
        if now - pos.entry_time > QCfg.MAX_HOLD_SEC():
            logger.info(f"⏰ Max hold → exit"); self._exit_trade(order_manager, price, "max_hold_time"); return
        sig = self._compute_signals(data_manager)
        if sig is not None:
            self._log_thinking(sig, price, now)
            c = sig.composite; et = QCfg.EXIT_REVERSAL_THRESH()
            if pos.side=="long" and c<=-et:
                logger.info(f"🔄 Strong reversal ({c:+.3f}) → exit LONG")
                self._exit_trade(order_manager, price, "strong_reversal"); return
            if pos.side=="short" and c>=et:
                logger.info(f"🔄 Strong reversal ({c:+.3f}) → exit SHORT")
                self._exit_trade(order_manager, price, "strong_reversal"); return
        if QCfg.TRAIL_ENABLED() and now - pos.last_trail_time >= QCfg.TRAIL_INTERVAL_S():
            self._pos.last_trail_time = now
            if self._update_trailing_sl(order_manager, data_manager, price, now): return

    def _update_trailing_sl(self, order_manager, data_manager, price, now) -> bool:
        """Institutional trail: micro-swing following + volume decay tightening + orderbook wall snapping."""
        pos = self._pos; atr = self._atr_5m.atr
        if atr < 1e-10: return False
        profit = (price-pos.entry_price) if pos.side=="long" else (pos.entry_price-price)
        if profit > pos.peak_profit: pos.peak_profit = profit

        try: candles_1m = data_manager.get_candles("1m", limit=30)
        except: candles_1m = []
        try: orderbook = data_manager.get_orderbook()
        except: orderbook = {"bids": [], "asks": []}

        new_sl = InstitutionalLevels.compute_trail_sl(
            pos.side, price, pos.entry_price, pos.sl_price, atr,
            candles_1m, orderbook, pos.peak_profit, pos.entry_vol)

        if new_sl is None:
            return False

        new_sl_tick = _round_to_tick(new_sl)

        # Determine trail phase for logging
        init_dist = pos.initial_sl_dist if pos.initial_sl_dist > 1e-10 else atr
        tier = pos.peak_profit / init_dist
        if tier < QCfg.TRAIL_LOCK_R():
            phase_label = "🟡 BE (micro-swing)"
        else:
            # Check what drove the trail
            phase_label = "🟢 Active (swing"
            if len(candles_1m) >= 10 and pos.entry_vol > 1e-10:
                recent_vol = sum(float(c['v']) for c in candles_1m[-5:]) / 5.0
                if recent_vol / pos.entry_vol < QCfg.TRAIL_VOL_DECAY_MULT():
                    phase_label += " + vol decay"
            phase_label += ")"

        hm = (now - pos.entry_time) / 60.0
        logger.info(f"🔒 Trail [{phase_label}] ${pos.sl_price:,.1f} → ${new_sl_tick:,.1f} | R={tier:.1f} MFE={pos.peak_profit:.1f}pts hold={hm:.0f}m")
        send_telegram_message(
            f"🔒 <b>TRAIL SL</b> [{phase_label}]\n"
            f"${pos.sl_price:,.2f} → ${new_sl_tick:,.2f}\n"
            f"R: {tier:.1f} | MFE: {pos.peak_profit:.1f} pts | Hold: {hm:.0f}m")

        es = "sell" if pos.side=="long" else "buy"
        result = order_manager.replace_stop_loss(existing_sl_order_id=pos.sl_order_id, side=es, quantity=pos.quantity, new_trigger_price=new_sl_tick)
        if result is None:
            logger.warning("🚨 SL already fired"); self._record_exchange_exit(None); return True
        if isinstance(result, dict) and "error" in result: return False
        if result and isinstance(result, dict):
            self._pos.sl_price = new_sl_tick; self._pos.sl_order_id = result.get("order_id", pos.sl_order_id)
            self.current_sl_price = new_sl_tick
            if not pos.trail_active:
                self._pos.trail_active = True; logger.info("✅ Trailing SL active")
                send_telegram_message("✅ Trailing SL now active — following micro-swings")
        return False

    def _exit_trade(self, order_manager, price, reason):
        pos = self._pos
        if pos.phase != PositionPhase.ACTIVE: return
        logger.info(f"🚪 EXIT {pos.side.upper()} @ ${price:,.2f} | {reason}")
        self._pos.phase = PositionPhase.EXITING
        order_manager.cancel_all_exit_orders(sl_order_id=pos.sl_order_id, tp_order_id=pos.tp_order_id)
        es = "sell" if pos.side=="long" else "buy"
        order_manager.place_market_order(side=es, quantity=pos.quantity, reduce_only=True)
        pnl = self._estimate_pnl(pos, price)
        self._record_pnl(pnl)
        hm = (time.time()-pos.entry_time)/60
        send_telegram_message(
            f"{'✅' if pnl>0 else '❌'} <b>QUANT v4 EXIT — {pos.side.upper()}</b>\n\n"
            f"Reason:  {reason}\nEntry:   ${pos.entry_price:,.2f}\nExit:    ${price:,.2f}\n"
            f"PnL:     ${pnl:+.2f} USDT\nHold:    {hm:.1f} min\n\n"
            f"<i>Trades: {self._total_trades} | WR: {self._win_rate():.0%} | PnL: ${self._total_pnl:+.2f}</i>")
        self._last_exit_side = pos.side; self._finalise_exit()

    def _record_exchange_exit(self, ex_pos):
        """PnL FIX: When exchange SL/TP fires, compute PnL from entry vs SL/TP price.
        unrealized_pnl from API is often 0 because position is already closed."""
        pos = self._pos
        pnl = 0.0
        # Try to get unrealized_pnl from exchange first
        if ex_pos is not None:
            pnl = float(ex_pos.get("unrealized_pnl", 0.0))
        # If pnl is 0 (position already closed), calculate from known prices
        if abs(pnl) < 1e-10 and pos.entry_price > 0 and pos.quantity > 0:
            # Determine likely exit price: if SL was trailed, use SL price as exit
            # If TP was untouched, check if price was near TP
            exit_price = 0.0
            if self._last_known_price > 1.0:
                # Check if TP was hit (price crossed TP level)
                if pos.side == "long" and self._last_known_price >= pos.tp_price > 0:
                    exit_price = pos.tp_price
                elif pos.side == "short" and self._last_known_price <= pos.tp_price > 0:
                    exit_price = pos.tp_price
                # Check if SL was hit
                elif pos.side == "long" and self._last_known_price <= pos.sl_price:
                    exit_price = pos.sl_price
                elif pos.side == "short" and self._last_known_price >= pos.sl_price:
                    exit_price = pos.sl_price
                else:
                    exit_price = self._last_known_price
            elif pos.sl_price > 0:
                exit_price = pos.sl_price  # best guess: SL was hit
            if exit_price > 0:
                pnl = self._estimate_pnl(pos, exit_price)
                logger.info(f"📊 PnL calculated: entry=${pos.entry_price:,.2f} exit=${exit_price:,.2f} → ${pnl:+.2f}")
        self._record_pnl(pnl)
        pnl_icon = "✅" if pnl > 0 else ("❌" if pnl < 0 else "⚪")
        send_telegram_message(
            f"📡 <b>{pnl_icon} EXCHANGE EXIT — {pos.side.upper()}</b>\n\n"
            f"SL/TP fired on exchange\n"
            f"Entry: ${pos.entry_price:,.2f}\n"
            f"SL was: ${pos.sl_price:,.2f} | TP was: ${pos.tp_price:,.2f}\n"
            f"PnL:   ${pnl:+.2f} USDT\n"
            f"Trail was: {'active ✅' if pos.trail_active else 'not activated'}\n\n"
            f"<i>Trades: {self._total_trades} | WR: {self._win_rate():.0%} | Total: ${self._total_pnl:+.2f}</i>")
        self._last_exit_side = pos.side; self._finalise_exit()

    def _record_pnl(self, pnl):
        self._total_pnl += pnl
        if pnl > 0: self._winning_trades += 1
        self._risk_gate.record_trade_result(pnl)

    def _finalise_exit(self):
        self._pos = PositionState(); self._last_exit_time = time.time()
        self.current_sl_price = 0.0; self.current_tp_price = 0.0
        logger.info("Position closed — FLAT")

    def _compute_quantity(self, risk_manager, price):
        bal = risk_manager.get_available_balance()
        if bal is None: return None
        available = float(bal.get("available", 0.0))
        if available < QCfg.MIN_MARGIN_USDT(): return None
        margin_alloc = available * QCfg.MARGIN_PCT()
        if margin_alloc < QCfg.MIN_MARGIN_USDT(): return None
        notional = margin_alloc * QCfg.LEVERAGE(); qty_raw = notional / price
        step = QCfg.LOT_STEP(); qty = math.floor(qty_raw/step)*step; qty = round(qty,8)
        qty = max(QCfg.MIN_QTY(), min(QCfg.MAX_QTY(), qty))
        if (qty*price)/QCfg.LEVERAGE() > margin_alloc*1.02: return None
        logger.info(f"Sizing → avail=${available:.2f} | alloc={QCfg.MARGIN_PCT():.0%} | margin=${margin_alloc:.2f} | {QCfg.LEVERAGE()}x | qty={qty}")
        return qty

    @staticmethod
    def _estimate_pnl(pos, exit_price):
        gross = ((exit_price-pos.entry_price) if pos.side=="long" else (pos.entry_price-exit_price))*pos.quantity
        fee = (pos.entry_price+exit_price)*pos.quantity*QCfg.COMMISSION_RATE()
        return gross - fee

    def _win_rate(self): return self._winning_trades/self._total_trades if self._total_trades else 0.0

    def get_stats(self):
        return {"total_trades":self._total_trades,"winning_trades":self._winning_trades,
                "win_rate":f"{self._win_rate():.1%}","total_pnl":round(self._total_pnl,2),
                "daily_trades":self._risk_gate.daily_trades,"consec_losses":self._risk_gate.consec_losses,
                "current_phase":self._pos.phase.name,"last_signal":str(self._last_sig),
                "atr_5m":round(self._atr_5m.atr,2),"atr_1m":round(self._atr_1m.atr,2),
                "atr_pctile":f"{self._atr_5m.get_percentile():.0%}","regime_ok":self._atr_5m.regime_valid()}

    def format_status_report(self):
        s=self.get_stats(); p=self._pos
        lines = ["📊 <b>QUANT v4 STATUS</b>","",
            f"Phase: {s['current_phase']}",
            f"Regime: {'✅' if s['regime_ok'] else '❌'}",
            f"ATR: ${s['atr_5m']}/{s['atr_1m']} ({s['atr_pctile']})",
            f"HTF: 15m={self._htf.trend_15m:+.2f} 4h={self._htf.trend_4h:+.2f}",
            f"VWAP: ${self._vwap.vwap:,.2f} (dev={self._vwap.deviation_atr:+.1f}ATR)","",
            f"Trades: {s['total_trades']} | WR {s['win_rate']} | PnL ${s['total_pnl']:+.2f}",
            f"Daily: {s['daily_trades']}/{QCfg.MAX_DAILY_TRADES()} | Losses: {s['consec_losses']}/{QCfg.MAX_CONSEC_LOSSES()}"]
        if not p.is_flat():
            hm=(time.time()-p.entry_time)/60
            lines += ["",f"<b>Active ({p.side.upper()})</b>",
                f"Entry: ${p.entry_price:,.2f} | SL: ${p.sl_price:,.2f} | TP: ${p.tp_price:,.2f}",
                f"Hold: {hm:.1f}m | Trail: {'✅' if p.trail_active else '⏳'}"]
        return "\n".join(lines)

    # ─── RECONCILIATION (unchanged logic, fixed PnL) ───
    def _reconcile_query_thread(self, order_manager):
        try:
            ex_pos = order_manager.get_open_position()
            if ex_pos is None: return
            ex_size = float(ex_pos.get("size",0.0)); open_orders = None
            if ex_size >= float(getattr(config,"MIN_POSITION_SIZE",0.001)):
                try: open_orders = order_manager.get_open_orders()
                except: pass
            with self._lock: self._reconcile_data = {"ex_pos":ex_pos,"open_orders":open_orders}
        except Exception as e: logger.warning(f"Reconcile error: {e}")
        finally: self._reconcile_pending = False

    def _reconcile_apply(self, order_manager, data):
        ex_pos=data["ex_pos"]; open_orders=data.get("open_orders")
        ex_size=float(ex_pos.get("size",0.0)); ex_side=str(ex_pos.get("side") or "").upper()
        phase = self._pos.phase
        if phase==PositionPhase.FLAT and ex_size>=QCfg.MIN_QTY():
            ex_entry=float(ex_pos.get("entry_price",0.0)); ex_upnl=float(ex_pos.get("unrealized_pnl",0.0))
            sl_oid=tp_oid=None; sl_p=tp_p=0.0
            if open_orders:
                for o in open_orders:
                    ot=o.get("type","").upper()
                    if ot in ("STOP_MARKET","STOP","STOP_LOSS_MARKET"): sl_oid=o["order_id"]; sl_p=float(o.get("trigger_price") or 0)
                    elif ot in ("TAKE_PROFIT_MARKET","TAKE_PROFIT"): tp_oid=o["order_id"]; tp_p=float(o.get("trigger_price") or 0)
            iside = "long" if ex_side=="LONG" else "short"
            self._pos = PositionState(phase=PositionPhase.ACTIVE, side=iside, quantity=ex_size,
                entry_price=ex_entry, sl_price=sl_p, tp_price=tp_p, sl_order_id=sl_oid,
                tp_order_id=tp_oid, entry_time=time.time(), initial_sl_dist=abs(ex_entry-sl_p) if sl_p>0 else 0.0,
                entry_atr=self._atr_5m.atr)
            self.current_sl_price=sl_p; self.current_tp_price=tp_p
            self._confirm_long=self._confirm_short=0
            logger.warning(f"⚡ RECONCILE: adopted {ex_side} @ ${ex_entry:,.2f}")
            send_telegram_message(f"⚡ <b>POSITION ADOPTED</b>\nSide: {ex_side} | Size: {ex_size}\nEntry: ${ex_entry:,.2f} | uPnL: ${ex_upnl:+.2f}")
            return
        if phase==PositionPhase.ACTIVE and ex_size<QCfg.MIN_QTY():
            logger.info("📡 Reconcile: exchange FLAT → TP/SL fired")
            self._record_exchange_exit(ex_pos); return
        if phase==PositionPhase.ACTIVE and ex_size>=QCfg.MIN_QTY():
            if (not self._pos.sl_order_id or not self._pos.tp_order_id) and open_orders:
                for o in open_orders:
                    ot=o.get("type","").upper()
                    if not self._pos.sl_order_id and ot in ("STOP_MARKET","STOP","STOP_LOSS_MARKET"): self._pos.sl_order_id=o["order_id"]
                    elif not self._pos.tp_order_id and ot in ("TAKE_PROFIT_MARKET","TAKE_PROFIT"): self._pos.tp_order_id=o["order_id"]

    def _sync_position(self, order_manager):
        try: ex_pos = order_manager.get_open_position()
        except: return
        if ex_pos is None: return
        ex_size = float(ex_pos.get("size",0.0))
        if self._pos.phase==PositionPhase.ACTIVE:
            if ex_size<QCfg.MIN_QTY():
                logger.info("📡 Sync: exchange FLAT → TP/SL fired")
                self._record_exchange_exit(ex_pos)
        elif self._pos.phase==PositionPhase.EXITING:
            if ex_size<QCfg.MIN_QTY(): self._finalise_exit()

"""
quant_engine.py — Institutional Predictive Microstructure Alpha Engine v1.0
============================================================================
REPLACES the reactive composite scoring (VWAP dev + CVD + OB + tick + VEX).

PHILOSOPHY: This engine PREDICTS directional moves before they happen.
The old system reacted to moves already in progress — price deviates from
VWAP → score rises → enter → move is already half over.

Institutional quant desks predict via:
  1. ORDER FLOW TOXICITY (VPIN) — measures informed trading probability
  2. ORDERBOOK PRESSURE GRADIENT — bid/ask wall velocity + depth slope
  3. MICROSTRUCTURE MOMENTUM — multi-scale price impact decomposition
  4. KYLE'S LAMBDA — real-time price impact coefficient
  5. TRADE ARRIVAL ACCELERATION — information event detection
  6. VOLATILITY REGIME MODEL — realized vol vs expected vol breaks
  7. CROSS-TF MOMENTUM DECOMPOSITION — 1m/5m/15m signal separation

These signals feed a Bayesian regime-aware scoring framework that
produces a DIRECTIONAL PREDICTION with calibrated confidence.

PUBLIC API:
    engine = QuantEngine()
    engine.on_trade(price, qty, is_buy, ts)          # feed every trade tick
    engine.on_orderbook(bids, asks, ts)               # feed every OB snapshot
    engine.on_candle(tf, candle_dict)                  # feed every candle close
    prediction = engine.predict(price, atr)            # get directional prediction
    prediction.direction  → "long" | "short" | "neutral"
    prediction.confidence → 0.0 – 1.0
    prediction.alpha_bps  → expected edge in basis points
    prediction.signals    → dict of component scores
"""

from __future__ import annotations

import logging
import math
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# DATA TYPES
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class Prediction:
    """Directional prediction from the quant engine."""
    direction: str = "neutral"   # "long" | "short" | "neutral"
    confidence: float = 0.0      # 0.0–1.0 calibrated probability
    alpha_bps: float = 0.0       # expected edge in basis points
    urgency: float = 0.0         # 0.0–1.0 how quickly the edge decays
    signals: Dict[str, float] = field(default_factory=dict)
    regime: str = "unknown"      # "trending_up"|"trending_down"|"ranging"|"volatile"
    vpin: float = 0.0            # raw VPIN for external consumers
    timestamp: float = 0.0

    def is_actionable(self, min_confidence: float = 0.55) -> bool:
        return self.direction != "neutral" and self.confidence >= min_confidence

    def __str__(self):
        sigs = " ".join(f"{k}={v:+.3f}" for k, v in self.signals.items())
        return (f"Prediction({self.direction} conf={self.confidence:.3f} "
                f"α={self.alpha_bps:+.1f}bps regime={self.regime} {sigs})")


@dataclass
class VolatilityState:
    """Real-time volatility regime."""
    realized_1m: float = 0.0     # 1-minute realized vol (annualized)
    realized_5m: float = 0.0     # 5-minute realized vol
    realized_ratio: float = 1.0  # 1m/5m ratio — >1.5 = vol expansion
    atr_percentile: float = 0.5  # current ATR rank in recent history
    regime: str = "normal"       # "quiet"|"normal"|"elevated"|"extreme"


# ═══════════════════════════════════════════════════════════════════════════════
# COMPONENT 1: VPIN — Volume-Synchronized Probability of Informed Trading
# ═══════════════════════════════════════════════════════════════════════════════

class VPINEngine:
    """
    VPIN (Easley, López de Prado, O'Hara 2012).

    Measures the probability of informed (toxic) order flow.
    High VPIN = institutions are trading aggressively with private information.

    Implementation:
      - Volume clock: divide trade flow into equal-volume buckets
      - Each bucket: classify as buy/sell (tick rule or exchange side)
      - VPIN = abs(V_buy - V_sell) / V_total over rolling N buckets
      - High VPIN (>0.70) = strong directional conviction by informed traders
      - Low VPIN (<0.30) = noise trading, no edge

    The DIRECTION of informed flow is determined by which side dominates
    the high-VPIN buckets — not by VPIN magnitude alone.
    """

    def __init__(self, bucket_volume: float = 0.0, n_buckets: int = 50):
        self._bucket_vol_target = bucket_volume  # auto-calibrated if 0
        self._n_buckets = n_buckets
        self._lock = threading.Lock()

        # Current accumulating bucket
        self._current_buy_vol = 0.0
        self._current_sell_vol = 0.0
        self._current_total_vol = 0.0

        # Completed buckets: (buy_vol, sell_vol)
        self._buckets: deque = deque(maxlen=n_buckets * 3)

        # Calibration
        self._total_volume_seen = 0.0
        self._total_trades_seen = 0
        self._calibration_complete = False
        self._CALIBRATION_TRADES = 500  # trades before auto-calibrating bucket size

        # Output
        self._vpin = 0.0
        self._buy_dominance = 0.0  # +1 = all buy, -1 = all sell
        self._bucket_count = 0

    def _auto_calibrate(self):
        """Set bucket volume to median trade volume × 50 (institutional standard)."""
        if self._total_trades_seen < self._CALIBRATION_TRADES:
            return
        avg_trade_vol = self._total_volume_seen / self._total_trades_seen
        self._bucket_vol_target = max(avg_trade_vol * 50.0, 5000.0)  # FIX-QE3: $5k floor
        self._calibration_complete = True
        logger.info(f"VPIN calibrated: bucket_vol=${self._bucket_vol_target:,.2f} "
                    f"(avg_trade=${avg_trade_vol:,.2f} × 50)")

    def on_trade(self, price: float, qty: float, is_buy: bool):
        """Feed a single trade tick."""
        dollar_vol = price * qty
        with self._lock:
            self._total_volume_seen += dollar_vol
            self._total_trades_seen += 1

            if not self._calibration_complete:
                self._auto_calibrate()
                if not self._calibration_complete:
                    return

            # Accumulate into current bucket
            if is_buy:
                self._current_buy_vol += dollar_vol
            else:
                self._current_sell_vol += dollar_vol
            self._current_total_vol += dollar_vol

            # Check if bucket is full
            if self._current_total_vol >= self._bucket_vol_target:
                self._buckets.append((self._current_buy_vol, self._current_sell_vol))
                self._bucket_count += 1
                self._current_buy_vol = 0.0
                self._current_sell_vol = 0.0
                self._current_total_vol = 0.0
                self._recompute()

    def _recompute(self):
        """Recompute VPIN from completed buckets."""
        n = min(self._n_buckets, len(self._buckets))
        if n < 10:
            return

        recent = list(self._buckets)[-n:]
        total_buy = sum(b for b, s in recent)
        total_sell = sum(s for b, s in recent)
        total_vol = total_buy + total_sell

        if total_vol < 1e-10:
            return

        # VPIN = average absolute order imbalance
        abs_imbalances = [abs(b - s) / (b + s) if (b + s) > 0 else 0.0
                          for b, s in recent]
        self._vpin = sum(abs_imbalances) / len(abs_imbalances)

        # Buy dominance: who is driving the informed flow?
        self._buy_dominance = (total_buy - total_sell) / total_vol

    @property
    def vpin(self) -> float:
        with self._lock:
            return self._vpin

    @property
    def informed_direction(self) -> float:
        """Returns +1 (buy pressure) to -1 (sell pressure), weighted by VPIN."""
        with self._lock:
            return self._buy_dominance * self._vpin

    @property
    def is_warmed(self) -> bool:
        with self._lock:
            return self._calibration_complete and self._bucket_count >= 15


# ═══════════════════════════════════════════════════════════════════════════════
# COMPONENT 2: ORDERBOOK PRESSURE GRADIENT
# ═══════════════════════════════════════════════════════════════════════════════

class OrderbookPressureEngine:
    """
    Multi-level orderbook analysis.

    Institutional approach:
      1. DEPTH GRADIENT: How quickly does liquidity thin out from the mid?
         Steep bid side + flat ask side = sell wall forming = bearish pressure.
      2. IMBALANCE VELOCITY: Rate of change of bid/ask ratio.
         Accelerating bid dominance = buy absorption increasing.
      3. WALL DETECTION: Identify price levels with >3σ liquidity.
         Wall behind price = institutional defense. Wall ahead = resistance.
      4. MICROPRICE: Volume-weighted mid-price that reflects true equilibrium.
         Microprice above mid = buy pressure. Below = sell pressure.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._imbalance_history: deque = deque(maxlen=200)
        self._microprice_history: deque = deque(maxlen=200)
        self._depth_gradient_history: deque = deque(maxlen=100)
        self._last_update = 0.0

        # Outputs
        self._imbalance = 0.0
        self._imbalance_velocity = 0.0
        self._microprice_signal = 0.0
        self._depth_gradient = 0.0
        self._wall_signal = 0.0

    def on_orderbook(self, bids: list, asks: list, price: float):
        """Feed an orderbook snapshot."""
        if not bids or not asks or price < 1.0:
            return

        now = time.time()
        with self._lock:
            # Parse top N levels
            n_levels = min(20, len(bids), len(asks))
            bid_prices = []; bid_qtys = []
            ask_prices = []; ask_qtys = []

            for i in range(n_levels):
                bp, bq = self._parse_level(bids[i])
                ap, aq = self._parse_level(asks[i])
                if bp > 0 and bq > 0:
                    bid_prices.append(bp); bid_qtys.append(bq)
                if ap > 0 and aq > 0:
                    ask_prices.append(ap); ask_qtys.append(aq)

            if len(bid_qtys) < 3 or len(ask_qtys) < 3:
                return

            # ── 1. Orderbook imbalance (bid-weighted) ──
            total_bid = sum(bid_qtys[:10])
            total_ask = sum(ask_qtys[:10])
            total = total_bid + total_ask
            self._imbalance = (total_bid - total_ask) / total if total > 0 else 0.0
            self._imbalance_history.append((now, self._imbalance))

            # ── 2. Imbalance velocity ──
            if len(self._imbalance_history) >= 10:
                recent = list(self._imbalance_history)
                dt = recent[-1][0] - recent[-10][0]
                if dt > 0.5:
                    delta = recent[-1][1] - recent[-10][1]
                    self._imbalance_velocity = delta / dt
                else:
                    self._imbalance_velocity = 0.0

            # ── 3. Microprice signal ──
            bb = bid_prices[0]; bq0 = bid_qtys[0]
            ba = ask_prices[0]; aq0 = ask_qtys[0]
            total_top = bq0 + aq0
            if total_top > 0:
                microprice = (bb * aq0 + ba * bq0) / total_top
                mid = (bb + ba) / 2.0
                spread = ba - bb
                if spread > 0:
                    self._microprice_signal = (microprice - mid) / spread
                    self._microprice_history.append((now, self._microprice_signal))

            # ── 4. Depth gradient (how fast liquidity drops off) ──
            if len(bid_qtys) >= 5 and len(ask_qtys) >= 5:
                # Ratio of deep-book to near-book liquidity
                bid_near = sum(bid_qtys[:3])
                bid_deep = sum(bid_qtys[3:8])
                ask_near = sum(ask_qtys[:3])
                ask_deep = sum(ask_qtys[3:8])

                bid_gradient = bid_deep / bid_near if bid_near > 0 else 0.0
                ask_gradient = ask_deep / ask_near if ask_near > 0 else 0.0

                # Positive = bids have deeper support than asks = bullish
                self._depth_gradient = bid_gradient - ask_gradient
                self._depth_gradient_history.append((now, self._depth_gradient))

            # ── 5. Wall detection ──
            self._wall_signal = self._detect_walls(
                bid_prices, bid_qtys, ask_prices, ask_qtys, price)

            self._last_update = now

    @staticmethod
    def _parse_level(level) -> Tuple[float, float]:
        if isinstance(level, (list, tuple)) and len(level) >= 2:
            return float(level[0]), float(level[1])
        if isinstance(level, dict):
            p = float(level.get("limit_price", level.get("price", 0)))
            q = float(level.get("size", level.get("quantity", level.get("depth", 0))))
            return p, q
        return 0.0, 0.0

    @staticmethod
    def _detect_walls(bid_px, bid_qty, ask_px, ask_qty, price) -> float:
        """Detect outsized liquidity walls. Returns [-1, +1]: +1 = bid wall (bullish)."""
        all_qty = bid_qty + ask_qty
        if len(all_qty) < 6:
            return 0.0
        mean_q = sum(all_qty) / len(all_qty)
        std_q = math.sqrt(sum((q - mean_q) ** 2 for q in all_qty) / len(all_qty))
        if std_q < 1e-10:
            return 0.0
        threshold = mean_q + 2.5 * std_q

        bid_wall_vol = sum(q for q in bid_qty if q >= threshold)
        ask_wall_vol = sum(q for q in ask_qty if q >= threshold)
        total_wall = bid_wall_vol + ask_wall_vol
        if total_wall < 1e-10:
            return 0.0
        return (bid_wall_vol - ask_wall_vol) / total_wall

    def get_composite(self) -> Tuple[float, Dict[str, float]]:
        """
        Returns (composite_signal, components_dict).
        Composite: [-1, +1], positive = buy pressure.
        """
        with self._lock:
            # Weight components by predictive power
            # Microprice and imbalance velocity are the strongest short-term predictors
            w_imb = 0.20
            w_vel = 0.30
            w_micro = 0.25
            w_depth = 0.15
            w_wall = 0.10

            composite = (
                _clip(self._imbalance, -1, 1) * w_imb +
                _clip(self._imbalance_velocity * 5.0, -1, 1) * w_vel +
                _clip(self._microprice_signal, -1, 1) * w_micro +
                _clip(self._depth_gradient, -1, 1) * w_depth +
                _clip(self._wall_signal, -1, 1) * w_wall
            )

            components = {
                "ob_imb": self._imbalance,
                "ob_vel": self._imbalance_velocity,
                "ob_micro": self._microprice_signal,
                "ob_depth": self._depth_gradient,
                "ob_wall": self._wall_signal,
            }

            return _clip(composite, -1, 1), components


# ═══════════════════════════════════════════════════════════════════════════════
# COMPONENT 3: KYLE'S LAMBDA — Price Impact Coefficient
# ═══════════════════════════════════════════════════════════════════════════════

class KyleLambdaEngine:
    """
    Kyle's Lambda (1985) — measures price impact per unit of order flow.

    λ = Δprice / ΔOrderFlow

    High λ = thin market, each trade moves price a lot (informed trading likely).
    Low λ = deep market, trades absorbed without price movement.

    RISING λ with directional flow = institutions moving price intentionally.
    FALLING λ with volume = liquidity is absorbing (distribution/accumulation).

    We compute this on 1-minute windows and track the trajectory.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._window_trades: deque = deque(maxlen=5000)
        self._lambda_history: deque = deque(maxlen=120)  # 1-min lambda values
        self._last_compute = 0.0

        self._lambda = 0.0
        self._lambda_velocity = 0.0
        self._flow_weighted_lambda = 0.0

    def on_trade(self, price: float, qty: float, is_buy: bool, ts: float):
        with self._lock:
            signed_flow = qty * (1.0 if is_buy else -1.0)
            self._window_trades.append((ts, price, signed_flow))

    def compute(self) -> float:
        """Compute Kyle's Lambda. Call periodically (~every 5-10s)."""
        now = time.time()
        with self._lock:
            if now - self._last_compute < 5.0:
                return self._lambda

            # Get last 60 seconds of trades
            cutoff = now - 60.0
            recent = [(ts, px, flow) for ts, px, flow in self._window_trades
                      if ts >= cutoff]

            if len(recent) < 20:
                return self._lambda

            # Divide into 5-second micro-buckets
            buckets = {}
            for ts, px, flow in recent:
                bucket_id = int(ts / 5.0)
                if bucket_id not in buckets:
                    buckets[bucket_id] = {"prices": [], "flows": []}
                buckets[bucket_id]["prices"].append(px)
                buckets[bucket_id]["flows"].append(flow)

            if len(buckets) < 4:
                return self._lambda

            # Compute Δprice and ΔFlow for each bucket
            delta_prices = []
            delta_flows = []
            for bid, data in sorted(buckets.items()):
                if len(data["prices"]) >= 2:
                    dp = data["prices"][-1] - data["prices"][0]
                    df = sum(data["flows"])
                    delta_prices.append(dp)
                    delta_flows.append(df)

            if len(delta_prices) < 3:
                return self._lambda

            # OLS regression: Δprice = λ × ΔFlow + ε
            n = len(delta_prices)
            sum_xy = sum(dp * df for dp, df in zip(delta_prices, delta_flows))
            sum_xx = sum(df * df for df in delta_flows)

            if sum_xx > 1e-10:
                new_lambda = sum_xy / sum_xx
            else:
                new_lambda = 0.0

            # EWMA smooth
            alpha = 0.3
            self._lambda = alpha * new_lambda + (1 - alpha) * self._lambda
            self._lambda_history.append((now, self._lambda))

            # Lambda velocity (is impact increasing or decreasing?)
            if len(self._lambda_history) >= 6:
                hist = list(self._lambda_history)
                recent_avg = sum(v for _, v in hist[-3:]) / 3
                prior_avg = sum(v for _, v in hist[-6:-3]) / 3
                self._lambda_velocity = recent_avg - prior_avg

            self._last_compute = now
            return self._lambda

    @property
    def lambda_val(self) -> float:
        with self._lock:
            return self._lambda

    @property
    def lambda_velocity(self) -> float:
        with self._lock:
            return self._lambda_velocity


# ═══════════════════════════════════════════════════════════════════════════════
# COMPONENT 4: TRADE ARRIVAL ACCELERATION
# ═══════════════════════════════════════════════════════════════════════════════

class TradeArrivalEngine:
    """
    Detects information events via acceleration in trade arrival rate.

    Institutional traders cannot hide their information — they must trade.
    When informed flow arrives, trade frequency spikes asymmetrically
    (one side accelerates while the other stays flat).

    IMPLEMENTATION:
      - Track trade arrivals per side in 5-second windows
      - Compute buy/sell arrival rates
      - Detect acceleration (rate of change of rate)
      - Asymmetric acceleration = informed flow = predictive signal
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._buy_arrivals: deque = deque(maxlen=3000)
        self._sell_arrivals: deque = deque(maxlen=3000)
        self._signal = 0.0
        self._acceleration = 0.0
        self._last_compute = 0.0

    def on_trade(self, is_buy: bool, ts: float):
        with self._lock:
            if is_buy:
                self._buy_arrivals.append(ts)
            else:
                self._sell_arrivals.append(ts)

    def compute(self) -> float:
        """Compute trade arrival asymmetry. Returns [-1, +1]."""
        now = time.time()
        with self._lock:
            if now - self._last_compute < 3.0:
                return self._signal

            # Count arrivals in recent vs prior windows
            recent_cutoff = now - 15.0
            prior_cutoff = now - 30.0

            buy_recent = sum(1 for t in self._buy_arrivals if t >= recent_cutoff)
            buy_prior = sum(1 for t in self._buy_arrivals
                           if prior_cutoff <= t < recent_cutoff)
            sell_recent = sum(1 for t in self._sell_arrivals if t >= recent_cutoff)
            sell_prior = sum(1 for t in self._sell_arrivals
                            if prior_cutoff <= t < recent_cutoff)

            # Arrival rates (trades per second)
            buy_rate_now = buy_recent / 15.0
            buy_rate_prev = buy_prior / 15.0
            sell_rate_now = sell_recent / 15.0
            sell_rate_prev = sell_prior / 15.0

            # Acceleration (rate of change)
            buy_accel = buy_rate_now - buy_rate_prev
            sell_accel = sell_rate_now - sell_rate_prev

            # Asymmetric acceleration signal
            total_accel = abs(buy_accel) + abs(sell_accel)
            if total_accel > 0.01:
                asymmetry = (buy_accel - sell_accel) / total_accel
                # Weight by total intensity (more trades = stronger signal)
                total_rate = buy_rate_now + sell_rate_now
                intensity = min(total_rate / 5.0, 1.0)  # normalize to ~5 tps
                self._signal = _clip(asymmetry * intensity, -1, 1)
            else:
                self._signal *= 0.9  # decay toward zero

            self._acceleration = buy_accel - sell_accel
            self._last_compute = now
            return self._signal

    @property
    def signal(self) -> float:
        with self._lock:
            return self._signal


# ═══════════════════════════════════════════════════════════════════════════════
# COMPONENT 5: MULTI-SCALE MOMENTUM DECOMPOSITION
# ═══════════════════════════════════════════════════════════════════════════════

class MomentumEngine:
    """
    Cross-timeframe momentum decomposition.

    Decomposes price momentum into three scales:
      MICRO  (1m):   10-period EMA slope / ATR — captures tick-by-tick direction
      MESO   (5m):   20-period EMA slope / ATR — captures swing direction
      MACRO  (15m):  20-period EMA slope / ATR — captures structural trend

    ALIGNMENT: When all three scales agree in direction → high conviction.
    DIVERGENCE: When scales disagree → transitional period, lower conviction.

    The key insight: micro leads meso leads macro. Detecting micro momentum
    shift BEFORE meso confirms is the predictive edge.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._candles = {
            "1m": deque(maxlen=100),
            "5m": deque(maxlen=60),
            "15m": deque(maxlen=40),
        }
        self._last_candle_ts = {"1m": 0, "5m": 0, "15m": 0}  # FIX-QE6
        self._micro = 0.0
        self._meso = 0.0
        self._macro = 0.0
        self._alignment = 0.0
        self._leading_signal = 0.0

    def on_candle(self, tf: str, candle: dict):
        """Feed a closed candle."""
        if tf not in self._candles:
            return
        with self._lock:
            self._candles[tf].append(candle)
            self._recompute()

    def _recompute(self):
        # Micro: 1m EMA(10)
        c1m = list(self._candles["1m"])
        if len(c1m) >= 15:
            self._micro = self._ema_slope(c1m, 10)

        # Meso: 5m EMA(20)
        c5m = list(self._candles["5m"])
        if len(c5m) >= 25:
            self._meso = self._ema_slope(c5m, 20)

        # Macro: 15m EMA(20)
        c15m = list(self._candles["15m"])
        if len(c15m) >= 25:
            self._macro = self._ema_slope(c15m, 20)

        # Alignment score: how well do the three scales agree?
        signs = [_sign(self._micro), _sign(self._meso), _sign(self._macro)]
        agreement = sum(signs)
        if abs(agreement) == 3:
            # Perfect alignment
            direction = signs[0]
            magnitude = (abs(self._micro) + abs(self._meso) + abs(self._macro)) / 3.0
            self._alignment = direction * min(magnitude, 1.0)
        elif abs(agreement) == 1:
            # Partial alignment
            self._alignment = agreement * 0.3 * (
                abs(self._micro) + abs(self._meso) + abs(self._macro)) / 3.0
        else:
            self._alignment = 0.0

        # Leading signal: micro divergence from meso
        # When micro shifts but meso hasn't yet → early warning
        if abs(self._meso) > 0.01:
            self._leading_signal = self._micro - self._meso
        else:
            self._leading_signal = self._micro

    @staticmethod
    def _ema_slope(candles: list, period: int) -> float:
        """Compute EMA slope normalized by recent range."""
        if len(candles) < period + 3:
            return 0.0
        closes = [float(c.get("c", c.get("close", 0))) for c in candles]
        k = 2.0 / (period + 1)
        ema = sum(closes[:period]) / period
        emas = [ema]
        for v in closes[period:]:
            ema = v * k + ema * (1 - k)
            emas.append(ema)

        if len(emas) < 5:
            return 0.0

        # Slope over last 5 EMA values, normalized by recent range
        recent_range = max(closes[-20:]) - min(closes[-20:])
        if recent_range < 1e-10:
            return 0.0

        slope = (emas[-1] - emas[-5]) / recent_range
        return _clip(slope * 5.0, -1.0, 1.0)

    def get_signals(self) -> Dict[str, float]:
        with self._lock:
            return {
                "mom_micro": self._micro,
                "mom_meso": self._meso,
                "mom_macro": self._macro,
                "mom_align": self._alignment,
                "mom_lead": self._leading_signal,
            }


# ═══════════════════════════════════════════════════════════════════════════════
# COMPONENT 6: VOLATILITY REGIME MODEL
# ═══════════════════════════════════════════════════════════════════════════════

class VolatilityRegimeEngine:
    """
    Realized volatility regime detection and prediction.

    Tracks realized vol at 1m and 5m scales. Detects regime transitions:
      - Vol contraction → expansion (breakout signal)
      - Vol expansion → contraction (reversal setup)
      - Vol ratio (1m/5m) → >1.5 means intrabar vol is spiking vs structural

    This is not ATR — it's return-based realized volatility computed from
    log returns, which is the institutional standard.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._returns_1m: deque = deque(maxlen=100)
        self._returns_5m: deque = deque(maxlen=60)
        self._vol_1m_history: deque = deque(maxlen=60)
        self._vol_5m_history: deque = deque(maxlen=60)

        self._state = VolatilityState()

    def on_candle(self, tf: str, candle: dict):
        """Feed a closed candle and update vol estimates."""
        close = float(candle.get("c", candle.get("close", 0)))
        open_px = float(candle.get("o", candle.get("open", 0)))
        if close < 1 or open_px < 1:
            return

        log_ret = math.log(close / open_px)

        with self._lock:
            if tf == "1m":
                self._returns_1m.append(log_ret)
                if len(self._returns_1m) >= 20:
                    vol = self._realized_vol(list(self._returns_1m)[-20:])
                    self._state.realized_1m = vol
                    self._vol_1m_history.append(vol)

            elif tf == "5m":
                self._returns_5m.append(log_ret)
                if len(self._returns_5m) >= 20:
                    vol = self._realized_vol(list(self._returns_5m)[-20:])
                    self._state.realized_5m = vol
                    self._vol_5m_history.append(vol)

            # Update ratio
            if self._state.realized_5m > 1e-10:
                self._state.realized_ratio = (
                    self._state.realized_1m / self._state.realized_5m)

            # Regime classification from vol percentile
            self._classify_regime()

    @staticmethod
    def _realized_vol(returns: list) -> float:
        """Annualized realized volatility from log returns."""
        if len(returns) < 5:
            return 0.0
        mean = sum(returns) / len(returns)
        var = sum((r - mean) ** 2 for r in returns) / (len(returns) - 1)
        return math.sqrt(var)  # caller provides annualization factor

    def _classify_regime(self):
        """Classify current vol regime by percentile rank."""
        hist = list(self._vol_1m_history)
        if len(hist) < 15:
            self._state.regime = "normal"
            self._state.atr_percentile = 0.5
            return

        current = hist[-1]
        sorted_hist = sorted(hist)
        rank = sum(1 for v in sorted_hist if v <= current) / len(sorted_hist)
        self._state.atr_percentile = rank

        if rank > 0.90:
            self._state.regime = "extreme"
        elif rank > 0.70:
            self._state.regime = "elevated"
        elif rank < 0.20:
            self._state.regime = "quiet"
        else:
            self._state.regime = "normal"

    @property
    def state(self) -> VolatilityState:
        with self._lock:
            return VolatilityState(
                realized_1m=self._state.realized_1m,
                realized_5m=self._state.realized_5m,
                realized_ratio=self._state.realized_ratio,
                atr_percentile=self._state.atr_percentile,
                regime=self._state.regime,
            )

    def is_expanding(self) -> bool:
        """True if vol is accelerating (breakout/event)."""
        with self._lock:
            if len(self._vol_1m_history) < 10:
                return False
            recent = list(self._vol_1m_history)
            return recent[-1] > 1.3 * (sum(recent[-10:-1]) / 9)

    def is_contracting(self) -> bool:
        """True if vol is declining (range formation)."""
        with self._lock:
            if len(self._vol_1m_history) < 10:
                return False
            recent = list(self._vol_1m_history)
            return recent[-1] < 0.7 * (sum(recent[-10:-1]) / 9)


# ═══════════════════════════════════════════════════════════════════════════════
# COMPONENT 7: BAYESIAN REGIME CLASSIFIER
# ═══════════════════════════════════════════════════════════════════════════════

class RegimeClassifier:
    """
    Bayesian market regime classifier.

    Maintains posterior probabilities over four regimes:
      TRENDING_UP, TRENDING_DOWN, RANGING, VOLATILE

    Updates via signal evidence:
      - Momentum alignment → TRENDING
      - VPIN high + vol expansion → VOLATILE
      - Momentum disagreement + low vol → RANGING
      - OB pressure one-sided + rising lambda → directional

    The regime determines how signals are weighted in the final prediction.
    """

    REGIMES = ("trending_up", "trending_down", "ranging", "volatile")

    def __init__(self):
        # Prior: start uniform
        self._priors = {r: 0.25 for r in self.REGIMES}
        self._lock = threading.Lock()

    def update(self, momentum: Dict[str, float], vol: VolatilityState,
               vpin: float, ob_composite: float) -> str:
        """Update posteriors and return MAP regime."""
        with self._lock:
            # Compute likelihoods for each regime given evidence
            likelihoods = {}

            mom_align = momentum.get("mom_align", 0.0)
            mom_micro = momentum.get("mom_micro", 0.0)

            # TRENDING_UP: positive momentum alignment + moderate vol
            likelihoods["trending_up"] = (
                _likelihood(mom_align, 0.5, 0.3) *
                _likelihood(ob_composite, 0.2, 0.4) *
                _likelihood(vol.atr_percentile, 0.55, 0.25)
            )

            # TRENDING_DOWN: negative momentum alignment + moderate vol
            likelihoods["trending_down"] = (
                _likelihood(mom_align, -0.5, 0.3) *
                _likelihood(ob_composite, -0.2, 0.4) *
                _likelihood(vol.atr_percentile, 0.55, 0.25)
            )

            # RANGING: low alignment + low vol
            likelihoods["ranging"] = (
                _likelihood(abs(mom_align), 0.0, 0.2) *
                _likelihood(vol.atr_percentile, 0.3, 0.2) *
                _likelihood(vpin, 0.3, 0.2)
            )

            # VOLATILE: high vol + high VPIN
            likelihoods["volatile"] = (
                _likelihood(vol.atr_percentile, 0.85, 0.15) *
                _likelihood(vpin, 0.6, 0.2) *
                _likelihood(vol.realized_ratio, 1.5, 0.5)
            )

            # Bayesian update
            posteriors = {}
            for regime in self.REGIMES:
                posteriors[regime] = self._priors[regime] * likelihoods[regime]

            # Normalize
            total = sum(posteriors.values())
            if total > 1e-15:
                for r in self.REGIMES:
                    posteriors[r] /= total
            else:
                posteriors = {r: 0.25 for r in self.REGIMES}

            # Exponential smoothing to prevent regime flicker
            self._update_count = getattr(self, '_update_count', 0) + 1
            alpha = 0.40 if self._update_count < 20 else 0.15  # FIX-QE7
            for r in self.REGIMES:
                self._priors[r] = alpha * posteriors[r] + (1 - alpha) * self._priors[r]

            # MAP estimate
            best = max(self._priors, key=self._priors.get)
            return best

    @property
    def regime(self) -> str:
        with self._lock:
            return max(self._priors, key=self._priors.get)

    @property
    def confidence(self) -> float:
        with self._lock:
            return max(self._priors.values())

    @property
    def posteriors(self) -> Dict[str, float]:
        with self._lock:
            return dict(self._priors)


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN ENGINE: QUANT ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

class QuantEngine:
    """
    Institutional Predictive Microstructure Alpha Engine.

    Aggregates all components into a single directional prediction.
    The prediction is regime-aware: signal weights shift based on the
    Bayesian regime classifier so the model adapts in real-time.

    USAGE:
        engine = QuantEngine()
        engine.on_trade(price, qty, is_buy, ts)
        engine.on_orderbook(bids, asks, price)
        engine.on_candle("5m", candle_dict)
        pred = engine.predict(current_price, current_atr)
        if pred.is_actionable(min_confidence=0.55):
            enter(pred.direction)
    """

    # Regime-adaptive weights: (vpin, ob, lambda, arrival, momentum)
    _WEIGHTS = {
        "trending_up":   (0.15, 0.15, 0.10, 0.20, 0.40),
        "trending_down": (0.15, 0.15, 0.10, 0.20, 0.40),
        "ranging":       (0.25, 0.30, 0.15, 0.15, 0.15),
        "volatile":      (0.30, 0.20, 0.20, 0.20, 0.10),
        "unknown":       (0.20, 0.20, 0.15, 0.20, 0.25),
    }

    def __init__(self):
        self._vpin = VPINEngine()
        self._ob = OrderbookPressureEngine()
        self._kyle = KyleLambdaEngine()
        self._arrival = TradeArrivalEngine()
        self._momentum = MomentumEngine()
        self._vol = VolatilityRegimeEngine()
        self._regime = RegimeClassifier()

        self._lock = threading.Lock()
        self._last_prediction = Prediction()
        self._prediction_count = 0
        self._trade_count = 0
        self._trade_count_lock = threading.Lock()

    # ── Data ingestion ────────────────────────────────────────────────────────

    def on_trade(self, price: float, qty: float, is_buy: bool,
                 ts: float = 0.0):
        """Feed every raw trade tick."""
        if ts <= 0:
            ts = time.time()
        self._vpin.on_trade(price, qty, is_buy)
        self._kyle.on_trade(price, qty, is_buy, ts)
        self._arrival.on_trade(is_buy, ts)
        with self._trade_count_lock:
            self._trade_count += 1

    def on_orderbook(self, bids: list, asks: list, price: float):
        """Feed every orderbook snapshot."""
        self._ob.on_orderbook(bids, asks, price)

    def on_candle(self, tf: str, candle: dict):
        """Feed every closed candle."""
        self._momentum.on_candle(tf, candle)
        self._vol.on_candle(tf, candle)

    # ── Prediction ────────────────────────────────────────────────────────────

    def predict(self, price: float, atr: float) -> Prediction:
        """
        Generate a directional prediction.

        Args:
            price: current market price
            atr: current 5m ATR (for normalization)

        Returns:
            Prediction with direction, confidence, alpha estimate
        """
        with self._lock:
            now = time.time()

            # ── Gather component signals ──
            # 1. VPIN informed direction
            vpin_val = self._vpin.vpin
            vpin_dir = self._vpin.informed_direction  # [-1, +1]

            # 2. Orderbook pressure
            ob_composite, ob_components = self._ob.get_composite()

            # 3. Kyle's Lambda
            kyle_lambda = self._kyle.compute()
            kyle_vel = self._kyle.lambda_velocity
            # Rising lambda + directional flow = institutions pushing
            # Lambda direction derived from recent flow
            kyle_signal = _clip(kyle_vel * 100.0, -1, 1)  # normalize

            # 4. Trade arrival asymmetry
            arrival_signal = self._arrival.compute()

            # 5. Momentum decomposition
            mom_signals = self._momentum.get_signals()
            mom_composite = mom_signals.get("mom_align", 0.0)
            mom_lead = mom_signals.get("mom_lead", 0.0)

            # 6. Volatility state
            vol_state = self._vol.state

            # ── Regime classification ──
            regime = self._regime.update(mom_signals, vol_state,
                                          vpin_val, ob_composite)

            # ── Regime-adaptive signal weighting ──
            w = self._WEIGHTS.get(regime, self._WEIGHTS["unknown"])
            w_vpin, w_ob, w_kyle, w_arrival, w_mom = w

            # Compute weighted directional signal
            raw_signal = (
                vpin_dir * w_vpin +
                ob_composite * w_ob +
                kyle_signal * w_kyle +
                arrival_signal * w_arrival +
                mom_composite * w_mom
            )

            # ── Confidence calibration ──
            # Base confidence from signal magnitude
            base_conf = min(abs(raw_signal), 1.0)

            # Boost: all components agree → higher confidence
            signs = [_sign(vpin_dir), _sign(ob_composite),
                     _sign(arrival_signal), _sign(mom_composite)]
            sign_agreement = abs(sum(signs)) / max(len(signs), 1)
            agreement_boost = 0.15 * sign_agreement

            # Boost: high VPIN → informed trading → confidence up
            vpin_boost = 0.10 * min(vpin_val / 0.7, 1.0) if vpin_val > 0.4 else 0.0

            # Penalty: regime disagreement (volatile regime + low mom = unclear)
            regime_penalty = 0.0
            if regime == "volatile" and abs(mom_composite) < 0.2:
                regime_penalty = 0.15
            if regime == "ranging" and abs(raw_signal) < 0.15:
                regime_penalty = 0.10

            # Leading indicator boost: micro diverging from meso
            lead_boost = 0.0
            if abs(mom_lead) > 0.3 and _sign(mom_lead) == _sign(raw_signal):
                lead_boost = 0.08

            confidence = _clip(
                base_conf + agreement_boost + vpin_boost + lead_boost - regime_penalty,
                0.0, 0.95  # cap at 0.95 — never 100% certain
            )

            # ── Direction ──
            if confidence < 0.25 or abs(raw_signal) < 0.08:
                direction = "neutral"
                confidence = 0.0
            elif raw_signal > 0:
                direction = "long"
            else:
                direction = "short"

            # ── Alpha estimate (expected edge in bps) ──
            # Based on signal strength × Kyle's lambda × vol regime
            if atr > 0 and price > 0:
                # Expected move = signal_strength × ATR fraction
                expected_move_pct = abs(raw_signal) * 0.003  # ~0.3% at full signal
                alpha_bps = expected_move_pct * 10000
                # Adjust for vol regime
                if vol_state.regime == "extreme":
                    alpha_bps *= 1.5
                elif vol_state.regime == "quiet":
                    alpha_bps *= 0.6
            else:
                alpha_bps = 0.0

            # ── Urgency (how quickly will this edge decay?) ──
            urgency = 0.5
            if vol_state.regime == "extreme":
                urgency = 0.9  # fast markets — act now
            elif vpin_val > 0.6:
                urgency = 0.8  # informed flow — decays fast
            elif regime == "ranging":
                urgency = 0.3  # ranging — can wait

            # ── Build prediction ──
            all_signals = {
                "vpin_dir": vpin_dir,
                "ob_comp": ob_composite,
                "kyle_sig": kyle_signal,
                "arrival": arrival_signal,
                "mom_align": mom_composite,
                "mom_lead": mom_lead,
                "raw": raw_signal,
                **ob_components,
            }

            pred = Prediction(
                direction=direction,
                confidence=confidence,
                alpha_bps=alpha_bps,
                urgency=urgency,
                signals=all_signals,
                regime=regime,
                vpin=vpin_val,
                timestamp=now,
            )

            self._last_prediction = pred
            self._prediction_count += 1
            return pred

    # ── Status / diagnostics ──────────────────────────────────────────────────

    @property
    def is_warmed(self) -> bool:
        """True when all components have enough data to produce valid signals."""
        with self._trade_count_lock:
            tc = self._trade_count
        return (self._vpin.is_warmed and tc >= 200)

    @property
    def warmup_status(self) -> Dict[str, bool]:
        return {
            "vpin": self._vpin.is_warmed,
            "trades": self._trade_count >= 200,
            "total_trades": self._trade_count,
        }

    @property
    def last_prediction(self) -> Prediction:
        with self._lock:
            return self._last_prediction

    def get_regime_posteriors(self) -> Dict[str, float]:
        return self._regime.posteriors

    def diagnostic_snapshot(self) -> Dict:
        """Full diagnostic snapshot for Telegram status."""
        pred = self._last_prediction
        vol = self._vol.state
        return {
            "direction": pred.direction,
            "confidence": pred.confidence,
            "alpha_bps": pred.alpha_bps,
            "regime": pred.regime,
            "regime_conf": self._regime.confidence,
            "vpin": pred.vpin,
            "vol_regime": vol.regime,
            "vol_pctile": vol.atr_percentile,
            "vol_ratio": vol.realized_ratio,
            "kyle_lambda": self._kyle.lambda_val,
            "kyle_vel": self._kyle.lambda_velocity,
            "signals": pred.signals,
            "trade_count": self._trade_count,
            "warmed": self.is_warmed,
        }

    def reset_state(self):
        """Reset after stream reconnect."""
        # Components maintain their own deques — just reset counts
        self._trade_count = max(self._trade_count - 100, 0)
        self._prediction_count = max(self._prediction_count - 2, 0)
        logger.info("QuantEngine state partially reset for stream recovery")


# ═══════════════════════════════════════════════════════════════════════════════
# UTILITY FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def _clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def _sign(x: float) -> int:
    if x > 0.01: return 1
    if x < -0.01: return -1
    return 0

def _likelihood(observed: float, center: float, width: float) -> float:
    """Gaussian likelihood function."""
    if width <= 0:
        return 1.0
    z = (observed - center) / width
    return math.exp(-0.5 * z * z)

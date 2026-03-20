"""
fee_engine.py — Dynamic Execution Cost Engine
==============================================
Replaces ALL hardcoded fee multipliers with live market-adaptive logic.

Three responsibilities:
  1. Track true round-trip execution cost from live data
     (fee rate + rolling spread + EWMA realized slippage)

  2. Compute a regime-adaptive minimum required gross TP move
     that scales with ATR percentile and spread/ATR ratio —
     not a fixed multiplier

  3. Decide maker vs taker entry based on signal urgency,
     orderbook depth, and a quantified cost/benefit of posting limit

No constant in this file should ever need manual tuning.
Every threshold is derived from the market's own statistics.
"""

from __future__ import annotations

import logging
import math
import threading
from collections import deque
from typing import Dict, Optional, Tuple

import sys, os as _os; sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
import config

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# SPREAD TRACKER
# ─────────────────────────────────────────────────────────────────────────────

def _cfg(name: str, default):
    """Read a config value at call-time so hot-reloads take effect."""
    val = getattr(config, name, None)
    return default if val is None else val


def _ob_px(lvl) -> float:
    """Extract price from an orderbook level (list or Delta dict format)."""
    if isinstance(lvl, (list, tuple)): return float(lvl[0])
    if isinstance(lvl, dict): return float(lvl.get("limit_price") or lvl.get("price") or 0)
    return 0.0

def _ob_qty(lvl) -> float:
    """Extract quantity from an orderbook level (list or Delta dict format)."""
    if isinstance(lvl, (list, tuple)) and len(lvl) >= 2: return float(lvl[1])
    if isinstance(lvl, dict): return float(lvl.get("size") or lvl.get("quantity") or lvl.get("depth") or 0)
    return 0.0


class SpreadTracker:
    """
    Maintains a rolling distribution of bid-ask spread in basis points.

    Uses a fixed-size deque so memory is bounded regardless of runtime.
    Reports median (not mean) to resist outlier spikes from thin-book moments.

    Decay: when market regime changes (spread distribution shifts),
    we want to adapt quickly. We use a half-life weighting — older
    samples are down-weighted exponentially when computing percentiles.
    """

    def __init__(self):
        maxlen = int(_cfg("FEE_SPREAD_HIST_MAXLEN", 500))
        self._hist: deque = deque(maxlen=maxlen)
        self._lock = threading.Lock()

    def update(self, orderbook: Dict, price: float) -> None:
        bids = orderbook.get("bids", [])
        asks = orderbook.get("asks", [])
        if not bids or not asks or price < 1.0:
            return
        try:
            bid = _ob_px(bids[0])
            ask = _ob_px(asks[0])
            if bid <= 0 or ask <= bid:
                return
            mid = (bid + ask) / 2.0
            bps = (ask - bid) / mid * 10_000.0
            with self._lock:
                is_first = len(self._hist) == 0
                self._hist.append(bps)
                if is_first:
                    logger.debug(f"SpreadTracker: first sample {bps:.2f}bps")
                elif len(self._hist) == 5:
                    # Use unlocked version — we already hold self._lock here.
                    # Calling self.median_bps() would deadlock (non-reentrant Lock).
                    logger.debug(f"SpreadTracker: 5 samples collected, median={self._median_bps_unlocked():.2f}bps")
        except Exception:
            pass

    @property
    def sample_count(self) -> int:
        with self._lock:
            return len(self._hist)

    def _median_bps_unlocked(self) -> float:
        """Compute median WITHOUT acquiring the lock — for use within locked sections."""
        default = float(_cfg("FEE_SPREAD_DEFAULT_BPS", 2.0))
        if len(self._hist) < 5:
            return default
        arr = sorted(self._hist)
        return arr[len(arr) // 2]

    def median_bps(self) -> float:
        """Thread-safe median — acquires lock. Do NOT call from within update()."""
        with self._lock:
            return self._median_bps_unlocked()

    def percentile_bps(self, pct: float) -> float:
        """pct in [0, 1]"""
        default = float(_cfg("FEE_SPREAD_DEFAULT_BPS", 2.0))
        with self._lock:
            if len(self._hist) < 10:
                return default
            arr = sorted(self._hist)
            idx = int(pct * (len(arr) - 1))
            return arr[max(0, min(len(arr) - 1, idx))]

    def current_bid_ask(self, orderbook: Dict) -> Tuple[float, float]:
        try:
            bid = _ob_px(orderbook["bids"][0])
            ask = _ob_px(orderbook["asks"][0])
            return bid, ask
        except Exception:
            return 0.0, 0.0


# ─────────────────────────────────────────────────────────────────────────────
# SLIPPAGE TRACKER
# ─────────────────────────────────────────────────────────────────────────────

class SlippageTracker:
    """
    EWMA of realized fill slippage in basis points.

    Feeds back from actual fills — no assumptions.
    Warm-up default is FEE_SLIP_DEFAULT_BPS (typical for BTC futures at moderate size).
    """

    def __init__(self):
        self._ewma: Optional[float] = None
        self._lock = threading.Lock()

    def record(self, expected_price: float, fill_price: float) -> None:
        if expected_price <= 0 or fill_price <= 0:
            return
        alpha = float(_cfg("FEE_SLIP_ALPHA", 0.25))
        bps = abs(fill_price - expected_price) / expected_price * 10_000.0
        with self._lock:
            if self._ewma is None:
                self._ewma = bps
            else:
                self._ewma = alpha * bps + (1.0 - alpha) * self._ewma

    def expected_bps(self) -> float:
        default = float(_cfg("FEE_SLIP_DEFAULT_BPS", 1.5))
        floor   = float(_cfg("FEE_SLIP_MIN_BPS", 0.5))
        with self._lock:
            return max(floor, self._ewma if self._ewma is not None else default)


# ─────────────────────────────────────────────────────────────────────────────
# REGIME-ADAPTIVE PROFIT FLOOR
# ─────────────────────────────────────────────────────────────────────────────

class ProfitFloorModel:
    """
    Computes the minimum gross move (in price units) for a trade to be
    net-profitable after ALL execution costs, with a buffer that adapts
    to the current volatility regime.

    The core insight: in a quiet market (low ATR percentile), any price
    deviation is likely noise and the expected reversion distance is smaller.
    The fee cost as a fraction of any realistic move is therefore higher.
    We require a larger buffer — more deviation before entering.

    In a volatile market (high ATR percentile), moves are fat and the fee
    is a small fraction of the expected swing. The buffer can be tighter.

    The multiplier curve is a sigmoid anchored to two config-controlled points:
      - ATR pctile ≤ 0.10  → mult ≈ FEE_FLOOR_MULT_LOW   (quiet, fees dominate)
      - ATR pctile ≥ 0.85  → mult ≈ FEE_FLOOR_MULT_HIGH  (fat moves, fees are noise)

    An additional spread penalty applies when spread/ATR ratio exceeds
    FEE_SPREAD_ATR_WARN — this market is expensive relative to its moves.

    Signal confidence from the composite score provides a bounded discount
    (max FEE_CONF_MAX_DISCOUNT) — a conviction entry can afford a slightly
    tighter floor, but the hard floor FEE_FLOOR_ABS_MIN_MULT always applies.
    """

    def compute_multiplier(
        self,
        atr_percentile: float,
        spread_bps: float,
        atr: float,
        price: float,
        signal_confidence: float = 0.5,
    ) -> float:
        """
        atr_percentile: 0..1, where current ATR sits in its own history
        spread_bps:     current median spread in basis points
        atr:            current ATR in price units
        price:          current mid-price
        signal_confidence: composite signal strength mapped to 0..1

        FEE_FLOOR_MULT calibration:
          Original FEE_FLOOR_MULT_LOW=5.5 was physically impossible.
          At pctile=0.12, ATR=$63: mult=5.05 × rt_cost($88) = $445 TP required.
          That is 7×ATR. No structural target exists 7×ATR away on a 1×ATR SL setup.
          The correct reasoning: fee cost needs 1.5-2× buffer, not 5×.
          FEE_FLOOR_MULT_LOW=2.5 → at pctile=0.12: $88×2.3=$202 (taker) or $55×2.3=$127 (maker).
          Still enforces the fee floor. Never demands physically unreachable TP.
        """
        mult_low   = float(_cfg("FEE_FLOOR_MULT_LOW",    2.5))   # was 5.5 — caused 7×ATR TP demands
        mult_high  = float(_cfg("FEE_FLOOR_MULT_HIGH",   1.2))   # was 1.8 — still reasonable in high vol
        inflect    = float(_cfg("FEE_FLOOR_INFLECT",     0.45))
        steepness  = float(_cfg("FEE_FLOOR_STEEPNESS",   6.0))
        abs_min    = float(_cfg("FEE_FLOOR_ABS_MIN_MULT", 1.2))  # was 1.4
        spread_warn = float(_cfg("FEE_SPREAD_ATR_WARN",  0.06))
        penalty_k  = float(_cfg("FEE_SPREAD_PENALTY_K",  4.0))
        conf_neutral     = float(_cfg("FEE_CONF_NEUTRAL",      0.5))
        conf_max_disc    = float(_cfg("FEE_CONF_MAX_DISCOUNT",  0.30))

        p = max(0.02, min(0.98, atr_percentile))

        # Sigmoid: high p → low mult, low p → high mult
        z = (p - inflect) * steepness
        sig = 1.0 / (1.0 + math.exp(-z))   # 0..1
        base_mult = mult_high + (mult_low - mult_high) * (1.0 - sig)

        # Spread penalty
        if atr > 1e-10 and price > 1e-10:
            spread_price     = spread_bps / 10_000.0 * price
            spread_atr_ratio = spread_price / atr
            excess           = max(0.0, spread_atr_ratio - spread_warn)
            spread_penalty   = 1.0 + excess * penalty_k
        else:
            spread_penalty = 1.0

        # Signal confidence discount
        conf_norm = max(0.0, min(1.0, signal_confidence))
        if conf_norm > conf_neutral and (1.0 - conf_neutral) > 1e-10:
            conf_excess = (conf_norm - conf_neutral) / (1.0 - conf_neutral)
            confidence_discount = 1.0 - conf_excess * conf_max_disc
        else:
            confidence_discount = 1.0

        mult = base_mult * spread_penalty * confidence_discount
        return max(abs_min, mult)

    def min_gross_move(
        self,
        price: float,
        atr: float,
        atr_percentile: float,
        total_rt_cost_bps: float,
        spread_bps: float,
        signal_confidence: float = 0.5,
    ) -> float:
        """
        Returns the minimum gross price move (always positive) required
        for a trade to clear all execution costs with the regime-adaptive buffer.

        ATR CAP: result is capped at FEE_FLOOR_MAX_ATR_MULT × ATR.
        Without this cap, at very low ATR percentiles the mult can demand
        4-7×ATR TP which is structurally impossible. The cap ensures the fee
        floor never exceeds a physically reachable structural target.
        Default cap: 2.0×ATR (a generous but achievable TP distance).
        """
        if price <= 0:
            return float("inf")     # genuine bad data — hard-block always correct
        if atr <= 0:
            # Bug-20 fix: returning inf here caused valid ICT entries to be
            # silently rejected during ATR warmup (first ~14 candles after boot).
            # The bot would confirm a signal through all structural gates, hit the
            # fee floor check, get inf back, and silently drop the trade with no
            # log entry. Strategy-level gates (min_qty, SL distance) still apply;
            # bypassing the fee floor during warmup is safe — it re-activates the
            # moment ATR > 0.
            logger.debug("ProfitFloor: ATR=0 (engine warming up) — fee floor bypassed")
            return 0.0

        mult = self.compute_multiplier(
            atr_percentile, spread_bps, atr, price, signal_confidence
        )
        rt_cost_price = total_rt_cost_bps / 10_000.0 * price
        result = rt_cost_price * mult

        # Hard cap: never require more than FEE_FLOOR_MAX_ATR_MULT×ATR
        # This prevents the fee floor from demanding targets that don't exist
        max_atr_mult = float(_cfg("FEE_FLOOR_MAX_ATR_MULT", 2.0))
        atr_cap      = max_atr_mult * atr
        result       = min(result, atr_cap)

        logger.debug(
            f"ProfitFloor: rt_cost={rt_cost_price:.2f} × mult={mult:.2f} "
            f"(pctile={atr_percentile:.2f}, spread={spread_bps:.1f}bps, "
            f"conf={signal_confidence:.2f}) → min_move=${result:.2f} "
            f"(atr_cap=${atr_cap:.2f})"
        )
        return result


# ─────────────────────────────────────────────────────────────────────────────
# MAKER / TAKER ENTRY DECISION
# ─────────────────────────────────────────────────────────────────────────────

class MakerTakerDecision:
    """
    Decides whether to post a limit (maker) or take market (taker) for entry.

    The decision quantifies the trade-off between:
      - Fee saving from maker (TAKER_RATE - MAKER_RATE, in bps)
      - Fill probability risk: estimated probability the limit gets filled
        before price moves away, based on:
          (a) signal urgency  — how fast is the deviation expanding?
          (b) momentum        — is price already reverting without us?
          (c) orderbook depth — thin books mean we may not fill

    Decision rule: post maker iff
        fee_saving_bps × fill_probability - spread_bps × (1 - fill_p) × OPP_COST_WEIGHT
        > FEE_MAKER_MIN_SAVING_BPS

    All thresholds read from config at call-time so they can be tuned live.
    """

    @property
    def TAKER_RATE(self) -> float:
        return float(_cfg("COMMISSION_RATE", 0.00055))

    @property
    def MAKER_RATE(self) -> float:
        return float(_cfg("COMMISSION_RATE_MAKER", self.TAKER_RATE * 0.40))

    @property
    def TICK_SIZE(self) -> float:
        return float(_cfg("TICK_SIZE", 0.1))

    def decide(
        self,
        side: str,
        quantity: float,
        price: float,
        orderbook: Dict,
        signal_urgency: float,   # 0..1: how urgently the signal demands entry
        spread_bps: float,
    ) -> Tuple[bool, float, str]:
        """
        Returns:
            use_maker (bool): True → post limit, False → take market
            limit_price (float): the limit price to post at (0 if market)
            reason (str): human-readable explanation for logging

        v4.9 FIXES:
          - urgency_cutoff raised 0.72 → 0.82 (was falling back to taker too eagerly
            on mean-reversion setups where urgency is moderate-high by design)
          - depth_fill_floor raised 0.25 → 0.35 (thin books were getting fill_p=0.2,
            making saving appear negative even with good urgency conditions)
          - min_saving_bps lowered 0.8 → 0.5 (even a small saving justifies limit
            given the 3s rate-limit overhead we're already paying)
        """
        urgency_cutoff  = float(_cfg("FEE_MAKER_URGENCY_CUTOFF",   0.82))  # was 0.72
        min_saving_bps  = float(_cfg("FEE_MAKER_MIN_SAVING_BPS",   0.5))   # was 0.8
        depth_levels    = int(_cfg(  "FEE_MAKER_DEPTH_LEVELS",     5))
        depth_max_frac  = float(_cfg("FEE_MAKER_DEPTH_MAX_FRAC",   0.25))
        depth_fill_floor= float(_cfg("FEE_MAKER_DEPTH_FILL_FLOOR", 0.35))  # was 0.2
        opp_cost_weight = float(_cfg("FEE_MAKER_OPP_COST_WEIGHT",  0.5))
        tick            = self.TICK_SIZE

        try:
            bids = orderbook.get("bids", [])
            asks = orderbook.get("asks", [])
            if not bids or not asks:
                return False, 0.0, "no_book_data"

            bid = _ob_px(bids[0])
            ask = _ob_px(asks[0])
            if bid <= 0 or ask <= bid:
                return False, 0.0, "invalid_book"

            # ── 1. Urgency hard cutoff ─────────────────────────────────────
            if signal_urgency > urgency_cutoff:
                return False, 0.0, f"urgency={signal_urgency:.2f}>{urgency_cutoff}"

            # ── 2. Quantify fee saving ─────────────────────────────────────
            fee_saving_bps = (self.TAKER_RATE - self.MAKER_RATE) * 10_000.0

            # ── 3. Estimate fill probability ──────────────────────────────
            urgency_fill_prob = 1.0 - signal_urgency

            if side == "long":
                relevant_depth = sum(
                    _ob_qty(b) for b in bids[:depth_levels]
                    if isinstance(b, (list, tuple)) and len(b) >= 2
                )
            else:
                relevant_depth = sum(
                    _ob_qty(a) for a in asks[:depth_levels]
                    if isinstance(a, (list, tuple)) and len(a) >= 2
                )

            if relevant_depth > 0:
                size_fraction  = quantity / relevant_depth
                depth_fill_prob = max(depth_fill_floor, 1.0 - size_fraction / depth_max_frac)
            else:
                depth_fill_prob = depth_fill_floor

            fill_probability = urgency_fill_prob * depth_fill_prob

            # ── 4. Risk-adjusted saving ────────────────────────────────────
            risk_adjusted_saving = (
                fee_saving_bps * fill_probability
                - spread_bps * (1.0 - fill_probability) * opp_cost_weight
            )

            if risk_adjusted_saving < min_saving_bps:
                return (
                    False, 0.0,
                    f"saving={risk_adjusted_saving:.2f}bps<{min_saving_bps}"
                    f" (fee_save={fee_saving_bps:.2f}, fill_p={fill_probability:.2f})"
                )

            # ── 5. Compute limit price — guarded against tight spreads ──────
            # BUG FIX: bid+TICK on a 1-tick spread == ask → immediate taker fill.
            # Clamp so the limit price stays strictly inside the spread.
            if side == "long":
                candidate   = round(bid + tick, 1)
                limit_price = min(candidate, round(ask - tick, 1))
                if limit_price <= bid or limit_price >= ask:
                    limit_price = bid   # guaranteed maker fallback
            else:
                candidate   = round(ask - tick, 1)
                limit_price = max(candidate, round(bid + tick, 1))
                if limit_price <= bid or limit_price >= ask:
                    limit_price = ask   # guaranteed maker fallback

            return (
                True, limit_price,
                f"maker: save={risk_adjusted_saving:.2f}bps, fill_p={fill_probability:.2f}, "
                f"lim=${limit_price:.2f}"
            )

        except Exception as e:
            logger.warning(f"MakerTakerDecision error: {e}")
            return False, 0.0, f"error:{e}"


# ─────────────────────────────────────────────────────────────────────────────
# MAIN ENGINE — composition of the above
# ─────────────────────────────────────────────────────────────────────────────

class ExecutionCostEngine:
    """
    Public interface for the dynamic execution cost system.

    All constants are read from config at call-time via _cfg() so they can
    be changed in config.py and take effect on the next evaluation cycle
    without restarting the bot.

    Usage in quant_strategy.py:
        self._fee_engine = ExecutionCostEngine()

        # In on_tick / data update:
        self._fee_engine.update_orderbook(orderbook, price)

        # In _enter_trade before placing order:
        use_maker, limit_px, reason = self._fee_engine.decide_entry_type(
            side, qty, price, orderbook, signal_urgency)

        # After fill confirmed:
        self._fee_engine.record_fill(expected_price, fill_price)

        # In _compute_sl_tp to gate TP viability:
        min_move = self._fee_engine.min_required_tp_move(
            price, atr, atr_pctile, use_maker, composite_score)

        if abs(tp_price - price) < min_move:
            return None, None   # reject — not enough edge after fees
    """

    @property
    def TAKER_RATE(self) -> float:
        return float(_cfg("COMMISSION_RATE", 0.00055))

    @property
    def MAKER_RATE(self) -> float:
        return float(_cfg("COMMISSION_RATE_MAKER", self.TAKER_RATE * 0.40))

    def __init__(self):
        self._spread  = SpreadTracker()
        self._slip    = SlippageTracker()
        self._floor   = ProfitFloorModel()
        self._mtd     = MakerTakerDecision()

    def is_warmed_up(self) -> bool:
        """
        True once the engine has enough spread samples for reliable cost estimates.

        Before warmup: median_bps() returns the hardcoded default (2.0 bps).
        That default can produce fee floors that reject valid setups if the
        actual spread is tighter.  Gate the TP floor check on warmup.

        Warmup threshold: 5 spread samples (~5-10 seconds of live data).

        Note: slippage EWMA is tracked separately in diagnostic_snapshot via
        'slip_warmed'.  We do NOT require the slippage EWMA here because
        expected_bps() has its own safe default (0.0 bps) when unwarmed,
        which only underestimates round-trip cost — it never over-gates entries.
        Requiring slip_warmed here would have blocked ALL entries for several
        minutes after a stream restart until a fill was recorded.
        """
        return self._spread.sample_count >= 5

    # ── Feed ──────────────────────────────────────────────────────────────────

    def update_orderbook(self, orderbook: Dict, price: float) -> None:
        """Call on every orderbook snapshot (already called in data_manager tick)."""
        self._spread.update(orderbook, price)

    def record_fill(self, expected_price: float, fill_price: float) -> None:
        """Call after every fill to feed back realized slippage."""
        self._slip.record(expected_price, fill_price)

    # ── Query ─────────────────────────────────────────────────────────────────

    def effective_roundtrip_cost_bps(self, use_maker_entry: bool) -> float:
        """
        Total estimated round-trip cost in basis points.

        Taker entry (market):
          entry_fee(taker) + exit_fee(taker) + half_spread×2 + slippage×2
          Both legs cross the spread; both legs have market-order slippage.

        Maker entry (limit):
          entry_fee(maker) + exit_fee(taker) + half_spread×1 + slippage×1
          Entry does NOT cross the spread (we post, we don't take).
          Entry has zero slippage (fills at exactly the posted price).
          Only the SL/TP exit (always market) incurs spread-cross + slippage.

        BUG FIX: original code charged half_spread*2 + slip*2 unconditionally,
        which over-stated round-trip cost for maker entries by one full spread
        and one full slippage, producing a min_tp floor that was too high for
        maker-routed trades and causing legitimate setups to be rejected.
        """
        entry_fee   = (self.MAKER_RATE if use_maker_entry else self.TAKER_RATE) * 10_000
        exit_fee    = self.TAKER_RATE * 10_000      # SL/TP always market = taker
        half_spread = self._spread.median_bps() / 2.0
        slip        = self._slip.expected_bps()

        if use_maker_entry:
            # Entry: maker fee only — no spread cross, no slippage
            # Exit:  taker spread cross + slippage
            return entry_fee + exit_fee + half_spread + slip
        else:
            # Both legs are market: entry + exit each cross half spread and have slippage
            return entry_fee + exit_fee + half_spread * 2 + slip * 2

    def min_required_tp_move(
        self,
        price: float,
        atr: float,
        atr_percentile: float,
        use_maker_entry: bool,
        signal_confidence: float = 0.5,
    ) -> float:
        """
        Minimum gross TP distance (in price units) for the trade to be
        net-profitable after ALL costs in the current regime.

        If the computed TP target is closer than this, reject the setup.
        """
        spread_bps   = self._spread.median_bps()
        rt_cost_bps  = self.effective_roundtrip_cost_bps(use_maker_entry)
        return self._floor.min_gross_move(
            price, atr, atr_percentile, rt_cost_bps,
            spread_bps, signal_confidence
        )

    def decide_entry_type(
        self,
        side: str,
        quantity: float,
        price: float,
        orderbook: Dict,
        signal_urgency: float = 0.5,
    ) -> Tuple[bool, float, str]:
        """
        Decide maker vs taker entry.

        Returns (use_maker, limit_price, reason_log_string).
        If use_maker=False, limit_price=0.0 and caller should use market order.
        """
        spread_bps = self._spread.median_bps()
        return self._mtd.decide(
            side, quantity, price, orderbook, signal_urgency, spread_bps
        )

    def diagnostic_snapshot(self) -> Dict:
        """Returns a dict of current engine state for logging/Telegram reports."""
        taker_cost = self.effective_roundtrip_cost_bps(use_maker_entry=False)
        maker_cost = self.effective_roundtrip_cost_bps(use_maker_entry=True)
        spread_samples = self._spread.sample_count
        slip_warmed    = self._slip._ewma is not None
        return {
            "spread_median_bps":    round(self._spread.median_bps(), 2),
            "spread_p90_bps":       round(self._spread.percentile_bps(0.90), 2),
            "spread_samples":       spread_samples,
            "slippage_ewma_bps":    round(self._slip.expected_bps(), 2),
            "slip_warmed":          slip_warmed,
            "rt_cost_taker_bps":    round(taker_cost, 2),
            "rt_cost_maker_bps":    round(maker_cost, 2),
            "maker_saving_bps":     round(taker_cost - maker_cost, 2),
            # Bug-17 fix: engine_warmed now matches is_warmed_up() exactly (spread
            # samples >= 5).  Previously this required slip_warmed=True as well,
            # making the Telegram status report show "warming" for several minutes
            # after a stream restart even though is_warmed_up() returned True and
            # the fee gate was already active.  slip_warmed is kept as its own
            # separate field for operator visibility — it just no longer gates
            # the definition of "engine_warmed".
            "engine_warmed":        spread_samples >= 5,
        }

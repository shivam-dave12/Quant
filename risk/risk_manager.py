"""
risk/risk_manager.py — Liquidity-First Risk Manager
=====================================================
Position sizing and risk control for the liquidity-first architecture.

Key changes from VWAP-reversion model:
  - calculate_position_size() now accepts an optional pool_tp_price so the
    R:R is evaluated against the actual opposing-pool target, not a VWAP
    fraction.  When pool_tp_price is provided it is used as the TP reference
    for R:R validation; the dollar risk budget is unchanged.
  - All other risk limits (daily loss, consecutive losses, drawdown, cooldown)
    are retained unchanged — they are architecture-agnostic.
  - Balance caching and thread-safety model unchanged.
"""

import logging
import time
import threading
from typing import Dict, Optional
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from collections import deque

import sys, os as _os; sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
import config

logger = logging.getLogger(__name__)


@dataclass
class TradeRecord:
    timestamp:   float
    side:        str
    entry_price: float
    exit_price:  float
    quantity:    float
    pnl:         float
    is_win:      bool
    reason:      str


class RiskManager:
    """
    Liquidity-first risk manager.

    Position sizing model (pool-to-pool):
      1. Dollar risk  = RISK_PER_TRADE% of available balance.
      2. Notional     = dollar_risk / sl_distance_pct.
      3. qty (BTC)    = notional / entry_price.
      4. Margin cap   applied if needed.
      5. R:R validated against pool_tp_price (opposing pool sweep) when
         provided; falls back to MIN_RISK_REWARD_RATIO config guard.
    """

    def __init__(self, shared_api=None):
        self._lock = threading.RLock()

        # Performance tracking
        self.total_trades       = 0
        self.winning_trades     = 0
        self.losing_trades      = 0
        self.realized_pnl       = 0.0
        self.daily_pnl          = 0.0
        self.consecutive_losses = 0

        # Risk limits (assigned BEFORE trade-history deques)
        self.daily_loss_limit         = config.MAX_DAILY_LOSS
        self.max_consecutive_losses   = config.MAX_CONSECUTIVE_LOSSES
        self.max_daily_trades         = config.MAX_DAILY_TRADES

        # Trade history (bounded deques)
        self.trade_history: deque = deque(maxlen=1000)
        self.daily_trades:  deque = deque(maxlen=self.max_daily_trades + 10)
        self.last_trade_time = 0.0

        # Balance tracking
        self.initial_balance       = 0.0
        self.current_balance       = 0.0
        self.available_balance     = 0.0
        self.balance_cache_time    = 0.0
        self.balance_cache_ttl     = config.BALANCE_CACHE_TTL_SEC
        self._balance_fetch_in_progress = False

        # Daily reset (IST UTC+5:30)
        self._IST = timezone(timedelta(hours=5, minutes=30))
        self._last_reset_date = datetime.now(self._IST).date()

        # Deferred daily-reset state.
        # When the calendar day rolls over while a position is open,
        # _pending_reset is set True and the actual counter clearing is
        # delayed until set_position_open(False) is called (i.e. the trade
        # closes).  The closing trade is then recorded in the NEW day's books.
        self._position_is_open: bool = False
        self._pending_reset:    bool = False

        # Shared API (CoinSwitchAPI, DeltaAPI, or ExecutionRouter)
        if shared_api is not None:
            self.api = shared_api
        else:
            self.api = None
            logger.warning("RiskManager: no shared_api — balance queries disabled")

        logger.info("✅ RiskManager initialized (liquidity-first mode)")

    # =========================================================================
    # BALANCE
    # =========================================================================

    def get_available_balance(self) -> Optional[Dict]:
        """
        Get available balance with caching.

        Thread-safety: cache check under lock; REST call outside lock so the
        main trading loop is never blocked during a network round-trip.
        The _balance_fetch_in_progress flag prevents concurrent pile-up.
        GlobalRateLimiter.wait() is intentionally NOT called here — balance
        reads must not contend with order placement rate limits.
        """
        with self._lock:
            now = time.time()
            if now - self.balance_cache_time < self.balance_cache_ttl:
                return {
                    "available": self.available_balance,
                    "total":     self.current_balance,
                    "cached":    True,
                }
            if self._balance_fetch_in_progress:
                return {
                    "available": self.available_balance,
                    "total":     self.current_balance,
                    "cached":    True,
                }
            if self.api is None:
                return None
            self._balance_fetch_in_progress = True
            _fallback_avail = self.available_balance
            _fallback_total = self.current_balance

        # REST call — outside lock
        try:
            balance_data = self.api.get_balance() if hasattr(self.api, "get_balance") else None

            if balance_data is None:
                with self._lock:
                    self._balance_fetch_in_progress = False
                return {"available": _fallback_avail, "total": _fallback_total,
                        "cached": True, "error": "null response"}

            if "error" in balance_data:
                logger.error(f"Balance fetch error: {balance_data['error']}")
                with self._lock:
                    self._balance_fetch_in_progress = False
                return {"available": _fallback_avail, "total": _fallback_total,
                        "cached": True, "error": balance_data["error"]}

            available = float(balance_data.get("available", 0.0))
            locked    = float(balance_data.get("locked",    0.0))
            total     = available + locked

            with self._lock:
                self.available_balance = available
                self.current_balance   = total
                self.balance_cache_time = time.time()
                self._balance_fetch_in_progress = False
                if self.initial_balance == 0.0:
                    self.initial_balance = total
                    logger.info(f"💰 Initial balance set: ${self.initial_balance:.2f}")

            return {"available": available, "total": total, "cached": False}

        except Exception as e:
            logger.error(f"Error fetching balance: {e}", exc_info=True)
            with self._lock:
                self._balance_fetch_in_progress = False
            return {"available": _fallback_avail, "total": _fallback_total,
                    "cached": True, "error": str(e)}

    # =========================================================================
    # POSITION SIZING (pool-aware)
    # =========================================================================

    def calculate_position_size(
        self,
        entry_price:   float,
        stop_loss:     float,
        side:          str,
        pool_tp_price: Optional[float] = None,   # ← opposing pool sweep price
    ) -> Optional[float]:
        """
        Calculate BTC position size for a liquidity-pool trade.

        Sizing steps:
          1. Dollar risk  = RISK_PER_TRADE% of available balance.
          2. Notional     = dollar_risk / sl_distance_pct.
          3. qty (BTC)    = notional / entry_price.
          4. Margin cap   if margin > BALANCE_USAGE_PERCENTAGE% or
                          MAX_MARGIN_PER_TRADE → scale down.
          5. R:R guard    if pool_tp_price is supplied, verify that the
                          trade's R:R meets MIN_RISK_REWARD_RATIO before
                          returning.  A pool-based TP should always pass;
                          if it fails the pool target is too close and the
                          setup should be skipped.
          6. Hard limits  MIN/MAX_POSITION_SIZE.

        Args:
            entry_price:   Intended fill price (OTE limit or market).
            stop_loss:     ICT structural SL (sweep wick → OB → swing).
            side:          "LONG" | "SHORT".
            pool_tp_price: Price of the opposing liquidity pool (target).
                           When provided, R:R is validated before returning.
                           Pass None to skip R:R validation (used during
                           sizing preview before TP is confirmed).

        Returns:
            BTC quantity (float) or None if size is invalid.

        NOTE: No division by LEVERAGE.  `quantity` is the actual BTC position.
        Leverage is implicit — the exchange holds (quantity × price / leverage)
        as margin.  Dividing qty by leverage during sizing was a prior bug
        that produced positions 25× too small.
        """
        try:
            entry_price = float(entry_price)
            stop_loss   = float(stop_loss)
            side        = str(side).upper()

            if side not in ("LONG", "SHORT"):
                logger.error(f"Invalid side: {side}")
                return None

            # ── Balance ───────────────────────────────────────────────
            balance_info = self.get_available_balance()
            if not balance_info:
                logger.error("Failed to get balance")
                return None
            available = balance_info.get("available", 0.0)
            if available <= 0:
                logger.error(f"No available balance: {available}")
                return None

            # ── SL distance ───────────────────────────────────────────
            if side == "LONG":
                price_distance = entry_price - stop_loss
                if stop_loss >= entry_price:
                    logger.error(f"Invalid LONG SL: {stop_loss} must be < entry {entry_price}")
                    return None
            else:
                price_distance = stop_loss - entry_price
                if stop_loss <= entry_price:
                    logger.error(f"Invalid SHORT SL: {stop_loss} must be > entry {entry_price}")
                    return None

            if price_distance <= 0:
                logger.error(f"Invalid SL distance: {price_distance:.2f}")
                return None

            sl_pct = price_distance / entry_price

            if sl_pct < 0.001:
                logger.error(f"SL too tight: {sl_pct*100:.3f}% (min 0.1%)")
                return None
            if sl_pct > 0.10:
                logger.warning(f"SL very wide: {sl_pct*100:.2f}% — proceeding with caution")

            # ── Step 1: Dollar risk budget ────────────────────────────
            dollar_risk     = available * (config.RISK_PER_TRADE / 100)
            dollar_risk     = max(config.MIN_MARGIN_PER_TRADE, dollar_risk)
            max_dollar_risk = available * (min(config.RISK_PER_TRADE * 3, 5.0) / 100)
            dollar_risk     = min(dollar_risk, max_dollar_risk)

            # ── Step 2: Risk-based notional + qty ─────────────────────
            notional      = dollar_risk / sl_pct
            position_size = notional / entry_price

            # ── Step 3: Margin cap ────────────────────────────────────
            margin_required = position_size * entry_price / config.LEVERAGE
            max_margin = min(
                available * (config.BALANCE_USAGE_PERCENTAGE / 100),
                config.MAX_MARGIN_PER_TRADE
            )
            max_margin = max(max_margin, config.MIN_MARGIN_PER_TRADE)

            if margin_required > max_margin:
                scale         = max_margin / margin_required
                position_size = position_size * scale
                notional      = position_size * entry_price
                margin_required = max_margin
                logger.debug(f"Position scaled by {scale:.3f} to respect margin cap ${max_margin:.2f}")

            # ── Step 4: Hard limits ───────────────────────────────────
            position_size = max(config.MIN_POSITION_SIZE,
                                min(position_size, config.MAX_POSITION_SIZE))
            position_size = round(position_size, 4)

            if position_size < config.MIN_POSITION_SIZE:
                logger.error(
                    f"Position size too small: {position_size} BTC "
                    f"(min: {config.MIN_POSITION_SIZE})"
                )
                return None

            # ── Step 5: Pool-based R:R validation ─────────────────────
            # When a pool TP price is given, ensure the trade meets the
            # minimum R:R against the actual pool target, not a proxy.
            if pool_tp_price is not None:
                tp_distance = abs(pool_tp_price - entry_price)
                actual_rr   = tp_distance / price_distance if price_distance > 0 else 0.0
                min_rr      = float(getattr(config, "MIN_RISK_REWARD_RATIO", 0.8))
                if actual_rr < min_rr:
                    logger.warning(
                        f"Pool R:R too low: {actual_rr:.2f} "
                        f"(pool_tp=${pool_tp_price:,.2f} vs entry=${entry_price:,.2f} "
                        f"SL_dist={price_distance:.2f}) — min={min_rr}"
                    )
                    return None
                logger.debug(f"Pool R:R validated: {actual_rr:.2f}:1 "
                             f"(pool_tp=${pool_tp_price:,.2f})")

            # ── Logging ───────────────────────────────────────────────
            actual_notional    = position_size * entry_price
            actual_margin      = actual_notional / config.LEVERAGE
            actual_dollar_risk = position_size * price_distance
            actual_risk_pct    = actual_dollar_risk / available * 100

            pool_rr_str = ""
            if pool_tp_price is not None:
                tp_dist  = abs(pool_tp_price - entry_price)
                pool_rr  = tp_dist / price_distance if price_distance > 0 else 0.0
                pool_rr_str = f" | Pool R:R: {pool_rr:.2f}:1 (TP@${pool_tp_price:,.2f})"

            logger.info(
                f"✅ Position sized: {position_size:.4f} BTC | "
                f"Notional: ${actual_notional:,.0f} | "
                f"Margin: ${actual_margin:.2f} | "
                f"$ Risk @ SL: ${actual_dollar_risk:.2f} ({actual_risk_pct:.2f}%)"
                + pool_rr_str
            )
            return position_size

        except ValueError as e:
            logger.error(f"Value error in position calculation: {e}")
            return None
        except Exception as e:
            logger.error(f"Error calculating position size: {e}", exc_info=True)
            return None

    # =========================================================================
    # TRADE NOTIFICATIONS
    # =========================================================================

    def notify_entry_placed(self) -> None:
        """
        Called immediately after a limit/market entry is confirmed placed.
        Resets the last_trade_time so can_trade() cooldown works correctly
        without waiting for the trade to close.
        """
        with self._lock:
            self.last_trade_time = time.time()
            logger.debug("🔔 RiskManager: entry placed — cooldown timer reset")

    # =========================================================================
    # TRADE GATE
    # =========================================================================

    def can_trade(self) -> tuple[bool, str]:
        with self._lock:
            now = time.time()

            # Reset daily counters FIRST — must happen before any gate check
            # so yesterday's losses don't block the first trade of the new day.
            self._reset_daily_if_needed()

            # ── Min time between trades ───────────────────────────────────────
            time_since_last = now - self.last_trade_time
            if (self.last_trade_time > 0 and
                    time_since_last < config.MIN_TIME_BETWEEN_TRADES * 60):
                remaining = int(config.MIN_TIME_BETWEEN_TRADES * 60 - time_since_last)
                return False, f"Cooldown: {remaining}s remaining"

            # ── Loss cooldown ─────────────────────────────────────────────────
            cooldown = getattr(config, "TRADE_COOLDOWN_SECONDS", 300)
            if (self.consecutive_losses > 0 and
                    self.last_trade_time > 0 and
                    time_since_last < cooldown):
                remaining = int(cooldown - time_since_last)
                return False, f"Loss cooldown: {remaining}s remaining"

            # ── Daily trade limit ─────────────────────────────────────────────
            if len(self.daily_trades) >= self.max_daily_trades:
                return False, f"Daily trade limit ({self.max_daily_trades})"

            # ── Daily loss limit (USDT) ───────────────────────────────────────
            if self.daily_pnl <= -self.daily_loss_limit:
                return False, f"Daily loss limit hit (${abs(self.daily_pnl):.2f})"

            # ── Daily loss limit (% of balance) ──────────────────────────────
            if self.current_balance > 0:
                daily_loss_pct = abs(self.daily_pnl) / self.current_balance * 100
                max_daily_pct  = getattr(config, "MAX_DAILY_LOSS_PCT", 5.0)
                if self.daily_pnl < 0 and daily_loss_pct >= max_daily_pct:
                    return False, (f"Daily loss % limit hit "
                                   f"({daily_loss_pct:.1f}% >= {max_daily_pct}%)")

            # ── Max drawdown ──────────────────────────────────────────────────
            if self.initial_balance > 0 and self.current_balance > 0:
                drawdown_pct = ((self.initial_balance - self.current_balance)
                                / self.initial_balance * 100)
                max_dd = getattr(config, "MAX_DRAWDOWN_PCT", 15.0)
                if drawdown_pct >= max_dd:
                    return False, (f"Max drawdown hit "
                                   f"({drawdown_pct:.1f}% >= {max_dd}%)")

            # ── Consecutive losses ────────────────────────────────────────────
            if self.consecutive_losses >= self.max_consecutive_losses:
                hours_since_last = (now - self.last_trade_time) / 3600
                AUTO_RESET_HOURS = 4.0
                if self.last_trade_time > 0 and hours_since_last >= AUTO_RESET_HOURS:
                    logger.warning(
                        f"⚠️ Consecutive losses auto-reset: {self.consecutive_losses} losses "
                        f"but {hours_since_last:.1f}h elapsed (> {AUTO_RESET_HOURS}h). "
                        f"Market context reset — allowing new evaluation."
                    )
                    self.consecutive_losses = 0
                else:
                    remaining_h = max(0.0, AUTO_RESET_HOURS - hours_since_last)
                    return False, (
                        f"Max consecutive losses ({self.consecutive_losses}) — "
                        f"auto-reset in {remaining_h:.1f}h or at day boundary"
                    )

            return True, "OK"

    # =========================================================================
    # RECORD TRADE
    # =========================================================================

    def record_trade(
        self,
        side:          str,
        entry_price:   float,
        exit_price:    float,
        quantity:      float,
        reason:        str,
        pnl_override:  float = None,
    ):
        """
        Record a completed trade.

        P&L (futures, no leverage multiplier bug):
          gross_pnl  = price_delta × quantity
          commission = quantity × avg_price × fee_rate × 2
          net_pnl    = gross_pnl − commission

        pnl_override: use caller-supplied net PnL when strategy has already
                      computed the correct figure (avoids double-accounting).
        """
        with self._lock:
            entry_price = float(entry_price)
            exit_price  = float(exit_price)
            quantity    = float(quantity)

            # If a midnight reset was deferred because a position was open,
            # apply it NOW — before recording this trade — so the closing
            # trade is counted in the new day, not the previous one.
            if self._pending_reset:
                logger.info(
                    "🔄 Applying deferred daily reset inside record_trade "
                    "(position closed — booking trade in the new day)."
                )
                self._apply_daily_reset()

            if pnl_override is not None:
                pnl = float(pnl_override)
            else:
                if side.upper() == "LONG":
                    gross_pnl = (exit_price - entry_price) * quantity
                else:
                    gross_pnl = (entry_price - exit_price) * quantity
                fee_rate   = getattr(config, "COMMISSION_RATE", 0.00055)
                commission = (entry_price + exit_price) * quantity * fee_rate
                pnl        = gross_pnl - commission
                logger.debug(
                    f"P&L breakdown: gross=${gross_pnl:+.4f} "
                    f"commission=${commission:.4f} net=${pnl:+.4f}"
                )

            is_win = pnl > 0

            notional_at_entry = entry_price * quantity
            margin_used       = notional_at_entry / config.LEVERAGE if config.LEVERAGE > 0 else notional_at_entry
            return_on_margin  = (pnl / margin_used * 100) if margin_used > 0 else 0.0

            trade = TradeRecord(
                timestamp   = time.time(),
                side        = side,
                entry_price = entry_price,
                exit_price  = exit_price,
                quantity    = quantity,
                pnl         = pnl,
                is_win      = is_win,
                reason      = reason,
            )

            self.trade_history.append(trade)
            self.daily_trades.append(trade)
            self.total_trades    += 1
            self.realized_pnl   += pnl
            self.daily_pnl      += pnl
            self.last_trade_time = time.time()

            if is_win:
                self.winning_trades     += 1
                self.consecutive_losses  = 0
            else:
                self.losing_trades      += 1
                self.consecutive_losses += 1

            logger.info(
                f"📊 Trade recorded: {side.upper()} | "
                f"Net P&L: ${pnl:+.2f} | "
                f"Return on margin: {return_on_margin:+.2f}% | "
                f"Qty: {quantity:.4f} BTC | "
                f"Total trades: {self.total_trades}"
            )

    # =========================================================================
    # DAILY RESET
    # =========================================================================

    def _apply_daily_reset(self) -> None:
        """
        Perform the actual counter reset.  Called under self._lock.
        Separated from _reset_daily_if_needed so it can be triggered both
        immediately (no open position) and deferred (position was open at
        midnight — fired from set_position_open / record_trade instead).
        """
        today          = datetime.now(self._IST).date()
        prev_day       = self._last_reset_date
        prev_cons_loss = self.consecutive_losses
        prev_daily_pnl = self.daily_pnl
        prev_n_trades  = len(self.daily_trades)

        self.daily_trades.clear()
        self.daily_pnl          = 0.0
        self.consecutive_losses = 0
        self._last_reset_date   = today
        self._pending_reset     = False

        logger.info(
            f"🔄 Daily reset: {prev_day} → {today} | "
            f"prev daily_pnl=${prev_daily_pnl:+.2f} | "
            f"prev consecutive_losses={prev_cons_loss} (reset to 0) | "
            f"prev daily_trades={prev_n_trades}"
        )

    def _reset_daily_if_needed(self) -> None:
        """
        Check whether the IST calendar day has rolled over and act:

        • No open position → apply reset immediately (normal path).
        • Position open    → defer: set _pending_reset = True and log once.
                             The reset will fire inside record_trade() /
                             set_position_open(False) when the trade closes,
                             so the closing trade is booked in the NEW day.

        Must be called under self._lock.
        """
        today = datetime.now(self._IST).date()

        if not hasattr(self, '_last_reset_date'):
            self._last_reset_date = today
            return

        if today <= self._last_reset_date and not self._pending_reset:
            return

        # Day has changed (or a deferred reset is waiting).
        if self._position_is_open:
            # Defer — mark pending once so we don't spam the log every tick.
            if not self._pending_reset:
                logger.info(
                    f"⏳ Daily reset deferred (position open at midnight) — "
                    f"will apply when the open trade closes and be counted "
                    f"in {today}."
                )
                self._pending_reset = True
            return

        # No open position — reset immediately.
        self._apply_daily_reset()

    # =========================================================================
    # POSITION STATE NOTIFIER
    # =========================================================================

    def set_position_open(self, is_open: bool) -> None:
        """
        Call this whenever a position is opened or closed so the risk manager
        can handle the deferred midnight reset correctly.

        • set_position_open(True)  — called when an entry is filled.
        • set_position_open(False) — called when the position is fully flat.

        If a midnight reset was pending (position was open when the calendar
        day changed), it is applied here the moment the position closes so
        that record_trade() — which is called immediately after — books the
        closing trade into the new day.
        """
        with self._lock:
            self._position_is_open = is_open
            if not is_open and self._pending_reset:
                logger.info(
                    "🔄 Applying deferred daily reset — position now closed."
                )
                self._apply_daily_reset()

    # =========================================================================
    # STATISTICS
    # =========================================================================

    def get_statistics(self) -> Dict:
        """Comprehensive risk and performance statistics."""
        with self._lock:
            total  = self.total_trades
            wins   = self.winning_trades
            losses = self.losing_trades

            win_rate = (wins / total * 100) if total > 0 else 0.0

            win_pnls  = [t.pnl for t in self.trade_history if t.is_win]
            loss_pnls = [t.pnl for t in self.trade_history if not t.is_win]
            avg_win   = (sum(win_pnls)  / len(win_pnls))  if win_pnls  else 0.0
            avg_loss  = (sum(loss_pnls) / len(loss_pnls)) if loss_pnls else 0.0

            gross_wins   = sum(win_pnls)
            gross_losses = abs(sum(loss_pnls))
            profit_factor = (gross_wins / gross_losses) if gross_losses > 0 else float("inf")

            expectancy = (win_rate / 100 * avg_win) + ((1 - win_rate / 100) * avg_loss)

            peak = 0.0
            equity = 0.0
            max_dd = 0.0
            for t in self.trade_history:
                equity += t.pnl
                if equity > peak:
                    peak = equity
                dd = peak - equity
                if dd > max_dd:
                    max_dd = dd

            streak = 0
            if self.trade_history:
                last_win = list(self.trade_history)[-1].is_win
                for t in reversed(list(self.trade_history)):
                    if t.is_win == last_win:
                        streak += 1
                    else:
                        break
                streak = streak if last_win else -streak

            return {
                "total_trades":       total,
                "winning_trades":     wins,
                "losing_trades":      losses,
                "daily_trades":       len(self.daily_trades),
                "win_rate":           round(win_rate, 2),
                "avg_win":            round(avg_win, 4),
                "avg_loss":           round(avg_loss, 4),
                "profit_factor":      round(profit_factor, 3),
                "expectancy":         round(expectancy, 4),
                "realized_pnl":       round(self.realized_pnl, 4),
                "daily_pnl":          round(self.daily_pnl, 4),
                "max_drawdown":       round(max_dd, 4),
                "consecutive_losses": self.consecutive_losses,
                "current_streak":     streak,
                "current_balance":    self.current_balance,
            }

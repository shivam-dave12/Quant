"""
Risk Manager - Industry Grade
Comprehensive risk management with position tracking
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
    timestamp: float
    side: str
    entry_price: float
    exit_price: float
    quantity: float
    pnl: float
    is_win: bool
    reason: str

class RiskManager:
    """Industry-grade risk manager"""

    def __init__(self, shared_api=None):
        self._lock = threading.RLock()

        # Performance tracking
        self.total_trades = 0
        self.winning_trades = 0
        self.losing_trades = 0
        self.realized_pnl = 0.0
        self.daily_pnl = 0.0
        self.consecutive_losses = 0

        # Trade history
        self.trade_history: deque[TradeRecord] = deque(maxlen=1000)
        self.daily_trades = []
        self.last_trade_time = 0.0

        # Risk limits
        self.daily_loss_limit = config.MAX_DAILY_LOSS
        self.max_consecutive_losses = config.MAX_CONSECUTIVE_LOSSES
        self.max_daily_trades = config.MAX_DAILY_TRADES

        # Balance tracking
        self.initial_balance = 0.0
        self.current_balance   = 0.0   # total wallet equity (available + locked)
        self.available_balance = 0.0   # free margin only — used for position sizing
        self.balance_cache_time = 0.0
        self.balance_cache_ttl = config.BALANCE_CACHE_TTL_SEC
        # Concurrency guard: prevents multiple threads piling into a REST balance fetch.
        # Callers that arrive while a fetch is in progress return the cached value
        # immediately instead of waiting — the main loop is never blocked.
        self._balance_fetch_in_progress = False

        # Daily reset tracking — date-anchored using IST (UTC+5:30)
        # Must match DailyRiskGate in quant_strategy to prevent cross-day PnL drift.
        self._IST = timezone(timedelta(hours=5, minutes=30))
        self._last_reset_date = datetime.now(self._IST).date()

        # Shared API — accepts CoinSwitchAPI, DeltaAPI, or ExecutionRouter.
        if shared_api is not None:
            self.api = shared_api
        else:
            self.api = None
            logger.warning("RiskManager: no shared_api — balance queries disabled")

        logger.info("✅ RiskManager initialized")

    def get_available_balance(self) -> Optional[Dict]:
        """
        Get available balance with caching.

        Thread-safety model:
          - Cache check and state update are lock-protected (fast, in-memory).
          - The REST call is made OUTSIDE the lock so the main loop is never
            blocked waiting for a network round-trip.
          - A _balance_fetch_in_progress flag prevents concurrent fetches from
            piling up: if a fetch is already running, callers return the cached
            value immediately rather than waiting up to 30s for a HTTP timeout.

        Critical: GlobalRateLimiter.wait() is intentionally NOT called here.
          The global rate limiter governs ORDER placement (cancel, place, modify).
          Balance reads are independent REST calls on a separate endpoint and
          must never contend with the order rate limiter — doing so blocked the
          main loop for up to 3 seconds every 35s (one full limiter interval).
        """
        with self._lock:
            now = time.time()

            # Fast path: valid cache — return immediately without any I/O
            if now - self.balance_cache_time < self.balance_cache_ttl:
                return {
                    "available": self.available_balance,
                    "total":     self.current_balance,
                    "cached":    True,
                }

            # If another thread is already fetching, return cached value rather
            # than blocking. The fresh data will be available on the next call.
            if self._balance_fetch_in_progress:
                return {
                    "available": self.available_balance,
                    "total":     self.current_balance,
                    "cached":    True,
                }

            if self.api is None:
                return None

            self._balance_fetch_in_progress = True
            # Snapshot cached values for fallback — used outside the lock
            _fallback_avail = self.available_balance
            _fallback_total = self.current_balance

        # ── REST call — deliberately outside the lock ─────────────────────────
        # The lock is not held during the HTTP request so on_tick, trail
        # management, and all other threads continue running normally.
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

            # ── Re-acquire lock to update shared state ─────────────────────────
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
        # NOTE: No finally block here.  The flag is cleared in every explicit
        # return path above (null response, error key, success block, exception
        # handler).  A finally block would re-acquire the lock and unconditionally
        # clear the flag — which would race against a NEW fetch that another thread
        # legitimately started between the last flag-clear inside the try/except
        # and the finally executing, silently clearing the wrong fetch's flag and
        # opening a window for duplicate concurrent fetches.


    def calculate_position_size(
        self,
        entry_price: float,
        stop_loss: float,
        side: str
    ) -> Optional[float]:
        """
        Calculate position size (BTC quantity to send to exchange).

        Industry-standard risk-based sizing:
          1. Dollar risk = RISK_PER_TRADE% of available balance
             (the maximum dollar loss accepted if SL fires, e.g. 0.60% of balance)
          2. Notional = dollar_risk / sl_distance_pct
          3. qty (BTC) = notional / entry_price
          4. Margin cap: if margin > BALANCE_USAGE_PERCENTAGE% of balance or
             MAX_MARGIN_PER_TRADE, scale down (capital allocation guard)
          5. Apply MIN/MAX_POSITION_SIZE hard limits

        NOTE: No division by LEVERAGE here.  The exchange uses the qty you
        send as the ACTUAL BTC position.  Leverage only determines how much
        margin is held against that notional.  Dividing qty by leverage was
        a previous bug that made actual positions 25× too small and required
        a compensating ×LEVERAGE in PnL — both are now removed.
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

            # ── SL distance validation ────────────────────────────────
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

            sl_pct = price_distance / entry_price          # e.g. 0.005 = 0.5%

            if sl_pct < 0.001:
                logger.error(f"SL too tight: {sl_pct*100:.3f}% (min 0.1%)")
                return None
            if sl_pct > 0.10:
                logger.warning(f"SL very wide: {sl_pct*100:.2f}% — proceeding with caution")

            # ── Step 1: Dollar risk budget ────────────────────────────
            # RISK_PER_TRADE is the % of balance we accept LOSING if SL fires.
            # e.g. RISK_PER_TRADE=0.60 → risk $6 on a $1000 balance.
            # MAX_MARGIN_PER_TRADE is a capital allocation cap (Step 3), NOT a
            # risk budget limit. Clamping dollar_risk by MAX_MARGIN confuses
            # "how much I can lose" with "how much exchange holds as collateral".
            dollar_risk = available * (config.RISK_PER_TRADE / 100)
            # Floor only: ensure minimum viable trade size
            dollar_risk = max(config.MIN_MARGIN_PER_TRADE, dollar_risk)
            # Ceiling: cap at a sensible multiple of the risk percentage
            # to prevent extreme sizing on very wide SLs
            max_dollar_risk = available * (min(config.RISK_PER_TRADE * 3, 5.0) / 100)
            dollar_risk = min(dollar_risk, max_dollar_risk)

            # ── Step 2: Risk-based notional + qty ─────────────────────
            # If we risk $dollar_risk at sl_pct, we can hold this notional:
            notional = dollar_risk / sl_pct            # e.g. $60 / 0.005 = $12,000
            position_size = notional / entry_price     # e.g. $12,000 / $90,000 = 0.133 BTC

            # ── Step 3: Margin cap ────────────────────────────────────
            # Ensure the margin required does not exceed the hard limits.
            # margin_required = notional / leverage  (what the exchange holds)
            margin_required = position_size * entry_price / config.LEVERAGE
            max_margin = min(
                available * (config.BALANCE_USAGE_PERCENTAGE / 100),
                config.MAX_MARGIN_PER_TRADE
            )
            max_margin = max(max_margin, config.MIN_MARGIN_PER_TRADE)

            if margin_required > max_margin:
                scale = max_margin / margin_required
                position_size = position_size * scale
                notional      = position_size * entry_price
                margin_required = max_margin
                logger.debug(
                    f"Position scaled by {scale:.3f} to respect "
                    f"margin cap ${max_margin:.2f}"
                )

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

            # ── Logging ───────────────────────────────────────────────
            actual_notional      = position_size * entry_price
            actual_margin        = actual_notional / config.LEVERAGE
            actual_dollar_risk   = position_size * price_distance   # loss if SL fires
            actual_risk_pct      = actual_dollar_risk / available * 100

            logger.info(
                f"✅ Position sized: {position_size:.4f} BTC | "
                f"Notional: ${actual_notional:,.0f} | "
                f"Margin: ${actual_margin:.2f} | "
                f"$ Risk @ SL: ${actual_dollar_risk:.2f} ({actual_risk_pct:.2f}% of balance) | "
                f"SL dist: {sl_pct*100:.3f}%"
            )
            return position_size

        except ValueError as e:
            logger.error(f"Value error in position calculation: {e}")
            return None
        except Exception as e:
            logger.error(f"Error calculating position size: {e}", exc_info=True)
            return None

    def notify_entry_placed(self) -> None:
        """
        Called by strategy._execute_entry immediately after limit order
        is confirmed placed. Updates last_trade_time so can_trade() cooldown
        works correctly without waiting for the trade to close.
        """
        with self._lock:
            self.last_trade_time = time.time()
            logger.debug("🔔 RiskManager: entry placed — cooldown timer reset")


    def can_trade(self) -> tuple[bool, str]:
        with self._lock:
            now = time.time()

            # ── Reset daily counters FIRST — must happen before any check ─────
            # If the calendar day just changed, consecutive_losses and daily_pnl
            # must be zeroed before the cooldown / loss-limit gates see them.
            # Placing this call below the cooldown checks meant yesterday's losses
            # blocked the first trade of the new trading day.
            self._reset_daily_if_needed()

            # ── Min time between trades ───────────────────────────────────────
            time_since_last = now - self.last_trade_time
            if (self.last_trade_time > 0 and
                    time_since_last < config.MIN_TIME_BETWEEN_TRADES * 60):
                remaining = int(
                    config.MIN_TIME_BETWEEN_TRADES * 60 - time_since_last)
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

            # ── Daily loss limit (% of balance) — new ────────────────────────
            if self.current_balance > 0:
                daily_loss_pct = abs(self.daily_pnl) / self.current_balance * 100
                max_daily_pct  = getattr(config, "MAX_DAILY_LOSS_PCT", 5.0)
                if self.daily_pnl < 0 and daily_loss_pct >= max_daily_pct:
                    return False, (f"Daily loss % limit hit "
                                f"({daily_loss_pct:.1f}% >= {max_daily_pct}%)")

            # ── Max drawdown check (new) ──────────────────────────────────────
            if self.initial_balance > 0 and self.current_balance > 0:
                drawdown_pct = ((self.initial_balance - self.current_balance)
                                / self.initial_balance * 100)
                max_dd = getattr(config, "MAX_DRAWDOWN_PCT", 15.0)
                if drawdown_pct >= max_dd:
                    return False, (f"Max drawdown hit "
                                f"({drawdown_pct:.1f}% >= {max_dd}%)")

            # ── Consecutive losses ────────────────────────────────────────────
            if self.consecutive_losses >= self.max_consecutive_losses:
                # Auto-reset after 4 hours: prevents infinite deadlock where
                # losses block trading → can't trade → can't win → never resets.
                # After 4h the market context has changed enough to try again.
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


    def record_trade(
        self,
        side: str,
        entry_price: float,
        exit_price: float,
        quantity: float,
        reason: str,
        pnl_override: float = None,
    ):
        """Record completed trade with industry-grade P&L accounting.

        P&L formula (futures):
            gross_pnl  = price_delta × quantity          (BTC qty × $ move)
            commission = quantity × avg_price × fee_rate × 2  (entry + exit)
            net_pnl    = gross_pnl − commission

        NOTE: NO multiplication by LEVERAGE.  `quantity` is the actual BTC
        quantity held on the exchange.  The leverage is implicit in the fact
        that you controlled `quantity × price` notional with only
        `quantity × price / leverage` margin.  Multiplying by leverage again
        was an accounting error introduced when position_size was (incorrectly)
        divided by leverage during sizing.  Both bugs are now removed.

        Args:
            pnl_override: If provided, use this as the final net dollar PnL.
                         Used by strategy._on_position_closed which already
                         computes the leveraged-correct figure.
        """
        with self._lock:
            entry_price = float(entry_price)
            exit_price  = float(exit_price)
            quantity    = float(quantity)

            if pnl_override is not None:
                # Caller already computed the correct net PnL
                pnl = float(pnl_override)
            else:
                # ── Gross PnL ──────────────────────────────────────────
                if side.upper() == "LONG":
                    gross_pnl = (exit_price - entry_price) * quantity
                else:
                    gross_pnl = (entry_price - exit_price) * quantity

                # ── Commission (both legs, taker rate) ─────────────────
                # CoinSwitch typical taker fee ≈ 0.055% per side.
                # Adjust COMMISSION_RATE in config if you know the exact rate.
                fee_rate   = getattr(config, "COMMISSION_RATE", 0.00055)
                commission = (entry_price + exit_price) * quantity * fee_rate
                pnl        = gross_pnl - commission

                logger.debug(
                    f"P&L breakdown: gross=${gross_pnl:+.4f} "
                    f"commission=${commission:.4f} net=${pnl:+.4f}"
                )

            is_win = pnl > 0

            # ── Return on margin ───────────────────────────────────────
            notional_at_entry = entry_price * quantity
            margin_used       = notional_at_entry / config.LEVERAGE if config.LEVERAGE > 0 else notional_at_entry
            return_on_margin  = (pnl / margin_used * 100) if margin_used > 0 else 0.0

            # ── Create record ──────────────────────────────────────────
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

            # ── Update statistics ──────────────────────────────────────
            self.trade_history.append(trade)
            self.daily_trades.append(trade)
            self.total_trades   += 1
            self.realized_pnl   += pnl
            self.daily_pnl      += pnl
            self.last_trade_time = time.time()

            if is_win:
                self.winning_trades  += 1
                self.consecutive_losses = 0
            else:
                self.losing_trades       += 1
                self.consecutive_losses  += 1

            logger.info(
                f"📊 Trade recorded: {side.upper()} | "
                f"Net P&L: ${pnl:+.2f} | "
                f"Return on margin: {return_on_margin:+.2f}% | "
                f"Qty: {quantity:.4f} BTC | "
                f"Total trades: {self.total_trades}"
            )

    def _reset_daily_if_needed(self):
        """
        Reset all daily counters when a new IST calendar day begins.

        Uses IST (UTC+5:30) to match DailyRiskGate in quant_strategy.
        This prevents cross-day PnL drift where a trade opening before
        midnight UTC and closing after causes PnL to be assigned to the
        wrong day.
        """
        today = datetime.now(self._IST).date()

        if not hasattr(self, '_last_reset_date'):
            self._last_reset_date = today
            return

        if today <= self._last_reset_date:
            return   # Same day — nothing to reset

        # ── New day detected ──────────────────────────────────────────
        prev_day        = self._last_reset_date
        prev_cons_loss  = self.consecutive_losses
        prev_daily_pnl  = self.daily_pnl
        prev_n_trades   = len(self.daily_trades)

        self.daily_trades       = []
        self.daily_pnl          = 0.0
        self.consecutive_losses = 0   # ← CRITICAL: unlocks the infinite deadlock
        self._last_reset_date   = today

        logger.info(
            f"🔄 Daily reset: {prev_day} → {today} | "
            f"prev daily_pnl=${prev_daily_pnl:+.2f} | "
            f"prev consecutive_losses={prev_cons_loss} (reset to 0) | "
            f"prev daily_trades={prev_n_trades}"
        )

    def get_statistics(self) -> Dict:
        """Get comprehensive risk and performance statistics."""
        with self._lock:
            total  = self.total_trades
            wins   = self.winning_trades
            losses = self.losing_trades

            win_rate = (wins / total * 100) if total > 0 else 0.0

            # Average win / loss
            win_pnls  = [t.pnl for t in self.trade_history if t.is_win]
            loss_pnls = [t.pnl for t in self.trade_history if not t.is_win]
            avg_win   = (sum(win_pnls)  / len(win_pnls))  if win_pnls  else 0.0
            avg_loss  = (sum(loss_pnls) / len(loss_pnls)) if loss_pnls else 0.0

            # Profit factor = gross wins / |gross losses|
            gross_wins   = sum(win_pnls)
            gross_losses = abs(sum(loss_pnls))
            profit_factor = (gross_wins / gross_losses) if gross_losses > 0 else float("inf")

            # Expectancy per trade
            expectancy = (win_rate / 100 * avg_win) + ((1 - win_rate / 100) * avg_loss)

            # Max drawdown on trade history
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

            # Current streak
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
                # Volume
                "total_trades":         total,
                "winning_trades":       wins,
                "losing_trades":        losses,
                "daily_trades":         len(self.daily_trades),
                # Performance
                "win_rate":             round(win_rate, 2),
                "avg_win":              round(avg_win, 4),
                "avg_loss":             round(avg_loss, 4),
                "profit_factor":        round(profit_factor, 3),
                "expectancy":           round(expectancy, 4),
                # P&L
                "realized_pnl":         round(self.realized_pnl, 4),
                "daily_pnl":            round(self.daily_pnl, 4),
                "max_drawdown":         round(max_dd, 4),
                # Risk
                "consecutive_losses":   self.consecutive_losses,
                "current_streak":       streak,
                "current_balance":      self.current_balance,
            }

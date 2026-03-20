"""
main.py — Unified Dual-Exchange Quant Bot
==========================================
Entry point for the combined CoinSwitch + Delta Exchange trading bot.

Architecture:
  - Both exchange data managers start concurrently
  - MarketAggregator fuses orderbook depth + CVD + tick flow from both
  - Candles come exclusively from the primary (execution) exchange
  - ExecutionRouter owns both OrderManagers; routes to the active one
  - /setexchange <delta|coinswitch> switches execution at runtime
    (blocked while a position is open; no restart needed)

Startup sequence:
  1. Validate credentials for all configured exchanges
  2. Construct API clients + OrderManagers for each exchange
  3. Build ExecutionRouter (default from config.EXECUTION_EXCHANGE)
  4. Construct data managers + MarketAggregator
  5. Set leverage on active exchange
  6. Start aggregator (boots both DMs concurrently)
  7. Wait for primary DM readiness
  8. Start strategy + main loop
"""

from __future__ import annotations

import io
import logging
import signal
import sys
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import config

# ── IST Timezone logging ──────────────────────────────────────────────────────

IST = timezone(timedelta(hours=5, minutes=30))


class ISTFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=IST)
        s  = dt.strftime("%Y-%m-%d %H:%M:%S")
        return f"{s},{int(record.msecs):03d}"


_ist_fmt = ISTFormatter(fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

_file_handler = logging.FileHandler("quant_bot.log", encoding="utf-8")
_file_handler.setFormatter(_ist_fmt)

_stream_handler = logging.StreamHandler(
    stream=io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    if hasattr(sys.stdout, "buffer") else sys.stdout
)
_stream_handler.setFormatter(_ist_fmt)

logging.basicConfig(
    level=getattr(config, "LOG_LEVEL", "INFO"),
    handlers=[_file_handler, _stream_handler],
)
logger = logging.getLogger(__name__)

# ── Global exception hooks — no silent crash ever again ──────────────────────

def _log_uncaught(exc_type, exc_value, exc_tb):
    """Log any uncaught main-thread exception before the process dies."""
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_tb)
        return
    logger.critical("💀 UNCAUGHT EXCEPTION — process is dying",
                    exc_info=(exc_type, exc_value, exc_tb))
    try:
        from telegram.notifier import send_telegram_message as _stm
        import traceback as _tb
        _stm(f"💀 <b>BOT CRASH</b>\n"
             f"<code>{''.join(_tb.format_exception(exc_type, exc_value, exc_tb))[-1500:]}</code>")
    except Exception:
        pass

sys.excepthook = _log_uncaught


def _log_thread_exception(args):
    """Log any uncaught exception in a daemon/worker thread."""
    if args.exc_type is SystemExit:
        return
    logger.error(
        "💥 UNCAUGHT THREAD EXCEPTION in thread '%s'",
        args.thread.name if args.thread else "unknown",
        exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
    )

threading.excepthook = _log_thread_exception

# ── Deferred imports (after path is set up) ───────────────────────────────────
from exchanges.coinswitch.api          import FuturesAPI  as CoinSwitchAPI
from exchanges.coinswitch.data_manager import CoinSwitchDataManager
from exchanges.delta.api               import DeltaAPI
from exchanges.delta.data_manager      import DeltaDataManager
from aggregator.market_aggregator      import MarketAggregator
from execution.order_manager           import OrderManager, CancelResult
from execution.router                  import ExecutionRouter
from risk.risk_manager                 import RiskManager
from strategy.quant_strategy           import QuantStrategy
from telegram.notifier import (
    install_global_telegram_log_handler,
    send_telegram_message,
)

install_global_telegram_log_handler(level=logging.WARNING, throttle_seconds=5.0)


# ─────────────────────────────────────────────────────────────────────────────
# QUANT BOT
# ─────────────────────────────────────────────────────────────────────────────

class QuantBot:
    """
    Unified dual-exchange quant bot.

    Public attributes accessed by the Telegram controller:
      .strategy          — QuantStrategy
      .order_manager     — ExecutionRouter (proxies to active OrderManager)
      .risk_manager      — RiskManager
      .data_manager      — MarketAggregator
      .execution_router  — ExecutionRouter (same object as order_manager)
      .trading_enabled   — bool (paused by /pause command)
    """

    def __init__(self) -> None:
        self.running               = False
        self.last_health_check_sec = 0.0
        self.last_report_sec       = 0.0
        self.last_heartbeat_sec    = 0.0

        self.data_manager:     Optional[MarketAggregator]  = None
        self.execution_router: Optional[ExecutionRouter]   = None
        self.order_manager:    Optional[ExecutionRouter]   = None  # alias
        self.risk_manager:     Optional[RiskManager]       = None
        self.strategy:         Optional[QuantStrategy]     = None

        self.trading_enabled      = True
        self.trading_pause_reason = ""

    # =========================================================================
    # INITIALIZE
    # =========================================================================

    def initialize(self) -> bool:
        try:
            logger.info("=" * 80)
            logger.info("⚡ UNIFIED QUANT BOT v5 — DUAL-EXCHANGE AGGREGATION")
            logger.info("   VWAP | CVD | Orderbook | Tick Flow | ICT Confluence")
            logger.info(f"   Symbol: {config.SYMBOL} | Leverage: {config.LEVERAGE}x | "
                        f"Execution: {config.EXECUTION_EXCHANGE.upper()}")
            logger.info("=" * 80)

            has_delta      = bool(config.DELTA_API_KEY and config.DELTA_SECRET_KEY)
            has_coinswitch = bool(config.COINSWITCH_API_KEY and config.COINSWITCH_SECRET_KEY)

            logger.info(f"Exchanges configured — Delta: {has_delta} | "
                        f"CoinSwitch: {has_coinswitch}")

            # ── Build API clients ─────────────────────────────────────────────
            cs_api    = None
            delta_api = None

            if has_coinswitch:
                cs_api = CoinSwitchAPI(
                    api_key    = config.COINSWITCH_API_KEY,
                    secret_key = config.COINSWITCH_SECRET_KEY,
                )
                logger.info("✅ CoinSwitch API client ready")

            if has_delta:
                delta_api = DeltaAPI(
                    api_key    = config.DELTA_API_KEY,
                    secret_key = config.DELTA_SECRET_KEY,
                    testnet    = getattr(config, "DELTA_TESTNET", False),
                )
                logger.info("✅ Delta API client ready")

            # ── Build OrderManagers ───────────────────────────────────────────
            cs_om    = None
            delta_om = None

            if cs_api:
                cs_om = OrderManager(cs_api, exchange_name="coinswitch")
            if delta_api:
                delta_om = OrderManager(delta_api, exchange_name="delta")

            # ── Build ExecutionRouter ─────────────────────────────────────────
            self.execution_router = ExecutionRouter(
                coinswitch_om = cs_om,
                delta_om      = delta_om,
                default       = config.EXECUTION_EXCHANGE,
            )
            self.order_manager = self.execution_router  # alias for controller

            # ── Build data managers ───────────────────────────────────────────
            # Determine which is primary (execution exchange) and which secondary
            exec_exch = config.EXECUTION_EXCHANGE.lower()

            if exec_exch == "delta" and has_delta:
                primary_dm   = DeltaDataManager()
                secondary_dm = CoinSwitchDataManager() if has_coinswitch else None
            elif exec_exch == "coinswitch" and has_coinswitch:
                primary_dm   = CoinSwitchDataManager()
                secondary_dm = DeltaDataManager() if has_delta else None
            elif has_delta:
                logger.warning(f"Requested execution exchange '{exec_exch}' not configured "
                               f"— falling back to delta")
                primary_dm   = DeltaDataManager()
                secondary_dm = CoinSwitchDataManager() if has_coinswitch else None
            else:
                primary_dm   = CoinSwitchDataManager()
                secondary_dm = None

            # ── Aggregator ────────────────────────────────────────────────────
            self.data_manager = MarketAggregator(
                primary_dm   = primary_dm,
                secondary_dm = secondary_dm,
            )

            # ── Risk manager ──────────────────────────────────────────────────
            # Pass the router itself (not .api) so get_balance() always delegates
            # to whichever exchange is currently active — survives /setexchange.
            self.risk_manager = RiskManager(
                shared_api = self.execution_router
            )

            # ── Strategy ──────────────────────────────────────────────────────
            self.strategy = QuantStrategy(self.execution_router)
            self.data_manager.register_strategy(self.strategy)

            logger.info("✅ Unified quant bot initialised")
            return True

        except Exception:
            logger.exception("❌ Failed to initialise quant bot")
            return False

    # =========================================================================
    # START
    # =========================================================================

    def start(self) -> bool:
        try:
            if not all([self.execution_router, self.risk_manager,
                        self.data_manager, self.strategy]):
                logger.error("Bot components not initialised — call initialize() first")
                return False

            # ── Set leverage on active exchange ───────────────────────────────
            active_exch = self.execution_router.active_exchange
            logger.info(f"Setting leverage to {config.LEVERAGE}x on {active_exch}...")
            try:
                resp = self.execution_router.set_leverage(leverage=int(config.LEVERAGE))
                if isinstance(resp, dict):
                    if resp.get("success") or not resp.get("error"):
                        logger.info(f"✅ Leverage set to {config.LEVERAGE}x")
                    else:
                        logger.warning(f"⚠️  Leverage set: {resp.get('error', resp)}")
            except Exception as e:
                logger.warning(f"⚠️  Leverage set failed (non-fatal): {e}")

            # ── Query initial balance ─────────────────────────────────────────
            try:
                bal = self.execution_router.get_balance()
                if bal and not bal.get("error"):
                    avail = float(bal.get("available", 0))
                    total = float(bal.get("total", avail))
                    currency = bal.get("currency", "USD")
                    logger.info(f"Balance — Available: ${avail:,.2f} | "
                                f"Total: ${total:,.2f} {currency}")
                elif bal and bal.get("error"):
                    logger.warning(f"Balance query: {bal['error']}")
            except Exception as e:
                logger.warning(f"Balance query failed (non-fatal): {e}")

            # ── Start data streams ────────────────────────────────────────────
            logger.info("Starting data streams (both exchanges)...")
            if not self.data_manager.start():
                logger.error("❌ Failed to start data streams")
                return False

            logger.info("Waiting for primary exchange data readiness...")
            ready = self.data_manager.wait_until_ready(
                timeout_sec=float(config.READY_TIMEOUT_SEC))
            if not ready:
                logger.error("❌ DataManager not ready within timeout")
                return False

            price = self.data_manager.get_last_price()
            agg_status = self.data_manager.get_secondary_status()
            logger.info(f"✅ Data ready. Price: ${price:,.2f} | "
                        f"Dual-feed: {agg_status['alive']}")

            self.running = True

            # ── Startup Telegram notification ─────────────────────────────────
            from strategy.quant_strategy import QCfg
            dual_feed = agg_status["alive"]
            secondary_name = agg_status.get("secondary", "none")
            send_telegram_message(
                "⚡ <b>UNIFIED QUANT BOT v5 STARTED</b>\n\n"
                f"Symbol:    {QCfg.SYMBOL()}\n"
                f"Price:     ${price:,.2f}\n"
                f"Execution: {self.execution_router.active_exchange.upper()}\n"
                f"Leverage:  {QCfg.LEVERAGE()}x\n"
                f"Margin:    {QCfg.MARGIN_PCT():.0%} per trade\n"
                f"Entry:     VWAP dev &gt; {QCfg.VWAP_ENTRY_ATR_MULT()}×ATR\n"
                f"SL:        Swing + {QCfg.SL_BUFFER_ATR_MULT()}×ATR\n"
                f"TP:        {QCfg.TP_VWAP_FRACTION():.0%} back to VWAP\n"
                f"Min R:R:   {QCfg.MIN_RR_RATIO()}\n\n"
                f"📡 <b>Data feed:</b> "
                f"{'DUAL — ' + secondary_name.upper() + ' secondary active' if dual_feed else 'SINGLE — primary only'}\n"
                f"<i>Weights: VWAP={QCfg.W_VWAP_DEV()} CVD={QCfg.W_CVD_DIV()} "
                f"OB={QCfg.W_OB()} TF={QCfg.W_TICK_FLOW()} VEX={QCfg.W_VOL_EXHAUSTION()}</i>\n\n"
                f"<i>/setexchange delta|coinswitch to switch execution exchange</i>"
            )

            logger.info("🚀 UNIFIED QUANT BOT RUNNING")
            return True

        except Exception:
            logger.exception("❌ Error starting quant bot")
            return False

    # =========================================================================
    # HEARTBEAT
    # =========================================================================

    def maybe_log_heartbeat(self) -> None:
        now = time.time()
        if now - self.last_heartbeat_sec < 60.0:
            return
        self.last_heartbeat_sec = now

        price = self.data_manager.get_last_price() if self.data_manager else 0.0
        pos   = self.strategy.get_position()       if self.strategy   else None
        agg   = self.data_manager.get_secondary_status() if self.data_manager else {}
        feed  = "dual" if agg.get("alive") else "single"

        if pos:
            side  = pos.get("side", "?").upper()
            entry = pos.get("entry_price", 0.0)
            sl    = pos.get("sl_price", 0.0)
            tp    = pos.get("tp_price", 0.0)
            pnl   = (price - entry) if side == "LONG" else (entry - price)
            logger.info(
                f"💓 ${price:,.2f} [{feed}] | IN {side} @ ${entry:,.2f} | "
                f"SL ${sl:,.2f}  TP ${tp:,.2f} | unrealised {pnl:+.2f} pts"
            )
        else:
            stats  = self.strategy.get_stats() if self.strategy else {}
            phase  = stats.get("current_phase", "FLAT")
            trades = stats.get("daily_trades", 0)
            pnl    = stats.get("total_pnl", 0.0)
            exch   = self.execution_router.active_exchange.upper() \
                     if self.execution_router else "?"
            logger.info(
                f"💓 ${price:,.2f} [{feed}|exec={exch}] | {phase} | "
                f"trades today: {trades} | session PnL: ${pnl:+.2f}"
            )

    # =========================================================================
    # STREAM SUPERVISOR
    # =========================================================================

    def maybe_supervise_streams(self) -> None:
        if not self.data_manager:
            return

        now      = time.time()
        interval = float(config.HEALTH_CHECK_INTERVAL_SEC)
        if now - self.last_health_check_sec < interval:
            return
        self.last_health_check_sec = now

        ws = getattr(self.data_manager, "ws", None)
        if ws is None:
            return

        stale_sec  = float(config.WS_STALE_SECONDS)
        ws_healthy = ws.is_healthy(timeout_seconds=int(stale_sec))

        price_stale_sec = getattr(config, "PRICE_STALE_SECONDS", 90.0)
        price_fresh     = self.data_manager.is_price_fresh(
            max_stale_seconds=price_stale_sec)

        if ws_healthy and price_fresh:
            return

        reason = []
        if not ws_healthy:
            reason.append(f"WS silent >{stale_sec:.0f}s")
        if not price_fresh:
            reason.append(f"Price frozen >{price_stale_sec:.0f}s")
        reason_str = " | ".join(reason)

        logger.warning("⚠️  Stream issue: %s — restarting...", reason_str)
        send_telegram_message(
            f"⚠️ STREAM ISSUE: {reason_str}\n🔄 Restarting streams...")

        ok = self.data_manager.restart_streams()
        if not ok:
            logger.error("❌ Stream restart failed — entries gated")
            return

        self.data_manager.wait_until_ready(
            timeout_sec=float(config.READY_TIMEOUT_SEC))

    # =========================================================================
    # PERIODIC REPORT
    # =========================================================================

    def maybe_send_report(self) -> None:
        interval = getattr(config, "TELEGRAM_REPORT_INTERVAL_SEC", 900)
        if interval <= 0:
            return
        now = time.time()
        if now - self.last_report_sec < interval:
            return
        self.last_report_sec = now

        if self.strategy:
            try:
                report = self.strategy.format_status_report()
                send_telegram_message(report)
            except Exception as e:
                logger.debug(f"Report error: {e}")

    # =========================================================================
    # MAIN LOOP
    # =========================================================================

    # =========================================================================
    # MAIN LOOP WATCHDOG — logs stuck threads before silently dying
    # =========================================================================

    def _watchdog_loop(self) -> None:
        """
        Runs in a daemon thread. Every 5 seconds checks whether the main loop
        completed its last tick within WATCHDOG_THRESH seconds. If not, dumps
        a full Python thread stack trace to the log so the freeze is diagnosable.
        """
        import traceback as _tb
        WATCHDOG_THRESH = float(getattr(config, "WATCHDOG_THRESH_SEC", 15.0))
        while self.running:
            time.sleep(5.0)
            if not self.running:
                break
            age = time.time() - self._last_tick_time
            if age > WATCHDOG_THRESH:
                logger.error(
                    "🚨 WATCHDOG: main loop has not completed a tick in %.0fs "
                    "(threshold=%.0fs). Dumping thread stacks:",
                    age, WATCHDOG_THRESH,
                )
                frames = []
                # Dump all thread stacks
                for tid, frame in sys._current_frames().items():
                    stack = "".join(_tb.format_stack(frame))
                    frames.append(f"Thread id={tid}:\n{stack}")
                logger.error("THREAD STACKS:\n%s", "\n---\n".join(frames))

    def run(self) -> None:
        if not all([self.strategy, self.data_manager,
                    self.execution_router, self.risk_manager]):
            logger.error("Bot components not initialised")
            return

        logger.info("📊 Main loop active (250ms tick)")
        self._last_tick_time = time.time()

        # Start watchdog — logs thread stacks if loop freezes >15s
        _wd = threading.Thread(target=self._watchdog_loop, daemon=True, name="watchdog")
        _wd.start()

        while self.running:
            try:
                time.sleep(0.25)
                # Bug-18 fix: do NOT update _last_tick_time here at the top of
                # the loop.  The watchdog measures how long since the last tick
                # COMPLETED.  Setting it here meant a 14.9-second hang inside
                # on_tick() looked like a 0.25-second tick to the watchdog — it
                # would never fire.  The update now sits only at the two
                # explicit completion points below.

                self.maybe_supervise_streams()
                self.maybe_send_report()
                self.maybe_log_heartbeat()

                pos = self.strategy.get_position() if self.strategy else None
                if not self.trading_enabled and pos is None:
                    # Paused path — no on_tick call but we still completed this
                    # iteration; reset the watchdog so it doesn't false-trip.
                    self._last_tick_time = time.time()
                    continue

                _t0 = time.time()
                self.strategy.on_tick(
                    self.data_manager,
                    self.execution_router,
                    self.risk_manager,
                    int(time.time() * 1000),
                )
                _tick_ms = (time.time() - _t0) * 1000
                if _tick_ms > 5000:
                    logger.warning(
                        "⚠️ on_tick took %.0fms — possible REST call in main thread",
                        _tick_ms,
                    )

                # on_tick completed — now safe to reset watchdog timer
                self._last_tick_time = time.time()

            except KeyboardInterrupt:
                logger.info("Keyboard interrupt — shutting down")
                break
            except Exception:
                logger.exception("❌ Main loop error")
                time.sleep(1.0)

        self.running = False

    # =========================================================================
    # STOP
    # =========================================================================

    def stop(self) -> None:
        logger.info("Stopping unified quant bot...")
        self.running = False

        stop_msg = "🛑 <b>UNIFIED QUANT BOT STOPPED</b>\nShut down gracefully"
        if self.strategy:
            pos = self.strategy.get_position()
            if pos:
                side  = pos.get("side", "?").upper()
                entry = pos.get("entry_price", 0)
                sl    = getattr(self.strategy, "current_sl_price", 0) or 0
                tp    = getattr(self.strategy, "current_tp_price", 0) or 0
                warn  = (
                    f"\n\n⚠️ POSITION LEFT OPEN\n"
                    f"Side: {side}  Entry: ${entry:.2f}\n"
                    f"SL: ${sl:.2f}  TP: ${tp:.2f}\n"
                    f"Exchange SL/TP orders remain live."
                )
                logger.critical("Active position on shutdown: %s", warn)
                stop_msg += warn

        if self.data_manager:
            self.data_manager.stop()

        send_telegram_message(stop_msg)
        logger.info("Unified quant bot stopped")


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    bot = QuantBot()

    if threading.current_thread() is threading.main_thread():
        def _signal_handler(signum, frame):
            logger.info(f"Shutdown signal {signum} received")
            bot.stop()
            sys.exit(0)
        signal.signal(signal.SIGINT,  _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)

    if not bot.initialize():
        sys.exit(1)

    if not bot.start():
        sys.exit(1)

    try:
        bot.run()
    except Exception:
        logger.exception("Fatal error in main loop")
        bot.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()

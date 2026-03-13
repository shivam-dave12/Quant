"""
Quant Bot — Institutional Multi-Factor Momentum + Order Flow
=============================================================
Drop-in replacement for ICT Bot v11.
Uses QuantStrategy (EMA cross, CVD, VWAP, BB/KC squeeze, Volume Flow)
instead of AdvancedICTStrategy (OB/FVG/ICT).

Same infrastructure:
  - ICTDataManager  (WebSocket + REST warmup)
  - OrderManager    (order placement, SL/TP, trailing)
  - RiskManager     (balance, daily limits, trade history)
  - TelegramNotifier / TelegramBotController

Changed:
  - Strategy import: QuantStrategy
  - Log file: quant_bot.log
  - Startup/stop messages updated
  - Feature probe at startup shows quant engine state
"""

import logging
import signal
import sys
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import config
from data_manager import ICTDataManager
from order_manager import OrderManager
from risk_manager import RiskManager
from quant_strategy import QuantStrategy
from telegram_notifier import (
    install_global_telegram_log_handler,
    send_telegram_message,
)

# ── IST Timezone Logging ─────────────────────────────────────────────
IST = timezone(timedelta(hours=5, minutes=30))


class ISTFormatter(logging.Formatter):
    """Logging formatter that emits timestamps in IST (UTC+5:30)."""
    converter = None

    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=IST)
        if datefmt:
            return dt.strftime(datefmt)
        s = dt.strftime("%Y-%m-%d %H:%M:%S")
        return f"{s},{int(record.msecs):03d}"


_ist_fmt = ISTFormatter(
    fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

_file_handler = logging.FileHandler("quant_bot.log")
_file_handler.setFormatter(_ist_fmt)

_stream_handler = logging.StreamHandler()
_stream_handler.setFormatter(_ist_fmt)

logging.basicConfig(
    level=getattr(config, "LOG_LEVEL", "INFO"),
    handlers=[_file_handler, _stream_handler],
)
logger = logging.getLogger(__name__)

install_global_telegram_log_handler(level=logging.WARNING, throttle_seconds=5.0)


class QuantBot:
    def __init__(self) -> None:
        self.running               = False
        self.last_health_check_sec = 0.0
        self.last_report_sec       = 0.0

        self.data_manager:  Optional[ICTDataManager] = None
        self.order_manager: Optional[OrderManager]   = None
        self.risk_manager:  Optional[RiskManager]    = None
        self.strategy:      Optional[QuantStrategy]  = None

        self.trading_enabled      = True
        self.trading_pause_reason = ""
        self.last_heartbeat_sec   = 0.0

    # =================================================================
    # INITIALIZE
    # =================================================================

    def initialize(self) -> bool:
        try:
            logger.info("=" * 80)
            logger.info("⚡ QUANT BOT — MULTI-FACTOR MOMENTUM + ORDER FLOW ENGINE")
            logger.info("   CVD | VWAP | EMA Cross | BB/KC Squeeze | Volume Flow")
            logger.info(f"   {config.SYMBOL} | {config.LEVERAGE}x leverage | "
                        f"{getattr(config, 'QUANT_MARGIN_PCT', 0.20):.0%} margin per trade")
            logger.info("=" * 80)

            self.data_manager  = ICTDataManager()
            self.order_manager = OrderManager()
            self.risk_manager  = RiskManager(shared_api=self.order_manager.api)
            self.strategy      = QuantStrategy(self.order_manager)
            self.data_manager.register_strategy(self.strategy)

            logger.info("✅ Quant bot initialized")
            return True

        except Exception:
            logger.exception("❌ Failed to initialize quant bot")
            return False

    # =================================================================
    # START
    # =================================================================

    def start(self) -> bool:
        try:
            if not all([self.order_manager, self.risk_manager,
                        self.data_manager, self.strategy]):
                logger.error("Bot components not initialized")
                return False

            # Set leverage
            logger.info("Setting leverage to %sx...", config.LEVERAGE)
            resp = self.order_manager.api.set_leverage(
                symbol=config.SYMBOL,
                exchange=config.EXCHANGE,
                leverage=int(config.LEVERAGE))
            if isinstance(resp, dict) and resp.get("error"):
                logger.warning("⚠️ Leverage response (may already be set): %s", resp)

            balance_info = self.risk_manager.get_available_balance()
            if balance_info:
                avail = float(balance_info.get("available", 0.0))
                total = float(balance_info.get("total", avail))
                logger.info("Balance — Available: $%.2f | Total: $%.2f USDT",
                            avail, total)

            logger.info("Starting data streams (WS + REST warmup)...")
            if not self.data_manager.start():
                logger.error("❌ Failed to start data streams")
                return False

            logger.info("Waiting for data readiness...")
            ready = self.data_manager.wait_until_ready(
                timeout_sec=float(config.READY_TIMEOUT_SEC))
            if not ready:
                logger.error("❌ DataManager not ready within timeout")
                return False

            price = self.data_manager.get_last_price()
            logger.info("✅ Data ready. Price: $%.2f", price)

            self.running = True

            # Log the strategy's initial config state
            from quant_strategy import QCfg
            send_telegram_message(
                "⚡ <b>QUANT BOT STARTED</b>\n\n"
                f"Symbol:   {QCfg.SYMBOL()}\n"
                f"Price:    ${price:,.2f}\n"
                f"Leverage: {QCfg.LEVERAGE()}x\n"
                f"Margin:   {QCfg.MARGIN_PCT():.0%} per trade\n"
                f"SL/TP:    {QCfg.SL_ATR_MULT()}×ATR / {QCfg.TP_ATR_MULT()}×ATR\n"
                f"Min R:R:  {QCfg.MIN_RR_RATIO()}\n\n"
                f"<i>Weights: CVD={QCfg.W_CVD()} VWAP={QCfg.W_VWAP()} "
                f"MOM={QCfg.W_MOM()} SQZ={QCfg.W_SQUEEZE()} VFL={QCfg.W_VOL()} "
                f"OB={QCfg.W_ORDERBOOK()} TF={QCfg.W_TICK_FLOW()}</i>"
            )

            logger.info("🚀 QUANT BOT RUNNING")
            return True

        except Exception:
            logger.exception("❌ Error starting quant bot")
            return False

    # =================================================================
    # HEARTBEAT — compact 60s pulse showing price + bot state
    # =================================================================

    def maybe_log_heartbeat(self) -> None:
        """Emit a one-line status pulse every 60 s so the terminal feels alive."""
        now = time.time()
        if now - self.last_heartbeat_sec < 60.0:
            return
        self.last_heartbeat_sec = now

        price = self.data_manager.get_last_price() if self.data_manager else 0.0
        pos   = self.strategy.get_position()        if self.strategy   else None

        if pos:
            side  = pos.get("side", "?").upper()
            entry = pos.get("entry_price", 0.0)
            sl    = pos.get("sl_price", 0.0)
            tp    = pos.get("tp_price", 0.0)
            pnl_pts = (price - entry) if side == "LONG" else (entry - price)
            logger.info(
                f"💓 ${price:,.2f} | IN {side} @ ${entry:,.2f} | "
                f"SL ${sl:,.2f}  TP ${tp:,.2f} | unrealised {pnl_pts:+.2f} pts"
            )
        else:
            stats = self.strategy.get_stats() if self.strategy else {}
            phase = stats.get("current_phase", "FLAT")
            trades = stats.get("daily_trades", 0)
            pnl   = stats.get("total_pnl", 0.0)
            logger.info(
                f"💓 ${price:,.2f} | {phase} | "
                f"trades today: {trades} | session PnL: ${pnl:+.2f}"
            )

    # =================================================================
    # STREAM SUPERVISOR  (unchanged from ICT bot)
    # =================================================================

    def maybe_supervise_streams(self) -> None:
        if not self.data_manager or not self.data_manager.ws:
            return

        now      = time.time()
        interval = float(config.HEALTH_CHECK_INTERVAL_SEC)
        if now - self.last_health_check_sec < interval:
            return
        self.last_health_check_sec = now

        stale_sec  = float(config.WS_STALE_SECONDS)
        ws_healthy = self.data_manager.ws.is_healthy(timeout_seconds=int(stale_sec))

        price_stale_sec = getattr(config, "PRICE_STALE_SECONDS", 90.0)
        price_fresh     = self.data_manager.is_price_fresh(max_stale_seconds=price_stale_sec)

        if ws_healthy and price_fresh:
            return

        reason = []
        if not ws_healthy:
            reason.append(f"WS silent >{stale_sec:.0f}s")
        if not price_fresh:
            reason.append(f"Price frozen >{price_stale_sec:.0f}s")
        reason_str = " | ".join(reason)

        logger.warning("⚠️ Stream issue: %s — restarting...", reason_str)
        send_telegram_message(f"⚠️ STREAM ISSUE: {reason_str}\n🔄 Restarting streams...")

        ok = self.data_manager.restart_streams()
        if not ok:
            logger.error("❌ Stream restart failed. Entries gated.")
            return

        self.data_manager.wait_until_ready(timeout_sec=float(config.READY_TIMEOUT_SEC))

    # =================================================================
    # PERIODIC REPORT
    # =================================================================

    def maybe_send_report(self) -> None:
        """Send a Telegram status report every TELEGRAM_REPORT_INTERVAL_SEC seconds."""
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

    # =================================================================
    # MAIN LOOP
    # =================================================================

    def run(self) -> None:
        if not all([self.strategy, self.data_manager,
                    self.order_manager, self.risk_manager]):
            logger.error("Bot components not initialized")
            return

        logger.info("📊 Main loop active (250ms tick)")

        while self.running:
            try:
                time.sleep(0.25)
                self.maybe_supervise_streams()
                self.maybe_send_report()
                self.maybe_log_heartbeat()

                pos = self.strategy.get_position() if self.strategy else None
                if not self.trading_enabled and pos is None:
                    continue

                self.strategy.on_tick(
                    self.data_manager,
                    self.order_manager,
                    self.risk_manager,
                    int(time.time() * 1000))

            except KeyboardInterrupt:
                logger.info("Keyboard interrupt")
                break
            except Exception:
                logger.exception("❌ Main loop error")
                time.sleep(1.0)

        self.running = False

    # =================================================================
    # STOP
    # =================================================================

    def stop(self) -> None:
        logger.info("Stopping quant bot...")
        self.running = False

        stop_msg = "🛑 <b>QUANT BOT STOPPED</b>\nShut down gracefully"
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
                    f"Exchange SL/TP orders remain live.")
                logger.critical("Active position on shutdown: %s", warn)
                stop_msg += warn

        if self.data_manager:
            self.data_manager.stop()

        send_telegram_message(stop_msg)
        logger.info("Quant bot stopped")


# =====================================================================
# ENTRY POINT
# =====================================================================

def main() -> None:
    bot = QuantBot()

    if threading.current_thread() is threading.main_thread():
        def signal_handler(signum, frame):
            logger.info("Shutdown signal received")
            bot.stop()
            sys.exit(0)
        signal.signal(signal.SIGINT,  signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    if not bot.initialize():
        sys.exit(1)
    if not bot.start():
        sys.exit(1)

    try:
        bot.run()
    except Exception:
        logger.exception("Fatal error in main")
        bot.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()

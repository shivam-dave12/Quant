"""
main.py — Unified Dual-Exchange Quant Bot (Predictive Engine v2)
================================================================
Updated for the new Predictive QuantEngine. One import change.
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

def _log_uncaught(exc_type, exc_value, exc_tb):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_tb); return
    logger.critical("UNCAUGHT EXCEPTION", exc_info=(exc_type, exc_value, exc_tb))
    try:
        from telegram.notifier import send_telegram_message as _stm
        import traceback as _tb
        _stm(f"💀 <b>BOT CRASH</b>\n<code>{''.join(_tb.format_exception(exc_type, exc_value, exc_tb))[-1500:]}</code>")
    except Exception: pass
sys.excepthook = _log_uncaught

def _log_thread_exception(args):
    if args.exc_type is SystemExit: return
    logger.error("THREAD EXCEPTION in '%s'", args.thread.name if args.thread else "?",
                 exc_info=(args.exc_type, args.exc_value, args.exc_traceback))
threading.excepthook = _log_thread_exception

from exchanges.coinswitch.api          import FuturesAPI  as CoinSwitchAPI
from exchanges.coinswitch.data_manager import CoinSwitchDataManager
from exchanges.delta.api               import DeltaAPI
from exchanges.delta.data_manager      import DeltaDataManager
from aggregator.market_aggregator      import MarketAggregator
from execution.order_manager           import OrderManager, CancelResult
from execution.router                  import ExecutionRouter
from risk.risk_manager                 import RiskManager

# ══════════════════════════════════════════════════════════════
# KEY CHANGE: Import from NEW strategy, not old quant_strategy
# ══════════════════════════════════════════════════════════════
from strategy.quant_integration        import QuantStrategy, QCfg

from telegram.notifier import (
    install_global_telegram_log_handler,
    send_telegram_message,
)
install_global_telegram_log_handler(level=logging.WARNING, throttle_seconds=5.0)


class QuantBot:
    def __init__(self):
        self.running               = False
        self.last_health_check_sec = 0.0
        self.last_report_sec       = 0.0
        self.last_heartbeat_sec    = 0.0
        self._tick_lock = threading.Lock()

        self.data_manager:     Optional[MarketAggregator]  = None
        self.execution_router: Optional[ExecutionRouter]   = None
        self.order_manager:    Optional[ExecutionRouter]   = None
        self.risk_manager:     Optional[RiskManager]       = None
        self.strategy:         Optional[QuantStrategy]     = None
        self.trading_enabled      = True
        self.trading_pause_reason = ""

    def initialize(self) -> bool:
        try:
            logger.info("=" * 80)
            logger.info("⚡ PREDICTIVE QUANT BOT v2 — DUAL-EXCHANGE")
            logger.info("   VPIN | OB Pressure | Kyle λ | Arrival | Momentum")
            logger.info(f"   Symbol: {config.SYMBOL} | Leverage: {config.LEVERAGE}x | "
                        f"Execution: {config.EXECUTION_EXCHANGE.upper()}")
            logger.info("=" * 80)

            has_delta      = bool(config.DELTA_API_KEY and config.DELTA_SECRET_KEY)
            has_coinswitch = bool(config.COINSWITCH_API_KEY and config.COINSWITCH_SECRET_KEY)
            logger.info(f"Exchanges — Delta: {has_delta} | CoinSwitch: {has_coinswitch}")

            cs_api = delta_api = None
            if has_coinswitch:
                cs_api = CoinSwitchAPI(api_key=config.COINSWITCH_API_KEY, secret_key=config.COINSWITCH_SECRET_KEY)
                logger.info("✅ CoinSwitch API ready")
            if has_delta:
                delta_api = DeltaAPI(api_key=config.DELTA_API_KEY, secret_key=config.DELTA_SECRET_KEY,
                                     testnet=getattr(config, "DELTA_TESTNET", False))
                logger.info("✅ Delta API ready")

            cs_om = OrderManager(cs_api, exchange_name="coinswitch") if cs_api else None
            delta_om = OrderManager(delta_api, exchange_name="delta") if delta_api else None

            self.execution_router = ExecutionRouter(coinswitch_om=cs_om, delta_om=delta_om,
                                                     default=config.EXECUTION_EXCHANGE)
            self.order_manager = self.execution_router

            exec_exch = config.EXECUTION_EXCHANGE.lower()
            if exec_exch == "delta" and has_delta:
                primary_dm = DeltaDataManager()
                secondary_dm = CoinSwitchDataManager() if has_coinswitch else None
            elif exec_exch == "coinswitch" and has_coinswitch:
                primary_dm = CoinSwitchDataManager()
                secondary_dm = DeltaDataManager() if has_delta else None
            elif has_delta:
                primary_dm = DeltaDataManager()
                secondary_dm = CoinSwitchDataManager() if has_coinswitch else None
            else:
                primary_dm = CoinSwitchDataManager()
                secondary_dm = None

            self.data_manager = MarketAggregator(primary_dm=primary_dm, secondary_dm=secondary_dm)
            self.risk_manager = RiskManager(shared_api=self.execution_router)
            self.strategy = QuantStrategy(self.execution_router)
            self.data_manager.register_strategy(self.strategy)

            logger.info("✅ Predictive quant bot initialised")
            return True
        except Exception:
            logger.exception("❌ Failed to initialise"); return False

    def start(self) -> bool:
        try:
            if not all([self.execution_router, self.risk_manager, self.data_manager, self.strategy]):
                logger.error("Components not initialised"); return False

            try:
                resp = self.execution_router.set_leverage(leverage=int(config.LEVERAGE))
                if isinstance(resp, dict) and (resp.get("success") or not resp.get("error")):
                    logger.info(f"✅ Leverage set to {config.LEVERAGE}x")
            except Exception as e:
                logger.warning(f"⚠️ Leverage: {e}")

            try:
                bal = self.execution_router.get_balance()
                if bal and not bal.get("error"):
                    logger.info(f"Balance — ${float(bal.get('available',0)):,.2f} available")
            except Exception: pass

            logger.info("Starting data streams...")
            if not self.data_manager.start():
                logger.error("❌ Data streams failed"); return False

            ready = self.data_manager.wait_until_ready(timeout_sec=float(config.READY_TIMEOUT_SEC))
            if not ready:
                logger.error("❌ Data not ready"); return False

            price = self.data_manager.get_last_price()
            self.running = True

            send_telegram_message(
                "⚡ <b>PREDICTIVE QUANT BOT v2 STARTED</b>\n\n"
                f"Symbol: {QCfg.SYMBOL()}\nPrice: ${price:,.2f}\n"
                f"Execution: {self.execution_router.active_exchange.upper()}\n"
                f"Leverage: {QCfg.LEVERAGE()}x\n"
                f"Min Confidence: {QCfg.MIN_CONFIDENCE()}\n"
                f"Min R:R: {QCfg.MIN_RR_RATIO()}\n"
                f"Confirm Ticks: {QCfg.CONFIRM_TICKS()}")

            logger.info("🚀 PREDICTIVE QUANT BOT v2 RUNNING")
            return True
        except Exception:
            logger.exception("❌ Start error"); return False

    def maybe_log_heartbeat(self):
        now = time.time()
        if now - self.last_heartbeat_sec < 60.0: return
        self.last_heartbeat_sec = now
        price = self.data_manager.get_last_price() if self.data_manager else 0.0
        pos = self.strategy.get_position() if self.strategy else None
        agg = self.data_manager.get_secondary_status() if self.data_manager else {}
        feed = "dual" if agg.get("alive") else "single"
        if pos:
            side = pos.get("side","?").upper(); entry = pos.get("entry_price",0)
            if entry > 0:
                pnl = (price-entry) if side=="LONG" else (entry-price)
                logger.info(f"💓 ${price:,.2f} [{feed}] | {side} @ ${entry:,.2f} | uPnL {pnl:+.2f}")
            else:
                logger.info(f"💓 ${price:,.2f} [{feed}] | PENDING FILL")
        else:
            stats = self.strategy.get_stats() if self.strategy else {}
            logger.info(f"💓 ${price:,.2f} [{feed}] | {stats.get('current_phase','FLAT')} | "
                        f"trades={stats.get('daily_trades',0)} PnL=${stats.get('total_pnl',0):+.2f}")

    def maybe_supervise_streams(self):
        if not self.data_manager: return
        now = time.time()
        if now - self.last_health_check_sec < float(config.HEALTH_CHECK_INTERVAL_SEC): return
        self.last_health_check_sec = now
        ws = getattr(self.data_manager, "ws", None)
        if ws is None: return
        stale_sec = float(config.WS_STALE_SECONDS)
        ws_ok = ws.is_healthy(timeout_seconds=int(stale_sec))
        price_ok = self.data_manager.is_price_fresh(max_stale_seconds=getattr(config,"PRICE_STALE_SECONDS",90.0))
        if ws_ok and price_ok: return
        reason = []
        if not ws_ok: reason.append(f"WS silent >{stale_sec:.0f}s")
        if not price_ok: reason.append("Price frozen")
        logger.warning("⚠️ Stream issue: %s — restarting", " | ".join(reason))
        self.data_manager.restart_streams()
        self.data_manager.wait_until_ready(timeout_sec=float(config.READY_TIMEOUT_SEC))

    def maybe_send_report(self):
        interval = getattr(config, "TELEGRAM_REPORT_INTERVAL_SEC", 900)
        if interval <= 0: return
        now = time.time()
        if now - self.last_report_sec < interval: return
        self.last_report_sec = now
        if self.strategy:
            try:
                report = self.strategy.get_status_text()
                send_telegram_message(report)
            except Exception: pass

    def _watchdog_loop(self):
        import traceback as _tb
        THRESH = float(getattr(config, "WATCHDOG_THRESH_SEC", 15.0))
        while self.running:
            time.sleep(5.0)
            if not self.running: break
            with self._tick_lock: ltt = self._last_tick_time
            age = time.time() - ltt
            if age > THRESH:
                logger.error("🚨 WATCHDOG: no tick in %.0fs", age)
                for tid, frame in sys._current_frames().items():
                    logger.error("Thread %d:\n%s", tid, "".join(_tb.format_stack(frame)))

    def run(self):
        if not all([self.strategy, self.data_manager, self.execution_router, self.risk_manager]):
            logger.error("Not initialised"); return
        logger.info("📊 Main loop active (250ms tick)")
        with self._tick_lock: self._last_tick_time = time.time()
        threading.Thread(target=self._watchdog_loop, daemon=True, name="watchdog").start()

        while self.running:
            try:
                time.sleep(0.25)
                self.maybe_supervise_streams()
                self.maybe_send_report()
                self.maybe_log_heartbeat()

                pos = self.strategy.get_position() if self.strategy else None
                if not self.trading_enabled and pos is None:
                    with self._tick_lock: self._last_tick_time = time.time()
                    continue

                _t0 = time.time()
                # on_tick accepts 3 or 4 args — new strategy ignores 4th
                self.strategy.on_tick(self.data_manager, self.execution_router, self.risk_manager)
                _ms = (time.time() - _t0) * 1000
                if _ms > 5000: logger.warning("⚠️ on_tick took %.0fms", _ms)
                with self._tick_lock: self._last_tick_time = time.time()

            except KeyboardInterrupt: break
            except Exception:
                logger.exception("❌ Main loop error"); time.sleep(1.0)
        self.running = False

    def stop(self):
        logger.info("Stopping bot...")
        self.running = False
        stop_msg = "🛑 <b>PREDICTIVE QUANT BOT v2 STOPPED</b>"
        if self.strategy:
            pos = self.strategy.get_position()
            if pos:
                stop_msg += (f"\n\n⚠️ POSITION LEFT OPEN\n{pos.get('side','?').upper()} "
                             f"@ ${pos.get('entry_price',0):.2f}")
        if self.data_manager: self.data_manager.stop()
        send_telegram_message(stop_msg)


def main():
    bot = QuantBot()
    if threading.current_thread() is threading.main_thread():
        def _sh(signum, frame): logger.info(f"Signal {signum}"); bot.stop(); sys.exit(0)
        signal.signal(signal.SIGINT, _sh); signal.signal(signal.SIGTERM, _sh)
    if not bot.initialize(): sys.exit(1)
    if not bot.start(): sys.exit(1)
    try: bot.run()
    except Exception: logger.exception("Fatal"); bot.stop(); sys.exit(1)

if __name__ == "__main__":
    main()

"""Entry point for the institutional ICT + Liquidity execution system.

Alpha authority is singular: 4H/15m draw-on-liquidity context and a fresh
5m liquidity raid with MSS/displacement and FVG repricing. Risk, bracket
protection and exact-fill reconciliation remain venue-scoped controls.
"""

from __future__ import annotations

import io
import logging
import os
import re
import signal
import sys
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import config

# ── IST Timezone logging ──────────────────────────────────────────────────────

IST = timezone(timedelta(hours=5, minutes=30))

_MOJIBAKE_SENTINELS = ("ð", "â", "Ã", "Â", "Î", "Ï")
_MOJIBAKE_RUN = re.compile(
    r"[\u0080-\u009f\u00a0-\u00ff\u0100-\u017f\u02c0-\u02ff"
    r"\u2010-\u201f\u2020-\u2026\u2030\u2039\u203a\u20ac\u2122]+"
)
_MOJIBAKE_DIRECT = {
    "🎯": "🎯", "🧭": "🧭", "📊": "📊", "💰": "💰",
    "🔒": "🔒", "🔄": "🔄", "🔱": "🔱", "🚨": "🚨",
    "💀": "💀", "💥": "💥", "✅": "✅", "❌": "❌",
    "❌": "❌", "⚠️": "⚠️", "⚠️": "⚠️", "⏱️": "⏱️",
    "⏱️": "⏱️", "⏱️": "⏱️", "⏱️": "⏱️", "⏳": "⏳",
    "≈": "≈", "±": "±", "×": "×", "σ": "σ",
    "⬜": "⬜", "░": "░", "█": "█",
}


def _repair_mojibake(text: str) -> str:
    """Repair UTF-8 text that was accidentally decoded as cp1252."""
    if not any(s in text for s in _MOJIBAKE_SENTINELS):
        return text
    for bad, good in _MOJIBAKE_DIRECT.items():
        text = text.replace(bad, good)

    def _as_original_utf8_bytes(frag: str) -> bytes:
        out = bytearray()
        for ch in frag:
            try:
                out.extend(ch.encode("cp1252"))
            except UnicodeEncodeError:
                code = ord(ch)
                if code <= 0xFF:
                    out.append(code)
                else:
                    raise
        return bytes(out)

    def _fix(match: re.Match) -> str:
        frag = match.group(0)
        if not any(s in frag for s in _MOJIBAKE_SENTINELS):
            return frag
        try:
            repaired = _as_original_utf8_bytes(frag).decode("utf-8")
        except UnicodeError:
            return frag
        old_bad = sum(frag.count(s) for s in _MOJIBAKE_SENTINELS)
        new_bad = sum(repaired.count(s) for s in _MOJIBAKE_SENTINELS)
        return repaired if new_bad < old_bad else frag

    for _ in range(3):
        repaired = _MOJIBAKE_RUN.sub(_fix, text)
        if repaired == text or not any(s in repaired for s in _MOJIBAKE_SENTINELS):
            return repaired
        text = repaired
        for bad, good in _MOJIBAKE_DIRECT.items():
            text = text.replace(bad, good)
    return text


class ISTFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=IST)
        s  = dt.strftime("%Y-%m-%d %H:%M:%S")
        return f"{s},{int(record.msecs):03d}"

    def format(self, record):
        return _repair_mojibake(super().format(record))


_ANSI_LOG_RE = re.compile(r"\x1b\[[0-9;]*m")


_LOG_LAYER_RULES = (
    ("WATCHDOG", "🛡", ("watchdog",)),
    ("TELEGRAM", "✉", ("telegram",)),
    ("EXECUTION", "⚙", ("execution", "order_manager", "router")),
    ("RISK", "⚖", ("risk", "risk_manager")),
    ("MARKET-DATA", "📡", ("data_manager", "websocket", "market_aggregator", "exchanges")),
    ("LIQUIDITY", "💧", ("liquidity_map", "liquidity_pool", "tp_ladder")),
    ("ICT_LIQUIDITY", "🏛", ("quant_strategy", "entry_engine", "liquidity_map")),
    
    ("RUNTIME", "▶", ("main", "__main__")),
)


def _architecture_layer(logger_name: str, message: str = "") -> tuple[str, str]:
    needle = f"{logger_name} {message}".lower()
    for label, icon, markers in _LOG_LAYER_RULES:
        if any(marker in needle for marker in markers):
            return label, icon
    return "SYSTEM", "•"


def _current_asset_log_prefix() -> str:
    """Return per-contract log prefix for multi-asset scoped strategy logs."""
    try:
        from core.instruments import current_instrument
        inst = current_instrument()
        if inst is None:
            return ""
        return f"[{inst.asset_id}|{inst.primary_exchange.value.upper()}:{inst.display_symbol}] "
    except Exception:
        return ""


class AssetContextFilter(logging.Filter):
    """Inject stable per-contract metadata into every log record."""
    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "asset_ctx"):
            record.asset_ctx = _current_asset_log_prefix()
        return True


class TerminalFormatter(ISTFormatter):
    """Architecture-aware terminal formatter.

    The live console should read like the actual bot architecture:
    DATA → ICT/LIQUIDITY THESIS → RISK/EXECUTION → EXACT RECONCILIATION.
    It does not expose old generation labels or static gate names.
    """

    _LEVEL_COLOR = {
        logging.DEBUG: "\033[2m",
        logging.INFO: "\033[96m",
        logging.WARNING: "\033[93m",
        logging.ERROR: "\033[91m",
        logging.CRITICAL: "\033[1;91m",
    }
    _RESET = "\033[0m"

    def __init__(self, enable_color: bool = True):
        super().__init__()
        self._enable_color = enable_color

    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=IST)
        return f"{dt.strftime('%H:%M:%S')}.{int(record.msecs):03d}"

    @staticmethod
    def _normalise_message(msg: str) -> str:
        msg = _repair_mojibake(msg)
        replacements = {
                        "mechanical guard": "mechanical stop",
            "mechanical guard": "mechanical stop",
            "blocked by": "paused by",
            "gate blocked": "safety-paused",
            "Institutional unified gate blocked": "Dynamic audit paused",
        }
        for old, new in replacements.items():
            msg = msg.replace(old, new)
        return msg

    def format(self, record):
        msg = self._normalise_message(record.getMessage())
        if not self._enable_color:
            msg = _ANSI_LOG_RE.sub("", msg)
        visible = _ANSI_LOG_RE.sub("", msg).lstrip()
        if visible.startswith("+ DELTA ") or visible.startswith("+ ENGINE "):
            return "\n" + msg

        level = record.levelname[:4].ljust(4)
        color = self._LEVEL_COLOR.get(record.levelno, "") if self._enable_color else ""
        reset = self._RESET if color else ""
        layer, icon = _architecture_layer(record.name, msg)
        asset_ctx = getattr(record, "asset_ctx", "") or ""
        prefix = f"{self.formatTime(record)} | {color}{level}{reset} | {icon} {layer:<12} | {asset_ctx}"
        if record.exc_info:
            msg = f"{msg}\n{self.formatException(record.exc_info)}"
        return prefix + msg.replace("\n", "\n" + " " * len(_ANSI_LOG_RE.sub("", prefix)))

class TerminalBurstFilter(logging.Filter):
    """Suppress repeated tick-level terminal messages while keeping file logs intact."""

    _NOISY_MARKERS = (
        "ICT_DECISION SNAPSHOT",
        "DESK_HEALTH",
        "Spread cost impairment",
    )
    _NUMBER_RE = re.compile(r"(?<![A-Za-z])[-+]?\$?\d[\d,]*(?:\.\d+)?%?")

    def __init__(self, interval_sec: float = 20.0):
        super().__init__()
        self.interval_sec = interval_sec
        self._next_ok = {}
        self._lock = threading.Lock()

    def filter(self, record: logging.LogRecord) -> bool:
        msg = _repair_mojibake(record.getMessage())
        if not any(marker in msg for marker in self._NOISY_MARKERS):
            return True
        key = (record.name, self._NUMBER_RE.sub("#", msg)[:240])
        now = time.time()
        with self._lock:
            next_ok = self._next_ok.get(key, 0.0)
            if now < next_ok:
                return False
            self._next_ok[key] = now + self.interval_sec
            if len(self._next_ok) > 500:
                expired = [k for k, v in self._next_ok.items() if v < now]
                for k in expired:
                    self._next_ok.pop(k, None)
            return True


_ist_fmt = ISTFormatter(fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(asset_ctx)s%(message)s")
_term_fmt = TerminalFormatter(
    enable_color=(
        bool(getattr(sys.stdout, "isatty", lambda: False)())
        or os.getenv("FORCE_COLOR", "").lower() in ("1", "true", "yes")
        or os.getenv("PY_COLORS", "") == "1"
    )
)

_file_handler = logging.FileHandler("quant_bot.log", encoding="utf-8")
_file_handler.setFormatter(_ist_fmt)
_file_handler.addFilter(AssetContextFilter())

_stream_handler = logging.StreamHandler(
    stream=io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    if hasattr(sys.stdout, "buffer") else sys.stdout
)
_stream_handler.setFormatter(_term_fmt)
_stream_handler.addFilter(AssetContextFilter())
_stream_handler.addFilter(TerminalBurstFilter())

logging.basicConfig(
    level=getattr(config, "LOG_LEVEL", "INFO"),
    handlers=[_file_handler, _stream_handler],
    force=True,
)
logger = logging.getLogger(__name__)

# Retired display pipeline removed; QuantStrategy status is authoritative.
_DISPLAY_ENGINE = False

# ── Global exception hooks ────────────────────────────────────────────────────

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

# ── Deferred imports ──────────────────────────────────────────────────────────
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
try:
    from watchdog import build_default_watchdog
except ImportError:
    build_default_watchdog = None

# Throttle raised 5 s → 30 s: pool-gate and other per-tick warnings
# arrive faster than 5 s and would still flood Telegram even after
# the primary dedup fixes.  30 s is a safe floor for operator paging.
install_global_telegram_log_handler(level=logging.WARNING, throttle_seconds=30.0)


# ─────────────────────────────────────────────────────────────────────────────
# QUANT BOT
# ─────────────────────────────────────────────────────────────────────────────

class QuantBot:
    """
    Institutional dual-exchange quant bot.

    Decision hierarchy on every tick:
      1. Build feed-reliability and market-state context.   → MarketAggregator
      2. Map live BSL/SSL features and archived sweeps.      → LiquidityMap
      3. Approve only 5m raid/MSS/FVG with HTF DOL bias and real target. → ICT_LIQUIDITY
      4. Validate sizing, liquidation, lots and fills.       → Risk/Execution
      5. Manage live trade via fixed SL + TP ladder.       → TPLadder

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
        self._tick_lock = threading.Lock()
        # BUG-FIX M1: _last_tick_time must be initialised here, not only inside run().
        # The watchdog thread starts inside run() but sleep(5) before reading this value.
        # If run() is called on a non-main thread and the watchdog fires before the
        # first tick_lock write, it would raise AttributeError.  Initialising to 0.0
        # means the watchdog will immediately log a stale-tick warning on the very
        # first check, which is the safe/correct behaviour.
        self._last_tick_time = 0.0

        self.data_manager:     Optional[MarketAggregator]  = None
        self.execution_router: Optional[ExecutionRouter]   = None
        self.order_manager:    Optional[ExecutionRouter]   = None  # alias
        self.risk_manager:     Optional[RiskManager]       = None
        self.strategy:         Optional[QuantStrategy]     = None
        self.watchdog                                      = None

        self.trading_enabled      = True
        self.trading_pause_reason = ""

    # =========================================================================
    # INITIALIZE
    # =========================================================================

    def initialize(self) -> bool:
        try:
            logger.info("=" * 80)
            logger.info("⚡ ICT + LIQUIDITY EXECUTION SYSTEM — DUAL-EXCHANGE")
            logger.info("   4H/15m DOL Bias → 5m Raid/MSS/FVG → Protected Execution")
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
            self.risk_manager = RiskManager(
                shared_api = self.execution_router
            )

            # ── Strategy ──────────────────────────────────────────────────────
            self.strategy = QuantStrategy(self.execution_router)
            self.data_manager.register_strategy(self.strategy)

            logger.info("✅ Liquidity-first quant bot initialised")
            return True

        except Exception:
            logger.exception("❌ Failed to initialise quant bot")
            return False

    # =========================================================================
    # START
    # =========================================================================

    def _publish_tick_time(self) -> None:
        if self.strategy is not None:
            try:
                setattr(self.strategy, "_last_tick_time", self._last_tick_time)
            except Exception:
                pass

    def _start_watchdog(self) -> None:
        if build_default_watchdog is None:
            logger.error("Full watchdog unavailable: build_default_watchdog import failed")
            send_telegram_message("⚠️ <b>WATCHDOG UNAVAILABLE</b>\nImport failed; fallback stale-tick logger only.")
            return
        if self.watchdog is not None:
            return
        try:
            forensic_dir = getattr(config, "WATCHDOG_FORENSIC_DIR", ".")
            self.watchdog = build_default_watchdog(
                strategy=self.strategy,
                data_manager=self.data_manager,
                execution_router=self.execution_router,
                risk_manager=self.risk_manager,
                notifier=send_telegram_message,
                config_module=config,
                forensic_dir=forensic_dir,
            )
            self.watchdog.start()
        except Exception as e:
            self.watchdog = None
            logger.exception("Full watchdog failed to start")
            send_telegram_message(
                f"⚠️ <b>WATCHDOG START FAILED</b>\n<code>{e}</code>\nFallback stale-tick logger only.")

    def start(self) -> bool:
        try:
            if not all([self.execution_router, self.risk_manager,
                        self.data_manager, self.strategy]):
                logger.error("Bot components not initialised — call initialize() first")
                return False

            # Leverage is an execution output, not a startup setting.  The venue
            # cap remains configured here; the structural funding model computes
            # and asserts actual leverage only after an approved ICT ticket.
            active_exch = self.execution_router.active_exchange
            logger.info(
                "Leverage venue_cap=%sx venue=%s leverage_set=DEFERRED_UNTIL_APPROVED_STRUCTURAL_ENTRY",
                int(getattr(config, "LEVERAGE", 1)), active_exch,
            )

            # ── Query initial balance ─────────────────────────────────────────
            try:
                bal = self.execution_router.get_balance()
                if bal and not bal.get("error"):
                    avail    = float(bal.get("available", 0))
                    total    = float(bal.get("total", avail))
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

            price      = self.data_manager.get_last_price()
            agg_status = self.data_manager.get_secondary_status()
            logger.info(f"✅ Data ready. Price: ${price:,.2f} | "
                        f"Dual-feed: {agg_status['alive']}")

            self.running = True
            with self._tick_lock:
                self._last_tick_time = time.time()
                self._publish_tick_time()
            self._start_watchdog()

            # ── Startup Telegram notification ─────────────────────────────────
            from strategy.quant_strategy import QCfg
            dual_feed      = agg_status["alive"]
            secondary_name = agg_status.get("secondary", "none")
            send_telegram_message(
                "⚡ <b>ICT + LIQUIDITY EXECUTION SYSTEM STARTED</b>\n\n"
                f"Symbol:    {QCfg.SYMBOL()}\n"
                f"Price:     ${price:,.2f}\n"
                f"Execution: {self.execution_router.active_exchange.upper()}\n"
                f"Leverage:  {QCfg.LEVERAGE()}x\n\n"
                "<b>Architecture:</b>\n"
                "  1️⃣  Market data: executable venue candles and order state\n"
                "  2️⃣  LiquidityMap: live BSL/SSL geometry and fresh raid detection\n"
                "  3️⃣  Entry authority: 4H/15m DOL bias with 5m raid-MSS-FVG trigger\n"
                "  4️⃣  Risk/Execution: structural-stop sizing, liquidation guard, native bracket\n"
                "  5️⃣  Reconciliation: exact broker fills and instrument-scoped P&amp;L\n"
                "\n"
                "<b>Authority model:</b>\n"
                "  🏛️ ICT + Liquidity is the only entry authority\n"
                "  🎯 SL beyond raided liquidity; TP at opposing higher-timeframe liquidity\n"
                "  🔒 No trade is active without venue protection\n"
                "  💰 PnL is exchange-fill reconciled, not estimated\n\n"
                f"📡 <b>Data feed:</b> "
                f"{'DUAL — ' + secondary_name.upper() + ' secondary active' if dual_feed else 'SINGLE — primary only'}\n\n"
                f"<i>/setexchange delta|coinswitch to switch execution exchange</i>"
            )

            logger.info("🚀 ICT + LIQUIDITY EXECUTION SYSTEM RUNNING")
            return True

        except Exception:
            logger.exception("❌ Error starting quant bot")
            return False

    # =========================================================================
    # HEARTBEAT  (liquidity-first layout)
    # =========================================================================

    def maybe_log_heartbeat(self) -> None:
        """Emit only authoritative ICT/Liquidity state and protected-position status."""
        if not self.strategy or not self.data_manager:
            return
        now = time.time()
        if now - getattr(self, "_last_heartbeat", 0.0) < float(getattr(config, "HEARTBEAT_INTERVAL_SEC", 15.0) or 15.0):
            return
        self._last_heartbeat = now
        try:
            price = float(self.data_manager.get_last_price() or 0.0)
            eng = getattr(self.strategy, "_entry_engine", None)
            info = eng.analysis_info if eng is not None else {}
            logger.info("ICT_LIQUIDITY HEARTBEAT state=%s price=%.4f 4H=%s 15m=%s 5m=%s",
                        info.get("state", "SCANNING"), price, info.get("context_4h", "WAIT"),
                        info.get("context_15m", "WAIT"), info.get("trigger", "WAIT"))
        except Exception as exc:
            logger.debug("Heartbeat unavailable: %s", exc)

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
        ws_healthy = ws.is_healthy(timeout_seconds=round(stale_sec))

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
    # MAIN LOOP + WATCHDOG
    # =========================================================================

    def _watchdog_loop(self) -> None:
        """
        Daemon thread — every 5 s checks whether the main loop completed
        its last tick within WATCHDOG_THRESH_SEC.  If not, dumps a full
        Python thread stack trace to the log so the freeze is diagnosable.
        """
        import traceback as _tb
        WATCHDOG_THRESH = float(getattr(config, "WATCHDOG_THRESH_SEC", 15.0))
        while self.running:
            time.sleep(5.0)
            if not self.running:
                break
            with self._tick_lock:
                _ltt = self._last_tick_time
            age = time.time() - _ltt
            if age > WATCHDOG_THRESH:
                logger.error(
                    "🚨 WATCHDOG: main loop has not completed a tick in %.0fs "
                    "(threshold=%.0fs). Dumping thread stacks:",
                    age, WATCHDOG_THRESH,
                )
                frames = []
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
        with self._tick_lock:
            self._last_tick_time = time.time()
            self._publish_tick_time()

        if self.watchdog is None:
            _wd = threading.Thread(target=self._watchdog_loop, daemon=True, name="watchdog")
            _wd.start()

        while self.running:
            try:
                time.sleep(0.25)

                self.maybe_supervise_streams()
                self.maybe_send_report()
                self.maybe_log_heartbeat()

                pos = self.strategy.get_position() if self.strategy else None
                if not self.trading_enabled and pos is None:
                    with self._tick_lock:
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

                with self._tick_lock:
                    self._last_tick_time = time.time()
                    self._publish_tick_time()

            except KeyboardInterrupt:
                logger.warning(
                    "KeyboardInterrupt ignored by Telegram-only shutdown guard; "
                    "use /stop from Telegram to stop the bot."
                )
                continue
            except Exception:
                logger.exception("❌ Main loop error")
                time.sleep(1.0)

        self.running = False

    # =========================================================================
    # STOP
    # =========================================================================

    def stop(self) -> None:
        logger.info("Stopping liquidity-first quant bot...")
        self.running = False

        stop_msg = "🛑 <b>LIQUIDITY-FIRST QUANT BOT STOPPED</b>\nShut down gracefully"
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

        if self.watchdog is not None:
            try:
                self.watchdog.stop()
            except Exception as e:
                logger.warning("watchdog stop failed: %s", e)
            finally:
                self.watchdog = None

        send_telegram_message(stop_msg)
        logger.info("Liquidity-first quant bot stopped")


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    if bool(getattr(config, "MULTI_ASSET_ENABLED", True)):
        from orchestration.multi_asset_bot import MultiAssetQuantBot
        bot = MultiAssetQuantBot()
    else:
        bot = QuantBot()

    if threading.current_thread() is threading.main_thread():
        from runtime_shutdown_guard import install_telegram_only_shutdown_guard

        install_telegram_only_shutdown_guard(logger, "single/multi-asset-main")

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

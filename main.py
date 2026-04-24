"""
main.py — Liquidity-First Dual-Exchange Quant Bot
===================================================
Entry point.

Architecture (liquidity-first):
  1. Multi-TF liquidity scanner tracks BSL/SSL pools on 1m, 5m, 15m, 1h, 4h.
  2. Pool priority engine scores every pool by HTF confluence, touch count,
     freshness, and volume at the level.
  3. Directional intent detector (primary gate): CVD divergence + orderbook
     delta + tick aggression determines whether flow is driving TOWARD the
     highest-priority pool.  If not → wait.
  4. ICT structure validation (secondary): AMD phase, OB/FVG alignment, and
     premium/discount zone confirm the structural context for the trade.
  5. Entry: limit at OTE inside the sweep zone, or market at confirmed sweep.
  6. SL: placed at ICT structure — sweep wick → OB → swing low/high.
  7. TP: set at the opposing liquidity pool's sweep price.
  8. Trail: ICT structure only — BOS swing → CHoCH tighten → 15m structure.
  9. Post-sweep engine: CVD + structure decides continue/reverse/range.

Exchange routing:
  - Both exchange data managers start concurrently.
  - MarketAggregator fuses OB depth + CVD + tick flow from both.
  - Candles come exclusively from the primary (execution) exchange.
  - ExecutionRouter owns both OrderManagers; routes to the active one.
  - /setexchange <delta|coinswitch> switches execution at runtime
    (blocked while a position is open; no restart needed).

Startup sequence:
  1. Validate credentials for all configured exchanges.
  2. Construct API clients + OrderManagers for each exchange.
  3. Build ExecutionRouter (default from config.EXECUTION_EXCHANGE).
  4. Construct data managers + MarketAggregator.
  5. Set leverage on active exchange.
  6. Start aggregator (boots both DMs concurrently).
  7. Wait for primary DM readiness.
  8. Start strategy + main loop.
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

# ── v9 display engine (optional) ─────────────────────────────────────────────
try:
    from strategy.v9_display import format_heartbeat as _fmt_hb
    _V9_DISPLAY = True
except ImportError:
    _V9_DISPLAY = False

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

install_global_telegram_log_handler(level=logging.WARNING, throttle_seconds=5.0)


# ─────────────────────────────────────────────────────────────────────────────
# QUANT BOT
# ─────────────────────────────────────────────────────────────────────────────

class QuantBot:
    """
    Liquidity-first dual-exchange quant bot.

    Decision hierarchy on every tick:
      1. Are there scored BSL/SSL pools within range?       → LiquidityMap
      2. Is flow (CVD + OB delta + tick aggression)
         driving toward the highest-priority pool?          → FlowDetector
      3. Does ICT structure confirm the trade context?      → ICTEngine
      4. Entry at OTE or market-at-sweep; SL at structure;
         TP at opposing pool.                               → EntryEngine
      5. Trail via ICT structure only (BOS/CHoCH).          → TrailEngine

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
            logger.info("⚡ LIQUIDITY-FIRST QUANT BOT — DUAL-EXCHANGE")
            logger.info("   Pools → Flow → ICT → Entry at OTE → TP at Pool")
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
            send_telegram_message("âš ï¸ <b>WATCHDOG UNAVAILABLE</b>\nImport failed; fallback stale-tick logger only.")
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
                f"âš ï¸ <b>WATCHDOG START FAILED</b>\n<code>{e}</code>\nFallback stale-tick logger only.")

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
                "⚡ <b>LIQUIDITY-FIRST QUANT BOT STARTED</b>\n\n"
                f"Symbol:    {QCfg.SYMBOL()}\n"
                f"Price:     ${price:,.2f}\n"
                f"Execution: {self.execution_router.active_exchange.upper()}\n"
                f"Leverage:  {QCfg.LEVERAGE()}x\n\n"
                "<b>Architecture:</b>\n"
                "  1️⃣  Multi-TF liquidity pool scanner\n"
                "  2️⃣  Pool priority engine (HTF × touches × freshness)\n"
                "  3️⃣  Flow detector: CVD + OB delta + tick aggression\n"
                "  4️⃣  ICT validation: AMD + OB/FVG + P/D zone\n"
                "  5️⃣  Entry at OTE · SL at ICT structure · TP at pool\n"
                "  6️⃣  Trail: BOS → CHoCH → 15m structure only\n\n"
                "<b>Active Patches:</b>\n"
                "  🔬 MTF Pool Probability (Issue-1): distance-decay × TF-base × sig × session\n"
                "  📘 Plain-Limit TP/SL (Issue-2): resting limit orders — maker rebate, zero latency\n"
                "  🏦 Liquidity-Only Trail (Issue-3): pool-anchor SL, chandelier fallback\n"
                "  🎯 Conviction Gate (Issue-4): 7-factor gate, score ≥ 0.75, ~76% WR target\n\n"
                f"📡 <b>Data feed:</b> "
                f"{'DUAL — ' + secondary_name.upper() + ' secondary active' if dual_feed else 'SINGLE — primary only'}\n\n"
                f"<i>/setexchange delta|coinswitch to switch execution exchange</i>"
            )

            logger.info("🚀 LIQUIDITY-FIRST QUANT BOT RUNNING")
            return True

        except Exception:
            logger.exception("❌ Error starting quant bot")
            return False

    # =========================================================================
    # HEARTBEAT  (liquidity-first layout)
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
        exch  = self.execution_router.active_exchange.upper() if self.execution_router else "?"

        # ── v9.0 display engine ───────────────────────────────────────────────
        if _V9_DISPLAY and self.strategy:
            strat         = self.strategy
            engine_state  = "SCANNING"
            tracking_info = None
            primary_target = None
            n_bsl = 999.0
            n_ssl = 999.0
            sweep_count  = 0
            flow_conv    = 0.0
            flow_dir     = ""
            bsl_pools    = []
            ssl_pools    = []
            atr_val      = 0.0
            cvd_trend    = 0.0
            tick_flow    = 0.0

            if hasattr(strat, '_entry_engine') and strat._entry_engine is not None:
                engine_state  = strat._entry_engine.state
                tracking_info = strat._entry_engine.tracking_info

            if hasattr(strat, '_atr_5m') and strat._atr_5m is not None:
                atr_val = strat._atr_5m.atr

            if hasattr(strat, '_cvd') and strat._cvd is not None:
                try:
                    cvd_trend = strat._cvd.get_trend_signal()
                except Exception:
                    pass

            if hasattr(strat, '_tick_eng') and strat._tick_eng is not None:
                try:
                    tick_flow = strat._tick_eng.get_signal()
                except Exception:
                    pass

            if hasattr(strat, '_liq_map') and strat._liq_map is not None:
                try:
                    snap           = strat._liq_map.get_snapshot(price, atr_val)
                    primary_target = snap.primary_target
                    n_bsl          = snap.nearest_bsl_atr
                    n_ssl          = snap.nearest_ssl_atr
                    sweep_count    = len(snap.recent_sweeps)
                    bsl_pools      = getattr(snap, 'bsl_pools', None) or []
                    ssl_pools      = getattr(snap, 'ssl_pools', None) or []
                except Exception:
                    pass

            # Flow conviction from detectors
            if hasattr(strat, '_flow_conviction'):
                flow_conv = getattr(strat, '_flow_conviction', 0.0)
                flow_dir  = getattr(strat, '_flow_direction', "")

            # ── v10: Extract ICT context for institutional display ─────────
            _session     = ""
            _kill_zone   = ""
            _amd_phase   = ""
            _amd_bias    = ""
            _dr_pd       = 0.5
            _s15m        = ""
            _s4h         = ""
            _htf_bias    = ""
            _sweep_anal  = None

            if hasattr(strat, '_ict') and strat._ict is not None:
                try:
                    _ict = strat._ict
                    _session   = getattr(_ict, '_session', '')
                    _kill_zone = getattr(_ict, '_killzone', '')
                    _amd       = getattr(_ict, '_amd', None)
                    if _amd:
                        _amd_phase = getattr(_amd, 'phase', '')
                        _amd_bias  = getattr(_amd, 'bias', '')
                    # Per-TF structure
                    _tf = getattr(_ict, '_tf', {})
                    if '15m' in _tf:
                        _s15m = getattr(_tf['15m'], 'trend', '')
                    if '4h' in _tf:
                        _s4h = getattr(_tf['4h'], 'trend', '')
                    # Dealing range
                    _dr = getattr(_ict, '_dealing_range', None)
                    if _dr:
                        _dr_pd = getattr(_dr, 'current_pd', 0.5)
                except Exception:
                    pass

            # HTF bias
            if hasattr(strat, '_htf') and strat._htf is not None:
                try:
                    _htf_bias = (f"15m={strat._htf.trend_15m:+.1f} "
                                 f"4h={strat._htf.trend_4h:+.1f}")
                except Exception:
                    pass

            # Sweep analysis from entry engine
            if hasattr(strat, '_entry_engine') and strat._entry_engine is not None:
                try:
                    _sweep_anal = getattr(strat._entry_engine, '_last_sweep_analysis', None)
                except Exception:
                    pass

            stats = strat.get_stats() if strat else {}

            msg = _fmt_hb(
                price=price, feed=feed, exchange=exch,
                position=pos, engine_state=engine_state,
                tracking_info=tracking_info,
                primary_target=primary_target,
                nearest_bsl_atr=n_bsl, nearest_ssl_atr=n_ssl,
                recent_sweep_count=sweep_count,
                total_trades=stats.get("total_trades", 0),
                total_pnl=stats.get("total_pnl", 0.0),
                flow_conviction=flow_conv,
                flow_direction=flow_dir,
                bsl_pools=bsl_pools,
                ssl_pools=ssl_pools,
                atr=atr_val,
                cvd_trend=cvd_trend,
                tick_flow=tick_flow,
                # v10 institutional context
                session=_session,
                kill_zone=_kill_zone,
                amd_phase=_amd_phase,
                amd_bias=_amd_bias,
                dealing_range_pd=_dr_pd,
                structure_15m=_s15m,
                structure_4h=_s4h,
                sweep_analysis=_sweep_anal,
                htf_bias=_htf_bias,
            )
            logger.info(msg)
            return

        # ── Legacy heartbeat ──────────────────────────────────────────────────
        if pos:
            side  = pos.get("side", "?").upper()
            entry = pos.get("entry_price", 0.0)
            sl    = pos.get("sl_price", 0.0)
            tp    = pos.get("tp_price", 0.0)
            if entry <= 0 or side not in ("LONG", "SHORT"):
                logger.info(f"${price:,.2f} [{feed}] | PENDING FILL")
            else:
                pnl = (price - entry) if side == "LONG" else (entry - price)
                logger.info(
                    f"${price:,.2f} [{feed}] | IN {side} @ ${entry:,.2f} | "
                    f"SL ${sl:,.2f}  TP ${tp:,.2f} | unrealised {pnl:+.2f} pts")
        else:
            stats   = self.strategy.get_stats() if self.strategy else {}
            phase   = stats.get("current_phase", "SCANNING")
            trades  = stats.get("daily_trades", 0)
            pnl     = stats.get("total_pnl", 0.0)
            # Show pool state in fallback heartbeat
            pool_str = ""
            if self.strategy and hasattr(self.strategy, '_liq_map') and self.strategy._liq_map:
                try:
                    snap     = self.strategy._liq_map.get_snapshot(price, self.strategy._atr_5m.atr)
                    tgt      = snap.primary_target
                    # BUG-FIX M2: PoolTarget has no .level_type or .price attributes.
                    # Correct paths: tgt.pool.side.value and tgt.pool.price.
                    pool_str = (
                        f" | target={'BSL' if tgt.pool.side.value == 'BSL' else 'SSL'}"
                        f"@${tgt.pool.price:,.0f}"
                        if tgt else " | no target"
                    )
                except Exception:
                    pass
            logger.info(
                f"${price:,.2f} [{feed}|exec={exch}] | {phase}{pool_str} | "
                f"trades today: {trades} | session PnL: ${pnl:+.2f}")

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

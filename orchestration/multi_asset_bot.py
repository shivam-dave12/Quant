"""
orchestration/multi_asset_bot.py — portfolio scanner for confirmed instruments
================================================================================

One strategy instance per tradable instrument.  Every instrument has its own data
manager, execution router, risk ledger, strategy state, liquidity map and trail
state.  PortfolioGuard enforces account-level exposure so the scanner can watch
multiple contracts without stacking correlated risk blindly.
"""
from __future__ import annotations

import logging
import signal
import sys
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

import config
from aggregator.market_aggregator import MarketAggregator
from core.instruments import ExchangeName, TradableInstrument, instrument_scope
from execution.instrument_registry import InstrumentRegistry, DiscoveryReport
from execution.order_manager import OrderManager
from execution.router import ExecutionRouter
from exchanges.coinswitch.api import FuturesAPI as CoinSwitchAPI
from exchanges.coinswitch.data_manager import CoinSwitchDataManager
from exchanges.delta.api import DeltaAPI
from exchanges.delta.data_manager import DeltaDataManager
from risk.risk_manager import RiskManager
from strategy.quant_strategy import QuantStrategy
from telegram.notifier import send_telegram_message

logger = logging.getLogger(__name__)


@dataclass
class AssetContext:
    instrument: TradableInstrument
    data_manager: MarketAggregator
    execution_router: ExecutionRouter
    risk_manager: RiskManager
    strategy: QuantStrategy
    last_tick_time: float = 0.0
    last_report_sec: float = 0.0
    last_heartbeat_sec: float = 0.0
    last_analysis_sec: float = 0.0
    ready: bool = False

    @property
    def phase_name(self) -> str:
        """Strategy phase without leaking strategy internals into orchestration."""
        try:
            phase = getattr(getattr(self.strategy, "_pos", None), "phase", None)
            return str(getattr(phase, "name", "FLAT") or "FLAT")
        except Exception:
            return "UNKNOWN"

    @property
    def has_position(self) -> bool:
        """True for ENTERING/ACTIVE/EXITING; this reserves the contract slot."""
        try:
            return self.strategy.get_position() is not None
        except Exception:
            return False


class PortfolioGuard:
    """
    Account-level exposure coordinator.

    It does not judge alpha.  It only enforces portfolio mechanics:
      • many contracts may trade simultaneously, up to the configured slot cap;
      • one reserved slot per contract (ENTERING/ACTIVE/EXITING all count);
      • each contract sees a portfolio-adjusted balance slice before the existing
        BTC-style risk/margin sizing runs inside QuantStrategy.
    """
    def __init__(self) -> None:
        self.max_open_positions = max(1, int(getattr(config, "PORTFOLIO_MAX_OPEN_POSITIONS", 4)))
        self.max_same_class = max(1, int(getattr(config, "PORTFOLIO_MAX_OPEN_PER_ASSET_CLASS", self.max_open_positions)))
        self.max_per_contract = max(1, int(getattr(config, "PORTFOLIO_MAX_OPEN_PER_CONTRACT", 1)))
        self.budget_mode = str(getattr(config, "PORTFOLIO_BUDGET_MODE", "equal_slots")).lower()
        self._lock = threading.RLock()

    def reserved_contexts(self, contexts: List[AssetContext]) -> List[AssetContext]:
        return [c for c in contexts if c.has_position]

    def count_open(self, contexts: List[AssetContext]) -> int:
        return len(self.reserved_contexts(contexts))

    def can_evaluate_entry(self, ctx: AssetContext, contexts: List[AssetContext]) -> tuple[bool, str]:
        with self._lock:
            reserved_ctx = self.reserved_contexts(contexts)

            # One position slot per contract.  Existing slot is allowed only for
            # management/exits/trailing; a second entry cannot be opened by this
            # context because QuantStrategy remains non-FLAT.
            if ctx.has_position:
                return True, f"{ctx.phase_name.lower()} position management"

            same_contract = [c for c in reserved_ctx if c.instrument.asset_id == ctx.instrument.asset_id]
            if len(same_contract) >= self.max_per_contract:
                return False, f"contract slot occupied {ctx.instrument.asset_id} {len(same_contract)}/{self.max_per_contract}"

            if len(reserved_ctx) >= self.max_open_positions:
                return False, f"portfolio exposure cap {len(reserved_ctx)}/{self.max_open_positions}"

            same_class = [c for c in reserved_ctx if c.instrument.asset_class == ctx.instrument.asset_class]
            if len(same_class) >= self.max_same_class:
                return False, f"asset-class exposure cap {ctx.instrument.asset_class.value} {len(same_class)}/{self.max_same_class}"

            return True, "portfolio slot available"

    def allocate_balance(self, ctx: Optional[AssetContext], contexts: List[AssetContext], raw_balance: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Return a balance view scoped to one contract's portfolio slot.

        QuantStrategy already applies RISK_PER_TRADE and BALANCE_USAGE_PERCENTAGE
        to whatever `available` it receives.  Therefore the institutional way to
        preserve the current BTC sizing semantics across multiple simultaneous
        contracts is to give each contract an account-equity slice, not the full
        account equity.  With 4 slots and 60% balance usage, each contract may use
        about 15% of total equity as margin; all 4 together remain around the old
        60% portfolio envelope.
        """
        if raw_balance is None:
            return None
        if not isinstance(raw_balance, dict):
            return raw_balance

        try:
            raw_available = max(0.0, float(raw_balance.get("available", 0.0) or 0.0))
            raw_total = max(0.0, float(raw_balance.get("total", raw_available) or raw_available))
        except Exception:
            return raw_balance

        # Slot count is the configured maximum, not current open count.  That
        # prevents the first signal of the day from consuming the whole account
        # just because other contracts have not fired yet.
        configured_slots = max(1, self.max_open_positions)
        active_universe = max(1, len(contexts or []))
        slot_count = min(configured_slots, active_universe) if self.budget_mode in {"active_equal_slots", "active_slots"} else configured_slots

        slot_equity = raw_total / float(slot_count) if slot_count > 0 else raw_total
        slot_available = min(slot_equity, raw_available)

        # If the account has less free cash than one nominal slot, use the real
        # exchange-available cash.  Never fabricate availability.
        adjusted = dict(raw_balance)
        adjusted["available_raw"] = raw_available
        adjusted["total_raw"] = raw_total
        adjusted["available"] = max(0.0, slot_available)
        adjusted["total"] = max(0.0, slot_equity)
        adjusted["portfolio_scoped"] = True
        adjusted["portfolio_budget_mode"] = self.budget_mode
        adjusted["portfolio_slot_count"] = slot_count
        adjusted["portfolio_slot_available"] = slot_available
        adjusted["portfolio_reserved_slots"] = self.count_open(contexts or [])
        adjusted["portfolio_max_slots"] = self.max_open_positions
        if ctx is not None:
            adjusted["portfolio_asset_id"] = ctx.instrument.asset_id
        return adjusted


class PortfolioRiskManager(RiskManager):
    """RiskManager wrapper that exposes a per-contract balance slice."""
    def __init__(self, shared_api=None, *, allocator: Callable[[Optional[AssetContext], List[AssetContext], Optional[Dict[str, Any]]], Optional[Dict[str, Any]]], context_getter: Callable[[], Optional[AssetContext]], contexts_getter: Callable[[], List[AssetContext]]):
        super().__init__(shared_api=shared_api)
        self._portfolio_allocator = allocator
        self._portfolio_context_getter = context_getter
        self._portfolio_contexts_getter = contexts_getter

    def get_available_balance(self) -> Optional[Dict]:
        raw = super().get_available_balance()
        try:
            ctx = self._portfolio_context_getter()
            contexts = self._portfolio_contexts_getter()
            return self._portfolio_allocator(ctx, contexts, raw)
        except Exception:
            logger.exception("Portfolio balance allocation failed; using raw exchange balance")
            return raw


class MultiAssetQuantBot:
    def __init__(self) -> None:
        self.running = False
        self.contexts: List[AssetContext] = []
        self.guard = PortfolioGuard()
        self.discovery_report: Optional[DiscoveryReport] = None
        self.registry: Optional[InstrumentRegistry] = None
        self.trading_enabled = True
        self.trading_pause_reason = ""
        self._last_scan_report = 0.0
        self._lock = threading.RLock()

    def _build_api_clients(self):
        has_delta = bool(config.DELTA_API_KEY and config.DELTA_SECRET_KEY)
        has_cs = bool(config.COINSWITCH_API_KEY and config.COINSWITCH_SECRET_KEY)
        delta_api = DeltaAPI(config.DELTA_API_KEY, config.DELTA_SECRET_KEY,
                             testnet=getattr(config, "DELTA_TESTNET", False)) if has_delta else None
        cs_api = CoinSwitchAPI(config.COINSWITCH_API_KEY, config.COINSWITCH_SECRET_KEY) if has_cs else None
        return delta_api, cs_api


    def _active_context(self) -> Optional[AssetContext]:
        for c in self.contexts:
            if c.has_position:
                return c
        return self.contexts[0] if self.contexts else None

    @property
    def strategy(self):
        c = self._active_context(); return c.strategy if c else None

    @property
    def data_manager(self):
        c = self._active_context(); return c.data_manager if c else None

    @property
    def execution_router(self):
        c = self._active_context(); return c.execution_router if c else None

    @property
    def order_manager(self):
        return self.execution_router

    @property
    def risk_manager(self):
        c = self._active_context(); return c.risk_manager if c else None

    def format_assets_report(self) -> str:
        def esc(x):
            return str(x).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        lines = ["📡 <b>MULTI-ASSET SCANNER</b>", ""]
        lines.append(f"Reserved slots: {self.guard.count_open(self.contexts)}/{self.guard.max_open_positions}")
        lines.append(f"Budget mode: {esc(self.guard.budget_mode)} · one contract slot max: {self.guard.max_per_contract}")
        for ctx in self.contexts:
            inst = ctx.instrument
            try:
                px = ctx.data_manager.get_last_price()
            except Exception:
                px = 0.0
            pos = ctx.strategy.get_position()
            state = ctx.phase_name if pos else ("READY" if ctx.ready else "NOT READY")
            try:
                bal = ctx.risk_manager.get_available_balance() or {}
                budget = float(bal.get("available", 0.0) or 0.0)
                raw = float(bal.get("available_raw", budget) or budget)
                budget_txt = f"slot=${budget:,.2f} raw=${raw:,.2f}"
            except Exception:
                budget_txt = "slot=n/a"
            lines.append(f"• <b>{esc(inst.asset_id)}</b> {esc(inst.primary_exchange.value.upper())} {esc(inst.display_symbol)} — {esc(state)} @ {px:,.4f} · {esc(budget_txt)}")
        if self.discovery_report and self.discovery_report.unavailable:
            lines.append("\n<b>Unavailable:</b>")
            for aid, reason in self.discovery_report.unavailable.items():
                lines.append(f"⚪ {esc(aid)} — {esc(reason)}")
        return "\n".join(lines)

    def initialize(self) -> bool:
        try:
            logger.info("=" * 92)
            logger.info("⚡ MULTI-ASSET INSTITUTIONAL LIQUIDITY SCANNER")
            logger.info("   Live exchange catalogs only — no synthetic commodity/index/equity feeds")
            logger.info("=" * 92)
            delta_api, cs_api = self._build_api_clients()
            self.registry = InstrumentRegistry(execution_preference=getattr(config, "EXECUTION_EXCHANGE", "delta"))
            self.discovery_report = self.registry.discover(
                delta_api=delta_api,
                coinswitch_api=cs_api,
                requested=getattr(config, "MULTI_ASSET_REQUESTS", None),
                max_active=int(getattr(config, "SCANNER_MAX_ACTIVE_INSTRUMENTS", 8)),
                require_primary=False,
            )
            for line in self.discovery_report.terminal_lines():
                logger.info(line)
            if not self.discovery_report.matched:
                logger.error("No confirmed tradable instruments found. Scanner will not start.")
                return False

            for inst in self.discovery_report.matched:
                ctx = self._build_asset_context(inst, delta_api, cs_api)
                if ctx is not None:
                    self.contexts.append(ctx)
            if not self.contexts:
                logger.error("No asset contexts could be built.")
                return False
            logger.info("✅ Built %d isolated strategy contexts", len(self.contexts))
            return True
        except Exception:
            logger.exception("MultiAssetQuantBot initialisation failed")
            return False

    def _build_asset_context(self, inst: TradableInstrument, delta_api, cs_api) -> Optional[AssetContext]:
        primary_ex = inst.primary_exchange
        cs_om = None
        delta_om = None
        if ExchangeName.COINSWITCH in inst.by_exchange and cs_api is not None:
            cs_om = OrderManager(cs_api, exchange_name="coinswitch", instrument=inst)
        if ExchangeName.DELTA in inst.by_exchange and delta_api is not None:
            delta_om = OrderManager(delta_api, exchange_name="delta", instrument=inst)
        if not cs_om and not delta_om:
            logger.warning("%s skipped: no executable order manager", inst.asset_id)
            return None
        router = ExecutionRouter(coinswitch_om=cs_om, delta_om=delta_om, default=primary_ex.value)

        if primary_ex == ExchangeName.DELTA:
            primary_dm = DeltaDataManager(instrument=inst)
            secondary_dm = CoinSwitchDataManager(instrument=inst) if ExchangeName.COINSWITCH in inst.by_exchange and cs_api else None
        else:
            primary_dm = CoinSwitchDataManager(instrument=inst)
            secondary_dm = DeltaDataManager(instrument=inst) if ExchangeName.DELTA in inst.by_exchange and delta_api else None
        data = MarketAggregator(primary_dm=primary_dm, secondary_dm=secondary_dm, instrument=inst)

        # Context is created after the risk manager, so use a tiny holder to let
        # PortfolioRiskManager resolve its owning context at call-time.
        ctx_holder: Dict[str, AssetContext] = {}
        risk = PortfolioRiskManager(
            shared_api=router,
            allocator=self.guard.allocate_balance,
            context_getter=lambda: ctx_holder.get("ctx"),
            contexts_getter=lambda: list(self.contexts) if self.contexts else list(ctx_holder.values()),
        )
        strategy = QuantStrategy(router, instrument=inst)
        data.register_strategy(strategy)
        ctx = AssetContext(inst, data, router, risk, strategy)
        ctx_holder["ctx"] = ctx
        return ctx

    def start(self) -> bool:
        if not self.contexts:
            logger.error("No contexts initialised")
            return False
        ok_any = False
        for ctx in self.contexts:
            inst = ctx.instrument
            try:
                with instrument_scope(inst):
                    logger.info("▶️ Starting %s [%s/%s]", inst.asset_id, inst.primary_exchange.value, inst.display_symbol)
                    try:
                        ctx.execution_router.set_leverage(int(config.LEVERAGE))
                    except Exception as e:
                        logger.warning("%s leverage set failed/non-fatal: %s", inst.asset_id, e)
                    if not ctx.data_manager.start():
                        logger.error("%s data stream start failed", inst.asset_id)
                        continue
                    ready = ctx.data_manager.wait_until_ready(timeout_sec=float(getattr(config, "READY_TIMEOUT_SEC", 180)))
                    ctx.ready = bool(ready)
                    if not ready:
                        logger.error("%s data manager not ready", inst.asset_id)
                        continue
                    ok_any = True
                    logger.info("✅ %s ready @ %.4f", inst.asset_id, ctx.data_manager.get_last_price())
            except Exception:
                logger.exception("%s start failed", inst.asset_id)
        if not ok_any:
            return False
        self.running = True
        if self.discovery_report:
            send_telegram_message(self.discovery_report.telegram_html())
        send_telegram_message(self._startup_message())
        return True

    def _startup_message(self) -> str:
        lines = ["⚡ <b>MULTI-ASSET INSTITUTIONAL BOT STARTED</b>", ""]
        lines.append("<b>Execution universe:</b>")
        for ctx in self.contexts:
            inst = ctx.instrument
            lines.append(f"• <b>{inst.asset_id}</b> — {inst.primary_exchange.value.upper()} {inst.display_symbol}")
        lines.append("")
        lines.append("<b>Portfolio rules:</b>")
        lines.append(f"• Multiple simultaneous contracts allowed: {self.guard.max_open_positions} portfolio slots")
        lines.append(f"• One live/entering/exit slot per contract: max {self.guard.max_per_contract}")
        lines.append(f"• Balance allocation: {self.guard.budget_mode}; each contract receives a slot-scoped balance before BTC-style sizing")
        lines.append("• Live exchange products only; unavailable OIL/GOLD/SILVER/SPX/stocks are skipped")
        lines.append("• Alpha remains posterior/EV based; PortfolioGuard only controls exposure mechanics")
        return "\n".join(lines)

    def run(self) -> None:
        logger.info("📊 Multi-asset loop active")
        while self.running:
            try:
                now_ms = int(time.time() * 1000)
                for ctx in list(self.contexts):
                    if not ctx.ready:
                        continue
                    allowed, reason = self.guard.can_evaluate_entry(ctx, self.contexts)
                    if not allowed and not ctx.has_position:
                        self._log_throttled_asset(ctx, f"Portfolio exposure gate: {reason}")
                        continue
                    if not self.trading_enabled and not ctx.has_position:
                        continue
                    with instrument_scope(ctx.instrument):
                        t0 = time.time()
                        ctx.strategy.on_tick(ctx.data_manager, ctx.execution_router, ctx.risk_manager, now_ms)
                        dt_ms = (time.time() - t0) * 1000.0
                    ctx.last_tick_time = time.time()
                    if dt_ms > 5000:
                        logger.warning("%s on_tick took %.0fms", ctx.instrument.asset_id, dt_ms)
                    self._maybe_analysis_audit(ctx, dt_ms)
                    self._maybe_asset_heartbeat(ctx)
                time.sleep(float(getattr(config, "SCANNER_TICK_SLEEP_SEC", 0.25)))
            except KeyboardInterrupt:
                break
            except Exception:
                logger.exception("Multi-asset loop error")
                time.sleep(1.0)
        self.running = False

    def _log_throttled_asset(self, ctx: AssetContext, msg: str, interval: float = 60.0) -> None:
        now = time.time()
        if now - ctx.last_heartbeat_sec >= interval:
            ctx.last_heartbeat_sec = now
            with instrument_scope(ctx.instrument):
                logger.info("%s | %s", ctx.instrument.asset_id, msg)

    def _maybe_analysis_audit(self, ctx: AssetContext, dt_ms: float) -> None:
        """Per-contract proof-of-analysis log.

        This is deliberately separate from strategy internals.  It shows the
        scanner is actually stepping every active contract, even when the
        contract has no sweep or posterior event to report.
        """
        now = time.time()
        interval = float(getattr(config, "SCANNER_ASSET_ANALYSIS_LOG_SEC", 15.0))
        if interval <= 0 or now - ctx.last_analysis_sec < interval:
            return
        ctx.last_analysis_sec = now
        try:
            inst = ctx.instrument
            price = ctx.data_manager.get_last_price()
            pos = ctx.strategy.get_position()
            state = ctx.phase_name if pos else "SCANNING"
            with instrument_scope(inst):
                logger.info(
                    "ANALYSIS_TICK asset=%s primary=%s symbol=%s state=%s price=%.4f eval_ms=%.1f slots=%d/%d",
                    inst.asset_id, inst.primary_exchange.value.upper(), inst.display_symbol,
                    state, price, dt_ms, self.guard.count_open(self.contexts), self.guard.max_open_positions,
                )
        except Exception as e:
            logger.debug("analysis audit failed for %s: %s", ctx.instrument.asset_id, e)

    def _maybe_asset_heartbeat(self, ctx: AssetContext) -> None:
        now = time.time()
        if now - ctx.last_heartbeat_sec < float(getattr(config, "SCANNER_ASSET_HEARTBEAT_SEC", 60.0)):
            return
        ctx.last_heartbeat_sec = now
        try:
            price = ctx.data_manager.get_last_price()
            pos = ctx.strategy.get_position()
            state = "IN_POSITION" if pos else "SCANNING"
            with instrument_scope(ctx.instrument):
                logger.info("%s %s %s | price %.4f | %s | open=%d/%d",
                            ctx.instrument.asset_id, ctx.instrument.primary_exchange.value.upper(),
                            ctx.instrument.display_symbol, price, state,
                            self.guard.count_open(self.contexts), self.guard.max_open_positions)
        except Exception as e:
            logger.debug("heartbeat failed for %s: %s", ctx.instrument.asset_id, e)

    def stop(self) -> None:
        logger.info("Stopping multi-asset bot...")
        self.running = False
        for ctx in self.contexts:
            try:
                ctx.data_manager.stop()
            except Exception:
                pass
        send_telegram_message("🛑 <b>MULTI-ASSET INSTITUTIONAL BOT STOPPED</b>")


def main() -> None:
    bot = MultiAssetQuantBot()
    if threading.current_thread() is threading.main_thread():
        def _signal_handler(signum, frame):
            logger.info("Shutdown signal %s received", signum)
            bot.stop()
            sys.exit(0)
        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)
    if not bot.initialize():
        sys.exit(1)
    if not bot.start():
        sys.exit(1)
    try:
        bot.run()
    except Exception:
        logger.exception("Fatal multi-asset runtime error")
        bot.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()

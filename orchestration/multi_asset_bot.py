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
from orchestration.portfolio_manager import PortfolioManager, PortfolioRiskManager
from core.market_policy import active_policy
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


# Retired legacy portfolio guard/risk-manager classes were removed.
# Runtime uses orchestration.portfolio_manager.PortfolioManager and
# PortfolioRiskManager only, so there is no duplicate exposure allocator.

class MultiAssetQuantBot:
    def __init__(self) -> None:
        self.running = False
        self.contexts: List[AssetContext] = []
        self.guard = PortfolioManager()
        self.discovery_report: Optional[DiscoveryReport] = None
        self.registry: Optional[InstrumentRegistry] = None
        self.trading_enabled = True
        self.trading_pause_reason = ""
        self._last_scan_report = 0.0
        self._lock = threading.RLock()

    def _build_api_clients(self):
        validator = getattr(config, "validate_exchange_credentials", None)
        if callable(validator):
            validator()
        has_delta = bool(config.DELTA_API_KEY and config.DELTA_SECRET_KEY)
        has_cs = bool(config.COINSWITCH_API_KEY and config.COINSWITCH_SECRET_KEY)
        delta_api = DeltaAPI(config.DELTA_API_KEY, config.DELTA_SECRET_KEY,
                             testnet=getattr(config, "DELTA_TESTNET", False)) if has_delta else None
        cs_api = CoinSwitchAPI(config.COINSWITCH_API_KEY, config.COINSWITCH_SECRET_KEY) if has_cs else None
        return delta_api, cs_api


    def _instrument_leverage(self, inst: TradableInstrument) -> int:
        configured = max(1, int(getattr(config, "LEVERAGE", 1)))
        max_lev = float(getattr(inst, "max_leverage", 0.0) or 0.0)
        if max_lev <= 0:
            # Conservative fallback caps when an exchange product row omits the
            # leverage field. Delta xStock UI contracts are 25x; do not send 40x
            # just because BTC uses 40x.
            ac = str(getattr(inst, "asset_class", "") or "").lower()
            if "equity" in ac or "commodity" in ac or "index" in ac:
                max_lev = 25.0
        if max_lev > 0:
            return max(1, min(configured, int(max_lev)))
        return configured

    def _set_leverage_with_backoff(self, ctx: AssetContext, target: int) -> int:
        """Set leverage, downgrading if Delta rejects max_leverage_exceeded."""
        attempts = [int(target)]
        for v in (25, 20, 10, 5, 3, 2, 1):
            if v < attempts[-1] and v not in attempts:
                attempts.append(v)
        last_err = ""
        for lev in attempts:
            try:
                res = ctx.execution_router.set_leverage(lev)
                ok = isinstance(res, dict) and bool(res.get("success", True)) and not res.get("_error")
                err = str((res or {}).get("error", "") if isinstance(res, dict) else "")
                last_err = err or str(res)[:160]
                if ok and "max_leverage_exceeded" not in err:
                    return int(lev)
                if "max_leverage_exceeded" not in last_err:
                    break
                logger.warning("%s leverage %sx rejected by exchange: %s — trying lower", ctx.instrument.asset_id, lev, last_err)
            except Exception as e:
                last_err = str(e)
                if "max_leverage_exceeded" not in last_err:
                    break
        logger.warning("%s leverage set not confirmed; continuing data-only until order route validates. last=%s", ctx.instrument.asset_id, last_err)
        return max(1, int(attempts[-1]))


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
            venues = ", ".join(f"{ex.value.upper()}:{ei.display_symbol}" for ex, ei in inst.by_exchange.items())
            lev_txt = f" · lev≤{inst.max_leverage:g}x" if getattr(inst, "max_leverage", 0.0) else ""
            try:
                pol_txt = self.guard.report_line(ctx)
            except Exception:
                pol_txt = "policy=n/a"
            lines.append(f"• <b>{esc(inst.asset_id)}</b> primary={esc(inst.primary_exchange.value.upper())} {esc(inst.display_symbol)} [{esc(venues)}] — {esc(state)} @ {px:,.4f} · {esc(budget_txt)}{lev_txt} · {esc(pol_txt)}")
        if self.discovery_report and self.discovery_report.unavailable:
            lines.append("\n<b>Unavailable:</b>")
            for aid, reason in self.discovery_report.unavailable.items():
                lines.append(f"⚪ {esc(aid)} — {esc(reason)}")
        return "\n".join(lines)


    # ---------------------------------------------------------------------
    # Institutional command-center reports used by Telegram commands.
    # These replace the old BTC-era single-strategy /pnl /position /equity
    # views.  They aggregate all asset desks under the central portfolio
    # manager while preserving per-contract detail.
    # ---------------------------------------------------------------------
    @staticmethod
    def _esc(x: Any) -> str:
        return str(x).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    @staticmethod
    def _fmt_money(v: float, n: int = 2) -> str:
        try:
            return f"${float(v):+,.{n}f}"
        except Exception:
            return "$+0.00"

    @staticmethod
    def _fmt_price(v: float) -> str:
        try:
            f = float(v or 0.0)
            if abs(f) >= 1000: return f"${f:,.2f}"
            if abs(f) >= 100: return f"${f:,.2f}"
            if abs(f) >= 1: return f"${f:,.4f}"
            return f"${f:,.6f}"
        except Exception:
            return "$0.00"

    def _ctx_position_metrics(self, ctx: AssetContext) -> Dict[str, Any]:
        inst = ctx.instrument
        pos = ctx.strategy.get_position()
        px = 0.0
        try: px = float(ctx.data_manager.get_last_price() or 0.0)
        except Exception: pass
        pol = active_policy(inst)
        out: Dict[str, Any] = {
            "asset": inst.asset_id, "symbol": inst.display_symbol,
            "venue": inst.primary_exchange.value.upper(), "class": pol.asset_class,
            "price": px, "position": pos, "upnl": 0.0, "r": 0.0,
            "mfe_r": 0.0, "hold_min": 0.0, "state": ctx.phase_name,
            "trail_active": False, "sl": 0.0, "tp": 0.0, "entry": 0.0,
            "qty": 0.0, "side": "", "policy": pol,
        }
        if not pos:
            return out
        try:
            side = str(pos.side or "").upper()
            entry = float(pos.entry_price or 0.0)
            qty = float(pos.quantity or 0.0)
            move_pts = (px - entry) if side == "LONG" else (entry - px)
            upnl = move_pts * qty
            init_dist = float(pos.initial_sl_dist or 0.0) or abs(entry - float(pos.sl_price or 0.0))
            r_now = move_pts / init_dist if init_dist > 1e-10 else 0.0
            mfe_r = float(pos.peak_profit or 0.0) / init_dist if init_dist > 1e-10 else 0.0
            hold_m = (time.time() - float(pos.entry_time or time.time())) / 60.0
            out.update({
                "side": side, "entry": entry, "qty": qty, "upnl": upnl, "r": r_now,
                "mfe_r": mfe_r, "hold_min": hold_m, "sl": float(pos.sl_price or 0.0),
                "tp": float(pos.tp_price or 0.0), "trail_active": bool(getattr(pos, "trail_active", False)),
                "trade_mode": getattr(pos, "trade_mode", ""), "entry_id": getattr(pos, "entry_order_id", ""),
                "sl_id": getattr(pos, "sl_order_id", ""), "tp_id": getattr(pos, "tp_order_id", ""),
            })
        except Exception:
            pass
        return out

    def _all_trade_records(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for ctx in self.contexts:
            try:
                for t in list(getattr(ctx.strategy, "_trade_history", [])):
                    r = dict(t)
                    r.setdefault("asset", ctx.instrument.asset_id)
                    r.setdefault("symbol", ctx.instrument.display_symbol)
                    r.setdefault("venue", ctx.instrument.primary_exchange.value.upper())
                    rows.append(r)
            except Exception:
                continue
        return rows

    def format_portfolio_pnl_report(self) -> str:
        rows = [self._ctx_position_metrics(c) for c in self.contexts]
        open_rows = [r for r in rows if r.get("position")]
        trades = self._all_trade_records()
        total_realised = sum(float(t.get("pnl", 0.0) or 0.0) for t in trades)
        total_upnl = sum(float(r.get("upnl", 0.0) or 0.0) for r in open_rows)
        wins = sum(1 for t in trades if t.get("is_win"))
        total = len(trades)
        wr = (wins / total * 100.0) if total else 0.0
        icon = "🟢" if total_realised + total_upnl >= 0 else "🔴"
        lines = [
            f"{icon} <b>PORTFOLIO PnL COMMAND CENTER</b>",
            "━━━━━━━━━━━━━━━━━━━━━━━━",
            f"<code>OPEN   {len(open_rows):>2}/{self.guard.max_open_positions:<2}   UPNL {self._fmt_money(total_upnl):>12}   REAL {self._fmt_money(total_realised):>12}</code>",
            f"<code>TRADES {total:>4}   WR {wr:>5.1f}%   BUDGET {self._esc(self.guard.budget_mode)}</code>",
        ]
        if open_rows:
            lines.append("\n<b>Live exposure by desk</b>")
            for r in sorted(open_rows, key=lambda x: str(x.get("asset"))):
                trail = "TRAIL" if r.get("trail_active") else "INIT"
                lines.append(
                    f"<code>{self._esc(r['asset']):<6} {self._esc(r['side']):<5} {self._fmt_money(r['upnl']):>11} "
                    f"R {float(r['r']):+5.2f} MFE {float(r['mfe_r']):>4.2f} {trail:<5}</code>"
                )
                lines.append(
                    f"<code>       px {self._fmt_price(r['price']):>12} entry {self._fmt_price(r['entry']):>12} SL {self._fmt_price(r['sl']):>12}</code>"
                )
        if trades:
            lines.append("\n<b>Recent realised trades</b>")
            for t in sorted(trades, key=lambda x: float(x.get("timestamp", 0.0) or 0.0))[-6:][::-1]:
                pnl = float(t.get("pnl", 0.0) or 0.0)
                ok = "✅" if pnl >= 0 else "❌"
                lines.append(
                    f"{ok} <code>{self._esc(t.get('asset','?')):<6} {self._esc(str(t.get('side','?')).upper()):<5} {self._fmt_money(pnl):>11} "
                    f"{self._esc(str(t.get('reason',''))[:12]):<12}</code>"
                )
        return "\n".join(lines)

    def format_portfolio_position_report(self) -> str:
        rows = [self._ctx_position_metrics(c) for c in self.contexts]
        lines = ["🏛 <b>PORTFOLIO POSITIONS</b>", "━━━━━━━━━━━━━━━━━━━━━━━━"]
        open_rows = [r for r in rows if r.get("position")]
        if not open_rows:
            lines.append("No live positions. Scanner desks remain active.")
        for r in open_rows:
            pol = r["policy"]
            lines.append(
                f"\n<b>{self._esc(r['asset'])}</b>  <code>{self._esc(r['venue'])}:{self._esc(r['symbol'])}</code> · {self._esc(pol.asset_class)}"
            )
            lines.append(
                f"<code>{self._esc(r['side']):<5} qty {float(r['qty']):.6f}   R {float(r['r']):+.2f}   MFE {float(r['mfe_r']):.2f}   hold {float(r['hold_min']):.0f}m</code>"
            )
            lines.append(
                f"<code>ENTRY {self._fmt_price(r['entry']):>12}  PX {self._fmt_price(r['price']):>12}  UPNL {self._fmt_money(r['upnl']):>11}</code>"
            )
            lines.append(
                f"<code>SL    {self._fmt_price(r['sl']):>12}  TP {self._fmt_price(r['tp']):>12}  TRAIL {'ON' if r.get('trail_active') else 'WATCH'}</code>"
            )
            if r.get("sl_id") or r.get("tp_id"):
                lines.append(f"<code>BRKT  SL {str(r.get('sl_id') or '-')[:8]}…  TP {str(r.get('tp_id') or '-')[:8]}…</code>")
        lines.append("\n<b>Scanner desks</b>")
        for r in rows:
            if r.get("position"):
                continue
            lines.append(f"<code>{self._esc(r['asset']):<6} {self._esc(r['symbol']):<12} {self._esc(r['state']):<10} px {self._fmt_price(r['price'])}</code>")
        return "\n".join(lines)

    def format_portfolio_equity_report(self) -> str:
        lines = ["💼 <b>PORTFOLIO EQUITY / BUDGET</b>", "━━━━━━━━━━━━━━━━━━━━━━━━"]
        raw_total = raw_avail = 0.0
        got = False
        for ctx in self.contexts[:1]:
            try:
                bal = ctx.risk_manager.get_available_balance() or {}
                raw_avail = float(bal.get("available_raw", bal.get("available", 0.0)) or 0.0)
                raw_total = float(bal.get("total_raw", bal.get("total", raw_avail)) or raw_avail)
                got = True
            except Exception:
                pass
        if got:
            lines.append(f"<code>ACCOUNT available {self._fmt_price(raw_avail):>12}  total {self._fmt_price(raw_total):>12}</code>")
        lines.append(f"<code>SLOTS   used {self.guard.count_open(self.contexts):>2}/{self.guard.max_open_positions:<2}  mode {self._esc(self.guard.budget_mode)}</code>")
        for ctx in self.contexts:
            try:
                bal = ctx.risk_manager.get_available_balance() or {}
                pol = active_policy(ctx.instrument)
                lines.append(
                    f"<code>{self._esc(ctx.instrument.asset_id):<6} cash {self._fmt_price(float(bal.get('available',0) or 0)):>10} "
                    f"riskbase {self._fmt_price(float(bal.get('risk_total',0) or 0)):>10} lev {pol.leverage:>2}x margin {pol.margin_pct:.0%} risk×{pol.risk_multiplier:.2f}</code>"
                )
            except Exception:
                continue
        return "\n".join(lines)

    def format_portfolio_trades_report(self) -> str:
        trades = self._all_trade_records()
        lines = ["📋 <b>PORTFOLIO TRADE TAPE</b>", "━━━━━━━━━━━━━━━━━━━━━━━━"]
        if not trades:
            return "\n".join(lines + ["No closed trades recorded yet."])
        for t in sorted(trades, key=lambda x: float(x.get("timestamp", 0.0) or 0.0))[-12:][::-1]:
            pnl = float(t.get("pnl", 0.0) or 0.0)
            ok = "✅" if pnl >= 0 else "❌"
            lines.append(
                f"{ok} <code>{self._esc(t.get('asset','?')):<6} {self._esc(str(t.get('side','?')).upper()):<5} "
                f"{self._fmt_price(float(t.get('entry',0) or 0)):>10}→{self._fmt_price(float(t.get('exit',0) or 0)):>10} "
                f"PnL {self._fmt_money(pnl):>10} R {float(t.get('r', t.get('achieved_r',0)) or 0):+5.2f}</code>"
            )
            lines.append(f"    <i>{self._esc(str(t.get('reason',''))[:80])}</i>")
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
            manager=self.guard,
        )
        strategy = QuantStrategy(router, instrument=inst)
        data.register_strategy(strategy)
        ctx = AssetContext(inst, data, router, risk, strategy)
        ctx_holder["ctx"] = ctx
        return ctx

    def _start_one_context(self, ctx: AssetContext) -> bool:
        inst = ctx.instrument
        try:
            with instrument_scope(inst):
                logger.info("▶️ Starting %s [%s/%s] | %s", inst.asset_id, inst.primary_exchange.value, inst.display_symbol, self.guard.report_line(ctx))
                target_lev = self._instrument_leverage(inst)
                effective_lev = self._set_leverage_with_backoff(ctx, target_lev)
                max_txt = f" (cap={inst.max_leverage:g}x)" if getattr(inst, "max_leverage", 0.0) else ""
                logger.info("%s leverage target=%sx effective=%sx%s", inst.asset_id, target_lev, effective_lev, max_txt)
                if not ctx.data_manager.start():
                    logger.error("%s data stream start failed", inst.asset_id)
                    return False
                ready = ctx.data_manager.wait_until_ready(timeout_sec=float(getattr(config, "READY_TIMEOUT_SEC", 180)))
                ctx.ready = bool(ready)
                if not ready:
                    logger.error("%s data manager not ready", inst.asset_id)
                    return False
                venues = ", ".join(f"{ex.value}:{ei.display_symbol}" for ex, ei in inst.by_exchange.items())
                logger.info("✅ %s ready @ %.4f | venues=%s | %s", inst.asset_id, ctx.data_manager.get_last_price(), venues, self.guard.report_line(ctx))
                return True
        except Exception:
            logger.exception("%s start failed", inst.asset_id)
            return False

    def start(self) -> bool:
        if not self.contexts:
            logger.error("No contexts initialised")
            return False
        parallelism = max(1, int(getattr(config, "SCANNER_START_PARALLELISM", 4)))
        ok_flags: Dict[str, bool] = {}
        sem = threading.Semaphore(parallelism)
        threads = []

        def _runner(c: AssetContext):
            with sem:
                ok_flags[c.instrument.asset_id] = self._start_one_context(c)

        for ctx in self.contexts:
            t = threading.Thread(target=_runner, args=(ctx,), name=f"asset-start-{ctx.instrument.asset_id}", daemon=True)
            t.start(); threads.append(t)
        for t in threads:
            t.join()

        ok_any = any(ok_flags.values())
        if not ok_any:
            return False
        self.running = True
        if self.discovery_report:
            send_telegram_message(self.discovery_report.telegram_html())
        send_telegram_message(self._startup_message())
        return True

    def _startup_message(self) -> str:
        lines = ["🏛 <b>PORTFOLIO COMMAND CENTER ONLINE</b>", ""]
        lines.append("<b>Execution universe — asset-scoped strategy desks:</b>")
        for ctx in self.contexts:
            inst = ctx.instrument
            venues = ", ".join(f"{ex.value.upper()}:{ei.display_symbol}" for ex, ei in inst.by_exchange.items())
            lev = self._instrument_leverage(inst)
            pol = active_policy(inst)
            cadence = getattr(pol, "loop_interval_sec", getattr(pol, "tick_eval_sec", 0.0))
            lines.append(f"• <b>{inst.asset_id}</b> — {inst.primary_exchange.value.upper()}:{inst.display_symbol} | venues {venues} | lev {lev}x | {pol.asset_class} | risk×{pol.risk_multiplier:.2f} | margin {pol.margin_pct:.0%} | cadence {float(cadence):.2f}s")
        lines.append("")
        lines.append("<b>Portfolio rules:</b>")
        lines.append(f"• Multiple simultaneous contracts allowed: {self.guard.max_open_positions} portfolio slots")
        lines.append(f"• One live/entering/exit slot per contract: max {self.guard.max_per_contract}")
        lines.append(f"• Balance allocation: {self.guard.budget_mode}; cash is slot-scoped, risk base is {self.guard.risk_budget_mode}; sizing uses per-instrument policy, not BTC defaults")
        lines.append("• Live exchange products only; no synthetic symbols. Delta SPXUSD is SPX6900 crypto, not S&P 500, so it is not used for SPX_INDEX.")
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
                    interval = self.guard.evaluation_interval(ctx)
                    if not ctx.has_position and ctx.last_tick_time > 0 and time.time() - ctx.last_tick_time < interval:
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
                    "ANALYSIS_TICK asset=%s primary=%s symbol=%s state=%s price=%.4f eval_ms=%.1f slots=%d/%d %s",
                    inst.asset_id, inst.primary_exchange.value.upper(), inst.display_symbol,
                    state, price, dt_ms, self.guard.count_open(self.contexts), self.guard.max_open_positions,
                    self.guard.report_line(ctx),
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

# Backward-compatible test/import alias. Runtime uses PortfolioManager.
PortfolioGuard = PortfolioManager

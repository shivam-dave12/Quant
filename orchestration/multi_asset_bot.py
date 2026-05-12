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
try:
    from exchanges.hyperliquid.api import HyperliquidAPI
    from exchanges.hyperliquid.data_manager import HyperliquidDataManager
except Exception:
    HyperliquidAPI = None  # type: ignore
    HyperliquidDataManager = None  # type: ignore
try:
    from exchanges.icici.api import BreezeRestClient
    from exchanges.icici.market_session import icici_market_session_state
    from exchanges.icici.data_manager import ICICIOptionDataManager
    from exchanges.icici.underlying_data_manager import ICICIUnderlyingDataManager
except Exception:
    BreezeRestClient = None  # type: ignore
    ICICIOptionDataManager = None  # type: ignore
    ICICIUnderlyingDataManager = None  # type: ignore
    def icici_market_session_state():  # type: ignore
        class _S:
            is_open = True
            reason = "ICICI market session guard unavailable"
        return _S()
from risk.risk_manager import RiskManager
from orchestration.portfolio_manager import PortfolioManager, PortfolioRiskManager
from core.market_policy import active_policy
from strategy.quant_strategy import QuantStrategy
from telegram.notifier import send_telegram_message
from observability import institutional as obs
try:
    from telemetry.dashboard_emitter import DashboardEmitter
except Exception:
    DashboardEmitter = None  # type: ignore
try:
    from agents.portfolio_cio import PortfolioCIO
except Exception:
    PortfolioCIO = None  # type: ignore
try:
    from agents.tradable_ticker_desk import TradableTickerDesk, TradableDeskSelection
except Exception:
    TradableTickerDesk = None  # type: ignore
    TradableDeskSelection = None  # type: ignore

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
    last_decision_log_sec: float = 0.0
    last_decision_log_key: str = ""
    last_gate_log_sec: float = 0.0
    ready: bool = False
    started_at_sec: float = 0.0

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




class MultiAssetQuantBot:
    def __init__(self) -> None:
        self.running = False
        self.contexts: List[AssetContext] = []
        self.guard = PortfolioManager()
        self.discovery_report: Optional[DiscoveryReport] = None
        self.registry: Optional[InstrumentRegistry] = None
        self.catalog_instruments: List[TradableInstrument] = []
        self.delta_api = None
        self.hyperliquid_api = None
        self.cs_api = None
        self.icici_api = None
        self.ticker_desk = TradableTickerDesk() if TradableTickerDesk is not None and bool(getattr(config, "DYNAMIC_TRADABLE_DESK_ENABLED", True)) else None
        self.last_desk_selection = None
        self._last_dynamic_desk_refresh = 0.0
        self.trading_enabled = True
        self.trading_pause_reason = ""
        self._last_scan_report = 0.0
        self._last_cio_cycle = 0.0
        self._last_cio_report_log = 0.0
        self._last_cio_report = None
        self._lock = threading.RLock()
        self.dashboard = DashboardEmitter.from_config() if DashboardEmitter is not None else None
        self.cio = (
            PortfolioCIO()
            if PortfolioCIO is not None and bool(getattr(config, "AGENTIC_FUND_ENABLED", True))
            else None
        )

    def _build_api_clients(self):
        wants_hl = bool(getattr(config, "HYPERLIQUID_ENABLED", True))
        has_delta = bool(config.DELTA_API_KEY and config.DELTA_SECRET_KEY)
        has_cs = bool(config.COINSWITCH_API_KEY and config.COINSWITCH_SECRET_KEY)
        has_icici_discovery = bool(getattr(config, "ICICI_DISCOVERY_ENABLED", False))
        wants_icici_details = bool(getattr(config, "DYNAMIC_DESK_ICICI_DETAILS_ENABLED", False))
        wants_icici_runtime = bool(getattr(config, "ICICI_ENABLED", False))
        hl_api = HyperliquidAPI() if wants_hl and HyperliquidAPI is not None else None
        delta_api = DeltaAPI(config.DELTA_API_KEY, config.DELTA_SECRET_KEY,
                             testnet=getattr(config, "DELTA_TESTNET", False)) if has_delta else None
        cs_api = CoinSwitchAPI(config.COINSWITCH_API_KEY, config.COINSWITCH_SECRET_KEY) if has_cs else None
        icici_api = BreezeRestClient() if (has_icici_discovery or wants_icici_details or wants_icici_runtime) and BreezeRestClient is not None else None
        if icici_api is not None and bool(getattr(config, "ICICI_BREEZE_PREFLIGHT_ON_STARTUP", True)) and (wants_icici_details or wants_icici_runtime):
            try:
                session_info = icici_api.preflight_session()
                logger.info("ICICI Breeze auth preflight OK — session=%s", session_info.get("session_token", "***"))
            except Exception as exc:
                msg = (
                    "ICICI Breeze auth preflight unavailable: %s. "
                    "Run token generator first and set BREEZE_API_SESSION/ICICI_API_SESSION or ICICI_API_SESSION_PATH; "
                    "Security Master discovery can continue, but authenticated quotes/details are disabled."
                )
                if wants_icici_runtime or bool(getattr(config, "ICICI_AUTH_REQUIRED_FOR_DETAILS", False)):
                    logger.error(msg, exc)
                    raise
                logger.warning(msg, exc)
        return hl_api, delta_api, cs_api, icici_api


    def _instrument_leverage(self, inst: TradableInstrument) -> int:
        # v83: leverage is produced by core.market_policy from the confirmed
        # instrument/exchange. No global LEVERAGE and no guessed xStock caps.
        with instrument_scope(inst):
            return max(1, int(active_policy(inst).leverage))

    def _set_leverage_with_backoff(self, ctx: AssetContext, target: int) -> int:
        """Set venue leverage only when the venue and product explicitly support it."""
        inst = ctx.instrument
        primary = getattr(inst, "primary_exchange", None)
        if primary == ExchangeName.ICICI or int(target) <= 1:
            logger.info("%s leverage unchanged at 1x — fully funded/non-leveraged venue policy", inst.asset_id)
            return 1
        try:
            res = ctx.execution_router.set_leverage(int(target))
            if res is None and primary == ExchangeName.DELTA:
                # Some Delta responses come back empty even when the request was
                # accepted. If the target came from configured/venue cap policy,
                # do not silently downgrade the internal desk to 1x. Explicit API
                # errors still force 1x below.
                logger.warning("%s leverage %sx returned empty confirmation; retaining target under configured Delta venue-cap policy", inst.asset_id, target)
                return int(target)
            ok = isinstance(res, dict) and bool(res.get("success", True)) and not res.get("_error")
            err = str((res or {}).get("error", "") if isinstance(res, dict) else "")
            if ok and not err:
                return int(target)
            logger.warning("%s leverage %sx not confirmed by exchange: %s — forcing 1x policy until product metadata/API response is corrected", inst.asset_id, target, err or str(res)[:160])
        except Exception as e:
            logger.warning("%s leverage %sx set failed: %s — forcing 1x policy until product metadata/API response is corrected", inst.asset_id, target, e)
        return 1


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
        return obs.format_desks(self)

    def format_institutional_desks_report(self) -> str:
        return obs.format_desks(self)

    def format_institutional_desk_report(self, desk: str) -> str:
        return obs.format_desk(self, desk)

    def format_institutional_asset_report(self, asset: str) -> str:
        return obs.format_asset(self, asset)

    def format_institutional_why_report(self, asset: str) -> str:
        return obs.format_why(self, asset)

    def format_institutional_health_report(self) -> str:
        return obs.format_health(self)

    def format_icici_desk_report(self) -> str:
        return obs.format_icici(self)

    def format_calculation_report(self, asset: str) -> str:
        return obs.format_calculations(self, asset)

    def format_parameter_report(self, query: str = "") -> str:
        return obs.format_parameters(self, query)

    def format_selector_report(self, desk: str = "") -> str:
        return obs.format_selector(self, desk)

    def format_shutdown_diagnostics_report(self) -> str:
        return obs.format_shutdown_diagnostics()

    def _cio_report(self):
        if self.cio is None:
            return None
        now = time.time()
        cadence = max(0.5, float(getattr(config, "FUND_CIO_DECISION_SEC", 3.0)))
        if self._last_cio_report is None or now - self._last_cio_cycle >= cadence:
            self._last_cio_cycle = now
            self._last_cio_report = self.cio.select_execution_queue(self.contexts, self.guard)
            try:
                self._dash_emit({
                    "type": "fund_cycle",
                    "mode": "paper" if getattr(self.cio.mandate, "paper_mode", True) else "live",
                    "message": self.cio.format_report(),
                    **self._last_cio_report.as_dict(),
                })
            except Exception:
                pass
        return self._last_cio_report

    def _agent_allows_context(self, ctx: AssetContext, report=None) -> tuple[bool, str]:
        """CIO gate for entry scanning. Open positions are always managed."""
        if ctx.has_position:
            return True, "position management"
        if self.cio is None:
            return True, "agentic CIO disabled"
        report = report or self._cio_report()
        if report is None:
            return True, "CIO unavailable"
        if ctx.instrument.asset_id in report.selected_ids():
            return True, "CIO selected desk"
        verdict = getattr(report, "risk_verdicts", {}).get(ctx.instrument.asset_id)
        if verdict is not None:
            return False, getattr(verdict, "reason", "CIO parked desk")
        return False, "CIO parked desk"

    def format_fund_report(self) -> str:
        return obs.format_desks(self)


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
                    if "timestamp" not in r and "ts" in r:
                        r["timestamp"] = r.get("ts")
                    if "margin_used" not in r:
                        try:
                            entry = float(r.get("entry", 0.0) or 0.0)
                            qty = float(r.get("qty", 0.0) or 0.0)
                            lev = float(active_policy(ctx.instrument).leverage or 1)
                            r["margin_used"] = (entry * qty / lev) if lev > 0 else entry * qty
                            pnl = float(r.get("pnl", 0.0) or 0.0)
                            r["margin_pnl_pct"] = pnl / r["margin_used"] * 100.0 if r["margin_used"] > 1e-10 else 0.0
                        except Exception:
                            r["margin_used"] = 0.0
                    rows.append(r)
            except Exception:
                continue
        return rows

    def _match_trail_contexts(self, asset_filter: Optional[str] = None) -> List[AssetContext]:
        if not asset_filter:
            return list(self.contexts)
        needle = str(asset_filter).strip().upper()
        out: List[AssetContext] = []
        for ctx in self.contexts:
            inst = ctx.instrument
            symbols = {str(inst.asset_id).upper(), str(inst.display_symbol).upper()}
            try:
                symbols.update(str(ei.display_symbol).upper() for ei in inst.by_exchange.values())
            except Exception:
                pass
            if needle in symbols:
                out.append(ctx)
        return out

    def set_trailing_override(self, enabled: Optional[bool], asset_filter: Optional[str] = None) -> Dict[str, Any]:
        targets = self._match_trail_contexts(asset_filter)
        changed: List[str] = []
        failed: List[str] = []
        for ctx in targets:
            try:
                ctx.strategy.set_trail_override(enabled)
                changed.append(ctx.instrument.asset_id)
            except Exception as exc:
                failed.append(f"{ctx.instrument.asset_id}:{exc}")
        self._dash_emit({
            "type": "alert" if failed else "tail_status",
            "severity": "warning" if failed else "info",
            "asset": asset_filter or "PORTFOLIO",
            "venue": "LOCAL",
            "symbol": "TRAIL",
            "title": "Trailing override changed",
            "message": f"enabled={enabled} target={asset_filter or 'ALL'} changed={changed} failed={failed}",
            "health": "WARN" if failed else "OK",
        }, critical=bool(failed))
        return {"changed": changed, "failed": failed, "target": asset_filter or "ALL", "enabled": enabled}

    def format_trailing_control_report(self, result: Optional[Dict[str, Any]] = None) -> str:
        lines = ["🛡 <b>PORTFOLIO TRAILING CONTROL</b>", "━━━━━━━━━━━━━━━━━━━━━━━━"]
        if result is not None:
            enabled = result.get("enabled")
            act = "ON" if enabled is True else ("OFF" if enabled is False else "CONFIG DEFAULT")
            lines.append(f"Command: <b>{self._esc(act)}</b> target <code>{self._esc(result.get('target','ALL'))}</code>")
            lines.append(f"Changed: <code>{self._esc(', '.join(result.get('changed') or []) or 'none')}</code>")
            if result.get("failed"):
                lines.append(f"Failed: <code>{self._esc(', '.join(result.get('failed') or []))}</code>")
        for ctx in self.contexts:
            try:
                override = getattr(getattr(ctx.strategy, "_pos", None), "trail_override", None)
                effective = bool(ctx.strategy.get_trail_enabled())
            except Exception:
                override = None; effective = False
            mode = "FORCED_ON" if override is True else ("FORCED_OFF" if override is False else "DEFAULT")
            pos = "LIVE" if ctx.has_position else ("READY" if ctx.ready else "WARMUP")
            lines.append(f"<code>{self._esc(ctx.instrument.asset_id):<6} {self._esc(ctx.instrument.display_symbol):<12} {pos:<7} trail {'ON' if effective else 'OFF'} ({mode})</code>")
        lines.append("\nUse <code>/trail on</code>, <code>/trail off</code>, <code>/trail on SILVER</code>, or <code>/trail auto</code>.")
        return "\n".join(lines)

    def format_portfolio_balance_report(self) -> str:
        return self.format_portfolio_equity_report()

    def _portfolio_trade_metrics(self, trades: List[Dict[str, Any]]) -> Dict[str, float]:
        total = len(trades)
        wins = sum(1 for t in trades if bool(t.get("is_win")))
        total_pnl = sum(float(t.get("pnl", 0.0) or 0.0) for t in trades)
        win_pnl = sum(float(t.get("pnl", 0.0) or 0.0) for t in trades if bool(t.get("is_win")))
        loss_pnl = abs(sum(float(t.get("pnl", 0.0) or 0.0) for t in trades if not bool(t.get("is_win"))))
        margin = sum(max(0.0, float(t.get("margin_used", 0.0) or 0.0)) for t in trades)
        win_margin = sum(max(0.0, float(t.get("margin_used", 0.0) or 0.0)) for t in trades if bool(t.get("is_win")))
        count_wr = (wins / total * 100.0) if total else 0.0
        cap_wr = (win_margin / margin * 100.0) if margin > 1e-10 else 0.0
        net_margin_return = (total_pnl / margin * 100.0) if margin > 1e-10 else 0.0
        profit_factor = (win_pnl / loss_pnl) if loss_pnl > 1e-10 else (999.0 if win_pnl > 0 else 0.0)
        return {
            "total": float(total), "wins": float(wins), "count_wr": count_wr,
            "capital_wr": cap_wr, "net_margin_return": net_margin_return,
            "total_pnl": total_pnl, "total_margin": margin,
            "profit_factor": profit_factor,
        }

    def format_portfolio_pnl_report(self) -> str:
        rows = [self._ctx_position_metrics(c) for c in self.contexts]
        open_rows = [r for r in rows if r.get("position")]
        trades = self._all_trade_records()
        total_realised = sum(float(t.get("pnl", 0.0) or 0.0) for t in trades)
        total_upnl = sum(float(r.get("upnl", 0.0) or 0.0) for r in open_rows)
        m = self._portfolio_trade_metrics(trades)
        total = int(m["total"])
        icon = "🟢" if total_realised + total_upnl >= 0 else "🔴"
        outcome = "PROFIT" if total_realised > 0 else ("LOSS" if total_realised < 0 else "FLAT")
        lines = [
            f"{icon} <b>PORTFOLIO PnL COMMAND CENTER</b>",
            "━━━━━━━━━━━━━━━━━━━━━━━━",
            f"<code>OPEN   {len(open_rows):>2}/{self.guard.max_open_positions:<2}   UPNL {self._fmt_money(total_upnl):>12}   REAL {self._fmt_money(total_realised):>12}</code>",
            f"<code>RESULT {outcome:<6} NetMargin {m['net_margin_return']:+6.2f}%  PF {m['profit_factor']:>6.2f}</code>",
            f"<code>TRADES {total:>4}   CountWR {m['count_wr']:>5.1f}%   CapitalWR {m['capital_wr']:>5.1f}%   BUDGET {self._esc(self.guard.budget_mode)}</code>",
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
                margin = float(t.get("margin_used", 0.0) or 0.0)
                mpct = float(t.get("margin_pnl_pct", 0.0) or 0.0)
                lines.append(
                    f"{ok} <code>{self._esc(t.get('asset','?')):<6} {self._esc(str(t.get('side','?')).upper()):<5} {self._fmt_money(pnl):>11} "
                    f"mgn ${margin:>6.2f} {mpct:+6.1f}% {self._esc(str(t.get('reason',''))[:12]):<12}</code>"
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
        lines.append(f"<code>FIRM    open {self.guard.count_open(self.contexts):>2}/{self.guard.max_open_positions:<2}  mode {self._esc(getattr(self.guard, 'desk_budget_mode', self.guard.budget_mode))}</code>")
        for ctx in self.contexts:
            try:
                bal = ctx.risk_manager.get_available_balance() or {}
                pol = active_policy(ctx.instrument)
                book = self.guard.book_for_context(ctx)
                lines.append(
                    f"<code>{self._esc(ctx.instrument.asset_id):<6} {self._esc(book.desk_id):<22} "
                    f"book {int(bal.get('portfolio_desk_open',0) or 0):>1}/{int(bal.get('portfolio_desk_max_open',book.max_open) or book.max_open):<1} "
                    f"cash {self._fmt_price(float(bal.get('available',0) or 0)):>10} "
                    f"riskbase {self._fmt_price(float(bal.get('risk_total',0) or 0)):>10}</code>"
                )
                lines.append(
                    f"<code>{'':<6} cap {float(bal.get('portfolio_desk_capital_weight', book.capital_weight) or 0):>5.0%} "
                    f"risk {float(bal.get('portfolio_desk_risk_weight', book.risk_weight) or 0):>5.0%} "
                    f"lev {pol.leverage:>2}x margin {pol.margin_pct:.0%} risk×{pol.risk_multiplier:.2f}</code>"
                )
            except Exception:
                continue
        return "\n".join(lines)


    def format_portfolio_status_report(self) -> str:
        lines = ["🏛 <b>MULTI-ASSET BOT STATUS</b>", "━━━━━━━━━━━━━━━━━━━━━━━━"]
        lines.append(f"Running: {'YES' if self.running else 'NO'} · Trading: {'ENABLED' if self.trading_enabled else 'PAUSED'}")
        if self.trading_pause_reason:
            lines.append(f"Pause reason: {self._esc(self.trading_pause_reason)}")
        lines.append(f"Slots: {self.guard.count_open(self.contexts)}/{self.guard.max_open_positions} · Universe: {len(self.contexts)}")
        for ctx in self.contexts:
            px = 0.0
            try:
                px = float(ctx.data_manager.get_last_price() or 0.0)
            except Exception:
                pass
            pos = ctx.strategy.get_position()
            state = ctx.phase_name if pos else ("READY" if ctx.ready else "WARMUP")
            lines.append(
                f"<code>{self._esc(ctx.instrument.asset_id):<6} "
                f"{self._esc(ctx.instrument.primary_exchange.value.upper()+':'+ctx.instrument.display_symbol):<18} "
                f"{self._esc(state):<9} px {self._fmt_price(px):>12}</code>"
            )
        return "\n".join(lines)

    def format_portfolio_market_report(self) -> str:
        lines = ["📊 <b>PORTFOLIO MARKET SNAPSHOT</b>", "━━━━━━━━━━━━━━━━━━━━━━━━"]
        for ctx in self.contexts:
            inst = ctx.instrument
            px = atr = 0.0
            try:
                px = float(ctx.data_manager.get_last_price() or 0.0)
            except Exception:
                pass
            try:
                atr = float(getattr(getattr(ctx.strategy, "_atr_5m", None), "atr", 0.0) or 0.0)
            except Exception:
                pass
            bsl = ssl = "-"
            try:
                liq = getattr(ctx.strategy, "_liq_map", None)
                if liq and px > 0 and atr > 0:
                    snap = liq.get_snapshot(px, atr)
                    if getattr(snap, "bsl_pools", None):
                        t = min(snap.bsl_pools, key=lambda x: x.distance_atr)
                        bsl = f"${t.pool.price:,.2f}/{t.distance_atr:.1f}A"
                    if getattr(snap, "ssl_pools", None):
                        t = min(snap.ssl_pools, key=lambda x: x.distance_atr)
                        ssl = f"${t.pool.price:,.2f}/{t.distance_atr:.1f}A"
            except Exception:
                pass
            lines.append(
                f"<code>{self._esc(inst.asset_id):<6} px {self._fmt_price(px):>12} "
                f"ATR {atr:>8.4f} BSL {self._esc(bsl):>15} SSL {self._esc(ssl):>15}</code>"
            )
        return "\n".join(lines)

    def format_portfolio_risk_report(self) -> str:
        lines = ["🛡 <b>PORTFOLIO RISK</b>", "━━━━━━━━━━━━━━━━━━━━━━━━"]
        lines.append(f"Firm open: {self.guard.count_open(self.contexts)}/{self.guard.max_open_positions} · Budget {self._esc(getattr(self.guard, 'desk_budget_mode', self.guard.budget_mode))}")
        for ctx in self.contexts:
            try:
                can, reason = ctx.risk_manager.can_trade()
            except Exception as e:
                can, reason = False, str(e)
            pol = active_policy(ctx.instrument)
            book = self.guard.book_for_context(ctx)
            pos = ctx.strategy.get_position()
            status = "LIVE" if pos else ("OPEN" if can else "LOCK")
            lines.append(
                f"<code>{self._esc(ctx.instrument.asset_id):<6} {status:<5} {self._esc(book.desk_id):<22} "
                f"book {self.guard.count_open_for_desk(book.desk_id, self.contexts)}/{book.max_open} "
                f"lev {pol.leverage:>2}x risk×{pol.risk_multiplier:.2f} · {self._esc(reason)[:60]}</code>"
            )
        return "\n".join(lines)

    def format_portfolio_sl_tp_report(self) -> str:
        rows = [self._ctx_position_metrics(c) for c in self.contexts]
        open_rows = [r for r in rows if r.get("position")]
        lines = ["🛑 <b>PORTFOLIO SL / TP</b>", "━━━━━━━━━━━━━━━━━━━━━━━━"]
        if not open_rows:
            lines.append("No live positions.")
        for r in sorted(open_rows, key=lambda x: str(x.get("asset"))):
            side = self._esc(r.get("side", ""))
            trail = "ON" if r.get("trail_active") else "OFF/INIT"
            lines.append(f"\n<b>{self._esc(r['asset'])}</b> <code>{self._esc(r['venue'])}:{self._esc(r['symbol'])}</code> {side}")
            lines.append(
                f"<code>ENTRY {self._fmt_price(r['entry']):>12}  PX {self._fmt_price(r['price']):>12}  "
                f"R {float(r['r']):+5.2f}</code>"
            )
            lines.append(
                f"<code>SL    {self._fmt_price(r['sl']):>12}  TP {self._fmt_price(r['tp']):>12}  "
                f"TRAIL {trail}</code>"
            )
            if r.get("sl_id") or r.get("tp_id"):
                lines.append(f"<code>ORDERS SL {str(r.get('sl_id') or '-')[:10]}… TP {str(r.get('tp_id') or '-')[:10]}…</code>")
        return "\n".join(lines)

    def format_portfolio_trades_report(self) -> str:
        trades = self._all_trade_records()
        lines = ["📋 <b>PORTFOLIO TRADE TAPE</b>", "━━━━━━━━━━━━━━━━━━━━━━━━"]
        if not trades:
            return "\n".join(lines + ["No closed trades recorded yet."])
        for t in sorted(trades, key=lambda x: float(x.get("timestamp", 0.0) or 0.0))[-12:][::-1]:
            pnl = float(t.get("pnl", 0.0) or 0.0)
            ok = "✅" if pnl >= 0 else "❌"
            margin = float(t.get("margin_used", 0.0) or 0.0)
            mpct = float(t.get("margin_pnl_pct", 0.0) or 0.0)
            lines.append(
                f"{ok} <code>{self._esc(t.get('asset','?')):<6} {self._esc(str(t.get('side','?')).upper()):<5} "
                f"{self._fmt_price(float(t.get('entry',0) or 0)):>10}→{self._fmt_price(float(t.get('exit',0) or 0)):>10} "
                f"PnL {self._fmt_money(pnl):>10} R {float(t.get('r', t.get('achieved_r',0)) or 0):+5.2f} "
                f"Mgn ${margin:,.2f} {mpct:+.1f}%</code>"
            )
            lines.append(f"    <i>{self._esc(str(t.get('reason',''))[:80])}</i>")
        return "\n".join(lines)

    def _dash_emit(self, event: Dict[str, Any], *, critical: bool = False) -> None:
        try:
            if self.dashboard is not None:
                self.dashboard.emit(event, critical=critical)
        except Exception:
            # Dashboard must never affect trading or strategy runtime.
            pass

    def _dash_universe(self) -> None:
        if not self.dashboard:
            return
        try:
            if self.discovery_report:
                for inst in self.discovery_report.matched:
                    runtime_ready = self._has_supported_runtime(inst)
                    self._dash_emit({
                        "type": "catalog_asset",
                        "asset": inst.asset_id,
                        "venue": inst.primary_exchange.value.upper(),
                        "symbol": inst.display_symbol,
                        "primary": inst.primary_exchange.value.upper(),
                        "policy": inst.asset_class.value,
                        "data_status": "DISCOVERED" if runtime_ready else "COVERAGE_ONLY",
                        "last_reason": ", ".join(f"{ex.value.upper()}:{ei.display_symbol}" for ex, ei in inst.by_exchange.items()),
                        "health": "OK" if runtime_ready else "WATCH",
                    })
            for ctx in self.contexts:
                inst = ctx.instrument
                pol = active_policy(inst)
                self._dash_emit({
                    "type": "market_data",
                    "asset": inst.asset_id,
                    "venue": inst.primary_exchange.value.upper(),
                    "symbol": inst.display_symbol,
                    "primary": inst.primary_exchange.value.upper(),
                    "data_status": "CONTEXT_BUILT",
                    "policy": pol.asset_class,
                    "leverage": f"{pol.leverage}x",
                    "margin": f"{pol.margin_pct:.0%}",
                    "risk_mult": pol.risk_multiplier,
                    "health": "OK",
                })
        except Exception:
            pass

    def _dash_heartbeat(self) -> None:
        self._dash_emit({
            "type": "heartbeat",
            "mode": "live" if self.trading_enabled else "paused",
            "source": "direct",
            "max_positions": self.guard.max_open_positions,
            "open_positions": self.guard.count_open(self.contexts),
            "message": "multi-asset bot heartbeat",
        })

    def _dash_context(self, ctx: AssetContext, *, dt_ms: float = 0.0, force: bool = False) -> None:
        if not self.dashboard:
            return
        now = time.time()
        interval = float(getattr(config, "DASHBOARD_POSITION_UPDATE_SEC", 1.0)) if ctx.has_position else float(getattr(config, "DASHBOARD_SCAN_UPDATE_SEC", 5.0))
        last = getattr(ctx, "last_dashboard_sec", 0.0)
        if not force and interval > 0 and now - last < interval:
            return
        setattr(ctx, "last_dashboard_sec", now)
        inst = ctx.instrument
        pol = active_policy(inst)
        try:
            price = float(ctx.data_manager.get_last_price() or 0.0)
        except Exception:
            price = 0.0
        pos = ctx.strategy.get_position()
        state = ctx.phase_name if pos else ("READY" if ctx.ready else "WARMUP")
        base = {
            "asset": inst.asset_id,
            "venue": inst.primary_exchange.value.upper(),
            "symbol": inst.display_symbol,
            "price": price,
            "state": state,
            "phase": state,
            "open_positions": self.guard.count_open(self.contexts),
            "max_positions": self.guard.max_open_positions,
            "policy": pol.asset_class,
            "leverage": f"{pol.leverage}x",
            "margin": f"{pol.margin_pct:.0%}",
            "risk_mult": pol.risk_multiplier,
            "health": "OK",
        }
        try:
            atr = float(getattr(getattr(ctx.strategy, "_atr_5m", None), "atr", 0.0) or 0.0)
            if atr > 0:
                base["atr"] = atr
        except Exception:
            pass
        self._dash_emit({"type": "scan", **base})
        if pos:
            try:
                side = str(getattr(pos, "side", "")).upper()
                entry = float(getattr(pos, "entry_price", 0.0) or 0.0)
                qty = float(getattr(pos, "quantity", 0.0) or 0.0)
                move = (price - entry) if side == "LONG" else (entry - price)
                upnl = move * qty
                init_dist = float(getattr(pos, "initial_sl_dist", 0.0) or 0.0) or abs(entry - float(getattr(pos, "sl_price", 0.0) or 0.0))
                r_now = move / init_dist if init_dist > 1e-10 else 0.0
                mfe_r = float(getattr(pos, "peak_profit", 0.0) or 0.0) / init_dist if init_dist > 1e-10 else 0.0
                notional = entry * qty if entry > 0 and qty > 0 else 0.0
                lev_txt = str(pol.leverage).replace("x", "")
                try:
                    lev = float(lev_txt)
                except Exception:
                    lev = 1.0
                margin_used = notional / lev if lev > 0 else notional
                margin_pnl_pct = (upnl / margin_used * 100.0) if margin_used > 1e-10 else 0.0
                self._dash_emit({
                    "type": "position_update",
                    **base,
                    "side": side,
                    "qty": qty,
                    "entry": entry,
                    "sl": float(getattr(pos, "sl_price", 0.0) or 0.0),
                    "tp": float(getattr(pos, "tp_price", 0.0) or 0.0),
                    "upnl": upnl,
                    "r": r_now,
                    "mfe_r": mfe_r,
                    "notional": notional,
                    "margin_used": margin_used,
                    "margin_pnl_pct": margin_pnl_pct,
                    "trailing": "ON" if bool(getattr(pos, "trail_active", False)) else "WATCH",
                    "bracket": "SLTP" if (getattr(pos, "sl_order_id", "") or getattr(pos, "tp_order_id", "")) else "UNKNOWN",
                })
            except Exception:
                pass

    def initialize(self) -> bool:
        try:
            logger.info("=" * 92)
            logger.info("⚡ MULTI-ASSET INSTITUTIONAL LIQUIDITY SCANNER")
            logger.info("   Live exchange catalogs only — no synthetic commodity/index/equity feeds")
            logger.info("=" * 92)
            hl_api, delta_api, cs_api, icici_api = self._build_api_clients()
            self.hyperliquid_api, self.delta_api, self.cs_api, self.icici_api = hl_api, delta_api, cs_api, icici_api
            self.registry = InstrumentRegistry(execution_preference=getattr(config, "EXECUTION_EXCHANGE", "delta"))
            self.discovery_report = self.registry.discover(
                hyperliquid_api=hl_api,
                delta_api=delta_api,
                coinswitch_api=cs_api,
                icici_api=icici_api,
                discovery_mode=getattr(config, "UNIVERSE_DISCOVERY_MODE", "dynamic"),
                include_asset_classes=getattr(config, "UNIVERSE_INCLUDE_ASSET_CLASSES", ""),
                include_exchanges=getattr(config, "UNIVERSE_INCLUDE_EXCHANGES", ""),
                icici_security_master_url=getattr(config, "ICICI_SECURITY_MASTER_URL", None),
                requested=getattr(config, "MULTI_ASSET_REQUESTS", None),
                # v80: do not cap discovery.  The dynamic desk caps runtime
                # subscriptions after cheap catalog/ticker screening.
                max_active=0,
                require_primary=False,
            )
            self.catalog_instruments = list(self.discovery_report.matched)
            for line in self.discovery_report.terminal_lines(preview=int(getattr(config, "DISCOVERY_REPORT_PREVIEW", 80))):
                logger.info(line)
            if not self.catalog_instruments:
                logger.error("No confirmed tradable instruments found. Scanner will not start.")
                return False

            selected = self._select_runtime_instruments(force=True)
            coverage_only = 0
            for inst in selected:
                if not self._has_supported_runtime(inst):
                    coverage_only += 1
                    continue
                ctx = self._build_asset_context(inst, hl_api, delta_api, cs_api)
                if ctx is not None:
                    self.contexts.append(ctx)
            if coverage_only:
                logger.info("%d desk-selected instruments are coverage-only until their data/execution adapters are enabled", coverage_only)
            if not self.contexts:
                logger.error("No runtime contexts could be built from the dynamic desk shortlist.")
                return False
            logger.info("✅ Built %d isolated strategy contexts from %d discovered instruments", len(self.contexts), len(self.catalog_instruments))
            self._dash_universe()
            return True
        except Exception:
            logger.exception("MultiAssetQuantBot initialisation failed")
            return False

    def _select_runtime_instruments(self, *, force: bool = False) -> List[TradableInstrument]:
        """Select the instruments allowed to own live data subscriptions.

        This is the v80 efficiency gate: discovery may return 188+ products, but
        only the desk shortlist below gets candle/orderbook/trade streams.
        """
        if not self.catalog_instruments:
            return []
        if self.ticker_desk is None:
            max_active = int(getattr(config, "SCANNER_MAX_ACTIVE_INSTRUMENTS", 0) or 0)
            return self.catalog_instruments if max_active <= 0 else self.catalog_instruments[:max_active]
        active_ids = [c.instrument.asset_id for c in self.contexts]
        protected_ids = [c.instrument.asset_id for c in self.contexts if c.has_position]
        selection = self.ticker_desk.select(
            self.catalog_instruments,
            delta_api=self.delta_api,
            coinswitch_api=self.cs_api,
            icici_api=self.icici_api,
            active_ids=active_ids,
            protected_ids=protected_ids,
        )
        self.last_desk_selection = selection
        self._last_dynamic_desk_refresh = time.time()
        if force or time.time() - self._last_scan_report >= float(getattr(config, "FUND_CIO_REPORT_SEC", 30.0)):
            self._last_scan_report = time.time()
            logger.info("%s", selection.compact_text(limit=int(getattr(config, "DYNAMIC_DESK_LOG_TOP_N", 12))))
        try:
            self._dash_emit({
                "type": "tradable_desk",
                "selected": [r.as_dict() for r in selection.rows if r.selected],
                "rejected": [r.as_dict() for r in selection.rows if not r.selected][:50],
                "message": selection.compact_text(limit=int(getattr(config, "DYNAMIC_DESK_LOG_TOP_N", 12))),
                "notes": list(selection.notes),
            })
        except Exception:
            pass
        return list(selection.selected)

    def _maybe_refresh_runtime_desks(self) -> None:
        if self.ticker_desk is None or not self.catalog_instruments:
            return
        interval = float(getattr(config, "DYNAMIC_DESK_REFRESH_SEC", 180.0))
        if interval <= 0 or time.time() - self._last_dynamic_desk_refresh < interval:
            return
        selected = self._select_runtime_instruments(force=False)
        selected_ids = {str(x.asset_id) for x in selected}
        existing = {str(c.instrument.asset_id): c for c in self.contexts}
        existing_ids = set(existing.keys())
        if selected_ids == existing_ids:
            logger.info("DynamicDesk refresh retained current live set unchanged — preserving analysis state for %d contexts", len(self.contexts))

        # Add newly selected desks.  They alone open fresh subscriptions.
        for inst in selected:
            if str(inst.asset_id) in existing or not self._has_supported_runtime(inst):
                continue
            ctx = self._build_asset_context(inst, self.hyperliquid_api, self.delta_api, self.cs_api)
            if ctx is None:
                continue
            if self._start_one_context(ctx):
                self.contexts.append(ctx)
                logger.info("DynamicDesk activated %s — live streams opened only after shortlist selection", inst.asset_id)

        # Retire unselected idle desks after a residency window.  Never retire a
        # context with an open/entering/exiting position.
        min_residency = float(getattr(config, "DYNAMIC_DESK_MIN_RESIDENCY_SEC", 600.0))
        kept: List[AssetContext] = []
        for ctx in self.contexts:
            age = time.time() - float(getattr(ctx, "started_at_sec", 0.0) or 0.0)
            if str(ctx.instrument.asset_id) in selected_ids or ctx.has_position or age < min_residency:
                kept.append(ctx)
                continue
            try:
                ctx.data_manager.stop()
            except Exception:
                pass
            logger.info("DynamicDesk parked %s — idle streams closed; will resubscribe only if setup quality improves", ctx.instrument.asset_id)
        self.contexts = kept

    @staticmethod
    def _has_supported_runtime(inst: TradableInstrument) -> bool:
        # Runtime is allowed only when there is a real venue adapter. ICICI is
        # options-only and requires the Breeze polling data manager; cash/futures
        # never become runtime desks.
        if inst.primary_exchange == ExchangeName.ICICI:
            return bool(
                getattr(config, "ICICI_OPTIONS_RUNTIME_ENABLED", True)
                and getattr(inst, "asset_class", None).value == "option"
                and ICICIOptionDataManager is not None
            )
        return bool({ExchangeName.HYPERLIQUID, ExchangeName.DELTA, ExchangeName.COINSWITCH}.intersection(set(inst.by_exchange.keys())))

    def _build_asset_context(self, inst: TradableInstrument, hl_api, delta_api, cs_api) -> Optional[AssetContext]:
        primary_ex = inst.primary_exchange
        execution_primary = primary_ex
        if primary_ex != ExchangeName.ICICI and ExchangeName.HYPERLIQUID in inst.by_exchange and hl_api is not None:
            execution_primary = ExchangeName.HYPERLIQUID
        hl_om = None
        cs_om = None
        delta_om = None
        icici_om = None
        if ExchangeName.HYPERLIQUID in inst.by_exchange and hl_api is not None:
            hl_om = OrderManager(hl_api, exchange_name="hyperliquid", instrument=inst)
        if ExchangeName.COINSWITCH in inst.by_exchange and cs_api is not None:
            cs_om = OrderManager(cs_api, exchange_name="coinswitch", instrument=inst)
        if ExchangeName.DELTA in inst.by_exchange and delta_api is not None:
            delta_om = OrderManager(delta_api, exchange_name="delta", instrument=inst)
        if ExchangeName.ICICI in inst.by_exchange and self.icici_api is not None:
            icici_om = OrderManager(self.icici_api, exchange_name="icici", instrument=inst)
        if not hl_om and not cs_om and not delta_om and not icici_om:
            logger.warning("%s skipped: no executable order manager", inst.asset_id)
            return None
        router = ExecutionRouter(coinswitch_om=cs_om, delta_om=delta_om, icici_om=icici_om, hyperliquid_om=hl_om, default=execution_primary.value)

        analysis_dm = None
        if execution_primary == ExchangeName.ICICI:
            if ICICIOptionDataManager is None:
                logger.warning("%s skipped: ICICI option data manager unavailable", inst.asset_id)
                return None
            primary_dm = ICICIOptionDataManager(instrument=inst, api=self.icici_api)
            secondary_dm = None
            if bool(getattr(config, "ICICI_USE_UNDERLYING_CHART_FOR_STRUCTURE", True)) and ICICIUnderlyingDataManager is not None:
                analysis_dm = ICICIUnderlyingDataManager(instrument=inst, api=self.icici_api)
        elif execution_primary == ExchangeName.HYPERLIQUID:
            if HyperliquidDataManager is None or hl_api is None:
                logger.warning("%s skipped: Hyperliquid data manager unavailable", inst.asset_id)
                return None
            primary_dm = HyperliquidDataManager(instrument=inst, api=hl_api)
            secondary_dm = DeltaDataManager(instrument=inst) if ExchangeName.DELTA in inst.by_exchange and delta_api else None
        elif execution_primary == ExchangeName.DELTA:
            primary_dm = DeltaDataManager(instrument=inst)
            secondary_dm = CoinSwitchDataManager(instrument=inst) if ExchangeName.COINSWITCH in inst.by_exchange and cs_api else None
        else:
            primary_dm = CoinSwitchDataManager(instrument=inst)
            secondary_dm = DeltaDataManager(instrument=inst) if ExchangeName.DELTA in inst.by_exchange and delta_api else None
        data = MarketAggregator(primary_dm=primary_dm, secondary_dm=secondary_dm, instrument=inst, analysis_dm=analysis_dm)

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

    def _icici_session_open_or_dormant(self, ctx: AssetContext) -> bool:
        """Return True when ICICI data should start now; otherwise mark dormant."""
        try:
            if ctx.instrument.primary_exchange != ExchangeName.ICICI:
                return True
            session = icici_market_session_state()
            if session.is_open or bool(getattr(config, "ICICI_ALLOW_CLOSED_MARKET_WARMUP", False)):
                return True
            # Closed market: do not trade, but allow Breeze historicalcharts warmup
            # so the desk can validate contracts and build context outside hours.
            if bool(getattr(config, "ICICI_ALLOW_CLOSED_MARKET_HISTORICAL_WARMUP", True)):
                try:
                    primary = getattr(ctx.data_manager, "_primary", None)
                    warm = getattr(primary, "warmup_closed_market", None)
                    if callable(warm):
                        warm(session.reason)
                    analysis = getattr(ctx.data_manager, "_analysis", None)
                    awarm = getattr(analysis, "warmup_closed_market", None)
                    if callable(awarm):
                        awarm(session.reason)
                except Exception as exc:
                    logger.warning("%s ICICI closed-market historical warmup failed: %s", ctx.instrument.asset_id, exc)
            ctx.ready = False
            ctx.started_at_sec = time.time()
            setattr(ctx, "dormant_reason", session.reason)
            logger.warning(
                "%s ICICI option runtime dormant — %s; historical warmup allowed but live quote/trading disabled until session opens",
                ctx.instrument.asset_id,
                session.reason,
            )
            return False
        except Exception:
            return True

    def _maybe_activate_dormant_icici(self, ctx: AssetContext) -> None:
        if ctx.ready or ctx.instrument.primary_exchange != ExchangeName.ICICI:
            return
        retry_sec = float(getattr(config, "ICICI_DORMANT_RETRY_SEC", 300.0))
        last = float(getattr(ctx, "last_icici_dormant_retry", 0.0) or 0.0)
        if time.time() - last < retry_sec:
            return
        setattr(ctx, "last_icici_dormant_retry", time.time())
        session = icici_market_session_state()
        if not session.is_open and not bool(getattr(config, "ICICI_ALLOW_CLOSED_MARKET_WARMUP", False)):
            self._log_throttled_asset(ctx, f"ICICI option desk dormant: {session.reason}")
            return
        logger.info("%s ICICI session is open; retrying option data manager start", ctx.instrument.asset_id)
        self._start_one_context(ctx)

    def _start_one_context(self, ctx: AssetContext) -> bool:
        inst = ctx.instrument
        try:
            with instrument_scope(inst):
                logger.info("▶️ Starting %s [%s/%s] | %s", inst.asset_id, inst.primary_exchange.value, inst.display_symbol, self.guard.report_line(ctx))
                if not self._icici_session_open_or_dormant(ctx):
                    return True
                target_lev = self._instrument_leverage(inst)
                effective_lev = self._set_leverage_with_backoff(ctx, target_lev)
                max_txt = f" (cap={inst.max_leverage:g}x)" if getattr(inst, "max_leverage", 0.0) else ""
                desk_id = getattr(getattr(self, "ticker_desk", None), "router", None).desk_id_for(inst) if getattr(getattr(self, "ticker_desk", None), "router", None) else "UNKNOWN"
                logger.info("[%s] %s leverage target=%sx effective=%sx%s", desk_id, inst.asset_id, target_lev, effective_lev, max_txt)
                if not ctx.data_manager.start():
                    logger.error("%s data stream start failed", inst.asset_id)
                    return False
                ready = ctx.data_manager.wait_until_ready(timeout_sec=float(getattr(config, "READY_TIMEOUT_SEC", 180)))
                ctx.ready = bool(ready)
                ctx.started_at_sec = time.time()
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
        self._dash_universe()
        self._dash_heartbeat()
        send_telegram_message(self._startup_message(), event_type="RUNTIME", enrich=False)
        return True

    def _startup_message(self) -> str:
        return obs.format_startup(self)

    def run(self) -> None:
        logger.info("🏛 Institutional desk loop active — desk-wise audit enabled")
        while self.running:
            try:
                now_ms = int(time.time() * 1000)
                self._maybe_refresh_runtime_desks()
                cio_report = self._cio_report()
                if cio_report is not None:
                    _now = time.time()
                    report_sec = float(getattr(config, "INSTITUTIONAL_DESK_CYCLE_LOG_SEC", getattr(config, "FUND_CIO_REPORT_SEC", 30.0)))
                    if report_sec > 0 and _now - self._last_cio_report_log >= report_sec:
                        self._last_cio_report_log = _now
                        logger.info("%s", obs.format_cycle_log(self))
                for ctx in list(self.contexts):
                    if not ctx.ready:
                        self._maybe_activate_dormant_icici(ctx)
                        continue
                    interval = self.guard.evaluation_interval(ctx)
                    if not ctx.has_position and ctx.last_tick_time > 0 and time.time() - ctx.last_tick_time < interval:
                        continue
                    agent_allowed, agent_reason = self._agent_allows_context(ctx, cio_report)
                    if not agent_allowed and not ctx.has_position:
                        self._log_throttled_asset(ctx, f"Agentic CIO gate: {agent_reason}")
                        continue
                    if self.cio is not None and not ctx.has_position:
                        mandate = getattr(self.cio, "mandate", None)
                        paper_mode = bool(getattr(mandate, "paper_mode", True))
                        live_enabled = bool(getattr(mandate, "live_ordering_enabled", False))
                        if paper_mode or not live_enabled:
                            try:
                                gate_detail = config.live_ordering_config_summary()
                            except Exception:
                                gate_detail = f"paper_mode={paper_mode} live_ordering={live_enabled}"
                            self._log_throttled_asset(
                                ctx,
                                "Agentic fund paper gate: live entry path held; "
                                "set FUND_PAPER_MODE=False and FUND_LIVE_ORDERING_ENABLED=True after validation; "
                                f"actual={gate_detail}",
                            )
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
                    self._maybe_decision_audit(ctx, dt_ms)
                    self._dash_context(ctx, dt_ms=dt_ms)
                time.sleep(float(getattr(config, "SCANNER_TICK_SLEEP_SEC", 0.25)))
            except KeyboardInterrupt:
                break
            except Exception:
                logger.exception("Multi-asset loop error")
                time.sleep(1.0)
        self.running = False

    def _log_throttled_asset(self, ctx: AssetContext, msg: str, interval: float = 60.0) -> None:
        now = time.time()
        if now - ctx.last_gate_log_sec >= interval:
            ctx.last_gate_log_sec = now
            with instrument_scope(ctx.instrument):
                logger.info("%s | %s", ctx.instrument.asset_id, msg)

    @staticmethod
    def _enum_name(value: Any) -> str:
        return str(getattr(value, "name", value) or "")

    @staticmethod
    def _short_text(value: Any, limit: int = 140) -> str:
        text = " ".join(str(value or "").replace("\n", " ").split())
        return text[:limit]

    @staticmethod
    def _fmt_bool(value: Any) -> str:
        return "Y" if bool(value) else "N"

    def _decision_audit_parts(self, ctx: AssetContext, dt_ms: float) -> tuple[str, list[str]]:
        inst = ctx.instrument
        strat = ctx.strategy
        entry = getattr(strat, "_entry_engine", None)
        engine_state = self._enum_name(getattr(entry, "state", "unknown"))
        price = 0.0
        try:
            price = float(ctx.data_manager.get_last_price() or 0.0)
        except Exception:
            pass
        book = self.guard.book_for_context(ctx)
        desk_open = self.guard.count_open_for_desk(book.desk_id, self.contexts)
        firm_open = self.guard.count_open(self.contexts)
        pos = None
        try:
            pos = strat.get_position()
        except Exception:
            pos = None

        parts = [
            "DESK_DECISION",
            f"asset={inst.asset_id}",
            f"desk={book.desk_id}",
            f"venue={inst.primary_exchange.value.upper()}",
            f"symbol={inst.display_symbol}",
            f"state={'IN_POSITION' if pos else 'SCANNING'}",
            f"engine={engine_state}",
            f"price={price:.4f}",
            f"eval_ms={dt_ms:.1f}",
            f"desk_open={desk_open}/{book.max_open}",
            f"firm_open={firm_open}/{self.guard.max_open_positions}",
        ]

        sig = getattr(strat, "_last_sig", None)
        if sig is not None:
            parts.append(
                "calc="
                f"comp={float(getattr(sig, 'composite', 0.0) or 0.0):+.4f},"
                f"thr={float(getattr(sig, 'threshold_used', 0.0) or 0.0):.4f},"
                f"dev={float(getattr(sig, 'deviation_atr', 0.0) or 0.0):+.2f}ATR,"
                f"atr={float(getattr(sig, 'atr', 0.0) or 0.0):.4f},"
                f"confirm={int(getattr(sig, 'n_confirming', 0) or 0)}/5,"
                f"regime={self._fmt_bool(getattr(sig, 'regime_ok', False))}"
            )

        sweep = getattr(entry, "_last_sweep_analysis", None) if entry is not None else None
        if isinstance(sweep, dict) and sweep:
            parts.append(
                "sweep="
                f"rev={float(sweep.get('rev_score', sweep.get('reversal_score', 0.0)) or 0.0):.0f},"
                f"cont={float(sweep.get('cont_score', sweep.get('continuation_score', 0.0)) or 0.0):.0f},"
                f"p={float(sweep.get('quant_posterior', sweep.get('posterior', 0.0)) or 0.0):.3f},"
                f"ev={float(sweep.get('quant_ev', sweep.get('expected_value', 0.0)) or 0.0):+.2f}R,"
                f"disp={float(sweep.get('displacement_atr', 0.0) or 0.0):.2f}ATR,"
                f"cisd={self._fmt_bool(sweep.get('cisd', False))},"
                f"ote={self._fmt_bool(sweep.get('ote', False))},"
                f"quality={float(sweep.get('quality_score', 0.0) or 0.0):.2f}"
            )

        inst_dec = getattr(strat, "_last_institutional_decision", None)
        if inst_dec is not None:
            reject = " | ".join(str(x) for x in (getattr(inst_dec, "reject_reasons", None) or [])[:3])
            allow = " | ".join(str(x) for x in (getattr(inst_dec, "allow_reasons", None) or [])[:3])
            parts.append(
                "decision="
                f"{'ALLOW' if bool(getattr(inst_dec, 'allowed', False)) else 'BLOCK'},"
                f"grade={getattr(inst_dec, 'grade', '?')},"
                f"score={float(getattr(inst_dec, 'score', 0.0) or 0.0):.2f},"
                f"rr={float(getattr(inst_dec, 'rr', 0.0) or 0.0):.2f},"
                f"target={float(getattr(inst_dec, 'target_realism', 0.0) or 0.0):.2f},"
                f"size={float(getattr(inst_dec, 'size_mult', 0.0) or 0.0):.2f}"
            )
            parts.append("blocker=" + self._short_text(reject or "none"))
            if allow:
                parts.append("allow=" + self._short_text(allow, 120))
        else:
            blocker = "waiting_for_valid_sweep"
            try:
                skip = getattr(entry, "scan_skip_info", None)
                if callable(skip):
                    skip = skip()
                if isinstance(skip, dict) and skip:
                    bits: list[str] = []
                    for group_name in ("liq", "bridge"):
                        group = skip.get(group_name)
                        if isinstance(group, dict):
                            vals = [f"{k}={v}" for k, v in group.items() if v]
                            if vals:
                                bits.append(group_name + ":" + ",".join(vals[:4]))
                    if bits:
                        blocker = "scan_deferred " + " ".join(bits)
            except Exception:
                pass
            try:
                pool_plan = getattr(entry, "pool_plan_info", None)
                if callable(pool_plan):
                    pool_plan = pool_plan()
                if isinstance(pool_plan, dict):
                    summary = self._short_text(pool_plan.get("summary"), 120)
                    if summary:
                        blocker = f"{pool_plan.get('role', 'POOL')}_plan {summary}"
            except Exception:
                pass
            parts.append("blocker=" + self._short_text(blocker))

        key_fields = [
            engine_state,
            parts[-1],
            parts[-2] if len(parts) > 1 else "",
            str(bool(pos)),
        ]
        return "|".join(key_fields), parts

    def _maybe_decision_audit(self, ctx: AssetContext, dt_ms: float) -> None:
        now = time.time()
        interval = float(getattr(
            config,
            "SCANNER_ASSET_DECISION_LOG_SEC",
            30.0,
        ) or 0.0)
        if interval <= 0:
            return
        stale_sec = max(interval, float(getattr(config, "SCANNER_ASSET_DECISION_STALE_LOG_SEC", 180.0) or 180.0))
        if now - ctx.last_decision_log_sec < interval:
            return
        try:
            key, parts = self._decision_audit_parts(ctx, dt_ms)
            if key == ctx.last_decision_log_key and now - ctx.last_decision_log_sec < stale_sec:
                return
            ctx.last_decision_log_key = key
            ctx.last_decision_log_sec = now
            with instrument_scope(ctx.instrument):
                logger.info(" | ".join(parts))
        except Exception as e:
            logger.debug("decision audit failed for %s: %s", ctx.instrument.asset_id, e)

    def stop(self) -> None:
        logger.info("Stopping multi-asset bot...")
        self.running = False
        for ctx in self.contexts:
            try:
                ctx.data_manager.stop()
            except Exception:
                pass
        send_telegram_message("🛑 <b>HEDGE-FUND COMMAND CENTER STOPPED</b>", event_type="RUNTIME", enrich=False)


def main() -> None:
    bot = MultiAssetQuantBot()
    from runtime.signal_guard import install_signal_handlers
    install_signal_handlers("multi_asset_bot", shutdown=bot.stop)
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

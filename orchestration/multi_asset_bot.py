"""
orchestration/multi_asset_bot.py — portfolio scanner for confirmed instruments
================================================================================

One strategy instance per tradable instrument.  Every instrument has its own data
manager, execution router, risk ledger, strategy state, liquidity map and fixed-SL TP ladder
state.  PortfolioManager enforces account-level exposure so the scanner can watch
multiple contracts without stacking correlated risk blindly.
"""
from __future__ import annotations

import logging
import signal
import sys
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import config
from aggregator.market_aggregator import MarketAggregator
from core.instruments import ExchangeName, TradableInstrument, instrument_scope
from core.pnl import gross_pnl_usd
from execution.instrument_registry import InstrumentRegistry, DiscoveryReport
from execution.order_manager import OrderManager
from execution.router import ExecutionRouter
try:
    from exchanges.coinswitch.api import FuturesAPI as CoinSwitchAPI
    from exchanges.coinswitch.data_manager import CoinSwitchDataManager
except Exception:  # optional venue dependency; other desks remain operational
    CoinSwitchAPI = None  # type: ignore
    CoinSwitchDataManager = None  # type: ignore
from exchanges.delta.api import DeltaAPI
from exchanges.delta.data_manager import DeltaDataManager
try:
    from exchanges.icici.api import BreezeRestClient
    from exchanges.icici.data_manager import ICICIOptionDataManager
    from exchanges.icici.market_session import icici_market_session_state
    from exchanges.icici.underlying_data_manager import ICICIUnderlyingDataManager
except Exception:  # pragma: no cover - ICICI is optional at runtime
    BreezeRestClient = None  # type: ignore
    ICICIOptionDataManager = None  # type: ignore
    icici_market_session_state = None  # type: ignore
    ICICIUnderlyingDataManager = None  # type: ignore
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
        has_delta = bool(config.DELTA_API_KEY and config.DELTA_SECRET_KEY)
        has_cs = bool(config.COINSWITCH_API_KEY and config.COINSWITCH_SECRET_KEY and CoinSwitchAPI is not None and CoinSwitchDataManager is not None)

        # V83 reference behaviour, adapted to v508:
        # build the Breeze client for configured-index discovery even before the
        # operator-generated API_Session exists. Constructor is auth-light; signed
        # Breeze calls still happen only inside the ICICI data/execution adapters.
        wants_icici_discovery = bool(getattr(config, "ICICI_DISCOVERY_ENABLED", False))
        wants_icici_runtime = bool(getattr(config, "ICICI_OPTIONS_RUNTIME_ENABLED", False))
        has_icici_runtime_keys = bool(
            getattr(config, "BREEZE_API_KEY", "")
            and getattr(config, "BREEZE_SECRET_KEY", "")
        )
        has_icici = bool(
            BreezeRestClient is not None
            and (wants_icici_discovery or (wants_icici_runtime and has_icici_runtime_keys))
        )
        delta_api = DeltaAPI(config.DELTA_API_KEY, config.DELTA_SECRET_KEY,
                             testnet=getattr(config, "DELTA_TESTNET", False)) if has_delta else None
        cs_api = CoinSwitchAPI(config.COINSWITCH_API_KEY, config.COINSWITCH_SECRET_KEY) if has_cs else None
        icici_api = BreezeRestClient() if has_icici else None
        if wants_icici_runtime and not has_icici_runtime_keys:
            logger.warning(
                "ICICI runtime enabled but Breeze API key/secret are missing; "
                "NIFTY discovery can continue, protected Breeze data/order calls will wait for credentials."
            )
        return delta_api, cs_api, icici_api

    @staticmethod
    def _is_icici_context(ctx: AssetContext) -> bool:
        try:
            inst = ctx.instrument
            return inst.primary_exchange == ExchangeName.ICICI or ExchangeName.ICICI in inst.by_exchange
        except Exception:
            return False

    @staticmethod
    def _icici_market_open() -> tuple[bool, str]:
        if not bool(getattr(config, "ICICI_ANALYZE_ONLY_DURING_MARKET_SESSION", True)):
            return True, "ICICI session guard disabled"
        if icici_market_session_state is None:
            return False, "ICICI market session guard unavailable"
        try:
            state = icici_market_session_state()
            return bool(state.is_open), str(state.reason or "")
        except Exception as exc:
            return False, f"ICICI market session check failed: {exc}"

    def _icici_account_preflight(self, ctx: AssetContext) -> bool:
        """Verify F&O funds and exact NFO option positions before ICICI analysis starts.

        The desk must never infer an option position from generic portfolio rows.
        Balance is sourced only through the adapter's FNO funds/NFO margin route;
        positions are sourced only through exact NFO Options PortfolioPositions rows.
        """
        try:
            balance = ctx.risk_manager.get_available_balance()
            if not isinstance(balance, dict) or balance.get("error"):
                logger.error(
                    "%s ICICI F&O preflight failed: could not verify FNO/NFO funds: %s",
                    ctx.instrument.asset_id, (balance or {}).get("error") if isinstance(balance, dict) else "no response",
                )
                return False
            if str(balance.get("segment") or "").upper() != "FNO":
                logger.error("%s ICICI F&O preflight rejected non-FNO balance payload: %s", ctx.instrument.asset_id, balance)
                return False
            logger.info(
                "%s ICICI F&O funds verified: source=%s available=₹%.2f allocated=₹%.2f blocked=₹%.2f nfo_cash_limit=₹%.2f",
                ctx.instrument.asset_id, balance.get("source", "unknown"),
                float(balance.get("available", 0.0) or 0.0),
                float(balance.get("fno_allocated", 0.0) or 0.0),
                float(balance.get("fno_blocked", 0.0) or 0.0),
                float(balance.get("nfo_cash_limit", 0.0) or 0.0),
            )
            broker_position = ctx.execution_router.get_open_position()
            if broker_position is None:
                logger.error("%s ICICI F&O preflight failed: PortfolioPositions could not be verified", ctx.instrument.asset_id)
                return False
            qty = float(broker_position.get("size", 0.0) or 0.0)
            if qty <= 0:
                logger.info(
                    "%s ICICI F&O positions verified FLAT: exact NFO option positions=0 ignored_non_option_rows=%d",
                    ctx.instrument.asset_id, int(broker_position.get("ignored_non_option_rows", 0) or 0),
                )
                return True
            if bool(broker_position.get("unadoptable")):
                logger.critical(
                    "%s ICICI F&O preflight blocked unmanaged option exposure: reason=%s qty=%.8g; no new orders permitted",
                    ctx.instrument.asset_id, broker_position.get("reason", "unknown"), qty,
                )
                return False
            logger.warning(
                "%s ICICI exact NFO option position found at startup and will be reconciled: %s %s %s qty=%.8g",
                ctx.instrument.asset_id, broker_position.get("stock_code"), broker_position.get("right"),
                broker_position.get("strike_price"), qty,
            )
            return True
        except Exception as exc:
            logger.exception("%s ICICI F&O account preflight failed: %s", ctx.instrument.asset_id, exc)
            return False


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
        lines = ["📡 <b>Portfolio Scanner Map</b>", "━━━━━━━━━━━━━━━━━━━━"]
        lines.append(f"🧩 Slots: <code>{self.guard.count_open(self.contexts)}/{self.guard.max_open_positions}</code>")
        lines.append(f"💼 Budget: <code>{esc(self.guard.budget_mode)}</code> · one-contract cap <code>{self.guard.max_per_contract}</code>")
        for ctx in self.contexts:
            inst = ctx.instrument
            try:
                px = ctx.data_manager.get_last_price()
            except Exception:
                px = 0.0
            cur = self._currency_for_instrument(inst)
            pos = ctx.strategy.get_position()
            state = ctx.phase_name if pos else ("READY" if ctx.ready else "NOT READY")
            try:
                bal = ctx.risk_manager.get_available_balance() or {}
                budget = float(bal.get("available", 0.0) or 0.0)
                raw = float(bal.get("available_raw", budget) or budget)
                budget_txt = f"slot={cur}{budget:,.2f} raw={cur}{raw:,.2f}"
            except Exception:
                budget_txt = "slot=n/a"
            venues = ", ".join(f"{ex.value.upper()}:{ei.display_symbol}" for ex, ei in inst.by_exchange.items())
            lev_txt = f" · lev≤{inst.max_leverage:g}x" if getattr(inst, "max_leverage", 0.0) else ""
            try:
                pol_txt = self.guard.report_line(ctx)
            except Exception:
                pol_txt = "policy=n/a"
            pnl_txt = ""
            if pos:
                try:
                    m = self._ctx_position_metrics(ctx)
                    pnl_txt = (
                        f" · uPnL={self._fmt_currency(m.get('upnl', 0.0), signed=True, currency=cur)} "
                        f"live={self._fmt_currency(m.get('lifecycle_pnl', m.get('upnl', 0.0)), signed=True, currency=cur)}"
                    )
                except Exception:
                    pnl_txt = ""
            status_icon = "🟢" if state in ("READY", "SCANNING") else ("🔵" if pos else "🟠")
            lines.append(
                f"{status_icon} <b>{esc(inst.asset_id)}</b>  <code>{esc(inst.primary_exchange.value.upper())}:{esc(inst.display_symbol)}</code>\n"
                f"   <code>{esc(state):<11} px {px:,.4f} · {esc(budget_txt)}{lev_txt}</code>\n"
                f"   <code>{esc(venues)}</code>\n"
                f"   <i>{esc(pol_txt)}{pnl_txt}</i>"
            )
        if self.discovery_report and self.discovery_report.unavailable:
            lines.append("\n⚪ <b>Unavailable</b>")
            for aid, reason in self.discovery_report.unavailable.items():
                lines.append(f"• {esc(aid)} — {esc(reason)}")
        return "\n".join(lines)


    # ---------------------------------------------------------------------
    # Institutional command-center reports used by Telegram commands.
    # Portfolio command-center reports used by Telegram commands.
    # views.  They aggregate all asset desks under the central portfolio
    # manager while preserving per-contract detail.
    # ---------------------------------------------------------------------
    @staticmethod
    def _esc(x: Any) -> str:
        return str(x).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    @staticmethod
    def _currency_for_instrument(inst: TradableInstrument) -> str:
        try:
            quote = str(getattr(getattr(inst, "primary", None), "quote_asset", "") or "").upper()
            ex = getattr(getattr(inst, "primary_exchange", None), "value", getattr(inst, "primary_exchange", ""))
            if quote == "INR" or str(ex).lower() == "icici":
                return "₹"
        except Exception:
            pass
        return "$"

    @classmethod
    def _fmt_currency(cls, v: float, *, signed: bool = False, currency: str = "$", n: int = 2) -> str:
        try:
            f = float(v or 0.0)
            sign = "+" if signed and f >= 0 else ""
            if not signed:
                sign = ""
            return f"{sign}{currency}{f:,.{n}f}"
        except Exception:
            return f"{currency}+0.00" if signed else f"{currency}0.00"

    def _ctx_position_metrics(self, ctx: AssetContext) -> Dict[str, Any]:
        inst = ctx.instrument
        pos_snapshot = ctx.strategy.get_position()
        px = 0.0
        try:
            px = float(ctx.data_manager.get_last_price() or 0.0)
        except Exception:
            pass
        pol = active_policy(inst)
        out: Dict[str, Any] = {
            "asset": inst.asset_id, "symbol": inst.display_symbol,
            "venue": inst.primary_exchange.value.upper(), "class": pol.asset_class,
            "currency": self._currency_for_instrument(inst),
            "desk": pol.desk_id, "desk_name": pol.desk_name, "strategy": pol.strategy_key,
            "price": px, "position": pos_snapshot, "upnl": 0.0,
            "unrealised_pnl": 0.0, "unrealized_pnl": 0.0,
            "open_realized": 0.0, "lifecycle_pnl": 0.0,
            "r": 0.0, "mfe_r": 0.0, "hold_min": 0.0, "state": ctx.phase_name,
            "sl": 0.0, "tp": 0.0, "entry": 0.0,
            "qty": 0.0, "side": "", "policy": pol,
            "entry_leverage": 0.0, "leverage": 0.0, "margin_used": 0.0,
        }
        if not pos_snapshot:
            ext = getattr(ctx.strategy, "_last_unmanaged_external_position", None)
            if isinstance(ext, dict) and float(ext.get("size", 0.0) or 0.0) > 0:
                out.update({
                    "external_position": ext,
                    "state": "EXTERNAL_UNADOPTED",
                    "side": str(ext.get("side") or "LONG").upper(),
                    "qty": float(ext.get("size", 0.0) or 0.0),
                    "entry": float(ext.get("entry_price", 0.0) or 0.0),
                    "upnl": float(ext.get("unrealized_pnl", 0.0) or 0.0),
                    "unrealised_pnl": float(ext.get("unrealized_pnl", 0.0) or 0.0),
                    "unrealized_pnl": float(ext.get("unrealized_pnl", 0.0) or 0.0),
                    "currency": "₹",
                    "contract_identity_source": ext.get("contract_identity_source", ""),
                    "unadoptable_reason": ext.get("reason", ""),
                    "option_symbol": ext.get("TradingSymbol") or ext.get("symbol") or "",
                    "right": ext.get("right") or "",
                    "strike": ext.get("strike_price") or "",
                    "expiry": ext.get("expiry_date") or "",
                })
            return out

        # get_position() returns a dict, while the strategy keeps the richer
        # PositionState at _pos.  Older portfolio reports treated the dict like
        # an object, swallowed AttributeError, and therefore rendered UPNL as 0.
        pos_obj = getattr(ctx.strategy, "_pos", None)
        try:
            if pos_obj is not None and callable(getattr(pos_obj, "is_flat", None)) and not pos_obj.is_flat():
                live = pos_obj
            else:
                live = pos_snapshot
        except Exception:
            live = pos_snapshot

        def _get(key: str, default: Any = 0.0) -> Any:
            try:
                if isinstance(live, dict):
                    return live.get(key, default)
                return getattr(live, key, default)
            except Exception:
                return default

        try:
            side = str(_get("side", "") or "").upper()
            entry = float(_get("entry_price", 0.0) or 0.0)
            qty = float(_get("quantity", 0.0) or 0.0)
            sl_price = float(_get("sl_price", 0.0) or 0.0)
            tp_price = float(_get("tp_price", 0.0) or 0.0)
            move_pts = (px - entry) if side == "LONG" else (entry - px)

            # Prefer the strategy's authoritative unrealised P&L calculator so
            # single-asset and portfolio Telegram reports cannot diverge.
            upnl = 0.0
            calc = getattr(ctx.strategy, "_unrealised_pnl_usd", None)
            if callable(calc):
                upnl = float(calc(px, live) or 0.0)
            else:
                inv = (
                    inst.primary_exchange == ExchangeName.DELTA
                    and str(inst.execution_symbol).upper() == "BTCUSD"
                )
                upnl = gross_pnl_usd(side, entry, px, qty, inverse=bool(inv))

            init_dist = float(_get("initial_sl_dist", 0.0) or 0.0) or abs(entry - sl_price)
            r_now = move_pts / init_dist if init_dist > 1e-10 else 0.0
            mfe_r = float(_get("peak_profit", 0.0) or 0.0) / init_dist if init_dist > 1e-10 else 0.0
            entry_time = float(_get("entry_time", time.time()) or time.time())
            hold_m = (time.time() - entry_time) / 60.0
            open_realized = float(_get("tp_ladder_realized_pnl", 0.0) or 0.0)
            entry_lev = float(_get("entry_leverage", 0.0) or getattr(ctx.strategy, "_active_effective_leverage", 0.0) or pol.leverage or 0.0)
            margin_used = (entry * qty / entry_lev) if entry > 0 and qty > 0 and entry_lev > 0 else 0.0
            out.update({
                "position": live,
                "side": side, "entry": entry, "qty": qty,
                "upnl": upnl, "unrealised_pnl": upnl, "unrealized_pnl": upnl,
                "open_realized": open_realized,
                "lifecycle_pnl": upnl + open_realized,
                "r": r_now, "mfe_r": mfe_r, "hold_min": hold_m,
                "sl": sl_price, "tp": tp_price,
                "entry_leverage": entry_lev, "leverage": entry_lev, "margin_used": margin_used,
                "entry_id": _get("entry_order_id", ""),
                "sl_id": _get("sl_order_id", ""),
                "tp_id": _get("tp_order_id", ""),
            })
        except Exception:
            pass
        return out

    @staticmethod
    def _float_val(v: Any, default: float = 0.0) -> float:
        try:
            f = float(v)
            return f if f == f else float(default)
        except Exception:
            return float(default)

    @staticmethod
    def _clip(value: Any, width: int) -> str:
        text = str(value or "")
        if len(text) <= width:
            return text
        return text[: max(0, width - 1)] + "~"

    @classmethod
    def _trade_ts(cls, t: Dict[str, Any]) -> float:
        return cls._float_val(t.get("timestamp"), 0.0)

    @classmethod
    def _trade_net(cls, t: Dict[str, Any]) -> float:
        return cls._float_val(t.get("pnl"), 0.0)

    @classmethod
    def _trade_fees(cls, t: Dict[str, Any]) -> float:
        if "total_fees" in t:
            return abs(cls._float_val(t.get("total_fees"), 0.0))
        return abs(cls._float_val(t.get("entry_fee"), 0.0) + cls._float_val(t.get("exit_fee"), 0.0))

    @classmethod
    def _trade_gross(cls, t: Dict[str, Any]) -> float:
        if "gross_pnl" in t:
            return cls._float_val(t.get("gross_pnl"), cls._trade_net(t))
        return cls._trade_net(t) + cls._trade_fees(t)

    @classmethod
    def _trade_r(cls, t: Dict[str, Any]) -> float:
        return cls._float_val(t.get("r", t.get("achieved_r", 0.0)), 0.0)

    @staticmethod
    def _blank_pnl_bucket() -> Dict[str, Any]:
        return {
            "trades": 0, "wins": 0, "losses": 0, "net": 0.0, "gross": 0.0,
            "fees": 0.0, "upnl": 0.0, "open_realized": 0.0,
            "live": 0.0, "open": 0, "win_sum": 0.0,
            "loss_sum": 0.0, "r_sum": 0.0,
        }

    def _add_trade_to_bucket(self, bucket: Dict[str, Any], trade: Dict[str, Any]) -> None:
        net = self._trade_net(trade)
        bucket["trades"] += 1
        bucket["net"] += net
        bucket["gross"] += self._trade_gross(trade)
        bucket["fees"] += self._trade_fees(trade)
        bucket["r_sum"] += self._trade_r(trade)
        if net > 0:
            bucket["wins"] += 1
            bucket["win_sum"] += net
        elif net < 0:
            bucket["losses"] += 1
            bucket["loss_sum"] += net

    @staticmethod
    def _bucket_wr(bucket: Dict[str, Any]) -> float:
        trades = int(bucket.get("trades", 0) or 0)
        return (float(bucket.get("wins", 0) or 0) / trades * 100.0) if trades else 0.0

    @staticmethod
    def _bucket_avg_win(bucket: Dict[str, Any]) -> float:
        wins = int(bucket.get("wins", 0) or 0)
        return (float(bucket.get("win_sum", 0.0) or 0.0) / wins) if wins else 0.0

    @staticmethod
    def _bucket_avg_loss(bucket: Dict[str, Any]) -> float:
        losses = int(bucket.get("losses", 0) or 0)
        return (float(bucket.get("loss_sum", 0.0) or 0.0) / losses) if losses else 0.0

    @staticmethod
    def _fmt_trade_time(timestamp: float) -> str:
        try:
            return time.strftime("%m-%d %H:%M", time.localtime(float(timestamp)))
        except Exception:
            return "--"

    def _all_trade_records(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for ctx in self.contexts:
            try:
                pol = active_policy(ctx.instrument)
                for t in list(getattr(ctx.strategy, "_trade_history", [])):
                    r = dict(t)
                    r.setdefault("asset", ctx.instrument.asset_id)
                    r.setdefault("symbol", ctx.instrument.display_symbol)
                    r.setdefault("venue", ctx.instrument.primary_exchange.value.upper())
                    r.setdefault("desk", pol.desk_id)
                    r.setdefault("desk_name", pol.desk_name)
                    r.setdefault("asset_class", pol.asset_class)
                    r.setdefault("currency", self._currency_for_instrument(ctx.instrument))
                    r["total_fees"] = self._trade_fees(r)
                    r["gross_pnl"] = self._trade_gross(r)
                    rows.append(r)
            except Exception:
                continue
        return rows

    def format_portfolio_pnl_report(self) -> str:
        rows = [self._ctx_position_metrics(c) for c in self.contexts]
        open_rows = [r for r in rows if r.get("position")]
        trades = self._all_trade_records()
        desk_cfg = getattr(config, "TRADING_DESKS", {}) or {}
        desk_order = [str(k).upper() for k in desk_cfg.keys()] or ["BTC", "COMMODITIES", "STOCKS"]
        desk_names = {str(k).upper(): str(v.get("display_name", k)) if isinstance(v, dict) else str(k) for k, v in desk_cfg.items()}
        desks: Dict[str, Dict[str, Any]] = {d: self._blank_pnl_bucket() for d in desk_order}
        assets: Dict[str, Dict[str, Any]] = {}
        totals: Dict[str, Dict[str, Any]] = {}
        def total(cur: str) -> Dict[str, Any]:
            cur = cur if cur in {"$", "₹"} else "$"
            totals.setdefault(cur, self._blank_pnl_bucket())
            return totals[cur]
        def money(value: float, *, signed: bool = False, currency: str = "$") -> str:
            try:
                value = float(value or 0.0)
                return f"{currency}{value:+,.2f}" if signed else f"{currency}{value:,.2f}"
            except Exception:
                return f"{currency}+0.00" if signed else f"{currency}0.00"
        for r in rows:
            desk = str(r.get("desk") or "BTC").upper(); cur = str(r.get("currency") or "$"); asset = str(r.get("asset") or "?").upper()
            if desk not in desks: desks[desk] = self._blank_pnl_bucket(); desk_order.append(desk)
            desks[desk].setdefault("currency", cur)
            if asset not in assets: assets[asset] = self._blank_pnl_bucket(); assets[asset].update({"desk": desk, "currency": cur})
            if r.get("position"):
                up = self._float_val(r.get("upnl"), 0.0); realised = self._float_val(r.get("open_realized"), 0.0); live = self._float_val(r.get("lifecycle_pnl"), up + realised)
                for b in (desks[desk], assets[asset], total(cur)):
                    b["upnl"] += up; b["open_realized"] += realised; b["live"] += live; b["open"] += 1
        for t in trades:
            desk = str(t.get("desk") or "BTC").upper(); cur = str(t.get("currency") or "$"); asset = str(t.get("asset") or "?").upper()
            if desk not in desks: desks[desk] = self._blank_pnl_bucket(); desk_order.append(desk)
            desks[desk].setdefault("currency", cur)
            if asset not in assets: assets[asset] = self._blank_pnl_bucket(); assets[asset].update({"desk": desk, "currency": cur})
            for b in (desks[desk], assets[asset], total(cur)): self._add_trade_to_bucket(b, t)
        used = [cur for cur, st in totals.items() if st.get("trades") or st.get("open")] or ["$"]
        ntrades = sum(int(st.get("trades", 0)) for st in totals.values()); wins = sum(int(st.get("wins", 0)) for st in totals.values())
        wr = wins / ntrades * 100.0 if ntrades else 0.0
        lines = ["🏛 <b>INSTITUTIONAL PORTFOLIO P&amp;L</b>", "<code>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━</code>"]
        for cur in used:
            b = totals.get(cur, self._blank_pnl_bucket()); code = "INR" if cur == "₹" else "USD"; icon = "🟢" if b["net"] + b["live"] >= 0 else "🔴"
            lines.append(f"{icon} <b>{code}</b> <code>NET {money(b['net'], signed=True, currency=cur):>12}  UPNL {money(b['upnl'], signed=True, currency=cur):>12}  LIVE {money(b['live'], signed=True, currency=cur):>12}</code>")
            lines.append(f"<code>   TOTAL {money(b['net'] + b['live'], signed=True, currency=cur):>12}  GROSS {money(b['gross'], signed=True, currency=cur):>12}  FEES {money(b['fees'], currency=cur):>12}</code>")
        lines += [f"<code>TRADES {ntrades:>4}  WR {wr:>5.1f}%  OPEN {len(open_rows):>2}/{self.guard.max_open_positions:<2}</code>", f"<code>BUDGET {self._esc(self.guard.budget_mode)}</code>", "\n<b>🏛 Desk PnL / P&amp;L</b>"]
        for desk in desk_order:
            b = desks.get(desk) or self._blank_pnl_bucket(); cur = str(b.get("currency") or "$"); name = self._clip(desk_names.get(desk, desk), 13); total_desk = b["net"] + b["live"]
            lines.append(f"<code>{self._esc(name):<13} net {money(b['net'], signed=True, currency=cur):>10} upnl {money(b['upnl'], signed=True, currency=cur):>10} live {money(b['live'], signed=True, currency=cur):>10} tot {money(total_desk, signed=True, currency=cur):>10} T {int(b['trades']):>3} WR {self._bucket_wr(b):>5.1f}%</code>")
        lines.append("\n<b>📦 Asset PnL / P&amp;L</b>")
        for asset, b in sorted(assets.items(), key=lambda kv: (str(kv[1].get("desk", "")), kv[0])):
            cur = str(b.get("currency") or "$"); desk = self._clip(b.get("desk", ""), 5); total_asset = b["net"] + b["live"]
            lines.append(f"<code>{self._esc(self._clip(asset,8)):<8} {self._esc(desk):<5} net {money(b['net'], signed=True, currency=cur):>10} upnl {money(b['upnl'], signed=True, currency=cur):>10} live {money(b['live'], signed=True, currency=cur):>10} tot {money(total_asset, signed=True, currency=cur):>10} T {int(b['trades']):>3} WR {self._bucket_wr(b):>5.1f}%</code>")
        if open_rows:
            lines.append("\n<b>📡 Open Positions</b>")
            for r in sorted(open_rows, key=lambda x: (str(x.get("desk")), str(x.get("asset")))):
                cur = str(r.get("currency") or "$")
                lines.append(f"<code>{self._esc(r['desk']):<5} {self._esc(r['asset']):<8} {self._esc(r['side']):<5} UPNL {money(r['upnl'], signed=True, currency=cur):>10} LIVE {money(r.get('lifecycle_pnl', r['upnl']), signed=True, currency=cur):>10} R {float(r['r']):+5.2f} MFE {float(r['mfe_r']):>4.2f}</code>")
                lines.append(f"<code>       lev {float(r.get('entry_leverage', 0.0) or 0.0):>4.0f}x margin {money(r.get('margin_used', 0.0), currency=cur):>10} px {money(r.get('price', 0.0), currency=cur):>12}</code>")
                lines.append(f"<code>       entry {money(r.get('entry', 0.0), currency=cur):>12} SL {money(r.get('sl', 0.0), currency=cur):>12}</code>")
        if trades:
            lines.append("\n<b>🧾 Recent Realised Trades</b>")
            for t in sorted(trades, key=self._trade_ts, reverse=True)[:6]:
                pnl = self._trade_net(t); cur = str(t.get("currency") or "$"); ok = "✅ WIN" if pnl > 0 else ("❌ LOSS" if pnl < 0 else "➖ FLAT")
                lines.append(f"{ok:<4} <code>{self._esc(t.get('desk','?')):<5} {self._esc(t.get('asset','?')):<8} {self._esc(str(t.get('side','?')).upper()):<5} net {money(pnl, signed=True, currency=cur):>10} R {self._trade_r(t):+5.2f} {self._esc(str(t.get('reason',''))[:18]):<18}</code>")
        return "\n".join(lines)

    def format_portfolio_position_report(self) -> str:
        rows = [self._ctx_position_metrics(c) for c in self.contexts]
        lines = ["🏛 <b>INSTITUTIONAL POSITIONS</b>", "<code>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━</code>"]
        open_rows = [r for r in rows if r.get("position")]
        external_rows = [r for r in rows if r.get("external_position")]
        if not open_rows and not external_rows:
            lines.append("No live positions. 🛰 Scanner Desks remain active.")
        if external_rows:
            lines.append("\n<b>🚨 Broker Positions Not Adopted</b>")
            for r in external_rows:
                cur = str(r.get("currency") or "₹")
                vehicle = " ".join(str(x) for x in (r.get("option_symbol"), r.get("right"), r.get("strike"), r.get("expiry")) if x)
                reason = str(r.get("unadoptable_reason") or r.get("contract_identity_source") or "external broker position")
                lines.append(
                    f"<code>{self._esc(r['asset']):<6} {self._esc(r['venue'])}:{self._esc(r['symbol'])} {self._esc(r['side']):<5} qty {float(r['qty']):.6f}</code>"
                )
                lines.append(
                    f"<code>ENTRY {self._fmt_currency(r['entry'], currency=cur):>12}  UPNL {self._fmt_currency(r['upnl'], signed=True, currency=cur):>12}</code>"
                )
                if vehicle.strip():
                    lines.append(f"<code>VEHICLE {self._esc(vehicle[:72])}</code>")
                lines.append(f"<i>{self._esc(reason[:140])}; no order will be sent until adoption is exact.</i>")
        for r in open_rows:
            pol = r["policy"]
            cur = str(r.get("currency") or "$")
            lines.append(
                f"\n<b>{self._esc(r['asset'])}</b>  <code>{self._esc(r['venue'])}:{self._esc(r['symbol'])}</code> · {self._esc(pol.asset_class)}"
            )
            lines.append(
                f"<code>{self._esc(r['side']):<5} qty {float(r['qty']):.6f}   lev {float(r.get('entry_leverage', 0.0) or 0.0):.0f}x   R {float(r['r']):+.2f}   MFE {float(r['mfe_r']):.2f}   hold {float(r['hold_min']):.0f}m</code>"
            )
            lines.append(
                f"<code>MARGIN {self._fmt_currency(r.get('margin_used', 0.0), currency=cur):>11}  policy cap {float(getattr(pol, 'leverage', 0.0) or 0.0):.0f}x</code>"
            )
            lines.append(
                f"<code>ENTRY {self._fmt_currency(r['entry'], currency=cur):>12}  PX {self._fmt_currency(r['price'], currency=cur):>12}  UPNL {self._fmt_currency(r['upnl'], signed=True, currency=cur):>11}</code>"
            )
            lines.append(
                f"<code>LIVE  {self._fmt_currency(r.get('lifecycle_pnl', r['upnl']), signed=True, currency=cur):>12}  ladder {self._fmt_currency(r.get('open_realized', 0.0), signed=True, currency=cur):>10}</code>"
            )
            lines.append(
                f"<code>SL    {self._fmt_currency(r['sl'], currency=cur):>12}  FINAL TP {self._fmt_currency(r['tp'], currency=cur):>12}  LADDER</code>"
            )
            if r.get("sl_id") or r.get("tp_id"):
                lines.append(f"<code>BRKT  SL {str(r.get('sl_id') or '-')[:8]}…  TP {str(r.get('tp_id') or '-')[:8]}…</code>")
        lines.append("\n<b>🛰 Scanner Desks</b>")
        for r in rows:
            if r.get("position"):
                continue
            cur = str(r.get("currency") or "$")
            lines.append(f"<code>{self._esc(r['asset']):<6} {self._esc(r['symbol']):<12} {self._esc(r['state']):<10} px {self._fmt_currency(r['price'], currency=cur)}</code>")
        return "\n".join(lines)

    def format_portfolio_equity_report(self) -> str:
        """Currency-separated cash, marked equity and open lifecycle exposure."""
        lines = ["💼 <b>INSTITUTIONAL EQUITY / BUDGET</b>", "<code>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━</code>"]
        rows = [self._ctx_position_metrics(c) for c in self.contexts]
        ledgers: Dict[str, Dict[str, float]] = {}
        for ctx, r in zip(self.contexts, rows):
            cur = str(r.get("currency") or self._currency_for_instrument(ctx.instrument))
            ledger = ledgers.setdefault(cur, {"available": 0.0, "total": 0.0, "upnl": 0.0, "ladder": 0.0, "live": 0.0})
            try:
                bal = ctx.risk_manager.get_available_balance() or {}
                ledger["available"] += float(bal.get("available_raw", bal.get("available", 0.0)) or 0.0)
                ledger["total"] += float(bal.get("total_raw", bal.get("total", bal.get("available", 0.0))) or 0.0)
            except Exception:
                pass
            if r.get("position"):
                ledger["upnl"] += self._float_val(r.get("upnl"), 0.0)
                ledger["ladder"] += self._float_val(r.get("open_realized"), 0.0)
                ledger["live"] += self._float_val(r.get("lifecycle_pnl"), self._float_val(r.get("upnl"), 0.0))
        if not ledgers:
            ledgers["$"] = {"available": 0.0, "total": 0.0, "upnl": 0.0, "ladder": 0.0, "live": 0.0}
        for cur, ledger in sorted(ledgers.items(), key=lambda kv: kv[0]):
            code = "INR" if cur == "₹" else "USD"
            marked = ledger["total"] + ledger["live"]
            lines.append(f"<b>{code}</b> <code>cash {self._fmt_currency(ledger['available'], currency=cur):>12} total {self._fmt_currency(ledger['total'], currency=cur):>12}</code>")
            lines.append(f"<code>     marked {self._fmt_currency(marked, currency=cur):>12} live {self._fmt_currency(ledger['live'], signed=True, currency=cur):>12} upnl {self._fmt_currency(ledger['upnl'], signed=True, currency=cur):>12}</code>")
        lines.append(f"<code>SLOTS   used {self.guard.count_open(self.contexts):>2}/{self.guard.max_open_positions:<2}  mode {self._esc(self.guard.budget_mode)}</code>")
        for ctx, r in zip(self.contexts, rows):
            try:
                bal = ctx.risk_manager.get_available_balance() or {}
                pol = active_policy(ctx.instrument)
                cur = str(r.get("currency") or self._currency_for_instrument(ctx.instrument))
                lev_display = float(r.get("entry_leverage", 0.0) or pol.leverage or 0.0) if r.get("position") else float(pol.leverage or 0.0)
                pos_tail = ""
                if r.get("position"):
                    pos_tail = (
                        f" margin {self._fmt_currency(r.get('margin_used', 0.0), currency=cur)}"
                        f" uPnL {self._fmt_currency(r.get('upnl', 0.0), signed=True, currency=cur)}"
                        f" live {self._fmt_currency(r.get('lifecycle_pnl', r.get('upnl', 0.0)), signed=True, currency=cur)}"
                    )
                lines.append(
                    f"<code>{self._esc(ctx.instrument.asset_id):<6} cash {self._fmt_currency(float(bal.get('available',0) or 0), currency=cur):>10} "
                    f"riskbase {self._fmt_currency(float(bal.get('risk_total',0) or 0), currency=cur):>10} lev {lev_display:>2.0f}x margin {pol.margin_pct:.0%} risk×{pol.risk_multiplier:.2f}{self._esc(pos_tail)}</code>"
                )
            except Exception:
                continue
        return "\n".join(lines)

    def format_portfolio_trades_report(self) -> str:
        trades = self._all_trade_records()
        lines = ["🧾 <b>INSTITUTIONAL TRADE TAPE</b>", "<code>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━</code>"]
        if not trades:
            return "\n".join(lines + ["No closed trades recorded yet."])
        limit = max(1, int(getattr(config, "TELEGRAM_RECENT_TRADES_LIMIT", 30)))
        ordered = sorted(trades, key=self._trade_ts, reverse=True)
        lines.append(f"<code>Showing {min(limit, len(ordered))}/{len(ordered)} closed trades, newest first</code>")
        for t in ordered[:limit]:
            pnl = self._trade_net(t)
            gross = self._trade_gross(t)
            fees = self._trade_fees(t)
            ok = "✅ WIN " if pnl > 0 else ("❌ LOSS" if pnl < 0 else "➖ FLAT")
            side = str(t.get("side", "?")).upper()
            desk = self._clip(t.get("desk", "?"), 5)
            asset = self._clip(t.get("asset", "?"), 8)
            cur = str(t.get("currency") or "$")
            lines.append(
                f"{ok} <code>{self._fmt_trade_time(self._trade_ts(t)):<11} {self._esc(desk):<5} "
                f"{self._esc(asset):<8} {self._esc(side):<5} "
                f"{self._fmt_currency(float(t.get('entry',0) or 0), currency=cur):>10}->{self._fmt_currency(float(t.get('exit',0) or 0), currency=cur):>10} "
                f"net {self._fmt_currency(pnl, signed=True, currency=cur):>10} R {self._trade_r(t):+5.2f}</code>"
            )
            lines.append(
                f"    <code>gross {self._fmt_currency(gross, signed=True, currency=cur):>10} fees {self._fmt_currency(fees, currency=cur):>10} "
                f"hold {self._float_val(t.get('hold_min'), 0.0):>5.1f}m</code>"
            )
            lines.append(f"    <i>{self._esc(str(t.get('reason',''))[:96])}</i>")
        return "\n".join(lines)

    def _filter_suspended_requests(self, requested):
        """Remove suspended desk instruments before catalog discovery.

        This is a hard desk suspension at orchestration level. Suspended assets
        never get a data manager, strategy context, risk manager, or router, so
        they cannot trade by accident.
        """
        req = list(requested or [])
        disabled_classes = {str(x).lower() for x in getattr(config, "SUSPENDED_ASSET_CLASSES", ())}
        if bool(getattr(config, "STOCK_DESK_TRADING_ENABLED", True)):
            disabled_classes.discard("equity")
            disabled_classes.discard("index")
        out = []
        skipped = []
        for r in req:
            ac = str((r or {}).get("asset_class", "") or "").lower()
            aid = str((r or {}).get("asset_id", "") or "")
            if ac in disabled_classes:
                skipped.append(aid or ac)
                continue
            out.append(r)
        if skipped:
            logger.warning(
                "⏸ STOCK DESK SUSPENDED — excluded from trading universe before discovery: %s",
                ", ".join(skipped),
            )
        return out

    def initialize(self) -> bool:
        try:
            logger.info("=" * 92)
            logger.info("⚡ MULTI-ASSET INSTITUTIONAL LIQUIDITY SCANNER")
            logger.info("   Live exchange catalogs only — stock desk suspended; no synthetic feeds")
            logger.info("=" * 92)
            delta_api, cs_api, icici_api = self._build_api_clients()
            self.registry = InstrumentRegistry(execution_preference=getattr(config, "EXECUTION_EXCHANGE", "delta"))
            requested = self._filter_suspended_requests(getattr(config, "MULTI_ASSET_REQUESTS", None))
            self.discovery_report = self.registry.discover(
                delta_api=delta_api,
                coinswitch_api=cs_api,
                icici_api=icici_api,
                include_exchanges=getattr(config, "UNIVERSE_INCLUDE_EXCHANGES", "delta,coinswitch"),
                icici_security_master_url=getattr(config, "ICICI_SECURITY_MASTER_URL", None),
                requested=requested,
                max_active=int(getattr(config, "SCANNER_MAX_ACTIVE_INSTRUMENTS", 8)),
                require_primary=False,
            )
            for line in self.discovery_report.terminal_lines():
                logger.info(line)
            if not self.discovery_report.matched:
                logger.error("No confirmed tradable instruments found. Scanner will not start.")
                return False

            for inst in self.discovery_report.matched:
                ctx = self._build_asset_context(inst, delta_api, cs_api, icici_api)
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

    def _build_asset_context(self, inst: TradableInstrument, delta_api, cs_api, icici_api=None) -> Optional[AssetContext]:
        primary_ex = inst.primary_exchange
        cs_om = None
        delta_om = None
        icici_om = None
        if ExchangeName.COINSWITCH in inst.by_exchange and cs_api is not None:
            cs_om = OrderManager(cs_api, exchange_name="coinswitch", instrument=inst)
        if ExchangeName.DELTA in inst.by_exchange and delta_api is not None:
            delta_om = OrderManager(delta_api, exchange_name="delta", instrument=inst)
        if ExchangeName.ICICI in inst.by_exchange and icici_api is not None:
            icici_om = OrderManager(icici_api, exchange_name="icici", instrument=inst)
        if not cs_om and not delta_om and not icici_om:
            logger.warning("%s skipped: no executable order manager", inst.asset_id)
            return None
        router = ExecutionRouter(coinswitch_om=cs_om, delta_om=delta_om, icici_om=icici_om, default=primary_ex.value)

        if primary_ex == ExchangeName.DELTA:
            primary_dm = DeltaDataManager(instrument=inst)
            secondary_dm = CoinSwitchDataManager(instrument=inst) if ExchangeName.COINSWITCH in inst.by_exchange and cs_api else None
            analysis_dm = None
        elif primary_ex == ExchangeName.ICICI:
            if ICICIOptionDataManager is None or ICICIUnderlyingDataManager is None or icici_api is None:
                logger.warning("%s skipped: ICICI data managers unavailable", inst.asset_id)
                return None
            primary_dm = ICICIOptionDataManager(instrument=inst, api=icici_api)
            secondary_dm = None
            analysis_dm = ICICIUnderlyingDataManager(instrument=inst, api=icici_api)
        else:
            primary_dm = CoinSwitchDataManager(instrument=inst)
            secondary_dm = DeltaDataManager(instrument=inst) if ExchangeName.DELTA in inst.by_exchange and delta_api else None
            analysis_dm = None
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

    def _start_one_context(self, ctx: AssetContext) -> bool:
        inst = ctx.instrument
        try:
            with instrument_scope(inst):
                if self._is_icici_context(ctx):
                    session_open, session_reason = self._icici_market_open()
                    if not session_open:
                        ctx.ready = False
                        logger.warning(
                            "%s ICICI desk dormant: %s. No NIFTY analysis, entries or adoption outside NSE/NFO hours.",
                            inst.asset_id,
                            session_reason,
                        )
                        return False
                logger.info("▶️ Starting %s [%s/%s] | %s", inst.asset_id, inst.primary_exchange.value, inst.display_symbol, self.guard.report_line(ctx))
                target_lev = self._instrument_leverage(inst)
                effective_lev = self._set_leverage_with_backoff(ctx, target_lev)
                max_txt = f" (cap={inst.max_leverage:g}x)" if getattr(inst, "max_leverage", 0.0) else ""
                logger.info("%s leverage target=%sx effective=%sx%s", inst.asset_id, target_lev, effective_lev, max_txt)
                if self._is_icici_context(ctx) and not self._icici_account_preflight(ctx):
                    ctx.ready = False
                    logger.error("%s ICICI desk disabled: F&O account verification did not pass", inst.asset_id)
                    return False
                if not ctx.data_manager.start():
                    logger.error("%s data stream start failed", inst.asset_id)
                    return False
                ready = ctx.data_manager.wait_until_ready(timeout_sec=float(getattr(config, "READY_TIMEOUT_SEC", 180)))
                ctx.ready = bool(ready)
                if not ready:
                    logger.error("%s data manager not ready", inst.asset_id)
                    return False
                if self._is_icici_context(ctx):
                    balance = ctx.risk_manager.get_available_balance() or {}
                    preparer = getattr(ctx.data_manager, "prepare_icici_session_contract_book", None)
                    if not callable(preparer) or not preparer(float(balance.get("available", 0.0) or 0.0)):
                        ctx.ready = False
                        logger.error("%s ICICI desk disabled: session CE/PE execution contract book could not be verified", inst.asset_id)
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
        lines = ["🏛 <b>Portfolio Command Center Online</b>", "━━━━━━━━━━━━━━━━━━━━"]
        lines.append("🧠 <b>Execution Universe</b>")
        for ctx in self.contexts:
            inst = ctx.instrument
            venues = ", ".join(f"{ex.value.upper()}:{ei.display_symbol}" for ex, ei in inst.by_exchange.items())
            lev = self._instrument_leverage(inst)
            pol = active_policy(inst)
            cadence = getattr(pol, "loop_interval_sec", getattr(pol, "tick_eval_sec", 0.0))
            lines.append(
                f"🟢 <b>{inst.asset_id}</b>  <code>{inst.primary_exchange.value.upper()}:{inst.display_symbol}</code>\n"
                f"   <code>lev {lev}x · risk×{pol.risk_multiplier:.2f} · margin {pol.margin_pct:.0%} · cadence {float(cadence):.2f}s</code>\n"
                f"   <code>{venues}</code>"
            )
        if not bool(getattr(config, "STOCK_DESK_TRADING_ENABLED", True)):
            lines.append("\n⏸ <b>Stock desk suspended</b> — equity/index contexts are not created and cannot route orders.")
        lines.extend([
            "\n🛡️ <b>Portfolio Rules</b>",
            f"• Slots: <code>{self.guard.max_open_positions}</code> portfolio positions, <code>{self.guard.max_per_contract}</code> per contract",
            f"• Balance allocation: <code>{self.guard.budget_mode}</code>; risk base <code>{self.guard.risk_budget_mode}</code>",
            "• Live exchange products only; no synthetic executable symbols",
            "• Alpha: single ICT + Liquidity authority; portfolio manager controls exposure mechanics only",
            "• Entry: 4H context → 15m confirmation → fresh 5m raid/MSS/FVG",
            "• Exits and P&amp;L: venue protection and exact-fill reconciliation",
        ])
        return "\n".join(lines)


    def run(self) -> None:
        logger.info("📊 Multi-asset loop active")
        while self.running:
            try:
                now_ms = int(time.time() * 1000)
                for ctx in list(self.contexts):
                    if not ctx.ready:
                        continue
                    if self._is_icici_context(ctx):
                        session_open, session_reason = self._icici_market_open()
                        if not session_open:
                            ctx.ready = False
                            self._log_throttled_asset(
                                ctx,
                                f"ICICI market closed: {session_reason}; NIFTY desk dormant, no analysis/entries.",
                            )
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
                logger.warning(
                    "KeyboardInterrupt ignored by Telegram-only shutdown guard; "
                    "use /stop from Telegram to stop the bot."
                )
                continue
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
        contract has no approved structural thesis or protected position to report.
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
        from runtime_shutdown_guard import install_telegram_only_shutdown_guard

        install_telegram_only_shutdown_guard(logger, "multi-asset-main")
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

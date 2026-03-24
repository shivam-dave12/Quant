"""
Telegram Bot Controller v13 — For Predictive Quant Engine v2
=============================================================
All command handlers rewritten for the new QuantStrategy interface.
No references to old quant_strategy.py internals.
"""

import logging, time, threading, requests, html as _html
from typing import Optional
from datetime import datetime, timezone
import sys, os as _os
sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
import telegram.config as telegram_config
import config
from telegram.notifier import _sanitize_html

logger = logging.getLogger(__name__)

def _esc(s) -> str:
    if s is None: return ""
    return _html.escape(str(s), quote=False)

bot_instance = None
bot_thread   = None
bot_running  = False


class TelegramBotController:
    def __init__(self):
        self.bot_token      = telegram_config.TELEGRAM_BOT_TOKEN
        self.chat_id        = str(telegram_config.TELEGRAM_CHAT_ID)
        self.last_update_id = 0
        self.running        = False
        if not self.bot_token or not self.chat_id:
            raise ValueError("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be set")
        logger.info("TelegramBotController v13 initialized")

    def send_message(self, message: str, parse_mode: str = "HTML") -> bool:
        try:
            if len(message) > 4000:
                chunks = []
                while message:
                    if len(message) <= 4000: chunks.append(message); break
                    sp = message.rfind('\n', 0, 4000)
                    if sp == -1: sp = 4000
                    chunks.append(message[:sp]); message = message[sp+1:] if sp < len(message) else ""
                for c in chunks: self._send_raw(c, parse_mode); time.sleep(0.5)
                return True
            return self._send_raw(message, parse_mode)
        except Exception as e:
            logger.error(f"Send error: {e}")
            try: return self._send_raw(message, parse_mode=None)
            except: return False

    def _send_raw(self, text, parse_mode="HTML"):
        if parse_mode == "HTML": text = _sanitize_html(text)
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": text, "disable_web_page_preview": True}
        if parse_mode: payload["parse_mode"] = parse_mode
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code != 200: logger.error(f"Telegram {resp.status_code}: {resp.text[:200]}")
        return resp.status_code == 200

    def get_updates(self, timeout=30):
        _rt = min(timeout, 15) + 2
        try:
            url = f"https://api.telegram.org/bot{self.bot_token}/getUpdates"
            params = {"offset": self.last_update_id + 1, "timeout": min(timeout, 15), "allowed_updates": ["message"]}
            resp = requests.get(url, params=params, timeout=(5.0, _rt))
            if resp.status_code != 200: return []
            data = resp.json()
            return data.get("result", []) if data.get("ok") else []
        except requests.exceptions.Timeout: return []
        except Exception as e: logger.error(f"getUpdates error: {e}"); return []

    def clear_old_messages(self):
        try:
            updates = self.get_updates(timeout=1)
            if updates: self.last_update_id = updates[-1]["update_id"]
        except: pass

    def set_my_commands(self):
        try:
            url = f"https://api.telegram.org/bot{self.bot_token}/setMyCommands"
            commands = [
                {"command":"start","description":"Start trading bot"},
                {"command":"stop","description":"Stop trading bot"},
                {"command":"status","description":"Full status"},
                {"command":"thinking","description":"Live prediction breakdown"},
                {"command":"structures","description":"ICT structure map"},
                {"command":"position","description":"Current position"},
                {"command":"trades","description":"Trade history"},
                {"command":"balance","description":"Wallet balance"},
                {"command":"pause","description":"Pause trading"},
                {"command":"resume","description":"Resume trading"},
                {"command":"close","description":"Close active position"},
                {"command":"config","description":"Show config"},
                {"command":"killswitch","description":"Emergency close all"},
                {"command":"set","description":"Set config value"},
                {"command":"setexchange","description":"Switch exchange"},
                {"command":"help","description":"Show commands"},
            ]
            requests.post(url, json={"commands": commands}, timeout=10)
        except: pass

    def _normalize_command(self, text):
        t = (text or "").strip()
        bare = {"start","stop","status","thinking","structures","position","trades",
                "config","pause","resume","balance","close","killswitch","set","help"}
        if not t.startswith("/"):
            parts = t.split(None, 1); cmd = parts[0].lower(); args = parts[1] if len(parts)>1 else ""
            return (f"/{cmd}", args) if cmd in bare else (t, "")
        parts = t.split(None, 1)
        return parts[0].lower(), parts[1] if len(parts)>1 else ""

    def handle_command(self, raw_text):
        global bot_instance, bot_thread, bot_running
        cmd, args = self._normalize_command(raw_text)
        try:
            handlers = {
                "/help": self._cmd_help, "/start": self._cmd_start, "/stop": self._cmd_stop,
                "/status": self._cmd_status, "/thinking": self._cmd_thinking,
                "/structures": self._cmd_structures, "/position": self._cmd_position,
                "/trades": self._cmd_trades, "/balance": self._cmd_balance,
                "/pause": self._cmd_pause, "/resume": self._cmd_resume,
                "/close": self._cmd_close, "/config": self._cmd_config,
                "/killswitch": self._cmd_killswitch,
            }
            if cmd in handlers: return handlers[cmd]()
            if cmd == "/set": return self._cmd_set(args)
            if cmd == "/setexchange": return self._cmd_setexchange(args)
            return f"Unknown: {cmd}\n" + self._cmd_help()
        except Exception as e:
            logger.error(f"Command error [{cmd}]: {e}", exc_info=True)
            return f"❌ Error: {e}"

    # ════════════════════════════════════════════════════
    # COMMANDS
    # ════════════════════════════════════════════════════

    def _cmd_help(self):
        return (
            "<b>Predictive Quant Bot v2</b>\n\n"
            "/status — Full status + prediction\n"
            "/thinking — Live signal breakdown\n"
            "/structures — ICT structure map\n"
            "/position — Current position\n"
            "/trades — Trade history\n"
            "/balance — Wallet balance\n"
            "/close — Close active position\n"
            "/pause /resume — Pause/resume trading\n"
            "/killswitch — Emergency close all\n"
            "/config — Show config\n"
            "/set key value — Live-adjust config\n"
            "/setexchange delta|coinswitch — Switch exchange")

    def _cmd_start(self):
        global bot_instance, bot_thread, bot_running
        if bot_running: return "Bot already running. /stop first."
        bot_thread = threading.Thread(target=self._run_bot_thread, daemon=True, name="quant-bot")
        bot_thread.start()
        return "🚀 Starting Predictive Quant Bot v2..."

    def _cmd_stop(self):
        global bot_instance, bot_running
        if not bot_running: return "Bot not running."
        if bot_instance: bot_instance.stop()
        bot_running = False
        return "🛑 Bot stopped."

    def _cmd_status(self):
        global bot_instance, bot_running
        if not bot_running or not bot_instance: return "Bot not running."
        strat = bot_instance.strategy
        if not strat: return "Strategy not ready."
        try:
            return strat.get_status_text()
        except Exception as e:
            return f"❌ Status error: {e}"

    def _cmd_thinking(self):
        """Live prediction breakdown from the quant engine."""
        global bot_instance, bot_running
        if not bot_running or not bot_instance: return "Bot not running."
        strat = bot_instance.strategy
        dm = bot_instance.data_manager
        if not strat or not dm: return "Not ready."

        try:
            price = dm.get_last_price()
            atr = strat._atr_5m.atr
            atr_pct = strat._atr_5m.get_percentile()
            adx_val = strat._adx.adx

            # Get prediction
            pred = strat._quant.last_prediction
            s = pred.signals
            posteriors = strat._quant.get_regime_posteriors()

            lines = [f"<b>🧠 THINKING @ ${price:,.2f}</b>"]

            # Market context
            lines.append("\n<b>━ Market Context</b>")
            lines.append(f"  ATR(5m): ${atr:.1f} ({atr_pct:.0%} pctile) ADX: {adx_val:.1f}")
            lines.append(f"  Vol regime: {strat._quant._vol.state.regime} "
                         f"ratio={strat._quant._vol.state.realized_ratio:.2f}")

            # Prediction
            lines.append("\n<b>━ Prediction</b>")
            lines.append(f"  Direction: <b>{pred.direction.upper()}</b>  "
                         f"Confidence: <b>{pred.confidence:.3f}</b>")
            lines.append(f"  Alpha: {pred.alpha_bps:+.1f}bps  Urgency: {pred.urgency:.2f}")
            lines.append(f"  Regime: {pred.regime} (conf={strat._quant._regime.confidence:.2f})")

            # Component signals
            lines.append("\n<b>━ Component Signals</b>")
            def sline(label, val):
                arrow = "▲" if val > 0.05 else ("▼" if val < -0.05 else "─")
                return f"  {label:<10} {arrow} {val:+.3f}"
            lines.append(sline("VPIN dir", s.get("vpin_dir", 0)))
            lines.append(sline("OB press", s.get("ob_comp", 0)))
            lines.append(sline("Kyle λ", s.get("kyle_sig", 0)))
            lines.append(sline("Arrival", s.get("arrival", 0)))
            lines.append(sline("Mom align", s.get("mom_align", 0)))
            lines.append(sline("Mom lead", s.get("mom_lead", 0)))
            lines.append(f"  {'─'*30}")
            lines.append(f"  Raw signal: {s.get('raw', 0):+.4f}")

            # OB microstructure detail
            lines.append("\n<b>━ Orderbook Detail</b>")
            lines.append(f"  Imbalance: {s.get('ob_imb',0):+.3f}  "
                         f"Velocity: {s.get('ob_vel',0):+.3f}")
            lines.append(f"  Microprice: {s.get('ob_micro',0):+.3f}  "
                         f"Depth: {s.get('ob_depth',0):+.3f}  "
                         f"Wall: {s.get('ob_wall',0):+.3f}")

            # Regime posteriors
            lines.append("\n<b>━ Regime Posteriors</b>")
            for r, p in sorted(posteriors.items(), key=lambda x: -x[1]):
                bar = "█" * int(p * 20) + "░" * (20 - int(p * 20))
                lines.append(f"  {r:<15} {bar} {p:.2f}")

            # Risk gate
            lines.append("\n<b>━ Risk &amp; Gates</b>")
            ok, reason = strat._risk_gate.allows_entry()
            cd = max(0, QCfg.COOLDOWN_SEC() - (time.time() - strat._last_exit_time))
            lines.append(f"  Risk: {'✅' if ok else '🚫'} {reason}")
            lines.append(f"  Cooldown: {'ready' if cd <= 0 else f'{cd:.0f}s'}")
            lines.append(f"  Daily: {strat._risk_gate.daily_trades}/{QCfg.MAX_DAILY_TRADES()}")
            lines.append(f"  Warmed: {strat._quant.is_warmed} "
                         f"({strat._quant.warmup_status.get('total_trades',0)} trades)")

            # Entry readiness
            min_conf = QCfg.MIN_CONFIDENCE()
            if pred.is_actionable(min_conf) and ok and cd <= 0:
                lines.append(f"\n  🎯 <b>ENTRY READY — {pred.direction.upper()}</b>")
            else:
                blockers = []
                if not pred.is_actionable(min_conf):
                    blockers.append(f"conf {pred.confidence:.3f} &lt; {min_conf}")
                if not ok: blockers.append(reason)
                if cd > 0: blockers.append(f"cooldown {cd:.0f}s")
                lines.append(f"\n  👀 <b>Watching</b> — " + ", ".join(blockers))

            self.send_message("\n".join(lines))
            return None
        except Exception as e:
            logger.error(f"Thinking error: {e}", exc_info=True)
            return f"❌ {e}"

    def _cmd_structures(self):
        """ICT structure map — ICT engine is still available for viewing."""
        global bot_instance, bot_running
        if not bot_running or not bot_instance: return "Bot not running."
        strat = bot_instance.strategy
        dm = bot_instance.data_manager
        if not strat or not dm: return "Not ready."

        ict = getattr(strat, '_ict', None)
        if ict is None: return "❌ ICT engine not loaded."
        if not getattr(ict, '_initialized', False):
            return "⏳ ICT engine warming up..."

        try:
            price = dm.get_last_price()
            atr = strat._atr_5m.atr if strat._atr_5m.atr > 1e-10 else price * 0.002
            now_ms = int(time.time() * 1000)
            st = ict.get_full_status(price, atr, now_ms)
            c = st["counts"]

            lines = [
                f"🏛️ <b>ICT Structures @ ${price:,.1f}</b>",
                f"ATR: ${atr:.1f} | OBs: {c['ob_bull']}🟢 {c['ob_bear']}🔴 | "
                f"FVGs: {c['fvg_bull']}🟦 {c['fvg_bear']}🟥 | "
                f"Liq: {c['liq_active']} active", ""]

            # Bull OBs
            if st["bull_obs"]:
                lines.append("<b>🟢 Bullish OBs</b>")
                for ob in st["bull_obs"][:5]:
                    lines.append(f"  ${ob['low']:,.0f}–${ob['high']:,.0f} "
                                 f"str={ob['strength']:.0f} {ob['dist_pts']:+.0f}pts "
                                 f"{'BOS✓' if ob['bos'] else ''}")
            # Bear OBs
            if st["bear_obs"]:
                lines.append("<b>🔴 Bearish OBs</b>")
                for ob in st["bear_obs"][:5]:
                    lines.append(f"  ${ob['low']:,.0f}–${ob['high']:,.0f} "
                                 f"str={ob['strength']:.0f} {ob['dist_pts']:+.0f}pts "
                                 f"{'BOS✓' if ob['bos'] else ''}")
            # FVGs
            fvgs = st["bull_fvgs"][:3] + st["bear_fvgs"][:3]
            if fvgs:
                lines.append("\n<b>FVGs</b>")
                for f in sorted(fvgs, key=lambda x: abs(x["dist_pts"])):
                    icon = "🟦" if f["direction"]=="bullish" else "🟥"
                    lines.append(f"  {icon} ${f['bottom']:,.0f}–${f['top']:,.0f} "
                                 f"fill={f['fill_pct']:.0%} {f['dist_pts']:+.0f}pts")
            # Liquidity
            nearby = [l for l in st["liq_active"] if abs(l["dist_pts"]) < 5*atr]
            if nearby:
                lines.append("\n<b>💧 Liquidity</b>")
                for l in nearby[:6]:
                    lines.append(f"  {l['pool_type']} ${l['price']:,.0f} x{l['touch_count']} "
                                 f"{l['dist_pts']:+.0f}pts")
            # Confluence
            long_c = ict.get_confluence("long", price, now_ms)
            short_c = ict.get_confluence("short", price, now_ms)
            lines.append(f"\n<b>Confluence</b>")
            lines.append(f"  LONG  Σ={long_c.total:.2f}")
            lines.append(f"  SHORT Σ={short_c.total:.2f}")

            self.send_message("\n".join(lines))
            return None
        except Exception as e:
            return f"❌ Structures error: {e}"

    def _cmd_position(self):
        global bot_instance, bot_running
        if not bot_running or not bot_instance: return "Bot not running."
        strat = bot_instance.strategy
        dm = bot_instance.data_manager
        if not strat: return "Strategy not ready."
        pos = strat.get_position()
        if not pos: return "📭 No active position."

        p = strat._pos; price = dm.get_last_price() if dm else 0.0
        atr = strat._atr_5m.atr
        side = p.side.upper(); entry = p.entry_price; sl = p.sl_price; tp = p.tp_price
        init_d = p.initial_sl_dist if p.initial_sl_dist > 1e-10 else abs(entry-sl)
        cr = ((price-entry)/init_d if side=="LONG" else (entry-price)/init_d) if init_d>0 else 0
        rr = abs(tp-entry)/init_d if init_d>0 and tp>0 else 0
        upnl = (price-entry)*p.quantity if side=="LONG" else (entry-price)*p.quantity
        mfe_r = p.peak_profit/init_d if init_d>0 else 0
        hm = (time.time()-p.entry_time)/60 if p.entry_time>0 else 0

        return (f"<b>Active — {side}</b>\n\n"
                f"Entry: ${entry:,.2f}\nNow: ${price:,.2f} ({cr:+.2f}R)\n"
                f"uPnL: ${upnl:+,.2f}\nQty: {p.quantity:.4f}\n\n"
                f"SL: ${sl:,.2f}  TP: ${tp:,.2f}\nR:R: 1:{rr:.2f}  MFE: {mfe_r:.2f}R\n\n"
                f"Hold: {hm:.1f}m  Trail: {'✅' if p.trail_active else '⏳'}")

    def _cmd_trades(self):
        global bot_instance, bot_running
        if not bot_running or not bot_instance: return "Bot not running."
        strat = bot_instance.strategy
        if not strat: return "Not ready."
        history = getattr(strat, '_trade_history', [])
        if not history: return "No trades yet."
        lines = ["<b>📋 Trades</b>\n"]
        for t in reversed(history[-10:]):
            icon = "✅" if t.get("is_win") else "❌"
            lines.append(f"  {icon} {t.get('side','?').upper()} ${t.get('entry',0):,.0f}→"
                         f"${t.get('exit',0):,.0f} PnL=${t.get('pnl',0):+.2f} "
                         f"R={t.get('achieved_r',0):+.2f}R ({t.get('reason','?')})")
        stats = strat.get_stats()
        lines.append(f"\nSession: {stats['total_trades']}T WR={stats['win_rate']:.0%} "
                     f"PnL=${stats['total_pnl']:+.2f}")
        return "\n".join(lines)

    def _cmd_balance(self):
        global bot_instance, bot_running
        if not bot_running or not bot_instance: return "Bot not running."
        rm = bot_instance.risk_manager
        if not rm: return "Not ready."
        try:
            bal = rm.get_available_balance()
            if not bal: return "Could not fetch balance."
            avail = float(bal.get("available", 0)); total = float(bal.get("total", avail))
            return (f"<b>Wallet</b>\nAvailable: <b>${avail:,.2f}</b>\n"
                    f"Total: ${total:,.2f}")
        except Exception as e: return f"Error: {e}"

    def _cmd_close(self):
        global bot_instance, bot_running
        if not bot_running or not bot_instance: return "Bot not running."
        strat = bot_instance.strategy
        om = bot_instance.order_manager
        if not strat or not om: return "Not ready."
        ok, msg = strat.close_position(om, "telegram_close")
        return f"{'✅' if ok else '❌'} {msg}"

    def _cmd_pause(self):
        global bot_instance
        if not bot_instance: return "Bot not running."
        bot_instance.trading_enabled = False
        return "⏸️ Trading PAUSED."

    def _cmd_resume(self):
        global bot_instance
        if not bot_instance: return "Bot not running."
        bot_instance.trading_enabled = True
        return "▶️ Trading RESUMED."

    def _cmd_config(self):
        import config as cfg
        return (
            "<b>Config</b>\n\n"
            f"Symbol: {cfg.SYMBOL}\nExchange: {cfg.EXCHANGE}\n"
            f"Leverage: {cfg.LEVERAGE}x\n\n"
            "<b>Entry</b>\n"
            f"  Min confidence: {getattr(cfg,'QUANT_MIN_CONFIDENCE',0.55)}\n"
            f"  Confirm ticks: {getattr(cfg,'QUANT_CONFIRM_TICKS',3)}\n"
            f"  Cooldown: {getattr(cfg,'QUANT_COOLDOWN_SEC',180)}s\n"
            f"  Max spread/ATR: {getattr(cfg,'QUANT_SPREAD_ATR_MAX',0.50)}\n\n"
            "<b>Risk</b>\n"
            f"  Min R:R: {getattr(cfg,'MIN_RISK_REWARD_RATIO',1.5)}\n"
            f"  Max daily: {getattr(cfg,'MAX_DAILY_TRADES',8)}\n"
            f"  Max consec loss: {getattr(cfg,'MAX_CONSECUTIVE_LOSSES',3)}\n"
            f"  Loss lockout: {getattr(cfg,'QUANT_LOSS_LOCKOUT_SEC',3600)}s\n"
            f"  Margin/trade: {getattr(cfg,'QUANT_MARGIN_PCT',0.20):.0%}")

    def _cmd_killswitch(self):
        global bot_instance
        if not bot_instance: return "Bot not running."
        try:
            bot_instance.trading_enabled = False
            om = bot_instance.order_manager
            results = []
            if not om: return "❌ No order manager."
            try:
                swept = om.cancel_symbol_conditionals(symbol=config.SYMBOL)
                results.append(f"✅ Swept {len(swept) if swept else 0} orders")
            except Exception as e: results.append(f"⚠️ Cancel: {e}")
            try:
                pos = om.get_open_position()
                if pos and float(pos.get("size", 0)) > 0:
                    side = str(pos.get("side","")).upper()
                    cs = "SELL" if side=="LONG" else "BUY"
                    om.place_market_order(side=cs, quantity=float(pos["size"]), reduce_only=True)
                    results.append(f"✅ Closed {side}")
                else: results.append("ℹ️ No position")
            except Exception as e: results.append(f"⚠️ Close: {e}")
            strat = bot_instance.strategy
            if strat:
                try:
                    from strategy.quant_integration import PositionState
                    with strat._lock:
                        strat._pos = PositionState()
                        strat._confirm_long = strat._confirm_short = 0
                    results.append("✅ Strategy reset")
                except Exception as e: results.append(f"⚠️ Reset: {e}")
            return "🚨 <b>KILLSWITCH</b>\n\n" + "\n".join(f"  {r}" for r in results) + "\n\nTrading PAUSED."
        except Exception as e: return f"❌ Killswitch error: {e}"

    def _cmd_set(self, args):
        if not args: return "Usage: /set key value"
        parts = args.split(None, 1)
        if len(parts) < 2: return "Usage: /set key value"
        key, val_str = parts[0].lower(), parts[1].strip()
        import config as cfg
        allowed = {
            "cooldown":("QUANT_COOLDOWN_SEC",int), "confidence":("QUANT_MIN_CONFIDENCE",float),
            "confirm_ticks":("QUANT_CONFIRM_TICKS",int), "margin_pct":("QUANT_MARGIN_PCT",float),
            "min_rr":("MIN_RISK_REWARD_RATIO",float), "max_daily":("MAX_DAILY_TRADES",int),
            "spread_max":("QUANT_SPREAD_ATR_MAX",float), "leverage":("LEVERAGE",int),
        }
        if key not in allowed: return f"❌ Unknown key: {key}\nAllowed: {', '.join(allowed.keys())}"
        attr, typ = allowed[key]
        try: new_val = typ(val_str)
        except: return f"❌ Invalid value: {val_str}"
        old_val = getattr(cfg, attr, "?")
        if key == "leverage":
            if bot_running and bot_instance and bot_instance.strategy:
                if bot_instance.strategy.get_position():
                    return "❌ Cannot change leverage with open position."
            setattr(cfg, attr, new_val)
            if bot_running and bot_instance and bot_instance.order_manager:
                try:
                    bot_instance.order_manager.set_leverage(leverage=int(new_val))
                except Exception as e:
                    setattr(cfg, attr, old_val); return f"❌ Exchange rejected: {e}"
        else:
            setattr(cfg, attr, new_val)
        return f"✅ <b>{attr}</b>: {old_val} → <b>{new_val}</b>"

    def _cmd_setexchange(self, args):
        global bot_instance, bot_running
        if not args:
            active = getattr(config, "EXECUTION_EXCHANGE", "?")
            return f"Current: <b>{active.upper()}</b>\nUsage: /setexchange delta|coinswitch"
        target = args.strip().lower()
        if not bot_running or not bot_instance:
            try:
                from core.types import Exchange
                Exchange.from_str(target)
                config.EXECUTION_EXCHANGE = Exchange.from_str(target).value
                return f"✅ Set to {target.upper()} (takes effect on start)"
            except ValueError: return f"❌ Unknown: {target}"
        router = getattr(bot_instance, "execution_router", None)
        if not router: return "❌ Router not available."
        strategy = getattr(bot_instance, "strategy", None)
        ok, msg = router.switch(target, strategy=strategy)
        return msg

    # ════════════════════════════════════════
    # BOT THREAD
    # ════════════════════════════════════════

    def _run_bot_thread(self):
        global bot_instance, bot_running
        try:
            bot_running = True
            _root = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
            if _root not in sys.path: sys.path.insert(0, _root)
            from main import QuantBot
            bot_instance = QuantBot()
            if not bot_instance.initialize():
                self.send_message("❌ Init failed."); bot_running = False; return
            if not bot_instance.start():
                self.send_message("❌ Start failed."); bot_running = False; return
            bot_instance.run()
        except Exception as e:
            logger.error(f"Bot crashed: {e}", exc_info=True)
            self.send_message(f"❌ Crashed: {e}")
        finally: bot_running = False

    # ════════════════════════════════════════
    # MAIN LOOP
    # ════════════════════════════════════════

    def start(self):
        self.running = True
        self.clear_old_messages()
        self.set_my_commands()
        self.send_message("⚡ <b>Predictive Quant v2 Controller Ready</b>\n" + self._cmd_help())
        while self.running:
            try:
                updates = self.get_updates(timeout=10)
                for upd in updates:
                    self.last_update_id = upd.get("update_id", self.last_update_id)
                    msg = upd.get("message") or {}
                    cid = str((msg.get("chat") or {}).get("id", ""))
                    text = (msg.get("text") or "").strip()
                    if cid != self.chat_id or not text: continue
                    logger.info(f"Cmd: {text}")
                    resp = self.handle_command(text)
                    if resp: self.send_message(resp)
            except KeyboardInterrupt: break
            except Exception as e:
                logger.error(f"Loop error: {e}", exc_info=True); time.sleep(2)

    def stop(self):
        self.running = False
        global bot_instance, bot_running
        if bot_running and bot_instance: bot_instance.stop()


# Import QCfg for /thinking to access config values
try:
    from strategy.quant_integration import QCfg
except ImportError:
    class QCfg:
        @staticmethod
        def COOLDOWN_SEC(): return 180
        @staticmethod
        def MAX_DAILY_TRADES(): return 8
        @staticmethod
        def MIN_CONFIDENCE(): return 0.55


def main():
    import io, signal
    from datetime import timezone, timedelta
    IST = timezone(timedelta(hours=5, minutes=30))
    class ISTFmt(logging.Formatter):
        def formatTime(self, record, datefmt=None):
            dt = datetime.fromtimestamp(record.created, tz=IST)
            return f"{dt.strftime('%Y-%m-%d %H:%M:%S')},{int(record.msecs):03d}"
    fmt = ISTFmt(fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    fh = logging.FileHandler("telegram_controller.log", encoding="utf-8"); fh.setFormatter(fmt)
    sh = logging.StreamHandler(
        stream=io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        if hasattr(sys.stdout, "buffer") else sys.stdout); sh.setFormatter(fmt)
    logging.basicConfig(level=getattr(config, "LOG_LEVEL", "INFO"), handlers=[fh, sh])
    def _sh(signum, frame): sys.exit(0)
    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGINT, _sh); signal.signal(signal.SIGTERM, _sh)
    try:
        TelegramBotController().start()
    except KeyboardInterrupt: pass
    except Exception as e: logger.error(f"Fatal: {e}", exc_info=True); sys.exit(1)

if __name__ == "__main__":
    main()

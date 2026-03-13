"""
Telegram Bot Controller — Quant Bot
=====================================
Commands:
  /start      - Start quant bot
  /stop       - Stop quant bot
  /status     - Full status: phase, signal, ATR, P&L
  /signal     - Live signal breakdown (all 5 engines)
  /position   - Active position details (entry, SL, TP, hold time)
  /trades     - Recent trade history + session stats
  /balance    - Wallet balance
  /pause      - Pause new entries (keep managing open position)
  /resume     - Resume trading
  /config     - Show current quant config values
  /set <k> <v>- Live-adjust a config key (no restart needed)
  /killswitch - Emergency: cancel all orders + close position
  /help       - Show all commands
"""

import logging
import time
import threading
import requests
from typing import Optional
from datetime import datetime, timezone
import sys

import telegram_config

logger = logging.getLogger(__name__)

bot_instance = None
bot_thread   = None
bot_running  = False


class TelegramBotController:
    def __init__(self):
        self.bot_token = telegram_config.TELEGRAM_BOT_TOKEN
        self.chat_id   = str(telegram_config.TELEGRAM_CHAT_ID)
        self.last_update_id = 0
        self.running = False

        if not self.bot_token or not self.chat_id:
            raise ValueError("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be set")

        logger.info("TelegramBotController (Quant) initialized")

    # ================================================================
    # MESSAGING
    # ================================================================

    def send_message(self, message: str, parse_mode: str = "HTML") -> bool:
        try:
            if len(message) > 4000:
                chunks = []
                while message:
                    if len(message) <= 4000:
                        chunks.append(message); break
                    split_at = message.rfind('\n', 0, 4000)
                    if split_at == -1: split_at = 4000
                    chunks.append(message[:split_at])
                    message = message[split_at:]
                for chunk in chunks:
                    self._send_raw(chunk, parse_mode)
                    time.sleep(0.5)
                return True
            return self._send_raw(message, parse_mode)
        except Exception as e:
            logger.error(f"Send error: {e}")
            try:
                return self._send_raw(message, parse_mode=None)
            except Exception:
                return False

    def _send_raw(self, text: str, parse_mode: Optional[str] = "HTML") -> bool:
        url     = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": text,
                   "disable_web_page_preview": True}
        if parse_mode:
            payload["parse_mode"] = parse_mode
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code != 200:
            logger.error(f"Telegram API {resp.status_code}: {resp.text[:200]}")
        return resp.status_code == 200

    def get_updates(self, timeout: int = 30) -> list:
        try:
            url    = f"https://api.telegram.org/bot{self.bot_token}/getUpdates"
            params = {"offset": self.last_update_id + 1, "timeout": timeout,
                      "allowed_updates": ["message"]}
            resp   = requests.get(url, params=params, timeout=timeout + 5)
            if resp.status_code != 200:
                return []
            data = resp.json()
            return data.get("result", []) if data.get("ok") else []
        except Exception:
            return []

    def clear_old_messages(self):
        try:
            updates = self.get_updates(timeout=1)
            if updates:
                self.last_update_id = updates[-1]["update_id"]
        except Exception as e:
            logger.error(f"Error clearing messages: {e}")

    def set_my_commands(self):
        try:
            url = f"https://api.telegram.org/bot{self.bot_token}/setMyCommands"
            commands = [
                {"command": "start",      "description": "Start quant bot"},
                {"command": "stop",       "description": "Stop quant bot"},
                {"command": "status",     "description": "Full status: signal, ATR, P&L"},
                {"command": "signal",     "description": "Live signal breakdown (all 5 engines)"},
                {"command": "position",   "description": "Active position details"},
                {"command": "trades",     "description": "Recent trade history"},
                {"command": "balance",    "description": "Wallet balance"},
                {"command": "pause",      "description": "Pause new entries"},
                {"command": "resume",     "description": "Resume trading"},
                {"command": "config",     "description": "Show quant config values"},
                {"command": "set",        "description": "Live-adjust config: /set key value"},
                {"command": "killswitch", "description": "Emergency: close all"},
                {"command": "help",       "description": "Show commands"},
            ]
            resp = requests.post(url, json={"commands": commands,
                                            "scope": {"type": "all_private_chats"},
                                            "language_code": "en"}, timeout=10)
            if resp.status_code != 200:
                logger.error(f"Command registration failed: {resp.text}")
        except Exception as e:
            logger.error(f"Error setting commands: {e}")

    # ================================================================
    # COMMAND ROUTING
    # ================================================================

    def _normalize_command(self, text: str) -> tuple:
        t = (text or "").strip()
        if not t.startswith("/"):
            parts = t.split(None, 1)
            cmd   = parts[0].lower()
            args  = parts[1] if len(parts) > 1 else ""
            known = ("start","stop","status","signal","position","trades",
                     "balance","pause","resume","config","set","killswitch","help")
            if cmd in known:
                return f"/{cmd}", args
            return t, ""
        parts = t.split(None, 1)
        return parts[0].lower(), parts[1] if len(parts) > 1 else ""

    def handle_command(self, raw_text: str) -> Optional[str]:
        cmd, args = self._normalize_command(raw_text)
        try:
            if   cmd in ("/help", "/commands"):  return self._cmd_help()
            elif cmd == "/start":                 return self._cmd_start()
            elif cmd == "/stop":                  return self._cmd_stop()
            elif cmd == "/status":                return self._cmd_status()
            elif cmd == "/signal":                return self._cmd_signal()
            elif cmd == "/position":              return self._cmd_position()
            elif cmd == "/trades":                return self._cmd_trades()
            elif cmd == "/balance":               return self._cmd_balance()
            elif cmd == "/pause":                 return self._cmd_pause()
            elif cmd == "/resume":                return self._cmd_resume()
            elif cmd == "/config":                return self._cmd_config()
            elif cmd == "/set":                   return self._cmd_set(args)
            elif cmd == "/killswitch":            return self._cmd_killswitch()
            else:
                return f"Unknown command: {cmd}\n\n" + self._cmd_help()
        except Exception as e:
            logger.error(f"Command error [{cmd}]: {e}", exc_info=True)
            return f"Error: {e}"

    # ================================================================
    # COMMAND IMPLEMENTATIONS
    # ================================================================

    def _cmd_help(self) -> str:
        return (
            "<b>⚡ Quant Bot Commands</b>\n\n"
            "/start      — Start quant bot\n"
            "/stop       — Stop quant bot\n"
            "/status     — Full status: signal, ATR, phase, P&amp;L\n"
            "/signal     — Live breakdown of all 5 alpha signals\n"
            "/position   — Active position (entry, SL, TP, hold)\n"
            "/trades     — Recent trade history + win rate\n"
            "/balance    — Wallet balance\n"
            "/pause      — Pause new entries\n"
            "/resume     — Resume trading\n"
            "/config     — Show quant config values\n"
            "/set &lt;key&gt; &lt;val&gt; — Live-adjust config\n"
            "/killswitch — Emergency: cancel all + close position\n"
            "/help       — This list"
        )

    def _cmd_start(self) -> str:
        global bot_instance, bot_thread, bot_running
        if bot_running and bot_thread and bot_thread.is_alive():
            return "Bot already running."
        bot_thread = threading.Thread(target=self._run_bot_thread, daemon=True)
        bot_thread.start()
        time.sleep(2.0)
        return "Starting quant bot... Use /status in 30s." if bot_thread.is_alive() \
               else "Start failed — check logs."

    def _cmd_stop(self) -> str:
        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return "Bot not running."
        bot_running = False
        if bot_instance:
            bot_instance.stop()
        return "Quant bot stopped."

    def _cmd_status(self) -> str:
        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return "Bot not running. Use /start"
        try:
            bot   = bot_instance
            strat = bot.strategy
            dm    = bot.data_manager
            rm    = bot.risk_manager
            if not strat or not dm or not rm:
                return "Bot components not ready yet."

            from quant_strategy import QCfg
            price = dm.get_last_price()
            bal   = rm.get_available_balance()
            avail = float(bal.get("available", 0)) if bal else 0.0
            total = float(bal.get("total", avail)) if bal else 0.0
            stats = strat.get_stats()

            sig   = strat._last_sig
            phase = stats["current_phase"]
            atr5  = stats["atr_5m"]
            atr1  = stats["atr_1m"]
            pctile= stats["atr_pctile"]
            regime= "✅ Valid" if stats["regime_ok"] else "🚫 Gated"

            # Composite bar
            comp  = sig.composite
            bar   = "█" * int(abs(comp) * 20)
            sign  = "+" if comp >= 0 else "-"

            pos     = strat.get_position()
            pos_txt = ""
            if pos:
                side  = pos.get("side", "?").upper()
                entry = pos.get("entry_price", 0)
                sl    = strat.current_sl_price
                tp    = strat.current_tp_price
                qty   = pos.get("quantity", 0)
                hold  = (time.time() - strat._pos.entry_time) / 60
                trail = "✅" if strat._pos.trail_active else "pending"
                pos_txt = (
                    f"\n\n<b>Active Position ({side})</b>\n"
                    f"Entry:  ${entry:,.2f}\n"
                    f"SL:     ${sl:,.2f}\n"
                    f"TP:     ${tp:,.2f}\n"
                    f"Qty:    {qty} BTC\n"
                    f"Hold:   {hold:.1f} min\n"
                    f"Trail:  {trail}"
                )

            msg = (
                f"<b>⚡ Quant Bot Status</b>\n"
                f"{'=' * 28}\n"
                f"Price:   <b>${price:,.2f}</b>\n"
                f"Balance: <b>${avail:,.2f}</b> (total ${total:,.2f})\n"
                f"Phase:   <b>{phase}</b>\n\n"
                f"<b>Signal</b>\n"
                f"Composite: {comp:+.4f}\n"
                f"  [{sign}{bar:<20}]\n"
                f"Regime:  {regime}  ATR 5m/${atr5} 1m/${atr1} ({pctile})\n\n"
                f"<b>Session P&amp;L</b>\n"
                f"Trades:  {stats['total_trades']} | WR {stats['win_rate']}\n"
                f"PnL:     ${stats['total_pnl']:+.2f} USDT\n"
                f"Daily:   {stats['daily_trades']}/{QCfg.MAX_DAILY_TRADES()} trades | "
                f"{stats['consec_losses']}/{QCfg.MAX_CONSEC_LOSSES()} losses"
                f"{pos_txt}"
            )
            self.send_message(msg)
            return None
        except Exception as e:
            logger.error(f"Status error: {e}", exc_info=True)
            return f"Status error: {e}"

    def _cmd_signal(self) -> str:
        """Live breakdown of all 5 alpha engines."""
        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return "Bot not running."
        try:
            strat = bot_instance.strategy
            dm    = bot_instance.data_manager
            if not strat or not dm:
                return "Components not ready."

            price = dm.get_last_price()
            sig   = strat._last_sig

            def bar(v):
                n = int(abs(v) * 15)
                return ("+" if v >= 0 else "-") + "█" * n + "░" * (15 - n)

            from quant_strategy import QCfg
            lt = QCfg.LONG_THRESHOLD()
            st = QCfg.SHORT_THRESHOLD()

            if sig.composite >= lt:
                action = f"✅ LONG SIGNAL  ({sig.composite:+.4f} ≥ {lt})"
            elif sig.composite <= -st:
                action = f"✅ SHORT SIGNAL ({sig.composite:+.4f} ≤ -{st})"
            else:
                need = min(lt - sig.composite, st + sig.composite)
                action = f"⏸  NEUTRAL — need {need:.3f} more to fire"

            # Trailing status
            pos = strat.get_position()
            pos_line = ""
            if pos:
                trail = "active ✅" if strat._pos.trail_active else "pending"
                pos_line = f"\n\nTrailing SL: {trail}"

            msg = (
                f"<b>⚡ Signal Breakdown @ ${price:,.2f}</b>\n"
                f"{'─' * 32}\n"
                f"CVD  (order flow)   W={QCfg.W_CVD():.0%}\n"
                f"  {bar(sig.cvd)}  {sig.cvd:+.4f}\n\n"
                f"VWAP (institutional) W={QCfg.W_VWAP():.0%}\n"
                f"  {bar(sig.vwap)}  {sig.vwap:+.4f}\n\n"
                f"MOM  (EMA cross)    W={QCfg.W_MOM():.0%}\n"
                f"  {bar(sig.mom)}  {sig.mom:+.4f}\n\n"
                f"SQZ  (KC squeeze)   W={QCfg.W_SQUEEZE():.0%}\n"
                f"  {bar(sig.squeeze)}  {sig.squeeze:+.4f}\n\n"
                f"VFL  (volume flow)  W={QCfg.W_VOL():.0%}\n"
                f"  {bar(sig.vol)}  {sig.vol:+.4f}\n"
                f"{'─' * 32}\n"
                f"COMPOSITE: {sig.composite:+.4f}\n"
                f"  {bar(sig.composite)}\n\n"
                f"{action}\n\n"
                f"ATR 5m: ${sig.atr:.1f}  ({sig.atr_pct:.0%} pctile)\n"
                f"Regime: {'✅ Valid' if sig.regime_ok else '🚫 Gated'}"
                f"{pos_line}"
            )
            return msg
        except Exception as e:
            logger.error(f"Signal error: {e}", exc_info=True)
            return f"Signal error: {e}"

    def _cmd_position(self) -> str:
        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return "Bot not running."
        strat = bot_instance.strategy
        if not strat:
            return "Strategy not ready."

        pos = strat.get_position()
        if not pos:
            phase = strat._pos.phase.name if strat._pos else "FLAT"
            return f"No active position. Phase: {phase}"

        side  = pos.get("side", "?").upper()
        entry = pos.get("entry_price", 0)
        sl    = strat.current_sl_price
        tp    = strat.current_tp_price
        qty   = pos.get("quantity", 0)
        risk  = strat._pos.initial_risk
        price = bot_instance.data_manager.get_last_price()
        hold  = (time.time() - strat._pos.entry_time) / 60
        trail = "✅ active" if strat._pos.trail_active else "pending"

        if entry > 0 and sl > 0:
            sl_dist = abs(entry - sl)
            profit  = (price - entry) if side == "LONG" else (entry - price)
            current_r = profit / sl_dist if sl_dist > 0 else 0.0
        else:
            current_r = 0.0

        from quant_strategy import QCfg
        max_hold_min = QCfg.MAX_HOLD_SEC() / 60

        return (
            f"<b>Active Position — {side}</b>\n\n"
            f"Entry:    ${entry:,.2f}\n"
            f"Current:  ${price:,.2f}\n"
            f"SL:       ${sl:,.2f}\n"
            f"TP:       ${tp:,.2f}\n"
            f"Qty:      {qty} BTC\n"
            f"Risk:     ${risk:.2f} USDT\n\n"
            f"Current R: {current_r:+.2f}R\n"
            f"Hold:     {hold:.1f} / {max_hold_min:.0f} min\n"
            f"Trail SL: {trail}\n\n"
            f"<i>Entry signal: {strat._pos.entry_signal}</i>"
        )

    def _cmd_trades(self) -> str:
        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return "Bot not running."
        strat = bot_instance.strategy
        rm    = bot_instance.risk_manager
        if not strat:
            return "Strategy not ready."

        stats = strat.get_stats()

        # Try to get trade history from risk manager
        history = getattr(rm, 'trade_history', [])
        lines   = ["<b>Recent Trades</b>\n"]
        if history:
            recent = list(history)[-6:]
            for trade in recent:
                side   = getattr(trade, "side", "?").upper()
                pnl    = getattr(trade, "pnl", 0)
                reason = getattr(trade, "reason", "?")
                icon   = "✅" if pnl >= 0 else "❌"
                lines.append(f"  {icon} {side}  ${pnl:+.2f}  [{reason}]")
        else:
            lines.append("  (no trade history in risk manager)")

        lines += [
            "",
            f"<b>Session Stats</b>",
            f"Trades:   {stats['total_trades']}",
            f"Win Rate: {stats['win_rate']}",
            f"PnL:      ${stats['total_pnl']:+.2f} USDT",
            f"Daily:    {stats['daily_trades']} trades | "
            f"{stats['consec_losses']} consec losses",
        ]
        return "\n".join(lines)

    def _cmd_balance(self) -> str:
        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return "Bot not running."
        rm = bot_instance.risk_manager
        if not rm:
            return "Risk manager not ready."
        try:
            bal   = rm.get_available_balance()
            if not bal:
                return "Could not fetch balance."
            avail = float(bal.get("available", 0))
            total = float(bal.get("total", avail))
            locked = total - avail
            return (
                f"<b>Wallet Balance</b>\n"
                f"Available: <b>${avail:,.2f}</b> USDT\n"
                f"Locked:    ${locked:,.2f} USDT\n"
                f"Total:     ${total:,.2f} USDT"
            )
        except Exception as e:
            return f"Balance error: {e}"

    def _cmd_pause(self) -> str:
        global bot_instance
        if not bot_instance:
            return "Bot not running."
        bot_instance.trading_enabled      = False
        bot_instance.trading_pause_reason = "Paused via Telegram"
        return "Trading PAUSED. Open position continues to be managed. Use /resume to re-enable."

    def _cmd_resume(self) -> str:
        global bot_instance
        if not bot_instance:
            return "Bot not running."
        bot_instance.trading_enabled      = True
        bot_instance.trading_pause_reason = ""
        return "Trading RESUMED."

    def _cmd_config(self) -> str:
        try:
            import config as cfg
            from quant_strategy import QCfg
            lines = [
                "<b>⚡ Quant Bot Config</b>\n",
                f"Symbol:        {cfg.SYMBOL}",
                f"Leverage:      {cfg.LEVERAGE}x",
                f"Margin/trade:  {QCfg.MARGIN_PCT():.0%}",
                f"Tick size:     ${QCfg.TICK_SIZE()}",
                "",
                "<b>Signal Thresholds</b>",
                f"Entry:         ±{QCfg.LONG_THRESHOLD():.2f}",
                f"Exit flip:     ±{QCfg.EXIT_FLIP_THRESH():.2f}",
                f"Confirm ticks: {QCfg.CONFIRM_TICKS()}",
                "",
                "<b>SL / TP</b>",
                f"SL mult:       {QCfg.SL_ATR_MULT()}×ATR",
                f"TP mult:       {QCfg.TP_ATR_MULT()}×ATR",
                f"Min R:R:       {QCfg.MIN_RR_RATIO()}",
                f"SL range:      {QCfg.MIN_SL_PCT():.1%} – {QCfg.MAX_SL_PCT():.1%}",
                "",
                "<b>Trailing SL</b>",
                f"Enabled:       {QCfg.TRAIL_ENABLED()}",
                f"Activate at:   {QCfg.TRAIL_ACTIVATE_R()}R",
                f"Trail by:      {QCfg.TRAIL_ATR_MULT()}×ATR",
                "",
                "<b>Risk Limits</b>",
                f"Max daily:     {QCfg.MAX_DAILY_TRADES()} trades",
                f"Max consec L:  {QCfg.MAX_CONSEC_LOSSES()}",
                f"Daily loss:    {QCfg.MAX_DAILY_LOSS_PCT()}%",
                "",
                "<b>Signal Weights</b>",
                f"CVD={QCfg.W_CVD()} VWAP={QCfg.W_VWAP()} MOM={QCfg.W_MOM()} "
                f"SQZ={QCfg.W_SQUEEZE()} VFL={QCfg.W_VOL()}",
                "",
                "<b>Timing</b>",
                f"Max hold:      {QCfg.MAX_HOLD_SEC()//60} min",
                f"Cooldown:      {QCfg.COOLDOWN_SEC()}s after exit",
                f"Eval interval: {QCfg.TICK_EVAL_SEC()}s",
            ]
            return "\n".join(lines)
        except Exception as e:
            return f"Config error: {e}"

    def _cmd_killswitch(self) -> str:
        global bot_instance
        if not bot_instance:
            return "Bot not running."
        try:
            import config
            bot_instance.trading_enabled = False
            om = bot_instance.order_manager
            if not om:
                return "Order manager not available."

            # Cancel all open orders
            try:
                om.api.cancel_all_orders(symbol=config.SYMBOL)
                logger.warning("KILLSWITCH: Cancelled all orders")
            except Exception as e:
                logger.error(f"Killswitch cancel error: {e}")

            # Close open position if any
            try:
                pos = om.get_open_position()
                if pos and float(pos.get("size", 0)) > 0:
                    exit_side = "sell" if str(pos.get("side","")).upper() == "LONG" else "buy"
                    om.place_market_order(
                        side=exit_side,
                        quantity=float(pos["size"]),
                        reduce_only=True
                    )
                    logger.warning(f"KILLSWITCH: Closed {pos.get('side')} position")
            except Exception as e:
                logger.error(f"Killswitch close error: {e}")

            # Reset strategy state
            if bot_instance.strategy:
                bot_instance.strategy._finalise_exit()

            return (
                "🔴 <b>KILLSWITCH ACTIVATED</b>\n"
                "• All orders cancelled\n"
                "• Position closed (if any)\n"
                "• Trading PAUSED\n\n"
                "Use /resume to re-enable trading."
            )
        except Exception as e:
            return f"Killswitch error: {e}"

    def _cmd_set(self, args: str) -> str:
        """Live-adjust a quant config parameter. Takes effect on next tick."""
        import config as cfg

        if not args or len(args.split()) < 2:
            return (
                "Usage: /set &lt;key&gt; &lt;value&gt;\n\n"
                "<b>Adjustable keys:</b>\n"
                "  leverage\n"
                "  quant_margin_pct          (e.g. 0.20)\n"
                "  quant_long_threshold      (e.g. 0.60)\n"
                "  quant_short_threshold     (e.g. 0.60)\n"
                "  quant_exit_flip           (e.g. 0.35)\n"
                "  quant_confirm_ticks       (e.g. 3)\n"
                "  quant_sl_atr_mult         (e.g. 1.2)\n"
                "  quant_tp_atr_mult         (e.g. 3.0)\n"
                "  min_risk_reward_ratio     (e.g. 2.0)\n"
                "  quant_max_hold_sec        (e.g. 3600)\n"
                "  quant_cooldown_sec        (e.g. 120)\n"
                "  quant_trail_enabled       (e.g. True)\n"
                "  quant_trail_activate_r    (e.g. 0.75)\n"
                "  max_daily_trades          (e.g. 6)\n"
                "  max_consecutive_losses    (e.g. 2)\n"
                "  max_daily_loss_pct        (e.g. 3.0)\n"
                "  quant_w_cvd / _vwap / _mom / _squeeze / _vol"
            )

        parts   = args.split(None, 1)
        key     = parts[0].lower()
        val_str = parts[1].strip()

        # Map command-friendly names → (config attr, type)
        allowed = {
            "leverage":                ("LEVERAGE",               int),
            "quant_margin_pct":        ("QUANT_MARGIN_PCT",       float),
            "quant_long_threshold":    ("QUANT_LONG_THRESHOLD",   float),
            "quant_short_threshold":   ("QUANT_SHORT_THRESHOLD",  float),
            "quant_exit_flip":         ("QUANT_EXIT_FLIP",        float),
            "quant_confirm_ticks":     ("QUANT_CONFIRM_TICKS",    int),
            "quant_sl_atr_mult":       ("QUANT_SL_ATR_MULT",      float),
            "quant_tp_atr_mult":       ("QUANT_TP_ATR_MULT",      float),
            "min_risk_reward_ratio":   ("MIN_RISK_REWARD_RATIO",  float),
            "quant_max_hold_sec":      ("QUANT_MAX_HOLD_SEC",     int),
            "quant_cooldown_sec":      ("QUANT_COOLDOWN_SEC",     int),
            "quant_trail_enabled":     ("QUANT_TRAIL_ENABLED",    bool),
            "quant_trail_activate_r":  ("QUANT_TRAIL_ACTIVATE_R", float),
            "quant_trail_atr_mult":    ("QUANT_TRAIL_ATR_MULT",   float),
            "max_daily_trades":        ("MAX_DAILY_TRADES",       int),
            "max_consecutive_losses":  ("MAX_CONSECUTIVE_LOSSES", int),
            "max_daily_loss_pct":      ("MAX_DAILY_LOSS_PCT",     float),
            "quant_w_cvd":             ("QUANT_W_CVD",            float),
            "quant_w_vwap":            ("QUANT_W_VWAP",           float),
            "quant_w_mom":             ("QUANT_W_MOM",            float),
            "quant_w_squeeze":         ("QUANT_W_SQUEEZE",        float),
            "quant_w_vol":             ("QUANT_W_VOL",            float),
        }

        if key not in allowed:
            return (f"Unknown key: <code>{key}</code>\n"
                    f"Use /set without args to see all allowed keys.")

        attr_name, val_type = allowed[key]

        try:
            if val_type == bool:
                new_val = val_str.lower() in ("true", "1", "yes", "on")
            else:
                new_val = val_type(val_str)
        except ValueError:
            return f"Invalid value <code>{val_str}</code> — expected {val_type.__name__}"

        # Safety bounds
        bounds = {
            "QUANT_MARGIN_PCT":        (0.05, 0.50),
            "QUANT_LONG_THRESHOLD":    (0.30, 0.90),
            "QUANT_SHORT_THRESHOLD":   (0.30, 0.90),
            "QUANT_EXIT_FLIP":         (0.10, 0.70),
            "QUANT_SL_ATR_MULT":       (0.50, 5.00),
            "QUANT_TP_ATR_MULT":       (0.50, 10.0),
            "MIN_RISK_REWARD_RATIO":   (0.50, 5.00),
            "LEVERAGE":                (1,    50),
        }
        if attr_name in bounds:
            lo, hi = bounds[attr_name]
            if not (lo <= new_val <= hi):
                return f"Value out of safe range [{lo}, {hi}]: {new_val}"

        # Warn if signal weights will no longer sum to 1.0
        weight_attrs = {"QUANT_W_CVD","QUANT_W_VWAP","QUANT_W_MOM",
                        "QUANT_W_SQUEEZE","QUANT_W_VOL"}
        if attr_name in weight_attrs:
            cur = {a: getattr(cfg, a, 0) for a in weight_attrs}
            cur[attr_name] = new_val
            total = sum(cur.values())
            if abs(total - 1.0) > 0.01:
                return (
                    f"⚠️ Weight sum would be {total:.3f} ≠ 1.0\n"
                    f"Adjust other weights so they sum to 1.0.\n"
                    f"Current: CVD={cfg.QUANT_W_CVD} VWAP={cfg.QUANT_W_VWAP} "
                    f"MOM={cfg.QUANT_W_MOM} SQZ={cfg.QUANT_W_SQUEEZE} VFL={cfg.QUANT_W_VOL}"
                )

        old_val = getattr(cfg, attr_name, "?")
        setattr(cfg, attr_name, new_val)
        logger.info(f"CONFIG LIVE-CHANGE via Telegram: {attr_name} = {old_val} → {new_val}")
        return (
            f"✅ <b>{attr_name}</b>\n"
            f"{old_val} → <b>{new_val}</b>\n"
            f"<i>Takes effect on next tick</i>"
        )

    # ================================================================
    # BOT THREAD
    # ================================================================

    def _run_bot_thread(self):
        global bot_instance, bot_running
        try:
            bot_running = True
            from main import QuantBot
            bot_instance = QuantBot()

            if not bot_instance.initialize():
                self.send_message("❌ Quant bot init failed.")
                bot_running = False
                return

            if not bot_instance.start():
                self.send_message("❌ Quant bot start failed.")
                bot_running = False
                return

            bot_instance.run()

        except Exception as e:
            logger.error(f"Bot crashed: {e}", exc_info=True)
            self.send_message(f"💥 Quant bot crashed: {e}")
        finally:
            bot_running = False
            logger.info("Bot thread finished")

    # ================================================================
    # MAIN LOOP
    # ================================================================

    def start(self):
        self.running = True
        self.clear_old_messages()
        self.set_my_commands()
        self.send_message(
            "⚡ <b>Quant Bot Controller Ready</b>\n\n" + self._cmd_help()
        )
        logger.info("Telegram controller started")

        while self.running:
            try:
                updates = self.get_updates(timeout=30)
                for upd in updates:
                    self.last_update_id = upd.get("update_id", self.last_update_id)
                    msg     = upd.get("message") or {}
                    chat_id = str((msg.get("chat") or {}).get("id", ""))
                    text    = (msg.get("text") or "").strip()

                    if chat_id != self.chat_id or not text:
                        continue

                    logger.info(f"Received: {text}")
                    response = self.handle_command(text)
                    if response:
                        self.send_message(response)

            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Command loop error: {e}", exc_info=True)
                time.sleep(2.0)

        logger.info("Controller stopped")

    def stop(self):
        self.running = False
        global bot_instance, bot_running
        if bot_running and bot_instance:
            bot_instance.stop()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("telegram_controller.log"),
            logging.StreamHandler(),
        ],
    )
    try:
        import config   # ensure config is importable
        controller = TelegramBotController()
        controller.start()
    except KeyboardInterrupt:
        logger.info("Shutdown requested")
    except Exception as e:
        logger.error(f"Fatal: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

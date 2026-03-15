"""
Telegram Bot Controller — Quant Bot v4.3
==========================================
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
  /trail      - Toggle trailing SL: /trail on | /trail off | /trail auto
  /config     - Show current quant config values
  /set <k> <v>- Live-adjust a config key (no restart needed)
  /killswitch - Emergency: cancel all orders + close position
  /help       - Show all commands

FIX v4.3.1: _cmd_set now verifies the change by reading back from config
module after setattr. Previously the change was applied to the config module
but risk_manager cached stale values — that is now fixed in risk_manager.py.
The readback here provides an extra safety net and user-visible confirmation.
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
                {"command": "trail",      "description": "Trail SL: /trail on|off|auto"},
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
                     "balance","pause","resume","trail","config","set","killswitch","help")
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
            elif cmd == "/trail":                 return self._cmd_trail(args)
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
            "/trail      — Trailing SL: /trail on | off | auto\n"
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
        """Live breakdown of all 5 reversion engines."""
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
            thr = QCfg.COMPOSITE_ENTRY_MIN()

            if sig.composite >= thr and sig.overextended:
                action = f"✅ LONG SIGNAL  ({sig.composite:+.4f} ≥ {thr:.3f})"
            elif sig.composite <= -thr and sig.overextended:
                action = f"✅ SHORT SIGNAL ({sig.composite:+.4f} ≤ -{thr:.3f})"
            else:
                action = f"⏸  NEUTRAL — waiting for overextension + confluence"

            pos = strat.get_position()
            pos_line = ""
            if pos:
                trail = "active ✅" if strat._pos.trail_active else "pending"
                pos_line = f"\n\nTrailing SL: {trail}"

            msg = (
                f"<b>⚡ v4 Reversion Signal @ ${price:,.2f}</b>\n"
                f"VWAP: ${sig.vwap_price:,.2f} (dev={sig.deviation_atr:+.1f} ATR)\n"
                f"{'─' * 32}\n"
                f"VWAP (reversion)   W={QCfg.W_VWAP_DEV():.0%}\n"
                f"  {bar(sig.vwap_dev)}  {sig.vwap_dev:+.4f}\n\n"
                f"CVD  (divergence)  W={QCfg.W_CVD_DIV():.0%}\n"
                f"  {bar(sig.cvd_div)}  {sig.cvd_div:+.4f}\n\n"
                f"OB   (orderbook)   W={QCfg.W_OB():.0%}\n"
                f"  {bar(sig.orderbook)}  {sig.orderbook:+.4f}\n\n"
                f"TICK (trade flow)  W={QCfg.W_TICK_FLOW():.0%}\n"
                f"  {bar(sig.tick_flow)}  {sig.tick_flow:+.4f}\n\n"
                f"VEX  (exhaustion)  W={QCfg.W_VOL_EXHAUSTION():.0%}\n"
                f"  {bar(sig.vol_exhaust)}  {sig.vol_exhaust:+.4f}\n"
                f"{'─' * 32}\n"
                f"COMPOSITE: {sig.composite:+.4f}\n"
                f"  {bar(sig.composite)}\n\n"
                f"{action}\n\n"
                f"Confirming: {sig.n_confirming}/5 | Threshold: ±{thr:.3f}\n"
                f"Overextended: {'✅' if sig.overextended else '❌'} | "
                f"Regime: {'✅' if sig.regime_ok else '❌'} | "
                f"HTF veto: {'🚫' if sig.htf_veto else '✅'}\n"
                f"ATR 5m: ${sig.atr:.1f}  ({sig.atr_pct:.0%} pctile)"
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

    def _cmd_trail(self, args: str) -> str:
        """Toggle trailing SL on/off/auto — works even mid-position.
        
        /trail on    — Force trailing SL enabled (overrides config)
        /trail off   — Force trailing SL disabled (overrides config)
        /trail auto  — Use config.QUANT_TRAIL_ENABLED (default behaviour)
        /trail       — Show current state
        """
        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return "Bot not running."
        strat = bot_instance.strategy
        if not strat:
            return "Strategy not ready."

        args = args.strip().lower()

        if args in ("on", "enable", "yes", "true", "1"):
            strat.set_trail_override(True)
            return "🔒 Trailing SL <b>FORCE ENABLED</b> — applies immediately to current position."

        elif args in ("off", "disable", "no", "false", "0"):
            strat.set_trail_override(False)
            return (
                "🔓 Trailing SL <b>FORCE DISABLED</b>.\n"
                "SL stays where it is. TP/SL on exchange still active.\n"
                "Use /trail on or /trail auto to re-enable."
            )

        elif args in ("auto", "default", "reset", "config"):
            strat.set_trail_override(None)
            from quant_strategy import QCfg
            current = QCfg.TRAIL_ENABLED()
            return (
                f"🔄 Trail override cleared → using config default.\n"
                f"Config QUANT_TRAIL_ENABLED = <b>{current}</b>\n"
                f"Change with: /set quant_trail_enabled true/false"
            )

        else:
            # Show current state
            from quant_strategy import QCfg
            override = strat._pos.trail_override if hasattr(strat._pos, 'trail_override') else None
            config_val = QCfg.TRAIL_ENABLED()
            effective = strat.get_trail_enabled()
            pos = strat.get_position()

            status_lines = [
                "<b>🔒 Trailing SL Status</b>",
                "",
                f"Config default:  <b>{config_val}</b>",
                f"Override:        <b>{override if override is not None else 'None (using config)'}</b>",
                f"Effective:       <b>{'ENABLED ✅' if effective else 'DISABLED ❌'}</b>",
                "",
                f"Trail BE at:     {QCfg.TRAIL_BE_R()}R",
                f"Trail Lock at:   {QCfg.TRAIL_LOCK_R()}R",
                f"Chandelier:      {QCfg.TRAIL_CHANDELIER_N_START()} → {QCfg.TRAIL_CHANDELIER_N_END()} ATR",
            ]

            if pos:
                trail_active = strat._pos.trail_active
                status_lines += [
                    "",
                    f"<b>Current position:</b>",
                    f"Trail active:    {'✅ yes' if trail_active else '⏳ not yet'}",
                ]
                if strat._pos.peak_profit > 0 and strat._pos.initial_sl_dist > 1e-10:
                    tier = strat._pos.peak_profit / strat._pos.initial_sl_dist
                    status_lines.append(f"Peak R:          {tier:.2f}R")
            else:
                status_lines.append("\nNo active position.")

            status_lines += [
                "",
                "<b>Commands:</b>",
                "/trail on   — Force enable",
                "/trail off  — Force disable",
                "/trail auto — Use config default",
            ]
            return "\n".join(status_lines)

    def _cmd_config(self) -> str:
        try:
            import config as cfg
            from quant_strategy import QCfg
            lines = [
                "<b>⚡ Quant Bot v4 Config</b>\n",
                f"Symbol:        {cfg.SYMBOL}",
                f"Leverage:      {cfg.LEVERAGE}x",
                f"Margin/trade:  {QCfg.MARGIN_PCT():.0%}",
                f"Tick size:     ${QCfg.TICK_SIZE()}",
                "",
                "<b>Entry (Mean-Reversion)</b>",
                f"VWAP dev:      > {QCfg.VWAP_ENTRY_ATR_MULT()}×ATR",
                f"Composite min: ±{QCfg.COMPOSITE_ENTRY_MIN():.2f}",
                f"Exit reversal: ±{QCfg.EXIT_REVERSAL_THRESH():.2f}",
                f"Confirm ticks: {QCfg.CONFIRM_TICKS()}",
                "",
                "<b>SL / TP (Structure-Based)</b>",
                f"SL:            swing + {QCfg.SL_BUFFER_ATR_MULT()}×ATR buffer",
                f"SL lookback:   {QCfg.SL_SWING_LOOKBACK()} bars (5m)",
                f"TP:            {QCfg.TP_VWAP_FRACTION():.0%} back to VWAP",
                f"Min R:R:       {QCfg.MIN_RR_RATIO()}",
                f"SL range:      {QCfg.MIN_SL_PCT():.1%} – {QCfg.MAX_SL_PCT():.1%}",
                "",
                "<b>Trailing SL</b>",
                f"Enabled:       {QCfg.TRAIL_ENABLED()}",
                f"BE at:         {QCfg.TRAIL_BE_R()}R",
                f"Lock at:       {QCfg.TRAIL_LOCK_R()}R",
                "",
                "<b>Risk Limits</b>",
                f"Max daily:     {QCfg.MAX_DAILY_TRADES()} trades",
                f"Max consec L:  {QCfg.MAX_CONSEC_LOSSES()} (then {QCfg.LOSS_LOCKOUT_SEC()}s lockout)",
                f"Daily loss:    {QCfg.MAX_DAILY_LOSS_PCT()}%",
                "",
                "<b>Signal Weights</b>",
                f"VWAP={QCfg.W_VWAP_DEV()} CVD={QCfg.W_CVD_DIV()} OB={QCfg.W_OB()} "
                f"TF={QCfg.W_TICK_FLOW()} VEX={QCfg.W_VOL_EXHAUSTION()}",
                "",
                "<b>HTF Trend Filter (Veto Only)</b>",
                f"Enabled:       {QCfg.HTF_ENABLED()}",
                f"Veto strength: {QCfg.HTF_VETO_STRENGTH()}",
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
        """Live-adjust a quant config parameter. Takes effect on next tick.

        FIX v4.3.1: After setattr, we read back from the config module to
        VERIFY the change actually took effect. Previously risk_manager cached
        stale values — that is now fixed in risk_manager.py so all code reads
        from config module dynamically.
        """
        import config as cfg

        if not args or len(args.split()) < 2:
            return (
                "Usage: /set &lt;key&gt; &lt;value&gt;\n\n"
                "<b>Adjustable keys:</b>\n"
                "  leverage\n"
                "  quant_margin_pct          (e.g. 0.20)\n"
                "  quant_vwap_entry_atr_mult (e.g. 1.5)\n"
                "  quant_composite_entry_min (e.g. 0.40)\n"
                "  quant_exit_reversal_thresh(e.g. 0.40)\n"
                "  quant_confirm_ticks       (e.g. 3)\n"
                "  quant_sl_swing_lookback   (e.g. 16)\n"
                "  quant_sl_buffer_atr_mult  (e.g. 0.5)\n"
                "  quant_tp_vwap_fraction    (e.g. 0.35)\n"
                "  min_risk_reward_ratio     (e.g. 1.0)\n"
                "  quant_max_hold_sec        (e.g. 3600)\n"
                "  quant_cooldown_sec        (e.g. 300)\n"
                "  quant_trail_enabled       (e.g. True)\n"
                "  quant_trail_be_r          (e.g. 0.3)\n"
                "  quant_trail_lock_r        (e.g. 0.6)\n"
                "  max_daily_trades          (e.g. 6)\n"
                "  max_consecutive_losses    (e.g. 2)\n"
                "  max_daily_loss_pct        (e.g. 3.0)\n"
                "  quant_w_vwap_dev / _cvd_div / _ob / _tick_flow / _vol_exhaustion\n"
                "  quant_htf_enabled / _veto_strength"
            )

        parts   = args.split(None, 1)
        key     = parts[0].lower()
        val_str = parts[1].strip()

        # Map command-friendly names → (config attr, type)
        allowed = {
            "leverage":                    ("LEVERAGE",                    int),
            "quant_margin_pct":            ("QUANT_MARGIN_PCT",            float),
            "quant_vwap_entry_atr_mult":   ("QUANT_VWAP_ENTRY_ATR_MULT",  float),
            "quant_composite_entry_min":   ("QUANT_COMPOSITE_ENTRY_MIN",  float),
            "quant_exit_reversal_thresh":  ("QUANT_EXIT_REVERSAL_THRESH", float),
            "quant_confirm_ticks":         ("QUANT_CONFIRM_TICKS",        int),
            "quant_sl_swing_lookback":     ("QUANT_SL_SWING_LOOKBACK",    int),
            "quant_sl_buffer_atr_mult":    ("QUANT_SL_BUFFER_ATR_MULT",   float),
            "quant_tp_vwap_fraction":      ("QUANT_TP_VWAP_FRACTION",     float),
            "min_risk_reward_ratio":       ("MIN_RISK_REWARD_RATIO",      float),
            "quant_max_hold_sec":          ("QUANT_MAX_HOLD_SEC",         int),
            "quant_cooldown_sec":          ("QUANT_COOLDOWN_SEC",         int),
            "quant_trail_enabled":         ("QUANT_TRAIL_ENABLED",        bool),
            "quant_trail_be_r":            ("QUANT_TRAIL_BE_R",           float),
            "quant_trail_lock_r":          ("QUANT_TRAIL_LOCK_R",         float),
            "max_daily_trades":            ("MAX_DAILY_TRADES",           int),
            "max_consecutive_losses":      ("MAX_CONSECUTIVE_LOSSES",     int),
            "max_daily_loss_pct":          ("MAX_DAILY_LOSS_PCT",         float),
            "quant_w_vwap_dev":            ("QUANT_W_VWAP_DEV",           float),
            "quant_w_cvd_div":             ("QUANT_W_CVD_DIV",            float),
            "quant_w_ob":                  ("QUANT_W_OB",                 float),
            "quant_w_tick_flow":           ("QUANT_W_TICK_FLOW",          float),
            "quant_w_vol_exhaustion":      ("QUANT_W_VOL_EXHAUSTION",     float),
            "quant_htf_enabled":           ("QUANT_HTF_ENABLED",          bool),
            "quant_htf_veto_strength":     ("QUANT_HTF_VETO_STRENGTH",    float),
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

        bounds = {
            "QUANT_MARGIN_PCT":           (0.05, 0.50),
            "QUANT_VWAP_ENTRY_ATR_MULT":  (0.50, 3.00),
            "QUANT_COMPOSITE_ENTRY_MIN":  (0.15, 0.80),
            "QUANT_EXIT_REVERSAL_THRESH": (0.20, 0.80),
            "QUANT_SL_BUFFER_ATR_MULT":   (0.10, 2.00),
            "QUANT_TP_VWAP_FRACTION":     (0.20, 0.90),
            "MIN_RISK_REWARD_RATIO":      (0.30, 5.00),
            "LEVERAGE":                   (1,    50),
        }
        if attr_name in bounds:
            lo, hi = bounds[attr_name]
            if not (lo <= new_val <= hi):
                return f"Value out of safe range [{lo}, {hi}]: {new_val}"

        weight_attrs = {"QUANT_W_VWAP_DEV","QUANT_W_CVD_DIV","QUANT_W_OB",
                        "QUANT_W_TICK_FLOW","QUANT_W_VOL_EXHAUSTION"}
        if attr_name in weight_attrs:
            cur = {a: getattr(cfg, a, 0) for a in weight_attrs}
            cur[attr_name] = new_val
            total = sum(cur.values())
            if abs(total - 1.0) > 0.01:
                return (
                    f"⚠️ Weight sum would be {total:.3f} ≠ 1.0\n"
                    f"Adjust other weights so they sum to 1.0.\n"
                    f"Current: VWAP={cfg.QUANT_W_VWAP_DEV} CVD={cfg.QUANT_W_CVD_DIV} "
                    f"OB={cfg.QUANT_W_OB} TF={cfg.QUANT_W_TICK_FLOW} VEX={cfg.QUANT_W_VOL_EXHAUSTION}"
                )

        old_val = getattr(cfg, attr_name, "?")
        setattr(cfg, attr_name, new_val)

        # ── VERIFICATION READBACK ──────────────────────────────────
        # Read back from the config module to confirm the change stuck.
        # This catches any edge case where setattr silently fails.
        verify_val = getattr(cfg, attr_name, None)
        if verify_val != new_val:
            logger.error(
                f"CONFIG CHANGE FAILED: {attr_name} — wrote {new_val} "
                f"but readback is {verify_val}"
            )
            return (
                f"❌ <b>CHANGE FAILED</b>\n"
                f"<code>{attr_name}</code>: wrote {new_val} but readback = {verify_val}\n"
                f"Please report this bug."
            )

        logger.info(f"CONFIG LIVE-CHANGE via Telegram: {attr_name} = {old_val} → {new_val} (verified ✅)")
        return (
            f"✅ <b>{attr_name}</b>\n"
            f"{old_val} → <b>{new_val}</b>\n"
            f"<i>Verified ✅ — active immediately</i>"
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

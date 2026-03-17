"""
Telegram Bot Controller v12 — Rewritten for QuantStrategy v4.8
================================================================
All command handlers rebuilt against the actual API:
  - QuantStrategy   (quant_strategy.py)
  - RiskManager     (risk_manager.py)
  - ICTEngine       (ict_engine.py)
  - Config          (config.py)

Commands:
  /start          - Start trading bot
  /stop           - Stop trading bot
  /status         - Full bot status + ICT structures
  /thinking       - Live signal breakdown + what is blocking entry
  /structures     - Full ICT structure map with prices
  /position       - Current position details
  /trades         - Recent trade history
  /config         - Show current config values
  /pause          - Pause trading (keep monitoring)
  /resume         - Resume trading
  /balance        - Wallet balance
  /trail          - Toggle trailing SL on/off/auto
  /killswitch     - Emergency: close all positions + cancel orders
  /set <key> <val>- Live-adjust config (e.g. /set cooldown 120)
  /help           - Show commands
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
        self.bot_token      = telegram_config.TELEGRAM_BOT_TOKEN
        self.chat_id        = str(telegram_config.TELEGRAM_CHAT_ID)
        self.last_update_id = 0
        self.running        = False

        if not self.bot_token or not self.chat_id:
            raise ValueError("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be set")

        logger.info("TelegramBotController v12 initialized")

    # ================================================================
    # MESSAGING
    # ================================================================

    def send_message(self, message: str, parse_mode: str = "HTML") -> bool:
        """Send message with auto-chunking (Telegram 4096 char limit)."""
        try:
            if len(message) > 4000:
                chunks = []
                while message:
                    if len(message) <= 4000:
                        chunks.append(message)
                        break
                    split_at = message.rfind('\n', 0, 4000)
                    if split_at == -1:
                        split_at = 4000
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
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id":                  self.chat_id,
            "text":                     text,
            "disable_web_page_preview": True,
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code != 200:
            logger.error(f"Telegram API error {resp.status_code}: {resp.text[:200]}")
        return resp.status_code == 200

    def get_updates(self, timeout: int = 30) -> list:
        try:
            url = f"https://api.telegram.org/bot{self.bot_token}/getUpdates"
            params = {
                "offset":          self.last_update_id + 1,
                "timeout":         timeout,
                "allowed_updates": ["message"],
            }
            resp = requests.get(url, params=params, timeout=timeout + 5)
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
                logger.info(f"Cleared {len(updates)} old messages")
        except Exception as e:
            logger.error(f"Error clearing messages: {e}")

    def set_my_commands(self):
        try:
            url = f"https://api.telegram.org/bot{self.bot_token}/setMyCommands"
            commands = [
                {"command": "start",      "description": "Start trading bot"},
                {"command": "stop",       "description": "Stop trading bot"},
                {"command": "status",     "description": "Full status + ICT overview"},
                {"command": "thinking",   "description": "Live signal breakdown"},
                {"command": "structures", "description": "ICT structure map with prices"},
                {"command": "position",   "description": "Current position details"},
                {"command": "trades",     "description": "Recent trade history"},
                {"command": "balance",    "description": "Wallet balance"},
                {"command": "pause",      "description": "Pause trading"},
                {"command": "resume",     "description": "Resume trading"},
                {"command": "trail",      "description": "Toggle trailing SL on/off/auto"},
                {"command": "config",     "description": "Show config values"},
                {"command": "killswitch", "description": "Emergency close all"},
                {"command": "set",        "description": "Set config value live"},
                {"command": "help",       "description": "Show commands"},
            ]
            payload = {
                "commands":     commands,
                "scope":        {"type": "all_private_chats"},
                "language_code": "en",
            }
            resp = requests.post(url, json=payload, timeout=10)
            if resp.status_code != 200:
                logger.error(f"Command registration failed: {resp.text}")
            else:
                logger.info("Telegram commands registered")
        except Exception as e:
            logger.error(f"Error setting commands: {e}")

    # ================================================================
    # COMMAND ROUTING
    # ================================================================

    def _normalize_command(self, text: str) -> tuple:
        t = (text or "").strip()
        bare_cmds = {
            "start", "stop", "status", "thinking", "structures", "position",
            "trades", "config", "pause", "resume", "balance", "trail",
            "killswitch", "set", "help",
        }
        if not t.startswith("/"):
            parts = t.split(None, 1)
            cmd   = parts[0].lower()
            args  = parts[1] if len(parts) > 1 else ""
            if cmd in bare_cmds:
                return f"/{cmd}", args
            return t, ""
        parts = t.split(None, 1)
        return parts[0].lower(), parts[1] if len(parts) > 1 else ""

    def handle_command(self, raw_text: str) -> Optional[str]:
        global bot_instance, bot_thread, bot_running
        cmd, args = self._normalize_command(raw_text)
        try:
            if cmd in ("/help", "/commands"):
                return self._cmd_help()
            elif cmd == "/start":
                return self._cmd_start()
            elif cmd == "/stop":
                return self._cmd_stop()
            elif cmd == "/status":
                return self._cmd_status()
            elif cmd == "/thinking":
                return self._cmd_thinking()
            elif cmd == "/structures":
                return self._cmd_structures()
            elif cmd == "/position":
                return self._cmd_position()
            elif cmd == "/trades":
                return self._cmd_trades()
            elif cmd == "/balance":
                return self._cmd_balance()
            elif cmd == "/pause":
                return self._cmd_pause()
            elif cmd == "/resume":
                return self._cmd_resume()
            elif cmd == "/trail":
                return self._cmd_trail(args)
            elif cmd == "/config":
                return self._cmd_config()
            elif cmd == "/killswitch":
                return self._cmd_killswitch()
            elif cmd == "/set":
                return self._cmd_set(args)
            else:
                return f"Unknown command: {cmd}\n\n" + self._cmd_help()
        except Exception as e:
            logger.error(f"Command error [{cmd}]: {e}", exc_info=True)
            return f"❌ Error in {cmd}: {e}"

    # ================================================================
    # COMMAND IMPLEMENTATIONS
    # ================================================================

    def _cmd_help(self) -> str:
        return (
            "<b>Quant Bot v4.8 Commands</b>\n\n"
            "/status — Full status + ICT overview\n"
            "/thinking — Live signal breakdown + entry gates\n"
            "/structures — ICT structure map with prices\n"
            "/position — Current position details\n"
            "/trades — Recent trade history\n"
            "/balance — Wallet balance\n"
            "/pause — Pause trading (keep monitoring)\n"
            "/resume — Resume trading\n"
            "/trail [on|off|auto] — Toggle trailing SL\n"
            "/config — Show config values\n"
            "/set &lt;key&gt; &lt;value&gt; — Adjust config live\n"
            "/killswitch — Emergency: close position + cancel orders\n"
            "/start — Start bot\n"
            "/stop — Stop bot\n"
            "/help — This list"
        )

    def _cmd_start(self) -> str:
        global bot_instance, bot_thread, bot_running
        if bot_running and bot_thread and bot_thread.is_alive():
            return "Bot already running."
        logger.info("Starting bot from Telegram...")
        bot_thread = threading.Thread(target=self._run_bot_thread, daemon=True)
        bot_thread.start()
        time.sleep(2.0)
        if bot_thread.is_alive():
            return "⏳ Starting bot... Check /status in 30s."
        return "❌ Start failed. Check logs."

    def _cmd_stop(self) -> str:
        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return "Bot not running."
        logger.info("Stopping bot from Telegram...")
        bot_running = False
        if bot_instance:
            bot_instance.stop()
        return "🛑 Bot stopped."

    def _cmd_status(self) -> str:
        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return "Bot not running. Use /start"
        try:
            strat = bot_instance.strategy
            if not strat:
                return "Strategy not ready."
            # format_status_report() includes regime, ICT, PnL, active position
            report = strat.format_status_report()
            self.send_message(report)
            return None
        except Exception as e:
            logger.error(f"Status error: {e}", exc_info=True)
            return f"❌ Status error: {e}"

    def _cmd_thinking(self) -> str:
        """
        Live signal breakdown — shows exactly what the bot sees and what
        is blocking or allowing entry. Uses QuantStrategy's actual internals.
        """
        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return "Bot not running. Use /start"

        try:
            import config as cfg
            strat = bot_instance.strategy
            dm    = bot_instance.data_manager
            rm    = bot_instance.risk_manager

            if not strat or not dm or not rm:
                return "Components not ready."

            price = dm.get_last_price()
            now   = time.time()
            sig   = strat._last_sig   # SignalBreakdown — always populated after first tick

            # ── Header ────────────────────────────────────────────────────────
            lines = [f"<b>🧠 THINKING @ ${price:,.2f}</b>\n"]

            # ── Market context ────────────────────────────────────────────────
            regime_val = getattr(strat._regime, '_regime', None)
            regime_str = regime_val.value if regime_val else "UNKNOWN"
            conf       = getattr(strat._regime, '_confidence', 0.0)
            adx_val    = strat._adx.adx
            pdi        = strat._adx.plus_di
            mdi        = strat._adx.minus_di
            trend_dir  = strat._adx.trend_direction()
            atr        = strat._atr_5m.atr
            atr_pct    = strat._atr_5m.get_percentile()
            vwap       = strat._vwap.vwap
            dev_atr    = strat._vwap.deviation_atr
            htf_15m    = strat._htf.trend_15m
            htf_4h     = strat._htf.trend_4h
            htf_str    = ("BULLISH" if htf_4h >  0.3
                          else ("BEARISH" if htf_4h < -0.3 else "NEUTRAL"))

            ict    = strat._ict
            sess   = ict._session  if ict else "N/A"
            kz     = ict._killzone if ict else ""
            kz_str = f" [{kz}]" if kz else ""

            lines.append("<b>Market Context</b>")
            lines.append(f"  Price: ${price:,.2f}  VWAP: ${vwap:,.2f}  Dev: {dev_atr:+.2f}ATR")
            lines.append(f"  ATR(5m): ${atr:.1f}  ({atr_pct:.0%} pctile)")
            lines.append(f"  Regime: <b>{regime_str}</b>  (conf={conf:.0%})")
            lines.append(f"  ADX: {adx_val:.1f}  +DI: {pdi:.1f}  -DI: {mdi:.1f}  dir={trend_dir}")
            lines.append(f"  HTF: {htf_str}  (4h={htf_4h:+.2f}  15m={htf_15m:+.2f})")
            lines.append(f"  Session: {sess}{kz_str}")

            # ── Risk gate ─────────────────────────────────────────────────────
            lines.append("")
            can_ok, risk_reason = rm.can_trade()
            daily_trades = len(getattr(rm, 'daily_trades', []))
            max_trades   = getattr(rm, 'max_daily_trades',
                                   getattr(cfg, 'MAX_DAILY_TRADES', 8))
            consec_loss  = getattr(rm, 'consecutive_losses', 0)
            daily_pnl    = getattr(rm, 'daily_pnl', 0.0)

            if can_ok:
                lines.append(
                    f"✅ Risk gate: CLEAR  "
                    f"({daily_trades}/{max_trades} trades  "
                    f"consec_loss={consec_loss}  today=${daily_pnl:+.2f})")
            else:
                lines.append(f"🚫 Risk gate: <b>BLOCKED</b>  → {risk_reason}")

            # ── Cooldown ──────────────────────────────────────────────────────
            cooldown_sec = float(getattr(cfg, 'QUANT_COOLDOWN_SEC', 180))
            cd_remaining = max(0.0, cooldown_sec - (now - strat._last_exit_time))
            if cd_remaining > 0:
                lines.append(f"⏳ Trade cooldown: {cd_remaining:.0f}s remaining")
            else:
                lines.append(f"✅ Cooldown: ready")

            # ── Breakout state ────────────────────────────────────────────────
            bo = strat._breakout
            if bo.is_active:
                retest = "RETEST READY ✅" if bo.retest_ready else "waiting for pullback retest"
                lines.append(f"🚀 Breakout {bo.direction.upper()} active — {retest}")

            # ── Signal bars ───────────────────────────────────────────────────
            lines.append("")
            lines.append("<b>Signals (last eval)</b>")

            def bar(v, w=10):
                h = w // 2
                f = min(int(abs(v) * h + 0.5), h)
                return ("·" * h + "█" * f + "░" * (h - f)) if v >= 0 \
                    else ("░" * (h - f) + "█" * f + "·" * h)

            def sig_line(label, val):
                arrow = "▲" if val > 0.05 else ("▼" if val < -0.05 else "─")
                return f"  {label:<7} {bar(val)} {arrow} {val:+.3f}"

            lines.append(sig_line("VWAP",  sig.vwap_dev))
            lines.append(sig_line("CVD",   sig.cvd_div))
            lines.append(sig_line("OB",    sig.orderbook))
            lines.append(sig_line("TICK",  sig.tick_flow))
            lines.append(sig_line("VEX",   sig.vol_exhaust))
            if sig.ict_total > 0.01:
                lines.append(sig_line("ICT",   sig.ict_total))

            lines.append(f"  {'─' * 32}")
            thr      = getattr(cfg, 'QUANT_COMPOSITE_ENTRY_MIN', 0.30)
            c        = sig.composite
            side_lbl = (sig.reversion_side.upper()
                        if sig.reversion_side else "?")
            lines.append(f"  Σ = {c:+.4f}  (threshold ±{thr:.3f})  side={side_lbl}")
            lines.append(f"  Confirming: {sig.n_confirming}/{'6' if ict else '5'} signals")

            # ── Entry gates ───────────────────────────────────────────────────
            lines.append("")
            lines.append("<b>Entry Gates</b>")
            g_ext  = sig.overextended
            g_reg  = sig.regime_ok
            g_htf  = not sig.htf_veto
            g_conf = sig.n_confirming >= 3
            g_comp = abs(c) >= thr
            all_pass = g_ext and g_reg and g_htf and g_conf and g_comp

            entry_mult = getattr(cfg, 'QUANT_VWAP_ENTRY_ATR_MULT', 1.2)
            lines.append(
                f"  {'✅' if g_ext  else '❌'} Overextended   "
                f"({dev_atr:+.2f} ATR from VWAP  need ≥{entry_mult:.1f})")
            lines.append(
                f"  {'✅' if g_reg  else '❌'} ATR regime     "
                f"({atr_pct:.0%} pctile)")
            lines.append(
                f"  {'✅' if g_htf  else '❌'} HTF veto       "
                f"(4h={htf_4h:+.2f}  15m={htf_15m:+.2f})")
            lines.append(
                f"  {'✅' if g_conf else '❌'} Confluence     "
                f"({sig.n_confirming}/{'6' if ict else '5'} signals agree)")
            lines.append(
                f"  {'✅' if g_comp else '❌'} Composite      "
                f"({c:+.3f}  threshold ±{thr:.3f})")

            # ICT detail line
            if ict and ict._initialized:
                lines.append(
                    f"  🏛️ ICT Σ={sig.ict_total:.2f}  "
                    f"OB={sig.ict_ob:.2f}  FVG={sig.ict_fvg:.2f}  "
                    f"Sweep={sig.ict_sweep:.2f}  KZ={sig.ict_session:.2f}")
                if sig.ict_details:
                    lines.append(f"     → {sig.ict_details}")
            elif ict:
                nb = len(list(ict.order_blocks_bull))
                ns = len(list(ict.order_blocks_bear))
                lines.append(f"  🏛️ ICT: warming up ({nb}🟢 {ns}🔴 OBs so far)")

            # ── Verdict ───────────────────────────────────────────────────────
            lines.append("")
            if all_pass and can_ok and cd_remaining == 0:
                cn_long  = getattr(strat, '_confirm_long', 0)
                cn_short = getattr(strat, '_confirm_short', 0)
                cn_need  = getattr(cfg, 'QUANT_CONFIRM_TICKS', 2)
                lines.append(
                    f"🎯 <b>ALL PASS — confirming entry</b>  "
                    f"({max(cn_long, cn_short)}/{cn_need} ticks)")
            else:
                missing = []
                if not g_ext:
                    missing.append(
                        f"VWAP dev {abs(dev_atr):.2f}ATR < {entry_mult:.1f}ATR required")
                if not g_reg:
                    missing.append(f"ATR regime gate ({atr_pct:.0%} pctile)")
                if not g_htf:
                    veto_dir = "LONG" if htf_4h < 0 else "SHORT"
                    missing.append(f"HTF vetoing {veto_dir}")
                if not g_conf:
                    missing.append(
                        f"Only {sig.n_confirming}/{'6' if ict else '5'} signals agree (need 3)")
                if not g_comp:
                    missing.append(f"Composite {c:+.3f} < ±{thr:.3f}")
                if not can_ok:
                    missing.append(f"Risk gate: {risk_reason}")
                if cd_remaining > 0:
                    missing.append(f"Cooldown {cd_remaining:.0f}s")

                lines.append("👀 <b>Watching</b>")
                for m in missing:
                    lines.append(f"   • {m}")

            self.send_message("\n".join(lines))
            return None

        except Exception as e:
            logger.error(f"Thinking error: {e}", exc_info=True)
            return f"❌ Thinking error: {e}"

    def _cmd_structures(self) -> str:
        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return "Bot not running."

        try:
            strat = bot_instance.strategy
            dm    = bot_instance.data_manager
            if not strat or not dm:
                return "Components not ready."

            price  = dm.get_last_price()
            now_ms = int(time.time() * 1000)

            ict = getattr(strat, '_ict', None)
            if ict is None:
                return "❌ ICT engine not available (ict_engine.py not loaded)."
            if not ict._initialized:
                nb = len(list(ict.order_blocks_bull))
                ns = len(list(ict.order_blocks_bear))
                return (f"⏳ ICT engine warming up...\n"
                        f"Detected so far: {nb}🟢 {ns}🔴 OBs\n"
                        f"Need ≥10 candles + 5s update cycle.")

            atr_eng = getattr(strat, '_atr_5m', None)
            atr_v   = atr_eng.atr if atr_eng and atr_eng.atr > 1e-10 else price * 0.002

            st   = ict.get_full_status(price, atr_v, now_ms)
            c    = st["counts"]
            sess = st.get("session", "UNKNOWN")
            kz   = st.get("killzone", "")
            kz_s = f" [{kz}]" if kz else ""

            lines = [
                f"🏛️ <b>ICT Structures @ ${price:,.1f}</b>",
                f"Session: {sess}{kz_s}  |  ATR(5m): ${atr_v:.1f}",
                (f"OBs: {c['ob_bull']}🟢 {c['ob_bear']}🔴  "
                 f"FVGs: {c['fvg_bull']}🟦 {c['fvg_bear']}🟥  "
                 f"Liq: {c['liq_active']} active / {c['liq_swept']} swept"),
                "",
            ]

            # ── Bullish OBs ──────────────────────────────────────────────
            if st["bull_obs"]:
                lines.append("<b>🟢 Bullish Order Blocks</b>")
                for ob in st["bull_obs"][:5]:
                    ote_lo = ob["high"] - 0.79 * (ob["high"] - ob["low"])
                    ote_hi = ob["high"] - 0.50 * (ob["high"] - ob["low"])
                    tags   = " ".join(f"[{t}]" for t in ob["tags"]) if ob["tags"] else ""
                    in_tag = " ◄ IN OB" if ob["in_ob"] else (" ← OTE" if ob["in_ote"] else "")
                    bos    = " BOS✓" if ob["bos"] else ""
                    lines.append(
                        f"  ${ob['low']:,.1f} – ${ob['high']:,.1f}  "
                        f"mid=${ob['midpoint']:,.1f}  "
                        f"{ob['dist_pts']:+.0f}pts/{ob['dist_atr']:.2f}ATR\n"
                        f"    str={ob['strength']:.0f}  v={ob['visit_count']}  "
                        f"age={ob['age_min']:.0f}m  "
                        f"OTE:${ote_lo:,.0f}–${ote_hi:,.0f}"
                        f"{bos} {tags}{in_tag}"
                    )
            else:
                lines.append("🟢 No active bullish OBs")

            lines.append("")

            # ── Bearish OBs ──────────────────────────────────────────────
            if st["bear_obs"]:
                lines.append("<b>🔴 Bearish Order Blocks</b>")
                for ob in st["bear_obs"][:5]:
                    ote_lo = ob["low"] + 0.50 * (ob["high"] - ob["low"])
                    ote_hi = ob["low"] + 0.79 * (ob["high"] - ob["low"])
                    tags   = " ".join(f"[{t}]" for t in ob["tags"]) if ob["tags"] else ""
                    in_tag = " ◄ IN OB" if ob["in_ob"] else (" ← OTE" if ob["in_ote"] else "")
                    bos    = " BOS✓" if ob["bos"] else ""
                    lines.append(
                        f"  ${ob['low']:,.1f} – ${ob['high']:,.1f}  "
                        f"mid=${ob['midpoint']:,.1f}  "
                        f"{ob['dist_pts']:+.0f}pts/{ob['dist_atr']:.2f}ATR\n"
                        f"    str={ob['strength']:.0f}  v={ob['visit_count']}  "
                        f"age={ob['age_min']:.0f}m  "
                        f"OTE:${ote_lo:,.0f}–${ote_hi:,.0f}"
                        f"{bos} {tags}{in_tag}"
                    )
            else:
                lines.append("🔴 No active bearish OBs")

            lines.append("")

            # ── FVGs ─────────────────────────────────────────────────────
            all_fvgs = st["bull_fvgs"][:3] + st["bear_fvgs"][:3]
            if all_fvgs:
                lines.append("<b>Fair Value Gaps</b>")
                for fvg in sorted(all_fvgs, key=lambda x: abs(x["dist_pts"])):
                    icon   = "🟦" if fvg["direction"] == "bullish" else "🟥"
                    in_tag = " ◄ IN GAP" if fvg["in_gap"] else ""
                    lines.append(
                        f"  {icon} ${fvg['bottom']:,.1f}–${fvg['top']:,.1f}  "
                        f"size=${fvg['size']:.1f}  fill={fvg['fill_pct']:.0%}  "
                        f"{fvg['dist_pts']:+.0f}pts/{fvg['dist_atr']:.2f}ATR  "
                        f"age={fvg['age_min']:.0f}m{in_tag}"
                    )
            else:
                lines.append("No active FVGs")

            lines.append("")

            # ── Liquidity ────────────────────────────────────────────────
            nearby = [l for l in st["liq_active"] if abs(l["dist_pts"]) < 5.0 * atr_v]
            if nearby:
                lines.append("<b>💧 Liquidity Pools</b>")
                eqh = sorted(
                    [l for l in nearby if l["pool_type"] == "EQH"],
                    key=lambda x: x["price"], reverse=True)
                eql = sorted(
                    [l for l in nearby if l["pool_type"] == "EQL"],
                    key=lambda x: x["price"], reverse=True)
                for l in eqh[:4]:
                    lines.append(
                        f"  EQH ▲ ${l['price']:,.1f}  x{l['touch_count']}  "
                        f"{l['dist_pts']:+.0f}pts/{abs(l['dist_pts'])/atr_v:.2f}ATR")
                for l in eql[:4]:
                    lines.append(
                        f"  EQL ▼ ${l['price']:,.1f}  x{l['touch_count']}  "
                        f"{l['dist_pts']:+.0f}pts/{abs(l['dist_pts'])/atr_v:.2f}ATR")

            if st["liq_swept"]:
                lines.append("<b>🌊 Recent Sweeps</b>")
                for l in st["liq_swept"][:3]:
                    disp = "DISP✓" if l["displacement"] else "weak"
                    wick = " WR✓" if l["wick_rejection"] else ""
                    age  = (f"{l['sweep_age_min']:.0f}m ago"
                            if l["sweep_age_min"] is not None else "")
                    lines.append(
                        f"  {l['pool_type']} ${l['price']:,.1f}  [{disp}{wick}]  {age}")

            lines.append("")

            # ── Swing levels ─────────────────────────────────────────────
            sh = st["swing_highs"][:4]
            sl = st["swing_lows"][:4]
            if sh or sl:
                lines.append("<b>📌 Swing Levels</b>")
                if sh:
                    sh_str = "  ".join(f"${h:,.0f}(+{h-price:.0f})" for h in sh)
                    lines.append(f"  Highs ▲: {sh_str}")
                if sl:
                    sl_str = "  ".join(f"${l:,.0f}({l-price:.0f})" for l in sl)
                    lines.append(f"  Lows  ▼: {sl_str}")

            lines.append("")

            # ── Confluence scores both sides ──────────────────────────────
            long_c  = ict.get_confluence("long",  price, now_ms)
            short_c = ict.get_confluence("short", price, now_ms)
            lines.append("<b>Confluence Scores</b>")
            lines.append(
                f"  LONG  Σ={long_c.total:.2f}  "
                f"OB={long_c.ob_score:.2f}  FVG={long_c.fvg_score:.2f}  "
                f"Sweep={long_c.sweep_score:.2f}  KZ={long_c.session_score:.2f}")
            if long_c.details:
                lines.append(f"    → {long_c.details}")
            lines.append(
                f"  SHORT Σ={short_c.total:.2f}  "
                f"OB={short_c.ob_score:.2f}  FVG={short_c.fvg_score:.2f}  "
                f"Sweep={short_c.sweep_score:.2f}  KZ={short_c.session_score:.2f}")
            if short_c.details:
                lines.append(f"    → {short_c.details}")

            self.send_message("\n".join(lines))
            return None

        except Exception as e:
            logger.error(f"Structures error: {e}", exc_info=True)
            return f"❌ Structures error: {e}"

    def _cmd_position(self) -> str:
        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return "Bot not running."

        strat = bot_instance.strategy
        dm    = bot_instance.data_manager
        if not strat:
            return "Strategy not ready."

        pos = strat.get_position()
        if not pos:
            return "📭 No active position."

        # Access PositionState directly — these fields are guaranteed
        p     = strat._pos
        price = dm.get_last_price() if dm else 0.0
        atr   = strat._atr_5m.atr

        side    = p.side.upper()
        entry   = p.entry_price
        sl      = p.sl_price
        tp      = p.tp_price
        qty     = p.quantity
        mode    = p.trade_mode
        phase   = p.phase.name

        # Issue 1 FIX: R:R must use INITIAL SL distance (not current, which shrinks
        # as trail moves up). current_sl distance understates planned R:R.
        init_sl_dist = getattr(p, 'initial_sl_dist', 0.0)
        sl_dist = init_sl_dist if init_sl_dist > 1e-10 else (abs(entry - sl) if sl > 0 else 0.0)
        current_sl_dist = abs(entry - sl) if sl > 0 else 0.0

        if sl_dist > 1e-10:
            current_r = ((price - entry) / sl_dist if side == "LONG"
                         else (entry - price) / sl_dist)
            planned_rr = abs(tp - entry) / sl_dist if tp > 0 else 0.0
        else:
            current_r  = 0.0
            planned_rr = 0.0

        upnl = ((price - entry) * qty if side == "LONG"
                else (entry - price) * qty)

        hold_min     = (time.time() - p.entry_time) / 60.0 if p.entry_time > 0 else 0.0
        trail_state  = "✅ active" if p.trail_active else "⏳ waiting"
        peak_profit  = getattr(p, 'peak_profit', 0.0)
        init_sl_dist = getattr(p, 'initial_sl_dist', sl_dist)
        mfe_r        = peak_profit / init_sl_dist if init_sl_dist > 1e-10 else 0.0
        be_moved     = ((side == "LONG"  and sl >= entry) or
                        (side == "SHORT" and sl <= entry and sl > 0))

        # Entry signal context
        sig_lines = []
        if p.entry_signal:
            es = p.entry_signal
            sig_lines.append(
                f"\n<b>Entry signal:</b>  Σ={es.composite:+.3f}  "
                f"regime={es.market_regime}  ADX={es.adx:.1f}")
            if es.ict_total > 0.01:
                sig_lines.append(
                    f"ICT: {es.ict_total:.2f} [{es.ict_details}]")

        return (
            f"<b>Active Position — {side}</b>\n"
            f"Mode: {mode.upper()}  |  Phase: {phase}\n\n"
            f"Entry:   ${entry:,.2f}\n"
            f"Current: ${price:,.2f}  ({current_r:+.2f}R)\n"
            f"uPnL:    ${upnl:+,.2f}\n"
            f"Qty:     {qty:.4f} BTC\n\n"
            f"SL:      ${sl:,.2f}  "
            f"(init dist: ${sl_dist:.0f} / {sl_dist/max(atr,1):.2f}ATR  "
            f"current dist: ${current_sl_dist:.0f})\n"
            f"TP:      ${tp:,.2f}\n"
            f"Planned R:R: 1:{planned_rr:.2f}\n"
            f"MFE: {mfe_r:.2f}R\n\n"
            f"Hold:  {hold_min:.1f}m\n"
            f"Trail: {trail_state}\n"
            f"BE:    {'Yes ✅' if be_moved else 'No'}"
            + "".join(sig_lines)
        )

    def _cmd_trades(self) -> str:
        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return "Bot not running."

        strat = bot_instance.strategy
        rm    = bot_instance.risk_manager
        if not strat or not rm:
            return "Components not ready."

        # ── Source: strategy._trade_history (ground truth, set at close) ──
        history = getattr(strat, '_trade_history', [])

        lines = ["<b>📋 Trade History</b>\n"]

        if history:
            # Show last 10 trades, newest first
            for t in reversed(history[-10:]):
                side    = t.get('side', '?').upper()
                mode    = t.get('mode', '?').upper()
                entry   = t.get('entry', 0.0)
                exit_p  = t.get('exit',  0.0)
                pnl     = t.get('pnl',   0.0)
                reason  = t.get('reason', '?')
                hold    = t.get('hold_min', 0.0)
                mfe_r   = t.get('mfe_r', 0.0)
                trailed = t.get('trailed', False)
                is_win  = t.get('is_win', False)
                init_sl = t.get('init_sl_dist', 0.0)
                raw_pts = ((exit_p - entry) if side == "LONG" else (entry - exit_p))
                ach_r   = raw_pts / init_sl if init_sl > 1e-10 else 0.0

                # Determine label
                if reason == "tp_hit":
                    label = "🎯 TP"
                elif reason == "trail_sl_hit":
                    label = "🔒 TRAIL"
                elif reason == "sl_hit":
                    label = "🛑 SL"
                else:
                    label = f"🚪 {reason[:8]}"

                result = "✅" if is_win else "❌"
                trail_tag = " [T]" if trailed else ""
                lines.append(
                    f"{result} {side} [{mode}]  "
                    f"${entry:,.0f}→${exit_p:,.0f}  "
                    f"PnL: <b>${pnl:+.2f}</b>  "
                    f"R: {ach_r:+.2f}  MFE: {mfe_r:.1f}R\n"
                    f"    {label}{trail_tag}  hold: {hold:.0f}m"
                )
        else:
            lines.append("  No trades recorded yet this session.")

        # ── Summary stats from strategy (ground truth) ──────────────────
        total_t = getattr(strat, '_total_trades', 0)
        wins    = getattr(strat, '_winning_trades', 0)
        losses  = total_t - wins
        wr      = wins / total_t * 100.0 if total_t > 0 else 0.0
        total_pnl = getattr(strat, '_total_pnl', 0.0)

        # Daily stats from risk gate (authoritative daily counters)
        daily_cnt  = strat._risk_gate.daily_trades if hasattr(strat, '_risk_gate') else 0
        consec     = strat._risk_gate.consec_losses if hasattr(strat, '_risk_gate') else 0
        max_d      = getattr(__import__('config'), 'MAX_DAILY_TRADES', 8)

        # Avg win / avg loss from history
        win_pnls  = [t['pnl'] for t in history if t.get('is_win')]
        loss_pnls = [t['pnl'] for t in history if not t.get('is_win')]
        avg_win   = sum(win_pnls)  / len(win_pnls)  if win_pnls  else 0.0
        avg_loss  = sum(loss_pnls) / len(loss_pnls) if loss_pnls else 0.0
        expectancy = (wr/100 * avg_win) + ((1 - wr/100) * avg_loss)

        lines += [
            "",
            "━━━━━━━━━━━━━━━━━━━━━━━━",
            f"Session:   {total_t} trades  W:{wins} L:{losses}  WR: <b>{wr:.0f}%</b>",
            f"Total PnL: <b>${total_pnl:+.2f}</b> USDT",
            f"Avg Win:   ${avg_win:+.2f}  Avg Loss: ${avg_loss:+.2f}",
            f"Expectancy: ${expectancy:+.2f}/trade",
            f"Today:     {daily_cnt}/{max_d} trades  consec_loss={consec}",
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
            bal = rm.get_available_balance()
            if not bal:
                return "Could not fetch balance."
            avail    = float(bal.get("available", 0))
            total    = float(bal.get("total", avail))
            locked   = total - avail
            dpnl     = getattr(rm, 'daily_pnl', 0.0)
            init_bal = getattr(rm, 'initial_balance', 0.0)
            dpct     = (dpnl / init_bal * 100.0 if init_bal > 1e-10 else 0.0)
            return (
                f"<b>Wallet Balance</b>\n"
                f"Available: <b>${avail:,.2f}</b> USDT\n"
                f"Locked:    ${locked:,.2f} USDT\n"
                f"Total:     ${total:,.2f} USDT\n\n"
                f"Today's PnL: ${dpnl:+,.2f} ({dpct:+.2f}%)"
            )
        except Exception as e:
            return f"Balance error: {e}"

    def _cmd_pause(self) -> str:
        global bot_instance
        if not bot_instance:
            return "Bot not running."
        bot_instance.trading_enabled      = False
        bot_instance.trading_pause_reason = "Paused via Telegram"
        return "⏸️ Trading PAUSED. Monitoring continues. Use /resume to re-enable."

    def _cmd_resume(self) -> str:
        global bot_instance
        if not bot_instance:
            return "Bot not running."
        bot_instance.trading_enabled      = True
        bot_instance.trading_pause_reason = ""
        return "▶️ Trading RESUMED."

    def _cmd_trail(self, args: str) -> str:
        global bot_instance
        if not bot_instance:
            return "Bot not running."
        strat = bot_instance.strategy
        if not strat:
            return "Strategy not ready."
        arg = (args or "").strip().lower()
        if arg in ("on", "enable", "1", "true", "yes"):
            strat.set_trail_override(True)
            return "🔒 Trailing SL: FORCED ON"
        elif arg in ("off", "disable", "0", "false", "no"):
            strat.set_trail_override(False)
            return "🔓 Trailing SL: FORCED OFF"
        else:
            strat.set_trail_override(None)
            enabled = strat.get_trail_enabled()
            return (f"🔄 Trailing SL: AUTO  "
                    f"(config default = {'ON' if enabled else 'OFF'})")

    def _cmd_config(self) -> str:
        import config as cfg
        lines = [
            "<b>Bot Configuration</b>\n",
            f"Symbol:     {cfg.SYMBOL}",
            f"Exchange:   {cfg.EXCHANGE}",
            f"Leverage:   {cfg.LEVERAGE}x",
            "",
            "<b>Position Sizing</b>",
            f"  Margin/trade: {getattr(cfg,'QUANT_MARGIN_PCT',0.20):.0%} of available balance",
            f"  Min margin:   ${getattr(cfg,'MIN_MARGIN_PER_TRADE',4.0):.2f} USDT",
            f"  Max position: {getattr(cfg,'MAX_POSITION_SIZE',1.0)} BTC",
            "",
            "<b>Entry</b>",
            f"  VWAP ATR mult:    {getattr(cfg,'QUANT_VWAP_ENTRY_ATR_MULT',1.2)}×ATR",
            f"  Composite min:    ±{getattr(cfg,'QUANT_COMPOSITE_ENTRY_MIN',0.30)}",
            f"  Confirm ticks:    {getattr(cfg,'QUANT_CONFIRM_TICKS',2)}",
            f"  Cooldown:         {getattr(cfg,'QUANT_COOLDOWN_SEC',180)}s",
            f"  Max hold:         {getattr(cfg,'QUANT_MAX_HOLD_SEC',2400)}s",
            "",
            "<b>Risk</b>",
            f"  Min SL dist:      {getattr(cfg,'MIN_SL_DISTANCE_PCT',0.003)*100:.2f}%",
            f"  Max SL dist:      {getattr(cfg,'MAX_SL_DISTANCE_PCT',0.035)*100:.2f}%",
            f"  SL ATR buffer:    {getattr(cfg,'QUANT_SL_BUFFER_ATR_MULT',0.4)}×ATR",
            f"  Min R:R:          {getattr(cfg,'MIN_RISK_REWARD_RATIO',0.8)}",
            f"  Max daily trades: {getattr(cfg,'MAX_DAILY_TRADES',8)}",
            f"  Max daily loss:   {getattr(cfg,'MAX_DAILY_LOSS_PCT',5.0):.1f}%",
            f"  Max consec loss:  {getattr(cfg,'MAX_CONSECUTIVE_LOSSES',3)}",
            "",
            "<b>Trail</b>",
            f"  Enabled:    {getattr(cfg,'QUANT_TRAIL_ENABLED',True)}",
            f"  BE (0→1):   {getattr(cfg,'QUANT_TRAIL_BE_R',0.3)}R",
            f"  Lock (1→2): {getattr(cfg,'QUANT_TRAIL_LOCK_R',0.8)}R",
            f"  Aggr (2→3): {getattr(cfg,'QUANT_TRAIL_AGGRESSIVE_R',1.5)}R",
        ]
        return "\n".join(lines)

    def _cmd_killswitch(self) -> str:
        global bot_instance
        if not bot_instance:
            return "Bot not running."
        try:
            import config
            bot_instance.trading_enabled = False
            om      = bot_instance.order_manager
            results = []

            if not om:
                return "❌ Order manager not available."

            # 1. Cancel all open orders
            try:
                resp = om.api.cancel_all_orders(
                    exchange=config.EXCHANGE, symbol=config.SYMBOL)
                if isinstance(resp, dict) and resp.get("error"):
                    results.append(f"⚠️ Cancel: {resp['error']}")
                else:
                    results.append("✅ All orders cancelled")
            except Exception as e:
                results.append(f"⚠️ Cancel error: {e}")

            # 2. Close open position
            try:
                pos = om.get_open_position()
                if pos and float(pos.get("size", 0)) > 0:
                    pos_side   = str(pos.get("side", "")).upper()
                    close_side = "SELL" if pos_side == "LONG" else "BUY"
                    qty        = float(pos["size"])
                    resp       = om.api.place_order(
                        symbol=config.SYMBOL,
                        side=close_side,
                        order_type="MARKET",
                        quantity=qty,
                        exchange=config.EXCHANGE,
                        reduce_only=True,
                    )
                    if isinstance(resp, dict) and resp.get("error"):
                        results.append(f"⚠️ Close: {resp['error']}")
                    else:
                        results.append(f"✅ Closed {pos_side} ({qty} BTC)")
                else:
                    results.append("ℹ️ No open position on exchange")
            except Exception as e:
                results.append(f"⚠️ Close error: {e}")

            # 3. Reset strategy state to FLAT
            strat = bot_instance.strategy
            if strat:
                try:
                    from quant_strategy import PositionState
                    with strat._lock:
                        strat._pos = PositionState()
                        strat._confirm_long = strat._confirm_short = 0
                        strat._confirm_trend_long = strat._confirm_trend_short = 0
                    results.append("✅ Strategy reset to FLAT")
                except Exception as e:
                    results.append(f"⚠️ State reset: {e}")

            result_str = "\n".join(f"  {r}" for r in results)
            return (
                f"🚨 <b>KILLSWITCH ACTIVATED</b>\n\n"
                f"{result_str}\n\n"
                f"Trading is PAUSED. Use /resume to re-enable."
            )
        except Exception as e:
            logger.error(f"Killswitch error: {e}", exc_info=True)
            return f"❌ Killswitch error: {e}"

    def _cmd_set(self, args: str) -> str:
        """
        Live-adjust a config parameter.
        For LEVERAGE: also calls the exchange API to set it live and blocks
        the change if a position is currently open (wrong leverage on open
        position is dangerous).
        """
        import config as cfg

        if not args or len(args.split()) < 2:
            return (
                "Usage: /set &lt;key&gt; &lt;value&gt;\n\n"
                "<b>Adjustable:</b>\n"
                "  leverage          int   (e.g. 20)\n"
                "  margin            float (e.g. 0.15)\n"
                "  cooldown          int   seconds\n"
                "  max_daily_trades  int\n"
                "  max_daily_loss    float %\n"
                "  max_consec_loss   int\n"
                "  min_rr            float\n"
                "  composite_min     float (e.g. 0.30)\n"
                "  trail_enabled     bool  (true/false)\n"
                "  max_hold          int   seconds\n"
                "  vwap_mult         float (e.g. 1.2)\n"
                "  sl_buffer         float (e.g. 0.4)\n"
            )

        parts   = args.split(None, 1)
        key     = parts[0].lower().strip()
        val_str = parts[1].strip()

        allowed = {
            "leverage":         ("LEVERAGE",                  int),
            "margin":           ("QUANT_MARGIN_PCT",          float),
            "cooldown":         ("QUANT_COOLDOWN_SEC",        int),
            "max_daily_trades": ("MAX_DAILY_TRADES",          int),
            "max_daily_loss":   ("MAX_DAILY_LOSS_PCT",        float),
            "max_consec_loss":  ("MAX_CONSECUTIVE_LOSSES",    int),
            "min_rr":           ("MIN_RISK_REWARD_RATIO",     float),
            "composite_min":    ("QUANT_COMPOSITE_ENTRY_MIN", float),
            "trail_enabled":    ("QUANT_TRAIL_ENABLED",       bool),
            "max_hold":         ("QUANT_MAX_HOLD_SEC",        int),
            "vwap_mult":        ("QUANT_VWAP_ENTRY_ATR_MULT", float),
            "sl_buffer":        ("QUANT_SL_BUFFER_ATR_MULT",  float),
        }

        if key not in allowed:
            return (f"Unknown key: <code>{key}</code>\n"
                    f"Allowed: {', '.join(sorted(allowed.keys()))}")

        attr_name, val_type = allowed[key]
        try:
            new_val = (val_str.lower() in ("true", "1", "yes", "on")
                       if val_type == bool else val_type(val_str))
        except ValueError:
            return f"Invalid value: <code>{val_str}</code> (expected {val_type.__name__})"

        old_val = getattr(cfg, attr_name, "?")

        # ── Leverage: exchange API call required ──────────────────────────────
        if key == "leverage":
            global bot_instance, bot_running

            # Refuse if a position is open — changing leverage mid-trade is
            # dangerous: the sizing already used the old leverage for qty calc.
            if bot_running and bot_instance and bot_instance.strategy:
                pos = bot_instance.strategy.get_position()
                if pos:
                    return (
                        f"❌ Cannot change leverage while position is open.\n"
                        f"Close position first, then /set leverage {new_val}."
                    )

            # Apply to config first so the bot uses it for the next sizing call
            setattr(cfg, attr_name, new_val)
            logger.info(f"CONFIG via Telegram: {attr_name} {old_val} → {new_val}")

            # Call the exchange API to set it live
            if bot_running and bot_instance:
                om = getattr(bot_instance, 'order_manager', None)
                if om and hasattr(om, 'api'):
                    try:
                        resp = om.api.set_leverage(
                            symbol   = cfg.SYMBOL,
                            exchange = cfg.EXCHANGE,
                            leverage = int(new_val),
                        )
                        if isinstance(resp, dict) and resp.get("error"):
                            # Exchange rejected — revert config to avoid mismatch
                            setattr(cfg, attr_name, old_val)
                            return (
                                f"❌ Exchange rejected leverage change: {resp['error']}\n"
                                f"Config reverted to {old_val}x."
                            )
                        logger.info(f"Exchange leverage set to {new_val}x: {resp}")
                        return (
                            f"✅ <b>Leverage updated</b>: {old_val}x → <b>{new_val}x</b>\n"
                            f"Config and exchange both updated."
                        )
                    except Exception as e:
                        # Revert config so it stays in sync with exchange
                        setattr(cfg, attr_name, old_val)
                        logger.error(f"set_leverage API error: {e}")
                        return (
                            f"❌ Exchange API error: {e}\n"
                            f"Config reverted to {old_val}x."
                        )
                else:
                    return (
                        f"⚠️ Config updated to {new_val}x but order manager "
                        f"not available — exchange NOT updated.\n"
                        f"Restart bot to apply leverage on exchange."
                    )
            else:
                # Bot not running — config-only change is fine
                return f"✅ <b>LEVERAGE</b>: {old_val} → <b>{new_val}</b>  (bot not running — exchange not updated)"

        # ── All other keys: config-only ───────────────────────────────────────
        setattr(cfg, attr_name, new_val)
        logger.info(f"CONFIG via Telegram: {attr_name} {old_val} → {new_val}")
        return f"✅ <b>{attr_name}</b>: {old_val} → <b>{new_val}</b>"

    # ================================================================
    # BOT THREAD
    # ================================================================

    def _run_bot_thread(self):
        global bot_instance, bot_running
        try:
            bot_running  = True
            from main import QuantBot
            bot_instance = QuantBot()
            if not bot_instance.initialize():
                self.send_message("❌ Bot init failed. Check logs.")
                bot_running = False
                return
            if not bot_instance.start():
                self.send_message("❌ Bot start failed. Check logs.")
                bot_running = False
                return
            bot_instance.run()
        except Exception as e:
            logger.error(f"Bot crashed: {e}", exc_info=True)
            self.send_message(f"❌ Bot crashed: {e}")
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
            "⚡ <b>Quant Bot v4.8 Controller Ready</b>\n\n" + self._cmd_help())
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
        import config
        controller = TelegramBotController()
        controller.start()
    except KeyboardInterrupt:
        logger.info("Shutdown requested")
    except Exception as e:
        logger.error(f"Fatal: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

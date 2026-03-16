"""
Telegram Bot Controller v11 — Comprehensive Bot Control
=========================================================
Commands:
  /start          - Start trading bot
  /stop           - Stop trading bot
  /status         - Full bot status + market overview
  /structures     - Deep ICT structure analysis
  /position       - Current position details
  /trades         - Recent trade history
  /config         - Show current config values
  /pause          - Pause trading (keep monitoring)
  /resume         - Resume trading
  /balance        - Wallet balance
  /killswitch     - Emergency: close all positions + cancel orders
  /set <key> <val>- Live-adjust config (e.g. /set leverage 20)
  /help           - Show commands
"""

import logging
import time
import threading
import requests
from typing import Optional
from datetime import datetime, timezone
import sys
import traceback

import telegram_config

logger = logging.getLogger(__name__)

bot_instance = None
bot_thread = None
bot_running = False


class TelegramBotController:
    def __init__(self):
        self.bot_token = telegram_config.TELEGRAM_BOT_TOKEN
        self.chat_id = str(telegram_config.TELEGRAM_CHAT_ID)
        self.last_update_id = 0
        self.running = False

        if not self.bot_token or not self.chat_id:
            raise ValueError("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be set")

        logger.info("TelegramBotController v11 initialized")

    # ================================================================
    # MESSAGING
    # ================================================================

    def send_message(self, message: str, parse_mode: str = "HTML") -> bool:
        """Send message with auto-truncation and error handling."""
        try:
            # Telegram message limit is 4096 chars
            if len(message) > 4000:
                # Split into chunks
                chunks = []
                while message:
                    if len(message) <= 4000:
                        chunks.append(message)
                        break
                    # Find a good split point
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
            # Fallback: try without parse_mode
            try:
                return self._send_raw(message, parse_mode=None)
            except Exception:
                return False

    def _send_raw(self, text: str, parse_mode: Optional[str] = "HTML") -> bool:
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
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
                "offset": self.last_update_id + 1,
                "timeout": timeout,
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
                {"command": "start", "description": "Start trading bot"},
                {"command": "stop", "description": "Stop trading bot"},
                {"command": "status", "description": "Full status + market overview"},
                {"command": "thinking", "description": "Deep analysis: what bot plans next"},
                {"command": "structures", "description": "ICT structure analysis"},
                {"command": "position", "description": "Current position details"},
                {"command": "trades", "description": "Recent trade history"},
                {"command": "balance", "description": "Wallet balance"},
                {"command": "pause", "description": "Pause trading"},
                {"command": "resume", "description": "Resume trading"},
                {"command": "config", "description": "Show config values"},
                {"command": "killswitch", "description": "Emergency close all"},
                {"command": "set", "description": "Set config value"},
                {"command": "help", "description": "Show commands"},
            ]

            payload = {
                "commands": commands,
                "scope": {"type": "all_private_chats"},
                "language_code": "en"
            }

            resp = requests.post(url, json=payload, timeout=10)

            if resp.status_code != 200:
                logger.error(f"Telegram command registration failed: {resp.text}")
            else:
                logger.info("Telegram commands registered successfully")

        except Exception as e:
            logger.error(f"Error setting commands: {e}")

    # ================================================================
    # COMMAND ROUTING
    # ================================================================

    def _normalize_command(self, text: str) -> tuple:
        """Parse command and arguments. Returns (command, args_string)."""
        t = (text or "").strip()
        if not t.startswith("/"):
            # Accept bare commands
            parts = t.split(None, 1)
            cmd = parts[0].lower()
            args = parts[1] if len(parts) > 1 else ""
            if cmd in ("start", "stop", "status", "thinking", "structures", "position",
                        "trades", "config", "pause", "resume", "balance",
                        "killswitch", "set", "help"):
                return f"/{cmd}", args
            return t, ""
        parts = t.split(None, 1)
        return parts[0].lower(), parts[1] if len(parts) > 1 else ""

    def handle_command(self, raw_text: str) -> Optional[str]:
        """Route command to handler. Returns response text or None (if sent directly)."""
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
            return f"Error: {e}"

    # ================================================================
    # COMMAND IMPLEMENTATIONS
    # ================================================================

    def _cmd_help(self) -> str:
        return (
            "<b>Quant Bot v4.8 Commands</b>\n\n"
            "/start — Start trading bot\n"
            "/stop — Stop trading bot\n"
            "/status — Full status + market\n"
            "/thinking — Deep analysis: what bot plans next\n"
            "/structures — ICT structure map\n"
            "/position — Current position\n"
            "/trades — Recent trade history\n"
            "/balance — Wallet balance\n"
            "/pause — Pause trading\n"
            "/resume — Resume trading\n"
            "/config — Show config\n"
            "/set &lt;key&gt; &lt;val&gt; — Adjust config\n"
            "/killswitch — Emergency close all\n"
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
            return "Starting bot... Check /status in 30s."
        return "Start failed. Check logs."

    def _cmd_stop(self) -> str:
        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return "Bot not running."
        logger.info("Stopping bot from Telegram...")
        bot_running = False
        if bot_instance:
            bot_instance.stop()
        return "Bot stopped."

    def _cmd_status(self) -> str:
        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return "Bot not running. Use /start"

        try:
            bot = bot_instance
            strat = bot.strategy
            dm = bot.data_manager
            rm = bot.risk_manager

            if not strat or not dm or not rm:
                return "Bot components not ready yet."

            price = dm.get_last_price()
            bal = rm.get_available_balance()
            bal_val = float(bal.get("available", 0)) if bal else 0.0
            stats = strat.get_strategy_stats()

            pos = strat.get_position()
            pos_line = ""
            if pos:
                side = pos.get("side", "?").upper()
                entry = strat.initial_entry_price or 0
                sl = strat.current_sl_price or 0
                tp = strat.current_tp_price or 0
                upnl = pos.get("unrealized_pnl", 0)
                pos_line = (
                    f"\n<b>POSITION</b>\n"
                    f"  {side} Entry: ${entry:,.2f}\n"
                    f"  SL: ${sl:,.2f} | TP: ${tp:,.2f}\n"
                    f"  uPnL: ${upnl:+,.2f}\n"
                    f"  BE: {'YES' if strat.breakeven_moved else 'NO'}"
                )

            # Regime info
            rs = strat.regime_engine.state
            regime_line = f"Regime: {rs.regime} (ADX {rs.adx:.1f})"

            # Session info
            session_line = (
                f"Session: {strat.current_session}"
                f"{' | KZ' if strat.in_killzone else ''}"
                f" | AMD: {strat.amd_phase}"
            )

            msg = (
                f"<b>Quant Bot v4.8 Status</b>\n"
                f"{'=' * 30}\n"
                f"Price: <b>${price:,.2f}</b>\n"
                f"Balance: <b>${bal_val:,.2f}</b> USDT\n"
                f"State: <b>{strat.state}</b>\n\n"
                f"HTF: <b>{strat.htf_bias}</b> ({strat.htf_bias_strength:.0%})\n"
                f"Daily: {strat.daily_bias}\n"
                f"{regime_line}\n"
                f"{session_line}\n\n"
                f"<b>Performance</b>\n"
                f"  Trades: {stats.get('total_exits', 0)}\n"
                f"  Win Rate: {stats.get('win_rate_pct', 0):.1f}%\n"
                f"  Daily P&L: ${stats.get('daily_pnl', 0):+,.2f}\n"
                f"  Total P&L: ${stats.get('total_pnl', 0):+,.2f}\n"
                f"  Consec Losses: {stats.get('consecutive_losses', 0)}\n\n"
                f"<b>Structures</b>\n"
                f"  OBs: {stats.get('bull_obs', 0)}B / {stats.get('bear_obs', 0)}S\n"
                f"  FVGs: {stats.get('bull_fvgs', 0)}B / {stats.get('bear_fvgs', 0)}S\n"
                f"  Liquidity: {stats.get('liq_pools', 0)} pools\n"
                f"  Swings: {stats.get('swing_highs', 0)}H / {stats.get('swing_lows', 0)}L\n"
                f"  MSS: {stats.get('ms_count', 0)}"
                f"{pos_line}"
            )

            self.send_message(msg)
            return None

        except Exception as e:
            logger.error(f"Status error: {e}", exc_info=True)
            return f"Status error: {e}"

    def _cmd_thinking(self) -> str:
        """
        Deep analysis: what the bot sees, what it plans, and what's blocking it.
        Runs a full dry-run of the entry evaluation for both sides.
        """
        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return "Bot not running. Use /start"

        try:
            import config as cfg
            bot = bot_instance
            strat = bot.strategy
            dm = bot.data_manager
            rm = bot.risk_manager

            if not strat or not dm or not rm:
                return "Bot components not ready yet."

            price = dm.get_last_price()
            now_ms = int(time.time() * 1000)

            lines = [f"<b>🧠 BOT THINKING @ ${price:,.1f}</b>\n"]

            # ── 1. Market context ────────────────────────────────
            rs = strat.regime_engine.state
            lines.append(f"<b>Context</b>")
            lines.append(f"  HTF: <b>{strat.htf_bias}</b> ({strat.htf_bias_strength:.0%})")
            lines.append(f"  Daily: {strat.daily_bias}")
            lines.append(f"  Regime: {rs.regime} (ADX {rs.adx:.1f}, ATR×{rs.atr_ratio:.2f})")
            lines.append(f"  Session: {strat.current_session}"
                         f"{' | KZ' if strat.in_killzone else ''}"
                         f" | AMD: {strat.amd_phase}")
            lines.append(f"  State: <b>{strat.state}</b>")

            # ── Dealing ranges ──────────────────────────────────
            ndr = strat._ndr
            if ndr.daily:
                d = ndr.daily
                zone = d.zone_pct(price)
                zone_label = "PREMIUM" if d.is_premium(price) else ("DISCOUNT" if d.is_discount(price) else "EQ")
                lines.append(f"  DR Daily: ${d.low:,.0f}–${d.high:,.0f} ({zone_label} {zone*100:.0f}%)")
            if ndr.intraday:
                i = ndr.intraday
                lines.append(f"  DR Intra: ${i.low:,.0f}–${i.high:,.0f}")

            # ── Range-bound mode ────────────────────────────────
            rb_active = strat._is_range_bound_mode()
            if rb_active:
                rb_dr = strat._get_range_bound_dr()
                if rb_dr:
                    z = rb_dr.zone_pct(price)
                    lines.append(f"\n📊 <b>RANGE-BOUND MODE ACTIVE</b>")
                    lines.append(f"  DR: ${rb_dr.low:,.0f}–${rb_dr.high:,.0f} (zone {z*100:.0f}%)")
                    lines.append(f"  Long zone: below {cfg.RANGE_BOUND_DISCOUNT_ENTRY*100:.0f}% ({'✅ YES' if z <= cfg.RANGE_BOUND_DISCOUNT_ENTRY else '❌ NO'})")
                    lines.append(f"  Short zone: above {cfg.RANGE_BOUND_PREMIUM_ENTRY*100:.0f}% ({'✅ YES' if z >= cfg.RANGE_BOUND_PREMIUM_ENTRY else '❌ NO'})")
                    lines.append(f"  Trades today: {strat._range_bound_daily_trades}/{cfg.RANGE_BOUND_MAX_DAILY_TRADES}")
            elif strat.htf_bias == "NEUTRAL":
                lines.append(f"\n📊 Range-bound: INACTIVE")
                if rs.adx > cfg.RANGE_BOUND_MAX_ADX:
                    lines.append(f"  ❌ ADX {rs.adx:.1f} > {cfg.RANGE_BOUND_MAX_ADX} (too directional)")
                rb_dr = strat._get_range_bound_dr()
                if rb_dr is None:
                    lines.append(f"  ❌ No valid DR within size bounds")

            # ── 2. Risk manager gate ────────────────────────────
            can_trade, risk_reason = rm.can_trade()
            if can_trade:
                lines.append(f"\n✅ Risk gate: CLEAR")
            else:
                lines.append(f"\n🚫 Risk gate: <b>BLOCKED</b>")
                lines.append(f"  {risk_reason}")

            # ── 3. Trade plans (dry-run both sides) ──────────────
            plans = {}  # store for "What needs to change?" section below
            for side in ["long", "short"]:
                plan = strat._build_trade_plan(side, price, dm, now_ms)
                plans[side] = plan
                status = plan.get("status", "?")
                label = side.upper()
                rb_tag = " [RANGE]" if plan.get("range_bound") else ""

                lines.append(f"\n{'─' * 25}")
                if status == "READY":
                    lines.append(f"🎯 <b>{label}{rb_tag}: READY</b>")
                    lines.append(f"  Entry: ${plan.get('entry', 0):,.1f}")
                    lines.append(f"  SL: ${plan.get('sl', 0):,.1f} ({plan.get('sl_reason', '?')})")
                    lines.append(f"  TP: ${plan.get('tp', 0):,.1f} ({plan.get('tp_reason', '?')})")
                    lines.append(f"  RR: {plan.get('rr', 0):.1f}x")
                    lines.append(f"  Score: {plan.get('score', 0):.0f}/{plan.get('threshold', 0):.0f}")
                    if plan.get("reasons"):
                        top = plan["reasons"][:5]
                        lines.append(f"  Reasons: {', '.join(top)}")
                else:
                    icon = "⛔"
                    lines.append(f"{icon} <b>{label}{rb_tag}: {status}</b>")
                    gate = plan.get("gate_failed", "unknown")
                    lines.append(f"  Reason: {gate}")

                    # Show what's missing
                    if plan.get("missing"):
                        lines.append(f"  Need: {plan['missing']}")

                    # Show partial score if we got that far
                    if plan.get("score"):
                        threshold = plan.get("threshold", 0)
                        score = plan.get("score", 0)
                        lines.append(f"  Score: {score:.0f}"
                                     + (f"/{threshold:.0f}" if threshold else ""))
                        if plan.get("reasons"):
                            top = plan["reasons"][:4]
                            lines.append(f"  {', '.join(top)}")

            # ── 4. What needs to change ──────────────────────────
            lines.append(f"\n{'─' * 25}")
            lines.append(f"<b>What needs to change?</b>")

            added = False

            # State-level blockers
            if strat.state == "POSITION_ACTIVE":
                lines.append("• Position already active — managing SL/TP")
                added = True
            elif strat.state == "ENTRY_PENDING":
                lines.append("• Entry order pending fill")
                added = True

            # Risk gate
            if not can_trade:
                lines.append(f"• Risk gate blocked: {risk_reason}")
                added = True

            # Bias/regime context
            if strat.htf_bias == "NEUTRAL":
                if rb_active:
                    lines.append("• Price must reach DR extremes (discount for long, premium for short)")
                else:
                    lines.append("• HTF bias must turn directional (BULLISH or BEARISH)")
                    lines.append(f"  OR ADX must drop below {getattr(cfg, 'RANGE_BOUND_MAX_ADX', 22)} for range-bound mode")
                added = True

            # Per-side plan blockers — the most useful part
            for s, plan in plans.items():
                status = plan.get("status", "")
                if status == "READY":
                    continue  # nothing to report for ready side
                label = s.upper()
                gate = plan.get("gate_failed") or ""
                missing = plan.get("missing") or ""
                score = plan.get("score", 0)
                threshold = plan.get("threshold", 0)

                if gate or missing:
                    lines.append(f"\n{label} needs:")
                    if gate:
                        lines.append(f"  • {gate}")
                    if missing:
                        lines.append(f"  • {missing}")
                    if score and threshold and score < threshold:
                        lines.append(f"  • Score {score:.0f}/{threshold:.0f} — needs {threshold - score:.0f} more pts")
                    added = True

            # Nearest structure zones
            se = getattr(strat, 'structure_engine', None)
            if se:
                zone_lines = []
                for s in ["long", "short"]:
                    zone = se.get_best_entry_zone(s, price, now_ms)
                    if zone:
                        z_lo, z_hi, z_type = zone
                        dist = abs(price - (z_lo + z_hi) / 2) / price * 100
                        zone_lines.append(f"• Nearest {s.upper()} zone: {z_type} ${z_lo:,.0f}–${z_hi:,.0f} ({dist:.2f}% away)")
                if zone_lines:
                    lines.append("")
                    lines.extend(zone_lines)
                    added = True

            if not added:
                lines.append("• No specific blockers identified — waiting for structure")

            self.send_message("\n".join(lines))
            return None

        except Exception as e:
            logger.error(f"Thinking error: {e}", exc_info=True)
            return f"Error: {e}"

    def _cmd_structures(self) -> str:
        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return "Bot not running."

        try:
            bot  = bot_instance
            strat = bot.strategy
            dm    = bot.data_manager
            if not strat or not dm:
                return "Components not ready."

            price  = dm.get_last_price()
            now_ms = int(time.time() * 1000)

            # ICT engine lives at strat._ict (ICTEngine instance)
            ict = getattr(strat, '_ict', None)
            if ict is None:
                return "❌ ICT engine not available (ict_engine.py not loaded)."
            if not ict._initialized:
                nb = len(list(ict.order_blocks_bull))
                ns = len(list(ict.order_blocks_bear))
                return (f"⏳ ICT engine warming up...\n"
                        f"Detected so far: {nb}🟢 {ns}🔴 OBs\n"
                        f"Need ≥10 candles and 5s update cycle to complete.")

            atr   = getattr(strat, '_atr_5m', None)
            atr_v = atr.atr if atr else 0.0
            if atr_v < 1e-10:
                atr_v = price * 0.002   # rough 0.2% fallback for display only

            st    = ict.get_full_status(price, atr_v, now_ms)
            c     = st["counts"]
            sess  = st.get("session", "UNKNOWN")
            kz    = st.get("killzone", "")
            kz_str = f" [{kz}]" if kz else ""

            lines = [
                f"🏛️ <b>ICT Structures @ ${price:,.1f}</b>",
                f"Session: {sess}{kz_str}  |  ATR(5m): ${atr_v:.1f}",
                f"OBs: {c['ob_bull']}🟢 {c['ob_bear']}🔴  "
                f"FVGs: {c['fvg_bull']}🟦 {c['fvg_bear']}🟥  "
                f"Liq: {c['liq_active']} active / {c['liq_swept']} swept",
                ""
            ]

            # ── BULLISH ORDER BLOCKS ──────────────────────────────────────
            if st["bull_obs"]:
                lines.append("<b>🟢 Bullish Order Blocks</b>")
                for ob in st["bull_obs"][:6]:
                    ote_zone_lo = ob["high"] - 0.79 * (ob["high"] - ob["low"])
                    ote_zone_hi = ob["high"] - 0.50 * (ob["high"] - ob["low"])
                    tags = " ".join(f"[{t}]" for t in ob["tags"]) if ob["tags"] else ""
                    in_tag = " ◄ PRICE IN OB" if ob["in_ob"] else (" ← OTE ZONE" if ob["in_ote"] else "")
                    bos = " BOS✓" if ob["bos"] else ""
                    lines.append(
                        f"  ${ob['low']:,.1f} ─ ${ob['high']:,.1f}  "
                        f"mid=${ob['midpoint']:,.1f}  "
                        f"dist={ob['dist_pts']:+.1f}pts ({ob['dist_atr']:.2f}ATR)\n"
                        f"    str={ob['strength']:.0f}  visits={ob['visit_count']}  "
                        f"age={ob['age_min']:.0f}m  "
                        f"OTE: ${ote_zone_lo:,.1f}─${ote_zone_hi:,.1f}"
                        f"{bos} {tags}{in_tag}"
                    )
            else:
                lines.append("🟢 No active bullish OBs")

            lines.append("")

            # ── BEARISH ORDER BLOCKS ──────────────────────────────────────
            if st["bear_obs"]:
                lines.append("<b>🔴 Bearish Order Blocks</b>")
                for ob in st["bear_obs"][:6]:
                    ote_zone_lo = ob["low"] + 0.50 * (ob["high"] - ob["low"])
                    ote_zone_hi = ob["low"] + 0.79 * (ob["high"] - ob["low"])
                    tags = " ".join(f"[{t}]" for t in ob["tags"]) if ob["tags"] else ""
                    in_tag = " ◄ PRICE IN OB" if ob["in_ob"] else (" ← OTE ZONE" if ob["in_ote"] else "")
                    bos = " BOS✓" if ob["bos"] else ""
                    lines.append(
                        f"  ${ob['low']:,.1f} ─ ${ob['high']:,.1f}  "
                        f"mid=${ob['midpoint']:,.1f}  "
                        f"dist={ob['dist_pts']:+.1f}pts ({ob['dist_atr']:.2f}ATR)\n"
                        f"    str={ob['strength']:.0f}  visits={ob['visit_count']}  "
                        f"age={ob['age_min']:.0f}m  "
                        f"OTE: ${ote_zone_lo:,.1f}─${ote_zone_hi:,.1f}"
                        f"{bos} {tags}{in_tag}"
                    )
            else:
                lines.append("🔴 No active bearish OBs")

            lines.append("")

            # ── FAIR VALUE GAPS ───────────────────────────────────────────
            all_fvgs = st["bull_fvgs"][:3] + st["bear_fvgs"][:3]
            if all_fvgs:
                lines.append("<b>Fair Value Gaps</b>")
                for fvg in sorted(all_fvgs, key=lambda x: abs(x["dist_pts"])):
                    icon = "🟦" if fvg["direction"] == "bullish" else "🟥"
                    in_tag = " ◄ PRICE IN GAP" if fvg["in_gap"] else ""
                    lines.append(
                        f"  {icon} ${fvg['bottom']:,.1f} ─ ${fvg['top']:,.1f}  "
                        f"size=${fvg['size']:.1f}  fill={fvg['fill_pct']:.0%}  "
                        f"dist={fvg['dist_pts']:+.1f}pts ({fvg['dist_atr']:.2f}ATR)  "
                        f"age={fvg['age_min']:.0f}m{in_tag}"
                    )
            else:
                lines.append("No active FVGs near price")

            lines.append("")

            # ── LIQUIDITY POOLS ───────────────────────────────────────────
            # Show all within 5×ATR
            nearby = [l for l in st["liq_active"] if abs(l["dist_pts"]) < 5.0 * atr_v]
            if nearby:
                lines.append("<b>💧 Liquidity Pools</b>")
                eqh = sorted([l for l in nearby if l["pool_type"] == "EQH"], key=lambda x: x["price"], reverse=True)
                eql = sorted([l for l in nearby if l["pool_type"] == "EQL"], key=lambda x: x["price"], reverse=True)
                for l in eqh[:4]:
                    dist_atr = abs(l["dist_pts"]) / atr_v
                    lines.append(
                        f"  EQH ▲ ${l['price']:,.1f}  "
                        f"x{l['touch_count']}  dist={l['dist_pts']:+.1f}pts ({dist_atr:.2f}ATR)")
                for l in eql[:4]:
                    dist_atr = abs(l["dist_pts"]) / atr_v
                    lines.append(
                        f"  EQL ▼ ${l['price']:,.1f}  "
                        f"x{l['touch_count']}  dist={l['dist_pts']:+.1f}pts ({dist_atr:.2f}ATR)")

            # Recent sweeps
            if st["liq_swept"]:
                lines.append("<b>🌊 Recent Sweeps</b>")
                for l in st["liq_swept"][:4]:
                    disp = "DISP✓" if l["displacement"] else "weak"
                    wick = "WR✓" if l["wick_rejection"] else ""
                    age  = f"{l['sweep_age_min']:.0f}m ago" if l["sweep_age_min"] is not None else ""
                    lines.append(
                        f"  {l['pool_type']} ${l['price']:,.1f}  [{disp} {wick}]  {age}")

            lines.append("")

            # ── SWING LEVELS ──────────────────────────────────────────────
            sh_above = st["swing_highs"][:5]
            sl_below = st["swing_lows"][:5]
            if sh_above or sl_below:
                lines.append("<b>📌 Swing Levels</b>")
                if sh_above:
                    sh_str = "  ".join(
                        f"${h:,.1f}(+{h-price:.0f})" for h in sh_above)
                    lines.append(f"  Highs ▲: {sh_str}")
                if sl_below:
                    sl_str = "  ".join(
                        f"${l:,.1f}({l-price:.0f})" for l in sl_below)
                    lines.append(f"  Lows  ▼: {sl_str}")

            # ── ICT CONFLUENCE SCORE ──────────────────────────────────────
            lines.append("")
            # Score for both sides
            long_conf  = ict.get_confluence("long",  price, now_ms)
            short_conf = ict.get_confluence("short", price, now_ms)
            lines.append("<b>Confluence Scores</b>")
            lines.append(
                f"  LONG  Σ={long_conf.total:.2f}  "
                f"OB={long_conf.ob_score:.2f}  FVG={long_conf.fvg_score:.2f}  "
                f"Sweep={long_conf.sweep_score:.2f}  KZ={long_conf.session_score:.2f}")
            if long_conf.details:
                lines.append(f"    → {long_conf.details}")
            lines.append(
                f"  SHORT Σ={short_conf.total:.2f}  "
                f"OB={short_conf.ob_score:.2f}  FVG={short_conf.fvg_score:.2f}  "
                f"Sweep={short_conf.sweep_score:.2f}  KZ={short_conf.session_score:.2f}")
            if short_conf.details:
                lines.append(f"    → {short_conf.details}")

            self.send_message("\n".join(lines))
            return None

        except Exception as e:
            logger.error(f"Structures error: {e}", exc_info=True)
            return f"❌ Error: {e}"

    def _cmd_position(self) -> str:
        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return "Bot not running."
        strat = bot_instance.strategy
        if not strat:
            return "Strategy not ready."

        pos = strat.get_position()
        if not pos:
            return "No active position."

        side = pos.get("side", "?").upper()
        entry = strat.initial_entry_price or 0
        sl = strat.current_sl_price or 0
        tp = strat.current_tp_price or 0
        qty = strat.entry_quantity
        upnl = pos.get("unrealized_pnl", 0)
        price = bot_instance.data_manager.get_last_price() if bot_instance.data_manager else 0

        risk = abs(entry - sl) if entry and sl else 0
        if risk > 0 and entry:
            current_r = (price - entry) / risk if side == "LONG" else (entry - price) / risk
        else:
            current_r = 0

        msg = (
            f"<b>Active Position</b>\n"
            f"Side: <b>{side}</b>\n"
            f"Entry: ${entry:,.2f}\n"
            f"Current: ${price:,.2f}\n"
            f"Qty: {qty:.4f} BTC\n\n"
            f"SL: ${sl:,.2f}\n"
            f"TP: ${tp:,.2f}\n"
            f"Current R: {current_r:+.2f}R\n"
            f"uPnL: ${upnl:+,.2f}\n\n"
            f"BE Moved: {'Yes' if strat.breakeven_moved else 'No'}\n"
            f"MFE: {strat.max_favorable_excursion:.2f}R\n"
            f"MAE: {strat.max_adverse_excursion:.2f}R\n"
            f"Score: {strat.entry_score:.0f}"
        )
        return msg

    def _cmd_trades(self) -> str:
        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return "Bot not running."
        rm = bot_instance.risk_manager
        if not rm:
            return "Risk manager not ready."

        history = getattr(rm, 'trade_history', [])
        if not history:
            return "No trades recorded yet."

        lines = ["<b>Recent Trades</b>\n"]
        # deque doesn't support slicing — convert to list first
        recent = list(history)[-5:]
        for trade in recent:
            # TradeRecord is a dataclass, use getattr not .get()
            side   = getattr(trade, "side", "?").upper()
            pnl    = getattr(trade, "pnl", 0)
            reason = getattr(trade, "reason", "?")
            icon   = "✅" if pnl >= 0 else "❌"
            lines.append(f"  {icon} {side} P&L: ${pnl:+,.2f} [{reason}]")

        stats = bot_instance.strategy.get_strategy_stats() if bot_instance.strategy else {}
        lines.append(f"\nTotal: {stats.get('total_exits', 0)} trades")
        lines.append(f"Win Rate: {stats.get('win_rate_pct', 0):.1f}%")
        lines.append(f"Total P&L: ${stats.get('total_pnl', 0):+,.2f}")
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
            avail = float(bal.get("available", 0))
            total = float(bal.get("total", avail))
            locked = total - avail
            return (
                f"<b>Wallet Balance</b>\n"
                f"Available: <b>${avail:,.2f}</b> USDT\n"
                f"Locked: ${locked:,.2f} USDT\n"
                f"Total: ${total:,.2f} USDT"
            )
        except Exception as e:
            return f"Balance error: {e}"

    def _cmd_pause(self) -> str:
        global bot_instance
        if not bot_instance:
            return "Bot not running."
        bot_instance.trading_enabled = False
        bot_instance.trading_pause_reason = "Paused via Telegram"
        return "Trading PAUSED. Monitoring continues. Use /resume to re-enable."

    def _cmd_resume(self) -> str:
        global bot_instance
        if not bot_instance:
            return "Bot not running."
        bot_instance.trading_enabled = True
        bot_instance.trading_pause_reason = ""
        return "Trading RESUMED."

    def _cmd_config(self) -> str:
        import config as cfg
        lines = [
            "<b>Bot Configuration</b>\n",
            f"Symbol: {cfg.SYMBOL}",
            f"Leverage: {cfg.LEVERAGE}x",
            f"Risk/Trade: {cfg.RISK_PER_TRADE}%",
            f"Max Daily Loss: ${cfg.MAX_DAILY_LOSS}",
            f"Max Consec Losses: {cfg.MAX_CONSECUTIVE_LOSSES}",
            f"Max Daily Trades: {cfg.MAX_DAILY_TRADES}",
            f"Min RR: {cfg.MIN_RISK_REWARD_RATIO}x",
            f"Entry Threshold (KZ): {cfg.ENTRY_THRESHOLD_KILLZONE}",
            f"Entry Threshold (Reg): {cfg.ENTRY_THRESHOLD_REGULAR}",
            f"Min SL Dist: {cfg.MIN_SL_DISTANCE_PCT*100:.1f}%",
            f"Max SL Dist: {cfg.MAX_SL_DISTANCE_PCT*100:.1f}%",
            f"SL ATR Buffer: {cfg.SL_ATR_BUFFER_MULT}x",
            f"Cooldown: {cfg.TRADE_COOLDOWN_SECONDS}s",
            f"Balance Usage: {cfg.BALANCE_USAGE_PERCENTAGE}%",
        ]
        return "\n".join(lines)

    def _cmd_killswitch(self) -> str:
        global bot_instance
        if not bot_instance:
            return "Bot not running."

        try:
            import config
            # Pause trading first
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

            # Close any open position
            try:
                pos = om.get_open_position()
                if pos and pos.get("size", 0) > 0:
                    side = "SELL" if pos["side"].upper() == "LONG" else "BUY"
                    om.api.place_order(
                        symbol=config.SYMBOL,
                        side=side,
                        order_type="MARKET",
                        quantity=pos["size"],
                        reduce_only=True
                    )
                    logger.warning(f"KILLSWITCH: Closed {pos['side']} position, size={pos['size']}")
            except Exception as e:
                logger.error(f"Killswitch close error: {e}")

            # Reset strategy state
            if bot_instance.strategy:
                bot_instance.strategy._reset_position_state()

            return (
                "KILLSWITCH ACTIVATED\n"
                "- All orders cancelled\n"
                "- Position closed (if any)\n"
                "- Trading PAUSED\n\n"
                "Use /resume to re-enable trading."
            )
        except Exception as e:
            return f"Killswitch error: {e}"

    def _cmd_set(self, args: str) -> str:
        """Live-adjust a config parameter."""
        import config as cfg

        if not args or len(args.split()) < 2:
            return (
                "Usage: /set &lt;key&gt; &lt;value&gt;\n\n"
                "Adjustable keys:\n"
                "  leverage, risk_per_trade, max_daily_loss,\n"
                "  entry_threshold_killzone, entry_threshold_regular,\n"
                "  min_rr, cooldown_seconds, max_daily_trades"
            )

        parts = args.split(None, 1)
        key = parts[0].lower()
        val_str = parts[1].strip()

        # Allowed keys with their config attribute names and types
        allowed = {
            "leverage":                   ("LEVERAGE", int),
            "risk_per_trade":             ("RISK_PER_TRADE", float),
            "max_daily_loss":             ("MAX_DAILY_LOSS", float),
            "entry_threshold_killzone":   ("ENTRY_THRESHOLD_KILLZONE", int),
            "entry_threshold_regular":    ("ENTRY_THRESHOLD_REGULAR", int),
            "min_rr":                     ("MIN_RISK_REWARD_RATIO", float),
            "cooldown_seconds":           ("TRADE_COOLDOWN_SECONDS", int),
            "max_daily_trades":           ("MAX_DAILY_TRADES", int),
        }

        if key not in allowed:
            return f"Unknown key: {key}\nAllowed: {', '.join(allowed.keys())}"

        attr_name, val_type = allowed[key]
        try:
            new_val = val_type(val_str)
        except ValueError:
            return f"Invalid value: {val_str} (expected {val_type.__name__})"

        old_val = getattr(cfg, attr_name, "?")
        setattr(cfg, attr_name, new_val)

        logger.info(f"CONFIG CHANGED via Telegram: {attr_name} = {old_val} -> {new_val}")
        return f"Set <b>{attr_name}</b>: {old_val} -> <b>{new_val}</b>"

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
                self.send_message("Bot init failed.")
                bot_running = False
                return

            if not bot_instance.start():
                self.send_message("Bot start failed.")
                bot_running = False
                return

            bot_instance.run()

        except Exception as e:
            logger.error(f"Bot crashed: {e}", exc_info=True)
            self.send_message(f"Bot crashed: {e}")
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
        self.send_message("Quant Bot v4.8 Controller Ready.\n\n" + self._cmd_help())

        logger.info("Telegram controller started")

        while self.running:
            try:
                updates = self.get_updates(timeout=30)
                for upd in updates:
                    self.last_update_id = upd.get("update_id", self.last_update_id)
                    msg = upd.get("message") or {}
                    chat_id = str((msg.get("chat") or {}).get("id", ""))
                    text = (msg.get("text") or "").strip()

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
        import config  # ensure config is importable
        controller = TelegramBotController()
        controller.start()
    except KeyboardInterrupt:
        logger.info("Shutdown requested")
    except Exception as e:
        logger.error(f"Fatal: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

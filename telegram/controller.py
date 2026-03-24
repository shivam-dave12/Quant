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
  /setexchange    - Switch execution exchange (delta|coinswitch)
  /set <key> <val>- Live-adjust config (e.g. /set cooldown 120)
  /help           - Show commands
"""

import logging
import time
import threading
import requests
import html as _html
from typing import Optional
from datetime import datetime, timezone
import sys

import sys, os as _os; sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
import telegram.config as telegram_config
import config
from telegram.notifier import _sanitize_html

logger = logging.getLogger(__name__)

def _esc(s) -> str:
    """Escape <, >, & in dynamic strings before embedding in Telegram HTML."""
    if s is None:
        return ""
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
                    else:
                        chunks.append(message[:split_at])
                        # Skip the newline itself so the next chunk has no leading blank line
                        message = message[split_at + 1:]
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
        # Sanitize before every send so controller-built messages are treated
        # identically to notifier-queue messages — strips unsupported tags and
        # escapes bare < / > that would cause 400 "Unsupported start tag" errors.
        if parse_mode == "HTML":
            text = _sanitize_html(text)
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
        # Bug-21 fix: cap the effective long-poll read timeout at 15 s (down from
        # timeout+5=35 s).  The old value meant stop() couldn't interrupt an
        # in-flight poll for up to 35 seconds — dangerous when shutting down with
        # an open position.  A 15-second read window still gives reliable delivery
        # for all Telegram use cases; the poll repeats immediately on return.
        _read_timeout = min(timeout, 15) + 2   # 2 s margin above poll interval
        try:
            url = f"https://api.telegram.org/bot{self.bot_token}/getUpdates"
            params = {
                "offset":          self.last_update_id + 1,
                "timeout":         min(timeout, 15),   # match read_timeout cap
                "allowed_updates": ["message"],
            }
            resp = requests.get(url, params=params,
                                timeout=(5.0, _read_timeout))
            # Bug-22 fix: log non-200 responses instead of silently returning [].
            if resp.status_code != 200:
                logger.warning("Telegram API HTTP %d — getUpdates skipped",
                               resp.status_code)
                return []
            data = resp.json()
            return data.get("result", []) if data.get("ok") else []
        except requests.exceptions.Timeout:
            # Normal for a long-poll that expires with no messages — not an error.
            return []
        except requests.exceptions.ConnectionError as e:
            # Bug-22 fix: network blip — warn so operator can see Telegram is down.
            logger.warning("Telegram connection error (will retry): %s", e)
            return []
        except ValueError as e:
            # Bug-22 fix: malformed JSON — log with context so it's diagnosable.
            logger.error("Telegram JSON parse error in getUpdates: %s", e)
            return []
        except Exception as e:
            # Bug-22 fix: catch-all still returns [] but now logs the failure so
            # prolonged Telegram outages are visible in the bot log.
            logger.error("Telegram getUpdates unexpected error: %s", e,
                         exc_info=True)
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
                {"command": "stats",      "description": "Signal attribution analysis"},
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
            "trades", "stats", "config", "pause", "resume", "balance", "trail",
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
            elif cmd == "/stats":
                return self._cmd_stats()
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
            elif cmd == "/setexchange":
                return self._cmd_setexchange(args)
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
            "/stats — Signal attribution analysis (tier/regime WR breakdown)\n"
            "/balance — Wallet balance\n"
            "/pause — Pause trading (keep monitoring)\n"
            "/resume — Resume trading\n"
            "/trail [on|off|auto] — Toggle trailing SL\n"
            "/config — Show config values\n"
            "/set &lt;key&gt; &lt;value&gt; — Adjust config live\n"
            "/setexchange &lt;delta|coinswitch&gt; — Switch execution exchange at runtime\n"
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
        Live signal breakdown — shows the full decision stack:
          Market context → HTF → AMD → Sweep → ICT tier → Quant gates → Verdict
          Plus trail state when a position is active.
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

            price  = dm.get_last_price()
            now    = time.time()
            now_ms = int(now * 1000)
            sig    = strat._last_sig
            atr    = strat._atr_5m.atr
            atr_pct = strat._atr_5m.get_percentile()
            vwap   = strat._vwap.vwap
            # dev_atr is SIGNED — use abs() for gate comparison, keep sign for display
            dev_atr = strat._vwap.deviation_atr
            htf_15m = strat._htf.trend_15m
            htf_4h  = strat._htf.trend_4h
            ict     = strat._ict
            pos     = strat._pos

            lines = [f"<b>🧠 THINKING @ ${price:,.2f}</b>"]

            # ══════════════════════════════════════════════════════════
            # 1. MARKET CONTEXT
            # ══════════════════════════════════════════════════════════
            regime_val = getattr(strat._regime, '_regime', None)
            regime_str = regime_val.value if regime_val else "UNKNOWN"
            conf       = getattr(strat._regime, '_confidence', 0.0)
            adx_val    = strat._adx.adx
            pdi        = strat._adx.plus_di
            mdi        = strat._adx.minus_di

            sess = ict._session  if ict else "N/A"
            kz   = ict._killzone if ict else ""
            kz_s = f" [{kz.upper()}]" if kz else " [off-session]"

            lines.append("\n<b>━ Market Context</b>")
            lines.append(f"  Price:  ${price:,.2f}  VWAP: ${vwap:,.2f}  Dev: {dev_atr:+.2f}ATR")
            lines.append(f"  ATR(5m): ${atr:.1f}  ({atr_pct:.0%} pctile)")
            lines.append(f"  Regime: <b>{_esc(regime_str)}</b>  conf={conf:.0%}"
                         f"  ADX={adx_val:.1f}  +DI={pdi:.1f}  -DI={mdi:.1f}")
            lines.append(f"  Session: {_esc(sess)}{_esc(kz_s)}")

            # ══════════════════════════════════════════════════════════
            # 2. HTF STRUCTURE
            # ══════════════════════════════════════════════════════════
            # vetoes_trade fires when:
            #   SHORT: t15m > +0.35 (15m bullish vetoes SHORT)
            #   LONG:  t15m < -0.35 (15m bearish vetoes LONG)
            # The direction being VETOED is determined by t15m sign, not t4h sign.
            veto_15m  = float(getattr(cfg, 'QUANT_HTF_15M_VETO',  0.35))
            veto_both = float(getattr(cfg, 'QUANT_HTF_BOTH_VETO', 0.20))
            veto_short = (htf_15m > veto_15m or
                          (htf_15m > veto_both and htf_4h > veto_both))
            veto_long  = (htf_15m < -veto_15m or
                          (htf_15m < -veto_both and htf_4h < -veto_both))

            if veto_short and not veto_long:
                htf_verdict = f"❌ VETOING SHORT  (15m={htf_15m:+.2f} &gt;{veto_15m:+.2f})"
            elif veto_long and not veto_short:
                htf_verdict = f"❌ VETOING LONG   (15m={htf_15m:+.2f} &lt;{-veto_15m:+.2f})"
            elif veto_short and veto_long:
                htf_verdict = f"❌ VETOING BOTH   (15m={htf_15m:+.2f} 4h={htf_4h:+.2f})"
            else:
                htf_verdict = f"✅ No veto"

            lines.append("\n<b>━ HTF Structure</b>")
            lines.append(f"  4H:  {htf_4h:+.2f}  15m: {htf_15m:+.2f}")
            lines.append(f"  {htf_verdict}")
            lines.append(f"  Source: {'ICT swing structure' if strat._htf.ict_source else 'EMA fallback'}")

            # ══════════════════════════════════════════════════════════
            # 3. AMD PHASE  (full ICT layer — was missing entirely)
            # ══════════════════════════════════════════════════════════
            lines.append("\n<b>━ AMD Cycle</b>")
            if ict and ict._initialized:
                try:
                    amd       = ict._amd
                    amd_phase = amd.phase
                    amd_conf  = amd.confidence
                    amd_bias  = amd.bias
                    amd_icons = {"DISTRIBUTION": "🎯", "MANIPULATION": "⚡",
                                 "REACCUMULATION": "🔄", "REDISTRIBUTION": "🔄",
                                 "ACCUMULATION": "💤"}
                    amd_icon  = amd_icons.get(amd_phase, "❓")
                    bias_icon = ("🔴" if amd_bias == "bearish"
                                 else ("🟢" if amd_bias == "bullish" else "⚪"))
                    lines.append(
                        f"  {amd_icon} <b>{_esc(amd_phase)}</b>  {bias_icon}{_esc(amd_bias)}  "
                        f"conf={amd_conf:.2f}")

                    # MTF alignment
                    mtf_aligned = getattr(sig, 'mtf_aligned', False)
                    in_disc     = getattr(sig, 'in_discount', False)
                    in_prem     = getattr(sig, 'in_premium',  False)
                    zone_str    = ("💰DISCOUNT" if in_disc
                                   else ("💸PREMIUM" if in_prem else "〰️EQUILIBRIUM"))
                    mtf_str     = "✅ALIGNED" if mtf_aligned else "❌SPLIT"
                    lines.append(f"  MTF: {mtf_str}  Price zone: {zone_str}")
                    if sig.mtf_details:
                        lines.append(f"  {_esc(sig.mtf_details)}")

                    # AMD phase interpretation for entry — thresholds MUST match ICTEntryGate exactly.
                    # Hard block threshold is 0.75 (changed from 0.55 to allow Tier-B in ranging markets).
                    if amd_phase == "ACCUMULATION" and amd_conf >= 0.75:
                        lines.append("  ⛔ Hard block: ACCUMULATION conf≥0.75 — no delivery in progress")
                    elif amd_phase == "ACCUMULATION" and amd_conf >= 0.50:
                        lines.append(f"  💤 ACCUMULATION (conf={amd_conf:.2f}) — Tier-B eligible (quant-primary)")
                    elif amd_phase == "MANIPULATION":
                        lines.append("  ⚡ Judas swing active — need confirmed sweep for Tier-S")
                    elif amd_phase in ("DISTRIBUTION", "REDISTRIBUTION"):
                        lines.append(f"  🎯 Delivery phase — Tier-A/S entry eligible (ICT gate next)")
                    elif amd_phase == "REACCUMULATION":
                        lines.append(f"  🔄 Re-entry phase — with-HTF Tier-A eligible")
                except Exception as _ae:
                    lines.append(f"  AMD: error reading — {_ae}")
            else:
                lines.append("  ⏳ ICT engine not initialised")

            # ══════════════════════════════════════════════════════════
            # 4. SWEEP SETUP  (was missing entirely)
            # ══════════════════════════════════════════════════════════
            lines.append("\n<b>━ Sweep Setup</b>")
            sweep = getattr(strat, '_active_sweep_setup', None)
            if sweep is not None:
                # Age
                age_ms  = now_ms - sweep.setup_time_ms
                age_min = age_ms / 60_000
                atr_v   = max(atr, 1.0)
                buf     = 0.5 * atr_v

                # Distance to OTE and reachability.
                # SHORT: BSL swept above, price displaced down. OTE is ABOVE current price.
                #   Price must retrace UP into OTE.
                #   Missed: price too far DOWN (below ote_low - buf) OR too far UP past zone.
                # LONG:  SSL swept below, price displaced up. OTE is BELOW current price.
                #   Price must retrace DOWN into OTE.
                #   Missed ONLY: price blew too far DOWN through zone (below ote_low - buf).
                #   Being above ote_high just means still approaching — always reachable.
                in_ote = sweep.ote_entry_zone_low <= price <= sweep.ote_entry_zone_high
                if sweep.side == "short":
                    dist_to_ote   = sweep.ote_entry_zone_low - price   # + = price below OTE (approaching)
                    ote_reachable = ((sweep.ote_entry_zone_low - buf)
                                     <= price
                                     <= (sweep.ote_entry_zone_high + buf))
                else:
                    dist_to_ote   = price - sweep.ote_entry_zone_high  # + = price above OTE (approaching)
                    ote_reachable = price >= (sweep.ote_entry_zone_low - buf)

                # Delivery target display — ternary outside format spec (f-string
                # cannot contain conditional expressions in the format specifier)
                delivery_str = (f"${sweep.delivery_target:,.0f}"
                                if sweep.delivery_target else "N/A")

                status_icon = "🟢" if sweep.status == "OTE_READY" else "🔵"
                lines.append(
                    f"  {status_icon} <b>{sweep.side.upper()}</b>  status={_esc(sweep.status)}  "
                    f"quality={sweep.quality_score():.2f}  age={age_min:.0f}m")
                lines.append(
                    f"  Sweep: ${sweep.sweep_price:,.0f}  "
                    f"OTE: [${sweep.ote_entry_zone_low:,.0f}–${sweep.ote_entry_zone_high:,.0f}]")
                if in_ote:
                    lines.append(f"  ✅ Price IN OTE zone — entry eligible")
                elif ote_reachable:
                    lines.append(
                        f"  ⏳ {abs(dist_to_ote):.0f}pts to OTE "
                        f"({'up' if sweep.side == 'short' else 'down'})")
                else:
                    lines.append(f"  ❌ OTE missed — price {abs(dist_to_ote):.0f}pts beyond zone")
                lines.append(
                    f"  SL: ${sweep.sl_sweep_candle:,.0f}  "
                    f"Delivery: {delivery_str}"
                    f"  {'FVG✓' if sweep.has_fvg_in_ote else ''}"
                    f"{'OB✓' if sweep.has_ob_in_ote else ''}")
            else:
                lines.append("  ─ No active sweep setup")
                if ict and ict._initialized:
                    swept_pools = [p for p in ict.liquidity_pools
                                   if p.swept and p.displacement_confirmed]
                    if swept_pools:
                        latest = max(swept_pools, key=lambda p: p.sweep_timestamp)
                        age_m  = (now_ms - latest.sweep_timestamp) / 60_000
                        lines.append(
                            f"  Last displaced sweep: ${latest.price:,.0f}  "
                            f"{age_m:.0f}m ago  "
                            f"({'SSL' if latest.level_type == 'SSL' else 'BSL'})")

            # ══════════════════════════════════════════════════════════
            # 5. ICT ENTRY TIER  (was missing entirely)
            # ══════════════════════════════════════════════════════════
            lines.append("\n<b>━ ICT Entry Gate</b>")
            if ict and ict._initialized:
                try:
                    from strategy.ict_trade_engine import ICTEntryGate, QuantHelperSignals
                    # BUG-C FIX: Use sweep side when OTE_READY, else VWAP reversion side.
                    # Previously always used sig.reversion_side — this is wrong for sweep
                    # entries where side = sweep_setup.side (possibly opposite VWAP).
                    # The gate evaluation must mirror the actual _evaluate_reversion_entry logic.
                    _active_sweep = getattr(strat, '_active_sweep_setup', None)
                    if (_active_sweep is not None and
                            _active_sweep.status == "OTE_READY"):
                        _side = _active_sweep.side
                    else:
                        _side = sig.reversion_side or "long"
                    _qh   = strat._get_quant_helpers(sig, _side)
                    _tier, _cn, _reason = ICTEntryGate.evaluate(
                        _side, sig, sweep, price, _qh)
                    tier_icons = {"S": "🥇", "A": "🥈", "B": "🥉",
                                  "BLOCKED": "⛔"}
                    lines.append(
                        f"  {tier_icons.get(_tier,'❓')} <b>Tier-{_tier}</b>  "
                        f"side={_side.upper()}  confirm_ticks={_cn}")
                    lines.append(f"  {_esc(_reason)}")
                    # HTF shown as context — not a gate condition
                    if getattr(sig, 'htf_veto', False):
                        lines.append(
                            f"  ℹ️ HTF opposing (15m={htf_15m:+.2f}) — "
                            f"informational only, direction set by ICT structure")
                except Exception as _te:
                    lines.append(f"  ICT gate error: {_esc(str(_te))}")

                # ICT scores — normalised (consistent with terminal display)
                ob_norm  = min(sig.ict_ob  / 2.0, 1.0)
                fvg_norm = min(sig.ict_fvg / 1.5, 1.0)
                lines.append(
                    f"  ICT Σ={sig.ict_total:.2f}  "
                    f"OB={ob_norm:.2f}(raw={sig.ict_ob:.1f})  "
                    f"FVG={fvg_norm:.2f}(raw={sig.ict_fvg:.1f})  "
                    f"Sweep={sig.ict_sweep:.2f}  KZ={sig.ict_session:.2f}")
                if sig.ict_details:
                    lines.append(f"  {_esc(sig.ict_details)}")
            else:
                lines.append("  ⏳ ICT engine initialising")

            # ══════════════════════════════════════════════════════════
            # 6. QUANT SIGNALS + GATES
            # ══════════════════════════════════════════════════════════
            lines.append("\n<b>━ Quant Signals</b>")

            def bar(v, w=10):
                h = w // 2; f_ = min(int(abs(v) * h + 0.5), h)
                return ("·" * h + "█" * f_ + "░" * (h - f_)) if v >= 0 \
                    else ("░" * (h - f_) + "█" * f_ + "·" * h)

            def sline(label, val):
                arrow = "▲" if val > 0.05 else ("▼" if val < -0.05 else "─")
                return f"  {label:<7} {bar(val)} {arrow} {val:+.3f}"

            lines.append(sline("VWAP",  sig.vwap_dev))
            lines.append(sline("CVD",   sig.cvd_div))
            lines.append(sline("OB",    sig.orderbook))
            lines.append(sline("TICK",  sig.tick_flow))
            lines.append(sline("VEX",   sig.vol_exhaust))
            lines.append(f"  {'─' * 32}")

            c   = sig.composite
            thr = float(getattr(cfg, 'QUANT_COMPOSITE_ENTRY_MIN', 0.30))
            # n_confirming counts QUANT signals only (max 5 — VWAP/CVD/OB/TICK/VEX)
            # ICT has its own gate above; /6 denominator in old code was wrong
            lines.append(f"  Σ = {c:+.4f}  (need ±{thr:.3f})")
            lines.append(f"  Confirming: {sig.n_confirming}/5 quant signals")

            lines.append("\n<b>━ Entry Gates</b>")
            # Overextended: gate uses ADX-adaptive threshold (0.4/0.6/0.9 ATR)
            if adx_val < 25.0:
                actual_thresh = 0.4
                thresh_regime = "ranging"
            elif adx_val < 35.0:
                actual_thresh = 0.6
                thresh_regime = "transitioning"
            else:
                actual_thresh = 0.9
                thresh_regime = "trending"
            g_ext  = sig.overextended
            g_reg  = sig.regime_ok
            g_conf = sig.n_confirming >= 3
            g_comp = abs(c) >= thr

            lines.append(
                f"  {'✅' if g_ext  else '❌'} Overextended   "
                f"(|dev|={abs(dev_atr):.2f}ATR  need ≥{actual_thresh:.1f}ATR [{thresh_regime}  ADX={adx_val:.1f}])")
            lines.append(
                f"  {'✅' if g_reg  else '❌'} ATR regime     "
                f"({atr_pct:.0%} pctile  valid 5–97%)")
            lines.append(
                f"  ⚪ HTF (info)       "
                f"(4h={htf_4h:+.2f}  15m={htf_15m:+.2f}  — directional context, not a gate)")
            lines.append(
                f"  {'✅' if g_conf else '❌'} Confluence     "
                f"({sig.n_confirming}/5 quant  need ≥3)")
            lines.append(
                f"  {'✅' if g_comp else '❌'} Composite      "
                f"(Σ={c:+.3f}  need ±{thr:.3f})")

            # ══════════════════════════════════════════════════════════
            # 7. RISK GATE + COOLDOWN
            # ══════════════════════════════════════════════════════════
            lines.append("\n<b>━ Risk & Cooldown</b>")
            can_ok, risk_reason = rm.can_trade()
            daily_trades = strat._risk_gate.daily_trades
            max_dt       = int(getattr(cfg, 'QUANT_MAX_DAILY_TRADES', 8))
            consec       = strat._risk_gate.consec_losses
            cooldown_sec = float(getattr(cfg, 'QUANT_COOLDOWN_SEC', 180))
            cd_rem       = max(0.0, cooldown_sec - (now - strat._last_exit_time))

            lines.append(
                f"  {'✅' if can_ok else '🚫'} Risk gate: "
                f"{daily_trades}/{max_dt} trades  consec_loss={consec}"
                + (f"  → {_esc(risk_reason)}" if not can_ok else ""))
            lines.append(
                f"  {'✅' if cd_rem == 0 else '⏳'} Cooldown: "
                + (f"{cd_rem:.0f}s remaining" if cd_rem > 0 else "ready"))

            bo = strat._breakout
            if bo.is_active:
                retest = "RETEST READY ✅" if bo.retest_ready else "awaiting pullback"
                lines.append(f"  🚀 Breakout {_esc(bo.direction.upper())} — {retest}")

            # ══════════════════════════════════════════════════════════
            # 8. ACTIVE POSITION + TRAIL STATE  (was missing entirely)
            # ══════════════════════════════════════════════════════════
            from strategy.quant_strategy import PositionPhase
            if pos.phase == PositionPhase.ACTIVE:
                lines.append("\n<b>━ Active Position + Trail</b>")
                profit_pts   = (pos.entry_price - price if pos.side == "short"
                                else price - pos.entry_price)
                sl_dist_now  = abs(price - pos.sl_price)
                tp_dist_now  = abs(price - pos.tp_price)
                init_dist    = pos.initial_sl_dist if pos.initial_sl_dist > 1e-10 else atr
                tier_r       = max(profit_pts, pos.peak_profit) / init_dist
                rr_now       = tp_dist_now / max(sl_dist_now, 1e-9)
                hold_m       = (now - pos.entry_time) / 60.0

                # Trail phase
                be_r   = float(getattr(cfg, 'QUANT_TRAIL_BE_R',        0.3))
                lock_r = float(getattr(cfg, 'QUANT_TRAIL_LOCK_R',       0.8))
                aggr_r = float(getattr(cfg, 'QUANT_TRAIL_AGGRESSIVE_R', 1.5))
                if tier_r < be_r:
                    phase_lbl = f"⬜ P0 — hands off (< {be_r:.1f}R)"
                elif tier_r < lock_r:
                    phase_lbl = f"🟡 P1 — structure trail (< {lock_r:.1f}R)"
                elif tier_r < aggr_r:
                    phase_lbl = f"🟠 P2 — locked trail (< {aggr_r:.1f}R)"
                else:
                    phase_lbl = f"🟢 P3 — aggressive trail"

                lines.append(
                    f"  {pos.side.upper()}  mode={_esc(pos.trade_mode)}  "
                    f"tier={_esc(getattr(pos,'ict_entry_tier',''))}")
                lines.append(
                    f"  Entry:  ${pos.entry_price:,.2f}  "
                    f"SL: ${pos.sl_price:,.2f}  TP: ${pos.tp_price:,.2f}")
                lines.append(
                    f"  P&L:    {profit_pts:+.1f}pts  "
                    f"MFE: {pos.peak_profit:.1f}pts  "
                    f"Hold: {hold_m:.0f}m")
                lines.append(
                    f"  R:      {tier_r:.2f}R  "
                    f"Remaining R:R to TP: 1:{rr_now:.1f}")
                lines.append(f"  Trail:  {phase_lbl}")

                # Break-even lock status
                be_price = (pos.entry_price + 0.25 * atr if pos.side == "long"
                            else pos.entry_price - 0.25 * atr)
                be_locked = ((pos.side == "long"  and pos.sl_price >= be_price) or
                             (pos.side == "short" and pos.sl_price <= be_price))
                if be_locked:
                    locked_profit = abs(pos.entry_price - pos.sl_price)
                    lines.append(
                        f"  🔒 Break-even locked  "
                        f"({locked_profit:.0f}pts profit protected if SL hit)")
                else:
                    to_be = abs(profit_pts) / init_dist if init_dist > 1e-9 else 0
                    lines.append(
                        f"  ⬜ BE lock at {be_r:.1f}R — currently {to_be:.2f}R "
                        f"({profit_pts:.0f}pts / {init_dist:.0f}pts SL)")

            # ══════════════════════════════════════════════════════════
            # 9. VERDICT
            # ══════════════════════════════════════════════════════════
            lines.append("\n<b>━ Verdict</b>")
            # HTF is informational — not in all_pass
            all_pass = g_ext and g_reg and g_conf and g_comp

            if pos.phase == PositionPhase.ACTIVE:
                lines.append(f"  📍 Position ACTIVE — entry gate not evaluated")
            elif all_pass and can_ok and cd_rem == 0:
                cn_long  = getattr(strat, '_confirm_long',  0)
                cn_short = getattr(strat, '_confirm_short', 0)
                cn_need  = int(getattr(cfg, 'QUANT_CONFIRM_TICKS', 2))
                lines.append(
                    f"  🎯 <b>ALL QUANT GATES PASS</b>  "
                    f"({max(cn_long, cn_short)}/{cn_need} confirm ticks)  "
                    f"— ICT gate is the binding decision above")
            else:
                missing = []
                if not g_ext:
                    missing.append(
                        f"VWAP: |{abs(dev_atr):.2f}|ATR &lt; {actual_thresh:.1f}ATR ({thresh_regime})")
                if not g_reg:
                    missing.append(f"ATR regime ({atr_pct:.0%})")
                if not g_conf:
                    missing.append(f"Confluence {sig.n_confirming}/5 &lt; 3")
                if not g_comp:
                    missing.append(f"Composite Σ={c:+.3f} &lt; ±{thr:.3f}")
                if not can_ok:
                    missing.append(f"Risk: {_esc(risk_reason)}")
                if cd_rem > 0:
                    missing.append(f"Cooldown {cd_rem:.0f}s")
                lines.append("  👀 <b>Watching</b>  —  blocked by:")
                for m in missing:
                    lines.append(f"    • {m}")
                # HTF shown as context even when not blocking
                if sig.htf_veto:
                    lines.append(
                        f"    ℹ️ HTF opposing ({htf_15m:+.2f} 15m) — informational only")

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
                f"Session: {_esc(sess)}{_esc(kz_s)}  |  ATR(5m): ${atr_v:.1f}",
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
                lines.append(f"    → {_esc(long_c.details)}")
            lines.append(
                f"  SHORT Σ={short_c.total:.2f}  "
                f"OB={short_c.ob_score:.2f}  FVG={short_c.fvg_score:.2f}  "
                f"Sweep={short_c.sweep_score:.2f}  KZ={short_c.session_score:.2f}")
            if short_c.details:
                lines.append(f"    → {_esc(short_c.details)}")

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
                    f"ICT: {es.ict_total:.2f} [{_esc(es.ict_details)}]")

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
                # Attribution fields (v7.0)
                ict_tier   = t.get('ict_tier', '')
                composite  = t.get('composite', 0.0)
                ict_total  = t.get('ict_total', 0.0)
                amd_phase  = t.get('amd_phase', '')
                adx_val    = t.get('adx', 0.0)
                tier_badge = f" [T{ict_tier}]" if ict_tier else ""
                # Fee breakdown fields (exact from Delta /v2/fills, else estimated)
                gross_pnl  = t.get('gross_pnl',  pnl)
                entry_fee  = t.get('entry_fee',  0.0)
                exit_fee   = t.get('exit_fee',   0.0)
                total_fees = t.get('total_fees', 0.0)
                exact_fees = t.get('exact_fees', False)

                # Determine label
                if reason == "tp_hit":
                    label = "🎯 TP"
                elif reason == "trail_sl_hit":
                    label = "🔒 TRAIL"
                elif reason == "sl_hit":
                    label = "🛑 SL"
                else:
                    label = f"🚪 {reason[:8]}"

                result    = "✅" if is_win else "❌"
                trail_tag = " [T]" if trailed else ""
                fee_tag   = "exact" if exact_fees else "est."

                # Fee line: show gross→net breakdown when fee data is present
                if total_fees > 0:
                    fee_line = (
                        f"\n    Fees({fee_tag}): entry=${entry_fee:.4f} "
                        f"exit=${exit_fee:.4f} "
                        f"total=${total_fees:.4f} | "
                        f"Gross=${gross_pnl:+.4f}"
                    )
                else:
                    fee_line = ""

                lines.append(
                    f"{result} {side} [{mode}]{tier_badge}  "
                    f"${entry:,.0f}→${exit_p:,.0f}  "
                    f"PnL: <b>${pnl:+.2f}</b>  "
                    f"R: {ach_r:+.2f}  MFE: {mfe_r:.1f}R\n"
                    f"    {label}{trail_tag}  hold: {hold:.0f}m"
                    + (f"  Σ={composite:+.3f} ICT={ict_total:.2f}"
                       f"  {_esc(amd_phase[:5])}  ADX={adx_val:.0f}" if composite else "")
                    + fee_line
                )
        else:
            lines.append("  No trades recorded yet this session.")

        # ── Summary stats from strategy (ground truth) ──────────────────
        total_t   = getattr(strat, '_total_trades', 0)
        wins      = getattr(strat, '_winning_trades', 0)
        losses    = total_t - wins
        wr        = wins / total_t * 100.0 if total_t > 0 else 0.0
        total_pnl = getattr(strat, '_total_pnl', 0.0)

        # Daily stats from risk gate (authoritative daily counters)
        daily_cnt = strat._risk_gate.daily_trades if hasattr(strat, '_risk_gate') else 0
        consec    = strat._risk_gate.consec_losses if hasattr(strat, '_risk_gate') else 0
        max_d     = getattr(__import__('config'), 'MAX_DAILY_TRADES', 8)

        # Avg win / avg loss from history
        win_pnls  = [t['pnl'] for t in history if t.get('is_win')]
        loss_pnls = [t['pnl'] for t in history if not t.get('is_win')]
        avg_win   = sum(win_pnls)  / len(win_pnls)  if win_pnls  else 0.0
        avg_loss  = sum(loss_pnls) / len(loss_pnls) if loss_pnls else 0.0
        expectancy = (wr/100 * avg_win) + ((1 - wr/100) * avg_loss)

        # Total fees paid — sum exact where available, estimated where not
        fees_exact   = [t['total_fees'] for t in history if t.get('exact_fees') and t.get('total_fees', 0) > 0]
        fees_est     = [t['total_fees'] for t in history if not t.get('exact_fees') and t.get('total_fees', 0) > 0]
        total_fees_s = sum(fees_exact) + sum(fees_est)
        n_exact      = len(fees_exact)
        n_est        = len(fees_est)
        if total_fees_s > 0:
            fee_src_tag = f"({n_exact} exact, {n_est} est.)"
            fee_summary = f"Total Fees:  ${total_fees_s:.4f} {fee_src_tag}"
        else:
            fee_summary = "Total Fees:  —"

        lines += [
            "",
            "━━━━━━━━━━━━━━━━━━━━━━━━",
            f"Session:   {total_t} trades  W:{wins} L:{losses}  WR: <b>{wr:.0f}%</b>",
            f"Total PnL: <b>${total_pnl:+.2f}</b> USDT",
            f"Avg Win:   ${avg_win:+.2f}  Avg Loss: ${avg_loss:+.2f}",
            f"Expectancy: ${expectancy:+.2f}/trade",
            fee_summary,
            f"Today:     {daily_cnt}/{max_d} trades  consec_loss={consec}",
        ]
        return "\n".join(lines)

    def _cmd_stats(self) -> str:
        """
        Signal attribution analysis — which signal combinations produce wins.
        Shows win-rate breakdown by ICT tier, regime, AMD phase and composite score.
        Includes realised PnL and fees breakdown where available.
        Requires ≥5 trades to produce meaningful stats.
        """
        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return "Bot not running."

        strat = bot_instance.strategy
        if not strat:
            return "Strategy not ready."

        history = getattr(strat, '_trade_history', [])
        if len(history) < 3:
            return f"📊 Not enough trades yet ({len(history)} recorded — need ≥3)."

        lines = ["<b>📊 Signal Attribution Analysis</b>\n"]

        total  = len(history)
        wins   = sum(1 for t in history if t.get('is_win'))
        wr_all = wins / total * 100.0

        lines.append(f"Total trades: {total}  WR: <b>{wr_all:.0f}%</b>\n")

        # ── By ICT tier ──────────────────────────────────────────────────
        lines.append("<b>By ICT Tier</b>")
        tier_groups: dict = {}
        for t in history:
            k = t.get('ict_tier', '') or 'none'
            tier_groups.setdefault(k, []).append(t)
        for tier in ['S', 'A', 'B', 'none']:
            grp = tier_groups.get(tier, [])
            if not grp:
                continue
            w       = sum(1 for t in grp if t.get('is_win'))
            wr      = w / len(grp) * 100.0
            avg_pnl = sum(t.get('pnl', 0) for t in grp) / len(grp)
            avg_fees= sum(t.get('total_fees', 0) for t in grp) / len(grp)
            fee_tag = " (fees est.)" if not any(t.get('exact_fees') for t in grp) else ""
            label   = f"Tier-{tier}" if tier != 'none' else "No tier"
            lines.append(
                f"  {label}: {len(grp)} trades  WR={wr:.0f}%  "
                f"avg net=${avg_pnl:+.2f}  avg fees=${avg_fees:.4f}{fee_tag}"
            )

        # ── By regime ────────────────────────────────────────────────────
        lines.append("\n<b>By Regime</b>")
        reg_groups: dict = {}
        for t in history:
            k = t.get('regime', 'UNKNOWN')
            reg_groups.setdefault(k, []).append(t)
        for reg, grp in sorted(reg_groups.items()):
            w  = sum(1 for t in grp if t.get('is_win'))
            wr = w / len(grp) * 100.0
            lines.append(f"  {_esc(reg)}: {len(grp)} trades  WR={wr:.0f}%")

        # ── By AMD phase ─────────────────────────────────────────────────
        lines.append("\n<b>By AMD Phase</b>")
        amd_groups: dict = {}
        for t in history:
            k = t.get('amd_phase', 'UNKNOWN')
            amd_groups.setdefault(k, []).append(t)
        for phase, grp in sorted(amd_groups.items()):
            w  = sum(1 for t in grp if t.get('is_win'))
            wr = w / len(grp) * 100.0
            lines.append(f"  {_esc(phase)}: {len(grp)} trades  WR={wr:.0f}%")

        # ── By composite score bucket ─────────────────────────────────────
        lines.append("\n<b>By Composite Score</b>")
        buckets = [
            ("≥0.70", lambda c: c >= 0.70),
            ("0.50–0.70", lambda c: 0.50 <= c < 0.70),
            ("0.35–0.50", lambda c: 0.35 <= c < 0.50),
            ("<0.35", lambda c: abs(c) < 0.35),
        ]
        for label, fn in buckets:
            grp = [t for t in history if fn(abs(t.get('composite', 0.0)))]
            if not grp:
                continue
            w  = sum(1 for t in grp if t.get('is_win'))
            wr = w / len(grp) * 100.0
            lines.append(f"  {label}: {len(grp)} trades  WR={wr:.0f}%")

        # ── Best and worst combos ─────────────────────────────────────────
        if len(history) >= 10:
            lines.append("\n<b>Top 3 win combos (tier+regime)</b>")
            combo_groups: dict = {}
            for t in history:
                k = f"{t.get('ict_tier','?')}|{t.get('regime','?')[:8]}"
                combo_groups.setdefault(k, []).append(t)
            ranked = []
            for combo, grp in combo_groups.items():
                if len(grp) >= 2:
                    w  = sum(1 for t in grp if t.get('is_win'))
                    wr = w / len(grp) * 100.0
                    ranked.append((wr, len(grp), combo))
            ranked.sort(reverse=True)
            for wr, cnt, combo in ranked[:3]:
                tier_lbl, reg_lbl = combo.split('|')
                lines.append(f"  Tier-{tier_lbl} + {_esc(reg_lbl)}: {cnt} trades  WR={wr:.0f}%")

        # ── Realised PnL and fee summary ──────────────────────────────────
        lines.append("\n<b>Realised PnL &amp; Fees</b>")
        total_gross  = sum(t.get('gross_pnl', t.get('pnl', 0)) for t in history)
        total_net    = sum(t.get('pnl', 0) for t in history)
        total_fees   = sum(t.get('total_fees', 0) for t in history)
        total_entry  = sum(t.get('entry_fee', 0) for t in history)
        total_exit   = sum(t.get('exit_fee',  0) for t in history)
        n_exact      = sum(1 for t in history if t.get('exact_fees'))
        n_est        = total - n_exact
        fee_note     = f"({n_exact} exact from exchange, {n_est} estimated)" if n_exact > 0 \
                       else "(all estimated — enable Delta for exact fees)"

        lines += [
            f"  Gross PnL:   ${total_gross:+.4f} USDT",
            f"  Total Fees:  ${total_fees:.4f} USDT  {fee_note}",
            f"    Entry fees: ${total_entry:.4f}  Exit fees: ${total_exit:.4f}",
            f"  Net PnL:     <b>${total_net:+.4f}</b> USDT",
            f"  Avg fee/trade: ${(total_fees/total):.4f}" if total > 0 else "",
        ]

        return "\n".join(l for l in lines if l is not None)

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
                swept = om.cancel_symbol_conditionals(symbol=config.SYMBOL)
                results.append(f"✅ Swept {len(swept)} conditional order(s)")
            except Exception as e:
                results.append(f"⚠️ Cancel error: {e}")

            # 2. Close open position
            try:
                pos = om.get_open_position()
                if pos and float(pos.get("size", 0)) > 0:
                    pos_side   = str(pos.get("side", "")).upper()
                    close_side = "SELL" if pos_side == "LONG" else "BUY"
                    qty        = float(pos["size"])
                    resp       = om.place_market_order(
                        side=close_side, quantity=qty, reduce_only=True)
                    if resp:
                        results.append(f"✅ Closed {pos_side} ({qty} contracts/BTC)")
                    else:
                        results.append(f"⚠️ Close order returned None")
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

    def _cmd_setexchange(self, args: str) -> str:
        """
        /setexchange delta|coinswitch

        Hot-switches the execution exchange at runtime.
        Blocked if a position is currently open (must close first).
        Verifies balance on the new exchange before switching.

        The data aggregator continues to pull from BOTH exchanges regardless
        of which exchange is active for execution — this command only affects
        where orders are placed.
        """
        global bot_instance, bot_running

        if not args:
            active = getattr(config, "EXECUTION_EXCHANGE", "?")
            return (
                f"Current execution exchange: <b>{active.upper()}</b>\n\n"
                f"Usage: /setexchange &lt;exchange&gt;\n"
                f"Valid values: <code>delta</code>, <code>coinswitch</code>\n\n"
                f"<i>Data is aggregated from both exchanges regardless of this setting.</i>"
            )

        target = args.strip().lower()

        if not bot_running or bot_instance is None:
            # Bot not running — update config only
            try:
                from core.types import Exchange
                Exchange.from_str(target)   # validates
                config.EXECUTION_EXCHANGE = Exchange.from_str(target).value
                return (f"✅ Execution exchange set to <b>{target.upper()}</b> "
                        f"(bot not running — takes effect on next start)")
            except ValueError:
                return f"❌ Unknown exchange: <code>{target}</code>"

        router = getattr(bot_instance, "execution_router", None)
        if router is None:
            return "❌ Execution router not available — bot may not be fully initialised."

        strategy = getattr(bot_instance, "strategy", None)
        success, message = router.switch(target, strategy=strategy)

        if success:
            # Also update leverage on the new exchange
            if bot_instance.order_manager:
                try:
                    bot_instance.order_manager.set_leverage(
                        leverage=int(config.LEVERAGE)
                    )
                except Exception as e:
                    message += f"\n⚠️ Leverage set failed: {e}"

        return message

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
                if om:
                    try:
                        resp = om.set_leverage(leverage=int(new_val))
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
            import sys, os as _os
            # Ensure project root is on path so 'main' resolves correctly
            _root = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
            if _root not in sys.path:
                sys.path.insert(0, _root)
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
            "⚡ <b>Unified Quant Bot v5 Controller Ready</b>\n"
            "Execution: " + getattr(config, "EXECUTION_EXCHANGE", "?").upper() + "\n\n"
            + self._cmd_help())
        logger.info("Telegram controller started")

        while self.running:
            try:
                # Bug-21 fix: use timeout=10 so the poll returns within ~12 s
                # of a stop() call rather than blocking for up to 35 s.  The
                # shorter interval has no practical effect on message delivery —
                # Telegram updates are pushed server-side regardless of the
                # client poll timeout.
                updates = self.get_updates(timeout=10)
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
    import io, signal
    from datetime import timezone, timedelta

    IST = timezone(timedelta(hours=5, minutes=30))

    class ISTFormatter(logging.Formatter):
        def formatTime(self, record, datefmt=None):
            from datetime import datetime
            dt = datetime.fromtimestamp(record.created, tz=IST)
            s  = dt.strftime("%Y-%m-%d %H:%M:%S")
            return f"{s},{int(record.msecs):03d}"

    _fmt = ISTFormatter(fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    _fh = logging.FileHandler("telegram_controller.log", encoding="utf-8")
    _fh.setFormatter(_fmt)

    _sh = logging.StreamHandler(
        stream=io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        if hasattr(sys.stdout, "buffer") else sys.stdout
    )
    _sh.setFormatter(_fmt)

    logging.basicConfig(
        level=getattr(config, "LOG_LEVEL", "INFO"),
        handlers=[_fh, _sh],
    )

    def _signal_handler(signum, frame):
        logger.info(f"Signal {signum} — stopping controller")
        sys.exit(0)

    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGINT,  _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)

    try:
        controller = TelegramBotController()
        controller.start()
    except KeyboardInterrupt:
        logger.info("Shutdown requested")
    except Exception as e:
        logger.error(f"Fatal: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

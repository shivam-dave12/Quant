"""
telegram/controller.py — Liquidity-First Telegram Bot Controller
=================================================================
All command handlers reflect the liquidity-first decision architecture:

  /thinking   — 5-layer decision stack (pools → flow → ICT → entry → trail)
  /status     — Full bot status (pool map + flow + position)
  /pools      — Live liquidity pool map with priority scores
  /flow       — Detailed orderflow state (CVD + OB delta + tick aggression)
  /structures — ICT structure map (OB / FVG / AMD — secondary layer)
  /position   — Current position with pool TP context
  /trades     — Recent trade history
  /stats      — Signal attribution analysis
  /balance    — Wallet balance
  /pause      — Pause trading (keep monitoring)
  /resume     — Resume trading
  /trail      — Toggle trailing SL on/off/auto
  /config     — Show current config values
  /killswitch — Emergency: close all positions + cancel orders
  /setexchange — Switch execution exchange (delta|coinswitch)
  /set <key> <val> — Live-adjust config
  /resetrisk  — Clear consecutive-loss lockout
  /huntstatus — Liquidity hunt engine status
  /help       — Show commands
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

# ── v9 display engine (optional) ─────────────────────────────────────────────
try:
    from strategy.v9_display import (
        format_thinking_telegram, format_pools_telegram,
        format_flow_telegram, format_status_report_v9,
        format_periodic_report_v9, HELP_TEXT as V9_HELP,
    )
    _V9_DISPLAY = True
except ImportError:
    _V9_DISPLAY = False


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

        logger.info("TelegramBotController (liquidity-first) initialized")

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
        if parse_mode == "HTML":
            text = _sanitize_html(text)
        url     = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
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
        _read_timeout = min(timeout, 15) + 2
        try:
            url    = f"https://api.telegram.org/bot{self.bot_token}/getUpdates"
            params = {
                "offset":          self.last_update_id + 1,
                "timeout":         min(timeout, 15),
                "allowed_updates": ["message"],
            }
            resp = requests.get(url, params=params, timeout=(5.0, _read_timeout))
            if resp.status_code != 200:
                logger.warning("Telegram API HTTP %d — getUpdates skipped", resp.status_code)
                return []
            data = resp.json()
            return data.get("result", []) if data.get("ok") else []
        except requests.exceptions.Timeout:
            return []
        except requests.exceptions.ConnectionError as e:
            logger.warning("Telegram connection error (will retry): %s", e)
            return []
        except ValueError as e:
            logger.error("Telegram JSON parse error in getUpdates: %s", e)
            return []
        except Exception as e:
            logger.error("Telegram getUpdates unexpected error: %s", e, exc_info=True)
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
            url      = f"https://api.telegram.org/bot{self.bot_token}/setMyCommands"
            commands = [
                {"command": "start",       "description": "Start trading bot"},
                {"command": "stop",        "description": "Stop trading bot"},
                {"command": "status",      "description": "Full status + pool map"},
                {"command": "thinking",    "description": "5-layer liquidity-first decision stack"},
                {"command": "pools",       "description": "Live liquidity pool map"},
                {"command": "flow",        "description": "CVD + OB delta + tick aggression"},
                {"command": "structures",  "description": "ICT structure map (secondary layer)"},
                {"command": "position",    "description": "Current position + pool TP"},
                {"command": "trades",      "description": "Recent trade history"},
                {"command": "stats",       "description": "Signal attribution analysis"},
                {"command": "balance",     "description": "Wallet balance"},
                {"command": "pause",       "description": "Pause trading"},
                {"command": "resume",      "description": "Resume trading"},
                {"command": "trail",       "description": "Toggle trailing SL on/off/auto"},
                {"command": "config",      "description": "Show config values"},
                {"command": "killswitch",  "description": "Emergency close all"},
                {"command": "set",         "description": "Set config value live"},
                {"command": "huntstatus",  "description": "Liquidity hunt engine state"},
                {"command": "help",        "description": "Show commands"},
            ]
            payload = {
                "commands":      commands,
                "scope":         {"type": "all_private_chats"},
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
            "start", "stop", "status", "thinking", "pools", "flow",
            "structures", "position", "trades", "stats", "config",
            "pause", "resume", "balance", "trail", "killswitch",
            "set", "help", "huntstatus", "setexchange", "resetrisk",
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
            if   cmd in ("/help", "/commands"): return self._cmd_help()
            elif cmd == "/start":               return self._cmd_start()
            elif cmd == "/stop":                return self._cmd_stop()
            elif cmd == "/status":              return self._cmd_status()
            elif cmd == "/thinking":            return self._cmd_thinking()
            elif cmd == "/pools":               return self._cmd_pools()
            elif cmd == "/flow":                return self._cmd_flow()
            elif cmd == "/structures":          return self._cmd_structures()
            elif cmd == "/position":            return self._cmd_position()
            elif cmd == "/trades":              return self._cmd_trades()
            elif cmd == "/stats":               return self._cmd_stats()
            elif cmd == "/balance":             return self._cmd_balance()
            elif cmd == "/pause":               return self._cmd_pause()
            elif cmd == "/resume":              return self._cmd_resume()
            elif cmd == "/trail":               return self._cmd_trail(args)
            elif cmd == "/config":              return self._cmd_config()
            elif cmd == "/killswitch":          return self._cmd_killswitch()
            elif cmd == "/resetrisk":           return self._cmd_resetrisk(args)
            elif cmd == "/set":                 return self._cmd_set(args)
            elif cmd == "/setexchange":         return self._cmd_setexchange(args)
            elif cmd == "/huntstatus":          return self._cmd_huntstatus()
            else:
                return f"Unknown command: {cmd}\n\n" + self._cmd_help()
        except Exception as e:
            logger.error(f"Command error [{cmd}]: {e}", exc_info=True)
            return f"❌ Error in {cmd}: {e}"

    # ================================================================
    # /help
    # ================================================================

    def _cmd_help(self) -> str:
        return (
            "<b>Liquidity-First Quant Bot — Commands</b>\n\n"
            "<b>Decision Stack</b>\n"
            "/thinking — 5-layer decision stack (pools→flow→ICT→entry→trail)\n"
            "/pools    — Live BSL/SSL pool map with priority scores\n"
            "/flow     — CVD divergence + OB delta + tick aggression\n"
            "/structures — ICT secondary layer (OB/FVG/AMD/swing)\n\n"
            "<b>Position</b>\n"
            "/position — Current position + pool TP context\n"
            "/trades   — Recent trade history\n"
            "/stats    — Attribution analysis (tier/regime WR breakdown)\n\n"
            "<b>Control</b>\n"
            "/status   — Full bot status\n"
            "/balance  — Wallet balance\n"
            "/pause    — Pause trading (keep monitoring)\n"
            "/resume   — Resume trading\n"
            "/trail [on|off|auto] — Toggle trailing SL\n"
            "/config   — Show config values\n"
            "/set &lt;key&gt; &lt;value&gt; — Adjust config live\n"
            "/setexchange &lt;delta|coinswitch&gt; — Switch execution exchange\n"
            "/killswitch — Emergency: close position + cancel orders\n"
            "/resetrisk  — Clear consecutive-loss lockout\n"
            "/resetrisk full — Also reset daily PnL + trade counters\n"
            "/huntstatus — Liquidity hunt engine state\n"
            "/start — Start bot\n"
            "/stop  — Stop bot\n"
            "/help  — This list"
        )

    # ================================================================
    # /thinking  ← CORE COMMAND — 5-layer liquidity-first stack
    # ================================================================

    def _cmd_thinking(self) -> str:
        """
        Live 5-layer liquidity-first decision stack:

          LAYER 1 — Liquidity Map    BSL/SSL pools + priority scores
          LAYER 2 — Flow Direction   CVD + OB delta + tick aggression
                                     → is flow driving toward target pool?
          LAYER 3 — ICT Secondary    AMD phase, OB/FVG alignment, P/D zone
          LAYER 4 — Entry Gate       Sweep/OTE status, SL/TP levels
          LAYER 5 — Post-trade       Trail engine state (BOS/CHoCH)
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

            price   = dm.get_last_price()
            now     = time.time()
            now_ms  = int(now * 1000)
            atr     = strat._atr_5m.atr
            ict     = strat._ict
            pos     = strat._pos
            sig     = strat._last_sig

            lines = [f"<b>🧠 LIQUIDITY-FIRST STACK @ ${price:,.2f}</b>"]

            # ══════════════════════════════════════════════════════════
            # LAYER 1 — LIQUIDITY MAP
            # Markets move from pool to pool. Which pool are we targeting?
            # ══════════════════════════════════════════════════════════
            lines.append("\n<b>━━ LAYER 1: LIQUIDITY MAP</b>")

            liq_map = getattr(strat, '_liq_map', None)
            if liq_map is not None:
                try:
                    snap    = liq_map.get_snapshot(price, atr)
                    summary = liq_map.get_status_summary(price, atr)

                    # BSL pools above price
                    # BUG-FIX: snap.bsl_pools contains PoolTarget objects.
                    # Price lives at p.pool.price, not p.price.
                    # priority_score→p.significance, touch_count→p.pool.touches,
                    # timeframe→p.pool.timeframe, fresh→pool status check.
                    bsl_near = [p for p in snap.bsl_pools if p.pool.price > price][:4]
                    ssl_near = [p for p in snap.ssl_pools if p.pool.price < price][:4]
                    bsl_near_sorted = sorted(bsl_near, key=lambda p: p.pool.price)
                    ssl_near_sorted = sorted(ssl_near, key=lambda p: p.pool.price, reverse=True)

                    lines.append("  <b>BSL (Buy-side above)</b>")
                    for p in bsl_near_sorted:
                        dist_atr = (p.pool.price - price) / max(atr, 1)
                        score    = p.significance
                        touches  = p.pool.touches
                        tf       = p.pool.timeframe
                        fresh    = ("✅" if p.pool.status.value not in ('SWEPT', 'CONSUMED')
                                    else "♻️")
                        lines.append(
                            f"    ${p.pool.price:,.1f}  dist={dist_atr:.1f}ATR  "
                            f"x{touches}  [{tf}]  {fresh}  score={score:.2f}")

                    lines.append("  <b>SSL (Sell-side below)</b>")
                    for p in ssl_near_sorted:
                        dist_atr = (price - p.pool.price) / max(atr, 1)
                        score    = p.significance
                        touches  = p.pool.touches
                        tf       = p.pool.timeframe
                        fresh    = ("✅" if p.pool.status.value not in ('SWEPT', 'CONSUMED')
                                    else "♻️")
                        lines.append(
                            f"    ${p.pool.price:,.1f}  dist={dist_atr:.1f}ATR  "
                            f"x{touches}  [{tf}]  {fresh}  score={score:.2f}")

                    # Primary target
                    # BUG-FIX C17-C19: PoolTarget has no .level_type or .price.
                    # Correct paths: pt.pool.side.value, pt.pool.price, pt.significance.
                    pt = snap.primary_target
                    if pt:
                        direction = "BSL ▲" if pt.pool.side.value == "BSL" else "SSL ▼"
                        pt_score  = pt.significance
                        lines.append(
                            f"\n  🎯 <b>Primary target: {direction} @ ${pt.pool.price:,.1f}"
                            f"  (score={pt_score:.2f})</b>")
                    else:
                        lines.append("\n  ─ No high-priority target pool identified")

                    # Recent sweeps
                    # BUG-FIX C20-C23: SweepResult has no sweep_timestamp, level_type,
                    # price, or displacement_confirmed. Correct attrs: detected_at (seconds),
                    # pool.side.value, pool.price, quality (use ≥0.5 as displacement proxy).
                    if snap.recent_sweeps:
                        lines.append(f"  🌊 Recent sweeps: {len(snap.recent_sweeps)}")
                        for sw in snap.recent_sweeps[-2:]:
                            age_m = (now - sw.detected_at) / 60.0
                            disp  = "DISP✓" if sw.quality >= 0.5 else "weak"
                            lines.append(
                                f"    {sw.pool.side.value} ${sw.pool.price:,.1f}"
                                f"  [{disp}]  {age_m:.0f}m ago")
                except Exception as _le:
                    lines.append(f"  LiqMap error: {_le}")
            else:
                lines.append("  ⏳ Liquidity map not available (v9 engine not active)")

            # ══════════════════════════════════════════════════════════
            # LAYER 2 — FLOW DIRECTION (primary gate)
            # Is CVD divergence + OB delta + tick aggression driving
            # toward the highest-priority pool?
            # ══════════════════════════════════════════════════════════
            lines.append("\n<b>━━ LAYER 2: FLOW DIRECTION  (primary gate)</b>")

            tick_flow    = strat._tick_eng.get_signal() if strat._tick_eng else 0.0
            cvd_trend    = strat._cvd.get_trend_signal() if strat._cvd else 0.0
            cvd_div      = 0.0
            try:
                cvd_div = strat._cvd.get_divergence_signal(dm.get_candles("1m", limit=60))
            except Exception:
                pass

            ob_imbalance = 0.0
            try:
                ob = dm.get_orderbook()
                if ob and ob.get("bids") and ob.get("asks"):
                    bv = sum(float(b[1]) for b in ob["bids"][:10])
                    av = sum(float(a[1]) for a in ob["asks"][:10])
                    total_vol = bv + av
                    if total_vol > 0:
                        ob_imbalance = (bv - av) / total_vol
            except Exception:
                pass

            streak     = getattr(strat, '_flow_streak_count_v2', 0)
            streak_dir = getattr(strat, '_flow_streak_dir_v2', "")

            def _flow_bar(v, w=8):
                h = w // 2
                f = min(int(abs(v) * h + 0.5), h)
                return ("·" * h + "█" * f + "░" * (h - f)) if v >= 0 \
                    else ("░" * (h - f) + "█" * f + "·" * h)

            def _fl(label, val):
                arrow = "▲" if val > 0.1 else ("▼" if val < -0.1 else "─")
                return f"  {label:<12} {_flow_bar(val)} {arrow} {val:+.3f}"

            lines.append(_fl("CVD div",    cvd_div))
            lines.append(_fl("OB delta",   ob_imbalance))
            lines.append(_fl("Tick aggr",  tick_flow))
            lines.append(_fl("CVD trend",  cvd_trend))

            # Flow conviction: weighted average of the three primary detectors
            signals     = [cvd_div * 0.40, ob_imbalance * 0.35, tick_flow * 0.25]
            conviction  = sum(signals)
            flow_dir    = "long" if conviction > 0.20 else ("short" if conviction < -0.20 else "")

            # Is flow toward the target pool?
            # BUG-FIX C24: Reuse the pt already fetched from the earlier snapshot
            # rather than calling get_snapshot() a third time.  PoolTarget has no
            # .level_type attribute — the correct path is .pool.side.value.
            # Note: pt may already be set from the Layer 1 section above; if the
            # liq_map block raised an exception pt remains None from the init below.
            pt = None
            if liq_map is not None:
                try:
                    pt = liq_map.get_snapshot(price, atr).primary_target
                except Exception:
                    pass

            toward_pool = False
            if pt is not None and flow_dir:
                if pt.pool.side.value == "BSL" and flow_dir == "long":
                    toward_pool = True
                elif pt.pool.side.value == "SSL" and flow_dir == "short":
                    toward_pool = True

            flow_gate_str = (
                f"✅ TOWARD {'BSL ▲' if flow_dir == 'long' else 'SSL ▼'}  conv={conviction:+.3f}"
                if toward_pool else
                f"❌ NOT toward pool  conv={conviction:+.3f}  dir={flow_dir or 'neutral'}"
            )
            lines.append(f"\n  {flow_gate_str}")
            if streak > 1:
                lines.append(f"  Streak: {streak} ticks {streak_dir}")

            # ══════════════════════════════════════════════════════════
            # LAYER 3 — ICT SECONDARY VALIDATION
            # AMD phase, OB/FVG alignment, premium/discount zone
            # ══════════════════════════════════════════════════════════
            lines.append("\n<b>━━ LAYER 3: ICT SECONDARY VALIDATION</b>")

            if ict and ict._initialized:
                # AMD phase
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

                    # Is AMD phase compatible with flow direction?
                    amd_compat = True
                    amd_note   = ""
                    if flow_dir == "long" and amd_bias == "bearish" and amd_conf >= 0.75:
                        amd_compat = False
                        amd_note   = "  ⚠️ AMD bearish (high conf) vs long flow"
                    elif flow_dir == "short" and amd_bias == "bullish" and amd_conf >= 0.75:
                        amd_compat = False
                        amd_note   = "  ⚠️ AMD bullish (high conf) vs short flow"

                    lines.append(
                        f"  {amd_icon} AMD: <b>{_esc(amd_phase)}</b>  "
                        f"{bias_icon}{_esc(amd_bias)}  conf={amd_conf:.2f}  "
                        f"{'✅' if amd_compat else '❌'} flow-compatible")
                    if amd_note:
                        lines.append(amd_note)
                except Exception as _ae:
                    lines.append(f"  AMD: error — {_ae}")

                # OB/FVG in path toward target pool
                try:
                    long_c  = ict.get_confluence("long",  price, now_ms)
                    short_c = ict.get_confluence("short", price, now_ms)
                    active_c = long_c if flow_dir == "long" else (short_c if flow_dir == "short" else long_c)
                    lines.append(
                        f"  ICT confluence ({flow_dir or 'n/a'}): Σ={active_c.total:.2f}  "
                        f"OB={active_c.ob_score:.2f}  FVG={active_c.fvg_score:.2f}  "
                        f"Sweep={active_c.sweep_score:.2f}  KZ={active_c.session_score:.2f}")
                    if active_c.details:
                        lines.append(f"  → {_esc(active_c.details)}")
                except Exception:
                    pass

                # Premium / discount zone
                mtf_in_disc  = getattr(sig, 'in_discount', False)
                mtf_in_prem  = getattr(sig, 'in_premium',  False)
                zone_str     = ("💰 DISCOUNT" if mtf_in_disc
                                else ("💸 PREMIUM" if mtf_in_prem else "〰️ EQUILIBRIUM"))
                lines.append(f"  Price zone: {zone_str}")
            else:
                lines.append("  ⏳ ICT engine initialising")

            # ══════════════════════════════════════════════════════════
            # LAYER 4 — ENTRY GATE
            # Sweep/OTE status, SL at ICT structure, TP at pool
            # ══════════════════════════════════════════════════════════
            lines.append("\n<b>━━ LAYER 4: ENTRY</b>")

            sweep = getattr(strat, '_active_sweep_setup', None)
            entry_engine = getattr(strat, '_entry_engine', None)
            engine_state = getattr(entry_engine, 'state', 'SCANNING') if entry_engine else 'SCANNING'

            lines.append(f"  Engine: <b>{_esc(engine_state)}</b>")

            if sweep is not None:
                age_m   = (now_ms - sweep.setup_time_ms) / 60_000
                in_ote  = sweep.ote_entry_zone_low <= price <= sweep.ote_entry_zone_high
                s_icon  = "🟢" if sweep.status == "OTE_READY" else "🔵"
                lines.append(
                    f"  {s_icon} Sweep setup: <b>{sweep.side.upper()}</b>  "
                    f"status={_esc(sweep.status)}  quality={sweep.quality_score():.2f}  age={age_m:.0f}m")
                lines.append(
                    f"  Sweep: ${sweep.sweep_price:,.0f}  "
                    f"OTE zone: [${sweep.ote_entry_zone_low:,.0f}–${sweep.ote_entry_zone_high:,.0f}]")
                if in_ote:
                    lines.append("  ✅ Price IN OTE zone — limit entry eligible")
                    # SL info
                    lines.append(f"  SL (ICT): wick ${sweep.sl_sweep_candle:,.0f}")
                else:
                    # BUG-FIX C31: Operator precedence in the original expression caused
                    # both sides to use the wrong zone edge.  For a LONG setup price must
                    # rise to the OTE LOW (discount zone entry); for SHORT, drop to HIGH.
                    if sweep.side == "long":
                        dist = max(0.0, sweep.ote_entry_zone_low - price)
                    else:
                        dist = max(0.0, price - sweep.ote_entry_zone_high)
                    lines.append(f"  ⏳ {dist:.0f}pts to OTE")

                # TP = opposing pool
                if sweep.delivery_target:
                    lines.append(f"  TP (pool): ${sweep.delivery_target:,.0f}")
                else:
                    lines.append("  TP: awaiting opposing pool identification")

                # FVG / OB in OTE
                extras = []
                if getattr(sweep, 'has_fvg_in_ote', False): extras.append("FVG✓")
                if getattr(sweep, 'has_ob_in_ote',  False): extras.append("OB✓")
                if extras:
                    lines.append(f"  ICT in OTE: {' '.join(extras)}")
            else:
                lines.append("  ─ No active sweep setup")
                lines.append("  Waiting for pool sweep + displacement confirmation")

            # Risk gate
            can_ok, risk_reason = rm.can_trade()
            lines.append(
                f"\n  {'✅' if can_ok else '🚫'} Risk gate: "
                + (f"OPEN" if can_ok else f"BLOCKED — {_esc(risk_reason)}"))

            cooldown_sec = float(getattr(cfg, 'QUANT_COOLDOWN_SEC', 180))
            last_exit    = getattr(strat, '_last_exit_time', 0)
            cd_rem       = max(0.0, cooldown_sec - (now - last_exit))
            lines.append(
                f"  {'✅' if cd_rem == 0 else '⏳'} Cooldown: "
                + (f"{cd_rem:.0f}s remaining" if cd_rem > 0 else "ready"))

            # ══════════════════════════════════════════════════════════
            # LAYER 5 — TRAIL / POST-SWEEP ENGINE
            # BOS swing → CHoCH tighten → 15m structure
            # ══════════════════════════════════════════════════════════
            from strategy.quant_strategy import PositionPhase
            lines.append("\n<b>━━ LAYER 5: TRAIL / POST-SWEEP ENGINE</b>")

            if pos.phase == PositionPhase.ACTIVE:
                profit_pts  = (pos.entry_price - price if pos.side == "short"
                               else price - pos.entry_price)
                init_dist   = pos.initial_sl_dist if pos.initial_sl_dist > 1e-10 else atr
                tier_r      = max(profit_pts, pos.peak_profit) / init_dist

                be_r   = float(getattr(cfg, 'QUANT_TRAIL_BE_R',        0.3))
                lock_r = float(getattr(cfg, 'QUANT_TRAIL_LOCK_R',       0.8))
                aggr_r = float(getattr(cfg, 'QUANT_TRAIL_AGGRESSIVE_R', 1.5))

                if   tier_r < be_r:   phase_lbl = f"⬜ HANDS OFF (<{be_r:.1f}R) — no trail yet"
                elif tier_r < lock_r: phase_lbl = f"🟡 BOS SWING TRAIL ({be_r:.1f}→{lock_r:.1f}R)"
                elif tier_r < aggr_r: phase_lbl = f"🟠 CHoCH TIGHTEN ({lock_r:.1f}→{aggr_r:.1f}R)"
                else:                 phase_lbl = f"🟢 15m STRUCTURE TRAIL (>{aggr_r:.1f}R)"

                lines.append(f"  {pos.side.upper()}  {phase_lbl}")
                lines.append(
                    f"  Entry ${pos.entry_price:,.2f}  "
                    f"SL ${pos.sl_price:,.2f}  "
                    f"TP ${pos.tp_price:,.2f}  ({tier_r:.2f}R)")

                # Post-sweep decision
                lines.append(f"\n  Post-sweep: CVD + structure → continue/reverse/range")
                if abs(cvd_div) > 0.3:
                    post_bias = "continue ▲" if (cvd_div > 0 and pos.side == "long") else \
                                "continue ▼" if (cvd_div < 0 and pos.side == "short") else \
                                "⚠️ flow diverging — watch for reversal"
                    lines.append(f"  CVD signal: {post_bias}")
            else:
                lines.append("  ─ No active position")
                if toward_pool:
                    lines.append(f"  Scanning for sweep entry toward {'BSL' if flow_dir == 'long' else 'SSL'}...")
                else:
                    lines.append("  Waiting for flow to align with pool direction")

            # ══════════════════════════════════════════════════════════
            # VERDICT
            # ══════════════════════════════════════════════════════════
            lines.append("\n<b>━━ VERDICT</b>")
            if pos.phase == PositionPhase.ACTIVE:
                lines.append("  📍 Position ACTIVE — managing via ICT structure trail")
            elif toward_pool and sweep is not None and sweep.status == "OTE_READY" and can_ok and cd_rem == 0:
                lines.append("  🎯 <b>ALL LAYERS GREEN — entry eligible at OTE</b>")
            elif toward_pool and sweep is None:
                lines.append("  ⏳ Flow confirmed → awaiting sweep + displacement")
            elif not toward_pool:
                lines.append("  👀 Watching — flow not yet toward target pool")
            else:
                missing = []
                if not toward_pool:    missing.append("Flow not toward pool")
                if sweep is None:      missing.append("No sweep setup")
                if not can_ok:         missing.append(f"Risk: {_esc(risk_reason)}")
                if cd_rem > 0:         missing.append(f"Cooldown {cd_rem:.0f}s")
                lines.append("  👀 <b>Watching</b> — blocked by:")
                for m in missing:
                    lines.append(f"    • {m}")

            self.send_message("\n".join(lines))
            return None

        except Exception as e:
            logger.error(f"Thinking error: {e}", exc_info=True)
            return f"❌ Thinking error: {e}"

    # ================================================================
    # /pools
    # ================================================================

    def _cmd_pools(self) -> str:
        """Show full liquidity pool map with priority scores."""
        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return "Bot not running."
        try:
            strat = bot_instance.strategy
            dm    = bot_instance.data_manager
            if not strat or not dm:
                return "Components not ready."

            price = dm.get_last_price()
            atr   = strat._atr_5m.atr

            if not hasattr(strat, '_liq_map') or strat._liq_map is None:
                return "Liquidity map not available (v9 engine not active)."

            snap    = strat._liq_map.get_snapshot(price, atr)
            summary = strat._liq_map.get_status_summary(price, atr)

            if _V9_DISPLAY:
                msg = format_pools_telegram(
                    price=price, atr=atr,
                    bsl_pools=snap.bsl_pools, ssl_pools=snap.ssl_pools,
                    primary_target=snap.primary_target,
                    recent_sweeps=snap.recent_sweeps,
                    tf_coverage=summary.get("tf_coverage", {}),
                )
                self.send_message(msg)
                return None

            # Fallback plain text pool display
            lines = [f"<b>💧 Liquidity Pool Map @ ${price:,.2f}</b>  ATR=${atr:.1f}"]
            pt = snap.primary_target
            # BUG-FIX C25: PoolTarget has no .level_type or .price attributes.
            # Correct paths: pt.pool.side.value and pt.pool.price.
            if pt:
                lines.append(f"\n🎯 <b>Primary target: "
                              f"{'BSL' if pt.pool.side.value == 'BSL' else 'SSL'} "
                              f"@ ${pt.pool.price:,.1f}</b>")

            # BUG-FIX C26: PoolTarget .price → .pool.price, priority_score → .significance,
            # touch_count → .pool.touches. Filter and sort via .pool.price.
            lines.append("\n<b>▲ BSL pools</b>")
            for p in sorted(
                [p for p in snap.bsl_pools if p.pool.price > price],
                key=lambda x: x.pool.price,
            )[:6]:
                d = (p.pool.price - price) / max(atr, 1)
                s = p.significance
                lines.append(
                    f"  ${p.pool.price:,.1f}  {d:.1f}ATR  "
                    f"x{p.pool.touches}  score={s:.2f}")

            # BUG-FIX C27: same fixes for SSL
            lines.append("\n<b>▼ SSL pools</b>")
            for p in sorted(
                [p for p in snap.ssl_pools if p.pool.price < price],
                key=lambda x: x.pool.price,
                reverse=True,
            )[:6]:
                d = (price - p.pool.price) / max(atr, 1)
                s = p.significance
                lines.append(
                    f"  ${p.pool.price:,.1f}  {d:.1f}ATR  "
                    f"x{p.pool.touches}  score={s:.2f}")

            # BUG-FIX C28: SweepResult has no .displacement_confirmed, .level_type,
            # or .price. Correct attrs: quality, pool.side.value, pool.price.
            if snap.recent_sweeps:
                lines.append(f"\n🌊 <b>Recent sweeps:</b> {len(snap.recent_sweeps)}")
                for sw in snap.recent_sweeps[-3:]:
                    disp = "DISP✓" if sw.quality >= 0.5 else "weak"
                    lines.append(
                        f"  {sw.pool.side.value} ${sw.pool.price:,.1f} [{disp}]")

            self.send_message("\n".join(lines))
            return None
        except Exception as e:
            logger.error(f"Pools error: {e}", exc_info=True)
            return f"Error: {e}"

    # ================================================================
    # /flow
    # ================================================================

    def _cmd_flow(self) -> str:
        """Show detailed orderflow state (CVD + OB delta + tick aggression)."""
        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return "Bot not running."
        try:
            strat = bot_instance.strategy
            dm    = bot_instance.data_manager
            if not strat or not dm:
                return "Components not ready."

            price = dm.get_last_price()

            tick_flow = strat._tick_eng.get_signal() if strat._tick_eng else 0.0
            cvd_trend = strat._cvd.get_trend_signal() if strat._cvd else 0.0
            cvd_div   = 0.0
            try:
                cvd_div = strat._cvd.get_divergence_signal(dm.get_candles("1m", limit=60))
            except Exception:
                pass

            ob_imbalance = 0.0
            try:
                ob = dm.get_orderbook()
                if ob and ob.get("bids") and ob.get("asks"):
                    bv = sum(float(b[1]) for b in ob["bids"][:10])
                    av = sum(float(a[1]) for a in ob["asks"][:10])
                    total_vol = bv + av
                    if total_vol > 0:
                        ob_imbalance = (bv - av) / total_vol
            except Exception:
                pass

            streak     = getattr(strat, '_flow_streak_count_v2', 0)
            streak_dir = getattr(strat, '_flow_streak_dir_v2', "")

            signals    = [cvd_div * 0.40, ob_imbalance * 0.35, tick_flow * 0.25]
            conviction = sum(signals)
            direction  = "long ▲" if conviction > 0.20 else ("short ▼" if conviction < -0.20 else "neutral")

            if _V9_DISPLAY:
                msg = format_flow_telegram(
                    price=price, tick_flow=tick_flow,
                    cvd_trend=cvd_trend, cvd_divergence=cvd_div,
                    ob_imbalance=ob_imbalance,
                    tick_streak=streak, streak_direction=streak_dir,
                    flow_conviction=conviction, flow_direction=direction,
                )
                self.send_message(msg)
                return None

            def bar(v, w=10):
                h = w // 2
                f = min(int(abs(v) * h + 0.5), h)
                return ("·" * h + "█" * f + "░" * (h - f)) if v >= 0 \
                    else ("░" * (h - f) + "█" * f + "·" * h)

            lines = [f"<b>📊 Flow Direction @ ${price:,.2f}</b>"]
            lines.append(f"\n  {'CVD div':<14} {bar(cvd_div)} {cvd_div:+.3f}")
            lines.append(f"  {'OB delta':<14} {bar(ob_imbalance)} {ob_imbalance:+.3f}")
            lines.append(f"  {'Tick aggression':<14} {bar(tick_flow)} {tick_flow:+.3f}")
            lines.append(f"  {'CVD trend':<14} {bar(cvd_trend)} {cvd_trend:+.3f}")
            lines.append(f"\n  Conviction: <b>{conviction:+.3f}</b>  → {direction}")
            if streak > 1:
                lines.append(f"  Streak: {streak} ticks {streak_dir}")

            self.send_message("\n".join(lines))
            return None
        except Exception as e:
            logger.error(f"Flow error: {e}", exc_info=True)
            return f"Error: {e}"

    # ================================================================
    # /status
    # ================================================================

    def _cmd_status(self) -> str:
        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return "Bot not running. Use /start"
        try:
            strat = bot_instance.strategy
            if not strat:
                return "Strategy not ready."
            report = strat.format_status_report()
            self.send_message(report)
            return None
        except Exception as e:
            logger.error(f"Status error: {e}", exc_info=True)
            return f"❌ Status error: {e}"

    # ================================================================
    # /structures  (ICT secondary layer)
    # ================================================================

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
            ict    = getattr(strat, '_ict', None)

            if ict is None:
                return "❌ ICT engine not available."
            if not ict._initialized:
                nb = len(list(ict.order_blocks_bull))
                ns = len(list(ict.order_blocks_bear))
                return (f"⏳ ICT engine warming up...\n"
                        f"Detected so far: {nb}🟢 {ns}🔴 OBs\n"
                        f"Need ≥10 candles + 5s update cycle.")

            atr_eng = getattr(strat, '_atr_5m', None)
            atr_v   = atr_eng.atr if atr_eng and atr_eng.atr > 1e-10 else price * 0.002
            st      = ict.get_full_status(price, atr_v, now_ms)
            c       = st["counts"]
            sess    = st.get("session", "UNKNOWN")
            kz      = st.get("killzone", "")
            kz_s    = f" [{kz}]" if kz else ""

            lines = [
                f"🏛️ <b>ICT Structures (secondary) @ ${price:,.1f}</b>",
                f"<i>AMD phase, OB/FVG alignment, P/D zone — confirms pool flow</i>",
                f"Session: {_esc(sess)}{_esc(kz_s)}  |  ATR(5m): ${atr_v:.1f}",
                (f"OBs: {c['ob_bull']}🟢 {c['ob_bear']}🔴  "
                 f"FVGs: {c['fvg_bull']}🟦 {c['fvg_bear']}🟥  "
                 f"Liq: {c['liq_active']} active / {c['liq_swept']} swept"),
                "",
            ]

            if st["bull_obs"]:
                lines.append("<b>🟢 Bullish Order Blocks</b>")
                for ob in st["bull_obs"][:5]:
                    ote_lo  = ob["high"] - 0.79 * (ob["high"] - ob["low"])
                    ote_hi  = ob["high"] - 0.50 * (ob["high"] - ob["low"])
                    in_tag  = " ◄ IN OB" if ob["in_ob"] else (" ← OTE" if ob["in_ote"] else "")
                    bos     = " BOS✓" if ob["bos"] else ""
                    lines.append(
                        f"  ${ob['low']:,.1f}–${ob['high']:,.1f}  "
                        f"str={ob['strength']:.0f}  v={ob['visit_count']}  "
                        f"age={ob['age_min']:.0f}m"
                        f"  OTE:${ote_lo:,.0f}–${ote_hi:,.0f}{bos}{in_tag}"
                    )

            if st["bear_obs"]:
                lines.append("\n<b>🔴 Bearish Order Blocks</b>")
                for ob in st["bear_obs"][:5]:
                    ote_lo  = ob["low"] + 0.50 * (ob["high"] - ob["low"])
                    ote_hi  = ob["low"] + 0.79 * (ob["high"] - ob["low"])
                    in_tag  = " ◄ IN OB" if ob["in_ob"] else (" ← OTE" if ob["in_ote"] else "")
                    bos     = " BOS✓" if ob["bos"] else ""
                    lines.append(
                        f"  ${ob['low']:,.1f}–${ob['high']:,.1f}  "
                        f"str={ob['strength']:.0f}  v={ob['visit_count']}  "
                        f"age={ob['age_min']:.0f}m"
                        f"  OTE:${ote_lo:,.0f}–${ote_hi:,.0f}{bos}{in_tag}"
                    )

            all_fvgs = st["bull_fvgs"][:3] + st["bear_fvgs"][:3]
            if all_fvgs:
                lines.append("\n<b>Fair Value Gaps</b>")
                for fvg in sorted(all_fvgs, key=lambda x: abs(x["dist_pts"])):
                    icon   = "🟦" if fvg["direction"] == "bullish" else "🟥"
                    in_tag = " ◄ IN GAP" if fvg["in_gap"] else ""
                    lines.append(
                        f"  {icon} ${fvg['bottom']:,.1f}–${fvg['top']:,.1f}  "
                        f"size=${fvg['size']:.1f}  fill={fvg['fill_pct']:.0%}  "
                        f"{fvg['dist_pts']:+.0f}pts/{fvg['dist_atr']:.2f}ATR{in_tag}"
                    )

            long_c  = ict.get_confluence("long",  price, now_ms)
            short_c = ict.get_confluence("short", price, now_ms)
            lines.append("\n<b>Confluence Scores (ICT secondary)</b>")
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

    # ================================================================
    # /position
    # ================================================================

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

        p     = strat._pos
        price = dm.get_last_price() if dm else 0.0
        atr   = strat._atr_5m.atr

        side   = p.side.upper()
        entry  = p.entry_price
        sl     = p.sl_price
        tp     = p.tp_price
        qty    = p.quantity
        mode   = p.trade_mode
        phase  = p.phase.name

        init_sl_dist    = getattr(p, 'initial_sl_dist', 0.0)
        sl_dist         = init_sl_dist if init_sl_dist > 1e-10 else (abs(entry - sl) if sl > 0 else 0.0)
        current_sl_dist = abs(entry - sl) if sl > 0 else 0.0

        if sl_dist > 1e-10:
            current_r  = ((price - entry) / sl_dist if side == "LONG"
                          else (entry - price) / sl_dist)
            planned_rr = abs(tp - entry) / sl_dist if tp > 0 else 0.0
        else:
            current_r  = 0.0
            planned_rr = 0.0

        upnl      = ((price - entry) * qty if side == "LONG" else (entry - price) * qty)
        hold_min  = (time.time() - p.entry_time) / 60.0 if p.entry_time > 0 else 0.0
        peak_prof = getattr(p, 'peak_profit', 0.0)
        mfe_r     = peak_prof / sl_dist if sl_dist > 1e-10 else 0.0
        be_moved  = ((side == "LONG"  and sl >= entry) or
                     (side == "SHORT" and sl <= entry and sl > 0))

        # Pool TP context
        pool_tp_note = ""
        if hasattr(strat, '_liq_map') and strat._liq_map is not None:
            try:
                snap = strat._liq_map.get_snapshot(price, atr)
                # BUG-FIX C29a: The pools were inverted.
                # LONG trades ride UP to BSL (buy-side above); SHORT trades fall to SSL below.
                # Old code gave LONG→SSL (below) and SHORT→BSL (above) — completely backwards.
                # BUG-FIX C29b: PoolTarget has no .price — all lookups go through .pool.price.
                if side == "LONG":
                    opp_pools = [pp for pp in snap.bsl_pools if pp.pool.price > price]
                else:
                    opp_pools = [pp for pp in snap.ssl_pools if pp.pool.price < price]
                if opp_pools:
                    opp = sorted(opp_pools,
                                 key=lambda pp: abs(pp.pool.price - tp))[0]
                    pool_tp_note = (
                        f"\nPool TP basis: ${opp.pool.price:,.1f} "
                        f"(opposing {'BSL' if side == 'LONG' else 'SSL'})"
                    )
            except Exception:
                pass

        return (
            f"<b>Active Position — {side}</b>\n"
            f"Mode: {mode.upper()}  |  Phase: {phase}\n\n"
            f"Entry:   ${entry:,.2f}\n"
            f"Current: ${price:,.2f}  ({current_r:+.2f}R)\n"
            f"uPnL:    ${upnl:+,.2f}\n"
            f"Qty:     {qty:.4f} BTC\n\n"
            f"SL (ICT struct): ${sl:,.2f}  "
            f"(init dist: ${sl_dist:.0f} / {sl_dist/max(atr,1):.2f}ATR  "
            f"current dist: ${current_sl_dist:.0f})\n"
            f"TP (pool):       ${tp:,.2f}{pool_tp_note}\n"
            f"Planned R:R: 1:{planned_rr:.2f}\n"
            f"MFE: {mfe_r:.2f}R\n\n"
            f"Hold:  {hold_min:.1f}m\n"
            f"Trail: {'✅ BOS/CHoCH active' if p.trail_active else '⏳ waiting for R threshold'}\n"
            f"BE:    {'Yes ✅' if be_moved else 'No'}"
        )

    # ================================================================
    # /trades
    # ================================================================

    def _cmd_trades(self) -> str:
        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return "Bot not running."

        strat = bot_instance.strategy
        rm    = bot_instance.risk_manager
        if not strat or not rm:
            return "Components not ready."

        history = getattr(strat, '_trade_history', [])
        lines   = ["<b>📋 Trade History</b>\n"]

        if history:
            for t in reversed(history[-10:]):
                side      = t.get('side', '?').upper()
                mode      = t.get('mode', '?').upper()
                entry     = t.get('entry', 0.0)
                exit_p    = t.get('exit',  0.0)
                pnl       = t.get('pnl',   0.0)
                reason    = t.get('reason', '?')
                hold      = t.get('hold_min', 0.0)
                mfe_r     = t.get('mfe_r', 0.0)
                trailed   = t.get('trailed', False)
                is_win    = t.get('is_win', False)
                init_sl   = t.get('init_sl_dist', 0.0)
                ict_tier  = t.get('ict_tier', '')
                pool_tp   = t.get('pool_tp_price', 0.0)
                raw_pts   = ((exit_p - entry) if side == "LONG" else (entry - exit_p))
                ach_r     = raw_pts / init_sl if init_sl > 1e-10 else 0.0

                total_fees = t.get('total_fees', 0.0)
                exact_fees = t.get('exact_fees', False)

                if   reason == "tp_hit":       label = "🎯 TP (pool sweep)"
                elif reason == "trail_sl_hit": label = "🔒 TRAIL (ICT struct)"
                elif reason == "sl_hit":       label = "🛑 SL (ICT struct)"
                else:                          label = f"🚪 {reason[:8]}"

                result    = "✅" if is_win else "❌"
                trail_tag = " [T]" if trailed else ""
                tier_badge = f" [T{ict_tier}]" if ict_tier else ""
                pool_tp_tag = f"  pool_tp=${pool_tp:,.0f}" if pool_tp else ""

                fee_line = ""
                if total_fees > 0:
                    fee_tag  = "exact" if exact_fees else "est."
                    fee_line = f"\n    Fees({fee_tag}): ${total_fees:.4f}"

                lines.append(
                    f"{result} {side} [{mode}]{tier_badge}  "
                    f"${entry:,.0f}→${exit_p:,.0f}  "
                    f"PnL: <b>${pnl:+.2f}</b>  R: {ach_r:+.2f}  MFE: {mfe_r:.1f}R\n"
                    f"    {label}{trail_tag}  hold: {hold:.0f}m{pool_tp_tag}"
                    + fee_line
                )
        else:
            lines.append("  No trades recorded yet this session.")

        total_t   = getattr(strat, '_total_trades', 0)
        wins      = getattr(strat, '_winning_trades', 0)
        losses    = total_t - wins
        wr        = wins / total_t * 100.0 if total_t > 0 else 0.0
        total_pnl = getattr(strat, '_total_pnl', 0.0)

        daily_cnt = strat._risk_gate.daily_trades if hasattr(strat, '_risk_gate') else 0
        consec    = strat._risk_gate.consec_losses if hasattr(strat, '_risk_gate') else 0
        max_d     = getattr(__import__('config'), 'MAX_DAILY_TRADES', 8)

        win_pnls  = [t['pnl'] for t in history if t.get('is_win')]
        loss_pnls = [t['pnl'] for t in history if not t.get('is_win')]
        avg_win   = sum(win_pnls)  / len(win_pnls)  if win_pnls  else 0.0
        avg_loss  = sum(loss_pnls) / len(loss_pnls) if loss_pnls else 0.0
        expectancy = (wr/100 * avg_win) + ((1 - wr/100) * avg_loss)

        total_fees_s = sum(t.get('total_fees', 0) for t in history)

        lines += [
            "",
            "━━━━━━━━━━━━━━━━━━━━━━━━",
            f"Session:   {total_t} trades  W:{wins} L:{losses}  WR: <b>{wr:.0f}%</b>",
            f"Total PnL: <b>${total_pnl:+.2f}</b> USDT",
            f"Avg Win:   ${avg_win:+.2f}  Avg Loss: ${avg_loss:+.2f}",
            f"Expectancy: ${expectancy:+.2f}/trade",
            f"Total Fees: ${total_fees_s:.4f}" if total_fees_s > 0 else "Total Fees: —",
            f"Today:     {daily_cnt}/{max_d} trades  consec_loss={consec}",
        ]
        return "\n".join(lines)

    # ================================================================
    # /stats
    # ================================================================

    def _cmd_stats(self) -> str:
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
        total = len(history)
        wins  = sum(1 for t in history if t.get('is_win'))
        wr_all = wins / total * 100.0
        lines.append(f"Total trades: {total}  WR: <b>{wr_all:.0f}%</b>\n")

        # By ICT tier
        lines.append("<b>By ICT Tier</b>")
        tier_groups: dict = {}
        for t in history:
            k = t.get('ict_tier', '') or 'none'
            tier_groups.setdefault(k, []).append(t)
        for tier in ['S', 'A', 'B', 'none']:
            grp = tier_groups.get(tier, [])
            if not grp: continue
            w   = sum(1 for t in grp if t.get('is_win'))
            wr  = w / len(grp) * 100.0
            avg = sum(t.get('pnl', 0) for t in grp) / len(grp)
            lines.append(f"  {'Tier-' + tier if tier != 'none' else 'No tier'}: {len(grp)} trades  WR={wr:.0f}%  avg net=${avg:+.2f}")

        # By exit reason (pool-centric)
        lines.append("\n<b>By Exit Reason</b>")
        exit_groups: dict = {}
        for t in history:
            k = t.get('reason', 'unknown')
            exit_groups.setdefault(k, []).append(t)
        for reason, grp in sorted(exit_groups.items()):
            w  = sum(1 for t in grp if t.get('is_win'))
            wr = w / len(grp) * 100.0
            lines.append(f"  {_esc(reason)}: {len(grp)} trades  WR={wr:.0f}%")

        # By AMD phase
        lines.append("\n<b>By AMD Phase</b>")
        amd_groups: dict = {}
        for t in history:
            k = t.get('amd_phase', 'UNKNOWN')
            amd_groups.setdefault(k, []).append(t)
        for phase, grp in sorted(amd_groups.items()):
            w  = sum(1 for t in grp if t.get('is_win'))
            wr = w / len(grp) * 100.0
            lines.append(f"  {_esc(phase)}: {len(grp)} trades  WR={wr:.0f}%")

        # PnL summary
        lines.append("\n<b>Realised PnL &amp; Fees</b>")
        total_net  = sum(t.get('pnl', 0) for t in history)
        total_fees = sum(t.get('total_fees', 0) for t in history)
        n_exact    = sum(1 for t in history if t.get('exact_fees'))
        n_est      = total - n_exact
        lines += [
            f"  Net PnL:    <b>${total_net:+.4f}</b> USDT",
            f"  Total Fees: ${total_fees:.4f} ({n_exact} exact, {n_est} est.)",
        ]
        return "\n".join(l for l in lines if l is not None)

    # ================================================================
    # /huntstatus
    # ================================================================

    def _cmd_huntstatus(self) -> str:
        if _V9_DISPLAY:
            return self._cmd_pools()

        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return "Bot not running."

        strat  = getattr(bot_instance, 'strategy', None)
        dm     = getattr(bot_instance, 'data_manager', None)
        if not strat or not dm:
            return "Components not ready."

        hunter = getattr(strat, '_liquidity_hunter', None)
        if hunter is None:
            # Fall back to pool map if no dedicated hunter
            return self._cmd_pools()

        price = dm.get_last_price()
        atr   = strat._atr_5m.atr
        st    = hunter.get_status_dict()

        state_icons = {
            "NO_RANGE":        "⚪",
            "RANGING":         "🔵",
            "STALKING":        "🟡",
            "APPROACHING":     "🟠",
            "SWEEP_CONFIRMED": "🟢",
            "CISD_WAIT":       "🔍",
            "OTE_WAIT":        "📐",
        }
        s_icon = state_icons.get(st["state"], "❓")

        lines = [f"<b>🎣 Liquidity Hunt Engine</b>"]
        lines.append(f"Price: ${price:,.2f}  ATR: ${atr:.1f}")
        lines.append(f"\n<b>State:</b> {s_icon} {st['state']}")

        if st["bsl"] is not None and st["ssl"] is not None:
            bsl     = st["bsl"]
            ssl     = st["ssl"]
            rng     = bsl - ssl
            rng_atr = rng / atr if atr > 1e-10 else 0
            pd_pct  = (price - ssl) / rng * 100 if rng > 0 else 0
            lines.append(f"SSL ${ssl:,.1f} ─ price ${price:,.2f} ({pd_pct:.0f}%) ─ BSL ${bsl:,.1f}")
            lines.append(f"Range: {rng:.0f}pts / {rng_atr:.1f}ATR")
        else:
            lines.append("No active BSL/SSL range detected.")

        pred = st.get("predicted_dir", "")
        pred_str = "▲ BSL" if pred == "bsl" else ("▼ SSL" if pred == "ssl" else "─ none")
        lines.append(f"\n<b>Prediction:</b> {pred_str}")
        lines.append(f"  Score: {st['score_ema']:+.3f}  raw: {st['raw_score']:+.3f}")

        if st.get("signal_ready"):
            lines.append(f"\n<b>🎯 SIGNAL READY</b>")
            lines.append(f"  Side:  <b>{(st['signal_side'] or '?').upper()}</b>")
            lines.append(f"  SL:    ${st['signal_sl']:,.1f}  TP: ${st['signal_tp']:,.1f}")
            lines.append(f"  R:R:   1:{st['signal_rr']:.2f}")
        else:
            lines.append("\n  No pending signal.")

        return "\n".join(lines)

    # ================================================================
    # /balance
    # ================================================================

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

    # ================================================================
    # /pause / /resume / /trail
    # ================================================================

    def _cmd_pause(self) -> str:
        global bot_instance
        if not bot_instance: return "Bot not running."
        bot_instance.trading_enabled      = False
        bot_instance.trading_pause_reason = "Paused via Telegram"
        return "⏸️ Trading PAUSED. Pool monitoring continues. Use /resume to re-enable."

    def _cmd_resume(self) -> str:
        global bot_instance
        if not bot_instance: return "Bot not running."
        bot_instance.trading_enabled      = True
        bot_instance.trading_pause_reason = ""
        return "▶️ Trading RESUMED."

    def _cmd_trail(self, args: str) -> str:
        global bot_instance
        if not bot_instance: return "Bot not running."
        strat = bot_instance.strategy
        if not strat: return "Strategy not ready."
        arg = (args or "").strip().lower()
        if arg in ("on", "enable", "1", "true", "yes"):
            strat.set_trail_override(True)
            return "🔒 Trailing SL: FORCED ON (ICT structure only)"
        elif arg in ("off", "disable", "0", "false", "no"):
            strat.set_trail_override(False)
            return "🔓 Trailing SL: FORCED OFF"
        else:
            strat.set_trail_override(None)
            enabled = strat.get_trail_enabled()
            return (f"🔄 Trailing SL: AUTO  "
                    f"(config default = {'ON' if enabled else 'OFF'})")

    # ================================================================
    # /config
    # ================================================================

    def _cmd_config(self) -> str:
        import config as cfg
        lines = [
            "<b>Bot Configuration (liquidity-first)</b>\n",
            f"Symbol:     {cfg.SYMBOL}",
            f"Exchange:   {cfg.EXECUTION_EXCHANGE.upper()}",
            f"Leverage:   {cfg.LEVERAGE}x",
            "",
            "<b>Architecture</b>",
            "  Primary:   BSL/SSL pool map + flow detector",
            "  Secondary: ICT structures (OB/FVG/AMD)",
            "  Entry:     Limit at OTE | Market at sweep",
            "  SL:        ICT structure (wick→OB→swing)",
            "  TP:        Opposing liquidity pool sweep price",
            "  Trail:     ICT structure (BOS→CHoCH→15m)",
            "",
            "<b>Position Sizing</b>",
            f"  Risk/trade:   {getattr(cfg,'RISK_PER_TRADE',0.60):.2f}% of balance",
            f"  Min margin:   ${getattr(cfg,'MIN_MARGIN_PER_TRADE',4.0):.2f} USDT",
            f"  Max position: {getattr(cfg,'MAX_POSITION_SIZE',1.0)} BTC",
            "",
            "<b>Entry</b>",
            f"  Confirm ticks: {getattr(cfg,'QUANT_CONFIRM_TICKS',2)}",
            f"  Cooldown:      {getattr(cfg,'QUANT_COOLDOWN_SEC',180)}s",
            f"  Max hold:      {getattr(cfg,'QUANT_MAX_HOLD_SEC',2400)}s",
            "",
            "<b>Risk</b>",
            f"  Min R:R:          {getattr(cfg,'MIN_RISK_REWARD_RATIO',0.8)}",
            f"  Max daily trades: {getattr(cfg,'MAX_DAILY_TRADES',8)}",
            f"  Max daily loss:   {getattr(cfg,'MAX_DAILY_LOSS_PCT',5.0):.1f}%",
            f"  Max consec loss:  {getattr(cfg,'MAX_CONSECUTIVE_LOSSES',3)}",
            "",
            "<b>Trail</b>",
            f"  Enabled:    {getattr(cfg,'QUANT_TRAIL_ENABLED',True)}",
            f"  BE (0→1):   {getattr(cfg,'QUANT_TRAIL_BE_R',0.3)}R",
            f"  BOS (1→2):  {getattr(cfg,'QUANT_TRAIL_LOCK_R',0.8)}R",
            f"  CHoCH(2→3): {getattr(cfg,'QUANT_TRAIL_AGGRESSIVE_R',1.5)}R",
        ]
        return "\n".join(lines)

    # ================================================================
    # /killswitch
    # ================================================================

    def _cmd_killswitch(self) -> str:
        global bot_instance
        if not bot_instance: return "Bot not running."
        try:
            import config
            bot_instance.trading_enabled = False
            om      = bot_instance.order_manager
            results = []

            if not om:
                return "❌ Order manager not available."

            try:
                swept = om.cancel_symbol_conditionals(symbol=config.SYMBOL)
                results.append(f"✅ Swept {len(swept)} conditional order(s)")
            except Exception as e:
                results.append(f"⚠️ Cancel error: {e}")

            try:
                pos = om.get_open_position()
                if pos and float(pos.get("size", 0)) > 0:
                    pos_side   = str(pos.get("side", "")).upper()
                    close_side = "SELL" if pos_side == "LONG" else "BUY"
                    qty        = float(pos["size"])
                    resp       = om.place_market_order(side=close_side, quantity=qty, reduce_only=True)
                    if resp:
                        results.append(f"✅ Closed {pos_side} ({qty} BTC)")
                    else:
                        results.append(f"⚠️ Close order returned None")
                else:
                    results.append("ℹ️ No open position on exchange")
            except Exception as e:
                results.append(f"⚠️ Close error: {e}")

            strat = bot_instance.strategy
            if strat:
                try:
                    # BUG-FIX C30: bare 'from quant_strategy import' fails when the module
                    # is loaded as strategy.quant_strategy (ModuleNotFoundError).
                    from strategy.quant_strategy import PositionState
                    with strat._lock:
                        strat._pos = PositionState()
                        strat._confirm_long = strat._confirm_short = 0
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

    # ================================================================
    # /resetrisk
    # ================================================================

    def _cmd_resetrisk(self, args: str) -> str:
        global bot_instance
        if not bot_instance: return "Bot not running."

        strat = getattr(bot_instance, 'strategy', None)
        rm    = getattr(bot_instance, 'risk_manager', None)
        if strat is None: return "❌ Strategy not initialised."

        try:
            from strategy.quant_strategy import PositionPhase
            with strat._lock:
                phase = strat._pos.phase
            if phase not in (PositionPhase.FLAT, PositionPhase.ENTERING):
                return (
                    "❌ Cannot reset risk gate while position is open.\n"
                    f"Current phase: {phase.name}\n"
                    "Close the position first, then /resetrisk."
                )
        except Exception as e:
            logger.warning(f"resetrisk phase check error: {e}")

        reset_daily = "full" in args.lower()
        lines = ["🔄 <b>Risk Gate Reset</b>"]

        gate = getattr(strat, '_risk_gate', None)
        if gate is not None:
            try:
                gate_result = gate.force_reset(reset_consec=True, reset_daily=reset_daily)
                lines.append(f"  DailyRiskGate: {gate_result}")
            except Exception as e:
                lines.append(f"  DailyRiskGate error: {e}")
        else:
            lines.append("  ⚠️ DailyRiskGate not found on strategy")

        if rm is not None:
            try:
                with rm._lock:
                    prev_cl = rm.consecutive_losses
                    prev_dp = rm.daily_pnl
                    rm.consecutive_losses = 0
                    if reset_daily:
                        rm.daily_pnl    = 0.0
                        rm.daily_trades = []
                detail = f"consec_losses {prev_cl}→0"
                if reset_daily:
                    detail += f" | daily_pnl ${prev_dp:+.2f}→$0.00 | daily_trades cleared"
                lines.append(f"  RiskManager: {detail}")
            except Exception as e:
                lines.append(f"  RiskManager error: {e}")

        if reset_daily:
            lines.append("\n<i>Full daily reset applied — counters treated as new session.</i>")
        else:
            lines.append("\n<i>Consecutive-loss lock cleared only. Daily PnL/trade count unchanged.</i>")
        return "\n".join(lines)

    # ================================================================
    # /setexchange
    # ================================================================

    def _cmd_setexchange(self, args: str) -> str:
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
            try:
                from core.types import Exchange
                Exchange.from_str(target)
                config.EXECUTION_EXCHANGE = Exchange.from_str(target).value
                return (f"✅ Execution exchange set to <b>{target.upper()}</b> "
                        f"(bot not running — takes effect on next start)")
            except ValueError:
                return f"❌ Unknown exchange: <code>{target}</code>"

        router = getattr(bot_instance, "execution_router", None)
        if router is None:
            return "❌ Execution router not available."

        strategy = getattr(bot_instance, "strategy", None)
        success, message = router.switch(target, strategy=strategy)

        if success and bot_instance.order_manager:
            try:
                bot_instance.order_manager.set_leverage(leverage=int(config.LEVERAGE))
            except Exception as e:
                message += f"\n⚠️ Leverage set failed: {e}"

        return message

    # ================================================================
    # /set
    # ================================================================

    def _cmd_set(self, args: str) -> str:
        import config as cfg

        if not args or len(args.split()) < 2:
            return (
                "Usage: /set &lt;key&gt; &lt;value&gt;\n\n"
                "<b>Adjustable:</b>\n"
                "  leverage          int   (e.g. 20)\n"
                "  cooldown          int   seconds\n"
                "  max_daily_trades  int\n"
                "  max_daily_loss    float %\n"
                "  max_consec_loss   int\n"
                "  min_rr            float\n"
                "  trail_enabled     bool  (true/false)\n"
                "  max_hold          int   seconds\n"
            )

        parts   = args.split(None, 1)
        key     = parts[0].lower().strip()
        val_str = parts[1].strip()

        allowed = {
            "leverage":         ("LEVERAGE",             int),
            "cooldown":         ("QUANT_COOLDOWN_SEC",   int),
            "max_daily_trades": ("MAX_DAILY_TRADES",     int),
            "max_daily_loss":   ("MAX_DAILY_LOSS_PCT",   float),
            "max_consec_loss":  ("MAX_CONSECUTIVE_LOSSES", int),
            "min_rr":           ("MIN_RISK_REWARD_RATIO", float),
            "trail_enabled":    ("QUANT_TRAIL_ENABLED",  bool),
            "max_hold":         ("QUANT_MAX_HOLD_SEC",   int),
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

        if key == "leverage":
            global bot_instance, bot_running
            if bot_running and bot_instance and bot_instance.strategy:
                pos = bot_instance.strategy.get_position()
                if pos:
                    return (
                        f"❌ Cannot change leverage while position is open.\n"
                        f"Close position first, then /set leverage {new_val}."
                    )
            setattr(cfg, attr_name, new_val)
            if bot_running and bot_instance:
                om = getattr(bot_instance, 'order_manager', None)
                if om:
                    try:
                        resp = om.set_leverage(leverage=int(new_val))
                        if isinstance(resp, dict) and resp.get("error"):
                            setattr(cfg, attr_name, old_val)
                            return f"❌ Exchange rejected: {resp['error']}\nConfig reverted to {old_val}x."
                        return (f"✅ <b>Leverage updated</b>: {old_val}x → <b>{new_val}x</b>\n"
                                f"Config and exchange both updated.")
                    except Exception as e:
                        setattr(cfg, attr_name, old_val)
                        return f"❌ Exchange API error: {e}\nConfig reverted to {old_val}x."
            return f"✅ <b>LEVERAGE</b>: {old_val} → <b>{new_val}</b>  (exchange not updated — bot not running)"

        setattr(cfg, attr_name, new_val)
        logger.info(f"CONFIG via Telegram: {attr_name} {old_val} → {new_val}")
        return f"✅ <b>{attr_name}</b>: {old_val} → <b>{new_val}</b>"

    # ================================================================
    # BOT THREAD
    # ================================================================

    def _run_bot_thread(self):
        global bot_instance, bot_running
        try:
            bot_running = True
            import sys, os as _os
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

    # ================================================================
    # MAIN LOOP
    # ================================================================

    def start(self):
        self.running = True
        self.clear_old_messages()
        self.set_my_commands()
        self.send_message(
            "⚡ <b>Liquidity-First Quant Bot Controller Ready</b>\n"
            "Execution: " + getattr(config, "EXECUTION_EXCHANGE", "?").upper() + "\n\n"
            + self._cmd_help())
        logger.info("Telegram controller started")

        while self.running:
            try:
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
            return f"{dt.strftime('%Y-%m-%d %H:%M:%S')},{int(record.msecs):03d}"

    _fmt = ISTFormatter(fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    _fh  = logging.FileHandler("telegram_controller.log", encoding="utf-8")
    _fh.setFormatter(_fmt)
    _sh  = logging.StreamHandler(
        stream=io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        if hasattr(sys.stdout, "buffer") else sys.stdout
    )
    _sh.setFormatter(_fmt)
    logging.basicConfig(level=getattr(config, "LOG_LEVEL", "INFO"), handlers=[_fh, _sh])

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

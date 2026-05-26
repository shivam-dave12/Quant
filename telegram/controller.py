"""Telegram controller for the Institutional Auction execution system.

Operator surfaces expose measurable auction evidence, competing structural archetypes,
bracket protection, exact-fill P&L and structural risk.
"""

import logging
import re
import time
import threading
import requests
import html as _html
from typing import Optional
from datetime import datetime, timezone, timedelta
import sys

import sys, os as _os; sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
import telegram.config as telegram_config
import config
from core.pnl import gross_pnl_usd
from telegram.notifier import _sanitize_html

logger = logging.getLogger(__name__)

_MOJIBAKE_SENTINELS = ("ð", "â", "Ã", "Â", "Î", "Ï")
_MOJIBAKE_RUN = re.compile(
    r"[\u0080-\u009f\u00a0-\u00ff\u0100-\u017f\u02c0-\u02ff"
    r"\u2010-\u201f\u2020-\u2026\u2030\u2039\u203a\u20ac\u2122]+"
)
_MOJIBAKE_DIRECT = {
    "🎯": "🎯", "🧭": "🧭", "📊": "📊", "💰": "💰",
    "🔒": "🔒", "🔄": "🔄", "🔱": "🔱", "🚨": "🚨",
    "💀": "💀", "💥": "💥", "✅": "✅", "❌": "❌",
    "❌": "❌", "⚠️": "⚠️", "⚠️": "⚠️", "⏱️": "⏱️",
    "⏱️": "⏱️", "⏱️": "⏱️", "⏱️": "⏱️", "⏳": "⏳",
    "≈": "≈", "±": "±", "×": "×", "σ": "σ",
    "⬜": "⬜", "░": "░", "█": "█",
}


def _repair_mojibake(text: str) -> str:
    if not any(s in text for s in _MOJIBAKE_SENTINELS):
        return text
    for bad, good in _MOJIBAKE_DIRECT.items():
        text = text.replace(bad, good)

    def _as_original_utf8_bytes(frag: str) -> bytes:
        out = bytearray()
        for ch in frag:
            try:
                out.extend(ch.encode("cp1252"))
            except UnicodeEncodeError:
                code = ord(ch)
                if code <= 0xFF:
                    out.append(code)
                else:
                    raise
        return bytes(out)

    def _fix(match: re.Match) -> str:
        frag = match.group(0)
        if not any(s in frag for s in _MOJIBAKE_SENTINELS):
            return frag
        try:
            repaired = _as_original_utf8_bytes(frag).decode("utf-8")
        except UnicodeError:
            return frag
        old_bad = sum(frag.count(s) for s in _MOJIBAKE_SENTINELS)
        new_bad = sum(repaired.count(s) for s in _MOJIBAKE_SENTINELS)
        return repaired if new_bad < old_bad else frag

    for _ in range(3):
        repaired = _MOJIBAKE_RUN.sub(_fix, text)
        if repaired == text or not any(s in repaired for s in _MOJIBAKE_SENTINELS):
            return repaired
        text = repaired
        for bad, good in _MOJIBAKE_DIRECT.items():
            text = text.replace(bad, good)
    return text


def _currency_for_strategy(strat) -> str:
    """Currency used by the selected instrument; never infer ICICI values as USD."""
    try:
        inst = getattr(strat, "_instrument", None)
        ex = str(getattr(getattr(inst, "primary_exchange", ""), "value", getattr(inst, "primary_exchange", ""))).lower()
        existing = str(getattr(getattr(strat, "_pos", None), "currency_symbol", "") or "")
        return existing or ("₹" if ex == "icici" else "$")
    except Exception:
        return "$"


def _currency_code_for_strategy(strat) -> str:
    return "INR" if _currency_for_strategy(strat) == "₹" else "USD"


def _fallback_unrealised_pnl_usd(strat, price: float, pos) -> float:
    try:
        calc = getattr(strat, "_unrealised_pnl_usd", None)
        if callable(calc):
            return float(calc(price, pos) or 0.0)
    except Exception:
        pass
    try:
        inst = getattr(strat, "_instrument", None)
        ex = str(getattr(getattr(inst, "primary_exchange", ""), "value", getattr(inst, "primary_exchange", ""))).lower()
        sym = str(getattr(inst, "execution_symbol", "") or getattr(config, "DELTA_SYMBOL", "BTCUSD")).upper()
        inverse = ex == "delta" and sym == "BTCUSD"
        return gross_pnl_usd(pos.side, pos.entry_price, price, pos.quantity, inverse=bool(inverse))
    except Exception:
        try:
            move = (price - pos.entry_price) if str(pos.side).lower() == "long" else (pos.entry_price - price)
            return move * float(pos.quantity or 0.0)
        except Exception:
            return 0.0


def _esc(s) -> str:
    """Escape <, >, & in dynamic strings before embedding in Telegram HTML."""
    if s is None:
        return ""
    return _html.escape(str(s), quote=False)


def _redact_telegram_token(value) -> str:
    """Never leak bot tokens through request exception strings/log URLs."""
    text = str(value)
    try:
        token = str(getattr(telegram_config, "TELEGRAM_BOT_TOKEN", "") or "")
        if token:
            text = text.replace(token, "<TELEGRAM_BOT_TOKEN_REDACTED>")
    except Exception:
        pass
    # Defensive fallback for any Telegram bot-token shaped URL fragment.
    return re.sub(r"/bot[^/\s?]+", "/bot<TELEGRAM_BOT_TOKEN_REDACTED>", text)


bot_instance = None
bot_thread   = None
bot_running  = False
bot_starting = False
bot_last_start_error = ""
bot_state_lock = threading.RLock()


class TelegramBotController:
    def __init__(self):
        self.bot_token      = telegram_config.TELEGRAM_BOT_TOKEN
        self.chat_id        = str(telegram_config.TELEGRAM_CHAT_ID)
        self.last_update_id = 0
        self.running        = False

        # SPAM-FIX 2026-04-26: rate-limit getUpdates HTTP error logs.
        # Telegram occasionally bursts 8–15 consecutive 429/502 responses on
        # getUpdates (seen in production log 06:40:52 — 06:40:59). Each was
        # WARN-level → forwarded to Telegram via TelegramLogHandler →
        # amplified the burst into a self-spam loop. We now log at INFO and
        # only emit a single WARN per unique (status_code) per 60s window so
        # genuine outages are still visible without flooding.
        self._last_getupdates_warn_ts: dict = {}   # status_code -> last warn ts
        self._getupdates_warn_throttle: float = 60.0
        self._last_getupdates_conn_warn_ts: float = 0.0
        self._getupdates_conn_failures: int = 0
        self._getupdates_http_failures: int = 0
        self._getupdates_backoff_base: float = float(getattr(config, "TELEGRAM_GETUPDATES_BACKOFF_BASE_SEC", 2.0))
        self._getupdates_backoff_max: float = float(getattr(config, "TELEGRAM_GETUPDATES_BACKOFF_MAX_SEC", 30.0))
        self._icici_otp_cv = threading.Condition()
        self._icici_pending_otp: str = ""
        self._icici_waiting_for_otp: bool = False
        self._icici_refresh_thread: Optional[threading.Thread] = None
        self._icici_refresh_result: str = ""
        self._icici_premarket_refresh_day: str = ""

        if not self.bot_token or not self.chat_id:
            raise ValueError("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be set")

        logger.info("TelegramBotController (liquidity-first) initialized")

    # ================================================================
    # MESSAGING
    # ================================================================

    def send_message(self, message: str, parse_mode: str = "HTML") -> bool:
        """Send message with auto-chunking (Telegram 4096 char limit)."""
        try:
            message = _repair_mojibake(str(message))
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
            logger.error("Send error: %s", _redact_telegram_token(e))
            try:
                return self._send_raw(message, parse_mode=None)
            except Exception:
                return False

    def _send_raw(self, text: str, parse_mode: Optional[str] = "HTML") -> bool:
        text = _repair_mojibake(str(text))
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
                # SPAM-FIX 2026-05-12: external container/network events can make
                # getUpdates fail immediately.  A tight loop here can flood logs,
                # burn CPU, and make PID-1 look unhealthy to the supervisor.  Back
                # off exponentially and only keep one warning per throttle window.
                self._getupdates_http_failures += 1
                delay = min(
                    self._getupdates_backoff_max,
                    max(0.5, self._getupdates_backoff_base) * (2 ** min(self._getupdates_http_failures - 1, 5)),
                )
                _now = time.time()
                _last = self._last_getupdates_warn_ts.get(resp.status_code, 0.0)
                if _now - _last >= self._getupdates_warn_throttle:
                    self._last_getupdates_warn_ts[resp.status_code] = _now
                    logger.warning(
                        "Telegram API HTTP %d — getUpdates skipped; retrying in %.1fs "
                        "(further occurrences within %.0fs stay DEBUG)",
                        resp.status_code, delay, self._getupdates_warn_throttle,
                    )
                else:
                    logger.debug("Telegram API HTTP %d — getUpdates skipped; retrying in %.1fs", resp.status_code, delay)
                time.sleep(delay)
                return []
            self._getupdates_conn_failures = 0
            self._getupdates_http_failures = 0
            data = resp.json()
            return data.get("result", []) if data.get("ok") else []
        except requests.exceptions.Timeout:
            return []
        except requests.exceptions.ConnectionError as e:
            # Institutional resilience: DNS/proxy/network drops must degrade the
            # command channel only.  Trading keeps running; Telegram polling backs
            # off, logs are token-redacted, and no tight retry loop is allowed.
            self._getupdates_conn_failures += 1
            delay = min(
                self._getupdates_backoff_max,
                max(0.5, self._getupdates_backoff_base) * (2 ** min(self._getupdates_conn_failures - 1, 5)),
            )
            _now = time.time()
            safe_err = _redact_telegram_token(e)
            if _now - self._last_getupdates_conn_warn_ts >= self._getupdates_warn_throttle:
                self._last_getupdates_conn_warn_ts = _now
                logger.warning("Telegram connection error; command channel retrying in %.1fs: %s", delay, safe_err)
            else:
                logger.debug("Telegram connection error; command channel retrying in %.1fs: %s", delay, safe_err)
            time.sleep(delay)
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
            url = f"https://api.telegram.org/bot{self.bot_token}/setMyCommands"
            commands = [
                {"command": "start", "description": "Start Institutional Auction scanner"},
                {"command": "stop", "description": "Stop scanner"},
                {"command": "status", "description": "Desk and position status"},
                {"command": "thinking", "description": "Current structural thesis"},
                {"command": "structures", "description": "4H/15m/5m structure state"},
                {"command": "pools", "description": "Live liquidity pools"},
                {"command": "position", "description": "Protected open positions"},
                {"command": "trades", "description": "Executed fills and P&L"},
                {"command": "balance", "description": "Venue account balances"},
                {"command": "config", "description": "Execution and risk policy"},
                {"command": "help", "description": "Commands"},
            ]
            requests.post(url, json={"commands": commands}, timeout=10)
            logger.info("Telegram commands registered")
        except Exception as exc:
            logger.warning("Failed to register Telegram commands: %s", exc)

    # ================================================================
    # COMMAND ROUTING
    # ================================================================

    def _normalize_command(self, text: str) -> tuple:
        t = (text or "").strip()
        bare_cmds = {
            "start", "stop", "status", "assets", "thinking", "pools",             "structures", "position", "trades", "stats", "config",
            "pause", "resume", "balance", "killswitch",
            "set", "help", "huntstatus", "setexchange", "resetrisk",
            "pnl", "market", "risk", "equity", "sl", "tp",
            "watchdog", "watchdog_status", "watchdog_heal",
            "watchdog_heal_on", "watchdog_heal_off",
            "watchdog_freeze", "watchdog_unfreeze",
            "icici", "icici_status", "icici_token", "icici_refresh",
            "icici_login", "icici_otp",
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
            if cmd and not str(cmd).startswith("/"):
                maybe_otp = re.sub(r"\D", "", str(cmd or ""))
                with self._icici_otp_cv:
                    waiting_for_icici_otp = bool(self._icici_waiting_for_otp)
                if waiting_for_icici_otp and len(maybe_otp) == 6:
                    return self._cmd_icici_otp(maybe_otp)
            if   cmd in ("/help", "/commands"): return self._cmd_help()
            elif cmd == "/start":               return self._cmd_start()
            elif cmd == "/stop":                return self._cmd_stop()
            elif cmd == "/status":              return self._cmd_status()
            elif cmd == "/assets":              return self._cmd_assets()
            elif cmd == "/thinking":            return self._cmd_thinking()
            elif cmd == "/pools":               return self._cmd_pools()
            elif cmd == "/structures":          return self._cmd_structures()
            elif cmd == "/position":            return self._cmd_position()
            elif cmd == "/trades":              return self._cmd_trades()
            elif cmd == "/stats":               return self._cmd_stats()
            elif cmd == "/balance":             return self._cmd_balance()
            elif cmd == "/pause":               return self._cmd_pause()
            elif cmd == "/resume":              return self._cmd_resume()
            elif cmd == "/config":              return self._cmd_config()
            elif cmd == "/killswitch":          return self._cmd_killswitch()
            elif cmd == "/resetrisk":           return self._cmd_resetrisk(args)
            elif cmd == "/set":                 return self._cmd_set(args)
            elif cmd == "/setexchange":         return self._cmd_setexchange(args)
            elif cmd == "/huntstatus":          return self._cmd_huntstatus()
            elif cmd == "/pnl":                 return self._cmd_pnl()
            elif cmd == "/market":              return self._cmd_market()
            elif cmd == "/risk":                return self._cmd_risk()
            elif cmd == "/equity":              return self._cmd_equity()
            elif cmd in ("/sl", "/tp"):         return self._cmd_sl_tp()
            elif cmd in ("/watchdog", "/watchdog_status"):
                return self._cmd_watchdog_status()
            elif cmd == "/watchdog_heal":
                return self._cmd_watchdog_heal(args)
            elif cmd == "/watchdog_heal_on":
                return self._cmd_watchdog_heal("on")
            elif cmd == "/watchdog_heal_off":
                return self._cmd_watchdog_heal("off")
            elif cmd == "/watchdog_freeze":
                return self._cmd_watchdog_freeze()
            elif cmd == "/watchdog_unfreeze":
                return self._cmd_watchdog_unfreeze()
            elif cmd in ("/icici", "/icici_status"):
                return self._cmd_icici_status()
            elif cmd in ("/icici_token", "/icici_refresh", "/icici_login"):
                return self._cmd_icici_token()
            elif cmd == "/icici_otp":
                return self._cmd_icici_otp(args)
            else:
                return f"Unknown command: {cmd}\n\n" + self._cmd_help()
        except Exception as e:
            logger.error(f"Command error [{cmd}]: {e}", exc_info=True)
            return f"❌ Error in {_esc(cmd)}: {_esc(e)}"

    # ================================================================
    # ICICI / Breeze token flow
    # ================================================================

    def _icici_otp_getter(self) -> str:
        timeout_sec = self._icici_otp_timeout_sec()
        with self._icici_otp_cv:
            self._icici_waiting_for_otp = True
            self._icici_pending_otp = ""
        self.send_message(
            "🔐 <b>ICICI Breeze OTP Required</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"⏳ Window: <code>{timeout_sec:.0f}s</code>\n"
            "📲 Reply with <code>/icici_otp 123456</code> or just the six-digit OTP.",
            parse_mode="HTML",
        )
        deadline = time.time() + timeout_sec
        with self._icici_otp_cv:
            while not self._icici_pending_otp and time.time() < deadline:
                self._icici_otp_cv.wait(timeout=max(0.5, min(5.0, deadline - time.time())))
            otp = self._icici_pending_otp.strip()
            self._icici_pending_otp = ""
            self._icici_waiting_for_otp = False
        if not otp:
            raise RuntimeError("ICICI OTP wait timed out")
        return otp

    def _cmd_icici_status(self) -> str:
        try:
            from exchanges.icici.breeze_auth import BreezeTokenService
            svc = BreezeTokenService()
            status = svc.session_status()
            configured = svc.has_minimum_config()
            refreshable = svc.can_refresh_without_operator()
            valid = bool(status.get("valid"))
            reason = str(status.get("reason") or "")
            age = status.get("age_sec")
            age_txt = "n/a" if age is None else f"{float(age):.0f}s"
            state_icon = "🟢" if valid else "🟠"
            return (
                f"{state_icon} <b>ICICI Breeze Desk</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                f"🧩 Configured: <code>{configured}</code>\n"
                f"🔑 Session valid: <code>{valid}</code>  <i>{_esc(reason)}</i>\n"
                f"⏱ Age: <code>{_esc(age_txt)}</code>\n"
                f"📅 Same trading day: <code>{bool(status.get('same_trading_day'))}</code>\n"
                f"🔄 Operator-free refresh: <code>{refreshable}</code>\n"
                f"🕘 Created: <code>{_esc(status.get('created_local') or '')}</code>\n"
                f"🕒 Now: <code>{_esc(status.get('now_local') or '')}</code>"
            )
        except Exception as e:
            return f"⚠️ <b>ICICI status error</b>\n<code>{_esc(e)}</code>"

    def _cmd_icici_token(self) -> str:
        if self._icici_refresh_thread is not None and self._icici_refresh_thread.is_alive():
            return (
                "🔄 <b>ICICI token refresh already running</b>\n"
                "If it is waiting for OTP, send <code>/icici_otp 123456</code>."
            )

        def _worker():
            try:
                from exchanges.icici.breeze_auth import BreezeTokenService
                from exchanges.icici.daily_token_guard import mark_valid
                from exchanges.icici.token_generator import assert_playwright_chromium_runtime_ready
                svc = BreezeTokenService()
                svc.require_configured(for_login=True)
                assert_playwright_chromium_runtime_ready(
                    auto_install=bool(getattr(config, "ICICI_PLAYWRIGHT_AUTO_INSTALL", True)),
                    headless=bool(getattr(config, "ICICI_TOKEN_GENERATOR_HEADLESS", True)),
                )
                session = svc.get_session(force_refresh=True, otp_getter=self._icici_otp_getter)
                status = svc.session_status(session)
                mark_valid(self._icici_auth_now().date().isoformat(), "telegram_token_worker")
                self._icici_refresh_result = (
                    "✅ <b>ICICI Breeze Session Refreshed</b>\n"
                    "━━━━━━━━━━━━━━━━━━━━\n"
                    f"🔑 Valid: <code>{bool(status.get('valid'))}</code>\n"
                    f"🧾 Reason: <code>{_esc(status.get('reason') or '')}</code>\n"
                    f"🕘 Created: <code>{_esc(status.get('created_local') or '')}</code>"
                )
            except Exception as e:
                try:
                    from exchanges.icici.daily_token_guard import mark_failed
                    mark_failed(self._icici_auth_now().date().isoformat(), "telegram_token_worker")
                except Exception:
                    pass
                self._icici_refresh_result = f"❌ <b>ICICI Breeze Refresh Failed</b>\n<code>{_esc(e)}</code>"
            try:
                self.send_message(self._icici_refresh_result, parse_mode="HTML")
            except Exception:
                pass

        self._icici_refresh_result = "running"
        self._icici_refresh_thread = threading.Thread(target=_worker, name="icici-token-refresh", daemon=True)
        self._icici_refresh_thread.start()
        return (
            "🚀 <b>ICICI Breeze token refresh started</b>\n"
            "🧪 Chromium preflight runs first. If ICICI asks for OTP, I will prompt here."
        )

    def _cmd_icici_otp(self, args: str) -> str:
        otp = re.sub(r"\D", "", str(args or ""))
        if len(otp) != 6:
            return "Send the OTP like: <code>/icici_otp 123456</code>"
        with self._icici_otp_cv:
            waiting = bool(self._icici_waiting_for_otp)
            self._icici_pending_otp = otp
            self._icici_waiting_for_otp = False
            self._icici_otp_cv.notify_all()
        return "ICICI OTP received. Continuing Breeze session refresh." if waiting else "ICICI OTP captured. Waiting token generator will consume it if login is pending."

    # /help
    # ================================================================

    def _cmd_help(self) -> str:
        return (
            "🏛️ <b>INSTITUTIONAL AUCTION EXECUTION SYSTEM</b>\n"
            "<code>Raid reversal | displacement continuation | liquidity-expansion retest → bracketed trade</code>\n\n"
            "/status — desks, open protection and P&L\n"
            "/thinking — current structural thesis\n"
            "/structures — 4H/15m/5m trigger geometry\n"
            "/pools — live liquidity map\n"
            "/position — protected open positions\n"
            "/trades — broker-reconciled trade ledger\n"
            "/balance — venue balances\n"
            "/config — execution and risk configuration\n"
            "/stop — stop scanner"
        )

    # ================================================================

    def _cmd_assets(self) -> str:
        global bot_instance
        if bot_instance is None:
            return "Bot is not running."
        fn = getattr(bot_instance, "format_assets_report", None)
        if callable(fn):
            return fn()
        return "Single-asset mode active. Use /status for the current BTCUSD scanner."

    # /thinking — institutional auction thesis console
    # ================================================================

    def _cmd_thinking(self) -> str:
        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return "Bot not running."
        try:
            portfolio_report = getattr(bot_instance, "format_portfolio_thinking_report", None)
            if callable(portfolio_report):
                return portfolio_report()
            strat = getattr(bot_instance, "strategy", None)
            if strat is None:
                return "Strategy not ready."
            eng = getattr(strat, "_entry_engine", None)
            info = eng.analysis_info if eng is not None else {}
            pos = getattr(strat, "_pos", None)
            def _fv(key, fmt=".4f"):
                try:
                    value = info.get(key, None)
                    return format(float(value), fmt) if value is not None else "N/A"
                except Exception:
                    return "N/A"
            lines = [
                "🏛️ <b>Institutional Auction Thesis</b>",
                "<code>Liquidity destination + protected structure + executable orderflow → protected execution</code>",
                "<i>N/A means the prerequisite structural stage has not evaluated.</i>",
                f"State: <b>{_esc(str(info.get('state', 'SCANNING')))}</b> | Block: {_esc(str(info.get('block_reason', 'WAIT')))}",
                f"4H: {_esc(str(info.get('context_4h', 'WAIT')))} score={_fv('context_4h_score','+.3f')} ATR={_fv('context_4h_atr')}",
                f"15m: {_esc(str(info.get('context_15m', 'WAIT')))} score={_fv('context_15m_score','+.3f')} ATR={_fv('context_15m_atr')}",
                f"Auction path: {_esc(str(info.get('archetype', 'DISCOVERY')))} | delivery={_esc(str(info.get('context_bias_path', 'AWAITING_DESTINATION')))} dir={_esc(str(info.get('context_direction', 'none')))} evidence={_fv('context_delivery_score','.2f')} strict={'Y' if info.get('context_aligned') else 'N'}",
                f"5m: ATR={_fv('entry_5m_atr')} trigger={_esc(str(info.get('trigger', 'WAITING_FOR_STRUCTURAL_OPPORTUNITY')))} minRR={_fv('min_structural_rr','.2f')}",
            ]
            for key, label in (("raid_quality", "Raid quality"), ("displacement_atr", "Displacement ATR"), ("delivery_score", "Delivery evidence score")):
                if key in info and info.get(key) is not None:
                    lines.append(f"{label}: {float(info[key]):.3f}")
            if info.get("probability_calibrated") and info.get("delivery_probability") is not None:
                lines.append(f"Calibrated delivery probability: {float(info['delivery_probability']):.3f}")
            else:
                lines.append("Calibrated delivery probability: N/A — structural risk and measured execution cost only")
            if info.get("raid_side") and not info.get("mss_broken"):
                lines.append(f"MSS={_fv('mss_level')} broken=N | FVG=N/A (requires MSS break) | SL/TP=N/A")
            elif info.get("mss_broken") and info.get("fvg_low") is None:
                lines.append("MSS broken=Y | FVG=N/A (awaiting valid displacement gap) | SL/TP=N/A")
            elif info.get("fvg_low") is not None:
                lines.append(f"FVG=[{_fv('fvg_low')},{_fv('fvg_high')}] SL={_fv('structural_stop')} TP={_fv('target_pool_price')}")
            if pos is not None and str(getattr(pos, 'phase', '')).upper().endswith('ACTIVE'):
                lines.append("\n🔒 <b>Position protected by venue orders</b>")
            else:
                lines.append("\nNo executable thesis currently approved.")
            return "\n".join(lines)
        except Exception as exc:
            logger.error("Thesis command error: %s", exc, exc_info=True)
            return f"❌ Thesis error: {_esc(str(exc))}"

    # ================================================================
    # /pools
    # ================================================================

    def _cmd_pools(self) -> str:
        """Show structural liquidity pools used by the institutional auction authority."""
        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return "Bot not running."
        try:
            strat = bot_instance.strategy
            dm = bot_instance.data_manager
            if not strat or not dm:
                return "Components not ready."
            analysis_unit = getattr(strat, "_analysis_unit", None)
            cur = analysis_unit() if callable(analysis_unit) else _currency_for_strategy(strat)
            analysis_price = getattr(dm, "get_analysis_price", None)
            price = float((analysis_price() if callable(analysis_price) else dm.get_last_price()) or 0.0)
            atr = float(strat._atr_5m.atr or 0.0)
            if not hasattr(strat, '_liq_map') or strat._liq_map is None:
                return "Structural liquidity map not ready."
            snap = strat._liq_map.get_snapshot(price, atr)
            lines = [f"<b>💧 INSTITUTIONAL AUCTION LIQUIDITY MAP @ {cur}{price:,.2f}</b>  5m ATR={cur}{atr:,.2f}"]
            if snap.primary_target:
                pt = snap.primary_target.pool
                lines.append(f"\n🎯 <b>Delivery target: {pt.side.value} @ {cur}{pt.price:,.2f}</b>")
            lines.append("\n<b>▲ External BSL</b>")
            for target in sorted([x for x in snap.bsl_pools if x.pool.price > price], key=lambda x: x.pool.price)[:6]:
                dist = (target.pool.price - price) / max(atr, 1e-12)
                lines.append(f"  {cur}{target.pool.price:,.2f}  {dist:.2f}ATR  touches={target.pool.touches}  structural={target.significance:.2f}")
            lines.append("\n<b>▼ External SSL</b>")
            for target in sorted([x for x in snap.ssl_pools if x.pool.price < price], key=lambda x: x.pool.price, reverse=True)[:6]:
                dist = (price - target.pool.price) / max(atr, 1e-12)
                lines.append(f"  {cur}{target.pool.price:,.2f}  {dist:.2f}ATR  touches={target.pool.touches}  structural={target.significance:.2f}")
            if snap.recent_sweeps:
                lines.append("\n🌊 <b>Fresh structural raids</b>")
                for raid in snap.recent_sweeps[-3:]:
                    lines.append(f"  {raid.pool.timeframe} {raid.pool.side.value} @ {cur}{raid.pool.price:,.2f}  q={raid.quality:.2f}")
            self.send_message("\n".join(lines))
            return None
        except Exception as e:
            logger.error(f"Pools error: {e}", exc_info=True)
            return f"Error: {e}"


    # ================================================================
    # /status
    # ================================================================

    def _cmd_status(self) -> str:
        global bot_instance, bot_running, bot_starting, bot_last_start_error
        if not bot_running or not bot_instance:
            if bot_starting:
                return (
                    "⏳ <b>Bot booting</b>\n"
                    "Pipeline: <code>auth → preflight → universe → data warmup → scanner</code>"
                )
            if bot_last_start_error:
                return f"❌ <b>Bot not running</b>\nLast startup error: <code>{_esc(bot_last_start_error)}</code>"
            return "Bot not running. Use /start"
        try:
            portfolio_report = getattr(bot_instance, "format_portfolio_status_report", None)
            if callable(portfolio_report):
                report = portfolio_report()
                self.send_message(report)
                return None
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
    # /structures — 4H/15m/5m execution geometry
    # ================================================================

    def _cmd_structures(self) -> str:
        return self._cmd_thinking()

    # ================================================================
    # /position
    # ================================================================

    def _cmd_position(self) -> str:
        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return "Bot not running."
        fn = getattr(bot_instance, "format_portfolio_position_report", None)
        if callable(fn):
            return fn()
        strat = getattr(bot_instance, "strategy", None)
        if strat is None:
            return "Strategy not ready."
        return strat.format_status_report()

    # ================================================================
    # /trades
    # ================================================================

    def _cmd_trades(self) -> str:
        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return "⚫ <b>BOT OFFLINE</b>\n<i>No active runtime is attached.</i>"
        fn = getattr(bot_instance, "format_portfolio_trades_report", None)
        if callable(fn):
            return fn()
        strat = bot_instance.strategy
        if not strat:
            return "⚠️ Trade ledger unavailable."
        cur = _currency_for_strategy(strat)
        history = list(getattr(strat, '_trade_history', []))
        lines = ["📋 <b>INSTITUTIONAL AUCTION EXECUTION LEDGER</b>", "<code>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━</code>"]
        for trade in reversed(history[-10:]):
            side = str(trade.get('side', '?')).upper()
            entry = float(trade.get('entry', 0.0) or 0.0)
            exit_px = float(trade.get('exit', 0.0) or 0.0)
            pnl = float(trade.get('pnl', 0.0) or 0.0)
            fees = float(trade.get('total_fees', 0.0) or 0.0)
            exact = bool(trade.get('exact_fees', False))
            reason = _esc(str(trade.get('reason', '?'))[:18].upper())
            icon = "✅" if bool(trade.get('is_win', False)) else "❌"
            lines.append(f"{icon} <b>{_esc(side)}</b>  {reason}")
            lines.append(f"<code>{cur}{entry:,.2f} → {cur}{exit_px:,.2f}   NET {cur}{pnl:+,.2f}   FEES {cur}{fees:,.4f} {'EXACT' if exact else 'PENDING'}</code>")
        if not history:
            lines.append("📭 <i>No broker-reconciled closed trades in this runtime.</i>")
        total_t = int(getattr(strat, '_total_trades', 0) or 0)
        wins = int(getattr(strat, '_winning_trades', 0) or 0)
        total_pnl = float(getattr(strat, '_total_pnl', 0.0) or 0.0)
        wr = wins / total_t * 100.0 if total_t else 0.0
        lines.extend(["<code>──────────────────────────────</code>", f"<code>NET {cur}{total_pnl:+,.2f}   TRADES {total_t}   WR {wr:.1f}%</code>"])
        return "\n".join(lines)


    # ================================================================
    # /stats
    # ================================================================

    def _cmd_stats(self) -> str:
        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return "Bot not running."
        fn = getattr(bot_instance, "format_portfolio_trades_report", None)
        if callable(fn):
            return fn()
        strat = getattr(bot_instance, "strategy", None)
        trades = list(getattr(strat, "_trade_history", []) or []) if strat is not None else []
        realised = [t for t in trades if not t.get("pnl_provisional", False)]
        pnl = sum(float(t.get("pnl", 0.0) or 0.0) for t in realised)
        wins = sum(1 for t in realised if float(t.get("pnl", 0.0) or 0.0) > 0)
        wr = (100.0 * wins / len(realised)) if realised else 0.0
        return (f"📊 <b>Broker-Reconciled Ledger</b>\nClosed exact fills: {len(realised)}\n"
                f"Win rate: {wr:.1f}%\nRealised P&amp;L: {pnl:+.2f}")

    # ================================================================

    # ================================================================
    # /watchdog
    # ================================================================

    def _get_watchdog(self):
        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return None, "Bot not running."
        wd = getattr(bot_instance, "watchdog", None)
        if wd is None:
            return None, "Watchdog not running."
        return wd, ""

    def _cmd_watchdog_status(self) -> str:
        wd, err = self._get_watchdog()
        if err:
            return err
        return wd.format_status_telegram()

    def _cmd_watchdog_heal(self, args: str) -> str:
        wd, err = self._get_watchdog()
        if err:
            return err
        mode = (args or "").strip().lower()
        if mode in ("on", "enable", "enabled", "true", "1"):
            wd.set_auto_heal_enabled(True)
            return "Watchdog auto-heal: ON"
        if mode in ("off", "disable", "disabled", "false", "0"):
            wd.set_auto_heal_enabled(False)
            return "Watchdog auto-heal: OFF"
        return f"Watchdog auto-heal is {'ON' if wd.auto_heal_enabled else 'OFF'}."

    def _cmd_watchdog_freeze(self) -> str:
        wd, err = self._get_watchdog()
        if err:
            return err
        wd.breaker.trip(reason="manual Telegram freeze")
        return "Watchdog circuit breaker: ENGAGED."

    def _cmd_watchdog_unfreeze(self) -> str:
        wd, err = self._get_watchdog()
        if err:
            return err
        if not wd.breaker.engaged:
            return "Watchdog circuit breaker already clear."
        wd.breaker.clear(operator="telegram")
        return "Watchdog circuit breaker: CLEARED."

    # ================================================================
    # /huntstatus
    # ================================================================

    def _cmd_huntstatus(self) -> str:
        return self._cmd_thinking()

    # ================================================================
    # /balance
    # ================================================================

    def _cmd_balance(self) -> str:
        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return "Bot not running."
        rm = bot_instance.risk_manager
        strat = getattr(bot_instance, 'strategy', None)
        if not rm:
            return "Risk manager not ready."
        try:
            bal = rm.get_available_balance()
            if not bal:
                return "Could not fetch balance."
            cur = _currency_for_strategy(strat)
            code = _currency_code_for_strategy(strat)
            avail = float(bal.get("available", 0.0) or 0.0)
            total = float(bal.get("total", avail) or avail)
            locked = max(0.0, total - avail)
            dpnl = float(getattr(rm, 'daily_pnl', 0.0) or 0.0)
            init_bal = float(getattr(rm, 'initial_balance', 0.0) or 0.0)
            dpct = dpnl / init_bal * 100.0 if init_bal > 1e-10 else 0.0
            return (f"💰 <b>Broker Funds ({code})</b>\n"
                    f"Available: <b>{cur}{avail:,.2f}</b>\n"
                    f"Blocked: {cur}{locked:,.2f}\n"
                    f"Total: {cur}{total:,.2f}\n"
                    f"Realised today: {cur}{dpnl:+,.2f} ({dpct:+.2f}%)")
        except Exception as e:
            return f"Balance error: {_esc(e)}"


    # ================================================================
    # /pause / /resume / 
    # ================================================================

    def _cmd_pause(self) -> str:
        global bot_instance
        if not bot_instance: return "Bot not running."
        bot_instance.trading_enabled      = False
        bot_instance.trading_pause_reason = "Paused via Telegram"
        return (
            "⏸️ <b>Trading PAUSED</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "• No new entries will be taken\n"
            "• Existing position (if any) is managed by fixed SL + TP ladder\n"
            "• Pool monitoring continues\n"
            "• Send <code>/resume</code> to re-enable entries"
        )

    def _cmd_resume(self) -> str:
        global bot_instance
        if not bot_instance: return "Bot not running."
        bot_instance.trading_enabled      = True
        bot_instance.trading_pause_reason = ""
        return (
            "▶️ <b>Trading RESUMED</b>\n"
            "Entries re-enabled. Bot will evaluate signals on next tick."
        )

    # ================================================================
    # /config
    # ================================================================

    def _cmd_config(self) -> str:
        import config as cfg
        return (
            "⚙️ <b>Institutional Auction Configuration</b>\n"
            "Entry authority: 4H/15m DOL bias / 5m execution\n"
            "Trigger: fresh external-liquidity raid → MSS/displacement → FVG rebalance\n"
            "Protection: venue-native SL/TP required before activation\n"
            "Accounting: exact broker fills; instrument-scoped currency/payoff model\n"
            f"Default exchange: {_esc(str(getattr(cfg, 'EXECUTION_EXCHANGE', '-')).upper())}"
        )

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
                    try:
                        from strategy.quant_strategy import PositionState
                    except ImportError:
                        from quant_strategy import PositionState
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
        if not bot_instance:
            return "Bot not running."
        strat = getattr(bot_instance, 'strategy', None)
        rm = getattr(bot_instance, 'risk_manager', None)
        if strat is None:
            return "❌ Strategy not initialised."
        cur = _currency_for_strategy(strat)
        try:
            try:
                from strategy.quant_strategy import PositionPhase
            except ImportError:
                from quant_strategy import PositionPhase
            with strat._lock:
                phase = strat._pos.phase
            if phase not in (PositionPhase.FLAT, PositionPhase.ENTERING):
                return f"❌ Cannot reset risk accounting while position is open.\nCurrent phase: {phase.name}"
        except Exception as e:
            logger.warning(f"resetrisk phase check error: {e}")
        reset_daily = "full" in args.lower()
        lines = ["🔄 <b>Risk Accounting Reset</b>"]
        gate = getattr(strat, '_risk_gate', None)
        if gate is not None:
            try:
                lines.append(f"  Circuit: {gate.force_reset(reset_consec=True, reset_daily=reset_daily)}")
            except Exception as e:
                lines.append(f"  Circuit error: {e}")
        if rm is not None:
            try:
                with rm._lock:
                    prev_cl, prev_dp = rm.consecutive_losses, rm.daily_pnl
                    rm.consecutive_losses = 0
                    if reset_daily:
                        rm.daily_pnl = 0.0
                        rm.daily_trades.clear() if hasattr(rm.daily_trades, 'clear') else None
                detail = f"loss streak {prev_cl}→0"
                if reset_daily:
                    detail += f" | realised {cur}{prev_dp:+.2f}→{cur}0.00 | daily ledger cleared"
                lines.append(f"  Account ledger: {detail}")
            except Exception as e:
                lines.append(f"  Account ledger error: {e}")
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

        if success:
            message += "\nLeverage remains unset while flat; it will be computed and applied only for an approved structural entry."
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
                "  risk              float (0.001-0.050, e.g. 0.025 = 2.5%)\n"
                "  cooldown          int   seconds\n"
                "  loss_lockout      int   seconds after any loss\n"
                "  consec_lockout    int   seconds after max consecutive losses\n"
                "  max_daily_trades  int\n"
                "  max_daily_loss    float %\n"
                "  max_consec_loss   int\n"
                "  min_rr            float\n"

                "  max_hold          int   seconds\n"
            )

        parts   = args.split(None, 1)
        key     = parts[0].lower().strip()
        val_str = parts[1].strip()

        allowed = {
            "leverage":         ("LEVERAGE",             int),
            # Stop-risk ceiling; margin policy remains desk-specific and visible
            # in ICT/Liquidity decision and funding logs.
            "risk":             ("RISK_PER_TRADE",       float),
            "cooldown":         ("MIN_TIME_BETWEEN_TRADES_SEC", int),
            "loss_lockout":     ("QUANT_LOCKOUT_AFTER_LOSS_SEC", int),
            "consec_lockout":   ("QUANT_LOSS_LOCKOUT_SEC", int),
            "max_daily_trades": ("MAX_DAILY_TRADES",     int),
            "max_daily_loss":   ("MAX_DAILY_LOSS_PCT",   float),
            "max_consec_loss":  ("MAX_CONSECUTIVE_LOSSES", int),
            "min_rr":           ("MIN_RISK_REWARD_RATIO", float),
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
            return (f"✅ <b>VENUE LEVERAGE CAP</b>: {old_val}x → <b>{new_val}x</b>\n"
                    "No exchange leverage is changed while flat. The next approved structural entry "
                    "computes and sets only the leverage required by its risk/margin geometry.")

        # ── Risk per trade validation ─────────────────────────────────────
        if key == "risk":
            if not (0.001 <= new_val <= 0.050):
                return (
                    f"Risk/trade must be 0.001-0.050 (0.1%-5.0%).\n"
                    f"You entered: {new_val} ({new_val*100:.2f}%)\n"
                    f"Example: /set risk 0.025  (= 2.5% of balance risked per trade at SL)"
                )
            if bot_running and bot_instance and bot_instance.strategy:
                pos = bot_instance.strategy.get_position()
                if pos:
                    return (
                        f"❌ Cannot change risk/trade while position is open.\n"
                        f"Close position first, then /set risk {new_val}."
                    )
            setattr(cfg, attr_name, new_val)
            logger.info(f"CONFIG via Telegram: {attr_name} {old_val} → {new_val}")
            return (
                f"✅ <b>RISK/TRADE</b>: {old_val*100:.2f}% → <b>{new_val*100:.2f}%</b>\n"
                f"Next trade will risk {new_val*100:.2f}% of available balance at SL."
            )

        if key == "cooldown":
            setattr(cfg, "TRADE_COOLDOWN_SECONDS", int(new_val))

        setattr(cfg, attr_name, new_val)
        logger.info(f"CONFIG via Telegram: {attr_name} {old_val} → {new_val}")

        # Bug #41 fix: several module-level constants in other modules are read
        # at import time and never re-read from config afterward.  When the
        # operator changes them via /set, the config module is updated but the
        # cached constant in the consuming module is stale.  Propagate the change
        # explicitly here.
        if key == "min_rr":
            try:
                import strategy.entry_engine as _ee
                if hasattr(_ee, '_MIN_RR_RATIO'):
                    _ee._MIN_RR_RATIO = float(new_val)
                    logger.info(f"CONFIG propagated to entry_engine._MIN_RR_RATIO = {new_val}")
            except ImportError:
                try:
                    import entry_engine as _ee
                    if hasattr(_ee, '_MIN_RR_RATIO'):
                        _ee._MIN_RR_RATIO = float(new_val)
                except ImportError:
                    pass

        return f"✅ <b>{attr_name}</b>: {old_val} → <b>{new_val}</b>"

    # ================================================================
    # /pnl — Quick PnL snapshot (most used command)
    # ================================================================

    def _cmd_pnl(self) -> str:
        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return "⚫ <b>BOT OFFLINE</b>\n<i>No active runtime is attached.</i>"
        fn = getattr(bot_instance, "format_portfolio_pnl_report", None)
        if callable(fn):
            return fn()
        strat, dm = bot_instance.strategy, bot_instance.data_manager
        if not strat or not dm:
            return "⚠️ P&amp;L report unavailable."
        cur = _currency_for_strategy(strat)
        price = float(dm.get_last_price() or 0.0)
        history = list(getattr(strat, '_trade_history', []))
        total_t = int(getattr(strat, '_total_trades', 0) or 0)
        wins = int(getattr(strat, '_winning_trades', 0) or 0)
        total_pnl = float(getattr(strat, '_total_pnl', 0.0) or 0.0)
        wr = wins / total_t * 100.0 if total_t else 0.0
        lines = [f"📈 <b>INSTITUTIONAL AUCTION P&amp;L</b>  <code>{cur}{price:,.2f}</code>"]
        if strat.get_position():
            p = strat._pos
            qty = float(getattr(p, 'quantity', 0.0) or 0.0)
            side = str(p.side or '?').upper()
            upnl = _fallback_unrealised_pnl_usd(strat, price, p)
            ladder = float(getattr(p, 'tp_ladder_realized_pnl', 0.0) or 0.0)
            init_dist = float(getattr(p, 'initial_sl_dist', 0.0) or abs(p.entry_price - p.sl_price) or 0.0)
            move = price - p.entry_price if side == 'LONG' else p.entry_price - price
            r_mult = move / init_dist if init_dist > 1e-10 else 0.0
            lines.extend([f"<code>{side} qty {qty:.6f}   entry {cur}{p.entry_price:,.2f}   mark {cur}{price:,.2f}</code>",
                          f"<code>protected SL {cur}{p.sl_price:,.2f}   TP {cur}{p.tp_price:,.2f}   R {r_mult:+.2f}</code>",
                          f"<code>live {cur}{upnl + ladder:+,.2f}   open {cur}{upnl:+,.2f}   partial-realised {cur}{ladder:+,.2f}</code>"])
        lines.append(f"<code>broker-reconciled realised {cur}{total_pnl:+,.2f}   trades {total_t}   WR {wr:.1f}%</code>")
        if history:
            lines.append("<b>Recent reconciled exits</b>")
            for t in reversed(history[-3:]):
                lines.append(f"<code>{str(t.get('side','?')).upper():<5} {cur}{float(t.get('pnl',0.0) or 0.0):+,.2f}   {_esc(str(t.get('reason','?'))[:18])}</code>")
        return "\n".join(lines)


    # ================================================================
    # /market — Quick market snapshot
    # ================================================================

    def _cmd_market(self) -> str:
        return self._cmd_thinking()

    # ================================================================
    # /risk — Risk gate status
    # ================================================================

    def _cmd_risk(self) -> str:
        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return "Bot not running."
        strat, rm = bot_instance.strategy, bot_instance.risk_manager
        if not strat or not rm:
            return "Components not ready."
        import config as cfg
        cur = _currency_for_strategy(strat)
        allowed, reason = rm.can_trade()
        dpnl = float(getattr(rm, 'daily_pnl', 0.0) or 0.0)
        init_bal = float(getattr(rm, 'initial_balance', 0.0) or 0.0)
        dpct = dpnl / init_bal * 100 if init_bal > 1e-10 else 0.0
        gap = float(getattr(cfg, 'MIN_TIME_BETWEEN_TRADES_SEC', 300.0) or 300.0)
        last_exit = float(getattr(strat, '_last_exit_time', 0.0) or 0.0)
        remaining = max(0.0, gap - (time.time() - last_exit)) if last_exit else 0.0
        return "\n".join(["🛡️ <b>EXECUTION RISK STATUS</b>",
                          f"Trade gate: {'OPEN' if allowed else _esc(reason)}",
                          f"Realised today: {cur}{dpnl:+,.2f} ({dpct:+.2f}% of {cur}{init_bal:,.2f})",
                          "Re-entry interval: {}".format("ready" if remaining == 0 else "{:.0f}s remaining".format(remaining)),
                          f"Position: {'ACTIVE' if strat.get_position() else 'FLAT'}"])


    # ================================================================
    # /equity — Balance + unrealised PnL
    # ================================================================

    def _cmd_equity(self) -> str:
        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return "Bot not running."
        fn = getattr(bot_instance, "format_portfolio_equity_report", None)
        if callable(fn):
            return fn()
        rm, strat, dm = bot_instance.risk_manager, bot_instance.strategy, bot_instance.data_manager
        if not rm or not strat or not dm:
            return "Components not ready."
        try:
            bal = rm.get_available_balance()
            if not bal:
                return "Could not fetch balance."
            cur = _currency_for_strategy(strat)
            total = float(bal.get('total', bal.get('available', 0.0)) or 0.0)
            avail = float(bal.get('available', 0.0) or 0.0)
            realised = float(getattr(strat, '_total_pnl', 0.0) or 0.0)
            lines = ["💰 <b>BROKER EQUITY</b>", f"Balance: {cur}{total:,.2f}   Available: {cur}{avail:,.2f}"]
            if strat.get_position():
                p = strat._pos
                mark = float(dm.get_last_price() or 0.0)
                open_pnl = _fallback_unrealised_pnl_usd(strat, mark, p)
                partial = float(getattr(p, 'tp_ladder_realized_pnl', 0.0) or 0.0)
                live = open_pnl + partial
                lines.append(f"Open P&amp;L: {cur}{open_pnl:+,.2f}   partial-realised: {cur}{partial:+,.2f}")
                lines.append(f"Live equity adjustment: {cur}{live:+,.2f}")
            lines.append(f"Broker-reconciled realised: {cur}{realised:+,.2f}")
            return "\n".join(lines)
        except Exception as e:
            return f"Equity error: {e}"


    # ================================================================
    # /sl or /tp — Quick SL/TP view
    # ================================================================

    def _cmd_sl_tp(self) -> str:
        global bot_instance, bot_running
        if not bot_running or not bot_instance:
            return "Bot not running."
        strat, dm = bot_instance.strategy, bot_instance.data_manager
        if not strat or not dm:
            return "Components not ready."
        if not strat.get_position():
            return "📭 No active position — no protected SL/TP."
        p = strat._pos
        cur = _currency_for_strategy(strat)
        price = float(dm.get_last_price() or 0.0)
        atr = float(strat._atr_5m.atr or 0.0)
        side = str(p.side or '?').upper()
        sl_dist, tp_dist = abs(price - p.sl_price), abs(p.tp_price - price)
        init_dist = float(getattr(p, 'initial_sl_dist', 0.0) or abs(p.entry_price - p.sl_price) or 0.0)
        move = price - p.entry_price if side == 'LONG' else p.entry_price - price
        r_mult = move / init_dist if init_dist > 1e-10 else 0.0
        return "\n".join([f"🛡️ <b>{side} PROTECTED STRUCTURE</b>",
                          f"Mark: {cur}{price:,.2f}   5m ATR: {cur}{atr:,.2f}",
                          f"Entry: {cur}{p.entry_price:,.2f}",
                          f"Structural SL: {cur}{p.sl_price:,.2f}   ({sl_dist / max(atr, 1e-12):.2f} ATR away)",
                          f"Liquidity TP: {cur}{p.tp_price:,.2f}   ({tp_dist / max(atr, 1e-12):.2f} ATR away)",
                          f"Live R: {r_mult:+.2f}R"])


    # ================================================================
    # ICICI Breeze auth preflight before data managers start
    # ================================================================

    def _icici_otp_timeout_sec(self) -> float:
        return max(45.0, float(getattr(config, "ICICI_OTP_WAIT_SEC", 180.0) or 180.0))

    def _clear_icici_pending_otp(self) -> None:
        with self._icici_otp_cv:
            self._icici_pending_otp = ""
            self._icici_waiting_for_otp = False

    def _icici_auth_tz(self) -> timezone:
        offset_min = int(getattr(config, "ICICI_SESSION_TIMEZONE_OFFSET_MIN", 330) or 330)
        return timezone(timedelta(minutes=offset_min))

    def _icici_auth_now(self) -> datetime:
        return datetime.now(self._icici_auth_tz())

    def _icici_local_now(self, now: Optional[datetime] = None) -> datetime:
        if now is None:
            return self._icici_auth_now()
        if now.tzinfo is None:
            return now.replace(tzinfo=self._icici_auth_tz())
        return now.astimezone(self._icici_auth_tz())

    def _parse_icici_premarket_time_min(self) -> int:
        raw = str(getattr(config, "ICICI_PREMARKET_TOKEN_REFRESH_TIME", "08:30") or "08:30").strip()
        match = re.fullmatch(r"([01]?\d|2[0-3]):([0-5]\d)", raw)
        if not match:
            logger.warning("Invalid ICICI_PREMARKET_TOKEN_REFRESH_TIME=%r; falling back to 08:30", raw)
            return 8 * 60 + 30
        return int(match.group(1)) * 60 + int(match.group(2))

    def _icici_same_day_session_ready(self) -> bool:
        try:
            from exchanges.icici.breeze_auth import BreezeTokenService
            svc = BreezeTokenService()
            status = svc.session_status()
            return bool(status.get("valid") and status.get("same_trading_day"))
        except Exception as exc:
            logger.info("ICICI premarket session status unavailable; daily refresh will run: %s", exc)
            return False

    def _maybe_run_icici_premarket_refresh(self, now: Optional[datetime] = None) -> None:
        """Once per trading day, ensure Breeze auth is ready before NIFTY opens."""
        if not bool(getattr(config, "ICICI_PREMARKET_TOKEN_REFRESH_ENABLED", True)):
            return
        if not self._should_auto_icici_token_on_start():
            return

        local_now = self._icici_local_now(now)
        day_key = local_now.date().isoformat()
        if getattr(self, "_icici_premarket_refresh_day", "") == day_key:
            return

        refresh_min = self._parse_icici_premarket_time_min()
        window_min = max(1.0, float(getattr(config, "ICICI_PREMARKET_TOKEN_REFRESH_WINDOW_MIN", 90.0) or 90.0))
        current_min = local_now.hour * 60 + local_now.minute + local_now.second / 60.0
        if current_min < refresh_min or current_min > refresh_min + window_min:
            return

        self._icici_premarket_refresh_day = day_key
        try:
            from exchanges.icici.daily_token_guard import claim_login_attempt, claim_notice, mark_valid
            if self._icici_same_day_session_ready():
                if not claim_notice(day_key, "telegram_premarket", "passed_notice"):
                    return
                mark_valid(day_key, "telegram_premarket")
                logger.info("ICICI premarket token check passed for %s; same-day Breeze session is valid.", day_key)
                self.send_message(
                    "ICICI premarket check passed.\n"
                    "Same-day Breeze session is valid; NIFTY options scanner may trade after market open."
                )
                return

            if not claim_login_attempt(day_key, "telegram_premarket"):
                logger.info("ICICI premarket token refresh already owned for %s; suppressing duplicate request.", day_key)
                return
            self.send_message(
                "ICICI daily premarket login required.\n"
                "No valid same-day Breeze session was found before NIFTY open. Starting token generation now."
            )
            self._cmd_icici_token()
        except Exception as exc:
            logger.error("ICICI premarket token refresh failed: %s", exc, exc_info=True)
            self.send_message(
                "ICICI premarket token refresh failed.\n"
                f"Reason: <code>{_esc(exc)}</code>\n"
                "Use <code>/icici_token</code> to retry before NIFTY trading."
            )

    def _format_icici_session_ok(self, session) -> str:
        try:
            status = session.masked()
            session_token = status.get("session_token", "***")
        except Exception:
            session_token = "***"
        try:
            from exchanges.icici.breeze_auth import BreezeTokenService
            svc = BreezeTokenService()
            st = svc.session_status(session)
            day = "same-day" if st.get("same_trading_day") else "stale-day"
        except Exception:
            day = "unknown"
        try:
            age = int(session.age_sec())
        except Exception:
            age = 0
        return (
            "✅ <b>ICICI Breeze Ready</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"🔑 Session age: <code>{age}s</code>\n"
            f"📅 Trading day: <code>{_esc(day)}</code>\n"
            f"🧷 SessionToken: <code>{_esc(session_token)}</code>\n"
            "🟢 NIFTY options scanner may start."
        )

    def _format_icici_preflight_ok(self, details: dict) -> str:
        browser_path = str((details or {}).get("browser_path") or "")
        runtime_path = str((details or {}).get("playwright_browsers_path") or "")
        return (
            "🧪 <b>ICICI Browser Preflight Passed</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"🌐 Chromium: <code>{_esc(browser_path[-72:] or 'ready')}</code>\n"
            f"📦 Runtime: <code>{_esc(runtime_path[-72:] or 'ready')}</code>\n"
            "🔐 Safe to request Breeze OTP."
        )

    def _should_auto_icici_token_on_start(self) -> bool:
        if not bool(getattr(config, "ICICI_AUTO_TOKEN_GENERATOR_ON_STARTUP", True)):
            return False
        return bool(
            getattr(config, "ICICI_OPTIONS_RUNTIME_ENABLED", False)
            or getattr(config, "ICICI_ENABLED", False)
            or getattr(config, "ICICI_BREEZE_PREFLIGHT_ON_STARTUP", True)
        )

    def _ensure_icici_session_before_bot_start(self) -> None:
        """Generate/validate Breeze token before ICICI DMs touch protected endpoints.

        This is intentionally in Telegram /start rather than the data manager:
        the controller owns the OTP conversation, while data managers must stay
        deterministic and never block mid-start waiting for operator input.
        """
        if not self._should_auto_icici_token_on_start():
            return
        try:
            from exchanges.icici.daily_token_guard import current_state, claim_login_attempt, mark_failed, mark_valid
            day_key = self._icici_auth_now().date().isoformat()
            if self._icici_refresh_thread is not None and self._icici_refresh_thread.is_alive():
                wait_sec = max(
                    self._icici_otp_timeout_sec() + 60.0,
                    float(getattr(config, "ICICI_STARTUP_TOKEN_WAIT_SEC", 300.0) or 300.0),
                )
                self.send_message(
                    "🔐 <b>ICICI Breeze token refresh already running</b>\n"
                    "Startup will wait for the active renewal instead of launching a second login."
                )
                self._icici_refresh_thread.join(timeout=wait_sec)
                if self._icici_refresh_thread.is_alive():
                    raise RuntimeError("ICICI token refresh did not complete before startup wait timeout")

            from exchanges.icici.breeze_auth import BreezeTokenService
            svc = BreezeTokenService()
            svc.require_configured(for_login=False)

            # Fast path: valid same-day cached session, current API_Session file,
            # or an explicit emergency SessionToken override. No .env session is
            # required for the normal path.
            try:
                session = svc.get_session(force_refresh=False)
                mark_valid(day_key, "telegram_start_cached")
                self.send_message(self._format_icici_session_ok(session))
                return
            except Exception as first_exc:
                logger.info("ICICI Breeze session not ready; launching Telegram OTP login before scanner start: %s", first_exc)

            svc.require_configured(for_login=True)
            if not claim_login_attempt(day_key, "telegram_start"):
                logger.info("ICICI startup login already owned for %s; suppressing duplicate startup token generation.", day_key)
                return
            self._clear_icici_pending_otp()
            from exchanges.icici.token_generator import assert_playwright_chromium_runtime_ready
            preflight = assert_playwright_chromium_runtime_ready(
                auto_install=bool(getattr(config, "ICICI_PLAYWRIGHT_AUTO_INSTALL", True)),
                headless=bool(getattr(config, "ICICI_TOKEN_GENERATOR_HEADLESS", True)),
            )
            self.send_message(self._format_icici_preflight_ok(preflight))
            self.send_message(
                "🔐 <b>ICICI Breeze Login Required</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                "📉 NIFTY chart analysis is paused until the same-day Breeze session is ready.\n"
                "📲 I am launching the token generator now; send only the OTP when requested."
            )
            session = svc.refresh(otp_getter=self._icici_otp_getter)
            mark_valid(day_key, "telegram_start")
            self.send_message(self._format_icici_session_ok(session))
        except Exception as exc:
            try:
                mark_failed(day_key, "telegram_start")
            except Exception:
                pass
            msg = f"ICICI Breeze startup token generation failed: {exc}"
            logger.error(msg, exc_info=True)
            if bool(getattr(config, "ICICI_AUTH_REQUIRED_FOR_DETAILS", True)):
                raise RuntimeError(msg) from exc
            self.send_message(
                "⚠️ <b>ICICI auth unavailable</b>; continuing without authenticated ICICI data.\n"
                f"Reason: <code>{_esc(exc)}</code>"
            )

    # ================================================================
    # BOT THREAD
    # ================================================================

    def _run_bot_thread(self):
        global bot_instance, bot_running, bot_starting, bot_last_start_error
        with bot_state_lock:
            bot_starting = True
            bot_running = False
            bot_last_start_error = ""
        try:
            import sys, os as _os
            _root = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
            if _root not in sys.path:
                sys.path.insert(0, _root)
            # Import main for the production logging bootstrap even when Telegram
            # starts the multi-asset runner directly.  The old path imported
            # QuantBot from main, so logging was initialised as a side effect;
            # without this, multi-asset Telegram starts used controller-only logs.
            import main as _main_logging_bootstrap  # noqa: F401
            import config as _cfg
            self._ensure_icici_session_before_bot_start()
            if bool(getattr(_cfg, "MULTI_ASSET_ENABLED", True)):
                from orchestration.multi_asset_bot import MultiAssetQuantBot
                logger.info("Telegram /start selected MultiAssetQuantBot (MULTI_ASSET_ENABLED=True)")
                bot_instance = MultiAssetQuantBot()
            else:
                from main import QuantBot
                logger.info("Telegram /start selected single-symbol QuantBot (MULTI_ASSET_ENABLED=False)")
                bot_instance = QuantBot()
            if not bot_instance.initialize():
                bot_last_start_error = "Bot init failed"
                self.send_message("❌ <b>Bot init failed</b>\nCheck logs before retrying.")
                return
            if not bot_instance.start():
                bot_last_start_error = "Bot start failed"
                self.send_message("❌ <b>Bot start failed</b>\nCheck logs before retrying.")
                return
            with bot_state_lock:
                bot_running = True
                bot_starting = False
            self.send_message(
                "✅ <b>Production Scanner Live</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                "🧠 Strategy desks initialized\n"
                "📡 Data managers ready\n"
                "🛡️ Risk/execution wiring online\n"
                "📊 Use /status or /assets for live telemetry."
            )
            bot_instance.run()
        except Exception as e:
            bot_last_start_error = str(e)
            logger.error(f"Bot crashed: {e}", exc_info=True)
            self.send_message(f"❌ <b>Bot crashed</b>\n<code>{_esc(e)}</code>")
        finally:
            with bot_state_lock:
                bot_running = False
                bot_starting = False
            logger.info("Bot thread finished")

    def _cmd_start(self) -> str:
        global bot_instance, bot_thread, bot_running, bot_starting, bot_last_start_error
        with bot_state_lock:
            thread_alive = bool(bot_thread and bot_thread.is_alive())
            if bot_running and thread_alive:
                return "🟢 <b>Bot already running.</b>"
            if bot_starting and thread_alive:
                return (
                    "⏳ <b>Startup already in progress</b>\n"
                    "Current boot path: <code>ICICI auth → browser preflight → universe → data warmup → scanner</code>"
                )
            bot_starting = True
            bot_last_start_error = ""
        logger.info("Starting bot from Telegram...")
        # Keep the trading runtime non-daemon so the process cannot silently
        # drop live risk if the controller loop is interrupted. /stop is the
        # only authorised path that calls bot_instance.stop().
        bot_thread = threading.Thread(target=self._run_bot_thread, daemon=False)
        bot_thread.start()
        time.sleep(2.0)
        if bot_thread.is_alive():
            return (
                "🚀 <b>Boot sequence started</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                "🔐 ICICI Breeze/session readiness\n"
                "🧪 Chromium runtime preflight\n"
                "📡 Live universe + data warmup\n"
                "🛡️ Risk/execution wiring\n"
                "Use /status in 30s."
            )
        err = _esc(bot_last_start_error or "Check logs.")
        return f"❌ <b>Start failed</b>\n<code>{err}</code>"

    def _cmd_stop(self) -> str:
        global bot_instance, bot_running, bot_starting
        if not bot_running or not bot_instance:
            if bot_starting:
                return "⏳ Startup is still in progress; wait for the ready/failure alert before stopping."
            return "Bot not running."
        logger.info("Stopping bot from Telegram...")
        with bot_state_lock:
            bot_running = False
        if bot_instance:
            bot_instance.stop()
        return "🛑 <b>Bot stopped.</b>"

    # ================================================================
    # MAIN LOOP
    # ================================================================

    def start(self):
        self.running = True
        self.clear_old_messages()
        self.set_my_commands()
        self.send_message(
            "⚡ <b>Institutional Quant Controller Ready</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "🏦 Execution: <code>" + getattr(config, "EXECUTION_EXCHANGE", "?").upper() + "</code>\n"
            "🔐 ICICI startup auth preflight: <code>" + str(bool(getattr(config, "ICICI_BREEZE_PREFLIGHT_ON_STARTUP", True))) + "</code>\n"
            "🧪 Playwright runtime preflight: <code>enabled</code>\n\n"
            + self._cmd_help())
        logger.info("Telegram controller started")
        self._maybe_run_icici_premarket_refresh()

        while self.running:
            try:
                self._maybe_run_icici_premarket_refresh()
                updates = self.get_updates(timeout=10)
                for upd in updates:
                    self.last_update_id = upd.get("update_id", self.last_update_id)
                    msg     = upd.get("message") or {}
                    chat_id = str((msg.get("chat") or {}).get("id", ""))
                    text    = (msg.get("text") or "").strip()
                    if chat_id != self.chat_id or not text:
                        continue

                    # Bug #38 fix: authenticate by user_id, not just chat_id.
                    # In a group chat, any member can send commands if only chat_id
                    # is checked.  TELEGRAM_ALLOWED_USER_IDS is a comma-separated
                    # list of integer user IDs in config (e.g. "123456789,987654321").
                    # When the config key is absent or empty, the check is skipped
                    # (backward-compatible for private-chat bots where chat_id == user_id).
                    _raw_allowed = str(getattr(config, "TELEGRAM_ALLOWED_USER_IDS", "") or "")
                    if _raw_allowed.strip():
                        try:
                            _allowed_ids = {
                                int(x.strip()) for x in _raw_allowed.split(",")
                                if x.strip().lstrip("-").isdigit()
                            }
                        except Exception:
                            _allowed_ids = set()
                        _sender_id = int((msg.get("from") or {}).get("id", 0))
                        if _allowed_ids and _sender_id not in _allowed_ids:
                            logger.warning(
                                "Telegram command REJECTED from user_id=%d "
                                "(not in TELEGRAM_ALLOWED_USER_IDS): %s",
                                _sender_id, text[:80]
                            )
                            continue

                    logger.info(f"Received: {text}")
                    response = self.handle_command(text)
                    if response:
                        self.send_message(response)
            except KeyboardInterrupt:
                logger.warning(
                    "KeyboardInterrupt ignored by Telegram-only shutdown guard; "
                    "use /stop from Telegram to stop the bot."
                )
                continue
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

        def format(self, record):
            return _repair_mojibake(super().format(record))

    _fmt = ISTFormatter(fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    _fh  = logging.FileHandler("telegram_controller.log", encoding="utf-8")
    _fh.setFormatter(_fmt)
    _sh  = logging.StreamHandler(
        stream=io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        if hasattr(sys.stdout, "buffer") else sys.stdout
    )
    _sh.setFormatter(_fmt)
    logging.basicConfig(level=getattr(config, "LOG_LEVEL", "INFO"), handlers=[_fh, _sh], force=True)

    if threading.current_thread() is threading.main_thread():
        from runtime_shutdown_guard import install_telegram_only_shutdown_guard

        install_telegram_only_shutdown_guard(logger, "telegram-controller")

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

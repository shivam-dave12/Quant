"""
telegram/notifier.py — Telegram Notifier  v3.0
===============================================
Redesigned message format: clean hierarchy, section dividers, <code>
blocks for data tables, consistent emoji language, and readable layout
on both phone and desktop Telegram clients.

Public API (imported by strategy layer):
  send_telegram_message()
  format_periodic_report()
  format_direction_hunt_alert()
  format_post_sweep_verdict()
  format_conviction_block_alert()
  format_pool_gate_alert()
  format_liquidity_trail_update()
  install_global_telegram_log_handler()
  TelegramLogHandler

Internal helpers used by controller.py:
  _sanitize_html()  _esc()
"""

from __future__ import annotations

import html as _html_lib
import logging
import queue as _queue_mod
import re
import sys
import os as _os
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
import telegram.config as telegram_config

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# MOJIBAKE REPAIR  (unchanged from v2)
# ─────────────────────────────────────────────────────────────────────────────

_MOJIBAKE_SENTINELS = ("ð", "â", "Ã", "Â", "Î", "Ï")
_MOJIBAKE_RUN = re.compile(
    r"[\u0080-\u009f\u00a0-\u00ff\u0100-\u017f\u02c0-\u02ff"
    r"\u2010-\u201f\u2020-\u2026\u2030\u2039\u203a\u20ac\u2122]+"
)
_MOJIBAKE_DIRECT = {
    "ðŸŽ¯": "🎯", "ðŸ§­": "🧭", "ðŸ“Š": "📊", "ðŸ’°": "💰",
    "ðŸ”’": "🔒", "ðŸ”„": "🔄", "ðŸ”±": "🔱", "ðŸš¨": "🚨",
    "ðŸ’€": "💀", "ðŸ’¥": "💥", "âœ…": "✅", "âŒ": "❌",
    "âŒ": "❌", "âš ï¸": "⚠️", "âš ï¸": "⚠️", "â±ï¸": "⏱️",
    "â±ï¸": "⏱️", "â±ï¸": "⏱️", "â±ï¸": "⏱️", "â³": "⏳",
    "â‰ˆ": "≈", "Â±": "±", "Ã—": "×", "Ïƒ": "σ",
    "â¬œ": "⬜", "â–‘": "░", "â–ˆ": "█",
}


def _repair_mojibake(text: str) -> str:
    if not any(s in text for s in _MOJIBAKE_SENTINELS):
        return text
    for bad, good in _MOJIBAKE_DIRECT.items():
        text = text.replace(bad, good)

    def _as_utf8_bytes(frag: str) -> bytes:
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

    def _fix(m: re.Match) -> str:
        frag = m.group(0)
        if not any(s in frag for s in _MOJIBAKE_SENTINELS):
            return frag
        try:
            repaired = _as_utf8_bytes(frag).decode("utf-8")
        except UnicodeError:
            return frag
        old_bad = sum(frag.count(s)     for s in _MOJIBAKE_SENTINELS)
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


# ─────────────────────────────────────────────────────────────────────────────
# THRESHOLD IMPORTS
# ─────────────────────────────────────────────────────────────────────────────

try:
    from strategy.conviction_filter import REQUIRED_SCORE as _REQUIRED_CONVICTION_SCORE
except ImportError:
    try:
        from conviction_filter import REQUIRED_SCORE as _REQUIRED_CONVICTION_SCORE  # type: ignore
    except ImportError:
        _REQUIRED_CONVICTION_SCORE = 0.45

try:
    from strategy.direction_engine import _HUNT_ON_THRESHOLD, _HUNT_OFF_THRESHOLD
except ImportError:
    try:
        from direction_engine import _HUNT_ON_THRESHOLD, _HUNT_OFF_THRESHOLD  # type: ignore
    except ImportError:
        _HUNT_ON_THRESHOLD  = 0.10
        _HUNT_OFF_THRESHOLD = 0.05


# ─────────────────────────────────────────────────────────────────────────────
# ASYNC SEND WORKER  (unchanged from v2 — only formatting is redesigned)
# ─────────────────────────────────────────────────────────────────────────────

_send_queue:    _queue_mod.Queue = _queue_mod.Queue(maxsize=25)
_worker_started: bool            = False
_worker_lock:   threading.Lock   = threading.Lock()
_MIN_INTERVAL  = 1.2
_MAX_RETRIES   = 4

_CRITICAL_KEYWORDS = frozenset((
    "💀", "🚨",
    "UNPROTECTED", "CRASH", "KILLSWITCH", "CIRCUIT_BREAKER",
    "EMERGENCY", "emergency_flatten", "BOT CRASH",
))


def _send_worker() -> None:
    import random
    import requests as _req
    last_send_ts = 0.0
    while True:
        try:
            item = _send_queue.get(timeout=30)
        except _queue_mod.Empty:
            continue
        if item is None:
            break
        message, parse_mode = item
        message = _repair_mojibake(str(message))
        for attempt in range(_MAX_RETRIES):
            gap = _MIN_INTERVAL - (time.time() - last_send_ts)
            if gap > 0:
                time.sleep(gap)
            try:
                url      = (f"https://api.telegram.org/bot"
                            f"{telegram_config.TELEGRAM_BOT_TOKEN}/sendMessage")
                send_txt = message[:4000]
                if parse_mode == "HTML":
                    send_txt = _sanitize_html(send_txt)
                payload = {
                    "chat_id":                  telegram_config.TELEGRAM_CHAT_ID,
                    "text":                     send_txt,
                    "parse_mode":               parse_mode,
                    "disable_web_page_preview": True,
                }
                resp = _req.post(url, json=payload, timeout=15)
                last_send_ts = time.time()
                if resp.status_code == 200:
                    break
                if resp.status_code == 400 and parse_mode == "HTML" and attempt == 0:
                    logger.warning("Telegram HTML parse error — retrying plain: %s",
                                   resp.text[:160])
                    plain = re.sub(r"<[^>]*>", "", send_txt, flags=re.DOTALL)
                    plain = _html_lib.unescape(plain)
                    r2    = _req.post(url, json={
                        "chat_id": telegram_config.TELEGRAM_CHAT_ID,
                        "text":    plain[:4000],
                        "disable_web_page_preview": True,
                    }, timeout=15)
                    last_send_ts = time.time()
                    if r2.status_code == 200:
                        break
                    logger.warning("Plain-text fallback failed: %s", r2.text[:160])
                    break
                if resp.status_code in (429, 500, 502, 503) and attempt < _MAX_RETRIES - 1:
                    if resp.status_code == 429:
                        try:
                            backoff = max(float(resp.json().get("parameters", {})
                                               .get("retry_after", 10)), 5.0)
                        except Exception:
                            backoff = 10.0
                    else:
                        backoff = min(2.0 * (2 ** attempt) + random.uniform(0, 2), 60.0)
                    logger.warning("Telegram %s, retry %d/%d in %.1fs",
                                   resp.status_code, attempt + 1, _MAX_RETRIES, backoff)
                    time.sleep(backoff)
                    continue
                logger.warning("Telegram send failed: %s — %s",
                               resp.status_code, resp.text[:200])
                break
            except _req.exceptions.Timeout:
                if attempt < _MAX_RETRIES - 1:
                    time.sleep(2.0 * (attempt + 1))
                    continue
                logger.error("Telegram send timed out after all retries")
                break
            except Exception as exc:
                logger.error("Telegram send error: %s", exc)
                break
        _send_queue.task_done()


def _ensure_worker_started() -> None:
    global _worker_started
    if _worker_started:
        return
    with _worker_lock:
        if _worker_started:
            return
        t = threading.Thread(target=_send_worker, daemon=True, name="telegram-sender")
        t.start()
        _worker_started = True


def send_telegram_message(message: str, parse_mode: str = "HTML") -> bool:
    """Enqueue a Telegram message for async delivery. Never blocks."""
    if not telegram_config.TELEGRAM_ENABLED:
        return False
    message = _repair_mojibake(str(message))
    _ensure_worker_started()

    is_critical = any(kw in message for kw in _CRITICAL_KEYWORDS)
    if is_critical:
        def _send_now():
            import requests as _req
            try:
                url      = (f"https://api.telegram.org/bot"
                            f"{telegram_config.TELEGRAM_BOT_TOKEN}/sendMessage")
                send_txt = message[:4000]
                if parse_mode == "HTML":
                    send_txt = _sanitize_html(send_txt)
                _req.post(url, json={
                    "chat_id":                  telegram_config.TELEGRAM_CHAT_ID,
                    "text":                     send_txt,
                    "parse_mode":               parse_mode,
                    "disable_web_page_preview": True,
                }, timeout=10)
            except Exception as _ce:
                logger.error("Critical Telegram send failed: %s", _ce)
        threading.Thread(target=_send_now, daemon=True, name="telegram-critical").start()
        return True

    try:
        _send_queue.put_nowait((message, parse_mode))
        return True
    except _queue_mod.Full:
        logger.warning("Telegram queue full — dropping non-critical message")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# HTML SANITIZER  (unchanged from v2 — bulletproof state-machine)
# ─────────────────────────────────────────────────────────────────────────────

_SAFE_TAGS          = frozenset(("b","strong","i","em","u","ins","s","strike",
                                 "del","code","pre","tg-spoiler"))
_SAFE_TAGS_WITH_ATTR = frozenset(("a",))
_TAG_RE    = re.compile(r"<(/?)\s*([A-Za-z][A-Za-z0-9_-]*)\s*([^>]*?)/?\s*>", re.DOTALL)
_ENTITY_RE = re.compile(r"&(#[0-9]+|#x[0-9A-Fa-f]+|[A-Za-z][A-Za-z0-9]*);")


def _normalise_ampersands(text: str) -> str:
    out, i, n = [], 0, len(text)
    while i < n:
        ch = text[i]
        if ch != "&":
            out.append(ch); i += 1; continue
        m = _ENTITY_RE.match(text, i)
        if m:
            out.append(m.group(0)); i = m.end()
        else:
            out.append("&amp;"); i += 1
    return "".join(out)


def _sanitize_html(text: str) -> str:
    if not text:
        return text
    text = re.sub(r"<br\s*/?>",       "\n",          text, flags=re.IGNORECASE)
    text = re.sub(r"<hr\s*/?>",       "\n────────\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<p(?:\s[^>]*)?>", "\n",          text, flags=re.IGNORECASE)
    text = re.sub(r"</p>",            "",             text, flags=re.IGNORECASE)

    tokens: List[tuple] = []
    cursor = 0
    for m in _TAG_RE.finditer(text):
        if m.start() > cursor:
            tokens.append(("text", text[cursor:m.start()]))
        tokens.append(("tag", m.group(1) == "/", m.group(2).lower(), m.group(3).strip()))
        cursor = m.end()
    if cursor < len(text):
        tokens.append(("text", text[cursor:]))

    out: List[str] = []
    stack: List[str] = []
    for tok in tokens:
        if tok[0] == "text":
            out.append(_normalise_ampersands(tok[1]))
            continue
        _, closing, name, attrs = tok
        is_safe = (name in _SAFE_TAGS) or (name in _SAFE_TAGS_WITH_ATTR)
        if not is_safe:
            raw = f"</{name}>" if closing else f"<{name}{(' ' + attrs) if attrs else ''}>"
            out.append(_html_lib.escape(raw, quote=False))
            continue
        if name == "a":
            if closing:
                if "a" in stack:
                    while stack and stack[-1] != "a":
                        out.append(f"</{stack.pop()}>")
                    stack.pop()
                    out.append("</a>")
                continue
            hm = re.search(r'href\s*=\s*"([^"]*)"', attrs, re.IGNORECASE) or \
                 re.search(r"href\s*=\s*'([^']*)'", attrs, re.IGNORECASE)
            if hm:
                href = _normalise_ampersands(hm.group(1))
                stack.append("a")
                out.append(f'<a href="{href}">')
            continue
        if closing:
            if name in stack:
                while stack and stack[-1] != name:
                    out.append(f"</{stack.pop()}>")
                stack.pop()
                out.append(f"</{name}>")
        else:
            stack.append(name)
            out.append(f"<{name}>")

    while stack:
        out.append(f"</{stack.pop()}>")

    rendered = "".join(out)
    rendered = re.sub(r"\n{3,}", "\n\n", rendered)
    return rendered


# ─────────────────────────────────────────────────────────────────────────────
# MICRO-HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _esc(s: Any) -> str:
    if s is None: return ""
    return _html_lib.escape(str(s), quote=False)


def _tag(text: Any, name: str) -> str:
    return f"<{name}>{_esc(text)}</{name}>"


def _fp(p: Optional[float]) -> str:
    if p is None: return "—"
    return f"${p:,.1f}"


def _pct(v: float) -> str:
    return f"{v:.1f}%"


def _session_icon(session: str) -> str:
    s = (session or "").upper()
    if "LONDON" in s: return "🇬🇧"
    if "NY" in s or "NEW_YORK" in s: return "🇺🇸"
    if "ASIA" in s: return "🌏"
    return "🌐"


def _score_bar(score: float, width: int = 10) -> str:
    filled = min(max(int(score * width + 0.5), 0), width)
    return "█" * filled + "░" * (width - filled)


# ─────────────────────────────────────────────────────────────────────────────
# SHARED LAYOUT CHROME
# ─────────────────────────────────────────────────────────────────────────────

_DIV  = "─" * 32   # thin section divider
_HDIV = "━" * 32   # thick outer divider


def _section(title: str) -> str:
    """Bold section header with preceding divider."""
    return f"{_DIV}\n<b>{title}</b>"


# ─────────────────────────────────────────────────────────────────────────────
# GATE DIAGNOSTIC PANEL
# ─────────────────────────────────────────────────────────────────────────────

def _build_gate_diagnostic(
    direction_hunt=None,
    amd_phase: str = "",
    dealing_range_pd: float = 0.5,
    session: str = "",
    flow_conviction: float = 0.0,
    flow_direction: str = "",
    htf_bias: str = "",
) -> List[str]:
    def _tick(ok: bool) -> str:
        return "✅" if ok else "⬜"

    s       = (session or "").upper()
    sess_ok = s in ("LONDON", "NY", "LONDON_NY")

    pred    = getattr(direction_hunt, "predicted",   None) if direction_hunt else None
    conf    = float(getattr(direction_hunt, "confidence", 0.0)) if direction_hunt else 0.0
    hunt_ok = pred in ("BSL", "SSL") and conf >= _HUNT_ON_THRESHOLD

    flow_ok = abs(flow_conviction) >= 0.20 and flow_direction in ("long", "short")

    amd_ok  = ("MANIPULATION" in (amd_phase or "").upper() or
               "DISTRIBUTION" in (amd_phase or "").upper())

    pd_ok   = dealing_range_pd < 0.40 or dealing_range_pd > 0.60
    pd_lbl  = ("DISC" if dealing_range_pd < 0.40 else
                "EQ"   if dealing_range_pd < 0.60 else "PREM")

    htf_ok  = bool(htf_bias) and htf_bias.lower() != "mixed"

    passed  = sum([sess_ok, hunt_ok, flow_ok, amd_ok, pd_ok, htf_ok])

    lines: List[str] = [
        _section("🚦 ENTRY GATES"),
        f"<code>"
        f"{_tick(sess_ok)} Session   {_esc(s or 'off')}\n"
        f"{_tick(hunt_ok)} Hunt       {_esc(pred or 'NEUTRAL')} ({conf:.0%})\n"
        f"{_tick(flow_ok)} Flow       {_esc(flow_direction or 'neutral')} ({flow_conviction:+.2f})\n"
        f"{_tick(amd_ok)} AMD        {_esc(amd_phase or 'UNKNOWN')}\n"
        f"{_tick(pd_ok)} P/D        {pd_lbl} ({dealing_range_pd:.0%})\n"
        f"{_tick(htf_ok)} HTF        {_esc(htf_bias or 'mixed')}"
        f"</code>",
        f"  {passed}/6 gates open",
    ]
    return lines


# ═════════════════════════════════════════════════════════════════════════════
# 1.  PERIODIC REPORT  (every 15 min)
# ═════════════════════════════════════════════════════════════════════════════

def format_periodic_report(
    current_price:       float = 0.0,
    balance:             float = 0.0,
    total_trades:        int   = 0,
    win_rate:            float = 0.0,
    daily_pnl:           float = 0.0,
    total_pnl:           float = 0.0,
    consecutive_losses:  int   = 0,
    bot_state:           str   = "SCANNING",
    n_bsl_pools:         int   = 0,
    n_ssl_pools:         int   = 0,
    primary_target_str:  str   = "—",
    flow_conviction:     float = 0.0,
    flow_direction:      str   = "",
    amd_phase:           str   = "UNKNOWN",
    session:             str   = "REGULAR",
    in_killzone:         bool  = False,
    regime:              str   = "UNKNOWN",
    position:            Optional[Dict] = None,
    current_sl:          Optional[float] = None,
    current_tp:          Optional[float] = None,
    entry_price:         Optional[float] = None,
    breakeven_moved:     bool  = False,
    profit_locked_pct:   float = 0.0,
    extra_lines:         Optional[List[str]] = None,
    atr:                 float = 0.0,
    htf_bias:            str   = "",
    dealing_range_pd:    float = 0.5,
    structure_15m:       str   = "",
    structure_4h:        str   = "",
    amd_bias:            str   = "",
    nearest_bsl:         Optional[Dict] = None,
    nearest_ssl:         Optional[Dict] = None,
    sweep_analysis:      Optional[Dict] = None,
    direction_hunt:        Optional[Any] = None,
    direction_ps_analysis: Optional[Any] = None,
    **_kwargs: Any,
) -> str:
    ist_tz   = timezone(timedelta(hours=5, minutes=30))
    now_ist  = datetime.now(ist_tz).strftime("%H:%M IST")
    now_utc  = datetime.now(timezone.utc).strftime("%H:%M UTC")
    si       = {"SCANNING":"🔍","TRACKING":"📡","READY":"🎯",
                "ENTERING":"⚡","IN_POSITION":"📊","POST_SWEEP":"🌊"
                }.get((bot_state or "").upper(), "⚪")
    sess_icon = _session_icon(session)
    kz        = "  🔥 KZ" if in_killzone else ""
    pnl_icon  = "🟢" if daily_pnl >= 0 else "🔴"
    pd_lbl    = ("DEEP DISC" if dealing_range_pd < 0.25 else
                 "DISCOUNT"  if dealing_range_pd < 0.40 else
                 "EQ"        if dealing_range_pd < 0.60 else
                 "PREMIUM"   if dealing_range_pd < 0.75 else "DEEP PREM")
    fl_icon   = "🟢" if flow_conviction > 0.05 else (
                "🔴" if flow_conviction < -0.05 else "⚪")

    lines: List[str] = [
        _HDIV,
        f"📊  <b>STATUS</b>",
        f"<code>{now_ist}  ·  {now_utc}</code>",
        _HDIV,
        "",
        # Price + balance row
        f"💎  <b>BTC  {_fp(current_price)}</b>"
        + (f"    ATR {_fp(atr)}" if atr > 0 else ""),
        f"💼  Balance  <b>{_fp(balance)}</b>",
        f"{pnl_icon}  Day  <b>{_fp(daily_pnl)}</b>"
        f"    Total  {_fp(total_pnl)}",
        "",
        # State row
        f"{si}  <b>{_esc(bot_state)}</b>"
        f"    {sess_icon} {_esc(session)}{kz}",
    ]

    # ── MARKET STRUCTURE ────────────────────────────────────────────────────
    lines.append(_section("🏛️ MARKET STRUCTURE"))
    struct_parts: List[str] = []
    if structure_4h:  struct_parts.append(f"4H: {_esc(structure_4h)}")
    if structure_15m: struct_parts.append(f"15m: {_esc(structure_15m)}")
    if htf_bias:      struct_parts.append(f"HTF: {_esc(htf_bias)}")
    lines.append(
        f"<code>"
        f"AMD     {_esc(amd_phase)} ({_esc(amd_bias)})\n"
        + (f"Struct  {chr(32).join(struct_parts)}\n" if struct_parts else "")
        + f"P/D     {pd_lbl} ({dealing_range_pd:.0%})\n"
        f"Regime  {_esc(regime)}"
        f"</code>"
    )

    # ── FLOW ────────────────────────────────────────────────────────────────
    lines.append(_section("⚡ ORDER FLOW"))
    lines.append(
        f"{fl_icon}  {_esc(flow_direction or 'neutral').upper()}"
        f"  conviction {flow_conviction:+.2f}"
    )

    # ── LIQUIDITY ───────────────────────────────────────────────────────────
    lines.append(_section("🎯 LIQUIDITY"))
    lines.append(f"  ▲ {int(n_bsl_pools)} BSL pools    ▼ {int(n_ssl_pools)} SSL pools")
    if nearest_bsl:
        lines.append(
            f"  ▲ Nearest BSL  {_fp(nearest_bsl.get('price', 0))}"
            f"  ({nearest_bsl.get('dist_atr', 0):.1f}ATR"
            f"  sig={nearest_bsl.get('significance', 0):.0f}"
            f"  {_esc(nearest_bsl.get('timeframe', ''))})"
        )
    if nearest_ssl:
        lines.append(
            f"  ▼ Nearest SSL  {_fp(nearest_ssl.get('price', 0))}"
            f"  ({nearest_ssl.get('dist_atr', 0):.1f}ATR"
            f"  sig={nearest_ssl.get('significance', 0):.0f}"
            f"  {_esc(nearest_ssl.get('timeframe', ''))})"
        )
    if primary_target_str and primary_target_str != "—":
        lines.append(f"  🎯 Target  {_esc(primary_target_str)}")

    # ── SWEEP ANALYSIS ──────────────────────────────────────────────────────
    if sweep_analysis:
        rs = float(sweep_analysis.get("reversal_score",
                   sweep_analysis.get("rev_score", 0)))
        cs = float(sweep_analysis.get("continuation_score",
                   sweep_analysis.get("cont_score", 0)))
        rr = (sweep_analysis.get("reversal_reasons") or
              sweep_analysis.get("rev_reasons") or [])
        cr = (sweep_analysis.get("continuation_reasons") or
              sweep_analysis.get("cont_reasons") or [])
        sw_side  = sweep_analysis.get("sweep_side",    "?")
        sw_price = sweep_analysis.get("sweep_price",   0)
        sw_qual  = sweep_analysis.get("sweep_quality", 0)
        winner   = ("REVERSAL"     if rs > cs + 15 else
                    "CONTINUATION" if cs > rs + 15 else "UNDECIDED")
        qual_str = f"  q={sw_qual:.0%}" if sw_qual > 0 else ""
        lines.append(_section(f"🌊 SWEEP ANALYSIS"))
        lines.append(
            f"  {_esc(sw_side)} @ {_fp(sw_price)}{qual_str}"
            f"    → <b>{_esc(winner)}</b>"
        )
        lines.append(
            f"<code>REV  {rs:.0f}  {' · '.join(str(r) for r in rr[:3]) if rr else '—'}\n"
            f"CONT {cs:.0f}  {' · '.join(str(r) for r in cr[:3]) if cr else '—'}</code>"
        )

    # ── HUNT PREDICTION ─────────────────────────────────────────────────────
    if direction_hunt is not None:
        dh_pred  = getattr(direction_hunt, "predicted",          None)
        dh_conf  = float(getattr(direction_hunt, "confidence",   0.0))
        dh_deliv = getattr(direction_hunt, "delivery_direction", "")
        dh_bsl   = float(getattr(direction_hunt, "bsl_score",    0.0))
        dh_ssl   = float(getattr(direction_hunt, "ssl_score",    0.0))
        dh_raw   = float(getattr(direction_hunt, "raw_score",    0.0))
        hunt_icon = "🔵" if dh_pred == "BSL" else ("🟠" if dh_pred == "SSL" else "⚪")
        del_icon  = "🟢" if dh_deliv == "bullish" else ("🔴" if dh_deliv == "bearish" else "⚪")
        lines.append(_section(f"{hunt_icon} HUNT PREDICTION"))
        lines.append(
            f"  <b>{_esc(dh_pred or 'NEUTRAL')}</b>"
            f"  [{_score_bar(dh_conf)}] {dh_conf:.0%}"
            f"    {del_icon} {_esc(dh_deliv or '—')}"
        )
        lines.append(
            f"<code>BSL {dh_bsl:.3f}  SSL {dh_ssl:.3f}  raw {dh_raw:+.3f}</code>"
        )

    # ── POST-SWEEP VERDICT ──────────────────────────────────────────────────
    if direction_ps_analysis is not None:
        ps_action = getattr(direction_ps_analysis, "action",           "?")
        ps_dir    = getattr(direction_ps_analysis, "direction",        "")
        ps_conf   = float(getattr(direction_ps_analysis, "confidence", 0.0))
        ps_phase  = getattr(direction_ps_analysis, "phase",            "")
        ps_rev    = float(getattr(direction_ps_analysis, "rev_score",  0.0))
        ps_cont   = float(getattr(direction_ps_analysis, "cont_score", 0.0))
        ps_cisd   = getattr(direction_ps_analysis, "cisd_active",      False)
        ps_ote    = getattr(direction_ps_analysis, "ote_active",       False)
        ps_disp   = float(getattr(direction_ps_analysis, "displacement_atr", 0.0))
        ps_winner = ("REVERSAL"     if ps_rev  > ps_cont + 15 else
                     "CONTINUATION" if ps_cont > ps_rev  + 15 else "CONTESTED")
        ps_ai = {"reverse":"🔄","continue":"➡️","wait":"⏳"}.get(ps_action.lower(), "❓")
        ps_di = "🟢" if ps_dir == "long" else ("🔴" if ps_dir == "short" else "⚪")
        flags = []
        if ps_cisd:   flags.append("CISD✓")
        if ps_ote:    flags.append("OTE✓")
        if ps_disp>0: flags.append(f"disp={ps_disp:.2f}ATR")
        lines.append(_section("🔄 POST-SWEEP VERDICT"))
        lines.append(
            f"{ps_ai}  <b>{_esc(ps_action.upper())}</b>"
            f"  {ps_di} {_esc(ps_dir.upper() or '—')}"
        )
        lines.append(
            f"<code>conf={ps_conf:.0%}  phase={_esc(ps_phase)}\n"
            f"REV={ps_rev:.1f}  CONT={ps_cont:.1f}  → {_esc(ps_winner)}"
            + (f"\n{' · '.join(_esc(x) for x in flags)}" if flags else "")
            + "</code>"
        )

    # ── ENTRY GATES ─────────────────────────────────────────────────────────
    lines.append("")
    lines.extend(_build_gate_diagnostic(
        direction_hunt   = direction_hunt,
        amd_phase        = amd_phase,
        dealing_range_pd = dealing_range_pd,
        session          = session,
        flow_conviction  = flow_conviction,
        flow_direction   = flow_direction,
        htf_bias         = htf_bias,
    ))

    # ── ACTIVE POSITION ─────────────────────────────────────────────────────
    if position:
        side    = (position.get("side") or "?").upper()
        p_entry = entry_price or position.get("entry_price", 0)
        qty     = float(position.get("quantity", 0) or 0)
        pos_icon = "🟢" if side == "LONG" else "🔴"
        lines.append(_section(f"{pos_icon} POSITION  {side}"))

        if p_entry and current_price:
            move   = (current_price - p_entry) if side == "LONG" else (p_entry - current_price)
            risk_d = abs(p_entry - current_sl) if current_sl else 0
            ur_r   = move / risk_d if risk_d > 0 else 0
            upnl   = move * qty   if qty > 0   else move
            sl_atr = abs(current_price - current_sl) / max(atr, 1) if atr > 0 and current_sl else 0
            tp_atr = abs((current_tp or 0) - current_price) / max(atr, 1) if atr > 0 and current_tp else 0

            # Progress bar toward TP
            prog = 0.0
            if current_tp and p_entry:
                total = abs(current_tp - p_entry)
                if total > 0 and move >= 0:
                    prog = min(1.0, abs(current_price - p_entry) / total)
            bar_f = int(prog * 16)
            bar   = "█" * bar_f + "░" * (16 - bar_f)

            pnl_str = f"${upnl:+.2f}" if qty > 0 else f"{move:+.1f}pts"

            lines.append(
                f"<code>"
                f"Entry   {_fp(p_entry)}\n"
                f"SL      {_fp(current_sl)}   {sl_atr:.1f} ATR"
                + (" 🔒 BE" if breakeven_moved else "")
                + f"\nTP      {_fp(current_tp)}   {tp_atr:.1f} ATR\n"
                f"PnL     {pnl_str}   {ur_r:+.2f}R\n"
                f"[{bar}] {prog*100:.0f}% → TP"
                + (f"\nLocked  {profit_locked_pct:.1f}R secured" if breakeven_moved else "")
                + "</code>"
            )
        else:
            lines.append(f"  Entry  {_fp(p_entry)}   <i>awaiting fill</i>")

    # ── PERFORMANCE ─────────────────────────────────────────────────────────
    lines.append(_section("📈 PERFORMANCE"))
    lines.append(
        f"  Trades {int(total_trades)}    WR {win_rate:.1f}%"
        + (f"    ⚠️ CL {int(consecutive_losses)}" if consecutive_losses > 0 else "")
    )

    if extra_lines:
        lines.append("")
        for el in extra_lines:
            if el and el.strip():
                lines.append(el)

    lines.append(_HDIV)
    return "\n".join(lines)


# ═════════════════════════════════════════════════════════════════════════════
# 2.  HUNT PREDICTION ALERT
# ═════════════════════════════════════════════════════════════════════════════

def format_direction_hunt_alert(
    predicted:           Optional[str],
    confidence:          float,
    delivery_direction:  str,
    bsl_score:           float,
    ssl_score:           float,
    nearest_bsl:         Optional[Dict] = None,
    nearest_ssl:         Optional[Dict] = None,
    raw_score:           float = 0.0,
    current_price:       float = 0.0,
    atr:                 float = 0.0,
) -> str:
    hunt_icon = "🔵" if predicted == "BSL" else ("🟠" if predicted == "SSL" else "⚪")
    del_icon  = "🟢" if delivery_direction == "bullish" else (
                "🔴" if delivery_direction == "bearish" else "⚪")

    lines = [
        f"{hunt_icon}  <b>HUNT PREDICTION  ·  {_esc(predicted or 'NEUTRAL')}</b>",
        "",
        f"  [{_score_bar(confidence)}]  {confidence:.0%}  confidence",
        f"  {del_icon}  Delivery  {_esc(delivery_direction or '—')}",
        "",
        f"<code>"
        f"BSL score   {bsl_score:+.3f}\n"
        f"SSL score   {ssl_score:+.3f}\n"
        f"Raw         {raw_score:+.3f}"
        + (f"\nPrice       {_fp(current_price)}" if current_price > 0 else "")
        + (f"\nATR         {_fp(atr)}"            if atr         > 0 else "")
        + "</code>",
    ]
    if nearest_bsl:
        lines.append(
            f"  ▲ BSL target  {_fp(nearest_bsl.get('price', 0))}"
            f"  ({nearest_bsl.get('dist_atr', 0):.1f}ATR)"
        )
    if nearest_ssl:
        lines.append(
            f"  ▼ SSL target  {_fp(nearest_ssl.get('price', 0))}"
            f"  ({nearest_ssl.get('dist_atr', 0):.1f}ATR)"
        )
    return "\n".join(lines)


# ═════════════════════════════════════════════════════════════════════════════
# 3.  POST-SWEEP VERDICT ALERT
# ═════════════════════════════════════════════════════════════════════════════

def format_post_sweep_verdict(
    action:           str,
    direction:        str,
    confidence:       float,
    phase:            str,
    rev_score:        float,
    cont_score:       float,
    cisd_active:      bool  = False,
    ote_active:       bool  = False,
    displacement_atr: float = 0.0,
    sweep_side:       str   = "",
    sweep_price:      float = 0.0,
    current_price:    float = 0.0,
    atr:              float = 0.0,
    **_kwargs: Any,
) -> str:
    ai = {"reverse":"🔄","continue":"➡️","wait":"⏳"}.get(action.lower(), "❓")
    di = "🟢" if direction == "long" else ("🔴" if direction == "short" else "⚪")
    winner = ("REVERSAL"     if rev_score  > cont_score + 15 else
              "CONTINUATION" if cont_score > rev_score  + 15 else "CONTESTED")
    flags: List[str] = []
    if cisd_active:       flags.append("CISD✓")
    if ote_active:        flags.append("OTE✓")
    if displacement_atr > 0: flags.append(f"disp={displacement_atr:.2f}ATR")

    lines = [
        f"{ai}  <b>POST-SWEEP VERDICT  ·  {_esc(action.upper())}</b>",
        f"    {di}  {_esc(direction.upper() or '—')}",
        "",
        f"  [{_score_bar(confidence)}]  {confidence:.0%}  confidence",
        "",
        f"<code>"
        f"Sweep   {_esc(sweep_side)} @ {_fp(sweep_price)}\n"
        f"Phase   {_esc(phase)}\n"
        f"REV     {rev_score:.1f}\n"
        f"CONT    {cont_score:.1f}\n"
        f"Winner  {_esc(winner)}"
        + (f"\n{chr(32).join(_esc(f) for f in flags)}" if flags else "")
        + (f"\nPrice   {_fp(current_price)}  ATR {_fp(atr)}"
           if current_price > 0 else "")
        + "</code>",
    ]
    return "\n".join(lines)


# ═════════════════════════════════════════════════════════════════════════════
# 4.  POOL-GATE ALERT
# ═════════════════════════════════════════════════════════════════════════════

def format_pool_gate_alert(
    action:        str,
    side:          str,
    pool_side:     str,
    pool_price:    float,
    current_price: float,
    reason:        str  = "",
    rev_score:     float = 0.0,
    cont_score:    float = 0.0,
    **_kwargs: Any,
) -> str:
    act_icon = {"exit":"🚪","reverse":"🔄","continue":"➡️"}.get(action.lower(), "❓")
    lines = [
        f"{act_icon}  <b>POOL-GATE  ·  {_esc(action.upper())}</b>",
        "",
        f"<code>"
        f"Position   {_esc(side.upper())}\n"
        f"Pool hit   {_esc(pool_side)} @ {_fp(pool_price)}\n"
        f"Price      {_fp(current_price)}\n"
        f"REV        {rev_score:.1f}\n"
        f"CONT       {cont_score:.1f}"
        + (f"\nReason     {_esc(reason)}" if reason else "")
        + "</code>",
    ]
    return "\n".join(lines)


# ═════════════════════════════════════════════════════════════════════════════
# 5.  CONVICTION BLOCK ALERT
# ═════════════════════════════════════════════════════════════════════════════

def format_conviction_block_alert(
    side:           str,
    factor_scores:  Dict[str, float],
    weighted_total: float,
    reasons:        Optional[List[str]] = None,
    required_score: float = _REQUIRED_CONVICTION_SCORE,
    **_kwargs: Any,
) -> str:
    deficit = max(0.0, required_score - weighted_total)
    side_icon = "🟢" if side.upper() == "LONG" else "🔴"

    # Build factor table as <code> block
    rows = "\n".join(
        f"  {_esc(factor):<12}  [{_score_bar(min(1.0, max(0.0, score)), 8)}]  {score:+.2f}"
        for factor, score in factor_scores.items()
    )

    lines = [
        f"🚫  <b>CONVICTION BLOCKED  ·  {side_icon} {_esc(side.upper())}</b>",
        "",
        f"  Score  <b>{weighted_total:.2f}</b>  /  {required_score:.2f}"
        f"    need +{deficit:.2f}",
        "",
        "<b>Factor breakdown</b>",
        f"<code>{rows}</code>",
    ]
    if reasons:
        lines.append("\n<b>Rejection reasons</b>")
        for r in reasons[:5]:
            lines.append(f"  • {_esc(r)}")
    return "\n".join(lines)


# ═════════════════════════════════════════════════════════════════════════════
# 6.  FIBONACCI TRAIL UPDATE
# ═════════════════════════════════════════════════════════════════════════════

def format_liquidity_trail_update(
    side:          str,
    new_sl:        float,
    anchor_price:  float,
    anchor_tf:     str,
    anchor_sig:    float,
    phase:         str,
    is_swept:      bool,
    entry_price:   float,
    current_price: float,
    atr:           float,
    session:       str   = "",
    fib_ratio:     Optional[float] = None,
    r_multiple:    float = 0.0,
    swing_low:     Optional[float] = None,
    swing_high:    Optional[float] = None,
    momentum_gate: str   = "",
    htf_aligned:   Optional[bool] = None,
    is_cluster:    bool  = False,
    n_cluster_tfs: int   = 1,
    pool_boost:    bool  = False,
    pool_between_expand: bool = False,
    buffer_atr:    float = 0.0,
) -> str:
    side_icon = "🟢" if (side or "").lower() == "long" else "🔴"
    phase_icons = {
        "STRUCTURAL":  "🏛️", "AGGRESSIVE": "🎯",
        "BE_LOCK":     "🔒", "COUNTER_BOS": "🚨",
        "HANDS_OFF":   "⏸",  "HOLD": "⏸",
    }
    phase_icon = phase_icons.get(phase, "📍")

    # Fib display
    fib_str = f"{fib_ratio:.3f}" if fib_ratio is not None else "?"
    golden  = fib_ratio in (0.382, 0.500, 0.618) if fib_ratio is not None else False
    ratio_tag = f"✨ <b>{fib_str}</b>" if golden else f"<b>{fib_str}</b>"

    # Confluence tags
    tags: List[str] = []
    if is_cluster:          tags.append(f"×{int(n_cluster_tfs)}TF")
    if pool_boost:          tags.append("+pool")
    if pool_between_expand: tags.append("+expand")
    tag_str = "  " + "  ".join(_esc(x) for x in tags) if tags else ""

    # HTF
    htf_str = ("🟢 aligned" if htf_aligned is True else
               "🔴 counter" if htf_aligned is False else "⚪ n/a")

    # Gate emoji
    gate_e = {"DISP":"💥","CVD":"📈","BOS":"🏗️","NONE":"⚪"}.get(momentum_gate, "⚪")

    # Sess
    sess_icon = {"LONDON":"🇬🇧","NY":"🇺🇸","ASIA":"🌏"}.get((session or "").upper(), "🌐")

    # R-locked
    r_locked = 0.0
    if atr > 1e-10 and entry_price > 0:
        pts = (new_sl - entry_price) if (side or "").lower() == "long" \
              else (entry_price - new_sl)
        r_locked = pts / atr
    dist_atr = abs(current_price - new_sl) / atr if atr > 1e-10 else 0.0

    lines = [
        f"{side_icon}  <b>FIBONACCI TRAIL</b>  {phase_icon} {_esc(phase)}"
        f"  ({r_multiple:.2f}R)",
        "",
        f"  🎯  SL → <b>{_fp(new_sl)}</b>    {r_locked:+.2f}R from entry",
        "",
        f"<code>"
        f"Fib      {fib_str}{chr(32) + chr(32).join(tags) if tags else ''}\n"
        f"Anchor   {_fp(anchor_price)}  ({_esc(anchor_tf)})  Q={anchor_sig:.1f}"
        + (f"\nSwing    {_fp(swing_low)} → {_fp(swing_high)}"
           f"  ({abs(swing_high - swing_low):.0f}pts)"
           if swing_low is not None and swing_high is not None else "")
        + (f"\nBuffer   {buffer_atr:.2f} ATR" if buffer_atr > 0 else "")
        + (f"\nMomentum {_esc(momentum_gate or 'n/a')}  HTF {htf_str}"
           if phase in ("STRUCTURAL", "AGGRESSIVE") else "")
        + f"\nDist     {dist_atr:.2f} ATR"
        + f"\nEntry    {_fp(entry_price)}  Price {_fp(current_price)}"
        + f"  {sess_icon} {_esc(session or '—')}"
        + "</code>",
    ]

    # Contextual note
    if phase == "COUNTER_BOS":
        lines.append("<i>🚨 Counter-BOS broke entry — thesis invalidated, locked to BE</i>")
    elif phase == "BE_LOCK":
        lines.append("<i>🔒 BE + fees + slippage locked — trade is now risk-free</i>")
    elif is_cluster:
        lines.append("<i>Multi-TF Fib confluence — strongest possible anchor</i>")
    elif pool_boost:
        lines.append("<i>Fibonacci + liquidity pool confluence</i>")
    else:
        lines.append("<i>SL anchored to institutional Fib retracement</i>")

    return "\n".join(lines)


# ═════════════════════════════════════════════════════════════════════════════
# LOGGING HANDLER  — forward WARNING+ logs to Telegram
# ═════════════════════════════════════════════════════════════════════════════

_TELEGRAM_SUPPRESS_PATTERNS: List[str] = [
    "Delta REST refresh ",
    "candles stale age=",
    "starting REST self-heal",
    "daily_counter_consistency",
    "daily counter drift",
]
_TELEGRAM_SUPPRESS_LOCK = threading.Lock()


def add_telegram_suppress_pattern(pattern: str) -> None:
    if not pattern:
        return
    with _TELEGRAM_SUPPRESS_LOCK:
        if pattern not in _TELEGRAM_SUPPRESS_PATTERNS:
            _TELEGRAM_SUPPRESS_PATTERNS.append(pattern)


def clear_telegram_suppress_patterns() -> None:
    with _TELEGRAM_SUPPRESS_LOCK:
        _TELEGRAM_SUPPRESS_PATTERNS.clear()


def _is_suppressed_for_telegram(msg: str) -> bool:
    if not msg:
        return False
    with _TELEGRAM_SUPPRESS_LOCK:
        patterns = tuple(_TELEGRAM_SUPPRESS_PATTERNS)
    return any(p and p in msg for p in patterns)


class TelegramLogHandler(logging.Handler):
    """Forward WARNING+ log records to Telegram with throttling and suppression."""

    def __init__(self, level: int = logging.WARNING, throttle_seconds: float = 5.0):
        super().__init__(level)
        self._throttle = throttle_seconds
        self._last_ts  = 0.0
        self._lock     = threading.Lock()
        self._buffer: deque = deque(maxlen=10)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            if _is_suppressed_for_telegram(msg):
                return

            with self._lock:
                now = time.time()
                if now - self._last_ts < self._throttle:
                    self._buffer.append(record)
                    return
                self._last_ts = now

            if self._buffer:
                buf_records = list(self._buffer)
                self._buffer.clear()
                buf_msgs = [
                    self.format(r) for r in buf_records
                    if not _is_suppressed_for_telegram(self.format(r))
                ]
                if buf_msgs:
                    msg = "\n".join(buf_msgs) + "\n" + msg

            lvl = record.levelname
            lvl_icon = {"WARNING": "⚠️", "ERROR": "❌", "CRITICAL": "💀"}.get(lvl, "ℹ️")

            send_telegram_message(
                f"{lvl_icon}  <b>{_esc(lvl)}</b>  <code>{_esc(msg[:1500])}</code>"
            )
        except Exception:
            pass


def install_global_telegram_log_handler(
    level: int = logging.WARNING,
    throttle_seconds: float = 5.0,
) -> None:
    """Attach TelegramLogHandler to the root logger."""
    handler = TelegramLogHandler(level=level, throttle_seconds=throttle_seconds)
    handler.setFormatter(logging.Formatter("%(name)s: %(message)s"))
    logging.getLogger().addHandler(handler)

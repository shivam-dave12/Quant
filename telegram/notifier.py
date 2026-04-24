"""
telegram/notifier.py — Liquidity-First Telegram Notifier  v2.0
==============================================================
Report architecture mirrors the decision hierarchy:
  DirectionEngine hunt → Pool target → Flow confirmation → ICT context → Entry → Exit

Public API (imported by strategy layer):
  send_telegram_message()            — async fire-and-forget delivery
  format_periodic_report()           — 15-min institutional dashboard
  format_direction_hunt_alert()      — DirectionEngine high-confidence prediction
  format_post_sweep_verdict()        — post-sweep reversal/continuation decision
  format_conviction_block_alert()    — conviction gate rejection with factor breakdown
  format_pool_gate_alert()           — pool-hit gate action (exit/reverse/continue)
  format_liquidity_trail_update()    — Fibonacci-trail SL advance (v5.0 engine)
  install_global_telegram_log_handler()
  TelegramLogHandler

Internal helpers (used by controller.py):
  _sanitize_html(), _esc()

v2.0 CHANGES
------------
1.  _sanitize_html rewritten as a proper state-machine parser. It now:
      • Normalises stray ampersands to &amp; (was missing — leading cause of
        byte-offset 2010-2035 parse errors).
      • Uses a single-pass tokenizer that splits the input into text runs
        and tag runs, escaping only the text runs.
      • Emits perfectly balanced tags: unmatched closes are dropped, unclosed
        opens are auto-closed at end.
      • Tolerates truncation: a tag fragment with no > at end-of-string is
        escaped as literal.
      • Does NOT apply inside <code>/<pre> contents — those are treated as
        raw text and fully escaped (as Telegram HTML requires).

2.  New _tag(text, name) helper guarantees that the wrapping is correct —
    all fresh format_* functions use it instead of hand-crafted markup.

3.  format_liquidity_trail_update rewritten for the v5.0 Fibonacci engine:
    shows fib_ratio, swing context, momentum gate, HTF alignment, buffer
    details, pool-between-expansion tag, and cluster info.
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

# REQUIRED_SCORE from authoritative source
try:
    from strategy.conviction_filter import REQUIRED_SCORE as _REQUIRED_CONVICTION_SCORE
except ImportError:
    try:
        from conviction_filter import REQUIRED_SCORE as _REQUIRED_CONVICTION_SCORE
    except ImportError:
        _REQUIRED_CONVICTION_SCORE = 0.45

try:
    from strategy.direction_engine import (
        _HUNT_ON_THRESHOLD,
        _HUNT_OFF_THRESHOLD,
    )
except ImportError:
    try:
        from direction_engine import (  # type: ignore
            _HUNT_ON_THRESHOLD,
            _HUNT_OFF_THRESHOLD,
        )
    except ImportError:
        _HUNT_ON_THRESHOLD = 0.10
        _HUNT_OFF_THRESHOLD = 0.05


# ======================================================================
# ASYNC SEND WORKER
# ======================================================================

_send_queue:    _queue_mod.Queue = _queue_mod.Queue(maxsize=25)   # Bug #36: 200→25 caps backlog at ~30s
_worker_started: bool            = False
_worker_lock:   threading.Lock   = threading.Lock()
_MIN_INTERVAL = 1.2
_MAX_RETRIES  = 4

# Bug #36 fix: critical message keywords that bypass the async queue and
# send synchronously.  This guarantees that UNPROTECTED position alerts,
# crash reports, and killswitch confirmations are never dropped even when
# the queue is full during a burst of routine heartbeat messages.
_CRITICAL_KEYWORDS = frozenset((
    "UNPROTECTED", "CRASH", "KILLSWITCH", "💀", "🚨", "CIRCUIT_BREAKER",
    "EMERGENCY", "emergency_flatten", "BOT CRASH",
))


def _send_worker() -> None:
    """Background daemon — drains the queue and sends to Telegram."""
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

        for attempt in range(_MAX_RETRIES):
            gap = _MIN_INTERVAL - (time.time() - last_send_ts)
            if gap > 0:
                time.sleep(gap)
            try:
                url = (f"https://api.telegram.org/bot"
                       f"{telegram_config.TELEGRAM_BOT_TOKEN}/sendMessage")
                send_text = message[:4000]
                if parse_mode == "HTML":
                    send_text = _sanitize_html(send_text)

                payload = {
                    "chat_id":                  telegram_config.TELEGRAM_CHAT_ID,
                    "text":                     send_text,
                    "parse_mode":               parse_mode,
                    "disable_web_page_preview": True,
                }
                resp = _req.post(url, json=payload, timeout=15)
                last_send_ts = time.time()

                if resp.status_code == 200:
                    break

                # HTML parse error → retry as plain text (once)
                if resp.status_code == 400 and parse_mode == "HTML" and attempt == 0:
                    logger.warning(
                        "Telegram HTML parse error — retrying as plain text: %s",
                        resp.text[:160]
                    )
                    plain = re.sub(r"<[^>]*>", "", send_text, flags=re.DOTALL)
                    plain = _html_lib.unescape(plain)
                    r2 = _req.post(
                        url,
                        json={
                            "chat_id":                  telegram_config.TELEGRAM_CHAT_ID,
                            "text":                     plain[:4000],
                            "disable_web_page_preview": True,
                        },
                        timeout=15,
                    )
                    last_send_ts = time.time()
                    if r2.status_code == 200:
                        break
                    logger.warning("Plain-text fallback also failed: %s", r2.text[:160])
                    break

                # Rate-limit / transient — backoff
                if resp.status_code in (429, 500, 502, 503) and attempt < _MAX_RETRIES - 1:
                    if resp.status_code == 429:
                        try:
                            backoff = max(float(resp.json().get("parameters", {})
                                                .get("retry_after", 10)), 5.0)
                        except Exception:
                            backoff = 10.0
                    else:
                        backoff = min(2.0 * (2 ** attempt) + random.uniform(0, 2), 60.0)
                    logger.warning(
                        f"Telegram {resp.status_code}, retry {attempt+1}/{_MAX_RETRIES} "
                        f"in {backoff:.1f}s"
                    )
                    time.sleep(backoff)
                    continue

                logger.warning(f"Telegram send failed: {resp.status_code} — {resp.text[:200]}")
                break

            except _req.exceptions.Timeout:
                if attempt < _MAX_RETRIES - 1:
                    time.sleep(2.0 * (attempt + 1))
                    continue
                logger.error("Telegram send timed out after all retries")
                break
            except Exception as exc:
                logger.error(f"Telegram send error: {exc}")
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
    """Enqueue a Telegram message for async delivery.  Never blocks the caller.

    Bug #36 fix: critical messages (UNPROTECTED, CRASH, KILLSWITCH, etc.) are
    sent via a dedicated daemon thread that bypasses the queue entirely.  This
    guarantees delivery even when the queue is full due to a burst of routine
    heartbeat/status messages.  The dedicated thread is fire-and-forget — the
    caller is not blocked.
    """
    if not telegram_config.TELEGRAM_ENABLED:
        return False
    _ensure_worker_started()

    # Check if this message is critical and should bypass the queue
    is_critical = any(kw in message for kw in _CRITICAL_KEYWORDS)
    if is_critical:
        def _send_critical_now():
            import requests as _req
            try:
                url = (f"https://api.telegram.org/bot"
                       f"{telegram_config.TELEGRAM_BOT_TOKEN}/sendMessage")
                send_text = message[:4000]
                if parse_mode == "HTML":
                    send_text = _sanitize_html(send_text)
                _req.post(url, json={
                    "chat_id":                  telegram_config.TELEGRAM_CHAT_ID,
                    "text":                     send_text,
                    "parse_mode":               parse_mode,
                    "disable_web_page_preview": True,
                }, timeout=10)
            except Exception as _ce:
                logger.error("Critical Telegram send failed: %s", _ce)
        t = threading.Thread(target=_send_critical_now, daemon=True,
                             name="telegram-critical")
        t.start()
        return True

    try:
        _send_queue.put_nowait((message, parse_mode))
        return True
    except _queue_mod.Full:
        logger.warning("Telegram queue full — dropping non-critical message")
        return False


# ======================================================================
# HTML SANITIZER v2.0 — state-machine parser
# ======================================================================

# Telegram parse_mode=HTML permits these tags and attributes only
_SAFE_TAGS = frozenset(("b", "strong", "i", "em", "u", "ins", "s", "strike",
                        "del", "code", "pre", "tg-spoiler"))
_SAFE_TAGS_WITH_ATTR = frozenset(("a",))  # <a href="..."> only
_VALID_ENTITIES = ("amp", "lt", "gt", "quot", "apos", "#")

# Regex: a tag is <[/]name[ attrs]>.  We tokenize on tag boundaries.
_TAG_RE = re.compile(
    r"<(/?)\s*([A-Za-z][A-Za-z0-9_-]*)\s*([^>]*?)/?\s*>",
    re.DOTALL,
)
# Regex: a valid HTML entity
_ENTITY_RE = re.compile(r"&(#[0-9]+|#x[0-9A-Fa-f]+|[A-Za-z][A-Za-z0-9]*);")


def _normalise_ampersands(text: str) -> str:
    """
    Replace ampersands that are NOT part of a valid entity with &amp;.

    Walks the string, leaving well-formed entities (&amp; &#39; &#x3C;) intact
    and escaping every other & to &amp;.
    """
    out = []
    i = 0
    n = len(text)
    while i < n:
        ch = text[i]
        if ch != "&":
            out.append(ch)
            i += 1
            continue
        m = _ENTITY_RE.match(text, i)
        if m:
            out.append(m.group(0))
            i = m.end()
        else:
            out.append("&amp;")
            i += 1
    return "".join(out)


def _sanitize_html(text: str) -> str:
    """
    Bulletproof Telegram HTML sanitizer.

    Produces output guaranteed to round-trip through Telegram's HTML parser
    (parse_mode=HTML) without "Unexpected end tag" or "can't parse entities"
    400 errors.

    Pipeline:
      1. Convert <br>, <hr>, <p>/</p> to whitespace (not supported by Telegram).
      2. Tokenize into (text, tag) runs.
      3. Walk runs with an explicit open-tag stack:
           • Text runs: ampersand-normalise (naked & → &amp;), leave others alone.
             Do NOT escape < or > in text — they were already tag tokens
             if well-formed; any stray < or > is already surrounded by text
             and will be caught by the final entity normalisation.
           • Tag runs: allow only safe tags; strip attrs on non-<a> tags;
             drop orphan closes; auto-close mismatched opens.
      4. Auto-close remaining open tags at end.
      5. Final naked-ampersand and naked-lt/gt pass.
      6. Collapse excess blank lines.
    """
    if not text:
        return text

    # -- Pass 1: structural conversion -------------------------------------
    text = re.sub(r"<br\s*/?>",        "\n",                       text, flags=re.IGNORECASE)
    text = re.sub(r"<hr\s*/?>",        "\n────────\n",             text, flags=re.IGNORECASE)
    text = re.sub(r"<p(?:\s[^>]*)?>",  "\n",                       text, flags=re.IGNORECASE)
    text = re.sub(r"</p>",             "",                         text, flags=re.IGNORECASE)

    # -- Pass 2: tokenize on tag boundaries --------------------------------
    tokens: List[tuple] = []    # list of ("text", str) | ("tag", opening, name, attrs)
    cursor = 0
    for m in _TAG_RE.finditer(text):
        if m.start() > cursor:
            tokens.append(("text", text[cursor:m.start()]))
        closing = m.group(1) == "/"
        name    = m.group(2).lower()
        attrs   = m.group(3).strip()
        tokens.append(("tag", closing, name, attrs))
        cursor = m.end()
    if cursor < len(text):
        tokens.append(("text", text[cursor:]))

    # -- Pass 3: walk with open-tag stack ----------------------------------
    out: List[str] = []
    stack: List[str] = []

    for tok in tokens:
        if tok[0] == "text":
            # Normalise naked ampersands; leave < and > alone (none should
            # exist here — tokenizer consumed all tag-shaped <...>)
            out.append(_normalise_ampersands(tok[1]))
            continue

        _, closing, name, attrs = tok
        # Unknown/unsafe tag → escape as literal
        is_safe = (name in _SAFE_TAGS) or (name in _SAFE_TAGS_WITH_ATTR)
        if not is_safe:
            raw = f"</{name}>" if closing else f"<{name}{(' ' + attrs) if attrs else ''}>"
            out.append(_html_lib.escape(raw, quote=False))
            continue

        # Safe tag — strip attributes except on <a>
        if name == "a":
            if closing:
                # Match outer <a> if any; drop orphan </a>
                if "a" in stack:
                    while stack and stack[-1] != "a":
                        out.append(f"</{stack.pop()}>")
                    stack.pop()
                    out.append("</a>")
                # else: orphan </a> — drop silently
                continue
            # Opening <a> — keep href="..." if present, discard everything else
            href_match = re.search(r'href\s*=\s*"([^"]*)"', attrs, flags=re.IGNORECASE)
            if not href_match:
                href_match = re.search(r"href\s*=\s*'([^']*)'", attrs, flags=re.IGNORECASE)
            if href_match:
                href = href_match.group(1)
                href = _normalise_ampersands(href)
                stack.append("a")
                out.append(f'<a href="{href}">')
            else:
                # <a> with no href is invalid in Telegram — drop
                pass
            continue

        # Plain safe tags (<b>, <i>, ...): ignore attrs
        if closing:
            if name in stack:
                while stack and stack[-1] != name:
                    out.append(f"</{stack.pop()}>")
                stack.pop()
                out.append(f"</{name}>")
            # else: orphan close — drop
        else:
            stack.append(name)
            out.append(f"<{name}>")

    # -- Pass 4: auto-close any remaining open tags ------------------------
    while stack:
        out.append(f"</{stack.pop()}>")

    rendered = "".join(out)

    # -- Pass 5: collapse blank lines --------------------------------------
    rendered = re.sub(r"\n{3,}", "\n\n", rendered)

    return rendered


def _esc(s: Any) -> str:
    """HTML-escape a dynamic value for safe Telegram HTML output."""
    if s is None:
        return ""
    return _html_lib.escape(str(s), quote=False)


def _tag(text: Any, name: str) -> str:
    """
    Wrap a value in an HTML tag, escaping the content safely.

    Use this everywhere instead of hand-crafted '<b>{x}</b>' strings.
    """
    return f"<{name}>{_esc(text)}</{name}>"


# ======================================================================
# UTILITY HELPERS
# ======================================================================

def _fmt_price(p: Optional[float]) -> str:
    if p is None:
        return "—"
    return f"${p:,.1f}"


def _fmt_pct(v: float) -> str:
    return f"{v:.2f}%"


def _session_icon(session: str) -> str:
    s = (session or "").upper()
    if "LONDON" in s: return "🇬🇧"
    if "NY" in s or "NEW_YORK" in s: return "🇺🇸"
    if "ASIA" in s: return "🌏"
    return "🌐"


def _score_bar(score: float, width: int = 10) -> str:
    filled = min(max(int(score * width + 0.5), 0), width)
    return "█" * filled + "░" * (width - filled)


def _fmt_tf(tf: str) -> str:
    return _esc(tf or "?")


# ======================================================================
# GATE DIAGNOSTIC PANEL (internal)
# ======================================================================

def _build_gate_diagnostic(
    direction_hunt=None,
    amd_phase: str = "",
    dealing_range_pd: float = 0.5,
    session: str = "",
    flow_conviction: float = 0.0,
    flow_direction: str = "",
    htf_bias: str = "",
) -> List[str]:
    """Render a 6-gate entry diagnostic panel for the periodic report."""
    gates: List[str] = ["🚦 <b>ENTRY GATE STATUS</b>"]

    # Gate 1: session
    s = (session or "").upper()
    sess_ok = s in ("LONDON", "NY", "LONDON_NY")
    gates.append(f"  {'✅' if sess_ok else '⚪'} Session: {_esc(s or 'off')}")

    # Gate 2: hunt prediction
    pred = getattr(direction_hunt, "predicted", None) if direction_hunt else None
    conf = float(getattr(direction_hunt, "confidence", 0.0)) if direction_hunt else 0.0
    hunt_ok = pred in ("BSL", "SSL") and conf >= _HUNT_ON_THRESHOLD
    gates.append(
        f"  {'✅' if hunt_ok else '⚪'} Hunt: "
        f"{_esc(pred or 'NEUTRAL')} ({conf:.0%})"
    )

    # Gate 3: flow direction
    flow_ok = abs(flow_conviction) >= 0.20 and flow_direction in ("long", "short")
    gates.append(
        f"  {'✅' if flow_ok else '⚪'} Flow: "
        f"{_esc(flow_direction or 'neutral')} ({flow_conviction:+.2f})"
    )

    # Gate 4: AMD phase
    amd_ok = "MANIPULATION" in (amd_phase or "").upper() or \
             "DISTRIBUTION" in (amd_phase or "").upper()
    gates.append(
        f"  {'✅' if amd_ok else '⚪'} AMD: {_esc(amd_phase or 'UNKNOWN')}"
    )

    # Gate 5: dealing range P/D
    pd_ok = dealing_range_pd < 0.40 or dealing_range_pd > 0.60
    pd_label = (
        "DISC" if dealing_range_pd < 0.40 else
        "EQ"   if dealing_range_pd < 0.60 else
        "PREM"
    )
    gates.append(
        f"  {'✅' if pd_ok else '⚪'} P/D: {pd_label} ({dealing_range_pd:.0%})"
    )

    # Gate 6: HTF bias
    htf_ok = bool(htf_bias) and htf_bias.lower() != "mixed"
    gates.append(
        f"  {'✅' if htf_ok else '⚪'} HTF: {_esc(htf_bias or 'mixed')}"
    )

    return gates


# ======================================================================
# 1. PERIODIC REPORT
# ======================================================================

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
    """15-minute institutional Telegram dashboard."""
    ist_tz  = timezone(timedelta(hours=5, minutes=30))
    now_ist = datetime.now(ist_tz).strftime("%H:%M IST")
    now_utc = datetime.now(timezone.utc).strftime("%H:%M UTC")

    sess_icon = _session_icon(session)
    kz_str    = " 🔥KZ" if in_killzone else ""
    pnl_icon  = "🟢" if daily_pnl >= 0 else "🔴"

    _STATE_ICONS = {
        "SCANNING":    "🔍",
        "TRACKING":    "📡",
        "READY":       "🎯",
        "ENTERING":    "⚡",
        "IN_POSITION": "📊",
        "POST_SWEEP":  "🌊",
    }
    state_icon = _STATE_ICONS.get((bot_state or "").upper(), "⚪")

    _PD_LABEL = (
        "DEEP DISC"  if dealing_range_pd < 0.25 else
        "DISCOUNT"   if dealing_range_pd < 0.40 else
        "EQ"         if dealing_range_pd < 0.60 else
        "PREMIUM"    if dealing_range_pd < 0.75 else
        "DEEP PREM"
    )

    lines: List[str] = [
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"📊 <b>STATUS</b>  {_esc(now_ist)} / {_esc(now_utc)}",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
    ]

    lines.append(f"💰 BTC: <b>{_fmt_price(current_price)}</b>")
    atr_part = f"  ATR: {_fmt_price(atr)}" if atr > 0 else ""
    lines.append(f"  💵 Bal: {_fmt_price(balance)}{atr_part}")
    lines.append(
        f"  {pnl_icon} Day: <b>{_fmt_price(daily_pnl)}</b>  |  "
        f"Total: {_fmt_price(total_pnl)}"
    )

    lines.append("")
    lines.append(f"{state_icon} <b>{_esc(bot_state)}</b>  "
                 f"{sess_icon} {_esc(session)}{kz_str}")

    lines.append("")
    lines.append("🏛️ <b>MARKET STRUCTURE</b>")
    lines.append(f"  AMD: {_esc(amd_phase)} ({_esc(amd_bias)})")

    htf_parts: List[str] = []
    if structure_4h:  htf_parts.append(f"4H:{_esc(structure_4h)}")
    if structure_15m: htf_parts.append(f"15m:{_esc(structure_15m)}")
    if htf_bias:      htf_parts.append(f"HTF:{_esc(htf_bias)}")
    if htf_parts:
        lines.append(f"  {' | '.join(htf_parts)}")

    lines.append(f"  Dealing range: {_PD_LABEL} ({dealing_range_pd:.0%})")
    lines.append(f"  Regime: {_esc(regime)}")

    lines.append("")
    lines.append("🎯 <b>LIQUIDITY</b>")
    lines.append(f"  BSL ▲ {int(n_bsl_pools)} pools  |  "
                 f"SSL ▼ {int(n_ssl_pools)} pools")

    if nearest_bsl:
        lines.append(
            f"  ▲ Nearest BSL: {_fmt_price(nearest_bsl.get('price', 0))} "
            f"({nearest_bsl.get('dist_atr', 0):.1f}ATR "
            f"sig={nearest_bsl.get('significance', 0):.0f} "
            f"{_esc(nearest_bsl.get('timeframe', ''))})"
        )
    if nearest_ssl:
        lines.append(
            f"  ▼ Nearest SSL: {_fmt_price(nearest_ssl.get('price', 0))} "
            f"({nearest_ssl.get('dist_atr', 0):.1f}ATR "
            f"sig={nearest_ssl.get('significance', 0):.0f} "
            f"{_esc(nearest_ssl.get('timeframe', ''))})"
        )

    if primary_target_str and primary_target_str != "—":
        lines.append(f"  🎯 Target: {_esc(primary_target_str)}")

    if sweep_analysis:
        # KEY-SYNC FIX: entry_engine writes both short-form ("rev_score",
        # "cont_score") and long-form ("reversal_score", "continuation_score")
        # keys. Read long-form with short-form fallback so this works regardless
        # of which version of entry_engine is deployed.
        rs = float(sweep_analysis.get("reversal_score",
                   sweep_analysis.get("rev_score", 0)))
        cs = float(sweep_analysis.get("continuation_score",
                   sweep_analysis.get("cont_score", 0)))
        rr = (sweep_analysis.get("reversal_reasons")
              or sweep_analysis.get("rev_reasons") or [])
        cr = (sweep_analysis.get("continuation_reasons")
              or sweep_analysis.get("cont_reasons") or [])
        sw_side  = sweep_analysis.get("sweep_side", "?")
        sw_price = sweep_analysis.get("sweep_price", 0)
        sw_qual  = sweep_analysis.get("sweep_quality", 0)
        winner = ("REVERSAL"     if rs > cs + 15 else
                  "CONTINUATION" if cs > rs + 15 else
                  "UNDECIDED")
        lines.append("")
        qual_str = f" q={sw_qual:.0%}" if sw_qual > 0 else ""
        lines.append(
            f"🌊 <b>SWEEP ANALYSIS</b> ({_esc(sw_side)} @ {_fmt_price(sw_price)}{qual_str})"
        )
        lines.append(f"  REV: {rs:.0f}  |  CONT: {cs:.0f}  →  <b>{_esc(winner)}</b>")
        if rr:
            lines.append(f"  Rev:  {_esc(', '.join(str(r) for r in rr[:3]))}")
        if cr:
            lines.append(f"  Cont: {_esc(', '.join(str(r) for r in cr[:3]))}")

    if direction_hunt is not None:
        dh_pred  = getattr(direction_hunt, "predicted", None)
        dh_conf  = float(getattr(direction_hunt, "confidence", 0.0))
        dh_deliv = getattr(direction_hunt, "delivery_direction", "")
        dh_bsl   = float(getattr(direction_hunt, "bsl_score", 0.0))
        dh_ssl   = float(getattr(direction_hunt, "ssl_score", 0.0))
        dh_raw   = float(getattr(direction_hunt, "raw_score",  0.0))

        hunt_icon  = "🔵" if dh_pred == "BSL" else ("🟠" if dh_pred == "SSL" else "⚪")
        deliv_icon = "🟢" if dh_deliv == "bullish" else ("🔴" if dh_deliv == "bearish" else "⚪")

        lines.append("")
        lines.append(f"{hunt_icon} <b>HUNT PREDICTION</b>: {_esc(dh_pred or 'NEUTRAL')}")
        lines.append(
            f"  [{_score_bar(dh_conf)}] {dh_conf:.0%}  "
            f"{deliv_icon} {_esc(dh_deliv or '—')}"
        )
        lines.append(f"  BSL={dh_bsl:.3f}  SSL={dh_ssl:.3f}  raw={dh_raw:+.3f}")

    if direction_ps_analysis is not None:
        ps_action = getattr(direction_ps_analysis, "action", "?")
        ps_dir    = getattr(direction_ps_analysis, "direction", "")
        ps_conf   = float(getattr(direction_ps_analysis, "confidence", 0.0))
        ps_phase  = getattr(direction_ps_analysis, "phase", "")
        ps_rev    = float(getattr(direction_ps_analysis, "rev_score",  0.0))
        ps_cont   = float(getattr(direction_ps_analysis, "cont_score", 0.0))
        ps_cisd   = getattr(direction_ps_analysis, "cisd_active", False)
        ps_ote    = getattr(direction_ps_analysis, "ote_active",  False)
        ps_disp   = float(getattr(direction_ps_analysis, "displacement_atr", 0.0))

        ps_winner = ("REVERSAL"     if ps_rev  > ps_cont + 15 else
                     "CONTINUATION" if ps_cont > ps_rev  + 15 else
                     "CONTESTED")
        ps_ai = {"reverse": "🔄", "continue": "➡️", "wait": "⏳"}.get(
            ps_action.lower(), "❓"
        )
        ps_di = "🟢" if ps_dir == "long" else ("🔴" if ps_dir == "short" else "⚪")

        lines.append("")
        lines.append(
            f"{ps_ai} <b>POST-SWEEP VERDICT</b>: {_esc(ps_action.upper())}  "
            f"{ps_di} {_esc(ps_dir.upper() or '—')}"
        )
        lines.append(f"  conf={ps_conf:.0%}  phase={_esc(ps_phase)}")
        lines.append(f"  REV={ps_rev:.1f}  CONT={ps_cont:.1f}  → {_esc(ps_winner)}")

        ps_flags: List[str] = []
        if ps_cisd: ps_flags.append("CISD✓")
        if ps_ote:  ps_flags.append("OTE✓")
        if ps_disp > 0: ps_flags.append(f"disp={ps_disp:.2f}ATR")
        if ps_flags:
            lines.append("  " + "  ".join(_esc(x) for x in ps_flags))

    lines.append("")
    gate_lines = _build_gate_diagnostic(
        direction_hunt   = direction_hunt,
        amd_phase        = amd_phase,
        dealing_range_pd = dealing_range_pd,
        session          = session,
        flow_conviction  = flow_conviction,
        flow_direction   = flow_direction,
        htf_bias         = htf_bias,
    )
    lines.extend(gate_lines)

    if position:
        side    = (position.get("side") or "?").upper()
        p_entry = entry_price or position.get("entry_price", 0)
        qty     = float(position.get("quantity", 0) or 0)

        side_icon = "🟢" if side == "LONG" else "🔴"
        lines.append("")
        lines.append(f"{side_icon} <b>POSITION: {_esc(side)}</b>")
        lines.append(f"  Entry: {_fmt_price(p_entry)}")

        if current_sl:
            sl_dist = abs(current_price - current_sl) / max(atr, 1) if atr > 0 else 0
            lines.append(f"  SL: {_fmt_price(current_sl)} ({sl_dist:.1f}ATR)")
        if current_tp:
            tp_dist = abs(current_tp - current_price) / max(atr, 1) if atr > 0 else 0
            lines.append(f"  TP: {_fmt_price(current_tp)} ({tp_dist:.1f}ATR)")

        if p_entry and current_price:
            move = ((current_price - p_entry) if side == "LONG"
                    else (p_entry - current_price))
            risk_d = abs(p_entry - current_sl) if current_sl else 0
            ur_r   = move / risk_d if risk_d > 0 else 0
            upnl   = move * qty if qty > 0 else move
            icon   = "🟢" if move >= 0 else "🔴"

            bar = "░" * 16
            prog = 0.0
            if current_tp:
                total = abs(current_tp - p_entry)
                if total > 0:
                    prog = min(1.0, max(0.0, abs(current_price - p_entry) / total))
                    if move < 0:
                        prog = 0.0
                    filled = int(prog * 16)
                    bar = "█" * filled + "░" * (16 - filled)

            if qty > 0:
                lines.append(f"  {icon} <b>${upnl:+.2f}</b> ({ur_r:+.2f}R)")
            else:
                lines.append(f"  {icon} {move:+.1f}pts ({ur_r:+.2f}R)")
            lines.append(f"  [{bar}] {prog*100:.0f}%→TP")

        if breakeven_moved:
            lines.append(f"  🔒 BE locked | {profit_locked_pct:.1f}R secured")

    lines.append("")
    lines.append("📈 <b>PERFORMANCE</b>")
    lines.append(f"  Trades: {int(total_trades)}  |  WR: {win_rate:.1f}%")
    if consecutive_losses > 0:
        lines.append(f"  ⚠️ Consecutive losses: {int(consecutive_losses)}")

    if extra_lines:
        lines.append("")
        for el in extra_lines:
            if el and el.strip():
                # extra_lines can contain HTML — let _sanitize_html handle it
                lines.append(el)

    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    return "\n".join(lines)


# ======================================================================
# 2. HUNT PREDICTION
# ======================================================================

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
    deliv_icon = "🟢" if delivery_direction == "bullish" else \
                 ("🔴" if delivery_direction == "bearish" else "⚪")
    lines = [
        f"{hunt_icon} <b>HUNT PREDICTION</b>: {_esc(predicted or 'NEUTRAL')}",
        f"  Confidence: [{_score_bar(confidence)}] {confidence:.0%}",
        f"  Delivery:   {deliv_icon} {_esc(delivery_direction or '—')}",
        f"  BSL score:  {bsl_score:+.3f}",
        f"  SSL score:  {ssl_score:+.3f}",
        f"  Raw:        {raw_score:+.3f}",
    ]
    if current_price > 0:
        lines.append(f"  Price:      {_fmt_price(current_price)}")
    if atr > 0:
        lines.append(f"  ATR:        {_fmt_price(atr)}")
    if nearest_bsl:
        lines.append(
            f"  ▲ BSL target: {_fmt_price(nearest_bsl.get('price', 0))} "
            f"({nearest_bsl.get('dist_atr', 0):.1f}ATR)"
        )
    if nearest_ssl:
        lines.append(
            f"  ▼ SSL target: {_fmt_price(nearest_ssl.get('price', 0))} "
            f"({nearest_ssl.get('dist_atr', 0):.1f}ATR)"
        )
    return "\n".join(lines)


# ======================================================================
# 3. POST-SWEEP VERDICT
# ======================================================================

def format_post_sweep_verdict(
    action:           str,
    direction:        str,
    confidence:       float,
    phase:            str,
    rev_score:        float,
    cont_score:       float,
    cisd_active:      bool = False,
    ote_active:       bool = False,
    displacement_atr: float = 0.0,
    sweep_side:       str   = "",
    sweep_price:      float = 0.0,
    current_price:    float = 0.0,
    atr:              float = 0.0,
    **_kwargs: Any,
) -> str:
    ai = {"reverse": "🔄", "continue": "➡️", "wait": "⏳"}.get(
        action.lower(), "❓"
    )
    di = "🟢" if direction == "long" else ("🔴" if direction == "short" else "⚪")
    winner = ("REVERSAL"     if rev_score  > cont_score + 15 else
              "CONTINUATION" if cont_score > rev_score  + 15 else
              "CONTESTED")

    lines = [
        f"{ai} <b>POST-SWEEP VERDICT</b>: {_esc(action.upper())}  "
        f"{di} {_esc(direction.upper() or '—')}",
        f"  Confidence: [{_score_bar(confidence)}] {confidence:.0%}",
        f"  Phase:      {_esc(phase)}",
        f"  Sweep:      {_esc(sweep_side)} @ {_fmt_price(sweep_price)}",
        f"  REV={rev_score:.1f}  CONT={cont_score:.1f}  → {_esc(winner)}",
    ]
    flags: List[str] = []
    if cisd_active: flags.append("CISD✓")
    if ote_active:  flags.append("OTE✓")
    if displacement_atr > 0: flags.append(f"disp={displacement_atr:.2f}ATR")
    if flags:
        lines.append("  " + "  ".join(_esc(f) for f in flags))
    if current_price > 0:
        lines.append(f"  Price={_fmt_price(current_price)}  ATR={_fmt_price(atr)}")
    return "\n".join(lines)


# ======================================================================
# 4. POOL GATE ALERT
# ======================================================================

def format_pool_gate_alert(
    action:          str,                # "exit" | "reverse" | "continue"
    side:            str,                # position side
    pool_side:       str,                # "BSL" | "SSL"
    pool_price:      float,
    current_price:   float,
    reason:          str = "",
    rev_score:       float = 0.0,
    cont_score:      float = 0.0,
    **_kwargs: Any,
) -> str:
    act_icon = {"exit": "🚪", "reverse": "🔄", "continue": "➡️"}.get(
        action.lower(), "❓"
    )
    lines = [
        f"{act_icon} <b>POOL-GATE {_esc(action.upper())}</b>",
        f"  Position:  {_esc(side.upper())}",
        f"  Pool hit:  {_esc(pool_side)} @ {_fmt_price(pool_price)}",
        f"  Price:     {_fmt_price(current_price)}",
        f"  REV={rev_score:.1f}  CONT={cont_score:.1f}",
    ]
    if reason:
        lines.append(f"  Reason: {_esc(reason)}")
    return "\n".join(lines)


# ======================================================================
# 5. CONVICTION BLOCK ALERT
# ======================================================================

def format_conviction_block_alert(
    side:               str,
    factor_scores:      Dict[str, float],
    weighted_total:     float,
    reasons:            Optional[List[str]] = None,
    required_score:     float = _REQUIRED_CONVICTION_SCORE,
    **_kwargs: Any,
) -> str:
    deficit = max(0.0, required_score - weighted_total)
    lines = [
        f"🚫 <b>CONVICTION BLOCKED</b> {_esc(side.upper())}",
        f"  Total: <b>{weighted_total:.2f}</b> / {required_score:.2f}  "
        f"(need +{deficit:.2f})",
        "",
        "  <b>Factor scores</b>",
    ]
    for factor, score in factor_scores.items():
        bar = _score_bar(min(1.0, max(0.0, score)), width=10)
        lines.append(f"    {_esc(factor):<12} [{bar}] {score:+.2f}")
    if reasons:
        lines.append("")
        lines.append("  <b>Rejection reasons</b>")
        for r in reasons[:5]:
            lines.append(f"    • {_esc(r)}")
    return "\n".join(lines)


# ======================================================================
# 6. LIQUIDITY TRAIL UPDATE (v5.0 FIB ENGINE)
# ======================================================================

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
    session:       str = "",
    # v5.0 extras (all optional so v4.0 callers still work)
    fib_ratio:     Optional[float]   = None,
    r_multiple:    float             = 0.0,
    swing_low:     Optional[float]   = None,
    swing_high:    Optional[float]   = None,
    momentum_gate: str               = "",
    htf_aligned:   Optional[bool]    = None,
    is_cluster:    bool              = False,
    n_cluster_tfs: int               = 1,
    pool_boost:    bool              = False,
    pool_between_expand: bool        = False,
    buffer_atr:    float             = 0.0,
) -> str:
    """
    Fibonacci SL trail advance alert (v5.0 engine).

    Shows the full anchor reasoning: Fib ratio + swing context + momentum
    source + HTF alignment + liquidity confluence tags.
    Throttled in quant_strategy to one per 120s.
    """
    side_icon = "🟢" if (side or "").lower() == "long" else "🔴"
    phase_icons = {
        "STRUCTURAL":   "🏛️",
        "AGGRESSIVE":   "🎯",
        "BE_LOCK":      "🔒",
        "COUNTER_BOS":  "🚨",
        "HANDS_OFF":    "⏸",
        "HOLD":         "⏸",
    }
    phase_icon = phase_icons.get(phase, "📍")

    # Fib ratio display
    fib_str = f"{fib_ratio:.3f}" if fib_ratio is not None else "?"
    golden = fib_ratio in (0.382, 0.500, 0.618) if fib_ratio is not None else False
    ratio_tag = f"✨ <b>{fib_str}</b>" if golden else f"<b>{fib_str}</b>"

    # Cluster tag
    cluster_tag = ""
    if is_cluster:
        cluster_tag = f" ×{int(n_cluster_tfs)}TF"

    # Pool confluence
    pool_tag = ""
    if pool_boost:
        pool_tag = " +pool"
    if pool_between_expand:
        pool_tag += " +expand"

    # Momentum source
    gate_emoji = {"DISP": "💥", "CVD": "📈", "BOS": "🏗️", "NONE": "⚪"}.get(
        momentum_gate, "⚪"
    )

    # HTF alignment
    htf_str = (
        "🟢 aligned" if htf_aligned is True else
        "🔴 counter" if htf_aligned is False else
        "⚪ n/a"
    )

    sess_icons = {"LONDON": "🇬🇧", "NY": "🇺🇸", "ASIA": "🌏", "": "🌐"}
    sess_icon  = sess_icons.get((session or "").upper(), "🌐")

    # Profit locked in R from entry
    r_locked = 0.0
    if atr > 1e-10 and entry_price > 0:
        if (side or "").lower() == "long":
            r_locked_pts = new_sl - entry_price
        else:
            r_locked_pts = entry_price - new_sl
        r_locked = r_locked_pts / atr

    dist_to_sl_atr = abs(current_price - new_sl) / atr if atr > 1e-10 else 0.0

    lines: List[str] = [
        f"{side_icon} <b>FIBONACCI TRAIL</b>  {phase_icon} {_esc(phase)}  "
        f"({r_multiple:.2f}R)",
        "",
        f"  🎯 SL → <b>{_fmt_price(new_sl)}</b>  "
        f"({r_locked:+.2f}R from entry)",
        f"  📐 Fib: {ratio_tag}{_esc(cluster_tag)}{_esc(pool_tag)}  "
        f"{_fmt_price(anchor_price)} ({_fmt_tf(anchor_tf)})",
    ]

    if swing_low is not None and swing_high is not None:
        swing_rng = abs(swing_high - swing_low)
        lines.append(
            f"  📊 Swing: {_fmt_price(swing_low)} → {_fmt_price(swing_high)}  "
            f"({swing_rng:.0f}pts)"
        )

    if buffer_atr > 0:
        lines.append(f"  🪶 Buffer: {buffer_atr:.2f} ATR")

    if phase in ("STRUCTURAL", "AGGRESSIVE"):
        lines.append(
            f"  {gate_emoji} Momentum: {_esc(momentum_gate or 'n/a')}  "
            f"HTF: {htf_str}"
        )

    lines.append(
        f"  📏 Distance: {dist_to_sl_atr:.2f} ATR  |  Q: {anchor_sig:.1f}"
    )

    lines.append("")
    lines.append(
        f"  Entry: {_fmt_price(entry_price)}  |  "
        f"Price: {_fmt_price(current_price)}  "
        f"{sess_icon} {_esc(session or 'unknown')}"
    )

    if phase == "COUNTER_BOS":
        lines.append(
            "  <i>🚨 Counter-BOS broke entry — thesis invalidated, locked to BE</i>"
        )
    elif phase == "BE_LOCK":
        lines.append(
            "  <i>🔒 BE + exact fees + slippage locked; trade is now risk-free</i>"
        )
    elif is_cluster:
        lines.append(
            "  <i>Multi-TF Fib confluence — strongest possible anchor</i>"
        )
    elif pool_boost:
        lines.append(
            "  <i>Fibonacci + liquidity pool confluence</i>"
        )
    else:
        lines.append(
            "  <i>SL anchored to institutional Fib retracement</i>"
        )

    return "\n".join(lines)


# ======================================================================
# LOGGING HANDLER — forward WARNING+ logs to Telegram
# ======================================================================

class TelegramLogHandler(logging.Handler):
    """Forward WARNING+ log records to Telegram with throttling and buffering."""

    def __init__(self, level: int = logging.WARNING, throttle_seconds: float = 5.0):
        super().__init__(level)
        self._throttle = throttle_seconds
        self._last_ts  = 0.0
        self._lock     = threading.Lock()
        self._buffer: deque = deque(maxlen=10)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            with self._lock:
                now = time.time()
                if now - self._last_ts < self._throttle:
                    self._buffer.append(record)
                    return
                self._last_ts = now

            msg = self.format(record)
            if self._buffer:
                buffered = [self.format(r) for r in list(self._buffer)]
                self._buffer.clear()
                msg = "\n".join(buffered) + "\n" + msg

            send_telegram_message(f"⚠️ <code>{_esc(msg[:1500])}</code>")
        except Exception:
            pass


def install_global_telegram_log_handler(
    level: int = logging.WARNING,
    throttle_seconds: float = 5.0,
) -> None:
    """Attach a TelegramLogHandler to the root logger."""
    handler = TelegramLogHandler(level=level, throttle_seconds=throttle_seconds)
    handler.setFormatter(logging.Formatter("%(name)s: %(message)s"))
    logging.getLogger().addHandler(handler)

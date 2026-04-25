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
    """Repair UTF-8 text that was accidentally decoded as cp1252."""
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

# ──────────────────────────────────────────────────────────────────────────
# v2.1 QUEUE: tiered priority queue with adaptive shedding
#
# The old design had three problems:
#   1. maxsize=25 saturates in <30s during heartbeat bursts
#      (200 items × 1.2s/msg = 4-min backlog)
#   2. CRITICAL messages were promoted to a separate thread, but everything
#      else went through a single FIFO — so a routine status report would
#      delay an exit notification for a full minute.
#   3. When the queue filled, ALL non-critical messages dropped — including
#      operator command responses to Telegram /position, /trades, etc.
#
# v2.1 design:
#   - PriorityQueue with 3 tiers:
#       0=CRITICAL (errors, exits, exchange events) — never dropped
#       1=IMPORTANT (entries, gate alerts, /command replies) — dropped LAST
#       2=ROUTINE (periodic reports, status, log mirror) — dropped FIRST
#   - maxsize=200 (4-min buffer at 1.2s/msg)
#   - When full, ROUTINE messages are evicted to make room for higher tiers.
#   - CRITICAL still also has a fast-path bypass thread for true emergencies.
# ──────────────────────────────────────────────────────────────────────────

PRIO_CRITICAL  = 0
PRIO_IMPORTANT = 1
PRIO_ROUTINE   = 2

_send_queue: _queue_mod.PriorityQueue = _queue_mod.PriorityQueue(maxsize=200)
_queue_seq: int = 0          # monotonic tiebreaker for PriorityQueue ordering
_queue_seq_lock = threading.Lock()
_worker_started: bool        = False
_worker_lock: threading.Lock = threading.Lock()
_MIN_INTERVAL = 1.2
_MAX_RETRIES  = 4

# Watchdog uses these counters via /watchdog_status and notifier_queue_depth check
_dropped_routine: int   = 0
_dropped_important: int = 0


def _next_seq() -> int:
    global _queue_seq
    with _queue_seq_lock:
        _queue_seq += 1
        return _queue_seq


def _classify_priority(message: str) -> int:
    """Triage by content. Operator-controllable via add_telegram_suppress_pattern."""
    upper = message.upper()
    if any(kw in message for kw in _CRITICAL_KEYWORDS) or "🚨" in message or "💀" in message:
        return PRIO_CRITICAL
    if any(tag in upper for tag in (
        "ENTRY", "EXIT", "POOL-GATE", "TRADE OPEN", "TRADE CLOSED",
        "POSITION ADOPTED", "WATCHDOG HEAL", "WATCHDOG CIRCUIT",
        "POST-EXIT GATE", "IC GATE",
        "POOL GATE", "CONVICTION BLOCK", "FIB TRAIL", "LIQUIDITY DRAW",
        "POST-SWEEP VERDICT",
    )):
        return PRIO_IMPORTANT
    return PRIO_ROUTINE


def _shed_routine_for_room() -> bool:
    """When the queue is full, drop one ROUTINE item to free a slot.
    Returns True if a slot was freed."""
    global _dropped_routine
    # PriorityQueue doesn't expose internals safely; we approximate by
    # iterating its internal heap under its mutex. This is best-effort:
    # we accept that we may not always find a routine to evict.
    try:
        with _send_queue.mutex:  # type: ignore[attr-defined]
            heap = _send_queue.queue  # type: ignore[attr-defined]
            for i, item in enumerate(heap):
                if item[0] >= PRIO_ROUTINE:
                    heap.pop(i)
                    _dropped_routine += 1
                    return True
    except Exception:
        pass
    return False


# Bug #36 fix: critical message keywords that bypass the async queue and
# send synchronously.  This guarantees that UNPROTECTED position alerts,
# crash reports, and killswitch confirmations are never dropped even when
# the queue is full during a burst of routine heartbeat messages.
_CRITICAL_KEYWORDS = frozenset((
    "💀", "🚨",
    "UNPROTECTED", "CRASH", "KILLSWITCH", "CIRCUIT_BREAKER",
    "EMERGENCY", "emergency_flatten", "BOT CRASH",
    "ORPHAN POSITION", "SIDE MISMATCH", "EXIT UNCONFIRMED",
))


def _send_worker() -> None:
    """Background daemon — drains the priority queue and sends to Telegram."""
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

        # PriorityQueue items: (priority, seq, message, parse_mode)
        # Legacy callers may still push (message, parse_mode); handle both.
        if len(item) == 4:
            _prio, _seq, message, parse_mode = item
        elif len(item) == 2:
            message, parse_mode = item
        else:
            logger.error("notifier: unexpected queue item shape: %d", len(item))
            _send_queue.task_done()
            continue
        message = _repair_mojibake(str(message))

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
    message = _repair_mojibake(str(message))
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
        prio = _classify_priority(message)
        item = (prio, _next_seq(), message, parse_mode)
        try:
            _send_queue.put_nowait(item)
            return True
        except _queue_mod.Full:
            # Try shedding a ROUTINE message to make room for higher tiers
            global _dropped_important
            if prio < PRIO_ROUTINE and _shed_routine_for_room():
                try:
                    _send_queue.put_nowait(item)
                    return True
                except _queue_mod.Full:
                    pass
            if prio == PRIO_ROUTINE:
                # Routine — drop silently with a single rate-limited log
                global _dropped_routine
                _dropped_routine += 1
                return False
            _dropped_important += 1
            logger.warning(
                "Telegram queue full — DROPPING priority=%d message (dropped: routine=%d important=%d)",
                prio, _dropped_routine, _dropped_important,
            )
            return False
    except Exception as _qe:
        logger.error("notifier: enqueue failed: %s", _qe)
        return False


def get_queue_stats() -> Dict[str, Any]:
    """Watchdog and /diagnostics introspection."""
    try:
        depth = _send_queue.qsize()
    except Exception:
        depth = -1
    return {
        "depth":            depth,
        "maxsize":          _send_queue.maxsize,
        "dropped_routine":  _dropped_routine,
        "dropped_important": _dropped_important,
    }


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

    def _escape_naked_angles(s: str) -> str:
        """
        v2.1 BUG-FIX: any < or > surviving in a text run is provably NOT a
        well-formed HTML tag (the tokenizer regex already extracted all
        well-formed tags). They must be escaped or Telegram parses them.

        Root-cause example: trail labels emit '(<1.0R)' — the '<' matches
        no tag (the regex requires [A-Za-z] after '<'), so it survives as
        text. Telegram then tries to parse '<1.0r)' as an HTML tag and
        returns 400 'Unsupported start tag'.
        """
        return s.replace("<", "&lt;").replace(">", "&gt;")

    for tok in tokens:
        if tok[0] == "text":
            # First normalise &, then escape any leftover < or > that
            # weren't consumed by the tag tokenizer (provably invalid HTML)
            txt = _normalise_ampersands(tok[1])
            txt = _escape_naked_angles(txt)
            out.append(txt)
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


# ══════════════════════════════════════════════════════════════════════
# FORMAT FUNCTIONS  — clean, signal-first Telegram output
# ══════════════════════════════════════════════════════════════════════

# ─── shared mini-helpers ──────────────────────────────────────────────

def _fp(p):
    return f"${p:,.1f}" if p is not None else "—"

def _fpct(v):
    return f"{v:.2f}%"

def _bar10(score: float) -> str:
    n = max(0, min(10, int(score * 10 + 0.5)))
    return "█" * n + "░" * (10 - n)

def _si(session: str) -> str:
    s = (session or "").upper()
    if "LONDON" in s: return "🇬🇧"
    if "NY"     in s: return "🇺🇸"
    if "ASIA"   in s: return "🌏"
    return "🌐"

def _pd(pd: float) -> str:
    if pd < 0.25: return "DEEP-DISC"
    if pd < 0.40: return "DISCOUNT"
    if pd < 0.60: return "EQUILIB"
    if pd < 0.75: return "PREMIUM"
    return "DEEP-PREM"


# ══════════════════════════════════════════════════════════════════════
# 1. PERIODIC REPORT  (15-min dashboard)
# ══════════════════════════════════════════════════════════════════════

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
    **_kw: Any,
) -> str:
    from datetime import datetime, timezone, timedelta
    ist = timezone(timedelta(hours=5, minutes=30))
    now_ist = datetime.now(ist).strftime("%H:%M IST")
    now_utc = datetime.now(timezone.utc).strftime("%H:%M UTC")

    STATE_ICONS = {
        "SCANNING":"🔍","TRACKING":"📡","READY":"🎯",
        "ENTERING":"⚡","IN_POSITION":"📊","POST_SWEEP":"🌊",
    }
    si        = STATE_ICONS.get((bot_state or "").upper(), "⚪")
    pnl_icon  = "🟢" if daily_pnl >= 0 else "🔴"
    kz_str    = "  🔥 KZ" if in_killzone else ""
    sess_icon = _si(session)
    fc_bar    = _bar10(min(1.0, abs(flow_conviction)))
    fc_arrow  = "▲" if flow_conviction > 0.05 else ("▼" if flow_conviction < -0.05 else "─")

    L = []   # lines

    # ── Header ───────────────────────────────────────────────────────────
    L += [
        f"<b>📊 STATUS  •  {now_ist}  /  {now_utc}</b>",
        f"<code>BTC {_fp(current_price)}   ATR {_fp(atr)}   Bal {_fp(balance)}</code>",
        f"{pnl_icon} Day <b>{_fp(daily_pnl)}</b>   Total {_fp(total_pnl)}",
        "",
    ]

    # ── Engine + session ──────────────────────────────────────────────────
    L.append(
        f"{si} <b>{_esc(bot_state)}</b>"
        f"  {sess_icon} {_esc(session)}{kz_str}"
    )

    # ── Market structure ──────────────────────────────────────────────────
    L += ["", "<b>🏛 Market Structure</b>"]
    L.append(f"  AMD  <b>{_esc(amd_phase)}</b>  ({_esc(amd_bias)})   {_pd(dealing_range_pd)} ({dealing_range_pd:.0%})")
    parts = []
    if structure_4h:  parts.append(f"4H {_esc(structure_4h)}")
    if structure_15m: parts.append(f"15m {_esc(structure_15m)}")
    if htf_bias:      parts.append(f"HTF {_esc(htf_bias)}")
    if parts:
        L.append(f"  {' · '.join(parts)}   Regime {_esc(regime)}")

    # ── Liquidity ─────────────────────────────────────────────────────────
    L += ["", "<b>💧 Liquidity</b>"]
    L.append(f"  BSL ▲ {int(n_bsl_pools)} pools  ·  SSL ▼ {int(n_ssl_pools)} pools")
    if nearest_bsl:
        L.append(
            f"  ▲ {_fp(nearest_bsl.get('price',0))}"
            f"  {nearest_bsl.get('dist_atr',0):.1f}ATR"
            f"  sig={nearest_bsl.get('significance',0):.0f}"
            f"  {_esc(nearest_bsl.get('timeframe',''))}"
        )
    if nearest_ssl:
        L.append(
            f"  ▼ {_fp(nearest_ssl.get('price',0))}"
            f"  {nearest_ssl.get('dist_atr',0):.1f}ATR"
            f"  sig={nearest_ssl.get('significance',0):.0f}"
            f"  {_esc(nearest_ssl.get('timeframe',''))}"
        )
    if primary_target_str and primary_target_str != "—":
        L.append(f"  🎯 {_esc(primary_target_str)}")

    # ── Flow ─────────────────────────────────────────────────────────────
    L += ["", "<b>⚡ Order Flow</b>"]
    L.append(
        f"  {_esc((flow_direction or 'neutral').upper())}  [{_esc(fc_bar)}] {fc_arrow}  {flow_conviction:+.2f}"
    )

    # ── Sweep analysis ────────────────────────────────────────────────────
    if sweep_analysis:
        rs  = float(sweep_analysis.get("reversal_score",   sweep_analysis.get("rev_score",  0)))
        cs  = float(sweep_analysis.get("continuation_score",sweep_analysis.get("cont_score", 0)))
        rr  = sweep_analysis.get("reversal_reasons")   or sweep_analysis.get("rev_reasons")  or []
        cr  = sweep_analysis.get("continuation_reasons")or sweep_analysis.get("cont_reasons") or []
        sw  = sweep_analysis.get("sweep_side","?")
        spx = sweep_analysis.get("sweep_price",0)
        spq = sweep_analysis.get("sweep_quality",0)
        winner = ("REVERSAL" if rs>cs+15 else "CONTINUATION" if cs>rs+15 else "UNDECIDED")
        L += ["", f"<b>🌊 Sweep  {_esc(sw)} @ {_fp(spx)}  q={spq:.0%}</b>"]
        L.append(f"  REV {rs:.0f}  CONT {cs:.0f}  → <b>{_esc(winner)}</b>")
        if rr: L.append(f"  Rev:  {_esc(', '.join(str(x) for x in rr[:3]))}")
        if cr: L.append(f"  Cont: {_esc(', '.join(str(x) for x in cr[:3]))}")

    # ── Hunt prediction ───────────────────────────────────────────────────
    if direction_hunt is not None:
        pred   = getattr(direction_hunt,"predicted",None)
        conf   = float(getattr(direction_hunt,"confidence",0.0))
        deliv  = getattr(direction_hunt,"delivery_direction","")
        bsl_s  = float(getattr(direction_hunt,"bsl_score",0.0))
        ssl_s  = float(getattr(direction_hunt,"ssl_score",0.0))
        hi     = "🔵" if pred=="BSL" else ("🟠" if pred=="SSL" else "⚪")
        di     = "🟢" if deliv=="bullish" else ("🔴" if deliv=="bearish" else "⚪")
        L += ["", f"{hi} <b>Hunt  {_esc(pred or 'NEUTRAL')}</b>"]
        L.append(f"  [{_esc(_bar10(conf))}] {conf:.0%}  {di} {_esc(deliv or '—')}")
        L.append(f"  BSL {bsl_s:.3f}  ·  SSL {ssl_s:.3f}")

    # ── Post-sweep verdict ────────────────────────────────────────────────
    if direction_ps_analysis is not None:
        pa     = getattr(direction_ps_analysis,"action","?")
        pd_dir = getattr(direction_ps_analysis,"direction","")
        pc     = float(getattr(direction_ps_analysis,"confidence",0.0))
        pr     = float(getattr(direction_ps_analysis,"rev_score",0.0))
        pc2    = float(getattr(direction_ps_analysis,"cont_score",0.0))
        phase  = getattr(direction_ps_analysis,"phase","")
        ai     = {"reverse":"🔄","continue":"➡️","wait":"⏳"}.get(pa.lower(),"❓")
        di2    = "🟢" if pd_dir=="long" else ("🔴" if pd_dir=="short" else "⚪")
        win    = ("REVERSAL" if pr>pc2+15 else "CONTINUATION" if pc2>pr+15 else "CONTESTED")
        L += ["", f"{ai} <b>Post-Sweep  {_esc(pa.upper())}</b>  {di2} {_esc((pd_dir or '—').upper())}"]
        L.append(f"  REV {pr:.1f}  CONT {pc2:.1f}  → {_esc(win)}  conf {pc:.0%}  phase {_esc(phase)}")

    # ── Gate status ───────────────────────────────────────────────────────
    L += ["", "<b>🚦 Entry Gates</b>"]
    def _gate(ok: bool, label: str) -> str:
        return f"  {'✅' if ok else '⚪'} {label}"

    s = (session or "").upper()
    L.append(_gate(s in ("LONDON","NY","LONDON_NY"), f"Session  {_esc(s or 'off')}"))

    pred_  = getattr(direction_hunt,"predicted",None) if direction_hunt else None
    conf_  = float(getattr(direction_hunt,"confidence",0.0)) if direction_hunt else 0.0
    L.append(_gate(pred_ in ("BSL","SSL") and conf_ >= _HUNT_ON_THRESHOLD,
                   f"Hunt  {_esc(pred_ or 'NEUTRAL')} ({conf_:.0%})"))

    L.append(_gate(abs(flow_conviction) >= 0.20 and flow_direction in ("long","short"),
                   f"Flow  {_esc(flow_direction or 'neutral')} ({flow_conviction:+.2f})"))

    amd_ok = "MANIPULATION" in (amd_phase or "").upper() or "DISTRIBUTION" in (amd_phase or "").upper()
    L.append(_gate(amd_ok, f"AMD  {_esc(amd_phase or 'UNKNOWN')}"))

    pd_ok = dealing_range_pd < 0.40 or dealing_range_pd > 0.60
    L.append(_gate(pd_ok, f"P/D  {_pd(dealing_range_pd)} ({dealing_range_pd:.0%})"))
    L.append(_gate(bool(htf_bias) and htf_bias.lower() != "mixed", f"HTF  {_esc(htf_bias or 'mixed')}"))

    # ── Active position ───────────────────────────────────────────────────
    if position:
        side    = (position.get("side") or "?").upper()
        p_entry = entry_price or position.get("entry_price", 0)
        qty     = float(position.get("quantity", 0) or 0)
        icon    = "🟢" if side == "LONG" else "🔴"

        L += ["", f"<b>{icon} Position  {_esc(side)}</b>"]
        L.append(f"  Entry {_fp(p_entry)}")
        if current_sl:
            sl_atr = abs(current_price - current_sl) / max(atr, 1) if atr else 0
            L.append(f"  SL    {_fp(current_sl)}  ({sl_atr:.1f}ATR)")
        if current_tp:
            tp_atr = abs(current_tp - current_price) / max(atr, 1) if atr else 0
            L.append(f"  TP    {_fp(current_tp)}  ({tp_atr:.1f}ATR)")

        if p_entry and current_price:
            move   = (current_price - p_entry) if side == "LONG" else (p_entry - current_price)
            risk_d = abs(p_entry - current_sl) if current_sl else 0
            ur_r   = move / risk_d if risk_d else 0
            upnl   = move * qty if qty else move
            total  = abs(current_tp - p_entry) if current_tp else 0
            prog   = min(1.0, max(0.0, abs(current_price - p_entry) / total)) if total and move >= 0 else 0.0
            mi     = "🟢" if move >= 0 else "🔴"
            pnl_str = f"${upnl:+.2f}" if qty else f"{move:+.1f}pts"
            L.append(f"  {mi} <b>{pnl_str}</b>  ({ur_r:+.2f}R)")
            L.append(f"  [{_esc(_bar10(prog) * 2)}] {prog*100:.0f}% → TP")

        if breakeven_moved:
            L.append(f"  🔒 BE locked  ·  {profit_locked_pct:.1f}R secured")

    # ── Performance ───────────────────────────────────────────────────────
    L += ["", "<b>📈 Performance</b>"]
    L.append(f"  Trades {int(total_trades)}  ·  WR {win_rate:.1f}%")
    if consecutive_losses:
        L.append(f"  ⚠️ Consecutive losses  {int(consecutive_losses)}")

    if extra_lines:
        L.append("")
        L.extend(el for el in extra_lines if el and el.strip())

    return "\n".join(L)


# ══════════════════════════════════════════════════════════════════════
# 2. HUNT PREDICTION
# ══════════════════════════════════════════════════════════════════════

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
    hi = "🔵" if predicted == "BSL" else ("🟠" if predicted == "SSL" else "⚪")
    di = "🟢" if delivery_direction == "bullish" else ("🔴" if delivery_direction == "bearish" else "⚪")

    L = [
        f"{hi} <b>Hunt Prediction  •  {_esc(predicted or 'NEUTRAL')}</b>",
        f"  [{_esc(_bar10(confidence))}] {confidence:.0%}  {di} {_esc(delivery_direction or '—')}",
        f"  BSL {bsl_score:+.3f}  ·  SSL {ssl_score:+.3f}  ·  raw {raw_score:+.3f}",
    ]
    if current_price:
        L.append(f"  Price {_fp(current_price)}   ATR {_fp(atr) if atr else '—'}")
    if nearest_bsl:
        L.append(f"  ▲ BSL {_fp(nearest_bsl.get('price',0))}  ({nearest_bsl.get('dist_atr',0):.1f}ATR)")
    if nearest_ssl:
        L.append(f"  ▼ SSL {_fp(nearest_ssl.get('price',0))}  ({nearest_ssl.get('dist_atr',0):.1f}ATR)")
    return "\n".join(L)


# ══════════════════════════════════════════════════════════════════════
# 3. POST-SWEEP VERDICT
# ══════════════════════════════════════════════════════════════════════

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
    **_kw: Any,
) -> str:
    ai = {"reverse":"🔄","continue":"➡️","wait":"⏳"}.get(action.lower(),"❓")
    di = "🟢" if direction == "long" else ("🔴" if direction == "short" else "⚪")
    winner = ("REVERSAL" if rev_score>cont_score+15 else
              "CONTINUATION" if cont_score>rev_score+15 else "CONTESTED")

    flags = []
    if cisd_active:         flags.append("CISD ✓")
    if ote_active:          flags.append("OTE ✓")
    if displacement_atr>0:  flags.append(f"disp {displacement_atr:.2f}ATR")

    L = [
        f"{ai} <b>Post-Sweep  {_esc(action.upper())}</b>  {di} {_esc((direction or '—').upper())}",
        f"  [{_esc(_bar10(confidence))}] {confidence:.0%}  phase {_esc(phase)}",
        f"  Sweep  {_esc(sweep_side)} @ {_fp(sweep_price)}",
        f"  REV {rev_score:.1f}  CONT {cont_score:.1f}  → <b>{_esc(winner)}</b>",
    ]
    if flags:
        L.append("  " + "  ·  ".join(_esc(f) for f in flags))
    if current_price:
        L.append(f"  Price {_fp(current_price)}  ATR {_fp(atr)}")
    return "\n".join(L)


# ══════════════════════════════════════════════════════════════════════
# 4. POOL GATE ALERT
# ══════════════════════════════════════════════════════════════════════

def format_pool_gate_alert(
    action:        str,
    side:          str,
    pool_side:     str,
    pool_price:    float,
    current_price: float,
    reason:        str   = "",
    rev_score:     float = 0.0,
    cont_score:    float = 0.0,
    **_kw: Any,
) -> str:
    ai = {"exit":"🚪","reverse":"🔄","continue":"➡️"}.get(action.lower(),"❓")
    L = [
        f"{ai} <b>Pool Gate  {_esc(action.upper())}</b>",
        f"  Position {_esc(side.upper())}  ·  Pool {_esc(pool_side)} @ {_fp(pool_price)}",
        f"  Price {_fp(current_price)}  ·  REV {rev_score:.1f}  CONT {cont_score:.1f}",
    ]
    if reason:
        L.append(f"  {_esc(reason)}")
    return "\n".join(L)


# ══════════════════════════════════════════════════════════════════════
# 5. CONVICTION BLOCK ALERT
# ══════════════════════════════════════════════════════════════════════

def format_conviction_block_alert(
    side:           str,
    factor_scores:  Dict[str, float],
    weighted_total: float,
    reasons:        Optional[List[str]] = None,
    required_score: float = _REQUIRED_CONVICTION_SCORE,
    **_kw: Any,
) -> str:
    deficit = max(0.0, required_score - weighted_total)
    total_bar = _bar10(min(1.0, weighted_total / max(required_score, 0.01)))

    L = [
        f"🚫 <b>Conviction Blocked  •  {_esc(side.upper())}</b>",
        f"  Score  <b>{weighted_total:.2f}</b> / {required_score:.2f}"
        f"  [{_esc(total_bar)}]  need +{deficit:.2f}",
        "",
        "  <b>Factors</b>",
    ]
    for factor, score in factor_scores.items():
        L.append(f"  <code>{_esc(factor):<14}</code> [{_esc(_bar10(min(1.0,max(0.0,score))))}] {score:+.2f}")

    if reasons:
        L += ["", "  <b>Reasons</b>"]
        for r in reasons[:5]:
            L.append(f"  · {_esc(r)}")
    return "\n".join(L)


# ══════════════════════════════════════════════════════════════════════
# 6. LIQUIDITY TRAIL UPDATE  (Fibonacci engine v5.0)
# ══════════════════════════════════════════════════════════════════════

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
    session:       str           = "",
    fib_ratio:     Optional[float] = None,
    r_multiple:    float           = 0.0,
    swing_low:     Optional[float] = None,
    swing_high:    Optional[float] = None,
    momentum_gate: str             = "",
    htf_aligned:   Optional[bool]  = None,
    is_cluster:    bool            = False,
    n_cluster_tfs: int             = 1,
    pool_boost:    bool            = False,
    pool_between_expand: bool      = False,
    buffer_atr:    float           = 0.0,
) -> str:
    si       = "🟢" if (side or "").lower() == "long" else "🔴"
    PHASE_I  = {"STRUCTURAL":"🏛️","AGGRESSIVE":"🎯","BE_LOCK":"🔒",
                "COUNTER_BOS":"🚨","HANDS_OFF":"⏸","HOLD":"⏸"}
    phase_i  = PHASE_I.get(phase, "📍")
    sess_i   = _si(session)

    fib_str  = f"{fib_ratio:.3f}" if fib_ratio is not None else "?"
    golden   = fib_ratio in (0.382, 0.500, 0.618) if fib_ratio is not None else False
    fib_tag  = f"✨ <b>{fib_str}</b>" if golden else f"<b>{fib_str}</b>"

    tags = []
    if is_cluster:            tags.append(f"×{n_cluster_tfs}TF")
    if pool_boost:            tags.append("+pool")
    if pool_between_expand:   tags.append("+expand")
    tag_str = "  " + "  ".join(_esc(t) for t in tags) if tags else ""

    gate_i = {"DISP":"💥","CVD":"📈","BOS":"🏗️","NONE":"⚪"}.get(momentum_gate,"⚪")
    htf_s  = ("🟢 aligned" if htf_aligned is True else
               "🔴 counter" if htf_aligned is False else "⚪ n/a")

    r_locked = 0.0
    if atr > 1e-10 and entry_price:
        pts = (new_sl - entry_price) if (side or "").lower() == "long" else (entry_price - new_sl)
        r_locked = pts / atr
    dist_atr = abs(current_price - new_sl) / atr if atr > 1e-10 else 0.0

    L = [
        f"{si} <b>Fibonacci Trail</b>  {phase_i} {_esc(phase)}  {r_multiple:.2f}R",
        "",
        f"  SL → <b>{_fp(new_sl)}</b>  ({r_locked:+.2f}R from entry)",
        f"  Fib  {fib_tag}{tag_str}   anchor {_fp(anchor_price)} ({_esc(anchor_tf)})",
    ]

    if swing_low is not None and swing_high is not None:
        rng = abs(swing_high - swing_low)
        L.append(f"  Swing  {_fp(swing_low)} → {_fp(swing_high)}  ({rng:.0f} pts)")

    if buffer_atr:
        L.append(f"  Buffer  {buffer_atr:.2f} ATR")

    if phase in ("STRUCTURAL","AGGRESSIVE"):
        L.append(f"  {gate_i} Momentum {_esc(momentum_gate or 'n/a')}  ·  HTF {htf_s}")

    L.append(f"  Distance {dist_atr:.2f}ATR  ·  Q {anchor_sig:.1f}")
    L += [
        "",
        f"  Entry {_fp(entry_price)}  ·  Price {_fp(current_price)}  {sess_i} {_esc(session or 'unknown')}",
    ]

    notes = {
        "COUNTER_BOS": "<i>🚨 Counter-BOS broke entry — thesis invalidated, locked to BE</i>",
        "BE_LOCK":     "<i>🔒 Risk-free — BE + fees + slippage locked</i>",
    }
    if phase in notes:
        L.append("  " + notes[phase])
    elif is_cluster:
        L.append("  <i>Multi-TF Fib confluence — strongest anchor</i>")
    elif pool_boost:
        L.append("  <i>Fibonacci + liquidity pool confluence</i>")
    else:
        L.append("  <i>SL anchored to institutional Fib retracement</i>")

    return "\n".join(L)

# ======================================================================
# LOGGING HANDLER — forward WARNING+ logs to Telegram
# ======================================================================

# ──────────────────────────────────────────────────────────────────────
# Telegram suppression patterns
#
# Certain WARNING-level log records are routine/diagnostic noise that
# should never page the user on Telegram (but should still appear in
# the local quant_bot.log file). Any record whose formatted message
# contains ANY of these substrings is dropped by TelegramLogHandler
# before the send.
#
# Matching is substring-against-the-formatted-message (case-sensitive
# to avoid accidental over-match). Formatter is
# "%(name)s: %(message)s" so logger-name prefixes are also searchable
# (e.g. "exchanges.delta.data_manager:").
#
# Maintained here so new noisy warnings can be muted centrally without
# editing every call site. Extend via `add_telegram_suppress_pattern`.
# ──────────────────────────────────────────────────────────────────────
_TELEGRAM_SUPPRESS_PATTERNS: List[str] = [
    # Delta data-manager routine self-heal (main cause of historic spam —
    # now also downgraded to INFO at source, but kept here as
    # belt-and-braces in case another path logs these at WARNING).
    "Delta REST refresh ",
    "candles stale age=",
    "starting REST self-heal",
    # Watchdog daily-counter consistency check — a known false-positive
    # comparison (gate counts ENTRIES; risk_manager counts COMPLETED
    # trades, or may not even track the same field). Fires every 5 min
    # while a position is open. Diagnostic only, no auto-heal path.
    "daily_counter_consistency",
    "daily counter drift",
]
_TELEGRAM_SUPPRESS_LOCK = threading.Lock()


def add_telegram_suppress_pattern(pattern: str) -> None:
    """Register an additional substring pattern to suppress from Telegram."""
    if not pattern:
        return
    with _TELEGRAM_SUPPRESS_LOCK:
        if pattern not in _TELEGRAM_SUPPRESS_PATTERNS:
            _TELEGRAM_SUPPRESS_PATTERNS.append(pattern)


def clear_telegram_suppress_patterns() -> None:
    """Remove all suppression patterns (primarily for tests)."""
    with _TELEGRAM_SUPPRESS_LOCK:
        _TELEGRAM_SUPPRESS_PATTERNS.clear()


def _is_suppressed_for_telegram(formatted_msg: str) -> bool:
    if not formatted_msg:
        return False
    with _TELEGRAM_SUPPRESS_LOCK:
        patterns = tuple(_TELEGRAM_SUPPRESS_PATTERNS)
    for pat in patterns:
        if pat and pat in formatted_msg:
            return True
    return False


class TelegramLogHandler(logging.Handler):
    """Forward WARNING+ log records to Telegram with throttling and buffering.

    Suppression: records whose formatted message matches any substring in
    `_TELEGRAM_SUPPRESS_PATTERNS` are dropped silently and do NOT consume
    the throttle/buffer slots. This prevents a recurring noisy WARNING
    from crowding out a genuinely important one that happens to arrive
    during the same throttle window.
    """

    def __init__(self, level: int = logging.WARNING, throttle_seconds: float = 5.0):
        super().__init__(level)
        self._throttle = throttle_seconds
        self._last_ts  = 0.0
        self._lock     = threading.Lock()
        self._buffer: deque = deque(maxlen=10)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            # Format once, so suppression and send use the same text.
            msg = self.format(record)

            # Early exit for suppressed patterns — don't advance throttle
            # state, don't occupy a buffer slot.
            if _is_suppressed_for_telegram(msg):
                return

            with self._lock:
                now = time.time()
                if now - self._last_ts < self._throttle:
                    self._buffer.append(record)
                    return
                self._last_ts = now

            if self._buffer:
                buffered_records = list(self._buffer)
                self._buffer.clear()
                # Filter buffered records through suppression too — a pattern
                # may have been added since they were buffered.
                buffered_msgs = [
                    self.format(r) for r in buffered_records
                    if not _is_suppressed_for_telegram(self.format(r))
                ]
                if buffered_msgs:
                    msg = "\n".join(buffered_msgs) + "\n" + msg

            send_telegram_message(format_log_alert(record.levelname, record.name, msg))
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


# ══════════════════════════════════════════════════════════════════════════
# v9.1 — INDUSTRY-GRADE TELEGRAM TEMPLATES
# ══════════════════════════════════════════════════════════════════════════
# These templates are used by quant_strategy.py and the controller for the
# user-facing high-frequency notifications (entry, exit, trail, gate veto).
#
# Design rules:
#   1. Lead with the fact (side / price / outcome) — user must know the
#      WHAT in the first line.
#   2. Numerics in <code> blocks for monospace alignment in Telegram.
#   3. No decorative borders (Telegram's renderer handles spacing).
#   4. <b>...</b> reserved for the SINGLE most important value per block.
#   5. All dynamic strings go through _esc() — never f-string raw.
#
# Public:
#   format_entry_alert(...)
#   format_exit_alert(...)
#   format_trail_advance(...)
#   format_gate_block_alert(...)
#   format_status_card(...)              — replaces verbose periodic_report
# ══════════════════════════════════════════════════════════════════════════


def _arrow(side: str) -> str:
    s = (side or "").lower()
    return "▲" if s == "long" else ("▼" if s == "short" else "•")


def _badge_pnl(pnl: float) -> str:
    if pnl > 0:  return "🟢"
    if pnl < 0:  return "🔴"
    return "⚪"


def _fpts(v: float) -> str:
    sign = "+" if v >= 0 else "−"
    return f"{sign}{abs(v):,.1f} pts"


def _fpnl(v: float) -> str:
    sign = "+" if v >= 0 else "−"
    return f"{sign}${abs(v):,.2f}"


def format_entry_alert(
    side:        str,
    entry:       float,
    sl:          float,
    tp:          float,
    qty:         float,
    mode:        str,
    tier:        str,
    sl_atr:      float,
    tp_atr:      float,
    rr:          float,
    reason:      str = "",
    session:     str = "",
    flow_conv:   float = 0.0,
) -> str:
    """
    Industry-grade entry alert. Single screen, monospace columns.

    ▲ LONG  $77,420.00   SWEEP_REVERSAL · S
    ─────────────────────────
    SL  $77,300.00  (0.8 ATR)
    TP  $77,800.00  (3.0 ATR)   R:R 1:3.16
    QTY 0.0250                  flow ▲ +0.42
    SSL sweep + bullish FVG + flow alignment
    """
    side_u = side.upper()
    head_emoji = "🟢" if side_u == "LONG" else "🔴"
    arr = _arrow(side)

    flow_glyph = "▲" if flow_conv > 0.05 else ("▼" if flow_conv < -0.05 else "·")

    rows = [
        f"{head_emoji} <b>{_esc(arr)} {_esc(side_u)}</b>  "
        f"<code>{_fmt_price(entry)}</code>"
        f"   {_esc(mode.upper())} · {_esc(tier or '-')}",
        "<code>──────────────────────────────</code>",
        f"<code>SL  {_fmt_price(sl):<14}</code>  ({sl_atr:.1f} ATR)",
        f"<code>TP  {_fmt_price(tp):<14}</code>  ({tp_atr:.1f} ATR)   R:R 1:{rr:.2f}",
        f"<code>QTY {qty:<14.4f}</code>  flow {_esc(flow_glyph)} {flow_conv:+.2f}",
    ]
    if session:
        rows.append(f"<code>SESS {_esc(session.upper())}</code>")
    if reason:
        rows.append(f"<i>{_esc(reason[:160])}</i>")
    return "\n".join(rows)


def format_exit_alert(
    side:        str,
    entry:       float,
    exit_price:  float,
    pnl:         float,
    r_realised:  float,
    mfe_r:       float,
    reason:      str,
    hold_min:    float,
    fees:        float = 0.0,
    qty:         float = 0.0,
) -> str:
    """
    Industry-grade exit alert. Outcome icon leads.

    ✅ EXIT LONG  $77,800.00   tp_hit
    ─────────────────────────
    PNL  +$10.04   (+4.00R)   MFE 4.20R
    HOLD 23m   FEE $0.014
    77,400.00 → 77,800.00   400.0 pts
    """
    side_u = side.upper()
    win = pnl > 0
    head_icon = "✅" if win else "❌"
    reason_label = {
        "tp_hit":       "TP HIT",
        "sl_hit":       "SL HIT",
        "trail_sl_hit": "TRAIL EXIT",
    }.get(reason, (reason or "exit").upper())

    pts_realised = (exit_price - entry) if side_u == "LONG" else (entry - exit_price)

    rows = [
        f"{head_icon} <b>EXIT {_esc(side_u)}</b>  "
        f"<code>{_fmt_price(exit_price)}</code>"
        f"   {_esc(reason_label)}",
        "<code>──────────────────────────────</code>",
        (f"<b>PNL {_fpnl(pnl)}</b>"
         f"   <code>{r_realised:+.2f}R</code>   MFE {mfe_r:.2f}R"),
        (f"<code>HOLD {hold_min:5.0f}m   FEE ${fees:.4f}</code>"
         + (f"   QTY {qty:.4f}" if qty > 0 else "")),
        f"<code>{_fmt_price(entry)} → {_fmt_price(exit_price)}</code>"
        f"   <i>{_fpts(pts_realised)}</i>",
    ]
    return "\n".join(rows)


def format_trail_advance(
    side:          str,
    new_sl:        float,
    old_sl:        float,
    phase:         str,
    r_locked:      float,
    r_multiple:    float,
    entry:         float,
    current_price: float,
    atr:           float,
    anchor_tf:     str = "",
    anchor_price:  float = 0.0,
    fib_ratio:     Optional[float] = None,
) -> str:
    """
    Industry-grade trail advance alert. Compact — fires often, must scan
    fast.

        🔒 TRAIL ↑ LONG   $77,540.00   PHASE_1_BE_FLOOR (1.20R)
        ─────────────────────────
        SL    $77,520.00 → $77,540.00   locked +0.20R
        FIB   0.500   anchor 15m @ $77,420.00
    """
    side_u = side.upper()
    arrow = "↑" if side_u == "LONG" else "↓"
    phase_disp = phase.upper() if phase else "TRAIL"
    rows = [
        f"🔒 <b>TRAIL {_esc(arrow)} {_esc(side_u)}</b>"
        f"   <code>{_fmt_price(new_sl)}</code>"
        f"   <i>{_esc(phase_disp)}</i>  ({r_multiple:.2f}R)",
        "<code>──────────────────────────────</code>",
        (f"<code>SL    {_fmt_price(old_sl)} → {_fmt_price(new_sl)}</code>"
         f"   locked <b>{r_locked:+.2f}R</b>"),
    ]
    if fib_ratio is not None and anchor_price > 0:
        rows.append(
            f"<code>FIB   {fib_ratio:.3f}</code>"
            f"   anchor {_esc(anchor_tf or '?')} @ <code>{_fmt_price(anchor_price)}</code>"
        )
    elif anchor_price > 0:
        rows.append(
            f"<code>ANCHOR {_esc(anchor_tf or '?')} @ {_fmt_price(anchor_price)}</code>"
        )
    return "\n".join(rows)


def format_gate_block_alert(
    side:        str,
    lens:        str,
    detail:      str,
    retry_in:    float,
    consec_loss: int = 0,
) -> str:
    """
    Post-Exit Gate veto. Throttled by quant_strategy (30s/lens) so this
    fires at most once per minute per lens.

        🚫 POST-EXIT GATE
        Side    SHORT
        Lens    SIDE_FLIP_DISTANCE
        Why     only 0.42 ATR from SL — need ≥1.5 ATR
        Retry   in 12s   (1L streak)
    """
    streak = f"   <i>({consec_loss}L streak)</i>" if consec_loss > 0 else ""
    return (
        "🚫 <b>POST-EXIT GATE</b>\n"
        "<code>──────────────────────────────</code>\n"
        f"<code>SIDE   {_esc(side.upper())}</code>\n"
        f"<code>LENS   {_esc(lens)}</code>\n"
        f"<i>{_esc(detail[:200])}</i>\n"
        f"<code>RETRY  in {retry_in:.0f}s</code>{streak}"
    )


def format_status_card(
    price:          float,
    atr:            float,
    balance:        float,
    daily_pnl:      float,
    total_pnl:      float,
    total_trades:   int,
    win_rate:       float,
    consec_loss:    int,
    state:          str,
    session:        str,
    in_killzone:    bool,
    amd_phase:      str,
    amd_bias:       str,
    flow_conv:      float,
    flow_dir:       str,
    structure_15m:  str = "",
    structure_4h:   str = "",
    nearest_bsl:    Optional[Dict] = None,
    nearest_ssl:    Optional[Dict] = None,
    primary_target: str = "",
    position:       Optional[Dict] = None,
) -> str:
    """
    Industry-grade replacement for the verbose periodic_report. Single-
    screen card, monospace columns, no nested sections.

        📊 STATUS  12:18 IST          NY KZ
        ─────────────────────────
        BTC      $77,520.50   ATR 125.7
        BAL      $1,000.00
        PNL DAY  +$15.50      TOTAL +$420.10
        ─────────────────────────
        STATE    SCANNING       4 trades · WR 50%
        AMD      DISTRIBUTION/bearish
        STRUCT   15m=range  4h=down
        FLOW     ▲ +0.42       BSL 1.4 ATR / SSL 3.2 ATR
        TARGET   $77,800 (BSL ▲)
    """
    from datetime import datetime, timezone, timedelta
    ist = timezone(timedelta(hours=5, minutes=30))
    now_ist = datetime.now(ist).strftime("%H:%M IST")
    kz_tag = "  🔥 KZ" if in_killzone else ""

    flow_glyph = "▲" if flow_conv > 0.05 else ("▼" if flow_conv < -0.05 else "·")
    pnl_emoji = _badge_pnl(daily_pnl)
    rule = "<code>──────────────────────────────</code>"

    rows = [
        f"📊 <b>STATUS</b>  {_esc(now_ist)}   <i>{_esc((session or '').upper())}</i>{kz_tag}",
        rule,
        f"<code>BTC      {_fmt_price(price):<14} ATR {atr:6.1f}</code>",
        f"<code>BAL      {_fmt_price(balance)}</code>",
        (f"{pnl_emoji} <code>PNL DAY  {_fpnl(daily_pnl):<14} TOTAL {_fpnl(total_pnl)}</code>"),
        rule,
        (f"<code>STATE    {_esc((state or 'SCANNING').upper()):<14}</code>"
         f"   {total_trades} trades · WR {win_rate:.0f}%"
         + (f" · {consec_loss}L" if consec_loss else "")),
        f"<code>AMD      {_esc(amd_phase or '—'):<14} {_esc(amd_bias or '—')}</code>",
    ]
    if structure_15m or structure_4h:
        rows.append(
            f"<code>STRUCT   15m={_esc(structure_15m or '—'):<6} 4h={_esc(structure_4h or '—')}</code>"
        )
    nbsl = (f"BSL {nearest_bsl.get('dist_atr', 0):.1f}A" if nearest_bsl else "BSL —")
    nssl = (f"SSL {nearest_ssl.get('dist_atr', 0):.1f}A" if nearest_ssl else "SSL —")
    rows.append(
        f"<code>FLOW     {_esc(flow_glyph)} {flow_conv:+.2f} {(flow_dir or 'neutral')[:6]:<7}"
        f"</code>   {nbsl} / {nssl}"
    )
    if primary_target and primary_target != "—":
        rows.append(f"<code>TARGET   {_esc(primary_target[:50])}</code>")

    if position:
        side = (position.get("side") or "?").upper()
        side_emoji = "🟢" if side == "LONG" else "🔴"
        entry = float(position.get("entry_price") or 0)
        sl    = float(position.get("sl_price") or 0)
        tp    = float(position.get("tp_price") or 0)
        qty   = float(position.get("quantity") or 0)
        if entry > 0 and side in ("LONG", "SHORT"):
            move = (price - entry) if side == "LONG" else (entry - price)
            init_sl = float(position.get("initial_sl_dist") or abs(entry - sl) or 1)
            r = move / init_sl if init_sl else 0
            upnl = move * qty
            rows.append(rule)
            rows.append(
                f"{side_emoji} <b>{_esc(_arrow(side))} {_esc(side)}</b>"
                f"   <code>ENTRY {_fmt_price(entry)}</code>"
            )
            rows.append(
                f"<code>SL    {_fmt_price(sl):<14} TP    {_fmt_price(tp)}</code>"
            )
            rows.append(
                f"<code>UPNL  {_fpnl(upnl):<14} R     {r:+.2f}</code>"
            )
    return "\n".join(rows)


# ============================================================================
# v3 operator-grade Telegram cards
# ============================================================================

_TG_RULE = "<code>" + ("\u2500" * 34) + "</code>"


def _tg_price(value: Any, digits: int = 2) -> str:
    try:
        if value is None:
            return "-"
        return f"${float(value):,.{digits}f}"
    except Exception:
        return "-"


def _tg_num(value: Any, digits: int = 2) -> str:
    try:
        return f"{float(value):,.{digits}f}"
    except Exception:
        return "-"


def _tg_pnl(value: Any) -> str:
    try:
        v = float(value)
        sign = "+" if v >= 0 else "-"
        return f"{sign}${abs(v):,.2f}"
    except Exception:
        return "$0.00"


def _tg_bar(value: float, width: int = 12) -> str:
    try:
        ratio = min(1.0, max(0.0, abs(float(value))))
    except Exception:
        ratio = 0.0
    filled = int(ratio * width + 0.5)
    return "\u2588" * filled + "\u2591" * (width - filled)


def _tg_arrow(value: float = 0.0, side: str = "") -> str:
    s = (side or "").lower()
    if s in ("long", "bullish", "bsl"):
        return "\u25b2"
    if s in ("short", "bearish", "ssl"):
        return "\u25bc"
    try:
        v = float(value)
        if v > 0.05:
            return "\u25b2"
        if v < -0.05:
            return "\u25bc"
    except Exception:
        pass
    return "\u2022"


def _tg_get(obj: Any, key: str, default: Any = None) -> Any:
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _tg_pool_line(label: str, pool: Any) -> Optional[str]:
    if not pool:
        return None
    price = _tg_get(pool, "price", 0.0)
    dist = _tg_get(pool, "dist_atr", _tg_get(pool, "distance_atr", 0.0))
    sig = _tg_get(pool, "significance", _tg_get(pool, "sig", 0.0))
    tf = _tg_get(pool, "timeframe", _tg_get(pool, "tf", ""))
    touches = _tg_get(pool, "touches", "")
    extra = f" t={_esc(touches)}" if touches not in ("", None) else ""
    return (
        f"<code>{_esc(label):<4} {_tg_price(price):>13}  "
        f"{float(dist or 0):>4.1f}A  sig {float(sig or 0):>5.1f}  "
        f"{_esc(tf or '-'):>4}{extra}</code>"
    )


def _tg_limit(lines: List[str], max_chars: int = 3900) -> str:
    out: List[str] = []
    size = 0
    for line in lines:
        add = len(line) + 1
        if size + add > max_chars:
            out.append("<i>...trimmed for Telegram limit</i>")
            break
        out.append(line)
        size += add
    return "\n".join(out)


def _tg_section(icon: str, title: str) -> str:
    return f"\n{icon} <b>{_esc(title)}</b>"


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
    primary_target_str:  str   = "-",
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
    **_kw: Any,
) -> str:
    now_ist = datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%H:%M IST")
    kz = "KZ ON" if in_killzone else "KZ off"
    flow_side = (flow_direction or "neutral").upper()
    flow_icon = _tg_arrow(flow_conviction, flow_direction)
    pd_label = _pd(float(dealing_range_pd or 0.5))
    pnl_icon = "\U0001f7e2" if float(daily_pnl or 0) >= 0 else "\U0001f534"

    lines = [
        f"\U0001f4ca <b>DELTA STATUS</b>  <code>{_esc(now_ist)}</code>",
        _TG_RULE,
        f"<code>BTC     {_tg_price(current_price):>14}   ATR {_tg_num(atr, 1):>7}</code>",
        f"<code>BAL     {_tg_price(balance):>14}   TRD {int(total_trades):>4}   WR {float(win_rate or 0):>5.1f}%</code>",
        f"{pnl_icon} <code>DAY     {_tg_pnl(daily_pnl):>14}   ALL {_tg_pnl(total_pnl):>14}</code>",
        f"<code>STATE   {_esc((bot_state or 'SCANNING').upper()):<14} {_esc((session or '-').upper()):<10} {kz}</code>",
        _tg_section("\U0001f3db", "Market Context"),
        f"<code>AMD     {_esc(amd_phase or '-'):<16} bias {_esc(amd_bias or '-')}</code>",
        f"<code>PD      {_esc(pd_label):<16} {float(dealing_range_pd or 0.5):>5.0%}   regime {_esc(regime or '-')}</code>",
        f"<code>STRUCT  15m={_esc(structure_15m or '-'):<10} 4h={_esc(structure_4h or '-'):<10} HTF={_esc(htf_bias or '-')}</code>",
        _tg_section("\U0001f4a7", "Liquidity Map"),
        f"<code>POOLS   BSL {int(n_bsl_pools):>3}     SSL {int(n_ssl_pools):>3}</code>",
    ]
    for label, pool in (("BSL", nearest_bsl), ("SSL", nearest_ssl)):
        row = _tg_pool_line(label, pool)
        if row:
            lines.append(row)
    if primary_target_str and str(primary_target_str) not in ("-", "—"):
        lines.append(f"<code>TARGET  {_esc(str(primary_target_str)[:70])}</code>")

    lines += [
        _tg_section("\u26a1", "Order Flow"),
        f"<code>FLOW    {flow_icon} {flow_side:<9} [{_tg_bar(flow_conviction)}] {float(flow_conviction or 0):+5.2f}</code>",
    ]

    if direction_hunt is not None:
        pred = _tg_get(direction_hunt, "predicted", "NEUTRAL")
        conf = float(_tg_get(direction_hunt, "confidence", 0.0) or 0.0)
        delivery = _tg_get(direction_hunt, "delivery_direction", "")
        lines.append(
            f"<code>HUNT    {_esc(str(pred or 'NEUTRAL')):<9} [{_tg_bar(conf)}] {conf:>5.0%} {_esc(delivery or '-')}</code>"
        )

    if sweep_analysis:
        rev = float(sweep_analysis.get("reversal_score", sweep_analysis.get("rev_score", 0.0)) or 0.0)
        cont = float(sweep_analysis.get("continuation_score", sweep_analysis.get("cont_score", 0.0)) or 0.0)
        winner = "REVERSAL" if rev > cont + 15 else ("CONTINUATION" if cont > rev + 15 else "CONTESTED")
        lines += [
            _tg_section("\U0001f30a", "Sweep Read"),
            f"<code>POOL    {_esc(sweep_analysis.get('sweep_side', '-')):<8} @ {_tg_price(sweep_analysis.get('sweep_price', 0.0)):>13}</code>",
            f"<code>SCORE   REV {rev:>5.1f}   CONT {cont:>5.1f}   {winner}</code>",
        ]

    if direction_ps_analysis is not None:
        action = _tg_get(direction_ps_analysis, "action", "-")
        direction = _tg_get(direction_ps_analysis, "direction", "-")
        conf = float(_tg_get(direction_ps_analysis, "confidence", 0.0) or 0.0)
        phase = _tg_get(direction_ps_analysis, "phase", "-")
        lines.append(f"<code>PS      {_esc(str(action).upper()):<9} {_esc(str(direction).upper()):<6} {conf:>5.0%} phase {_esc(phase)}</code>")

    lines += [_tg_section("\U0001f6a6", "Execution Gates")]
    lines.append(f"<code>SESSION {'PASS' if session else 'WAIT':<6}  FLOW {'PASS' if abs(flow_conviction) >= 0.20 else 'WAIT':<6}  HTF {'PASS' if htf_bias and htf_bias.lower() != 'mixed' else 'WAIT':<6}</code>")

    if position:
        side = str(position.get("side") or "?").upper()
        qty = float(position.get("quantity") or 0.0)
        entry = float(entry_price or position.get("entry_price") or 0.0)
        sl = float(current_sl or position.get("sl_price") or 0.0)
        tp = float(current_tp or position.get("tp_price") or 0.0)
        move = (float(current_price or 0.0) - entry) if side == "LONG" else (entry - float(current_price or 0.0))
        risk = abs(entry - sl)
        r_now = move / risk if risk > 1e-10 else 0.0
        upnl = move * qty if qty else move
        rr = abs(tp - entry) / risk if risk > 1e-10 and tp else 0.0
        lines += [
            _tg_section("\U0001f512", "Active Position"),
            f"<code>SIDE    {_esc(side):<8} qty {qty:.6f}   R {r_now:+.2f}</code>",
            f"<code>ENTRY   {_tg_price(entry):>14}   UPNL {_tg_pnl(upnl):>14}</code>",
            f"<code>SL      {_tg_price(sl):>14}   TP {_tg_price(tp):>14}   RR 1:{rr:.2f}</code>",
        ]
        if breakeven_moved:
            lines.append(f"<code>LOCK    BE protected   secured {float(profit_locked_pct or 0):+.2f}R</code>")

    lines += [
        _tg_section("\U0001f4c8", "Risk & Performance"),
        f"<code>LOSS STREAK {int(consecutive_losses):>2}   TOTAL TRADES {int(total_trades):>4}</code>",
    ]
    if extra_lines:
        lines.append(_tg_section("\U0001f4dd", "Notes"))
        for item in extra_lines[:8]:
            if item and str(item).strip():
                lines.append(f"<i>{_esc(str(item)[:220])}</i>")
    return _tg_limit(lines)


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
    **_kw: Any,
) -> str:
    reason = _kw.get("reason", "")
    factors = _kw.get("factors", None)
    lines = [
        f"\U0001f9ed <b>LIQUIDITY DRAW</b>  <code>{_esc(predicted or 'NEUTRAL')}</code>",
        _TG_RULE,
        f"<code>CONF    [{_tg_bar(confidence)}] {float(confidence or 0):>5.0%}   delivery {_esc(delivery_direction or '-')}</code>",
        f"<code>SCORE   BSL {float(bsl_score or 0):+7.3f}   SSL {float(ssl_score or 0):+7.3f}   raw {float(raw_score or 0):+7.3f}</code>",
    ]
    if current_price:
        lines.append(f"<code>MARK    {_tg_price(current_price):>14}   ATR {_tg_num(atr, 1):>7}</code>")
    for label, pool in (("BSL", nearest_bsl), ("SSL", nearest_ssl)):
        row = _tg_pool_line(label, pool)
        if row:
            lines.append(row)
    if _kw.get("swept_pool_price") or _kw.get("opposing_pool_price"):
        lines.append(f"<code>SWEPT   {_tg_price(_kw.get('swept_pool_price')):>14}   OPP {_tg_price(_kw.get('opposing_pool_price')):>14}</code>")
    if _kw.get("amd_phase") or _kw.get("htf_bias"):
        lines.append(f"<code>CTX     AMD {_esc(_kw.get('amd_phase', '-') or '-'):<14} HTF {_esc(_kw.get('htf_bias', '-') or '-')}</code>")
    if factors:
        if isinstance(factors, dict):
            frag = ", ".join(f"{k}:{v}" for k, v in list(factors.items())[:5])
        else:
            frag = ", ".join(str(x) for x in list(factors)[:5])
        lines.append(f"<i>{_esc(frag[:220])}</i>")
    if reason:
        lines.append(f"<i>{_esc(str(reason)[:260])}</i>")
    return _tg_limit(lines)


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
    **_kw: Any,
) -> str:
    swept_price = _kw.get("swept_pool_price", sweep_price)
    swept_side = _kw.get("swept_pool_type", sweep_side)
    winner = "REVERSAL" if rev_score > cont_score + 15 else ("CONTINUATION" if cont_score > rev_score + 15 else "CONTESTED")
    tags = []
    if cisd_active:
        tags.append("CISD")
    if ote_active:
        tags.append("OTE")
    if displacement_atr:
        tags.append(f"disp {float(displacement_atr):.2f}A")
    lines = [
        f"\U0001f30a <b>POST-SWEEP VERDICT</b>  <code>{_esc(str(action).upper())}</code>",
        _TG_RULE,
        f"<code>DIR     {_esc(str(direction).upper() or '-'):<8} [{_tg_bar(confidence)}] {float(confidence or 0):>5.0%}   phase {_esc(phase or '-')}</code>",
        f"<code>POOL    {_esc(swept_side or '-'):<8} @ {_tg_price(swept_price):>13}   mark {_tg_price(current_price):>13}</code>",
        f"<code>SCORE   REV {float(rev_score or 0):>6.1f}   CONT {float(cont_score or 0):>6.1f}   {winner}</code>",
    ]
    if tags:
        lines.append(f"<code>TAGS    {_esc(' / '.join(tags))}</code>")
    for label, key in (("REV", "rev_reasons"), ("CONT", "cont_reasons")):
        vals = _kw.get(key) or []
        if vals:
            lines.append(f"<i>{label}: {_esc(', '.join(str(x) for x in vals[:4])[:240])}</i>")
    if _kw.get("reason"):
        lines.append(f"<i>{_esc(str(_kw.get('reason'))[:260])}</i>")
    return _tg_limit(lines)


def format_pool_gate_alert(
    action:        str,
    side:          str = "",
    pool_side:     str = "",
    pool_price:    float = 0.0,
    current_price: float = 0.0,
    reason:        str   = "",
    rev_score:     float = 0.0,
    cont_score:    float = 0.0,
    **_kw: Any,
) -> str:
    side = side or _kw.get("pos_side", "")
    confidence = float(_kw.get("confidence", 0.0) or 0.0)
    lines = [
        f"\U0001f6a6 <b>POOL GATE</b>  <code>{_esc(str(action).upper())}</code>",
        _TG_RULE,
        f"<code>POS     {_esc(str(side).upper() or '-'):<8} mark {_tg_price(current_price):>13}   conf {confidence:>5.0%}</code>",
    ]
    if pool_price:
        lines.append(f"<code>POOL    {_esc(pool_side or '-'):<8} @ {_tg_price(pool_price):>13}</code>")
    if _kw.get("pos_entry") or _kw.get("pos_sl") or _kw.get("pos_tp"):
        lines.append(
            f"<code>BRKT    entry {_tg_price(_kw.get('pos_entry')):>13}   SL {_tg_price(_kw.get('pos_sl')):>13}   TP {_tg_price(_kw.get('pos_tp')):>13}</code>"
        )
    if rev_score or cont_score:
        lines.append(f"<code>SCORE   REV {float(rev_score or 0):>6.1f}   CONT {float(cont_score or 0):>6.1f}</code>")
    if reason:
        lines.append(f"<i>{_esc(str(reason)[:300])}</i>")
    return _tg_limit(lines)


def format_conviction_block_alert(
    side:           str,
    factor_scores:  Optional[Dict[str, float]] = None,
    weighted_total: Optional[float] = None,
    reasons:        Optional[List[str]] = None,
    required_score: float = _REQUIRED_CONVICTION_SCORE,
    **_kw: Any,
) -> str:
    score = float(weighted_total if weighted_total is not None else _kw.get("score", 0.0) or 0.0)
    factors = factor_scores or _kw.get("factors") or {}
    reject_reasons = reasons or _kw.get("reject_reasons") or []
    allow_reasons = _kw.get("allow_reasons") or []
    deficit = max(0.0, float(required_score or 0.0) - score)
    lines = [
        f"\U0001f6ab <b>CONVICTION BLOCK</b>  <code>{_esc(str(side).upper())}</code>",
        _TG_RULE,
        f"<code>SCORE   {score:>6.2f} / {float(required_score or 0):>5.2f}   need +{deficit:.2f}   [{_tg_bar(score / max(required_score, 0.01))}]</code>",
    ]
    if _kw.get("entry_price") or _kw.get("sl_price") or _kw.get("tp_price"):
        lines.append(
            f"<code>SETUP   entry {_tg_price(_kw.get('entry_price')):>13}   SL {_tg_price(_kw.get('sl_price')):>13}   TP {_tg_price(_kw.get('tp_price')):>13}   RR 1:{float(_kw.get('rr_ratio', 0) or 0):.2f}</code>"
        )
    if factors:
        lines.append(_tg_section("\U0001f9ee", "Factor Stack"))
        for name, val in list(factors.items())[:10]:
            try:
                fval = float(val)
            except Exception:
                fval = 0.0
            lines.append(f"<code>{_esc(str(name))[:18]:<18} {fval:+6.2f} [{_tg_bar(fval)}]</code>")
    if reject_reasons:
        lines.append(_tg_section("\u26d4", "Reject Reasons"))
        for item in reject_reasons[:6]:
            lines.append(f"<i>{_esc(str(item)[:220])}</i>")
    if allow_reasons:
        lines.append(_tg_section("\u2705", "Positive Evidence"))
        for item in allow_reasons[:4]:
            lines.append(f"<i>{_esc(str(item)[:180])}</i>")
    return _tg_limit(lines)


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
    session:       str           = "",
    fib_ratio:     Optional[float] = None,
    r_multiple:    float           = 0.0,
    swing_low:     Optional[float] = None,
    swing_high:    Optional[float] = None,
    momentum_gate: str             = "",
    htf_aligned:   Optional[bool]  = None,
    is_cluster:    bool            = False,
    n_cluster_tfs: int             = 1,
    pool_boost:    bool            = False,
    pool_between_expand: bool      = False,
    buffer_atr:    float           = 0.0,
) -> str:
    side_u = str(side or "").upper()
    move = (float(current_price or 0) - float(entry_price or 0)) if side_u == "LONG" else (float(entry_price or 0) - float(current_price or 0))
    locked = (float(new_sl or 0) - float(entry_price or 0)) if side_u == "LONG" else (float(entry_price or 0) - float(new_sl or 0))
    locked_atr = locked / max(float(atr or 0), 1e-10)
    tags = []
    if is_cluster:
        tags.append(f"{int(n_cluster_tfs)}TF cluster")
    if pool_boost:
        tags.append("pool boost")
    if pool_between_expand:
        tags.append("buffer expanded")
    if is_swept:
        tags.append("anchor swept")
    if htf_aligned is not None:
        tags.append("HTF aligned" if htf_aligned else "HTF mixed")
    lines = [
        f"\U0001f512 <b>FIB TRAIL ADVANCE</b>  <code>{_esc(side_u)}</code>",
        _TG_RULE,
        f"<code>SL      {_tg_price(new_sl):>14}   phase {_esc(phase or '-')}</code>",
        f"<code>R       live {float(r_multiple or 0):+6.2f}R   lock {locked:+8.1f} pts ({locked_atr:+.2f}A)   move {_tg_num(move, 1):>8} pts</code>",
        f"<code>FIB     {fib_ratio if fib_ratio is not None else '-':>8}   anchor {_esc(anchor_tf or '-'):>4} @ {_tg_price(anchor_price):>13}   sig {float(anchor_sig or 0):.1f}</code>",
        f"<code>MARK    {_tg_price(current_price):>14}   ENTRY {_tg_price(entry_price):>14}   ATR {_tg_num(atr, 1):>7}</code>",
    ]
    if swing_low is not None or swing_high is not None:
        lines.append(f"<code>SWING   low {_tg_price(swing_low):>13}   high {_tg_price(swing_high):>13}</code>")
    if momentum_gate:
        lines.append(f"<code>GATE    {_esc(momentum_gate)}</code>")
    if tags:
        lines.append(f"<i>{_esc(' / '.join(tags))}</i>")
    if session or buffer_atr:
        lines.append(f"<code>CTX     session {_esc(session or '-')}   buffer {float(buffer_atr or 0):.2f} ATR</code>")
    return _tg_limit(lines)


def format_log_alert(level: str, logger_name: str, message: str) -> str:
    level_u = (level or "WARNING").upper()
    icon = {
        "WARNING": "\u26a0\ufe0f",
        "ERROR": "\U0001f6a8",
        "CRITICAL": "\U0001f6a8",
    }.get(level_u, "\U0001f4dd")
    now_ist = datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%H:%M:%S IST")
    clean = _repair_mojibake(str(message or "")).strip()
    lines = [
        f"{icon} <b>LOG { _esc(level_u) }</b>  <code>{_esc(now_ist)}</code>",
        _TG_RULE,
        f"<code>SRC     {_esc((logger_name or '-')[-32:])}</code>",
        f"<pre>{_esc(clean[:1800])}</pre>",
    ]
    return _tg_limit(lines, max_chars=2200)

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

        if not _content_dedup_should_pass(prio, message):
            return False

        # SPAM-FIX 2026-04-26: rate governor.  Drops non-CRITICAL messages
        # silently when the rolling 60s window exceeds TG_RATE_LIMIT_PER_MIN.
        # A periodic summary log is emitted so the operator sees it happened.
        if not _rate_governor_should_pass(prio):
            return False

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
        "dedup_hits":       _dedup_hits,
        "content_dedup_hits": _content_dedup_hits,
        "rate_governed":    _rate_governed,
    }


# ══════════════════════════════════════════════════════════════════════
# SPAM-FIX 2026-04-26 — generic dedup helper + global rate governor
# ══════════════════════════════════════════════════════════════════════
#
# The post-sweep verdict (quant_strategy.py:4263) and other repeating
# alerts each implement their own ad-hoc dedup. This helper centralises
# the pattern so any caller can opt in:
#
#     from telegram.notifier import send_telegram_dedup
#     send_telegram_dedup("post_sweep:long:0.55", ttl=60.0,
#                          message=format_post_sweep_verdict(...))
#
# The (key, ttl) pair gates the send: if the same key was sent within
# TTL seconds, the new send is dropped silently and a counter is bumped.
# Keys should be coarse enough that real state changes produce a NEW key
# (e.g. round confidence to 15% buckets, not 5%).
#
# In addition, we enforce a global RATE GOVERNOR: no more than
# TG_RATE_LIMIT_PER_MIN messages of priority >= 1 (IMPORTANT/ROUTINE)
# in any rolling 60-second window. CRITICAL messages bypass the
# governor entirely. When the governor trips, ROUTINE drops first, then
# IMPORTANT, with a single periodic "[N suppressed]" summary so the
# operator knows it happened.

_dedup_lock = threading.Lock()
_dedup_state: Dict[str, float] = {}     # key -> next_allowed_ts
_dedup_hits: int = 0                    # observability counter

_rate_lock = threading.Lock()
_rate_window: deque = deque(maxlen=512)  # ts of recent non-CRITICAL sends
_rate_governed: int = 0
_rate_suppressed_summary_ts: float = 0.0
_rate_suppressed_since_summary: int = 0
_content_dedup_lock = threading.Lock()
_content_dedup_state: Dict[str, float] = {}
_content_dedup_hits: int = 0

# Tunables — overridable via add_telegram_suppress_pattern's neighbour API
TG_RATE_LIMIT_PER_MIN: int = 30          # rolling 60s budget for non-CRITICAL
TG_RATE_SUMMARY_INTERVAL: float = 300.0  # how often to emit "[N suppressed]"
TG_CONTENT_DEDUP_TTL: float = 20.0       # same alert shape within this window


def _dedup_should_send(key: str, ttl: float) -> bool:
    """Return True if (key, ttl) permits a send right now; False if dedup'd."""
    if not key or ttl <= 0:
        return True
    global _dedup_hits
    now = time.time()
    with _dedup_lock:
        next_ok = _dedup_state.get(key, 0.0)
        if now < next_ok:
            _dedup_hits += 1
            return False
        _dedup_state[key] = now + ttl
        # Opportunistic GC: keep state small.
        if len(_dedup_state) > 500:
            cutoff = now - 60.0
            for k in [k for k, v in _dedup_state.items() if v < cutoff]:
                _dedup_state.pop(k, None)
        return True


def _rate_governor_should_pass(prio: int) -> bool:
    """Rolling 60s rate limit. CRITICAL bypasses; others budgeted."""
    if prio == PRIO_CRITICAL:
        return True
    global _rate_governed, _rate_suppressed_since_summary, _rate_suppressed_summary_ts
    now = time.time()
    cutoff = now - 60.0
    with _rate_lock:
        while _rate_window and _rate_window[0] < cutoff:
            _rate_window.popleft()
        if len(_rate_window) >= TG_RATE_LIMIT_PER_MIN:
            _rate_governed += 1
            _rate_suppressed_since_summary += 1
            # Periodic summary so the operator sees it happened.
            if now - _rate_suppressed_summary_ts >= TG_RATE_SUMMARY_INTERVAL:
                _rate_suppressed_summary_ts = now
                n = _rate_suppressed_since_summary
                _rate_suppressed_since_summary = 0
                # Use the worker queue path for the summary itself —
                # priority IMPORTANT, never CRITICAL (don't bypass the queue
                # for a meta-message).
                logger.warning(
                    "Telegram rate governor: %d non-critical messages suppressed "
                    "in last ~%.0fs (limit=%d/min). Check for spam loop.",
                    n, TG_RATE_SUMMARY_INTERVAL, TG_RATE_LIMIT_PER_MIN,
                )
            return False
        _rate_window.append(now)
        return True


_CONTENT_NUMBER_RE = re.compile(r"(?<![A-Za-z])[-+]?\$?\d[\d,]*(?:\.\d+)?%?")
_CONTENT_WS_RE = re.compile(r"\s+")


def _content_fingerprint(message: str) -> str:
    text = re.sub(r"<[^>]*>", " ", str(message))
    text = _html_lib.unescape(text)
    text = _CONTENT_NUMBER_RE.sub("#", text)
    text = _CONTENT_WS_RE.sub(" ", text).strip().upper()
    return text[:280]


def _content_dedup_should_pass(prio: int, message: str) -> bool:
    if prio == PRIO_CRITICAL or TG_CONTENT_DEDUP_TTL <= 0:
        return True
    key = _content_fingerprint(message)
    if not key:
        return True
    global _content_dedup_hits
    now = time.time()
    with _content_dedup_lock:
        next_ok = _content_dedup_state.get(key, 0.0)
        if now < next_ok:
            _content_dedup_hits += 1
            return False
        _content_dedup_state[key] = now + TG_CONTENT_DEDUP_TTL
        if len(_content_dedup_state) > 800:
            for k, v in list(_content_dedup_state.items()):
                if v < now:
                    _content_dedup_state.pop(k, None)
        return True


def send_telegram_dedup(
    key: str,
    ttl: float,
    message: str,
    parse_mode: str = "HTML",
) -> bool:
    """
    Send a Telegram message, deduplicated by (key, ttl).

    Returns True if the message was enqueued, False if it was dropped
    by the dedup window or rate governor.

    Use coarse keys: round confidence to 15% buckets, not 5%. Round prices
    to 0.5-ATR bins, not exact dollars. The point of dedup is to drop
    "same alert again because state wiggled" — make the key change only
    on real state changes.

    Examples:
        # post-sweep verdict
        send_telegram_dedup(f"ps:{action}:{direction}:{round(conf*7)/7:.2f}",
                             ttl=60.0, message=...)
        # pool-gate near-touch
        send_telegram_dedup(f"pool:{side}:{int(price/atr/0.5)}",
                             ttl=120.0, message=...)
    """
    if not _dedup_should_send(key, ttl):
        return False
    return send_telegram_message(message, parse_mode=parse_mode)


def reset_dedup_state() -> None:
    """For tests + operator /reset_dedup. Wipes all dedup keys."""
    with _dedup_lock:
        _dedup_state.clear()
    with _rate_lock:
        _rate_window.clear()
    with _content_dedup_lock:
        _content_dedup_state.clear()


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


# ======================================================================
# GATE DIAGNOSTIC PANEL (internal)
# ======================================================================

# ══════════════════════════════════════════════════════════════════════
# FORMAT FUNCTIONS  — clean, signal-first Telegram output
# ══════════════════════════════════════════════════════════════════════

# ─── shared mini-helpers ──────────────────────────────────────────────

def _pd(pd: float) -> str:
    if pd < 0.25: return "DEEP-DISC"
    if pd < 0.40: return "DISCOUNT"
    if pd < 0.60: return "EQUILIB"
    if pd < 0.75: return "PREMIUM"
    return "DEEP-PREM"


# ══════════════════════════════════════════════════════════════════════
# NOTE 2026-04-26 (cleanup):
#   This region used to hold the original first definitions of
#   format_periodic_report, format_direction_hunt_alert,
#   format_post_sweep_verdict, format_conviction_block_alert, and
#   format_liquidity_trail_update. They were silently shadowed by the
#   later v9.1 industry-grade definitions (Python last-`def`-wins),
#   so they were dead code and have been removed. The live versions
#   are below in the v9.1 INDUSTRY-GRADE TELEGRAM TEMPLATES section.
# ══════════════════════════════════════════════════════════════════════


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
    # Pool-gate diagnostic messages — these are downgraded to INFO at source
    # and a formatted Telegram alert is sent via send_telegram_message()
    # directly.  Belt-and-braces guard: if any code path accidentally logs
    # these at WARNING they must NOT produce a second Telegram notification.
    "POOL-GATE reverse signal: no exit taken",
    "POOL-GATE BE blocked: desired=",
    "POOL-GATE BE still blocked: desired=",
    "POOL-GATE reverse held:",
    # ── SPAM-FIX 2026-04-26 ──────────────────────────────────────────────
    # The following patterns were identified from a 32k-line production log
    # as the dominant Telegram spam sources beyond the post-sweep verdict
    # itself (which is fixed at the source — see quant_strategy._ps_tg_last_hash).
    #
    # 1. SWEEP REJECTED (tf_quality): 113 instances/session. Routine gate
    #    rejection. Source-downgraded to INFO; this is belt-and-braces.
    "SWEEP REJECTED (tf_quality):",
    # 2. Telegram API HTTP errors on getUpdates: when Telegram itself
    #    rate-limits the bot, the WARN was being routed BACK into the
    #    Telegram queue, amplifying the burst. Source-downgraded to
    #    throttled WARN/INFO; this is belt-and-braces.
    "Telegram API HTTP",
    "getUpdates skipped",
    "Telegram connection error",
    # 3. Watchdog stuck-flag self-heal: routine maintenance, not actionable
    "watchdog[stuck_exit_completed]",
    "watchdog[stuck_trail_flag]",
    "watchdog[no_trades_after_first]",
    # 4. Notifier internal retry chatter — the queue/retry mechanism is
    #    its own observability layer; don't notify Telegram about Telegram
    #    being slow.
    "Telegram 429",
    "Telegram 502",
    "Telegram 503",
    "Telegram queue full",
    # 5. WebSocket reconnect warnings — handled by reconnect logic; they
    #    fire briefly during normal network blips and would otherwise
    #    cluster as a 3-message Telegram burst per blip.
    "DeltaWebSocket closed",
    "DeltaWebSocket reconnecting",
    # 6. FibTrail dispatch block (rare but bursts when it does) — already
    #    surfaced via the throttled trail Telegram update, no need for
    #    duplicate via log handler.
    "FibTrail dispatch blocked:",
    # 7. Circuit-breaker steady state. The breaker trip/clear messages are
    #    actionable; the per-entry "still frozen" state is local telemetry.
    "Entries blocked: watchdog circuit breaker is engaged",
    "Entries still blocked by watchdog circuit breaker",
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

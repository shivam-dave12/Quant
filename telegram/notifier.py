"""Telegram notifications for the Institutional Auction execution system.

Only structural thesis geometry, venue protection, exact-fill reconciliation
and instrument-correct P&L are displayed.
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
    "🎯": "🎯", "🧭": "🧭", "📊": "📊", "💰": "💰",
    "🔒": "🔒", "🔄": "🔄", "🔱": "🔱", "🚨": "🚨",
    "💀": "💀", "💥": "💥", "✅": "✅", "❌": "❌",
    "❌": "❌", "⚠️": "⚠️", "⚠️": "⚠️", "⏱️": "⏱️",
    "⏱️": "⏱️", "⏱️": "⏱️", "⏱️": "⏱️", "⏳": "⏳",
    "≈": "≈", "±": "±", "×": "×", "σ": "σ",
    "⬜": "⬜", "░": "░", "█": "█",
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



# ======================================================================
# ASYNC SEND WORKER
# ======================================================================

# ──────────────────────────────────────────────────────────────────────────
# v2.1 QUEUE: tiered priority queue with load-aware shedding
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
        "ENTRY", "EXIT", "TRADE OPEN", "TRADE CLOSED",
        "POSITION ADOPTED", "WATCHDOG HEAL", "WATCHDOG CIRCUIT",
        "AUCTION_DECISION", "INSTITUTIONAL_ORDER_THESIS", "INSTITUTIONAL_AUCTION",
        "STRUCTURAL SL", "LIQUIDITY TARGET", "EXACT-FILL",
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




# ======================================================================
# MULTI-ASSET TELEGRAM CONTEXT ENRICHMENT
# ======================================================================

def _tg_current_instrument():
    try:
        from core.instruments import current_instrument
        return current_instrument()
    except Exception:
        return None


def _tg_asset_policy(inst):
    try:
        from core.market_policy import active_policy
        return active_policy(inst)
    except Exception:
        return None


def _tg_asset_header(inst=None, event_type: str = "", context: Optional[Dict[str, Any]] = None) -> str:
    """Build an institutional asset-specific Telegram header.

    Centralised formatting ensures Telegram always receives the correct
    contract, venue, currency, protected-state and portfolio context.
    """
    inst = inst or _tg_current_instrument()
    if inst is None:
        return ""
    context = context or {}
    try:
        asset = _esc(getattr(inst, "asset_id", "ASSET"))
        name = _esc(getattr(inst, "display_name", asset))
        primary = getattr(inst, "primary_exchange", None)
        primary_name = _esc(getattr(primary, "value", str(primary or "-")).upper())
        symbol = _esc(getattr(inst, "display_symbol", getattr(inst, "execution_symbol", "-")))
        asset_class = _esc(getattr(getattr(inst, "asset_class", ""), "value", str(getattr(inst, "asset_class", ""))).upper())
        venues = []
        for ex, ei in getattr(inst, "by_exchange", {}).items():
            try:
                venues.append(f"{getattr(ex,'value',str(ex)).upper()}:{getattr(ei,'display_symbol',getattr(ei,'symbol','-'))}")
            except Exception:
                continue
        venue_txt = _esc(", ".join(venues) if venues else f"{primary_name}:{symbol}")
        pol = _tg_asset_policy(inst)
        # Prefer actual runtime/entry leverage over policy/config leverage.
        # Policy leverage is a venue cap; executed leverage is selected from
        # structural-risk funding and liquidation-safety geometry.
        lev = (context.get("entry_leverage") or context.get("actual_leverage")
               or context.get("leverage") or getattr(pol, "leverage", None)
               or getattr(inst, "max_leverage", 0) or "-")
        margin = getattr(pol, "margin_pct", None)
        risk_mult = getattr(pol, "risk_multiplier", None)
        cadence = getattr(pol, "evaluation_interval_sec", None)
        state = _esc(str(context.get("state") or context.get("phase") or "-").upper())
        price = context.get("price")
        slots = context.get("slots") or context.get("portfolio_slots") or ""
        event = _esc(str(event_type or context.get("event_type") or "STRATEGY").upper().replace("_", " "))
        line1 = f"🏛 <b>{event}</b>  <code>{asset}</code> <i>{name}</i>"
        line2 = f"<code>{primary_name}:{symbol}</code> · {asset_class} · venues <code>{venue_txt}</code>"
        bits = []
        if lev != "-":
            try: bits.append(f"lev {float(lev):g}x")
            except Exception: bits.append(f"lev {lev}")
        if margin is not None:
            try: bits.append(f"margin {float(margin):.0%}")
            except Exception: pass
        if risk_mult is not None:
            try: bits.append(f"risk×{float(risk_mult):.2f}")
            except Exception: pass
        if cadence is not None:
            try: bits.append(f"cadence {float(cadence):.2f}s")
            except Exception: pass
        if state and state != "-": bits.append(f"state {state}")
        if price is not None:
            try: bits.append(f"px {_tg_price(float(price), venue=primary_name)}")
            except Exception: pass
        if slots: bits.append(f"slots {slots}")
        line3 = "<code>" + _esc(" | ".join(bits)) + "</code>" if bits else ""
        return "\n".join([x for x in (line1, line2, line3, _TG_RULE) if x])
    except Exception:
        return ""


def _tg_message_already_asset_scoped(message: str) -> bool:
    m = str(message or "")[:240]
    return ("<b>ASSET" in m or "🏛 <b>" in m or "MULTI-ASSET" in m or "EXECUTION UNIVERSE" in m)


def _tg_infer_event_type(message: str) -> str:
    m = str(message or "").upper()
    if "BRACKET" in m or "ENTRY" in m or "POSITION OPEN" in m:
        return "EXECUTION"
    if "SL" in m or "STOP" in m or "PROTECTION" in m:
        return "PROTECTED RISK"
    if "EXIT" in m or "PNL" in m or "TP HIT" in m:
        return "EXIT"
    if "AUCTION_DECISION" in m or "INSTITUTIONAL_ORDER_THESIS" in m or "INSTITUTIONAL_AUCTION" in m:
        return "INSTITUTIONAL AUCTION DECISION"
    if "LIQUIDITY" in m or "SWEEP" in m or "POOL" in m:
        return "LIQUIDITY"
    if "STATUS" in m or "THINK" in m:
        return "STATUS"
    return "ASSET EVENT"


def _tg_enrich_asset_message(message: str, *, instrument=None, event_type: Optional[str] = None, context: Optional[Dict[str, Any]] = None) -> str:
    inst = instrument or _tg_current_instrument()
    if inst is None:
        return message
    if _tg_message_already_asset_scoped(message):
        return message
    header = _tg_asset_header(inst, event_type or _tg_infer_event_type(message), context=context)
    if not header:
        return message
    return f"{header}\n{message}"

def send_telegram_message(message: str, parse_mode: str = "HTML", *, instrument=None, event_type: Optional[str] = None, context: Optional[Dict[str, Any]] = None, enrich: bool = True) -> bool:
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
    if enrich:
        try:
            message = _tg_enrich_asset_message(message, instrument=instrument, event_type=event_type, context=context)
        except Exception:
            pass
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


def _tg_currency_symbol(venue: str = "", inst: Any = None) -> str:
    text = str(venue or "").lower()
    if inst is not None:
        try:
            text += " " + str(getattr(getattr(inst, "primary_exchange", ""), "value", getattr(inst, "primary_exchange", ""))).lower()
            text += " " + str(getattr(getattr(inst, "primary", None), "quote_asset", "")).lower()
        except Exception:
            pass
    if "groww" in text or "inr" in text:
        return "\u20b9"
    return "$"


def _tg_current_venue() -> str:
    inst = _tg_current_instrument()
    try:
        return str(getattr(getattr(inst, "primary_exchange", ""), "value", getattr(inst, "primary_exchange", ""))).upper()
    except Exception:
        return ""


# ======================================================================
# UTILITY HELPERS
# ======================================================================

def _fmt_price(p: Optional[float], venue: str = "") -> str:
    if p is None:
        return "—"
    cur = _tg_currency_symbol(venue or _tg_current_venue(), _tg_current_instrument())
    digits = 2 if cur == "\u20b9" else 1
    return f"{cur}{p:,.{digits}f}"


# ======================================================================
# LOGGING HANDLER# ======================================================================
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
    # Structural sweep-quality deferrals are INFO-level decision context and
    # should never be duplicated as Telegram WARNING notifications.
    "SWEEP QUALITY IMPAIRED [tf_quality]:",
    "SWEEP DEFERRED [tf_quality]:",
    # 2. Telegram API HTTP errors on getUpdates: when Telegram itself
    #    rate-limits the bot, the WARN was being routed BACK into the
    #    Telegram queue, amplifying the burst. Source-downgraded to
    #    throttled WARN/INFO; this is belt-and-braces.
    "Telegram API HTTP",
    "getUpdates skipped",
    "Telegram connection error",
    # 3. Watchdog stuck-flag self-heal: routine maintenance, not actionable
    "watchdog[stuck_exit_completed]",
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
    "Entries paused: watchdog circuit breaker is engaged",
    "Entries still paused by watchdog circuit breaker",
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


def _venue_currency(venue: str = "", inst: Any = None) -> str:
    return _tg_currency_symbol(venue, inst)


def _side_arrow(side: str) -> str:
    return "▲ LONG" if str(side or "").lower() == "long" else "▼ SHORT"


def format_entry_alert(*, side: str, price: float = 0.0, entry: float = 0.0, sl: float = 0.0, tp: float = 0.0,
                       qty: float = 0.0, leverage: float = 1.0, venue: str = "",
                       context_4h: Any = "-", context_15m: Any = "-",
                       raid_quality: float = 0.0, displacement_atr: float = 0.0,
                       delivery_score: float = 0.0, delivery_probability: Optional[float] = None,
                       delivery_utility_r: float = 0.0, probability_calibrated: bool = False,
                       archetype: str = "STRUCTURAL_AUCTION", rr: float = 0.0,
                       instrument: Any = None, **kwargs) -> str:
    inst = instrument or _tg_current_instrument()
    sym = _venue_currency(venue, inst)
    price = float(price or entry or 0.0)
    risk_usd = float(kwargs.get("risk_usd", kwargs.get("structural_risk", 0.0)) or 0.0)
    margin_used = float(kwargs.get("margin_used", 0.0) or 0.0)
    fee_status = str(kwargs.get("fee_status", kwargs.get("fee_line", "")) or "")
    raid_label = str(kwargs.get("raid_label", "-") or "-")
    raid_price = float(kwargs.get("raid_price", 0.0) or 0.0)
    target_label = str(kwargs.get("target_label", "opposing HTF liquidity") or "opposing HTF liquidity")
    vehicle = ""
    try:
        primary = getattr(inst, "primary", None)
        raw = getattr(primary, "raw", {}) if primary is not None else {}
        selected = raw.get("selected_option_contract", {}) if isinstance(raw, dict) else {}
        row = selected.get("raw", selected) if isinstance(selected, dict) else {}
        contract = row.get("TradingSymbol") or row.get("trading_symbol") or ""
        right = str(row.get("right") or "").lower()
        action = "BUY CALL" if right.startswith("c") else ("BUY PUT" if right.startswith("p") else "BUY OPTION")
        if contract:
            vehicle = f"\nOption Vehicle: {action} <code>{_html_lib.escape(str(contract))}</code>"
    except Exception:
        vehicle = ""
    calibration = (f"Calibrated delivery P={float(delivery_probability):.3f} | utility={float(delivery_utility_r):+.3f}R"
                   if probability_calibrated and delivery_probability is not None
                   else "Calibrated delivery P=N/A | sizing=structural risk + measured execution cost")
    price_block = (
        f"ENTRY  {sym}{float(price):,.4f}\n"
        f"SL     {sym}{float(sl):,.4f}\n"
        f"TP     {sym}{float(tp):,.4f}\n"
        f"R:R    1:{float(rr):.2f}"
    )
    size_block = (
        f"QTY    {float(qty):.8g}\n"
        f"LEV    {float(leverage):.1f}x\n"
        f"RISK   {sym}{risk_usd:,.2f}"
    )
    if margin_used > 0:
        size_block += f"\nMARGIN {sym}{margin_used:,.2f}"
    context_block = (
        f"4H     {_esc(context_4h)}\n"
        f"15m    {_esc(context_15m)}\n"
        f"RAID   {_esc(raid_label)}" + (f" @ {sym}{raid_price:,.4f}" if raid_price > 0 else "") + "\n"
        f"DISP   {float(displacement_atr):.2f} ATR\n"
        f"SCORE  delivery {float(delivery_score):+.2f} | raid {float(raid_quality):.2f}"
    )
    lines = [
        f"<b>ENTRY TICKET | {_side_arrow(side)}</b>",
        f"<code>{_esc(archetype)}</code>",
        "<b>Price Map</b>",
        f"<pre>{_esc(price_block)}</pre>",
        "<b>Size / Risk</b>",
        f"<pre>{_esc(size_block)}</pre>",
        "<b>Structure</b>",
        f"<pre>{context_block}</pre>",
        f"<b>Model</b>\n<code>{_esc(calibration)}</code>",
        f"<b>Exit Authority</b>\n<code>Structural SL + {_esc(target_label)}</code>",
    ]
    if fee_status:
        lines.append(f"<b>Fees</b>\n<code>{_esc(fee_status)}</code>")
    if vehicle:
        lines.append(vehicle.lstrip())
    return "\n".join(lines)


def format_exit_alert(*, side: str = "", entry_price: float = 0.0, exit_price: float = 0.0,
                      pnl: float = 0.0, reason: str = "", venue: str = "", quantity: float = 0.0,
                      exact_fill: bool = True, provisional: bool = False, **kwargs) -> str:
    sym = _venue_currency(venue, _tg_current_instrument())
    entry_price = float(kwargs.get("entry", entry_price) or 0.0)
    quantity = float(kwargs.get("qty", quantity) or 0.0)
    residual_qty = float(kwargs.get("residual_qty", 0.0) or 0.0)
    partial_qty = float(kwargs.get("partial_qty", 0.0) or 0.0)
    gross = float(kwargs.get("gross", pnl) or 0.0)
    fees = float(kwargs.get("fees", 0.0) or 0.0)
    r_realised = float(kwargs.get("r_realised", 0.0) or 0.0)
    mfe_r = float(kwargs.get("mfe_r", 0.0) or 0.0)
    planned_rr = float(kwargs.get("planned_rr", 0.0) or 0.0)
    margin_pct = float(kwargs.get("margin_pct", 0.0) or 0.0)
    margin_used = float(kwargs.get("margin_used", 0.0) or 0.0)
    fee_source = str(kwargs.get("fee_source", "") or "")
    tp_ladder_net = float(kwargs.get("tp_ladder_net", 0.0) or 0.0)
    residual_net = float(kwargs.get("residual_net", pnl) or 0.0)
    exact_fill = bool(kwargs.get("exact_fees", exact_fill))
    provisional = bool(kwargs.get("pnl_provisional", provisional))
    state = "EXACT BROKER FILL" if exact_fill and not provisional else "PENDING FEE/FILL RECONCILIATION"
    price_block = (
        f"ENTRY  {sym}{entry_price:,.4f}\n"
        f"EXIT   {sym}{float(exit_price):,.4f}\n"
        f"R      {r_realised:+.2f}R / plan {planned_rr:.2f}R\n"
        f"MFE    {mfe_r:+.2f}R"
    )
    pnl_block = (
        f"GROSS  {sym}{gross:+,.4f}\n"
        f"FEES   {sym}{fees:,.4f}\n"
        f"NET    {sym}{float(pnl):+,.4f}\n"
        f"ROE    {margin_pct:+.2f}%"
    )
    if margin_used > 0:
        pnl_block += f"\nMARGIN {sym}{margin_used:,.2f}"
    qty_block = (
        f"START  {quantity:.8g}\n"
        f"PART   {partial_qty:.8g}\n"
        f"FINAL  {residual_qty:.8g}"
    )
    lines = [
        f"<b>EXIT REPORT | {_side_arrow(side)}</b>",
        f"<code>{_esc(reason or '-')} | {state}</code>",
        "<b>Price / R</b>",
        f"<pre>{_esc(price_block)}</pre>",
        "<b>P&amp;L</b>",
        f"<pre>{_esc(pnl_block)}</pre>",
        "<b>Quantity</b>",
        f"<pre>{_esc(qty_block)}</pre>",
    ]
    if abs(tp_ladder_net) > 1e-12 or abs(residual_net - pnl) > 1e-12:
        ladder_block = (
            f"LADDER    {sym}{tp_ladder_net:+,.4f}\n"
            f"RESIDUAL  {sym}{residual_net:+,.4f}"
        )
        lines.extend(["<b>Lifecycle Split</b>", f"<pre>{_esc(ladder_block)}</pre>"])
    if fee_source:
        lines.extend(["<b>Fee Source</b>", f"<code>{_esc(fee_source)}</code>"])
    return "\n".join(lines)


def format_partial_exit_alert(*, side: str = "", price: float = 0.0, qty: float = 0.0,
                              pnl: float = 0.0, venue: str = "", target: str = "TP", **kwargs) -> str:
    sym = _venue_currency(venue, _tg_current_instrument())
    target = str(kwargs.get("role", target) or target)
    price = float(kwargs.get("fill_price", price) or 0.0)
    qty = float(kwargs.get("qty_closed", qty) or 0.0)
    remaining = float(kwargs.get("qty_remaining", 0.0) or 0.0)
    gross = float(kwargs.get("gross", pnl) or 0.0)
    fees = float(kwargs.get("fees", 0.0) or 0.0)
    pnl = float(kwargs.get("net", pnl) or 0.0)
    cumulative = float(kwargs.get("cumulative_net", pnl) or 0.0)
    sl = float(kwargs.get("sl", 0.0) or 0.0)
    final_tp = float(kwargs.get("final_tp", 0.0) or 0.0)
    status = str(kwargs.get("status", "FILLED") or "FILLED")
    exact = bool(kwargs.get("exact_fees", True))
    fill_block = (
        f"FILL   {sym}{price:,.4f}\n"
        f"CLOSED {qty:.8g}\n"
        f"LEFT   {remaining:.8g}\n"
        f"STATE  {status}"
    )
    pnl_block = (
        f"GROSS  {sym}{gross:+,.4f}\n"
        f"FEES   {sym}{fees:,.4f}\n"
        f"NET    {sym}{pnl:+,.4f}\n"
        f"TOTAL  {sym}{cumulative:+,.4f}"
    )
    lines = [
        f"<b>PARTIAL EXIT | {_esc(target)} | {_side_arrow(side)}</b>",
        f"<code>{'exact broker fill' if exact else 'fee pending'}</code>",
        "<b>Fill</b>",
        f"<pre>{_esc(fill_block)}</pre>",
        "<b>P&amp;L</b>",
        f"<pre>{_esc(pnl_block)}</pre>",
    ]
    if sl > 0 or final_tp > 0:
        structure_block = f"SL     {sym}{sl:,.4f}\nFINAL  {sym}{final_tp:,.4f}"
        lines.extend(["<b>Remaining Structure</b>", f"<pre>{_esc(structure_block)}</pre>"])
    return "\n".join(lines)


def format_periodic_report(*, asset: str = "", symbol: str = "", state: str = "SCANNING",
                           side: str = "", entry_price: float = 0.0, current_price: float = 0.0,
                           sl_price: float = 0.0, tp_price: float = 0.0, pnl: float = 0.0,
                           venue: str = "", context_4h: Any = "-", context_15m: Any = "-",
                           trigger: Any = "WAIT", instrument: Any = None, position: Optional[Dict[str, Any]] = None, balance: float = 0.0, daily_pnl: float = 0.0, total_pnl: float = 0.0, total_trades: int = 0, win_rate: float = 0.0, **kwargs) -> str:
    inst = instrument or _tg_current_instrument()
    sym = _venue_currency(venue, inst)
    position = position or {}
    side = side or str(position.get("side", "") or "")
    entry_price = float(entry_price or position.get("entry_price", 0.0) or 0.0)
    sl_price = float(sl_price or position.get("sl_price", 0.0) or 0.0)
    tp_price = float(tp_price or position.get("tp_price", 0.0) or 0.0)
    pnl = float(pnl or position.get("unrealized_pnl", 0.0) or 0.0)
    lines = [f"🏛️ <b>INSTITUTIONAL AUCTION | {_html_lib.escape(str(asset or symbol or 'DESK'))}</b>",
             f"Price: {sym}{float(current_price):,.2f} | Balance: {sym}{float(balance):,.2f}",
             f"Today: {'+' if float(daily_pnl) >= 0 else '-'}{sym}{abs(float(daily_pnl)):,.2f} | Total: {'+' if float(total_pnl) >= 0 else '-'}{sym}{abs(float(total_pnl)):,.2f}",
             f"State: {_html_lib.escape(str(state))}",
             f"4H: {_html_lib.escape(str(context_4h))} | 15m: {_html_lib.escape(str(context_15m))}",
             f"5m: {_html_lib.escape(str(trigger))}"]
    if side:
        lines += [f"Position: {_side_arrow(side)} | {sym}{float(entry_price):,.2f} → {sym}{float(current_price):,.2f}",
                  f"SL {sym}{float(sl_price):,.2f} | TP {sym}{float(tp_price):,.2f} | P&amp;L {sym}{float(pnl):+,.2f}"]
    return "\n".join(lines)


def format_log_alert(level: str, logger_name: str, message: str) -> str:
    label = "AUCTION/LIQUIDITY" if any(x in str(logger_name).lower() for x in ("strategy", "entry_engine", "liquidity")) else "RUNTIME"
    return f"{_html_lib.escape(str(level))} | <b>{label}</b>\n<code>{_html_lib.escape(str(message))}</code>"

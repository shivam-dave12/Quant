"""
telegram/notifier.py — Liquidity-First Telegram Notifier
=========================================================
Report architecture mirrors the decision hierarchy:
  DirectionEngine hunt → Pool target → Flow confirmation → ICT context → Entry → Exit

Public API (all imported by strategy layer):
  send_telegram_message()            — async fire-and-forget delivery
  format_periodic_report()           — 15-min institutional dashboard (includes gate diagnostic)
  format_direction_hunt_alert()      — DirectionEngine high-confidence prediction
  format_post_sweep_verdict()        — post-sweep reversal/continuation decision
  format_conviction_block_alert()    — conviction gate rejection with factor breakdown
  format_pool_gate_alert()           — pool-hit gate action (exit/reverse/continue)
  format_liquidity_trail_update()    — liquidity-trail SL advance
  install_global_telegram_log_handler()
  TelegramLogHandler

Internal helpers (used by controller.py):
  _sanitize_html()

Removed (dead code — never imported by any strategy/controller file):
  format_market_outlook()    — superseded by format_periodic_report()
  format_entry_alert()       — superseded by v9_display.format_entry_alert_v9()
  format_trail_update()      — superseded by format_liquidity_trail_update()
  format_position_close()    — superseded by v9_display
  format_pool_sweep_alert()  — superseded by v9_display

Bug fixes applied:
  BUG-W1: format_conviction_block_alert pool_sig weight 0.25 → 0.20 (matches conviction_filter.py)
  BUG-W2: format_conviction_block_alert CISD weight 0.20 → 0.25 (matches conviction_filter.py)
  BUG-T1: format_conviction_block_alert required threshold 0.55 → 0.65 (matches REQUIRED_SCORE default)
  NEW-G1: _build_gate_diagnostic() — 6-gate status panel added to every periodic report
  NEW-A1: format_conviction_blocked alias → format_conviction_block_alert (defensive)
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

# ── Import REQUIRED_SCORE from the authoritative source so this file can
#    never drift out of sync with conviction_filter.py.
try:
    from strategy.conviction_filter import REQUIRED_SCORE as _REQUIRED_CONVICTION_SCORE
except ImportError:
    try:
        from conviction_filter import REQUIRED_SCORE as _REQUIRED_CONVICTION_SCORE
    except ImportError:
        _REQUIRED_CONVICTION_SCORE = 0.45   # matches conviction_filter.py / config.py

# ── DirectionEngine confidence thresholds (mirrored from direction_engine.py)
_HUNT_ON_THRESHOLD  = 0.10   # must exceed to commit a direction
_HUNT_OFF_THRESHOLD = 0.05   # must drop below to revert to NEUTRAL


# ======================================================================
# ASYNC SEND WORKER
# ======================================================================

_send_queue:    _queue_mod.Queue = _queue_mod.Queue(maxsize=200)
_worker_started: bool            = False
_worker_lock:   threading.Lock   = threading.Lock()
_MIN_INTERVAL   = 1.2    # slightly above Telegram's 1-msg/sec guideline
_MAX_RETRIES    = 4      # covers 429 + one 502 retry


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
                url       = (f"https://api.telegram.org/bot"
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
                resp         = _req.post(url, json=payload, timeout=15)
                last_send_ts = time.time()

                if resp.status_code == 200:
                    break

                # HTML parse error → retry as plain text (once)
                if resp.status_code == 400 and parse_mode == "HTML" and attempt == 0:
                    logger.warning(
                        "Telegram HTML parse error — retrying as plain text: "
                        f"{resp.text[:120]}"
                    )
                    plain = re.sub(r"<[^>]+>", "", send_text, flags=re.DOTALL)
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
                    break

                # Rate-limit / transient errors — exponential backoff
                if resp.status_code in (429, 500, 502, 503) and attempt < _MAX_RETRIES - 1:
                    if resp.status_code == 429:
                        try:
                            backoff = max(
                                float(resp.json().get("parameters", {}).get("retry_after", 10)),
                                5.0,
                            )
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
    """Enqueue a Telegram message for async delivery.  Never blocks the caller."""
    if not telegram_config.TELEGRAM_ENABLED:
        return False
    _ensure_worker_started()
    try:
        _send_queue.put_nowait((message, parse_mode))
        return True
    except _queue_mod.Full:
        logger.warning("Telegram queue full — dropping message")
        return False


# ======================================================================
# HTML SANITIZER  (also imported by controller.py)
# ======================================================================

def _sanitize_html(text: str) -> str:
    """
    Remove or escape HTML constructs that Telegram's parse_mode=HTML rejects.
    Telegram supports only: <b> <i> <u> <s> <code> <pre> <a href> <tg-spoiler>
    and their closing counterparts.
    """
    text = re.sub(r"<br\s*/?>",  "\n",          text, flags=re.IGNORECASE)
    text = re.sub(r"<hr\s*/?>",  "\n──────\n",   text, flags=re.IGNORECASE)
    text = re.sub(r"<p\s*/?>",   "\n",           text, flags=re.IGNORECASE)
    text = re.sub(r"</p>",        "",             text, flags=re.IGNORECASE)

    _SAFE_TAGS = {
        "b", "/b", "i", "/i", "u", "/u", "s", "/s",
        "code", "/code", "pre", "/pre",
        "tg-spoiler", "/tg-spoiler",
    }

    def _fix_tag(m: re.Match) -> str:
        inner    = m.group(1).strip()
        if inner.lower().startswith("a ") or inner.lower() == "/a":
            return m.group(0)
        tag_name = inner.lower().split()[0] if inner else ""
        return m.group(0) if tag_name in _SAFE_TAGS else ""

    text = re.sub(r"<([^>]*)>", _fix_tag, text)

    _SAFE_RE = re.compile(
        r"<(?=/?(?:b|i|u|s|code|pre|tg-spoiler|a)(?:[\s>\"/]|$))",
        re.IGNORECASE,
    )
    parts = text.split("<")
    if len(parts) > 1:
        rebuilt = [parts[0]]
        for part in parts[1:]:
            rebuilt.append("<" if _SAFE_RE.match("<" + part) else "&lt;")
            rebuilt.append(part)
        text = "".join(rebuilt)

    return re.sub(r"\n{3,}", "\n\n", text)


def _esc(s: Any) -> str:
    """HTML-escape a value for safe Telegram HTML output."""
    if s is None:
        return ""
    return _html_lib.escape(str(s), quote=False)


# ======================================================================
# UTILITY HELPERS
# ======================================================================

def _fmt_price(p: Optional[float]) -> str:
    if p is None:
        return "—"
    return f"${p:,.1f}"


def _fmt_pct(v: float) -> str:
    return f"{v:.2f}%"


def _time_ago(ts_ms: int) -> str:
    if not ts_ms:
        return "?"
    elapsed = (time.time() * 1000 - ts_ms) / 1000
    if elapsed < 60:
        return f"{elapsed:.0f}s ago"
    if elapsed < 3600:
        return f"{elapsed/60:.0f}m ago"
    return f"{elapsed/3600:.1f}h ago"


def _flow_bar(conviction: float) -> str:
    """Visual signed flow bar for periodic report headers."""
    n = max(0, min(5, int(abs(conviction) * 5)))
    if conviction > 0.05:
        return "▁" * (5 - n) + "▓" * n + " ▲"
    if conviction < -0.05:
        return "▓" * n + "▁" * (5 - n) + " ▼"
    return "▁▁▁▁▁ ─"


def _session_icon(session: str) -> str:
    return {"asia": "🌙", "london": "🌅", "ny": "🏛️", "late_ny": "🌇"}.get(
        (session or "").lower().replace(" ", "_"), "⚪"
    )


def _score_bar(value: float, width: int = 10) -> str:
    """Filled progress bar scaled to [0, 1]."""
    filled = min(width, int(max(0.0, min(1.0, value)) * width))
    return "█" * filled + "░" * (width - filled)


def _signed_bar(value: float, half: int = 5) -> str:
    """Signed bar: negative fills left half, positive fills right half."""
    mag = min(half, int(abs(value) * half + 0.5))
    if value >= 0:
        return "░" * half + "█" * mag + "░" * (half - mag)
    return "░" * (half - mag) + "█" * mag + "░" * half


# ======================================================================
# GATE DIAGNOSTIC — internal builder for format_periodic_report
# ======================================================================

def _build_gate_diagnostic(
    direction_hunt:   Optional[Any],
    amd_phase:        str,
    dealing_range_pd: float,
    session:          str,
    flow_conviction:  float,
    flow_direction:   str,
    htf_bias:         str,
) -> List[str]:
    """
    Build the 🚦 ENTRY GATE STATUS section for periodic reports.

    Shows the six institutional gates with ✅/⚠️/❌, current value,
    required threshold, and a plain-English diagnostic reason.  Each gate
    mirrors the real decision logic in direction_engine.py and
    conviction_filter.py so the operator can immediately see what must
    change before the bot takes a trade.

    Gate 1  — DirectionEngine confidence (0.18 ON / 0.11 OFF Schmitt trigger)
    Gate 2  — AMD phase (MANIPULATION/DISTRIBUTION = active; ACCUMULATION ≈ 0)
    Gate 3  — Dealing Range P/D% (LONG discount <45%; SHORT premium >55%)
    Gate 4  — Session quality (score and killzone flag)
    Gate 5  — Flow conviction (directional order-flow alignment)
    Gate 6  — DirectionEngine 10-factor breakdown (when factors are available)
    """
    lines: List[str] = []
    lines.append("🚦 <b>ENTRY GATE STATUS</b>")

    # ── Gate 1: DirectionEngine confidence ──────────────────────────────────
    if direction_hunt is not None:
        dh_conf  = float(getattr(direction_hunt, "confidence", 0.0))
        dh_pred  = getattr(direction_hunt, "predicted", None)
        dh_bsl   = float(getattr(direction_hunt, "bsl_score", 0.0))
        dh_ssl   = float(getattr(direction_hunt, "ssl_score", 0.0))

        if dh_conf >= _HUNT_ON_THRESHOLD:
            g1_icon = "✅"
            g1_note = f"hunting {dh_pred or '?'}"
        elif dh_conf >= _HUNT_OFF_THRESHOLD:
            g1_icon = "⚠️"
            g1_note = f"hysteresis band — last={dh_pred or 'NEUTRAL'}"
        else:
            g1_icon = "❌"
            g1_note = f"NEUTRAL — need {_HUNT_ON_THRESHOLD:.0%} (BSL={dh_bsl:.3f} SSL={dh_ssl:.3f})"

        lines.append(
            f"  {g1_icon} DirectionEngine   conf={dh_conf:.1%}  "
            f"threshold={_HUNT_ON_THRESHOLD:.0%}  [{g1_note}]"
        )
    else:
        lines.append("  ⚪ DirectionEngine   no data")

    # ── Gate 2: AMD phase ────────────────────────────────────────────────────
    _AMD_SCORE_MAP = {
        "MANIPULATION":   1.00,
        "DISTRIBUTION":   0.90,
        "REDISTRIBUTION": 0.75,
        "REACCUMULATION": 0.70,
        "ACCUMULATION":   0.00,
    }
    phase_upper = (amd_phase or "").upper()
    amd_score   = _AMD_SCORE_MAP.get(phase_upper, 0.30)
    if amd_score >= 0.70:
        g2_icon = "✅"
        g2_note = "active delivery phase"
    elif amd_score >= 0.30:
        g2_icon = "⚠️"
        g2_note = f"weak phase — score={amd_score:.2f}"
    else:
        g2_icon = "❌"
        g2_note = "ACCUMULATION — no institutional delivery expected"

    lines.append(
        f"  {g2_icon} AMD Phase         {_esc(amd_phase or 'UNKNOWN')}  "
        f"score={amd_score:.2f}×0.05  [{g2_note}]"
    )

    # ── Gate 3: Dealing Range ────────────────────────────────────────────────
    pd_pct   = dealing_range_pd * 100
    pd_label = (
        "DEEP DISCOUNT" if dealing_range_pd < 0.25 else
        "DISCOUNT"      if dealing_range_pd < 0.40 else
        "EQUILIBRIUM"   if dealing_range_pd < 0.60 else
        "PREMIUM"       if dealing_range_pd < 0.75 else
        "DEEP PREMIUM"
    )
    long_ok  = dealing_range_pd < 0.65
    short_ok = dealing_range_pd > 0.35

    if long_ok and short_ok:
        g3_icon = "✅"
        g3_note = "both sides valid"
    elif long_ok:
        g3_icon = "✅"
        g3_note = "LONG valid (discount)"
    elif short_ok:
        g3_icon = "✅"
        g3_note = "SHORT valid (premium)"
    else:
        g3_icon = "⚠️"
        g3_note = "equilibrium zone — OTE score reduced"

    lines.append(
        f"  {g3_icon} Dealing Range     {pd_label} ({pd_pct:.0f}%)  "
        f"[{g3_note}]"
    )

    # ── Gate 4: Session quality ──────────────────────────────────────────────
    _SESSION_SCORE_MAP = {
        "LONDON":    1.00,
        "NY":        1.00,
        "NEW_YORK":  1.00,
        "LONDON_NY": 0.95,
        "WEEKEND":   0.80,
        "OFF_HOURS": 0.75,
        "ASIA":      0.60,
        "":          0.60,
    }
    sess_key   = (session or "").upper().replace(" ", "_")
    sess_score = _SESSION_SCORE_MAP.get(sess_key, 0.40)

    if sess_score >= 0.85:
        g4_icon = "✅"
        g4_note = "institutional killzone"
    elif sess_score >= 0.55:
        g4_icon = "⚠️"
        g4_note = f"reduced quality (score={sess_score:.2f}×0.10={sess_score*0.10:.3f})"
    else:
        g4_icon = "❌"
        g4_note = f"very low quality (score={sess_score:.2f}×0.10={sess_score*0.10:.3f})"

    lines.append(
        f"  {g4_icon} Session           {_esc(session or 'UNKNOWN')}  "
        f"qual={sess_score:.2f}  [{g4_note}]"
    )

    # ── Gate 5: Flow conviction ──────────────────────────────────────────────
    flow_abs = abs(flow_conviction)
    if flow_abs >= 0.30:
        g5_icon = "✅"
        g5_note = f"{_esc(flow_direction.upper() or '?')} ({flow_conviction:+.3f})"
    elif flow_abs >= 0.10:
        g5_icon = "⚠️"
        g5_note = f"weak {_esc(flow_direction or 'neutral')} ({flow_conviction:+.3f})"
    else:
        g5_icon = "❌"
        g5_note = "NEUTRAL — no directional order-flow"

    lines.append(
        f"  {g5_icon} Flow Conviction   {_flow_bar(flow_conviction)}  [{g5_note}]"
    )

    # ── Gate 6: DirectionEngine 10-factor breakdown ──────────────────────────
    if direction_hunt is not None:
        dh_factors = getattr(direction_hunt, "factors", None)
        if dh_factors is not None:
            _FACTOR_LABELS = [
                ("amd",            "AMD       "),
                ("htf_structure",  "HTF struct"),
                ("dealing_range",  "Deal range"),
                ("order_flow",     "Order flow"),
                ("pool_asymmetry", "Pool asym "),
                ("ob_fvg_pull",    "OB/FVG    "),
                ("displacement",   "Displacmnt"),
                ("session",        "Session   "),
                ("micro_bos",      "Micro BOS "),
                ("volume",         "Volume    "),
            ]
            lines.append("")
            lines.append("  <b>📐 10-FACTOR SCORES</b>  (+ = BSL hunt, − = SSL hunt)")
            for attr, label in _FACTOR_LABELS:
                val  = float(getattr(dh_factors, attr, 0.0))
                bar  = _signed_bar(val, half=4)
                sign = "+" if val >= 0 else ""
                icon = "🟢" if val > 0.15 else ("🔴" if val < -0.15 else "⚪")
                lines.append(f"    {icon} {label} [{bar}] {sign}{val:.2f}")

    # ── Primary blocker summary ──────────────────────────────────────────────
    blockers: List[str] = []
    if direction_hunt is not None:
        dh_conf = float(getattr(direction_hunt, "confidence", 0.0))
        if dh_conf < _HUNT_ON_THRESHOLD:
            blockers.append(f"DirectionEngine conf={dh_conf:.1%}<{_HUNT_ON_THRESHOLD:.0%}")
    if amd_score < 0.30:
        blockers.append(f"AMD={_esc(amd_phase)}(score={amd_score:.2f})")
    if not (long_ok or short_ok):
        blockers.append(f"DR={_esc(pd_label)}({pd_pct:.0f}%)")
    if sess_score < 0.55:
        blockers.append(f"Session={_esc(session)}(score={sess_score:.2f})")
    if flow_abs < 0.10:
        blockers.append("Flow=NEUTRAL")

    if blockers:
        lines.append("")
        lines.append(f"  🔴 <b>Primary blockers:</b> {' | '.join(blockers)}")
    else:
        lines.append("")
        lines.append("  🟢 <b>All macro gates clear</b> — waiting for sweep + displacement")

    return lines


# ======================================================================
# 1. PERIODIC STATUS REPORT  (15-min institutional dashboard)
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
    # Pool state
    n_bsl_pools:         int   = 0,
    n_ssl_pools:         int   = 0,
    primary_target_str:  str   = "—",
    flow_conviction:     float = 0.0,
    flow_direction:      str   = "",
    # ICT
    amd_phase:           str   = "UNKNOWN",
    session:             str   = "REGULAR",
    in_killzone:         bool  = False,
    regime:              str   = "UNKNOWN",
    # Position
    position:            Optional[Dict] = None,
    current_sl:          Optional[float] = None,
    current_tp:          Optional[float] = None,
    entry_price:         Optional[float] = None,
    breakeven_moved:     bool  = False,
    profit_locked_pct:   float = 0.0,
    extra_lines:         Optional[List[str]] = None,
    # v10 extended
    atr:                 float = 0.0,
    htf_bias:            str   = "",
    dealing_range_pd:    float = 0.5,
    structure_15m:       str   = "",
    structure_4h:        str   = "",
    amd_bias:            str   = "",
    nearest_bsl:         Optional[Dict] = None,
    nearest_ssl:         Optional[Dict] = None,
    sweep_analysis:      Optional[Dict] = None,
    # DirectionEngine state
    direction_hunt:        Optional[Any] = None,
    direction_ps_analysis: Optional[Any] = None,
    **_kwargs: Any,
) -> str:
    """
    15-minute institutional Telegram dashboard.

    Layout:
      Header (time / price / balance / PnL)
      Market Structure (AMD / HTF / Dealing Range / Regime)
      Liquidity Map (BSL/SSL counts, nearest pools, sweep analysis)
      Hunt Prediction (DirectionEngine snapshot)
      Post-Sweep Verdict (when in post-sweep mode)
      🚦 ENTRY GATE STATUS  ← diagnostic section added by _build_gate_diagnostic()
      Position (when in trade)
      Performance
      Extra lines (cost engine, expectancy, ATR, VWAP)
    """
    ist_tz  = timezone(timedelta(hours=5, minutes=30))
    now_ist = datetime.now(ist_tz).strftime("%H:%M IST")
    now_utc = datetime.now(timezone.utc).strftime("%H:%M UTC")

    sess_icon = _session_icon(session)
    kz_str    = " 🔥KZ" if in_killzone else ""
    pnl_icon  = "🟢" if daily_pnl >= 0 else "🔴"

    _STATE_ICONS = {
        "SCANNING":   "🔍",
        "TRACKING":   "📡",
        "READY":      "🎯",
        "ENTERING":   "⚡",
        "IN_POSITION":"📊",
        "POST_SWEEP": "🌊",
    }
    state_icon = _STATE_ICONS.get(bot_state.upper(), "⚪")

    _PD_LABEL = (
        "DEEP DISC"  if dealing_range_pd < 0.25 else
        "DISCOUNT"   if dealing_range_pd < 0.40 else
        "EQ"         if dealing_range_pd < 0.60 else
        "PREMIUM"    if dealing_range_pd < 0.75 else
        "DEEP PREM"
    )

    lines: List[str] = [
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"📊 <b>STATUS</b>  {now_ist} / {now_utc}",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
    ]

    # ── Price + Balance ──────────────────────────────────────────────────────
    lines.append(f"💰 BTC: <b>{_fmt_price(current_price)}</b>")
    atr_part = f"  ATR: {_fmt_price(atr)}" if atr > 0 else ""
    lines.append(f"  💵 Bal: {_fmt_price(balance)}{atr_part}")
    lines.append(
        f"  {pnl_icon} Day: <b>{_fmt_price(daily_pnl)}</b>  |  Total: {_fmt_price(total_pnl)}"
    )

    # ── State + Session ──────────────────────────────────────────────────────
    lines.append("")
    lines.append(f"{state_icon} <b>{_esc(bot_state)}</b>  {sess_icon} {_esc(session)}{kz_str}")

    # ── Market Structure ─────────────────────────────────────────────────────
    lines.append("")
    lines.append("🏛️ <b>MARKET STRUCTURE</b>")
    lines.append(f"  AMD: {_esc(amd_phase)} ({_esc(amd_bias)})")

    htf_parts: List[str] = []
    if structure_4h:
        htf_parts.append(f"4H:{_esc(structure_4h)}")
    if structure_15m:
        htf_parts.append(f"15m:{_esc(structure_15m)}")
    if htf_bias:
        htf_parts.append(f"HTF:{_esc(htf_bias)}")
    if htf_parts:
        lines.append(f"  {' | '.join(htf_parts)}")

    lines.append(f"  Dealing range: {_PD_LABEL} ({dealing_range_pd:.0%})")
    lines.append(f"  Regime: {_esc(regime)}")

    # ── Liquidity Map ────────────────────────────────────────────────────────
    lines.append("")
    lines.append("🎯 <b>LIQUIDITY</b>")
    lines.append(f"  BSL ▲ {n_bsl_pools} pools  |  SSL ▼ {n_ssl_pools} pools")

    if nearest_bsl:
        bsl_p   = nearest_bsl.get("price", 0)
        bsl_d   = nearest_bsl.get("dist_atr", 0)
        bsl_sig = nearest_bsl.get("significance", 0)
        bsl_tf  = nearest_bsl.get("timeframe", "")
        lines.append(
            f"  ▲ Nearest BSL: {_fmt_price(bsl_p)} "
            f"({bsl_d:.1f}ATR sig={bsl_sig:.0f} {bsl_tf})"
        )
    if nearest_ssl:
        ssl_p   = nearest_ssl.get("price", 0)
        ssl_d   = nearest_ssl.get("dist_atr", 0)
        ssl_sig = nearest_ssl.get("significance", 0)
        ssl_tf  = nearest_ssl.get("timeframe", "")
        lines.append(
            f"  ▼ Nearest SSL: {_fmt_price(ssl_p)} "
            f"({ssl_d:.1f}ATR sig={ssl_sig:.0f} {ssl_tf})"
        )

    if primary_target_str and primary_target_str != "—":
        lines.append(f"  🎯 Target: {_esc(primary_target_str)}")

    # ── Sweep Analysis ───────────────────────────────────────────────────────
    if sweep_analysis:
        rs        = float(sweep_analysis.get("reversal_score",     0))
        cs        = float(sweep_analysis.get("continuation_score", 0))
        rr        = sweep_analysis.get("reversal_reasons",     [])
        cr        = sweep_analysis.get("continuation_reasons", [])
        sw_side   = sweep_analysis.get("sweep_side",  "?")
        sw_price  = sweep_analysis.get("sweep_price", 0)
        winner    = (
            "REVERSAL"     if rs > cs + 15 else
            "CONTINUATION" if cs > rs + 15 else
            "UNDECIDED"
        )
        lines.append("")
        lines.append(
            f"🌊 <b>SWEEP ANALYSIS</b> ({_esc(sw_side)} @ {_fmt_price(sw_price)})"
        )
        lines.append(f"  REV: {rs:.0f}  |  CONT: {cs:.0f}  →  <b>{winner}</b>")
        if rr:
            lines.append(f"  Rev:  {', '.join(str(r) for r in rr[:3])}")
        if cr:
            lines.append(f"  Cont: {', '.join(str(r) for r in cr[:3])}")

    # ── DirectionEngine — Hunt Prediction snapshot ───────────────────────────
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

    # ── DirectionEngine — Post-Sweep Verdict snapshot ────────────────────────
    if direction_ps_analysis is not None:
        ps_action = getattr(direction_ps_analysis, "action",         "?")
        ps_dir    = getattr(direction_ps_analysis, "direction",       "")
        ps_conf   = float(getattr(direction_ps_analysis, "confidence", 0.0))
        ps_phase  = getattr(direction_ps_analysis, "phase",           "")
        ps_rev    = float(getattr(direction_ps_analysis, "rev_score",  0.0))
        ps_cont   = float(getattr(direction_ps_analysis, "cont_score", 0.0))
        ps_cisd   = getattr(direction_ps_analysis, "cisd_active",    False)
        ps_ote    = getattr(direction_ps_analysis, "ote_active",     False)
        ps_disp   = float(getattr(direction_ps_analysis, "displacement_atr", 0.0))

        ps_winner = (
            "REVERSAL"     if ps_rev  > ps_cont + 15 else
            "CONTINUATION" if ps_cont > ps_rev  + 15 else
            "CONTESTED"
        )
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
        lines.append(f"  REV={ps_rev:.1f}  CONT={ps_cont:.1f}  → {ps_winner}")

        ps_flags: List[str] = []
        if ps_cisd:
            ps_flags.append("CISD✓")
        if ps_ote:
            ps_flags.append("OTE✓")
        if ps_disp > 0:
            ps_flags.append(f"disp={ps_disp:.2f}ATR")
        if ps_flags:
            lines.append("  " + "  ".join(ps_flags))

    # ── 🚦 ENTRY GATE STATUS ─────────────────────────────────────────────────
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

    # ── Position ─────────────────────────────────────────────────────────────
    if position:
        side    = position.get("side", "?").upper()
        p_entry = entry_price or position.get("entry_price", 0)
        qty     = float(position.get("quantity", 0) or 0)

        side_icon = "🟢" if side == "LONG" else "🔴"
        lines.append("")
        lines.append(f"{side_icon} <b>POSITION: {side}</b>")
        lines.append(f"  Entry: {_fmt_price(p_entry)}")

        if current_sl:
            sl_dist = abs(current_price - current_sl) / max(atr, 1) if atr > 0 else 0
            lines.append(f"  SL: {_fmt_price(current_sl)} ({sl_dist:.1f}ATR)")
        if current_tp:
            tp_dist = abs(current_tp - current_price) / max(atr, 1) if atr > 0 else 0
            lines.append(f"  TP: {_fmt_price(current_tp)} ({tp_dist:.1f}ATR)")

        if p_entry and current_price:
            move   = (
                (current_price - p_entry) if side == "LONG"
                else (p_entry - current_price)
            )
            risk_d = abs(p_entry - current_sl) if current_sl else 0
            ur_r   = move / risk_d if risk_d > 0 else 0
            upnl   = move * qty if qty > 0 else move
            icon   = "🟢" if move >= 0 else "🔴"

            # Progress bar toward TP
            bar = "░" * 16
            prog = 0.0
            if current_tp:
                total = abs(current_tp - p_entry)
                if total > 0:
                    prog  = min(1.0, max(0.0, abs(current_price - p_entry) / total))
                    if move < 0:
                        prog = 0.0
                    filled = int(prog * 16)
                    bar    = "█" * filled + "░" * (16 - filled)

            if qty > 0:
                lines.append(f"  {icon} <b>${upnl:+.2f}</b> ({ur_r:+.2f}R)")
            else:
                lines.append(f"  {icon} {move:+.1f}pts ({ur_r:+.2f}R)")
            lines.append(f"  [{bar}] {prog*100:.0f}%→TP")

        if breakeven_moved:
            lines.append(f"  🔒 BE locked | {profit_locked_pct:.1f}R secured")

    # ── Performance ──────────────────────────────────────────────────────────
    lines.append("")
    lines.append("📈 <b>PERFORMANCE</b>")
    lines.append(f"  Trades: {total_trades}  |  WR: {win_rate:.1f}%")
    if consecutive_losses > 0:
        lines.append(f"  ⚠️ Consecutive losses: {consecutive_losses}")

    # ── Extra lines (cost engine, expectancy, VWAP) ──────────────────────────
    if extra_lines:
        lines.append("")
        for el in extra_lines:
            if el and el.strip():
                lines.append(el)

    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    return "\n".join(lines)


# ======================================================================
# 2. DIRECTION ENGINE — HUNT PREDICTION ALERT
# ======================================================================

def format_direction_hunt_alert(
    predicted:           Optional[str],
    confidence:          float,
    delivery_direction:  str,
    raw_score:           float,
    bsl_score:           float,
    ssl_score:           float,
    reason:              str   = "",
    dealing_range_pd:    float = 0.5,
    swept_pool_price:    Optional[float] = None,
    opposing_pool_price: Optional[float] = None,
    current_price:       float = 0.0,
    amd_phase:           str   = "",
    htf_bias:            str   = "",
    session:             str   = "",
    in_killzone:         bool  = False,
    factors:             Optional[Any] = None,
) -> str:
    """
    DirectionEngine high-confidence hunt prediction alert.
    Sent when the 10-factor model crosses _HUNT_ON_THRESHOLD (18%).
    """
    hunt_icon  = "🔵" if predicted == "BSL" else ("🟠" if predicted == "SSL" else "⚪")
    deliv_icon = "🟢" if delivery_direction == "bullish" else (
        "🔴" if delivery_direction == "bearish" else "⚪"
    )
    kz_str = " 🔥KZ" if in_killzone else ""
    pd_label = (
        "DEEP DISC" if dealing_range_pd < 0.25 else
        "DISCOUNT"  if dealing_range_pd < 0.40 else
        "EQ"        if dealing_range_pd < 0.60 else
        "PREMIUM"   if dealing_range_pd < 0.75 else
        "DEEP PREM"
    )

    lines: List[str] = [
        f"{hunt_icon} <b>HUNT PREDICTION: {_esc(predicted or 'NEUTRAL')}</b>",
        f"  {deliv_icon} Delivery: <b>{_esc(delivery_direction or '—')}</b>",
        "",
        "<b>📊 CONFIDENCE</b>",
        f"  [{_score_bar(confidence)}] {confidence:.0%}",
        f"  BSL score: {bsl_score:.3f}  |  SSL score: {ssl_score:.3f}",
        f"  Raw (±1): {raw_score:+.3f}",
    ]

    if factors is not None:
        _FACTOR_LABELS = [
            ("amd",            "AMD phase  "),
            ("htf_structure",  "HTF struct "),
            ("dealing_range",  "Deal range "),
            ("order_flow",     "Order flow "),
            ("pool_asymmetry", "Pool asym  "),
            ("ob_fvg_pull",    "OB/FVG pull"),
            ("displacement",   "Displacemnt"),
            ("session",        "Session    "),
            ("micro_bos",      "Micro BOS  "),
            ("volume",         "Volume     "),
        ]
        lines.append("")
        lines.append("<b>🔢 10-FACTOR BREAKDOWN</b>")
        for attr, label in _FACTOR_LABELS:
            val  = float(getattr(factors, attr, 0.0))
            sign = "+" if val >= 0 else ""
            lines.append(f"  {label}  [{_signed_bar(val)}] {sign}{val:.2f}")

    lines += [
        "",
        "<b>🎯 TARGETS</b>",
    ]
    if swept_pool_price:
        lines.append(f"  Hunt target (sweep):  {_fmt_price(swept_pool_price)}")
    if opposing_pool_price:
        lines.append(f"  Delivery target (TP): {_fmt_price(opposing_pool_price)}")
    if current_price:
        lines.append(f"  Current price:        {_fmt_price(current_price)}")

    lines += [
        "",
        "<b>🏛️ CONTEXT</b>",
        f"  Dealing range: {pd_label} ({dealing_range_pd:.0%})",
    ]
    if amd_phase:
        lines.append(f"  AMD: {_esc(amd_phase)}")
    if htf_bias:
        lines.append(f"  HTF bias: {_esc(htf_bias)}")
    if session:
        lines.append(f"  Session: {_esc(session)}{kz_str}")
    if reason:
        lines += ["", "<b>💡 REASON</b>", f"  {_esc(reason[:300])}"]

    return "\n".join(lines)


# ======================================================================
# 3. DIRECTION ENGINE — POST-SWEEP VERDICT
# ======================================================================

def format_post_sweep_verdict(
    action:           str,
    direction:        str,
    confidence:       float,
    phase:            str,
    cisd_active:      bool  = False,
    ote_active:       bool  = False,
    displacement_atr: float = 0.0,
    rev_score:        float = 0.0,
    cont_score:       float = 0.0,
    rev_reasons:      Optional[List[str]] = None,
    cont_reasons:     Optional[List[str]] = None,
    reason:           str   = "",
    swept_pool_price: Optional[float] = None,
    swept_pool_type:  str   = "",
    current_price:    float = 0.0,
) -> str:
    """
    Post-sweep accumulative evidence verdict.
    Sent when action is 'reverse' or 'continue' (not 'wait').
    """
    rev_reasons  = rev_reasons  or []
    cont_reasons = cont_reasons or []

    action_icons = {"reverse": "🔄", "continue": "➡️", "wait": "⏳"}
    action_icon  = action_icons.get(action.lower(), "❓")
    dir_icon     = "🟢" if direction.lower() == "long" else (
        "🔴" if direction.lower() == "short" else "⚪"
    )
    phase_icons = {
        "DISPLACEMENT": "⚡",
        "CISD":         "🔀",
        "OTE":          "🎯",
        "MATURE":       "📐",
    }
    phase_icon = phase_icons.get(phase.upper(), "📍")

    total = rev_score + cont_score
    rev_pct  = rev_score  / total if total > 0 else 0.0
    cont_pct = cont_score / total if total > 0 else 0.0

    winner = (
        "REVERSAL"     if rev_score  > cont_score + 15 else
        "CONTINUATION" if cont_score > rev_score  + 15 else
        "CONTESTED"
    )

    lines: List[str] = [
        f"{action_icon} <b>POST-SWEEP: {_esc(action.upper())}</b>  "
        f"{dir_icon} {_esc(direction.upper() or '—')}",
        f"  [{_score_bar(confidence)}] {confidence:.0%}  "
        f"|  {phase_icon} Phase: <b>{_esc(phase)}</b>",
    ]

    if swept_pool_price or swept_pool_type:
        ctx = f"  Swept: {_esc(swept_pool_type)} @ {_fmt_price(swept_pool_price)}"
        if current_price:
            ctx += f"  |  Now: {_fmt_price(current_price)}"
        lines.append(ctx)

    flags: List[str] = []
    if cisd_active:
        flags.append("🔀 CISD confirmed")
    if ote_active:
        flags.append("🎯 OTE reached")
    if displacement_atr > 0:
        flags.append(f"⚡ Disp {displacement_atr:.2f}ATR")
    if flags:
        lines.append("  " + "  |  ".join(flags))

    lines += [
        "",
        "<b>⚖️ EVIDENCE SCOREBOARD</b>",
        f"  REV  [{_score_bar(rev_pct)}] {rev_score:.1f}",
        f"  CONT [{_score_bar(cont_pct)}] {cont_score:.1f}",
        f"  → <b>{winner}</b>",
    ]

    if rev_reasons:
        lines.append(f"  Rev:  {', '.join(_esc(r) for r in rev_reasons[:4])}")
    if cont_reasons:
        lines.append(f"  Cont: {', '.join(_esc(r) for r in cont_reasons[:4])}")
    if reason:
        lines += ["", "<b>💡 REASON</b>", f"  {_esc(reason[:300])}"]

    return "\n".join(lines)


# ======================================================================
# 4. DIRECTION ENGINE — POOL-HIT GATE ALERT
# ======================================================================

def format_pool_gate_alert(
    action:        str,
    confidence:    float,
    reason:        str,
    pos_side:      str,
    pos_entry:     float,
    current_price: float,
    pos_sl:        float = 0.0,
    pos_tp:        float = 0.0,
    next_target:   Optional[float] = None,
    atr:           float = 0.0,
) -> str:
    """
    Pool-hit gate decision alert.
    Sent when action is exit, reverse, or continue (not hold).
    """
    action_icons = {
        "exit":     "🏁",
        "reverse":  "🔄",
        "continue": "➡️",
        "hold":     "🔒",
    }
    action_icon = action_icons.get(action.lower(), "❓")
    side_icon   = "🟢" if pos_side.upper() == "LONG" else "🔴"

    profit = (
        (current_price - pos_entry) if pos_side.upper() == "LONG"
        else (pos_entry - current_price)
    )
    risk  = abs(pos_entry - pos_sl) if pos_sl else 0
    cur_r = profit / risk if risk > 0 else 0.0

    lines: List[str] = [
        f"{action_icon} <b>POOL-GATE: {_esc(action.upper())}</b>  {side_icon} {_esc(pos_side.upper())}",
        f"  [{_score_bar(confidence)}] {confidence:.0%}",
        "",
        "<b>📊 POSITION</b>",
        f"  Entry: {_fmt_price(pos_entry)} | Price: {_fmt_price(current_price)}",
        f"  R now: {cur_r:+.2f}R | Profit: {profit:+.1f}pts",
    ]
    if pos_sl:
        sl_dist = abs(current_price - pos_sl) / max(atr, 1) if atr > 0 else 0
        lines.append(f"  SL: {_fmt_price(pos_sl)} ({sl_dist:.1f}ATR from price)")
    if pos_tp:
        tp_dist = abs(pos_tp - current_price) / max(atr, 1) if atr > 0 else 0
        lines.append(f"  TP: {_fmt_price(pos_tp)} ({tp_dist:.1f}ATR from price)")
    if action.lower() == "continue" and next_target:
        lines += ["", "<b>🎯 NEXT TARGET</b>", f"  {_fmt_price(next_target)}"]

    lines += ["", "<b>💡 REASON</b>", f"  {_esc(reason[:300])}"]
    return "\n".join(lines)


# ======================================================================
# 5. CONVICTION GATE BLOCK ALERT
# ======================================================================

def format_conviction_block_alert(
    side:              str,
    score:             float,
    reject_reasons:    Optional[List[str]] = None,
    allow_reasons:     Optional[List[str]] = None,
    factors:           Any  = None,
    entry_price:       float = 0.0,
    sl_price:          float = 0.0,
    tp_price:          float = 0.0,
    rr_ratio:          float = 0.0,
    blocked_by_timing: bool  = False,
) -> str:
    """
    Conviction gate block alert — sent (throttled 60s/side) every time the
    conviction filter rejects an entry signal.

    Factor weights match conviction_filter.py exactly:
      pool_sig × 0.20  |  displacement × 0.25  |  cisd × 0.25
      ote      × 0.15  |  session      × 0.10  |  amd  × 0.05

    Required threshold = REQUIRED_SCORE imported from conviction_filter.py
    (default 0.65 — correct for crypto 24/7 with crypto-specific calibration).

    BUG-W1 FIX: pool_sig weight was 0.25; correct value is 0.20.
    BUG-W2 FIX: cisd weight was 0.20; correct value is 0.25.
    BUG-T1 FIX: required was hardcoded 0.55; now imported as 0.65.
    """
    reject_reasons = reject_reasons or []
    allow_reasons  = allow_reasons  or []

    side_icon = "🟢" if side.lower() == "long" else "🔴"
    required  = _REQUIRED_CONVICTION_SCORE

    score_pct  = min(1.0, score / required) if required > 0 else 0.0
    score_bar  = _score_bar(score_pct)
    status_icon = "✅" if score >= required else "❌"

    # Header — distinguish timing holds from structural rejections
    if blocked_by_timing and score >= required:
        header_icon = "⏱️"
        header_text = "CONVICTION GATE — TIMING HOLD"
        subtext     = (
            f"  Score: ✅ <b>{score:.3f}</b> / {required:.2f}  "
            f"(setup quality: GOOD)"
        )
    elif blocked_by_timing:
        header_icon = "⏱️"
        header_text = "CONVICTION GATE — TIMING HOLD"
        subtext     = (
            f"  Score: {status_icon} <b>{score:.3f}</b> / {required:.2f} required"
        )
    else:
        header_icon = "🚫"
        header_text = "CONVICTION GATE BLOCKED"
        subtext     = (
            f"  Score: {status_icon} <b>{score:.3f}</b> / {required:.2f} required"
        )

    lines: List[str] = [
        f"{header_icon} <b>{header_text}</b>  {side_icon} {side.upper()}",
        "",
        subtext,
        f"  [{score_bar}]",
    ]

    if entry_price:
        lines.append(
            f"  Entry: {_fmt_price(entry_price)}  "
            f"SL: {_fmt_price(sl_price)}  "
            f"TP: {_fmt_price(tp_price)}  "
            f"R:R: {rr_ratio:.1f}R"
        )

    # Factor breakdown — only when factors were actually computed (any non-zero)
    factors_computed = (
        factors is not None and
        any(
            getattr(factors, a, 0.0) > 0
            for a in (
                "pool_sig_score", "displacement_score", "cisd_score",
                "ote_score", "session_score", "amd_score",
            )
        )
    )

    if factors_computed:
        # Weights match conviction_filter.py lines 434-441 exactly.
        _FACTOR_ROWS = [
            ("Pool sig",     "pool_sig_score",     0.20),
            ("Displacement", "displacement_score", 0.25),
            ("CISD",         "cisd_score",         0.25),
            ("OTE zone",     "ote_score",          0.15),
            ("Session",      "session_score",      0.10),
            ("AMD phase",    "amd_score",          0.05),
        ]
        lines.append("")
        lines.append("<b>📊 FACTOR BREAKDOWN</b>")
        for fname, fattr, fwt in _FACTOR_ROWS:
            fval    = float(getattr(factors, fattr, 0.0))
            fbar    = _score_bar(fval, width=5)
            contrib = fval * fwt
            ficon   = "✅" if fval >= 0.6 else ("⚠️" if fval >= 0.3 else "❌")
            lines.append(
                f"  {ficon} {fname:<14} [{fbar}] {fval:.2f}  "
                f"(×{fwt:.2f} = {contrib:.3f})"
            )

        dr_ok = getattr(factors, "dealing_range_ok", None)
        if dr_ok is not None:
            dr_icon = "✅" if dr_ok else "❌"
            lines.append(
                f"  {dr_icon} Dealing Range   DR data: {'available' if dr_ok else 'unavailable'}"
            )

    elif factors is not None:
        lines.append("")
        lines.append(
            "  <i>Factor scoring skipped — mandatory structural gate failed early</i>"
        )

    # Reject reasons — split timing vs structural
    timing_reasons    = [r for r in reject_reasons if any(
        k in r for k in ("MIN_INTERVAL", "MAX_ENTRIES", "SESSION_INVALIDATED")
    )]
    structural_reasons = [r for r in reject_reasons if r not in timing_reasons]

    if timing_reasons:
        lines.append("")
        lines.append("<b>⏱️ TIMING</b>")
        for r in timing_reasons[:3]:
            lines.append(f"  🕐 {_esc(r[:120])}")

    if structural_reasons:
        lines.append("")
        lines.append("<b>🚫 REJECT REASONS</b>")
        for r in structural_reasons[:5]:
            is_gate = any(k in r for k in (
                "BLOCKED", "GATE", "INVALIDATED",
                "AMD_BLOCKED", "SCORE_TOO_LOW", "SCORE:",
            ))
            icon = "🔴" if is_gate else "⚠️"
            lines.append(f"  {icon} {_esc(r[:120])}")

    if allow_reasons:
        lines.append("")
        lines.append("<b>✅ PASSED</b>")
        for a in allow_reasons[:4]:
            lines.append(f"  ✅ {_esc(a[:80])}")

    return "\n".join(lines)


# Defensive alias — guards against any future import by the old name.
format_conviction_blocked = format_conviction_block_alert


# ======================================================================
# 6. LIQUIDITY TRAIL SL UPDATE ALERT
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
) -> str:
    """
    Liquidity-trail SL advance alert.

    Shows new SL, the pool anchor it was set relative to (swept vs unswept),
    distance metrics in ATR, and R-multiples locked from entry.
    Throttled in quant_strategy to once per 120s.
    """
    side_icon = "🟢" if side.lower() == "long" else "🔴"
    phase_icons = {
        "SWEPT_POOL":   "🏦",
        "UNSWEPT_POOL": "🎯",
        "HOLD":         "⏸",
    }
    phase_icon     = phase_icons.get(phase, "📍")
    anchor_status  = (
        "✅ SWEPT (confirmed S/R)" if is_swept
        else "⚡ UNSWEPT (stop cluster)"
    )
    sess_icons     = {"LONDON": "🇬🇧", "NY": "🇺🇸", "ASIA": "🌏", "": "🌐"}
    sess_icon      = sess_icons.get(session.upper(), "🌐")

    profit_r = 0.0
    if atr > 1e-10:
        profit_r = (
            (new_sl - entry_price) / atr if side.lower() == "long"
            else (entry_price - new_sl) / atr
        )

    dist_to_sl_atr  = abs(current_price - new_sl)   / atr if atr > 1e-10 else 0.0
    dist_anchor_atr = abs(anchor_price  - new_sl)   / atr if atr > 1e-10 else 0.0
    _ = dist_anchor_atr  # kept for future use; suppress unused warning

    lines: List[str] = [
        f"{side_icon} <b>LIQUIDITY TRAIL UPDATE</b>  {phase_icon} {_esc(phase.replace('_', ' '))}",
        "",
        f"  {side_icon} SL moved to: <b>{_fmt_price(new_sl)}</b>",
        f"  📍 Anchor pool: {_fmt_price(anchor_price)} ({_esc(anchor_tf)})  sig={anchor_sig:.1f}",
        f"  {anchor_status}",
        "",
        f"  Entry:  {_fmt_price(entry_price)}",
        f"  Price:  {_fmt_price(current_price)}",
        f"  SL gap: {dist_to_sl_atr:.2f} ATR from price",
        f"  Locked: {profit_r:+.2f} ATR from entry",
        "",
        f"  {sess_icon} Session: {_esc(session or 'unknown')}",
    ]

    if phase == "SWEPT_POOL":
        lines.append("  <i>SL anchored to confirmed institutional support/resistance</i>")
    else:
        lines.append("  <i>SL anchored to unswept stop cluster — structural barrier</i>")

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

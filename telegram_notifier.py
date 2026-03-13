"""
Telegram Notifier v10.1 — Institutional-Grade ICT Reports
============================================================
Every message includes exact price levels so you can verify
structures on chart. Full trade plan visibility.

Report Types:
  1. Market Outlook  — full structure map with trade plan (every 5m)
  2. Entry Alert     — comprehensive entry with full context
  3. Trail Update    — SL move with structure justification
  4. Position Close  — detailed P&L with trade review
  5. Periodic Status — balance/P&L summary
  6. Structure Deep  — /structures command response
"""

import logging
import time
import threading
import requests
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
from collections import deque
import telegram_config
import html as _html_lib


logger = logging.getLogger(__name__)

import queue as _queue_mod

_send_queue: _queue_mod.Queue = _queue_mod.Queue(maxsize=200)
_worker_started = False
_worker_lock    = threading.Lock()
_MIN_INTERVAL   = 1.0
_MAX_RETRIES    = 3

def _sanitize_html(text: str) -> str:
    """
    Remove or escape HTML constructs that Telegram's HTML parse_mode rejects.

    Telegram supports only: <b>, <i>, <u>, <s>, <code>, <pre>, <a href="...">,
    <tg-spoiler>, and their closing counterparts.  Any other tag (including empty
    tags like <> or unknown tags like <br>, <hr>, <p>) raises a 400 parse error.

    Strategy:
      1. Replace <br> / <br/> with a newline (most common culprit).
      2. Strip any remaining unsupported tags while preserving their inner text.
      3. Collapse runs of blank lines to at most two consecutive newlines.
    """
    import re

    # Common safe substitutions
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<hr\s*/?>', '\n──────\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<p\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '', text, flags=re.IGNORECASE)

    # Supported tags — keep these intact
    _SAFE_TAGS = {
        'b', '/b', 'i', '/i', 'u', '/u', 's', '/s',
        'code', '/code', 'pre', '/pre',
        'tg-spoiler', '/tg-spoiler',
    }

    def _fix_tag(m):
        inner = m.group(1).strip()
        # Keep <a href="..."> and </a>
        if inner.lower().startswith('a ') or inner.lower() == '/a':
            return m.group(0)
        # Keep safe tags
        tag_name = inner.lower().split()[0] if inner else ''
        if tag_name in _SAFE_TAGS:
            return m.group(0)
        # Strip unsupported tag (keep its text content if any)
        return ''

    text = re.sub(r'<([^>]*)>', _fix_tag, text)

    # Escape any remaining bare `<` that are NOT the opening of a valid Telegram
    # HTML tag.  These arise from dynamic content such as numeric comparisons
    # ("adx < 5.0", "price < sl"), f-strings with dict reprs ("<class ...>"),
    # or reason strings returned by strategy internals.
    #
    # After the tag-stripping pass above, the only `<` chars that should remain
    # are those starting recognised safe tags.  Anything else must be escaped
    # so Telegram's HTML parser doesn't choke on them.
    #
    # Matches `<` NOT followed by an optional `/` and then a valid tag name:
    #   b, i, u, s, code, pre, tg-spoiler, a (with optional attrs)
    text = re.sub(
        r'<(?!/?(?:b|i|u|s|code|pre|tg-spoiler|a(?:[\s>\/]|$)))(?![^>]*>)',
        r'&lt;',
        text,
        flags=re.IGNORECASE,
    )

    # Collapse excessive blank lines (> 2 consecutive newlines → 2)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text




def _esc(s) -> str:
    """Escape <, >, & for Telegram HTML parse_mode."""
    if s is None:
        return ""
    return _html_lib.escape(str(s), quote=False)


def _send_worker():
    """Background thread that drains the message queue and sends to Telegram.
    
    Retries transient errors (502, 429, 5xx) with exponential backoff.
    Never blocks the main trading loop.
    """
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
            # Rate limit between sends
            now = time.time()
            gap = _MIN_INTERVAL - (now - last_send_ts)
            if gap > 0:
                time.sleep(gap)

            try:
                url = f"https://api.telegram.org/bot{telegram_config.TELEGRAM_BOT_TOKEN}/sendMessage"

                # Sanitize HTML before sending to prevent "Unsupported start tag" 400s
                send_text = message[:4000]
                send_mode = parse_mode
                if parse_mode == "HTML":
                    send_text = _sanitize_html(send_text)

                payload = {
                    "chat_id": telegram_config.TELEGRAM_CHAT_ID,
                    "text": send_text,
                    "parse_mode": send_mode,
                    "disable_web_page_preview": True,
                }
                resp = requests.post(url, json=payload, timeout=15)
                last_send_ts = time.time()

                if resp.status_code == 200:
                    break

                # 400 parse error — retry once without parse_mode (plain text)
                if resp.status_code == 400 and parse_mode == "HTML" and attempt == 0:
                    logger.warning(
                        f"Telegram HTML parse error — retrying as plain text: {resp.text[:120]}"
                    )
                    import re as _re
                    # DOTALL: strip tags that span multiple lines (e.g. logged tracebacks)
                    plain_text = _re.sub(r'<[^>]+>', '', send_text, flags=_re.DOTALL)
                    plain_payload = {
                        "chat_id": telegram_config.TELEGRAM_CHAT_ID,
                        "text": plain_text[:4000],
                        "disable_web_page_preview": True,
                    }
                    resp2 = requests.post(url, json=plain_payload, timeout=15)
                    last_send_ts = time.time()
                    if resp2.status_code == 200:
                        break
                    logger.warning(
                        f"Telegram plain-text fallback also failed: {resp2.status_code}"
                    )
                    break

                # Retryable errors
                if resp.status_code in (429, 500, 502, 503) and attempt < _MAX_RETRIES - 1:
                    backoff = 2.0 * (2 ** attempt)
                    if resp.status_code == 429:
                        # Respect Telegram's retry_after if provided
                        try:
                            retry_after = resp.json().get("parameters", {}).get("retry_after", backoff)
                            backoff = max(backoff, float(retry_after))
                        except Exception:
                            pass
                    logger.warning(f"Telegram {resp.status_code}, retry {attempt+1}/{_MAX_RETRIES} "
                                   f"in {backoff:.1f}s")
                    time.sleep(backoff)
                    continue

                # Non-retryable failure
                logger.warning(f"Telegram send failed: {resp.status_code} — {resp.text[:200]}")
                break

            except requests.exceptions.Timeout:
                if attempt < _MAX_RETRIES - 1:
                    logger.warning(f"Telegram timeout, retry {attempt+1}/{_MAX_RETRIES}")
                    time.sleep(2.0 * (attempt + 1))
                    continue
                logger.error("Telegram send timed out after all retries")
                break
            except Exception as e:
                logger.error(f"Telegram send error: {e}")
                break

        _send_queue.task_done()


def _ensure_worker_started():
    """Start the background worker thread exactly once (thread-safe)."""
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
# CORE SENDER — NON-BLOCKING
# ======================================================================

def send_telegram_message(message: str, parse_mode: str = "HTML") -> bool:
    """Enqueue a Telegram message for async delivery.
    
    Returns True if enqueued, False if queue full or disabled.
    NEVER blocks the calling thread (main trading loop).
    """
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
# UTILITY HELPERS
# ======================================================================

def _fmt_price(p: float) -> str:
    """Format BTC price consistently."""
    if p is None:
        return "—"
    return f"${p:,.1f}"


def _fmt_pct(v: float) -> str:
    """Format percentage."""
    return f"{v:.2f}%"


def _time_ago(ts_ms: int) -> str:
    """Human-readable time ago from millisecond timestamp."""
    if not ts_ms:
        return "?"
    elapsed = (time.time() * 1000 - ts_ms) / 1000
    if elapsed < 60:
        return f"{elapsed:.0f}s ago"
    if elapsed < 3600:
        return f"{elapsed/60:.0f}m ago"
    return f"{elapsed/3600:.1f}h ago"


def _ob_label(ob) -> str:
    """Format order block as concise label with price range."""
    v_tag = "🔴" if getattr(ob, 'visit_count', 0) >= 2 else ("⚪" if getattr(ob, 'visit_count', 0) == 1 else "🟣")
    bos = "✓BOS" if getattr(ob, 'bos_confirmed', False) else ""
    disp = "✓DISP" if getattr(ob, 'has_displacement', False) else ""
    tags = " ".join(filter(None, [bos, disp]))
    return (
        f"{v_tag} {_fmt_price(ob.low)}–{_fmt_price(ob.high)} "
        f"str={getattr(ob, 'strength', 0):.0f} v={getattr(ob, 'visit_count', 0)} "
        f"{tags} {_time_ago(getattr(ob, 'timestamp', 0))}"
    )


def _fvg_label(fvg) -> str:
    """Format FVG with price range and fill %."""
    fill = getattr(fvg, 'fill_percentage', 0) * 100
    return (
        f"{_fmt_price(fvg.bottom)}–{_fmt_price(fvg.top)} "
        f"fill={fill:.0f}% {_time_ago(getattr(fvg, 'timestamp', 0))}"
    )


def _liq_label(pool) -> str:
    """Format liquidity pool with sweep status."""
    swept = "✅SWEPT" if getattr(pool, 'swept', False) else "⏳"
    disp = "+DISP" if getattr(pool, 'displacement_confirmed', False) else ""
    wick = "+WICK" if getattr(pool, 'wick_rejection', False) else ""
    return (
        f"{getattr(pool, 'pool_type', '?')} @ {_fmt_price(pool.price)} "
        f"x{getattr(pool, 'touch_count', 0)} {swept} {disp} {wick}"
    )


def _mss_label(ms) -> str:
    """Format market structure shift."""
    icon = "📈" if getattr(ms, 'direction', '') == "bullish" else "📉"
    return (
        f"{icon} {getattr(ms, 'structure_type', '?')} "
        f"{getattr(ms, 'direction', '?')} [{getattr(ms, 'timeframe', '?')}] "
        f"@ {_fmt_price(getattr(ms, 'price', 0))} {_time_ago(getattr(ms, 'timestamp', 0))}"
    )


# ======================================================================
# 1. MARKET OUTLOOK — The "Thinking" Report
# ======================================================================

def format_market_outlook(
    current_price: float,
    htf_bias: str = "NEUTRAL",
    htf_bias_strength: float = 0.0,
    htf_components: Optional[Dict] = None,
    daily_bias: str = "NEUTRAL",
    regime: str = "UNKNOWN",
    regime_adx: float = 0.0,
    session: str = "REGULAR",
    in_killzone: bool = False,
    amd_phase: str = "UNKNOWN",
    # Dealing ranges with actual prices
    dr_weekly: Optional[Any] = None,
    dr_daily: Optional[Any] = None,
    dr_intraday: Optional[Any] = None,
    dr_zone_tag: str = "",
    # Structures — pass actual objects for price levels
    bullish_obs: Optional[List] = None,
    bearish_obs: Optional[List] = None,
    bullish_fvgs: Optional[List] = None,
    bearish_fvgs: Optional[List] = None,
    liquidity_pools: Optional[List] = None,
    market_structures: Optional[List] = None,
    swing_highs: Optional[List] = None,
    swing_lows: Optional[List] = None,
    # Trade plan
    long_plan: Optional[Dict] = None,
    short_plan: Optional[Dict] = None,
    # Entry eval status
    entry_eval_status: str = "",
    # ── Consolidated stats (merged from periodic report) ──
    balance: float = 0.0,
    total_trades: int = 0,
    win_rate: float = 0.0,
    daily_pnl: float = 0.0,
    total_pnl: float = 0.0,
    consecutive_losses: int = 0,
    bot_state: str = "READY",
    position: Optional[Dict] = None,
    current_sl: Optional[float] = None,
    current_tp: Optional[float] = None,
    entry_price: Optional[float] = None,
    breakeven_moved: bool = False,
    profit_locked_pct: float = 0.0,
    regime_atr_ratio: float = 0.0,
    regime_size_mult: float = 0.0,
    volume_delta: Optional[Dict] = None,
) -> str:
    """
    CONSOLIDATED report — market outlook + bot stats + position.
    Single comprehensive Telegram message replacing both the old
    'market outlook' and 'periodic report' to avoid double notifications.
    """
    bullish_obs = bullish_obs or []
    bearish_obs = bearish_obs or []
    bullish_fvgs = bullish_fvgs or []
    bearish_fvgs = bearish_fvgs or []
    liquidity_pools = liquidity_pools or []
    market_structures = market_structures or []
    swing_highs = swing_highs or []
    swing_lows = swing_lows or []
    htf_components = htf_components or {}

    kz = "🔥 KILLZONE" if in_killzone else "⚪"
    lines = []

    # ── Header ──────────────────────────────────────────
    lines.append("🧠 <b>ICT BOT — MARKET OUTLOOK</b>")
    lines.append(f"⏰ {datetime.now(timezone.utc).strftime('%H:%M UTC')} | {_fmt_price(current_price)}")
    lines.append("")

    # ── Directional Bias ────────────────────────────────
    lines.append(f"<b>📐 BIAS</b>")
    lines.append(f"  HTF: <b>{htf_bias}</b> ({htf_bias_strength:.0%})")
    if htf_components:
        ema_pos = htf_components.get("ema", "?")
        ms_dir = htf_components.get("ms", "?")
        hh_hl = htf_components.get("swing", "?")
        bos_d = htf_components.get("bos", "?")
        lines.append(f"  ↳ EMA34={ema_pos} MS={ms_dir} Pattern={hh_hl} BOS={bos_d}")
    lines.append(f"  Daily: {daily_bias} | Regime: <b>{regime}</b> (ADX {regime_adx:.1f})")
    lines.append(f"  {kz} Session: {session} | AMD: {amd_phase}")
    lines.append("")

    # ── Dealing Ranges with Price Levels ────────────────
    lines.append("<b>📏 DEALING RANGES (IPDA)</b>")
    if dr_weekly:
        w_low = getattr(dr_weekly, 'low', 0)
        w_high = getattr(dr_weekly, 'high', 0)
        w_eq = (w_low + w_high) / 2 if w_low and w_high else 0
        lines.append(f"  Weekly: {_fmt_price(w_low)} — <i>EQ {_fmt_price(w_eq)}</i> — {_fmt_price(w_high)}")
    else:
        lines.append("  Weekly: not formed")
    if dr_daily:
        d_low = getattr(dr_daily, 'low', 0)
        d_high = getattr(dr_daily, 'high', 0)
        d_eq = (d_low + d_high) / 2 if d_low and d_high else 0
        lines.append(f"  Daily:  {_fmt_price(d_low)} — <i>EQ {_fmt_price(d_eq)}</i> — {_fmt_price(d_high)}")
    else:
        lines.append("  Daily:  not formed")
    if dr_intraday:
        i_low = getattr(dr_intraday, 'low', 0)
        i_high = getattr(dr_intraday, 'high', 0)
        i_eq = (i_low + i_high) / 2 if i_low and i_high else 0
        lines.append(f"  Intra:  {_fmt_price(i_low)} — <i>EQ {_fmt_price(i_eq)}</i> — {_fmt_price(i_high)}")
    else:
        lines.append("  Intra:  not formed")
    if dr_zone_tag:
        lines.append(f"  ➡️ Price in: <b>{dr_zone_tag}</b>")
    lines.append("")

    # ── Key Swing Levels ────────────────────────────────
    lines.append("<b>🔀 KEY SWINGS</b>")
    # Nearest highs above price
    highs_above = sorted([s for s in swing_highs if getattr(s, 'price', 0) > current_price],
                         key=lambda s: s.price)[:3]
    lows_below = sorted([s for s in swing_lows if getattr(s, 'price', 0) < current_price],
                        key=lambda s: s.price, reverse=True)[:3]
    if highs_above:
        h_str = " | ".join(f"{_fmt_price(s.price)} [{getattr(s, 'timeframe', '?')}]" for s in highs_above)
        lines.append(f"  ⬆️ Highs above: {h_str}")
    if lows_below:
        l_str = " | ".join(f"{_fmt_price(s.price)} [{getattr(s, 'timeframe', '?')}]" for s in lows_below)
        lines.append(f"  ⬇️ Lows below: {l_str}")
    lines.append("")

    # ── Liquidity Pools ─────────────────────────────────
    active_pools = [p for p in liquidity_pools if not getattr(p, 'swept', False)]
    swept_pools = [p for p in liquidity_pools if getattr(p, 'swept', False)]

    lines.append(f"<b>💧 LIQUIDITY ({len(active_pools)} active / {len(swept_pools)} swept)</b>")
    eqh = [p for p in active_pools if getattr(p, 'pool_type', '') == 'EQH']
    eql = [p for p in active_pools if getattr(p, 'pool_type', '') == 'EQL']
    if eqh:
        for p in sorted(eqh, key=lambda x: x.price)[:3]:
            dist = abs(current_price - p.price) / current_price * 100
            lines.append(f"  🔺 EQH @ {_fmt_price(p.price)} x{p.touch_count} ({_fmt_pct(dist)} away)")
    if eql:
        for p in sorted(eql, key=lambda x: x.price, reverse=True)[:3]:
            dist = abs(current_price - p.price) / current_price * 100
            lines.append(f"  🔻 EQL @ {_fmt_price(p.price)} x{p.touch_count} ({_fmt_pct(dist)} away)")
    if swept_pools:
        for p in swept_pools[-2:]:
            lines.append(f"  ✅ {_liq_label(p)}")
    lines.append("")

    # ── Order Blocks (sorted by proximity) ──────────────
    now_ms = int(time.time() * 1000)
    active_bull_obs = [o for o in bullish_obs if getattr(o, 'is_active', lambda t: True)(now_ms)]
    active_bear_obs = [o for o in bearish_obs if getattr(o, 'is_active', lambda t: True)(now_ms)]

    lines.append(f"<b>📦 ORDER BLOCKS ({len(active_bull_obs)}B / {len(active_bear_obs)}S active)</b>")
    # Bull OBs — sorted by distance to price (closest first)
    for ob in sorted(active_bull_obs, key=lambda o: abs(current_price - o.midpoint))[:3]:
        dist = abs(current_price - ob.midpoint) / current_price * 100
        in_ob = "⚡IN" if ob.contains_price(current_price) else f"{_fmt_pct(dist)}"
        ote = " OTE✓" if ob.in_optimal_zone(current_price) else ""
        lines.append(f"  🟢 {_fmt_price(ob.low)}–{_fmt_price(ob.high)} str={ob.strength:.0f} v={ob.visit_count} [{in_ob}{ote}]")
    for ob in sorted(active_bear_obs, key=lambda o: abs(current_price - o.midpoint))[:3]:
        dist = abs(current_price - ob.midpoint) / current_price * 100
        in_ob = "⚡IN" if ob.contains_price(current_price) else f"{_fmt_pct(dist)}"
        ote = " OTE✓" if ob.in_optimal_zone(current_price) else ""
        lines.append(f"  🔴 {_fmt_price(ob.low)}–{_fmt_price(ob.high)} str={ob.strength:.0f} v={ob.visit_count} [{in_ob}{ote}]")
    lines.append("")

    # ── Fair Value Gaps ─────────────────────────────────
    active_bull_fvgs = [f for f in bullish_fvgs if getattr(f, 'is_active', lambda t: True)(now_ms)]
    active_bear_fvgs = [f for f in bearish_fvgs if getattr(f, 'is_active', lambda t: True)(now_ms)]

    lines.append(f"<b>📊 FVGs ({len(active_bull_fvgs)}B / {len(active_bear_fvgs)}S active)</b>")
    for fvg in sorted(active_bull_fvgs, key=lambda f: abs(current_price - f.midpoint))[:2]:
        in_fvg = "⚡IN" if fvg.is_price_in_gap(current_price) else ""
        lines.append(f"  🟢 {_fmt_price(fvg.bottom)}–{_fmt_price(fvg.top)} fill={fvg.fill_percentage*100:.0f}% {in_fvg}")
    for fvg in sorted(active_bear_fvgs, key=lambda f: abs(current_price - f.midpoint))[:2]:
        in_fvg = "⚡IN" if fvg.is_price_in_gap(current_price) else ""
        lines.append(f"  🔴 {_fmt_price(fvg.bottom)}–{_fmt_price(fvg.top)} fill={fvg.fill_percentage*100:.0f}% {in_fvg}")
    lines.append("")

    # ── Recent Market Structure ─────────────────────────
    recent_ms = list(market_structures)[-5:]
    lines.append(f"<b>📐 RECENT MSS ({len(market_structures)} total)</b>")
    for ms in reversed(recent_ms):
        lines.append(f"  {_mss_label(ms)}")
    lines.append("")

    # ── TRADE PLAN — The "What I'm Thinking" Section ────
    lines.append("<b>🎯 TRADE PLAN</b>")

    if long_plan:
        lines.append(f"  <b>📗 LONG PLAN:</b>")
        lines.append(f"    Status: {long_plan.get('status', '?')}")
        if long_plan.get('gate_failed'):
            lines.append(f"    ❌ Blocked: {_esc(long_plan['gate_failed'])}")
        else:
            if long_plan.get('entry'):
                lines.append(f"    Entry: {_fmt_price(long_plan['entry'])}")
            if long_plan.get('sl'):
                lines.append(f"    SL: {_fmt_price(long_plan['sl'])} ({_esc(long_plan.get('sl_reason', ''))})")
            if long_plan.get('tp'):
                lines.append(f"    TP: {_fmt_price(long_plan['tp'])} ({_esc(long_plan.get('tp_reason', ''))})")
            if long_plan.get('rr'):
                lines.append(f"    RR: {long_plan['rr']:.1f} | Score: {long_plan.get('score', 0):.0f}/{long_plan.get('threshold', 0):.0f}")
            if long_plan.get('missing'):
                lines.append(f"    ⏳ Need: {_esc(long_plan['missing'])}")
    else:
        lines.append("  📗 LONG: No valid setup")

    if short_plan:
        lines.append(f"  <b>📕 SHORT PLAN:</b>")
        lines.append(f"    Status: {short_plan.get('status', '?')}")
        if short_plan.get('gate_failed'):
            lines.append(f"    ❌ Blocked: {_esc(short_plan['gate_failed'])}")
        else:
            if short_plan.get('entry'):
                lines.append(f"    Entry: {_fmt_price(short_plan['entry'])}")
            if short_plan.get('sl'):
                lines.append(f"    SL: {_fmt_price(short_plan['sl'])} ({_esc(short_plan.get('sl_reason', ''))})")
            if short_plan.get('tp'):
                lines.append(f"    TP: {_fmt_price(short_plan['tp'])} ({_esc(short_plan.get('tp_reason', ''))})")
            if short_plan.get('rr'):
                lines.append(f"    RR: {short_plan['rr']:.1f} | Score: {short_plan.get('score', 0):.0f}/{short_plan.get('threshold', 0):.0f}")
            if short_plan.get('missing'):
                lines.append(f"    ⏳ Need: {_esc(short_plan['missing'])}")
    else:
        lines.append("  📕 SHORT: No valid setup")

    if entry_eval_status:
        lines.append(f"\n  💡 {entry_eval_status}")

    # ── CONSOLIDATED: Bot Stats + Position (merged from periodic report) ──
    lines.append("")
    pnl_icon = "🟢" if daily_pnl >= 0 else "🔴"
    lines.append(f"<b>💰 ACCOUNT</b>")
    lines.append(f"  Balance: {_fmt_price(balance)} | State: <b>{bot_state}</b>")
    lines.append(f"  {pnl_icon} Daily P&amp;L: {_fmt_price(daily_pnl)} | Total: {_fmt_price(total_pnl)}")
    lines.append(f"  Trades: {total_trades} | WR: {win_rate:.1f}% | Losses: {consecutive_losses}")
    if regime_atr_ratio > 0:
        lines.append(f"  ATR×: {regime_atr_ratio:.2f} | Size×: {regime_size_mult:.2f}")

    # Active position
    if position:
        p_side = position.get("side", "?").upper()
        p_entry = entry_price or position.get("entry_price", 0)
        lines.append("")
        lines.append(f"<b>🔹 POSITION: {p_side}</b>")
        lines.append(f"  Entry: {_fmt_price(p_entry)}")
        if current_sl:
            lines.append(f"  SL: {_fmt_price(current_sl)}")
        if current_tp:
            lines.append(f"  TP: {_fmt_price(current_tp)}")
        if breakeven_moved:
            lines.append(f"  🔒 BE moved | Locked: {profit_locked_pct:.1f}R")
        if p_entry and current_price:
            qty = float(position.get("quantity", 0) or position.get("qty", 0) or 0)
            if p_side == "LONG":
                price_delta = current_price - p_entry
            else:
                price_delta = p_entry - current_price
            try:
                import config as _cfg
                _leverage = getattr(_cfg, "LEVERAGE", 1)
            except Exception:
                _leverage = 1
            usdt_pnl = price_delta * qty * _leverage if qty > 0 else price_delta
            risk_price = abs(p_entry - current_sl) if current_sl else 0
            ur_r = price_delta / risk_price if risk_price > 0 else 0
            upnl_icon = "🟢" if price_delta >= 0 else "🔴"
            if qty > 0:
                lines.append(f"  {upnl_icon} uPnL: <b>${usdt_pnl:+.2f}</b> ({ur_r:+.1f}R)")
            else:
                lines.append(f"  {upnl_icon} uPnL: Δ{_fmt_price(abs(price_delta))} ({ur_r:+.1f}R)")

    if volume_delta and isinstance(volume_delta, dict):
        dp = volume_delta.get("delta_pct", 0.0)
        if dp != 0:
            lines.append(f"  Vol Δ: {dp:+.2%}")

    return "\n".join(lines)


# ======================================================================
# 2. ENTRY ALERT — Full Context Entry Notification
# ======================================================================

def format_entry_alert(
    side: str,
    score: float,
    threshold: float,
    entry_price: float,
    sl_price: float,
    tp_price: float,
    position_size: float,
    rr: float,
    reasons: List[str],
    # Structure context
    trigger_ob: Optional[Any] = None,
    trigger_fvg: Optional[Any] = None,
    sweep_pool: Optional[Any] = None,
    mss_event: Optional[Any] = None,
    nearest_swing_low: Optional[float] = None,
    nearest_swing_high: Optional[float] = None,
    # Environment
    htf_bias: str = "?",
    daily_bias: str = "?",
    regime: str = "?",
    session: str = "?",
    in_killzone: bool = False,
    dr_zone: str = "?",
    regime_size_mult: float = 1.0,
    dr_mult: float = 1.0,
    current_price: float = 0.0,
) -> str:
    """Comprehensive entry notification with every price level for chart verification."""

    risk = abs(entry_price - sl_price)
    reward = abs(tp_price - entry_price)

    side_icon = "🟢" if side.upper() == "LONG" else "🔴"
    kz = " 🔥KZ" if in_killzone else ""

    lines = []
    lines.append(f"{side_icon} <b>ENTRY: {side.upper()}</b>  Score: <b>{score:.0f}/{threshold:.0f}</b>{kz}")
    lines.append("")

    # ── Price Levels (the most important section) ───────
    lines.append("<b>💰 LEVELS</b>")
    lines.append(f"  Price Now: {_fmt_price(current_price)}")
    lines.append(f"  Entry:     <b>{_fmt_price(entry_price)}</b>")
    lines.append(f"  SL:        <b>{_fmt_price(sl_price)}</b> (risk: {_fmt_price(risk)})")
    lines.append(f"  TP:        <b>{_fmt_price(tp_price)}</b> (reward: {_fmt_price(reward)})")
    lines.append(f"  RR:        <b>{rr:.1f}:1</b>")
    lines.append(f"  Size:      {position_size:.4f} BTC")
    lines.append("")

    # ── Structure Justification ─────────────────────────
    lines.append("<b>🔍 STRUCTURE</b>")
    if trigger_ob:
        lines.append(f"  OB: {_fmt_price(trigger_ob.low)}–{_fmt_price(trigger_ob.high)} str={trigger_ob.strength:.0f} v={trigger_ob.visit_count}")
    if trigger_fvg:
        lines.append(f"  FVG: {_fmt_price(trigger_fvg.bottom)}–{_fmt_price(trigger_fvg.top)} fill={trigger_fvg.fill_percentage*100:.0f}%")
    if sweep_pool:
        lines.append(f"  Sweep: {getattr(sweep_pool, 'pool_type', '?')} @ {_fmt_price(sweep_pool.price)}")
    if mss_event:
        lines.append(f"  MSS: {getattr(mss_event, 'structure_type', '?')} {getattr(mss_event, 'direction', '?')} [{getattr(mss_event, 'timeframe', '?')}]")
    if nearest_swing_low:
        lines.append(f"  Swing Low:  {_fmt_price(nearest_swing_low)}")
    if nearest_swing_high:
        lines.append(f"  Swing High: {_fmt_price(nearest_swing_high)}")
    lines.append("")

    # ── Environment ─────────────────────────────────────
    lines.append("<b>🌍 ENVIRONMENT</b>")
    lines.append(f"  HTF: {htf_bias} | Daily: {daily_bias}")
    lines.append(f"  Regime: {regime} | Session: {session}")
    lines.append(f"  DR zone: {dr_zone}")
    lines.append(f"  Size mult: regime={regime_size_mult:.2f} DR={dr_mult:.2f}")
    lines.append("")

    # ── Confluence Reasons ──────────────────────────────
    lines.append("<b>📊 CONFLUENCE</b>")
    for i, r in enumerate(reasons[:8], 1):
        lines.append(f"  {i}. {_esc(r)}")

    return "\n".join(lines)


# ======================================================================
# 3. TRAIL UPDATE — SL Move with Context
# ======================================================================

def format_trail_update(
    side: str,
    old_sl: float,
    new_sl: float,
    entry_price: float,
    current_price: float,
    trail_reason: str,
    current_rr: float,
    profit_locked_pct: float,
    breakeven_moved: bool,
) -> str:
    """Trailing SL update notification."""
    side_icon = "🟢" if side.upper() == "LONG" else "🔴"
    direction = "⬆️" if (side.upper() == "LONG" and new_sl > old_sl) else "⬇️"

    risk = abs(entry_price - old_sl) if old_sl else 0
    locked_r = profit_locked_pct

    lines = []
    lines.append(f"{side_icon} <b>TRAILING SL UPDATE</b>")
    lines.append(f"  {direction} SL: {_fmt_price(old_sl)} → <b>{_fmt_price(new_sl)}</b>")
    lines.append(f"  Entry: {_fmt_price(entry_price)} | Price: {_fmt_price(current_price)}")
    lines.append(f"  Current RR: {current_rr:.1f} | Locked: {locked_r:.1f}R")
    lines.append(f"  Reason: {_esc(trail_reason)}")
    if breakeven_moved:
        lines.append("  🔒 Breakeven active — risk-free trade")

    return "\n".join(lines)


# ======================================================================
# 4. POSITION CLOSE — Detailed Trade Review
# ======================================================================

def format_position_close(
    side: str,
    entry_price: float,
    close_price: float,
    sl_price: float,
    tp_price: float,
    pnl: float,
    close_reason: str,
    # Context at entry
    entry_score: float = 0.0,
    entry_reasons: Optional[List[str]] = None,
    # Trailing info
    breakeven_moved: bool = False,
    max_favorable: float = 0.0,
    max_adverse: float = 0.0,
    # Stats
    total_pnl: float = 0.0,
    win_rate: float = 0.0,
    total_trades: int = 0,
    consecutive_losses: int = 0,
) -> str:
    """Comprehensive position close notification with trade review."""
    entry_reasons = entry_reasons or []
    side_icon = "🟢" if side.upper() == "LONG" else "🔴"
    result_icon = "✅" if pnl > 0 else "❌"
    risk = abs(entry_price - sl_price) if sl_price else 0
    # RR must use price-to-price ratio (not USDT pnl / price distance)
    if side.upper() == "LONG":
        price_move = close_price - entry_price
    else:
        price_move = entry_price - close_price
    rr_achieved = price_move / risk if risk > 0 else 0

    lines = []
    lines.append(f"{result_icon} <b>POSITION CLOSED: {side.upper()}</b>")
    lines.append("")

    lines.append("<b>💰 RESULT</b>")
    lines.append(f"  PnL: <b>{_fmt_price(pnl)}</b> ({rr_achieved:+.1f}R)")
    lines.append(f"  Reason: {_esc(close_reason)}")
    lines.append("")

    lines.append("<b>📊 LEVELS</b>")
    lines.append(f"  Entry: {_fmt_price(entry_price)}")
    lines.append(f"  Exit:  {_fmt_price(close_price)}")
    lines.append(f"  SL:    {_fmt_price(sl_price)}")
    lines.append(f"  TP:    {_fmt_price(tp_price)}")
    lines.append("")

    lines.append("<b>📈 TRADE METRICS</b>")
    if max_favorable:
        mfe_r = max_favorable / risk if risk > 0 else 0
        lines.append(f"  Max Favorable: {_fmt_price(max_favorable)} ({mfe_r:.1f}R)")
    if max_adverse:
        mae_r = max_adverse / risk if risk > 0 else 0
        lines.append(f"  Max Adverse:   {_fmt_price(max_adverse)} ({mae_r:.1f}R)")
    if breakeven_moved:
        lines.append("  🔒 Breakeven was moved")
    if entry_score:
        lines.append(f"  Entry Score:   {entry_score:.0f}")
    lines.append("")

    lines.append("<b>📊 SESSION</b>")
    lines.append(f"  Total PnL: {_fmt_price(total_pnl)}")
    lines.append(f"  Trades: {total_trades} | WR: {win_rate:.1f}%")
    if consecutive_losses > 0:
        lines.append(f"  ⚠️ Consec Losses: {consecutive_losses}")

    return "\n".join(lines)


# ======================================================================
# 5. PERIODIC STATUS — Balance/Health Summary
# ======================================================================

def format_periodic_report(
    current_price: float = 0.0,
    balance: float = 0.0,
    total_trades: int = 0,
    win_rate: float = 0.0,
    daily_pnl: float = 0.0,
    total_pnl: float = 0.0,
    consecutive_losses: int = 0,
    htf_bias: str = "UNKNOWN",
    htf_bias_strength: float = 0.0,
    daily_bias: str = "NEUTRAL",
    session: str = "REGULAR",
    in_killzone: bool = False,
    amd_phase: str = "UNKNOWN",
    bot_state: str = "UNKNOWN",
    regime: str = "UNKNOWN",
    regime_adx: float = 0.0,
    position: Optional[Dict] = None,
    current_sl: Optional[float] = None,
    current_tp: Optional[float] = None,
    entry_price: Optional[float] = None,
    breakeven_moved: bool = False,
    profit_locked_pct: float = 0.0,
    # Structures count
    bull_obs: int = 0,
    bear_obs: int = 0,
    bull_fvgs: int = 0,
    bear_fvgs: int = 0,
    liq_pools: int = 0,
    swing_h: int = 0,
    swing_l: int = 0,
    mss_count: int = 0,
    # DR prices
    dr_weekly_str: str = "—",
    dr_daily_str: str = "—",
    dr_intraday_str: str = "—",
    volume_delta: Optional[Dict] = None,
    extra_lines: Optional[List[str]] = None,
) -> str:
    """Periodic status report — sent every 15m."""
    kz_icon = "🔥" if in_killzone else "⚪"
    pnl_icon = "🟢" if daily_pnl >= 0 else "🔴"

    lines = [
        "<b>📊 ICT BOT v10 STATUS</b>",
        f"⏰ {datetime.now(timezone.utc).strftime('%H:%M UTC')}",
        "",
        f"💰 BTC: <b>{_fmt_price(current_price)}</b>",
        f"💵 Balance: {_fmt_price(balance)}",
        f"{pnl_icon} Daily P&L: {_fmt_price(daily_pnl)}",
        f"📈 Total P&L: {_fmt_price(total_pnl)}",
        "",
        f"🎯 State: <b>{bot_state}</b>",
        f"📐 HTF: <b>{htf_bias}</b> ({htf_bias_strength:.0%}) | Daily: {daily_bias}",
        f"🏛️ Regime: {regime} (ADX {regime_adx:.1f})",
        f"{kz_icon} {session} | AMD: {amd_phase}",
        "",
        f"📏 DR: W={dr_weekly_str} D={dr_daily_str} I={dr_intraday_str}",
        "",
        f"📦 OB: {bull_obs}B/{bear_obs}S | FVG: {bull_fvgs}B/{bear_fvgs}S",
        f"💧 Liq: {liq_pools} | Swings: {swing_h}H/{swing_l}L | MSS: {mss_count}",
    ]

    # Active position
    if position:
        side  = position.get("side", "?").upper()
        entry_p = entry_price or position.get("entry_price", 0)
        lines.append("")
        lines.append(f"<b>🔹 POSITION: {side}</b>")
        lines.append(f"  Entry: {_fmt_price(entry_p)}")
        if current_sl:
            lines.append(f"  SL: {_fmt_price(current_sl)}")
        if current_tp:
            lines.append(f"  TP: {_fmt_price(current_tp)}")
        if breakeven_moved:
            lines.append(f"  🔒 BE moved | Locked: {profit_locked_pct:.1f}R")
        # Unrealized P&L — actual USDT amount (price delta × qty × leverage)
        # qty was divided by leverage during position sizing, so multiply back
        if entry_p and current_price:
            qty = float(position.get("quantity", 0) or position.get("qty", 0) or 0)
            if side == "LONG":
                price_delta = current_price - entry_p
            else:
                price_delta = entry_p - current_price
            import config as _cfg
            _leverage = getattr(_cfg, "LEVERAGE", 1)
            usdt_pnl = price_delta * qty * _leverage if qty > 0 else price_delta
            risk_price = abs(entry_p - current_sl) if current_sl else 0
            ur_r = price_delta / risk_price if risk_price > 0 else 0
            upnl_icon = "🟢" if price_delta >= 0 else "🔴"
            if qty > 0:
                lines.append(f"  {upnl_icon} Unrealized: <b>${usdt_pnl:+.2f}</b> ({ur_r:+.1f}R) | Δ{_fmt_price(abs(price_delta))}")
            else:
                lines.append(f"  {upnl_icon} Unrealized: Δ{_fmt_price(price_delta)} ({ur_r:+.1f}R)")

    if volume_delta and isinstance(volume_delta, dict):
        dp = volume_delta.get("delta_pct", 0.0)
        if dp != 0:
            lines.append(f"  Vol Δ: {dp:+.2%}")

    lines.append("")
    lines.append(f"📊 Trades: {total_trades} | WR: {win_rate:.1f}% | Consec L: {consecutive_losses}")

    if extra_lines:
        for el in extra_lines:
            if el and el.strip():
                lines.append(el)

    return "\n".join(lines)


# ======================================================================
# 6. STRUCTURE DEEP REPORT — /structures command
# ======================================================================

def format_structures_report(
    current_price: float = 0.0,
    htf_bias: str = "UNKNOWN",
    htf_bias_strength: float = 0.0,
    daily_bias: str = "NEUTRAL",
    session: str = "REGULAR",
    in_killzone: bool = False,
    amd_phase: str = "UNKNOWN",
    bullish_obs: Optional[List] = None,
    bearish_obs: Optional[List] = None,
    bullish_fvgs: Optional[List] = None,
    bearish_fvgs: Optional[List] = None,
    liquidity_pools: Optional[List] = None,
    market_structures: Optional[List] = None,
    swing_highs: Optional[List] = None,
    swing_lows: Optional[List] = None,
    volume_delta: Optional[Dict] = None,
) -> str:
    """Deep ICT structure analysis report with all price levels."""
    bullish_obs = bullish_obs or []
    bearish_obs = bearish_obs or []
    bullish_fvgs = bullish_fvgs or []
    bearish_fvgs = bearish_fvgs or []
    liquidity_pools = liquidity_pools or []
    market_structures = market_structures or []
    swing_highs = swing_highs or []
    swing_lows = swing_lows or []

    now_ms = int(time.time() * 1000)

    lines = [
        "<b>🔬 ICT STRUCTURE ANALYSIS</b>",
        f"Price: <b>{_fmt_price(current_price)}</b>",
        f"HTF: {htf_bias} ({htf_bias_strength:.0%}) | Daily: {daily_bias}",
        f"Session: {session} | AMD: {amd_phase}",
        "",
    ]

    # Order Blocks — ALL with full detail
    lines.append(f"<b>📦 BULLISH OBs ({len(bullish_obs)})</b>")
    for ob in sorted(bullish_obs, key=lambda o: abs(current_price - o.midpoint)):
        if not ob.is_active(now_ms):
            continue
        dist = abs(current_price - ob.midpoint) / current_price * 100
        in_ob = "⚡IN" if ob.contains_price(current_price) else ""
        ote = " OTE✓" if ob.in_optimal_zone(current_price) else ""
        bos = " BOS✓" if ob.bos_confirmed else ""
        disp = " DISP✓" if ob.has_displacement else ""
        lines.append(
            f"  {_fmt_price(ob.low)}–{_fmt_price(ob.high)} "
            f"str={ob.strength:.0f} v={ob.visit_count} "
            f"{_fmt_pct(dist)}{in_ob}{ote}{bos}{disp}")

    lines.append(f"<b>📦 BEARISH OBs ({len(bearish_obs)})</b>")
    for ob in sorted(bearish_obs, key=lambda o: abs(current_price - o.midpoint)):
        if not ob.is_active(now_ms):
            continue
        dist = abs(current_price - ob.midpoint) / current_price * 100
        in_ob = "⚡IN" if ob.contains_price(current_price) else ""
        ote = " OTE✓" if ob.in_optimal_zone(current_price) else ""
        bos = " BOS✓" if ob.bos_confirmed else ""
        disp = " DISP✓" if ob.has_displacement else ""
        lines.append(
            f"  {_fmt_price(ob.low)}–{_fmt_price(ob.high)} "
            f"str={ob.strength:.0f} v={ob.visit_count} "
            f"{_fmt_pct(dist)}{in_ob}{ote}{bos}{disp}")

    # FVGs
    lines.append("")
    lines.append(f"<b>📊 BULLISH FVGs ({len(bullish_fvgs)})</b>")
    for fvg in sorted(bullish_fvgs, key=lambda f: abs(current_price - f.midpoint)):
        if not fvg.is_active(now_ms):
            continue
        in_fvg = " ⚡IN" if fvg.is_price_in_gap(current_price) else ""
        lines.append(f"  {_fvg_label(fvg)}{in_fvg}")

    lines.append(f"<b>📊 BEARISH FVGs ({len(bearish_fvgs)})</b>")
    for fvg in sorted(bearish_fvgs, key=lambda f: abs(current_price - f.midpoint)):
        if not fvg.is_active(now_ms):
            continue
        in_fvg = " ⚡IN" if fvg.is_price_in_gap(current_price) else ""
        lines.append(f"  {_fvg_label(fvg)}{in_fvg}")

    # Liquidity
    lines.append("")
    lines.append(f"<b>💧 LIQUIDITY ({len(liquidity_pools)} pools)</b>")
    for lp in sorted(liquidity_pools, key=lambda p: abs(current_price - p.price)):
        lines.append(f"  {_liq_label(lp)}")

    # MSS
    lines.append("")
    lines.append(f"<b>📐 MARKET STRUCTURE ({len(market_structures)})</b>")
    for ms in reversed(list(market_structures)[-8:]):
        lines.append(f"  {_mss_label(ms)}")

    # Swings
    lines.append("")
    lines.append(f"<b>🔀 SWING HIGHS (nearest 5)</b>")
    for s in sorted(swing_highs, key=lambda x: abs(current_price - x.price))[:5]:
        lines.append(f"  {_fmt_price(s.price)} [{getattr(s, 'timeframe', '?')}]")
    lines.append(f"<b>🔀 SWING LOWS (nearest 5)</b>")
    for s in sorted(swing_lows, key=lambda x: abs(current_price - x.price))[:5]:
        lines.append(f"  {_fmt_price(s.price)} [{getattr(s, 'timeframe', '?')}]")

    return "\n".join(lines)


# ======================================================================
# 7. ENTRY REJECTION LOG — Why a setup was rejected
# ======================================================================

def format_rejection_log(
    side: str,
    current_price: float,
    l1_result: str = "",
    l2_result: str = "",
    l3_result: str = "",
    score: float = 0.0,
    threshold: float = 0.0,
    reasons: Optional[List[str]] = None,
) -> str:
    """Log entry rejection with full reasoning — console only, not sent to Telegram."""
    reasons = reasons or []
    lines = [
        f"⛔ {side.upper()} rejected @ {_fmt_price(current_price)}",
    ]
    if l1_result:
        lines.append(f"  L1: {l1_result}")
    if l2_result:
        lines.append(f"  L2: {l2_result}")
    if l3_result:
        lines.append(f"  L3: {l3_result}")
    if score > 0:
        lines.append(f"  Score: {score:.0f}/{threshold:.0f}")
    if reasons:
        lines.append(f"  Reasons: {', '.join(reasons[:5])}")
    return "\n".join(lines)


# ======================================================================
# LOGGING HANDLER — forward WARNING+ to Telegram
# ======================================================================

class TelegramLogHandler(logging.Handler):
    """Forward WARNING+ logs to Telegram with throttling."""

    def __init__(self, level=logging.WARNING, throttle_seconds: float = 5.0):
        super().__init__(level)
        self._throttle  = throttle_seconds
        self._last_ts   = 0.0
        self._lock       = threading.Lock()
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

            # CRITICAL: escape content so log messages containing <, >, &
            # (from exception reprs, Python type names, f-strings with dicts, etc.)
            # do not break Telegram's HTML parser.
            send_telegram_message(f"⚠️ <code>{_esc(msg[:1500])}</code>")
        except Exception:
            pass


def install_global_telegram_log_handler(level=logging.WARNING,
                                         throttle_seconds: float = 5.0) -> None:
    handler = TelegramLogHandler(level=level, throttle_seconds=throttle_seconds)
    handler.setFormatter(logging.Formatter("%(name)s: %(message)s"))
    logging.getLogger().addHandler(handler)

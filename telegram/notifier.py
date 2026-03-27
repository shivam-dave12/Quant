"""
telegram/notifier.py — Liquidity-First Telegram Notifier
==========================================================
Report layout reflects the decision architecture:
  Pool target → Flow confirmation → ICT context → Entry levels → Exit plan

Report types:
  1. Market Outlook    — pool map + flow state + trade plan (periodic)
  2. Entry Alert       — pool target + OTE entry + ICT context
  3. Trail Update      — ICT structure basis (BOS/CHoCH/swing)
  4. Position Close    — trade review with pool TP analysis
  5. Periodic Status   — balance / PnL summary
  6. Pool Sweep Alert  — immediate notification when a pool is swept
"""

import logging
import re
import time
import threading
import requests
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
from collections import deque
import sys, os as _os; sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
import telegram.config as telegram_config
import html as _html_lib

logger = logging.getLogger(__name__)

import queue as _queue_mod

_send_queue: _queue_mod.Queue = _queue_mod.Queue(maxsize=200)
_worker_started = False
_worker_lock    = threading.Lock()
_MIN_INTERVAL   = 1.0
_MAX_RETRIES    = 3


# ======================================================================
# HTML SANITIZER
# ======================================================================

def _sanitize_html(text: str) -> str:
    """
    Remove or escape HTML constructs that Telegram's parse_mode rejects.
    Telegram supports: <b>, <i>, <u>, <s>, <code>, <pre>, <a href="...">,
    <tg-spoiler> and their closing counterparts only.
    """
    text = re.sub(r'<br\s*/?>', '\n',         text, flags=re.IGNORECASE)
    text = re.sub(r'<hr\s*/?>', '\n──────\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<p\s*/?>', '\n',          text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '',                 text, flags=re.IGNORECASE)

    _SAFE_TAGS = {
        'b', '/b', 'i', '/i', 'u', '/u', 's', '/s',
        'code', '/code', 'pre', '/pre',
        'tg-spoiler', '/tg-spoiler',
    }

    def _fix_tag(m):
        inner    = m.group(1).strip()
        if inner.lower().startswith('a ') or inner.lower() == '/a':
            return m.group(0)
        tag_name = inner.lower().split()[0] if inner else ''
        if tag_name in _SAFE_TAGS:
            return m.group(0)
        return ''

    text = re.sub(r'<([^>]*)>', _fix_tag, text)

    _SAFE_TAG_RE = re.compile(
        r'<(?=/?(?:b|i|u|s|code|pre|tg-spoiler|a)(?:[\s>"/]|$))',
        re.IGNORECASE,
    )
    parts = text.split('<')
    if len(parts) > 1:
        rebuilt = [parts[0]]
        for part in parts[1:]:
            if _SAFE_TAG_RE.match('<' + part):
                rebuilt.append('<')
            else:
                rebuilt.append('&lt;')
            rebuilt.append(part)
        text = ''.join(rebuilt)

    text = re.sub(r'\n{3,}', '\n\n', text)
    return text


def _esc(s) -> str:
    if s is None: return ""
    return _html_lib.escape(str(s), quote=False)


# ======================================================================
# ASYNC SEND WORKER
# ======================================================================

def _send_worker():
    """Background thread — drains the message queue and sends to Telegram."""
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
            now = time.time()
            gap = _MIN_INTERVAL - (now - last_send_ts)
            if gap > 0:
                time.sleep(gap)

            try:
                url       = f"https://api.telegram.org/bot{telegram_config.TELEGRAM_BOT_TOKEN}/sendMessage"
                send_text = message[:4000]
                send_mode = parse_mode
                if parse_mode == "HTML":
                    send_text = _sanitize_html(send_text)

                payload = {
                    "chat_id":                  telegram_config.TELEGRAM_CHAT_ID,
                    "text":                     send_text,
                    "parse_mode":               send_mode,
                    "disable_web_page_preview": True,
                }
                resp         = requests.post(url, json=payload, timeout=15)
                last_send_ts = time.time()

                if resp.status_code == 200:
                    break

                if resp.status_code == 400 and parse_mode == "HTML" and attempt == 0:
                    logger.warning(f"Telegram HTML parse error — retrying as plain text: {resp.text[:120]}")
                    plain_text = re.sub(r'<[^>]+>', '', send_text, flags=re.DOTALL)
                    plain_payload = {
                        "chat_id":                  telegram_config.TELEGRAM_CHAT_ID,
                        "text":                     plain_text[:4000],
                        "disable_web_page_preview": True,
                    }
                    resp2        = requests.post(url, json=plain_payload, timeout=15)
                    last_send_ts = time.time()
                    if resp2.status_code == 200:
                        break
                    break

                if resp.status_code in (429, 500, 502, 503) and attempt < _MAX_RETRIES - 1:
                    backoff = 2.0 * (2 ** attempt)
                    if resp.status_code == 429:
                        try:
                            retry_after = resp.json().get("parameters", {}).get("retry_after", backoff)
                            backoff = max(backoff, float(retry_after))
                        except Exception:
                            pass
                    logger.warning(f"Telegram {resp.status_code}, retry {attempt+1}/{_MAX_RETRIES} in {backoff:.1f}s")
                    time.sleep(backoff)
                    continue

                logger.warning(f"Telegram send failed: {resp.status_code} — {resp.text[:200]}")
                break

            except requests.exceptions.Timeout:
                if attempt < _MAX_RETRIES - 1:
                    time.sleep(2.0 * (attempt + 1))
                    continue
                logger.error("Telegram send timed out after all retries")
                break
            except Exception as e:
                logger.error(f"Telegram send error: {e}")
                break

        _send_queue.task_done()


def _ensure_worker_started():
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
    """Enqueue a Telegram message for async delivery. Never blocks the caller."""
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
    if p is None: return "—"
    return f"${p:,.1f}"


def _fmt_pct(v: float) -> str:
    return f"{v:.2f}%"


def _time_ago(ts_ms: int) -> str:
    if not ts_ms: return "?"
    elapsed = (time.time() * 1000 - ts_ms) / 1000
    if elapsed < 60:    return f"{elapsed:.0f}s ago"
    if elapsed < 3600:  return f"{elapsed/60:.0f}m ago"
    return f"{elapsed/3600:.1f}h ago"


def _pool_label(pool) -> str:
    level_type = getattr(pool, 'level_type', getattr(pool, 'pool_type', '?'))
    touches    = getattr(pool, 'touch_count', 0)
    score      = getattr(pool, 'priority_score', 0.0)
    fresh      = "✅" if getattr(pool, 'fresh', True) else "♻️"
    return f"{level_type} @ {_fmt_price(pool.price)}  x{touches}  {fresh}  score={score:.2f}"


def _ob_label(ob) -> str:
    v_tag = "🔴" if getattr(ob, 'visit_count', 0) >= 2 else ("⚪" if getattr(ob, 'visit_count', 0) == 1 else "🟣")
    bos   = "✓BOS"  if getattr(ob, 'bos_confirmed',   False) else ""
    disp  = "✓DISP" if getattr(ob, 'has_displacement', False) else ""
    tags  = " ".join(filter(None, [bos, disp]))
    return (f"{v_tag} {_fmt_price(ob.low)}–{_fmt_price(ob.high)} "
            f"str={getattr(ob,'strength',0):.0f} v={getattr(ob,'visit_count',0)} "
            f"{tags} {_time_ago(getattr(ob,'timestamp',0))}")


def _fvg_label(fvg) -> str:
    fill = getattr(fvg, 'fill_percentage', 0) * 100
    return (f"{_fmt_price(fvg.bottom)}–{_fmt_price(fvg.top)} "
            f"fill={fill:.0f}% {_time_ago(getattr(fvg,'timestamp',0))}")


def _liq_label(pool) -> str:
    swept = "✅SWEPT" if getattr(pool, 'swept', False) else "⏳"
    disp  = "+DISP"   if getattr(pool, 'displacement_confirmed', False) else ""
    wick  = "+WICK"   if getattr(pool, 'wick_rejection', False) else ""
    return (f"{getattr(pool,'pool_type','?')} @ {_fmt_price(pool.price)} "
            f"x{getattr(pool,'touch_count',0)} {swept} {disp} {wick}")


# ======================================================================
# 1. MARKET OUTLOOK — Liquidity-First Layout
# ======================================================================

def format_market_outlook(
    current_price:     float,
    # ── LAYER 1: Liquidity pools ──
    bsl_pools:         Optional[List] = None,
    ssl_pools:         Optional[List] = None,
    primary_target:    Optional[Any]  = None,
    recent_sweeps:     Optional[List] = None,
    # ── LAYER 2: Flow ──
    flow_conviction:   float = 0.0,
    flow_direction:    str   = "",
    cvd_divergence:    float = 0.0,
    ob_imbalance:      float = 0.0,
    tick_aggression:   float = 0.0,
    # ── LAYER 3: ICT secondary ──
    amd_phase:         str   = "UNKNOWN",
    amd_bias:          str   = "",
    amd_confidence:    float = 0.0,
    htf_bias:          str   = "NEUTRAL",
    session:           str   = "REGULAR",
    in_killzone:       bool  = False,
    regime:            str   = "UNKNOWN",
    regime_adx:        float = 0.0,
    # ── LAYER 4: Entry plan ──
    long_plan:         Optional[Dict] = None,
    short_plan:        Optional[Dict] = None,
    entry_eval_status: str            = "",
    # ── Bot stats ──
    balance:           float = 0.0,
    total_trades:      int   = 0,
    win_rate:          float = 0.0,
    daily_pnl:         float = 0.0,
    total_pnl:         float = 0.0,
    consecutive_losses: int  = 0,
    bot_state:         str   = "SCANNING",
    position:          Optional[Dict] = None,
    current_sl:        Optional[float] = None,
    current_tp:        Optional[float] = None,
    entry_price:       Optional[float] = None,
    breakeven_moved:   bool  = False,
    profit_locked_pct: float = 0.0,
    # Legacy fields (kept for compatibility)
    bullish_obs:       Optional[List] = None,
    bearish_obs:       Optional[List] = None,
    bullish_fvgs:      Optional[List] = None,
    bearish_fvgs:      Optional[List] = None,
    liquidity_pools:   Optional[List] = None,
    **kwargs,
) -> str:
    """
    Consolidated market outlook in liquidity-first layout:
      Pool target → Flow state → ICT context → Trade plan → Bot stats
    """
    bsl_pools    = bsl_pools    or []
    ssl_pools    = ssl_pools    or []
    recent_sweeps = recent_sweeps or []

    kz    = "🔥 KILLZONE" if in_killzone else "⚪"
    lines = []

    # ── Header ──────────────────────────────────────────────────────
    lines.append("🎯 <b>LIQUIDITY-FIRST BOT — OUTLOOK</b>")
    lines.append(f"⏰ {datetime.now(timezone.utc).strftime('%H:%M UTC')} | {_fmt_price(current_price)}")
    lines.append("")

    # ── LAYER 1: Liquidity Map ───────────────────────────────────────
    lines.append("<b>━ LAYER 1: LIQUIDITY MAP</b>")

    if primary_target:
        direction = "BSL ▲" if getattr(primary_target, 'level_type', '') == 'BSL' else "SSL ▼"
        score     = getattr(primary_target, 'priority_score', 0.0)
        lines.append(f"  🎯 <b>Target: {direction} @ {_fmt_price(primary_target.price)}</b>  score={score:.2f}")

    bsl_near = sorted([p for p in bsl_pools if p.price > current_price], key=lambda p: p.price)[:4]
    ssl_near = sorted([p for p in ssl_pools if p.price < current_price], key=lambda p: p.price, reverse=True)[:4]

    if bsl_near:
        lines.append("  ▲ BSL: " + "  ".join(
            f"{_fmt_price(p.price)}(x{getattr(p,'touch_count',0)})" for p in bsl_near))
    if ssl_near:
        lines.append("  ▼ SSL: " + "  ".join(
            f"{_fmt_price(p.price)}(x{getattr(p,'touch_count',0)})" for p in ssl_near))

    if recent_sweeps:
        lines.append(f"  🌊 Recent sweeps: {len(recent_sweeps)}")
        for sw in recent_sweeps[-2:]:
            disp = "DISP✓" if getattr(sw, 'displacement_confirmed', False) else "weak"
            lines.append(f"    {getattr(sw,'level_type','?')} {_fmt_price(sw.price)} [{disp}]")
    lines.append("")

    # ── LAYER 2: Flow Direction ─────────────────────────────────────
    lines.append("<b>━ LAYER 2: FLOW DIRECTION</b>")

    def fbar(v, w=8):
        h = w // 2
        f = min(int(abs(v) * h + 0.5), h)
        return ("·" * h + "█" * f + "░" * (h - f)) if v >= 0 else ("░" * (h - f) + "█" * f + "·" * h)

    lines.append(f"  CVD div    {fbar(cvd_divergence)} {cvd_divergence:+.3f}")
    lines.append(f"  OB delta   {fbar(ob_imbalance)}   {ob_imbalance:+.3f}")
    lines.append(f"  Tick aggr  {fbar(tick_aggression)} {tick_aggression:+.3f}")

    toward_pool = False
    if primary_target and flow_direction:
        toward_pool = (
            (getattr(primary_target, 'level_type', '') == 'BSL' and flow_direction == 'long') or
            (getattr(primary_target, 'level_type', '') == 'SSL' and flow_direction == 'short')
        )

    flow_icon = "✅" if toward_pool else "❌"
    lines.append(
        f"  {flow_icon} Conviction: {flow_conviction:+.3f}  "
        f"{'→ toward pool' if toward_pool else '→ NOT toward pool'}")
    lines.append("")

    # ── LAYER 3: ICT Secondary ──────────────────────────────────────
    lines.append("<b>━ LAYER 3: ICT SECONDARY</b>")
    amd_icons = {"DISTRIBUTION": "🎯", "MANIPULATION": "⚡",
                 "REACCUMULATION": "🔄", "REDISTRIBUTION": "🔄", "ACCUMULATION": "💤"}
    amd_icon  = amd_icons.get(amd_phase, "❓")
    bias_icon = "🔴" if amd_bias == "bearish" else ("🟢" if amd_bias == "bullish" else "⚪")
    lines.append(f"  {amd_icon} AMD: <b>{_esc(amd_phase)}</b>  {bias_icon}{_esc(amd_bias)}  conf={amd_confidence:.2f}")
    lines.append(f"  HTF: {_esc(htf_bias)}  Regime: {_esc(regime)} (ADX {regime_adx:.1f})")
    lines.append(f"  {kz} {_esc(session)}")
    lines.append("")

    # ── LAYER 4: Trade Plan ─────────────────────────────────────────
    lines.append("<b>━ LAYER 4: TRADE PLAN</b>")

    if long_plan:
        lines.append("  <b>📗 LONG:</b>")
        lines.append(f"    Status: {long_plan.get('status', '?')}")
        if long_plan.get('gate_failed'):
            lines.append(f"    ❌ Blocked: {_esc(long_plan['gate_failed'])}")
        else:
            if long_plan.get('entry'):
                lines.append(f"    Entry (OTE): {_fmt_price(long_plan['entry'])}")
            if long_plan.get('sl'):
                lines.append(f"    SL (ICT):    {_fmt_price(long_plan['sl'])}  ({_esc(long_plan.get('sl_reason',''))})")
            if long_plan.get('tp'):
                lines.append(f"    TP (pool):   {_fmt_price(long_plan['tp'])}")
            if long_plan.get('rr'):
                lines.append(f"    R:R: {long_plan['rr']:.1f}")
    else:
        lines.append("  📗 LONG: No valid setup")

    if short_plan:
        lines.append("  <b>📕 SHORT:</b>")
        lines.append(f"    Status: {short_plan.get('status', '?')}")
        if short_plan.get('gate_failed'):
            lines.append(f"    ❌ Blocked: {_esc(short_plan['gate_failed'])}")
        else:
            if short_plan.get('entry'):
                lines.append(f"    Entry (OTE): {_fmt_price(short_plan['entry'])}")
            if short_plan.get('sl'):
                lines.append(f"    SL (ICT):    {_fmt_price(short_plan['sl'])}  ({_esc(short_plan.get('sl_reason',''))})")
            if short_plan.get('tp'):
                lines.append(f"    TP (pool):   {_fmt_price(short_plan['tp'])}")
            if short_plan.get('rr'):
                lines.append(f"    R:R: {short_plan['rr']:.1f}")
    else:
        lines.append("  📕 SHORT: No valid setup")

    if entry_eval_status:
        lines.append(f"\n  💡 {entry_eval_status}")

    # ── Account / position ──────────────────────────────────────────
    lines.append("")
    pnl_icon = "🟢" if daily_pnl >= 0 else "🔴"
    lines.append("<b>💰 ACCOUNT</b>")
    lines.append(f"  Balance: {_fmt_price(balance)}  State: <b>{_esc(bot_state)}</b>")
    lines.append(f"  {pnl_icon} Daily P&amp;L: {_fmt_price(daily_pnl)} | Total: {_fmt_price(total_pnl)}")
    lines.append(f"  Trades: {total_trades} | WR: {win_rate:.1f}% | Losses: {consecutive_losses}")

    if position:
        p_side  = position.get("side", "?").upper()
        p_entry = entry_price or position.get("entry_price", 0)
        lines.append(f"\n<b>🔹 POSITION: {p_side}</b>")
        lines.append(f"  Entry: {_fmt_price(p_entry)}")
        if current_sl:
            lines.append(f"  SL (ICT struct): {_fmt_price(current_sl)}")
        if current_tp:
            lines.append(f"  TP (pool):       {_fmt_price(current_tp)}")
        if breakeven_moved:
            lines.append(f"  🔒 BE moved | Locked: {profit_locked_pct:.1f}R")
        if p_entry and current_price:
            qty  = float(position.get("quantity", 0) or position.get("qty", 0) or 0)
            move = (current_price - p_entry) if p_side == "LONG" else (p_entry - current_price)
            upnl = move * qty if qty > 0 else move
            risk = abs(p_entry - current_sl) if current_sl else 0
            ur_r = move / risk if risk > 0 else 0
            icon = "🟢" if move >= 0 else "🔴"
            if qty > 0:
                lines.append(f"  {icon} uPnL: <b>${upnl:+.2f}</b> ({ur_r:+.1f}R)")
            else:
                lines.append(f"  {icon} uPnL: Δ{_fmt_price(abs(move))} ({ur_r:+.1f}R)")

    return "\n".join(lines)


# ======================================================================
# 2. ENTRY ALERT — Liquidity-Pool-Centric
# ======================================================================

def format_entry_alert(
    side:           str,
    entry_price:    float,
    sl_price:       float,
    tp_price:       float,
    position_size:  float,
    rr:             float,
    # Pool context
    target_pool:    Optional[Any] = None,
    pool_type:      str = "",          # "BSL" | "SSL"
    pool_price:     float = 0.0,       # opposing pool (TP target)
    # Flow at entry
    flow_conviction: float = 0.0,
    cvd_divergence:  float = 0.0,
    ob_imbalance:    float = 0.0,
    tick_aggression: float = 0.0,
    # ICT context
    amd_phase:      str = "?",
    ict_tier:       str = "",
    sweep_price:    float = 0.0,
    ob_in_ote:      bool = False,
    fvg_in_ote:     bool = False,
    entry_mode:     str = "OTE",       # "OTE" | "SWEEP"
    # Environment
    session:        str = "?",
    in_killzone:    bool = False,
    htf_bias:       str = "?",
    regime:         str = "?",
    current_price:  float = 0.0,
    # Extended context
    atr:            float = 0.0,
    nearest_liq_dist_atr: float = 0.0,
    dealing_range_pd: float = 0.5,
    # Legacy
    score:          float = 0.0,
    threshold:      float = 0.0,
    reasons:        Optional[List[str]] = None,
    **kwargs,
) -> str:
    """
    Institutional entry alert — pool-first layout with full context.
    """
    reasons   = reasons or []
    risk      = abs(entry_price - sl_price)
    reward    = abs(tp_price - entry_price)
    side_icon = "🟢" if side.upper() == "LONG" else "🔴"
    kz        = " 🔥 KILLZONE" if in_killzone else ""
    mode_str  = f"[{entry_mode}]" if entry_mode else ""
    tier_str  = f" Tier-{ict_tier}" if ict_tier else ""

    # Session quality indicator
    sess_quality = {"asia": "🌙", "london": "🌅", "ny": "🏛️", "late_ny": "🌇"}.get(
        session.lower().replace(" ", "_"), "⚪")

    # Dealing range position
    pd_label = ("DEEP DISC" if dealing_range_pd < 0.25 else
                "DISCOUNT" if dealing_range_pd < 0.40 else
                "EQUILIBRIUM" if dealing_range_pd < 0.60 else
                "PREMIUM" if dealing_range_pd < 0.75 else "DEEP PREM")

    lines = []
    lines.append(f"{'━' * 32}")
    lines.append(f"{side_icon} <b>ENTRY {side.upper()}</b>  {mode_str}{tier_str}")
    lines.append(f"{'━' * 32}")

    # ── Pool target ──────────────────────────────────────────
    lines.append("")
    lines.append("🎯 <b>TRADE THESIS</b>")
    if pool_price and pool_type:
        pool_dir = "BSL ▲" if pool_type == "BSL" else "SSL ▼"
        lines.append(f"  Target: {pool_dir} @ {_fmt_price(pool_price)}")
        if atr > 0:
            dist_atr = abs(pool_price - entry_price) / atr
            lines.append(f"  Distance: {dist_atr:.1f} ATR ({_fmt_price(abs(pool_price - entry_price))})")
    if sweep_price:
        lines.append(f"  Sweep origin: {_fmt_price(sweep_price)}")
    lines.append(f"  Dealing range: {pd_label} ({dealing_range_pd:.0%})")

    # ── Flow state ───────────────────────────────────────────
    lines.append("")
    lines.append("📊 <b>ORDER FLOW</b>")
    flow_str = f"{flow_conviction:+.2f}"
    flow_bar = _flow_bar(flow_conviction)
    lines.append(f"  Conviction: {flow_bar} {flow_str}")
    lines.append(f"  CVD: {cvd_divergence:+.2f}  OB: {ob_imbalance:+.2f}  Tick: {tick_aggression:+.2f}")

    # ── Entry levels ─────────────────────────────────────────
    lines.append("")
    lines.append("💰 <b>LEVELS</b>")
    lines.append(f"  Entry:  <b>{_fmt_price(entry_price)}</b>")
    lines.append(f"  SL:     {_fmt_price(sl_price)}  ({_fmt_price(risk)} risk)")
    lines.append(f"  TP:     {_fmt_price(tp_price)}  ({_fmt_price(reward)} reward)")
    lines.append(f"  R:R:    <b>{rr:.1f}:1</b>")
    lines.append(f"  Size:   {position_size:.4f} BTC")
    if atr > 0:
        sl_atr = risk / atr
        tp_atr = reward / atr
        lines.append(f"  ATR:    SL={sl_atr:.1f}x  TP={tp_atr:.1f}x  (ATR={_fmt_price(atr)})")

    # ── ICT context ──────────────────────────────────────────
    lines.append("")
    lines.append("🏛️ <b>ICT</b>")
    ob_str = "✅ OB" if ob_in_ote else "—"
    fvg_str = "✅ FVG" if fvg_in_ote else "—"
    lines.append(f"  AMD: {_esc(amd_phase)}  |  {ob_str}  {fvg_str}")
    lines.append(f"  HTF: {_esc(htf_bias)}  |  Regime: {_esc(regime)}")
    if reasons:
        lines.append(f"  Confluence: {', '.join(_esc(r) for r in reasons[:5])}")

    # ── Session ──────────────────────────────────────────────
    lines.append("")
    lines.append(f"{sess_quality} Session: {_esc(session)}{kz}")
    if nearest_liq_dist_atr > 0:
        lines.append(f"  Nearest opposing liq: {nearest_liq_dist_atr:.1f} ATR")
    lines.append(f"{'━' * 32}")

    return "\n".join(lines)


def _flow_bar(conviction: float) -> str:
    """Visual flow conviction bar."""
    n = max(0, min(5, int(abs(conviction) * 5)))
    if conviction > 0:
        return "▁" * (5 - n) + "▓" * n + " ▲"
    elif conviction < 0:
        return "▓" * n + "▁" * (5 - n) + " ▼"
    return "▁▁▁▁▁ ─"


# ======================================================================
# 3. POOL SWEEP ALERT — immediate notification
# ======================================================================

def format_pool_sweep_alert(
    side:         str,            # "long" | "short" (direction of sweep)
    pool_type:    str,            # "BSL" | "SSL"
    sweep_price:  float,
    current_price: float,
    displacement: bool  = False,
    wick_rejection: bool = False,
    ote_zone_low:  float = 0.0,
    ote_zone_high: float = 0.0,
    delivery_target: float = 0.0,
    flow_conviction: float = 0.0,
) -> str:
    """
    Immediate alert when a liquidity pool is swept with displacement.
    Sent as soon as sweep + displacement is confirmed — before entry.
    """
    icon  = "🌊"
    disp  = "✅ DISPLACEMENT CONFIRMED" if displacement else "⚠️ weak sweep"
    wick  = "  WICK REJECTION ✅" if wick_rejection else ""

    lines = [
        f"{icon} <b>POOL SWEEP: {pool_type} @ {_fmt_price(sweep_price)}</b>",
        f"  {disp}{wick}",
        f"  Price now: {_fmt_price(current_price)}",
        f"  Flow conviction: {flow_conviction:+.3f}",
    ]

    if ote_zone_low and ote_zone_high:
        lines.append(f"  OTE entry zone: [{_fmt_price(ote_zone_low)} – {_fmt_price(ote_zone_high)}]")
    if delivery_target:
        lines.append(f"  Delivery target (pool TP): {_fmt_price(delivery_target)}")

    if displacement:
        lines.append(f"\n  ⏳ Waiting for price to retrace into OTE for limit entry")
    else:
        lines.append(f"\n  ─ Monitoring — need displacement to confirm sweep")

    return "\n".join(lines)


# ======================================================================
# 4. TRAIL UPDATE — ICT structure basis only
# ======================================================================

def format_trail_update(
    side:              str,
    old_sl:            float,
    new_sl:            float,
    entry_price:       float,
    current_price:     float,
    trail_reason:      str,        # "BOS_swing" | "CHoCH_tighten" | "15m_structure"
    trail_phase:       str = "",   # "P1_BOS" | "P2_CHOCH" | "P3_15m"
    current_rr:        float = 0.0,
    profit_locked_pct: float = 0.0,
    breakeven_moved:   bool  = False,
    pool_tp:           float = 0.0,
    # Extended
    atr:               float = 0.0,
    peak_rr:           float = 0.0,
    session:           str   = "",
    sl_dist_atr:       float = 0.0,
    nearest_liq_price: float = 0.0,
) -> str:
    """
    Trail SL update — institutional format with progress visualization.
    """
    side_icon = "🟢" if side.upper() == "LONG" else "🔴"
    improvement = abs(new_sl - old_sl)

    phase_labels = {
        "P1_BOS":    "🟡 Phase 1 — BOS swing",
        "P2_CHOCH":  "🟠 Phase 2 — Structure confirmed",
        "P3_15m":    "🟢 Phase 3 — 15m macro trail",
    }
    phase_str = phase_labels.get(trail_phase, trail_phase)

    # Progress bar: how far from entry to TP
    if pool_tp and entry_price:
        total_dist = abs(pool_tp - entry_price)
        if total_dist > 0:
            progress = min(1.0, abs(current_price - entry_price) / total_dist)
            bar_len = 20
            filled = int(progress * bar_len)
            bar = "█" * filled + "░" * (bar_len - filled)
            pct = progress * 100
        else:
            bar = "░" * 20
            pct = 0
    else:
        bar = "░" * 20
        pct = 0

    lines = [
        f"🔒 <b>TRAIL SL</b>  {phase_str}",
        "",
        f"  {side_icon} SL: {_fmt_price(old_sl)} → <b>{_fmt_price(new_sl)}</b> (+{_fmt_price(improvement)})",
        f"  Entry: {_fmt_price(entry_price)}  |  Now: {_fmt_price(current_price)}",
    ]

    if atr > 0:
        dist_from_price = abs(current_price - new_sl) / atr
        lines.append(f"  Distance: {dist_from_price:.1f} ATR from price")

    lines.append(f"  R: {current_rr:+.2f}R  |  Peak: {peak_rr:.2f}R  |  Locked: {profit_locked_pct:.1f}R")
    lines.append(f"  Basis: {_esc(trail_reason)}")

    if pool_tp:
        lines.append(f"  Progress to TP: [{bar}] {pct:.0f}%")
        lines.append(f"  TP target: {_fmt_price(pool_tp)}")

    if breakeven_moved:
        lines.append("  🔒 Risk-free — break-even locked")

    if nearest_liq_price > 0:
        lines.append(f"  ⚡ Nearest liq: {_fmt_price(nearest_liq_price)}")

    if session:
        sess_icon = {"asia": "🌙", "london": "🌅", "ny": "🏛️"}.get(
            session.lower().replace(" ", "_"), "⚪")
        lines.append(f"  {sess_icon} {_esc(session)}")

    return "\n".join(lines)


# ======================================================================
# 5. POSITION CLOSE — Pool TP analysis
# ======================================================================

def format_position_close(
    side:          str,
    entry_price:   float,
    close_price:   float,
    sl_price:      float,
    tp_price:      float,
    pnl:           float,
    close_reason:  str,
    # Pool context
    pool_tp_price: float = 0.0,   # original pool target
    pool_reached:  bool  = False,
    # ICT context at entry
    ict_tier:      str   = "",
    amd_phase:     str   = "",
    trail_phase:   str   = "",
    # Trade metrics
    max_favorable: float = 0.0,
    max_adverse:   float = 0.0,
    breakeven_moved: bool = False,
    entry_fee:     float = 0.0,
    exit_fee:      float = 0.0,
    exact_fees:    bool  = False,
    # Session stats
    total_pnl:     float = 0.0,
    win_rate:      float = 0.0,
    total_trades:  int   = 0,
    consecutive_losses: int = 0,
    # Extended
    hold_minutes:  float = 0.0,
    entry_session: str   = "",
    exit_session:  str   = "",
    atr:           float = 0.0,
) -> str:
    """Position close with comprehensive post-trade analysis."""
    side_icon   = "🟢" if side.upper() == "LONG" else "🔴"
    risk        = abs(entry_price - sl_price) if sl_price else 0
    price_move  = ((close_price - entry_price) if side.upper() == "LONG"
                   else (entry_price - close_price))
    rr_achieved = price_move / risk if risk > 0 else 0

    # Trade grading
    if pnl > 0 and rr_achieved >= 2.0:
        grade = "A+"
        grade_icon = "🏆"
    elif pnl > 0 and rr_achieved >= 1.0:
        grade = "A"
        grade_icon = "✅"
    elif pnl > 0:
        grade = "B"
        grade_icon = "✅"
    elif pnl == 0 or abs(rr_achieved) < 0.1:
        grade = "C"
        grade_icon = "⚪"
    elif max_favorable > risk and pnl < 0:
        grade = "D"
        grade_icon = "⚠️"  # Had profit but gave it back — trail issue
    else:
        grade = "F"
        grade_icon = "❌"

    # Pool TP analysis
    pool_analysis = ""
    if pool_tp_price and pool_tp_price > 0:
        pool_dist    = abs(pool_tp_price - entry_price)
        pool_rr      = pool_dist / risk if risk > 0 else 0
        close_pct_of_pool = (abs(price_move) / pool_dist * 100) if pool_dist > 0 else 0
        if pool_reached:
            pool_analysis = f"\n  🎯 Pool {_fmt_price(pool_tp_price)} REACHED ✅"
        else:
            remaining = abs(pool_tp_price - close_price)
            pool_analysis = (f"\n  ─ Pool {_fmt_price(pool_tp_price)} "
                             f"({close_pct_of_pool:.0f}% captured, "
                             f"{_fmt_price(remaining)} remaining)")

    # MFE analysis — did we leave money on the table?
    mfe_analysis = ""
    if max_favorable > 0 and risk > 0:
        mfe_r = max_favorable / risk
        if pnl <= 0 and mfe_r >= 0.5:
            left_on_table = max_favorable - price_move
            mfe_analysis = (f"\n  ⚠️ Peak was +{_fmt_price(max_favorable)} "
                            f"({mfe_r:.1f}R) — gave back {_fmt_price(left_on_table)}")

    tier_str    = f" Tier-{ict_tier}" if ict_tier else ""
    total_fees  = entry_fee + exit_fee
    fee_tag     = "exact" if exact_fees else "est."

    lines = [
        f"{'━' * 32}",
        f"{grade_icon} <b>CLOSED {side.upper()}</b> — Grade: <b>{grade}</b>{tier_str}",
        f"{'━' * 32}",
        "",
    ]

    # ── Result ───────────────────────────────────────────────
    lines.append("💰 <b>RESULT</b>")
    pnl_icon = "🟢" if pnl > 0 else ("🔴" if pnl < 0 else "⚪")
    lines.append(f"  {pnl_icon} PnL: <b>{_fmt_price(pnl)}</b> ({rr_achieved:+.2f}R)")
    lines.append(f"  Reason: {_esc(close_reason)}")
    if pool_analysis:
        lines.append(pool_analysis)
    if mfe_analysis:
        lines.append(mfe_analysis)

    # ── Levels ───────────────────────────────────────────────
    lines.append("")
    lines.append("📊 <b>LEVELS</b>")
    lines.append(f"  Entry: {_fmt_price(entry_price)}  →  Exit: {_fmt_price(close_price)}")
    lines.append(f"  SL: {_fmt_price(sl_price)}  |  TP: {_fmt_price(tp_price)}")
    if atr > 0:
        move_atr = abs(price_move) / atr
        lines.append(f"  Move: {move_atr:.1f} ATR  |  ATR: {_fmt_price(atr)}")

    # ── Trade quality metrics ────────────────────────────────
    lines.append("")
    lines.append("📈 <b>METRICS</b>")
    if max_favorable > 0:
        mfe_r = max_favorable / risk if risk > 0 else 0
        lines.append(f"  MFE: +{_fmt_price(max_favorable)} ({mfe_r:.1f}R)")
    if max_adverse > 0:
        mae_r = max_adverse / risk if risk > 0 else 0
        lines.append(f"  MAE: -{_fmt_price(max_adverse)} ({mae_r:.1f}R)")
    if hold_minutes > 0:
        lines.append(f"  Hold: {hold_minutes:.0f}m")
    if breakeven_moved:
        lines.append("  🔒 BE was moved")
    if total_fees > 0:
        lines.append(f"  Fees: ${total_fees:.4f} ({fee_tag})")
    if trail_phase:
        lines.append(f"  Trail: {_esc(trail_phase)}")
    if amd_phase:
        lines.append(f"  AMD: {_esc(amd_phase)}")

    # ── Session context ──────────────────────────────────────
    if entry_session or exit_session:
        lines.append("")
        lines.append(f"  📍 {_esc(entry_session or '?')} → {_esc(exit_session or '?')}")

    # ── Session stats ────────────────────────────────────────
    lines.append("")
    lines.append("📊 <b>SESSION</b>")
    pnl_total_icon = "🟢" if total_pnl >= 0 else "🔴"
    lines.append(f"  {pnl_total_icon} Total PnL: {_fmt_price(total_pnl)}")
    lines.append(f"  Trades: {total_trades} | WR: {win_rate:.1f}%")
    if consecutive_losses > 0:
        lines.append(f"  ⚠️ Consecutive losses: {consecutive_losses}")
    lines.append(f"{'━' * 32}")

    return "\n".join(lines)


# ======================================================================
# 6. PERIODIC STATUS
# ======================================================================

def format_periodic_report(
    current_price:      float = 0.0,
    balance:            float = 0.0,
    total_trades:       int   = 0,
    win_rate:           float = 0.0,
    daily_pnl:          float = 0.0,
    total_pnl:          float = 0.0,
    consecutive_losses: int   = 0,
    bot_state:          str   = "SCANNING",
    # Pool state summary
    n_bsl_pools:        int   = 0,
    n_ssl_pools:        int   = 0,
    primary_target_str: str   = "—",
    flow_conviction:    float = 0.0,
    flow_direction:     str   = "",
    # ICT
    amd_phase:          str   = "UNKNOWN",
    session:            str   = "REGULAR",
    in_killzone:        bool  = False,
    regime:             str   = "UNKNOWN",
    # Position
    position:           Optional[Dict]  = None,
    current_sl:         Optional[float] = None,
    current_tp:         Optional[float] = None,
    entry_price:        Optional[float] = None,
    breakeven_moved:    bool  = False,
    profit_locked_pct:  float = 0.0,
    extra_lines:        Optional[List[str]] = None,
    # Extended
    atr:                float = 0.0,
    htf_bias:           str   = "",
    dealing_range_pd:   float = 0.5,
    **kwargs,
) -> str:
    """Periodic status — institutional dashboard format."""
    now_str = datetime.now(timezone.utc).strftime('%H:%M UTC')
    pnl_icon = "🟢" if daily_pnl >= 0 else "🔴"
    flow_bar = _flow_bar(flow_conviction)

    # Session icon
    sess_icon = {"asia": "🌙", "london": "🌅", "ny": "🏛️", "late_ny": "🌇"}.get(
        session.lower().replace(" ", "_"), "⚪")
    kz_str = " 🔥 KILLZONE" if in_killzone else ""

    # State icon
    state_icons = {
        "SCANNING": "🔍", "TRACKING": "📡", "READY": "🎯",
        "ENTERING": "⚡", "IN_POSITION": "📊", "POST_SWEEP": "🌊",
    }
    state_icon = state_icons.get(bot_state.upper(), "⚪")

    lines = [
        f"{'═' * 28}",
        f"📊 <b>BOT STATUS</b>  {now_str}",
        f"{'═' * 28}",
        "",
        f"💰 BTC: <b>{_fmt_price(current_price)}</b>",
    ]

    if atr > 0:
        lines.append(f"   ATR: {_fmt_price(atr)}  |  Regime: {_esc(regime)}")

    lines.append(f"   💵 Bal: {_fmt_price(balance)}  |  {pnl_icon} Day: {_fmt_price(daily_pnl)}")

    # ── State + Session ──────────────────────────────────────
    lines.append("")
    lines.append(f"{state_icon} <b>{_esc(bot_state)}</b>")
    lines.append(f"{sess_icon} {_esc(session)}{kz_str}")

    # ── Pool map summary ─────────────────────────────────────
    lines.append("")
    lines.append("🎯 <b>LIQUIDITY</b>")
    lines.append(f"  BSL ▲ {n_bsl_pools} pools  |  SSL ▼ {n_ssl_pools} pools")
    lines.append(f"  Target: {_esc(primary_target_str)}")
    lines.append(f"  Flow: {flow_bar}")

    # ── ICT context ──────────────────────────────────────────
    lines.append("")
    lines.append("🏛️ <b>ICT</b>")
    lines.append(f"  AMD: {_esc(amd_phase)}")
    if htf_bias:
        lines.append(f"  HTF: {_esc(htf_bias)}")
    if dealing_range_pd != 0.5:
        pd_label = ("DEEP DISC" if dealing_range_pd < 0.25 else
                    "DISCOUNT" if dealing_range_pd < 0.40 else
                    "EQ" if dealing_range_pd < 0.60 else
                    "PREMIUM" if dealing_range_pd < 0.75 else "DEEP PREM")
        lines.append(f"  Dealing range: {pd_label} ({dealing_range_pd:.0%})")

    # ── Position ─────────────────────────────────────────────
    if position:
        side    = position.get("side", "?").upper()
        p_entry = entry_price or position.get("entry_price", 0)
        qty     = float(position.get("quantity", 0) or 0)
        lines.append("")
        side_icon = "🟢" if side == "LONG" else "🔴"
        lines.append(f"{side_icon} <b>POSITION: {side}</b>")
        lines.append(f"  Entry: {_fmt_price(p_entry)}")
        if current_sl:
            lines.append(f"  SL: {_fmt_price(current_sl)}")
        if current_tp:
            lines.append(f"  TP: {_fmt_price(current_tp)}")

        if p_entry and current_price:
            move = (current_price - p_entry) if side == "LONG" else (p_entry - current_price)
            risk_d = abs(p_entry - current_sl) if current_sl else 0
            ur_r = move / risk_d if risk_d > 0 else 0
            upnl = move * qty if qty > 0 else move
            icon = "🟢" if move >= 0 else "🔴"
            if qty > 0:
                lines.append(f"  {icon} <b>${upnl:+.2f}</b> ({ur_r:+.1f}R)")
            else:
                lines.append(f"  {icon} {move:+.1f}pts ({ur_r:+.1f}R)")

        if breakeven_moved:
            lines.append(f"  🔒 BE locked | {profit_locked_pct:.1f}R secured")

        # Progress bar
        if current_tp and p_entry and current_price:
            total = abs(current_tp - p_entry)
            if total > 0:
                prog = min(1.0, max(0, abs(current_price - p_entry) / total))
                if move < 0:
                    prog = 0
                bar_len = 16
                filled = int(prog * bar_len)
                bar = "█" * filled + "░" * (bar_len - filled)
                lines.append(f"  [{bar}] {prog*100:.0f}%→TP")

    # ── Performance ──────────────────────────────────────────
    lines.append("")
    lines.append("📈 <b>PERFORMANCE</b>")
    lines.append(f"  Trades: {total_trades} | WR: {win_rate:.1f}%")
    lines.append(f"  Total PnL: {_fmt_price(total_pnl)}")
    if consecutive_losses > 0:
        lines.append(f"  ⚠️ Consec losses: {consecutive_losses}")

    if extra_lines:
        lines.append("")
        for el in extra_lines:
            if el and el.strip():
                lines.append(el)

    lines.append(f"{'═' * 28}")
    return "\n".join(lines)


# ======================================================================
# LOGGING HANDLER — forward WARNING+ to Telegram
# ======================================================================

class TelegramLogHandler(logging.Handler):
    """Forward WARNING+ logs to Telegram with throttling."""

    def __init__(self, level=logging.WARNING, throttle_seconds: float = 5.0):
        super().__init__(level)
        self._throttle = throttle_seconds
        self._last_ts  = 0.0
        self._lock      = threading.Lock()
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


def install_global_telegram_log_handler(level=logging.WARNING,
                                         throttle_seconds: float = 5.0) -> None:
    handler = TelegramLogHandler(level=level, throttle_seconds=throttle_seconds)
    handler.setFormatter(logging.Formatter("%(name)s: %(message)s"))
    logging.getLogger().addHandler(handler)

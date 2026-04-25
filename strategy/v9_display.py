# -*- coding: utf-8 -*-
"""
strategy/v9_display.py — Display Engine v10  (pretty terminal + Telegram)
==========================================================================
All functions are pure formatters — no side effects.

TERMINAL:  ANSI colours, box-drawing, progress bars, colour-coded signals.
TELEGRAM:  Clean HTML, consistent emoji anchors, signal-first layout.
"""

from __future__ import annotations

import time
import logging
import re
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# ANSI palette  (terminal only)
# ─────────────────────────────────────────────────────────────────────────────
class C:
    RST  = "\033[0m"
    BOLD = "\033[1m"
    DIM  = "\033[2m"

    BLK  = "\033[30m"; RED  = "\033[31m"; GRN  = "\033[32m"
    YLW  = "\033[33m"; BLU  = "\033[34m"; MAG  = "\033[35m"
    CYN  = "\033[36m"; WHT  = "\033[37m"

    BRED = "\033[91m"; BGRN = "\033[92m"; BYLW = "\033[93m"
    BBLU = "\033[94m"; BMAG = "\033[95m"; BCYN = "\033[96m"; BWHT = "\033[97m"

    BG_RED  = "\033[41m"; BG_GRN  = "\033[42m"
    BG_YLW  = "\033[43m"; BG_BLU  = "\033[44m"

def _c(text: str, *codes: str) -> str:
    return "".join(codes) + str(text) + C.RST

def _price(p: float) -> str:
    return f"${p:,.2f}"

def _esc(s: Any) -> str:
    return str(s).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

# ─────────────────────────────────────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────────────────────────────────────
def _bar(ratio: float, width: int = 20, full: str = "█", empty: str = "░") -> str:
    filled = max(0, min(width, int(ratio * width)))
    return full * filled + empty * (width - filled)

def _pnl_color(v: float) -> str:
    return C.BGRN if v >= 0 else C.BRED

def _ist_now() -> str:
    return datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%H:%M:%S IST")

def _flow_bar(conviction: float, width: int = 8) -> str:
    n = min(width, int(abs(conviction) * width))
    if conviction > 0.05:
        return _c("▓" * n + "░" * (width - n) + " ▲", C.BGRN)
    elif conviction < -0.05:
        return _c("▓" * n + "░" * (width - n) + " ▼", C.BRED)
    return _c("─" * width + " ─", C.DIM)


# ═════════════════════════════════════════════════════════════════════════════
# 1. TERMINAL HEARTBEAT  (main.py — every 60 s)
# ═════════════════════════════════════════════════════════════════════════════

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _visible_len(text: Any) -> int:
    return len(_ANSI_RE.sub("", str(text)))


def _term_progress(value: float, width: int = 18) -> str:
    filled = max(0, min(width, int(abs(value) * width + 0.5)))
    return "#" * filled + "." * (width - filled)


def _term_box(title: str, rows: List[str], width: int = 98, accent: str = C.BBLU) -> str:
    inner = width - 4
    title = f" {title.strip()} "
    top_fill = max(0, width - 2 - len(title))
    top = _c("+" + title + "-" * top_fill + "+", accent, C.BOLD)
    bottom = _c("+" + "-" * (width - 2) + "+", accent)
    out = [top]
    for row in rows:
        text = str(row)
        pad = max(0, inner - _visible_len(text))
        out.append(_c("| ", accent) + text + " " * pad + _c(" |", accent))
    out.append(bottom)
    return "\n".join(out)


def _term_section(name: str) -> str:
    return _c(f"[{name.upper()}]", C.BOLD, C.BWHT)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _pool_value(item: Any, attr: str, default: Any = None) -> Any:
    try:
        pool = getattr(item, "pool", item)
        return getattr(pool, attr, default)
    except Exception:
        return default


def _pool_metric(item: Any, attr: str, default: Any = None) -> Any:
    try:
        return getattr(item, attr, default)
    except Exception:
        return default


def _fmt_term_pool(item: Any, label: str, price: float, atr: float, target: Optional[Any]) -> str:
    px = _safe_float(_pool_value(item, "price", 0.0))
    dist = _pool_metric(item, "distance_atr", None)
    if dist is None and atr > 0 and px > 0:
        dist = abs(px - price) / atr
    sig = _safe_float(_pool_metric(item, "significance", _pool_value(item, "significance", 0.0)))
    tf = str(_pool_value(item, "timeframe", "") or "")
    touches = int(_safe_float(_pool_value(item, "touches", 0), 0))
    flags: List[str] = []
    if bool(_pool_value(item, "ob_aligned", False)):
        flags.append("OB")
    if bool(_pool_value(item, "fvg_aligned", False)):
        flags.append("FVG")
    htf = int(_safe_float(_pool_value(item, "htf_count", 0), 0))
    if htf >= 2:
        flags.append(f"HTF{htf}")
    is_target = False
    try:
        is_target = bool(target and abs(px - target.pool.price) <= max(atr * 0.3, 30.0))
    except Exception:
        pass
    mark = _c("TARGET", C.BYLW, C.BOLD) if is_target else ""
    color = C.BGRN if label == "BSL" else C.BRED
    return (
        f"{_c(label, color, C.BOLD):<12} {_c(_price(px), C.BCYN):<20} "
        f"{_safe_float(dist):>4.1f} ATR  sig {sig:>5.1f}  t {touches:<2d} "
        f"{tf:<4} {'/'.join(flags):<12} {mark}"
    )


def _target_summary(target: Optional[Any]) -> str:
    if target is None:
        return _c("none", C.DIM)
    try:
        side = str(getattr(target, "direction", "") or "").upper()
        px = _pool_value(target, "price", getattr(target.pool, "price", 0.0))
        dist = _safe_float(getattr(target, "distance_atr", 0.0))
        sig = _safe_float(getattr(target, "significance", 0.0))
        sources = getattr(target, "tf_sources", None) or []
        src = f" | TF {','.join(str(x) for x in sources[:4])}" if sources else ""
        return f"{_c(side or 'POOL', C.BYLW, C.BOLD)} -> {_c(_price(px), C.BCYN)} | {dist:.1f} ATR | sig {sig:.0f}{src}"
    except Exception:
        return _c("detected", C.BYLW)


def _format_heartbeat_industry(
    price: float,
    feed: str,
    exchange: str,
    position: Optional[Dict],
    engine_state: str,
    tracking_info: Optional[Dict],
    primary_target: Optional[Any],
    nearest_bsl_atr: float,
    nearest_ssl_atr: float,
    recent_sweep_count: int,
    total_trades: int,
    total_pnl: float,
    flow_conviction: float = 0.0,
    flow_direction: str = "",
    bsl_pools: Optional[list] = None,
    ssl_pools: Optional[list] = None,
    atr: float = 0.0,
    cvd_trend: float = 0.0,
    tick_flow: float = 0.0,
    session: str = "",
    kill_zone: str = "",
    amd_phase: str = "",
    amd_bias: str = "",
    dealing_range_pd: float = 0.5,
    structure_15m: str = "",
    structure_4h: str = "",
    sweep_analysis: Optional[Dict] = None,
    htf_bias: str = "",
) -> str:
    rows: List[str] = []
    side_color = C.BCYN
    if position:
        side_color = C.BGRN if str(position.get("side", "")).lower() == "long" else C.BRED
    state_color = C.BGRN if engine_state in ("READY", "IN_POSITION") else C.BCYN
    kz = _c("KILLZONE", C.BYLW, C.BOLD) if kill_zone else _c("off-kz", C.DIM)
    pd_label = (
        "deep-discount" if dealing_range_pd < 0.25 else
        "discount" if dealing_range_pd < 0.40 else
        "equilibrium" if dealing_range_pd < 0.60 else
        "premium" if dealing_range_pd < 0.75 else "deep-premium"
    )

    rows.append(
        f"{_term_section('market')} {_c(_price(price), C.BOLD, side_color)}  "
        f"feed {feed or '-'} | exchange {exchange or '-'} | ATR {_c(f'${atr:,.1f}' if atr else '-', C.YLW)}"
    )
    rows.append(
        f"state {_c(engine_state or 'SCANNING', C.BOLD, state_color)} | "
        f"session {(session or '-').upper()} | {kz} | {_c(_ist_now(), C.DIM)}"
    )
    rows.append(
        f"AMD {amd_phase or '-'} / {amd_bias or '-'} | PD {pd_label} {dealing_range_pd:.0%} | "
        f"15m {structure_15m or '-'} | 4H {structure_4h or '-'} | HTF {htf_bias or '-'}"
    )
    rows.append("")
    rows.append(f"{_term_section('liquidity')} target {_target_summary(primary_target)}")
    rows.append(
        f"nearest BSL {_c(f'{nearest_bsl_atr:.1f} ATR', C.BGRN)} | "
        f"nearest SSL {_c(f'{nearest_ssl_atr:.1f} ATR', C.BRED)} | recent sweeps {recent_sweep_count}"
    )
    for item in (bsl_pools or [])[:3]:
        rows.append(_fmt_term_pool(item, "BSL", price, atr, primary_target))
    for item in (ssl_pools or [])[:3]:
        rows.append(_fmt_term_pool(item, "SSL", price, atr, primary_target))

    rows.append("")
    flow_bar = _term_progress(min(1.0, abs(flow_conviction)), 20)
    flow_col = C.BGRN if flow_conviction > 0.05 else (C.BRED if flow_conviction < -0.05 else C.DIM)
    rows.append(
        f"{_term_section('flow')} {_c((flow_direction or 'neutral').upper(), C.BOLD, flow_col)} "
        f"[{_c(flow_bar, flow_col)}] {flow_conviction:+.2f} | CVD {cvd_trend:+.2f} | tick {tick_flow:+.2f}"
    )
    if tracking_info:
        rows.append(
            f"tracking {str(tracking_info.get('direction', '?')).upper()} -> "
            f"{tracking_info.get('target', '?')} | ticks {tracking_info.get('flow_ticks', 0)} | "
            f"started {tracking_info.get('started', '-')}"
        )
    if sweep_analysis:
        rev = _safe_float(sweep_analysis.get("reversal_score", sweep_analysis.get("rev_score", 0.0)))
        cont = _safe_float(sweep_analysis.get("continuation_score", sweep_analysis.get("cont_score", 0.0)))
        sweep_side = sweep_analysis.get("sweep_side", "?")
        sweep_px = _safe_float(sweep_analysis.get("sweep_price", 0.0))
        winner = "REVERSAL" if rev > cont + 15 else ("CONTINUATION" if cont > rev + 15 else "CONTESTED")
        rows.append(f"sweep {sweep_side} @ {_price(sweep_px)} | REV {rev:.1f} vs CONT {cont:.1f} | {winner}")

    rows.append("")
    if position:
        side = str(position.get("side", "?")).upper()
        entry = _safe_float(position.get("entry_price", 0.0))
        sl = _safe_float(position.get("sl_price", 0.0))
        tp = _safe_float(position.get("tp_price", 0.0))
        qty = _safe_float(position.get("quantity", 0.0))
        init_sl = _safe_float(position.get("initial_sl_dist", abs(entry - sl)), abs(entry - sl))
        move = (price - entry) if side == "LONG" else (entry - price)
        r_now = move / init_sl if init_sl > 1e-10 else 0.0
        rr = abs(tp - entry) / abs(entry - sl) if entry and sl and tp and abs(entry - sl) > 1e-10 else 0.0
        upnl = move * qty if qty else move
        prog = min(1.0, max(0.0, abs(price - entry) / max(abs(tp - entry), 1e-9))) if move >= 0 and tp else 0.0
        rows.append(
            f"{_term_section('position')} {_c(side, C.BOLD, side_color)} qty {qty:.6f} | "
            f"entry {_c(_price(entry), C.CYN)} | mark {_c(_price(price), C.BCYN)}"
        )
        if atr > 0:
            rows.append(
                f"SL {_c(_price(sl), C.BRED)} ({abs(price - sl) / atr:.1f} ATR) | "
                f"TP {_c(_price(tp), C.BGRN)} ({abs(tp - price) / atr:.1f} ATR) | planned R:R 1:{rr:.2f}"
            )
        else:
            rows.append(f"SL {_c(_price(sl), C.BRED)} | TP {_c(_price(tp), C.BGRN)} | planned R:R 1:{rr:.2f}")
        rows.append(
            f"unrealized {_c(f'${upnl:+,.2f}' if qty else f'{upnl:+,.1f} pts', _pnl_color(upnl))} | "
            f"R {_c(f'{r_now:+.2f}', _pnl_color(r_now))} | to-target [{_c(_term_progress(prog, 20), C.BGRN)}] {prog:.0%}"
        )
    else:
        rows.append(f"{_term_section('position')} flat | scanning with no synthetic fallback levels")

    rows.append(
        f"{_term_section('performance')} trades {total_trades} | session PnL "
        f"{_c(_price(total_pnl), _pnl_color(total_pnl))}"
    )
    return _term_box("DELTA LIQUIDITY ENGINE", rows)


def _format_thinking_terminal_industry(
    engine_state: str,
    flow_direction: str,
    flow_conviction: float,
    cvd_trend: float,
    tick_flow: float,
    ob_imbalance: float,
    primary_target: Optional[Any],
    nearest_bsl_atr: float,
    nearest_ssl_atr: float,
    recent_sweep_count: int,
    tracking_info: Optional[Dict],
    amd_phase: str = "",
    amd_bias: str = "",
    structure_5m: str = "",
    kill_zone: str = "",
    price: float = 0.0,
    atr: float = 0.0,
) -> str:
    flow_col = C.BGRN if flow_conviction > 0.05 else (C.BRED if flow_conviction < -0.05 else C.DIM)
    rows = [
        f"price {_c(_price(price), C.BCYN, C.BOLD)} | ATR {_c(f'${atr:,.1f}' if atr else '-', C.YLW)} | "
        f"state {_c(engine_state or 'SCANNING', C.BOLD, C.BCYN)} | {_c(_ist_now(), C.DIM)}",
        f"flow {_c((flow_direction or 'neutral').upper(), C.BOLD, flow_col)} "
        f"[{_c(_term_progress(min(1.0, abs(flow_conviction)), 18), flow_col)}] {flow_conviction:+.2f} | "
        f"tick {tick_flow:+.2f} | CVD {cvd_trend:+.2f} | OB {ob_imbalance:+.2f}",
        f"target {_target_summary(primary_target)}",
        f"liquidity BSL {nearest_bsl_atr:.1f} ATR | SSL {nearest_ssl_atr:.1f} ATR | sweeps {recent_sweep_count}",
        f"ICT AMD {amd_phase or '-'} / {amd_bias or '-'} | 5m {structure_5m or '-'} | killzone {kill_zone or '-'}",
    ]
    if tracking_info:
        rows.append(
            f"tracking {str(tracking_info.get('direction', '?')).upper()} -> "
            f"{tracking_info.get('target', '?')} | ticks {tracking_info.get('flow_ticks', 0)}"
        )
    return _term_box("ENGINE THINKING", rows, width=92, accent=C.BCYN)


def format_heartbeat(
    price: float,
    feed: str,
    exchange: str,
    position: Optional[Dict],
    engine_state: str,
    tracking_info: Optional[Dict],
    primary_target: Optional[Any],
    nearest_bsl_atr: float,
    nearest_ssl_atr: float,
    recent_sweep_count: int,
    total_trades: int,
    total_pnl: float,
    flow_conviction: float = 0.0,
    flow_direction: str = "",
    bsl_pools: Optional[list] = None,
    ssl_pools: Optional[list] = None,
    atr: float = 0.0,
    cvd_trend: float = 0.0,
    tick_flow: float = 0.0,
    session: str = "",
    kill_zone: str = "",
    amd_phase: str = "",
    amd_bias: str = "",
    dealing_range_pd: float = 0.5,
    structure_15m: str = "",
    structure_4h: str = "",
    sweep_analysis: Optional[Dict] = None,
    htf_bias: str = "",
) -> str:
    return _format_heartbeat_industry(
        price=price,
        feed=feed,
        exchange=exchange,
        position=position,
        engine_state=engine_state,
        tracking_info=tracking_info,
        primary_target=primary_target,
        nearest_bsl_atr=nearest_bsl_atr,
        nearest_ssl_atr=nearest_ssl_atr,
        recent_sweep_count=recent_sweep_count,
        total_trades=total_trades,
        total_pnl=total_pnl,
        flow_conviction=flow_conviction,
        flow_direction=flow_direction,
        bsl_pools=bsl_pools,
        ssl_pools=ssl_pools,
        atr=atr,
        cvd_trend=cvd_trend,
        tick_flow=tick_flow,
        session=session,
        kill_zone=kill_zone,
        amd_phase=amd_phase,
        amd_bias=amd_bias,
        dealing_range_pd=dealing_range_pd,
        structure_15m=structure_15m,
        structure_4h=structure_4h,
        sweep_analysis=sweep_analysis,
        htf_bias=htf_bias,
    )
    W = 72
    TOP  = _c("╔" + "═" * (W - 2) + "╗", C.BBLU)
    BOT  = _c("╚" + "═" * (W - 2) + "╝", C.BBLU)
    MID  = _c("╠" + "═" * (W - 2) + "╣", C.BBLU)
    SEP  = _c("├" + "─" * (W - 2) + "┤", C.BLU)
    BAR  = lambda s: _c("║ ", C.BBLU) + s + _c(" ║", C.BBLU)

    def pad(s: str, raw_len: int) -> str:
        """Pad to fill inner width accounting for ANSI codes."""
        inner = W - 4
        vis   = inner - raw_len
        return s + " " * max(0, vis)

    now = _ist_now()
    atr_str   = f"${atr:.1f}" if atr > 0 else "—"
    pd_label  = (
        "DEEP-DISC" if dealing_range_pd < 0.25 else
        "DISCOUNT"  if dealing_range_pd < 0.40 else
        "EQUIL"     if dealing_range_pd < 0.60 else
        "PREMIUM"   if dealing_range_pd < 0.75 else "DEEP-PREM"
    )
    sess_map  = {"asia": "🌙", "london": "🌅", "ny": "🏛️", "late_ny": "🌇"}
    sess_icon = sess_map.get((session or "").lower().replace(" ", "_"), "⚪")
    kz_str    = _c(" 🔥 KZ", C.BYLW) if kill_zone else ""

    lines = [TOP]

    # ── IN POSITION ───────────────────────────────────────────────────────────
    if position:
        side  = position.get("side", "?").upper()
        entry = position.get("entry_price", 0.0)
        sl    = position.get("sl_price", 0.0)
        tp    = position.get("tp_price", 0.0)
        if entry <= 0 or side not in ("LONG", "SHORT"):
            lines.append(BAR(_c(f"  {_price(price)}  [{feed}]  PENDING FILL", C.BYLW)))
            lines.append(BOT)
            return "\n".join(lines)

        pnl       = (price - entry) if side == "LONG" else (entry - price)
        init_sl   = position.get("initial_sl_dist", abs(entry - sl)) or abs(entry - sl)
        curr_r    = pnl / init_sl if init_sl > 1e-10 else 0.0
        sl_atr    = abs(price - sl) / atr if atr > 1e-10 else 0.0
        tp_atr    = abs(price - tp) / atr if atr > 1e-10 else 0.0
        peak_r    = position.get("peak_profit", pnl) / init_sl if init_sl > 1e-10 else 0.0
        trail     = position.get("trail_active", False)
        progress  = min(1.0, max(0, abs(price - entry) / max(abs(tp - entry), 1))) if pnl >= 0 else 0.0
        pc        = C.BGRN if side == "LONG" else C.BRED
        pi        = "▲ LONG" if side == "LONG" else "▼ SHORT"

        header = f"  {_c(pi, C.BOLD, pc)}   {_c(_price(price), C.BOLD, C.BCYN)}   {sess_icon} {(session or '').upper()}{kz_str}   {_c(now, C.DIM)}"
        lines.append(BAR(header))
        lines.append(MID)

        lines.append(BAR(f"  Entry  {_c(_price(entry), C.CYN)}   ATR {_c(atr_str, C.YLW)}   {_c(pd_label, C.DIM)}"))
        lines.append(BAR(f"  SL     {_c(_price(sl), C.BRED)}   ({sl_atr:.1f} ATR){_c('  🔒 TRAIL', C.BYLW) if trail else ''}"))
        lines.append(BAR(f"  TP     {_c(_price(tp), C.BGRN)}   ({tp_atr:.1f} ATR)"))
        lines.append(SEP)

        pnl_col = C.BGRN if pnl >= 0 else C.BRED
        pnl_line = f"  PnL  {_c(f'{pnl:+.1f} pts', C.BOLD, pnl_col)}   {_c(f'{curr_r:+.2f}R', pnl_col)}   Peak {_c(f'{peak_r:.2f}R', C.DIM)}"
        lines.append(BAR(pnl_line))
        bar_str  = f"  [{_c(_bar(progress), C.BGRN)}] {progress*100:.0f}% → TP"
        lines.append(BAR(bar_str))
        lines.append(SEP)
        lines.append(BAR(f"  AMD {_c(amd_phase or '—', C.MAG)}({amd_bias or '—'})   15m {_c(structure_15m or '—', C.CYN)}   4H {_c(structure_4h or '—', C.CYN)}"))
        lines.append(BAR(f"  Flow {_flow_bar(flow_conviction)}  {flow_direction or 'neutral'}({flow_conviction:+.2f})   CVD {cvd_trend:+.2f}   Tick {tick_flow:+.2f}"))
        lines.append(SEP)
        lines.append(BAR(f"  Trades {total_trades}   Session PnL {_c(_price(total_pnl), _pnl_color(total_pnl))}"))
        lines.append(BOT)
        return "\n".join(lines)

    # ── SCANNING / TRACKING ───────────────────────────────────────────────────
    state_map = {
        "TRACKING":   _c("📡 TRACKING",   C.BCYN),
        "READY":      _c("🎯 READY",      C.BGRN, C.BOLD),
        "POST_SWEEP": _c("🌊 POST-SWEEP", C.BMAG),
        "SCANNING":   _c("🔍 SCANNING",   C.DIM),
    }
    state_str = state_map.get(engine_state, _c(engine_state, C.DIM))
    if engine_state == "TRACKING" and tracking_info:
        d = tracking_info.get("direction", "?").upper()
        t = tracking_info.get("target", "?")
        n = tracking_info.get("flow_ticks", 0)
        state_str = _c(f"📡 TRACKING {d}→{t}  ({n} ticks)", C.BCYN)

    header = f"  ⚡ v10 LIQUIDITY-FIRST   {_c(_price(price), C.BOLD, C.BCYN)}   {sess_icon} {(session or '').upper()}{kz_str}   {_c(now, C.DIM)}"
    sub    = f"  {state_str}   ATR {_c(atr_str, C.YLW)}   {_c(pd_label, C.DIM)}"
    lines.append(BAR(header))
    lines.append(BAR(sub))
    lines.append(MID)

    # Pool rows
    def _prow(pools, label, near_atr, color):
        rows = []
        if pools:
            for i, p in enumerate(pools[:4]):
                try:
                    flags = []
                    if getattr(p.pool, "ob_aligned",  False): flags.append("OB")
                    if getattr(p.pool, "fvg_aligned", False): flags.append("FVG")
                    htf = getattr(p.pool, "htf_count", 0)
                    if htf >= 2: flags.append(f"HTF×{htf}")
                    f_str = f" [{','.join(flags)}]" if flags else ""
                    is_tgt = (primary_target is not None and
                              abs(p.pool.price - primary_target.pool.price) < max(atr * 0.3, 30))
                    tgt    = _c(" ◀ TARGET", C.BYLW, C.BOLD) if is_tgt else ""
                    tf     = getattr(p.pool, "timeframe", "")
                    da     = getattr(p, "distance_atr",  0.0)
                    sig    = getattr(p, "significance",  0.0)
                    tch    = getattr(p.pool, "touches",  0)
                    pfx    = f"  {label}" if i == 0 else "       "
                    rows.append(BAR(
                        f"{_c(pfx, color)}  {_c(_price(p.pool.price), C.BCYN)}"
                        f"  {da:.1f}ATR  sig={sig:.0f}  t={tch}"
                        f"{f'  {tf}' if tf else ''}  {_c(f_str, C.DIM)}{tgt}"
                    ))
                except Exception:
                    pass
        else:
            rows.append(BAR(f"  {_c(label, color)}  {near_atr:.1f} ATR away"))
        return rows

    lines.extend(_prow(bsl_pools or [], "BSL ▲", nearest_bsl_atr, C.BGRN))
    lines.append(BAR(""))
    lines.extend(_prow(ssl_pools or [], "SSL ▼", nearest_ssl_atr, C.BRED))
    lines.append(SEP)

    # Target
    if primary_target:
        try:
            t = primary_target
            lines.append(BAR(
                f"  🎯 Target  {_c(t.direction.upper(), C.BOLD)}  →  {_c(_price(t.pool.price), C.BCYN)}"
                f"   {t.distance_atr:.1f} ATR  sig={t.significance:.0f}"
            ))
        except Exception:
            lines.append(BAR("  🎯 Target  —"))
    else:
        lines.append(BAR(f"  🎯 Target  {_c('none', C.DIM)}"))

    lines.append(BAR(
        f"  Flow  {_flow_bar(flow_conviction)}  {flow_direction or 'neutral'}({flow_conviction:+.2f})"
        f"   CVD {cvd_trend:+.2f}   Tick {tick_flow:+.2f}"
    ))
    lines.append(BAR(
        f"  AMD {_c(amd_phase or '—', C.MAG)}({amd_bias or '—'})"
        f"   15m {_c(structure_15m or '—', C.CYN)}"
        f"   4H  {_c(structure_4h  or '—', C.CYN)}"
    ))

    # Post-sweep inset
    if engine_state == "POST_SWEEP" and sweep_analysis:
        rs      = sweep_analysis.get("rev_score", 0)
        cs      = sweep_analysis.get("cont_score", 0)
        sw_side = sweep_analysis.get("sweep_side", "?")
        sw_px   = sweep_analysis.get("sweep_price", 0)
        sw_q    = sweep_analysis.get("sweep_quality", 0)
        total   = max(rs + cs, 1)
        rev_w   = int(rs / total * 20)
        bar     = _c("◀" + "█" * rev_w, C.BRED) + _c("░" * (20 - rev_w) + "▶", C.BGRN)
        winner  = ("REVERSAL" if rs >= 45 and abs(rs-cs) >= 10 else
                   "CONTINUATION" if cs >= 40 and abs(rs-cs) >= 10 else "WAIT")
        lines.append(SEP)
        lines.append(BAR(f"  🌊 SWEEP  {sw_side} @ ${sw_px:,.0f}  q={sw_q:.0%}"))
        lines.append(BAR(f"  {bar}"))
        lines.append(BAR(f"  REV={rs:.0f}  CONT={cs:.0f}  → {_c(winner, C.BOLD, C.BYLW)}"))

    lines.append(SEP)
    sweep_p = f"Sweeps={recent_sweep_count}" if recent_sweep_count else ""
    stats   = "  ".join(p for p in [sweep_p, f"Trades={total_trades}", f"PnL={_c(_price(total_pnl), _pnl_color(total_pnl))}"] if p)
    lines.append(BAR(f"  {stats}"))
    lines.append(BOT)
    return "\n".join(lines)


# ═════════════════════════════════════════════════════════════════════════════
# 2. TERMINAL THINKING LOG  (quant_strategy.py — every 30 s)
# ═════════════════════════════════════════════════════════════════════════════

def format_thinking_terminal(
    engine_state: str,
    flow_direction: str,
    flow_conviction: float,
    cvd_trend: float,
    tick_flow: float,
    ob_imbalance: float,
    primary_target: Optional[Any],
    nearest_bsl_atr: float,
    nearest_ssl_atr: float,
    recent_sweep_count: int,
    tracking_info: Optional[Dict],
    amd_phase: str = "",
    amd_bias: str = "",
    structure_5m: str = "",
    kill_zone: str = "",
    price: float = 0.0,
    atr: float = 0.0,
) -> str:
    return _format_thinking_terminal_industry(
        engine_state=engine_state,
        flow_direction=flow_direction,
        flow_conviction=flow_conviction,
        cvd_trend=cvd_trend,
        tick_flow=tick_flow,
        ob_imbalance=ob_imbalance,
        primary_target=primary_target,
        nearest_bsl_atr=nearest_bsl_atr,
        nearest_ssl_atr=nearest_ssl_atr,
        recent_sweep_count=recent_sweep_count,
        tracking_info=tracking_info,
        amd_phase=amd_phase,
        amd_bias=amd_bias,
        structure_5m=structure_5m,
        kill_zone=kill_zone,
        price=price,
        atr=atr,
    )
    ts    = _ist_now()
    state = _c(engine_state, C.BOLD, C.BCYN if engine_state != "SCANNING" else C.DIM)
    hdr   = (
        f"  {_c('▸ THINK', C.BOLD, C.BBLU)}"
        f"  {_c(_price(price), C.BCYN)}"
        f"  ATR {_c(f'${atr:.1f}', C.YLW)}"
        f"  {state}"
        f"  {_c(ts, C.DIM)}"
    )
    W   = 68
    sep = _c("  " + "·" * (W - 2), C.DIM)

    tgt_str = _c("none", C.DIM)
    if primary_target:
        try:
            t = primary_target
            tgt_str = (
                f"{_c(t.direction, C.BOLD)} → {_c(_price(t.pool.price), C.BCYN)}"
                f"  {t.distance_atr:.1f}ATR  sig={t.significance:.0f}"
            )
        except Exception:
            pass

    ict_parts = []
    if amd_phase:    ict_parts.append(f"AMD={_c(amd_phase[:6], C.MAG)}")
    if amd_bias:     ict_parts.append(f"Bias={amd_bias}")
    if structure_5m: ict_parts.append(f"5m={_c(structure_5m, C.CYN)}")
    if kill_zone:    ict_parts.append(_c(f"🔥 KZ={kill_zone}", C.BYLW))

    lines = [
        sep, hdr, sep,
        f"  Flow  {_flow_bar(flow_conviction)}  {flow_direction or 'neutral'}"
        f"({flow_conviction:+.2f})   Tick {tick_flow:+.2f}   CVD {cvd_trend:+.2f}   OB {ob_imbalance:+.2f}",
        f"  Target  {tgt_str}",
        f"  BSL {_c(f'{nearest_bsl_atr:.1f}ATR', C.BGRN)}   SSL {_c(f'{nearest_ssl_atr:.1f}ATR', C.BRED)}"
        f"   Sweeps(5m)={recent_sweep_count}",
    ]
    if ict_parts:
        lines.append(f"  ICT   {' │ '.join(ict_parts)}")
    if tracking_info:
        d = tracking_info.get("direction","?").upper()
        t = tracking_info.get("target","?")
        n = tracking_info.get("flow_ticks", 0)
        s = tracking_info.get("started","")
        lines.append(f"  {_c('▶ Tracking', C.BCYN, C.BOLD)}  {d} → {t}   {n} ticks   {_c(s, C.DIM)}")
    lines.append(sep)
    return "\n".join(lines)


# ═════════════════════════════════════════════════════════════════════════════
# 3. TELEGRAM: /thinking
# ═════════════════════════════════════════════════════════════════════════════

def format_thinking_telegram(
    price: float,
    atr: float,
    atr_pctile: float,
    engine_state: str,
    flow_direction: str,
    flow_conviction: float,
    tick_flow: float,
    cvd_trend: float,
    ob_imbalance: float,
    tick_streak: int,
    bsl_pools: list,
    ssl_pools: list,
    primary_target: Optional[Any],
    nearest_bsl_atr: float,
    nearest_ssl_atr: float,
    recent_sweeps: list,
    amd_phase: str = "",
    amd_bias: str = "",
    amd_confidence: float = 0.0,
    in_premium: bool = False,
    in_discount: bool = False,
    structure_5m: str = "",
    structure_15m: str = "",
    kill_zone: str = "",
    tracking_info: Optional[Dict] = None,
    position: Optional[Dict] = None,
    trail_phase: str = "",
) -> str:
    STATE_ICONS = {
        "SCANNING":   "🔍", "TRACKING":   "📡", "READY":      "🎯",
        "ENTERING":   "⚡", "IN_POSITION":"📊", "POST_SWEEP": "🌊",
    }
    si = STATE_ICONS.get(engine_state, "⚪")
    fc_bar = "█" * min(10, int(abs(flow_conviction) * 10)) + "░" * max(0, 10 - int(abs(flow_conviction) * 10))
    fc_dir = "▲" if flow_conviction > 0.05 else ("▼" if flow_conviction < -0.05 else "─")
    zone   = "DISCOUNT" if in_discount else ("PREMIUM" if in_premium else "EQUILIBRIUM")

    lines = [
        f"<b>🧠 THINKING  •  ${price:,.2f}</b>",
        f"<code>{si} {_esc(engine_state):<12}  ATR ${atr:.1f} ({atr_pctile:.0%})</code>",
    ]

    if tracking_info:
        d = tracking_info.get("direction","?").upper()
        t = tracking_info.get("target","?")
        n = tracking_info.get("flow_ticks", 0)
        lines.append(f"  <b>Tracking</b>  {_esc(d)} → {_esc(t)}  ({n} ticks)")

    # Flow
    lines += [
        "",
        f"<b>⚡ Order Flow</b>",
        f"  Direction   <b>{_esc((flow_direction or 'neutral').upper())}</b>  [{_esc(fc_bar)}] {fc_dir}  {flow_conviction:+.2f}",
        f"  Tick {tick_flow:+.2f}   Streak {tick_streak}   CVD {cvd_trend:+.2f}   OB {ob_imbalance:+.2f}",
    ]
    thresh_delta = abs(flow_conviction) - 0.55
    gate_str = (f"✅ +{thresh_delta:.2f} above threshold" if thresh_delta >= 0
                else f"⛔ {thresh_delta:.2f} below threshold")
    lines.append(f"  Gate  {gate_str}")

    # Liquidity
    lines += ["", "<b>💧 Liquidity</b>"]
    lines.append(f"  BSL {nearest_bsl_atr:.1f}ATR  ·  SSL {nearest_ssl_atr:.1f}ATR")
    if primary_target:
        try:
            t = primary_target
            lines.append(
                f"  🎯 <b>{_esc(t.direction.upper())} → ${t.pool.price:,.1f}</b>"
                f"  {t.distance_atr:.1f}ATR  sig={t.significance:.0f}  t={t.pool.touches}"
            )
            if t.tf_sources:
                lines.append(f"     TFs: {_esc(', '.join(t.tf_sources))}")
        except Exception:
            lines.append("  Target  —")
    else:
        lines.append("  Target  none")

    for label, pools in [("BSL ▲", bsl_pools[:3]), ("SSL ▼", ssl_pools[:3])]:
        if pools:
            lines.append(f"  <b>{label}</b>")
            for p in pools:
                try:
                    flags = []
                    if p.pool.ob_aligned: flags.append("OB")
                    if p.pool.fvg_aligned: flags.append("FVG")
                    if p.pool.htf_count >= 2: flags.append(f"HTF×{p.pool.htf_count}")
                    f_str = f" [{','.join(flags)}]" if flags else ""
                    lines.append(
                        f"    ${p.pool.price:,.1f}  {p.distance_atr:.1f}ATR"
                        f"  sig={p.significance:.0f}  t={p.pool.touches}{_esc(f_str)}"
                    )
                except Exception:
                    pass

    if recent_sweeps:
        lines.append(f"  <b>Sweeps ({len(recent_sweeps)})</b>")
        for s in recent_sweeps[:3]:
            try:
                age = time.time() - s.detected_at
                lines.append(
                    f"    {_esc(s.pool.side.value)} ${s.pool.price:,.1f}"
                    f"  q={s.quality:.2f}  {age:.0f}s ago"
                )
            except Exception:
                pass

    # ICT
    lines += ["", "<b>🏛 ICT Context</b>"]
    if amd_phase:
        lines.append(f"  AMD  <b>{_esc(amd_phase)}</b>  {_esc(amd_bias or 'neutral')}  conf={amd_confidence:.2f}")
    lines.append(f"  Zone {_esc(zone)}  ·  5m {_esc(structure_5m or '?')}  ·  15m {_esc(structure_15m or '?')}")
    if kill_zone:
        lines.append(f"  🔥 Kill zone  <b>{_esc(kill_zone.upper())}</b>")

    # Position
    if position:
        side  = position.get("side","?").upper()
        entry = position.get("entry_price", 0.0)
        sl    = position.get("sl_price", 0.0)
        tp    = position.get("tp_price", 0.0)
        if entry > 0:
            pnl_pts = (price - entry) if side == "LONG" else (entry - price)
            init_sl = position.get("initial_sl_dist", abs(entry - sl))
            curr_r  = pnl_pts / init_sl if init_sl > 1e-10 else 0.0
            icon    = "🟢" if side == "LONG" else "🔴"
            lines += [
                "",
                f"<b>{icon} Position  {_esc(side)}</b>",
                f"  ${entry:,.2f}  →  {pnl_pts:+.1f} pts  ({curr_r:+.1f}R)",
                f"  SL ${sl:,.2f}  ·  TP ${tp:,.2f}",
            ]
            if trail_phase:
                lines.append(f"  Trail  {_esc(trail_phase)}")

    # Verdict
    verdicts = {
        "TRACKING":   "Building conviction — flow sustained toward pool",
        "READY":      "⚡ Entry imminent — conviction met",
        "POST_SWEEP": "Evaluating: reverse, continue, or wait?",
        "IN_POSITION":"Managing active trade",
    }
    verdict = verdicts.get(engine_state) or (
        "Waiting for directional flow" if abs(flow_conviction) < 0.3 else
        "Flow present — no pool in range" if not primary_target else
        "Monitoring flow alignment with target"
    )
    lines += ["", f"<b>💬 Verdict</b>", f"  {_esc(verdict)}"]
    return "\n".join(lines)


# ═════════════════════════════════════════════════════════════════════════════
# 4. TELEGRAM: /pools
# ═════════════════════════════════════════════════════════════════════════════

def format_pools_telegram(
    price: float,
    atr: float,
    bsl_pools: list,
    ssl_pools: list,
    primary_target: Optional[Any],
    recent_sweeps: list,
    tf_coverage: Dict[str, int],
) -> str:
    tf_parts = [f"{tf}:{n}" for tf in ["1m","5m","15m","1h","4h","1d"]
                if (n := tf_coverage.get(tf, 0)) > 0]

    lines = [
        f"<b>💧 Liquidity Map  •  ${price:,.2f}</b>",
        f"ATR ${atr:.1f}   TFs {' '.join(tf_parts) or '—'}",
    ]

    if primary_target:
        try:
            t = primary_target
            lines.append(
                f"\n🎯 Primary  <b>{_esc(t.direction.upper())} → ${t.pool.price:,.1f}</b>"
                f"  {t.distance_atr:.1f}ATR  sig={t.significance:.0f}"
            )
        except Exception:
            pass

    def _pool_section(label, pools):
        lines.append(f"\n<b>{label}</b>")
        if not pools:
            lines.append("  (none detected)")
            return
        for i, p in enumerate(pools[:6]):
            try:
                flags = []
                if p.pool.ob_aligned:  flags.append("OB")
                if p.pool.fvg_aligned: flags.append("FVG")
                if p.pool.htf_count >= 2: flags.append(f"HTF×{p.pool.htf_count}")
                f_str  = f" [{','.join(flags)}]" if flags else ""
                mark   = " ← TGT" if (primary_target and
                          abs(p.pool.price - primary_target.pool.price) < atr * 0.3) else ""
                lines.append(
                    f"  {i+1}. <code>${p.pool.price:,.1f}</code>"
                    f"  {p.distance_atr:.1f}ATR  sig={p.significance:.0f}"
                    f"  t={p.pool.touches}  {_esc(p.pool.timeframe)}"
                    f"{_esc(f_str)}{mark}"
                )
            except Exception:
                pass

    _pool_section("BSL ▲ (buy stops above)", bsl_pools)
    _pool_section("SSL ▼ (sell stops below)", ssl_pools)

    if recent_sweeps:
        lines.append("\n<b>🌊 Recent Sweeps</b>")
        for s in recent_sweeps[:5]:
            try:
                age = time.time() - s.detected_at
                lines.append(
                    f"  {_esc(s.pool.side.value)} ${s.pool.price:,.1f}"
                    f"  q={s.quality:.2f}  vol={s.volume_ratio:.1f}×  {age:.0f}s ago"
                )
            except Exception:
                pass

    return "\n".join(lines)


# ═════════════════════════════════════════════════════════════════════════════
# 5. TELEGRAM: /flow
# ═════════════════════════════════════════════════════════════════════════════

def format_flow_telegram(
    price: float,
    tick_flow: float,
    cvd_trend: float,
    cvd_divergence: float,
    ob_imbalance: float,
    tick_streak: int,
    streak_direction: str,
    flow_conviction: float,
    flow_direction: str,
    cvd_raw: float = 0.0,
    recent_buy_vol: float = 0.0,
    recent_sell_vol: float = 0.0,
) -> str:
    fc_bar = "█" * min(10, int(abs(flow_conviction)*10)) + "░" * max(0,10 - int(abs(flow_conviction)*10))
    dir_up = flow_conviction > 0.05
    dir_dn = flow_conviction < -0.05
    arrow  = "▲ BUY" if dir_up else ("▼ SELL" if dir_dn else "─ NEUTRAL")

    lines = [
        f"<b>⚡ Order Flow  •  ${price:,.2f}</b>",
        f"<b>{_esc((flow_direction or 'neutral').upper())}</b>  [{_esc(fc_bar)}] {arrow}  {flow_conviction:+.2f}",
        "",
        "<b>Components</b>",
    ]

    def _comp(name: str, val: float, thresh_pos: float, thresh_neg: float) -> str:
        if val > thresh_pos:   tag = "▲ bullish"
        elif val < thresh_neg: tag = "▼ bearish"
        else:                  tag = "── flat"
        return f"  <code>{name:<14}</code> {val:+.2f}  {tag}"

    lines.append(_comp("Tick flow",    tick_flow,    0.5, -0.5))
    lines.append(_comp("CVD trend",    cvd_trend,    0.2, -0.2))
    lines.append(_comp("CVD diverge",  cvd_divergence, 0.1, -0.1))
    lines.append(_comp("OB imbalance", ob_imbalance, 0.15,-0.15))
    lines.append(f"  <code>Tick streak    </code> {tick_streak}  ({_esc(streak_direction or 'none')})")

    if recent_buy_vol or recent_sell_vol:
        total   = recent_buy_vol + recent_sell_vol
        buy_pct = recent_buy_vol / total * 100 if total else 50
        bar     = "█" * int(buy_pct / 5) + "░" * (20 - int(buy_pct / 5))
        lines += [
            "",
            f"<b>Volume Split</b>",
            f"  Buy {buy_pct:.0f}%  [{_esc(bar)}]  Sell {100-buy_pct:.0f}%",
        ]

    cvd_ok  = (flow_direction == "long" and cvd_trend > 0.20) or (flow_direction == "short" and cvd_trend < -0.20)
    tick_ok = tick_streak >= 3
    conv_ok = abs(flow_conviction) >= 0.55

    lines += [
        "",
        "<b>Entry Gates</b>",
        f"  {'✅' if conv_ok else '⛔'} Conviction ≥ 0.55   ({abs(flow_conviction):.2f})",
        f"  {'✅' if cvd_ok  else '⛔'} CVD agrees",
        f"  {'✅' if tick_ok else '⛔'} Sustained ≥ 3 ticks  ({tick_streak})",
    ]
    return "\n".join(lines)


# ═════════════════════════════════════════════════════════════════════════════
# 6. TELEGRAM: Periodic report
# ═════════════════════════════════════════════════════════════════════════════

def format_periodic_report_v9(
    price: float,
    balance: float,
    atr: float,
    engine_state: str,
    tracking_info: Optional[Dict],
    flow_direction: str,
    flow_conviction: float,
    primary_target: Optional[Any],
    bsl_count: int,
    ssl_count: int,
    nearest_bsl_atr: float,
    nearest_ssl_atr: float,
    recent_sweep_count: int,
    amd_phase: str,
    amd_bias: str,
    kill_zone: str,
    total_trades: int,
    win_rate: float,
    daily_pnl: float,
    total_pnl: float,
    consecutive_losses: int,
    position: Optional[Dict] = None,
    current_sl: float = 0.0,
    current_tp: float = 0.0,
    trail_phase: str = "",
) -> str:
    now_utc = datetime.now(timezone.utc).strftime("%H:%M UTC")
    pnl_icon  = "🟢" if daily_pnl >= 0 else "🔴"
    STATE_ICONS = {
        "SCANNING":"🔍","TRACKING":"📡","READY":"🎯",
        "ENTERING":"⚡","IN_POSITION":"📊","POST_SWEEP":"🌊",
    }
    si = STATE_ICONS.get(engine_state, "⚪")
    fc_arrow = "▲" if flow_conviction > 0.05 else ("▼" if flow_conviction < -0.05 else "─")

    lines = [
        f"<b>📊 STATUS  •  {now_utc}</b>",
        f"<code>BTC ${price:,.2f}   ATR ${atr:.1f}   Bal ${balance:,.2f}</code>",
        f"{pnl_icon} Day <b>${daily_pnl:+.2f}</b>   Session <b>${total_pnl:+.2f}</b>",
        "",
        f"{si} <b>{_esc(engine_state)}</b>   Flow {_esc((flow_direction or 'neutral').upper())} {fc_arrow} {flow_conviction:+.2f}",
    ]

    if tracking_info:
        d = tracking_info.get("direction","?").upper()
        t = tracking_info.get("target","?")
        n = tracking_info.get("flow_ticks", 0)
        lines.append(f"  📡 {_esc(d)} → {_esc(t)}  ({n} ticks)")

    # Liquidity
    lines += ["", "<b>💧 Liquidity</b>"]
    if primary_target:
        try:
            t = primary_target
            lines.append(f"  🎯 <b>{_esc(t.direction.upper())} → ${t.pool.price:,.1f}</b>  {t.distance_atr:.1f}ATR  sig={t.significance:.0f}")
        except Exception:
            lines.append("  Target  —")
    else:
        lines.append("  Target  none")

    lines.append(f"  BSL {bsl_count}  ·  SSL {ssl_count}  ·  BSL {nearest_bsl_atr:.1f}ATR  SSL {nearest_ssl_atr:.1f}ATR  ·  Sweeps {recent_sweep_count}")

    if amd_phase:
        kz = f"  🔥 {_esc(kill_zone)}" if kill_zone else ""
        lines.append(f"  AMD <b>{_esc(amd_phase)}</b> ({_esc(amd_bias or 'neutral')}){kz}")

    # Position
    if position:
        side  = position.get("side","?").upper()
        entry = position.get("entry_price", 0.0)
        if entry > 0:
            pnl_pts = (price - entry) if side == "LONG" else (entry - price)
            icon = "🟢" if side == "LONG" else "🔴"
            lines += [
                "",
                f"<b>{icon} Position  {_esc(side)}</b>",
                f"  Entry ${entry:,.2f}   PnL {pnl_pts:+.1f} pts",
                f"  SL ${current_sl:,.2f}  ·  TP ${current_tp:,.2f}",
            ]
            if trail_phase:
                lines.append(f"  Trail {_esc(trail_phase)}")

    # Performance
    lines += [
        "",
        f"<b>📈 Performance</b>",
        f"  Trades {total_trades}  ·  WR {win_rate:.0f}%  ·  ConsecL {consecutive_losses}",
    ]
    return "\n".join(lines)


# ═════════════════════════════════════════════════════════════════════════════
# 7. TELEGRAM: Entry alert
# ═════════════════════════════════════════════════════════════════════════════

def format_entry_alert_v9(
    side: str,
    entry_type: str,
    entry_price: float,
    sl_price: float,
    tp_price: float,
    rr_ratio: float,
    quantity: float,
    target_pool_price: float = 0.0,
    target_pool_type: str = "",
    target_pool_sig: float = 0.0,
    target_tf_sources: str = "",
    flow_conviction: float = 0.0,
    cvd_trend: float = 0.0,
    sweep_pool_price: float = 0.0,
    sweep_quality: float = 0.0,
    ict_validation: str = "",
    amd_phase: str = "",
    kill_zone: str = "",
) -> str:
    is_long = side.upper() == "LONG"
    icon    = "🟢" if is_long else "🔴"
    risk    = abs(entry_price - sl_price)
    reward  = abs(tp_price    - entry_price)
    dollar_risk = risk * quantity

    TYPE_MAP = {
        "APPROACH":     "Flow → Pool",
        "REVERSAL":     "Sweep Reversal",
        "CONTINUATION": "Sweep Continuation",
    }
    type_label = TYPE_MAP.get(entry_type.upper(), entry_type)

    lines = [
        f"<b>{icon} NEW TRADE  •  {_esc(side.upper())}  [{_esc(type_label)}]</b>",
        "",
        f"<code>Entry  ${entry_price:,.2f}</code>",
        f"<code>SL     ${sl_price:,.2f}   risk ${risk:.1f}</code>",
        f"<code>TP     ${tp_price:,.2f}   reward ${reward:.1f}</code>",
        f"<code>R:R    1 : {rr_ratio:.1f}   Qty {quantity:.4f} BTC   ${dollar_risk:.2f} at risk</code>",
    ]

    lines.append("\n<b>Rationale</b>")
    et = entry_type.upper()
    if et == "APPROACH":
        lines.append(f"  Flow pushing {_esc(side.lower())} → unswept pool")
        if target_pool_price:
            lines.append(f"  Target  {_esc(target_pool_type)} ${target_pool_price:,.1f}  sig={target_pool_sig:.0f}")
        if target_tf_sources:
            lines.append(f"  Seen on  {_esc(target_tf_sources)}")
    elif et == "REVERSAL":
        lines.append("  Pool swept → CISD → reversing")
        if sweep_pool_price:
            lines.append(f"  Swept  ${sweep_pool_price:,.1f}  q={sweep_quality:.2f}")
        if target_pool_price:
            lines.append(f"  Delivering to  ${target_pool_price:,.1f}")
    elif et == "CONTINUATION":
        lines.append("  Pool swept — flow continues")
        if target_pool_price:
            lines.append(f"  Next target  ${target_pool_price:,.1f}")

    lines.append(f"  Flow {flow_conviction:+.2f}  ·  CVD {cvd_trend:+.2f}")
    if ict_validation:
        lines.append(f"  ICT  {_esc(ict_validation)}")
    if kill_zone:
        lines.append(f"  🔥 Session  {_esc(kill_zone.upper())}")
    return "\n".join(lines)


# ═════════════════════════════════════════════════════════════════════════════
# 8. TELEGRAM: /status
# ═════════════════════════════════════════════════════════════════════════════

def format_status_report_v9(
    price: float,
    atr: float,
    atr_pctile: float,
    balance: float,
    engine_state: str,
    tracking_info: Optional[Dict],
    flow_direction: str,
    flow_conviction: float,
    primary_target: Optional[Any],
    bsl_count: int,
    ssl_count: int,
    nearest_bsl_atr: float,
    nearest_ssl_atr: float,
    recent_sweep_count: int,
    total_trades: int,
    winning_trades: int,
    total_pnl: float,
    daily_trades: int,
    max_daily: int,
    consec_losses: int,
    max_consec: int,
    avg_win: float = 0.0,
    avg_loss: float = 0.0,
    expectancy: float = 0.0,
    position_lines: List[str] = None,
    fee_lines: List[str] = None,
) -> str:
    wr     = winning_trades / total_trades * 100 if total_trades else 0.0
    losses = total_trades - winning_trades
    STATE_ICONS = {
        "SCANNING":"🔍","TRACKING":"📡","READY":"🎯",
        "ENTERING":"⚡","IN_POSITION":"📊","POST_SWEEP":"🌊",
    }
    si = STATE_ICONS.get(engine_state, "⚪")
    fc_arrow = "▲" if flow_conviction > 0.05 else ("▼" if flow_conviction < -0.05 else "─")

    lines = [
        f"<b>📊 STATUS  v10 LIQUIDITY-FIRST</b>",
        f"<code>BTC ${price:,.2f}   ATR ${atr:.1f} ({atr_pctile:.0%})   Bal ${balance:,.2f}</code>",
        "",
        f"{si} <b>{_esc(engine_state)}</b>   Flow {_esc((flow_direction or 'neutral').upper())} {fc_arrow} {flow_conviction:+.2f}",
    ]
    if tracking_info:
        d = tracking_info.get("direction","?").upper()
        t = tracking_info.get("target","?")
        n = tracking_info.get("flow_ticks", 0)
        lines.append(f"  📡 {_esc(d)} → {_esc(t)}  ({n} ticks)")

    # Liquidity
    lines += ["", "<b>💧 Liquidity</b>"]
    if primary_target:
        try:
            t = primary_target
            lines.append(f"  🎯 <b>{_esc(t.direction.upper())} → ${t.pool.price:,.1f}</b>  {t.distance_atr:.1f}ATR  sig={t.significance:.0f}")
        except Exception:
            lines.append("  Target  —")
    else:
        lines.append("  Target  none")

    lines.append(
        f"  Pools {bsl_count} BSL / {ssl_count} SSL"
        f"   BSL {nearest_bsl_atr:.1f}ATR  SSL {nearest_ssl_atr:.1f}ATR"
        f"   Sweeps {recent_sweep_count}"
    )

    # Fee engine
    if fee_lines:
        lines.append("")
        lines.extend(fee_lines)

    # Position
    if position_lines:
        lines.append("")
        lines.extend(position_lines)

    # Performance
    lines += [
        "",
        "<b>📈 Session P&amp;L</b>",
        f"  Trades {total_trades}  W {winning_trades}  L {losses}  WR {wr:.0f}%",
        f"  PnL ${total_pnl:+.2f}   AvgW ${avg_win:+.2f}   AvgL ${avg_loss:+.2f}",
        f"  Expectancy ${expectancy:+.2f}/trade",
        f"  Daily {daily_trades}/{max_daily}   ConsecL {consec_losses}/{max_consec}",
    ]
    return "\n".join(lines)


# ═════════════════════════════════════════════════════════════════════════════
# 9. HELP TEXT
# ═════════════════════════════════════════════════════════════════════════════

HELP_TEXT = (
    "<b>Commands</b>\n"
    "/status       — Full status + liquidity overview\n"
    "/thinking     — Live decision stack + flow + pools\n"
    "/pools        — Full liquidity pool map (all TFs)\n"
    "/flow         — Detailed orderflow breakdown\n"
    "/position     — Current position details\n"
    "/trades       — Recent trade history\n"
    "/stats        — Performance analysis\n"
    "/balance      — Wallet balance\n"
    "/pause        — Pause trading (keep monitoring)\n"
    "/resume       — Resume trading\n"
    "/trail [on|off|auto] — Toggle trailing SL\n"
    "/config       — Show config values\n"
    "/set &lt;key&gt; &lt;val&gt; — Adjust config live\n"
    "/setexchange &lt;delta|coinswitch&gt; — Switch exchange\n"
    "/killswitch   — Emergency close + cancel all\n"
    "/resetrisk    — Clear consecutive-loss lockout\n"
    "/start  /stop — Start / stop bot\n"
    "/help         — This list"
)

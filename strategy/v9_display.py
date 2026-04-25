# -*- coding: utf-8 -*-
"""
strategy/v9_display.py — Display Engine v11 (industry-grade panels)
====================================================================

DESIGN PRINCIPLES
-----------------
1. Information density over decoration. Every glyph carries data.
   No nested borders, no decorative filler — the eye lands on the number.

2. Visual hierarchy by typography, not boxes. Single rule lines (─) and
   uppercase headers section the panels. Color encodes semantics:
       green  = long / profit
       red    = short / loss
       yellow = warning / kill-zone
       gray   = metadata / labels

3. Stable column geometry. All panels are 78 chars wide; numeric columns
   right-aligned at fixed offsets so the eye scans vertically.

4. One-screen heartbeat. Full state in 14 lines or fewer (16 with active
   position). More detail goes to /position or /diagnostics on demand.

PUBLIC API (compatibility-preserving)
--------------------------------------
    format_heartbeat(...)             - terminal heartbeat (every 60s)
    format_thinking_terminal(...)     - terminal thinking log (every 30s)
    format_entry_terminal(...)        - terminal entry banner (NEW)
    format_exit_terminal(...)         - terminal exit banner (NEW)
    format_post_exit_gate_block(...)  - terminal post-exit-gate veto (NEW)

Signatures of format_heartbeat / format_thinking_terminal are preserved
so main.py and quant_strategy.py do not need to change.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# ANSI palette
# ─────────────────────────────────────────────────────────────────────────────
class C:
    RST  = "\033[0m"
    BOLD = "\033[1m"
    DIM  = "\033[2m"

    RED  = "\033[31m"; GRN  = "\033[32m"; YLW  = "\033[33m"
    BLU  = "\033[34m"; MAG  = "\033[35m"; CYN  = "\033[36m"
    BRED = "\033[91m"; BGRN = "\033[92m"; BYLW = "\033[93m"
    BBLU = "\033[94m"; BMAG = "\033[95m"; BCYN = "\033[96m"

    GRAY = "\033[90m"


def _c(text: str, *codes: str) -> str:
    return "".join(codes) + str(text) + C.RST


def _ist_now() -> str:
    return datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%H:%M:%S IST")


def _price(p: Optional[float]) -> str:
    if p is None or p <= 0:
        return "—"
    return f"${p:,.2f}"


def _pnl_color(v: float) -> str:
    return C.BGRN if v > 0 else (C.BRED if v < 0 else C.GRAY)


def _r_color(r: float) -> str:
    return C.BGRN if r > 0 else (C.BRED if r < 0 else C.GRAY)


def _signed_pnl(v: float) -> str:
    sign = "+" if v >= 0 else "−"
    return f"{sign}${abs(v):,.2f}"


_RULE = "─" * 78
_DOTS = "·" * 78


def _bar(ratio: float, width: int = 20) -> str:
    ratio = max(0.0, min(1.0, ratio))
    n = int(ratio * width)
    return "█" * n + "░" * (width - n)


def _flow_glyph(conviction: float) -> str:
    if conviction > 0.05:  return _c("▲", C.BGRN, C.BOLD)
    if conviction < -0.05: return _c("▼", C.BRED, C.BOLD)
    return _c("·", C.GRAY)


def _session_label(session: str, kill_zone: str) -> str:
    s = (session or "").upper().replace(" ", "_")
    base = {
        "ASIA":     "ASIA",
        "LONDON":   "LON ",
        "NY":       "NY  ",
        "NEW_YORK": "NY  ",
        "LATE_NY":  "LNY ",
        "WEEKEND":  "WKND",
    }.get(s, (s[:4] if s else "----"))
    if kill_zone:
        return _c(base, C.BYLW, C.BOLD) + _c(" KZ", C.BYLW)
    return _c(base, C.GRAY)


def _pd_label(pd: float) -> str:
    if pd < 0.25:  return _c("DEEP-DISC", C.BGRN)
    if pd < 0.40:  return _c("DISCOUNT ", C.GRN)
    if pd < 0.60:  return _c("EQUIL    ", C.GRAY)
    if pd < 0.75:  return _c("PREMIUM  ", C.YLW)
    return            _c("DEEP-PREM", C.BRED)


def _state_label(state: str, tracking_info: Optional[Dict]) -> str:
    state = state or "SCANNING"
    if state == "TRACKING" and tracking_info:
        d = (tracking_info.get("direction") or "?").upper()
        t = tracking_info.get("target", "?")
        n = tracking_info.get("flow_ticks", 0)
        return _c(f"TRACKING  {d} → {t}   ({n} ticks)", C.BCYN, C.BOLD)
    return {
        "READY":      _c("READY      entry signal armed", C.BGRN, C.BOLD),
        "POST_SWEEP": _c("POST-SWEEP evaluating reversal/continuation", C.BMAG, C.BOLD),
        "TRACKING":   _c("TRACKING", C.BCYN),
        "SCANNING":   _c("SCANNING   awaiting sweep", C.GRAY),
        "IN_POSITION":_c("IN POSITION", C.BCYN, C.BOLD),
        "ENTERING":   _c("ENTERING   order in flight", C.BYLW, C.BOLD),
    }.get(state, _c(state, C.GRAY))


# ─────────────────────────────────────────────────────────────────────────────
# 1. HEARTBEAT
# ─────────────────────────────────────────────────────────────────────────────


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
    L: List[str] = []
    sess     = _session_label(session, kill_zone)
    feed_tag = _c(f"{feed}/{exchange.lower()}", C.GRAY)
    pd_lbl   = _pd_label(dealing_range_pd)
    atr_str  = f"{atr:5.1f}" if atr > 0 else "  ―  "

    L.append(_c(_RULE, C.BLU))
    L.append(
        f"  {_c('PRICE', C.GRAY)}  {_c(_price(price), C.BOLD, C.BCYN):<24}"
        f"  {_c('ATR', C.GRAY)} {_c(atr_str, C.YLW)}"
        f"  {_c('ZONE', C.GRAY)} {pd_lbl}"
        f"  {sess}   {_c(_ist_now(), C.GRAY)}"
    )
    L.append(
        f"  {_c('FEED', C.GRAY)} {feed_tag}"
        f"    {_c('TRADES', C.GRAY)} {total_trades}"
        f"    {_c('PNL', C.GRAY)} {_c(_signed_pnl(total_pnl), _pnl_color(total_pnl))}"
    )
    L.append(_c(_RULE, C.BLU))

    if position:
        side  = (position.get("side") or "?").upper()
        entry = float(position.get("entry_price") or 0.0)
        sl    = float(position.get("sl_price") or 0.0)
        tp    = float(position.get("tp_price") or 0.0)
        qty   = float(position.get("quantity") or 0.0)
        peak_profit = float(position.get("peak_profit") or 0.0)

        if entry <= 0 or side not in ("LONG", "SHORT"):
            L.append(_c("  POSITION  pending fill…", C.BYLW))
            L.append(_c(_RULE, C.BLU))
            return "\n".join(L)

        init_sl = float(position.get("initial_sl_dist") or abs(entry - sl) or 0.0)
        cur_pnl = (price - entry) if side == "LONG" else (entry - price)
        cur_r   = cur_pnl / init_sl if init_sl > 1e-10 else 0.0
        peak_r  = peak_profit / init_sl if init_sl > 1e-10 else 0.0
        upnl    = cur_pnl * qty

        side_col  = C.BGRN if side == "LONG" else C.BRED
        side_arr  = "▲" if side == "LONG" else "▼"
        trail_tag = _c(" TRAIL", C.BYLW) if position.get("trail_active") else ""
        sl_atr = abs(price - sl) / atr if atr > 1e-10 else 0.0
        tp_atr = abs(price - tp) / atr if atr > 1e-10 else 0.0
        prog   = (max(0.0, min(1.0, abs(price - entry) / max(abs(tp - entry), 1e-9)))
                  if cur_pnl >= 0 and tp > 0 else 0.0)

        L.append(
            f"  {_c(side_arr + ' ' + side, C.BOLD, side_col)}"
            f"   {_c('ENTRY', C.GRAY)} {_c(_price(entry), C.BCYN)}"
            f"   {_c('QTY', C.GRAY)} {qty:.4f}{trail_tag}"
        )
        L.append(
            f"  {_c('SL', C.GRAY)}   {_c(_price(sl), C.BRED):<14} "
            f"{_c(f'{sl_atr:4.1f} ATR', C.GRAY)}    "
            f"{_c('TP', C.GRAY)}   {_c(_price(tp), C.BGRN):<14} "
            f"{_c(f'{tp_atr:4.1f} ATR', C.GRAY)}"
        )
        L.append(
            f"  {_c('PNL', C.GRAY)}  "
            f"{_c(f'{cur_pnl:+7.1f} pts', C.BOLD, _pnl_color(cur_pnl))}  "
            f"{_c(f'${upnl:+7.2f}', _pnl_color(upnl))}   "
            f"{_c('R', C.GRAY)} {_c(f'{cur_r:+5.2f}', _r_color(cur_r))}   "
            f"{_c('PEAK', C.GRAY)} {_c(f'{peak_r:5.2f}R', C.GRAY)}"
        )
        bar_str = _c(_bar(prog, 30), C.BGRN if cur_pnl >= 0 else C.GRAY)
        L.append(f"  [{bar_str}] {_c(f'{prog*100:3.0f}% → TP', C.GRAY)}")
        L.append(_c(_DOTS, C.GRAY))
        L.append(_format_context_line(
            amd_phase, amd_bias, structure_15m, structure_4h,
            flow_conviction, flow_direction, cvd_trend, tick_flow,
        ))
        L.append(_c(_RULE, C.BLU))
        return "\n".join(L)

    # ── Scanning branch ───────────────────────────────────────────────────
    L.append(f"  {_state_label(engine_state, tracking_info)}")
    L.append(_c(_DOTS, C.GRAY))
    L.extend(_format_pool_block(bsl_pools or [], "BSL", "▲", C.BGRN, primary_target,
                                  price, atr))
    L.append("")
    L.extend(_format_pool_block(ssl_pools or [], "SSL", "▼", C.BRED, primary_target,
                                  price, atr))
    L.append(_c(_DOTS, C.GRAY))

    if primary_target is not None:
        try:
            d = (primary_target.direction or "?").upper()
            L.append(
                f"  {_c('TARGET', C.GRAY)} {_c(d, C.BOLD)}"
                f"  → {_c(_price(primary_target.pool.price), C.BCYN)}"
                f"  {primary_target.distance_atr:4.1f} ATR"
                f"  sig={primary_target.significance:5.1f}"
            )
        except Exception:
            L.append(f"  {_c('TARGET', C.GRAY)} —")
    else:
        L.append(f"  {_c('TARGET', C.GRAY)} —")

    L.append(_format_context_line(
        amd_phase, amd_bias, structure_15m, structure_4h,
        flow_conviction, flow_direction, cvd_trend, tick_flow,
    ))

    if engine_state == "POST_SWEEP" and sweep_analysis:
        rs = sweep_analysis.get("rev_score", 0)
        cs = sweep_analysis.get("cont_score", 0)
        sw_side = sweep_analysis.get("sweep_side", "?")
        sw_px   = sweep_analysis.get("sweep_price", 0)
        sw_q    = sweep_analysis.get("sweep_quality", 0)
        winner = ("REVERSAL"     if rs >= 45 and abs(rs - cs) >= 10 else
                  "CONTINUATION" if cs >= 40 and abs(rs - cs) >= 10 else
                  "WAIT")
        win_col = (C.BGRN if winner == "REVERSAL" else
                   C.BMAG if winner == "CONTINUATION" else C.BYLW)
        L.append(
            f"  {_c('SWEEP', C.BMAG)} {sw_side} @ {_c(_price(sw_px), C.BCYN)}"
            f"  q={sw_q:.0%}   {_c('REV', C.GRAY)} {rs:3.0f}"
            f"   {_c('CONT', C.GRAY)} {cs:3.0f}   → {_c(winner, C.BOLD, win_col)}"
        )

    L.append(_c(_RULE, C.BLU))
    return "\n".join(L)


def _format_pool_block(pools: list, label: str, arrow: str, color: str,
                         primary_target, price: float, atr: float) -> List[str]:
    rows: List[str] = []
    if not pools:
        rows.append(f"  {_c(label + ' ' + arrow, color)}    {_c('no pools in range', C.GRAY)}")
        return rows
    for i, p in enumerate(pools[:3]):
        try:
            tags: List[str] = []
            if getattr(p.pool, "ob_aligned", False):  tags.append("OB")
            if getattr(p.pool, "fvg_aligned", False): tags.append("FVG")
            htf = getattr(p.pool, "htf_count", 0)
            if htf >= 2: tags.append(f"HTF×{htf}")
            tag_str = f" [{','.join(tags)}]" if tags else ""
            tf  = getattr(p.pool, "timeframe", "")
            da  = getattr(p, "distance_atr", 0.0)
            sig = getattr(p, "significance", 0.0)
            tch = getattr(p.pool, "touches", 0)
            is_target = (primary_target is not None and
                         abs(p.pool.price - primary_target.pool.price) < max(atr * 0.3, 30))
            tgt_marker = _c(" ◄ TARGET", C.BYLW, C.BOLD) if is_target else ""
            prefix = (f"  {_c(label + ' ' + arrow, color)}"
                      if i == 0 else "         ")
            rows.append(
                f"{prefix}  {_c(_price(p.pool.price), C.BCYN):<14}"
                f"  {da:4.1f} ATR  sig={sig:5.1f}  t={tch}"
                f"  {tf or '   '}{_c(tag_str, C.GRAY)}{tgt_marker}"
            )
        except Exception:
            continue
    return rows


def _format_context_line(amd_phase: str, amd_bias: str,
                           structure_15m: str, structure_4h: str,
                           flow_conv: float, flow_dir: str,
                           cvd_trend: float, tick_flow: float) -> str:
    amd  = (amd_phase or "—")[:13]
    bias = (amd_bias or "—")[:6]
    s15  = (structure_15m or "—")[:6]
    s4h  = (structure_4h or "—")[:6]
    flow_g   = _flow_glyph(flow_conv)
    flow_col = (C.BGRN if flow_conv > 0.05 else
                C.BRED if flow_conv < -0.05 else C.GRAY)
    return (
        f"  {_c('AMD', C.GRAY)} {_c(amd, C.MAG)}/{bias}   "
        f"{_c('STRUCT', C.GRAY)} 15m={_c(s15, C.CYN)} 4h={_c(s4h, C.CYN)}   "
        f"{_c('FLOW', C.GRAY)} {flow_g} {_c(f'{flow_conv:+.2f}', flow_col)}   "
        f"{_c('CVD', C.GRAY)} {_c(f'{cvd_trend:+.2f}', _r_color(cvd_trend))}   "
        f"{_c('TICK', C.GRAY)} {_c(f'{tick_flow:+.2f}', _r_color(tick_flow))}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# 2. THINKING LOG (terminal)
# ─────────────────────────────────────────────────────────────────────────────


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
    skip_info: Optional[Dict] = None,
) -> str:
    L: List[str] = []
    state = _state_label(engine_state, tracking_info)
    L.append(f"  THINK  {state}    {_c(_ist_now(), C.GRAY)}")
    L.append(
        f"         FLOW {_flow_glyph(flow_conviction)} {flow_conviction:+.2f}/{flow_direction or '—'}"
        f"   CVD {cvd_trend:+.2f}   TICK {tick_flow:+.2f}   OB {ob_imbalance:+.2f}"
    )
    if primary_target is not None:
        try:
            L.append(
                f"         TGT {(primary_target.direction or '?').upper()}"
                f"  → {_price(primary_target.pool.price)}"
                f"   {primary_target.distance_atr:.1f} ATR  sig={primary_target.significance:.1f}"
            )
        except Exception:
            L.append("         TGT —")
    else:
        L.append(
            f"         BSL {nearest_bsl_atr:.1f} ATR    "
            f"SSL {nearest_ssl_atr:.1f} ATR    "
            f"SWEEPS {recent_sweep_count}"
        )
    L.append(
        f"         AMD {amd_phase or '—'}/{amd_bias or '—'}"
        f"   5m={structure_5m or '—'}   KZ={kill_zone or '—'}"
    )
    if skip_info:
        parts = []
        for k, v in skip_info.items():
            if isinstance(v, dict):
                inner = " ".join(f"{kk}={vv}" for kk, vv in v.items() if vv)
                if inner:
                    parts.append(f"{k}({inner})")
        if parts:
            L.append(f"  {_c('SKIP', C.YLW)}   {'  '.join(parts)}")
    return "\n".join(L)


# ─────────────────────────────────────────────────────────────────────────────
# 3. BANNERS (terminal)
# ─────────────────────────────────────────────────────────────────────────────


def format_entry_terminal(side: str, entry: float, sl: float, tp: float,
                           qty: float, mode: str, tier: str,
                           sl_atr: float, tp_atr: float, rr: float,
                           reason: str = "") -> str:
    side_u   = side.upper()
    side_col = C.BGRN if side_u == "LONG" else C.BRED
    arr      = "▲" if side_u == "LONG" else "▼"
    L = [_c(_RULE, side_col)]
    L.append(
        f"  {_c('ENTRY', C.BOLD, side_col)} {_c(arr + ' ' + side_u, C.BOLD, side_col)}"
        f"   {_c(_price(entry), C.BOLD, C.BCYN)}"
        f"   {_c(mode.upper(), C.BMAG)}"
        f"   tier={_c(tier or '?', C.BYLW)}"
        f"   {_c(_ist_now(), C.GRAY)}"
    )
    L.append(
        f"         SL {_c(_price(sl), C.BRED)} ({sl_atr:.1f} ATR)"
        f"   TP {_c(_price(tp), C.BGRN)} ({tp_atr:.1f} ATR)"
        f"   R:R 1:{rr:.2f}   qty {qty:.4f}"
    )
    if reason:
        L.append(f"         {_c(reason, C.GRAY)}")
    L.append(_c(_RULE, side_col))
    return "\n".join(L)


def format_exit_terminal(side: str, entry: float, exit_price: float,
                          pnl: float, r_realised: float, mfe_r: float,
                          reason: str, hold_min: float, fees: float = 0.0) -> str:
    side_u  = side.upper()
    pnl_col = _pnl_color(pnl)
    win     = pnl > 0
    icon    = "✓" if win else "✗"
    rule_col = C.BGRN if win else C.BRED
    reason_lbl = {
        "tp_hit":       "TP (pool sweep)",
        "sl_hit":       "SL (structural)",
        "trail_sl_hit": "TRAIL SL",
    }.get(reason, reason or "—")
    L = [_c(_RULE, rule_col)]
    L.append(
        f"  {_c('EXIT', C.BOLD, rule_col)} {_c(icon + ' ' + side_u, C.BOLD, rule_col)}"
        f"   {_c(_price(exit_price), C.BOLD, C.BCYN)}"
        f"   {_c(reason_lbl, C.BYLW)}"
        f"   {_c(_ist_now(), C.GRAY)}"
    )
    L.append(
        f"         PNL {_c(_signed_pnl(pnl), C.BOLD, pnl_col)}"
        f"   R {_c(f'{r_realised:+.2f}', _r_color(r_realised))}"
        f"   MFE {mfe_r:.2f}R"
        f"   hold {hold_min:.0f}m   fee ${fees:.4f}"
    )
    L.append(f"         entry {_price(entry)} → exit {_price(exit_price)}")
    L.append(_c(_RULE, rule_col))
    return "\n".join(L)


def format_post_exit_gate_block(side: str, lens: str, detail: str,
                                 retry_in_sec: float) -> str:
    return (
        f"  {_c('POST-EXIT GATE', C.BOLD, C.BYLW)}"
        f"   block {side.upper()}"
        f"   lens={_c(lens, C.YLW)}"
        f"   {detail}"
        f"   {_c(f'retry in {retry_in_sec:.0f}s', C.GRAY)}"
    )

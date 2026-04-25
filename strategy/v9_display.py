# -*- coding: utf-8 -*-
"""
strategy/v9_display.py — Display Engine  v10
=============================================
All functions are pure formatters — no side-effects.

TERMINAL  : ANSI-coloured box layouts.
            Disable colours with env var  QUANT_NO_COLOR=1
TELEGRAM  : HTML-formatted messages (parse_mode=HTML)

Used by: quant_strategy.py · main.py · controller.py · notifier.py
"""

from __future__ import annotations
import os
import sys
import re
import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_IST = timezone(timedelta(hours=5, minutes=30))

# ─────────────────────────────────────────────────────────────────────────────
# ANSI COLOUR PALETTE
# ─────────────────────────────────────────────────────────────────────────────

ANSI_ESC_RE = re.compile(r"\x1b\[[0-9;]*m")


def strip_ansi(text: str) -> str:
    """Strip ANSI escape codes (used by file-log formatter in main.py)."""
    return ANSI_ESC_RE.sub("", text)


def _detect_color() -> bool:
    if os.getenv("QUANT_NO_COLOR") or os.getenv("NO_COLOR"):
        return False
    if os.getenv("TERM", "") in ("dumb", ""):
        return False
    try:
        stdout = getattr(sys, "__stdout__", sys.stdout)
        return bool(hasattr(stdout, "fileno") and os.isatty(stdout.fileno()))
    except Exception:
        return False


_COLOR_ON: bool = _detect_color()


class _C:
    """ANSI colour helpers — each returns the string with codes (or plain if disabled)."""

    @staticmethod
    def _e(code: str, t: str) -> str:
        return f"\033[{code}m{t}\033[0m" if _COLOR_ON else t

    # ── structural ────────────────────────────────────────────────────────
    @staticmethod
    def bold(t: str) -> str:      return _C._e("1",    t)
    @staticmethod
    def dim(t: str) -> str:       return _C._e("2",    t)

    # ── semantic ──────────────────────────────────────────────────────────
    @staticmethod
    def header(t: str) -> str:    return _C._e("1;97", t)   # bold white
    @staticmethod
    def subhdr(t: str) -> str:    return _C._e("1;96", t)   # bold cyan
    @staticmethod
    def label(t: str) -> str:     return _C._e("90",   t)   # dark grey
    @staticmethod
    def muted(t: str) -> str:     return _C._e("2;37", t)   # dim white
    @staticmethod
    def price(t: str) -> str:     return _C._e("1;97", t)   # bold white
    @staticmethod
    def bsl(t: str) -> str:       return _C._e("96",   t)   # bright cyan
    @staticmethod
    def ssl(t: str) -> str:       return _C._e("95",   t)   # bright magenta
    @staticmethod
    def target(t: str) -> str:    return _C._e("1;93", t)   # bold yellow
    @staticmethod
    def long_(t: str) -> str:     return _C._e("92",   t)   # bright green
    @staticmethod
    def short_(t: str) -> str:    return _C._e("91",   t)   # bright red
    @staticmethod
    def pnl_pos(t: str) -> str:   return _C._e("1;92", t)   # bold bright green
    @staticmethod
    def pnl_neg(t: str) -> str:   return _C._e("1;91", t)   # bold bright red
    @staticmethod
    def warn(t: str) -> str:      return _C._e("33",   t)   # yellow
    @staticmethod
    def sep(t: str) -> str:       return _C._e("2;37", t)   # dim (box chrome)
    # state-specific colours
    @staticmethod
    def c_scan(t: str) -> str:    return _C._e("36",   t)   # cyan
    @staticmethod
    def c_track(t: str) -> str:   return _C._e("33",   t)   # yellow
    @staticmethod
    def c_ready(t: str) -> str:   return _C._e("1;93", t)   # bold yellow
    @staticmethod
    def c_sweep(t: str) -> str:   return _C._e("35",   t)   # magenta
    @staticmethod
    def c_enter(t: str) -> str:   return _C._e("1;93", t)   # bold yellow


# ─────────────────────────────────────────────────────────────────────────────
# BOX-DRAWING HELPERS
# ─────────────────────────────────────────────────────────────────────────────

_W = 74  # box width (characters, excluding ANSI)


def _top(w: int = _W) -> str:
    return _C.sep("╔" + "═" * (w - 2) + "╗")

def _bot(w: int = _W) -> str:
    return _C.sep("╚" + "═" * (w - 2) + "╝")

def _thick(w: int = _W) -> str:
    return _C.sep("╠" + "═" * (w - 2) + "╣")

def _thin(w: int = _W) -> str:
    return _C.sep("╟" + "─" * (w - 2) + "╢")

def _rule(w: int = _W) -> str:
    return _C.sep("─" * w)

def _row(content: str) -> str:
    """Left-border ║ + one space + content (no right padding needed)."""
    return _C.sep("║") + "  " + content


# ─────────────────────────────────────────────────────────────────────────────
# MICRO-FORMATTERS
# ─────────────────────────────────────────────────────────────────────────────

def _fp(p: float) -> str:
    return f"${p:,.2f}" if p >= 1000 else f"${p:.4f}"


def _esc(s: Any) -> str:
    """HTML-escape for Telegram."""
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _flow_bar(conviction: float, width: int = 6) -> str:
    n = min(width, max(0, int(abs(conviction) * width + 0.5)))
    empty = width - n
    if conviction > 0.05:
        return _C.long_("▓" * n + "░" * empty + " ▲")
    if conviction < -0.05:
        return _C.short_("▓" * n + "░" * empty + " ▼")
    return _C.muted("░" * width + " ─")


def _pbar(pct: float, width: int = 20, clr=None) -> str:
    filled = max(0, min(width, int(pct * width)))
    bar = "█" * filled + "░" * (width - filled)
    return clr(bar) if clr else bar


def _pnl_c(v: float) -> str:
    s = f"${v:+,.2f}"
    return _C.pnl_pos(s) if v >= 0 else _C.pnl_neg(s)


def _r_c(v: float) -> str:
    s = f"{v:+.2f}R"
    return _C.pnl_pos(s) if v >= 0 else _C.pnl_neg(s)


def _pd_label(pd: float) -> str:
    if pd < 0.25: return "DEEP-DISC"
    if pd < 0.40: return "DISCOUNT"
    if pd < 0.60: return "EQUILIB"
    if pd < 0.75: return "PREMIUM"
    return "DEEP-PREM"


_SESS_ICONS = {
    "asia": "🌙", "london": "🌅", "ny": "🏛️",
    "new_york": "🏛️", "late_ny": "🌇", "london_ny": "🌅",
}
_STATE_ICON = {
    "SCANNING": "🔍", "TRACKING": "📡", "READY": "🎯",
    "POST_SWEEP": "🌊", "ENTERING": "⚡", "IN_POSITION": "📊",
}
_STATE_CLR = {
    "SCANNING":   _C.c_scan,
    "TRACKING":   _C.c_track,
    "READY":      _C.c_ready,
    "POST_SWEEP": _C.c_sweep,
    "ENTERING":   _C.c_enter,
}


# ─────────────────────────────────────────────────────────────────────────────
# 1.  TERMINAL HEARTBEAT  (main.py — every 60 s)
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
    """Rich ANSI-coloured box heartbeat. Called every 60 s from main.py."""

    ist_now = datetime.now(_IST)
    utc_now = datetime.now(timezone.utc)
    ts       = f"{ist_now.strftime('%H:%M:%S')} IST  /  {utc_now.strftime('%H:%M')} UTC"

    sess_key  = (session or "").lower().replace(" ", "_")
    sess_icon = _SESS_ICONS.get(sess_key, "⚪")
    kz_str    = "  🔥 KZ" if kill_zone else ""
    atr_str   = f"${atr:.1f}" if atr > 0 else "—"
    pd_lbl    = _pd_label(dealing_range_pd)

    lines = ["\n" + _top()]

    # ── IN-POSITION block ──────────────────────────────────────────────────
    if position:
        side  = (position.get("side") or "?").upper()
        entry = position.get("entry_price", 0.0)
        sl    = position.get("sl_price",    0.0)
        tp    = position.get("tp_price",    0.0)

        # Title row
        pos_icon = "🟢" if side == "LONG" else "🔴"
        clr_side = _C.long_ if side == "LONG" else _C.short_
        lines.append(_row(
            f"{pos_icon}  {_C.header('IN ' + side)}"
            f"    {_C.price(f'BTC ${price:,.2f}')}"
            f"    {sess_icon} {_C.dim((session or '').upper())}{kz_str}"
            f"    {_C.label(ts)}"
        ))
        lines.append(_thick())

        if entry <= 0 or side not in ("LONG", "SHORT"):
            lines.append(_row(_C.warn("  Awaiting fill confirmation…")))
            lines.append(_bot())
            return "\n".join(lines)

        pnl      = (price - entry) if side == "LONG" else (entry - price)
        init_sl  = position.get("initial_sl_dist", abs(entry - sl)) or abs(entry - sl)
        curr_r   = pnl / init_sl if init_sl > 1e-10 else 0.0
        peak     = position.get("peak_profit", pnl)
        peak_r   = peak / init_sl if init_sl > 1e-10 else 0.0
        sl_dist  = abs(price - sl)
        tp_dist  = abs(price - tp)
        sl_atr   = sl_dist / atr if atr > 1e-10 else 0.0
        tp_atr   = tp_dist / atr if atr > 1e-10 else 0.0
        trail    = position.get("trail_active", False)

        total_mv = abs(tp - entry)
        prog     = (min(1.0, abs(price - entry) / total_mv) if total_mv > 0 else 0)
        if pnl < 0:
            prog = 0
        bar_clr  = _C.pnl_pos if pnl >= 0 else _C.pnl_neg
        bar_str  = "[" + _pbar(prog, 22, bar_clr) + f"]  {prog*100:.0f}% → TP"

        lines += [
            _row(
                f"  {_C.label('Entry')}  {_C.price(f'${entry:,.2f}')}"
                f"    {_C.label('ATR')} {atr_str}"
                f"    {_C.muted(pd_lbl)}  {_C.dim(f'({dealing_range_pd:.0%})')}"
            ),
            _row(
                f"  {_C.label('SL')}     {clr_side(f'${sl:,.2f}')}"
                f"    {_C.dim(f'({sl_atr:.1f} ATR from price)')}"
                + (_C.warn("    🔒 TRAIL") if trail else "")
            ),
            _row(
                f"  {_C.label('TP')}     {clr_side(f'${tp:,.2f}')}"
                f"    {_C.dim(f'({tp_atr:.1f} ATR from price)')}"
            ),
            _thin(),
            _row(
                f"  PnL  {_pnl_c(pnl)}"
                f"    {_r_c(curr_r)}"
                f"    {_C.label('Peak')} {_C.dim(f'{peak_r:.2f}R')}"
            ),
            _row(f"  {bar_str}"),
            _thin(),
            _row(
                f"  {_C.label('AMD')}   {amd_phase or '—'}"
                + (f"  {_C.dim(f'({amd_bias})')}" if amd_bias else "")
                + f"    {_C.label('15m')} {structure_15m or '—'}"
                f"    {_C.label('4H')} {structure_4h or '—'}"
            ),
            _row(
                f"  {_C.label('Flow')}  {_flow_bar(flow_conviction)}"
                f"  {_C.dim(flow_direction or 'neutral')} ({flow_conviction:+.2f})"
                f"    CVD {cvd_trend:+.2f}    Tick {tick_flow:+.2f}"
            ),
        ]

    # ── FLAT / SCANNING block ──────────────────────────────────────────────
    else:
        st_key  = (engine_state or "SCANNING").upper()
        st_icon = _STATE_ICON.get(st_key, "⚪")
        st_clr  = _STATE_CLR.get(st_key, _C.muted)

        # Title
        lines.append(_row(
            f"⚡  {_C.header('QUANT v10')}"
            f"    {_C.price(f'BTC ${price:,.2f}')}"
            f"    {sess_icon} {_C.dim((session or '').upper())}{kz_str}"
            f"    {_C.label(ts)}"
        ))
        lines.append(_row(
            f"  {st_icon}  {st_clr(st_key)}"
            f"    {_C.label('ATR')} {atr_str}"
            f"    {_C.muted(pd_lbl)}  {_C.dim(f'({dealing_range_pd:.0%})')}"
        ))
        lines.append(_thick())

        # Pool rows helper
        def _pool_rows(pools, side_lbl, near_atr, clr_fn) -> List[str]:
            rows: List[str] = []
            if pools:
                for i, p in enumerate(pools[:4]):
                    try:
                        flags: List[str] = []
                        if getattr(p.pool, "ob_aligned",  False): flags.append("OB")
                        if getattr(p.pool, "fvg_aligned", False): flags.append("FVG")
                        htf = getattr(p.pool, "htf_count", 0)
                        if htf >= 2: flags.append(f"HTFx{htf}")
                        flag_s = f" [{','.join(flags)}]" if flags else ""
                        is_tgt = (
                            primary_target is not None and
                            abs(p.pool.price - primary_target.pool.price)
                            < max(atr * 0.3, 30)
                        )
                        tgt_m = _C.target("  ◀ TARGET") if is_tgt else ""
                        tf_s  = getattr(p.pool, "timeframe", "")
                        dist  = getattr(p, "distance_atr", 0.0)
                        sig   = getattr(p, "significance", 0.0)
                        t_ch  = getattr(p.pool, "touches", 0)
                        pfx   = f"  {side_lbl}" if i == 0 else "         "
                        rows.append(_row(
                            clr_fn(f"{pfx}  ${p.pool.price:,.1f}")
                            + f"  {_C.dim(f'{dist:.1f} ATR')}"
                            + f"  sig={sig:.0f}"
                            + f"  t={t_ch}"
                            + (f"  {_C.dim(tf_s)}" if tf_s else "")
                            + (_C.muted(flag_s) if flags else "")
                            + tgt_m
                        ))
                    except Exception:
                        pass
            if not rows:
                rows.append(_row(f"  {side_lbl}  {_C.muted('no pools in range')}"))
            return rows

        lines += _pool_rows(bsl_pools or [], "▲ BSL", nearest_bsl_atr, _C.bsl)
        lines.append(_row(""))
        lines += _pool_rows(ssl_pools or [], "▼ SSL", nearest_ssl_atr, _C.ssl)
        lines.append(_thin())

        # Target
        if primary_target:
            try:
                t = primary_target
                lines.append(_row(
                    f"  🎯  {_C.target('Target')}"
                    f"    {t.direction.upper()} → {_C.price(f'${t.pool.price:,.1f}')}"
                    f"    {_C.dim(f'({t.distance_atr:.1f} ATR  sig={t.significance:.0f})')}"
                ))
            except Exception:
                lines.append(_row(f"  🎯  {_C.muted('Target: data pending')}"))
        else:
            lines.append(_row(f"  🎯  {_C.muted('Target: none detected')}"))

        # Flow + structure
        fl_dir = flow_direction or "neutral"
        fl_clr = _C.long_ if flow_conviction > 0.05 else (
                 _C.short_ if flow_conviction < -0.05 else _C.muted)
        lines.append(_row(
            f"  {_C.label('Flow')}  {_flow_bar(flow_conviction)}"
            f"  {fl_clr(fl_dir)} ({flow_conviction:+.2f})"
            f"    CVD {cvd_trend:+.2f}    Tick {tick_flow:+.2f}"
        ))
        lines.append(_row(
            f"  {_C.label('AMD')}   {amd_phase or '—'}"
            + (f"  {_C.dim(f'({amd_bias})')}" if amd_bias else "")
            + f"    {_C.label('15m')} {structure_15m or '—'}"
            + f"    {_C.label('4H')} {structure_4h or '—'}"
        ))

        # Post-sweep analysis
        if engine_state == "POST_SWEEP" and sweep_analysis:
            rs  = sweep_analysis.get("rev_score",  0)
            cs  = sweep_analysis.get("cont_score", 0)
            rr  = sweep_analysis.get("rev_reasons",  [])
            cr  = sweep_analysis.get("cont_reasons", [])
            sw_side  = sweep_analysis.get("sweep_side",    "?")
            sw_price = sweep_analysis.get("sweep_price",   0)
            sw_qual  = sweep_analysis.get("sweep_quality", 0)
            winner   = (
                "REVERSAL"     if rs >= 45 and abs(rs - cs) >= 10 else
                "CONTINUATION" if cs >= 40 and abs(rs - cs) >= 10 else "WAIT"
            )
            tot = max(rs + cs, 1)
            rv  = int(rs / tot * 18)
            ct  = 18 - rv
            sbar = "◀" + _C.short_("█" * rv) + _C.long_("█" * ct) + "▶"
            lines.append(_thin())
            lines.append(_row(
                f"  🌊  {_C.c_sweep('SWEEP')}    "
                f"{sw_side} @ ${sw_price:,.0f}    q={sw_qual:.0%}"
            ))
            lines.append(_row(f"  {sbar}"))
            lines.append(_row(
                f"  REV {rs:.0f}  {' · '.join(rr[:2]) if rr else '—'}"
                f"    CONT {cs:.0f}  {' · '.join(cr[:2]) if cr else '—'}"
            ))
            lines.append(_row(
                f"  → {_C.target(winner)}    gap={abs(rs-cs):.0f}"
            ))

        # Tracking detail
        if engine_state == "TRACKING" and tracking_info:
            d     = tracking_info.get("direction", "?").upper()
            tgt   = tracking_info.get("target",     "?")
            ticks = tracking_info.get("flow_ticks",  0)
            start = tracking_info.get("started",     "")
            lines.append(_thin())
            lines.append(_row(
                f"  📡  {_C.c_track('TRACKING')}"
                f"    {d} → {tgt}"
                f"    {_C.dim(f'{ticks} ticks  ·  {start}')}"
            ))

    # ── Footer ─────────────────────────────────────────────────────────────
    lines.append(_thick())
    parts = []
    if recent_sweep_count > 0:
        parts.append(f"Sweeps {recent_sweep_count}")
    parts.append(f"T={total_trades}")
    parts.append(f"Session PnL  {_pnl_c(total_pnl)}")
    lines.append(_row("  " + _C.dim("    ·    ".join(parts))))
    lines.append(_bot())
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# 2.  TERMINAL THINKING LOG  (quant_strategy.py — every 30 s)
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
) -> str:
    """Compact thinking snapshot — every 30 s."""
    ts      = datetime.now(_IST).strftime("%H:%M:%S")
    atr_str = f"${atr:.1f}" if atr > 0 else "—"
    st_clr  = _STATE_CLR.get((engine_state or "").upper(), _C.muted)

    lines = [
        _rule(),
        _row(
            f"{_C.label(ts)}"
            f"    {_C.price(f'BTC ${price:,.2f}')}"
            f"    {_C.label('ATR')} {atr_str}"
            f"    {_C.dim('State:')} {st_clr(engine_state or '—')}"
        ),
    ]

    # Flow
    fl_dir = flow_direction or "neutral"
    fl_clr = _C.long_ if flow_conviction > 0.05 else (
             _C.short_ if flow_conviction < -0.05 else _C.muted)
    lines.append(_row(
        f"  {_C.label('Flow')}   {_flow_bar(flow_conviction)}"
        f"  {fl_clr(fl_dir)} ({flow_conviction:+.2f})"
        f"    Tick {tick_flow:+.2f}  CVD {cvd_trend:+.2f}  OB {ob_imbalance:+.2f}"
    ))

    # Target / liquidity
    tgt_str = "none"
    if primary_target:
        try:
            t = primary_target
            tgt_str = (
                f"{t.direction} → ${t.pool.price:,.0f}"
                f"  ({t.pool.side.value}  {t.distance_atr:.1f}ATR"
                f"  sig={t.significance:.0f}"
                f"  TFs={','.join(t.tf_sources)})"
            )
        except Exception:
            tgt_str = "data pending"
    lines.append(_row(
        f"  {_C.label('Target')} {_C.target(tgt_str)}"
    ))
    lines.append(_row(
        f"  {_C.label('BSL')} {nearest_bsl_atr:.1f}A"
        f"    {_C.label('SSL')} {nearest_ssl_atr:.1f}A"
        f"    {_C.label('Sweeps(5m)')} {recent_sweep_count}"
    ))

    # ICT context
    ict: List[str] = []
    if amd_phase:   ict.append(f"AMD={amd_phase[:4]}")
    if amd_bias:    ict.append(f"Bias={amd_bias}")
    if structure_5m: ict.append(f"5m={structure_5m}")
    if kill_zone:   ict.append(f"KZ={kill_zone}")
    if ict:
        lines.append(_row(
            f"  {_C.label('ICT')}    {_C.dim('  ·  '.join(ict))}"
        ))

    # Tracking
    if tracking_info:
        d     = tracking_info["direction"].upper()
        tgt   = tracking_info["target"]
        ticks = tracking_info["flow_ticks"]
        start = tracking_info["started"]
        lines.append(_row(
            f"  📡 {_C.c_track('Tracking')}"
            f"    {d} → {tgt}"
            f"    {_C.dim(f'{ticks} ticks  ·  {start}')}"
        ))

    lines.append(_rule())
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# 3.  TELEGRAM: /thinking   (HTML)
# ─────────────────────────────────────────────────────────────────────────────

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

    si = _STATE_ICON.get((engine_state or "").upper(), "⚪")
    threshold = abs(flow_conviction) - 0.55
    flow_icon = "🟢" if flow_conviction > 0.3 else ("🔴" if flow_conviction < -0.3 else "⚪")

    lines = [
        f"🧠 <b>THINKING  ·  BTC ${price:,.2f}</b>",
        f"<code>ATR ${atr:.1f} ({atr_pctile:.0%})  ·  {_esc(engine_state)}</code>",
        "",
        f"{si} <b>{_esc(engine_state)}</b>",
    ]
    if tracking_info:
        lines.append(
            f"  📡 Tracking <b>{_esc(tracking_info['direction']).upper()}</b>"
            f" → {_esc(tracking_info['target'])}"
            f"    {tracking_info['flow_ticks']}t  ·  {_esc(tracking_info['started'])}"
        )

    # Flow
    lines += [
        "",
        "⚡ <b>ORDER FLOW</b>",
        f"  {flow_icon} <b>{_esc((flow_direction or 'neutral').upper())}</b>"
        f"  conv={flow_conviction:+.2f}",
        f"  Tick {tick_flow:+.2f}  ·  Streak {tick_streak}"
        f"  ·  CVD {cvd_trend:+.2f}  ·  OB {ob_imbalance:+.2f}",
        ("  ✅ Flow gate PASS  (+" + f"{threshold:.2f})"
         if threshold >= 0 else
         f"  ❌ Flow gate  {threshold:.2f} below threshold"),
    ]

    # Liquidity
    lines += ["", "🎯 <b>LIQUIDITY MAP</b>",
              f"  BSL {nearest_bsl_atr:.1f} ATR  ·  SSL {nearest_ssl_atr:.1f} ATR"]
    if primary_target:
        try:
            t = primary_target
            lines.append(
                f"  <b>Target: {_esc(t.direction).upper()} → ${t.pool.price:,.1f}</b>"
                f"  ({t.distance_atr:.1f}A  sig={t.significance:.0f}  t={t.pool.touches})"
            )
            if t.tf_sources:
                lines.append(f"  TFs: {_esc(', '.join(t.tf_sources))}")
        except Exception:
            pass

    for lbl, pools in [("▲ BSL", bsl_pools[:3]), ("▼ SSL", ssl_pools[:3])]:
        if pools:
            lines.append(f"  {lbl}:")
            for p in pools:
                try:
                    flags = []
                    if p.pool.ob_aligned:    flags.append("OB")
                    if p.pool.fvg_aligned:   flags.append("FVG")
                    if p.pool.htf_count >= 2: flags.append(f"HTFx{p.pool.htf_count}")
                    fs = f" [{','.join(flags)}]" if flags else ""
                    lines.append(
                        f"    ${p.pool.price:,.1f}"
                        f"  {p.distance_atr:.1f}A  sig={p.significance:.0f}"
                        f"  t={p.pool.touches}{_esc(fs)}"
                    )
                except Exception:
                    pass

    if recent_sweeps:
        lines.append(f"  Recent sweeps ({len(recent_sweeps)}):")
        for s in recent_sweeps[:3]:
            try:
                age = time.time() - s.detected_at
                lines.append(
                    f"    {_esc(s.pool.side.value)} ${s.pool.price:,.1f}"
                    f"  q={s.quality:.2f}  dir={_esc(s.direction)}  {age:.0f}s ago"
                )
            except Exception:
                pass

    # ICT
    zone = "DISCOUNT" if in_discount else ("PREMIUM" if in_premium else "EQUILIBRIUM")
    amd_bias_icon = "🟢" if amd_bias == "bullish" else ("🔴" if amd_bias == "bearish" else "⚪")
    lines += [
        "",
        "🏛️ <b>ICT CONTEXT</b>",
        f"  AMD  <b>{_esc(amd_phase or '—')}</b>"
        f"  {amd_bias_icon} {_esc(amd_bias or '—')}  conf={amd_confidence:.2f}",
        f"  Zone {zone}  ·  5m {_esc(structure_5m or '?')}  ·  15m {_esc(structure_15m or '?')}",
    ]
    if kill_zone:
        lines.append(f"  KZ {_esc(kill_zone.upper())}")

    # Position
    if position:
        side  = (position.get("side") or "?").upper()
        entry = position.get("entry_price", 0.0)
        sl    = position.get("sl_price", 0.0)
        tp    = position.get("tp_price", 0.0)
        if entry > 0:
            pnl_pts = (price - entry) if side == "LONG" else (entry - price)
            init_sl = position.get("initial_sl_dist", abs(entry - sl))
            curr_r  = pnl_pts / init_sl if init_sl > 1e-10 else 0.0
            pos_icon = "🟢" if pnl_pts >= 0 else "🔴"
            lines += [
                "",
                f"{pos_icon} <b>POSITION  {side}</b>",
                f"  ${entry:,.2f}  {pnl_pts:+.1f}pts ({curr_r:+.1f}R)",
                f"  SL ${sl:,.2f}  ·  TP ${tp:,.2f}",
            ]
            if trail_phase:
                lines.append(f"  Trail {_esc(trail_phase)}")

    # Verdict
    verdicts: Dict[str, str] = {
        "TRACKING":    "Building conviction — flow sustained toward pool",
        "READY":       "⚡ Entry imminent — conviction met, computing SL/TP",
        "POST_SWEEP":  "Evaluating: reverse, continue, or wait?",
        "IN_POSITION": "Managing active position",
    }
    verdict = verdicts.get(engine_state)
    if not verdict:
        if abs(flow_conviction) < 0.3:
            verdict = "Waiting for directional flow"
        elif not primary_target:
            verdict = "Flow present — no significant pool in range"
        else:
            verdict = "Monitoring flow alignment with target pool"
    lines += ["", f"💭 <b>VERDICT</b>", f"  {_esc(verdict)}"]

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# 4.  TELEGRAM: /pools
# ─────────────────────────────────────────────────────────────────────────────

def format_pools_telegram(
    price: float,
    atr: float,
    bsl_pools: list,
    ssl_pools: list,
    primary_target: Optional[Any],
    recent_sweeps: list,
    tf_coverage: Dict[str, int],
) -> str:

    tf_parts = [f"{tf}:{c}"
                for tf in ["1m","5m","15m","1h","4h","1d"]
                if (c := tf_coverage.get(tf, 0)) > 0]

    lines = [
        f"🗺️ <b>LIQUIDITY MAP  ·  BTC ${price:,.2f}</b>",
        f"<code>ATR ${atr:.1f}"
        + (f"  ·  TFs: {' '.join(tf_parts)}" if tf_parts else "")
        + "</code>",
    ]

    if primary_target:
        try:
            t = primary_target
            lines.append(
                f"\n🎯 <b>Target: {_esc(t.direction).upper()}"
                f" → ${t.pool.price:,.1f}</b>"
                f"  ({t.distance_atr:.1f}ATR  sig={t.significance:.0f})"
            )
        except Exception:
            pass

    for lbl, pools in [
        ("▲ BSL  —  buy stops above", bsl_pools),
        ("▼ SSL  —  sell stops below", ssl_pools),
    ]:
        lines.append(f"\n<b>{lbl}</b>")
        if pools:
            for i, p in enumerate(pools[:6]):
                try:
                    flags = []
                    if p.pool.ob_aligned:    flags.append("OB")
                    if p.pool.fvg_aligned:   flags.append("FVG")
                    if p.pool.htf_count >= 2: flags.append(f"HTFx{p.pool.htf_count}")
                    fs = f" [{','.join(flags)}]" if flags else ""
                    is_tgt = (primary_target and
                              abs(p.pool.price - primary_target.pool.price) < atr * 0.3)
                    lines.append(
                        f"  {i+1}. <b>${p.pool.price:,.1f}</b>"
                        f"  {p.distance_atr:.1f}A  sig={p.significance:.0f}"
                        f"  t={p.pool.touches}  {_esc(p.pool.timeframe)}"
                        + _esc(fs)
                        + ("  ←" if is_tgt else "")
                    )
                except Exception:
                    pass
        else:
            lines.append("  (none detected)")

    if recent_sweeps:
        lines.append("\n<b>Recent sweeps</b>")
        for s in recent_sweeps[:5]:
            try:
                age = time.time() - s.detected_at
                lines.append(
                    f"  {_esc(s.pool.side.value)} ${s.pool.price:,.1f}"
                    f"  q={s.quality:.2f}  vol={s.volume_ratio:.1f}×"
                    f"  dir={_esc(s.direction)}  {age:.0f}s ago"
                )
            except Exception:
                pass

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# 5.  TELEGRAM: /flow
# ─────────────────────────────────────────────────────────────────────────────

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
    dir_icon = "🟢" if flow_conviction > 0.1 else ("🔴" if flow_conviction < -0.1 else "⚪")
    threshold = abs(flow_conviction) - 0.55
    cvd_agrees = (
        (flow_direction == "long"  and cvd_trend >  0.20) or
        (flow_direction == "short" and cvd_trend < -0.20)
    )

    def _neutral(v, buy_t=0.5, sell_t=-0.5):
        if v > buy_t:  return "↑↑ strong buy"
        if v < sell_t: return "↓↓ strong sell"
        return "── neutral"

    lines = [
        f"⚡ <b>ORDER FLOW  ·  BTC ${price:,.2f}</b>",
        "",
        f"{dir_icon} <b>{_esc((flow_direction or 'neutral').upper())}</b>"
        f"    conviction {flow_conviction:+.2f}",
        "",
        "<b>Components</b>",
        f"<code>"
        f"Tick flow     {tick_flow:+.2f}   {_neutral(tick_flow)}\n"
        f"CVD trend     {cvd_trend:+.2f}   {'bearish' if cvd_trend < -0.2 else ('bullish' if cvd_trend > 0.2 else 'flat')}\n"
        f"CVD diverge   {cvd_divergence:+.2f}\n"
        f"OB imbalance  {ob_imbalance:+.2f}   {'bids heavy' if ob_imbalance > 0.15 else ('asks heavy' if ob_imbalance < -0.15 else 'balanced')}\n"
        f"Tick streak   {tick_streak}"
        + (f"  ({_esc(streak_direction)})" if streak_direction else "")
        + "</code>",
    ]

    if recent_buy_vol > 0 or recent_sell_vol > 0:
        total   = recent_buy_vol + recent_sell_vol
        buy_pct = recent_buy_vol / total * 100 if total > 0 else 50
        lines += [
            "",
            "<b>Volume split</b>",
            f"  Buy {buy_pct:.0f}%  ·  Sell {100-buy_pct:.0f}%",
        ]

    lines += [
        "",
        "<b>Entry gates</b>",
        f"  {'✅' if threshold >= 0 else '❌'}  Conviction ≥ 0.55    ({abs(flow_conviction):.2f})",
        f"  {'✅' if cvd_agrees else '❌'}  CVD agrees with flow",
        f"  {'✅' if tick_streak >= 3 else '❌'}  Sustained ≥ 3 ticks  ({tick_streak})",
    ]

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# 6.  TELEGRAM: 15-min periodic report  (v9 version)
# ─────────────────────────────────────────────────────────────────────────────

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
    pnl_icon = "🟢" if daily_pnl >= 0 else "🔴"
    si = _STATE_ICON.get((engine_state or "").upper(), "⚪")
    fl_icon = "🟢" if flow_conviction > 0.05 else ("🔴" if flow_conviction < -0.05 else "⚪")

    lines = [
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"📊  <b>STATUS</b>  ·  {si} {_esc(engine_state)}",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
        f"💎  BTC  <b>${price:,.2f}</b>    ATR ${atr:.1f}",
        f"💼  Balance  <b>${balance:,.2f}</b>",
        f"{pnl_icon}  Day  <b>${daily_pnl:+,.2f}</b>    Total  ${total_pnl:+,.2f}",
    ]

    if tracking_info:
        lines.append(
            f"\n📡  Tracking <b>{_esc(tracking_info['direction']).upper()}</b>"
            f" → {_esc(tracking_info['target'])}"
            f"  ({tracking_info['flow_ticks']}t)"
        )

    lines.append(
        f"{fl_icon}  Flow  {_esc(flow_direction or 'neutral')} ({flow_conviction:+.2f})"
    )

    lines += [
        "",
        "─────────────────────────────────",
        "🎯  <b>LIQUIDITY</b>",
    ]
    if primary_target:
        try:
            t = primary_target
            lines.append(
                f"  Target  <b>{_esc(t.direction).upper()} → ${t.pool.price:,.1f}</b>"
                f"  ({t.distance_atr:.1f}A  sig={t.significance:.0f})"
            )
        except Exception:
            pass
    lines.append(
        f"  Pools  ▲{bsl_count} BSL  ·  ▼{ssl_count} SSL"
        f"  ·  Sweeps {recent_sweep_count}"
    )
    lines.append(f"  Nearest  BSL {nearest_bsl_atr:.1f}A  ·  SSL {nearest_ssl_atr:.1f}A")

    if amd_phase:
        kz = f"  ·  KZ {_esc(kill_zone)}" if kill_zone else ""
        lines.append(
            f"\n🏛️  AMD  <b>{_esc(amd_phase)}</b>"
            f"  ({_esc(amd_bias or 'neutral')}){kz}"
        )

    if position:
        side  = (position.get("side") or "?").upper()
        entry = position.get("entry_price", 0.0)
        if entry > 0:
            pnl_pts  = (price - entry) if side == "LONG" else (entry - price)
            pos_icon = "🟢" if pnl_pts >= 0 else "🔴"
            lines += [
                "",
                f"{pos_icon}  <b>POSITION  {side}</b>",
                f"  Entry ${entry:,.2f}  ·  {pnl_pts:+.1f}pts",
                f"  SL ${current_sl:,.2f}  ·  TP ${current_tp:,.2f}",
            ]
            if trail_phase:
                lines.append(f"  Trail {_esc(trail_phase)}")

    lines += [
        "",
        "─────────────────────────────────",
        "📈  <b>PERFORMANCE</b>",
        f"  Trades {total_trades}  ·  WR {win_rate:.0f}%"
        f"  ·  CL {consecutive_losses}",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
    ]
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# 7.  TELEGRAM: Entry alert  (v9)
# ─────────────────────────────────────────────────────────────────────────────

def format_entry_alert_v9(
    side: str,
    entry_type: str,
    entry_price: float,
    sl_price: float,
    tp_price: float,
    rr_ratio: float,
    quantity: float,
    target_pool_price: float = 0.0,
    target_pool_type:  str   = "",
    target_pool_sig:   float = 0.0,
    target_tf_sources: str   = "",
    flow_conviction:   float = 0.0,
    cvd_trend:         float = 0.0,
    sweep_pool_price:  float = 0.0,
    sweep_quality:     float = 0.0,
    ict_validation:    str   = "",
    amd_phase:         str   = "",
    kill_zone:         str   = "",
) -> str:
    side_upper  = side.upper()
    side_icon   = "🟢" if side_upper == "LONG" else "🔴"
    risk        = abs(entry_price - sl_price)
    reward      = abs(tp_price   - entry_price)
    dollar_risk = risk * quantity

    type_labels = {
        "APPROACH":     "APPROACH  ·  flow → pool",
        "REVERSAL":     "SWEEP REVERSAL",
        "CONTINUATION": "SWEEP CONTINUATION",
    }
    type_label = type_labels.get(entry_type.upper(), entry_type)

    lines = [
        f"{side_icon} <b>NEW TRADE  ·  {side_upper}</b>",
        f"<code>{_esc(type_label)}</code>",
        "",
        "<b>Levels</b>",
        f"<code>"
        f"Entry   ${entry_price:,.2f}\n"
        f"SL      ${sl_price:,.2f}   risk ${risk:.1f}\n"
        f"TP      ${tp_price:,.2f}   reward ${reward:.1f}\n"
        f"R:R     1:{rr_ratio:.1f}   qty {quantity:.4f} BTC   ${dollar_risk:.2f} at risk"
        "</code>",
        "",
        "<b>Why this trade</b>",
    ]

    if entry_type.upper() == "APPROACH":
        lines.append(f"  Flow pushing {side.lower()} toward unswept pool")
        if target_pool_price > 0:
            lines.append(
                f"  Target  {_esc(target_pool_type)} ${target_pool_price:,.1f}"
                f"  (sig={target_pool_sig:.0f})"
            )
            if target_tf_sources:
                lines.append(f"  TFs  {_esc(target_tf_sources)}")
    elif entry_type.upper() == "REVERSAL":
        lines.append("  Pool swept → CISD confirmed → reversing")
        if sweep_pool_price > 0:
            lines.append(f"  Swept   ${sweep_pool_price:,.1f}  (q={sweep_quality:.2f})")
        if target_pool_price > 0:
            lines.append(f"  Deliver to  ${target_pool_price:,.1f}")
    elif entry_type.upper() == "CONTINUATION":
        lines.append("  Pool swept — flow continues")
        if target_pool_price > 0:
            lines.append(f"  Next target  ${target_pool_price:,.1f}")

    lines.append(f"  Flow {flow_conviction:+.2f}  ·  CVD {cvd_trend:+.2f}")
    if ict_validation:
        lines.append(f"  ICT  {_esc(ict_validation)}")
    if kill_zone:
        lines.append(f"  Session  {_esc(kill_zone.upper())}")

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# 8.  TELEGRAM: /status  (v9 full report)
# ─────────────────────────────────────────────────────────────────────────────

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
    position_lines: Optional[List[str]] = None,
    fee_lines: Optional[List[str]] = None,
) -> str:
    wr       = winning_trades / total_trades * 100 if total_trades > 0 else 0.0
    losses   = total_trades - winning_trades
    si       = _STATE_ICON.get((engine_state or "").upper(), "⚪")
    fl_icon  = "🟢" if flow_conviction > 0.05 else ("🔴" if flow_conviction < -0.05 else "⚪")
    pnl_icon = "🟢" if total_pnl >= 0 else "🔴"

    lines = [
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "📊  <b>QUANT v10  —  FULL STATUS</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
        f"💎  BTC  <b>${price:,.2f}</b>    ATR ${atr:.1f} ({atr_pctile:.0%})",
        f"💼  Balance  <b>${balance:,.2f}</b>",
        "",
        f"{si}  <b>{_esc(engine_state)}</b>",
        f"{fl_icon}  Flow  {_esc(flow_direction or 'neutral')} ({flow_conviction:+.2f})",
    ]

    if tracking_info:
        lines.append(
            f"  📡 <b>{_esc(tracking_info['direction']).upper()}</b>"
            f" → {_esc(tracking_info['target'])}"
            f"  ({tracking_info['flow_ticks']}t)"
        )

    lines += ["", "─────────────────────────────────", "🎯  <b>LIQUIDITY</b>"]
    if primary_target:
        try:
            t = primary_target
            lines.append(
                f"  <b>{_esc(t.direction).upper()} → ${t.pool.price:,.1f}</b>"
                f"  ({t.distance_atr:.1f}ATR  sig={t.significance:.0f})"
            )
        except Exception:
            pass
    lines.append(
        f"  ▲{bsl_count} BSL  ▼{ssl_count} SSL"
        f"  ·  BSL {nearest_bsl_atr:.1f}A  SSL {nearest_ssl_atr:.1f}A"
        f"  ·  Sweeps {recent_sweep_count}"
    )

    lines += ["", "─────────────────────────────────", "📈  <b>PERFORMANCE</b>"]
    lines += [
        f"  Trades {total_trades}    W {winning_trades}  L {losses}    WR {wr:.0f}%",
        f"  {pnl_icon}  Total PnL  <b>${total_pnl:+.2f}</b>",
        f"<code>"
        f"Avg W  ${avg_win:+.2f}  ·  Avg L ${avg_loss:+.2f}  ·  E ${expectancy:+.2f}\n"
        f"Daily {daily_trades}/{max_daily}  ·  CL {consec_losses}/{max_consec}"
        "</code>",
    ]

    if fee_lines:
        lines += [""] + fee_lines

    if position_lines:
        lines += [""] + position_lines

    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# 9.  HELP TEXT
# ─────────────────────────────────────────────────────────────────────────────

HELP_TEXT = (
    "<b>Commands</b>\n"
    "\n"
    "/status      Full status + liquidity overview\n"
    "/thinking    Live decision stack · flow · pools\n"
    "/pools       Full liquidity pool map (all TFs)\n"
    "/flow        Detailed orderflow breakdown\n"
    "/position    Current position details\n"
    "/trades      Recent trade history\n"
    "/stats       Performance analysis\n"
    "/balance     Wallet balance\n"
    "\n"
    "/pause       Pause trading (keep monitoring)\n"
    "/resume      Resume trading\n"
    "/trail [on|off|auto]   Toggle trailing SL\n"
    "/config      Show config values\n"
    "/set &lt;key&gt; &lt;value&gt;   Adjust config live\n"
    "/setexchange &lt;delta|coinswitch&gt;   Switch execution\n"
    "\n"
    "/killswitch  Emergency close + cancel all\n"
    "/resetrisk   Clear consecutive-loss lockout\n"
    "/start       Start bot\n"
    "/stop        Stop bot\n"
    "/help        This list"
)

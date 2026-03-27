# -*- coding: utf-8 -*-
"""
strategy/v9_display.py -- Display Engine for Liquidity-First Strategy v9
==========================================================================
Centralized display/formatting for terminal logging, Telegram notifications,
and controller commands. All functions are pure formatters -- no side effects.

USED BY:
  - quant_strategy.py  (terminal thinking log, status report)
  - main.py            (heartbeat)
  - controller.py      (telegram commands: /thinking, /status, /pools, /flow)
  - notifier.py        (periodic report, entry alert, trail update, close alert)
"""

from __future__ import annotations
import time
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def _fp(p: float) -> str:
    """Format price with commas."""
    if p >= 1000:
        return f"${p:,.2f}"
    return f"${p:.4f}"


def _esc(s: str) -> str:
    """Escape HTML for Telegram."""
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# =====================================================================
# 1. TERMINAL HEARTBEAT (main.py)
# =====================================================================

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
    # v9 rich display
    bsl_pools: Optional[list] = None,
    ssl_pools: Optional[list] = None,
    atr: float = 0.0,
    cvd_trend: float = 0.0,
    tick_flow: float = 0.0,
    # v10 institutional context
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
    """Rich multi-line heartbeat snapshot for terminal. Called every 60s from main.py."""

    W = 72  # display width
    SEP  = "─" * W
    DSEP = "═" * W
    from datetime import datetime, timezone, timedelta
    # IST time
    ist_now = datetime.now(timezone(timedelta(hours=5, minutes=30)))
    now_str = ist_now.strftime("%H:%M:%S IST")

    atr_str = f"${atr:.1f}" if atr > 0 else "—"

    # Session determination
    sess_icon = {"asia": "🌙", "london": "🌅", "ny": "🏛️", "late_ny": "🌇"}.get(
        (session or "").lower().replace(" ", "_"), "⚪")
    kz_str = " 🔥KZ" if kill_zone else ""

    # Dealing range label
    pd_label = ("DEEP-DISC" if dealing_range_pd < 0.25 else
                "DISCOUNT" if dealing_range_pd < 0.40 else
                "EQUILIBRIUM" if dealing_range_pd < 0.60 else
                "PREMIUM" if dealing_range_pd < 0.75 else "DEEP-PREM")

    # ── ACTIVE POSITION ──────────────────────────────────────────────────────
    if position:
        side  = position.get("side", "?").upper()
        entry = position.get("entry_price", 0.0)
        sl    = position.get("sl_price", 0.0)
        tp    = position.get("tp_price", 0.0)
        if entry <= 0 or side not in ("LONG", "SHORT"):
            return f"${price:,.2f} [{feed}] | PENDING FILL"
        pnl      = (price - entry) if side == "LONG" else (entry - price)
        init_sl  = position.get("initial_sl_dist", abs(entry - sl)) or abs(entry - sl)
        curr_r   = pnl / init_sl if init_sl > 1e-10 else 0.0
        sl_dist  = abs(price - sl)
        tp_dist  = abs(price - tp)
        sl_atr   = sl_dist / atr if atr > 1e-10 else 0.0
        tp_atr   = tp_dist / atr if atr > 1e-10 else 0.0
        pnl_icon = "🟢" if pnl >= 0 else "🔴"

        # Progress bar
        total_move = abs(tp - entry)
        progress = min(1.0, max(0, abs(price - entry) / total_move)) if total_move > 0 else 0
        if pnl < 0: progress = 0
        bar_len = 20
        filled = int(progress * bar_len)
        bar = "█" * filled + "░" * (bar_len - filled)

        # MFE tracking
        peak_profit = position.get("peak_profit", pnl)
        peak_r = peak_profit / init_sl if init_sl > 1e-10 else 0.0
        trail_active = position.get("trail_active", False)

        lines = [
            DSEP,
            f" {pnl_icon} IN {side}   ${price:,.2f}   {sess_icon}{(session or '').upper()}{kz_str}   {now_str}",
            SEP,
            f"   Entry:  ${entry:,.2f}   |   ATR: {atr_str}   |   {pd_label}",
            f"   SL:     ${sl:,.2f}   ({sl_atr:.1f} ATR from price){'  🔒TRAIL' if trail_active else ''}",
            f"   TP:     ${tp:,.2f}   ({tp_atr:.1f} ATR from price)",
            f"   PnL:    {pnl:+.1f}pts   R: {curr_r:+.2f}R   Peak: {peak_r:.2f}R",
            f"   [{bar}] {progress*100:.0f}% → TP",
            SEP,
            f"   AMD: {amd_phase or '—'}({amd_bias or '—'})  |  15m: {structure_15m or '—'}  |  4H: {structure_4h or '—'}",
            f"   Flow: {flow_direction or 'neutral'}({flow_conviction:+.2f})  CVD: {cvd_trend:+.2f}  Tick: {tick_flow:+.2f}",
            SEP,
            f"   T={total_trades}  |  Session PnL=${total_pnl:+.2f}",
            DSEP,
        ]
        return "\n".join(lines)

    # ── FLAT — rich scanning display ──────────────────────────────────────────

    # Engine / state line
    if engine_state == "TRACKING" and tracking_info:
        d     = tracking_info.get("direction", "?")
        tgt   = tracking_info.get("target", "?")
        ticks = tracking_info.get("flow_ticks", 0)
        state_str = f"📡 TRACKING {d.upper()}→{tgt} ({ticks} ticks)"
    elif engine_state == "READY":
        state_str = "🎯 READY — awaiting fill"
    elif engine_state == "POST_SWEEP":
        state_str = "🌊 POST_SWEEP — scoring rev vs cont"
    else:
        state_str = "🔍 SCANNING"

    # Flow visual bar
    flow_n = max(0, min(5, int(abs(flow_conviction) * 5)))
    if flow_conviction > 0.05:
        flow_bar = "▁" * (5 - flow_n) + "▓" * flow_n + " ▲"
        flow_label = "LONG"
    elif flow_conviction < -0.05:
        flow_bar = "▓" * flow_n + "▁" * (5 - flow_n) + " ▼"
        flow_label = "SHORT"
    else:
        flow_bar = "▁▁▁▁▁ ─"
        flow_label = "NEUTRAL"

    # ── Pool rows ──────────────────────────────────────────────────────────────
    def _pool_rows(pools, side_label, nearest_atr):
        rows = []
        if pools:
            for i, p in enumerate(pools[:4]):
                try:
                    flags = []
                    if getattr(p.pool, 'ob_aligned', False):  flags.append("OB")
                    if getattr(p.pool, 'fvg_aligned', False): flags.append("FVG")
                    htf = getattr(p.pool, 'htf_count', 0)
                    if htf >= 2: flags.append(f"HTFx{htf}")
                    flag_str = f"[{','.join(flags)}]" if flags else ""
                    is_tgt = (
                        primary_target is not None and
                        abs(p.pool.price - primary_target.pool.price) < max(atr * 0.3, 30)
                    )
                    tgt_mark = " ◀ TARGET" if is_tgt else ""
                    tf_str   = getattr(p.pool, 'timeframe', '')
                    tf_part  = f" {tf_str}" if tf_str else ""
                    dist_atr = getattr(p, 'distance_atr', 0.0)
                    sig      = getattr(p, 'significance', 0.0)
                    touches  = getattr(p.pool, 'touches', 0)
                    prefix   = f"  {side_label}" if i == 0 else "       "
                    rows.append(
                        f"{prefix}  ${p.pool.price:,.1f}"
                        f"  {dist_atr:.1f}ATR"
                        f"  sig={sig:.0f}"
                        f"  t={touches}"
                        f"{tf_part}"
                        f"  {flag_str}"
                        f"{tgt_mark}"
                    )
                except Exception:
                    pass
        elif nearest_atr < 50:
            rows.append(f"  {side_label}  {nearest_atr:.1f}ATR away")
        else:
            rows.append(f"  {side_label}  no pools in range")
        return rows

    bsl_rows = _pool_rows(bsl_pools or [], "BSL↑", nearest_bsl_atr)
    ssl_rows = _pool_rows(ssl_pools or [], "SSL↓", nearest_ssl_atr)

    # Primary target summary
    if primary_target:
        try:
            t = primary_target
            tgt_line = (
                f"  🎯 Target: {t.direction.upper()} → ${t.pool.price:,.1f}"
                f"  ({t.distance_atr:.1f}ATR  sig={t.significance:.0f})"
            )
        except Exception:
            tgt_line = "  🎯 Target: data pending"
    else:
        tgt_line = "  🎯 Target: none detected"

    # Post-sweep analysis display
    sweep_lines = []
    if engine_state == "POST_SWEEP" and sweep_analysis:
        rs = sweep_analysis.get("rev_score", 0)
        cs = sweep_analysis.get("cont_score", 0)
        rr = sweep_analysis.get("rev_reasons", [])
        cr = sweep_analysis.get("cont_reasons", [])
        sw_side = sweep_analysis.get("sweep_side", "?")
        sw_price = sweep_analysis.get("sweep_price", 0)
        sw_qual = sweep_analysis.get("sweep_quality", 0)

        winner = "REVERSAL" if rs > cs else ("CONTINUATION" if cs > rs else "UNDECIDED")
        gap = abs(rs - cs)
        bar_total = max(rs + cs, 1)
        rev_pct = int(rs / bar_total * 20)
        cont_pct = 20 - rev_pct
        score_bar = "◀" + "█" * rev_pct + "░" * cont_pct + "▶"

        sweep_lines.extend([
            "",
            f"  🌊 SWEEP: {sw_side} @ ${sw_price:,.0f} (quality={sw_qual:.0%})",
            f"  {score_bar}",
            f"  REV={rs:.0f}  {' + '.join(rr[:3]) if rr else '—'}",
            f"  CONT={cs:.0f} {' + '.join(cr[:3]) if cr else '—'}",
            f"  → {winner} (gap={gap:.0f}, need≥15)",
        ])

    # Footer stats
    sweep_part = f"Sweeps={recent_sweep_count}" if recent_sweep_count > 0 else ""
    stats_parts = [p for p in [sweep_part, f"T={total_trades}", f"PnL=${total_pnl:+.2f}"] if p]

    lines = [
        DSEP,
        f" ⚡ v10 LIQUIDITY-FIRST   ${price:,.2f}   {sess_icon}{(session or '').upper()}{kz_str}   {now_str}",
        f" {state_str}   ATR: {atr_str}   {pd_label}",
        SEP,
    ]
    lines.extend(bsl_rows)
    lines.append("")
    lines.extend(ssl_rows)
    lines.append(SEP)
    lines.append(tgt_line)
    lines.append(f"  Flow: {flow_bar}  {flow_label}({flow_conviction:+.2f})  CVD: {cvd_trend:+.2f}  Tick: {tick_flow:+.2f}")
    lines.append(f"  AMD: {amd_phase or '—'}({amd_bias or '—'})  |  15m: {structure_15m or '—'}  |  4H: {structure_4h or '—'}")
    if sweep_lines:
        lines.extend(sweep_lines)
    lines.append(SEP)
    lines.append(f"  {' | '.join(stats_parts)}")
    lines.append(DSEP)
    return "\n".join(lines)


# =====================================================================
# 2. TERMINAL THINKING LOG (quant_strategy.py -- every 30s)
# =====================================================================

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
    """Multi-line thinking log for terminal. Every 30s."""

    lines = []
    lines.append(f"--- THINK @ ${price:,.2f} | ATR=${atr:.1f} | State={engine_state} ---")

    # Flow
    flow_str = flow_direction or "neutral"
    lines.append(f"  Flow: {flow_str}({flow_conviction:+.2f}) "
                 f"| Tick={tick_flow:+.2f} CVD={cvd_trend:+.2f} "
                 f"OB_imb={ob_imbalance:+.2f}")

    # Liquidity map
    tgt_str = "none"
    if primary_target:
        try:
            t = primary_target
            tgt_str = (f"{t.direction}->${t.pool.price:,.0f} "
                       f"({t.pool.side.value} {t.distance_atr:.1f}ATR "
                       f"sig={t.significance:.0f} "
                       f"TFs={','.join(t.tf_sources)})")
        except Exception:
            pass
    lines.append(f"  Target: {tgt_str}")
    lines.append(f"  BSL={nearest_bsl_atr:.1f}ATR | SSL={nearest_ssl_atr:.1f}ATR"
                 f" | Sweeps(5m)={recent_sweep_count}")

    # ICT context
    ict_parts = []
    if amd_phase:
        ict_parts.append(f"AMD={amd_phase[:4]}")
    if amd_bias:
        ict_parts.append(f"Bias={amd_bias}")
    if structure_5m:
        ict_parts.append(f"5m={structure_5m}")
    if kill_zone:
        ict_parts.append(f"KZ={kill_zone}")
    if ict_parts:
        lines.append(f"  ICT: {' | '.join(ict_parts)}")

    # Tracking
    if tracking_info:
        lines.append(f"  >> Tracking: {tracking_info['direction'].upper()}"
                     f" -> {tracking_info['target']}"
                     f" | {tracking_info['flow_ticks']} ticks"
                     f" | {tracking_info['started']}")

    return "\n".join(lines)


# =====================================================================
# 3. TELEGRAM: /thinking COMMAND
# =====================================================================

def format_thinking_telegram(
    price: float,
    atr: float,
    atr_pctile: float,
    engine_state: str,
    # Flow
    flow_direction: str,
    flow_conviction: float,
    tick_flow: float,
    cvd_trend: float,
    ob_imbalance: float,
    tick_streak: int,
    # Liquidity
    bsl_pools: list,
    ssl_pools: list,
    primary_target: Optional[Any],
    nearest_bsl_atr: float,
    nearest_ssl_atr: float,
    recent_sweeps: list,
    # ICT
    amd_phase: str = "",
    amd_bias: str = "",
    amd_confidence: float = 0.0,
    in_premium: bool = False,
    in_discount: bool = False,
    structure_5m: str = "",
    structure_15m: str = "",
    kill_zone: str = "",
    # Tracking
    tracking_info: Optional[Dict] = None,
    # Position (if active)
    position: Optional[Dict] = None,
    trail_phase: str = "",
) -> str:
    """Full /thinking display for Telegram."""

    lines = [f"<b>THINKING @ ${price:,.2f}</b>"]

    # ---- Engine State ----
    state_icons = {
        "SCANNING": "SCANNING",
        "TRACKING": ">> TRACKING",
        "READY": "** READY **",
        "ENTERING": "ENTERING...",
        "IN_POSITION": "IN POSITION",
        "POST_SWEEP": "!! POST SWEEP !!",
    }
    lines.append(f"\n<b>-- Engine</b>")
    lines.append(f"  State: <b>{_esc(state_icons.get(engine_state, engine_state))}</b>")
    lines.append(f"  ATR(5m): ${atr:.1f} ({atr_pctile:.0%} pctile)")

    if tracking_info:
        lines.append(f"  Tracking: {_esc(tracking_info['direction']).upper()}"
                     f" -> {_esc(tracking_info['target'])}"
                     f" | {tracking_info['flow_ticks']} ticks"
                     f" | {_esc(tracking_info['started'])}")

    # ---- Order Flow (PRIMARY DRIVER) ----
    lines.append(f"\n<b>-- Order Flow (primary)</b>")
    flow_icon = ""
    if flow_conviction > 0.3:
        flow_icon = " BUY"
    elif flow_conviction < -0.3:
        flow_icon = " SELL"
    lines.append(f"  Direction: <b>{_esc(flow_direction or 'neutral')}{flow_icon}</b>")
    lines.append(f"  Conviction: {flow_conviction:+.2f}")
    lines.append(f"  Tick flow:  {tick_flow:+.2f} | Streak: {tick_streak}")
    lines.append(f"  CVD trend:  {cvd_trend:+.2f}")
    lines.append(f"  OB imbal:   {ob_imbalance:+.2f}")

    # Gate check
    from_threshold = abs(flow_conviction) - 0.55
    if from_threshold >= 0:
        lines.append(f"  Flow gate: PASSED (+{from_threshold:.2f} above threshold)")
    else:
        lines.append(f"  Flow gate: {from_threshold:.2f} below threshold")

    # ---- Liquidity Map (PRIMARY DRIVER) ----
    lines.append(f"\n<b>-- Liquidity Map</b>")
    lines.append(f"  BSL nearest: {nearest_bsl_atr:.1f} ATR | SSL nearest: {nearest_ssl_atr:.1f} ATR")

    if primary_target:
        try:
            t = primary_target
            lines.append(f"  Primary target: <b>{_esc(t.direction).upper()}"
                         f" -> ${t.pool.price:,.1f}</b> ({_esc(t.pool.side.value)})")
            lines.append(f"    Distance: {t.distance_atr:.1f} ATR"
                         f" | Significance: {t.significance:.0f}"
                         f" | Touches: {t.pool.touches}")
            if t.tf_sources:
                lines.append(f"    Seen on: {_esc(', '.join(t.tf_sources))}")
        except Exception:
            lines.append("  Primary target: none")
    else:
        lines.append("  Primary target: none")

    # Top pools
    for label, pools in [("BSL (above)", bsl_pools[:3]), ("SSL (below)", ssl_pools[:3])]:
        if pools:
            lines.append(f"  {label}:")
            for p in pools:
                try:
                    flags = []
                    if p.pool.ob_aligned:
                        flags.append("OB")
                    if p.pool.fvg_aligned:
                        flags.append("FVG")
                    if p.pool.htf_count >= 2:
                        flags.append(f"HTF x{p.pool.htf_count}")
                    flag_str = f" [{','.join(flags)}]" if flags else ""
                    lines.append(f"    ${p.pool.price:,.1f}"
                                 f" ({p.distance_atr:.1f}A sig={p.significance:.0f}"
                                 f" t={p.pool.touches}{_esc(flag_str)})")
                except Exception:
                    pass

    # Recent sweeps
    if recent_sweeps:
        lines.append(f"\n  Recent sweeps ({len(recent_sweeps)}):")
        for s in recent_sweeps[:3]:
            try:
                age = time.time() - s.detected_at
                lines.append(f"    {_esc(s.pool.side.value)} ${s.pool.price:,.1f}"
                             f" q={s.quality:.2f} dir={_esc(s.direction)}"
                             f" {age:.0f}s ago")
            except Exception:
                pass

    # ---- ICT Context (SECONDARY / VALIDATOR) ----
    lines.append(f"\n<b>-- ICT Context (validator)</b>")
    if amd_phase:
        bias_tag = ""
        if amd_bias == "bullish":
            bias_tag = " bullish"
        elif amd_bias == "bearish":
            bias_tag = " bearish"
        lines.append(f"  AMD: <b>{_esc(amd_phase)}</b>{bias_tag}"
                     f"  conf={amd_confidence:.2f}")
    else:
        lines.append("  AMD: not available")

    zone = "DISCOUNT" if in_discount else ("PREMIUM" if in_premium else "EQUILIBRIUM")
    lines.append(f"  Zone: {zone} | 5m: {_esc(structure_5m or '?')}"
                 f" | 15m: {_esc(structure_15m or '?')}")
    if kill_zone:
        lines.append(f"  Kill zone: {_esc(kill_zone.upper())}")

    # ---- Active Position ----
    if position:
        side = position.get("side", "?").upper()
        entry = position.get("entry_price", 0.0)
        sl = position.get("sl_price", 0.0)
        tp = position.get("tp_price", 0.0)
        if entry > 0:
            pnl_pts = (price - entry) if side == "LONG" else (entry - price)
            init_sl = position.get("initial_sl_dist", abs(entry - sl))
            curr_r = pnl_pts / init_sl if init_sl > 1e-10 else 0.0
            lines.append(f"\n<b>-- Position</b>")
            lines.append(f"  {side} @ ${entry:,.2f} | {pnl_pts:+.1f}pts ({curr_r:+.1f}R)")
            lines.append(f"  SL: ${sl:,.2f} | TP: ${tp:,.2f}")
            if trail_phase:
                lines.append(f"  Trail: {_esc(trail_phase)}")

    # ---- Verdict ----
    lines.append(f"\n<b>-- Verdict</b>")
    if engine_state == "TRACKING":
        lines.append("  Building conviction... flow sustained toward pool")
    elif engine_state == "READY":
        lines.append("  ENTRY IMMINENT -- conviction met, computing SL/TP")
    elif engine_state == "POST_SWEEP":
        lines.append("  Evaluating: reverse, continue, or wait?")
    elif engine_state == "IN_POSITION":
        lines.append("  Managing active position")
    else:
        if abs(flow_conviction) < 0.3:
            lines.append("  Waiting for directional flow")
        elif not primary_target:
            lines.append("  Flow present but no significant pool in range")
        else:
            lines.append("  Monitoring flow alignment with target pool")

    return "\n".join(lines)


# =====================================================================
# 4. TELEGRAM: /pools COMMAND (replaces /huntstatus)
# =====================================================================

def format_pools_telegram(
    price: float,
    atr: float,
    bsl_pools: list,
    ssl_pools: list,
    primary_target: Optional[Any],
    recent_sweeps: list,
    tf_coverage: Dict[str, int],
) -> str:
    """Full liquidity pool map for /pools command."""

    lines = [f"<b>LIQUIDITY MAP @ ${price:,.2f}</b>"]
    lines.append(f"ATR: ${atr:.1f}")

    # TF coverage
    tf_parts = []
    for tf in ["1m", "5m", "15m", "1h", "4h", "1d"]:
        count = tf_coverage.get(tf, 0)
        if count > 0:
            tf_parts.append(f"{tf}:{count}")
    if tf_parts:
        lines.append(f"Active TFs: {' '.join(tf_parts)}")

    # Primary target
    if primary_target:
        try:
            t = primary_target
            lines.append(f"\nPrimary: <b>{_esc(t.direction).upper()}"
                         f" -> ${t.pool.price:,.1f}</b>"
                         f" ({t.distance_atr:.1f}ATR sig={t.significance:.0f})")
        except Exception:
            pass

    # BSL pools
    lines.append(f"\n<b>BSL (buy stops above)</b>")
    if bsl_pools:
        for i, p in enumerate(bsl_pools[:6]):
            try:
                flags = []
                if p.pool.ob_aligned:
                    flags.append("OB")
                if p.pool.fvg_aligned:
                    flags.append("FVG")
                if p.pool.htf_count >= 2:
                    flags.append(f"HTFx{p.pool.htf_count}")
                f_str = f" [{','.join(flags)}]" if flags else ""
                marker = " ←" if (primary_target and
                    abs(p.pool.price - primary_target.pool.price) < atr * 0.3) else ""
                lines.append(f"  {i+1}. ${p.pool.price:,.1f}"
                             f" | {p.distance_atr:.1f}ATR"
                             f" | sig={p.significance:.0f}"
                             f" | t={p.pool.touches}"
                             f" | {_esc(p.pool.timeframe)}"
                             f"{_esc(f_str)}{marker}")
            except Exception:
                pass
    else:
        lines.append("  (none detected)")

    # SSL pools
    lines.append(f"\n<b>SSL (sell stops below)</b>")
    if ssl_pools:
        for i, p in enumerate(ssl_pools[:6]):
            try:
                flags = []
                if p.pool.ob_aligned:
                    flags.append("OB")
                if p.pool.fvg_aligned:
                    flags.append("FVG")
                if p.pool.htf_count >= 2:
                    flags.append(f"HTFx{p.pool.htf_count}")
                f_str = f" [{','.join(flags)}]" if flags else ""
                marker = " ←" if (primary_target and
                    abs(p.pool.price - primary_target.pool.price) < atr * 0.3) else ""
                lines.append(f"  {i+1}. ${p.pool.price:,.1f}"
                             f" | {p.distance_atr:.1f}ATR"
                             f" | sig={p.significance:.0f}"
                             f" | t={p.pool.touches}"
                             f" | {_esc(p.pool.timeframe)}"
                             f"{_esc(f_str)}{marker}")
            except Exception:
                pass
    else:
        lines.append("  (none detected)")

    # Recent sweeps
    if recent_sweeps:
        lines.append(f"\n<b>Recent sweeps</b>")
        for s in recent_sweeps[:5]:
            try:
                age = time.time() - s.detected_at
                lines.append(f"  {_esc(s.pool.side.value)} ${s.pool.price:,.1f}"
                             f" | q={s.quality:.2f}"
                             f" | vol={s.volume_ratio:.1f}x"
                             f" | dir={_esc(s.direction)}"
                             f" | {age:.0f}s ago")
            except Exception:
                pass

    return "\n".join(lines)


# =====================================================================
# 5. TELEGRAM: /flow COMMAND (new)
# =====================================================================

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
    # CVD engine details
    cvd_raw: float = 0.0,
    # Optional recent trade data
    recent_buy_vol: float = 0.0,
    recent_sell_vol: float = 0.0,
) -> str:
    """Detailed orderflow breakdown for /flow command."""

    lines = [f"<b>ORDER FLOW @ ${price:,.2f}</b>"]

    # Direction summary
    dir_str = flow_direction or "neutral"
    lines.append(f"\nDirection: <b>{_esc(dir_str.upper())}</b>"
                 f" | Conviction: {flow_conviction:+.2f}")

    # Component breakdown
    lines.append(f"\n<b>Components</b>")
    lines.append(f"  Tick flow:    {tick_flow:+.2f}"
                 f"  {'↓↓ strong sell' if tick_flow < -0.5 else ('↑↑ strong buy' if tick_flow > 0.5 else '── neutral')}")
    lines.append(f"  CVD trend:    {cvd_trend:+.2f}"
                 f"  {'bearish' if cvd_trend < -0.2 else ('bullish' if cvd_trend > 0.2 else 'flat')}")
    lines.append(f"  CVD diverg:   {cvd_divergence:+.2f}")
    lines.append(f"  OB imbalance: {ob_imbalance:+.2f}"
                 f"  {'bids heavy' if ob_imbalance > 0.15 else ('asks heavy' if ob_imbalance < -0.15 else 'balanced')}")
    lines.append(f"  Tick streak:  {tick_streak}"
                 f" ({_esc(streak_direction) if streak_direction else 'none'})")

    # Volume
    if recent_buy_vol > 0 or recent_sell_vol > 0:
        total = recent_buy_vol + recent_sell_vol
        buy_pct = recent_buy_vol / total * 100 if total > 0 else 50
        lines.append(f"\n<b>Volume split</b>")
        lines.append(f"  Buy:  {buy_pct:.0f}%  |  Sell: {100-buy_pct:.0f}%")

    # Gates
    lines.append(f"\n<b>Entry gates</b>")
    gates = []
    if abs(flow_conviction) >= 0.55:
        gates.append("Conviction >= 0.55: PASS")
    else:
        gates.append(f"Conviction >= 0.55: FAIL ({abs(flow_conviction):.2f})")

    cvd_agrees = (
        (flow_direction == "long" and cvd_trend > 0.20) or
        (flow_direction == "short" and cvd_trend < -0.20)
    )
    gates.append(f"CVD agrees: {'PASS' if cvd_agrees else 'FAIL'}")
    gates.append(f"Sustained (3+ ticks): {'PASS' if tick_streak >= 3 else f'FAIL ({tick_streak})'}")

    for g in gates:
        lines.append(f"  {g}")

    return "\n".join(lines)


# =====================================================================
# 6. TELEGRAM: PERIODIC REPORT (replaces format_periodic_report)
# =====================================================================

def format_periodic_report_v9(
    price: float,
    balance: float,
    atr: float,
    # Engine
    engine_state: str,
    tracking_info: Optional[Dict],
    # Flow
    flow_direction: str,
    flow_conviction: float,
    # Liquidity
    primary_target: Optional[Any],
    bsl_count: int,
    ssl_count: int,
    nearest_bsl_atr: float,
    nearest_ssl_atr: float,
    recent_sweep_count: int,
    # ICT
    amd_phase: str,
    amd_bias: str,
    kill_zone: str,
    # Performance
    total_trades: int,
    win_rate: float,
    daily_pnl: float,
    total_pnl: float,
    consecutive_losses: int,
    # Position
    position: Optional[Dict] = None,
    current_sl: float = 0.0,
    current_tp: float = 0.0,
    trail_phase: str = "",
) -> str:
    """15-minute periodic report -- Telegram."""

    pnl_icon = "+" if daily_pnl >= 0 else "-"

    lines = [
        "<b>STATUS v9 -- LIQUIDITY-FIRST</b>",
        f"{datetime.now(timezone.utc).strftime('%H:%M UTC')}",
        "",
        f"BTC: <b>${price:,.2f}</b> | ATR: ${atr:.1f}",
        f"Balance: ${balance:,.2f} | Daily: ${daily_pnl:+.2f}",
        "",
        f"<b>Engine: {_esc(engine_state)}</b>",
    ]

    # Tracking
    if tracking_info:
        lines.append(f"  Tracking: {_esc(tracking_info['direction']).upper()}"
                     f" -> {_esc(tracking_info['target'])}"
                     f" ({tracking_info['flow_ticks']}t)")

    # Flow
    lines.append(f"Flow: {_esc(flow_direction or 'neutral')}"
                 f" ({flow_conviction:+.2f})")

    # Liquidity
    lines.append("")
    lines.append(f"<b>Liquidity</b>")
    if primary_target:
        try:
            t = primary_target
            lines.append(f"  Target: {_esc(t.direction).upper()}"
                         f" ${t.pool.price:,.1f}"
                         f" ({t.distance_atr:.1f}A sig={t.significance:.0f})")
        except Exception:
            lines.append("  Target: none")
    else:
        lines.append("  Target: none")
    lines.append(f"  Pools: {bsl_count} BSL / {ssl_count} SSL"
                 f" | Sweeps: {recent_sweep_count}")
    lines.append(f"  Nearest: BSL={nearest_bsl_atr:.1f}A SSL={nearest_ssl_atr:.1f}A")

    # ICT
    if amd_phase:
        kz = f" | KZ={_esc(kill_zone)}" if kill_zone else ""
        lines.append(f"  AMD: {_esc(amd_phase)} ({_esc(amd_bias or 'neutral')}){kz}")

    # Position
    if position:
        side = position.get("side", "?").upper()
        entry = position.get("entry_price", 0.0)
        if entry > 0:
            pnl_pts = (price - entry) if side == "LONG" else (entry - price)
            lines.append("")
            lines.append(f"<b>Position: {side}</b>")
            lines.append(f"  Entry: ${entry:,.2f} | {pnl_pts:+.1f}pts")
            lines.append(f"  SL: ${current_sl:,.2f} | TP: ${current_tp:,.2f}")
            if trail_phase:
                lines.append(f"  Trail: {_esc(trail_phase)}")

    # Performance
    lines.append("")
    lines.append(f"Trades: {total_trades} | WR: {win_rate:.0f}%"
                 f" | PnL: ${total_pnl:+.2f} | CL: {consecutive_losses}")

    return "\n".join(lines)


# =====================================================================
# 7. TELEGRAM: ENTRY ALERT (replaces format_entry_alert)
# =====================================================================

def format_entry_alert_v9(
    side: str,
    entry_type: str,
    entry_price: float,
    sl_price: float,
    tp_price: float,
    rr_ratio: float,
    quantity: float,
    # Target
    target_pool_price: float = 0.0,
    target_pool_type: str = "",
    target_pool_sig: float = 0.0,
    target_tf_sources: str = "",
    # Flow
    flow_conviction: float = 0.0,
    cvd_trend: float = 0.0,
    # Sweep (if reversal/continuation)
    sweep_pool_price: float = 0.0,
    sweep_quality: float = 0.0,
    # ICT
    ict_validation: str = "",
    # Context
    amd_phase: str = "",
    kill_zone: str = "",
) -> str:
    """Entry notification for new strategy."""

    side_icon = "LONG" if side.upper() == "LONG" else "SHORT"
    risk = abs(entry_price - sl_price)
    reward = abs(tp_price - entry_price)
    dollar_risk = risk * quantity

    type_labels = {
        "APPROACH": "APPROACH (flow -> pool)",
        "REVERSAL": "SWEEP REVERSAL",
        "CONTINUATION": "SWEEP CONTINUATION",
    }
    type_label = type_labels.get(entry_type.upper(), entry_type)

    lines = [
        f"<b>NEW TRADE -- {side_icon} [{_esc(type_label)}]</b>",
        "",
        "<b>Levels</b>",
        f"  Entry: <b>${entry_price:,.2f}</b>",
        f"  SL:    ${sl_price:,.2f} (risk: ${risk:.1f})",
        f"  TP:    ${tp_price:,.2f} (reward: ${reward:.1f})",
        f"  R:R:   <b>1:{rr_ratio:.1f}</b>",
        f"  Qty:   {quantity:.4f} BTC | Risk: ${dollar_risk:.2f}",
        "",
        "<b>Why this trade</b>",
    ]

    if entry_type.upper() == "APPROACH":
        lines.append(f"  Flow is pushing {side.lower()} toward unswept pool")
        if target_pool_price > 0:
            lines.append(f"  Target: {_esc(target_pool_type)} ${target_pool_price:,.1f}"
                         f" (sig={target_pool_sig:.0f})")
            if target_tf_sources:
                lines.append(f"  Seen on: {_esc(target_tf_sources)}")
    elif entry_type.upper() == "REVERSAL":
        lines.append(f"  Pool swept -> CISD confirmed -> reversing")
        if sweep_pool_price > 0:
            lines.append(f"  Swept: ${sweep_pool_price:,.1f} (q={sweep_quality:.2f})")
        if target_pool_price > 0:
            lines.append(f"  Delivering to: ${target_pool_price:,.1f}")
    elif entry_type.upper() == "CONTINUATION":
        lines.append(f"  Pool swept but flow continues")
        if target_pool_price > 0:
            lines.append(f"  Next target: ${target_pool_price:,.1f}")

    lines.append(f"  Flow: {flow_conviction:+.2f} | CVD: {cvd_trend:+.2f}")

    if ict_validation:
        lines.append(f"  ICT: {_esc(ict_validation)}")
    if kill_zone:
        lines.append(f"  Session: {_esc(kill_zone.upper())}")

    return "\n".join(lines)


# =====================================================================
# 8. TELEGRAM: /status (format_status_report replacement)
# =====================================================================

def format_status_report_v9(
    price: float,
    atr: float,
    atr_pctile: float,
    balance: float,
    # Engine
    engine_state: str,
    tracking_info: Optional[Dict],
    # Flow
    flow_direction: str,
    flow_conviction: float,
    # Liquidity
    primary_target: Optional[Any],
    bsl_count: int,
    ssl_count: int,
    nearest_bsl_atr: float,
    nearest_ssl_atr: float,
    recent_sweep_count: int,
    # Performance
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
    # Position
    position_lines: List[str] = None,
    # Fee engine
    fee_lines: List[str] = None,
) -> str:
    """Full /status report."""

    wr = winning_trades / total_trades * 100 if total_trades > 0 else 0.0
    losses = total_trades - winning_trades

    lines = [
        "<b>QUANT v9 STATUS -- LIQUIDITY-FIRST</b>",
        "",
        f"ATR: ${atr:.1f} ({atr_pctile:.0%} pctile) | Balance: ${balance:,.2f}",
        "",
        f"<b>Engine: {_esc(engine_state)}</b>",
        f"Flow: {_esc(flow_direction or 'neutral')} ({flow_conviction:+.2f})",
    ]

    if tracking_info:
        lines.append(f"Tracking: {_esc(tracking_info['direction']).upper()}"
                     f" -> {_esc(tracking_info['target'])}"
                     f" ({tracking_info['flow_ticks']}t,"
                     f" {_esc(tracking_info['started'])})")

    # Liquidity
    lines.append("")
    if primary_target:
        try:
            t = primary_target
            lines.append(f"Target: <b>{_esc(t.direction).upper()}"
                         f" -> ${t.pool.price:,.1f}</b>"
                         f" ({t.distance_atr:.1f}ATR sig={t.significance:.0f})")
        except Exception:
            lines.append("Target: none")
    else:
        lines.append("Target: none")

    lines.append(f"Pools: {bsl_count} BSL / {ssl_count} SSL"
                 f" | BSL={nearest_bsl_atr:.1f}A SSL={nearest_ssl_atr:.1f}A"
                 f" | Sweeps: {recent_sweep_count}")

    # Performance
    lines.append("")
    lines.append("<b>Session P&amp;L</b>")
    lines.append(f"  Trades: {total_trades}  W:{winning_trades} L:{losses}  WR: {wr:.0f}%")
    lines.append(f"  Total PnL: ${total_pnl:+.2f}")
    lines.append(f"  Avg W: ${avg_win:+.2f}  Avg L: ${avg_loss:+.2f}")
    lines.append(f"  Expectancy: ${expectancy:+.2f}/trade")
    lines.append(f"  Daily: {daily_trades}/{max_daily}"
                 f" | ConsecL: {consec_losses}/{max_consec}")

    # Fee engine
    if fee_lines:
        lines.append("")
        for fl in fee_lines:
            lines.append(fl)

    # Position
    if position_lines:
        lines.append("")
        for pl in position_lines:
            lines.append(pl)

    return "\n".join(lines)


# =====================================================================
# 9. HELP TEXT (updated commands)
# =====================================================================

HELP_TEXT = (
    "<b>Commands</b>\n"
    "/status -- Full status + liquidity overview\n"
    "/thinking -- Live decision stack + flow + pools\n"
    "/pools -- Full liquidity pool map (all TFs)\n"
    "/flow -- Detailed orderflow breakdown\n"
    "/position -- Current position details\n"
    "/trades -- Recent trade history\n"
    "/stats -- Performance analysis\n"
    "/balance -- Wallet balance\n"
    "/pause -- Pause trading (keep monitoring)\n"
    "/resume -- Resume trading\n"
    "/trail [on|off|auto] -- Toggle trailing SL\n"
    "/config -- Show config values\n"
    "/set &lt;key&gt; &lt;value&gt; -- Adjust config live\n"
    "/setexchange &lt;delta|coinswitch&gt; -- Switch execution\n"
    "/killswitch -- Emergency close + cancel all\n"
    "/resetrisk -- Clear consecutive-loss lockout\n"
    "/start -- Start bot\n"
    "/stop -- Stop bot\n"
    "/help -- This list"
)

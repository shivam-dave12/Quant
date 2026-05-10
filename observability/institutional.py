"""Institutional observability for desk-wise quant runtime.

This module intentionally centralises operator-facing logging and Telegram
reporting. It does not make trade decisions; it converts existing runtime state
into hedge-fund style decision/audit views.
"""
from __future__ import annotations

import html
import logging
import math
import re
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

LOG = logging.getLogger(__name__)

LEGACY_LOG_MARKERS = (
    "ANALYSIS_TICK",
    "DIR_TELEMETRY",
    "[THINK]",
    "POST_SWEEP [",
    "POST-SWEEP QUANT WAIT",
    "Spread cost impairment",
    "CIO_SELECTION\n",
    "Multi-asset loop active",
    "SWEEPS detected",
    "POST-SWEEP ENTERED",
    "RAW_TP_AUDIT",
    "RAW_SL_AUDIT",
)

CRITICAL_ALLOW_MARKERS = (
    "BOT CRASH", "UNCAUGHT", "KILLSWITCH", "ORDER", "FILLED", "REJECT", "ERROR", "CRITICAL",
)


def esc(v: Any) -> str:
    return html.escape(str(v), quote=False)


def fnum(v: Any, n: int = 2, default: str = "—") -> str:
    try:
        x = float(v)
        if not math.isfinite(x):
            return default
        return f"{x:,.{n}f}"
    except Exception:
        return default


def fpct(v: Any, n: int = 1, default: str = "—") -> str:
    try:
        x = float(v)
        if not math.isfinite(x):
            return default
        return f"{x * 100:.{n}f}%"
    except Exception:
        return default


def price(v: Any) -> str:
    try:
        x = float(v or 0.0)
        if abs(x) >= 1000:
            return f"${x:,.2f}"
        if abs(x) >= 1:
            return f"${x:,.4f}"
        return f"${x:,.6f}"
    except Exception:
        return "$—"


def bar(v: Any, width: int = 10) -> str:
    try:
        x = max(0.0, min(1.0, float(v)))
    except Exception:
        x = 0.0
    n = int(round(x * width))
    return "█" * n + "░" * (width - n)


def signed(v: Any, n: int = 2) -> str:
    try:
        x = float(v)
        return f"{x:+.{n}f}"
    except Exception:
        return "+0.00"


def _safe_get(obj: Any, name: str, default: Any = None) -> Any:
    try:
        return getattr(obj, name, default)
    except Exception:
        return default


def should_suppress_legacy_log(record: logging.LogRecord) -> bool:
    try:
        msg = record.getMessage()
    except Exception:
        return False
    if record.levelno >= logging.WARNING:
        return False
    return any(m in msg for m in LEGACY_LOG_MARKERS)


class InstitutionalLogFilter(logging.Filter):
    """Remove old retail/tick-noise logs from terminal/file output."""
    def filter(self, record: logging.LogRecord) -> bool:
        return not should_suppress_legacy_log(record)


def desk_id(bot: Any, ctx: Any) -> str:
    try:
        if getattr(bot, "ticker_desk", None) is not None:
            return str(bot.ticker_desk.router.desk_id_for(ctx.instrument))
    except Exception:
        pass
    try:
        return str(getattr(ctx.instrument, "asset_class", "UNKNOWN")).upper()
    except Exception:
        return "UNKNOWN"


def _policy(ctx: Any) -> Any:
    try:
        from core.instruments import instrument_scope
        from core.market_policy import active_policy
        with instrument_scope(ctx.instrument):
            return active_policy(ctx.instrument)
    except Exception:
        return None


def _last_price(ctx: Any) -> float:
    try:
        return float(ctx.data_manager.get_last_price() or 0.0)
    except Exception:
        return 0.0


def _data_quality(ctx: Any) -> Dict[str, Any]:
    dm = getattr(ctx, "data_manager", None)
    for meth in ("get_data_quality", "get_feed_reliability"):
        try:
            fn = getattr(dm, meth, None)
            if callable(fn):
                out = fn()
                if isinstance(out, dict):
                    return out
        except Exception:
            pass
    return {}


def _strategy_metrics(ctx: Any) -> Dict[str, Any]:
    s = getattr(ctx, "strategy", None)
    out: Dict[str, Any] = {}
    names = [
        "_state", "_last_posterior", "_last_posterior_report", "_last_decision_report",
        "_last_direction", "_last_direction_report", "_last_tp_plan", "_last_sl_plan",
        "_last_entry_audit", "_last_liquidity_report", "_last_setup_score",
    ]
    for n in names:
        try:
            v = getattr(s, n, None)
            if v is not None:
                out[n] = v
        except Exception:
            pass
    try:
        pos = s.get_position()
        out["position"] = pos
    except Exception:
        out["position"] = None
    return out


def _dictish(obj: Any) -> Dict[str, Any]:
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "as_dict"):
        try:
            d = obj.as_dict()
            if isinstance(d, dict):
                return d
        except Exception:
            pass
    out: Dict[str, Any] = {}
    for n in ("probability", "p", "edge", "ev", "expected_value", "llr", "uncertainty", "reason", "verdict", "side", "setup_score", "score"):
        try:
            if hasattr(obj, n):
                out[n] = getattr(obj, n)
        except Exception:
            pass
    return out


def context_snapshot(bot: Any, ctx: Any) -> Dict[str, Any]:
    inst = ctx.instrument
    pol = _policy(ctx)
    q = _data_quality(ctx)
    sm = _strategy_metrics(ctx)
    posterior = _dictish(sm.get("_last_posterior") or sm.get("_last_posterior_report") or sm.get("_last_decision_report"))
    direction = _dictish(sm.get("_last_direction") or sm.get("_last_direction_report"))
    pos = sm.get("position")
    state = getattr(ctx, "phase_name", "UNKNOWN") if pos else ("READY" if getattr(ctx, "ready", False) else "NOT_READY")
    try:
        venue = inst.primary_exchange.value.upper()
    except Exception:
        venue = str(getattr(inst, "primary_exchange", "?")).upper()
    d = {
        "asset": getattr(inst, "asset_id", "?"),
        "symbol": getattr(inst, "display_symbol", "?"),
        "venue": venue,
        "desk": desk_id(bot, ctx),
        "ready": bool(getattr(ctx, "ready", False)),
        "state": state,
        "price": _last_price(ctx),
        "policy_class": getattr(pol, "asset_class", "?"),
        "leverage": getattr(pol, "leverage", getattr(inst, "max_leverage", 1)),
        "margin_pct": getattr(pol, "margin_pct", 0.0),
        "risk_mult": getattr(pol, "risk_multiplier", 0.0),
        "loop_sec": getattr(pol, "loop_interval_sec", getattr(pol, "tick_eval_sec", 0.0)),
        "data_mode": q.get("mode", "single"),
        "sources": q.get("sources", 1),
        "data_note": q.get("note", ""),
        "posterior": posterior,
        "direction": direction,
        "has_position": bool(pos),
        "position": pos,
        "last_tick_age": max(0.0, time.time() - float(getattr(ctx, "last_tick_time", 0.0) or 0.0)) if getattr(ctx, "last_tick_time", 0.0) else None,
    }
    return d


def group_snapshots(bot: Any) -> Dict[str, List[Dict[str, Any]]]:
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for ctx in list(getattr(bot, "contexts", []) or []):
        snap = context_snapshot(bot, ctx)
        groups[snap["desk"]].append(snap)
    for rows in groups.values():
        rows.sort(key=lambda r: (not r.get("ready"), -float(r.get("price") or 0.0)))
    return dict(sorted(groups.items()))


def health_summary(bot: Any) -> Dict[str, Any]:
    ctxs = list(getattr(bot, "contexts", []) or [])
    ready = [c for c in ctxs if getattr(c, "ready", False)]
    pos = [c for c in ctxs if getattr(c, "has_position", False)]
    not_ready = [c for c in ctxs if not getattr(c, "ready", False)]
    return {
        "contexts": len(ctxs),
        "ready": len(ready),
        "not_ready": len(not_ready),
        "positions": len(pos),
        "running": bool(getattr(bot, "running", False)),
        "trading_enabled": bool(getattr(bot, "trading_enabled", True)),
        "pause_reason": getattr(bot, "trading_pause_reason", ""),
    }


def format_startup(bot: Any) -> str:
    h = health_summary(bot)
    groups = group_snapshots(bot)
    lines = [
        "🏛️ <b>HEDGE-FUND COMMAND CENTER ONLINE</b>",
        "<code>Desk → Asset thesis → Venue route → Risk policy → Execution</code>",
        "",
        f"🧭 <b>Runtime</b>: running={h['running']} · contexts={h['contexts']} · ready={h['ready']} · positions={h['positions']}",
    ]
    for desk, rows in groups.items():
        lines.append(f"\n<b>📚 {esc(desk)}</b> · {len(rows)} instruments")
        for r in rows[:12]:
            dot = "🟢" if r["ready"] else "🟡"
            lines.append(
                f"{dot} <b>{esc(r['asset'])}</b> {esc(r['venue'])}:{esc(r['symbol'])} · "
                f"px {price(r['price'])} · lev {fnum(r['leverage'],0)}x · "
                f"risk×{fnum(r['risk_mult'],2)} · {esc(r['state'])}"
            )
        if len(rows) > 12:
            lines.append(f"… +{len(rows)-12} more")
    lines.append("\n<b>Controls</b>: /desks · /desk BTC_GLOBAL · /asset BTC · /why BTC · /risk · /health · /icici")
    return "\n".join(lines)


def format_desks(bot: Any) -> str:
    h = health_summary(bot)
    groups = group_snapshots(bot)
    lines = [
        "🏛️ <b>PORTFOLIO DESK BOARD</b>",
        f"<code>ready {h['ready']}/{h['contexts']} · positions {h['positions']} · trading {'ON' if h['trading_enabled'] else 'PAUSED'}</code>",
    ]
    for desk, rows in groups.items():
        ready = sum(1 for r in rows if r["ready"])
        pos = sum(1 for r in rows if r["has_position"])
        avg_loop = sum(float(r.get("loop_sec") or 0.0) for r in rows) / max(1, len(rows))
        lines.append(f"\n📚 <b>{esc(desk)}</b> · ready {ready}/{len(rows)} · pos {pos} · cadence {avg_loop:.2f}s")
        for r in rows[:10]:
            p = _dictish(r.get("posterior"))
            ptxt = ""
            if p:
                ptxt = f" · p={fnum(p.get('p', p.get('probability')),2)} EV={signed(p.get('ev', p.get('expected_value',0)),2)}"
            lines.append(
                f"  {'🟢' if r['ready'] else '🟡'} <b>{esc(r['asset'])}</b> {esc(r['venue'])}:{esc(r['symbol'])} "
                f"{price(r['price'])} · lev {fnum(r['leverage'],0)}x · {esc(r['state'])}{ptxt}"
            )
        if len(rows) > 10:
            lines.append(f"  … +{len(rows)-10} parked/monitoring")
    return "\n".join(lines)


def find_context(bot: Any, query: str) -> Optional[Any]:
    q = (query or "").strip().upper()
    if not q:
        return None
    for ctx in list(getattr(bot, "contexts", []) or []):
        inst = ctx.instrument
        vals = [getattr(inst, "asset_id", ""), getattr(inst, "display_symbol", "")]
        try:
            vals.append(inst.primary_exchange.value)
        except Exception:
            pass
        if any(q == str(v).upper() or q in str(v).upper() for v in vals):
            return ctx
    return None


def format_desk(bot: Any, desk_query: str) -> str:
    groups = group_snapshots(bot)
    q = (desk_query or "").strip().upper()
    if not q:
        return "Use: <code>/desk BTC_GLOBAL</code> or <code>/desk ICICI_INDEX_OPTIONS</code>"
    desk = None
    for k in groups:
        if q == k.upper() or q in k.upper():
            desk = k; break
    if desk is None:
        return f"Desk not found: <code>{esc(q)}</code>\nAvailable: <code>{esc(', '.join(groups.keys()))}</code>"
    rows = groups[desk]
    lines = [f"🏛️ <b>{esc(desk)} DESK</b>", f"<code>{len(rows)} instruments under active supervision</code>"]
    for r in rows:
        p = _dictish(r.get("posterior"))
        d = _dictish(r.get("direction"))
        lines.append(
            f"\n<b>{esc(r['asset'])}</b> · {esc(r['venue'])}:{esc(r['symbol'])}\n"
            f"  px {price(r['price'])} · state {esc(r['state'])} · lev {fnum(r['leverage'],0)}x · margin {fpct(r['margin_pct'])}\n"
            f"  data {esc(r['data_mode'])}/{esc(r['sources'])} · loop {fnum(r['loop_sec'],2)}s · tick_age {fnum(r['last_tick_age'],1)}s\n"
            f"  posterior p={fnum(p.get('p', p.get('probability')),2)} EV={signed(p.get('ev', p.get('expected_value',0)),2)} LLR={fnum(p.get('llr'),2)} · dir={esc(d.get('delivery', d.get('side','—')))}"
        )
    return "\n".join(lines)


def format_asset(bot: Any, query: str) -> str:
    ctx = find_context(bot, query)
    if ctx is None:
        return f"Asset not found: <code>{esc(query)}</code>"
    r = context_snapshot(bot, ctx)
    p = _dictish(r.get("posterior"))
    d = _dictish(r.get("direction"))
    lines = [
        f"🔬 <b>{esc(r['asset'])} INSTITUTIONAL AUDIT</b>",
        f"<code>{esc(r['desk'])} · {esc(r['venue'])}:{esc(r['symbol'])}</code>",
        "",
        f"📍 <b>State</b>: {esc(r['state'])} · ready={r['ready']} · px={price(r['price'])}",
        f"⚖️ <b>Risk Policy</b>: class={esc(r['policy_class'])} · lev={fnum(r['leverage'],0)}x · margin={fpct(r['margin_pct'])} · risk×{fnum(r['risk_mult'],2)}",
        f"📡 <b>Data</b>: mode={esc(r['data_mode'])} · sources={esc(r['sources'])} · note={esc(r['data_note'] or 'ok')}",
        "",
        "🧠 <b>Posterior / EV</b>",
        f"  p(edge) {fnum(p.get('p', p.get('probability')),3)} {bar(p.get('p', p.get('probability',0)))}",
        f"  EV {signed(p.get('ev', p.get('expected_value',0)),3)} · LLR {fnum(p.get('llr'),2)} · uncertainty {fnum(p.get('uncertainty'),2)}",
        f"  verdict: {esc(p.get('verdict', p.get('reason','—')))}",
        "",
        "🧭 <b>Direction / Auction</b>",
        f"  side={esc(d.get('side', d.get('delivery','—')))} · raw={signed(d.get('raw', d.get('score',0)),3)} · reason={esc(d.get('reason','—'))}",
    ]
    return "\n".join(lines)


def format_why(bot: Any, query: str) -> str:
    ctx = find_context(bot, query)
    if ctx is None:
        return f"Asset not found: <code>{esc(query)}</code>"
    r = context_snapshot(bot, ctx)
    p = _dictish(r.get("posterior"))
    d = _dictish(r.get("direction"))
    reasons = []
    if not r["ready"]:
        reasons.append("data manager not ready")
    if p:
        reasons.append(str(p.get("reason") or p.get("verdict") or "posterior computed"))
    if d:
        reasons.append(str(d.get("reason") or d.get("delivery") or "direction telemetry available"))
    if not reasons:
        reasons.append("no actionable setup; desk remains in observation mode")
    return (
        f"🧾 <b>WHY / WHY-NOT — {esc(r['asset'])}</b>\n"
        f"<code>{esc(r['desk'])} · {esc(r['venue'])}:{esc(r['symbol'])}</code>\n\n"
        f"📍 State: <b>{esc(r['state'])}</b> · Price {price(r['price'])}\n"
        f"🧠 p(edge)={fnum(p.get('p', p.get('probability')),3)} EV={signed(p.get('ev', p.get('expected_value',0)),3)}\n"
        f"🧭 Direction={esc(d.get('delivery', d.get('side','—')))} raw={signed(d.get('raw', d.get('score',0)),3)}\n\n"
        f"<b>Decision trail</b>\n" + "\n".join(f"• {esc(x)}" for x in reasons[:8])
    )


def format_health(bot: Any) -> str:
    h = health_summary(bot)
    groups = group_snapshots(bot)
    lines = [
        "🩺 <b>SYSTEM HEALTH</b>",
        f"running={h['running']} · trading={'ON' if h['trading_enabled'] else 'PAUSED'} · contexts={h['contexts']} · ready={h['ready']} · not_ready={h['not_ready']} · positions={h['positions']}",
    ]
    if h.get("pause_reason"):
        lines.append(f"pause_reason={esc(h['pause_reason'])}")
    for desk, rows in groups.items():
        not_ready = [r for r in rows if not r["ready"]]
        lines.append(f"\n{esc(desk)}: ready {len(rows)-len(not_ready)}/{len(rows)}")
        for r in not_ready[:5]:
            lines.append(f"  🟡 {esc(r['asset'])}: {esc(r['state'])} · data={esc(r['data_note'] or 'not ready')}")
    return "\n".join(lines)


def format_icici(bot: Any) -> str:
    rows = []
    for ctx in list(getattr(bot, "contexts", []) or []):
        try:
            if ctx.instrument.primary_exchange.value.lower() == "icici":
                rows.append(context_snapshot(bot, ctx))
        except Exception:
            pass
    lines = ["🇮🇳 <b>ICICI OPTIONS DESK</b>", "<code>Underlying chart = structure · option premium = execution · long premium only</code>"]
    if not rows:
        lines.append("No active ICICI option contexts.")
        return "\n".join(lines)
    for r in rows:
        lines.append(
            f"\n<b>{esc(r['asset'])}</b> · {esc(r['symbol'])}\n"
            f"  desk={esc(r['desk'])} · state={esc(r['state'])} · ready={r['ready']} · px={price(r['price'])}\n"
            f"  data={esc(r['data_mode'])}/{esc(r['sources'])} · note={esc(r['data_note'] or 'ok')}"
        )
    return "\n".join(lines)



# ---------------------------------------------------------------------------
# Institutional calculation/parameter/selector audit commands
# ---------------------------------------------------------------------------

def _flatten(prefix: str, value: Any, out: List[Tuple[str, Any]], *, max_items: int = 120) -> None:
    """Flatten dict/dataclass-ish values for Telegram-safe audit display."""
    if len(out) >= max_items:
        return
    if value is None:
        return
    if isinstance(value, dict):
        for k, v in value.items():
            key = f"{prefix}.{k}" if prefix else str(k)
            _flatten(key, v, out, max_items=max_items)
        return
    if isinstance(value, (list, tuple, set)):
        if not value:
            return
        if len(value) <= 8 and all(not isinstance(x, (dict, list, tuple, set)) for x in value):
            out.append((prefix, ", ".join(str(x) for x in value)))
            return
        for i, v in enumerate(list(value)[:8]):
            _flatten(f"{prefix}[{i}]", v, out, max_items=max_items)
        if len(value) > 8:
            out.append((f"{prefix}.truncated", f"+{len(value)-8} more"))
        return
    # dataclass / object snapshot: only simple public attrs
    if not isinstance(value, (str, int, float, bool)) and hasattr(value, "__dict__"):
        d = {k: v for k, v in vars(value).items() if not k.startswith("__") and not callable(v)}
        if d:
            _flatten(prefix, d, out, max_items=max_items)
            return
    out.append((prefix, value))


def _fmt_kv_rows(rows: List[Tuple[str, Any]], limit: int = 80) -> List[str]:
    lines: List[str] = []
    for k, v in rows[:limit]:
        if isinstance(v, float):
            vv = fnum(v, 6)
        else:
            vv = str(v)
        if len(vv) > 180:
            vv = vv[:177] + "..."
        lines.append(f"  <code>{esc(k)}</code> = <b>{esc(vv)}</b>")
    if len(rows) > limit:
        lines.append(f"  … +{len(rows)-limit} more values")
    return lines


def _raw_strategy_audit(ctx: Any) -> Dict[str, Any]:
    s = getattr(ctx, "strategy", None)
    out: Dict[str, Any] = {}
    if s is None:
        return out
    # Capture last-calculation caches without forcing the strategy to recompute.
    preferred = [
        "_last_posterior", "_last_posterior_report", "_last_decision_report",
        "_last_direction", "_last_direction_report", "_last_tp_plan", "_last_sl_plan",
        "_last_entry_audit", "_last_liquidity_report", "_last_setup_score",
        "_last_fee_audit", "_last_rr_audit", "_last_trailing_audit", "_last_rejection_reason",
    ]
    for name in preferred:
        try:
            val = getattr(s, name, None)
            if val is not None:
                out[name] = _dictish(val) or val
        except Exception:
            pass
    # Add selected simple internal state that explains what the engine is doing.
    for name in ("_state", "_pos", "_side", "_entry_price", "_atr_5m", "_atr_15m", "_last_price", "_last_trade_ts"):
        try:
            val = getattr(s, name, None)
            if val is not None:
                out[name] = _dictish(val) or val
        except Exception:
            pass
    return out


def format_calculations(bot: Any, query: str) -> str:
    """Full calculation audit for a symbol/asset."""
    ctx = find_context(bot, query)
    if ctx is None:
        return f"Use: <code>/calc BTC</code> or <code>/calc NIFTY</code>\nAsset not found: <code>{esc(query)}</code>"
    snap = context_snapshot(bot, ctx)
    inst = ctx.instrument
    policy = _policy(ctx)
    raw = _raw_strategy_audit(ctx)
    rows: List[Tuple[str, Any]] = []
    rows.extend([
        ("asset", snap.get("asset")),
        ("desk", snap.get("desk")),
        ("venue", snap.get("venue")),
        ("execution_symbol", getattr(inst, "execution_symbol", "")),
        ("display_symbol", getattr(inst, "display_symbol", "")),
        ("ready", snap.get("ready")),
        ("state", snap.get("state")),
        ("last_price", snap.get("price")),
        ("tick_age_sec", snap.get("last_tick_age")),
    ])
    if policy is not None:
        rows.extend([
            ("policy.asset_class", getattr(policy, "asset_class", "")),
            ("policy.leverage", getattr(policy, "leverage", "")),
            ("policy.margin_pct", getattr(policy, "margin_pct", "")),
            ("policy.risk_multiplier", getattr(policy, "risk_multiplier", "")),
            ("policy.loop_interval_sec", getattr(policy, "loop_interval_sec", getattr(policy, "tick_eval_sec", ""))),
        ])
    _flatten("instrument.primary", getattr(inst, "primary", None), rows, max_items=220)
    _flatten("data_quality", _data_quality(ctx), rows, max_items=220)
    _flatten("strategy", raw, rows, max_items=220)
    lines = [
        f"🧮 <b>CALCULATION AUDIT — {esc(snap.get('asset'))}</b>",
        f"<code>{esc(snap.get('desk'))} · {esc(snap.get('venue'))}:{esc(snap.get('symbol'))}</code>",
        "",
        "<b>All values below are read from the live runtime cache; this command does not alter decisions.</b>",
    ]
    lines.extend(_fmt_kv_rows(rows, limit=120))
    return "\n".join(lines)


def format_parameters(bot: Any, query: str = "") -> str:
    """Runtime parameters: config + per-asset policy + selected instrument metadata."""
    import config as cfg
    q = (query or "").strip()
    ctx = find_context(bot, q) if q else None
    names = [
        "FUND_PAPER_MODE", "FUND_LIVE_ORDERING_ENABLED", "EXECUTION_EXCHANGE", "DELTA_TESTNET",
        "DYNAMIC_DESK_MAX_ACTIVE_CONTEXTS", "DYNAMIC_DESK_INCUMBENT_MIN_SCORE", "DYNAMIC_DESK_REPLACE_MARGIN_PCT",
        "DESK_BTC_GLOBAL_MAX_ACTIVE", "DESK_CRYPTO_ALTS_MAX_ACTIVE", "DESK_COMMODITIES_GLOBAL_MAX_ACTIVE",
        "DESK_US_STOCK_DERIVATIVES_MAX_ACTIVE", "DESK_ICICI_INDEX_OPTIONS_MAX_ACTIVE", "DESK_ICICI_STOCK_OPTIONS_MAX_ACTIVE",
        "DESK_MONITOR_ALL_IDS", "MAX_POLICY_LEVERAGE", "POLICY_CRYPTO_LEVERAGE_UTIL", "POLICY_COMMODITY_LEVERAGE_UTIL",
        "POLICY_EQUITY_LEVERAGE_UTIL", "ICICI_LONG_PREMIUM_ONLY", "ICICI_USE_UNDERLYING_CHART_FOR_STRUCTURE",
        "ICICI_ALLOW_CLOSED_MARKET_HISTORICAL_WARMUP", "ICICI_CLOSED_MARKET_QUOTE_PROBE", "ICICI_BREEZE_MIN_CALL_GAP_SEC",
        "TELEGRAM_FORWARD_RAW_LOGS", "TELEGRAM_LEGACY_STRATEGY_ALERTS_ENABLED",
    ]
    rows: List[Tuple[str, Any]] = [(n, getattr(cfg, n, "<missing>")) for n in names]
    lines = ["⚙️ <b>RUNTIME PARAMETERS</b>", "<code>config.py is policy source; .env is secrets only</code>"]
    lines.extend(_fmt_kv_rows(rows, limit=120))
    if ctx is not None:
        snap = context_snapshot(bot, ctx)
        pol = _policy(ctx)
        prows: List[Tuple[str, Any]] = []
        _flatten("active_policy", pol, prows, max_items=80)
        _flatten("instrument", ctx.instrument, prows, max_items=120)
        lines.append(f"\n🔬 <b>ACTIVE POLICY — {esc(snap.get('asset'))}</b>")
        lines.extend(_fmt_kv_rows(prows, limit=90))
    else:
        lines.append("\nUse <code>/params BTC</code> for per-asset policy + instrument metadata.")
    return "\n".join(lines)


def format_selector(bot: Any, desk_query: str = "") -> str:
    td = getattr(bot, "ticker_desk", None)
    sel = getattr(td, "last_selection", None)
    if sel is None:
        return "🧭 <b>SELECTOR AUDIT</b>\nNo selector snapshot is available yet. Start the bot or wait for the first desk cycle."
    q = (desk_query or "").strip().upper()
    rows = list(getattr(sel, "rows", ()) or ())
    if q:
        rows = [r for r in rows if q in str(getattr(r, "desk_id", "")).upper() or q in str(getattr(r, "asset_id", "")).upper()]
    selected = [r for r in rows if getattr(r, "selected", False)]
    parked = [r for r in rows if not getattr(r, "selected", False)]
    lines = [
        "🧭 <b>DESK SELECTOR AUDIT</b>",
        f"<code>snapshot_age={fnum(time.time()-float(sel.timestamp),1)}s · selected={len(selected)} · parked={len(parked)} · rows={len(rows)}</code>",
    ]
    if getattr(sel, "notes", None):
        lines.append("\n<b>Desk notes</b>")
        lines.extend(f"• {esc(n)}" for n in list(sel.notes)[:20])
    by_desk: Dict[str, List[Any]] = defaultdict(list)
    for r in rows:
        by_desk[str(getattr(r, "desk_id", "UNKNOWN"))].append(r)
    for desk, drows in sorted(by_desk.items()):
        lines.append(f"\n📚 <b>{esc(desk)}</b>")
        for r in drows[:16]:
            mark = "✅" if getattr(r, "selected", False) else "▫️"
            lines.append(
                f"{mark} <b>{esc(getattr(r,'asset_id','?'))}</b> rank={getattr(r,'rank',0)}/{getattr(r,'desk_rank',0)} "
                f"score={fnum(getattr(r,'score',0),3)} route={esc(getattr(r,'route_exchange',''))}:{fnum(getattr(r,'route_score',0),2)} "
                f"spread={fnum(getattr(r,'spread_bps',0),1)}bps liq={fnum(getattr(r,'turnover_score',0),2)} vol={fnum(getattr(r,'volatility_score',0),2)} "
                f"opt={fnum(getattr(r,'option_score',0),2)} reason={esc(getattr(r,'reason',''))} route_reason={esc(getattr(r,'route_reason',''))}"
            )
        if len(drows) > 16:
            lines.append(f"… +{len(drows)-16} more rows")
    return "\n".join(lines)


def format_shutdown_diagnostics() -> str:
    """Explain last SIGTERM/SIGINT using locally available evidence."""
    import os, json
    from pathlib import Path
    p = Path("data/last_shutdown.json")
    lines = [
        "🧯 <b>SHUTDOWN / CRASH DIAGNOSTICS</b>",
        "<code>SIGTERM(15) is an external termination signal. Python cannot identify the sender from a normal signal handler.</code>",
    ]
    if p.exists():
        try:
            data = json.loads(p.read_text())
            lines.append("\n<b>Last captured shutdown</b>")
            for k, v in data.items():
                lines.append(f"  <code>{esc(k)}</code> = <b>{esc(v)}</b>")
        except Exception as e:
            lines.append(f"Could not parse {p}: {esc(e)}")
    else:
        lines.append("\nNo local shutdown file captured yet.")
    lines += [
        "\n<b>Most common sources</b>",
        "• <code>docker stop</code> or container restart policy",
        "• ECS/systemd/PM2/supervisor health restart",
        "• EC2 shutdown/reboot/deploy script",
        "• OOM killer or host pressure causing supervisor to terminate process",
        "• Manual Ctrl+C/SIGTERM from another shell",
        "\n<b>Run on AWS host</b>",
        "<code>docker ps -a --no-trunc</code>",
        "<code>docker inspect &lt;container&gt; --format '{{.State.ExitCode}} {{.State.OOMKilled}} {{.State.Error}} {{.State.FinishedAt}}'</code>",
        "<code>dmesg -T | egrep -i 'killed process|oom|out of memory'</code>",
        "<code>journalctl -u docker --since '30 min ago'</code>",
    ]
    return "\n".join(lines)


def format_cycle_log(bot: Any) -> str:
    groups = group_snapshots(bot)
    parts = ["🏛 DESK_CYCLE"]
    for desk, rows in groups.items():
        ready = sum(1 for r in rows if r["ready"])
        pos = sum(1 for r in rows if r["has_position"])
        top = ", ".join(f"{r['asset']}@{price(r['price'])}" for r in rows[:4])
        parts.append(f"[{desk}] ready={ready}/{len(rows)} pos={pos} :: {top}")
    return "\n".join(parts)

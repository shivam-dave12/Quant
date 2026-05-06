from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from threading import RLock
from time import time
from typing import Any, Deque, Dict, List, Optional
try:
    from core.redaction import redact_sensitive
except Exception:
    def redact_sensitive(x): return x

MAX_POINTS = 2500
MAX_EVENTS = 4000
MAX_ALERTS = 1000
MAX_TRADES = 1000
MAX_DECISIONS = 1500


@dataclass
class AssetState:
    asset: str
    symbol: str = ""
    venue: str = ""
    price: float = 0.0
    state: str = "UNKNOWN"
    phase: str = "UNKNOWN"
    direction: str = ""
    confidence: float = 0.0
    posterior: float = 0.0
    ev: float = 0.0
    llr: float = 0.0
    uncertainty: float = 0.0
    spread_bps: float = 0.0
    spread_atr: float = 0.0
    atr: float = 0.0
    size_mult: float = 1.0
    risk_mult: float = 0.0
    margin_pct: float = 0.0
    leverage: str = ""
    margin: str = ""
    open_positions: int = 0
    max_positions: int = 0
    last_reason: str = ""
    last_decision: str = ""
    last_update: float = field(default_factory=time)
    data_status: str = "UNKNOWN"
    primary: str = ""
    secondary: str = ""
    policy: str = ""
    health: str = "UNKNOWN"


@dataclass
class PositionState:
    key: str
    asset: str
    symbol: str
    venue: str
    side: str
    qty: float = 0.0
    entry: float = 0.0
    price: float = 0.0
    sl: float = 0.0
    tp: float = 0.0
    upnl: float = 0.0
    r: float = 0.0
    mfe_r: float = 0.0
    trailing: str = "OFF"
    bracket: str = "UNKNOWN"
    state: str = "ACTIVE"
    opened_at: float = field(default_factory=time)
    updated_at: float = field(default_factory=time)
    notes: str = ""


@dataclass
class AlertState:
    ts: float
    severity: str
    title: str
    message: str
    asset: str = ""
    symbol: str = ""
    venue: str = ""


@dataclass
class TradeState:
    ts: float
    asset: str
    symbol: str
    venue: str
    side: str
    entry: float
    exit: float
    pnl: float
    r: float = 0.0
    margin_used: float = 0.0
    margin_pnl_pct: float = 0.0
    capital_result: str = ""
    reason: str = ""


@dataclass
class DecisionState:
    ts: float
    asset: str
    symbol: str
    venue: str
    kind: str
    side: str = ""
    p: float = 0.0
    ev: float = 0.0
    rr: float = 0.0
    entry: float = 0.0
    sl: float = 0.0
    tp: float = 0.0
    reason: str = ""
    raw: str = ""


class DashboardState:
    def __init__(self) -> None:
        self._lock = RLock()
        self.started_at = time()
        self.last_heartbeat = 0.0
        self.bot_online = False
        self.mode = "unknown"
        self.source = "none"
        self.assets: Dict[str, AssetState] = {}
        self.positions: Dict[str, PositionState] = {}
        self.alerts: Deque[AlertState] = deque(maxlen=MAX_ALERTS)
        self.trades: Deque[TradeState] = deque(maxlen=MAX_TRADES)
        self.decisions: Deque[DecisionState] = deque(maxlen=MAX_DECISIONS)
        self.events: Deque[dict[str, Any]] = deque(maxlen=MAX_EVENTS)
        self.price_history: Dict[str, Deque[dict[str, float]]] = defaultdict(lambda: deque(maxlen=MAX_POINTS))
        self.spread_history: Dict[str, Deque[dict[str, float]]] = defaultdict(lambda: deque(maxlen=MAX_POINTS))
        self.r_history: Dict[str, Deque[dict[str, float]]] = defaultdict(lambda: deque(maxlen=MAX_POINTS))
        self.posterior_history: Dict[str, Deque[dict[str, float]]] = defaultdict(lambda: deque(maxlen=MAX_POINTS))
        self.pnl_history: Deque[dict[str, float]] = deque(maxlen=MAX_POINTS)
        self.total_realized = 0.0
        self.ingested_lines = 0
        self.parsed_events = 0

    def snapshot(self, asset: Optional[str] = None) -> dict[str, Any]:
        with self._lock:
            now = time()
            if self.last_heartbeat and now - self.last_heartbeat < 25:
                self.bot_online = True
            else:
                self.bot_online = False
            total_upnl = sum(p.upnl for p in self.positions.values())
            self.pnl_history.append({"ts": now, "value": total_upnl + self.total_realized, "upnl": total_upnl, "realized": self.total_realized})
            assets = [asdict(x) for x in sorted(self.assets.values(), key=lambda a: a.asset)]
            if asset:
                asset_u = asset.upper()
                assets = [x for x in assets if x.get("asset") == asset_u]
            return {
                "system": {
                    "bot_online": self.bot_online,
                    "mode": self.mode,
                    "source": self.source,
                    "last_heartbeat": self.last_heartbeat,
                    "uptime_sec": now - self.started_at,
                    "open_positions": len(self.positions),
                    "assets": len(self.assets),
                    "total_upnl": total_upnl,
                    "total_realized": self.total_realized,
                    "event_count": len(self.events),
                    "ingested_lines": self.ingested_lines,
                    "parsed_events": self.parsed_events,
                },
                "assets": assets,
                "positions": [asdict(x) for x in sorted(self.positions.values(), key=lambda p: p.asset)],
                "alerts": [asdict(x) for x in list(self.alerts)],
                "trades": [asdict(x) for x in list(self.trades)],
                "decisions": [asdict(x) for x in list(self.decisions)],
                "events": list(self.events),
                "charts": self._charts(asset),
                "heatmap": self._heatmap(),
                "ts": now,
            }

    def asset_detail(self, asset: str) -> dict[str, Any]:
        a = asset.upper()
        snap = self.snapshot(asset=a)
        snap["positions"] = [x for x in snap["positions"] if x["asset"] == a]
        snap["alerts"] = [x for x in snap["alerts"] if x["asset"] == a]
        snap["trades"] = [x for x in snap["trades"] if x["asset"] == a]
        snap["decisions"] = [x for x in snap["decisions"] if x["asset"] == a]
        snap["events"] = [x for x in snap["events"] if str(x.get("asset", "")).upper() == a][:250]
        return snap

    def _charts(self, asset: Optional[str]) -> dict[str, Any]:
        if asset:
            a = asset.upper()
            return {
                "price": {a: list(self.price_history.get(a, []))},
                "spread": {a: list(self.spread_history.get(a, []))},
                "r": {a: list(self.r_history.get(a, []))},
                "posterior": {a: list(self.posterior_history.get(a, []))},
                "pnl": list(self.pnl_history),
            }
        return {
            "price": {k: list(v) for k, v in self.price_history.items()},
            "spread": {k: list(v) for k, v in self.spread_history.items()},
            "r": {k: list(v) for k, v in self.r_history.items()},
            "posterior": {k: list(v) for k, v in self.posterior_history.items()},
            "pnl": list(self.pnl_history),
        }

    def _heatmap(self) -> list[dict[str, Any]]:
        out = []
        for a in self.assets.values():
            score = 0.0
            score += min(max(a.confidence, a.posterior), 1.0) * 45.0
            score += max(min(a.ev, 2.0), 0.0) * 20.0
            score += max(0.0, 1.0 - min(a.spread_atr, 3.0) / 3.0) * 20.0
            score += 15.0 if a.state in {"SCANNING", "POST_SWEEP", "DIRECTION"} else 5.0
            out.append({"asset": a.asset, "score": round(score, 2), "state": a.state, "phase": a.phase, "price": a.price, "spread_bps": a.spread_bps, "posterior": max(a.posterior, a.confidence), "ev": a.ev})
        return sorted(out, key=lambda x: x["score"], reverse=True)

    def diagnostics(self) -> dict[str, Any]:
        with self._lock:
            now = time()
            online_age = now - self.last_heartbeat if self.last_heartbeat else None
            return {
                "bot_online": bool(self.bot_online),
                "source": self.source,
                "last_heartbeat": self.last_heartbeat,
                "heartbeat_age_sec": online_age,
                "ingested_lines": self.ingested_lines,
                "parsed_events": self.parsed_events,
                "assets": len(self.assets),
                "positions": len(self.positions),
                "alerts": len(self.alerts),
                "expected_inputs": [
                    "direct telemetry: DASHBOARD_ENABLED=true and DASHBOARD_URL points to this backend",
                    "fallback log tail: trading-dashboard-tail service posts /api/events and /api/ingested-line",
                ],
                "status": "OK" if self.last_heartbeat and online_age is not None and online_age < 25 else "NO_HEARTBEAT",
                "probable_root_cause": (
                    "No direct telemetry or log-tail heartbeat received. Check bot env DASHBOARD_URL/DASHBOARD_ENABLED or systemctl status trading-dashboard-tail."
                    if not self.last_heartbeat else "Heartbeat exists; check parsed_events/assets if no cards are visible."
                ),
            }

    def apply(self, event: dict[str, Any]) -> None:
        with self._lock:
            event = redact_sensitive(dict(event))
            event.setdefault("ts", time())
            event.setdefault("type", "event")
            etype = str(event.get("type", "event"))
            # Count every structured event including heartbeat.  The old dashboard
            # returned before appending heartbeat, so a running tail/direct emitter
            # could still display parsed_events=0 and look dead.
            self.events.appendleft(event)
            self.parsed_events += 1
            if etype == "heartbeat":
                self.last_heartbeat = float(event.get("ts", time()))
                self.bot_online = True
                self.mode = str(event.get("mode", self.mode))
                self.source = str(event.get("source", self.source))
                return
            if etype in {"catalog_asset", "market_data", "scan", "direction", "spread", "candidate_deferred", "candidate_approved", "posterior", "sl_anchor", "sl_envelope", "tp_audit", "trail_proposal", "trail_hold", "trail_dispatch", "tail_status"}:
                self._update_asset(event)
                if etype in {"candidate_deferred", "candidate_approved", "posterior", "sl_anchor", "sl_envelope", "tp_audit", "trail_proposal", "trail_hold", "trail_dispatch"}:
                    self._add_decision(event)
                return
            if etype in {"position_opened", "position_update", "bracket_update", "trail_update"}:
                self._update_position(event)
                return
            if etype == "position_closed":
                self._close_position(event)
                return
            if etype in {"alert", "warning", "error", "protection_failure", "execution_failure", "market_data_error"}:
                self._add_alert(event)
                return

    def note_line(self) -> None:
        with self._lock:
            self.ingested_lines += 1

    def _asset_key(self, event: dict[str, Any]) -> str:
        return str(event.get("asset") or event.get("symbol") or "UNKNOWN").upper()

    def _position_key(self, event: dict[str, Any]) -> str:
        return f"{str(event.get('asset','')).upper()}::{str(event.get('venue','')).upper()}::{str(event.get('symbol','')).upper()}"

    def _update_asset(self, event: dict[str, Any]) -> None:
        k = self._asset_key(event)
        cur = self.assets.get(k) or AssetState(asset=k)
        for field_name in ["symbol", "venue", "state", "phase", "direction", "last_reason", "last_decision", "data_status", "primary", "secondary", "leverage", "margin", "policy", "health"]:
            val = event.get(field_name)
            if val not in (None, ""):
                setattr(cur, field_name, str(val))
        for field_name in ["price", "confidence", "posterior", "ev", "llr", "uncertainty", "spread_bps", "spread_atr", "atr", "size_mult", "risk_mult", "margin_pct"]:
            val = event.get(field_name)
            if val not in (None, ""):
                try: setattr(cur, field_name, float(val))
                except Exception: pass
        for field_name in ["open_positions", "max_positions"]:
            val = event.get(field_name)
            if val not in (None, ""):
                try: setattr(cur, field_name, int(val))
                except Exception: pass
        cur.last_update = float(event.get("ts", time()))
        if not cur.health or cur.health == "UNKNOWN":
            cur.health = "OK" if cur.data_status not in {"ERROR", "FAILED"} else "BAD"
        self.assets[k] = cur
        if cur.price:
            self.price_history[k].append({"ts": cur.last_update, "value": cur.price})
        if cur.spread_bps or cur.spread_atr:
            self.spread_history[k].append({"ts": cur.last_update, "bps": cur.spread_bps, "atr": cur.spread_atr})
        if cur.posterior or cur.ev:
            self.posterior_history[k].append({"ts": cur.last_update, "p": cur.posterior, "ev": cur.ev, "conf": cur.confidence})

    def _update_position(self, event: dict[str, Any]) -> None:
        k = self._position_key(event)
        cur = self.positions.get(k) or PositionState(
            key=k, asset=str(event.get("asset", "")).upper(), symbol=str(event.get("symbol", "")),
            venue=str(event.get("venue", "")).upper(), side=str(event.get("side", "")).upper())
        for field_name in ["asset", "symbol", "venue", "side", "state", "trailing", "bracket", "notes"]:
            val = event.get(field_name)
            if val not in (None, ""):
                setattr(cur, field_name, str(val).upper() if field_name in {"asset", "venue", "side", "trailing", "state"} else str(val))
        for field_name in ["qty", "entry", "price", "sl", "tp", "upnl", "r", "mfe_r"]:
            val = event.get(field_name)
            if val not in (None, ""):
                try: setattr(cur, field_name, float(val))
                except Exception: pass
        cur.updated_at = float(event.get("ts", time()))
        self.positions[k] = cur
        self._update_asset({**event, "type": "market_data", "state": cur.state, "price": cur.price})
        self.r_history[cur.asset].append({"ts": cur.updated_at, "r": cur.r, "mfe_r": cur.mfe_r})

    def _close_position(self, event: dict[str, Any]) -> None:
        k = self._position_key(event)
        self.positions.pop(k, None)
        pnl = float(event.get("pnl", 0.0) or 0.0)
        self.total_realized += pnl
        self.trades.appendleft(TradeState(
            ts=float(event.get("ts", time())), asset=str(event.get("asset", "")).upper(),
            symbol=str(event.get("symbol", "")), venue=str(event.get("venue", "")).upper(),
            side=str(event.get("side", "")).upper(), entry=float(event.get("entry", 0.0) or 0.0),
            exit=float(event.get("exit", 0.0) or 0.0), pnl=pnl,
            r=float(event.get("r", event.get("achieved_r", 0.0)) or 0.0),
            margin_used=float(event.get("margin_used", 0.0) or 0.0),
            margin_pnl_pct=float(event.get("margin_pnl_pct", 0.0) or 0.0),
            capital_result=str(event.get("capital_result", "PROFIT" if pnl > 0 else ("LOSS" if pnl < 0 else "FLAT"))),
            reason=str(event.get("reason", ""))))

    def _add_alert(self, event: dict[str, Any]) -> None:
        self.alerts.appendleft(AlertState(
            ts=float(event.get("ts", time())), severity=str(event.get("severity", "warning")),
            title=str(event.get("title", event.get("type", "alert"))), message=str(event.get("message", "")),
            asset=str(event.get("asset", "")).upper(), symbol=str(event.get("symbol", "")), venue=str(event.get("venue", "")).upper()))
        self._update_asset({**event, "data_status": "ERROR" if event.get("severity") == "critical" else "WARN", "health": "BAD" if event.get("severity") == "critical" else "WARN"})

    def _add_decision(self, event: dict[str, Any]) -> None:
        self.decisions.appendleft(DecisionState(
            ts=float(event.get("ts", time())), asset=str(event.get("asset", "")).upper(), symbol=str(event.get("symbol", "")), venue=str(event.get("venue", "")).upper(),
            kind=str(event.get("type", "decision")), side=str(event.get("side", "")), p=float(event.get("posterior", event.get("confidence", 0.0)) or 0.0),
            ev=float(event.get("ev", 0.0) or 0.0), rr=float(event.get("rr", 0.0) or 0.0), entry=float(event.get("entry", 0.0) or 0.0),
            sl=float(event.get("sl", 0.0) or 0.0), tp=float(event.get("tp", 0.0) or 0.0), reason=str(event.get("last_reason", event.get("reason", ""))), raw=str(event.get("message", event.get("raw", "")))[:1800]))

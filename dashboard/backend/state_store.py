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
MAX_AGENT_HISTORY = 1500


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


@dataclass
class AgentState:
    agent: str
    role: str = ""
    status: str = "IDLE"
    score: float = 0.0
    selected: int = 0
    rejected: int = 0
    approved: int = 0
    blocked: int = 0
    latency_ms: float = 0.0
    last_reason: str = ""
    detail: dict[str, Any] = field(default_factory=dict)
    last_update: float = field(default_factory=time)


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
        self.agent_history: Dict[str, Deque[dict[str, float]]] = defaultdict(lambda: deque(maxlen=MAX_AGENT_HISTORY))
        self.pnl_history: Deque[dict[str, float]] = deque(maxlen=MAX_POINTS)
        self.agents: Dict[str, AgentState] = {}
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
            metrics = self._metrics(now, total_upnl)
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
                "agents": [asdict(x) for x in sorted(self.agents.values(), key=lambda a: a.agent)],
                "charts": self._charts(asset),
                "heatmap": self._heatmap(),
                "metrics": metrics,
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
                "agents": {k: list(v) for k, v in self.agent_history.items()},
                "pnl": list(self.pnl_history),
            }
        return {
            "price": {k: list(v) for k, v in self.price_history.items()},
            "spread": {k: list(v) for k, v in self.spread_history.items()},
            "r": {k: list(v) for k, v in self.r_history.items()},
            "posterior": {k: list(v) for k, v in self.posterior_history.items()},
            "agents": {k: list(v) for k, v in self.agent_history.items()},
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
            score = max(0.0, min(100.0, score))
            out.append({"asset": a.asset, "score": round(score, 2), "state": a.state, "phase": a.phase, "price": a.price, "spread_bps": a.spread_bps, "posterior": max(a.posterior, a.confidence), "ev": a.ev})
        return sorted(out, key=lambda x: x["score"], reverse=True)

    def _metrics(self, now: float, total_upnl: float) -> dict[str, Any]:
        trade_list = list(self.trades)
        wins = [t for t in trade_list if t.pnl > 0]
        losses = [t for t in trade_list if t.pnl < 0]
        gross_profit = sum(t.pnl for t in wins)
        gross_loss = abs(sum(t.pnl for t in losses))
        fresh_assets = sum(1 for a in self.assets.values() if now - a.last_update < 30)
        stale_assets = max(0, len(self.assets) - fresh_assets)
        latest_event_ts = max((float(e.get("ts", 0.0) or 0.0) for e in self.events), default=0.0)
        best = self._heatmap()[0] if self.assets else {}
        return {
            "decision_count": len(self.decisions),
            "alert_count": len(self.alerts),
            "trade_count": len(trade_list),
            "win_rate": (len(wins) / len(trade_list)) if trade_list else 0.0,
            "profit_factor": (gross_profit / gross_loss) if gross_loss > 0 else (gross_profit if gross_profit > 0 else 0.0),
            "avg_r": (sum(t.r for t in trade_list) / len(trade_list)) if trade_list else 0.0,
            "risk_heat": total_upnl + self.total_realized,
            "fresh_assets": fresh_assets,
            "stale_assets": stale_assets,
            "coverage": (fresh_assets / len(self.assets)) if self.assets else 0.0,
            "last_event_age_sec": (now - latest_event_ts) if latest_event_ts else None,
            "best_asset": best.get("asset", ""),
            "best_score": float(best.get("score", 0.0) or 0.0),
            "agent_count": len(self.agents),
            "agents_active": sum(1 for a in self.agents.values() if a.status not in {"IDLE", "OFFLINE"}),
        }

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
            if etype in {"fund_cycle", "agent_update"}:
                self._update_agents(event)
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

    def _update_agents(self, event: dict[str, Any]) -> None:
        now = float(event.get("ts", time()) or time())
        if event.get("type") == "agent_update":
            name = str(event.get("agent", "Agent")).strip() or "Agent"
            cur = self.agents.get(name) or AgentState(agent=name)
            for field_name in ["role", "status", "last_reason"]:
                val = event.get(field_name)
                if val not in (None, ""):
                    setattr(cur, field_name, str(val))
            for field_name in ["score", "latency_ms"]:
                val = event.get(field_name)
                if val not in (None, ""):
                    try: setattr(cur, field_name, float(val))
                    except Exception: pass
            for field_name in ["selected", "rejected", "approved", "blocked"]:
                val = event.get(field_name)
                if val not in (None, ""):
                    try: setattr(cur, field_name, int(val))
                    except Exception: pass
            cur.detail = dict(event.get("detail") or cur.detail or {})
            cur.last_update = now
            self.agents[name] = cur
            self.agent_history[name].append({"ts": now, "score": cur.score, "selected": cur.selected, "blocked": cur.blocked, "latency_ms": cur.latency_ms})
            return

        selected = list(event.get("selected") or [])
        rejected = list(event.get("rejected") or [])
        verdicts = dict(event.get("risk_verdicts") or {})
        setup_candidates = list(event.get("setup_candidates") or [])
        mode = str(event.get("mode", "paper")).upper()
        selected_score = sum(float(x.get("score", 0.0) or 0.0) for x in selected) / max(1, len(selected))
        agent_rows = [
            ("PortfolioCIO", "Governor", "ACTIVE", selected_score, len(selected), len(rejected), 0, 0, "selected executable desks"),
            ("UniverseAgent", "Discovery", "ACTIVE", self._agent_quality_from_assets(), len(self.assets), 0, 0, 0, "live instruments and data readiness"),
            ("TickerSelectionAgent", "Ranking", "ACTIVE", selected_score, len(selected), len(rejected), 0, 0, "ranked by spread, freshness, depth, warmup"),
            ("SetupSelectionAgent", "Alpha triage", "ACTIVE", self._avg_setup_score(setup_candidates), len(setup_candidates), 0, 0, 0, "cached setup quality and EV"),
            ("RiskCommitteeAgent", "Risk", mode, self._risk_score(verdicts), len(selected), len(rejected), self._approved_count(verdicts), self._blocked_count(verdicts), "deterministic pre-entry gate"),
            ("ExecutionDeskAgent", "Execution", "PAPER" if mode == "PAPER" else "LIVE", 1.0 if mode == "PAPER" else 0.85, 0, 0, 0, 0, "venue routing guard"),
            ("PostTradeLearningAgent", "Learning", "WATCH", self._learning_score(), len(self.trades), 0, 0, 0, "closed-trade attribution"),
        ]
        for name, role, status, score, selected_n, rejected_n, approved_n, blocked_n, reason in agent_rows:
            cur = self.agents.get(name) or AgentState(agent=name)
            cur.role = role
            cur.status = status
            cur.score = max(0.0, min(1.0, float(score or 0.0)))
            cur.selected = int(selected_n or 0)
            cur.rejected = int(rejected_n or 0)
            cur.approved = int(approved_n or 0)
            cur.blocked = int(blocked_n or 0)
            cur.last_reason = reason
            cur.detail = {
                "selected": selected[:12],
                "rejected": rejected[:12],
                "verdicts": verdicts,
                "setup_candidates": setup_candidates[:12],
                "notes": list(event.get("notes") or []),
            }
            cur.last_update = now
            self.agents[name] = cur
            self.agent_history[name].append({"ts": now, "score": cur.score, "selected": cur.selected, "blocked": cur.blocked, "latency_ms": cur.latency_ms})

    def _agent_quality_from_assets(self) -> float:
        if not self.assets:
            return 0.0
        fresh = sum(1 for a in self.assets.values() if time() - a.last_update < 30)
        return fresh / max(1, len(self.assets))

    @staticmethod
    def _avg_setup_score(rows: list[dict[str, Any]]) -> float:
        if not rows:
            return 0.0
        return sum(float(x.get("score", 0.0) or 0.0) for x in rows) / max(1, len(rows))

    @staticmethod
    def _approved_count(verdicts: dict[str, Any]) -> int:
        return sum(1 for v in verdicts.values() if bool((v or {}).get("approved")))

    @staticmethod
    def _blocked_count(verdicts: dict[str, Any]) -> int:
        return sum(1 for v in verdicts.values() if not bool((v or {}).get("approved")))

    def _risk_score(self, verdicts: dict[str, Any]) -> float:
        if not verdicts:
            return 0.0
        approved = self._approved_count(verdicts)
        return approved / max(1, len(verdicts))

    def _learning_score(self) -> float:
        trades = list(self.trades)
        if not trades:
            return 0.0
        wins = sum(1 for t in trades if t.pnl > 0)
        return wins / max(1, len(trades))

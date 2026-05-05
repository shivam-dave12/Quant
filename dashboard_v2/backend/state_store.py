from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from threading import RLock
from time import time
from typing import Any, Deque, Dict, List

MAX_POINTS = 900
MAX_EVENTS = 2000
MAX_ALERTS = 500
MAX_TRADES = 500


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
    spread_bps: float = 0.0
    spread_atr: float = 0.0
    atr: float = 0.0
    size_mult: float = 1.0
    open_positions: int = 0
    max_positions: int = 0
    last_reason: str = ""
    last_update: float = field(default_factory=time)
    data_status: str = "UNKNOWN"
    primary: str = ""
    secondary: str = ""
    leverage: str = ""
    margin: str = ""


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
    reason: str = ""


class DashboardState:
    def __init__(self) -> None:
        self._lock = RLock()
        self.started_at = time()
        self.last_heartbeat = 0.0
        self.bot_online = False
        self.mode = "unknown"
        self.source = "log-tail"
        self.assets: Dict[str, AssetState] = {}
        self.positions: Dict[str, PositionState] = {}
        self.alerts: Deque[AlertState] = deque(maxlen=MAX_ALERTS)
        self.trades: Deque[TradeState] = deque(maxlen=MAX_TRADES)
        self.events: Deque[dict[str, Any]] = deque(maxlen=MAX_EVENTS)
        self.price_history: Dict[str, Deque[dict[str, float]]] = defaultdict(lambda: deque(maxlen=MAX_POINTS))
        self.spread_history: Dict[str, Deque[dict[str, float]]] = defaultdict(lambda: deque(maxlen=MAX_POINTS))
        self.r_history: Dict[str, Deque[dict[str, float]]] = defaultdict(lambda: deque(maxlen=MAX_POINTS))
        self.pnl_history: Deque[dict[str, float]] = deque(maxlen=MAX_POINTS)
        self.total_realized = 0.0

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            now = time()
            if self.last_heartbeat and now - self.last_heartbeat < 20:
                self.bot_online = True
            total_upnl = sum(p.upnl for p in self.positions.values())
            self.pnl_history.append({"ts": now, "value": total_upnl + self.total_realized, "upnl": total_upnl, "realized": self.total_realized})
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
                },
                "assets": [asdict(x) for x in sorted(self.assets.values(), key=lambda a: a.asset)],
                "positions": [asdict(x) for x in sorted(self.positions.values(), key=lambda p: p.asset)],
                "alerts": [asdict(x) for x in list(self.alerts)],
                "trades": [asdict(x) for x in list(self.trades)],
                "events": list(self.events),
                "charts": {
                    "price": {k: list(v) for k, v in self.price_history.items()},
                    "spread": {k: list(v) for k, v in self.spread_history.items()},
                    "r": {k: list(v) for k, v in self.r_history.items()},
                    "pnl": list(self.pnl_history),
                },
                "ts": now,
            }

    def apply(self, event: dict[str, Any]) -> None:
        with self._lock:
            event = dict(event)
            event.setdefault("ts", time())
            event.setdefault("type", "event")
            etype = str(event.get("type", "event"))
            self.events.appendleft(event)
            if etype == "heartbeat":
                self.last_heartbeat = float(event.get("ts", time()))
                self.bot_online = True
                self.mode = str(event.get("mode", self.mode))
                self.source = str(event.get("source", self.source))
                return
            if etype in {"catalog_asset", "market_data", "scan", "direction", "spread", "candidate_deferred", "candidate_approved"}:
                self._update_asset(event)
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

    def _asset_key(self, event: dict[str, Any]) -> str:
        return str(event.get("asset") or event.get("symbol") or "UNKNOWN").upper()

    def _position_key(self, event: dict[str, Any]) -> str:
        return f"{str(event.get('asset','')).upper()}::{str(event.get('venue','')).upper()}::{str(event.get('symbol','')).upper()}"

    def _update_asset(self, event: dict[str, Any]) -> None:
        k = self._asset_key(event)
        cur = self.assets.get(k) or AssetState(asset=k)
        for field_name in ["symbol", "venue", "state", "phase", "direction", "last_reason", "data_status", "primary", "secondary", "leverage", "margin"]:
            val = event.get(field_name)
            if val not in (None, ""):
                setattr(cur, field_name, str(val))
        for field_name in ["price", "confidence", "spread_bps", "spread_atr", "atr", "size_mult"]:
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
        self.assets[k] = cur
        if cur.price:
            self.price_history[k].append({"ts": cur.last_update, "value": cur.price})
        if cur.spread_bps or cur.spread_atr:
            self.spread_history[k].append({"ts": cur.last_update, "bps": cur.spread_bps, "atr": cur.spread_atr})

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
            r=float(event.get("r", event.get("achieved_r", 0.0)) or 0.0), reason=str(event.get("reason", ""))))

    def _add_alert(self, event: dict[str, Any]) -> None:
        self.alerts.appendleft(AlertState(
            ts=float(event.get("ts", time())), severity=str(event.get("severity", "warning")),
            title=str(event.get("title", event.get("type", "alert"))), message=str(event.get("message", "")),
            asset=str(event.get("asset", "")).upper(), symbol=str(event.get("symbol", "")), venue=str(event.get("venue", "")).upper()))

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field, asdict
from threading import RLock
from time import time
from typing import Any, Deque, Dict, List, Optional


@dataclass
class SystemStatus:
    bot_online: bool = False
    last_heartbeat: float = 0.0
    mode: str = "paper"
    environment: str = "local"
    open_positions: int = 0
    max_positions: int = 0
    total_upnl: float = 0.0
    total_realized: float = 0.0
    notes: str = ""


@dataclass
class PositionCard:
    asset: str
    symbol: str
    venue: str
    side: str
    state: str = "ACTIVE"
    qty: float = 0.0
    entry: float = 0.0
    price: float = 0.0
    sl: float = 0.0
    tp: float = 0.0
    upnl: float = 0.0
    achieved_r: float = 0.0
    mfe_r: float = 0.0
    trailing: str = "OFF"
    opened_at: float = field(default_factory=time)
    updated_at: float = field(default_factory=time)
    notes: str = ""


@dataclass
class ScannerCard:
    asset: str
    symbol: str
    venue: str
    phase: str = "SCANNING"
    price: float = 0.0
    spread_bps: float = 0.0
    atr: float = 0.0
    posterior: float = 0.0
    setup_quality: float = 0.0
    last_reason: str = ""
    updated_at: float = field(default_factory=time)


@dataclass
class AlertItem:
    id: str
    ts: float
    severity: str
    title: str
    message: str
    asset: str = ""
    symbol: str = ""
    venue: str = ""


@dataclass
class TradeItem:
    id: str
    ts: float
    asset: str
    symbol: str
    venue: str
    side: str
    entry: float
    exit: float
    pnl: float
    achieved_r: float
    reason: str = ""


class DashboardState:
    def __init__(self) -> None:
        self._lock = RLock()
        self.system = SystemStatus()
        self.positions: Dict[str, PositionCard] = {}
        self.scanners: Dict[str, ScannerCard] = {}
        self.alerts: Deque[AlertItem] = deque(maxlen=250)
        self.trades: Deque[TradeItem] = deque(maxlen=250)
        self.events: Deque[dict[str, Any]] = deque(maxlen=800)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            self.system.open_positions = len(self.positions)
            self.system.total_upnl = sum(p.upnl for p in self.positions.values())
            return {
                "system": asdict(self.system),
                "positions": [asdict(x) for x in sorted(self.positions.values(), key=lambda p: (p.asset, p.symbol))],
                "scanners": [asdict(x) for x in sorted(self.scanners.values(), key=lambda s: (s.asset, s.symbol))],
                "alerts": [asdict(x) for x in list(self.alerts)],
                "trades": [asdict(x) for x in list(self.trades)],
                "events": list(self.events),
                "ts": time(),
            }

    def append_event(self, event: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            event = dict(event)
            event.setdefault("ts", time())
            etype = str(event.get("type", "event"))
            self.events.appendleft(event)

            if etype == "heartbeat":
                self.system.bot_online = True
                self.system.last_heartbeat = float(event.get("ts", time()))
                self.system.mode = str(event.get("mode", self.system.mode))
                self.system.environment = str(event.get("environment", self.system.environment))
                self.system.max_positions = int(event.get("max_positions", self.system.max_positions or 0))
                self.system.notes = str(event.get("notes", self.system.notes))
                self.system.total_realized = float(event.get("total_realized", self.system.total_realized))
            elif etype in {"scan_update", "candidate_deferred", "candidate_approved"}:
                key = self._scanner_key(event)
                self.scanners[key] = ScannerCard(
                    asset=str(event.get("asset", "")).upper(),
                    symbol=str(event.get("symbol", "")),
                    venue=str(event.get("venue", "")).upper(),
                    phase=str(event.get("phase", etype.upper())),
                    price=float(event.get("price", 0.0) or 0.0),
                    spread_bps=float(event.get("spread_bps", 0.0) or 0.0),
                    atr=float(event.get("atr", 0.0) or 0.0),
                    posterior=float(event.get("posterior", 0.0) or 0.0),
                    setup_quality=float(event.get("setup_quality", 0.0) or 0.0),
                    last_reason=str(event.get("reason", event.get("message", ""))),
                    updated_at=float(event.get("ts", time())),
                )
            elif etype in {"position_opened", "position_update"}:
                key = self._position_key(event)
                existing = self.positions.get(key)
                card = PositionCard(
                    asset=str(event.get("asset", existing.asset if existing else "")).upper(),
                    symbol=str(event.get("symbol", existing.symbol if existing else "")),
                    venue=str(event.get("venue", existing.venue if existing else "")).upper(),
                    side=str(event.get("side", existing.side if existing else "")).upper(),
                    state=str(event.get("state", existing.state if existing else "ACTIVE")),
                    qty=float(event.get("qty", existing.qty if existing else 0.0) or 0.0),
                    entry=float(event.get("entry", existing.entry if existing else 0.0) or 0.0),
                    price=float(event.get("price", existing.price if existing else 0.0) or 0.0),
                    sl=float(event.get("sl", existing.sl if existing else 0.0) or 0.0),
                    tp=float(event.get("tp", existing.tp if existing else 0.0) or 0.0),
                    upnl=float(event.get("upnl", existing.upnl if existing else 0.0) or 0.0),
                    achieved_r=float(event.get("achieved_r", existing.achieved_r if existing else 0.0) or 0.0),
                    mfe_r=float(event.get("mfe_r", existing.mfe_r if existing else 0.0) or 0.0),
                    trailing=str(event.get("trailing", existing.trailing if existing else "OFF")).upper(),
                    opened_at=float(event.get("opened_at", existing.opened_at if existing else event.get("ts", time()))),
                    updated_at=float(event.get("ts", time())),
                    notes=str(event.get("notes", existing.notes if existing else "")),
                )
                self.positions[key] = card
            elif etype == "position_closed":
                key = self._position_key(event)
                self.positions.pop(key, None)
                self.trades.appendleft(
                    TradeItem(
                        id=str(event.get("id", key)),
                        ts=float(event.get("ts", time())),
                        asset=str(event.get("asset", "")).upper(),
                        symbol=str(event.get("symbol", "")),
                        venue=str(event.get("venue", "")).upper(),
                        side=str(event.get("side", "")).upper(),
                        entry=float(event.get("entry", 0.0) or 0.0),
                        exit=float(event.get("exit", 0.0) or 0.0),
                        pnl=float(event.get("pnl", 0.0) or 0.0),
                        achieved_r=float(event.get("achieved_r", 0.0) or 0.0),
                        reason=str(event.get("reason", "")),
                    )
                )
                self.system.total_realized += float(event.get("pnl", 0.0) or 0.0)
            elif etype in {"alert", "warning", "protection_failure", "execution_failure", "data_warning"}:
                sev = str(event.get("severity", "critical" if etype == "protection_failure" else "warning"))
                self.alerts.appendleft(
                    AlertItem(
                        id=str(event.get("id", f"{etype}-{int(time()*1000)}")),
                        ts=float(event.get("ts", time())),
                        severity=sev,
                        title=str(event.get("title", etype.replace("_", " ").title())),
                        message=str(event.get("message", "")),
                        asset=str(event.get("asset", "")).upper(),
                        symbol=str(event.get("symbol", "")),
                        venue=str(event.get("venue", "")).upper(),
                    )
                )
            return event

    @staticmethod
    def _scanner_key(event: dict[str, Any]) -> str:
        return f"{str(event.get('asset','')).upper()}::{str(event.get('venue','')).upper()}::{event.get('symbol','')}"

    @staticmethod
    def _position_key(event: dict[str, Any]) -> str:
        return f"{str(event.get('asset','')).upper()}::{str(event.get('venue','')).upper()}::{event.get('symbol','')}"

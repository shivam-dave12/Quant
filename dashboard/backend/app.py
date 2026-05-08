from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, ConfigDict

from state_store import DashboardState

BASE_DIR = Path(__file__).resolve().parents[1]
FRONTEND_DIR = BASE_DIR / "frontend"
app = FastAPI(title="Institutional Portfolio Command Center", version="20.0.0")
state = DashboardState()

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

class EventPayload(BaseModel):
    model_config = ConfigDict(extra="allow")
    type: str = Field(default="event")
    ts: Optional[float] = None
    asset: Optional[str] = None
    symbol: Optional[str] = None
    venue: Optional[str] = None
    side: Optional[str] = None
    state: Optional[str] = None
    phase: Optional[str] = None
    direction: Optional[str] = None
    price: Optional[float] = None
    entry: Optional[float] = None
    sl: Optional[float] = None
    tp: Optional[float] = None
    qty: Optional[float] = None
    upnl: Optional[float] = None
    r: Optional[float] = None
    achieved_r: Optional[float] = None
    mfe_r: Optional[float] = None
    trailing: Optional[str] = None
    bracket: Optional[str] = None
    reason: Optional[str] = None
    message: Optional[str] = None
    severity: Optional[str] = None
    title: Optional[str] = None
    mode: Optional[str] = None
    source: Optional[str] = None
    spread_bps: Optional[float] = None
    spread_atr: Optional[float] = None
    atr: Optional[float] = None
    posterior: Optional[float] = None
    confidence: Optional[float] = None
    ev: Optional[float] = None
    llr: Optional[float] = None
    uncertainty: Optional[float] = None
    size_mult: Optional[float] = None
    risk_mult: Optional[float] = None
    margin_pct: Optional[float] = None
    open_positions: Optional[int] = None
    max_positions: Optional[int] = None
    primary: Optional[str] = None
    secondary: Optional[str] = None
    leverage: Optional[str] = None
    margin: Optional[str] = None
    policy: Optional[str] = None
    data_status: Optional[str] = None
    health: Optional[str] = None
    last_reason: Optional[str] = None
    last_decision: Optional[str] = None
    exit: Optional[float] = None
    pnl: Optional[float] = None
    rr: Optional[float] = None
    setup_quality: Optional[float] = None
    log_path: Optional[str] = None
    ingested_lines: Optional[int] = None
    parsed_events: Optional[int] = None

@app.get("/api/health")
def health() -> dict[str, Any]:
    snap = state.snapshot()
    return {"ok": True, "dashboard": "v20", "ts": snap["ts"], "bot_online": snap["system"]["bot_online"], "assets": snap["system"]["assets"], "events": snap["system"]["event_count"]}


@app.get("/api/diagnostics")
def diagnostics() -> dict[str, Any]:
    return state.diagnostics()

@app.get("/api/state")
def get_state(asset: Optional[str] = None) -> dict[str, Any]:
    return state.snapshot(asset=asset)

@app.get("/api/assets/{asset}")
def get_asset(asset: str) -> dict[str, Any]:
    return state.asset_detail(asset)

@app.post("/api/events")
def post_event(payload: EventPayload) -> dict[str, Any]:
    ev = payload.model_dump(exclude_none=True)
    state.apply(ev)
    return {"ok": True, "event": ev}

@app.post("/api/ingested-line")
def ingested_line() -> dict[str, Any]:
    state.note_line()
    return {"ok": True}

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    await ws.accept()
    try:
        await ws.send_json(state.snapshot())
        while True:
            msg = await ws.receive_json()
            asset = msg.get("asset") if isinstance(msg, dict) else None
            await ws.send_json(state.snapshot(asset=asset))
    except WebSocketDisconnect:
        return

app.mount("/assets", StaticFiles(directory=FRONTEND_DIR), name="assets")

@app.get("/")
def index() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "index.html")

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from state_store import DashboardState

BASE_DIR = Path(__file__).resolve().parents[1]
FRONTEND_DIR = BASE_DIR / "frontend"

app = FastAPI(title="Portfolio Command Center", version="1.0.0")
state = DashboardState()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class EventPayload(BaseModel):
    type: str = Field(default="event")
    asset: Optional[str] = None
    symbol: Optional[str] = None
    venue: Optional[str] = None
    side: Optional[str] = None
    state: Optional[str] = None
    phase: Optional[str] = None
    price: Optional[float] = None
    entry: Optional[float] = None
    sl: Optional[float] = None
    tp: Optional[float] = None
    qty: Optional[float] = None
    upnl: Optional[float] = None
    achieved_r: Optional[float] = None
    mfe_r: Optional[float] = None
    trailing: Optional[str] = None
    reason: Optional[str] = None
    message: Optional[str] = None
    severity: Optional[str] = None
    title: Optional[str] = None
    mode: Optional[str] = None
    environment: Optional[str] = None
    max_positions: Optional[int] = None
    total_realized: Optional[float] = None
    spread_bps: Optional[float] = None
    atr: Optional[float] = None
    posterior: Optional[float] = None
    setup_quality: Optional[float] = None
    notes: Optional[str] = None
    opened_at: Optional[float] = None
    exit: Optional[float] = None
    pnl: Optional[float] = None


@app.get("/api/health")
def health() -> dict[str, Any]:
    snap = state.snapshot()
    return {"ok": True, "ts": snap["ts"], "bot_online": snap["system"]["bot_online"]}


@app.get("/api/state")
def get_state() -> dict[str, Any]:
    return state.snapshot()


@app.post("/api/events")
def post_event(payload: EventPayload) -> dict[str, Any]:
    if hasattr(payload, "model_dump"):
        data = payload.model_dump(exclude_none=True)
    else:  # pydantic v1 compatibility
        data = payload.dict(exclude_none=True)
    event = state.append_event(data)
    return {"ok": True, "event": event}


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    await ws.accept()
    try:
        await ws.send_json(state.snapshot())
        while True:
            _ = await ws.receive_text()
            await ws.send_json(state.snapshot())
    except WebSocketDisconnect:
        return


app.mount("/assets", StaticFiles(directory=FRONTEND_DIR), name="assets")


@app.get("/")
def index() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "index.html")

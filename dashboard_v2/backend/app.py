from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

try:
    from .state_store import DashboardState
except ImportError:
    from state_store import DashboardState

ROOT = Path(__file__).resolve().parents[1]
FRONTEND = ROOT / "frontend"
state = DashboardState()
app = FastAPI(title="Institutional Multi-Asset Dashboard", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/api/health")
def health() -> dict[str, Any]:
    snap = state.snapshot()
    return {"ok": True, "bot_online": snap["system"]["bot_online"], "assets": snap["system"]["assets"], "open_positions": snap["system"]["open_positions"]}


@app.get("/api/state")
def get_state() -> dict[str, Any]:
    return state.snapshot()


@app.post("/api/events")
def post_event(payload: Dict[str, Any]) -> dict[str, Any]:
    state.apply(payload)
    return {"ok": True}


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket) -> None:
    await ws.accept()
    try:
        while True:
            await ws.send_json(state.snapshot())
            await ws.receive_text()
    except WebSocketDisconnect:
        return
    except Exception:
        return


app.mount("/assets", StaticFiles(directory=str(FRONTEND)), name="assets")


@app.get("/")
def index() -> FileResponse:
    return FileResponse(FRONTEND / "index.html")

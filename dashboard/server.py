"""Read-only HTTP dashboard backed by the active strategy state and research records.

The dashboard cannot mutate policy, submit orders or promote models. It exposes the
same decision, execution, attribution and protection evidence written by the single
active strategy authority.
"""
from __future__ import annotations

import asyncio
import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from orchestration.platform import InstitutionalPlatform


class DashboardServer:
    def __init__(self, platform: InstitutionalPlatform, *, host: str = "127.0.0.1", port: int = 8088) -> None:
        self.platform = platform
        self.host = host
        self.port = int(port)
        self.httpd = ThreadingHTTPServer((host, self.port), self._handler())
        self.port = int(self.httpd.server_port)
        self._serving = False

    def snapshot(self) -> dict[str, Any]:
        latest_decisions = self.platform.research.read("decisions")[-10:]
        latest_executions = self.platform.research.read("executions")[-10:]
        latest_attribution = self.platform.research.read("attribution")[-10:]
        return {
            "platform": self.platform.status(),
            "latest_decisions": latest_decisions,
            "latest_executions": latest_executions,
            "latest_attribution": latest_attribution,
        }

    def records(self, stream: str, limit: int = 100) -> list[dict[str, Any]]:
        if stream not in {"observations", "decisions", "executions", "labels", "predictions", "attribution"}:
            raise ValueError("UNSUPPORTED_DASHBOARD_STREAM")
        return self.platform.research.read(stream)[-max(1, min(int(limit), 1000)):]

    @staticmethod
    def html() -> str:
        return """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Institutional Multi-Desk Platform</title><style>
:root{--bg:#0b1220;--panel:#121e32;--border:#24354e;--text:#eef5ff;--muted:#9fb0c6;--ok:#48d18a;--bad:#ff647c;--accent:#6aa7ff}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:14px Inter,Arial,sans-serif}
header{padding:22px 26px;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;align-items:center}
h1{font-size:20px;margin:0}.sub{color:var(--muted);margin-top:6px}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(250px,1fr));gap:14px;padding:18px}
.card{background:var(--panel);border:1px solid var(--border);border-radius:16px;padding:16px;overflow:auto}.metric{font-size:28px;font-weight:700;margin-top:8px}
.ok{color:var(--ok)}.bad{color:var(--bad)}table{width:100%;border-collapse:collapse}th,td{padding:9px 7px;text-align:left;border-bottom:1px solid var(--border);font-size:12px}th{color:var(--muted)}
.wide{grid-column:1/-1}pre{white-space:pre-wrap;color:var(--muted);font-size:12px}button{background:var(--accent);border:0;border-radius:10px;color:#081020;padding:8px 12px;font-weight:600}
</style></head><body><header><div><h1>Institutional Multi-Desk Platform</h1><div class="sub">Protected-execution state, net-edge decisions and realised attribution only</div></div><button onclick="load()">Refresh</button></header>
<div class="grid"><section class="card"><div class="sub">Runtime</div><div id="running" class="metric"></div></section><section class="card"><div class="sub">Groww Live</div><div id="groww" class="metric"></div></section><section class="card"><div class="sub">Delta Live</div><div id="delta" class="metric"></div></section><section class="card"><div class="sub">Metals Live</div><div id="metals" class="metric"></div></section>
<section class="card wide"><h2>Latest Decisions</h2><table><thead><tr><th>Desk</th><th>Instrument</th><th>Direction</th><th>Decision</th><th>Net edge bps</th><th>Reasons</th></tr></thead><tbody id="decisions"></tbody></table></section>
<section class="card wide"><h2>Protected Executions</h2><pre id="executions"></pre></section><section class="card wide"><h2>P&amp;L Attribution</h2><pre id="attribution"></pre></section></div>
<script>const flag=(v)=>`<span class="${v?'ok':'bad'}">${v?'ENABLED':'DISABLED'}</span>`;async function load(){let d=await fetch('/api/snapshot').then(r=>r.json()),p=d.platform;
document.getElementById('running').innerHTML=flag(p.strategy_authority_ready);document.getElementById('groww').innerHTML=flag(p.live_flags.groww);document.getElementById('delta').innerHTML=flag(p.live_flags.delta);document.getElementById('metals').innerHTML=flag(p.live_flags.metals);
document.getElementById('decisions').innerHTML=d.latest_decisions.slice().reverse().map(x=>`<tr><td>${x.desk||''}</td><td>${x.instrument||''}</td><td>${x.direction||''}</td><td>${x.code||''}</td><td>${Number(x.net_edge_bps||0).toFixed(2)}</td><td>${(x.reasons||[]).join('; ')}</td></tr>`).join('');
document.getElementById('executions').textContent=JSON.stringify(d.latest_executions.slice().reverse(),null,2);document.getElementById('attribution').textContent=JSON.stringify(d.latest_attribution.slice().reverse(),null,2)}load();setInterval(load,5000);</script></body></html>"""

    def _handler(self):
        dashboard = self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, _format: str, *args: Any) -> None:
                return
            def _write(self, payload: Any, status: int = HTTPStatus.OK, content_type: str = "application/json") -> None:
                raw = payload.encode("utf-8") if isinstance(payload, str) else json.dumps(payload, default=str).encode("utf-8")
                self.send_response(status); self.send_header("Content-Type", f"{content_type}; charset=utf-8")
                self.send_header("Content-Length", str(len(raw))); self.send_header("Cache-Control", "no-store")
                self.end_headers(); self.wfile.write(raw)
            def do_GET(self) -> None:
                parsed = urlparse(self.path)
                if parsed.path == "/":
                    self._write(dashboard.html(), content_type="text/html"); return
                if parsed.path == "/health":
                    self._write({"status": "ok", "running": dashboard.platform.status()["strategy_authority_ready"]}); return
                if parsed.path == "/api/snapshot":
                    self._write(dashboard.snapshot()); return
                if parsed.path.startswith("/api/"):
                    stream = parsed.path.removeprefix("/api/")
                    try:
                        limit = int(parse_qs(parsed.query).get("limit", ["100"])[0])
                        self._write(dashboard.records(stream, limit)); return
                    except (ValueError, TypeError):
                        self._write({"error": "unsupported request"}, HTTPStatus.BAD_REQUEST); return
                self._write({"error": "not found"}, HTTPStatus.NOT_FOUND)
        return Handler

    def serve_forever(self) -> None:
        self._serving = True
        try:
            self.httpd.serve_forever()
        finally:
            self._serving = False

    async def run(self) -> None:
        await asyncio.to_thread(self.serve_forever)

    def shutdown(self) -> None:
        if self._serving:
            self.httpd.shutdown()
        self.httpd.server_close()

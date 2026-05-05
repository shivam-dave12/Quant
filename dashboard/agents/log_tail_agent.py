from __future__ import annotations

import argparse
import json
import re
import time
from pathlib import Path
from typing import Any, Optional

import requests

ANSI = re.compile(r"\x1b\[[0-9;]*m")
CTX = re.compile(r"\[(?P<asset>[A-Z0-9_\-]+)\|(?P<venue>[A-Z]+):(?P<symbol>[A-Z0-9_:\-]+)\]")
CATALOG = re.compile(r"✅\s+(?P<asset>[A-Z0-9_]+)\s+primary=(?P<primary>\w+)\s+(?P<venues>.+)$")
AGG = re.compile(r"MarketAggregator initialised \[(?P<asset>[^\]]+)\] \(primary=(?P<primary>[^ ]+) secondary=(?P<secondary>[^\)]+)\)")
STRAT = re.compile(r"\[(?P<asset>[^\|]+)\|(?P<venue>[^:]+):(?P<symbol>[^\]]+)\].*leverage=(?P<leverage>[^ ]+) \| (?P<margin>[^ ]+) margin")
ANALYSIS = re.compile(r"\[(?P<asset>[^\|]+)\|(?P<venue>[^:]+):(?P<symbol>[^\]]+)\]\s+ANALYSIS_TICK asset=(?P<a2>\S+) primary=(?P<primary>\S+) symbol=(?P<s2>\S+) state=(?P<state>\S+) price=(?P<price>[0-9.]+).*slots=(?P<open>\d+)/(?:\s*)?(?P<max>\d+) policy=(?P<policy>\S+) lev=(?P<lev>[^ ]+) margin=(?P<margin>[^ ]+) risk_mult=(?P<risk>[0-9.]+)")
SCAN = re.compile(r"\[(?P<asset>[^\|]+)\|(?P<venue>[^:]+):(?P<symbol>[^\]]+)\]\s+(?P<a2>\S+)\s+(?P<v2>\S+)\s+(?P<s2>\S+) \| price (?P<price>[0-9.]+) \| (?P<state>[A-Z_]+) \| open=(?P<open>\d+)/(?:\s*)?(?P<max>\d+)")
DIR = re.compile(r"\[(?P<asset>[^\|]+)\|(?P<venue>[^:]+):(?P<symbol>[^\]]+)\].*DIR_TELEMETRY: hunt=(?P<hunt>\w+) conf=(?P<conf>[0-9.]+).*raw=(?P<raw>[+\-0-9.]+)")
SPREAD = re.compile(r"\[(?P<asset>[^\|]+)\|(?P<venue>[^:]+):(?P<symbol>[^\]]+)\].*Spread cost impairment.*ratio=(?P<ratio>[0-9.]+).*spread=(?P<bps>[0-9.]+)bps.*ticks=(?P<ticks>[0-9.]+).*ATR=\$(?P<atr>[0-9.]+).*size_mult=(?P<size>[0-9.]+)")
POSTERIOR = re.compile(r"\[(?P<asset>[^\|]+)\|(?P<venue>[^:]+):(?P<symbol>[^\]]+)\].*POSTERIOR ACCEPTED: (?P<side>REVERSAL|CONTINUATION)?\s*(?P<trade_side>LONG|SHORT)?.*p=(?P<p>[0-9.]+).*EV=(?P<ev>[+\-0-9.]+).*LLR=(?P<llr>[0-9.]+).*U=(?P<u>[0-9.]+)")
THESIS = re.compile(r"\[(?P<asset>[^\|]+)\|(?P<venue>[^:]+):(?P<symbol>[^\]]+)\].*ENTRY THESIS.* (?P<side>LONG|SHORT) @ \$(?P<entry>[0-9,\.]+) \| SL=\$(?P<sl>[0-9,\.]+) TP=\$(?P<tp>[0-9,\.]+) R:R=(?P<rr>[0-9.]+)")
DEFERRED = re.compile(r"\[(?P<asset>[^\|]+)\|(?P<venue>[^:]+):(?P<symbol>[^\]]+)\].*CANDIDATE DEFERRED \[(?P<why>[^\]]+)\]: (?P<body>.*)")
SLANCHOR = re.compile(r"\[(?P<asset>[^\|]+)\|(?P<venue>[^:]+):(?P<symbol>[^\]]+)\].*RAW_SL_ANCHOR: anchor=\$(?P<anchor>[0-9,\.]+) sl=\$(?P<sl>[0-9,\.]+) quality=(?P<quality>[0-9.]+) buffer=(?P<buffer>[0-9.]+)ATR")
SLENV = re.compile(r"\[(?P<asset>[^\|]+)\|(?P<venue>[^:]+):(?P<symbol>[^\]]+)\].*SL envelope (?P<phase>[^:]+): (?P<body>.*)")
ENTER = re.compile(r"\[(?P<asset>[^\|]+)\|(?P<venue>[^:]+):(?P<symbol>[^\]]+)\].*ENTERING (?P<side>LONG|SHORT) @ \$(?P<entry>[0-9,\.]+).*SL=\$(?P<sl>[0-9,\.]+) TP=\$(?P<tp>[0-9,\.]+)")
ACTIVE = re.compile(r"\[(?P<asset>[^\|]+)\|(?P<venue>[^:]+):(?P<symbol>[^\]]+)\].*ACTIVE (?P<side>LONG|SHORT).*@ \$(?P<price>[0-9,\.]+).*SL=\$(?P<sl>[0-9,\.]+).*TP=\$(?P<tp>[0-9,\.]+)")
BRACKET_SL = re.compile(r"\[(?P<asset>[^\|]+)\|(?P<venue>[^:]+):(?P<symbol>[^\]]+)\].*Bracket SL order:.*@ \$(?P<sl>[0-9,\.]+)")
BRACKET_TP = re.compile(r"\[(?P<asset>[^\|]+)\|(?P<venue>[^:]+):(?P<symbol>[^\]]+)\].*Bracket TP order:.*@ \$(?P<tp>[0-9,\.]+)")
TRAIL = re.compile(r"\[(?P<asset>[^\|]+)\|(?P<venue>[^:]+):(?P<symbol>[^\]]+)\].*(?:InstitutionalTrail|TRAIL|SL replaced|SL edited).*?(?:SL|trigger)=?\$?(?P<sl>[0-9,\.]+)?", re.I)
EXIT = re.compile(r"\[(?P<asset>[^\|]+)\|(?P<venue>[^:]+):(?P<symbol>[^\]]+)\].*EXIT.*?(?P<side>LONG|SHORT)?.*PNL.*?\$?(?P<pnl>[\-+0-9\.]+)", re.I)
PRICE_ANY = re.compile(r"price[= ](?P<price>[0-9]+(?:\.[0-9]+)?)")


def fnum(x: Any, default: float = 0.0) -> float:
    try:
        return float(str(x).replace(',', ''))
    except Exception:
        return default


def clean(line: str) -> str:
    try:
        obj = json.loads(line)
        line = obj.get("log", line)
    except Exception:
        pass
    return ANSI.sub("", line).strip()


def base_ctx(m) -> dict[str, Any]:
    d = m.groupdict()
    return {"asset": str(d.get("asset", "")).upper(), "venue": str(d.get("venue", "")).upper(), "symbol": str(d.get("symbol", ""))}


def parse(line: str) -> Optional[dict[str, Any]]:
    s = clean(line)
    if not s:
        return None
    if m := CATALOG.search(s):
        venues = m.group('venues')
        symbol = venues.split(':', 1)[1].split(',')[0].strip() if ':' in venues else ''
        return {"type": "catalog_asset", "asset": m.group('asset'), "venue": m.group('primary').upper(), "symbol": symbol, "primary": m.group('primary'), "last_reason": venues, "health": "OK"}
    if m := AGG.search(s):
        return {"type": "market_data", "asset": m.group('asset').upper(), "primary": m.group('primary'), "secondary": m.group('secondary'), "data_status": "INITIALISED", "health": "OK"}
    if m := STRAT.search(s):
        ev = base_ctx(m); ev.update({"type": "market_data", "leverage": m.group('leverage'), "margin": m.group('margin'), "data_status": "STRATEGY_READY", "health": "OK"}); return ev
    if m := ANALYSIS.search(s):
        ev = base_ctx(m); ev.update({"type": "scan", "price": fnum(m.group('price')), "state": m.group('state'), "phase": m.group('state'), "open_positions": int(m.group('open')), "max_positions": int(m.group('max')), "policy": m.group('policy'), "leverage": m.group('lev'), "margin": m.group('margin'), "risk_mult": fnum(m.group('risk')), "health": "OK"}); return ev
    if m := SCAN.search(s):
        ev = base_ctx(m); ev.update({"type": "scan", "price": fnum(m.group('price')), "state": m.group('state'), "phase": m.group('state'), "open_positions": int(m.group('open')), "max_positions": int(m.group('max')), "health": "OK"}); return ev
    if m := DIR.search(s):
        ev = base_ctx(m); ev.update({"type": "direction", "direction": m.group('hunt'), "confidence": fnum(m.group('conf')), "phase": "DIRECTION", "last_decision": s, "health": "OK"}); return ev
    if m := SPREAD.search(s):
        ev = base_ctx(m); ev.update({"type": "spread", "spread_atr": fnum(m.group('ratio')), "spread_bps": fnum(m.group('bps')), "atr": fnum(m.group('atr')), "size_mult": fnum(m.group('size')), "phase": "COST_IMPAIRMENT", "last_reason": s, "health": "OK"}); return ev
    if m := POSTERIOR.search(s):
        ev = base_ctx(m); ev.update({"type": "posterior", "phase": "POSTERIOR_ACCEPTED", "side": m.group('trade_side') or '', "posterior": fnum(m.group('p')), "ev": fnum(m.group('ev')), "llr": fnum(m.group('llr')), "uncertainty": fnum(m.group('u')), "last_decision": s, "message": s, "health": "OK"}); return ev
    if m := THESIS.search(s):
        ev = base_ctx(m); ev.update({"type": "candidate_approved", "phase": "THESIS_PRICED", "side": m.group('side'), "entry": fnum(m.group('entry')), "sl": fnum(m.group('sl')), "tp": fnum(m.group('tp')), "rr": fnum(m.group('rr')), "last_decision": s, "message": s}); return ev
    if m := DEFERRED.search(s):
        ev = base_ctx(m); body = m.group('body'); ev.update({"type": "candidate_deferred", "phase": "DEFERRED", "last_reason": f"{m.group('why')}: {body}", "reason": body, "message": s}); return ev
    if m := SLANCHOR.search(s):
        ev = base_ctx(m); ev.update({"type": "sl_anchor", "phase": "SL_ANCHOR", "sl": fnum(m.group('sl')), "setup_quality": fnum(m.group('quality')), "last_reason": f"anchor ${m.group('anchor')} buffer {m.group('buffer')} ATR", "message": s}); return ev
    if m := SLENV.search(s):
        ev = base_ctx(m); ev.update({"type": "sl_envelope", "phase": "SL_ENVELOPE", "last_reason": m.group('body'), "message": s}); return ev
    if m := ENTER.search(s):
        ev = base_ctx(m); ev.update({"type": "position_opened", "side": m.group('side'), "entry": fnum(m.group('entry')), "sl": fnum(m.group('sl')), "tp": fnum(m.group('tp')), "state": "ENTERING", "bracket": "PENDING"}); return ev
    if m := ACTIVE.search(s):
        ev = base_ctx(m); ev.update({"type": "position_update", "side": m.group('side'), "price": fnum(m.group('price')), "sl": fnum(m.group('sl')), "tp": fnum(m.group('tp')), "state": "ACTIVE", "bracket": "VERIFIED"}); return ev
    if m := BRACKET_SL.search(s):
        ev = base_ctx(m); ev.update({"type": "bracket_update", "sl": fnum(m.group('sl')), "bracket": "SL_VERIFIED"}); return ev
    if m := BRACKET_TP.search(s):
        ev = base_ctx(m); ev.update({"type": "bracket_update", "tp": fnum(m.group('tp')), "bracket": "TP_VERIFIED"}); return ev
    if m := TRAIL.search(s):
        ev = base_ctx(m); ev.update({"type": "trail_update", "trailing": "ON", "sl": fnum(m.group('sl')) if m.group('sl') else None, "message": s}); return ev
    if m := EXIT.search(s):
        ev = base_ctx(m); ev.update({"type": "position_closed", "side": m.group('side') or '', "pnl": fnum(m.group('pnl')), "reason": s}); return ev
    if any(x in s for x in ["ERRO", "ERROR", "Traceback", "NameError", "TypeError", "AttributeError"]):
        m = CTX.search(s)
        ev = base_ctx(m) if m else {"asset":"","venue":"","symbol":""}
        ev.update({"type": "error", "severity": "critical", "title": "Runtime error", "message": s[:1600], "health": "BAD"}); return ev
    if "WARN" in s or "WARNING" in s:
        m = CTX.search(s)
        ev = base_ctx(m) if m else {"asset":"","venue":"","symbol":""}
        ev.update({"type": "warning", "severity": "warning", "title": "Warning", "message": s[:1600], "health": "WARN"}); return ev
    return None


class Poster:
    def __init__(self, url: str) -> None:
        self.url = url.rstrip('/')
    def post(self, ev: dict[str, Any]) -> None:
        ev.setdefault('source', 'log-tail')
        ev.setdefault('ts', time.time())
        try:
            requests.post(self.url + '/api/events', json=ev, timeout=1.5)
        except Exception:
            pass
    def note_line(self) -> None:
        try:
            requests.post(self.url + '/api/ingested-line', timeout=0.5)
        except Exception:
            pass


def run(path: Path, dashboard: str, from_start: bool) -> None:
    poster = Poster(dashboard)
    lines = 0
    parsed = 0
    last_hb = 0.0
    last_status = 0.0

    def send_status(reason: str = "running") -> None:
        poster.post({
            "type": "heartbeat",
            "mode": "live",
            "source": "log-tail-v23",
            "message": reason,
            "log_path": str(path),
            "ingested_lines": lines,
            "parsed_events": parsed,
        })
        poster.post({
            "type": "tail_status",
            "asset": "DASHBOARD",
            "venue": "LOCAL",
            "symbol": "LOGTAIL",
            "phase": "RUNNING",
            "message": f"{reason}; log={path}; lines={lines}; parsed={parsed}",
            "health": "OK",
        })

    if not path.exists():
        poster.post({
            "type": "error",
            "severity": "critical",
            "title": "Dashboard log-tail path missing",
            "message": f"Log path does not exist: {path}",
            "source": "log-tail-v23",
        })
        while True:
            poster.post({"type": "heartbeat", "mode": "error", "source": "log-tail-v23", "message": f"missing log path {path}"})
            time.sleep(5)

    send_status("starting")
    with path.open('r', encoding='utf-8', errors='ignore') as f:
        if not from_start:
            # Tail only new lines by default.  Installer uses --from-start so a
            # restarted dashboard is populated immediately.
            f.seek(0, 2)
        while True:
            line = f.readline()
            now = time.time()
            if not line:
                if now - last_hb > 2:
                    send_status("waiting for new log lines")
                    last_hb = now
                time.sleep(0.5)
                continue
            lines += 1
            poster.note_line()
            ev = parse(line)
            if ev:
                parsed += 1
                poster.post(ev)
            if now - last_status > 10:
                send_status("parsing")
                last_status = now


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description='Advanced sidecar log parser for portfolio dashboard')
    ap.add_argument('--log', required=True)
    ap.add_argument('--dashboard', default='http://127.0.0.1:8000')
    ap.add_argument('--from-start', action='store_true', help='Backfill current log from beginning before following')
    args = ap.parse_args()
    run(Path(args.log), args.dashboard, args.from_start)

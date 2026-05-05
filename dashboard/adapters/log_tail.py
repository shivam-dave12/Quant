from __future__ import annotations

import argparse
import json
import re
import time
from pathlib import Path

import requests


ASSET_RE = re.compile(r"\[(?P<asset>[A-Z0-9_\-]+)\|(?P<venue>[A-Z]+):(?P<symbol>[A-Z0-9_\-]+)\]")
NUM_RE = re.compile(r"\$([0-9][0-9,]*(?:\.\d+)?)")
SIDE_RE = re.compile(r"\b(LONG|SHORT)\b")
UPNL_RE = re.compile(r"UPNL\s*=\s*([\-+]?\d+(?:\.\d+)?)")
R_RE = re.compile(r"\b([\-+]?\d+(?:\.\d+)?)R\b")
QTY_RE = re.compile(r"qty=([\d.]+)")


class DashboardPoster:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    def send(self, payload: dict) -> None:
        try:
            requests.post(f"{self.base_url}/api/events", json=payload, timeout=2.0)
        except Exception:
            pass



def parse_asset_parts(line: str) -> dict[str, str]:
    m = ASSET_RE.search(line)
    if not m:
        return {"asset": "", "venue": "", "symbol": ""}
    return {k: v for k, v in m.groupdict().items()}


def parse_prices(line: str) -> list[float]:
    vals = []
    for raw in NUM_RE.findall(line):
        vals.append(float(raw.replace(",", "")))
    return vals


def classify(line: str) -> dict | None:
    parts = parse_asset_parts(line)
    if "ENTERING" in line and "SL=$" in line and "TP=$" in line:
        nums = parse_prices(line)
        side = SIDE_RE.search(line)
        return {
            "type": "position_opened",
            **parts,
            "side": side.group(1) if side else "",
            "entry": nums[0] if len(nums) > 0 else 0.0,
            "sl": nums[1] if len(nums) > 1 else 0.0,
            "tp": nums[2] if len(nums) > 2 else 0.0,
            "state": "ENTERING",
            "title": "Position entering",
        }
    if "ACTIVE" in line and "SL=$" in line and "TP=$" in line:
        nums = parse_prices(line)
        side = SIDE_RE.search(line)
        return {
            "type": "position_update",
            **parts,
            "side": side.group(1) if side else "",
            "price": nums[0] if len(nums) > 0 else 0.0,
            "sl": nums[1] if len(nums) > 1 else 0.0,
            "tp": nums[2] if len(nums) > 2 else 0.0,
            "state": "ACTIVE",
            "title": "Position active",
        }
    if "CANDIDATE DEFERRED" in line:
        return {
            "type": "candidate_deferred",
            **parts,
            "phase": "DEFERRED",
            "reason": line.split("CANDIDATE DEFERRED:", 1)[-1].strip(),
            "message": line,
        }
    if "ENTRY CANDIDATE APPROVED" in line:
        return {
            "type": "candidate_approved",
            **parts,
            "phase": "APPROVED",
            "message": line,
        }
    if "SCAN" in line or "SCANNING" in line:
        return {
            "type": "scan_update",
            **parts,
            "phase": "SCANNING",
            "message": line,
        }
    if "PROTECTION FAILURE" in line or "ENTRY ORDER FAILED" in line or "BRACKET ENTRY REFUSED" in line:
        return {
            "type": "protection_failure",
            **parts,
            "severity": "critical",
            "title": "Protection failure",
            "message": line,
        }
    if "Trailing SL" in line or "TRAIL UPDATE" in line:
        side = SIDE_RE.search(line)
        prices = parse_prices(line)
        trailing = "ON"
        return {
            "type": "position_update",
            **parts,
            "side": side.group(1) if side else "",
            "sl": prices[0] if prices else 0.0,
            "trailing": trailing,
            "state": "ACTIVE",
            "message": line,
        }
    if "EXIT" in line and "PNL" in line:
        prices = parse_prices(line)
        side = SIDE_RE.search(line)
        r_match = R_RE.search(line)
        return {
            "type": "position_closed",
            **parts,
            "side": side.group(1) if side else "",
            "entry": prices[0] if len(prices) > 0 else 0.0,
            "exit": prices[1] if len(prices) > 1 else 0.0,
            "pnl": prices[2] if len(prices) > 2 else 0.0,
            "achieved_r": float(r_match.group(1)) if r_match else 0.0,
            "reason": line,
        }
    if "WARN" in line or "ERRO" in line or "ERROR" in line:
        return {
            "type": "alert",
            **parts,
            "severity": "warning" if "WARN" in line else "critical",
            "title": "Log alert",
            "message": line,
        }
    return None


def follow(log_path: Path, base_url: str) -> None:
    poster = DashboardPoster(base_url)
    poster.send({"type": "heartbeat", "environment": "local", "mode": "live", "notes": f"tailing {log_path.name}"})
    with log_path.open("r", encoding="utf-8", errors="ignore") as f:
        f.seek(0, 2)
        while True:
            pos = f.tell()
            line = f.readline()
            if not line:
                time.sleep(0.5)
                f.seek(pos)
                continue
            try:
                obj = json.loads(line)
                raw = obj.get("log", "")
            except Exception:
                raw = line
            event = classify(raw)
            if event:
                poster.send(event)
            poster.send({"type": "heartbeat", "environment": "local", "mode": "live"})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tail a bot log and feed the local dashboard")
    parser.add_argument("--log", required=True, help="Path to JSON log file")
    parser.add_argument("--dashboard", default="http://127.0.0.1:8000", help="Dashboard base URL")
    args = parser.parse_args()
    follow(Path(args.log), args.dashboard)

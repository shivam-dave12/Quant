
from __future__ import annotations

import json
import logging
import os
import queue
import threading
import time
from typing import Any, Dict, Optional
from urllib import request

logger = logging.getLogger(__name__)


def _truthy(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on", "enabled"}


class DashboardEmitter:
    """Non-blocking structured telemetry bridge to the dashboard.

    This class is deliberately not part of alpha/risk/execution decisions.  It
    uses a bounded queue and a background worker.  If the dashboard is offline,
    events are dropped instead of delaying the trading loop.
    """

    _shared: Optional["DashboardEmitter"] = None
    _shared_lock = threading.Lock()

    def __init__(self, *, enabled: bool = False, url: str = "http://127.0.0.1:8000", timeout: float = 0.25, max_queue: int = 2000) -> None:
        self.enabled = bool(enabled)
        self.url = str(url or "http://127.0.0.1:8000").rstrip("/")
        self.timeout = float(timeout or 0.25)
        self.q: queue.Queue[Dict[str, Any]] = queue.Queue(maxsize=max(10, int(max_queue or 2000)))
        self._worker: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self.sent = 0
        self.dropped = 0
        if self.enabled:
            self.start()

    @classmethod
    def from_config(cls) -> "DashboardEmitter":
        """Return one shared emitter per process.

        MultiAssetQuantBot creates one QuantStrategy per asset.  Creating a new
        HTTP worker per strategy is unnecessary and can become operational noise.
        A single bounded queue preserves the non-blocking guarantee while avoiding
        dozens of dashboard worker threads.
        """
        with cls._shared_lock:
            if cls._shared is not None:
                return cls._shared
            try:
                import config
                enabled = _truthy(os.getenv("DASHBOARD_ENABLED", getattr(config, "DASHBOARD_ENABLED", True)), True)
                url = os.getenv("DASHBOARD_URL", str(getattr(config, "DASHBOARD_URL", "http://127.0.0.1:8000")))
                timeout = float(os.getenv("DASHBOARD_TIMEOUT_SEC", getattr(config, "DASHBOARD_TIMEOUT_SEC", 0.25)))
                maxq = int(os.getenv("DASHBOARD_QUEUE_MAX", getattr(config, "DASHBOARD_QUEUE_MAX", 2000)))
                cls._shared = cls(enabled=enabled, url=url, timeout=timeout, max_queue=maxq)
            except Exception:
                cls._shared = cls(enabled=_truthy(os.getenv("DASHBOARD_ENABLED"), False), url=os.getenv("DASHBOARD_URL", "http://127.0.0.1:8000"))
            return cls._shared

    def start(self) -> None:
        if not self.enabled:
            return
        if self._worker and self._worker.is_alive():
            return
        self._worker = threading.Thread(target=self._run, name="dashboard-emitter", daemon=True)
        self._worker.start()

    def stop(self) -> None:
        self._stop.set()

    def emit(self, event: Dict[str, Any]) -> bool:
        if not self.enabled:
            return False
        ev = dict(event or {})
        ev.setdefault("type", "event")
        ev.setdefault("ts", time.time())
        ev.setdefault("source", "direct")
        try:
            self.q.put_nowait(ev)
            return True
        except queue.Full:
            self.dropped += 1
            return False

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                event = self.q.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                data = json.dumps(event).encode("utf-8")
                req = request.Request(
                    f"{self.url}/api/events",
                    data=data,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with request.urlopen(req, timeout=self.timeout) as resp:
                    resp.read(64)
                self.sent += 1
            except Exception as exc:
                # Do not log every network miss.  Dashboard must never influence
                # trading runtime.  Emit one debug only for local diagnosis.
                logger.debug("dashboard event dropped: %s", exc)
                self.dropped += 1
            finally:
                try:
                    self.q.task_done()
                except Exception:
                    pass


def instrument_fields(inst: Any) -> Dict[str, Any]:
    if inst is None:
        return {}
    try:
        primary = getattr(inst, "primary_exchange", None)
        primary_name = getattr(primary, "value", str(primary or "")).upper()
        return {
            "asset": str(getattr(inst, "asset_id", "")).upper(),
            "symbol": str(getattr(inst, "display_symbol", getattr(inst, "execution_symbol", ""))),
            "venue": primary_name,
        }
    except Exception:
        return {}


def policy_fields(inst: Any) -> Dict[str, Any]:
    try:
        from core.market_policy import active_policy
        pol = active_policy(inst)
        return {
            "policy": str(getattr(pol, "asset_class", "")),
            "leverage": str(getattr(pol, "leverage", "")),
            "margin_pct": float(getattr(pol, "margin_pct", 0.0) or 0.0),
            "risk_mult": float(getattr(pol, "risk_multiplier", 0.0) or 0.0),
        }
    except Exception:
        return {}


def position_fields(ctx: Any) -> Dict[str, Any]:
    try:
        inst = getattr(ctx, "instrument", None)
        strat = getattr(ctx, "strategy", None)
        pos = strat.get_position() if strat is not None else None
        data = {**instrument_fields(inst), **policy_fields(inst)}
        try:
            data["price"] = float(ctx.data_manager.get_last_price() or 0.0)
        except Exception:
            data["price"] = 0.0
        data["state"] = str(getattr(ctx, "phase_name", "UNKNOWN"))
        if pos is not None:
            side = str(getattr(pos, "side", "")).upper()
            entry = float(getattr(pos, "entry_price", 0.0) or 0.0)
            qty = float(getattr(pos, "quantity", 0.0) or 0.0)
            px = float(data.get("price", 0.0) or 0.0)
            move = (px - entry) if side == "LONG" else (entry - px)
            init_sl = float(getattr(pos, "initial_sl_dist", 0.0) or 0.0) or abs(entry - float(getattr(pos, "sl_price", 0.0) or 0.0))
            r = move / init_sl if init_sl > 1e-10 else 0.0
            data.update({
                "type": "position_update",
                "side": side,
                "qty": qty,
                "entry": entry,
                "sl": float(getattr(pos, "sl_price", 0.0) or 0.0),
                "tp": float(getattr(pos, "tp_price", 0.0) or 0.0),
                "upnl": move * qty,
                "r": r,
                "mfe_r": float(getattr(pos, "peak_profit", 0.0) or 0.0) / init_sl if init_sl > 1e-10 else 0.0,
                "trailing": "ON" if bool(getattr(pos, "trail_active", False)) else "OFF",
                "bracket": "VERIFIED" if getattr(pos, "sl_order_id", "") and getattr(pos, "tp_order_id", "") else "UNKNOWN",
            })
        else:
            data.update({"type": "scan", "phase": "SCANNING" if getattr(ctx, "ready", False) else "WARMUP"})
        return data
    except Exception:
        return {"type": "event", "message": "position_fields failed"}

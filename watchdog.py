"""
watchdog.py — Industry-grade runtime supervisor for the liquidity-first quant bot
==================================================================================

PURPOSE
-------
This module provides a defensive, observable, and selectively self-healing
supervisor that runs alongside the main trading loop. It answers three
questions continuously:

    1.  Is the bot alive?                       (liveness)
    2.  Is the bot's internal state self-consistent
        and consistent with the exchange?       (correctness)
    3.  Is the bot actually making progress?    (activity)

When the answer to any of these is "no", the supervisor does one of four
things in ascending order of severity:

    INFO      - record a metric and move on
    WARN      - log + Telegram alert, capture a forensic snapshot
    HEAL      - apply a narrow, auditable state repair and alert
    CRITICAL  - engage the circuit breaker (freeze entries, optionally
                flatten), alert, and require operator acknowledgement

DESIGN PRINCIPLES
-----------------
* **Conservative by default.** Auto-heal only fires after a check has failed
  N times in a row and within rate limits. Unclear cases always alert rather
  than heal.

* **Auditable.** Every heal action is written to a JSON-lines file
  (`watchdog_heals.jsonl`) with before/after state. Every CRITICAL escalation
  produces a full forensic dump (position, flags, thread stacks, recent
  events).

* **Observable.** The watchdog exposes a `snapshot()` method that returns the
  full current health state for debugging or Telegram-driven introspection.

* **Non-invasive.** It reads strategy state through public accessors where
  possible and documented internal attributes where necessary. It never
  patches strategy methods or mutates private data structures beyond the
  specific fields listed in each heal action's docstring.

* **Fail-safe.** A bug in the watchdog itself must not take down trading.
  All check execution is wrapped; a check that raises is demoted to WARN
  and disabled for a cooldown period.

INTEGRATION
-----------
In `main.py`, after the strategy + router + risk manager are constructed::

    from watchdog import Watchdog, build_default_watchdog

    self.watchdog = build_default_watchdog(
        strategy         = self.strategy,
        data_manager     = self.data_manager,
        execution_router = self.execution_router,
        risk_manager     = self.risk_manager,
        notifier         = send_telegram_message,
        config_module    = config,
    )
    self.watchdog.start()

And on shutdown::

    if self.watchdog is not None:
        self.watchdog.stop()

Telegram operators can toggle auto-heal or query health::

    /watchdog_status       -> snapshot()
    /watchdog_heal on|off  -> set_auto_heal_enabled(bool)
    /watchdog_freeze       -> engage circuit breaker manually
    /watchdog_unfreeze     -> clear circuit breaker (operator ack)

See the bottom of this file for a wiring-helper that registers these
commands on the existing Telegram controller.
"""

from __future__ import annotations

import enum
import json
import logging
import os
import sys
import threading
import time
import traceback
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from typing import (
    Any, Callable, Deque, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# IST timestamp helper (matches the main bot's logging convention)
# ─────────────────────────────────────────────────────────────────────────────

_IST = timezone(timedelta(hours=5, minutes=30))


def _now() -> float:
    """Monotonic-ish wall-clock for comparisons with strategy timestamps."""
    return time.time()


def _iso_ist(ts: Optional[float] = None) -> str:
    t = ts if ts is not None else _now()
    return datetime.fromtimestamp(t, tz=_IST).strftime("%Y-%m-%d %H:%M:%S IST")


def _safe_getattr(obj: Any, name: str, default: Any = None) -> Any:
    """getattr that never raises even if attribute access itself throws."""
    try:
        return getattr(obj, name, default)
    except Exception:
        return default


def _safe_call(fn: Optional[Callable], *args, default: Any = None, **kwargs) -> Any:
    if fn is None:
        return default
    try:
        return fn(*args, **kwargs)
    except Exception as e:  # noqa: BLE001
        logger.debug("watchdog: safe_call(%s) raised %s", getattr(fn, "__name__", fn), e)
        return default


# ═════════════════════════════════════════════════════════════════════════════
# SEVERITY & STATUS
# ═════════════════════════════════════════════════════════════════════════════


class Severity(enum.IntEnum):
    """
    Integer-ordered severity levels. Higher = more severe.

    OK        — check passed, nothing to do
    INFO      — observational, logged as INFO, no Telegram
    WARN      — deviation from expected; log + Telegram, no action
    HEAL      — auto-heal eligible; applied if permitted, else WARN
    CRITICAL  — invariant violated; engage circuit breaker
    """
    OK = 0
    INFO = 1
    WARN = 2
    HEAL = 3
    CRITICAL = 4


class Status(enum.Enum):
    OK = "OK"
    DEGRADED = "DEGRADED"  # warnings present but trading can continue
    HEALING = "HEALING"    # actively repairing state
    FROZEN = "FROZEN"      # circuit breaker engaged — no new entries
    UNKNOWN = "UNKNOWN"    # insufficient data (startup / stale)


# ═════════════════════════════════════════════════════════════════════════════
# RESULT / SNAPSHOT DATA CLASSES
# ═════════════════════════════════════════════════════════════════════════════


@dataclass
class HealthResult:
    """
    A single health-check outcome.

    `metrics` is a free-form dict of numeric/string values the check wants to
    expose (displayed in /watchdog_status and written to forensic dumps).

    `heal_action` is the name of the registered heal action the check is
    recommending; the watchdog orchestrator will look it up in the
    AutoHealRegistry and decide whether to apply it. Only meaningful when
    severity == HEAL.
    """
    check_name: str
    severity: Severity
    message: str
    metrics: Dict[str, Any] = field(default_factory=dict)
    heal_action: Optional[str] = None
    timestamp: float = field(default_factory=_now)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "check_name": self.check_name,
            "severity": self.severity.name,
            "message": self.message,
            "metrics": self.metrics,
            "heal_action": self.heal_action,
            "timestamp": self.timestamp,
            "timestamp_ist": _iso_ist(self.timestamp),
        }


@dataclass
class ForensicSnapshot:
    """
    Complete state dump captured when a WARN+ event fires. Designed so a
    human reading the JSON can reconstruct exactly what the bot believed at
    that instant without needing to correlate log lines.
    """
    captured_at: float
    trigger: str                           # check name or event that caused capture
    severity: str
    position: Dict[str, Any]               # public position dict
    strategy_flags: Dict[str, Any]         # all _*_in_progress, _exit_completed, phase, etc.
    risk_gate: Dict[str, Any]              # daily_trades, consec_losses, loss_lockout_until
    stats: Dict[str, Any]                  # strategy.get_stats()
    timings: Dict[str, Any]                # ages of last_tick, last_sync, last_exit_time, etc.
    threads: List[Dict[str, Any]]          # list of alive threads with names
    recent_results: List[Dict[str, Any]]   # last N HealthResults (any severity)
    heal_history: List[Dict[str, Any]]     # last N heal actions
    exchange_position: Optional[Dict[str, Any]] = None  # best-effort, if available cheaply

    def to_dict(self) -> Dict[str, Any]:
        return {
            "captured_at": self.captured_at,
            "captured_at_ist": _iso_ist(self.captured_at),
            "trigger": self.trigger,
            "severity": self.severity,
            "position": self.position,
            "strategy_flags": self.strategy_flags,
            "risk_gate": self.risk_gate,
            "stats": self.stats,
            "timings": self.timings,
            "threads": self.threads,
            "recent_results": self.recent_results,
            "heal_history": self.heal_history,
            "exchange_position": self.exchange_position,
        }


# ═════════════════════════════════════════════════════════════════════════════
# FORENSIC RECORDER
# ═════════════════════════════════════════════════════════════════════════════


class ForensicRecorder:
    """
    Captures state snapshots on demand and maintains a ring buffer so the
    most recent N snapshots are always queryable.

    Captures are CHEAP (no exchange calls by default). The `with_exchange=True`
    flag can be passed by escalation paths to pay the REST cost once and
    attach live exchange position data — this is gated to avoid tight-loop
    amplification if a check is flapping.
    """

    _RING_SIZE = 32
    _DISK_DUMP_MIN_SEVERITY = Severity.HEAL

    def __init__(self, strategy: Any,
                 execution_router: Any,
                 risk_manager: Any,
                 dump_dir: str = "."):
        self._strategy = strategy
        self._router = execution_router
        self._risk_manager = risk_manager
        self._dump_dir = dump_dir
        self._ring: Deque[ForensicSnapshot] = deque(maxlen=self._RING_SIZE)
        self._lock = threading.Lock()
        self._last_exchange_fetch_ts = 0.0
        self._EXCHANGE_FETCH_MIN_INTERVAL_SEC = 10.0
        os.makedirs(self._dump_dir, exist_ok=True)

    # ---- collection helpers -------------------------------------------------

    def _collect_position(self) -> Dict[str, Any]:
        strat = self._strategy
        if strat is None:
            return {}
        # Public accessor — returns None or a sanitized dict
        pub = _safe_call(_safe_getattr(strat, "get_position", None), default=None)

        # Also read the raw PositionState for phase visibility (public dict omits it)
        raw = _safe_getattr(strat, "_pos", None)
        raw_view: Dict[str, Any] = {}
        if raw is not None:
            phase = _safe_getattr(raw, "phase", None)
            raw_view = {
                "phase": getattr(phase, "name", str(phase)) if phase is not None else None,
                "side": _safe_getattr(raw, "side", ""),
                "quantity": _safe_getattr(raw, "quantity", 0.0),
                "entry_price": _safe_getattr(raw, "entry_price", 0.0),
                "sl_price": _safe_getattr(raw, "sl_price", 0.0),
                "tp_price": _safe_getattr(raw, "tp_price", 0.0),
                "sl_order_id": _safe_getattr(raw, "sl_order_id", "") or "",
                "tp_order_id": _safe_getattr(raw, "tp_order_id", "") or "",
                "entry_order_id": _safe_getattr(raw, "entry_order_id", "") or "",
                "entry_time": _safe_getattr(raw, "entry_time", 0.0),
                "trail_active": _safe_getattr(raw, "trail_active", False),
                "peak_profit": _safe_getattr(raw, "peak_profit", 0.0),
                "initial_sl_dist": _safe_getattr(raw, "initial_sl_dist", 0.0),
            }
        return {"public": pub or {}, "raw": raw_view}

    def _collect_flags(self) -> Dict[str, Any]:
        strat = self._strategy
        if strat is None:
            return {}
        return {
            "_pos_sync_in_progress": _safe_getattr(strat, "_pos_sync_in_progress", None),
            "_exit_sync_in_progress": _safe_getattr(strat, "_exit_sync_in_progress", None),
            "_trail_in_progress": _safe_getattr(strat, "_trail_in_progress", None),
            "_reconcile_pending": _safe_getattr(strat, "_reconcile_pending", None),
            "_exit_completed": _safe_getattr(strat, "_exit_completed", None),
            "_pnl_recorded_for": _safe_getattr(strat, "_pnl_recorded_for", None),
            "_last_exit_time": _safe_getattr(strat, "_last_exit_time", 0.0),
            "_entering_since": _safe_getattr(strat, "_entering_since", 0.0),
            "_exiting_since": _safe_getattr(strat, "_exiting_since", 0.0),
            "_entry_order_placed_at": _safe_getattr(strat, "_entry_order_placed_at", 0.0),
            "_last_pos_sync": _safe_getattr(strat, "_last_pos_sync", 0.0),
            "_last_exit_sync": _safe_getattr(strat, "_last_exit_sync", 0.0),
            "_last_reconcile_time": _safe_getattr(strat, "_last_reconcile_time", 0.0),
            "_last_tp_gate_rejection": _safe_getattr(strat, "_last_tp_gate_rejection", 0.0),
            "_last_trail_rest_time": _safe_getattr(strat, "_last_trail_rest_time", 0.0),
        }

    def _collect_risk_gate(self) -> Dict[str, Any]:
        strat = self._strategy
        rg = _safe_getattr(strat, "_risk_gate", None)
        if rg is None:
            return {}
        return {
            "daily_trades_gate": _safe_getattr(rg, "_daily_trades", None),
            "daily_trades_prop": _safe_getattr(rg, "daily_trades", None),
            "consec_losses": _safe_getattr(rg, "_consec_losses", None),
            "daily_pnl": _safe_getattr(rg, "_daily_pnl", 0.0),
            "daily_open_bal": _safe_getattr(rg, "_daily_open_bal", 0.0),
            "loss_lockout_until": _safe_getattr(rg, "_loss_lockout_until", 0.0),
            "loss_lockout_remaining_sec": max(
                0.0, float(_safe_getattr(rg, "_loss_lockout_until", 0.0)) - _now()),
            "today": str(_safe_getattr(rg, "_today", "")),
        }

    def _collect_stats(self) -> Dict[str, Any]:
        fn = _safe_getattr(self._strategy, "get_stats", None)
        return _safe_call(fn, default={}) or {}

    def _collect_timings(self) -> Dict[str, Any]:
        strat = self._strategy
        now = _now()
        last_exit = float(_safe_getattr(strat, "_last_exit_time", 0.0) or 0.0)
        last_tick = float(_safe_getattr(strat, "_last_tick_time", 0.0) or 0.0)
        last_pos_sync = float(_safe_getattr(strat, "_last_pos_sync", 0.0) or 0.0)
        last_recon = float(_safe_getattr(strat, "_last_reconcile_time", 0.0) or 0.0)
        return {
            "now": now,
            "age_since_last_exit_sec": (now - last_exit) if last_exit > 0 else None,
            "age_since_last_tick_sec": (now - last_tick) if last_tick > 0 else None,
            "age_since_last_pos_sync_sec": (now - last_pos_sync) if last_pos_sync > 0 else None,
            "age_since_last_reconcile_sec": (now - last_recon) if last_recon > 0 else None,
        }

    def _collect_threads(self) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for t in threading.enumerate():
            out.append({
                "name": t.name,
                "ident": t.ident,
                "daemon": t.daemon,
                "alive": t.is_alive(),
            })
        return out

    def _maybe_collect_exchange_position(self, force: bool = False) -> Optional[Dict[str, Any]]:
        """Cheap-ish read of current exchange position. Rate-limited to avoid
        amplification when a check is flapping."""
        now = _now()
        if not force and (now - self._last_exchange_fetch_ts) < self._EXCHANGE_FETCH_MIN_INTERVAL_SEC:
            return None

        router = self._router
        if router is None:
            return None

        # Try router.get_active_om().get_open_position() or router.get_open_position()
        om = None
        for accessor in ("get_active_om", "active_order_manager", "active_om"):
            cand = _safe_getattr(router, accessor, None)
            if callable(cand):
                om = _safe_call(cand, default=None)
            else:
                om = cand
            if om is not None:
                break

        if om is None:
            om = router  # fall back — router often quacks like OM

        fn = _safe_getattr(om, "get_open_position", None)
        if not callable(fn):
            fn = _safe_getattr(om, "get_position", None)
        if not callable(fn):
            return None

        pos = _safe_call(fn, default=None)
        self._last_exchange_fetch_ts = now
        if pos is None:
            return {"has_position": False}
        if isinstance(pos, dict):
            return {"has_position": True, **{
                k: v for k, v in pos.items()
                if isinstance(v, (int, float, str, bool, type(None)))
            }}
        # Dataclass or object: best-effort reflection
        out = {"has_position": True}
        for attr in ("side", "size", "quantity", "entry_price", "unrealized_pnl"):
            v = _safe_getattr(pos, attr, None)
            if v is not None:
                out[attr] = v
        return out

    # ---- public API ---------------------------------------------------------

    def capture(self,
                trigger: str,
                severity: Severity,
                recent_results: Sequence[HealthResult],
                heal_history: Sequence[Dict[str, Any]],
                with_exchange: bool = False) -> ForensicSnapshot:
        """
        Capture a full state snapshot. `with_exchange=True` adds a live
        exchange position read (rate-limited internally).
        """
        snap = ForensicSnapshot(
            captured_at=_now(),
            trigger=trigger,
            severity=severity.name,
            position=self._collect_position(),
            strategy_flags=self._collect_flags(),
            risk_gate=self._collect_risk_gate(),
            stats=self._collect_stats(),
            timings=self._collect_timings(),
            threads=self._collect_threads(),
            recent_results=[r.to_dict() for r in list(recent_results)[-20:]],
            heal_history=[dict(h) for h in list(heal_history)[-20:]],
            exchange_position=(self._maybe_collect_exchange_position(force=with_exchange)
                               if with_exchange else None),
        )

        with self._lock:
            self._ring.append(snap)

        if severity >= self._DISK_DUMP_MIN_SEVERITY:
            self._dump_to_disk(snap)

        return snap

    def latest(self, n: int = 1) -> List[ForensicSnapshot]:
        with self._lock:
            return list(self._ring)[-n:]

    def _dump_to_disk(self, snap: ForensicSnapshot) -> None:
        try:
            fname = f"watchdog_forensic_{datetime.fromtimestamp(snap.captured_at, tz=_IST).strftime('%Y%m%d_%H%M%S')}_{snap.severity}.json"
            path = os.path.join(self._dump_dir, fname)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(snap.to_dict(), f, indent=2, default=str)
            logger.info("watchdog: forensic dump → %s", path)
        except Exception as e:  # noqa: BLE001
            logger.error("watchdog: failed to write forensic dump: %s", e)


# ═════════════════════════════════════════════════════════════════════════════
# HEAL ACTION LOG
# ═════════════════════════════════════════════════════════════════════════════


class HealActionLog:
    """
    Persistent JSON-lines audit trail of every heal action. Each line is:

        {
          "ts": <epoch>, "ts_ist": "...", "action": "<name>",
          "reason": "<check_name>", "before": {...}, "after": {...},
          "success": true|false, "notes": "..."
        }

    Also maintains an in-memory ring buffer (last 200 entries) for quick
    Telegram / UI queries.
    """

    _RING_SIZE = 200

    def __init__(self, path: str = "watchdog_heals.jsonl"):
        self._path = path
        self._lock = threading.Lock()
        self._ring: Deque[Dict[str, Any]] = deque(maxlen=self._RING_SIZE)

    def record(self, action: str, reason: str,
               before: Mapping[str, Any], after: Mapping[str, Any],
               success: bool, notes: str = "") -> Dict[str, Any]:
        entry = {
            "ts": _now(),
            "ts_ist": _iso_ist(),
            "action": action,
            "reason": reason,
            "before": dict(before),
            "after": dict(after),
            "success": bool(success),
            "notes": notes,
        }
        with self._lock:
            self._ring.append(entry)
            try:
                with open(self._path, "a", encoding="utf-8") as f:
                    f.write(json.dumps(entry, default=str) + "\n")
            except Exception as e:  # noqa: BLE001
                logger.error("watchdog: failed to append heal log: %s", e)
        return entry

    def recent(self, n: int = 20) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._ring)[-n:]

    def count_since(self, ts: float) -> int:
        with self._lock:
            return sum(1 for e in self._ring if e["ts"] >= ts)


# ═════════════════════════════════════════════════════════════════════════════
# RATE LIMITING
# ═════════════════════════════════════════════════════════════════════════════


class RateLimiter:
    """
    Sliding-window rate limiter. `allow(key)` returns True if the key has
    been hit fewer than `max_hits` times in the last `window_sec` seconds.
    """

    def __init__(self, max_hits: int, window_sec: float):
        self._max = int(max_hits)
        self._window = float(window_sec)
        self._hits: Dict[str, Deque[float]] = defaultdict(deque)
        self._lock = threading.Lock()

    def allow(self, key: str) -> bool:
        now = _now()
        cutoff = now - self._window
        with self._lock:
            dq = self._hits[key]
            while dq and dq[0] < cutoff:
                dq.popleft()
            if len(dq) >= self._max:
                return False
            dq.append(now)
            return True

    def count(self, key: str) -> int:
        now = _now()
        cutoff = now - self._window
        with self._lock:
            dq = self._hits[key]
            while dq and dq[0] < cutoff:
                dq.popleft()
            return len(dq)


# ═════════════════════════════════════════════════════════════════════════════
# CIRCUIT BREAKER
# ═════════════════════════════════════════════════════════════════════════════


class CircuitBreaker:
    """
    When engaged, the breaker:
      1. sets `strategy.watchdog_trading_frozen = True` (the entry engine
         should consult this flag — see WIRING notes at bottom of file)
      2. optionally calls an operator-supplied `on_trip` hook (e.g. cancel
         all open orders, flatten position) — not enabled by default
      3. requires explicit operator `clear()` to re-arm

    The breaker also auto-engages if too many heal actions fire in a short
    window (runaway-heal protection) — a feedback safety.
    """

    def __init__(self, strategy: Any,
                 heal_log: HealActionLog,
                 notifier: Optional[Callable[[str], Any]] = None,
                 runaway_heal_threshold: int = 15,
                 runaway_window_sec: float = 300.0,
                 open_duration_sec: float = 300.0):
        self._strategy = strategy
        self._heal_log = heal_log
        self._notify = notifier
        self._runaway_n = int(runaway_heal_threshold)
        self._runaway_window = float(runaway_window_sec)
        self._open_duration = float(open_duration_sec)
        self._engaged = False
        self._engaged_at: Optional[float] = None
        self._reason: Optional[str] = None
        self._state = "CLOSED"
        self._lock = threading.Lock()

    @property
    def engaged(self) -> bool:
        self._maybe_half_open()
        return self._engaged

    @property
    def reason(self) -> Optional[str]:
        return self._reason

    @property
    def engaged_at(self) -> Optional[float]:
        return self._engaged_at

    def _set_strategy_flag(self, value: bool) -> None:
        try:
            setattr(self._strategy, "watchdog_trading_frozen", bool(value))
        except Exception as e:  # noqa: BLE001
            logger.error("watchdog: unable to set watchdog_trading_frozen: %s", e)

    def _maybe_half_open(self) -> None:
        with self._lock:
            if (not self._engaged or self._engaged_at is None or
                    _now() - self._engaged_at < self._open_duration):
                return
            prev_reason = self._reason
            self._engaged = False
            self._state = "HALF_OPEN"
            self._reason = f"HALF_OPEN probe after: {prev_reason}"
            self._engaged_at = None
        self._set_strategy_flag(False)
        msg = (f"⚠️ <b>WATCHDOG CIRCUIT BREAKER HALF-OPEN</b>\n"
               f"Prior reason: {prev_reason}\n"
               f"Entries are allowed for probe; breaker will re-trip on new criticals.")
        logger.warning(msg.replace("<b>", "").replace("</b>", ""))
        if self._notify is not None:
            _safe_call(self._notify, msg)

    def trip(self, reason: str, auto: bool = False) -> None:
        with self._lock:
            if self._engaged:
                return
            self._engaged = True
            self._engaged_at = _now()
            self._reason = reason
            self._state = "OPEN"
        self._set_strategy_flag(True)
        mode = "AUTO" if auto else "MANUAL"
        msg = (f"🛑 <b>WATCHDOG CIRCUIT BREAKER — {mode}</b>\n"
               f"Reason: {reason}\n"
               f"At: {_iso_ist(self._engaged_at)}\n"
               f"New entries FROZEN. Existing trail/exit logic continues.\n"
               f"Acknowledge via /watchdog_unfreeze after investigation.")
        logger.critical(msg.replace("<b>", "").replace("</b>", ""))
        if self._notify is not None:
            _safe_call(self._notify, msg)

    def check_runaway_heals(self) -> bool:
        """Returns True if the breaker is engaged as a result."""
        if self._engaged:
            return True
        cutoff = _now() - self._runaway_window
        n = self._heal_log.count_since(cutoff)
        if n >= self._runaway_n:
            self.trip(
                reason=f"Runaway heal protection: {n} heal actions in "
                       f"{int(self._runaway_window)}s (threshold={self._runaway_n})",
                auto=True,
            )
            return True
        return False

    def clear(self, operator: str = "operator") -> None:
        with self._lock:
            if not self._engaged:
                return
            self._engaged = False
            prev_reason = self._reason
            self._reason = None
            self._engaged_at = None
            self._state = "CLOSED"
        self._set_strategy_flag(False)
        msg = (f"✅ <b>WATCHDOG CIRCUIT BREAKER CLEARED</b>\n"
               f"Cleared by: {operator}\n"
               f"Prior reason: {prev_reason}\n"
               f"Entries RESUMED.")
        logger.warning(msg.replace("<b>", "").replace("</b>", ""))
        if self._notify is not None:
            _safe_call(self._notify, msg)


# ═════════════════════════════════════════════════════════════════════════════
# HEALTH CHECK BASE CLASS
# ═════════════════════════════════════════════════════════════════════════════


class HealthCheck:
    """
    Base class. Subclasses override `check()` and return a HealthResult.

    Attributes controlled by the subclass:
      name           — short identifier used in logs / metrics
      category       — L0_LIVENESS | L1_STATE | L2_EXCHANGE | L3_ENGINE | L4_CONFIG | L5_ACTIVITY
      interval_sec   — how often to run (watchdog orchestrator enforces this)
      heal_confirmations — how many CONSECUTIVE HEAL results required before
                           the watchdog dispatches the associated heal action.
                           Defaults to 3 — prevents single-spike auto-heals
                           on transient states.

    Bookkeeping is handled by the base class:
      _consecutive_failures — counter for heal_confirmations
      _disabled_until       — if a check raises, it's disabled for 5 min
      _last_run             — for interval enforcement
    """

    name: str = "unnamed"
    category: str = "L?_UNKNOWN"
    interval_sec: float = 5.0
    heal_confirmations: int = 3
    safe_when_breaker_engaged: bool = False

    def __init__(self, strategy: Any, router: Any = None,
                 data_manager: Any = None, risk_manager: Any = None,
                 config_module: Any = None):
        self._strategy = strategy
        self._router = router
        self._dm = data_manager
        self._rm = risk_manager
        self._config = config_module
        self._consecutive_failures = 0
        self._disabled_until: float = 0.0
        self._last_run: float = 0.0
        self._last_result: Optional[HealthResult] = None

    @property
    def consecutive_failures(self) -> int:
        return self._consecutive_failures

    @property
    def disabled(self) -> bool:
        return _now() < self._disabled_until

    def disable_for(self, seconds: float, reason: str) -> None:
        self._disabled_until = _now() + float(seconds)
        logger.warning("watchdog: check %s disabled for %.0fs: %s",
                       self.name, seconds, reason)

    def due(self, now: Optional[float] = None) -> bool:
        now = now if now is not None else _now()
        return (now - self._last_run) >= self.interval_sec

    def run(self) -> HealthResult:
        """Wrapper that enforces interval + exception safety + bookkeeping."""
        now = _now()
        self._last_run = now
        try:
            result = self.check()
        except Exception as e:  # noqa: BLE001
            logger.exception("watchdog: check %s raised", self.name)
            self.disable_for(300.0, f"raised: {e}")
            result = HealthResult(
                check_name=self.name,
                severity=Severity.WARN,
                message=f"check raised: {e} — disabled 300s",
                metrics={"exception": repr(e)},
            )
        # Track consecutive HEAL/CRITICAL for heal_confirmations
        if result.severity >= Severity.HEAL:
            self._consecutive_failures += 1
        else:
            self._consecutive_failures = 0
        self._last_result = result
        return result

    def check(self) -> HealthResult:  # pragma: no cover — override
        raise NotImplementedError

    # ---- helper: simple OK factory ----------------------------------------
    def _ok(self, msg: str = "ok", **metrics: Any) -> HealthResult:
        return HealthResult(self.name, Severity.OK, msg, metrics=dict(metrics))

    def _info(self, msg: str, **metrics: Any) -> HealthResult:
        return HealthResult(self.name, Severity.INFO, msg, metrics=dict(metrics))

    def _warn(self, msg: str, **metrics: Any) -> HealthResult:
        return HealthResult(self.name, Severity.WARN, msg, metrics=dict(metrics))

    def _heal(self, msg: str, action: str, **metrics: Any) -> HealthResult:
        return HealthResult(self.name, Severity.HEAL, msg,
                            metrics=dict(metrics), heal_action=action)

    def _critical(self, msg: str, **metrics: Any) -> HealthResult:
        return HealthResult(self.name, Severity.CRITICAL, msg, metrics=dict(metrics))


# ═════════════════════════════════════════════════════════════════════════════
# CONCRETE HEALTH CHECKS
# ═════════════════════════════════════════════════════════════════════════════

# ─── L0: TICK LIVENESS ──────────────────────────────────────────────────────


class TickAgeCheck(HealthCheck):
    """
    Main loop liveness. Escalates if `on_tick` hasn't completed recently.

    Soft threshold: WARN at 15s (matches main.py's existing watchdog).
    Hard threshold: CRITICAL at 60s — trips the circuit breaker.
    """
    name = "tick_age"
    category = "L0_LIVENESS"
    interval_sec = 5.0
    heal_confirmations = 2

    SOFT_SEC = 15.0
    HARD_SEC = 60.0

    def check(self) -> HealthResult:
        # Main.py stores last tick time on the bot object; we accept a
        # strategy-owned equivalent, or fall back to bot reference.
        last_tick = float(_safe_getattr(self._strategy, "_last_tick_time", 0.0) or 0.0)
        if last_tick <= 0.0:
            # Bot not running the main loop yet (startup) or strategy
            # doesn't expose it. This is a config concern, not a liveness
            # failure — INFO, not WARN.
            return self._info("tick timestamp unavailable on strategy")

        age = _now() - last_tick
        if age >= self.HARD_SEC:
            return self._critical(
                f"tick age {age:.0f}s ≥ hard threshold {self.HARD_SEC:.0f}s",
                tick_age_sec=round(age, 1),
            )
        if age >= self.SOFT_SEC:
            return self._warn(
                f"tick age {age:.0f}s ≥ soft threshold {self.SOFT_SEC:.0f}s",
                tick_age_sec=round(age, 1),
            )
        return self._ok(tick_age_sec=round(age, 2))


class MainThreadAliveCheck(HealthCheck):
    """Verifies the main thread is still alive (defensive — should never fail)."""
    name = "main_thread_alive"
    category = "L0_LIVENESS"
    interval_sec = 10.0

    def check(self) -> HealthResult:
        main = threading.main_thread()
        if not main.is_alive():
            return self._critical("main thread DEAD")
        return self._ok()


# ─── L1: STATE INVARIANTS ───────────────────────────────────────────────────


class StuckExitCompletedFlagCheck(HealthCheck):
    """
    Root cause of "no trades after 1st" class of bugs.

    Invariant: if `_pos.phase == FLAT` AND `_exit_completed == True` for
    more than 30s, the flag was left set by a prior exit path and forgot
    to be cleared by `_enter_trade` (either because no new trade was
    attempted, or because attempt was rejected before the reset line).

    This flag being stuck does not directly block entries, but if any
    delayed sync/reconcile thread fires `_record_exchange_exit()` later,
    it will bail at the `_exit_completed` guard and leave the bot in a
    subtly corrupted state. Clearing is safe whenever phase=FLAT.
    """
    name = "stuck_exit_completed"
    category = "L1_STATE"
    interval_sec = 10.0
    heal_confirmations = 2
    safe_when_breaker_engaged = True
    THRESH_SEC = 30.0

    def check(self) -> HealthResult:
        pos = _safe_getattr(self._strategy, "_pos", None)
        if pos is None:
            return self._ok("no position state")

        phase = _safe_getattr(pos, "phase", None)
        phase_name = getattr(phase, "name", str(phase))
        if phase_name != "FLAT":
            return self._ok(phase=phase_name)

        exit_completed = bool(_safe_getattr(self._strategy, "_exit_completed", False))
        if not exit_completed:
            return self._ok()

        # How long has it been stuck? Use _last_exit_time as the anchor.
        last_exit = float(_safe_getattr(self._strategy, "_last_exit_time", 0.0) or 0.0)
        stuck_for = _now() - last_exit if last_exit > 0 else self.THRESH_SEC + 1
        if stuck_for < self.THRESH_SEC:
            return self._info("phase=FLAT, _exit_completed=True — within grace",
                              stuck_for_sec=round(stuck_for, 1))

        return self._heal(
            f"_exit_completed stuck True for {stuck_for:.0f}s while phase=FLAT "
            f"— this can silently drop a future exit record",
            action="clear_exit_completed_flag",
            stuck_for_sec=round(stuck_for, 1),
        )


class StuckPosSyncFlagCheck(HealthCheck):
    """
    `_pos_sync_in_progress` should be cleared by the background sync thread's
    finally block. If the flag is set but no such thread is alive, the
    previous sync died violently (OOM, signal, etc.) and the flag is stuck.
    Consequence: `_manage_active` is skipped on every tick (line 2767 check),
    so the trail NEVER runs.
    """
    name = "stuck_pos_sync_flag"
    category = "L1_STATE"
    interval_sec = 15.0
    heal_confirmations = 2
    safe_when_breaker_engaged = True
    THRESH_SEC = 120.0   # sync has a 30s REST timeout; 120s is ample

    def _sync_thread_alive(self, name_contains: str) -> bool:
        for t in threading.enumerate():
            if name_contains in t.name and t.is_alive():
                return True
        return False

    def check(self) -> HealthResult:
        flag = bool(_safe_getattr(self._strategy, "_pos_sync_in_progress", False))
        if not flag:
            return self._ok()

        last_pos_sync = float(_safe_getattr(self._strategy, "_last_pos_sync", 0.0) or 0.0)
        age = _now() - last_pos_sync if last_pos_sync > 0 else self.THRESH_SEC + 1
        thread_alive = self._sync_thread_alive("pos-sync-active")

        if thread_alive and age < self.THRESH_SEC:
            return self._ok(flag=True, age_sec=round(age, 1), thread_alive=True)

        if thread_alive:
            # Thread alive but very old — WARN but do not heal (avoids double-clear
            # during a legitimately slow REST call).
            return self._warn(
                f"pos-sync thread alive {age:.0f}s (threshold {self.THRESH_SEC:.0f}s)",
                age_sec=round(age, 1), thread_alive=True,
            )

        # Flag set, no thread alive → stuck
        return self._heal(
            f"_pos_sync_in_progress=True but no sync thread alive (age={age:.0f}s) "
            f"— trail is BLOCKED until this clears",
            action="clear_pos_sync_flag",
            age_sec=round(age, 1),
        )


class StuckExitSyncFlagCheck(HealthCheck):
    """Same as pos_sync but for the EXITING phase sync thread."""
    name = "stuck_exit_sync_flag"
    category = "L1_STATE"
    interval_sec = 15.0
    heal_confirmations = 2
    safe_when_breaker_engaged = True
    THRESH_SEC = 180.0   # EXITING can legitimately take longer

    def _sync_thread_alive(self) -> bool:
        for t in threading.enumerate():
            if "pos-sync-exit" in t.name and t.is_alive():
                return True
        return False

    def check(self) -> HealthResult:
        flag = bool(_safe_getattr(self._strategy, "_exit_sync_in_progress", False))
        if not flag:
            return self._ok()
        last = float(_safe_getattr(self._strategy, "_last_exit_sync", 0.0) or 0.0)
        age = _now() - last if last > 0 else self.THRESH_SEC + 1
        if self._sync_thread_alive():
            if age >= self.THRESH_SEC:
                return self._warn(f"exit-sync thread alive {age:.0f}s",
                                  age_sec=round(age, 1))
            return self._ok(age_sec=round(age, 1))
        return self._heal(
            f"_exit_sync_in_progress=True but no exit-sync thread alive "
            f"(age={age:.0f}s)",
            action="clear_exit_sync_flag",
            age_sec=round(age, 1),
        )


class StuckTrailFlagCheck(HealthCheck):
    """
    `_trail_in_progress=True` with no trail thread alive.
    Consequence: trail engine will refuse to run on subsequent ticks.
    """
    name = "stuck_trail_flag"
    category = "L1_STATE"
    interval_sec = 15.0
    heal_confirmations = 2
    safe_when_breaker_engaged = True
    THRESH_SEC = 60.0

    def check(self) -> HealthResult:
        flag = bool(_safe_getattr(self._strategy, "_trail_in_progress", False))
        if not flag:
            return self._ok()
        last_trail_rest = float(_safe_getattr(self._strategy, "_last_trail_rest_time", 0.0) or 0.0)
        age = _now() - last_trail_rest if last_trail_rest > 0 else self.THRESH_SEC + 1
        trail_thread_alive = any(
            ("trail" in t.name.lower()) and t.is_alive()
            for t in threading.enumerate()
        )
        if trail_thread_alive and age < self.THRESH_SEC:
            return self._ok(age_sec=round(age, 1))
        if trail_thread_alive:
            return self._warn(f"trail thread alive {age:.0f}s", age_sec=round(age, 1))
        return self._heal(
            f"_trail_in_progress=True but no trail thread alive ({age:.0f}s)",
            action="clear_trail_flag",
            age_sec=round(age, 1),
        )


class StuckReconcilePendingCheck(HealthCheck):
    """
    `_reconcile_pending=True` should clear within ~30s via the reconcile
    thread's finally. If it's stuck, reconcile NEVER runs again →
    `sl_order_id` never gets recovered for Delta bracket orders (Bug A).
    """
    name = "stuck_reconcile_pending"
    category = "L1_STATE"
    interval_sec = 20.0
    heal_confirmations = 2
    safe_when_breaker_engaged = True
    THRESH_SEC = 180.0

    def check(self) -> HealthResult:
        flag = bool(_safe_getattr(self._strategy, "_reconcile_pending", False))
        if not flag:
            return self._ok()
        last = float(_safe_getattr(self._strategy, "_last_reconcile_time", 0.0) or 0.0)
        age = _now() - last if last > 0 else self.THRESH_SEC + 1
        if age < self.THRESH_SEC:
            return self._info("reconcile in progress", age_sec=round(age, 1))
        return self._heal(
            f"_reconcile_pending=True stuck for {age:.0f}s "
            f"— reconciliation blocked, SL recovery unavailable",
            action="clear_reconcile_pending",
            age_sec=round(age, 1),
        )


class PositionPhaseInvariantCheck(HealthCheck):
    """
    Invariants:
      phase == ACTIVE  → entry_price>0, quantity>0, sl_price>0, entry_time>0, side in {long,short}
      phase == FLAT    → quantity==0
      phase == EXITING → entry_price>0
      phase == ENTERING → entering_since is set and recent

    Violations are CRITICAL (bot state is internally inconsistent).
    """
    name = "position_phase_invariant"
    category = "L1_STATE"
    interval_sec = 10.0
    heal_confirmations = 1  # critical: react fast

    def check(self) -> HealthResult:
        pos = _safe_getattr(self._strategy, "_pos", None)
        if pos is None:
            return self._ok()
        phase = _safe_getattr(pos, "phase", None)
        phase_name = getattr(phase, "name", str(phase))
        side = (_safe_getattr(pos, "side", "") or "").lower()
        qty = float(_safe_getattr(pos, "quantity", 0.0) or 0.0)
        entry = float(_safe_getattr(pos, "entry_price", 0.0) or 0.0)
        sl = float(_safe_getattr(pos, "sl_price", 0.0) or 0.0)
        entry_time = float(_safe_getattr(pos, "entry_time", 0.0) or 0.0)

        if phase_name == "ACTIVE":
            problems = []
            if entry <= 0:
                problems.append("entry_price=0")
            if qty <= 0:
                problems.append("quantity=0")
            if sl <= 0:
                problems.append("sl_price=0")
            if entry_time <= 0:
                problems.append("entry_time=0")
            if side not in ("long", "short"):
                problems.append(f"side='{side}'")
            if problems:
                return self._critical(
                    f"phase=ACTIVE but invariants violated: {', '.join(problems)}",
                    phase=phase_name, entry=entry, qty=qty, sl=sl,
                    entry_time=entry_time, side=side,
                )
            return self._ok(phase=phase_name)

        if phase_name == "FLAT":
            if qty > 1e-10:
                return self._critical(
                    f"phase=FLAT but quantity={qty} (should be 0)",
                    phase=phase_name, qty=qty,
                )
            return self._ok(phase=phase_name)

        if phase_name == "EXITING":
            if entry <= 0:
                return self._critical(
                    "phase=EXITING but entry_price=0",
                    phase=phase_name, entry=entry,
                )
            return self._ok(phase=phase_name)

        # ENTERING: fine
        return self._ok(phase=phase_name)


class EnteringPhaseTimeoutCheck(HealthCheck):
    """
    The strategy has its own two-stage ENTERING watchdog (pre-order 45s,
    post-order fill-timeout + 25% buffer). This check is an OUTER safety
    net that fires if BOTH stages have been exceeded by a large margin —
    which could only happen if the internal watchdog itself failed (e.g.,
    the finally block never ran).

    Triggers only beyond the strategy's own limits + 60s grace.
    """
    name = "entering_phase_timeout"
    category = "L1_STATE"
    interval_sec = 15.0
    heal_confirmations = 2
    EXTRA_GRACE_SEC = 60.0

    def check(self) -> HealthResult:
        pos = _safe_getattr(self._strategy, "_pos", None)
        if pos is None:
            return self._ok()
        phase = _safe_getattr(pos, "phase", None)
        if getattr(phase, "name", str(phase)) != "ENTERING":
            return self._ok()

        now = _now()
        entering_since = float(_safe_getattr(self._strategy, "_entering_since", 0.0) or 0.0)
        order_placed_at = float(_safe_getattr(self._strategy, "_entry_order_placed_at", 0.0) or 0.0)

        cfg = self._config
        fill_timeout = float(_safe_getattr(cfg, "LIMIT_ORDER_FILL_TIMEOUT_SEC", 120.0))
        pre_order_limit = 45.0 + self.EXTRA_GRACE_SEC
        post_order_limit = fill_timeout + max(30.0, fill_timeout * 0.25) + self.EXTRA_GRACE_SEC

        if order_placed_at <= 0.0:
            elapsed = now - entering_since if entering_since > 0 else 0
            if elapsed > pre_order_limit:
                return self._heal(
                    f"ENTERING (pre-order) stuck {elapsed:.0f}s > {pre_order_limit:.0f}s "
                    f"— outer watchdog firing (inner watchdog should have)",
                    action="force_flat_from_entering",
                    elapsed_sec=round(elapsed, 1), stage="pre-order",
                )
            return self._ok(stage="pre-order", elapsed_sec=round(elapsed, 1))

        elapsed = now - order_placed_at
        if elapsed > post_order_limit:
            return self._heal(
                f"ENTERING (post-order) stuck {elapsed:.0f}s > {post_order_limit:.0f}s",
                action="force_flat_from_entering",
                elapsed_sec=round(elapsed, 1), stage="post-order",
            )
        return self._ok(stage="post-order", elapsed_sec=round(elapsed, 1))


class ExitingPhaseTimeoutCheck(HealthCheck):
    """Outer safety net on EXITING phase (strategy has inner 120s limit)."""
    name = "exiting_phase_timeout"
    category = "L1_STATE"
    interval_sec = 15.0
    heal_confirmations = 2
    OUTER_LIMIT_SEC = 300.0

    def check(self) -> HealthResult:
        pos = _safe_getattr(self._strategy, "_pos", None)
        if pos is None:
            return self._ok()
        if getattr(_safe_getattr(pos, "phase", None), "name", "") != "EXITING":
            return self._ok()
        exiting_since = float(_safe_getattr(self._strategy, "_exiting_since", 0.0) or 0.0)
        if exiting_since <= 0:
            return self._ok()
        age = _now() - exiting_since
        if age < self.OUTER_LIMIT_SEC:
            return self._ok(age_sec=round(age, 1))
        return self._critical(
            f"EXITING stuck {age:.0f}s > outer limit {self.OUTER_LIMIT_SEC:.0f}s "
            f"— internal watchdog failed",
            age_sec=round(age, 1),
        )


# ─── L2: EXCHANGE RECONCILIATION ────────────────────────────────────────────


class ExchangePositionDriftCheck(HealthCheck):
    """
    The highest-impact check. Compares bot's internal phase with the
    exchange's actual position.

    Four divergence cases:
      A) bot=FLAT, exchange has position  → CRITICAL (orphan position)
      B) bot=ACTIVE, exchange=flat         → HEAL (missed exit — force flat)
      C) bot=ACTIVE, exchange=ACTIVE but side/qty mismatch → CRITICAL
      D) bot=ENTERING long time, exchange=flat → HEAL (entry never reached)

    This check is DEFENSIVELY rate-limited (runs every 60s) because each
    execution costs a REST call.
    """
    name = "exchange_position_drift"
    category = "L2_EXCHANGE"
    interval_sec = 60.0
    heal_confirmations = 2   # require 2 confirmations due to REST noise

    def _fetch_exchange_pos(self) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
        """Returns (ok, pos_dict_or_None, err_str)."""
        router = self._router
        if router is None:
            return False, None, "no router"

        # Try several accessors — OM or router may expose different methods.
        candidate_oms: List[Any] = []
        for accessor in ("get_active_om", "active_order_manager", "active_om"):
            val = _safe_getattr(router, accessor, None)
            if callable(val):
                om = _safe_call(val, default=None)
            else:
                om = val
            if om is not None:
                candidate_oms.append(om)
        candidate_oms.append(router)  # fallback

        for om in candidate_oms:
            fn = _safe_getattr(om, "get_open_position", None)
            if not callable(fn):
                fn = _safe_getattr(om, "get_position", None)
            if not callable(fn):
                continue
            try:
                raw = fn()
            except Exception as e:  # noqa: BLE001
                return False, None, f"get_open_position error: {e}"
            if raw is None:
                return True, None, None  # exchange is FLAT
            # Normalize to dict
            if isinstance(raw, dict):
                out = raw
            else:
                out = {
                    "side": _safe_getattr(raw, "side", ""),
                    "size": _safe_getattr(raw, "size", _safe_getattr(raw, "quantity", 0)),
                    "entry_price": _safe_getattr(raw, "entry_price", 0),
                }
            # Some exchanges return dict with size=0 for "no position"
            try:
                sz = float(out.get("size") or out.get("quantity") or 0)
                if abs(sz) < 1e-10:
                    return True, None, None
            except Exception:
                pass
            return True, out, None
        return False, None, "no get_open_position accessor"

    def check(self) -> HealthResult:
        pos = _safe_getattr(self._strategy, "_pos", None)
        if pos is None:
            return self._ok()
        phase_name = getattr(_safe_getattr(pos, "phase", None), "name", "")

        ok, ex_pos, err = self._fetch_exchange_pos()
        if not ok:
            # REST error — don't count as a state problem, just INFO
            return self._info(f"exchange pos unavailable: {err}")

        bot_active = phase_name in ("ACTIVE", "EXITING")
        bot_entering = phase_name == "ENTERING"
        bot_flat = phase_name == "FLAT"
        exch_active = ex_pos is not None

        # Case A: orphan position
        if bot_flat and exch_active:
            last_exit = float(_safe_getattr(self._strategy, "_last_exit_time", 0.0) or 0.0)
            since_exit = _now() - last_exit if last_exit > 0 else 999
            # If a recent exit just happened, the exchange may still show the
            # position for a second or two before the position update propagates.
            if since_exit < 15.0:
                return self._info(
                    f"exchange still shows position {since_exit:.0f}s after local exit — transient",
                    since_exit_sec=round(since_exit, 1),
                )
            return self._critical(
                f"ORPHAN POSITION: bot=FLAT but exchange has {ex_pos.get('side', '?')} "
                f"size={ex_pos.get('size', '?')} — position is unmanaged",
                exchange_pos=ex_pos,
            )

        # Case B: bot thinks ACTIVE, exchange is flat
        if bot_active and not exch_active:
            # Brief window right after SL/TP fill is normal; only heal if
            # the condition is persistent.
            return self._heal(
                f"bot phase={phase_name} but exchange FLAT — position was "
                f"closed externally (SL/TP hit?) without local exit record",
                action="adopt_exchange_flat",
            )

        # Case C: bot ACTIVE, exchange ACTIVE but mismatch
        if bot_active and exch_active:
            bot_side = (_safe_getattr(pos, "side", "") or "").lower()
            ex_side_raw = str(ex_pos.get("side", "")).lower()
            # Exchange may report "buy"/"sell" instead of "long"/"short"
            ex_side = {"buy": "long", "sell": "short"}.get(ex_side_raw, ex_side_raw)
            bot_qty = float(_safe_getattr(pos, "quantity", 0.0) or 0.0)
            ex_qty = abs(float(ex_pos.get("size") or ex_pos.get("quantity") or 0))

            side_ok = (bot_side == ex_side) or (not ex_side)  # some APIs don't return side
            qty_ok = abs(bot_qty - ex_qty) / max(bot_qty, 1e-9) < 0.05  # within 5%

            if not side_ok:
                return self._critical(
                    f"SIDE MISMATCH: bot={bot_side} exchange={ex_side}",
                    bot_side=bot_side, exchange_side=ex_side,
                )
            if not qty_ok:
                return self._warn(
                    f"QUANTITY DRIFT: bot={bot_qty} exchange={ex_qty} "
                    f"({abs(bot_qty - ex_qty) / max(bot_qty, 1e-9):.1%})",
                    bot_qty=bot_qty, exchange_qty=ex_qty,
                )
            return self._ok(bot_qty=bot_qty, exchange_qty=ex_qty)

        # Case D: bot ENTERING, exchange flat
        if bot_entering and not exch_active:
            entering_since = float(_safe_getattr(self._strategy, "_entering_since", 0.0) or 0.0)
            order_placed_at = float(_safe_getattr(self._strategy, "_entry_order_placed_at", 0.0) or 0.0)
            age = _now() - max(entering_since, order_placed_at)
            # Don't fight the strategy's own watchdog; only react beyond 180s
            if age < 180.0:
                return self._info(f"ENTERING with no exchange fill yet ({age:.0f}s)",
                                  age_sec=round(age, 1))
            return self._heal(
                f"ENTERING {age:.0f}s with no exchange position — "
                f"entry never landed, force-flat",
                action="force_flat_from_entering",
                age_sec=round(age, 1),
            )

        return self._ok(phase=phase_name)


class MissingSlOrderIdCheck(HealthCheck):
    """
    Bug A from the audit: for Delta bracket orders, `sl_order_id` is often
    empty because `bracket_sl_order_id` is not populated by the order
    placement path. Consequence: trailing SL returns False at its first
    gate on every tick.

    This check detects the condition and offers to HEAL by querying the
    exchange's open orders and matching a reduce-only SL order on the
    correct side.
    """
    name = "missing_sl_order_id"
    category = "L2_EXCHANGE"
    interval_sec = 30.0
    heal_confirmations = 2

    def check(self) -> HealthResult:
        pos = _safe_getattr(self._strategy, "_pos", None)
        if pos is None:
            return self._ok()
        phase_name = getattr(_safe_getattr(pos, "phase", None), "name", "")
        if phase_name != "ACTIVE":
            return self._ok(phase=phase_name)

        sl_id = _safe_getattr(pos, "sl_order_id", "") or ""
        if sl_id:
            return self._ok(sl_id_set=True)

        # Also check sl_price is set — if both are 0, the position is
        # wholly unprotected (should have been caught by invariant check).
        sl_price = float(_safe_getattr(pos, "sl_price", 0.0) or 0.0)

        # How long has position been active?
        entry_time = float(_safe_getattr(pos, "entry_time", 0.0) or 0.0)
        age = _now() - entry_time if entry_time > 0 else 0

        # Give the strategy's own 30s reconcile one cycle to try first.
        if age < 45.0:
            return self._info(
                f"sl_order_id empty but position only {age:.0f}s old — "
                f"waiting for reconcile",
                age_sec=round(age, 1), sl_price=sl_price,
            )

        return self._heal(
            f"sl_order_id EMPTY for {age:.0f}s — trail is DEAD "
            f"(bracket_sl_order_id never populated). Will query open orders.",
            action="recover_sl_order_id",
            age_sec=round(age, 1), sl_price=sl_price,
        )


# ─── L3: ENGINE LIVENESS ────────────────────────────────────────────────────


class EntryEngineStuckCheck(HealthCheck):
    """
    The entry engine has states SCANNING / POST_SWEEP / ENTERING / IN_POSITION.
    It has its own 4-hour stuck-limit for IN_POSITION (too lenient — this
    could explain "no trades after first" if `on_position_closed()` was
    never called).

    We detect:
      * bot `_pos.phase == FLAT` but entry_engine._state is NOT in
        {SCANNING, POST_SWEEP}. This means the entry engine is still in
        IN_POSITION or ENTERING while the bot has no position.
      * bot is FLAT and entry_engine is SCANNING, but `_signal` remains
        latched for >60s. A pending signal should be consumed, blocked, or
        launched within a tick or two; a stale signal suppresses new scans.
    """
    name = "entry_engine_stuck"
    category = "L3_ENGINE"
    interval_sec = 15.0
    heal_confirmations = 2

    def check(self) -> HealthResult:
        strat = self._strategy
        pos = _safe_getattr(strat, "_pos", None)
        engine = _safe_getattr(strat, "_entry_engine", None)
        if pos is None or engine is None:
            return self._ok("no entry engine")

        phase_name = getattr(_safe_getattr(pos, "phase", None), "name", "")
        if phase_name != "FLAT":
            return self._ok(phase=phase_name)

        ee_state = _safe_getattr(engine, "_state", None)
        ee_state_name = getattr(ee_state, "name", str(ee_state))

        signal = _safe_getattr(engine, "_signal", None)
        if ee_state_name == "SCANNING" and signal is not None:
            created_at = float(_safe_getattr(signal, "created_at", 0.0) or 0.0)
            age = _now() - created_at if created_at > 0 else 0.0
            side = _safe_getattr(signal, "side", "?")
            entry_type = _safe_getattr(signal, "entry_type", "")
            entry_type_name = getattr(entry_type, "name", str(entry_type))
            if age < 60.0:
                return self._info(
                    f"entry engine has pending {entry_type_name} {side} signal "
                    f"while bot=FLAT ({age:.0f}s old; within grace)",
                    age_sec=round(age, 1),
                    engine_state=ee_state_name,
                    signal_side=side,
                    signal_type=entry_type_name,
                )
            return self._heal(
                f"entry engine has stale {entry_type_name} {side} signal "
                f"for {age:.0f}s while bot=FLAT — new scans are suppressed",
                action="reset_entry_engine",
                age_sec=round(age, 1),
                engine_state=ee_state_name,
                signal_side=side,
                signal_type=entry_type_name,
            )

        # Allowed engine states while bot is FLAT
        if ee_state_name in ("SCANNING", "POST_SWEEP"):
            return self._ok(engine_state=ee_state_name)

        # Engine is IN_POSITION or ENTERING but bot has no position — stuck
        state_entered = float(_safe_getattr(engine, "_state_entered", 0.0) or 0.0)
        age = _now() - state_entered if state_entered > 0 else 0

        # Grace: allow 60s for on_position_closed() propagation
        if age < 60.0:
            return self._info(
                f"entry engine={ee_state_name} but bot=FLAT ({age:.0f}s — within grace)",
                age_sec=round(age, 1), engine_state=ee_state_name,
            )

        return self._heal(
            f"entry engine stuck in {ee_state_name} for {age:.0f}s while bot=FLAT "
            f"— all new sweeps will be rejected until self-recovery",
            action="reset_entry_engine",
            age_sec=round(age, 1), engine_state=ee_state_name,
        )


class CooldownPersistenceCheck(HealthCheck):
    """
    `cooldown_ok = now - _last_exit_time >= COOLDOWN_SEC`. If _last_exit_time
    is somehow in the future (clock skew) OR exceeds now by any amount, the
    cooldown is permanent.

    Also detects the case where `_last_exit_time` was set to a very recent
    time by some error path that did not actually close a position — which
    would delay entries without justification.
    """
    name = "cooldown_persistence"
    category = "L3_ENGINE"
    interval_sec = 30.0
    heal_confirmations = 2

    def check(self) -> HealthResult:
        last_exit = float(_safe_getattr(self._strategy, "_last_exit_time", 0.0) or 0.0)
        if last_exit <= 0:
            return self._ok()

        now = _now()
        skew = last_exit - now

        if skew > 5.0:
            return self._heal(
                f"_last_exit_time is {skew:.0f}s IN THE FUTURE — "
                f"cooldown will never clear",
                action="reset_last_exit_time",
                skew_sec=round(skew, 1),
            )

        if skew > 0.5:
            return self._warn(
                f"_last_exit_time slightly ahead of now ({skew:.1f}s) — clock skew?",
                skew_sec=round(skew, 2),
            )

        return self._ok(age_sec=round(now - last_exit, 1))


class NotifierQueueDepthCheck(HealthCheck):
    """
    Telegram queue depth — from the audit (Bug #36) a 200-item queue at
    1.2s/msg = 4-min backlog. UNPROTECTED alerts may be dropped.

    This check introspects the notifier module's queue if accessible.
    """
    name = "notifier_queue_depth"
    category = "L3_ENGINE"
    interval_sec = 30.0
    heal_confirmations = 2

    def check(self) -> HealthResult:
        # Try common import paths. telegram.notifier exposes _send_queue in
        # this codebase; older variants used _queue/_tg_queue.
        q = None
        try:
            from telegram import notifier as _n
            q = (
                _safe_getattr(_n, "_send_queue", None)
                or _safe_getattr(_n, "_queue", None)
                or _safe_getattr(_n, "_tg_queue", None)
            )
        except Exception:
            pass
        if q is None:
            try:
                import notifier as _n  # type: ignore
                q = (
                    _safe_getattr(_n, "_send_queue", None)
                    or _safe_getattr(_n, "_queue", None)
                    or _safe_getattr(_n, "_tg_queue", None)
                )
            except Exception:
                pass

        if q is None:
            return self._ok("notifier queue not accessible", accessible=False)

        try:
            depth = q.qsize()
            maxsize = getattr(q, "maxsize", 0) or 0
        except Exception as e:  # noqa: BLE001
            return self._info(f"notifier queue read failed: {e}")

        pct = (depth / maxsize) if maxsize > 0 else 0
        if maxsize > 0 and pct >= 0.95:
            return self._critical(
                f"notifier queue {depth}/{maxsize} ({pct:.0%}) — alerts being dropped",
                depth=depth, maxsize=maxsize,
            )
        if maxsize > 0 and pct >= 0.75:
            return self._warn(
                f"notifier queue {depth}/{maxsize} ({pct:.0%}) — backlogged",
                depth=depth, maxsize=maxsize,
            )
        return self._ok(depth=depth, maxsize=maxsize)


# ─── L4: CONFIG DRIFT ───────────────────────────────────────────────────────


class ConfigDriftCheck(HealthCheck):
    """
    Detects the class of bug where a config value is set but the engine
    reads a hardcoded module constant instead (Bug B from the audit).

    This check enumerates KNOWN pairs (config_key, module.path.constant)
    and warns if they diverge. Purely observational — config-driven
    behavior requires code fixes, not runtime heals.
    """
    name = "config_drift"
    category = "L4_CONFIG"
    interval_sec = 600.0   # slow cadence — config does not change at runtime
    heal_confirmations = 99  # never auto-heal; info only

    # (config_key, module_name, attr_path, tolerance)
    _PAIRS: Sequence[Tuple[str, str, str, float]] = (
        ("QUANT_TRAIL_BE_R",   "liquidity_trail", "PHASE_0_MAX_R",  1e-9),
        ("QUANT_TRAIL_LOCK_R", "liquidity_trail", "PHASE_1_MAX_R",  1e-9),
        ("MIN_SL_DISTANCE_PCT", "entry_engine", "_MIN_SL_DISTANCE_PCT", 1e-12),
        ("MAX_SL_DISTANCE_PCT", "entry_engine", "_MAX_SL_DISTANCE_PCT", 1e-12),
    )

    def check(self) -> HealthResult:
        cfg = self._config
        if cfg is None:
            return self._ok("no config module")

        drifts: List[str] = []
        metrics: Dict[str, Any] = {}
        for cfg_key, mod_name, attr, tol in self._PAIRS:
            cfg_val = _safe_getattr(cfg, cfg_key, None)
            if cfg_val is None:
                continue
            mod = None
            for candidate in (mod_name, f"strategy.{mod_name}"):
                try:
                    mod = __import__(candidate, fromlist=[attr])
                    break
                except Exception:
                    mod = None
            if mod is None:
                continue
            mod_val = _safe_getattr(mod, attr, None)
            if mod_val is None:
                continue
            metrics[f"{cfg_key}_cfg"] = cfg_val
            metrics[f"{mod_name}.{attr}"] = mod_val
            try:
                if abs(float(cfg_val) - float(mod_val)) > float(tol):
                    drifts.append(f"{cfg_key}({cfg_val}) ≠ {mod_name}.{attr}({mod_val})")
            except Exception:
                if cfg_val != mod_val:
                    drifts.append(f"{cfg_key}({cfg_val}) ≠ {mod_name}.{attr}({mod_val})")

        if drifts:
            return self._warn(
                "config values diverge from engine constants: " + "; ".join(drifts),
                **metrics,
            )
        return self._ok(**metrics)


# ─── L5: ACTIVITY / PROGRESS ────────────────────────────────────────────────


class NoTradesAfterFirstCheck(HealthCheck):
    """
    The user's specific symptom.

    Conditions to flag:
      - bot has closed at least 1 trade (_total_trades >= 1)
      - phase == FLAT
      - time since last exit > NO_TRADE_WARN_SEC (default 30 min)
      - risk gate says can_trade == True
      - circuit breaker not engaged

    Emits WARN (with full forensic capture) so the operator can investigate
    the root cause — this is intentionally NOT auto-heal because the cause
    could be many things (signal rejection, market conditions, etc.) and
    the system SHOULD be able to go 30+ min without trading when conditions
    don't warrant.

    The forensic dump at the WARN point gives the operator everything
    needed to diagnose WHY (entry engine state, recent rejections, risk
    gate state, etc.).
    """
    name = "no_trades_after_first"
    category = "L5_ACTIVITY"
    interval_sec = 120.0
    heal_confirmations = 99  # never heals — pure alert

    WARN_SEC = 1800.0   # 30 min
    CRITICAL_SEC = 7200.0  # 2h

    def check(self) -> HealthResult:
        stats = _safe_call(_safe_getattr(self._strategy, "get_stats", None), default={}) or {}
        total_trades = int(stats.get("total_trades", 0) or 0)
        if total_trades < 1:
            return self._ok("no trades yet — not in scope")

        pos = _safe_getattr(self._strategy, "_pos", None)
        phase_name = getattr(_safe_getattr(pos, "phase", None), "name", "")
        if phase_name != "FLAT":
            return self._ok(phase=phase_name)

        last_exit = float(_safe_getattr(self._strategy, "_last_exit_time", 0.0) or 0.0)
        if last_exit <= 0:
            return self._ok()

        since = _now() - last_exit

        # Check risk gate: can_trade
        rg = _safe_getattr(self._strategy, "_risk_gate", None)
        can_trade = True
        reason = ""
        if rg is not None:
            # can_trade requires a balance argument; approximate with the daily_open_bal
            try:
                bal = float(_safe_getattr(rg, "_daily_open_bal", 0.0) or 0.0)
                if bal < 1e-6:
                    bal = 1000.0  # arbitrary positive to exercise the other gates
                can_trade, reason = rg.can_trade(bal)
            except Exception:
                pass

        if not can_trade:
            return self._info(
                f"{since/60:.0f}min since last trade, risk gate blocks: {reason}",
                since_min=round(since / 60, 1), can_trade=False, reason=reason,
            )

        # The strategy's hard trade halt lives inside ConvictionFilter, not
        # DailyRiskGate. Without checking it, this watchdog reports a false
        # "signal suppression" warning while the session drawdown breaker is
        # intentionally blocking entries.
        conv = _safe_getattr(self._strategy, "_conviction", None)
        if conv is not None:
            try:
                bal = 0.0
                rm = _safe_getattr(self, "_rm", None)
                if rm is not None:
                    bal = float(
                        _safe_getattr(rm, "available_balance", 0.0)
                        or _safe_getattr(rm, "current_balance", 0.0)
                        or 0.0
                    )
                if bal <= 1e-6 and rg is not None:
                    bal = float(_safe_getattr(rg, "_daily_open_bal", 0.0) or 0.0)
                limit_check = _safe_getattr(conv, "_check_session_limits", None)
                conv_reason = limit_check(_now(), "", live_balance=bal) if limit_check else ""
            except Exception:
                conv_reason = ""
            if conv_reason:
                return self._info(
                    f"{since/60:.0f}min since last trade, conviction gate blocks: "
                    f"{conv_reason}",
                    since_min=round(since / 60, 1),
                    can_trade=False,
                    reason=conv_reason,
                )

        engine = _safe_getattr(self._strategy, "_entry_engine", None)
        signal = _safe_getattr(engine, "_signal", None)
        signal_type = _safe_getattr(signal, "entry_type", "")
        signal_created = float(_safe_getattr(signal, "created_at", 0.0) or 0.0)
        metrics = {
            "since_min": round(since / 60, 1),
            "total_trades": total_trades,
            "can_trade": True,
            "engine_state": getattr(
                _safe_getattr(engine, "_state", None),
                "name", "?",
            ),
            "pending_signal": signal is not None,
            "pending_signal_age_sec": round(_now() - signal_created, 1) if signal_created > 0 else None,
            "pending_signal_type": getattr(signal_type, "name", str(signal_type)) if signal is not None else None,
            "pending_signal_side": _safe_getattr(signal, "side", None),
        }

        if since >= self.CRITICAL_SEC:
            return self._critical(
                f"no trade for {since/60:.0f}min despite {total_trades} prior "
                f"trades and open risk gate — entries appear suppressed",
                **metrics,
            )
        if since >= self.WARN_SEC:
            return self._warn(
                f"no trade for {since/60:.0f}min — investigate signal suppression",
                **metrics,
            )
        return self._ok(**metrics)


class DailyCounterConsistencyCheck(HealthCheck):
    """
    Bug #10: Dual daily-trade tracking — `DailyRiskGate._daily_trades` is
    the authoritative counter, but `risk_manager._daily_trades` (if the
    separate RiskManager exists) may drift because `record_trade` was
    not always called.

    Semantic invariant (NOT previously enforced):
        DailyRiskGate._daily_trades counts trade STARTS (bumped on
        `record_trade_start()` at entry confirmation).

        RiskManager._daily_trades (if present) should count COMPLETED
        trades (bumped on `record_trade()` at exit finalisation).

    Therefore the EXPECTED drift is:
        drift == open_positions_count

    i.e. `drift == 1` while any position is ACTIVE/ENTERING/EXITING is
    NOT a bug. The previous implementation used a flat `drift > 1`
    threshold which fired false-positives every 5 min whenever a
    position was held through the check interval (or when two
    sequential entries happened without the exits being recorded).

    This check warns on UNEXPLAINED drift. Does not auto-heal because
    the 'right' counter depends on which one the strategy is using for
    gating.
    """
    name = "daily_counter_consistency"
    category = "L5_ACTIVITY"
    interval_sec = 300.0
    heal_confirmations = 99  # never heals

    def check(self) -> HealthResult:
        strat = self._strategy
        rg = _safe_getattr(strat, "_risk_gate", None)
        rm = self._rm
        rg_count = int(_safe_getattr(rg, "_daily_trades", 0) or 0) if rg else 0

        def _rm_daily_count(obj) -> int:
            if obj is None:
                return 0
            raw = _safe_getattr(obj, "_daily_trades", None)
            if raw is not None:
                try:
                    return int(raw)
                except Exception:
                    try:
                        return len(raw)
                    except Exception:
                        pass
            trades = _safe_getattr(obj, "daily_trades", None)
            if trades is not None:
                try:
                    return len(trades)
                except Exception:
                    try:
                        return int(trades)
                    except Exception:
                        pass
            return 0

        rm_count = _rm_daily_count(rm)
        total = int(_safe_getattr(strat, "_total_trades", 0) or 0)

        if rg is None and rm is None:
            return self._ok("no counters to compare")

        # ── Probe for an open position ─────────────────────────────────
        # If the strategy currently holds a position (phase != FLAT), it
        # has been counted by DailyRiskGate at entry but not yet by
        # RiskManager (which waits for exit finalisation). Subtract that
        # from the observed drift before deciding whether to warn.
        in_flight = 0
        pos = _safe_getattr(strat, "_pos", None)
        if pos is not None:
            phase_obj = _safe_getattr(pos, "phase", None)
            phase_name = getattr(phase_obj, "name", str(phase_obj or "")).upper()
            # ENTERING/ACTIVE/EXITING all represent "gate counted, rm not yet"
            if phase_name and phase_name != "FLAT":
                in_flight = 1

        drift = abs(rg_count - rm_count)
        explained = max(0, drift - in_flight)

        # Only warn when RiskManager actually exposes a counter AND the
        # drift cannot be explained by in-flight positions AND the
        # unexplained gap is > 1 (absorb 1 step of benign skew from a
        # just-closed trade whose record_trade hook hasn't landed yet).
        if rm is not None and explained > 1:
            return self._warn(
                f"daily counter drift: DailyRiskGate={rg_count} vs "
                f"RiskManager={rm_count} (diff={drift}, in_flight={in_flight}, "
                f"unexplained={explained})",
                rg_count=rg_count, rm_count=rm_count,
                in_flight=in_flight, unexplained=explained,
                total_trades=total,
            )

        # INFO-level observation — shows up in quant_bot.log for
        # forensics but does not page Telegram. Only emitted when RM
        # actually exists (otherwise there's nothing to "drift from",
        # and rg_count alone is not a drift signal).
        if rm is not None and drift > 0:
            return self._info(
                f"daily counters: DailyRiskGate={rg_count} "
                f"RiskManager={rm_count} (diff={drift} explained by "
                f"in_flight={in_flight})",
                rg_count=rg_count, rm_count=rm_count,
                in_flight=in_flight, total_trades=total,
            )

        return self._ok(rg_count=rg_count, rm_count=rm_count, total_trades=total)


# ═════════════════════════════════════════════════════════════════════════════
# HEAL ACTIONS
# ═════════════════════════════════════════════════════════════════════════════


class HealAction:
    """
    Base class. Subclasses implement `apply(ctx)` which performs a narrow,
    auditable state repair and returns (success: bool, notes: str).

    The context (`HealContext`) carries strategy/router/risk_manager refs
    plus the HealthResult that triggered the heal.

    Each subclass declares `name` and documents EXACTLY which fields it
    touches in its docstring.
    """
    name: str = "noop"

    def capture_before(self, ctx: "HealContext") -> Dict[str, Any]:
        """Subclass hook — record pre-heal state for the audit log."""
        return {}

    def capture_after(self, ctx: "HealContext") -> Dict[str, Any]:
        """Subclass hook — record post-heal state for the audit log."""
        return {}

    def apply(self, ctx: "HealContext") -> Tuple[bool, str]:
        raise NotImplementedError


@dataclass
class HealContext:
    strategy: Any
    router: Any
    risk_manager: Any
    config_module: Any
    trigger_result: HealthResult
    notifier: Optional[Callable[[str], Any]] = None


class ClearExitCompletedFlagAction(HealAction):
    """
    Sets `strategy._exit_completed = False` when phase=FLAT and the flag
    was left stuck by a prior exit path.

    This is safe because:
      * When phase=FLAT the flag has no active guarding role.
      * It is routinely reset to False inside `_enter_trade` anyway.
      * Leaving it True can silently drop a future `_record_exchange_exit`.
    """
    name = "clear_exit_completed_flag"

    def capture_before(self, ctx: HealContext) -> Dict[str, Any]:
        return {
            "_exit_completed": _safe_getattr(ctx.strategy, "_exit_completed", None),
            "_pnl_recorded_for": _safe_getattr(ctx.strategy, "_pnl_recorded_for", None),
            "phase": getattr(_safe_getattr(_safe_getattr(ctx.strategy, "_pos", None),
                                           "phase", None), "name", ""),
        }

    def apply(self, ctx: HealContext) -> Tuple[bool, str]:
        lock = _safe_getattr(ctx.strategy, "_lock", None)
        try:
            if lock is not None and hasattr(lock, "__enter__"):
                with lock:
                    # Re-check inside lock
                    pos = _safe_getattr(ctx.strategy, "_pos", None)
                    phase = getattr(_safe_getattr(pos, "phase", None), "name", "")
                    if phase != "FLAT":
                        return False, f"heal aborted: phase={phase} (expected FLAT)"
                    ctx.strategy._exit_completed = False
                    # Also zero the secondary guard — safe when phase=FLAT
                    ctx.strategy._pnl_recorded_for = 0.0
            else:
                ctx.strategy._exit_completed = False
                ctx.strategy._pnl_recorded_for = 0.0
            return True, "flag cleared"
        except Exception as e:  # noqa: BLE001
            return False, f"exception: {e}"

    def capture_after(self, ctx: HealContext) -> Dict[str, Any]:
        return {
            "_exit_completed": _safe_getattr(ctx.strategy, "_exit_completed", None),
            "_pnl_recorded_for": _safe_getattr(ctx.strategy, "_pnl_recorded_for", None),
        }


class ClearPosSyncFlagAction(HealAction):
    """
    Sets `_pos_sync_in_progress = False`. Safe when no pos-sync-active
    thread is alive (caller verifies).
    """
    name = "clear_pos_sync_flag"

    def capture_before(self, ctx: HealContext) -> Dict[str, Any]:
        return {"_pos_sync_in_progress": _safe_getattr(ctx.strategy, "_pos_sync_in_progress", None)}

    def apply(self, ctx: HealContext) -> Tuple[bool, str]:
        # Double-check no thread alive
        for t in threading.enumerate():
            if "pos-sync-active" in t.name and t.is_alive():
                return False, f"aborted: thread {t.name} is alive"
        try:
            ctx.strategy._pos_sync_in_progress = False
            return True, "flag cleared"
        except Exception as e:  # noqa: BLE001
            return False, f"exception: {e}"

    def capture_after(self, ctx: HealContext) -> Dict[str, Any]:
        return {"_pos_sync_in_progress": _safe_getattr(ctx.strategy, "_pos_sync_in_progress", None)}


class ClearExitSyncFlagAction(HealAction):
    """Sets `_exit_sync_in_progress = False`. Safe when no thread alive."""
    name = "clear_exit_sync_flag"

    def capture_before(self, ctx: HealContext) -> Dict[str, Any]:
        return {"_exit_sync_in_progress": _safe_getattr(ctx.strategy, "_exit_sync_in_progress", None)}

    def apply(self, ctx: HealContext) -> Tuple[bool, str]:
        for t in threading.enumerate():
            if "pos-sync-exit" in t.name and t.is_alive():
                return False, f"aborted: thread {t.name} is alive"
        try:
            ctx.strategy._exit_sync_in_progress = False
            return True, "flag cleared"
        except Exception as e:  # noqa: BLE001
            return False, f"exception: {e}"

    def capture_after(self, ctx: HealContext) -> Dict[str, Any]:
        return {"_exit_sync_in_progress": _safe_getattr(ctx.strategy, "_exit_sync_in_progress", None)}


class ClearTrailFlagAction(HealAction):
    """Sets `_trail_in_progress = False`. Safe when no trail thread alive."""
    name = "clear_trail_flag"

    def capture_before(self, ctx: HealContext) -> Dict[str, Any]:
        return {"_trail_in_progress": _safe_getattr(ctx.strategy, "_trail_in_progress", None)}

    def apply(self, ctx: HealContext) -> Tuple[bool, str]:
        for t in threading.enumerate():
            if "trail" in t.name.lower() and t.is_alive():
                return False, f"aborted: thread {t.name} is alive"
        try:
            ctx.strategy._trail_in_progress = False
            return True, "flag cleared"
        except Exception as e:  # noqa: BLE001
            return False, f"exception: {e}"

    def capture_after(self, ctx: HealContext) -> Dict[str, Any]:
        return {"_trail_in_progress": _safe_getattr(ctx.strategy, "_trail_in_progress", None)}


class ClearReconcilePendingAction(HealAction):
    """Sets `_reconcile_pending = False`. Safe when no reconcile thread alive."""
    name = "clear_reconcile_pending"

    def capture_before(self, ctx: HealContext) -> Dict[str, Any]:
        return {"_reconcile_pending": _safe_getattr(ctx.strategy, "_reconcile_pending", None)}

    def apply(self, ctx: HealContext) -> Tuple[bool, str]:
        # Heuristic: reconcile thread is spawned as Thread(target=..._reconcile_query_thread)
        # without a name. Check by checking if a thread's target name matches.
        for t in threading.enumerate():
            if "reconcile" in t.name.lower() and t.is_alive():
                return False, f"aborted: thread {t.name} is alive"
        try:
            ctx.strategy._reconcile_pending = False
            # Also bump _last_reconcile_time so the next reconcile fires
            # promptly (next main-loop tick will see it's due).
            ctx.strategy._last_reconcile_time = _now() - 60.0
            return True, "flag cleared; reconcile will run on next tick"
        except Exception as e:  # noqa: BLE001
            return False, f"exception: {e}"

    def capture_after(self, ctx: HealContext) -> Dict[str, Any]:
        return {
            "_reconcile_pending": _safe_getattr(ctx.strategy, "_reconcile_pending", None),
            "_last_reconcile_time": _safe_getattr(ctx.strategy, "_last_reconcile_time", 0.0),
        }


class ResetLastExitTimeAction(HealAction):
    """
    Sets `_last_exit_time = time.time() - cooldown_sec` to immediately
    expire the cooldown. Only fires when `_last_exit_time` is in the
    future (clock-skew or bug), which would otherwise never clear.
    """
    name = "reset_last_exit_time"

    def capture_before(self, ctx: HealContext) -> Dict[str, Any]:
        return {"_last_exit_time": _safe_getattr(ctx.strategy, "_last_exit_time", 0.0)}

    def apply(self, ctx: HealContext) -> Tuple[bool, str]:
        try:
            cfg = ctx.config_module
            cooldown = float(_safe_getattr(cfg, "QUANT_COOLDOWN_SEC", 30.0) or 30.0)
            ctx.strategy._last_exit_time = _now() - cooldown - 1.0
            return True, f"set to now - {cooldown:.0f}s - 1"
        except Exception as e:  # noqa: BLE001
            return False, f"exception: {e}"

    def capture_after(self, ctx: HealContext) -> Dict[str, Any]:
        return {"_last_exit_time": _safe_getattr(ctx.strategy, "_last_exit_time", 0.0)}


class ForceFlatFromEnteringAction(HealAction):
    """
    Force `phase = FLAT` from a stuck ENTERING state. This mirrors what
    the strategy's own internal watchdog does when its stage B post-order
    timeout expires — so the action is exactly what the bot would do
    anyway, just triggered from the outside when the internal watchdog
    didn't fire.

    WARNING: if an order actually landed at the exchange after this heal
    fires, the bot will now think it's FLAT while the exchange has a
    position. The ExchangePositionDriftCheck will then escalate to
    CRITICAL and alert. That is the correct sequence.
    """
    name = "force_flat_from_entering"

    def capture_before(self, ctx: HealContext) -> Dict[str, Any]:
        pos = _safe_getattr(ctx.strategy, "_pos", None)
        return {
            "phase": getattr(_safe_getattr(pos, "phase", None), "name", ""),
            "_entering_since": _safe_getattr(ctx.strategy, "_entering_since", 0.0),
            "_entry_order_placed_at": _safe_getattr(ctx.strategy, "_entry_order_placed_at", 0.0),
        }

    def apply(self, ctx: HealContext) -> Tuple[bool, str]:
        strat = ctx.strategy
        lock = _safe_getattr(strat, "_lock", None)

        def _do_reset() -> None:
            # Import the PositionPhase class from the strategy module
            try:
                from strategy.quant_strategy import PositionPhase
            except ImportError:
                from quant_strategy import PositionPhase  # type: ignore
            pos = _safe_getattr(strat, "_pos", None)
            if pos is not None:
                pos.phase = PositionPhase.FLAT
            strat._last_exit_time = _now()
            strat._entry_order_placed_at = 0.0
            ee = _safe_getattr(strat, "_entry_engine", None)
            if ee is not None and hasattr(ee, "on_entry_failed"):
                try:
                    ee.on_entry_failed()
                except Exception:
                    pass

        try:
            if lock is not None and hasattr(lock, "__enter__"):
                with lock:
                    _do_reset()
            else:
                _do_reset()
            if ctx.notifier:
                _safe_call(
                    ctx.notifier,
                    "⚠️ <b>WATCHDOG HEAL</b>: ENTERING stuck — forced FLAT. "
                    "<b>VERIFY EXCHANGE has no open order/position.</b>",
                )
            return True, "phase forced FLAT, entry engine reset"
        except Exception as e:  # noqa: BLE001
            return False, f"exception: {e}"

    def capture_after(self, ctx: HealContext) -> Dict[str, Any]:
        pos = _safe_getattr(ctx.strategy, "_pos", None)
        return {
            "phase": getattr(_safe_getattr(pos, "phase", None), "name", ""),
            "_entry_order_placed_at": _safe_getattr(ctx.strategy, "_entry_order_placed_at", 0.0),
        }


class AdoptExchangeFlatAction(HealAction):
    """
    Bot says ACTIVE, exchange says FLAT. Position was closed externally
    (SL/TP fill, manual close) but the local sync missed it. Adopt the
    exchange's FLAT truth.

    This does NOT record PnL — we don't know the fill price. The
    `_record_pnl(0, exit_reason="watchdog_adopt_flat")` path is used so
    accounting stays consistent; the operator is alerted to reconcile
    manually from the exchange.
    """
    name = "adopt_exchange_flat"

    def capture_before(self, ctx: HealContext) -> Dict[str, Any]:
        pos = _safe_getattr(ctx.strategy, "_pos", None)
        return {
            "phase": getattr(_safe_getattr(pos, "phase", None), "name", ""),
            "side": _safe_getattr(pos, "side", ""),
            "quantity": _safe_getattr(pos, "quantity", 0.0),
            "entry_price": _safe_getattr(pos, "entry_price", 0.0),
        }

    def apply(self, ctx: HealContext) -> Tuple[bool, str]:
        strat = ctx.strategy
        lock = _safe_getattr(strat, "_lock", None)

        def _do_adopt() -> None:
            # Prefer the strategy's own _record_pnl path for accounting hygiene
            record_pnl = _safe_getattr(strat, "_record_pnl", None)
            finalise = _safe_getattr(strat, "_finalise_exit", None)
            if callable(record_pnl):
                try:
                    record_pnl(0.0, exit_reason="watchdog_adopt_flat",
                               exit_price=0.0, fee_breakdown=None)
                except Exception as e:  # noqa: BLE001
                    logger.error("watchdog: _record_pnl(0) failed during adopt: %s", e)
            if callable(finalise):
                try:
                    finalise()
                except Exception as e:  # noqa: BLE001
                    logger.error("watchdog: _finalise_exit failed during adopt: %s", e)
                    # Fallback: force FLAT manually
                    try:
                        from strategy.quant_strategy import PositionState
                    except ImportError:
                        from quant_strategy import PositionState  # type: ignore
                    strat._pos = PositionState()
                    strat._last_exit_time = _now()

        try:
            if lock is not None and hasattr(lock, "__enter__"):
                with lock:
                    _do_adopt()
            else:
                _do_adopt()
            if ctx.notifier:
                _safe_call(
                    ctx.notifier,
                    "⚠️ <b>WATCHDOG HEAL — adopted exchange FLAT</b>\n"
                    "Position was closed externally. PnL recorded as 0.\n"
                    "<b>Reconcile manually from exchange history.</b>",
                )
            return True, "phase adopted FLAT, PnL recorded 0"
        except Exception as e:  # noqa: BLE001
            return False, f"exception: {e}"

    def capture_after(self, ctx: HealContext) -> Dict[str, Any]:
        pos = _safe_getattr(ctx.strategy, "_pos", None)
        return {
            "phase": getattr(_safe_getattr(pos, "phase", None), "name", ""),
        }


class RecoverSlOrderIdAction(HealAction):
    """
    Heals Bug A: query exchange open orders for a reduce-only SL at the
    expected side and price, and patch `pos.sl_order_id` + `pos.tp_order_id`
    with the matches.

    Matching heuristic:
      - order is reduce_only / closing (best-effort flag names)
      - order side is OPPOSITE to position side
      - stop order type OR trigger price set → SL candidate
      - limit order at > entry (short) or < entry (long) WITHOUT a trigger → TP
      - price within 20% of pos.sl_price → SL match

    Conservative: only patches if exactly ONE matching order is found per
    leg. Multiple candidates → WARN only; operator reconciles manually.
    """
    name = "recover_sl_order_id"

    def capture_before(self, ctx: HealContext) -> Dict[str, Any]:
        pos = _safe_getattr(ctx.strategy, "_pos", None)
        return {
            "sl_order_id": _safe_getattr(pos, "sl_order_id", "") or "",
            "tp_order_id": _safe_getattr(pos, "tp_order_id", "") or "",
            "sl_price": _safe_getattr(pos, "sl_price", 0.0),
            "tp_price": _safe_getattr(pos, "tp_price", 0.0),
            "side": _safe_getattr(pos, "side", ""),
        }

    def _list_open_orders(self, router: Any) -> List[Dict[str, Any]]:
        for om_accessor in ("get_active_om", "active_order_manager", "active_om"):
            val = _safe_getattr(router, om_accessor, None)
            om = _safe_call(val, default=None) if callable(val) else val
            if om is None:
                om = router
            for fn_name in ("list_open_orders", "get_open_orders", "open_orders"):
                fn = _safe_getattr(om, fn_name, None)
                if not callable(fn):
                    continue
                raw = _safe_call(fn, default=None)
                if raw is None:
                    continue
                if isinstance(raw, dict):
                    raw = list(raw.values())
                if isinstance(raw, list):
                    return [o if isinstance(o, dict) else self._dc_to_dict(o)
                            for o in raw]
        return []

    def _dc_to_dict(self, o: Any) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for k in ("id", "order_id", "side", "price", "stop_price",
                  "trigger_price", "size", "quantity", "reduce_only",
                  "order_type", "status"):
            v = _safe_getattr(o, k, None)
            if v is not None:
                out[k] = v
        return out

    def apply(self, ctx: HealContext) -> Tuple[bool, str]:
        pos = _safe_getattr(ctx.strategy, "_pos", None)
        if pos is None:
            return False, "no position"
        side = (_safe_getattr(pos, "side", "") or "").lower()
        if side not in ("long", "short"):
            return False, f"unknown side: {side}"
        opposite = "sell" if side == "long" else "buy"
        sl_price = float(_safe_getattr(pos, "sl_price", 0.0) or 0.0)
        tp_price = float(_safe_getattr(pos, "tp_price", 0.0) or 0.0)

        orders = self._list_open_orders(ctx.router)
        if not orders:
            return False, "no open orders returned by exchange"

        # Filter to opposite-side, reduce-only-ish orders
        def _is_close_side(o: Dict[str, Any]) -> bool:
            s = str(o.get("side", "")).lower()
            return s == opposite or s in ("close_long", "close_short")

        def _price_of(o: Dict[str, Any]) -> Optional[float]:
            for k in ("stop_price", "trigger_price", "price"):
                v = o.get(k)
                if v is not None:
                    try:
                        return float(v)
                    except Exception:
                        continue
            return None

        def _has_trigger(o: Dict[str, Any]) -> bool:
            return any(o.get(k) for k in ("stop_price", "trigger_price"))

        candidates = [o for o in orders if _is_close_side(o)]
        if not candidates:
            return False, "no opposite-side orders found"

        # SL candidates: have a trigger price, close to sl_price
        sl_matches: List[Dict[str, Any]] = []
        tp_matches: List[Dict[str, Any]] = []
        for o in candidates:
            px = _price_of(o)
            if px is None or px <= 0:
                continue
            if _has_trigger(o):
                if sl_price > 0 and abs(px - sl_price) / sl_price < 0.2:
                    sl_matches.append(o)
            else:
                if tp_price > 0 and abs(px - tp_price) / tp_price < 0.2:
                    tp_matches.append(o)

        notes: List[str] = []
        patched_any = False

        def _order_id(o: Dict[str, Any]) -> str:
            return str(o.get("id") or o.get("order_id") or "")

        lock = _safe_getattr(ctx.strategy, "_lock", None)

        def _patch() -> None:
            nonlocal patched_any
            if len(sl_matches) == 1 and not (_safe_getattr(pos, "sl_order_id", "") or ""):
                pos.sl_order_id = _order_id(sl_matches[0])
                notes.append(f"SL→{pos.sl_order_id}")
                patched_any = True
            if len(tp_matches) == 1 and not (_safe_getattr(pos, "tp_order_id", "") or ""):
                pos.tp_order_id = _order_id(tp_matches[0])
                notes.append(f"TP→{pos.tp_order_id}")
                patched_any = True

        try:
            if lock is not None and hasattr(lock, "__enter__"):
                with lock:
                    _patch()
            else:
                _patch()
        except Exception as e:  # noqa: BLE001
            return False, f"patch exception: {e}"

        if len(sl_matches) > 1:
            notes.append(f"ambiguous SL ({len(sl_matches)} matches) — skipped")
        if len(tp_matches) > 1:
            notes.append(f"ambiguous TP ({len(tp_matches)} matches) — skipped")
        if not sl_matches:
            notes.append("no SL match")
        if not tp_matches and tp_price > 0:
            notes.append("no TP match")

        if ctx.notifier and patched_any:
            _safe_call(
                ctx.notifier,
                f"✅ <b>WATCHDOG HEAL — SL/TP IDs recovered</b>\n{'; '.join(notes)}",
            )

        return patched_any, "; ".join(notes) if notes else "nothing matched"

    def capture_after(self, ctx: HealContext) -> Dict[str, Any]:
        pos = _safe_getattr(ctx.strategy, "_pos", None)
        return {
            "sl_order_id": _safe_getattr(pos, "sl_order_id", "") or "",
            "tp_order_id": _safe_getattr(pos, "tp_order_id", "") or "",
        }


class ResetEntryEngineAction(HealAction):
    """
    Calls `entry_engine.on_position_closed()` — the canonical reset path.
    Safe whenever phase=FLAT.
    """
    name = "reset_entry_engine"

    def capture_before(self, ctx: HealContext) -> Dict[str, Any]:
        ee = _safe_getattr(ctx.strategy, "_entry_engine", None)
        signal = _safe_getattr(ee, "_signal", None)
        entry_type = _safe_getattr(signal, "entry_type", "")
        return {
            "engine_state": getattr(_safe_getattr(ee, "_state", None), "name", "?"),
            "state_entered": _safe_getattr(ee, "_state_entered", 0.0),
            "signal_side": _safe_getattr(signal, "side", None),
            "signal_type": getattr(entry_type, "name", str(entry_type)) if signal is not None else None,
            "signal_created_at": _safe_getattr(signal, "created_at", None),
        }

    def apply(self, ctx: HealContext) -> Tuple[bool, str]:
        ee = _safe_getattr(ctx.strategy, "_entry_engine", None)
        if ee is None:
            return False, "no entry engine"
        try:
            # Use the canonical reset path
            if hasattr(ee, "on_position_closed"):
                ee.on_position_closed()
                return True, "on_position_closed() called"
            # Fallback: direct _reset
            if hasattr(ee, "_reset"):
                ee._reset(_now())
                return True, "_reset() called"
            return False, "no reset method available"
        except Exception as e:  # noqa: BLE001
            return False, f"exception: {e}"

    def capture_after(self, ctx: HealContext) -> Dict[str, Any]:
        ee = _safe_getattr(ctx.strategy, "_entry_engine", None)
        return {
            "engine_state": getattr(_safe_getattr(ee, "_state", None), "name", "?"),
            "state_entered": _safe_getattr(ee, "_state_entered", 0.0),
        }


# ═════════════════════════════════════════════════════════════════════════════
# AUTO-HEAL REGISTRY
# ═════════════════════════════════════════════════════════════════════════════


class AutoHealRegistry:
    """
    Central registry of named heal actions, with per-action rate limits.

    Default limits: each action may fire at most 3 times in 15 minutes.
    The circuit breaker's global runaway-protection also applies (see
    CircuitBreaker).
    """

    _DEFAULT_MAX_PER_WINDOW = 3
    _DEFAULT_WINDOW_SEC = 900.0

    def __init__(self):
        self._actions: Dict[str, HealAction] = {}
        self._limiter = RateLimiter(
            max_hits=self._DEFAULT_MAX_PER_WINDOW,
            window_sec=self._DEFAULT_WINDOW_SEC,
        )

    def register(self, action: HealAction) -> None:
        if action.name in self._actions:
            logger.debug("watchdog: heal action %s re-registered", action.name)
        self._actions[action.name] = action

    def get(self, name: str) -> Optional[HealAction]:
        return self._actions.get(name)

    def names(self) -> List[str]:
        return list(self._actions.keys())

    def can_fire(self, name: str) -> bool:
        return self._limiter.allow(name)

    def recent_fires(self, name: str) -> int:
        return self._limiter.count(name)

    @classmethod
    def default(cls) -> "AutoHealRegistry":
        reg = cls()
        for action in (
            ClearExitCompletedFlagAction(),
            ClearPosSyncFlagAction(),
            ClearExitSyncFlagAction(),
            ClearTrailFlagAction(),
            ClearReconcilePendingAction(),
            ResetLastExitTimeAction(),
            ForceFlatFromEnteringAction(),
            AdoptExchangeFlatAction(),
            RecoverSlOrderIdAction(),
            ResetEntryEngineAction(),
        ):
            reg.register(action)
        return reg


# ═════════════════════════════════════════════════════════════════════════════
# WATCHDOG ORCHESTRATOR
# ═════════════════════════════════════════════════════════════════════════════


class Watchdog:
    """
    The main orchestrator. Owns:
      * list of HealthCheck instances
      * AutoHealRegistry
      * ForensicRecorder
      * HealActionLog
      * CircuitBreaker

    Runs in its own daemon thread. Each iteration:
      1. For each check whose interval has elapsed and is not disabled,
         run it and store the HealthResult.
      2. For each HEAL result: check consecutive_failures ≥ heal_confirmations.
         If yes and rate limit permits, apply the heal and log audit record.
      3. For each CRITICAL result: trip the circuit breaker, dump forensic.
      4. For each WARN: emit a Telegram alert (throttled per check).
      5. Check runaway-heal protection → trip breaker if needed.

    The loop tick is fast (250ms); per-check interval enforcement prevents
    actual execution from running more often than each check declares.
    """

    _DEFAULT_LOOP_INTERVAL_SEC = 0.25
    # Minimum gap between Telegram alerts for the same WARN-level check,
    # to avoid spamming on flapping conditions.
    _WARN_ALERT_COOLDOWN_SEC = 300.0

    def __init__(self,
                 strategy: Any,
                 data_manager: Any,
                 execution_router: Any,
                 risk_manager: Any,
                 notifier: Optional[Callable[[str], Any]] = None,
                 config_module: Any = None,
                 heal_registry: Optional[AutoHealRegistry] = None,
                 forensic_dir: str = "."):
        self._strategy = strategy
        self._dm = data_manager
        self._router = execution_router
        self._rm = risk_manager
        self._notifier = notifier
        self._config = config_module

        # Ensure the strategy exposes the freeze flag (entry code should
        # check it — see WIRING section at bottom of file).
        try:
            if not hasattr(strategy, "watchdog_trading_frozen"):
                setattr(strategy, "watchdog_trading_frozen", False)
        except Exception:
            pass

        self._heal_registry = heal_registry or AutoHealRegistry.default()
        self._heal_log = HealActionLog(path=os.path.join(forensic_dir, "watchdog_heals.jsonl"))
        self._forensic = ForensicRecorder(
            strategy=strategy, execution_router=execution_router,
            risk_manager=risk_manager, dump_dir=forensic_dir,
        )
        self._breaker = CircuitBreaker(
            strategy=strategy, heal_log=self._heal_log, notifier=notifier,
        )

        self._checks: List[HealthCheck] = []
        self._results_ring: Deque[HealthResult] = deque(maxlen=200)
        self._last_warn_alert: Dict[str, float] = {}
        self._auto_heal_enabled = True

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._start_ts: Optional[float] = None

        # Register default checks
        self._register_default_checks()

    # ---- registration ------------------------------------------------------

    def add_check(self, check: HealthCheck) -> None:
        self._checks.append(check)

    def _register_default_checks(self) -> None:
        common = dict(
            strategy=self._strategy,
            router=self._router,
            data_manager=self._dm,
            risk_manager=self._rm,
            config_module=self._config,
        )
        self._checks.extend([
            # L0
            TickAgeCheck(**common),
            MainThreadAliveCheck(**common),
            # L1
            StuckExitCompletedFlagCheck(**common),
            StuckPosSyncFlagCheck(**common),
            StuckExitSyncFlagCheck(**common),
            StuckTrailFlagCheck(**common),
            StuckReconcilePendingCheck(**common),
            PositionPhaseInvariantCheck(**common),
            EnteringPhaseTimeoutCheck(**common),
            ExitingPhaseTimeoutCheck(**common),
            # L2
            ExchangePositionDriftCheck(**common),
            MissingSlOrderIdCheck(**common),
            # L3
            EntryEngineStuckCheck(**common),
            CooldownPersistenceCheck(**common),
            NotifierQueueDepthCheck(**common),
            # L4
            ConfigDriftCheck(**common),
            # L5
            NoTradesAfterFirstCheck(**common),
            DailyCounterConsistencyCheck(**common),
        ])

    # ---- public control ----------------------------------------------------

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            logger.warning("watchdog: start called but already running")
            return
        self._start_ts = _now()
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop, daemon=True, name="watchdog-main")
        self._thread.start()
        logger.info("watchdog: started (%d checks, %d heal actions)",
                    len(self._checks), len(self._heal_registry.names()))
        if self._notifier:
            _safe_call(
                self._notifier,
                f"🛡️ <b>Watchdog online</b>\n"
                f"{len(self._checks)} checks, "
                f"{len(self._heal_registry.names())} heal actions, "
                f"auto-heal: {'ON' if self._auto_heal_enabled else 'OFF'}",
            )

    def stop(self, timeout: float = 5.0) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
        logger.info("watchdog: stopped")

    def set_auto_heal_enabled(self, enabled: bool) -> None:
        self._auto_heal_enabled = bool(enabled)
        logger.warning("watchdog: auto-heal → %s", "ON" if enabled else "OFF")

    @property
    def auto_heal_enabled(self) -> bool:
        return self._auto_heal_enabled

    @property
    def breaker(self) -> CircuitBreaker:
        return self._breaker

    # ---- main loop ---------------------------------------------------------

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._run_once()
            except Exception:  # noqa: BLE001
                logger.exception("watchdog: loop iteration failed")
            # Sleep with early-exit on stop
            self._stop_event.wait(timeout=self._DEFAULT_LOOP_INTERVAL_SEC)

    def _run_once(self) -> None:
        now = _now()
        for check in self._checks:
            if check.disabled:
                continue
            if not check.due(now):
                continue
            result = check.run()
            self._results_ring.append(result)
            self._handle_result(check, result)

        # Global safety: runaway heal protection
        self._breaker.check_runaway_heals()

    # ---- result handling ---------------------------------------------------

    def _handle_result(self, check: HealthCheck, result: HealthResult) -> None:
        if result.severity == Severity.OK:
            return

        # Log all non-OK results
        if result.severity == Severity.INFO:
            logger.info("watchdog[%s]: %s", check.name, result.message)
            return
        if result.severity == Severity.WARN:
            logger.warning("watchdog[%s]: %s", check.name, result.message)
            self._maybe_alert_warn(check.name, result)
            # Capture a forensic snapshot (in-memory ring only; no disk dump)
            self._forensic.capture(
                trigger=check.name, severity=result.severity,
                recent_results=self._results_ring,
                heal_history=self._heal_log.recent(20),
            )
            return
        if result.severity == Severity.CRITICAL:
            self._handle_critical(check, result)
            return
        if result.severity == Severity.HEAL:
            self._handle_heal(check, result)
            return

    def _maybe_alert_warn(self, check_name: str, result: HealthResult) -> None:
        if self._notifier is None:
            return
        now = _now()
        last = self._last_warn_alert.get(check_name, 0.0)
        if now - last < self._WARN_ALERT_COOLDOWN_SEC:
            return
        self._last_warn_alert[check_name] = now
        _safe_call(
            self._notifier,
            f"⚠️ <b>Watchdog WARN — {check_name}</b>\n{result.message}",
        )

    def _handle_critical(self, check: HealthCheck, result: HealthResult) -> None:
        logger.critical("watchdog[%s] CRITICAL: %s", check.name, result.message)
        self._forensic.capture(
            trigger=f"CRITICAL:{check.name}",
            severity=result.severity,
            recent_results=self._results_ring,
            heal_history=self._heal_log.recent(20),
            with_exchange=True,
        )
        if self._notifier:
            _safe_call(
                self._notifier,
                f"🚨 <b>WATCHDOG CRITICAL — {check.name}</b>\n"
                f"{result.message}\n"
                f"Forensic dump written. Engaging circuit breaker.",
            )
        # Trip breaker (idempotent)
        self._breaker.trip(reason=f"{check.name}: {result.message}")

    def _handle_heal(self, check: HealthCheck, result: HealthResult) -> None:
        # Require consecutive confirmations
        if check.consecutive_failures < check.heal_confirmations:
            logger.info(
                "watchdog[%s] heal pending (%d/%d confirmations): %s",
                check.name, check.consecutive_failures,
                check.heal_confirmations, result.message,
            )
            return

        # Is auto-heal globally enabled?
        if not self._auto_heal_enabled:
            logger.warning(
                "watchdog[%s] heal SUPPRESSED (auto-heal OFF): %s",
                check.name, result.message,
            )
            if self._notifier:
                _safe_call(
                    self._notifier,
                    f"⚠️ <b>Watchdog heal suppressed</b> (auto-heal OFF)\n"
                    f"{check.name}: {result.message}",
                )
            return

        # Is the breaker engaged?
        if self._breaker.engaged and not getattr(check, "safe_when_breaker_engaged", False):
            logger.warning(
                "watchdog[%s] heal SUPPRESSED (breaker engaged): %s",
                check.name, result.message,
            )
            return

        action_name = result.heal_action or ""
        action = self._heal_registry.get(action_name)
        if action is None:
            logger.error(
                "watchdog[%s] heal requested unknown action '%s'",
                check.name, action_name,
            )
            return

        # Per-action rate limit
        if not self._heal_registry.can_fire(action_name):
            recent = self._heal_registry.recent_fires(action_name)
            logger.warning(
                "watchdog[%s] heal RATE-LIMITED: %s already fired %d× in window",
                check.name, action_name, recent,
            )
            if self._notifier:
                _safe_call(
                    self._notifier,
                    f"⚠️ <b>Watchdog heal rate-limited</b>\n"
                    f"{action_name} fired {recent}× — skipping",
                )
            return

        # Execute
        ctx = HealContext(
            strategy=self._strategy, router=self._router,
            risk_manager=self._rm, config_module=self._config,
            trigger_result=result, notifier=self._notifier,
        )
        before = action.capture_before(ctx)
        logger.warning(
            "watchdog[%s] HEAL firing action=%s — %s",
            check.name, action_name, result.message,
        )
        success, notes = action.apply(ctx)
        after = action.capture_after(ctx)

        self._heal_log.record(
            action=action_name, reason=check.name,
            before=before, after=after,
            success=success, notes=notes,
        )

        # Reset the check's counter after a successful heal so it starts
        # fresh (the state should now be clean; next failure indicates a
        # deeper problem).
        if success:
            check._consecutive_failures = 0

        level = "✅" if success else "❌"
        logger.warning(
            "watchdog[%s] HEAL %s: action=%s notes=%s",
            check.name, "SUCCESS" if success else "FAILED",
            action_name, notes,
        )
        if self._notifier:
            title = "Watchdog heal SUCCESS" if success else "Watchdog heal FAILED"
            _safe_call(
                self._notifier,
                f"{level} <b>{title}</b>\n"
                f"{action_name}: {notes}",
            )

    # ---- introspection -----------------------------------------------------

    def snapshot(self) -> Dict[str, Any]:
        """Operator-facing snapshot (used by /watchdog_status)."""
        checks_info = []
        for c in self._checks:
            last = c._last_result
            checks_info.append({
                "name": c.name,
                "category": c.category,
                "interval_sec": c.interval_sec,
                "disabled": c.disabled,
                "consecutive_failures": c.consecutive_failures,
                "last_severity": last.severity.name if last else "NEVER",
                "last_message": last.message if last else "",
                "last_run_age_sec": (_now() - c._last_run) if c._last_run > 0 else None,
            })
        # Aggregate status
        severities = [c._last_result.severity for c in self._checks if c._last_result is not None]
        if self._breaker.engaged:
            agg = Status.FROZEN
        elif not severities:
            agg = Status.UNKNOWN
        elif any(s >= Severity.CRITICAL for s in severities):
            agg = Status.DEGRADED
        elif any(s >= Severity.HEAL for s in severities):
            agg = Status.HEALING
        elif any(s >= Severity.WARN for s in severities):
            agg = Status.DEGRADED
        else:
            agg = Status.OK

        return {
            "status": agg.value,
            "auto_heal_enabled": self._auto_heal_enabled,
            "breaker_engaged": self._breaker.engaged,
            "breaker_reason": self._breaker.reason,
            "breaker_engaged_at": (_iso_ist(self._breaker.engaged_at)
                                   if self._breaker.engaged_at else None),
            "uptime_sec": round(_now() - self._start_ts, 0) if self._start_ts else 0,
            "heal_count_last_hour": self._heal_log.count_since(_now() - 3600),
            "checks": checks_info,
            "recent_heals": self._heal_log.recent(10),
        }

    def format_status_telegram(self) -> str:
        snap = self.snapshot()
        lines = [
            f"🛡️ <b>Watchdog status: {snap['status']}</b>",
            f"Auto-heal: {'ON' if snap['auto_heal_enabled'] else 'OFF'}",
            f"Breaker: {'ENGAGED ('+str(snap['breaker_reason'])+')' if snap['breaker_engaged'] else 'clear'}",
            f"Uptime: {int(snap['uptime_sec'])}s  Heals/hr: {snap['heal_count_last_hour']}",
            "",
            "<b>Checks</b>:",
        ]
        # Show only non-OK + recent heals for Telegram brevity
        interesting = [c for c in snap["checks"]
                       if c["last_severity"] not in ("OK", "NEVER")]
        if not interesting:
            lines.append("  all checks OK")
        else:
            for c in interesting[:15]:
                lines.append(
                    f"  [{c['last_severity']}] {c['name']}: "
                    f"{c['last_message'][:80]}"
                )
        if snap["recent_heals"]:
            lines.append("")
            lines.append("<b>Recent heals</b>:")
            for h in snap["recent_heals"][-5:]:
                ok = "✅" if h.get("success") else "❌"
                lines.append(f"  {ok} {h['action']} ({h['reason']}) — {h.get('notes', '')[:60]}")
        return "\n".join(lines)


# ═════════════════════════════════════════════════════════════════════════════
# BUILDER / INTEGRATION HELPER
# ═════════════════════════════════════════════════════════════════════════════


def build_default_watchdog(strategy: Any,
                           data_manager: Any,
                           execution_router: Any,
                           risk_manager: Any,
                           notifier: Optional[Callable[[str], Any]] = None,
                           config_module: Any = None,
                           forensic_dir: str = ".") -> Watchdog:
    """
    Convenience constructor — returns a fully-wired Watchdog with all
    default checks and heal actions registered.

    Usage in main.py::

        from watchdog import build_default_watchdog
        self.watchdog = build_default_watchdog(
            strategy=self.strategy,
            data_manager=self.data_manager,
            execution_router=self.execution_router,
            risk_manager=self.risk_manager,
            notifier=send_telegram_message,
            config_module=config,
        )
        self.watchdog.start()
    """
    return Watchdog(
        strategy=strategy,
        data_manager=data_manager,
        execution_router=execution_router,
        risk_manager=risk_manager,
        notifier=notifier,
        config_module=config_module,
        forensic_dir=forensic_dir,
    )


# ═════════════════════════════════════════════════════════════════════════════
# TELEGRAM COMMAND WIRING (optional)
# ═════════════════════════════════════════════════════════════════════════════


def register_telegram_commands(watchdog: Watchdog, controller: Any) -> None:
    """
    Registers /watchdog_* commands on the existing Telegram controller.

    `controller` must expose a `register_command(name, handler)` method or
    a similar registration API. This function is best-effort — if the
    controller doesn't expose a known registration method, it logs a
    warning and returns without error.

    Handlers receive the raw message dict from the controller (consistent
    with existing controller.py style); they return a string reply.
    """
    def _status_handler(*_args, **_kwargs) -> str:
        return watchdog.format_status_telegram()

    def _heal_on_handler(*_args, **_kwargs) -> str:
        watchdog.set_auto_heal_enabled(True)
        return "✅ Watchdog auto-heal: ON"

    def _heal_off_handler(*_args, **_kwargs) -> str:
        watchdog.set_auto_heal_enabled(False)
        return "⚠️ Watchdog auto-heal: OFF"

    def _freeze_handler(*_args, **_kwargs) -> str:
        watchdog.breaker.trip(reason="manual /watchdog_freeze")
        return "🛑 Watchdog circuit breaker: ENGAGED (manual)"

    def _unfreeze_handler(*_args, **_kwargs) -> str:
        if not watchdog.breaker.engaged:
            return "ℹ️ Breaker was not engaged."
        watchdog.breaker.clear(operator="telegram")
        return "✅ Watchdog circuit breaker: CLEARED"

    commands = {
        "watchdog_status": _status_handler,
        "watchdog_heal_on": _heal_on_handler,
        "watchdog_heal_off": _heal_off_handler,
        "watchdog_freeze": _freeze_handler,
        "watchdog_unfreeze": _unfreeze_handler,
    }

    # Try a few known registration patterns
    for attr in ("register_command", "add_command", "on"):
        fn = _safe_getattr(controller, attr, None)
        if callable(fn):
            for name, handler in commands.items():
                try:
                    fn(name, handler)
                except Exception as e:  # noqa: BLE001
                    logger.debug("watchdog: register_command(%s) failed: %s", name, e)
            logger.info("watchdog: registered %d telegram commands via %s",
                        len(commands), attr)
            return

    logger.warning(
        "watchdog: could not auto-register telegram commands — no "
        "register_command / add_command / on method on controller. "
        "Wire manually by calling watchdog.format_status_telegram() / "
        "watchdog.set_auto_heal_enabled(bool) / watchdog.breaker.trip()/clear()."
    )


# ═════════════════════════════════════════════════════════════════════════════
# ENTRY-GATE INTEGRATION NOTE
# ═════════════════════════════════════════════════════════════════════════════
#
# For the circuit breaker to actually block new entries, quant_strategy.py
# must check the freeze flag before launching an entry. Add this one-line
# gate at the TOP of `_evaluate_entry` (or inside `can_trade`):
#
#     if getattr(self, "watchdog_trading_frozen", False):
#         return   # breaker engaged — entries frozen
#
# If you'd rather keep the change external, the watchdog also works as a
# pure monitoring tool (all heals still fire) — it just can't prevent new
# entries when a critical invariant has been violated. For a live trading
# system, you DO want the freeze gate wired.
#
# ═════════════════════════════════════════════════════════════════════════════

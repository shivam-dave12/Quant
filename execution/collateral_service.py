"""Shared, freshness-gated broker collateral snapshots.

Market evaluation must never call broker balance endpoints.  This service owns
a single refresh schedule per collateral authority and publishes immutable,
timestamped snapshots to every strategy context.  Per-contract strategy workers
remain concurrent; only state refresh is centralised and rate-limit aware.
"""
from __future__ import annotations

from dataclasses import dataclass
import logging
import threading
import time
from typing import Any

try:
    import config
except Exception:  # pragma: no cover
    config = None  # type: ignore

logger = logging.getLogger(__name__)


def _cfg(name: str, default: Any) -> Any:
    return getattr(config, name, default) if config is not None else default


@dataclass(frozen=True)
class CollateralSnapshot:
    authority_key: str
    venue: str
    available: float
    source: str
    updated_monotonic: float
    verified: bool = True
    error: str = ""

    def age_sec(self) -> float:
        return max(0.0, time.monotonic() - self.updated_monotonic)


class BrokerCollateralSnapshotService:
    """Deduplicated broker-state refresh with stale-data rejection and backoff."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._sources: dict[str, tuple[str, Any]] = {}
        self._router_keys: dict[tuple[int, str], str] = {}
        self._snapshots: dict[str, CollateralSnapshot] = {}
        self._next_due: dict[str, float] = {}
        self._failures: dict[str, int] = {}
        self._last_warning: dict[str, float] = {}
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    @staticmethod
    def _authority_key(venue: str, manager: Any) -> str:
        venue_key = str(venue).lower()
        if venue_key != "hyperliquid":
            return venue_key
        adapter = getattr(manager, "_adapter", manager)
        symbol = str(getattr(adapter, "symbol", "") or "")
        dex = symbol.split(":", 1)[0].lower() if ":" in symbol else "main"
        return f"hyperliquid:{dex}"

    def register_router(self, router: Any) -> None:
        getter = getattr(router, "available_exchanges", None)
        manager_for = getattr(router, "manager_for", None)
        if not callable(getter) or not callable(manager_for):
            return
        try:
            venues = tuple(str(v).lower() for v in getter())
        except Exception:
            return
        with self._lock:
            for venue in venues:
                try:
                    manager = manager_for(venue)
                except Exception:
                    continue
                key = self._authority_key(venue, manager)
                self._router_keys[(id(router), venue)] = key
                # Any manager attached to the same collateral authority can refresh it.
                # Keep the first to prevent per-instrument duplicate calls.
                self._sources.setdefault(key, (venue, manager))
                self._next_due.setdefault(key, 0.0)
        self._wake.set()

    def start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(target=self._run, name="broker-collateral-snapshot-service", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()

    @staticmethod
    def _interval(key: str) -> float:
        if key == "coinswitch":
            return max(15.0, float(_cfg("BROKER_COLLATERAL_REFRESH_COINSWITCH_SEC", 45.0)))
        if key.startswith("hyperliquid:"):
            return max(10.0, float(_cfg("BROKER_COLLATERAL_REFRESH_HYPERLIQUID_SEC", 20.0)))
        if key == "delta":
            return max(5.0, float(_cfg("BROKER_COLLATERAL_REFRESH_DELTA_SEC", 15.0)))
        return max(15.0, float(_cfg("BROKER_COLLATERAL_REFRESH_DEFAULT_SEC", 30.0)))

    @staticmethod
    def _valid_balance(balance: Any) -> tuple[bool, float, str, str]:
        if not isinstance(balance, dict):
            return False, 0.0, "invalid_balance_payload", "invalid_balance_payload"
        source = str(balance.get("source") or "broker_balance")
        if balance.get("error"):
            return False, 0.0, source, str(balance.get("error"))
        if balance.get("balance_verified") is False:
            return False, 0.0, source, str(balance.get("reason") or "balance_not_verified")
        try:
            available = max(0.0, float(balance.get("available", 0.0) or 0.0))
        except Exception:
            return False, 0.0, source, "available_balance_not_numeric"
        return True, available, source, ""

    def _warn_throttled(self, key: str, message: str) -> None:
        now = time.monotonic()
        interval = max(30.0, float(_cfg("BROKER_COLLATERAL_ERROR_LOG_THROTTLE_SEC", 60.0)))
        with self._lock:
            last = self._last_warning.get(key, 0.0)
            if now - last < interval:
                return
            self._last_warning[key] = now
        logger.warning("Collateral snapshot refresh retained last verified state authority=%s reason=%s", key, message)

    def _refresh(self, key: str, venue: str, manager: Any) -> None:
        now = time.monotonic()
        ok = False
        error = ""
        try:
            balance = manager.get_balance() if hasattr(manager, "get_balance") else {}
            ok, available, source, error = self._valid_balance(balance)
            if ok:
                snap = CollateralSnapshot(key, venue, available, source, now, True, "")
                with self._lock:
                    prior = self._snapshots.get(key)
                    self._snapshots[key] = snap
                    self._failures[key] = 0
                if prior is None:
                    logger.info("Collateral snapshot ready authority=%s source=%s available=$%.4f", key, source, available)
            else:
                self._warn_throttled(key, error)
        except Exception as exc:
            error = str(exc)
            self._warn_throttled(key, error)
        with self._lock:
            failures = 0 if ok else min(8, self._failures.get(key, 0) + 1)
            self._failures[key] = failures
            base = self._interval(key)
            backoff = base if ok else min(base * (2 ** failures), float(_cfg("BROKER_COLLATERAL_MAX_BACKOFF_SEC", 300.0)))
            self._next_due[key] = now + backoff

    def _run(self) -> None:
        while not self._stop.is_set():
            now = time.monotonic()
            with self._lock:
                sources = list(self._sources.items())
                due = [(key, venue, manager) for key, (venue, manager) in sources if now >= self._next_due.get(key, 0.0)]
                wait_candidates = [self._next_due.get(key, now + 1.0) - now for key, _ in sources]
            for key, venue, manager in due:
                if self._stop.is_set():
                    break
                self._refresh(key, venue, manager)
            with self._lock:
                if self._sources:
                    next_wait = max(0.25, min((self._next_due.get(k, time.monotonic() + 1.0) - time.monotonic()) for k in self._sources))
                else:
                    next_wait = 1.0
            self._wake.wait(timeout=min(next_wait, 2.0))
            self._wake.clear()

    def cash_by_venue(self, router: Any, venues: set[str] | None = None) -> dict[str, float]:
        self.register_router(router)
        now = time.monotonic()
        max_age = max(5.0, float(_cfg("BROKER_COLLATERAL_SNAPSHOT_MAX_AGE_SEC", 120.0)))
        requested = venues or set(getattr(router, "available_exchanges", lambda: ())())
        out: dict[str, float] = {}
        with self._lock:
            for venue in requested:
                venue_key = str(venue).lower()
                authority = self._router_keys.get((id(router), venue_key))
                snap = self._snapshots.get(authority or "")
                out[venue_key] = (
                    max(0.0, snap.available)
                    if snap is not None and snap.verified and now - snap.updated_monotonic <= max_age
                    else 0.0
                )
        return out

    def snapshot_for(self, router: Any, venue: str) -> CollateralSnapshot | None:
        self.register_router(router)
        with self._lock:
            key = self._router_keys.get((id(router), str(venue).lower()))
            return self._snapshots.get(key or "")

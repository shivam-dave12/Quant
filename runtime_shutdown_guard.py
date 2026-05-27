"""Container-safe process shutdown handling.

A trading service running as PID 1 must cooperate with its container runtime:
Podman, Docker, Kubernetes and systemd use SIGTERM to request a graceful stop.
Ignoring SIGTERM does not keep a container alive; it causes the runtime to issue
SIGKILL after its grace period and prevents deterministic cleanup.

Authenticated Telegram commands still control trading while the service is live.
External SIGTERM/SIGINT controls only process lifecycle and performs an orderly
stop of data streams and strategy threads; it is not an order-management signal.
"""

from __future__ import annotations

import os
import signal
import threading
from typing import Callable, Iterable, Optional

_INSTALLED = False
_SHUTDOWN_REQUESTED = threading.Event()
_CALLBACK: Optional[Callable[[str], None]] = None
_LOCK = threading.RLock()


def _safe_cmdline(pid: int) -> str:
    """Best-effort /proc cmdline reader used only for diagnostics."""
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as fh:
            raw = fh.read().replace(b"\x00", b" ").strip()
        return raw.decode("utf-8", errors="replace") or "?"
    except Exception:
        return "?"


def _signal_name(signum: int) -> str:
    try:
        return signal.Signals(signum).name
    except Exception:
        return f"SIG{signum}"


def _default_signals() -> list[int]:
    names = ("SIGTERM", "SIGINT", "SIGHUP", "SIGQUIT")
    return [getattr(signal, name) for name in names if getattr(signal, name, None) is not None]


def shutdown_requested() -> bool:
    """Return whether lifecycle shutdown has been requested by the host runtime."""
    return _SHUTDOWN_REQUESTED.is_set()


def install_graceful_shutdown_handler(
    logger,
    runtime_name: str,
    shutdown_callback: Optional[Callable[[str], None]] = None,
    signals_to_handle: Optional[Iterable[int]] = None,
) -> None:
    """Translate process stop signals into a fast, orderly application stop.

    The callback must be non-blocking: it should flip runtime flags and wake any
    wait loop. Resource shutdown runs in normal program flow after the loop exits.
    """
    global _INSTALLED, _CALLBACK

    with _LOCK:
        if shutdown_callback is not None:
            _CALLBACK = shutdown_callback
        if _INSTALLED:
            logger.info("Graceful shutdown handler already active for %s", runtime_name)
            return

        handled = list(signals_to_handle) if signals_to_handle is not None else _default_signals()
        pid = os.getpid()
        ppid = os.getppid()
        proc_cmd = _safe_cmdline(pid)
        parent_cmd = _safe_cmdline(ppid)

        def _handler(signum, frame):  # noqa: ANN001 - Python signal callback signature
            del frame
            name = _signal_name(int(signum))
            first_request = not _SHUTDOWN_REQUESTED.is_set()
            _SHUTDOWN_REQUESTED.set()
            if first_request:
                logger.info(
                    "🛑 Runtime shutdown requested by %s for %s; beginning graceful stop | "
                    "pid=%s ppid=%s parent=%r cmd=%r",
                    name,
                    runtime_name,
                    pid,
                    ppid,
                    parent_cmd,
                    proc_cmd,
                )
            else:
                logger.warning("Repeated %s received while %s is stopping", name, runtime_name)
            callback = _CALLBACK
            if callback is not None:
                try:
                    callback(name)
                except Exception:
                    logger.exception("Graceful shutdown callback failed for %s", runtime_name)

        for sig in handled:
            try:
                signal.signal(sig, _handler)
            except Exception as exc:
                logger.warning("Could not register %s for %s: %s", _signal_name(int(sig)), runtime_name, exc)

        logger.info(
            "Container-safe shutdown handler active for %s | handled=%s | pid=%s ppid=%s parent=%r",
            runtime_name,
            ",".join(_signal_name(int(sig)) for sig in handled),
            pid,
            ppid,
            parent_cmd,
        )
        _INSTALLED = True


def install_telegram_only_shutdown_guard(logger, runtime_name: str, signals_to_guard=None) -> None:
    """Backward-compatible alias retained for old imports; no signal is ignored."""
    logger.warning(
        "Deprecated telegram-only shutdown guard requested for %s; using container-safe graceful shutdown instead",
        runtime_name,
    )
    install_graceful_shutdown_handler(logger, runtime_name, signals_to_handle=signals_to_guard)

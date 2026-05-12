"""Runtime shutdown guard for Telegram-controlled trading processes.

Policy:
    The trading runtime must not shut itself down because the host, shell,
    Docker, systemd, SSH session, or process supervisor sends SIGTERM/SIGINT.
    Only an authenticated Telegram /stop command is authorised to call
    bot.stop().

Notes:
    * SIGKILL cannot be intercepted by any Python process.
    * Some supervisors send SIGKILL after a grace period if SIGTERM is ignored;
      configure the supervisor not to do that for this bot.
"""

from __future__ import annotations

import os
import signal
from typing import Iterable, Optional


_INSTALLED = False


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


def _guarded_signals() -> list[int]:
    names = ("SIGTERM", "SIGINT", "SIGHUP", "SIGQUIT")
    out: list[int] = []
    for name in names:
        sig = getattr(signal, name, None)
        if sig is not None and sig not in out:
            out.append(sig)
    return out


def install_telegram_only_shutdown_guard(
    logger,
    runtime_name: str,
    signals_to_guard: Optional[Iterable[int]] = None,
) -> None:
    """Install a process-level guard that refuses external shutdown signals.

    The handler deliberately does not call sys.exit(), controller.stop(), or
    bot.stop(). It only records the termination attempt so the process keeps
    trading until Telegram /stop performs the authorised shutdown path.
    """
    global _INSTALLED

    if _INSTALLED:
        logger.info(
            "🛡 Telegram-only shutdown guard already active for %s",
            runtime_name,
        )
        return

    guarded = list(signals_to_guard) if signals_to_guard is not None else _guarded_signals()
    pid = os.getpid()
    ppid = os.getppid()
    proc_cmd = _safe_cmdline(pid)
    parent_cmd = _safe_cmdline(ppid)

    def _handler(signum, frame):  # noqa: ANN001 - Python signal callback signature
        name = _signal_name(int(signum))
        logger.warning(
            "🛡 TELEGRAM-ONLY SHUTDOWN GUARD: external %s(%s) ignored for %s. "
            "Only authenticated Telegram /stop may stop the bot. "
            "pid=%s ppid=%s parent=%r cmd=%r",
            name,
            signum,
            runtime_name,
            pid,
            ppid,
            parent_cmd,
            proc_cmd,
        )

    for sig in guarded:
        try:
            signal.signal(sig, _handler)
        except Exception as exc:
            logger.warning(
                "Could not protect %s for %s: %s",
                _signal_name(int(sig)),
                runtime_name,
                exc,
            )

    logger.info(
        "🛡 Telegram-only shutdown guard active for %s | guarded=%s | pid=%s ppid=%s parent=%r",
        runtime_name,
        ",".join(_signal_name(int(sig)) for sig in guarded),
        pid,
        ppid,
        parent_cmd,
    )
    _INSTALLED = True

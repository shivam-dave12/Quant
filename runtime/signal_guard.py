"""Production signal handling for long-running trading processes."""
from __future__ import annotations

import json
import logging
import os
import signal
import time
from pathlib import Path
from typing import Callable, Optional

import config

logger = logging.getLogger(__name__)


def _record_signal(component: str, signum: int, protected: bool) -> None:
    try:
        Path("data").mkdir(exist_ok=True)
        Path("data/last_shutdown.json").write_text(json.dumps({
            "component": component,
            "signal": int(signum),
            "signal_name": getattr(signal.Signals(signum), "name", str(signum)),
            "protected": bool(protected),
            "pid": os.getpid(),
            "ppid": os.getppid(),
            "time": time.strftime("%Y-%m-%d %H:%M:%S %Z"),
            "note": (
                "External SIGTERM was ignored by runtime protection."
                if protected else
                "External SIGTERM/SIGINT accepted by process signal handler."
            ),
        }, indent=2), encoding="utf-8")
    except Exception:
        pass


def _on_main_thread() -> bool:
    try:
        import threading
        return threading.current_thread() is threading.main_thread()
    except Exception:
        return True


def install_signal_handlers(
    component: str,
    *,
    shutdown: Optional[Callable[[], None]] = None,
    notify: Optional[Callable[[str], None]] = None,
) -> None:
    """Ignore accidental SIGTERM while preserving explicit shutdown paths."""
    if not _on_main_thread():
        return

    def _handle(signum, _frame) -> None:
        protect = (
            int(signum) == int(signal.SIGTERM)
            and bool(getattr(config, "RUNTIME_PROTECT_EXTERNAL_SIGTERM", True))
        )
        _record_signal(component, int(signum), protect)
        if protect:
            msg = (
                f"{component}: ignored external SIGTERM({int(signum)}) because "
                "RUNTIME_PROTECT_EXTERNAL_SIGTERM=True"
            )
            logger.warning(msg)
            if notify is not None:
                try:
                    notify(
                        "External SIGTERM received and ignored by runtime protection. "
                        "Use /stop for intentional shutdown."
                    )
                except Exception:
                    pass
            return

        logger.info("%s: accepted signal %s; shutting down", component, int(signum))
        if shutdown is not None:
            try:
                shutdown()
            except Exception:
                logger.exception("%s: shutdown callback failed", component)
        raise SystemExit(0)

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)

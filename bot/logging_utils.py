from __future__ import annotations

import logging
import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo


DEFAULT_LOG_TIMEZONE = "Asia/Kolkata"


class TimezoneFormatter(logging.Formatter):
    def __init__(self, *args, timezone_name: str = DEFAULT_LOG_TIMEZONE, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._timezone = ZoneInfo(timezone_name)

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        dt = datetime.fromtimestamp(record.created, self._timezone)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.strftime("%Y-%m-%d %H:%M:%S %Z")


def _set_process_timezone(timezone_name: str) -> None:
    os.environ["TZ"] = timezone_name
    if hasattr(time, "tzset"):
        time.tzset()


def setup_logging(level: str = "INFO", timezone_name: str = DEFAULT_LOG_TIMEZONE) -> None:
    _set_process_timezone(timezone_name)
    try:
        from rich.logging import RichHandler  # type: ignore

        handlers = [
            RichHandler(
                rich_tracebacks=True,
                show_path=False,
                log_time_format="%H:%M:%S",
            )
        ]
        fmt = "%(message)s"
    except Exception:
        handler = logging.StreamHandler()
        handler.setFormatter(
            TimezoneFormatter(
                "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
                datefmt="%H:%M:%S",
                timezone_name=timezone_name,
            )
        )
        handlers = [handler]
        fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=fmt,
        datefmt="%H:%M:%S",
        handlers=handlers,
        force=True,
    )

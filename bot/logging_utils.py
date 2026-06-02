from __future__ import annotations

import logging


def setup_logging(level: str = "INFO") -> None:
    try:
        from rich.logging import RichHandler  # type: ignore
        handlers = [RichHandler(rich_tracebacks=True, show_path=False)]
        fmt = "%(message)s"
    except Exception:
        handlers = [logging.StreamHandler()]
        fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=fmt,
        datefmt="%H:%M:%S",
        handlers=handlers,
    )

"""Entry point for the institutional multi-desk algo platform."""

from __future__ import annotations

import logging
import sys
import threading
from datetime import datetime, timedelta, timezone

import config


IST = timezone(timedelta(hours=5, minutes=30))


class ISTFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=IST)
        return f"{dt.strftime('%Y-%m-%d %H:%M:%S')},{int(record.msecs):03d}"


logging.basicConfig(
    level=getattr(config, "LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    handlers=[logging.FileHandler("institutional_bot.log", encoding="utf-8"), logging.StreamHandler(sys.stdout)],
    force=True,
)
for handler in logging.getLogger().handlers:
    handler.setFormatter(ISTFormatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"))

logger = logging.getLogger(__name__)


class InstitutionalBot:
    """Single-symbol compatibility wrapper around the multi-desk runtime."""

    def __init__(self) -> None:
        from orchestration.multi_asset_bot import MultiAssetInstitutionalBot

        self._delegate = MultiAssetInstitutionalBot()

    def initialize(self) -> bool:
        return self._delegate.initialize()

    def start(self) -> bool:
        return self._delegate.start()

    def run(self) -> None:
        return self._delegate.run()

    def stop(self) -> None:
        return self._delegate.stop()

    def __getattr__(self, name: str):
        return getattr(self._delegate, name)


def main() -> None:
    from orchestration.multi_asset_bot import MultiAssetInstitutionalBot

    bot = MultiAssetInstitutionalBot()
    if threading.current_thread() is threading.main_thread():
        try:
            from runtime_shutdown_guard import install_telegram_only_shutdown_guard

            install_telegram_only_shutdown_guard(logger, "institutional-main")
        except Exception:
            logger.warning("shutdown guard unavailable", exc_info=True)
    if not bot.initialize():
        sys.exit(1)
    if not bot.start():
        sys.exit(1)
    try:
        bot.run()
    except Exception:
        logger.exception("fatal runtime error")
        bot.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()


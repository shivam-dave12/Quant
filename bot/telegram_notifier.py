from __future__ import annotations

import logging
from dataclasses import dataclass

import requests

from .config import BotConfig

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class TelegramNotifier:
    bot_token: str
    chat_id: str
    enabled: bool = True

    @classmethod
    def from_config(cls, cfg: BotConfig) -> "TelegramNotifier":
        return cls(
            bot_token=cfg.telegram_bot_token,
            chat_id=cfg.telegram_chat_id,
            enabled=bool(cfg.telegram_alerts_enabled and cfg.telegram_bot_token and cfg.telegram_chat_id),
        )

    def send_message(self, text: str) -> bool:
        if not self.enabled:
            return False
        try:
            response = requests.post(
                f"https://api.telegram.org/bot{self.bot_token}/sendMessage",
                json={
                    "chat_id": self.chat_id,
                    "text": text,
                    "disable_web_page_preview": True,
                },
                timeout=10,
            )
            if response.ok:
                return True
            log.warning("telegram alert failed | status=%s body=%s", response.status_code, response.text[:500])
        except Exception:
            log.exception("telegram alert failed")
        return False

"""Optional Telegram alert sink for structured platform events."""
from __future__ import annotations
import html, json, logging
from typing import Any
import requests
class TelegramNotifier:
    def __init__(self, token: str, chat_id: str, timeout: float = 10.0) -> None:
        self.token, self.chat_id, self.timeout = token, chat_id, timeout
    def alert(self, event: str, payload: dict[str, Any]) -> None:
        if not self.token or not self.chat_id: return
        text = f"<b>{html.escape(event)}</b>\n<pre>{html.escape(json.dumps(payload, default=str, sort_keys=True, indent=2))}</pre>"
        response = requests.post(f"https://api.telegram.org/bot{self.token}/sendMessage", json={"chat_id": self.chat_id, "text": text, "parse_mode": "HTML"}, timeout=self.timeout)
        if not response.ok: logging.getLogger(__name__).error("Telegram alert failed: %s", response.text)

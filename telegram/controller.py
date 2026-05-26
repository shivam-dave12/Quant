"""Read-only and emergency-halt Telegram control plane for the single strategy authority."""
from __future__ import annotations
import asyncio, json
from typing import Any
import requests
from orchestration.platform import InstitutionalPlatform

class TelegramController:
    def __init__(self, *, platform: InstitutionalPlatform, token: str, chat_id: str, session: Any | None = None) -> None:
        if not token or not chat_id: raise RuntimeError("TELEGRAM_CREDENTIALS_NOT_CONFIGURED")
        self.platform, self.token, self.chat_id = platform, token, str(chat_id); self.session = session or requests.Session(); self.offset = 0
    def _url(self, method: str) -> str: return f"https://api.telegram.org/bot{self.token}/{method}"
    def reply(self, text: str) -> None:
        response=self.session.post(self._url("sendMessage"),json={"chat_id":self.chat_id,"text":text},timeout=10); response.raise_for_status()
    def handle(self, text: str) -> None:
        command=text.strip().split()[0].lower(); status=self.platform.status()
        if command in {"/start","/status","/models"}: self.reply(json.dumps(status,default=str,indent=2))
        elif command == "/positions": self.reply(json.dumps(status["positions"],default=str,indent=2))
        elif command in {"/decisions","/executions","/attribution"}: self.reply(json.dumps(self.platform.research.read(command[1:])[-5:],default=str,indent=2))
        elif command == "/halt": self.platform.halt_new_entries("TELEGRAM_EMERGENCY_HALT"); self.reply("New entries halted. Existing protected positions require monitoring/reconciliation.")
        else: self.reply("Commands: /status /models /positions /decisions /executions /attribution /halt")
    async def run(self) -> None:
        while True:
            response=await asyncio.to_thread(self.session.get,self._url("getUpdates"),params={"offset":self.offset,"timeout":20},timeout=30); response.raise_for_status()
            for item in response.json().get("result",[]):
                self.offset=int(item["update_id"])+1; message=item.get("message",{})
                if str(message.get("chat",{}).get("id",""))==self.chat_id and message.get("text"): self.handle(str(message["text"]))
            await asyncio.sleep(.1)

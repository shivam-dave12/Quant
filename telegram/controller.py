"""Telegram-first process controller for the institutional trading runtime."""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from typing import Any

import requests

from core.config import CONFIG
from core.environment import environment_diagnostics
from orchestration.bootstrap import RuntimeBootstrap
from orchestration.supervisor import ControllerPolicy, TelegramRuntimeSupervisor


class TelegramController:
    """Authorised command processor; the bot runtime exists only after `/start`."""

    def __init__(
        self,
        *,
        supervisor: TelegramRuntimeSupervisor,
        token: str,
        chat_id: str,
        session: Any | None = None,
    ) -> None:
        if not token or not chat_id:
            diagnostics = environment_diagnostics(("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"))
            raise RuntimeError(
                "TELEGRAM_CREDENTIALS_NOT_CONFIGURED | "
                f"source={diagnostics['source']} | "
                f"token_present={diagnostics['present']['TELEGRAM_BOT_TOKEN']} | "
                f"chat_id_present={diagnostics['present']['TELEGRAM_CHAT_ID']} | "
                "for Podman use --env-file /absolute/path/.env, or mount a read-only file "
                "and set BOT_ENV_FILE=/run/secrets/quant.env"
            )
        self.supervisor = supervisor
        self.token = token
        self.chat_id = str(chat_id)
        self.session = session or requests.Session()
        self.offset = 0

    def _url(self, method: str) -> str:
        return f"https://api.telegram.org/bot{self.token}/{method}"

    def reply(self, text: str) -> None:
        response = self.session.post(
            self._url("sendMessage"),
            json={"chat_id": self.chat_id, "text": text[:4000]},
            timeout=10,
        )
        response.raise_for_status()

    async def handle(self, text: str) -> None:
        parts = text.strip().split()
        command = parts[0].lower() if parts else ""
        if command == "/start":
            mode = parts[1].lower() if len(parts) > 1 else None
            self.reply(json.dumps(await self.supervisor.start(mode), default=str, indent=2))
        elif command == "/stop":
            self.reply(json.dumps(await self.supervisor.stop(), default=str, indent=2))
        elif command == "/halt":
            self.reply(json.dumps(self.supervisor.halt(), default=str, indent=2))
        elif command == "/resume":
            self.reply(json.dumps(self.supervisor.resume(), default=str, indent=2))
        elif command == "/status":
            self.reply(json.dumps(self.supervisor.status(), default=str, indent=2))
        elif command == "/models":
            self.reply(json.dumps(self.supervisor.status().get("platform") or self.supervisor.status()["preflight"], default=str, indent=2))
        elif command == "/positions":
            platform_status = self.supervisor.status().get("platform") or {}
            self.reply(json.dumps(platform_status.get("positions", []), default=str, indent=2))
        elif command in {"/decisions", "/executions", "/attribution"}:
            self.reply(json.dumps(self.supervisor.records(command[1:]), default=str, indent=2))
        else:
            self.reply(
                "Commands: /start [shadow|paper|live] /stop /halt /resume "
                "/status /models /positions /decisions /executions /attribution"
            )

    async def run(self) -> None:
        self.reply("Telegram controller online. Runtime is idle until /start shadow, /start paper, or /start live.")
        while True:
            response = await asyncio.to_thread(
                self.session.get,
                self._url("getUpdates"),
                params={"offset": self.offset, "timeout": 20},
                timeout=30,
            )
            response.raise_for_status()
            for item in response.json().get("result", []):
                self.offset = int(item["update_id"]) + 1
                message = item.get("message", {})
                if str(message.get("chat", {}).get("id", "")) == self.chat_id and message.get("text"):
                    await self.handle(str(message["text"]))
            await asyncio.sleep(0.1)


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    return default if value is None else value.strip().lower() in {"1", "true", "yes", "on"}


def build_supervisor(args: argparse.Namespace) -> TelegramRuntimeSupervisor:
    return TelegramRuntimeSupervisor(
        bootstrap=RuntimeBootstrap(CONFIG),
        policy=ControllerPolicy(
            configuration_module=args.configuration_module or "",
            default_mode=args.default_mode,
            dashboard_enabled=args.dashboard,
            dashboard_host=args.dashboard_host,
            dashboard_port=args.dashboard_port,
        ),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Telegram-owned institutional bot controller")
    parser.add_argument("--preflight-status", action="store_true")
    parser.add_argument("--configuration-module", default=os.getenv("BOT_CONFIGURATION_MODULE", ""))
    parser.add_argument("--default-mode", choices=("shadow", "paper", "live"), default=os.getenv("BOT_TELEGRAM_DEFAULT_MODE", "shadow"))
    parser.add_argument("--dashboard", action=argparse.BooleanOptionalAction, default=_env_bool("BOT_DASHBOARD_ENABLED", True))
    parser.add_argument("--dashboard-host", default=os.getenv("BOT_DASHBOARD_HOST", "0.0.0.0"))
    parser.add_argument("--dashboard-port", type=int, default=int(os.getenv("BOT_DASHBOARD_PORT", "8088")))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    bootstrap = RuntimeBootstrap(CONFIG)
    if args.preflight_status:
        print(json.dumps(bootstrap.preflight_status(), default=str, indent=2))
        return
    supervisor = build_supervisor(args)
    controller = TelegramController(
        supervisor=supervisor,
        token=CONFIG.secrets.telegram_bot_token,
        chat_id=CONFIG.secrets.telegram_chat_id,
    )
    asyncio.run(controller.run())


if __name__ == "__main__":
    main()

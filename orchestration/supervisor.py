"""Telegram-owned bot lifecycle supervisor; no trade runtime is created before /start."""
from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

from dashboard.server import DashboardServer
from orchestration.bootstrap import RuntimeBootstrap, RuntimeBundle


@dataclass(frozen=True)
class ControllerPolicy:
    configuration_module: str
    default_mode: str = "shadow"
    dashboard_enabled: bool = True
    dashboard_host: str = "0.0.0.0"
    dashboard_port: int = 8088


class TelegramRuntimeSupervisor:
    """Owns the active runtime task; authorised Telegram commands mutate its lifecycle."""

    def __init__(self, *, bootstrap: RuntimeBootstrap, policy: ControllerPolicy) -> None:
        if policy.default_mode not in {"shadow", "paper", "live"}:
            raise ValueError("INVALID_DEFAULT_RUNTIME_MODE")
        self.bootstrap = bootstrap
        self.policy = policy
        self.bundle: RuntimeBundle | None = None
        self.runtime_task: asyncio.Task[None] | None = None
        self.dashboard: DashboardServer | None = None
        self.dashboard_task: asyncio.Task[None] | None = None
        self.last_error: str | None = None
        self.last_stop_reason: str | None = None

    def _runtime_running(self) -> bool:
        return self.runtime_task is not None and not self.runtime_task.done()

    def _platform_status(self) -> dict[str, Any] | None:
        return self.bundle.platform.status() if self.bundle else None

    def status(self) -> dict[str, Any]:
        if self.runtime_task and self.runtime_task.done() and not self.runtime_task.cancelled() and self.last_error is None:
            exc = self.runtime_task.exception()
            if exc is not None:
                self.last_error = f"{type(exc).__name__}: {exc}"
        runtime_state = "RUNNING" if self._runtime_running() else "IDLE"
        if self.runtime_task and self.runtime_task.done() and self.last_error:
            runtime_state = "FAILED"
        return {
            "controller_online": True,
            "runtime_state": runtime_state,
            "runtime_mode": self.bundle.mode if self.bundle else None,
            "last_error": self.last_error,
            "last_stop_reason": self.last_stop_reason,
            "preflight": self.bootstrap.preflight_status(),
            "platform": self._platform_status(),
            "commands": [
                "/start shadow|paper|live",
                "/stop",
                "/halt",
                "/resume",
                "/status",
                "/positions",
                "/models",
                "/decisions",
                "/executions",
                "/attribution",
            ],
        }

    async def start(self, mode: str | None = None) -> dict[str, Any]:
        requested_mode = mode or self.policy.default_mode
        if requested_mode not in {"shadow", "paper", "live"}:
            return {"ok": False, "error": "MODE_MUST_BE_SHADOW_PAPER_OR_LIVE"}
        if self._runtime_running():
            return {"ok": False, "error": "RUNTIME_ALREADY_RUNNING", "mode": self.bundle.mode if self.bundle else None}
        await self._clear_completed_tasks()
        try:
            self.bundle = self.bootstrap.build_runtime(
                mode=requested_mode,
                configuration_module=self.policy.configuration_module,
            )
            self.runtime_task = asyncio.create_task(
                self.bundle.coordinator.run(live=requested_mode == "live"),
                name="institutional-trading-runtime",
            )
            self.runtime_task.add_done_callback(self._capture_runtime_completion)
            if self.policy.dashboard_enabled:
                self.dashboard = DashboardServer(
                    self.bundle.platform,
                    host=self.policy.dashboard_host,
                    port=self.policy.dashboard_port,
                )
                self.dashboard_task = asyncio.create_task(self.dashboard.run(), name="institutional-dashboard")
            await asyncio.sleep(0)
            if self.runtime_task.done():
                self.runtime_task.result()
            self.last_error = None
            self.last_stop_reason = None
            return {"ok": True, "runtime_state": "RUNNING", "mode": requested_mode}
        except Exception as exc:
            await self._shutdown_tasks()
            self.bundle = None
            self.last_error = f"{type(exc).__name__}: {exc}"
            return {"ok": False, "runtime_state": "FAILED", "error": self.last_error}

    async def stop(self) -> dict[str, Any]:
        if not self.bundle:
            return {"ok": True, "runtime_state": "IDLE", "message": "Runtime is not active."}
        positions = self.bundle.platform.status().get("positions", [])
        if positions:
            self.bundle.platform.halt_new_entries("TELEGRAM_STOP_REQUEST_WITH_OPEN_POSITIONS")
            self.last_stop_reason = "OPEN_PROTECTED_POSITIONS_MONITORING_RETAINED"
            return {
                "ok": False,
                "runtime_state": "RUNNING_HALTED",
                "message": "New entries halted. Runtime remains online to monitor/reconcile open protected positions.",
                "positions": positions,
            }
        await self._shutdown_tasks()
        self.bundle = None
        self.last_stop_reason = "TELEGRAM_STOP"
        return {"ok": True, "runtime_state": "IDLE", "message": "Runtime stopped by Telegram command."}

    def halt(self) -> dict[str, Any]:
        if not self.bundle:
            return {"ok": False, "error": "RUNTIME_NOT_ACTIVE"}
        self.bundle.platform.halt_new_entries("TELEGRAM_EMERGENCY_HALT")
        return {"ok": True, "message": "New entries halted; position monitoring remains active."}

    def resume(self) -> dict[str, Any]:
        if not self.bundle:
            return {"ok": False, "error": "RUNTIME_NOT_ACTIVE"}
        try:
            self.bundle.platform.resume_new_entries("TELEGRAM_RESUME")
        except RuntimeError as exc:
            return {"ok": False, "error": str(exc)}
        return {"ok": True, "message": "New entries resumed after safety checks."}

    def records(self, stream: str) -> list[dict[str, Any]]:
        if not self.bundle:
            return []
        return self.bundle.platform.research.read(stream)[-5:]


    def _capture_runtime_completion(self, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            self.last_error = f"{type(exc).__name__}: {exc}"

    async def _clear_completed_tasks(self) -> None:
        if self.runtime_task and self.runtime_task.done():
            with suppress(asyncio.CancelledError, Exception):
                self.runtime_task.result()
            await self._shutdown_tasks()
            self.bundle = None

    async def _shutdown_tasks(self) -> None:
        for task in (self.dashboard_task, self.runtime_task):
            if task and not task.done():
                task.cancel()
        for task in (self.dashboard_task, self.runtime_task):
            if task:
                with suppress(asyncio.CancelledError, Exception):
                    await task
        if self.dashboard:
            self.dashboard.shutdown()
        self.runtime_task = None
        self.dashboard_task = None
        self.dashboard = None
